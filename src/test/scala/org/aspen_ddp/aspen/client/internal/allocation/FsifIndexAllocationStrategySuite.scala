package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{KeyValueObjectState, ObjectSizeLimitExceeded, RegisteredTypeFactory, TransactionAborted}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.util.{byte2int, byte2uuid}
import org.aspen_ddp.aspen.compute.{DurableTaskPointer, ServiceEntry}
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}
import org.aspen_ddp.aspen.server.store.backend.{FsifConfig, MapBackend, RocksDBConfig}

import scala.concurrent.{ExecutionContext, Future}

class FsifIndexAllocationStrategySuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(FsifIndexAllocationStrategy)

  private given ExecutionContext = executionContext

  private val ida = Replication(3, 2)

  /** A new ordinary pool on the bootstrap device set, with its stores served.
    *
    * The TestNetwork's host (`smgr`) never picks a new pool's stores up by itself: its host directory is `os.root`,
    * which holds no device directories, so it has no LocalStorageDeviceState and its device check has nothing to
    * reconcile. The stores are loaded into it directly, as in-memory MapBackends, which is all a store on the
    * bootstrap device is in this harness.
    */
  private def createServedPool(): Future[PoolId] =
    for
      poolId <- client.createNewStoragePool(s"fsif-strategy-${java.util.UUID.randomUUID()}", ida, None, RocksDBConfig(),
                  StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      loads = (0 until ida.width).toList.map: i =>
        net.smgr.loadStore(net.storageDeviceId, new MapBackend(StoreId(poolId, i.toByte)), None)
      _ = handleEvents()
      _ <- Future.sequence(loads)
      _ <- waitForTransactionsToComplete()
    yield poolId

  /** Allocates the zeroed bitmap and points the pool at the strategy, in one transaction. Returns a fresh allocator
    * and the bitmap pointer.
    */
  private def installStrategy(poolId: PoolId, indexCount: Int): Future[(PoolObjectAllocator, KeyValueObjectPointer)] =
    for
      poolPtr <- client.getStoragePoolPointer(poolId)
      radiclePool <- client.getStoragePool(Radicle.poolId)
      tx = client.newTransaction()
      entries <- FsifIndexAllocationStrategy.prepareBitmap(indexCount, radiclePool.allocator)(using tx)
      _ = tx.update(poolPtr, None, None, entries.keys.toList.map(DoesNotExist(_)),
            entries.toList.map((k, v) => Insert(k, v.bytes)))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ = client.dropCachedStoragePool(poolId)
      pool <- client.getStoragePool(poolId)
    yield
      val (_, bitmap) = FsifIndexAllocationStrategy.decodeConfig(
        entries(StoragePoolState.AllocationStrategyConfigKey).bytes)
      (new PoolObjectAllocator(client, pool), bitmap)

  private def install(indexCount: Int): Future[(PoolId, PoolObjectAllocator, KeyValueObjectPointer)] =
    for
      poolId <- createServedPool()
      (allocator, bitmap) <- installStrategy(poolId, indexCount)
    yield (poolId, allocator, bitmap)

  /** Every index whose bit is set, read fresh */
  private def setBits(bitmap: KeyValueObjectPointer): Future[Set[Int]] =
    client.read(bitmap).map: kvos =>
      kvos.contents.toList.flatMap: (key, vs) =>
        val segment = byte2int(key.bytes)
        (0 until FsifIndexBitmap.SegmentBits).filter(b => FsifIndexBitmap.isSet(vs.value.bytes, b))
          .map(b => FsifIndexBitmap.index(segment, b))
      .toSet

  private def indexOf(ptr: ObjectPointer): Int = byte2int(ptr.storePointer)

  private def allocateOne(allocator: PoolObjectAllocator): Future[ObjectPointer] =
    val tx = client.newTransaction()
    for
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](1)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
    yield ptr

  private def delete(ptr: ObjectPointer): Future[Unit] =
    for
      meta <- client.readMetadata(ptr)
      tx = client.newTransaction()
      _ = tx.setRefcount(ptr, meta.refcount, meta.refcount.decrement())
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
    yield ()

  /** Every enrolled system task's state object, read fresh */
  private def systemTasks(): Future[List[KeyValueObjectState]] =
    val services = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- services.get(Key(SystemTaskExecutorService.ServiceUUID))
      enrolled <- SystemTaskServiceState.scan(client, ServiceEntry.decode(vs.get.value.bytes).statePointer)
      tasks <- Future.sequence(enrolled.map((_, ptr) => client.read(ptr)))
    yield tasks

  private def releaseTasks(): Future[List[KeyValueObjectState]] =
    systemTasks().map(_.filter: kvos =>
      byte2uuid(kvos.contents(SimpleTaskExecutor.TaskTypeKey).value.bytes) == FsifReleaseIndicesTask.typeUUID)

  /** Runs one enrolled task to completion in this client. No host claims the SystemTaskExecutorService here, so
    * nothing else runs it.
    */
  private def runTask(kvos: KeyValueObjectState): Future[Unit] =
    // createTask ignores the TaskExecutor
    val task = FsifReleaseIndicesTask.createTask(client, DurableTaskPointer(kvos.pointer), kvos.revision,
      kvos.contents, null)
    for
      _ <- task.completed
      _ <- waitForTransactionsToComplete()
    yield ()

  atest("allocations get distinct 4-byte indices and set exactly their bits"):
    for
      (_, allocator, bitmap) <- install(3000)
      tx = client.newTransaction()
      dos <- Future.sequence((1 to 5).map(i => allocator.allocateDataObject(DataBuffer(Array[Byte](i.toByte)))(using tx)))
      kvs <- Future.sequence((1 to 2).map(_ => allocator.allocateKeyValueObject(Map())(using tx)))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      bits <- setBits(bitmap)
    yield
      val ptrs: List[ObjectPointer] = dos.toList ++ kvs.toList
      ptrs.foreach(_.storePointer.length should be(4))
      val indices = ptrs.map(indexOf)
      indices.distinct.size should be(7)
      indices.foreach(i => i should (be >= 0 and be < 3000))
      bits should be(indices.toSet)

  atest("allocations in one transaction stay in one segment until it is full"):
    // Only pointers are taken: segment choice happens when a pointer is created, and committing 1030 allocations
    // (each with its AllocationTree finalization action) would add nothing to what this checks
    for
      (_, allocator, _) <- install(2048)
      tx = client.newTransaction()
      ptrs <- Future.sequence((1 to 1030).map(_ => allocator.createDataObjectPointer()(using tx)))
    yield
      val bySegment = ptrs.map(indexOf).groupBy(i => FsifIndexBitmap.locate(i)._1).view.mapValues(_.size).toMap
      bySegment.values.toList.sorted should be(List(6, 1024))
      ptrs.map(indexOf).distinct.size should be(1030)

  atest("an aborted allocation leaves the bitmap unchanged"):
    for
      (_, allocator, bitmap) <- install(64)
      tx = client.newTransaction()
      _ <- allocator.allocateDataObject(DataBuffer(Array[Byte](1)))(using tx)
      _ = tx.update(radicle, None, None, List(DoesNotExist(Radicle.SystemIdKey)), Nil)
      failure <- tx.commit().failed
      _ <- waitForTransactionsToComplete()
      bits <- setBits(bitmap)
    yield
      failure shouldBe a[TransactionAborted]
      bits should be(Set())

  atest("a pointer created but never staged sets no bit"):
    for
      (_, allocator, bitmap) <- install(64)
      tx = client.newTransaction()
      unstaged <- allocator.createDataObjectPointer()(using tx)
      staged <- allocator.allocateDataObject(DataBuffer(Array[Byte](1)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      bits <- setBits(bitmap)
    yield
      indexOf(unstaged) should not be indexOf(staged)
      bits should be(Set(indexOf(staged)))

  atest("deleting an object clears its bit in the same transaction and the index can be reused"):
    for
      (_, allocator, bitmap) <- install(1)
      first <- allocateOne(allocator)
      afterAlloc <- setBits(bitmap)
      _ <- delete(first)
      afterDelete <- setBits(bitmap)
      second <- allocateOne(allocator)
      bits <- setBits(bitmap)
    yield
      afterAlloc should be(Set(0))
      afterDelete should be(Set())
      indexOf(second) should be(0)
      bits should be(Set(0))

  atest("an index freed in a transaction is not reallocated in it"):
    for
      (poolId, allocator, bitmap) <- install(1)
      o <- allocateOne(allocator)
      meta <- client.readMetadata(o)
      tx = client.newTransaction()
      _ = tx.setRefcount(o, meta.refcount, meta.refcount.decrement())
      failure <- allocator.allocateDataObject(DataBuffer(Array[Byte](2)))(using tx).failed
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      afterDelete <- setBits(bitmap)
      next <- allocateOne(allocator)
    yield
      failure should be(FsifIndexAllocationStrategy.IndicesExhausted(poolId))
      afterDelete should be(Set())
      indexOf(next) should be(0)

  atest("a full pool fails allocation with IndicesExhausted"):
    for
      (poolId, allocator, bitmap) <- install(3)
      ptrs <- (1 to 3).foldLeft(Future.successful(List.empty[ObjectPointer])): (prev, _) =>
                prev.flatMap(acc => allocateOne(allocator).map(_ :: acc))
      tx = client.newTransaction()
      failure <- allocator.allocateDataObject(DataBuffer(Array[Byte](4)))(using tx).failed
      bits <- setBits(bitmap)
    yield
      ptrs.map(indexOf).toSet should be(Set(0, 1, 2))
      failure should be(FsifIndexAllocationStrategy.IndicesExhausted(poolId))
      bits should be(Set(0, 1, 2))

  atest("concurrent allocations in one segment: the second to commit aborts"):
    for
      (_, allocator, bitmap) <- install(64)
      tx1 = client.newTransaction()
      tx2 = client.newTransaction()
      p1 <- allocator.allocateDataObject(DataBuffer(Array[Byte](1)))(using tx1)
      _ <- allocator.allocateDataObject(DataBuffer(Array[Byte](2)))(using tx2)
      _ <- tx1.commit()
      failure <- tx2.commit().failed
      _ <- waitForTransactionsToComplete()
      bits <- setBits(bitmap)
    yield
      failure shouldBe a[TransactionAborted]
      bits should be(Set(indexOf(p1)))

  atest("deleting an object whose storePointer is not a valid index fails the transaction"):
    for
      poolId <- createServedPool()
      plain <- client.getStoragePool(poolId)
      legacy <- allocateOne(new PoolObjectAllocator(client, plain))
      _ <- installStrategy(poolId, 64)
      failure <- delete(legacy).failed
      _ <- waitForTransactionsToComplete()
      remaining <- client.readMetadataOptional(legacy)
    yield
      legacy.storePointer.isEmpty should be(true)
      failure shouldBe an[IllegalArgumentException]
      remaining.isDefined should be(true)

  atest("a deferred release keeps the bit set and enrolls one task that clears it"):
    for
      (_, allocator, bitmap) <- install(16)
      o <- allocateOne(allocator)
      before <- systemTasks()
      meta <- client.readMetadata(o)
      tx = client.newTransaction()
      _ = tx.setRefcount(o, meta.refcount, meta.refcount.decrement())
      _ = tx.deferAllocationRelease()
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      afterDelete <- setBits(bitmap)
      all <- systemTasks()
      tasks <- releaseTasks()
      _ <- runTask(tasks.head)
      afterTask <- setBits(bitmap)
    yield
      afterDelete should be(Set(indexOf(o)))
      all.size should be(before.size + 1)
      tasks.size should be(1)
      afterTask should be(Set())

  /** Deletes `ptr` in a deferring transaction; `fail` adds a requirement that aborts it */
  private def deferredDelete(ptr: ObjectPointer, fail: Boolean): Future[Unit] =
    for
      meta <- client.readMetadata(ptr)
      tx = client.newTransaction()
      _ = tx.setRefcount(ptr, meta.refcount, meta.refcount.decrement())
      _ = tx.deferAllocationRelease()
      _ = if fail then tx.update(radicle, None, None, List(DoesNotExist(Radicle.SystemIdKey)), Nil)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
    yield ()

  atest("an aborted deferred delete enrolls no task"):
    for
      (_, allocator, bitmap) <- install(16)
      o <- allocateOne(allocator)
      before <- systemTasks()
      failure <- deferredDelete(o, fail = true).failed
      _ <- waitForTransactionsToComplete()
      afterAbort <- systemTasks()
      bits <- setBits(bitmap)
      // The same transaction without the failing requirement does enroll one, so the abort is what prevented it
      _ <- deferredDelete(o, fail = false)
      afterCommit <- releaseTasks()
    yield
      failure shouldBe a[TransactionAborted]
      afterAbort.size should be(before.size)
      bits should be(Set(indexOf(o)))
      afterCommit.size should be(1)

  atest("deferring in a transaction that also allocates still sets the new bit in the transaction"):
    for
      (_, allocator, bitmap) <- install(16)
      o <- allocateOne(allocator)
      meta <- client.readMetadata(o)
      tx = client.newTransaction()
      _ = tx.setRefcount(o, meta.refcount, meta.refcount.decrement())
      _ = tx.deferAllocationRelease()
      p <- allocator.allocateDataObject(DataBuffer(Array[Byte](2)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      afterCommit <- setBits(bitmap)
      tasks <- releaseTasks()
      _ <- runTask(tasks.head)
      afterTask <- setBits(bitmap)
    yield
      indexOf(p) should not be indexOf(o)
      afterCommit should be(Set(indexOf(o), indexOf(p)))
      tasks.size should be(1)
      afterTask should be(Set(indexOf(p)))

  atest("a deferring transaction that also allocates in the bootstrap pool commits and enrolls the task"):
    // The bootstrap pool's Default strategy needs no commit hook, so the task-state allocation the FSIF hook adds
    // there never counts as growth of an already-prepared pool, whatever order the hooks would run in
    for
      (_, allocator, bitmap) <- install(16)
      o <- allocateOne(allocator)
      radiclePool <- client.getStoragePool(Radicle.poolId)
      meta <- client.readMetadata(o)
      tx = client.newTransaction()
      _ = tx.setRefcount(o, meta.refcount, meta.refcount.decrement())
      _ = tx.deferAllocationRelease()
      other <- radiclePool.allocator.allocateDataObject(DataBuffer(Array[Byte](3)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      otherMeta <- client.readMetadataOptional(other)
      afterCommit <- setBits(bitmap)
      tasks <- releaseTasks()
      _ <- runTask(tasks.head)
      afterTask <- setBits(bitmap)
    yield
      otherMeta.isDefined should be(true)
      afterCommit should be(Set(indexOf(o)))
      tasks.size should be(1)
      afterTask should be(Set())

  atest("deferred deletions in two FSIF pools in one transaction fail it with IllegalArgumentException"):
    for
      (poolA, allocatorA, bitmapA) <- install(16)
      (poolB, allocatorB, bitmapB) <- install(16)
      a <- allocateOne(allocatorA)
      b <- allocateOne(allocatorB)
      before <- systemTasks()
      metaA <- client.readMetadata(a)
      metaB <- client.readMetadata(b)
      tx = client.newTransaction()
      _ = tx.setRefcount(a, metaA.refcount, metaA.refcount.decrement())
      _ = tx.setRefcount(b, metaB.refcount, metaB.refcount.decrement())
      _ = tx.deferAllocationRelease()
      failure <- tx.commit().failed
      _ <- waitForTransactionsToComplete()
      after <- systemTasks()
      bitsA <- setBits(bitmapA)
      bitsB <- setBits(bitmapB)
    yield
      failure shouldBe an[IllegalArgumentException]
      failure.getMessage should include(poolA.toString)
      failure.getMessage should include(poolB.toString)
      after.size should be(before.size)
      bitsA should be(Set(indexOf(a)))
      bitsB should be(Set(indexOf(b)))

  private def readPoolState(poolId: PoolId): Future[KeyValueObjectState] =
    client.getStoragePoolPointer(poolId).flatMap(client.read(_))

  atest("a new FSIF pool records the strategy and a zeroed bitmap in its metadata group"):
    for
      poolId <- client.createNewStoragePool(s"fsif-new-${java.util.UUID.randomUUID()}", ida, None,
                  FsifConfig.forObjectSize(ida, 4096, 3000), StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      _ <- waitForTransactionsToComplete()
      kvos <- readPoolState(poolId)
      (indexCount, bitmapPtr) = FsifIndexAllocationStrategy.decodeConfig(
        kvos.contents(StoragePoolState.AllocationStrategyConfigKey).value.bytes)
      bitmap <- client.read(bitmapPtr)
      groupId <- client.getDefaultMetadataGroupId()
      group <- client.getAllocationGroupState(groupId)
      pool <- client.getStoragePool(poolId)
    yield
      byte2uuid(kvos.contents(StoragePoolState.AllocationStrategyKey).value.bytes) should be(
        FsifIndexAllocationStrategy.typeUUID)
      indexCount should be(3000)
      group.members.exists(_.uuid == bitmapPtr.poolId.uuid) should be(true)
      bitmap.contents.size should be(3)
      bitmap.contents.values.foreach: vs =>
        vs.value.bytes.foreach(_ should be(0.toByte))
      pool.allocationStrategy shouldBe a[FsifIndexAllocationStrategy.Strategy]

  atest("a new RocksDB pool has no allocation strategy"):
    for
      poolId <- client.createNewStoragePool(s"rocks-new-${java.util.UUID.randomUUID()}", ida, None, RocksDBConfig(),
                  StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      kvos <- readPoolState(poolId)
    yield
      kvos.contents.contains(StoragePoolState.AllocationStrategyKey) should be(false)

  atest("createNewMetadataPool refuses FSIF"):
    client.createNewMetadataPool(s"fsif-meta-${java.util.UUID.randomUUID()}", ida, None,
      FsifConfig.forObjectSize(ida, 4096, 3000), StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L).failed.map: e =>
      e shouldBe an[IllegalArgumentException]

class FsifBitmapSizeLimitSuite extends IntegrationTestSuite:

  override def testPoolMaxSliceSize: Option[Int] = Some(1024)

  private given ExecutionContext = executionContext

  atest("a bitmap larger than its allocator's object limit fails with ObjectSizeLimitExceeded"):
    for
      radiclePool <- client.getStoragePool(Radicle.poolId)
      tx = client.newTransaction()
      failure <- FsifIndexAllocationStrategy.prepareBitmap(64 * 1024, radiclePool.allocator)(using tx)
        .flatMap(_ => tx.commit()).failed
    yield
      failure shouldBe an[ObjectSizeLimitExceeded]
