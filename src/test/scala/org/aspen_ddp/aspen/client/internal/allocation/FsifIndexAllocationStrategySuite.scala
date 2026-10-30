package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{RegisteredTypeFactory, TransactionAborted}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist
import org.aspen_ddp.aspen.common.util.byte2int
import org.aspen_ddp.aspen.server.store.backend.{MapBackend, RocksDBConfig}

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
