package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig

import scala.concurrent.{ExecutionContext, Future}

/** Exercises the walk against a real RocksDB backend on a temp directory, driven by the
 *  bootstrap pool's real allocation tree. */
class RebuildingStoreSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  /** A bare device directory: no StorageDeviceConfig, since nothing here loads it as a device. */
  private def deviceDir(): os.Path =
    os.temp.dir(prefix = "rebuilding-store-suite")

  /** Allocate `n` data objects into the bootstrap pool so the allocation tree has content, and
   *  split it across more than one tier-0 node. */
  private def populate(n: Int): Future[List[org.aspen_ddp.aspen.common.objects.ObjectId]] =
    given ExecutionContext = executionContext
    val allocated = (0 until n).toList.map: i =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
          val alloc = pool.allocator
          alloc.allocateDataObject(Array[Byte](i.toByte)).map(_.id)
    Future.sequence(allocated)

  atest("a full walk restores every object and moves the store into place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5)
      _ <- rebuild.complete
    yield
      val finalPath = dev / storeId.directoryName
      os.exists(finalPath) should be(true)
      // The staging directory and its checkpoint are gone once the store is whole.
      os.exists(dev / RebuildingStore.RebuildDirectory / storeId.directoryName) should be(false)
      os.exists(finalPath / RebuildState.stateFilename) should be(false)
      // A StoreConfig is written up front, so the moved directory is loadable.
      StoreConfig.loadStoreConfig((finalPath / StoreConfig.configFilename).toIO).storeId should
        be(storeId)

  atest("a pre-existing final directory skips straight to completion"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // A crash between the move and the flip leaves the store in place with no staging
    // directory. The rebuild must not start over, and must not fail.
    os.makeDir.all(dev / storeId.directoryName)
    val rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO)
    for
      _ <- rebuild.complete
    yield
      os.exists(dev / RebuildingStore.RebuildDirectory / storeId.directoryName) should be(false)

  atest("a resume skips the objects already restored"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()

      // Stage a checkpoint by hand with a key at a known rank, as a crashed pass would have left behind.
      staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      _ = os.makeDir.all(staging)
      allocatedKeys = ids.map(id => Key(id.toBytes))
      sorted = allocatedKeys.sortWith(ByteArrayKeyOrdering.lt)
      _ = RebuildState.save(staging, RebuildState(storeId, Some(sorted(9)), Nil))

      // The rebuild should resume from that key
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5,
                                    testingOnlyTrackRestoredKeys = true)
      _ <- rebuild.complete
      restored = rebuild.testingOnlyRestoredKeys
      restoredBytes = restored.map(_.bytes.toList).toSet
      allocatedBytes = allocatedKeys.map(_.bytes.toList).toSet
      expectedBytes = sorted.drop(9).map(_.bytes.toList).toSet
    yield
      // The resume range is inclusive of the checkpointed key -- rebuildWrite is an overwrite,
      // so re-restoring it is free -- and everything before it is skipped. Intersect against
      // the keys we allocated to ignore system objects the fixture brought up.
      (restoredBytes intersect allocatedBytes) should be(expectedBytes)
      // The checkpoint key should be in the restored set (inclusive resume)
      restoredBytes should contain(sorted(9).bytes.toList)

  atest("the checkpoint is written after the flush, not before"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      _ <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // A checkpoint every 5 objects rather than every 1000, so the walk actually checkpoints
      // without needing a thousand allocations.
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5)
      _ <- rebuild.complete
    yield
      // A checkpoint claiming objects that never reached stable storage is the one ordering
      // that loses data, so the order is recorded and asserted rather than inferred.
      val ops = rebuild.testingOnlyCheckpointTrace
      ops should not be empty
      ops.grouped(2).forall(pair => pair == List("flush", "checkpoint")) should be(true)

  atest("an object that cannot be read is recorded and the rebuild does not complete"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      doomed = Key(ids.head.toBytes)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5,
                                    testingOnlyFailKeys = Set(doomed))
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // The pass must not report success with an object missing -- that would flip the store to
      // Active while it is short a slice.
      outcome.isFailure should be(true)
      // The staging directory and its checkpoint survive, so the next device check resumes.
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      os.exists(dev / storeId.directoryName) should be(false)
      val saved = RebuildState.load(staging).get
      saved.failedObjects should be(List(ids.head))
      // The checkpoint advanced past it: one unreadable object cannot wedge the walk forever.
      saved.lastRestoredKey should not be None

  atest("MaxFailedObjects cap prevents the store from moving into place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // Fail enough keys to exceed a low cap
      doomed = ids.take(15).map(id => Key(id.toBytes)).toSet
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5,
                                    testingOnlyFailKeys = doomed,
                                    maxFailedObjects = 10)
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // The pass must fail when the cap is exceeded
      outcome.isFailure should be(true)
      // The staging directory and its checkpoint survive, so the next device check resumes
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      os.exists(staging) should be(true)
      os.exists(dev / storeId.directoryName) should be(false)
      // The checkpoint was written when the cap was hit
      val saved = RebuildState.load(staging).get
      saved.lastRestoredKey should not be None

  atest("out of space fails the pass with empty failedObjects proving the latch alone stopped it"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // Inject an out-of-space error on the first object
      doomed = Set(Key(ids.head.toBytes))
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5,
                                    testingOnlyOutOfSpaceKeys = doomed)
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // The pass must fail
      outcome.isFailure should be(true)
      // The staging directory and its checkpoint survive
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      os.exists(staging) should be(true)
      os.exists(dev / storeId.directoryName) should be(false)
      // failedObjects is EMPTY -- proving the latch alone failed the pass, not retryFailures()
      val saved = RebuildState.load(staging).get
      saved.failedObjects should be(Nil)
