package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.scalatest.compatible
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key, ObjectId, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.BufferedConsistentRocksDB

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Exercises the walk against a real RocksDB backend on a temp directory, driven by the
 *  bootstrap pool's real allocation tree. */
class RebuildingStoreSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  /** A bare device directory: no StorageDeviceConfig, since nothing here loads it as a device. */
  private def deviceDir(): os.Path =
    os.temp.dir(prefix = "rebuilding-store-suite")

  /** Allocate `n` data objects into the bootstrap pool so the allocation tree has content, and
   *  split it across more than one tier-0 node. */
  private def populate(n: Int): Future[List[ObjectId]] =
    given ExecutionContext = executionContext
    val allocated = (0 until n).toList.map: i =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
          val alloc = pool.allocator
          alloc.allocateDataObject(Array[Byte](i.toByte)).map(_.id)
    Future.sequence(allocated)

  /** The rebuild's outcome, or None if it has still not finished after `maxYields` turns of the
   *  test's execution context.
   *
   *  Never awaits an incomplete future. The failure mode this exists for is a rebuild whose
   *  future never completes at all, and awaiting one of those hangs the suite instead of
   *  failing it.
   */
  private def outcomeOrStalled(rebuild: RebuildingStore,
                               maxYields: Int = 5000): Future[Option[Try[Unit]]] =
    given ExecutionContext = executionContext
    def loop(n: Int): Future[Option[Try[Unit]]] = rebuild.complete.value match
      case Some(outcome) => Future.successful(Some(outcome))
      case None if n == 0 => Future.successful(None)
      case None => Future(()).flatMap(_ => loop(n - 1))
    loop(maxYields)

  atest("a full walk restores every object and moves the store into place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5)
      _ <- rebuild.complete

      // Reopen the rebuilt store and verify the objects are actually retrievable.
      db = new BufferedConsistentRocksDB((dev / storeId.directoryName).toNIO)
      // Hand-rolled key encoding: RocksDBBackend.tokey is private and the backend's read path
      // (RocksDBBackend.read) requires a CompletionHandler, which is heavyweight for this check.
      // This couples to the encoding but is the simplest accessible read path. If
      // RocksDBBackend.tokey's encoding changes, this test will read the wrong keys and silently
      // stop proving reconstruction correctness.
      tokey = (id: ObjectId) =>
        val bb = ByteBuffer.allocate(16)
        bb.putLong(0, id.uuid.getMostSignificantBits)
        bb.putLong(8, id.uuid.getLeastSignificantBits)
        bb.array()
      readResults <- Future.sequence(ids.map(id => db.get(tokey(id))))
      _ <- db.close()
    yield
      val finalPath = dev / storeId.directoryName
      os.exists(finalPath) should be(true)
      // The staging directory and its checkpoint are gone once the store is whole.
      os.exists(dev / RebuildingStore.RebuildDirectory / storeId.directoryName) should be(false)
      os.exists(finalPath / RebuildState.stateFilename) should be(false)
      // A StoreConfig is written up front, so the moved directory is loadable.
      StoreConfig.loadStoreConfig((finalPath / StoreConfig.configFilename).toIO).storeId should
        be(storeId)

      // Every allocated object was reconstructed and is readable from the rebuilt store.
      readResults.size should be(20)
      readResults.forall(_.isDefined) should be(true)

  atest("a pre-existing final directory skips straight to completion"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // A crash between the move and the flip leaves the store in place. The rebuild must not
    // start over, and must not fail.
    os.makeDir.all(dev / storeId.directoryName)
    // Staged as a crash just before the move would have left it: the move is not atomic with
    // the flip, so a staging directory carrying a checkpoint can outlive the final directory
    // appearing. Pre-created so that asserting on its removal is an assertion about
    // cleanupStaging rather than about a directory that was never there.
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    os.makeDir.all(staging)
    RebuildState.save(staging, RebuildState(storeId, Some(Key(Array[Byte](7))), Nil))

    for
      _ <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    testingOnlyTrackRestoredKeys = true)
      _ <- rebuild.complete
    yield
      os.exists(staging) should be(false)
      // Nothing was walked: the store is already whole, and re-reading a pool's worth of objects
      // to rewrite a store that already holds them is precisely the work this branch exists to
      // skip.
      rebuild.testingOnlyRestoredKeys should be(Nil)

  /** Shared body of the torn-checkpoint tests: `contents` is written where a checkpoint belongs
   *  and the pass must restart from the beginning rather than throw at its first statement. */
  private def tornCheckpointRestartsTheWalk(contents: String): Future[compatible.Assertion] =
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      _ = os.makeDir.all(staging)
      _ = os.write.over(staging / RebuildState.stateFilename, contents)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5,
                                    testingOnlyTrackRestoredKeys = true)
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // Aspen is crash-only, so this file is what an ordinary termination in an unlucky window
      // leaves behind. Throwing on it would make the store unrebuildable until an operator
      // deleted the file by hand.
      outcome.isSuccess should be(true)
      val restoredBytes = rebuild.testingOnlyRestoredKeys.map(_.bytes.toList).toSet
      val allocatedBytes = ids.map(id => Key(id.toBytes).bytes.toList).toSet
      // From the beginning: every object, not the tail of some range the torn file half-named.
      (allocatedBytes subsetOf restoredBytes) should be(true)
      os.exists(dev / storeId.directoryName) should be(true)

  atest("an empty checkpoint restarts the walk from the beginning"):
    tornCheckpointRestartsTheWalk("")

  atest("an unparsable checkpoint restarts the walk from the beginning"):
    tornCheckpointRestartsTheWalk("failed-objects: [unclosed")

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

      // The count is what makes this a test of the cap rather than a second copy of the test
      // above. On the cap path the latch fires at failure 11 -- one past a cap of 10 -- the
      // remaining doomed keys short-circuit, and runPass fails at the abortCause check, so
      // retryFailures never runs and the checkpoint keeps all 11. With the cap raised out of
      // reach all 15 injected failures accumulate instead, and every other assertion here still
      // passes. Named for the mechanism rather than the arithmetic, so the intent survives a
      // change to either number.
      val expectedFailures = 10 + 1
      saved.failedObjects.size should be(expectedFailures)
      outcome.failed.get.getMessage should include("more than 10 unreadable objects")

  atest("an undecodable pointer in the allocation tree is a failed object, not a wedged walk"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // A key of the right shape -- sixteen bytes, so it names an object -- whose value is not a
    // pointer. The leading 0xff makes Varint.getUnsignedInt read past the end of the buffer, so
    // ObjectPointer's decode throws BufferUnderflowException. Written through the real tree
    // rather than injected, because the decode is what is under test.
    val bogusId = ObjectId(UUID.fromString("77777777-7777-7777-7777-777777777777"))
    val bogusKey = Key(bogusId.toBytes)

    for
      _ <- populate(20)
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
               pool.allocationTree.set(bogusKey, Value(Array[Byte](-1, -1, -1)))
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev.toNIO,
                                    checkpointInterval = 5)
      settled <- outcomeOrStalled(rebuild)
    yield
      // The decode runs in restoreObject's plain body, outside any Future combinator. Before it
      // was made total, the throw was absorbed by ExecutionContext.reportFailure from inside
      // walkFrom's own onComplete recursion: the recursion stopped, walkFrom's promise was never
      // completed, runPass never completed, and the StoreManager rebuild slot was never
      // released. Two of those deadlock a host at the default maxConcurrentRebuilds of 2. So the
      // assertion that matters is that the future completed at all.
      settled.isDefined should be(true)
      // Having completed, it must report the pass as unsuccessful rather than move a store that
      // is short an object into place.
      settled.get.isFailure should be(true)
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      os.exists(dev / storeId.directoryName) should be(false)
      // Recorded as one more failed object, subject to the existing cap and retry machinery.
      RebuildState.load(staging).get.failedObjects should be(List(bogusId))

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
