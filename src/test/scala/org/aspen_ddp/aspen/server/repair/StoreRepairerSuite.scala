package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{ObjectState as ClientObjectState, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Future, Promise}

class StoreRepairerSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  /** Duplicated rather than shared with MissedUpdateFinalizationAction on purpose: a bug in the
    * key layout must not cancel itself out between the writer and the reader.
    */
  private def buildErrorKey(poolIndex: Byte, objectId: ObjectId): Key =
    val bb = ByteBuffer.allocate(17)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(bb.array())

  /** Records what the service asked the host to do, and lets the test make the store depart. */
  private class RecordingTarget(hosted: List[StoreId]) extends RepairTarget:
    val departed = new AtomicBoolean(false)
    @volatile var repairs: List[ObjectId] = Nil
    @volatile var deletions: List[(ObjectId, Seq[Byte])] = Nil

    def repairableStoreIds: List[StoreId] = if departed.get then Nil else hosted

    def isRepairable(storeId: StoreId): Boolean = repairableStoreIds.contains(storeId)

    def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
      synchronized { repairs = repairs :+ os.pointer.id }
      completion.success(())

    def repairDelete(sid: StoreId, objectId: ObjectId,
                     storePointer: Array[Byte], completion: Promise[Unit]): Unit =
      synchronized { deletions = deletions :+ (objectId, storePointer.toSeq) }
      completion.success(())

  private def errorTree: Future[TieredKeyValueList] =
    client.getStoragePool(PoolId.BootstrapPoolId).map(_.errorTree)

  /** Seeds an errorTree entry directly. Provoking a real missed update is not deterministic in
    * TestNetwork, and the entry's shape is all StoreRepairer cares about.
    */
  private def seedErrorEntry(objectId: ObjectId, storePointer: Array[Byte]): Future[Unit] =
    errorTree.flatMap: tree =>
      val tx: Transaction = client.newRepairTransaction()
      given Transaction = tx
      tree.set(buildErrorKey(0.toByte, objectId), Value(storePointer)).flatMap: _ =>
        tx.commit().map(_ => ())

  private def errorEntryExists(objectId: ObjectId): Future[Boolean] =
    errorTree.flatMap(_.get(buildErrorKey(0.toByte, objectId))).map(_.isDefined)

  /** Allocates a real object. AllocationFinalizationAction writes the allocationTree entry, so
    * nothing here has to fake one -- waiting for the transaction to settle is enough.
    */
  private def allocateObject(): Future[ObjectId] =
    val f = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        pool <- client.getStoragePool(PoolId.BootstrapPoolId)
        allocator = new PoolObjectAllocator(client, pool)
        ptr <- allocator.allocateKeyValueObject(Map(Key(Array[Byte](9)) -> Value(Array[Byte](9))))
      yield ptr
    for
      ptr <- f
      _ <- net.waitForTransactionsToComplete()
    yield ptr.id

  test("an entry whose object is in the allocation tree is repaired and the entry removed"):
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      objectId <- allocateObject()
      _ <- seedErrorEntry(objectId, Array[Byte](1, 2, 3))
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(objectId)
    yield
      target.repairs should contain (objectId)
      result.repaired shouldBe 1
      result.deferred shouldBe 0
      stillThere shouldBe false

  test("an aged entry with no allocation is deleted from the store and the tree"):
    val objectId = ObjectId(new UUID(7, 7))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    // minErrorEntryAgeForDeletion of zero makes a freshly seeded entry immediately eligible.
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](4, 5))
      result <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe List((objectId, Seq[Byte](4, 5)))
      result.repaired shouldBe 1
      stillThere shouldBe false

  test("an empty store pointer reaches repairDelete as empty bytes"):
    val objectId = ObjectId(new UUID(8, 8))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte]())
      _ <- repairer.scan(storeId, policy)
    yield
      target.deletions shouldBe List((objectId, Seq.empty[Byte]))

  test("a fresh entry with no allocation is deferred, not deleted"):
    val objectId = ObjectId(new UUID(9, 9))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe Nil
      result.seen shouldBe 1
      result.deferred shouldBe 1
      result.repaired shouldBe 0
      stillThere shouldBe true

  test("a store that departs mid-scan stops receiving repair calls"):
    val objectId = ObjectId(new UUID(10, 10))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      _ = target.departed.set(true)
      result <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe Nil
      result.seen shouldBe 0
      stillThere shouldBe true

  test("a failed deletion leaves the error tree entry in place"):
    val objectId = ObjectId(new UUID(11, 11))
    val failing = new RepairTarget:
      def repairableStoreIds: List[StoreId] = List(storeId)
      def isRepairable(storeId: StoreId): Boolean = repairableStoreIds.contains(storeId)
      def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
      def repairDelete(sid: StoreId, oid: ObjectId,
                       sp: Array[Byte], completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
    val repairer = StoreRepairer(client, failing)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      _ <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      stillThere shouldBe true

  test("a failed update leaves the error tree entry in place"):
    val failing = new RepairTarget:
      def repairableStoreIds: List[StoreId] = List(storeId)
      def isRepairable(storeId: StoreId): Boolean = repairableStoreIds.contains(storeId)
      def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
      def repairDelete(sid: StoreId, oid: ObjectId,
                       sp: Array[Byte], completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
    val repairer = StoreRepairer(client, failing)
    for
      objectId <- allocateObject()
      _ <- seedErrorEntry(objectId, Array[Byte](2))
      _ <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(objectId)
    yield
      stillThere shouldBe true

  // Idempotence only. This does NOT prove the recursion fix: in TestNetwork the store that
  // misses updates is index 2, so a re-tracking regression would write its entry outside the
  // [0,1) range this scan covers. Task 1's missedUpdateTrackingEnabled test is the direct proof
  // that repair transactions disable tracking; this one checks a drained range stays drained.
  test("a second scan after a drained tree finds nothing"):
    val objectId = ObjectId(new UUID(12, 12))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      first <- repairer.scan(storeId, policy)
      _ <- net.waitForTransactionsToComplete()
      second <- repairer.scan(storeId, policy)
    yield
      first.seen shouldBe 1
      second.seen shouldBe 0

  test("a malformed errorTree entry does not hang the scan"):
    val objectId = ObjectId(new UUID(13, 13))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      // Seed a too-short key (only 5 bytes instead of 17) directly into the errorTree
      tree <- errorTree
      tx = client.newRepairTransaction()
      _ <- tree.set(Key(Array[Byte](0, 1, 2, 3, 4)), Value(Array[Byte]()))(using tx)
      _ <- tx.commit()
      // Seed a valid entry after the malformed one to prove the scan continued
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      result <- repairer.scan(storeId, policy)
      validEntryStillThere <- errorEntryExists(objectId)
    yield
      // The malformed entry is skipped (seen count excludes it because it threw before
      // incrementing), but the scan completes and processes the valid entry
      target.deletions shouldBe List((objectId, Seq[Byte](1)))
      result.repaired shouldBe 1
      validEntryStillThere shouldBe false

  test("a future dated entry is never eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val future = HLCTimestamp((1_756_742_400_000L + 5_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(future, now, Duration(60, SECONDS)).shouldBe(false)

  test("a freshly written entry is not yet eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val recent = HLCTimestamp((1_756_742_400_000L - 5_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(recent, now, Duration(60, SECONDS)).shouldBe(false)

  test("an aged entry is eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val old = HLCTimestamp((1_756_742_400_000L - 120_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(old, now, Duration(60, SECONDS)).shouldBe(true)
