package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.ida.{AppendDelta, RandomAccessReedSolomon, Replication, XorRegion}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Delete, Insert, Key, SetMax, SetRight, KeyValueOperation, KeyValueObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{FullContentLock, KeyRevision}
import org.aspen_ddp.aspen.common.transaction.{StaleObjectState, KeyTimestampError, TransactionRequirement, ContentMismatch, DataUpdate, DataUpdateOperation, KeyExistenceError, KeyValueUpdate, LocalTimeError, LocalTimeRequirement, MissingObjectUpdate, ObjectExists, RefcountMismatch, RefcountUpdate, MissingObject, RequirementCheckFailure, RequirementError, ExclusiveRevisionLock, RevisionMismatch, SharedRevisionLock, SliceSizeExceeded, TransactionCollision, TransactionId, UnflaggedDelete, BlindWrite, AppendMismatch, VersionBump, WithinRangeError}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap
import scala.language.implicitConversions

object RequirementsCheckerSuite {
  val oid1 = ObjectId(new UUID(0,1))
  val oid2 = ObjectId(new UUID(0,2))
  val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))
  val rev2 = ObjectRevision(TransactionId(new UUID(0, 4)))
  val ref1 = ObjectRefcount(1,1)
  val ref2 = ObjectRefcount(2,2)
  val ts1  = HLCTimestamp(1)
  val ts2  = HLCTimestamp(2)
  val tx1 = TransactionId(new UUID(0, 5))
  val tx2 = TransactionId(new UUID(0, 6))

  val p1 = DataObjectPointer(oid1, PoolId(new UUID(0,0)), Array[Byte]())
  val p2 = DataObjectPointer(oid2, PoolId(new UUID(0,0)), Array[Byte]())

  val kp1 = KeyValueObjectPointer(oid1, PoolId(new UUID(0,0)), Array[Byte]())
}

class RequirementsCheckerSuite extends AnyFunSuite with Matchers {

  import RequirementsCheckerSuite._

  def mkobjs(): (ObjectState, ObjectState) = {

    val o1 = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    val o2 = new ObjectState(
      oid2,
      Metadata(rev2, ref2, ts2),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    (o1, o2)
  }

  /* def check(transactionId: TransactionId,
            requirements: List[TransactionRequirement],
            objects: HashMap[ObjectId, ObjectState],
            objectUpdates: HashMap[ObjectId, DataBuffer]):
  (HashMap[ObjectId, Value], List[Value]) = {*/

  test("DataUpdate okay") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  // Slice of data store `store` holding `content`, plus a delta appending `more` to it
  private def appendFixture(content: Array[Byte], more: Array[Byte], store: Int = 0) = {
    val ida = RandomAccessReedSolomon(5, 3, 4, 8)
    val slice = ida.encode(DataBuffer(content))(store)
    val delta = ida.appendDeltas(content.length, more)(store)
    (slice, delta)
  }

  private def checkAppend(slice: DataBuffer, payload: Option[DataBuffer], reqRev: ObjectRevision = rev1,
                          op: DataUpdateOperation.Value = DataUpdateOperation.Append) = {
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data, slice)
    o.lockedToTransaction = Some(tx1)
    val req = DataUpdate(p1, reqRev, op)
    val updates = payload.map(p => HashMap[ObjectId, DataBuffer](oid1 -> p)).getOrElse(HashMap[ObjectId, DataBuffer]())
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), HashMap(oid1 -> o), updates)
  }

  test("Append with matching revision and delta passes") {
    val (slice, delta) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array.tabulate[Byte](30)(i => (i + 50).toByte))
    val (oerrs, errs) = checkAppend(slice, Some(delta.encode))
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("Append with wrong revision fails with RevisionMismatch") {
    val (slice, delta) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1, 2, 3))
    val (oerrs, errs) = checkAppend(slice, Some(delta.encode), rev2)
    assert(oerrs == Map(oid1 -> RevisionMismatch()))
    assert(errs.isEmpty)
  }

  test("Append whose expected slice length differs fails with AppendMismatch") {
    val (slice, delta) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1, 2, 3))
    val longer = DataBuffer(slice.getByteArray ++ Array.fill[Byte](8)(0))
    val (oerrs, errs) = checkAppend(longer, Some(delta.encode))
    assert(oerrs == Map(oid1 -> AppendMismatch()))
    assert(errs.isEmpty)
  }

  test("Append whose expected trailer differs fails with AppendMismatch") {
    val (slice, delta) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1, 2, 3), store = 2)
    // Store k-1 holds the trailer, so its delta carries an expected tail
    assert(delta.expectedTail.isDefined)
    val changed = slice.getByteArray
    changed(changed.length - 1) = (changed(changed.length - 1) ^ 0x55).toByte
    val (oerrs, errs) = checkAppend(DataBuffer(changed), Some(delta.encode))
    assert(oerrs == Map(oid1 -> AppendMismatch()))
    assert(errs.isEmpty)
  }

  test("Append with an undecodable payload fails with AppendMismatch") {
    val (slice, _) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1, 2, 3))
    val (oerrs, errs) = checkAppend(slice, Some(DataBuffer(Array[Byte](1, 2, 3))))
    assert(oerrs == Map(oid1 -> AppendMismatch()))
    assert(errs.isEmpty)
  }

  test("WriteRange with a matching mid-slice delta passes") {
    val (slice, _) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1))
    val delta = AppendDelta(slice.size, slice.size, List(XorRegion(2, Array[Byte](5, 6))), None)
    val (oerrs, errs) = checkAppend(slice, Some(delta.encode), op = DataUpdateOperation.WriteRange)
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("WriteRange whose expected slice length differs fails with AppendMismatch") {
    val (slice, _) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1))
    val delta = AppendDelta(slice.size + 8, slice.size + 8, List(XorRegion(2, Array[Byte](5, 6))), None)
    val (oerrs, errs) = checkAppend(slice, Some(delta.encode), op = DataUpdateOperation.WriteRange)
    assert(oerrs == Map(oid1 -> AppendMismatch()))
    assert(errs.isEmpty)
  }

  test("WriteRange without an object update fails with MissingObjectUpdate") {
    val (slice, _) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1))
    val (oerrs, errs) = checkAppend(slice, None, op = DataUpdateOperation.WriteRange)
    assert(oerrs == Map(oid1 -> MissingObjectUpdate()))
    assert(errs.isEmpty)
  }

  test("Append without an object update fails with MissingObjectUpdate") {
    val (slice, _) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array[Byte](1, 2, 3))
    val (oerrs, errs) = checkAppend(slice, None)
    assert(oerrs == Map(oid1 -> MissingObjectUpdate()))
    assert(errs.isEmpty)
  }

  test("VersionBump okay") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = VersionBump(p1, rev1)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1,HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("ExclusiveRevisionLock okay") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = ExclusiveRevisionLock(p1, rev1)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("every requirement on a deleted object (refcount zero) fails with MissingObject") {
    val deleted = ObjectRefcount(2, 0)
    def objects(): HashMap[ObjectId, ObjectState] =
      HashMap(oid1 -> new ObjectState(oid1, Metadata(rev1, deleted, ts1), ObjectType.Data, DataBuffer(new Array[Byte](0))))
    val updates: HashMap[ObjectId, DataBuffer] = HashMap(oid1 -> DataBuffer.Empty)
    def kvObjects(): HashMap[ObjectId, ObjectState] =
      val o = new ObjectState(oid1, Metadata(rev1, deleted, ts1), ObjectType.Data, DataBuffer(new Array[Byte](0)))
      o.kvState = Some(new KVObjectState)
      HashMap(oid1 -> o)
    val kvReq = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](1)))))
    val (kvErrs, kvNonObject) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(kvReq), kvObjects(), updates)
    kvErrs shouldBe Map(oid1 -> MissingObject())
    kvNonObject shouldBe empty

    for req <- List[TransactionRequirement](
           ExclusiveRevisionLock(p1, rev1),
           SharedRevisionLock(p1, rev1),
           VersionBump(p1, rev1),
           DataUpdate(p1, rev1, DataUpdateOperation.Overwrite),
           RefcountUpdate(p1, deleted, ObjectRefcount(3, 1)))
    do
      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects(), updates)
      withClue(req.toString) {
        oerrs shouldBe Map(oid1 -> MissingObject())
        errs shouldBe empty
      }
  }

  private def sharedLockFixture(): Map[ObjectId, ObjectState] =
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data, DataBuffer(new Array[Byte](0)))
    Map(o.objectId -> o)

  private val tx3 = TransactionId(new UUID(0, 7))

  test("SharedRevisionLock okay") {
    val objects = sharedLockFixture()
    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(SharedRevisionLock(p1, rev1)),
      objects, Map())
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("two SharedRevisionLocks on one object both pass") {
    val objects = sharedLockFixture()
    RequirementsLocker.lock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(SharedRevisionLock(p1, rev1)),
      objects, Map())
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  /** Fixed clock for the exclusive preference window, so no test depends on wall-clock timing */
  private val now = 1000000L
  private val window = SharedRevisionLock.ExclusivePreferenceWindow.toNanos

  /** Locks tx1 shared, then checks a tx2 DataUpdate on the same object at time `now` */
  private def collideWithSharedLock(objects: Map[ObjectId, ObjectState]): Map[ObjectId, RequirementError] =
    RequirementsLocker.lock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now,
      List(DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)), objects, Map(oid1 -> DataBuffer.Empty),
      nanoTime = () => now)
    assert(errs.isEmpty)
    oerrs

  private def checkSharedAt(objects: Map[ObjectId, ObjectState], time: Long) =
    RequirementsChecker.check(tx3, HLCTimestamp.now, List(SharedRevisionLock(p1, rev1)), objects, Map(),
      nanoTime = () => time)

  test("DataUpdate collides with a held SharedRevisionLock") {
    val objects = sharedLockFixture()
    val oerrs = collideWithSharedLock(objects)
    assert(oerrs == Map(oid1 -> TransactionCollision(tx1)))
    assert(objects(oid1).exclusiveWaitUntil == now + window)
  }

  // transaction-requirements 8.3: only a writer that fails on nothing but the shared holders opens the window
  test("a writer at a stale revision reports the mismatch and opens no exclusive preference window") {
    val objects = sharedLockFixture()
    RequirementsLocker.lock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now,
      List(DataUpdate(p1, rev2, DataUpdateOperation.Overwrite)), objects, Map(oid1 -> DataBuffer.Empty),
      nanoTime = () => now)
    assert(oerrs == Map(oid1 -> RevisionMismatch()))
    assert(errs.isEmpty)
    assert(objects(oid1).exclusiveWaitUntil == 0)
    val (soerrs, serrs) = checkSharedAt(objects, now + 1)
    assert(soerrs.isEmpty)
    assert(serrs.isEmpty)
  }

  test("a writer that fails on another object opens no exclusive preference window") {
    val objects = sharedLockFixture() ++ Map(oid2 -> mkobjs()._2)
    RequirementsLocker.lock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    val reqs = List(DataUpdate(p1, rev1, DataUpdateOperation.Overwrite), VersionBump(p2, rev1))
    val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, reqs, objects,
      Map(oid1 -> DataBuffer.Empty), nanoTime = () => now)
    assert(oerrs == Map(oid1 -> TransactionCollision(tx1), oid2 -> RevisionMismatch()))
    assert(errs.isEmpty)
    assert(objects(oid1).exclusiveWaitUntil == 0)
  }

  test("a writer that fails a non-object requirement opens no exclusive preference window") {
    val objects = sharedLockFixture()
    RequirementsLocker.lock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    val reqs = List(DataUpdate(p1, rev1, DataUpdateOperation.Overwrite),
      LocalTimeRequirement(HLCTimestamp(1), LocalTimeRequirement.Requirement.LessThan))
    val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, reqs, objects,
      Map(oid1 -> DataBuffer.Empty), nanoTime = () => now)
    assert(oerrs == Map(oid1 -> TransactionCollision(tx1)))
    assert(errs == List(LocalTimeError()))
    assert(objects(oid1).exclusiveWaitUntil == 0)
  }

  test("SharedRevisionLock refused during the exclusive preference window") {
    val objects = sharedLockFixture()
    collideWithSharedLock(objects)
    val (oerrs, errs) = checkSharedAt(objects, now + window - 1)
    assert(oerrs.keySet == Set(oid1))
    assert(oerrs(oid1).isInstanceOf[TransactionCollision])
    assert(errs.isEmpty)
  }

  test("SharedRevisionLock accepted once the exclusive preference window closes") {
    val objects = sharedLockFixture()
    collideWithSharedLock(objects)
    val (oerrs, errs) = checkSharedAt(objects, now + window)
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("a committed transaction's re-check ignores locks held by other transactions") {
    val objects = sharedLockFixture()
    RequirementsLocker.lock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    objects(oid1).lockedToTransaction = Some(tx3)
    val reqs = List(DataUpdate(p1, rev1, DataUpdateOperation.Overwrite))
    val updates = Map(oid1 -> DataBuffer.Empty)

    RequirementsChecker.check(tx2, HLCTimestamp.now, reqs, objects, updates)._1.keySet shouldBe Set(oid1)
    objects(oid1).exclusiveWaitUntil = 0

    val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, reqs, objects, updates, applying = true)
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
    // Applying takes no locks, so it does not hold off shared lockers either
    assert(objects(oid1).exclusiveWaitUntil == 0)
  }

  test("unlock releases only locks held by the unlocking transaction") {
    val objects = sharedLockFixture()
    objects(oid1).lockedToTransaction = Some(tx2)
    RequirementsLocker.unlock(tx1, List(DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)), objects, Map())
    assert(objects(oid1).lockedToTransaction == Some(tx2))
  }

  test("a committing SharedRevisionLock is applied during the exclusive preference window") {
    val objects = sharedLockFixture()
    collideWithSharedLock(objects)
    // Tx unlocks before applying, so the re-check at apply time must not be refused by the window
    RequirementsLocker.unlock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(SharedRevisionLock(p1, rev1)), objects, Map())
    assert(result.uncommittable.isEmpty)
  }

  test("SharedRevisionLock fails on revision mismatch") {
    val objects = sharedLockFixture()
    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(SharedRevisionLock(p1, rev2)),
      objects, Map())
    assert(oerrs == Map(oid1 -> RevisionMismatch()))
    assert(errs.isEmpty)
  }

  test("SharedRevisionLock unlock releases the object for exclusive use") {
    val objects = sharedLockFixture()
    RequirementsLocker.lock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    assert(objects(oid1).lockHolders == Set(tx1))
    RequirementsLocker.unlock(tx1, List(SharedRevisionLock(p1, rev1)), objects, Map())
    assert(objects(oid1).sharedLocks.isEmpty)
    val (oerrs, _) = RequirementsChecker.check(tx2, HLCTimestamp.now,
      List(DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)), objects, Map(oid1 -> DataBuffer.Empty))
    assert(oerrs.isEmpty)
  }

  test("VersionBump failure on tx collision") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx2)

    val req = VersionBump(p1, rev1)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> TransactionCollision(tx2))
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("ExclusiveRevisionLock failure on tx collision") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx2)

    val req = ExclusiveRevisionLock(p1, rev1)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> TransactionCollision(tx2))
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("ExclusiveRevisionLock failure on revision mismatch") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    val req = ExclusiveRevisionLock(p1, rev2)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> RevisionMismatch())
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  // A deleted object has no ObjectState on its stores, so a replay that pins it cannot pass there
  test("a revision requirement on an object absent from the store fails") {
    for req <- List(ExclusiveRevisionLock(p1, rev1), DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)) do
      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), HashMap[ObjectId, ObjectState](),
        HashMap[ObjectId, DataBuffer](oid1 -> DataBuffer.Empty))
      withClue(s"$req: ") {
        oerrs shouldBe Map(oid1 -> MissingObject())
        errs shouldBe empty
      }
  }

  test("VersionBump failure on revision mismatch") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    val req = VersionBump(p1, rev2)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> RevisionMismatch())
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("Refcount okay") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = RefcountUpdate(p1, ref1, ref2)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("DataUpdate no collision when already locked to same transaction") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    o.lockedToTransaction = Some(tx1)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("Missing object update data") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> MissingObjectUpdate())
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("Transaction collision") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    o.lockedToTransaction = Some(tx2)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> TransactionCollision(tx2))
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("Revision mismatch") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = DataUpdate(p1, rev2, DataUpdateOperation.Overwrite)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> RevisionMismatch())
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("Refcount mismatch") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = RefcountUpdate(p1, ref2, ref1)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    expected += (o.objectId -> RefcountMismatch())
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("Localtime GreaterThan holds when the local time is later") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = LocalTimeRequirement(ts1, LocalTimeRequirement.Requirement.GreaterThan)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("Localtime LessThan fails when the local time is later") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = LocalTimeRequirement(ts1, LocalTimeRequirement.Requirement.LessThan)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap
    var expected: HashMap[ObjectId, RequirementError] = new HashMap

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs == List(LocalTimeError()))
  }

  test("Localtime comparisons hold exactly as named against the local time") {
    import LocalTimeRequirement.Requirement.{Equals, GreaterThan, LessThan}

    def holds(req: LocalTimeRequirement.Requirement.Value, ts: Long, now: Long): Boolean =
      try
        RequirementsChecker.checkLocalTime(LocalTimeRequirement(HLCTimestamp(ts), req), HLCTimestamp(now))
        true
      catch case _: Throwable => false

    assert(holds(Equals, 5, 5))
    assert(!holds(Equals, 5, 4))
    assert(!holds(Equals, 5, 6))
    assert(holds(LessThan, 5, 4))
    assert(!holds(LessThan, 5, 5))
    assert(!holds(LessThan, 5, 6))
    assert(holds(GreaterThan, 5, 6))
    assert(!holds(GreaterThan, 5, 5))
    assert(!holds(GreaterThan, 5, 4))
  }

  test("KeyValues checks") {
    val k1 = Key(Array[Byte](1))
    val k2 = Key(Array[Byte](2))
    val k3 = Key(Array[Byte](3))
    val k4 = Key(Array[Byte](4))

    var content: HashMap[Key, ValueState] = new HashMap

    content += (k1 -> new ValueState(Value(Array[Byte]()), rev1, ts1, None))
    // tx2 holds k2's lock. A required object revision locks the whole object, so it collides with that lock for any
    // other transaction: the checks below that are about something else run as tx2
    content += (k2 -> new ValueState(Value(Array[Byte]()), rev1, ts1, Some(tx2)))

    val kvos = new KVObjectState
    kvos.content = content

    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.kvState = Some(kvos)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    {
      val lst = List(KeyRevision(k1, rev1), KeyRevision(k2, rev1))
      val req = KeyValueUpdate(kp1, Some(rev1), Some(FullContentLock(lst)), List())

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val lst = List(KeyRevision(k1, rev1), KeyRevision(k2, rev1))
      val req = KeyValueUpdate(kp1, Some(rev1), Some(FullContentLock(lst)), List())

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> TransactionCollision(tx2))
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val lst = List(KeyRevision(k1, rev2), KeyRevision(k2, rev1))
      val req = KeyValueUpdate(kp1, Some(rev1), Some(FullContentLock(lst)), List())

      // Checked as tx2, which holds k2's lock: for any other transaction the lock collision is reported first
      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> ContentMismatch())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val lst = List(KeyRevision(k1, rev1))
      val req = KeyValueUpdate(kp1, Some(rev1), Some(FullContentLock(lst)), List())

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> ContentMismatch())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val lst = List(KeyRevision(k1, rev2), KeyRevision(k2, rev1), KeyRevision(k3, rev1))
      val req = KeyValueUpdate(kp1, Some(rev1), Some(FullContentLock(lst)), List())

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> ContentMismatch())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.KeyRevision(k1, rev1)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.KeyRevision(k1, rev2)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> RevisionMismatch())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {// content lock prevents regular locking
      kvos.contentLocked = Some(tx2)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.Exists(k1)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> TransactionCollision(tx2))
      assert(oerrs == expected)
      assert(errs.isEmpty)
      kvos.contentLocked = None
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.Exists(k1)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.MayExist(k1)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.MayExist(k4)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.DoesNotExist(k1)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> KeyExistenceError())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.DoesNotExist(k4)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.Exists(k2)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> TransactionCollision(tx2))
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.TimestampEquals(k1, ts1)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    // k1 and k2 hold timestamp ts1. Each comparison holds exactly as named: the key's timestamp against the given one
    def timestampCheck(kreq: KeyValueUpdate.KeyRequirement): Map[ObjectId, RequirementError] =
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(kreq))
      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)
      assert(errs.isEmpty)
      oerrs

    val ts0 = HLCTimestamp(0)
    assert(timestampCheck(KeyValueUpdate.TimestampLessThan(k2, ts2)).isEmpty)
    assert(timestampCheck(KeyValueUpdate.TimestampLessThan(k2, ts1)) == Map(o.objectId -> KeyTimestampError()))
    assert(timestampCheck(KeyValueUpdate.TimestampLessThan(k2, ts0)) == Map(o.objectId -> KeyTimestampError()))
    assert(timestampCheck(KeyValueUpdate.TimestampGreaterThan(k1, ts0)).isEmpty)
    assert(timestampCheck(KeyValueUpdate.TimestampGreaterThan(k1, ts1)) == Map(o.objectId -> KeyTimestampError()))
    assert(timestampCheck(KeyValueUpdate.TimestampGreaterThan(k1, ts2)) == Map(o.objectId -> KeyTimestampError()))
    assert(timestampCheck(KeyValueUpdate.TimestampEquals(k1, ts2)) == Map(o.objectId -> KeyTimestampError()))

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      kvos.min = Some(k1)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
      kvos.min = None
    }

    {
      kvos.max = Some(k3)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
      kvos.max = None
    }

    {
      kvos.min = Some(k1)
      kvos.max = Some(k3)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
      kvos.min = None
      kvos.max = None
    }

    {
      kvos.min = Some(k3)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> WithinRangeError())
      assert(oerrs == expected)
      assert(errs.isEmpty)
      kvos.min = None
    }

    {
      kvos.min = Some(k2)
      kvos.max = Some(k4)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k1, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> WithinRangeError())
      assert(oerrs == expected)
      assert(errs.isEmpty)
      kvos.min = None
      kvos.max = None
    }

    {
      kvos.min = Some(k1)
      kvos.max = Some(k3)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k4, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> WithinRangeError())
      assert(oerrs == expected)
      assert(errs.isEmpty)
      kvos.min = None
      kvos.max = None
    }
  }

  test("unexpected error checking an object requirement fails that object") {
    // A malformed key requirement makes the inner match throw a MatchError. The check cannot
    // be evaluated, so it must be reported as an error against the object rather than ignored.
    val kvos = new KVObjectState

    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.kvState = Some(kvos)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val req = KeyValueUpdate(kp1, Some(rev1), None, List(null))

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    var expected: HashMap[ObjectId, RequirementError] = new HashMap
    expected += (o.objectId -> RequirementCheckFailure())
    assert(oerrs == expected)
    assert(errs.isEmpty)
  }

  test("unexpected error checking a non-object requirement fails the transaction") {
    // A LocalTimeRequirement with no valid comparison makes checkLocalTime throw a MatchError.
    // There is no object to attribute it to, so it becomes a non-object error.
    val (o1, _) = mkobjs()

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    val updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o1.objectId -> o1)

    val req = LocalTimeRequirement(ts1, null)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs == List(RequirementCheckFailure()))
  }

  // --- Max slice size ---------------------------------------------------------------------------

  private def checkSized(op: DataUpdateOperation.Value, current: DataBuffer, payload: DataBuffer,
                         maxSliceSize: Option[Long]) =
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data, current)
    val req = DataUpdate(p1, rev1, op)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), HashMap(oid1 -> o), HashMap(oid1 -> payload),
      maxSliceSize)

  test("Overwrite exactly at the max slice size passes") {
    val (oerrs, errs) = checkSized(DataUpdateOperation.Overwrite, DataBuffer(new Array[Byte](0)),
      DataBuffer(new Array[Byte](100)), Some(100))
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("Overwrite one byte over the max slice size fails with SliceSizeExceeded") {
    val (oerrs, errs) = checkSized(DataUpdateOperation.Overwrite, DataBuffer(new Array[Byte](0)),
      DataBuffer(new Array[Byte](101)), Some(100))
    assert(oerrs == Map(oid1 -> SliceSizeExceeded(101, 100)))
    assert(errs.isEmpty)
  }

  test("shrinking Overwrite of an object already above the max slice size passes") {
    val (oerrs, errs) = checkSized(DataUpdateOperation.Overwrite, DataBuffer(new Array[Byte](200)),
      DataBuffer(new Array[Byte](150)), Some(100))
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("growing Overwrite of an object already above the max slice size fails") {
    val (oerrs, errs) = checkSized(DataUpdateOperation.Overwrite, DataBuffer(new Array[Byte](200)),
      DataBuffer(new Array[Byte](201)), Some(100))
    assert(oerrs == Map(oid1 -> SliceSizeExceeded(201, 100)))
    assert(errs.isEmpty)
  }

  test("Append whose new slice length crosses the max slice size fails with SliceSizeExceeded") {
    val (slice, delta) = appendFixture(Array.tabulate[Byte](20)(_.toByte), Array.tabulate[Byte](30)(_.toByte))
    assert(delta.newSliceLength > slice.size)
    val atLimit = checkSized(DataUpdateOperation.Append, slice, delta.encode, Some(delta.newSliceLength.toLong))
    assert(atLimit._1.isEmpty)
    assert(atLimit._2.isEmpty)
    val (oerrs, errs) = checkSized(DataUpdateOperation.Append, slice, delta.encode,
      Some(delta.newSliceLength.toLong - 1))
    assert(oerrs == Map(oid1 -> SliceSizeExceeded(delta.newSliceLength, delta.newSliceLength - 1)))
    assert(errs.isEmpty)
  }

  test("WriteRange whose new slice length crosses the max slice size fails with SliceSizeExceeded") {
    val slice = DataBuffer(Array.tabulate[Byte](16)(_.toByte))
    val delta = AppendDelta(16, 20, List(XorRegion(16, Array[Byte](1, 2, 3, 4))), None)
    val atLimit = checkSized(DataUpdateOperation.WriteRange, slice, delta.encode, Some(20))
    assert(atLimit._1.isEmpty)
    assert(atLimit._2.isEmpty)
    val (oerrs, errs) = checkSized(DataUpdateOperation.WriteRange, slice, delta.encode, Some(19))
    assert(oerrs == Map(oid1 -> SliceSizeExceeded(20, 19)))
    assert(errs.isEmpty)
  }

  test("WriteRange inside an object already above the max slice size passes") {
    val slice = DataBuffer(Array.tabulate[Byte](16)(_.toByte))
    val delta = AppendDelta(16, 16, List(XorRegion(2, Array[Byte](1, 2))), None)
    val (oerrs, errs) = checkSized(DataUpdateOperation.WriteRange, slice, delta.encode, Some(8))
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  /** A KeyRevision for each key `ops` delete, as a Delete needs (refuseBlindWrites) */
  private def deleteRevisions(kvos: KVObjectState, ops: List[KeyValueOperation]): List[KeyValueUpdate.KeyRequirement] =
    ops.collect { case d: Delete => KeyRevision(d.key, kvos.content(d.key).revision) }

  private def checkKVSized(kvos: KVObjectState, ops: List[KeyValueOperation], maxSliceSize: Option[Long]) =
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.KeyValue, kvos.encode())
    o.kvState = Some(kvos)
    val req = KeyValueUpdate(kp1, Some(rev1), None, deleteRevisions(kvos, ops),
      deletesKeys = ops.exists(_.isInstanceOf[Delete]))
    val payload = KeyValueOperation.encode(ops, Replication(3, 2))(0)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), HashMap(oid1 -> o), HashMap(oid1 -> payload),
      maxSliceSize)

  test("KeyValue insert that pushes the serialized size over the max slice size fails") {
    val k = Key(Array[Byte](1))
    val v = Array.fill[Byte](10)(7)
    // Empty state: mask byte + 1-byte count. Entry: 16-byte revision + 8-byte timestamp
    // + (1 + 1) key + (1 + 10) value.
    val newSize = 2 + 24 + 2 + 11
    assert(KVObjectState.encodedSize(None, None, None, None,
      Map(k -> new ValueState(Value(v), rev1, ts1, None))) == newSize)

    val atLimit = checkKVSized(new KVObjectState, List(Insert(k, v)), Some(newSize.toLong))
    assert(atLimit._1.isEmpty)
    assert(atLimit._2.isEmpty)

    val (oerrs, errs) = checkKVSized(new KVObjectState, List(Insert(k, v)), Some(newSize.toLong - 1))
    assert(oerrs == Map(oid1 -> SliceSizeExceeded(newSize, newSize - 1)))
    assert(errs.isEmpty)
  }

  test("KeyValue delete from an object already above the max slice size passes") {
    val kvos = new KVObjectState
    kvos.content = Map(
      Key(Array[Byte](1)) -> new ValueState(Value(Array.fill[Byte](50)(1)), rev1, ts1, None),
      Key(Array[Byte](2)) -> new ValueState(Value(Array.fill[Byte](50)(2)), rev1, ts1, None))
    val (oerrs, errs) = checkKVSized(kvos, List(new Delete(Key(Array[Byte](1)))), Some(10))
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
  }

  test("maxSliceSize None never fails on size") {
    val big = DataBuffer(new Array[Byte](1 << 20))
    val ow = checkSized(DataUpdateOperation.Overwrite, DataBuffer(new Array[Byte](0)), big, None)
    assert(ow._1.isEmpty)
    assert(ow._2.isEmpty)

    val (slice, delta) = appendFixture(Array.tabulate[Byte](20)(_.toByte), new Array[Byte](10000))
    val ap = checkSized(DataUpdateOperation.Append, slice, delta.encode, None)
    assert(ap._1.isEmpty)
    assert(ap._2.isEmpty)

    val kv = checkKVSized(new KVObjectState, List(Insert(Key(Array[Byte](1)), new Array[Byte](10000))), None)
    assert(kv._1.isEmpty)
    assert(kv._2.isEmpty)
  }


  /** checkKVSized with `reserved` held on the object (ObjectState.sliceReservations) by other commit votes */
  private def checkKVReserved(kvos: KVObjectState, ops: List[KeyValueOperation], reserved: Map[TransactionId, SliceGrowth],
                              limit: Long, applying: Boolean = false) =
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.KeyValue, kvos.encode())
    o.kvState = Some(kvos)
    o.sliceReservations = reserved
    val req = KeyValueUpdate(kp1, Some(rev1), None, deleteRevisions(kvos, ops),
      deletesKeys = ops.exists(_.isInstanceOf[Delete]))
    val payload = KeyValueOperation.encode(ops, Replication(3, 2))(0)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), HashMap(oid1 -> o), HashMap(oid1 -> payload),
      Some(limit), applying = applying)._1

  private def kvWith(n: Int, valueSize: Int = 1): KVObjectState =
    val kvos = new KVObjectState
    kvos.content = (0 until n).map(i => Key(BigInt(i + 1000).toByteArray) ->
      new ValueState(Value(new Array[Byte](valueSize)), rev1, ts1, None)).toMap
    kvos

  test("A growing KeyValue update counts the growth other commit votes hold reserved, at vote and at apply") {
    val kvos = kvWith(1)
    val ins = List(Insert(Key(Array[Byte](1)), new Array[Byte](10)))
    val alone = kvos.encodedSizeAfter(ins, ObjectRevision(tx1), HLCTimestamp.now).toLong
    val other = Map(tx2 -> SliceGrowth(37, 1))
    checkKVReserved(kvos, ins, Map(), alone) shouldBe empty
    checkKVReserved(kvos, ins, other, alone + 37) shouldBe empty
    checkKVReserved(kvos, ins, other, alone + 36) shouldBe Map(oid1 -> SliceSizeExceeded(alone + 37, alone + 36))
    checkKVReserved(kvos, ins, other, alone + 36, applying = true) shouldBe
      Map(oid1 -> SliceSizeExceeded(alone + 37, alone + 36))
    // A transaction's own reservation is not counted twice
    checkKVReserved(kvos, ins, Map(tx1 -> SliceGrowth(37, 1)), alone) shouldBe empty
    // Shrinking reservations lower nothing: they may not apply first
    checkKVReserved(kvos, ins, Map(tx2 -> SliceGrowth(-100, -1)), alone - 1) shouldBe
      Map(oid1 -> SliceSizeExceeded(alone, alone - 1))
  }

  test("The reserved entries count toward the entry-count varint") {
    // 126 entries: a one-byte count. This insert and a reserved one make 128, a two-byte count
    val kvos = kvWith(126)
    val ins = List(Insert(Key(Array[Byte](1)), new Array[Byte](10)))
    val alone = kvos.encodedSizeAfter(ins, ObjectRevision(tx1), HLCTimestamp.now).toLong
    val other = Map(tx2 -> SliceGrowth(37, 1))
    checkKVReserved(kvos, ins, other, alone + 38) shouldBe empty
    checkKVReserved(kvos, ins, other, alone + 37) shouldBe Map(oid1 -> SliceSizeExceeded(alone + 38, alone + 37))
  }

  test("A KeyValue update that grows nothing passes beside reservations past the limit") {
    val kvos = kvWith(3, valueSize = 50)
    val del = List(new Delete(Key(BigInt(1000).toByteArray)))
    checkKVReserved(kvos, del, Map(tx2 -> SliceGrowth(500, 5)), 10) shouldBe empty
    // A smaller value for an existing key
    val smaller = List(Insert(Key(BigInt(1001).toByteArray), new Array[Byte](10)))
    checkKVReserved(kvos, smaller, Map(tx2 -> SliceGrowth(500, 5)), 10) shouldBe empty
  }

  test("KVObjectState.growth plus the count varint change is encodedSizeAfter's change") {
    val kvos = kvWith(127, valueSize = 5)
    kvos.max = Some(Key(Array[Byte](9)))
    val ts = HLCTimestamp.now
    val cases = List(
      List(Insert(Key(Array[Byte](1)), new Array[Byte](10))),
      List(new Delete(Key(BigInt(1000).toByteArray))),
      List(Insert(Key(BigInt(1001).toByteArray), new Array[Byte](300)), new Delete(Key(BigInt(1002).toByteArray)),
        Insert(Key(Array[Byte](2)), new Array[Byte](1)), SetMax(Key(Array[Byte](1, 2, 3))), SetRight(new Array[Byte](4))),
      List(Insert(Key(Array[Byte](3)), new Array[Byte](1)), new Delete(Key(Array[Byte](3)))))
    for ops <- cases do
      val g = kvos.growth(ops, ObjectRevision(tx1), ts)
      val count = kvos.content.size
      val after = kvos.encodedSizeAfter(ops, ObjectRevision(tx1), ts)
      val varint = (n: Int) => org.aspen_ddp.aspen.common.util.Varint.getUnsignedIntEncodingLength(n)
      withClue(ops.toString) {
        (kvos.encodedSize + g.bytes + varint(count + g.entries) - varint(count)) shouldBe after
      }
  }

  test("A commit vote reserves its slice growth with its locks and releases it with them") {
    val kvos = kvWith(1)
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.KeyValue, kvos.encode())
    o.kvState = Some(kvos)
    val key = Key(Array[Byte](1))
    val req = KeyValueUpdate(kp1, None, None, List(KeyValueUpdate.DoesNotExist(key)))
    val ops = List(Insert(key, new Array[Byte](10)))
    val payload = KeyValueOperation.encode(ops, Replication(3, 2))(0)
    val ts = HLCTimestamp.now
    val objects = HashMap(oid1 -> o)
    val growth = RequirementsChecker.sliceGrowth(tx1, ts, List(req), objects, HashMap(oid1 -> payload))
    growth shouldBe Map(oid1 -> kvos.growth(ops, ObjectRevision(tx1), ts))
    growth(oid1).entries shouldBe 1
    val writes = KVWrites.from(List(req), Map(oid1 -> payload))
    RequirementsLocker.lock(tx1, List(req), objects, writes, growth = growth)
    o.sliceReservations shouldBe Map(tx1 -> growth(oid1))
    RequirementsLocker.unlock(tx1, List(req), objects, writes)
    o.sliceReservations shouldBe empty
  }

  private def emptyKVObject(): Map[ObjectId, ObjectState] =
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.KeyValue, DataBuffer(new Array[Byte](0)))
    o.kvState = Some(new KVObjectState)
    Map(o.objectId -> o)

  test("a transaction's own KV locks do not collide with its re-check") {
    val objects = emptyKVObject()
    val k = Key(Array[Byte](1))
    val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.DoesNotExist(k),
      KeyValueUpdate.WithinRange(k, ByteArrayKeyOrdering)))
    val updates = Map(oid1 -> DataBuffer.Empty)

    RequirementsLocker.lock(tx1, List(req), objects, Map())

    RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates) shouldBe (Map(), Nil)
    RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)._1 shouldBe
      Map(oid1 -> TransactionCollision(tx1))

    RequirementsLocker.unlock(tx1, List(req), objects, Map())
    RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates) shouldBe (Map(), Nil)
  }

  test("a full content lock on an empty object collides with another transaction's content lock") {
    val objects = emptyKVObject()
    val req = KeyValueUpdate(kp1, None, Some(FullContentLock(Nil)), Nil)
    val updates = Map(oid1 -> DataBuffer.Empty)

    RequirementsLocker.lock(tx1, List(req), objects, Map())

    RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)._1 shouldBe
      Map(oid1 -> TransactionCollision(tx1))
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates) shouldBe (Map(), Nil)
  }

  // ---- Object-level requirements and key-level locks collide in both arrival orders ----

  private val kk1 = Key(Array[Byte](1))
  private val kk9 = Key(Array[Byte](9))

  /** A KeyValue object at rev1 holding kk1 at rev1 */
  private def kvObjectWithKey(): Map[ObjectId, ObjectState] =
    val objects = emptyKVObject()
    objects(oid1).kvState.get.content += (kk1 -> new ValueState(Value(Array[Byte]()), rev1, ts1, None))
    objects

  private val keyLevel = KeyValueUpdate(kp1, None, None, List(KeyRevision(kk1, rev1)))
  private val absentKey = KeyValueUpdate(kp1, None, None, List(KeyValueUpdate.DoesNotExist(kk9)))
  private val objectRevision = KeyValueUpdate(kp1, Some(rev1), None, Nil)
  private val keyObjectRevision = KeyValueUpdate(kp1, None, None, List(KeyValueUpdate.KeyObjectRevision(kk9, rev1)))
  private val contentLock = KeyValueUpdate(kp1, None, Some(FullContentLock(List(KeyRevision(kk1, rev1)))), Nil)
  private val refcount = RefcountUpdate(kp1, ref1, ref2)
  private val versionBump = VersionBump(kp1, rev1)
  private val revisionLock = ExclusiveRevisionLock(kp1, rev1)
  private val shared = SharedRevisionLock(kp1, rev1)

  /** tx1 locks `first`, then tx2 is checked with `second`. Returns tx2's errors */
  private def lockThenCheck(first: TransactionRequirement, second: TransactionRequirement) =
    val objects = kvObjectWithKey()
    val updates = Map(oid1 -> DataBuffer.Empty)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(first), objects, updates, nanoTime = () => 0L) shouldBe
      (Map(), Nil)
    RequirementsLocker.lock(tx1, List(first), objects, Map())
    RequirementsChecker.check(tx2, HLCTimestamp.now, List(second), objects, updates, nanoTime = () => 0L)._1

  private val conflicting: List[(String, TransactionRequirement, TransactionRequirement)] = List(
    ("a required object revision", objectRevision, keyLevel),
    ("a required object revision", objectRevision, absentKey),
    ("a key's object revision requirement", keyObjectRevision, keyLevel),
    ("a full content lock", contentLock, keyLevel),
    ("a full content lock", contentLock, absentKey),
    ("a refcount update", refcount, keyLevel),
    ("a version bump", versionBump, keyLevel),
    ("a revision lock", revisionLock, absentKey),
    ("a SharedRevisionLock", shared, keyLevel),
    ("a SharedRevisionLock", shared, absentKey))

  for ((name, objectLevel, keyReq), i) <- conflicting.zipWithIndex do
    test(s"$name and a held key lock collide in both arrival orders ($i)") {
      lockThenCheck(objectLevel, keyReq) shouldBe Map(oid1 -> TransactionCollision(tx1))
      lockThenCheck(keyReq, objectLevel) shouldBe Map(oid1 -> TransactionCollision(tx1))
    }

  test("key-level requirements on different keys do not collide") {
    lockThenCheck(keyLevel, absentKey) shouldBe Map()
    lockThenCheck(absentKey, keyLevel) shouldBe Map()
  }

  test("a stale object state fails every check, also when applying") {
    val objects = kvObjectWithKey()
    objects(oid1).reloadRequired = true
    for applying <- List(false, true) do
      RequirementsChecker.check(tx1, HLCTimestamp.now, List(keyLevel), objects, Map(oid1 -> DataBuffer.Empty),
        applying = applying)._1 shouldBe Map(oid1 -> StaleObjectState())
      RequirementsChecker.check(tx1, HLCTimestamp.now, List(shared), objects, Map(), applying = applying)._1 shouldBe
        Map(oid1 -> StaleObjectState())
  }

  // ---- A KeyValue update's operations lock what they write ----

  private def kvData(ops: List[KeyValueOperation]): DataBuffer = KeyValueOperation.encode(ops, Replication(3, 2))(0)

  /** A KeyValueUpdate of oid1 with no requirements beyond `reqs`, and its object data */
  private case class KVTx(txid: TransactionId, reqs: List[KeyValueUpdate.KeyRequirement], ops: List[KeyValueOperation],
                          requiredRevision: Option[ObjectRevision] = None):
    val requirements: List[TransactionRequirement] = List(KeyValueUpdate(kp1, requiredRevision, None, reqs,
      deletesKeys = ops.exists(_.isInstanceOf[Delete])))
    val updates: Map[ObjectId, DataBuffer] = Map(oid1 -> kvData(ops))
    def writes: Map[ObjectId, KVWrites] = KVWrites.from(requirements, updates)
    def check(objects: Map[ObjectId, ObjectState], applying: Boolean = false): Map[ObjectId, RequirementError] =
      RequirementsChecker.check(txid, HLCTimestamp.now, requirements, objects, updates, applying = applying,
        nanoTime = () => 0L)._1
    def lock(objects: Map[ObjectId, ObjectState]): Unit = RequirementsLocker.lock(txid, requirements, objects, writes)
    def unlock(objects: Map[ObjectId, ObjectState]): Unit =
      RequirementsLocker.unlock(txid, requirements, objects, writes)

  /** `first` passes and locks, then `second` is checked. Returns second's errors */
  private def lockThenCheckKV(objects: Map[ObjectId, ObjectState], first: KVTx, second: KVTx) =
    first.check(objects) shouldBe Map()
    first.lock(objects)
    second.check(objects)

  private def insert(key: Key) = Insert(key, Array[Byte](7))

  // Key-level inserts (no required revision), each checking its key as an Insert must (refuseBlindWrites)
  private val insertPresent = (t: TransactionId) => KVTx(t, List(KeyValueUpdate.Exists(kk1)), List(insert(kk1)))
  private val insertAbsent = (t: TransactionId) => KVTx(t, List(KeyValueUpdate.DoesNotExist(kk9)), List(insert(kk9)))

  test("two MayExist requirements on an absent key collide") {
    val may = (t: TransactionId) => KVTx(t, List(KeyValueUpdate.MayExist(kk9)), Nil)
    lockThenCheckKV(emptyKVObject(), may(tx1), may(tx2)) shouldBe Map(oid1 -> TransactionCollision(tx1))
  }

  test("two key-level inserts of one key collide, absent or present") {
    lockThenCheckKV(emptyKVObject(), insertAbsent(tx1), insertAbsent(tx2)) shouldBe
      Map(oid1 -> TransactionCollision(tx1))
    lockThenCheckKV(kvObjectWithKey(), insertPresent(tx1), insertPresent(tx2)) shouldBe
      Map(oid1 -> TransactionCollision(tx1))
  }

  test("key-level inserts of different keys do not collide") {
    lockThenCheckKV(kvObjectWithKey(), insertPresent(tx1), insertAbsent(tx2)) shouldBe Map()
    lockThenCheckKV(kvObjectWithKey(), insertAbsent(tx1), insertPresent(tx2)) shouldBe Map()
  }

  test("a key-level insert and key requirements on its key collide in both arrival orders") {
    val pairs = List(
      (() => kvObjectWithKey(), KVTx(_, List(KeyRevision(kk1, rev1)), Nil), insertPresent),
      (() => kvObjectWithKey(), KVTx(_, List(KeyValueUpdate.Exists(kk1)), Nil), insertPresent),
      (() => emptyKVObject(), KVTx(_, List(KeyValueUpdate.DoesNotExist(kk9)), Nil), insertAbsent))
    for (mk, req, write) <- pairs do
      lockThenCheckKV(mk(), req(tx1), write(tx2)) shouldBe Map(oid1 -> TransactionCollision(tx1))
      lockThenCheckKV(mk(), write(tx1), req(tx2)) shouldBe Map(oid1 -> TransactionCollision(tx1))
  }

  test("a vote's planned locks collide exactly as the locks it takes (Tx.lockWaitBlocked)") {
    val kinds: List[TransactionId => KVTx] = List(
      insertPresent, insertAbsent,
      KVTx(_, List(KeyRevision(kk1, rev1)), Nil),
      KVTx(_, List(KeyRevision(kk9, rev1)), Nil),
      KVTx(_, List(KeyValueUpdate.Exists(kk1)), Nil),
      KVTx(_, List(KeyValueUpdate.MayExist(kk1)), Nil),
      KVTx(_, List(KeyValueUpdate.MayExist(kk9)), Nil),
      KVTx(_, List(KeyValueUpdate.DoesNotExist(kk9)), Nil),
      KVTx(_, List(KeyValueUpdate.WithinRange(kk9, ByteArrayKeyOrdering)), Nil),
      KVTx(_, List(KeyValueUpdate.KeyObjectRevision(kk9, rev1)), Nil),
      KVTx(_, List(KeyRevision(kk1, rev1)), List(Delete(kk1))),
      KVTx(_, Nil, Nil, requiredRevision = Some(rev1)))
    val collided = for holderKind <- kinds; waiterKind <- kinds yield
      val (holder, waiter) = (holderKind(tx1), waiterKind(tx2))
      val objects = kvObjectWithKey()
      val planned = RequirementsLocker.plannedLocks(holder.requirements, objects, holder.writes).map(_._2)
      val view = RequirementsChecker.PlannedLocks(tx1, objects(oid1), planned)
      val fromPlan = RequirementsChecker.lockHoldersIn(tx2, waiter.requirements, waiter.writes,
        o => if o == oid1 then Some(view) else None)
      holder.lock(objects)
      withClue(s"holder $holder, waiter $waiter: ") {
        fromPlan shouldBe RequirementsChecker.lockHolders(tx2, waiter.requirements, objects, waiter.writes, Set(oid1))
      }
      fromPlan.nonEmpty
    collided.toSet shouldBe Set(true, false)
  }

  test("a key Delete without deletesKeys is rejected") {
    val (objErrors, _) = RequirementsChecker.check(tx1, HLCTimestamp.now,
      List(KeyValueUpdate(kp1, None, None, Nil, deletesKeys = false)), kvObjectWithKey(),
      Map(oid1 -> kvData(List(Delete(kk1)))))
    objErrors(oid1) shouldBe UnflaggedDelete()
  }

  test("a flagged key Delete passes") {
    val (objErrors, _) = RequirementsChecker.check(tx1, HLCTimestamp.now,
      List(KeyValueUpdate(kp1, None, None, List(KeyRevision(kk1, rev1)), deletesKeys = true)), kvObjectWithKey(),
      Map(oid1 -> kvData(List(Delete(kk1)))))
    objErrors shouldBe empty
  }

  test("a key Delete collides with a lock on its key") {
    val delete = KVTx(tx2, List(KeyRevision(kk1, rev1)), List(Delete(kk1)))
    lockThenCheckKV(kvObjectWithKey(), KVTx(tx1, List(KeyValueUpdate.Exists(kk1)), Nil), delete) shouldBe
      Map(oid1 -> TransactionCollision(tx1))
  }

  test("an object-level operation locks the whole object") {
    val relink = (t: TransactionId) => KVTx(t, Nil, List(SetRight(Array[Byte](1))), requiredRevision = Some(rev1))
    val withinRange = (t: TransactionId) => KVTx(t, List(KeyValueUpdate.WithinRange(kk9, ByteArrayKeyOrdering)), Nil)
    for other <- List(withinRange, insertAbsent, insertPresent) do
      lockThenCheckKV(kvObjectWithKey(), relink(tx1), other(tx2)) shouldBe Map(oid1 -> TransactionCollision(tx1))
      lockThenCheckKV(kvObjectWithKey(), other(tx1), relink(tx2)) shouldBe Map(oid1 -> TransactionCollision(tx1))
  }

  // repair-process 1.2 (Q8): a blind Delete leaves no trace for a repair snapshot, and a blind object-level change does
  // not move the revision a repair proof locks. The store refuses them, as TransactionBuilder does
  test("a key Delete with no KeyRevision on its key and no content lock is refused") {
    KVTx(tx1, Nil, List(Delete(kk1))).check(kvObjectWithKey()) shouldBe Map(oid1 -> BlindWrite())
    // A KeyRevision on another key does not cover it
    KVTx(tx1, List(KeyRevision(kk9, rev1)), List(Delete(kk1))).check(kvObjectWithKey()) shouldBe
      Map(oid1 -> BlindWrite())
    // Nor does the object's required revision: an Insert of the key moves no revision, so a Delete applied after one
    // would pass its re-check and remove the key unreported (transaction-requirements 9.3)
    KVTx(tx1, Nil, List(Delete(kk1)), requiredRevision = Some(rev1)).check(kvObjectWithKey()) shouldBe
      Map(oid1 -> BlindWrite())
    KVTx(tx1, Nil, List(Delete(kk1)), requiredRevision = Some(rev1)).check(kvObjectWithKey(), applying = true) shouldBe
      Map(oid1 -> BlindWrite())
    // Refused at the apply re-check too
    KVTx(tx1, Nil, List(Delete(kk1))).check(kvObjectWithKey(), applying = true) shouldBe Map(oid1 -> BlindWrite())
  }

  test("an Insert with no requirement that checks its key, and no required revision or content lock, is refused") {
    KVTx(tx1, Nil, List(insert(kk9))).check(kvObjectWithKey()) shouldBe Map(oid1 -> BlindWrite())
    // MayExist, WithinRange and KeyObjectRevision check nothing of the key's own state, nor does another key's check
    for req <- List(KeyValueUpdate.MayExist(kk9), KeyValueUpdate.WithinRange(kk9, ByteArrayKeyOrdering),
                    KeyValueUpdate.KeyObjectRevision(kk9, rev1), KeyRevision(kk1, rev1)) do
      KVTx(tx1, List(req), List(insert(kk9))).check(kvObjectWithKey()) shouldBe Map(oid1 -> BlindWrite())
    // Refused at the apply re-check too
    KVTx(tx1, Nil, List(insert(kk9))).check(kvObjectWithKey(), applying = true) shouldBe Map(oid1 -> BlindWrite())
  }

  test("an Insert passes with a check on its key, a required revision or a content lock") {
    for req <- List(KeyValueUpdate.DoesNotExist(kk9)) do
      KVTx(tx1, List(req), List(insert(kk9))).check(kvObjectWithKey()) shouldBe Map()
    for req <- List(KeyRevision(kk1, rev1), KeyValueUpdate.Exists(kk1)) do
      KVTx(tx1, List(req), List(insert(kk1))).check(kvObjectWithKey()) shouldBe Map()
    KVTx(tx1, Nil, List(insert(kk9)), requiredRevision = Some(rev1)).check(kvObjectWithKey()) shouldBe Map()
    val locked = KeyValueUpdate(kp1, None, Some(KeyValueUpdate.FullContentLock(List(KeyRevision(kk1, rev1)))), Nil)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(locked), kvObjectWithKey(),
      Map(oid1 -> kvData(List(insert(kk9)))), nanoTime = () => 0L)._1 shouldBe Map()
  }

  test("an object-level operation with no required revision is refused, even under a content lock") {
    KVTx(tx1, Nil, List(SetRight(Array[Byte](1)))).check(kvObjectWithKey()) shouldBe Map(oid1 -> BlindWrite())
    KVTx(tx1, Nil, List(SetMax(kk9))).check(kvObjectWithKey()) shouldBe Map(oid1 -> BlindWrite())
    // A content lock does not move the revision a repair proof locks
    val locked = KeyValueUpdate(kp1, None, Some(KeyValueUpdate.FullContentLock(List(KeyRevision(kk1, rev1)))), Nil)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(locked), kvObjectWithKey(),
      Map(oid1 -> kvData(List(SetMax(kk9)))), nanoTime = () => 0L)._1 shouldBe Map(oid1 -> BlindWrite())
  }

  test("object-level operations pass with a required revision, deletes with their key's KeyRevision or a content " +
       "lock, and both in an allocation") {
    val ops = List(Delete(kk1), SetMax(kk9), SetRight(Array[Byte](1)))
    KVTx(tx1, List(KeyRevision(kk1, rev1)), ops, requiredRevision = Some(rev1)).check(kvObjectWithKey()) shouldBe Map()

    val locked = KeyValueUpdate(kp1, None, Some(KeyValueUpdate.FullContentLock(List(KeyRevision(kk1, rev1)))), Nil,
      deletesKeys = true)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(locked), kvObjectWithKey(),
      Map(oid1 -> kvData(List(Delete(kk1)))), nanoTime = () => 0L)._1 shouldBe Map()

    // The initial content of an object the transaction allocates
    val initial = KeyValueUpdate(kp1, None, None, Nil, deletesKeys = true)
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(initial), kvObjectWithKey(), Map(oid1 -> kvData(ops)),
      nanoTime = () => 0L, allocating = Set(oid1))._1 shouldBe Map()
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(initial), kvObjectWithKey(), Map(oid1 -> kvData(ops)),
      nanoTime = () => 0L)._1 shouldBe Map(oid1 -> BlindWrite())
  }

  test("applying a committed transaction ignores other transactions' write locks") {
    val objects = kvObjectWithKey()
    insertPresent(tx1).lock(objects)
    insertAbsent(tx1).lock(objects)
    insertPresent(tx2).check(objects, applying = true) shouldBe Map()
    insertAbsent(tx2).check(objects, applying = true) shouldBe Map()
  }

  test("unlock releases write locks after the commit inserted or deleted the key") {
    val objects = kvObjectWithKey()
    val tx = KVTx(tx1, List(KeyValueUpdate.DoesNotExist(kk9), KeyRevision(kk1, rev1)), List(Delete(kk1), insert(kk9)))
    tx.check(objects) shouldBe Map()
    tx.lock(objects)
    objects(oid1).lockedWriteTransactions shouldBe Set(tx1)

    val kvs = objects(oid1).kvState.get
    kvs.content = kvs.content - kk1 + (kk9 -> new ValueState(Value(Array[Byte](7)), ObjectRevision(tx1), ts1, None))
    tx.unlock(objects)
    objects(oid1).lockedWriteTransactions shouldBe Set()
  }

  test("unlock leaves another transaction's lock on a written key") {
    val objects = emptyKVObject()
    insertAbsent(tx1).lock(objects)
    insertAbsent(tx2).unlock(objects)
    objects(oid1).lockedWriteTransactions shouldBe Set(tx1)
  }

  test("every requirement on an allocated object the store already holds fails with ObjectExists (Req F11)") {
    val (o1, o2) = mkobjs()
    // Refcount and revision as a never-revised allocation leaves them: the requirements alone would pass
    o1.metadata = Metadata(ObjectRevision.Allocating, ObjectRefcount.Allocating, ts1)
    val reqs = List(
      RefcountUpdate(p1, ObjectRefcount.Allocating, ObjectRefcount.Default),
      DataUpdate(p1, ObjectRevision.Allocating, DataUpdateOperation.Overwrite),
      DataUpdate(p2, rev2, DataUpdateOperation.Overwrite))
    val objects = HashMap(oid1 -> o1, oid2 -> o2)
    val updates = HashMap(oid1 -> DataBuffer(Array[Byte](1)), oid2 -> DataBuffer(Array[Byte](2)))
    RequirementsChecker.check(tx1, HLCTimestamp.now, reqs, objects, updates)._1 shouldBe empty
    RequirementsChecker.check(tx1, HLCTimestamp.now, reqs, objects, updates, allocatedOver = Set(oid1))._1 shouldBe
      Map(oid1 -> ObjectExists())

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, reqs, objects, updates, allocatedOver = Set(oid1))
    result.uncommittable shouldBe Set(oid1)
    o1.metadata.revision shouldBe ObjectRevision.Allocating
    o2.metadata.revision shouldBe ObjectRevision(tx1)
  }

  test("an allocated object the store holds at the transaction's own revision counts as already applied") {
    val (o1, _) = mkobjs()
    o1.metadata = Metadata(ObjectRevision(tx1), ObjectRefcount.Default, ts1)
    val reqs = List(RefcountUpdate(kp1, ObjectRefcount.Allocating, ObjectRefcount.Allocating))
    RequirementsApplyer.appliedBy(tx1, reqs, HashMap(oid1 -> o1)) shouldBe empty
    RequirementsApplyer.appliedBy(tx1, reqs, HashMap(oid1 -> o1), allocatedOver = Set(oid1)) shouldBe Set(oid1)
    o1.metadata = Metadata(rev1, ObjectRefcount.Default, ts1)
    RequirementsApplyer.appliedBy(tx1, reqs, HashMap(oid1 -> o1), allocatedOver = Set(oid1)) shouldBe empty
  }
}
