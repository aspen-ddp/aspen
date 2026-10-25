package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.ida.{AppendDelta, RandomAccessReedSolomon, Replication, XorRegion}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Delete, Insert, Key, KeyValueOperation, KeyValueObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{FullContentLock, KeyRevision}
import org.aspen_ddp.aspen.common.transaction.{ContentMismatch, DataUpdate, DataUpdateOperation, KeyExistenceError, KeyValueUpdate, LocalTimeError, LocalTimeRequirement, MissingObjectUpdate, RefcountMismatch, RefcountUpdate, RequirementCheckFailure, RequirementError, RevisionLock, RevisionMismatch, SliceSizeExceeded, TransactionCollision, TransactionId, AppendMismatch, VersionBump, WithinRangeError}
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

  test("RevisionLock okay") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx1)

    val req = RevisionLock(p1, rev1)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer.Empty)

    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(oerrs.isEmpty)
    assert(errs.isEmpty)
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

  test("RevisionLock failure on tx collision") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.lockedToTransaction = Some(tx2)

    val req = RevisionLock(p1, rev1)

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

  test("RevisionLock failure on revision mismatch") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    val req = RevisionLock(p1, rev2)

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

  test("Localtime okay") {
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
    assert(errs == List(LocalTimeError()))
  }

  test("Localtime error") {
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
    assert(errs.isEmpty)
  }

  test("KeyValues checks") {
    val k1 = Key(Array[Byte](1))
    val k2 = Key(Array[Byte](2))
    val k3 = Key(Array[Byte](3))
    val k4 = Key(Array[Byte](4))

    var content: HashMap[Key, ValueState] = new HashMap

    content += (k1 -> new ValueState(Value(Array[Byte]()), rev1, ts1, None))
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

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> ContentMismatch())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val lst = List(KeyRevision(k1, rev1))
      val req = KeyValueUpdate(kp1, Some(rev1), Some(FullContentLock(lst)), List())

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> ContentMismatch())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val lst = List(KeyRevision(k1, rev2), KeyRevision(k2, rev1), KeyRevision(k3, rev1))
      val req = KeyValueUpdate(kp1, Some(rev1), Some(FullContentLock(lst)), List())

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> ContentMismatch())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.KeyRevision(k1, rev1)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.KeyRevision(k1, rev2)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

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

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.MayExist(k1)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.MayExist(k4)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.DoesNotExist(k1)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      var expected: HashMap[ObjectId, RequirementError] = new HashMap
      expected += (o.objectId -> KeyExistenceError())
      assert(oerrs == expected)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.DoesNotExist(k4)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

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

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.TimestampLessThan(k2, ts1)))

      val (oerrs, errs) = RequirementsChecker.check(tx2, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.TimestampGreaterThan(k1, ts2)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
    }

    {
      kvos.min = Some(k1)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
      kvos.min = None
    }

    {
      kvos.max = Some(k3)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
      kvos.max = None
    }

    {
      kvos.min = Some(k1)
      kvos.max = Some(k3)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

      assert(oerrs.isEmpty)
      assert(errs.isEmpty)
      kvos.min = None
      kvos.max = None
    }

    {
      kvos.min = Some(k3)
      val req = KeyValueUpdate(kp1, Some(rev1), None, List(KeyValueUpdate.WithinRange(k2, ByteArrayKeyOrdering)))

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

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

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

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

      val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)

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

  private def checkKVSized(kvos: KVObjectState, ops: List[KeyValueOperation], maxSliceSize: Option[Long]) =
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.KeyValue, kvos.encode())
    o.kvState = Some(kvos)
    val req = KeyValueUpdate(kp1, Some(rev1), None, Nil)
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

}
