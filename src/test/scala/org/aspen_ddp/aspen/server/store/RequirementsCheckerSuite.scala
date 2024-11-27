package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, KeyValueObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StorePointer
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{FullContentLock, KeyRevision}
import org.aspen_ddp.aspen.common.transaction.{ContentMismatch, DataUpdate, DataUpdateOperation, KeyExistenceError, KeyValueUpdate, LocalTimeError, LocalTimeRequirement, MissingObjectUpdate, RefcountMismatch, RefcountUpdate, RequirementError, RevisionLock, RevisionMismatch, TransactionCollision, TransactionId, VersionBump, WithinRangeError}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

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

  val p1 = new DataObjectPointer(oid1, PoolId(new UUID(0,0)), None, Replication(1,1), Array())
  val p2 = new DataObjectPointer(oid2, PoolId(new UUID(0,0)), None, Replication(1,1), Array())

  val kp1 = new KeyValueObjectPointer(oid1, PoolId(new UUID(0,0)), None, Replication(1,1), Array())
}

class RequirementsCheckerSuite extends AnyFunSuite with Matchers {

  import RequirementsCheckerSuite._

  def mkobjs(): (ObjectState, ObjectState) = {

    val o1 = new ObjectState(
      oid1,
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
    )

    val o2 = new ObjectState(
      oid2,
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev2, ref2, ts2),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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

  test("VersionBump okay") {
    val o = new ObjectState(
      oid1,
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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
      StorePointer(1, new Array[Byte](0)),
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
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

}

