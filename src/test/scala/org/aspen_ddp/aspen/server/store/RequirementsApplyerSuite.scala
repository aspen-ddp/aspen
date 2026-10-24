package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.ida.{AppendDelta, AppendingReedSolomon, XorRegion}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, RefcountUpdate, TransactionId}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap
import scala.language.implicitConversions

object RequirementsApplyerSuite {
  val oid1 = ObjectId(new UUID(0,1))
  val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))
  val rev2 = ObjectRevision(TransactionId(new UUID(0, 4)))
  val ref1 = ObjectRefcount(1,1)
  val ts1  = HLCTimestamp(1)
  val tx1 = TransactionId(new UUID(0, 5))

  val p1 = DataObjectPointer(oid1, PoolId(new UUID(0,0)), Array[Byte]())
  val kp1 = KeyValueObjectPointer(oid1, PoolId(new UUID(0,0)), Array[Byte]())
}

class RequirementsApplyerSuite extends AnyFunSuite with Matchers {

  import RequirementsApplyerSuite._

  def mkDataObject(): ObjectState = new ObjectState(
    oid1,
    Metadata(rev1, ref1, ts1),
    ObjectType.Data,
    DataBuffer(Array[Byte](1, 2, 3))
  )

  test("DataUpdate applies with no skips or failures") {
    val o = mkDataObject()

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer(Array[Byte](9, 9)))

    val req = DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(result.skipped.isEmpty)
    assert(result.failed.isEmpty)
    assert(result.uncommittable.isEmpty)
    assert(o.metadata.revision == ObjectRevision(tx1))
    assert(o.data.getByteArray sameElements Array[Byte](9, 9))
  }

  test("failed requirement checks are reported as skipped, not failed") {
    val o = mkDataObject()

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer(Array[Byte](9, 9)))

    // rev2 does not match the object's rev1
    val req = DataUpdate(p1, rev2, DataUpdateOperation.Overwrite)

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(result.skipped == Set(oid1))
    assert(result.failed.isEmpty)

    // The object must be left untouched
    assert(o.metadata.revision == rev1)
    assert(o.data.getByteArray sameElements Array[Byte](1, 2, 3))
  }

  test("unexpected error while applying an update marks the object failed") {
    // The requirements check passes but the encoded update carries an unknown opcode, so
    // KeyValueOperation.decode throws partway through application. The object's metadata has
    // already been rewritten at that point, leaving it corrupt: it must be reported as failed
    // so the caller neither commits it nor keeps the in-memory state.
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.kvState = Some(new KVObjectState)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    // 0x20 is not a valid KeyValueOperation code. The trailing byte is the varint data length.
    updates += (o.objectId -> DataBuffer(Array[Byte](0x20, 0x00)))

    val req = KeyValueUpdate(kp1, Some(rev1), None, Nil)

    // Confirm the check itself is happy, so the failure can only come from apply
    val (oerrs, errs) = RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)
    assert(oerrs.isEmpty)
    assert(errs.isEmpty)

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(result.skipped.isEmpty)
    assert(result.failed == Set(oid1))
    assert(result.uncommittable == Set(oid1))
  }

  test("an object that fails one requirement is not modified by its later requirements") {
    val o = new ObjectState(
      oid1,
      Metadata(rev1, ref1, ts1),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0))
    )

    o.kvState = Some(new KVObjectState)

    var objects: HashMap[ObjectId, ObjectState] = new HashMap
    var updates: HashMap[ObjectId, DataBuffer] = new HashMap

    objects += (o.objectId -> o)
    updates += (o.objectId -> DataBuffer(Array[Byte](0x20, 0x00)))

    val badUpdate = KeyValueUpdate(kp1, Some(rev1), None, Nil)
    val refcountUpdate = RefcountUpdate(kp1, ref1, ObjectRefcount(2, 2))

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now,
      List(badUpdate, refcountUpdate), objects, updates)

    assert(result.failed == Set(oid1))

    // The refcount update follows the failure and must not have been applied
    assert(o.metadata.refcount == ref1)
  }

  test("Append XORs the delta into the zero-extended slice and bumps the revision") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    val content = Array.tabulate[Byte](20)(_.toByte)
    val more = Array.tabulate[Byte](30)(i => (i + 50).toByte)

    for i <- 0 until 5 do
      val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data,
        ida.encode(DataBuffer(content))(i))
      val delta = ida.appendDeltas(content.length, more)(i)

      val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(DataUpdate(p1, rev1, DataUpdateOperation.Append)),
        HashMap(oid1 -> o), HashMap(oid1 -> delta.encode))

      assert(result.uncommittable.isEmpty)
      assert(o.metadata.revision == ObjectRevision(tx1))
      assert(o.data.getByteArray sameElements ida.encode(DataBuffer(content ++ more))(i).getByteArray)
  }

  test("a mismatched Append leaves state untouched") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    val content = Array.tabulate[Byte](20)(_.toByte)
    val slice = ida.encode(DataBuffer(content))(0)
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data, slice)
    // Delta computed for a different current length
    val delta = ida.appendDeltas(content.length + 40, Array[Byte](1, 2, 3))(0)

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(DataUpdate(p1, rev1, DataUpdateOperation.Append)),
      HashMap(oid1 -> o), HashMap(oid1 -> delta.encode))

    assert(result.uncommittable == Set(oid1))
    assert(o.metadata.revision == rev1)
    assert(o.data.getByteArray sameElements slice.getByteArray)
  }

  test("WriteRange XORs a mid-slice delta without changing the length") {
    val slice = DataBuffer(Array.tabulate[Byte](16)(_.toByte))
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data, slice)
    val delta = AppendDelta(16, 16, List(XorRegion(4, Array[Byte](1, 2, 3))), None)

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(DataUpdate(p1, rev1, DataUpdateOperation.WriteRange)),
      HashMap(oid1 -> o), HashMap(oid1 -> delta.encode))

    val expected = Array.tabulate[Byte](16)(_.toByte)
    expected(4) = (expected(4) ^ 1).toByte
    expected(5) = (expected(5) ^ 2).toByte
    expected(6) = (expected(6) ^ 3).toByte
    assert(result.uncommittable.isEmpty)
    assert(o.metadata.revision == ObjectRevision(tx1))
    assert(o.data.getByteArray sameElements expected)
  }

  test("a mismatched WriteRange leaves state untouched") {
    val slice = DataBuffer(Array.tabulate[Byte](16)(_.toByte))
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data, slice)
    val delta = AppendDelta(24, 24, List(XorRegion(4, Array[Byte](1, 2, 3))), None)

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(DataUpdate(p1, rev1, DataUpdateOperation.WriteRange)),
      HashMap(oid1 -> o), HashMap(oid1 -> delta.encode))

    assert(result.uncommittable == Set(oid1))
    assert(o.metadata.revision == rev1)
    assert(o.data.getByteArray sameElements slice.getByteArray)
  }

  test("an object whose update would exceed the max slice size is skipped and not applied") {
    val o = mkDataObject()
    val req = DataUpdate(p1, rev1, DataUpdateOperation.Overwrite)

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(req), HashMap(oid1 -> o),
      HashMap(oid1 -> DataBuffer(new Array[Byte](5))), maxSliceSize = Some(4))

    assert(result.skipped == Set(oid1))
    assert(result.failed.isEmpty)
    assert(o.metadata.revision == rev1)
    assert(o.data.getByteArray sameElements Array[Byte](1, 2, 3))
  }
}
