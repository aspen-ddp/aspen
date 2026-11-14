package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.ida.{AppendDelta, RandomAccessReedSolomon, Replication, XorRegion}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, KeyValueOperation, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, RequirementCheckFailure, DataUpdateOperation, KeyValueUpdate, LocalTimeRequirement,
  RefcountUpdate, ExclusiveRevisionLock, RevisionMismatch, SharedRevisionLock, TransactionId, VersionBump}
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

  private def applyKVInsert(requiredRevision: Option[ObjectRevision], timestamp: HLCTimestamp): ObjectState = {
    val kvos = new KVObjectState
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.KeyValue, kvos.encode())
    o.kvState = Some(kvos)
    val payload = KeyValueOperation.encode(List(Insert(Key(Array[Byte](1)), Array[Byte](2))), Replication(3, 2))(0)
    val req = KeyValueUpdate(kp1, requiredRevision, None, Nil)
    val result = RequirementsApplyer.apply(tx1, timestamp, List(req), HashMap(oid1 -> o), HashMap(oid1 -> payload))
    assert(result.skipped.isEmpty)
    assert(result.failed.isEmpty)
    o
  }

  test("KeyValueUpdate without a required revision advances the timestamp but not the revision") {
    val ts = HLCTimestamp(100)
    val o = applyKVInsert(None, ts)
    assert(o.metadata.revision == rev1)
    assert(o.metadata.timestamp == ts)
  }

  test("KeyValueUpdate with a required revision advances the timestamp and the revision") {
    val ts = HLCTimestamp(100)
    val o = applyKVInsert(Some(rev1), ts)
    assert(o.metadata.revision == ObjectRevision(tx1))
    assert(o.metadata.timestamp == ts)
  }

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
    assert(result.skippedReasons(oid1).isInstanceOf[RevisionMismatch])
    assert(result.failed.isEmpty)

    // The object must be left untouched
    assert(o.metadata.revision == rev1)
    assert(o.data.getByteArray sameElements Array[Byte](1, 2, 3))
  }

  /** A KeyValue object and an update of it carrying an unknown opcode */
  private def undecodableKVUpdate() =
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.Data, DataBuffer(new Array[Byte](0)))
    o.kvState = Some(new KVObjectState)
    // 0x20 is not a valid KeyValueOperation code. The trailing byte is the varint data length.
    (o, Map(o.objectId -> o), Map(o.objectId -> DataBuffer(Array[Byte](0x20, 0x00))))

  test("an undecodable KeyValue update fails its check and is skipped, leaving the object untouched") {
    val (o, objects, updates) = undecodableKVUpdate()
    val req = KeyValueUpdate(kp1, Some(rev1), None, Nil)

    // The check decodes the operations, so the update cannot pass it and then fail partway through application
    RequirementsChecker.check(tx1, HLCTimestamp.now, List(req), objects, updates)._1 shouldBe
      Map(oid1 -> RequirementCheckFailure())

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now, List(req), objects, updates)

    assert(result.skipped == Set(oid1))
    assert(result.failed.isEmpty)
    assert(o.metadata == Metadata(rev1, ref1, ts1))
  }

  test("an object that fails one requirement is not modified by its later requirements") {
    val (o, objects, updates) = undecodableKVUpdate()
    val badUpdate = KeyValueUpdate(kp1, Some(rev1), None, Nil)
    val refcountUpdate = RefcountUpdate(kp1, ref1, ObjectRefcount(2, 2))

    val result = RequirementsApplyer.apply(tx1, HLCTimestamp.now,
      List(badUpdate, refcountUpdate), objects, updates)

    assert(result.skipped == Set(oid1))

    // The refcount update follows the failure and must not have been applied
    assert(o.metadata.refcount == ref1)
  }

  test("Append XORs the delta into the zero-extended slice and bumps the revision") {
    val ida = RandomAccessReedSolomon(5, 3, 4, 8)
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
    val ida = RandomAccessReedSolomon(5, 3, 4, 8)
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

  // Blind writes (todo 2.2: Req F1, F2, F4, CRL F13, R-F4, Rebuild N1)

  // Wall-clock milliseconds: the low 16 bits of an HLCTimestamp are its logical counter
  private def wall(ms: Long): HLCTimestamp = HLCTimestamp(ms << 16)

  private val blindKey = Key(Array[Byte](7))
  private val txA = TransactionId(new UUID(0, 10))
  private val txB = TransactionId(new UUID(0, 11))

  private def emptyKVObject(): ObjectState =
    val kvos = new KVObjectState
    val o = new ObjectState(oid1, Metadata(rev1, ref1, ts1), ObjectType.KeyValue, kvos.encode())
    o.kvState = Some(kvos)
    o

  /** Applies a blind Insert(blindKey, value) by `txid` at `ts` (no required revision, no key requirement) */
  private def applyBlindInsert(o: ObjectState, txid: TransactionId, ts: HLCTimestamp, value: Array[Byte],
                               maxSliceSize: Option[Long] = None): RequirementsApplyer.Result =
    val payload = KeyValueOperation.encode(List(Insert(blindKey, value)), Replication(3, 2))(0)
    RequirementsApplyer.apply(txid, ts, List(KeyValueUpdate(kp1, None, None, Nil)), HashMap(oid1 -> o),
      HashMap(oid1 -> payload), maxSliceSize)

  private def blindValue(o: ObjectState): (Seq[Byte], ObjectRevision, HLCTimestamp) =
    val vs = o.kvState.get.content(blindKey)
    (vs.value.bytes.toSeq, vs.revision, vs.timestamp)

  test("two blind inserts of a key applied in opposite orders converge to the later one (Req F1)") {
    val a = Array[Byte](1)
    val b = Array[Byte](2)
    val inOrder = emptyKVObject()
    applyBlindInsert(inOrder, txA, wall(100), a)
    applyBlindInsert(inOrder, txB, wall(200), b)

    val reversed = emptyKVObject()
    applyBlindInsert(reversed, txB, wall(200), b)
    val late = applyBlindInsert(reversed, txA, wall(100), a)

    // The superseded insert is neither a requirement failure nor an error
    assert(late.uncommittable.isEmpty)
    blindValue(reversed) shouldBe ((b.toSeq, ObjectRevision(txB), wall(200)))
    blindValue(reversed) shouldBe blindValue(inOrder)
    assert(reversed.kvState.get.encode().getByteArray sameElements inOrder.kvState.get.encode().getByteArray)
  }

  test("a re-applied older blind insert does not regress a newer one; the same insert re-applies idempotently (Req F2)") {
    val o = emptyKVObject()
    applyBlindInsert(o, txA, wall(100), Array[Byte](1))
    applyBlindInsert(o, txB, wall(200), Array[Byte](2))

    // A crash re-apply of A's recovered vote
    applyBlindInsert(o, txA, wall(100), Array[Byte](1))
    blindValue(o) shouldBe ((Seq[Byte](2), ObjectRevision(txB), wall(200)))

    applyBlindInsert(o, txB, wall(200), Array[Byte](2))
    blindValue(o) shouldBe ((Seq[Byte](2), ObjectRevision(txB), wall(200)))
  }

  test("blind inserts at equal timestamps are ordered by revision") {
    val ts = wall(100)
    val first = emptyKVObject()
    applyBlindInsert(first, txA, ts, Array[Byte](1))
    applyBlindInsert(first, txB, ts, Array[Byte](2))
    val second = emptyKVObject()
    applyBlindInsert(second, txB, ts, Array[Byte](2))
    applyBlindInsert(second, txA, ts, Array[Byte](1))
    blindValue(first) shouldBe blindValue(second)
  }

  test("the object timestamp never goes backwards for blind KeyValue and Refcount updates (Req F4)") {
    val o = emptyKVObject()
    applyBlindInsert(o, txB, wall(200), Array[Byte](2))
    applyBlindInsert(o, txA, wall(100), Array[Byte](1))
    assert(o.metadata.timestamp == wall(200))

    val d = mkDataObject()
    d.metadata = d.metadata.copy(timestamp = wall(500))
    val result = RequirementsApplyer.apply(txA, wall(300), List(RefcountUpdate(p1, ref1, ObjectRefcount(2, 2))),
      HashMap(oid1 -> d), HashMap())
    assert(result.uncommittable.isEmpty)
    assert(d.metadata.refcount == ObjectRefcount(2, 2))
    assert(d.metadata.timestamp == wall(500))
  }

  test("the slice size checked for a superseded insert is the size applied") {
    val o = emptyKVObject()
    applyBlindInsert(o, txB, wall(200), new Array[Byte](40))
    val size = o.kvState.get.encodedSize
    // The late, smaller insert keeps the larger value, so the size is unchanged: a limit equal to it passes and the
    // estimate must not be the smaller value's
    val late = applyBlindInsert(o, txA, wall(100), Array[Byte](1), maxSliceSize = Some(size.toLong))
    assert(late.uncommittable.isEmpty)
    assert(o.kvState.get.encodedSize == size)
    assert(o.kvState.get.encodedSizeAfter(List(Insert(blindKey, Array[Byte](1))), ObjectRevision(txA),
      wall(100)) == size)
  }

  test("changedObjects includes every object a DataUpdate, KeyValueUpdate, RefcountUpdate or VersionBump names") {
    val ids = (2 to 5).map(i => ObjectId(new UUID(0, i))).toList
    val pool = PoolId(new UUID(0, 0))
    val reqs = List(
      DataUpdate(DataObjectPointer(ids(0), pool, Array[Byte]()), rev1, DataUpdateOperation.Overwrite),
      KeyValueUpdate(KeyValueObjectPointer(ids(1), pool, Array[Byte]()), None, None, Nil),
      RefcountUpdate(DataObjectPointer(ids(2), pool, Array[Byte]()), ref1, ObjectRefcount(2, 0)),
      VersionBump(DataObjectPointer(ids(3), pool, Array[Byte]()), rev1))
    RequirementsApplyer.changedObjects(reqs) shouldBe ids.toSet
  }

  test("changedObjects excludes objects only locked or checked, and non-object requirements") {
    val pool = PoolId(new UUID(0, 0))
    val reqs = List(
      ExclusiveRevisionLock(DataObjectPointer(ObjectId(new UUID(0, 6)), pool, Array[Byte]()), rev1),
      SharedRevisionLock(DataObjectPointer(ObjectId(new UUID(0, 7)), pool, Array[Byte]()), rev1),
      LocalTimeRequirement(ts1, LocalTimeRequirement.Requirement.LessThan))
    RequirementsApplyer.changedObjects(reqs) shouldBe empty
  }
}
