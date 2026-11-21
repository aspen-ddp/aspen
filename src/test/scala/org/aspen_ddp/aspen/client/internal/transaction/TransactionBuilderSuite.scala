package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.{BlindKeyValueWrite, RefcountSerialNotAdvanced, UnpinnedTransaction, AppendNotSupported, AppendPosition, ConflictingRequirements, MultipleDataUpdatesToObject}
import org.aspen_ddp.aspen.common.ida.{AppendDelta, RandomAccessReedSolomon, IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, VersionBump}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, DeleteLeft, DeleteMax, DeleteMin, DeleteRight, Insert, Key, KeyValueObjectPointer, KeyValueOperation, ObjectId, ObjectRevision, SetLeft, SetMax, SetMin, SetRight}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, Exists, FullContentLock, KeyRevision, MayExist}
import org.aspen_ddp.aspen.common.objects.ObjectRefcount
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.client.internal.allocation.AllocationFinalizationAction
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class TransactionBuilderSuite extends AnyFunSuite with Matchers {

  private val oid1 = ObjectId(new UUID(0, 1))
  private val tx1 = TransactionId(new UUID(0, 2))
  private val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))
  private val p1 = DataObjectPointer(oid1, PoolId(new UUID(0, 0)), Array[Byte]())

  private def mkBuilder(): TransactionBuilder =
    new TransactionBuilder(tx1, (_, _) => 0.toByte, ClientId(new UUID(0, 4)))

  private val rs = RandomAccessReedSolomon(5, 3, 4, 8)
  private val L = 30
  private val pos = AppendPosition(p1, rev1, L)
  private val appended = Array.tabulate[Byte](50)(i => (i * 7 + 1).toByte)

  private def build(bldr: TransactionBuilder, ida: IDA) =
    bldr.buildTranaction(Map(p1.poolId -> ida), Map(p1.poolId -> None), Map())

  private val kvp = KeyValueObjectPointer(ObjectId(new UUID(0, 9)), PoolId(new UUID(0, 0)), Array[Byte]())

  private def kvDeletesKeys(ops: List[KeyValueOperation]): Boolean =
    val bldr = mkBuilder()
    bldr.replaySafe("test")
    val deleted = ops.collect { case d: Delete => KeyRevision(d.key, rev1) }
    bldr.update(kvp, Some(rev1), None, deleted, ops)
    val (txd, _, _) = bldr.buildTranaction(Map(kvp.poolId -> Replication(3, 2)),
      Map(kvp.poolId -> None), Map())
    txd.requirements.collectFirst { case r: KeyValueUpdate => r.deletesKeys }.get

  test("every allocated object gets one AllocationFinalizationAction (Rebuild F17)") {
    val bldr = mkBuilder()
    bldr.replaySafe("test")
    val kv2 = KeyValueObjectPointer(ObjectId(new UUID(0, 10)), PoolId(new UUID(0, 0)), Array[Byte]())
    bldr.addAllocatingObject(kvp)
    bldr.addAllocatingObject(kv2)
    bldr.addAllocatingObject(kvp)
    val (txd, _, _) = bldr.buildTranaction(Map(kvp.poolId -> Replication(3, 2)), Map(kvp.poolId -> None), Map())
    val allocActions = txd.finalizationActions.filter(_.typeId.uuid == AllocationFinalizationAction.typeUUID)
    allocActions.map(fa => ObjectPointer(fa.data).id).sortBy(_.uuid) shouldBe List(kvp.id, kv2.id).sortBy(_.uuid)
  }

  test("an update with a key Delete sets deletesKeys") {
    kvDeletesKeys(List(Delete(Key(Array[Byte](1))), Insert(Key(Array[Byte](2)), Array[Byte](1)))) shouldBe true
  }

  test("an update without a key Delete leaves deletesKeys false") {
    kvDeletesKeys(List(Insert(Key(Array[Byte](2)), Array[Byte](1)), DeleteRight(), DeleteLeft(), DeleteMin(),
      DeleteMax())) shouldBe false
  }

  test("append adds an Append DataUpdate with the position's revision") {
    val bldr = mkBuilder()
    bldr.append(pos, DataBuffer(appended))
    assert(!bldr.isEmpty)

    val (txd, _, _) = build(bldr, rs)

    txd.requirements should contain (DataUpdate(p1, rev1, DataUpdateOperation.Append))
  }

  test("append sends each store its own encoded delta") {
    val bldr = mkBuilder()
    bldr.append(pos, DataBuffer(appended))

    val (_, updates, _) = build(bldr, rs)

    val expected = rs.appendDeltas(L, appended)
    updates.size should be (rs.width)
    (0 until rs.width).foreach: i =>
      val td = updates(StoreId(p1.poolId, i.toByte))
      td.localUpdates.size should be (1)
      td.localUpdates.head.objectId should be (oid1)
      AppendDelta.decode(td.localUpdates.head.data) should be (expected(i))
  }

  test("append and overwrite of the same object conflict") {
    val bldr = mkBuilder()
    bldr.append(pos, DataBuffer(appended))

    intercept[MultipleDataUpdatesToObject]:
      bldr.overwrite(p1, rev1, DataBuffer(Array[Byte](1)))

    val b2 = mkBuilder()
    b2.overwrite(p1, rev1, DataBuffer(Array[Byte](1)))
    intercept[MultipleDataUpdatesToObject]:
      b2.append(pos, DataBuffer(appended))

    val b3 = mkBuilder()
    b3.exclusiveLockRevision(p1, rev1)
    intercept[ConflictingRequirements]:
      b3.append(pos, DataBuffer(appended))
  }

  test("building an append for a classic ReedSolomon pool throws AppendNotSupported") {
    val bldr = mkBuilder()
    val classic = ReedSolomon(5, 3, 4)
    bldr.append(pos, DataBuffer(appended))

    val err = intercept[AppendNotSupported]:
      build(bldr, classic)

    err.objectPointer should be (p1)
    err.ida should be (classic)
  }

  test("overwrite records an update") {
    val bldr = mkBuilder()

    assert(bldr.isEmpty)

    bldr.overwrite(p1, rev1, DataBuffer(Array[Byte](1, 2, 3)))

    assert(!bldr.isEmpty)
  }

  // Pin check. Enforcement is switched on per builder: the global switch is shared with suites running in parallel
  private def withPins[T](f: => T): T = f

  private def buildPin(bldr: TransactionBuilder) =
    bldr.buildTranaction(Map(p1.poolId -> Replication(3, 2)),
      Map(p1.poolId -> None), Map())

  private val k1 = Key(Array[Byte](1))
  private val k2 = Key(Array[Byte](2))
  private val ins1 = Insert(k1, Array[Byte](9))
  private val rc1 = ObjectRefcount(1, 1)
  private val rc2 = ObjectRefcount(2, 2)

  test("pin: overwrite at a revision") {
    withPins:
      val b = mkBuilder(); b.overwrite(p1, rev1, DataBuffer(Array[Byte](1))); buildPin(b)
  }
  test("pin: bumpVersion") {
    withPins:
      val b = mkBuilder(); b.bumpVersion(p1, rev1); buildPin(b)
  }
  test("pin: update with a required revision") {
    withPins:
      val b = mkBuilder(); b.update(kvp, Some(rev1), None, Nil, List(ins1)); buildPin(b)
  }
  test("pin: KeyRevision with an Insert of the key") {
    withPins:
      val b = mkBuilder(); b.update(kvp, None, None, List(KeyRevision(k1, rev1)), List(ins1)); buildPin(b)
  }
  test("pin: KeyRevision with a Delete of the key") {
    withPins:
      val b = mkBuilder(); b.update(kvp, None, None, List(KeyRevision(k1, rev1)), List(Delete(k1))); buildPin(b)
  }
  test("pin: setRefcount with a different refcount") {
    withPins:
      val b = mkBuilder(); b.setRefcount(p1, rc1, rc2); buildPin(b)
  }

  test("no pin: allocation only") {
    withPins:
      val b = mkBuilder(); b.addAllocatingObject(p1)
      intercept[UnpinnedTransaction](buildPin(b)).getMessage should include (
        s"${p1.id}: allocated by the transaction (an allocation is not a pin)")
  }
  test("no pin: exclusiveLockRevision only") {
    withPins:
      val b = mkBuilder(); b.exclusiveLockRevision(p1, rev1)
      intercept[UnpinnedTransaction](buildPin(b)).getMessage should include (
        s"${p1.id}: an ExclusiveRevisionLock (a lock is not a pin)")
  }
  test("no pin: DoesNotExist with an Insert") {
    withPins:
      val b = mkBuilder(); b.update(kvp, None, None, List(DoesNotExist(k1)), List(ins1))
      intercept[UnpinnedTransaction](buildPin(b)).getMessage should include (
        s"${kvp.id}: the KeyValue update has no required revision and writes $k1 with no KeyRevision on any of them " +
        "(DoesNotExist is not a pin)")
  }
  test("a Delete guarded only by Exists is a blind write") {
    withPins:
      val b = mkBuilder(); b.update(kvp, None, None, List(Exists(k1)), List(Delete(k1)))
      intercept[BlindKeyValueWrite](buildPin(b))
  }
  test("no pin: KeyRevision with an Insert of another key") {
    withPins:
      val b = mkBuilder()
      b.update(kvp, None, None, List(KeyRevision(k1, rev1), DoesNotExist(k2)), List(Insert(k2, Array[Byte](9))))
      intercept[UnpinnedTransaction](buildPin(b)).getMessage should include (
        s"writes $k2 but has a KeyRevision only on $k1")
  }
  test("no pin: setRefcount to the same refcount") {
    withPins:
      val b = mkBuilder(); b.setRefcount(p1, rc1, rc1)
      val message = intercept[UnpinnedTransaction](buildPin(b)).getMessage
      message should include (s"${p1.id}: the RefcountUpdate leaves the refcount at $rc1")
      message should include ("has no pin and is not marked replay-safe")
      message should include ("Transaction.replaySafe(reason)")
  }

  test("a refcount update that does not advance the update serial is refused, except an allocation's or an identity") {
    val b = mkBuilder(); b.setRefcount(p1, ObjectRefcount(5, 2), ObjectRefcount(5, 1))
    intercept[RefcountSerialNotAdvanced](buildPin(b)) shouldBe
      RefcountSerialNotAdvanced(p1, ObjectRefcount(5, 2), ObjectRefcount(5, 1))
    val c = mkBuilder(); c.replaySafe("test"); c.setRefcount(p1, ObjectRefcount(5, 2), ObjectRefcount(4, 3))
    intercept[RefcountSerialNotAdvanced](buildPin(c))
    val d = mkBuilder(); d.replaySafe("test"); d.setRefcount(p1, rc1, rc1); buildPin(d)
    // Allocation, staged in either order
    val e = mkBuilder(); e.replaySafe("test"); e.setRefcount(p1, ObjectRefcount.Allocating, ObjectRefcount(0, 3))
    e.addAllocatingObject(p1); e.overwrite(p1, ObjectRevision.Allocating, DataBuffer(Array[Byte](1))); buildPin(e)
  }

  // Blind writes (todo 2.2): stores apply a blind Insert last-writer-wins, but keep no tombstone for a Delete and no
  // timestamps for object-level parts, so the builder refuses those blind

  test("a blind Delete is refused even when replay-safe") {
    val b = mkBuilder(); b.replaySafe("test"); b.update(kvp, None, None, Nil, List(Delete(k1)))
    intercept[BlindKeyValueWrite](buildPin(b))
  }
  test("a Delete of one key does not borrow another key's KeyRevision") {
    val b = mkBuilder(); b.update(kvp, None, None, List(KeyRevision(k2, rev1)), List(Delete(k1), Insert(k2, Array[Byte](1))))
    intercept[BlindKeyValueWrite](buildPin(b))
  }
  test("a Delete under only a required object revision is refused: an Insert of its key moves no revision") {
    val b = mkBuilder(); b.update(kvp, Some(rev1), None, Nil, List(Delete(k1)))
    intercept[BlindKeyValueWrite](buildPin(b))
    val c = mkBuilder(); c.update(kvp, Some(rev1), None, List(KeyRevision(k1, rev1)), List(Delete(k1))); buildPin(c)
  }
  test("each blind object-level operation is refused") {
    for op <- List(SetMin(Key(Array[Byte](1))), SetMax(Key(Array[Byte](1))), SetLeft(Array[Byte](1)), SetRight(Array[Byte](1)),
                   DeleteMin(), DeleteMax(), DeleteLeft(), DeleteRight()) do
      val b = mkBuilder(); b.replaySafe("test"); b.update(kvp, None, None, Nil, List(op))
      intercept[BlindKeyValueWrite](buildPin(b))
  }
  test("an object-level operation under only a content lock is refused; a Delete under one is not blind") {
    val lock = Some(FullContentLock(List(KeyRevision(k1, rev1))))
    val b = mkBuilder(); b.update(kvp, None, lock, Nil, List(SetMax(Key(Array[Byte](1)))))
    intercept[BlindKeyValueWrite](buildPin(b))
    val d = mkBuilder(); d.replaySafe("test"); d.update(kvp, None, lock, Nil, List(Delete(k1))); buildPin(d)
  }
  test("an object-level operation under a required object revision is not blind") {
    val b = mkBuilder(); b.update(kvp, Some(rev1), None, Nil, List(SetMin(Key(Array[Byte](1))))); buildPin(b)
  }
  test("an Insert that checks nothing of its key is refused even when replay-safe") {
    for reqs <- List(Nil, List(MayExist(k1)), List(KeyRevision(k2, rev1))) do
      val b = mkBuilder(); b.replaySafe("test"); b.update(kvp, None, None, reqs, List(ins1))
      intercept[BlindKeyValueWrite](buildPin(b))
  }
  test("each refused write's message names the object, the operation's key, what it lacks and the fix") {
    def message(reqs: List[KeyValueUpdate.KeyRequirement], ops: List[KeyValueOperation],
                rev: Option[ObjectRevision] = None): String =
      val b = mkBuilder(); b.replaySafe("test"); b.update(kvp, rev, None, reqs, ops)
      intercept[BlindKeyValueWrite](buildPin(b)).getMessage
    val insert = message(List(MayExist(k1)), List(ins1))
    insert should include (kvp.id.toString)
    insert should include (s"Insert of $k1 has no requirement that checks the key")
    insert should include ("KeyRevision, Exists or DoesNotExist")
    insert should include ("MayExist")
    val delete = message(Nil, List(Delete(k1)), Some(rev1))
    delete should include (s"Delete of $k1 has neither a KeyRevision on the key nor a content lock")
    delete should include ("a required revision")
    val objectLevel = message(Nil, List(SetMax(k2)))
    objectLevel should include ("SetMax changes an object-level field")
    objectLevel should include ("without a required revision")
  }
  test("an Insert passes with a check on its key, a required revision or a content lock") {
    for reqs <- List(List(DoesNotExist(k1)), List(Exists(k1)), List(KeyRevision(k1, rev1))) do
      val b = mkBuilder(); b.replaySafe("test"); b.update(kvp, None, None, reqs, List(ins1)); buildPin(b)
    val r = mkBuilder(); r.update(kvp, Some(rev1), None, Nil, List(ins1)); buildPin(r)
    val c = mkBuilder(); c.replaySafe("test")
    c.update(kvp, None, Some(FullContentLock(List(KeyRevision(k2, rev1)))), Nil, List(ins1)); buildPin(c)
  }
  test("an allocated object's initial content may set object-level parts and keys") {
    val b = mkBuilder()
    b.addAllocatingObject(kvp)
    b.update(kvp, None, None, Nil, List(SetMin(Key(Array[Byte](1))), SetRight(Array[Byte](2)), ins1))
    b.setRefcount(kvp, ObjectRefcount.Allocating, ObjectRefcount.Default)
    b.replaySafe("test")
    buildPin(b)
  }

  test("replaySafe lets a pinless transaction through and records the reason in its notes") {
    withPins:
      val b = mkBuilder(); b.exclusiveLockRevision(p1, rev1); b.replaySafe("harmless")
      val (txd, _, _) = buildPin(b)
      txd.notes should contain ("replay-safe: harmless")
      txd.replaySafe shouldBe true
  }
  test("a description records whether it is replaySafe, for the stores' pin check") {
    val b = mkBuilder(); b.bumpVersion(p1, rev1)
    buildPin(b)._1.replaySafe shouldBe false
  }
  test("a KeyValueUpdate is flagged pinned only when it writes a key it carries a KeyRevision on") {
    def flag(ops: List[KeyValueOperation]): Boolean =
      val inserted = ops.collect { case i: Insert if i.key != k1 => DoesNotExist(i.key) }
      val b = mkBuilder(); b.replaySafe("test"); b.update(kvp, None, None, KeyRevision(k1, rev1) :: inserted, ops)
      buildPin(b)._1.requirements.collectFirst { case u: KeyValueUpdate => u.pinned }.get
    flag(List(ins1)) shouldBe true
    flag(List(Delete(k1), Insert(k2, Array[Byte](1)))) shouldBe true
    flag(List(Insert(k2, Array[Byte](1)))) shouldBe false
  }
  test("replaySafe on a pinned transaction changes nothing") {
    withPins:
      val b = mkBuilder(); b.bumpVersion(p1, rev1); b.replaySafe("harmless")
      val (txd, _, _) = buildPin(b)
      txd.requirements should contain (VersionBump(p1, rev1))
  }
}
