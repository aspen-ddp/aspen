package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.{UnpinnedTransaction, AppendNotSupported, AppendPosition, ConflictingRequirements, MultipleDataUpdatesToObject}
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.common.ida.{AppendDelta, RandomAccessReedSolomon, IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, VersionBump}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, DeleteLeft, DeleteMax, DeleteMin, DeleteRight, Insert, Key, KeyValueObjectPointer, KeyValueOperation, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, Exists, KeyRevision}
import org.aspen_ddp.aspen.common.objects.ObjectRefcount
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
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
    bldr.buildTranaction(OpportunisticRebuildManager.None, Map(p1.poolId -> ida), Map(p1.poolId -> None), Map())

  private val kvp = KeyValueObjectPointer(ObjectId(new UUID(0, 9)), PoolId(new UUID(0, 0)), Array[Byte]())

  private def kvDeletesKeys(ops: List[KeyValueOperation]): Boolean =
    val bldr = mkBuilder()
    bldr.replaySafe("test")
    bldr.update(kvp, None, None, Nil, ops)
    val (txd, _, _) = bldr.buildTranaction(OpportunisticRebuildManager.None, Map(kvp.poolId -> Replication(3, 2)),
      Map(kvp.poolId -> None), Map())
    txd.requirements.collectFirst { case r: KeyValueUpdate => r.deletesKeys }.get

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
    b3.lockRevision(p1, rev1)
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
    bldr.buildTranaction(OpportunisticRebuildManager.None, Map(p1.poolId -> Replication(3, 2)),
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
      intercept[UnpinnedTransaction](buildPin(b))
  }
  test("no pin: lockRevision only") {
    withPins:
      val b = mkBuilder(); b.lockRevision(p1, rev1)
      intercept[UnpinnedTransaction](buildPin(b))
  }
  test("no pin: DoesNotExist with an Insert") {
    withPins:
      val b = mkBuilder(); b.update(kvp, None, None, List(DoesNotExist(k1)), List(ins1))
      intercept[UnpinnedTransaction](buildPin(b))
  }
  test("no pin: Exists with a Delete") {
    withPins:
      val b = mkBuilder(); b.update(kvp, None, None, List(Exists(k1)), List(Delete(k1)))
      intercept[UnpinnedTransaction](buildPin(b))
  }
  test("no pin: KeyRevision with an Insert of another key") {
    withPins:
      val b = mkBuilder(); b.update(kvp, None, None, List(KeyRevision(k1, rev1)), List(Insert(k2, Array[Byte](9))))
      intercept[UnpinnedTransaction](buildPin(b))
  }
  test("no pin: setRefcount to the same refcount") {
    withPins:
      val b = mkBuilder(); b.setRefcount(p1, rc1, rc1)
      intercept[UnpinnedTransaction](buildPin(b))
  }

  test("replaySafe lets a pinless transaction through and records the reason in its notes") {
    withPins:
      val b = mkBuilder(); b.lockRevision(p1, rev1); b.replaySafe("harmless")
      val (txd, _, _) = buildPin(b)
      txd.notes should contain ("replay-safe: harmless")
  }
  test("replaySafe on a pinned transaction changes nothing") {
    withPins:
      val b = mkBuilder(); b.bumpVersion(p1, rev1); b.replaySafe("harmless")
      val (txd, _, _) = buildPin(b)
      txd.requirements should contain (VersionBump(p1, rev1))
  }
}
