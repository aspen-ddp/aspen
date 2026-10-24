package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.{AppendPosition, ConflictingRequirements, DataObjectState, MultipleDataUpdatesToObject, ObjectSizeLimitExceeded, RangeWriteBeyondEnd, RangeWriteNotSupported, RangedWrite, RangedWriteState}
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.common.ida.{AppendDelta, AppendingReedSolomon, IDA, RangeEdit, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class TransactionRangedWriteSuite extends AnyFunSuite with Matchers {

  private val oid1 = ObjectId(new UUID(0, 1))
  private val tx1 = TransactionId(new UUID(0, 2))
  private val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))
  private val p1 = DataObjectPointer(oid1, PoolId(new UUID(0, 0)), Array[Byte]())

  private val ars = AppendingReedSolomon(5, 3, 4, 8)
  private val repl = Replication(3, 2)
  private val L = 30
  private val content = Array.tabulate[Byte](L)(i => (i * 7 + 1).toByte)

  private def mkBuilder(): TransactionBuilder =
    new TransactionBuilder(tx1, (_, _) => 0.toByte, ClientId(new UUID(0, 4)))

  private def fullState(bytes: Array[Byte] = content): RangedWriteState =
    RangedWriteState.fromObject(DataObjectState(p1, rev1, ObjectRefcount(0, 1), HLCTimestamp.now, HLCTimestamp.now,
      ars, 0, DataBuffer(bytes)))

  private def build(bldr: TransactionBuilder, ida: IDA, maxObjectSize: Option[Int] = None) =
    bldr.buildTranaction(OpportunisticRebuildManager.None, Map(p1.poolId -> ida), Map(p1.poolId -> maxObjectSize))

  private def bytes(s: String): Array[Byte] = s.getBytes("UTF-8")

  test("overlapping writes throw immediately") {
    val bldr = mkBuilder()
    intercept[IllegalArgumentException]:
      bldr.rangedWrites(fullState(), List(RangedWrite(0, bytes("abcd")), RangedWrite(3, bytes("x"))), false)
    // Order of the list does not matter
    intercept[IllegalArgumentException]:
      bldr.rangedWrites(fullState(), List(RangedWrite(3, bytes("x")), RangedWrite(0, bytes("abcd"))), false)
    assert(bldr.isEmpty)
  }

  test("touching writes are accepted") {
    val bldr = mkBuilder()
    bldr.rangedWrites(fullState(), List(RangedWrite(4, bytes("x")), RangedWrite(0, bytes("abcd"))), false)
    assert(!bldr.isEmpty)
    build(bldr, ars)
  }

  test("empty write list, empty write data and negative offset throw IllegalArgumentException") {
    val bldr = mkBuilder()
    intercept[IllegalArgumentException]:
      bldr.rangedWrites(fullState(), Nil, false)
    intercept[IllegalArgumentException]:
      bldr.rangedWrites(fullState(), List(RangedWrite(2, Array[Byte]())), false)
    intercept[IllegalArgumentException]:
      bldr.rangedWrites(fullState(), List(RangedWrite(-1, bytes("a"))), false)
    assert(bldr.isEmpty)
  }

  test("second update to the same object throws MultipleDataUpdatesToObject") {
    val w = List(RangedWrite(0, bytes("a")))

    val b1 = mkBuilder()
    b1.rangedWrites(fullState(), w, false)
    intercept[MultipleDataUpdatesToObject]:
      b1.rangedWrites(fullState(), w, false)
    intercept[MultipleDataUpdatesToObject]:
      b1.overwrite(p1, rev1, DataBuffer(Array[Byte](1)))
    intercept[MultipleDataUpdatesToObject]:
      b1.append(AppendPosition(p1, rev1, L), DataBuffer(Array[Byte](1)))

    val b2 = mkBuilder()
    b2.append(AppendPosition(p1, rev1, L), DataBuffer(Array[Byte](1)))
    intercept[MultipleDataUpdatesToObject]:
      b2.rangedWrites(fullState(), w, false)

    val b3 = mkBuilder()
    b3.lockRevision(p1, rev1)
    intercept[ConflictingRequirements]:
      b3.rangedWrites(fullState(), w, false)
  }

  test("building with a classic ReedSolomon pool fails with RangeWriteNotSupported") {
    val bldr = mkBuilder()
    val classic = ReedSolomon(5, 3, 4)
    bldr.rangedWrites(fullState(), List(RangedWrite(0, bytes("a"))), false)

    val err = intercept[RangeWriteNotSupported]:
      build(bldr, classic)

    err.objectPointer should be (p1)
    err.ida should be (classic)
  }

  test("gap without zeroFill fails at build; with zeroFill builds") {
    val w = List(RangedWrite(0, bytes("a")), RangedWrite(L + 1, bytes("b")))

    val b1 = mkBuilder()
    b1.rangedWrites(fullState(), w, false)
    val err = intercept[RangeWriteBeyondEnd]:
      build(b1, ars)
    err.objectPointer should be (p1)
    err.currentLength should be (L)
    err.offset should be (L + 1)

    val b2 = mkBuilder()
    b2.rangedWrites(fullState(), w, true)
    val (_, updates, _) = build(b2, ars)
    updates.size should be (ars.width)
  }

  test("offset equal to length builds without zeroFill") {
    val b1 = mkBuilder()
    b1.rangedWrites(fullState(), List(RangedWrite(L, bytes("abc"))), false)
    build(b1, ars)

    // A write touching the end of an extending write leaves no gap
    val b3 = mkBuilder()
    b3.rangedWrites(fullState(), List(RangedWrite(L - 1, bytes("ab")), RangedWrite(L + 1, bytes("cd"))), false)
    build(b3, ars)

    // Zero length object
    val b2 = mkBuilder()
    b2.rangedWrites(fullState(Array[Byte]()), List(RangedWrite(0, bytes("abc"))), false)
    build(b2, repl)
  }

  test("result longer than maxObjectSize fails; exactly maxObjectSize builds") {
    val w = List(RangedWrite(L - 2, bytes("abcd"))) // new length L + 2

    val b1 = mkBuilder()
    b1.rangedWrites(fullState(), w, false)
    val err = intercept[ObjectSizeLimitExceeded]:
      build(b1, ars, Some(L + 1))
    err.objectPointer should be (p1)
    err.newLength should be (L + 2)
    err.maxObjectSize should be (L + 1)

    val b2 = mkBuilder()
    b2.rangedWrites(fullState(), w, false)
    build(b2, ars, Some(L + 2))

    // Overwrites within the object are measured against the current length
    val b3 = mkBuilder()
    b3.rangedWrites(fullState(), List(RangedWrite(0, bytes("a"))), false)
    build(b3, repl, Some(L))
  }

  test("ARS length overflow fails at build with ObjectSizeLimitExceeded") {
    val bldr = mkBuilder()
    bldr.rangedWrites(fullState(), List(RangedWrite(Int.MaxValue - ars.rowBytes, bytes("a"))), true)
    val err = intercept[ObjectSizeLimitExceeded]:
      build(bldr, ars)
    err.maxObjectSize should be (Int.MaxValue - ars.rowBytes)
  }

  test("the requirement is DataUpdate(ptr, state.revision, WriteRange)") {
    val bldr = mkBuilder()
    val writes = List(RangedWrite(10, bytes("xyz")), RangedWrite(2, bytes("ab")), RangedWrite(28, bytes("tail!")))
    bldr.rangedWrites(fullState(), writes, false)

    val (txd, updates, _) = build(bldr, ars)

    txd.requirements should contain (DataUpdate(p1, rev1, DataUpdateOperation.WriteRange))

    val edits = List(
      RangeEdit(2, bytes("ab"), content.slice(2, 4)),
      RangeEdit(10, bytes("xyz"), content.slice(10, 13)),
      RangeEdit(28, bytes("tail!"), content.slice(28, 30)))
    val expected = ars.rangedWriteDeltas(L, edits)
    updates.size should be (ars.width)
    (0 until ars.width).foreach: i =>
      val td = updates(StoreId(p1.poolId, i.toByte))
      td.localUpdates.size should be (1)
      td.localUpdates.head.objectId should be (oid1)
      AppendDelta.decode(td.localUpdates.head.data) should be (expected(i))
  }

  test("state missing old bytes for a written range throws") {
    // Holds [4, 10) and [10, 12) of a 30 byte object
    val state = RangedWriteState.fromRanges(p1, rev1, L, List((4, content.slice(4, 10)), (10, content.slice(10, 12))))

    // Covered across adjacent pieces
    state.oldBytes(5, 6).toList should be (content.slice(5, 11).toList)
    // Clipped to the object length; beyond the end needs nothing
    val tailState = RangedWriteState.fromRanges(p1, rev1, L, List((28, content.slice(28, 30))))
    tailState.oldBytes(28, 10).toList should be (content.slice(28, 30).toList)
    state.oldBytes(L, 5).length should be (0)

    intercept[IllegalArgumentException]:
      state.oldBytes(3, 2)
    intercept[IllegalArgumentException]:
      state.oldBytes(11, 2)

    val bldr = mkBuilder()
    intercept[IllegalArgumentException]:
      bldr.rangedWrites(state, List(RangedWrite(0, bytes("a"))), false)
    assert(bldr.isEmpty)

    bldr.rangedWrites(state, List(RangedWrite(4, bytes("abcdefgh"))), false)
    val (_, updates, _) = build(bldr, repl)
    val expected = repl.rangedWriteDeltas(L, List(RangeEdit(4, bytes("abcdefgh"), content.slice(4, 12))))
    AppendDelta.decode(updates(StoreId(p1.poolId, 0)).localUpdates.head.data) should be (expected(0))
  }

  test("fromObject holds the full logical content") {
    val s = fullState()
    s.pointer should be (p1)
    s.revision should be (rev1)
    s.length should be (L)
    s.oldBytes(0, L).toList should be (content.toList)
  }
}
