package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AppendPosition, DataObjectRangeState, RangedWrite, RangedWriteState}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class RangedWriteFetchSuite extends AsyncFunSuite with Matchers:

  private val p1 = DataObjectPointer(ObjectId(new UUID(0, 1)), PoolId(new UUID(0, 0)), Array[Byte]())
  private val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))
  private val rev2 = ObjectRevision(TransactionId(new UUID(0, 4)))
  private val ida = Replication(3, 2)
  private val L = 30
  private val content = Array.tabulate[Byte](L)(i => (i * 7 + 1).toByte)

  /** What readRange(offset, Some(length)) returns at `rev`: clipped to the object */
  private def rangeRead(offset: Int, length: Int, rev: ObjectRevision = rev1,
                        objectLength: Option[Int] = Some(L)): DataObjectRangeState =
    val bytes = if offset >= L then Array[Byte]() else content.slice(offset, math.min(offset + length, L))
    new DataObjectRangeState(p1, rev, ObjectRefcount(0, 1), HLCTimestamp.now, HLCTimestamp.now, ida, offset,
      DataBuffer(bytes), objectLength)

  private def readsFor(writes: List[RangedWrite], rev: ObjectRevision = rev1): List[DataObjectRangeState] =
    writes.map(w => rangeRead(w.offset, w.data.length, rev))

  private def position(rev: ObjectRevision = rev1): AppendPosition = AppendPosition(p1, rev, L)

  private def w(offset: Int, length: Int): RangedWrite = RangedWrite(offset, Array.fill[Byte](length)(9))

  private val writes = List(w(20, 3), w(2, 4), w(L - 2, 10), w(L, 5), w(L + 7, 2))

  test("combine builds a state holding the old bytes under every write"):
    val state = RangedWriteFetch.combine(p1, writes, position(), readsFor(writes))
    state.revision should be (rev1)
    state.length should be (L)
    writes.foreach: wr =>
      val expected = if wr.offset >= L then Array[Byte]() else content.slice(wr.offset, math.min(wr.end.toInt, L))
      state.oldBytes(wr.offset, wr.data.length).toList should be (expected.toList)
    succeed

  test("combine clips over-long reads to the object length"):
    // A read whose object length is unknown may carry bytes past the end; only bytes within L are kept
    val long = new DataObjectRangeState(p1, rev1, ObjectRefcount(0, 1), HLCTimestamp.now, HLCTimestamp.now, ida,
      L - 2, DataBuffer(content.slice(L - 2, L) ++ Array[Byte](0, 0, 0)), None)
    val state = RangedWriteFetch.combine(p1, List(w(L - 2, 10)), position(), List(long))
    state.oldBytes(L - 2, 10).toList should be (content.slice(L - 2, L).toList)

  test("combine fails when a read at the same revision does not cover its write"):
    val short = readsFor(writes).updated(1, rangeRead(2, 2))
    intercept[IllegalStateException]:
      RangedWriteFetch.combine(p1, writes, position(), short)
    succeed
