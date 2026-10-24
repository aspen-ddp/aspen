package org.aspen_ddp.aspen.common.ida

import org.aspen_ddp.aspen.common.DataBuffer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions
import scala.util.Random

object RangedWriteDeltaSuite:

  def bytes(n: Int, rnd: Random): Array[Byte] =
    val a = new Array[Byte](n)
    rnd.nextBytes(a)
    a

  /** Builds the RangeEdit for writing newBytes at offset into old */
  def edit(old: Array[Byte], offset: Int, newBytes: Array[Byte]): RangeEdit =
    val overlap = math.max(0, math.min(newBytes.length, old.length - offset))
    RangeEdit(offset, newBytes, if overlap > 0 then old.slice(offset, offset + overlap) else Array.emptyByteArray)

  /** The content after applying the writes; gaps beyond the old end are zero */
  def applyWrites(old: Array[Byte], writes: List[(Int, Array[Byte])]): Array[Byte] =
    val newLength = writes.foldLeft(old.length)((n, w) => math.max(n, w._1 + w._2.length))
    val arr = java.util.Arrays.copyOf(old, newLength)
    writes.foreach((off, b) => System.arraycopy(b, 0, arr, off, b.length))
    arr

  /** Random non-overlapping writes over [0, limit) */
  def randomWrites(rnd: Random, limit: Int, maxCount: Int): List[(Int, Array[Byte])] =
    val n = 1 + rnd.nextInt(maxCount)
    val points = List.fill(2 * n)(rnd.nextInt(limit + 1)).sorted
    points.grouped(2).toList.collect {
      case List(a, b) if b > a => (a, bytes(b - a, rnd))
    } match
      case Nil => List((rnd.nextInt(limit), bytes(1 + rnd.nextInt(5), rnd)))
      case ws => ws

  /** Applies the deltas (through an encode/decode round trip) to the slices of encode(old) */
  def applyDeltas(ida: IDA & AppendableIDA, old: Array[Byte], writes: List[(Int, Array[Byte])]): Array[Array[Byte]] =
    val slices = ida.encode(old)
    val deltas = ida.rangedWriteDeltas(old.length, writes.map((o, b) => edit(old, o, b)))
    assert(deltas.length == ida.width, s"${deltas.length} deltas for width ${ida.width}")
    Array.tabulate(ida.width) { i =>
      AppendDelta.decode(deltas(i).encode).applyTo(DataBuffer(slices(i))).getByteArray
    }


class RangedWriteDeltaSuite extends AnyFunSuite with Matchers:

  import RangedWriteDeltaSuite.*

  private val Configs = List(Replication(1, 1), Replication(3, 2), Replication(5, 3))

  private def check(ida: IDA & AppendableIDA, old: Array[Byte], writes: List[(Int, Array[Byte])]): Unit =
    val expected = ida.encode(applyWrites(old, writes))
    val actual = applyDeltas(ida, old, writes)
    withClue(s"$ida len=${old.length} writes=${writes.map((o, b) => (o, b.length))}") {
      actual.indices.foreach(i => actual(i).toSeq should be (expected(i).toSeq))
    }

  test("Replication: delta-applied slices equal a fresh encode for edit shapes") {
    val rnd = new Random(1)
    for
      ida <- Configs
      len <- List(0, 1, 7, 50, 300)
    do
      val old = bytes(len, rnd)
      val shapes = List(
        List((0, bytes(1, rnd))),
        List((len, bytes(5, rnd))),
        List((len + 17, bytes(3, rnd))),
        List((math.max(0, len - 2), bytes(10, rnd))),
        List((0, bytes(len + 4, rnd))))
        ++ (if len >= 20 then List(List((3, bytes(4, rnd)), (7, bytes(2, rnd)), (15, bytes(5, rnd)))) else Nil)
      shapes.foreach(w => check(ida, old, w))
      (0 until 30).foreach(_ => check(ida, old, randomWrites(rnd, len + 40, 4)))
  }

  test("Replication: deltas carry exactly the edited bytes") {
    val rnd = new Random(2)
    val ida = Replication(3, 2)
    val old = bytes(1000, rnd)
    val writes = List((10, bytes(5, rnd)), (500, bytes(7, rnd)), (998, bytes(6, rnd)))
    val deltas = ida.rangedWriteDeltas(old.length, writes.map((o, b) => edit(old, o, b)))
    deltas.foreach { d =>
      d.expectedSliceLength should be (1000)
      d.newSliceLength should be (1004)
      d.expectedTail should be (None)
      d.regions.map(r => (r.offset, r.bytes.length)) should be (List((10, 5), (500, 7), (998, 6)))
      d.regions.head.bytes.toSeq should be (old.slice(10, 15).zip(writes.head._2).map((a, b) => (a ^ b).toByte).toSeq)
      // Beyond the old end the region carries the new bytes themselves
      d.regions(2).bytes.drop(2).toSeq should be (writes(2)._2.drop(2).toSeq)
    }
  }

  test("Replication: touching edits merge into one region") {
    val rnd = new Random(3)
    val old = bytes(100, rnd)
    val writes = List((20, bytes(5, rnd)), (10, bytes(10, rnd)))
    val d = Replication(3, 2).rangedWriteDeltas(old.length, writes.map((o, b) => edit(old, o, b)))(0)
    d.regions.map(r => (r.offset, r.bytes.length)) should be (List((10, 15)))
    d.newSliceLength should be (d.expectedSliceLength)
  }

  test("rangedWriteDeltas rejects invalid edits") {
    val old = new Array[Byte](100)
    for ida <- List[IDA & AppendableIDA](Replication(3, 2), AppendingReedSolomon(5, 3, 4, 8)) do
      an [IllegalArgumentException] should be thrownBy ida.rangedWriteDeltas(100, Nil)
      an [IllegalArgumentException] should be thrownBy
        ida.rangedWriteDeltas(100, List(edit(old, 10, new Array[Byte](5)), edit(old, 12, new Array[Byte](5))))
      an [IllegalArgumentException] should be thrownBy
        ida.rangedWriteDeltas(100, List(RangeEdit(-1, new Array[Byte](2), Array.emptyByteArray)))
      an [IllegalArgumentException] should be thrownBy
        ida.rangedWriteDeltas(100, List(RangeEdit(98, new Array[Byte](5), new Array[Byte](5))))
      an [IllegalArgumentException] should be thrownBy
        ida.rangedWriteDeltas(100, List(RangeEdit(5, Array.emptyByteArray, Array.emptyByteArray)))
  }
