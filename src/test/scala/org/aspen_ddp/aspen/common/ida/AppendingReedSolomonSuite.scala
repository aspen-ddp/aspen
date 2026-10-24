package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.common.DataBuffer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class AppendingReedSolomonSuite extends AnyFunSuite with Matchers:

  import ReedSolomonSuite.{Sizes, Generators, kSubsets, segmentsFor}

  private val Configs: List[AppendingReedSolomon] = List(
    AppendingReedSolomon(1, 1, 1, 8),
    AppendingReedSolomon(3, 2, 2, 8),
    AppendingReedSolomon(3, 2, 3, 8),
    AppendingReedSolomon(4, 4, 4, 16),
    AppendingReedSolomon(5, 3, 4, 64),
    AppendingReedSolomon(6, 4, 5, 8),
    AppendingReedSolomon(8, 5, 6, 64),
    AppendingReedSolomon(10, 7, 9, 8))

  private def setTrailer(shard: Array[Byte], value: Long): Array[Byte] =
    val copy = shard.clone()
    ByteBuffer.wrap(copy).putLong(copy.length - 8, value)
    copy

  test("construction rejects invalid parameters") {
    an [IllegalArgumentException] should be thrownBy AppendingReedSolomon(5, 3, 4, 7)
    an [IllegalArgumentException] should be thrownBy AppendingReedSolomon(5, 3, 4, AppendingReedSolomon.MaxCellSize + 1)
    an [IllegalArgumentException] should be thrownBy AppendingReedSolomon(5, 0, 4, 8)
    an [IllegalArgumentException] should be thrownBy AppendingReedSolomon(3, 4, 4, 8)
    an [IllegalArgumentException] should be thrownBy AppendingReedSolomon(128, 3, 4, 8)
    an [IllegalArgumentException] should be thrownBy AppendingReedSolomon(5, 3, 2, 8)
  }

  test("segment length is rows times cellSize") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    ida.rowBytes should be (24)
    ida.calculateEncodedSegmentLength(0) should be (8)
    ida.calculateEncodedSegmentLength(16) should be (8)
    ida.calculateEncodedSegmentLength(17) should be (16)
    ida.calculateEncodedSegmentLength(40) should be (16)
    ida.calculateEncodedSegmentLength(41) should be (24)
  }

  test("encode then restore round trips") {
    for
      ida <- Configs
      n <- Sizes
      (gname, gen) <- Generators
    do
      val content = gen(n)
      val shards = ida.encode(content)
      shards.length should be (ida.width)
      shards.foreach(_.length should be (ida.calculateEncodedSegmentLength(n)))
      withClue(s"$ida n=$n gen=$gname") {
        ida.restoreArray(segmentsFor(shards, 0 until ida.width)).sameElements(content) should be (true)
      }
  }

  test("restores from every k-subset of slices") {
    for
      ida <- Configs.filter(_.width <= 6)
      n <- List(0, 1, ida.rowBytes - 9, ida.rowBytes - 8, ida.rowBytes - 7, 3 * ida.rowBytes + 5, 1000)
      if n >= 0
    do
      val content = Array.tabulate(n)(i => (i * 7 + 3).toByte)
      val shards = ida.encode(content)
      for subset <- kSubsets(ida.width, ida.restoreThreshold) do
        withClue(s"$ida n=$n subset=$subset") {
          ida.restoreArray(segmentsFor(shards, subset)).sameElements(content) should be (true)
        }
  }

  test("data store k-1 holds the trailer in plain bytes") {
    for
      ida <- Configs
      n <- Sizes
    do
      val shards = ida.encode(Array.tabulate(n)(i => i.toByte))
      val last = shards(ida.restoreThreshold - 1)
      withClue(s"$ida n=$n") {
        ByteBuffer.wrap(last).getLong(last.length - 8) should be (n.toLong)
      }
  }

  test("layout places stream byte p at its row and cell") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    val shards = ida.encode(Array.tabulate(100)(_.toByte))
    for p <- 0 until 100 do
      val shard = shards((p % 24) / 8)
      shard((p / 24) * 8 + p % 8) should be (p.toByte)
  }

  test("restore rejects a corrupt trailer") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    val shards = ida.encode(Array.tabulate(50)(_.toByte)) // 3 rows of 24 bytes
    val rows = ida.rowsFor(50)
    rows should be (3)

    def restoreWith(trailer: Long) =
      val bad = shards.clone()
      bad(2) = setTrailer(shards(2), trailer)
      ida.restoreArray(segmentsFor(bad, 0 until 5))

    an [IDARestoreError] should be thrownBy restoreWith(rows.toLong * ida.rowBytes)
    an [IDARestoreError] should be thrownBy restoreWith(0L)
    an [IDARestoreError] should be thrownBy restoreWith(-1L)
  }

  test("encodeInto matches encode") {
    for
      ida <- Configs
      n <- Sizes
    do
      val content = Array.tabulate(n)(i => (i * 13 + 1).toByte)
      val expected = ida.encode(content)
      val len = ida.calculateEncodedSegmentLength(n)
      val bbs = Array.fill(ida.width)(ByteBuffer.allocate(len))
      ida.encodeInto(DataBuffer(content), bbs)
      withClue(s"$ida n=$n") {
        for i <- 0 until ida.width do
          bbs(i).position() should be (len)
          bbs(i).array().sameElements(expected(i)) should be (true)
      }
  }

  test("serializeIDAType round trips including cellSize") {
    for ida <- List(AppendingReedSolomon(5, 3, 4, 64), AppendingReedSolomon(3, 2, 3, 4096),
      AppendingReedSolomon(3, 2, 3, AppendingReedSolomon.MaxCellSize)) do
      val bb = ByteBuffer.allocate(ida.getSerializedIDATypeLength)
      ida.getSerializedIDATypeLength should be (6)
      ida.serializeIDAType(bb)
      bb.position() should be (6)
      bb.flip()
      IDA.deserializeIDAType(bb) should be (ida)
  }

  private val RangedConfigs: List[AppendingReedSolomon] = Configs ++ List(
    AppendingReedSolomon(4, 3, 3, 8),
    AppendingReedSolomon(7, 4, 5, 9),
    AppendingReedSolomon(3, 3, 3, 13))

  private def checkRanged(ida: AppendingReedSolomon, old: Array[Byte], writes: List[(Int, Array[Byte])]): Unit =
    import RangedWriteDeltaSuite.{applyDeltas, applyWrites}
    val expected = ida.encode(applyWrites(old, writes))
    val actual = applyDeltas(ida, old, writes)
    withClue(s"$ida len=${old.length} writes=${writes.map((o, b) => (o, b.length))}") {
      actual.indices.foreach(i => actual(i).toSeq should be (expected(i).toSeq))
    }

  test("rangedWriteDeltas: delta-applied slices equal a fresh encode for edit shapes") {
    import RangedWriteDeltaSuite.{bytes, randomWrites}
    val rnd = new scala.util.Random(11)
    for ida <- RangedConfigs do
      val rb = ida.rowBytes
      for len <- List(0, 1, rb - 9, rb - 8, rb - 7, rb, rb + 1, 3 * rb - 8, 4 * rb + 5, 10 * rb + 3) do
        if len >= 0 then
          val old = bytes(len, rnd)
          def at(off: Int, n: Int): Option[(Int, Array[Byte])] =
            if off >= 0 && n > 0 then Some((off, bytes(n, rnd))) else None
          val shapes: List[List[(Int, Array[Byte])]] = List(
            at(1, 3),                            // within a row
            at(rb - 2, 5),                       // spanning rows
            at(rb - 3, 3),                       // ending exactly on a row boundary
            at(2 * rb - 1, rb + 2),              // spanning several rows
            at(len - 2, 10),                     // extending past the end
            at(len, 1),                          // starting at the old length
            at(len, rb - 7 - len % rb),          // one byte past 8-byte room in the final row, so the trailer moves
            at(len + 2 * rb + 3, 4),             // beyond the length with a gap
            at(0, len + 1),                      // rewrite everything and extend
            at(len - 1, 1)                       // last byte, no extension
          ).flatten.map(List(_)) ++ List(
            List(at(0, 2), at(rb + 1, 1), at(len + 1, 3)).flatten,
            List(at(len, 2), at(len + 5 * rb, 2)).flatten)
          def disjoint(w: List[(Int, Array[Byte])]): Boolean =
            w.sortBy(_._1).sliding(2).forall {
              case List(a, b) => a._1 + a._2.length <= b._1
              case _ => true
            }
          shapes.filter(w => w.nonEmpty && disjoint(w)).foreach(w => checkRanged(ida, old, w))
          (0 until 15).foreach(_ => checkRanged(ida, old, randomWrites(rnd, len + 3 * rb, 4)))
  }

  test("rangedWriteDeltas: a non-extending write keeps the slice length") {
    import RangedWriteDeltaSuite.{bytes, edit}
    val rnd = new scala.util.Random(12)
    for ida <- RangedConfigs do
      val old = bytes(5 * ida.rowBytes + 3, rnd)
      val deltas = ida.rangedWriteDeltas(old.length, List(edit(old, ida.rowBytes + 1, bytes(4, rnd))))
      deltas.foreach(d => d.newSliceLength should be (d.expectedSliceLength))
      deltas(ida.restoreThreshold - 1).expectedTail.map(_.toSeq) should be (
        Some(ByteBuffer.allocate(8).putLong(old.length.toLong).array().toSeq))
  }

  test("rangedWriteDeltas: distant edits send no bytes from the rows between") {
    import RangedWriteDeltaSuite.{bytes, edit}
    val rnd = new scala.util.Random(13)
    for ida <- RangedConfigs do
      val rb = ida.rowBytes
      val old = bytes(200 * rb, rnd)
      // Two edits, each within one row, 150 rows apart, length unchanged: two affected rows
      val writes = List((2 * rb + 1, 3), (152 * rb + 1, 3)).map((o, n) => edit(old, o, bytes(n, rnd)))
      val deltas = ida.rangedWriteDeltas(old.length, writes)
      deltas.foreach { d =>
        val dataBytes = d.regions.map(_.bytes.length).sum
        dataBytes should be <= (2 * ida.cellSize)
        d.encode.size should be <= (18 + 2 * 8 + 2 * ida.cellSize + 8)
      }
      // The same edits extending the object also touch the old and new trailer rows only
      val grow = writes :+ RangeEdit(old.length + 3 * rb, bytes(2, rnd), Array.emptyByteArray)
      ida.rangedWriteDeltas(old.length, grow).foreach { d =>
        d.regions.map(_.bytes.length).sum should be <= (5 * ida.cellSize)
      }
  }
