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
    AppendingReedSolomon(3, 1, 2, 8),
    AppendingReedSolomon(3, 2, 3, 8),
    AppendingReedSolomon(4, 4, 4, 16),
    AppendingReedSolomon(5, 3, 4, 64),
    AppendingReedSolomon(6, 3, 5, 8),
    AppendingReedSolomon(8, 4, 6, 64),
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
