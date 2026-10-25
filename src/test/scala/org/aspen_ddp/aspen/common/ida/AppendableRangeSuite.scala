package org.aspen_ddp.aspen.common.ida

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class AppendableRangeSuite extends AnyFunSuite with Matchers:

  private val Configs: List[RandomAccessReedSolomon] = List(
    RandomAccessReedSolomon(5, 3, 4, 8),
    RandomAccessReedSolomon(3, 2, 3, 16))

  private val Sizes = List(0, 23, 24, 100, 777)

  private def content(n: Int): Array[Byte] = Array.tabulate(n)(i => (i * 31 + n).toByte)

  /** Simulates stores returning [start, end) of each slice, using the last `width` slices to mix in parity */
  private def window(ida: RandomAccessReedSolomon, slices: Array[Array[Byte]], start: Int, end: Int) =
    slices.indices.takeRight(ida.restoreThreshold).map(i => (i.toByte, slices(i).slice(start, end))).toList

  test("every row-aligned window decodes to the matching logical bytes") {
    for ida <- Configs; n <- Sizes do
      val c = content(n)
      val slices = ida.encode(c)
      val rows = ida.rowsFor(n)
      for startRow <- 0 until rows; endRow <- (startRow + 1) to rows do
        val r = ida.restoreRange(startRow * ida.cellSize, rows * ida.cellSize,
          window(ida, slices, startRow * ida.cellSize, endRow * ida.cellSize))
        r.logicalOffset should be (startRow * ida.rowBytes)
        // A non-final window may extend past the content into zero fill
        val padded = c.padTo(rows * ida.rowBytes, 0.toByte)
        r.data should be (padded.slice(r.logicalOffset, r.logicalOffset + r.data.length))
        // Trimmed to a request ending at the window end, no zero fill may be accepted as data
        val reqEnd = math.min(endRow * ida.rowBytes, n)
        val trimmed = r.data.slice(0, math.max(0, reqEnd - r.logicalOffset))
        trimmed should be (c.slice(r.logicalOffset, math.max(r.logicalOffset, reqEnd)))
        if endRow == rows then
          r.objectLength should be (Some(n))
        else
          r.objectLength should be (None)
          r.data.length should be ((endRow - startRow) * ida.rowBytes)
  }

  test("windows ending past the logical end return only real bytes") {
    val ida = Configs.head
    val c = content(100)
    val slices = ida.encode(c)
    val rows = ida.rowsFor(100)
    val start = (rows - 1) * ida.cellSize
    val r = ida.restoreRange(start, rows * ida.cellSize, window(ida, slices, start, rows * ida.cellSize))
    r.data.length should be (100 - r.logicalOffset)
  }

  test("length probe of the final cellSize bytes yields the length") {
    for ida <- Configs; n <- Sizes do
      val slices = ida.encode(content(n))
      val size = slices(0).length
      ida.lengthProbeBytes should be (ida.cellSize)
      val r = ida.restoreRange(size - ida.lengthProbeBytes, size, window(ida, slices, size - ida.lengthProbeBytes, size))
      r.objectLength should be (Some(n))
    val rep = Replication(3, 2)
    rep.lengthProbeBytes should be (0)
  }

  test("sliceRangeFor covers the requested logical range") {
    val rnd = Random(7)
    for ida <- Configs do
      val n = 777
      val c = content(n)
      val slices = ida.encode(c)
      val size = slices(0).length
      for _ <- 0 until 200 do
        val offset = rnd.nextInt(n)
        val length = 1 + rnd.nextInt(n - offset)
        val (s, e) = ida.sliceRangeFor(offset, length)
        ida.sliceOffsetFor(offset) should be (s)
        val r = ida.restoreRange(s, size, window(ida, slices, s, math.min(e, size)))
        r.logicalOffset should be <= offset
        (r.logicalOffset + r.data.length) should be >= (offset + length)
        r.data.slice(offset - r.logicalOffset, offset - r.logicalOffset + length) should be (c.slice(offset, offset + length))
  }

  test("sliceRangeFor does not overflow for huge lengths with k=1") {
    val ida = RandomAccessReedSolomon(1, 1, 1, 8)
    val c = content(100)
    val slices = ida.encode(c)
    val size = slices(0).length
    for (offset, length) <- List((0, Int.MaxValue), (50, Int.MaxValue - 50), (Int.MaxValue - 10, 10),
                                 (Int.MaxValue - 1, 1), (Int.MaxValue, 0), (0, Int.MaxValue - 1)) do
      val (s, e) = ida.sliceRangeFor(offset, length)
      s should be >= 0
      e should be >= s
    val (s, e) = ida.sliceRangeFor(0, Int.MaxValue)
    s should be (0)
    e should be (Int.MaxValue)
    val r = ida.restoreRange(s, size, window(ida, slices, s, math.min(e, size)))
    r.objectLength should be (Some(100))
    r.data.slice(0, 100) should be (c)
    // Replication shares the hazard
    val (rs, re) = Replication(3, 2).sliceRangeFor(Int.MaxValue - 5, Int.MaxValue)
    rs should be (Int.MaxValue - 5)
    re should be (Int.MaxValue)
  }

  test("Replication range restore is the identity") {
    val ida = Replication(3, 2)
    ida.sliceRangeFor(10, 20) should be ((10, 30))
    ida.sliceOffsetFor(10) should be (10)
    val seg = Array[Byte](1, 2, 3)
    val r = ida.restoreRange(10, 50, List((0.toByte, seg)))
    r should be (RestoredRange(10, Array[Byte](1, 2, 3), Some(50)))
    r.hashCode should be (RestoredRange(10, Array[Byte](1, 2, 3), Some(50)).hashCode)
  }

  test("restoreRange rejects a misaligned start") {
    val ida = Configs.head
    val slices = ida.encode(content(100))
    an [IDARestoreError] should be thrownBy
      ida.restoreRange(4, slices(0).length, window(ida, slices, 4, 16))
  }

  test("bounded reads ending beyond the logical end never return zero fill as data") {
    for ida <- Configs; n <- List(0, 20, 23, 24, 100, 777) do
      val c = content(n)
      val slices = ida.encode(c)
      val size = slices(0).length
      for offset <- 0 to n; length <- (n - offset + 1) to (n - offset + ida.rowBytes) do
        val (s, e) = ida.sliceRangeFor(offset, length)
        val end = math.min(e, size)
        val r = ida.restoreRange(s, size, window(ida, slices, s, end))
        val from = offset - r.logicalOffset
        val got = r.data.slice(from, from + length)
        got should be (c.slice(offset, n))
  }

  test("an empty window at the slice end returns an empty, length-unknown range") {
    val ida = Configs.head
    val slices = ida.encode(content(100))
    val size = slices(0).length
    val r = ida.restoreRange(size, size, window(ida, slices, size, size))
    r should be (RestoredRange((size / ida.cellSize) * ida.rowBytes, Array.emptyByteArray, None))
  }
