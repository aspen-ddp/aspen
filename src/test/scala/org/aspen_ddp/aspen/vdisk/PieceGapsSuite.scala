package org.aspen_ddp.aspen.vdisk

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PieceGapsSuite extends AnyFunSuite with Matchers:

  /** Sorted, non-overlapping pieces that count how many of their elements are looked at */
  private final class Counting(underlying: Vector[(Int, Array[Byte])]) extends IndexedSeq[(Int, Array[Byte])]:
    var touched = 0
    def length: Int = underlying.length
    def apply(i: Int): (Int, Array[Byte]) =
      touched += 1
      underlying(i)
    override def iterator: Iterator[(Int, Array[Byte])] = underlying.iterator.map: p =>
      touched += 1
      p

  private def pieces(ranges: (Int, Int)*): Vector[(Int, Array[Byte])] =
    ranges.map((o, len) => (o, new Array[Byte](len))).toVector

  test("gaps finds the holes of a range, at its edges and between pieces"):
    val ps = pieces((10, 10), (30, 5), (40, 20))
    VirtualDisk.gaps(0, 70, ps) should be(List((0, 10), (20, 30), (35, 40), (60, 70)))
    VirtualDisk.gaps(12, 18, ps) should be(Nil)
    VirtualDisk.gaps(15, 45, ps) should be(List((20, 30), (35, 40)))
    VirtualDisk.gaps(20, 30, ps) should be(List((20, 30)))
    VirtualDisk.gaps(0, 5, Vector.empty) should be(List((0, 5)))
    VirtualDisk.covers(ps, 40, 60) should be(true)
    VirtualDisk.covers(ps, 39, 60) should be(false)

  test("gaps of two piece sequences that overlap each other are what neither covers"):
    val clean = pieces((0, 10), (20, 10))
    val overlay = pieces((5, 10), (28, 4))
    VirtualDisk.gaps(0, 40, clean, overlay) should be(List((15, 20), (32, 40)))
    VirtualDisk.gaps(0, 30, clean, overlay) should be(List((15, 20)))

  test("gaps of a block held as many small pieces looks only at the pieces near the range"):
    // A block filled by 4 KiB random-read fetches: thousands of adjacent pieces, which a read used to scan and sort
    val n = 1280
    val ps = Counting(pieces((0 until n).map(i => (i * 4096, 4096))*))
    VirtualDisk.gaps(700 * 4096, 701 * 4096, ps) should be(Nil)
    ps.touched should be < 40

  test("assemble copies the committed pieces under the segment, then the overlay over them"):
    val clean = Vector((0, Array.fill[Byte](4)(1)), (8, Array.fill[Byte](4)(2)))
    val fetched = List((4, Array.fill[Byte](4)(3)))
    val overlay = SegmentSnapshot(Vector((6, Array.fill[Byte](3)(9))), 1)
    VirtualDisk.assemble(BlockSegment(0, 2, 12, 0), clean, fetched, overlay).toSeq should be(
      Seq[Byte](1, 1, 3, 3, 9, 9, 9, 2, 2, 2, 0, 0))

  test("assemble of a block held as many small pieces looks only at the pieces under the segment"):
    val n = 1280
    val ps = Counting(pieces((0 until n).map(i => (i * 4096, 4096))*))
    VirtualDisk.assemble(BlockSegment(0, 700 * 4096, 4096, 0), ps, Nil, SegmentSnapshot(Vector.empty, 0))
    ps.touched should be < 40
