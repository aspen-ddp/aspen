package org.aspen_ddp.aspen.common.objects

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ByteRangesSuite extends AnyFunSuite with Matchers:

  test("ranges come in request order followed by the tail piece"):
    ByteRanges(List((10, 5), (0, 3)), 4).pieces(20) should be (List((10, 15), (0, 3), (16, 20)))

  test("a range past the end is an empty piece at sliceSize"):
    ByteRanges(List((30, 5)), 0).pieces(20) should be (List((20, 20), (20, 20)))

  test("negative offset and length are clamped"):
    ByteRanges(List((-5, 8), (4, -3)), 0).pieces(20) should be (List((0, 3), (4, 4), (20, 20)))

  test("a zero length range is empty at its offset"):
    ByteRanges(List((7, 0)), 0).pieces(20) should be (List((7, 7), (20, 20)))

  test("Int.MaxValue length runs to the end of the slice"):
    ByteRanges(List((Int.MaxValue, Int.MaxValue), (5, Int.MaxValue)), 0).pieces(20) should be (
      List((20, 20), (5, 20), (20, 20)))

  test("a tail longer than the slice covers the whole slice"):
    ByteRanges(Nil, 100).pieces(20) should be (List((0, 20)))

  test("tail 0 gives an empty tail piece at sliceSize"):
    ByteRanges(List((1, 2)), 0).pieces(20) should be (List((1, 3), (20, 20)))

  test("an empty range list gives only the tail piece"):
    ByteRanges(Nil, 6).pieces(20) should be (List((14, 20)))
    ByteRanges(Nil, 0).pieces(0) should be (List((0, 0)))
