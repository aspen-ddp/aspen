package org.aspen_ddp.aspen.common

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HLCTimestampSuite extends AnyFunSuite with Matchers:

  private def ts(wall: Long, logical: Int): HLCTimestamp = HLCTimestamp((wall << 16) | logical)

  test("the logical counter keeps all 16 bits"):
    ts(5, 0xFFFF).logical shouldBe 0xFFFF
    ts(5, 200).logical shouldBe 200
    ts(5, 200).wallTime shouldBe 5

  test("within one wall time, timestamps order by the whole logical counter"):
    // A Byte counter put 128..255 below 0..127 and wrapped every 256
    ts(5, 1000) should be > ts(5, 999)
    ts(5, 200) should be > ts(5, 100)
    ts(5, 256) should be > ts(5, 255)
    ts(5, 0xFFFF) should be > ts(5, 0)
    HLCTimestamp(2000) should be > HLCTimestamp(1000)

  test("a later wall time is later whatever the logical counters"):
    ts(6, 0) should be > ts(5, 0xFFFF)
    ts(5, 0xFFFF) should be < ts(6, 0)

  test("equal timestamps compare equal"):
    ts(5, 300).compare(ts(5, 300)) shouldBe 0

  test("ordering is the encoded value's, so it agrees with the 8-byte encoding"):
    val values = List(ts(0, 0), ts(0, 127), ts(0, 128), ts(0, 0xFFFF), ts(1, 0), ts(1, 1), ts(1L << 40, 3))
    values.sorted shouldBe values.sortBy(_.asLong)

  test("happensAfter a future timestamp increments its logical counter"):
    val future = ts(System.currentTimeMillis() + 60_000, 7)
    HLCTimestamp.happensAfter(future) shouldBe ts(future.wallTime, 8)

  test("happensAfter carries into the wall time when the logical counter is full"):
    // Bit 16 of the wall time is set: an OR of the overflowed counter into it carries nothing
    val wall = ((System.currentTimeMillis() + 60_000) | 1L)
    val full = ts(wall, 0xFFFF)
    val next = HLCTimestamp.happensAfter(full)
    next should be > full
    next shouldBe ts(wall + 1, 0)

  test("happensAfter a past timestamp is the current wall time"):
    val past = ts(1000, 5)
    val next = HLCTimestamp.happensAfter(past)
    next should be > past
    next.logical shouldBe 0
