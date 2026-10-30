package org.aspen_ddp.aspen.client.internal.allocation

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FsifIndexBitmapSuite extends AnyFunSuite with Matchers:
  import FsifIndexBitmap.*

  private def seg(setBits: Int*): Array[Byte] =
    setBits.foldLeft(new Array[Byte](SegmentBytes))((a, b) => withBit(a, b, true))

  test("segmentsFor rounds up to whole 1024-bit segments") {
    segmentsFor(1) should be (1)
    segmentsFor(1024) should be (1)
    segmentsFor(1025) should be (2)
    segmentsFor(102400) should be (100)
  }

  test("zeroedContent has every segment key with 128 zero bytes") {
    val c = zeroedContent(3000)
    c.keySet should be ((0 until 3).map(segmentKey).toSet)
    c.values.foreach(v => v.bytes.toList should be (Array.fill(128)(0.toByte).toList))
  }

  test("usableBits limits the last segment to the index count") {
    usableBits(2, 3000) should be (952)
    usableBits(0, 3000) should be (1024)
  }

  test("freeBit skips set and reserved bits and never returns a bit at or past usableBits") {
    freeBit(seg(0, 1), 1024, _ == 2) should be (Some(3))
    freeBit(seg(0, 1, 2, 3, 4), 5, _ => false) should be (None)
  }

  test("withBit copies, and index/locate round-trip") {
    val orig = new Array[Byte](SegmentBytes)
    val upd = withBit(orig, 17, true)
    orig.forall(_ == 0) should be (true)
    isSet(upd, 17) should be (true)
    isSet(withBit(upd, 17, false), 17) should be (false)
    index(2, 17) should be (2065)
    locate(index(2, 17)) should be ((2, 17))
  }

  test("storePointer and indexOf round-trip and indexOf rejects bad pointers") {
    indexOf(storePointer(77), 100) should be (77)
    an [IllegalArgumentException] should be thrownBy indexOf(Array[Byte](0, 0, 0), 100)
    an [IllegalArgumentException] should be thrownBy indexOf(storePointer(100), 100)
    an [IllegalArgumentException] should be thrownBy indexOf(storePointer(-1), 100)
  }

  test("encodeIndices round-trips") {
    decodeIndices(encodeIndices(List(0, 5, 70000))) should be (List(0, 5, 70000))
  }
