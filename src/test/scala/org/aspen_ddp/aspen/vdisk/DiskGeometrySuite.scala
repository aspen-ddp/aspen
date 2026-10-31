package org.aspen_ddp.aspen.vdisk

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class DiskGeometrySuite extends AnyFunSuite with Matchers:

  val g = DiskGeometry(65536, 4096, 16384)

  test("blockObjectSizeFor rounds down to a multiple of the logical block size"):
    DiskGeometry.blockObjectSizeFor(5120152, 4096) should be (Some(5120000))
    DiskGeometry.blockObjectSizeFor(5120000, 4096) should be (Some(5120000))
    DiskGeometry.blockObjectSizeFor(4095, 4096) should be (None)
    DiskGeometry.blockObjectSizeFor(16384 + 50, 512) should be (Some(16384))

  test("blockCount covers the disk size"):
    g.blockCount should be (4L)
    DiskGeometry(65536 + 4096, 4096, 16384).blockCount should be (5L)

  test("segments split a request at block boundaries"):
    g.segments(12288, 8192) should be (List(BlockSegment(0, 12288, 4096, 0), BlockSegment(1, 0, 4096, 4096)))
    g.segments(0, 0) should be (Nil)

  test("validate"):
    g.validate(100, 4096) should be (Some(VDiskError.InvalidRequest("offset 100 is not aligned to 4096")))
    g.validate(0, 100).get shouldBe a [VDiskError.InvalidRequest]
    g.validate(-4096, 4096).get shouldBe a [VDiskError.InvalidRequest]
    g.validate(61440, 8192).get shouldBe a [VDiskError.OutOfRange]
    g.validate(0, 0) should be (None)
    g.validate(61440, 4096) should be (None)

  test("blocksFullyCovered"):
    g.blocksFullyCovered(8192, 32768) should be (List(1L))
    g.blocksFullyCovered(0, 65536) should be (List(0L, 1L, 2L, 3L))
    g.blocksFullyCovered(0, 16383) should be (Nil)

  test("blocksFullyCovered counts a short last block reached by the range"):
    val short = DiskGeometry(65536 + 4096, 4096, 16384)
    short.blocksFullyCovered(65536, 4096) should be (List(4L))
    short.blocksFullyCovered(49152, 20480) should be (List(3L, 4L))
    short.blocksFullyCovered(0, 65536 + 4096) should be (List(0L, 1L, 2L, 3L, 4L))
    short.blocksFullyCovered(65536, 0) should be (Nil)
    short.blocksFullyCovered(61440, 4096) should be (Nil)

  test("randomized segments tile the request"):
    val rnd = Random(12345)
    val geom = DiskGeometry(1L << 24, 512, 8192)
    for _ <- 0 until 1000 do
      val offset = rnd.nextInt((geom.size / 512).toInt) * 512L
      val maxBlocks = ((geom.size - offset) / 512).toInt
      val length = rnd.nextInt(math.min(maxBlocks, 100) + 1) * 512
      geom.validate(offset, length) should be (None)
      val segs = geom.segments(offset, length)
      segs.map(_.length).sum should be (length)
      var expected = 0
      for s <- segs do
        s.offsetInRequest should be (expected)
        expected += s.length
        s.length should be > 0
        s.offsetInBlock should be >= 0
        (s.offsetInBlock + s.length) should be <= geom.blockObjectSize
        (s.blockIndex * geom.blockObjectSize + s.offsetInBlock) should be (offset + s.offsetInRequest)
