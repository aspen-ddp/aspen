package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import scala.util.Random

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class FormatsSuite extends FileBasedTests:

  private def splitMix(seed: Int): Long =
    var z = seed * 0x9e3779b97f4a7c15L + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  private val md = Metadata(ObjectRevision(TransactionId(new UUID(5, 6))), ObjectRefcount(3, 4), HLCTimestamp(99))
  private val oid = ObjectId(new UUID(0x1122334455667788L, 0x99aabbccddeeff00L))

  private def header(slice: Array[Byte]) =
    SegmentHeader.forSlice(ObjectType.KeyValue, oid, 7, md, DataBuffer(slice))

  test("layout stride"):
    FsifLayout(1024, 8).stride shouldBe 4096L
    FsifLayout(4096, 8).stride shouldBe 8192L
    FsifLayout(4096, 8).slotOffset(3) shouldBe 3 * 8192L

  test("header round trip"):
    for t <- ObjectType.values.toList do
      val slice = Array.tabulate[Byte](100)(_.toByte)
      val h = SegmentHeader.forSlice(t, oid, -1, md, DataBuffer(slice))
      val enc = SegmentHeader.encode(h)
      enc.length shouldBe 61
      SegmentHeader.decode(enc) shouldBe Some(h)
      SegmentHeader.verify(h, slice) shouldBe true

  test("header decode rejects short and bad type"):
    val enc = SegmentHeader.encode(header(Array[Byte](1, 2)))
    SegmentHeader.decode(enc.take(60)) shouldBe None
    SegmentHeader.decode(Array.empty[Byte]) shouldBe None
    val bad = enc.clone()
    bad(0) = 9
    SegmentHeader.decode(bad) shouldBe None

  test("verify fails on corrupt slice or header"):
    val slice = Array.tabulate[Byte](100)(_.toByte)
    val h = header(slice)
    val s2 = slice.clone()
    s2(50) = (s2(50) ^ 1).toByte
    SegmentHeader.verify(h, s2) shouldBe false
    SegmentHeader.verify(h, slice.take(99)) shouldBe false
    val enc = SegmentHeader.encode(h)
    for i <- 0 until 57 do
      val e = enc.clone()
      e(i) = (e(i) ^ 1).toByte
      SegmentHeader.decode(e).foreach(d => SegmentHeader.verify(d, slice) shouldBe false)

  private def sampleMap(n: Int, seed: Int) =
    val r = new Random(splitMix(seed))
    val m = IndexMap.empty(n)
    for i <- 0 until n do
      if r.nextBoolean() then
        m.slots(i) = r.nextInt(n)
        m.ids(i) = ObjectId(new UUID(r.nextLong(), r.nextLong()))
    m

  private def same(a: IndexMap, b: IndexMap) =
    a.slots.toList shouldBe b.slots.toList
    a.ids.toList shouldBe b.ids.toList

  test("empty map is unmapped with zero ids"):
    val m = IndexMap.empty(4)
    m.slots.toList shouldBe List(-1, -1, -1, -1)
    m.ids.toList shouldBe List.fill(4)(ObjectId(new UUID(0, 0)))

  test("index round trip and copy"):
    val m = sampleMap(64, 1)
    val enc = IndexFile.encode(123456789012L, m)
    enc.length shouldBe 8 + 8 + 64 * 20 + 4
    val (seq, d) = IndexFile.decode(enc, 64).get
    seq shouldBe 123456789012L
    same(d, m)
    val c = m.copy()
    same(c, m)
    c.slots(0) = -7
    same(m, sampleMap(64, 1))

  test("index decode rejects malformed input"):
    val enc = IndexFile.encode(1L, sampleMap(16, 2))
    IndexFile.decode(enc.take(enc.length - 1), 16) shouldBe None
    IndexFile.decode(enc :+ 0.toByte, 16) shouldBe None
    IndexFile.decode(enc, 17) shouldBe None
    IndexFile.decode(Array.empty[Byte], 16) shouldBe None
    val flipped = enc.clone()
    flipped(30) = (flipped(30) ^ 1).toByte
    IndexFile.decode(flipped, 16) shouldBe None
    val magic = enc.clone()
    magic(0) = (magic(0) ^ 1).toByte
    IndexFile.decode(magic, 16) shouldBe None

  test("index decode rejects slot below -1 even with valid crc"):
    val m = IndexMap.empty(4)
    m.slots(2) = -2
    IndexFile.decode(IndexFile.encode(1L, m), 4) shouldBe None
