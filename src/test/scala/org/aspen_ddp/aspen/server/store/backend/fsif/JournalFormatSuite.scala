package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class JournalFormatSuite extends FileBasedTests:
  import JournalFormat.Scan

  private val md = Metadata(ObjectRevision(TransactionId(new UUID(5, 6))), ObjectRefcount(3, 4), HLCTimestamp(99))
  private def oid(i: Int) = ObjectId(new UUID(i, ~i))

  private def delta(i: Int): JournalRecord =
    val slice = Array.tabulate[Byte](16)(j => (i + j).toByte)
    val h = SegmentHeader.forSlice(ObjectType.Data, oid(i), i, md, DataBuffer(slice))
    JournalRecord.Delta(i, i + 1, h, List(DeltaRange(0, Array[Byte](1, 2, 3)), DeltaRange(10, Array[Byte](9))))

  private def mapping(i: Int): JournalRecord = JournalRecord.Mapping(i, if i % 2 == 0 then -1 else i, oid(i))

  // Array-bearing records need structural comparison
  private def norm(r: JournalRecord): Any = r match
    case JournalRecord.Delta(e, s, h, rs) => ("D", e, s, h, rs.map(x => (x.offset, x.bytes.toList)))
    case m => m

  private val Salt = 0x1234567890abcdefL
  private val batches = List(List(mapping(1), delta(2)), List(delta(3)), List(mapping(4), mapping(5), delta(6)))

  private def file(bs: List[List[JournalRecord]], seq: Long = 7, salt: Long = Salt): Array[Byte] =
    bs.zipWithIndex.foldLeft(JournalFormat.encodeHeader(seq, salt)): (acc, b) =>
      acc ++ JournalFormat.encodeBatch(salt, b._2, b._1)

  test("round trip"):
    JournalFormat.scan(file(batches)) match
      case Scan.Valid(seq, salt, recs, len, next) =>
        seq shouldBe 7
        salt shouldBe Salt
        recs.map(norm) shouldBe batches.flatten.map(norm).toVector
        len shouldBe file(batches).length
        next shouldBe 3
      case other => fail(other.toString)

  test("truncation at every offset inside the last batch"):
    val head = file(batches.take(2))
    val full = file(batches)
    for cut <- head.length until full.length do
      JournalFormat.scan(full.take(cut)) match
        case Scan.Valid(_, _, recs, len, next) =>
          recs.map(norm) shouldBe batches.take(2).flatten.map(norm).toVector
          len shouldBe head.length
          next shouldBe 2
        case other => fail(s"cut $cut: $other")

  test("flip in a middle batch is corrupt, in the last batch is a torn tail"):
    val full = file(batches)
    val b0 = JournalFormat.HeaderSize + JournalFormat.encodeBatch(Salt, 0, batches(0)).length
    val mid = full.clone()
    mid(b0 + 14) = (mid(b0 + 14) ^ 1).toByte
    JournalFormat.scan(mid) shouldBe a[Scan.Corrupt]
    val last = full.clone()
    last(full.length - 6) = (last(full.length - 6) ^ 1).toByte
    JournalFormat.scan(last) match
      case Scan.Valid(_, _, recs, len, next) =>
        recs.size shouldBe 3
        len shouldBe file(batches.take(2)).length
        next shouldBe 2
      case other => fail(other.toString)

  test("garbage files"):
    for n <- List(0, 7, 28) do
      JournalFormat.scan(Array.fill[Byte](n)(0x55)) shouldBe Scan.Empty
    JournalFormat.scan(Array.fill[Byte](29)(0x55)) shouldBe a[Scan.Corrupt]
    JournalFormat.scan(Array.fill[Byte](500)(0x55)) shouldBe a[Scan.Corrupt]

  test("header only"):
    JournalFormat.scan(JournalFormat.encodeHeader(5, Salt)) shouldBe Scan.Valid(5, Salt, Vector(), 28, 0)

  test("empty batch is allowed"):
    JournalFormat.scan(file(List(Nil, List(mapping(1))))) match
      case Scan.Valid(_, _, recs, _, next) =>
        recs.size shouldBe 1
        next shouldBe 2
      case other => fail(other.toString)

  test("wrong batch number with valid CRC"):
    val f = JournalFormat.encodeHeader(1, Salt) ++ JournalFormat.encodeBatch(Salt, 1, Nil)
    JournalFormat.scan(f) shouldBe a[Scan.Corrupt]
    JournalFormat.scan(JournalFormat.encodeHeader(1, Salt) ++ JournalFormat.encodeBatch(Salt, 5, Nil)) shouldBe
      Scan.Valid(1, Salt, Vector(), 28, 0)

  test("scan never throws on random mutations"):
    val full = file(batches)
    val rnd = new scala.util.Random(42)
    for _ <- 0 until 2000 do
      val a = full.clone()
      for _ <- 0 until 1 + rnd.nextInt(3) do a(rnd.nextInt(a.length)) = rnd.nextInt().toByte
      JournalFormat.scan(a.take(rnd.nextInt(a.length + 1)))

  private def batchWith(n: Long, rec: Array[Byte]): Array[Byte] =
    val bb = java.nio.ByteBuffer.allocate(4 + 8 + rec.length + 4)
    bb.putInt(8 + rec.length).putLong(n).put(rec)
    val c = new java.util.zip.CRC32
    c.update(java.nio.ByteBuffer.allocate(8).putLong(Salt).array())
    c.update(bb.array(), 0, 12 + rec.length)
    bb.putInt(c.getValue.toInt)
    bb.array()

  test("crc-valid batch with bad records is corrupt"):
    val hdr = JournalFormat.encodeHeader(1, Salt)
    def rec(tpe: Int, payload: Array[Byte]) =
      java.nio.ByteBuffer.allocate(5 + payload.length).putInt(1 + payload.length).put(tpe.toByte).put(payload).array()
    val badSlot = java.nio.ByteBuffer.allocate(24).putInt(1).putInt(-2).array()
    val shortMap = new Array[Byte](23)
    val longMap = new Array[Byte](25)
    for r <- List(rec(9, Array.empty), rec(1, badSlot), rec(1, shortMap), rec(1, longMap), Array[Byte](0, 0, 0, 9, 1)) do
      JournalFormat.scan(hdr ++ batchWith(0, r)) shouldBe a[Scan.Corrupt]
    // delta with a range past the slice end
    val h = delta(1).asInstanceOf[JournalRecord.Delta].header
    val bad = JournalRecord.Delta(1, 2, h, List(DeltaRange(10, new Array[Byte](7))))
    JournalFormat.scan(hdr ++ JournalFormat.encodeBatch(Salt, 0, List(bad))) shouldBe a[Scan.Corrupt]
    val ok = JournalRecord.Delta(1, 2, h, List(DeltaRange(10, new Array[Byte](6))))
    JournalFormat.scan(hdr ++ JournalFormat.encodeBatch(Salt, 0, List(ok))) shouldBe a[Scan.Valid]

  test("torn batch embedding a later batch (different salt) in user data stays a torn tail"):
    val head = file(batches.take(2))
    val inner = JournalFormat.encodeBatch(Salt + 1, 3, List(mapping(1)))
    val h = delta(9).asInstanceOf[JournalRecord.Delta].header.copy(sliceLength = inner.length)
    val carrier = JournalRecord.Delta(1, 2, h, List(DeltaRange(0, inner)))
    val torn = JournalFormat.encodeBatch(Salt, 2, List(carrier))
    val idx = torn.indexOfSlice(inner)
    idx should be > 0
    for v <- List(torn.take(idx + inner.length), torn.take(torn.length - 1)) do
      JournalFormat.scan(head ++ v) match
        case Scan.Valid(_, _, recs, len, next) =>
          recs.size shouldBe 3
          len shouldBe head.length
          next shouldBe 2
        case other => fail(other.toString)

  test("mid-journal batch with a rotted len (larger, in bounds) is corrupt"):
    val full = file(batches)
    val b0 = JournalFormat.HeaderSize
    val rotted = full.clone()
    val len = java.nio.ByteBuffer.wrap(rotted).getInt(b0)
    java.nio.ByteBuffer.wrap(rotted).putInt(b0, len + 20)
    b0 + 4 + len + 20 + 4 should be <= rotted.length
    JournalFormat.scan(rotted) shouldBe a[Scan.Corrupt]
