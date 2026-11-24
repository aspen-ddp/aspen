package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32C

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class DeviceJournalFormatSuite extends FileBasedTests:
  import DeviceJournalFormat.*

  private val md = Metadata(ObjectRevision(TransactionId(new UUID(5, 6))), ObjectRefcount(3, 4), HLCTimestamp(99))
  private def oid(i: Int) = ObjectId(new UUID(i, ~i))
  private val jid = new UUID(11, 22)
  private val Salt = 0x1234567890abcdefL

  private def delta(i: Int): JournalRecord =
    val slice = Array.tabulate[Byte](16)(j => (i + j).toByte)
    val h = SegmentHeader.forSlice(ObjectType.Data, oid(i), i, md, DataBuffer(slice))
    JournalRecord.Delta(i, i + 1, h, List(DeltaRange(0, Array[Byte](1, 2, 3)), DeltaRange(10, Array[Byte](9))))

  private def mapping(i: Int): JournalRecord = JournalRecord.Mapping(i, i, oid(i))

  private def norm(r: JournalRecord): Any = r match
    case JournalRecord.Delta(e, s, h, rs) => ("D", e, s, h, rs.map(x => (x.offset, x.bytes.toList)))
    case m => m

  private def normAll(b: Batch): (Long, Vector[(UUID, Any)]) =
    (b.serial, b.records.map(t => (t.attachment, norm(t.record))))

  private val batch = Batch(42, Vector(
    TaggedRecord(new UUID(1, 1), mapping(1)),
    TaggedRecord(new UUID(2, 2), delta(2)),
    TaggedRecord(new UUID(1, 1), delta(3))))

  test("sizing round-trips and rejects a flipped byte"):
    val s = JournalSizing(jid, 4, 8192L * 4096)
    val enc = encodeSizing(s)
    decodeSizing(enc) shouldBe Some(s)
    for i <- enc.indices do
      val c = enc.clone()
      c(i) = (c(i) ^ 1).toByte
      decodeSizing(c) shouldBe None

  test("sizing requires at least three files and a 4096 multiple above the header"):
    an[IllegalArgumentException] should be thrownBy JournalSizing(jid, 2, 8192)
    an[IllegalArgumentException] should be thrownBy JournalSizing(jid, 3, 8191)
    an[IllegalArgumentException] should be thrownBy JournalSizing(jid, 3, 4096)

  test("a header round-trips only under its own journal id"):
    val h = FileHeader(jid, Salt, 17, 9)
    val enc = encodeHeader(h)
    enc.length shouldBe HeaderSize
    decodeHeader(enc, jid) shouldBe Some(h)
    decodeHeader(enc, new UUID(0, 1)) shouldBe None
    val c = enc.clone()
    c(30) = (c(30) ^ 1).toByte
    decodeHeader(c, jid) shouldBe None

  test("a batch round-trips at a 4096 boundary"):
    val enc = encodeBatch(jid, Salt, batch)
    (enc.length % 4096) shouldBe 0
    enc.length shouldBe batchSize(batch.records)
    val pre = new Array[Byte](4096)
    val file = pre ++ enc ++ new Array[Byte](4096)
    decodeBatchAt(file, 4096, jid, Salt) match
      case BatchRead.Ok(b, next) =>
        normAll(b) shouldBe normAll(batch)
        next shouldBe 4096 + enc.length
      case other => fail(s"unexpected $other")

  test("taggedRecordSize adds 16 bytes"):
    taggedRecordSize(delta(1)) shouldBe JournalFormat.recordSize(delta(1)) + 16

  test("a batch is Invalid under another salt or journal id"):
    val enc = encodeBatch(jid, Salt, batch)
    decodeBatchAt(enc, 0, jid, Salt + 1) shouldBe BatchRead.Invalid
    decodeBatchAt(enc, 0, new UUID(0, 9), Salt) shouldBe BatchRead.Invalid

  test("truncation at every byte of a batch is Invalid, never Unparseable"):
    val enc = encodeBatch(jid, Salt, batch)
    val used = 4 + ByteBuffer.wrap(enc).getInt(0) + 4
    for k <- 0 until used do
      decodeBatchAt(enc.take(k), 0, jid, Salt) shouldBe BatchRead.Invalid

  test("a CRC-valid batch with an unknown record type is Unparseable"):
    val rec = ByteBuffer.allocate(16 + 4 + 1).putLong(1).putLong(2).putInt(1).put(99.toByte).array()
    val body = ByteBuffer.allocate(8 + 4 + rec.length).putLong(5).putInt(1).put(rec).array()
    val buf = ByteBuffer.allocate(4 + body.length + 4)
    buf.putInt(body.length).put(body)
    val c = new CRC32C
    c.update(ByteBuffer.allocate(24).putLong(jid.getMostSignificantBits).putLong(jid.getLeastSignificantBits)
      .putLong(Salt).array())
    c.update(buf.array(), 0, 4 + body.length)
    buf.putInt(c.getValue.toInt)
    val file = buf.array().padTo(4096, 0.toByte)
    decodeBatchAt(file, 0, jid, Salt) match
      case BatchRead.Unparseable(_) => succeed
      case other => fail(s"unexpected $other")

  test("all-zero bytes are Invalid"):
    decodeBatchAt(new Array[Byte](8192), 0, jid, Salt) shouldBe BatchRead.Invalid
    decodeBatchAt(new Array[Byte](8192), 4096, jid, Salt) shouldBe BatchRead.Invalid
