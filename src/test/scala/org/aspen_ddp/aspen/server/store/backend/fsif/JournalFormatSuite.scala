package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import scala.util.Try

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

/** The record codec the device journal carries (DeviceJournalFormatSuite covers batches and their framing) */
class JournalFormatSuite extends FileBasedTests:
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

  /** Parses an encoded record (length prefix included) */
  private def parse(bytes: Array[Byte]): JournalRecord = JournalFormat.parseRecord(bytes, 4, bytes.length)

  test("round trip, with recordSize the encoded size"):
    val noRanges = JournalRecord.Delta(1, 2, delta(1).asInstanceOf[JournalRecord.Delta].header, Nil)
    for r <- List(mapping(1), mapping(2), delta(3), noRanges) do
      val enc = JournalFormat.encodeRecord(r)
      enc.length shouldBe JournalFormat.recordSize(r)
      norm(parse(enc)) shouldBe norm(r)

  test("malformed records are rejected"):
    def rec(tpe: Int, payload: Array[Byte]) =
      java.nio.ByteBuffer.allocate(5 + payload.length).putInt(1 + payload.length).put(tpe.toByte).put(payload).array()
    val badSlot = java.nio.ByteBuffer.allocate(24).putInt(1).putInt(-2).array()
    for r <- List(rec(9, Array.empty), rec(1, badSlot), rec(1, new Array[Byte](23)), rec(1, new Array[Byte](25))) do
      Try(parse(r)).isFailure shouldBe true
    // A Delta with a range past the slice end
    val h = delta(1).asInstanceOf[JournalRecord.Delta].header
    val pastEnd = JournalRecord.Delta(1, 2, h, List(DeltaRange(10, new Array[Byte](7))))
    Try(parse(JournalFormat.encodeRecord(pastEnd))).isFailure shouldBe true
    val atEnd = JournalRecord.Delta(1, 2, h, List(DeltaRange(10, new Array[Byte](6))))
    Try(parse(JournalFormat.encodeRecord(atEnd))).isSuccess shouldBe true
