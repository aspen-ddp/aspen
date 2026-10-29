package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32

import org.aspen_ddp.aspen.common.objects.ObjectId

final case class DeltaRange(offset: Int, bytes: Array[Byte])

enum JournalRecord:
  case Mapping(ext: Int, slot: Int, objectId: ObjectId)
  case Delta(ext: Int, slot: Int, header: SegmentHeader, ranges: List[DeltaRange])

/** Header: "FSIFJRNL"(8) seq(8) salt(8) CRC(4 over the first 24 bytes).
  * Batch: len(4) batchNo(8) records... crc(4); len counts batchNo + records, crc covers salt(8, not stored) + len +
  * batchNo + records. The salt keeps user data embedded in a torn batch from ever passing as a later batch.
  * Record: len(4) type(1) payload; len counts type + payload.
  * Mapping (1): ext 4, slot 4, msb 8, lsb 8. Delta (2): ext 4, slot 4, header 61, rangeCount 4, (offset 4, length 4, bytes)*
  */
object JournalFormat:
  val HeaderSize = 28
  private val Magic = "FSIFJRNL".getBytes("US-ASCII")
  private val MappingType: Byte = 1
  private val DeltaType: Byte = 2

  private def crc(a: Array[Byte], off: Int, len: Int): Int =
    val c = new CRC32
    c.update(a, off, len)
    c.getValue.toInt

  private def saltedCrc(salt: Long, a: Array[Byte], off: Int, len: Int): Int =
    val c = new CRC32
    c.update(ByteBuffer.allocate(8).putLong(salt).array())
    c.update(a, off, len)
    c.getValue.toInt

  def encodeHeader(seq: Long, salt: Long): Array[Byte] =
    val arr = new Array[Byte](HeaderSize)
    val bb = ByteBuffer.wrap(arr)
    bb.put(Magic)
    bb.putLong(seq)
    bb.putLong(salt)
    bb.putInt(crc(arr, 0, 24))
    arr

  private def encodeRecord(r: JournalRecord): Array[Byte] =
    r match
      case JournalRecord.Mapping(ext, slot, id) =>
        val bb = ByteBuffer.allocate(4 + 1 + 24)
        bb.putInt(25).put(MappingType).putInt(ext).putInt(slot)
        bb.putLong(id.uuid.getMostSignificantBits).putLong(id.uuid.getLeastSignificantBits)
        bb.array()
      case JournalRecord.Delta(ext, slot, header, ranges) =>
        val payload = 4 + 4 + SegmentHeader.Size + 4 + ranges.map(r => 8 + r.bytes.length).sum
        val bb = ByteBuffer.allocate(4 + 1 + payload)
        bb.putInt(1 + payload).put(DeltaType).putInt(ext).putInt(slot)
        bb.put(SegmentHeader.encode(header)).putInt(ranges.size)
        ranges.foreach: r =>
          bb.putInt(r.offset).putInt(r.bytes.length).put(r.bytes)
        bb.array()

  /** Bytes a batch adds beyond its records: len(4) + batchNo(8) + crc(4) */
  val BatchOverhead = 16

  /** Encoded size of one record, len prefix included */
  def recordSize(r: JournalRecord): Int =
    r match
      case _: JournalRecord.Mapping => 4 + 1 + 24
      case JournalRecord.Delta(_, _, _, ranges) =>
        4 + 1 + 4 + 4 + SegmentHeader.Size + 4 + ranges.map(r => 8 + r.bytes.length).sum

  def encodeBatch(salt: Long, batchNumber: Long, records: List[JournalRecord]): Array[Byte] =
    val encoded = records.map(encodeRecord)
    val body = 8 + encoded.map(_.length).sum
    val arr = new Array[Byte](4 + body + 4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(body).putLong(batchNumber)
    encoded.foreach(bb.put)
    bb.putInt(saltedCrc(salt, arr, 0, 4 + body))
    arr

  enum Scan:
    case Empty
    case Valid(seq: Long, salt: Long, records: Vector[JournalRecord], validLength: Long, nextBatch: Long)
    case Corrupt(reason: String)

  /** Total batch length at o if a CRC-valid batch with any number sits there, else -1 */
  private def validBatchLength(bytes: Array[Byte], salt: Long, o: Long): Int =
    val total = bytes.length.toLong
    if total - o < 4 then
      -1
    else
      val len = ByteBuffer.wrap(bytes).getInt(o.toInt)
      if len < 8 || o + 4L + len + 4L > total then
        -1
      else
        val end = o.toInt + 4 + len
        if ByteBuffer.wrap(bytes).getInt(end) == saltedCrc(salt, bytes, o.toInt, 4 + len) then 4 + len + 4 else -1

  private def findLater(bytes: Array[Byte], salt: Long, o: Long, n: Long): Boolean =
    val bb = ByteBuffer.wrap(bytes)
    val target = n + 1
    val low = target.toByte
    // Every position after o is a candidate: skipping a claimed extent would let a rotted len hide later batches
    var p = o + 4
    var found = false
    while !found && p + 8 <= bytes.length do
      if bytes(p.toInt + 7) == low && bb.getLong(p.toInt) == target then
        found = validBatchLength(bytes, salt, p - 4) > 0
      p += 1
    found

  private class BadRecord(msg: String) extends Exception(msg, null, false, false)

  private def parseRecords(bytes: Array[Byte], from: Int, to: Int): Vector[JournalRecord] =
    val out = Vector.newBuilder[JournalRecord]
    var pos = from
    while pos < to do
      if to - pos < 5 then throw BadRecord("truncated record header")
      val len = ByteBuffer.wrap(bytes).getInt(pos)
      if len < 1 || len.toLong > to - pos - 4 then throw BadRecord("bad record length")
      val end = pos + 4 + len
      out += parseRecord(bytes, pos + 4, end)
      pos = end
    out.result()

  private def parseRecord(bytes: Array[Byte], start: Int, end: Int): JournalRecord =
    val bb = ByteBuffer.wrap(bytes, start + 1, end - start - 1)
    bytes(start) match
      case MappingType =>
        if bb.remaining() != 24 then throw BadRecord("bad mapping length")
        val ext = bb.getInt()
        val slot = bb.getInt()
        if slot < -1 then throw BadRecord("bad mapping slot")
        JournalRecord.Mapping(ext, slot, ObjectId(new UUID(bb.getLong(), bb.getLong())))
      case DeltaType =>
        if bb.remaining() < 8 + SegmentHeader.Size + 4 then throw BadRecord("short delta")
        val ext = bb.getInt()
        val slot = bb.getInt()
        if slot < -1 then throw BadRecord("bad delta slot")
        val hb = new Array[Byte](SegmentHeader.Size)
        bb.get(hb)
        val header = SegmentHeader.decode(hb).getOrElse(throw BadRecord("bad delta header"))
        if header.sliceLength < 0 then throw BadRecord("negative slice length")
        val count = bb.getInt()
        if count < 0 || count.toLong * 8 > bb.remaining() then throw BadRecord("bad range count")
        val ranges = List.newBuilder[DeltaRange]
        var i = 0
        while i < count do
          if bb.remaining() < 8 then throw BadRecord("truncated range")
          val off = bb.getInt()
          val len = bb.getInt()
          if off < 0 || len < 0 || off.toLong + len > header.sliceLength || len > bb.remaining() then
            throw BadRecord("bad range")
          val data = new Array[Byte](len)
          bb.get(data)
          ranges += DeltaRange(off, data)
          i += 1
        if bb.hasRemaining then throw BadRecord("trailing bytes in delta")
        JournalRecord.Delta(ext, slot, header, ranges.result())
      case t => throw BadRecord(s"unknown record type $t")

  /** (seq, salt) if the file starts with a valid header, whatever follows it */
  def decodeHeader(bytes: Array[Byte]): Option[(Long, Long)] =
    val ok = bytes.length >= HeaderSize &&
      java.util.Arrays.equals(bytes, 0, 8, Magic, 0, 8) &&
      ByteBuffer.wrap(bytes).getInt(24) == crc(bytes, 0, 24)
    if ok then Some((ByteBuffer.wrap(bytes).getLong(8), ByteBuffer.wrap(bytes).getLong(16))) else None

  def scan(bytes: Array[Byte]): Scan =
    val header = decodeHeader(bytes)
    if header.isEmpty then
      if bytes.length <= HeaderSize then Scan.Empty else Scan.Corrupt("invalid journal header")
    else
      val (seq, salt) = header.get
      val records = Vector.newBuilder[JournalRecord]
      var o = HeaderSize.toLong
      var n = 0L
      var result: Option[Scan] = None
      while result.isEmpty && o < bytes.length do
        val size = validBatchLength(bytes, salt, o)
        val number = if size > 0 then ByteBuffer.wrap(bytes).getLong(o.toInt + 4) else -1L
        if size > 0 && number == n then
          try
            records ++= parseRecords(bytes, o.toInt + 12, o.toInt + size - 4)
            o += size
            n += 1
          catch
            case e: BadRecord =>
              result = Some(Scan.Corrupt(s"batch $n at $o: ${e.getMessage}"))
        else if findLater(bytes, salt, o, n) then
          result = Some(Scan.Corrupt(s"batch $n at $o is invalid but a later batch is valid"))
        else
          result = Some(Scan.Valid(seq, salt, records.result(), o, n))
      result.getOrElse(Scan.Valid(seq, salt, records.result(), o, n))
