package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32C

import scala.util.control.NonFatal

/** The journal's creation parameters, recorded once in its sizing file */
final case class JournalSizing(journalId: UUID, files: Int, fileSize: Long):
  require(files >= 3, s"a journal needs at least 3 files, not $files")
  require(fileSize % 4096 == 0, s"journal file size $fileSize is not a multiple of 4096")
  require(fileSize > DeviceJournalFormat.HeaderSize, s"journal file size $fileSize does not exceed the header")
  require(fileSize <= (1L << 30), s"journal file size $fileSize exceeds 1 GiB")

/** `first` is the serial of the file's first batch, `oldestNeeded` the oldest serial still needed at the pass's start */
final case class FileHeader(journalId: UUID, salt: Long, first: Long, oldestNeeded: Long)

final case class TaggedRecord(attachment: UUID, record: JournalRecord)

final case class Batch(serial: Long, records: Vector[TaggedRecord])

enum BatchRead:
  case Ok(batch: Batch, next: Int)
  /** Not a batch of this pass: zeros, torn, stale or another journal's */
  case Invalid
  /** CRC-valid but its contents cannot be read */
  case Unparseable(reason: String)

/** Sizing: "FSIFDJSZ"(8) journalId(16) files(4) fileSize(8) crc(4 over the first 36).
  * Header (padded to 4096): "FSIFDJHD"(8) journalId(16) salt(8) first(8) oldestNeeded(8) crc(4 over the first 48).
  * Batch: length(4) serial(8) count(4) records... crc(4) zero padding to 4096. Length counts serial through the last
  * record. Record: attachment(16) then a JournalFormat record. The CRC is CRC32C over journalId(16), salt(8) (neither
  * stored in the batch) then the bytes from length through the last record.
  */
object DeviceJournalFormat:
  val HeaderSize = 4096
  private val Block = 4096
  private val SizingMagic = "FSIFDJSZ".getBytes("US-ASCII")
  private val HeaderMagic = "FSIFDJHD".getBytes("US-ASCII")
  private val SizingLength = 8 + 16 + 4 + 8 + 4
  private val HeaderUsed = 8 + 16 + 8 + 8 + 8 + 4

  private def putId(bb: ByteBuffer, id: UUID): Unit =
    bb.putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits)

  private def getId(bb: ByteBuffer): UUID = new UUID(bb.getLong(), bb.getLong())

  private def crc(a: Array[Byte], off: Int, len: Int): Int =
    val c = new CRC32C
    c.update(a, off, len)
    c.getValue.toInt

  private def batchCrc(journalId: UUID, salt: Long, a: Array[Byte], off: Int, len: Int): Int =
    val c = new CRC32C
    val prefix = ByteBuffer.allocate(24)
    putId(prefix, journalId)
    prefix.putLong(salt)
    c.update(prefix.array())
    c.update(a, off, len)
    c.getValue.toInt

  def encodeSizing(s: JournalSizing): Array[Byte] =
    val arr = new Array[Byte](SizingLength)
    val bb = ByteBuffer.wrap(arr)
    bb.put(SizingMagic)
    putId(bb, s.journalId)
    bb.putInt(s.files).putLong(s.fileSize)
    bb.putInt(crc(arr, 0, SizingLength - 4))
    arr

  def decodeSizing(b: Array[Byte]): Option[JournalSizing] =
    if b.length != SizingLength ||
      !java.util.Arrays.equals(b, 0, 8, SizingMagic, 0, 8) ||
      ByteBuffer.wrap(b).getInt(SizingLength - 4) != crc(b, 0, SizingLength - 4)
    then
      None
    else
      val bb = ByteBuffer.wrap(b, 8, SizingLength - 8)
      val id = getId(bb)
      val files = bb.getInt()
      val size = bb.getLong()
      if files >= 3 && size % 4096 == 0 && size > HeaderSize && size <= (1L << 30) then Some(JournalSizing(id, files, size)) else None

  def encodeHeader(h: FileHeader): Array[Byte] =
    val arr = new Array[Byte](HeaderSize)
    val bb = ByteBuffer.wrap(arr)
    bb.put(HeaderMagic)
    putId(bb, h.journalId)
    bb.putLong(h.salt).putLong(h.first).putLong(h.oldestNeeded)
    bb.putInt(crc(arr, 0, HeaderUsed - 4))
    arr

  def decodeHeader(b: Array[Byte], journalId: UUID): Option[FileHeader] =
    if b.length < HeaderUsed ||
      !java.util.Arrays.equals(b, 0, 8, HeaderMagic, 0, 8) ||
      ByteBuffer.wrap(b).getInt(HeaderUsed - 4) != crc(b, 0, HeaderUsed - 4)
    then
      None
    else
      val bb = ByteBuffer.wrap(b, 8, HeaderUsed - 8)
      val id = getId(bb)
      if id != journalId then None else Some(FileHeader(id, bb.getLong(), bb.getLong(), bb.getLong()))

  def taggedRecordSize(r: JournalRecord): Int = JournalFormat.recordSize(r) + 16

  private def unpadded(records: Seq[TaggedRecord]): Int =
    4 + 8 + 4 + records.map(t => taggedRecordSize(t.record)).sum + 4

  private def padded(n: Int): Int = (n + Block - 1) / Block * Block

  def batchSize(records: Seq[TaggedRecord]): Int = padded(unpadded(records))

  def encodeBatch(journalId: UUID, salt: Long, b: Batch): Array[Byte] =
    val arr = new Array[Byte](batchSize(b.records))
    val bb = ByteBuffer.wrap(arr)
    val end = unpadded(b.records) - 4
    bb.putInt(end - 4).putLong(b.serial).putInt(b.records.size)
    b.records.foreach: t =>
      putId(bb, t.attachment)
      bb.put(JournalFormat.encodeRecord(t.record))
    bb.putInt(batchCrc(journalId, salt, arr, 0, end))
    arr

  def decodeBatchAt(file: Array[Byte], pos: Int, journalId: UUID, salt: Long): BatchRead =
    if pos < 0 || file.length - pos < 4 + 12 + 4 then
      BatchRead.Invalid
    else
      val len = ByteBuffer.wrap(file).getInt(pos)
      if len < 12 || len.toLong > file.length - pos - 8 then
        BatchRead.Invalid
      else
        val end = pos + 4 + len
        if ByteBuffer.wrap(file).getInt(end) != batchCrc(journalId, salt, file, pos, 4 + len) then
          BatchRead.Invalid
        else
          try
            val bb = ByteBuffer.wrap(file)
            val serial = bb.getLong(pos + 4)
            val count = bb.getInt(pos + 12)
            if count < 0 then
              BatchRead.Unparseable("negative record count")
            else
              val out = Vector.newBuilder[TaggedRecord]
              var p = pos + 16
              var i = 0
              while i < count do
                if end - p < 16 + 5 then
                  throw IllegalArgumentException("truncated record")
                val att = new UUID(bb.getLong(p), bb.getLong(p + 8))
                val rl = bb.getInt(p + 16)
                if rl < 1 || rl.toLong > end - p - 20 then
                  throw IllegalArgumentException("bad record length")
                out += TaggedRecord(att, JournalFormat.parseRecord(file, p + 20, p + 20 + rl))
                p += 20 + rl
                i += 1
              if p != end then
                BatchRead.Unparseable("trailing bytes after the last record")
              else
                BatchRead.Ok(Batch(serial, out.result()), pos + padded(end + 4 - pos))
          catch
            case NonFatal(e) => BatchRead.Unparseable(String.valueOf(e.getMessage))
