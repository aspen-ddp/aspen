package org.aspen_ddp.aspen.server.crl.simple

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.UUID
import java.util.zip.CRC32C
import scala.annotation.tailrec

object StreamReader:
  val ZeroUUID: UUID = new UUID(0, 0)

  /** Entries start on 4 KiB boundaries (LogEntry.padTo4k) */
  val EntryAlignment: Long = 4096

  private val CrcChunkSize: Int = 1024 * 1024

class StreamReader(val streamId: StreamId, val filePath: os.Path, val crlId: UUID):
  import StreamReader.*

  private val ochannel: Option[FileChannel] =
    if os.exists(filePath) then
      // FileChannel rather than os-lib, for positional reads
      Some(FileChannel.open(filePath.toNIO, java.nio.file.StandardOpenOption.READ))
    else
      None

  private val fileSize: Long = ochannel.map(_.size()).getOrElse(0L)

  def size: Long = fileSize

  /** The UUID of the stream's current cycle: the one in the header at offset 0 */
  val currentUUID: UUID = ochannel match
    case Some(_) => readUUID(0)
    case None => UUID.randomUUID()

  private def readFully(offset: Long, bb: ByteBuffer): Unit = ochannel match
    case None => assert(false)
    case Some(channel) =>
      var pos = offset
      while bb.hasRemaining && channel.read(bb, pos) >= 0 do
        pos = offset + bb.position()
      bb.flip()

  private def readUUID(offset: Long): UUID =
    if offset + 16 > fileSize then
      ZeroUUID
    else
      val buff = ByteBuffer.allocate(16)
      readFully(offset, buff)
      Tx.getUUID(buff)

  /** The header at `offset` if it is intact: its checksum matches, it bears this CRL's id, its stream UUID is not zero
    * (a zero-filled region is not an entry), the entry fits in the file, and its trailing UUID matches the header's.
    * The id is what keeps object data, which old cycles leave in the streams, from passing for an entry: bytes shaped
    * like one (crafted, or an image of another CRL) bear another id. Says nothing of its body (bodyIntact) or of
    * which cycle of the stream wrote it */
  def readHeader(entryOffset: Long): Option[LogEntry.EntryHeader] =
    if ochannel.isEmpty || entryOffset < 0 || entryOffset + LogEntry.StaticEntryHeaderSize > fileSize then
      None
    else
      val arr = new Array[Byte](LogEntry.StaticEntryHeaderSize)
      readFully(entryOffset, ByteBuffer.wrap(arr))
      if !LogEntry.headerIntact(arr) then
        None
      else
        val h = LogEntry.loadHeader(ByteBuffer.wrap(arr))
        val fits = h.dynamicDataSize >= 0 && h.staticDataSize >= 16 &&
          h.dynamicDataSize <= fileSize && h.staticDataSize <= fileSize &&
          entryOffset + h.entrySize <= fileSize
        if h.crlId != crlId || h.streamUUID == ZeroUUID || !fits || readUUID(entryOffset + h.trailingUUIDOffset) != h.streamUUID then
          None
        else
          Some(h)

  /** Whether the body of the entry at `entryOffset` carries the checksum its header records */
  def bodyIntact(entryOffset: Long, h: LogEntry.EntryHeader): Boolean =
    val crc = new CRC32C
    val buff = ByteBuffer.allocate(CrcChunkSize)
    var pos = entryOffset + LogEntry.StaticEntryHeaderSize
    var remaining = h.dynamicDataSize + h.staticDataSize
    var short = false
    while remaining > 0 && !short do
      buff.clear()
      buff.limit(math.min(remaining, CrcChunkSize.toLong).toInt)
      readFully(pos, buff)
      short = buff.limit() == 0
      pos += buff.limit()
      remaining -= buff.limit()
      crc.update(buff)
    !short && crc.getValue.toInt == h.bodyCrc

  /** The intact header at `entryOffset` if the stream's current cycle wrote it */
  def readEntry(entryOffset: Long): Option[LogEntry.EntryHeader] =
    readHeader(entryOffset).filter(_.streamUUID == currentUUID)

  def headEntry: Option[LogEntry.EntryHeader] = readEntry(0)

  /** The newest entry of the current cycle that is wholly intact, with its offset. Entries are followed from the head
    * while their headers are intact; only the last one's body is checked, because only the write in flight at a crash
    * can be torn. A torn last entry yields the one before it, unchecked: recovery checks every entry it reads */
  def lastIntactEntry(): Option[(Long, LogEntry.EntryHeader)] =
    @tailrec
    def follow(prev: Option[(Long, LogEntry.EntryHeader)], cur: (Long, LogEntry.EntryHeader))
    : (Option[(Long, LogEntry.EntryHeader)], (Long, LogEntry.EntryHeader)) =
      val next = cur._1 + cur._2.entrySize
      readEntry(next) match
        case Some(h) if h.entrySerialNumber > cur._2.entrySerialNumber => follow(Some(cur), (next, h))
        case _ => (prev, cur)

    headEntry.flatMap: head =>
      val (prev, last) = follow(None, (0L, head))
      if bodyIntact(last._1, last._2) then
        Some(last)
      else
        scribe.warn(s"CRL stream $filePath: dropping the entry at ${last._1} (serial number " +
                    s"${last._2.entrySerialNumber}), whose body fails its checksum: a write torn by a crash")
        prev

  /** The first wholly intact entry, of any cycle, at or after `from` whose serial number is above `serialNumber`.
    * Intact headers are skipped by their entry's size, anything else by EntryAlignment. Bodies are read only for
    * headers above the serial number, so a stream of older entries costs one header read per entry */
  def newerIntactEntry(from: Long, serialNumber: Long): Option[(Long, LogEntry.EntryHeader)] =
    var offset = from
    var found: Option[(Long, LogEntry.EntryHeader)] = None
    while found.isEmpty && offset + LogEntry.StaticEntryHeaderSize <= fileSize do
      readHeader(offset) match
        case Some(h) =>
          if h.entrySerialNumber > serialNumber && bodyIntact(offset, h) then
            found = Some((offset, h))
          offset += h.entrySize
        case None =>
          offset = (offset / EntryAlignment + 1) * EntryAlignment
    found

  def close(): Unit = ochannel.foreach(_.close())

  def read(offset: Long, length: Int): ByteBuffer =
    assert(offset >= 0 && offset + length <= fileSize)
    val bb = ByteBuffer.allocate(length)
    readFully(offset, bb)
    bb
