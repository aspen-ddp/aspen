package org.aspen_ddp.aspen.server.crl.simple

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.UUID
import scala.annotation.tailrec

class StreamReader(val streamId: StreamId, val filePath: Path):

  private val ochannel =
    if Files.exists(filePath) then
      Some(FileChannel.open(filePath, StandardOpenOption.READ))
    else
      None

  val currentUUID: UUID = ochannel match
    case Some(channel) =>
      readUUID(0)
    case None =>
      UUID.randomUUID()

  private def readUUID(offset: Long): UUID = ochannel match
    case None => UUID(0,0)
    case Some(channel) =>
      val startPosition = channel.position()
      val buff = ByteBuffer.allocate(16)
      channel.position(offset)
      channel.read(buff)
      channel.position(startPosition)
      buff.position(0)
      LogContent.getUUID(buff)


  def headEntry: Option[LogEntry.EntryHeader] = readEntry(0).map(_._2)


  // Walks through each entry until the end of file is encountered or a corrupt entry is found.
  // Return value is (nextWriteEntryOffset, entryHeader). The offset is needed as an initializstion
  // parameter for Stream instances
  @tailrec
  final def findHighestEntry(entryOffset: Long = 0,
                             oheader: Option[(Long, LogEntry.EntryHeader)] = None): Option[(Long, LogEntry.EntryHeader)] =
    readEntry(entryOffset) match
      case None => oheader
      case Some((nextEntryOffset, header)) => findHighestEntry(entryOffset + header.entrySize, Some((nextEntryOffset, header)))


  def readEntry(entryOffset: Long): Option[(Long, LogEntry.EntryHeader)] = ochannel match
    case None => None

    case Some(channel) =>
      if entryOffset + LogEntry.StaticEntryHeaderSize >= channel.size() then
        return None

      channel.position(entryOffset)

      val bb = ByteBuffer.allocate(LogEntry.StaticEntryHeaderSize)
      channel.read(bb)
      bb.position(0)

      val h = LogEntry.loadHeader(bb)

      if entryOffset + h.trailingUUIDOffset + 16 > channel.size() then
        // Can easily happen if we're reading a corrupted header
        return None

      //println(s"Reading tail at offset: ${h.trailingUUIDOffset}, Static offset: ${h.staticDataSize - 16}")
      val tailUUID = readUUID(entryOffset + h.trailingUUIDOffset)

      if h.streamUUID == currentUUID && tailUUID == currentUUID then
        Some((entryOffset + h.entrySize, h))
      else
        //println(s"UUID MISMATCH Current:$currentUUID. Header:${h.streamUUID}. Tail:$tailUUID")
        None

  def read(offset: Long, length: Int): ByteBuffer = ochannel match
    case None => assert(false)
    case Some(channel) =>
      assert(offset + length <= channel.size())
      channel.position(offset)
      val bb = ByteBuffer.allocate(length)
      channel.read(bb)
      bb.position(0)
      bb



