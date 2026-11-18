package org.aspen_ddp.aspen.server.crl.simple

import scribe.Logging

import java.util.UUID

class Stream(val streamId: StreamId,
             val crlId: UUID,
             val streamWriter: StreamWriter,
             private var currentUUID: UUID,
             initialNextEntryOffset: Long) extends Logging:

  private var nextWriteOffset: Long = initialNextEntryOffset

  def recycleStream(): Unit =
    nextWriteOffset = 0
    currentUUID = UUID.randomUUID()


  /** Tests only: the bytes left after the entries written since the stream was last entered */
  private[simple] def room: Long = streamWriter.maxSizeInBytes - nextWriteOffset

  def canWriteEntry(entry: LogEntry): Boolean =
    nextWriteOffset + entry.entrySize <= streamWriter.maxSizeInBytes


  /** Writes `entry` to the stream as entry `serial`, linking to `previous`, and returns its location */
  def writeEntry(entry: LogEntry, serial: Long, previous: StreamLocation, onWriteComplete: () => Unit): StreamLocation =
    if !canWriteEntry(entry) then
      logger.error(s"FAILED TO WRITE CRL LOG ENTRY. Max Size ${streamWriter.maxSizeInBytes}. Offset $nextWriteOffset. Entry Size: ${entry.entrySize}")

    require(canWriteEntry(entry))

    val entryLocation = StreamLocation(streamId, nextWriteOffset, entry.entrySize.toInt)
    val entrySize = entry.entrySize

    val buffers = entry.createEntryBuffers(serial, previous, nextWriteOffset, crlId, currentUUID, streamId)

    // The entry's handlers are handed to the completion thread once the CRL thread processes this completion
    // (SimpleCRL, WriteComplete)
    streamWriter.write(streamId, nextWriteOffset, buffers, onWriteComplete)

    nextWriteOffset += entrySize
    entryLocation