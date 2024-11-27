package org.aspen_ddp.aspen.server.crl.simple

import org.apache.logging.log4j.scala.Logging

import java.util.UUID

class Stream(val streamId: StreamId,
             val streamWriter: StreamWriter,
             private var currentUUID: UUID,
             initialNextEntryOffset: Long) extends Logging:

  private var nextWriteOffset: Long = initialNextEntryOffset

  def recycleStream(): Unit =
    nextWriteOffset = 0
    currentUUID = UUID.randomUUID()


  def canWriteEntry(entry: LogEntry): Boolean =
    nextWriteOffset + entry.entrySize <= streamWriter.maxSizeInBytes


  // Writes entry to the stream and returns its location
  def writeEntry(entry: LogEntry, onWriteComplete: () => Unit): StreamLocation =
    if !canWriteEntry(entry) then
      logger.error(s"FAILED TO WRITE CRL LOG ENTRY. Max Size ${streamWriter.maxSizeInBytes}. Offset $nextWriteOffset. Entry Size: ${entry.entrySize}")

    require(canWriteEntry(entry))

    val entryLocation = StreamLocation(streamId, nextWriteOffset, entry.entrySize.toInt)
    val entrySize = entry.entrySize

    val buffers = entry.createEntryBuffers(nextWriteOffset, currentUUID, streamId)

    streamWriter.write(streamId, nextWriteOffset, buffers, () =>
      entry.runCompletionHandlers()
      onWriteComplete()
    )

    nextWriteOffset += entrySize
    entryLocation