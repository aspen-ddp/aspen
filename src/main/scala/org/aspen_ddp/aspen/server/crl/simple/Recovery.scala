package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.TransactionRecoveryState
import scribe.Logging

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.HashMap

object Recovery extends Logging:

  /** Thrown when the chain of entries from the newest back to the oldest one needed is broken: an entry cannot be
   *  read, or its previous-entry link ends or does not lead to an older entry. Its CRL cannot be recovered. */
  class BrokenEntryChain(reason: String) extends Exception(s"CRL entry chain is broken: $reason")

  case class Result(activeStreamId: StreamId,
                    currentStreamUUID: UUID,
                    initialNextEntrySerialNumber: Long,
                    initialNextEntryOffset: Long,
                    initialOldestEntryNeeded: Long,
                    initialPreviousEntryLocation: StreamLocation,
                    trsList: List[TransactionRecoveryState],
                    /** Applied stubs: not transactions, recovered apart from them */
                    appliedStubs: Set[TxId] = Set())

  def recover(files: List[(StreamId, os.Path)]): Result =
    val readers = files.sortBy(_._1.number).map(t => StreamReader(t._1, t._2)).toArray
    val rs = LogEntry.RecoveringState(HashMap(), Set())
    var highestHead: Option[(StreamId, LogEntry.EntryHeader)] = None
    var activeStreamId = StreamId(0)
    var initialNextEntrySerialNumber: Long = 0
    var initialNextEntryOffset: Long = 0
    var initialOldestEntryNeeded: Long = 0
    var initialPreviousEntryLocation: StreamLocation = StreamLocation.Null
    var currentStreamUUID = UUID.randomUUID()

    for idx <- files.indices do
      (highestHead, readers(idx).headEntry) match
        case (None, None) =>
        case (None, Some(h)) => highestHead = Some((StreamId(idx), h))
        case (Some(_), None) =>
        case (Some(prev), Some(h)) =>
          if h.entrySerialNumber > prev._2.entrySerialNumber then
            highestHead = Some((StreamId(idx), h))

    highestHead.foreach: (streamId, _) =>
      readers(streamId.number).findHighestEntry() match
        case None => assert(false, "Failed to find highest entry")
        case Some((nextWriteEntryOffset, lastEntry)) =>
          val lastEntryLocation = StreamLocation(
            streamId,
            nextWriteEntryOffset - lastEntry.entrySize,
            lastEntry.entrySize.toInt)
          activeStreamId = streamId
          initialNextEntryOffset = nextWriteEntryOffset
          // The next entry links to the last one. Linking to the last one's own previous entry skipped it, so the next
          // recovery's walk could not reach an oldest entry needed that was the skipped one
          initialPreviousEntryLocation = lastEntryLocation
          initialNextEntrySerialNumber = lastEntry.entrySerialNumber + 1
          initialOldestEntryNeeded = lastEntry.oldestEntryNeeded
          // New entries continue the active stream, so they carry its UUID. The walk below visits older entries,
          // possibly in other streams, and must not change it: entries written under another stream's UUID would not
          // be readable from the active stream at the next recovery
          currentStreamUUID = lastEntry.streamUUID

          // Walks from the newest entry back to the oldest one needed. Each step must reach an entry older than the
          // last, so a broken chain throws rather than looping
          @tailrec
          def loadPrevious(previousEntryLocation: StreamLocation, newerSerialNumber: Long): Unit =
            if previousEntryLocation.streamId.number < 0 || previousEntryLocation.streamId.number >= readers.length then
              throw BrokenEntryChain(s"the entry after serial number $newerSerialNumber links to " +
                                     s"$previousEntryLocation, before reaching the oldest entry needed, " +
                                     s"${lastEntry.oldestEntryNeeded}")
            val reader = readers(previousEntryLocation.streamId.number)
            reader.readEntry(previousEntryLocation.offset) match
              case None => throw BrokenEntryChain(s"no entry can be read at $previousEntryLocation")
              case Some((_, h)) =>
                if h.entrySerialNumber >= newerSerialNumber then
                  throw BrokenEntryChain(s"the entry at $previousEntryLocation has serial number " +
                                         s"${h.entrySerialNumber}, not older than the $newerSerialNumber linking to it")
                val bb = reader.read(
                  previousEntryLocation.offset + LogEntry.StaticEntryHeaderSize + h.dynamicDataSize,
                  h.staticDataSize.toInt)
                LogEntry.loadStaticEntryContent(h, bb, rs)
                if h.entrySerialNumber > lastEntry.oldestEntryNeeded then
                  loadPrevious(h.previousEntryStreamLocation, h.entrySerialNumber)

          loadPrevious(lastEntryLocation, lastEntry.entrySerialNumber + 1)

    def readStreamLocation(loc: StreamLocation): DataBuffer =
      DataBuffer(readers(loc.streamId.number).read(loc.offset, loc.length))

    val trsList = rs.txs.valuesIterator.map( ltx =>
      val ous: List[ObjectUpdate] = ltx.updateLocations match
        case None => Nil
        case Some(lst) => lst.map(t => ObjectUpdate(t._1, readStreamLocation(t._2)))

      TransactionRecoveryState(
        ltx.id.storeId,
        readStreamLocation(ltx.txdLocation),
        ous,
        ltx.disposition,
        TransactionStatus.Unresolved,
        ltx.paxosAcceptorState)
    ).toList

    Result(
      activeStreamId,
      currentStreamUUID,
      initialNextEntrySerialNumber,
      initialNextEntryOffset,
      initialOldestEntryNeeded,
      initialPreviousEntryLocation,
      trsList,
      rs.stubs)
