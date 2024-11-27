package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{AllocationRecoveryState, TransactionRecoveryState}
import org.apache.logging.log4j.scala.Logging

import java.nio.file.Path
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.HashMap

object Recovery extends Logging:

  case class Result(activeStreamId: StreamId,
                    currentStreamUUID: UUID,
                    initialNextEntrySerialNumber: Long,
                    initialNextEntryOffset: Long,
                    initialOldestEntryNeeded: Long,
                    initialPreviousEntryLocation: StreamLocation,
                    trsList: List[TransactionRecoveryState],
                    arsList: List[AllocationRecoveryState])

  def recover(files: List[(StreamId, Path)]): Result =
    val readers = files.sortBy(_._1.number).map(t => StreamReader(t._1, t._2)).toArray
    val rs = LogEntry.RecoveringState(HashMap(), Set(), HashMap(), Set())
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
          activeStreamId = streamId
          initialNextEntryOffset = nextWriteEntryOffset
          initialPreviousEntryLocation = lastEntry.previousEntryStreamLocation
          initialNextEntrySerialNumber = lastEntry.entrySerialNumber + 1
          initialOldestEntryNeeded = lastEntry.oldestEntryNeeded

          @tailrec
          def loadPrevious(previousEntryLocation: StreamLocation): Unit =
            val reader = readers(previousEntryLocation.streamId.number)
            reader.readEntry(previousEntryLocation.offset) match
              case None => assert(false, "Failed to load a CRL entry")
              case Some((_, h)) =>
                val bb = reader.read(
                  previousEntryLocation.offset + LogEntry.StaticEntryHeaderSize + h.dynamicDataSize,
                  h.staticDataSize.toInt)
                LogEntry.loadStaticEntryContent(h, bb, rs)
                currentStreamUUID = h.streamUUID
                if h.entrySerialNumber != lastEntry.oldestEntryNeeded then
                  loadPrevious(h.previousEntryStreamLocation)

          loadPrevious(StreamLocation(
            streamId,
            nextWriteEntryOffset - lastEntry.entrySize,
            lastEntry.entrySize.toInt))

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

    val arsLst = rs.allocations.valuesIterator.map( a =>
      AllocationRecoveryState(
        a.txid.storeId,
        a.storePointer,
        a.newObjectId,
        a.objectType,
        a.objectSize,
        readStreamLocation(a.dataLocation),
        a.initialRefcount,
        a.timestamp,
        a.txid.transactionId,
        a.serializedRevisionGuard)
    ).toList

    Result(
      activeStreamId,
      currentStreamUUID,
      initialNextEntrySerialNumber,
      initialNextEntryOffset,
      initialOldestEntryNeeded,
      initialPreviousEntryLocation,
      trsList,
      arsLst)



