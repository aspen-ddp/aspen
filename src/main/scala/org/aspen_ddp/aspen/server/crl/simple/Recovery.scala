package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.transaction.ObjectUpdate
import org.aspen_ddp.aspen.server.crl.{CrlCorrupt, TransactionRecoveryState}
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
                    txs: List[RecoveredTx],
                    /** Applied stubs: not transactions, recovered apart from them */
                    stubs: List[RecoveredStub]):
    def trsList: List[TransactionRecoveryState] = txs.map(_.state)
    def appliedStubs: Map[TxId, HLCTimestamp] = stubs.map(s => s.id -> s.start).toMap

  /** A live transaction, with where its data lies and the serial number of the oldest entry holding any of it: its
    * place in the opened CRL's queue (SimpleCRL writes nothing at open) */
  case class RecoveredTx(id: TxId,
                         state: TransactionRecoveryState,
                         txdLocation: StreamLocation,
                         updateLocations: Option[List[(ObjectId, StreamLocation)]],
                         entrySerialNumber: Long)

  /** A live applied stub, with its anchor and the serial number of the newest entry holding it */
  case class RecoveredStub(id: TxId, start: HLCTimestamp, anchor: StreamLocation, entrySerialNumber: Long)

  /** @param crlId the id the CRL recorded at its creation (SimpleCRL.Sizing): only headers bearing it are entries
    * @param ring  the order the streams are written in (SimpleCRL.Sizing.ring), a permutation of `files`' ids */
  def recover(files: List[(StreamId, os.Path)], crlId: UUID, ring: Vector[StreamId]): Result =
    val readers = files.sortBy(_._1.number).map(t => StreamReader(t._1, t._2, crlId)).toArray
    try recover(readers, ring)
    finally readers.foreach(_.close())

  private def recover(readers: Array[StreamReader], ring: Vector[StreamId]): Result =
    val rs = LogEntry.RecoveringState(HashMap(), Set())
    var activeStreamId = StreamId(0)
    var initialNextEntrySerialNumber: Long = 0
    var initialNextEntryOffset: Long = 0
    var initialOldestEntryNeeded: Long = 0
    var initialPreviousEntryLocation: StreamLocation = StreamLocation.Null
    var currentStreamUUID = UUID.randomUUID()

    // The newest intact entry: the stream with the highest head serial whose cycle holds a wholly intact entry. A
    // stream whose only entries are a torn head (a rotation write in flight at a crash) yields to the one before it
    val byHead = readers.indices.flatMap(i => readers(i).headEntry.map(h => (i, h)))
      .sortBy(-_._2.entrySerialNumber)
    val newest = byHead.iterator.flatMap((i, _) => readers(i).lastIntactEntry().map(e => (i, e))).nextOption()

    // Entry extents whose checksums the walk verified, by stream: (start of dynamic data, end of dynamic data, entry
    // serial number)
    var verified: Map[Int, List[(Long, Long, Long)]] = Map()

    newest.foreach: (streamIndex, last) =>
      val (lastOffset, lastEntry) = last
      val streamId = StreamId(streamIndex)
      val nextWriteEntryOffset = lastOffset + lastEntry.entrySize
      val lastEntryLocation = StreamLocation(streamId, lastOffset, lastEntry.entrySize.toInt)

      // Only the write in flight at a crash can be torn, so an intact entry newer than the last one means an entry
      // before it is corrupt: in this stream past the last entry, or in the next stream, whose corrupt head would
      // hide a cycle newer than this stream's. The next stream is the ring's successor, not the next number
      List((streamIndex, nextWriteEntryOffset), (StreamId.successor(ring, streamId).number, 0L)).foreach: (i, from) =>
        readers(i).newerIntactEntry(from, lastEntry.entrySerialNumber).foreach: (offset, h) =>
          throw CrlCorrupt(s"stream $i holds an intact entry at $offset with serial number " +
                                 s"${h.entrySerialNumber}, newer than the last one recovery found, " +
                                 s"${lastEntry.entrySerialNumber} in stream $streamIndex at $lastOffset")

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
      // last, so a broken chain throws rather than looping. Every entry walked must pass its checksums
      @tailrec
      def loadPrevious(previousEntryLocation: StreamLocation, newerSerialNumber: Long): Unit =
        if previousEntryLocation.streamId.number < 0 || previousEntryLocation.streamId.number >= readers.length then
          throw BrokenEntryChain(s"the entry after serial number $newerSerialNumber links to " +
                                 s"$previousEntryLocation, before reaching the oldest entry needed, " +
                                 s"${lastEntry.oldestEntryNeeded}")
        val reader = readers(previousEntryLocation.streamId.number)
        val offset = previousEntryLocation.offset
        val h = reader.readHeader(offset) match
          case None => throw CrlCorrupt(s"the entry at $previousEntryLocation fails its header checks")
          case Some(h) if h.streamUUID != reader.currentUUID =>
            throw BrokenEntryChain(s"the entry at $previousEntryLocation belongs to another cycle of its stream")
          case Some(h) => h
        if h.entrySerialNumber >= newerSerialNumber then
          throw BrokenEntryChain(s"the entry at $previousEntryLocation has serial number " +
                                 s"${h.entrySerialNumber}, not older than the $newerSerialNumber linking to it")
        // The last entry's body was checked when it was found
        if previousEntryLocation != lastEntryLocation && !reader.bodyIntact(offset, h) then
          throw CrlCorrupt(s"the body of the entry at $previousEntryLocation, serial number " +
                                 s"${h.entrySerialNumber}, fails its checksum")
        val dynamicStart = offset + LogEntry.StaticEntryHeaderSize
        val dynamicEnd = dynamicStart + h.dynamicDataSize
        verified += reader.streamId.number ->
          ((dynamicStart, dynamicEnd, h.entrySerialNumber) :: verified.getOrElse(reader.streamId.number, Nil))
        val bb = reader.read(dynamicEnd, h.staticDataSize.toInt)
        LogEntry.loadStaticEntryContent(h, bb, rs, StreamLocation(reader.streamId, dynamicEnd, 0))
        if h.entrySerialNumber > lastEntry.oldestEntryNeeded then
          loadPrevious(h.previousEntryStreamLocation, h.entrySerialNumber)

      loadPrevious(lastEntryLocation, lastEntry.entrySerialNumber + 1)

    /** The serial number of the verified entry whose dynamic data holds `loc`. Data is read only from entries the
      * walk verified, so it is covered by their checksums */
    def entryHolding(loc: StreamLocation): Long =
      verified.getOrElse(loc.streamId.number, Nil).collectFirst:
        case (start, end, serial) if loc.offset >= start && loc.length >= 0 && loc.offset + loc.length <= end => serial
      .getOrElse:
        throw BrokenEntryChain(s"a live record names $loc, outside the dynamic data of every entry recovered")

    def readStreamLocation(loc: StreamLocation): DataBuffer =
      entryHolding(loc)
      DataBuffer(readers(loc.streamId.number).read(loc.offset, loc.length))

    val txs = rs.txs.valuesIterator.map: ltx =>
      val ous: List[ObjectUpdate] = ltx.updateLocations match
        case None => Nil
        case Some(lst) => lst.map(t => ObjectUpdate(t._1, readStreamLocation(t._2)))

      val trs = TransactionRecoveryState(
        ltx.id.storeId,
        readStreamLocation(ltx.txdLocation),
        ous,
        ltx.disposition,
        ltx.status,
        ltx.paxosAcceptorState)
      val locations = ltx.txdLocation :: ltx.updateLocations.getOrElse(Nil).map(_._2)
      RecoveredTx(ltx.id, trs, ltx.txdLocation, ltx.updateLocations, locations.map(entryHolding).min)
    .toList

    Result(
      activeStreamId,
      currentStreamUUID,
      initialNextEntrySerialNumber,
      initialNextEntryOffset,
      initialOldestEntryNeeded,
      initialPreviousEntryLocation,
      txs,
      rs.stubs.iterator.map((id, s) => RecoveredStub(id, s.start, s.anchor, s.entrySerialNumber)).toList)
