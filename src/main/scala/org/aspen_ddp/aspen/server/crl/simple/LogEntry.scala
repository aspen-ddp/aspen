package org.aspen_ddp.aspen.server.crl.simple

import scribe.Logging

import org.aspen_ddp.aspen.common.HLCTimestamp

import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32C
import scala.collection.immutable.HashMap

class LogEntry(val previousEntryLocation: StreamLocation,
               val entrySerialNumber: Long,
               var oldestEntryNeeded: Long) extends Logging:
  import LogEntry._

  private var completionHandlers: List[() => Unit] = Nil
  private var txs: HashMap[TxId, Tx] = new HashMap()
  private var txDeletions: List[TxId] = Nil
  private var txDelSet: Set[TxId] = Set()
  private var stubs: HashMap[TxId, Stub] = new HashMap()
  private var stubDeletions: Set[TxId] = Set()
  /** Transactions whose deletion in this entry a later save cancels (deleteTxUnlessResaved): made by a stub's save or
    * a dropStore, and by no final deletion */
  private var resavable: Set[TxId] = Set()


  def isEmpty: Boolean =
    txs.isEmpty
      && txDeletions.isEmpty
      && stubs.isEmpty
      && stubDeletions.isEmpty


  def staticDataSize: Long =
    txs.valuesIterator.foldLeft(0L)((s, tx) => s + tx.staticDataSize)
    + TxId.StaticSize * txDeletions.length
    + Stub.StaticSize * stubs.size
    + TxId.StaticSize * stubDeletions.size
    + 16 // Trailing file UUID. Must match header UUID during load process or corrupted entry

  def dynamicDataSize: Long =
    txs.valuesIterator.foldLeft(0L)((s, t) => s + t.dynamicDataSize)

  def entrySize: Long =
    val base = StaticEntryHeaderSize + dynamicDataSize + staticDataSize
    val nPadBytes = padTo4k(base)
    base + nPadBytes

  def runCompletionHandlers(): Unit = completionHandlers.foreach(ch => ch())

  def addTx(tx: Tx, completionHandler: () => Unit): Unit =
    // Do not check if txs already contains the transaction. Allow the duplicate insert so we queue
    // the completion handler for the subsequent insert attempts.
    //
    // Deletions are final (deletesFinally). A save of a transaction deleted earlier in this same entry is a timing
    // artifact and is skipped, but it still reports success once the entry (and thus the delete) is durable.
    // SimpleCRL never creates a record for such a save: it queues only the handler (addCompletionHandler), since a
    // record that is never written would pin oldestEntryNeeded and be reported as live (crash-recovery-log F4).
    //
    // A deletion made by a stub's save or a dropStore (deleteTxUnlessResaved) is not final: a save after it is a new
    // state of the transaction (live beside the stub, or of the store loaded again). The save cancels that deletion,
    // so it is written; recovery takes the newest save
    if resavable.contains(tx.id) then
      resavable -= tx.id
      txDelSet -= tx.id
      txDeletions = txDeletions.filterNot(_ == tx.id)
    if !deletesFinally(tx.id) then
      txs += (tx.id -> tx)
    completionHandlers = completionHandler :: completionHandlers

  /** True when this entry deletes `txid` and a save of it would not cancel the deletion: the save is skipped (addTx) */
  def deletesFinally(txid: TxId): Boolean =
    txDelSet.contains(txid) && !resavable.contains(txid)

  /** Runs `completionHandler` once this entry is durable */
  def addCompletionHandler(completionHandler: () => Unit): Unit =
    completionHandlers = completionHandler :: completionHandlers

  def deleteTx(txid: TxId): Unit =
    txDeletions = txid :: txDeletions
    txDelSet = txDelSet + txid
    resavable -= txid

  /** Deletes the transaction state a stub's save replaces, or a dropStore drops. Unlike deleteTx, a later save of it
    * in this entry cancels the deletion (addTx). A final deletion already made stays final */
  def deleteTxUnlessResaved(txid: TxId): Unit =
    val alreadyDeleted = txDelSet.contains(txid)
    deleteTx(txid)
    if !alreadyDeleted then
      resavable += txid

  /** Unlike a transaction's save, a stub's save after its deletion in this entry is written: the newer of the two
    * wins, so an entry never holds both for one id */
  def addStub(stub: Stub, completionHandler: () => Unit): Unit =
    stubDeletions -= stub.id
    stubs += (stub.id -> stub)
    completionHandlers = completionHandler :: completionHandlers

  def deleteStub(txid: TxId): Unit =
    stubs -= txid
    stubDeletions += txid

  def createEntryBuffers(startingOffset: Long,
                         streamUUID: UUID,
                         streamId: StreamId): Array[ByteBuffer] =

    val dynDataSize = dynamicDataSize
    val statDataSize = staticDataSize
    var buffers: List[ByteBuffer] = Nil

    // Header ---------
    val harr = new Array[Byte](StaticEntryHeaderSize)
    val hbuff = ByteBuffer.wrap(harr)

    Tx.putUUID(hbuff, streamUUID)
    hbuff.putLong(entrySerialNumber)
    hbuff.putLong(oldestEntryNeeded)
    Tx.putStreamLocation(hbuff, previousEntryLocation)
    hbuff.putLong(dynDataSize)
    hbuff.putLong(statDataSize)
    hbuff.putInt(txs.size)
    hbuff.putInt(txDeletions.length)
    hbuff.putInt(stubs.size)
    hbuff.putInt(stubDeletions.size)
    // body_crc and header_crc are filled in once the body is built

    buffers = ByteBuffer.wrap(harr) :: buffers
    val bodyCrc = new CRC32C

    // DynamicData ---------
    var offset = startingOffset + StaticEntryHeaderSize

    def allocate(buff: ByteBuffer): StreamLocation =
      val loc = StreamLocation(streamId, offset, buff.remaining())
      offset += buff.remaining()
      buffers = buff :: buffers
      bodyCrc.update(buff.duplicate())
      loc

    txs.valuesIterator.foreach: tx =>
      if tx.txdLocation.isEmpty then
        tx.txdLocation = Some(allocate(tx.state.serializedTxd.asReadOnlyBuffer()))

      if tx.keepObjectUpdates && tx.objectUpdateLocations.isEmpty then
        tx.objectUpdateLocations = Some(tx.state.objectUpdates.map: ou =>
          (ou.objectId, allocate(ou.data.asReadOnlyBuffer()))
        )

    stubs.valuesIterator.foreach: stub =>
      if stub.anchor.isEmpty then
        stub.anchor = Some(StreamLocation(streamId, offset, 0))

    // StaticData ---------
    val staticArr = new Array[Byte](statDataSize.toInt)

    val bb = ByteBuffer.wrap(staticArr)

    txs.valuesIterator.foreach(_.writeStaticEntry(bb))
    stubs.valuesIterator.foreach(_.writeStaticEntry(bb))
    txDeletions.foreach(Tx.putTxId(bb, _))
    stubDeletions.foreach(Tx.putTxId(bb, _))
    Tx.putUUID(bb, streamUUID)

    assert(bb.position() == bb.limit)

    buffers = ByteBuffer.wrap(staticArr) :: buffers

    bodyCrc.update(staticArr)
    hbuff.putInt(BodyCrcOffset, bodyCrc.getValue.toInt)
    hbuff.putInt(HeaderCrcOffset, headerCrc(harr))

    // Pad to 4k alignment
    val nPadBytes = padTo4k(startingOffset + StaticEntryHeaderSize + dynDataSize + statDataSize)

    if nPadBytes != 0 then
      buffers = ByteBuffer.allocate(nPadBytes) :: buffers

    buffers.reverse.toArray


object LogEntry:

  val StaticEntryHeaderSize: Int = 16 + 8 + 8 + 14 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4

  private val BodyCrcOffset: Int = StaticEntryHeaderSize - 8
  private val HeaderCrcOffset: Int = StaticEntryHeaderSize - 4

  /** CRC32C of a header's bytes before its header_crc */
  def headerCrc(header: Array[Byte]): Int =
    val crc = new CRC32C
    crc.update(header, 0, HeaderCrcOffset)
    crc.getValue.toInt

  /** Whether `header` (StaticEntryHeaderSize bytes) carries the checksum of its own fields */
  def headerIntact(header: Array[Byte]): Boolean =
    ByteBuffer.wrap(header).getInt(HeaderCrcOffset) == headerCrc(header)

  /// Entry Header
  ///   stream_uuid - 16
  ///   entry_serial_number - 8
  ///   oldest_entry_needed - 8
  ///   prev_entry_file_location - 14 (2 + 8 + 4)
  ///   dynamic_data_size - 8
  ///   static_data_size - 8
  ///   num_transactions - 4
  ///   num_tx_deletions - 4
  ///   num_applied_stubs - 4
  ///   num_applied_stub_deletions - 4
  ///   body_crc - 4: CRC32C of the dynamic and static data, through the trailing UUID (not the padding)
  ///   header_crc - 4: CRC32C of the header bytes before it
  ///
  /// Recovery takes nothing from an entry whose checksums fail (Recovery, crash-recovery-log F5)
  ///
  /// Static data: transactions, applied stubs (TxId and start timestamp each), transaction deletions, applied stub
  /// deletions (TxId each), stream UUID
  case class EntryHeader(streamUUID: UUID,
                         entrySerialNumber: Long,
                         oldestEntryNeeded: Long,
                         previousEntryStreamLocation: StreamLocation,
                         dynamicDataSize: Long,
                         staticDataSize: Long,
                         numTransactions: Int,
                         numDeletedTransactions: Int,
                         numAppliedStubs: Int,
                         numDeletedAppliedStubs: Int,
                         bodyCrc: Int):
    def trailingUUIDOffset: Long =
      StaticEntryHeaderSize + dynamicDataSize + staticDataSize - 16

    def entrySize: Long =
      val base = StaticEntryHeaderSize + dynamicDataSize + staticDataSize
      base + padTo4k(base)


  /** An applied stub found by recovery: its start timestamp, and the anchor and serial number of the newest entry
    * holding it, which the opened CRL keeps as its own (SimpleCRL writes nothing at open) */
  case class LoadingStub(start: HLCTimestamp, anchor: StreamLocation, entrySerialNumber: Long)

  class RecoveringState(var txs: HashMap[TxId, Tx.LoadingTx],
                        var txDeletions: Set[TxId],
                        var stubs: HashMap[TxId, LoadingStub] = new HashMap(),
                        var stubDeletions: Set[TxId] = Set())


  def loadHeader(hbuff: ByteBuffer): EntryHeader =
    val streamUUID = Tx.getUUID(hbuff)
    val entrySerialNumber = hbuff.getLong()
    val oldestEntryNeeded = hbuff.getLong()
    val previousEntryLocation = Tx.getStreamLocation(hbuff)
    val dynamicDataSize = hbuff.getLong()
    val staticDataSize = hbuff.getLong()
    val numTransactions = hbuff.getInt()
    val numTxDeletions = hbuff.getInt()
    val numStubs = hbuff.getInt()
    val numStubDeletions = hbuff.getInt()
    val bodyCrc = hbuff.getInt()

    EntryHeader(streamUUID, entrySerialNumber, oldestEntryNeeded, previousEntryLocation, dynamicDataSize,
      staticDataSize, numTransactions, numTxDeletions, numStubs, numStubDeletions, bodyCrc)


  /** `stubAnchor`: where createEntryBuffers put the anchor of a stub first written in this entry, the end of its
    * dynamic data */
  def loadStaticEntryContent(header: EntryHeader,
                             bb: ByteBuffer,
                             rstate: RecoveringState,
                             stubAnchor: StreamLocation): Unit =

    // Entries are loaded newest to oldest, so rstate already holds the content of every newer
    // entry. A deletion therefore applies only to saves in this entry (a save is never added
    // after a delete of the same id within one entry) and to saves in older entries. It must
    // not remove a save from a newer entry: that is a re-save of a previously deleted id.
    val loaded = List.fill(header.numTransactions)(Tx.loadTx(bb))
    val stubsHere = List.fill(header.numAppliedStubs)(Stub.load(bb))
    val deletedHere = List.fill(header.numDeletedTransactions)(Tx.getTxId(bb)).toSet
    val stubsDeletedHere = List.fill(header.numDeletedAppliedStubs)(Tx.getTxId(bb)).toSet

    loaded.foreach: ltx =>
      if !rstate.txDeletions.contains(ltx.id) && !deletedHere.contains(ltx.id) && !rstate.txs.contains(ltx.id) then
        rstate.txs += (ltx.id -> ltx)

    rstate.txDeletions ++= deletedHere

    // Applied stubs are separate entries, with the same rule. An entry never holds a stub and its deletion
    // (LogEntry.addStub), so only newer deletions can remove one
    stubsHere.foreach: (id, start) =>
      if !rstate.stubDeletions.contains(id) && !stubsDeletedHere.contains(id) && !rstate.stubs.contains(id) then
        rstate.stubs += (id -> LoadingStub(start, stubAnchor, header.entrySerialNumber))

    rstate.stubDeletions ++= stubsDeletedHere


  def padTo4k(offset: Long): Int =
    if (offset < 4096)
      4096 - offset.toInt
    else
      val remainder = offset % 4096
      if remainder == 0 then
        0
      else
        4096 - remainder.toInt
