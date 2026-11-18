package org.aspen_ddp.aspen.server.crl.simple

import scribe.Logging

import org.aspen_ddp.aspen.common.HLCTimestamp

import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32C
import scala.collection.immutable.HashMap

/** What an entry writes for one transaction */
sealed trait TxWrite:
  def form: TxForm
  def dynamicDataSize: Long

/** A save's form (crash-recovery-log 6.1, W1a). `writesPieces`: the entry writes the form's txd and kept updates.
  * Otherwise it names the pieces of the record's last-built form where they lie at its build: an earlier entry wrote
  * them (FIFO, so it is built first), and an evacuation may have moved them since */
final case class TxSave(form: TxForm, writesPieces: Boolean) extends TxWrite:
  def dynamicDataSize: Long = if writesPieces then form.piecesSize else 0

/** An evacuation's copy of a record's last-built form (W5): its pieces lying in `streams` are written again, the
  * others named where they lie. Nothing pending is written. Built at once, so `built` is the image's form then */
final case class TxEvacuation(built: BuiltTx, streams: Set[StreamId]) extends TxWrite:
  def form: TxForm = built.form
  def dynamicDataSize: Long =
    built.pieces.iterator.filter(p => streams.contains(p.location.streamId)).foldLeft(0L)(_ + _.location.length)

/** One entry, sealed (W1) once it holds as much as SimpleCRL lets it. Its size is fixed by what is added to it: its
  * serial number, previous entry and offset are given only when it is built (createEntryBuffers), since entries
  * queued ahead of it, evacuations among them, take theirs first.
  *
  * @param image the CRL's durable image: building the entry applies its forms and deletions to it, and to the pieces
  *              it pins */
class LogEntry(image: Image) extends Logging:
  import LogEntry._

  private var completionHandlers: List[() => Unit] = Nil
  private var txs: HashMap[TxId, TxWrite] = new HashMap()
  private var txDeletions: List[TxId] = Nil
  private var txDelSet: Set[TxId] = Set()
  private var stubs: HashMap[TxId, HLCTimestamp] = new HashMap()
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
    txs.valuesIterator.foldLeft(0L)((s, tx) => s + tx.form.staticSize)
    + TxId.StaticSize * txDeletions.length
    + Stub.StaticSize * stubs.size
    + TxId.StaticSize * stubDeletions.size
    + 16 // Trailing file UUID. Must match header UUID during load process or corrupted entry

  def dynamicDataSize: Long =
    txs.valuesIterator.foldLeft(0L)((s, t) => s + t.dynamicDataSize)

  def entrySize: Long = padded(StaticEntryHeaderSize + dynamicDataSize + staticDataSize)

  /** Its size with `bytes` more content */
  def entrySizeWith(bytes: Long): Long = padded(StaticEntryHeaderSize + dynamicDataSize + staticDataSize + bytes)

  /** Its size once `save` of `txid` is added, replacing any form of it already here */
  def entrySizeWith(txid: TxId, save: TxWrite): Long =
    val replaced = txs.get(txid).fold(0L)(w => w.form.staticSize + w.dynamicDataSize)
    entrySizeWith(save.form.staticSize + save.dynamicDataSize - replaced)

  /** The form of `txid` this entry writes, if any */
  def txWrite(txid: TxId): Option[TxWrite] = txs.get(txid)

  def runCompletionHandlers(): Unit = completionHandlers.foreach(ch => ch())

  def addTx(txid: TxId, save: TxWrite, completionHandler: () => Unit): Unit =
    // Do not check if txs already contains the transaction. Allow the duplicate insert so we queue
    // the completion handler for the subsequent insert attempts.
    //
    // Deletions are final (deletesFinally). A save of a transaction deleted earlier in an unwritten entry is a timing
    // artifact and is skipped, but it still reports success once the delete is durable. SimpleCRL never adds such a
    // save here: it queues only the handler, since readStoreState would report the record live (crash-recovery-log
    // F4, W1b).
    //
    // A deletion made by a stub's save or a dropStore (deleteTxUnlessResaved) is not final: a save after it is a new
    // state of the transaction (live beside the stub, or of the store loaded again). The save cancels that deletion,
    // so it is written; recovery takes the newest save
    if resavable.contains(txid) then
      resavable -= txid
      txDelSet -= txid
      txDeletions = txDeletions.filterNot(_ == txid)
    if !deletesFinally(txid) then
      txs += (txid -> save)
    completionHandlers = completionHandler :: completionHandlers

  /** True when this entry deletes `txid` and a save of it would not cancel the deletion: the save is skipped (addTx) */
  def deletesFinally(txid: TxId): Boolean =
    txDelSet.contains(txid) && !resavable.contains(txid)

  /** Runs `completionHandler` once this entry is durable */
  def addCompletionHandler(completionHandler: () => Unit): Unit =
    completionHandlers = completionHandler :: completionHandlers

  /** A save of `txid` already in this entry is taken out: its record is deleted. Recovery would skip it beside the
    * deletion anyway. Saves of it in older entries stay: they are built first, and this deletion supersedes them */
  def deleteTx(txid: TxId): Unit =
    txs -= txid
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
  def addStub(txid: TxId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
    stubDeletions -= txid
    stubs += (txid -> start)
    completionHandlers = completionHandler :: completionHandlers

  def deleteStub(txid: TxId): Unit =
    stubs -= txid
    stubDeletions += txid

  /** Builds the entry's buffers, as entry `entrySerialNumber` linking to `previousEntryLocation`, and applies it to
    * the image: each form it writes becomes its record's last-built form, each deletion takes its record out. The
    * pieces it writes are pinned, and the pieces its forms or deletions supersede unpinned, before oldestEntryNeeded is
    * stamped. Until this entry is durable, the last durable one, stamped while the superseded pieces were pinned, still
    * covers them */
  def createEntryBuffers(entrySerialNumber: Long,
                         previousEntryLocation: StreamLocation,
                         startingOffset: Long,
                         crlId: UUID,
                         streamUUID: UUID,
                         streamId: StreamId): Array[ByteBuffer] =

    val dynDataSize = dynamicDataSize
    val statDataSize = staticDataSize
    val livePieces = image.livePieces
    var dynamicBuffers: List[ByteBuffer] = Nil
    val bodyCrc = new CRC32C

    // DynamicData ---------
    var offset = startingOffset + StaticEntryHeaderSize

    def allocate(data: org.aspen_ddp.aspen.common.DataBuffer): Piece =
      val buff = data.asReadOnlyBuffer()
      val loc = StreamLocation(streamId, offset, buff.remaining())
      offset += buff.remaining()
      dynamicBuffers = buff :: dynamicBuffers
      bodyCrc.update(buff.duplicate())
      livePieces.add(entrySerialNumber)
      Piece(loc, entrySerialNumber)

    def lastBuilt(txid: TxId): BuiltTx = image.txs.getOrElse(txid,
      throw IllegalStateException(s"CRL entry $entrySerialNumber names the pieces of $txid, which has no built form"))

    val builtTxs: List[(TxId, BuiltTx)] = txs.toList.map: (txid, write) =>
      val built = write match
        case TxSave(form, true) => BuiltTx(form, form.pieceData.map(allocate))
        case TxSave(form, false) =>
          val old = lastBuilt(txid)
          // Same update count as the form before it (W4), and data is never kept again once dropped
          require(form.updateCount == old.form.updateCount || !form.keepUpdates,
            s"CRL form of $txid changes its update count without writing its pieces")
          BuiltTx(form, if form.keepUpdates then old.pieces else old.pieces.take(1))
        case TxEvacuation(b, streams) =>
          require(lastBuilt(txid) eq b, s"CRL evacuation of $txid is not of its last-built form")
          BuiltTx(b.form, b.pieces.lazyZip(b.form.pieceData).map: (p, data) =>
            if streams.contains(p.location.streamId) then allocate(data) else p)
      image.txs.get(txid).foreach(_.pieces.filterNot(built.pieces.contains).foreach(p => livePieces.remove(p.serial)))
      image.txs += txid -> built
      txid -> built

    // Every stub here is anchored at the end of this entry's dynamic data, where recovery finds it
    val anchor = StreamLocation(streamId, offset, 0)
    stubs.foreach: (txid, start) =>
      livePieces.add(entrySerialNumber)
      image.stubs.get(txid).foreach(old => livePieces.remove(old.anchor.serial))
      image.stubs += txid -> BuiltStub(start, Piece(anchor, entrySerialNumber))

    // A deletion takes its record out of the image only now (W3): until this entry is durable, a recovery starts from
    // an entry naming the record live, so its pieces stayed pinned and evacuations carried it
    txDeletions.foreach: txid =>
      image.txs.get(txid).foreach(_.pieces.foreach(p => livePieces.remove(p.serial)))
      image.txs -= txid
    stubDeletions.foreach: txid =>
      image.stubs.get(txid).foreach(old => livePieces.remove(old.anchor.serial))
      image.stubs -= txid

    // oldestEntryNeeded is stamped now: after this entry's pieces are added and the pieces it supersedes removed, so it
    // is the oldest entry holding a piece of the image once this entry is durable. Every built record has a piece (a
    // txd, an anchor), and its static record is in no older entry than any of them, so the walk back to it reaches
    // every live static record too
    val oldest = livePieces.oldest.getOrElse(entrySerialNumber)

    // Header ---------
    val harr = new Array[Byte](StaticEntryHeaderSize)
    val hbuff = ByteBuffer.wrap(harr)

    Tx.putUUID(hbuff, streamUUID)
    Tx.putUUID(hbuff, crlId)
    hbuff.putLong(entrySerialNumber)
    hbuff.putLong(oldest)
    Tx.putStreamLocation(hbuff, previousEntryLocation)
    hbuff.putLong(dynDataSize)
    hbuff.putLong(statDataSize)
    hbuff.putInt(txs.size)
    hbuff.putInt(txDeletions.length)
    hbuff.putInt(stubs.size)
    hbuff.putInt(stubDeletions.size)
    // body_crc and header_crc are filled in once the body is built

    var buffers: List[ByteBuffer] = dynamicBuffers ++ List(ByteBuffer.wrap(harr))

    // StaticData ---------
    val staticArr = new Array[Byte](statDataSize.toInt)

    val bb = ByteBuffer.wrap(staticArr)

    builtTxs.foreach((txid, built) => Tx.writeStatic(bb, txid, built.form, built.pieces))
    stubs.foreach((txid, start) => Stub.writeStatic(bb, txid, start))
    txDeletions.foreach(Tx.putTxId(bb, _))
    stubDeletions.foreach(Tx.putTxId(bb, _))
    Tx.putUUID(bb, streamUUID)

    assert(bb.position() == bb.limit)
    assert(offset == startingOffset + StaticEntryHeaderSize + dynDataSize)

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

  val StaticEntryHeaderSize: Int = 16 + 16 + 8 + 8 + 14 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4

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
  ///   crl_id - 16: the id of the CRL that wrote the entry (SimpleCRL.Sizing.crlId). A header bearing another id is not
  ///     an entry of this CRL, so object data in its streams (a crafted entry, an image of another CRL) never passes
  ///     for one
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
                         crlId: UUID,
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
    val crlId = Tx.getUUID(hbuff)
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

    EntryHeader(streamUUID, crlId, entrySerialNumber, oldestEntryNeeded, previousEntryLocation, dynamicDataSize,
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


  /** An entry of `size` bytes with its padding to the next 4 KiB boundary */
  def padded(size: Long): Long = size + padTo4k(size)

  def padTo4k(offset: Long): Int =
    if (offset < 4096)
      4096 - offset.toInt
    else
      val remainder = offset % 4096
      if remainder == 0 then
        0
      else
        4096 - remainder.toInt
