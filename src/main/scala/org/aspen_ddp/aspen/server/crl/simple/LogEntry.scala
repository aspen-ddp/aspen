package org.aspen_ddp.aspen.server.crl.simple

import org.apache.logging.log4j.scala.Logging

import java.nio.ByteBuffer
import java.util.UUID
import scala.collection.immutable.HashMap

class LogEntry(val previousEntryLocation: StreamLocation,
               val entrySerialNumber: Long,
               val oldestEntryNeeded: Long) extends Logging:
  import LogEntry._

  private var completionHandlers: List[() => Unit] = Nil
  private var txs: HashMap[TxId, Tx] = new HashMap()
  private var txDeletions: List[TxId] = Nil
  private var txDelSet: Set[TxId] = Set()


  def isEmpty: Boolean =
    txs.isEmpty
      && txDeletions.isEmpty


  def staticDataSize: Long =
    txs.valuesIterator.foldLeft(0L)((s, tx) => s + tx.staticDataSize)
    + TxId.StaticSize * txDeletions.length
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
    // the oompletion handler for the subsequent insert attempts
    if !txDelSet.contains(tx.id) then
      txs += (tx.id -> tx)
      completionHandlers = completionHandler :: completionHandlers

  def deleteTx(txid: TxId): Unit =
    txDeletions = txid :: txDeletions
    txDelSet = txDelSet + txid

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

    buffers = ByteBuffer.wrap(harr) :: buffers

    // DynamicData ---------
    var offset = startingOffset + StaticEntryHeaderSize

    def allocate(buff: ByteBuffer): StreamLocation =
      val loc = StreamLocation(streamId, offset, buff.remaining())
      offset += buff.remaining()
      buffers = buff :: buffers
      loc

    txs.valuesIterator.foreach: tx =>
      if tx.txdLocation.isEmpty then
        tx.txdLocation = Some(allocate(tx.state.serializedTxd.asReadOnlyBuffer()))

      if tx.keepObjectUpdates && tx.objectUpdateLocations.isEmpty then
        tx.objectUpdateLocations = Some(tx.state.objectUpdates.map: ou =>
          (ou.objectId, allocate(ou.data.asReadOnlyBuffer()))
        )

    // StaticData ---------
    val staticArr = new Array[Byte](statDataSize.toInt)

    val bb = ByteBuffer.wrap(staticArr)

    txs.valuesIterator.foreach(_.writeStaticEntry(bb))
    txDeletions.foreach(Tx.putTxId(bb, _))
    Tx.putUUID(bb, streamUUID)

    assert(bb.position() == bb.limit)

    buffers = ByteBuffer.wrap(staticArr) :: buffers

    // Pad to 4k alignment
    val nPadBytes = padTo4k(startingOffset + StaticEntryHeaderSize + dynDataSize + statDataSize)

    if nPadBytes != 0 then
      buffers = ByteBuffer.allocate(nPadBytes) :: buffers

    buffers.reverse.toArray


object LogEntry:

  val StaticEntryHeaderSize: Int = 16 + 8 + 8 + 14 + 8 + 8 + 4 + 4

  /// Entry Header
  ///   stream_uuid - 16
  ///   entry_serial_number - 8
  ///   oldest_entry_needed - 8
  ///   prev_entry_file_location - 14 (2 + 8 + 4)
  ///   dynamic_data_size - 8
  ///   static_data_size - 8
  ///   num_transactions - 4
  ///   num_tx_deletions - 4
  case class EntryHeader(streamUUID: UUID,
                         entrySerialNumber: Long,
                         oldestEntryNeeded: Long,
                         previousEntryStreamLocation: StreamLocation,
                         dynamicDataSize: Long,
                         staticDataSize: Long,
                         numTransactions: Int,
                         numDeletedTransactions: Int):
    def trailingUUIDOffset: Long =
      StaticEntryHeaderSize + dynamicDataSize + staticDataSize - 16

    def entrySize: Long =
      val base = StaticEntryHeaderSize + dynamicDataSize + staticDataSize
      base + padTo4k(base)


  class RecoveringState(var txs: HashMap[TxId, Tx.LoadingTx],
                        var txDeletions: Set[TxId])


  def loadHeader(hbuff: ByteBuffer): EntryHeader =
    val streamUUID = Tx.getUUID(hbuff)
    val entrySerialNumber = hbuff.getLong()
    val oldestEntryNeeded = hbuff.getLong()
    val previousEntryLocation = Tx.getStreamLocation(hbuff)
    val dynamicDataSize = hbuff.getLong()
    val staticDataSize = hbuff.getLong()
    val numTransactions = hbuff.getInt()
    val numTxDeletions = hbuff.getInt()

    EntryHeader(streamUUID, entrySerialNumber, oldestEntryNeeded, previousEntryLocation, dynamicDataSize,
      staticDataSize, numTransactions, numTxDeletions)


  def loadStaticEntryContent(header: EntryHeader,
                             bb: ByteBuffer,
                             rstate: RecoveringState): Unit =

    for (_ <- 0 until header.numTransactions)
      val ltx = Tx.loadTx(bb)
      if !rstate.txDeletions.contains(ltx.id) && !rstate.txs.contains(ltx.id) then
        rstate.txs += (ltx.id -> ltx)

    for (_ <- 0 until header.numDeletedTransactions)
      val txid = Tx.getTxId(bb)
      rstate.txs -= txid
      rstate.txDeletions += txid


  def padTo4k(offset: Long): Int =
    if (offset < 4096)
      4096 - offset.toInt
    else
      val remainder = offset % 4096
      if remainder == 0 then
        0
      else
        4096 - remainder.toInt
