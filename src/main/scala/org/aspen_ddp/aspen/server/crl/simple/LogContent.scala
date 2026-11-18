package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.TransactionRecoveryState

import java.nio.ByteBuffer
import java.util.UUID
import scala.collection.immutable.HashMap


object Tx:

  // 16 bytes - 8 bytes MostSignificantBits, 8 bytes LeastSignificantBits
  def putUUID(bb: ByteBuffer, uuid: UUID): Unit =
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)

  // 33 bytes - 16 pool UUID, 1 byte Index, 16 byte UUID
  def putTxId(bb: ByteBuffer, txid: TxId): Unit =
    putUUID(bb, txid.storeId.poolId.uuid)
    bb.put(txid.storeId.poolIndex)
    putUUID(bb, txid.transactionId.uuid)

  // 14 bytes - 2 byte stream index, 8 byte offset, 4 byte length
  def putStreamLocation(bb: ByteBuffer, loc: StreamLocation): Unit =
    bb.putShort(loc.streamId.number.toShort)
    bb.putLong(loc.offset)
    bb.putInt(loc.length)

  def getUUID(bb: ByteBuffer): UUID =
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new java.util.UUID(msb, lsb)

  def getTxId(bb: ByteBuffer): TxId =
    val poolId = getUUID(bb)
    val storeIndes = bb.get()
    val transactionId = getUUID(bb)
    TxId(StoreId(PoolId(poolId), storeIndes), TransactionId(transactionId))

  def getStreamLocation(bb: ByteBuffer): StreamLocation =
    val streamId = bb.getShort()
    val offset = bb.getLong()
    val length = bb.getInt()
    StreamLocation(StreamId(streamId), offset, length)

  case class LoadingTx(id: TxId,
                       txdLocation: StreamLocation,
                       updateLocations: Option[List[(ObjectId, StreamLocation)]],
                       disposition: TransactionDisposition.Value,
                       status: TransactionStatus.Value,
                       paxosAcceptorState: PersistentState)

  def loadTx(bb: ByteBuffer): LoadingTx =
    val txid = getTxId(bb)
    val txdLocation = getStreamLocation(bb)
    val disposition = bb.get() match
      case 0 => TransactionDisposition.Undetermined
      case 1 => TransactionDisposition.VoteCommit
      case 2 => TransactionDisposition.VoteAbort
      case x => throw CorruptedEntry(s"Invalid disposition encoding value: $x")

    val pmask = bb.get()
    val prom_num = bb.getInt()
    val prom_peer = bb.get()
    val accept_num = bb.getInt()
    val accept_peer = bb.get()
    val promised = if ((pmask & 1 << 0) != 0) Some(ProposalId(prom_num, prom_peer)) else None
    val accepted = if ((pmask & 1 << 1) != 0) Some((ProposalId(accept_num, accept_peer), (pmask & 1 << 2) != 0)) else None
    val paxState = PersistentState(promised, accepted)
    val status = (pmask >> 3) & 3 match
      case 0 => TransactionStatus.Unresolved
      case 1 => TransactionStatus.Committed
      case 2 => TransactionStatus.Aborted
      case x => throw CorruptedEntry(s"Invalid status encoding value: $x")
    val numObjectUpdates = bb.getInt()
    var updateLocations: Option[List[(ObjectId, StreamLocation)]] = None
    var updateList: List[(ObjectId, StreamLocation)] = Nil

    for (_ <- 0 until numObjectUpdates)
      val uuid = getUUID(bb)
      val loc = getStreamLocation(bb)
      updateList = (ObjectId(uuid), loc) :: updateList

    updateLocations = if numObjectUpdates <= 0 then
      None
    else
      Some(updateList.reverse)

    LoadingTx(txid, txdLocation, updateLocations, disposition, status, paxState)


  /** A transaction's static record without its updates: TxId 33, txd location 14, disposition 1, Paxos state 11
    * (mask 1, promised 5, accepted 5), update count 4 */
  val StaticBaseSize: Long = TxId.StaticSize + StreamLocation.StaticSize + 1 + 11 + 4

  /** Each update in a static record: object UUID 16 and its data's location */
  val UpdateStaticSize: Long = 16 + StreamLocation.StaticSize

  /** Writes the static record of `form` naming `pieces` (its txd, then each kept update, as TxForm.pieceData)
    *
    *    txid:  33 bytes - store id (16-bye pool id + 1 byte index) + 16 byte Transaction UUID
    *    serialized_transaction_description: FileLocation, 14
    *    tx_disposition: TransactionDisposition, 1
    *    paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
    *        bit 0 - have promise proposal
    *        bit 1 - have accepted proposal
    *        bit 2 - accepted boolean value (only valid if bit 1 is set)
    *        bits 3-4 - the saved TransactionStatus: 0 Unresolved, 1 Committed, 2 Aborted. Recovery restores it, so a
    *                   crash returns the status readStoreState does (crash-recovery-log F14)
    *    object_updates: 4-byte-count, num_updates * (16:objuuid + FileLocation))
    */
  def writeStatic(bb: ByteBuffer, id: TxId, form: TxForm, pieces: Vector[Piece]): Unit =
    require(pieces.size == 1 + form.updateCount)
    val state = form.state
    putTxId(bb, id)
    putStreamLocation(bb, pieces.head.location)

    val encodedDisposition = state.disposition match
      case TransactionDisposition.Undetermined => 0
      case TransactionDisposition.VoteCommit => 1
      case TransactionDisposition.VoteAbort => 2
    bb.put(encodedDisposition.toByte)

    var mask = 0
    if state.paxosAcceptorState.promised.isDefined then
      mask |= 1 << 0
    state.paxosAcceptorState.accepted.foreach: t =>
      mask |= 1 << 1
      if t._2 then
        mask |= 1 << 2 // Mark as accepted
    mask |= (state.status match
      case TransactionStatus.Unresolved => 0
      case TransactionStatus.Committed => 1
      case TransactionStatus.Aborted => 2) << 3

    bb.put(mask.toByte)

    state.paxosAcceptorState.promised match
      case None =>
        bb.putInt(0)
        bb.put(0.toByte)
      case Some(p) =>
        bb.putInt(p.number)
        bb.put(p.peer)

    state.paxosAcceptorState.accepted match
      case None =>
        bb.putInt(0)
        bb.put(0.toByte)
      case Some(t) =>
        bb.putInt(t._1.number)
        bb.put(t._1.peer)

    bb.putInt(form.updateCount)
    if form.keepUpdates then
      state.objectUpdates.lazyZip(pieces.tail).foreach: (ou, p) =>
        putUUID(bb, ou.objectId.uuid)
        putStreamLocation(bb, p.location)


/** Where one piece of a record's dynamic data lies (a txd, an object update, a stub's anchor), and the serial number
  * of the entry holding it, which the piece keeps in LivePieces while the record's last-built form names it */
case class Piece(location: StreamLocation, serial: Long)


/** The form of a transaction an entry writes, taken when the save is added to it (crash-recovery-log 6.1, W1a): the
  * save's immutable state and whether its update data is kept. A later save never changes it, so an entry's size is
  * fixed once its requests are added, and it never writes data charged after it was sealed.
  * `dropTransactionObjectData` affects only forms taken after it */
final case class TxForm(state: TransactionRecoveryState, keepUpdates: Boolean):
  def updateCount: Int = if keepUpdates then state.objectUpdates.size else 0

  def staticSize: Long = Tx.StaticBaseSize + updateCount * Tx.UpdateStaticSize

  /** The contents of its pieces: the txd, then each kept update's data, by index */
  def pieceData: Vector[DataBuffer] =
    state.serializedTxd +: (if keepUpdates then state.objectUpdates.iterator.map(_.data).toVector else Vector())

  def piecesSize: Long = pieceData.foldLeft(0L)(_ + _.size)


/** A record's last-built form (W3), the form of the last built entry holding it, with where its pieces lie:
  * `pieces` follows `form.pieceData`. An evacuation relocates its pieces; nothing else changes it until an entry
  * superseding it (its next form, its deletion) is built */
final case class BuiltTx(form: TxForm, pieces: Vector[Piece]):
  def piecesIn(stream: StreamId): Boolean = pieces.exists(_.location.streamId == stream)


/** An applied stub's last-built form: its static record is its TxId and its transaction's start timestamp (by which
  * its store expires it). `anchor` is a zero-length location at the end of the dynamic data of the entry that wrote
  * it, so an evacuation of that stream carries it as it does a transaction's data */
final case class BuiltStub(start: HLCTimestamp, anchor: Piece)


object Stub:
  /** TxId and start timestamp */
  val StaticSize: Long = TxId.StaticSize + 8

  def writeStatic(bb: ByteBuffer, id: TxId, start: HLCTimestamp): Unit =
    Tx.putTxId(bb, id)
    bb.putLong(start.asLong)

  def load(bb: ByteBuffer): (TxId, HLCTimestamp) =
    val id = Tx.getTxId(bb)
    (id, HLCTimestamp(bb.getLong()))


/** The durable image (crash-recovery-log 6.1, W3): every record no built entry has deleted, in its last-built form.
  * Exactly its pieces are pinned in `livePieces`, so a deleted record keeps its pieces pinned, and evacuations carry
  * it, until the entry holding its deletion is built: until then the last durable entry still names it live. Changed
  * only by an entry's build (LogEntry.createEntryBuffers). Owned by the CRL thread */
final class Image(val livePieces: LivePieces):
  var txs: HashMap[TxId, BuiltTx] = HashMap()
  var stubs: HashMap[TxId, BuiltStub] = HashMap()


/** A live transaction as the CRL holds it in memory: its newest saved state (what readStoreState returns, object data
  * included after dropTransactionObjectData) and the newest form an entry took of it, which decides what the next
  * form writes (SimpleCRL.onSaveTransaction) */
final class LiveTx(var state: TransactionRecoveryState):
  /** False once its data is dropped: forms taken from then on leave the updates out. Never set again */
  var keepObjectUpdates: Boolean = true

  /** The newest form an entry took of it, whether that form writes its pieces, and the number of the entry that took
    * it (SimpleCRL.currentEntryNumber; -1 for a recovered record): a later save into that same entry replaces the form
    * there */
  var latest: TxForm = TxForm(state, keepUpdates = true)
  var latestWritesPieces: Boolean = false
  var latestEntry: Long = -1
