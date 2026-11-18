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


/** Where one piece of a record's dynamic data lies (a txd, an object update, a stub's anchor), and the serial number
  * of the entry holding it, which the piece keeps in LivePieces while the record's newest static record names it */
case class Piece(location: StreamLocation, serial: Long)


/** A live entry of the CRL: a transaction's state (Tx) or an applied stub (Stub). Each of its pieces is located on its
  * own, so a rotation relocates only the pieces lying in the streams it evacuates */
sealed abstract class Record(val id: TxId):
  /** The serials of pieces forgotten since its static record was last built. Its newest durable static record may
    * still name them, so they stay in LivePieces until the entry holding its next static record is built */
  private var released: List[Long] = Nil

  protected def release(piece: Piece): Unit = released = piece.serial :: released

  /** Its located pieces, each counted in LivePieces under its serial. A piece is located when the entry writing it
    * builds its buffers. Dropped updates stay here until the record's next static record is built */
  def pieces: Iterator[Piece]

  def piecesIn(stream: StreamId): Boolean = pieces.exists(_.location.streamId == stream)

  /** Forgets the pieces lying in `stream`, so the next entry holding the record writes them again */
  def unlocate(stream: StreamId): Unit

  /** Called once the entry holding its new static record has located every piece: the static record no longer names
    * the forgotten pieces, so they are unpinned. Earlier entries' oldestEntryNeeded were stamped while they were
    * pinned, so a crash before this entry is durable recovers them from the last durable entry */
  def unpinReleased(live: LivePieces): Unit =
    released.foreach(live.remove)
    released = Nil

  /** The record is deleted: every serial it pins is removed. The deletion's entry and every later one may stamp
    * oldestEntryNeeded past them: a recovery walking back from any of those reaches no save of the record without first
    * reaching its deletion, so it needs none of them. Every earlier entry was stamped while they were pinned */
  def forget(live: LivePieces): Unit =
    pieces.foreach(p => live.remove(p.serial))
    unpinReleased(live)
    unlocateAll()

  protected def unlocateAll(): Unit

  def dynamicDataSize: Long

  def staticDataSize: Long

  def writeStaticEntry(bb: ByteBuffer): Unit


/** An applied stub (StoreRecoveryState): its static record is its TxId and its transaction's start timestamp (by which
  * its store expires it). `anchor` is a zero-length location in the entry that wrote it, so a rotation recycling that
  * stream copies it forward as it does a transaction's data */
final class Stub(id: TxId, val start: HLCTimestamp) extends Record(id):
  var anchor: Option[Piece] = None

  def pieces: Iterator[Piece] = anchor.iterator

  def unlocate(stream: StreamId): Unit = anchor match
    case Some(p) if p.location.streamId == stream =>
      release(p)
      anchor = None
    case _ =>

  protected def unlocateAll(): Unit = anchor = None

  def dynamicDataSize: Long = 0

  def staticDataSize: Long = Stub.StaticSize

  def writeStaticEntry(bb: ByteBuffer): Unit =
    Tx.putTxId(bb, id)
    bb.putLong(start.asLong)


object Stub:
  /** TxId and start timestamp */
  val StaticSize: Long = TxId.StaticSize + 8

  def load(bb: ByteBuffer): (TxId, HLCTimestamp) =
    val id = Tx.getTxId(bb)
    (id, HLCTimestamp(bb.getLong()))


/** `updatePieces` has one element per `state.objectUpdates`, by index. With `keepObjectUpdates` false
  * (dropTransactionObjectData) the update pieces are left out of its next static record, and stay pinned until it is
  * built */
final class Tx(id: TxId,
         private var _state: TransactionRecoveryState,
         var keepObjectUpdates: Boolean = true) extends Record(id):

  var txdPiece: Option[Piece] = None
  var updatePieces: Vector[Option[Piece]] = Vector.fill(_state.objectUpdates.size)(None)

  def state: TransactionRecoveryState = _state

  /** A state with another update count (a driver's data-less prepare, then a client's prepare with the data; a save
    * without one object's update) forgets every update piece, so the next entry writes the new updates. The txd keeps
    * its location */
  def state_=(s: TransactionRecoveryState): Unit =
    if s.objectUpdates.size != _state.objectUpdates.size then
      updatePieces.flatten.foreach(release)
      updatePieces = Vector.fill(s.objectUpdates.size)(None)
    _state = s

  def pieces: Iterator[Piece] = txdPiece.iterator ++ updatePieces.iterator.flatten

  def unlocate(stream: StreamId): Unit =
    txdPiece.filter(_.location.streamId == stream).foreach: p =>
      release(p)
      txdPiece = None
    updatePieces = updatePieces.map:
      case Some(p) if p.location.streamId == stream =>
        release(p)
        None
      case other => other

  protected def unlocateAll(): Unit =
    txdPiece = None
    updatePieces = Vector.fill(state.objectUpdates.size)(None)

  /** The entry holding its next static record is being built and the data was dropped: the update pieces are left
    * out of it, and unpinned with it (unpinReleased) */
  def releaseDroppedUpdates(): Unit =
    if !keepObjectUpdates then
      updatePieces.flatten.foreach(release)
      updatePieces = Vector.fill(state.objectUpdates.size)(None)

  def dynamicDataSize: Long =
    // Dropped, or already written to a stream not evacuated since: no need to write them now
    val updatesSize = if !keepObjectUpdates then
      0L
    else
      updatePieces.lazyZip(state.objectUpdates).foldLeft(0L):
        case (sum, (None, ou)) => sum + ou.data.size
        case (sum, _) => sum

    val txdSize = if txdPiece.nonEmpty then 0 else state.serializedTxd.size

    updatesSize + txdSize


  def staticDataSize: Long =
    // Transaction Entry
    //     txid:  33 bytes - store id (16-bye pool id + 1 byte index) + 16 byte Transaction UUID
    //     serialized_transaction_description: FileLocation, 14
    //     tx_disposition: TransactionDisposition, 1
    //     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
    //         bit 0 - have promise proposal
    //         bit 1 - have accepted proposal
    //         bit 2 - accepted boolean value (only valid if bit 1 is set)
    //         bits 3-4 - the saved TransactionStatus: 0 Unresolved, 1 Committed, 2 Aborted. Recovery restores it, so a
    //                    crash returns the status readStoreState does (crash-recovery-log F14)
    //     object_updates: 4-byte-count, num_updates * (16:objuuid + FileLocation))
    //
    val base = TxId.StaticSize + StreamLocation.StaticSize + 1 + 11 + 4

    if keepObjectUpdates then
      base + state.objectUpdates.size * (16 + StreamLocation.StaticSize)
    else
      base

  def writeStaticEntry(bb: ByteBuffer): Unit =
    require(txdPiece.nonEmpty) // Must be set
    if keepObjectUpdates then
      require(updatePieces.size == state.objectUpdates.size && updatePieces.forall(_.nonEmpty))

    Tx.putTxId(bb, id)

    txdPiece.foreach(p => Tx.putStreamLocation(bb, p.location))

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

    if keepObjectUpdates then
      bb.putInt(state.objectUpdates.length)
      state.objectUpdates.lazyZip(updatePieces).foreach: (ou, p) =>
        Tx.putUUID(bb, ou.objectId.uuid)
        Tx.putStreamLocation(bb, p.get.location)
    else
      bb.putInt(0)

  /** Writes nothing: its next static record leaves the update pieces out (releaseDroppedUpdates), and until then they
    * stay located and pinned, as the newest durable static record still names them */
  def dropTransactionObjectData(): Unit =
    keepObjectUpdates = false
