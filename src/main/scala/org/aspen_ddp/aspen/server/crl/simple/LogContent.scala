package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.{TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.server.crl.{AllocationRecoveryState, TransactionRecoveryState}

import java.nio.ByteBuffer
import java.util.UUID

sealed abstract class LogContent:

  var next: Option[LogContent] = None
  var prev: Option[LogContent] = None
  var entrySerialNumber: Long = 0

  def clearDynamicData(): Unit

  def dynamicDataSize: Long
  
  def staticDataSize: Long

  // Sets all optional StreamLocations with matching StreamIds to None and returns
  // true if any matching streams are found
  def closeStream(id: StreamId): Boolean

  def writeStaticEntry(bb: ByteBuffer): Unit


object LogContent:

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


object Tx:
  case class LoadingTx(id: TxId,
                       txdLocation: StreamLocation,
                       updateLocations: Option[List[(ObjectId, StreamLocation)]],
                       disposition: TransactionDisposition.Value,
                       paxosAcceptorState: PersistentState)

  def loadTx(bb: ByteBuffer): LoadingTx =
    val txid = LogContent.getTxId(bb)
    val txdLocation = LogContent.getStreamLocation(bb)
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
    val numObjectUpdates = bb.getInt()
    var updateLocations: Option[List[(ObjectId, StreamLocation)]] = None
    var updateList: List[(ObjectId, StreamLocation)] = Nil

    for (_ <- 0 until numObjectUpdates)
      val uuid = LogContent.getUUID(bb)
      val loc = LogContent.getStreamLocation(bb)
      updateList = (ObjectId(uuid), loc) :: updateList

    updateLocations = if numObjectUpdates <= 0 then
      None
    else
      Some(updateList.reverse)

    LoadingTx(txid, txdLocation, updateLocations, disposition, paxState)


class Tx(val id: TxId,
         var state: TransactionRecoveryState,
         var txdLocation: Option[StreamLocation] = None,
         var objectUpdateLocations: Option[List[(ObjectId, StreamLocation)]] = None,
         var keepObjectUpdates: Boolean = true) extends LogContent:

  override def clearDynamicData(): Unit =
    txdLocation = None
    objectUpdateLocations = None

  override def dynamicDataSize: Long =
    val updatesSize = if !keepObjectUpdates || objectUpdateLocations.nonEmpty then
      0 // Dropped or already written to an active stream. No need to write them now
    else
      state.objectUpdates.foldLeft(0)((sum, ou) => sum + ou.data.size)

    val txdSize = if txdLocation.nonEmpty then 0 else state.serializedTxd.size

    updatesSize + txdSize


  override def staticDataSize: Long =
    // Transaction Entry
    //     txid:  33 bytes - store id (16-bye pool id + 1 byte index) + 16 byte Transaction UUID
    //     serialized_transaction_description: FileLocation, 14
    //     tx_disposition: TransactionDisposition, 1
    //     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
    //         bit 0 - have promise proposal
    //         bit 1 - have accepted proposal
    //         bit 2 - accepted boolean value (only valid if bit 1 is set)
    //     object_updates: 4-byte-count, num_updates * (16:objuuid + FileLocation))
    //
    val base = TxId.StaticSize + StreamLocation.StaticSize + 1 + 11 + 4

    if keepObjectUpdates then
      base + state.objectUpdates.size * (16 + StreamLocation.StaticSize)
    else
      base

  override def writeStaticEntry(bb: ByteBuffer): Unit =
    require(txdLocation.nonEmpty) // Must be set
    if keepObjectUpdates then
      require(state.objectUpdates.isEmpty || objectUpdateLocations.nonEmpty)

    LogContent.putTxId(bb, id)

    txdLocation.foreach(loc => LogContent.putStreamLocation(bb, loc))

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
      objectUpdateLocations.foreach: lst =>
        lst.foreach: tpl =>
          LogContent.putUUID(bb, tpl._1.uuid)
          LogContent.putStreamLocation(bb, tpl._2)
    else
      bb.putInt(0)


  override def closeStream(id: StreamId): Boolean =
    val txd = txdLocation match
      case None => false
      case Some(loc) =>
        if loc.streamId == id then
          txdLocation = None
          true
        else
          false

    val ous = objectUpdateLocations match
      case None => false
      case Some(lst) =>
        if lst.exists(_._2.streamId == id) then
          objectUpdateLocations = None
          true
        else
          false

    txd || ous

  def dropTransactionObjectData(): Unit =
    keepObjectUpdates = false
    objectUpdateLocations = None


object Alloc:
  case class LoadingAlloc( txid: TxId,
                           dataLocation: StreamLocation,
                           storePointer: StorePointer,
                           newObjectId: ObjectId,
                           objectType: ObjectType.Value,
                           objectSize: Option[Int],
                           initialRefcount: ObjectRefcount,
                           timestamp: HLCTimestamp,
                           serializedRevisionGuard: DataBuffer )

  def loadAlloc(bb: ByteBuffer): LoadingAlloc =
    val txid = LogContent.getTxId(bb)
    val spLen = bb.getInt()
    val sparr = new Array[Byte](spLen)
    bb.get(sparr)
    val storePointer = StorePointer(txid.storeId.poolIndex, sparr)
    val newObjectId = ObjectId(LogContent.getUUID(bb))
    val objectType = bb.get() match {
      case 0 => ObjectType.Data
      case 1 => ObjectType.KeyValue
      case _ => throw new CorruptedEntry("Invalid Object Type")
    }
    val sz = bb.getInt()
    val objectSize = if (sz != 0) Some(sz) else None
    val dataLocation = LogContent.getStreamLocation(bb)
    val rserial = bb.getInt()
    val rcount = bb.getInt()
    val refcount = ObjectRefcount(rserial, rcount)
    val timestamp = HLCTimestamp(bb.getLong())
    val guard = new Array[Byte](bb.getInt())
    bb.get(guard)
    val serializedRevisionGuard = DataBuffer(guard)

    LoadingAlloc(txid, dataLocation, storePointer, newObjectId, objectType, objectSize, refcount,
      timestamp, serializedRevisionGuard)


class Alloc(var dataLocation: Option[StreamLocation],
            var state: AllocationRecoveryState) extends LogContent:

  def txid: TxId = TxId(state.storeId, state.allocationTransactionId)

  override def clearDynamicData(): Unit =
    dataLocation = None

  override def dynamicDataSize: Long =
    if dataLocation.isEmpty then
      state.objectData.size
    else
      0

  override def staticDataSize: Long =
    // Allocation Entry
    //     txid: StoreId 17 + allocation_transaction_id 16
    //     store_pointer: StorePointer, 4 + nbytes
    //     id: ObjectId, 16
    //     objectType: ObjectType, 1
    //     size: Option[Int], 4 - 0 means None
    //     data: StreamLocation, 14
    //     refcount: Refcount, 8 (4-byte serial, 4-byte count)
    //     timestamp: HLCTimestamp, 8
    //     serialized_revision_guard: DataBuffer <== 4 + nbytes
    //
    17 + 16 + 4 + state.storePointer.data.length
      + 16 + 1 + 4 + StreamLocation.StaticSize + 8 + 8 + 4 + state.serializedRevisionGuard.size

  override def writeStaticEntry(bb: ByteBuffer): Unit =
    require(dataLocation.nonEmpty)

    LogContent.putTxId(bb, TxId(state.storeId, state.allocationTransactionId))
    bb.putInt(state.storePointer.data.length)
    bb.put(state.storePointer.data)
    LogContent.putUUID(bb, state.newObjectId.uuid)
    val kind = state.objectType match {
      case ObjectType.Data => 0
      case ObjectType.KeyValue => 1
    }
    bb.put(kind.toByte)
    bb.putInt(state.objectSize.getOrElse(0))
    dataLocation.foreach: loc =>
      LogContent.putStreamLocation(bb, loc)
    bb.putInt(state.initialRefcount.updateSerial)
    bb.putInt(state.initialRefcount.count)
    bb.putLong(state.timestamp.asLong)
    bb.putInt(state.serializedRevisionGuard.size)
    bb.put(state.serializedRevisionGuard.asReadOnlyBuffer())
  
  override def closeStream(id: StreamId): Boolean =
    dataLocation match
      case None => false
      case Some(loc) => 
        if loc.streamId == id then
          dataLocation = None
          true
        else
          false
    
  