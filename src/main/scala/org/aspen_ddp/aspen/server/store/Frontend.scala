package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, OpportunisticRebuild, ReadResponse, TxAccept, TxFinalized, TxHeartbeat, TxMessage, TxPrepare, TxResolved, TxStatusRequest}
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, ReadType}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.{RefcountUpdate, TransactionDescription, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, Commit, CommitState, Completion, Read}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionStatusCache, Tx}
import scribe.Logging
import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState

import scala.concurrent.{Future, Promise}


object Frontend {

  sealed abstract class ReadKind

  case class NetworkRead(clientId: ClientId, requestUUID: UUID, readType: ReadType) extends ReadKind

  case class TransactionRead(transactionId: TransactionId) extends ReadKind

  case class OpportuneRebuild(or: OpportunisticRebuild) extends ReadKind

  case class RepairRead(cos: ClientObjectState, completion: Promise[Unit]) extends ReadKind
}

class Frontend(val storeId: StoreId,
               val backend: Backend,
               val objectCache: ObjectCache,
               val net: Messenger,
               val crl: CrashRecoveryLog,
               val statusCache: TransactionStatusCache,
               /** The store's effective slice size limit, enforced by its transactions. None means unlimited. */
               val maxSliceSize: Option[Long] = None) extends Logging {

  import Frontend._

  var transactions: Map[TransactionId, Tx] = Map()

  private var pendingReads: Map[ObjectId, List[ReadKind]] = Map()

  /** The state of every object a transaction references (see retain), consulted before the cache.
    *
    * Requirement locks live on these states, so every transaction must see the same instance for as long as any
    * transaction references it, whatever the cache does: a state that is evicted, or dropped because it can no longer
    * be served, is never replaced by a fresh load while it is referenced. One that can no longer be served is marked
    * reloadRequired instead, and requirement checks fail on it. It is replaced by a fresh load once no locks remain on
    * it, or when the last reference to it is released.
    */
  private var referenced: Map[ObjectId, ObjectState] = Map()

  /** Transaction messages held while the store's recovery state is still being loaded. See awaitRecovery */
  private var heldMessages: Option[List[TxMessage]] = None

  recoverTransactions()

  /** Recreates the transactions the CRL holds for this store that are not already known */
  private def recoverTransactions(): Unit =
    crl.getFullRecoveryState(storeId).foreach { trs =>
      val txd = TransactionDescription.deserialize(trs.serializedTxd)
      if !transactions.contains(txd.transactionId) then
        val pointers = txd.allHostedObjects(storeId)
        val tx = new Tx(trs, txd, this, net, crl, statusCache, Nil, pointers)
        transactions += (txd.transactionId -> tx)
        pointers.foreach(ptr => readObjectForTransaction(tx, ptr))
    }

  /** The store's CRL state is being loaded after this Frontend was created (from a crl_save.log). Until recover is
    * called, transaction messages are held: started without their recovery state, transactions would hold no locks
    * and a prepare would replace the recovered Paxos acceptor state with an initial one.
    */
  def awaitRecovery(): Unit =
    if heldMessages.isEmpty then
      heldMessages = Some(Nil)

  /** The CRL now holds the store's recovery state: recreates its transactions, then handles the held messages */
  def recover(): Unit =
    val held = heldMessages.getOrElse(Nil).reverse
    heldMessages = None
    recoverTransactions()
    held.foreach(receiveTransactionMessage)


  def close(): Future[Unit] = backend.close()

  def path: os.Path = backend.path

  def commit(os: ObjectState, cs: CommitState, txid: TransactionId): Unit =
    if os.metadata.refcount.count == 0 then
      // Deleted: the state is no longer served, nor checked by transactions that still reference it
      discard(os.objectId)
    else
      objectCache.insert(os)
    backend.commit(cs, txid)

  /** Called when a transaction takes `os` for one of its objects. A state that only this transaction can know, such
    * as that of an object it allocates, is not shared with others (`shared = false`).
    */
  def retain(os: ObjectState, shared: Boolean): Unit =
    os.transactionReferences += 1
    if shared then
      referenced += (os.objectId -> os)

  /** Releases a reference taken by retain */
  def release(os: ObjectState): Unit =
    os.transactionReferences -= 1
    if os.transactionReferences == 0 && referenced.get(os.objectId).exists(_ eq os) then
      referenced -= os.objectId

  /** Stops serving an object's in-memory state: it was deleted, or it differs from what the backend holds. Later
    * reads load it from the backend. A state still referenced by transactions is marked reloadRequired rather than
    * forgotten, so that the locks on it keep being honoured. See referenced.
    */
  def discard(objectId: ObjectId): Unit =
    referenced.get(objectId).foreach(_.reloadRequired = true)
    objectCache.get(objectId).foreach(_.reloadRequired = true)
    objectCache.remove(objectId)

  /** The state transactions must use for an object, if one is in memory */
  private def stateForTransaction(objectId: ObjectId): Option[ObjectState] =
    referenced.get(objectId) match
      // Transactions holding locks on a discarded state still reference it. Checks against it fail until they resolve
      case Some(os) if !os.reloadRequired || os.lockedWriteTransactions.nonEmpty => Some(os)
      case Some(_) =>
        referenced -= objectId
        cachedState(objectId)
      case None => cachedState(objectId)

  /** An in-memory state that may be served */
  private def servableState(objectId: ObjectId): Option[ObjectState] =
    referenced.get(objectId).filter(!_.reloadRequired).orElse(cachedState(objectId))

  private def cachedState(objectId: ObjectId): Option[ObjectState] =
    objectCache.get(objectId).filter(!_.reloadRequired)

  def receiveTransactionMessage(msg: TxMessage): Unit = heldMessages match
    case Some(held) => heldMessages = Some(msg :: held)
    case None => handleTransactionMessage(msg)

  private def handleTransactionMessage(msg: TxMessage): Unit = msg match {
    case m: TxPrepare => receivePrepare(m)
    case m: TxAccept => transactions.get(m.transactionId).foreach(tx => tx.receiveAccept(m))
    case m: TxResolved =>
      transactions.get(m.transactionId) match
        case Some(tx) => tx.receiveResolved(m)
        // Remembered for a prepare that arrives after this. See receivePrepare
        case None =>
          if !statusCache.getStatus(m.transactionId).exists(_.finalized) then
            statusCache.updateStatus(m.transactionId, statusOf(m.committed))
    case m: TxFinalized =>
      transactions.get(m.transactionId) match
        case Some(tx) =>
          tx.receiveFinalized(m)
          tx.releaseObjects() // Already done unless some of its objects had not loaded
        // Remembered for a prepare that arrives after this. See receivePrepare
        case None => statusCache.updateStatus(m.transactionId, statusOf(m.committed), finalized = true)
      transactions -= m.transactionId
    case m: TxHeartbeat => transactions.get(m.transactionId).foreach(tx => tx.receiveHeartbeat(m))
    case m: TxStatusRequest => transactions.get(m.transactionId).foreach(tx => tx.receiveStatusRequest(m))
    case _ => // Other TxMessages are not relevant
  }

  private def receivePrepare(m: TxPrepare): Unit = transactions.get(m.txd.transactionId) match {
    case Some(tx) =>
      logger.trace(s"**** EXISTING TX: ${m.txd.transactionId}. $tx")
      tx.receivePrepare(m)
    // A prepare can arrive after the transaction's resolution (or finalization) did, as when this store was slow or the
    // client's prepare is a retransmission. Started afresh, the Tx would never learn the outcome it missed: a commit vote
    // would hold its requirement locks until finalization, which may itself wait on transactions those locks block, or
    // indefinitely once nothing drives the finalized transaction. Store has already answered a finalized one from the
    // status cache, so it is ignored. A resolved one takes no locks; it still learns its outcome from TxFinalized.
    case None if statusCache.getStatus(m.txd.transactionId).exists(_.finalized) =>
      logger.trace(s"**** IGNORING PREPARE FOR FINALIZED TX: ${m.txd.transactionId}")

    case None =>
      logger.trace(s"**** CREATING NEW TX: ${m.txd.transactionId}")
      val known = statusCache.getStatus(m.txd.transactionId).filter(_.status != TransactionStatus.Unresolved)
      val trs = TransactionRecoveryState.initial(m.to, m.txd, m.objectUpdates)
      val pointers = m.txd.allHostedObjects(m.to)
      val tx = new Tx(trs, m.txd, this, net, crl, statusCache, m.preTxRebuilds, pointers)
      transactions += (m.txd.transactionId -> tx)
      tx.receivePrepare(m)
      known.foreach { entry =>
        tx.outcomeDecided()
        statusCache.updateStatus(m.txd.transactionId, entry.status) // The new Tx recorded itself as Unresolved
      }
      pointers.foreach(ptr => readObjectForTransaction(tx, ptr))
  }

  private def statusOf(committed: Boolean): TransactionStatus.Value =
    if committed then TransactionStatus.Committed else TransactionStatus.Aborted

  def backendOperationComplete(completion: Completion): Unit = completion match {
    case r: Read => backendReadComplete(r.objectId, r.result)
    case c: Commit =>
      // commit() cached the new state before the backend stored it. The backend refused it, so the cache must not
      // serve it: reads would return state the store does not hold, and readObjectForRepair would see a repair as
      // already applied. Evicting a newer cached state too only costs a backend read.
      if c.result.isRight then
        discard(c.objectId)
      transactions.get(c.transactionId).foreach { tx =>
        tx.commitComplete(c.objectId, c.result)
      }
  }

  def readObjectForNetwork(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer, readType: ReadType): Unit = {
    val objectId = pointer.id

    servableState(objectId) match
      case Some(os) =>
        logger.trace(s"Reading Cached object for read $readUUID. Revision ${os.metadata.revision}")
        val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
          os.data.size, PartialRead.filter(os.objectType, os.data, readType), os.lockedWriteTransactions )

        val rr = ReadResponse(clientId, storeId, readUUID, HLCTimestamp.now, Right(cs) )
        net.sendClientResponse(rr)

      case None =>
        logger.trace(s"Reading uncached object from backing store for read $readUUID")

        val nr = NetworkRead(clientId, readUUID, readType)

        pendingReads.get(objectId) match
          case Some(lst) =>
            pendingReads += (objectId -> (nr :: lst))

          case None =>
            pendingReads += (objectId -> (nr :: Nil))
            backend.read(pointer)
  }

  def readObjectForOpportunisticRebuild(op: OpportunisticRebuild): Unit = {
    val objectId = op.pointer.id
    servableState(objectId) match {
      case Some(os) => opportunisticRebuild(op, os)
      case None =>
        if op.pointer.poolId == storeId.poolId then
          pendingReads.get(objectId) match {
            case Some(lst) =>
              pendingReads += (objectId -> (OpportuneRebuild(op) :: lst))

            case None =>
              pendingReads += (objectId -> (OpportuneRebuild(op) :: Nil))
              backend.read(op.pointer)
          }
    }
  }

  def readObjectForRepair(current: ClientObjectState, completion: Promise[Unit]): Unit = {
    val objectId = current.pointer.id
    servableState(objectId) match {
      case Some(os) => repair(current, completion, os)
      case None =>
        if current.pointer.poolId == storeId.poolId then
          pendingReads.get(objectId) match {
            case Some(lst) =>
              pendingReads += (objectId -> (RepairRead(current, completion) :: lst))

            case None =>
              pendingReads += (objectId -> (RepairRead(current, completion) :: Nil))
              backend.read(current.pointer)
          }
    }
  }

  /** Repairs a missed deletion by dropping the object from this store.
    *
    * There is nothing to read first: the object is gone everywhere else, so there is no state
    * to compare against. We evict the cache for the same reason `commit` does when a refcount
    * reaches zero, then hand the removal to the backend.
    */
  def deleteObjectForRepair(objectId: ObjectId,
                            storePointer: Array[Byte],
                            completion: Promise[Unit]): Unit =
    logger.trace(s"Repairing missed deletion of object $objectId")
    discard(objectId)
    backend.repairDelete(objectId, storePointer, completion)

  private def readObjectForTransaction(transaction: Tx, pointer: ObjectPointer): Unit =
    val objectId = pointer.id

    logger.trace(s"Loading object for Tx: ${transaction.transactionId}. Object: $objectId")

    if transaction.txd.allocatingObjects.contains(pointer.id) then
      logger.trace(s"Allocating new object: $objectId in Tx: ${transaction.transactionId}.")

      val metadata = Metadata(
        ObjectRevision.Allocating,
        ObjectRefcount.Allocating,
        transaction.txd.startTimestamp
      )

      val os = new ObjectState(objectId, metadata, pointer.objectType, DataBuffer.Empty)

      transaction.objectLoaded(os)
    else
      stateForTransaction(objectId) match
        case Some(os) =>
          logger.trace(s"Loading object from CACHE for Tx: ${transaction.transactionId}. Object: $objectId")
          transaction.objectLoaded(os)

        case None =>
          val tr = TransactionRead(transaction.transactionId)

          pendingReads.get(objectId) match
            case Some(lst) =>
              pendingReads += (objectId -> (tr :: lst))

            case None =>
              pendingReads += (objectId -> (tr :: Nil))
              logger.trace(s"Loading object from Backend for Tx: ${transaction.transactionId}. Object: $objectId")
              backend.read(pointer)

  private def opportunisticRebuild(op: OpportunisticRebuild, os: ObjectState): Unit = {
    if (os.metadata.revision == op.revision) {
      val rc = if (op.refcount.updateSerial > os.metadata.refcount.updateSerial)
        op.refcount
      else
        os.metadata.refcount

      os.metadata = Metadata(op.revision, rc, op.timestamp)
      os.data = op.data
      val cs = CommitState(op.pointer, os.metadata, os.objectType, os.data)
      val txid = TransactionId(op.revision.lastUpdateTxUUID)
      // No need to wait for this to complete
      commit(os, cs, txid)
    }
  }

  private def repair(current: ClientObjectState, completion: Promise[Unit], os: ObjectState): Unit =
    if current.timestamp <= os.metadata.timestamp then
      completion.success(())
    else
      current.getRebuildDataForStore(storeId) match
        case None => completion.success(())
        case Some(storeData) =>
          os.metadata = Metadata(current.revision, current.refcount, current.timestamp)
          os.data = storeData
          val cs = CommitState(current.pointer, os.metadata,
            current.pointer.objectType, os.data)
          val txid = TransactionId(current.revision.lastUpdateTxUUID)
          backend.repair(cs, completion)


  def backendReadComplete(objectId: ObjectId,
                          result: Either[ReadState, ReadError.Value]): Unit = {
    result match {
      case Left(rs) =>
        // A state still referenced by transactions is never replaced. See referenced
        val loaded = new ObjectState(objectId, rs.metadata, rs.objectType, rs.data)
        val inUse = referenced.get(objectId).filter(!_.reloadRequired)
        if inUse.isEmpty then
          objectCache.insert(loaded)
        val os = inUse.getOrElse(loaded)

        pendingReads.get(objectId).foreach { lpr =>
          lpr.foreach {
            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. SUCCESS. Sending read response")
              val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
                os.data.size, PartialRead.filter(os.objectType, os.data, netRead.readType), os.lockedWriteTransactions )

              val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Right(cs) )
              net.sendClientResponse(rr)

            case tr: TransactionRead =>
              logger.trace(s"Completed read for transaction ${tr.transactionId}. Object: ${os.objectId}")
              transactions.get(tr.transactionId).foreach { tx => tx.objectLoaded(stateForTransaction(objectId).getOrElse(os)) }

            case OpportuneRebuild(op) =>
              logger.trace(s"Completed read for OpportuneRebuild. Object: ${os.objectId}")
              opportunisticRebuild(op, os)

            case RepairRead(cos, completion) =>
              logger.trace(s"Completed read for repair. Object: ${os.objectId}")
              repair(cos, completion, os)
          }
        }

      case Right(err) =>
        pendingReads.get(objectId).foreach { lpr =>
          lpr.foreach {
            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. ERROR $err. Sending read response")
              val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Left(err) )
              net.sendClientResponse(rr)

            case tr: TransactionRead => transactions.get(tr.transactionId).foreach { tx => tx.objectLoadFailed(objectId, err) }

            case _: OpportuneRebuild => // Can't guarantee correctness so we need to ignore this

            case RepairRead(cos, completion) =>
              // Failure to read probably means a repair is required
              val os = new ObjectState(objectId, Metadata.Zeroed,
                ObjectType.Data, DataBuffer.Empty)
              repair(cos, completion, os)
          }
        }
    }

    pendingReads -= objectId
  }
}
