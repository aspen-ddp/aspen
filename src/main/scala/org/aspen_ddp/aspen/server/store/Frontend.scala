package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, OpportunisticRebuild, ReadResponse, TxAccept, TxFinalized, TxHeartbeat, TxMessage, TxPrepare, TxResolved, TxStatusRequest}
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, ReadType}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.{RefcountUpdate, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendTiming, Commit, CommitState, Completion, Read}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionStatusCache, Tx}
import scribe.Logging
import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Future, Promise}


object Frontend {

  /** How long a store withholds its vote from drivers' prepares while waiting for a client's object data.
    * Several client retransmissions long, since the only cost of a late abort is a client retry.
    */
  val MissingDataWait: Duration = Duration(5, SECONDS)

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
               val maxSliceSize: Option[Long] = None,
               val missingDataWait: Duration = Frontend.MissingDataWait) extends Logging {

  import Frontend._

  var transactions: Map[TransactionId, Tx] = Map()

  /** Transactions waiting for a lock holder, by holder. See Tx.awaitableHolders */
  private val collisionWaiters = scala.collection.mutable.HashMap[TransactionId, List[Tx]]()

  def holdsLocks(txid: TransactionId): Boolean = transactions.get(txid).exists(_.holdsLocks)

  def awaitLocksReleased(holder: TransactionId, waiter: Tx): Unit =
    val others = collisionWaiters.getOrElse(holder, Nil).filterNot(_ eq waiter)
    collisionWaiters(holder) = waiter :: others

  /** `holder` released its locks, so the transactions waiting for it decide their votes */
  def locksReleased(holder: TransactionId): Unit =
    collisionWaiters.remove(holder).foreach(_.foreach(_.collisionHolderReleased(holder)))

  /** Outstanding backend commits, oldest first. Each holds a reference to its state (see retain) from commit until
    * the backend reports it complete, so the state is served and checked from memory rather than read from a backend
    * that may not have it yet. `start` times the commit as the store sees it: BackendTiming
    */
  private case class InFlight(os: ObjectState, start: Long, deletes: Boolean)
  private val inFlight = scala.collection.mutable.HashMap[(ObjectId, TransactionId), List[InFlight]]()

  /** Outstanding backend commits deleting each object. Reads that would reach the backend get the answer it will give
    * once the deletion is applied
    */
  private val deleting = scala.collection.mutable.HashMap[ObjectId, Int]()
  private val commitStage = s"store commit (${backend.getClass.getSimpleName})"

  private var pendingReads: Map[ObjectId, List[ReadKind]] = Map()

  /** The state of every object a transaction or an in-flight backend commit references (see retain), consulted before
    * the cache.
    *
    * Requirement locks live on these states, so every transaction must see the same instance for as long as any
    * transaction references it, whatever the cache does: a state that is evicted, or dropped because it can no longer
    * be served, is never replaced by a fresh load while it is referenced. One that can no longer be served is marked
    * reloadRequired instead, and requirement checks fail on it. It is replaced by a fresh load once no locks remain on
    * it, or when the last reference to it is released.
    */
  private var referenced: Map[ObjectId, ObjectState] = Map()

  /** Transaction messages held, newest first, until recovery completes. See awaitRecovery and recoverTransactions */
  private var heldMessages: List[TxMessage] = Nil

  /** The store's CRL state is still being loaded (from a crl_save.log). See awaitRecovery */
  private var awaitingCrl: Boolean = false

  /** Recovered transactions whose objects have not all loaded yet, and whether each has a persisted vote */
  private var recoveringTransactions: Map[TransactionId, Boolean] = Map()

  /** Recovered transactions without a persisted vote that have loaded their objects, waiting to decide it. See
    * recoveredTransactionLoaded
    */
  private var undecidedRecovered: List[Tx] = Nil

  private def holding: Boolean = awaitingCrl || recoveringTransactions.nonEmpty || undecidedRecovered.nonEmpty

  recoverTransactions()

  /** Recreates the transactions the CRL holds for this store that are not already known.
    *
    * Transaction messages are held until every recovered transaction has loaded its objects, and with them taken the
    * locks of its recovered commit vote. A transaction started meanwhile could otherwise load a shared object first and
    * take conflicting locks.
    */
  private def recoverTransactions(): Unit =
    // Every recovered transaction is registered before any of their reads starts: one that loads synchronously (its
    // objects cached, or all allocated) must not find the others missing and release the held messages early
    val recovered = crl.getFullRecoveryState(storeId).flatMap { trs =>
      val txd = TransactionDescription.deserialize(trs.serializedTxd)
      if transactions.contains(txd.transactionId) then
        None
      else
        val pointers = txd.allHostedObjects(storeId)
        val tx = new Tx(trs, txd, this, net, crl, statusCache, Nil, pointers, recovered = true)
        transactions += (txd.transactionId -> tx)
        // A transaction with no objects here never loads any
        if pointers.nonEmpty then
          recoveringTransactions += (txd.transactionId -> (trs.disposition != TransactionDisposition.Undetermined))
        Some((tx, pointers))
    }
    recovered.foreach((tx, pointers) => pointers.foreach(ptr => readObjectForTransaction(tx, ptr)))

  /** A recovered transaction has loaded (or failed to load) all its objects. One with a persisted vote completes its
    * load at once, taking the locks of a commit vote. One without decides its vote only once every recovered
    * transaction with a persisted vote has done so: deciding earlier, it could take locks that conflict with theirs.
    */
  def recoveredTransactionLoaded(tx: Tx, hasVote: Boolean): Unit =
    recoveringTransactions -= tx.transactionId
    if hasVote then
      tx.completeLoad()
    else
      undecidedRecovered = undecidedRecovered :+ tx
    if !recoveringTransactions.valuesIterator.contains(true) then
      val undecided = undecidedRecovered
      undecidedRecovered = Nil
      undecided.foreach(_.completeLoad())
    releaseHeldMessages()

  /** The store's CRL state is being loaded after this Frontend was created (from a crl_save.log). Until recover is
    * called, transaction messages are held: started without their recovery state, transactions would hold no locks
    * and a prepare would replace the recovered Paxos acceptor state with an initial one.
    */
  def awaitRecovery(): Unit = awaitingCrl = true

  /** The CRL now holds the store's recovery state: recreates its transactions. Held messages are handled, in arrival
    * order, once those transactions have loaded their objects
    */
  def recover(): Unit =
    awaitingCrl = false
    recoverTransactions()
    releaseHeldMessages()

  private def releaseHeldMessages(): Unit =
    if !holding then
      val held = heldMessages.reverse
      heldMessages = Nil
      held.foreach(handleTransactionMessage)


  def close(): Future[Unit] = backend.close()

  def path: os.Path = backend.path

  /** Hands `os` to the backend and keeps it in memory until the backend reports the commit complete.
    *
    * @param forTransaction the commit applies a transaction's committed update, rather than a rebuild
    */
  def commit(os: ObjectState, cs: CommitState, txid: TransactionId, forTransaction: Boolean = false): Unit =
    val deletes = os.metadata.refcount.count == 0
    if deletes then
      // Deleted: the state is no longer served, nor checked by transactions that still reference it
      discard(os.objectId)
    else
      objectCache.insert(os)
    retain(os, shared = true)
    val key = (cs.objectId, txid)
    inFlight(key) = inFlight.getOrElse(key, Nil) :+ InFlight(os, System.nanoTime(), deletes)
    if deletes then
      deleting(os.objectId) = deleting.getOrElse(os.objectId, 0) + 1
    backend.commit(cs, txid)

  /** Called when a transaction takes `os` for one of its objects, and by commit for the commit's duration. A state that
    * only this transaction can know, such as that of an object it allocates, is not shared with others
    * (`shared = false`). The first reference pins the state in the cache: counted, and never evicted.
    */
  def retain(os: ObjectState, shared: Boolean): Unit =
    os.references += 1
    if os.references == 1 then
      objectCache.pin(os)
    if shared then
      referenced += (os.objectId -> os)

  /** Releases a reference taken by retain. The last one unpins the state: still valid and cached, it becomes evictable
    * again as the most recently used entry
    */
  def release(os: ObjectState): Unit =
    os.references -= 1
    if os.references == 0 then
      if referenced.get(os.objectId).exists(_ eq os) then
        referenced -= os.objectId
      objectCache.unpin(os)

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
      case Some(os) if !os.reloadRequired || os.lockHolders.nonEmpty => Some(os)
      case Some(_) =>
        referenced -= objectId
        cachedState(objectId)
      case None => cachedState(objectId)

  /** An in-memory state that may be served */
  private def servableState(objectId: ObjectId): Option[ObjectState] =
    referenced.get(objectId).filter(!_.reloadRequired).orElse(cachedState(objectId))

  private def cachedState(objectId: ObjectId): Option[ObjectState] =
    objectCache.get(objectId).filter(!_.reloadRequired)

  def receiveTransactionMessage(msg: TxMessage): Unit =
    if holding || heldMessages.nonEmpty then
      heldMessages = msg :: heldMessages
    else
      handleTransactionMessage(msg)

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
      // Either a retransmission or this store's data arriving too late to be used
      if m.fromClient && m.objectUpdates.nonEmpty then
        val age = (HLCTimestamp.now - m.txd.startTimestamp).toMillis
        logger.warn(s"Client prepare with data for Tx ${m.txd.transactionId} on store ${m.to} arrived after finalization, " +
          s"${age}ms after the transaction started")

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
      val key = (c.objectId, c.transactionId)
      inFlight.get(key) match
        case Some(f :: rest) =>
          if rest.isEmpty then inFlight.remove(key) else inFlight(key) = rest
          BackendTiming.report.record(commitStage, System.nanoTime() - f.start)
          c.result match
            case Left(_) =>
              release(f.os)
              if f.deletes then
                val n = deleting.getOrElse(c.objectId, 1) - 1
                if n == 0 then deleting.remove(c.objectId) else deleting(c.objectId) = n
            case Right(err) =>
              // The state stays pinned and served, and a deletion keeps answering ObjectNotFound, while the store is
              // loaded: the store may have voted to commit at the write threshold, so it must neither lose the
              // committed state nor return the pre-commit one. Backlog.txt covers handling this properly
              logger.error(s"Store $storeId: backend commit of object ${c.objectId} for transaction " +
                s"${c.transactionId} failed: $err. Its committed state stays in memory while the store is loaded")
        case _ =>
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

      case None if deleting.contains(objectId) =>
        net.sendClientResponse(ReadResponse(clientId, storeId, readUUID, HLCTimestamp.now,
          Left(ReadError.ObjectNotFound)))

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
      case None if deleting.contains(objectId) => // Being deleted: nothing to rebuild
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
      case None if deleting.contains(objectId) => repairMissing(current, completion)
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

        case None if deleting.contains(objectId) =>
          transaction.objectLoadFailed(objectId, ReadError.ObjectNotFound)

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


  /** Repairs an object this store cannot read: failure to read probably means a repair is required */
  private def repairMissing(current: ClientObjectState, completion: Promise[Unit]): Unit =
    repair(current, completion, new ObjectState(current.pointer.id, Metadata.Zeroed, ObjectType.Data, DataBuffer.Empty))

  def backendReadComplete(objectId: ObjectId,
                          result: Either[ReadState, ReadError.Value]): Unit = {
    // Taken before dispatch: handling a read can start another read of the same object (a transaction message held
    // for recovery is handled once a recovered transaction loads), which must not join this completed one
    val waiting = pendingReads.get(objectId)
    pendingReads -= objectId

    // A read issued before a deletion's commit may return the state the deletion is replacing
    val effective = if deleting.contains(objectId) then Right(ReadError.ObjectNotFound) else result

    effective match {
      case Left(rs) =>
        // A state still referenced by transactions is never replaced. See referenced
        val loaded = new ObjectState(objectId, rs.metadata, rs.objectType, rs.data)
        val inUse = referenced.get(objectId).filter(!_.reloadRequired)
        if inUse.isEmpty then
          objectCache.insert(loaded)
        val os = inUse.getOrElse(loaded)

        waiting.foreach { lpr =>
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
        waiting.foreach { lpr =>
          lpr.foreach {
            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. ERROR $err. Sending read response")
              val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Left(err) )
              net.sendClientResponse(rr)

            case tr: TransactionRead => transactions.get(tr.transactionId).foreach { tx => tx.objectLoadFailed(objectId, err) }

            case _: OpportuneRebuild => // Can't guarantee correctness so we need to ignore this

            case RepairRead(cos, completion) => repairMissing(cos, completion)
          }
        }
    }
  }
}
