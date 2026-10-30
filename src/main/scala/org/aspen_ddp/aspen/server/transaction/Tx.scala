package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.network.{TxAccept, TxAcceptResponse, TxCommitted, TxFinalized, TxHeartbeat, TxPrepare, TxPrepareResponse, TxResolved, TxStatusRequest, TxStatusResponse}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer, ReadError}
import org.aspen_ddp.aspen.common.paxos.{Accept, Acceptor, Prepare}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild, RequirementError, TransactionCollision, TransactionDescription, TransactionDisposition, TransactionId, TransactionObjectRequirement, TransactionRequirement, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{CommitError, CommitState, UpdateForm}
import org.aspen_ddp.aspen.server.store.{Frontend, ObjectState, RequirementsApplyer, RequirementsChecker, RequirementsLocker}
import scribe.Logging

import scala.concurrent.duration._


class Tx( trs: TransactionRecoveryState,
          val txd: TransactionDescription,
          private val frontend: Frontend,
          private val net: Messenger,
          private val crl: CrashRecoveryLog,
          private val statusCache: TransactionStatusCache,
          private val preTxRebuilds: List[PreTransactionOpportunisticRebuild],
          private val objectPointers: List[ObjectPointer]) extends Logging {

  val transactionId: TransactionId = txd.transactionId

  private val storeId = trs.storeId
  private val serializedTxd = trs.serializedTxd
  private val objectUpdates = trs.objectUpdates
  private val pointersById: Map[ObjectId, ObjectPointer] = objectPointers.iterator.map(p => p.id -> p).toMap

  // Only the requirements of objects this store hosts, plus non-object requirements. Other pools' stores vote on
  // their own objects: the driver tallies each object's votes from its own pool's stores
  private val requirements: List[TransactionRequirement] = txd.requirements.filter:
    case tor: TransactionObjectRequirement => pointersById.contains(tor.objectPointer.id)
    case _ => true

  private def pointerOf(objectId: ObjectId): ObjectPointer = pointersById(objectId)
  private var disposition = trs.disposition
  private var collisions: List[TransactionId] = Nil
  private var status = trs.status
  private var oresolution: Option[Boolean] = None
  private var ofinalized: Option[Boolean] = None
  private var objects: Map[ObjectId, ObjectState] = Map()
  private var pendingObjectLoads: Int = objectPointers.size
  private var pendingObjectCommits: Int = 0
  private var delayedPrepare: Option[TxPrepareResponse] = None

  private val acceptor = new Acceptor(storeId.poolIndex, trs.paxosAcceptorState)
  private var lastEvent: Long = System.nanoTime()
  private var saveObjectUpdates: Boolean = true
  private var committed: Boolean = false
  private var committing: Boolean = false
  private var locked: Boolean = false
  private var lastProposer: StoreId = StoreId(txd.primaryObject.poolId, txd.designatedLeaderUID)
  private var objectCommitErrors: List[ObjectId] = Nil

  statusCache.updateStatus(transactionId, status)

  private def allObjectsLoaded: Boolean = pendingObjectLoads == 0

  private def updateLastEvent(): Unit = lastEvent = System.nanoTime()

  def lastEventTime: Long = lastEvent

  def durationSinceLastEvent: Duration = Duration(System.nanoTime() - lastEvent, NANOSECONDS)

  private def unlock(): Unit = if (locked) {
    locked = false

    RequirementsLocker.unlock(transactionId, requirements, objects)
  }


  private def trsObjectUpdates: List[ObjectUpdate] = if (saveObjectUpdates) objectUpdates else Nil

  def objectLoaded(os: ObjectState): Unit = {
    objects += (os.objectId -> os)

    os.transactionReferences += 1 // Ensure this object stays in memory while the transaction is in progress

    pendingObjectLoads -= 1

    logger.trace(s"Object loaded tx: ${txd.transactionId}. Object: ${os.objectId}. Pending: $pendingObjectLoads")

    if (allObjectsLoaded)
      onAllObjectsLoaded()
  }

  def objectLoadFailed(objectId: ObjectId, err: ReadError.Value): Unit = {
    logger.warn(s"Failed to load object $objectId: $err")

    pendingObjectLoads -= 1

    if (allObjectsLoaded)
      onAllObjectsLoaded()
  }

  private def onAllObjectsLoaded(): Unit = {
    logger.trace(s"AllObjectsLoaded for tx ${transactionId}. Have delayedPrepare: ${delayedPrepare.nonEmpty}")

    // Apply pre-transaction rebuilds
    preTxRebuilds.foreach { r =>
      objects.get(r.objectId).foreach { os =>
        if (os.metadata.revision == r.requiredMetadata.revision) {
          os.metadata = r.requiredMetadata
          os.data = r.data
          val cs = CommitState(pointerOf(os.objectId), os.metadata, os.objectType, os.data)
          val txid = TransactionId(r.requiredMetadata.revision.lastUpdateTxUUID)
          // No need to wait for this to complete
          frontend.commit(os, cs, txid)
        }
      }
    }

    // Determine local disposition
    val ou = objectUpdates.iterator.map(ou => ou.objectId -> ou.data).toMap

    val (m, l) = RequirementsChecker.check(transactionId, txd.startTimestamp, requirements, objects, ou,
      frontend.maxSliceSize)

    // Find all objects with transaction collisions for inclusion into the response message
    m.foreach { t => t._2 match {
      case TransactionCollision(txid) =>
        logger.trace(s"Tx ${txd.transactionId}. Collision with Tx $txid")
        collisions = txid :: collisions
      case _ =>
    }}

    disposition = if (m.isEmpty && l.isEmpty) TransactionDisposition.VoteCommit else TransactionDisposition.VoteAbort

    if (disposition == TransactionDisposition.VoteAbort) {
      m.foreach(t => logger.trace(s"Tx ${txd.transactionId}. Object: ${t._1.uuid}. Requirement Error: ${t._2}"))
      l.foreach(e => logger.trace(s"Tx ${txd.transactionId}. Requirement Error: ${e}"))
    }

    // Case Resolution Complete
    oresolution.foreach { resolution =>
      resolvedAndAllObjectsLoaded(resolution)
    }

    // Case received prepare before the objects are loaded. This will always be true for the first prepare
    // message as the receivePrepare() method is called immediately after transaction creation.

    delayedPrepare.foreach { response =>
      logger.trace(s"AllObjectsLoaded. Saving to CRL before sending PrepareResponse for tx ${transactionId}")
      val r = response.copy(disposition=disposition, collisions=collisions)
      delayedPrepare = None
      val state = TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates,
        disposition, status, acceptor.persistentState)
      crl.save(txd.transactionId, state, () =>
        logger.trace(s"AllObjectsLoaded. CRL Save Complete. Sending PrepareResponse for tx ${transactionId}")
        net.sendTransactionMessage(r)
      )
    }
  }

  private def doCommit(): Unit = if (!committed && !committing && allObjectsLoaded) {
    committing = true

    unlock()

    val ou = objectUpdates.iterator.map(ou => ou.objectId -> ou.data).toMap

    val result = RequirementsApplyer.apply(transactionId, txd.startTimestamp, requirements,
      objects, ou, frontend.maxSliceSize)

    // Objects that threw partway through their update hold partially-applied, potentially
    // corrupt state. Drop them from the cache so the next access reloads the last durable
    // version from the backing store rather than serving the corruption.
    result.failed.foreach { objectId =>
      logger.error(s"Tx $transactionId. Discarding cached state of object $objectId after a failed update")
      frontend.objectCache.remove(objectId)
    }

    val uncommittable = result.uncommittable

    // Count what is actually committed. uncommittable may name objects this store never
    // loaded, so objects.size - uncommittable.size can undercount the commits in flight.
    val toCommit = objects.valuesIterator.filter(os => !uncommittable.contains(os.objectId)).toList

    pendingObjectCommits = toCommit.size

    // Assigned before the commit loop: a synchronous backend completes inside it
    objectCommitErrors = uncommittable.toList

    toCommit.foreach { os =>
      val allocating = txd.allocatingObjects.contains(os.objectId)
      val form = UpdateForm.of(os.objectId, requirements, ou, allocating)
      val cs = CommitState(pointerOf(os.objectId), os.metadata, os.objectType, os.data, allocating, form)

      frontend.commit(os, cs, transactionId)
    }

    // Nothing was committed, so no commit completions will arrive to trigger the notification.
    // Without this the proposer never learns this store's outcome.
    if pendingObjectCommits == 0 then
      allObjectCommitsComplete()
  }

  private def allObjectCommitsComplete(): Unit = {
    committed = true
    saveObjectUpdates = false
    crl.dropTransactionObjectData(storeId, transactionId)
    val m = TxCommitted(lastProposer, storeId, transactionId, objectCommitErrors)
    net.sendTransactionMessage(m)
  }

  private def resolvedAndAllObjectsLoaded(committed: Boolean): Unit = {
    if (committed)
      doCommit()
    else
      crl.deleteTransaction(storeId, transactionId)

    unlock() // Ensure we've unlocked the objects now that result of the Tx is known

    // No need to force the objects referenced by this transaction to stay in memory.
    // Release the reference locking them into the cache
    objects.valuesIterator.foreach { os =>
      os.transactionReferences -= 1
    }
  }

  private def resolved(committed: Boolean): Unit = if (oresolution.isEmpty) {
    oresolution = Some(committed)

    status = if (committed)
      TransactionStatus.Committed
    else
      TransactionStatus.Aborted

    statusCache.updateStatus(transactionId, status)

    if (allObjectsLoaded)
      resolvedAndAllObjectsLoaded(committed)
  }

  def receivePrepare(m: TxPrepare): Unit = {
    logger.trace(s"*** Got Prepare: ${m.transactionId}")
    lastProposer = m.from

    // The client re-sends its prepares until it learns the outcome, whether or not any driver is
    // making progress. Counting them as activity would keep a transaction whose designated leader
    // is offline from ever looking idle, so no other store would take it over.
    if !m.fromClient then
      updateLastEvent()

    if (committed) {
      // Ensure the proposer knows we've successfully committed
      val txc = TxCommitted(lastProposer, storeId, transactionId, objectCommitErrors)
      net.sendTransactionMessage(txc)
    }

    val result = acceptor.receivePrepare(Prepare(m.proposalId)) match {
      case Right(p) => Right(TxPrepareResponse.Promise(p.lastAccepted))
      case Left(n) => Left(TxPrepareResponse.Nack(n.promisedProposalId))
    }

    val txr = TxPrepareResponse(m.from, storeId, transactionId, result, m.proposalId, disposition, collisions)
    logger.trace(s"*** PrepareResponse: ${m.transactionId}, Disposition:$disposition. All Objects Loaded: $allObjectsLoaded")

    if allObjectsLoaded then
      val state = TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates,
        disposition, status, acceptor.persistentState)
      crl.save(txd.transactionId, state, () =>
        logger.trace(s"CRL Transaction save complete. Sending PrepareResponse: ${m.transactionId}")
        net.sendTransactionMessage(txr)
      )
    else
      logger.trace(s"Not all objects loaded. Delaying PrepareResponse: ${m.transactionId}")
      delayedPrepare = Some(txr)
  }

  def receiveAccept(m: TxAccept): Unit = {
    updateLastEvent()

    val result = acceptor.receiveAccept(Accept(m.proposalId, m.value)) match {
      case Right(a) => Right(TxAcceptResponse.Accepted(a.proposalValue))
      case Left(n) => Left(TxAcceptResponse.Nack(n.promisedProposalId))
    }
    val tar = TxAcceptResponse(m.from, storeId, transactionId, m.proposalId, result)
    val state = TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates,
      disposition, status, acceptor.persistentState)

    crl.save(txd.transactionId, state, () =>
      logger.trace(s"CRL Accept save completed for tx: ${tar.transactionId}. Sending AcceptResponse")
      net.sendTransactionMessage(tar)
    )
  }

  def receiveResolved(m: TxResolved): Unit = {
    updateLastEvent()
    resolved(m.committed)
  }

  def receiveFinalized(m: TxFinalized): Unit = if (ofinalized.isEmpty) {
    updateLastEvent()
    resolved(m.committed)
    ofinalized = Some(m.committed)
    if (m.committed)
      crl.deleteTransaction(storeId, transactionId)
    statusCache.updateStatus(transactionId, status, finalized = true)
  }

  def receiveHeartbeat(m: TxHeartbeat): Unit = {
    updateLastEvent()
  }

  def receiveStatusRequest(m: TxStatusRequest): Unit = {
    val r = TxStatusResponse(m.from, storeId, transactionId, m.requestUUID,
      Some(TxStatusResponse.TxStatus(status, ofinalized.nonEmpty)))
    net.sendTransactionMessage(r)
  }

  def commitComplete(objectId: ObjectId, result: Either[Unit, CommitError.Value]): Unit = {
    result match
      case Right(err) =>
        logger.error(s"Tx $transactionId. Store $storeId failed to commit object $objectId: $err")
        objectCommitErrors = objectId :: objectCommitErrors
      case Left(_) =>
    pendingObjectCommits -= 1
    if (pendingObjectCommits == 0)
      allObjectCommitsComplete()
  }
}
