package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.{TxAccept, TxAcceptResponse, TxCommitted, TxFinalized, TxHeartbeat, TxPrepare, TxPrepareResponse, TxResolved, TxStatusRequest, TxStatusResponse}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer, ReadError}
import org.aspen_ddp.aspen.common.paxos.{Accept, Acceptor, Prepare}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, KeyValueUpdate, ObjectUpdate, PreTransactionOpportunisticRebuild, RequirementError, TransactionCollision, TransactionDescription, TransactionDisposition, TransactionId, TransactionObjectRequirement, TransactionRequirement, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{CommitError, CommitState, UpdateForm}
import org.aspen_ddp.aspen.server.store.{Frontend, ObjectState, RequirementsApplyer, RequirementsChecker, RequirementsLocker}
import scribe.Logging

import scala.concurrent.duration._


object Tx {
  /** How long a transaction may go unapplied before its store logs it */
  val StuckReportAfter: FiniteDuration = FiniteDuration(2, SECONDS)
  val StuckReportEvery: FiniteDuration = FiniteDuration(5, SECONDS)

  /** How long after a transaction started its client's prepare may first reach a store before the store logs it */
  val LateClientPrepareAfter: FiniteDuration = FiniteDuration(500, MILLISECONDS)
}

class Tx( trs: TransactionRecoveryState,
          val txd: TransactionDescription,
          private val frontend: Frontend,
          private val net: Messenger,
          private val crl: CrashRecoveryLog,
          private val statusCache: TransactionStatusCache,
          private val preTxRebuilds: List[PreTransactionOpportunisticRebuild],
          private val objectPointers: List[ObjectPointer],
          /** Recreated from persisted recovery state. See onAllObjectsLoaded */
          val recovered: Boolean = false) extends Logging {

  val transactionId: TransactionId = txd.transactionId

  private val storeId = trs.storeId
  private val serializedTxd = trs.serializedTxd
  // A driver's prepare carries no object data, so a Tx it started gets the data from a later client prepare
  private var objectUpdates = trs.objectUpdates
  private val startNanos = System.nanoTime()
  private var clientPrepared = false
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
  private var commitHeld: Boolean = false
  private var locked: Boolean = false
  private var outcomeKnown: Boolean = false
  private var lastProposer: StoreId = StoreId(txd.primaryObject.poolId, txd.designatedLeaderUID)
  private var objectCommitErrors: List[ObjectId] = Nil

  statusCache.updateStatus(transactionId, status)

  private def allObjectsLoaded: Boolean = pendingObjectLoads == 0

  private def missingObjectData: Boolean =
    val have = objectUpdates.iterator.map(_.objectId).toSet
    requirements.exists:
      case r: DataUpdate => !have.contains(r.objectPointer.id)
      case r: KeyValueUpdate => !have.contains(r.objectPointer.id)
      case _ => false

  /** A driver's small prepare can overtake the client's prepare carrying this store's data, and the missing data would
    * force an abort vote on a healthy transaction. So the vote is withheld: no reply is always allowed by Paxos, and
    * the drivers retransmit. The wait ends with the data, with a client's prepare (it is all the data the client will
    * send), with the outcome (the vote no longer matters), or after `missingDataWait`, which covers a dead client.
    * Checked only as prepares arrive, since a vote is of use only to a driver that is still asking for it.
    */
  private def awaitingData: Boolean =
    disposition == TransactionDisposition.Undetermined && !recovered && !clientPrepared && !outcomeKnown &&
      oresolution.isEmpty && missingObjectData && System.nanoTime() - startNanos < frontend.missingDataWait.toNanos

  /** The commit decision can overtake the client's prepare too. Applying now would lose the object update, so the
    * commit waits, without a TxCommitted, and the driver sees the store's TxCommitted when the data arrives instead of
    * marking a missed update. The wait ends with the data, with a client's prepare, or with TxFinalized, after which
    * nothing is waiting on this store and repair restores it. No locks are held: a commit vote requires the data.
    */
  private def commitAwaitsData: Boolean = !recovered && !clientPrepared && ofinalized.isEmpty && missingObjectData

  private def releaseHeldCommit(): Unit =
    if commitHeld && !commitAwaitsData then
      commitHeld = false
      resolvedAndAllObjectsLoaded(true)

  private def updateLastEvent(): Unit = lastEvent = System.nanoTime()

  def lastEventTime: Long = lastEvent

  def durationSinceLastEvent: Duration = Duration(System.nanoTime() - lastEvent, NANOSECONDS)

  private def stateSummary(now: Long): String =
    val age = Duration(now - startNanos, NANOSECONDS).toMillis
    val idle = Duration(now - lastEvent, NANOSECONDS).toMillis
    val revisions = objects.valuesIterator.map(os => s"${os.objectId.uuid}=${os.metadata.revision.lastUpdateTxUUID}")
      .mkString(",")
    s"Age: ${age}ms. Idle: ${idle}ms. Vote: $disposition. Resolution: $oresolution. Finalized: $ofinalized. " +
      s"Locked: $locked. CommitHeld: $commitHeld. Committing: $committing. Committed: $committed. " +
      s"PendingLoads: $pendingObjectLoads. PendingCommits: $pendingObjectCommits. ClientPrepared: $clientPrepared. " +
      s"Recovered: $recovered. Last proposer: $lastProposer. Object revisions: $revisions"

  private var lastStuckReport: Option[Long] = None

  /** Logs a transaction whose store has not applied it long after it started: it holds its locks, has a held commit,
    * or is waiting on the backend. Repeats every `Tx.StuckReportEvery`. Returns whether it logged.
    */
  def reportIfStuck(now: Long = System.nanoTime()): Boolean =
    val stuck = locked || commitHeld || (committing && !committed)
    val due = stuck && now - startNanos >= Tx.StuckReportAfter.toNanos &&
      lastStuckReport.forall(last => now - last >= Tx.StuckReportEvery.toNanos)
    if due then
      lastStuckReport = Some(now)
      logger.warn(s"Stuck Tx $transactionId on store $storeId. ${stateSummary(now)}")
    due

  /** Logs the outcome reaching a store that held the transaction's locks for a long time, naming who told it */
  private def reportLateOutcome(message: String, from: StoreId): Unit =
    val now = System.nanoTime()
    if locked && oresolution.isEmpty && now - startNanos >= Tx.StuckReportAfter.toNanos then
      logger.warn(s"Late $message from $from for Tx $transactionId on store $storeId. ${stateSummary(now)}")

  /** Logs a client's prepare reaching this store long after the transaction started. That prepare carries the store's
    * object data, so a late one can leave the store unable to vote or apply, behind on the object until it is repaired.
    * Returns whether it logged.
    */
  private[aspen] def reportLateClientPrepare(ageMillis: Long): Boolean =
    val late = ageMillis >= Tx.LateClientPrepareAfter.toMillis
    if late then
      logger.warn(s"Late client prepare for Tx $transactionId on store $storeId, ${ageMillis}ms after the transaction " +
        s"started. ${stateSummary(System.nanoTime())}")
    late

  /** Holds the requirement locks from a commit vote until the transaction resolves, so that this store votes abort
    * on conflicting transactions in the meantime. Locks have no purpose once the outcome is known, including the
    * resolved status of a Tx recovered from the CRL.
    */
  private def lock(): Unit =
    if !locked && !outcomeKnown && status == TransactionStatus.Unresolved &&
      disposition == TransactionDisposition.VoteCommit then
      locked = true
      RequirementsLocker.lock(transactionId, requirements, objects)

  /** The transaction is known to be resolved, though this Tx has not been told the outcome: its prepare arrived
    * after the resolution did. Its vote cannot affect the outcome, so it takes no locks.
    */
  def outcomeDecided(): Unit = outcomeKnown = true

  private def unlock(): Unit = if (locked) {
    locked = false

    RequirementsLocker.unlock(transactionId, requirements, objects)
  }


  private def trsObjectUpdates: List[ObjectUpdate] = if (saveObjectUpdates) objectUpdates else Nil

  def objectLoaded(os: ObjectState): Unit = {
    objects += (os.objectId -> os)

    // Ensures every transaction uses this state, and keeps it in memory, while this transaction is in progress
    frontend.retain(os, shared = !txd.allocatingObjects.contains(os.objectId))

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

  private def onAllObjectsLoaded(): Unit =
    if recovered then
      // Frontend orders the recovered transactions' completions. See Frontend.recoveredTransactionLoaded
      frontend.recoveredTransactionLoaded(this, hasVote = disposition != TransactionDisposition.Undetermined)
    else
      completeLoad()

  /** Runs once all objects have loaded (or failed to). Called by Frontend for a recovered transaction */
  def completeLoad(): Unit = {
    logger.trace(s"AllObjectsLoaded for tx ${transactionId}. Have delayedPrepare: ${delayedPrepare.nonEmpty}")

    // Apply pre-transaction rebuilds
    preTxRebuilds.foreach { r =>
      objects.get(r.objectId).foreach { os =>
        if (os.metadata.revision == r.requiredMetadata.revision && !os.reloadRequired) {
          os.metadata = r.requiredMetadata
          os.data = r.data
          val cs = CommitState(pointerOf(os.objectId), os.metadata, os.objectType, os.data)
          val txid = TransactionId(r.requiredMetadata.revision.lastUpdateTxUUID)
          // No need to wait for this to complete
          frontend.commit(os, cs, txid)
        }
      }
    }

    // A recovered transaction keeps the vote it persisted: the vote may already have been counted, and recomputing it
    // against state that changed since could contradict it. A recovered commit vote takes its locks again; a recovered
    // abort vote must not become a commit vote that conflicts with a recovered commit vote it collided with.
    if !(recovered && disposition != TransactionDisposition.Undetermined) && !awaitingData then
      determineDisposition()

    lock()

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

  private def determineDisposition(): Unit = {
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
      AbortVoteReport.report.record(transactionId, m.toList.map((o, e) => Some(o) -> e) ++ l.map(None -> _))
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
      frontend.discard(objectId)
    }

    result.skippedReasons.foreach { (objectId, reason) =>
      val current = objects.get(objectId).map(_.metadata.revision.lastUpdateTxUUID)
      logger.warn(s"Tx $transactionId. Store $storeId cannot apply the committed update of object $objectId: $reason. " +
        s"Vote: $disposition. Client prepare seen: $clientPrepared. Revision on store: $current")
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
    if committed && commitAwaitsData then
      logger.trace(s"Holding the commit of $transactionId until its object data arrives")
      commitHeld = true
    else
      if (committed)
        doCommit()
      else
        crl.deleteTransaction(storeId, transactionId)

      unlock() // Ensure we've unlocked the objects now that result of the Tx is known

      releaseObjects()
  }

  private var objectsReleased = false

  /** Releases this transaction's references to its objects. Also called by Frontend when it forgets a transaction
    * whose objects have not all loaded, which therefore never reached resolvedAndAllObjectsLoaded.
    */
  def releaseObjects(): Unit = if !objectsReleased then
    objectsReleased = true
    unlock()
    objects.valuesIterator.foreach(frontend.release)

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

  /** Adopts the object data of a prepare that has some this Tx lacks: the first prepare to arrive may have been a
    * driver's, which carries none, while the client's was lost or delayed. A vote already given stands, but the data
    * is still needed to apply a commit decided by the other stores' votes. See commitAwaitsData
    */
  private def adoptObjectUpdates(m: TxPrepare): Unit =
    val have = objectUpdates.iterator.map(_.objectId).toSet
    val added = m.objectUpdates.filterNot(ou => have.contains(ou.objectId))
    if added.nonEmpty && !committing && !committed then
      objectUpdates = objectUpdates ++ added

  def receivePrepare(m: TxPrepare): Unit = {
    logger.trace(s"*** Got Prepare: ${m.transactionId}")
    lastProposer = m.from

    if m.fromClient then
      if !clientPrepared then
        reportLateClientPrepare((HLCTimestamp.now - txd.startTimestamp).toMillis)
      clientPrepared = true

    adoptObjectUpdates(m)

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

    releaseHeldCommit()

    if awaitingData then
      logger.trace(s"Withholding the vote on ${m.transactionId} until its object data arrives")
    else
      // completeLoad left the vote undecided if the Tx was waiting for data then
      if allObjectsLoaded && !recovered && oresolution.isEmpty && disposition == TransactionDisposition.Undetermined then
        determineDisposition()
        lock()

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
    reportLateOutcome("TxResolved", m.from)
    updateLastEvent()
    resolved(m.committed)
  }

  def receiveFinalized(m: TxFinalized): Unit = if (ofinalized.isEmpty) {
    reportLateOutcome("TxFinalized", m.from)
    updateLastEvent()
    resolved(m.committed)
    ofinalized = Some(m.committed)
    releaseHeldCommit()
    // Whatever the outcome: a prepare after an abort, or from a recovery driver, may have saved this Tx again
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
