package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network._
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.paxos
import org.aspen_ddp.aspen.common.paxos.{Learner, ProposalId, Proposer}
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger
import scribe.Logging

import scala.concurrent.duration.{Duration, FiniteDuration, NANOSECONDS, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

object TransactionDriver {

  /** The delay before a failed finalizer is started again, doubling with each failure up to `FinalizerRetryMax` */
  val FinalizerRetryInitial: Duration = Duration(1, SECONDS)
  val FinalizerRetryMax: Duration = Duration(60, SECONDS)

  private[transaction] def finalizerRetryDelay(failures: Int): Duration =
    val doublings = math.min(failures - 1, 6)
    val delay = FinalizerRetryInitial * math.pow(2, doublings)
    if delay > FinalizerRetryMax then FinalizerRetryMax else delay

  /** How long after its commit resolution a driver's unfinalized transaction counts as a finalization stall
    * (`finalizationStall`), reported by its store at error (FinalizationStallReport). It is the backoff cap: by then
    * the finalizer failed several times, or has not finished in a minute
    */
  val FinalizationStallAfter: FiniteDuration = FiniteDuration(60, SECONDS)

  /** How a driver starts.
    *
    * @param initialProposal the driver is the designated leader's first for the transaction: the one its client's
    *                        prepare creates, at the proposal id that prepare carries, `(1, L)`. Every other driver starts
    *                        at round 2 or higher (paxos-transactions 7.5): a restarted or replacement leader reusing
    *                        `(1, L)` would be a second proposer with the first one's id, and a delayed Accept of the
    *                        first could flip acceptors that hold the second's value
    * @param highestSeen     the highest proposal id this store's own acceptor promised or accepted for the transaction.
    *                        A driver that does not start at the initial proposal starts above it
    * @param onSplitDecision called with a conflicting second outcome (P19), to log it as an error and count it.
    *                        A store passes its host status cache's TransactionStatusCache.recordSplitDecision; the
    *                        default only logs
    */
  case class Start(initialProposal: Boolean = false,
                   highestSeen: Option[ProposalId] = None,
                   onSplitDecision: (TransactionId, String) => Unit = logSplitDecision,
                   /** Whether the transaction is past the horizon by the driver's host clock: late condition 1 is
                     * then never taken (see `holdsProof`). Store passes its Frontend's clock and horizon */
                   pastHorizon: () => Boolean = () => false)

  private def logSplitDecision(txid: TransactionId, detail: String): Unit =
    scribe.error(s"Split decision: transaction $txid: $detail")

  trait Factory {
    def failedDriverDuration: Duration = Duration(2, SECONDS)

    def create(ec: ExecutionContext,
               storeId: StoreId,
               messenger:Messenger,
               backgroundTasks: BackgroundTaskManager,
               txd: TransactionDescription,
               finalizerFactory: TransactionFinalizer.Factory,
               start: Start): TransactionDriver
  }

  object noErrorRecoveryFactory extends Factory {

    class NoRecoveryTransactionDriver(ec: ExecutionContext,
                                      storeId: StoreId,
                                      messenger: Messenger,
                                      backgroundTasks: BackgroundTaskManager,
                                      txd: TransactionDescription,
                                      finalizerFactory: TransactionFinalizer.Factory,
                                      start: Start) extends TransactionDriver(
      storeId, messenger, backgroundTasks, txd, finalizerFactory, start)(using ec) {

      given ExecutionContext = ec

      var hung = false

      val hangCheckTask: BackgroundTaskManager.ScheduledTask = backgroundTasks.schedule(Duration(10, SECONDS)) {
        //val test = messenger.system.map(_.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST"))
        //println(s"**** HUNG TRANSACTION: $test")
        println(s"**** HUNG TRANSACTION")
        printState()
        synchronized {
          hung = true
        }
      }

      override def shutdown(): Unit =
        super.shutdown()
        hangCheckTask.cancel()

      override protected def onFinalized(committed: Boolean): Unit = {
        super.onFinalized(committed)
        synchronized {
          if (hung) {
            //val test = messenger.system.map(_.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST"))
            //println(s"**** HUNG TRANSACTION EVENTUALLY COMPLETED! : $test ")
            println(s"**** HUNG TRANSACTION EVENTUALLY COMPLETED!")
          }
        }
        hangCheckTask.cancel()
      }
    }

    def create(ec: ExecutionContext,
               storeId: StoreId,
               messenger:Messenger,
               backgroundTasks: BackgroundTaskManager,
               txd: TransactionDescription,
               finalizerFactory: TransactionFinalizer.Factory,
               start: Start): TransactionDriver = {
      new NoRecoveryTransactionDriver(ec, storeId, messenger, backgroundTasks, txd, finalizerFactory, start)
    }
  }
}


abstract class TransactionDriver(
                                  val storeId: StoreId,
                                  val messenger: Messenger,
                                  val backgroundTasks: BackgroundTaskManager,
                                  val txd: TransactionDescription,
                                  private val finalizerFactory: TransactionFinalizer.Factory,
                                  val start: TransactionDriver.Start = TransactionDriver.Start())(using ec: ExecutionContext) extends Logging {
  def ida: IDA = txd.primaryObjectIDA

  protected val proposer: Proposer = new Proposer(storeId.poolIndex, ida.width, ida.writeThreshold)
  start.highestSeen.foreach(proposer.updateHighestProposalId)

  /** Only the primary pool's stores propose (paxos-transactions 7.6, learn-only drivers). The proposer id is the
    * store's pool index, so a store of another pool with the same index would be a second proposer with the same ids,
    * and an acceptor accepts an equal id with any value. A driver on any other store is learn-only: it sends prepares,
    * whose replies (and the status-cache relays they draw) tell it the outcome, and it resolves, proves late and
    * finalizes as any driver does, but it never sets a proposal and never sends an Accept. Its prepares do not count as
    * activity at the primary stores (Tx.receivePrepare), so one of them still takes the transaction over when idle
    */
  val mayPropose: Boolean = storeId.poolId == txd.primaryObject.poolId
  protected val learner: Learner = new Learner(ida.width, ida.writeThreshold)
  protected val validAcceptorSet: Set[Byte] = (0 until ida.width).map(_.toByte).toSet
  protected val allObjects: Set[ObjectPointer] = txd.allReferencedObjectsSet
  protected val primaryObjectDataStores: Set[StoreId] = txd.primaryObjectDataStores
  protected val allDataStores: List[StoreId] = txd.allDataStores.toList

  protected var resolved: Boolean = false
  protected var finalized: Boolean = false
  protected var peerDispositions: Map[StoreId, TransactionDisposition.Value] = Map()

  /** The incarnation of every store heard from in any prepare reply, promise or nack, of any round. A reply without
    * one leaves a known value in place. It should not change within one transaction's voting (a rebuilt store is muted
    * for every transaction started before its acceptorSince), but that leans on the mute's clock assumptions: a store
    * that reports two different incarnations is recorded as unknown instead (conflictedIncarnations), for good, so its
    * entry names no incarnation, which a Tx holding its own commit vote applies through
    * (Frontend.unackedLookupComplete)
    */
  private var peerIncarnations: Map[StoreId, StoreIncarnation] = Map()

  /** Stores that reported more than one incarnation: see peerIncarnations */
  private var conflictedIncarnations: Set[StoreId] = Set()

  /** Must hold the lock */
  private def recordIncarnation(from: StoreId, inc: StoreIncarnation): Unit =
    if !conflictedIncarnations.contains(from) then
      peerIncarnations.get(from) match
        case Some(known) if known != inc =>
          logger.warn(s"Tx ${txd.transactionId} on store $storeId: store $from reported incarnations ${known.uuid} " +
            s"and ${inc.uuid}: recorded as unknown")
          peerIncarnations -= from
          conflictedIncarnations += from
        case _ =>
          peerIncarnations += (from -> inc)

  protected var acceptedPeers: Set[StoreId] = Set[StoreId]()
  /** Starts the finalization actions of a committed transaction, again after a failure (see FinalizerRunner) */
  private val finalizerRunner = new FinalizerRunner(this, storeId, txd, finalizerFactory, messenger, backgroundTasks,
    isFinalized = () => finalized,
    configure = f =>
      f.commitVoters(countedCommitVoters, peerIncarnations)

      // Update with initial commitErrors. This avoids problems when all TxCommit messages arrive
      // before we notice that resolution has been achieved
      f.updateCommitErrors(commitErrors, keptForReplay)
    ,
    onComplete = handedOff => onFinalized(true, handedOff))

  /** When this driver resolved the transaction committed (System.nanoTime), for the stall report */
  private var resolvedCommitNanos: Option[Long] = None

  /** The stores TxFinalized reports as owed an UnackedCommits entry. Empty unless this driver's finalizer wrote
    * entries (a non-late commit), or a TxFinalized from another driver, which reported them, finalized it
    */
  private var owed: Set[StoreId] = Set()
  protected var knownResolved: Set[StoreId] = Set()
  protected var resolvedValue: Boolean = false

  /** A store proved the commit (a `TxResolved` with `proof = true`): this driver never proposes abort, adopts only an
    * accepted commit, and resolves late once a prepare quorum of the current round reports no accepted commit
    * (paxos-transactions 10.5), but only while T is inside the horizon by this host's clock (`Start.pastHorizon`). See
    * `resolveLate`
    */
  private var holdsProof: Boolean = false

  /** The commit is late (see `resolveLate`): every TxResolved and TxFinalized this driver sends carries `late = true` */
  protected var late: Boolean = false

  // Stores IDs of objects that could not be updated on stores as part of a successful commit
  protected var commitErrors: Map[StoreId, List[ObjectId]] = Map()

  // Stores whose TxCommitted said they keep the transaction's CRL state for a reload to replay (TxCommitted.keptForReplay)
  protected var keptForReplay: Set[StoreId] = Set()

  private val completionPromise: Promise[TransactionDescription] = Promise()

  private var shouldSendAcceptMessages: Boolean = false

  /** This round's prepare quorum has queued its Accept: later promises of the round queue no second batch
    * (paxos-transactions 7.3) */
  private var acceptQueuedThisRound: Boolean = false
  private var clientResolved: Option[TransactionResolved] = None
  private var clientFinalized: Option[TransactionFinalized] = None
  private var txMessages: Option[List[TxMessage]] = None
  private var heartbeatDue: Boolean = false

  def designatedLeader: Boolean = mayPropose && txd.designatedLeaderUID == storeId.poolIndex

  /** Reports a conflicting second outcome (Start.onSplitDecision), which logs it as an error. Under the horizon none
    * should ever arise (P19)
    */
  private def splitDecision(detail: String): Unit =
    start.onSplitDecision(txd.transactionId, s"driver on store $storeId: $detail")

  def complete: Future[TransactionDescription] = completionPromise.future

  {
    val kind: String = if (designatedLeader)
      "Designated Leader"
    else
      "Transaction Recovery"
    logger.info(s"Driving transaction to completion ($kind): ${txd.shortString}")
  }

  //complete.foreach(_ => logger.info(s"Transaction driven to completion: ${txd.transactionId}"))

  def printState(print: String => Unit = println): Unit = synchronized {
    val sb = new StringBuilder

    sb.append("\n")
    sb.append(s"***** Transaction Status: ${txd.transactionId}")
    sb.append(s"  Store: $storeId\n")
    sb.append(s"  Objects: ${txd.allReferencedObjectsSet}\n")
    sb.append(s"  Resolved: $resolved. Finalized: $finalized. Result: ${learner.finalValue}\n")
    sb.append(s"  Peer Dispositions: $peerDispositions\n")
    sb.append(s"  Accepted Peers: $acceptedPeers\n")
    sb.append(s"  Finalizer: ${finalizerRunner.debugStatus}\n")
    sb.append(s"  ${txd.shortString}")

    print(sb.toString)
  }

  protected def isValidAcceptor(ds: StoreId): Boolean = {
    ds.poolId == txd.primaryObject.poolId && validAcceptorSet.contains(ds.poolIndex)
  }

  /** The driver resolved or finalized the transaction: it knows the outcome, and its finalization may still be running */
  def hasOutcome: Boolean = synchronized { resolved || finalized }

  /** The driver finalized: its TxFinalized is sent and it has no more work */
  def isFinalized: Boolean = synchronized { finalized }

  /** Releases the driver's timers. Overrides must call it: it stops a pending finalizer restart */
  def shutdown(): Unit = synchronized {
    finalizerRunner.shutdown()
  }

  private val startNanos: Long = System.nanoTime()
  private val stuckThrottle = new StuckReportThrottle(startNanos)

  /** Logs a driver that has not finalized long after it started, with the stores it has yet to hear from. Repeats
    * every `Tx.StuckReportEvery`. Returns whether it logged.
    */
  def reportIfStuck(now: Long = System.nanoTime()): Boolean = synchronized {
    val due = stuckThrottle.due(now, stuck = !finalized)
    if (due) {
      val unresolvedStores = allDataStores.filterNot(knownResolved.contains).map(_.poolIndex).mkString(",")
      val votes = peerDispositions.values.groupBy(identity).map((d, l) => s"$d=${l.size}").mkString(",")
      logger.warn(s"Stuck driver of Tx ${txd.transactionId} on store $storeId. " +
        s"Age: ${Duration(now - startNanos, NANOSECONDS).toMillis}ms. Resolved: $resolved. Value: $resolvedValue. " +
        s"Votes: $votes. Accepted: ${acceptedPeers.size}. Stores not known resolved: $unresolvedStores. " +
        s"Finalizer: ${finalizerRunner.debugStatus}")
    }
    due
  }

  /** A committed transaction this driver has not finalized `FinalizationStallAfter` or longer after resolving it: its
    * finalizer keeps failing (an action failing on every attempt, a metadata pool unable to take the marker's
    * writes, an entry too large for the tree), or has not finished. None otherwise. The driver keeps retrying; the
    * store reports these (FinalizationStallReport) so an operator can find the cause
    */
  def finalizationStall(now: Long = System.nanoTime()): Option[FinalizationStall] = synchronized {
    resolvedCommitNanos.filter(_ => !finalized).flatMap { at =>
      val age = FiniteDuration(now - at, NANOSECONDS)
      Option.when(age >= TransactionDriver.FinalizationStallAfter)(
        FinalizationStall(txd.transactionId, age, finalizerRunner.failureCount, finalizerRunner.lastFailureCause))
    }
  }

  /** Records messages for the next sendMessages. Nothing else sends what this replaces, so it must not be unsent */
  private def queueTxMessages(messages: List[TxMessage]): Unit = {
    txMessages.foreach { unsent =>
      logger.warn(s"Tx ${txd.transactionId} on store $storeId: ${unsent.size} unsent " +
        s"${unsent.headOption.map(_.getClass.getSimpleName)} replaced by " +
        s"${messages.headOption.map(_.getClass.getSimpleName)}")
    }
    txMessages = Some(messages)
  }

  /** A driver's first tick sends no TxHeartbeat, so transactions that finish within a heartbeat period send none. The
    * stores' silence is then at most two periods, well inside `Host.txHeartbeatTimeout`.
    */
  def heartbeat(): Unit = {
    reportIfStuck()
    // One snapshot under the lock, sent outside it: onFinalized, on another thread, writes `finalized` before the
    // outcome, so unlocked reads could send TxFinalized(committed = false) for a committed transaction
    val (isFinalized, committed, isLate, due, owedNow) = synchronized {
      val d = heartbeatDue
      heartbeatDue = true
      (finalized, resolvedValue, late, d, owed)
    }
    if (isFinalized)
      messenger.sendTransactionMessages(allDataStores.map(toStoreId =>
        TxFinalized(toStoreId, storeId, txd.transactionId, committed, isLate, owedNow, txd.startTimestamp)))
    else if (due)
      messenger.sendTransactionMessages(allDataStores.map(toStoreId => TxHeartbeat(toStoreId, storeId, txd.transactionId)))
  }

  // Must be called outside a syncrhonized block. Doing so avoids a race condition within the unit test
  // which can lead to a deadlock
  private def sendMessages(): Unit = {
    val (sendAccept, cliResolved, cliFinalized, txMsgs) = synchronized {
      val sa = shouldSendAcceptMessages
      val cm = clientResolved
      val cf = clientFinalized
      val tm = txMessages
      shouldSendAcceptMessages = false
      clientResolved = None
      clientFinalized = None
      txMessages = None
      (sa, cm, cf, tm)
    }

    if (sendAccept)
      sendAcceptMessages()

    txMsgs.foreach(messenger.sendTransactionMessages)
    cliFinalized.foreach(messenger.sendClientResponse)
    cliResolved.foreach(messenger.sendClientResponse)
  }

  def receiveTxUnknownStore(msg: TxUnknownStore): Unit = {
    // Don't worry about re-sending lost messages. Odds are the store was
    // transferred to a new host and has been offline for a relatively long
    // time. If the Tx is stalled waiting for this store to come online,
    // Dropping the cache will ensure that the next retransmit will use the
    // new host. Otherwise we can just complete without this store and let
    // the repair process take care of the missed Tx
    messenger.dropCacheForStore(msg.from)
  }

  private def queueAcceptMessages(): Unit =
    shouldSendAcceptMessages = true
    acceptQueuedThisRound = true

  def receiveTxPrepareResponse(msg: TxPrepareResponse): Unit = {

    synchronized {
      logger.trace(s"Received PrepareResponse ${msg.transactionId} ProposalId: ${msg.proposalId} current ProposalId ${proposer.currentProposalId} from ${msg.from}. Disposition: ${msg.disposition}")

      msg.incarnation.foreach(inc => recordIncarnation(msg.from, inc))

      if (msg.proposalId != proposer.currentProposalId)
        return

      // A collision is an ordinary abort vote, even when the colliding transaction has since completed. A store's
      // disposition is fixed once given, so re-sending the prepare would only return the same collision. The client
      // retries the aborted transaction instead. A store withholds the vote instead while the collision is with a
      // transaction this one is built on (see Tx.awaitableHolders), so such collisions rarely reach here.

      msg.response match {
        case Left(nack) =>
          onNack(nack.promisedId)

        case Right(promise) =>

          peerDispositions += (msg.from -> msg.disposition)

          if (isValidAcceptor(msg.from))
            proposer.receivePromise(paxos.Promise(msg.from.poolIndex, msg.proposalId, promise.lastAccepted))

          // A proof holder (paxos-transactions 10.5) adopts only an accepted commit of the current, post-proof round:
          // commit was chosen at some ballot c and every value accepted at c or higher is commit, so an accepted abort
          // predates c. A quorum reporting no accepted commit is memoryless and resolves late, inside the horizon only
          // (see resolveLate): past it the proof holder waits for a TxFinalized or an accepted commit, or is stopped
          // with its store's Tx (Store.removeExpiredDrivers). Neither case uses the vote tally,
          // which picks only a local proposal (a proof holder needs none, and the prover never votes, so the tally
          // could wait forever); Proposer.commitProven makes the proposal commit or none
          if (holdsProof && !resolved && !start.pastHorizon() && proposer.prepareQuorumWithoutAcceptedCommit)
            resolveLate()
          else if (holdsProof && !resolved && proposer.prepareQuorumReached && proposer.proposalValue.isDefined)
            shouldSendAcceptMessages = mayPropose
          else if (mayPropose && !holdsProof && proposer.prepareQuorumReached && !proposer.haveLocalProposal) {

            // Every object is classified, none skipped (paxos-transactions 7.2). Votes never change, so an object that
            // can no longer reach its write threshold of commit votes, whatever the silent stores say, makes abort
            // certain however many answers other objects still lack. An object is undecided while it has fewer than its
            // write threshold of commit votes and could still reach it
            var abortCertain = false
            var undecided = false

            for (ptr <- allObjects) {
              var nAbortVotes = 0
              var nCommitVotes = 0
              val objIDA = txd.poolIDAMap(ptr.poolId)

              (0 until objIDA.width).foreach { poolIndex =>
                // Undetermined is no vote (paxos-transactions 7.2): a Tx resolved before it voted answers so, and
                // counting it as an abort could let a later round propose abort over write-threshold real commit votes
                peerDispositions.get(StoreId(ptr.poolId, poolIndex.toByte)).foreach {
                  case TransactionDisposition.Undetermined => ()
                  case TransactionDisposition.VoteCommit => nCommitVotes += 1
                  case _ => nAbortVotes += 1
                }
              }

              if (objIDA.width - nAbortVotes < objIDA.writeThreshold)
                abortCertain = true
              else if (nCommitVotes < objIDA.writeThreshold)
                undecided = true
            }

            if (abortCertain) {
              proposer.setLocalProposal(false)
              queueAcceptMessages()
            } else if (!undecided) {
              // A write threshold of commit votes on every object (P6). A driver holding a proof never reaches here
              proposer.setLocalProposal(true)
              queueAcceptMessages()
            } else if (proposer.maySendAccept) {
              // The votes cannot decide yet, but a promise of this round's quorum carries an accepted value, which a
              // proposer may always propose. Without this, stores answering Undetermined (resolved before voting)
              // would stall a round whose quorum already reports the chosen value
              queueAcceptMessages()
            }
          } else if (mayPropose && !holdsProof && proposer.prepareQuorumReached && !acceptQueuedThisRound) {
            // A later round (paxos-transactions 7.3). The local proposal outlives the round it was set in: votes never
            // change, so the tally would decide the same again. Proposer.proposalValue still prefers an accepted value
            // the new quorum reports. Without this the Accept would wait for the next periodic retransmit
            queueAcceptMessages()
          }
      }
    }

    // call outside synchronization block
    sendMessages()
  }

  def receiveTxAcceptResponse(msg: TxAcceptResponse): Unit = {

    synchronized {
      logger.trace(s"Received AcceptResponse ${msg.transactionId} ProposalId: ${msg.proposalId} current ProposalId ${proposer.currentProposalId} from ${msg.from}. Response: ${msg.response}")

      if (msg.proposalId != proposer.currentProposalId)
        return

      // We shouldn't ever receive an AcceptResponse from a non-acceptor but just to be safe...
      if (!isValidAcceptor(msg.from))
        return

      msg.response match {
        case Left(nack) =>
          onNack(nack.promisedId)

        case Right(accepted) =>
          acceptedPeers += msg.from

          logger.trace(s"Received Accepted from ${msg.from}. ProposalId: ${msg.proposalId}. Committed: ${accepted.value}")

          learner.receiveAccepted(paxos.Accepted(msg.from.poolIndex, msg.proposalId, accepted.value))

          learner.finalValue.foreach(onResolution(_, sendResolutionMessage = true))
      }
    }

    // call outside synchronization block
    sendMessages()
  }

  def receiveTxResolved(msg: TxResolved): Unit = {
    val startedPostProofRound = synchronized {
      knownResolved += msg.from
      if (msg.committed && msg.late) {
        resolveLate()
        false
      } else if (msg.committed && msg.proof) {
        // The first proof starts a new round: promises to prepares sent before it may predate the choice of commit
        // (see resolveLate), so they never count. Every later round is post-proof too. A driver that already
        // resolved learned the outcome by Paxos, and its own finalizer must complete
        val start = !holdsProof && !resolved && !finalized
        holdsProof = true
        proposer.commitProven()
        if (start)
          nextRound()
        start
      } else {
        onResolution(msg.committed, sendResolutionMessage=false)
        false
      }
    }

    // call outside synchronization block
    sendMessages()
    if (startedPostProofRound)
      sendPrepareMessages()
  }

  def receiveTxCommitted(msg: TxCommitted): Unit = {
    synchronized {
      knownResolved += msg.from
      commitErrors += (msg.from -> msg.objectCommitErrors)
      if msg.keptForReplay then
        keptForReplay += msg.from
      finalizerRunner.updateCommitErrors(commitErrors, keptForReplay)
    }
  }

  def receiveTxFinalized(msg: TxFinalized): Unit = {
    synchronized {
      knownResolved += msg.from
      // A late commit after an abort is reported by resolveLate
      if ((resolved || finalized) && resolvedValue != msg.committed && !(msg.committed && msg.late))
        splitDecision(s"TxFinalized(committed = ${msg.committed}) from ${msg.from} after the outcome $resolvedValue")
      if (resolved && resolvedValue && !msg.committed) {
        // Committed wins, as in the status cache (P19): the stores hold the commit and nothing rolls it back, so
        // this driver keeps finalizing it rather than spreading the abort
      } else if (msg.committed && msg.late)
        resolveLate()
      else {
        // Finalizes at once (transaction-recovery 5.4), inside the status-cache window, never waiting for this driver's
        // own mark. The cancel comes first: a marker not yet handed off then writes nothing, and one already handed off
        // is not recalled and lists the stores it may still write (handedOffStores), whatever that write's outcome. So
        // the TxFinalized this driver sends lists every store that may get an entry from either driver. A stub may then
        // be written before its entry is durable; the repair scan allows for that (transaction-recovery 5.4, applied
        // stubs)
        finalizerRunner.cancel()
        val planned = if msg.committed then finalizerRunner.handedOffStores else Set[StoreId]()
        onFinalized(msg.committed, msg.owed ++ planned)
      }
    }

    // call outside synchronization block
    sendMessages()
  }

  /** Finalizes with the stores `owedStores` as the TxFinalized `owed`, set in the same critical section as the
    * finalization so a late resolution between them cannot send a non-empty `owed` with `late`. Aborts and late commits
    * owe nothing
    */
  private def onFinalized(committed: Boolean, owedStores: Set[StoreId]): Unit =
    synchronized:
      if !finalized then
        owed = if committed && !late then owedStores else Set()
      onFinalized(committed)

  protected def onFinalized(committed: Boolean): Unit = {
    synchronized {
      if (!finalized) {
        finalized = true
        resolvedValue = committed

        txd.originatingClient.foreach(client => {
          logger.trace(s"Sending TxFinalized(${txd.transactionId}) to originating client $client. Committed: $committed")
          clientFinalized = Some(TransactionFinalized(client, storeId, txd.transactionId, committed))
        })

        val messages = allDataStores.map(toStoreId =>
          TxFinalized(toStoreId, storeId, txd.transactionId, committed, late, owed, txd.startTimestamp))

        logger.trace(s"Sending TxFinalized(${txd.transactionId}) to ${messages.head.to.poolId}:(${messages.map(_.to.poolIndex)})")

        queueTxMessages(messages)

        shutdown() // release retry resources

        completionPromise.success(txd)
      }
    }

    // call outside synchronization block
    sendMessages()
  }

  protected def nextRound(): Unit = synchronized {
    peerDispositions = Map()
    acceptedPeers = Set()
    acceptQueuedThisRound = false
    proposer.nextRound()
  }

  /** The outcome to send in place of prepares: a late commit, or the value the learner learned */
  private def knownOutcome: Option[Boolean] = if late then Some(true) else learner.finalValue

  protected def sendPrepareMessages(): Unit = {
    val (proposalId, alreadyPrepared, ofinal, isLate) = synchronized {
      (proposer.currentProposalId, peerDispositions, knownOutcome, late)
    }

    // Send TxResolved if resolution has been achieved, otherwise Prepare messages
    val messages = ofinal match {
      case Some(result) => txd.allDataStores.map(TxResolved(_, storeId, txd.transactionId, result, isLate, proof = false))
      case None => txd.allDataStores.filter(!alreadyPrepared.contains(_)).map(to =>
        TxPrepare(to, storeId, txd, proposalId, Nil, fromClient = false))
    }

//    if (messages.nonEmpty)
//      logger.trace(s"Sending ${messages.head.getClass.getSimpleName}(${txd.transactionId) to ${messages.head.to.poolId}:${messages.map(_.to.poolIndex).toList}")

    messenger.sendTransactionMessages(messages.toList)
  }

  protected def sendPrepareMessage(toStoreId: StoreId): Unit = {
    val (proposalId, ofinal, isLate) = synchronized { (proposer.currentProposalId, knownOutcome, late) }

    ofinal match {
      case Some(result) =>
        //logger.trace(s"Sending TxResolved(${txd.transactionId}) committed = $result to $toStoreId")
        messenger.sendTransactionMessage(TxResolved(toStoreId, storeId, txd.transactionId, result, isLate, proof = false))

      case None =>
        //logger.trace(s"Sending TxPrepare(${txd.transactionId}) to $toStoreId")
        messenger.sendTransactionMessage(TxPrepare(toStoreId, storeId, txd, proposalId, Nil, fromClient = false))
    }
  }

  /** The Accept of the current round, if any. None on a learn-only driver (mayPropose) */
  protected def currentAcceptMessage(): Option[paxos.Accept] = synchronized {
    if mayPropose then proposer.currentAcceptMessage() else None
  }

  protected def sendAcceptMessages(): Unit = {
    synchronized {
      currentAcceptMessage().map(paxAccept => (paxAccept, acceptedPeers))
    } foreach { t =>
      val (paxAccept, alreadyAccepted) = t
      val messages = primaryObjectDataStores.filter(!alreadyAccepted.contains(_)).map { toStoreId =>
        TxAccept(toStoreId, storeId, txd.transactionId, paxAccept.proposalId, paxAccept.proposalValue)
      }.toList
      if (messages.nonEmpty)
        logger.trace(s"Sending TxAccept(${txd.transactionId}) to ${messages.head.to.poolId}:(${messages.map(_.to.poolIndex)})")
      messenger.sendTransactionMessages(messages)
    }
  }

  protected def onNack(promisedId: paxos.ProposalId): Unit = {
    synchronized {
      logger.trace(s"NACK received! Current ProposalId: ${proposer.currentProposalId}. Highest: ${promisedId}")
      proposer.updateHighestProposalId(promisedId)
    }
    nextRound()
  }

  /** The stores this driver received commit votes from, when they reach the write threshold of every object; None
    * otherwise (the outcome came from another driver or a recovered accepted value). Only these voters can hold a
    * saved commit vote that the commit relied on, so only they need a UnackedCommits entry
    */
  private def countedCommitVoters: Option[Set[StoreId]] =
    val voters = peerDispositions.collect { case (s, TransactionDisposition.VoteCommit) => s }.toSet
    val enough = allObjects.forall: ptr =>
      val ida = txd.poolIDAMap(ptr.poolId)
      (0 until ida.width).count(i => voters.contains(StoreId(ptr.poolId, i.toByte))) >= ida.writeThreshold
    Option.when(enough)(voters)

  /** Resolves a late commit: one this driver reaches only after the transaction was finalized and forgotten
    * (docs/superpowers/specs/2026-11-09-replay-pins-design.md, 1b and 1c). Reached on:
    *  - a proof (`TxResolved` with `proof = true`) held together with a prepare quorum of valid acceptors, in a round
    *    started after the proof arrived, none of whose promises carries an accepted commit (paxos-transactions 10.5).
    *    The proof shows the commit was chosen at some ballot c, so a quorum Q accepted it before the proof was sent,
    *    and so before any post-proof prepare. Every value accepted at c or higher is commit, so a reported accepted
    *    abort predates c and shows nothing of Q's acceptance. That quorum intersects the responding one, so some
    *    acceptor in Q forgot the transaction. An acceptor forgets a
    *    committed transaction only on TxFinalized, or on a late commit, which follows it (Tx.unackedCommitFound
    *    applies an owed update but keeps the Tx until then, as Tx.unackedCommitNotOwed does for a recovered vote,
    *    applying nothing); a rebuilt one is muted and does not answer. The mute
    *    compares the client-chosen start timestamp with the rebuilt store's acceptor-since time, and holds because no
    *    store joins a transaction started more than Frontend.MaxStartLead ahead of its host's clock: residual, hosts'
    *    clocks skewed by more than RebuildingStore.AcceptorSinceMargin less that lead, or a zombie old copy. A prover
    *    (Tx.proveCommitted) answers the post-proof round with a promise carrying no accepted value: it holds no state
    *    for the transaction, so if it was in Q it forgot on TxFinalized too. Promises to earlier rounds never count: they may have been given
    *    before Q accepted, at a higher proposal of another driver, and then prove nothing (the prover may be a muted
    *    rebuilt store while that driver's finalizer still runs). The first proof therefore starts a new round
    *    (`receiveTxResolved`). Taken only while T is inside the horizon by this host's clock (`Start.pastHorizon`).
    *    The argument needs a commit chosen by Paxos: an expiry commit (Frontend.decideExpiry) is accepted by no
    *    acceptor, and its holders not yet frozen by their own clocks answer memoryless promises while the expiry
    *    driver's finalizer still writes its marks; resolving late then would cancel that finalizer. An expiry commit is
    *    decided only past the horizon plus the expiry margin by the decider's clock, and the margin (8 s) covers the
    *    inter-host skew (RebuildingStore.AcceptorSinceMargin less MaxStartLead), so it exists only once T is past the
    *    horizon by every host's clock. Past the horizon the condition proves nothing anyway: a store that forgot T
    *    refuses its prepares (Frontend.forgetFloorMillis), and inside its own horizon answers by the finalized relay;
    *  - a `TxResolved` or `TxFinalized` with `late = true`: another driver, or a store's finalized relay, already
    *    knows it is late. A store's status cache marks a transaction finalized only from a real TxFinalized or a late
    *    outcome.
    *
    * Either way some acceptor forgot T, which it does only on TxFinalized (or a late outcome, which itself follows one),
    * and TxFinalized is sent only once a finalizer completed every action, the marker's UnackedCommits entries included
    * (RegisteredTransactionFinalizerFactory.Finalizer; see the ordering premise on `onResolution`). So this runs no
    * finalizer (and cancels any it started: re-running them would itself be
    * a replay) and finalizes at once. Every TxResolved and TxFinalized sent from here on carries `late = true`, so
    * receiving stores apply the commit only where an UnackedCommits entry says it is owed. A late commit sends no
    * accepts.
    */
  private def resolveLate(): Unit = synchronized {
    if (!finalized) {
      if (resolved && !resolvedValue)
        splitDecision("late commit after an abort resolution; ignored")
      else {
        logger.info(s"Tx ${txd.transactionId} on store $storeId: late commit, finalization actions already ran")
        late = true
        resolved = true
        resolvedValue = true
        finalizerRunner.cancel()
        onFinalized(true)
      }
    }
  }

  /** Starts the finalizer of a committed transaction, or finalizes an aborted one at once.
    *
    * Ordering premise: this driver sends TxFinalized only from onFinalized, which a commit reaches only once the
    * finalizer completes (`FinalizerRunner`: a failed one is started again, never skipped), or on a TxFinalized from
    * another driver, sent under the same rule, or on a late commit (`resolveLate`), which follows a TxFinalized, so a
    * finalization whose actions all completed. A store forgets a committed transaction it voted on only on TxFinalized
    * (Frontend.receiveTransactionMessage via Tx.receiveFinalized), or on a late commit, which follows TxFinalized
    * (Tx.lateCommit). Finding its UnackedCommits entry (Tx.unackedCommitFound) applies the update but does not forget
    * it, nor does finding one of another incarnation (Tx.unackedCommitNotOwed), which applies nothing. So everything the first finalizer to complete writes, in every pool, is durable before any voter forgets the
    * transaction. A second driver finalized by its TxFinalized does not wait for its own cancelled mark: entries that
    * mark had already handed off may land after its TxFinalized, which lists their stores (transaction-recovery 5.4);
    * so may those of this driver's own failed finalizers, whose stores its TxFinalized lists too (`failedHandedOff`). A
    * muted Tx (Frontend.forgetMuted) and a prover (Tx.proveCommitted) are dropped without TxFinalized, but neither
    * voted commit. Tx.unackedCommitFound, Tx.unackedCommitNotOwed (for a recovered or expiring vote) and
    * Tx.proveCommitted record nothing in the host's status cache, so a
    * finalized status, and the TxFinalized(late = true) a store relays from it, always stems from a real TxFinalized
    * or a late outcome.
    */
  protected def onResolution(committed: Boolean, sendResolutionMessage: Boolean): Unit = synchronized {
    if (resolved && resolvedValue != committed)
      splitDecision(s"resolved $committed after the outcome $resolvedValue; the first is kept")
    if (!resolved) {

      resolved = true

      resolvedValue = committed

      logger.trace(s"Got TxResolved message for transaction ${txd.transactionId}. Committed: $committed")

      if (committed) {

        resolvedCommitNanos = Some(System.nanoTime())

        // TODO: If sendResolutionMessage is false, another driver sent us a resolution message and will have
        //       started executing the finalizers. Ideally, we'd watch the heartbeats of the other driver and
        //       only start the finalizers ourselves if they time out to prevent contention. Could be tricky to
        //       get this right though so we'll go with the simple approach for now

        finalizerRunner.start()
      } else
        onFinalized(false)

      if (sendResolutionMessage) {
        val messages = (allDataStores.iterator ++ txd.notifyOnResolution.iterator).map { toStoreId =>
          TxResolved(toStoreId, storeId, txd.transactionId, committed, late, proof = false)
        }.toList

        //logger.trace(s"Sending TxResolved(${txd.transactionId}) committed = $committed to ${messages.head.to.poolId}:(${messages.map(_.to.poolIndex)})")

        queueTxMessages(messages)

        clientResolved = txd.originatingClient.map { clientId =>
          logger.trace(s"Sending TxResolved(${txd.transactionId}) committed = $committed to originating client $clientId")
          TransactionResolved(clientId, storeId, txd.transactionId, committed)
        }
      }

    }
  }
}

