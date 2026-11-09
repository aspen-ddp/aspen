package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network._
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.paxos
import org.aspen_ddp.aspen.common.paxos.{Learner, Proposer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionDisposition}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger
import scribe.Logging

import scala.concurrent.duration.{Duration, NANOSECONDS, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.boundary, boundary.break

object TransactionDriver {

  trait Factory {
    def failedDriverDuration: Duration = Duration(2, SECONDS)

    def create(ec: ExecutionContext,
               storeId: StoreId,
               messenger:Messenger,
               backgroundTasks: BackgroundTaskManager,
               txd: TransactionDescription,
               finalizerFactory: TransactionFinalizer.Factory): TransactionDriver
  }

  object noErrorRecoveryFactory extends Factory {

    class NoRecoveryTransactionDriver(ec: ExecutionContext,
                                      storeId: StoreId,
                                      messenger: Messenger,
                                      backgroundTasks: BackgroundTaskManager,
                                      txd: TransactionDescription,
                                      finalizerFactory: TransactionFinalizer.Factory) extends TransactionDriver(
      storeId, messenger, backgroundTasks, txd, finalizerFactory)(using ec) {

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
               finalizerFactory: TransactionFinalizer.Factory): TransactionDriver = {
      new NoRecoveryTransactionDriver(ec, storeId, messenger, backgroundTasks, txd, finalizerFactory)
    }
  }
}


abstract class TransactionDriver(
                                  val storeId: StoreId,
                                  val messenger: Messenger,
                                  val backgroundTasks: BackgroundTaskManager,
                                  val txd: TransactionDescription,
                                  private val finalizerFactory: TransactionFinalizer.Factory)(using ec: ExecutionContext) extends Logging {
  def ida: IDA = txd.primaryObjectIDA

  protected val proposer: Proposer = new Proposer(storeId.poolIndex, ida.width, ida.writeThreshold)
  protected val learner: Learner = new Learner(ida.width, ida.writeThreshold)
  protected val validAcceptorSet: Set[Byte] = (0 until ida.width).map(_.toByte).toSet
  protected val allObjects: Set[ObjectPointer] = txd.allReferencedObjectsSet
  protected val primaryObjectDataStores: Set[StoreId] = txd.primaryObjectDataStores
  protected val allDataStores: List[StoreId] = txd.allDataStores.toList

  protected var resolved: Boolean = false
  protected var finalized: Boolean = false
  protected var peerDispositions: Map[StoreId, TransactionDisposition.Value] = Map()
  protected var acceptedPeers: Set[StoreId] = Set[StoreId]()
  protected var finalizer: Option[TransactionFinalizer] = None
  protected var knownResolved: Set[StoreId] = Set()
  protected var resolvedValue: Boolean = false

  /** A store proved the commit (a `TxResolved` with `proof = true`): this driver never proposes abort, and resolves
    * late once a prepare quorum of the current round remembers nothing of the transaction. See `resolveLate`
    */
  private var holdsProof: Boolean = false

  /** The commit is late (see `resolveLate`): every TxResolved and TxFinalized this driver sends carries `late = true` */
  protected var late: Boolean = false

  // Stores IDs of objects that could not be updated on stores as part of a successful commit
  protected var commitErrors: Map[StoreId, List[ObjectId]] = Map()

  private val completionPromise: Promise[TransactionDescription] = Promise()

  private var shouldSendAcceptMessages: Boolean = false
  private var clientResolved: Option[TransactionResolved] = None
  private var clientFinalized: Option[TransactionFinalized] = None
  private var txMessages: Option[List[TxMessage]] = None
  private var heartbeatDue: Boolean = false

  def designatedLeader: Boolean = txd.designatedLeaderUID == storeId.poolIndex

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
    sb.append(s"  Finalizer: ${finalizer.map(o => o.debugStatus)}\n")
    sb.append(s"  ${txd.shortString}")

    print(sb.toString)
  }

  protected def isValidAcceptor(ds: StoreId): Boolean = {
    ds.poolId == txd.primaryObject.poolId && validAcceptorSet.contains(ds.poolIndex)
  }

  def shutdown(): Unit = {}

  private val startNanos: Long = System.nanoTime()
  private var lastStuckReport: Option[Long] = None

  /** Logs a driver that has not finalized long after it started, with the stores it has yet to hear from. Repeats
    * every `Tx.StuckReportEvery`. Returns whether it logged.
    */
  def reportIfStuck(now: Long = System.nanoTime()): Boolean = synchronized {
    val due = !finalized && now - startNanos >= Tx.StuckReportAfter.toNanos &&
      lastStuckReport.forall(last => now - last >= Tx.StuckReportEvery.toNanos)
    if (due) {
      lastStuckReport = Some(now)
      val unresolvedStores = allDataStores.filterNot(knownResolved.contains).map(_.poolIndex).mkString(",")
      val votes = peerDispositions.values.groupBy(identity).map((d, l) => s"$d=${l.size}").mkString(",")
      logger.warn(s"Stuck driver of Tx ${txd.transactionId} on store $storeId. " +
        s"Age: ${Duration(now - startNanos, NANOSECONDS).toMillis}ms. Resolved: $resolved. Value: $resolvedValue. " +
        s"Votes: $votes. Accepted: ${acceptedPeers.size}. Stores not known resolved: $unresolvedStores. " +
        s"Finalizer: ${finalizer.map(_.debugStatus)}")
    }
    due
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
    if (finalized)
      messenger.sendTransactionMessages(allDataStores.map(toStoreId => TxFinalized(toStoreId, storeId, txd.transactionId, resolvedValue, late)))
    else if (heartbeatDue)
      messenger.sendTransactionMessages(allDataStores.map(toStoreId => TxHeartbeat(toStoreId, storeId, txd.transactionId)))
    heartbeatDue = true
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

  def receiveTxPrepare(msg: TxPrepare): Unit = synchronized {
    proposer.updateHighestProposalId(msg.proposalId)
  }

  def receiveTxPrepareResponse(msg: TxPrepareResponse): Unit = {

    synchronized {
      logger.trace(s"Received PrepareResponse ${msg.transactionId} ProposalId: ${msg.proposalId} current ProposalId ${proposer.currentProposalId} from ${msg.from}. Disposition: ${msg.disposition}")

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

          if (holdsProof && !resolved && proposer.prepareQuorumWithoutAcceptedValue)
            resolveLate()
          else if (holdsProof && proposer.prepareQuorumReached && !proposer.haveLocalProposal &&
                   proposer.proposalValue.isDefined)
            // An accepted value to adopt: Paxos proposes it as usual. The vote tally only picks a local proposal,
            // which a proof holder needs none of, and the prover never votes, so the tally could wait forever
            shouldSendAcceptMessages = true
          else if (proposer.prepareQuorumReached && !proposer.haveLocalProposal) {

            var canCommitTransaction = true
            var cantAttempt = false

            // Before we can make a decision, we must ensure that we have a write-threshold number of replies for
            // each of the objects referenced by the transaction
            boundary:
              for (ptr <- allObjects) {
                var nReplies = 0
                var nAbortVotes = 0
                var nCommitVotes = 0
                var canCommitObject = true
                val objIDA = txd.poolIDAMap(ptr.poolId)

                (0 until objIDA.width).foreach { poolIndex =>
                  peerDispositions.get(StoreId(ptr.poolId, poolIndex.toByte)).foreach { disposition =>
                    nReplies += 1
                    disposition match {
                      case TransactionDisposition.VoteCommit => nCommitVotes += 1
                      case _ =>
                        // TODO: We can be quite a bit smarter about choosing when we must abort
                        nAbortVotes += 1
                    }
                  }
                }

                if (nReplies < objIDA.writeThreshold) {
                  cantAttempt = true
                  break()
                }

                if (nReplies != objIDA.width && nCommitVotes < objIDA.writeThreshold && objIDA.width - nAbortVotes >= objIDA.writeThreshold) {
                  cantAttempt = true
                  break()
                }

                if (objIDA.width - nAbortVotes < objIDA.writeThreshold)
                  canCommitObject = false

                // Once canCommitTransaction flips to false, it stays there
                canCommitTransaction = canCommitTransaction && canCommitObject
              }

            if (cantAttempt)
              return

            // If we get here, we've made our decision. A driver holding a proof never proposes abort: it leaves the
            // proposal unset, so only an accepted value (adopted by Paxos as usual) or a later round decides
            if !proposer.haveLocalProposal && (canCommitTransaction || !holdsProof) then
              proposer.setLocalProposal(canCommitTransaction)

            shouldSendAcceptMessages = true
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
        val start = !holdsProof && !resolved
        holdsProof = true
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
      finalizer.foreach(_.updateCommitErrors(commitErrors))
    }
  }

  def receiveTxFinalized(msg: TxFinalized): Unit = {
    synchronized {
      knownResolved += msg.from
      if (msg.committed && msg.late)
        resolveLate()
      else {
        finalizer.foreach( _.cancel() )
        onFinalized(msg.committed)
      }
    }

    // call outside synchronization block
    sendMessages()
  }

  def mayBeDiscarded: Boolean = synchronized { finalized }

  protected def onFinalized(committed: Boolean): Unit = {
    synchronized {
      if (!finalized) {
        finalized = true
        resolvedValue = committed

        txd.originatingClient.foreach(client => {
          logger.trace(s"Sending TxFinalized(${txd.transactionId}) to originating client $client. Committed: $committed")
          clientFinalized = Some(TransactionFinalized(client, storeId, txd.transactionId, committed))
        })

        val messages = allDataStores.map(toStoreId => TxFinalized(toStoreId, storeId, txd.transactionId, committed, late))

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
        TxPrepare(to, storeId, txd, proposalId, Nil, Nil, fromClient = false))
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
        messenger.sendTransactionMessage(TxPrepare(toStoreId, storeId, txd, proposalId, Nil, Nil, fromClient = false))
    }
  }

  protected def sendAcceptMessages(): Unit = {
    synchronized {
      proposer.currentAcceptMessage().map(paxAccept => (paxAccept, acceptedPeers))
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

  /** Starts the finalizer of a committed transaction, or finalizes an aborted one at once.
    *
    * Ordering premise: this driver sends TxFinalized only from onFinalized, which a commit reaches only once the
    * finalizer completes (`f.complete foreach onFinalized`), or on a TxFinalized from another driver, sent under the
    * same rule, or on a late commit (`resolveLate`), which follows a finalization that already completed. A store
    * forgets a committed transaction only on TxFinalized (Frontend.receiveTransactionMessage via Tx.receiveFinalized)
    * or when the UnackedCommits tree holds its entry (Frontend.unackedLookupComplete via Tx.unackedCommitFound). So
    * everything the finalizer writes, its UnackedCommits entries included, is durable before any store forgets the
    * transaction. A muted Tx (Frontend.forgetMuted) is dropped without either, but it never voted. Tx.unackedCommitFound
    * records the transaction Committed, not finalized, in its host's status cache (replay-pins design, section 1c), so
    * a finalized status, and the TxFinalized a store relays from it, always stems from a real TxFinalized.
    */
  /** Resolves a late commit: one this driver reaches only after the transaction was finalized and forgotten
    * (docs/superpowers/specs/2026-11-09-replay-pins-design.md, 1b and 1c). Reached on:
    *  - a proof (`TxResolved` with `proof = true`) held together with a prepare quorum of valid acceptors, in a round
    *    started after the proof arrived, none of whose promises carries an accepted value. The proof shows the commit
    *    was chosen, so a quorum Q accepted it before the proof was sent, and so before any post-proof prepare. That
    *    quorum intersects the responding one, and an acceptor drops its state only on TxFinalized (a rebuilt one is
    *    muted and does not answer), so the transaction was finalized. Promises to earlier rounds never count: they
    *    may have been given before Q accepted, at a higher proposal of another driver, and then prove nothing (the
    *    prover may be a muted rebuilt store while that driver's finalizer still runs). The first proof therefore
    *    starts a new round (`receiveTxResolved`);
    *  - a `TxResolved` or `TxFinalized` with `late = true`: another driver, or a store's finalized relay, already
    *    knows it is late. A store's status cache marks a transaction finalized only on a real TxFinalized.
    *
    * The original finalization completed (see the ordering premise on `onResolution`): its UnackedCommits entries are
    * durable, so this runs no finalizer (and cancels any it started: re-running them would itself be a replay) and
    * finalizes at once. Every TxResolved and TxFinalized sent from here on carries `late = true`, so receiving stores
    * apply the commit only where an UnackedCommits entry says it is owed. A late commit sends no accepts.
    */
  private def resolveLate(): Unit = synchronized {
    if (!finalized) {
      if (resolved && !resolvedValue)
        logger.error(s"Tx ${txd.transactionId} on store $storeId: late commit after an abort resolution; ignored")
      else {
        logger.info(s"Tx ${txd.transactionId} on store $storeId: late commit, finalization actions already ran")
        late = true
        resolved = true
        resolvedValue = true
        finalizer.foreach(_.cancel())
        onFinalized(true)
      }
    }
  }

  protected def onResolution(committed: Boolean, sendResolutionMessage: Boolean): Unit = synchronized {
    if (!resolved) {

      resolved = true

      resolvedValue = committed

      logger.trace(s"Got TxResolved message for transaction ${txd.transactionId}. Committed: $committed")

      if (committed) {

        // TODO: If sendResolutionMessage is false, another driver sent us a resolution message and will have
        //       started executing the finalizers. Ideally, we'd watch the heartbeats of the other driver and
        //       only start the finalizers ourselves if they time out to prevent contention. Could be tricky to
        //       get this right though so we'll go with the simple approach for now

        logger.trace(s"Running finalization actions for transaction ${txd.transactionId}")
        val f = finalizerFactory.create(txd, messenger)

        f.commitVoters(countedCommitVoters)

        // Update with initial commitErrors. This avoids problems when all TxCommit messages arrive
        // before we notice that resolution has been achieved
        f.updateCommitErrors(commitErrors)

        f.complete foreach { _ =>
          logger.trace(s"Finalization actions completed for transaction ${txd.transactionId}")
          onFinalized(committed)
        }

        finalizer = Some(f)
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

