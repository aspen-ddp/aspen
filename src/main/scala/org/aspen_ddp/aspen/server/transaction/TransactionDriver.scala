package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network._
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.paxos
import org.aspen_ddp.aspen.common.paxos.{Learner, Proposer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionDisposition, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTask
import org.aspen_ddp.aspen.server.network.Messenger
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.boundary, boundary.break

object TransactionDriver {

  trait Factory {
    def failedDriverDuration: Duration = Duration(2, SECONDS)

    def create( ec: ExecutionContext,
                storeId: StoreId,
                messenger:Messenger,
                backgroundTasks: BackgroundTask,
                txd: TransactionDescription,
                finalizerFactory: TransactionFinalizer.Factory): TransactionDriver
  }

  object noErrorRecoveryFactory extends Factory {

    class NoRecoveryTransactionDriver( ec: ExecutionContext,
                                       storeId: StoreId,
                                       messenger: Messenger,
                                       backgroundTasks: BackgroundTask,
                                       txd: TransactionDescription,
                                       finalizerFactory: TransactionFinalizer.Factory) extends TransactionDriver(
      storeId, messenger, backgroundTasks, txd, finalizerFactory)(using ec) {

      given ExecutionContext = ec

      var hung = false

      val hangCheckTask: BackgroundTask.ScheduledTask = backgroundTasks.schedule(Duration(10, SECONDS)) {
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

    def create( ec: ExecutionContext,
                storeId: StoreId,
                messenger:Messenger,
                backgroundTasks: BackgroundTask,
                txd: TransactionDescription,
                finalizerFactory: TransactionFinalizer.Factory): TransactionDriver = {
      new NoRecoveryTransactionDriver(ec, storeId, messenger, backgroundTasks, txd, finalizerFactory)
    }
  }
}


abstract class TransactionDriver(
                                  val storeId: StoreId,
                                  val messenger: Messenger,
                                  val backgroundTasks: BackgroundTask,
                                  val txd: TransactionDescription,
                                  private val finalizerFactory: TransactionFinalizer.Factory)(using ec: ExecutionContext) extends Logging {
  def ida: IDA = txd.primaryObject.ida

  protected val proposer: Proposer = new Proposer(storeId.poolIndex, ida.width, ida.writeThreshold)
  protected val learner: Learner = new Learner(ida.width, ida.writeThreshold)
  protected val validAcceptorSet: Set[Byte] = txd.primaryObject.storePointers.iterator.map(sp => sp.poolIndex).toSet
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

  // Stores IDs of objects that could not be updated on stores as part of a successful commit
  protected var commitErrors: Map[StoreId, List[ObjectId]] = Map()

  private val completionPromise: Promise[TransactionDescription] = Promise()

  private var shouldSendAcceptMessages: Boolean = false
  private var targetedPrepare: Option[StoreId] = None
  private var clientResolved: Option[TransactionResolved] = None
  private var clientFinalized: Option[TransactionFinalized] = None
  private var txMessages: Option[List[TxMessage]] = None

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

  def heartbeat(): Unit = {
    if (finalized)
      messenger.sendTransactionMessages(allDataStores.map(toStoreId => TxFinalized(toStoreId, storeId, txd.transactionId, resolvedValue)))
    else
      messenger.sendTransactionMessages(allDataStores.map(toStoreId => TxHeartbeat(toStoreId, storeId, txd.transactionId)))
  }

  // Must be called outside a syncrhonized block. Doing so avoids a race condition within the unit test
  // which can lead to a deadlock
  private def sendMessages(): Unit = {
    val (sendAccept, tprep, cliResolved, cliFinalized, txMsgs) = synchronized {
      val sa = shouldSendAcceptMessages
      val tp = targetedPrepare
      val cm = clientResolved
      val cf = clientFinalized
      val tm = txMessages
      shouldSendAcceptMessages = false
      clientResolved = None
      clientFinalized = None
      txMessages = None
      (sa, tp, cm, cf, tm)
    }

    if (sendAccept)
      sendAcceptMessages()

    tprep.foreach(sendPrepareMessage)
    txMsgs.foreach(messenger.sendTransactionMessages)
    cliFinalized.foreach(messenger.sendClientResponse)
    cliResolved.foreach(messenger.sendClientResponse)
  }

  def receiveTxPrepare(msg: TxPrepare): Unit = synchronized {
    proposer.updateHighestProposalId(msg.proposalId)
  }

  def receiveTxPrepareResponse(msg: TxPrepareResponse,
                               transactionCache: TransactionStatusCache): Unit = {

    synchronized {
      logger.trace(s"Received PrepareResponse ${msg.transactionId} ProposalId: ${msg.proposalId} current ProposalId ${proposer.currentProposalId} from ${msg.from}. Disposition: ${msg.disposition}")

      if (msg.proposalId != proposer.currentProposalId)
        return

      // If the response says there is a transaction collision with a transaction we know has successfully completed,
      // there's a race condition. Drop the response and re-send the prepare. Eventually it will either be successfully
      // processed or a different error will occur
      val collisionsWithCompletedTransactions = msg.collisions.nonEmpty && msg.collisions.forall { txid =>
        transactionCache.getStatus(txid) match {
          case None => false
          case Some(e) => e.status == TransactionStatus.Committed || e.status == TransactionStatus.Aborted
        }
      }

      if (collisionsWithCompletedTransactions) {
        // Race condition. Drop this message and re-send the prepare
        targetedPrepare = Some(msg.from)
        return
      }

      msg.response match {
        case Left(nack) =>
          onNack(nack.promisedId)

        case Right(promise) =>

          peerDispositions += (msg.from -> msg.disposition)

          if (isValidAcceptor(msg.from))
            proposer.receivePromise(paxos.Promise(msg.from.poolIndex, msg.proposalId, promise.lastAccepted))

          if (proposer.prepareQuorumReached && !proposer.haveLocalProposal) {

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

                ptr.storePointers.foreach(sp => peerDispositions.get(StoreId(ptr.poolId, sp.poolIndex)).foreach(disposition => {
                  nReplies += 1
                  disposition match {
                    case TransactionDisposition.VoteCommit => nCommitVotes += 1
                    case _ =>
                      // TODO: We can be quite a bit smarter about choosing when we must abort
                      nAbortVotes += 1
                  }
                }))

                if (nReplies < ptr.ida.writeThreshold) {
                  cantAttempt = true
                  break()
                }

                if (nReplies != ptr.ida.width && nCommitVotes < ptr.ida.writeThreshold && ptr.ida.width - nAbortVotes >= ptr.ida.writeThreshold) {
                  cantAttempt = true
                  break()
                }

                if (ptr.ida.width - nAbortVotes < ptr.ida.writeThreshold)
                  canCommitObject = false

                // Once canCommitTransaction flips to false, it stays there
                canCommitTransaction = canCommitTransaction && canCommitObject
              }

            if (cantAttempt)
              return

            if !proposer.haveLocalProposal then
              // If we get here, we've made our decision
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
    synchronized {
      knownResolved += msg.from
      onResolution(msg.committed, sendResolutionMessage=false)
    }

    // call outside synchronization block
    sendMessages()
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
      finalizer.foreach( _.cancel() )
      onFinalized(msg.committed)
    }

    // call outside synchronization block
    sendMessages()
  }

  def mayBeDiscarded: Boolean = synchronized { finalized }

  protected def onFinalized(committed: Boolean): Unit = {
    synchronized {
      if (!finalized) {
        finalized = true

        txd.originatingClient.foreach(client => {
          logger.trace(s"Sending TxFinalized(${txd.transactionId}) to originating client $client. Committed: $committed")
          clientFinalized = Some(TransactionFinalized(client, storeId, txd.transactionId, committed))
        })

        val messages = allDataStores.map(toStoreId => TxFinalized(toStoreId, storeId, txd.transactionId, committed))

        logger.trace(s"Sending TxFinalized(${txd.transactionId}) to ${messages.head.to.poolId}:(${messages.map(_.to.poolIndex)})")

        txMessages = Some(messages)

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

  protected def sendPrepareMessages(): Unit = {
    val (proposalId, alreadyPrepared, ofinal) = synchronized { (proposer.currentProposalId, peerDispositions, learner.finalValue) }

    // Send TxResolved if resolution has been achieved, otherwise Prepare messages
    val messages = ofinal match {
      case Some(result) => txd.allDataStores.map(TxResolved(_, storeId, txd.transactionId, result))
      case None => txd.allDataStores.filter(!alreadyPrepared.contains(_)).map(to =>
        TxPrepare(to, storeId, txd, proposalId, Nil, Nil))
    }

//    if (messages.nonEmpty)
//      logger.trace(s"Sending ${messages.head.getClass.getSimpleName}(${txd.transactionId) to ${messages.head.to.poolId}:${messages.map(_.to.poolIndex).toList}")

    messenger.sendTransactionMessages(messages.toList)
  }

  protected def sendPrepareMessage(toStoreId: StoreId): Unit = {
    val (proposalId, ofinal) = synchronized { (proposer.currentProposalId, learner.finalValue) }

    ofinal match {
      case Some(result) =>
        //logger.trace(s"Sending TxResolved(${txd.transactionId}) committed = $result to $toStoreId")
        messenger.sendTransactionMessage(TxResolved(toStoreId, storeId, txd.transactionId, result))

      case None =>
        //logger.trace(s"Sending TxPrepare(${txd.transactionId}) to $toStoreId")
        messenger.sendTransactionMessage(TxPrepare(toStoreId, storeId, txd, proposalId, Nil, Nil))
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
          TxResolved(toStoreId, storeId, txd.transactionId, committed)
        }.toList

        //logger.trace(s"Sending TxResolved(${txd.transactionId}) committed = $committed to ${messages.head.to.poolId}:(${messages.map(_.to.poolIndex)})")

        txMessages = Some(messages)

        clientResolved = txd.originatingClient.map { clientId =>
          logger.trace(s"Sending TxResolved(${txd.transactionId}) committed = $committed to originating client $clientId")
          TransactionResolved(clientId, storeId, txd.transactionId, committed)
        }
      }

    }
  }
}

