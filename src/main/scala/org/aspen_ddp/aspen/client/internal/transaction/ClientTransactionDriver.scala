package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.ReadDriverClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.{TransactionFinalized, TransactionResolved, TransactionVote, TxPrepare}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionDisposition}
import scribe.Logging

import scala.concurrent.{Future, Promise}

/** How a client transaction driver concluded. Unknown: the transaction is past the horizon without a resolution, so it
  * may have committed or aborted and the client can no longer find out */
enum ClientOutcome:
  case Committed, Aborted, Unknown

object ClientOutcome:
  def apply(committed: Boolean): ClientOutcome = if committed then Committed else Aborted

object ClientTransactionDriver {
  type Factory = (ReadDriverClient, TransactionDescription, Map[StoreId, TransactionData]) => ClientTransactionDriver

  def noErrorRecoveryFactory(
                              client: ReadDriverClient,
                              txd: TransactionDescription,
                              updateData: Map[StoreId, TransactionData]): ClientTransactionDriver = new ClientTransactionDriver(client, txd, updateData)
}

class ClientTransactionDriver(
                               val client: ReadDriverClient,
                               val txd: TransactionDescription,
                               val updateData: Map[StoreId, TransactionData]) extends Logging {

  protected val promise: Promise[ClientOutcome] = Promise()

  private val stoppedPromise: Promise[Unit] = Promise()
  private var votes = Map[StoreId, TransactionDisposition.Value]()

  def result: Future[ClientOutcome] = promise.future

  /** Completes when the driver needs no more messages: the leader's TransactionResolved/TransactionFinalized arrived,
    * or it reached Unknown. A completion from votes alone does not complete it when the driver retransmits. This
    * driver does not retransmit, so it stops together with `result` */
  def stopped: Future[Unit] = stoppedPromise.future

  /** True when the driver keeps sending after `result` completes from votes (until `stop()`) */
  protected def retransmits: Boolean = false

  protected def stop(): Unit = synchronized { stoppedPromise.trySuccess(()) }

  def begin(): Unit = sendPrepareMessages()

  def shutdown(): Unit = {}

  /** An outcome opposite to the one already reached is a split decision (paxos F11) and is logged as an error.
    * TransactionManager keeps a driver until it is `stopped`, so a leader's abort after a completion from commit votes
    * reaches here and is logged; once stopped by the leader's outcome the driver is forgotten, and a later conflicting
    * outcome is left to the stores and their drivers to detect
    */
  protected def complete(outcome: ClientOutcome): Unit = synchronized {
    promise.future.value.flatMap(_.toOption).foreach { first =>
      if (first != outcome && first != ClientOutcome.Unknown && outcome != ClientOutcome.Unknown)
        logger.error(s"Split decision: transaction ${txd.transactionId} concluded $first, then reported $outcome")
    }
    if (!promise.isCompleted) {
      logger.trace(s"Client Transaction Completed: ${txd.transactionId}")
      HLCTimestamp.update(txd.startTimestamp)
      promise.success(outcome)
    }
    if (!retransmits)
      stop()
  }

  def receive(finalized: TransactionFinalized): Unit = synchronized {
    complete(ClientOutcome(finalized.committed))
    stop()
  }

  def receive(resolved: TransactionResolved): Unit = synchronized {
    complete(ClientOutcome(resolved.committed))
    stop()
  }

  /** Records the store's first vote. Commit is final once every object has `writeThreshold` commit votes; abort votes
    * never complete the driver (abort comes only from the leader's TransactionResolved/TransactionFinalized) */
  def receive(vote: TransactionVote): Unit = synchronized {
    if (vote.disposition != TransactionDisposition.Undetermined)
      votes.get(vote.fromStore) match
        case Some(first) =>
          if (first != vote.disposition)
            logger.error(s"Store ${vote.fromStore} voted $first, then ${vote.disposition} on transaction " +
              s"${txd.transactionId}; ignoring the later vote")
        case None =>
          votes += vote.fromStore -> vote.disposition
          if (commitCertain)
            complete(ClientOutcome.Committed)
  }

  /** Every referenced object has at least its pool's writeThreshold commit votes */
  protected def commitCertain: Boolean = synchronized {
    txd.allReferencedObjectsSet.forall { ptr =>
      val ida = txd.poolIDAMap(ptr.poolId)
      val commits = (0 until ida.width).count { i =>
        votes.get(StoreId(ptr.poolId, i.toByte)).contains(TransactionDisposition.VoteCommit)
      }
      commits >= ida.writeThreshold
    }
  }

  protected def sendPrepareMessages(): Unit = {
    val isCompleted = stopped.isCompleted

    if (!isCompleted) {
      val poolId = txd.primaryObject.poolId

      val fromStore = StoreId(poolId, txd.designatedLeaderUID)

      txd.allDataStores.foreach { toStore =>

        val transactionData = updateData.getOrElse(toStore, TransactionData(Nil))

        val initialPrepare = TxPrepare(toStore, fromStore, txd, ProposalId.initialProposal(txd.designatedLeaderUID),
          transactionData.localUpdates, fromClient = true)

        client.messenger.sendTransactionMessage(initialPrepare)
      }
    }
  }
}
