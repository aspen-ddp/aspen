package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.ReadDriverClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.{TransactionFinalized, TransactionResolved, TxPrepare}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
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

  def result: Future[ClientOutcome] = promise.future

  def begin(): Unit = sendPrepareMessages()

  def shutdown(): Unit = {}

  /** An outcome opposite to the one already reached is a split decision (paxos F11) and is logged as an error. Only
    * duplicates that race the first outcome can reach here: TransactionManager forgets the driver once it has one, so
    * the stores and their drivers are the detectors
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
  }

  def receive(finalized: TransactionFinalized): Unit = complete(ClientOutcome(finalized.committed))

  def receive(resolved: TransactionResolved): Unit = complete(ClientOutcome(resolved.committed))

  protected def sendPrepareMessages(): Unit = {
    val isCompleted = synchronized { promise.isCompleted }

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
