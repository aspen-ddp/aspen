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

  //protected val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  protected val promise: Promise[ClientOutcome] = Promise()

  def result: Future[ClientOutcome] = promise.future

  def begin(): Unit = sendPrepareMessages()

  def shutdown(): Unit = {}

  protected def complete(outcome: ClientOutcome): Unit = synchronized {
    if (!promise.isCompleted) {
      logger.trace(s"Client Transaction Completed: ${txd.transactionId}")
      HLCTimestamp.update(txd.startTimestamp)
      promise.success(outcome)
    }
  }

  /*
  def receive(acceptResponse: TxAcceptResponse): Unit = synchronized {
    if (promise.isCompleted)
      return

    acceptResponse.response match {
      case Left(nack) => // Nothing to do
      case Right(accepted) =>
        learner.receiveAccepted(paxos.Accepted(acceptResponse.from.poolIndex, acceptResponse.proposalId, accepted.value)) match {
          case None =>
          case Some(committed) => complete(committed)
        }
    }
  }
  */

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
