package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.{TransactionFinalized, TransactionResolved, TxPrepare}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{Future, Promise}

object ClientTransactionDriver {
  type Factory = (AspenClient, TransactionDescription, Map[StoreId, TransactionData]) => ClientTransactionDriver

  def noErrorRecoveryFactory(
                              client: AspenClient,
                              txd: TransactionDescription,
                              updateData: Map[StoreId, TransactionData]): ClientTransactionDriver = new ClientTransactionDriver(client, txd, updateData)
}

class ClientTransactionDriver(
                               val client: AspenClient,
                               val txd: TransactionDescription,
                               val updateData: Map[StoreId, TransactionData]) extends Logging {

  //protected val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  protected val promise: Promise[Boolean] = Promise()

  def result: Future[Boolean] = promise.future

  def begin(): Unit = sendPrepareMessages()

  def shutdown(): Unit = {}

  protected def complete(committed: Boolean): Unit = synchronized {
    if (!promise.isCompleted) {
      logger.trace(s"Client Transaction Completed: ${txd.transactionId}")
      HLCTimestamp.update(txd.startTimestamp)
      promise.success(committed)
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

  def receive(finalized: TransactionFinalized): Unit = complete(finalized.committed)

  def receive(resolved: TransactionResolved): Unit = complete(resolved.committed)

  protected def sendPrepareMessages(): Unit = {
    val isCompleted = synchronized { promise.isCompleted }

    if (!isCompleted) {
      val poolId = txd.primaryObject.poolId

      val fromStore = StoreId(poolId, txd.designatedLeaderUID)

      txd.allDataStores.foreach { toStore =>

        val transactionData = updateData.getOrElse(toStore, TransactionData(Nil, Nil))

        val initialPrepare = TxPrepare(toStore, fromStore, txd, ProposalId.initialProposal(txd.designatedLeaderUID),
          transactionData.localUpdates, transactionData.preTransactionRebuilds)

        client.messenger.sendTransactionMessage(initialPrepare)
      }
    }
  }
}
