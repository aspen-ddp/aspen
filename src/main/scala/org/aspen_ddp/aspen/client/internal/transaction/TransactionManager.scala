package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.network.{TransactionFinalized, TransactionResolved, TransactionVote}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}

import scala.concurrent.{ExecutionContext, Future}

class TransactionManager(val client: AspenClient,
                         defaultDriverFactory: ClientTransactionDriver.Factory) {

  private var transactions = Map[TransactionId, ClientTransactionDriver]()

  /** The transactions whose drivers have not concluded */
  private[transaction] def pending: Set[TransactionId] = synchronized { transactions.keySet }

  def shutdown(): Unit = synchronized {
    transactions.values.foreach(_.shutdown())
  }

  def runTransaction(
                      txd: TransactionDescription,
                      updateData: Map[StoreId, TransactionData],
                      driverFactory: Option[ClientTransactionDriver.Factory]): Future[ClientOutcome] = {
    val td = driverFactory.getOrElse(defaultDriverFactory)(client, txd, updateData)

    synchronized {
      transactions += (txd.transactionId -> td)
    }

    // Forgotten when the driver needs no more messages: the leader's outcome (receive), or Unknown once past the
    // horizon, which no message would ever remove. A commit learned from votes keeps the driver for the leader's reply
    td.stopped.onComplete(_ => forget(txd.transactionId, td))(using ExecutionContext.parasitic)

    td.begin()

    td.result
  }

  private def forget(txid: TransactionId, td: ClientTransactionDriver): Unit = synchronized {
    if (transactions.get(txid).exists(_ eq td))
      transactions -= txid
  }

  def receive(resolved: TransactionResolved): Unit = {

    val otd = synchronized { transactions.get(resolved.transactionId) }
    otd.foreach { td =>
      td.receive(resolved)
      synchronized { transactions -= resolved.transactionId }
    }
  }
  /** Delivers to the registered driver. Commit votes may complete its result, but the driver is kept until it is
    * `stopped`, so the leader's outcome still reaches it */
  def receive(vote: TransactionVote): Unit = {
    val otd = synchronized { transactions.get(vote.transactionId) }
    otd.foreach(_.receive(vote))
  }

  def receive(finalized: TransactionFinalized): Unit = {
    val otd = synchronized { transactions.get(finalized.transactionId) }
    otd.foreach { td =>
      td.receive(finalized)
      synchronized { transactions -= finalized.transactionId }
    }
  }
}
