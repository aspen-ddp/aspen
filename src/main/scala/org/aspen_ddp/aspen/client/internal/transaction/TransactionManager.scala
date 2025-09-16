package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.network.{TransactionFinalized, TransactionResolved}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}

import scala.concurrent.Future

class TransactionManager(val client: AspenClient,
                         defaultDriverFactory: ClientTransactionDriver.Factory) {

  private var transactions = Map[TransactionId, ClientTransactionDriver]()

  def shutdown(): Unit = synchronized {
    transactions.values.foreach(_.shutdown())
  }

  def runTransaction(
                      txd: TransactionDescription,
                      updateData: Map[StoreId, TransactionData],
                      driverFactory: Option[ClientTransactionDriver.Factory]): Future[Boolean] = {
    val td = driverFactory.getOrElse(defaultDriverFactory)(client, txd, updateData)

    synchronized {
      transactions += (txd.transactionId -> td)
    }

    td.begin()

    td.result
  }

  def receive(resolved: TransactionResolved): Unit = {

    val otd = synchronized { transactions.get(resolved.transactionId) }
    otd.foreach { td =>
      td.receive(resolved)
      synchronized { transactions -= resolved.transactionId }
    }
  }
  def receive(finalized: TransactionFinalized): Unit = {
    val otd = synchronized { transactions.get(finalized.transactionId) }
    otd.foreach { td =>
      td.receive(finalized)
      synchronized { transactions -= finalized.transactionId }
    }
  }
}
