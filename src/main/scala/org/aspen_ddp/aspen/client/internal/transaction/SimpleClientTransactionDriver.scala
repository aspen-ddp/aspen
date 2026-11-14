package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.ReadDriverClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionHorizon}
import scribe.Logging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

object SimpleClientTransactionDriver {

  def factory(retransmitDelay: Duration,
              horizon: FiniteDuration = TransactionHorizon.Horizon): ClientTransactionDriver.Factory = {
    def f(
           client: ReadDriverClient,
           txd: TransactionDescription,
           updateData: Map[StoreId, TransactionData]): ClientTransactionDriver =
      new SimpleClientTransactionDriver(retransmitDelay, horizon, client, txd, updateData)

    f
  }

}

/** Retransmits the prepare until the transaction resolves or is past the horizon plus `MaxStartLead` on the client's
  * clock, when it completes with `ClientOutcome.Unknown` instead: the stores no longer join the transaction, so
  * nothing more can change its outcome. A transaction already that late at `begin()` is never sent.
  */
class SimpleClientTransactionDriver(
                                     val retransmitDelay: Duration,
                                     val horizon: FiniteDuration,
                                     client: ReadDriverClient,
                                     txd: TransactionDescription,
                                     updateData: Map[StoreId, TransactionData])
    extends ClientTransactionDriver(client, txd, updateData) with Logging {

  private given ExecutionContext = client.clientContext

  private var retries = 0
  private var complete = false

  private def pastGiveUp: Boolean =
    HLCTimestamp.now.wallTime - txd.startTimestamp.wallTime > TransactionHorizon.clientGiveUp(horizon).toMillis

  private val task = client.backgroundTaskManager.schedulePeriodic(retransmitDelay) {
    val send = synchronized {
      if (complete)
        false
      else if (pastGiveUp) {
        giveUp()
        false
      } else {
        retries += 1
        if (retries % 3 == 0)
          logger.info(s"***** HUNG Client Transaction ${txd.transactionId}")
        true
      }
    }
    if (send)
      sendPrepareMessages()
  }

  private def giveUp(): Unit = {
    logger.warn(s"Client Transaction ${txd.transactionId} is past the transaction horizon: outcome unknown")
    complete(ClientOutcome.Unknown)
  }

  override def begin(): Unit =
    val late = synchronized { !complete && pastGiveUp }
    if late then
      giveUp()
    else
      super.begin()

  override def shutdown(): Unit = task.cancel()

  override protected def complete(outcome: ClientOutcome): Unit = synchronized {
    task.cancel()
    complete = true
    super.complete(outcome)
  }
}
