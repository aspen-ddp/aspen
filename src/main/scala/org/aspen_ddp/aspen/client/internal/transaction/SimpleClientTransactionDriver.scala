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
              horizon: FiniteDuration = TransactionHorizon.Horizon,
              nowMillis: () => Long = () => HLCTimestamp.now.wallTime): ClientTransactionDriver.Factory = {
    def f(
           client: ReadDriverClient,
           txd: TransactionDescription,
           updateData: Map[StoreId, TransactionData]): ClientTransactionDriver =
      new SimpleClientTransactionDriver(retransmitDelay, horizon, nowMillis, client, txd, updateData)

    f
  }

}

/** Retransmits the prepare until the leader's outcome arrives (a completion from the stores' votes does not stop it) or is past the horizon plus `MaxStartLead` on the client's
  * clock, when it completes with `ClientOutcome.Unknown` instead: the stores no longer join the transaction, so
  * nothing more can change its outcome. A transaction already that late at `begin()` is never sent.
  */
class SimpleClientTransactionDriver(
                                     val retransmitDelay: Duration,
                                     val horizon: FiniteDuration,
                                     nowMillis: () => Long,
                                     client: ReadDriverClient,
                                     txd: TransactionDescription,
                                     updateData: Map[StoreId, TransactionData])
    extends ClientTransactionDriver(client, txd, updateData) with Logging {

  private given ExecutionContext = client.clientContext

  private var retries = 0

  private def pastGiveUp: Boolean =
    nowMillis() - txd.startTimestamp.wallTime > TransactionHorizon.clientGiveUp(horizon).toMillis

  /** One retransmit tick: gives up if past the horizon, else resends. Run by the periodic task; tests call it directly */
  private[transaction] def tick(): Unit = {
    val send = synchronized {
      if (stopped.isCompleted)
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

  private val task = client.backgroundTaskManager.schedulePeriodic(retransmitDelay)(tick())

  private def giveUp(): Unit = {
    logger.warn(s"Client Transaction ${txd.transactionId} is past the transaction horizon: outcome unknown")
    complete(ClientOutcome.Unknown)
    stop()
  }

  override def begin(): Unit =
    val late = synchronized { !stopped.isCompleted && pastGiveUp }
    if late then
      giveUp()
    else
      super.begin()

  override def shutdown(): Unit = task.cancel()

  override protected def retransmits: Boolean = true

  /** A completion from votes leaves the task running: the leader and the stores still need the prepare */
  override protected def stop(): Unit = synchronized {
    task.cancel()
    super.stop()
  }
}
