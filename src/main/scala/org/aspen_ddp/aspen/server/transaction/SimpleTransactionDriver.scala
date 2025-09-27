package org.aspen_ddp.aspen.server.transaction

import java.util.concurrent.ThreadLocalRandom
import org.aspen_ddp.aspen.common.network.{TxPrepare, TxResolved}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger
import org.apache.logging.log4j.scala.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object SimpleTransactionDriver {

  def factory(initialDelay: Duration, maxDelay: Duration): TransactionDriver.Factory =
    new TransactionDriver.Factory:
      def create(executionContext: ExecutionContext,
                 storeId: StoreId,
                 messenger: Messenger,
                 backgroundTasks: BackgroundTaskManager,
                 txd: TransactionDescription,
                 finalizerFactory: TransactionFinalizer.Factory): TransactionDriver =
        new SimpleTransactionDriver(executionContext, initialDelay, maxDelay, storeId, messenger, backgroundTasks, txd, finalizerFactory)
}

/** Provides a store-side transaction driver with a very simple retransmit strategy and exponential backoff mechanism
  *  for dealing with Paxos contention.
  */
class SimpleTransactionDriver(executionContext: ExecutionContext,
                              val initialDelay: Duration,
                              val maxDelay: Duration,
                              storeId: StoreId,
                              messenger: Messenger,
                              backgroundTasks: BackgroundTaskManager,
                              txd: TransactionDescription,
                              finalizerFactory: TransactionFinalizer.Factory) extends TransactionDriver(
  storeId, messenger, backgroundTasks, txd, finalizerFactory)(using executionContext) {

  private var backoffDelay = initialDelay
  private var nextTry = backgroundTasks.schedule(initialDelay) { sendPeerMessages() }

  private var sendCount = 0

  if !designatedLeader then nextRound()

  override def shutdown(): Unit = synchronized:
    logger.trace(s"**** Shutting down tx: ${txd.transactionId}")
    nextTry.cancel()

  private def sendPeerMessages(): Unit = synchronized:
    // Generally this shouldn't be called if finalized=true but a race condition between onFinalized and the next
    // call to sendMessages could do so. We need to prevent execution in this case so we don't start up the the
    // retry loop again
    if !finalized then

      if resolved then
        logger.trace(s"*** Sending TxResolved for transaction ${txd.transactionId}")
        txd.allDataStores.filter(!knownResolved.contains(_)).map(toStore => TxResolved(toStore, storeId, txd.transactionId, resolvedValue))
      else
        proposer.currentAcceptMessage() match
          case Some(_) =>
            // If the stores detect resolution before we do, they can discard their transactions. Subsequent accept messages
            // we send will be silently ignored so we'll never receive responses from them. To work around this, we'll
            // periodically send prepares before the accepts. If they had forgotten the Tx, they'll restart it
            if sendCount % 3 == 0 then
              sendPrepareMessages()

            sendAcceptMessages()
          case None =>
            txd.allDataStores.foreach(sendPrepareMessage)
      sendCount += 1

      if sendCount % 5 == 0 then
        printState(s => logger.debug(s))

      // Continually re-broadcast the prepare/accept messages for our current proposal at a fixed rate
      // if we get interrupted, the backoff mechanism will protect against contention
      nextTry.cancel()
      nextTry = backgroundTasks.schedule(maxDelay):
        sendPeerMessages()

  override protected def nextRound(): Unit = synchronized:
    super.nextRound()

    backoffDelay = backoffDelay * 2

    if backoffDelay > maxDelay then
      backoffDelay = maxDelay

    val thisDelay = ThreadLocalRandom.current().nextInt(0, backoffDelay.toMillis.toInt)

    nextTry.cancel()
    nextTry = backgroundTasks.schedule(Duration(thisDelay, MILLISECONDS)):
      sendPeerMessages()

}
