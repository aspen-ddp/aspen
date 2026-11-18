package org.aspen_ddp.aspen.server.transaction

import java.util.concurrent.ThreadLocalRandom
import org.aspen_ddp.aspen.common.network.{TxPrepare, TxResolved}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object SimpleTransactionDriver {

  def factory(initialDelay: Duration, maxDelay: Duration): TransactionDriver.Factory =
    new TransactionDriver.Factory:
      // A live driver re-broadcasts every maxDelay. Two intervals tolerates one late or lost
      // round before the other stores conclude the driver has failed and start their own.
      override def failedDriverDuration: Duration = maxDelay * 2

      def create(executionContext: ExecutionContext,
                 storeId: StoreId,
                 messenger: Messenger,
                 backgroundTasks: BackgroundTaskManager,
                 txd: TransactionDescription,
                 finalizerFactory: TransactionFinalizer.Factory,
                 start: TransactionDriver.Start): TransactionDriver =
        new SimpleTransactionDriver(executionContext, initialDelay, maxDelay, storeId, messenger, backgroundTasks, txd,
          finalizerFactory, start)
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
                              finalizerFactory: TransactionFinalizer.Factory,
                              start: TransactionDriver.Start = TransactionDriver.Start()) extends TransactionDriver(
  storeId, messenger, backgroundTasks, txd, finalizerFactory, start)(using executionContext) {

  private var backoffDelay = initialDelay
  private var nextTry = backgroundTasks.schedule(initialDelay) { sendPeerMessages() }

  private var sendCount = 0

  // Only the designated leader's first driver keeps the initial proposal (1, L) its client's prepare carries: any other
  // starts above round 1 and above what its own acceptor promised (TransactionDriver.Start, paxos-transactions 7.5)
  if !(designatedLeader && start.initialProposal) then nextRound()

  /** Set by shutdown: a timer already firing neither sends nor schedules again */
  private var stopped = false

  override def shutdown(): Unit = synchronized:
    logger.trace(s"**** Shutting down tx: ${txd.transactionId}")
    super.shutdown()
    stopped = true
    nextTry.cancel()

  private def sendPeerMessages(): Unit = synchronized:
    // Generally this shouldn't be called if finalized=true but a race condition between onFinalized and the next
    // call to sendMessages could do so. We need to prevent execution in this case so we don't start up the the
    // retry loop again
    if !finalized && !stopped then

      if resolved then
        logger.trace(s"*** Sending TxResolved for transaction ${txd.transactionId}")
        messenger.sendTransactionMessages(txd.allDataStores.filter(!knownResolved.contains(_)).toList
          .map(toStore => TxResolved(toStore, storeId, txd.transactionId, resolvedValue, late, proof = false)))
      else
        currentAcceptMessage() match
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
    if !stopped then
      nextTry = backgroundTasks.schedule(Duration(thisDelay, MILLISECONDS)):
        sendPeerMessages()

}
