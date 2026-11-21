package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger
import scribe.Logging

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Runs a committed transaction's finalization actions for its `TransactionDriver`, starting them again from scratch
  * after a backoff whenever a finalizer cannot be created or fails.
  *
  * Finalization has a failure path (paxos-transactions 7.7): a finalizer that cannot be created, or whose
  * `complete` fails (an action gave up, such as a missed-update marker write failing with a
  * TransactionCreationError), is logged at error and started again after a backoff
  * (`TransactionDriver.finalizerRetryDelay`), until one completes, the driver is finalized another way, or it shuts
  * down. The actions are idempotent and at-least-once, so re-running them is what a recovery driver would do. The
  * transaction is never finalized without its actions: TxFinalized lets the stores forget it, and without the
  * marker's ErrorTree marks and UnackedCommits entries a store that missed it would stay stale, unrepaired. A failure
  * that repeats forever is a stall, logged on every attempt, never a lost mark.
  *
  * Every method and completion callback runs under `lock`, the owning driver's monitor, so the runner's state shares
  * the driver's single lock.
  *
  * @param lock        the driver, whose monitor guards this runner
  * @param isFinalized whether the driver finalized, which ends all restarts
  * @param configure   gives a new finalizer the driver's current votes and commit errors
  * @param onComplete  called when a finalizer completed every action, with every store a finalizer of this driver
  *                    handed an UnackedCommits entry off for
  */
private[transaction] class FinalizerRunner(lock: AnyRef,
                                           storeId: StoreId,
                                           txd: TransactionDescription,
                                           factory: TransactionFinalizer.Factory,
                                           messenger: Messenger,
                                           backgroundTasks: BackgroundTaskManager,
                                           isFinalized: () => Boolean,
                                           configure: TransactionFinalizer => Unit,
                                           onComplete: Set[StoreId] => Unit)
                                          (using ec: ExecutionContext) extends Logging:

  private var current: Option[TransactionFinalizer] = None

  /** The stores the hand-offs of this driver's failed finalizers planned entries for. A failed marker may have written
    * some pools' entries before another pool's write gave up, so TxFinalized lists them too
    */
  private var failedHandedOff: Set[StoreId] = Set()

  /** Failed finalizers so far, for the restart backoff */
  private var failures = 0

  /** The last failed finalizer's error, for the stall report */
  private var lastFailure: Option[Throwable] = None

  private var restart: BackgroundTaskManager.ScheduledTask = BackgroundTaskManager.NoTask

  /** Set by `shutdown`: no finalizer is started again */
  private var stopped = false

  def failureCount: Int = lock.synchronized { failures }

  def lastFailureCause: Option[Throwable] = lock.synchronized { lastFailure }

  def debugStatus: Option[List[(String, Boolean)]] = lock.synchronized { current.map(_.debugStatus) }

  /** Every store a finalizer of this driver, failed or running, handed off an UnackedCommits entry for */
  def handedOffStores: Set[StoreId] = lock.synchronized {
    failedHandedOff ++ current.fold(Set[StoreId]())(_.handedOffStores)
  }

  /** Passes commit errors reported after the finalizer started on to it */
  def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]], keptForReplay: Set[StoreId]): Unit =
    lock.synchronized { current.foreach(_.updateCommitErrors(commitErrors, keptForReplay)) }

  /** Cancels the running finalizer, if any. A marker not yet handed off then writes nothing */
  def cancel(): Unit = lock.synchronized { current.foreach(_.cancel()) }

  /** Stops a pending restart and prevents any further one */
  def shutdown(): Unit = lock.synchronized {
    stopped = true
    restart.cancel()
  }

  /** Creates and starts a finalizer */
  def start(): Unit = lock.synchronized {
    logger.trace(s"Running finalization actions for transaction ${txd.transactionId}")
    val created =
      try Right(factory.create(txd, messenger))
      catch case NonFatal(err) => Left(err)

    created match
      case Left(err) =>
        current = None
        failed(None, err)

      case Right(f) =>
        configure(f)

        current = Some(f)

        f.complete.onComplete {
          case Success(_) =>
            logger.trace(s"Finalization actions completed for transaction ${txd.transactionId}")
            onComplete(handedOffStores)
          case Failure(err) =>
            lock.synchronized { failed(Some(f), err) }
        }
  }

  /** Schedules a fresh finalizer unless this one was replaced, or the driver finalized or stopped */
  private def failed(f: Option[TransactionFinalizer], err: Throwable): Unit =
    if !isFinalized() && !stopped && current == f then
      f.foreach: f =>
        failedHandedOff ++= f.handedOffStores
        f.cancel()
      current = None
      failures += 1
      lastFailure = Some(err)
      val delay = TransactionDriver.finalizerRetryDelay(failures)
      logger.error(s"Tx ${txd.transactionId} on store $storeId: finalization failed (attempt $failures), " +
        s"starting it again in $delay: $err")
      restart = backgroundTasks.schedule(delay) {
        lock.synchronized { if !isFinalized() && !stopped && current.isEmpty then start() }
      }
