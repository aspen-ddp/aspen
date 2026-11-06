package org.aspen_ddp.aspen.server

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS}

trait SimpleDriverRecoveryMixin extends Host {

  val checkPeriod: Duration = Duration(500, MILLISECONDS)

  private var periodicTask = backgroundTasks.schedulePeriodic(checkPeriod) { addRecoveryEvent() }

  override def shutdown()(using ec: ExecutionContext): Future[Unit] = {
    periodicTask.cancel()
    super.shutdown()
  }

  /** Runs on the control thread. Each store's transactions are walked by a task on that store's own executor. */
  override protected def handleRecoveryEvent(): Unit = {
    postToEachStore { store =>
      val now = System.nanoTime()
      store.frontend.transactions.valuesIterator.foreach { tx =>
        val delay = Duration(now - tx.lastEventTime, NANOSECONDS)
        if (delay > txDriverFactory.failedDriverDuration) {
          store.driveTransaction(tx.txd)
        }
      }
    }
  }
}
