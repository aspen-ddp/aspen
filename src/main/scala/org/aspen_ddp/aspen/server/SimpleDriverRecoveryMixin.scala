package org.aspen_ddp.aspen.server

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS}

trait SimpleDriverRecoveryMixin extends StoreManager {

  val checkPeriod: Duration = Duration(500, MILLISECONDS)

  private var periodicTask = backgroundTasks.schedulePeriodic(checkPeriod) { addRecoveryEvent() }

  override def shutdown()(using ec: ExecutionContext): Future[Unit] = {
    periodicTask.cancel()
    super.shutdown()
  }

  override protected def handleRecoveryEvent(): Unit = {
    val now = System.nanoTime()
    stores.valuesIterator.foreach { store =>
      store.frontend.transactions.valuesIterator.foreach { tx =>
        val delay = Duration(now - tx.lastEventTime, NANOSECONDS)
        if (delay > txDriverFactory.failedDriverDuration) {
          store.driveTransaction(tx.txd)
        }
      }
    }
  }
}
