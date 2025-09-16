package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.util.BackgroundTask

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object SimpleReadDriver {
  class Factory(
                 val initialDelay: Duration,
                 val maxDelay: Duration){
    def apply(
               client: AspenClient,
               objectPointer: ObjectPointer,
               readUUID:UUID,
               comment: String,
               disableOpportunisticRebuild: Boolean): ReadDriver = {
      new SimpleReadDriver(initialDelay, maxDelay, client,
        objectPointer, readUUID, comment, disableOpportunisticRebuild)
    }
  }
}

/** This class provides a *very* simple exponential backoff retry mechanism for reads in that completes when either the object
  *  is successfully read or a fatal error is encountered.
  *
  */
class SimpleReadDriver(
                        val initialDelay: Duration,
                        val maxDelay: Duration,
                        client: AspenClient,
                        objectPointer: ObjectPointer,
                        readUUID:UUID,
                        comment: String,
                        disableOpportunisticRebuild: Boolean) extends BaseReadDriver( client,
  objectPointer, readUUID, comment, disableOpportunisticRebuild) {

  given ec: ExecutionContext = client.clientContext

  private[this] var task: Option[BackgroundTask.ScheduledTask] = None

  override def onComplete(): Unit = synchronized {
    logger.trace(s"READ UUID $readUUID: COMPLETE. Cancelling future retransmits")
    task.foreach(_.cancel())
  }

  override def begin(): Unit = synchronized {
    task = Some(client.backgroundTasks.retryWithExponentialBackoff(tryNow=false, initialDelay=initialDelay, maxDelay=maxDelay) {
      super.begin()
      false
    })
    super.begin()
  }

  override def shutdown(): Unit = task.foreach( t => t.cancel() )
}
