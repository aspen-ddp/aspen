package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, ObjectState}
import org.aspen_ddp.aspen.common.network.{ReadResponse, TransactionCompletionQuery, TransactionCompletionResponse}
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.util.BackgroundTask
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

class ReadManager(val client: AspenClient, val driverFactory: ReadDriver.Factory) extends Logging {

  implicit val ec: ExecutionContext = client.clientContext

  private[this] var outstandingReads = Map[UUID, ReadDriver]()
  private[this] var completionTimes = Map[UUID, (Long, ReadDriver)]()
  private[this] var completionQueries = Map[UUID, CompletionQuery]()

  private class CompletionQuery(val pointer: ObjectPointer, val transactionUUID: UUID) {
    val queryUUID: UUID = UUID.randomUUID()
    val promise: Promise[Unit] = Promise()
    var responses: Map[StoreId, Boolean] = Map()

    private val retransmitTask = client.backgroundTasks.retryWithExponentialBackoff(tryNow = true, Duration(10, SECONDS), Duration(3, MINUTES)) {
      pointer.hostingStores.foreach { storeId =>
        client.messenger.sendClientRequest(TransactionCompletionQuery(storeId, client.clientId, queryUUID, TransactionId(transactionUUID)))
      }
      false
    }

    completionQueries += queryUUID -> this

    def update(storeId: StoreId, complete: Boolean): Unit = {
      responses += storeId -> complete
      if (responses.valuesIterator.count(b => b) >= pointer.ida.consistentRestoreThreshold) {
        completionQueries -= queryUUID
        retransmitTask.cancel()
        promise.success(())
      }
    }
  }

  /** To facilitate Opportunistic Rebuild, we'll hold on to reads after they complete until we either hear from all
    * stores or pass a fixed delay after resolving the read. This way, read responses received after the consistent
    * read threshold is achieved will still make their way to the read driver and potentially result in opportunistic
    * rebuild messages.
    */
  val pruneStaleReadsTask: BackgroundTask.ScheduledTask = client.backgroundTasks.schedulePeriodic(Duration(1, SECONDS)) {
    val completionSnap = synchronized { completionTimes }
    val now = System.nanoTime()/1000000
    val prune = completionSnap.filter( t => (now - t._2._1) > client.opportunisticRebuildManager.slowReadReplyDuration.toMillis )
    if (prune.nonEmpty) { synchronized {
      prune.foreach { t =>
        completionTimes -= t._1
        outstandingReads -= t._1
      }
    }}
  }

  def receive(m: ReadResponse): Unit = {
    synchronized { outstandingReads.get(m.readUUID) } match {
      case None => logger.warn(s"Received ReadResponse for UNKNOWN read ${m.readUUID}")

      case Some(driver) =>
        val wasCompleted = driver.readResult.isCompleted
        val allResponded = driver.receiveReadResponse(m)
        val isCompleted = driver.readResult.isCompleted

        if (isCompleted) {
          synchronized {
            if (allResponded) {
              completionTimes -= m.readUUID
              outstandingReads -= m.readUUID
            }
            else if (!wasCompleted)
              completionTimes += (m.readUUID -> (System.nanoTime()/1000000, driver))
          }
        }
    }
  }

  def receive(m: TransactionCompletionResponse): Unit = synchronized {
    completionQueries.get(m.queryUUID).foreach(_.update(m.fromStore, m.isComplete))
  }


  def shutdown(): Unit = synchronized {
    outstandingReads.foreach( t => t._2.shutdown() )
  }

  /** Creates a ReadDriver from the passed-in factory function and returns a Future to the eventual result.
    *
    */
  def read(
            objectPointer: ObjectPointer,
            comment: String,
            disableOpportunisticRebuild:Boolean=false,
            ): Future[ObjectState] = {

    val readUUID = UUID.randomUUID()

    val driver = driverFactory(client, objectPointer, readUUID, comment, disableOpportunisticRebuild)

    synchronized { outstandingReads += (readUUID -> driver) }

    driver.begin()

    driver.readResult
  }

  def getTransactionFinalized(pointer: ObjectPointer, transactionUUID: UUID): Future[Unit] = {
    val q = new CompletionQuery(pointer, transactionUUID)
    q.promise.future
  }
}
