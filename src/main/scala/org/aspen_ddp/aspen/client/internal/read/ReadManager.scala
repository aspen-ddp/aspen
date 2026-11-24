package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{ObjectState, ReadDriverClient}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.{ReadResponse, TransactionCompletionQuery, TransactionCompletionResponse}
import org.aspen_ddp.aspen.common.objects.{FullObject, ObjectPointer, ReadError, ReadType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import scribe.Logging

import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

class ReadManager(val client: ReadDriverClient, val driverFactory: ReadDriver.Factory) extends Logging {

  given ExecutionContext = client.clientContext

  private var outstandingReads = Map[UUID, ReadDriver]()
  private var completionQueries = Map[UUID, CompletionQuery]()

  private class CompletionQuery(val pointer: ObjectPointer, val ida: IDA, val transactionUUID: UUID) {
    val queryUUID: UUID = UUID.randomUUID()
    val promise: Promise[Unit] = Promise()
    var responses: Map[StoreId, Boolean] = Map()

    private val retransmitTask = client.backgroundTaskManager.retryWithExponentialBackoff(tryNow = true, Duration(10, SECONDS), Duration(3, MINUTES)) {
      (0 until ida.width).foreach { i =>
        client.messenger.sendClientRequest(TransactionCompletionQuery(StoreId(pointer.poolId, i.toByte), client.clientId, queryUUID, TransactionId(transactionUUID)))
      }
      false
    }

    completionQueries += queryUUID -> this

    def update(storeId: StoreId, complete: Boolean): Unit = {
      responses += storeId -> complete
      if (responses.valuesIterator.count(b => b) >= ida.consistentRestoreThreshold) {
        completionQueries -= queryUUID
        retransmitTask.cancel()
        promise.success(())
      }
    }
  }

  /** Network plugins may deliver messages on their I/O threads (QUIC runs handlers on a Netty event loop).
    * Decoding store responses and restoring the object is O(object size) work, so it is handed off to the
    * client's ExecutionContext rather than stalling every other connection sharing that thread.
    */
  def receive(m: ReadResponse): Unit = {
    synchronized { outstandingReads.get(m.readUUID) } match {
      case None =>
        // A read is released as soon as it completes, so a response from a slower store lands here. Nothing is
        // owed to the read, but a store that no longer lives at the cached endpoint must still be forgotten
        logger.trace(s"Received ReadResponse for completed or unknown read ${m.readUUID}")
        if m.result == Left(ReadError.StoreNotFound) then
          client.messenger.dropCacheForStore(m.fromStore)

      case Some(driver) => summon[ExecutionContext].execute(() => deliver(driver, m))
    }
  }

  private def deliver(driver: ReadDriver, m: ReadResponse): Unit = {
    // Every store having replied once does not decide the read: replies that cannot be restored leave it alive
    // to be re-sent, and its later responses must still reach the driver
    driver.receiveReadResponse(m)

    if (driver.readResult.isCompleted)
      synchronized { outstandingReads -= m.readUUID }
  }

  private[internal] def outstandingReadCount: Int = synchronized { outstandingReads.size }

  def receive(m: TransactionCompletionResponse): Unit = synchronized {
    completionQueries.get(m.queryUUID).foreach(_.update(m.fromStore, m.isComplete))
  }


  def shutdown(): Unit = synchronized {
    outstandingReads.foreach( t => t._2.shutdown() )
  }

  /** Creates a ReadDriver from the passed-in factory function and returns a Future to the eventual result.
    *
    * A finite `timeout` bounds the read: once it expires, an incomplete read fails with ReadTimeout, its driver
    * stops (ReadDriver.timeOut) and the read is released. An infinite one, the default, schedules nothing: the read
    * retries until it completes.
    */
  def read(
            objectPointer: ObjectPointer,
            ida: IDA,
            comment: String,
            readType: ReadType = FullObject(),
            timeout: Duration = Duration.Inf
            ): Future[ObjectState] = {

    val readUUID = UUID.randomUUID()

    val driver = driverFactory(client, objectPointer, ida, readUUID, comment, readType)

    synchronized { outstandingReads += (readUUID -> driver) }

    driver.begin()

    if timeout.isFinite then
      // The timer captures only the read's id: a cancelled task stays queued in BackgroundTaskManager's scheduler
      // until its delay expires, and must not keep a completed read's slices and object reachable until then
      val timer = client.backgroundTaskManager.schedule(timeout):
        synchronized { outstandingReads.get(readUUID) }.foreach(_.timeOut())
        synchronized { outstandingReads -= readUUID }
      driver.readResult.onComplete(_ => timer.cancel())(using ExecutionContext.parasitic)

    driver.readResult
  }

  def getTransactionFinalized(pointer: ObjectPointer, ida: IDA, transactionUUID: UUID): Future[Unit] = {
    val q = new CompletionQuery(pointer, ida, transactionUUID)
    q.promise.future
  }
}
