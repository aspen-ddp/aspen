package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, CurrentAllocationsReply, CurrentAllocationsRequest}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

object CurrentAllocationsQuery:
  /** The query was given up because its caller's `stopWhen` held */
  final class Abandoned extends Exception("CurrentAllocations query abandoned")

/** Asks `stores` for the allocations and votes they hold (`CurrentAllocationsRequest`), resending to the stores that
  * have not replied until `needed` of them have. Completes with each replying store's reply, or fails with
  * `Abandoned` at the first resend after `stopWhen` holds. A store still loading its CRL, or not hosted anywhere,
  * does not reply and so never counts toward `needed`.
  */
private[client] final class CurrentAllocationsQuery(clientId: ClientId,
                                                    send: ClientRequest => Unit,
                                                    tasks: BackgroundTaskManager,
                                                    stores: List[StoreId],
                                                    needed: Int,
                                                    stopWhen: () => Boolean,
                                                    onDone: UUID => Unit):
  require(needed <= stores.size, s"needs $needed replies from ${stores.size} stores")

  val queryUUID: UUID = UUID.randomUUID()

  private val promise: Promise[Map[StoreId, CurrentAllocationsReply]] = Promise()
  private var replies: Map[StoreId, CurrentAllocationsReply] = Map()

  def result: Future[Map[StoreId, CurrentAllocationsReply]] = promise.future

  private val resend = tasks.retryWithExponentialBackoff(tryNow = true, Duration(1, SECONDS), Duration(10, SECONDS)):
    if promise.isCompleted then
      true
    else if stopWhen() then
      onDone(queryUUID)
      promise.tryFailure(new CurrentAllocationsQuery.Abandoned)
      true
    else
      val waiting = synchronized(stores.filterNot(replies.contains))
      waiting.foreach(s => send(CurrentAllocationsRequest(s, clientId, queryUUID)))
      false

  def receive(m: CurrentAllocationsReply): Unit =
    val done = synchronized:
      if stores.contains(m.fromStore) && !promise.isCompleted then
        replies += m.fromStore -> m
        if replies.size >= needed then Some(replies) else None
      else
        None
    done.foreach: r =>
      resend.cancel()
      onDone(queryUUID)
      promise.trySuccess(r)
