package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

import scala.util.Try

/** Looks up a store's TracelessCommits entry for a transaction, and gives `answer` the result: Success(found) or a
  * Failure. `answer` may be called from any thread. It belongs to the Frontend that made the lookup: it posts the
  * result to that Frontend's store executor (Frontend.post), and a store replaced since drops it, so an answer read for
  * a Frontend gone by then is never taken by its successor. `requestId` identifies the lookup within its Frontend.
  *
  * Stores have no client, so the Host supplies this (Host.tracelessLookup). A recovered commit vote uses it to learn
  * that its transaction committed and was forgotten while the store was down. See Frontend.requestTracelessLookup
  */
trait TracelessCommitLookup:
  def lookup(storeId: StoreId, transactionId: TransactionId, requestId: Long, answer: Try[Boolean] => Unit): Unit

object TracelessCommitLookup:
  /** For tests and stores without a host: never answers */
  val Never: TracelessCommitLookup = (_, _, _, _) => ()
