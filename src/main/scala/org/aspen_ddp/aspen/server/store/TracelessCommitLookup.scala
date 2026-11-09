package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

/** Looks up a store's TracelessCommits entry for a transaction. The answer is delivered on the store's executor by
  * calling Frontend.tracelessLookupComplete(transactionId, requestId, result); result is Success(found) or a Failure.
  * `requestId` identifies the lookup, so the Frontend can tell an answer read before an abort arrived from one read
  * after it.
  *
  * Stores have no client, so the Host supplies this (Host.tracelessLookup). A recovered commit vote uses it to learn
  * that its transaction committed and was forgotten while the store was down. See Frontend.requestTracelessLookup
  */
trait TracelessCommitLookup:
  def lookup(storeId: StoreId, transactionId: TransactionId, requestId: Long): Unit

object TracelessCommitLookup:
  /** For tests and stores without a host: never answers */
  val Never: TracelessCommitLookup = (_, _, _) => ()
