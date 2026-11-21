package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.crl.{StoreRecoveryState, TransactionRecoveryState}

/** The requests a SimpleCRL's callers, its writer, its growth thread and its completion thread queue for its CRL
  * thread. SimpleCRL re-exports them */
object CrlRequests:

  sealed abstract class ActionRequest()

  case class SaveTransaction(txid: TransactionId,
                             state: TransactionRecoveryState,
                             completionHandler: () => Unit) extends ActionRequest

  case class DropTransactionData(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransaction(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransactions(storeId: StoreId, transactionIds: List[TransactionId],
                                completionHandler: () => Unit) extends ActionRequest

  case class SaveAppliedStub(storeId: StoreId, transactionId: TransactionId, start: HLCTimestamp,
                             completionHandler: () => Unit) extends ActionRequest

  case class DeleteAppliedStub(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  /** Drops every entry of the store held when it is processed (CrashRecoveryLog.dropStore) */
  case class DropStore(storeId: StoreId, completionHandler: () => Unit) extends ActionRequest

  case class WriteComplete(serialNumber: Long) extends ActionRequest

  /** Queued when the writer thread dies: the CRL thread dies of it */
  case class WriterDied(cause: Throwable) extends ActionRequest

  /** Queued when a completion handler throws on the completion thread: the CRL thread dies of it, as of a writer's
    * death (fail-stop) */
  case class HandlerFailed(cause: Throwable) extends ActionRequest

  case class Shutdown(completionHandler: () => Unit) extends ActionRequest

  case class GetRecoveryState(storeId: StoreId,
                              completion: StoreRecoveryState => Unit
                             ) extends ActionRequest

  case class GetStoresWithEntries(completion: Set[StoreId] => Unit) extends ActionRequest

  /** Runs `f` on the CRL thread, in order with the other requests: for tests that hold the thread */
  case class RunOnCrlThread(f: () => Unit) extends ActionRequest

  /** Runs the handler once every request queued before it is durable (CrashRecoveryLog.whenDurable) */
  case class Barrier(handler: () => Unit) extends ActionRequest

  /** Queued by the growth thread once stream `streamNumber` is preallocated, renamed into place and its directory
    * forced: the CRL thread commits it into the ring */
  case class GrowthReady(streamNumber: Int) extends ActionRequest
