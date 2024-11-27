package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.server.network.Messenger

import scala.concurrent.Future

trait TransactionFinalizer {

  def complete: Future[Unit]

  /** Called each time an TxCommitted message is received from a new peer. The provided map contains the
    * aggregated content from all previously-received TxCommitted messages. The list of UUIDs contains the
    * object UUIDs that could not be updated as part of the transaction due to transaction requirement
    * mismatches. Mostly likely, the object copy/slices on those stores will need to be repaired.
    *
    * Finalizers are created immediately upon reaching the threshold commit requirement. Finalizers
    * that depend on knowing which peers have successfully processed the transaction (as indicated by
    * them sending Accepted messages) may delay action for some time to receive additional notifications
    * via this callback.
    */
  def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit

  /**
    * @return List of (test-class-name, is-complete)
    */
  def debugStatus: List[(String, Boolean)]

  /** Called when a TxFinalized message is received. */
  def cancel(): Unit

}

object TransactionFinalizer {
  trait Factory {
    def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer
  }
}
