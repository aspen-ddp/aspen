package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateFinalizationAction
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.server
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.transaction.TransactionFinalizer

import scala.concurrent.{ExecutionContext, Future}

object RegisteredTransactionFinalizerFactory {

  /** Runs a committed transaction's finalization actions. Every action executes at once except the
    * MissedUpdateFinalizationAction, which executes only once every other action's `complete` has succeeded.
    * `complete` covers all of them, and `cancel` cancels all of them, including a marker not yet started.
    *
    * Invariant: a transaction's UnackedCommits entries (written by the MissedUpdateFinalizationAction) become durable
    * only after every other finalization action of the transaction completed, and its driver sends TxFinalized only
    * after all of them did. An acceptor forgets a committed transaction only on TxFinalized, or on a late commit, which
    * itself follows TxFinalized; finding its UnackedCommits entry (Tx.unackedCommitFound) applies the update but keeps
    * the Tx until then. So an acceptor that forgot the transaction implies all its finalization actions, the marker's
    * writes in every pool included, are done. A late commit (TransactionDriver.resolveLate) relies on this when it runs
    * none of them. The ordering is kept as defence in depth.
    *
    * The marker receives `updateCommitErrors` and `commitVoters` from the start, before it executes, and keeps them.
    */
  class Finalizer(val actions: List[FinalizationAction],
                  runsLast: FinalizationAction => Boolean = Finalizer.isMissedUpdateMarker)
                 (using ec: ExecutionContext) extends TransactionFinalizer {

    private val (last, first) = actions.partition(runsLast)
    private var cancelled = false

    first.foreach(_.execute())

    // The check and the execute share the lock with cancel: a cancel between them would find no marker task to
    // cancel, and the execute would then schedule its writes regardless
    Future.sequence(first.map(_.complete)).foreach { _ =>
      synchronized { if !cancelled then last.foreach(_.execute()) }
    }

    def complete: Future[Unit] = Future.sequence(actions.map(_.complete)).map(_=>())

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = {
      actions.foreach(_.updateCommitErrors(commitErrors))
    }

    override def commitVoters(voters: Option[Set[StoreId]]): Unit = actions.foreach(_.commitVoters(voters))

    override def owedStores: Set[StoreId] = actions.flatMap(_.owedStores).toSet

    def debugStatus: List[(String, Boolean)] = {
      actions.map(f => (f.getClass.getSimpleName, f.complete.isCompleted))
    }

    def cancel(): Unit =
      synchronized { cancelled = true }
      actions.foreach(_.cancel())
  }

  object Finalizer:
    def isMissedUpdateMarker(action: FinalizationAction): Boolean = action.isInstanceOf[MissedUpdateFinalizationAction]
}

class RegisteredTransactionFinalizerFactory(val client: AspenClient) extends TransactionFinalizer.Factory {
  def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = {
    val actions = txd.finalizationActions.map { sfa =>
      val f = client.typeRegistry.getType[FinalizationActionFactory](sfa.typeId.uuid).get
      f.createFinalizationAction(client, txd, sfa.data)
    }

    new server.RegisteredTransactionFinalizerFactory.Finalizer(actions)(using client.clientContext)
  }
}
