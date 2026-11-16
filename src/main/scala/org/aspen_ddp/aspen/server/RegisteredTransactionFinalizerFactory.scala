package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateFinalizationAction
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.server
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.transaction.TransactionFinalizer

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object RegisteredTransactionFinalizerFactory {

  /** Runs a committed transaction's finalization actions. Every action executes at once except the
    * MissedUpdateFinalizationAction (the marker), which executes once every other action's `complete` has settled,
    * succeeded or failed (Ruling 8, revised 2026-11-15). `complete` covers all of them, and `cancel` cancels all of
    * them, including a marker not yet started.
    *
    * Invariant: a durable UnackedCommits entry implies the transaction committed, and nothing more: the other actions
    * may have failed, to be run again by a fresh finalizer. What implies that every action is done is an acceptor
    * having forgotten the transaction. A driver sends TxFinalized only once a finalizer's `complete` has succeeded, so
    * once every action, the marker's writes in every pool included, is done. An acceptor forgets a committed
    * transaction only on TxFinalized, or on a late outcome, which itself follows TxFinalized; finding its UnackedCommits
    * entry (Tx.unackedCommitFound) applies the update but keeps the Tx until then, and an expiry Discard needs a quorum
    * that forgot it. A late commit (TransactionDriver.resolveLate) relies on this when it runs none of the actions.
    *
    * Running the marker after a failed action writes the ErrorTree marks and UnackedCommits entries while the failing
    * action is retried, so the stores that missed the transaction are repaired during a stall (transaction-recovery
    * R-N2). Each retry runs the marker again; its writes are idempotent.
    *
    * The marker receives `updateCommitErrors` and `commitVoters` from the start, before it executes, and keeps them.
    *
    * `complete` settles once every action that executes has settled: it fails with the first failure, or succeeds if
    * none failed. It relies on every action settling (an action that gives up fails its `complete`, never leaves it
    * pending); failing earlier would let the driver cancel the marker before its turn. The driver then starts a fresh
    * finalizer (TransactionDriver.startFinalizer).
    */
  class Finalizer(val actions: List[FinalizationAction],
                  runsLast: FinalizationAction => Boolean = Finalizer.isMissedUpdateMarker)
                 (using ec: ExecutionContext) extends TransactionFinalizer {

    private val (last, first) = actions.partition(runsLast)
    private var cancelled = false

    first.foreach(_.execute())

    private def settled(fs: List[FinalizationAction]): Future[List[Try[Unit]]] =
      Future.sequence(fs.map(_.complete.transform(t => Success(t))))

    private val completion: Promise[Unit] = Promise()

    // The check and the execute share the lock with cancel: a cancel between them would find no marker task to
    // cancel, and the execute would then schedule its writes regardless
    settled(first).foreach { firstResults =>
      val runLast = synchronized {
        if !cancelled then last.foreach(_.execute())
        !cancelled
      }
      val lastResults = if runLast then settled(last) else Future.successful(Nil)
      lastResults.foreach { results =>
        (firstResults ++ results).collectFirst { case Failure(err) => err } match
          case Some(err) => completion.tryFailure(err)
          case None => completion.trySuccess(())
      }
    }

    def complete: Future[Unit] = completion.future

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = {
      actions.foreach(_.updateCommitErrors(commitErrors))
    }

    override def commitVoters(voters: Option[Set[StoreId]], incarnations: Map[StoreId, StoreIncarnation]): Unit =
      actions.foreach(_.commitVoters(voters, incarnations))

    override def handedOffStores: Set[StoreId] = actions.flatMap(_.handedOffStores).toSet

    def debugStatus: List[(String, Boolean)] = {
      actions.map(f => (f.getClass.getSimpleName, f.complete.isCompleted))
    }

    def cancel(): Unit =
      synchronized { cancelled = true }
      actions.foreach(_.cancel())
  }

  object Finalizer:
    /** A finalizer over actions some of which could not be created (an unknown type, or a factory that threw): each
      * such action is a failed one, so the others, the marker included, still run, and the driver's failure path
      * retries the whole finalizer with backoff, logged
      */
    def of(actions: List[Try[FinalizationAction]])(using ExecutionContext): Finalizer =
      new Finalizer(actions.map {
        case Success(a) => a
        case Failure(err) => new FailedFinalizationAction(err)
      })

    /** Stands for an action that could not be created: it fails at once */
    private final class FailedFinalizationAction(err: Throwable) extends FinalizationAction:
      def complete: Future[Unit] = Future.failed(err)
      def execute(): Unit = ()

    def isMissedUpdateMarker(action: FinalizationAction): Boolean = action.isInstanceOf[MissedUpdateFinalizationAction]
}

class RegisteredTransactionFinalizerFactory(val client: AspenClient) extends TransactionFinalizer.Factory {
  def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = {
    val actions = txd.finalizationActions.map { sfa =>
      Try {
        val f = client.typeRegistry.getType[FinalizationActionFactory](sfa.typeId.uuid).getOrElse(
          throw new NoSuchElementException(s"Unknown finalization action type ${sfa.typeId.uuid}"))
        f.createFinalizationAction(client, txd, sfa.data)
      }
    }

    server.RegisteredTransactionFinalizerFactory.Finalizer.of(actions)(using client.clientContext)
  }
}
