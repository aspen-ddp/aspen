package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory}
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.server
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.transaction.TransactionFinalizer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object RegisteredTransactionFinalizerFactory {

  /** Runs a committed transaction's finalization actions. Every action, the MissedUpdateFinalizationAction (the
    * marker) included, executes at once and in parallel, so the stores that missed the transaction are marked for
    * repair whatever the other actions do, even while one of them retries for a long time (transaction-recovery 4.4).
    * `complete` covers all of them, and `cancel` cancels all of them.
    *
    * Invariant: a durable UnackedCommits entry implies the transaction committed, and nothing more: the other actions
    * may still be running, or may have failed, to be run again by a fresh finalizer. What implies that every action is
    * done is an acceptor having forgotten the transaction. A driver sends TxFinalized only once a finalizer's
    * `complete` has succeeded, so once every action, the marker's writes in every pool included, is done. An acceptor
    * forgets a committed transaction only on TxFinalized, or on a late outcome, which itself follows TxFinalized;
    * finding its UnackedCommits entry (Tx.unackedCommitFound) applies the update but keeps the Tx until then, and an
    * expiry Discard needs a quorum that forgot it. A late commit (TransactionDriver.resolveLate) relies on this when it
    * runs none of the actions.
    *
    * Each retry of a failed finalizer runs the marker again; its writes are idempotent.
    *
    * The marker receives `updateCommitErrors` and `commitVoters` right after the finalizer is created, before its
    * `errorTimeout` lets it write, and keeps later updates.
    *
    * `complete` settles once every action has settled: it fails with the first failure, or succeeds if none failed. It
    * relies on every action settling (an action that gives up fails its `complete`, never leaves it pending); failing
    * earlier would let the driver cancel the marker before it writes. The driver then starts a fresh finalizer
    * (TransactionDriver.startFinalizer).
    */
  class Finalizer(val actions: List[FinalizationAction])(using ec: ExecutionContext) extends TransactionFinalizer {

    actions.foreach(_.execute())

    val complete: Future[Unit] =
      Future.sequence(actions.map(_.complete.transform(t => Success(t)))).flatMap { results =>
        results.collectFirst { case Failure(err) => err } match
          case Some(err) => Future.failed(err)
          case None => Future.unit
      }

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = {
      actions.foreach(_.updateCommitErrors(commitErrors))
    }

    override def commitVoters(voters: Option[Set[StoreId]], incarnations: Map[StoreId, StoreIncarnation]): Unit =
      actions.foreach(_.commitVoters(voters, incarnations))

    override def handedOffStores: Set[StoreId] = actions.flatMap(_.handedOffStores).toSet

    def debugStatus: List[(String, Boolean)] = {
      actions.map(f => (f.getClass.getSimpleName, f.complete.isCompleted))
    }

    def cancel(): Unit = actions.foreach(_.cancel())
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
