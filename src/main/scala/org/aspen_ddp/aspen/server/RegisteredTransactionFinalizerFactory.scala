package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory}
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.server
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.transaction.TransactionFinalizer

import scala.concurrent.{ExecutionContext, Future}

object RegisteredTransactionFinalizerFactory {
  class Finalizer(val client: AspenClient, val actions: List[FinalizationAction]) extends TransactionFinalizer {
    implicit val ec: ExecutionContext = client.clientContext

    actions.foreach(_.execute())

    def complete: Future[Unit] = Future.sequence(actions.map(_.complete)).map(_=>())

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = {
      actions.foreach(_.updateCommitErrors(commitErrors))
    }

    def debugStatus: List[(String, Boolean)] = {
      actions.map(f => (f.getClass.getSimpleName, f.complete.isCompleted))
    }

    def cancel(): Unit = actions.foreach(_.cancel())
  }
}

class RegisteredTransactionFinalizerFactory(val client: AspenClient) extends TransactionFinalizer.Factory {
  def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = {
    val actions = txd.finalizationActions.map { sfa =>
      val f = client.typeRegistry.getType[FinalizationActionFactory](sfa.typeId.uuid).get
      f.createFinalizationAction(client, txd, sfa.data)
    }

    new server.RegisteredTransactionFinalizerFactory.Finalizer(client, actions)
  }
}
