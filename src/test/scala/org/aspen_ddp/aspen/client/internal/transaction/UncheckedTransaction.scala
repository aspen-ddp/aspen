package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.BaseAspenClient
import org.aspen_ddp.aspen.common.transaction.TransactionId

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Runs transactions built with a raw TransactionBuilder whose size maps are empty, so none of the client-side size
  * checks apply. Models a client that ignores the limits, to exercise the stores' backstop.
  */
object UncheckedTransaction:

  /** Builds the transaction with `build` and runs it, returning whether it committed */
  def run(aspenClient: AspenClient)(build: TransactionBuilder => Unit): Future[Boolean] =
    val client = aspenClient.asInstanceOf[BaseAspenClient]
    given ExecutionContext = client.clientContext
    val bldr = new TransactionBuilder(TransactionId(UUID.randomUUID()), DesignatedLeader.choose(client.messenger, _, _),
      client.clientId)
    build(bldr)
    val pools = Future.sequence(bldr.allReferencedPoolIds.toList.map(id => client.getStoragePool(id).map(id -> _)))
    pools.flatMap: ps =>
      val (txd, data, _) = bldr.buildTranaction(client.opportunisticRebuildManager,
        ps.map((id, pool) => id -> pool.ida).toMap, Map(), Map())
      client.txManager.runTransaction(txd, data, None)
