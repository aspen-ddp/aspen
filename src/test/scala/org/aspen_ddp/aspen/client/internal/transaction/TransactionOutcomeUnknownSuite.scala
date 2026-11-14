package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.BaseAspenClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.client.{ReadDriverClient, StoragePool, Transaction, TransactionAborted, TransactionOutcomeUnknown, TransactionStatusCache}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.aspen_ddp.aspen.client.internal.read.BaseReadDriver

import scala.concurrent.{ExecutionContext, Future}

class TransactionOutcomeUnknownSuite extends IntegrationTestSuite:

  /** Records what the transaction reports to the client's status cache */
  private class RecordingCache extends TransactionStatusCache:
    @volatile var recorded: List[TransactionId] = Nil
    override def transactionAborted(txid: TransactionId): Unit = recorded ::= txid
    override def transactionCommitted(txid: TransactionId): Unit = recorded ::= txid
    override def transactionFinalized(txid: TransactionId): Unit = recorded ::= txid

  /** A driver that concludes with `outcome` as soon as it begins */
  private class FixedDriver(outcome: ClientOutcome, c: ReadDriverClient, txd: TransactionDescription,
                            data: Map[StoreId, TransactionData]) extends ClientTransactionDriver(c, txd, data):
    override def begin(): Unit = complete(outcome)

  /** Commits one refcount-pin transaction whose driver concludes with `outcome`, returning the failure and the
    * transactions the client's status cache heard of */
  private def commitWith(outcome: ClientOutcome): Future[(Throwable, List[TransactionId])] =
    commitWithManager(outcome).map((err, recorded, _) => (err, recorded))(using executionContext)

  /** As commitWith, with the TransactionManager that ran the transaction */
  private def commitWithManager(outcome: ClientOutcome): Future[(Throwable, List[TransactionId], TransactionManager)] =
    given ExecutionContext = executionContext
    val cache = new RecordingCache
    val pools = net.client
    val recClient = new BaseAspenClient(executionContext, radicle, org.aspen_ddp.aspen.common.network.ClientId(new java.util.UUID(0, 2)),
      net.cliMessenger, cache, BaseReadDriver.noErrorRecoveryReadDriver,
      ClientTransactionDriver.noErrorRecoveryFactory, Nil):
      // The network answers reads only for its own client
      override def getStoragePool(poolId: PoolId): Future[StoragePool] = pools.getStoragePool(poolId)
      override protected def resolveIda(pointer: ObjectPointer): Future[IDA] = Future.successful(net.ida)
      override protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(
        prepare: Transaction => Future[T]): Future[T] = transact(prepare)
    val txm = new TransactionManager(recClient, (c, txd, data) => new FixedDriver(outcome, c, txd, data))
    for
      before <- client.read(radicle)
      tx = TransactionImpl.Factory(recClient, txm, DesignatedLeader.choose(recClient.messenger, _, _), None)
      _ = tx.setRefcount(radicle, before.refcount, before.refcount.increment())
      err <- tx.commit().failed
    yield
      (err, cache.recorded, txm)

  atest("an Unknown outcome fails commit with TransactionOutcomeUnknown and records nothing in the client status cache"):
    commitWith(ClientOutcome.Unknown).map: (err, recorded) =>
      err shouldBe a[TransactionOutcomeUnknown]
      recorded should be(Nil)

  atest("a driver that concludes Unknown is forgotten by its TransactionManager"):
    commitWithManager(ClientOutcome.Unknown).map: (err, _, txm) =>
      err shouldBe a[TransactionOutcomeUnknown]
      txm.pending should be(Set())

  atest("an Aborted outcome still fails commit with TransactionAborted and is recorded"):
    commitWith(ClientOutcome.Aborted).map: (err, recorded) =>
      err shouldBe a[TransactionAborted]
      recorded.size should be(1)
