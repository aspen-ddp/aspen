package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.network.{TxFinalized, TxPrepare, TxResolved}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.HostObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionStatusCache}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class StoreSuite extends AnyFunSuite with Matchers:
  import FrontendSuite.*

  private val leader = StoreId(poolId, 1)
  private val peer = StoreId(poolId, 2)
  private val txd = mktxd().copy(designatedLeaderUID = 1.toByte,
    requirements = List(DataUpdate(op1, rev0, DataUpdateOperation.Overwrite)))

  private def prepare(to: StoreId, from: StoreId) =
    TxPrepare(to, from, txd, ProposalId.initialProposal(txd.designatedLeaderUID), Nil, Nil, fromClient = false)

  private def mkStore(id: StoreId, statusCache: TransactionStatusCache, heartbeatTimeout: Duration,
                      backendOverride: Option[MapBackend] = None) =
    val backend = backendOverride.getOrElse(new MapBackend(id))
    val net = new TestNet
    val crl = new TestCrl
    val store = new Store(StorageDeviceId(new UUID(0, 99)), ExecutionContext.global, backend,
      new HostObjectCache(1024 * 1024).forStore(id), net, BackgroundTaskManager.NoBackgroundTaskManager, crl, statusCache,
      null, TransactionDriver.noErrorRecoveryFactory, heartbeatTimeout, None)
    backend.setCompletionHandler(op => store.frontend.backendOperationComplete(op))
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    (store, net, crl)

  test("the status cache outlasts an idle transaction being noticed") {
    val period = Duration(1, SECONDS)
    Host.txStatusCacheDuration(period) should be > (Host.txHeartbeatTimeout(period) + period)
  }

  test("a store that finalized a transaction tells a recovery driver the transaction committed") {
    val statusCache = new TransactionStatusCache()
    statusCache.updateStatus(txid1, TransactionStatus.Committed, finalized = true)
    val (store, net, _) = mkStore(storeId, statusCache, Duration(40, SECONDS))

    store.receiveTransactionMessage(prepare(storeId, peer))

    net.sentTx.collect { case f: TxFinalized => f } match
      case f :: Nil =>
        f.to shouldBe peer
        f.committed shouldBe true
        // The finalized relay reports a finalized commit late: the driver runs no finalization actions again
        f.late shouldBe true
      case other => throw new AssertionError(s"Expected one TxFinalized, got $other")
    store.frontend.transactions.contains(txid1) shouldBe false
  }

  test("the finalized relay reports a finalized abort as not late") {
    val statusCache = new TransactionStatusCache()
    statusCache.updateStatus(txid1, TransactionStatus.Aborted, finalized = true)
    val (store, net, _) = mkStore(storeId, statusCache, Duration(40, SECONDS))

    store.receiveTransactionMessage(prepare(storeId, peer))

    net.sentTx.collect { case f: TxFinalized => (f.committed, f.late) } shouldBe List((false, false))
  }

  test("a relayed resolution of a non-finalized entry carries the entry's late flag") {
    for late <- List(false, true) do
      val statusCache = new TransactionStatusCache()
      statusCache.updateStatus(txid1, TransactionStatus.Committed, late = late)
      val (store, net, _) = mkStore(storeId, statusCache, Duration(40, SECONDS))

      store.receiveTransactionMessage(prepare(storeId, peer))

      net.sentTx.collect { case r: TxResolved => (r.committed, r.late, r.proof) } shouldBe List((true, late, false))
      net.sentTx.collect { case f: TxFinalized => f } shouldBe Nil
  }

  test("after a lost TxFinalized a held commit ends once a recovery driver learns the outcome") {
    val (store, _, crl) = mkStore(storeId, new TransactionStatusCache(), Duration.Zero)

    store.receiveTransactionMessage(prepare(storeId, leader))
    store.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = true, late = false, proof = false))
    store.frontend.transactions.contains(txid1) shouldBe true

    store.heartbeat()
    store.hasTransactions shouldBe true

    store.receiveTransactionMessage(TxFinalized(storeId, peer, txid1, committed = true, late = false))
    store.frontend.transactions.contains(txid1) shouldBe false
    store.hasTransactions shouldBe false
    crl.txDel shouldBe true
  }

  test("a finalized transaction with an outstanding commit is not driven again") {
    val backend = new DeferredCommitBackend
    val (store, _, crl) = mkStore(storeId, new TransactionStatusCache(), Duration.Zero, Some(backend))

    store.receiveTransactionMessage(prepare(storeId, leader).copy(fromClient = true,
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    store.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = true, late = false, proof = false))
    store.receiveTransactionMessage(TxFinalized(storeId, leader, txid1, committed = true, late = false))
    backend.heldCommits shouldBe 1
    crl.deleted shouldBe Nil

    store.heartbeat()
    store.hasTransactions shouldBe false

    backend.deliverCommits()
    crl.deleted shouldBe List(txid1)
  }
