package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.network.{TxAccept, TxAcceptResponse, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{Delete, KeyValueObjectPointer, KeyValueOperation, ObjectId}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, ObjectUpdate, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.crl.TransactionRecoveryState
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.HostObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
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

  // ---- Ruling 15: a recovered commit vote that found its UnackedCommits entry proves the commit to its own driver ----

  /** Records the driver's batched sends too, which TestNet drops */
  private class RecordingNet extends TestNet:
    override def sendTransactionMessages(msgs: List[TxMessage]): Unit = msgs.foreach(sendTransactionMessage)

  private class StubFinalizer extends TransactionFinalizer with TransactionFinalizer.Factory:
    var created = 0
    def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer =
      created += 1
      this
    def complete: scala.concurrent.Future[Unit] = scala.concurrent.Future.unit
    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = ()
    def debugStatus: List[(String, Boolean)] = Nil
    def cancel(): Unit = ()

  private val foundTxid = TransactionId(new UUID(0, 60))
  private val kvPointer = KeyValueObjectPointer(oid2, poolId, Array[Byte]())

  /** A store recovering a commit vote for a traceless KV delete of key 2, its other stores having forgotten it. Its
    * entry is found, then its heartbeat starts its driver
    */
  private def foundEntryStore(): (Store, RecordingNet, StubFinalizer) =
    val backend = new MapBackend(storeId)
    val kv = mkKVObjectState()
    backend.bootstrapAllocate(oid2, kv.objectType, kv.metadata, kv.data)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(mkKVObjectState()) // So the recovered transaction loads during construction
    val txd = mktxd().copy(transactionId = foundTxid,
      requirements = List(KeyValueUpdate(kvPointer, None, None, Nil, deletesKeys = true)))
    val update = KeyValueOperation.encode(List(new Delete(k(2))), ida)(0)
    val trs = TransactionRecoveryState(storeId, txd.serialize(), List(ObjectUpdate(oid2, update)),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val net = new RecordingNet
    val lookup = new ManualLookup
    val finalizer = new StubFinalizer
    // Parasitic: the driver's finalizer completion runs inline, so the test sees its TxFinalized
    val store = new Store(StorageDeviceId(new UUID(0, 99)), ExecutionContext.parasitic, backend, cache, net,
      BackgroundTaskManager.NoBackgroundTaskManager, crl, new TransactionStatusCache(), finalizer,
      TransactionDriver.noErrorRecoveryFactory, Duration.Zero, None, unackedLookup = lookup)
    backend.setCompletionHandler(op => store.frontend.backendOperationComplete(op))
    lookup.answer(foundTxid, scala.util.Success(true))
    KVObjectState.decode(backend.get(oid2).get.data).content.keySet should not contain k(2)
    store.heartbeat() // The Tx has been idle for the zero heartbeat timeout: its store drives it
    store.hasTransactions shouldBe true
    (store, net, finalizer)

  private val initialProposal = ProposalId.initialProposal(mktxd().designatedLeaderUID)

  /** The proposal of the driver's latest prepares, or its initial one if it sent none */
  private def currentProposal(net: TestNet): ProposalId =
    net.sentTx.collect { case p: TxPrepare if p.transactionId == foundTxid => p.proposalId }.maxOption
      .getOrElse(initialProposal)

  private def promise(store: Store, from: Int, p: ProposalId, accepted: Option[(ProposalId, Boolean)]): Unit =
    store.receiveTransactionMessage(TxPrepareResponse(storeId, StoreId(poolId, from.toByte), foundTxid,
      Right(TxPrepareResponse.Promise(accepted)), p, TransactionDisposition.VoteAbort, Nil))

  test("a found recovered vote's driver never decides abort and resolves late on memoryless promises") {
    val (store, net, finalizer) = foundEntryStore()
    val p = currentProposal(net)

    // The other stores forgot the transaction: memoryless promises with abort votes
    promise(store, 1, p, None)
    promise(store, 2, p, None)

    net.sentTx.collect { case a: TxAccept => a.value } shouldBe Nil
    // The proof started a post-proof round (Ruling 6), whose promises are what count
    p should not be initialProposal
    net.sentTx.collect { case f: TxFinalized => (f.committed, f.late) }.distinct shouldBe List((true, true))
    finalizer.created shouldBe 0

    store.receiveTransactionMessage(TxFinalized(storeId, storeId, foundTxid, committed = true, late = true))
    store.frontend.transactions.contains(foundTxid) shouldBe false
  }

  test("a found recovered vote's driver adopts an accepted commit and resolves normally") {
    val (store, net, finalizer) = foundEntryStore()
    val p = currentProposal(net)
    promise(store, 1, p, Some((initialProposal, true)))
    promise(store, 2, p, None)

    val accepts = net.sentTx.collect { case a: TxAccept => a }
    accepts.map(_.value).distinct shouldBe List(true)
    for i <- List(1, 2) do
      store.receiveTransactionMessage(TxAcceptResponse(storeId, StoreId(poolId, i.toByte), foundTxid,
        accepts.head.proposalId, Right(TxAcceptResponse.Accepted(true))))

    finalizer.created shouldBe 1
    net.sentTx.collect { case f: TxFinalized => (f.committed, f.late) }.distinct shouldBe List((true, false))
  }

