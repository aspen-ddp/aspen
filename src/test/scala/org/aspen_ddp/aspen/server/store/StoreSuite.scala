package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.network.{TxAccept, TxAcceptResponse, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved, TxStatusRequest, TxStatusResponse}
import org.aspen_ddp.aspen.common.objects.{Delete, KeyValueObjectPointer, KeyValueOperation, ObjectId, ObjectPointer, ObjectRevision}
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

import org.aspen_ddp.aspen.common.HLCTimestamp
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class StoreSuite extends AnyFunSuite with Matchers:
  import FrontendSuite.*

  private val leader = StoreId(poolId, 1)
  private val peer = StoreId(poolId, 2)
  /** Made per use, so it starts after the stores built before it loaded (Frontend.forgetFloorMillis) */
  private def txd = mktxd().copy(designatedLeaderUID = 1.toByte,
    requirements = List(DataUpdate(op1, rev0, DataUpdateOperation.Overwrite)))

  private def prepare(to: StoreId, from: StoreId) =
    TxPrepare(to, from, txd, ProposalId.initialProposal(txd.designatedLeaderUID), Nil, fromClient = false)

  private def mkStore(id: StoreId, statusCache: TransactionStatusCache, heartbeatTimeout: Duration,
                      backendOverride: Option[MapBackend] = None,
                      backgroundTasks: BackgroundTaskManager = BackgroundTaskManager.NoBackgroundTaskManager,
                      driverFactory: TransactionDriver.Factory = TransactionDriver.noErrorRecoveryFactory,
                      loadedAt: Option[Long] = FrontendSuite.longLoaded,
                      wallClockMillis: () => Long = () => System.currentTimeMillis(),
                      unackedLookup: UnackedCommitLookup = UnackedCommitLookup.Never,
                      net: TestNet = new TestNet) =
    val backend = backendOverride.getOrElse(new MapBackend(id))
    val crl = new TestCrl
    val store = new Store(StorageDeviceId(new UUID(0, 99)), ExecutionContext.global, backend,
      new HostObjectCache(1024 * 1024).forStore(id), net, backgroundTasks, crl, statusCache,
      null, driverFactory, heartbeatTimeout, None, unackedLookup = unackedLookup, loadedAt = loadedAt,
      wallClockMillis = wallClockMillis)
    backend.setCompletionHandler(op => store.frontend.backendOperationComplete(op))
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    (store, net, crl)

  test("the status cache outlasts an idle transaction being noticed") {
    val period = Duration(1, SECONDS)
    Host.txStatusCacheDuration(period, 20.seconds) should be > (Host.txHeartbeatTimeout(period) + period)
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

      net.sentTx.collect { case r: TxResolved => (r.committed, r.late, r.proof) }.distinct shouldBe
        List((true, late, false))
      net.sentTx.collect { case f: TxFinalized => f } shouldBe Nil
  }

  // paxos-transactions 10.2: the sender's recovery driver learns the outcome, and the store's own new Tx and driver do
  test("a relayed resolution of a non-finalized entry reaches the sender and the relaying store") {
    for status <- List(TransactionStatus.Committed, TransactionStatus.Aborted) do
      val statusCache = new TransactionStatusCache()
      statusCache.updateStatus(txid1, status)
      val (store, net, _) = mkStore(storeId, statusCache, Duration(40, SECONDS))

      store.receiveTransactionMessage(prepare(storeId, peer))

      net.sentTx.collect { case r: TxResolved => (r.to, r.from) }.toSet shouldBe Set((peer, storeId), (storeId, peer))
  }

  test("a relayed resolution answering a store's own prepare is delivered once") {
    val statusCache = new TransactionStatusCache()
    statusCache.updateStatus(txid1, TransactionStatus.Committed)
    val (store, net, _) = mkStore(storeId, statusCache, Duration(40, SECONDS))

    store.receiveTransactionMessage(prepare(storeId, storeId))

    net.sentTx.collect { case r: TxResolved => (r.to, r.from) } shouldBe List((storeId, storeId))
  }

  // transaction-recovery 4.2: after a lost TxFinalized, the entry the store's own resolved Tx wrote is no news to its
  // driver, which would otherwise take it as a non-late resolution and run T's finalization actions again
  test("the relay of a non-finalized entry tells a store whose own Tx is resolved nothing, and still tells the sender") {
    val statusCache = new TransactionStatusCache()
    val (store, net, _) = mkStore(storeId, statusCache, Duration(40, SECONDS))
    store.receiveTransactionMessage(prepare(storeId, leader))
    store.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = true, late = false, proof = false))
    statusCache.getStatus(txid1).map(e => (e.status, e.finalized)) shouldBe Some((TransactionStatus.Committed, false))

    net.sentTx = Nil
    store.receiveTransactionMessage(prepare(storeId, storeId))
    store.receiveTransactionMessage(prepare(storeId, peer))

    net.sentTx.collect { case r: TxResolved => (r.to, r.from, r.late) } shouldBe List((peer, storeId, false))
  }

  // paxos-transactions 10.6: a sibling store's delayed resolution no longer downgrades the host's finalized entry, so
  // the relay still answers a replayed prepare with a late, finalized commit
  test("a sibling store's delayed resolution does not downgrade a finalized late entry") {
    val statusCache = new TransactionStatusCache()
    val (sibling, _, _) = mkStore(peer, statusCache, Duration(40, SECONDS))
    val (store, net, _) = mkStore(storeId, statusCache, Duration(40, SECONDS))

    // The sibling's Tx exists before the transaction is finalized elsewhere on the host, and late
    sibling.receiveTransactionMessage(prepare(peer, leader))
    statusCache.updateStatus(txid1, TransactionStatus.Committed, finalized = true, late = true)
    sibling.receiveTransactionMessage(TxResolved(peer, leader, txid1, committed = true, late = false, proof = false))

    store.receiveTransactionMessage(prepare(storeId, leader))

    net.sentTx.collect { case r: TxResolved => (r.committed, r.late) } shouldBe List((true, true))
    net.sentTx.collect { case f: TxFinalized => (f.committed, f.late) } shouldBe List((true, true))
    store.frontend.transactions.contains(txid1) shouldBe false
  }

  test("a designated leader does not drive a transaction started too far ahead of its clock") {
    val (store, net, _) = mkStore(leader, new TransactionStatusCache(), Duration(40, SECONDS))
    val ahead = HLCTimestamp((System.currentTimeMillis() + 3600000) << 16)
    val early = TxPrepare(leader, leader, txd.copy(startTimestamp = ahead),
      ProposalId.initialProposal(txd.designatedLeaderUID), Nil, fromClient = true)
    store.receiveTransactionMessage(early)

    store.hasTransactions shouldBe false
    store.frontend.transactions.contains(txid1) shouldBe false
    net.sentTx shouldBe Nil

    // The retransmission once the clocks agree (here: the same transaction stamped within the bound) is driven
    store.receiveTransactionMessage(prepare(leader, leader))
    store.hasTransactions shouldBe true
  }

  test("the designated leader starts no driver for a prepare below its forget floor") {
    // Started before the store loaded: below its floor, and unknown to it
    val start = HLCTimestamp((System.currentTimeMillis() - 1000) << 16)
    val (store, net, crl) = mkStore(leader, new TransactionStatusCache(), Duration(40, SECONDS),
      loadedAt = Some(System.currentTimeMillis()))
    val old = TxPrepare(leader, leader, txd.copy(startTimestamp = start),
      ProposalId.initialProposal(txd.designatedLeaderUID), Nil, fromClient = true)
    store.receiveTransactionMessage(old)

    store.hasTransactions shouldBe false
    store.frontend.transactions.contains(txid1) shouldBe false
    net.sentTx shouldBe Nil
    crl.txSaved shouldBe false
  }

  /** Records the Start of every driver it creates */
  private class RecordingFactory extends TransactionDriver.Factory:
    var starts: List[TransactionDriver.Start] = Nil
    def create(ec: ExecutionContext, storeId: StoreId, messenger: Messenger, backgroundTasks: BackgroundTaskManager,
               txd: TransactionDescription, finalizerFactory: TransactionFinalizer.Factory,
               start: TransactionDriver.Start): TransactionDriver =
      starts = starts :+ start
      TransactionDriver.noErrorRecoveryFactory.create(ec, storeId, messenger, backgroundTasks, txd, finalizerFactory,
        start)

  // transaction-recovery 4.3, proposal ids
  test("only a client prepare creating the transaction at its leader starts a driver at the initial proposal") {
    val factory = new RecordingFactory
    val (store, _, _) = mkStore(leader, new TransactionStatusCache(), Duration(40, SECONDS), driverFactory = factory)
    store.receiveTransactionMessage(prepare(leader, leader).copy(fromClient = true))
    factory.starts.map(_.initialProposal) shouldBe List(true)

    val other = new RecordingFactory
    val (store2, _, _) = mkStore(leader, new TransactionStatusCache(), Duration(40, SECONDS), driverFactory = other)
    store2.receiveTransactionMessage(prepare(leader, peer))
    other.starts.map(_.initialProposal) shouldBe List(false)
  }

  test("a driver started for an idle Tx starts above what the store's acceptor promised, and reports split decisions") {
    val factory = new RecordingFactory
    val statusCache = new TransactionStatusCache()
    val (store, _, _) = mkStore(peer, statusCache, Duration.Zero, driverFactory = factory)
    store.receiveTransactionMessage(prepare(peer, leader).copy(proposalId = ProposalId(3, 1), fromClient = true))
    store.frontend.transactions(txid1).highestProposal shouldBe Some(ProposalId(3, 1))

    store.heartbeat()
    factory.starts.map(s => (s.initialProposal, s.highestSeen)) shouldBe List((false, Some(ProposalId(3, 1))))

    factory.starts.head.onSplitDecision(txid1, "test")
    statusCache.splitDecisions shouldBe 1
  }

  /** A fixed start on the injected clock: the store's view of time is the test's alone */
  private val fixedStart = 1_000_000_000_000L
  private def fixedTxd = txd.copy(startTimestamp = HLCTimestamp(fixedStart << 16))

  test("a driver past the horizon is removed once its store's Tx expired") {
    var now = fixedStart
    val (store, net, _) = mkStore(leader, new TransactionStatusCache(), Duration(40, SECONDS),
      wallClockMillis = () => now)
    // A client's prepare without the data: an abort vote at once
    store.receiveTransactionMessage(TxPrepare(leader, leader, fixedTxd, ProposalId.initialProposal(1.toByte), Nil,
      fromClient = true))
    store.hasTransactions shouldBe true
    val start = fixedStart

    now = start + 20_000 + store.expiryMargin.toMillis + 1
    store.heartbeat()
    store.hasTransactions shouldBe true // Its Tx still holds T: no peer has answered
    val round = net.sentTx.collect { case r: TxStatusRequest => r }
    round.map(_.to).toSet shouldBe Set(storeId, peer)

    round.foreach(r => store.receiveTransactionMessage(TxStatusResponse(leader, r.to, txid1, r.requestUUID, None)))
    store.frontend.transactions.contains(txid1) shouldBe false
    store.heartbeat()
    store.hasTransactions shouldBe false
  }

  test("a committed Tx whose TxFinalized was lost concludes past the horizon, and its driver is removed") {
    var now = fixedStart
    val lookup = new ManualLookup
    val (store, net, crl) = mkStore(storeId, new TransactionStatusCache(), Duration.Zero,
      wallClockMillis = () => now, unackedLookup = lookup)
    lookup.frontend = Some(store.frontend)
    store.receiveTransactionMessage(TxPrepare(storeId, leader, fixedTxd, ProposalId.initialProposal(1.toByte),
      List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))), fromClient = true))
    store.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = true, late = false, proof = false))
    // Its TxFinalized is lost: idle, it gets a driver, which its peers (past their floor) never answer
    store.heartbeat()
    store.hasTransactions shouldBe true

    now = fixedStart + 20_000 + store.expiryMargin.toMillis + 1
    store.heartbeat()
    val round = net.sentTx.collect { case r: TxStatusRequest => r }
    round.map(_.to).toSet shouldBe Set(leader, peer)
    round.foreach(r => store.receiveTransactionMessage(TxStatusResponse(storeId, r.to, txid1, r.requestUUID, None)))
    lookup.answer(txid1, lookup.absent) // Not owed a stub
    store.frontend.transactions.contains(txid1) shouldBe false
    crl.deleted should contain (txid1)

    store.heartbeat()
    store.hasTransactions shouldBe false
  }

  /** Also records batched sends (a driver's Accepts), which TestNet drops */
  private class BatchNet extends TestNet:
    var batched: List[TxMessage] = Nil
    override def sendTransactionMessages(msg: List[TxMessage]): Unit = batched = batched ++ msg

  // paxos-transactions 7.2: a commit vote, an abort vote and a silent third store. Before expiry the driver's tally
  // waits for the silent store. Past it both voters are frozen (they promise and accept nothing more). The expiry tally
  // still holds while the third store is silent (ExpiryTally rule G): it may have voted commit, which with the leader's
  // vote is Replication(3, 2)'s write threshold, so a client may have learned commit from the votes. (Under the old
  // rules it discarded at once: no accept of commit existed.) Once the third store reports it never voted, one commit
  // voter is below the read threshold of 2, so T's update can never be restored, and T is discarded (rule D, which
  // comes first; rule E, no pool able to reach its write threshold of commit votes, would discard it too)
  test("a commit voter whose peer voted abort is discarded past expiry once its third store replies, not before") {
    var now = fixedStart
    val lookup = new ManualLookup
    val batchNet = new BatchNet
    val (store, net, crl) = mkStore(leader, new TransactionStatusCache(), Duration(40, SECONDS),
      wallClockMillis = () => now, unackedLookup = lookup, net = batchNet)
    lookup.frontend = Some(store.frontend)
    store.receiveTransactionMessage(TxPrepare(leader, leader, fixedTxd, ProposalId.initialProposal(1.toByte),
      List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))), fromClient = true))
    val own = net.sentTx.collect { case r: TxPrepareResponse => r }
    own.map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
    val round = own.head.proposalId

    def promise(from: StoreId, disposition: TransactionDisposition.Value): Unit =
      store.receiveTransactionMessage(TxPrepareResponse(leader, from, txid1, Right(TxPrepareResponse.Promise(None)),
        round, disposition, Nil))
    def sentAccepts = batchNet.batched.collect { case a: TxAccept => (a.to, a.value) }.toSet

    promise(leader, TransactionDisposition.VoteCommit)
    promise(peer, TransactionDisposition.VoteAbort)
    sentAccepts shouldBe empty

    // Just short of expiry, a resent prepare's reply still waits for the silent store
    now = fixedStart + 20_000 + store.expiryMargin.toMillis
    promise(peer, TransactionDisposition.VoteAbort)
    sentAccepts shouldBe empty

    // Past expiry: the peer reports its frozen abort vote, with nothing accepted
    now += 1
    store.heartbeat()
    val status = net.sentTx.collect { case r: TxStatusRequest => r }
    status.map(_.to).toSet shouldBe Set(storeId, peer)
    status.filter(_.to == peer).foreach: r =>
      store.receiveTransactionMessage(TxStatusResponse(leader, peer, txid1, r.requestUUID,
        Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
          holdsAcceptorState = true))))
    // The third store is silent: hold
    lookup.requests shouldBe Nil
    store.frontend.transactions.contains(txid1) shouldBe true
    // It answers "unknown": it never joined T
    status.filter(_.to == storeId).foreach: r =>
      store.receiveTransactionMessage(TxStatusResponse(leader, storeId, txid1, r.requestUUID, None))
    // The commit voter looks up its UnackedCommits entry before it discards T
    lookup.requests shouldBe List(txid1)
    lookup.answer(txid1, lookup.absent)
    store.frontend.transactions.contains(txid1) shouldBe false
    crl.deleted should contain (txid1)
  }

  // A closed store's drivers would otherwise keep re-preparing its transactions from their timers
  test("closing a store stops its transaction drivers") {
    val tasks = new org.aspen_ddp.aspen.server.transaction.SimpleTransactionDriverSuite.ManualTasks(ExecutionContext.global)
    val factory = org.aspen_ddp.aspen.server.transaction.SimpleTransactionDriver.factory(100.milliseconds, 3.seconds)
    val (store, net, _) = mkStore(leader, new TransactionStatusCache(), Duration(40, SECONDS),
      backgroundTasks = tasks, driverFactory = factory)

    store.receiveTransactionMessage(prepare(leader, leader))
    store.hasTransactions shouldBe true

    store.close()
    store.hasTransactions shouldBe false
    net.sentTx = Nil

    tasks.fireAll()
    net.sentTx.collect { case m @ (_: TxPrepare | _: TxAccept | _: TxResolved) => m } shouldBe Nil
  }

  test("after a lost TxFinalized a held commit ends once a recovery driver learns the outcome") {
    val (store, _, crl) = mkStore(storeId, new TransactionStatusCache(), Duration.Zero)

    store.receiveTransactionMessage(prepare(storeId, leader))
    store.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = true, late = false, proof = false))
    store.frontend.transactions.contains(txid1) shouldBe true

    store.heartbeat()
    store.hasTransactions shouldBe true

    store.receiveTransactionMessage(TxFinalized(storeId, peer, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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
    store.receiveTransactionMessage(TxFinalized(storeId, leader, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    backend.heldCommits shouldBe 1
    crl.deleted shouldBe Nil

    store.heartbeat()
    store.hasTransactions shouldBe false

    backend.deliverCommits()
    crl.deleted shouldBe List(txid1)
  }

  // ---- A recovered commit vote that found its UnackedCommits entry proves the commit (transaction-recovery 7.5) ----

  /** Records the driver's batched sends too, which TestNet drops */
  private class RecordingNet extends TestNet:
    override def sendTransactionMessages(msgs: List[TxMessage]): Unit = msgs.foreach(sendTransactionMessage)

  private class StubFinalizer extends TransactionFinalizer with TransactionFinalizer.Factory:
    var created = 0
    def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer =
      created += 1
      this
    var completion: scala.concurrent.Promise[Unit] = scala.concurrent.Promise.successful(())
    var handedOff: Set[StoreId] = Set()
    def complete: scala.concurrent.Future[Unit] = completion.future
    override def handedOffStores: Set[StoreId] = handedOff
    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]], keptForReplay: Set[StoreId]): Unit = ()
    def debugStatus: List[(String, Boolean)] = Nil
    def cancel(): Unit = ()

  private val foundTxid = TransactionId(new UUID(0, 60))
  private val kvPointer = KeyValueObjectPointer(oid2, poolId, Array[Byte]())

  /** A store recovering a commit vote for a traceless KV delete of key 2, its other stores having forgotten it. Its
    * entry is found, then its heartbeat starts its driver
    */
  private def foundEntryStore(wallClockMillis: () => Long = () => System.currentTimeMillis()
                             ): (Store, RecordingNet, StubFinalizer) =
    val (store, net, finalizer, lookup, backend) = recoveredVoteStore(wallClockMillis)
    lookup.answer(foundTxid, lookup.own)
    KVObjectState.decode(backend.get(oid2).get.data).content.keySet should not contain k(2)
    store.heartbeat() // The Tx has been idle for the zero heartbeat timeout: its store drives it
    store.hasTransactions shouldBe true
    (store, net, finalizer)

  /** The store of foundEntryStore before its recovered vote's lookup is answered */
  private def recoveredVoteStore(wallClockMillis: () => Long = () => System.currentTimeMillis()
                                ): (Store, RecordingNet, StubFinalizer, ManualLookup, MapBackend) =
    val backend = new MapBackend(storeId)
    val kv = mkKVObjectState()
    backend.bootstrapAllocate(oid2, kv.objectType, kv.metadata, kv.data)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(mkKVObjectState()) // So the recovered transaction loads during construction
    val txd = mktxd().copy(transactionId = foundTxid,
      requirements = List(KeyValueUpdate(kvPointer, None, None, List(KeyValueUpdate.KeyRevision(k(2), rev0)),
        deletesKeys = true)))
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
      TransactionDriver.noErrorRecoveryFactory, Duration.Zero, None, unackedLookup = lookup, loadedAt = FrontendSuite.longLoaded,
      wallClockMillis = wallClockMillis)
    lookup.frontend = Some(store.frontend)
    backend.setCompletionHandler(op => store.frontend.backendOperationComplete(op))
    (store, net, finalizer, lookup, backend)

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
    // The proof started a post-proof round, whose promises are what count
    p should not be initialProposal
    net.sentTx.collect { case f: TxFinalized => (f.committed, f.late) }.distinct shouldBe List((true, true))
    finalizer.created shouldBe 0

    store.receiveTransactionMessage(TxFinalized(storeId, storeId, foundTxid, committed = true, late = true, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    store.frontend.transactions.contains(foundTxid) shouldBe false
  }

  // paxos-transactions.md, late condition 1: an expiry commit is accepted by no acceptor, so its holders not yet frozen
  // by their own clocks answer memoryless promises while the expiry driver's finalizer writes its marks. A late
  // TxFinalized from this driver would cancel that finalizer. Past the horizon the driver waits for the real one
  test("past the horizon a found recovered vote's driver does not resolve late on memoryless promises") {
    var now = System.currentTimeMillis()
    val (store, net, finalizer) = foundEntryStore(() => now)
    val tx = store.frontend.transactions(foundTxid)
    now = tx.txd.startTimestamp.wallTime + store.frontend.horizon.toMillis + 1
    val p = currentProposal(net)
    p should not be initialProposal

    // Holders resolved by the expiry commit, not yet frozen: memoryless promises
    promise(store, 1, p, None)
    promise(store, 2, p, None)

    net.sentTx.collect { case a: TxAccept => a.value } shouldBe Nil
    net.sentTx.collect { case f: TxFinalized => f } shouldBe Nil
    net.sentTx.collect { case r: TxResolved if r.late => r } shouldBe Nil
    finalizer.created shouldBe 0
    store.hasTransactions shouldBe true

    // The expiry driver's TxFinalized, sent once its marks are durable
    store.receiveTransactionMessage(TxFinalized(storeId, StoreId(poolId, 1.toByte), foundTxid, committed = true,
      late = false, owed = Set(storeId), startTimestamp = tx.txd.startTimestamp))
    net.sentTx.collect { case f: TxFinalized => (f.committed, f.late) }.distinct shouldBe List((true, false))
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

  test("another driver's TxFinalized finalizes a driver whose cancelled marker is still writing, listing its stores") {
    val (store, net, finalizer) = foundEntryStore()
    finalizer.handedOff = Set(leader)
    finalizer.completion = scala.concurrent.Promise() // Never completes: the mark is still being written
    val p = currentProposal(net)
    promise(store, 1, p, Some((initialProposal, true)))
    promise(store, 2, p, None)
    val accepts = net.sentTx.collect { case a: TxAccept => a }
    for i <- List(1, 2) do
      store.receiveTransactionMessage(TxAcceptResponse(storeId, StoreId(poolId, i.toByte), foundTxid,
        accepts.head.proposalId, Right(TxAcceptResponse.Accepted(true))))
    finalizer.created shouldBe 1
    net.sentTx = Nil

    store.receiveTransactionMessage(TxFinalized(storeId, peer, foundTxid, committed = true, late = false, owed = Set(peer),
      startTimestamp = HLCTimestamp.now))
    net.sentTx.collect { case f: TxFinalized => (f.owed, f.late) }.distinct shouldBe List((Set(peer, leader), false))
    store.hasTransactions shouldBe false
  }

  // paxos-transactions.md, split decisions: a driver that resolved commit ignores an abort TxFinalized (committed
  // wins) and keeps finalizing, so its store keeps it until its own TxFinalized
  test("a driver that resolved commit and ignores an abort TxFinalized stays in its store until it finalizes") {
    val (store, net, finalizer) = foundEntryStore()
    finalizer.completion = scala.concurrent.Promise() // The finalization actions are still running
    val p = currentProposal(net)
    promise(store, 1, p, Some((initialProposal, true)))
    promise(store, 2, p, None)
    val accepts = net.sentTx.collect { case a: TxAccept => a }
    for i <- List(1, 2) do
      store.receiveTransactionMessage(TxAcceptResponse(storeId, StoreId(poolId, i.toByte), foundTxid,
        accepts.head.proposalId, Right(TxAcceptResponse.Accepted(true))))
    finalizer.created shouldBe 1
    net.sentTx = Nil

    store.receiveTransactionMessage(TxFinalized(storeId, peer, foundTxid, committed = false, late = false,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    net.sentTx.collect { case f: TxFinalized => f } shouldBe Nil
    store.hasTransactions shouldBe true

    finalizer.completion.success(())
    val own = net.sentTx.collect { case f: TxFinalized => f }
    own.map(f => (f.committed, f.late)).distinct shouldBe List((true, false))
    store.receiveTransactionMessage(own.find(_.to == storeId).get)
    store.hasTransactions shouldBe false
  }

  // ---- Recovered commit votes are driven at once (transaction-recovery 7.5) ----

  test("a recovered vote whose lookup is unanswered or failing is driven at once, and an entry found later gives its running driver the proof") {
    val (store, net, finalizer, lookup, _) = recoveredVoteStore()
    store.heartbeat() // Idle past the zero heartbeat timeout: driven although the lookup is unanswered
    store.hasTransactions shouldBe true

    lookup.answer(foundTxid, scala.util.Failure(new Exception("metadata pool down")))
    store.heartbeat() // Retries the lookup; the driver keeps running
    lookup.requests.count(_ == foundTxid) shouldBe 2
    store.hasTransactions shouldBe true

    lookup.answer(foundTxid, lookup.own)
    // The proof reached the running driver: it starts a post-proof round, and memoryless promises resolve it late
    val p = currentProposal(net)
    p should not be initialProposal
    promise(store, 1, p, None)
    promise(store, 2, p, None)
    net.sentTx.collect { case a: TxAccept => a.value } shouldBe Nil
    net.sentTx.collect { case f: TxFinalized => (f.committed, f.late) }.distinct shouldBe List((true, true))
    finalizer.created shouldBe 0
  }

  test("with lookups failing, a recovered vote whose peers all hold commit votes commits") {
    // A cold start of the whole cluster with the metadata group unreadable: every store recovered its commit vote
    val (store, net, finalizer, lookup, _) = recoveredVoteStore()
    lookup.answer(foundTxid, scala.util.Failure(new Exception("metadata pool down")))
    store.heartbeat()
    store.hasTransactions shouldBe true

    val p = currentProposal(net)
    for i <- List(1, 2) do
      store.receiveTransactionMessage(TxPrepareResponse(storeId, StoreId(poolId, i.toByte), foundTxid,
        Right(TxPrepareResponse.Promise(None)), p, TransactionDisposition.VoteCommit, Nil))
    val accepts = net.sentTx.collect { case a: TxAccept => a }
    accepts.map(_.value).distinct shouldBe List(true)
    for i <- List(1, 2) do
      store.receiveTransactionMessage(TxAcceptResponse(storeId, StoreId(poolId, i.toByte), foundTxid,
        accepts.head.proposalId, Right(TxAcceptResponse.Accepted(true))))
    finalizer.created shouldBe 1

    // The non-late commit reaches the Tx: it applies T and releases its locks with no lookup answered
    store.receiveTransactionMessage(TxResolved(storeId, storeId, foundTxid, committed = true, late = false,
      proof = false))
    store.frontend.transactions(foundTxid).status shouldBe TransactionStatus.Committed
  }

  // ---- Driver traffic while a recovered Tx loads (transaction-recovery 7.3) ----

  /** A MapBackend whose reads complete only on deliver */
  private class DeferredReadBackend(id: StoreId) extends MapBackend(id):
    private var deferred: List[ObjectPointer] = Nil
    override def read(pointer: ObjectPointer): Unit = deferred = deferred :+ pointer
    def deliver(): Unit =
      val reads = deferred
      deferred = Nil
      reads.foreach(super.read)

  test("a recovery driver started while its recovered Tx loads starts above the recovered promise, and the Tx " +
       "answers its traffic only once loaded") {
    val backend = new DeferredReadBackend(storeId)
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    val rtxd = txd
    val promised = ProposalId(5, 2)
    val trs = TransactionRecoveryState(storeId, rtxd.serialize(), List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState(Some(promised), None))
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val net = new RecordingNet
    val finalizer = new StubFinalizer
    val tasks = new org.aspen_ddp.aspen.server.transaction.SimpleTransactionDriverSuite.ManualTasks(
      ExecutionContext.parasitic)
    val store = new Store(StorageDeviceId(new UUID(0, 99)), ExecutionContext.parasitic, backend,
      new HostObjectCache(1024 * 1024).forStore(storeId), net, tasks, crl, new TransactionStatusCache(), finalizer,
      org.aspen_ddp.aspen.server.transaction.SimpleTransactionDriver.factory(1.second, 1.second), Duration.Zero, None,
      loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => store.frontend.backendOperationComplete(op))

    // The CRL state has loaded, the recovered Tx's object has not: its idle Tx is driven at heartbeat
    store.frontend.recoveryPending shouldBe false
    store.heartbeat()
    store.hasTransactions shouldBe true
    tasks.fireAll() // The driver's first send
    val prepares = net.sentTx.collect { case p: TxPrepare => p }
    prepares.map(_.to).toSet shouldBe Set(storeId, leader, peer)
    // Above the promise recovered from the CRL, which the Tx knows before its objects load (paxos-transactions 6.4)
    prepares.foreach(_.proposalId should be > promised)

    def fromOwnTx: List[TxMessage] = net.sentTx.filter:
      case r: TxPrepareResponse => r.from == storeId
      case r: TxAcceptResponse => r.from == storeId
      case _ => false
    def deliverOwn(): Unit =
      val own = net.sentTx.filter(m => m.to == storeId && !fromOwnTx.contains(m))
      net.sentTx = Nil
      own.foreach(store.receiveTransactionMessage)

    deliverOwn() // The driver's prepare to its own store: held while the Tx loads
    // The peers promise with commit votes, which decide commit without this store's vote
    val p = prepares.head.proposalId
    for i <- List(1, 2) do
      store.receiveTransactionMessage(TxPrepareResponse(storeId, StoreId(poolId, i.toByte), txid1,
        Right(TxPrepareResponse.Promise(None)), p, TransactionDisposition.VoteCommit, Nil))
    val accepts = net.sentTx.collect { case a: TxAccept => a }
    accepts.map(_.value).distinct shouldBe List(true)
    deliverOwn() // Its accept: held too
    for i <- List(1, 2) do
      store.receiveTransactionMessage(TxAcceptResponse(storeId, StoreId(poolId, i.toByte), txid1, p,
        Right(TxAcceptResponse.Accepted(true))))
    finalizer.created shouldBe 1
    net.sentTx.collect { case r: TxResolved => r.committed }.distinct shouldBe List(true)
    deliverOwn() // Its resolution and finalization: held as well
    fromOwnTx shouldBe Nil
    backend.get(oid1).get.metadata.revision shouldBe rev0

    // Loaded: the held traffic reaches the Tx in order. It promises with its recovered vote, accepts, and applies
    backend.deliver()
    net.sentTx.collect { case r: TxPrepareResponse if r.from == storeId => (r.proposalId, r.disposition, r.response) } shouldBe
      List((p, TransactionDisposition.VoteCommit, Right(TxPrepareResponse.Promise(None))))
    net.sentTx.collect { case r: TxAcceptResponse if r.from == storeId => r.response } shouldBe
      List(Right(TxAcceptResponse.Accepted(true)))
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid1)
    // The stub finalizer completed at once: the driver's TxFinalized, held with the rest, then concluded the Tx
    store.frontend.transactions.contains(txid1) shouldBe false
  }

  test("a recovered vote whose first lookup finds no entry is driven as usual, without a proof") {
    val (store, net, _, lookup, _) = recoveredVoteStore()
    lookup.answer(foundTxid, lookup.absent)
    store.heartbeat()
    store.hasTransactions shouldBe true

    // A later lookup follows an abort, which reaches the store's driver too: the driver resolves on it, so a proof
    // handed to it later starts no round
    store.receiveTransactionMessage(TxResolved(storeId, StoreId(poolId, 1), foundTxid, committed = false,
      late = false, proof = false))
    lookup.requests.count(_ == foundTxid) shouldBe 2 // The Tx holds the abort for a fresh lookup
    // The driver finalized the abort at once
    net.sentTx.collect { case f: TxFinalized if f.from == storeId => f.committed }.distinct shouldBe List(false)

    lookup.answer(foundTxid, lookup.own)
    store.frontend.transactions(foundTxid).status shouldBe TransactionStatus.Committed
  }

