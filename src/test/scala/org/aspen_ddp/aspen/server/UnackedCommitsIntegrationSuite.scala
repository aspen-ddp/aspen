package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.TestTransactions

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.transaction.UnackedCommitEntry
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.{TxFinalized, TxMessage, TxResolved}
import org.aspen_ddp.aspen.common.paxos.PersistentState
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, KeyValueObjectPointer,
  ObjectId, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.repair.{RepairPolicy, StoreRepairer}
import org.aspen_ddp.aspen.server.store.{UnackedCommitLookup, UnackedLookupResult}

import java.nio.{ByteBuffer, ByteOrder}
import scala.concurrent.Future
import scala.util.{Success, Try}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

/** A store that voted commit, crashed before learning the outcome, and is restarted after the transaction was
  * finalized and forgotten (its status cache entry gone) still applies the committed update, from the pool's
  * UnackedCommits tree. Proof-of-commit cannot help here: once the status cache has forgotten the transaction, it is
  * past every store's forget floor, so no peer creates the Tx a proof needs. Such a Tx then ends by expiry.
  *
  * The bootstrap host records its stores' CRL state, so restarting a store recovers its commit vote, and its recovery
  * drivers send. Its heartbeat is short so the status cache expires quickly. Its time (heartbeats and driver timers)
  * runs only while a test pumps with `clock`: until the restart, the held vote must not get a driver, which would
  * learn the outcome from the status cache.
  *
  * A recovering store that never accepted the commit, driving the transaction with no store that did, gets no
  * accepted value from Paxos: the other stores forgot it. Before the transaction horizon its driver decided abort on
  * their fresh votes, dropping the update; now they refuse it, and without the UnackedCommits lookup the Tx would be
  * discarded by expiry, which waits for that lookup to find no entry.
  */
class UnackedCommitsIntegrationSuite extends IntegrationTestSuite with org.scalatest.ParallelTestExecution:

  override def testIda: IDA = ReedSolomon(5, 3, 4)

  private val unknownIncarnationTest =
    "a recovered commit vote whose UnackedCommits entry names no incarnation applies it under Replication(3, 2)"

  /** t = r: one lost holder of a committed update leaves it unreadable unless its owed voter applies it */
  override def testIdaFor(name: String): IDA =
    if name == unknownIncarnationTest then Replication(3, 2) else testIda

  /** Holds the host's UnackedCommits lookups while `holding`, for the test to answer: through the real lookup, or
    * with a given result. Each is answered through the Frontend's own `answer`, as the host's lookup answers
    */
  class HoldingLookup(inner: UnackedCommitLookup) extends UnackedCommitLookup:
    final class Held(val storeId: StoreId, val transactionId: TransactionId, val requestId: Long,
                     val answer: Try[UnackedLookupResult] => Unit):
      def release(): Unit = inner.lookup(storeId, transactionId, requestId, answer)
    @volatile var holding = false
    @volatile var pending: List[Held] = Nil
    @volatile var made: Int = 0
    /** Stores whose found entries are answered as naming no incarnation, as a takeover driver's marker writes them */
    @volatile var unknownIncarnation: Set[StoreId] = Set()
    def lookup(storeId: StoreId, transactionId: TransactionId, requestId: Long,
               reply: Try[UnackedLookupResult] => Unit): Unit =
      made += 1
      val answer: Try[UnackedLookupResult] => Unit =
        if unknownIncarnation.contains(storeId) then
          {
            case Success(UnackedLookupResult.Found(_)) => reply(Success(UnackedLookupResult.Found(None)))
            case other => reply(other)
          }
        else reply
      if holding then pending = pending :+ Held(storeId, transactionId, requestId, answer)
      else inner.lookup(storeId, transactionId, requestId, answer)

  @volatile private var lookups: HoldingLookup = scala.compiletime.uninitialized

  /** A short horizon, so the status cache forgets a transaction (horizon + MaxStartLead + 8 heartbeats: 7.4 s) and
    * expiry starts (horizon + 3.8 s margin) quickly. Each test needs only that its transaction is finalized, forgotten
    * and past every store's floor when its stores restart; until then they get no heartbeat, so nothing expires early.
    * Not below about 2 s: the setup transactions' prepares must reach every store within the horizon under load
    */
  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(300, MILLISECONDS), recordCrl = true, recoveryDrivers = true,
      transactionHorizon = Duration(3, SECONDS),
      wrapUnackedLookup = Some(inner => { lookups = new HoldingLookup(inner); lookups }))

  private val twoStoresTest = "two stores recovering commit votes for a committed TKVL delete both apply it"

  private val k = Key(Array[Byte](1))

  private def sid(i: Int): StoreId = net.stores(i).storeId

  /** Stores standing in for crashed ones: they get no heartbeat, so the commit vote they hold gets no driver */
  @volatile private var down: Set[StoreId] = Set()

  private def deadline(): Long = System.nanoTime() + Duration(30, SECONDS).toNanos

  /** One step of the host: its time too with `clock` (TestNetwork.heartbeat), otherwise its events only */
  private def step(clock: Boolean): Unit =
    if clock then net.heartbeat(except = down) else net.handleEvents()

  private def pump[T](f: Future[T], clock: Boolean, until: Long = deadline()): Future[T] =
    if f.isCompleted then
      f
    else if System.nanoTime() > until then
      net.printTransactionStatus()
      Future.failed(new AssertionError("Timed out pumping events"))
    else
      Future { Thread.sleep(1); step(clock) }.flatMap(_ => pump(f, clock, until))

  /** Polls `cond`, pumping the host meanwhile */
  private def eventually(what: String, clock: Boolean, until: Long = deadline())(cond: => Future[Boolean]): Future[Unit] =
    cond.flatMap: ok =>
      if ok then
        Future.unit
      else if System.nanoTime() > until then
        net.printTransactionStatus()
        Future.failed(new AssertionError(s"Never happened: $what"))
      else
        Future { Thread.sleep(1); step(clock) }.flatMap(_ => eventually(what, clock, until)(cond))

  /** Completes once no host drives a transaction */
  private def settle(clock: Boolean): Future[Unit] =
    net.handleEvents()
    eventually("transactions completed", clock)(Future.successful(!net.hasTransactions))

  private def storeKeys(i: Int, ptr: KeyValueObjectPointer): Set[Key] =
    net.stores(i).m.get(ptr.id).flatMap(_.kvState).map(_.content.keySet).getOrElse(Set())

  private def storeRevision(i: Int, ptr: DataObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(ptr.id).map(_.metadata.revision)

  /** Commits the transaction `build` makes. Every store votes on it, but the stores of `holders` never hear its
    * outcome (TxResolved and TxFinalized to them are dropped), so each keeps its commit vote unresolved, as a store
    * that crashed right after voting does. They are reported offline meanwhile, so none of them leads it. The drop
    * stays in place until restartAfterForgotten. The stores of `silent` get none of its messages and send none: they
    * never see it, and are reported offline too, so none of them is chosen to lead it.
    */
  private def commitHeldBy(holders: Set[StoreId], silent: Set[StoreId] = Set())
                          (build: Transaction => Future[Unit]): Future[TransactionId] =
    val tx = TestTransactions.replaySafe(client.newTransaction())
    net.offlineStores = holders ++ silent
    def outcome(m: TxMessage) = m.isInstanceOf[TxResolved] || m.isInstanceOf[TxFinalized]
    net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id &&
      (silent.contains(m.to) || silent.contains(m.from) || (holders.contains(m.to) && outcome(m)))
    for
      _ <- build(tx)
      _ <- pump(tx.commit(), clock = false)
      // The finalizer marks the holders, which sent no TxCommitted, and its own transactions complete
      _ <- settle(clock = false)
    yield
      net.offlineStores = Set()
      tx.id

  /** Store `i` crashed after saving its commit vote on `txid` and before accepting the commit: its recovered Paxos
    * acceptor holds no accepted value. In a real pool the other four stores' accepts decide the commit; here the
    * designated leader's own store gets the client's prepare only after the resolution (lockstep delivery order), so
    * the commit needed store `i`'s accept, which is erased afterwards. Once the other stores have forgotten the
    * transaction, what every store holds is the same either way
    */
  private def neverAccepted(i: Int, txid: TransactionId): Unit =
    net.crl.asInstanceOf[TestNetwork.RecordingCRL].rewrite(sid(i), txid)(_.copy(paxosAcceptorState =
      PersistentState.initial))

  /** Waits until the host has forgotten `txid` (its status cache entry expired), then restarts `restarted`,
    * crash-only: each recovers its commit vote from the CRL. The stores of `stillDown` stay crashed: no message of
    * the transaction reaches them or leaves them, and they get no heartbeat
    */
  private def restartAfterForgotten(txid: TransactionId, restarted: List[Int],
                                    stillDown: Set[Int] = Set()): Future[Unit] =
    for
      _ <- eventually("the status cache forgot the transaction", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(txid).isEmpty)
      _ <- restart(txid, restarted, stillDown)
    yield ()

  private def restart(txid: TransactionId, restarted: List[Int], stillDown: Set[Int]): Future[Unit] =
    down = stillDown.map(sid)
    net.dropTxMessage = m => m.transactionId == txid && (down.contains(m.to) || down.contains(m.from))
    pump(Future.sequence(restarted.map(i => net.restartStore(net.stores(i)))), clock = true).map(_ => ())

  private def errorKey(i: Int, objectId: ObjectId): Key =
    val bb = ByteBuffer.allocate(17)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(i.toByte)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(bb.array())

  /** The stores of `stores` with an ErrorTree entry for `objectId` */
  private def errorEntries(stores: List[Int], objectId: ObjectId): Future[List[Int]] =
    for
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      found <- Future.sequence(stores.map(i => pool.errorTree.get(errorKey(i, objectId)).map(o => i -> o.isDefined)))
    yield found.collect { case (i, true) => i }

  /** The pool's UnackedCommits entries for `txid`, by pool index. Only those: the system's own transactions write
    * entries too when a store misses one under load
    */
  private def unackedEntries(txid: TransactionId): Future[Map[Int, UnackedCommitEntry]] =
    for
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      all <- pool.unackedCommitsTree.foldLeft(Map[Key, UnackedCommitEntry]()): (acc, contents) =>
        acc ++ contents.map((key, vs) => key -> UnackedCommitEntry.decode(vs.value.bytes))
    yield all.collect:
      case (key, entry) if UnackedCommitEntry.decodeTransactionId(key) == txid => key.bytes(0).toInt -> entry

  /** A KeyValueObjects TKVL in the bootstrap pool holding `k`, with its single (tier 0) node */
  private def treeHoldingK(): Future[(TieredKeyValueList, KeyValueObjectPointer)] =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      tx0 = TestTransactions.replaySafe(client.newTransaction())
      ptr <- pool.allocator.allocateKeyValueObject()(using tx0)
      _ = tx0.exclusiveLockRevision(radicle, ikvos.revision)
      _ <- pump(tx0.commit(), clock = false)
      tx1 = TestTransactions.replaySafe(client.newTransaction())
      froot <- KVObjectRootManager.createNewTree(client, ptr, Key(Array[Byte](0)), ByteArrayKeyOrdering,
        new SinglePoolNodeAllocator(client, Radicle.poolId), Map())(using tx1)
      _ <- pump(tx1.commit(), clock = false)
      root <- froot
      tree <- root.getTree()
      tx2 = client.newTransaction()
      _ <- tree.set(k, Value(Array[Byte](7)))(using tx2)
      _ <- pump(tx2.commit(), clock = false)
      _ <- settle(clock = false)
      (tier, _, onode) <- root.getFreshRootNode()
    yield
      tier shouldBe 0
      val nodePtr = onode.get.pointer.asInstanceOf[KeyValueObjectPointer]
      withClue("every store holds k before the scenario: "):
        (0 until 5).foreach(i => storeKeys(i, nodePtr) should contain (k))
      (tree, nodePtr)

  test(twoStoresTest):
    // Store 2 voted commit but never accepted; store 3 accepted too. Store 2 recovers first, while store 3 is still
    // down, so no store it hears from holds the accepted commit. Each applies the delete from its UnackedCommits entry
    // and would wait for a TxFinalized: past the horizon its peers refuse its driver (their forget floor), so it ends by
    // expiry. Its peers answer its status query "unknown" (store 3, still down, is silent: a possible holder), more than
    // width - writeThreshold of them, so no quorum can ever decide T: its update applied, it concludes as finalized
    // with an applied stub, and its driver is stopped. Store 3 does the same once restarted (store 2 now forgot T)
    for
      (tree, nodePtr) <- treeHoldingK()
      txid <- commitHeldBy(Set(sid(2), sid(3)))(tx => tree.delete(k)(using tx))
      _ = neverAccepted(2, txid)
      marked <- unackedEntries(txid)
      _ = withClue("only the holders lack the delete, and each has a UnackedCommits entry: "):
            (0 until 5).foreach: i =>
              if i == 2 || i == 3 then storeKeys(i, nodePtr) should contain (k)
              else storeKeys(i, nodePtr) should not contain k
            // The designated leader's store can get one too: when the client's prepare reaches it after the
            // resolution, it applies the commit only at TxFinalized, too late to send TxCommitted in time
            marked.keySet should contain allElementsOf List(2, 3)
            marked.values.foreach(_.objects.map(_.id) should contain (nodePtr.id))
      _ <- restartAfterForgotten(txid, List(2), stillDown = Set(3))
      // Applied by the recovered transaction itself: no repair has run yet
      _ <- eventually("store 2 applied the delete", clock = true):
             Future.successful(!storeKeys(2, nodePtr).contains(k))
      _ <- settle(clock = true)
      _ <- restart(txid, List(3), stillDown = Set())
      _ <- eventually("store 3 applied the delete", clock = true):
             Future.successful(!storeKeys(3, nodePtr).contains(k))
      _ <- settle(clock = true)
      repairer = StoreRepairer(client, net.smgr)
      allStores = (0 until 5).toList
      _ <- eventually("every entry about the delete was repaired away", clock = true):
             for
               _ <- allStores.foldLeft(Future.unit): (f, i) =>
                      f.flatMap(_ => pump(repairer.scan(sid(i), RepairPolicy.Default), clock = true)).map(_ => ())
               _ <- settle(clock = true)
               unacked <- unackedEntries(txid)
               errors <- errorEntries(allStores, nodePtr.id)
             yield unacked.isEmpty && errors.isEmpty
      value <- tree.get(k)
    yield
      (0 until 5).foreach(i => storeKeys(i, nodePtr) should not contain k)
      value shouldBe None

  test("a recovered commit vote for a committed data overwrite is proven and applied"):
    for
      dataPtr <- client.transactUntilSuccessful: tx =>
                   given Transaction = tx
                   tx.replaySafe("test")
                   client.getStoragePool(Radicle.poolId).flatMap(_.allocator.allocateDataObject(DataBuffer(Array[Byte](1))))
      _ <- settle(clock = false)
      before <- client.read(dataPtr)
      // Store 2 voted commit but never accepted, so its recovery driver finds no accepted value
      txid <- commitHeldBy(Set(sid(2))): tx =>
                tx.overwrite(dataPtr, before.revision, DataBuffer(Array[Byte](2)))
                Future.unit
      _ = neverAccepted(2, txid)
      marked <- unackedEntries(txid)
      _ = withClue("only store 2 lacks the overwrite, and store 2 owes the transaction, so it has an UnackedCommits entry: "):
            storeRevision(2, dataPtr) shouldBe Some(before.revision)
            List(0, 1, 3, 4).foreach(i => storeRevision(i, dataPtr) shouldBe Some(ObjectRevision(txid)))
            marked.keySet should contain (2)
      _ <- restartAfterForgotten(txid, List(2))
      // Applied from store 2's own UnackedCommits entry, found as it recovers: no driver takes part. The other stores
      // no longer know the transaction and are past their forget floor for it, so they refuse every prepare of it and
      // prove nothing (proofs need a Tx). The test ends before the Tx, idle for Host.txHeartbeatTimeout, gets a
      // driver; past the horizon that Tx ends by expiry (see "two stores recovering commit votes")
      _ <- eventually("store 2 applied the overwrite", clock = true):
             Future.successful(storeRevision(2, dataPtr).contains(ObjectRevision(txid)))
      _ <- settle(clock = true)
      after <- client.read(dataPtr)
    yield
      (0 until 5).foreach(i => storeRevision(i, dataPtr) shouldBe Some(ObjectRevision(txid)))
      after.revision shouldBe ObjectRevision(txid)
      after.data.getByteArray.toSeq shouldBe Seq[Byte](2)

  // transaction-recovery.md section 7, entries naming no incarnation. A driver that did not count the votes (a takeover
  // after the original driver died before its marker wrote) gives a store it never heard from an entry naming no
  // incarnation. Here store 2 cast a counted, saved commit vote, store 1 never saw the transaction, and only store 0
  // applied it: with t = r = 2 the update is readable only if store 2 applies it from its entry. The lookup reports
  // store 2's entry as naming no incarnation, which is what the takeover's marker writes; the stores' state is the same
  // either way.
  test(unknownIncarnationTest):
    for
      dataPtr <- client.transactUntilSuccessful: tx =>
                   given Transaction = tx
                   tx.replaySafe("test")
                   client.getStoragePool(Radicle.poolId).flatMap(_.allocator.allocateDataObject(DataBuffer(Array[Byte](1))))
      _ <- settle(clock = false)
      before <- client.read(dataPtr)
      txid <- commitHeldBy(Set(sid(2)), silent = Set(sid(1))): tx =>
                tx.overwrite(dataPtr, before.revision, DataBuffer(Array[Byte](2)))
                Future.unit
      _ = neverAccepted(2, txid)
      marked <- unackedEntries(txid)
      _ = withClue("only store 0 holds the overwrite, and store 2 owes it: "):
            storeRevision(0, dataPtr) shouldBe Some(ObjectRevision(txid))
            storeRevision(1, dataPtr) shouldBe Some(before.revision)
            storeRevision(2, dataPtr) shouldBe Some(before.revision)
            marked.keySet shouldBe Set(2)
      _ = lookups.unknownIncarnation = Set(sid(2))
      _ <- restartAfterForgotten(txid, List(2))
      _ <- eventually("store 2 applied the overwrite", clock = true):
             Future.successful(storeRevision(2, dataPtr).contains(ObjectRevision(txid)))
      _ <- settle(clock = true)
      after <- client.read(dataPtr)
    yield
      after.revision shouldBe ObjectRevision(txid)
      after.data.getByteArray.toSeq shouldBe Seq[Byte](2)

  private def frontendOf(i: Int) = net.smgr.control.router.loaded.get(sid(i)).map(_.frontend)

  test("a recovered commit vote superseded before its crash quarantines the object until repair verifies it " +
       "(Req F5, Todo 2.16)"):
    // Store 2 applies T from its UnackedCommits entry and T2, built on T, supersedes it. Store 2 then crashes with T's
    // CRL state still held (restored below): the recovered vote's re-check fails, as for a state invalidated under its
    // locks. Rather than fail-stop, or count with neither lock nor update, the store quarantines the object until the
    // host's repair service verifies its copy against a read of the pool at least as new as T
    val crl = net.crl.asInstanceOf[TestNetwork.RecordingCRL]
    for
      dataPtr <- client.transactUntilSuccessful: tx =>
                   given Transaction = tx
                   tx.replaySafe("test")
                   client.getStoragePool(Radicle.poolId).flatMap(_.allocator.allocateDataObject(DataBuffer(Array[Byte](1))))
      _ <- settle(clock = false)
      before <- client.read(dataPtr)
      txid <- commitHeldBy(Set(sid(2))): tx =>
                tx.overwrite(dataPtr, before.revision, DataBuffer(Array[Byte](2)))
                Future.unit
      _ = neverAccepted(2, txid)
      held = crl.getFullRecoveryState(sid(2)).find(_.txd.transactionId == txid).get
      _ <- restartAfterForgotten(txid, List(2))
      _ <- eventually("store 2 applied T", clock = true):
             Future.successful(storeRevision(2, dataPtr).contains(ObjectRevision(txid)))
      _ <- settle(clock = true)
      // A store refuses transactions started within MaxStartLead of its load
      _ <- eventually("store 2 joins new transactions", clock = true)(Future.successful(net.loadFloorPassed(sid(2))))
      tx2 = TestTransactions.replaySafe(client.newTransaction())
      _ = tx2.overwrite(dataPtr, ObjectRevision(txid), DataBuffer(Array[Byte](3)))
      _ <- pump(tx2.commit(), clock = true)
      _ <- settle(clock = true)
      _ <- eventually("every store applied T2", clock = true):
             Future.successful((0 until 5).forall(i => storeRevision(i, dataPtr).contains(ObjectRevision(tx2.id))))
      _ <- settle(clock = true)
      // A crash while T's state is still in the CRL
      _ = crl.save(txid, held, () => ())
      _ = crl.deleteAppliedStub(sid(2), txid)
      _ <- pump(net.restartStore(net.stores(2)), clock = true)
      _ <- eventually("store 2 quarantined the object", clock = true):
             Future.successful(frontendOf(2).exists(_.testingOnlyQuarantines > 0))
      _ <- eventually("repair verified the object and the quarantine cleared", clock = true):
             Future.successful(frontendOf(2).exists(f => !f.isQuarantined(dataPtr.id)))
      _ <- eventually("T's CRL state concluded", clock = true):
             Future.successful(!crl.getFullRecoveryState(sid(2)).exists(_.txd.transactionId == txid))
      _ <- eventually("store 2 joins new transactions again", clock = true):
             Future.successful(net.loadFloorPassed(sid(2)))
      tx3 = TestTransactions.replaySafe(client.newTransaction())
      _ = tx3.overwrite(dataPtr, ObjectRevision(tx2.id), DataBuffer(Array[Byte](4)))
      _ <- pump(tx3.commit(), clock = true)
      _ <- eventually("every store applied T3", clock = true):
             Future.successful((0 until 5).forall(i => storeRevision(i, dataPtr).contains(ObjectRevision(tx3.id))))
      _ <- settle(clock = true)
    yield
      (0 until 5).foreach(i => storeRevision(i, dataPtr) shouldBe Some(ObjectRevision(tx3.id)))
      frontendOf(2).exists(_.failStopped) shouldBe false

  test("a UnackedCommits lookup answer for a replaced store is not taken by its successor"):
    for
      (tree, nodePtr) <- treeHoldingK()
      txid <- commitHeldBy(Set(sid(2)))(tx => tree.delete(k)(using tx))
      _ = lookups.holding = true
      _ <- restartAfterForgotten(txid, List(2))
      _ <- eventually("the first store's lookup", clock = false)(Future.successful(lookups.pending.size == 1))
      // Restarted again before its lookup is answered: the replacement makes its own
      _ <- pump(net.restartStore(net.stores(2)), clock = false)
      _ <- eventually("the replacement's lookup", clock = false)(Future.successful(lookups.pending.size == 2))
      List(first, second) = lookups.pending: @unchecked
      _ = first.requestId shouldBe second.requestId
      // The first store's answer, that the entry is there, arrives after it was replaced
      _ = first.answer(scala.util.Success(UnackedLookupResult.Found(Some(StoreIncarnation.default(sid(2))))))
      _ = net.handleEvents()
      _ = withClue("the replacement does not take its predecessor's answer: "):
            storeKeys(2, nodePtr) should contain (k)
      _ = second.release()
      _ <- eventually("the replacement applied the delete on its own answer", clock = false):
             Future.successful(!storeKeys(2, nodePtr).contains(k))
    yield succeed

  test("a UnackedCommits lookup that fails is made again at the store heartbeat, and then applies"):
    for
      (tree, nodePtr) <- treeHoldingK()
      txid <- commitHeldBy(Set(sid(2)))(tx => tree.delete(k)(using tx))
      _ = lookups.holding = true
      _ <- restartAfterForgotten(txid, List(2))
      _ <- eventually("the recovered store's lookup", clock = true)(Future.successful(lookups.pending.size == 1))
      _ = lookups.holding = false
      _ = lookups.pending.head.answer(scala.util.Failure(new java.io.IOException("injected lookup failure")))
      _ = net.handleEvents()
      _ = storeKeys(2, nodePtr) should contain (k)
      // The heartbeat makes another, through the host's real lookup, which finds the entry
      _ <- eventually("store 2 applied the delete after a heartbeat retry", clock = true):
             Future.successful(!storeKeys(2, nodePtr).contains(k))
    yield
      lookups.made should be >= 2
