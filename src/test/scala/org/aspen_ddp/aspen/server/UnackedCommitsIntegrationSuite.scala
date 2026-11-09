package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.transaction.UnackedCommitEntry
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.network.{TxFinalized, TxMessage, TxResolved}
import org.aspen_ddp.aspen.common.paxos.PersistentState
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, KeyValueObjectPointer,
  ObjectId, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.repair.{RepairPolicy, StoreRepairer}
import org.aspen_ddp.aspen.server.store.UnackedCommitLookup

import java.nio.{ByteBuffer, ByteOrder}
import scala.concurrent.Future
import scala.util.Try
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

/** A store that voted commit, crashed before learning the outcome, and is restarted after the transaction was
  * finalized and forgotten (its status cache entry gone) still applies the committed update: from the pool's
  * UnackedCommits tree for any change, and through proof-of-commit as well for one that leaves a trace.
  *
  * The bootstrap host records its stores' CRL state, so restarting a store recovers its commit vote, and its recovery
  * drivers send. Its heartbeat is short so the status cache expires quickly. Its time (heartbeats and driver timers)
  * runs only while a test pumps with `clock`: until the restart, the held vote must not get a driver, which would
  * learn the outcome from the status cache.
  *
  * A recovering store that never accepted the commit, driving the transaction with no store that did, gets no
  * accepted value from Paxos: the other stores forgot it. Without the UnackedCommits lookup or proof-of-commit,
  * its driver decides abort on their votes, and it drops the update.
  */
class UnackedCommitsIntegrationSuite extends IntegrationTestSuite:

  override def testIda: IDA = ReedSolomon(5, 3, 4)

  /** Holds the host's UnackedCommits lookups while `holding`, for the test to answer: through the real lookup, or
    * with a given result. Each is answered through the Frontend's own `answer`, as the host's lookup answers
    */
  class HoldingLookup(inner: UnackedCommitLookup) extends UnackedCommitLookup:
    final class Held(val storeId: StoreId, val transactionId: TransactionId, val requestId: Long,
                     val answer: Try[Boolean] => Unit):
      def release(): Unit = inner.lookup(storeId, transactionId, requestId, answer)
    @volatile var holding = false
    @volatile var pending: List[Held] = Nil
    @volatile var made: Int = 0
    def lookup(storeId: StoreId, transactionId: TransactionId, requestId: Long, answer: Try[Boolean] => Unit): Unit =
      made += 1
      if holding then pending = pending :+ Held(storeId, transactionId, requestId, answer)
      else inner.lookup(storeId, transactionId, requestId, answer)

  @volatile private var lookups: HoldingLookup = scala.compiletime.uninitialized

  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(300, MILLISECONDS), recordCrl = true, recoveryDrivers = true,
      wrapUnackedLookup = Some(inner => { lookups = new HoldingLookup(inner); lookups }))

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
    * stays in place until restartAfterForgotten.
    */
  private def commitHeldBy(holders: Set[StoreId])(build: Transaction => Future[Unit]): Future[TransactionId] =
    val tx = client.newTransaction()
    net.offlineStores = holders
    net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id && holders.contains(m.to) &&
      (m.isInstanceOf[TxResolved] || m.isInstanceOf[TxFinalized])
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
      tx0 = client.newTransaction()
      ptr <- pool.allocator.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- pump(tx0.commit(), clock = false)
      tx1 = client.newTransaction()
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

  test("two stores recovering commit votes for a committed TKVL delete both apply it"):
    // Store 2 voted commit but never accepted; store 3 accepted too. Store 2 recovers first, while store 3 is still
    // down, so no store it hears from holds the accepted commit
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
      // Its recovery driver starts once the Tx is idle for Host.txHeartbeatTimeout. The other stores no longer know the
      // transaction, and prove its commit from the revision they show
      _ <- eventually("store 2 applied the overwrite", clock = true):
             Future.successful(storeRevision(2, dataPtr).contains(ObjectRevision(txid)))
      _ <- settle(clock = true)
      after <- client.read(dataPtr)
    yield
      (0 until 5).foreach(i => storeRevision(i, dataPtr) shouldBe Some(ObjectRevision(txid)))
      after.revision shouldBe ObjectRevision(txid)
      after.data.getByteArray.toSeq shouldBe Seq[Byte](2)

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
      _ = first.answer(scala.util.Success(true))
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
