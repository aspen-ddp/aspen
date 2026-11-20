package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.{TestTransactions, Transaction}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.client.internal.transaction.UnackedCommitEntry
import org.aspen_ddp.aspen.common.network.{TxCommitted, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, Insert, Key, KeyValueObjectPointer, ObjectId,
  ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionDisposition, TransactionId,
  TransactionStatus}
import org.aspen_ddp.aspen.server.repair.{RepairPolicy, StoreRepairer}

import java.nio.{ByteBuffer, ByteOrder}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

/** A transaction's client prepares, re-delivered after every store finalized and forgot it, must not apply it again.
  *
  * The bootstrap host records its stores' CRL state and has recovery drivers, as UnackedCommitsIntegrationSuite's
  * does. Its heartbeat and transaction horizon are short so the status cache forgets a transaction quickly (7.4 s), and
  * expiry starts soon (horizon + 3.8 s). Since the forget floor, every replay here comes after the cache forgot the
  * transaction, so past every store's floor, and is refused outright; each test still checks that nothing is applied
  * again. The tests need only that their transactions' prepares reach every store within the horizon, which the test
  * thread's own deliveries do within milliseconds. Not below about 2 s, for slack under load
  */
class ReplayIntegrationSuite extends IntegrationTestSuite with org.scalatest.ParallelTestExecution:

  override def testIda: IDA = ReedSolomon(5, 3, 4)

  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(300, MILLISECONDS), recordCrl = true, recoveryDrivers = true,
      transactionHorizon = Duration(3, SECONDS))

  /** The prefer-abort test waits out the missing-data wait of every store its recovery driver prepares: a short one
    * keeps it fast. Other tests keep the default, which no step of theirs should reach
    */
  private val reDrivenAbortTest = "an aborted data-less transaction re-driven by a recovered commit vote aborts again"

  /** Needs a leader driver that sends prepares of its own (TestNetwork.HostOptions.leaderRetransmits) */
  private val driverFirstTest =
    "a store that gets the driver's prepare before the client's withholds its vote, then checks and commits"

  override def testHostOptionsFor(name: String): TestNetwork.HostOptions =
    if name == reDrivenAbortTest then
      testHostOptions.copy(missingDataWait = Duration(200, MILLISECONDS))
    else if name == driverFirstTest then
      testHostOptions.copy(leaderRetransmits = true)
    else
      testHostOptions

  private val k = Key(Array[Byte](1))

  private def deadline(): Long = System.nanoTime() + Duration(30, SECONDS).toNanos

  /** One step of the host: its time too with `clock` (TestNetwork.heartbeat), otherwise its events only */
  private def step(clock: Boolean): Unit =
    if clock then net.heartbeat() else net.handleEvents()

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

  /** Records the client prepares of the transaction `build` makes and commits it */
  private def commitRecorded(build: Transaction => Future[Unit]): Future[TransactionId] =
    val tx = client.newTransaction()
    net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.isInstanceOf[TxPrepare]
    for
      _ <- build(tx)
      _ <- pump(tx.commit(), clock = false)
      _ <- settle(clock = false)
    yield
      net.recordTxMessage = _ => false
      tx.id

  @volatile private var recorded: List[TxMessage] = Nil

  /** Waits until the host has forgotten `txid` (its status cache entry expired, so every store forgot it), then
    * delivers again every client prepare of it recorded
    */
  private def replayClientPrepares(txid: TransactionId,
                                   order: List[TxPrepare] => List[TxPrepare] = leaderLast): Future[Unit] =
    for
      _ <- eventually("the status cache forgot the transaction", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(txid).isEmpty)
    yield
      recorded = recorded ++ net.takeRecordedTxMessages()
      val prepares = recorded.collect:
        case m: TxPrepare if m.fromClient && m.transactionId == txid => m
      assert(prepares.nonEmpty, s"no client prepare of $txid was recorded")
      order(prepares).foreach(net.deliverTransactionMessage)

  private def isLeader(m: TxPrepare): Boolean =
    m.to.poolId == m.txd.primaryObject.poolId && m.to.poolIndex == m.txd.designatedLeaderUID

  /** The designated leader's last: its prepare starts its driver, which here never retransmits
    * (TestNetwork.HostOptions.recoveryDrivers). A store that gets the driver's data-less prepare before its own client
    * prepare withholds its vote for it and answers only a later prepare (Tx.awaitingClientPrepare), so a driver that
    * never asks again would wait forever. A host's drivers retransmit
    */
  private def leaderLast(prepares: List[TxPrepare]): List[TxPrepare] =
    val (leader, others) = prepares.partition(isLeader)
    others ++ leader

  /** Replays `txid`'s client prepares (replayClientPrepares) and checks that every store refused them: the replay
    * comes after the status cache forgot the transaction, which it outlasts the horizon by, so every store is past its
    * forget floor (Frontend.forgetFloorMillis). No store creates a Tx, and nothing of the transaction is sent
    */
  private def replayRefused(txid: TransactionId): Future[Unit] =
    net.recordTxMessage = (m: TxMessage) => m.transactionId == txid &&
      !(m match { case p: TxPrepare => p.fromClient; case _ => false })
    for
      _ <- replayClientPrepares(txid)
    yield
      val refused = net.stores.forall: b =>
        net.smgr.control.router.loaded.get(b.storeId).forall(!_.frontend.transactions.contains(txid))
      net.recordTxMessage = _ => false
      val sent = net.takeRecordedTxMessages()
      withClue("every store refused the replay: no Tx, nothing sent: "):
        refused shouldBe true
        sent shouldBe Nil

  /** Completes once no store of the bootstrap pool holds a Tx for `txid` */
  private def forgottenByStores(txid: TransactionId): Future[Unit] =
    eventually("every store forgot the transaction", clock = true):
      Future.successful(net.stores.forall: b =>
        net.smgr.control.router.loaded.get(b.storeId).forall(!_.frontend.transactions.contains(txid)))

  /** What store `i` holds for key `k` of `ptr`: the revision of its value */
  private def storeKeyRevision(i: Int, ptr: KeyValueObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(ptr.id).flatMap(_.kvState).flatMap(_.content.get(k)).map(_.revision)

  private def sid(i: Int): StoreId = net.stores(i).storeId

  private def storeRevision(i: Int, ptr: DataObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(ptr.id).map(_.metadata.revision)

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

  /** A data object x and a KeyValue object y, with `yContent`, every store holding both */
  private def allocateXY(yContent: Map[Key, Value]): Future[(DataObjectPointer, KeyValueObjectPointer)] =
    for
      pool <- client.getStoragePool(Radicle.poolId)
      xy <- client.transactUntilSuccessful: tx =>
              given Transaction = tx
              tx.replaySafe("test")
              for
                x <- pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
                y <- pool.allocator.allocateKeyValueObject(yContent)
              yield (x, y)
      _ <- settle(clock = false)
    yield xy

  test("a replayed pinned transaction does not re-apply its delete over a re-inserted key"):
    for
      pool <- client.getStoragePool(Radicle.poolId)
      (x, y) <- client.transactUntilSuccessful: tx =>
                  given Transaction = tx
                  tx.replaySafe("test")
                  for
                    x <- pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
                    y <- pool.allocator.allocateKeyValueObject(Map(k -> Value(Array[Byte](1))))
                  yield (x, y)
      _ <- settle(clock = false)
      xBefore <- client.read(x)
      yBefore <- client.read(y)
      // T: the overwrite of x at its revision is a pin, and so is the delete of k from y at k's revision. (A blind
      // delete, the original subject here, is now refused by the builder: BlindKeyValueWrite)
      t <- commitRecorded: tx =>
             tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
             tx.update(y, None, None, List(KeyValueUpdate.KeyRevision(k, yBefore.contents(k).revision)), List(Delete(k)))
             Future.unit
      yAfterT <- client.read(y)
      _ = yAfterT.contents.keySet should not contain k
      // T2: k is inserted again
      t2 <- client.transactUntilSuccessful: tx =>
              tx.replaySafe("test")
              tx.update(y, None, None, List(KeyValueUpdate.DoesNotExist(k)), List(Insert(k, Array[Byte](3))))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      _ <- replayRefused(t)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      yAfterReplay <- client.read(y)
    yield
      // Every store refuses the replay, so no store applies T's delete again over T2's k. Before the forget floor,
      // a store taking the replay proved T committed from x's revision (Tx.proveCommitted), and the replay's driver
      // resolved it as a late commit, owed to no store
      withClue("every store of y still holds k as T2 wrote it: "):
        (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      yAfterReplay.contents.get(k).map(_.value.bytes.toSeq) shouldBe Some(Seq[Byte](3))

  test("a replayed pinned KV insert after a later delete of its key changes nothing"):
    for
      (x, y) <- allocateXY(Map())
      xBefore <- client.read(x)
      // T: the overwrite of x at its revision is the pin; the insert of k into y is checked by DoesNotExist only
      t <- commitRecorded: tx =>
             tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
             tx.update(y, None, None, List(KeyValueUpdate.DoesNotExist(k)), List(Insert(k, Array[Byte](2))))
             Future.unit
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t)))
      // T2: k is deleted, at T's revision of it
      t2 <- client.transactUntilSuccessful: tx =>
              tx.update(y, None, None, List(KeyValueUpdate.KeyRevision(k, ObjectRevision(t))), List(Delete(k)))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe None)
      _ <- replayRefused(t)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      yAfterReplay <- client.read(y)
      xAfterReplay <- client.read(x)
    yield
      // Every store refuses the replay and applies nothing. Before the forget floor, checked afresh, T failed x's
      // revision, and every store proved it committed from x and applied nothing
      withClue("no store of y holds k again: "):
        (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe None)
      (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(t)))
      yAfterReplay.contents.keySet should not contain k
      xAfterReplay.revision shouldBe ObjectRevision(t)

  test("a replayed pinned data overwrite after a later overwrite changes nothing"):
    for
      (x, _) <- allocateXY(Map())
      xBefore <- client.read(x)
      t <- commitRecorded: tx =>
             tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
             Future.unit
      t2 <- client.transactUntilSuccessful: tx =>
              tx.overwrite(x, ObjectRevision(t), DataBuffer(Array[Byte](3)))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(t2)))
      _ <- replayRefused(t)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      xAfterReplay <- client.read(x)
    yield
      // Every store refuses the replay. Before the forget floor, checked afresh, T failed x's revision and no object
      // showed its update: every store voted abort, and the replay aborted a committed transaction (paxos-transactions
      // 10.10)
      withClue("every store of x still holds T2's overwrite: "):
        (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(t2)))
      xAfterReplay.revision shouldBe ObjectRevision(t2)
      xAfterReplay.data.getByteArray.toSeq shouldBe Seq[Byte](3)

  test("a store that missed T's prepare and applied a later write to the key skips T's stale checked write"):
    val lagging = 4
    for
      (x, y) <- allocateXY(Map(k -> Value(Array[Byte](1))))
      yBefore <- client.read(y)
      rev0 = yBefore.contents(k).revision
      xBefore <- client.read(x)
      // T: k's overwrite is per-write checked (KeyRevision), which pins it. The lagging store gets none of T's
      // messages (they are recorded) and is reported offline, so it does not lead
      t <- {
        val tx = client.newTransaction()
        net.offlineStores = Set(sid(lagging))
        net.droppedTx = Set((sid(lagging), tx.id))
        net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.to == sid(lagging)
        tx.update(y, None, None, List(KeyValueUpdate.KeyRevision(k, rev0)), List(Insert(k, Array[Byte](2))))
        for
          _ <- pump(tx.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.recordTxMessage = _ => false
          net.offlineStores = Set()
          tx.id
      }
      marked <- errorEntries((0 until 5).toList, y.id)
      _ = withClue("only the lagging store lacks T, and the finalizer marked it: "):
            (0 until 5).foreach: i =>
              if i == lagging then storeKeyRevision(i, y) shouldBe Some(rev0)
              else storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t))
            // The designated leader's store can be marked too: when the client's prepare reaches it after the
            // resolution, it applies the commit only at TxFinalized, too late to send TxCommitted in time
            marked should contain (lagging)
      // T2: a write of k checked only by Exists, pinned by x, which every store applies, the lagging one included
      t2 <- client.transactUntilSuccessful: tx =>
              tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](3)))
              tx.update(y, None, None, List(KeyValueUpdate.Exists(k)), List(Insert(k, Array[Byte](3))))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      // Once the host forgot T, its client prepare and then its outcome reach the lagging store late
      _ <- eventually("the status cache forgot T", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(t).isEmpty)
      late = net.takeRecordedTxMessages()
      prepare = late.collect { case m: TxPrepare if m.fromClient => m }
      outcome = late.collect { case m: TxResolved if m.committed => m }.take(1) ++
                late.collect { case m: TxFinalized if m.committed => m }.take(1)
      _ = withClue("T's client prepare and committed outcome to the lagging store were recorded: "):
            prepare should not be empty
            outcome.size shouldBe 2
      _ = net.droppedTx = Set()
      _ = prepare.foreach(net.deliverTransactionMessage)
      // T is now past the lagging store's forget floor (the status cache outlasts the horizon), so it refuses the late
      // prepare outright: it never joins T, and its stale checked write is never applied (Frontend.forgetFloorMillis)
      _ = withClue("the lagging store refuses T's late prepare: "):
            net.smgr.control.router.loaded.get(sid(lagging)).exists(_.frontend.transactions.contains(t)) shouldBe false
      _ = outcome.foreach(net.deliverTransactionMessage)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      _ = withClue("the lagging store applied the commit's checked write only if k was still as T read it: "):
            storeKeyRevision(lagging, y) shouldBe Some(ObjectRevision(t2))
      stillMarked <- errorEntries((0 until 5).toList, y.id)
      _ = stillMarked should contain (lagging)
      repairer = StoreRepairer(client, net.smgr)
      // Every store's entries: the designated leader's may be marked too
      _ <- eventually("every entry about y was repaired away", clock = true):
             for
               _ <- (0 until 5).foldLeft(Future.unit): (f, i) =>
                      f.flatMap(_ => pump(repairer.scan(sid(i), RepairPolicy.Default), clock = true)).map(_ => ())
               _ <- settle(clock = true)
               errors <- errorEntries((0 until 5).toList, y.id)
             yield errors.isEmpty
      yAfter <- client.read(y)
    yield
      (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      yAfter.contents.get(k).map(_.value.bytes.toSeq) shouldBe Some(Seq[Byte](3))

  // paxos-transactions 6.2: a store that gets a driver's data-less prepare before its client's prepare (prefer-abort,
  // Tx.awaitingClientPrepare). Since the forget floor a replay is refused outright, so this order arises end to end
  // only in a live transaction, whose client prepare to a store is slower than the leader driver's. Here the leader's
  // driver retransmits, as a host's does (HostOptions.leaderRetransmits), so its data-less prepares reach stores 3 and 4
  test(driverFirstTest):
    val held = Set(3, 4).map(sid)
    for
      (x, _) <- allocateXY(Map())
      xBefore <- client.read(x)
      tx = client.newTransaction()
      // The client's prepares to stores 3 and 4 are held back; the leader's driver prepares still reach them. They are
      // reported offline, so neither leads. Without them only 3 stores vote, below the write threshold of 4
      isClientPrepareToHeld = (m: TxMessage) => m.transactionId == tx.id && held.contains(m.to) &&
                                (m match { case p: TxPrepare => p.fromClient; case _ => false })
      _ = net.offlineStores = held
      _ = net.dropTxMessage = isClientPrepareToHeld
      _ = net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id &&
            ((held.contains(m.to) && m.isInstanceOf[TxPrepare]) ||
             (held.contains(m.from) && m.isInstanceOf[TxPrepareResponse]))
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      commit = tx.commit()
      heldTx = () => held.forall(s => net.smgr.control.router.loaded.get(s).exists(_.frontend.transactions.contains(tx.id)))
      _ <- eventually("the driver's prepare created a Tx at stores 3 and 4", clock = true)(Future.successful(heldTx()))
      _ = net.handleEvents()
      before = net.takeRecordedTxMessages()
      _ = withClue("stores 3 and 4 got only the driver's data-less prepares, answered none, and T is undecided: "):
            before.collect { case p: TxPrepare if !p.fromClient => p.to }.toSet shouldBe held
            before.collect { case p: TxPrepare if !p.fromClient => p.objectUpdates }.flatten shouldBe Nil
            before.collect { case r: TxPrepareResponse => r } shouldBe Nil
            commit.isCompleted shouldBe false
      clientPrepares = before.collect { case p: TxPrepare if p.fromClient => p }
      _ = withClue("the client's prepares to stores 3 and 4 were held: ")(clientPrepares.map(_.to).toSet shouldBe held)
      _ = net.dropTxMessage = _ => false
      _ = clientPrepares.foreach(net.deliverTransactionMessage)
      _ <- pump(commit, clock = true)
      _ <- settle(clock = true)
      after = { net.recordTxMessage = _ => false; net.offlineStores = Set(); net.takeRecordedTxMessages() }
      xAfter <- client.read(x)
    yield
      // The first held store to get the client's prepare checked the requirements, as usual, and answered the leader's
      // driver with a commit vote, the fourth of the write threshold: the driver's prepare that overtook the client's
      // did not make it vote abort. The second may answer only after T resolved, with no vote (Undetermined)
      val votes = after.collect { case r: TxPrepareResponse => r.from -> r.disposition }
      withClue("the first held store voted commit once the client's prepare arrived, and neither voted abort: "):
        votes should contain (clientPrepares.head.to -> TransactionDisposition.VoteCommit)
        votes.map(_._2) should not contain TransactionDisposition.VoteAbort
      withClue("every store applied T: "):
        (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(tx.id)))
      xAfter.revision shouldBe ObjectRevision(tx.id)
      xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](2)

  test(reDrivenAbortTest):
    val voter = 1
    val blocked = Set(3, 4).map(sid)
    // Whether `m` is an outcome of `txid` (TxResolved or TxFinalized) sent to one of `to`
    def outcomeTo(txid: TransactionId, to: Set[StoreId])(m: TxMessage): Boolean =
      m.transactionId == txid && to.contains(m.to) && (m.isInstanceOf[TxResolved] || m.isInstanceOf[TxFinalized])
    for
      (x, _) <- allocateXY(Map())
      xBefore <- client.read(x)
      // T0 exclusively locks x and commits, but stores 3 and 4 never hear its outcome yet, so they keep its lock. They
      // are reported offline meanwhile, so neither leads it
      t0Outcomes <- {
        val tx = TestTransactions.replaySafe(client.newTransaction())
        net.offlineStores = blocked
        net.dropTxMessage = outcomeTo(tx.id, blocked)
        net.recordTxMessage = outcomeTo(tx.id, blocked)
        tx.exclusiveLockRevision(x, xBefore.revision)
        for
          _ <- pump(tx.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.recordTxMessage = _ => false
          net.takeRecordedTxMessages()
      }
      // T: a data-less refcount change of x. Stores 3 and 4 vote abort on the collision with T0's lock, so T aborts.
      // Store 1 votes commit and never hears the outcome, as a store that crashed right after voting does. The client's
      // prepares to stores 3 and 4 are held until store 1 has voted: on the host's shared status cache, a prepare
      // reaching store 1 after the abort would be answered from it, with no vote
      tx = TestTransactions.replaySafe(client.newTransaction())
      _ = net.offlineStores = blocked + sid(voter)
      _ = net.dropTxMessage = outcomeTo(tx.id, Set(sid(voter)))
      _ = net.droppedTx = blocked.map(_ -> tx.id)
      _ = net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && blocked.contains(m.to) &&
            (m match { case p: TxPrepare => p.fromClient; case _ => false })
      _ = tx.setRefcount(x, xBefore.refcount, xBefore.refcount.increment())
      commit = tx.commit()
      voterVote = () => net.crl.getFullRecoveryState(sid(voter)).find(_.txd.transactionId == tx.id)
                    .map(s => (s.disposition, s.status))
      _ <- eventually("store 1 voted on T", clock = false)(Future.successful(voterVote().nonEmpty))
      _ = net.droppedTx = Set()
      _ = net.recordTxMessage = _ => false
      _ = net.takeRecordedTxMessages().foreach(net.deliverTransactionMessage)
      aborted <- pump(commit, clock = false).transform(r => scala.util.Success(r.isFailure))
      _ <- settle(clock = false)
      _ = withClue("T aborted, and store 1 holds its unresolved commit vote: "):
            aborted shouldBe true
            voterVote() shouldBe Some((TransactionDisposition.VoteCommit, TransactionStatus.Unresolved))
      // The collision clears: T0's outcome reaches stores 3 and 4
      _ = net.offlineStores = Set()
      _ = withClue("T0's outcomes to stores 3 and 4 were recorded: ")(t0Outcomes should not be empty)
      _ = t0Outcomes.foreach(net.deliverTransactionMessage)
      _ <- eventually("the status cache forgot T", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(tx.id).isEmpty)
      _ = (0 until 5).filter(_ != voter).foreach: i =>
            net.smgr.control.router.loaded.get(sid(i)).exists(_.frontend.transactions.contains(tx.id)) shouldBe false
      // Store 1 restarts crash-only and recovers its commit vote. Its recovery driver re-prepares T to stores that
      // forgot it, and whose requirement on x now holds again: they are past their forget floor for T, so they refuse
      // it, and T cannot commit. Store 1's Tx ends by expiry: its peers answer its status query "unknown", more than
      // width - writeThreshold of the pool; its UnackedCommits lookup finds no entry (T aborted), so it is discarded and
      // its driver stopped. Before the horizon, prefer-abort made the re-driven T abort again; the outcome is the same
      _ = net.dropTxMessage = _ => false
      _ <- pump(net.restartStore(net.stores(voter)), clock = true)
      _ <- forgottenByStores(tx.id)
      _ <- settle(clock = true)
      xAfter <- client.read(x)
    yield
      withClue("no store applied T's refcount change: "):
        (0 until 5).foreach(i => net.stores(i).m(x.id).metadata.refcount shouldBe xBefore.refcount)
      xAfter.refcount shouldBe xBefore.refcount

  test("a replay at a pool whose stores all acknowledged late leaves every store unchanged"):
    // Pool A (the bootstrap pool) holds T's pin, x; pool B (the initial metadata pool) only T's DoesNotExist insert of
    // k into y, which nothing pins there. Every pool-B store applies T but acknowledges it too late, so the finalizer
    // writes each an UnackedCommits entry and lists it in TxFinalized's owed: each writes an applied stub
    val poolB = PoolId.InitialMetadataPoolId
    val bStores = net.metadataStores
    def bKeyRevision(i: Int, ptr: KeyValueObjectPointer): Option[ObjectRevision] =
      bStores(i).m.get(ptr.id).flatMap(_.kvState).flatMap(_.content.get(k)).map(_.revision)
    def bFrontend(i: Int) = net.smgr.control.router.loaded.get(bStores(i).storeId).map(_.frontend)
    for
      poolA <- client.getStoragePool(Radicle.poolId)
      pb <- client.getStoragePool(poolB)
      (x, y) <- client.transactUntilSuccessful: tx =>
                  given Transaction = tx
                  tx.replaySafe("test")
                  for
                    x <- poolA.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
                    y <- pb.allocator.allocateKeyValueObject(Map())
                  yield (x, y)
      _ <- settle(clock = false)
      xBefore <- client.read(x)
      t <- {
        val tx = client.newTransaction()
        net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.isInstanceOf[TxPrepare]
        // Never acknowledged in time: as a TxCommitted that arrives after errorTimeout
        net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.isInstanceOf[TxCommitted] &&
          m.from.poolId == poolB
        tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
        tx.update(y, None, None, List(KeyValueUpdate.DoesNotExist(k)), List(Insert(k, Array[Byte](2))))
        for
          _ <- pump(tx.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.recordTxMessage = _ => false
          net.dropTxMessage = _ => false
          tx.id
      }
      entries <- pb.unackedCommitsTree.foldLeft(Set[Int]()): (acc, contents) =>
                   acc ++ contents.keysIterator.filter(key => UnackedCommitEntry.decodeTransactionId(key) == t)
                     .map(_.bytes(0).toInt)
      _ = withClue("every pool-B store applied T, and a write threshold of them is marked owed: "):
            // The designated leader's store may get the client's prepare only after the resolution (lockstep delivery
            // order): its vote is then not counted, so it is owed nothing
            entries.size should be >= 4
            (0 until 5).foreach(i => bKeyRevision(i, y) shouldBe Some(ObjectRevision(t)))
      // T2: k is deleted, at T's revision of it
      _ <- client.transactUntilSuccessful: tx =>
             tx.update(y, None, None, List(KeyValueUpdate.KeyRevision(k, ObjectRevision(t))), List(Delete(k)))
             Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => bKeyRevision(i, y) shouldBe None)
      // The replay. Before the forget floor, every pool-B store's DoesNotExist(k) passed again and it voted commit, and
      // pool A proved T committed from x, so the replay's driver resolved late. The order is kept from then: pool B's
      // prepares first, the designated leader's (a pool-B store: its primary object is y) last of them, then pool A's
      // The replay's outcomes, sent once its prepares are delivered (the prepares recorded so far are T's own)
      _ = net.recordTxMessage = (m: TxMessage) => m.transactionId == t &&
            (m.isInstanceOf[TxFinalized] || m.isInstanceOf[TxResolved])
      _ <- replayClientPrepares(t, prepares =>
             val (b, a) = prepares.partition(_.to.poolId == poolB)
             val (leader, others) = b.partition(isLeader)
             withClue("T's designated leader is a pool-B store: ")(leader should not be empty)
             others ++ leader ++ a)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      _ <- eventually("every pool-B store forgot the transaction", clock = true):
             Future.successful((0 until 5).forall(i => bFrontend(i).forall(!_.transactions.contains(t))))
      outcomes = { net.recordTxMessage = _ => false; net.takeRecordedTxMessages() }
      yAfterReplay <- client.read(y)
    yield
      // The replay comes after the status cache forgot T, so T is past every store's forget floor (the cache outlasts
      // the horizon): every store refuses it, the designated leader starts no driver, and nothing is decided again.
      // Before the floor it resolved as a late commit through each pool-B store's stub
      withClue("the replay was refused everywhere, so no outcome was sent: "):
        outcomes.collect { case m: TxFinalized => m } shouldBe Nil
        outcomes.collect { case m: TxResolved => m } shouldBe Nil
      withClue("no pool-B store applied T again: "):
        (0 until 5).foreach(i => bKeyRevision(i, y) shouldBe None)
      yAfterReplay.contents.keySet should not contain k
      entries.forall(i => bFrontend(i).exists(_.hasAppliedStub(t))) shouldBe true

  // transaction-recovery 5.4: a second driver's host crashing after its marker handed
  // entries off and before its TxFinalized reached the stores leaves entries with no stub, possibly at a whole pool.
  // The single-host TestNetwork cannot run that driver on another host; this builds the state its crash leaves (every
  // store applied and forgot T, holds an entry and no stub), which is what decides the outcome. Under the forget floor
  // no store that forgot T rejoins it: inside its status cache's memory a replayed prepare is ignored, and past it
  // refused, so no fresh Tx can look its entry up and apply T again
  test("a replay at a pool whose stores hold entries and no stubs applies nothing, inside and past the caches"):
    val poolB = PoolId.InitialMetadataPoolId
    val bStores = net.metadataStores
    def bKeyRevision(i: Int, ptr: KeyValueObjectPointer): Option[ObjectRevision] =
      bStores(i).m.get(ptr.id).flatMap(_.kvState).flatMap(_.content.get(k)).map(_.revision)
    def bFrontend(i: Int) = net.smgr.control.router.loaded.get(bStores(i).storeId).map(_.frontend)
    def bTxCreated(t: TransactionId): Boolean = (0 until 5).exists(i => bFrontend(i).exists(_.transactions.contains(t)))
    for
      poolA <- client.getStoragePool(Radicle.poolId)
      pb <- client.getStoragePool(poolB)
      (x, y) <- client.transactUntilSuccessful: tx =>
                  given Transaction = tx
                  tx.replaySafe("test")
                  for
                    x <- poolA.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
                    y <- pb.allocator.allocateKeyValueObject(Map())
                  yield (x, y)
      _ <- settle(clock = false)
      xBefore <- client.read(x)
      t <- {
        val tx = client.newTransaction()
        net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.isInstanceOf[TxPrepare]
        // Never acknowledged in time, so the finalizer writes each pool-B store an entry
        net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.isInstanceOf[TxCommitted] &&
          m.from.poolId == poolB
        tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
        tx.update(y, None, None, List(KeyValueUpdate.DoesNotExist(k)), List(Insert(k, Array[Byte](2))))
        for
          _ <- pump(tx.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.recordTxMessage = _ => false
          net.dropTxMessage = _ => false
          tx.id
      }
      entries <- pb.unackedCommitsTree.foldLeft(Set[Int]()): (acc, contents) =>
                   acc ++ contents.keysIterator.filter(key => UnackedCommitEntry.decodeTransactionId(key) == t)
                     .map(_.bytes(0).toInt)
      _ = withClue("every pool-B store applied T, and a write threshold of them has an entry: "):
            entries.size should be >= 4
            (0 until 5).foreach(i => bKeyRevision(i, y) shouldBe Some(ObjectRevision(t)))
      // The second driver's lost TxFinalized: the stores hold their entries and no stub
      _ = (0 until 5).foreach(i => net.smgr.control.router.post(bStores(i).storeId)(_.frontend.dropAppliedStub(t)))
      _ <- settle(clock = false)
      _ = withClue("no pool-B store holds a stub for T: "):
            (0 until 5).foreach(i => bFrontend(i).exists(_.hasAppliedStub(t)) shouldBe false)
      // T2: k is deleted, at T's revision of it
      _ <- client.transactUntilSuccessful: tx =>
             tx.update(y, None, None, List(KeyValueUpdate.KeyRevision(k, ObjectRevision(t))), List(Delete(k)))
             Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => bKeyRevision(i, y) shouldBe None)
      // Replay 1, while the host's status cache still holds T finalized: every store ignores the prepare
      _ = withClue("the host still remembers T as finalized: "):
            net.smgr.control.txStatusCache.getStatus(t).exists(_.finalized) shouldBe true
      prepares = net.takeRecordedTxMessages().collect { case m: TxPrepare if m.fromClient && m.transactionId == t => m }
      _ = withClue("T's client prepares were recorded: ")(prepares should not be empty)
      _ = {
        val (b, a) = prepares.partition(_.to.poolId == poolB)
        val (leader, others) = b.partition(isLeader)
        (others ++ leader ++ a).foreach(net.deliverTransactionMessage)
        net.handleEvents()
      }
      createdInsideCache = bTxCreated(t)
      _ <- settle(clock = true)
      _ = withClue("inside the caches' memory no pool-B store created a Tx or applied T again: "):
            createdInsideCache shouldBe false
            (0 until 5).foreach(i => bKeyRevision(i, y) shouldBe None)
      // Replay 2, once the cache forgot T: past every store's forget floor, refused
      _ <- eventually("the status cache forgot the transaction", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(t).isEmpty)
      _ = {
        val (b, a) = prepares.partition(_.to.poolId == poolB)
        val (leader, others) = b.partition(isLeader)
        (others ++ leader ++ a).foreach(net.deliverTransactionMessage)
        net.handleEvents()
      }
      createdPastCache = bTxCreated(t)
      _ <- settle(clock = true)
      yAfter <- client.read(y)
    yield
      withClue("past the caches no pool-B store created a Tx either: ")(createdPastCache shouldBe false)
      withClue("no pool-B store applied T again: "):
        (0 until 5).foreach(i => bKeyRevision(i, y) shouldBe None)
      yAfter.contents.keySet should not contain k
