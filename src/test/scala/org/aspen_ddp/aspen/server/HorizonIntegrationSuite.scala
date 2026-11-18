package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.{TestTransactions, Transaction, TransactionAborted, TransactionOutcomeUnknown}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.network.{ClientResponse, TransactionFinalized, TransactionResolved, TransactionVote,
  TxFinalized, TxMessage, TxPrepare, TxResolved, TxStatusRequest}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.Frontend
import org.aspen_ddp.aspen.server.store.backend.MapBackend

import org.scalatest.Assertion

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, SECONDS}
import scala.util.{Failure, Success, Try}

/** The transaction horizon end to end (design docs/superpowers/specs/2026-11-13-transaction-horizon-design.md, tests
  * 1-6 and 9): the forget floor, the client's give-up and expiry, on the TestNetwork's lockstep hosts.
  *
  * The horizon is short, so the status cache forgets a transaction, and expiry starts, within seconds. Each test needs
  * only that a transaction's prepares reach its stores within the horizon of its start, and the test's own deliveries
  * do so on the test's thread within milliseconds: 2 s leaves the slack of a long scheduling stall. The bootstrap host
  * records its stores' CRL state, and its non-leader stores drive idle transactions (recovery drivers), as
  * ReplayIntegrationSuite's does. A second host (TestNetwork.newHost) stands in for one with its own status cache: one
  * a store was transferred to, or one restarted.
  */
class HorizonIntegrationSuite extends IntegrationTestSuite with org.scalatest.ParallelTestExecution:

  override def testIda: IDA = ReedSolomon(5, 3, 4)

  private val horizon: FiniteDuration = Duration(2, SECONDS)

  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(100, MILLISECONDS), recordCrl = true, recoveryDrivers = true,
      transactionHorizon = horizon)

  private val nearHorizonTest = "a store first reached near the horizon after finalization learns the outcome from " +
    "its peers' caches"

  /** How long before the horizon the near-horizon test's late store is first reached */
  private val nearHorizonLead: FiniteDuration = Duration(1200, MILLISECONDS)

  override def testHostOptionsFor(name: String): TestNetwork.HostOptions =
    if name == nearHorizonTest then
      // An idle Tx self-drives after txHeartbeatTimeout (5 heartbeats): 1.5 s here, longer than nearHorizonLead, so the
      // late store's driver asks its peers only once they are past the horizon
      testHostOptions.copy(heartbeatPeriod = Duration(300, MILLISECONDS))
    else
      testHostOptions

  private def deadline(): Long = System.nanoTime() + Duration(30, SECONDS).toNanos

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

  /** `f`'s outcome, once it completes */
  private def outcome[T](f: Future[T], clock: Boolean): Future[Try[T]] =
    pump(f, clock).transform(r => Success(r))

  /** Polls `cond`, pumping the hosts meanwhile */
  private def eventually(what: String, clock: Boolean)(cond: => Boolean): Future[Unit] =
    val until = deadline()
    def loop(): Future[Unit] =
      if cond then
        Future.unit
      else if System.nanoTime() > until then
        net.printTransactionStatus()
        Future.failed(new AssertionError(s"Never happened: $what"))
      else
        Future { Thread.sleep(1); step(clock) }.flatMap(_ => loop())
    loop()

  /** Completes once no host drives a transaction */
  private def settle(clock: Boolean): Future[Unit] =
    eventually("transactions completed", clock)(!net.hasTransactions)

  private def sid(i: Int): StoreId = net.stores(i).storeId

  /** Store `i`'s Frontend, on whichever host serves it */
  private def frontend(i: Int): Option[Frontend] =
    net.hostFor(sid(i)).control.router.loaded.get(sid(i)).map(_.frontend)

  private def holds(i: Int, txid: TransactionId): Boolean = frontend(i).exists(_.transactions.contains(txid))

  private def holders(txid: TransactionId): Set[Int] = (0 until 5).filter(holds(_, txid)).toSet

  private def crlStores(txid: TransactionId): Set[Int] =
    (0 until 5).filter(i => net.crl.getFullRecoveryState(sid(i)).exists(_.txd.transactionId == txid)).toSet

  private def cachedStatus(txid: TransactionId): Option[TransactionStatus.Value] =
    net.smgr.control.txStatusCache.getStatus(txid).map(_.status)

  private def storeRevision(i: Int, x: DataObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(x.id).map(_.metadata.revision)

  private def isClientPrepare(txid: TransactionId)(m: TxMessage): Boolean = m match
    case p: TxPrepare => p.fromClient && p.transactionId == txid
    case _ => false

  private def isOutcome(txid: TransactionId)(m: TxMessage): Boolean =
    m.transactionId == txid && (m.isInstanceOf[TxResolved] || m.isInstanceOf[TxFinalized])

  /** Everything that tells the client T's outcome: the leader's messages, and the stores' votes */
  private def isClientOutcome(txid: TransactionId)(r: ClientResponse): Boolean = r match
    case m: TransactionResolved => m.transactionId == txid
    case m: TransactionFinalized => m.transactionId == txid
    case m: TransactionVote => m.transactionId == txid
    case _ => false

  private def recordedClientPrepares(txid: TransactionId): List[TxPrepare] =
    net.takeRecordedTxMessages().collect { case p: TxPrepare if isClientPrepare(txid)(p) => p }

  private def isLeader(m: TxPrepare): Boolean =
    m.to.poolId == m.txd.primaryObject.poolId && m.to.poolIndex == m.txd.designatedLeaderUID

  /** The designated leader's last, as a client's batch arrives (ReplayIntegrationSuite.leaderLast) */
  private def leaderLast(prepares: List[TxPrepare]): List[TxPrepare] =
    val (leader, others) = prepares.partition(isLeader)
    others ++ leader

  private def wallMillis(): Long = System.currentTimeMillis()

  /** A data object every store holds */
  private def allocateX(): Future[DataObjectPointer] =
    for
      pool <- client.getStoragePool(Radicle.poolId)
      x <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             tx.replaySafe("test")
             pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
      _ <- settle(clock = false)
    yield x

  /** Commits a pinned overwrite of `x` at `revision`, recording its client prepares */
  private def commitRecorded(x: DataObjectPointer, revision: ObjectRevision): Future[(TransactionId, List[TxPrepare])] =
    val tx = client.newTransaction()
    net.recordTxMessage = isClientPrepare(tx.id)
    tx.overwrite(x, revision, DataBuffer(Array[Byte](2)))
    for
      _ <- pump(tx.commit(), clock = false)
      _ <- settle(clock = false)
    yield
      net.recordTxMessage = _ => false
      (tx.id, recordedClientPrepares(tx.id))

  /** Runs `attempt` until it is not spoiled, at most `attempts` times. An attempt yields None when a scheduling stall
    * pushed a step meant to happen inside T's horizon past it, which the attempt checks after the step (the clock read
    * afterwards is past start + horizon): its outcome then shows nothing, and a fresh attempt, on a fresh object and
    * transaction, runs instead. Only that explanation retries; any other unexpected outcome fails the attempt
    */
  private def retrySpoiled(what: String, attempts: Int = SpoiledAttempts, n: Int = 0)
                          (attempt: Int => Future[Option[Assertion]]): Future[Assertion] =
    attempt(n).flatMap:
      case Some(result) => Future.successful(result)
      case None if n + 1 < attempts => retrySpoiled(what, attempts, n + 1)(attempt)
      case None => Future.failed(new AssertionError(s"every attempt was spoiled by scheduling stalls: $what"))

  /** How many attempts retrySpoiled makes: each is spoiled only by a stall of over a second at one step */
  private val SpoiledAttempts = 5

  /** Whether T, started at `start`, was already past the horizon when this was called */
  private def pastHorizon(start: Long): Boolean = wallMillis() - horizon.toMillis > start

  /** Waits until the hosts' wall clock is past `millis` */
  private def clockPast(millis: Long): Future[Unit] =
    eventually(s"the clock passed $millis", clock = false)(wallMillis() > millis)

  test("a stale retransmit of a finalized traceless commit past the horizon reports unknown, not aborted"):
    for
      x <- allocateX()
      xBefore <- client.read(x)
      // T: a refcount change, which leaves no trace a store could prove its commit from. The client never hears its
      // outcome, as one partitioned from the stores right after sending its prepares
      tx = client.newTransaction()
      _ = net.recordTxMessage = isClientPrepare(tx.id)
      _ = net.dropClientResponse = isClientOutcome(tx.id)
      _ = tx.setRefcount(x, xBefore.refcount, xBefore.refcount.increment())
      commit = tx.commit()
      _ <- eventually("every store applied T", clock = false):
             (0 until 5).forall(i => net.stores(i).m(x.id).metadata.refcount == xBefore.refcount.increment())
      _ <- settle(clock = false)
      prepares = recordedClientPrepares(tx.id)
      _ = withClue("T committed and was finalized at every store, and forgotten by them: "):
            prepares.size shouldBe 5
            holders(tx.id) shouldBe Set()
            cachedStatus(tx.id) shouldBe Some(TransactionStatus.Committed)
            commit.isCompleted shouldBe false
      // Past the horizon plus MaxStartLead the client gives up: it can no longer find out
      result <- outcome(commit, clock = true)
      _ = withClue("the client gives up with outcome unknown: "):
            result should matchPattern { case Failure(_: TransactionOutcomeUnknown) => }
      _ <- eventually("the status cache forgot T", clock = true)(cachedStatus(tx.id).isEmpty)
      // A stale retransmit of T's prepares. Before the horizon the stores re-decided it from scratch: the refcount no
      // longer matches, nothing proves the commit, and T aborted (paxos-transactions 10.10). Now every store is past
      // its forget floor for T and refuses it: no Tx, no vote, no outcome
      _ = net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && !isClientPrepare(tx.id)(m)
      _ = leaderLast(prepares).foreach(net.deliverTransactionMessage)
      _ = withClue("no store joins T again: ")(holders(tx.id) shouldBe Set())
      _ <- settle(clock = true)
      sent = net.takeRecordedTxMessages()
      xAfter <- client.read(x)
    yield
      net.recordTxMessage = _ => false
      withClue("no store sent anything for T's retransmit: ")(sent shouldBe Nil)
      crlStores(tx.id) shouldBe Set()
      xAfter.refcount shouldBe xBefore.refcount.increment()

  test("a delayed client prepare of a forgotten abort does not commit inside or past the horizon"):
    retrySpoiled("the replay inside the horizon")(_ => forgottenAbortReplay())

  /** One attempt of the test above: None if its replay meant for inside the horizon came after it */
  private def forgottenAbortReplay(): Future[Option[Assertion]] =
    val blocked = Set(3, 4).map(sid)
    def outcomeTo(txid: TransactionId)(m: TxMessage): Boolean = isOutcome(txid)(m) && blocked.contains(m.to)
    for
      x <- allocateX()
      xBefore <- client.read(x)
      // T0 exclusively locks x and commits, but stores 3 and 4 do not hear its outcome yet, so they keep its lock. They
      // are reported offline meanwhile, so neither leads. No heartbeat runs until the lock clears, so no idle driver
      // of theirs learns the outcome early
      t0Outcomes <- {
        val t0 = TestTransactions.replaySafe(client.newTransaction())
        net.offlineStores = blocked
        net.dropTxMessage = outcomeTo(t0.id)
        net.recordTxMessage = outcomeTo(t0.id)
        t0.exclusiveLockRevision(x, xBefore.revision)
        for
          _ <- pump(t0.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.recordTxMessage = _ => false
          net.takeRecordedTxMessages()
      }
      // T: a pinned overwrite of x. Stores 3 and 4 vote abort on the collision with T0's lock, so T aborts
      tx = client.newTransaction()
      _ = net.recordTxMessage = isClientPrepare(tx.id)
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      aborted <- outcome(tx.commit(), clock = false)
      _ <- settle(clock = false)
      prepares = recordedClientPrepares(tx.id)
      start = prepares.head.txd.startTimestamp.wallTime
      _ = withClue("T aborted: "):
            aborted should matchPattern { case Failure(_: TransactionAborted) => }
            prepares.size shouldBe 5
      // The collision clears: T0's outcome reaches stores 3 and 4, so T's requirements hold again everywhere
      _ = net.dropTxMessage = _ => false
      _ = net.offlineStores = Set()
      _ = withClue("T0's outcomes to stores 3 and 4 were recorded: ")(t0Outcomes should not be empty)
      _ = t0Outcomes.foreach(net.deliverTransactionMessage)
      _ <- settle(clock = false)
      _ = holders(tx.id) shouldBe Set()
      // The host's last write of T's status-cache entry was before now
      forgottenAt = wallMillis()
      // Inside the horizon, but after the old status cache (7 heartbeats) would have forgotten T, a delayed duplicate
      // of T's client prepares reaches every store. Stores that forgot the abort voted commit afresh, and T committed.
      // Now the cache outlasts the horizon: the stores still know T aborted
      _ <- clockPast(forgottenAt + 8 * testHostOptions.heartbeatPeriod.toMillis)
      known = cachedStatus(tx.id)
      _ = net.recordTxMessage = isOutcome(tx.id)
      _ = leaderLast(prepares).foreach(net.deliverTransactionMessage)
      // Read after the replay was processed: TestNetwork.deliverTransactionMessage drains the host's events
      // (handleEvents) before and after each delivery, and lockstep threading runs the store inline, so each replayed
      // prepare's floor check and the finalized relay's answer ran inside its own call. If T was not past the horizon
      // now, the replay reached every store inside it. Reading it after settle would only add false retries: settle
      // drains follow-on work, which depends on the cache entry, not the floor
      spoiled = pastHorizon(start)
      _ <- settle(clock = true)
      _ <- eventually("every store forgot T again", clock = true)(holders(tx.id).isEmpty)
      inside = net.takeRecordedTxMessages()
      _ = net.recordTxMessage = _ => false
      result <- if spoiled then Future.successful(None) else pastHorizonReplay(x, xBefore.revision, tx.id, prepares,
                  known, inside).map(Some(_))
    yield result

  /** The rest of an attempt whose replay was inside the horizon: its checks, then the replay past the horizon */
  private def pastHorizonReplay(x: DataObjectPointer, revision: ObjectRevision, txid: TransactionId,
                                prepares: List[TxPrepare], known: Option[TransactionStatus.Value],
                                inside: List[TxMessage]): Future[Assertion] =
    withClue("the host still knew T aborted at the replay: ")(known shouldBe Some(TransactionStatus.Aborted))
    withClue("inside the horizon the replay learns the abort, and nothing commits: "):
      inside.collect { case m: TxResolved if !m.committed => m } should not be empty
      inside.collect { case m: TxResolved if m.committed => m } shouldBe Nil
      inside.collect { case m: TxFinalized if m.committed => m } shouldBe Nil
      (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(revision))
    for
      // Past the horizon, once the cache forgot T, the same duplicate is refused everywhere
      _ <- eventually("the status cache forgot T", clock = true)(cachedStatus(txid).isEmpty)
      _ = net.recordTxMessage = (m: TxMessage) => m.transactionId == txid && !isClientPrepare(txid)(m)
      _ = leaderLast(prepares).foreach(net.deliverTransactionMessage)
      _ = withClue("past the horizon no store joins T: ")(holders(txid) shouldBe Set())
      _ <- settle(clock = true)
      past = net.takeRecordedTxMessages()
      xAfter <- client.read(x)
    yield
      net.recordTxMessage = _ => false
      withClue("no store sent anything for the replay past the horizon: ")(past shouldBe Nil)
      crlStores(txid) shouldBe Set()
      (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(revision))
      xAfter.revision shouldBe revision
      xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](1)

  test(nearHorizonTest):
    val late = 4
    val other = net.newHost()
    for
      // An object for each attempt, every store holding it. Allocated first, with the store on the bootstrap host, like
      // every transaction the suite's setup started
      xs <- (0 until SpoiledAttempts).foldLeft(Future.successful(Vector.empty[DataObjectPointer])): (f, _) =>
              f.flatMap(acc => allocateX().map(acc :+ _))
      // The late store on a host of its own, so its status cache knows nothing of T. T starts once the store's load
      // plus MaxStartLead has passed, so T is above its forget floor until the horizon
      _ <- pump(net.moveStore(net.stores(late), other, closeFirst = true), clock = false)
      _ <- eventually("the late store passed its load floor", clock = false)(net.loadFloorPassed(sid(late)))
      result <- retrySpoiled("the late store's prepare inside the horizon")(n => lateStoreLearns(late, other, xs(n)))
    yield result

  /** One attempt of the test above, on `x`: None if the late store's prepare came after the horizon */
  private def lateStoreLearns(late: Int, other: Host, x: DataObjectPointer): Future[Option[Assertion]] =
    for
      xBefore <- client.read(x)
      // T: a pinned overwrite of x, which commits and is finalized without the late store ever hearing of it. It is
      // reported offline meanwhile, so it does not lead
      tx = client.newTransaction()
      _ = net.offlineStores = Set(sid(late))
      _ = net.droppedTx = Set(sid(late) -> tx.id)
      _ = net.recordTxMessage = (m: TxMessage) => isClientPrepare(tx.id)(m) && m.to == sid(late)
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      _ <- pump(tx.commit(), clock = false)
      _ <- settle(clock = false)
      prepare = recordedClientPrepares(tx.id)
      _ = net.recordTxMessage = _ => false
      _ = net.droppedTx = Set()
      _ = net.offlineStores = Set()
      _ = withClue("T's client prepare to the late store was recorded, and T committed elsewhere: "):
            prepare.size shouldBe 1
            (0 until 4).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(tx.id)))
            cachedStatus(tx.id) shouldBe Some(TransactionStatus.Committed)
      start = prepare.head.txd.startTimestamp.wallTime
      // The client's prepare reaches the late store just inside the horizon: the store joins T and votes, and its Tx
      // learns no outcome until its idle driver asks the other stores, after the horizon
      _ <- clockPast(start + horizon.toMillis - nearHorizonLead.toMillis)
      _ = net.deliverTransactionMessage(prepare.head)
      joined = holds(late, tx.id)
      // Read after the delivery: a refusal is explained only if T was past the horizon by then
      spoiled = !joined && pastHorizon(start)
      _ = if !spoiled then withClue("the late store, above its floor, joins T: ")(joined shouldBe true)
      result <- if spoiled then Future.successful(None) else learnsFromCaches(late, other, tx.id, prepare.head, start)
                  .map(Some(_))
    yield result

  /** The rest of an attempt in which the late store joined T inside the horizon */
  private def learnsFromCaches(late: Int, other: Host, txid: TransactionId, prepare: TxPrepare,
                               start: Long): Future[Assertion] =
    for
      _ <- eventually("the late store learned T's outcome", clock = true):
             other.control.txStatusCache.getStatus(txid).exists(_.status != TransactionStatus.Unresolved)
      learnedAt = wallMillis()
      _ <- eventually("the late store forgot T", clock = true)(!holds(late, txid))
      _ <- settle(clock = true)
    yield
      // The other stores were past the horizon when its driver asked, so only their status caches could answer it,
      // which they outlast (horizon + MaxStartLead + 8 heartbeats from their last write)
      withClue("the late store learned the outcome only after the horizon: ")(learnedAt should be > start + horizon.toMillis)
      (0 until 4).foreach(i => frontend(i).exists(_.belowForgetFloor(prepare.txd)) shouldBe true)
      withClue("it learned the commit: "):
        other.control.txStatusCache.getStatus(txid).map(_.status) shouldBe Some(TransactionStatus.Committed)
      crlStores(txid) shouldBe Set()

  test("stuck holders short of a quorum all discard"):
    val absent = Set(3, 4).map(sid)
    for
      x <- allocateX()
      xBefore <- client.read(x)
      // T: a pinned overwrite of x. Stores 3 and 4 hear nothing of it but status requests, so stores 0-2 vote commit
      // and hold it: three holders, short of the write threshold of four, and two non-holders, more than
      // width - writeThreshold = 1. Every holder's driver (the designated leader's, and the idle recovery drivers of
      // the others) is stuck
      tx = client.newTransaction()
      _ = net.offlineStores = absent
      _ = net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id && absent.contains(m.to) &&
            !m.isInstanceOf[TxStatusRequest]
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      commit = tx.commit()
      _ <- eventually("stores 0-2 voted commit on T", clock = false):
             (0 until 3).forall(i => net.crl.getFullRecoveryState(sid(i))
               .exists(s => s.txd.transactionId == tx.id && s.disposition == TransactionDisposition.VoteCommit))
      _ = withClue("only stores 0-2 hold T: ")(holders(tx.id) shouldBe Set(0, 1, 2))
      // The client gives up past the horizon plus MaxStartLead; the holders, past the horizon plus the expiry margin,
      // ask every store. Each holder reports itself a holder; stores 3 and 4, below their floors, report "unknown".
      // So every holder discards T once its UnackedCommits lookup finds no entry; none keeps another alive
      result <- outcome(commit, clock = true)
      _ = withClue("the client gives up with outcome unknown: "):
            result should matchPattern { case Failure(_: TransactionOutcomeUnknown) => }
      _ <- eventually("every holder discarded T", clock = true)(holders(tx.id).isEmpty && crlStores(tx.id).isEmpty)
      _ <- settle(clock = true)
      _ = net.dropTxMessage = _ => false
      _ = net.offlineStores = Set()
      _ = withClue("no store applied T: ")((0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(xBefore.revision)))
      // The holders' locks are released: a new overwrite at the same revision commits at a write threshold of stores
      next = client.newTransaction()
      _ = next.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](3)))
      _ <- pump(next.commit(), clock = false)
      _ <- settle(clock = false)
      xAfter <- client.read(x)
    yield
      xAfter.revision shouldBe ObjectRevision(next.id)
      xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](3)

  test("holders of a chosen commit apply it past expiry though a rebuilt store forgot it"):
    // cross-document-review 2.2: T's commit is chosen by stores 0-3 and reported to the client, but no
    // store learns it: its leader, store 0, alone on its host, loses its host and its CRL before it finalizes. Store
    // 0 is rebuilt, blank, and promoted at once; store 4 never hears of T. Past the horizon stores 1-3, the only
    // holders left, cannot form a quorum and ask their peers. Counting the rebuilt store as a non-holder would make
    // them discard a commit the client was told of. Instead stores report commit as their latest accept, which traces
    // back to a commit-certain tally (L2), and three commit voters hold the update, its read threshold (rule B). So
    // they decide commit, apply it and finalize it (ExpiryTally.Decision.Commit)
    val lost = 0
    val absent = sid(4)
    val hostA = net.newHost()
    val hostB = net.newHost()
    val rebuiltDir = os.temp.dir(deleteOnExit = true)
    for
      x <- allocateX()
      xBefore <- client.read(x)
      // Store 0 on a host of its own, so that host's status cache dies with it. T starts once it joins transactions
      _ <- pump(net.moveStore(net.stores(lost), hostA, closeFirst = true), clock = false)
      _ <- eventually("store 0 passed its load floor", clock = false)(net.loadFloorPassed(sid(lost)))
      lostData = net.stores(lost).m
      tx = client.newTransaction()
      // Store 0 leads; no store hears T's outcome; store 4 hears nothing but status requests; the finalizer never runs
      _ = net.offlineStores = (1 until 5).map(sid).toSet
      _ = net.FinalizerFactory.stalled = _.transactionId == tx.id
      _ = net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id &&
            (isOutcome(tx.id)(m) || (m.to == absent && !m.isInstanceOf[TxStatusRequest]))
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      result <- outcome(tx.commit(), clock = false)
      _ = net.offlineStores = Set()
      _ = withClue("the client was told T committed: ")(result shouldBe a[Success[?]])
      _ = withClue("stores 1-3 hold T unresolved, with commit votes: "):
            (1 until 4).foreach: i =>
              frontend(i).flatMap(_.transactions.get(tx.id)).map(_.status) shouldBe Some(TransactionStatus.Unresolved)
              net.crl.getFullRecoveryState(sid(i)).exists(s => s.txd.transactionId == tx.id &&
                s.disposition == TransactionDisposition.VoteCommit) shouldBe true
      // Store 0's host crashes and its CRL is lost. The rebuild restores its data as the pool holds it, before T, and
      // writes its acceptor-since; the rebuilt store is promoted on another host
      _ <- pump(hostA.control.loader.closeStore(sid(lost), extractCrl = false), clock = false)
      // The stalled finalizer, and the lost outcome messages, died with its host. Store 4 still hears nothing of T
      _ = net.FinalizerFactory.stalled = _ => false
      _ = net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.to == absent &&
            !m.isInstanceOf[TxStatusRequest]
      _ = net.crl.deleteTransaction(sid(lost), tx.id)
      _ = RebuildingStore.writeAcceptorSince(rebuiltDir)
      rebuilt = new MapBackend(sid(lost)):
        override def path: os.Path = rebuiltDir
      _ = rebuilt.m = lostData
      _ <- pump(net.moveStore(rebuilt, hostB, closeFirst = false), clock = false)
      _ = withClue("the rebuilt store is muted for T: ")(frontend(lost).flatMap(_.acceptorSince).isDefined shouldBe true)
      // Past the horizon plus the expiry margin the holders query their peers, decide commit, apply it and finalize it
      _ <- eventually("the holders applied T and forgot it", clock = true):
             (1 until 4).forall(i => storeRevision(i, x).contains(ObjectRevision(tx.id))) &&
               holders(tx.id).isEmpty && crlStores(tx.id).isEmpty
      _ = net.dropTxMessage = _ => false
      _ <- settle(clock = true)
      xAfter <- client.read(x)
    yield
      withClue("the committed update is readable at its read threshold: "):
        xAfter.revision shouldBe ObjectRevision(tx.id)
        xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](2)

  /** Delivers T's client prepare to store `i`, which forgot T, once it is served by `host` and loaded after T started:
    * it is refused there, though the host's status cache knows nothing of T and T is inside the horizon
    */
  private def refusedAfterLoad(i: Int, host: Host, prepare: TxPrepare): Unit =
    val txid = prepare.transactionId
    withClue(s"store $i is loaded on a host that knows nothing of T, after T started: "):
      assert(net.hostFor(sid(i)) eq host)
      host.control.txStatusCache.getStatus(txid) shouldBe None
      frontend(i).map(_.loadedAtMillis).getOrElse(0L) should be > prepare.txd.startTimestamp.wallTime
    net.recordTxMessage = (m: TxMessage) => m.transactionId == txid && m.from == sid(i)
    net.deliverTransactionMessage(prepare)
    net.recordTxMessage = _ => false
    withClue(s"store $i refuses T: no Tx, no CRL state, no reply: "):
      holds(i, txid) shouldBe false
      crlStores(txid) shouldBe Set()
      net.takeRecordedTxMessages() shouldBe Nil

  test("a host restart does not let its stores rejoin a transaction they forgot"):
    val moved = 4
    for
      x <- allocateX()
      xBefore <- client.read(x)
      first = net.newHost()
      _ <- pump(net.moveStore(net.stores(moved), first, closeFirst = true), clock = false)
      // T starts once store 4 joins new transactions there
      _ <- eventually("store 4 passed its load floor", clock = false)(net.loadFloorPassed(sid(moved)))
      (t, prepares) <- commitRecorded(x, xBefore.revision)
      prepare = prepares.filter(_.to == sid(moved))
      _ = withClue("store 4 committed T and forgot it; its host remembers the outcome: "):
            prepare.size shouldBe 1
            storeRevision(moved, x) shouldBe Some(ObjectRevision(t))
            holds(moved, t) shouldBe false
            first.control.txStatusCache.getStatus(t).map(_.status) shouldBe Some(TransactionStatus.Committed)
      // Its host restarts crash-only, losing its status cache: the store is loaded again, after T started, on a new
      // host. Its load raises its forget floor past T's start
      _ <- clockPast(prepare.head.txd.startTimestamp.wallTime)
      second = net.newHost()
      _ <- pump(net.moveStore(net.stores(moved), second, closeFirst = false), clock = false)
    yield
      refusedAfterLoad(moved, second, prepare.head)
      storeRevision(moved, x) shouldBe Some(ObjectRevision(t))

  test("a store transferred to another host does not rejoin a transaction it forgot"):
    val moved = 4
    for
      x <- allocateX()
      xBefore <- client.read(x)
      (t, prepares) <- commitRecorded(x, xBefore.revision)
      prepare = prepares.filter(_.to == sid(moved))
      _ = withClue("store 4 committed T and forgot it: "):
            prepare.size shouldBe 1
            storeRevision(moved, x) shouldBe Some(ObjectRevision(t))
            holds(moved, t) shouldBe false
      // The store is transferred: closed on the bootstrap host, whose status cache still knows T, and loaded on another
      // host, after T started. Its load there raises its forget floor past T's start
      _ <- clockPast(prepare.head.txd.startTimestamp.wallTime)
      other = net.newHost()
      _ <- pump(net.moveStore(net.stores(moved), other, closeFirst = true), clock = false)
    yield
      cachedStatus(t) shouldBe Some(TransactionStatus.Committed)
      refusedAfterLoad(moved, other, prepare.head)
      storeRevision(moved, x) shouldBe Some(ObjectRevision(t))

  /** Commits a pinned overwrite of `o` at `revision` and waits for it to settle */
  private def overwrite(o: DataObjectPointer, revision: ObjectRevision): Future[TransactionId] =
    val tx = client.newTransaction()
    tx.overwrite(o, revision, DataBuffer(Array[Byte](2)))
    for
      _ <- pump(tx.commit(), clock = false)
      _ <- settle(clock = false)
    yield tx.id

  /** Serves each of `moved` on a host of its own, once each joins new transactions there */
  private def ownHosts(moved: List[Int]): Future[Map[Int, Host]] =
    val hosts = moved.map(i => i -> net.newHost()).toMap
    for
      _ <- moved.foldLeft(Future.unit): (f, i) =>
             f.flatMap(_ => pump(net.moveStore(net.stores(i), hosts(i), closeFirst = true), clock = false))
      _ <- eventually("the moved stores passed their load floors", clock = false)(moved.forall(i => net.loadFloorPassed(sid(i))))
    yield hosts

  // paxos-transactions 10.10 (join ceiling): a host's join ceiling (JoinCeiling) bounds what its stores joined before
  // it went down, so once restarted they join what started since, rather than refusing everything started before their
  // load plus MaxStartLead. Without it both tests fail: the restarted stores refuse the transaction

  test("a restarted host's stores join a transaction started while it was down, which needs them to commit"):
    for
      x <- allocateX()
      xBefore <- client.read(x)
      host = net.newHost()
      _ <- pump(net.moveStore(net.stores(3), host, closeFirst = true), clock = false)
      _ <- pump(net.moveStore(net.stores(4), host, closeFirst = true), clock = false)
      _ <- eventually("stores 3 and 4 passed their load floors", clock = false)(
             net.loadFloorPassed(sid(3)) && net.loadFloorPassed(sid(4)))
      ceiling = host.joinCeiling.current.get
      _ = net.crashHost(host)
      // T starts past the ceiling the crashed host persisted, so neither of its stores can have joined T. Stores 0-2
      // alone cannot commit it: the write threshold is 4
      _ <- clockPast(ceiling)
      tx = client.newTransaction()
      _ = net.recordTxMessage = isClientPrepare(tx.id)
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      committed = tx.commit()
      _ <- eventually("stores 0-2 hold T", clock = false)((0 until 3).forall(holds(_, tx.id)))
      prepares = recordedClientPrepares(tx.id).filter(p => p.to == sid(3) || p.to == sid(4))
      _ = net.recordTxMessage = _ => false
      _ = withClue("T cannot commit while the host is down: "):
            prepares.size shouldBe 2
            committed.isCompleted shouldBe false
      restarted <- pump(net.restartHost(host), clock = false)
      // The client's retransmission reaches the restarted stores: both join T
      _ = prepares.foreach(net.deliverTransactionMessage)
      _ = withClue("the restarted stores join T, started below their load plus MaxStartLead: "):
            holds(3, tx.id) || storeRevision(3, x).contains(ObjectRevision(tx.id)) shouldBe true
            holds(4, tx.id) || storeRevision(4, x).contains(ObjectRevision(tx.id)) shouldBe true
      result <- outcome(committed, clock = false)
      _ <- settle(clock = false)
    yield
      assert(restarted ne host)
      result shouldBe a[Success[?]]
      (0 until 5).map(storeRevision(_, x)) shouldBe (0 until 5).map(_ => Some(ObjectRevision(tx.id)))

  test("a rolling restart of the stores' hosts loses no transaction, and each restarted store joins at once"):
    // Each round crashes one host, commits a transaction on the other four stores, restarts the host and, once past
    // its persisted ceiling (about MaxStartLead plus JoinCeiling.Step after the crash), commits another on an object
    // every store holds. The restarted store misses the first only. Without the ceiling it would refuse every
    // transaction started before its load plus MaxStartLead, the second included
    val rolled = List(2, 3, 4)
    def round(i: Int, host: Host, missed: DataObjectPointer, y: DataObjectPointer): Future[Unit] =
      val ceiling = host.joinCeiling.current.get
      net.crashHost(host)
      for
        missedBefore <- client.read(missed)
        down <- overwrite(missed, missedBefore.revision)
        _ <- pump(net.restartHost(host), clock = false)
        _ <- clockPast(ceiling)
        yBefore <- client.read(y)
        after <- overwrite(y, yBefore.revision)
      yield
        withClue(s"store $i, restarted, missed only the transaction of its downtime: "):
          storeRevision(i, missed) shouldBe Some(missedBefore.revision)
          storeRevision(i, y) shouldBe Some(ObjectRevision(after))
        (0 until 5).filter(_ != i).map(storeRevision(_, missed)) shouldBe List.fill(4)(Some(ObjectRevision(down)))
    for
      missed <- rolled.foldLeft(Future.successful(Map[Int, DataObjectPointer]())): (f, i) =>
                  f.flatMap(m => allocateX().map(o => m + (i -> o)))
      y <- allocateX()
      hosts <- ownHosts(rolled)
      _ <- rolled.foldLeft(Future.unit)((f, i) => f.flatMap(_ => round(i, hosts(i), missed(i), y)))
    yield
      (0 until 5).map(storeRevision(_, y)).distinct.size shouldBe 1
