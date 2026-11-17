package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.{Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.network.{ClientResponse, Read, TransactionFinalized, TransactionResolved,
  TransactionVote, TxAccept, TxMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.server.store.Frontend

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.util.{Failure, Success, Try}

/** The client's one-round-trip commit end to end (design docs/superpowers/specs/2026-11-16-client-1rtt-commit-design.md,
  * section 7): the client completes Committed once every object has its pool's writeThreshold durable commit votes
  * (TransactionVote), without waiting for the leader, and learns abort only from the leader's Paxos outcome.
  *
  * Replication(3, 2) on the TestNetwork's lockstep hosts. Every store's driver retransmits, the designated leader's
  * included, as a host's does, so a transaction whose Accept round was lost resolves once the drops stop. (A leader
  * driver that never retransmits would keep the other stores' Txs from looking idle with its heartbeats, and T would
  * end only by expiry.)
  */
class FastCommitIntegrationSuite extends IntegrationTestSuite with org.scalatest.ParallelTestExecution:

  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(100, MILLISECONDS), recordCrl = true, recoveryDrivers = true,
      leaderRetransmits = true)

  /** A client that records its own outcomes, as a production client does, so its reads know its commits */
  private val knowsOwnCommits = "a read straight after a fast commit returns the new revision without spinning"

  /** Its stores park a re-read far longer than the test runs, so a parked re-read is answered only once T applies,
    * however loaded the machine */
  override def testHostOptionsFor(name: String): TestNetwork.HostOptions =
    if name == knowsOwnCommits then
      testHostOptions.copy(clientStatusCache = true, parkedReadWait = Duration(30, SECONDS))
    else
      testHostOptions

  private def deadline(after: Duration = Duration(30, SECONDS)): Long = System.nanoTime() + after.toNanos

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

  /** Pumps the hosts for `d` */
  private def pumpFor(d: Duration, clock: Boolean): Future[Unit] =
    val until = System.nanoTime() + d.toNanos
    def loop(): Future[Unit] =
      if System.nanoTime() > until then Future.unit
      else Future { Thread.sleep(1); step(clock) }.flatMap(_ => loop())
    loop()

  /** `f`'s outcome, once it completes */
  private def outcome[T](f: Future[T], clock: Boolean, until: Long = deadline()): Future[Try[T]] =
    pump(f, clock, until).transform(r => Success(r))

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

  private def holders(txid: TransactionId): Set[Int] =
    (0 until 3).filter(i => frontend(i).exists(_.transactions.contains(txid))).toSet

  private def storeRevision(i: Int, x: DataObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(x.id).map(_.metadata.revision)

  private def isLeaderOutcome(txid: TransactionId)(r: ClientResponse): Boolean = r match
    case m: TransactionResolved => m.transactionId == txid
    case m: TransactionFinalized => m.transactionId == txid
    case _ => false

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

  test("the client learns commit from votes while every leader outcome to it is dropped"):
    for
      x <- allocateX()
      xBefore <- client.read(x)
      tx = client.newTransaction()
      _ = net.dropClientResponse = isLeaderOutcome(tx.id)
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      // No wall-clock bound is needed: every leader outcome to the client is dropped for the whole wait, so only the
      // stores' votes can complete it
      result <- outcome(tx.commit(), clock = false)
      _ = withClue("the client completed Committed from the stores' votes alone: "):
            result shouldBe a[Success[?]]
      _ <- settle(clock = false)
      _ = net.dropClientResponse = _ => false
      xAfter <- client.read(x)
    yield
      (0 until 3).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(tx.id)))
      xAfter.revision shouldBe ObjectRevision(tx.id)
      xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](2)

  test("the client learns commit while the Accept round is blocked, and the stores commit once it is unblocked"):
    for
      x <- allocateX()
      xBefore <- client.read(x)
      tx = client.newTransaction()
      _ = net.dropTxMessage = (m: TxMessage) => m.isInstanceOf[TxAccept] && m.transactionId == tx.id
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      // Every Accept is dropped for the whole wait, so no leader outcome exists: only the votes can complete it
      result <- outcome(tx.commit(), clock = false)
      _ = withClue("the client completed Committed with no Accept delivered: "):
            result shouldBe a[Success[?]]
            holders(tx.id) shouldBe Set(0, 1, 2)
            (0 until 3).foreach(i => storeRevision(i, x) shouldBe Some(xBefore.revision))
      // The leader's driver retransmits its Accepts once they are no longer dropped
      _ = net.dropTxMessage = _ => false
      _ <- settle(clock = true)
      xAfter <- client.read(x)
    yield
      withClue("every store applied the commit and finalized T: "):
        holders(tx.id) shouldBe Set()
        (0 until 3).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(tx.id)))
      xAfter.revision shouldBe ObjectRevision(tx.id)
      xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](2)

  test("a transaction with abort votes is reported aborted only by the leader"):
    // Two abort votes on Replication(3, 2) make T abort-certain, but the client completes Aborted only from the
    // leader's message, sent once Paxos chose abort
    val behind = 2
    for
      x <- allocateX()
      xBefore <- client.read(x)
      // T0 overwrites x at every store but `behind`, which gets none of T0's messages and is reported offline so it
      // does not lead. Stores 0 and 1 commit it (writeThreshold 2)
      t0 <- {
        val t0 = client.newTransaction()
        net.offlineStores = Set(sid(behind))
        net.droppedTx = Set((sid(behind), t0.id))
        t0.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
        for
          _ <- pump(t0.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.offlineStores = Set()
          net.droppedTx = Set()
          t0.id
      }
      _ = withClue("only the behind store still holds x's old revision: "):
            (0 until 3).foreach: i =>
              storeRevision(i, x) shouldBe Some(if i == behind then xBefore.revision else ObjectRevision(t0))
      // T requires x's old revision: stores 0 and 1 vote abort, the behind store votes commit. Every response to the
      // client is recorded as it is delivered, none is dropped, with whether the client's driver had completed by then
      tx = client.newTransaction()
      delivered = new java.util.concurrent.ConcurrentLinkedQueue[(ClientResponse, Boolean)]()
      _ = net.dropClientResponse = (r: ClientResponse) =>
            val ours = r match
              case m: TransactionResolved => m.transactionId == tx.id
              case m: TransactionFinalized => m.transactionId == tx.id
              case m: TransactionVote => m.transactionId == tx.id
              case _ => false
            if ours then
              delivered.add((r, net.clientDriver(tx.id).exists(_.result.isCompleted)))
            false
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](3)))
      result <- outcome(tx.commit(), clock = false)
      _ <- settle(clock = false)
      _ = net.dropClientResponse = _ => false
    yield
      import scala.jdk.CollectionConverters.*
      val responses = delivered.asScala.toList
      val votes = responses.collect { case (v: TransactionVote, _) => v.fromStore.poolIndex -> v.disposition }.toMap
      // The leader's outcome: TransactionResolved, or TransactionFinalized, which it sends first when finalization
      // needs no work (an abort's)
      val firstOutcome = responses.indexWhere: (r, _) =>
        r.isInstanceOf[TransactionResolved] || r.isInstanceOf[TransactionFinalized]
      withClue("the client aborted: "):
        result should matchPattern { case Failure(_: TransactionAborted) => }
      withClue("every store's vote reached the client: "):
        votes shouldBe Map(0 -> TransactionDisposition.VoteAbort, 1 -> TransactionDisposition.VoteAbort,
          behind -> TransactionDisposition.VoteCommit)
      withClue("the client's driver completed only once the leader's outcome was delivered: "):
        firstOutcome should be >= 0
        responses.take(firstOutcome + 1).map(_._2) should not contain true
        responses.collect { case (m: TransactionResolved, _) => m.committed } should not contain true
        responses.collect { case (m: TransactionFinalized, _) => m.committed } should not contain true
      withClue("T applied nowhere: "):
        (0 until 3).map(storeRevision(_, x)) shouldBe
          (0 until 3).map(i => Some(if i == behind then xBefore.revision else ObjectRevision(t0)))

  // Spec 5: the client learns commit from the votes before any store applies it. A read then finds T's lock at every
  // store, and the client knows T committed, so it re-reads each store naming T (Read.awaitCommit). The store parks
  // the re-read until T's commit applies, rather than answering the lock again. The Accepts stay dropped until a
  // re-read naming T is seen, so the re-reads cannot be skipped by T applying first.
  //
  // The test client also re-sends the whole read every BaseReadDriver.ResendEvery (100 ms). Such a resend names no
  // commit, so the store answers it at once, with T's lock while T has not applied, and the client re-reads naming T
  // once more: how many re-reads a store sees depends on how long T takes to apply. What does not depend on load:
  // every re-read naming T follows a reply that showed T's lock, so a store never sees more of them than full reads.
  // A store that answered a re-read with the lock again would get another at once, with no full read between
  test(knowsOwnCommits):
    val awaitingReads = new java.util.concurrent.ConcurrentLinkedQueue[Read]()
    val fullReads = new java.util.concurrent.ConcurrentLinkedQueue[Read]()
    for
      x <- allocateX()
      xBefore <- client.read(x)
      tx = client.newTransaction()
      _ = net.dropTxMessage = (m: TxMessage) => m.isInstanceOf[TxAccept] && m.transactionId == tx.id
      _ = tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
      // Every Accept is dropped, so only the votes can complete it
      result <- outcome(tx.commit(), clock = false)
      _ = result shouldBe a[Success[?]]
      _ = withClue("no store has applied T: "):
            holders(tx.id) shouldBe Set(0, 1, 2)
            (0 until 3).foreach(i => storeRevision(i, x) shouldBe Some(xBefore.revision))
      _ = net.observeRead = r =>
            if r.objectPointer.id == x.id then
              if r.awaitCommit.nonEmpty then awaitingReads.add(r) else fullReads.add(r)
      read = client.read(x)
      // The leader's Accept retransmissions are lost too, until the client has re-read naming T
      _ <- eventually("a re-read naming T", clock = true)(!awaitingReads.isEmpty)
      _ = withClue("the parked re-read is not answered while T has not applied: "):
            read.isCompleted shouldBe false
            (0 until 3).foreach(i => storeRevision(i, x) shouldBe Some(xBefore.revision))
      _ = net.dropTxMessage = _ => false
      xAfter <- pump(read, clock = true)
      _ = net.observeRead = _ => ()
    yield
      import scala.jdk.CollectionConverters.*
      xAfter.revision shouldBe ObjectRevision(tx.id)
      xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](2)
      val rereads = awaitingReads.asScala.toList
      rereads should not be empty
      rereads.map(_.awaitCommit).toSet shouldBe Set(Set(tx.id))
      val perStore = rereads.groupMapReduce(_.toStore)(_ => 1)(_ + _)
      val fullPerStore = fullReads.asScala.toList.groupMapReduce(_.toStore)(_ => 1)(_ + _)
      withClue(s"re-reads naming T per store: $perStore; full reads per store: $fullPerStore: "):
        perStore.filter((store, n) => n > fullPerStore.getOrElse(store, 0)) shouldBe empty

  // The client learns an allocation committed from the votes, before any store applies it. Each commit voter then
  // misses the object in its backend and holds the allocation unapplied, so it answers a read AllocationPending. That
  // proves nothing, but taking a threshold of such replies as the read's end failed it InvalidObject: the client could
  // not read the object it had just been told it allocated. The read waits for the allocation to apply instead
  test("a read of an object whose allocation the client learned committed from the votes waits for it to apply"):
    for
      pool <- client.getStoragePool(Radicle.poolId)
      tx = client.newTransaction()
      _ = tx.replaySafe("test")
      _ = net.dropTxMessage = (m: TxMessage) => m.isInstanceOf[TxAccept] && m.transactionId == tx.id
      x <- pool.allocator.allocateDataObject(DataBuffer(Array[Byte](7)))(using tx)
      // Every Accept is dropped, so only the votes can complete it
      result <- outcome(tx.commit(), clock = false)
      _ = result shouldBe a[Success[?]]
      _ = withClue("no store has applied the allocation: "):
            holders(tx.id) shouldBe Set(0, 1, 2)
            (0 until 3).foreach(i => storeRevision(i, x) shouldBe None)
      read = client.read(x)
      // Long enough for many of the test client's full resends (BaseReadDriver.ResendEvery) and heartbeats
      _ <- pumpFor(Duration(300, MILLISECONDS), clock = true)
      _ = withClue(s"the read neither failed nor completed while the allocation had not applied (${read.value}): "):
            read.isCompleted shouldBe false
            (0 until 3).foreach(i => storeRevision(i, x) shouldBe None)
      _ = net.dropTxMessage = _ => false
      xAfter <- pump(read, clock = true)
    yield
      xAfter.revision shouldBe ObjectRevision(tx.id)
      xAfter.data.getByteArray.toSeq shouldBe Seq[Byte](7)
