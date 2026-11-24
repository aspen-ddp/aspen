package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.rebuild.RebuildDisk
import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.{TestTransactions, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.network.{ClientResponse, TransactionVote, TxMessage, TxPrepareResponse}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.server.repair.{RepairPolicy, StoreRepairer}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.{Frontend, StoreIncarnationFile}
import org.aspen_ddp.aspen.server.store.backend.MapBackend

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.util.{Success, Try}

/** A rebuilt store and the votes of its lost incarnation (cross-document-review.md, locks across crashes and copies).
  * A commit vote of the old incarnation still counts toward its transaction T after the store is rebuilt, though the
  * rebuilt store holds none of T's locks. The rebuilt store must not grant a commit vote to a conflicting T' while T
  * may still commit, or T commits after T' with its lock requirement broken.
  *
  * Replication(3, 2) on the TestNetwork's lockstep hosts. T's leader is store 0. Store 2 never hears of T. Store 0's
  * own vote is held from the leader, so the leader's round holds store 1's commit vote alone and waits for a quorum.
  */
class RebuiltVoterLockIntegrationSuite extends IntegrationTestSuite with org.scalatest.ParallelTestExecution:

  // Five stores for the growth test: a lost commit voter of a fresh write stays within the margin (t = 4 > r = 3)
  override def testIdaFor(name: String): org.aspen_ddp.aspen.common.ida.IDA =
    if name.contains("admits no growth") then org.aspen_ddp.aspen.common.ida.Replication(5, 4) else testIda

  // T's stores resolve it by expiry once its client has committed it, so the growth test shortens the horizon
  override def testHostOptionsFor(name: String): TestNetwork.HostOptions =
    if name.contains("admits no growth") then testHostOptions.copy(transactionHorizon = Duration(2, SECONDS))
    else testHostOptions

  // Retransmitting drivers, the leader's included: the leader's round waits for store 0's vote and gets it by resending
  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(100, MILLISECONDS), recoveryDrivers = true,
      leaderRetransmits = true)

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

  private def outcome[T](f: Future[T], clock: Boolean): Future[Try[T]] =
    pump(f, clock).transform(r => Success(r))

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

  private def settle(clock: Boolean): Future[Unit] =
    eventually("transactions completed", clock)(!net.hasTransactions)

  private def sid(i: Int): StoreId = net.stores(i).storeId

  private def frontend(i: Int): Option[Frontend] =
    net.hostFor(sid(i)).control.router.loaded.get(sid(i)).map(_.frontend)

  private def vote(i: Int, txid: TransactionId): Option[TransactionDisposition.Value] =
    frontend(i).flatMap(_.transactions.get(txid)).map(_.vote)

  private def allocate(): Future[DataObjectPointer] =
    for
      pool <- client.getStoragePool(Radicle.poolId)
      x <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             tx.replaySafe("test")
             pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
      _ <- settle(clock = false)
    yield x

  /** Store 0's vote on `txid`, to its leader (itself) and to the client */
  private def store0Vote(txid: TransactionId)(m: TxMessage): Boolean = m match
    case r: TxPrepareResponse => r.transactionId == txid && r.from == sid(0)
    case _ => false

  private def store0ClientVote(txid: TransactionId)(r: ClientResponse): Boolean = r match
    case v: TransactionVote => v.transactionId == txid && v.fromStore == sid(0)
    case _ => false

  /** Loses store 1 and serves its rebuild on a fresh host: its data as the pool held it, an acceptor start after T's.
    * Every vote on T was cast before the rebuild, so its peers' held votes are complete at once (inherited-votes
    * gate now). Yields the rebuilt store's directory
    */
  private def loseAndRebuildStore1(): Future[os.Path] =
    val rebuiltDir = os.temp.dir(deleteOnExit = true)
    val lostData = net.stores(1).m
    for
      _ <- pump(net.hostFor(sid(1)).control.loader.closeStore(sid(1), extractCrl = false), clock = false)
      _ = RebuildDisk.writeAcceptorSince(rebuiltDir, HLCTimestamp.now)
      _ = RebuildDisk.writeInheritedVotes(rebuiltDir, System.currentTimeMillis())
      _ = StoreIncarnationFile.writeFresh(rebuiltDir)
      rebuilt = new MapBackend(sid(1)):
        override def path: os.Path = rebuiltDir
      _ = rebuilt.m = lostData
      _ <- pump(net.moveStore(rebuilt, net.newHost(), closeFirst = false), clock = false)
      _ <- eventually("the rebuilt store passed its load floor", clock = false)(net.loadFloorPassed(sid(1)))
    yield rebuiltDir

  private def lockedThenConflicting(lock: (Transaction, DataObjectPointer, ObjectRevision) => Unit) =
    for
      d <- allocate()
      f <- allocate()
      dBefore <- client.read(d)
      fBefore <- client.read(f)
      // T: an update of D and a lock on F, led by store 0
      tx = client.newTransaction()
      _ = net.unreachableStores = Set(sid(1), sid(2))
      _ = net.droppedTx = Set((sid(2), tx.id))
      _ = net.dropTxMessage = store0Vote(tx.id)
      _ = net.dropClientResponse = store0ClientVote(tx.id)
      _ = tx.overwrite(d, dBefore.revision, DataBuffer(Array[Byte](2)))
      _ = lock(tx, f, fBefore.revision)
      tCommit = tx.commit()
      _ <- eventually("stores 0 and 1 voted commit on T", clock = false):
             vote(0, tx.id).contains(TransactionDisposition.VoteCommit) &&
               vote(1, tx.id).contains(TransactionDisposition.VoteCommit)
      _ = net.unreachableStores = Set()
      _ = withClue("T is undecided: ")(tCommit.isCompleted shouldBe false)
      rebuiltDir <- loseAndRebuildStore1()
      _ = withClue("the rebuilt store is muted for T: ")(frontend(1).flatMap(_.acceptorSince).isDefined shouldBe true)
      // Its peers report store 0's commit vote on T: the rebuilt store's old vote may still count for T
      _ <- eventually("the rebuilt store holds T's inherited vote", clock = true):
             frontend(1).flatMap(_.inheritedVoteTransactions).contains(Set(tx.id))
      // T': a fence takeover, bumping F, led by store 2
      tx2 = client.newTransaction()
      _ = net.unreachableStores = Set(sid(0), sid(1))
      _ = tx2.bumpVersion(f, fBefore.revision)
      t2Commit = tx2.commit()
      _ = net.unreachableStores = Set()
      t2Result <- outcome(t2Commit, clock = true)
      _ = withClue(s"T' must not commit while T, which locks F, may still commit ($t2Result): "):
            t2Result.failed.toOption.exists(_.isInstanceOf[TransactionAborted]) shouldBe true
      // Store 0's vote reaches the leader: T commits
      _ = net.dropTxMessage = _ => false
      _ = net.dropClientResponse = _ => false
      tResult <- outcome(tCommit, clock = true)
      _ = withClue("T commits: ")(tResult.isSuccess shouldBe true)
      _ = net.droppedTx = Set()
      _ <- settle(clock = true)
      // T' again: once T resolved the rebuilt store releases F, and forgets its inherited votes
      _ <- eventually("the rebuilt store released T's inherited vote", clock = true):
             frontend(1).flatMap(_.inheritedVoteTransactions).contains(Set())
      _ = withClue("the inherited-votes file is gone: "):
            os.exists(rebuiltDir / RebuildingStore.InheritedVotesFile) shouldBe false
      fAfterT <- client.read(f)
      tx3 = client.newTransaction()
      _ = tx3.bumpVersion(f, fAfterT.revision)
      t3Result <- outcome(tx3.commit(), clock = true)
      _ <- settle(clock = true)
    yield
      withClue("a conflicting transaction commits once T resolved: ")(t3Result.isSuccess shouldBe true)

  test("a rebuilt voter refuses a commit over an exclusive lock its lost incarnation's vote backs"):
    lockedThenConflicting((tx, f, rev) => tx.exclusiveLockRevision(f, rev))

  test("a rebuilt voter refuses a commit over a shared lock its lost incarnation's vote backs"):
    lockedThenConflicting((tx, f, rev) => tx.sharedLockRevision(f, rev))

  // ---- Inherited growth (Frontend, Inherited growth; cross-document-review.md 2.3) ----
  //
  // The rare case: T grows a KeyValue object O with stores 0, 1, 3 and 4 voting commit (Replication(5, 4)), store 1 is
  // lost and rebuilt while T is undecided, so its copy of O lacks T, and T then commits counting store 1's old vote.
  // Until a repair installs T on the rebuilt store, a growth of O it voted for would count it twice in the bound that
  // keeps every committed growth of O within the slice limit (transaction-requirements R16). T's leader is store 0,
  // whose own vote is held from it, so T waits; store 2 never hears of T.

  private def kvKey(s: String): Key = Key(s.getBytes)

  private def allocateKV(): Future[KeyValueObjectPointer] =
    for
      pool <- client.getStoragePool(Radicle.poolId)
      kp <- client.transactUntilSuccessful: tx =>
              given Transaction = tx
              tx.replaySafe("test")
              pool.allocator.allocateKeyValueObject()
      _ <- settle(clock = false)
    yield kp

  /** A transaction inserting `key` into `kp`, which grows it */
  private def growth(kp: KeyValueObjectPointer, key: String): Transaction =
    val tx = TestTransactions.replaySafe(client.newTransaction())
    tx.update(kp, None, None, List(KeyValueUpdate.DoesNotExist(kvKey(key))), List(Insert(kvKey(key), new Array[Byte](8))))
    tx

  test("a rebuilt voter admits no growth of a KeyValue object its lost incarnation's vote may be missing from, until repaired"):
    var store1Votes = Map[TransactionId, TransactionDisposition.Value]()
    def recordStore1Votes(r: ClientResponse): Boolean =
      r match
        case v: TransactionVote if v.fromStore == sid(1) => store1Votes += v.transactionId -> v.disposition
        case _ => ()
      false
    def guarded(kp: KeyValueObjectPointer): Boolean = frontend(1).exists(_.inheritedGrowthLock(kp.id))
    for
      kp <- allocateKV()
      tx = growth(kp, "t")
      _ = net.unreachableStores = Set(sid(1), sid(2), sid(3), sid(4))
      _ = net.droppedTx = Set((sid(2), tx.id))
      _ = net.dropTxMessage = store0Vote(tx.id)
      _ = net.dropClientResponse = store0ClientVote(tx.id)
      tCommit = tx.commit()
      _ <- eventually("stores 0, 1, 3 and 4 voted commit on T", clock = false):
             List(0, 1, 3, 4).forall(i => vote(i, tx.id).contains(TransactionDisposition.VoteCommit))
      _ = net.unreachableStores = Set()
      _ = withClue("store 1's copy, which the rebuild serves, lacks T: "):
            net.stores(1).m.get(kp.id).flatMap(_.kvState).exists(_.content.contains(kvKey("t"))) shouldBe false
      rebuiltDir <- loseAndRebuildStore1()
      _ <- eventually("the rebuilt store holds T's inherited vote", clock = true):
             frontend(1).flatMap(_.inheritedVoteTransactions).contains(Set(tx.id))
      // T commits, its resolution reaching the rebuilt store without its data
      _ = net.dropTxMessage = _ => false
      _ = net.dropClientResponse = _ => false
      tResult <- outcome(tCommit, clock = true)
      _ = withClue("T commits: ")(tResult.isSuccess shouldBe true)
      _ = net.droppedTx = Set()
      _ <- settle(clock = true)
      _ <- eventually("the rebuilt store released T's inherited vote", clock = true):
             frontend(1).flatMap(_.inheritedVoteTransactions).contains(Set())
      _ = withClue("O is guarded against growth: ")(guarded(kp) shouldBe true)
      _ = withClue("the inherited-votes file stays while O is guarded: "):
            os.exists(rebuiltDir / RebuildingStore.InheritedVotesFile) shouldBe true
      // U grows O: the rebuilt store votes abort, and the other four commit it
      _ = net.dropClientResponse = recordStore1Votes
      u = growth(kp, "u")
      uResult <- outcome(u.commit(), clock = true)
      _ <- settle(clock = true)
      _ = withClue("U commits without the rebuilt store: ")(uResult.isSuccess shouldBe true)
      _ = withClue("the rebuilt store votes abort on U: ")(store1Votes.get(u.id) shouldBe
            Some(TransactionDisposition.VoteAbort))
      // Repair installs T (and U) on the rebuilt store from a read after T committed, which ends the guard
      repairer = StoreRepairer(client, net.hostFor(sid(1)))
      _ <- {
        def repairUntilReleased(passes: Int): Future[Unit] =
          if !guarded(kp) then Future.unit
          else if passes == 0 then Future.failed(new AssertionError("repair never released the growth guard"))
          else pump(repairer.scan(sid(1), RepairPolicy.Default), clock = true)
            .flatMap(_ => settle(clock = true)).flatMap(_ => repairUntilReleased(passes - 1))
        repairUntilReleased(50)
      }
      v = growth(kp, "v")
      vResult <- outcome(v.commit(), clock = true)
      _ <- settle(clock = true)
      read <- client.read(kp)
    yield
      withClue("V commits: ")(vResult.isSuccess shouldBe true)
      withClue("once repaired, the rebuilt store votes commit on a growth: "):
        store1Votes.get(v.id) shouldBe Some(TransactionDisposition.VoteCommit)
      withClue("the inherited-votes file is gone: ")(os.exists(rebuiltDir / RebuildingStore.InheritedVotesFile) shouldBe false)
      read.contents.keySet should contain allOf (kvKey("t"), kvKey("u"), kvKey("v"))
