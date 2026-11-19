package org.aspen_ddp.aspen.server.transaction

import scala.concurrent.ExecutionContext.Implicits.global
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientResponse, TxAccept, TxAcceptResponse, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, RefcountUpdate, TransactionDescription, TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.NoBackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.transaction.TransactionFinalizer.Factory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}


object TransactionDriverSuite {
  val poolId = PoolId(java.util.UUID.randomUUID())
  val ds0 = StoreId(poolId, 0)
  val ds1 = StoreId(poolId, 1)
  val ds2 = StoreId(poolId, 2)
  val ds3 = StoreId(poolId, 3)

  val rev: ObjectRevision = ObjectRevision.Null
  val oid = ObjectId(java.util.UUID.randomUUID())
  val ida = Replication(3, 2)
  val simpleObj = DataObjectPointer(oid, poolId, Array[Byte]())

  def mktxd(optr: ObjectPointer, du: List[DataUpdate] = Nil, ru: List[RefcountUpdate] = Nil) =
    val allPools = (optr :: du.map(_.objectPointer) ++ ru.map(_.objectPointer)).map(_.poolId).toSet
    val poolMap = allPools.map(p => p -> ida).toMap
    TransactionDescription(
      TransactionId(java.util.UUID.randomUUID()), HLCTimestamp(100), optr, 0, du ++ ru, Nil, None, Nil, Nil, ida, poolMap)

  def mkprep(paxosRound: Int, toPeer: Byte, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(StoreId(poolId,toPeer), StoreId(poolId,fromPeer), txd, ProposalId(paxosRound,fromPeer), Nil, fromClient = false)

  class TMessenger extends Messenger {
    var messages: List[TxMessage] = List[TxMessage]()

    def sendClientResponse(msg: ClientResponse): Unit = ()

    def sendTransactionMessage(msg: TxMessage): Unit = messages = msg :: messages

    def sendTransactionMessages(msg: List[TxMessage]): Unit = messages = msg ++ messages

    def dropCacheForStore(storeId: StoreId): Unit = ()

    def clear(): Unit = messages = List()
  }

  class TTD (storeId: StoreId,
             messenger: Messenger,
             initialPrepare: TxPrepare,
             finalizerFactory: Factory,
             onComplete: TransactionId => Unit,
             tasks: BackgroundTaskManager = NoBackgroundTaskManager,
             ec: ExecutionContext = scala.concurrent.ExecutionContext.global,
             start: TransactionDriver.Start = TransactionDriver.Start())
    extends TransactionDriver(storeId, messenger, tasks, initialPrepare.txd, finalizerFactory, start)(using ec) {
    override protected def onFinalized(committed: Boolean): Unit = {
      super.onFinalized(committed)
      onComplete(txd.transactionId)
    }

    def testResendPrepares(): Unit = sendPrepareMessages()
    def testResendPrepare(to: StoreId): Unit = sendPrepareMessage(to)
    def testHaveLocalProposal: Boolean = synchronized { proposer.haveLocalProposal }
    def testProposalId: ProposalId = synchronized { proposer.currentProposalId }
    def testFinalized: Boolean = synchronized { finalized }
  }

  class TFinalizer(autoComplete: Boolean = true) extends TransactionFinalizer with TransactionFinalizer.Factory {
    var cancelled = false
    var created = false
    var peers: Set[StoreId] = Set[StoreId]()

    override def cancel(): Unit = cancelled = true

    /** The stores a handed-off mark plans entries for */
    var handedOff: Set[StoreId] = Set()

    /** Whether the finalizer was cancelled when `handedOffStores` was last read */
    var readCancelled: Option[Boolean] = None
    override def handedOffStores: Set[StoreId] =
      readCancelled = Some(cancelled)
      handedOff

    /** When set, `complete` is this promise: models a marker whose write completes it though it was cancelled */
    var completion: Option[Promise[Unit]] = None

    def complete: Future[Unit] = if (completion.isDefined) completion.get.future else if (autoComplete) Future.unit else Promise[Unit]().future

    def debugStatus: List[(String, Boolean)] = Nil

    var createCount = 0

    override def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = {
      created = true
      createCount += 1
      this
    }

    /** The stores the driver last reported as keeping the transaction's state for replay */
    var keptForReplay: Set[StoreId] = Set()

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]], keptForReplay: Set[StoreId]): Unit = synchronized {
      peers = commitErrors.keySet
      this.keptForReplay = keptForReplay
    }

    /** None until the driver reports its commit voters */
    var voters: Option[Option[Set[StoreId]]] = None

    /** None until the driver reports its commit voters */
    var incarnations: Option[Map[StoreId, StoreIncarnation]] = None

    override def commitVoters(voters: Option[Set[StoreId]], incarnations: Map[StoreId, StoreIncarnation]): Unit =
      synchronized {
        this.voters = Some(voters)
        this.incarnations = Some(incarnations)
      }
  }


  /** Runs tasks only when told to, recording each one's delay */
  class StepTasks extends BackgroundTaskManager(ExecutionContext.parasitic):
    final class Task(val delay: Duration, fn: () => Unit) extends BackgroundTaskManager.ScheduledTask:
      var cancelled = false
      def cancel(): Unit = cancelled = true
      def run(): Unit = if !cancelled then fn()
    var tasks: List[Task] = Nil
    override def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask = synchronized:
      val t = new Task(delay, () => fn)
      tasks = tasks :+ t
      t
    def runAll(): Unit =
      val ts = synchronized { val t = tasks; tasks = Nil; t }
      ts.foreach(_.run())

  /** A finalizer whose completion the test sets, and which plans entries for `handedOff` */
  class StepFinalizer(val handedOff: Set[StoreId]) extends TransactionFinalizer:
    val done: Promise[Unit] = Promise()
    var cancelled = false
    def complete: Future[Unit] = done.future
    override def handedOffStores: Set[StoreId] = handedOff
    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]], keptForReplay: Set[StoreId]): Unit = ()
    def debugStatus: List[(String, Boolean)] = Nil
    def cancel(): Unit = cancelled = true

  /** Hands out a new StepFinalizer per create, planning the next set of `plans` (empty once they run out). A create
    * whose plan is None throws, as a host lacking an action's type does (transaction-recovery 4.4)
    */
  class StepFactory(plans: Option[Set[StoreId]]*) extends TransactionFinalizer.Factory:
    private var remaining = plans.toList
    var created: List[StepFinalizer] = Nil
    def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer =
      val plan = remaining match
        case p :: rest => remaining = rest; p
        case Nil => Some(Set[StoreId]())
      plan match
        case None => throw new NoSuchElementException("no such finalization action type")
        case Some(stores) =>
          val f = new StepFinalizer(stores)
          created = created :+ f
          f

  def commitBy(driver: TransactionDriver, txd: TransactionDescription): Unit =
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))

}

class TransactionDriverSuite extends AnyFunSuite with Matchers {

  import TransactionDriverSuite._

  private def promiseFrom(driver: TransactionDriver, from: StoreId, disposition: TransactionDisposition.Value,
                      accepted: Option[(ProposalId, Boolean)] = None, proposal: ProposalId = ProposalId(1, 0)): Unit =
    driver.receiveTxPrepareResponse(TxPrepareResponse(driver.storeId, from, driver.txd.transactionId,
      Right(TxPrepareResponse.Promise(accepted)), proposal, disposition, Nil))

  private def accepts(messenger: TMessenger): List[TxAccept] = messenger.messages.collect { case a: TxAccept => a }

  // paxos-transactions 7.2: a Tx resolved before it voted answers Undetermined. Counted as aborts, two of them make
  // commit look impossible for a width-3, threshold-2 object, though they are no votes at all
  test("Undetermined replies are no votes: they never make the tally propose abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())

    promiseFrom(driver, ds0, TransactionDisposition.VoteCommit)
    promiseFrom(driver, ds1, TransactionDisposition.Undetermined)
    promiseFrom(driver, ds2, TransactionDisposition.Undetermined)

    driver.testHaveLocalProposal shouldBe false
    accepts(messenger) shouldBe Nil
  }

  test("a quorum reporting an accepted value is proposed while Undetermined replies leave the tally undecided") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(2, 0, 0, txd), new TFinalizer(), _ => ())

    promiseFrom(driver, ds1, TransactionDisposition.Undetermined, accepted = Some((ProposalId(1, 1), true)))
    promiseFrom(driver, ds2, TransactionDisposition.Undetermined)

    driver.testHaveLocalProposal shouldBe false
    accepts(messenger).map(a => (a.to, a.value)).toSet shouldBe Set((ds0, true), (ds1, true), (ds2, true))
  }

  // paxos-transactions 7.2: the tally stopped at the first object short of answers, even when another object could no
  // longer commit
  test("an object that can no longer commit decides abort though another object is short of answers") {
    val otherPool = PoolId(java.util.UUID.randomUUID())
    val otherObj = DataObjectPointer(ObjectId(java.util.UUID.randomUUID()), otherPool)
    val primaryUpdate = DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite)
    val otherUpdate = DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite)
    // Both orders of the object set, so the short object is visited first in one of them
    for updates <- List(List(primaryUpdate, otherUpdate), List(otherUpdate, primaryUpdate)) do
      val txd = mktxd(simpleObj, updates)
      val messenger = new TMessenger()
      val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())

      promiseFrom(driver, ds0, TransactionDisposition.VoteCommit)
      promiseFrom(driver, ds1, TransactionDisposition.Undetermined) // a quorum, but one vote: short of answers
      promiseFrom(driver, StoreId(otherPool, 1), TransactionDisposition.VoteAbort)
      promiseFrom(driver, StoreId(otherPool, 2), TransactionDisposition.VoteAbort)

      accepts(messenger).map(a => (a.to, a.value)).toSet shouldBe Set((ds0, false), (ds1, false), (ds2, false))
  }

  // One commit vote, one abort vote and a silent store leave the outcome to the silent store. The driver
  // never proposes abort on it: expiry rule E ends the Tx instead
  test("an undecided tally never proposes abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())

    promiseFrom(driver, ds0, TransactionDisposition.VoteCommit)
    promiseFrom(driver, ds1, TransactionDisposition.VoteAbort)
    driver.testHaveLocalProposal shouldBe false
    accepts(messenger) shouldBe Nil

    // The resent prepares' replies re-run the tally
    promiseFrom(driver, ds1, TransactionDisposition.VoteAbort)
    driver.testHaveLocalProposal shouldBe false
    accepts(messenger) shouldBe Nil
  }

  test("an abort-certain tally still proposes abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())

    promiseFrom(driver, ds0, TransactionDisposition.VoteAbort)
    promiseFrom(driver, ds1, TransactionDisposition.VoteAbort)
    accepts(messenger).map(a => (a.to, a.value)).toSet shouldBe Set((ds0, false), (ds1, false), (ds2, false))
  }

  test("a quorum reporting an accepted commit is proposed over an abort vote") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(2, 0, 0, txd), new TFinalizer(), _ => ())

    promiseFrom(driver, ds0, TransactionDisposition.VoteCommit, accepted = Some((ProposalId(1, 1), true)))
    promiseFrom(driver, ds1, TransactionDisposition.VoteAbort)

    accepts(messenger).map(a => (a.to, a.value)).toSet shouldBe Set((ds0, true), (ds1, true), (ds2, true))
  }

  test("a driver holding a proof does not propose abort on an undecided tally") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(2, 0, 0, txd), new TFinalizer(), _ => ())

    driver.receiveTxResolved(TxResolved(ds0, ds0, txd.transactionId, committed = true, late = false, proof = true))
    val round = driver.testProposalId
    promiseFrom(driver, ds0, TransactionDisposition.VoteCommit, proposal = round)
    promiseFrom(driver, ds1, TransactionDisposition.VoteAbort, proposal = round)

    accepts(messenger).filter(!_.value) shouldBe Nil
  }

  // paxos-transactions 7.6: proposer ids are pool indexes, so a store of another pool with the leader's index would
  // share its ids
  test("a driver on a secondary-pool store is learn-only: no proposal and no Accept, but it learns the outcome") {
    val otherPool = PoolId(java.util.UUID.randomUUID())
    val otherObj = DataObjectPointer(ObjectId(java.util.UUID.randomUUID()), otherPool, Array[Byte]())
    val txd = mktxd(simpleObj, List(DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite),
      DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite)))
    val secondary = StoreId(otherPool, 0)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    val finalizedSent = Promise[Unit]()
    val driver = new TTD(secondary, messenger, mkprep(1, 0, 0, txd), finalizer, _ => finalizedSent.trySuccess(()))

    driver.designatedLeader shouldBe false
    driver.mayPropose shouldBe false

    for from <- List(ds0, ds1, ds2, StoreId(otherPool, 0), StoreId(otherPool, 1), StoreId(otherPool, 2)) do
      promiseFrom(driver, from, TransactionDisposition.VoteCommit, accepted = Some((ProposalId(1, 0), true)))

    driver.testHaveLocalProposal shouldBe false
    accepts(messenger) shouldBe Nil

    driver.receiveTxResolved(TxResolved(secondary, ds1, txd.transactionId, committed = true, late = false,
      proof = false))
    scala.concurrent.Await.result(finalizedSent.future, scala.concurrent.duration.Duration(5, "s"))
    finalizer.created shouldBe true
    accepts(messenger) shouldBe Nil
  }

  // paxos-transactions 10.6, status cache
  test("a conflicting TxResolved or TxFinalized after a resolution is reported and changes nothing") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    var reported: List[String] = Nil
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(autoComplete = false), _ => (),
      start = TransactionDriver.Start(onSplitDecision = (_, detail) => reported = detail :: reported))

    commitBy(driver, txd)
    driver.hasOutcome shouldBe true
    reported shouldBe Nil

    driver.receiveTxResolved(TxResolved(ds0, ds1, txd.transactionId, committed = false, late = false, proof = false))
    reported.size shouldBe 1
    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    reported.size shouldBe 2

    messenger.clear()
    driver.heartbeat()
    driver.heartbeat()
    messenger.messages.collect { case f: TxFinalized => f.committed }.toSet should not contain false
  }

  test("an agreeing TxResolved after a resolution is not reported") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    var reported = 0
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), new TFinalizer(autoComplete = false), _ => (),
      start = TransactionDriver.Start(onSplitDecision = (_, _) => reported += 1))

    commitBy(driver, txd)
    driver.receiveTxResolved(TxResolved(ds0, ds1, txd.transactionId, committed = true, late = false, proof = false))
    reported shouldBe 0
  }

  test("A driver sends heartbeats only once it has outlived a heartbeat period") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())
    messenger.messages = Nil

    driver.heartbeat()
    assert(messenger.messages.isEmpty)

    driver.heartbeat()
    assert(messenger.messages.size == txd.allDataStores.size)
    assert(messenger.messages.forall(_.isInstanceOf[org.aspen_ddp.aspen.common.network.TxHeartbeat]))
  }

  // The heartbeat snapshots finalized, the outcome and late under the driver's lock: onFinalized writes finalized before
  // the outcome, so an unlocked read in between saw a committed transaction as aborted. The window lies between two
  // writes inside one synchronized block, so it cannot be opened deterministically; this pins the expected outcome
  test("a driver finalized by another driver's TxFinalized heartbeats the committed outcome") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    messenger.clear()
    driver.heartbeat()

    val finals = messenger.messages.collect { case f: TxFinalized => (f.committed, f.late) }
    finals.size should be (txd.allDataStores.size)
    finals.toSet should be (Set((true, false)))
  }

  test("A driver that has not finalized is reported as stuck, at intervals") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())
    val start = System.nanoTime()

    assert(!driver.reportIfStuck(start))

    val late = start + Tx.StuckReportAfter.toNanos + 1000000000L
    assert(driver.reportIfStuck(late))
    assert(!driver.reportIfStuck(late + 1))
    assert(driver.reportIfStuck(late + Tx.StuckReportEvery.toNanos))
  }

  test("Simple PrepareResponse Handling") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver = new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1, 0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be(Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1, 0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages.toSet should be(Set(
      TxAccept(ds0, ds0, txd.transactionId, ProposalId(1, 0), value = true),
      TxAccept(ds1, ds0, txd.transactionId, ProposalId(1, 0), value = true),
      TxAccept(ds2, ds0, txd.transactionId, ProposalId(1, 0), value = true)))
  }

  // paxos-transactions 7.3: the local proposal outlives its round, so the tally does not run again after a round
  // change. The new round's quorum must still send its Accept at once, not wait for a periodic retransmit (this driver
  // has no timer)
  test("after a round change, the new round's prepare quorum sends its Accept at once, and only once") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())

    promiseFrom(driver, ds0, TransactionDisposition.VoteCommit)
    promiseFrom(driver, ds1, TransactionDisposition.VoteCommit)
    driver.testHaveLocalProposal shouldBe true
    accepts(messenger).map(_.proposalId).toSet shouldBe Set(ProposalId(1, 0))

    // A competing proposer promised a higher id: the driver moves to a later round
    driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, ds1, txd.transactionId, ProposalId(1, 0),
      Left(TxAcceptResponse.Nack(ProposalId(5, 1)))))
    val round2 = driver.testProposalId
    round2 should be > ProposalId(5, 1)
    messenger.clear()

    promiseFrom(driver, ds0, TransactionDisposition.VoteCommit, proposal = round2)
    accepts(messenger) shouldBe Nil
    promiseFrom(driver, ds1, TransactionDisposition.VoteCommit, proposal = round2)
    accepts(messenger).toSet shouldBe Set(ds0, ds1, ds2).map(to => TxAccept(to, ds0, txd.transactionId, round2, value = true))

    // A further promise of the same round queues no second batch
    messenger.clear()
    promiseFrom(driver, ds2, TransactionDisposition.VoteCommit, proposal = round2)
    accepts(messenger) shouldBe Nil
  }

  // A recovery driver for a transaction the other stores forgot: a store that has applied it answers with the commit
  // rather than a vote (Tx.proveCommitted). The votes it gets meanwhile must not decide abort first
  test("A recovery driver waits for a store that proves the commit rather than deciding abort on the other votes") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    // Called after onFinalized has sent its messages
    val finalizedSent = Promise[Unit]()
    val driver = new TTD(ds0, messenger, mkprep(2, 0, 0, txd), new TFinalizer(), _ => finalizedSent.trySuccess(()))

    def vote(from: StoreId, disposition: TransactionDisposition.Value): Unit =
      driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, from, txd.transactionId,
        Right(TxPrepareResponse.Promise(None)), ProposalId(1, 0), disposition, Nil))

    vote(ds0, TransactionDisposition.VoteCommit) // The recovered commit vote
    vote(ds2, TransactionDisposition.VoteAbort) // A store that never received the transaction
    messenger.messages.collect { case a: TxAccept => a } should be (Nil)

    driver.receiveTxResolved(org.aspen_ddp.aspen.common.network.TxResolved(ds0, ds1, txd.transactionId,
      committed = true, late = false, proof = false))

    // The finalizer completes on another thread
    scala.concurrent.Await.result(finalizedSent.future, scala.concurrent.duration.Duration(5, "s"))
    messenger.messages.collect { case f: TxFinalized => f.committed }.toSet should be (Set(true))
  }

  test("Ignore invalid acceptors") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds3, // invalid, poolIndex doesn't host a slice
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      StoreId(PoolId(java.util.UUID.randomUUID()), 1), // invalid, poolUUID doesn't match
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages.toSet should be (Set(
      TxAccept(ds0,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds1,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds2,ds0,txd.transactionId,ProposalId(1,0),value = true)))
  }

  test("Multi-object PrepareResponse Handling") {
    val otherPool = PoolId(java.util.UUID.randomUUID())
    val otherObj = DataObjectPointer(ObjectId(java.util.UUID.randomUUID()), otherPool)

    val ods0 = StoreId(otherPool, 0)
    val ods1 = StoreId(otherPool, 1)

    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite) ::Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages.toSet should be (Set(
      TxAccept(ds0,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds1,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds2,ds0,txd.transactionId,ProposalId(1,0),value = true)))
  }

  test("Multi-object PrepareResponse Handling - Abort") {
    val otherPool = PoolId(java.util.UUID.randomUUID())
    val otherObj = DataObjectPointer(ObjectId(java.util.UUID.randomUUID()), otherPool)

    val ods0 = StoreId(otherPool, 0)
    val ods1 = StoreId(otherPool, 1)
    val ods2 = StoreId(otherPool, 2)

    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite) ::Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds2,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteAbort,
      Nil))

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods2,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteAbort,
      Nil))

    messenger.messages.toSet should be (Set(
      TxAccept(ds0,ds0,txd.transactionId,ProposalId(1,0),value = false),
      TxAccept(ds1,ds0,txd.transactionId,ProposalId(1,0),value = false),
      TxAccept(ds2,ds0,txd.transactionId,ProposalId(1,0),value = false)))
  }

  // A store's vote is fixed when the prepare is first evaluated, so a re-sent prepare gets the same collision back.
  // Dropping responses that collide with a completed transaction (txdA) until it left the store's status cache
  // stalled the colliding transaction for the cache's lifetime.
  test("Abort votes colliding with a completed transaction resolve the transaction promptly") {
    val txdA = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver = new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    for from <- List(ds0, ds1) do
      driver.receiveTxPrepareResponse(TxPrepareResponse(
        ds0,
        from,
        txd.transactionId,
        Right(TxPrepareResponse.Promise(None)),
        ProposalId(1, 0),
        TransactionDisposition.VoteAbort,
        List(txdA.transactionId)))

    messenger.messages.toSet should be(Set(
      TxAccept(ds0, ds0, txd.transactionId, ProposalId(1, 0), value = false),
      TxAccept(ds1, ds0, txd.transactionId, ProposalId(1, 0), value = false),
      TxAccept(ds2, ds0, txd.transactionId, ProposalId(1, 0), value = false)))

    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(
        ds0,
        from,
        txd.transactionId,
        ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(false))))

    completed should be(true)
  }

  test("Simple AcceptResponse Handling - Abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds0,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.testFinalized should be (false)
    completed should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds1,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.testFinalized should be (true)
    completed should be (true)
  }

  test("Simple AcceptResponse Handling - Commit") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer(false)
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds0,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(true))))

    driver.testFinalized should be (false)
    completed should be (false)
    finalizer.created should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds1,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(true))))

    driver.testFinalized should be (false)
    completed should be (false)
    finalizer.created should be (true)
    finalizer.cancelled should be (false)

    driver.receiveTxFinalized(TxFinalized(ds0, ds0, txd.transactionId, true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))

    driver.testFinalized should be (true)
    completed should be (true)
    finalizer.cancelled should be (true)
  }

  test("A driver that counted a write threshold of commit votes gives the finalizer those voters") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val finalizer = new TFinalizer(false)
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), finalizer, _ => ())

    for from <- List(ds0, ds1) do
      driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, from, txd.transactionId,
        Right(TxPrepareResponse.Promise(None)), ProposalId(1, 0), TransactionDisposition.VoteCommit, Nil))
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))

    finalizer.created should be (true)
    finalizer.voters should be (Some(Some(Set(ds0, ds1))))
  }

  test("A driver that learned the commit without counting a write threshold of votes reports no voters") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val finalizer = new TFinalizer(false)
    val driver = new TTD(ds0, new TMessenger(), mkprep(2, 0, 0, txd), finalizer, _ => ())

    driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, ds0, txd.transactionId,
      Right(TxPrepareResponse.Promise(None)), ProposalId(1, 0), TransactionDisposition.VoteCommit, Nil))
    driver.receiveTxResolved(org.aspen_ddp.aspen.common.network.TxResolved(ds0, ds1, txd.transactionId,
      committed = true, late = false, proof = false))

    finalizer.created should be (true)
    finalizer.voters should be (Some(None))
  }

  test("a driver that counted a write threshold of commit votes gives the finalizer their incarnations") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val finalizer = new TFinalizer(false)
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), finalizer, _ => ())
    val incs = Map(ds0 -> StoreIncarnation(new java.util.UUID(1, 0)), ds1 -> StoreIncarnation(new java.util.UUID(1, 1)))

    for from <- List(ds0, ds1) do
      driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, from, txd.transactionId,
        Right(TxPrepareResponse.Promise(None)), ProposalId(1, 0), TransactionDisposition.VoteCommit, Nil,
        Some(incs(from))))
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))

    finalizer.created should be (true)
    finalizer.voters should be (Some(Some(Set(ds0, ds1))))
    finalizer.incarnations should be (Some(incs))
  }

  test("a driver that adopted the commit without counting votes still reports the incarnations it heard") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val finalizer = new TFinalizer(false)
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), finalizer, _ => ())
    val inc0 = StoreIncarnation(new java.util.UUID(2, 0))

    driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, ds0, txd.transactionId,
      Right(TxPrepareResponse.Promise(None)), ProposalId(1, 0), TransactionDisposition.VoteCommit, Nil, Some(inc0)))
    driver.receiveTxResolved(org.aspen_ddp.aspen.common.network.TxResolved(ds0, ds1, txd.transactionId,
      committed = true, late = false, proof = false))

    finalizer.created should be (true)
    finalizer.voters should be (Some(None))
    // ds1 and ds2 never answered a prepare, so the driver knows no incarnation for them
    finalizer.incarnations should be (Some(Map(ds0 -> inc0)))
  }

  test("a reply without an incarnation does not erase one already heard") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val finalizer = new TFinalizer(false)
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), finalizer, _ => ())
    val inc0 = StoreIncarnation(new java.util.UUID(3, 0))

    for inc <- List(Some(inc0), None) do
      driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, ds0, txd.transactionId,
        Right(TxPrepareResponse.Promise(None)), ProposalId(1, 0), TransactionDisposition.VoteCommit, Nil, inc))
    driver.receiveTxResolved(org.aspen_ddp.aspen.common.network.TxResolved(ds0, ds1, txd.transactionId,
      committed = true, late = false, proof = false))

    finalizer.created should be (true)
    finalizer.incarnations should be (Some(Map(ds0 -> inc0)))
  }

  test("a store that reports two different incarnations is recorded as unknown") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val finalizer = new TFinalizer(false)
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), finalizer, _ => ())
    val inc0 = StoreIncarnation(new java.util.UUID(4, 0))
    val inc0b = StoreIncarnation(new java.util.UUID(4, 1))
    val inc1 = StoreIncarnation(new java.util.UUID(4, 2))

    // ds0 reports two incarnations, then the first again: once conflicted, it stays unknown
    for (from, inc) <- List(ds0 -> inc0, ds1 -> inc1, ds0 -> inc0b, ds0 -> inc0) do
      driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, from, txd.transactionId,
        Right(TxPrepareResponse.Promise(None)), ProposalId(1, 0), TransactionDisposition.VoteCommit, Nil, Some(inc)))
    driver.receiveTxResolved(org.aspen_ddp.aspen.common.network.TxResolved(ds0, ds1, txd.transactionId,
      committed = true, late = false, proof = false))

    finalizer.created should be (true)
    finalizer.incarnations should be (Some(Map(ds1 -> inc1)))
  }

  test("Simple AcceptResponse Handling - Ignore invalid acceptor") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds0,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.testFinalized should be (false)
    completed should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds3,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.testFinalized should be (false)
    completed should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds1,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.testFinalized should be (true)
    completed should be (true)
  }

  // Late commits (docs/superpowers/specs/2026-11-09-replay-pins-design.md, 1b and 1c)

  private def promise(driver: TTD, txd: TransactionDescription, from: StoreId, disposition: TransactionDisposition.Value,
                      lastAccepted: Option[(ProposalId, Boolean)] = None): Unit =
    driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, from, txd.transactionId,
      Right(TxPrepareResponse.Promise(lastAccepted)), driver.testProposalId, disposition, Nil))

  private def accepted(driver: TTD, txd: TransactionDescription, from: StoreId): Unit =
    driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, driver.testProposalId,
      Right(TxAcceptResponse.Accepted(true))))

  private def proofFrom(driver: TTD, txd: TransactionDescription, from: StoreId): Unit =
    driver.receiveTxResolved(TxResolved(ds0, from, txd.transactionId, committed = true, late = false, proof = true))

  private def outcomes(messenger: TMessenger): List[TxMessage] =
    messenger.messages.filter(m => m.isInstanceOf[TxResolved] || m.isInstanceOf[TxFinalized])

  private def allLate(messages: List[TxMessage]): Boolean = messages.forall {
    case r: TxResolved => r.late && r.committed
    case f: TxFinalized => f.late && f.committed
    case _ => true
  }

  private def abortAccepts(messenger: TMessenger): List[TxAccept] =
    messenger.messages.collect { case a: TxAccept if !a.value => a }

  test("a proof with a quorum of memoryless promises resolves late, runs no finalizer, and sends late everywhere") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    proofFrom(driver, txd, ds1)
    completed should be (false)
    outcomes(messenger) should be (Nil)

    promise(driver, txd, ds0, TransactionDisposition.VoteAbort)
    completed should be (false)
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    completed should be (true)
    driver.testFinalized should be (true)
    finalizer.createCount should be (0)
    abortAccepts(messenger) should be (Nil)
    messenger.messages.collect { case f: TxFinalized => f.to }.toSet should be (txd.allDataStores.toSet)
    allLate(outcomes(messenger)) should be (true)

    messenger.clear()
    driver.heartbeat()
    driver.testResendPrepares()
    driver.testResendPrepare(ds1)
    messenger.messages.collect { case p: TxPrepare => p } should be (Nil)
    outcomes(messenger).collect { case r: TxResolved => r.to }.toSet should be (txd.allDataStores.toSet)
    outcomes(messenger).collect { case f: TxFinalized => f.to }.toSet should be (txd.allDataStores.toSet)
    allLate(outcomes(messenger)) should be (true)
  }

  // Past the horizon no store that forgot T answers a prepare, so a memoryless quorum shows only stores that never
  // accepted, as after an expiry commit (which no acceptor accepts): resolving late would cancel the expiry driver's
  // finalizer before its marks are written (paxos-transactions.md, late condition 1)
  test("past the horizon a proof with a quorum of memoryless promises does not resolve late") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true,
      start = TransactionDriver.Start(pastHorizon = () => true))

    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds0, TransactionDisposition.VoteAbort)
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    completed should be (false)
    driver.testFinalized should be (false)
    finalizer.createCount should be (0)
    abortAccepts(messenger) should be (Nil)
    outcomes(messenger) should be (Nil)

    // The expiry driver's TxFinalized, sent once its marks are durable, finalizes it with the stores it lists
    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = false,
      owed = Set(ds2), txd.startTimestamp))
    completed should be (true)
    val finals = messenger.messages.collect { case f: TxFinalized => f }
    finals should not be (Nil)
    finals.foreach { f =>
      f.late should be (false)
      f.committed should be (true)
      f.owed should be (Set(ds2))
    }
  }

  test("past the horizon a proof holder still adopts an accepted commit of its post-proof round") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => (),
      start = TransactionDriver.Start(pastHorizon = () => true))

    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds0, TransactionDisposition.VoteCommit, Some((ProposalId(1, 2), true)))
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    outcomes(messenger) should be (Nil)
    messenger.messages.collect { case a: TxAccept => a.value }.toSet should be (Set(true))
  }

  // The counterexample to counting pre-proof promises: they may predate another driver's choice of commit at a higher
  // proposal, which an unfinalized prover (a muted rebuilt store) then shows. Only a post-proof round counts
  test("memoryless promises received before the proof do not make it late; a post-proof round's quorum does") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    // Votes reach a quorum but cannot decide: one commit vote, one abort vote, of three stores
    promise(driver, txd, ds0, TransactionDisposition.VoteCommit)
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)
    messenger.messages.collect { case a: TxAccept => a } should be (Nil)
    val preProofRound = driver.testProposalId
    messenger.clear()

    proofFrom(driver, txd, ds1)

    completed should be (false)
    outcomes(messenger) should be (Nil)
    driver.testProposalId should be > preProofRound
    messenger.messages.collect { case p: TxPrepare => p.proposalId }.toSet should be (Set(driver.testProposalId))

    // A late answer to the pre-proof round is discarded
    driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, ds1, txd.transactionId,
      Right(TxPrepareResponse.Promise(None)), preProofRound, TransactionDisposition.VoteAbort, Nil))
    completed should be (false)

    promise(driver, txd, ds0, TransactionDisposition.VoteCommit)
    completed should be (false)
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    completed should be (true)
    finalizer.createCount should be (0)
    abortAccepts(messenger) should be (Nil)
    outcomes(messenger) should not be (Nil)
    allLate(outcomes(messenger)) should be (true)
  }

  test("memoryless promises before the proof, then a post-proof round remembering the commit, commit normally") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    val finalizedSent = Promise[Unit]()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => finalizedSent.trySuccess(()))

    promise(driver, txd, ds0, TransactionDisposition.VoteCommit)
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    // Meanwhile another driver chose commit at a higher proposal and a store shows it
    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds0, TransactionDisposition.VoteCommit, Some((ProposalId(1, 2), true)))
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort, Some((ProposalId(1, 2), true)))

    finalizer.createCount should be (0)
    outcomes(messenger) should be (Nil)
    messenger.messages.collect { case a: TxAccept => a.value }.toSet should be (Set(true))

    for from <- List(ds0, ds2) do accepted(driver, txd, from)

    // The finalizer completes on another thread
    scala.concurrent.Await.result(finalizedSent.future, scala.concurrent.duration.Duration(5, "s"))
    finalizer.createCount should be (1)
    outcomes(messenger) should not be (Nil)
    outcomes(messenger).forall {
      case r: TxResolved => !r.late && r.committed
      case f: TxFinalized => !f.late && f.committed
      case _ => true
    } should be (true)
  }

  test("a proof with an accepted commit among the promises resolves normally and runs the finalizer") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    val finalizedSent = Promise[Unit]()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => finalizedSent.trySuccess(()))

    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds0, TransactionDisposition.VoteCommit, Some((ProposalId(1, 2), true)))
    promise(driver, txd, ds2, TransactionDisposition.VoteCommit)

    finalizer.createCount should be (0)
    messenger.messages.collect { case a: TxAccept => a.value }.toSet should be (Set(true))
    outcomes(messenger) should be (Nil)

    for from <- List(ds0, ds2) do accepted(driver, txd, from)

    // The finalizer completes on another thread
    scala.concurrent.Await.result(finalizedSent.future, scala.concurrent.duration.Duration(5, "s"))
    finalizer.createCount should be (1)
    outcomes(messenger) should not be (Nil)
    outcomes(messenger).forall {
      case r: TxResolved => !r.late && r.committed
      case f: TxFinalized => !f.late && f.committed
      case _ => true
    } should be (true)
  }

  // paxos-transactions 10.5: a proof means commit was chosen at some ballot c, and every value accepted at c or higher
  // is commit. An accepted abort can only predate c, so a post-proof quorum reporting no accepted commit is memoryless
  test("a proof holder whose post-proof quorum reports only an older accepted abort resolves late and sends no abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds0, TransactionDisposition.VoteAbort, Some((ProposalId(1, 2), false)))
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    completed should be (true)
    finalizer.createCount should be (0)
    abortAccepts(messenger) should be (Nil)
    outcomes(messenger) should not be (Nil)
    allLate(outcomes(messenger)) should be (true)
  }

  test("a proof holder adopts an accepted commit even when an accepted abort has a higher ballot") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => ())

    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds0, TransactionDisposition.VoteAbort, Some((ProposalId(1, 1), true)))
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort, Some((ProposalId(1, 2), false)))

    abortAccepts(messenger) should be (Nil)
    messenger.messages.collect { case a: TxAccept => a.value }.toSet should be (Set(true))
    driver.testFinalized should be (false)
    outcomes(messenger) should be (Nil)
  }

  test("a pre-proof local abort proposal is not sent once the post-proof round reports an accepted commit") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => ())

    // A quorum of abort votes before the proof: the driver proposes abort at its first round
    for from <- List(ds0, ds1, ds2) do promise(driver, txd, from, TransactionDisposition.VoteAbort)
    driver.testHaveLocalProposal should be (true)
    messenger.clear()

    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds0, TransactionDisposition.VoteAbort, Some((ProposalId(1, 2), true)))
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    abortAccepts(messenger) should be (Nil)
    messenger.messages.collect { case a: TxAccept => a.value }.toSet should be (Set(true))
  }

  test("a driver holding a proof never proposes abort, even when votes say abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => ())

    proofFrom(driver, txd, ds1)
    // A quorum of abort votes, one remembering an accepted commit: Paxos adopts the accepted value
    promise(driver, txd, ds0, TransactionDisposition.VoteAbort, Some((ProposalId(1, 2), true)))
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    driver.testHaveLocalProposal should be (false)
    abortAccepts(messenger) should be (Nil)
    messenger.messages.collect { case a: TxAccept => a.value }.toSet should be (Set(true))
    finalizer.createCount should be (0)
    driver.testFinalized should be (false)
  }

  test("a driver holding a proof without a prepare quorum neither resolves nor proposes") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    proofFrom(driver, txd, ds1)
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)
    // Not an acceptor: does not count toward the quorum
    promise(driver, txd, ds3, TransactionDisposition.VoteAbort)

    completed should be (false)
    driver.testFinalized should be (false)
    driver.testHaveLocalProposal should be (false)
    finalizer.createCount should be (0)
    messenger.messages.collect { case a: TxAccept => a } should be (Nil)
    outcomes(messenger) should be (Nil)
  }

  test("a finalized-committed late TxFinalized resolves late") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = true, owed = Set(),
      startTimestamp = HLCTimestamp.now))

    completed should be (true)
    finalizer.createCount should be (0)
    outcomes(messenger) should not be (Nil)
    allLate(outcomes(messenger)) should be (true)

    messenger.clear()
    driver.heartbeat()
    outcomes(messenger) should not be (Nil)
    allLate(outcomes(messenger)) should be (true)
  }

  test("a late TxFinalized cancels a finalizer already started") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer(autoComplete = false)
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))
    finalizer.createCount should be (1)
    messenger.clear()

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = true, owed = Set(),
      startTimestamp = HLCTimestamp.now))

    completed should be (true)
    finalizer.cancelled should be (true)
    messenger.messages.collect { case f: TxFinalized => f } should not be (Nil)
    allLate(outcomes(messenger)) should be (true)
  }

  test("a driver's TxFinalized carries the finalizer's owed set; a late or abort TxFinalized carries none") {
    def owedSent(m: TMessenger) = m.messages.collect { case f: TxFinalized => f.owed }.toSet

    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)

    // own finalizer
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    finalizer.handedOff = Set(ds1)
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => ())
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))
    // The finalizer completes on another thread
    val deadline = System.nanoTime() + 5_000_000_000L
    while !driver.testFinalized && System.nanoTime() < deadline do Thread.sleep(5)
    driver.testFinalized should be (true)
    messenger.clear()
    driver.heartbeat()
    owedSent(messenger) should be (Set(Set(ds1)))

    // late
    val m2 = new TMessenger()
    val d2 = new TTD(ds0, m2, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())
    d2.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = true, owed = Set(ds1),
      startTimestamp = HLCTimestamp.now))
    owedSent(m2) should be (Set(Set[StoreId]()))

    // abort
    val m3 = new TMessenger()
    val d3 = new TTD(ds0, m3, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())
    d3.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = false, late = false, owed = Set(ds1),
      startTimestamp = HLCTimestamp.now))
    owedSent(m3) should be (Set(Set[StoreId]()))
  }

  for handedOff <- List(Set[StoreId](), Set(ds0, ds2)) do
    test(s"another driver's TxFinalized finalizes at once with received owed and the planned set (planned: ${handedOff.map(_.poolIndex)})") {
      val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
      val messenger = new TMessenger()
      val finalizer = new TFinalizer(autoComplete = false) // The mark's write never lands
      finalizer.handedOff = handedOff
      var completed = false
      val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)
      for from <- List(ds0, ds1) do
        driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
          Right(TxAcceptResponse.Accepted(true))))
      finalizer.createCount should be (1)
      messenger.clear()

      driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = false, owed = Set(ds1),
        startTimestamp = HLCTimestamp.now))

      // Cancelled first, so no mark is handed off after the planned set is read
      finalizer.cancelled should be (true)
      finalizer.readCancelled should be (Some(true))
      completed should be (true)
      driver.testFinalized should be (true)
      val sent = messenger.messages.collect { case f: TxFinalized => (f.owed, f.late) }.toSet
      sent should be (Set((handedOff + ds1, false)))
      messenger.clear()
      driver.heartbeat()
      messenger.messages.collect { case f: TxFinalized => (f.owed, f.late) }.toSet should be (Set((handedOff + ds1, false)))
    }

  test("a driver finalized by another driver's TxFinalized sends no TxResolved, and a later late message changes nothing") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer(autoComplete = false)
    finalizer.handedOff = Set(ds0)
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => ())
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))
    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = false, owed = Set(ds1),
      startTimestamp = HLCTimestamp.now))
    messenger.clear()

    // Late messages reach a finalized driver: nothing they do can drop the planned stores or send late
    driver.receiveTxResolved(TxResolved(ds0, ds2, txd.transactionId, committed = true, late = true, proof = false))
    driver.receiveTxFinalized(TxFinalized(ds0, ds2, txd.transactionId, committed = true, late = true, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    driver.heartbeat()
    messenger.messages.collect { case r: TxResolved => r } should be (Nil)
    messenger.messages.collect { case f: TxFinalized => (f.owed, f.late) }.toSet should be (Set((Set(ds0, ds1), false)))
  }

  test("a cancelled marker whose write completes the finalizer later changes nothing") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer(autoComplete = false)
    val done = Promise[Unit]()
    finalizer.completion = Some(done)
    finalizer.handedOff = Set(ds0)
    val calls = new java.util.concurrent.atomic.AtomicInteger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => calls.incrementAndGet())
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))
    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = false, owed = Set(ds1),
      startTimestamp = HLCTimestamp.now))
    driver.testFinalized should be (true)
    calls.get should be (1)

    // The completion callback, on the global context, reaches onFinalized again: it must change nothing
    done.success(())
    val deadline = System.nanoTime() + 5_000_000_000L
    while calls.get < 2 && System.nanoTime() < deadline do Thread.sleep(1)
    calls.get should be (2)
    messenger.clear()
    driver.heartbeat()
    messenger.messages.collect { case f: TxFinalized => f.owed }.toSet should be (Set(Set(ds0, ds1)))
  }

  test("a late resolution after the owed set is chosen still sends an empty owed") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())
    driver.receiveTxResolved(TxResolved(ds0, ds1, txd.transactionId, committed = true, late = true, proof = false))
    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = false, owed = Set(ds1),
      startTimestamp = HLCTimestamp.now))
    messenger.messages.collect { case f: TxFinalized => f.owed }.toSet should be (Set(Set[StoreId]()))
  }

  test("a late TxResolved resolves late at once") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer()
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    driver.receiveTxResolved(TxResolved(ds0, ds1, txd.transactionId, committed = true, late = true, proof = false))

    completed should be (true)
    finalizer.createCount should be (0)
    abortAccepts(messenger) should be (Nil)
    messenger.messages.collect { case f: TxFinalized => f.to }.toSet should be (txd.allDataStores.toSet)
    allLate(outcomes(messenger)) should be (true)
  }

  test("a proof reaching a driver that already resolved commit leaves its finalizer running") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer(autoComplete = false)
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => completed = true)

    for from <- List(ds0, ds1) do
      promise(driver, txd, from, TransactionDisposition.VoteCommit)
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(true))))
    finalizer.createCount should be (1)

    proofFrom(driver, txd, ds2)
    promise(driver, txd, ds2, TransactionDisposition.VoteCommit)

    completed should be (false)
    finalizer.cancelled should be (false)
    messenger.messages.collect { case f: TxFinalized => f } should be (Nil)
  }

  test("a proof reaching a finalized driver starts no round") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), new TFinalizer(), _ => ())

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    val round = driver.testProposalId
    messenger.clear()

    proofFrom(driver, txd, ds2)

    driver.testProposalId should be (round)
    messenger.messages.collect { case p: TxPrepare => p } should be (Nil)
  }

  test("a resolved driver holding a proof sends no accepts for an adopted value") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val finalizer = new TFinalizer(autoComplete = false)
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), finalizer, _ => ())

    // Resolved by another driver; the proof then arrives, and the driver holds it without a new round
    driver.receiveTxResolved(TxResolved(ds0, ds1, txd.transactionId, committed = true, late = false, proof = false))
    proofFrom(driver, txd, ds2)
    messenger.clear()

    promise(driver, txd, ds0, TransactionDisposition.VoteCommit, Some((ProposalId(1, 2), true)))
    promise(driver, txd, ds2, TransactionDisposition.VoteAbort)

    messenger.messages.collect { case a: TxAccept => a } should be (Nil)
    finalizer.createCount should be (1)
    finalizer.cancelled should be (false)
  }

  // paxos-transactions 7.7: finalization has a failure path. A failed finalizer is started again from scratch
  // after a backoff; the transaction is never finalized without its actions
  test("a failed finalizer is started again after a backoff, and TxFinalized waits for one that completes") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val tasks = new StepTasks
    val factory = new StepFactory(Some(Set(ds2)), Some(Set(ds1)))
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), factory, _ => completed = true, tasks,
      ExecutionContext.parasitic)
    tasks.tasks = Nil
    commitBy(driver, txd)
    factory.created.size should be (1)

    // The marker gave up part way: some of its entries (for ds2) may have landed
    factory.created.head.done.failure(new Exception("marker gave up"))
    factory.created.head.cancelled should be (true)
    completed should be (false)
    tasks.tasks.map(_.delay) should be (List(TransactionDriver.FinalizerRetryInitial))

    tasks.runAll()
    factory.created.size should be (2)
    completed should be (false)
    messenger.clear()

    factory.created(1).done.success(())
    completed should be (true)
    driver.testFinalized should be (true)
    // The failed finalizer's planned stores are listed as owed with the completed one's
    messenger.messages.collect { case f: TxFinalized => f.owed }.toSet should be (Set(Set(ds1, ds2)))
  }

  test("a finalizer that cannot be created is started again, and the resolution is still sent") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val tasks = new StepTasks
    val factory = new StepFactory(None, Some(Set()))
    var completed = false
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), factory, _ => completed = true, tasks,
      ExecutionContext.parasitic)
    tasks.tasks = Nil
    messenger.clear()
    commitBy(driver, txd)
    factory.created should be (Nil)
    messenger.messages.collect { case r: TxResolved => r.committed }.toSet should be (Set(true))
    tasks.tasks.size should be (1)

    tasks.runAll()
    factory.created.size should be (1)
    factory.created.head.done.success(())
    completed should be (true)
  }

  test("repeated finalizer failures back off, doubling up to the cap") {
    TransactionDriver.finalizerRetryDelay(1) should be (TransactionDriver.FinalizerRetryInitial)
    TransactionDriver.finalizerRetryDelay(2) should be (TransactionDriver.FinalizerRetryInitial * 2)
    TransactionDriver.finalizerRetryDelay(6) should be (TransactionDriver.FinalizerRetryInitial * 32)
    TransactionDriver.finalizerRetryDelay(7) should be (TransactionDriver.FinalizerRetryMax)
    TransactionDriver.finalizerRetryDelay(1000) should be (TransactionDriver.FinalizerRetryMax)

    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val tasks = new StepTasks
    val factory = new StepFactory()
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), factory, _ => (), tasks,
      ExecutionContext.parasitic)
    tasks.tasks = Nil
    commitBy(driver, txd)
    for n <- 1 to 3 do
      factory.created.last.done.failure(new Exception(s"failure $n"))
      tasks.tasks.map(_.delay) should be (List(TransactionDriver.finalizerRetryDelay(n)))
      tasks.runAll()
    factory.created.size should be (4)
  }

  test("a committed transaction still unfinalized past FinalizationStallAfter reports a stall, with its failures") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val tasks = new StepTasks
    val factory = new StepFactory()
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), factory, _ => (), tasks,
      ExecutionContext.parasitic)
    tasks.tasks = Nil
    val before = System.nanoTime()
    driver.finalizationStall(before + TransactionDriver.FinalizationStallAfter.toNanos * 2) should be (None)

    commitBy(driver, txd)
    val resolvedBy = System.nanoTime()
    val err = new Exception("action gave up")
    factory.created.last.done.failure(err)
    tasks.runAll()
    factory.created.last.done.failure(err)

    driver.finalizationStall(before + TransactionDriver.FinalizationStallAfter.toNanos - 1) should be (None)
    val stall = driver.finalizationStall(resolvedBy + TransactionDriver.FinalizationStallAfter.toNanos).get
    stall.transactionId should be (txd.transactionId)
    stall.failures should be (2)
    stall.lastFailure should be (Some(err))
    stall.age should be >= TransactionDriver.FinalizationStallAfter

    tasks.runAll()
    factory.created.last.done.success(())
    driver.testFinalized should be (true)
    driver.finalizationStall(resolvedBy + TransactionDriver.FinalizationStallAfter.toNanos * 2) should be (None)
  }

  test("an aborted transaction never reports a finalization stall") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), new StepFactory(), _ => (), new StepTasks,
      ExecutionContext.parasitic)
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, ProposalId(1, 0),
        Right(TxAcceptResponse.Accepted(false))))
    driver.testFinalized should be (true)
    driver.finalizationStall(System.nanoTime() + TransactionDriver.FinalizationStallAfter.toNanos * 2) should be (None)
  }

  test("another driver's TxFinalized during a pending restart finalizes at once, listing the failed plan, and starts no finalizer") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val tasks = new StepTasks
    val factory = new StepFactory(Some(Set(ds2)))
    val driver = new TTD(ds0, messenger, mkprep(1, 0, 0, txd), factory, _ => (), tasks, ExecutionContext.parasitic)
    tasks.tasks = Nil
    commitBy(driver, txd)
    factory.created.head.done.failure(new Exception("marker gave up"))
    messenger.clear()

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = false, owed = Set(ds1),
      startTimestamp = HLCTimestamp.now))
    driver.testFinalized should be (true)
    messenger.messages.collect { case f: TxFinalized => f.owed }.toSet should be (Set(Set(ds1, ds2)))

    tasks.runAll()
    factory.created.size should be (1)
  }

  test("a late resolution during a pending restart, or a shutdown, starts no finalizer") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)

    val tasks = new StepTasks
    val factory = new StepFactory()
    val driver = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), factory, _ => (), tasks, ExecutionContext.parasitic)
    tasks.tasks = Nil
    commitBy(driver, txd)
    factory.created.head.done.failure(new Exception("action gave up"))
    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = true, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    driver.testFinalized should be (true)
    tasks.runAll()
    factory.created.size should be (1)

    val tasks2 = new StepTasks
    val factory2 = new StepFactory()
    val driver2 = new TTD(ds0, new TMessenger(), mkprep(1, 0, 0, txd), factory2, _ => (), tasks2,
      ExecutionContext.parasitic)
    tasks2.tasks = Nil
    commitBy(driver2, txd)
    factory2.created.head.done.failure(new Exception("action gave up"))
    driver2.shutdown()
    tasks2.tasks.forall(_.cancelled) should be (true)
    tasks2.tasks.foreach(t => t.cancelled = false)
    tasks2.runAll()
    factory2.created.size should be (1)
    driver2.testFinalized should be (false)
  }
}
