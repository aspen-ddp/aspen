package org.aspen_ddp.aspen.server.transaction

import scala.concurrent.ExecutionContext.Implicits.global
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientResponse, TxAccept, TxAcceptResponse, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, RefcountUpdate, TransactionDescription, TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.NoBackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.transaction.TransactionFinalizer.Factory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Future, Promise}


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

  def mkprep(paxosRound: Int, toPeer: Byte, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(StoreId(poolId,toPeer), StoreId(poolId,fromPeer), txd, ProposalId(paxosRound,fromPeer), Nil, Nil, fromClient = false)

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
             onComplete: TransactionId => Unit) extends TransactionDriver(storeId, messenger, NoBackgroundTaskManager, initialPrepare.txd, finalizerFactory) {
    override protected def onFinalized(committed: Boolean): Unit = {
      super.onFinalized(committed)
      onComplete(txd.transactionId)
    }

    def testResendPrepares(): Unit = sendPrepareMessages()
    def testResendPrepare(to: StoreId): Unit = sendPrepareMessage(to)
    def testHaveLocalProposal: Boolean = synchronized { proposer.haveLocalProposal }
    def testProposalId: ProposalId = synchronized { proposer.currentProposalId }
  }

  class TFinalizer(autoComplete: Boolean = true) extends TransactionFinalizer with TransactionFinalizer.Factory {
    var cancelled = false
    var created = false
    var peers: Set[StoreId] = Set[StoreId]()

    override def cancel(): Unit = cancelled = true

    def complete: Future[Unit] = if (autoComplete) Future.unit else Promise[Unit]().future

    def debugStatus: List[(String, Boolean)] = Nil

    var createCount = 0

    override def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = {
      created = true
      createCount += 1
      this
    }

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = synchronized {
      peers = commitErrors.keySet
    }

    /** None until the driver reports its commit voters */
    var voters: Option[Option[Set[StoreId]]] = None

    override def commitVoters(voters: Option[Set[StoreId]]): Unit = synchronized {
      this.voters = Some(voters)
    }
  }

}

class TransactionDriverSuite extends AnyFunSuite with Matchers {

  import TransactionDriverSuite._

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

    driver.mayBeDiscarded should be (false)
    completed should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds1,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.mayBeDiscarded should be (true)
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

    driver.mayBeDiscarded should be (false)
    completed should be (false)
    finalizer.created should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds1,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(true))))

    driver.mayBeDiscarded should be (false)
    completed should be (false)
    finalizer.created should be (true)
    finalizer.cancelled should be (false)

    driver.receiveTxFinalized(TxFinalized(ds0, ds0, txd.transactionId, true, late = false))

    driver.mayBeDiscarded should be (true)
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

    driver.mayBeDiscarded should be (false)
    completed should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds3,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.mayBeDiscarded should be (false)
    completed should be (false)

    driver.receiveTxAcceptResponse(TxAcceptResponse(
      ds0,
      ds1,
      txd.transactionId,
      ProposalId(1,0),
      Right(TxAcceptResponse.Accepted(false))))

    driver.mayBeDiscarded should be (true)
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
    driver.mayBeDiscarded should be (true)
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
    driver.mayBeDiscarded should be (false)
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
    driver.mayBeDiscarded should be (false)
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

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = true))

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

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, txd.transactionId, committed = true, late = true))

    completed should be (true)
    finalizer.cancelled should be (true)
    messenger.messages.collect { case f: TxFinalized => f } should not be (Nil)
    allLate(outcomes(messenger)) should be (true)
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
}
