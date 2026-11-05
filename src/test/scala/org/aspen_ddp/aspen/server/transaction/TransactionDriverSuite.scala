package org.aspen_ddp.aspen.server.transaction

import scala.concurrent.ExecutionContext.Implicits.global
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientResponse, TxAccept, TxAcceptResponse, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse}
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
  }

  class TFinalizer(autoComplete: Boolean = true) extends TransactionFinalizer with TransactionFinalizer.Factory {
    var cancelled = false
    var created = false
    var peers: Set[StoreId] = Set[StoreId]()

    override def cancel(): Unit = cancelled = true

    def complete: Future[Unit] = if (autoComplete) Future.unit else Promise[Unit]().future

    def debugStatus: List[(String, Boolean)] = Nil

    override def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = {
      created = true
      this
    }

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = synchronized {
      peers = commitErrors.keySet
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

    driver.receiveTxFinalized(TxFinalized(ds0, ds0, txd.transactionId, true))

    driver.mayBeDiscarded should be (true)
    completed should be (true)
    finalizer.cancelled should be (true)
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
}