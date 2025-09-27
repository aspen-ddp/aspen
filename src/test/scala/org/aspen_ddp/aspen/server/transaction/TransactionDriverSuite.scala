package org.aspen_ddp.aspen.server.transaction

import scala.concurrent.ExecutionContext.Implicits.global
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientResponse, TxAccept, TxAcceptResponse, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
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
  val arr = new Array[Byte](0)
  val oid = ObjectId(java.util.UUID.randomUUID())
  val simpleObj = DataObjectPointer(oid, poolId, None, Replication(3,2),
    Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))

  //def mkobj = ObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), new Array[StorePointer](0))

  def mktxd(optr: ObjectPointer, du: List[DataUpdate] = Nil, ru: List[RefcountUpdate] = Nil) = TransactionDescription(
    TransactionId(java.util.UUID.randomUUID()), HLCTimestamp(100), optr, 0, du ++ ru, Nil)

  def mkprep(paxosRound: Int, toPeer: Byte, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(StoreId(poolId,toPeer), StoreId(poolId,fromPeer), txd, ProposalId(paxosRound,fromPeer), Nil, Nil)

  class TMessenger extends Messenger {
    var messages: List[TxMessage] = List[TxMessage]()

    def sendClientResponse(msg: ClientResponse): Unit = ()

    def sendTransactionMessage(msg: TxMessage): Unit = messages = msg :: messages

    def sendTransactionMessages(msg: List[TxMessage]): Unit = messages = msg ++ messages

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

    def complete: Future[Unit] = if (autoComplete) Future.successful(()) else Promise[Unit]().future

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

  val noTxCache = new TransactionStatusCache()

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
      Nil), noTxCache)

    messenger.messages should be(Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1, 0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

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
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds3, // invalid, poolIndex doesn't host a slice
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      StoreId(PoolId(java.util.UUID.randomUUID()), 1), // invalid, poolUUID doesn't match
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages.toSet should be (Set(
      TxAccept(ds0,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds1,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds2,ds0,txd.transactionId,ProposalId(1,0),value = true)))
  }

  test("Multi-object PrepareResponse Handling") {
    val otherPool = PoolId(java.util.UUID.randomUUID())
    val otherObj = DataObjectPointer(ObjectId(java.util.UUID.randomUUID()), otherPool, None, Replication(3,2),
      Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))

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
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages.toSet should be (Set(
      TxAccept(ds0,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds1,ds0,txd.transactionId,ProposalId(1,0),value = true),
      TxAccept(ds2,ds0,txd.transactionId,ProposalId(1,0),value = true)))
  }

  test("Multi-object PrepareResponse Handling - Abort") {
    val otherPool = PoolId(java.util.UUID.randomUUID())
    val otherObj = DataObjectPointer(ObjectId(java.util.UUID.randomUUID()), otherPool, None, Replication(3,2),
      Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))

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
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ds2,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods0,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteCommit,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods1,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteAbort,
      Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
      ds0,
      ods2,
      txd.transactionId,
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(1,0),
      TransactionDisposition.VoteAbort,
      Nil), noTxCache)

    messenger.messages.toSet should be (Set(
      TxAccept(ds0,ds0,txd.transactionId,ProposalId(1,0),value = false),
      TxAccept(ds1,ds0,txd.transactionId,ProposalId(1,0),value = false),
      TxAccept(ds2,ds0,txd.transactionId,ProposalId(1,0),value = false)))
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