package org.aspen_ddp.aspen.server.store

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, OpportunisticRebuild, ReadResponse, TxCommitted, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, FullObject, Insert, Key, KeySet, KeyValueOperation, KeyValueObjectPointer, Metadata, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, Value}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.paxos.PersistentState
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, ObjectUpdate, RefcountUpdate, RevisionLock, SharedRevisionLock, TransactionCollision, TransactionDescription, TransactionDisposition, TransactionId, TransactionRequirement, TransactionStatus}
import org.aspen_ddp.aspen.TempDirManager
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, CompletionHandler, MapBackend}
import org.aspen_ddp.aspen.server.store.cache.{HostObjectCache, ObjectCache, StoreObjectCache}
import org.aspen_ddp.aspen.server.transaction.{TransactionStatusCache, Tx}
import org.aspen_ddp.aspen.client.DataObjectState
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future, Promise}
import scala.language.implicitConversions

object FrontendSuite {

  val poolId = PoolId(new UUID(0,1))
  val storeId = StoreId(poolId, 0)
  val oid1 = ObjectId(new UUID(0,2))
  val oid2 = ObjectId(new UUID(0,3))
  val clientId = ClientId(new UUID(0,4))
  val txid1 = TransactionId(new UUID(0,5))
  val txid2 = TransactionId(new UUID(0,6))
  val rev0 = new ObjectRevision(new UUID(0, 7))
  val ida = Replication(3, 2)
  val op1 = DataObjectPointer(oid1, poolId, Array[Byte]())
  val otherPoolId = PoolId(new UUID(0,8))
  val otherPtr = DataObjectPointer(ObjectId(new UUID(0,9)), otherPoolId, Array[Byte]())

  class TestNet extends Messenger {

    var cr: Option[ClientResponse] = None
    var tx: Option[TxMessage] = None
    var sentTx: List[TxMessage] = Nil

    override def sendClientResponse(msg: ClientResponse): Unit = cr = Some(msg)

    override def sendTransactionMessage(msg: TxMessage): Unit =
      tx = Some(msg)
      sentTx = sentTx :+ msg

    override def sendTransactionMessages(msg: List[TxMessage]): Unit = Some(msg.head)

    override def dropCacheForStore(storeId: StoreId): Unit = ()

    def clientMessage(): Option[ClientResponse] = {
      val t = cr
      cr = None
      t
    }

    def txMessage(): Option[TxMessage] = {
      val t = tx
      tx = None
      t
    }
  }

  class TestCrl extends CrashRecoveryLog {

    var txSaved = false
    var txDel = false
    var aDrop = false
    var deleted: List[TransactionId] = Nil
    var dropped: List[TransactionId] = Nil

    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = Nil

    override def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]] =
      Future.successful(Nil)

    override def save(txid: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit =
      txSaved = true
      completionHandler()

    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit =
      aDrop = true
      dropped = dropped :+ txid

    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit =
      txDel = true
      deleted = deleted :+ txid
  }

  /** Holds commits until deliverCommits, applying each to the map only then, and counts backend reads and repairs.
    * With `deferReads`, reads are held too, until deliverReads.
    */
  class DeferredCommitBackend extends MapBackend(storeId):
    private var handler: Option[CompletionHandler] = None
    private var held: List[(CommitState, TransactionId)] = Nil
    private var heldReads: List[ObjectPointer] = Nil
    var reads: Int = 0
    var repairs: Int = 0
    var deferReads: Boolean = false

    override def setCompletionHandler(h: CompletionHandler): Unit =
      handler = Some(h)
      super.setCompletionHandler(h)

    override def read(pointer: ObjectPointer): Unit =
      reads += 1
      if deferReads then heldReads = heldReads :+ pointer else super.read(pointer)

    def deliverReads(): Unit =
      val ptrs = heldReads
      heldReads = Nil
      ptrs.foreach(super.read)

    override def repair(state: CommitState, complete: Promise[Unit]): Unit =
      repairs += 1
      super.repair(state, complete)

    override def commit(state: CommitState, transactionId: TransactionId): Unit =
      held = held :+ (state, transactionId)

    def heldCommits: Int = held.size

    /** Completes the oldest held commit */
    def deliverOne(result: Either[Unit, CommitError.Value] = Left(())): Unit =
      val (state, txid) = held.head
      held = held.tail
      if result.isLeft then
        if state.metadata.refcount.count == 0 then
          m -= state.objectId
        else
          m += (state.objectId -> new ObjectState(state.objectId, state.metadata, state.objectType, state.data))
      handler.foreach(_.complete(Commit(this.storeId, state.objectId, txid, result)))

    def deliverCommits(result: Either[Unit, CommitError.Value] = Left(())): Unit =
      while held.nonEmpty do deliverOne(result)

  def mkFrontend(): (Frontend, MapBackend, ObjectCache) =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val frontend = new Frontend(storeId, backend, cache, new TestNet, new TestCrl,
      new TransactionStatusCache())
    (frontend, backend, cache)

  /** Like mkFrontend but also returns the TestNet and routes backend completions to the Frontend */
  def mkFrontendWithNet(): (Frontend, MapBackend, ObjectCache, TestNet) =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, backend, cache, net)

  val kvPtr = KeyValueObjectPointer(oid2, poolId, Array[Byte]())
  def k(n: Int): Key = Key(Array[Byte](n.toByte))

  def mkKVObjectState(): ObjectState =
    val contents = (1 to 3).map { n =>
      k(n) -> new ValueState(Value(Array[Byte](n.toByte)), rev0, HLCTimestamp(n.toLong), None)
    }.toMap
    new ObjectState(oid2,
      Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      ObjectType.KeyValue,
      KVObjectState.encode(Some(k(0)), None, None, None, contents))

  /** Content keys of the KV object carried by the next ReadResponse sent to the TestNet */
  def readResponseKeys(net: TestNet): (Set[Int], Int) =
    net.clientMessage() match
      case Some(rr: ReadResponse) =>
        val cs = rr.result.toOption.get
        (KVObjectState.decode(cs.objectData.get).content.keySet.map(_.bytes(0).toInt), cs.sizeOnStore)
      case other => throw new AssertionError(s"Expected a ReadResponse, got $other")

  def mkPrepare(txd: TransactionDescription, fromClient: Boolean): TxPrepare =
    TxPrepare(storeId, StoreId(poolId, txd.designatedLeaderUID), txd,
      ProposalId.initialProposal(txd.designatedLeaderUID), Nil, Nil, fromClient)

  def mktxd(): TransactionDescription =
    TransactionDescription(txid1, HLCTimestamp(100), op1, 0, Nil, Nil, Some(clientId), Nil, Nil, ida,
      Map(poolId -> ida))

  def mkObjectState(objectId: ObjectId): ObjectState =
    new ObjectState(objectId,
      Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)))
}

class FrontendSuite extends AnyFunSuite with Matchers {
  import FrontendSuite._

  def checkPartialRead(preCache: Boolean): Unit =
    val (frontend, backend, cache, net) = mkFrontendWithNet()
    val os = mkKVObjectState()
    val fullSize = os.data.size

    backend.bootstrapAllocate(oid2, os.objectType, os.metadata, os.data)
    if preCache then
      cache.insert(os)

    frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, KeySet(List(k(1))))
    readResponseKeys(net) shouldBe (Set(1), fullSize)

    frontend.readObjectForNetwork(clientId, new UUID(1, 2), kvPtr, FullObject())
    readResponseKeys(net) shouldBe (Set(1, 2, 3), fullSize)

    KVObjectState.decode(cache.get(oid2).get.data).content.keySet.map(_.bytes(0).toInt) shouldBe Set(1, 2, 3)

  test("partial network read returns only requested keys and leaves the cache intact") {
    checkPartialRead(preCache = false)
  }

  test("partial network read of a cached object returns only requested keys and leaves the cache intact") {
    checkPartialRead(preCache = true)
  }

  def checkMetadataOnlyRead(preCache: Boolean): Unit =
    val (frontend, backend, cache, net) = mkFrontendWithNet()
    val os = mkKVObjectState()

    backend.bootstrapAllocate(oid2, os.objectType, os.metadata, os.data)
    if preCache then
      cache.insert(os)

    frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, MetadataOnly())
    net.clientMessage() match
      case Some(rr: ReadResponse) =>
        val cs = rr.result.toOption.get
        cs.objectData shouldBe None
        cs.sizeOnStore shouldBe os.data.size
        (cs.revision, cs.refcount, cs.timestamp) shouldBe (os.metadata.revision, os.metadata.refcount, os.metadata.timestamp)
      case other => throw new AssertionError(s"Expected a ReadResponse, got $other")

    cache.get(oid2).get.data.getByteArray shouldBe os.data.getByteArray

  test("metadata-only network read returns no object data") {
    checkMetadataOnlyRead(preCache = false)
  }

  test("metadata-only network read of a cached object returns no object data") {
    checkMetadataOnlyRead(preCache = true)
  }

  test("repair deletion removes the object from the backend") {
    val (frontend, backend, _) = mkFrontend()
    val os = mkObjectState(oid1)

    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    backend.get(oid1) should not be None

    val completion = Promise[Unit]()
    frontend.deleteObjectForRepair(oid1, ObjectPointer.EmptyArray, completion)

    Await.result(completion.future, Duration(5, SECONDS))

    backend.get(oid1) shouldBe None
  }

  test("repair deletion evicts the object from the object cache") {
    val (frontend, backend, cache) = mkFrontend()
    val os = mkObjectState(oid1)

    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    cache.insert(os)
    cache.get(oid1) should not be None

    val completion = Promise[Unit]()
    frontend.deleteObjectForRepair(oid1, ObjectPointer.EmptyArray, completion)

    Await.result(completion.future, Duration(5, SECONDS))

    cache.get(oid1) shouldBe None
  }

  // A client re-sends its prepare until it learns the outcome. If that reset the idle time, a
  // transaction whose designated leader is offline would never look idle to the other stores, so
  // none of them would take over and the transaction would hang for as long as the client waited.
  test("client prepare retransmissions do not reset a transaction's idle time") {
    val (frontend, _, _) = mkFrontend()
    val prepare = mkPrepare(mktxd(), fromClient = true)

    frontend.receiveTransactionMessage(prepare)
    val tx = frontend.transactions(txid1)
    val firstEvent = tx.lastEventTime

    Thread.sleep(5)
    frontend.receiveTransactionMessage(prepare)

    tx.lastEventTime shouldBe firstEvent
  }

  test("prepares from a transaction driver reset a transaction's idle time") {
    val (frontend, _, _) = mkFrontend()
    val prepare = mkPrepare(mktxd(), fromClient = false)

    frontend.receiveTransactionMessage(prepare)
    val tx = frontend.transactions(txid1)
    val firstEvent = tx.lastEventTime

    Thread.sleep(5)
    frontend.receiveTransactionMessage(prepare)

    tx.lastEventTime should be > firstEvent
  }

  test("repair deletion succeeds when the store does not have the object") {
    val (frontend, backend, _) = mkFrontend()

    backend.get(oid2) shouldBe None

    val completion = Promise[Unit]()
    frontend.deleteObjectForRepair(oid2, ObjectPointer.EmptyArray, completion)

    Await.result(completion.future, Duration(5, SECONDS))

    backend.get(oid2) shouldBe None
  }

  test("backend commit errors are reported in TxCommitted") {
    var handler: Option[CompletionHandler] = None
    val backend = new MapBackend(storeId):
      override def setCompletionHandler(h: CompletionHandler): Unit =
        handler = Some(h)
        super.setCompletionHandler(h)
      override def commit(state: CommitState, transactionId: TransactionId): Unit =
        handler.foreach(_.complete(Commit(storeId, state.objectId, transactionId,
          Right(CommitError.IOFailure))))
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)

    val txd = mktxd().copy(requirements = List(DataUpdate(op1, rev0, DataUpdateOperation.Overwrite)))
    val prepare = mkPrepare(txd, fromClient = true).copy(objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))))
    frontend.receiveTransactionMessage(prepare)
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))

    net.txMessage() match
      case Some(c: TxCommitted) => c.objectCommitErrors shouldBe List(oid1)
      case other => throw new AssertionError(s"Expected TxCommitted, got $other")
  }

  /** Prepares a transaction updating op1 (requiring `op1Revision`) and an object in another pool, and returns the
    * store's disposition. The other pool's object is neither hosted nor loaded by this store.
    */
  def crossPoolPrepare(op1Revision: ObjectRevision): (MapBackend, TestNet, TransactionDisposition.Value) =
    val (frontend, backend, _, net) = mkFrontendWithNet()
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)

    val txd = mktxd().copy(
      requirements = List(DataUpdate(op1, op1Revision, DataUpdateOperation.Overwrite),
                          DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)),
      poolIDAMap = Map(poolId -> ida, otherPoolId -> ida))
    val prepare = mkPrepare(txd, fromClient = true).copy(objectUpdates = List(
      ObjectUpdate(oid1, DataBuffer(Array[Byte](1))), ObjectUpdate(otherPtr.id, DataBuffer(Array[Byte](2)))))
    frontend.receiveTransactionMessage(prepare)

    net.txMessage() match
      case Some(r: TxPrepareResponse) =>
        frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
        (backend, net, r.disposition)
      case other => throw new AssertionError(s"Expected a TxPrepareResponse, got $other")

  test("a store votes commit when the transaction's other requirements are for objects in another pool") {
    val (backend, net, disposition) = crossPoolPrepare(rev0)

    disposition shouldBe TransactionDisposition.VoteCommit
    net.txMessage() match
      case Some(c: TxCommitted) => c.objectCommitErrors shouldBe Nil
      case other => throw new AssertionError(s"Expected TxCommitted, got $other")
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](1)
    backend.get(otherPtr.id) shouldBe None
  }

  test("a store votes abort on a failed requirement of an object it hosts in a cross-pool transaction") {
    val (_, _, disposition) = crossPoolPrepare(new ObjectRevision(new UUID(0, 10)))

    disposition shouldBe TransactionDisposition.VoteAbort
  }

  // ---- A prepare without the object data the store needs ----

  private def dataUpdateTxd = mktxd().copy(requirements = List(DataUpdate(op1, rev0, DataUpdateOperation.Overwrite)))

  private def withDataless(missingDataWait: Duration = Frontend.MissingDataWait)
                          (f: (Frontend, MapBackend, TestNet, TestCrl) => Unit): Unit =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val crl = new TestCrl
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), None, missingDataWait)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    f(frontend, backend, net, crl)

  private def clientPrepare(txd: TransactionDescription) = mkPrepare(txd, fromClient = true).copy(
    objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](7)))))

  private def expectVote(net: TestNet, vote: TransactionDisposition.Value): Unit = net.txMessage() match
    case Some(r: TxPrepareResponse) => r.disposition shouldBe vote
    case other => throw new AssertionError(s"Expected a TxPrepareResponse, got $other")

  test("a driver's prepare that overtakes the client's data gets no vote until the data arrives") {
    withDataless(): (frontend, backend, net, crl) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false)) // A retransmission
      net.txMessage() shouldBe None
      crl.txSaved shouldBe false

      frontend.receiveTransactionMessage(clientPrepare(txd))
      expectVote(net, TransactionDisposition.VoteCommit)
      crl.txSaved shouldBe true

      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
      net.txMessage() match
        case Some(c: TxCommitted) => c.objectCommitErrors shouldBe Nil
        case other => throw new AssertionError(s"Expected TxCommitted, got $other")
      backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
  }

  test("a store still waiting for data votes abort once the wait has passed") {
    withDataless(missingDataWait = Duration.Zero): (frontend, _, net, _) =>
      frontend.receiveTransactionMessage(mkPrepare(dataUpdateTxd, fromClient = false))
      expectVote(net, TransactionDisposition.VoteAbort)
  }

  test("a vote given without the data stands, but data that arrives later is still committed") {
    withDataless(missingDataWait = Duration.Zero): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      expectVote(net, TransactionDisposition.VoteAbort)

      frontend.receiveTransactionMessage(clientPrepare(txd))
      expectVote(net, TransactionDisposition.VoteAbort)

      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
      net.txMessage() match
        case Some(c: TxCommitted) => c.objectCommitErrors shouldBe Nil
        case other => throw new AssertionError(s"Expected TxCommitted, got $other")
      backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
  }

  test("a client's prepare without the data gets an abort vote at once: no more data is coming") {
    withDataless(): (frontend, _, net, _) =>
      frontend.receiveTransactionMessage(mkPrepare(dataUpdateTxd, fromClient = true))
      expectVote(net, TransactionDisposition.VoteAbort)
  }

  test("a driver's prepare needing no data from this store gets its vote at once") {
    withDataless(): (frontend, _, net, _) =>
      val txd = mktxd().copy(requirements = List(SharedRevisionLock(op1, rev0)))
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      expectVote(net, TransactionDisposition.VoteCommit)
  }

  private def committedMessages(net: TestNet): List[TxCommitted] = net.sentTx.collect { case c: TxCommitted => c }

  test("a commit that overtakes the client's data is applied when the data arrives and reported to the designated leader") {
    withDataless(): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      val leader = StoreId(poolId, txd.designatedLeaderUID)
      val recoveryDriver = StoreId(poolId, 2)
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false).copy(from = recoveryDriver))
      frontend.receiveTransactionMessage(TxResolved(storeId, recoveryDriver, txid1, committed = true))
      committedMessages(net) shouldBe Nil
      backend.get(oid1).get.metadata.revision shouldBe rev0

      frontend.receiveTransactionMessage(clientPrepare(txd))
      val committed = committedMessages(net)
      committed should not be empty
      committed.foreach: c =>
        c.to shouldBe leader
        c.objectCommitErrors shouldBe Nil
      backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
  }

  test("a resolution ends the wait for a vote, so a driver's later prepare is answered") {
    withDataless(): (frontend, _, net, _) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
      net.txMessage() shouldBe None

      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      net.txMessage() should not be None
  }

  test("a client's prepare without the data ends a commit's wait: no more data is coming") {
    withDataless(): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true))
      committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
      backend.get(oid1).get.metadata.revision shouldBe rev0
  }

  test("finalization ends a commit's wait for data") {
    withDataless(): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
      committedMessages(net) shouldBe Nil

      frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true))
      committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
      frontend.transactions.contains(txid1) shouldBe false

      // Too late: repair restores this store
      frontend.receiveTransactionMessage(clientPrepare(txd))
      backend.get(oid1).get.metadata.revision shouldBe rev0
  }

  test("finalization deletes the CRL entry of a committed transaction, whichever outcome a recovery driver reports") {
    withDataless(): (frontend, _, _, crl) =>
      val txd = dataUpdateTxd
      val recoveryDriver = StoreId(poolId, 2)
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
      val p = mkPrepare(txd, fromClient = false)
      frontend.receiveTransactionMessage(p.copy(from = recoveryDriver, proposalId = ProposalId(p.proposalId.number + 1, 2.toByte)))
      crl.txSaved shouldBe true
      crl.txDel shouldBe false

      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = false))
      crl.txDel shouldBe true
      frontend.transactions.contains(txid1) shouldBe false
  }

  test("finalization deletes the CRL entry a prepare saved after an abort") {
    withDataless(): (frontend, _, _, crl) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false))
      crl.txDel = false
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))

      frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false))
      crl.txDel shouldBe true
  }

  test("an abort does not wait for data") {
    withDataless(): (frontend, _, net, crl) =>
      frontend.receiveTransactionMessage(mkPrepare(dataUpdateTxd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false))
      crl.txDel shouldBe true
      committedMessages(net) shouldBe Nil
  }

  // ---- Requirement locks held between a commit vote and resolution ----

  val txid3 = TransactionId(new UUID(0, 11))
  val fenceOid = ObjectId(new UUID(0, 12))
  val fencePtr = DataObjectPointer(fenceOid, poolId, Array[Byte]())

  def lockTxd(txid: TransactionId, reqs: List[TransactionRequirement]): TransactionDescription =
    mktxd().copy(transactionId = txid, requirements = reqs)

  def overwrite(ptr: ObjectPointer): DataUpdate = DataUpdate(ptr, rev0, DataUpdateOperation.Overwrite)

  /** Sends a prepare for the transaction and returns this store's TxPrepareResponse */
  def prepare(frontend: Frontend, net: TestNet, txd: TransactionDescription,
              kvUpdate: DataBuffer = DataBuffer(Array[Byte]())): TxPrepareResponse =
    val updates = txd.requirements.collect {
      case r: DataUpdate => ObjectUpdate(r.objectPointer.id, DataBuffer(Array[Byte](1)))
      case r: KeyValueUpdate => ObjectUpdate(r.objectPointer.id, kvUpdate)
    }
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(objectUpdates = updates))
    net.txMessage() match
      case Some(r: TxPrepareResponse) => r
      case other => throw new AssertionError(s"Expected a TxPrepareResponse, got $other")

  def resolve(frontend: Frontend, net: TestNet, txid: TransactionId, committed: Boolean): Unit =
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid, committed))
    net.txMessage()

  def lockFixture(): (Frontend, MapBackend, TestNet) =
    val (frontend, backend, _, net) = mkFrontendWithNet()
    populate(backend)
    (frontend, backend, net)

  /** op1 and the fence as data objects, kvPtr as a KeyValue object holding keys 1 to 3 */
  def populate(backend: MapBackend): Unit =
    for oid <- List(oid1, fenceOid) do
      val os = mkObjectState(oid)
      backend.bootstrapAllocate(oid, os.objectType, os.metadata, os.data)
    val kv = mkKVObjectState()
    backend.bootstrapAllocate(oid2, kv.objectType, kv.metadata, kv.data)

  /** A Frontend whose backend reports every commit as failed */
  def failingCommitFixture(): (Frontend, MapBackend, TestNet) =
    var handler: Option[CompletionHandler] = None
    val backend = new MapBackend(storeId):
      override def setCompletionHandler(h: CompletionHandler): Unit =
        handler = Some(h)
        super.setCompletionHandler(h)
      override def commit(state: CommitState, transactionId: TransactionId): Unit =
        handler.foreach(_.complete(Commit(storeId, state.objectId, transactionId, Right(CommitError.IOFailure))))
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, new TestCrl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    (frontend, backend, net)

  test("a second transaction requiring the same revision votes abort while the first holds its vote") {
    val (frontend, _, net) = lockFixture()

    val r1 = prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))
    r1.disposition shouldBe TransactionDisposition.VoteCommit

    val r2 = prepare(frontend, net, lockTxd(txid2, List(overwrite(op1))))
    r2.disposition shouldBe TransactionDisposition.VoteAbort
    r2.collisions shouldBe List(txid1)
  }

  /** An overwrite of op1 built on `holder`'s commit: it requires the revision `holder` writes */
  def overwriteAfter(holder: TransactionId): DataUpdate =
    DataUpdate(op1, ObjectRevision(holder), DataUpdateOperation.Overwrite)

  /** Sends a client prepare carrying data for every DataUpdate, without expecting an answer */
  def sendPrepare(frontend: Frontend, txd: TransactionDescription, proposalId: Option[ProposalId] = None): Unit =
    val updates = txd.requirements.collect { case r: DataUpdate => ObjectUpdate(r.objectPointer.id, DataBuffer(Array[Byte](1))) }
    val m = mkPrepare(txd, fromClient = true)
    frontend.receiveTransactionMessage(m.copy(objectUpdates = updates, proposalId = proposalId.getOrElse(m.proposalId)))

  def prepareResponses(net: TestNet, txid: TransactionId): List[TxPrepareResponse] =
    net.sentTx.collect { case r: TxPrepareResponse if r.transactionId == txid => r }

  test("a transaction built on a lock holder's commit waits for it, then votes commit") {
    val (frontend, _, net) = lockFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit

    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    prepareResponses(net, txid2) shouldBe empty

    resolve(frontend, net, txid1, committed = true)
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
    frontend.transactions(txid2).holdsLocks shouldBe true
  }

  test("a waiting transaction votes abort when the lock holder aborts") {
    val (frontend, _, net) = lockFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))

    resolve(frontend, net, txid1, committed = false)
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
  }

  test("a wait for a lock holder ends with an abort vote after CollisionWait") {
    val (frontend, _, net) = lockFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    val tx2 = frontend.transactions(txid2)
    val start = System.nanoTime()

    tx2.checkCollisionWait(start)
    prepareResponses(net, txid2) shouldBe empty

    tx2.checkCollisionWait(start + Tx.CollisionWait.toNanos)
    val r = prepareResponses(net, txid2)
    r.map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
    r.head.collisions shouldBe List(txid1)

    // The holder's release later changes nothing: the vote stands
    resolve(frontend, net, txid1, committed = true)
    prepareResponses(net, txid2).size shouldBe 1
  }

  test("prepares that arrive during a wait are answered when it ends, the latest proposal included") {
    val (frontend, _, net) = lockFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))
    val txd2 = lockTxd(txid2, List(overwriteAfter(txid1)))
    val first = mkPrepare(txd2, fromClient = true).proposalId
    val retry = ProposalId(first.number + 1, first.peer)
    sendPrepare(frontend, txd2)
    sendPrepare(frontend, txd2, Some(retry))
    prepareResponses(net, txid2) shouldBe empty

    resolve(frontend, net, txid1, committed = true)
    val r = prepareResponses(net, txid2)
    r.map(_.proposalId) should contain (retry)
    r.map(_.disposition).toSet shouldBe Set(TransactionDisposition.VoteCommit)
  }

  test("a transaction holding locks is reported as stuck, at intervals, until it is applied") {
    val (frontend, _, net) = lockFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))
    val tx = frontend.transactions(txid1)
    val start = System.nanoTime()

    tx.reportIfStuck(start) shouldBe false

    val late = start + Tx.StuckReportAfter.toNanos + 1000000000L
    tx.reportIfStuck(late) shouldBe true
    tx.reportIfStuck(late + 1) shouldBe false
    tx.reportIfStuck(late + Tx.StuckReportEvery.toNanos) shouldBe true

    resolve(frontend, net, txid1, committed = true)
    tx.reportIfStuck(late + 2 * Tx.StuckReportEvery.toNanos) shouldBe false
  }

  test("a client prepare is reported only when it reaches the store late") {
    val (frontend, _, net) = lockFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))
    val tx = frontend.transactions(txid1)
    val after = Tx.LateClientPrepareAfter.toMillis

    tx.reportLateClientPrepare(after - 1) shouldBe false
    tx.reportLateClientPrepare(after) shouldBe true
  }

  test("locks are released when the first transaction resolves aborted") {
    val (frontend, _, net) = lockFixture()

    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort

    resolve(frontend, net, txid1, committed = false)

    prepare(frontend, net, lockTxd(txid3, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a SharedRevisionLock holder blocks an overwrite until it resolves") {
    val (frontend, backend, net) = lockFixture()

    val a = lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0), overwrite(op1)))
    prepare(frontend, net, a).disposition shouldBe TransactionDisposition.VoteCommit

    val b = prepare(frontend, net, lockTxd(txid2, List(overwrite(fencePtr))))
    b.disposition shouldBe TransactionDisposition.VoteAbort
    b.collisions shouldBe List(txid1)

    resolve(frontend, net, txid1, committed = true)
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid1)
    backend.get(fenceOid).get.metadata.revision shouldBe rev0

    prepare(frontend, net, lockTxd(txid3, List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  test("two SharedRevisionLock holders both vote commit") {
    val (frontend, _, net) = lockFixture()

    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(SharedRevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  // Readers re-read while a store reports a lock held by a transaction they know committed. A shared lock holder never
  // changes the object, so reporting it made reads of a fence spin until the holder resolved
  test("a network read reports exclusive lock holders but not SharedRevisionLock holders") {
    val (frontend, _, net) = lockFixture()

    val a = lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0), overwrite(op1)))
    prepare(frontend, net, a).disposition shouldBe TransactionDisposition.VoteCommit

    def reportedLocks(ptr: ObjectPointer): Set[TransactionId] =
      frontend.readObjectForNetwork(clientId, new UUID(1, 1), ptr, FullObject())
      net.clientMessage() match
        case Some(rr: ReadResponse) => rr.result.toOption.get.lockedWriteTransactions
        case other => throw new AssertionError(s"Expected a ReadResponse, got $other")

    reportedLocks(fencePtr) shouldBe Set()
    reportedLocks(op1) shouldBe Set(txid1)
  }

  test("a transaction that votes abort takes no locks") {
    val (frontend, _, net) = lockFixture()

    // Fails on its revision, so it must not lock op1
    val stale = DataUpdate(op1, new ObjectRevision(new UUID(0, 10)), DataUpdateOperation.Overwrite)
    prepare(frontend, net, lockTxd(txid1, List(stale))).disposition shouldBe TransactionDisposition.VoteAbort

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  /** A Frontend that recovers txid1 (an overwrite of op1 that voted commit) from its CRL with the given status */
  def recoveredFrontend(status: TransactionStatus.Value): (Frontend, TestNet) =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    // Cached so the recovered transaction's object loads during Frontend construction
    cache.insert(os)

    val txd = lockTxd(txid1, List(overwrite(op1)))
    val trs = TransactionRecoveryState(storeId, txd.serialize(), List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))),
      TransactionDisposition.VoteCommit, status, PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, net)

  test("a transaction recovered from the CRL already resolved takes no locks") {
    val (frontend, net) = recoveredFrontend(TransactionStatus.Aborted)
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a transaction recovered from the CRL with a commit vote holds its locks") {
    val (frontend, net) = recoveredFrontend(TransactionStatus.Unresolved)

    val r2 = prepare(frontend, net, lockTxd(txid2, List(overwrite(op1))))
    r2.disposition shouldBe TransactionDisposition.VoteAbort
    r2.collisions shouldBe List(txid1)

    resolve(frontend, net, txid1, committed = false)
    prepare(frontend, net, lockTxd(txid3, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a committed transaction is applied while another transaction holds a lock on its object") {
    val (frontend, backend, net) = lockFixture()

    // txid1 locks op1. txid2 collides here but is committed by the votes of other stores
    prepare(frontend, net, lockTxd(txid1, List(RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 2)))))
      .disposition shouldBe TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort

    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid2, committed = true))
    net.txMessage() match
      case Some(c: TxCommitted) => c.objectCommitErrors shouldBe Nil
      case other => throw new AssertionError(s"Expected TxCommitted, got $other")
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)

    // txid1's lock survives txid2's commit and is released by its own resolution
    prepare(frontend, net, lockTxd(txid3, List(RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 2)))))
      .collisions shouldBe List(txid1)
    resolve(frontend, net, txid1, committed = true)
    backend.get(oid1).get.metadata.refcount shouldBe ObjectRefcount(2, 2)
  }

  test("a prepare arriving after its transaction was finalized starts no transaction and takes no locks") {
    val (frontend, _, net) = lockFixture()
    val txd = lockTxd(txid1, List(overwrite(op1)))

    // Aborted by the other stores and finalized before this store saw its prepare
    prepare(frontend, net, txd).disposition shouldBe TransactionDisposition.VoteCommit
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false))
    frontend.transactions.contains(txid1) shouldBe false

    // A retransmitted prepare
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    frontend.transactions.contains(txid1) shouldBe false

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a prepare arriving after a TxFinalized for a transaction this store never saw starts no transaction") {
    val (frontend, _, net) = lockFixture()
    val txd = lockTxd(txid1, List(overwrite(op1)))

    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false))
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    frontend.transactions.contains(txid1) shouldBe false

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a prepare arriving after its transaction resolved aborted takes no locks") {
    val (frontend, _, net) = lockFixture()

    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false))
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a prepare arriving after its transaction resolved committed takes no locks") {
    val (frontend, _, net) = lockFixture()

    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  // ---- Locks survive the discard of a referenced object state ----

  def keyRevision(n: Int): KeyValueUpdate =
    KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.KeyRevision(k(n), rev0)))

  /** 0x20 is not a KeyValueOperation code, so applying this update throws partway through */
  val corruptKVUpdate: DataBuffer = DataBuffer(Array[Byte](0x20, 0x00))

  test("an update that fails partway through does not drop another transaction's key lock") {
    val (frontend, _, net) = lockFixture()

    prepare(frontend, net, lockTxd(txid1, List(keyRevision(1)))).disposition shouldBe TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(keyRevision(2))), corruptKVUpdate).disposition shouldBe
      TransactionDisposition.VoteCommit
    resolve(frontend, net, txid2, committed = true)

    val r = prepare(frontend, net, lockTxd(txid3, List(keyRevision(1))))
    r.disposition shouldBe TransactionDisposition.VoteAbort
  }

  test("a failed backend commit does not drop another transaction's shared lock") {
    val (frontend, _, net) = failingCommitFixture()

    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    // Collides here, but is committed by the other stores' votes. Its backend commit fails
    prepare(frontend, net, lockTxd(txid2, List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteAbort
    resolve(frontend, net, txid2, committed = true)

    // The backend still holds rev0, but the shared lock must still be honoured
    prepare(frontend, net, lockTxd(txid3, List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteAbort

    // Once the holder resolves, the failed commit's state stays in force: the store may have voted for it
    resolve(frontend, net, txid1, committed = false)
    prepare(frontend, net, lockTxd(TransactionId(new UUID(0, 13)), List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteAbort
    val afterFailed = DataUpdate(fencePtr, ObjectRevision(txid2), DataUpdateOperation.Overwrite)
    prepare(frontend, net, lockTxd(TransactionId(new UUID(0, 14)), List(afterFailed))).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  test("deleting an object does not drop another transaction's shared lock on it") {
    val (frontend, _, net) = lockFixture()

    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    val delete = RefcountUpdate(fencePtr, ObjectRefcount(1, 1), ObjectRefcount(2, 0))
    prepare(frontend, net, lockTxd(txid2, List(delete))).disposition shouldBe TransactionDisposition.VoteAbort
    resolve(frontend, net, txid2, committed = true)

    prepare(frontend, net, lockTxd(txid3, List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteAbort
  }

  test("locks are honoured by a store without an object cache") {
    val backend = new MapBackend(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, ObjectCache.NoCache, net, new TestCrl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)

    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    val r = prepare(frontend, net, lockTxd(txid2, List(overwrite(op1))))
    r.disposition shouldBe TransactionDisposition.VoteAbort
    r.collisions shouldBe List(txid1)

    resolve(frontend, net, txid1, committed = false)
    prepare(frontend, net, lockTxd(txid3, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("transaction messages received while awaiting recovery are handled after the recovered transactions") {
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    populate(backend)
    var recoverable: List[TransactionRecoveryState] = Nil
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = recoverable
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    frontend.awaitRecovery()

    frontend.receiveTransactionMessage(mkPrepare(lockTxd(txid2, List(overwrite(op1))), fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    frontend.transactions shouldBe empty
    net.txMessage() shouldBe None

    // The CRL now holds txid1, a commit vote on op1, as loading a crl_save.log leaves it
    recoverable = List(TransactionRecoveryState(storeId, lockTxd(txid1, List(overwrite(op1))).serialize(),
      List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))), TransactionDisposition.VoteCommit,
      TransactionStatus.Unresolved, PersistentState.initial))
    frontend.recover()

    frontend.transactions.keySet shouldBe Set(txid1, txid2)
    net.txMessage() match
      case Some(r: TxPrepareResponse) =>
        r.transactionId shouldBe txid2
        r.disposition shouldBe TransactionDisposition.VoteAbort
        r.collisions shouldBe List(txid1)
      case other => throw new AssertionError(s"Expected a TxPrepareResponse, got $other")
  }

  // ---- Recovered transactions take their locks before newer transactions are handled ----

  /** Holds backend reads until `deliver`, as a real backend completes them later through the Host's event queue */
  class DeferredReadBackend extends MapBackend(storeId):
    private var deferred: List[ObjectPointer] = Nil
    override def read(pointer: ObjectPointer): Unit = deferred = deferred :+ pointer
    def deliver(): Unit =
      val reads = deferred
      deferred = Nil
      reads.foreach(super.read)

  def commitVote(txid: TransactionId, reqs: List[TransactionRequirement]): TransactionRecoveryState =
    TransactionRecoveryState(storeId, lockTxd(txid, reqs).serialize(),
      List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))), TransactionDisposition.VoteCommit,
      TransactionStatus.Unresolved, PersistentState.initial)

  def sendPrepare(frontend: Frontend, txd: TransactionDescription): Unit =
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))

  /** The prepare response sent last, for `txid` */
  def responseFor(net: TestNet, txid: TransactionId): TxPrepareResponse =
    net.txMessage() match
      case Some(r: TxPrepareResponse) if r.transactionId == txid => r
      case other => throw new AssertionError(s"Expected a TxPrepareResponse for $txid, got $other")

  test("a prepare held during a crl_save.log load is handled after the recovered transactions take their locks") {
    val backend = new DeferredReadBackend
    populate(backend)
    var recoverable: List[TransactionRecoveryState] = Nil
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = recoverable
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    frontend.awaitRecovery()

    sendPrepare(frontend, lockTxd(txid2, List(overwrite(op1))))
    recoverable = List(commitVote(txid1, List(overwrite(op1))))
    frontend.recover()
    backend.deliver() // The shared read of op1 completes after recover returned
    backend.deliver() // and so does any read issued once the held prepare is handled

    val r = responseFor(net, txid2)
    r.disposition shouldBe TransactionDisposition.VoteAbort
    r.collisions shouldBe List(txid1)
  }

  test("a prepare arriving during startup recovery is handled after the recovered transactions take their locks") {
    val backend = new DeferredReadBackend
    populate(backend)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
        List(commitVote(txid1, List(overwrite(op1))))
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    sendPrepare(frontend, lockTxd(txid2, List(overwrite(op1))))
    backend.deliver()
    backend.deliver()

    val r = responseFor(net, txid2)
    r.disposition shouldBe TransactionDisposition.VoteAbort
    r.collisions shouldBe List(txid1)
  }

  test("a recovered commit vote is kept although its requirements no longer hold") {
    // It required a revision the object no longer has, as when its vote was persisted before another commit
    val stale = DataUpdate(op1, new ObjectRevision(new UUID(0, 10)), DataUpdateOperation.Overwrite)
    val backend = new MapBackend(storeId)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
        List(commitVote(txid1, List(stale)))
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    cache.insert(os)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    sendPrepare(frontend, lockTxd(txid1, List(stale)))
    responseFor(net, txid1).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("held messages are released when a recovered transaction's object fails to load") {
    val backend = new DeferredReadBackend // op1 is never allocated, so its read fails
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
        List(commitVote(txid1, List(overwrite(op1))))
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    sendPrepare(frontend, lockTxd(txid2, List(overwrite(op1))))
    frontend.transactions.contains(txid2) shouldBe false
    backend.deliver()
    backend.deliver()

    frontend.transactions.contains(txid2) shouldBe true
    responseFor(net, txid2).disposition shouldBe TransactionDisposition.VoteAbort
  }

  def recoveryState(txid: TransactionId, reqs: List[TransactionRequirement],
                    disposition: TransactionDisposition.Value): TransactionRecoveryState =
    TransactionRecoveryState(storeId, lockTxd(txid, reqs).serialize(),
      List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))), disposition, TransactionStatus.Unresolved,
      PersistentState.initial)

  test("held messages wait for every recovered transaction, not just the first to load") {
    val backend = new DeferredReadBackend
    populate(backend)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    // The fence is cached, so the first recovered transaction loads synchronously, inside recover
    val fence = mkObjectState(fenceOid)
    cache.insert(fence)
    var recoverable: List[TransactionRecoveryState] = Nil
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = recoverable
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    frontend.awaitRecovery()

    // A retransmitted prepare of the second recovered transaction, and one that conflicts with it
    sendPrepare(frontend, lockTxd(txid2, List(overwrite(op1))))
    sendPrepare(frontend, lockTxd(txid3, List(overwrite(op1))))
    recoverable = List(
      recoveryState(txid1, List(SharedRevisionLock(fencePtr, rev0)), TransactionDisposition.VoteCommit),
      recoveryState(txid2, List(overwrite(op1)), TransactionDisposition.VoteCommit))
    frontend.recover()
    backend.deliver()
    backend.deliver()

    frontend.transactions(txid2).recovered shouldBe true
    val r = responseFor(net, txid3)
    r.disposition shouldBe TransactionDisposition.VoteAbort
    r.collisions shouldBe List(txid2)
  }

  test("a recovered transaction with no persisted vote decides only after recovered votes have locked") {
    val backend = new DeferredReadBackend
    populate(backend)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(
        recoveryState(txid1, List(overwrite(op1)), TransactionDisposition.VoteCommit),
        recoveryState(txid2, List(overwrite(op1)), TransactionDisposition.Undetermined))
    val net = new TestNet
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    // The shared read of op1 is dispatched to the newest waiter, the undetermined transaction, first
    backend.deliver()

    sendPrepare(frontend, lockTxd(txid2, List(overwrite(op1))))
    val r = responseFor(net, txid2)
    r.disposition shouldBe TransactionDisposition.VoteAbort
    r.collisions shouldBe List(txid1)
    cache.get(oid1).flatMap(_.lockedToTransaction) shouldBe Some(txid1)
  }

  test("a recovered commit vote whose object failed to load reports no locking error") {
    val out = new java.io.ByteArrayOutputStream
    Console.withOut(out) {
      val backend = new DeferredReadBackend // op1 is never allocated, so its read fails
      val crl = new TestCrl:
        override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
          List(recoveryState(txid1, List(overwrite(op1)), TransactionDisposition.VoteCommit))
      val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
        crl, new TransactionStatusCache())
      backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
      backend.deliver()
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false))
    }
    out.toString should not include "UNEXPECTED"
  }

  // ---- States stay in memory from a commit until the backend reports it complete ----

  val others: List[DataObjectPointer] =
    (20 to 22).toList.map(n => DataObjectPointer(ObjectId(new UUID(0, n)), poolId, Array[Byte]()))

  /** A Frontend over a DeferredCommitBackend, with a cache that holds two empty data objects */
  def commitWindowFixture(cacheBytes: Long = 2 * HostObjectCache.EntryOverhead, crl: CrashRecoveryLog = new TestCrl)
      : (Frontend, DeferredCommitBackend, StoreObjectCache, TestNet) =
    val backend = new DeferredCommitBackend
    populate(backend)
    for p <- others do
      val os = mkObjectState(p.id)
      backend.bootstrapAllocate(p.id, os.objectType, os.metadata, os.data)
    val cache = new HostObjectCache(cacheBytes).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, backend, cache, net)

  /** Network-reads `ptr`, returning the object's data or the read error */
  def readData(frontend: Frontend, net: TestNet, ptr: ObjectPointer): Either[ReadError.Value, List[Byte]] =
    frontend.readObjectForNetwork(clientId, new UUID(1, 1), ptr, FullObject())
    net.clientMessage() match
      case Some(rr: ReadResponse) => rr.result.map(_.objectData.get.getByteArray.toList)
      case other => throw new AssertionError(s"Expected a ReadResponse, got $other")

  /** Reads the other objects, so the cache holds only them and pinned states */
  def evictionPressure(frontend: Frontend, net: TestNet): Unit =
    others.foreach(p => readData(frontend, net, p) shouldBe Right(Nil))

  test("a committed state is served from memory until its backend commit completes") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    // The retention does not depend on the transaction
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true))
    frontend.transactions.contains(txid1) shouldBe false

    evictionPressure(frontend, net)
    val reads = backend.reads
    readData(frontend, net, op1) shouldBe Right(List[Byte](1))
    backend.reads shouldBe reads

    backend.deliverCommits()
    evictionPressure(frontend, net)
    val after = backend.reads
    readData(frontend, net, op1) shouldBe Right(List[Byte](1))
    backend.reads shouldBe after + 1
  }

  test("a lock-only object gets no backend commit") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0), overwrite(op1)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    committedMessages(net) shouldBe Nil
    backend.deliverOne()
    committedMessages(net).size shouldBe 1
  }

  test("a transaction that changes nothing here still reports committed") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(RevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 0
    committedMessages(net).size shouldBe 1
  }

  test("a read during a deletion's commit gets ObjectNotFound without reaching the backend") {
    val (frontend, backend, _, net) = commitWindowFixture()
    val delete = RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 0))
    prepare(frontend, net, lockTxd(txid1, List(delete))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1

    val reads = backend.reads
    readData(frontend, net, op1) shouldBe Left(ReadError.ObjectNotFound)
    // A transaction finds the object missing too
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort
    backend.reads shouldBe reads

    backend.deliverCommits()
    readData(frontend, net, op1) shouldBe Left(ReadError.ObjectNotFound)
    backend.reads shouldBe reads + 1
  }

  test("a failed backend commit keeps serving the committed state") {
    for err <- List(CommitError.InvalidPointer, CommitError.ObjectMismatch, CommitError.SliceTooLarge,
                    CommitError.IOFailure) do
      withClue(s"$err: "):
        val (frontend, backend, _, net) = commitWindowFixture()
        prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe
          TransactionDisposition.VoteCommit
        resolve(frontend, net, txid1, committed = true)
        backend.deliverCommits(Right(err))

        val reads = backend.reads
        readData(frontend, net, op1) shouldBe Right(List[Byte](1))
        evictionPressure(frontend, net)
        val afterPressure = backend.reads
        readData(frontend, net, op1) shouldBe Right(List[Byte](1))
        backend.reads shouldBe afterPressure
        afterPressure shouldBe reads + others.size
  }

  test("two in-flight commits of one object serve the newest state") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)

    val txd2 = lockTxd(txid2, List(overwriteAfter(txid1)))
    frontend.receiveTransactionMessage(mkPrepare(txd2, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](2))))))
    responseFor(net, txid2).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid2, committed = true)
    backend.heldCommits shouldBe 2

    val reads = backend.reads
    backend.deliverOne()
    evictionPressure(frontend, net)
    val afterPressure = backend.reads
    readData(frontend, net, op1) shouldBe Right(List[Byte](2))
    backend.reads shouldBe afterPressure

    backend.deliverOne()
    readData(frontend, net, op1) shouldBe Right(List[Byte](2))
    backend.get(oid1).get.data.getByteArray.toList shouldBe List[Byte](2)
    afterPressure shouldBe reads + others.size
  }

  test("an aborted transaction leaves its objects cached") {
    val (frontend, _, cache, net) = commitWindowFixture()
    readData(frontend, net, others(0)) shouldBe Right(Nil)
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    // Read while the transaction holds op1: evicts others(0), the oldest
    readData(frontend, net, others(1)) shouldBe Right(Nil)
    resolve(frontend, net, txid1, committed = false)

    // op1 returned most recently used, so this evicts others(1) instead
    readData(frontend, net, others(2)) shouldBe Right(Nil)
    cache.get(oid1) should not be empty
    cache.get(others(1).id) shouldBe None
  }

  test("an aborted allocation is not cached") {
    val (frontend, _, cache, net) = commitWindowFixture()
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 30)), poolId, Array[Byte]())
    val txd = lockTxd(txid1, List(
      RefcountUpdate(newPtr, ObjectRefcount.Allocating, ObjectRefcount.Default),
      DataUpdate(newPtr, ObjectRevision.Allocating, DataUpdateOperation.Overwrite))).copy(
      allocatingObjects = Set(newPtr.id))
    prepare(frontend, net, txd).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = false)

    cache.get(newPtr.id) shouldBe None
    cache.sizeInBytes shouldBe 0
  }

  test("a pinned state counts toward the store's bytes") {
    val (frontend, backend, cache, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)

    evictionPressure(frontend, net)
    // op1's in-flight state (one data byte) and the last object read, which the cache keeps although over budget
    cache.sizeInBytes shouldBe 2 * HostObjectCache.EntryOverhead + 1
    cache.pinnedCount shouldBe 1

    backend.deliverCommits()
    cache.pinnedCount shouldBe 0
    cache.sizeInBytes shouldBe HostObjectCache.EntryOverhead + 1
  }

  test("a commit of a fresh state does not replace a discarded state whose locks are held") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(kvPtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    // Committed by the other stores' votes; its update fails partway through, discarding the locked state
    prepare(frontend, net, lockTxd(txid2, List(keyRevision(1))), corruptKVUpdate).disposition shouldBe
      TransactionDisposition.VoteAbort
    resolve(frontend, net, txid2, committed = true)

    // A network read loads a fresh state, which an opportunistic rebuild then commits
    frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, FullObject())
    net.clientMessage() should not be empty
    val kv = mkKVObjectState()
    val rebuilt = KVObjectState.encode(Some(k(0)), None, None, None, Map(k(1) ->
      new ValueState(Value(Array[Byte](9)), rev0, HLCTimestamp(1), None)))
    frontend.readObjectForOpportunisticRebuild(OpportunisticRebuild(storeId, clientId, kvPtr, rev0,
      kv.metadata.refcount, HLCTimestamp(50), rebuilt))
    backend.heldCommits shouldBe 1
    backend.deliverCommits()

    // The shared lock is still honoured
    prepare(frontend, net, lockTxd(txid3, List(keyRevision(2)))).disposition shouldBe TransactionDisposition.VoteAbort

    // Once its holder resolves, the rebuilt state is served from memory
    resolve(frontend, net, txid1, committed = false)
    val reads = backend.reads
    frontend.readObjectForNetwork(clientId, new UUID(1, 2), kvPtr, FullObject())
    net.clientMessage() match
      case Some(rr: ReadResponse) => rr.result.toOption.get.objectData.get.getByteArray.toList shouldBe rebuilt.getByteArray.toList
      case other => throw new AssertionError(s"Expected a ReadResponse, got $other")
    backend.reads shouldBe reads
  }

  test("a backend read completing during a commit does not cache the state the commit replaces") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(kvPtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(keyRevision(1))), corruptKVUpdate)
    resolve(frontend, net, txid2, committed = true)
    frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, FullObject())
    net.clientMessage() should not be empty
    val kv = mkKVObjectState()
    val rebuilt = KVObjectState.encode(Some(k(0)), None, None, None, Map(k(1) ->
      new ValueState(Value(Array[Byte](9)), rev0, HLCTimestamp(1), None)))
    frontend.readObjectForOpportunisticRebuild(OpportunisticRebuild(storeId, clientId, kvPtr, rev0,
      kv.metadata.refcount, HLCTimestamp(50), rebuilt))
    backend.heldCommits shouldBe 1

    // A read issued before the rebuild's commit returns the state that commit replaces
    frontend.backendReadComplete(oid2, Left(ReadState(oid2, kv.metadata, kv.objectType, kv.data, Set())))

    def served(): List[Byte] =
      frontend.readObjectForNetwork(clientId, new UUID(1, 2), kvPtr, FullObject())
      net.clientMessage() match
        case Some(rr: ReadResponse) => rr.result.toOption.get.objectData.get.getByteArray.toList
        case other => throw new AssertionError(s"Expected a ReadResponse, got $other")

    served() shouldBe rebuilt.getByteArray.toList
    backend.deliverCommits()
    resolve(frontend, net, txid1, committed = false)
    val reads = backend.reads
    served() shouldBe rebuilt.getByteArray.toList
    backend.reads shouldBe reads
  }

  test("a failed in-place update during an in-flight commit does not cache the pre-commit backend state") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    val insertKey4 = KeyValueOperation.encode(List(new Insert(k(4), Array[Byte](4))), ida)(0)
    prepare(frontend, net, lockTxd(txid1, List(keyRevision(1))), insertKey4).disposition shouldBe
      TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    // Its update fails partway through, discarding the state txid1's commit holds
    prepare(frontend, net, lockTxd(txid2, List(keyRevision(2))), corruptKVUpdate)
    resolve(frontend, net, txid2, committed = true)
    backend.heldCommits shouldBe 1

    def keys(): Set[Int] =
      frontend.readObjectForNetwork(clientId, new UUID(1, 3), kvPtr, FullObject())
      readResponseKeys(net)._1

    // Answered from the backend, which does not have txid1's commit yet
    keys() shouldBe Set(1, 2, 3)
    backend.deliverCommits()
    keys() shouldBe Set(1, 2, 3, 4)
  }

  def repairState(): DataObjectState =
    new DataObjectState(op1, ObjectRevision(txid2), ObjectRefcount(1, 1), HLCTimestamp(1L << 20), HLCTimestamp(1L << 20), ida, 1,
      DataBuffer(Array[Byte](5)))

  test("a repair during a deletion's commit is not applied") {
    val (frontend, backend, _, net) = commitWindowFixture()
    val delete = RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 0))
    prepare(frontend, net, lockTxd(txid1, List(delete))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1

    val done = Promise[Unit]()
    frontend.readObjectForRepair(repairState(), done)
    done.isCompleted shouldBe true
    backend.repairs shouldBe 0
  }

  test("a repair whose read completes during a deletion's commit is not applied") {
    val (frontend, backend, _, _) = commitWindowFixture()
    backend.deferReads = true
    val done = Promise[Unit]()
    frontend.readObjectForRepair(repairState(), done)
    backend.reads shouldBe 1

    val metadata = Metadata(ObjectRevision(txid1), ObjectRefcount(2, 0), HLCTimestamp(200))
    frontend.commit(new ObjectState(oid1, metadata, ObjectType.Data, DataBuffer.Empty),
      CommitState(op1, metadata, ObjectType.Data, DataBuffer.Empty), txid1)
    backend.deliverReads()
    done.isCompleted shouldBe true
    backend.repairs shouldBe 0
  }

  // ---- A store keeps a committed transaction's CRL state until its backend commits succeed ----

  /** Prepares txid1 overwriting op1 and resolves it committed, its backend commit held */
  def heldCommit(crl: CrashRecoveryLog): (Frontend, DeferredCommitBackend, TestNet) =
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024, crl = crl)
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    (frontend, backend, net)

  def finalizeTx1(frontend: Frontend): Unit =
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true))

  test("finalization before the backend commit completes defers the CRL deletion") {
    val crl = new TestCrl
    val (frontend, backend, _) = heldCommit(crl)
    finalizeTx1(frontend)
    crl.deleted shouldBe Nil
    frontend.transactions.contains(txid1) shouldBe false

    backend.deliverCommits()
    crl.deleted shouldBe List(txid1)
  }

  test("a backend commit completing before finalization leaves the deletion to it") {
    val crl = new TestCrl
    val (frontend, backend, _) = heldCommit(crl)
    backend.deliverCommits()
    crl.dropped shouldBe List(txid1)
    crl.deleted shouldBe Nil

    finalizeTx1(frontend)
    crl.deleted shouldBe List(txid1)
  }

  test("a failed backend commit keeps the transaction's CRL entry and its object data") {
    val crl = new TestCrl
    val (frontend, backend, net) = heldCommit(crl)
    backend.deliverCommits(Right(CommitError.IOFailure))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
    finalizeTx1(frontend)
    crl.deleted shouldBe Nil
    crl.dropped shouldBe Nil
    frontend.transactions.contains(txid1) shouldBe false
  }

  test("a failed backend commit after finalization keeps the transaction's CRL entry") {
    val crl = new TestCrl
    val (frontend, backend, _) = heldCommit(crl)
    finalizeTx1(frontend)
    backend.deliverCommits(Right(CommitError.IOFailure))
    crl.deleted shouldBe Nil
    crl.dropped shouldBe Nil
  }

  /** A SimpleCRL whose saves complete before save returns, so the store answers on the test's thread */
  class SyncCrl(val crl: SimpleCRL) extends CrashRecoveryLog:
    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
      crl.getFullRecoveryState(storeId)
    override def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]] = crl.closeStore(storeId)
    override def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
      val done = new LinkedBlockingQueue[java.lang.Boolean]()
      crl.save(txid, state, () => done.put(true))
      if done.poll(10, java.util.concurrent.TimeUnit.SECONDS) eq null then
        fail(s"SimpleCRL did not complete the save of transaction $txid within 10 seconds")
      completionHandler()
    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit =
      crl.dropTransactionObjectData(storeId, txid)
    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = crl.deleteTransaction(storeId, txid)

  /** The CRL state a store closed with after txid1's backend commit failed and it was finalized */
  def savedAfterFailedCommit(): List[TransactionRecoveryState] =
    val tdir = new TempDirManager
    val crl = new SyncCrl(SimpleCRL(tdir.tdir, 3, 1024 * 1024).crl)
    try
      val (frontend, backend, _) = heldCommit(crl)
      backend.deliverCommits(Right(CommitError.IOFailure))
      finalizeTx1(frontend)
      Await.result(crl.closeStore(storeId), Duration(10, SECONDS))
    finally
      crl.crl.shutdown()
      tdir.delete()

  test("a failed commit's CRL entry is in the store's saved state") {
    savedAfterFailedCommit() match
      case trs :: Nil =>
        TransactionDescription.deserialize(trs.serializedTxd).transactionId shouldBe txid1
        trs.objectUpdates.map(ou => (ou.objectId, ou.data.getByteArray.toList)) shouldBe List((oid1, List[Byte](1)))
      case other => throw new AssertionError(s"Expected txid1's entry, got $other")
  }

  test("a store loaded with such an entry recreates the transaction") {
    val saved = savedAfterFailedCommit()
    val backend = new MapBackend(storeId)
    populate(backend)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = saved
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet, crl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    frontend.transactions.contains(txid1) shouldBe true
  }

  test("locks are released at finalization while a commit is outstanding") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    finalizeTx1(frontend)
    prepare(frontend, net, lockTxd(txid2, List(overwriteAfter(txid1)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    backend.heldCommits shouldBe 1
  }
}
