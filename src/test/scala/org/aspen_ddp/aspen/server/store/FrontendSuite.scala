package org.aspen_ddp.aspen.server.store

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import org.aspen_ddp.aspen.common.ida.{AppendDelta, Replication, XorRegion}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, OpportunisticRebuild, ReadResponse, TxAccept, TxAcceptResponse, TxCommitted, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, FullObject, Insert, Key, KeySet, KeyValueOperation, KeyValueObjectPointer, Metadata, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, Value}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.paxos.PersistentState
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, ObjectUpdate, RefcountUpdate, RevisionLock, SharedRevisionLock, VersionBump, TransactionCollision, TransactionDescription, TransactionDisposition, TransactionId, TransactionRequirement, TransactionStatus}
import org.aspen_ddp.aspen.TempDirManager
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, StoreRecoveryState, TransactionRecoveryState}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, CompletionHandler, MapBackend, UpdateForm}
import org.aspen_ddp.aspen.server.store.cache.{HostObjectCache, ObjectCache, StoreObjectCache}
import org.aspen_ddp.aspen.server.transaction.{TransactionStatusCache, Tx}
import org.aspen_ddp.aspen.client.DataObjectState
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.aspen_ddp.aspen.server.Host

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

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

  /** Records the UnackedCommits lookups a Frontend makes, and answers them when the test says */
  class ManualLookup extends UnackedCommitLookup:
    var frontend: Option[Frontend] = None
    /** The lookups made, oldest first, with their request ids */
    var made: List[(TransactionId, Long)] = Nil
    private var answers: Map[(TransactionId, Long), Try[Boolean] => Unit] = Map()
    def requests: List[TransactionId] = made.map(_._1)
    def lookup(storeId: StoreId, transactionId: TransactionId, requestId: Long, answer: Try[Boolean] => Unit): Unit =
      made = made :+ (transactionId -> requestId)
      answers += (transactionId, requestId) -> answer
    /** Answers the lookup `requestId` of `txid`, by default the latest made */
    def answer(txid: TransactionId, result: Try[Boolean], requestId: Option[Long] = None): Unit =
      val id = requestId.getOrElse(made.filter(_._1 == txid).last._2)
      answers((txid, id))(result)

  class TestCrl extends CrashRecoveryLog {

    var txSaved = false
    var txDel = false
    var aDrop = false
    var deleted: List[TransactionId] = Nil
    var dropped: List[TransactionId] = Nil

    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = Nil

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

    var stubsSaved: List[TransactionId] = Nil
    var stubsDeleted: List[TransactionId] = Nil
    /** While set, stub saves complete only when the test runs the handlers held here */
    var holdStubSaves = false
    var heldStubSaves: List[() => Unit] = Nil

    override def getAppliedStubs(storeId: StoreId): Set[TransactionId] = Set()

    override def saveAppliedStub(storeId: StoreId, txid: TransactionId, completionHandler: () => Unit): Unit =
      stubsSaved = stubsSaved :+ txid
      if holdStubSaves then heldStubSaves = heldStubSaves :+ completionHandler else completionHandler()

    override def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit =
      stubsDeleted = stubsDeleted :+ txid
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

    /** With `deferRepairs`, repair writes are held until deliverRepairs */
    var deferRepairs: Boolean = false
    private var heldRepairs: List[(CommitState, Promise[Unit])] = Nil

    override def repair(state: CommitState, complete: Promise[Unit]): Unit =
      repairs += 1
      if deferRepairs then heldRepairs = heldRepairs :+ (state, complete) else super.repair(state, complete)

    def deliverRepairs(): Unit =
      val held = heldRepairs
      heldRepairs = Nil
      held.foreach((state, complete) => super.repair(state, complete))

    /** The transactions of the commits received, in the order received */
    var issued: List[TransactionId] = Nil
    /** The most commits held at once */
    var maxHeld: Int = 0

    override def commit(state: CommitState, transactionId: TransactionId): Unit =
      held = held :+ (state, transactionId)
      issued = issued :+ transactionId
      maxHeld = math.max(maxHeld, held.size)

    def heldCommits: Int = held.size

    /** Completes the oldest held commit. A ranged commit writes only its spans over the stored slice, as a backend that
      * avoids rewriting unchanged bytes does
      */
    def deliverOne(result: Either[Unit, CommitError.Value] = Left(())): Unit =
      val (state, txid) = held.head
      held = held.tail
      if result.isLeft then
        if state.metadata.refcount.count == 0 then
          m -= state.objectId
        else
          val data = (state.form, m.get(state.objectId)) match
            case (UpdateForm.Ranges(_, spans), Some(stored)) =>
              val bytes = new Array[Byte](state.data.size)
              val base = stored.data.getByteArray
              System.arraycopy(base, 0, bytes, 0, math.min(base.length, bytes.length))
              val src = state.data.getByteArray
              spans.foreach((off, len) => System.arraycopy(src, off, bytes, off, len))
              DataBuffer(bytes)
            case _ => state.data
          m += (state.objectId -> new ObjectState(state.objectId, state.metadata, state.objectType, data))
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
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))

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
        frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
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

      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
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

      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
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

  // ---- Prefer-abort: a driver's prepare never gathers a fresh commit vote ----
  //
  // A store-side driver's prepare (fromClient = false) may re-drive a transaction that aborted and was forgotten: the
  // store then has no vote, and its requirements may hold again. It waits for the client's prepare, and votes abort
  // without checking them if none comes.

  /** A data-less refcount change of op1 whose requirement holds on this store */
  private def refcountTxd = mktxd().copy(requirements =
    List(RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 1))))

  test("a data-less transaction prepared only by a driver gets an abort vote after the wait, requirements unchecked") {
    withDataless(missingDataWait = Duration.Zero): (frontend, backend, net, _) =>
      frontend.receiveTransactionMessage(mkPrepare(refcountTxd, fromClient = false))
      // The requirement holds, so a check would have voted commit
      expectVote(net, TransactionDisposition.VoteAbort)
      frontend.transactions(txid1).holdsLocks shouldBe false
      // A later client prepare does not change the vote given
      frontend.receiveTransactionMessage(mkPrepare(refcountTxd, fromClient = true))
      expectVote(net, TransactionDisposition.VoteAbort)
  }

  test("a client prepare arriving within the wait is checked normally and can vote commit") {
    withDataless(): (frontend, _, net, crl) =>
      frontend.receiveTransactionMessage(mkPrepare(refcountTxd, fromClient = false))
      frontend.receiveTransactionMessage(mkPrepare(refcountTxd, fromClient = false)) // A retransmission
      net.txMessage() shouldBe None
      crl.txSaved shouldBe false

      frontend.receiveTransactionMessage(mkPrepare(refcountTxd, fromClient = true))
      expectVote(net, TransactionDisposition.VoteCommit)
      frontend.transactions(txid1).holdsLocks shouldBe true
  }

  test("a lock-only transaction prepared only by a driver waits for the client's prepare") {
    withDataless(): (frontend, _, net, _) =>
      val txd = mktxd().copy(requirements = List(SharedRevisionLock(op1, rev0)))
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      net.txMessage() shouldBe None
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true))
      expectVote(net, TransactionDisposition.VoteCommit)
  }

  private def committedMessages(net: TestNet): List[TxCommitted] = net.sentTx.collect { case c: TxCommitted => c }

  test("a commit that overtakes the client's data is applied when the data arrives and reported to the designated leader") {
    withDataless(): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      val leader = StoreId(poolId, txd.designatedLeaderUID)
      val recoveryDriver = StoreId(poolId, 2)
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false).copy(from = recoveryDriver))
      frontend.receiveTransactionMessage(TxResolved(storeId, recoveryDriver, txid1, committed = true, late = false, proof = false))
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
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      net.txMessage() shouldBe None

      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      net.txMessage() should not be None
  }

  test("a client's prepare without the data ends a commit's wait: no more data is coming") {
    withDataless(): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true))
      committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
      backend.get(oid1).get.metadata.revision shouldBe rev0
  }

  test("finalization ends a commit's wait for data") {
    withDataless(): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      committedMessages(net) shouldBe Nil

      frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set()))
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
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      val p = mkPrepare(txd, fromClient = false)
      frontend.receiveTransactionMessage(p.copy(from = recoveryDriver, proposalId = ProposalId(p.proposalId.number + 1, 2.toByte)))
      crl.txSaved shouldBe true
      crl.txDel shouldBe false

      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = false, late = false, owed = Set()))
      crl.txDel shouldBe true
      frontend.transactions.contains(txid1) shouldBe false
  }

  test("finalization deletes the CRL entry a prepare saved after an abort") {
    withDataless(): (frontend, _, _, crl) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false, late = false, proof = false))
      crl.txDel = false
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))

      frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set()))
      crl.txDel shouldBe true
  }

  test("an abort does not wait for data") {
    withDataless(): (frontend, _, net, crl) =>
      frontend.receiveTransactionMessage(mkPrepare(dataUpdateTxd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false, late = false, proof = false))
      crl.txDel shouldBe true
      committedMessages(net) shouldBe Nil
  }

  // ---- A prepare for a forgotten transaction this store has already applied ----
  //
  // A recovery driver may prepare a transaction the other stores finalized and forgot: a store that voted commit
  // crashed before applying it, and recovered after. Its object at the transaction's revision proves the commit.
  // Voting on requirements that the commit itself made fail would let the driver decide abort, and the recovering
  // store would drop an update that counted towards the write threshold.

  private val provenRev = ObjectRevision(txid1)
  private val recoveryDriver = StoreId(poolId, 2)
  private val appliedData = Array[Byte](3, 3)

  /** A store holding op1 already at txid1's revision, with the fence object at `fenceRevision` */
  private def withApplied(fenceRevision: ObjectRevision = rev0)
                         (f: (Frontend, MapBackend, TestNet, TestCrl) => Unit): Unit =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val crl = new TestCrl
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(appliedData))
    backend.bootstrapAllocate(fenceOid, ObjectType.Data, Metadata(fenceRevision, ObjectRefcount(1, 1),
      HLCTimestamp(1)), DataBuffer(new Array[Byte](0)))
    f(frontend, backend, net, crl)

  private def driverPrepare(txd: TransactionDescription): TxPrepare =
    val p = mkPrepare(txd, fromClient = false)
    p.copy(from = recoveryDriver, proposalId = ProposalId(p.proposalId.number + 1, 2.toByte))

  private def resolvedMessages(net: TestNet): List[TxResolved] = net.sentTx.collect { case r: TxResolved => r }
  private def votes(net: TestNet): List[TxPrepareResponse] = net.sentTx.collect { case r: TxPrepareResponse => r }

  test("a driver's prepare for a forgotten transaction this store has applied is answered with a proof") {
    withApplied(): (frontend, _, net, crl) =>
      frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd))

      resolvedMessages(net) shouldBe List(TxResolved(recoveryDriver, storeId, txid1, committed = true, late = false, proof = true))
      votes(net) shouldBe Nil
      crl.txSaved shouldBe false
      // No locks: another transaction on the object is unaffected
      frontend.transactions(txid1).holdsLocks shouldBe false
  }

  test("a driver prepare reaching a store whose object shows the update proves at once") {
    withApplied(): (frontend, _, net, _) =>
      // Data-less, so no data wait applies either: only the wait for the client's prepare, which a proof skips
      val txd = mktxd().copy(requirements = List(VersionBump(op1, rev0)))
      frontend.receiveTransactionMessage(driverPrepare(txd))

      resolvedMessages(net) shouldBe List(TxResolved(recoveryDriver, storeId, txid1, committed = true, late = false, proof = true))
      votes(net) shouldBe Nil
  }

  test("a prover answers a later prepare as a memoryless acceptor with an abort vote, saved to the CRL") {
    withApplied(): (frontend, _, net, crl) =>
      frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd))
      val later = driverPrepare(dataUpdateTxd)
      frontend.receiveTransactionMessage(later.copy(proposalId = ProposalId(later.proposalId.number + 1, 2.toByte)))

      resolvedMessages(net).size shouldBe 1
      votes(net).map(v => (v.disposition, v.response)) shouldBe
        List((TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(None))))
      crl.txSaved shouldBe true
      frontend.transactions(txid1).holdsLocks shouldBe false
  }

  test("a prover whose prepare arrived before its objects loaded still sends the promise it gave") {
    // The driver's post-proof round may reach a store whose objects are still loading: the acceptor promises, and the
    // response waits for the load. Proving on load must not drop it, or a driver that never retransmits waits forever
    val backend = new DeferredCommitBackend
    backend.deferReads = true
    val net = new TestNet
    val crl = new TestCrl
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(appliedData))
    // The client's prepare starts the Tx and its loads; a driver's post-proof round prepare arrives meanwhile
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    val p = driverPrepare(dataUpdateTxd)
    frontend.receiveTransactionMessage(p)
    net.sentTx shouldBe Nil

    backend.deliverReads()

    resolvedMessages(net).map(_.proof) shouldBe List(true)
    votes(net).map(v => (v.proposalId, v.disposition, v.response)) shouldBe
      List((p.proposalId, TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(None))))
    crl.txSaved shouldBe true
  }

  test("a prover answering its first prepare only once its objects load sends the proof and a memoryless abort vote") {
    val backend = new DeferredCommitBackend
    backend.deferReads = true
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, new TestCrl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(appliedData))
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    backend.deliverReads()

    // The proof goes first; the driver counts no promise to the round it answers (TransactionDriver.resolveLate)
    net.sentTx.head match
      case r: TxResolved => r.proof shouldBe true
      case other => throw new AssertionError(s"Expected the proof first, got $other")
    votes(net).map(v => (v.disposition, v.response)) shouldBe
      List((TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(None))))
  }

  test("a prover is kept, recording nothing, on a non-late resolution, and forgotten on TxFinalized") {
    val statusCache = new TransactionStatusCache()
    val backend = new MapBackend(storeId)
    val net = new TestNet
    val crl = new TestCrl
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      statusCache)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(appliedData))
    frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd))
    frontend.receiveTransactionMessage(TxResolved(storeId, recoveryDriver, txid1, committed = true, late = false,
      proof = false))

    // An acceptor forgets only on TxFinalized or a late outcome (Ruling 16)
    frontend.transactions.contains(txid1) shouldBe true
    crl.txDel shouldBe false
    statusCache.getStatus(txid1).map(_.status).forall(_ == TransactionStatus.Unresolved) shouldBe true

    frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = false, owed = Set()))
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net) shouldBe Nil
    crl.txDel shouldBe true
  }

  test("a recovered Tx whose acceptor accepted a value does not prove, and promises what it accepted") {
    // Ruling 16: only a memoryless acceptor proves. An accept has no disposition guard, so an undecided Tx may hold one
    val accepted = (ProposalId(2, 2), true)
    val backend = new MapBackend(storeId)
    val os = new ObjectState(oid1, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)), ObjectType.Data,
      DataBuffer(appliedData))
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(os) // So the recovered transaction loads during construction
    val trs = TransactionRecoveryState(storeId, dataUpdateTxd.serialize(), Nil, TransactionDisposition.Undetermined,
      TransactionStatus.Unresolved, PersistentState(Some(accepted._1), Some(accepted)))
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val net = new TestNet
    // No vote and no client prepare: it answers a driver's prepare only once the wait for the client's is over
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), None, Duration.Zero)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    val p = driverPrepare(dataUpdateTxd)
    frontend.receiveTransactionMessage(p.copy(proposalId = ProposalId(3, 2)))

    resolvedMessages(net) shouldBe Nil
    votes(net).map(v => (v.disposition, v.response)) shouldBe
      List((TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(Some(accepted)))))

    // A non-late resolution is an ordinary one for it, not a prover's: it is kept until TxFinalized
    frontend.receiveTransactionMessage(TxResolved(storeId, recoveryDriver, txid1, committed = true, late = false,
      proof = false))
    frontend.transactions.contains(txid1) shouldBe true
  }

  test("a live Tx that accepted a value before its objects loaded does not prove") {
    val backend = new DeferredCommitBackend
    backend.deferReads = true
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, new TestCrl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(appliedData))
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    frontend.receiveTransactionMessage(TxAccept(storeId, StoreId(poolId, dataUpdateTxd.designatedLeaderUID), txid1,
      ProposalId.initialProposal(dataUpdateTxd.designatedLeaderUID), true))
    backend.deliverReads()

    resolvedMessages(net) shouldBe Nil
    votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
  }

  test("a prover records nothing in the status cache") {
    val statusCache = new TransactionStatusCache()
    val backend = new MapBackend(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, new TestCrl,
      statusCache)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(appliedData))
    frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd))

    resolvedMessages(net).map(_.proof) shouldBe List(true)
    statusCache.getStatus(txid1).map(_.status).forall(_ == TransactionStatus.Unresolved) shouldBe true
  }

  test("a proven commit is not applied again and sends no TxCommitted when finalized") {
    withApplied(): (frontend, backend, net, crl) =>
      frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd))
      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = false, owed = Set()))

      committedMessages(net) shouldBe Nil
      backend.get(oid1).get.data.getByteArray shouldBe appliedData
      frontend.transactions.contains(txid1) shouldBe false
      crl.txDel shouldBe true
  }

  test("a late client prepare with data for an applied transaction does not apply the data again") {
    withApplied(): (frontend, backend, net, _) =>
      frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = true, owed = Set()))

      resolvedMessages(net).map(r => (r.committed, r.proof)) shouldBe List((true, true))
      // Answered after its objects loaded: the held response goes out as a memoryless abort vote
      votes(net).map(v => (v.disposition, v.response)) shouldBe
        List((TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(None))))
      committedMessages(net) shouldBe Nil
      backend.get(oid1).get.data.getByteArray shouldBe appliedData
  }

  test("a prover applies nothing to its other objects") {
    withApplied(): (frontend, backend, net, crl) =>
      // op1 shows the update; the fence still lacks it, and its data is in the prepare
      val txd = lockTxd(txid1, List(overwrite(op1), overwrite(fencePtr)))
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(objectUpdates = List(
        ObjectUpdate(oid1, DataBuffer(Array[Byte](7))), ObjectUpdate(fenceOid, DataBuffer(Array[Byte](8))))))
      resolvedMessages(net).map(_.proof) shouldBe List(true)

      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = false, owed = Set()))

      backend.get(fenceOid).get.metadata.revision shouldBe rev0
      backend.get(fenceOid).get.data.size shouldBe 0
      committedMessages(net) shouldBe Nil
      frontend.transactions.contains(txid1) shouldBe false
      crl.txDel shouldBe true
  }

  test("a prover is forgotten on a late commit, recording it finalized, committed and late") {
    val statusCache = new TransactionStatusCache()
    val backend = new MapBackend(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, new TestCrl,
      statusCache)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(provenRev, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(appliedData))
    frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd))
    frontend.receiveTransactionMessage(TxResolved(storeId, recoveryDriver, txid1, committed = true, late = true,
      proof = false))

    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net) shouldBe Nil
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  test("a forgotten transaction is proven committed although a lock-only object has moved on") {
    withApplied(fenceRevision = new ObjectRevision(new UUID(0, 99))): (frontend, _, net, _) =>
      val txd = lockTxd(txid1, List(overwrite(op1), SharedRevisionLock(fencePtr, rev0)))
      frontend.receiveTransactionMessage(driverPrepare(txd))

      resolvedMessages(net).map(_.committed) shouldBe List(true)
      votes(net) shouldBe Nil
  }

  test("a forgotten KeyValue transaction without a required revision is proven committed by a key it wrote") {
    val (frontend, backend, _, net) = mkFrontendWithNet()
    val contents = Map(k(1) -> new ValueState(Value(Array[Byte](1)), provenRev, HLCTimestamp(100), None))
    backend.bootstrapAllocate(oid2, ObjectType.KeyValue, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(100)),
      KVObjectState.encode(None, None, None, None, contents))
    val txd = lockTxd(txid1, List(KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.DoesNotExist(k(1))))))

    frontend.receiveTransactionMessage(driverPrepare(txd))

    resolvedMessages(net).map(_.committed) shouldBe List(true)
    votes(net) shouldBe Nil
  }

  test("a forgotten transaction whose object is at another revision still gets an abort vote") {
    withApplied(): (frontend, _, net, _) =>
      val other = TransactionId(new UUID(0, 98))
      frontend.receiveTransactionMessage(driverPrepare(lockTxd(other, List(overwrite(op1)))).copy(fromClient = true,
        objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](7))))))

      resolvedMessages(net) shouldBe Nil
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
  }

  // ---- A prepare for an unknown transaction that started long ago ----
  //
  // There is no age limit: replay safety comes from pins, not from refusing old prepares. Such a prepare is checked
  // afresh, and proven committed only on the abort path.

  private def ago(d: FiniteDuration): HLCTimestamp = HLCTimestamp((HLCTimestamp.now.wallTime - d.toMillis) << 16)

  private def withPlainFrontend(statusCache: TransactionStatusCache = new TransactionStatusCache())
                         (f: (Frontend, MapBackend, TestNet) => Unit): Unit =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl, statusCache)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    f(frontend, backend, net)

  /** Sends a client prepare, started `age` ago, of txid1: an insert of key 1 with only `MayExist(k(1))` required, to
    * kvPtr whose key 1 already carries txid1's revision. Its requirements pass, so it is answered
    * TxResolved(committed) only by a store that proves without checking them: a muted one
    */
  private def appliedKVPrepare(frontend: Frontend, backend: MapBackend, age: FiniteDuration): Unit =
    val contents = Map(k(1) -> new ValueState(Value(Array[Byte](1)), provenRev, HLCTimestamp(100), None))
    backend.bootstrapAllocate(oid2, ObjectType.KeyValue, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(100)),
      KVObjectState.encode(None, None, None, None, contents))
    val insert = KeyValueOperation.encode(List(new Insert(k(1), Array[Byte](1))), ida)(0)
    val update = KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.MayExist(k(1))))
    val txd = lockTxd(txid1, List(update)).copy(startTimestamp = ago(age))
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid2, insert))))

  test("an old unknown prepare whose requirements pass is voted on, though an object shows it") {
    // The proof runs only on the abort path, whatever the transaction's age
    withPlainFrontend(): (frontend, backend, net) =>
      appliedKVPrepare(frontend, backend, FiniteDuration(60, SECONDS))

      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      resolvedMessages(net) shouldBe Nil
  }

  // ---- A rebuilt store's acceptor start (Frontend.acceptorSince) ----
  //
  // A rebuilt store lost the Paxos state of its old incarnation. A transaction that started before its acceptor existed
  // may have been chosen with the old incarnation's accept, so the store gives it no promise, vote or accept.

  private def withAcceptorSince(statusCache: TransactionStatusCache = new TransactionStatusCache())
                               (f: (Frontend, MapBackend, TestNet, TestCrl) => Unit): Unit =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val crl = new TestCrl
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache,
      acceptorSince = Some(ago(FiniteDuration(2, SECONDS))))
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    f(frontend, backend, net, crl)

  private def acceptResponses(net: TestNet): List[TxAcceptResponse] =
    net.sentTx.collect { case r: TxAcceptResponse => r }

  test("a rebuilt store gives a transaction older than its acceptor no promise or vote, and saves nothing") {
    withAcceptorSince(): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(3, SECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net) shouldBe Nil
      resolvedMessages(net) shouldBe Nil
      crl.txSaved shouldBe false
      frontend.transactions.contains(txid1) shouldBe false
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
  }

  test("a rebuilt store gives an older transaction's data-less driver prepare no promise or vote") {
    withAcceptorSince(): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(3, SECONDS)))
      frontend.receiveTransactionMessage(driverPrepare(txd))

      votes(net) shouldBe Nil
      crl.txSaved shouldBe false
      frontend.transactions.contains(txid1) shouldBe false
  }

  test("a rebuilt store accepts nothing for a transaction older than its acceptor") {
    withAcceptorSince(): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(3, SECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.receiveTransactionMessage(TxAccept(storeId, StoreId(poolId, txd.designatedLeaderUID), txid1,
        ProposalId.initialProposal(txd.designatedLeaderUID), true))

      acceptResponses(net) shouldBe Nil
      crl.txSaved shouldBe false
  }

  test("a rebuilt store still proves committed a transaction older than its acceptor whose update it shows") {
    withAcceptorSince(): (frontend, backend, net, crl) =>
      appliedKVPrepare(frontend, backend, FiniteDuration(3, SECONDS))
      // A muted prover answers a later prepare with the proof again, never a promise
      val later = driverPrepare(frontend.transactions(txid1).txd)
      frontend.receiveTransactionMessage(later)

      resolvedMessages(net).map(r => (r.committed, r.proof)) shouldBe List((true, true), (true, true))
      votes(net) shouldBe Nil
      crl.txSaved shouldBe false
  }

  test("a rebuilt store votes on a transaction started after its acceptor") {
    withAcceptorSince(): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(1, SECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      crl.txSaved shouldBe true
  }

  test("a rebuilt store gives no promise for an older transaction cached as resolved, and keeps the cached outcome") {
    val statusCache = new TransactionStatusCache()
    statusCache.updateStatus(txid1, TransactionStatus.Committed)
    withAcceptorSince(statusCache): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(3, SECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net) shouldBe Nil
      crl.txSaved shouldBe false
      frontend.transactions.contains(txid1) shouldBe false
      statusCache.getStatus(txid1).map(_.status) shouldBe Some(TransactionStatus.Committed)
  }

  test("a late client prepare with data inside the status cache is answered from the cache") {
    val statusCache = new TransactionStatusCache()
    statusCache.updateStatus(txid1, TransactionStatus.Committed, finalized = true)
    withPlainFrontend(statusCache): (frontend, _, net) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(6500, MILLISECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net) shouldBe Nil
      frontend.transactions.contains(txid1) shouldBe false
  }

  test("an unknown prepare for a transaction cached as resolved uses the cached outcome") {
    val statusCache = new TransactionStatusCache()
    statusCache.updateStatus(txid1, TransactionStatus.Committed)
    withPlainFrontend(statusCache): (frontend, _, net) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(6500, MILLISECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      // Checked as usual, and the outcome known: no locks, the cached status kept
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      frontend.transactions(txid1).holdsLocks shouldBe false
      statusCache.getStatus(txid1).map(_.status) shouldBe Some(TransactionStatus.Committed)
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
  }

  test("an unknown prepare older than the status cache duration is checked afresh") {
    withPlainFrontend(): (frontend, _, net) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(60, SECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      frontend.transactions(txid1).holdsLocks shouldBe true
  }

  // ---- Write locks are held until a commit is applied ----

  test("a commit held for missing data has no vote and so no write locks; it applies when the data arrives") {
    // A commit vote requires the data (Tx.commitAwaitsData), so a commit held for data never has locks to keep
    withDataless(): (frontend, backend, net, _) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      frontend.transactions(txid1).holdsLocks shouldBe false
      backend.get(oid1).get.metadata.revision shouldBe rev0

      frontend.receiveTransactionMessage(clientPrepare(txd))
      backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid1)
      frontend.transactions.get(txid1).forall(!_.holdsLocks) shouldBe true
  }

  test("a commit held behind an in-flight commit of its object keeps its write locks until applied") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    def locksOn(ptr: ObjectPointer): Set[TransactionId] =
      frontend.readObjectForNetwork(clientId, new UUID(1, 1), ptr, FullObject())
      net.clientMessage() match
        case Some(rr: ReadResponse) => rr.result.toOption.get.lockedWriteTransactions
        case other => throw new AssertionError(s"Expected a ReadResponse, got $other")

    // txid2 votes commit and locks op1 and the fence
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1), overwrite(fencePtr))))
      .disposition shouldBe TransactionDisposition.VoteCommit
    // txid1 collides on the fence but is committed by the other stores' votes; its backend commit is held
    prepare(frontend, net, lockTxd(txid1, List(overwrite(fencePtr)))).disposition shouldBe TransactionDisposition.VoteAbort
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1

    // txid2 is committed too, but its commit waits for txid1's to persist
    resolve(frontend, net, txid2, committed = true)
    backend.heldCommits shouldBe 1
    backend.issued shouldBe List(txid1)
    frontend.transactions(txid2).holdsLocks shouldBe true
    locksOn(op1) shouldBe Set(txid2)
    locksOn(fencePtr) shouldBe Set(txid2)

    // txid1 persists: txid2 is applied in memory, and only then are its locks released
    backend.deliverOne()
    backend.issued shouldBe List(txid1, txid2)
    locksOn(op1) shouldBe Set()
    locksOn(fencePtr) shouldBe Set()
    frontend.transactions.get(txid2).forall(!_.holdsLocks) shouldBe true
    readData(frontend, net, op1) shouldBe Right(List[Byte](1))
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
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid, committed, late = false, proof = false))
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
  def recoveredFrontend(status: TransactionStatus.Value, lookup: ManualLookup = new ManualLookup): (Frontend, TestNet) =
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
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(),
      unackedLookup = lookup)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, net)

  test("a transaction recovered from the CRL already resolved takes no locks") {
    val (frontend, net) = recoveredFrontend(TransactionStatus.Aborted)
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a transaction recovered from the CRL with a commit vote holds its locks") {
    val lookup = new ManualLookup
    val (frontend, net) = recoveredFrontend(TransactionStatus.Unresolved, lookup)

    val r2 = prepare(frontend, net, lockTxd(txid2, List(overwrite(op1))))
    r2.disposition shouldBe TransactionDisposition.VoteAbort
    r2.collisions shouldBe List(txid1)

    resolve(frontend, net, txid1, committed = false)
    // Neither the recovery's UnackedCommits lookup nor the one started after the abort finds an entry
    lookup.answer(txid1, Success(false))
    lookup.answer(txid1, Success(false))
    prepare(frontend, net, lockTxd(txid3, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a recovered commit vote answers a driver prepare with its vote") {
    val (frontend, net) = recoveredFrontend(TransactionStatus.Unresolved)
    frontend.receiveTransactionMessage(mkPrepare(lockTxd(txid1, List(overwrite(op1))), fromClient = false))
    net.sentTx.collect { case r: TxPrepareResponse if r.transactionId == txid1 => r.disposition } shouldBe
      List(TransactionDisposition.VoteCommit)
  }

  test("a committed transaction is applied while another transaction holds a lock on its object") {
    val (frontend, backend, net) = lockFixture()

    // txid1 locks op1. txid2 collides here but is committed by the votes of other stores
    prepare(frontend, net, lockTxd(txid1, List(RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 2)))))
      .disposition shouldBe TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort

    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid2, committed = true, late = false, proof = false))
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
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set()))
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

    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set()))
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    frontend.transactions.contains(txid1) shouldBe false

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a prepare arriving after its transaction resolved aborted takes no locks") {
    val (frontend, _, net) = lockFixture()

    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false, late = false, proof = false))
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a prepare arriving after its transaction resolved committed takes no locks") {
    val (frontend, _, net) = lockFixture()

    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))))

    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  // ---- Locks survive the discard of a referenced object state ----

  def keyRevision(n: Int): KeyValueUpdate =
    KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.KeyRevision(k(n), rev0)))

  // A failed update discards the object's state (Tx.commit). The requirement check decodes the operations, so bad
  // data can no longer pass it and then fail in application; these tests discard the state directly.

  test("discarding an object's state does not drop another transaction's key lock") {
    val (frontend, _, net) = lockFixture()

    prepare(frontend, net, lockTxd(txid1, List(keyRevision(1)))).disposition shouldBe TransactionDisposition.VoteCommit
    frontend.discard(kvPtr.id)

    val r = prepare(frontend, net, lockTxd(txid3, List(keyRevision(1))))
    r.disposition shouldBe TransactionDisposition.VoteAbort
  }

  test("after a failed backend commit, every transaction on the object votes abort, shared lock holders included") {
    val (frontend, _, net) = failingCommitFixture()

    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    // Collides here, but is committed by the other stores' votes. Its backend commit fails
    prepare(frontend, net, lockTxd(txid2, List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteAbort
    resolve(frontend, net, txid2, committed = true)

    prepare(frontend, net, lockTxd(txid3, List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteAbort

    // Even one built on the failed commit, after the holder resolves: until the store reloads
    resolve(frontend, net, txid1, committed = false)
    val afterFailed = DataUpdate(fencePtr, ObjectRevision(txid2), DataUpdateOperation.Overwrite)
    prepare(frontend, net, lockTxd(TransactionId(new UUID(0, 14)), List(afterFailed))).disposition shouldBe
      TransactionDisposition.VoteAbort
    prepare(frontend, net, lockTxd(TransactionId(new UUID(0, 15)), List(SharedRevisionLock(fencePtr, ObjectRevision(txid2)))))
      .disposition shouldBe TransactionDisposition.VoteAbort
    // Other objects are unaffected
    prepare(frontend, net, lockTxd(TransactionId(new UUID(0, 16)), List(SharedRevisionLock(op1, rev0)))).disposition shouldBe
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
    frontend.recover(StoreRecoveryState(recoverable))

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
    frontend.recover(StoreRecoveryState(recoverable))
    backend.deliver() // The shared read of op1 completes after recover returned
    backend.deliver() // and so does any read issued once the held prepare is handled

    val r = responseFor(net, txid2)
    r.disposition shouldBe TransactionDisposition.VoteAbort
    r.collisions shouldBe List(txid1)
  }

  test("a store recovering from a crl_save.log may hold its transactions throughout recover") {
    // mayHoldTransaction is read from other threads (the repair scan) while recover runs on the store's
    var frontend: Option[Frontend] = None
    var seenDuringRecovery: List[Boolean] = Nil
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
        frontend.foreach(f => seenDuringRecovery = f.mayHoldTransaction(txid1) :: seenDuringRecovery)
        List(commitVote(txid1, List(overwrite(op1))))
    val backend = new MapBackend(storeId)
    populate(backend)
    val f = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet, crl,
      new TransactionStatusCache(), deferRecovery = true)
    backend.setCompletionHandler(op => f.backendOperationComplete(op))
    frontend = Some(f)

    // Read as the host reads it: before recover, while recovery is awaited
    f.recover(StoreRecoveryState(crl.getFullRecoveryState(storeId)))
    seenDuringRecovery shouldBe List(true)
    f.mayHoldTransaction(txid1) shouldBe true
  }

  test("a store's stub set is loaded on recovery") {
    val backend = new MapBackend(storeId)
    populate(backend)
    // Recovered at construction, read from the CRL
    val crl = new TestCrl:
      override def getAppliedStubs(storeId: StoreId): Set[TransactionId] = Set(txid1)
    val f = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet, crl,
      new TransactionStatusCache())
    f.hasAppliedStub(txid1) shouldBe true
    f.hasAppliedStub(txid2) shouldBe false
    f.transactions shouldBe empty

    // Deferred, as a host recovers: from the state it read
    val deferred = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
      new TestCrl, new TransactionStatusCache(), deferRecovery = true)
    deferred.hasAppliedStub(txid2) shouldBe false
    deferred.recover(StoreRecoveryState(Nil, Set(txid2)))
    deferred.hasAppliedStub(txid2) shouldBe true
    deferred.appliedStubs shouldBe Set(txid2)
    // A stub is not a transaction
    deferred.transactions shouldBe empty
  }

  test("recordAppliedStub continues once the stub is durable, and dropAppliedStub deletes it") {
    val backend = new MapBackend(storeId)
    populate(backend)
    val crl = new TestCrl
    crl.holdStubSaves = true
    val f = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet, crl,
      new TransactionStatusCache())
    var durable = false
    f.recordAppliedStub(txid1) { durable = true }
    f.hasAppliedStub(txid1) shouldBe true
    crl.stubsSaved shouldBe List(txid1)
    durable shouldBe false
    crl.heldStubSaves.foreach(_())
    durable shouldBe true

    f.dropAppliedStub(txid1)
    f.hasAppliedStub(txid1) shouldBe false
    crl.stubsDeleted shouldBe List(txid1)
    // Only a stub held is deleted
    f.dropAppliedStub(txid2)
    crl.stubsDeleted shouldBe List(txid1)
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

  /** The ReadResponse sent last, if any, taken */
  def takeReadResponse(net: TestNet): Option[ReadResponse] =
    net.clientMessage().collect { case rr: ReadResponse => rr }

  test("a read of a recovered commit vote's object, arriving before the vote has locked, is not answered unlocked") {
    // A read that joined the recovered transaction's load was answered before the load took its locks: a voter that
    // has not applied the transaction reported the state it replaces, unlocked, and a repair trusting that read could
    // undo the commit on a store that applied it
    val backend = new DeferredReadBackend
    populate(backend)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
        List(commitVote(txid1, List(overwrite(op1))))
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    frontend.readObjectForNetwork(clientId, new UUID(1, 1), op1, FullObject())
    backend.deliver()
    backend.deliver()
    takeReadResponse(net).foreach(_.result.toOption.get.lockedWriteTransactions should contain (txid1))

    // Once recovered, reads are answered, with the recovered vote's lock
    frontend.readObjectForNetwork(clientId, new UUID(1, 2), op1, FullObject())
    backend.deliver()
    takeReadResponse(net).map(_.result.toOption.get.lockedWriteTransactions) shouldBe Some(Set(txid1))
  }

  test("a repair does not reconcile a store while it holds for recovered transactions") {
    val backend = new DeferredReadBackend
    populate(backend)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
        List(commitVote(txid1, List(overwrite(op1))))
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
      crl, new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    val snap = RepairSnapshot.missing(oid1)
    val read = new DataObjectState(op1, ObjectRevision(txid2), ObjectRefcount(1, 1), HLCTimestamp(500), HLCTimestamp(500),
      ida, 1, DataBuffer(Array[Byte](9)))
    val p = Promise[ReconcileResult]()
    frontend.reconcileForRepair(snap, read, p)
    backend.deliver()
    backend.deliver()
    p.future.value.flatMap(_.toOption).map(_.stale) shouldBe Some(true)
  }

  test("a network read reports a held key lock and an object-level lock") {
    val (frontend, _, net) = lockFixture()

    val keyLock = lockTxd(txid1, List(KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.KeyRevision(k(1), rev0)))))
    prepare(frontend, net, keyLock).disposition shouldBe TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit

    def state(ptr: ObjectPointer): ReadResponse.CurrentState =
      frontend.readObjectForNetwork(clientId, new UUID(1, 1), ptr, FullObject())
      takeReadResponse(net).get.result.toOption.get

    val kv = state(kvPtr)
    kv.lockedKeys shouldBe Set(k(1))
    kv.objectLocked shouldBe false
    val data = state(op1)
    data.objectLocked shouldBe true
    data.lockedWriteTransactions shouldBe Set(txid2)
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
    frontend.recover(StoreRecoveryState(recoverable))
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
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false, late = false, proof = false))
    }
    out.toString should not include "UNEXPECTED"
  }

  // ---- A recovered commit vote learns its commit from the UnackedCommits tree ----
  //
  // T committed and was finalized and forgotten while this store was down. Its change (a key delete) leaves no trace a
  // proof of commit or repair could find, so the finalizer recorded the miss in the pool's UnackedCommits tree.

  private val unackedTxid = TransactionId(new UUID(0, 40))
  private val unackedDriver = StoreId(poolId, 2)

  /** T's update: a KV Delete of key 2 on kvPtr */
  private val deleteK2: KeyValueUpdate = KeyValueUpdate(kvPtr, None, None, Nil, deletesKeys = true)

  /** A Frontend recovering T, a commit vote of `reqs` deleting key 2 of kvPtr, with its lookups made through `lookup` */
  private def unackedFixture(reqs: List[TransactionRequirement] = List(deleteK2),
                             statusCache: TransactionStatusCache = new TransactionStatusCache())
      : (Frontend, MapBackend, TestNet, TestCrl, ManualLookup) =
    val backend = new MapBackend(storeId)
    populate(backend)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    // Cached so the recovered transaction's object loads during Frontend construction
    cache.insert(mkKVObjectState())
    val update = KeyValueOperation.encode(List(new Delete(k(2))), ida)(0)
    val trs = TransactionRecoveryState(storeId, lockTxd(unackedTxid, reqs).serialize(),
      List(ObjectUpdate(oid2, update)), TransactionDisposition.VoteCommit, TransactionStatus.Unresolved,
      PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val net = new TestNet
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache, unackedLookup = lookup)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, backend, net, crl, lookup)

  private def kvKeys(backend: MapBackend): Set[Key] = KVObjectState.decode(backend.get(oid2).get.data).content.keySet

  private def lockedWriteTransactions(frontend: Frontend, net: TestNet, ptr: ObjectPointer): Set[TransactionId] =
    frontend.readObjectForNetwork(clientId, new UUID(1, 7), ptr, FullObject())
    net.clientMessage() match
      case Some(rr: ReadResponse) => rr.result.toOption.get.lockedWriteTransactions
      case other => throw new AssertionError(s"Expected a ReadResponse, got $other")

  /** Runs `f`, returning the Tx log lines naming T, with their levels */
  private def unackedLog(f: => Unit): List[(scribe.Level, String)] =
    val lines = new LinkedBlockingQueue[(scribe.Level, String)]()
    val handler = scribe.handler.LogHandler(scribe.Level.Info): r =>
      val text = r.logOutput.plainText
      if text.contains(unackedTxid.toString) then
        lines.add(r.level -> text)
    scribe.Logger.root.withHandler(handler).replace()
    try f
    finally scribe.Logger.root.withoutHandler(handler).replace()
    lines.toArray(Array.empty[(scribe.Level, String)]).toList

  test("a recovered commit vote with a UnackedCommits entry applies its update and is forgotten on TxFinalized") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = unackedFixture(statusCache = statusCache)
    lookup.requests shouldBe List(unackedTxid)
    kvKeys(backend) should contain (k(2))

    val log = unackedLog:
      lookup.answer(unackedTxid, Success(true))

    kvKeys(backend) should not contain k(2)
    frontend.transactions(unackedTxid).provesCommitToItsDriver shouldBe true
    frontend.transactions(unackedTxid).awaitsFirstUnackedLookup shouldBe false
    // Ruling 9: still live, its CRL and acceptor state kept until TxFinalized. Ruling 12: no status cache entry
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
    crl.deleted should not contain (unackedTxid)
    statusCache.getStatus(unackedTxid).map(_.status).forall(_ == TransactionStatus.Unresolved) shouldBe true

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false, owed = Set()))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.deleted should contain (unackedTxid)
    log.exists((level, text) => level == scribe.Level.Info &&
      text.contains(s"learned committed transaction $unackedTxid") &&
      text.contains("applying its update to objects")) shouldBe true
  }

  test("a recovered commit vote of a non-traceless transaction applies on load when its entry exists") {
    val backend = new MapBackend(storeId)
    populate(backend)
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(mkKVObjectState())
    cache.insert(os)
    val reqs = List(DataUpdate(op1, os.metadata.revision, DataUpdateOperation.Overwrite))
    val trs = TransactionRecoveryState(storeId, lockTxd(unackedTxid, reqs).serialize(),
      List(ObjectUpdate(oid1, DataBuffer(Array[Byte](9)))), TransactionDisposition.VoteCommit,
      TransactionStatus.Unresolved, PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, cache, new TestNet, crl, new TransactionStatusCache(),
      unackedLookup = lookup)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    lookup.requests shouldBe List(unackedTxid)

    lookup.answer(unackedTxid, Success(true))

    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](9)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
    frontend.receiveTransactionMessage(TxFinalized(storeId, StoreId(poolId, 2), unackedTxid, committed = true,
      late = false, owed = Set()))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.deleted should contain (unackedTxid)
  }

  test("a recovered commit vote without a UnackedCommits entry accepts an abort") {
    val (frontend, backend, _, crl, lookup) = unackedFixture()
    lookup.answer(unackedTxid, Success(false))

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set()))
    // The abort waits for a lookup started after it arrived
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    frontend.transactions.contains(unackedTxid) shouldBe true
    lookup.answer(unackedTxid, Success(false))

    kvKeys(backend) should contain (k(2))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.deleted should contain (unackedTxid)
  }

  test("a retransmitted abort does not push back the lookup a held abort waits for") {
    // A driver resends its resolution until it hears back: each copy moving the counting window forward would keep a
    // lookup in flight from ever counting, and the Tx holding its locks
    val (frontend, _, _, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, Success(false))

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    lookup.answer(unackedTxid, Success(false))

    frontend.transactions.get(unackedTxid).map(_.status).forall(_ == TransactionStatus.Aborted) shouldBe true
    frontend.transactions.get(unackedTxid).map(_.holdsLocks) shouldBe Some(false)
  }

  test("a held TxFinalized abort is not replaced by a later held TxResolved, so the Tx is forgotten once accepted") {
    val (frontend, _, _, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, Success(false))

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set()))
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.answer(unackedTxid, Success(false))

    frontend.transactions.contains(unackedTxid) shouldBe false
  }

  test("an abort arriving before the UnackedCommits entry is visible waits for a later lookup and then commits") {
    val (frontend, backend, _, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, Success(false))

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 2 // a fresh lookup started after the abort
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Unresolved

    lookup.answer(unackedTxid, Success(true))
    kvKeys(backend) should not contain k(2)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
  }

  test("an abort arriving while the recovery's UnackedCommits lookup is in flight waits for a fresh lookup") {
    val (frontend, backend, _, _, lookup) = unackedFixture()
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 1

    // Started before the abort, so its answer does not count: another lookup is made
    lookup.answer(unackedTxid, Success(false))
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Unresolved

    lookup.answer(unackedTxid, Success(false))
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Aborted
    kvKeys(backend) should contain (k(2))
  }

  test("a failing UnackedCommits lookup holds the abort and is retried at heartbeat") {
    val (frontend, _, net, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, Failure(new Exception("metadata pool down")))
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set()))
    lookup.requests.count(_ == unackedTxid) shouldBe 2

    // Only failed lookups are retried
    frontend.retryUnackedLookups()
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    lookup.answer(unackedTxid, Failure(new Exception("still down")))
    frontend.retryUnackedLookups()
    lookup.requests.count(_ == unackedTxid) shouldBe 3

    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Unresolved
    lockedWriteTransactions(frontend, net, kvPtr) should contain (unackedTxid)
  }

  test("an object whose requirements fail on the UnackedCommits local apply is skipped and the Tx still completes") {
    // The store's key 2 is at a different revision than T requires
    val stale = KeyValueUpdate(kvPtr, None, None,
      List(KeyValueUpdate.KeyRevision(k(2), new ObjectRevision(new UUID(0, 41)))), deletesKeys = true)
    val (frontend, backend, _, crl, lookup) = unackedFixture(List(stale))

    lookup.answer(unackedTxid, Success(true))

    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
    kvKeys(backend) should contain (k(2))
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false, owed = Set()))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.deleted should contain (unackedTxid)
  }

  test("a UnackedCommits lookup whose answer is lost is made again at heartbeat once it times out") {
    val (frontend, backend, _, _, lookup) = unackedFixture()
    val lost = lookup.made.last._2 // the recovery's lookup, whose answer never arrives
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 1

    val now = System.nanoTime()
    frontend.retryUnackedLookups(now)
    lookup.requests.count(_ == unackedTxid) shouldBe 1
    frontend.retryUnackedLookups(now + Frontend.UnackedLookupTimeout.toNanos + 1)
    lookup.requests.count(_ == unackedTxid) shouldBe 2

    // The lost lookup read the tree before the abort arrived: its late answer finding no entry does not count
    lookup.answer(unackedTxid, Success(false), Some(lost))
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Unresolved
    lookup.requests.count(_ == unackedTxid) shouldBe 2

    lookup.answer(unackedTxid, Success(true))
    kvKeys(backend) should not contain k(2)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
  }

  test("a UnackedCommits lookup answering after the Tx resolved committed is ignored") {
    val (frontend, backend, _, _, lookup) = unackedFixture()
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false, owed = Set()))
    kvKeys(backend) should not contain k(2)
    frontend.transactions.contains(unackedTxid) shouldBe false

    lookup.answer(unackedTxid, Success(true))
    frontend.transactions.contains(unackedTxid) shouldBe false
  }

  test("a recovered commit vote that found its entry is forgotten on a later abort without undoing its update") {
    val (frontend, backend, net, crl, lookup) = unackedFixture()
    lookup.answer(unackedTxid, Success(true))
    kvKeys(backend) should not contain k(2)
    val committedBefore = committedMessages(net).size
    committedBefore should be > 0

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false,
      proof = false))
    frontend.transactions.contains(unackedTxid) shouldBe true // TxFinalized forgets it
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set()))

    frontend.transactions.contains(unackedTxid) shouldBe false
    kvKeys(backend) should not contain k(2)
    committedMessages(net).size shouldBe committedBefore
    crl.deleted should contain (unackedTxid)
  }

  test("a recovered commit vote that found its entry is forgotten on a late commit without applying it again") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, _, lookup) = unackedFixture(statusCache = statusCache)
    lookup.answer(unackedTxid, Success(true))
    val committedBefore = committedMessages(net).size

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = true, owed = Set()))

    frontend.transactions.contains(unackedTxid) shouldBe false
    kvKeys(backend) should not contain k(2)
    committedMessages(net).size shouldBe committedBefore
    lookup.requests.count(_ == unackedTxid) shouldBe 1
    statusCache.getStatus(unackedTxid) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  // ---- Late commits (replay-pins design, 1c): applied only by a commit vote whose UnackedCommits entry exists ----

  /** A live Frontend over populated objects, whose op1 commit vote for txid1 is in place, with lookups through `lookup` */
  private def lateFixture(statusCache: TransactionStatusCache = new TransactionStatusCache())
      : (Frontend, MapBackend, TestNet, TestCrl, ManualLookup) =
    val backend = new MapBackend(storeId)
    populate(backend)
    val net = new TestNet
    val crl = new TestCrl
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      statusCache, unackedLookup = lookup)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, backend, net, crl, lookup)

  private def lateCommit(frontend: Frontend, finalized: Boolean): Unit =
    if finalized then
      frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, txid1, committed = true, late = true, owed = Set()))
    else
      frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, txid1, committed = true, late = true,
        proof = false))

  test("a live Tx with its own commit vote applies a late commit when its entry exists") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache)
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)

    lateCommit(frontend, finalized = true)
    lookup.requests shouldBe List(txid1)
    frontend.transactions(txid1).status shouldBe TransactionStatus.Unresolved

    lookup.answer(txid1, Success(true))
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net).map(_.objectCommitErrors).distinct shouldBe List(Nil)
    crl.deleted should contain (txid1)
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  test("a live Tx with its own commit vote applies nothing on a late commit when its entry is absent") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache)
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)

    lateCommit(frontend, finalized = false)
    lateCommit(frontend, finalized = true) // A retransmit makes no second lookup
    lookup.requests shouldBe List(txid1)
    lookup.answer(txid1, Success(false))

    backend.get(oid1).get.data.size shouldBe 0
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net) shouldBe Nil
    crl.deleted should contain (txid1)
    lockedWriteTransactions(frontend, net, op1) shouldBe empty
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  test("a Tx with its own commit vote holds a late commit while the lookup fails, and applies once it succeeds") {
    val (frontend, backend, net, _, lookup) = lateFixture()
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)
    lateCommit(frontend, finalized = true)

    lookup.answer(txid1, Failure(new Exception("metadata pool down")))
    frontend.transactions(txid1).status shouldBe TransactionStatus.Unresolved
    lockedWriteTransactions(frontend, net, op1) should contain (txid1)
    frontend.retryUnackedLookups()
    lookup.requests.count(_ == txid1) shouldBe 2

    // A lost answer is asked again once it times out
    frontend.retryUnackedLookups(System.nanoTime() + Frontend.UnackedLookupTimeout.toNanos + 1)
    lookup.requests.count(_ == txid1) shouldBe 3

    lookup.answer(txid1, Success(true))
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    frontend.transactions.contains(txid1) shouldBe false
  }

  test("a Tx without a commit vote applies nothing on a late commit and sends no TxCommitted") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache)
    // op1 is at rev0, so this requirement fails: an abort vote
    val txd = lockTxd(txid1, List(DataUpdate(op1, new ObjectRevision(new UUID(0, 77)), DataUpdateOperation.Overwrite)))
    frontend.receiveTransactionMessage(clientPrepare(txd))
    expectVote(net, TransactionDisposition.VoteAbort)

    lateCommit(frontend, finalized = false)

    lookup.requests shouldBe Nil
    backend.get(oid1).get.data.size shouldBe 0
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net) shouldBe Nil
    crl.deleted should contain (txid1)
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  test("a Tx that learned its commit normally finalizes on a late commit") {
    val (frontend, backend, net, crl, lookup) = lateFixture()
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, txid1, committed = true, late = false,
      proof = false))
    val committedBefore = committedMessages(net).size
    lateCommit(frontend, finalized = false)

    lookup.requests shouldBe Nil
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    committedMessages(net).size shouldBe committedBefore
    frontend.transactions.contains(txid1) shouldBe false
    crl.deleted should contain (txid1)
  }

  test("a late TxFinalized for an unknown transaction is cached as finalized, committed and late") {
    val statusCache = new TransactionStatusCache()
    val (frontend, _, _, _, _) = lateFixture(statusCache)
    lateCommit(frontend, finalized = true)
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  test("a late TxResolved for an unknown transaction is cached as finalized, committed and late") {
    // A late outcome implies the transaction was finalized, as the Tx paths record it (Ruling 18)
    val statusCache = new TransactionStatusCache()
    val (frontend, _, _, _, _) = lateFixture(statusCache)
    lateCommit(frontend, finalized = false)
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  test("a proof is no outcome: neither a live Tx nor the status cache takes it") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, _, _) = lateFixture(statusCache)
    val proof = TxResolved(storeId, StoreId(poolId, 1), txid1, committed = true, late = false, proof = true)
    frontend.receiveTransactionMessage(proof)
    statusCache.getStatus(txid1) shouldBe None

    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)
    frontend.receiveTransactionMessage(proof)

    frontend.transactions(txid1).status shouldBe TransactionStatus.Unresolved
    backend.get(oid1).get.data.size shouldBe 0
    committedMessages(net) shouldBe Nil
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
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set()))
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
    // A transaction finds the object missing too, once it no longer waits for the deletion to persist
    sendPrepare(frontend, lockTxd(txid2, List(overwrite(op1))))
    prepareResponses(net, txid2) shouldBe empty
    backend.reads shouldBe reads

    backend.deliverCommits()
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
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

  /** A rebuild-like commit of op1 holding `b`, at the revision `txid` writes: not a transaction's commit */
  def rebuildCommit(frontend: Frontend, txid: TransactionId, b: Byte): Unit =
    val metadata = Metadata(ObjectRevision(txid), ObjectRefcount(1, 1), HLCTimestamp(200))
    val data = DataBuffer(Array[Byte](b))
    frontend.commit(new ObjectState(oid1, metadata, ObjectType.Data, data),
      CommitState(op1, metadata, ObjectType.Data, data), txid)

  test("two in-flight commits of one object serve the newest state") {
    val (frontend, backend, _, net) = commitWindowFixture()
    // Transactions' commits of one object are never in flight together; rebuilds' may be
    rebuildCommit(frontend, txid1, 1)
    rebuildCommit(frontend, txid2, 2)
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

  test("a transaction does not wait for a rebuild's in-flight commit") {
    val (frontend, backend, _, net) = commitWindowFixture()
    rebuildCommit(frontend, txid1, 1)
    backend.heldCommits shouldBe 1
    prepare(frontend, net, lockTxd(txid2, List(overwriteAfter(txid1)))).disposition shouldBe
      TransactionDisposition.VoteCommit
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

  test("a transaction on an object waits behind an opportunistic rebuild's backend write") {
    // Backend writes of one object are unordered: a transaction's commit issued while the rebuild's write is
    // outstanding could land first and be overwritten by it
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    frontend.readObjectForNetwork(clientId, new UUID(1, 1), op1, FullObject())
    net.clientMessage() should not be empty
    val os = mkObjectState(oid1)
    frontend.readObjectForOpportunisticRebuild(OpportunisticRebuild(storeId, clientId, op1, rev0,
      os.metadata.refcount, HLCTimestamp(50), DataBuffer(Array[Byte](4))))
    backend.heldCommits shouldBe 1

    sendPrepare(frontend, lockTxd(txid1, List(overwrite(op1))))
    prepareResponses(net, txid1) shouldBe Nil

    backend.deliverCommits()
    prepareResponses(net, txid1).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("a commit of a fresh state does not replace a discarded state whose locks are held") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(kvPtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    frontend.discard(kvPtr.id)

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
    frontend.discard(kvPtr.id)
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

  /** 0x20 is not a KeyValueOperation code, so this update fails its requirement check */
  val corruptKVUpdate: DataBuffer = DataBuffer(Array[Byte](0x20, 0x00))

  test("an unappliable update held behind an in-flight commit is checked only once that commit persists") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    val insertKey4 = KeyValueOperation.encode(List(new Insert(k(4), Array[Byte](4))), ida)(0)
    prepare(frontend, net, lockTxd(txid1, List(keyRevision(1))), insertKey4).disposition shouldBe
      TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    // Waits for txid1's commit to persist, and is committed by the other stores' votes meanwhile
    frontend.receiveTransactionMessage(mkPrepare(lockTxd(txid2, List(keyRevision(2))), fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid2, corruptKVUpdate))))
    prepareResponses(net, txid2) shouldBe empty
    resolve(frontend, net, txid2, committed = true)
    backend.heldCommits shouldBe 1

    def keys(): Set[Int] =
      frontend.readObjectForNetwork(clientId, new UUID(1, 3), kvPtr, FullObject())
      readResponseKeys(net)._1

    // txid1's committed state, from memory
    keys() shouldBe Set(1, 2, 3, 4)
    // txid2's update is then skipped, leaving txid1's state, which the backend now has
    backend.deliverCommits()
    backend.issued shouldBe List(txid1)
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

    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(RepairSnapshot.missing(oid1), repairState(), done)
    done.isCompleted shouldBe true
    done.future.value.get.get.complete shouldBe false
    backend.repairs shouldBe 0
  }

  test("a repair reconciles the keys of a KV object but leaves a key this store holds locked") {
    val (frontend, backend, net) = lockFixture()
    prepare(frontend, net, lockTxd(txid1, List(keyRevision(1)))).disposition shouldBe TransactionDisposition.VoteCommit

    val snapshot = Promise[RepairSnapshot]()
    frontend.snapshotForRepair(kvPtr, snapshot)
    // The read saw key 1 rewritten, key 3 deleted and key 4 inserted, and (not having asked this store) no locks
    def vs(n: Int, r: ObjectRevision) =
      org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState(Value(Array[Byte](n.toByte)), r, HLCTimestamp(9))
    val read = new org.aspen_ddp.aspen.client.KeyValueObjectState(kvPtr, rev0, ObjectRefcount(1, 1),
      HLCTimestamp(9), HLCTimestamp(9), ida, Some(k(0)), None, None, None,
      Map(k(1) -> vs(10, ObjectRevision(txid2)), k(2) -> vs(2, rev0), k(4) -> vs(4, ObjectRevision(txid2))))
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(snapshot.future.value.get.get, read, done)

    done.future.value.get.get shouldBe ReconcileResult(Set(k(1)), false, false)
    val stored = backend.m(oid2).kvState.get.content
    stored.keySet shouldBe Set(k(1), k(2), k(4))
    stored(k(1)).revision shouldBe rev0
    stored(k(4)).revision shouldBe ObjectRevision(txid2)
  }

  /** A read of op1 newer than anything the fixtures commit, at revision txid2 */
  def newerRepairState(): DataObjectState =
    val ts = HLCTimestamp((System.currentTimeMillis() + 3_600_000L) << 16)
    new DataObjectState(op1, ObjectRevision(txid2), ObjectRefcount(1, 1), ts, ts, ida, 1, DataBuffer(Array[Byte](5)))

  def repairSnapshot(frontend: Frontend, ptr: ObjectPointer): RepairSnapshot =
    val p = Promise[RepairSnapshot]()
    frontend.snapshotForRepair(ptr, p)
    p.future.value.get.get

  test("a repair during an in-flight commit is stale and writes nothing") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1

    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, op1), newerRepairState(), done)
    done.future.value.get.get.stale shouldBe true
    backend.repairs shouldBe 0
  }

  test("a prepare arriving while a repair write is pending withholds its vote until the write completes") {
    val (frontend, backend, _, net) = commitWindowFixture()
    backend.deferRepairs = true
    val done = Promise[ReconcileResult]()
    val read = newerRepairState()
    frontend.reconcileForRepair(repairSnapshot(frontend, op1), read, done)
    backend.repairs shouldBe 1
    done.isCompleted shouldBe false

    // Built on the repaired state, and started after it
    val update = DataUpdate(op1, ObjectRevision(txid2), DataUpdateOperation.Overwrite)
    val txd = lockTxd(txid1, List(update)).copy(startTimestamp = HLCTimestamp.happensAfter(read.timestamp))
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    prepareResponses(net, txid1) shouldBe empty

    backend.deliverRepairs()
    done.future.value.get.get.complete shouldBe true
    prepareResponses(net, txid1).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("a transaction prepared during a repair write is checked against, and commits on top of, the repaired state") {
    // No cache: only the Frontend's own pinning keeps the repaired state in memory while its write is pending
    val backend = new DeferredCommitBackend
    populate(backend)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, ObjectCache.NoCache, net, new TestCrl,
      new TransactionStatusCache())
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    backend.deferRepairs = true

    // The read has key 4, which the store missed
    def vs(n: Int) = org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState(Value(Array[Byte](n.toByte)), rev0,
      HLCTimestamp(n.toLong))
    val read = new org.aspen_ddp.aspen.client.KeyValueObjectState(kvPtr, rev0, ObjectRefcount(1, 1),
      HLCTimestamp(50), HLCTimestamp(50), ida, Some(k(0)), None, None, None, (1 to 4).map(n => k(n) -> vs(n)).toMap)
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, kvPtr), read, done)
    backend.repairs shouldBe 1

    // An insert of key 5 with only a key-level requirement, prepared while the repair write is pending
    val insert = KeyValueOperation.encode(List(new Insert(k(5), Array[Byte](5))), ida)(0)
    val update = KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.DoesNotExist(k(5))))
    frontend.receiveTransactionMessage(mkPrepare(lockTxd(txid1, List(update)), fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid2, insert))))
    prepareResponses(net, txid1) shouldBe empty

    backend.deliverRepairs()
    done.future.value.get.get.complete shouldBe true
    prepareResponses(net, txid1).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
    resolve(frontend, net, txid1, committed = true)
    backend.deliverCommits()
    backend.m(oid2).kvState.get.content.keySet shouldBe (1 to 5).map(k).toSet
  }

  test("a repair whose read completes during a backend commit of the object is stale and writes nothing") {
    val (frontend, backend, _, _) = commitWindowFixture()
    // As the snapshot would record the committed state, so the snapshot check alone would let the write through
    val snapshot = RepairSnapshot(oid1, ObjectRevision(txid1), ObjectRefcount(1, 1), HLCTimestamp(200), Map(), None)
    backend.deferReads = true
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(snapshot, newerRepairState(), done)
    backend.reads shouldBe 1

    // A rebuild's commit: the read may return the state it replaces, which is then not the state served
    val metadata = Metadata(ObjectRevision(txid1), ObjectRefcount(1, 1), HLCTimestamp(200))
    frontend.commit(new ObjectState(oid1, metadata, ObjectType.Data, DataBuffer(Array[Byte](3))),
      CommitState(op1, metadata, ObjectType.Data, DataBuffer(Array[Byte](3))), txid1)
    backend.deliverReads()
    done.future.value.get.get.stale shouldBe true
    backend.repairs shouldBe 0
  }

  test("a repair whose read completes during a deletion's commit is not applied") {
    val (frontend, backend, _, _) = commitWindowFixture()
    backend.deferReads = true
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(RepairSnapshot.missing(oid1), repairState(), done)
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
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set()))

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
    override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] = crl.readStoreState(storeId)
    override def getAppliedStubs(storeId: StoreId): Set[TransactionId] = crl.getAppliedStubs(storeId)
    override def saveAppliedStub(storeId: StoreId, txid: TransactionId, completionHandler: () => Unit): Unit =
      crl.saveAppliedStub(storeId, txid, completionHandler)
    override def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit = crl.deleteAppliedStub(storeId, txid)
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
      Await.result(crl.readStoreState(storeId), Duration(10, SECONDS)).transactions
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

  test("after finalization, a transaction built on an in-flight commit still waits for it to persist") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    finalizeTx1(frontend)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    prepareResponses(net, txid2) shouldBe empty

    backend.deliverOne()
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
    frontend.transactions(txid2).holdsLocks shouldBe true
  }

  // ---- One in-flight transaction commit per object ----

  /** Sends a client prepare carrying `data` for op1 */
  def sendData(frontend: Frontend, txd: TransactionDescription, data: DataBuffer): Unit =
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, data))))

  test("a transaction built on an in-flight commit waits for it to persist") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    prepareResponses(net, txid2) shouldBe empty
    frontend.transactions(txid2).holdsLocks shouldBe false

    backend.deliverOne()
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
    frontend.transactions(txid2).holdsLocks shouldBe true
  }

  test("a transaction not built on the in-flight commit waits rather than colliding") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    // Requires op1's revision from before txid1
    sendPrepare(frontend, lockTxd(txid2, List(overwrite(op1))))
    prepareResponses(net, txid2) shouldBe empty

    backend.deliverOne()
    val r = prepareResponses(net, txid2)
    r.map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
    r.head.collisions shouldBe Nil
  }

  test("a lock-only transaction waits on an in-flight commit of its object") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(SharedRevisionLock(op1, ObjectRevision(txid1)))))
    prepareResponses(net, txid2) shouldBe empty

    backend.deliverOne()
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("a wait on an in-flight commit has no timer") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    val tx2 = frontend.transactions(txid2)
    val start = System.nanoTime()

    tx2.checkCollisionWait(start + 10 * Tx.CollisionWait.toNanos)
    prepareResponses(net, txid2) shouldBe empty
    // Reported as stuck while it waits, as a hung backend would leave it
    tx2.reportIfStuck(start + Tx.StuckReportAfter.toNanos + 1000000000L) shouldBe true

    backend.deliverOne()
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("prepares that arrive during a wait on an in-flight commit are answered when it ends") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    val txd2 = lockTxd(txid2, List(overwriteAfter(txid1)))
    val first = mkPrepare(txd2, fromClient = true).proposalId
    val retry = ProposalId(first.number + 1, first.peer)
    sendPrepare(frontend, txd2)
    sendPrepare(frontend, txd2, Some(retry))
    prepareResponses(net, txid2) shouldBe empty

    backend.deliverOne()
    val r = prepareResponses(net, txid2)
    r.map(_.proposalId) should contain (retry)
    r.map(_.disposition).toSet shouldBe Set(TransactionDisposition.VoteCommit)
  }

  test("a SharedRevisionLock holder does not wait for another holder's in-flight commit") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0), overwrite(op1)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1

    prepare(frontend, net, lockTxd(txid2, List(SharedRevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  test("a resolved commit waits for the in-flight commit it follows") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    prepareResponses(net, txid2) shouldBe empty

    // Committed by the other stores' votes while this store withholds its vote
    resolve(frontend, net, txid2, committed = true)
    backend.heldCommits shouldBe 1
    backend.issued shouldBe List(txid1)

    backend.deliverOne()
    backend.issued shouldBe List(txid1, txid2)
    backend.deliverOne()
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
    committedMessages(net).filter(_.transactionId == txid2).map(_.objectCommitErrors) shouldBe List(Nil)
  }

  test("a transaction resolved before its objects load still commits once the in-flight commit it follows persists") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    // The fence's read completes only after the resolution, as when TxResolved queues ahead of a read completion
    backend.deferReads = true
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1), overwrite(fencePtr))))
    resolve(frontend, net, txid2, committed = true)
    backend.deliverReads()
    backend.issued shouldBe List(txid1)

    backend.deliverOne()
    backend.issued.take(2) shouldBe List(txid1, txid2)
    while backend.heldCommits > 0 do backend.deliverOne()
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
  }

  test("a held commit finalized before it is applied is still applied, and its CRL entry kept until it persists") {
    val crl = new TestCrl
    val (frontend, backend, net) = heldCommit(crl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = true)
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false, owed = Set()))
    frontend.transactions.contains(txid2) shouldBe false
    crl.deleted should not contain txid2

    backend.deliverOne()
    backend.issued shouldBe List(txid1, txid2)
    crl.deleted should not contain txid2
    backend.deliverOne()
    crl.deleted should contain (txid2)
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
  }

  test("a waiting transaction that resolves is no longer called when the commit persists") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = false)
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = false, late = false, owed = Set()))

    backend.deliverOne()
    prepareResponses(net, txid2) shouldBe empty
    backend.issued shouldBe List(txid1)
  }

  test("chained commits persist in commit order") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    val base = Array.tabulate[Byte](16)(_.toByte)
    sendData(frontend, lockTxd(txid1, List(overwrite(op1))), DataBuffer(base.clone()))
    responseFor(net, txid1).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)

    // Each built on the previous one, and committed by the other stores' votes
    val d2 = AppendDelta(16, 16, List(XorRegion(4, Array[Byte](1, 2, 3))), None)
    val d3 = AppendDelta(16, 16, List(XorRegion(10, Array[Byte](7, 7))), None)
    sendData(frontend, lockTxd(txid2, List(DataUpdate(op1, ObjectRevision(txid1), DataUpdateOperation.WriteRange))),
      d2.encode)
    sendData(frontend, lockTxd(txid3, List(DataUpdate(op1, ObjectRevision(txid2), DataUpdateOperation.WriteRange))),
      d3.encode)
    resolve(frontend, net, txid2, committed = true)
    resolve(frontend, net, txid3, committed = true)

    while backend.heldCommits > 0 do backend.deliverOne()

    backend.issued shouldBe List(txid1, txid2, txid3)
    backend.maxHeld shouldBe 1
    val expected = base.clone()
    expected(4) = (expected(4) ^ 1).toByte
    expected(5) = (expected(5) ^ 2).toByte
    expected(6) = (expected(6) ^ 3).toByte
    expected(10) = (expected(10) ^ 7).toByte
    expected(11) = (expected(11) ^ 7).toByte
    backend.get(oid1).get.data.getByteArray.toList shouldBe expected.toList
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid3)
  }

  test("waiters on a failed backend commit vote abort") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    prepareResponses(net, txid2) shouldBe empty

    backend.deliverOne(Right(CommitError.IOFailure))
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)

    sendPrepare(frontend, lockTxd(txid3, List(overwriteAfter(txid1))))
    prepareResponses(net, txid3).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
  }

  test("a held commit whose predecessor's backend commit fails commits nothing for that object") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = true)

    backend.deliverOne(Right(CommitError.IOFailure))
    backend.issued shouldBe List(txid1)
    committedMessages(net).filter(_.transactionId == txid2).map(_.objectCommitErrors) shouldBe List(List(oid1))
  }

  // ---- Backpressure: prepares for unseen transactions are dropped while the device CRL is filling ----

  /** A Store over a DeferredCommitBackend whose device admits through `admission`. Its transactions are led by
    * another store, so it starts no driver
    */
  def admissionStore(admission: CrlAdmission, crl: TestCrl = new TestCrl): (Store, DeferredCommitBackend, TestNet) =
    val backend = new DeferredCommitBackend
    populate(backend)
    val net = new TestNet
    val store = new Store(StorageDeviceId(new UUID(0, 99)), scala.concurrent.ExecutionContext.parasitic, backend,
      new HostObjectCache(1024 * 1024).forStore(storeId), net, BackgroundTaskManager.NoBackgroundTaskManager, crl,
      new TransactionStatusCache(), null, TransactionDriver.noErrorRecoveryFactory, Duration(60, SECONDS), None,
      admission)
    backend.setCompletionHandler(op => store.frontend.backendOperationComplete(op))
    (store, backend, net)

  def ledTxd(txid: TransactionId, reqs: List[TransactionRequirement]): TransactionDescription =
    lockTxd(txid, reqs).copy(designatedLeaderUID = 1.toByte)

  /** A client prepare carrying data for every DataUpdate */
  def dataPrepare(txd: TransactionDescription): TxPrepare =
    val updates = txd.requirements.collect { case r: DataUpdate => ObjectUpdate(r.objectPointer.id, DataBuffer(Array[Byte](1))) }
    mkPrepare(txd, fromClient = true).copy(objectUpdates = updates)

  /** The bytes a prepare is admitted for */
  def admissionBytes(m: TxPrepare): Long = CrlAdmission.TxOverheadBytes + m.txd.serialize().size +
    m.objectUpdates.map(_.data.size + CrlAdmission.ObjectUpdateOverheadBytes).sum

  test("a new prepare is dropped past the mark") {
    val admission = new CrlAdmission(100)
    admission.tryAdmit(100) shouldBe true
    val crl = new TestCrl
    val (store, _, net) = admissionStore(admission, crl)

    store.receiveTransactionMessage(dataPrepare(ledTxd(txid1, List(overwrite(op1)))))

    store.frontend.transactions shouldBe empty
    store.hasTransactions shouldBe false
    net.sentTx shouldBe empty
    net.cr shouldBe None
    crl.txSaved shouldBe false
    admission.admittedBytes shouldBe 100
  }

  test("a stub write is not subject to CrlAdmission backpressure") {
    val admission = new CrlAdmission(100)
    admission.tryAdmit(100) shouldBe true
    admission.full shouldBe true
    val crl = new TestCrl
    val (store, _, _) = admissionStore(admission, crl)

    // Written past the mark, and charged without a check
    var durable = false
    store.frontend.recordAppliedStub(txid1) { durable = true }
    durable shouldBe true
    crl.stubsSaved shouldBe List(txid1)
    store.frontend.hasAppliedStub(txid1) shouldBe true
    admission.admittedBytes shouldBe 100 + CrlAdmission.AppliedStubBytes
    // Saved again, it is charged once
    store.frontend.recordAppliedStub(txid1)(())
    crl.stubsSaved shouldBe List(txid1, txid1)
    admission.admittedBytes shouldBe 100 + CrlAdmission.AppliedStubBytes
  }

  test("stubs count toward the device's admission until they leave the CRL") {
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new TestCrl
    val (store, _, _) = admissionStore(admission, crl)
    store.frontend.recordAppliedStub(txid1)(())
    store.frontend.recordAppliedStub(txid2)(())
    admission.admittedBytes shouldBe 2 * CrlAdmission.AppliedStubBytes

    store.frontend.dropAppliedStub(txid2)
    admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes
    // Only a stub held is released
    store.frontend.dropAppliedStub(txid2)
    admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes

    // Closed, its stubs stay counted, retained for it until its entries leave the CRL (extraction, dropStore)
    store.frontend.close()
    admission.retainedBytes(storeId) shouldBe CrlAdmission.AppliedStubBytes
    admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes
    admission.releaseRetained(storeId)
    admission.admittedBytes shouldBe 0

    // Recovered stubs are counted again, without a check
    val full = new CrlAdmission(1)
    full.tryAdmit(1) shouldBe true
    val backend = new MapBackend(storeId)
    populate(backend)
    val recovering = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
      new TestCrl, new TransactionStatusCache(), admission = full, deferRecovery = true)
    recovering.recover(StoreRecoveryState(Nil, Set(txid1, txid2)))
    full.admittedBytes shouldBe 1 + 2 * CrlAdmission.AppliedStubBytes
    recovering.releaseAllAdmissions()
    full.admittedBytes shouldBe 1
  }

  test("a stub releases what its transaction was admitted for only once it is durable") {
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new TestCrl
    crl.holdStubSaves = true
    val (store, _, _) = admissionStore(admission, crl)
    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    // Admitted, as a forgotten transaction whose CRL state the stub replaces would be
    store.frontend.admitPrepare(p1) shouldBe true
    store.frontend.transactions.contains(txid1) shouldBe false
    admission.admittedBytes shouldBe admissionBytes(p1)

    store.frontend.recordAppliedStub(txid1)(())
    admission.admittedBytes shouldBe admissionBytes(p1) + CrlAdmission.AppliedStubBytes
    crl.heldStubSaves.foreach(_())
    admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes
  }

  test("an applied stub cannot replace a transaction whose backend commits are outstanding or failed") {
    val (frontend, backend, _) = heldCommit(new TestCrl)
    an[IllegalArgumentException] should be thrownBy frontend.recordAppliedStub(txid1)(())
    backend.deliverCommits(Right(CommitError.IOFailure))
    an[IllegalArgumentException] should be thrownBy frontend.recordAppliedStub(txid1)(())
    frontend.hasAppliedStub(txid1) shouldBe false
  }

  test("a known transaction's retransmitted prepare is answered past the mark") {
    // Below the limit until tx1 is admitted, which crosses it
    val admission = new CrlAdmission(1)
    val crl = new TestCrl
    val (store, backend, net) = admissionStore(admission, crl)
    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))

    store.receiveTransactionMessage(p1)
    prepareResponses(net, txid1).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
    admission.admittedBytes shouldBe admissionBytes(p1)

    // A new transaction is dropped
    store.receiveTransactionMessage(dataPrepare(ledTxd(txid2, List(overwrite(fencePtr)))))
    store.frontend.transactions.contains(txid2) shouldBe false
    prepareResponses(net, txid2) shouldBe empty

    store.receiveTransactionMessage(p1)
    prepareResponses(net, txid1).size shouldBe 2

    store.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    backend.deliverCommits()
    committedMessages(net).map(_.transactionId) shouldBe List(txid1)
    store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set()))
    store.frontend.transactions.contains(txid1) shouldBe false
    crl.deleted shouldBe List(txid1)
    admission.admittedBytes shouldBe 0
  }

  def checkReleasedAtFinalization(committed: Boolean, finalizeBeforePersisted: Boolean): Unit =
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new TestCrl
    val (store, backend, net) = admissionStore(admission, crl)
    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    store.receiveTransactionMessage(p1)
    admission.admittedBytes shouldBe admissionBytes(p1)

    store.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed, late = false, proof = false))
    if finalizeBeforePersisted then
      store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed, late = false, owed = Set()))
      // A committed transaction's CRL entry, and so its admission, outlives the Tx until its commit persists
      admission.admittedBytes shouldBe (if committed then admissionBytes(p1) else 0L)
      backend.deliverCommits()
    else
      backend.deliverCommits()
      store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed, late = false, owed = Set()))
    store.frontend.transactions.contains(txid1) shouldBe false
    crl.deleted should contain (txid1)
    admission.admittedBytes shouldBe 0

  test("a finalized transaction releases its admission: commit") {
    checkReleasedAtFinalization(committed = true, finalizeBeforePersisted = false)
  }

  test("a finalized transaction releases its admission: commit finalized before it persists") {
    checkReleasedAtFinalization(committed = true, finalizeBeforePersisted = true)
  }

  test("a finalized transaction releases its admission: abort") {
    checkReleasedAtFinalization(committed = false, finalizeBeforePersisted = false)
  }

  test("object data arriving after a data-less prepare is charged once, and returned at finalization") {
    // Past the mark once tx1 is admitted: the data is charged anyway, since tx1 is known
    val admission = new CrlAdmission(1)
    val (store, backend, net) = admissionStore(admission)
    val withData = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    val dataLess = withData.copy(objectUpdates = Nil, fromClient = false)

    store.receiveTransactionMessage(dataLess)
    admission.admittedBytes shouldBe admissionBytes(dataLess)
    store.receiveTransactionMessage(withData)
    admission.admittedBytes shouldBe admissionBytes(withData)
    // A retransmission is not charged again
    store.receiveTransactionMessage(withData)
    admission.admittedBytes shouldBe admissionBytes(withData)
    prepareResponses(net, txid1) should not be empty

    store.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    backend.deliverCommits()
    store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set()))
    admission.admittedBytes shouldBe 0
  }

  test("a transaction another store on the host has seen is still counted here") {
    val admission = new CrlAdmission(Long.MaxValue)
    val statusCache = new TransactionStatusCache()
    // Another store sharing the host's status cache has started the transaction
    statusCache.updateStatus(txid1, TransactionStatus.Unresolved)
    val backend = new DeferredCommitBackend
    populate(backend)
    val net = new TestNet
    val store = new Store(StorageDeviceId(new UUID(0, 99)), scala.concurrent.ExecutionContext.parasitic, backend,
      new HostObjectCache(1024 * 1024).forStore(storeId), net, BackgroundTaskManager.NoBackgroundTaskManager,
      new TestCrl, statusCache, null, TransactionDriver.noErrorRecoveryFactory, Duration(60, SECONDS), None, admission)
    backend.setCompletionHandler(op => store.frontend.backendOperationComplete(op))
    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))

    store.receiveTransactionMessage(p1)
    admission.admittedBytes shouldBe admissionBytes(p1)
  }

  test("a transaction waiting on an in-flight commit keeps its admission") {
    val admission = new CrlAdmission(Long.MaxValue)
    val (store, backend, net) = admissionStore(admission)
    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    store.receiveTransactionMessage(p1)
    store.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set()))
    backend.heldCommits shouldBe 1

    val p2 = dataPrepare(ledTxd(txid2, List(overwriteAfter(txid1))))
    store.receiveTransactionMessage(p2)
    prepareResponses(net, txid2) shouldBe empty
    admission.admittedBytes shouldBe admissionBytes(p1) + admissionBytes(p2)

    // tx1's commit persists: its entry goes, and tx2 votes, still admitted
    backend.deliverOne()
    prepareResponses(net, txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
    admission.admittedBytes shouldBe admissionBytes(p2)
  }

  test("a prepare for a transaction already finalized here is admitted without counting, and answered") {
    val admission = new CrlAdmission(100)
    admission.tryAdmit(100) shouldBe true
    val statusCache = new TransactionStatusCache()
    statusCache.updateStatus(txid1, TransactionStatus.Committed, finalized = true)
    val backend = new DeferredCommitBackend
    populate(backend)
    val net = new TestNet
    val store = new Store(StorageDeviceId(new UUID(0, 99)), scala.concurrent.ExecutionContext.parasitic, backend,
      new HostObjectCache(1024 * 1024).forStore(storeId), net, BackgroundTaskManager.NoBackgroundTaskManager,
      new TestCrl, statusCache, null, TransactionDriver.noErrorRecoveryFactory, Duration(60, SECONDS), None, admission)

    store.receiveTransactionMessage(dataPrepare(ledTxd(txid1, List(overwrite(op1)))))
    net.sentTx.collect { case f: TxFinalized => f.transactionId } shouldBe List(txid1)
    admission.admittedBytes shouldBe 100
  }

  test("closing the store keeps what its transactions were admitted for counted until they leave the CRL") {
    val admission = new CrlAdmission(Long.MaxValue)
    val (store, _, _) = admissionStore(admission)
    store.receiveTransactionMessage(dataPrepare(ledTxd(txid1, List(overwrite(op1)))))
    val bytes = admission.admittedBytes
    bytes should be > 0L
    Await.result(store.close(), Duration(5, SECONDS))
    // Its entries stay live in the CRL
    admission.admittedBytes shouldBe bytes
    admission.retainedBytes(storeId) shouldBe bytes
    admission.releaseRetained(storeId)
    admission.admittedBytes shouldBe 0
    admission.retainedBytes(storeId) shouldBe 0
  }

  /** A Frontend over `admission` recovering txid1 (a commit vote overwriting oid1) from its CRL, its object cached
    * so it loads while the Frontend is built. Returns the Frontend, its CRL and the recovered state's bytes
    */
  def recoveredAdmission(admission: CrlAdmission, lookup: ManualLookup = new ManualLookup): (Frontend, TestCrl, Long) =
    val backend = new MapBackend(storeId)
    populate(backend)
    val trs = TransactionRecoveryState(storeId, lockTxd(txid1, List(overwrite(op1))).serialize(),
      List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1, 2, 3)))), TransactionDisposition.VoteCommit,
      TransactionStatus.Unresolved, PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(mkObjectState(oid1))
    val frontend = new Frontend(storeId, backend, cache, new TestNet, crl, new TransactionStatusCache(),
      admission = admission, unackedLookup = lookup)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, crl, CrlAdmission.TxOverheadBytes + trs.serializedTxd.size + CrlAdmission.objectUpdateBytes(3))

  test("recovered transactions are counted for their bytes, and released when they finalize") {
    val admission = new CrlAdmission(Long.MaxValue)
    val lookup = new ManualLookup
    val (frontend, crl, bytes) = recoveredAdmission(admission, lookup)
    frontend.transactions.contains(txid1) shouldBe true
    admission.admittedBytes shouldBe bytes

    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false, late = false, proof = false))
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set()))
    // Neither the recovery's UnackedCommits lookup nor the one started after the aborts finds an entry
    lookup.answer(txid1, Success(false))
    lookup.answer(txid1, Success(false))
    crl.deleted should contain (txid1)
    admission.admittedBytes shouldBe 0
  }

  test("closing a store retains what its recovered transactions are counted for") {
    val admission = new CrlAdmission(Long.MaxValue)
    val (frontend, _, bytes) = recoveredAdmission(admission)
    admission.admittedBytes shouldBe bytes
    Await.result(frontend.close(), Duration(5, SECONDS))
    admission.retainedBytes(storeId) shouldBe bytes
    admission.releaseRetained(storeId)
    admission.admittedBytes shouldBe 0
  }

  test("a prepare admitted while its saved state loads is not counted again when that state is recovered") {
    val admission = new CrlAdmission(Long.MaxValue)
    val backend = new MapBackend(storeId)
    populate(backend)
    var recoverable: List[TransactionRecoveryState] = Nil
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = recoverable
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(mkObjectState(oid1))
    val frontend = new Frontend(storeId, backend, cache, new TestNet, crl, new TransactionStatusCache(),
      admission = admission)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    frontend.awaitRecovery()

    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    frontend.admitPrepare(p1) shouldBe true
    frontend.receiveTransactionMessage(p1)
    recoverable = List(TransactionRecoveryState(storeId, p1.txd.serialize(), p1.objectUpdates,
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial))
    frontend.recover(StoreRecoveryState(recoverable))
    frontend.transactions.contains(txid1) shouldBe true
    admission.admittedBytes shouldBe admissionBytes(p1)
  }
}
