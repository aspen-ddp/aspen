package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, ReadResponse, TxCommitted, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, FullObject, Key, KeySet, KeyValueObjectPointer, Metadata, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.paxos.PersistentState
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, ObjectUpdate, RefcountUpdate, SharedRevisionLock, TransactionCollision, TransactionDescription, TransactionDisposition, TransactionId, TransactionRequirement, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, CompletionHandler, MapBackend}
import org.aspen_ddp.aspen.server.store.cache.{HostObjectCache, ObjectCache}
import org.aspen_ddp.aspen.server.transaction.TransactionStatusCache
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

    override def sendClientResponse(msg: ClientResponse): Unit = cr = Some(msg)

    override def sendTransactionMessage(msg: TxMessage): Unit = tx = Some(msg)

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

    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = Nil

    override def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]] =
      Future.successful(Nil)

    override def save(txid: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit =
      txSaved = true
      completionHandler()

    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = aDrop = true

    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = txDel = true
  }

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

  test("a backend commit error evicts the state the commit cached") {
    for err <- List(CommitError.InvalidPointer, CommitError.ObjectMismatch, CommitError.SliceTooLarge,
                    CommitError.IOFailure) do
      var handler: Option[CompletionHandler] = None
      val backend = new MapBackend(storeId):
        override def setCompletionHandler(h: CompletionHandler): Unit =
          handler = Some(h)
          super.setCompletionHandler(h)
        override def commit(state: CommitState, transactionId: TransactionId): Unit =
          handler.foreach(_.complete(Commit(storeId, state.objectId, transactionId, Right(err))))
      val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
      val frontend = new Frontend(storeId, backend, cache, new TestNet, new TestCrl, new TransactionStatusCache())
      backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

      val os = mkObjectState(oid1)
      backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)

      val txd = mktxd().copy(requirements = List(DataUpdate(op1, rev0, DataUpdateOperation.Overwrite)))
      val prepare = mkPrepare(txd, fromClient = true).copy(objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))))
      frontend.receiveTransactionMessage(prepare)
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true))

      withClue(s"$err: ")(cache.get(oid1) shouldBe None)
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

    // Once the holder resolves, the discarded state is reloaded from the backend
    resolve(frontend, net, txid1, committed = false)
    prepare(frontend, net, lockTxd(TransactionId(new UUID(0, 13)), List(overwrite(fencePtr)))).disposition shouldBe
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
}
