package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.server.host.JoinCeiling
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import org.aspen_ddp.aspen.common.ida.{AppendDelta, Replication, XorRegion}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, HeldVote, ReadResponse, TransactionVote, TxAccept, TxAcceptResponse, TxCommitted, TxFinalized, TxHeartbeat, TxMessage, TxPrepare, TxPrepareResponse, TxResolved, TxStatusRequest, TxStatusResponse}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, FullObject, Insert, Key, KeySet, KeyValueOperation, KeyValueObjectPointer, Metadata, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, Value}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.paxos.PersistentState
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, ObjectUpdate, RefcountUpdate, ExclusiveRevisionLock, SharedRevisionLock, VersionBump, TransactionCollision, TransactionDescription, TransactionDisposition, TransactionId, TransactionObjectRequirement, TransactionRequirement, TransactionStatus}
import org.aspen_ddp.aspen.TempDirManager
import org.aspen_ddp.aspen.server.crl.{AdmissionTier, CrashRecoveryLog, CrlAdmission, StoreRecoveryState,
  TransactionRecoveryState}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, LogRateLimit}
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
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
  /** Loaded long before any test transaction starts: the default (construction time) would refuse a transaction
    * built in the same millisecond before the Frontend. Tests of the load floor pass their own */
  val longLoaded: Option[Long] = Some(0L)
  val rev0 = new ObjectRevision(new UUID(0, 7))
  val ida = Replication(3, 2)
  val op1 = DataObjectPointer(oid1, poolId, Array[Byte]())
  val otherPoolId = PoolId(new UUID(0,8))
  val otherPtr = DataObjectPointer(ObjectId(new UUID(0,9)), otherPoolId, Array[Byte]())

  class TestNet extends Messenger {

    var cr: Option[ClientResponse] = None
    /** Every client response sent, oldest first */
    val clientResponses = scala.collection.mutable.ListBuffer[ClientResponse]()
    var tx: Option[TxMessage] = None
    var sentTx: List[TxMessage] = Nil

    override def sendClientResponse(msg: ClientResponse): Unit =
      cr = Some(msg)
      clientResponses += msg

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
    private var answers: Map[(TransactionId, Long), Try[UnackedLookupResult] => Unit] = Map()
    def requests: List[TransactionId] = made.map(_._1)
    def lookup(storeId: StoreId, transactionId: TransactionId, requestId: Long,
               answer: Try[UnackedLookupResult] => Unit): Unit =
      made = made :+ (transactionId -> requestId)
      answers += (transactionId, requestId) -> answer
    /** Answers the lookup `requestId` of `txid`, by default the latest made */
    def answer(txid: TransactionId, result: Try[UnackedLookupResult], requestId: Option[Long] = None): Unit =
      val id = requestId.getOrElse(made.filter(_._1 == txid).last._2)
      answers((txid, id))(result)
    /** An entry naming the incarnation of `frontend`, the store that made the lookup */
    def own: Try[UnackedLookupResult] = Success(UnackedLookupResult.Found(Some(frontend.get.incarnation)))
    /** No entry */
    def absent: Try[UnackedLookupResult] = Success(UnackedLookupResult.Absent)

  /** Records quarantine verifications and answers them when the test says */
  class ManualVerifier extends QuarantineVerifier:
    var made: List[(ObjectId, HLCTimestamp)] = Nil
    private var answers: List[Try[Boolean] => Unit] = Nil
    def verify(storeId: StoreId, pointer: ObjectPointer, minTimestamp: HLCTimestamp,
               answer: Try[Boolean] => Unit): Unit =
      made = made :+ (pointer.id -> minTimestamp)
      answers = answers :+ answer
    /** Answers the latest verification */
    def answer(result: Try[Boolean]): Unit = answers.last(result)

  class TestCrl extends CrashRecoveryLog {

    var txSaved = false
    var txDel = false
    /** How many times admission asked the CRL to grow */
    var growthRequests = 0

    override def requestGrowth(): Unit = growthRequests += 1
    var aDrop = false
    var deleted: List[TransactionId] = Nil
    var dropped: List[TransactionId] = Nil

    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = Nil

    /** Every transaction state saved, in order */
    var savedStates: List[TransactionRecoveryState] = Nil
    /** While set, transaction saves complete only when the test runs the handlers held here */
    var holdTxSaves = false
    var heldTxSaves: List[() => Unit] = Nil

    def completeHeldTxSaves(): Unit =
      val held = heldTxSaves
      heldTxSaves = Nil
      held.foreach(_())

    override def save(txid: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit =
      txSaved = true
      savedStates = savedStates :+ state
      if holdTxSaves then heldTxSaves = heldTxSaves :+ completionHandler else completionHandler()

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

    override def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = Map()

    override def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
      stubsSaved = stubsSaved :+ txid
      if holdStubSaves then heldStubSaves = heldStubSaves :+ completionHandler else completionHandler()

    override def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit =
      stubsDeleted = stubsDeleted :+ txid

    /** While set, durability barriers complete only when the test runs the handlers held here */
    var holdBarriers = false
    var heldBarriers: List[() => Unit] = Nil

    override def whenDurable(storeId: StoreId, handler: () => Unit): Unit =
      if holdBarriers then heldBarriers = heldBarriers :+ handler else handler()

    def completeHeldBarriers(): Unit =
      val held = heldBarriers
      heldBarriers = Nil
      held.foreach(_())
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

    /** With `deferRepairDeletes`, repair deletions are held until deliverRepairDeletes */
    var deferRepairDeletes: Boolean = false
    private var heldRepairDeletes: List[(ObjectId, Array[Byte], Promise[Unit])] = Nil

    override def repairDelete(objectId: ObjectId, storePointer: Array[Byte], complete: Promise[Unit]): Unit =
      if deferRepairDeletes then heldRepairDeletes = heldRepairDeletes :+ (objectId, storePointer, complete)
      else super.repairDelete(objectId, storePointer, complete)

    def deliverRepairDeletes(): Unit =
      val held = heldRepairDeletes
      heldRepairDeletes = Nil
      held.foreach((oid, sp, complete) => super.repairDelete(oid, sp, complete))

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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    (frontend, backend, cache)

  /** Like mkFrontend but also returns the TestNet and routes backend completions to the Frontend */
  def mkFrontendWithNet(): (Frontend, MapBackend, ObjectCache, TestNet) =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      ProposalId.initialProposal(txd.designatedLeaderUID), Nil, fromClient)

  /** Started now on the wall clock: after the Frontends built before it loaded, inside the forget floor. Marked
    * replaySafe: many tests here are lock-only or allocate without a pin, which a client would mark so. The pin rule
    * is tested with replaySafe = false (DescriptionValidator tests below)
    */
  def mktxd(): TransactionDescription =
    TransactionDescription(txid1, HLCTimestamp(System.currentTimeMillis() << 16), op1, 0, Nil, Nil, Some(clientId), Nil, Nil, ida,
      Map(poolId -> ida), replaySafe = true)

  def mkObjectState(objectId: ObjectId): ObjectState =
    new ObjectState(objectId,
      Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)))
}

class FrontendSuite extends AnyFunSuite with Matchers {
  import FrontendSuite._

  /** The start a stub written directly here carries: now, so no heartbeat of a test expires it (expireStubs) */
  private def stubStart: HLCTimestamp = HLCTimestamp.now

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

  // Paxos F4: a secondary-pool store's driver is learn-only. Its retransmits must not keep a transaction whose
  // designated leader is offline from looking idle at the primary stores, or none would take it over
  test("prepares and heartbeats from a learn-only driver do not reset a transaction's idle time") {
    val (frontend, _, _) = mkFrontend()
    val txd = mktxd()
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true))
    val tx = frontend.transactions(txid1)
    val firstEvent = tx.lastEventTime
    val learnOnly = StoreId(PoolId(new UUID(0, 77)), txd.designatedLeaderUID)

    Thread.sleep(5)
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false).copy(from = learnOnly))
    frontend.receiveTransactionMessage(TxHeartbeat(storeId, learnOnly, txid1))
    tx.lastEventTime shouldBe firstEvent

    frontend.receiveTransactionMessage(TxHeartbeat(storeId, StoreId(poolId, 1), txid1))
    tx.lastEventTime should be > firstEvent
  }

  // Paxos F11
  test("an outcome opposite to the one a Tx holds is counted as a split decision, and the Tx keeps its own") {
    val backend = new MapBackend(storeId)
    val statusCache = new TransactionStatusCache()
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
      new TestCrl, statusCache, loadedAt = FrontendSuite.longLoaded)
    val leader = StoreId(poolId, 1)
    frontend.receiveTransactionMessage(mkPrepare(mktxd(), fromClient = true))
    val tx = frontend.transactions(txid1)

    frontend.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = true, late = false, proof = false))
    statusCache.splitDecisions shouldBe 0
    frontend.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = true, late = false, proof = false))
    statusCache.splitDecisions shouldBe 0

    frontend.receiveTransactionMessage(TxResolved(storeId, leader, txid1, committed = false, late = false, proof = false))
    statusCache.splitDecisions shouldBe 1
    tx.knownOutcome shouldBe Some(true)
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
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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

  private def withDataless(missingDataWait: Duration = Frontend.MissingDataWait,
                           incarnation: Option[StoreIncarnation] = None)
                          (f: (Frontend, MapBackend, TestNet, TestCrl) => Unit): Unit =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val crl = new TestCrl
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), None, missingDataWait, loadedAt = FrontendSuite.longLoaded,
      incarnationOverride = incarnation)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    val os = mkObjectState(oid1)
    backend.bootstrapAllocate(oid1, os.objectType, os.metadata, os.data)
    f(frontend, backend, net, crl)

  private def clientPrepare(txd: TransactionDescription) = mkPrepare(txd, fromClient = true).copy(
    objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](7)))))

  private def expectVote(net: TestNet, vote: TransactionDisposition.Value): Unit = net.txMessage() match
    case Some(r: TxPrepareResponse) => r.disposition shouldBe vote
    case other => throw new AssertionError(s"Expected a TxPrepareResponse, got $other")

  test("a prepare reply carries the store's incarnation") {
    withDataless(incarnation = Some(StoreIncarnation(new UUID(7, 7)))): (frontend, backend, net, crl) =>
      frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
      votes(net).map(_.incarnation) shouldBe List(Some(StoreIncarnation(new UUID(7, 7))))
  }

  // ---- The durable vote sent to the originating client (client 1-RTT commit design 4.1) ----

  private def txVotes(net: TestNet): List[TransactionVote] =
    net.clientResponses.toList.collect { case v: TransactionVote => v }

  private def voteTxd(txid: TransactionId = txid1) =
    lockTxd(txid, List(overwrite(op1))).copy(startTimestamp = startedAgo(1_000))

  test("a client prepare's durable vote is sent to the originating client") {
    val crl = new TestCrl
    crl.holdTxSaves = true
    withFloorFrontend(crl = crl): (frontend, _, net, _) =>
      frontend.receiveTransactionMessage(clientPrepare(voteTxd()))
      crl.heldTxSaves.size shouldBe 1
      net.sentTx shouldBe empty
      txVotes(net) shouldBe Nil
      crl.completeHeldTxSaves()
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      txVotes(net) shouldBe List(TransactionVote(clientId, storeId, txid1, TransactionDisposition.VoteCommit))
  }

  test("a retransmitted prepare re-sends the vote") {
    val crl = new TestCrl
    crl.holdTxSaves = true
    withFloorFrontend(crl = crl): (frontend, _, net, _) =>
      frontend.receiveTransactionMessage(clientPrepare(voteTxd()))
      crl.completeHeldTxSaves()
      frontend.receiveTransactionMessage(clientPrepare(voteTxd()))
      txVotes(net).size shouldBe 1
      crl.completeHeldTxSaves()
      txVotes(net) shouldBe List.fill(2)(TransactionVote(clientId, storeId, txid1, TransactionDisposition.VoteCommit))
  }

  test("an abort vote is sent too") {
    val txd = lockTxd(txid1, List(DataUpdate(op1, ObjectRevision(txid2), DataUpdateOperation.Overwrite)))
      .copy(startTimestamp = startedAgo(1_000))
    withFloorFrontend(): (frontend, _, net, _) =>
      frontend.receiveTransactionMessage(clientPrepare(txd))
      txVotes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
  }

  test("a frozen Tx sends no vote") {
    // Recovered with no vote, past the horizon plus the default margin by the store's clock
    val txd = voteTxd().copy(startTimestamp = startedAgo(40_000))
    val trs = TransactionRecoveryState(storeId, txd.serialize(), Nil, TransactionDisposition.Undetermined,
      TransactionStatus.Unresolved, PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    withFloorFrontend(loadedAt = floorNow, crl = crl): (frontend, _, net, _) =>
      frontend.transactions(txid1).frozen shouldBe true
      frontend.receiveTransactionMessage(driverPrepare(txd))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      txVotes(net) shouldBe Nil
      votes(net) shouldBe Nil
  }

  test("no originating client, no vote") {
    withFloorFrontend(): (frontend, _, net, _) =>
      frontend.receiveTransactionMessage(clientPrepare(voteTxd().copy(originatingClient = None)))
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      txVotes(net) shouldBe Nil
  }

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

  // paxos F14: adopted data must be durable in the CRL before the commit that needs it starts its backend write, or a
  // crash in between recovers the Tx without it (P13)
  private def savedWithData(crl: TestCrl): Boolean = crl.savedStates.exists(_.objectUpdates.exists(_.objectId == oid1))

  test("a commit held for data applies the late client data only once a save holding that data is durable") {
    withDataless(): (frontend, backend, net, crl) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      crl.holdTxSaves = true

      frontend.receiveTransactionMessage(clientPrepare(txd))
      savedWithData(crl) shouldBe true
      committedMessages(net) shouldBe Nil
      backend.get(oid1).get.metadata.revision shouldBe rev0

      crl.holdTxSaves = false
      crl.completeHeldTxSaves()
      committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
      backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
  }

  test("an abort voter given late client data applies a later commit only once that data is durable") {
    withDataless(missingDataWait = Duration.Zero): (frontend, backend, net, crl) =>
      val txd = dataUpdateTxd
      frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
      expectVote(net, TransactionDisposition.VoteAbort)
      crl.holdTxSaves = true

      frontend.receiveTransactionMessage(clientPrepare(txd))
      savedWithData(crl) shouldBe true
      frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      committedMessages(net) shouldBe Nil
      backend.get(oid1).get.metadata.revision shouldBe rev0

      crl.holdTxSaves = false
      crl.completeHeldTxSaves()
      committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
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

      frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
        startTimestamp = HLCTimestamp.now))
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

      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = false, late = false, owed = Set(),
        startTimestamp = HLCTimestamp.now))
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

      frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set(),
        startTimestamp = HLCTimestamp.now))
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
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      statusCache, loadedAt = FrontendSuite.longLoaded)
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

    frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net) shouldBe Nil
    crl.txDel shouldBe true
  }

  /** A Frontend recovering txid1 (dataUpdateTxd) undecided, with the given acceptor state, and the default wait for a
    * client's prepare. Its object is cached so the recovered transaction loads during construction
    */
  private def recoveredUndecided(acceptor: PersistentState): (Frontend, TestNet) =
    val backend = new MapBackend(storeId)
    backend.bootstrapAllocate(oid1, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(100)),
      DataBuffer(Array[Byte](1)))
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(backend.get(oid1).get)
    val trs = TransactionRecoveryState(storeId, dataUpdateTxd.serialize(), Nil, TransactionDisposition.Undetermined,
      TransactionStatus.Unresolved, acceptor)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(),
      loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, net)

  // paxos-transactions P-N7: the wait for the client's prepare is for live transactions only
  test("a recovered undecided Tx answers a driver's prepare at once, with an abort vote and what it accepted") {
    val accepted = (ProposalId(2, 2), true)
    val (frontend, net) = recoveredUndecided(PersistentState(Some(accepted._1), Some(accepted)))
    frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd).copy(proposalId = ProposalId(3, 2)))
    votes(net).map(v => (v.disposition, v.response)) shouldBe
      List((TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(Some(accepted)))))
  }

  test("a recovered undecided Tx with no Paxos state answers a driver's prepare at once with an abort vote") {
    val (frontend, net) = recoveredUndecided(PersistentState.initial)
    frontend.receiveTransactionMessage(driverPrepare(dataUpdateTxd))
    votes(net).map(v => (v.disposition, v.response)) shouldBe
      List((TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(None))))
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
    // No vote and no client prepare, but recovered: it does not wait for the client's prepare (P-N7)
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      statusCache, loadedAt = FrontendSuite.longLoaded)
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
      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = false, owed = Set(),
        startTimestamp = HLCTimestamp.now))

      committedMessages(net) shouldBe Nil
      backend.get(oid1).get.data.getByteArray shouldBe appliedData
      frontend.transactions.contains(txid1) shouldBe false
      crl.txDel shouldBe true
  }

  test("a late client prepare with data for an applied transaction does not apply the data again") {
    withApplied(): (frontend, backend, net, _) =>
      frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = true, owed = Set(),
        startTimestamp = HLCTimestamp.now))

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

      frontend.receiveTransactionMessage(TxFinalized(storeId, recoveryDriver, txid1, committed = true, late = false, owed = Set(),
        startTimestamp = HLCTimestamp.now))

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
      statusCache, loadedAt = FrontendSuite.longLoaded)
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

  // ---- A prepare for an unknown transaction that started a while ago ----
  //
  // Inside the forget floor there is no other age limit: replay safety comes from pins, not from refusing old
  // prepares. Such a prepare is checked afresh, and proven committed only on the abort path. Below the floor it is
  // refused (see "The forget floor" below).

  /** On the host's wall clock, as Frontend.wallClockMillis reads it */
  private def ago(d: FiniteDuration): HLCTimestamp = HLCTimestamp((System.currentTimeMillis() - d.toMillis) << 16)

  /** `loadedAt`: Frontend.loadedAt. Set back for transactions started before the test built the Frontend */
  private def withPlainFrontend(statusCache: TransactionStatusCache = new TransactionStatusCache(),
                                loadedAt: Option[Long] = None)
                         (f: (Frontend, MapBackend, TestNet) => Unit): Unit =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl, statusCache, loadedAt = loadedAt)
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
    // The proof runs only on the abort path, whatever the transaction's age inside the horizon
    withPlainFrontend(loadedAt = Some(0L)): (frontend, backend, net) =>
      appliedKVPrepare(frontend, backend, FiniteDuration(10, SECONDS))

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
    // Loaded MaxStartLead before its acceptor started, so the load's floor (the load plus MaxStartLead) is the
    // acceptor's start: older transactions meet the acceptor's mute, newer ones neither
    val since = ago(FiniteDuration(2, SECONDS))
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache,
      acceptorSince = Some(since), loadedAt = Some(since.wallTime - Frontend.MaxStartLead.toMillis))
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

  // ---- A rebuilt store's inherited votes (Frontend.inheritedVoteLock) ----
  //
  // Its old incarnation's commit votes may still count, with none of their locks held here, so it votes abort on what
  // they may lock until its peers show their transactions resolved.

  /** Records held-vote queries and answers them when the test says */
  class ManualHeldVoteQuery extends HeldVoteQuery:
    private var answers: List[Try[List[HeldVote]] => Unit] = Nil
    def made: Int = answers.size
    def query(storeId: StoreId, answer: Try[List[HeldVote]] => Unit): Unit = answers = answers :+ answer
    def answer(result: Try[List[HeldVote]]): Unit = answers.last(result)

  private val inheritedTxid = TransactionId(new UUID(0, 99))

  /** A rebuilt store (acceptor started 2 s ago) holding its inherited-votes file, gate at `after` */
  private def withInheritedVotes(after: Long = 0L)(f: (Frontend, ManualHeldVoteQuery, os.Path) => Unit): Unit =
    withInheritedVotesClock(after)((frontend, query, dir, _) => f(frontend, query, dir))

  private def withInheritedVotesClock(after: Long)
                                     (f: (Frontend, ManualHeldVoteQuery, os.Path, TestClock) => Unit): Unit =
    val clock = new TestClock(System.currentTimeMillis())
    val dir = os.temp.dir(deleteOnExit = true)
    RebuildingStore.writeInheritedVotes(dir, after)
    val backend = new MapBackend(storeId):
      override def path: os.Path = dir
    val query = new ManualHeldVoteQuery
    val since = ago(FiniteDuration(2, SECONDS))
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
      new TestCrl, new TransactionStatusCache(), acceptorSince = Some(since),
      inheritedVotesAfter = RebuildingStore.readInheritedVotes(dir, FiniteDuration(20, SECONDS)),
      heldVoteQuery = query, loadedAt = Some(since.wallTime - Frontend.MaxStartLead.toMillis),
      wallClockMillis = () => clock.now)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    f(frontend, query, dir, clock)

  /** This store's vote on a fresh transaction (started after the acceptor) with `reqs` */
  private def voteOn(frontend: Frontend, txid: TransactionId, reqs: List[TransactionRequirement])
      : TransactionDisposition.Value =
    val txd = lockTxd(txid, reqs).copy(startTimestamp = ago(FiniteDuration(1, SECONDS)))
    val updates = reqs.collect { case r: DataUpdate => ObjectUpdate(r.objectPointer.id, DataBuffer(Array[Byte](1))) }
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(objectUpdates = updates))
    frontend.transactions(txid).vote

  test("a rebuilt store votes abort on every object until a query of its peers' held votes past its gate completes") {
    withInheritedVotes(after = System.currentTimeMillis() + 60_000): (frontend, query, _) =>
      voteOn(frontend, txid1, List(overwrite(op1))) shouldBe TransactionDisposition.VoteAbort
      frontend.retryHeldVoteQuery()
      withClue("no query before the gate: ")(query.made shouldBe 0)
      frontend.inheritedVoteTransactions shouldBe None
  }

  test("a rebuilt store votes abort only on the objects of the transactions its inherited votes may still back") {
    withInheritedVotes(): (frontend, query, dir) =>
      frontend.retryHeldVoteQuery()
      query.made shouldBe 1
      // A transaction started after the acceptor never had the old incarnation's vote: not inherited
      query.answer(Success(List(HeldVote(inheritedTxid, ago(FiniteDuration(3, SECONDS)), Set(fenceOid)),
        HeldVote(txid2, ago(FiniteDuration(1, SECONDS)), Set(oid1)))))
      frontend.inheritedVoteTransactions shouldBe Some(Set(inheritedTxid))
      voteOn(frontend, txid1, List(overwrite(op1))) shouldBe TransactionDisposition.VoteCommit
      voteOn(frontend, TransactionId(new UUID(0, 98)), List(VersionBump(fencePtr, rev0))) shouldBe
        TransactionDisposition.VoteAbort
      os.exists(dir / RebuildingStore.InheritedVotesFile) shouldBe true
  }

  test("a rebuilt store releases an inherited vote once its peers no longer report its transaction") {
    withInheritedVotesClock(0L): (frontend, query, dir, clock) =>
      frontend.retryHeldVoteQuery()
      query.answer(Success(List(HeldVote(inheritedTxid, ago(FiniteDuration(3, SECONDS)), Set(fenceOid)))))
      withClue("no query again before the backoff: "):
        frontend.retryHeldVoteQuery()
        query.made shouldBe 1
      clock.now += Frontend.InheritedVoteQueryInitialInterval.toMillis
      frontend.retryHeldVoteQuery()
      query.made shouldBe 2
      query.answer(Success(Nil))
      frontend.inheritedVoteTransactions shouldBe Some(Set())
      voteOn(frontend, txid1, List(VersionBump(fencePtr, rev0))) shouldBe TransactionDisposition.VoteCommit
      withClue("the file is removed: ")(os.exists(dir / RebuildingStore.InheritedVotesFile) shouldBe false)
      frontend.retryHeldVoteQuery()
      withClue("no query once none is held: ")(query.made shouldBe 2)
  }

  test("a rebuilt store releases an inherited vote when its transaction's outcome reaches it") {
    withInheritedVotes(): (frontend, query, dir) =>
      frontend.retryHeldVoteQuery()
      query.answer(Success(List(HeldVote(inheritedTxid, ago(FiniteDuration(3, SECONDS)), Set(fenceOid)))))
      frontend.receiveTransactionMessage(TxFinalized(storeId, StoreId(poolId, 1), inheritedTxid, committed = false,
        late = false, owed = Set(), startTimestamp = ago(FiniteDuration(3, SECONDS))))
      frontend.inheritedVoteTransactions shouldBe Some(Set())
      os.exists(dir / RebuildingStore.InheritedVotesFile) shouldBe false
  }

  test("a failed query of a rebuilt store's peers keeps every object locked") {
    withInheritedVotes(): (frontend, query, _) =>
      frontend.retryHeldVoteQuery()
      query.answer(Failure(new Exception("no quorum")))
      frontend.inheritedVoteTransactions shouldBe None
      voteOn(frontend, txid1, List(overwrite(op1))) shouldBe TransactionDisposition.VoteAbort
  }

  test("a store reports the commit votes it holds unresolved, and not its abort votes or resolved ones") {
    val (frontend, _, net) = lockFixture()
    val start = ago(FiniteDuration(1, SECONDS))
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = start)).disposition shouldBe
      TransactionDisposition.VoteCommit
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1))).copy(startTimestamp = start)).disposition shouldBe
      TransactionDisposition.VoteAbort
    frontend.heldVotes() shouldBe List(HeldVote(txid1, start, Set(oid1)))
    resolve(frontend, net, txid1, committed = false)
    frontend.heldVotes() shouldBe Nil
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
    // Loaded long before T: below the floor a resolved entry no longer creates a Tx
    withPlainFrontend(statusCache, loadedAt = Some(0L)): (frontend, _, net) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(6500, MILLISECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      // Checked as usual, and the outcome known: no locks, the cached status kept
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      frontend.transactions(txid1).holdsLocks shouldBe false
      statusCache.getStatus(txid1).map(_.status) shouldBe Some(TransactionStatus.Committed)
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
  }

  test("an unknown prepare inside the horizon with no status cache entry is checked afresh") {
    withPlainFrontend(loadedAt = Some(0L)): (frontend, _, net) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ago(FiniteDuration(10, SECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      frontend.transactions(txid1).holdsLocks shouldBe true
  }

  // ---- A start timestamp ahead of the host's clock (Frontend.MaxStartLead) ----
  //
  // The start timestamp is the client's. A store takes part only in a transaction started no more than MaxStartLead
  // ahead of its host's wall clock, so a transaction an old incarnation joined started before its rebuilt store's
  // acceptor-since time, whatever the client's clock (paxos P-N2).

  private def ahead(d: FiniteDuration): HLCTimestamp = HLCTimestamp((System.currentTimeMillis() + d.toMillis) << 16)

  test("a rebuilt store gives no vote to a transaction its old incarnation may have joined, though a fast client " +
       "stamped it after the acceptor-since time") {
    // Fresh-rebuild shape: acceptor-since is the finish time plus the margin. The client runs 11 s fast
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val crl = new TestCrl
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(),
      acceptorSince = Some(ahead(FiniteDuration(10, SECONDS))), loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ahead(FiniteDuration(11, SECONDS)))
    frontend.receiveTransactionMessage(clientPrepare(txd))

    votes(net) shouldBe Nil
    crl.txSaved shouldBe false
    frontend.transactions.contains(txid1) shouldBe false
  }

  test("a store gives no promise or vote to a transaction started too far ahead of its clock, and saves nothing") {
    val crl = new TestCrl
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ahead(FiniteDuration(3600, SECONDS)))
    frontend.receiveTransactionMessage(clientPrepare(txd))
    frontend.receiveTransactionMessage(TxAccept(storeId, StoreId(poolId, txd.designatedLeaderUID), txid1,
      ProposalId.initialProposal(txd.designatedLeaderUID), true))

    votes(net) shouldBe Nil
    acceptResponses(net) shouldBe Nil
    crl.txSaved shouldBe false
    frontend.transactions.contains(txid1) shouldBe false
    lockedWriteTransactions(frontend, net, op1) shouldBe empty
  }

  test("a store votes on a transaction started ahead of its clock by less than MaxStartLead") {
    // Loaded long ago: a store loaded within MaxStartLead before the start refuses it (forgetFloorMillis)
    withPlainFrontend(loadedAt = FrontendSuite.longLoaded): (frontend, _, net) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = ahead(FiniteDuration(1, SECONDS)))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("a store votes on a retransmitted prepare once its clock catches up with the transaction's start") {
    var clock = System.currentTimeMillis()
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl, new TransactionStatusCache(),
      wallClockMillis = () => clock, loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp((clock + 60000) << 16))
    frontend.receiveTransactionMessage(clientPrepare(txd))
    votes(net) shouldBe Nil
    frontend.transactions.contains(txid1) shouldBe false

    clock += 59000
    frontend.receiveTransactionMessage(clientPrepare(txd))
    votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("a store still proves committed a transaction started too far ahead whose update it shows") {
    withPlainFrontend(): (frontend, backend, net) =>
      appliedKVPrepare(frontend, backend, FiniteDuration(-3600, SECONDS))

      resolvedMessages(net).map(r => (r.committed, r.proof)) shouldBe List((true, true))
      votes(net) shouldBe Nil
  }

  // ---- The forget floor (Frontend.forgetFloorMillis) ----
  //
  // A store refuses a prepare for a transaction it does not hold, and whose outcome the status cache does not know,
  // that started below its floor: the horizon before now, the store's load plus MaxStartLead, or the status cache's
  // eviction watermark.
  // No Tx, no promise, no vote, no reply. A fixed injected clock keeps these deterministic

  private val floorNow = 1_000_000_000_000L
  private def startedAgo(ms: Long): HLCTimestamp = HLCTimestamp((floorNow - ms) << 16)

  private def withFloorFrontend(loadedAt: Long = floorNow - 60_000,
                                arrivedAt: Option[Long] = None,
                                statusCache: TransactionStatusCache =
                                  new TransactionStatusCache(wallClockMillis = () => floorNow),
                                crl: TestCrl = new TestCrl,
                                missingDataWait: Duration = Frontend.MissingDataWait,
                                admission: CrlAdmission = CrlAdmission.Unlimited,
                                joinCeiling: JoinCeiling = JoinCeiling.Unbounded)
                               (f: (Frontend, MapBackend, TestNet, TestCrl) => Unit): Unit =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    populate(backend)
    // Cached so a recovered transaction's object loads during Frontend construction
    cache.insert(mkObjectState(oid1))
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache, missingDataWait = missingDataWait,
      admission = admission, wallClockMillis = () => floorNow, horizon = FiniteDuration(20, SECONDS),
      loadedAt = Some(loadedAt), arrivedAt = arrivedAt, joinCeiling = joinCeiling)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    f(frontend, backend, net, crl)

  /** Admits the prepare as Store does, then delivers it */
  private def admitAndReceive(frontend: Frontend, m: TxPrepare): Unit =
    frontend.admitPrepare(m) shouldBe PrepareAdmission.Admitted
    frontend.receiveTransactionMessage(m)

  test("a prepare for an unknown transaction older than the horizon creates no Tx, sends nothing and saves nothing") {
    val admission = new CrlAdmission(1024 * 1024)
    withFloorFrontend(admission = admission): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = startedAgo(21_000))
      frontend.belowForgetFloor(txd) shouldBe true
      admitAndReceive(frontend, clientPrepare(txd))
      admitAndReceive(frontend, driverPrepare(txd))
      frontend.receiveTransactionMessage(TxAccept(storeId, recoveryDriver, txid1,
        ProposalId.initialProposal(txd.designatedLeaderUID), true))

      frontend.transactions.contains(txid1) shouldBe false
      net.sentTx shouldBe empty
      crl.txSaved shouldBe false
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
      admission.admittedBytes shouldBe 0
  }

  test("a prepare for an unknown transaction started before the store loaded is refused") {
    withFloorFrontend(loadedAt = floorNow): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = startedAgo(1_000))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      frontend.transactions.contains(txid1) shouldBe false
      net.sentTx shouldBe empty
      crl.txSaved shouldBe false
  }

  // Before its load the store may have joined, and forgotten, a transaction started up to MaxStartLead ahead of its
  // clock: the load raises the floor to the load plus the lead
  private val lead: Long = Frontend.MaxStartLead.toMillis

  test("a prepare for an unknown transaction started inside MaxStartLead after the store loaded is refused") {
    val loaded = floorNow - 500
    for start <- List(loaded + 1, loaded + lead / 2, loaded + lead - 1) do
      withFloorFrontend(loadedAt = loaded): (frontend, _, net, crl) =>
        val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp(start << 16))
        frontend.forgetFloorMillis shouldBe loaded + lead
        frontend.receiveTransactionMessage(clientPrepare(txd))

        withClue(s"started ${start - loaded} ms after the load: "):
          frontend.transactions.contains(txid1) shouldBe false
          net.sentTx shouldBe empty
          crl.txSaved shouldBe false
  }

  test("a prepare for an unknown transaction started MaxStartLead or more after the store loaded is answered") {
    val loaded = floorNow - lead - 500
    for start <- List(loaded + lead, loaded + lead + 1) do
      withFloorFrontend(loadedAt = loaded): (frontend, _, net, crl) =>
        val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp(start << 16))
        frontend.receiveTransactionMessage(clientPrepare(txd))

        withClue(s"started ${start - loaded} ms after the load: "):
          votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
          crl.txSaved shouldBe true
  }

  // A store that arrived from another host (a transfer in, an adoption) may have joined there, and forgotten, a
  // transaction started up to MaxStartLead ahead of that host's clock, which may lead this one's: the load raises the
  // floor to the load plus AcceptorSinceMargin, which covers the lead and the skew between hosts (paxos P-N2)
  private val margin: Long = RebuildingStore.AcceptorSinceMargin.toMillis

  test("a store that arrived from another host refuses transactions started inside AcceptorSinceMargin after its load") {
    val loaded = floorNow - 500
    for start <- List(loaded + lead, loaded + margin - 1) do
      withFloorFrontend(loadedAt = loaded, arrivedAt = Some(loaded - 100)): (frontend, _, net, crl) =>
        val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp(start << 16))
        frontend.loadFloorMillis shouldBe loaded + margin
        frontend.receiveTransactionMessage(clientPrepare(txd))

        withClue(s"started ${start - loaded} ms after the load: "):
          frontend.transactions.contains(txid1) shouldBe false
          net.sentTx shouldBe empty
          crl.txSaved shouldBe false
  }

  test("a store that arrived from another host answers a transaction started AcceptorSinceMargin after its load") {
    val loaded = floorNow - margin - 500
    withFloorFrontend(loadedAt = loaded, arrivedAt = Some(loaded)): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp((loaded + margin) << 16))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("a restart on the same host within AcceptorSinceMargin of the store's arrival keeps the arrival's margin") {
    // The restarted store may still have forgotten a transaction it joined on the host it came from
    val loaded = floorNow - 500
    withFloorFrontend(loadedAt = loaded, arrivedAt = Some(loaded - margin + 1)): (frontend, _, _, _) =>
      frontend.loadFloorMillis shouldBe loaded + margin
  }

  test("a restart on the same host once AcceptorSinceMargin has passed since the store's arrival adds only the lead") {
    val loaded = floorNow - 500
    withFloorFrontend(loadedAt = loaded, arrivedAt = Some(loaded - margin)): (frontend, _, _, _) =>
      frontend.loadFloorMillis shouldBe loaded + lead
  }

  // The host's join ceiling (JoinCeiling, cross-document review G4): a store's previous life on this host joined no
  // transaction started at or above the ceiling it ran under, so the load floor goes no higher than that ceiling

  /** A join ceiling the host's previous process persisted at `persisted`, renewed by `clock` as the host's startup
    * does, unless `renewed` is false
    */
  private def ceilingLeftAt(persisted: Long, renewed: Boolean = true, clock: () => Long = () => floorNow): JoinCeiling =
    val persistence = new JoinCeiling.Persistence:
      @volatile var value: Option[Long] = Some(persisted)
      def read(): Option[Long] = value
      def write(ceiling: Long): Unit = value = Some(ceiling)
      def remove(): Unit = value = None
    val ceiling = new JoinCeiling(Some(persistence), FiniteDuration(100, MILLISECONDS), clock)
    if renewed then ceiling.renew()
    ceiling

  test("a store restarted after a downtime longer than the lead refuses only what started below its host's ceiling") {
    val loaded = floorNow - 500
    val ceiling = loaded - 5_000
    withFloorFrontend(loadedAt = loaded, joinCeiling = ceilingLeftAt(ceiling)): (frontend, _, net, crl) =>
      frontend.loadFloorMillis shouldBe ceiling
      val refused = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp((ceiling - 1) << 16))
      frontend.receiveTransactionMessage(clientPrepare(refused))
      frontend.transactions.contains(txid1) shouldBe false
      net.sentTx shouldBe empty
      // Started during the downtime, after the ceiling: the store never joined it
      val joined = lockTxd(txid2, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp(ceiling << 16))
      frontend.receiveTransactionMessage(clientPrepare(joined))
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
  }

  test("a store restarted soon after its host went down keeps the floor of its load plus the lead") {
    val loaded = floorNow - 500
    withFloorFrontend(loadedAt = loaded, joinCeiling = ceilingLeftAt(loaded + lead + 1)): (frontend, _, _, _) =>
      frontend.loadFloorMillis shouldBe loaded + lead
  }

  test("a store that arrived from another host keeps its arrival's margin under its host's ceiling") {
    val loaded = floorNow - 500
    // Within the margin of the arrival: the margin past the load, whatever the ceiling
    withFloorFrontend(loadedAt = loaded, arrivedAt = Some(loaded - 100),
                      joinCeiling = ceilingLeftAt(loaded - 50)): (frontend, _, _, _) =>
      frontend.loadFloorMillis shouldBe loaded + margin
    // Past it, a ceiling below the arrival plus the margin does not lower the floor beneath that
    val arrived = loaded - margin - 1_000
    withFloorFrontend(loadedAt = loaded, arrivedAt = Some(arrived),
                      joinCeiling = ceilingLeftAt(arrived + 10)): (frontend, _, _, _) =>
      frontend.loadFloorMillis shouldBe arrived + margin
  }

  test("a store mutes an unknown transaction started at or above its host's persisted join ceiling") {
    // Its renewal has fallen behind: the ceiling persisted is just ahead of now
    val ceiling = floorNow + 100
    withFloorFrontend(joinCeiling = ceilingLeftAt(ceiling, renewed = false)): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp(ceiling << 16))
      frontend.startsTooFarAhead(txd) shouldBe true
      frontend.receiveTransactionMessage(clientPrepare(txd))
      votes(net) shouldBe Nil
      crl.txSaved shouldBe false
      frontend.joinCeiling.renew()
      frontend.startsTooFarAhead(txd) shouldBe false
  }

  test("a prepare below the eviction watermark is refused") {
    val statusCache = new TransactionStatusCache(wallClockMillis = () => floorNow)
    withFloorFrontend(statusCache = statusCache): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = startedAgo(1_000))
      frontend.belowForgetFloor(txd) shouldBe false
      // Some transaction's entry is dropped for size after T started: T's may have been among those dropped
      statusCache.updateStatus(txid2, TransactionStatus.Committed, finalized = true)
      statusCache.evictForTest(txid2)
      frontend.receiveTransactionMessage(clientPrepare(txd))

      frontend.transactions.contains(txid1) shouldBe false
      net.sentTx shouldBe empty
      crl.txSaved shouldBe false
  }

  test("a prepare inside the horizon after load is answered as today") {
    withFloorFrontend(): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = startedAgo(19_000))
      frontend.receiveTransactionMessage(clientPrepare(txd))

      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      frontend.transactions(txid1).holdsLocks shouldBe true
      crl.txSaved shouldBe true
  }

  test("a recovered Tx older than the floor still answers its peers' prepares") {
    // Restarted: the store loaded now, and recovered a transaction started long before from its CRL. Before T's
    // expiry (horizon plus the default 8 s margin), past which its acceptor is frozen
    val txd = dataUpdateTxd.copy(startTimestamp = startedAgo(22_000))
    val trs = TransactionRecoveryState(storeId, txd.serialize(), Nil, TransactionDisposition.Undetermined,
      TransactionStatus.Unresolved, PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    // No client prepare will come: it answers a driver's once the (zero) wait for it is over
    withFloorFrontend(loadedAt = floorNow, crl = crl, missingDataWait = Duration.Zero): (frontend, _, net, _) =>
      frontend.belowForgetFloor(txd) shouldBe true
      frontend.receiveTransactionMessage(driverPrepare(txd))

      votes(net).map(v => (v.disposition, v.response)) shouldBe
        List((TransactionDisposition.VoteAbort, Right(TxPrepareResponse.Promise(None))))
  }

  test("a prepare below the floor with a resolved, unfinalized status-cache entry is refused") {
    // The host's cache is shared: another store's Tx may have written the entry after this store forgot T. A Tx
    // created here could apply T again once this store's applied stub had expired (Todo 4.6)
    val statusCache = new TransactionStatusCache(wallClockMillis = () => floorNow)
    statusCache.updateStatus(txid1, TransactionStatus.Committed)
    val admission = new CrlAdmission(1024 * 1024)
    withFloorFrontend(statusCache = statusCache, admission = admission): (frontend, _, net, crl) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = startedAgo(25_000))
      frontend.belowForgetFloor(txd) shouldBe true
      admitAndReceive(frontend, clientPrepare(txd))

      frontend.transactions.contains(txid1) shouldBe false
      net.sentTx shouldBe empty
      crl.txSaved shouldBe false
      admission.admittedBytes shouldBe 0
      statusCache.getStatus(txid1).map(_.status) shouldBe Some(TransactionStatus.Committed)
  }

  test("a prepare inside the floor with a resolved status-cache entry still learns the outcome") {
    val statusCache = new TransactionStatusCache(wallClockMillis = () => floorNow)
    statusCache.updateStatus(txid1, TransactionStatus.Committed)
    withFloorFrontend(statusCache = statusCache): (frontend, _, net, _) =>
      val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = startedAgo(15_000))
      frontend.belowForgetFloor(txd) shouldBe false
      frontend.receiveTransactionMessage(clientPrepare(txd))

      // Checked, the outcome known, no locks, the cached status kept
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      frontend.transactions(txid1).holdsLocks shouldBe false
      statusCache.getStatus(txid1).map(_.status) shouldBe Some(TransactionStatus.Committed)
  }

  test("a refused prepare from a client clock slower than the horizon logs a warning with the lag") {
    val slowTxid = TransactionId(new UUID(0, 77))
    withFloorFrontend(): (frontend, _, _, _) =>
      val txd = lockTxd(slowTxid, List(overwrite(op1))).copy(startTimestamp = startedAgo(25_000))
      val log = logNaming(slowTxid):
        frontend.receiveTransactionMessage(clientPrepare(txd))
        // A driver's prepare is not the client's clock: not logged
        frontend.receiveTransactionMessage(driverPrepare(txd))

      log.map(_._1) shouldBe List(scribe.Level.Warn)
      log.head._2 should include ("25000ms")
      frontend.transactions.contains(slowTxid) shouldBe false
  }

  test("refused client prepares are warned of once, then at most once per period with the count since") {
    // While a status cache eviction's watermark binds, every client prepare is refused: one warning each would flood
    // the log
    var clock = floorNow
    val backend = new MapBackend(storeId)
    populate(backend)
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
      new TestCrl, new TransactionStatusCache(wallClockMillis = () => clock), wallClockMillis = () => clock,
      horizon = FiniteDuration(20, SECONDS), loadedAt = Some(floorNow - 60_000))
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    val List(first, held, later) = List(81, 82, 83).map(i => TransactionId(new UUID(0, i))): @unchecked
    def refuse(txid: TransactionId): Unit =
      val txd = lockTxd(txid, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp((clock - 25_000) << 16))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.transactions.contains(txid) shouldBe false
    val log = logNaming(first)(refuse(first)) ++ logNaming(held)(refuse(held)) ++
      logNaming(later) { clock += Frontend.RefusalLogPeriod.toMillis; refuse(later) }

    log.map(_._1) shouldBe List(scribe.Level.Warn, scribe.Level.Warn)
    log.head._2 should include (first.toString)
    log(1)._2 should include (later.toString)
    log(1)._2 should include (LogRateLimit.heldText(1))
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

    // txid2 votes commit and locks op1 and the fence. Its fence write is a refcount change, which txid1's overwrite
    // below does not invalidate: an update txid1 did invalidate could not be applied, and the store would fail-stop
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1),
      RefcountUpdate(fencePtr, ObjectRefcount(1, 1), ObjectRefcount(2, 2)))))
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
    // One backend commit per object txid2 changes
    backend.issued shouldBe List(txid1, txid2, txid2)
    locksOn(op1) shouldBe Set()
    locksOn(fencePtr) shouldBe Set()
    frontend.transactions.get(txid2).forall(!_.holdsLocks) shouldBe true
    readData(frontend, net, op1) shouldBe Right(List[Byte](1))
  }

  // ---- Requirement locks held between a commit vote and resolution ----

  val txid3 = TransactionId(new UUID(0, 11))
  val fenceOid = ObjectId(new UUID(0, 12))
  val fencePtr = DataObjectPointer(fenceOid, poolId, Array[Byte]())

  /** The primary object is one the transaction references, as stores require (DescriptionValidator) */
  def lockTxd(txid: TransactionId, reqs: List[TransactionRequirement]): TransactionDescription =
    val primary = reqs.collectFirst { case r: TransactionObjectRequirement => r.objectPointer }.getOrElse(op1)
    mktxd().copy(transactionId = txid, requirements = reqs, primaryObject = primary)

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
  def populate(backend: MapBackend, kv: ObjectState = mkKVObjectState()): Unit =
    for oid <- List(oid1, fenceOid) do
      val os = mkObjectState(oid)
      backend.bootstrapAllocate(oid, os.objectType, os.metadata, os.data)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      unackedLookup = lookup, loadedAt = FrontendSuite.longLoaded)
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
    lookup.answer(txid1, lookup.absent)
    lookup.answer(txid1, lookup.absent)
    prepare(frontend, net, lockTxd(txid3, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  // Two recovered commit votes on one exclusive lock (transaction-requirements.md 10.5a): txid1 committed, applied
  // and persisted but not finalized; txid2, built on it, voted after. Whichever loads last takes the lock, and
  // txid1's resolution must leave txid2 holding it
  for order <- List("earlier first", "later first") do
    test(s"two recovered commit votes on one lock: the earlier's release leaves the later locked ($order)") {
      val backend = new MapBackend(storeId)
      val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
      val applied = mkObjectState(oid1)
      applied.metadata = applied.metadata.copy(revision = ObjectRevision(txid1))
      backend.bootstrapAllocate(oid1, applied.objectType, applied.metadata, applied.data)
      // Cached, so the recovered transactions load in CRL order inside the Frontend's construction
      cache.insert(applied)
      val votes = List(commitVote(txid1, List(overwrite(op1))), commitVote(txid2, List(overwriteAfter(txid1))))
      val crl = new TestCrl:
        override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
          if order == "earlier first" then votes else votes.reverse
      val lookup = new ManualLookup
      val net = new TestNet
      val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(),
        unackedLookup = lookup, loadedAt = FrontendSuite.longLoaded)
      lookup.frontend = Some(frontend)
      backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

      resolve(frontend, net, txid1, committed = true)
      val r = prepare(frontend, net, lockTxd(txid3, List(overwriteAfter(txid1))))
      r.disposition shouldBe TransactionDisposition.VoteAbort
      r.collisions shouldBe List(txid2)

      resolve(frontend, net, txid2, committed = true)
      val txid4 = TransactionId(new UUID(0, 12))
      prepare(frontend, net, lockTxd(txid4, List(overwriteAfter(txid2)))).disposition shouldBe
        TransactionDisposition.VoteCommit
    }

  // A blind key write passes its apply re-check after it applied, so a recovered one applies again: its Insert, equal
  // to the entry it wrote, must keep the lock a later recovered vote holds on the key
  for order <- List("earlier first", "later first") do
    test(s"a recovered blind insert applied again keeps the later recovered vote's key lock ($order)") {
      val backend = new MapBackend(storeId)
      val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
      val revA = ObjectRevision(txid1)
      val tsA = HLCTimestamp(100L << 16)
      // txid1, a MayExist insert of k(1), applied and persisted: k(1) carries its revision and timestamp
      val contents = Map(k(1) -> new ValueState(Value(Array[Byte](1)), revA, tsA, None))
      val applied = new ObjectState(oid2, Metadata(rev0, ObjectRefcount(1, 1), tsA), ObjectType.KeyValue,
        KVObjectState.encode(None, None, None, None, contents))
      backend.bootstrapAllocate(oid2, applied.objectType, applied.metadata, applied.data)
      cache.insert(applied)
      def insert(v: Byte) = KeyValueOperation.encode(List(new Insert(k(1), Array[Byte](v))), ida)(0)
      def checked(txid: TransactionId) = lockTxd(txid, List(KeyValueUpdate(kvPtr, None, None,
        List(KeyValueUpdate.KeyRevision(k(1), revA)), pinned = true)))
      val blind = lockTxd(txid1, List(KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.MayExist(k(1))))))
        .copy(startTimestamp = tsA)
      def vote(t: TransactionDescription, v: Byte) = TransactionRecoveryState(storeId, t.serialize(),
        List(ObjectUpdate(oid2, insert(v))), TransactionDisposition.VoteCommit, TransactionStatus.Unresolved,
        PersistentState.initial)
      val votes = List(vote(blind, 1), vote(checked(txid2).copy(startTimestamp = HLCTimestamp(200L << 16)), 2))
      val crl = new TestCrl:
        override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
          if order == "earlier first" then votes else votes.reverse
      val lookup = new ManualLookup
      val net = new TestNet
      val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(),
        unackedLookup = lookup, loadedAt = FrontendSuite.longLoaded)
      lookup.frontend = Some(frontend)
      backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

      resolve(frontend, net, txid1, committed = true)
      cache.get(oid2).get.lockedWriteTransactions shouldBe Set(txid2)
      val r = prepare(frontend, net, checked(txid3), insert(3))
      r.disposition shouldBe TransactionDisposition.VoteAbort
      r.collisions shouldBe List(txid2)
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

  test("a store applying two blind inserts of a key in opposite order keeps the later one (Req F1)") {
    val (frontend, backend, net, stops) = failStopFixture()
    val now = System.currentTimeMillis()
    def blindInsert(txid: TransactionId, startMs: Long) =
      lockTxd(txid, List(KeyValueUpdate(kvPtr, None, None, Nil))).copy(startTimestamp = HLCTimestamp(startMs << 16))
    def insertOf(v: Byte) = KeyValueOperation.encode(List(new Insert(k(1), Array[Byte](v))), ida)(0)

    // A votes commit here and holds k(1). B, started after A committed elsewhere, collides here but is committed by
    // the other stores, and its resolution reaches this store before A's (delayed) one
    prepare(frontend, net, blindInsert(txid1, now - 10), insertOf(1)).disposition shouldBe TransactionDisposition.VoteCommit
    prepare(frontend, net, blindInsert(txid2, now), insertOf(2)).disposition shouldBe TransactionDisposition.VoteAbort
    resolve(frontend, net, txid2, committed = true)
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    net.txMessage() match
      case Some(c: TxCommitted) => c.objectCommitErrors shouldBe Nil
      case other => throw new AssertionError(s"Expected TxCommitted, got $other")

    val stored = backend.get(oid2).get
    val vs = KVObjectState.decode(stored.data).content(k(1))
    vs.revision shouldBe ObjectRevision(txid2)
    stored.metadata.timestamp shouldBe HLCTimestamp(now << 16)
    stops() shouldBe Nil
  }

  // ---- The store validates the description before voting (transaction-requirements F7, Q-N4; DescriptionValidator) ----

  /** lockFixture whose Frontend knows its pool's IDA */
  def validatingFixture(): (Frontend, MapBackend, TestNet) =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded, poolIDA = Some(ida))
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    (frontend, backend, net)

  def insertK(n: Int): DataBuffer = KeyValueOperation.encode(List(new Insert(k(n), Array[Byte](9))), ida)(0)

  /** A key update on kvPtr checking k(1) at its revision, flagged pinned */
  def pinnedUpdate: KeyValueUpdate =
    KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.KeyRevision(k(1), rev0)), pinned = true)

  test("a store votes abort on a description naming another IDA for its pool") {
    val (frontend, _, net) = validatingFixture()
    val other = Replication(5, 3)
    val txd = lockTxd(txid1, List(overwrite(op1))).copy(primaryObjectIDA = other, poolIDAMap = Map(poolId -> other))
    prepare(frontend, net, txd).disposition shouldBe TransactionDisposition.VoteAbort
    frontend.holdsLocks(txid1) shouldBe false
  }

  test("a store votes abort on a description that updates one object twice") {
    val (frontend, _, net) = validatingFixture()
    val txd = lockTxd(txid1, List(pinnedUpdate, KeyValueUpdate(kvPtr, Some(rev0), None, Nil)))
    prepare(frontend, net, txd, insertK(1)).disposition shouldBe TransactionDisposition.VoteAbort
  }

  test("a store votes abort on an unpinned description not marked replaySafe, and commit once it is marked") {
    val (frontend, _, net) = validatingFixture()
    val blind = KeyValueUpdate(kvPtr, None, None, Nil)
    prepare(frontend, net, lockTxd(txid1, List(blind)).copy(replaySafe = false), insertK(4)).disposition shouldBe
      TransactionDisposition.VoteAbort
    prepare(frontend, net, lockTxd(txid2, List(blind)), insertK(4)).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  test("a store votes abort on a pinned flag whose checked key the update does not write") {
    val (frontend, _, net) = validatingFixture()
    val txd = lockTxd(txid1, List(pinnedUpdate)).copy(replaySafe = false)
    prepare(frontend, net, txd, insertK(2)).disposition shouldBe TransactionDisposition.VoteAbort
    prepare(frontend, net, txd.copy(transactionId = txid2), insertK(1)).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  test("a store relies on the pinned flag of an object another pool holds") {
    // The store holds only kvPtr's unpinned blind insert. The pin is on another pool's object, whose stores check
    // the flag: a false one gets their abort votes, so the transaction cannot commit
    val (frontend, _, net) = validatingFixture()
    val elsewhere = KeyValueObjectPointer(ObjectId(new UUID(0, 13)), otherPoolId, Array[Byte]())
    val pinElsewhere = KeyValueUpdate(elsewhere, None, None, List(KeyValueUpdate.KeyRevision(k(1), rev0)), pinned = true)
    val txd = lockTxd(txid1, List(KeyValueUpdate(kvPtr, None, None, Nil), pinElsewhere)).copy(
      replaySafe = false, poolIDAMap = Map(poolId -> ida, otherPoolId -> ida))
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid2, insertK(4)))))
    net.txMessage() match
      case Some(r: TxPrepareResponse) => r.disposition shouldBe TransactionDisposition.VoteCommit
      case other => throw new AssertionError(s"Expected a TxPrepareResponse, got $other")
  }

  // ---- A commit voter that cannot hold the update it voted for (transaction-requirements, the apply re-check) ----

  /** lockFixture whose Frontend records its fail-stops and verifies quarantines through `verifier` */
  def failStopFixture(verifier: QuarantineVerifier = QuarantineVerifier.Never)
      : (Frontend, MapBackend, TestNet, () => List[Throwable]) =
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val net = new TestNet
    var stops = List[Throwable]()
    val frontend = new Frontend(storeId, backend, cache, net, new TestCrl, new TransactionStatusCache(),
      quarantineVerifier = verifier, loadedAt = FrontendSuite.longLoaded, onFailStop = err => stops = stops :+ err)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    (frontend, backend, net, () => stops)

  /** Resolves `txid` committed and returns the objectCommitErrors of its TxCommitted */
  def commitErrors(frontend: Frontend, net: TestNet, txid: TransactionId): List[ObjectId] =
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid, committed = true, late = false, proof = false))
    committedMessages(net).filter(_.transactionId == txid).flatMap(_.objectCommitErrors)

  test("a live commit voter whose state is invalidated under its locks quarantines the object, not the store") {
    val (frontend, backend, net, stops) = failStopFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    net.clientMessage() // txid1's vote to its client

    // The cached state can no longer be served while txid1 holds its locks: the committed update cannot be applied
    frontend.discard(oid1)
    commitErrors(frontend, net, txid1) shouldBe List(oid1)
    backend.get(oid1).get.metadata.revision shouldBe rev0
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
    frontend.isQuarantined(oid1) shouldBe true

    // A transaction built on the pre-txid1 state gets no commit vote, which together with txid1's own counted vote
    // could commit both
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort
  }

  test("a live commit voter whose object a conflicting commit reached first quarantines it, and repair clears it") {
    val verifier = new ManualVerifier
    val (frontend, backend, net, stops) = failStopFixture(verifier)
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    // txid1 collides with txid2's lock here, but the other stores' votes commit it. Its apply ignores the lock
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort
    commitErrors(frontend, net, txid1) shouldBe Nil
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid1)

    // txid2 commits too: its revision requirement no longer holds. The object is quarantined, the store kept
    commitErrors(frontend, net, txid2) shouldBe List(oid1)
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
    frontend.isQuarantined(oid1) shouldBe true
    verifier.made.map(_._1) shouldBe List(oid1)
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid1)
    prepare(frontend, net, lockTxd(txid3, List(overwriteAfter(txid1)))).disposition shouldBe
      TransactionDisposition.VoteAbort

    verifier.answer(Success(true))
    frontend.isQuarantined(oid1) shouldBe false
    prepare(frontend, net, lockTxd(TransactionId(new UUID(0, 21)), List(overwriteAfter(txid1)))).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  test("a live commit voter whose lock-only object a later commit changed first neither fail-stops nor quarantines") {
    val (frontend, backend, net, stops) = failStopFixture()
    // txid1 only locks the fence (a vdisk block commit's SharedRevisionLock, say)
    prepare(frontend, net, lockTxd(txid1, List(SharedRevisionLock(fencePtr, rev0)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    // txid2 bumps the fence at the same revision (a takeover): it collides here but the other stores commit it
    prepare(frontend, net, lockTxd(txid2, List(overwrite(fencePtr)))).disposition shouldBe
      TransactionDisposition.VoteAbort
    commitErrors(frontend, net, txid2) shouldBe Nil
    backend.get(fenceOid).get.metadata.revision shouldBe ObjectRevision(txid2)

    // txid1's outcome arrives after txid2's: its lock-only re-check fails, and there is nothing to apply. The skip is
    // reported, as every skipped object is
    commitErrors(frontend, net, txid1) shouldBe List(fenceOid)
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
    frontend.isQuarantined(fenceOid) shouldBe false
    frontend.testingOnlyQuarantines shouldBe 0
  }

  test("a live commit voter whose object a later refcount change reached first does not fail-stop") {
    val (frontend, _, net, stops) = failStopFixture()
    val first = lockTxd(txid1, List(overwrite(op1)))
    prepare(frontend, net, first).disposition shouldBe TransactionDisposition.VoteCommit
    // Started later than txid1, so its apply raises the object timestamp past txid1's start
    val later = lockTxd(txid2, List(RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 2)))).copy(
      startTimestamp = HLCTimestamp(first.startTimestamp.asLong + (1000L << 16)))
    prepare(frontend, net, later).disposition shouldBe TransactionDisposition.VoteAbort
    commitErrors(frontend, net, txid2) shouldBe Nil
    // txid2 raised the object timestamp past txid1's start: txid1's re-check fails, so the object is quarantined
    commitErrors(frontend, net, txid1) shouldBe List(oid1)
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
    frontend.isQuarantined(oid1) shouldBe true
  }

  test("an abort voter that cannot apply a committed update does not fail-stop") {
    val (frontend, backend, net, stops) = failStopFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    // txid2 collides here and is committed by the votes of other stores; txid1 then aborts
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort
    resolve(frontend, net, txid1, committed = false)
    frontend.discard(oid1)
    resolve(frontend, net, txid2, committed = true)
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
    prepare(frontend, net, lockTxd(txid3, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a failed backend commit of a commit voter's update does not fail-stop the store") {
    val crl = new TestCrl
    val (frontend, backend, net) = heldCommit(crl)
    backend.deliverCommits(Right(CommitError.IOFailure))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
    frontend.failStopped shouldBe false
  }

  /** A Frontend recovering a commit vote of txid1 (overwrite op1 at rev0) whose object the backend holds as `stored`,
    * or answers with `readError`. Records its fail-stops
    */
  def recoveredVote(stored: ObjectState, readError: Option[ReadError.Value] = None,
                    frozen: Boolean = false,
                    txd: TransactionDescription = lockTxd(txid1, List(overwrite(op1))),
                    status: TransactionStatus.Value = TransactionStatus.Unresolved,
                    updates: List[ObjectUpdate] = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1)))),
                    crl: TestCrl = new TestCrl,
                    verifier: QuarantineVerifier = QuarantineVerifier.Never)
      : (Frontend, TestNet, () => List[Throwable]) =
    val backend = new MapBackend(storeId):
      private var failNext = readError
      private var handler: Option[CompletionHandler] = None
      override def isWriteFrozen: Boolean = frozen
      override def setCompletionHandler(h: CompletionHandler): Unit =
        handler = Some(h)
        super.setCompletionHandler(h)
      override def read(pointer: ObjectPointer): Unit = failNext match
        case Some(err) =>
          failNext = None
          handler.foreach(_.complete(org.aspen_ddp.aspen.server.store.backend.Read(storeId, pointer.id, Right(err))))
        case None => super.read(pointer)
    backend.bootstrapAllocate(stored.objectId, stored.objectType, stored.metadata, stored.data)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    val trs = TransactionRecoveryState(storeId, txd.serialize(), updates, TransactionDisposition.VoteCommit, status,
      PersistentState.initial)
    val net = new TestNet
    var stops = List[Throwable]()
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(),
      unackedLookup = lookup, quarantineVerifier = verifier, loadedAt = FrontendSuite.longLoaded,
      deferRecovery = true, onFailStop = err => stops = stops :+ err)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    frontend.recover(StoreRecoveryState(List(trs), Map()))
    (frontend, net, () => stops)

  test("a recovered commit vote whose object fails to load fail-stops the store (Req F6)") {
    val (frontend, net, stops) = recoveredVote(mkObjectState(oid1), Some(ReadError.CorruptedObject))
    stops().size shouldBe 1
    // The object loads now, with no lock on it: nothing may vote on it
    frontend.receiveTransactionMessage(mkPrepare(lockTxd(txid2, List(overwrite(op1))), fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    net.sentTx.collect { case r: TxPrepareResponse if r.transactionId == txid2 => r } shouldBe Nil
  }

  test("a recovered commit vote whose object is not found does not fail-stop") {
    val (frontend, _, stops) = recoveredVote(mkObjectState(oid1), Some(ReadError.ObjectNotFound))
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
  }

  test("a recovered commit vote whose object fails to load on a write-frozen store poisons it instead of fail-stopping") {
    val (frontend, net, stops) = recoveredVote(mkObjectState(oid1), Some(ReadError.CorruptedObject), frozen = true)
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
    // The object now loads, with no lock on it: every transaction on it votes abort
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteAbort
    frontend.readObjectForNetwork(clientId, new UUID(1, 1), op1, FullObject())
    net.clientMessage().isDefined shouldBe true
  }

  /** A recovered commit vote of txid1 (overwrite op1 at rev0) whose re-check fails: the store holds op1 at txid2's
    * revision. Either txid2 superseded txid1's update before the crash, or the state was invalidated under txid1's locks:
    * the store cannot tell, so op1 is quarantined once txid1 resolves committed
    */
  def quarantinedVote(): (Frontend, TestNet, TestCrl, ManualVerifier, () => List[Throwable]) =
    val later = mkObjectState(oid1)
    later.metadata = later.metadata.copy(revision = ObjectRevision(txid2))
    val crl = new TestCrl
    val verifier = new ManualVerifier
    val (frontend, net, stops) = recoveredVote(later, crl = crl, verifier = verifier)
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    (frontend, net, crl, verifier, stops)

  test("a recovered commit vote whose apply re-check fails quarantines the object rather than count with neither " +
       "its lock nor its update (Req F5, Todo 2.16)") {
    val (frontend, net, crl, verifier, stops) = quarantinedVote()
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
    stops() shouldBe Nil
    frontend.failStopped shouldBe false
    frontend.isQuarantined(oid1) shouldBe true
    verifier.made.map(_._1) shouldBe List(oid1)
    // Its object data stays: a reloaded store recovers the transaction and quarantines again
    crl.dropped shouldBe Nil

    // Before the fix this voted commit: with txid1's lock gone and its update possibly never applied, a transaction
    // built on the store's state could commit beside txid1
    prepare(frontend, net, lockTxd(txid3, List(overwriteAfter(txid2)))).disposition shouldBe
      TransactionDisposition.VoteAbort
  }

  test("a quarantine clears once repair verifies the object, and the CRL state it kept is then concluded") {
    val (frontend, net, crl, verifier, stops) = quarantinedVote()
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid1) shouldBe false
    // Kept: the quarantine lives in memory only, and the CRL state is what quarantines again after a crash
    crl.deleted shouldBe Nil

    // Not complete: retried after 1 heartbeat, then 2
    verifier.answer(Success(false))
    frontend.isQuarantined(oid1) shouldBe true
    frontend.retryQuarantineVerifications()
    verifier.made.size shouldBe 2
    verifier.answer(Failure(new Exception("read failed")))
    frontend.retryQuarantineVerifications()
    verifier.made.size shouldBe 2
    frontend.retryQuarantineVerifications()
    verifier.made.size shouldBe 3

    verifier.answer(Success(true))
    frontend.isQuarantined(oid1) shouldBe false
    crl.deleted shouldBe List(txid1)
    stops() shouldBe Nil
    prepare(frontend, net, lockTxd(txid3, List(overwriteAfter(txid2)))).disposition shouldBe
      TransactionDisposition.VoteCommit
  }

  test("a transaction committed by other stores' votes applies nothing to a quarantined object and reports it") {
    val (frontend, net, _, _, stops) = quarantinedVote()
    prepare(frontend, net, lockTxd(txid3, List(overwriteAfter(txid2)))).disposition shouldBe
      TransactionDisposition.VoteAbort
    resolve(frontend, net, txid3, committed = true)
    committedMessages(net).filter(_.transactionId == txid3).map(_.objectCommitErrors) shouldBe List(List(oid1))
    stops() shouldBe Nil
    frontend.isQuarantined(oid1) shouldBe true
  }

  test("a commit vote recovered with a persisted Committed status applies its update on load (R-F6)") {
    val crl = new TestCrl
    val (frontend, net, stops) = recoveredVote(mkObjectState(oid1), status = TransactionStatus.Committed, crl = crl)
    // No outcome message: the resolution it saved is restored, so it applies at once
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
    crl.dropped shouldBe List(txid1)
    stops() shouldBe Nil
    // Resolved, so it holds no locks
    prepare(frontend, net, lockTxd(txid2, List(overwriteAfter(txid1)))).disposition shouldBe
      TransactionDisposition.VoteCommit
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    crl.deleted shouldBe List(txid1)
    frontend.transactions.contains(txid1) shouldBe false
  }

  test("a commit vote recovered Committed after its update persisted applies nothing again and does not fail-stop") {
    val applied = mkObjectState(oid1)
    applied.metadata = applied.metadata.copy(revision = ObjectRevision(txid1))
    // Its object data was dropped once the commit persisted
    val (_, net, stops) = recoveredVote(applied, status = TransactionStatus.Committed, updates = Nil)
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
    stops() shouldBe Nil
  }

  test("a commit vote recovered with a persisted Aborted status deletes its state on load and holds no locks") {
    val crl = new TestCrl
    val (frontend, net, stops) = recoveredVote(mkObjectState(oid1), status = TransactionStatus.Aborted, crl = crl)
    crl.deleted shouldBe List(txid1)
    committedMessages(net) shouldBe Nil
    stops() shouldBe Nil
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a prepare arriving after its transaction was finalized starts no transaction and takes no locks") {
    val (frontend, _, net) = lockFixture()
    val txd = lockTxd(txid1, List(overwrite(op1)))

    // Aborted by the other stores and finalized before this store saw its prepare
    prepare(frontend, net, txd).disposition shouldBe TransactionDisposition.VoteCommit
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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

    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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
    val frontend = new Frontend(storeId, backend, ObjectCache.NoCache, net, new TestCrl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      new TransactionStatusCache(), deferRecovery = true, loadedAt = FrontendSuite.longLoaded)
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
      override def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = Map(txid1 -> stubStart)
    val f = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet, crl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    f.hasAppliedStub(txid1) shouldBe true
    f.hasAppliedStub(txid2) shouldBe false
    f.transactions shouldBe empty

    // Deferred, as a host recovers: from the state it read
    val deferred = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet,
      new TestCrl, new TransactionStatusCache(), deferRecovery = true, loadedAt = FrontendSuite.longLoaded)
    deferred.hasAppliedStub(txid2) shouldBe false
    deferred.recover(StoreRecoveryState(Nil, Map(txid2 -> stubStart)))
    deferred.hasAppliedStub(txid2) shouldBe true
    deferred.appliedStubs.keySet shouldBe Set(txid2)
    // A stub is not a transaction
    deferred.transactions shouldBe empty
  }

  test("recordAppliedStub continues once the stub is durable, and dropAppliedStub deletes it") {
    val backend = new MapBackend(storeId)
    populate(backend)
    val crl = new TestCrl
    crl.holdStubSaves = true
    val f = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet, crl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    var durable = false
    f.recordAppliedStub(txid1, stubStart) { durable = true }
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
    val frontend = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
        crl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
  private val deleteK2: KeyValueUpdate =
    KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.KeyRevision(k(2), rev0)), deletesKeys = true)

  /** A Frontend recovering T, a commit vote of `reqs` deleting key 2 of kvPtr, with its lookups made through `lookup` */
  private def unackedFixture(reqs: List[TransactionRequirement] = List(deleteK2),
                             statusCache: TransactionStatusCache = new TransactionStatusCache(),
                             stubs: Set[TransactionId] = Set(),
                             verifier: QuarantineVerifier = QuarantineVerifier.Never,
                             ops: List[KeyValueOperation] = List(new Delete(k(2))),
                             kvState: () => ObjectState = () => mkKVObjectState())
      : (Frontend, MapBackend, TestNet, TestCrl, ManualLookup) =
    val backend = new MapBackend(storeId)
    populate(backend, kvState())
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    // Cached so the recovered transaction's object loads during Frontend construction
    cache.insert(kvState())
    val update = KeyValueOperation.encode(ops, ida)(0)
    val trs = TransactionRecoveryState(storeId, lockTxd(unackedTxid, reqs).serialize(),
      List(ObjectUpdate(oid2, update)), TransactionDisposition.VoteCommit, TransactionStatus.Unresolved,
      PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
      override def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] =
        stubs.iterator.map(_ -> stubStart).toMap
    val net = new TestNet
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache, unackedLookup = lookup,
      quarantineVerifier = verifier, loadedAt = FrontendSuite.longLoaded)
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
  private def unackedLog(f: => Unit): List[(scribe.Level, String)] = logNaming(unackedTxid)(f)

  /** Runs `f`, returning the log lines at info or above naming `txid`, with their levels */
  private def logNaming(txid: TransactionId)(f: => Unit): List[(scribe.Level, String)] =
    val lines = new LinkedBlockingQueue[(scribe.Level, String)]()
    val handler = scribe.handler.LogHandler(scribe.Level.Info): r =>
      val text = r.logOutput.plainText
      if text.contains(txid.toString) then
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
      lookup.answer(unackedTxid, lookup.own)

    kvKeys(backend) should not contain k(2)
    frontend.transactions(unackedTxid).provesCommitToItsDriver shouldBe true
    // Ruling 9: still live, its CRL and acceptor state kept until TxFinalized. Ruling 12: no status cache entry
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
    crl.deleted should not contain (unackedTxid)
    statusCache.getStatus(unackedTxid).map(_.status).forall(_ == TransactionStatus.Unresolved) shouldBe true

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.stubsSaved should contain (unackedTxid) // An applied stub replaces its state (replay-pins addendum A)
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
      unackedLookup = lookup, loadedAt = FrontendSuite.longLoaded)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    lookup.requests shouldBe List(unackedTxid)

    lookup.answer(unackedTxid, lookup.own)

    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](9)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
    frontend.receiveTransactionMessage(TxFinalized(storeId, StoreId(poolId, 2), unackedTxid, committed = true,
      late = false, owed = Set(), startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.stubsSaved should contain (unackedTxid) // An applied stub replaces its state (replay-pins addendum A)
  }

  test("a recovered commit vote without a UnackedCommits entry accepts an abort") {
    val (frontend, backend, _, crl, lookup) = unackedFixture()
    lookup.answer(unackedTxid, lookup.absent)

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    // The abort waits for a lookup started after it arrived
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    frontend.transactions.contains(unackedTxid) shouldBe true
    lookup.answer(unackedTxid, lookup.absent)

    kvKeys(backend) should contain (k(2))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.deleted should contain (unackedTxid)
  }

  test("a retransmitted abort does not push back the lookup a held abort waits for") {
    // A driver resends its resolution until it hears back: each copy moving the counting window forward would keep a
    // lookup in flight from ever counting, and the Tx holding its locks
    val (frontend, _, _, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, lookup.absent)

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    lookup.answer(unackedTxid, lookup.absent)

    frontend.transactions.get(unackedTxid).map(_.status).forall(_ == TransactionStatus.Aborted) shouldBe true
    frontend.transactions.get(unackedTxid).map(_.holdsLocks) shouldBe Some(false)
  }

  // transaction-recovery.md section 7, entries naming no incarnation: a driver that did not count the votes writes one
  // for a store it never heard from, which may be the counted voter the commit relied on
  test("a recovered commit vote whose entry names no incarnation applies it and never accepts its held abort") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = unackedFixture(statusCache = statusCache)
    lookup.answer(unackedTxid, lookup.absent)

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false,
      proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    lookup.answer(unackedTxid, Success(UnackedLookupResult.Found(None)))

    // As for a matching entry: the Tx resolves committed, applies its update and waits for TxFinalized, recording
    // nothing in the status cache, its held abort never accepted
    kvKeys(backend) should not contain k(2)
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
    val tx = frontend.transactions(unackedTxid)
    tx.status shouldBe TransactionStatus.Committed
    tx.isFinalized shouldBe false
    tx.provesCommitToItsDriver shouldBe true
    tx.awaitsUnackedLookup shouldBe false
    tx.holdsLocks shouldBe false
    crl.deleted should not contain (unackedTxid)
    crl.stubsSaved shouldBe Nil
    statusCache.getStatus(unackedTxid) shouldBe
      Some(TransactionStatusCache.Entry(TransactionStatus.Unresolved, finalized = false, late = false))
    lockedWriteTransactions(frontend, net, kvPtr) should not contain (unackedTxid)

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(unackedTxid) shouldBe false
    kvKeys(backend) should not contain k(2)
    crl.stubsSaved should contain (unackedTxid) // Applied through its entry: a stub replaces its state
    statusCache.getStatus(unackedTxid) shouldBe
      Some(TransactionStatusCache.Entry(TransactionStatus.Committed, finalized = true, late = false))
  }

  test("a recovered commit vote whose entry names another incarnation resolves committed, applies nothing and never " +
    "accepts its held abort") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = unackedFixture(statusCache = statusCache)
    lookup.answer(unackedTxid, lookup.absent)

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false,
      proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    lookup.answer(unackedTxid, Success(UnackedLookupResult.Found(Some(incarnationA))))

    // T committed, but its finalization may not be complete: the Tx resolves committed, applies nothing, and waits for
    // TxFinalized, recording nothing in the status cache (as for a matching entry), its held abort never accepted
    kvKeys(backend) should contain (k(2))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid2)) // reported as not committed here
    val tx = frontend.transactions(unackedTxid)
    tx.status shouldBe TransactionStatus.Committed
    tx.isFinalized shouldBe false
    tx.provesCommitToItsDriver shouldBe true
    tx.awaitsUnackedLookup shouldBe false
    tx.holdsLocks shouldBe false
    crl.deleted should not contain (unackedTxid)
    crl.stubsSaved shouldBe Nil
    statusCache.getStatus(unackedTxid) shouldBe
      Some(TransactionStatusCache.Entry(TransactionStatus.Unresolved, finalized = false, late = false))
    lockedWriteTransactions(frontend, net, kvPtr) should not contain (unackedTxid)

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(unackedTxid) shouldBe false
    kvKeys(backend) should contain (k(2))
    crl.stubsSaved shouldBe Nil
    crl.deleted should contain (unackedTxid)
    statusCache.getStatus(unackedTxid) shouldBe
      Some(TransactionStatusCache.Entry(TransactionStatus.Committed, finalized = true, late = false))
  }

  test("a held TxFinalized abort is not replaced by a later held TxResolved, so the Tx is forgotten once accepted") {
    val (frontend, _, _, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, lookup.absent)

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.answer(unackedTxid, lookup.absent)

    frontend.transactions.contains(unackedTxid) shouldBe false
  }

  test("an abort arriving before the UnackedCommits entry is visible waits for a later lookup and then commits") {
    val (frontend, backend, _, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, lookup.absent)

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 2 // a fresh lookup started after the abort
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Unresolved

    lookup.answer(unackedTxid, lookup.own)
    kvKeys(backend) should not contain k(2)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
  }

  test("an abort arriving while the recovery's UnackedCommits lookup is in flight waits for a fresh lookup") {
    val (frontend, backend, _, _, lookup) = unackedFixture()
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false, proof = false))
    lookup.requests.count(_ == unackedTxid) shouldBe 1

    // Started before the abort, so its answer does not count: another lookup is made
    lookup.answer(unackedTxid, lookup.absent)
    lookup.requests.count(_ == unackedTxid) shouldBe 2
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Unresolved

    lookup.answer(unackedTxid, lookup.absent)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Aborted
    kvKeys(backend) should contain (k(2))
  }

  test("a failing UnackedCommits lookup holds the abort and is retried at heartbeat") {
    val (frontend, _, net, _, lookup) = unackedFixture()
    lookup.answer(unackedTxid, Failure(new Exception("metadata pool down")))
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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

  test("an object whose requirements fail on the UnackedCommits local apply is quarantined and the Tx still completes") {
    // The store's key 2 is at a different revision than T requires
    val stale = KeyValueUpdate(kvPtr, None, None,
      List(KeyValueUpdate.KeyRevision(k(2), new ObjectRevision(new UUID(0, 41)))), deletesKeys = true)
    val verifier = new ManualVerifier
    val (frontend, backend, _, crl, lookup) = unackedFixture(List(stale), verifier = verifier)

    lookup.answer(unackedTxid, lookup.own)

    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
    kvKeys(backend) should contain (k(2))
    frontend.isQuarantined(oid2) shouldBe true
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(unackedTxid) shouldBe false
    // The store may not hold T's update: its state, not a stub, stays until repair verifies the object
    crl.stubsSaved shouldBe Nil
    crl.deleted shouldBe Nil
    verifier.answer(Success(true))
    frontend.isQuarantined(oid2) shouldBe false
    crl.stubsSaved should contain (unackedTxid) // An applied stub replaces its state (replay-pins addendum A)
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
    lookup.answer(unackedTxid, lookup.absent, Some(lost))
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Unresolved
    lookup.requests.count(_ == unackedTxid) shouldBe 2

    lookup.answer(unackedTxid, lookup.own)
    kvKeys(backend) should not contain k(2)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
  }

  test("a UnackedCommits lookup answering after the Tx resolved committed is ignored") {
    val (frontend, backend, _, _, lookup) = unackedFixture()
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    kvKeys(backend) should not contain k(2)
    frontend.transactions.contains(unackedTxid) shouldBe false

    lookup.answer(unackedTxid, lookup.own)
    frontend.transactions.contains(unackedTxid) shouldBe false
  }

  test("a recovered commit vote that found its entry is forgotten on a later abort without undoing its update") {
    val (frontend, backend, net, crl, lookup) = unackedFixture()
    lookup.answer(unackedTxid, lookup.own)
    kvKeys(backend) should not contain k(2)
    val committedBefore = committedMessages(net).size
    committedBefore should be > 0

    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = false, late = false,
      proof = false))
    frontend.transactions.contains(unackedTxid) shouldBe true // TxFinalized forgets it
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))

    frontend.transactions.contains(unackedTxid) shouldBe false
    kvKeys(backend) should not contain k(2)
    committedMessages(net).size shouldBe committedBefore
    crl.stubsSaved should contain (unackedTxid) // An applied stub replaces its state (replay-pins addendum A)
  }

  test("a recovered commit vote that found its entry is forgotten on a late commit without applying it again") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, _, lookup) = unackedFixture(statusCache = statusCache)
    lookup.answer(unackedTxid, lookup.own)
    val committedBefore = committedMessages(net).size

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = true, owed = Set(),
      startTimestamp = HLCTimestamp.now))

    frontend.transactions.contains(unackedTxid) shouldBe false
    kvKeys(backend) should not contain k(2)
    committedMessages(net).size shouldBe committedBefore
    lookup.requests.count(_ == unackedTxid) shouldBe 1
    statusCache.getStatus(unackedTxid) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  // ---- Late commits (replay-pins design, 1c): applied only by a commit vote whose UnackedCommits entry exists ----

  /** A live Frontend over populated objects, whose op1 commit vote for txid1 is in place, with lookups through `lookup` */
  private def lateFixture(statusCache: TransactionStatusCache = new TransactionStatusCache(),
                          stubs: Set[TransactionId] = Set(),
                          incarnation: Option[StoreIncarnation] = None,
                          acceptorSince: Option[HLCTimestamp] = None,
                          verifier: QuarantineVerifier = QuarantineVerifier.Never,
                          kvState: ObjectState = mkKVObjectState())
      : (Frontend, MapBackend, TestNet, TestCrl, ManualLookup) =
    val backend = new MapBackend(storeId)
    populate(backend, kvState)
    val net = new TestNet
    val crl = new TestCrl:
      override def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] =
        stubs.iterator.map(_ -> stubStart).toMap
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      statusCache, unackedLookup = lookup, acceptorSince = acceptorSince, loadedAt = FrontendSuite.longLoaded,
      incarnationOverride = incarnation, quarantineVerifier = verifier)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    (frontend, backend, net, crl, lookup)

  private def lateCommit(frontend: Frontend, finalized: Boolean): Unit =
    if finalized then
      frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, txid1, committed = true, late = true, owed = Set(),
        startTimestamp = HLCTimestamp.now))
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

    lookup.answer(txid1, lookup.own)
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net).map(_.objectCommitErrors).distinct shouldBe List(Nil)
    crl.stubsSaved should contain (txid1) // An applied stub replaces its state (replay-pins addendum A)
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
    lookup.answer(txid1, lookup.absent)

    backend.get(oid1).get.data.size shouldBe 0
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net) shouldBe Nil
    crl.deleted should contain (txid1)
    lockedWriteTransactions(frontend, net, op1) shouldBe empty
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  // ---- Store incarnation (Todo 2.12, Rebuild N3): an entry applies only at the incarnation it names ----

  private val incarnationA = StoreIncarnation(new UUID(0xA, 1))
  private val incarnationB = StoreIncarnation(new UUID(0xB, 1))

  /** A live commit vote of `frontend` for dataUpdateTxd, its late commit held for a lookup answered with `entry` */
  private def lateCommitAnswered(frontend: Frontend, net: TestNet, lookup: ManualLookup,
                                 entry: Option[StoreIncarnation]): Unit =
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)
    lateCommit(frontend, finalized = false)
    lookup.requests shouldBe List(txid1)
    lookup.answer(txid1, Success(UnackedLookupResult.Found(entry)))

  /** The late commit was concluded committed without applying anything: see lateCommitAnswered */
  private def appliedNothing(frontend: Frontend, backend: MapBackend, net: TestNet, crl: TestCrl,
                             statusCache: TransactionStatusCache): Unit =
    backend.get(oid1).get.data.size shouldBe 0
    committedMessages(net) shouldBe Nil
    frontend.transactions.contains(txid1) shouldBe false
    crl.stubsSaved shouldBe Nil
    crl.deleted should contain (txid1)
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
    lockedWriteTransactions(frontend, net, op1) shouldBe empty

  test("a replay at a rebuilt store applies nothing through an entry for the store's earlier incarnation") {
    // The N3 reproduction: the rebuilt store (incarnation B) inherited the entry its earlier incarnation A was owed.
    // Its acceptorSince is earlier than T's start, so the replayed client prepare is not muted and votes commit
    val statusCache = new TransactionStatusCache()
    val since = HLCTimestamp((System.currentTimeMillis() - 60000) << 16)
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache, incarnation = Some(incarnationB),
      acceptorSince = Some(since))
    frontend.incarnation shouldBe incarnationB
    lateCommitAnswered(frontend, net, lookup, Some(incarnationA))
    appliedNothing(frontend, backend, net, crl, statusCache)
  }

  // ---- A re-applied Insert must not resurrect a key deleted since (transaction-requirements.md 10.5c) ----
  //
  // T inserts key 4 under DoesNotExist(4). A later transaction deleted key 4 after T applied here: the key is absent
  // again and the object's timestamp is past T's start. A re-apply's re-check passes, so the store cannot tell this from
  // a state T never reached, and must not write key 4 back.

  private val insertK4 = KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.DoesNotExist(k(4))))
  private val insertK4Ops: List[KeyValueOperation] = List(new Insert(k(4), Array[Byte](4)))

  /** oid2 holding `keys` at object timestamp `timestamp`: an hour ahead by default, past the start of any T here */
  private def kvAt(timestamp: HLCTimestamp = HLCTimestamp((System.currentTimeMillis() + 3600000L) << 16),
                   keys: Seq[Int] = 1 to 3): ObjectState =
    val contents = keys.map { n =>
      k(n) -> new ValueState(Value(Array[Byte](n.toByte)), rev0, HLCTimestamp(n.toLong), None)
    }.toMap
    new ObjectState(oid2, Metadata(rev0, ObjectRefcount(1, 1), timestamp), ObjectType.KeyValue,
      KVObjectState.encode(Some(k(0)), None, None, None, contents))

  test("a recovered commit vote whose Insert key was deleted since quarantines the object rather than re-insert it") {
    val verifier = new ManualVerifier
    val (frontend, backend, net, crl, lookup) = unackedFixture(List(insertK4), verifier = verifier, ops = insertK4Ops,
      kvState = () => kvAt())
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = true, late = false,
      proof = false))

    kvKeys(backend) should not contain k(4)
    frontend.failStopped shouldBe false
    frontend.isQuarantined(oid2) shouldBe true
    verifier.made.map(_._1) shouldBe List(oid2)
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid2))
    // Its object data stays: a reloaded store recovers the transaction and quarantines again
    crl.dropped shouldBe Nil

    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    crl.deleted shouldBe Nil
    verifier.answer(Success(true))
    frontend.isQuarantined(oid2) shouldBe false
    crl.deleted should contain (unackedTxid)
  }

  test("a recovered commit vote re-applies its Insert where no later write reached the object or the key is present") {
    for (req, state) <- List(
           (insertK4, () => kvAt(HLCTimestamp(1))), // nothing written since: T never applied here
           // A blind Insert of key 4, present: last writer wins settles it
           (KeyValueUpdate(kvPtr, None, None, Nil), () => kvAt(keys = 1 to 4)))
    do
      val verifier = new ManualVerifier
      val (frontend, backend, net, _, _) = unackedFixture(List(req), verifier = verifier, ops = insertK4Ops,
        kvState = state)
      frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, unackedTxid, committed = true,
        late = false, proof = false))
      kvKeys(backend) should contain (k(4))
      frontend.failStopped shouldBe false
      frontend.isQuarantined(oid2) shouldBe false
      verifier.made shouldBe Nil
      committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
  }

  test("a replay applying a late commit through an entry naming no incarnation quarantines an object whose Insert " +
       "key was deleted since") {
    // The store never saw T; repair brought it to the deleting transaction's state, and the replay's vote passed
    val verifier = new ManualVerifier
    val (frontend, backend, net, crl, lookup) = lateFixture(verifier = verifier, kvState = kvAt())
    val txd = lockTxd(txid1, List(insertK4))
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid2, KeyValueOperation.encode(insertK4Ops, ida)(0)))))
    expectVote(net, TransactionDisposition.VoteCommit)
    lateCommit(frontend, finalized = false)
    lookup.answer(txid1, Success(UnackedLookupResult.Found(None)))

    kvKeys(backend) should not contain k(4)
    frontend.failStopped shouldBe false
    frontend.isQuarantined(oid2) shouldBe true
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid2))
    // Neither its state nor a stub is concluded until repair verifies the object
    crl.stubsSaved shouldBe Nil
    crl.deleted shouldBe Nil
    verifier.answer(Success(true))
    frontend.isQuarantined(oid2) shouldBe false
    crl.stubsSaved should contain (txid1)
  }

  test("a live Tx with its own commit vote applies a late commit when its entry's incarnation is unknown") {
    // Its vote was cast by this incarnation: it counted (a takeover driver never heard from this store), or this store
    // never saw T and its vote passed T's pin. T committed either way (transaction-recovery.md section 7)
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache, incarnation = Some(incarnationB))
    lateCommitAnswered(frontend, net, lookup, None)
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    frontend.transactions.contains(txid1) shouldBe false
    committedMessages(net).map(_.objectCommitErrors).distinct shouldBe List(Nil)
    crl.stubsSaved should contain (txid1)
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
    lockedWriteTransactions(frontend, net, op1) shouldBe empty
  }

  test("a held late commit at a store holding a stub applies nothing whatever its entry's incarnation") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache, incarnation = Some(incarnationB))
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)
    lateCommit(frontend, finalized = true)
    lookup.requests shouldBe List(txid1)
    // Recorded while the lookup is in flight
    frontend.recordAppliedStub(txid1, stubStart)(())
    crl.stubsSaved shouldBe List(txid1)

    lookup.answer(txid1, Success(UnackedLookupResult.Found(Some(incarnationA))))

    backend.get(oid1).get.data.size shouldBe 0
    committedMessages(net) shouldBe Nil
    frontend.transactions.contains(txid1) shouldBe false
    frontend.hasAppliedStub(txid1) shouldBe true
    crl.stubsSaved shouldBe List(txid1)
    crl.stubsDeleted shouldBe Nil
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

    lookup.answer(txid1, lookup.own)
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

  test("a Tx that learned its commit normally finalizes on a late commit once no entry names it") {
    val (frontend, backend, net, crl, lookup) = lateFixture()
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, txid1, committed = true, late = false,
      proof = false))
    val committedBefore = committedMessages(net).size
    lateCommit(frontend, finalized = false)

    // Forgotten without a TxFinalized listing owed: it looks up its entry first (replay-pins addendum A, case 3)
    lookup.requests shouldBe List(txid1)
    lookup.answer(txid1, lookup.absent)
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    committedMessages(net).size shouldBe committedBefore
    frontend.transactions.contains(txid1) shouldBe false
    crl.deleted should contain (txid1)
    crl.stubsSaved shouldBe Nil
  }

  test("a late commit of an applied Tx whose lookup never answers is forgotten without a stub once past expiry") {
    // R-N10 (Todo 4.4): the case-3 lookup only decides the stub. Past the horizon the forget floor refuses every new Tx
    // for T here, so a stub could suppress nothing, and the Tx need not wait for the metadata group
    withExpiryFrontend: (frontend, backend, net, crl, lookup, clock, statusCache) =>
      val txd = expiringTxd(List(overwrite(op1)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      expectVote(net, TransactionDisposition.VoteCommit)
      frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, txid1, committed = true, late = false,
        proof = false))
      backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
      lateCommit(frontend, finalized = false)
      lookup.requests shouldBe List(txid1)
      lookup.answer(txid1, scala.util.Failure(new Exception("metadata pool down")))

      frontend.expiryHeartbeat()
      frontend.transactions.contains(txid1) shouldBe true // Not yet past expiry: still held for its lookup

      passHorizon(clock)
      frontend.expiryHeartbeat()
      frontend.transactions.contains(txid1) shouldBe false
      crl.stubsSaved shouldBe Nil
      crl.deleted should contain (txid1)
      frontend.hasAppliedStub(txid1) shouldBe false
      statusCache.getStatus(txid1).map(e => (e.status, e.finalized)) shouldBe Some((TransactionStatus.Committed, true))
      statusRequests(net) shouldBe Nil // Concluded without querying its peers

      // A retry made meanwhile, answered late, changes nothing
      frontend.retryUnackedLookups(System.nanoTime() + Frontend.UnackedLookupTimeout.toNanos + 1)
      lookup.requests shouldBe List(txid1)
      lookup.answer(txid1, lookup.own)
      crl.stubsSaved shouldBe Nil
  }

  // ---- Applied stub expiry (Frontend.expireStubs, Todo 4.6) ----
  //
  // Past the horizon plus the expiry margin no prepare for T creates a Tx here, so a stub held for T, with no Tx for T
  // held, can suppress nothing: the store drops it at heartbeat, whether or not a repair scan ever runs

  test("an applied stub is dropped once its transaction is past the horizon plus the expiry margin") {
    val admission = new CrlAdmission(1024 * 1024)
    withExpiryFrontendAdmitting(admission): (frontend, _, _, crl, _, clock, _) =>
      val start = startedAgo(1_000)
      // Named in `owed` with no Tx here: the stub carries the start TxFinalized names
      frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, txid1, committed = true, late = false,
        owed = Set(storeId), startTimestamp = start))
      frontend.appliedStubs shouldBe Map(txid1 -> start)
      crl.stubsSaved shouldBe List(txid1)
      admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes

      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid1) shouldBe true // Not yet past expiry

      passHorizon(clock)
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid1) shouldBe false
      crl.stubsDeleted shouldBe List(txid1)
      admission.admittedBytes shouldBe 0
  }

  test("an applied stub is kept past expiry while the store holds a Tx for its transaction") {
    withExpiryFrontend: (frontend, _, net, crl, _, clock, _) =>
      val txd = expiringTxd(List(overwrite(op1)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      expectVote(net, TransactionDisposition.VoteCommit)
      // A replay's Tx beside a stub for its transaction: the stub is what keeps it from applying T again
      frontend.recordAppliedStub(txid1, txd.startTimestamp)(())

      passHorizon(clock)
      frontend.expiryHeartbeat()
      frontend.transactions.contains(txid1) shouldBe true
      frontend.hasAppliedStub(txid1) shouldBe true
      crl.stubsDeleted shouldBe Nil
  }

  /** The first wall-clock millisecond at which a stub for a transaction started at `start` may be dropped */
  private def stubDue(start: HLCTimestamp): Long =
    start.wallTime + 20_000 + Frontend.DefaultExpiryMargin.toMillis + 1

  test("applied stubs recorded out of start order expire in start order") {
    withExpiryFrontend: (frontend, _, _, crl, _, clock, _) =>
      val later = startedAgo(1_000)
      val earlier = startedAgo(5_000)
      frontend.recordAppliedStub(txid2, later)(())
      frontend.recordAppliedStub(txid1, earlier)(())

      clock.now = stubDue(earlier)
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid1) shouldBe false
      frontend.hasAppliedStub(txid2) shouldBe true

      clock.now = stubDue(later) - 1
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid2) shouldBe true
      clock.now = stubDue(later)
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid2) shouldBe false
      crl.stubsDeleted shouldBe List(txid1, txid2)
  }

  test("a stub a Tx keeps past its expiry blocks no later stub, and is dropped once the Tx is gone") {
    withExpiryFrontend: (frontend, _, net, crl, lookup, clock, _) =>
      val txd = expiringTxd(List(overwrite(op1)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      expectVote(net, TransactionDisposition.VoteCommit)
      frontend.recordAppliedStub(txid1, txd.startTimestamp)(())
      val later = startedAgo(500)
      frontend.recordAppliedStub(txid2, later)(())

      clock.now = stubDue(later)
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid1) shouldBe true // Its Tx is held
      frontend.hasAppliedStub(txid2) shouldBe false

      frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, txid1, committed = false, late = false,
        owed = Set(), startTimestamp = txd.startTimestamp))
      // Expiring, the commit vote holds the abort until a lookup finds no entry
      lookup.answer(txid1, lookup.absent)
      frontend.transactions.contains(txid1) shouldBe false
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid1) shouldBe false
      crl.stubsDeleted shouldBe List(txid2, txid1)
  }

  test("a stub dropped and recorded again expires by its new start, not its old queue entry") {
    withExpiryFrontend: (frontend, _, _, crl, _, clock, _) =>
      val first = startedAgo(5_000)
      frontend.recordAppliedStub(txid1, first)(())
      frontend.dropAppliedStub(txid1)
      val second = startedAgo(1_000)
      frontend.recordAppliedStub(txid1, second)(())

      clock.now = stubDue(first)
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid1) shouldBe true // The old entry is stale

      clock.now = stubDue(second)
      frontend.expiryHeartbeat()
      frontend.hasAppliedStub(txid1) shouldBe false
      crl.stubsDeleted shouldBe List(txid1, txid1)
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

  // ---- Applied stubs (replay-pins addendum A): a store that applied T never applies it again ----

  /** A non-late committed TxFinalized of `txid` listing `owed` */
  private def owedFinalized(txid: TransactionId, owed: Set[StoreId] = Set(storeId)): TxFinalized =
    TxFinalized(storeId, unackedDriver, txid, committed = true, late = false, owed = owed,
      startTimestamp = HLCTimestamp.now)

  test("a TxFinalized naming this store in owed writes a stub before deleting the transaction's state") {
    val (frontend, backend, net, crl, _) = lateFixture()
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)

    frontend.receiveTransactionMessage(owedFinalized(txid1))

    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    frontend.transactions.contains(txid1) shouldBe false
    frontend.hasAppliedStub(txid1) shouldBe true
    // The stub's save replaces the transaction's state: it is not deleted first
    crl.stubsSaved shouldBe List(txid1)
    crl.deleted should not contain (txid1)

    // Not named: deleted, with no stub
    val (other, _, otherNet, otherCrl, _) = lateFixture()
    other.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(otherNet, TransactionDisposition.VoteCommit)
    other.receiveTransactionMessage(owedFinalized(txid1, owed = Set(StoreId(poolId, 1))))
    otherCrl.stubsSaved shouldBe Nil
    otherCrl.deleted should contain (txid1)
    other.hasAppliedStub(txid1) shouldBe false
  }

  test("a stub owed by TxFinalized waits for the backend commits, and a failed commit writes none") {
    val crl = new TestCrl
    val (frontend, backend, _) = heldCommit(crl)
    frontend.receiveTransactionMessage(owedFinalized(txid1))
    frontend.transactions.contains(txid1) shouldBe false
    crl.stubsSaved shouldBe Nil
    backend.deliverCommits()
    crl.stubsSaved shouldBe List(txid1)
    crl.deleted shouldBe Nil

    // A failed commit keeps the CRL state (object data included) while the store is loaded: no stub replaces it
    val failedCrl = new TestCrl
    val (failed, failedBackend, _) = heldCommit(failedCrl)
    failed.receiveTransactionMessage(owedFinalized(txid1))
    failedBackend.deliverCommits(Right(CommitError.IOFailure))
    failedCrl.stubsSaved shouldBe Nil
    failedCrl.deleted shouldBe Nil
    failed.hasAppliedStub(txid1) shouldBe false
  }

  test("a TxFinalized naming this store with no Tx writes a stub") {
    val (frontend, _, _, crl, _) = lateFixture()
    frontend.receiveTransactionMessage(owedFinalized(txid1, owed = Set(StoreId(poolId, 1))))
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, txid1, committed = true, late = true,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, txid3, committed = false, late = false,
      owed = Set(storeId), startTimestamp = HLCTimestamp.now))
    crl.stubsSaved shouldBe Nil

    frontend.receiveTransactionMessage(owedFinalized(txid2))
    crl.stubsSaved shouldBe List(txid2)
    frontend.hasAppliedStub(txid2) shouldBe true
    frontend.transactions.contains(txid2) shouldBe false
    // A retransmit writes nothing more
    frontend.receiveTransactionMessage(owedFinalized(txid2))
    crl.stubsSaved shouldBe List(txid2)

    // A second driver's TxFinalized naming this store after its Tx was forgotten with its backend commit outstanding:
    // the stub waits for the commit
    val heldCrl = new TestCrl
    val (held, heldBackend, _) = heldCommit(heldCrl)
    finalizeTx1(held)
    held.receiveTransactionMessage(owedFinalized(txid1))
    heldCrl.stubsSaved shouldBe Nil
    heldBackend.deliverCommits()
    heldCrl.stubsSaved shouldBe List(txid1)
    heldCrl.deleted shouldBe Nil
  }

  test("applying through a found entry writes a stub") {
    // A recovered commit vote: kept until TxFinalized (Ruling 9), which writes the stub though it lists no one
    val (frontend, backend, _, crl, lookup) = unackedFixture()
    lookup.answer(unackedTxid, lookup.own)
    kvKeys(backend) should not contain k(2)
    crl.stubsSaved shouldBe Nil
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(unackedTxid) shouldBe false
    crl.stubsSaved shouldBe List(unackedTxid)
    crl.deleted should not contain (unackedTxid)

    // A held late commit
    val (late, lateBackend, lateNet, lateCrl, lateLookup) = lateFixture()
    late.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(lateNet, TransactionDisposition.VoteCommit)
    lateCommit(late, finalized = true)
    lateLookup.answer(txid1, lateLookup.own)
    lateBackend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    late.transactions.contains(txid1) shouldBe false
    lateCrl.stubsSaved shouldBe List(txid1)
    lateCrl.deleted should not contain (txid1)
  }

  test("a Tx forgotten after a late resolution looks up its entry and writes a stub when found") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache)
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    frontend.receiveTransactionMessage(TxResolved(storeId, unackedDriver, txid1, committed = true, late = false,
      proof = false))
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    val committedBefore = committedMessages(net).size

    // Its TxFinalized was lost; a late outcome lists no owed stores, so the store asks the tree
    lateCommit(frontend, finalized = false)
    lookup.requests shouldBe List(txid1)
    frontend.transactions.contains(txid1) shouldBe true
    lateCommit(frontend, finalized = true) // A retransmit makes no second lookup
    lookup.requests shouldBe List(txid1)

    // A failed lookup holds it, and is retried at heartbeat
    lookup.answer(txid1, Failure(new Exception("metadata pool down")))
    frontend.transactions.contains(txid1) shouldBe true
    frontend.retryUnackedLookups()
    lookup.requests shouldBe List(txid1, txid1)

    lookup.answer(txid1, lookup.own)
    frontend.transactions.contains(txid1) shouldBe false
    crl.stubsSaved shouldBe List(txid1)
    crl.deleted should not contain (txid1)
    committedMessages(net).size shouldBe committedBefore
    backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
  }

  test("a late commit whose entry is found applies nothing when a stub exists") {
    val statusCache = new TransactionStatusCache()
    val (frontend, backend, net, crl, lookup) = lateFixture(statusCache, stubs = Set(txid1))
    frontend.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(net, TransactionDisposition.VoteCommit)

    lateCommit(frontend, finalized = true)
    // Whatever the tree holds: the stub already says the store holds the update
    lookup.requests shouldBe Nil
    backend.get(oid1).get.data.size shouldBe 0
    committedMessages(net) shouldBe Nil
    frontend.transactions.contains(txid1) shouldBe false
    lockedWriteTransactions(frontend, net, op1) shouldBe empty
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))

    // A stub recorded while the lookup is in flight is honoured when the entry is found
    val (inFlight, inFlightBackend, inFlightNet, _, inFlightLookup) = lateFixture()
    inFlight.receiveTransactionMessage(clientPrepare(dataUpdateTxd))
    expectVote(inFlightNet, TransactionDisposition.VoteCommit)
    lateCommit(inFlight, finalized = true)
    inFlightLookup.requests shouldBe List(txid1)
    inFlight.recordAppliedStub(txid1, stubStart)(())
    inFlightLookup.answer(txid1, inFlightLookup.own)
    inFlightBackend.get(oid1).get.data.size shouldBe 0
    committedMessages(inFlightNet) shouldBe Nil
    inFlight.transactions.contains(txid1) shouldBe false
  }

  test("a recovered commit vote with a stub applies nothing") {
    val (frontend, backend, net, crl, lookup) = unackedFixture(stubs = Set(unackedTxid))
    frontend.hasAppliedStub(unackedTxid) shouldBe true
    lookup.answer(unackedTxid, lookup.own)

    kvKeys(backend) should contain (k(2))
    // Its TxCommitted reports the object it did not change, so a driver's finalizer marks it for repair
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid2))
    // Still an acceptor: it is forgotten only on TxFinalized, as a recovered vote that found its entry is (Ruling 9)
    frontend.transactions(unackedTxid).status shouldBe TransactionStatus.Committed
    frontend.transactions(unackedTxid).provesCommitToItsDriver shouldBe true
    lockedWriteTransactions(frontend, net, kvPtr) should not contain (unackedTxid)
    frontend.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = false,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(unackedTxid) shouldBe false
    kvKeys(backend) should contain (k(2))
    crl.deleted should contain (unackedTxid)

    // A late commit reaching it applies nothing either
    val (late, lateBackend, _, _, lateLookup) = unackedFixture(stubs = Set(unackedTxid))
    late.receiveTransactionMessage(TxFinalized(storeId, unackedDriver, unackedTxid, committed = true, late = true,
      owed = Set(), startTimestamp = HLCTimestamp.now))
    lateLookup.answer(unackedTxid, lateLookup.own)
    kvKeys(lateBackend) should contain (k(2))
    late.transactions.contains(unackedTxid) shouldBe false
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
  def commitWindowFixture(cacheBytes: Long = 2 * HostObjectCache.EntryOverhead, crl: CrashRecoveryLog = new TestCrl,
                          statusCache: TransactionStatusCache = new TransactionStatusCache(),
                          wallClockMillis: () => Long = () => System.currentTimeMillis())
      : (Frontend, DeferredCommitBackend, StoreObjectCache, TestNet) =
    val backend = new DeferredCommitBackend
    populate(backend)
    for p <- others do
      val os = mkObjectState(p.id)
      backend.bootstrapAllocate(p.id, os.objectType, os.metadata, os.data)
    val cache = new HostObjectCache(cacheBytes).forStore(storeId)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache, loadedAt = FrontendSuite.longLoaded,
                                wallClockMillis = wallClockMillis)
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
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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
    prepare(frontend, net, lockTxd(txid1, List(ExclusiveRevisionLock(fencePtr, rev0)))).disposition shouldBe
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

  /** A commit of op1 holding `b`, at the revision `txid` writes, without a Tx */
  def bareCommit(frontend: Frontend, txid: TransactionId, b: Byte): Unit =
    val metadata = Metadata(ObjectRevision(txid), ObjectRefcount(1, 1), HLCTimestamp(200))
    val data = DataBuffer(Array[Byte](b))
    frontend.commit(new ObjectState(oid1, metadata, ObjectType.Data, data),
      CommitState(op1, metadata, ObjectType.Data, data), txid)

  test("two in-flight commits of one object serve the newest state") {
    val (frontend, backend, _, net) = commitWindowFixture()
    bareCommit(frontend, txid1, 1)
    bareCommit(frontend, txid2, 2)
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

  // ---- Replayed allocations (transaction-requirements F11, cross-document X11) ----

  /** txid's allocation of the data object `ptr`, as TransactionBuilder makes it */
  def allocateData(txid: TransactionId, ptr: DataObjectPointer): TransactionDescription =
    lockTxd(txid, List(
      RefcountUpdate(ptr, ObjectRefcount.Allocating, ObjectRefcount.Default),
      DataUpdate(ptr, ObjectRevision.Allocating, DataUpdateOperation.Overwrite))).copy(
      allocatingObjects = Set(ptr.id))

  test("an allocation of an object the store does not hold votes commit and applies") {
    val (frontend, backend, net) = lockFixture()
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 31)), poolId, Array[Byte]())
    prepare(frontend, net, allocateData(txid1, newPtr)).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    val stored = backend.get(newPtr.id).get
    stored.metadata.revision shouldBe ObjectRevision(txid1)
    stored.metadata.refcount shouldBe ObjectRefcount.Default
    stored.data.getByteArray shouldBe Array[Byte](1)
  }

  // transaction-requirements Q-N5: the finalizer, and the client that learned the commit from the votes, read a new
  // object as soon as its allocation resolves, possibly before the stores apply it. A commit voter's fresh state is
  // known only to its Tx, so the read misses; answering ObjectNotFound there would let a read of a live, committed
  // object prove it deleted. The read is parked until the allocation applies (or aborts), and then answered
  test("a read of an allocation voted for but not applied is answered once it applies") {
    val (frontend, backend, net) = lockFixture()
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 32)), poolId, Array[Byte]())
    def read(n: Int): Option[Either[ReadError.Value, ReadResponse.CurrentState]] =
      frontend.readObjectForNetwork(clientId, new UUID(2, n), newPtr, MetadataOnly())
      takeReadResponse(net).map(_.result)

    read(1) shouldBe Some(Left(ReadError.ObjectNotFound))
    prepare(frontend, net, allocateData(txid1, newPtr)).disposition shouldBe TransactionDisposition.VoteCommit
    read(2) shouldBe None
    resolve(frontend, net, txid1, committed = true)
    val parked = takeReadResponse(net).get
    parked.readUUID shouldBe new UUID(2, 2)
    parked.result.map(_.revision) shouldBe Right(ObjectRevision(txid1))
    read(3).map(_.map(_.revision)) shouldBe Some(Right(ObjectRevision(txid1)))

    // Deleted while the allocating Tx is still held (not finalized): an absence again, not a pending allocation
    val delete = RefcountUpdate(newPtr, ObjectRefcount.Default,
      ObjectRefcount(ObjectRefcount.Default.updateSerial + 1, 0))
    prepare(frontend, net, lockTxd(txid2, List(delete))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid2, committed = true)
    read(4).get match
      case Left(err) => err shouldBe ReadError.ObjectNotFound
      case Right(cs) => cs.refcount.count shouldBe 0
  }

  // A client's backend load of the object can be in flight when the allocation's commit applies in memory (a real
  // backend completes reads later, on another thread). Its miss then predates the committed state, which is what the
  // read must get: no Tx has the allocation pending any more, so the miss would read as a plain absence
  test("a backend miss for a read that completes after the allocation applied serves the committed state") {
    val backend = new DeferredCommitBackend
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, new TestCrl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 36)), poolId, Array[Byte]())
    prepare(frontend, net, allocateData(txid1, newPtr)).disposition shouldBe TransactionDisposition.VoteCommit
    backend.deferReads = true
    frontend.readObjectForNetwork(clientId, new UUID(2, 1), newPtr, MetadataOnly())
    takeReadResponse(net) shouldBe None
    resolve(frontend, net, txid1, committed = true)
    takeReadResponse(net) shouldBe None
    backend.deliverReads()
    takeReadResponse(net).map(_.result.map(_.revision)) shouldBe Some(Right(ObjectRevision(txid1)))
  }

  test("a parked read of an allocation is answered ObjectNotFound when the allocation aborts") {
    val (frontend, _, net) = lockFixture()
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 34)), poolId, Array[Byte]())
    prepare(frontend, net, allocateData(txid1, newPtr)).disposition shouldBe TransactionDisposition.VoteCommit
    frontend.readObjectForNetwork(clientId, new UUID(2, 1), newPtr, MetadataOnly())
    takeReadResponse(net) shouldBe None
    resolve(frontend, net, txid1, committed = false)
    takeReadResponse(net).map(_.result) shouldBe Some(Left(ReadError.ObjectNotFound))
  }

  test("a read of an aborted allocation answers ObjectNotFound") {
    val (frontend, _, net) = lockFixture()
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 33)), poolId, Array[Byte]())
    prepare(frontend, net, allocateData(txid1, newPtr)).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = false)
    frontend.readObjectForNetwork(clientId, new UUID(2, 1), newPtr, MetadataOnly())
    takeReadResponse(net).get.result shouldBe Left(ReadError.ObjectNotFound)
  }

  test("a replayed allocation over a live object in the backend votes abort, and its commit leaves the object alone") {
    val (frontend, backend, net, stops) = failStopFixture()
    prepare(frontend, net, allocateData(txid1, op1)).disposition shouldBe TransactionDisposition.VoteAbort
    // Committed by stores that still voted on a fresh state: this store applies nothing, and reports the object
    resolve(frontend, net, txid1, committed = true)
    val stored = backend.get(oid1).get
    stored.metadata.revision shouldBe rev0
    stored.metadata.refcount shouldBe ObjectRefcount(1, 1)
    stored.data.size shouldBe 0
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
    stops() shouldBe Nil
    // The live object is still usable
    prepare(frontend, net, lockTxd(txid2, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
  }

  test("a replayed allocation over a cached live object votes abort") {
    val (frontend, backend, net) = lockFixture()
    frontend.readObjectForNetwork(clientId, new UUID(1, 31), op1, FullObject())
    net.clientMessage().isDefined shouldBe true
    prepare(frontend, net, allocateData(txid1, op1)).disposition shouldBe TransactionDisposition.VoteAbort
    resolve(frontend, net, txid1, committed = true)
    backend.get(oid1).get.metadata.revision shouldBe rev0
  }

  test("a replayed KV allocation over a never-revised live KV object votes abort, which its requirements alone allow") {
    // A KV allocation is a KeyValueUpdate with no required revision and a RefcountUpdate from Allocating to the
    // default initial refcount, (0,1), which is Allocating too. A KV object that only blind updates touched since its
    // allocation keeps revision Allocating and refcount (0,1): every requirement of the replay holds on it
    val (frontend, backend, net) = lockFixture()
    val live = mkKVObjectState()
    backend.bootstrapAllocate(oid2, live.objectType,
      Metadata(ObjectRevision.Allocating, ObjectRefcount.Allocating, HLCTimestamp(1)), live.data)
    val txd = lockTxd(txid1, List(
      KeyValueUpdate(kvPtr, None, None, Nil),
      RefcountUpdate(kvPtr, ObjectRefcount.Allocating, ObjectRefcount.Allocating))).copy(
      allocatingObjects = Set(oid2))
    val initial = KeyValueOperation.encode(List(new Insert(k(9), Array[Byte](9))), ida)(0)
    prepare(frontend, net, txd, initial).disposition shouldBe TransactionDisposition.VoteAbort
    resolve(frontend, net, txid1, committed = true)
    kvKeys(backend) shouldBe Set(k(1), k(2), k(3))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid2))
  }

  test("an allocation over an object whose deletion is in flight votes abort") {
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024)
    val delete = RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 0))
    prepare(frontend, net, lockTxd(txid1, List(delete))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    frontend.receiveTransactionMessage(mkPrepare(allocateData(txid2, op1), fromClient = true).copy(
      objectUpdates = List(ObjectUpdate(oid1, DataBuffer(Array[Byte](1))))))
    backend.deliverCommits()
    votes(net).filter(_.transactionId == txid2).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
    resolve(frontend, net, txid2, committed = true)
    backend.get(oid1) shouldBe None
  }

  test("a replayed allocation reaching a late commit at a store holding its entry applies nothing (X11 step 3)") {
    val (frontend, backend, net, _, lookup) = lateFixture()
    frontend.receiveTransactionMessage(clientPrepare(allocateData(txid1, op1)))
    expectVote(net, TransactionDisposition.VoteAbort)
    lateCommit(frontend, finalized = true)
    // An abort voter applies no late commit: it makes no lookup, and its entry stays for the repair scan
    lookup.requests shouldBe Nil
    frontend.transactions.contains(txid1) shouldBe false
    backend.get(oid1).get.metadata.revision shouldBe rev0
    backend.get(oid1).get.data.size shouldBe 0
  }

  test("a recovered allocating commit vote whose update the backend already holds neither reports nor fail-stops") {
    val applied = mkObjectState(oid1)
    applied.metadata = Metadata(ObjectRevision(txid1), ObjectRefcount.Default, HLCTimestamp(1))
    val (frontend, net, stops) = recoveredVote(applied, txd = allocateData(txid1, op1))
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
    stops() shouldBe Nil
  }

  // ---- Recovered transactions whose objects fail to load (transaction-recovery R-F17) ----

  test("a recovered commit vote whose object is not found applies nothing, reports it, and quarantines nothing") {
    // ObjectNotFound for an object the vote does not allocate: before the crash the store applied T and a later
    // transaction deleted the object (its lock admits nothing else). The finalizer marks the object, and repair finds
    // it deleted
    val (frontend, net, stops) = recoveredVote(mkObjectState(oid1), Some(ReadError.ObjectNotFound))
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(oid1))
    stops() shouldBe Nil
    frontend.isQuarantined(oid1) shouldBe false
  }

  test("a recovered allocating commit vote whose object the backend misses creates it from its saved data") {
    // The crash came before the allocation's backend commit: the miss is a fresh state, not a load failure
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 34)), poolId, Array[Byte]())
    val (frontend, net, stops) = recoveredVote(mkObjectState(oid1), txd = allocateData(txid1, newPtr),
      updates = List(ObjectUpdate(newPtr.id, DataBuffer(Array[Byte](7)))))
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(Nil)
    stops() shouldBe Nil
    readData(frontend, net, newPtr) shouldBe Right(List[Byte](7))
  }

  test("a recovered allocating commit vote without its data whose object was deleted reports it and quarantines " +
       "nothing") {
    // The CRL dropped the data once the allocation applied; a later transaction then deleted the object. A quarantine
    // could never clear: no read verifies a deleted object
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 35)), poolId, Array[Byte]())
    val (frontend, net, stops) = recoveredVote(mkObjectState(oid1), txd = allocateData(txid1, newPtr), updates = Nil)
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(newPtr.id))
    stops() shouldBe Nil
    frontend.isQuarantined(newPtr.id) shouldBe false
    frontend.testingOnlyQuarantines shouldBe 0
    readData(frontend, net, newPtr) shouldBe Left(ReadError.ObjectNotFound)
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

    done.future.value.get.get shouldBe ReconcileResult(Set(k(1)), false, false, changed = true)
    val stored = backend.m(oid2).kvState.get.content
    stored.keySet shouldBe Set(k(1), k(2), k(4))
    stored(k(1)).revision shouldBe rev0
    stored(k(4)).revision shouldBe ObjectRevision(txid2)
  }

  def kvRead(keys: Range): org.aspen_ddp.aspen.client.KeyValueObjectState =
    def vs(n: Int) = org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState(Value(Array[Byte](n.toByte)), rev0,
      HLCTimestamp(n.toLong))
    new org.aspen_ddp.aspen.client.KeyValueObjectState(kvPtr, rev0, ObjectRefcount(1, 1),
      HLCTimestamp(1), HLCTimestamp(1), ida, Some(k(0)), None, None, None, keys.map(n => k(n) -> vs(n)).toMap)

  test("reconciling a store that matches the read reports changed = false") {
    val (frontend, backend, _) = lockFixture()
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, kvPtr), kvRead(1 to 3), done)
    val result = done.future.value.get.get
    result.complete shouldBe true
    result.changed shouldBe false
    backend.m(oid2).kvState.get.content.keySet shouldBe (1 to 3).map(k).toSet
  }

  test("reconciling a store missing a key reports changed = true") {
    val (frontend, backend, _) = lockFixture()
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, kvPtr), kvRead(1 to 4), done)
    val result = done.future.value.get.get
    result.complete shouldBe true
    result.changed shouldBe true
    backend.m(oid2).kvState.get.content.keySet shouldBe (1 to 4).map(k).toSet
  }
  test("a failed repair write restores the state a live commit voter holds locked, which then commits normally") {
    var stops = List[Throwable]()
    val backend = new MapBackend(storeId):
      override def repair(state: CommitState, complete: Promise[Unit]): Unit =
        complete.failure(new Exception("repair slot occupied"))
    populate(backend)
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, new TestCrl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded, onFailStop = err => stops = stops :+ err)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))

    // A live commit voter rewrites key 1
    val update = KeyValueUpdate(kvPtr, None, None, List(KeyValueUpdate.KeyRevision(k(1), rev0)))
    prepare(frontend, net, lockTxd(txid1, List(update)), insertK(1)).disposition shouldBe
      TransactionDisposition.VoteCommit
    net.clientMessage() // txid1's vote to its client

    // Repair installs key 4 into the state txid1 holds locked (key 1 is deferred), and the write fails
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, kvPtr), kvRead(1 to 4), done)
    done.future.value.get.isFailure shouldBe true
    frontend.readObjectForNetwork(clientId, new UUID(1, 1), kvPtr, FullObject())
    readResponseKeys(net)._1 shouldBe Set(1, 2, 3)

    // The voter's state was restored, not discarded: its commit applies
    frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
    committedMessages(net).filter(_.transactionId == txid1).flatMap(_.objectCommitErrors) shouldBe Nil
    stops shouldBe Nil
    frontend.isQuarantined(oid2) shouldBe false
    val stored = backend.m(oid2).kvState.get.content
    stored.keySet shouldBe Set(k(1), k(2), k(3))
    stored(k(1)).revision shouldBe ObjectRevision(txid1)
  }


  /** A read of op1 newer than anything the fixtures commit, at revision txid2 */
  def newerRepairState(): DataObjectState =
    val ts = HLCTimestamp((System.currentTimeMillis() + 1_000L) << 16)
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

  // Repair F8: a repair asked to wait for its object to settle parks while the object is busy
  test("a repair asked to wait for its object to settle installs once the in-flight commit completes") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1

    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, op1).copy(awaitSettle = true), newerRepairState(), done)
    done.isCompleted shouldBe false
    backend.repairs shouldBe 0

    backend.deliverCommits()
    val result = done.future.value.get.get
    result.complete shouldBe true
    result.changed shouldBe true
    backend.repairs shouldBe 1
    backend.m(oid1).metadata.revision shouldBe ObjectRevision(txid2)
  }

  test("a parked repair over a part the completing commit changed answers stale and writes nothing") {
    val (frontend, backend, _, net) = commitWindowFixture()
    // Taken before the commit: the commit changes the revision the snapshot recorded
    val snapshot = repairSnapshot(frontend, op1)
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)

    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(snapshot.copy(awaitSettle = true), newerRepairState(), done)
    done.isCompleted shouldBe false
    backend.deliverCommits()
    done.future.value.get.get.stale shouldBe true
    backend.repairs shouldBe 0
  }

  test("a parked repair whose object never settles answers stale at its deadline") {
    val now = new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis())
    val (frontend, backend, _, net) = commitWindowFixture(wallClockMillis = () => now.get)
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)

    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, op1).copy(awaitSettle = true), newerRepairState(), done)
    frontend.expireParkedRepairs()
    done.isCompleted shouldBe false

    now.addAndGet(Frontend.RepairSettleWait.toMillis)
    frontend.expireParkedRepairs()
    done.future.value.get.get.stale shouldBe true
    // The commit completing later finds nothing parked
    backend.deliverCommits()
    backend.repairs shouldBe 0
  }

  test("fail-stop fails a parked repair") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)

    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, op1).copy(awaitSettle = true), newerRepairState(), done)
    frontend.failStop("test")
    done.future.value.get.isFailure shouldBe true
    backend.repairs shouldBe 0
  }

  // Repair F6: a missed deletion's repair must not race a read, a commit or a transaction on the object
  def refusedRepairDelete(frontend: Frontend, oid: ObjectId): Boolean =
    val completion = Promise[Unit]()
    frontend.deleteObjectForRepair(oid, ObjectPointer.EmptyArray, completion)
    completion.future.value match
      case Some(scala.util.Failure(_: Frontend.RepairDeleteDeferred)) => true
      case other => throw new AssertionError(s"Expected a refusal, got $other")

  test("a repair deletion is refused while a backend read of the object is pending") {
    val (frontend, backend, _, net) = commitWindowFixture()
    backend.deferReads = true
    frontend.readObjectForNetwork(clientId, new UUID(1, 1), op1, FullObject())
    refusedRepairDelete(frontend, oid1) shouldBe true
    backend.deliverReads()
    net.clientMessage() match
      case Some(rr: ReadResponse) => rr.result.isRight shouldBe true
      case other => throw new AssertionError(s"Expected a ReadResponse, got $other")
    backend.m.contains(oid1) shouldBe true
  }

  test("a repair deletion is refused while a commit of the object is in flight, or a transaction holds it") {
    val (frontend, backend, _, net) = commitWindowFixture()
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    // Locked by the commit vote
    refusedRepairDelete(frontend, oid1) shouldBe true
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    refusedRepairDelete(frontend, oid1) shouldBe true
    backend.m.contains(oid1) shouldBe true
  }

  test("a repair write in flight refuses a repair deletion of the object") {
    val (frontend, backend, _, _) = commitWindowFixture()
    backend.deferRepairs = true
    val done = Promise[ReconcileResult]()
    frontend.reconcileForRepair(repairSnapshot(frontend, op1), newerRepairState(), done)
    backend.repairs shouldBe 1
    refusedRepairDelete(frontend, oid1) shouldBe true
  }

  test("while a repair deletion is outstanding the object reads as not found, and is not cached") {
    val (frontend, backend, cache, net) = commitWindowFixture()
    backend.deferRepairDeletes = true
    val completion = Promise[Unit]()
    frontend.deleteObjectForRepair(oid1, ObjectPointer.EmptyArray, completion)
    completion.isCompleted shouldBe false
    backend.m.contains(oid1) shouldBe true

    readData(frontend, net, op1) shouldBe Left(ReadError.ObjectNotFound)
    repairSnapshot(frontend, op1).isMissing shouldBe true
    cache.get(oid1) shouldBe None
    // A second repair deletion waits for the first
    refusedRepairDelete(frontend, oid1) shouldBe true

    backend.deliverRepairDeletes()
    completion.future.value shouldBe Some(scala.util.Success(()))
    backend.m.contains(oid1) shouldBe false
    readData(frontend, net, op1) shouldBe Left(ReadError.ObjectNotFound)
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
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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

    // A commit: the read may return the state it replaces, which is then not the state served
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
  def heldCommit(crl: CrashRecoveryLog, statusCache: TransactionStatusCache = new TransactionStatusCache())
      : (Frontend, DeferredCommitBackend, TestNet) =
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024, crl = crl,
      statusCache = statusCache)
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    (frontend, backend, net)

  /** A status cache whose size evictions leave the forget floor below the transactions here: its clock reads the
    * epoch, so the eviction watermark stays decades in the past. A replay after an eviction is otherwise refused
    * (Frontend.forgetFloorMillis), so the replay paths tested with it are defensive: they hold should a replay ever
    * reach a store that forgot its transaction
    */
  def replayableStatusCache(): TransactionStatusCache = new TransactionStatusCache(wallClockMillis = () => 0L)

  def finalizeTx1(frontend: Frontend): Unit =
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))

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
    override def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = crl.getAppliedStubs(storeId)
    override def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
      crl.saveAppliedStub(storeId, txid, stubStart, completionHandler)
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
    override def whenDurable(storeId: StoreId, handler: () => Unit): Unit =
      val done = new LinkedBlockingQueue[java.lang.Boolean]()
      crl.whenDurable(storeId, () => done.put(true))
      if done.poll(10, java.util.concurrent.TimeUnit.SECONDS) eq null then
        fail("SimpleCRL did not complete a durability barrier within 10 seconds")
      handler()

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

  // ---- A deletion is ordered after its allocation's CRL data is durably gone (R-N13, Todo 2.17) ----

  /** Removes an object whose commit sets its refcount to 0, as the RocksDB and FSIF backends do (MapBackend keeps the
    * zero-refcount state, which a recovered allocation's load would find and refuse) */
  class DeletingMapBackend extends MapBackend(storeId):
    private var handler: Option[CompletionHandler] = None
    override def setCompletionHandler(h: CompletionHandler): Unit =
      handler = Some(h)
      super.setCompletionHandler(h)
    override def commit(state: CommitState, transactionId: TransactionId): Unit =
      if state.metadata.refcount.count == 0 then
        repairDelete(state.objectId, Array(), Promise[Unit]())
        handler.foreach(_.complete(org.aspen_ddp.aspen.server.store.backend.Commit(storeId, state.objectId,
          transactionId, Left(()))))
      else
        super.commit(state, transactionId)

  test("a recovered allocation does not re-create an object deleted after its apply (R-N13)") {
    val tdir = new TempDirManager
    try
      val newPtr = DataObjectPointer(ObjectId(new UUID(0, 37)), poolId, Array[Byte]())
      val backend = new DeletingMapBackend
      val crl = new SyncCrl(SimpleCRL(tdir.tdir, 3, 1024 * 1024).crl)
      try
        val net = new TestNet
        val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
          new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
        backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
        // txid1 allocates the object and applies; its finalization is still running when txid2 deletes it
        prepare(frontend, net, allocateData(txid1, newPtr)).disposition shouldBe TransactionDisposition.VoteCommit
        resolve(frontend, net, txid1, committed = true)
        backend.get(newPtr.id).isDefined shouldBe true
        val delete = RefcountUpdate(newPtr, ObjectRefcount.Default,
          ObjectRefcount(ObjectRefcount.Default.updateSerial + 1, 0))
        prepare(frontend, net, lockTxd(txid2, List(delete))).disposition shouldBe TransactionDisposition.VoteCommit
        resolve(frontend, net, txid2, committed = true)
        backend.get(newPtr.id) shouldBe None
      finally
        crl.crl.shutdown()
      // The crash: the store reloads from the CRL's files, with txid1 unfinalized
      val reopened = SimpleCRL(tdir.tdir, 3, 1024 * 1024)
      try
        val recovered = reopened.trsList.filter(trs =>
          TransactionDescription.deserialize(trs.serializedTxd).transactionId == txid1)
        recovered.size shouldBe 1
        val net = new TestNet
        val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net,
          new TestCrl, new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded, deferRecovery = true)
        backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
        frontend.recover(StoreRecoveryState(recovered, Map()))
        frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false,
          proof = false))
        backend.get(newPtr.id) shouldBe None
        committedMessages(net).map(_.objectCommitErrors) shouldBe List(List(newPtr.id))
        frontend.isQuarantined(newPtr.id) shouldBe false
      finally
        reopened.crl.shutdown()
    finally
      tdir.delete()
  }

  /** A store whose txid1 allocated `newPtr` and applied it, with txid2's deletion of it voted for */
  def allocatedThenDeleting(newPtr: DataObjectPointer)
      : (Frontend, DeletingMapBackend, TestCrl, TestNet) =
    val backend = new DeletingMapBackend
    populate(backend)
    val crl = new TestCrl
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    prepare(frontend, net, allocateData(txid1, newPtr)).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.get(newPtr.id).isDefined shouldBe true
    val delete = RefcountUpdate(newPtr, ObjectRefcount.Default,
      ObjectRefcount(ObjectRefcount.Default.updateSerial + 1, 0))
    prepare(frontend, net, lockTxd(txid2, List(delete))).disposition shouldBe TransactionDisposition.VoteCommit
    (frontend, backend, crl, net)

  test("a deletion waits for its unconcluded allocation's CRL state to be saved without the object's update") {
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 38)), poolId, Array[Byte]())
    val (frontend, backend, crl, net) = allocatedThenDeleting(newPtr)
    crl.holdTxSaves = true
    resolve(frontend, net, txid2, committed = true)
    backend.get(newPtr.id).isDefined shouldBe true // The deletion is held
    val resaved = crl.savedStates.last
    TransactionDescription.deserialize(resaved.serializedTxd).transactionId shouldBe txid1
    resaved.objectUpdates shouldBe Nil
    // Served as deleted meanwhile
    readData(frontend, net, newPtr) shouldBe Left(ReadError.ObjectNotFound)

    crl.completeHeldTxSaves()
    backend.get(newPtr.id) shouldBe None
    // The update is gone from txid1's state for good: its later saves leave it out
    frontend.receiveTransactionMessage(TxAccept(storeId, storeId, txid1, ProposalId(9, 0), value = true))
    crl.completeHeldTxSaves()
    crl.savedStates.last.objectUpdates shouldBe Nil
  }

  test("a deletion after its allocation was finalized waits until the allocation's CRL deletion is durable") {
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 39)), poolId, Array[Byte]())
    val (frontend, backend, crl, net) = allocatedThenDeleting(newPtr)
    crl.holdBarriers = true
    finalizeTx1(frontend)
    crl.deleted shouldBe List(txid1)
    val saves = crl.savedStates.size
    resolve(frontend, net, txid2, committed = true)
    backend.get(newPtr.id).isDefined shouldBe true
    crl.savedStates.size shouldBe saves // A concluded state is never saved again

    crl.completeHeldBarriers()
    backend.get(newPtr.id) shouldBe None
  }

  test("a deletion of an object no allocation here covers is issued at once") {
    val backend = new DeletingMapBackend
    populate(backend)
    val crl = new TestCrl
    crl.holdBarriers = true
    crl.holdTxSaves = false
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net, crl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    val delete = RefcountUpdate(op1, ObjectRefcount(1, 1), ObjectRefcount(2, 0))
    prepare(frontend, net, lockTxd(txid2, List(delete))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid2, committed = true)
    backend.get(oid1) shouldBe None
    crl.heldBarriers shouldBe Nil
  }

  test("a repair deletion soon after the object's allocation was finalized waits for the CRL deletion too") {
    val newPtr = DataObjectPointer(ObjectId(new UUID(0, 40)), poolId, Array[Byte]())
    val (frontend, backend, crl, net) = allocatedThenDeleting(newPtr)
    resolve(frontend, net, txid2, committed = false) // The deletion aborted here; repair deletes the object later
    crl.holdBarriers = true
    finalizeTx1(frontend)
    val completion = Promise[Unit]()
    frontend.deleteObjectForRepair(newPtr.id, ObjectPointer.EmptyArray, completion)
    completion.isCompleted shouldBe false
    backend.get(newPtr.id).isDefined shouldBe true

    crl.completeHeldBarriers()
    completion.isCompleted shouldBe true
    backend.get(newPtr.id) shouldBe None
  }

  test("a store loaded with such an entry recreates the transaction") {
    val saved = savedAfterFailedCommit()
    val backend = new MapBackend(storeId)
    populate(backend)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = saved
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), new TestNet, crl,
      new TransactionStatusCache(), loadedAt = FrontendSuite.longLoaded)
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
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    crl.deleted should not contain txid2

    backend.deliverOne()
    backend.issued shouldBe List(txid1, txid2)
    crl.deleted should not contain txid2
    backend.deliverOne()
    crl.deleted should contain (txid2)
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
  }

  test("a second TxFinalized listing the store while its commit is held for its turn writes the stub once it persists") {
    val crl = new TestCrl
    val (frontend, backend, net) = heldCommit(crl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = true)
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    // Another driver's TxFinalized (Ruling 3), or a retransmit, lists this store: the Tx holding the commit is forgotten
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false,
      owed = Set(storeId), startTimestamp = HLCTimestamp.now))
    crl.stubsSaved shouldBe Nil

    backend.deliverOne()
    backend.issued shouldBe List(txid1, txid2)
    crl.stubsSaved shouldBe Nil
    backend.deliverOne()
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
    crl.stubsSaved shouldBe List(txid2)
    crl.deleted should not contain txid2
  }

  // M2: the registration is removed only by the Tx that made it
  test("a replay's Tx forgotten while a finalized commit is held for its turn leaves the held Tx registered") {
    val crl = new TestCrl
    val (frontend, backend, net) = heldCommit(crl, replayableStatusCache())
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = true)
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    // The status entry is evicted for size, so a replayed prepare starts a fresh Tx, which is forgotten in turn
    frontend.statusCache.evictForTest(txid2)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    frontend.transactions.contains(txid2) shouldBe true
    // A late outcome reaches it: with no UnackedCommits entry it applies nothing and is forgotten at once
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = true, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    frontend.statusCache.evictForTest(txid2)

    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false,
      owed = Set(storeId), startTimestamp = HLCTimestamp.now))
    // The held Tx takes the stub, written once its commit persists, not before; its state is kept until then
    crl.stubsSaved shouldBe Nil
    crl.deleted should not contain txid2
    backend.deliverCommits()
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
    crl.stubsSaved shouldBe List(txid2)
  }

  // The stub a replay's Tx is owed when it is forgotten passes to the held Tx: the guard above must not drop it
  test("a replay's Tx forgotten owing a stub while a finalized commit is held hands the stub to the held Tx") {
    val crl = new TestCrl
    val (frontend, backend, net) = heldCommit(crl, replayableStatusCache())
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = true)
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    // The status entry is evicted for size, so a replayed driver prepare, which carries no data, starts a fresh Tx
    frontend.statusCache.evictForTest(txid2)
    frontend.receiveTransactionMessage(mkPrepare(lockTxd(txid2, List(overwriteAfter(txid1))), fromClient = false))
    frontend.transactions.contains(txid2) shouldBe true
    // A TxFinalized listing this store reaches the replay's Tx. With no data it holds no commit of its own, so it is
    // forgotten at once, owing the stub
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false,
      owed = Set(storeId), startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    crl.stubsSaved shouldBe Nil
    crl.deleted should not contain txid2

    backend.deliverCommits()
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
    crl.stubsSaved shouldBe List(txid2)
  }

  // The held Tx owes the stub first; a replay's Tx then defers its own finalization behind the same commit
  test("a stub owed to a held commit survives a replay's Tx deferring its finalization for the same transaction") {
    val crl = new TestCrl
    val (frontend, backend, net) = heldCommit(crl, replayableStatusCache())
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = true)
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    // A TxFinalized listing this store reaches the store with no Tx: the held Tx owes the stub
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false,
      owed = Set(storeId), startTimestamp = HLCTimestamp.now))
    // The status entry is evicted for size, so a replayed prepare starts a fresh Tx, finalized behind the held commit
    frontend.statusCache.evictForTest(txid2)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    frontend.transactions.contains(txid2) shouldBe true
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    frontend.transactions.contains(txid2) shouldBe false
    crl.stubsSaved shouldBe Nil

    backend.deliverCommits()
    backend.get(oid1).get.metadata.revision shouldBe ObjectRevision(txid2)
    crl.stubsSaved shouldBe List(txid2)
  }

  test("a waiting transaction that resolves is no longer called when the commit persists") {
    val (frontend, backend, net) = heldCommit(new TestCrl)
    sendPrepare(frontend, lockTxd(txid2, List(overwriteAfter(txid1))))
    resolve(frontend, net, txid2, committed = false)
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid2, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))

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
  def admissionStore(admission: CrlAdmission, crl: TestCrl = new TestCrl,
                     tier: AdmissionTier = AdmissionTier.Data): (Store, DeferredCommitBackend, TestNet) =
    val backend = new DeferredCommitBackend
    populate(backend)
    val net = new TestNet
    val store = new Store(StorageDeviceId(new UUID(0, 99)), scala.concurrent.ExecutionContext.parasitic, backend,
      new HostObjectCache(1024 * 1024).forStore(storeId), net, BackgroundTaskManager.NoBackgroundTaskManager, crl,
      new TransactionStatusCache(), null, TransactionDriver.noErrorRecoveryFactory, Duration(60, SECONDS), None,
      admission, loadedAt = FrontendSuite.longLoaded, admissionTier = tier)
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

  // ---- CRL-W2: a transaction whose CRL bytes exceed crl.maxTransactionBytes is dropped, never saved ----

  /** A TestCrl that takes transactions of at most 10 000 CRL bytes */
  class SmallTxCrl extends TestCrl:
    override def maxTransactionBytes: Long = 10000

  /** `m` carrying 20 000 bytes of object data for its first DataUpdate */
  def oversized(m: TxPrepare): TxPrepare =
    m.copy(objectUpdates = m.objectUpdates.map(ou => ObjectUpdate(ou.objectId, DataBuffer(new Array[Byte](20000)))))

  test("oversizedPrepare: a prepare larger than the CRL takes is Oversized and charges nothing") {
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new SmallTxCrl
    val (store, _, net) = admissionStore(admission, crl)
    val p1 = oversized(dataPrepare(ledTxd(txid1, List(overwrite(op1)))))

    store.frontend.admitPrepare(p1) shouldBe PrepareAdmission.Oversized
    admission.admittedBytes shouldBe 0

    // Dropped without a reply, the CRL untouched
    store.receiveTransactionMessage(p1)
    store.frontend.transactions shouldBe empty
    net.sentTx shouldBe empty
    crl.txSaved shouldBe false
    admission.admittedBytes shouldBe 0
  }

  test("oversizedLateData: data arriving after a data-less prepare that makes it oversized is dropped") {
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new SmallTxCrl
    val (store, _, _) = admissionStore(admission, crl)
    val withData = oversized(dataPrepare(ledTxd(txid1, List(overwrite(op1)))))
    val dataLess = withData.copy(objectUpdates = Nil, fromClient = false)

    store.frontend.admitPrepare(dataLess) shouldBe PrepareAdmission.Admitted
    val afterFirst = admission.admittedBytes
    afterFirst shouldBe admissionBytes(dataLess)
    store.frontend.admitPrepare(withData) shouldBe PrepareAdmission.Oversized
    admission.admittedBytes shouldBe afterFirst

    // Through the store, the data-less prepare is handled and the oversized one dropped: no save carries the data
    store.receiveTransactionMessage(dataLess)
    store.receiveTransactionMessage(withData)
    admission.admittedBytes shouldBe afterFirst
    crl.savedStates.foreach(_.objectUpdates shouldBe empty)
  }

  test("fullLaneStillFull: a small new prepare at the ceiling is Full, not Oversized") {
    val admission = new CrlAdmission(100)
    admission.tryAdmit(100) shouldBe true
    val (store, _, _) = admissionStore(admission, new SmallTxCrl)

    store.frontend.admitPrepare(dataPrepare(ledTxd(txid1, List(overwrite(op1))))) shouldBe PrepareAdmission.Full
    admission.admittedBytes shouldBe 100
  }

  test("growthRequested: Full asks the CRL to grow, Admitted and Oversized do not") {
    val admission = new CrlAdmission(1024 * 1024)
    val crl = new SmallTxCrl
    val (store, _, _) = admissionStore(admission, crl)
    val small = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    val txid4 = TransactionId(new UUID(0, 12))

    store.frontend.admitPrepare(small) shouldBe PrepareAdmission.Admitted
    crl.growthRequests shouldBe 0
    store.frontend.admitPrepare(oversized(dataPrepare(ledTxd(txid2, List(overwrite(op1)))))) shouldBe
      PrepareAdmission.Oversized
    crl.growthRequests shouldBe 0

    // A new transaction past the ceiling, and late data past it (A1), each ask once
    val dataLess = small.copy(objectUpdates = Nil, fromClient = false)
    store.frontend.admitPrepare(dataLess.copy(txd = ledTxd(txid3, List(overwrite(op1))))) shouldBe PrepareAdmission.Admitted
    admission.charge(admission.ceiling(AdmissionTier.Data) - admission.admittedBytes)
    store.frontend.admitPrepare(dataPrepare(ledTxd(txid4, List(overwrite(op1))))) shouldBe PrepareAdmission.Full
    crl.growthRequests shouldBe 1
    store.frontend.admitPrepare(dataPrepare(ledTxd(txid3, List(overwrite(op1))))) shouldBe PrepareAdmission.Full
    crl.growthRequests shouldBe 2
  }

  // CRL admission rule A1 (crash-recovery-log 6.1): late data is checked against the lane as a new transaction is
  test("lateDataFull: data for an admitted transaction is dropped as Full past the lane, and charged below it") {
    val admission = new CrlAdmission(1024 * 1024)
    val crl = new TestCrl
    val (store, _, _) = admissionStore(admission, crl)
    val withData = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    val dataLess = withData.copy(objectUpdates = Nil, fromClient = false)

    store.frontend.admitPrepare(dataLess) shouldBe PrepareAdmission.Admitted
    // Other transactions fill the data lane
    admission.charge(admission.ceiling(AdmissionTier.Data) - admission.admittedBytes)
    val full = admission.admittedBytes
    store.frontend.admitPrepare(withData) shouldBe PrepareAdmission.Full
    admission.admittedBytes shouldBe full
    // Through the store, the data-carrying prepare is dropped: no save carries the data
    store.receiveTransactionMessage(dataLess)
    store.receiveTransactionMessage(withData)
    admission.admittedBytes shouldBe full
    crl.savedStates.foreach(_.objectUpdates shouldBe empty)
    // A data-less retransmission is still handled: it charges nothing
    store.frontend.admitPrepare(dataLess) shouldBe PrepareAdmission.Admitted

    // Below the ceiling the data is charged
    admission.release(1)
    store.frontend.admitPrepare(withData) shouldBe PrepareAdmission.Admitted
    admission.admittedBytes shouldBe full - 1 + admissionBytes(withData) - admissionBytes(dataLess)
  }

  // CRL admission rule A4 (crash-recovery-log 6.1): a Tx whose admission was released saves nothing more
  test("a resolved-aborted Tx whose state was deleted answers a prepare and an accept with TxResolved, saving nothing") {
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new TestCrl
    val (store, _, net) = admissionStore(admission, crl)
    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    store.receiveTransactionMessage(p1)
    store.frontend.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = false, late = false,
      proof = false))
    crl.deleted shouldBe List(txid1)
    admission.admittedBytes shouldBe 0
    // Kept until TxFinalized
    store.frontend.transactions.contains(txid1) shouldBe true
    val saves = crl.savedStates.size
    val driver = StoreId(poolId, 2.toByte)

    val prepare = mkPrepare(p1.txd, fromClient = false).copy(from = driver, proposalId = ProposalId(5, 2))
    store.frontend.admitPrepare(prepare) shouldBe PrepareAdmission.Admitted
    store.frontend.receiveTransactionMessage(prepare)
    store.frontend.receiveTransactionMessage(TxAccept(storeId, driver, txid1, ProposalId(5, 2), value = false))

    crl.savedStates.size shouldBe saves
    net.sentTx.collect { case r: TxResolved if r.to == driver => (r.transactionId, r.committed, r.proof) } shouldBe
      List((txid1, false, false), (txid1, false, false))
    net.sentTx.collect { case r: TxPrepareResponse if r.to == driver => r } shouldBe empty
    net.sentTx.collect { case r: TxAcceptResponse if r.to == driver => r } shouldBe empty
    admission.admittedBytes shouldBe 0
  }

  // Cross-document-review X5: data transactions filling a shared device must not block the marker batches, on the
  // device's metadata-pool stores, that those transactions finalize with
  test("past the data lane a data store drops a new prepare while a metadata store on the device admits one") {
    val admission = new CrlAdmission(800)
    admission.tryAdmit(admission.ceiling(AdmissionTier.Data)) shouldBe true
    val dataCrl = new TestCrl
    val (dataStore, _, _) = admissionStore(admission, dataCrl)
    val metaCrl = new TestCrl
    val (metaStore, _, _) = admissionStore(admission, metaCrl, AdmissionTier.Metadata)

    dataStore.receiveTransactionMessage(dataPrepare(ledTxd(txid1, List(overwrite(op1)))))
    dataStore.frontend.transactions shouldBe empty
    dataCrl.txSaved shouldBe false

    val m = dataPrepare(ledTxd(txid2, List(overwrite(op1))))
    metaStore.receiveTransactionMessage(m)
    metaStore.frontend.transactions.keySet shouldBe Set(txid2)
    metaCrl.txSaved shouldBe true
    admission.admittedBytes shouldBe 600 + admissionBytes(m)
  }

  test("past the metadata lane only a bootstrap store admits a new prepare") {
    val admission = new CrlAdmission(800)
    admission.tryAdmit(admission.ceiling(AdmissionTier.Metadata)) shouldBe true
    val (metaStore, _, _) = admissionStore(admission, tier = AdmissionTier.Metadata)
    val bootCrl = new TestCrl
    val (bootStore, _, _) = admissionStore(admission, bootCrl, AdmissionTier.Bootstrap)

    metaStore.receiveTransactionMessage(dataPrepare(ledTxd(txid1, List(overwrite(op1)))))
    metaStore.frontend.transactions shouldBe empty

    bootStore.receiveTransactionMessage(dataPrepare(ledTxd(txid2, List(overwrite(op1)))))
    bootStore.frontend.transactions.keySet shouldBe Set(txid2)
    bootCrl.txSaved shouldBe true
  }

  test("a stub write is not subject to CrlAdmission backpressure") {
    val admission = new CrlAdmission(100)
    admission.tryAdmit(100) shouldBe true
    admission.full() shouldBe true
    val crl = new TestCrl
    val (store, _, _) = admissionStore(admission, crl)

    // Written past the mark, and charged without a check
    var durable = false
    store.frontend.recordAppliedStub(txid1, stubStart) { durable = true }
    durable shouldBe true
    crl.stubsSaved shouldBe List(txid1)
    store.frontend.hasAppliedStub(txid1) shouldBe true
    admission.admittedBytes shouldBe 100 + CrlAdmission.AppliedStubBytes
    // Saved again, it is charged once
    store.frontend.recordAppliedStub(txid1, stubStart)(())
    crl.stubsSaved shouldBe List(txid1, txid1)
    admission.admittedBytes shouldBe 100 + CrlAdmission.AppliedStubBytes
  }

  test("stubs count toward the device's admission until they leave the CRL") {
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new TestCrl
    val (store, _, _) = admissionStore(admission, crl)
    store.frontend.recordAppliedStub(txid1, stubStart)(())
    store.frontend.recordAppliedStub(txid2, stubStart)(())
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
      new TestCrl, new TransactionStatusCache(), admission = full, deferRecovery = true, loadedAt = FrontendSuite.longLoaded)
    recovering.recover(StoreRecoveryState(Nil, Map(txid1 -> stubStart, txid2 -> stubStart)))
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
    store.frontend.admitPrepare(p1) shouldBe PrepareAdmission.Admitted
    store.frontend.transactions.contains(txid1) shouldBe false
    admission.admittedBytes shouldBe admissionBytes(p1)

    store.frontend.recordAppliedStub(txid1, stubStart)(())
    admission.admittedBytes shouldBe admissionBytes(p1) + CrlAdmission.AppliedStubBytes
    crl.heldStubSaves.foreach(_())
    admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes
  }

  test("an applied stub cannot replace a transaction whose backend commits are outstanding or failed") {
    val (frontend, backend, _) = heldCommit(new TestCrl)
    an[IllegalArgumentException] should be thrownBy frontend.recordAppliedStub(txid1, stubStart)(())
    backend.deliverCommits(Right(CommitError.IOFailure))
    an[IllegalArgumentException] should be thrownBy frontend.recordAppliedStub(txid1, stubStart)(())
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
    store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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
      store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed, late = false, owed = Set(),
        startTimestamp = HLCTimestamp.now))
      // A committed transaction's CRL entry, and so its admission, outlives the Tx until its commit persists
      admission.admittedBytes shouldBe (if committed then admissionBytes(p1) else 0L)
      backend.deliverCommits()
    else
      backend.deliverCommits()
      store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed, late = false, owed = Set(),
        startTimestamp = HLCTimestamp.now))
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

  test("a transaction whose state an owed stub replaces releases its admission once the stub is durable") {
    for finalizeBeforePersisted <- List(false, true) do
      val admission = new CrlAdmission(Long.MaxValue)
      val crl = new TestCrl
      crl.holdStubSaves = true
      val (store, backend, _) = admissionStore(admission, crl)
      val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
      store.receiveTransactionMessage(p1)
      store.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, committed = true, late = false, proof = false))
      val owed = TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(storeId),
        startTimestamp = HLCTimestamp.now)
      if finalizeBeforePersisted then
        store.receiveTransactionMessage(owed)
        backend.deliverCommits()
      else
        backend.deliverCommits()
        store.receiveTransactionMessage(owed)
      store.frontend.transactions.contains(txid1) shouldBe false
      crl.stubsSaved shouldBe List(txid1)
      admission.admittedBytes shouldBe admissionBytes(p1) + CrlAdmission.AppliedStubBytes
      crl.heldStubSaves.foreach(_())
      admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes
  }

  test("a prepare arriving before a stub is durable is admitted afresh, not through the replaced state's record") {
    val admission = new CrlAdmission(Long.MaxValue)
    val crl = new TestCrl
    crl.holdStubSaves = true
    val (store, _, _) = admissionStore(admission, crl)
    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    store.frontend.admitPrepare(p1) shouldBe PrepareAdmission.Admitted
    store.frontend.recordAppliedStub(txid1, stubStart)(())
    // A replayed prepare: charged for itself, while the replaced state stays counted until the stub is durable
    store.frontend.admitPrepare(p1) shouldBe PrepareAdmission.Admitted
    admission.admittedBytes shouldBe 2 * admissionBytes(p1) + CrlAdmission.AppliedStubBytes
    crl.heldStubSaves.foreach(_())
    admission.admittedBytes shouldBe admissionBytes(p1) + CrlAdmission.AppliedStubBytes
    store.frontend.releaseAdmission(txid1)
    admission.admittedBytes shouldBe CrlAdmission.AppliedStubBytes

    // Closed before the stub is durable, the replaced state stays counted, retained for the store
    crl.heldStubSaves = Nil
    store.frontend.admitPrepare(p1) shouldBe PrepareAdmission.Admitted
    store.frontend.recordAppliedStub(txid1, stubStart)(())
    store.frontend.close()
    admission.retainedBytes(storeId) shouldBe admissionBytes(p1) + CrlAdmission.AppliedStubBytes
    crl.heldStubSaves.foreach(_())
    admission.admittedBytes shouldBe admissionBytes(p1) + CrlAdmission.AppliedStubBytes
  }

  test("object data arriving after a data-less prepare is charged once, and returned at finalization") {
    // Below the lane: data arriving for an admitted transaction past it is dropped (lateDataFull)
    val admission = new CrlAdmission(Long.MaxValue)
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
    store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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
      new TestCrl, statusCache, null, TransactionDriver.noErrorRecoveryFactory, Duration(60, SECONDS), None, admission, loadedAt = FrontendSuite.longLoaded)
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
    store.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
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
      new TestCrl, statusCache, null, TransactionDriver.noErrorRecoveryFactory, Duration(60, SECONDS), None, admission, loadedAt = FrontendSuite.longLoaded)

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
      admission = admission, unackedLookup = lookup, loadedAt = FrontendSuite.longLoaded)
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
    frontend.receiveTransactionMessage(TxFinalized(storeId, storeId, txid1, committed = false, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    // Neither the recovery's UnackedCommits lookup nor the one started after the aborts finds an entry
    lookup.answer(txid1, lookup.absent)
    lookup.answer(txid1, lookup.absent)
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
      admission = admission, loadedAt = FrontendSuite.longLoaded)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    frontend.awaitRecovery()

    val p1 = dataPrepare(ledTxd(txid1, List(overwrite(op1))))
    frontend.admitPrepare(p1) shouldBe PrepareAdmission.Admitted
    frontend.receiveTransactionMessage(p1)
    recoverable = List(TransactionRecoveryState(storeId, p1.txd.serialize(), p1.objectUpdates,
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial))
    frontend.recover(StoreRecoveryState(recoverable))
    frontend.transactions.contains(txid1) shouldBe true
    admission.admittedBytes shouldBe admissionBytes(p1)
  }

  // ---- Status query: every store answers (spec section 2) ----

  private val reqUUID = new UUID(1, 99)
  private def statusRequest(startTimestamp: HLCTimestamp): TxStatusRequest =
    TxStatusRequest(storeId, StoreId(poolId, 1), txid1, reqUUID, startTimestamp)
  private def statusResponses(net: TestNet): List[TxStatusResponse] =
    net.sentTx.collect { case r: TxStatusResponse => r }

  test("a store holding T answers a status request only once frozen, with its vote and latest accept") {
    withExpiryFrontend: (frontend, _, net, _, _, clock, _) =>
      val txd = expiringTxd(List(overwrite(op1)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.receiveTransactionMessage(TxAccept(storeId, peer1, txid1, ProposalId.initialProposal(0.toByte), true))
      net.sentTx = Nil
      // Before T's expiry its acceptor may still accept: its state is not final, so it does not answer
      frontend.receiveTransactionMessage(statusRequest(txd.startTimestamp))
      statusResponses(net) shouldBe Nil

      passHorizon(clock)
      frontend.receiveTransactionMessage(statusRequest(txd.startTimestamp))
      statusResponses(net).map(r => (r.to, r.requestUUID, r.status)) shouldBe
        List((StoreId(poolId, 1), reqUUID,
          Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
            holdsAcceptorState = true, accepted = Some(true), votedCommit = true))))
  }

  test("a frozen commit voter whose vote save is not durable is silent to status requests until it is") {
    // A reported commit vote counts toward ExpiryTally's rules A and F: it must survive a crash, with its lock
    withExpiryFrontend: (frontend, _, net, crl, _, clock, _) =>
      crl.holdTxSaves = true
      val txd = expiringTxd(List(overwrite(op1)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.transactions(txid1).vote shouldBe TransactionDisposition.VoteCommit
      passHorizon(clock)
      statusOf(frontend, net, txd) shouldBe Nil

      crl.completeHeldTxSaves()
      statusOf(frontend, net, txd) shouldBe List(Some(true))
  }

  test("an expiring commit voter does not count its own vote before it is durable") {
    withExpiryFrontend: (frontend, _, net, crl, _, clock, _) =>
      crl.holdTxSaves = true
      val txd = expiringTxd(List(overwrite(op1)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      // With its own vote counted this would be rule A: two commit votes, the write threshold of Replication(3, 2)
      reply(frontend, net, peer1, acceptedCommit)
      net.sentTx.collect { case r: TxResolved => r } shouldBe Nil

      crl.completeHeldTxSaves()
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, acceptedCommit)
      net.sentTx.collect { case r: TxResolved => (r.to, r.committed) }.toSet shouldBe
        Set((peer1, true), (peer2, true))
  }

  test("a frozen Tx whose objects have not loaded is silent to status requests until they load") {
    // Its vote is undecided until the objects load. Expectation changed in the final review (2026-11-16): a frozen Tx
    // now decides no vote, since a vote decided after a status reply could contradict that reply, so once its objects
    // load it reports a final non-vote (it used to vote commit at load, and send that vote past expiry). The silence
    // before the load is kept: the gate (Tx.receiveStatusRequest) answers only once the objects are loaded
    val clock = new TestClock(floorNow)
    val backend = new DeferredCommitBackend
    backend.deferReads = true
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net,
      new TestCrl, new TransactionStatusCache(wallClockMillis = () => clock.now), wallClockMillis = () => clock.now,
      horizon = FiniteDuration(20, SECONDS), loadedAt = Some(floorNow - 60_000))
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    val txd = expiringTxd(List(overwrite(op1)))
    frontend.receiveTransactionMessage(clientPrepare(txd))
    passHorizon(clock)
    frontend.transactions(txid1).frozen shouldBe true
    net.sentTx = Nil

    frontend.receiveTransactionMessage(statusRequest(txd.startTimestamp))
    statusResponses(net) shouldBe Nil

    backend.deliverReads()
    frontend.receiveTransactionMessage(statusRequest(txd.startTimestamp))
    statusResponses(net).map(_.status.map(_.votedCommit)) shouldBe List(Some(false))
    txVotes(net) shouldBe Nil
  }

  // A frozen Tx never decides a vote, so "frozen, loaded and undecided" is a final non-voter: the status reply that
  // reports it (votedCommit = false) can never be contradicted by a later vote (ExpiryTally: reported states are final)

  /** A status request for `txid` from store 1, and the votedCommit of this store's replies to it */
  private def statusOf(frontend: Frontend, net: TestNet, txd: TransactionDescription): List[Option[Boolean]] =
    net.sentTx = Nil
    frontend.receiveTransactionMessage(TxStatusRequest(storeId, StoreId(poolId, 1), txd.transactionId, reqUUID,
      txd.startTimestamp))
    statusResponses(net).map(_.status.map(_.votedCommit))

  test("a frozen Tx waiting on an in-flight commit never votes once that commit persists") {
    var now = System.currentTimeMillis()
    val (frontend, backend, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024, wallClockMillis = () => now)
    prepare(frontend, net, lockTxd(txid1, List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
    resolve(frontend, net, txid1, committed = true)
    backend.heldCommits shouldBe 1
    // Built on txid1's commit, so it would vote commit once that commit persists
    val txd2 = lockTxd(txid2, List(overwriteAfter(txid1))).copy(startTimestamp = HLCTimestamp(now << 16))
    sendPrepare(frontend, txd2)
    prepareResponses(net, txid2) shouldBe empty

    now = now + org.aspen_ddp.aspen.common.transaction.TransactionHorizon.Horizon.toMillis +
      Frontend.DefaultExpiryMargin.toMillis + 1
    frontend.transactions(txid2).frozen shouldBe true
    statusOf(frontend, net, txd2) shouldBe List(Some(false))
    net.clientResponses.clear()

    backend.deliverOne()
    prepareResponses(net, txid2) shouldBe empty
    txVotes(net) shouldBe Nil
    frontend.transactions(txid2).holdsLocks shouldBe false
    statusOf(frontend, net, txd2) shouldBe List(Some(false))
  }

  test("a frozen Tx waiting for its client's prepare never votes when that prepare arrives late") {
    var now = System.currentTimeMillis()
    val (frontend, _, _, net) = commitWindowFixture(cacheBytes = 1024 * 1024, wallClockMillis = () => now)
    val txd = lockTxd(txid1, List(SharedRevisionLock(op1, rev0))).copy(startTimestamp = HLCTimestamp(now << 16))
    // Only a driver's prepare: the vote waits for the client's (prefer-abort)
    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = false))
    prepareResponses(net, txid1) shouldBe empty

    now = now + org.aspen_ddp.aspen.common.transaction.TransactionHorizon.Horizon.toMillis +
      Frontend.DefaultExpiryMargin.toMillis + 1
    frontend.transactions(txid1).frozen shouldBe true
    statusOf(frontend, net, txd) shouldBe List(Some(false))
    net.clientResponses.clear()

    frontend.receiveTransactionMessage(mkPrepare(txd, fromClient = true))
    prepareResponses(net, txid1) shouldBe empty
    txVotes(net) shouldBe Nil
    frontend.transactions(txid1).holdsLocks shouldBe false
    statusOf(frontend, net, txd) shouldBe List(Some(false))
  }

  // Cross-document review G1. Replication(3, 2): read threshold 2, width - writeThreshold 1
  test("an expiring commit voter whose peers accepted commit decides commit, applies it and has it finalized") {
    var finalizing: List[TransactionId] = Nil
    withExpiryFrontendAdmitting(CrlAdmission.Unlimited, txd => finalizing ::= txd.transactionId):
      (frontend, backend, net, _, lookup, clock, statusCache) =>
        val txd = expiringTxd(List(overwrite(op1)))
        frontend.receiveTransactionMessage(clientPrepare(txd))
        passHorizon(clock)
        frontend.expiryHeartbeat()
        reply(frontend, net, peer1, acceptedCommit)

        // ExpiryTally rule A, with store 2 silent: this store and store 1 voted commit, the write threshold of
        // Replication(3, 2). No tally had the two non-commit votes an abort-certain one needs, so by L1 no abort was
        // ever accepted (under the old rules one accept was not enough: store 2's reply was awaited)
        net.sentTx.collect { case r: TxResolved => (r.to, r.committed, r.late, r.proof) }.toSet shouldBe
          Set((peer1, true, false, false), (peer2, true, false, false))
        backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
        frontend.transactions(txid1).status shouldBe TransactionStatus.Committed
        finalizing shouldBe List(txid1)
        lookup.requests shouldBe Nil
  }

  test("a Tx past its expiry promises and accepts nothing") {
    withExpiryFrontend: (frontend, _, net, crl, _, clock, _) =>
      val txd = expiringTxd(List(overwrite(op1)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      passHorizon(clock)
      net.sentTx = Nil
      frontend.receiveTransactionMessage(driverPrepare(txd))
      frontend.receiveTransactionMessage(TxAccept(storeId, peer1, txid1, ProposalId(2, 1.toByte), false))

      votes(net) shouldBe Nil
      net.sentTx.collect { case r: TxAcceptResponse => r } shouldBe Nil
      frontend.transactions(txid1).statusForExpiry.accepted shouldBe None
  }

  test("a store with only a cached outcome answers with it") {
    val statusCache = new TransactionStatusCache(wallClockMillis = () => floorNow)
    withFloorFrontend(statusCache = statusCache): (frontend, _, net, _) =>
      // Above the floor: only the cached outcome makes it answer
      statusCache.updateStatus(txid1, TransactionStatus.Committed, finalized = true, late = true)
      frontend.receiveTransactionMessage(statusRequest(startedAgo(1_000)))

      statusResponses(net).map(_.status) shouldBe
        List(Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = true, late = true,
          holdsAcceptorState = false)))
  }

  test("a store that neither holds nor remembers T answers unknown only below its floor") {
    withFloorFrontend(): (frontend, _, net, _) =>
      frontend.receiveTransactionMessage(statusRequest(startedAgo(5_000)))
      net.sentTx shouldBe empty

      frontend.receiveTransactionMessage(statusRequest(startedAgo(21_000)))
      statusResponses(net).map(r => (r.transactionId, r.status)) shouldBe List((txid1, None))
  }

  test("an Unresolved cache entry counts as no entry for a status request") {
    val statusCache = new TransactionStatusCache(wallClockMillis = () => floorNow)
    withFloorFrontend(statusCache = statusCache): (frontend, _, net, _) =>
      statusCache.updateStatus(txid1, TransactionStatus.Unresolved, finalized = false)
      frontend.receiveTransactionMessage(statusRequest(startedAgo(5_000)))
      net.sentTx shouldBe empty
  }

  // Cross-document review G1: the rebuilt store's old incarnation may have accepted in a transaction started before
  // its acceptor, and lost that state undecided. Counted as a non-holder, it could make the last holders of a chosen
  // commit discard it at expiry (HorizonIntegrationSuite "holders of a chosen commit keep it past expiry ...")
  test("a Tx muted at a rebuilt store answers holdsAcceptorState = true: its old incarnation may have held T") {
    withAcceptorSince(): (frontend, backend, net, _) =>
      // The muted prover of a transaction older than the acceptor whose update the store shows
      appliedKVPrepare(frontend, backend, FiniteDuration(3, SECONDS))
      val txd = frontend.transactions(txid1).txd
      frontend.transactions(txid1).muted shouldBe true
      frontend.receiveTransactionMessage(statusRequest(txd.startTimestamp))

      statusResponses(net).map(_.status.map(_.holdsAcceptorState)) shouldBe List(Some(true))
  }

  test("a rebuilt store without a Tx answers amnesiac, never unknown, for a transaction older than its acceptor") {
    withAcceptorSince(): (frontend, _, net, _) =>
      // Below the store's floor too, where an ordinary store answers unknown
      val start = ago(FiniteDuration(3, SECONDS))
      frontend.belowForgetFloor(start) shouldBe true
      frontend.receiveTransactionMessage(statusRequest(start))
      statusResponses(net).map(_.status) shouldBe
        List(Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
          holdsAcceptorState = true, amnesiac = true)))
  }

  test("a rebuilt store's cached outcome for a transaction older than its acceptor counts as possibly holding it") {
    val statusCache = new TransactionStatusCache()
    withAcceptorSince(statusCache): (frontend, _, net, _) =>
      statusCache.updateStatus(txid1, TransactionStatus.Committed, finalized = false)
      frontend.receiveTransactionMessage(statusRequest(ago(FiniteDuration(3, SECONDS))))

      statusResponses(net).map(_.status) shouldBe
        List(Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = false, late = false,
          holdsAcceptorState = true, amnesiac = true)))
  }

  test("a Tx muted for starting too far ahead does not answer: it may join once forgotten") {
    // Its objects are still loading, so it is not forgotten yet (forgetMuted). Once it is, a retransmission after the
    // clock catches up creates an unmuted Tx, so it is a possible holder, not a permanent non-holder
    val backend = new DeferredCommitBackend
    backend.deferReads = true
    val net = new TestNet
    val frontend = new Frontend(storeId, backend, new HostObjectCache(1024 * 1024).forStore(storeId), net,
      new TestCrl, new TransactionStatusCache(wallClockMillis = () => floorNow), wallClockMillis = () => floorNow,
      loadedAt = Some(floorNow - 60_000))
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    populate(backend)
    val txd = lockTxd(txid1, List(overwrite(op1))).copy(startTimestamp = HLCTimestamp((floorNow + 60_000) << 16))
    frontend.receiveTransactionMessage(clientPrepare(txd))
    frontend.transactions(txid1).muted shouldBe true
    net.sentTx = Nil
    frontend.receiveTransactionMessage(statusRequest(txd.startTimestamp))

    // Its state is not final, so it is a silent store: possibly a holder
    statusResponses(net) shouldBe Nil
  }

  // ---- Expiry: an unresolved Tx past the horizon decides from its peers' status (spec section 2) ----
  //
  // The store's clock is the test's: T starts 1 s before the Frontend's "now", which then moves past the horizon plus
  // the expiry margin. Each heartbeat is a direct call. The primary pool is op1's, Replication(3, 2): more than one
  // non-holder discards

  private final class TestClock(var now: Long)

  private val peer1 = StoreId(poolId, 1)
  private val peer2 = StoreId(poolId, 2)

  private def withExpiryFrontend(f: (Frontend, MapBackend, TestNet, TestCrl, ManualLookup, TestClock,
                                     TransactionStatusCache) => Unit): Unit =
    withExpiryFrontendAdmitting(CrlAdmission.Unlimited)(f)

  private def withExpiryFrontendAdmitting(admission: CrlAdmission,
                                          onExpiryCommit: TransactionDescription => Unit = _ => ())
                                         (f: (Frontend, MapBackend, TestNet, TestCrl, ManualLookup, TestClock,
                                              TransactionStatusCache) => Unit): Unit =
    val clock = new TestClock(floorNow)
    val statusCache = new TransactionStatusCache(wallClockMillis = () => clock.now)
    val backend = new MapBackend(storeId)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    populate(backend)
    val net = new TestNet
    val crl = new TestCrl
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache, unackedLookup = lookup,
      wallClockMillis = () => clock.now, horizon = FiniteDuration(20, SECONDS), loadedAt = Some(floorNow - 60_000),
      admission = admission, onExpiryCommit = onExpiryCommit)
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    f(frontend, backend, net, crl, lookup, clock, statusCache)

  /** Past the horizon plus the default expiry margin, measured from T's start (1 s before floorNow) */
  private def passHorizon(clock: TestClock): Unit =
    clock.now = floorNow - 1_000 + 20_000 + Frontend.DefaultExpiryMargin.toMillis + 1

  private def expiringTxd(reqs: List[TransactionRequirement]): TransactionDescription =
    lockTxd(txid1, reqs).copy(startTimestamp = startedAgo(1_000))

  private def statusRequests(net: TestNet): List[TxStatusRequest] = net.sentTx.collect { case r: TxStatusRequest => r }

  /** Answers the latest status query round of txid1 from `from` */
  private def reply(frontend: Frontend, net: TestNet, from: StoreId, status: Option[TxStatusResponse.TxStatus]): Unit =
    val round = statusRequests(net).last.requestUUID
    frontend.receiveTransactionMessage(TxStatusResponse(storeId, from, txid1, round, status))

  /** A rebuilt peer, for T started before its acceptor: it may have voted, or accepted, anything */
  private val amnesiacPeer = Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false,
    late = false, holdsAcceptorState = true, amnesiac = true))
  /** A frozen peer that did not vote commit and accepted nothing */
  private val nonVoter = Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
    holdsAcceptorState = true))

  private val holding = Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
    holdsAcceptorState = true))

  /** A frozen peer that voted commit and whose latest accept is commit */
  private val acceptedCommit = Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false,
    late = false, holdsAcceptorState = true, accepted = Some(true), votedCommit = true))

  test("a Tx past the horizon queries every other store of the transaction, backing off to eight heartbeats") {
    withExpiryFrontend: (frontend, _, net, _, _, clock, _) =>
      val txd = expiringTxd(List(overwrite(op1), DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)))
        .copy(poolIDAMap = Map(poolId -> ida, otherPoolId -> ida))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      clock.now = floorNow - 1_000 + 20_000 + Frontend.DefaultExpiryMargin.toMillis
      frontend.expiryHeartbeat()
      statusRequests(net) shouldBe Nil // Not yet past the horizon plus the margin

      passHorizon(clock)
      val queried = (1 to 24).filter: _ =>
        val before = statusRequests(net).size
        frontend.expiryHeartbeat()
        statusRequests(net).size > before
      queried shouldBe List(1, 2, 4, 8, 16, 24)

      val rounds = statusRequests(net).groupBy(_.requestUUID)
      rounds.size shouldBe 6 // A fresh request id each round
      rounds.values.foreach: round =>
        round.map(_.to).toSet shouldBe txd.allDataStores - storeId
        round.foreach(r => (r.from, r.transactionId, r.startTimestamp) shouldBe (storeId, txid1, txd.startTimestamp))
  }

  test("an abort voter past the horizon whose peers answer unknown is discarded and releases its locks") {
    withExpiryFrontend: (frontend, _, net, crl, lookup, clock, statusCache) =>
      // Its required revision is not the object's: an abort vote
      val txd = expiringTxd(List(DataUpdate(op1, ObjectRevision(txid2), DataUpdateOperation.Overwrite)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteAbort)
      passHorizon(clock)
      frontend.expiryHeartbeat()

      // Only store 2, silent, could still hold the update: below the read threshold of 2, so T can never be applied
      reply(frontend, net, peer1, None)

      frontend.transactions.contains(txid1) shouldBe false
      crl.deleted should contain (txid1)
      lookup.requests shouldBe Nil // An abort vote is owed nothing
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
      // No outcome is known: the status cache keeps what the Tx wrote
      statusCache.getStatus(txid1).map(_.status) shouldBe Some(TransactionStatus.Unresolved)
  }

  test("a commit voter past the horizon with no UnackedCommits entry is discarded once a later lookup finds none") {
    withExpiryFrontend: (frontend, backend, net, crl, lookup, clock, statusCache) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      passHorizon(clock)
      frontend.expiryHeartbeat()
      lookup.requests shouldBe Nil

      reply(frontend, net, peer1, None)
      reply(frontend, net, peer2, None)
      // The tally discards, but a commit voter first looks up its entry, started after the decision
      lookup.requests shouldBe List(txid1)
      frontend.transactions.contains(txid1) shouldBe true
      lockedWriteTransactions(frontend, net, op1) should contain (txid1)
      // A later round's replies make no second lookup
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, None)
      reply(frontend, net, peer2, None)
      lookup.requests shouldBe List(txid1)

      lookup.answer(txid1, lookup.absent)
      frontend.transactions.contains(txid1) shouldBe false
      crl.deleted should contain (txid1)
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
      backend.get(oid1).get.data.size shouldBe 0
      statusCache.getStatus(txid1).map(_.status) shouldBe Some(TransactionStatus.Unresolved)
  }

  // Paxos F4: a secondary-pool store's driver is learn-only, so when no primary store is reachable its Tx ends by
  // expiry. Its own pool's answers decide too: with both peers unknown, its object's update is held by one store, below
  // the pool's read threshold
  test("a secondary-pool commit voter whose pool peers answer unknown is discarded once a lookup finds no entry") {
    withExpiryFrontend: (frontend, _, net, crl, lookup, clock, _) =>
      val txd = expiringTxd(List(overwrite(op1), DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)))
        .copy(primaryObject = otherPtr, poolIDAMap = Map(poolId -> ida, otherPoolId -> ida))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      passHorizon(clock)
      frontend.expiryHeartbeat()

      reply(frontend, net, peer1, None)
      lookup.requests shouldBe Nil // Store 2 may yet hold the update
      reply(frontend, net, peer2, None)
      lookup.requests shouldBe List(txid1)
      lookup.answer(txid1, lookup.absent)
      frontend.transactions.contains(txid1) shouldBe false
      crl.deleted should contain (txid1)
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
  }

  test("a commit voter past the horizon with an UnackedCommits entry applies and resolves committed") {
    withExpiryFrontend: (frontend, backend, net, crl, lookup, clock, statusCache) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, None)
      reply(frontend, net, peer2, None)

      lookup.answer(txid1, lookup.own)
      // Owed: T committed and was finalized (its entry is durable), so it applies and is forgotten as a late commit
      backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
      frontend.transactions.contains(txid1) shouldBe false
      crl.stubsSaved should contain (txid1)
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
      statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
        finalized = true, late = true))
  }

  test("a commit voter keeps holding while its UnackedCommits lookup fails") {
    withExpiryFrontend: (frontend, backend, net, crl, lookup, clock, _) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, None)
      reply(frontend, net, peer2, None)

      lookup.answer(txid1, Failure(new Exception("metadata pool down")))
      frontend.transactions.contains(txid1) shouldBe true
      lockedWriteTransactions(frontend, net, op1) should contain (txid1)
      crl.deleted should not contain txid1

      frontend.retryUnackedLookups()
      lookup.requests shouldBe List(txid1, txid1)
      lookup.answer(txid1, Failure(new Exception("still down")))
      frontend.transactions.contains(txid1) shouldBe true

      frontend.retryUnackedLookups()
      lookup.answer(txid1, lookup.absent)
      frontend.transactions.contains(txid1) shouldBe false
      backend.get(oid1).get.data.size shouldBe 0
  }

  test("each status query round counts only its own replies") {
    withExpiryFrontend: (frontend, _, net, _, lookup, clock, _) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      val first = statusRequests(net).last.requestUUID
      // Store 1 rebuilt, store 2 silent: hold (ExpiryTally rule G)
      reply(frontend, net, peer1, amnesiacPeer)
      frontend.expiryHeartbeat() // A new round
      val second = statusRequests(net).last.requestUUID
      second should not be first

      // Store 2's answer to the first round, a non-voter's, with store 1's from that round, would decide discard
      // (rule D: one holder of the update, below the read threshold of two). Counted in the new round, where store 1 is
      // silent, it holds
      net.sentTx = Nil
      frontend.receiveTransactionMessage(TxStatusResponse(storeId, peer2, txid1, first, nonVoter))
      frontend.receiveTransactionMessage(TxStatusResponse(storeId, peer2, txid1, second, nonVoter))
      lookup.requests shouldBe Nil
      frontend.transactions(txid1).status shouldBe TransactionStatus.Unresolved
  }

  test("a Tx past the horizon whose silent peer may have accepted abort keeps holding and later learns the outcome") {
    withExpiryFrontend: (frontend, backend, net, _, lookup, clock, _) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      // Store 1 rebuilt and store 2 silent (ExpiryTally rule G): store 2 may hold an accepted abort (with store 1's
      // lost vote, two non-commit votes make an abort-certain tally), or have voted commit (rule A)
      reply(frontend, net, peer1, amnesiacPeer)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, amnesiacPeer)
      lookup.requests shouldBe Nil
      frontend.transactions(txid1).status shouldBe TransactionStatus.Unresolved
      lockedWriteTransactions(frontend, net, op1) should contain (txid1)

      // A later round finds store 2 resolved: the outcome is adopted
      frontend.expiryHeartbeat()
      frontend.expiryHeartbeat()
      reply(frontend, net, peer2, Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = false,
        late = false, holdsAcceptorState = true)))
      frontend.transactions(txid1).status shouldBe TransactionStatus.Committed
      backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)

      frontend.receiveTransactionMessage(TxFinalized(storeId, peer1, txid1, committed = true, late = false,
        owed = Set(), startTimestamp = HLCTimestamp.now))
      frontend.transactions.contains(txid1) shouldBe false
  }

  test("an expiring commit voter adopts an abort only once a later lookup finds no entry") {
    withExpiryFrontend: (frontend, backend, net, crl, lookup, clock, statusCache) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, Some(TxStatusResponse.TxStatus(TransactionStatus.Aborted, finalized = true,
        late = false, holdsAcceptorState = false)))
      // As for a recovered commit vote: the abort waits for a lookup started after it
      lookup.requests shouldBe List(txid1)
      frontend.transactions.contains(txid1) shouldBe true

      lookup.answer(txid1, lookup.absent)
      frontend.transactions.contains(txid1) shouldBe false
      crl.deleted should contain (txid1)
      backend.get(oid1).get.data.size shouldBe 0
      statusCache.getStatus(txid1).map(s => (s.status, s.finalized)) shouldBe Some((TransactionStatus.Aborted, true))
  }

  test("an expiring Tx adopts a finalized commit through the late-commit path") {
    withExpiryFrontend: (frontend, backend, net, crl, lookup, clock, _) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = true,
        late = false, holdsAcceptorState = false)))
      lookup.requests shouldBe List(txid1)

      lookup.answer(txid1, lookup.absent) // Not owed: it applies nothing
      frontend.transactions.contains(txid1) shouldBe false
      backend.get(oid1).get.data.size shouldBe 0
  }

  test("after expiry, a later prepare for T creates no Tx") {
    withExpiryFrontend: (frontend, _, net, crl, _, clock, _) =>
      val txd = expiringTxd(List(DataUpdate(op1, ObjectRevision(txid2), DataUpdateOperation.Overwrite)))
      frontend.receiveTransactionMessage(clientPrepare(txd))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, None)
      reply(frontend, net, peer2, None)
      frontend.transactions.contains(txid1) shouldBe false
      net.sentTx = Nil

      // A discarded acceptor is indistinguishable from an offline one: it never rejoins T
      frontend.receiveTransactionMessage(clientPrepare(txd))
      frontend.receiveTransactionMessage(driverPrepare(txd))
      frontend.transactions.contains(txid1) shouldBe false
      net.sentTx shouldBe empty
      // Nor does it say anything but "unknown" when asked
      frontend.receiveTransactionMessage(statusRequest(txd.startTimestamp))
      statusResponses(net).map(_.status) shouldBe List(None)
  }

  /** A recovered commit vote whose lookup answers `found` (`None`: its own incarnation) never gets its TxFinalized:
    * every peer forgot T. Having learned T from its UnackedCommits entry, it waits for nothing else, and concludes as
    * finalized once its peers can no longer decide T. Returns the backend and the CRL
    */
  private def entryCommitConcludesAtExpiry(found: Option[Option[StoreIncarnation]]): (MapBackend, TestCrl) =
    val clock = new TestClock(floorNow)
    val statusCache = new TransactionStatusCache(wallClockMillis = () => clock.now)
    val backend = new MapBackend(storeId)
    populate(backend)
    val cache = new HostObjectCache(1024 * 1024).forStore(storeId)
    cache.insert(mkKVObjectState())
    val update = KeyValueOperation.encode(List(new Delete(k(2))), ida)(0)
    val trs = TransactionRecoveryState(storeId, expiringTxd(List(deleteK2)).serialize(),
      List(ObjectUpdate(oid2, update)), TransactionDisposition.VoteCommit, TransactionStatus.Unresolved,
      PersistentState.initial)
    val crl = new TestCrl:
      override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = List(trs)
    val net = new TestNet
    val lookup = new ManualLookup
    val frontend = new Frontend(storeId, backend, cache, net, crl, statusCache, unackedLookup = lookup,
      wallClockMillis = () => clock.now, horizon = FiniteDuration(20, SECONDS), loadedAt = Some(floorNow))
    lookup.frontend = Some(frontend)
    backend.setCompletionHandler(op => frontend.backendOperationComplete(op))
    lookup.answer(txid1, found.fold(lookup.own)(inc => Success(UnackedLookupResult.Found(inc))))
    frontend.transactions(txid1).provesCommitToItsDriver shouldBe true

    passHorizon(clock)
    frontend.expiryHeartbeat()
    reply(frontend, net, peer1, holding)
    frontend.transactions.contains(txid1) shouldBe true // Holders may yet finalize it
    frontend.expiryHeartbeat()
    reply(frontend, net, peer1, None)
    reply(frontend, net, peer2, None)

    frontend.transactions.contains(txid1) shouldBe false
    lookup.requests shouldBe List(txid1) // The entry was already found
    statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
      finalized = true, late = true))
    (backend, crl)

  test("a recovered commit vote applied from its entry concludes as finalized once its peers can no longer decide T") {
    val (backend, crl) = entryCommitConcludesAtExpiry(None)
    kvKeys(backend) should not contain k(2)
    crl.stubsSaved should contain (txid1)
  }

  test("a recovered commit vote whose entry names no incarnation applies it and concludes as finalized once its " +
    "peers can no longer decide T") {
    val (backend, crl) = entryCommitConcludesAtExpiry(Some(None))
    kvKeys(backend) should not contain k(2)
    crl.stubsSaved should contain (txid1)
  }

  test("a recovered commit vote whose entry names another incarnation concludes as finalized, applying nothing, " +
    "once its peers can no longer decide T") {
    val (backend, crl) = entryCommitConcludesAtExpiry(Some(Some(incarnationA)))
    kvKeys(backend) should contain (k(2))
    crl.stubsSaved shouldBe Nil
    crl.deleted should contain (txid1)
  }

  // ---- Expiry of a Tx that knows its outcome but missed its TxFinalized ----

  private val cacheOnlyCommit = Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = false,
    late = false, holdsAcceptorState = false))

  test("a committed Tx whose TxFinalized was lost concludes past the horizon once its peers can no longer decide it") {
    for found <- List(false, true) do
      withExpiryFrontend: (frontend, backend, net, crl, lookup, clock, statusCache) =>
        frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
        frontend.receiveTransactionMessage(TxResolved(storeId, peer1, txid1, committed = true, late = false,
          proof = false))
        backend.get(oid1).get.data.getByteArray shouldBe Array[Byte](7)
        frontend.transactions(txid1).status shouldBe TransactionStatus.Committed
        // Its TxFinalized never arrives, and its peers forgot T
        passHorizon(clock)
        frontend.expiryHeartbeat()
        // Store 1's host cache still knows the commit, with no Tx: a non-holder. Re-adopting it would change nothing
        reply(frontend, net, peer1, cacheOnlyCommit)
        frontend.transactions.contains(txid1) shouldBe true
        reply(frontend, net, peer2, None)

        // Concluded as a late commit of a transaction applied here: its own entry decides whether a stub is owed
        lookup.requests shouldBe List(txid1)
        lookup.answer(txid1, if found then lookup.own else lookup.absent)
        frontend.transactions.contains(txid1) shouldBe false
        if found then crl.stubsSaved should contain (txid1) else crl.deleted should contain (txid1)
        lockedWriteTransactions(frontend, net, op1) shouldBe empty
        statusCache.getStatus(txid1) shouldBe Some(TransactionStatusCache.Entry(TransactionStatus.Committed,
          finalized = true, late = true))
  }

  test("a committed Tx whose TxFinalized was lost keeps holding while a quorum of acceptor-state holders remains") {
    withExpiryFrontend: (frontend, _, net, _, lookup, clock, _) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      frontend.receiveTransactionMessage(TxResolved(storeId, peer1, txid1, committed = true, late = false,
        proof = false))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = false,
        late = false, holdsAcceptorState = true)))
      reply(frontend, net, peer2, cacheOnlyCommit)
      lookup.requests shouldBe Nil
      frontend.transactions.contains(txid1) shouldBe true

      // A later round learns it was finalized: the late-commit path concludes it
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = true,
        late = false, holdsAcceptorState = false)))
      lookup.answer(txid1, lookup.absent)
      frontend.transactions.contains(txid1) shouldBe false
  }

  test("an aborted Tx whose TxFinalized was lost concludes past the horizon once its peers can no longer decide it") {
    withExpiryFrontend: (frontend, _, net, crl, lookup, clock, statusCache) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      frontend.receiveTransactionMessage(TxResolved(storeId, peer1, txid1, committed = false, late = false,
        proof = false))
      frontend.transactions(txid1).status shouldBe TransactionStatus.Aborted
      passHorizon(clock)
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, None)
      reply(frontend, net, peer2, None)

      frontend.transactions.contains(txid1) shouldBe false
      lookup.requests shouldBe Nil
      crl.deleted should contain (txid1)
      statusCache.getStatus(txid1).map(s => (s.status, s.finalized)) shouldBe Some((TransactionStatus.Aborted, true))
  }

  test("a discarded Tx waiting out a collision is not woken when its lock holder releases") {
    // Nothing schedules a call into a Tx: waits are checked at heartbeat over the live Txs, and wake-ups (a holder's
    // release, a persisted commit) find the discarded Tx with nothing left to wait for
    withExpiryFrontend: (frontend, _, net, _, lookup, clock, _) =>
      // T2 votes commit on op1 and holds its lock; T1 builds on T2's write and waits for it (Tx.awaitableHolders)
      val t2 = lockTxd(txid2, List(overwrite(op1))).copy(startTimestamp = startedAgo(1_000))
      frontend.receiveTransactionMessage(clientPrepare(t2))
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwriteAfter(txid2)))))
      votes(net).map(v => (v.transactionId, v.disposition)) shouldBe List((txid2, TransactionDisposition.VoteCommit))
      passHorizon(clock)
      frontend.expiryHeartbeat()
      for txid <- List(txid1, txid2); from <- List(peer1, peer2) do
        val round = statusRequests(net).filter(_.transactionId == txid).last.requestUUID
        frontend.receiveTransactionMessage(TxStatusResponse(storeId, from, txid, round, None))
      // T1 has no vote: discarded at once. T2 waits for its lookup
      frontend.transactions.contains(txid1) shouldBe false
      frontend.transactions.contains(txid2) shouldBe true
      net.sentTx = Nil

      lookup.answer(txid2, lookup.absent) // T2 is discarded, releasing its locks
      frontend.transactions.contains(txid2) shouldBe false
      frontend.expiryHeartbeat()
      frontend.transactions.values.foreach(_.checkCollisionWait(Long.MaxValue))
      net.sentTx.collect { case r: TxPrepareResponse => r } shouldBe Nil
      lockedWriteTransactions(frontend, net, op1) shouldBe empty
  }

  // ---- Parked reads: a re-read awaiting a committed transaction's apply (Frontend.parkRead, spec 5) ----

  private def readResponses(net: TestNet): List[ReadResponse] = net.clientResponses.toList.collect { case r: ReadResponse => r }

  private def awaitingRead(frontend: Frontend, readUUID: UUID, awaitCommit: Set[TransactionId]): Unit =
    frontend.readObjectForNetwork(clientId, readUUID, op1, FullObject(), awaitCommit)

  test("a read awaiting a lock holder's commit is answered when the commit applies") {
    withExpiryFrontend: (frontend, _, net, _, _, _, _) =>
      prepare(frontend, net, expiringTxd(List(overwrite(op1)))).disposition shouldBe TransactionDisposition.VoteCommit
      awaitingRead(frontend, new UUID(2, 1), Set(txid1))
      readResponses(net) shouldBe Nil

      resolve(frontend, net, txid1, committed = true)
      val rs = readResponses(net)
      rs.size shouldBe 1
      rs.head.readUUID shouldBe new UUID(2, 1)
      val cs = rs.head.result.toOption.get
      cs.revision shouldBe ObjectRevision(txid1)
      cs.lockedWriteTransactions shouldBe empty
  }

  test("a read awaiting a lock holder's commit is answered when the holder aborts") {
    withExpiryFrontend: (frontend, _, net, _, _, _, _) =>
      prepare(frontend, net, expiringTxd(List(overwrite(op1))))
      awaitingRead(frontend, new UUID(2, 1), Set(txid1))
      resolve(frontend, net, txid1, committed = false)
      readResponses(net).map(_.result.toOption.get.revision) shouldBe List(rev0)
  }

  test("a read awaiting a transaction that holds no lock is answered at once") {
    withExpiryFrontend: (frontend, _, net, _, _, _, _) =>
      prepare(frontend, net, expiringTxd(List(overwrite(op1))))
      awaitingRead(frontend, new UUID(2, 1), Set(txid2))
      readResponses(net).map(_.result.toOption.get.lockedWriteTransactions) shouldBe List(Set(txid1))
  }

  test("a parked read is answered at ParkedReadWait with the current state") {
    withExpiryFrontend: (frontend, _, net, _, _, clock, _) =>
      prepare(frontend, net, expiringTxd(List(overwrite(op1))))
      awaitingRead(frontend, new UUID(2, 1), Set(txid1))
      clock.now += Frontend.ParkedReadWait.toMillis - 1
      frontend.expireParkedReads()
      readResponses(net) shouldBe Nil

      clock.now += 1
      frontend.expireParkedReads()
      readResponses(net).map(_.result.toOption.get.lockedWriteTransactions) shouldBe List(Set(txid1))
      // Answered once: neither a later heartbeat nor the holder's release answers it again
      frontend.expireParkedReads()
      resolve(frontend, net, txid1, committed = true)
      readResponses(net).size shouldBe 1
  }

  test("a parked read is answered when its transaction is discarded at expiry") {
    withExpiryFrontend: (frontend, _, net, _, lookup, clock, _) =>
      frontend.receiveTransactionMessage(clientPrepare(expiringTxd(List(overwrite(op1)))))
      votes(net).map(_.disposition) shouldBe List(TransactionDisposition.VoteCommit)
      passHorizon(clock)
      awaitingRead(frontend, new UUID(2, 1), Set(txid1))
      frontend.expiryHeartbeat()
      reply(frontend, net, peer1, None)
      reply(frontend, net, peer2, None)
      readResponses(net) shouldBe Nil

      lookup.answer(txid1, lookup.absent)
      frontend.transactions.contains(txid1) shouldBe false
      val rs = readResponses(net)
      rs.size shouldBe 1
      rs.head.result.toOption.get.revision shouldBe rev0
      rs.head.result.toOption.get.lockedWriteTransactions shouldBe empty
  }

  test("a resent read replaces its parked entry") {
    withExpiryFrontend: (frontend, _, net, _, _, _, _) =>
      prepare(frontend, net, expiringTxd(List(overwrite(op1))))
      awaitingRead(frontend, new UUID(2, 1), Set(txid1))
      awaitingRead(frontend, new UUID(2, 1), Set(txid1))
      readResponses(net) shouldBe Nil
      resolve(frontend, net, txid1, committed = true)
      readResponses(net).map(_.readUUID) shouldBe List(new UUID(2, 1))
  }

  test("past MaxParkedReads a read is answered at once") {
    withExpiryFrontend: (frontend, _, net, _, _, _, _) =>
      prepare(frontend, net, expiringTxd(List(overwrite(op1))))
      (1 to Frontend.MaxParkedReads).foreach(n => awaitingRead(frontend, new UUID(3, n), Set(txid1)))
      readResponses(net) shouldBe Nil
      awaitingRead(frontend, new UUID(4, 1), Set(txid1))
      readResponses(net).map(_.readUUID) shouldBe List(new UUID(4, 1))

      resolve(frontend, net, txid1, committed = true)
      readResponses(net).size shouldBe Frontend.MaxParkedReads + 1
  }

  test("a parked read of an unapplied allocation is answered AllocationPending at ParkedReadWait") {
    withExpiryFrontend: (frontend, _, net, _, _, clock, _) =>
      val newPtr = DataObjectPointer(ObjectId(new UUID(0, 35)), poolId, Array[Byte]())
      prepare(frontend, net, allocateData(txid1, newPtr).copy(startTimestamp = startedAgo(1_000)))
        .disposition shouldBe TransactionDisposition.VoteCommit
      frontend.readObjectForNetwork(clientId, new UUID(2, 1), newPtr, MetadataOnly())
      readResponses(net) shouldBe Nil
      clock.now += Frontend.ParkedReadWait.toMillis - 1
      frontend.expireParkedReads()
      readResponses(net) shouldBe Nil

      clock.now += 1
      frontend.expireParkedReads()
      readResponses(net).map(_.result) shouldBe List(Left(ReadError.AllocationPending))
      // Answered once
      resolve(frontend, net, txid1, committed = true)
      readResponses(net).size shouldBe 1
  }

  test("fail-stop drops parked reads without answering") {
    withExpiryFrontend: (frontend, _, net, _, _, clock, _) =>
      prepare(frontend, net, expiringTxd(List(overwrite(op1))))
      awaitingRead(frontend, new UUID(2, 1), Set(txid1))
      frontend.failStop("test")
      clock.now += Frontend.ParkedReadWait.toMillis
      frontend.expireParkedReads()
      readResponses(net) shouldBe Nil
  }
}
