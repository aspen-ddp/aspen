package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, ReadResponse, TxCommitted, TxMessage, TxPrepare, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, FullObject, Key, KeySet, KeyValueObjectPointer, Metadata, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription, TransactionId}
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
}
