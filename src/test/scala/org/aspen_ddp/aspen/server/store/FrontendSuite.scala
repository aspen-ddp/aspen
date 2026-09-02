package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, TxMessage, TxPrepare, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription, TransactionId}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.SimpleLRUObjectCache
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

  def mkFrontend(): (Frontend, MapBackend, SimpleLRUObjectCache) =
    val backend = new MapBackend(storeId)
    val cache = new SimpleLRUObjectCache(10)
    val frontend = new Frontend(storeId, backend, cache, new TestNet, new TestCrl,
      new TransactionStatusCache())
    (frontend, backend, cache)

  def mkObjectState(objectId: ObjectId): ObjectState =
    new ObjectState(objectId,
      Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)))
}

class FrontendSuite extends AnyFunSuite with Matchers {
  import FrontendSuite._

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

  test("repair deletion succeeds when the store does not have the object") {
    val (frontend, backend, _) = mkFrontend()

    backend.get(oid2) shouldBe None

    val completion = Promise[Unit]()
    frontend.deleteObjectForRepair(oid2, ObjectPointer.EmptyArray, completion)

    Await.result(completion.future, Duration(5, SECONDS))

    backend.get(oid2) shouldBe None
  }
}
