package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.allocation.AllocationManager
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.client.{AspenClient, CorruptedObject, DataObjectState, Host, HostId, InvalidObject, KeyValueObjectState, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, ReadResponse}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import org.aspen_ddp.aspen.server.store.backend.BackendType
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
import org.aspen_ddp.aspen.common.ida.IDA

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.language.implicitConversions

object BaseReadDriverSuite {
  val awaitDuration = Duration(100, MILLISECONDS)

  val objId = ObjectId(new UUID(0,1))
  val poolId = PoolId(new UUID(0,2))
  val readUUID = new UUID(0,3)
  val cliUUID = new UUID(0,4)

  val ida = Replication(3,2)

  val ds0 = StoreId(poolId, 0)
  val ds1 = StoreId(poolId, 1)
  val ds2 = StoreId(poolId, 2)

  val sp0 = StorePointer(0, List[Byte](0).toArray)
  val sp1 = StorePointer(1, List[Byte](1).toArray)
  val sp2 = StorePointer(2, List[Byte](2).toArray)

  val ptr = DataObjectPointer(objId, poolId, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  val kvptr = KeyValueObjectPointer(objId, poolId, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  val rev = ObjectRevision.Null
  val ref = ObjectRefcount(1,1)

  val odata = DataBuffer(List[Byte](1,2,3,4).toArray)

  val noLocks = Some(Map[StoreId, List[TransactionDescription]]())

  val client = ClientId(cliUUID)

  class TClient(override val clientId: ClientId) extends AspenClient {

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache

    val typeRegistry: TypeRegistry = null

    def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] = Future.failed(new Exception("TODO"))
    def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] = Future.failed(new Exception("TODO"))

    def newTransaction(): Transaction = null

    def getStoragePool(poolId: PoolId): Future[Option[StoragePool]] = null

    def getStoragePool(poolName: String): Future[Option[StoragePool]] = null

    override def updateStorageHost(storeId: StoreId, newHostId: HostId): Future[Unit] = ???

    override def newStoragePool(newPoolName: String,
                       hostCncFrontends: List[CnCFrontend],
                       ida: IDA,
                       backendType: BackendType): Future[StoragePool] = ???

    protected def createStoragePool(config: StoragePool.Config): Future[StoragePool] = ???

    def getHost(hostId: HostId): Future[Option[Host]] = null

    def getHost(hostName: String): Future[Option[Host]] = null

    val retryStrategy: RetryStrategy = null

    def backgroundTaskManager: BackgroundTaskManager = BackgroundTaskManager.NoBackgroundTaskManager

    def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None

    private[client] val messenger: Messenger = Messenger.None

    private[client] val allocationManager: AllocationManager = null

    private[client] val objectCache: ObjectCache = ObjectCache.NoCache

    private[aspen] def receiveClientResponse(msg: ClientResponse): Unit = ()

    private[aspen] def getSystemAttribute(key: String): Option[String] = None
    private[aspen] def setSystemAttribute(key: String, value: String): Unit = ()

    //private[aspen] def createFinalizerFor(txd: TransactionDescription): TransactionFinalizer = null
  }

}

class BaseReadDriverSuite  extends AsyncFunSuite with Matchers {
  import BaseReadDriverSuite._


  def mkReader(client: AspenClient,
               objectPointer: ObjectPointer = ptr,
               readUUID:UUID = readUUID,
               comment: String = "",
               disableOpportunisticRebuild: Boolean = false) = {
    new BaseReadDriver(client, objectPointer, readUUID, comment, disableOpportunisticRebuild) {
      given ec: ExecutionContext = this.client.clientContext
    }
  }

  test("Fail with invalid object") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Left(ReadError.ObjectMismatch)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[InvalidObject] {
      r.readResult
    }
  }

  test("Fail with corrupted object") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.CorruptedObject)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Left(ReadError.CorruptedObject)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[CorruptedObject] {
      r.readResult
    }
  }

  test("Succeed with errors") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    //    o match {
    //      case Left(_) =>
    //      case Right((ds:DataObjectState, o)) =>
    //        println(s"ptr(${ds.pointer}), rev(${ds.revision}), ref(${ds.refcount}), ts(${ds.timestamp}), data(${com.ibm.aspen.util.db2string(ds.data)})")
    //        println(s"ptr(${ptr}), rev(${nrev2}), ref(${ref}), ts(${ts}), data(${com.ibm.aspen.util.db2string(odata)})")
    //    }

    o should be (DataObjectState(ptr, nrev2, ref, ts, readTime, 5, odata))
  }

  test("Ignore old revisions") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, ts, 5, odata))
  }

  test("Use minimum readTime") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    val minTs = HLCTimestamp(ts.asLong-100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, minTs, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, minTs, 5, odata))
  }


  test("Successful read with data and locks") {
    val m = new TClient(client)
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, rev, ref, ts, ts, 5, odata))
  }

}
