package org.aspen_ddp.aspen.client.internal

import java.util.UUID
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, StorageDeviceId, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, HostMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

class AllocationGroupCacheSuite extends AsyncFunSuite with Matchers:

  val clientId = ClientId(new UUID(0, 1))

  val groupId = AllocationGroupId(new UUID(0, 10))
  val objId = ObjectId(new UUID(0, 20))
  val poolId = PoolId(new UUID(0, 30))
  val pointer = DataObjectPointer(objId, poolId)

  val state = AllocationGroupState(groupId, 0, "test-group", Nil, Nil)
  val state2 = AllocationGroupState(groupId, 0, "test-group-updated", Nil, Nil)

  private val ida = Replication(3, 2)
  private val rev = ObjectRevision.Null
  private val ref = ObjectRefcount(1, 1)

  def mkDataObjectState(ptr: DataObjectPointer, ags: AllocationGroupState): DataObjectState =
    val bytes = ags.toBytes
    val ts = HLCTimestamp.now
    DataObjectState(ptr, rev, ref, ts, ts, ida, bytes.length, DataBuffer(bytes))

  class TClient(override val clientId: ClientId) extends AspenClient:

    var getAllocationGroupPointerFn: AllocationGroupId => Future[DataObjectPointer] =
      _ => Future.failed(new Exception("getAllocationGroupPointer not configured"))

    var getAllocationGroupStateFn: AllocationGroupId => Future[AllocationGroupState] =
      _ => Future.failed(new Exception("getAllocationGroupState not configured"))

    var readDataObjectFn: DataObjectPointer => Future[DataObjectState] =
      _ => Future.failed(new Exception("read not configured"))

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache
    val typeRegistry: TypeRegistry = null

    def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] = readDataObjectFn(pointer)
    def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] = Future.failed(new Exception("TODO"))

    def newTransaction(): Transaction = null
    protected def createStoragePool(config: StoragePoolState): Future[PoolId] = ???
    def getStoragePoolId(poolName: String): Future[PoolId] = ???
    def getHostId(hostName: String): Future[HostId] = ???
    def getAllocationGroupId(groupName: String): Future[AllocationGroupId] = ???

    def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] = ???
    def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer] = ???
    def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] = ???

    override def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer] =
      getAllocationGroupPointerFn(allocationGroupId)

    override def getAllocationGroupState(allocationGroupId: AllocationGroupId): Future[AllocationGroupState] =
      getAllocationGroupStateFn(allocationGroupId)

    def createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId] = ???

    val retryStrategy: RetryStrategy = null
    def backgroundTaskManager: BackgroundTaskManager = BackgroundTaskManager.NoBackgroundTaskManager
    def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None
    private[client] val messenger: Messenger = Messenger.None
    private[client] val objectCache: ObjectCache = ObjectCache.NoCache
    val radicle: KeyValueObjectPointer = null

    private[aspen] def receiveClientResponse(msg: ClientResponse): Unit = ()
    override def sendHostMessage(msg: HostMessage): Unit = ()
    private[aspen] def getSystemAttribute(key: String): Option[String] = None
    private[aspen] def setSystemAttribute(key: String, value: String): Unit = ()

  end TClient

  test("fetchState fetches and caches state on cache miss"):
    val m = new TClient(clientId)
    var pointerCallCount = 0
    var stateCallCount = 0

    m.getAllocationGroupPointerFn = id =>
      pointerCallCount += 1
      Future.successful(pointer)

    m.getAllocationGroupStateFn = id =>
      stateCallCount += 1
      Future.successful(state)

    val cache = new AllocationGroupCache(m)

    cache.fetchState(groupId).map: result =>
      result should be(state)
      stateCallCount should be(1)
      pointerCallCount should be(1)

  test("fetchState returns cached state on second call without re-fetching"):
    val m = new TClient(clientId)
    var pointerCallCount = 0
    var stateCallCount = 0

    m.getAllocationGroupPointerFn = id =>
      pointerCallCount += 1
      Future.successful(pointer)

    m.getAllocationGroupStateFn = id =>
      stateCallCount += 1
      Future.successful(state)

    val cache = new AllocationGroupCache(m)

    for
      result1 <- cache.fetchState(groupId)
      result2 <- cache.fetchState(groupId)
    yield
      result1 should be(state)
      result2 should be(state)
      // getAllocationGroupState called once for the initial fetch
      stateCallCount should be(1)

  test("stale state triggers background refresh and returns cached state immediately"):
    val m = new TClient(clientId)

    m.getAllocationGroupPointerFn = _ => Future.successful(pointer)
    m.getAllocationGroupStateFn = _ => Future.successful(state)
    m.readDataObjectFn = ptr => Future.successful(mkDataObjectState(ptr, state2))

    val cache = new AllocationGroupCache(m, refreshDuration = Duration(100, MILLISECONDS))

    for
      result1 <- cache.fetchState(groupId)
      _ <- Future { Thread.sleep(150) }
      result2 <- cache.fetchState(groupId)
      _ = result2 should be(state)
      _ <- Future { Thread.sleep(50) }
      result3 <- cache.fetchState(groupId)
    yield
      result1 should be(state)
      result3 should be(state2)

  test("fetchState does not trigger duplicate concurrent refreshes"):
    val m = new TClient(clientId)
    var readCallCount = 0
    val refreshPromise = Promise[DataObjectState]()

    m.getAllocationGroupPointerFn = _ => Future.successful(pointer)
    m.getAllocationGroupStateFn = _ => Future.successful(state)
    m.readDataObjectFn = _ =>
      readCallCount += 1
      refreshPromise.future

    val cache = new AllocationGroupCache(m, Duration(1, MILLISECONDS))

    for
      _ <- cache.fetchState(groupId)
      _ <- Future { Thread.sleep(10) }
      stale1 <- cache.fetchState(groupId)
      stale2 <- cache.fetchState(groupId)
      _ = refreshPromise.success(mkDataObjectState(pointer, state2))
      _ <- Future { Thread.sleep(50) }
    yield
      stale1 should be(state)
      stale2 should be(state)
      readCallCount should be(1)

  test("failed refresh preserves stale state and allows future refreshes"):
    val m = new TClient(clientId)
    var readCallCount = 0

    m.getAllocationGroupPointerFn = _ => Future.successful(pointer)
    m.getAllocationGroupStateFn = _ => Future.successful(state)
    m.readDataObjectFn = ptr =>
      readCallCount += 1
      if readCallCount == 1 then Future.failed(new Exception("network error"))
      else Future.successful(mkDataObjectState(ptr, state2))

    val cache = new AllocationGroupCache(m, Duration(1, MILLISECONDS))

    for
      first <- cache.fetchState(groupId)
      _ <- Future { Thread.sleep(10) }
      _ <- cache.fetchState(groupId)
      _ <- Future { Thread.sleep(50) }
      stillCached <- cache.fetchState(groupId)
      _ <- Future { Thread.sleep(50) }
      recovered <- cache.fetchState(groupId)
    yield
      first should be(state)
      stillCached should be(state)
      recovered should be(state2)

  test("fetchState propagates failure when no cached state exists"):
    val m = new TClient(clientId)
    m.getAllocationGroupPointerFn = _ => Future.successful(pointer)
    m.getAllocationGroupStateFn = _ => Future.failed(new Exception("unavailable"))

    val cache = new AllocationGroupCache(m)

    recoverToSucceededIf[Exception]:
      cache.fetchState(groupId)
