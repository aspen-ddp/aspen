package org.aspen_ddp.aspen.client.internal

import java.util.UUID
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, StorageDeviceId, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, HostMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

class AllocationGroupCacheSuite extends AsyncFunSuite with Matchers:

  val clientId = ClientId(new UUID(0, 1))

  val groupId = AllocationGroupId(new UUID(0, 10))
  val groupId2 = AllocationGroupId(new UUID(0, 11))

  val objId = ObjectId(new UUID(0, 20))
  val poolId = PoolId(new UUID(0, 30))
  val pointer = DataObjectPointer(objId, poolId)

  val state = AllocationGroupState(groupId, 0, "test-group", Nil, Nil)
  val state2 = AllocationGroupState(groupId, 0, "test-group-updated", Nil, Nil)

  class TClient(override val clientId: ClientId) extends AspenClient:

    var getAllocationGroupPointerFn: AllocationGroupId => Future[DataObjectPointer] =
      _ => Future.failed(new Exception("getAllocationGroupPointer not configured"))

    var getAllocationGroupStateFn: AllocationGroupId => Future[AllocationGroupState] =
      _ => Future.failed(new Exception("getAllocationGroupState not configured"))

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache
    val typeRegistry: TypeRegistry = null

    def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] = Future.failed(new Exception("TODO"))
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

  // Task 1: fetchState on empty cache fetches state and caches it
  test("fetchState fetches and caches state on cache miss") {
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

    cache.fetchState(groupId).map { result =>
      result should be(state)
      stateCallCount should be(1)
      // pointer fetch is fire-and-forget in case 3, state fetch is the main path
      pointerCallCount should be(1)
    }
  }

  // Task 2: cached state is returned on second call
  test("fetchState returns cached state on second call without re-fetching") {
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
  }

  // Task 3: stale state triggers background refresh
  test("stale state triggers background refresh and returns cached state immediately") {
    val m = new TClient(clientId)
    var stateCallCount = 0

    m.getAllocationGroupPointerFn = id =>
      Future.successful(pointer)

    m.getAllocationGroupStateFn = id =>
      stateCallCount += 1
      if stateCallCount == 1 then Future.successful(state)
      else Future.successful(state2)

    // Use a refresh duration that the first sleep will exceed
    val cache = new AllocationGroupCache(m, refreshDuration = Duration(100, MILLISECONDS))

    for
      result1 <- cache.fetchState(groupId)
      // Wait long enough to ensure the state is stale
      _ <- Future { Thread.sleep(150) }
      result2 <- cache.fetchState(groupId)
      // The stale state should be returned immediately
      _ = result2 should be(state)
      // Wait for the background refresh to complete
      _ <- Future { Thread.sleep(50) }
      result3 <- cache.fetchState(groupId)
    yield
      result1 should be(state)
      // After refresh completes, the new state should be returned
      result3 should be(state2)
      stateCallCount should be(2)
  }

end AllocationGroupCacheSuite
