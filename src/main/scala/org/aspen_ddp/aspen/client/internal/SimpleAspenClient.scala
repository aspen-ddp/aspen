package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.{ReadManager, SimpleReadDriver}
import org.aspen_ddp.aspen.client.internal.transaction.{SimpleClientTransactionDriver, TransactionImpl, TransactionManager}
import org.aspen_ddp.aspen.client.registries.Registry.DuplicateRegistration
import org.aspen_ddp.aspen.client.registries.{NamespacedUUIDRegistry, UUIDObjectRegistry}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}

import java.util.UUID
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

class SimpleAspenClient(val msngr: ClientMessenger,
                        override val clientId: ClientId,
                        val executionContext: ExecutionContext,
                        val radicle: KeyValueObjectPointer,
                        txStatusCacheDuration: FiniteDuration,
                        initialReadDelay: Duration,
                        maxReadDelay: Duration,
                        txRetransmitDelay: Duration,
                        allocationRetransmitDelay: Duration,
                        userTypeFactories: List[RegisteredTypeFactory] = Nil) extends AspenClient:

  import SimpleAspenClient.*

  given ec: ExecutionContext = executionContext

  var attributes: Map[String, String] = Map()

  val typeRegistry: TypeRegistry = TypeRegistry(
    org.aspen_ddp.aspen.common.TypeFactories.factories,
    org.aspen_ddp.aspen.client.TypeFactories.factories,
    org.aspen_ddp.aspen.server.TypeFactories.factories,
    userTypeFactories
  )

  override val txStatusCache: TransactionStatusCache = new TransactionStatusCache(txStatusCacheDuration)

  private val rmgr = new ReadManager(this,
    new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply)

  private val objectRegistry = new UUIDObjectRegistry(this, radicle, Radicle.ObjectRegistryKey)
  private val namespacedRegistry = new NamespacedUUIDRegistry(this, radicle, Radicle.NamespacedRegistryKey)

  private val storagePoolsTree = new MetadataTree(this, radicle, Radicle.StoragePoolsTreeKey)
  private val allocationGroupsTree = new MetadataTree(this, radicle, Radicle.AllocationGroupsTreeKey)
  private val hostsTree = new MetadataTree(this, radicle, Radicle.HostsTreeKey)
  private val storageDevicesTree = new MetadataTree(this, radicle, Radicle.StorageDevicesTreeKey)

  private val allocatorManager = new ObjectAllocatorManager(this)

  override def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    getStoragePool(pointer.poolId).flatMap { pool =>
      rmgr.read(pointer, pool.ida, comment).map(_.asInstanceOf[DataObjectState])
    }

  override def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    getStoragePool(pointer.poolId).flatMap { pool =>
      rmgr.read(pointer, pool.ida, comment).map(_.asInstanceOf[KeyValueObjectState])
    }

  private val txManager = new TransactionManager(this, SimpleClientTransactionDriver.factory(txRetransmitDelay))
  
  override def newTransaction(): Transaction =
    TransactionImpl(this, txManager, _ => 0, None)

  override def getAllocator(allocatorId: ObjectAllocatorId): Future[ObjectAllocator] =
    allocatorManager.getAllocator(allocatorId)
  
  override def getStoragePoolId(poolName: String): Future[PoolId] =
    namespacedRegistry.getRegisteredObject("pool", poolName).map(PoolId(_))

  override def getHostId(hostName: String): Future[HostId] =
    namespacedRegistry.getRegisteredObject("host", hostName).map(HostId(_))

  override def getAllocationGroupId(groupName: String): Future[AllocationGroupId] =
    namespacedRegistry.getRegisteredObject("group", groupName).map(AllocationGroupId(_))


  override def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] =
    storagePoolsTree.get(poolId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer] =
    hostsTree.get(hostId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] =
    storageDevicesTree.get(storageDeviceId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer] =
    allocationGroupsTree.get(allocationGroupId.uuid).map(_.asInstanceOf[DataObjectPointer])


  override def createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId] =
    val ags = AllocationGroupState(
      AllocationGroupId(UUID.randomUUID()),
      level,
      groupName,
      Nil,
      Nil
    )

    def onFail(err: Throwable): Future[Unit] = err match
      case e: DuplicateRegistration => throw StopRetrying(e)

    transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr <- bsPool.allocator.allocateDataObject(DataBuffer(ags.toBytes))
        _ <- allocationGroupsTree.preparePut(ags.groupId.uuid, ptr)
        _ <- namespacedRegistry.prepareRegisterObject("group", ags.name, ags.groupId.uuid)
      yield
        ags.groupId

  override protected def createStoragePool(config: StoragePoolState): Future[PoolId] = {
    // TODO: Need Recovery. Will forever retry if creating a pool with name that already exists!
    transactUntilSuccessful: tx =>
      given Transaction = tx
  
      def createPoolObj(alloc: ObjectAllocator): Future[KeyValueObjectPointer] =
        for
          errTreeRoot <- alloc.allocateKeyValueObject(Map())
          allocTreeRoot <- alloc.allocateKeyValueObject(Map())

          nodeAllocator = SinglePoolNodeAllocator(this, radicle.poolId)

          poolConfig = config.encode()
          errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), nodeAllocator).encode()
          allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), nodeAllocator).encode()

          poolPtr <- alloc.allocateKeyValueObject(Map(
            StoragePoolState.ConfigKey -> Value(poolConfig),
            StoragePoolState.ErrorTreeKey -> Value(errorTree),
            StoragePoolState.AllocationTreeKey -> Value(allocTree)
          ))
        yield 
          poolPtr

      case class DeviceUpdate(storageDeviceId: StorageDeviceId,
                              pointer: KeyValueObjectPointer,
                              kvos: KeyValueObjectState,
                              stores: List[StoreId],
                              state: StorageDeviceState)

      def collectDevices(stores: Array[StoragePoolState.StoreEntry]): List[Future[DeviceUpdate]] =
        val devMap = stores.zipWithIndex.foldLeft(Map[StorageDeviceId, List[StoreId]]()): (m, tpl) =>
          val (entry, poolIndex) = tpl
          val storeId = StoreId(config.poolId, poolIndex.toByte)
          val l = m.get(entry.storageDeviceId) match
            case None => storeId :: Nil
            case Some(lst) => storeId :: lst
          m + (entry.storageDeviceId -> l)

        devMap.map { (storageDeviceId, stores) =>
          for
            devPtr <- client.getStorageDevicePointer(storageDeviceId)
            devKvos <- client.read(devPtr)
          yield
            DeviceUpdate(storageDeviceId, devPtr, devKvos, stores, StorageDeviceState(devKvos))
        }.toList

      def updateDevice(du: DeviceUpdate): Unit =
        val updates = du.stores.map { storeId =>
          storeId -> StorageDeviceState.StoreEntry(
            StorageDeviceState.StoreStatus.Initializing,
            None
          )
        }.toMap
        
        val newState = du.state.copy(stores = du.state.stores ++ updates)
        
        val reqs = List(KeyRevision(StorageDeviceState.StateKey, du.kvos.contents(StorageDeviceState.StateKey).revision))
        val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

        tx.update(du.pointer, None, None, reqs, ops)
        
        tx.result.foreach: _ =>
          val msg = CheckStorageDevice(
            du.state.hostId,
            clientId,
            du.storageDeviceId
          )
          sendHostMessage(msg)
      
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        poolPtr <- createPoolObj(bsPool.allocator)
        _ <- storagePoolsTree.preparePut(config.poolId.uuid, poolPtr)
        _ <- namespacedRegistry.prepareRegisterObject("pool", config.name, config.poolId.uuid)
        devUpdates <- Future.sequence(collectDevices(config.stores))
      yield 
        devUpdates.foreach(updateDevice)
        
        config.poolId
  }

  override def shutdown(): Unit = backgroundTaskManager.shutdown(Duration(50, MILLISECONDS))

  val retryStrategy: RetryStrategy = new ExponentialBackoffRetryStrategy(this)

  val backgroundTaskManager: BackgroundTaskManager = new BackgroundTaskManager(executionContext)

  def clientContext: ExecutionContext = executionContext

  def opportunisticRebuildManager: OpportunisticRebuildManager = new SimpleOpportunisticRebuildManager(this)

  val messenger: ClientMessenger = msngr

  val objectCache: ObjectCache = new SimpleObjectCache

  def receiveClientResponse(msg: ClientResponse): Unit = msg match
    case m: ReadResponse => rmgr.receive(m)
    case m: TransactionCompletionResponse => rmgr.receive(m)
    case m: TransactionResolved => txManager.receive(m)
    case m: TransactionFinalized => txManager.receive(m)

  private[aspen] def sendHostMessage(msg: HostMessage): Unit =
    messenger.sendHostMessage(msg)

  def getSystemAttribute(key: String): Option[String] = attributes.get(key)
  def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value

  override def getCachedAllocator(allocatorId: ObjectAllocatorId): Option[ObjectAllocator] =
    allocatorManager.get(allocatorId)

  override def cacheAllocator(allocator: ObjectAllocator): Unit =
    allocatorManager.put(allocator)

