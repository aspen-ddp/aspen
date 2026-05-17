package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, ExponentialBackoffRetryStrategy, KeyValueObjectState, ObjectAllocator, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import org.aspen_ddp.aspen.client.internal.allocation.{AllocationManager, SuperSimpleAllocationDriver}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.internal.read.{ReadManager, SimpleReadDriver}
import org.aspen_ddp.aspen.client.internal.transaction.{SimpleClientTransactionDriver, TransactionImpl, TransactionManager}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{AllocateResponse, CheckStorageDevice, ClientId, ClientResponse, HostMessage, ReadResponse, TransactionCompletionResponse, TransactionFinalized, TransactionResolved}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{KeyRequirement, KeyRevision}
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, byte2uuid, uuid2byte}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration, HOURS, MILLISECONDS}

class SimpleAspenClient(val msngr: ClientMessenger,
                        override val clientId: ClientId,
                        val executionContext: ExecutionContext,
                        val radicle: KeyValueObjectPointer,
                        txStatusCacheDuration: FiniteDuration,
                        initialReadDelay: Duration,
                        maxReadDelay: Duration,
                        txRetransmitDelay: Duration,
                        allocationRetransmitDelay: Duration) extends AspenClient:
  
  given ec: ExecutionContext = executionContext

  var attributes: Map[String, String] = Map()

  val typeRegistry: TypeRegistry = new TypeRegistry(StaticTypeRegistry.types.toMap)

  override val txStatusCache: TransactionStatusCache = new TransactionStatusCache(txStatusCacheDuration)

  private val rmgr = new ReadManager(this,
    new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply)
  
  private val storagePoolTree     = new TieredKeyValueList(this, KVObjectRootManager(this, Radicle.PoolTreeKey, radicle))
  private val storagePoolNameTree = new TieredKeyValueList(this, KVObjectRootManager(this, Radicle.PoolNameTreeKey, radicle))
  private val hostTree            = new TieredKeyValueList(this, KVObjectRootManager(this, Radicle.HostsTreeKey, radicle))
  private val hostNameTree        = new TieredKeyValueList(this, KVObjectRootManager(this, Radicle.HostsNameTreeKey, radicle))
  private val storageDeviceTree   = new TieredKeyValueList(this, KVObjectRootManager(this, Radicle.StorageDeviceTreeKey, radicle))

  override def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    rmgr.read(pointer, comment).map(_.asInstanceOf[DataObjectState])

  override def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    rmgr.read(pointer, comment).map(_.asInstanceOf[KeyValueObjectState])

  private val txManager = new TransactionManager(this, SimpleClientTransactionDriver.factory(txRetransmitDelay))
  
  override def newTransaction(): Transaction =
    TransactionImpl(this, txManager, _ => 0, None)
  
  override def getStoragePoolId(poolName: String): Future[PoolId] =
    storagePoolNameTree.get(Key(poolName)).map:
      case None => throw new NoSuchElementException(poolName)
      case Some(vs) => PoolId(byte2uuid(vs.value.bytes))

  override def getHostId(hostName: String): Future[HostId] =
    hostNameTree.get(Key(hostName)).map:
      case None => throw new NoSuchElementException(hostName)
      case Some(vs) => HostId(byte2uuid(vs.value.bytes))

  override def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] =
    storagePoolTree.get(Key(poolId.uuid)).map:
      case None => throw new NoSuchElementException(poolId.toString)
      case Some(vs) => KeyValueObjectPointer(vs.value.bytes)

  override def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer] =
    hostTree.get(Key(hostId.uuid)).map:
      case None => throw new NoSuchElementException(hostId.toString)
      case Some(vs) => KeyValueObjectPointer(vs.value.bytes)

  override def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] =
    storageDeviceTree.get(Key(storageDeviceId.uuid)).map:
      case None => throw new NoSuchElementException(storageDeviceId.toString)
      case Some(vs) => KeyValueObjectPointer(vs.value.bytes)

  override protected def createStoragePool(config: StoragePoolState): Future[PoolId] =
    transactUntilSuccessful: tx =>
      given Transaction = tx
  
      def createPoolObj(alloc: ObjectAllocator): Future[KeyValueObjectPointer] =
        for
          radicleKvos <- read(radicle)
          
          revisionGuard = ObjectRevisionGuard(radicleKvos.pointer, radicleKvos.revision)
          
          errTreeRoot <- alloc.allocateKeyValueObject(revisionGuard, Map())
          allocTreeRoot <- alloc.allocateKeyValueObject(revisionGuard, Map())
          
          nodeAllocator = SinglePoolNodeAllocator(this, radicle.poolId)
  
          poolConfig = config.encode()
          errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), nodeAllocator).encode()
          allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), nodeAllocator).encode()
          
          poolPtr <- alloc.allocateKeyValueObject(revisionGuard, Map(
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
        poolPtr <- createPoolObj(bsPool.defaultAllocator)
        _ <- storagePoolTree.set(Key(config.poolId.uuid), Value(poolPtr.toArray))
        _ <- storagePoolNameTree.set(Key(config.name), Value(uuid2byte(config.poolId.uuid)))
        devUpdates <- Future.sequence(collectDevices(config.stores))
      yield 
        devUpdates.foreach(updateDevice)
        
        config.poolId

  override def shutdown(): Unit = backgroundTaskManager.shutdown(Duration(50, MILLISECONDS))

  val retryStrategy: RetryStrategy = new ExponentialBackoffRetryStrategy(this)

  val backgroundTaskManager: BackgroundTaskManager = new BackgroundTaskManager(executionContext)

  def clientContext: ExecutionContext = executionContext

  def opportunisticRebuildManager: OpportunisticRebuildManager = new SimpleOpportunisticRebuildManager(this)

  val messenger: ClientMessenger = msngr

  val allocationManager: AllocationManager = new AllocationManager(this,
    SuperSimpleAllocationDriver.factory(allocationRetransmitDelay))

  val objectCache: ObjectCache = new SimpleObjectCache

  def receiveClientResponse(msg: ClientResponse): Unit = msg match
    case m: ReadResponse => rmgr.receive(m)
    case m: TransactionCompletionResponse => rmgr.receive(m)
    case m: TransactionResolved => txManager.receive(m)
    case m: TransactionFinalized => txManager.receive(m)
    case m: AllocateResponse => allocationManager.receive(m)

  private[aspen] def sendHostMessage(msg: HostMessage): Unit =
    messenger.sendHostMessage(msg)

  def getSystemAttribute(key: String): Option[String] = attributes.get(key)
  def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value

