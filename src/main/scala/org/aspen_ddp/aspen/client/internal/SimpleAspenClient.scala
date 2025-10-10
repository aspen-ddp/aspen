package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, ExponentialBackoffRetryStrategy, Host, HostId, KeyValueObjectState, ObjectAllocator, ObjectCache, RetryStrategy, StorageDeviceId, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import org.aspen_ddp.aspen.client.internal.allocation.{AllocationManager, SuperSimpleAllocationDriver}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.internal.read.{ReadManager, SimpleReadDriver}
import org.aspen_ddp.aspen.client.internal.transaction.{SimpleClientTransactionDriver, TransactionImpl, TransactionManager}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.network.{AllocateResponse, ClientId, ClientResponse, ReadResponse, TransactionCompletionResponse, TransactionFinalized, TransactionResolved}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{KeyRequirement, KeyRevision}
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, byte2uuid, uuid2byte}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}

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
  
  override def updateStorageHost(storeId: StoreId, newHostId: HostId): Future[Unit] =

    given tx: Transaction = newTransaction()
    
    def updateConfig(config: StoragePool.Config): Unit = ()
      //config.stores(storeId.poolIndex) = newHostId

    for
      poolPtr <- getStoragePoolPointer(storeId.poolId)
      currentKvos <- read(poolPtr)
      poolConfig = StoragePool.Config(currentKvos)
      _=updateConfig(poolConfig)
      _=tx.update(poolPtr, None, None,
        KeyRevision(StoragePool.ConfigKey, 
          currentKvos.contents(StoragePool.ConfigKey).revision) :: Nil,
        Insert(StoragePool.ConfigKey, poolConfig.encode()) :: Nil)
      _ <- tx.commit()
    yield
      ()

  override protected def createStoragePool(config: StoragePool.Config): Future[StoragePool] =
    val root = new KVObjectRootManager(this, Radicle.PoolTreeKey, radicle)
    val tkvl = new TieredKeyValueList(this, root)
    val nameRoot = new KVObjectRootManager(this, Radicle.PoolNameTreeKey, radicle)
    val nameTkvl = new TieredKeyValueList(this, nameRoot)

    given tx: Transaction = newTransaction()

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
          StoragePool.ConfigKey -> Value(poolConfig),
          StoragePool.ErrorTreeKey -> Value(errorTree),
          StoragePool.AllocationTreeKey -> Value(allocTree)
        ))
      yield poolPtr
    
    for
      bsPool <- getStoragePool(PoolId.BootstrapPoolId)
      poolPtr <- createPoolObj(bsPool.defaultAllocator)
      _ <- tkvl.set(Key(config.poolId.uuid), Value(poolPtr.toArray))
      _ <- nameTkvl.set(Key(config.name), Value(uuid2byte(config.poolId.uuid)))
      _ <- tx.commit()
      poolKvos <- read(poolPtr)
    yield
      SimpleStoragePool(this, poolKvos)

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

  def getSystemAttribute(key: String): Option[String] = attributes.get(key)
  def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value

