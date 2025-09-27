package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, ExponentialBackoffRetryStrategy, Host, HostId, KeyValueObjectState, ObjectAllocator, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
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
                        allocationRetransmitDelay: Duration,
                        bootstrapHosts: Map[HostId, Host]) extends AspenClient {
  
  given ec: ExecutionContext = executionContext

  var attributes: Map[String, String] = Map()

  val typeRegistry: TypeRegistry = new TypeRegistry(StaticTypeRegistry.types.toMap)

  override val txStatusCache: TransactionStatusCache = new TransactionStatusCache(txStatusCacheDuration)

  private val rmgr = new ReadManager(this,
    new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply)

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    rmgr.read(pointer, comment).map(_.asInstanceOf[DataObjectState])

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    rmgr.read(pointer, comment).map(_.asInstanceOf[KeyValueObjectState])

  private val txManager = new TransactionManager(this, SimpleClientTransactionDriver.factory(txRetransmitDelay))

  def newTransaction(): Transaction =
    TransactionImpl(this, txManager, _ => 0, None)

  def getStoragePool(poolId: PoolId): Future[Option[StoragePool]] =
    val root = new KVObjectRootManager(this, Radicle.PoolTreeKey, radicle)
    val tkvl = new TieredKeyValueList(this, root)

    tkvl.get(Key(poolId.uuid)).flatMap:
      case None => Future.successful(None)
      case Some(poolPtr) => read(KeyValueObjectPointer(poolPtr.value.bytes)).map: poolKvos =>
        Some(SimpleStoragePool(this, poolKvos))

  def getStoragePool(poolName: String): Future[Option[StoragePool]] =
    if poolName.toLowerCase == "bootstrap" then
      getStoragePool(PoolId(new UUID(0,0)))
    else
      val root = new KVObjectRootManager(this, Radicle.PoolTreeKey, radicle)
      val tkvl = new TieredKeyValueList(this, root)

      tkvl.get(Key(poolName)).flatMap:
        case None => Future.successful(None)
        case Some(poolIdBytes) => getStoragePool(PoolId(byte2uuid(poolIdBytes.value.bytes)))

  override def updateStorageHost(storeId: StoreId, newHostId: HostId): Future[Unit] =
    val root = new KVObjectRootManager(this, Radicle.PoolTreeKey, radicle)
    val tkvl = new TieredKeyValueList(this, root)

    given tx: Transaction = newTransaction()
    
    def updateConfig(config: StoragePool.Config): Unit =
      config.storeHosts(storeId.poolIndex) = newHostId

    for
      optr <- tkvl.get(Key(storeId.poolId.uuid))
      ptrValue = optr match
        case None => throw new Exception(s"Pool not found: ${storeId.poolId}")
        case Some(ptrValue) => ptrValue
      poolPtr = KeyValueObjectPointer(ptrValue.value.bytes)
      currentKvos <- read(poolPtr)
      keyVal = currentKvos.contents.get(StoragePool.ConfigKey) match
        case None => throw new Exception(s"Invalid Pool Definition! Missing config key ${storeId.poolId}")
        case Some(kr) => kr
      poolConfig = StoragePool.Config(keyVal.value.bytes)
      _=updateConfig(poolConfig)
      _=tx.update(poolPtr, None, None,
        KeyRevision(StoragePool.ConfigKey, keyVal.revision) :: Nil,
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
      obsPool <- getStoragePool("bootstrap")
      bsPool = obsPool.get
      poolPtr <- createPoolObj(bsPool.defaultAllocator)
      _ <- tkvl.set(Key(config.poolId.uuid), Value(poolPtr.toArray))
      _ <- nameTkvl.set(Key(config.name), Value(uuid2byte(config.poolId.uuid)))
      _ <- tx.commit()
      poolKvos <- read(poolPtr)
    yield
      SimpleStoragePool(this, poolKvos)

  def getHost(hostId: HostId): Future[Option[Host]] =
    bootstrapHosts.get(hostId) match
      case Some(host) => return Future.successful(Some(host))
      case None =>

    val root = new KVObjectRootManager(this, Radicle.HostsTreeKey, radicle)
    val tkvl = new TieredKeyValueList(this, root)
    for
      ohostValue <- tkvl.get(Key(hostId.uuid))
    yield
      ohostValue match
        case Some(hostValue) => Some(Host(hostValue.value.bytes))
        case None => None

  def getHost(hostName: String): Future[Option[Host]] =
    val root = new KVObjectRootManager(this, Radicle.HostsNameTreeKey, radicle)
    val tkvl = new TieredKeyValueList(this, root)
    tkvl.get(Key(hostName)).flatMap {
        case Some(uuid) => getHost(HostId(byte2uuid(uuid.value.bytes)))
        case None => Future.successful(None)
    }


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
}
