package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ClientId, ClientResponse, HostMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, uuid2byte}
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
import org.aspen_ddp.aspen.server.store.backend.BackendConfig

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object AspenClient:
  class InvalidDestination extends Exception("Source and destination devices must be different")
  class StoreNotActive(storeId: StoreId) extends Exception(s"Store $storeId is not in the Active state")

trait AspenClient extends ObjectReader:
  
  import AspenClient.*
  
  val clientId: ClientId

  val txStatusCache: TransactionStatusCache

  val typeRegistry: TypeRegistry

  protected var storagePoolCache: Map[PoolId, StoragePool] = Map.empty

  def clientContext: ExecutionContext

  def client: AspenClient = this

  def shutdown(): Unit = ()

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]

  def newTransaction(): Transaction

  def getStoragePool(poolId: PoolId): Future[StoragePool] =
    given ExecutionContext = this.clientContext
    synchronized:
      storagePoolCache.get(poolId) match
        case Some(pool) => Future.successful(pool)
        case None =>
          getStoragePoolPointer(poolId).flatMap: pointer =>
            read(pointer).map: kvos =>
              val pool = SimpleStoragePool(this, kvos)
              synchronized:
                storagePoolCache += poolId -> pool
              pool

  def getStoragePoolState(poolId: PoolId): Future[StoragePoolState] =
    given ExecutionContext = this.clientContext
    getStoragePoolPointer(poolId).flatMap: pointer =>
      read(pointer).map: kvos =>
        StoragePoolState(kvos)

  def getHostState(hostId: HostId): Future[HostState] =
    given ExecutionContext = this.clientContext
    getHostPointer(hostId).flatMap: pointer =>
      read(pointer).map: kvos =>
        HostState(kvos)
  
  def getStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    given ExecutionContext = this.clientContext
    getStorageDevicePointer(storageDeviceId).flatMap: pointer =>
      read(pointer).map: kvos =>
        StorageDeviceState(kvos)

  def getAllocationGroupState(allocationGroupId: AllocationGroupId): Future[AllocationGroupState] =
    given ExecutionContext = this.clientContext
    getAllocationGroupPointer(allocationGroupId).flatMap: pointer =>
      read(pointer).map: dos =>
        AllocationGroupState(dos)
        
  def getStoragePoolId(poolName: String): Future[PoolId]
  def getHostId(hostName: String): Future[HostId]
  def getAllocationGroupId(groupName: String): Future[AllocationGroupId]
  
  private[aspen] def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer]
  private[aspen] def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer]
  private[aspen] def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer]
  private[aspen] def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer]

  protected def createStoragePool(config: StoragePoolState): Future[PoolId]

  def createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId]

  def transact[T](prepare: Transaction => Future[T])(using ec: ExecutionContext): Future[T] =
    val tx = newTransaction()

    val fprep = try prepare(tx) catch
      case err: Throwable => Future.failed(err)

    val fresult = for
      prepResult <- fprep
      _ <- tx.commit()
    yield prepResult

    fresult.failed.foreach(err => tx.invalidateTransaction(err))

    fresult

  def transactUntilSuccessful[T](prepare: Transaction => Future[T])(using ec: ExecutionContext): Future[T] =
    retryStrategy.retryUntilSuccessful:
      transact(prepare)
    
  def transactUntilSuccessfulWithRecovery[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T])(using ec: ExecutionContext): Future[T] =
    retryStrategy.retryUntilSuccessful(onCommitFailure):
      transact(prepare)
      
  def createNewStoragePool(name: String,
                           ida: IDA,
                           maxObjectSize: Option[Int],
                           storageDeviceIds: List[StorageDeviceId],
                           backendConfig: BackendConfig,
                           maximumStoreSize: Long): Future[PoolId] =
    if storageDeviceIds.size < ida.width then
      Future.failed(new IllegalArgumentException("storageDeviceIds list must be at least as long as ida.width"))
    else
      given ExecutionContext = this.clientContext
      val poolId = PoolId(UUID.randomUUID())
      for
        devices <- Future.sequence(storageDeviceIds.map(sid => getStorageDeviceState(sid)))
        stores = devices.map(dev => StoragePoolState.StoreEntry(dev.hostId, dev.storageDeviceId)).toArray
        config = StoragePoolState(
          poolId,
          name,
          ida,
          maxObjectSize,
          stores,
          backendConfig,
          0L,
          maximumStoreSize
        )
        _ <- createStoragePool(config)
      yield
        poolId
      
  def transferStore(storeId: StoreId, destinationId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = this.clientContext

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: InvalidDestination => throw StopRetrying(e)
      case e: StoreNotActive => throw StopRetrying(e)
      
    transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx
      
      for
        pool <- getStoragePool(storeId.poolId)
        poolState <- pool.getState()
        sourceId = poolState.stores(storeId.poolIndex).storageDeviceId
        srcPtr <- getStorageDevicePointer(sourceId)
        srcKvos <- read(srcPtr)
        srcState = StorageDeviceState(srcKvos)
        dstPtr <- getStorageDevicePointer(destinationId)
        dstKvos <- read(dstPtr)
        dstState = StorageDeviceState(dstKvos)
      yield
        if sourceId == destinationId then
          throw InvalidDestination()
          
        srcState.stores.get(storeId) match
          case None => throw StoreNotActive(storeId)
          case Some(entry) =>
            if entry.status != StorageDeviceState.StoreStatus.Active then
              throw StoreNotActive(storeId)
            
            // Update Source Device
            val newSrcEntry = StorageDeviceState.StoreEntry(
              StorageDeviceState.StoreStatus.TransferringOut,
              Some(destinationId)
            )
            val newSrcStores = srcState.stores + (storeId -> newSrcEntry)
            val newSrcState = srcState.copy(stores = newSrcStores)

            val srcReqs = List(KeyRevision(StorageDeviceState.StateKey, srcKvos.contents(StorageDeviceState.StateKey).revision))
            val srcOps = List(Insert(StorageDeviceState.StateKey, newSrcState.encode()))
            
            tx.update(srcPtr, None, None, srcReqs, srcOps)
            
            // Update Destination Device
            val newDstEntry = StorageDeviceState.StoreEntry(
              StorageDeviceState.StoreStatus.TransferringIn,
              Some(sourceId)
            )
            val newDstStores = dstState.stores + (storeId -> newDstEntry)
            val newDstState = dstState.copy(stores = newDstStores)

            val dstReqs = List(KeyRevision(StorageDeviceState.StateKey, dstKvos.contents(StorageDeviceState.StateKey).revision))
            val dstOps = List(Insert(StorageDeviceState.StateKey, newDstState.encode()))

            tx.update(dstPtr, None, None, dstReqs, dstOps)
            
            tx.result.foreach: _ =>
              val msg = CheckStorageDevice(
                dstState.hostId,
                clientId,
                destinationId
              )
              sendHostMessage(msg)

  def getBootstrapConfig(): Future[String] =
    given ExecutionContext = this.clientContext
    
    client.read(radicle).map: radicleKvos =>
      new String(radicleKvos.contents(Radicle.BootstrapConfigKey).value.bytes, StandardCharsets.UTF_8)
      
  def retryStrategy: RetryStrategy

  def backgroundTaskManager: BackgroundTaskManager

  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager

  private[client] val messenger: Messenger

  private[client] val objectCache: ObjectCache
  
  private[aspen] val radicle: KeyValueObjectPointer

  private[aspen] def receiveClientResponse(msg: ClientResponse): Unit
  private[aspen] def sendHostMessage(msg: HostMessage): Unit

  private[aspen] def getSystemAttribute(key: String): Option[String]
  private[aspen] def setSystemAttribute(key: String, value: String): Unit

