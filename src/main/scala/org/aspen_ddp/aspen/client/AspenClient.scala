package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.allocation.AllocationManager
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
import org.aspen_ddp.aspen.server.store.backend.BackendConfig

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait AspenClient extends ObjectReader:

  val clientId: ClientId

  val txStatusCache: TransactionStatusCache

  val typeRegistry: TypeRegistry

  def clientContext: ExecutionContext

  def client: AspenClient = this

  def shutdown(): Unit = ()

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]

  def newTransaction(): Transaction

  def getStoragePool(poolId: PoolId): Future[StoragePool] =
    given ExecutionContext = this.clientContext
    getStoragePoolPointer(poolId).flatMap: pointer =>
      read(pointer).map: kvos =>
        SimpleStoragePool(this, kvos)
  
  def getHost(hostId: HostId): Future[Host] =
    given ExecutionContext = this.clientContext
    getHostPointer(hostId).flatMap: pointer =>
      read(pointer).map: kvos =>
        Host(kvos)
  
  def getStorageDevice(storageDeviceId: StorageDeviceId): Future[StorageDevice] =
    given ExecutionContext = this.clientContext
    getStorageDevicePointer(storageDeviceId).flatMap: pointer =>
      read(pointer).map: kvos =>
        StorageDevice(kvos)
        
  def getStoragePoolId(poolName: String): Future[PoolId]
  def getHostId(hostName: String): Future[HostId]
  
  private[aspen] def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer]
  private[aspen] def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer]
  private[aspen] def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer]

  private[aspen] def updateStorageHost(storeId: StoreId, newHostId: HostId): Future[Unit]

  def newStoragePool(newPoolName: String,
                     hostCncFrontends: List[CnCFrontend],
                     ida: IDA,
                     backendType: BackendConfig): Future[StoragePool] = ???

  protected def createStoragePool(config: StoragePool.Config): Future[StoragePool]

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

  def retryStrategy: RetryStrategy

  def backgroundTaskManager: BackgroundTaskManager

  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager

  private[client] val messenger: Messenger

  private[client] val allocationManager: AllocationManager

  private[client] val objectCache: ObjectCache

  private[aspen] def receiveClientResponse(msg: ClientResponse): Unit

  private[aspen] def getSystemAttribute(key: String): Option[String]
  private[aspen] def setSystemAttribute(key: String, value: String): Unit

