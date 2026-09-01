package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ClientResponse, HostMessage, ServiceMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.uuid2byte
import org.aspen_ddp.aspen.server.store.backend.BackendConfig
import scribe.Logging

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object AspenClient:
  class InvalidDestination extends Exception("Source and destination devices must be different")
  class StoreNotActive(storeId: StoreId) extends Exception(s"Store $storeId is not in the Active state")
  class InvalidDeviceSetLevel(childLevel: Int, parentLevel: Int)
    extends Exception(s"Device set level $childLevel must be less than parent level $parentLevel")
  /** A store was about to be written onto a device an operator has declared dead. Terminal:
   *  retrying cannot help, since a tombstone is one-way. */
  class DeviceFailed(deviceId: StorageDeviceId)
    extends Exception(s"Storage device ${deviceId.uuid} has been declared failed")

/** The primary interface for applications using Aspen object storage: object allocation,
 *  transaction building, and object retrieval.
 *
 *  @see [[ReadDriverClient]], which declares the members a ReadDriver needs. They are not
 *       repeated here.
 */
trait AspenClient extends ObjectReader, ReadDriverClient, Logging:
  
  import AspenClient.*

  val typeRegistry: TypeRegistry

  protected var storagePoolCache: Map[PoolId, StoragePool] = Map.empty

  def client: AspenClient = this

  def shutdown(): Unit = ()

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]

  def newTransaction(): Transaction

  def getAllocator(allocatorId: ObjectAllocatorId): Future[ObjectAllocator]

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

  def getStorageDeviceSetState(storageDeviceSetId: StorageDeviceSetId): Future[StorageDeviceSetState] =
    given ExecutionContext = this.clientContext
    getStorageDeviceSetPointer(storageDeviceSetId).flatMap: pointer =>
      read(pointer).map: dos =>
        StorageDeviceSetState(dos)

  def getStoragePoolId(poolName: String): Future[PoolId]
  def getHostId(hostName: String): Future[HostId]
  def getAllocationGroupId(groupName: String): Future[AllocationGroupId]
  def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId]

  def listStoragePools(): Future[List[(String, PoolId)]]
  def listHosts(): Future[List[(String, HostId)]]
  def listAllocationGroups(): Future[List[(String, AllocationGroupId)]]
  def listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]]

  private[aspen] def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer]
  private[aspen] def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer]
  private[aspen] def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer]
  private[aspen] def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer]
  private[aspen] def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer]

  protected def createStoragePool(config: StoragePoolState): Future[PoolId]

  def createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId]

  def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId]

  /** Register a new host, in a single atomic transaction: a new HostState object is allocated
   *  and registered in the hosts tree, and `name` is registered in the host namespace.
   *
   *  Fails with KeyAlreadyExists if `name` is already registered, and does not retry: a name
   *  collision is an operator error rather than a transient one.
   *
   *  The host is created with no storage devices. Devices are added afterwards by
   *  createStorageDevice, which is also what makes the host useful for anything. Nothing here
   *  touches the new machine's filesystem -- see server.HostManager.createHost for that. */
  def createHost(name: String,
                 address: String,
                 dataPort: Int,
                 cncPort: Int,
                 storeTransferPort: Int): Future[HostId]

  /** Register a new storage device on `hostId` and place it in the level-0 device set
   *  `deviceSetId`, in a single atomic transaction: a new StorageDeviceState object is
   *  allocated and registered in the storage devices tree, the owning HostState gains the
   *  device id, and the set's memberDevices gains the device id.
   *
   *  Fails with NoSuchElementException if the host or the set is unknown, or with
   *  StorageDeviceSetState.NotLevelZero if the set is not level 0.
   *
   *  No store is created. Stores appear on the device when a pool is assigned to it. */
  def createStorageDevice(hostId: HostId, deviceSetId: StorageDeviceSetId): Future[StorageDeviceId]

  /** Add a storage pool to an allocation group, both identified by name. Fails with
   *  NoSuchElementException if either name is not registered. Uses the system durable
   *  task path for any usage cascade (no local TaskExecutor). */
  def addPoolToGroup(poolName: String, groupName: String): Future[Unit] =
    given ExecutionContext = this.clientContext
    for
      poolId  <- getStoragePoolId(poolName)
      groupId <- getAllocationGroupId(groupName)
      _       <- AllocationGroupState.addPool(this, poolId, groupId, None)
    yield ()

  /** Nest a source allocation group inside a destination allocation group, both
   *  identified by name. The destination group's level must be strictly greater than
   *  the source group's level (enforced by AllocationGroupState.addGroup, which throws
   *  AllocationGroupState.InvalidLevel otherwise). Fails with NoSuchElementException if
   *  either name is not registered. Uses the system durable task path for any usage
   *  cascade (no local TaskExecutor). */
  def addGroupToGroup(sourceGroupName: String, destGroupName: String): Future[Unit] =
    given ExecutionContext = this.clientContext
    for
      childId  <- getAllocationGroupId(sourceGroupName)
      parentId <- getAllocationGroupId(destGroupName)
      _        <- AllocationGroupState.addGroup(this, childId, parentId, None)
    yield ()

  /** Move a storage device (by id) from its current level-0 device set into
   *  `targetSetId` (which must also be level 0), in a single atomic transaction.
   *  Fails with NoSuchElementException if the device or target set is unknown, or
   *  StorageDeviceSetState.NotLevelZero if the target set is not level 0. */
  def moveDeviceToSet(deviceId: StorageDeviceId, targetSetId: StorageDeviceSetId): Future[Unit] =
    StorageDeviceSetState.moveDevice(this, deviceId, targetSetId)

  /** Begin migrating every store of `poolId` onto devices of `targetSetId`.
   *
   *  One transaction flips the pool's `storageDeviceSet`, records
   *  `Migration(targetSetId, InProgress)`, swaps the pool between the two sets'
   *  `assignedPools`, strips the pool's stores from both sets' `pendingTransfers`, and (on a
   *  first call) enrolls a MigratePoolToSetDurableTask with the system task executor. There is
   *  therefore no window in which a pool is InProgress with no task driving it.
   *
   *  Returns as soon as that transaction commits; progress is observable via the pool's
   *  `migration` field. A call naming the pool's current set is a silent no-op. A call against
   *  a pool that is already migrating retargets it, reusing the running task.
   *
   *  Fails with NoSuchElementException if the pool or the target set does not exist.
   */
  def migratePoolToSet(poolId: PoolId, targetSetId: StorageDeviceSetId): Future[Unit]

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
                           backendConfig: BackendConfig,
                           storageDeviceSet: StorageDeviceSetId,
                           maximumStoreSize: Long): Future[PoolId] =
    given ExecutionContext = this.clientContext
    val poolId = PoolId(UUID.randomUUID())
    for
      set <- getStorageDeviceSetState(storageDeviceSet)
      deviceIds <- set.selectDevicesForPool(ida.width, this)
      devices <- Future.sequence(deviceIds.map(sid => getStorageDeviceState(sid)))
      // A tombstoned device's zeroed storageDeviceId would otherwise flow into
      // StoragePoolState.StoreEntry on the next line and blow up collectDevices' tree lookup
      // before any downstream guard can run. Report the real id, not the zeroed one.
      _ = deviceIds.zip(devices).find((_, dev) => dev.isFailed).foreach((sid, _) => throw DeviceFailed(sid))
      stores = devices.map(dev => StoragePoolState.StoreEntry(dev.hostId, dev.storageDeviceId)).toArray
      config = StoragePoolState(
        poolId,
        name,
        ida,
        maxObjectSize,
        stores,
        backendConfig,
        storageDeviceSet,
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
      case e: DeviceFailed => throw StopRetrying(e)

    val fStaged: Future[CheckStorageDevice] = transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx

      for
        // getStoragePoolState, not getStoragePool(...).getState(): SimpleStoragePool caches its
        // StoragePoolState from construction and nothing ever calls dropCachedState(), so the
        // cached copy is a permanent snapshot of where the stores were when this client first
        // touched the pool. The source device must be read fresh -- a store that has already
        // moved once (a migration retarget, or two successive rebalances) would otherwise be
        // transferred out of the device it used to live on. When the stale source happens to
        // equal the true destination this surfaces as a StopRetrying InvalidDestination and
        // wedges the caller; when it does not, it marks the wrong device TransferringOut.
        poolState <- getStoragePoolState(storeId.poolId)
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

        // A store on a tombstoned device is silently lost: nothing reconciles it, because
        // reconcileDeviceState ignores failed devices outright.
        if dstState.isFailed then
          throw DeviceFailed(destinationId)

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

            // Built here, sent by the map below once the transaction has committed.
            // Registering this on tx.result instead would send it from a task the returned
            // future does not wait on: tx.commit() hands back the same promise as tx.result,
            // so a caller that exits the moment this future completes could lose the message.
            // Each retry attempt builds its own, and only the successful attempt's value
            // reaches the map below, so a retried transaction cannot double-send.
            CheckStorageDevice(
              dstState.hostId,
              clientId,
              destinationId
            )

    fStaged.map(sendBestEffortHostMessage)

  def getBootstrapConfig(): Future[String] =
    given ExecutionContext = this.clientContext

    client.read(radicle).map: radicleKvos =>
      new String(radicleKvos.contents(Radicle.BootstrapConfigKey).value.bytes, StandardCharsets.UTF_8)

  def retryStrategy: RetryStrategy

  /** Hosts currently believed to be offline. Stub returns empty until host
   *  liveness tracking exists; used by rebalancing to avoid moving stores onto,
   *  or counting availability from, offline hosts. */
  def offlineHosts(): Set[HostId] = Set()

  private[aspen] val radicle: KeyValueObjectPointer

  private[aspen] def receiveClientResponse(msg: ClientResponse): Unit
  private[aspen] def sendHostMessage(msg: HostMessage): Unit

  /** Sends a message that is purely a latency optimization: the receiving host reaches the
   *  same state on its own through its periodic polling, just later. A failure to send one
   *  must therefore never fail the operation that produced it.
   */
  private[aspen] def sendBestEffortHostMessage(msg: HostMessage): Unit =
    try sendHostMessage(msg)
    catch
      case t: Throwable => logger.warn(s"Failed to send best-effort host message $msg: $t")

  def sendServiceMessage(serviceUUID: UUID, encodedContent: Array[Byte]): Future[Unit] =
    given ExecutionContext = clientContext
    getServiceHost(serviceUUID).map:
      case Some(hostId) =>
        sendHostMessage(ServiceMessage(hostId, clientId, serviceUUID, encodedContent))
      case None => ()

  /** Create and enroll a system-level DurableTask, executed by the SystemTaskExecutorService
   *  on some host. Retries until the task object is created and enrolled. The returned Future
   *  completes at enrollment; there is no task-completion notification.
   *
   *  `taskTypeUUID` must resolve to a DurableTaskFactory in the type registry. */
  def createSystemDurableTask(taskTypeUUID: UUID,
                              initialState: Map[Key, Array[Byte]]): Future[Unit]

  /** Stage the allocation and enrollment of a system-level DurableTask into the supplied
   *  transaction. The returned future resolves once both operations are staged in `tx`;
   *  commit and retry are the caller's responsibility. A best-effort wake-up message to the
   *  SystemTaskExecutorService is sent after the transaction commits.
   *
   *  `taskTypeUUID` must resolve to a DurableTaskFactory in the type registry. */
  def prepareSystemDurableTask(taskTypeUUID: UUID,
                               initialState: Map[Key, Array[Byte]])
                              (using tx: Transaction): Future[Unit]

  private[aspen] def getServiceHost(serviceUUID: UUID): Future[Option[HostId]]

  private[aspen] def setSystemAttribute(key: String, value: String): Unit

  private[aspen] def getCachedAllocator(allocatorId: ObjectAllocatorId): Option[ObjectAllocator] = None
  private[aspen] def cacheAllocator(allocator: ObjectAllocator): Unit = ()

