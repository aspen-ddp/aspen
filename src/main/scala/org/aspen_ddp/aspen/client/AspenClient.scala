package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.internal.transaction.{MissedUpdateMarker, TransactionBuilder}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.eventlog.EventLogState.ObserverConfig
import org.aspen_ddp.aspen.common.eventlog.{Event, EventFactory, EventLogEntry, EventLogReader, SystemEventLog}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, IDAPolicy, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, Pem}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ClientResponse, HostMessage, ServiceMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}
import org.aspen_ddp.aspen.server.store.backend.{BackendConfig, FsifConfig}
import scribe.Logging

import java.nio.charset.StandardCharsets
import java.security.PublicKey
import java.security.cert.X509Certificate
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object AspenClient:
  class InvalidDestination extends Exception("Source and destination devices must be different")
  class StoreNotActive(storeId: StoreId) extends Exception(s"Store $storeId is not in the Active state")
  class StoreNotRebuilding(storeId: StoreId) extends Exception(s"Store $storeId is not being rebuilt")
  class InvalidDeviceSetLevel(childLevel: Int, parentLevel: Int)
    extends Exception(s"Device set level $childLevel must be less than parent level $parentLevel")
  /** A store was about to be written onto a device an operator has declared dead. Terminal:
   *  retrying cannot help, since a tombstone is one-way. */
  class DeviceFailed(deviceId: StorageDeviceId)
    extends Exception(s"Storage device ${deviceId.uuid} has been declared failed")
  /** A store was about to be placed on a device that is shutting down or shut down. Selection
   *  skips such devices; this reports the race where one was chosen anyway. */
  class DeviceNotOperational(deviceId: StorageDeviceId)
    extends Exception(s"Storage device ${deviceId.uuid} is not operational")
  /** fail-storage-device was invoked against a device that already carries a tombstone.
   *  Terminal: the tombstone is one-way, and a recovered device is re-introduced under a new id. */
  class DeviceAlreadyFailed(deviceId: StorageDeviceId)
    extends Exception(s"Storage device ${deviceId.uuid} has already been declared failed")
  /** shutdown-device was refused: with the device's stores offline, pool `poolId` would have
   *  fewer stores on Operational devices than its IDA's write threshold, so nothing could write
   *  it -- and, for the bootstrap pool, nothing could read or write the metadata a shutdown and
   *  any later restart depend on. Terminal: the operator must first move stores off the device
   *  or bring other devices back into service. */
  class ShutdownWouldBreakPool(val poolId: PoolId)
    extends Exception(s"Shutting down the device would leave pool ${poolId.uuid} with fewer stores " +
                      s"on operational devices than its write threshold")
  /** createNewStoragePool was named a group that is not a metadata group, so it cannot hold the new pool's metadata
   *  trees. Thrown before any transaction. */
  class NotAMetadataGroup(val groupId: AllocationGroupId)
    extends Exception(s"Allocation group ${groupId.uuid} is not a metadata group")

/** The primary interface for applications using Aspen object storage: object allocation,
 *  transaction building, and object retrieval.
 *
 *  @see [[ReadDriverClient]], which declares the members a ReadDriver needs. They are not
 *       repeated here.
 */
trait AspenClient extends ObjectReader, ReadDriverClient, Logging:
  
  import AspenClient.*

  val typeRegistry: TypeRegistry

  protected var storagePoolCache: Map[PoolId, Future[StoragePool]] = Map.empty

  /** Tests only: handed every transaction this client commits, as built and before the pin check. Read at commit */
  @volatile private[aspen] var testingOnlyOnBuilt: Option[TransactionBuilder.Built => Unit] = None

  def client: AspenClient = this

  private[aspen] lazy val missedUpdateMarker: MissedUpdateMarker = new MissedUpdateMarker(this)

  def shutdown(): Unit = ()

  /** A reader whose reads give up after `timeout`, failing with ReadTimeout and sending no further requests.
    *
    * Reads are untimed by default: they retry until they restore the object or fail fatally, so a partition or
    * downed hosts only delay them. A timed reader is for a caller that must not wait forever, and has every read
    * method of the client. A ReadTimeout is not an InvalidObject: the object may well exist. The timeout is one
    * deadline for the whole call: the lookup of the object's pool (when not cached) and the object read. A pool load
    * a timed read gives up on keeps running for other callers. `Duration.Inf` returns this client; any other
    * duration must be finite and positive (IllegalArgumentException otherwise).
    */
  def withReadTimeout(timeout: scala.concurrent.duration.Duration): ObjectReader

  /** A reader whose every read is settled: the state it returns reflects every transaction that committed before the
    * read began, even one no store of the object has applied yet, where an ordinary read can return the state such a
    * transaction replaces when this client cannot tell it committed (it learned the commit only through another
    * object). A settled read waits for the transactions holding write locks in each store's first reply to resolve,
    * so it is slower on a busy object; for a reader that needs a transaction it saw applied on one object to show on
    * every object it reads next (the system event log reader, `client/internal/CLAUDE.md`). Untimed: a settled read
    * waits as long as a first-reply lock holder stays undecided, which lasts while a store of that transaction is
    * silent
    */
  private[aspen] def settledReader: ObjectReader

  /** A settledReader whose every read gives up at `deadline`, failing with ReadTimeout; a read begun past the
    * deadline fails at once. One deadline for a sequence of reads, such as the system event log reader's */
  private[aspen] def settledReaderBy(deadline: scala.concurrent.duration.Deadline): ObjectReader

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]

  def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState]

  def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                   comment: String): Future[KeyValueObjectState]

  def readMetadata(pointer: ObjectPointer, comment: String): Future[MetadataObjectState]

  def newTransaction(): Transaction

  /** A transaction for the repair service's proofs only.
    *
    * Missed-update tracking is off: a proof's no-op commit would otherwise record misses on the very objects being
    * repaired. Entry deletions do not use this: an entry about pool P lives one level closer to the bootstrap pool, so
    * the chain of entries ends and the deletion runs tracked, with `MissedUpdateFinalizationAction.limitTracking`
    * turning tracking off only for the bootstrap pool's own trees.
    */
  private[aspen] def newRepairTransaction(): Transaction =
    val tx = newTransaction()
    tx.disableMissedUpdateTracking()
    tx

  def getAllocator(allocatorId: ObjectAllocatorId): Future[ObjectAllocator]

  /** One StoragePool per pool: concurrent callers share the load in flight, so every caller (the commit hooks
    * included) holds the same instance and allocation strategy. A failed load is forgotten so the next call retries
    */
  def getStoragePool(poolId: PoolId): Future[StoragePool] =
    given ExecutionContext = this.clientContext
    synchronized:
      storagePoolCache.get(poolId) match
        case Some(pool) => pool
        case None =>
          val load = getStoragePoolPointer(poolId).flatMap: pointer =>
            read(pointer).map(kvos => SimpleStoragePool(this, kvos): StoragePool)
          storagePoolCache += poolId -> load
          load.failed.foreach: _ =>
            synchronized:
              if storagePoolCache.get(poolId).contains(load) then
                storagePoolCache -= poolId
          load

  /** Forgets the cached StoragePool, and any load in flight, so the next getStoragePool rereads the pool object */
  private[aspen] def dropCachedStoragePool(poolId: PoolId): Unit = synchronized:
    storagePoolCache -= poolId

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

  /** The pool's state and the timestamp of the transaction that last wrote it */
  def getStoragePoolStateAndTimestamp(poolId: PoolId): Future[(StoragePoolState, HLCTimestamp)] =
    given ExecutionContext = this.clientContext
    getStoragePoolPointer(poolId).flatMap: pointer =>
      read(pointer).map: kvos =>
        (StoragePoolState(kvos), kvos.contents(StoragePoolState.ConfigKey).timestamp)

  /** The host's state and the timestamp of the transaction that last wrote it */
  def getHostStateAndTimestamp(hostId: HostId): Future[(HostState, HLCTimestamp)] =
    given ExecutionContext = this.clientContext
    getHostPointer(hostId).flatMap: pointer =>
      read(pointer).map: kvos =>
        (HostState(kvos), kvos.contents(HostState.StateKey).timestamp)
  
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

  // ---- Generic registries ----
  //
  // Two registries live in the radicle: the object registry maps a UUID to an ObjectPointer,
  // the namespaced registry maps "<namespace>.<name>" to a UUID. A subsystem that owns
  // persistent named state composes the two -- it registers its name in its own namespace,
  // and registers the resulting id against its root object.
  //
  // The prepare* forms stage into a caller-supplied transaction so a registration can be made
  // atomic with the allocation of the thing being registered. A name that is already taken
  // surfaces as KeyAlreadyExists from the prepare* forms and as Registry.DuplicateRegistration
  // from the non-transactional register* forms.
  //
  // The typed getStoragePoolId / listHosts / ... family above is the supported way to reach the
  // namespaces Aspen itself owns; these are the escape hatch for everyone else.

  def getRegisteredObject(objectId: UUID): Future[ObjectPointer]
  def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer]
  def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer]
  def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit]
  def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit]

  def getRegisteredId(namespace: String, name: String): Future[UUID]
  def registerId(namespace: String, name: String, objectId: UUID): Future[Unit]
  def prepareRegisterId(namespace: String, name: String, objectId: UUID)(using tx: Transaction): Future[Unit]
  def listRegisteredIds(namespace: String): Future[List[(String, UUID)]]

  private[aspen] def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer]
  private[aspen] def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer]
  private[aspen] def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer]
  private[aspen] def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer]
  private[aspen] def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer]

  /** @param metadataGroup the metadata group whose pools hold the new pool's ErrorTree and AllocationTree, or
    *                      `None` to keep them in the bootstrap pool
    */
  protected def createStoragePool(config: StoragePoolState, metadataGroup: Option[AllocationGroupId]): Future[PoolId]

  /** Creates a new allocation group.
    *
    * @param maxSliceSize the group's slice limit, fixed for the group's lifetime, or `None` for no limit. Allowed
    *                     only with `ExactMatch` and must be at least `ida.minSliceSize`; anything else throws
    *                     `IllegalArgumentException`. It is a floor: only pools and groups with no limit or one at
    *                     least as large may join, and an unlimited group accepts only unlimited members. The object
    *                     size limit users see is the group's derived `maxObjectSize`.
    * @param idaPolicy   `ExactMatch` (the default) requires every member's IDA to equal `ida`; `AllowAll` accepts
    *                    any member. Fixed for the group's lifetime.
    * @param ida         the IDA members must equal. Required for `ExactMatch` and must be `None` for `AllowAll`;
    *                    anything else throws `IllegalArgumentException`.
    * @param isMetadataGroup true for a metadata group, which accepts only metadata pools and other metadata
    *                        groups. Fixed for the group's lifetime.
    */
  def createAllocationGroup(groupName: String,
                            level: Int,
                            maxSliceSize: Option[Int] = None,
                            idaPolicy: IDAPolicy = IDAPolicy.ExactMatch,
                            ida: Option[IDA] = None,
                            isMetadataGroup: Boolean = false): Future[AllocationGroupId]

  def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId]

  /** Register a new host, in a single atomic transaction: a new HostState object is allocated
   *  and registered in the hosts tree, and `name` is registered in the host namespace.
   *
   *  Fails with KeyAlreadyExists if `name` is already registered, and does not retry: a name
   *  collision is an operator error rather than a transient one.
   *
   *  The host is created with no storage devices. Devices are added afterwards by
   *  createStorageDevice, which is also what makes the host useful for anything. Nothing here
   *  touches the new machine's filesystem -- see server.HostManager.createHost for that.
   *
   *  `hostId` is supplied by the caller so that anything bound to the id beforehand, such as
   *  the host's TLS certificate, can be prepared before this registration commits. */
  def createHost(name: String,
                 address: String,
                 dataPort: Int,
                 httpPort: Int,
                 storeTransferPort: Int,
                 hostId: HostId = HostId(UUID.randomUUID())): Future[HostId]

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

  /** Declare `deviceId` dead and begin reconstructing every store that lived on it.
   *
   *  One transaction enrolls a FailedStorageDeviceDurableTask, which then tombstones the device
   *  -- removing it from its set and its owning host, setting its status to Failed and zeroing both ids -- and moves its stores
   *  one at a time onto live devices of each store's pool's set, marked Rebuilding. Returns as
   *  soon as the enrollment commits; progress is observable via `show-device`.
   *
   *  The tombstone is one-way. A recovered device is re-introduced as a new device with a new id.
   *
   *  For a drive that has physically failed and whose data is unrecoverable. Unlike shutdownStorageDevice there is,
   *  by design, no pool-health check: the device's data is already gone, so refusing could save nothing and would only
   *  delay rebuilding it elsewhere. Its absence is not a flaw (systemic-loss-protection.md S10); do not add one.
   *
   *  Fails with NoSuchElementException if the device does not exist, or DeviceAlreadyFailed if it
   *  is already tombstoned. Two simultaneous calls both enroll, which is harmless: the task's
   *  steps are idempotent and racing drains lose on their KeyRevision requirements and retry. A retry after an
   *  enrollment that committed but was reported TransactionOutcomeUnknown finds its own task and succeeds.
   */
  def failStorageDevice(deviceId: StorageDeviceId): Future[Unit]

  def transact[T](prepare: Transaction => Future[T])(using ec: ExecutionContext): Future[T] =
    val tx = newTransaction()

    val fprep = try prepare(tx) catch
      case err: Throwable => Future.failed(err)

    // Only a preparation failure aborts. Once commit() is called the outcome belongs to the
    // stores and abort() would throw AbortAfterCommit.
    fprep.failed.foreach(err => tx.abort(err))

    for
      prepResult <- fprep
      _ <- tx.commit()
    yield prepResult

  def transactUntilSuccessful[T](prepare: Transaction => Future[T])(using ec: ExecutionContext): Future[T] =
    retryStrategy.retryUntilSuccessful:
      transact(prepare)
    
  def transactUntilSuccessfulWithRecovery[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T])(using ec: ExecutionContext): Future[T] =
    retryStrategy.retryUntilSuccessful(onCommitFailure):
      transact(prepare)
      
  /** Creates a new storage pool.
    *
    * @param maxSliceSize the largest slice, in bytes, any store of the pool holds for one object, or `None` for no
    *                     limit. Fixed for the pool's lifetime. The pool's `maxObjectSize` is derived from it.
    * @param backendConfig the stores' backend. Its own optional `maxSliceSize` combines with the pool's as
    *                      `min(pool, backend)`; that effective limit is what stores enforce and what the pool's
    *                      `maxObjectSize` is derived from.
    * @param metadataGroup the metadata group whose pools hold the new pool's ErrorTree and AllocationTree, or
    *                      `None` for the default one (`getDefaultMetadataGroupId`). Fails with `NotAMetadataGroup`,
    *                      before any transaction, if the group is not a metadata group.
    * @throws IllegalArgumentException (immediately, before any state changes) if `maxSliceSize` or the backend's
    *                                  `maxSliceSize` is smaller than `ida.minSliceSize`, since such a limit admits
    *                                  no objects at all
    */
  def createNewStoragePool(name: String,
                           ida: IDA,
                           maxSliceSize: Option[Int],
                           backendConfig: BackendConfig,
                           storageDeviceSet: StorageDeviceSetId,
                           maximumStoreSize: Long,
                           metadataGroup: Option[AllocationGroupId] = None): Future[PoolId] =
    given ExecutionContext = this.clientContext

    def resolvedGroup: Future[Option[AllocationGroupId]] =
      for
        groupId <- metadataGroup.fold(getDefaultMetadataGroupId())(Future.successful)
        group <- getAllocationGroupState(groupId)
      yield
        if !group.isMetadataGroup then
          throw NotAMetadataGroup(groupId)
        Some(groupId)

    createNewPool(name, ida, maxSliceSize, backendConfig, storageDeviceSet, maximumStoreSize,
      isMetadataPool = false, metadataGroup = resolvedGroup)

  /** Creates a new metadata pool: one that holds the metadata trees of other pools and may join only metadata
    * groups. Its own trees live in the bootstrap pool. Arguments are as for `createNewStoragePool`.
    */
  def createNewMetadataPool(name: String,
                            ida: IDA,
                            maxSliceSize: Option[Int],
                            backendConfig: BackendConfig,
                            storageDeviceSet: StorageDeviceSetId,
                            maximumStoreSize: Long): Future[PoolId] =
    backendConfig match
      case _: FsifConfig =>
        Future.failed(IllegalArgumentException("FSIF is not supported for metadata pools"))
      case _ =>
        createNewPool(name, ida, maxSliceSize, backendConfig, storageDeviceSet, maximumStoreSize,
          isMetadataPool = true, metadataGroup = Future.successful(None))

  private def createNewPool(name: String,
                            ida: IDA,
                            maxSliceSize: Option[Int],
                            backendConfig: BackendConfig,
                            storageDeviceSet: StorageDeviceSetId,
                            maximumStoreSize: Long,
                            isMetadataPool: Boolean,
                            metadataGroup: => Future[Option[AllocationGroupId]]): Future[PoolId] =
    maxSliceSize.foreach: limit =>
      if limit < ida.minSliceSize then
        throw new IllegalArgumentException(
          s"maxSliceSize ($limit) must be at least ${ida.minSliceSize} for $ida")
    backendConfig.maxSliceSize.foreach: limit =>
      if limit < ida.minSliceSize then
        throw new IllegalArgumentException(
          s"backend maxSliceSize ($limit) must be at least ${ida.minSliceSize} for $ida")

    given ExecutionContext = this.clientContext
    val poolId = PoolId(UUID.randomUUID())
    for
      // Resolved after the synchronous checks above, so an invalid limit throws before any read
      group <- metadataGroup
      set <- getStorageDeviceSetState(storageDeviceSet)
      firstIds <- set.selectDevicesForPool(ida.width, this)
      firstDevices <- Future.sequence(firstIds.map(sid => getStorageDeviceState(sid)))
      // A device that is shutting down or shut down is never a legal member. Re-select once with
      // those devices soft-excluded; if selection still has to name one, there is no alternative.
      skipped = firstIds.zip(firstDevices).collect { case (sid, dev) if !dev.isOperational => sid }.toSet
      deviceIds <-
        if skipped.isEmpty then Future.successful(firstIds)
        else set.selectDevicesForPool(ida.width, this, exclude = skipped)
      devices <-
        if skipped.isEmpty then Future.successful(firstDevices)
        else Future.sequence(deviceIds.map(sid => getStorageDeviceState(sid)))
      // A tombstoned device's zeroed storageDeviceId would otherwise flow into
      // StoragePoolState.StoreEntry on the next line and blow up collectDevices' tree lookup
      // before any downstream guard can run. Report the real id, not the zeroed one.
      _ = deviceIds.zip(devices).find((_, dev) => dev.isFailed).foreach((sid, _) => throw DeviceFailed(sid))
      _ = deviceIds.zip(devices).find((_, dev) => !dev.isOperational).foreach((sid, _) => throw DeviceNotOperational(sid))
      stores = devices.map(dev => StoragePoolState.StoreEntry(dev.hostId, dev.storageDeviceId)).toArray
      config = StoragePoolState(
        poolId,
        name,
        ida,
        maxSliceSize,
        stores,
        backendConfig,
        storageDeviceSet,
        0L,
        maximumStoreSize,
        isMetadataPool = isMetadataPool
      )
      _ <- createStoragePool(config, group)
    yield
      poolId
      
  /** Lets the rebuild of `storeId` finish past objects no peer can supply (rebuild-process 5.5). Sets
   *  `acceptLoss` on the store's Rebuilding entry on the device its pool names, and nudges that device's
   *  host. The rebuild's next pass that ends with objects still unreadable records each in the pool's ErrorTree against
   *  the store, so repair
   *  restores it if it ever becomes readable, and finishes. Fails with StoreNotRebuilding unless the entry is
   *  Rebuilding. The flip that promotes the store clears the flag. Only a collision is retried. */
  def acceptRebuildLoss(storeId: StoreId): Future[Unit] =
    given ExecutionContext = this.clientContext

    val fStaged: Future[CheckStorageDevice] = transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      given Transaction = tx

      for
        poolState <- getStoragePoolState(storeId.poolId)
        deviceId = poolState.stores(storeId.poolIndex).storageDeviceId
        ptr <- getStorageDevicePointer(deviceId)
        kvos <- read(ptr)
        state = StorageDeviceState(kvos)
      yield
        state.stores.get(storeId) match
          case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Rebuilding && !state.isFailed =>
            val newState = state.copy(stores = state.stores + (storeId -> entry.copy(acceptLoss = true)))
            tx.update(ptr, None, None,
              List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
              List(Insert(StorageDeviceState.StateKey, newState.encode())))
            CheckStorageDevice(state.hostId, clientId, deviceId)
          case _ => throw new StoreNotRebuilding(storeId)

    fStaged.map(sendBestEffortHostMessage)

  /** Re-places a store being rebuilt onto `destinationId` (rebuild-process 4.2): its rebuild there starts afresh, and
   *  the one on its current device is abandoned. The remedy for a destination that is out of space, co-locates the
   *  pool's stores, or sits on suspect media. One transaction removes the store's Rebuilding entry from its current
   *  device, writes a Rebuilding entry on the destination, and repoints the pool (and, for a bootstrap-pool store, the
   *  radicle's bootstrap config), as FailedStorageDeviceDurableTask.moveStore does. Both hosts are nudged: the old
   *  one's reconcile cancels its rebuild and deletes its staging copy, the new one's starts the rebuild.
   *
   *  Fails with StoreNotRebuilding unless the store's entry on the device its pool names is Rebuilding, and with
   *  InvalidDestination, DeviceFailed or DeviceNotOperational for a destination that cannot take it (the current
   *  device, a failed or non-operational one, or one that already lists the store). Only a collision is retried. */
  def replaceRebuild(storeId: StoreId, destinationId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = this.clientContext

    // After an attempt that committed but was reported TransactionOutcomeUnknown, the retry finds the pool naming the
    // destination and the destination Rebuilding, and succeeds with that attempt's nudges rather than failing with
    // InvalidDestination (paxos-transactions 10.11)
    val retry = StopRetrying.collisionRetry()
    @volatile var staged: List[CheckStorageDevice] = Nil
    // The new Rebuilding episode, one for every attempt, so a retry recognizes its own commit by it
    val episode = UUID.randomUUID()

    val fStaged: Future[List[CheckStorageDevice]] =
      transactUntilSuccessfulWithRecovery(retry.onFailure): tx =>
        given Transaction = tx

        def replace(poolPtr: KeyValueObjectPointer, poolKvos: KeyValueObjectState, poolState: StoragePoolState,
                    sourceId: StorageDeviceId, dstPtr: KeyValueObjectPointer, dstKvos: KeyValueObjectState,
                    dstState: StorageDeviceState): Future[List[CheckStorageDevice]] = for
          srcPtr <- getStorageDevicePointer(sourceId)
          srcKvos <- read(srcPtr)
          srcState = StorageDeviceState(srcKvos)
          _ =
            if !srcState.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Rebuilding) then
              throw new StoreNotRebuilding(storeId)
            if sourceId == destinationId || dstState.stores.contains(storeId) then
              throw InvalidDestination()
            if dstState.isFailed then
              throw DeviceFailed(destinationId)
            if !dstState.isOperational then
              throw DeviceNotOperational(destinationId)
          // Repointed before prepRadicleUpdate reads the pool's store map, as moveStore does
          _ = poolState.stores(storeId.poolIndex) = StoragePoolState.StoreEntry(dstState.hostId, destinationId)
          _ <- BootstrapConfig.prepRadicleUpdate(this, storeId, poolState, dstState.hostId)
        yield
          tx.update(srcPtr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey, srcKvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, srcState.removeStore(storeId).encode())))
          val newDst = dstState.setRebuilding(storeId, episode)
          tx.update(dstPtr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey, dstKvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, newDst.encode())))
          tx.update(poolPtr, None, None,
            List(KeyRevision(StoragePoolState.ConfigKey, poolKvos.contents(StoragePoolState.ConfigKey).revision)),
            List(Insert(StoragePoolState.ConfigKey, poolState.encode())))
          val nudges = List(CheckStorageDevice(dstState.hostId, clientId, destinationId),
                            CheckStorageDevice(srcState.hostId, clientId, sourceId))
          staged = nudges
          nudges

        for
          poolPtr <- getStoragePoolPointer(storeId.poolId)
          poolKvos <- read(poolPtr)
          poolState = StoragePoolState(poolKvos)
          sourceId = poolState.stores(storeId.poolIndex).storageDeviceId
          dstPtr <- getStorageDevicePointer(destinationId)
          dstKvos <- read(dstPtr)
          dstState = StorageDeviceState(dstKvos)
          done = retry.mayHaveCommitted && sourceId == destinationId &&
            dstState.stores.get(storeId).exists(_.rebuildEpisode.contains(episode))
          // Nothing staged when done: the empty transaction succeeds
          nudges <- if done then Future.successful(staged) else replace(poolPtr, poolKvos, poolState, sourceId,
                                                                          dstPtr, dstKvos, dstState)
        yield nudges

    fStaged.map(_.foreach(sendBestEffortHostMessage))

  def transferStore(storeId: StoreId, destinationId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = this.clientContext

    // Only a collision is retried. The rebalance and migration tasks await this future inside a
    // pass and treat a failure as "back off and retry on the next poll".
    //
    // One transfer id for every attempt. After an attempt that committed but was reported
    // TransactionOutcomeUnknown, the retry finds the source TransferringOut under this id, or the
    // pool already naming the destination once the transfer has finished, and succeeds with the
    // same nudge rather than failing with StoreNotActive or InvalidDestination (paxos-transactions 10.11)
    val transferId = UUID.randomUUID()
    val retry = StopRetrying.collisionRetry()
    val fStaged: Future[CheckStorageDevice] = transactUntilSuccessfulWithRecovery(retry.onFailure): tx =>
      given Transaction = tx

      for
        // The pool state must be read fresh: a StoragePool object holds no store-to-device mapping,
        // and a snapshot would show where the stores were when it was taken. The source device must be read fresh -- a store that has already
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
        val alreadyStarted = retry.mayHaveCommitted && (sourceId == destinationId ||
          srcState.stores.get(storeId).exists(_.transferId.contains(transferId)))

        if alreadyStarted then
          // Nothing staged: the empty transaction succeeds, and the destination is nudged again
          CheckStorageDevice(dstState.hostId, clientId, destinationId)
        else
          if sourceId == destinationId then
            throw InvalidDestination()

          // A store on a tombstoned device is silently lost: nothing reconciles it, because
          // reconcileDeviceState ignores failed devices outright.
          if dstState.isFailed then
            throw DeviceFailed(destinationId)

          if !dstState.isOperational then
            throw DeviceNotOperational(destinationId)

          srcState.stores.get(storeId) match
            case None => throw StoreNotActive(storeId)
            case Some(entry) =>
              if entry.status != StorageDeviceState.StoreStatus.Active then
                throw StoreNotActive(storeId)

              // Update Source Device
              val newSrcEntry = StorageDeviceState.StoreEntry(
                StorageDeviceState.StoreStatus.TransferringOut,
                Some(destinationId),
                Some(transferId)
              )
              val newSrcStores = srcState.stores + (storeId -> newSrcEntry)
              val newSrcState = srcState.copy(stores = newSrcStores)

              val srcReqs = List(KeyRevision(StorageDeviceState.StateKey, srcKvos.contents(StorageDeviceState.StateKey).revision))
              val srcOps = List(Insert(StorageDeviceState.StateKey, newSrcState.encode()))

              tx.update(srcPtr, None, None, srcReqs, srcOps)

              // Update Destination Device
              val newDstEntry = StorageDeviceState.StoreEntry(
                StorageDeviceState.StoreStatus.TransferringIn,
                Some(sourceId),
                Some(transferId)
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

  /** Rolls back an outstanding transfer of `storeId`: the source entry goes back to Active with no
   *  transfer device and the destination's TransferringIn entry is removed, in one transaction.
   *
   *  Completes with true when it rolled a transfer back and false when it found nothing to do.
   *
   *  Succeeds and changes nothing when no transfer is outstanding -- the store is Active, or the
   *  transfer already completed and the pool names the destination. When it does change something,
   *  best-effort CheckStorageDevice nudges go to the source and destination hosts once the
   *  transaction has committed, so a host that has begun moving bytes sees the withdrawn entries
   *  promptly rather than on its next device poll. Nothing is sent when nothing changed.
   */
  def abortStoreTransfer(storeId: StoreId): Future[Boolean] =
    given ExecutionContext = this.clientContext

    // As in transferStore: only a collision is retried. An attempt that committed but was reported
    // TransactionOutcomeUnknown leaves nothing outstanding, so the retry stages nothing; it then
    // reports that attempt's rollback and sends its nudges (paxos-transactions 10.11)
    val retry = StopRetrying.collisionRetry()
    @volatile var staged: List[CheckStorageDevice] = Nil
    val fStaged: Future[List[CheckStorageDevice]] = transactUntilSuccessfulWithRecovery(retry.onFailure): tx =>
      given Transaction = tx

      for
        // Fresh pool state, not the cached one, for the same reason as in transferStore. After a
        // completed transfer it names the destination, whose entry is Active: nothing to abort.
        poolState <- getStoragePoolState(storeId.poolId)
        sourceId = poolState.stores(storeId.poolIndex).storageDeviceId
        srcPtr <- getStorageDevicePointer(sourceId)
        srcKvos <- read(srcPtr)
        srcState = StorageDeviceState(srcKvos)
        outstanding = srcState.stores.get(storeId).collect:
          case StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.TransferringOut, Some(dstId), _, _) => dstId
        dstRead <- outstanding match
          case None => Future.successful(None)
          case Some(dstId) =>
            for
              dstPtr <- getStorageDevicePointer(dstId)
              dstKvos <- read(dstPtr)
            yield Some((dstPtr, dstKvos))
      yield
        // Built here and sent after commit, as in transferStore, so a retried attempt cannot
        // double-send. Empty when nothing was changed.
        dstRead.toList.flatMap: (dstPtr, dstKvos) =>
          // Offline rather than Active on a source being shut down: see serviceStatus. Its copy
          // is closed, in outbound/ where its transfer moved it.
          val newSrcState = srcState.setStoreEntry(storeId, srcState.serviceStatus, None)
          tx.update(srcPtr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey, srcKvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, newSrcState.encode())))

          // Only the entry this transfer created. The failed-device drain can leave the destination
          // Rebuilding, or reassigned to another source, while the source still reads
          // TransferringOut; that entry belongs to the drain and is left alone.
          val dstState = StorageDeviceState(dstKvos)
          val ours = dstState.stores.get(storeId).exists: entry =>
            entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
            entry.transferDevice.contains(sourceId)
          if ours then
            tx.update(dstPtr, None, None,
              List(KeyRevision(StorageDeviceState.StateKey, dstKvos.contents(StorageDeviceState.StateKey).revision)),
              List(Insert(StorageDeviceState.StateKey, dstState.removeStore(storeId).encode())))

          // The destination is nudged only when this call changed its entry, and never when it is
          // tombstoned: its hostId is then the zeroed FailedHostId, which no host answers to.
          CheckStorageDevice(srcState.hostId, clientId, sourceId) ::
            (if ours && !dstState.isFailed then
               List(CheckStorageDevice(dstState.hostId, clientId, outstanding.get))
             else
               Nil)
        match
          case Nil if retry.mayHaveCommitted => staged
          case nudges =>
            staged = nudges
            nudges

    fStaged.map: nudges =>
      nudges.foreach(sendBestEffortHostMessage)
      nudges.nonEmpty

  /** Requests a planned shutdown of the device: every Active store becomes Offline and the device
   *  ShuttingDown. The owning host does the rest and moves the device to Shutdown once its stores
   *  are closed. Idempotent: on a device already ShuttingDown or Shutdown nothing is written, but
   *  the nudge is still sent so that a host that missed it can catch up. Fails with `DeviceFailed`
   *  for a tombstoned device, and with `ShutdownWouldBreakPool` when taking an Operational
   *  device's stores offline would leave one of its pools unwritable (see checkPoolsStayWritable).
   */
  def shutdownStorageDevice(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = this.clientContext
    changeDeviceStatus(deviceId): (state, tx) =>
      state.status match
        case StorageDeviceState.DeviceStatus.Operational =>
          checkPoolsStayWritable(state)(using tx).map: _ =>
            Some(state.withActiveStoresOffline.copy(status = StorageDeviceState.DeviceStatus.ShuttingDown))
        case _ => Future.successful(None)

  /** Fails the shutdown of `device` with `ShutdownWouldBreakPool` if any pool holding a store on
   *  it would be left with fewer writable stores than its write threshold. A store is writable
   *  only if it sits on another Operational device whose state lists it as Active: Rebuilding,
   *  Offline, TransferringIn and other non-Active stores cannot take writes. A ShuttingDown,
   *  Shutdown or Failed device counts as unavailable: its stores are offline or about to be. So does
   *  a device a pool names but that is not registered, so that only the target device's own lookup
   *  can fail with NoSuchElementException. A pool named by an entry but no longer registered is
   *  skipped, as there is nothing left to keep writable.
   *
   *  Run inside the shutdown's transaction, so a collision retry re-reads everything. Each other
   *  device counted as Operational has its revision locked by the transaction: two devices of
   *  one pool shut down at once would otherwise each count the other as available, and both
   *  commit. With the locks, whichever commits second sees the first one's revision change,
   *  retries, and is refused.
   */
  private def checkPoolsStayWritable(device: StorageDeviceState)(using tx: Transaction): Future[Unit] =
    given ExecutionContext = this.clientContext
    val deviceId = device.storageDeviceId
    val poolIds = device.stores.keysIterator.map(_.poolId).toList.distinct
    for
      pools <- Future.sequence(poolIds.map: poolId =>
                 getStoragePoolState(poolId).map(Some(_)).recover:
                   case _: NoSuchElementException => None)
      others = pools.flatten.flatMap(_.stores.map(_.storageDeviceId)).distinct.filter(_ != deviceId)
      otherStates <- Future.sequence(others.map: id =>
                       getStorageDevicePointer(id).map(Some(_)).recover:
                         case _: NoSuchElementException => None
                       .flatMap:
                         case Some(ptr) => read(ptr).map(kvos => Some((id, ptr, kvos)))
                         case None => Future.successful(None))
    yield
      val operational = otherStates.flatten.flatMap: (id, ptr, kvos) =>
        val state = StorageDeviceState(kvos)
        if state.isOperational then Some(id -> (ptr, kvos, state)) else None
      .toMap
      def writable(storeId: StoreId, e: StoragePoolState.StoreEntry): Boolean =
        e.storageDeviceId != deviceId && operational.get(e.storageDeviceId).exists: (_, _, state) =>
          state.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active)
      pools.flatten.find: pool =>
        pool.stores.iterator.zipWithIndex.count((e, i) => writable(StoreId(pool.poolId, i.toByte), e)) <
          pool.ida.writeThreshold
      .foreach: pool =>
        throw StopRetrying(ShutdownWouldBreakPool(pool.poolId))
      operational.valuesIterator.foreach: (ptr, kvos, _) =>
        tx.exclusiveLockRevision(ptr, kvos.revision)

  /** Reverses `shutdownStorageDevice`: Offline stores become Active and the device Operational.
   *  Idempotent in the same way: on an Operational device nothing is written but the nudge is
   *  still sent. Fails with `DeviceFailed` for a tombstoned device.
   */
  def restartStorageDevice(deviceId: StorageDeviceId): Future[Unit] =
    changeDeviceStatus(deviceId): (state, _) =>
      state.status match
        case StorageDeviceState.DeviceStatus.Operational => Future.successful(None)
        case _ =>
          Future.successful(Some(state.withOfflineStoresActive.copy(status = StorageDeviceState.DeviceStatus.Operational)))

  private def changeDeviceStatus(deviceId: StorageDeviceId)
                                (change: (StorageDeviceState, Transaction) => Future[Option[StorageDeviceState]]): Future[Unit] =
    given ExecutionContext = this.clientContext

    // As in abortStoreTransfer: only a collision is retried, and the nudge is built inside the
    // transaction but sent after commit so a retried attempt cannot double-send.
    val fNudge: Future[CheckStorageDevice] = transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      given Transaction = tx

      for
        devPtr <- getStorageDevicePointer(deviceId)
        kvos <- read(devPtr)
        state = StorageDeviceState(kvos)
        _ = if state.isFailed then throw StopRetrying(AspenClient.DeviceFailed(deviceId))
        changed <- change(state, tx)
      yield
        changed.foreach: newState =>
          tx.update(devPtr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, newState.encode())))

        CheckStorageDevice(state.hostId, clientId, deviceId)

    fNudge.map(sendBestEffortHostMessage)

  def getBootstrapConfig(): Future[String] =
    given ExecutionContext = this.clientContext

    client.read(radicle).map: radicleKvos =>
      new String(radicleKvos.contents(Radicle.BootstrapConfigKey).value.bytes, StandardCharsets.UTF_8)

  /** The allocation group whose metadata pools hold new pools' metadata trees, as recorded in the Radicle.
    * Fails with `IllegalStateException` if the Radicle has no such entry.
    */
  def getDefaultMetadataGroupId(): Future[AllocationGroupId] =
    given ExecutionContext = this.clientContext

    client.read(radicle).map: radicleKvos =>
      radicleKvos.contents.get(Radicle.DefaultStoragePoolMetadataAllocatorKey) match
        case Some(vs) => AllocationGroupId(byte2uuid(vs.value.bytes))
        case None => throw new IllegalStateException("The Radicle has no default storage pool metadata group")

  /** Signs a certificate binding `id` to `publicKey` using the cluster CA stored in the radicle.
   *
   *  There is no access control: any client able to read the radicle can read the CA private
   *  key and therefore sign certificates for arbitrary UUIDs.
   */
  def createTLSCertificate(id: UUID, publicKey: PublicKey): Future[X509Certificate] =
    given ExecutionContext = this.clientContext

    def readPem(kvos: KeyValueObjectState, key: Key): Future[String] =
      val ptr = DataObjectPointer(kvos.contents(key).value.bytes)
      client.read(ptr).map: dos =>
        new String(dos.data.getByteArray, StandardCharsets.UTF_8)

    client.read(radicle).flatMap: kvos =>
      val fCert = readPem(kvos, Radicle.CACertificateKey)
      val fKey = readPem(kvos, Radicle.CAPrivateKeyKey)
      for
        certPem <- fCert
        keyPem <- fKey
      yield
        CertificateAuthority(Pem.decodeCertificate(certPem), Pem.decodePrivateKey(keyPem))
          .signCertificate(id, publicKey)

  def retryStrategy: RetryStrategy

  /** Completes once host online/offline state has been loaded into memory and the registry of hosts has
   *  been read, after which `isHostOnline` and `offlineHosts` are accurate. Hosts take part in the gossip
   *  protocol and complete at once; a client has to ask a relay host for the state first, so it completes
   *  when that report has been applied, and never if no relay answers. Callers that depend on the answer
   *  wait for this with a timeout.
   */
  def hostLivenessReady: Future[Unit] = Future.unit

  /** True unless `hostId` is known to be offline. Accurate once `hostLivenessReady` has completed. */
  def isHostOnline(hostId: HostId): Boolean = true

  /** Registered hosts currently believed to be offline; used by rebalancing to avoid moving stores onto,
   *  or counting availability from, offline hosts. Empty until `hostLivenessReady` has completed. */
  def offlineHosts(): Set[HostId] = Set()

  private[aspen] val radicle: KeyValueObjectPointer

  private[aspen] def receiveClientResponse(msg: ClientResponse): Unit

  /** The allocations `stores` hold unfinalized, once `needed` of them have answered (`CurrentAllocationsRequest`,
   *  rebuild-process 5.3). Resends until then; fails with `CurrentAllocationsQuery.Abandoned` once `stopWhen` holds */
  private[aspen] def currentAllocations(stores: List[StoreId], needed: Int, stopWhen: () => Boolean)
    : Future[Map[StoreId, List[org.aspen_ddp.aspen.common.network.PendingAllocation]]]

  /** The votes `stores` hold that may still count (`HeldVote`, Frontend.heldVotes), once `needed` of them have
   *  answered, from every reply. Resends until then; fails with `CurrentAllocationsQuery.Abandoned` once `stopWhen`
   *  holds */
  private[aspen] def heldVotes(stores: List[StoreId], needed: Int, stopWhen: () => Boolean)
    : Future[List[org.aspen_ddp.aspen.common.network.HeldVote]]

  /** Completes after `delay`, on the client's background timer */
  private[aspen] def after(delay: scala.concurrent.duration.FiniteDuration): Future[Unit]
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
   *  `taskTypeUUID` must resolve to a DurableTaskFactory in the type registry.
   *
   *  The enrollment is a checked insert of a new key, which is not a pin (see `Pins`). A transaction with no other
   *  pin passes `pinned = true`: the insert then also requires the system-task tree node's revision, which serializes
   *  every enrollment on that node. Transactions that carry a pin of their own (FSIF index releases, pool migration,
   *  allocation group usage updates) leave it false, so they do not contend on the node.
   *
   *  The task id is fresh by default. A caller whose own guards would not stop a retry from enrolling again passes
   *  `taskId`, chosen outside its retry closure: once the registry holds it (an attempt that committed but was
   *  reported TransactionOutcomeUnknown), nothing is staged, so the retry does not enroll a second task
   *  (paxos-transactions 10.11). That costs a registry read, so it is opt-in. */
  def prepareSystemDurableTask(taskTypeUUID: UUID,
                               initialState: Map[Key, Array[Byte]],
                               pinned: Boolean = false,
                               taskId: Option[UUID] = None)
                              (using tx: Transaction): Future[Unit]

  /** Inserts `e` into the system event log's incoming entries, under a fresh random key. Completes when staged in
   *  `tx`; committing is the caller's. A best-effort nudge to the event log service is sent once `tx` commits.
   *
   *  The insert requires its key not to exist, which is no pin: a transaction without a pin of its own must be
   *  marked `tx.replaySafe("event log entries may be duplicated")` (a replay may log the entry twice). To log several
   *  entries in one transaction, use `prepareSystemEventLogEntries` rather than calling this twice.
   */
  def prepareSystemEventLogEntry(e: EventLogEntry)(using tx: Transaction): Future[Unit] =
    SystemEventLog.prepareEntries(this, List(e))

  /** As `prepareSystemEventLogEntry`, for several entries in one batch update */
  def prepareSystemEventLogEntries(es: Seq[EventLogEntry])(using tx: Transaction): Future[Unit] =
    SystemEventLog.prepareEntries(this, es)

  /** Logs `e` to the system event log in a transaction of its own. There is no automatic retry: a failure is
   *  returned to the caller. The entry may be logged more than once (a replayed transaction can insert it again
   *  after the event log service moved it), so readers must tolerate duplicates.
   */
  def logSystemEvent(e: EventLogEntry): Future[Unit] =
    SystemEventLog.logSystemEvent(this, e)

  /** Directs the system event log to allocator `id`, failing when no such allocator exists. New full log objects
   *  and new loggedEvents tree nodes come from `id`; the tails are replaced by new ones from `id` at the event log
   *  service's next compaction. Objects already written stay where they are. An allocator whose object size cap is
   *  smaller than the current one's turns tail records larger than the new cap into `DroppedEvent`s at that
   *  compaction (common/eventlog/DESIGN.md section 8).
   */
  def setSystemLogAllocator(id: ObjectAllocatorId): Future[Unit] =
    SystemEventLog.setAllocator(this, id)

  /** The system event log's entries timestamped in `[from, to]` (`from` defaults to the earliest timestamp; `to`
   *  None: no upper bound), sorted by timestamp, at most `limit` of them, type ids mapped back to type UUIDs. Entries
   *  logged but not yet moved by the event log service are included. No entry logged before the read began is
   *  omitted, but an entry may appear more than once (logged twice, or read before and again after the service
   *  moved it on: from incomingEvents to a tail, or from a tail to a full object). A read racing changes that move records between full objects restarts,
   *  failing with IllegalStateException after `EventLogReader.MaxRestarts` restarts. The whole read, restarts
   *  included, fails with SystemEventLogReadTimeout once `timeout` has passed: its reads wait for undecided
   *  transactions holding locks on the log, which can take as long as a store of such a transaction is unreachable.
   *  It never returns a partial result.
   */
  def readSystemEvents(from: HLCTimestamp = EventLogReader.Earliest,
                       to: Option[HLCTimestamp] = None,
                       limit: Option[Int] = None,
                       timeout: scala.concurrent.duration.FiniteDuration = EventLogReader.DefaultReadTimeout)
      : Future[Vector[EventLogEntry]] =
    new EventLogReader(this).read(from, to, limit, timeout)

  /** Adds an observer to the system event log service: `factoryType`'s EventObserverFactory builds it from `config`.
   *  Fails with IllegalArgumentException when no EventObserverFactory is registered for `factoryType`. Adding an
   *  existing pair is a no-op. Observers are best-effort (see EventObserver).
   */
  def addSystemEventObserver(factoryType: UUID, config: Array[Byte]): Future[Unit] =
    SystemEventLog.addObserver(this, factoryType, config)

  /** Removes an observer; a pair that is not present is a no-op */
  def removeSystemEventObserver(factoryType: UUID, config: Array[Byte]): Future[Unit] =
    SystemEventLog.removeObserver(this, factoryType, config)

  def listSystemEventObservers(): Future[List[ObserverConfig]] =
    SystemEventLog.listObservers(this)

  /** `e` as an Event, by its type's registered EventFactory; None when no EventFactory is registered for it */
  def decodeSystemEvent(e: EventLogEntry): Option[Event] =
    typeRegistry.getType[RegisteredTypeFactory](e.typeUUID).collect:
      case f: EventFactory => f.createEvent(e)

  private[aspen] def getServiceHost(serviceUUID: UUID): Future[Option[HostId]]

  private[aspen] def setSystemAttribute(key: String, value: String): Unit

  private[aspen] def getCachedAllocator(allocatorId: ObjectAllocatorId): Option[ObjectAllocator] = None
  private[aspen] def cacheAllocator(allocator: ObjectAllocator): Unit = ()

