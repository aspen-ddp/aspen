package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, IDAPolicy, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, Pem}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ClientResponse, HostMessage, ServiceMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.util.uuid2byte
import org.aspen_ddp.aspen.server.store.backend.BackendConfig
import scribe.Logging

import java.nio.charset.StandardCharsets
import java.security.PublicKey
import java.security.cert.X509Certificate
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
  /** fail-storage-device was invoked against a device that already carries a tombstone.
   *  Terminal: the tombstone is one-way, and a recovered device is re-introduced under a new id. */
  class DeviceAlreadyFailed(deviceId: StorageDeviceId)
    extends Exception(s"Storage device ${deviceId.uuid} has already been declared failed")

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

  def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState]

  def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                   comment: String): Future[KeyValueObjectState]

  def readMetadata(pointer: ObjectPointer, comment: String): Future[MetadataObjectState]

  def newTransaction(): Transaction

  /** A transaction for the repair service's errorTree deletes.
    *
    * Missed-update tracking must be off: if the delete's own commit misses a store, the
    * MissedUpdateFinalizationAction writes a fresh errorTree entry for the errorTree node
    * itself, which the next sweep repairs, which writes another. A one-shot CLI pass survived
    * that; a service that runs forever does not.
    */
  private[aspen] def newRepairTransaction(): Transaction =
    val tx = newTransaction()
    tx.disableMissedUpdateTracking()
    tx

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

  protected def createStoragePool(config: StoragePoolState): Future[PoolId]

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
    */
  def createAllocationGroup(groupName: String,
                            level: Int,
                            maxSliceSize: Option[Int] = None,
                            idaPolicy: IDAPolicy = IDAPolicy.ExactMatch,
                            ida: Option[IDA] = None): Future[AllocationGroupId]

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
   *  Fails with NoSuchElementException if the device does not exist, or DeviceAlreadyFailed if it
   *  is already tombstoned. Two simultaneous calls both enroll, which is harmless: the task's
   *  steps are idempotent and racing drains lose on their KeyRevision requirements and retry.
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
    * @throws IllegalArgumentException (immediately, before any state changes) if `maxSliceSize` or the backend's
    *                                  `maxSliceSize` is smaller than `ida.minSliceSize`, since such a limit admits
    *                                  no objects at all
    */
  def createNewStoragePool(name: String,
                           ida: IDA,
                           maxSliceSize: Option[Int],
                           backendConfig: BackendConfig,
                           storageDeviceSet: StorageDeviceSetId,
                           maximumStoreSize: Long): Future[PoolId] =
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
        maxSliceSize,
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

    // Only a collision is retried. The rebalance and migration tasks await this future inside a
    // pass and treat a failure as "back off and retry on the next poll".
    val fStaged: Future[CheckStorageDevice] = transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
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
        if sourceId == destinationId then
          throw InvalidDestination()

        // A store on a tombstoned device is silently lost: nothing reconciles it, because
        // reconcileDeviceState ignores failed devices outright.
        if dstState.isFailed then
          throw DeviceFailed(destinationId)

        // Fresh per attempt: only the committed attempt's id is ever recorded.
        val transferId = UUID.randomUUID()

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

    // As in transferStore: only a collision is retried.
    val fStaged: Future[List[CheckStorageDevice]] = transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
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
          case StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.TransferringOut, Some(dstId), _) => dstId
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
          val newSrcState = srcState.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Active, None)
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

    fStaged.map: nudges =>
      nudges.foreach(sendBestEffortHostMessage)
      nudges.nonEmpty

  def getBootstrapConfig(): Future[String] =
    given ExecutionContext = this.clientContext

    client.read(radicle).map: radicleKvos =>
      new String(radicleKvos.contents(Radicle.BootstrapConfigKey).value.bytes, StandardCharsets.UTF_8)

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

