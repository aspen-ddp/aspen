package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.{ReadDriver, ReadManager}
import org.aspen_ddp.aspen.client.internal.transaction.{ClientTransactionDriver, TransactionImpl, TransactionManager}
import org.aspen_ddp.aspen.client.registries.{NamespacedUUIDRegistry, UUIDObjectRegistry}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, SinglePoolNodeAllocator, TieredKeyValueList}
// Explicit: common.objects also defines a ReadError (an Enumeration object). Only the client
// one is a type, so the wildcards below would resolve correctly anyway, but name it outright.
import org.aspen_ddp.aspen.client.ReadError
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.metadata.management.MigratePoolToSetDurableTask
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, uuid2byte}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.compute.{DurableTaskFactory, ServiceEntry}
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.systemtask.{NewSystemTaskAdded, SystemTaskExecutorService, SystemTaskMessage, SystemTaskServiceState}

import java.util.UUID
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future}

private object Namespaces:
  val Pool = "pool"
  val Host = "host"
  val Group = "group"
  val DeviceSet = "device-set"

/** Shared implementation for all AspenClient implementations.
  *
  * Everything that SimpleAspenClient and TestNetwork.TClient have in common lives here.
  * The differences between a full production client and a minimal test client are confined
  * to a small set of "dials":
  *   - constructor parameters that can be built without `this` (caches, driver factories)
  *   - the abstract members below (`opportunisticRebuildManager`, `resolveIda`, `runCreate`)
  */
abstract class BaseAspenClient(
    val executionContext: ExecutionContext,
    private[aspen] val radicle: KeyValueObjectPointer,
    override val clientId: ClientId,
    private[client] val messenger: ClientMessenger,
    override val txStatusCache: TransactionStatusCache,
    private[client] val objectCache: ObjectCache,
    readDriverFactory: ReadDriver.Factory,
    txDriverFactory: ClientTransactionDriver.Factory,
    userTypeFactories: List[RegisteredTypeFactory]) extends AspenClient:

  given ec: ExecutionContext = executionContext

  var attributes: Map[String, String] = Map()

  val typeRegistry: TypeRegistry = TypeRegistry(
    org.aspen_ddp.aspen.common.TypeFactories.factories,
    org.aspen_ddp.aspen.client.TypeFactories.factories,
    org.aspen_ddp.aspen.server.TypeFactories.factories,
    userTypeFactories
  )

  val retryStrategy: RetryStrategy = new ExponentialBackoffRetryStrategy(this)

  lazy val backgroundTaskManager: BackgroundTaskManager = new BackgroundTaskManager(executionContext)

  private val rmgr = new ReadManager(this, readDriverFactory)

  private val objectRegistry = new UUIDObjectRegistry(this, radicle, Radicle.ObjectRegistryKey)
  private val namespacedRegistry = new NamespacedUUIDRegistry(this, radicle, Radicle.NamespacedRegistryKey)

  private val storagePoolsTree = new MetadataTree(this, radicle, Radicle.StoragePoolsTreeKey)
  private val allocationGroupsTree = new MetadataTree(this, radicle, Radicle.AllocationGroupsTreeKey)
  private val hostsTree = new MetadataTree(this, radicle, Radicle.HostsTreeKey)
  private val storageDevicesTree = new MetadataTree(this, radicle, Radicle.StorageDevicesTreeKey)
  private val storageDeviceSetsTree = new MetadataTree(this, radicle, Radicle.StorageDeviceSetsTreeKey)

  // protected so subclass allocator-cache overrides can delegate to it
  protected val allocatorManager = new ObjectAllocatorManager(this)

  private val txManager = new TransactionManager(this, txDriverFactory)

  def clientContext: ExecutionContext = executionContext

  // ---- Extension points (recovery dials) ----

  /** Resolves the IDA used to read the given object. Production resolves it from the pool;
    * the test client returns a fixed IDA to break the bootstrap-pool read cycle. */
  protected def resolveIda(pointer: ObjectPointer): Future[IDA]

  /** Runs a create-style transaction. Production retries with recovery; the test client runs
    * a single attempt so failures surface instead of being retried away. */
  protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T]

  // `opportunisticRebuildManager` remains abstract (declared on the ReadDriverClient trait) and
  // is overridden by each subclass.

  // ---- Reads ----

  override def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    resolveIda(pointer).flatMap { ida =>
      rmgr.read(pointer, ida, comment).map(_.asInstanceOf[DataObjectState])
    }

  override def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    resolveIda(pointer).flatMap { ida =>
      rmgr.read(pointer, ida, comment).map(_.asInstanceOf[KeyValueObjectState])
    }

  // ---- Transactions ----

  override def newTransaction(): Transaction =
    TransactionImpl(this, txManager, _ => 0, None)

  override def getAllocator(allocatorId: ObjectAllocatorId): Future[ObjectAllocator] =
    allocatorManager.getAllocator(allocatorId)

  // ---- Id lookups ----

  override def getStoragePoolId(poolName: String): Future[PoolId] =
    namespacedRegistry.getRegisteredObject(Namespaces.Pool, poolName).map(PoolId(_))

  override def getHostId(hostName: String): Future[HostId] =
    namespacedRegistry.getRegisteredObject(Namespaces.Host, hostName).map(HostId(_))

  override def getAllocationGroupId(groupName: String): Future[AllocationGroupId] =
    namespacedRegistry.getRegisteredObject(Namespaces.Group, groupName).map(AllocationGroupId(_))

  override def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId] =
    namespacedRegistry.getRegisteredObject(Namespaces.DeviceSet, setName).map(StorageDeviceSetId(_))

  override def listStoragePools(): Future[List[(String, PoolId)]] =
    namespacedRegistry.getAllEntries(Namespaces.Pool).map(_.map((n, u) => n -> PoolId(u)))

  override def listHosts(): Future[List[(String, HostId)]] =
    namespacedRegistry.getAllEntries(Namespaces.Host).map(_.map((n, u) => n -> HostId(u)))

  override def listAllocationGroups(): Future[List[(String, AllocationGroupId)]] =
    namespacedRegistry.getAllEntries(Namespaces.Group).map(_.map((n, u) => n -> AllocationGroupId(u)))

  override def listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]] =
    namespacedRegistry.getAllEntries(Namespaces.DeviceSet).map(_.map((n, u) => n -> StorageDeviceSetId(u)))

  // ---- Pointer lookups ----

  override def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] =
    storagePoolsTree.get(poolId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer] =
    hostsTree.get(hostId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] =
    storageDevicesTree.get(storageDeviceId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer] =
    allocationGroupsTree.get(allocationGroupId.uuid).map(_.asInstanceOf[DataObjectPointer])

  override def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer] =
    storageDeviceSetsTree.get(storageDeviceSetId.uuid).map(_.asInstanceOf[DataObjectPointer])

  // ---- Creates (all routed through the runCreate seam) ----

  override def createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId] =
    val ags = AllocationGroupState(
      AllocationGroupId(UUID.randomUUID()),
      level,
      groupName,
      Nil,
      Nil
    )

    // KeyAlreadyExists, not Registry.DuplicateRegistration: prepareRegisterObject reaches the
    // name registration through Registry.prepareRegister, which reports a taken name that way.
    // DuplicateRegistration comes only from Registry.register, the non-transactional path.
    def onFail(err: Throwable): Future[Unit] = err match
      case e: KeyAlreadyExists => throw StopRetrying(e)
      case _ => Future.unit

    runCreate(onFail): tx =>
      given Transaction = tx
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr <- bsPool.allocator.allocateDataObject(DataBuffer(ags.toBytes))
        _ <- allocationGroupsTree.preparePut(ags.groupId.uuid, ptr)
        _ <- namespacedRegistry.prepareRegisterObject(Namespaces.Group, ags.name, ags.groupId.uuid)
      yield
        ags.groupId

  override def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId] =
    val sds = StorageDeviceSetState(
      StorageDeviceSetId(UUID.randomUUID()),
      name,
      level,
      parent,
      Nil,
      Nil,
      Nil
    )

    // When a parent is supplied, add the new set to the parent's memberSets as part of
    // the same transaction so the parent/child link is established atomically.
    def addToParent(parentId: StorageDeviceSetId)(using tx: Transaction): Future[Unit] =
      for
        parentPtr <- getStorageDeviceSetPointer(parentId)
        parentDos <- read(parentPtr)
      yield
        val parentState = StorageDeviceSetState(parentDos)
        if sds.level >= parentState.level then
          throw AspenClient.InvalidDeviceSetLevel(sds.level, parentState.level)
        val updated = parentState.copy(memberSets = sds.setId :: parentState.memberSets)
        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(updated.toBytes))

    // KeyAlreadyExists, not Registry.DuplicateRegistration: prepareRegisterObject reaches the
    // name registration through Registry.prepareRegister, which reports a taken name that way.
    // DuplicateRegistration comes only from Registry.register, the non-transactional path.
    def onFail(err: Throwable): Future[Unit] = err match
      case e: KeyAlreadyExists => throw StopRetrying(e)
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: AspenClient.InvalidDeviceSetLevel => throw StopRetrying(e)
      case _ => Future.unit

    runCreate(onFail): tx =>
      given Transaction = tx
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr <- bsPool.allocator.allocateDataObject(DataBuffer(sds.toBytes))
        _ <- storageDeviceSetsTree.preparePut(sds.setId.uuid, ptr)
        _ <- namespacedRegistry.prepareRegisterObject(Namespaces.DeviceSet, sds.name, sds.setId.uuid)
        _ <- parent match
               case None => Future.unit
               case Some(parentId) => addToParent(parentId)
      yield
        sds.setId

  override def createHost(name: String,
                          address: String,
                          dataPort: Int,
                          cncPort: Int,
                          storeTransferPort: Int): Future[HostId] =
    // Minted outside runCreate for the same reason createStorageDevice mints its device id
    // there: a retried attempt must reuse the id rather than leak a fresh UUID per attempt.
    val hostId = HostId(UUID.randomUUID())
    val state = HostState(hostId, name, address, dataPort, cncPort, storeTransferPort, Set())

    // KeyAlreadyExists, not Registry.DuplicateRegistration: prepareRegisterObject reaches the
    // name registration through Registry.prepareRegister, which reports a taken name that way.
    // DuplicateRegistration comes only from Registry.register, the non-transactional path.
    def onFail(err: Throwable): Future[Unit] = err match
      case e: KeyAlreadyExists => throw StopRetrying(e)
      case _ => Future.unit

    // No TaskExecutorRootKey is written. StoreManager inserts it on first start under a
    // DoesNotExist requirement, so seeding it here would only duplicate that.
    runCreate(onFail): tx =>
      given Transaction = tx
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr    <- bsPool.allocator.allocateKeyValueObject(
                    Map(HostState.StateKey -> Value(state.encode())))
        _      <- hostsTree.preparePut(hostId.uuid, ptr)
        _      <- namespacedRegistry.prepareRegisterObject(Namespaces.Host, name, hostId.uuid)
      yield
        hostId

  override def createStorageDevice(hostId: HostId,
                                   deviceSetId: StorageDeviceSetId): Future[StorageDeviceId] =
    // The device id is generated once, outside runCreate, so a retried attempt reuses the
    // same id rather than leaking a fresh UUID per attempt. (The allocated object itself is
    // not reused -- each attempt mints a new ObjectId.) createStorageDeviceSet does the same
    // with setId.
    val deviceId = StorageDeviceId(UUID.randomUUID())
    val state = StorageDeviceState(deviceId, hostId, 0L, 0L, Map(), deviceSetId)

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: StorageDeviceSetState.NotLevelZero => throw StopRetrying(e)
      case _ => Future.unit

    runCreate(onFail): tx =>
      given Transaction = tx
      for
        bsPool   <- getStoragePool(PoolId.BootstrapPoolId)
        devPtr   <- bsPool.allocator.allocateKeyValueObject(
                      Map(StorageDeviceState.StateKey -> Value(state.encode())))
        _        <- storageDevicesTree.preparePut(deviceId.uuid, devPtr)

        hostPtr  <- getHostPointer(hostId)
        hostKvos <- read(hostPtr)
        setPtr   <- getStorageDeviceSetPointer(deviceSetId)
        setDos   <- read(setPtr)
      yield
        // Updates are already staged by the time this runs: allocateKeyValueObject issued
        // the new object's update and setRefcount, and preparePut staged its tree insert.
        // Throwing below therefore rejects a partially staged transaction, which is safe
        // because the throw fails this future before the yield completes and transact
        // invalidates the transaction on failure, discarding everything staged.
        val setState = StorageDeviceSetState(setDos)

        if setState.level != 0 then
          throw new StorageDeviceSetState.NotLevelZero(deviceSetId)

        val hostState = HostState(hostKvos)
        tx.update(hostPtr, None, None,
          List(KeyRevision(HostState.StateKey,
                           hostKvos.contents(HostState.StateKey).revision)),
          List(Insert(HostState.StateKey,
                      hostState.addStorageDevice(deviceId).encode())))

        // Dedup-guarded so a retried attempt cannot list the device twice. The host side is
        // naturally safe because HostState.storageDevices is a Set.
        tx.overwrite(setPtr, setDos.revision,
          DataBuffer(setState.copy(
            memberDevices = deviceId :: setState.memberDevices.filter(_ != deviceId)).toBytes))

        deviceId

  override protected def createStoragePool(config: StoragePoolState): Future[PoolId] =
    // A taken pool name is permanent, so it must stop the retry loop rather than be treated as
    // a transient commit failure. KeyAlreadyExists, not Registry.DuplicateRegistration:
    // prepareRegisterObject reaches the name registration through Registry.prepareRegister,
    // which reports a taken name that way. DuplicateRegistration comes only from
    // Registry.register, the non-transactional path. Everything else retries, which for the
    // production client matches the previous transactUntilSuccessful (retry, no recovery); for
    // the test client runCreate performs a single attempt regardless.
    def onFail(err: Throwable): Future[Unit] = err match
      case e: KeyAlreadyExists => throw StopRetrying(e)
      case _ => Future.unit

    val fStaged = runCreate(onFail): tx =>
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

      def stageDeviceUpdate(du: DeviceUpdate): CheckStorageDevice =
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

        // Built here, sent by the map below once the transaction has committed. Registering
        // this on tx.result instead would send it from a task the returned future does not
        // wait on: tx.commit() hands back the same promise as tx.result, so a caller that
        // exits the moment this future completes could lose the message.
        CheckStorageDevice(
          du.state.hostId,
          clientId,
          du.storageDeviceId
        )

      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        poolPtr <- createPoolObj(bsPool.allocator)
        _ <- storagePoolsTree.preparePut(config.poolId.uuid, poolPtr)
        _ <- namespacedRegistry.prepareRegisterObject(Namespaces.Pool, config.name, config.poolId.uuid)
        devUpdates <- Future.sequence(collectDevices(config.stores))
        setPtr <- getStorageDeviceSetPointer(config.storageDeviceSet)
        setDos <- read(setPtr)
      yield
        val nudges = devUpdates.map(stageDeviceUpdate)

        // Record the pool in the device set's assignedPools (reverse of the pool's
        // storageDeviceSet reference). Idempotent so transaction retries are safe.
        val setState = StorageDeviceSetState(setDos)
        if !setState.assignedPools.contains(config.poolId) then
          val updatedSet = setState.copy(assignedPools = config.poolId :: setState.assignedPools)
          tx.overwrite(setPtr, setDos.revision, DataBuffer(updatedSet.toBytes))

        // Each retry attempt builds its own list and only the successful attempt's value
        // reaches the map below, so a retried transaction cannot double-send.
        nudges

    fStaged.map: nudges =>
      nudges.foreach(sendBestEffortHostMessage)
      config.poolId

  // ---- Misc plumbing ----

  override def shutdown(): Unit = backgroundTaskManager.shutdown(Duration(50, MILLISECONDS))

  def receiveClientResponse(msg: ClientResponse): Unit = msg match
    case m: ReadResponse => rmgr.receive(m)
    case m: TransactionCompletionResponse => rmgr.receive(m)
    case m: TransactionResolved => txManager.receive(m)
    case m: TransactionFinalized => txManager.receive(m)

  private[aspen] def sendHostMessage(msg: HostMessage): Unit =
    messenger.sendHostMessage(msg)

  private lazy val servicesTkvl =
    TieredKeyValueList(this, KVObjectRootManager(this, Radicle.ServicesTreeKey, radicle))

  override def getServiceHost(serviceUUID: UUID): Future[Option[HostId]] =
    servicesTkvl.get(Key(serviceUUID)).map:
      case None => None
      case Some(vs) =>
        val entry = ServiceEntry.decode(vs.value.bytes)
        if entry.isClaimed then Some(HostId(entry.hostId)) else None

  override def createSystemDurableTask(taskTypeUUID: UUID,
                                       initialState: Map[Key, Array[Byte]]): Future[Unit] =
    given ExecutionContext = clientContext
    transactUntilSuccessful: tx =>
      given Transaction = tx
      prepareSystemDurableTask(taskTypeUUID, initialState)

  /** Signals that the pool's set pointer already equals the target. Thrown from inside the
   *  transaction so nothing is staged: Aspen has no empty transactions. Recovered to unit. */
  private class SameSetNoOp extends Throwable("pool is already assigned to the target set")

  override def migratePoolToSet(poolId: PoolId,
                                targetSetId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = clientContext

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: SameSetNoOp => throw StopRetrying(e)

    val migrated = transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx

      for
        poolPtr <- getStoragePoolPointer(poolId)
        poolKvos <- read(poolPtr, "migrate pool to set")
        vs = poolKvos.contents(StoragePoolState.ConfigKey)
        poolState = StoragePoolState(vs.value.bytes)
        // `previousSet` is the pool's original set on a first call and the now-abandoned
        // target on a retarget. One write path covers both.
        previousSetId = poolState.storageDeviceSet
        _ <-
          if previousSetId == targetSetId then
            throw new SameSetNoOp
          else
            for
              targetPtr <- getStorageDeviceSetPointer(targetSetId)
              targetDos <- read(targetPtr, "migrate pool to set")
              previousPtr <- getStorageDeviceSetPointer(previousSetId)
              previousDos <- read(previousPtr, "migrate pool to set")
              _ <-
                val poolStoreIds =
                  poolState.stores.indices.map(i => StoreId(poolId, i.toByte)).toSet

                // Pool config: flip the pointer and record the migration. The pointer flips at
                // the start so a store lost mid-migration is rebuilt into the target set
                // directly (selectDeviceForRebuild uses the pool's set) rather than into the
                // old set and then migrated again.
                val updatedPool = poolState.copy(
                  storageDeviceSet = targetSetId,
                  migration = Some(StoragePoolState.Migration(
                    targetSetId, StoragePoolState.MigrationStatus.InProgress)))
                val poolReqs = List(KeyRevision(StoragePoolState.ConfigKey, vs.revision))
                val poolOps = List(Insert(StoragePoolState.ConfigKey, updatedPool.encode()))
                tx.update(poolPtr, None, None, poolReqs, poolOps)

                // Target set: gains the pool (dedup-guarded); defensively strip any pending
                // transfers for it.
                val target = StorageDeviceSetState(targetDos)
                val newTarget = target.copy(
                  assignedPools = poolId :: target.assignedPools.filter(_ != poolId),
                  pendingTransfers = target.pendingTransfers.filterNot:
                    (sid, _, _) => poolStoreIds.contains(sid))
                tx.overwrite(targetPtr, targetDos.revision, DataBuffer(newTarget.toBytes))

                // Previous set: loses the pool (self-healing if absent) and its pending
                // transfers, which can never become safe once the pool leaves planning state.
                val previous = StorageDeviceSetState(previousDos)
                val newPrevious = previous.copy(
                  assignedPools = previous.assignedPools.filter(_ != poolId),
                  pendingTransfers = previous.pendingTransfers.filterNot:
                    (sid, _, _) => poolStoreIds.contains(sid))
                tx.overwrite(previousPtr, previousDos.revision, DataBuffer(newPrevious.toBytes))

                // Enroll only on a first call. A retarget reuses the running task, which holds
                // only the pool id and re-reads its target every pass.
                val alreadyRunning = poolState.migration.exists(
                  _.status == StoragePoolState.MigrationStatus.InProgress)
                if alreadyRunning then
                  Future.unit
                else
                  MigratePoolToSetDurableTask.prepareSystemTask(this, poolId)
            yield ()
      yield ()

    migrated.recover:
      case _: SameSetNoOp => ()

  override def prepareSystemDurableTask(taskTypeUUID: UUID,
                                        initialState: Map[Key, Array[Byte]])
                                       (using tx: Transaction): Future[Unit] =
    given ExecutionContext = clientContext

    // Sanity check: the type must resolve to a DurableTaskFactory.
    typeRegistry.getType[DurableTaskFactory](taskTypeUUID) match
      case None =>
        Future.failed(StopRetrying(
          new IllegalArgumentException(s"prepareSystemDurableTask: no DurableTaskFactory registered for $taskTypeUUID")))
      case Some(_) =>
        val taskId = UUID.randomUUID()

        def serviceStatePtr(): Future[KeyValueObjectPointer] =
          servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map:
            case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
            case None => throw StopRetrying(
              new IllegalStateException("SystemTaskExecutorService is not registered"))

        val taskContent: Map[Key, Value] =
          (initialState + (SimpleTaskExecutor.TaskTypeKey -> uuid2byte(taskTypeUUID)))
            .map((k, v) => k -> Value(v))

        for
          statePtr <- serviceStatePtr()
          pool <- getStoragePool(Radicle.poolId)
          allocator = new PoolObjectAllocator(this, pool)
          taskStatePtr <- allocator.allocateKeyValueObject(taskContent)
          _ <- SystemTaskServiceState.enrollInTx(this, statePtr, taskId, taskStatePtr)
        yield
          tx.result.foreach: _ =>
            sendServiceMessage(SystemTaskExecutorService.ServiceUUID,
              SystemTaskMessage.encode(NewSystemTaskAdded(taskId)))

  def getSystemAttribute(key: String): Option[String] = attributes.get(key)
  def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value
