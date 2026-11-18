package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.allocation.{FsifIndexAllocationStrategy, PoolObjectAllocator}
import org.aspen_ddp.aspen.server.store.backend.FsifConfig
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.{ReadDriver, ReadManager}
import org.aspen_ddp.aspen.client.internal.transaction.{ClientTransactionDriver, DesignatedLeader, TransactionImpl, TransactionManager}
import org.aspen_ddp.aspen.client.registries.{NamespacedUUIDRegistry, UUIDObjectRegistry}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, NodeAllocator, Root, SinglePoolNodeAllocator, TieredKeyValueList, TreeKind}
// Explicit: common.objects also defines a ReadError (an Enumeration object). Only the client
// one is a type, so the wildcards below would resolve correctly anyway, but name it outright.
import org.aspen_ddp.aspen.client.ReadError
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.{AppendableIDA, IDA}
import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.metadata.management.{FailedStorageDeviceDurableTask, MigratePoolToSetDurableTask}
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
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, NANOSECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

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
  *   - constructor parameters that can be built without `this` (driver factories)
  *   - the abstract members below (`resolveIda`, `runCreate`)
  */
abstract class BaseAspenClient(
    val executionContext: ExecutionContext,
    private[aspen] val radicle: KeyValueObjectPointer,
    override val clientId: ClientId,
    private[client] val messenger: ClientMessenger,
    override val txStatusCache: TransactionStatusCache,
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

  // private[client] so tests can run a raw TransactionBuilder, bypassing TransactionImpl's client-side checks
  private[client] val txManager = new TransactionManager(this, txDriverFactory)

  def clientContext: ExecutionContext = executionContext

  // ---- Extension points (recovery dials) ----

  /** Resolves the IDA used to read the given object. Production resolves it from the pool;
    * the test client returns a fixed IDA to break the bootstrap-pool read cycle. */
  protected def resolveIda(pointer: ObjectPointer): Future[IDA]

  /** Runs a create-style transaction. Production retries with recovery; the test client runs
    * a single attempt so failures surface instead of being retried away. */
  protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T]

  // ---- Reads ----
  //
  // Each read method's body takes the timeout ReadManager.read applies. The ObjectReader overrides pass Duration.Inf
  // (untimed); TimedObjectReader, from withReadTimeout, passes its own

  override def withReadTimeout(timeout: Duration): ObjectReader = timeout match
    case Duration.Inf => this
    case fd: FiniteDuration if fd > Duration.Zero => new TimedObjectReader(this, fd)
    case _ => throw new IllegalArgumentException(s"A read timeout must be positive or Duration.Inf, not $timeout")

  override def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    read(pointer, comment, Duration.Inf)

  private[client] def read(pointer: DataObjectPointer, comment: String, timeout: Duration): Future[DataObjectState] =
    resolveIdaWithin(pointer, timeout).flatMap { (ida, remaining) =>
      rmgr.read(pointer, ida, comment, timeout = remaining).map(_.asInstanceOf[DataObjectState])
    }

  override def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    read(pointer, comment, Duration.Inf)

  private[client] def read(pointer: KeyValueObjectPointer, comment: String,
                           timeout: Duration): Future[KeyValueObjectState] =
    resolveIdaWithin(pointer, timeout).flatMap { (ida, remaining) =>
      rmgr.read(pointer, ida, comment, timeout = remaining).map(_.asInstanceOf[KeyValueObjectState])
    }

  override def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState] =
    readKeys(pointer, keys, comment, Duration.Inf)

  private[client] def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String,
                               timeout: Duration): Future[KeyValueObjectState] =
    resolveIdaWithin(pointer, timeout).flatMap { (ida, remaining) =>
      rmgr.read(pointer, ida, comment, readType = KeySet(keys), timeout = remaining).map(_.asInstanceOf[KeyValueObjectState])
    }

  override def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                            comment: String): Future[KeyValueObjectState] =
    readKeyRange(pointer, startKey, endKey, ordering, comment, Duration.Inf)

  private[client] def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                                   comment: String, timeout: Duration): Future[KeyValueObjectState] =
    resolveIdaWithin(pointer, timeout).flatMap { (ida, remaining) =>
      rmgr.read(pointer, ida, comment, readType = KeyRange(startKey, endKey, ordering), timeout = remaining)
        .map(_.asInstanceOf[KeyValueObjectState])
    }

  override def readMetadata(pointer: ObjectPointer, comment: String): Future[MetadataObjectState] =
    readMetadata(pointer, comment, Duration.Inf)

  private[client] def readMetadata(pointer: ObjectPointer, comment: String,
                                   timeout: Duration): Future[MetadataObjectState] =
    resolveIdaWithin(pointer, timeout).flatMap { (ida, remaining) =>
      rmgr.read(pointer, ida, comment, readType = MetadataOnly(), timeout = remaining).map(_.asInstanceOf[MetadataObjectState])
    }

  /** Resolves the IDA within `timeout`, as resolveIdaWithin, and requires range read support */
  private def resolveAppendableIda(pointer: DataObjectPointer,
                                   timeout: Duration): Future[(IDA & AppendableIDA, Duration)] =
    resolveIdaWithin(pointer, timeout).flatMap:
      case (ida: (IDA & AppendableIDA), remaining) => Future.successful((ida, remaining))
      case (ida, _) => Future.failed(new RangeReadNotSupported(pointer, ida))

  /** The pointer's IDA, and what is left of `timeout` for the object read: a timed read has one deadline for both.
    * Only the caller's wait is bounded. The pool load behind resolveIda is shared by every caller of
    * getStoragePool, untimed ones included, so it is never failed by a timeout: it carries on and fills the cache
    * once the stores answer. A resolved (cached) IDA and an untimed read take no timer
    */
  private def resolveIdaWithin(pointer: ObjectPointer, timeout: Duration): Future[(IDA, Duration)] =
    val resolving = resolveIda(pointer)
    timeout match
      case fd: FiniteDuration if !resolving.isCompleted =>
        val started = System.nanoTime()
        val p = Promise[(IDA, Duration)]()
        val timer = backgroundTaskManager.schedule(fd)(p.tryFailure(new ReadTimeout(pointer)))
        resolving.onComplete: result =>
          timer.cancel()
          val remaining = fd - FiniteDuration(System.nanoTime() - started, NANOSECONDS)
          if remaining <= Duration.Zero then
            p.tryFailure(new ReadTimeout(pointer))
          else
            p.tryComplete(result.map(ida => (ida, remaining)))
        p.future
      case _ => resolving.map(ida => (ida, timeout))

  /** Reads several ranges plus a tail with one ByteRanges request, so every piece comes from the same revision.
    * `ranges` are (offset, length) pairs in logical coordinates and `tail` gives the number of slice tail bytes
    * for the resolved IDA
    */
  private def readByteRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)], tail: (IDA & AppendableIDA) => Int,
                             comment: String, timeout: Duration): Future[DataObjectRangesState] =
    // Resolved first so that an empty list still fails for an IDA without range reads
    resolveAppendableIda(pointer, timeout).flatMap { (ida, remaining) =>
      if ranges.exists((offset, length) => offset < 0 || length < 0) then
        Future.failed(new IllegalArgumentException(s"Invalid ranges: $ranges"))
      else
        val sliceRanges = ranges.map: (offset, length) =>
          val (start, end) = ida.sliceRangeFor(offset, length)
          (start, end - start)
        rmgr.read(pointer, ida, comment, readType = ByteRanges(sliceRanges, tail(ida)), timeout = remaining)
          .map(_.asInstanceOf[DataObjectRangesState])
    }

  override def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int],
                         comment: String): Future[DataObjectRangeState] =
    readRange(pointer, offset, length, comment, Duration.Inf)

  private[client] def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int], comment: String,
                                timeout: Duration): Future[DataObjectRangeState] =
    if offset < 0 || length.exists(_ < 0) then
      Future.failed(new IllegalArgumentException(s"Invalid range: offset $offset length $length"))
    else
      resolveAppendableIda(pointer, timeout).flatMap { (ida, remaining) =>
        // An open ended range is requested directly in slice coordinates so that no end can overflow
        val readType = length match
          case Some(n) =>
            val (start, end) = ida.sliceRangeFor(offset, n)
            ByteRanges(List((start, end - start)), 0)
          case None => ByteRanges(List((ida.sliceOffsetFor(offset), Int.MaxValue)), 0)
        rmgr.read(pointer, ida, comment, readType = readType, timeout = remaining).map: os =>
          trimRange(os.asInstanceOf[DataObjectRangesState].ranges.head, offset, length)
      }

  /** Restored windows are row-aligned and may run past the request, so they are trimmed to the request */
  private def trimRange(state: DataObjectRangeState, offset: Int, length: Option[Int]): DataObjectRangeState =
    val bytes = state.data.getByteArray
    val from = math.min(offset - state.offset, bytes.length)
    val requestedEnd = length.fold(Long.MaxValue)(offset.toLong + _)
    val limit = math.min(requestedEnd, state.objectLength.fold(Long.MaxValue)(_.toLong))
    val end = math.min(limit - state.offset, bytes.length.toLong).toInt
    val trimmed = if end <= from then Array.emptyByteArray else bytes.slice(from, end)
    new DataObjectRangeState(state.pointer, state.revision, state.refcount, state.timestamp, state.readTimestamp,
      state.ida, offset, DataBuffer(trimmed), state.objectLength)

  /** The pieces of a multi-range read, each trimmed to its requested (offset, length) */
  private def trimmedPieces(state: DataObjectRangesState, ranges: List[(Int, Int)]): List[DataObjectRangeState] =
    ranges.zip(state.ranges).map((range, piece) => trimRange(piece, range._1, Some(range._2)))

  override def readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)],
                          comment: String): Future[List[DataObjectRangeState]] =
    readRanges(pointer, ranges, comment, Duration.Inf)

  private[client] def readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)], comment: String,
                                 timeout: Duration): Future[List[DataObjectRangeState]] =
    readByteRanges(pointer, ranges, _ => 0, comment, timeout).map(trimmedPieces(_, ranges))

  override def readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite],
                                    comment: String): Future[RangedWriteState] =
    readRangedWriteState(pointer, writes, comment, Duration.Inf)

  private[client] def readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite], comment: String,
                                           timeout: Duration): Future[RangedWriteState] =
    // The length probe and one range per write are a single request, so they share a revision
    val ranges = writes.map(w => (w.offset, w.data.length))
    readByteRanges(pointer, ranges, _.lengthProbeBytes, comment, timeout).flatMap: state =>
      state.appendPosition match
        case Some(position) =>
          Future.successful(RangedWriteFetch.combine(pointer, writes, position, trimmedPieces(state, ranges)))
        case None => Future.failed(new CorruptedObject(pointer))

  override def readAppendPosition(pointer: DataObjectPointer, comment: String): Future[AppendPosition] =
    readAppendPosition(pointer, comment, Duration.Inf)

  private[client] def readAppendPosition(pointer: DataObjectPointer, comment: String,
                                         timeout: Duration): Future[AppendPosition] =
    readByteRanges(pointer, Nil, _.lengthProbeBytes, comment, timeout).flatMap: state =>
      state.appendPosition match
        case Some(position) => Future.successful(position)
        case None => Future.failed(new CorruptedObject(pointer))

  // ---- Transactions ----

  override def newTransaction(): Transaction =
    TransactionImpl(this, txManager, DesignatedLeader.choose(messenger, _, _), None)

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

  // ---- Host liveness ----

  // Registered host ids, so that "offline" can be answered for hosts the network layer has not
  // heard of at all. Loaded before hostLivenessReady completes and refreshed in the background.
  @volatile private var knownHostIds: Set[HostId] = Set.empty
  @volatile private var knownHostsStamp: Long = System.nanoTime()
  private val refreshingKnownHosts = new java.util.concurrent.atomic.AtomicBoolean(false)
  private val KnownHostsRefreshNanos = 60L * 1000 * 1000 * 1000

  private def refreshKnownHosts(): Future[Unit] =
    listHosts().map: hosts =>
      knownHostIds = hosts.map(_._2).toSet
      knownHostsStamp = System.nanoTime()

  override lazy val hostLivenessReady: Future[Unit] =
    messenger.hostLivenessReady.flatMap(_ => refreshKnownHosts())

  override def isHostOnline(hostId: HostId): Boolean = messenger.isHostOnline(hostId)

  override def offlineHosts(): Set[HostId] =
    if !hostLivenessReady.isCompleted then
      Set.empty
    else
      if System.nanoTime() - knownHostsStamp > KnownHostsRefreshNanos &&
         refreshingKnownHosts.compareAndSet(false, true) then
        refreshKnownHosts().onComplete(_ => refreshingKnownHosts.set(false))
      knownHostIds.filterNot(messenger.isHostOnline)

  override def listHosts(): Future[List[(String, HostId)]] =
    namespacedRegistry.getAllEntries(Namespaces.Host).map(_.map((n, u) => n -> HostId(u)))

  override def listAllocationGroups(): Future[List[(String, AllocationGroupId)]] =
    namespacedRegistry.getAllEntries(Namespaces.Group).map(_.map((n, u) => n -> AllocationGroupId(u)))

  override def listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]] =
    namespacedRegistry.getAllEntries(Namespaces.DeviceSet).map(_.map((n, u) => n -> StorageDeviceSetId(u)))

  // ---- Generic registries ----
  //
  // Straight delegation. The typed lookups above deliberately keep going to the registries
  // directly rather than routing through these: they are the curated API over the namespaces
  // Aspen owns, and a subclass override of getRegisteredId should not be able to retarget
  // every pool/host/group/device-set lookup in the system.

  override def getRegisteredObject(objectId: UUID): Future[ObjectPointer] =
    objectRegistry.getRegisteredObject(objectId)

  override def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer] =
    objectRegistry.getRegisteredKeyValueObject(objectId)

  override def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer] =
    objectRegistry.getRegisteredDataObject(objectId)

  override def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit] =
    objectRegistry.registerObject(objectId, pointer)

  override def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    objectRegistry.prepareRegisterObject(objectId, pointer)

  override def getRegisteredId(namespace: String, name: String): Future[UUID] =
    namespacedRegistry.getRegisteredObject(namespace, name)

  override def registerId(namespace: String, name: String, objectId: UUID): Future[Unit] =
    namespacedRegistry.registerObject(namespace, name, objectId)

  override def prepareRegisterId(namespace: String, name: String, objectId: UUID)(using tx: Transaction): Future[Unit] =
    namespacedRegistry.prepareRegisterObject(namespace, name, objectId)

  override def listRegisteredIds(namespace: String): Future[List[(String, UUID)]] =
    namespacedRegistry.getAllEntries(namespace)

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

  override def createAllocationGroup(groupName: String, level: Int,
                                     maxSliceSize: Option[Int],
                                     idaPolicy: IDAPolicy,
                                     ida: Option[IDA],
                                     isMetadataGroup: Boolean): Future[AllocationGroupId] =
    idaPolicy match
      case IDAPolicy.ExactMatch =>
        if ida.isEmpty then
          throw new IllegalArgumentException("An ExactMatch allocation group requires an ida")
      case IDAPolicy.AllowAll =>
        if ida.nonEmpty then
          throw new IllegalArgumentException("An AllowAll allocation group may not specify an ida")
        // Members may use any IDA, so a slice limit would not determine an object size limit
        if maxSliceSize.nonEmpty then
          throw new IllegalArgumentException("An AllowAll allocation group may not specify a maxSliceSize")

    // A limit below the IDA's minimum admits no objects at all
    for
      limit <- maxSliceSize
      i <- ida
    do
      if limit < i.minSliceSize then
        throw new IllegalArgumentException(s"maxSliceSize ($limit) must be at least ${i.minSliceSize} for $i")

    val ags = AllocationGroupState(
      AllocationGroupId(UUID.randomUUID()),
      level,
      groupName,
      Nil,
      Nil,
      maxSliceSize,
      idaPolicy,
      ida,
      isMetadataGroup
    )

    runIdempotentCreate(allocationGroupsTree, ags.groupId.uuid, Future.successful(ags.groupId)): tx =>
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

    runIdempotentCreate(storageDeviceSetsTree, sds.setId.uuid, Future.successful(sds.setId)): tx =>
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
                          httpPort: Int,
                          storeTransferPort: Int,
                          hostId: HostId): Future[HostId] =
    // The id is fixed outside runCreate, as createStorageDevice does for its device id, so a
    // retried attempt reuses it rather than leaking a fresh UUID per attempt.
    val state = HostState(hostId, name, address, dataPort, httpPort, storeTransferPort, Set())

    // No TaskExecutorRootKey is written. Host inserts it on first start under a
    // DoesNotExist requirement, so seeding it here would only duplicate that.
    runIdempotentCreate(hostsTree, hostId.uuid, Future.successful(hostId)): tx =>
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

    runIdempotentCreate(storageDevicesTree, deviceId.uuid, Future.successful(deviceId)): tx =>
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
        // aborts the transaction on failure, discarding everything staged.
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

  /** Runs a create whose id is chosen outside the retry. After an attempt that committed but was reported
    * TransactionOutcomeUnknown, a tree already mapping `id` holds that attempt's own object (Todo 5.10): nothing is
    * staged, so the empty transaction succeeds, and `created` is the result. Otherwise the retry would fail with
    * KeyAlreadyExists, which reads as a name or id someone else took. A first attempt that finds the id taken still
    * fails that way */
  private def runIdempotentCreate[T](tree: MetadataTree, id: UUID, created: => Future[T])(
      prepare: Transaction => Future[T]): Future[T] =
    given ExecutionContext = clientContext
    val retry = StopRetrying.collisionRetry()
    runCreate(retry.onFailure): tx =>
      if retry.mayHaveCommitted then
        tree.tree.get(Key(id)).flatMap:
          case Some(_) => created
          case None => prepare(tx)
      else
        prepare(tx)

  override protected def createStoragePool(config: StoragePoolState,
                                           metadataGroup: Option[AllocationGroupId]): Future[PoolId] =
    // Metadata pools' trees live in the bootstrap pool, every other pool's in a metadata group
    require(config.isMetadataPool == metadataGroup.isEmpty,
      s"Pool ${config.name}: isMetadataPool=${config.isMetadataPool} but metadataGroup=$metadataGroup")
    // Only a collision is retried: a taken pool name (KeyAlreadyExists), a failed device and an
    // allocation failure are all permanent. The test client's runCreate performs a single
    // attempt regardless.
    // The nudges of a pool found created by an attempt reported Unknown: each device holding one of its stores
    def createdPoolNudges(): Future[List[CheckStorageDevice]] =
      Future.sequence(config.stores.map(_.storageDeviceId).distinct.toList.map: deviceId =>
        getStorageDeviceState(deviceId).map(st => CheckStorageDevice(st.hostId, clientId, deviceId)))

    val fStaged = runIdempotentCreate(storagePoolsTree, config.poolId.uuid, createdPoolNudges()): tx =>
      given Transaction = tx

      // The allocator for the trees' root nodes and the NodeAllocator their later nodes come from
      def treeAllocators(): Future[(ObjectAllocator, NodeAllocator)] = metadataGroup match
        case None =>
          getStoragePool(radicle.poolId).map: radiclePool =>
            (radiclePool.allocator, SinglePoolNodeAllocator.forPool(this, radiclePool))
        case Some(groupId) =>
          val allocatorId = GroupObjectAllocatorId(groupId)
          getAllocator(allocatorId).map: groupAllocator =>
            val maxNodeSize = NodeAllocator.fittingNodeSize(SinglePoolNodeAllocator.DefaultMaxNodeSize,
              NodeAllocator.nodeSizeLimit(groupAllocator, TreeKind.KeyValueObjects))
            (groupAllocator, new NodeAllocator(this, List((allocatorId, maxNodeSize))))

      def createPoolObj(alloc: ObjectAllocator): Future[KeyValueObjectPointer] =
        for
          (treeAlloc, nodeAllocator) <- treeAllocators()
          errTreeRoot <- treeAlloc.allocateKeyValueObject(Map())
          allocTreeRoot <- treeAlloc.allocateKeyValueObject(Map())
          unackedRoot <- treeAlloc.allocateKeyValueObject(Map())

          poolConfig = config.encode()
          errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), nodeAllocator).encode()
          allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), nodeAllocator).encode()
          unackedTree = Root(0, ByteArrayKeyOrdering, Some(unackedRoot), nodeAllocator).encode()

          // An FSIF pool selects FsifIndexAllocationStrategy and gets its zeroed index bitmap from the same
          // metadata allocator as the trees
          strategyEntries <- config.backendConfig match
            case FsifConfig(_, segmentCount) =>
              FsifIndexAllocationStrategy.prepareBitmap(segmentCount, treeAlloc)
            case _ => Future.successful(Map[Key, Value]())

          poolPtr <- alloc.allocateKeyValueObject(Map(
            StoragePoolState.ConfigKey -> Value(poolConfig),
            StoragePoolState.ErrorTreeKey -> Value(errorTree),
            StoragePoolState.AllocationTreeKey -> Value(allocTree),
            StoragePoolState.UnackedCommitsTreeKey -> Value(unackedTree)
          ) ++ strategyEntries)
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
        // collectDevices re-reads device state on every retry attempt, so this catches a device
        // tombstoned after createNewStoragePool's pre-flight check. The partially staged
        // transaction (allocated objects, staged puts) is discarded by transact's abort on
        // failure, as at createStorageDevice (lines 298-302). No test covers this guard because
        // deterministically staging the tombstone-during-retry race is not worth contorting the
        // suite for.
        if du.state.isFailed then
          throw AspenClient.DeviceFailed(du.storageDeviceId)

        if !du.state.isOperational then
          throw AspenClient.DeviceNotOperational(du.storageDeviceId)

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

  private val allocationQueries = new java.util.concurrent.ConcurrentHashMap[UUID, CurrentAllocationsQuery]()

  override private[aspen] def currentAllocations(stores: List[StoreId], needed: Int, stopWhen: () => Boolean)
    : Future[Map[StoreId, List[PendingAllocation]]] =
    given ExecutionContext = clientContext
    pendingTransactions(stores, needed, stopWhen).map(_.view.mapValues(_.allocations).toMap)

  override private[aspen] def heldVotes(stores: List[StoreId], needed: Int, stopWhen: () => Boolean)
    : Future[List[HeldVote]] =
    given ExecutionContext = clientContext
    pendingTransactions(stores, needed, stopWhen).map(_.valuesIterator.flatMap(_.heldVotes).toList)

  private def pendingTransactions(stores: List[StoreId], needed: Int, stopWhen: () => Boolean)
    : Future[Map[StoreId, CurrentAllocationsReply]] =
    val q = new CurrentAllocationsQuery(clientId, messenger.sendClientRequest, backgroundTaskManager, stores, needed,
      stopWhen, id => allocationQueries.remove(id))
    allocationQueries.put(q.queryUUID, q)
    // A query that completed before it was registered must not stay registered
    if q.result.isCompleted then allocationQueries.remove(q.queryUUID)
    q.result

  override private[aspen] def after(delay: FiniteDuration): Future[Unit] =
    val p = scala.concurrent.Promise[Unit]()
    backgroundTaskManager.schedule(delay)(p.trySuccess(()))
    p.future

  def receiveClientResponse(msg: ClientResponse): Unit = msg match
    case m: ReadResponse => rmgr.receive(m)
    case m: CurrentAllocationsReply => Option(allocationQueries.get(m.queryUUID)).foreach(_.receive(m))
    case m: TransactionCompletionResponse => rmgr.receive(m)
    case m: TransactionResolved => txManager.receive(m)
    case m: TransactionFinalized => txManager.receive(m)
    case m: TransactionVote => txManager.receive(m)

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
    // One id for every attempt: a retry after an attempt that committed but was reported Unknown finds it enrolled
    val taskId = UUID.randomUUID()
    transactUntilSuccessful: tx =>
      given Transaction = tx
      // Nothing else in this transaction pins it
      prepareSystemDurableTask(taskTypeUUID, initialState, pinned = true, Some(taskId))

  /** Signals that the pool's set pointer already equals the target. Thrown from inside the
   *  transaction so nothing is staged: Aspen has no empty transactions. Recovered to unit. */
  private class SameSetNoOp extends Throwable("pool is already assigned to the target set")

  override def migratePoolToSet(poolId: PoolId,
                                targetSetId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = clientContext

    // SameSetNoOp stops the loop like any other non-collision error and is recovered below.
    val migrated = transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
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

  // Deliberately no pool-health guard (shutdownStorageDevice has one): this is for a physically failed drive whose data
  // is already unrecoverable, so refusing would only delay the rebuild of what was lost (AspenClient.failStorageDevice)
  override def failStorageDevice(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = clientContext

    // One task id for every attempt. A retry after an attempt that committed but was reported Unknown finds its own
    // task enrolled, or the device already tombstoned by it, and succeeds rather than enrolling a second task or
    // failing with DeviceAlreadyFailed (Todo 5.10)
    val taskId = UUID.randomUUID()
    val retry = StopRetrying.collisionRetry()
    transactUntilSuccessfulWithRecovery(retry.onFailure): tx =>
      given Transaction = tx

      for
        devPtr <- getStorageDevicePointer(deviceId)
        devKvos <- read(devPtr, "fail storage device")
        state = StorageDeviceState(devKvos)
        enrolled <- if retry.mayHaveCommitted then systemTaskEnrolled(taskId) else Future.successful(false)
        _ <-
          if enrolled then
            Future.unit
          else if state.isFailed then
            throw AspenClient.DeviceAlreadyFailed(deviceId)
          else
            // Only the device id is recorded. The task re-reads everything else -- including the
            // device's set -- inside its own transactions, so nothing here can go stale between
            // enrollment and the first pass.
            // The device state is only read, so the enrollment must pin the transaction
            FailedStorageDeviceDurableTask.prepareSystemTask(this, deviceId, pinned = true, Some(taskId))
      yield ()

  /** Whether the system-task registry holds `taskId` */
  private def systemTaskEnrolled(taskId: UUID): Future[Boolean] =
    given ExecutionContext = clientContext
    systemTaskServiceStatePointer().flatMap(statePtr => SystemTaskServiceState.contains(this, statePtr, taskId))

  private def systemTaskServiceStatePointer(): Future[KeyValueObjectPointer] =
    given ExecutionContext = clientContext
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map:
      case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
      case None => throw StopRetrying(
        new IllegalStateException("SystemTaskExecutorService is not registered"))

  override def prepareSystemDurableTask(taskTypeUUID: UUID,
                                        initialState: Map[Key, Array[Byte]],
                                        pinned: Boolean,
                                        fixedTaskId: Option[UUID])
                                       (using tx: Transaction): Future[Unit] =
    given ExecutionContext = clientContext

    // Sanity check: the type must resolve to a DurableTaskFactory.
    typeRegistry.getType[DurableTaskFactory](taskTypeUUID) match
      case None =>
        Future.failed(StopRetrying(
          new IllegalArgumentException(s"prepareSystemDurableTask: no DurableTaskFactory registered for $taskTypeUUID")))
      case Some(_) =>
        val taskId = fixedTaskId.getOrElse(UUID.randomUUID())

        val taskContent: Map[Key, Value] =
          (initialState + (SimpleTaskExecutor.TaskTypeKey -> uuid2byte(taskTypeUUID)))
            .map((k, v) => k -> Value(v))

        def enroll(statePtr: KeyValueObjectPointer): Future[Unit] =
          for
            pool <- getStoragePool(Radicle.poolId)
            allocator = new PoolObjectAllocator(this, pool)
            taskStatePtr <- allocator.allocateKeyValueObject(taskContent)
            _ <- SystemTaskServiceState.enrollInTx(this, statePtr, taskId, taskStatePtr, pinned)
          yield ()

        // Sent whether this attempt enrolled the task or found it enrolled by one reported Unknown, whose own
        // tx.result failed: the wake-up is best effort and idempotent
        tx.result.foreach: _ =>
          sendServiceMessage(SystemTaskExecutorService.ServiceUUID,
            SystemTaskMessage.encode(NewSystemTaskAdded(taskId)))

        for
          statePtr <- systemTaskServiceStatePointer()
          enrolled <-
            if fixedTaskId.isDefined then SystemTaskServiceState.contains(this, statePtr, taskId)
            else Future.successful(false)
          _ <- if enrolled then Future.unit else enroll(statePtr)
        yield ()

  def getSystemAttribute(key: String): Option[String] = attributes.get(key)
  def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value
