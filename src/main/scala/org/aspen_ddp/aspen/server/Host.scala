package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, StoragePool, Transaction, ObjectState as ClientObjectState}
import org.aspen_ddp.aspen.server.repair.{RepairService, RepairTarget, StoreNotHosted}

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, runSequentially}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, Completion, CompletionHandler, RocksDBConfig}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.store.{Frontend, Store}
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import scribe.Logging
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectId, ReadError, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRevision}
import org.aspen_ddp.aspen.server.transfer.{StoreTransfer, StoreTransferService}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.compute.{DurableServiceExecutor, ServiceEntry, TaskExecutor}
import org.aspen_ddp.aspen.compute.impl.{SimpleDurableServiceExecutor, SimpleTaskExecutor}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskRunner}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.server.usage.StoragePoolUsageManager
import org.aspen_ddp.aspen.server.usage.StorageDeviceUsageManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
import org.aspen_ddp.aspen.common.rebalancing.{RebalancingDurableService, RebalancingMessage, TransferComplete}

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object Host:
  private sealed abstract class Event

  private case class IOCompletion(op: Completion) extends Event
  private case class TransactionMessage(msg: TxMessage) extends Event
  private case class ClientReq(msg: ClientRequest) extends Event
  private case class HostMsg(msg: HostMessage) extends Event
  private case class Repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]) extends Event
  private case class RepairDelete(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte],
                                  completion: Promise[Unit]) extends Event:
    override def equals(other: Any): Boolean = other match
      case that: RepairDelete => storeId == that.storeId && objectId == that.objectId &&
        java.util.Arrays.equals(storePointer, that.storePointer) && completion == that.completion
      case _ => false

    override def hashCode: Int = (storeId, objectId, java.util.Arrays.hashCode(storePointer), completion).##
  private case class LoadStore(storageDeviceId: StorageDeviceId, backend: Backend,
                                  maxSliceSize: Option[Long], completion: Promise[Unit]) extends Event
  private case class LoadStoreById(sstorageDeviceId: StorageDeviceId, toreId: StoreId) extends Event
  private case class Exit() extends Event
  private case class RecoveryEvent() extends Event
  private case class HeartbeatEvent() extends Event
  private case class CheckAllDevices() extends Event
  private case class ScanDevices() extends Event
  private case class ShutdownMarkerResolved(path: os.Path,
                                            cfg: StorageDeviceConfig,
                                            markerHost: HostId,
                                            state: Try[StorageDeviceState]) extends Event
  private case class ShutdownStore(storeId: StoreId, completion: Promise[Unit]) extends Event
  private case class InitializeTaskExecutor() extends Event

  /** What updateStateForTransferredStore decided about a transfer that has finished shipping
   *  bytes, so its caller knows whether the destination is allowed to keep the copy that just
   *  arrived.
   *
   *  Returned rather than acted on inside that method because the two halves live on opposite
   *  sides of the boundary: the decision is metadata and belongs in the transaction, while
   *  loading or discarding a store directory is local disk work that only the event loop does.
   */
  private[aspen] enum TransferOutcome:
    /** The pool now names the destination, which therefore owns the arrived copy. */
    case PoolRepointed

    /** As PoolRepointed, but the destination is shutting down or shut down, so its entry was
     *  written Offline rather than Active. The arrived copy is ours but must not be served: it
     *  stays unloaded behind its transferred-in marker until a restart or adoption makes the
     *  entry Active, when the reconcile's completed-transfers-in pass loads it. */
    case PoolRepointedOffline

    /** The destination was declared failed mid-flight, so the source was put back to Active and
     *  the pool left naming it. The arrived copy belongs to nobody. */
    case SourceRestored

    /** The transfer was aborted (AspenClient.abortStoreTransfer) before its bytes were recorded:
     *  the source's entry is no longer TransferringOut to the destination. The pool still names
     *  the source, which kept its copy, so the arrived copy belongs to nobody.
     *
     *  Also the outcome when the source was tombstoned and drained first: the store was rebuilt on
     *  another device, the pool names that one, and the destination's stale entry is removed. */
    case Aborted

  /** What markRebuiltStoreActive decided about a rebuilt store, so its caller knows whether the
   *  device is allowed to adopt the copy the rebuild placed.
   *
   *  Returned rather than acted on inside that method because the two halves live on opposite
   *  sides of the boundary: the decision is metadata and belongs in the transaction, while
   *  loading or discarding a store directory is local disk work that only the event loop does.
   */
  private[aspen] enum RebuildOutcome:
    /** The device entry was flipped to Active (or was already Active), so the store should be
     *  loaded. */
    case Adopt

    /** The device is shutting down or shut down, so the entry was written Offline rather than
     *  Active (or was found Offline already). The rebuilt store is the device's, but must not be
     *  served: it is kept offline behind a transfer-out marker, like every other store the
     *  shutdown closed, until a restart or adoption brings it back. */
    case AdoptOffline

    /** The tree does not say this device owns a rebuilt store: the device was tombstoned,
     *  the entry vanished, or the entry has a status other than Rebuilding/Active. The rebuilt
     *  store belongs to nobody and must be discarded. */
    case Discard

  /** What completeDeviceShutdown decided, so shutDownDevice knows whether to unload the device,
   *  go round again, or undo its own on-disk work.
   *
   *  Returned rather than acted on inside that method for the same reason as TransferOutcome:
   *  the decision is metadata, while unloading and marker files are local work.
   */
  private[aspen] enum ShutdownOutcome:
    /** The device is Shutdown and has no Active entry. It may be unloaded. */
    case Completed

    /** Some entry was Active although the shutdown request had committed -- defence in depth,
     *  as every writer of an Active entry writes Offline on a shutting-down device -- and has
     *  been flipped to Offline instead of committing Shutdown. Its store may be open, so the
     *  shutdown goes round again to close it. */
    case StoresReactivated

    /** The device is no longer ShuttingDown: a restart arrived (or the device was declared
     *  failed). Nothing was written. */
    case Cancelled


  class IOHandler(host: Host) extends CompletionHandler:
    override def complete(op: Completion): Unit =
      host.events.add(IOCompletion(op))

  class LocalStorageDeviceState(val storageDeviceId: StorageDeviceId,
                                val devicePath: os.Path,
                                val configFile: os.Path):
    var loadedStores: Set[StoreId] = Set()
    var offlineStores: Set[StoreId] = Set()
    
    def totalSize: Long = configFile.toIO.getTotalSpace
    def currentUsage: Long = totalSize - configFile.toIO.getFreeSpace

  /** `toDeviceId` is the local device receiving the store, so a check of that device can tell
   *  which of its transfers in the tree no longer records. */
  class PendingTransfer(val msg: StartStoreTransfer,
                        val toDeviceId: StorageDeviceId,
                        var lastSend: HLCTimestamp)

  /** A failed transfer in waiting out TransferInRetryDelay. `fromDeviceId` is the source the retry
   *  would ask for the store again, `toDeviceId` the local device receiving it. */
  class TransferInRetry(val fromDeviceId: StorageDeviceId,
                        val toDeviceId: StorageDeviceId,
                        val run: () => Unit)

  /** How long a failed transfer in waits before it is restarted */
  val TransferInRetryDelay: Duration = Duration(30, SECONDS)

  /** How long after a transfer out begins, or is found still sending, its source device is
   *  checked again. See scheduleDeviceRecheck. */
  val TransferOutRecheckDelay: Duration = Duration(10, SECONDS)


class Host(val client: AspenClient,
                   val hostId: HostId,
                   val aspenSystemId: UUID,
                   val rootDir: os.Path,
                   val ec: ExecutionContext,
                   val objectCacheFactory: StoreId => ObjectCache,
                   val net: Messenger,
                   val backgroundTasks: BackgroundTaskManager,
                   crlFactory: CrashRecoveryLogFactory,
                   val finalizerFactory: TransactionFinalizer.Factory,
                   val txDriverFactory: TransactionDriver.Factory,
                   val heartbeatPeriod: Duration,
                   val checkStorageDevicePeriod: Duration,
                   val storeTransferService: StoreTransferService = StoreTransferService.Disabled,
                   val storeRebuildFactory: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                   /** A rebuild reads a whole store's worth of data through the client, so an
                    *  eight-store device must not start eight at once. Entries beyond this bound
                    *  wait; every completion re-checks all loaded devices and starts whatever is
                    *  queued. */
                   val maxConcurrentRebuilds: Int = 2,
                   /** How often storage-devices/ is listed for newly present device directories.
                    *  Only the local directory scan runs this often. The network-reading
                    *  reconcile of every loaded device stays on checkStorageDevicePeriod. */
                   val deviceScanPeriod: Duration = Duration(10, SECONDS)) extends Logging with RepairTarget {
  import Host._
  
  given ExecutionContext = ec

  private val events = new LinkedBlockingQueue[Event]()

  private val ioHandler = new IOHandler(this)

  private val txStatusCache = new TransactionStatusCache()

  private val crl = crlFactory.createCRL()

  private val threadPool = Executors.newFixedThreadPool(1)

  protected var shutdownCalled = false
  private val shutdownPromise: Promise[Unit] = Promise()

  val storageDevicesDir: os.Path = rootDir / StorageDeviceManager.StorageDevicesDirName

  // tryLoadDevice is the only writer that adds entries in production. Entries are removed when a
  // device is shut down and unloaded, so a lookup miss means a device that is not loaded right
  // now: one never seen, one rejected by the load decision, or one unloaded by a shutdown. Tests
  // inject entries directly via RecordingHost.injectLoadedDevice.
  protected var storageDevices: Map[StorageDeviceId, LocalStorageDeviceState] = Map()

  // Directories under storage-devices/ that hold a loaded device. checkForNewDevices skips these
  // without reading anything in them, which keeps the frequent scan down to one directory
  // listing. An entry is dropped when its path stops being listed, so a disk pulled and
  // re-inserted on this host is checked again in full (and the duplicate guard in tryLoadDevice
  // finds it already loaded). Unloading a device must also remove its entry.
  private var loadedDeviceDirs: Map[os.Path, StorageDeviceId] = Map()

  // Directories whose problem has already been logged. The scan rechecks an unloaded directory
  // every deviceScanPeriod, so without this every unowned or broken device would produce a
  // warning every few seconds forever. In memory only: a restart logs each problem once more. A
  // path leaves the set when its device loads, so a later problem with it is reported again.
  private var reportedDeviceDirs: Set[os.Path] = Set()

  // Device directories carrying shutdown.state whose StorageDeviceState lookup, or the adoption
  // that lookup led to, is still in flight. One at a time per path: the scan reaches such a
  // directory every deviceScanPeriod, and without this a slow read would pile up a lookup per
  // scan, and a slow adoption a second transaction racing the first. A path leaves the set once
  // its outcome has been acted on, so the next scan decides afresh.
  private var pendingMarkerResolutions: Set[os.Path] = Set()

  protected var stores: Map[StoreId, Store] = Map()

  private var offlineStores: Set[StoreId] = Set()
  private var creatingStores: Set[StoreId] = Set()
  private var rebuildingStores: Map[StoreId, StoreRebuild] = Map()
  // The transfer UUID of each store this host is currently receiving. Stores being shipped out
  // are tracked by storeTransferService, which is what knows when a send has ended.
  private var transferringIn: Map[StoreId, UUID] = Map()
  private var abandoningTransferIns: Set[StoreId] = Set()
  private var pendingStartTransfers: Map[StoreId, PendingTransfer] = Map()
  // The restart of each failed transfer in that is waiting out TransferInRetryDelay. Its store
  // stays in transferringIn meanwhile, so device checks do not start a parallel attempt.
  private var transferInRetries: Map[StoreId, TransferInRetry] = Map()
  private var activeDeviceChecks: Set[StorageDeviceId] = Set()
  // Devices with a re-check waiting out TransferOutRecheckDelay. At most one each.
  private var scheduledDeviceRechecks: Set[StorageDeviceId] = Set()
  private var deferredDeviceChecks: Set[StorageDeviceId] = Set()

  // Devices whose shutDownDevice sequence is running. One at a time per device: every check of a
  // ShuttingDown device calls shutDownDevice, and the sequence spans several asynchronous steps,
  // so without this a nudge or the hourly check arriving mid-shutdown would close stores, write
  // markers and transact the device state a second time in parallel with the first.
  private var shuttingDownDevices: Set[StorageDeviceId] = Set()

  private val taskExecutorPromise: Promise[TaskExecutor] = Promise()
  // Runs the DurableServices this host claims. Every host scans the services TKVL for unclaimed or expired
  // entries, with its scan interval scaled by the known host count (net.knownHostCount) so the cluster as a whole
  // scans about every 15 seconds however many hosts there are. See the DurableService scaladoc for details.
  private val serviceExecutorPromise: Promise[SimpleDurableServiceExecutor] = Promise()
  private val systemTaskRunnerPromise: Promise[SystemTaskRunner] = Promise()
  private val poolUsageManager = new StoragePoolUsageManager(client)
  private val deviceUsageManager = new StorageDeviceUsageManager(client)
  private var usageUpdateTask: Option[ScheduledTask] = None

  /** All StoreMoved and HostEndpointChanged gossip goes through here, so hosts that miss the
   *  first broadcast still hear it from a retransmit. */
  private[aspen] val gossipRetransmitter =
    new GossipRetransmitter(GossipRetransmitter.stateFile(rootDir), net.broadcastGossip)

  private val gossipRetransmitTask = backgroundTasks.schedulePeriodic(GossipRetransmitter.Period):
    gossipRetransmitter.retransmit()

  private val pendingStartTask = backgroundTasks.schedulePeriodic(Duration(30, SECONDS)):
    retransmitPendingStartTransfers()

  /** Re-sends the StartStoreTransfer of every transfer in that has not yet completed and was last
   *  sent more than 30 s ago. `force` skips the age check so tests need not wait it out. */
  private[aspen] def retransmitPendingStartTransfers(force: Boolean = false): Unit = synchronized {
    val now = HLCTimestamp.now
    pendingStartTransfers.valuesIterator.foreach: pt =>
      if force || now - pt.lastSend > Duration(30, SECONDS) then
        pt.lastSend = now
        client.sendHostMessage(pt.msg)
  }

  private val heartbeatTask = backgroundTasks.schedulePeriodic(heartbeatPeriod) {
    events.put(HeartbeatEvent())
  }
  
  private val checkStorageDeviceTask = backgroundTasks.schedulePeriodic(checkStorageDevicePeriod) {
    events.put(CheckAllDevices())
  }

  private val deviceScanTask = backgroundTasks.schedulePeriodic(deviceScanPeriod) {
    events.put(ScanDevices())
  }

  /** Continual repair of this host's stores. Constructed here rather than after task-executor
    * initialization: early sweeps simply find few stores, and RepairService falls back to
    * default policy for metadata that is not readable yet.
    */
  private val repairService = new RepairService(client, hostId, this, backgroundTasks)(using ec)

  checkForNewDevices()

  // After we've loaded all the stores, initiate an initial check of all our
  // devices in case operations were preformed while we were down and missed
  // the CheckDeviceState messages
  events.put(CheckAllDevices())
  events.put(InitializeTaskExecutor())

  private def initializeTaskExecutor(): Unit =
    import scala.util.{Failure, Success}

    val hostPointerFuture = client.getHostPointer(hostId)

    hostPointerFuture.onComplete:
      case Success(hostPtr) =>
        client.read(hostPtr).foreach: hostKvos =>
          hostKvos.contents.get(HostState.TaskExecutorRootKey) match
            case Some(vs) =>
              val executorPtr = KeyValueObjectPointer(vs.value.bytes)
              client.getStoragePool(Radicle.poolId).foreach: pool =>
                val allocator = new PoolObjectAllocator(client, pool)
                SimpleTaskExecutor(client, allocator, executorPtr).foreach: executor =>
                  synchronized:
                    taskExecutorPromise.success(executor)
                    startUsageTracking(executor)
                    val serviceExec = new SimpleDurableServiceExecutor(client, hostId, backgroundTasks,
                      hostCount = () => net.knownHostCount)
                    serviceExecutorPromise.success(serviceExec)
                    initializeSystemTaskRunner(executor)

            case None =>
              client.getStoragePool(Radicle.poolId).foreach: pool =>
                val allocator = new PoolObjectAllocator(client, pool)

                client.transactUntilSuccessful: tx =>
                  given scala.concurrent.ExecutionContext = ec
                  for
                    executorRoot <- allocator.allocateKeyValueObject(Map())(using tx)
                    currentHostKvos <- client.read(hostPtr)
                  yield
                    val reqs = List(DoesNotExist(HostState.TaskExecutorRootKey))
                    val ops = List(Insert(HostState.TaskExecutorRootKey, executorRoot.toArray))
                    tx.update(hostPtr, None, None, reqs, ops)
                    executorRoot
                .foreach: executorRoot =>
                  SimpleTaskExecutor(client, allocator, executorRoot).foreach: executor =>
                    synchronized:
                      taskExecutorPromise.success(executor)
                      startUsageTracking(executor)
                      val serviceExec = new SimpleDurableServiceExecutor(client, hostId, backgroundTasks,
                        hostCount = () => net.knownHostCount)
                      serviceExecutorPromise.success(serviceExec)
                      initializeSystemTaskRunner(executor)

      case Failure(err) =>
        // In test environments or when host is not yet registered, silently skip initialization
        logger.debug(s"TaskExecutor initialization skipped: ${err.getMessage}")

  private def initializeSystemTaskRunner(executor: TaskExecutor): Unit =
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, client.radicle))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).foreach:
      case Some(vs) =>
        val statePtr = ServiceEntry.decode(vs.value.bytes).statePointer
        val runner = new SystemTaskRunner(client, statePtr, hostId, executor)
        synchronized:
          if !systemTaskRunnerPromise.isCompleted then
            systemTaskRunnerPromise.success(runner)
      case None =>
        logger.debug("SystemTaskExecutorService not registered; system task runner not started")

  private def startUsageTracking(executor: TaskExecutor): Unit =
    poolUsageManager.setTaskExecutor(executor)

    usageUpdateTask = Some(backgroundTasks.scheduleNonConcurrentPollingTask(
      "store-usage-update", Duration(20, SECONDS)):
      val (slist, dlist) =
        synchronized:
          (stores.valuesIterator.toList, storageDevices.valuesIterator.toList)
      val fs = runSequentially(slist): store =>
        poolUsageManager.updateStoreSize(store.storeId, store.estimateSize())
      val fd = runSequentially(dlist): sds =>
        deviceUsageManager.updateDeviceUsage(sds.storageDeviceId, sds.currentUsage, sds.totalSize)

      Future.sequence(List(fs, fd))
    )

  def getTaskExecutor(): Future[TaskExecutor] = taskExecutorPromise.future

  def getServiceExecutor(): Future[DurableServiceExecutor] = serviceExecutorPromise.future

  /** Scans storage-devices/ and loads any device found there that this host owns.
   *
   *  Called at construction and from the event loop: every ScanDevices (each deviceScanPeriod),
   *  every CheckAllDevices, and every CheckStorageDevice naming a device we have not loaded.
   *  Directories in loadedDeviceDirs are skipped without being read, so the common-case scan is
   *  a single listing. Every other directory gets tryLoadDevice's full check each time; its
   *  problems are logged once (warnOnce) and then rechecked silently.
   *
   *  Repeated scans are safe: tryLoadDevice also skips any device already present in
   *  storageDevices, so a device's LocalStorageDeviceState -- and with it the offlineStores set
   *  that reconcileDeviceState reads -- survives, and its children are not re-offered to
   *  tryLoadStore over already-open backends.
   *
   *  Returns the devices this call loaded, so ScanDevices can check them at once.
   *
   *  Mutual exclusion: the handleEvent calls hold the instance lock; the constructor call
   *  precedes start(), so no event-loop thread exists yet.
   */
  private def checkForNewDevices(): List[StorageDeviceId] =
    if ! os.isDir(storageDevicesDir) then
      warnOnce(storageDevicesDir, s"Invalid storage devices directory: $storageDevicesDir")
      Nil
    else
      // Listing can fail with an IO error even when isDir just succeeded. Uncaught, the
      // constructor and the event loop fare differently: from the constructor it would abort
      // Host construction outright, while from the event loop start()'s catch-all would keep the
      // loop running but report only the event class and the exception. Warn here so both name
      // the directory that could not be listed.
      Try(os.list(storageDevicesDir)) match
        case Failure(t) =>
          warnOnce(storageDevicesDir, s"Failed to list storage devices directory: $storageDevicesDir. Error: $t")
          Nil
        case Success(paths) =>
          reportedDeviceDirs -= storageDevicesDir
          // A loaded device whose directory is gone is forgotten here, so it is read in full
          // again if it comes back. Its storageDevices entry is untouched: its stores are open.
          val listed = paths.toSet
          loadedDeviceDirs = loadedDeviceDirs.filter((path, _) => listed.contains(path))
          paths.filterNot(loadedDeviceDirs.contains).flatMap(tryLoadDevice).toList

  /** Logs `msg` unless a problem with `path` has already been logged by this process. */
  private def warnOnce(path: os.Path, msg: => String): Unit =
    if ! reportedDeviceDirs.contains(path) then
      reportedDeviceDirs += path
      logDeviceDirWarning(path, msg)

  // protected so a test subclass can count what warnOnce lets through.
  protected def logDeviceDirWarning(path: os.Path, msg: String): Unit = logger.warn(msg)

  /** Called instead of loading a device that carries `shutdown.state`, naming `markerHost`.
   *
   *  Whether such a device stays shut down, is restarted in place, or is adopted by this host
   *  depends on its StorageDeviceState, which is a network read, so the load decision cannot be
   *  made here. The lookup is issued and its result queued as a ShutdownMarkerResolved event,
   *  which shutdownMarkerResolved acts on from the event loop. Until then the directory is not
   *  in loadedDeviceDirs, so later scans reach it again; pendingMarkerResolutions keeps them from
   *  issuing a second lookup while this one is in flight.
   *
   *  Caller holds the instance lock.
   */
  private def resolveShutdownMarker(storageDevicePath: os.Path,
                                    sdCfg: StorageDeviceConfig,
                                    markerHost: HostId): Unit =
    if ! pendingMarkerResolutions.contains(storageDevicePath) then
      pendingMarkerResolutions += storageDevicePath
      // A throw before the Future exists would register no callback, and the path would stay
      // pending -- and the device unresolved -- for the life of the process.
      val lookup =
        try lookupStorageDeviceState(sdCfg.storageDeviceId)
        catch case t: Throwable => Future.failed(t)
      lookup.onComplete: result =>
        events.put(ShutdownMarkerResolved(storageDevicePath, sdCfg, markerHost, result))

  /** Acts on the StorageDeviceState looked up for a device carrying `shutdown.state`:
   *
   *  | marker names | device state                      | action                                  |
   *  |--------------|-----------------------------------|-----------------------------------------|
   *  | this host    | Shutdown                          | not loaded, quietly: it awaits a restart or a move |
   *  | this host    | ShuttingDown                      | loaded: a crash before Shutdown committed. The reconcile finishes the shutdown |
   *  | this host    | Operational, owned here           | restarted while unloaded: brought back into service |
   *  | another host | Shutdown, owned by the marker host| adopted, if every store was closed cleanly |
   *  | another host | Operational, owned here           | adopted earlier, crashed before the load: brought into service |
   *  | any          | Failed, or anything else          | warned about once, not loaded            |
   *  | lookup failed|                                   | warned about once, retried on the next scan |
   *
   *  The ShuttingDown row for another host is the important refusal: a disk pulled before its
   *  shutdown finished may still have stores whose CRL state is only on the old host, and its
   *  metadata still names that host. It must go back there to finish shutting down.
   *
   *  Every row that does nothing leaves the directory out of loadedDeviceDirs, so the next scan
   *  decides again from a fresh read: a restart or a finished shutdown is picked up that way.
   *
   *  Called from handleEvent, which holds the instance lock.
   */
  private def shutdownMarkerResolved(path: os.Path,
                                     sdCfg: StorageDeviceConfig,
                                     markerHost: HostId,
                                     result: Try[StorageDeviceState]): Unit =
    import StorageDeviceState.DeviceStatus
    pendingMarkerResolutions -= path
    val deviceId = sdCfg.storageDeviceId
    try
      // The disk was read before the lookup went out. If it has been pulled, or its marker has
      // changed, since then, this result describes a directory that is no longer there to act
      // on; the next scan starts over from what the disk says now.
      val stillMarked = os.isDir(path) && DeviceMarkers.readShutdownState(path).contains(markerHost)
      if ! stillMarked || loadedDeviceDirs.contains(path) then
        ()
      else if storageDevices.get(deviceId).exists(_.devicePath != path) then
        warnOnce(path, s"Storage device $deviceId is already loaded from " +
                       s"${storageDevices(deviceId).devicePath}; ignoring duplicate at $path")
      else
        result match
          case Failure(err) =>
            warnOnce(path, s"Failed to read the state of shut-down storage device $deviceId at $path. " +
                           s"Not loading it. Will retry on the next scan. Error: $err")

          case Success(state) =>
            val ownedHere = state.hostId == hostId
            def refuse(why: String): Unit =
              warnOnce(path, s"Storage device $deviceId at $path was shut down by host $markerHost, " +
                             s"and the storage-devices tree records it as ${state.status} owned by " +
                             s"host ${state.hostId}. $why")

            if state.isFailed then
              refuse("It has been declared failed. Not loading it")
            else if markerHost == hostId then
              state.status match
                case DeviceStatus.Shutdown if ownedHere => ()
                case DeviceStatus.ShuttingDown if ownedHere =>
                  logger.info(s"Storage device $deviceId at $path was being shut down when this host " +
                              s"stopped. Loading it to finish the shutdown")
                  loadOwnedDevice(path, path / StorageDeviceConfig.configFilename, sdCfg)
                    .foreach(checkStorageDevice)
                case DeviceStatus.Operational if ownedHere =>
                  logger.info(s"Storage device $deviceId at $path has been restarted. Loading it")
                  bringIntoService(path, sdCfg, state, rewriteOwner = false)
                case _ =>
                  refuse("Not loading it")
            else
              state.status match
                case DeviceStatus.Shutdown if state.hostId == markerHost =>
                  tryAdoptDevice(path, sdCfg, state, markerHost)
                case DeviceStatus.Operational if ownedHere =>
                  logger.info(s"Storage device $deviceId at $path was adopted by this host but not " +
                              s"yet loaded. Finishing the adoption")
                  bringIntoService(path, sdCfg, state, rewriteOwner = true)
                case DeviceStatus.ShuttingDown if state.hostId == markerHost =>
                  refuse("Its shutdown did not finish, so some of its stores may not have been " +
                         "closed. Not adopting it. Return it to that host to finish the shutdown")
                case _ =>
                  refuse("Not loading or adopting it")
    catch
      case t: Throwable =>
        warnOnce(path, s"Failed to resolve the shutdown of storage device $deviceId at $path. " +
                       s"Will retry on the next scan. Error: $t")

  /** Whether the store at `storePath`, whose entry is Offline, can be put back into service by an
   *  adoption: every piece of its state must be in its own directory, since the host that took
   *  it offline is not here.
   *
   *  An Offline entry on a shut-down device comes from one of three places, and each leaves a
   *  recognisable directory:
   *  - a store the shutdown closed: its transfer-out marker and the CRL save file ShutdownStore
   *    wrote beside it (a transfer out rolled back to Offline is closed the same way);
   *  - a store rebuilt during or after the shutdown (keepRebuiltStoreOffline): the same pair, the
   *    CRL file empty since a fresh rebuild has no transaction state;
   *  - a transfer in that landed during the shutdown (PoolRepointedOffline): its transferred-in
   *    marker. It was never opened on this device, so it has no CRL state to carry.
   *  Anything else -- a missing directory, a marker with no CRL file -- is a store whose close did
   *  not finish, and adopting it would drop the transactions its CRL state was holding.
   */
  private def closedForAdoption(storePath: os.Path): Boolean =
    os.isDir(storePath) &&
      ((os.exists(storePath / StoreTransfer.TransferOutMarkerFile) && os.exists(Backend.crlSaveFile(storePath))) ||
        os.exists(storePath / StoreTransfer.TransferredInMarkerFile))

  /** Validates a device shut down by `previousHostId` and, if every Offline store was closed
   *  cleanly, adopts it. On success the post-commit steps run from the transaction's callback.
   *
   *  The directory stays in pendingMarkerResolutions until the adoption has been acted on, so a
   *  scan arriving meanwhile does not start a second one.
   *
   *  Caller holds the instance lock.
   */
  private def tryAdoptDevice(path: os.Path,
                             sdCfg: StorageDeviceConfig,
                             state: StorageDeviceState,
                             previousHostId: HostId): Unit =
    val deviceId = sdCfg.storageDeviceId
    val unclean = state.stores.collect {
      case (storeId, entry) if entry.status == StorageDeviceState.StoreStatus.Offline &&
                               !closedForAdoption(path / storeId.directoryName) => storeId
    }.toList.sortBy(s => (s.poolId.uuid, s.poolIndex))

    if unclean.nonEmpty then
      warnOnce(path, s"Storage device $deviceId at $path was shut down by host $previousHostId, but " +
                     s"stores ${unclean.mkString(", ")} were not closed cleanly (missing directory, " +
                     s"transfer-out marker or CRL save file). Not adopting it. It may have been " +
                     s"removed too early: return it to that host to finish the shutdown")
    else
      logger.info(s"Adopting storage device $deviceId at $path, shut down by host $previousHostId")
      pendingMarkerResolutions += path
      val fadopt =
        try adoptDevice(deviceId, previousHostId)
        catch case t: Throwable => Future.failed(t)
      fadopt.onComplete: result =>
        synchronized:
          pendingMarkerResolutions -= path
          result match
            case Success(true) =>
              // What adoptDevice wrote, without a re-read: the rows it changed are exactly these.
              val adopted = state.withOfflineStoresActive.copy(
                hostId = hostId, status = StorageDeviceState.DeviceStatus.Operational)
              try bringIntoService(path, sdCfg, adopted, rewriteOwner = true)
              catch case t: Throwable =>
                // The adoption is committed, so the next scan reaching this directory takes the
                // "Operational, owned here" row and runs these steps again.
                logger.error(s"Adopted storage device $deviceId but failed to load it from $path. " +
                             s"Will retry on the next scan. Error: $t")
            case Success(false) =>
              logger.info(s"Storage device $deviceId changed state before it could be adopted. " +
                          s"The next scan will look at it again")
            case Failure(t) =>
              warnOnce(path, s"Failed to adopt storage device $deviceId at $path. Will retry on the " +
                             s"next scan. Error: $t")

  /** The local half of a restart or adoption, once `state` -- Operational and owned by this host
   *  -- has committed. Each step is idempotent, so a crash between any two of them is finished by
   *  the next scan, which finds shutdown.state still present and lands here again:
   *  1. `owning-host` is rewritten to name this host (adoption only), so once shutdown.state is
   *     gone the device loads here on its own.
   *  2. The transfer-out marker the shutdown left is removed from every store now Active, except
   *     one whose entry still names a transfer or that this process is sending: there the marker
   *     belongs to a transfer. Offline entries -- none are expected -- keep theirs and stay
   *     unloaded. A transferred-in marker is left alone: the reconcile's completed-transfers-in
   *     pass clears it and loads the store, as it would after any crash.
   *  3. shutdown.state is removed.
   *  4. The device is loaded through tryLoadDevice and checked at once, so its Rebuilding and
   *     TransferringIn work resumes now rather than at the next periodic check.
   *  5. StoreMoved is gossiped for every Active store, read from the pool as for a completed
   *     transfer in: the shutdown announced them all offline, and after an adoption peers must
   *     also learn the new host. The stores' LoadStore events are already queued ahead of any
   *     message a peer sends in response.
   *
   *  A failed marker removal in step 2 only leaves that store offline: the reconcile's
   *  abandoned-transfer-out pass picks it up on a later check. A failed step 3 leaves the whole
   *  device to the next scan.
   *
   *  Caller holds the instance lock.
   */
  private def bringIntoService(path: os.Path,
                               sdCfg: StorageDeviceConfig,
                               state: StorageDeviceState,
                               rewriteOwner: Boolean): Unit =
    if rewriteOwner then
      DeviceMarkers.writeOwningHost(path, hostId)

    val active = state.stores.collect {
      case (storeId, entry) if entry.status == StorageDeviceState.StoreStatus.Active => storeId -> entry
    }
    active.foreach: (storeId, entry) =>
      val markerFile = path / storeId.directoryName / StoreTransfer.TransferOutMarkerFile
      if entry.transferDevice.isEmpty && !storeTransferService.isTransferringOut(storeId) &&
         os.exists(markerFile) then
        try os.remove(markerFile)
        catch case t: Throwable =>
          logger.error(s"Failed to remove the transfer-out marker for store $storeId at $markerFile. " +
                       s"The store stays offline until a later check of its device. Error: $t")

    if os.exists(path / DeviceMarkers.ShutdownStateFile) then
      DeviceMarkers.removeShutdownState(path)

    tryLoadDevice(path).foreach: deviceId =>
      checkStorageDevice(deviceId)
      active.keysIterator.foreach(gossipStoreOnline)

  /** Loads the device at `storageDevicePath` if this host owns it, returning its id when this
   *  call is what loaded it.
   *
   *  The load decision is made from device-local files before anything on the device is
   *  touched -- not even its stale staging directory is removed -- because a device that is
   *  not ours may still be needed exactly as it is by the host that does own it:
   *  - `shutdown.state` present: handed to resolveShutdownMarker, which decides.
   *  - otherwise `owning-host` names this host: loaded.
   *  - otherwise (another host, no file, malformed file): not loaded and never claimed. A disk
   *    pulled before its shutdown finished, or moved without one, must go back to its owner or
   *    be adopted through a shutdown. Claiming it here would steal stores from a host that may
   *    still be serving them from a copy or that crashed mid-transaction.
   *  The decision needs no network, which matters at startup: a host may need its own devices
   *  loaded before it can read any metadata.
   */
  private def tryLoadDevice(storageDevicePath: os.Path): Option[StorageDeviceId] =
    val sdCfgPath = storageDevicePath / StorageDeviceConfig.configFilename

    if os.isDir(storageDevicePath) && os.exists(sdCfgPath) then
      try
        val sdCfg = StorageDeviceConfig.loadStorageDeviceConfig(sdCfgPath)
        if sdCfg.aspenSystemId != aspenSystemId then
          warnOnce(storageDevicePath, s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
          None
        else
          DeviceMarkers.readShutdownState(storageDevicePath) match
            case Some(markerHost) =>
              resolveShutdownMarker(storageDevicePath, sdCfg, markerHost)
              None
            case None =>
              DeviceMarkers.readOwningHost(storageDevicePath) match
                case Some(owner) if owner == hostId => loadOwnedDevice(storageDevicePath, sdCfgPath, sdCfg)
                case owner =>
                  val who = owner.map(h => s"host $h").getOrElse(s"no host (${DeviceMarkers.OwningHostFile} is missing)")
                  warnOnce(storageDevicePath, s"Storage device ${sdCfg.storageDeviceId} at $storageDevicePath " +
                                              s"is owned by $who, not this host. Not loading or claiming it")
                  None
      catch
        case t: Throwable =>
          warnOnce(storageDevicePath, s"Failed to load storage device found at path $storageDevicePath. Error: $t")
          None
    else
      None

  /** The load half of tryLoadDevice, run only once the device is known to be ours. */
  private def loadOwnedDevice(storageDevicePath: os.Path,
                              sdCfgPath: os.Path,
                              sdCfg: StorageDeviceConfig): Option[StorageDeviceId] =
    storageDevices.get(sdCfg.storageDeviceId) match
      case Some(existing) =>
        // Already loaded. Usually this is just a rescan finding the same directory
        // again -- a directory that briefly disappeared, since loaded ones are not otherwise
        // read -- which must be a no-op so the device's state and open backends survive. A
        // different directory claiming the same id -- a duplicate mount or a copied config
        // file -- would otherwise replace the map entry and orphan the original's
        // already-open stores, so report it and keep the original.
        if existing.devicePath != storageDevicePath then
          warnOnce(storageDevicePath, s"Storage device ${sdCfg.storageDeviceId} is already loaded from " +
                                      s"${existing.devicePath}; ignoring duplicate at $storageDevicePath")
        else
          loadedDeviceDirs += storageDevicePath -> sdCfg.storageDeviceId
        None
      case None =>
        // Nothing can be receiving onto a device that is not loaded yet, so whatever is in
        // its staging directory was left by an attempt that crashed or was abandoned. A copy
        // that had finished arriving was already moved to its final path.
        val staging = storageDevicePath / StoreTransfer.StagingDirectory
        try os.remove.all(staging)
        catch case t: Throwable =>
          logger.warn(s"Failed to remove stale transfer staging directory $staging: $t")
        Try(os.list(storageDevicePath)) match
          case Failure(t) =>
            // Same hazard as the scan in checkForNewDevices: listing can fail with an IO
            // error even when isDir just succeeded. The catch in tryLoadDevice would absorb
            // the exception and leave the device unregistered and retryable just the same;
            // this guard exists to name the directory instead.
            warnOnce(storageDevicePath, s"Failed to list storage device directory $storageDevicePath. " +
                                        s"Will retry on the next scan. Error: $t")
            None
          case Success(storeFiles) =>
            val sds = new LocalStorageDeviceState(sdCfg.storageDeviceId, storageDevicePath, sdCfgPath)
            logger.info(s"Loading storage device $storageDevicePath. StorageDeviceId ${sds.storageDeviceId}")
            storeFiles.foreach: potentialStoreFile =>
              tryLoadStore(sds, potentialStoreFile)
            // Registered only after every child has been offered to tryLoadStore. If
            // anything above throws, the catch in tryLoadDevice logs it and the device stays
            // unregistered, so the next scan retries the whole device rather than
            // skipping it as already loaded. That retry re-offers children whose
            // LoadStore events are already queued; tryLoadStore's own catch absorbs the
            // resulting duplicate-backend failures.
            storageDevices += sdCfg.storageDeviceId -> sds
            loadedDeviceDirs += storageDevicePath -> sdCfg.storageDeviceId
            reportedDeviceDirs -= storageDevicePath
            Some(sdCfg.storageDeviceId)

  // protected so tests can subclass with a recording implementation instead of opening a
  // real RocksDB backend. `stores` and `storageDevices` are already protected.
  protected def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: os.Path): Unit =
    val storeCfgPath = potentialStoreFile / StoreConfig.configFilename
    if os.exists(storeCfgPath) then
      try
        val storeCfg = StoreConfig.loadStoreConfig(storeCfgPath)
        if os.exists(potentialStoreFile / StoreTransfer.TransferOutMarkerFile) then
          logger.info(s"Skipping load of offline store marked for transfer out. StoreId ${storeCfg.storeId}. $potentialStoreFile")
          offlineStores += storeCfg.storeId
          sds.offlineStores += storeCfg.storeId
        else if os.exists(potentialStoreFile / StoreTransfer.TransferredInMarkerFile) then
          // Arrived, but the pool may still name the source. startStoreTransferIn resumes the
          // metadata step for it, and reconcileDeviceState loads it if that step already committed.
          logger.info(s"Skipping load of transferred-in store awaiting its pool update. StoreId ${storeCfg.storeId}. $potentialStoreFile")
          offlineStores += storeCfg.storeId
          sds.offlineStores += storeCfg.storeId
        else
          val backend = storeCfg.backend match
            case b: StoreConfig.RocksDB => new RocksDBBackend(potentialStoreFile, storeCfg.storeId, ec)
          sds.loadedStores += backend.storeId
          sds.offlineStores -= backend.storeId
          offlineStores -= backend.storeId
          logger.info(s"Loading store ${storeCfg.storeId}: $potentialStoreFile")
          loadStore(sds.storageDeviceId, backend, storeCfg.effectiveMaxSliceSize.map(_.toLong))
      catch
        case t: Throwable => logger.warn(s"Failed to load store $potentialStoreFile. Error: $t")

  def start(): Unit =
    threadPool.submit(new Runnable {
      override def run(): Unit =
        while !shutdownCalled do
          var event = events.poll(3, TimeUnit.SECONDS)
          while (event != null)
            try
              handleEvent(event)
            catch
              case t: Throwable =>
                // Nothing consumes the Future this Runnable was submitted with, so without
                // this the escape would terminate the event loop with no log line at all and
                // the host would go silently deaf while still appearing to run.
                //
                // Swallowing and continuing is deliberate even though Aspen is crash-only: a
                // single poisoned event must not take down an otherwise-healthy host, and
                // handleEvent dispatches one event at a time with no loop state carried
                // between them.
                //
                // The trade is that a throw part-way through a handler leaves whatever that
                // handler had already mutated half-updated, and LoadStore, Repair and
                // ShutdownStore each carry a Promise completed by the handler or by work the
                // handler starts, so that promise stays uncompleted and its awaiter hangs
                // forever. Neither is new -- a dead loop left both too -- but a dead loop hung
                // everything at once and so was obvious, whereas now one caller hangs inside
                // a host that still looks healthy. This log line is the only signal that it
                // happened.
                //
                // Only the event class is interpolated: this call is inside the catch, so a
                // throwing toString on the event's payload would escape and kill the loop
                // anyway. The stack trace below names the handler.
                logger.error(s"Unhandled exception processing event ${event.getClass.getSimpleName}", t)
            event = events.poll(0, TimeUnit.SECONDS)
    })

  def getDevicePath(storageDeviceId: StorageDeviceId): Option[os.Path] = synchronized {
    storageDevices.get(storageDeviceId).map(_.devicePath)
  }

  /** Records the outcome of a store transfer that has finished shipping bytes.
   *
   *  Normally: the pool is repointed at the destination, the source drops the store, and the
   *  destination gains an Active entry.
   *
   *  If the destination was declared failed while the transfer was in flight, none of that is
   *  safe, and the failure is silent rather than loud. Its hostId reads as HostId(0,0), so the
   *  pool would be repointed at a host that does not exist. reconcileDeviceState ignores a failed
   *  device outright, so nothing on the destination would ever serve the store. And the only
   *  thing that could rebuild it -- FailedStorageDeviceDurableTask's drain -- has already dropped
   *  the entry, or will, precisely because a TransferringIn entry means the destination does not
   *  own the store. The slice would be lost with a pool entry that still looks healthy.
   *
   *  So take the repair instead: put the source's entry back to Active, leave the pool naming the
   *  source (it never stopped), and leave the tombstone's own store map alone -- that map is the
   *  drain's work list and its to remove.
   *
   *  The returned TransferOutcome is how the caller learns which of the two happened. It must not
   *  load the arrived copy on the restore path: nothing would ever reclaim it, since the
   *  deleted-stores pass looks only at offlineStores and reconcileDeviceState ignores a failed
   *  device outright.
   *
   *  protected so a test subclass can observe the returned Future; the production caller is
   *  startStoreTransferIn.
   */
  protected def updateStateForTransferredStore(storeId: StoreId,
                                               fromDeviceId: StorageDeviceId,
                                               toDeviceid: StorageDeviceId): Future[TransferOutcome] =
    // Only a collision is retried. Any other error -- an id with no entry in its tree, a pool
    // index the config does not have, generateBootstrapConfig's two requires, an object that
    // cannot be read -- is permanent, and retrying it would be a 60 s loop for the life of the
    // process that reports nothing at all.
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        fromDevPtr <- client.getStorageDevicePointer(fromDeviceId)
        toDevPtr <- client.getStorageDevicePointer(toDeviceid)
        poolKvos <- client.read(poolPtr)
        fromDevKvos <- client.read(fromDevPtr)
        toDevKvos <- client.read(toDevPtr)
        poolCfg = StoragePoolState(poolKvos)
        toDev = StorageDeviceState(toDevKvos)
        fromDev = StorageDeviceState(fromDevKvos)
        // Skipped entirely on the restore path. A tombstone's hostId is HostId(0,0), which has no
        // entry in the host tree, so prepRadicleUpdate's getHostState on it fails on every
        // attempt -- and the restore does not move the store between hosts, so there is nothing
        // for the bootstrap config to learn.
        //
        // On the normal path it is called before poolCfg.stores is mutated below, which is the
        // opposite of the order FailedStorageDeviceDurableTask.moveStore uses. Safe only because
        // prepRadicleUpdate filters its host list down to the hosts its store map references:
        // without that, the moved store's old host reaches generateBootstrapConfig with no stores
        // and trips its require(storesOnHost.nonEmpty).
        _ <-
          if toDev.isFailed then
            Future.unit
          else
            BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, toDev.hostId)(using tx)
      yield
        if toDev.isFailed then
          logger.warn(s"Transfer of store $storeId onto device $toDeviceid finished after that " +
                      s"device was declared failed. Restoring the store on source device " +
                      s"$fromDeviceId rather than repointing the pool at a dead device")

          // None when the drain's disown pass has already restored the entry, or when a
          // concurrent call to this method has. Writing nothing is then correct.
          fromDev.restoreAbandonedTransferSource(storeId, toDeviceid).foreach: restored =>
            tx.update(fromDevPtr, None, None,
              List(KeyRevision(StorageDeviceState.StateKey,
                fromDevKvos.contents(StorageDeviceState.StateKey).revision)),
              List(Insert(StorageDeviceState.StateKey, restored.encode())))

            // This method runs on the destination host, so the source is a different
            // host and learns nothing from the write above. Its copy is still offline
            // behind the transfer-out marker while the pool goes on naming it, which means the
            // slice answers nothing until that host's own poll reinstates it -- up to
            // Main.CheckStorageDevicesPeriod away. Best-effort, because that poll is the
            // guarantee and this is only the latency.
            tx.result.foreach: _ =>
              client.sendBestEffortHostMessage(
                CheckStorageDevice(fromDev.hostId, client.clientId, fromDeviceId))

          TransferOutcome.SourceRestored

        // Aborted (AspenClient.abortStoreTransfer) while the bytes were in flight, which puts the
        // source back to Active and leaves the pool naming it. Repointing the pool now would
        // complete a transfer its initiator has been told never happened.
        else if fromDev.stores.get(storeId).exists(entry =>
                  entry.status != StorageDeviceState.StoreStatus.TransferringOut ||
                  !entry.transferDevice.contains(toDeviceid)) then
          logger.info(s"Transfer of store $storeId from device $fromDeviceId onto device " +
                      s"$toDeviceid finished after it was aborted. Discarding the arrived copy")
          TransferOutcome.Aborted

        // If the from device doesn't contain the storeId, we're already done.
        // A concurrent call to this method must have succeeded
        else if fromDev.stores.contains(storeId) then
          poolCfg.stores(storeId.poolIndex) = StoragePoolState.StoreEntry(hostId, toDeviceid)
          val poolReqs = List(KeyRevision(StoragePoolState.ConfigKey, poolKvos.contents(StoragePoolState.ConfigKey).revision))
          val poolOps = List(Insert(StoragePoolState.ConfigKey, poolCfg.encode()))
          tx.update(poolPtr, None, None, poolReqs, poolOps)

          val newFromStores = fromDev.stores - storeId
          val newFromDev = fromDev.copy(stores = newFromStores)
          val fromDevReqs = List(KeyRevision(StorageDeviceState.StateKey, fromDevKvos.contents(StorageDeviceState.StateKey).revision))
          val fromDevOps = List(Insert(StorageDeviceState.StateKey, newFromDev.encode()))
          tx.update(fromDevPtr, None, None, fromDevReqs, fromDevOps)

          // Offline on a destination asked to shut down since the transfer began; see
          // serviceStatus. The pool is repointed all the same: the bytes are here.
          val newEntry = StorageDeviceState.StoreEntry(toDev.serviceStatus, None)
          val newToStores = toDev.stores + (storeId -> newEntry)
          val newtoDev = toDev.copy(stores = newToStores)
          val toDevReqs = List(KeyRevision(StorageDeviceState.StateKey, toDevKvos.contents(StorageDeviceState.StateKey).revision))
          val toDevOps = List(Insert(StorageDeviceState.StateKey, newtoDev.encode()))
          tx.update(toDevPtr, None, None, toDevReqs, toDevOps)

          // If state update transaction is successful, send a CheckStorageDevice
          // message to the host of the old storage device so they can delete
          // the store content
          tx.result.foreach: timestamp =>
            val msg = CheckStorageDevice(
              fromDev.hostId,
              client.clientId,
              fromDev.storageDeviceId
            )
            client.sendHostMessage(msg)

            // Best-effort wake-up for the rebalancing service (harmless for non-rebalance
            // transfers; the service also polls, so delivery is not required for correctness).
            client.sendServiceMessage(
              RebalancingDurableService.ServiceUUID,
              RebalancingMessage.encode(
                TransferComplete(toDev.storageDeviceSet, storeId, fromDeviceId, toDeviceid)))

            // Tell every other host (and client) where the store now lives. Sent only on this
            // branch: the other outcomes did not repoint the pool. This transaction wrote the pool
            // config, so its timestamp is the config value's. An Offline entry is announced
            // too, followed by its StoreOffline: the store does live here now, it just does not
            // answer until its device is back in service.
            gossipRetransmitter.broadcast(StoreMoved(storeId, hostId, timestamp, HLCTimestamp.now))
            if newEntry.status == StorageDeviceState.StoreStatus.Offline then
              gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))

          if newEntry.status == StorageDeviceState.StoreStatus.Offline then TransferOutcome.PoolRepointedOffline
          else TransferOutcome.PoolRepointed

        // The source has already dropped the store. Either a concurrent call to this method
        // repointed the pool at the destination, which owns the arrived copy either way, so this
        // is the same outcome reached by somebody else. Or the source was tombstoned and its
        // drain rebuilt the store elsewhere, in which case the pool names some other device and
        // loading the arrived copy would serve a store nobody routes to.
        else if poolCfg.stores(storeId.poolIndex).storageDeviceId == toDeviceid then
          // Offline when that call wrote it Offline, or the shutdown request has taken it Offline
          // since: either way the copy is ours but must not be served.
          if toDev.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Offline) then
            TransferOutcome.PoolRepointedOffline
          else
            TransferOutcome.PoolRepointed

        else
          logger.info(s"Transfer of store $storeId from device $fromDeviceId onto device " +
                      s"$toDeviceid finished after the source was drained and the pool repointed " +
                      s"to ${poolCfg.stores(storeId.poolIndex).storageDeviceId}. Discarding the " +
                      s"arrived copy")

          // The drain touches only the tombstone and the rebuild device, so this half of the
          // transfer is still recorded here. Guarded on the entry so a concurrent call, or an
          // entry that has since become something else, writes nothing.
          toDev.stores.get(storeId)
            .filter: entry =>
              entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
              entry.transferDevice.contains(fromDeviceId)
            .foreach: _ =>
              tx.update(toDevPtr, None, None,
                List(KeyRevision(StorageDeviceState.StateKey,
                  toDevKvos.contents(StorageDeviceState.StateKey).revision)),
                List(Insert(StorageDeviceState.StateKey, toDev.removeStore(storeId).encode())))

          TransferOutcome.Aborted

  /** Makes this host's HostState endpoint match the configured one, gossiping
   *  HostEndpointChanged once the change has committed.
   *
   *  Completes false, with no transaction, when the stored endpoint already matches. Completes
   *  true after a committed update. A failure is logged and retried every 5 seconds in the
   *  background; the returned Future reflects only the first attempt, so callers must not
   *  depend on it to start the host.
   */
  def reconcileEndpoint(address: String,
                        dataPort: Int,
                        httpPort: Int,
                        storeTransferPort: Int): Future[Boolean] =
    def attempt(): Future[Boolean] =
      readHostStateForEndpoint().flatMap: hs =>
        if hs.address == address && hs.dataPort == dataPort &&
           hs.httpPort == httpPort && hs.storeTransferPort == storeTransferPort then
          Future.successful(false)
        else
          client.transactUntilSuccessful: tx =>
            for
              hostPtr <- client.getHostPointer(hostId)
              kvos <- client.read(hostPtr)
            yield
              val current = HostState(kvos)
              tx.update(hostPtr, None, None,
                List(KeyRevision(HostState.StateKey, kvos.contents(HostState.StateKey).revision)),
                List(Insert(HostState.StateKey,
                  current.copy(address = address, dataPort = dataPort, httpPort = httpPort,
                               storeTransferPort = storeTransferPort).encode())))
              tx
          // The committed transaction's timestamp is the new HostState value's
          .flatMap(_.result)
          .map: timestamp =>
            logger.info(s"Host endpoint updated to $address:$dataPort")
            gossipRetransmitter.broadcast(
              HostEndpointChanged(hostId, address, dataPort, httpPort, storeTransferPort, timestamp))
            true

    // Each call is one attempt; a failed one logs and schedules another. The result is the
    // attempt it was called for, so the first call's Future reflects only the first attempt.
    def run(): Future[Boolean] =
      val result = attempt()
      result.failed.foreach: err =>
        logger.warn(s"Failed to reconcile host endpoint, retrying in 5 seconds: $err")
        scheduleEndpointRetry(run())
      result

    run()

  /** Test seam: the read that decides whether the endpoint needs updating. */
  protected def readHostStateForEndpoint(): Future[HostState] = client.getHostState(hostId)

  /** Test seam: runs a failed reconcileEndpoint attempt's retry after the retry delay. */
  protected def scheduleEndpointRetry(fn: => Unit): Unit =
    backgroundTasks.schedule(Duration(5, SECONDS))(fn)

  /** Reclaims the store copy a transfer just delivered onto a device that turned out to be
   *  tombstoned.
   *
   *  Only reached with TransferOutcome.SourceRestored, which means the pool still names the
   *  source and the source still holds the data -- so there is nothing here worth keeping. Its
   *  transferred-in marker keeps tryLoadStore from loading it, but nothing else would ever
   *  reclaim it, since reconcileDeviceState ignores a failed device outright. Were it loaded, and
   *  the source happened to live on this same host, the load would replace the source's entry in
   *  `stores`, which is keyed by StoreId alone.
   *
   *  Nothing is unloaded because nothing was loaded: the load is downstream of the decision.
   */
  private def discardStoreDirectory(storeId: StoreId,
                                    deviceId: StorageDeviceId,
                                    reason: String): Unit = synchronized {
    storageDevices.get(deviceId) match
      case None =>
        // The operation completed on a device this process never loaded, or one a shutdown has
        // since unloaded. Log rather than assume.
        logger.warn(s"Cannot discard the copy of store $storeId on unloaded device $deviceId")

      case Some(sds) =>
        val storePath = sds.devicePath / storeId.directoryName
        if os.exists(storePath) then
          logger.warn(s"Discarding the copy of store $storeId on device $deviceId. " +
                      s"$reason: $storePath")
          try
            os.remove.all(storePath)
          catch
            case t: Throwable =>
              // The store is not loaded, so nothing serves it now. The cost of the leak is a
              // directory on a dead device that a restart would load and then never route to.
              logger.error(s"Failed to delete the discarded store copy $storePath. Error: $t")
  }

  private def discardArrivedTransferIn(storeId: StoreId,
                                       toDeviceId: StorageDeviceId): Unit =
    discardStoreDirectory(storeId, toDeviceId,
      "The pool does not name this device, so this copy belongs to nobody")

  /** Restarts a failed transfer in after TransferInRetryDelay rather than at once. A deterministic
   *  failure (the sender's archiver exiting non-zero, a full disk here) would otherwise loop hot,
   *  re-shipping the store each time, since every restart supersedes the sender's attempt at once.
   *
   *  The store stays in transferringIn until the retry fires, so device checks in the meantime
   *  do not start a parallel attempt. `transferUUID` is the failed attempt's: the retry does
   *  nothing if that attempt is no longer the store's current one.
   *
   *  The destination's entry is re-read before restarting, since the transfer may have been
   *  abandoned during the wait. The retry stays scheduled until that read returns, so an abandon
   *  committing meanwhile still finds it in transferInRetries and cancels it.
   */
  private def scheduleTransferInRetry(storeId: StoreId,
                                      transferUUID: UUID,
                                      fromHostId: HostId,
                                      fromDeviceId: StorageDeviceId,
                                      toDeviceid: StorageDeviceId,
                                      transferId: Option[UUID]): Unit = synchronized {
    def current: Boolean =
      transferInRetries.contains(storeId) && transferringIn.get(storeId).contains(transferUUID)

    def restart(entryState: scala.util.Try[StorageDeviceState]): Unit = synchronized {
      if current then
        transferInRetries -= storeId
        transferringIn -= storeId
        val stillWanted = entryState match
          case Success(state) =>
            state.stores.get(storeId).exists: entry =>
              entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
              entry.transferDevice.contains(fromDeviceId) &&
              entry.transferId == transferId
          case Failure(t) =>
            // No worse than restarting blind: an abandoned attempt's restart is cancelled again
            // once the abandon commits.
            logger.warn(s"Failed to re-read device $toDeviceid before retrying the transfer in of " +
                        s"store $storeId. Retrying anyway. Error: $t")
            true
        if stillWanted then
          startStoreTransferIn(storeId, fromHostId, fromDeviceId, toDeviceid, transferId)
        else
          logger.info(s"Not retrying the transfer in of store $storeId: device $toDeviceid no " +
                      s"longer records it as transferring in from device $fromDeviceId")
    }

    def retry(): Unit = synchronized {
      if current then
        val fstate =
          try client.getStorageDeviceState(toDeviceid)
          catch case t: Throwable => Future.failed(t)
        fstate.onComplete(restart)
    }

    logger.info(s"Retrying the transfer in of store $storeId in $TransferInRetryDelay")
    transferInRetries += storeId -> new TransferInRetry(fromDeviceId, toDeviceid, () => retry())
    backgroundTasks.schedule(TransferInRetryDelay)(retry())
  }

  /** Forgets this host's attempt to receive `storeId` from `fromDeviceId`, whose transfer has been
   *  abandoned. Covers an attempt still waiting on bytes and one waiting out its retry delay; an
   *  attempt already past promotion is left to recordTransferIn, which drops its own entry.
   *
   *  The service's registration is dropped too, failing its future. The completion handler in
   *  startStoreTransferIn sees the attempt is no longer current and neither retries nor promotes.
   */
  private def dropAbandonedTransferIn(storeId: StoreId,
                                      fromDeviceId: StorageDeviceId,
                                      toDeviceId: StorageDeviceId): Unit =
    val cancelled = synchronized {
      val pending = pendingStartTransfers.get(storeId).filter(_.msg.fromDevice == fromDeviceId)
      val retry = transferInRetries.get(storeId).filter(_.fromDeviceId == fromDeviceId)
      transferringIn.get(storeId) match
        case Some(transferUUID) if pending.nonEmpty || retry.nonEmpty =>
          logger.info(s"Dropping the abandoned transfer in of store $storeId from device $fromDeviceId")
          pendingStartTransfers -= storeId
          transferInRetries -= storeId
          transferringIn -= storeId
          // A retry waiting on a failed promotion still has the extracted copy in staging, which
          // the service no longer owns. Nothing else can be receiving this store now.
          if retry.nonEmpty then
            storageDevices.get(toDeviceId).foreach: sds =>
              val staged = sds.devicePath / StoreTransfer.StagingDirectory / storeId.directoryName
              try os.remove.all(staged)
              catch case t: Throwable =>
                logger.error(s"Failed to remove the staged copy of store $storeId at $staged: $t")
          Some(transferUUID)
        case _ => None
    }
    cancelled.foreach(storeTransferService.cancelTransferIn(storeId, _))

  /** Removes the transferred-in marker from a promoted store, returning whether the store may now
   *  be loaded. A store loaded with the marker still in place would be skipped by the next
   *  restart's scan, so on failure it is left unloaded for reconcileDeviceState's
   *  transferred-in pass to try again.
   */
  private def clearTransferredInMarker(storeId: StoreId, storePath: os.Path): Boolean =
    try
      os.remove(storePath / StoreTransfer.TransferredInMarkerFile)
      true
    catch
      case t: Throwable =>
        logger.error(s"Failed to remove the transferred-in marker for store $storeId at " +
                     s"$storePath. The store stays offline. Error: $t")
        false

  /** Runs the metadata step for a transferred store that has been promoted to its final path,
   *  then loads or discards the arrived copy according to the outcome. Reached both straight
   *  after promotion and, via startStoreTransferIn, for a copy promoted before a crash.
   *
   *  `transferUUID` is the attempt's entry in transferringIn, which is dropped only once nothing
   *  more can happen to the arrived copy. Dropped any earlier, a device check could start a
   *  second attempt that re-ships the whole store, or resume this step a second time in parallel.
   */
  private def recordTransferIn(storeId: StoreId,
                               transferUUID: UUID,
                               fromDeviceId: StorageDeviceId,
                               toDeviceid: StorageDeviceId): Unit =
    def finished(): Unit = synchronized {
      if transferringIn.get(storeId).contains(transferUUID) then
        transferringIn -= storeId
    }

    // Loading is downstream of the metadata decision, not ahead of it. An unconditional load
    // hands a live copy of the store to a device that updateStateForTransferredStore may be about
    // to decide must not own it, and nothing would ever take it back.
    //
    // Waiting costs no availability: the pool goes on naming the source until that transaction
    // commits, so nothing routes here before then anyway.
    val foutcome =
      try updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid)
      catch case t: Throwable => Future.failed(t)
    foutcome.onComplete: outcome =>
      try
        outcome match
          case Success(TransferOutcome.PoolRepointed) =>
            synchronized {
              storageDevices.get(toDeviceid).foreach: sds =>
                if clearTransferredInMarker(storeId, sds.devicePath / storeId.directoryName) then
                  loadStoreById(toDeviceid, storeId)
                else
                  // Recorded as a restart's scan would have, which is what the transferred-in
                  // pass keys off.
                  offlineStores += storeId
                  sds.offlineStores += storeId
            }

          case Success(TransferOutcome.PoolRepointedOffline) =>
            // Left exactly as it arrived, behind its transferred-in marker, and recorded offline
            // as a restart's scan would record it. Once a restart or adoption makes the entry
            // Active, the reconcile's completed-transfers-in pass clears the marker and loads it.
            // Unlike the shutdown's own markers this needs no disk write, so it is the same
            // whether or not the device is still loaded.
            synchronized {
              offlineStores += storeId
              storageDevices.get(toDeviceid).foreach: sds =>
                sds.offlineStores += storeId
            }
            logger.info(s"Store $storeId has transferred onto device $toDeviceid, which is being " +
                        s"shut down. It stays offline")

          case Success(TransferOutcome.SourceRestored) | Success(TransferOutcome.Aborted) =>
            discardArrivedTransferIn(storeId, toDeviceid)

          case Failure(t) =>
            // The only observer this Future has. Its onFail routes the permanent errors to
            // StopRetrying, which fails the promise -- and an unobserved failed Promise is not
            // reported by the ExecutionContext, so without this line a permanent give-up is
            // exactly as silent as the infinite retry loop it replaced. The arrived copy keeps its
            // transferred-in marker and the destination's entry stays TransferringIn, so the next
            // device check resumes this step; until one succeeds the source's copy stays offline
            // behind its own marker.
            logger.error(s"Failed to record the completed transfer of store $storeId " +
                         s"from device $fromDeviceId to device $toDeviceid. The store is " +
                         s"not loaded here and the pool still names the source", t)
      finally
        finished()

  private def startStoreTransferIn(storeId: StoreId,
                                   fromHostId: HostId,
                                   fromDeviceId: StorageDeviceId,
                                   toDeviceid: StorageDeviceId,
                                   transferId: Option[UUID]): Unit = synchronized {
    require(fromDeviceId != toDeviceid)

    // The shutdown guard covers a source read issued before this device's shutdown began, and a
    // retry whose delay ran out during it: the shutdown has already cancelled this device's
    // transfers in and would not see a new one.
    if ! transferringIn.contains(storeId) && ! shuttingDownDevices.contains(toDeviceid) then
      storageDevices.get(toDeviceid).foreach: toDevice =>
        val devicePath = toDevice.devicePath
        val finalPath = devicePath / storeId.directoryName

        // A promoted copy belongs to the transfer whose id its marker records. One left by an
        // earlier transfer that was aborted or given up on predates whatever the source has
        // accepted since, so resuming it would install stale data. Discard it and start over.
        val promotedMarker = finalPath / StoreTransfer.TransferredInMarkerFile
        if os.exists(promotedMarker) then
          val markerId = try os.read(promotedMarker).trim catch case _: Throwable => ""
          if !transferId.exists(_.toString == markerId) then
            logger.warn(s"Discarding the arrived copy of store $storeId on device $toDeviceid: it " +
                        s"belongs to transfer '$markerId', not to the current transfer $transferId")
            try
              os.remove.all(finalPath)
              offlineStores -= storeId
              toDevice.offlineStores -= storeId
            catch case t: Throwable =>
              logger.error(s"Failed to remove the stale arrived copy of store $storeId at $finalPath: $t")

        // A copy promoted by an earlier attempt -- normally one that crashed before its metadata
        // step finished -- needs no bytes, only that step.
        if os.exists(finalPath / StoreTransfer.TransferredInMarkerFile) then
          logger.info(s"Store $storeId has already arrived on device $toDeviceid. Resuming its " +
                      s"pool update")
          val transferUUID = UUID.randomUUID()
          transferringIn += storeId -> transferUUID
          recordTransferIn(storeId, transferUUID, fromDeviceId, toDeviceid)

        // Promotion moves the arrived copy here and would fail on an existing directory, after
        // the whole store had been shipped. Whatever is here is not ours to delete, so refuse
        // before asking for any bytes.
        else if os.exists(finalPath) then
          logger.error(s"Cannot transfer store $storeId onto device $toDeviceid: store path " +
                       s"already exists. $finalPath")
        else
          // Fresh per attempt. The sender ignores a request whose timestamp is not newer than the
          // last one it acted on, and the service accepts a connection only for the UUID it is
          // currently registered with, so a retry after a failure needs both to be new.
          val transferUUID = UUID.randomUUID()
          val timestamp = HLCTimestamp.now

          val fcomplete = storeTransferService.beginTransferIn(
            storeId, transferUUID, devicePath / StoreTransfer.StagingDirectory)

          transferringIn += storeId -> transferUUID

          // Retransmission stops on completion, success or failure, not on first data: the
          // sender needs the request until it has finished, and a failure restarts with a new
          // message below.
          def stopRetransmitting(): Unit = synchronized {
            if pendingStartTransfers.get(storeId).exists(_.msg.transferUUID == transferUUID) then
              pendingStartTransfers -= storeId
          }

          // False once the attempt has been dropped by dropAbandonedTransferIn, whose cancellation
          // is what completed the future, or by a supersession.
          def isCurrent: Boolean = synchronized(transferringIn.get(storeId).contains(transferUUID))

          fcomplete.onComplete:
            case Success(_) if ! isCurrent =>
              // The bytes landed as the transfer was abandoned. Nobody owns the staged copy now,
              // and nothing can be receiving this store, since a new attempt needs the
              // transferringIn entry that dropping this one removed.
              synchronized {
                if ! transferringIn.contains(storeId) then
                  val stagedPath = devicePath / StoreTransfer.StagingDirectory / storeId.directoryName
                  try os.remove.all(stagedPath)
                  catch case t: Throwable =>
                    logger.error(s"Failed to remove the staged copy of store $storeId at $stagedPath: $t")
              }

            case Failure(_) if ! isCurrent =>
              stopRetransmitting()

            case Success(_) =>
              stopRetransmitting()

              // The transfer-out marker was archived along with the source's store directory and
              // is swapped for the transferred-in marker before the move, so the copy reaches
              // finalPath already marked. Unmarked, a crash before the metadata step finishes
              // would have the restart load a copy the pool does not name, and the device check
              // would then refuse the existing directory instead of resuming that step.
              val stagedPath = devicePath / StoreTransfer.StagingDirectory / storeId.directoryName
              val promoted =
                try
                  val marker = stagedPath / StoreTransfer.TransferOutMarkerFile
                  if os.exists(marker) then
                    os.remove(marker)
                  os.write.over(stagedPath / StoreTransfer.TransferredInMarkerFile,
                                transferId.map(_.toString).getOrElse(""))
                  os.move(stagedPath, finalPath)
                  true
                catch
                  case t: Throwable =>
                    logger.error(s"Failed to move the transferred store $storeId from " +
                                 s"$stagedPath to $finalPath. Restarting the transfer. Error: $t")
                    false

              if ! promoted then
                // Nothing has been recorded yet, so the pool still names the source and this is
                // no different from a transfer that failed in flight. The restart's extraction
                // replaces the staged copy, and if the failure left a directory at finalPath the
                // restart refuses with an error rather than retrying forever.
                scheduleTransferInRetry(storeId, transferUUID, fromHostId, fromDeviceId, toDeviceid, transferId)
              else
                recordTransferIn(storeId, transferUUID, fromDeviceId, toDeviceid)
            case Failure(_) =>
              stopRetransmitting()
              scheduleTransferInRetry(storeId, transferUUID, fromHostId, fromDeviceId, toDeviceid, transferId)

          val msg = StartStoreTransfer(
            fromHostId,
            client.clientId,
            fromDeviceId,
            storeId,
            timestamp,
            transferUUID
          )

          pendingStartTransfers += storeId -> new PendingTransfer(msg, toDeviceid, HLCTimestamp.now)

          client.sendHostMessage(msg)
  }

  /** Returns the request's handling, which fails when the request is refused. Only tests look at
   *  it (testingOnlyStartStoreTransferOut); the event loop drops it, as a refusal is answered by
   *  the receiver's retransmit. */
  private def startStoreTransferOut(m: StartStoreTransfer): Future[Unit] = synchronized {
    def err(msg: String): Nothing =
      logger.info(msg)
      throw new Exception(msg)

    // Any failure here, including a destination HostState that cannot be read, drops this
    // request; the receiver's retransmit is what tries again. Retransmits and superseded
    // requests are not filtered here either: storeTransferService ignores a request whose
    // timestamp is not newer than the last one it acted on for the store, and a newer one
    // replaces the send in progress.
    //
    // This path reads pool and device state fresh and closes the store itself, so a request
    // that arrives before this host's own device check has noticed the transfer needs no
    // separate scan.
    for
      // The pool state must be read fresh: a StoragePool object holds no store-to-device mapping,
      // and a snapshot would show where the stores were when it was taken. The source device must be read fresh -- for a store that has already
      // moved once (a migration retarget, or two successive rebalances) the snapshot names the
      // device it used to live on, that device no longer lists the store, and the transfer-out
      // dies below on "Transfer probably completed" without ever starting.
      pstate <- client.getStoragePoolState(m.storeId.poolId)
      poolEntry = pstate.stores(m.storeId.poolIndex)
      fromDevice <- client.getStorageDeviceState(poolEntry.storageDeviceId)
      devEntry = fromDevice.stores.get(m.storeId) match
        case None => err(s"Store ${m.storeId} missing from device. Transfer probably completed")
        case Some(e) => e
      toDeviceId = devEntry.transferDevice match
        case None => err(s"Store ${m.storeId} not in transfer state. Transfer probably completed")
        case Some(sid) => sid
      toDevice <- client.getStorageDeviceState(toDeviceId)
      // The destination was declared failed while the transfer was in flight, so both its ids
      // read as zero. There is no fresher source for them -- unlike the source device's id
      // below, which the pool entry carries -- so the only safe thing is to refuse. Shipping
      // bytes to HostId(0,0) would run indefinitely against a host that does not exist.
      _ = if toDevice.isFailed then
            err(s"Destination device $toDeviceId for transfer of store ${m.storeId} has been " +
                s"declared failed")
      // A source being shut down has had, or is about to have, its stores closed for the shelf,
      // and its transfers out cancelled. Starting one would reopen work the shutdown waits on.
      // The receiver's retransmit keeps asking, so the transfer resumes after a restart, or from
      // an adopting host once the device moves. Failed is not refused here: its stores are
      // drained by FailedStorageDeviceDurableTask, which this path predates.
      _ = if fromDevice.status == StorageDeviceState.DeviceStatus.ShuttingDown ||
             fromDevice.status == StorageDeviceState.DeviceStatus.Shutdown then
            err(s"Source device ${poolEntry.storageDeviceId} for transfer of store ${m.storeId} is " +
                s"${fromDevice.status}")
      // Keyed off the pool entry, not fromDevice.storageDeviceId: those are the same value in
      // the healthy case, but a tombstoned source zeroes its own copy, and the lookup would
      // then miss and blame a removed disk for a device that is loaded and present.
      sourceDs = storageDevices.get(poolEntry.storageDeviceId) match
        case None => err(s"Source storage device for transfer ${poolEntry.storageDeviceId} not loaded. Disk removed?")
        case Some(sds) => sds
      // Resolved fresh on every request, so a destination that has moved since the last
      // attempt is reached at its current address.
      toHostState <- client.getHostState(toDevice.hostId).recover:
        case t =>
          logger.warn(s"Failed to read the HostState of host ${toDevice.hostId}, destination of " +
                      s"transfer ${m.transferUUID} of store ${m.storeId}. Error: $t")
          throw t
    yield
      synchronized {
        // The reads above may predate the shutdown request, so the device's own guard is checked
        // as well: the shutdown has started and its step 1 may already be past this store.
        if shuttingDownDevices.contains(sourceDs.storageDeviceId) then
          err(s"Source device ${sourceDs.storageDeviceId} for transfer of store ${m.storeId} is " +
              s"being shut down")
        if devEntry.status == StorageDeviceState.StoreStatus.TransferringOut then
          // A retransmitted request finds the store already closed, and its going offline already gossiped
          val wasLoaded = stores.contains(m.storeId)
          val fclosed = stores.get(m.storeId) match
            case None => Future.unit
            case Some(_) => closeStore(m.storeId)

          fclosed.foreach: _ =>
            synchronized {
              val storePath = sourceDs.devicePath / m.storeId.directoryName
              try
                // Written before archiving, so it travels with the store and keeps this copy
                // offline across restarts until the transfer is resolved one way or the other.
                os.write.over(storePath / StoreTransfer.TransferOutMarkerFile,
                              devEntry.transferId.map(_.toString).getOrElse(""))
                storeTransferService.beginTransferOut(m.storeId, m.transferUUID, storePath,
                  toHostState.address, toHostState.storeTransferPort, m.timestamp)
                // The transfer may have been aborted after the reads above, and the abort's
                // nudge handled before the marker above existed. No check would then ever see
                // the marker beside the Active entry, leaving the store dark for a full
                // checkStorageDevicePeriod while the tree counts it online.
                scheduleDeviceRecheck(sourceDs.storageDeviceId)
                if wasLoaded then
                  gossipRetransmitter.broadcast(StoreOffline(m.storeId, HLCTimestamp.now))
              catch
                case t: Throwable =>
                  logger.warn(s"Failed to start transfer out of store ${m.storeId}, transfer " +
                              s"${m.transferUUID}. Error: $t")
            }
      }
  }

  /** Creates and loads an Initializing store, then flips its entry out of Initializing.
   *
   *  The flip writes serviceStatus: Active normally, Offline when the device has been asked to
   *  shut down since the create began. The store is loaded either way, and recorded in
   *  loadedStores, so the shutdown closes and marks it like the device's other open stores.
   *
   *  A create that has not started its local work by the time this device's shutdown begins (or
   *  its unload) does none: nothing is created or loaded, and the entry stays Initializing for a
   *  later check once the device is back in service. Writing Offline over a store that does not
   *  exist would have a restart bring an empty slot into service. The decision and the
   *  loadedStores record share one hold of the lock, so a shutdown either sees the store or the
   *  create sees the shutdown.
   *
   *  Only a collision is retried. Any other failure -- the pool cannot be read, the directory
   *  cannot be written -- is logged and releases creatingStores, so the next check tries again
   *  rather than a silent retry loop running for the life of the process.
   */
  private def createNewStore(local: LocalStorageDeviceState, storeId: StoreId): Unit = synchronized {
    val storePath = local.devicePath / storeId.directoryName
    val deviceId = local.storageDeviceId

    if ! creatingStores.contains(storeId) && ! shuttingDownDevices.contains(deviceId) then
      creatingStores += storeId

      // True when the store exists on disk (created here, or by an attempt that crashed before
      // its flip), false when the create was skipped for a shutdown.
      val fcreate: Future[Boolean] = if os.exists(storePath) then
        Future.successful(true)
      else
        client.getStoragePoolState(storeId.poolId).flatMap: pstate =>
          synchronized {
            if shuttingDownDevices.contains(deviceId) || ! storageDevices.get(deviceId).contains(local) then
              logger.info(s"Not creating store $storeId: device $deviceId is being shut down")
              Future.successful(false)
            else
              val storeConfig = pstate.backendConfig match
                case cfg: RocksDBConfig => StoreConfig(storeId, StoreConfig.RocksDB(cfg.maxSliceSize), pstate.maxSliceSize)

              os.makeDir.all(storePath)
              os.write.over(storePath / StoreConfig.configFilename, storeConfig.yamlConfig)

              val backend = storeConfig.backend match
                case _: StoreConfig.RocksDB => new RocksDBBackend(storePath, storeId, ec)

              // Recorded as tryLoadStore records a load, so a shutdown of this device finds the
              // store and closes it.
              local.loadedStores += storeId
              loadStore(deviceId, backend, storeConfig.effectiveMaxSliceSize.map(_.toLong)).map(_ => true)
          }

      val fflip = client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
        for
          created <- fcreate
          ptr <- client.getStorageDevicePointer(deviceId)
          kvos <- client.read(ptr)
          state = StorageDeviceState(kvos)
        yield
          state.stores.get(storeId).foreach: entry =>
            if created && entry.status == StorageDeviceState.StoreStatus.Initializing then
              val status = state.serviceStatus
              val newState = state.setStoreEntry(storeId, status, None)

              val reqs = List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision))
              val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

              logger.info(s"Updating device state to mark store $storeId as $status")
              tx.update(ptr, None, None, reqs, ops)

              tx.result.foreach: _ =>
                logger.info(s"Successfully updated device state to mark store $storeId as $status")

      fflip.onComplete: result =>
        result.failed.foreach: t =>
          logger.error(s"Failed to create store $storeId on device $deviceId. The next check of the " +
                       s"device will try again. Error: $t")
        synchronized:
          creatingStores -= storeId
  }

  /** Begins reconstruction of one store from the rest of its pool.
   *
   *  Runs under the instance lock, so it does no more than construct the rebuild and register
   *  its continuation; the walk itself runs on the client's execution context.
   *
   *  On success the walk places the rebuilt store at `<device>/<storeId.directoryName>`. Loading
   *  is downstream of the metadata decision: markRebuiltStoreActive decides whether the device is
   *  allowed to adopt the copy (reading the tree state inside a transaction to guard against
   *  concurrent tombstone), then the completion handler loads if adopted or discards if the device
   *  was failed. The slot is released only after the flip commits, so a concurrent check cannot
   *  restart the same store between the release and the commit.
   *
   *  Every completion re-checks the host's other loaded devices, because
   *  `rebuildingStores` is host-wide: a slot released by device A's rebuild could unblock a queued
   *  rebuild on device B, and without a re-check of B it would wait for the periodic sweep (one
   *  hour) with data under-replicated. The three outcomes that settle the store's metadata
   *  (`Adopt`, `Discard`, and a permanently failed flip) additionally re-check this device, to
   *  start the next store queued behind the slot just freed. The walk-failure path does not: see
   *  its comment.
   */
  private def startStoreRebuild(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    synchronized {
      if !rebuildingStores.contains(storeId) && rebuildingStores.size < maxConcurrentRebuilds &&
         !shuttingDownDevices.contains(local.storageDeviceId) then
        val sr = storeRebuildFactory.createRebuild(
          client, storeId, local.storageDeviceId, local.devicePath)

        def release(): Unit = synchronized:
          rebuildingStores -= storeId

        def recheckAll(): Unit =
          synchronized(storageDevices.keys).foreach(checkStorageDevice)

        /** Every loaded device except the one this rebuild ran on.
         *
         *  For the failure path only. The freed slot is host-wide, so the other devices are still
         *  worth re-checking, but re-checking this one restarts the rebuild that just failed:
         *  `release()` has already dropped the store from `rebuildingStores`, the tree entry still
         *  reads `Rebuilding`, and reconcileDeviceState's Rebuilding pass therefore starts it
         *  again. The single-flight guard does not intervene -- this callback fires long after
         *  startDeviceCheck returned, so `activeDeviceChecks` is empty.
         *
         *  The cost, accepted: on a single-device host the filter leaves nothing to re-check, so
         *  a sibling store queued behind the freed slot waits for the periodic sweep rather than
         *  starting at once. Delaying an innocent sibling by one sweep is the cheaper error than
         *  spinning on the failed store with no backoff.
         */
        def recheckOtherDevices(): Unit =
          synchronized(storageDevices.keys)
            .filter(_ != local.storageDeviceId)
            .foreach(checkStorageDevice)

        // Guard-map leak defense: invoke `complete` in a try/catch that substitutes Future.failed(t)
        // on throw, so that either way a Future exists and onComplete below is registered. That
        // guarantees the Failure branch's release() runs. The map entry is added unconditionally
        // after the catch, so reordering alone does not close the leak — what matters is that a
        // throwing complete cannot leave the callback unregistered.
        val completeFuture = try
          sr.complete
        catch
          case t: Throwable =>
            logger.error(s"Rebuild factory's complete threw for store $storeId: $t")
            Future.failed(t)

        synchronized:
          rebuildingStores += storeId -> sr

        // The pool already names this device, so peers would otherwise pick a store that cannot
        // answer until the rebuild completes
        gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))

        completeFuture.onComplete:
          case Success(_) =>
            // Loading is downstream of the metadata decision, not ahead of it. An unconditional
            // load hands a live copy to a device that markRebuiltStoreActive may be about to
            // decide must not own it (because the device was tombstoned mid-rebuild), and nothing
            // would ever take it back.
            markRebuiltStoreActive(local.storageDeviceId, storeId).onComplete:
              case Success(RebuildOutcome.Adopt) =>
                try
                  loadStoreById(local.storageDeviceId, storeId)
                finally
                  release()
                  recheckAll()

              case Success(RebuildOutcome.AdoptOffline) =>
                try
                  keepRebuiltStoreOffline(local, storeId)
                finally
                  release()
                  recheckAll()

              case Success(RebuildOutcome.Discard) =>
                try
                  discardStoreDirectory(storeId, local.storageDeviceId,
                    "The tree does not say this device owns a rebuilt store")
                finally
                  release()
                  recheckAll()

              case Failure(t) =>
                // The only observer this Future has. Its onFail routes the permanent errors to
                // StopRetrying, which fails the promise -- and an unobserved failed Promise is
                // not reported by the ExecutionContext, so without this line a permanent give-up
                // is exactly as silent as the infinite retry loop it replaced. What is left
                // behind matters: the store is on disk, the tree still says Rebuilding.
                try
                  logger.error(s"Failed to record the completed rebuild of store $storeId on " +
                               s"device ${local.storageDeviceId}. The store is on disk in " +
                               s"${local.devicePath / storeId.directoryName} but is not " +
                               s"loaded, and the device entry still says Rebuilding", t)
                finally
                  release()
                  recheckAll()

          case Failure(RebuildingStore.RebuildCancelled(_)) =>
            // Cancelled by a shutdown of this device. Not an error, and this device is not
            // re-checked: it is shutting down. The checkpoint stays in staging, so the rebuild
            // resumes once the device is back in service.
            try
              logger.info(s"Rebuild of store $storeId cancelled")
            finally
              release()
              recheckOtherDevices()

          case Failure(err) =>
            // The checkpoint survives in the staging directory, so the next check of this device
            // resumes rather than restarting. That next check is deliberately left to the
            // ordinary periodic sweep: this device is excluded from the re-check here because
            // including it is a retry, and an unbounded one. A rebuild that fails in well under a
            // millisecond -- an unreadable checkpoint, a pool that cannot be read -- would spin
            // through tree reads and WARN lines forever, with no backoff and no give-up. The
            // other devices are re-checked because the slot just freed is host-wide and may be
            // what a queued rebuild elsewhere is waiting on.
            try
              logger.warn(s"Rebuild of store $storeId failed: $err")
            finally
              release()
              recheckOtherDevices()
    }

  /** Decides whether a rebuilt store should be adopted or discarded.
   *
   *  The device may have been tombstoned or removed from the tree between a rebuild starting and
   *  finishing. Reading the state inside the transaction and checking isFailed before the status
   *  check ensures the decision is made from current metadata, not stale assumptions.
   *
   *  Only a collision is retried. Retrying a permanent error -- getStorageDevicePointer's
   *  NoSuchElementException, an unreadable device object -- would never complete the future, and
   *  this device's rebuild queue would never advance.
   */
  protected def markRebuiltStoreActive(storageDeviceId: StorageDeviceId,
                                       storeId: StoreId): Future[RebuildOutcome] =
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        ptr <- client.getStorageDevicePointer(storageDeviceId)
        kvos <- client.read(ptr)
        state = StorageDeviceState(kvos)
      yield
        // Check isFailed first. A concurrent tombstone leaves stores intact but zeroes the ids,
        // so the entry still reads Rebuilding. Loading hands a live copy to a failed device, and
        // reconcileDeviceState bails on isFailed so nothing ever takes it back. Meanwhile the
        // drain is moving the store elsewhere, so two hosts serve a live copy of the same StoreId.
        if state.isFailed then
          RebuildOutcome.Discard
        else
          state.stores.get(storeId) match
            case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Rebuilding =>
              // Offline on a device asked to shut down since the rebuild began; see serviceStatus.
              val status = state.serviceStatus
              val newState = state.setStoreEntry(storeId, status, None)

              val reqs = List(KeyRevision(StorageDeviceState.StateKey,
                kvos.contents(StorageDeviceState.StateKey).revision))
              val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

              logger.info(s"Updating device state to mark rebuilt store $storeId as $status")
              tx.update(ptr, None, None, reqs, ops)

              if status == StorageDeviceState.StoreStatus.Active then
                tx.result.foreach: _ =>
                  logger.info(s"Rebuild of store $storeId complete")
                  gossipStoreOnline(storeId)
                RebuildOutcome.Adopt
              else
                tx.result.foreach: _ =>
                  logger.info(s"Rebuild of store $storeId complete. Its device is shutting down, " +
                              s"so it stays offline")
                RebuildOutcome.AdoptOffline

            case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Active =>
              // Already flipped by a concurrent rebuild completion. No update staged.
              RebuildOutcome.Adopt

            case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Offline =>
              // Already flipped by a concurrent completion, onto a device shutting down -- or
              // flipped Active and then taken Offline by the shutdown request. Either way the
              // copy is this device's and must stay out of service. No update staged.
              RebuildOutcome.AdoptOffline

            case _ =>
              // Entry missing entirely, or has a status other than Rebuilding/Active (Initializing,
              // TransferringIn, TransferringOut). In all these cases the tree does not say this
              // device owns a rebuilt store. Latent rather than live: no current in-tree writer
              // produces "healthy device, entry vanished", but it's the wrong default.
              RebuildOutcome.Discard

  /** Keeps a rebuilt store whose entry was written Offline (RebuildOutcome.AdoptOffline) out of
   *  service: it is not loaded, and it is recorded offline behind an empty transfer-out marker
   *  plus an empty CRL save file. That is exactly how the shutdown leaves the stores it closed, so
   *  a restart, or an adoption's check that every Offline store was closed cleanly, treats it
   *  the same way. Empty because a store fresh from a rebuild has no transaction state.
   *
   *  Written even when the device has already been unloaded (its shutdown finished first): the
   *  device path is still known, and without the pair an adopting host would refuse the whole
   *  device for a store that is in fact complete. A disk already pulled makes the writes fail,
   *  which is logged; an adopter then refuses the device until it is returned and shut down
   *  again, the same as any other store whose close did not finish.
   */
  private def keepRebuiltStoreOffline(local: LocalStorageDeviceState, storeId: StoreId): Unit = synchronized {
    offlineStores += storeId
    if storageDevices.get(local.storageDeviceId).contains(local) then
      local.offlineStores += storeId
    val storePath = local.devicePath / storeId.directoryName
    try
      os.write.over(storePath / StoreTransfer.TransferOutMarkerFile, "")
      if ! os.exists(Backend.crlSaveFile(storePath)) then
        CrashRecoveryLog.saveStoreState(storeId, Nil, Backend.crlSaveFile(storePath))
    catch
      case t: Throwable =>
        // Offline for the life of this process either way. A restart cancelling the shutdown
        // loses the abandoned-transfer-out pass's reinstatement without the marker, so the store
        // waits for this host's next start to load; an adopter refuses the device.
        logger.error(s"Failed to mark rebuilt store $storeId offline at $storePath: $t")
  }

  /** Gossips StoreMoved for a store this host has brought back online without repointing the
   *  pool: one whose rebuild has completed, or whose abandoned transfer out has been reinstated.
   *
   *  For a rebuild, the pool was repointed when the rebuild was assigned, but peers only learn it
   *  now, as at the end of a transfer. That was a different transaction, so the pool is read for
   *  the host and timestamp its config value carries. The store came online now, before the read,
   *  which is what clears its StoreOffline. Best-effort: a failed read only means peers find the
   *  store the slow way and keep it offline until their next recheck.
   */
  private def gossipStoreOnline(storeId: StoreId): Unit =
    val onlineTimestamp = HLCTimestamp.now
    client.getStoragePoolStateAndTimestamp(storeId.poolId).onComplete:
      case Success((poolState, timestamp)) =>
        val hostId = poolState.stores(storeId.poolIndex).hostId
        gossipRetransmitter.broadcast(StoreMoved(storeId, hostId, timestamp, onlineTimestamp))
      case Failure(err) =>
        logger.warn(s"Failed to read pool ${storeId.poolId} to gossip that store $storeId is online: $err")

  /** Moves a device shut down by `previousHostId` onto this host, in one transaction:
   *  - the device: Offline stores Active, status Operational, hostId this host;
   *  - every pool entry naming this device: hostId this host. Entries naming another device are
   *    left alone -- a TransferringIn entry's pool still names its source;
   *  - the previous host's HostState loses the device, this host's gains it;
   *  - for a bootstrap-pool store, the bootstrap config in the radicle, as a transfer does.
   *
   *  Every object written carries a KeyRevision, so a restart on the old host or a second
   *  adopter racing this one makes one of them lose and re-read. Returns false, writing nothing,
   *  when the re-read finds the device no longer Shutdown and owned by `previousHostId`. The
   *  previous host's HostState is skipped if that host has no entry in the hosts tree: it was
   *  removed, and there is nothing left to take the device out of.
   *
   *  Only a collision is retried, as everywhere else in Host: a permanent error would otherwise
   *  hold the device's pendingMarkerResolutions entry for the life of the process.
   *
   *  protected so a test subclass can record the calls.
   */
  protected def adoptDevice(storageDeviceId: StorageDeviceId, previousHostId: HostId): Future[Boolean] =
    import StorageDeviceState.DeviceStatus

    case class PoolUpdate(pointer: KeyValueObjectPointer, kvos: KeyValueObjectState, config: StoragePoolState)

    def readPool(poolId: PoolId): Future[PoolUpdate] =
      for
        ptr <- client.getStoragePoolPointer(poolId)
        kvos <- client.read(ptr)
      yield PoolUpdate(ptr, kvos, StoragePoolState(kvos))

    def readHost(id: HostId): Future[(KeyValueObjectPointer, KeyValueObjectState)] =
      for
        ptr <- client.getHostPointer(id)
        kvos <- client.read(ptr)
      yield (ptr, kvos)

    def writeHost(ptr: KeyValueObjectPointer, kvos: KeyValueObjectState, hs: HostState)(using tx: Transaction): Unit =
      tx.update(ptr, None, None,
        List(KeyRevision(HostState.StateKey, kvos.contents(HostState.StateKey).revision)),
        List(Insert(HostState.StateKey, hs.encode())))

    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      given Transaction = tx
      for
        devPtr <- client.getStorageDevicePointer(storageDeviceId)
        devKvos <- client.read(devPtr)
        state = StorageDeviceState(devKvos)
        adopted <-
          if state.status != DeviceStatus.Shutdown || state.hostId != previousHostId then
            Future.successful(false)
          else
            // Pool entries are rewritten before prepRadicleUpdate reads them, so its store map
            // names this host for every moved bootstrap store, not only the one it is given.
            val moved = state.stores.keysIterator.toList.sortBy(s => (s.poolId.uuid, s.poolIndex))
            for
              pools <- Future.sequence(moved.map(_.poolId).distinct.map(readPool))
              newHost <- readHost(hostId)
              oldHost <- readHost(previousHostId).map(Some(_)).recover:
                           case _: NoSuchElementException => None
              repointed = moved.filter: storeId =>
                pools.find(_.config.poolId == storeId.poolId).exists: pu =>
                  val entry = pu.config.stores(storeId.poolIndex)
                  if entry.storageDeviceId == storageDeviceId && entry.hostId != hostId then
                    pu.config.stores(storeId.poolIndex) = entry.copy(hostId = hostId)
                    true
                  else
                    false
              _ <- repointed.find(_.poolId == PoolId.BootstrapPoolId) match
                     case Some(storeId) =>
                       val cfg = pools.find(_.config.poolId == storeId.poolId).get.config
                       BootstrapConfig.prepRadicleUpdate(client, storeId, cfg, hostId)
                     case None => Future.unit
            yield
              val changedPools = repointed.map(_.poolId).toSet
              pools.filter(pu => changedPools.contains(pu.config.poolId)).foreach: pu =>
                tx.update(pu.pointer, None, None,
                  List(KeyRevision(StoragePoolState.ConfigKey, pu.kvos.contents(StoragePoolState.ConfigKey).revision)),
                  List(Insert(StoragePoolState.ConfigKey, pu.config.encode())))

              val newState = state.withOfflineStoresActive.copy(hostId = hostId, status = DeviceStatus.Operational)
              tx.update(devPtr, None, None,
                List(KeyRevision(StorageDeviceState.StateKey, devKvos.contents(StorageDeviceState.StateKey).revision)),
                List(Insert(StorageDeviceState.StateKey, newState.encode())))

              oldHost match
                case Some((ptr, kvos)) => writeHost(ptr, kvos, HostState(kvos).removeStorageDevice(storageDeviceId))
                case None =>
                  logger.info(s"Previous host $previousHostId of storage device $storageDeviceId is not " +
                              s"registered. Adopting the device without updating its host state")
              val (newPtr, newKvos) = newHost
              writeHost(newPtr, newKvos, HostState(newKvos).addStorageDevice(storageDeviceId))

              tx.result.foreach: _ =>
                logger.info(s"Adopted storage device $storageDeviceId from host $previousHostId")
              true
      yield adopted

  /** Reads the state recorded for `storageDeviceId` in the storage-devices tree.
   *
   *  A seam rather than a direct client call so a test can hold a lookup in flight while the
   *  device it names is loaded underneath it. That interleaving is what a device appearing on
   *  disk during its own check produces, and it cannot be staged through the real client,
   *  whose reads resolve on their own schedule.
   *
   *  Called while holding the instance lock, so an override must return promptly rather than
   *  block: the lock it holds is the one handleEvent takes.
   *
   *  May throw rather than return a failed Future. startDeviceCheck treats the two alike, so an
   *  override is free to do either.
   */
  protected def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    client.getStorageDeviceState(storageDeviceId)

  /** Reads the source device of a transfer in, which the reconcile's TransferringIn pass re-reads
   *  on every check so a source that has moved, shut down or failed is seen. A seam so a test can
   *  tell when the read has been acted on, which is otherwise unobservable when the pass decides
   *  not to start anything. */
  protected def readTransferSourceState(fromDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    client.getStorageDeviceState(fromDeviceId)

  /** Takes one store out of service for a device shutdown. The default is closeStore, which saves
   *  the store's CRL state beside it. A seam so a test can hold a close open, and count closes,
   *  without real backends. */
  protected def closeStoreForShutdown(storeId: StoreId): Future[Unit] = closeStore(storeId)

  /** The last step of a shutdown: one transaction on the device state that either commits
   *  Shutdown or explains why it did not.
   *
   *  The Active check is defence in depth. Every transaction that brings a store into service
   *  writes StorageDeviceState.serviceStatus, which is Offline once the shutdown request has
   *  committed, so no Active entry should be found here. Should one appear anyway (a writer
   *  that misses that rule), committing Shutdown would shelve a disk whose metadata says one of
   *  its stores is serving. Instead those entries are flipped to Offline, as the request itself
   *  did for the entries Active at the time, and the caller goes round again to close them.
   *
   *  An already-Shutdown device counts as Completed with nothing written. Only this method writes
   *  Shutdown, so seeing it means an earlier attempt committed -- a retry after a commit whose
   *  outcome was not learned, or a crash before the unload -- and a restart would have made it
   *  Operational rather than leaving it Shutdown. Reporting Cancelled there would remove
   *  shutdown.state from a device that is shut down.
   *
   *  Only a collision is retried. Nothing else would complete the future, and the device's
   *  shutdown guard would be held for the life of the process.
   *
   *  protected so a test can run work between the stores closing and this transaction.
   */
  protected def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome] =
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        ptr <- client.getStorageDevicePointer(storageDeviceId)
        kvos <- client.read(ptr)
      yield
        val state = StorageDeviceState(kvos)

        def write(newState: StorageDeviceState): Unit =
          tx.update(ptr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, newState.encode())))

        state.status match
          case StorageDeviceState.DeviceStatus.Shutdown => ShutdownOutcome.Completed
          case StorageDeviceState.DeviceStatus.ShuttingDown =>
            if state.stores.valuesIterator.exists(_.status == StorageDeviceState.StoreStatus.Active) then
              write(state.withActiveStoresOffline)
              ShutdownOutcome.StoresReactivated
            else
              write(state.copy(status = StorageDeviceState.DeviceStatus.Shutdown))
              ShutdownOutcome.Completed
          case _ => ShutdownOutcome.Cancelled

  /** Shuts down a loaded device whose state reads ShuttingDown (or Shutdown, see below).
   *
   *  Called from reconcileDeviceState in place of its normal passes, so while a device is shutting
   *  down nothing new starts on it: no store is created, no transfer in begins, no rebuild starts.
   *  The sequence, each step after the previous one completes:
   *  1. stopDeviceWork: cancel the rebuilds and transfers running on the device.
   *  2. closeStoresForShutdown: close every open store, mark it and announce it offline.
   *  3. Write shutdown.state naming this host, so the device is not loaded again on its own.
   *  4. completeDeviceShutdown, then act on its outcome.
   *
   *  Every step is idempotent, and that is what makes the whole safe to repeat. A crash anywhere
   *  leaves the device ShuttingDown with this host's owning-host file: the restart loads it (the
   *  load table's ShuttingDown row) and the first check runs this again, which skips whatever
   *  was already done. A failure releases the guard and leaves the device to the next check.
   *
   *  A loaded device reading Shutdown gets the same treatment. That is a shutdown whose commit
   *  landed but whose unload did not happen in this process, and finishing it is the same work:
   *  completeDeviceShutdown reports Completed without writing.
   *
   *  Caller holds the instance lock; the continuations take it as they need it.
   */
  private def shutDownDevice(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val deviceId = local.storageDeviceId
    if ! shuttingDownDevices.contains(deviceId) then
      shuttingDownDevices += deviceId
      logger.info(s"Shutting down storage device $deviceId at ${local.devicePath}")

      def release(): Unit = synchronized:
        shuttingDownDevices -= deviceId

      // A throw before the first Future exists would otherwise skip the onComplete below, and
      // with it the guard's release: every later check of this device would then do nothing.
      val fshutdown =
        try
          stopDeviceWork(local, remote)
          for
            _ <- closeStoresForShutdown(local)
            _ = DeviceMarkers.writeShutdownState(local.devicePath, hostId)
            outcome <- completeDeviceShutdown(deviceId)
            _ <-
              if outcome == ShutdownOutcome.Completed then closeThenUnload(local)
              else
                Future.unit
          yield outcome
        catch case t: Throwable => Future.failed(t)

      fshutdown.onComplete:
        case Success(ShutdownOutcome.Completed) =>
          release()
          logger.info(s"Storage device $deviceId is shut down and unloaded. It is safe to remove")

        case Success(ShutdownOutcome.StoresReactivated) =>
          // The newly Offline entries' stores may be open. Going round again closes them; the
          // guard is released first so that check's reconcile can start the sequence.
          logger.info(s"Stores on storage device $deviceId became Active during its shutdown. " +
                      s"Taking them offline and trying again")
          release()
          checkStorageDevice(deviceId)

        case Success(ShutdownOutcome.Cancelled) =>
          // A restart arrived mid-shutdown. Removing shutdown.state lets the device load again
          // after a restart of this host. The check that follows runs the normal reconcile,
          // whose abandoned-transfer-out pass finds each store this shutdown closed -- Active
          // again, offline here, and carrying the marker -- and loads it.
          logger.info(s"Shutdown of storage device $deviceId was cancelled by a restart")
          try
            if os.exists(local.devicePath / DeviceMarkers.ShutdownStateFile) then
              DeviceMarkers.removeShutdownState(local.devicePath)
          catch case t: Throwable =>
            // Harmless while this process runs: the device stays loaded. A restart of this host
            // would find the file and treat the device as shut down until it is resolved.
            logger.error(s"Failed to remove ${DeviceMarkers.ShutdownStateFile} from " +
                         s"${local.devicePath}: $t")
          release()
          checkStorageDevice(deviceId)

        case Failure(t) =>
          release()
          logger.warn(s"Shutdown of storage device $deviceId failed. The next check of the device " +
                      s"will try again. Error: $t")

  /** Step 1 of shutDownDevice: stops everything running on the device. Nothing is waited for.
   *  Work that finishes regardless -- a rebuild already past its last read, a transfer in already
   *  in its pool update, a store creation already loading -- writes its entry Offline rather than
   *  Active, because each of those transactions reads the device state and writes serviceStatus.
   *
   *  The device entries are left exactly as they are. The interrupted work is resumed after a
   *  restart or adoption by the same crash-recovery paths that resume it after a crash: a
   *  rebuild from its checkpoint in staging, a transfer in by the reconcile's TransferringIn
   *  pass, a transfer out by the receiver's retransmitted StartStoreTransfer.
   */
  private def stopDeviceWork(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val cancelledIns = synchronized {
      val deviceId = local.storageDeviceId
      def entriesIn(status: StorageDeviceState.StoreStatus): Set[StoreId] =
        remote.stores.collect { case (storeId, entry) if entry.status == status => storeId }.toSet

      // Cancelled rebuilds fail their complete with RebuildCancelled, which releases their slot.
      entriesIn(StorageDeviceState.StoreStatus.Rebuilding).foreach: storeId =>
        rebuildingStores.get(storeId).foreach: sr =>
          logger.info(s"Cancelling the rebuild of store $storeId for the shutdown of device $deviceId")
          sr.cancel()

      // Forgotten before the service is told, so each attempt's completion handler finds itself
      // no longer current and neither retries nor promotes. An attempt already past that check
      // runs its pool update, which writes the entry Offline.
      val ins = entriesIn(StorageDeviceState.StoreStatus.TransferringIn).toList.flatMap: storeId =>
        if pendingStartTransfers.get(storeId).exists(_.toDeviceId == deviceId) then
          pendingStartTransfers -= storeId
        if transferInRetries.get(storeId).exists(_.toDeviceId == deviceId) then
          transferInRetries -= storeId
        val uuid = transferringIn.get(storeId)
        transferringIn -= storeId
        uuid.map(storeId -> _)

      // The store is already closed and marked by its transfer. Cancelling the send leaves it so.
      entriesIn(StorageDeviceState.StoreStatus.TransferringOut)
        .filter(storeTransferService.isTransferringOut)
        .foreach: storeId =>
          logger.info(s"Cancelling the transfer out of store $storeId for the shutdown of device $deviceId")
          storeTransferService.cancelTransferOut(storeId)

      ins
    }

    cancelledIns.foreach: (storeId, transferUUID) =>
      logger.info(s"Cancelling the transfer in of store $storeId for the shutdown of device ${local.storageDeviceId}")
      storeTransferService.cancelTransferIn(storeId, transferUUID)

  /** Step 2 of shutDownDevice, also run once more after Shutdown commits: closes each store open
   *  on the device, then writes its transfer-out marker (empty, as no transfer owns it) and
   *  gossips StoreOffline.
   *
   *  The marker is what keeps the store from loading on any later start of this device, here or
   *  on an adopting host, until a restart or adoption clears it. It is the same file a transfer
   *  out writes, which lets the reconcile's abandoned-transfer-out pass bring the store back if
   *  the shutdown is cancelled: that pass fires only for an Active entry, so it cannot misfire
   *  against the Offline entries of a device that stays shut down.
   *
   *  Picked: every store loaded on the device that is still open, plus any closed one whose
   *  directory lacks the marker -- an earlier attempt whose marker write failed. Stores closed by
   *  a transfer out carry their own marker and are left alone, as are stores never loaded here
   *  (Rebuilding, TransferringIn), which get no handling: they are left as a crash leaves them.
   *  One at a time, so each store's close has finished before its marker is written.
   */
  private def closeStoresForShutdown(local: LocalStorageDeviceState): Future[Unit] =
    runSequentially(storesToClose(local)): storeId =>
      closeStoreForShutdown(storeId).map: _ =>
        synchronized:
          // ShutdownStore records this too. Done here as well so the bookkeeping that later
          // steps read does not depend on how the store was closed.
          offlineStores += storeId
          local.offlineStores += storeId
          os.write.over(local.devicePath / storeId.directoryName / StoreTransfer.TransferOutMarkerFile, "")
          gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))
    .map(_ => ())

  /** The stores closeStoresForShutdown picks. */
  private def storesToClose(local: LocalStorageDeviceState): List[StoreId] = synchronized {
    local.loadedStores.filter: storeId =>
      val storePath = local.devicePath / storeId.directoryName
      val unmarked = os.isDir(storePath) && !os.exists(storePath / StoreTransfer.TransferOutMarkerFile)
      !offlineStores.contains(storeId) || unmarked
    .toList.sortBy(s => (s.poolId.uuid, s.poolIndex))
  }

  /** Unloads a device whose Shutdown has committed, first closing any store loaded on it since
   *  the shutdown's own close: a rebuild or transfer in that flipped its entry Active before
   *  completeDeviceShutdown flipped it back may have loaded its store meanwhile. Unloaded with
   *  such a store open, nothing would ever close it.
   *
   *  The emptiness check and the unload share one hold of the lock, which is the lock the
   *  LoadStoreById handler takes. A load that has not happened by then finds the device gone
   *  and does nothing; one that has is in loadedStores and is closed by another round.
   */
  private def closeThenUnload(local: LocalStorageDeviceState): Future[Unit] = synchronized {
    if storesToClose(local).isEmpty then
      unloadDevice(local.storageDeviceId)
      Future.unit
    else
      closeStoresForShutdown(local).flatMap(_ => closeThenUnload(local))
  }

  /** Forgets a device that has been shut down. Its stores stay in offlineStores, so messages for
   *  them are dropped rather than answered with UnknownStore, and it stays in this host's
   *  HostState: this host still owns it until another host adopts it.
   *
   *  Dropping its loadedDeviceDirs entry means the scan reads the directory again, finds
   *  shutdown.state and leaves the device unloaded until a restart or adoption resolves it.
   */
  private def unloadDevice(storageDeviceId: StorageDeviceId): Unit = synchronized {
    storageDevices -= storageDeviceId
    loadedDeviceDirs = loadedDeviceDirs.filter((_, id) => id != storageDeviceId)
  }

  /** Reconciles a loaded device's on-disk state against the state recorded for it in the
   *  storage-devices tree: deletes stores transferred away, reinstates stores left offline by an
   *  abandoned transfer out, creates Initializing stores, and starts transfers in.
   *
   *  If the tree records a different host, none of that happens and the device is not claimed.
   *  tryLoadDevice only loads a device whose `owning-host` file names this host, so a mismatch
   *  means the file and the tree disagree -- a hand-edited or copied file, or a device adopted
   *  by another host and then carried back here without a shutdown (an adoption itself rewrites
   *  the file only after its commit, while shutdown.state still keeps the device unloaded) --
   *  and reconciling against another host's records could delete or create
   *  stores that are not ours. It is logged once per device and left for an operator.
   *  A device the tree records as failed is ignored outright: it is neither claimed nor
   *  reconciled, and its stores are being rebuilt elsewhere.
   *  A device the tree records as ShuttingDown (or Shutdown) is handed to shutDownDevice instead
   *  of any of the passes, so nothing new starts on a disk about to be shelved.
   *
   *  Callers hold the instance lock; the continuations it registers do not.
   */
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
    if remote.isFailed then
      // An operator has declared this device dead. Its directory can still mount -- a dead
      // controller rather than a dead platter -- and its zeroed host id must not be mistaken for
      // another owner by the branch below. Its stores are being rebuilt elsewhere.
      logger.info(s"Ignoring failed storage device ${local.storageDeviceId}")
    else if remote.hostId != hostId then
      warnOnce(local.devicePath, s"Storage device ${local.storageDeviceId} owned by host ${remote.hostId} " +
                                 s"is loaded here. Not claiming it")
    else if remote.status == StorageDeviceState.DeviceStatus.ShuttingDown ||
            remote.status == StorageDeviceState.DeviceStatus.Shutdown ||
            shuttingDownDevices.contains(local.storageDeviceId) then
      // None of the passes below run: each would start work on a device about to be shelved.
      // A store transferred away meanwhile keeps its directory until the device is next
      // reconciled while Operational, here or on an adopting host.
      //
      // That includes an Operational read while a shutdown is still in flight -- the restart's
      // own nudge, typically. The in-flight sequence ends in Cancelled and checks the device
      // again once its own work has stopped, and that check runs the passes. shutDownDevice
      // itself does nothing then, as the guard is held.
      shutDownDevice(local, remote)
    else
      //----------------------
      // Deleted Stores
      //
      local.offlineStores.filter(storeId =>
        !remote.stores.contains(storeId)
      ).foreach: storeId =>
        offlineStores -= storeId
        local.offlineStores -= storeId
        // Dropped from loadedStores too, or a later shutdown would take this store for one still
        // open (it is no longer in offlineStores) and fail writing a marker into a deleted
        // directory on every attempt.
        local.loadedStores -= storeId
        val storePath = local.devicePath / storeId.directoryName
        if os.exists(storePath) then
          logger.info(s"Deleting successfully transferred store $storePath")
          try
            os.remove.all(storePath)
          catch
            case t: Throwable => logger.error(s"Failed to delete store $storePath. Error: $t")

      //----------------------
      // Abandoned Transfers Out
      //
      // Nothing else ever clears a transfer-out marker from the source. startStoreTransferIn
      // deletes it only from the destination's unpacked copy, and a transfer that completes
      // normally has the source's whole store directory deleted by the pass above -- the marker
      // goes with it. So a transfer that ends without completing, which is what a destination
      // declared failed mid-flight produces, strands the marker forever, and tryLoadStore re-reads
      // it on every restart and puts the store straight back into offlineStores.
      //
      // That pairs badly with the metadata repair for the same event. That repair puts this
      // entry back to Active and leaves the pool naming this device, so afterwards the metadata
      // reads entirely healthy while the slice is served by nobody, here or anywhere. Repairing
      // the metadata alone would trade a visibly stalled TransferringOut entry for an invisible
      // outage.
      //
      // On the source host's own poll rather than inline at either repair site, because only
      // this host can touch this disk and one of those two sites -- the drain -- runs wherever
      // the durable task happens to live. Polling for desired state rather than being told is
      // also what the rest of this method does.
      //
      // The guard is narrow on purpose. Active *and* no transferDevice is the only combination
      // that says no transfer is outstanding; an entry still TransferringOut, or Active with a
      // destination still named, means the marker is doing its job. isTransferringOut is the
      // same question asked of this process rather than the tree, and covers the window where
      // the drain restores the entry while this host is still shipping bytes.
      //
      // A store still sending is left alone but re-checked shortly. An abort's nudge usually
      // arrives while the send is still running, and the send ends (the destination cancels its
      // receive) with nothing to prompt another check for a full checkStorageDevicePeriod.
      val (stillSending, abandonedOut) = local.offlineStores.filter(storeId =>
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.Active && entry.transferDevice.isEmpty
        ) &&
        os.exists(local.devicePath / storeId.directoryName / StoreTransfer.TransferOutMarkerFile)
      ).partition(storeTransferService.isTransferringOut)
      abandonedOut.foreach: storeId =>
        reinstateAbandonedTransferOut(local, storeId)
      if stillSending.nonEmpty then
        scheduleDeviceRecheck(local.storageDeviceId)

      //----------------------
      // Completed Transfers In
      //
      // A crash between the pool update committing and the transferred-in marker's removal
      // leaves an Active entry naming this device over a copy that tryLoadStore skips. The entry
      // is no longer TransferringIn, so startStoreTransferIn never sees it, and nothing else
      // removes the marker. Active is the tree's word that the pool now names this device, so
      // the copy is ours to load. transferringIn covers the window where this process's own
      // metadata step has committed but has not yet removed the marker itself.
      local.offlineStores.filter(storeId =>
        !transferringIn.contains(storeId) &&
        remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active) &&
        os.exists(local.devicePath / storeId.directoryName / StoreTransfer.TransferredInMarkerFile)
      ).foreach: storeId =>
        val storePath = local.devicePath / storeId.directoryName
        logger.warn(s"Store $storeId on device ${local.storageDeviceId} is Active but still " +
                    s"carries its transferred-in marker. Completing the transfer and loading the store")
        if clearTransferredInMarker(storeId, storePath) then
          tryLoadStore(local, storePath)

      //----------------------
      // New Stores
      //
      remote.stores.filter((storeId, entry) =>
        entry.status == StorageDeviceState.StoreStatus.Initializing
      ).map( (storeId, _) =>
        storeId
      ).foreach: storeId =>
        createNewStore(local, storeId)

      //----------------------
      // Aborted Transfers In
      //
      // An abort removes this device's TransferringIn entry and tells nobody, so this check is the
      // only thing that learns of it. Left alone, an attempt still waiting on bytes keeps
      // retransmitting its request and holds the store in transferringIn, which would make the
      // pass below skip a later transfer of the same store. Before that pass for that reason.
      // An attempt past promotion is not covered here; updateStateForTransferredStore sees the
      // abort in the source's entry instead.
      val trackedTransfersIn =
        pendingStartTransfers.valuesIterator
          .filter(_.toDeviceId == local.storageDeviceId)
          .map(p => (p.msg.storeId, p.msg.fromDevice)).toList ++
        transferInRetries.iterator
          .filter((_, r) => r.toDeviceId == local.storageDeviceId)
          .map((storeId, r) => (storeId, r.fromDeviceId)).toList
      trackedTransfersIn.filterNot((storeId, fromDeviceId) =>
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
          entry.transferDevice.contains(fromDeviceId))
      ).foreach: (storeId, fromDeviceId) =>
        dropAbandonedTransferIn(storeId, fromDeviceId, local.storageDeviceId)

      // The same store can be re-targeted at another device of this host. transferringIn is keyed
      // by store, so the earlier attempt would block the new one until its own device's next
      // check noticed the abort. This device's entry is the newer word on who receives the store,
      // so the other device's attempt is dropped here instead of a poll later.
      val retargeted =
        pendingStartTransfers.valuesIterator
          .filter(_.toDeviceId != local.storageDeviceId)
          .map(p => (p.msg.storeId, p.msg.fromDevice, p.toDeviceId)).toList ++
        transferInRetries.iterator
          .filter((_, r) => r.toDeviceId != local.storageDeviceId)
          .map((storeId, r) => (storeId, r.fromDeviceId, r.toDeviceId)).toList
      retargeted.filter((storeId, _, _) =>
        remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.TransferringIn)
      ).foreach: (storeId, fromDeviceId, toDeviceId) =>
        dropAbandonedTransferIn(storeId, fromDeviceId, toDeviceId)

      //----------------------
      // Transferring In Stores
      //
      remote.stores.filter { (storeId, entry) =>
        entry.status == StorageDeviceState.StoreStatus.TransferringIn
      }.map { (storeId, status) =>
        (storeId, status.transferDevice, status.transferId)
      }.toList.foreach: (storeId, ofromDeviceId, transferId) =>
        ofromDeviceId.foreach: fromDeviceId =>
          readTransferSourceState(fromDeviceId).foreach: fromDevice =>
            if fromDevice.isFailed then
              // A tombstoned source has both ids zeroed, so fromDevice.hostId is HostId(0,0).
              // Starting the transfer would address every StartStoreTransfer to a host that
              // does not exist; the messages are dropped, the entry never resolves, and this
              // check re-sends them forever. Nothing reports the stall either: rebalancing's
              // ownedStores filters TransferringIn out of its accounting, so the store is
              // invisible to the rebalancer too.
              abandonTransferIn(local.storageDeviceId, storeId, fromDeviceId)
            else if ! fromDevice.isOperational then
              // The source is shutting down or shut down, and its host refuses to send. The
              // transfer stays pending: the next check of this device reads the source again,
              // owner included, so a source adopted by another host is asked there.
              logger.info(s"Not starting the transfer in of store $storeId onto device " +
                          s"${local.storageDeviceId}: source device $fromDeviceId is " +
                          s"${fromDevice.status}. Retrying on a later check")
            else
              startStoreTransferIn(storeId, fromDevice.hostId, fromDeviceId, local.storageDeviceId, transferId)

      //----------------------
      // Rebuilding Stores
      //
      remote.stores.filter((_, entry) =>
        entry.status == StorageDeviceState.StoreStatus.Rebuilding
      ).keys.toList.sortBy(_.poolIndex).foreach: storeId =>
        startStoreRebuild(local, storeId)

  /** Brings a store back online after its transfer out was abandoned rather than completed.
   *
   *  Purely local: the tree already says Active and the pool already names this device, so there
   *  is nothing to transact and no retry loop to get wrong. Removing the marker is what makes
   *  the repair survive a restart; loading is what makes it take effect now.
   *
   *  Loading goes through tryLoadStore rather than opening a backend here, because that method
   *  owns the rest of the bookkeeping -- reading the store config, and clearing the store from
   *  both this device's offlineStores and the host.s. Those two sets and the marker file are
   *  three representations of one fact and must not be allowed to disagree.
   *
   *  If the load fails after the marker is gone, tryLoadStore's own catch logs it and the store
   *  stays in offlineStores for the life of the process: this pass will not pick it up again,
   *  since it keys off the marker that is now removed. The next restart loads it normally. That
   *  is a far smaller failure than the permanent, silent one this pass exists to close.
   *
   *  Caller holds the instance lock and has established the tree entry and the marker.
   */
  private def reinstateAbandonedTransferOut(local: LocalStorageDeviceState,
                                            storeId: StoreId): Unit =
    val storePath = local.devicePath / storeId.directoryName

    logger.warn(s"Store $storeId on device ${local.storageDeviceId} is offline behind a " +
                s"transfer-out marker, but the storage-devices tree records it as Active with " +
                s"no transfer outstanding. Clearing the abandoned transfer and loading the store")

    val markerRemoved =
      try
        os.remove(storePath / StoreTransfer.TransferOutMarkerFile)
        true
      catch
        case t: Throwable =>
          // Leave the store offline. Loading it with the marker still in place would be undone
          // by the next restart, so this pass would have logged a repair that did not hold.
          logger.error(s"Failed to remove the transfer-out marker for store $storeId at " +
                       s"$storePath. The store stays offline. Error: $t")
          false

    if markerRemoved then
      tryLoadStore(local, storePath)
      if !local.offlineStores.contains(storeId) then
        gossipStoreOnline(storeId)

  /** Drops a TransferringIn entry whose source device has been declared failed.
   *
   *  The store itself is not lost: FailedStorageDeviceDurableTask's drain owns the source's
   *  TransferringOut entry and rebuilds the store onto a live device. This only clears the
   *  destination's half, which that drain deliberately does not touch.
   *
   *  The two cannot fight. The drain writes the source device, the pool, and the rebuild
   *  destination it selected; this writes only this device. Where they coincide -- the drain
   *  happening to select this device -- both writes carry a KeyRevision on the same state key,
   *  so one loses and re-reads: if the drain wins, the entry reads Rebuilding and the guard
   *  below leaves it alone; if this wins, the drain's Rebuilding entry simply re-adds the store.
   */
  private def abandonTransferIn(toDeviceId: StorageDeviceId,
                                storeId: StoreId,
                                fromDeviceId: StorageDeviceId): Unit = synchronized {
    // The entry that triggers this stands until the transaction commits, so every device check
    // in the meantime calls here again. Without the guard each one starts its own retry loop,
    // all writing the same key and losing to each other on KeyRevision. Same shape as
    // createNewStore's creatingStores and startStoreTransferIn's transferringIn.
    if ! abandoningTransferIns.contains(storeId) then
      abandoningTransferIns += storeId

      logger.warn(s"Abandoning transfer of store $storeId onto device $toDeviceId: source " +
                  s"device $fromDeviceId has been declared failed")

      // Nothing consumes this Future, so retrying a permanent error would be a silent 60 s loop
      // for the life of the process -- and now that the guard above is set, one that no later
      // check could ever displace. Only a collision is retried.
      val fabandon = client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
        for
          ptr <- client.getStorageDevicePointer(toDeviceId)
          kvos <- client.read(ptr)
          state = StorageDeviceState(kvos)
        yield
          // Re-read inside the transaction. A concurrent check may have dropped the entry already,
          // and the entry may have moved on to Rebuilding or Active, neither of which is ours to
          // remove.
          state.stores.get(storeId).foreach: entry =>
            if entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
               entry.transferDevice.contains(fromDeviceId) then
              val newState = state.removeStore(storeId)
              tx.update(ptr, None, None,
                List(KeyRevision(StorageDeviceState.StateKey,
                  kvos.contents(StorageDeviceState.StateKey).revision)),
                List(Insert(StorageDeviceState.StateKey, newState.encode())))

      // Cleared on failure as well as success. A StopRetrying means this attempt gave up, and the
      // next device check -- an hour out, and possibly after whatever made the read fail has been
      // repaired -- should be free to try again rather than find the flag stuck forever.
      fabandon.onComplete: result =>
        synchronized:
          abandoningTransferIns -= storeId
        // Whether or not this transaction was the one to remove the entry, it is gone (or has
        // moved on to Rebuilding), so nothing will complete this host's attempt. Its registration
        // has no timeout of its own and its retransmits go to a host that does not exist.
        if result.isSuccess then
          dropAbandonedTransferIn(storeId, fromDeviceId, toDeviceId)
  }

  /** Checks `storageDeviceId` again after TransferOutRecheckDelay, so a store left offline behind
   *  a transfer-out marker is reinstated soon after an aborted transfer's send ends rather than at
   *  the next periodic check. At most one is scheduled per device; requests made meanwhile are
   *  covered by it.
   */
  private def scheduleDeviceRecheck(storageDeviceId: StorageDeviceId): Unit = synchronized {
    if ! scheduledDeviceRechecks.contains(storageDeviceId) then
      scheduledDeviceRechecks += storageDeviceId
      backgroundTasks.schedule(TransferOutRecheckDelay)(runDeviceRecheck(storageDeviceId))
  }

  private def runDeviceRecheck(storageDeviceId: StorageDeviceId): Unit = synchronized {
    if scheduledDeviceRechecks.contains(storageDeviceId) then
      scheduledDeviceRechecks -= storageDeviceId
      checkStorageDevice(storageDeviceId)
  }

  /** Requests a check of one storage device.
   *
   *  At most one lookup per device is outstanding at a time. A request arriving while one is
   *  in flight is deferred rather than dropped: dropping it costs a full
   *  checkStorageDevicePeriod -- Main.CheckStorageDevicesPeriod in production, an hour at
   *  present -- which is the same cost as losing the notification message outright. At most
   *  one deferral is held per device, so this still throttles the pile-up of checks that
   *  builds up over an offline period.
   *
   *  One deferral suffices because a check is a reconcile, not a delta: it re-reads the
   *  storage-devices tree and acts on whatever that says, so a single re-run subsumes any
   *  number of requests coalesced into it. That is what makes a Set correct here rather than
   *  lossy.
   */
  private def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =
    synchronized:
      if activeDeviceChecks.contains(storageDeviceId) then
        deferredDeviceChecks += storageDeviceId
      else
        startDeviceCheck(storageDeviceId)

  /** Issues the one outstanding lookup for a device and acts on its result: a loaded device
   *  is reconciled against the tree, a device this process has not loaded -- never seen on disk,
   *  rejected by the load decision, or unloaded by a shutdown -- has its stores
   *  added to offlineStores so they stay silent rather than answering UnknownStore, and a
   *  failed lookup is warned about. The guard entry is then released and any request deferred
   *  behind it re-dispatched.
   *
   *  Caller holds the instance lock and has established that no check is active for this
   *  device.
   *
   *  Which of those two branches runs is decided when the lookup completes, not when it is
   *  issued. Runtime device discovery can load a device while its check is in flight, and a
   *  branch chosen at dispatch time would then mark a loaded device's stores offline --
   *  re-adding ids that tryLoadStore and the LoadStore handler have just removed, and that
   *  nothing removes again: the only other site that clears offlineStores is
   *  reconcileDeviceState's deleted-stores pass, which touches ids recorded in the device's
   *  own offlineStores set, which these never enter.
   *
   *  The lookup is issued under the caller's lock and the callback re-takes it, so the whole
   *  of reconcileDeviceState -- filesystem work included -- runs with the event loop stalled.
   *  That same lock is what makes the load-state re-read safe: it is the one handleEvent
   *  takes, so no device can be loading while the callback runs.
   *
   *  It is also what keeps a deferral from being stranded. checkStorageDevice's deferral
   *  write and this callback's release-and-read of the guard all happen under that one
   *  instance monitor, so a colliding request either finds no active check and starts its
   *  own, or records a deferral the in-flight check's endDeviceCheck is guaranteed to see.
   *
   *  Releasing the guard is hostage to the lookup completing. The default
   *  lookupStorageDeviceState reads through the client, whose read driver retransmits with
   *  exponential backoff and imposes no timeout, so an unreachable pool holds this device's
   *  guard for the whole outage while the deferral coalesces everything arriving meanwhile
   *  into one re-check. That is intended, and it is what "one lookup in flight" amounts to in
   *  production.
   *
   *  The entry must be released on four exit paths: both outcomes of the lookup, a throw out of
   *  the callback body, and a throw from lookupStorageDeviceState itself. The first three are
   *  the finally. The fourth cannot be -- a throw before the Future exists means no callback is
   *  ever registered and so no finally ever runs -- hence the wrapper around the lookup call,
   *  which treats such a throw as a failed lookup. Releasing on fewer than all four would skip
   *  every later check of that device for the life of the process.
   *
   *  The callback-body path is not hypothetical: the reconcile touches the filesystem and
   *  issues transactions, so it can throw. It and the synchronous throw are both covered by
   *  HostDeviceDiscoverySuite.
   *
   *  Treating a synchronous throw as a failed lookup rather than letting it propagate leaves
   *  this method with no non-fatal synchronous throw path, and that is what makes
   *  endDeviceCheck's re-dispatch safe: a call that cannot throw cannot replace an exception
   *  already unwinding out of the try body, which is how a reconcileDeviceState failure used to
   *  be lost. Not literally none: the onComplete registration below sits outside the wrapper and
   *  does rethrow if the ExecutionContext throws a fatal, which no real thread-pool EC does.
   *
   *  Totality also keeps the CheckAllDevices sweep whole. That handler iterates storageDevices
   *  and calls this method per device with nothing catching in between, so a propagating throw
   *  would abandon every device after the throwing one until the next period -- an hour in
   *  production. No test covers that, and a refactor reintroducing a throw here would lose it
   *  silently.
   *
   *  The wrapper is scoped to the lookup call and not to the onComplete registration, but not
   *  for the reason one might expect. Widening it would not catch a non-fatal throw out of the
   *  callback body even under an inline or parasitic ExecutionContext: Future's
   *  Transformation.run absorbs those and routes them to reportFailure, so onComplete returns
   *  normally. What a wider scope would catch is a fatal throw, turning it into a logged
   *  warning. What the narrow scope gives up is an ExecutionContext that rejects the submission
   *  or accepts it and never runs the callback: either still leaks the entry, and neither is
   *  guarded here.
   */
  private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
    // Releases the guard and re-dispatches whatever was deferred behind it. Called from the
    // callback's finally and from the synchronous-throw path below; both hold the instance
    // lock. Nested rather than a method on the class because it has no other caller, and
    // because closing over storageDeviceId leaves the two call sites no way to disagree about
    // which device they are releasing.
    //
    // The deferral flag is cleared before the re-dispatch, not after, and the order is enforced
    // by HostDeviceDiscoverySuite rather than argued. Clearing after recurses an extra
    // frame whenever the re-dispatched lookup throws synchronously -- the nested call absorbs
    // its own throw and re-enters here with the flag still set -- which needs no inline or
    // parasitic ExecutionContext to reach. The callback-body test catches it as a third lookup
    // attempt.
    def endDeviceCheck(): Unit =
      activeDeviceChecks -= storageDeviceId
      if deferredDeviceChecks.contains(storageDeviceId) then
        deferredDeviceChecks -= storageDeviceId
        startDeviceCheck(storageDeviceId)

    activeDeviceChecks += storageDeviceId

    // Deliberately around the lookup call alone and not the onComplete registration below; the
    // scaladoc has why. Throwable rather than NonFatal is also deliberate: the re-dispatch
    // safety argument needs this method to be total, so do not condense to Try(...), which
    // would narrow the catch to NonFatal and reopen the leak for a fatal throw.
    val lookup =
      try Success(lookupStorageDeviceState(storageDeviceId))
      catch case t: Throwable => Failure(t)

    lookup match
      case Success(pendingState) =>
        pendingState.onComplete: result =>
          synchronized:
            try
              result match
                case Success(remote) =>
                  // Load state re-read here, not at dispatch. See startDeviceCheck's scaladoc.
                  storageDevices.get(storageDeviceId) match
                    case Some(local) => reconcileDeviceState(local, remote)
                    case None =>
                      // A device not loaded right now: one that has not appeared on disk in this
                      // process, one the load decision rejected, or one a shutdown unloaded.
                      // Silence its stores rather than sending "UnknownStore".
                      remote.stores.keysIterator.foreach: storeId =>
                        offlineStores += storeId

                // The lookup fails whenever the device has no entry in the storage-devices tree:
                // a config written out-of-band naming an id that was never registered -- the
                // supported path cannot produce this, since
                // StorageDeviceManager.createStorageDevice commits the registration before
                // writing the config file, so its orphan is the reverse one, a registration with
                // no directory (see the ConfigWriteFailed advice in the cmdline Main) -- or a tree
                // entry removed after the fact, which no command does today. It also fails on any
                // failure of the metadata read itself, transient or not, which is the only
                // routinely reachable case.
                //
                // A copied or moved config is NOT one of these: its device is registered, so the
                // lookup succeeds. A device carried to another host keeps its registration too, so
                // a check of it there succeeds and takes the not-loaded branch above: it is not
                // loaded there until adopted (its owning-host file names its old host).
                case Failure(err) =>
                  val what =
                    if storageDevices.contains(storageDeviceId) then "storage device"
                    else "never-loaded storage device"
                  logger.warn(s"Failed to read state for $what $storageDeviceId. It may not " +
                              s"be registered in the storage-devices tree. Error: $err")
            finally endDeviceCheck()

      // A distinct message from the Failure(err) branch above, which explains itself with a
      // missing tree registration. True of a failed read; not necessarily true of a throw, which
      // an override is free to raise after reads have already gone out.
      //
      // The throwable is passed rather than interpolated, as the event loop's catch-all does and
      // unlike the read failures above. A read failure is a reachable outcome that its message
      // fully explains; a throw here is a bug on the dispatch path, where the class and message
      // alone can amount to a bare "java.lang.NullPointerException" naming no location. Before
      // the wrapper existed such a throw reached that catch-all and was logged with its stack,
      // so dropping it would make this a diagnostic downgrade.
      case Failure(err) =>
        logger.warn(s"Lookup of state for storage device $storageDeviceId threw instead of " +
                    s"returning a pending read", err)
        endDeviceCheck()

  def containsStore(storeId: StoreId): Boolean = synchronized {
    logger.trace(s"********* CONTAINS STORE: ${storeId}: ${stores.contains(storeId)}. Stores: ${stores}")
    stores.contains(storeId)
  } 
  
  def getStoreIds: List[StoreId] = synchronized {
    stores.keysIterator.toList
  }

  /** Stores this host can currently repair. A store mid-rebuild is already having everything
    * read, and a store transferring out is about to stop being ours; scanning either wastes the
    * load budget on work that will be redone or discarded.
    */
  def repairableStoreIds: List[StoreId] = synchronized {
    (stores.keySet -- offlineStores -- rebuildingStores.keySet --
      storeTransferService.transferringOutStores).toList
  }

  /** O(1) per-entry counterpart to `repairableStoreIds`: whether the given store is currently
    * repairable on this host. Must agree with `repairableStoreIds.contains(storeId)` exactly.
    */
  def isRepairable(storeId: StoreId): Boolean = synchronized {
    stores.contains(storeId) && !offlineStores.contains(storeId) &&
      !rebuildingStores.contains(storeId) && !storeTransferService.isTransferringOut(storeId)
  }

  def hasTransactions: Boolean = synchronized {
    stores.valuesIterator.exists(_.hasTransactions)
  }

  def logTransactionStatus(log: String => Unit): Unit = synchronized {
    stores.values.foreach(_.logTransactionStatus(log))
  }

  /** @param maxSliceSize the store's effective slice size limit, from its StoreConfig. None means unlimited. */
  def loadStore(storageDeviceId: StorageDeviceId, backend: Backend, maxSliceSize: Option[Long]): Future[Unit] = {
    val p = Promise[Unit]()
    events.put(LoadStore(storageDeviceId, backend, maxSliceSize, p))
    p.future
  }

  def loadStoreById(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    events.put(LoadStoreById(storageDeviceId, storeId))

  def receiveTransactionMessage(msg: TxMessage): Unit = {
    events.put(TransactionMessage(msg))
  }

  def receiveClientRequest(msg: ClientRequest): Unit = {
    events.put(ClientReq(msg))
  }

  def receiveHostMessage(msg: HostMessage): Unit =
    events.put(HostMsg(msg))

  def repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
    events.put(Repair(storeId, os, completion))

  /** Repairs a missed deletion on the given store. Used when the object is gone from the rest
    * of the system, leaving only the ObjectId and the storePointer bytes recorded in the pool's
    * errorTree to identify it.
    */
  def repairDelete(storeId: StoreId,
                   objectId: ObjectId,
                   storePointer: Array[Byte],
                   completion: Promise[Unit]): Unit =
    events.put(RepairDelete(storeId, objectId, storePointer, completion))

  def shutdown()(using ec: ExecutionContext): Future[Unit] = {
    events.put(Exit())
    // Lets the event loop's thread exit once it has handled Exit; it is not a daemon thread.
    threadPool.shutdown()
    serviceExecutorPromise.future.foreach(_.shutdown())
    systemTaskRunnerPromise.future.foreach(_.shutdown())
    pendingStartTask.cancel()
    gossipRetransmitTask.cancel()
    heartbeatTask.cancel()
    checkStorageDeviceTask.cancel()
    usageUpdateTask.foreach(_.cancel())
    repairService.cancel()
    shutdownPromise.future
  }
  
  private[aspen] def closeStore(storeId: StoreId): Future[Unit] = {
    val p = Promise[Unit]()
    events.put(ShutdownStore(storeId, p))
    p.future
  }

  protected def addRecoveryEvent(): Unit = events.add(RecoveryEvent())

  /** Placeholder for mixin class to implement transaction recovery */
  protected def handleRecoveryEvent(): Unit = ()

  def hasEvents: Boolean = synchronized {
    events.size() != 0
  }

  /** Handles all events in the event queue. Returns when the queue is empty */
  def testingOnlyHandleEvents(): Unit = {
    var event = events.poll(0, TimeUnit.NANOSECONDS)
    while (event != null) {
      handleEvent(event)
      event = events.poll(0, TimeUnit.NANOSECONDS)
    }
  }

  /** Testing hook: runs a CheckAllDevices event synchronously.
   *
   *  Direct rather than events.put + testingOnlyHandleEvents so a test does not also drain
   *  the InitializeTaskExecutor event queued by the constructor, which would attempt a real
   *  task-executor bootstrap transaction.
   */
  private[aspen] def testingOnlyCheckAllDevices(): Unit = handleEvent(CheckAllDevices())

  /** Testing hook: runs a ScanDevices event synchronously. See testingOnlyCheckAllDevices. */
  private[aspen] def testingOnlyScanDevices(): Unit = handleEvent(ScanDevices())

  private[aspen] def testingOnlyLoadedDeviceDirs: Map[os.Path, StorageDeviceId] = synchronized(loadedDeviceDirs)

  private[aspen] def testingOnlyReportedDeviceDirs: Set[os.Path] = synchronized(reportedDeviceDirs)

  /** Testing hook: device directories with a shutdown-marker lookup or adoption in flight. */
  private[aspen] def testingOnlyPendingMarkerResolutions: Set[os.Path] = synchronized(pendingMarkerResolutions)

  /** Testing hook: how many ShutdownMarkerResolved events are queued. */
  private[aspen] def testingOnlyQueuedMarkerResolutions: Int =
    events.toArray.count(_.isInstanceOf[ShutdownMarkerResolved])

  /** Testing hook: removes the queued ShutdownMarkerResolved events and handles them
   *  synchronously, leaving every other event queued. See testingOnlyCheckAllDevices. */
  private[aspen] def testingOnlyHandleMarkerResolutions(): Unit =
    val it = events.iterator()
    var resolved = List[Event]()
    while it.hasNext do
      val e = it.next()
      if e.isInstanceOf[ShutdownMarkerResolved] then
        it.remove()
        resolved = e :: resolved
    resolved.reverse.foreach(handleEvent)

  /** Testing hook: runs receiveHostMessage's handler synchronously. See
   *  testingOnlyCheckAllDevices for why this bypasses the event queue. */
  private[aspen] def testingOnlyHandleHostMessage(msg: HostMessage): Unit =
    handleEvent(HostMsg(msg))

  /** Testing hook: handles a StartStoreTransfer as the event loop would, returning the Future that
   *  fails when the request is refused, so a test can tell a refusal from a request still in flight. */
  private[aspen] def testingOnlyStartStoreTransferOut(m: StartStoreTransfer): Future[Unit] =
    synchronized(startStoreTransferOut(m))

  /** Testing hook: runs the post-transfer metadata update synchronously.
   *
   *  In production this is driven by a transfer in completing, which needs a real device
   *  directory and a real byte stream. The metadata half is what the tests are about. */
  private[aspen] def testingOnlyUpdateStateForTransferredStore(storeId: StoreId,
                                                               fromDeviceId: StorageDeviceId,
                                                               toDeviceId: StorageDeviceId): Future[TransferOutcome] =
    updateStateForTransferredStore(storeId, fromDeviceId, toDeviceId)

  /** Testing hook: the stores with a transfer in under way, including one waiting out its retry delay. */
  private[aspen] def testingOnlyTransferringIn: Set[StoreId] =
    synchronized(transferringIn.keySet)

  /** Testing hook: the stores whose failed transfer in is waiting out its retry delay. */
  private[aspen] def testingOnlyScheduledTransferInRetries: Set[StoreId] =
    synchronized(transferInRetries.keySet)

  /** Testing hook: runs every scheduled transfer-in retry now rather than after its delay. */
  private[aspen] def testingOnlyRetryTransferIns(): Unit =
    synchronized(transferInRetries.values.toList).foreach(_.run())

  /** Testing hook: the devices with a re-check waiting out TransferOutRecheckDelay. */
  private[aspen] def testingOnlyScheduledDeviceRechecks: Set[StorageDeviceId] =
    synchronized(scheduledDeviceRechecks)

  /** Testing hook: runs every scheduled device re-check now rather than after its delay. */
  private[aspen] def testingOnlyRunDeviceRechecks(): Unit =
    synchronized(scheduledDeviceRechecks).foreach(runDeviceRecheck)

  /** Testing hook: the storage devices with a state lookup currently in flight. */
  private[aspen] def testingOnlyActiveDeviceChecks: Set[StorageDeviceId] =
    synchronized(activeDeviceChecks)

  /** Testing hook: the stores currently marked offline. */
  private[aspen] def testingOnlyOfflineStores: Set[StoreId] =
    synchronized(offlineStores)

  /** Testing hook: devices with a check request deferred behind an in-flight lookup. */
  private[aspen] def testingOnlyDeferredDeviceChecks: Set[StorageDeviceId] =
    synchronized(deferredDeviceChecks)

  private def handleEvent(event: Event): Unit = synchronized {
    event match {

      case IOCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.frontend.backendOperationComplete(op)
      }

      case TransactionMessage(msg) => stores.get(msg.to) match
        case None => 
          if ! offlineStores.contains(msg.to) then
            val reply = TxUnknownStore(msg.from, msg.to, msg.transactionId)
            net.sendTransactionMessage(reply)
            
        case Some(store) => store.receiveTransactionMessage(msg)
        

      case ClientReq(msg) => stores.get(msg.toStore) match
        case None => msg match
          case r: Read =>
            if ! offlineStores.contains(r.toStore) then
              val msg = ReadResponse(
                r.fromClient,
                r.toStore,
                r.readUUID,
                HLCTimestamp.now,
                Left(ReadError.StoreNotFound)
              )
              net.sendClientResponse(msg)

          case op: OpportunisticRebuild =>
          case s: TransactionCompletionQuery =>

        case Some(store) => msg match
          case r: Read =>
            if r.objectPointer.poolId == store.storeId.poolId then
              store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer, r.readType)

          case op: OpportunisticRebuild => store.frontend.readObjectForOpportunisticRebuild(op)

          case s: TransactionCompletionQuery =>
            val isComplete = txStatusCache.getStatus(s.transactionId) match {
              case None => false
              case Some(e) => e.status match {
                case TransactionStatus.Unresolved => false
                case _ => true
              }
            }
            val r = TransactionCompletionResponse(s.fromClient, s.toStore, s.queryUUID, isComplete)
            net.sendClientResponse(r)

      case HostMsg(msg) => msg match
        case m: StartStoreTransfer => startStoreTransferOut(m)
        case m: CheckStorageDevice =>
          // A device id we do not know may simply be one that has appeared on disk since
          // our last scan -- create-storage-device sends this message as a discovery nudge
          // for exactly that case -- so rescan before concluding the device is not ours.
          if ! storageDevices.contains(m.deviceId) then
            checkForNewDevices()
          checkStorageDevice(m.deviceId)
        case m: ServiceMessage =>
          serviceExecutorPromise.future.foreach(_.deliverMessage(m))
        case m: ExecuteSystemTask =>
          systemTaskRunnerPromise.future.foreach(_.receive(m))
      
      case Repair(storeId, os, completion) => stores.get(storeId) match
        case Some(store) => store.repair(os, completion)
        case None =>
          // The store left between the sweep selecting it and this event being handled. Leaving
          // the promise open would strand the scan that awaits it, and with it the sweep's
          // single-flight slot -- repair would stop host-wide until the process restarts.
          // Failing also leaves the errorTree entry in place for the store's new host.
          logger.debug(s"Ignoring repair of ${os.pointer.id}: store $storeId is not hosted here")
          completion.failure(StoreNotHosted(storeId))

      case RepairDelete(storeId, objectId, storePointer, completion) => stores.get(storeId) match
        case Some(store) => store.repairDelete(objectId, storePointer, completion)
        case None =>
          // Must fail, not succeed. Succeeding would tell the caller the slice was deleted, and
          // the caller would drop the errorTree entry -- leaving the object on the departed
          // store with no record that it needs removing.
          logger.debug(s"Ignoring repair deletion of $objectId: store $storeId is not hosted here")
          completion.failure(StoreNotHosted(storeId))

      case RecoveryEvent() =>
        handleRecoveryEvent()

      case LoadStore(storageDeviceId, backend, maxSliceSize, p) =>
        val store = new Store(storageDeviceId, ec, backend, objectCacheFactory(backend.storeId), net, backgroundTasks, crl,
          txStatusCache,finalizerFactory, txDriverFactory, heartbeatPeriod*8, maxSliceSize)
        backend.setCompletionHandler(ioHandler)
        stores += (backend.storeId -> store)
        offlineStores -= backend.storeId

        if os.exists(backend.crlSaveFile) then
          val (storeId, trs) = CrashRecoveryLog.loadStoreState(backend.crlSaveFile)
          crl.loadStore(storeId, trs).foreach: _ =>
            os.remove(backend.crlSaveFile)
            p.success(())
        else
          p.success(())

      case LoadStoreById(storageDeviceId, storeId) =>
        storageDevices.get(storageDeviceId).foreach: sds =>
          tryLoadStore(sds, sds.devicePath / storeId.directoryName)
        
      case HeartbeatEvent() =>
        //logger.trace("Main loop got heartbeat event")
        stores.valuesIterator.foreach(_.heartbeat())
        if systemTaskRunnerPromise.isCompleted then
          systemTaskRunnerPromise.future.foreach(_.heartbeat())

      case CheckAllDevices() =>
        // Discover first so a device found by this scan is checked within the same event
        // and its Initializing stores are created now rather than a period from now.
        checkForNewDevices()
        storageDevices.valuesIterator.foreach: sds =>
          checkStorageDevice(sds.storageDeviceId)

      case ShutdownMarkerResolved(path, cfg, markerHost, state) =>
        shutdownMarkerResolved(path, cfg, markerHost, state)

      case ScanDevices() =>
        // Local only: listing the directory reads nothing over the network. A device this scan
        // loads is checked at once, as CheckAllDevices would, so its Initializing stores are
        // created now rather than up to checkStorageDevicePeriod from now.
        checkForNewDevices().foreach(checkStorageDevice)

      case InitializeTaskExecutor() =>
        initializeTaskExecutor()

      case ShutdownStore(storeId, completion) =>
        stores.get(storeId) match
          case None => completion.success(())
          case Some(store) =>
            stores -= storeId
            offlineStores += storeId
            store.objectCache.clear()
            storageDevices.get(store.storageDeviceId).foreach: sds =>
              sds.offlineStores += storeId
            crl.closeStore(storeId).foreach: trs =>
              CrashRecoveryLog.saveStoreState(storeId, trs, store.backend.crlSaveFile)
              store.close().foreach: _ =>
                completion.success(())
        
      case null => // nothing to do
      
      case _:Exit =>
        shutdownCalled = true
        shutdownPromise.success(())
    }
  }
}
