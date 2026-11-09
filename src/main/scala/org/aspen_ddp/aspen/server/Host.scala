package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, ObjectState as ClientObjectState}
import org.aspen_ddp.aspen.client.internal.transaction.TracelessCommitEntry
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.compute.{DurableServiceExecutor, TaskExecutor}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory}
import org.aspen_ddp.aspen.server.host.{BackgroundControlScheduler, ClientDeviceMetadata, ControlScheduler, DeviceControl, DeviceMetadata, HostThreading, LocalDevices, LocalStorageDeviceState, LockstepThreading, StoreRebuilds}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.rebuild.StoreRebuildFactory
import org.aspen_ddp.aspen.server.repair.{RepairService, RepairTarget}
import org.aspen_ddp.aspen.server.store.{ReconcileResult, RepairSnapshot, Store, TracelessCommitLookup}
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendOpener, CompletionHandler, DefaultBackendOpener}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import org.aspen_ddp.aspen.server.transfer.StoreTransferService
import scribe.Logging

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try


object Host:
  private[server] sealed abstract class Event

  private[server] case class HostMsg(msg: HostMessage) extends Event
  private[server] case class LoadStore(storageDeviceId: StorageDeviceId, backend: Backend,
                                  maxSliceSize: Option[Long], completion: Promise[Unit]) extends Event
  private[server] case class LoadStoreById(sstorageDeviceId: StorageDeviceId, toreId: StoreId) extends Event
  /** The CRL state of a loaded store failed to load: it is unloaded, if it is still the store installed */
  private[server] case class UnloadFailedStore(storeId: StoreId, store: Store) extends Event
  /** A backend failed-stop (its onFatal). `backend` is set once its open returns; it stays empty if the open
   *  threw. A backend may be opened off the event thread, so the event can be handled before then:
   *  the backend is not served yet, and LoadStore retires it on seeing it failed. */
  private[server] case class BackendFailed(storeId: StoreId, backend: AtomicReference[Backend], err: Throwable) extends Event

  /** The close of a store (ShutdownStore) found its backend failed-stop: the store was marked rebuild-required
   *  rather than closed cleanly, so a caller must not mark it as cleanly closed (transfer-out marker) or ship it. */
  final case class StoreFailedOnClose(storeId: StoreId)
    extends Exception(s"Store $storeId failed on an I/O error while closing; it is marked for rebuild")

  /** A store whose load awaits space: retried once `scansLeft` more device scans have passed. Each failed retry
   *  doubles `waitScans`, up to the cap (SpaceRetryCap in scans). `onServed` is the waiting load's tryLoadStore callback: the next open of the
   *  store (a retry or any other load) takes it over, and it is dropped with the entry if the store leaves the wait
   *  for any other reason. */
  private[server] final case class SpaceWait(sds: LocalStorageDeviceState, storePath: os.Path, waitScans: Int,
                                             scansLeft: Int, onServed: Option[() => Unit] = None)

  /** How long a store's Tx may sit idle before the store drives it itself. This is how a lost TxFinalized is recovered */
  def txHeartbeatTimeout(heartbeatPeriod: Duration): Duration = heartbeatPeriod * 5

  /** Outlasts an idle Tx being noticed, one heartbeat after its timeout, so that the driver then started learns the
    * outcome from the stores' status caches. Without those the stores would treat its prepare as a new transaction
    * and report a committed one as aborted.
    */
  def txStatusCacheDuration(heartbeatPeriod: Duration): FiniteDuration =
    FiniteDuration((heartbeatPeriod * 7).toNanos, NANOSECONDS)

  /** Each tick is a transaction per running system task, so it is not tied to the transaction heartbeat */
  val SystemTaskHeartbeatPeriod: Duration = Duration(5, SECONDS)

  /** Longest wait between load retries of a store whose device is out of space */
  val SpaceRetryCap: Duration = Duration(5, MINUTES)

  private[server] case class Exit() extends Event
  private[server] case class RecoveryEvent() extends Event

  private[server] case class SystemTaskHeartbeatEvent() extends Event
  private[server] case class CheckAllDevices() extends Event
  private[server] case class ScanDevices() extends Event
  /** The StorageDeviceState looked up for a device directory this host does not load on its own: one carrying
   *  `shutdown.state` (`fromShutdownMarker`), or one whose `owning-host` names another host. `previousHost` is the
   *  host that file names. See DeviceDiscovery.handleOwnershipResolved. */
  private[server] case class DeviceOwnershipResolved(path: os.Path,
                                                     cfg: StorageDeviceConfig,
                                                     previousHost: HostId,
                                                     fromShutdownMarker: Boolean,
                                                     state: Try[StorageDeviceState]) extends Event
  /** Closes a store. `extractCrl` (a network transfer out only) moves its CRL state out of the device CRL into its
   *  crl_save.log; otherwise the state stays live in the device CRL. See StoreLoader.handleClose. */
  private[server] case class ShutdownStore(storeId: StoreId, extractCrl: Boolean, completion: Promise[Unit])
    extends Event
  /** A backend open (`create = false`, from tryLoadStore) or create (createNewStore) started by StoreLoader.startOpen
   *  has ended. `previous` is the store's out-of-space wait before the open, for the backoff; `created` is completed
   *  for a create once its store is loaded, or failed; `onServed` is tryLoadStore's, run only if the store is served.
   *  `inline` is set when the open completed in startOpen itself, which then handles it without posting it. */
  private[server] case class BackendOpened(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                                create: Boolean, previous: Option[SpaceWait], result: Try[Backend],
                                created: Option[Promise[Unit]], onServed: Option[() => Unit],
                                inline: Boolean = false) extends Event
  private[server] case class InitializeTaskExecutor() extends Event
  /** Posted by the constructor after its device scan, so it follows every LoadStore that scan queued: from then on
   *  the router answers messages for stores not loaded here (StoreRouter.markReady). */
  private[server] case class StoresReady() extends Event

  /** What Host posts to its threading's control events: one event, handled by handleEvent. Carries the event so the
   *  test hooks can find queued events, and names it in the log line written when its handler throws. */
  private[server] final class EventTask(host: Host, val event: Event) extends HostThreading.Named:
    override def run(): Unit = host.handleEvent(event)
    override def name: String = event.getClass.getSimpleName

  /** Replacements for what Host would otherwise do itself, injected by tests. `None` is the production
   *  implementation, built directly. A `Some` is passed straight to DeviceControl in its place.
   *
   *  Beyond the fields the design names, `wrapMetadata`, `loadStore`, `tryLoadStore` and `loadStoreById` exist
   *  because a recording seam must stand in front of the real thing, which a test cannot build before the Host
   *  exists: `wrapMetadata` receives the metadata Host would have used (so adoption during the Host's own
   *  constructor scan is still seen), and the others replace the DeviceControl callbacks of the same name.
   *  `warn` also receives the directory, as DeviceControl's callback does. `wrapIoHandler` wraps each backend's own
   *  completion handler. */
  final case class Seams(metadata: Option[DeviceMetadata] = None,
                         wrapMetadata: Option[DeviceMetadata => DeviceMetadata] = None,
                         scheduler: Option[ControlScheduler] = None,
                         backends: Option[BackendOpener] = None,
                         warn: Option[(os.Path, String) => Unit] = None,
                         closeForTransferOut: Option[StoreId => Future[Unit]] = None,
                         closeForShutdown: Option[StoreId => Future[Unit]] = None,
                         gossipStoreOnline: Option[StoreId => Unit] = None,
                         loadStore: Option[(StorageDeviceId, Backend, Option[Long]) => Future[Unit]] = None,
                         tryLoadStore: Option[(LocalStorageDeviceState, os.Path, Option[() => Unit]) => Unit] = None,
                         loadStoreById: Option[(StorageDeviceId, StoreId) => Unit] = None,
                         scheduleShutdownWorkTimeout: Option[(StorageDeviceId, () => Unit) => Unit] = None,
                         wrapIoHandler: Option[CompletionHandler => CompletionHandler] = None,
                         /** The CRL of stores that loadStore loads onto a device this host has not loaded, for tests
                          *  that load stores directly. Without it such a load fails: a store uses its device's CRL. */
                         unloadedDeviceCrl: Option[CrashRecoveryLog] = None,
                         /** Writes a store directory's rebuild-required marker (StoreRebuilds.markRebuildRequired), for
                          *  tests of a write that fails and leaves nothing behind */
                         writeRebuildRequiredMarker: Option[os.Path => Unit] = None)

  object Seams:
    val default: Seams = Seams()

  /** How long after a transfer out begins, or is found still sending, its source device is
   *  checked again. See scheduleDeviceRecheck. */
  val TransferOutRecheckDelay: Duration = Duration(10, SECONDS)


class Host(val client: AspenClient,
                   val hostId: HostId,
                   val aspenSystemId: UUID,
                   val rootDir: os.Path,
                   val ec: ExecutionContext,
                   /** Where the host's events and stores run. See HostThreading. */
                   val threading: HostThreading,
                   val objectCacheFactory: (StoreId, ExecutionContext) => ObjectCache,
                   val net: Messenger,
                   val backgroundTasks: BackgroundTaskManager,
                   /** Opens each storage device's CRL as the device loads */
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
                   val deviceScanPeriod: Duration = Duration(10, SECONDS),
                   /** How long a device shutdown waits for the work it cancelled -- rebuilds,
                    *  transfer-in pool updates already in flight -- to stop before it closes the
                    *  device's stores anyway. See awaitShutdownWork. */
                   val shutdownWorkTimeout: Duration = Duration(60, SECONDS),
                   /** What tests replace. See Host.Seams. */
                   seams: Host.Seams = Host.Seams.default) extends Logging with RepairTarget {
  import Host._

  private val txStatusCache = new TransactionStatusCache(Host.txStatusCacheDuration(heartbeatPeriod))

  private val localDevices = new LocalDevices()

  // It takes the DeviceControl below, whose control context its callbacks run on, but only uses it once built.
  private val clientMetadata = new ClientDeviceMetadata(deviceControl)

  // A delay expires on a BackgroundTaskManager thread; its function is posted to the control thread.
  private val controlScheduler: ControlScheduler = seams.scheduler.getOrElse(
    new BackgroundControlScheduler(backgroundTasks, fn => threading.controlEvents.execute(() => fn())))

  private val hostMetadata: DeviceMetadata = seams.metadata.getOrElse(clientMetadata)

  /** Stores have no client, so a recovered commit vote's TracelessCommits lookup reads its pool's tree through the
   *  host's, on `ec`. The answer is posted to the store's executor, and dropped if the store is gone by then. */
  private val tracelessLookup: TracelessCommitLookup = (storeId, txid, requestId) =>
    given ExecutionContext = ec
    Future.unit
      .flatMap(_ => client.getStoragePool(storeId.poolId))
      .flatMap(_.tracelessCommitsTree.get(TracelessCommitEntry.key(storeId, txid)))
      .onComplete: result =>
        deviceControl.router.post(storeId)(_.frontend.tracelessLookupComplete(txid, requestId,
          result.map(_.isDefined)))

  private val deviceControl: DeviceControl = new DeviceControl(
    metadata = seams.wrapMetadata.fold(hostMetadata)(wrap => wrap(hostMetadata)),
    scheduler = controlScheduler,
    backends = seams.backends.getOrElse(new DefaultBackendOpener(ec)),
    devices = localDevices,
    client = client,
    hostId = hostId,
    aspenSystemId = aspenSystemId,
    rootDir = rootDir,
    net = net,
    backgroundTasks = backgroundTasks,
    threading = threading,
    post = event => Host.this.post(event),
    crlFactory = crlFactory,
    unloadedDeviceCrl = seams.unloadedDeviceCrl,
    txStatusCache = txStatusCache,
    wrapCompletionHandler = seams.wrapIoHandler.getOrElse(identity),
    objectCacheFactory = objectCacheFactory,
    finalizerFactory = finalizerFactory,
    txDriverFactory = txDriverFactory,
    heartbeatPeriod = heartbeatPeriod,
    deviceScanPeriod = deviceScanPeriod,
    storeRebuildFactory = storeRebuildFactory,
    maxConcurrentRebuilds = maxConcurrentRebuilds,
    loadStore = seams.loadStore.getOrElse(
      (deviceId, backend, maxSliceSize) => Host.this.loadStore(deviceId, backend, maxSliceSize)),
    tryLoadStore = seams.tryLoadStore.getOrElse(
      (sds, path, onServed) => deviceControl.loader.tryLoadStore(sds, path, onServed)),
    loadStoreById = seams.loadStoreById.getOrElse((deviceId, storeId) => Host.this.loadStoreById(deviceId, storeId)),
    warn = seams.warn.getOrElse((_, msg) => logger.warn(msg)),
    storeTransferService = storeTransferService,
    shutdownWorkTimeout = shutdownWorkTimeout,
    gossipStoreOnline = seams.gossipStoreOnline.getOrElse(storeId => deviceControl.services.gossipStoreOnline(storeId)),
    closeStoreForTransferOut = seams.closeForTransferOut.getOrElse(
      storeId => deviceControl.transfers.closeForTransferOut(storeId)),
    closeStoreForShutdown = seams.closeForShutdown.getOrElse(
      storeId => deviceControl.shutdown.closeForShutdown(storeId)),
    scheduleShutdownWorkTimeout = seams.scheduleShutdownWorkTimeout.getOrElse(
      (_, fn) => controlScheduler.schedule(shutdownWorkTimeout)(fn())),
    scheduleEndpointRetry = fn => controlScheduler.schedule(Duration(5, SECONDS))(fn()),
    ec = ec,
    writeRebuildRequiredMarker = seams.writeRebuildRequiredMarker.getOrElse(StoreRebuilds.writeRebuildRequiredMarker),
    tracelessLookup = tracelessLookup)

  private val shutdownPromise: Promise[Unit] = Promise()

  val storageDevicesDir: os.Path = rootDir / StorageDeviceManager.StorageDevicesDirName

  /** Posts `task` to each loaded store's executor, where it runs unless that store's close has started. Call on the
   *  control thread (from handleRecoveryEvent). */
  protected def onEachStore(task: Store => Unit): Unit = deviceControl.router.onEachStore(task)

  private def reconciler = deviceControl.reconciler

  // Constructed here, as the retransmit task it schedules starts with the Host.
  private val services = deviceControl.services

  /** All StoreMoved and HostEndpointChanged gossip goes through here. See HostServices. */
  private[aspen] def gossipRetransmitter: GossipRetransmitter = services.gossipRetransmitter

  // Fires on a BackgroundTaskManager thread; the retransmit reads control state, so it is posted to the control thread
  // Re-sends the StartStoreTransfer of every transfer in that has not yet completed and was last sent more than
  // 30 s ago. See StoreTransfers.retransmitPendingStarts.
  private val pendingStartTask = backgroundTasks.schedulePeriodic(Duration(30, SECONDS)):
    threading.controlEvents.execute(() => transfers.retransmitPendingStarts())

  private def transfers = deviceControl.transfers

  // One dispatch step that posts each store's heartbeat to its executor
  private val heartbeatTask = backgroundTasks.schedulePeriodic(heartbeatPeriod) {
    deviceControl.router.heartbeat()
  }

  private val systemTaskHeartbeatTask = backgroundTasks.schedulePeriodic(Host.SystemTaskHeartbeatPeriod) {
    post(SystemTaskHeartbeatEvent())
  }
  
  private val checkStorageDeviceTask = backgroundTasks.schedulePeriodic(checkStorageDevicePeriod) {
    post(CheckAllDevices())
  }

  private val deviceScanTask = backgroundTasks.schedulePeriodic(deviceScanPeriod) {
    post(ScanDevices())
  }

  /** Continual repair of this host's stores. Constructed here rather than after task-executor
    * initialization: early sweeps simply find few stores, and RepairService falls back to
    * default policy for metadata that is not readable yet.
    */
  private val repairService = new RepairService(client, hostId, this, backgroundTasks)(using ec)

  deviceControl.discovery.checkForNewDevices()

  // After the LoadStores the scan queued: until it is handled, messages for stores not loaded get no reply
  post(StoresReady())

  // After we've loaded all the stores, initiate an initial check of all our
  // devices in case operations were preformed while we were down and missed
  // the CheckDeviceState messages
  post(CheckAllDevices())
  post(InitializeTaskExecutor())

  def getTaskExecutor(): Future[TaskExecutor] = services.getTaskExecutor()

  def getServiceExecutor(): Future[DurableServiceExecutor] = services.getServiceExecutor()

  /** Starts running the host's events. Until then they wait. */
  def start(): Unit = threading.start()

  /** Posts one event to the threading's control events. Store-bound work never comes through here: the hot path
   *  goes straight to the StoreRouter, which takes the dispatch hop to each store's executor. */
  private def post(event: Event): Unit =
    threading.controlEvents.execute(EventTask(this, event))

  /** Callable from any thread: reads the published HostStoreView. */
  def getDevicePath(storageDeviceId: StorageDeviceId): Option[os.Path] =
    deviceControl.currentView.devicePaths.get(storageDeviceId)

  /** Makes this host's HostState endpoint match the configured one. See HostServices.reconcileEndpoint. */
  def reconcileEndpoint(address: String,
                        dataPort: Int,
                        httpPort: Int,
                        storeTransferPort: Int): Future[Boolean] =
    services.reconcileEndpoint(address, dataPort, httpPort, storeTransferPort)

  // containsStore, getStoreIds, repairableStoreIds and isRepairable are called from other threads (the repair
  // service, the network bridge, RunningHost.stop), so each reads the published HostStoreView.

  def containsStore(storeId: StoreId): Boolean =
    deviceControl.currentView.served.contains(storeId)

  def getStoreIds: List[StoreId] = deviceControl.currentView.served.toList

  /** Stores this host can currently repair. A store mid-rebuild is already having everything
    * read, and a store transferring out is about to stop being ours; scanning either wastes the
    * load budget on work that will be redone or discarded.
    */
  def repairableStoreIds: List[StoreId] = deviceControl.currentView.repairable.toList

  /** O(1) per-entry counterpart to `repairableStoreIds`: whether the given store is currently
    * repairable on this host. Must agree with `repairableStoreIds.contains(storeId)` exactly, which it does by
    * reading the same snapshot.
    */
  def isRepairable(storeId: StoreId): Boolean = deviceControl.currentView.repairable.contains(storeId)

  /** Called from TestNetwork's checker thread: reads the router's concurrent map. */
  def hasTransactions: Boolean = deviceControl.router.hasTransactions

  def logTransactionStatus(log: String => Unit): Unit = deviceControl.router.logTransactionStatus(log)

  /** @param maxSliceSize the store's effective slice size limit, from its StoreConfig. None means unlimited. */
  def loadStore(storageDeviceId: StorageDeviceId, backend: Backend, maxSliceSize: Option[Long]): Future[Unit] = {
    val p = Promise[Unit]()
    post(LoadStore(storageDeviceId, backend, maxSliceSize, p))
    p.future
  }

  def loadStoreById(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    post(LoadStoreById(storageDeviceId, storeId))

  // The hot path: called on network threads, routed straight to the store's executor (see StoreRouter)

  def receiveTransactionMessage(msg: TxMessage): Unit =
    deviceControl.router.deliverTx(msg)

  def receiveClientRequest(msg: ClientRequest): Unit =
    deviceControl.router.deliverClient(msg)

  def receiveHostMessage(msg: HostMessage): Unit =
    post(HostMsg(msg))

  /** Called from the repair scan's thread: reads the router's concurrent map and the store's volatile snapshots */
  def holdsTransaction(storeId: StoreId, txid: TransactionId): Boolean =
    deviceControl.router.holdsTransaction(storeId, txid)

  def snapshot(storeId: StoreId, pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
    deviceControl.router.snapshotForRepair(storeId, pointer, p)

  def reconcile(storeId: StoreId, snapshot: RepairSnapshot, read: ClientObjectState,
                p: Promise[ReconcileResult]): Unit =
    deviceControl.router.reconcileForRepair(storeId, snapshot, read, p)

  /** Repairs a missed deletion on the given store. Used when the object is gone from the rest
    * of the system, leaving only the ObjectId and the storePointer bytes recorded in the pool's
    * errorTree to identify it.
    */
  def repairDelete(storeId: StoreId,
                   objectId: ObjectId,
                   storePointer: Array[Byte],
                   completion: Promise[Unit]): Unit =
    deviceControl.router.repairDelete(storeId, objectId, storePointer, completion)

  def shutdown()(using ec: ExecutionContext): Future[Unit] = {
    post(Exit())
    // Lets the control thread exit once it has handled Exit; it is not a daemon thread.
    threading.shutdown()
    services.shutdown()
    pendingStartTask.cancel()
    heartbeatTask.cancel()
    systemTaskHeartbeatTask.cancel()
    checkStorageDeviceTask.cancel()
    deviceScanTask.cancel()
    repairService.cancel()
    shutdownPromise.future
  }
  
  private[aspen] def closeStore(storeId: StoreId): Future[Unit] =
    deviceControl.loader.closeStore(storeId, extractCrl = false)

  protected def addRecoveryEvent(): Unit = post(RecoveryEvent())

  /** Placeholder for mixin class to implement transaction recovery */
  protected def handleRecoveryEvent(): Unit = ()

  /** Lockstep threading: runs all posted work, returning when none is left. Any other threading runs its own work,
   *  so this returns at once, doing nothing. */
  def testingOnlyHandleEvents(): Unit = threading match
    case t: LockstepThreading => t.drain()
    case _ => ()

  /** The components, for tests. */
  private[aspen] def control: DeviceControl = deviceControl

  /** Runs on the control thread (lockstep: the draining test thread). */
  private def handleEvent(event: Event): Unit =
    event match {

      case HostMsg(msg) => msg match
        case m: StartStoreTransfer => transfers.startOut(m)
        case m: CheckStorageDevice => reconciler.handleCheckMessage(m)
        case m: (ServiceMessage | ExecuteSystemTask) => services.deliver(m)
      
      case RecoveryEvent() =>
        handleRecoveryEvent()

      case e: LoadStore =>
        deviceControl.loader.handleLoad(e)

      case e: BackendFailed =>
        deviceControl.rebuilds.handleBackendFailed(e)

      case e: BackendOpened =>
        deviceControl.loader.handleBackendOpened(e)

      case e: UnloadFailedStore =>
        deviceControl.loader.handleUnloadFailed(e)

      case e: LoadStoreById =>
        deviceControl.loader.handleLoadById(e)

      case SystemTaskHeartbeatEvent() =>
        services.systemTaskHeartbeat()

      case CheckAllDevices() => reconciler.checkAll()

      case e: DeviceOwnershipResolved =>
        deviceControl.discovery.handleOwnershipResolved(e)

      case ScanDevices() => reconciler.scan()

      case StoresReady() =>
        deviceControl.router.markReady()

      case InitializeTaskExecutor() =>
        services.initializeTaskExecutor()

      case e: ShutdownStore =>
        deviceControl.loader.handleClose(e)
        
      case null => // nothing to do
      
      case _:Exit =>
        shutdownPromise.success(())
    }
}
