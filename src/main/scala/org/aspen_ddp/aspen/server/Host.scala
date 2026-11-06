package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, StoragePool, Transaction, ObjectState as ClientObjectState}
import org.aspen_ddp.aspen.server.host.{AdoptionClass, ClientDeviceMetadata, DefaultBackendOpener, DeviceControl, DeviceMetadata, ControlScheduler, LocalDevices}
import org.aspen_ddp.aspen.server.repair.{RepairService, RepairTarget, StoreNotHosted}

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, runSequentially}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, Completion, CompletionHandler, FsifConfig, RocksDBConfig}
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifIo, FsifLayout, FsifRecovery}
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
import java.util.concurrent.atomic.AtomicReference
import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object Host:
  private[server] sealed abstract class Event

  private[server] case class IOCompletion(op: Completion) extends Event
  private[server] case class TransactionMessage(msg: TxMessage) extends Event
  private[server] case class ClientReq(msg: ClientRequest) extends Event
  private[server] case class HostMsg(msg: HostMessage) extends Event
  private[server] case class Repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]) extends Event
  private[server] case class RepairDelete(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte],
                                  completion: Promise[Unit]) extends Event:
    override def equals(other: Any): Boolean = other match
      case that: RepairDelete => storeId == that.storeId && objectId == that.objectId &&
        java.util.Arrays.equals(storePointer, that.storePointer) && completion == that.completion
      case _ => false

    override def hashCode: Int = (storeId, objectId, java.util.Arrays.hashCode(storePointer), completion).##
  private[server] case class LoadStore(storageDeviceId: StorageDeviceId, backend: Backend,
                                  maxSliceSize: Option[Long], completion: Promise[Unit]) extends Event
  private[server] case class LoadStoreById(sstorageDeviceId: StorageDeviceId, toreId: StoreId) extends Event
  private[server] case class UnloadFailedStore(storeId: StoreId, store: Store) extends Event
  /** The CRL holds the state loaded from a store's crl_save.log: its transactions can be recovered */
  private[server] case class StoreCrlLoaded(storeId: StoreId, store: Store, saveFile: os.Path, p: Promise[Unit]) extends Event
  /** An FSIF backend failed-stop (its onFatal). `backend` is set once its constructor returns; it stays empty if the
   *  constructor threw. An FSIF backend is opened off the event thread, so the event can be handled before then:
   *  the backend is not served yet, and LoadStore retires it on seeing it failed. */
  private[server] case class BackendFailed(storeId: StoreId, backend: AtomicReference[Backend], err: Throwable) extends Event

  /** The close of a store (ShutdownStore) found its backend failed-stop: the store was marked rebuild-required
   *  rather than closed cleanly, so a caller must not mark it as cleanly closed (transfer-out marker) or ship it. */
  final case class StoreFailedOnClose(storeId: StoreId)
    extends Exception(s"Store $storeId failed on an I/O error while closing; it is marked for rebuild")

  /** An FSIF store waiting for space to load: retried once `scansLeft` more device scans have passed. Each failed
   *  retry doubles `waitScans`, up to the cap (SpaceRetryCap in scans). */
  /** A store whose load awaits space. `onServed` is the waiting load's tryLoadStore callback: the next open of the
   *  store (a retry or any other load) takes it over, and it is dropped with the entry if the store leaves the wait
   *  for any other reason. */
  private[server] final case class SpaceWait(sds: LocalStorageDeviceState, storePath: os.Path, waitScans: Int, scansLeft: Int,
                                     onServed: Option[() => Unit] = None)

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

  /** Host-wide pool for FSIF opens and creates (recovery reads and fsyncs whole stores): cached daemon threads
   *  named fsif-open-N */
  private[server] lazy val FsifOpenEc: ExecutionContext =
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool: r =>
      val t = new Thread(r, s"fsif-open-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t)
  private[server] case class Exit() extends Event
  private[server] case class RecoveryEvent() extends Event
  private[server] case class HeartbeatEvent() extends Event

  private[server] case class SystemTaskHeartbeatEvent() extends Event
  private[server] case class CheckAllDevices() extends Event
  private[server] case class ScanDevices() extends Event
  private[server] case class ShutdownMarkerResolved(path: os.Path,
                                            cfg: StorageDeviceConfig,
                                            markerHost: HostId,
                                            state: Try[StorageDeviceState]) extends Event
  private[server] case class ShutdownStore(storeId: StoreId, completion: Promise[Unit]) extends Event
  /** An FSIF open (`create = false`, from tryLoadStore) or create (createNewStore) run on fsifOpenEc has ended.
   *  `previous` is the store's out-of-space wait before the open, for the backoff; `created` is completed for a
   *  create once its store is loaded, or failed; `onServed` is tryLoadStore's, run only if the store is served. */
  private[server] case class FsifOpened(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                                create: Boolean, previous: Option[SpaceWait], result: Try[Backend],
                                created: Option[Promise[Unit]], onServed: Option[() => Unit]) extends Event
  private[server] case class InitializeTaskExecutor() extends Event

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

    /** The destination device now names another host: it was shut down and adopted while this
     *  host's pool update was still in flight. Nothing was written. The arrived copy is left as
     *  it is, behind its transferred-in marker, for the owner's reconcile to finish: its
     *  TransferringIn pass finds the promoted copy and runs this update itself. */
    case NotOwned

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

    /** The device now names another host: it was shut down and adopted while the rebuild was
     *  finishing. Nothing was written and the rebuilt copy is left in place. The owner's
     *  reconcile restarts the rebuild, which finds the store already at its final path and only
     *  flips the entry. */
    case NotOwned

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


  /** What adoptDevice decided, from the device state it re-read inside its transaction. */
  private[aspen] enum AdoptOutcome:
    /** Committed. `state` is exactly what was written, for the post-commit steps to act on. */
    case Adopted(state: StorageDeviceState)

    /** The device is no longer Shutdown and owned by the previous host. Nothing was written. */
    case Changed

    /** These Offline stores were not closed cleanly on disk (adoptionClass). Nothing was
     *  written. */
    case Unclean(stores: List[StoreId])

  class IOHandler(host: Host) extends CompletionHandler:
    override def complete(op: Completion): Unit =
      host.events.add(IOCompletion(op))

  type LocalStorageDeviceState = org.aspen_ddp.aspen.server.host.LocalStorageDeviceState

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
                   val deviceScanPeriod: Duration = Duration(10, SECONDS),
                   /** How long a device shutdown waits for the work it cancelled -- rebuilds,
                    *  transfer-in pool updates already in flight -- to stop before it closes the
                    *  device's stores anyway. See awaitShutdownWork. */
                   val shutdownWorkTimeout: Duration = Duration(60, SECONDS)) extends Logging with RepairTarget {
  import Host._
  
  given ExecutionContext = ec

  private val events = new LinkedBlockingQueue[Event]()

  private val ioHandler = new IOHandler(this)

  private val txStatusCache = new TransactionStatusCache(Host.txStatusCacheDuration(heartbeatPeriod))

  private val crl = crlFactory.createCRL()

  private val localDevices = new LocalDevices(this)

  private val clientMetadata = new ClientDeviceMetadata(client, hostId)

  // Holds the real FSIF open and thread pool. The seams below are adapters that call back into this Host's
  // protected methods, so test subclasses that override them still take effect.
  private val defaultBackends = new DefaultBackendOpener(ec)

  private val deviceControl = new DeviceControl(
    metadata = new DeviceMetadata:
      override def updateStateForTransferredStore(storeId: StoreId, fromDeviceId: StorageDeviceId,
                                                  toDeviceid: StorageDeviceId,
                                                  gossipRetransmitter: GossipRetransmitter) =
        Host.this.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid)
      override def readHostStateForEndpoint() = Host.this.readHostStateForEndpoint()
      override def abortLostTransfer(storeId: StoreId) = Host.this.abortLostTransfer(storeId)
      override def markRebuiltStoreActive(storageDeviceId: StorageDeviceId, storeId: StoreId,
                                          gossipStoreOnline: StoreId => Unit) =
        Host.this.markRebuiltStoreActive(storageDeviceId, storeId)
      override def adoptDevice(storageDeviceId: StorageDeviceId, previousHostId: HostId, devicePath: os.Path,
                               uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                               crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                               markRebuildRequired: os.Path => Unit) =
        Host.this.adoptDevice(storageDeviceId, previousHostId, devicePath)
      override def lookupStorageDeviceState(storageDeviceId: StorageDeviceId) =
        Host.this.lookupStorageDeviceState(storageDeviceId)
      override def readTransferSourceState(fromDeviceId: StorageDeviceId) =
        Host.this.readTransferSourceState(fromDeviceId)
      override def completeDeviceShutdown(storageDeviceId: StorageDeviceId) =
        Host.this.completeDeviceShutdown(storageDeviceId)
    ,
    // Not yet posted to the control thread: until the extracted classes take over scheduling, a delay runs its
    // function where the BackgroundTaskManager runs it, as before.
    scheduler = new ControlScheduler:
      override def schedule(delay: Duration)(fn: => Unit): ScheduledTask = backgroundTasks.schedule(delay)(fn)
    ,
    backends = new DefaultBackendOpener(ec):
      override def openFsifBackend(storeId: StoreId, storePath: os.Path, layout: FsifLayout,
                                   onFatal: Throwable => Unit) =
        Host.this.openFsifBackend(storeId, storePath, layout, onFatal)
      override def fsifOpenEc = Host.this.fsifOpenEc
    ,
    devices = localDevices,
    client = client,
    hostId = hostId,
    aspenSystemId = aspenSystemId,
    rootDir = rootDir,
    net = net,
    backgroundTasks = backgroundTasks,
    host = this,
    post = event => events.put(event),
    crl = crl,
    txStatusCache = txStatusCache,
    ioHandler = ioHandler,
    objectCacheFactory = objectCacheFactory,
    finalizerFactory = finalizerFactory,
    txDriverFactory = txDriverFactory,
    heartbeatPeriod = heartbeatPeriod,
    deviceScanPeriod = deviceScanPeriod,
    loadStore = (deviceId, backend, maxSliceSize) => Host.this.loadStore(deviceId, backend, maxSliceSize),
    tryLoadStore = (sds, path, onServed) => Host.this.tryLoadStore(sds, path, onServed),
    warn = (path, msg) => Host.this.logDeviceDirWarning(path, msg))

  private val threadPool = Executors.newFixedThreadPool(1)

  protected var shutdownCalled = false
  private val shutdownPromise: Promise[Unit] = Promise()

  val storageDevicesDir: os.Path = rootDir / StorageDeviceManager.StorageDevicesDirName

  // tryLoadDevice is the only writer that adds entries in production. Entries are removed when a
  // device is shut down and unloaded, so a lookup miss means a device that is not loaded right
  // now: one never seen, one rejected by the load decision, or one unloaded by a shutdown. Tests
  // inject entries directly via RecordingHost.injectLoadedDevice.
  protected def storageDevices: Map[StorageDeviceId, LocalStorageDeviceState] = localDevices.devices
  protected def storageDevices_=(devices: Map[StorageDeviceId, LocalStorageDeviceState]): Unit =
    localDevices.devices = devices

  protected def stores: Map[StoreId, Store] = deviceControl.router.loaded

  private var creatingStores: Set[StoreId] = Set()

  private var rebuildingStores: Map[StoreId, StoreRebuild] = Map()
  // Completed when the rebuild of each store in rebuildingStores has released its slot, its
  // outcome acted on. What a shutdown waits on for the rebuilds it cancels.
  private var rebuildsSettled: Map[StoreId, Promise[Unit]] = Map()

  /** Stores whose requestStoreRebuild transaction is in flight, so a reconcile meanwhile does
   *  not start a second one. */
  private var pendingRebuildRequests: Set[StoreId] = Set()
  private var activeDeviceChecks: Set[StorageDeviceId] = Set()
  // Devices with a re-check waiting out TransferOutRecheckDelay. At most one each.
  private var scheduledDeviceRechecks: Set[StorageDeviceId] = Set()
  private var deferredDeviceChecks: Set[StorageDeviceId] = Set()

  // Devices whose shutDownDevice sequence is running. One at a time per device: every check of a
  // ShuttingDown device calls shutDownDevice, and the sequence spans several asynchronous steps,
  // so without this a nudge or the hourly check arriving mid-shutdown would close stores, write
  // markers and transact the device state a second time in parallel with the first.
  private[server] var shuttingDownDevices: Set[StorageDeviceId] = Set()

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

  private def transfers = deviceControl.transfers

  /** Re-sends the StartStoreTransfer of every transfer in that has not yet completed and was last
   *  sent more than 30 s ago. `force` skips the age check so tests need not wait it out. See
   *  StoreTransfers.retransmitPendingStarts. */
  private[aspen] def retransmitPendingStartTransfers(force: Boolean = false): Unit =
    transfers.retransmitPendingStarts(force)

  private val heartbeatTask = backgroundTasks.schedulePeriodic(heartbeatPeriod) {
    events.put(HeartbeatEvent())
  }

  private val systemTaskHeartbeatTask = backgroundTasks.schedulePeriodic(Host.SystemTaskHeartbeatPeriod) {
    events.put(SystemTaskHeartbeatEvent())
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

  deviceControl.discovery.checkForNewDevices()

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

  // protected so tests can subclass with a recording implementation instead of opening a
  // real RocksDB backend. `stores` and `storageDevices` are already protected.
  //
  // protected so tests can subclass with a recording implementation instead of opening a
  // real RocksDB backend. `stores` and `storageDevices` are already protected.
  // protected so a test subclass can count what warnOnce lets through.
  protected def logDeviceDirWarning(path: os.Path, msg: String): Unit = logger.warn(msg)

  protected def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: os.Path,
                             onServed: Option[() => Unit] = None): Unit =
    deviceControl.loader.tryLoadStore(sds, potentialStoreFile, onServed)

  /** Where FSIF stores are opened (recovered) and created, off the event thread. protected as a test seam. */
  protected def fsifOpenEc: ExecutionContext = defaultBackends.fsifOpenEc

  /** Opens an existing FSIF store (FsifBackend.open: a directory without the marker is initialized only if its
   *  files are blank, otherwise recovered). protected as a test seam: a subclass can fail the open or keep
   *  `onFatal`. */
  protected def openFsifBackend(storeId: StoreId,
                                storePath: os.Path,
                                layout: FsifLayout,
                                onFatal: Throwable => Unit): Backend =
    defaultBackends.openFsifBackend(storeId, storePath, layout, onFatal)

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
    localDevices.devicePath(storageDeviceId)
  }

  /** Records the outcome of a store transfer that has finished shipping bytes. See
   *  ClientDeviceMetadata.updateStateForTransferredStore.
   *
   *  protected so a test subclass can observe the returned Future; the production caller is
   *  StoreTransfers.startIn.
   */
  protected def updateStateForTransferredStore(storeId: StoreId,
                                               fromDeviceId: StorageDeviceId,
                                               toDeviceid: StorageDeviceId): Future[TransferOutcome] =
    clientMetadata.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid, gossipRetransmitter)

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
  protected def readHostStateForEndpoint(): Future[HostState] = clientMetadata.readHostStateForEndpoint()

  /** Test seam: runs a failed reconcileEndpoint attempt's retry after the retry delay. */
  protected def scheduleEndpointRetry(fn: => Unit): Unit =
    backgroundTasks.schedule(Duration(5, SECONDS))(fn)

  private def discardStoreDirectory(storeId: StoreId,
                                    deviceId: StorageDeviceId,
                                    reason: String): Unit =
    localDevices.discardStoreDirectory(storeId, deviceId, reason)

  /** The abort returnLostTransferToRebuild issues. A seam for tests. */
  protected def abortLostTransfer(storeId: StoreId): Future[Boolean] = clientMetadata.abortLostTransfer(storeId)

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
              val backendCfg = pstate.backendConfig match
                case cfg: RocksDBConfig => StoreConfig.RocksDB(cfg.maxSliceSize)
                case cfg: FsifConfig => StoreConfig.Fsif(cfg.segmentPayloadSize, cfg.segmentCount)
              val storeConfig = StoreConfig(storeId, backendCfg, pstate.maxSliceSize)

              os.makeDir.all(storePath)
              os.write.over(storePath / StoreConfig.configFilename, storeConfig.yamlConfig)

              if deviceControl.loader.isFsif(storeConfig) then
                // Initialized on fsifOpenEc; startFsifOpen records the store in loadedStores now, so a
                // shutdown of this device waits for the create and then closes the store. Out of space is
                // handed to the load retries (fsifOpened).
                val created = Promise[Unit]()
                deviceControl.loader.startFsifOpen(local, storeConfig, storePath, create = true, Some(created))
                created.future.map(_ => true)
              else
                val backend = deviceControl.loader.openBackend(storeConfig, storePath, create = true)
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
         !shuttingDownDevices.contains(local.storageDeviceId) && discardLostStore(local, storeId) then
        val sr = storeRebuildFactory.createRebuild(
          client, storeId, local.storageDeviceId, local.devicePath)

        val settled = Promise[Unit]()

        def release(): Unit = synchronized:
          rebuildingStores -= storeId
          if rebuildsSettled.get(storeId).exists(_ eq settled) then
            rebuildsSettled -= storeId
          settled.trySuccess(())

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
          rebuildsSettled += storeId -> settled

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

              case Success(RebuildOutcome.NotOwned) =>
                // The device is another host's now; the copy is left for it. Only the other
                // devices are worth re-checking: this one is not ours to reconcile.
                try
                  logger.info(s"Rebuild of store $storeId finished after device " +
                              s"${local.storageDeviceId} moved to another host. Leaving it to that host")
                finally
                  release()
                  recheckOtherDevices()

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

  /** Decides whether a rebuilt store should be adopted or discarded. See
   *  ClientDeviceMetadata.markRebuiltStoreActive. Protected as a test seam. */
  protected def markRebuiltStoreActive(storageDeviceId: StorageDeviceId,
                                       storeId: StoreId): Future[RebuildOutcome] =
    clientMetadata.markRebuiltStoreActive(storageDeviceId, storeId, gossipStoreOnline)

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
    deviceControl.router.markOffline(storeId)
    if storageDevices.get(local.storageDeviceId).contains(local) then
      local.offlineStores += storeId
    val storePath = local.devicePath / storeId.directoryName
    try
      DurableFiles.write(storePath / StoreTransfer.TransferOutMarkerFile, "")
      if ! os.exists(Backend.crlSaveFile(storePath)) then
        CrashRecoveryLog.saveStoreState(storeId, Nil, Backend.crlSaveFile(storePath))
    catch
      case t: Throwable =>
        // Offline for the life of this process either way. A restart cancelling the shutdown
        // loses the abandoned-transfer-out pass's reinstatement without the marker, so the store
        // waits for this host's next start to load; an adopter refuses the device.
        logger.error(s"Failed to mark rebuilt store $storeId offline at $storePath: $t")
  }

  /** Writes the marker that keeps the store at `storePath` from ever being loaded again: its CRL
   *  save file is unusable and was the only copy of its transaction state. */
  private[server] def markRebuildRequired(storePath: os.Path): Unit =
    DurableFiles.write(storePath / RebuildingStore.RebuildRequiredMarkerFile, "")

  /** The LoadStore handler found `backend`'s CRL save file unusable. The file sits in the store's
   *  own directory, so it is the only copy of that state: the store is permanently failed. It
   *  is closed unserved, marked, and left offline; the reconcile then flips its entry to
   *  Rebuilding, and the rebuild discards the directory.
   *
   *  A store arriving over the network never reaches here with a bad file: its staged copy is
   *  checked before promotion, while the source still holds the state.
   *
   *  Caller holds the instance lock.
   */
  private[server] def crlSaveFileLost(storageDeviceId: StorageDeviceId,
                              backend: Backend,
                              err: Throwable): Unit =
    logger.error(s"Store ${backend.storeId} cannot be loaded: its CRL save file is unusable and its transaction " +
                 s"state is lost. Rebuilding it. Error: $err")
    retireUnservedStore(storageDeviceId, backend)

  /** crlSaveFileLost's steps, for any store that must never serve: closes `backend` unserved, marks the store
   *  rebuild-required, leaves it offline, and checks the device so the reconcile flips its entry to Rebuilding.
   *  The close is not awaited. Caller holds the instance lock. */
  private[server] def retireUnservedStore(storageDeviceId: StorageDeviceId, backend: Backend): Unit =
    val storeId = backend.storeId
    deviceControl.router.markOffline(storeId)
    storageDevices.get(storageDeviceId).foreach: sds =>
      sds.loadedStores -= storeId
      sds.offlineStores += storeId
    backend.close()
    try
      markRebuildRequired(backend.path)
    catch
      case t: Throwable =>
        // Offline for the life of this process either way. The next start finds the same
        // unusable state and lands here again.
        logger.error(s"Failed to mark store $storeId for rebuild at ${backend.path}: $t")
    checkStorageDevice(storageDeviceId)

  /** A store closed by ShutdownStore whose backend failed-stop meanwhile: its page-cache state can't be trusted, so
   *  it is rebuilt rather than treated as cleanly closed. Marked rebuild-required, and the CRL save file the close
   *  just wrote is removed so nothing takes the directory for a clean close; it is dropped from loadedStores so a
   *  shutdown retry does not pick it again. Caller holds the instance lock. */
  private[server] def failedOnClose(store: Store): Unit =
    val storeId = store.storeId
    val path = store.backend.path
    logger.error(s"Store $storeId failed on an I/O error while closing. Marking it for rebuild instead of " +
                 s"closing it cleanly. $path")
    deviceControl.router.markOffline(storeId)
    storageDevices.get(store.storageDeviceId).foreach: sds =>
      sds.loadedStores -= storeId
      sds.offlineStores += storeId
    try
      markRebuildRequired(path)
      DurableFiles.remove(Backend.crlSaveFile(path))
    catch
      case t: Throwable => logger.error(s"Failed to mark store $storeId for rebuild at $path: $t")
    checkStorageDevice(store.storageDeviceId)

  /** An FSIF backend failed-stop: page-cache state can no longer be trusted, so the store is rebuilt. If it is
   *  served, it is unloaded as UnloadFailedStore does, its CRL state dropped (the rebuild discards the directory),
   *  marked rebuild-required and left offline, and its device checked so the reconcile flips its entry to
   *  Rebuilding. The mark is written before the close, so a crash meanwhile still never serves it. Nothing is
   *  awaited: the close completes in the background. A backend not (or no longer) served is ignored: either its
   *  LoadStore is still queued, and retires it on seeing it failed, or it was closed and replaced.
   *
   *  Caller holds the instance lock. */
  private def backendFailed(storeId: StoreId, backend: Option[Backend], err: Throwable): Unit =
    stores.get(storeId) match
      case Some(store) if backend.exists(_ eq store.backend) =>
        logger.error(s"Store $storeId failed on an I/O error and can no longer be trusted. Taking it offline and " +
                     s"rebuilding it. Error: $err")
        deviceControl.router.remove(storeId)
        store.objectCache.clear()
        storageDevices.get(store.storageDeviceId).foreach: sds =>
          sds.loadedStores -= storeId
          sds.offlineStores += storeId
        try
          markRebuildRequired(store.backend.path)
        catch
          case t: Throwable =>
            // Offline for the life of this process either way. A restart recovers whatever is
            // durable, which a failed store may well still hold.
            logger.error(s"Failed to mark store $storeId for rebuild at ${store.backend.path}: $t")
        crl.closeStore(storeId).onComplete: _ =>
          store.close()
        checkStorageDevice(store.storageDeviceId)
      case served =>
        // Not served: its LoadStore is still queued (and retires it), it was replaced by a newer
        // backend, or it was closed. Unless replaced, its directory is marked, so a store closed
        // around the failure is never later served, shipped or adopted as cleanly closed.
        backend match
          case Some(b) if served.isEmpty && !stores.valuesIterator.exists(_.backend.path == b.path) &&
                          os.isDir(b.path) &&
                          storageDevices.valuesIterator.exists(_.devicePath == b.path / os.up) =>
            logger.error(s"Store $storeId failed on an I/O error after it stopped being served. Marking it for " +
                         s"rebuild. Error: $err")
            try
              markRebuildRequired(b.path)
            catch
              case t: Throwable => logger.error(s"Failed to mark store $storeId for rebuild at ${b.path}: $t")
          case _ =>
            logger.info(s"Ignoring the failure of a replaced backend of store $storeId. Error: $err")

  /** Flips the entry of a store whose CRL state was lost from Active to Rebuilding, on its own
   *  device. Nothing else changes: the pool already names this device and host. Writes nothing
   *  if the device is failed or owned elsewhere, or the entry is no longer Active. Re-checks the
   *  device afterwards, so the Rebuilding pass starts the rebuild now.
   *
   *  Only a collision is retried, as everywhere else in Host. A permanent failure is logged and
   *  left for a later reconcile, which finds the marker again.
   */
  private def requestStoreRebuild(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    synchronized:
      if !pendingRebuildRequests.contains(storeId) then
        pendingRebuildRequests += storeId
        val f =
          try
            client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
              for
                ptr <- client.getStorageDevicePointer(storageDeviceId)
                kvos <- client.read(ptr)
                state = StorageDeviceState(kvos)
              yield
                val active = state.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active)
                if !state.isFailed && state.hostId == hostId && active then
                  val newState = state.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Rebuilding, None)
                  tx.update(ptr, None, None,
                    List(KeyRevision(StorageDeviceState.StateKey,
                      kvos.contents(StorageDeviceState.StateKey).revision)),
                    List(Insert(StorageDeviceState.StateKey, newState.encode())))
                  true
                else
                  // No update staged, as in markRebuiltStoreActive.
                  false
          catch case t: Throwable => Future.failed(t)

        f.onComplete: result =>
          synchronized(pendingRebuildRequests -= storeId)
          result match
            case Success(true) =>
              logger.info(s"Store $storeId on device $storageDeviceId is now Rebuilding")
              checkStorageDevice(storageDeviceId)
            case Success(false) => ()
            case Failure(t) =>
              logger.warn(s"Failed to mark store $storeId on device $storageDeviceId for rebuild. " +
                          s"Will retry on a later check. Error: $t")

  /** Removes a store directory whose CRL state was lost, before its rebuild starts: a rebuild that
   *  finds a directory at the store's path takes it for a store already in place. Renamed out of
   *  the way first, which is atomic and forced to disk, so a crash during the delete never leaves
   *  a partial directory at the store's path. Returns whether the path is now clear.
   *
   *  Caller holds the instance lock.
   */
  private def discardLostStore(local: LocalStorageDeviceState, storeId: StoreId): Boolean =
    val storePath = local.devicePath / storeId.directoryName
    if !os.exists(storePath / RebuildingStore.RebuildRequiredMarkerFile) then
      true
    else
      try
        val discardDir = local.devicePath / RebuildingStore.DiscardDirectory
        os.makeDir.all(discardDir)
        val target = discardDir / s"${storeId.directoryName}-${UUID.randomUUID()}"
        logger.warn(s"Discarding store $storeId at $storePath, whose CRL state was lost, before rebuilding it")
        os.move(storePath, target)
        DurableFiles.syncDirectory(local.devicePath)
        try os.remove.all(target)
        catch case t: Throwable => logger.warn(s"Failed to delete discarded store copy $target: $t")
        true
      catch
        case t: Throwable =>
          logger.error(s"Failed to discard store $storeId at $storePath. Not rebuilding it yet. Error: $t")
          false

  /** Gossips StoreMoved for a store this host has brought back online without repointing the
   *  pool: one whose rebuild has completed, or whose abandoned transfer out has been reinstated.
   *
   *  For a rebuild, the pool was repointed when the rebuild was assigned, but peers only learn it
   *  now, as at the end of a transfer. That was a different transaction, so the pool is read for
   *  the host and timestamp its config value carries. The store came online now, before the read,
   *  which is what clears its StoreOffline. Best-effort: a failed read only means peers find the
   *  store the slow way and keep it offline until their next recheck.
   */
  // protected as a test seam, so a test can see that it was called without a readable pool
  private[server] def gossipStoreOnlineSeam(storeId: StoreId): Unit = gossipStoreOnline(storeId)

  protected def gossipStoreOnline(storeId: StoreId): Unit =
    val onlineTimestamp = HLCTimestamp.now
    client.getStoragePoolStateAndTimestamp(storeId.poolId).onComplete:
      case Success((poolState, timestamp)) =>
        val hostId = poolState.stores(storeId.poolIndex).hostId
        gossipRetransmitter.broadcast(StoreMoved(storeId, hostId, timestamp, onlineTimestamp))
      case Failure(err) =>
        logger.warn(s"Failed to read pool ${storeId.poolId} to gossip that store $storeId is online: $err")

  /** Adopts a Shutdown device from its previous host. See ClientDeviceMetadata.adoptDevice.
   *
   *  protected so a test subclass can record the calls.
   */
  protected def adoptDevice(storageDeviceId: StorageDeviceId,
                            previousHostId: HostId,
                            devicePath: os.Path): Future[AdoptOutcome] =
    val discovery = deviceControl.discovery
    clientMetadata.adoptDevice(storageDeviceId, previousHostId, devicePath, discovery.uncleanStores,
                               (path, state) => discovery.offlineStoresIn(path, state, AdoptionClass.CrlLost),
                               markRebuildRequired)

  /** Reads the state recorded for `storageDeviceId` in the storage-devices tree. A seam rather than a direct
   *  client call so a test can hold a lookup in flight; see ClientDeviceMetadata.lookupStorageDeviceState.
   *
   *  May throw rather than return a failed Future. startDeviceCheck treats the two alike, so an
   *  override is free to do either.
   */
  protected def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    clientMetadata.lookupStorageDeviceState(storageDeviceId)

  /** Reads the source device of a transfer in, which the reconcile's TransferringIn pass re-reads
   *  on every check. A seam so a test can tell when the read has been acted on. */
  protected def readTransferSourceState(fromDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    clientMetadata.readTransferSourceState(fromDeviceId)

  /** Takes a store out of service for a transfer out: closeStore if it is open, else nothing. A
   *  seam so a test can hold the close open. Caller holds the instance lock. */
  protected def closeStoreForTransferOut(storeId: StoreId): Future[Unit] =
    transfers.closeForTransferOut(storeId)

  /** Lets StoreTransfers reach the closeStoreForTransferOut seam. */
  private[server] def closeStoreForTransferOutSeam(storeId: StoreId): Future[Unit] =
    closeStoreForTransferOut(storeId)

  /** Takes one store out of service for a device shutdown. The default is closeStore, which saves
   *  the store's CRL state beside it. A seam so a test can hold a close open, and count closes,
   *  without real backends. */
  protected def closeStoreForShutdown(storeId: StoreId): Future[Unit] = closeStore(storeId)

  /** The last step of a shutdown: one transaction on the device state that either commits
   *  Shutdown or explains why it did not. See ClientDeviceMetadata.completeDeviceShutdown.
   *
   *  protected so a test can run work between the stores closing and this transaction.
   */
  protected def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome] =
    clientMetadata.completeDeviceShutdown(storageDeviceId)

  /** Shuts down a loaded device whose state reads ShuttingDown (or Shutdown, see below).
   *
   *  Called from reconcileDeviceState in place of its normal passes, so while a device is shutting
   *  down nothing new starts on it: no store is created, no transfer in begins, no rebuild starts.
   *  The sequence, each step after the previous one completes:
   *  1. stopDeviceWork: cancel the rebuilds and transfers running on the device, then
   *     awaitShutdownWork: wait, at most shutdownWorkTimeout, for the cancelled rebuilds and any
   *     transfer-in pool update already running to finish acting on their outcome.
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
          val stopping = stopDeviceWork(local, remote)
          for
            _ <- awaitShutdownWork(deviceId, stopping)
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

  /** Step 1 of shutDownDevice: stops everything running on the device, returning, by store, what
   *  awaitShutdownWork must wait for: each cancelled rebuild's slot release, and each transfer in
   *  whose pool update is already running. Work that finishes regardless -- a rebuild already past
   *  its last read, a transfer in already in its pool update, a store creation already loading --
   *  writes its entry Offline rather than Active, because each of those transactions reads the
   *  device state and writes serviceStatus.
   *
   *  The device entries are left exactly as they are. The interrupted work is resumed after a
   *  restart or adoption by the same crash-recovery paths that resume it after a crash: a
   *  rebuild from its checkpoint in staging, a transfer in by the reconcile's TransferringIn
   *  pass, a transfer out by the receiver's retransmitted StartStoreTransfer.
   */
  private def stopDeviceWork(local: LocalStorageDeviceState,
                             remote: StorageDeviceState): Map[StoreId, Future[Unit]] =
    val (cancelledIns, stopping) = synchronized {
      val deviceId = local.storageDeviceId
      def entriesIn(status: StorageDeviceState.StoreStatus): Set[StoreId] =
        remote.stores.collect { case (storeId, entry) if entry.status == status => storeId }.toSet

      // Cancelled rebuilds fail their complete with RebuildCancelled, which releases their slot.
      // One already past its walk finishes instead, and writes its entry Offline.
      val rebuilds = entriesIn(StorageDeviceState.StoreStatus.Rebuilding).toList.flatMap: storeId =>
        rebuildingStores.get(storeId).map: sr =>
          logger.info(s"Cancelling the rebuild of store $storeId for the shutdown of device $deviceId")
          sr.cancel()
          storeId -> rebuildsSettled.get(storeId).map(_.future).getOrElse(Future.unit)

      // Transfers in: the pool updates already running are waited for, the attempts still shipping are
      // forgotten here and cancelled with the service below. See StoreTransfers.inFlightFor.
      val (ins, recording) =
        transfers.inFlightFor(deviceId, entriesIn(StorageDeviceState.StoreStatus.TransferringIn))

      // The store is already closed and marked by its transfer. Cancelling the send leaves it so.
      entriesIn(StorageDeviceState.StoreStatus.TransferringOut)
        .filter(storeTransferService.isTransferringOut)
        .foreach: storeId =>
          logger.info(s"Cancelling the transfer out of store $storeId for the shutdown of device $deviceId")
          storeTransferService.cancelTransferOut(storeId)

      (ins, rebuilds.toMap ++ recording)
    }

    cancelledIns.foreach: (storeId, transferUUID) =>
      logger.info(s"Cancelling the transfer in of store $storeId for the shutdown of device ${local.storageDeviceId}")
      storeTransferService.cancelTransferIn(storeId, transferUUID)

    stopping

  /** The wait at the end of step 1 of shutDownDevice: completes once every future in `work` has
   *  (success or failure alike), or once shutdownWorkTimeout has passed, whichever is first.
   *
   *  Without it the shutdown could close the device's stores and commit Shutdown -- telling the
   *  operator the disk may be pulled -- while a cancelled rebuild still had its staging RocksDB
   *  open on it, or a transfer in was about to load the store it had just recorded. The timeout
   *  bounds what a stuck walk (a read that never returns) can cost: the shutdown then proceeds
   *  as it did before this wait existed, and the warning names the stores left running. They
   *  are left as a crash leaves them; their entries keep the device's state honest either way.
   */
  private def awaitShutdownWork(deviceId: StorageDeviceId, work: Map[StoreId, Future[Unit]]): Future[Unit] =
    if work.isEmpty then
      Future.unit
    else
      val done = Promise[Unit]()
      Future.sequence(work.valuesIterator.map(_.transform(_ => Success(()))).toList)
        .foreach(_ => done.trySuccess(()))
      scheduleShutdownWorkTimeout(deviceId):
        if done.trySuccess(()) then
          val running = work.collect { case (storeId, f) if !f.isCompleted => storeId }
            .toList.sortBy(s => (s.poolId.uuid, s.poolIndex))
          logger.warn(s"Work on storage device $deviceId did not stop within $shutdownWorkTimeout of " +
                      s"its shutdown starting: stores ${running.mkString(", ")}. Shutting it down anyway")
      done.future

  /** Runs `fn` after shutdownWorkTimeout. A seam so a test can fire the timeout itself, since the
   *  test hosts' BackgroundTaskManager never fires anything. */
  protected def scheduleShutdownWorkTimeout(storageDeviceId: StorageDeviceId)(fn: => Unit): Unit =
    backgroundTasks.schedule(shutdownWorkTimeout)(fn)

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
      // A store whose backend failed while closing is marked rebuild-required instead of closed
      // cleanly (see failedOnClose), and an adopter takes it as Rebuilding.
      closeStoreForShutdown(storeId).transform:
        case Failure(_: StoreFailedOnClose) => Success(false)
        case other => other.map(_ => true)
      .map: clean =>
        synchronized:
          // ShutdownStore records this too. Done here as well so the bookkeeping that later
          // steps read does not depend on how the store was closed.
          deviceControl.router.markOffline(storeId)
          local.offlineStores += storeId
          // Forced to disk, as the CRL file ShutdownStore saved beside it was: once Shutdown
          // commits the disk may be pulled at any moment, and an adopter refuses a store whose
          // marker or CRL file did not survive.
          if clean then
            DurableFiles.write(local.devicePath / storeId.directoryName / StoreTransfer.TransferOutMarkerFile, "")
          gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))
    .map(_ => ())

  /** The stores closeStoresForShutdown picks. */
  private def storesToClose(local: LocalStorageDeviceState): List[StoreId] = synchronized {
    local.loadedStores.filter: storeId =>
      val storePath = local.devicePath / storeId.directoryName
      val unmarked = os.isDir(storePath) && !os.exists(storePath / StoreTransfer.TransferOutMarkerFile)
      !deviceControl.router.isOffline(storeId) || unmarked
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
    deviceControl.discovery.forgetDevice(storageDeviceId)
  }

  /** Reconciles a loaded device's on-disk state against the state recorded for it in the
   *  storage-devices tree: deletes stores transferred away, flips stores whose CRL state was lost
   *  to Rebuilding, reinstates stores left offline by an abandoned transfer out, creates
   *  Initializing stores, and starts transfers in.
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
      deviceControl.discovery.warnOnce(local.devicePath, s"Storage device ${local.storageDeviceId} owned by host ${remote.hostId} " +
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
        deviceControl.router.clearOffline(storeId)
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
      // Lost CRL State
      //
      // A store marked rebuild-required (crlSaveFileLost, or an adoption) whose entry still
      // reads Active is flipped to Rebuilding; the Rebuilding pass below then discards its
      // directory and rebuilds it. Before the abandoned-transfers-out pass, which would
      // otherwise take a marked store left behind a transfer-out marker for one to reinstate.
      val lostCrl = local.offlineStores.filter(storeId =>
        os.exists(local.devicePath / storeId.directoryName / RebuildingStore.RebuildRequiredMarkerFile))
      lostCrl.foreach: storeId =>
        if remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active) then
          requestStoreRebuild(local.storageDeviceId, storeId)

      //----------------------
      // Abandoned Transfers Out
      //
      // Nothing else ever clears a transfer-out marker from the source. StoreTransfers.startIn
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
      val (stillSending, abandonedOut) = (local.offlineStores -- lostCrl).filter(storeId =>
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.Active && entry.transferDevice.isEmpty
        ) &&
        os.exists(local.devicePath / storeId.directoryName / StoreTransfer.TransferOutMarkerFile)
      ).partition(storeTransferService.isTransferringOut)
      abandonedOut.foreach: storeId =>
        transfers.reinstateAbandonedOut(local, storeId)
      if stillSending.nonEmpty then
        scheduleDeviceRecheck(local.storageDeviceId)

      //----------------------
      // Completed Transfers In
      //
      // A crash between the pool update committing and the transferred-in marker's removal
      // leaves an Active entry naming this device over a copy that tryLoadStore skips. The entry
      // is no longer TransferringIn, so StoreTransfers.startIn never sees it, and nothing else
      // removes the marker. Active is the tree's word that the pool now names this device, so
      // the copy is ours to load. transferringIn covers the window where this process's own
      // metadata step has committed but has not yet removed the marker itself.
      local.offlineStores.filter(storeId =>
        !transfers.transferringIn.contains(storeId) &&
        remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active) &&
        os.exists(local.devicePath / storeId.directoryName / StoreTransfer.TransferredInMarkerFile)
      ).foreach: storeId =>
        val storePath = local.devicePath / storeId.directoryName
        logger.warn(s"Store $storeId on device ${local.storageDeviceId} is Active but still " +
                    s"carries its transferred-in marker. Completing the transfer and loading the store")
        if transfers.clearTransferredInMarker(storeId, storePath) then
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
        transfers.pendingStartTransfers.valuesIterator
          .filter(_.toDeviceId == local.storageDeviceId)
          .map(p => (p.msg.storeId, p.msg.fromDevice)).toList ++
        transfers.transferInRetries.iterator
          .filter((_, r) => r.toDeviceId == local.storageDeviceId)
          .map((storeId, r) => (storeId, r.fromDeviceId)).toList
      trackedTransfersIn.filterNot((storeId, fromDeviceId) =>
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
          entry.transferDevice.contains(fromDeviceId))
      ).foreach: (storeId, fromDeviceId) =>
        transfers.dropAbandonedTransferIn(storeId, fromDeviceId, local.storageDeviceId)

      // The same store can be re-targeted at another device of this host. transferringIn is keyed
      // by store, so the earlier attempt would block the new one until its own device's next
      // check noticed the abort. This device's entry is the newer word on who receives the store,
      // so the other device's attempt is dropped here instead of a poll later.
      val retargeted =
        transfers.pendingStartTransfers.valuesIterator
          .filter(_.toDeviceId != local.storageDeviceId)
          .map(p => (p.msg.storeId, p.msg.fromDevice, p.toDeviceId)).toList ++
        transfers.transferInRetries.iterator
          .filter((_, r) => r.toDeviceId != local.storageDeviceId)
          .map((storeId, r) => (storeId, r.fromDeviceId, r.toDeviceId)).toList
      retargeted.filter((storeId, _, _) =>
        remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.TransferringIn)
      ).foreach: (storeId, fromDeviceId, toDeviceId) =>
        transfers.dropAbandonedTransferIn(storeId, fromDeviceId, toDeviceId)

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
              transfers.abandonIn(local.storageDeviceId, storeId, fromDeviceId)
            else if ! fromDevice.isOperational then
              // The source is shutting down or shut down, and its host refuses to send. The
              // transfer stays pending: the next check of this device reads the source again,
              // owner included, so a source adopted by another host is asked there.
              logger.info(s"Not starting the transfer in of store $storeId onto device " +
                          s"${local.storageDeviceId}: source device $fromDeviceId is " +
                          s"${fromDevice.status}. Retrying on a later check")
            else
              // An attempt already asking the source's old host -- the source was shut down and
              // adopted since -- is pointed at the new one and asked again now, rather than at
              // the next retransmit.
              synchronized:
                transfers.pendingStartTransfers.get(storeId).filter(_.msg.fromDevice == fromDeviceId).foreach: pt =>
                  if transfers.readdressPendingTransfer(pt, fromDevice) then
                    pt.lastSend = HLCTimestamp.now
                    client.sendHostMessage(pt.msg)
              transfers.startIn(storeId, fromDevice.hostId, fromDeviceId, local.storageDeviceId, transferId)

      //----------------------
      // Rebuilding Stores
      //
      remote.stores.filter((_, entry) =>
        entry.status == StorageDeviceState.StoreStatus.Rebuilding
      ).keys.toList.sortBy(_.poolIndex).foreach: storeId =>
        startStoreRebuild(local, storeId)

  /** Checks `storageDeviceId` again after TransferOutRecheckDelay, so a store left offline behind
   *  a transfer-out marker is reinstated soon after an aborted transfer's send ends rather than at
   *  the next periodic check. At most one is scheduled per device; requests made meanwhile are
   *  covered by it.
   */
  private[server] def scheduleDeviceRecheck(storageDeviceId: StorageDeviceId): Unit = synchronized {
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
  private[server] def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =
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
                        deviceControl.router.markOffline(storeId)

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

  def getStoreIds: List[StoreId] = deviceControl.router.storeIds

  /** Stores this host can currently repair. A store mid-rebuild is already having everything
    * read, and a store transferring out is about to stop being ours; scanning either wastes the
    * load budget on work that will be redone or discarded.
    */
  def repairableStoreIds: List[StoreId] = synchronized {
    (deviceControl.router.storeIdSet -- deviceControl.router.offlineStores -- rebuildingStores.keySet --
      storeTransferService.transferringOutStores).toList
  }

  /** O(1) per-entry counterpart to `repairableStoreIds`: whether the given store is currently
    * repairable on this host. Must agree with `repairableStoreIds.contains(storeId)` exactly.
    */
  def isRepairable(storeId: StoreId): Boolean = synchronized {
    stores.contains(storeId) && !deviceControl.router.isOffline(storeId) &&
      !rebuildingStores.contains(storeId) && !storeTransferService.isTransferringOut(storeId)
  }

  def hasTransactions: Boolean = deviceControl.router.hasTransactions

  def logTransactionStatus(log: String => Unit): Unit = deviceControl.router.logTransactionStatus(log)

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
    systemTaskHeartbeatTask.cancel()
    checkStorageDeviceTask.cancel()
    usageUpdateTask.foreach(_.cancel())
    repairService.cancel()
    shutdownPromise.future
  }
  
  private[aspen] def closeStore(storeId: StoreId): Future[Unit] =
    deviceControl.loader.closeStore(storeId)

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

  private[aspen] def testingOnlyLoadedDeviceDirs: Map[os.Path, StorageDeviceId] = synchronized(deviceControl.discovery.loadedDeviceDirs)

  private[aspen] def testingOnlyReportedDeviceDirs: Set[os.Path] = synchronized(deviceControl.discovery.reportedDeviceDirs)

  /** Testing hook: device directories with a shutdown-marker lookup or adoption in flight. */
  private[aspen] def testingOnlyPendingMarkerResolutions: Set[os.Path] = synchronized(deviceControl.discovery.pendingMarkerResolutions)

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
    synchronized(transfers.startOut(m))

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
    synchronized(transfers.transferringIn.keySet)

  /** Testing hook: the stores whose failed transfer in is waiting out its retry delay. */
  private[aspen] def testingOnlyScheduledTransferInRetries: Set[StoreId] =
    synchronized(transfers.transferInRetries.keySet)

  /** Testing hook: runs every scheduled transfer-in retry now rather than after its delay. */
  private[aspen] def testingOnlyRetryTransferIns(): Unit =
    synchronized(transfers.transferInRetries.values.toList).foreach(_.run())

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
  /** Whether a store awaiting space holds a real onServed callback; None if it is not awaiting space */
  private[aspen] def testingOnlyAwaitingSpaceHasCallback(storeId: StoreId): Option[Boolean] =
    synchronized(deviceControl.loader.awaitingSpaceHasCallback(storeId))

  private[aspen] def testingOnlyOfflineStores: Set[StoreId] =
    synchronized(deviceControl.router.offlineStores)

  /** Testing hook: devices with a check request deferred behind an in-flight lookup. */
  private[aspen] def testingOnlyDeferredDeviceChecks: Set[StorageDeviceId] =
    synchronized(deferredDeviceChecks)

  private def handleEvent(event: Event): Unit = synchronized {
    event match {

      case IOCompletion(op) => deviceControl.router.deliverCompletion(op)

      case TransactionMessage(msg) => deviceControl.router.deliverTx(msg)

      case ClientReq(msg) => deviceControl.router.deliverClient(msg)

      case HostMsg(msg) => msg match
        case m: StartStoreTransfer => transfers.startOut(m)
        case m: CheckStorageDevice =>
          // A device id we do not know may simply be one that has appeared on disk since
          // our last scan -- create-storage-device sends this message as a discovery nudge
          // for exactly that case -- so rescan before concluding the device is not ours.
          if ! storageDevices.contains(m.deviceId) then
            deviceControl.discovery.checkForNewDevices()
          checkStorageDevice(m.deviceId)
        case m: ServiceMessage =>
          serviceExecutorPromise.future.foreach(_.deliverMessage(m))
        case m: ExecuteSystemTask =>
          systemTaskRunnerPromise.future.foreach(_.receive(m))
      
      case Repair(storeId, os, completion) => deviceControl.router.repair(storeId, os, completion)

      case RepairDelete(storeId, objectId, storePointer, completion) =>
        deviceControl.router.repairDelete(storeId, objectId, storePointer, completion)

      case RecoveryEvent() =>
        handleRecoveryEvent()

      case e: LoadStore =>
        deviceControl.loader.handleLoad(e)

      case BackendFailed(storeId, ref, err) =>
        backendFailed(storeId, Option(ref.get), err)

      case e: FsifOpened =>
        deviceControl.loader.handleFsifOpened(e)

      case e: StoreCrlLoaded =>
        deviceControl.loader.handleCrlLoaded(e)

      case e: UnloadFailedStore =>
        deviceControl.loader.handleUnloadFailed(e)

      case e: LoadStoreById =>
        deviceControl.loader.handleLoadById(e)

      case HeartbeatEvent() =>
        //logger.trace("Main loop got heartbeat event")
        deviceControl.router.heartbeat()

      case SystemTaskHeartbeatEvent() =>
        if systemTaskRunnerPromise.isCompleted then
          systemTaskRunnerPromise.future.foreach(_.heartbeat())

      case CheckAllDevices() =>
        // Discover first so a device found by this scan is checked within the same event
        // and its Initializing stores are created now rather than a period from now.
        deviceControl.discovery.checkForNewDevices()
        storageDevices.valuesIterator.foreach: sds =>
          checkStorageDevice(sds.storageDeviceId)

      case e: ShutdownMarkerResolved =>
        deviceControl.discovery.handleMarkerResolved(e)

      case ScanDevices() =>
        // Local only: listing the directory reads nothing over the network. A device this scan
        // loads is checked at once, as CheckAllDevices would, so its Initializing stores are
        // created now rather than up to checkStorageDevicePeriod from now.
        deviceControl.discovery.checkForNewDevices().foreach(checkStorageDevice)
        deviceControl.loader.retryStoresAwaitingSpace()

      case InitializeTaskExecutor() =>
        initializeTaskExecutor()

      case e: ShutdownStore =>
        deviceControl.loader.handleClose(e)
        
      case null => // nothing to do
      
      case _:Exit =>
        shutdownCalled = true
        shutdownPromise.success(())
    }
  }
}
