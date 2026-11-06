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
import org.aspen_ddp.aspen.server.store.backend.{Backend, Completion, CompletionHandler}
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
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
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
    storeRebuildFactory = storeRebuildFactory,
    maxConcurrentRebuilds = maxConcurrentRebuilds,
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

  private def reconciler = deviceControl.reconciler

  // Constructed here, as the retransmit task it schedules starts with the Host.
  private val services = deviceControl.services

  /** All StoreMoved and HostEndpointChanged gossip goes through here. See HostServices. */
  private[aspen] def gossipRetransmitter: GossipRetransmitter = services.gossipRetransmitter

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

  def getTaskExecutor(): Future[TaskExecutor] = services.getTaskExecutor()

  def getServiceExecutor(): Future[DurableServiceExecutor] = services.getServiceExecutor()

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

  /** Makes this host's HostState endpoint match the configured one. See HostServices.reconcileEndpoint. */
  def reconcileEndpoint(address: String,
                        dataPort: Int,
                        httpPort: Int,
                        storeTransferPort: Int): Future[Boolean] =
    services.reconcileEndpoint(address, dataPort, httpPort, storeTransferPort)

  /** Test seam: the read that decides whether the endpoint needs updating. */
  protected def readHostStateForEndpoint(): Future[HostState] = clientMetadata.readHostStateForEndpoint()

  /** Test seam: runs a failed reconcileEndpoint attempt's retry after the retry delay. */
  protected def scheduleEndpointRetry(fn: => Unit): Unit =
    backgroundTasks.schedule(Duration(5, SECONDS))(fn)

  /** Lets HostServices reach the scheduleEndpointRetry seam. */
  private[server] def scheduleEndpointRetrySeam(fn: => Unit): Unit = scheduleEndpointRetry(fn)

  private def discardStoreDirectory(storeId: StoreId,
                                    deviceId: StorageDeviceId,
                                    reason: String): Unit =
    localDevices.discardStoreDirectory(storeId, deviceId, reason)

  /** The abort returnLostTransferToRebuild issues. A seam for tests. */
  protected def abortLostTransfer(storeId: StoreId): Future[Boolean] = clientMetadata.abortLostTransfer(storeId)

  /** Decides whether a rebuilt store should be adopted or discarded. See
   *  ClientDeviceMetadata.markRebuiltStoreActive. Protected as a test seam. */
  protected def markRebuiltStoreActive(storageDeviceId: StorageDeviceId,
                                       storeId: StoreId): Future[RebuildOutcome] =
    clientMetadata.markRebuiltStoreActive(storageDeviceId, storeId, gossipStoreOnline)

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

  protected def gossipStoreOnline(storeId: StoreId): Unit = services.gossipStoreOnline(storeId)

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
                               deviceControl.rebuilds.markRebuildRequired)

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
  protected def closeStoreForShutdown(storeId: StoreId): Future[Unit] =
    deviceControl.shutdown.closeForShutdown(storeId)

  /** Lets DeviceShutdown reach the closeStoreForShutdown seam. */
  private[server] def closeStoreForShutdownSeam(storeId: StoreId): Future[Unit] =
    closeStoreForShutdown(storeId)

  /** Runs `fn` after shutdownWorkTimeout. A seam so a test can fire the timeout itself, since the
   *  test hosts' BackgroundTaskManager never fires anything. */
  protected def scheduleShutdownWorkTimeout(storageDeviceId: StorageDeviceId)(fn: => Unit): Unit =
    backgroundTasks.schedule(shutdownWorkTimeout)(fn)

  /** Lets DeviceShutdown reach the scheduleShutdownWorkTimeout seam. */
  private[server] def scheduleShutdownWorkTimeoutSeam(storageDeviceId: StorageDeviceId)(fn: => Unit): Unit =
    scheduleShutdownWorkTimeout(storageDeviceId)(fn)

  /** The last step of a shutdown: one transaction on the device state that either commits
   *  Shutdown or explains why it did not. See ClientDeviceMetadata.completeDeviceShutdown.
   *
   *  protected so a test can run work between the stores closing and this transaction.
   */
  protected def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome] =
    clientMetadata.completeDeviceShutdown(storageDeviceId)

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
    (deviceControl.router.storeIdSet -- deviceControl.router.offlineStores -- deviceControl.rebuilds.rebuildingStoreIds --
      storeTransferService.transferringOutStores).toList
  }

  /** O(1) per-entry counterpart to `repairableStoreIds`: whether the given store is currently
    * repairable on this host. Must agree with `repairableStoreIds.contains(storeId)` exactly.
    */
  def isRepairable(storeId: StoreId): Boolean = synchronized {
    stores.contains(storeId) && !deviceControl.router.isOffline(storeId) &&
      !deviceControl.rebuilds.isRebuilding(storeId) && !storeTransferService.isTransferringOut(storeId)
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
    services.shutdown()
    pendingStartTask.cancel()
    heartbeatTask.cancel()
    systemTaskHeartbeatTask.cancel()
    checkStorageDeviceTask.cancel()
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
    reconciler.scheduledDeviceRechecks

  /** Testing hook: runs every scheduled device re-check now rather than after its delay. */
  private[aspen] def testingOnlyRunDeviceRechecks(): Unit =
    reconciler.scheduledDeviceRechecks.foreach(reconciler.runDeviceRecheck)

  /** Testing hook: the storage devices with a state lookup currently in flight. */
  private[aspen] def testingOnlyActiveDeviceChecks: Set[StorageDeviceId] =
    reconciler.activeDeviceChecks

  /** Testing hook: the stores currently marked offline. */
  /** Whether a store awaiting space holds a real onServed callback; None if it is not awaiting space */
  private[aspen] def testingOnlyAwaitingSpaceHasCallback(storeId: StoreId): Option[Boolean] =
    synchronized(deviceControl.loader.awaitingSpaceHasCallback(storeId))

  private[aspen] def testingOnlyOfflineStores: Set[StoreId] =
    synchronized(deviceControl.router.offlineStores)

  /** Testing hook: devices with a check request deferred behind an in-flight lookup. */
  private[aspen] def testingOnlyDeferredDeviceChecks: Set[StorageDeviceId] =
    reconciler.deferredDeviceChecks

  private def handleEvent(event: Event): Unit = synchronized {
    event match {

      case IOCompletion(op) => deviceControl.router.deliverCompletion(op)

      case TransactionMessage(msg) => deviceControl.router.deliverTx(msg)

      case ClientReq(msg) => deviceControl.router.deliverClient(msg)

      case HostMsg(msg) => msg match
        case m: StartStoreTransfer => transfers.startOut(m)
        case m: CheckStorageDevice => reconciler.handleCheckMessage(m)
        case m: (ServiceMessage | ExecuteSystemTask) => services.deliver(m)
      
      case Repair(storeId, os, completion) => deviceControl.router.repair(storeId, os, completion)

      case RepairDelete(storeId, objectId, storePointer, completion) =>
        deviceControl.router.repairDelete(storeId, objectId, storePointer, completion)

      case RecoveryEvent() =>
        handleRecoveryEvent()

      case e: LoadStore =>
        deviceControl.loader.handleLoad(e)

      case e: BackendFailed =>
        deviceControl.rebuilds.handleBackendFailed(e)

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
        services.systemTaskHeartbeat()

      case CheckAllDevices() => reconciler.checkAll()

      case e: ShutdownMarkerResolved =>
        deviceControl.discovery.handleMarkerResolved(e)

      case ScanDevices() => reconciler.scan()

      case InitializeTaskExecutor() =>
        services.initializeTaskExecutor()

      case e: ShutdownStore =>
        deviceControl.loader.handleClose(e)
        
      case null => // nothing to do
      
      case _:Exit =>
        shutdownCalled = true
        shutdownPromise.success(())
    }
  }
}
