package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, FatalReadError, KeyValueObjectState, StopRetrying, StoragePool, Transaction, ObjectState as ClientObjectState}

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, runSequentially}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, Completion, CompletionHandler, RocksDBBackend, RocksDBConfig}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.store.{Frontend, Store}
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import scribe.Logging
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ReadError, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRevision}
import org.aspen_ddp.aspen.server.transfer.{StoreTransferFactory, StoreTransferIn, StoreTransferOut, TransferringIn, TransferringOut}
import org.aspen_ddp.aspen.server.rebuild.{StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.compute.{DurableServiceExecutor, ServiceEntry, TaskExecutor}
import org.aspen_ddp.aspen.compute.impl.{SimpleDurableServiceExecutor, SimpleTaskExecutor}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskRunner}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.server.usage.StoragePoolUsageManager
import org.aspen_ddp.aspen.server.usage.StorageDeviceUsageManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
import org.aspen_ddp.aspen.common.rebalancing.{RebalancingDurableService, RebalancingMessage, TransferComplete}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object StoreManager:
  private sealed abstract class Event

  private case class IOCompletion(op: Completion) extends Event
  private case class TransactionMessage(msg: TxMessage) extends Event
  private case class ClientReq(msg: ClientRequest) extends Event
  private case class HostMsg(msg: HostMessage) extends Event
  private case class Repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]) extends Event
  private case class LoadStore(storageDeviceId: StorageDeviceId, backend: Backend, completion: Promise[Unit]) extends Event
  private case class LoadStoreById(sstorageDeviceId: StorageDeviceId, toreId: StoreId) extends Event
  private case class Exit() extends Event
  private case class RecoveryEvent() extends Event
  private case class HeartbeatEvent() extends Event
  private case class CheckAllDevices() extends Event
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

    /** The destination was declared failed mid-flight, so the source was put back to Active and
     *  the pool left naming it. The arrived copy belongs to nobody. */
    case SourceRestored

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

    /** The tree does not say this device owns a rebuilt store: the device was tombstoned,
     *  the entry vanished, or the entry has a status other than Rebuilding/Active. The rebuilt
     *  store belongs to nobody and must be discarded. */
    case Discard


  class IOHandler(mgr: StoreManager) extends CompletionHandler:
    override def complete(op: Completion): Unit =
      mgr.events.add(IOCompletion(op))

  class LocalStorageDeviceState(val storageDeviceId: StorageDeviceId,
                                val devicePath: Path,
                                val configFile: File):
    var loadedStores: Set[StoreId] = Set()
    var offlineStores: Set[StoreId] = Set()
    
    def totalSize: Long = configFile.getTotalSpace
    def currentUsage: Long = totalSize - configFile.getFreeSpace

  class PendingTransfer(val msg: StartStoreTransfer, var lastSend: HLCTimestamp)


class StoreManager(val client: AspenClient,
                   val hostId: HostId,
                   val aspenSystemId: UUID,
                   val rootDir: Path,
                   val ec: ExecutionContext,
                   val objectCacheFactory: () => ObjectCache,
                   val net: Messenger,
                   val backgroundTasks: BackgroundTaskManager,
                   crlFactory: CrashRecoveryLogFactory,
                   val finalizerFactory: TransactionFinalizer.Factory,
                   val txDriverFactory: TransactionDriver.Factory,
                   val heartbeatPeriod: Duration,
                   val checkStorageDevicePeriod: Duration,
                   val storeTransferFactory: StoreTransferFactory = StoreTransferFactory.Filesystem,
                   val storeRebuildFactory: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                   /** A rebuild reads a whole store's worth of data through the client, so an
                    *  eight-store device must not start eight at once. Entries beyond this bound
                    *  wait; every completion re-checks all loaded devices and starts whatever is
                    *  queued. */
                   val maxConcurrentRebuilds: Int = 2) extends Logging {
  import StoreManager._
  
  given ExecutionContext = ec

  private val events = new LinkedBlockingQueue[Event]()

  private val ioHandler = new IOHandler(this)

  private val txStatusCache = new TransactionStatusCache()

  private val crl = crlFactory.createCRL()

  private val threadPool = Executors.newFixedThreadPool(1)

  protected var shutdownCalled = false
  private val shutdownPromise: Promise[Unit] = Promise()

  val storageDevicesDir: Path = rootDir.resolve(StorageDeviceManager.StorageDevicesDirName)

  // Append-only for the life of the process: tryLoadDevice is the only writer in production and
  // nothing anywhere removes an entry. So a device present here stays present, and a lookup miss
  // means a device this process has never loaded rather than one that has gone away. Tests inject
  // entries directly via RecordingStoreManager.injectLoadedDevice.
  protected var storageDevices: Map[StorageDeviceId, LocalStorageDeviceState] = Map()
  protected var stores: Map[StoreId, Store] = Map()

  private var offlineStores: Set[StoreId] = Set()
  private var creatingStores: Set[StoreId] = Set()
  private var rebuildingStores: Map[StoreId, StoreRebuild] = Map()
  private var transferringOut: Map[StoreId, StoreTransferOut] = Map()
  private var transferringInUUIDs: Map[UUID, StoreTransferIn] = Map()
  private var transferringInStoreIds: Set[StoreId] = Set()
  private var abandoningTransferIns: Set[StoreId] = Set()
  private var pendingStartTransfers: Map[StoreId, PendingTransfer] = Map()
  private var activeDeviceChecks: Set[StorageDeviceId] = Set()
  private var deferredDeviceChecks: Set[StorageDeviceId] = Set()

  private val taskExecutorPromise: Promise[TaskExecutor] = Promise()
  private val serviceExecutorPromise: Promise[SimpleDurableServiceExecutor] = Promise()
  private val systemTaskRunnerPromise: Promise[SystemTaskRunner] = Promise()
  private val poolUsageManager = new StoragePoolUsageManager(client)
  private val deviceUsageManager = new StorageDeviceUsageManager(client)
  private var usageUpdateTask: Option[ScheduledTask] = None

  private val pendingStartTask = backgroundTasks.schedulePeriodic(Duration(30, SECONDS)):
    synchronized {
      val now = HLCTimestamp.now
      pendingStartTransfers.valuesIterator.foreach: pt =>
        if now - pt.lastSend > Duration(30, SECONDS) then
          pt.lastSend = now
          client.sendHostMessage(pt.msg)
    }

  private val heartbeatTask = backgroundTasks.schedulePeriodic(heartbeatPeriod) {
    events.put(HeartbeatEvent())
  }
  
  private val checkStorageDeviceTask = backgroundTasks.schedulePeriodic(checkStorageDevicePeriod) {
    events.put(CheckAllDevices())
  }
  
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
                    val serviceExec = new SimpleDurableServiceExecutor(client, hostId, backgroundTasks)
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
                      val serviceExec = new SimpleDurableServiceExecutor(client, hostId, backgroundTasks)
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

    usageUpdateTask = Some(backgroundTasks.scheduleNonConcurrentPollingTask(Duration(20, SECONDS)):
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

  /** Scans storage-devices/ and loads any device found there.
   *
   *  Called at construction and from the event loop, both on every CheckAllDevices and on a
   *  CheckStorageDevice naming a device we have not loaded. Repeated scans are safe:
   *  tryLoadDevice skips any device already present in storageDevices, so a device's
   *  LocalStorageDeviceState -- and with it the offlineStores set that reconcileDeviceState
   *  reads -- survives, and its children are not re-offered to tryLoadStore over
   *  already-open backends.
   *
   *  Mutual exclusion: the handleEvent calls hold the instance lock; the constructor call
   *  precedes start(), so no event-loop thread exists yet.
   */
  private def checkForNewDevices(): Unit =
    if ! Files.isDirectory(storageDevicesDir) then
      logger.warn(s"Invalid storage devices directory: $storageDevicesDir")
    else
      // listFiles returns null on an IO error even when isDirectory just succeeded. Unguarded
      // that is an NPE, and the constructor and the event loop fare differently: from the
      // constructor it would abort StoreManager construction outright, while from the event loop
      // start()'s catch-all would keep the loop running but report only the event class and the
      // NPE. Warn here so both name the directory that could not be listed.
      storageDevicesDir.toFile.listFiles() match
        case null  => logger.warn(s"Failed to list storage devices directory: $storageDevicesDir")
        case files => files.foreach(tryLoadDevice)

  private def tryLoadDevice(sdFile: File): Unit =
    val storageDevicePath = sdFile.toPath
    val sdCfgPath = storageDevicePath.resolve(StorageDeviceConfig.configFilename)

    if Files.isDirectory(sdFile.toPath) && Files.exists(sdCfgPath) then
      try
        val configFile = sdCfgPath.toFile
        val sdCfg = StorageDeviceConfig.loadStorageDeviceConfig(configFile)
        if sdCfg.aspenSystemId != aspenSystemId then
          logger.warn(s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
        else
          storageDevices.get(sdCfg.storageDeviceId) match
            case Some(existing) =>
              // Already loaded. Usually this is just a rescan finding the same directory
              // again, which must be a no-op so the device's state and open backends
              // survive. A different directory claiming the same id -- a duplicate mount or
              // a copied config file -- would otherwise replace the map entry and orphan the
              // original's already-open stores, so report it and keep the original.
              if existing.devicePath != storageDevicePath then
                logger.warn(s"Storage device ${sdCfg.storageDeviceId} is already loaded from " +
                            s"${existing.devicePath}; ignoring duplicate at $storageDevicePath")
            case None =>
              sdFile.listFiles match
                case null =>
                  // Same hazard as the scan in checkForNewDevices: listFiles returns null on
                  // an IO error even when isDirectory just succeeded. The catch below would
                  // absorb the resulting NPE and leave the device unregistered and retryable
                  // just the same; this guard exists to name the directory instead.
                  logger.warn(s"Failed to list storage device directory $sdFile. Will retry on the next scan")
                case storeFiles =>
                  val sds = new LocalStorageDeviceState(sdCfg.storageDeviceId, storageDevicePath, configFile)
                  logger.info(s"Loading storage device $sdFile. StorageDeviceId ${sds.storageDeviceId}")
                  storeFiles.foreach: potentialStoreFile =>
                    tryLoadStore(sds, potentialStoreFile)
                  // Registered only after every child has been offered to tryLoadStore. If
                  // anything above throws, the catch below logs it and the device stays
                  // unregistered, so the next scan retries the whole device rather than
                  // skipping it as already loaded. That retry re-offers children whose
                  // LoadStore events are already queued; tryLoadStore's own catch absorbs the
                  // resulting duplicate-backend failures.
                  storageDevices += sdCfg.storageDeviceId -> sds
      catch
        case t: Throwable => logger.warn(s"Failed to load storage device found at path $sdFile. Error: $t")

  // protected so tests can subclass with a recording implementation instead of opening a
  // real RocksDB backend. `stores` and `storageDevices` are already protected.
  protected def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: File): Unit =
    val storeCfgPath = potentialStoreFile.toPath.resolve(StoreConfig.configFilename)
    if Files.exists(storeCfgPath) then
      try
        val storeCfg = StoreConfig.loadStoreConfig(storeCfgPath.toFile)
        if os.exists(os.Path(potentialStoreFile) / TransferringOut.MarkerFile) then
          logger.info(s"Skipping load of offline store marked for transfer out. StoreId ${storeCfg.storeId}. $potentialStoreFile")
          offlineStores += storeCfg.storeId
          sds.offlineStores += storeCfg.storeId
        else
          val backend = storeCfg.backend match
            case b: StoreConfig.RocksDB => new RocksDBBackend(potentialStoreFile.toPath, storeCfg.storeId, ec)
          sds.loadedStores += backend.storeId
          sds.offlineStores -= backend.storeId
          offlineStores -= backend.storeId
          logger.info(s"Loading store ${storeCfg.storeId}: $potentialStoreFile")
          loadStore(sds.storageDeviceId, backend)
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

  def getDevicePath(storageDeviceId: StorageDeviceId): Option[Path] = synchronized {
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
    // Nothing consumes the Future this returns, so under a bare transactUntilSuccessful a
    // permanent error is a 60 s retry loop that runs for the life of the process and reports
    // nothing at all. These are the permanent classes: an id with no entry in its tree, a pool
    // index the config does not have, generateBootstrapConfig's two requires, and an object that
    // cannot be read. The catch-all keeps every other error on the retry path, where a lost
    // transaction race and a transient read failure belong.
    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: IndexOutOfBoundsException => throw StopRetrying(e)
      case e: IllegalArgumentException => throw StopRetrying(e)
      case e: FatalReadError => throw StopRetrying(e)
      case _ => Future.unit

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
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
            // StoreManager and learns nothing from the write above. Its copy is still offline
            // behind the transfer-out marker while the pool goes on naming it, which means the
            // slice answers nothing until that host's own poll reinstates it -- up to
            // Main.CheckStorageDevicesPeriod away. Best-effort, because that poll is the
            // guarantee and this is only the latency.
            tx.result.foreach: _ =>
              client.sendBestEffortHostMessage(
                CheckStorageDevice(fromDev.hostId, client.clientId, fromDeviceId))

          TransferOutcome.SourceRestored

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

          val newEntry = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
          val newToStores = toDev.stores + (storeId -> newEntry)
          val newtoDev = toDev.copy(stores = newToStores)
          val toDevReqs = List(KeyRevision(StorageDeviceState.StateKey, toDevKvos.contents(StorageDeviceState.StateKey).revision))
          val toDevOps = List(Insert(StorageDeviceState.StateKey, newtoDev.encode()))
          tx.update(toDevPtr, None, None, toDevReqs, toDevOps)

          // If state update transaction is successful, send a CheckStorageDevice
          // message to the host of the old storage device so they can delete
          // the store content
          tx.result.foreach: _ =>
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

          TransferOutcome.PoolRepointed

        else
          // The source has already dropped the store, so a concurrent call to this method
          // repointed the pool at the destination. The destination owns the arrived copy either
          // way, so this is the same outcome -- just reached by somebody else.
          TransferOutcome.PoolRepointed

  /** Reclaims the store copy a transfer just delivered onto a device that turned out to be
   *  tombstoned.
   *
   *  Only reached with TransferOutcome.SourceRestored, which means the pool still names the
   *  source and the source still holds the data -- so there is nothing here worth keeping. Left
   *  in place it would be loaded by tryLoadStore on every restart (TransferringIn removes the
   *  transfer marker from the unpacked copy, so nothing stops it), and if the source happens to
   *  live on this same host that load would replace the source's entry in `stores`, which is
   *  keyed by StoreId alone.
   *
   *  Nothing is unloaded because nothing was loaded: the load is downstream of the decision.
   */
  private def discardStoreDirectory(storeId: StoreId,
                                    deviceId: StorageDeviceId,
                                    reason: String): Unit = synchronized {
    storageDevices.get(deviceId) match
      case None =>
        // The device map is append-only, so this means the operation completed on a device this
        // process never loaded. Log rather than assume.
        logger.warn(s"Cannot discard the copy of store $storeId on unloaded device $deviceId")

      case Some(sds) =>
        val storePath = os.Path(sds.devicePath) / storeId.directoryName
        if os.exists(storePath) then
          logger.warn(s"Discarding the copy of store $storeId on failed device $deviceId. " +
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
      "The pool still names the transfer source, which kept its copy, so this one belongs to nobody")

  private def startStoreTransferIn(storeId: StoreId,
                                   fromHostId: HostId,
                                   fromDeviceId: StorageDeviceId,
                                   toDeviceid: StorageDeviceId): Unit = synchronized {
    require(fromDeviceId != toDeviceid)

    if ! transferringInStoreIds.contains(storeId) then
      storageDevices.get(toDeviceid).foreach: toDevice =>
        val ti = storeTransferFactory.createTransferIn(
          client,
          storeId,
          toDeviceid,
          toDevice.devicePath
        )
        transferringInUUIDs += ti.transferUUID -> ti
        transferringInStoreIds += storeId

        def cleanup(): Unit = synchronized {
          transferringInUUIDs -= ti.transferUUID
          transferringInStoreIds -= storeId
        }

        ti.complete.onComplete:
          case Success(_) =>
            cleanup()

            // Loading is downstream of the metadata decision, not ahead of it. An unconditional
            // load hands a live copy of the store to a device that updateStateForTransferredStore
            // may be about to decide must not own it, and nothing would ever take it back.
            //
            // Waiting costs no availability: the pool goes on naming the source until that
            // transaction commits, so nothing routes here before then anyway.
            updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid).onComplete:
              case Success(TransferOutcome.PoolRepointed) =>
                loadStoreById(toDeviceid, storeId)

              case Success(TransferOutcome.SourceRestored) =>
                discardArrivedTransferIn(storeId, toDeviceid)

              case Failure(t) =>
                // The only observer this Future has. Its onFail routes the permanent errors to
                // StopRetrying, which fails the promise -- and an unobserved failed Promise is
                // not reported by the ExecutionContext, so without this line a permanent give-up
                // is exactly as silent as the infinite retry loop it replaced. What is left
                // behind matters: the source's entry stays TransferringOut, which the abandoned
                // transfer-out pass will not reinstate because it requires Active, so the
                // source's copy stays offline behind its marker with nothing to say why.
                logger.error(s"Failed to record the completed transfer of store $storeId from " +
                             s"device $fromDeviceId to device $toDeviceid. The store is not " +
                             s"loaded here and the pool still names the source", t)
          case Failure(_) =>
            cleanup()
            startStoreTransferIn(storeId, fromHostId, fromDeviceId, toDeviceid)

        val msg = StartStoreTransfer(
          fromHostId,
          client.clientId,
          fromDeviceId,
          storeId,
          HLCTimestamp.now,
          ti.transferUUID
        )

        pendingStartTransfers += storeId -> new PendingTransfer(msg, HLCTimestamp.now)

        client.sendHostMessage(msg)
  }

  private def startStoreTransferOut(m: StartStoreTransfer): Unit = synchronized {
    def startTransfer(): Unit = {
      def err(msg: String): Nothing =
        logger.info(msg)
        throw new Exception(msg)

      for
        // getStoragePoolState, not getStoragePool(...).getState(): SimpleStoragePool caches its
        // StoragePoolState from construction and nothing ever calls dropCachedState(), so the
        // cached copy is a permanent snapshot of where the stores were when this client first
        // touched the pool. The source device must be read fresh -- for a store that has already
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
        // Keyed off the pool entry, not fromDevice.storageDeviceId: those are the same value in
        // the healthy case, but a tombstoned source zeroes its own copy, and the lookup would
        // then miss and blame a removed disk for a device that is loaded and present.
        sourceDs = storageDevices.get(poolEntry.storageDeviceId) match
          case None => err(s"Source storage device for transfer ${poolEntry.storageDeviceId} not loaded. Disk removed?")
          case Some(sds) => sds
      yield
        synchronized {
          if devEntry.status == StorageDeviceState.StoreStatus.TransferringOut then
            if ! transferringOut.contains(m.storeId) then
              val fclosed = stores.get(m.storeId) match
                case None => Future.unit
                case Some(_) => closeStore(m.storeId)

              fclosed.foreach: _ =>
                synchronized {
                  if ! transferringOut.contains(m.storeId) then
                    val to = storeTransferFactory.createTransferOut(
                      client,
                      sourceDs.storageDeviceId,
                      sourceDs.devicePath,
                      m.storeId,
                      toDevice.hostId,
                      toDevice.storageDeviceId,
                      m.timestamp,
                      m.transferUUID
                    )
                    transferringOut += m.storeId -> to

                    to.complete.foreach: _ =>
                      synchronized {
                        transferringOut -= m.storeId
                      }
                }
        }
    }

    transferringOut.get(m.storeId) match
      case None => startTransfer()
      case Some(to) =>
        if to.transferUUID != m.transferUUID && to.timestamp < m.timestamp then
          to.abort()
          transferringOut -= m.storeId
          startTransfer()
  }

  private def transferDataReceived(m: StoreTransferData): Unit = synchronized {
    transferringInUUIDs.get(m.transferUUID).foreach: ti =>
      if pendingStartTransfers.contains(ti.storeId) then
        pendingStartTransfers -= ti.storeId
      ti.dataReceived(m.data)
  }

  // protected so a test subclass can record the claim instead of transacting against a host
  // object. Nothing outside StoreManager calls it.
  protected def updateHostId(storageDeviceId: StorageDeviceId): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given Transaction = tx

      case class PoolState(poolId: PoolId,
                           pointer: KeyValueObjectPointer,
                           kvos: KeyValueObjectState,
                           stores: List[StoreId],
                           config: StoragePoolState)

      def collectPools(stores: List[StoreId]): List[Future[PoolState]] =
        val poolMap = stores.foldLeft(Map[PoolId, List[StoreId]]()): (m, storeId) =>
          val l = m.get(storeId.poolId) match
            case None => storeId :: Nil
            case Some(lst) => storeId :: lst
          m + (storeId.poolId -> l)

        poolMap.map { (poolId, stores) =>
          for
            poolPtr <- client.getStoragePoolPointer(poolId)
            poolKvos <- client.read(poolPtr)
          yield
            PoolState(poolId, poolPtr, poolKvos, stores, StoragePoolState(poolKvos))
        }.toList

      def updatePool(ps: PoolState): Unit =
        ps.stores.foreach: storeId =>
          val newEntry = ps.config.stores(storeId.poolIndex).copy(hostId = hostId)
          ps.config.stores(storeId.poolIndex) = newEntry

        val reqs = List(KeyRevision(StoragePoolState.ConfigKey, ps.kvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, ps.config.encode()))

        tx.update(ps.pointer, None, None, reqs, ops)

      for
        devPtr <- client.getStorageDevicePointer(storageDeviceId)
        devKvos <- client.read(devPtr)
        state = StorageDeviceState(devKvos)
        pools <- Future.sequence(collectPools(state.stores.keysIterator.toList))
        newHostPtr <- client.getHostPointer(hostId)
        newHostKvos <- client.read(newHostPtr)
        oldHostPtr <- client.getHostPointer(state.hostId)
        oldHostKvos <- client.read(oldHostPtr)
      yield
        // Check to ensure another concurrent call to this method didn't already
        // succeed
        if state.hostId != hostId then
          logger.info(s"Updating host for storage device ${storageDeviceId}")

          pools.foreach: ps =>
            updatePool(ps)

          //----------------------
          // Update Device State
          //
          val newDevState = state.copy(hostId = hostId)
          val reqs = List(KeyRevision(StorageDeviceState.StateKey, devKvos.contents(StorageDeviceState.StateKey).revision))
          val ops = List(Insert(StorageDeviceState.StateKey, newDevState.encode()))

          tx.update(devPtr, None, None, reqs, ops)

          //----------------------
          // Update Old HostState State
          //
          val oldHostState = HostState(oldHostKvos)
          val updatedOldHostState = oldHostState.copy(storageDevices = oldHostState.storageDevices - storageDeviceId)

          tx.update(oldHostPtr, None, None,
            List(KeyRevision(HostState.StateKey, oldHostKvos.contents(HostState.StateKey).revision)),
            List(Insert(HostState.StateKey, updatedOldHostState.encode())))

          //----------------------
          // Update New HostState State
          //
          val newHostState = HostState(newHostKvos)
          val updatedNewHostState = newHostState.copy(storageDevices = newHostState.storageDevices + storageDeviceId)

          tx.update(newHostPtr, None, None,
            List(KeyRevision(HostState.StateKey, newHostKvos.contents(HostState.StateKey).revision)),
            List(Insert(HostState.StateKey, updatedNewHostState.encode())))

          tx.result.foreach: _ =>
            logger.info(s"Successfully updated host for storage device ${storageDeviceId}")

  private def createNewStore(local: LocalStorageDeviceState, storeId: StoreId): Unit = synchronized {
    val storePath = os.Path(local.devicePath) / storeId.directoryName

    if ! creatingStores.contains(storeId) then
      creatingStores += storeId

      val fcreate = if os.exists(storePath) then
        Future.unit
      else
        client.getStoragePoolState(storeId.poolId).flatMap: pstate =>
          val backend = pstate.backendConfig match
            case cfg: RocksDBConfig => new RocksDBBackend(storePath.toNIO, storeId, ec)

          loadStore(local.storageDeviceId, backend)

      client.transactUntilSuccessful: tx =>
        for
          _ <- fcreate
          ptr <- client.getStorageDevicePointer(local.storageDeviceId)
          kvos <- client.read(ptr)
          state = StorageDeviceState(kvos)
        yield
          state.stores.get(storeId).foreach: entry =>
            if entry.status == StorageDeviceState.StoreStatus.Initializing then
              val newStores = state.stores + (storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
              val newState = state.copy(stores = newStores)

              val reqs = List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision))
              val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

              logger.info(s"Updating device state to mark store $storeId as Active")
              tx.update(ptr, None, None, reqs, ops)

              tx.result.foreach: _ =>
                logger.info(s"Successfully updated device state to mark store $storeId as Active")
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
      if !rebuildingStores.contains(storeId) && rebuildingStores.size < maxConcurrentRebuilds then
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
                               s"${os.Path(local.devicePath) / storeId.directoryName} but is not " +
                               s"loaded, and the device entry still says Rebuilding", t)
                finally
                  release()
                  recheckAll()

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
   *  Without exhaustive onFail, getStorageDevicePointer's NoSuchElementException is neither
   *  StopRetrying nor a match, so ExponentialBackoffRetryStrategy reschedules forever, the future
   *  never completes, and this device's rebuild queue never advances.
   */
  protected def markRebuiltStoreActive(storageDeviceId: StorageDeviceId,
                                       storeId: StoreId): Future[RebuildOutcome] =
    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: FatalReadError => throw StopRetrying(e)
      case _ => Future.unit

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
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
              val newState = state.setStoreEntry(
                storeId, StorageDeviceState.StoreStatus.Active, None)

              val reqs = List(KeyRevision(StorageDeviceState.StateKey,
                kvos.contents(StorageDeviceState.StateKey).revision))
              val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

              logger.info(s"Updating device state to mark rebuilt store $storeId as Active")
              tx.update(ptr, None, None, reqs, ops)

              tx.result.foreach: _ =>
                logger.info(s"Rebuild of store $storeId complete")

              RebuildOutcome.Adopt

            case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Active =>
              // Already flipped by a concurrent rebuild completion. No update staged.
              RebuildOutcome.Adopt

            case _ =>
              // Entry missing entirely, or has a status other than Rebuilding/Active (Initializing,
              // TransferringIn, TransferringOut). In all these cases the tree does not say this
              // device owns a rebuilt store. Latent rather than live: no current in-tree writer
              // produces "healthy device, entry vanished", but it's the wrong default.
              RebuildOutcome.Discard

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

  /** Reconciles a loaded device's on-disk state against the state recorded for it in the
   *  storage-devices tree: deletes stores transferred away, reinstates stores left offline by an
   *  abandoned transfer out, creates Initializing stores, and starts transfers in.
   *
   *  If the tree records a different host, none of that happens: the device has migrated here,
   *  so this claims it with updateHostId and re-requests the check once the claim commits.
   *  A device the tree records as failed is ignored outright: it is neither claimed nor
   *  reconciled, and its stores are being rebuilt elsewhere.
   *
   *  Callers hold the instance lock; the continuations it registers do not.
   */
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
    if remote.isFailed then
      // An operator has declared this device dead. Its directory can still mount -- a dead
      // controller rather than a dead platter -- and without this the zeroed host id reads as a
      // migration: updateHostId would call getHostPointer on a host that does not exist, fail
      // inside transactUntilSuccessful, and retry forever while holding this device's entry in
      // activeDeviceChecks. Every later check of the device would then be dropped silently.
      logger.info(s"Ignoring failed storage device ${local.storageDeviceId}")
    else if remote.hostId != hostId then
      updateHostId(local.storageDeviceId).foreach: _ =>
        checkStorageDevice(local.storageDeviceId)
    else
      //----------------------
      // Deleted Stores
      //
      local.offlineStores.filter(storeId =>
        !remote.stores.contains(storeId)
      ).foreach: storeId =>
        offlineStores -= storeId
        local.offlineStores -= storeId
        val storePath = os.Path(local.devicePath) / storeId.directoryName
        if os.exists(storePath) then
          logger.info(s"Deleting successfully transferred store $storePath")
          try
            os.remove.all(storePath)
          catch
            case t: Throwable => logger.error(s"Failed to delete store $storePath. Error: $t")

      //----------------------
      // Abandoned Transfers Out
      //
      // Nothing else ever clears a transfer-out marker from the source. TransferringIn deletes
      // it only from the destination's unpacked copy, and a transfer that completes normally
      // has the source's whole store directory deleted by the pass above -- the marker goes with
      // it. So a transfer that ends without completing, which is what a destination declared
      // failed mid-flight produces, strands the marker forever, and tryLoadStore re-reads it on
      // every restart and puts the store straight back into offlineStores.
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
      // destination still named, means the marker is doing its job. transferringOut is the same
      // question asked of this process rather than the tree, and covers the window where the
      // drain restores the entry while this host is still shipping bytes.
      local.offlineStores.filter(storeId =>
        !transferringOut.contains(storeId) &&
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.Active && entry.transferDevice.isEmpty
        ) &&
        os.exists(os.Path(local.devicePath) / storeId.directoryName / TransferringOut.MarkerFile)
      ).foreach: storeId =>
        reinstateAbandonedTransferOut(local, storeId)

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
      // Transferring In Stores
      //
      remote.stores.filter { (storeId, entry) =>
        entry.status == StorageDeviceState.StoreStatus.TransferringIn
      }.map { (storeId, status) =>
        (storeId, status.transferDevice)
      }.toList.foreach: (storeId, ofromDeviceId) =>
        ofromDeviceId.foreach: fromDeviceId =>
          client.getStorageDeviceState(fromDeviceId).foreach: fromDevice =>
            if fromDevice.isFailed then
              // A tombstoned source has both ids zeroed, so fromDevice.hostId is HostId(0,0).
              // Starting the transfer would address every StartStoreTransfer to a host that
              // does not exist; the messages are dropped, the entry never resolves, and this
              // check re-sends them forever. Nothing reports the stall either: rebalancing's
              // ownedStores filters TransferringIn out of its accounting, so the store is
              // invisible to the rebalancer too.
              abandonTransferIn(local.storageDeviceId, storeId, fromDeviceId)
            else
              startStoreTransferIn(storeId, fromDevice.hostId, fromDeviceId, local.storageDeviceId)

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
   *  both this device's offlineStores and the manager's. Those two sets and the marker file are
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
    val storePath = os.Path(local.devicePath) / storeId.directoryName

    logger.warn(s"Store $storeId on device ${local.storageDeviceId} is offline behind a " +
                s"transfer-out marker, but the storage-devices tree records it as Active with " +
                s"no transfer outstanding. Clearing the abandoned transfer and loading the store")

    val markerRemoved =
      try
        os.remove(storePath / TransferringOut.MarkerFile)
        true
      catch
        case t: Throwable =>
          // Leave the store offline. Loading it with the marker still in place would be undone
          // by the next restart, so this pass would have logged a repair that did not hold.
          logger.error(s"Failed to remove the transfer-out marker for store $storeId at " +
                       s"$storePath. The store stays offline. Error: $t")
          false

    if markerRemoved then
      tryLoadStore(local, storePath.toNIO.toFile)

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
    // createNewStore's creatingStores and startStoreTransferIn's transferringInStoreIds.
    if ! abandoningTransferIns.contains(storeId) then
      abandoningTransferIns += storeId

      logger.warn(s"Abandoning transfer of store $storeId onto device $toDeviceId: source " +
                  s"device $fromDeviceId has been declared failed")

      // Nothing consumes this Future, so a bare transactUntilSuccessful would turn a permanent
      // error into a silent 60 s loop for the life of the process -- and now that the guard above
      // is set, one that no later check could ever displace. Only two permanent classes are
      // reachable from these two reads: a device id with no entry in the storage-devices tree,
      // and an object that cannot be read. Everything else stays on the retry path.
      def onFail(err: Throwable): Future[Unit] = err match
        case e: NoSuchElementException => throw StopRetrying(e)
        case e: FatalReadError => throw StopRetrying(e)
        case _ => Future.unit

      val fabandon = client.transactUntilSuccessfulWithRecovery(onFail): tx =>
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
      fabandon.onComplete: _ =>
        synchronized:
          abandoningTransferIns -= storeId
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
   *  is reconciled against the tree, a device this process has not loaded -- devices are never
   *  unloaded, so this is one that has not appeared on disk in this process -- has its stores
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
   *  StoreManagerDeviceDiscoverySuite.
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
    // by StoreManagerDeviceDiscoverySuite rather than argued. Clearing after recurses an extra
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
                      // A device that has not appeared on disk in this process -- nothing ever
                      // unloads one. Silence its stores rather than sending "UnknownStore".
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
                // lookup succeeds. A config carried to another host then takes
                // reconcileDeviceState's hostId mismatch branch, which is the designed
                // host-migration path, not a warn.
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

  def hasTransactions: Boolean = synchronized {
    stores.valuesIterator.exists(_.hasTransactions)
  }

  def logTransactionStatus(log: String => Unit): Unit = synchronized {
    stores.values.foreach(_.logTransactionStatus(log))
  }

  def loadStore(storageDeviceId: StorageDeviceId, backend: Backend): Future[Unit] = {
    val p = Promise[Unit]()
    events.put(LoadStore(storageDeviceId, backend, p))
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

  def shutdown()(using ec: ExecutionContext): Future[Unit] = {
    events.put(Exit())
    serviceExecutorPromise.future.foreach(_.shutdown())
    systemTaskRunnerPromise.future.foreach(_.shutdown())
    pendingStartTask.cancel()
    heartbeatTask.cancel()
    checkStorageDeviceTask.cancel()
    usageUpdateTask.foreach(_.cancel())
    shutdownPromise.future
  }
  
  private def closeStore(storeId: StoreId): Future[Unit] = {
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

  /** Testing hook: runs receiveHostMessage's handler synchronously. See
   *  testingOnlyCheckAllDevices for why this bypasses the event queue. */
  private[aspen] def testingOnlyHandleHostMessage(msg: HostMessage): Unit =
    handleEvent(HostMsg(msg))

  /** Testing hook: runs the post-transfer metadata update synchronously.
   *
   *  In production this is driven by a StoreTransferIn completing, which needs a real device
   *  directory and a real byte stream. The metadata half is what the tests are about. */
  private[aspen] def testingOnlyUpdateStateForTransferredStore(storeId: StoreId,
                                                               fromDeviceId: StorageDeviceId,
                                                               toDeviceId: StorageDeviceId): Future[TransferOutcome] =
    updateStateForTransferredStore(storeId, fromDeviceId, toDeviceId)

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
              store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer)

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
        case m: StoreTransferData => transferDataReceived(m)
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
      
      case Repair(storeId, os, completion) => stores.get(storeId).foreach: store =>
        store.repair(os, completion)

      case RecoveryEvent() =>
        handleRecoveryEvent()

      case LoadStore(storageDeviceId, backend, p) =>
        val store = new Store(storageDeviceId, ec, backend, objectCacheFactory(), net, backgroundTasks, crl,
          txStatusCache,finalizerFactory, txDriverFactory, heartbeatPeriod*8)
        backend.setCompletionHandler(ioHandler)
        stores += (backend.storeId -> store)
        offlineStores -= backend.storeId

        if Files.exists(backend.crlSaveFile) then
          val (storeId, trs) = CrashRecoveryLog.loadStoreState(backend.crlSaveFile)
          crl.loadStore(storeId, trs).foreach: _ =>
            Files.delete(backend.crlSaveFile)
            p.success(())
        else
          p.success(())

      case LoadStoreById(storageDeviceId, storeId) =>
        storageDevices.get(storageDeviceId).foreach: sds =>
          tryLoadStore(sds, sds.devicePath.resolve(storeId.directoryName).toFile)
        
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

      case InitializeTaskExecutor() =>
        initializeTaskExecutor()

      case ShutdownStore(storeId, completion) =>
        stores.get(storeId) match
          case None => completion.success(())
          case Some(store) =>
            stores -= storeId
            offlineStores += storeId
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
