package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StoragePool, Transaction, ObjectState as ClientObjectState}

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
                   val storeTransferFactory: StoreTransferFactory = StoreTransferFactory.Filesystem) extends Logging {
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

  protected var storageDevices: Map[StorageDeviceId, LocalStorageDeviceState] = Map()
  protected var stores: Map[StoreId, Store] = Map()

  private var offlineStores: Set[StoreId] = Set()
  private var creatingStores: Set[StoreId] = Set()
  private var transferringOut: Map[StoreId, StoreTransferOut] = Map()
  private var transferringInUUIDs: Map[UUID, StoreTransferIn] = Map()
  private var transferringInStoreIds: Set[StoreId] = Set()
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
   *  LocalStorageDeviceState -- and with it the offlineStores set that checkStorageDevice's
   *  check() reads -- survives, and its children are not re-offered to tryLoadStore over
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

  private def updateStateForTransferredStore(storeId: StoreId,
                                             fromDeviceId: StorageDeviceId,
                                             toDeviceid: StorageDeviceId): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      
      def prepUpdateBootstrapConfig(poolCfg: StoragePoolState, toDevice: StorageDeviceState): Future[Unit] =
        if storeId.poolId != PoolId.BootstrapPoolId then
          Future.unit
        else 
          for
            toHost <- client.getHostState(toDevice.hostId)
            poolHosts <- Future.sequence(poolCfg.stores.zipWithIndex.toList.map((e, index) => client.getHostState(e.hostId).map(host => (StoreId(storeId.poolId, index.toByte), host))))
            radicleKvos <- client.read(client.radicle)
          yield
            val hostsMap = poolHosts.map((_, host) => host.hostId -> host).toMap + (toHost.hostId -> toHost)
            val hostsList = hostsMap.valuesIterator.toList
            val storeMap = poolHosts.map: (sid, host) =>
              if sid == storeId then
                (sid, toHost.hostId)
              else
                (sid, host.hostId)
                
            val bootstrapConfig = BootstrapConfig.generateBootstrapConfig(
              aspenSystemId,
              poolCfg.ida,
              hostsList,
              storeMap
            )

            val reqs = List(KeyRevision(Radicle.BootstrapConfigKey, radicleKvos.contents(Radicle.BootstrapConfigKey).revision))
            val ops = List(Insert(Radicle.BootstrapConfigKey, bootstrapConfig.getBytes(StandardCharsets.UTF_8)))
            tx.update(client.radicle, None, None, reqs, ops)

      for
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        fromDevPtr <- client.getStorageDevicePointer(fromDeviceId)
        toDevPtr <- client.getStorageDevicePointer(toDeviceid)
        poolKvos <- client.read(poolPtr)
        fromDevKvos <- client.read(fromDevPtr)
        toDevKvos <- client.read(toDevPtr)
        poolCfg = StoragePoolState(poolKvos)
        toDev = StorageDeviceState(toDevKvos)
        _ <- prepUpdateBootstrapConfig(poolCfg, toDev)
      yield
        val fromDev = StorageDeviceState(fromDevKvos)

        // If the from device doesn't contain the storeId, we're already done.
        // A concurrent call to this method must have succeeded
        if fromDev.stores.contains(storeId) then
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
            loadStoreById(toDeviceid, storeId)
            updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid)
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
        pool <- client.getStoragePool(m.storeId.poolId)
        pstate <- pool.getState()
        poolEntry = pstate.stores(m.storeId.poolIndex)
        fromDevice <- client.getStorageDeviceState(poolEntry.storageDeviceId)
        devEntry = fromDevice.stores.get(m.storeId) match
          case None => err(s"Store ${m.storeId} missing from device. Transfer probably completed")
          case Some(e) => e
        toDeviceId = devEntry.transferDevice match
          case None => err(s"Store ${m.storeId} not in transfer state. Transfer probably completed")
          case Some(sid) => sid
        toDevice <- client.getStorageDeviceState(toDeviceId)
        sourceDs = storageDevices.get(fromDevice.storageDeviceId) match
          case None => err(s"Source storage device for transfer ${fromDevice.storageDeviceId} not loaded. Disk removed?")
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

  private def updateHostId(storageDeviceId: StorageDeviceId): Future[Unit] =
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

  /** Reads the state recorded for `storageDeviceId` in the storage-devices tree.
   *
   *  A seam rather than a direct client call so a test can hold a lookup in flight while the
   *  device it names is loaded underneath it. That interleaving is what a device appearing on
   *  disk during its own check produces, and it cannot be staged through the real client,
   *  whose reads resolve on their own schedule.
   *
   *  Called while holding the instance lock, so an override must return promptly rather than
   *  block: the lock it holds is the one handleEvent takes.
   */
  protected def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    client.getStorageDeviceState(storageDeviceId)

  /** Reconciles a loaded device's on-disk state against the state recorded for it in the
   *  storage-devices tree: deletes stores transferred away, creates Initializing stores, and
   *  starts transfers in.
   *
   *  If the tree records a different host, none of that happens: the device has migrated here,
   *  so this claims it with updateHostId and re-requests the check once the claim commits.
   *
   *  Callers hold the instance lock; the continuations it registers do not.
   */
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
    if remote.hostId != hostId then
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
            startStoreTransferIn(storeId, fromDevice.hostId, fromDeviceId, local.storageDeviceId)

  /** Requests a check of one storage device.
   *
   *  At most one lookup per device is outstanding at a time. A request arriving while one is
   *  in flight is deferred rather than dropped: dropping it costs a full
   *  checkStorageDevicePeriod -- Main.CheckStorageDevicesPeriod, an hour at present -- which
   *  is the same cost as losing the notification message outright. At most one deferral is
   *  held per device, so this still throttles the pile-up of checks that builds up over an
   *  offline period.
   */
  private def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =
    synchronized:
      if activeDeviceChecks.contains(storageDeviceId) then
        deferredDeviceChecks += storageDeviceId
      else
        startDeviceCheck(storageDeviceId)

  /** Issues the one outstanding lookup for a device and acts on its result: a loaded device
   *  is reconciled against the tree, an unloaded one has its stores added to offlineStores so
   *  they stay silent rather than answering UnknownStore while the device is down, and a
   *  failed lookup is warned about. The guard entry is then released and any request deferred
   *  behind it re-dispatched.
   *
   *  The lookup is issued under the caller's lock and the callback re-takes it, so the whole
   *  of reconcileDeviceState -- filesystem work included -- runs with the event loop stalled.
   *  That same lock is what makes the load-state re-read safe: it is the one handleEvent
   *  takes, so no device can be loading while the callback runs.
   *
   *  Caller holds the instance lock and has established that no check is active for this
   *  device.
   *
   *  The loaded/unloaded branch is chosen when the lookup completes, not when it is issued.
   *  Runtime device discovery can load a device while its check is in flight, and a branch
   *  chosen at dispatch time would then mark a loaded device's stores offline -- re-adding ids
   *  that tryLoadStore and the LoadStore handler have just removed, and that nothing removes
   *  again: the only other site that clears offlineStores is reconcileDeviceState's
   *  deleted-stores pass, which touches ids recorded in the device's own offlineStores set,
   *  which these never enter.
   *
   *  The entry must be released on both outcomes of the lookup and on a throw out of the
   *  callback body, hence the finally. That last is not hypothetical: the reconcile touches
   *  the filesystem and issues transactions, so it can throw. Releasing only on success would
   *  skip every later check of that device for the life of the process. A synchronous throw
   *  from lookupStorageDeviceState, before the Future exists, would still leak it -- known,
   *  tracked in TODO.txt, and deliberately not guarded here.
   */
  private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
    activeDeviceChecks += storageDeviceId

    lookupStorageDeviceState(storageDeviceId).onComplete: result =>
      synchronized:
        try
          result match
            case Success(remote) =>
              // Load state re-read here, not at dispatch. See the scaladoc.
              storageDevices.get(storageDeviceId) match
                case Some(local) => reconcileDeviceState(local, remote)
                case None =>
                  // Find out what stores are on the offline/failed store and add them to our
                  // offlineStores set. We don't want to send "UnknownStore" responses while
                  // the device is down
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
                else "unloaded storage device"
              logger.warn(s"Failed to read state for $what $storageDeviceId. It may not " +
                          s"be registered in the storage-devices tree. Error: $err")
        // The deferral flag is cleared before the re-dispatch, not after. No test can tell the
        // two apart, because onComplete never runs inline on the ExecutionContexts used today,
        // so the nested callback cannot re-enter this finally while the flag is still set.
        // Under an inline or parasitic EC the other order recurses without bound, and it also
        // strands the flag if lookupStorageDeviceState throws synchronously.
        finally
          activeDeviceChecks -= storageDeviceId
          if deferredDeviceChecks.contains(storageDeviceId) then
            deferredDeviceChecks -= storageDeviceId
            startDeviceCheck(storageDeviceId)

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
