package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.LogRateLimit
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, Host, StoreConfig}
import org.aspen_ddp.aspen.server.Host.{BackendFailed, BackendOpened, LoadStore, LoadStoreById, ShutdownStore,
  LoadRetry, StoreFailedOnClose, UnloadFailedStore}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, StoreRecoveryState}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.{Store, StoreIncarnationFile}
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendStateLost, StoreCreation}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Opens and serves the stores of the devices loaded on this host, and closes them.
 *
 *  The exception is backend opens, which run wherever the BackendOpener puts them: one that does not complete in the
 *  caller touches no host state and posts its outcome back as BackendOpened. Every backend takes the same path; none
 *  is named here. Loads go back through `ctx.tryLoadStore`/`ctx.loadStore`, which call the overridable methods on
 *  Host. */
class StoreLoader(ctx: DeviceControl) extends Logging:
  import ctx.given

  private def router = ctx.router

  // Stores whose load failed with their data intact: their device is out of space, the backend open failed for any
  // reason but a lost state (an I/O error, a lock or resource error), the config could not be read, no executor
  // slot was free (HostThreading.AlreadyAssigned), or its Store failed to build (handleLoad). Each is offline and
  // unloaded,
  // and ScanDevices (every deviceScanPeriod, 10 s) tries the load again with an exponential backoff counted in
  // scans: next scan, then 2, 4, ... scans later, capped at LoadRetryCap. Each retry opens the store through
  // startOpen, never blocking the control thread. A store leaves this map when a load starts; a retry that fails the
  // same way puts it back with a doubled wait. Never marked for rebuild: nothing says its data is gone. Without the
  // retry a store whose entry is Active would stay unserved for the life of the process while its previous copy is
  // released (cross-document-review 2.2). A store whose crl_save.log state its device's CRL cannot admit now
  // (handleLoad, crash-recovery-log 6.1 A3) waits here too, retried at every scan: its CRL frees room as transactions
  // conclude.
  private var storesAwaitingRetry: Map[StoreId, LoadRetry] = Map()
  // Stores whose backend open (recovery) or create is in progress (startOpen), each with the ShutdownStore
  // events that arrived meanwhile, newest first. Such a store is in its device's loadedStores, so a
  // shutdown closes it; its ShutdownStore is put again once the open's outcome is handled, after
  // the LoadStore that serves it, so it is closed like any served store.
  private var opening: Map[StoreId, List[ShutdownStore]] = Map()
  // How many opens each store has started in this process (startOpen). Compared across an asynchronous step, it
  // says whether the store was opened at any point in between, even if closed again since
  private var openCounts: Map[StoreId, Long] = Map()

  // Stores whose backend this process created (startOpen with `create`: a new pool's store, StoreRebuilds.createNewStore),
  // from their open until their Store is built. Such a store has never served, so it cannot have joined and forgotten a
  // transaction: it records no load time (Frontend.loadedAt), and joins transactions started at once. Every other load
  // (a restart, an adoption, a transfer in) records its load, which raises its forget floor: by MaxStartLead, or by
  // RebuildingStore.AcceptorSinceMargin soon after the store arrived from another host (StoreLoader.ArrivedAtFile)
  private var newStores: Set[StoreId] = Set()
  // The retry wait, in device scans, of each store whose last LoadStore failed to build its Store (handleLoad). A
  // build failure has no open of its own to carry the previous wait (startOpen takes the store out of
  // storesAwaitingRetry), so it is kept here, doubled per failure in a row up to the cap, and dropped once the store
  // is installed
  private var buildRetryWaits: Map[StoreId, Int] = Map()

  /** LoadRetryCap in device scans, at least 1 */
  private val loadRetryCapScans: Int =
    val period = ctx.deviceScanPeriod
    if period.isFinite && period.toMillis > 0 then
      math.max(1L, Host.LoadRetryCap.toMillis / period.toMillis).toInt
    else
      1

  /** Whether a store awaiting a load retry holds a real onServed callback; None if it awaits none */
  private[server] def awaitingRetryHasCallback(storeId: StoreId): Option[Boolean] =
    storesAwaitingRetry.get(storeId).map(_.onServed.isDefined)

  /** The wait, in device scans, of a store awaiting a load retry; None if it awaits none */
  private[server] def awaitingRetryWaitScans(storeId: StoreId): Option[Int] =
    storesAwaitingRetry.get(storeId).map(_.waitScans)

  // `onServed` runs, on the control thread, once the store is served: once the LoadStore its open queues has
  // installed it and it has recovered (its LoadStore's future succeeds), whether the backend opened in the caller
  // (RocksDB) or off the control thread. It never runs if the store is not served (skipped, already open or opening,
  // its open fails, or its LoadStore leaves it offline), except that a load that fails with its data intact (an open
  // error, an unreadable config, no executor slot, no CRL room yet) hands it to the store's next open (LoadRetry). It
  // runs at most once. A caller that must act once the store is served passes it rather than reading offlineStores
  // after the call.
  def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: os.Path,
                   onServed: Option[() => Unit] = None): Unit =
    val storeCfgPath = potentialStoreFile / StoreConfig.configFilename
    // Every existence test here is FileStat's: a stat that fails is no absence. A marker taken as absent would load a
    // store it must keep offline, so a failed stat takes the unreadable-config retry like a failed read
    // (cross-document-review 2.2)
    val hasConfig =
      try Right(FileStat.exists(storeCfgPath))
      catch case NonFatal(t) => Left(t)
    hasConfig match
      case Left(t) => awaitUnreadRetry(sds, potentialStoreFile, onServed, t)
      case Right(true) =>
        try
          val storeCfg = StoreConfig.loadStoreConfig(storeCfgPath)
          if FileStat.exists(potentialStoreFile / RebuildingStore.RebuildRequiredMarkerFile) then
            // Its CRL state is gone. The reconcile flips its entry to Rebuilding, and the rebuild
            // discards this directory before writing a fresh copy.
            logger.warn(s"Not loading store ${storeCfg.storeId}: its CRL save file was lost and it " +
                        s"awaits a rebuild. $potentialStoreFile")
            keepMarkedCopyOffline(sds, storeCfg.storeId)
          else if FileStat.exists(potentialStoreFile / StoreTransfer.TransferOutMarkerFile) then
            logger.info("Skipping load of offline store marked for transfer out. " +
                        s"StoreId ${storeCfg.storeId}. $potentialStoreFile")
            keepMarkedCopyOffline(sds, storeCfg.storeId)
          else if FileStat.exists(potentialStoreFile / StoreTransfer.TransferredInMarkerFile) then
            // Arrived, but the pool may still name the source. StoreTransfers.startIn resumes the
            // metadata step for it, and reconcileDeviceState loads it if that step already committed.
            logger.info("Skipping load of transferred-in store awaiting its pool update. " +
                        s"StoreId ${storeCfg.storeId}. $potentialStoreFile")
            keepMarkedCopyOffline(sds, storeCfg.storeId)
          else if FileStat.exists(potentialStoreFile / RebuildingStore.RebuildCompleteMarkerFile) then
            // A finished rebuild whose flip may not have committed (a crash between the move and the flip,
            // rebuild-process 5.7). The rebuild's completion removes the marker and loads it once the flip commits; for
            // an entry already Active, the reconcile does (DeviceReconciler)
            logger.info("Skipping load of rebuilt store awaiting its device entry's flip out of Rebuilding. " +
                        s"StoreId ${storeCfg.storeId}. $potentialStoreFile")
            keepMarkedCopyOffline(sds, storeCfg.storeId)
          else if opening.contains(storeCfg.storeId) || router.get(storeCfg.storeId).isDefined then
            logger.debug(s"Not loading store ${storeCfg.storeId} again: it is already open or opening")
          else
            startOpen(sds, storeCfg, potentialStoreFile, create = false, None, onServed)
        catch
          case t: Throwable => awaitUnreadRetry(sds, potentialStoreFile, onServed, t)
      case Right(false) if os.isDir(potentialStoreFile) && isStoreDirectoryName(potentialStoreFile) =>
        // Never a half-made store (creates, transfers and rebuilds move a store into place whole), so a store whose
        // config was lost. An Active entry naming it is rebuilt by the reconcile's Missing Stores pass
        logger.error(s"Not loading $potentialStoreFile: it has no ${StoreConfig.configFilename}")
      case Right(false) => ()

  /** Records a store directory a marker keeps from loading as offline on its device, and silences the store unless it
   *  is served or opening here. The router's mark is by store id, and the store may be served from another device of
   *  this host (the source of a transfer onto this one, whose path holds an earlier tenancy's copy): marking it would
   *  report that store offline (HostStoreView) and keep the shutdown of its own device from closing it. */
  private def keepMarkedCopyOffline(sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    if router.get(storeId).isEmpty && !opening.contains(storeId) then
      router.markOffline(storeId)
    sds.offlineStores += storeId

  /** A load that threw before any open started: its config could not be read (an I/O error, a resource limit, a
   *  corrupt file), or the stat of its config or a marker failed (FileStat). Also a LoadStore whose save file or
   *  incarnation file could not be read or stat'd (handleLoad), with no open in flight by then. Unless the store is open or opening (an offer repeated over a served
   *  store), it is left offline and put in storesAwaitingRetry, as a failed open is, so the device scans offer it to
   *  tryLoadStore again with backoff. Without this, a load from a pass that removed its marker first (Completed
   *  Transfers In, Completed Rebuilds) or that offers it once (device load, reloadClosedStores) would leave an Active
   *  store unserved and untracked until a restart (cross-document-review 2.2). `onServed` is kept for the load that
   *  serves it. A directory whose name is no store id is only logged. */
  private def awaitUnreadRetry(sds: LocalStorageDeviceState, storePath: os.Path, onServed: Option[() => Unit],
                               err: Throwable): Unit =
    storeIdOf(storePath) match
      case Failure(_) =>
        logger.warn(s"Failed to load store $storePath. Error: $err")
      case Success(storeId) if opening.contains(storeId) || router.get(storeId).isDefined =>
        logger.warn(s"Failed to load store $storeId again over its open or opening copy: $storePath. Error: $err")
      case Success(storeId) =>
        val previous = storesAwaitingRetry.get(storeId)
        val waitScans = previous.map(w => math.min(w.waitScans * 2, loadRetryCapScans)).getOrElse(1)
        if previous.exists(_.waitScans == loadRetryCapScans) then
          // Once per cap interval: a config that stays unreadable leaves the store unserved
          logger.error(s"Store $storeId still cannot be loaded: its directory could not be read. Retrying in " +
                       s"$waitScans device scans. $storePath. Error: $err")
        else if previous.isDefined then
          logger.debug(s"Store $storeId still cannot be loaded. Retrying in $waitScans device scans. Error: $err")
        else
          logger.warn(s"Store $storeId cannot be loaded: its directory could not be read. Leaving it offline and " +
                      s"retrying, with backoff. $storePath. Error: $err")
        router.markOffline(storeId)
        sds.offlineStores += storeId
        storesAwaitingRetry += storeId -> LoadRetry(sds, storePath, waitScans, waitScans,
          composeServed(previous.flatMap(_.onServed), onServed))

  /** The store a directory's name identifies, if it is a store directory name */
  private def storeIdOf(storePath: os.Path): Try[StoreId] =
    Try(StoreId(storePath.last)).filter(_.directoryName == storePath.last)

  private def isStoreDirectoryName(storePath: os.Path): Boolean = storeIdOf(storePath).isSuccess

  /** A waiting load's callback and a new load's, composed only when both are real callbacks, so retries without one
   *  never nest */
  private def composeServed(waiting: Option[() => Unit], current: Option[() => Unit]): Option[() => Unit] =
    (waiting, current) match
      case (Some(w), Some(c)) => Some(() => { w(); c() })
      case _ => waiting.orElse(current)

  /** Whether the store's backend is being opened or created (startOpen) */
  private[host] def isOpening(storeId: StoreId): Boolean = opening.contains(storeId)

  /** How many opens of the store have started in this process */
  private[host] def opensStarted(storeId: StoreId): Long = openCounts.getOrElse(storeId, 0L)

  /** Records an opened backend as loaded and queues its LoadStore. The store keeps its router loading mark until
   *  that LoadStore installs it or gives up (handleLoad). */
  private[host] def serveOpened(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                                  backend: Backend): Future[Unit] =
    sds.loadedStores += backend.storeId
    sds.offlineStores -= backend.storeId
    router.clearOffline(backend.storeId)
    logger.info(s"Loading store ${storeCfg.storeId}: $storePath")
    ctx.loadStore(sds.storageDeviceId, backend, storeCfg.effectiveMaxSliceSize.map(_.toLong), Some(storeCfg.ida),
      storeCfg.admissionTier)

  /** Opens (or, with `create`, creates) a store's backend through the BackendOpener, which decides where the open
   *  runs. An open that has completed by the time the opener returns (one run in the caller) is handled here and
   *  now; any other's outcome is posted back as BackendOpened, so the control thread never waits on I/O or fsync.
   *  Meanwhile the store is in its device's loadedStores and in `opening`, so a shutdown waits for it and no second
   *  open starts, and the router keeps it silent (loading) rather than answering that it is not here. */
  private[host] def startOpen(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                              create: Boolean, created: Option[Promise[Unit]],
                              onServed: Option[() => Unit] = None): Unit =
    val storeId = storeCfg.storeId
    val previous = storesAwaitingRetry.get(storeId)
    storesAwaitingRetry -= storeId
    // A load that failed with its data intact handed its callback to this open, which now runs it, once, if it
    // serves the store
    val servedCallback = composeServed(previous.flatMap(_.onServed), onServed)
    opening += storeId -> Nil
    openCounts += storeId -> (opensStarted(storeId) + 1)
    sds.loadedStores += storeId
    router.markLoading(storeId)
    val fopen =
      try openBackend(storeCfg, storePath, create, sds.devicePath / StoreCreation.CreatingDirectory)
      catch case NonFatal(t) => Future.failed(t)
    def opened(result: Try[Backend], inline: Boolean): BackendOpened =
      BackendOpened(sds, storeCfg, storePath, create, previous, result, created, servedCallback, inline)
    fopen.value match
      case Some(result) => handleBackendOpened(opened(result, inline = true))
      case None =>
        // Leaves the control context: the open runs where the opener put it, and its result comes back as a posted
        // BackendOpened. ExecutionContext.parasitic only posts, so nothing else runs off the control thread.
        fopen.onComplete(result => ctx.post(opened(result, inline = false)))(using ExecutionContext.parasitic)

  /** The outcome of startOpen, on the control thread:
   *  - opened: the store is served (LoadStore, which retires a backend that failed-stop meanwhile), unless its
   *    device was unloaded meanwhile or another backend serves it, when it is closed unserved;
   *  - BackendStateLost: the store's acknowledged state is gone. It is marked rebuild-required and left offline, as
   *    crlSaveFileLost leaves a store, and the reconcile then flips its entry to Rebuilding;
   *  - a failed create that left nothing at the path: left offline; its entry is still Initializing, so the next
   *    device check creates it again;
   *  - any other failure (out of space, Backend.isOutOfSpace, or an I/O, lock or resource error): nothing says the
   *    data is gone, so the store is left offline and unloaded, recorded in storesAwaitingRetry, and ScanDevices
   *    retries it with backoff.
   *  ShutdownStore completions deferred meanwhile are then put again, after the LoadStore. The load's onServed
   *  callback, if any, waits for that LoadStore (awaitServed).
   *
   *  An `inline` outcome is handled within startOpen, where nothing can have changed since the open began; the
   *  device may not be registered yet (DeviceDiscovery registers it once each store has been offered). */
  def handleBackendOpened(event: BackendOpened): Unit =
    val BackendOpened(sds, storeCfg, storePath, create, previous, result, created, onServed, inline) = event
    val storeId = storeCfg.storeId
    val deferred = opening.getOrElse(storeId, Nil)
    opening -= storeId
    val registered = ctx.devices.devices.get(sds.storageDeviceId).exists(_ eq sds)
    def leaveOffline(): Unit =
      router.markOffline(storeId)
      router.clearLoading(storeId)
      sds.offlineStores += storeId
      sds.loadedStores -= storeId
    result match
      case Success(backend) if (!inline && !registered) || router.get(storeId).isDefined =>
        logger.warn(s"Not serving store $storeId opened at $storePath: its device was unloaded or another " +
                    s"backend serves it meanwhile. Closing it")
        if router.get(storeId).isEmpty then
          sds.loadedStores -= storeId
          router.clearLoading(storeId)
        if backend.hasFailed then
          try ctx.rebuilds.markRebuildRequired(storePath)
          catch case t: Throwable => logger.error(s"Failed to mark store $storeId for rebuild at $storePath: $t")
        backend.close()
        created.foreach(_.tryFailure(new IllegalStateException(s"store $storeId was opened but not served")))
      case Success(backend) =>
        if create then
          newStores += storeId
        val served = serveOpened(sds, storeCfg, storePath, backend)
        created.foreach(_.completeWith(served))
        onServed.foreach(awaitServed(storeId, served, _))
      case Failure(e: BackendStateLost) =>
        logger.error(s"Store $storeId cannot be loaded: its backend found its acknowledged state lost. " +
                     s"Rebuilding it. $storePath. Error: ${e.getMessage}")
        leaveOffline()
        try
          ctx.rebuilds.markRebuildRequired(storePath)
        catch
          case t: Throwable =>
            // Offline for the life of this process either way. The next start recovers the
            // same files and lands here again.
            logger.error(s"Failed to mark store $storeId for rebuild at $storePath: $t")
        // A device still being loaded is checked once it is registered.
        if registered then
          ctx.reconciler.checkStorageDevice(sds.storageDeviceId)
        created.foreach(_.tryFailure(e))
      case Failure(e) if create && !os.exists(storePath) =>
        // The create failed and left nothing at the path (StoreCreation), so the entry is still Initializing and the
        // next device check creates it again (DeviceReconciler, StoreRebuilds.createNewStore). A create that
        // succeeded and whose open failed left a complete store, which the next check flips without loading: it
        // takes the load retries below.
        logger.warn(s"Store $storeId cannot be created on device ${sds.storageDeviceId}. Leaving it offline; the " +
                    s"next device check creates it again. Error: $e")
        leaveOffline()
        created.foreach(_.tryFailure(e))
      case Failure(e) =>
        val outOfSpace = Backend.isOutOfSpace(e)
        val waitScans = previous.map(w => math.min(w.waitScans * 2, loadRetryCapScans)).getOrElse(1)
        val why = if outOfSpace then "its device is out of space" else "its backend failed to open"
        if previous.isDefined then
          logger.debug(s"Store $storeId still cannot be loaded: $why. Retrying in $waitScans device scans. " +
                       s"Error: $e")
        else
          logger.warn(s"Store $storeId cannot be loaded: $why. Leaving it offline and retrying, with backoff" +
                      s"${if outOfSpace then ", until space returns" else ""}. $storePath. Error: $e")
        leaveOffline()
        storesAwaitingRetry += storeId -> LoadRetry(sds, storePath, waitScans, waitScans, onServed)
        created.foreach(_.tryFailure(e))
    deferred.reverse.foreach(ctx.post)

  /** Runs a load's onServed `callback` on the control thread once its LoadStore has served the store (`served`
   *  succeeds). A LoadStore that leaves the store offline with its data intact puts it in storesAwaitingRetry
   *  (handleLoad, or the SaveFileRemovalFailed that StoreRouter.crlLoaded posts before failing `served`), and the
   *  callback joins that entry, to run for the open that serves it. The join is posted to controlEvents, behind that
   *  event (lockstep keeps control events and control-context callbacks apart). Any other failure (its device
   *  unloaded, its save file sent to a rebuild, its CRL state failing to load) drops it: whatever takes the store
   *  over next has not served it either. */
  private def awaitServed(storeId: StoreId, served: Future[Unit], callback: () => Unit): Unit =
    served.onComplete:
      case Success(_) =>
        try callback()
        catch case NonFatal(t) => logger.warn(s"The served callback of store $storeId failed: $t")
      case Failure(_) =>
        ctx.threading.controlEvents.execute: () =>
          storesAwaitingRetry.get(storeId).foreach: wait =>
            storesAwaitingRetry += storeId -> wait.copy(onServed = composeServed(wait.onServed, Some(callback)))

  /** Called once per device scan. Retries the load of every store in storesAwaitingRetry whose backoff has run
   *  out, whose device is still loaded here and not shutting down, and which is still offline and unloaded. Others
   *  are dropped, with their onServed callback unrun: whatever replaced or removed them has taken over. A retry's
   *  open takes the callback over from the entry (startOpen). */
  def retryFailedLoads(): Unit =
    storesAwaitingRetry.foreach: (storeId, wait) =>
      val deviceId = wait.sds.storageDeviceId
      if ! ctx.devices.devices.get(deviceId).exists(_ eq wait.sds) || router.get(storeId).isDefined ||
         ! wait.sds.offlineStores.contains(storeId) || opening.contains(storeId) then
        storesAwaitingRetry -= storeId
      else if FileStat.existence(wait.storePath / StoreConfig.configFilename).contains(false) then
        // Nothing left to load. An Active entry naming it is rebuilt by the reconcile's Missing Stores pass. A failed
        // stat is retried as below: tryLoadStore takes it for an unreadable config
        logger.error(s"Store $storeId awaiting a load retry no longer has a ${StoreConfig.configFilename} at " +
                     s"${wait.storePath}. Dropping the retry")
        storesAwaitingRetry -= storeId
      else if wait.scansLeft > 1 then
        storesAwaitingRetry += storeId -> wait.copy(scansLeft = wait.scansLeft - 1)
      else if ! ctx.shutdown.isShuttingDown(deviceId) then
        ctx.tryLoadStore(wait.sds, wait.storePath, None)

  /** Opens the backend `storeCfg` names at `storePath`, wherever the BackendOpener runs it; with `create`, first
   *  creates it there atomically (BackendOpener.create, scratch under `creatingRoot`). A backend's fail-stop is
   *  posted as BackendFailed. */
  private def openBackend(storeCfg: StoreConfig, storePath: os.Path, create: Boolean,
                          creatingRoot: os.Path): Future[Backend] =
    val ref = new AtomicReference[Backend]()
    // Called once, on a backend thread: it only posts BackendFailed to the control thread, so it never blocks there.
    val onFatal: Throwable => Unit = err => ctx.post(BackendFailed(storeCfg.storeId, ref, err))
    val created =
      if create then ctx.backends.create(storeCfg, storePath, creatingRoot)
      else Future.unit
    // parasitic: a create that completed in the caller (RocksDB) opens in the caller too, so the outcome stays inline
    created.flatMap(_ => ctx.backends.open(storeCfg, storePath, onFatal))(using ExecutionContext.parasitic).map { backend =>
      ref.set(backend)
      backend
    }(using ExecutionContext.parasitic)

  /** Closes a store (handleClose). With `extractCrl` (a network transfer out only) its CRL state is taken out of the
   *  device CRL into its crl_save.log, to travel with it; otherwise it stays live in the device CRL, which recovers
   *  it when the store next loads from this device, here or on a host that adopts the device. */
  def closeStore(storeId: StoreId, extractCrl: Boolean): Future[Unit] =
    val p = Promise[Unit]()
    ctx.post(ShutdownStore(storeId, extractCrl, p))
    p.future

  def handleLoad(e: LoadStore): Unit =
    val LoadStore(storageDeviceId, backend, maxSliceSize, p, poolIDA, admissionTier) = e
    // Taken whatever this load's outcome: a later load of the store is not its first
    val isNew = newStores.contains(backend.storeId)
    newStores -= backend.storeId
    // The save file is read before the Store exists: a store whose CRL state cannot be
    // recovered must never serve, not even briefly.
    val failed = backend.hasFailed
    // The store's device's CRL. A store loaded directly onto a device this host has not loaded uses the test seam's
    val deviceCrl = ctx.devices.devices.get(storageDeviceId).map(_.crl).orElse(ctx.unloadedDeviceCrl)
    // Whether the store has a save file: a stat that fails says nothing (FileStat). Taken as absent, it would recover
    // the store from a device CRL that holds none of its state, and the file, still there, would have the next load
    // drop everything saved since (cross-document-review 2.2). So the load is held back and retried instead
    val hasSaveFile = if failed then Success(false) else Try(FileStat.exists(backend.crlSaveFile))
    // The store's identity, read before anything is built. Never created here: every store directory gets one when it
    // is made (StoreCreation, RebuildingStore.finish). One that is missing is lost, which makes the store a new
    // incarnation, and one written fresh over a store keeping its data and CRL state would skip the commits it owes
    // (a recovered commit vote finding its UnackedCommits entry under the old incarnation; cross-document-review 2.2)
    val incarnation =
      if failed then Failure(new IllegalStateException("not read")) else
        Try(StoreIncarnationFile.load(backend.path, backend.storeId))
    val savedState =
      if failed then
        // Failed-stop between its open and this event. Its BackendFailed found nothing to
        // unload, or will, so it is retired here.
        Left(new java.io.IOException(s"the backend of store ${backend.storeId} failed before it was served"))
      else if hasSaveFile.getOrElse(false) then
        CrashRecoveryLog.loadValidStoreState(backend.crlSaveFile, backend.storeId).map(Some(_))
      else
        Right(None)

    // Why the saved state can never fit its device's CRL, if it cannot
    val neverFitsReason = (savedState, deviceCrl) match
      case (Right(Some(state)), Some(crl)) => neverFits(state, crl, admissionOf(storageDeviceId))
      case _ => None

    (savedState, deviceCrl) match
      case (Left(err), _) if failed =>
        logger.error(s"Store ${backend.storeId} cannot be loaded: its backend failed. Rebuilding it. Error: $err")
        ctx.rebuilds.retireUnserved(storageDeviceId, backend)
        router.clearLoading(backend.storeId)
        p.failure(err)

      case _ if hasSaveFile.isFailure || incarnation.failed.toOption.exists(e =>
          !e.isInstanceOf[StoreIncarnationFile.IncarnationLost]) =>
        // Not known to be gone, only unread: left offline, its files untouched, and retried with backoff as an
        // unreadable config is. A reload over a served store leaves that store serving
        val err = hasSaveFile.failed.toOption.orElse(incarnation.failed.toOption).get
        val replaced = router.entry(backend.storeId).isDefined
        if replaced then
          logger.warn(s"Not reloading store ${backend.storeId}: its save file or incarnation file could not be read. " +
                      s"The loaded copy keeps serving. Error: $err")
        abandonLoad(storageDeviceId, backend, replaced, err, p)
        if !replaced then
          ctx.devices.devices.get(storageDeviceId).foreach(awaitUnreadRetry(_, backend.path, None, err))

      case (Left(err), _) =>
        ctx.rebuilds.crlSaveFileLost(storageDeviceId, backend, err)
        router.clearLoading(backend.storeId)
        p.failure(err)

      case _ if incarnation.isFailure =>
        // Its incarnation file is gone: the store's identity is lost with it, so it is rebuilt as a new incarnation
        val err = incarnation.failed.get
        logger.error(s"Store ${backend.storeId} cannot be loaded: its incarnation file is missing. Rebuilding it. " +
                     s"Error: $err")
        ctx.rebuilds.retireUnserved(storageDeviceId, backend)
        router.clearLoading(backend.storeId)
        p.failure(err)

      case (Right(Some(_)), Some(_)) if neverFitsReason.nonEmpty =>
        // No CRL of this device can ever take it (crash-recovery-log 6.1, A3), as for a store moved from a device with
        // a larger CRL: retrying would keep the store offline for good, so it is rebuilt as one whose file is unusable
        val err = new IllegalStateException(s"the CRL state of store ${backend.storeId} can never fit its device's " +
          s"CRL: ${neverFitsReason.get}")
        ctx.rebuilds.crlSaveFileLost(storageDeviceId, backend, err)
        router.clearLoading(backend.storeId)
        p.failure(err)

      case (Right(_), None) =>
        // Its device was unloaded since the open: nothing may serve it
        val err = new IllegalStateException(s"device $storageDeviceId of store ${backend.storeId} is not loaded")
        logger.warn(s"Not loading store ${backend.storeId}: its device $storageDeviceId is not loaded")
        abandonLoad(storageDeviceId, backend, replaced = router.entry(backend.storeId).isDefined, err, p)

      case (Right(trs), Some(crl)) =>
        val storeId = backend.storeId
        // A store loaded again over a loaded one replaces it on the same executor, as the map entry is replaced
        val replacing = router.entry(storeId)
        val executor = replacing match
          case Some(entry) => Success(entry.executor)
          case None => Try(ctx.threading.assign(storeId, storageDeviceId))
        executor match
          case Failure(err) =>
            // HostThreading.AlreadyAssigned: an earlier close has not released its slot yet, and that slot is not
            // this load's to free. assign made no placement, so there is nothing to release. Retried at the next
            // device scan, by when the close has normally released it
            logger.error(s"Store $storeId cannot be loaded: no executor could be assigned to it. Leaving it offline " +
                         s"and retrying at the device scans. Error: $err")
            ctx.devices.devices.get(storageDeviceId).foreach: sds =>
              storesAwaitingRetry += storeId -> LoadRetry(sds, backend.path, 1, 1)
            abandonLoad(storageDeviceId, backend, replaced = false, err, p)
          case Success(executor) =>
            // Everything up to install may throw (the Store constructor, the backend's setCompletionHandler). The
            // store is then not served, so the load is undone here rather than throwing out of the handler.
            // The store's object cache view, once made: a failed build clears it, so it does not stay registered
            var cache: Option[ObjectCache] = None
            // The store, once made: a failed build releases what its recovered transactions were counted for
            var madeStore: Option[Store] = None
            val built = Try:
              val objectCache = ctx.objectCacheFactory(storeId, executor)
              cache = Some(objectCache)
              // Built, wired and set to await its CRL state here, on the control thread, before install publishes
              // it: no other thread can reach it yet, and no message finds it missing once it is registered.
              // Ownership passes to its executor at install (spec section 5, refined).
              // The store's transaction drivers run on the host's ec, not the control context; they post back as
              // messages
              val store = new Store(storageDeviceId, ctx.ec, backend, objectCache,
                ctx.net, ctx.backgroundTasks, crl, ctx.txStatusCache, ctx.finalizerFactory, ctx.txDriverFactory,
                Host.txHeartbeatTimeout(ctx.heartbeatPeriod), maxSliceSize,
                ctx.devices.devices.get(storageDeviceId).map(_.admission).getOrElse(CrlAdmission.Unlimited),
                ctx.unackedLookup, Some(router.post(backend.storeId)),
                RebuildingStore.readAcceptorSince(backend.path),
                inheritedVotesAfter = RebuildingStore.readInheritedVotes(backend.path, ctx.transactionHorizon),
                heldVoteQuery = ctx.heldVoteQuery,
                incarnationOverride = Some(incarnation.get),
                poolIDA = poolIDA,
                admissionTier = admissionTier,
                // Its backend's completion handler is set below, after the Store is built: recovered transactions read
                // their objects once it is installed (awaitCrlLoad)
                deferRecovery = true,
                missingDataWait = ctx.missingDataWait,
                parkedReadWait = ctx.parkedReadWait,
                horizon = ctx.transactionHorizon,
                loadedAt = if isNew then Some(0L) else ctx.storeLoadedAt(storeId),
                arrivedAt = if isNew then None else StoreLoader.readArrivedAt(backend.path),
                joinCeiling = ctx.joinCeiling,
                heartbeatPeriod = FiniteDuration(ctx.heartbeatPeriod.toMillis, MILLISECONDS),
                // On the store's executor: only posts to the control thread
                onFailStop = (s, err) => ctx.post(Host.StoreFailStopped(s, err, StoreRebuilds.FailStopped)))
              madeStore = Some(store)
              val entry = new StoreRouter.Entry(store, executor)
              // Bound to this entry, so a reload's new backend gets its own; wrapped by the test seam if any
              backend.setCompletionHandler(ctx.wrapCompletionHandler(router.completionHandler(entry)))
              entry
            built match
              case Failure(err) =>
                logger.error(s"Failed to build store $storeId. Not serving it. Error: $err")
                // On the executor, where other views post its sheds, and before its slot is released
                cache.foreach(c => executor.execute(() => c.clear()))
                madeStore.foreach(_.frontend.releaseAllAdmissions())
                if replacing.isEmpty then
                  ctx.threading.release(storeId)
                  // Nothing says its data is gone (an EMFILE, an ENOSPC writing a file at build): left offline and
                  // retried at the device scans with backoff, as a failed open is. Without it, an Active store would
                  // stay unserved and untracked until a restart (systemic-loss-protection 4.6)
                  ctx.devices.devices.get(storageDeviceId).foreach: sds =>
                    awaitBuildRetry(sds, backend.path, storeId, err)
                abandonLoad(storageDeviceId, backend, replacing.isDefined, err, p)
              // A crl_save.log state is charged, checked, before any of it is saved (crash-recovery-log 6.1, A3).
              // Retained for the store, the charge is swapped for its recovery's own count (Frontend.recoverState),
              // as what the device's open or its last close retained is: the store's records are never uncounted
              // (A2). Charged here, after the build, so no later failure of the load leaves it charged: a failed
              // loadStore drops the store's entries and releases what is retained for it (handleUnloadFailed)
              case Success(entry) if !trs.forall(state => entry.store.frontend.admission.tryChargeLoaded(storeId,
                  CrlAdmission.recoveredBytes(state), crl.maxTransactionBytes)) =>
                val err = new IllegalStateException(s"the CRL of store $storeId's device has no room for its saved " +
                  s"state now")
                // A full CRL asks to grow, as a transaction refused for room does (Frontend)
                crl.requestGrowth()
                crlRoomLog.admit().foreach: held =>
                  logger.warn(s"Store $storeId is not loaded yet: its crl_save.log state does not fit what its " +
                              s"device's CRL admits now. Leaving it offline, its save file kept, and retrying at the " +
                              s"device scans until the CRL grows (it may need a larger crl-max-streams or more free " +
                              s"space)${LogRateLimit.heldText(held)}")
                executor.execute(() => entry.store.objectCache.clear())
                if replacing.isEmpty then
                  ctx.threading.release(storeId)
                  // Retried as a store awaiting space is, once a scan, until it fits
                  ctx.devices.devices.get(storageDeviceId).foreach: sds =>
                    storesAwaitingRetry += storeId -> LoadRetry(sds, backend.path, 1, 1)
                abandonLoad(storageDeviceId, backend, replacing.isDefined, err, p)
              case Success(entry) =>
                buildRetryWaits -= storeId
                router.install(entry)
                awaitCrlLoad(entry.store, trs, p)

  /** Records a store whose Store failed to build in storesAwaitingRetry: retried next scan, then 2, 4 ... scans
   *  later per build failure in a row, capped at LoadRetryCap, and logged at error once per cap interval. A waiting
   *  entry's onServed is kept for the load that serves it. */
  private def awaitBuildRetry(sds: LocalStorageDeviceState, storePath: os.Path, storeId: StoreId,
                              err: Throwable): Unit =
    val previous = buildRetryWaits.get(storeId)
    val waitScans = previous.map(w => math.min(w * 2, loadRetryCapScans)).getOrElse(1)
    buildRetryWaits += storeId -> waitScans
    if previous.isEmpty || previous.contains(loadRetryCapScans) then
      logger.error(s"Store $storeId cannot be loaded: its Store failed to build. Leaving it offline and retrying in " +
                   s"$waitScans device scans. $storePath. Error: $err")
    else
      logger.debug(s"Store $storeId still fails to build. Retrying in $waitScans device scans. Error: $err")
    val onServed = storesAwaitingRetry.get(storeId).flatMap(_.onServed)
    storesAwaitingRetry += storeId -> LoadRetry(sds, storePath, waitScans, waitScans, onServed)

  /** The admission of a device's CRL, Unlimited for a device not loaded here (the test seam's CRL) */
  private def admissionOf(storageDeviceId: StorageDeviceId): CrlAdmission =
    ctx.devices.devices.get(storageDeviceId).map(_.admission).getOrElse(CrlAdmission.Unlimited)

  /** Why `state` can never be saved in `crl` counting with `admission`, if it cannot: its bytes exceed the most
   *  admission limit the CRL can reach under any device config (maxAdmissionLimitBytes) plus the per-transaction
   *  bound, the most a load is charged against (CrlAdmission.tryChargeLoaded) once the CRL has grown, or one of its
   *  transactions exceeds that bound (crash-recovery-log 10.20). Neither depends on the device config, so no config
   *  edit can declare a state lost. A state within that limit but above the one now is refused by tryChargeLoaded,
   *  which asks the CRL to grow and leaves the store waiting offline (crash-recovery-log 6.1, A3) */
  private def neverFits(state: StoreRecoveryState, crl: CrashRecoveryLog, admission: CrlAdmission): Option[String] =
    val bytes = CrlAdmission.recoveredBytes(state)
    val slack = crl.maxTransactionBytes
    val limit = math.max(admission.limitBytes, crl.maxAdmissionLimitBytes)
    val max = if limit > Long.MaxValue - slack then Long.MaxValue else limit + slack
    state.transactions.find(trs => CrlAdmission.recoveredBytes(trs) > slack) match
      case Some(trs) =>
        Some(s"a transaction of ${CrlAdmission.recoveredBytes(trs)} bytes exceeds its $slack per transaction")
      case None if bytes > max => Some(s"its $bytes bytes exceed the $max it admits for one load at its largest")
      case None => None

  /** A crl_save.log state refused for want of room in its device's CRL is warned of once a minute at most */
  private val crlRoomLog = new LogRateLimit(60_000)

  /** A LoadStore that failed before its store was installed: closes `backend` unserved and fails `p`. Unless an
   *  earlier store is still served in its place (`replaced`), the store is also left offline and dropped from its
   *  device's loadedStores, undoing serveOpened. */
  private def abandonLoad(storageDeviceId: StorageDeviceId, backend: Backend, replaced: Boolean, err: Throwable,
                          p: Promise[Unit]): Unit =
    val storeId = backend.storeId
    router.clearLoading(storeId)
    if ! replaced then
      router.markOffline(storeId)
      ctx.devices.devices.get(storageDeviceId).foreach: sds =>
        sds.loadedStores -= storeId
        sds.offlineStores += storeId
    try
      backend.close().onComplete:
        case Failure(t) => logger.warn(s"Failed to close the unserved backend of store $storeId: $t")
        case Success(_) =>
    catch case NonFatal(t) => logger.warn(s"Failed to close the unserved backend of store $storeId: $t")
    p.tryFailure(err)

  /** The rest of a LoadStore once its store is installed: with no saved state, recovers it from the device CRL and
   *  completes `p` at once; otherwise completes `p` once the CRL holds `trs` and the store has recovered from it. Its
   *  transactions are recreated, and its transaction messages handled, only once it has recovered. */
  private def awaitCrlLoad(store: Store, trs: Option[StoreRecoveryState], p: Promise[Unit]): Unit =
    val storeId = store.storeId
    trs match
      // An earlier load's removal of the save file failed (StoreRouter.crlLoaded): its state is in the device CRL,
      // but the file, gone or not, is removed durably before the store recovers
      case None if router.saveFileRemovalPending(store.backend.crlSaveFile) =>
        router.crlLoaded(store, store.backend.crlSaveFile, p)
      // The device CRL already holds its state: recovered on its executor, ahead of any message it holds
      case None =>
        router.recoverDeferred(store)
        p.success(())
      case Some(trs) =>
        // The save file is the store's whole state: it has served nowhere since the file was written (a load removes
        // the file), so whatever the device CRL holds for it is a copy of part of the file (a crash before the
        // extraction's deletion, or during an earlier load) or left by an earlier tenancy of the store on this device
        // whose copy left its path without a retire, which must never be recovered as its own. Dropped first, durably,
        // so no save of the file meets the drop
        val loaded =
          try store.crl.dropStore(storeId).flatMap(_ => store.crl.loadStore(storeId, trs))
          catch case NonFatal(t) => Future.failed(t)
        loaded.onComplete:
          case Success(_) =>
            // Recovered on the store's executor (StoreRouter.crlLoaded)
            router.crlLoaded(store, store.backend.crlSaveFile, p)
          case Failure(err) =>
            // The save file stays, so a restart tries again. Until then the store must
            // not serve without the state the file holds.
            logger.error(s"Failed to load the CRL state of store $storeId. Taking it " +
                         s"offline. Error: $err")
            ctx.post(UnloadFailedStore(storeId, store))
            p.failure(err)

  /** Unloads a store whose CRL state failed to load, if it is still the one installed. Its close runs on its
   *  executor (closeUnloaded). */
  def handleUnloadFailed(e: UnloadFailedStore): Unit =
    val UnloadFailedStore(storeId, store) = e
    router.entry(storeId).filter(_.store eq store).foreach: entry =>
      router.remove(storeId)
      ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
        sds.offlineStores += storeId
      // Drops whatever part of the saved state did reach the CRL. The save file is not
      // rewritten from it: the file on disk is still the whole state.
      // The save file on disk still holds the whole state, so the entries may go for good
      closeUnloaded(entry, dropDurably = true)

  /** Unloads a store whose crl_save.log could not be removed durably after its load (StoreRouter.crlLoaded), if it
   *  is still the one installed. It has not recovered, so it served nothing: its device-CRL entries, the whole of
   *  its state should the unlink have succeeded, are kept (closeUnloaded without dropDurably). It is left offline
   *  and unloaded, as abandonLoad leaves a store, and retried at every device scan; the retry's load removes the
   *  file durably, forcing the directory even if it is gone, before the store recovers (awaitCrlLoad). */
  def handleSaveFileRemovalFailed(e: Host.SaveFileRemovalFailed): Unit =
    val Host.SaveFileRemovalFailed(storeId, store) = e
    router.entry(storeId).filter(_.store eq store).foreach: entry =>
      router.remove(storeId)
      ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
        sds.loadedStores -= storeId
        sds.offlineStores += storeId
        storesAwaitingRetry += storeId -> LoadRetry(sds, store.backend.path, 1, 1)
      closeUnloaded(entry, dropDurably = false)

  /** Closes the store of `entry`, already removed from the router, without saving its CRL state: clears its object
   *  cache on its executor, then closes it there, and releases its executor slot once the close completes. Nothing
   *  is awaited. With `dropDurably` its entries are deleted from the CRL (CrashRecoveryLog.dropStore), which only a
   *  caller whose state is recoverable elsewhere (a save file still on disk) or whose `rebuild-required` marker is on
   *  disk may ask for; otherwise they stay live in the device CRL, and a restart recovers them. */
  private[host] def closeUnloaded(entry: StoreRouter.Entry, dropDurably: Boolean): Unit =
    val store = entry.store
    startClose(entry):
      store.objectCache.clear()
      if dropDurably then
        store.crl.dropStore(store.storeId).map(_ => true)(using ExecutionContext.parasitic)
      else
        Future.successful(false)
    .onComplete: dropped =>
      onExecutor(entry)(store.close()).onComplete: _ =>
        // Released once the close has retained them: dropped entries are no longer in the CRL
        if dropped == Success(true) then
          store.frontend.admission.releaseRetained(store.storeId)
        ctx.threading.release(store.storeId)

  /** The first step of a close, on the executor of `entry`, whose store the router no longer holds. Every task
   *  queued ahead of it has run against the open store. It then sets `closing`, so I/O completions posted from now on
   *  are dropped, and posts `f` behind it: completions already on the executor's queue (for I/O those tasks issued, a
   *  MapBackend's arriving inline) are delivered first. A completion from another thread that checked `closing` just
   *  before it was set and is queued after `f` is dropped, which is a lost message (spec section 5 allows it). `f`
   *  sets `closed`, so nothing queued behind it reaches the store, and starts the close. Lockstep: both run inline,
   *  as the event loop's ShutdownStore handler did, and a completion is dropped if its queued item drains after the
   *  close began. */
  private def startClose[T](entry: StoreRouter.Entry)(f: => Future[T]): Future[T] =
    onExecutor(entry):
      entry.closing = true
      val p = Promise[T]()
      entry.executor.execute: () =>
        entry.closed = true
        try p.completeWith(f)
        catch case NonFatal(t) => p.failure(t)
      p.future

  /** Runs `f` on the executor of `entry`, whose store the router no longer holds, and completes with its result.
   *  Callbacks on the returned Future name the control context. Lockstep: inline, so the order is the event loop's. */
  private def onExecutor[T](entry: StoreRouter.Entry)(f: => Future[T]): Future[T] =
    val p = Promise[T]()
    entry.executor.execute: () =>
      try p.completeWith(f)
      catch case NonFatal(t) => p.failure(t)
    p.future

  def handleLoadById(e: LoadStoreById): Unit =
    ctx.devices.devices.get(e.sstorageDeviceId).foreach: sds =>
      ctx.tryLoadStore(sds, sds.devicePath / e.toreId.directoryName, None)

  /** Closes a store (spec section 5): removes it from the router, which marks it offline so new traffic gets the
   *  offline handling, then runs the close on its executor, behind any tasks already queued for it: the object cache
   *  cleared and, with `extractCrl`, `crl.readStoreState` reading the store's state, which stays live in the device
   *  CRL; then that state saved to its crl_save.log (extractCrl only) and `store.close()`. Without `extractCrl` no
   *  save file is written. Back on the control thread, its executor slot is released, whatever the outcome, and the
   *  fail-stop check decides the completion; a failed step fails it. Extracted entries are deleted from the device
   *  CRL (memory and files) only once the save file is durable, so a crash or a failed save before that leaves the
   *  device CRL holding them. A store whose backend failed-stop
   *  is marked rebuild-required (failedOnClose); its entries are dropped when the rebuild discards it
   *  (StoreRebuilds.discardLostStore), after that marker is durable. */
  def handleClose(e: ShutdownStore): Unit =
    val ShutdownStore(storeId, extractCrl, completion) = e
    router.entry(storeId) match
      case None if opening.contains(storeId) =>
        // Closed once its open ends and it is served (handleBackendOpened puts this event again)
        opening += storeId -> (e :: opening(storeId))
      case None => completion.success(())
      case Some(entry) =>
        val store = entry.store
        // isWriteFrozen may be read from any thread
        if store.backend.isWriteFrozen then
          // The close abandons the held writes as a crash would: they were never
          // acknowledged, so the CRL state (in the device CRL, or saved below) keeps their data, and
          // acknowledged commits are durable in the store's journal. It completes without waiting for space.
          logger.warn(s"Closing store $storeId while its device is out of space: its held writes are " +
                      s"abandoned, as in a crash, and redone from the CRL when it next loads")
        router.remove(storeId)
        ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
          sds.offlineStores += storeId
        startClose(entry):
          store.objectCache.clear()
          if extractCrl then
            store.crl.readStoreState(storeId).map(Some(_))
          else
            Future.successful(None)
        .transformWith: extractedTry =>
          onExecutor(entry):
            // Closed whatever the CRL step did, so the store's backend and CRL admissions are released; a failed
            // step then fails the close
            val saved = Try(extractedTry.get.foreach(trs =>
              CrashRecoveryLog.saveStoreState(storeId, trs, store.backend.crlSaveFile)))
            store.close().flatMap(_ => Future.fromTry(saved))(using ExecutionContext.parasitic)
          .flatMap: _ =>
            val extracted = extractedTry.get
            // Deleted from the device CRL files only once the save file holding them is durable, so a restart does
            // not recover them on this device too. A crash between the two leaves them in both: harmless, as loading
            // the save file re-saves them under the same ids
            extracted.fold(Future.unit): state =>
              store.crl.deleteTransactions(storeId, state.entryIds).map: _ =>
                // Every entry of the closed store has left the device CRL
                store.frontend.admission.releaseRetained(storeId)
        .onComplete: result =>
          // The close has ended: the backend and directory belong to the control side again
          ctx.threading.release(storeId)
          result match
            case Success(_) =>
              // An FSIF close completes even after a fail-stop during it, and this store is no
              // longer served, so its BackendFailed (if any) would find nothing to act on.
              if store.backend.hasFailed then
                ctx.rebuilds.failedOnClose(store)
                completion.failure(StoreFailedOnClose(storeId))
              else
                completion.success(())
            case Failure(t) =>
              logger.error(s"Failed to close store $storeId: $t")
              completion.failure(t)

object StoreLoader:
  /** In a store directory: this host's wall clock, in milliseconds, when the store arrived from another host, by a
   *  transfer in (StoreTransfers.clearTransferredInMarker) or an adoption of its device (DeviceDiscovery). The store
   *  may have joined, and forgotten, a transaction there started up to MaxStartLead ahead of that host's clock, which
   *  may lead this one's: a load less than RebuildingStore.AcceptorSinceMargin after the arrival raises its forget floor
   *  by that margin (Frontend.arrivedAt). Durable and written before the store can load here, so a crash between the
   *  arrival and the load, or a restart soon after it, keeps the margin. Kept for good: once the margin has passed it
   *  changes nothing, and the next arrival rewrites it. A network transfer may carry the source's copy, which the
   *  arrival here then replaces before the store loads */
  val ArrivedAtFile = "arrived-at"

  def writeArrivedAt(storePath: os.Path): Unit =
    DurableFiles.write(storePath / ArrivedAtFile, System.currentTimeMillis().toString)

  /** The store's last arrival from another host, if any. A file that cannot be read counts as an arrival now: later
   *  than the truth, which only refuses more transactions */
  def readArrivedAt(storePath: os.Path): Option[Long] =
    val file = storePath / ArrivedAtFile
    if !os.exists(file) then
      None
    else
      Some(Try(os.read(file).trim.toLong).getOrElse(System.currentTimeMillis()))
