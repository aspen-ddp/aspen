package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StoreConfig}
import org.aspen_ddp.aspen.server.Host.{BackendFailed, BackendOpened, LoadStore, LoadStoreById, ShutdownStore,
  SpaceWait, StoreFailedOnClose, UnloadFailedStore}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendStateLost}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
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

  // Stores whose backend could not be opened because their device is out of space. Each is
  // offline and unloaded, and ScanDevices (every deviceScanPeriod, 10 s) tries the load again with
  // an exponential backoff counted in scans: next scan, then 2, 4, ... scans later, capped at
  // SpaceRetryCap. Each retry opens the store through startOpen, never blocking the control thread. A store
  // leaves this map when its load succeeds or fails for any other reason. Never marked for
  // rebuild: the store's data is intact, it just cannot finish recovery until space returns.
  private var storesAwaitingSpace: Map[StoreId, SpaceWait] = Map()
  // Stores whose backend open (recovery) or create is in progress (startOpen), each with the ShutdownStore
  // events that arrived meanwhile, newest first. Such a store is in its device's loadedStores, so a
  // shutdown closes it; its ShutdownStore is put again once the open's outcome is handled, after
  // the LoadStore that serves it, so it is closed like any served store.
  private var opening: Map[StoreId, List[ShutdownStore]] = Map()
  // How many opens each store has started in this process (startOpen). Compared across an asynchronous step, it
  // says whether the store was opened at any point in between, even if closed again since
  private var openCounts: Map[StoreId, Long] = Map()

  /** SpaceRetryCap in device scans, at least 1 */
  private val spaceRetryCapScans: Int =
    val period = ctx.deviceScanPeriod
    if period.isFinite && period.toMillis > 0 then
      math.max(1L, Host.SpaceRetryCap.toMillis / period.toMillis).toInt
    else
      1

  /** Whether a store awaiting space holds a real onServed callback; None if it is not awaiting space */
  private[server] def awaitingSpaceHasCallback(storeId: StoreId): Option[Boolean] =
    storesAwaitingSpace.get(storeId).map(_.onServed.isDefined)

  // `onServed` runs, on the control thread, once the store is recorded as served and its LoadStore queued: before
  // this returns when the backend opens in the caller (RocksDB), otherwise once BackendOpened is handled. It never
  // runs if the store is not served (skipped, already open or opening, or its open fails), except that an open that
  // runs out of space hands it to the store's next open (SpaceWait). It runs at most once. A caller that must act
  // once the store is served passes it rather than reading offlineStores after the call.
  def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: os.Path,
                   onServed: Option[() => Unit] = None): Unit =
    val storeCfgPath = potentialStoreFile / StoreConfig.configFilename
    if os.exists(storeCfgPath) then
      try
        val storeCfg = StoreConfig.loadStoreConfig(storeCfgPath)
        if os.exists(potentialStoreFile / RebuildingStore.RebuildRequiredMarkerFile) then
          // Its CRL state is gone. The reconcile flips its entry to Rebuilding, and the rebuild
          // discards this directory before writing a fresh copy.
          logger.warn(s"Not loading store ${storeCfg.storeId}: its CRL save file was lost and it " +
                      s"awaits a rebuild. $potentialStoreFile")
          router.markOffline(storeCfg.storeId)
          sds.offlineStores += storeCfg.storeId
        else if os.exists(potentialStoreFile / StoreTransfer.TransferOutMarkerFile) then
          logger.info("Skipping load of offline store marked for transfer out. " +
                      s"StoreId ${storeCfg.storeId}. $potentialStoreFile")
          router.markOffline(storeCfg.storeId)
          sds.offlineStores += storeCfg.storeId
        else if os.exists(potentialStoreFile / StoreTransfer.TransferredInMarkerFile) then
          // Arrived, but the pool may still name the source. StoreTransfers.startIn resumes the
          // metadata step for it, and reconcileDeviceState loads it if that step already committed.
          logger.info("Skipping load of transferred-in store awaiting its pool update. " +
                      s"StoreId ${storeCfg.storeId}. $potentialStoreFile")
          router.markOffline(storeCfg.storeId)
          sds.offlineStores += storeCfg.storeId
        else if opening.contains(storeCfg.storeId) || router.get(storeCfg.storeId).isDefined then
          logger.debug(s"Not loading store ${storeCfg.storeId} again: it is already open or opening")
        else
          startOpen(sds, storeCfg, potentialStoreFile, create = false, None, onServed)
      catch
        case t: Throwable => logger.warn(s"Failed to load store $potentialStoreFile. Error: $t")

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
    ctx.loadStore(sds.storageDeviceId, backend, storeCfg.effectiveMaxSliceSize.map(_.toLong))

  /** Opens (or, with `create`, creates) a store's backend through the BackendOpener, which decides where the open
   *  runs. An open that has completed by the time the opener returns (one run in the caller) is handled here and
   *  now; any other's outcome is posted back as BackendOpened, so the control thread never waits on I/O or fsync.
   *  Meanwhile the store is in its device's loadedStores and in `opening`, so a shutdown waits for it and no second
   *  open starts, and the router keeps it silent (loading) rather than answering that it is not here. */
  private[host] def startOpen(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                              create: Boolean, created: Option[Promise[Unit]],
                              onServed: Option[() => Unit] = None): Unit =
    val storeId = storeCfg.storeId
    val previous = storesAwaitingSpace.get(storeId)
    storesAwaitingSpace -= storeId
    // A load that failed for lack of space handed its callback to this open, which now runs it, once, if it serves
    // the store. Composed only when both are real callbacks, so retries without one never nest
    val servedCallback = (previous.flatMap(_.onServed), onServed) match
      case (Some(waiting), Some(current)) => Some(() => { waiting(); current() })
      case (waiting, current) => waiting.orElse(current)
    opening += storeId -> Nil
    openCounts += storeId -> (opensStarted(storeId) + 1)
    sds.loadedStores += storeId
    router.markLoading(storeId)
    val fopen =
      try openBackend(storeCfg, storePath, create)
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
   *  - out of space (Backend.isOutOfSpace): the data is intact, so the store is left offline and unloaded, recorded
   *    in storesAwaitingSpace, and ScanDevices retries it with backoff;
   *  - any other failure is logged and the store is not loaded, as for a backend that fails to open.
   *  ShutdownStore completions deferred meanwhile are then put again, after the LoadStore, and then the load's
   *  onServed callback, if any, runs for a served store.
   *
   *  An `inline` outcome is handled within startOpen, where nothing can have changed since the open began; the
   *  device may not be registered yet (DeviceDiscovery registers it once each store has been offered). */
  def handleBackendOpened(event: BackendOpened): Unit =
    val BackendOpened(sds, storeCfg, storePath, create, previous, result, created, onServed, inline) = event
    val storeId = storeCfg.storeId
    val deferred = opening.getOrElse(storeId, Nil)
    opening -= storeId
    val registered = ctx.devices.devices.get(sds.storageDeviceId).exists(_ eq sds)
    var servedCallback: Option[() => Unit] = None
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
        val served = serveOpened(sds, storeCfg, storePath, backend)
        created.foreach(_.completeWith(served))
        servedCallback = onServed
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
      case Failure(e) if Backend.isOutOfSpace(e) =>
        if create then
          // The next check finds the directory and flips the entry without loading it, so it is handed to the
          // out-of-space retries: their load initializes it again (no marker, and the failed initialize left its
          // files empty).
          logger.warn(s"Store $storeId cannot be created: device ${sds.storageDeviceId} is out of space. Leaving " +
                      s"it offline and retrying every device scan. Error: $e")
          leaveOffline()
          storesAwaitingSpace += storeId -> SpaceWait(sds, storePath, 1, 1, onServed)
        else
          val waitScans = previous.map(w => math.min(w.waitScans * 2, spaceRetryCapScans)).getOrElse(1)
          if previous.isDefined then
            logger.debug(s"Store $storeId still cannot be loaded: its device is out of space. Retrying in " +
                         s"$waitScans device scans. Error: $e")
          else
            logger.warn(s"Store $storeId cannot be loaded: its device is out of space. Leaving it offline and " +
                        s"retrying, with backoff, until space returns. $storePath. Error: $e")
          leaveOffline()
          storesAwaitingSpace += storeId -> SpaceWait(sds, storePath, waitScans, waitScans, onServed)
        created.foreach(_.tryFailure(e))
      case Failure(e) =>
        logger.warn(s"Failed to load store $storePath. Error: $e")
        sds.loadedStores -= storeId
        router.clearLoading(storeId)
        created.foreach(_.tryFailure(e))
    deferred.reverse.foreach(ctx.post)
    // After the replay, so a callback that throws cannot lose a deferred shutdown's completion
    servedCallback.foreach: callback =>
      try callback()
      catch case NonFatal(t) => logger.warn(s"The served callback of store $storeId failed: $t")

  /** Called once per device scan. Retries the load of every store in storesAwaitingSpace whose backoff has run
   *  out, whose device is still loaded here and not shutting down, and which is still offline and unloaded. Others
   *  are dropped, with their onServed callback unrun: whatever replaced or removed them has taken over. A retry's
   *  open takes the callback over from the entry (startOpen). */
  def retryStoresAwaitingSpace(): Unit =
    storesAwaitingSpace.foreach: (storeId, wait) =>
      val deviceId = wait.sds.storageDeviceId
      if ! ctx.devices.devices.get(deviceId).exists(_ eq wait.sds) || router.get(storeId).isDefined ||
         ! wait.sds.offlineStores.contains(storeId) || opening.contains(storeId) ||
         ! os.exists(wait.storePath / StoreConfig.configFilename) then
        storesAwaitingSpace -= storeId
      else if wait.scansLeft > 1 then
        storesAwaitingSpace += storeId -> wait.copy(scansLeft = wait.scansLeft - 1)
      else if ! ctx.shutdown.isShuttingDown(deviceId) then
        ctx.tryLoadStore(wait.sds, wait.storePath, None)

  /** Opens (or, with `create`, creates) the backend `storeCfg` names at `storePath`, wherever the BackendOpener runs
   *  it. A backend's fail-stop is posted as BackendFailed. */
  private def openBackend(storeCfg: StoreConfig, storePath: os.Path, create: Boolean): Future[Backend] =
    val ref = new AtomicReference[Backend]()
    // Called once, on a backend thread: it only posts BackendFailed to the control thread, so it never blocks there.
    val onFatal: Throwable => Unit = err => ctx.post(BackendFailed(storeCfg.storeId, ref, err))
    ctx.backends.open(storeCfg, storePath, create, onFatal).map { backend =>
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
    val LoadStore(storageDeviceId, backend, maxSliceSize, p) = e
    // The save file is read before the Store exists: a store whose CRL state cannot be
    // recovered must never serve, not even briefly.
    val failed = backend.hasFailed
    // The store's device's CRL. A store loaded directly onto a device this host has not loaded uses the test seam's
    val deviceCrl = ctx.devices.devices.get(storageDeviceId).map(_.crl).orElse(ctx.unloadedDeviceCrl)
    val savedState =
      if failed then
        // Failed-stop between its open and this event. Its BackendFailed found nothing to
        // unload, or will, so it is retired here.
        Left(new java.io.IOException(s"the backend of store ${backend.storeId} failed before it was served"))
      else if os.exists(backend.crlSaveFile) then
        CrashRecoveryLog.loadValidStoreState(backend.crlSaveFile, backend.storeId).map(Some(_))
      else
        Right(None)

    (savedState, deviceCrl) match
      case (Left(err), _) if failed =>
        logger.error(s"Store ${backend.storeId} cannot be loaded: its backend failed. Rebuilding it. Error: $err")
        ctx.rebuilds.retireUnserved(storageDeviceId, backend)
        router.clearLoading(backend.storeId)
        p.failure(err)

      case (Left(err), _) =>
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
            // this load's to free. assign made no placement, so there is nothing to release.
            logger.error(s"Store $storeId cannot be loaded: no executor could be assigned to it. Error: $err")
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
                ctx.tracelessLookup, Some(router.post(backend.storeId)), Host.txHorizon(ctx.heartbeatPeriod),
                RebuildingStore.readAcceptorSince(backend.path),
                // Its backend's completion handler is set below, after the Store is built: recovered transactions read
                // their objects once it is installed (awaitCrlLoad)
                deferRecovery = true)
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
                abandonLoad(storageDeviceId, backend, replacing.isDefined, err, p)
              case Success(entry) =>
                // Its recovered transactions are counted by the new store, in place of what its last close kept
                if replacing.isEmpty then
                  entry.store.frontend.admission.releaseRetained(storeId)
                router.install(entry)
                awaitCrlLoad(entry.store, trs, p)

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
  private def awaitCrlLoad(store: Store, trs: Option[List[TransactionRecoveryState]], p: Promise[Unit]): Unit =
    val storeId = store.storeId
    trs match
      // The device CRL already holds its state: recovered on its executor, ahead of any message it holds
      case None =>
        router.recoverDeferred(store)
        p.success(())
      case Some(trs) =>
        store.crl.loadStore(storeId, trs).onComplete:
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
          // The close abandons the held writes as a crash would (Ruling 27): they were never
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
            extracted.fold(Future.unit): trs =>
              store.crl.deleteTransactions(storeId, trs.map(_.txd.transactionId)).map: _ =>
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
