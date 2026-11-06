package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StoreConfig}
import org.aspen_ddp.aspen.server.Host.{BackendFailed, FsifOpened, LoadStore, LoadStoreById, ShutdownStore,
  SpaceWait, StoreCrlLoaded, StoreFailedOnClose, UnloadFailedStore}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifIo, FsifRecovery}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Opens and serves the stores of the devices loaded on this host, and closes them.
 *
 *  The exception is the FSIF opens, which run on `fsifOpenEc`, touch no host state and post their outcome back as
 *  FsifOpened. Loads go back through `ctx.tryLoadStore`/`ctx.loadStore`, which call the overridable methods on Host. */
class StoreLoader(ctx: DeviceControl) extends Logging:
  import ctx.given

  private def router = ctx.router

  // FSIF stores whose backend could not be opened because their device is out of space. Each is
  // offline and unloaded, and ScanDevices (every deviceScanPeriod, 10 s) tries the load again with
  // an exponential backoff counted in scans: next scan, then 2, 4, ... scans later, capped at
  // SpaceRetryCap. Each retry runs recovery on fsifOpenEc, never the control thread. A store
  // leaves this map when its load succeeds or fails for any other reason. Never marked for
  // rebuild: the store's data is intact, it just cannot finish recovery until space returns.
  private var storesAwaitingSpace: Map[StoreId, SpaceWait] = Map()
  // FSIF stores whose open (recovery) or create runs on fsifOpenEc, each with the ShutdownStore
  // completions that arrived meanwhile. Such a store is in its device's loadedStores, so a
  // shutdown closes it; its ShutdownStore is put again once the open's outcome is handled, after
  // the LoadStore that serves it, so it is closed like any served store.
  private var fsifOpening: Map[StoreId, List[Promise[Unit]]] = Map()

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

  // `onServed` runs, on the control thread, once the store is recorded as served and its LoadStore queued:
  // before this returns for RocksDB, but only when FsifOpened is handled for FSIF, whose open runs on fsifOpenEc.
  // It never runs if the store is not served (skipped, already open or opening, or its open fails), except that an
  // FSIF open that runs out of space hands it to the store's next open (SpaceWait). It runs at most once. A caller
  // that must act once the store is served passes it rather than reading offlineStores after the call.
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
        else if isFsif(storeCfg) then
          // Recovery may read and fsync a whole store: it runs on fsifOpenEc, and FsifOpened serves the store
          if fsifOpening.contains(storeCfg.storeId) || router.get(storeCfg.storeId).isDefined then
            logger.debug(s"Not loading store ${storeCfg.storeId} again: it is already open or opening")
          else
            startFsifOpen(sds, storeCfg, potentialStoreFile, create = false, None, onServed)
        else
          serveOpened(sds, storeCfg, potentialStoreFile, openBackend(storeCfg, potentialStoreFile, create = false))
          onServed.foreach(_())
      catch
        case t: Throwable => logger.warn(s"Failed to load store $potentialStoreFile. Error: $t")

  /** Records an opened backend as loaded and queues its LoadStore. */
  private[host] def serveOpened(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                                  backend: Backend): Future[Unit] =
    sds.loadedStores += backend.storeId
    sds.offlineStores -= backend.storeId
    router.clearOffline(backend.storeId)
    logger.info(s"Loading store ${storeCfg.storeId}: $storePath")
    ctx.loadStore(sds.storageDeviceId, backend, storeCfg.effectiveMaxSliceSize.map(_.toLong))

  /** Runs an FSIF open (recovery) or create on fsifOpenEc, off the control thread (the spec's anti-goal: never block it
   *  on I/O or fsync), and posts the outcome as FsifOpened. Meanwhile the store is in its device's loadedStores and
   *  in fsifOpening, so a shutdown waits for it and no second open starts. */
  private[host] def startFsifOpen(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
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
    fsifOpening += storeId -> Nil
    sds.loadedStores += storeId
    try
      // Leaves the control context: the open runs on fsifOpenEc and its result comes back as a posted FsifOpened.
      ctx.backends.fsifOpenEc.execute: () =>
        val result = Try(openBackend(storeCfg, storePath, create))
        ctx.post(FsifOpened(sds, storeCfg, storePath, create, previous, result, created, servedCallback))
    catch
      case t: Throwable =>
        fsifOpening -= storeId
        sds.loadedStores -= storeId
        throw t

  /** The outcome of startFsifOpen, on the control thread:
   *  - opened: the store is served (LoadStore, which retires a backend that failed-stop meanwhile), unless its
   *    device was unloaded meanwhile or another backend serves it, when it is closed unserved;
   *  - FsifRecovery.StoreLost: the store's acknowledged state is gone. It is marked rebuild-required and left
   *    offline, as crlSaveFileLost leaves a store, and the reconcile then flips its entry to Rebuilding;
   *  - out of space (FsifIo.isOutOfSpace): the data is intact, so the store is left offline and unloaded, recorded
   *    in storesAwaitingSpace, and ScanDevices retries it with backoff;
   *  - any other failure is logged and the store is not loaded, as for a backend that fails to open.
   *  ShutdownStore completions deferred meanwhile are then put again, after the LoadStore, and then the load's
   *  onServed callback, if any, runs for a served store. */
  def handleFsifOpened(event: FsifOpened): Unit =
    val FsifOpened(sds, storeCfg, storePath, create, previous, result, created, onServed) = event
    val storeId = storeCfg.storeId
    val deferred = fsifOpening.getOrElse(storeId, Nil)
    fsifOpening -= storeId
    val registered = ctx.devices.devices.get(sds.storageDeviceId).exists(_ eq sds)
    var servedCallback: Option[() => Unit] = None
    def leaveOffline(): Unit =
      router.markOffline(storeId)
      sds.offlineStores += storeId
      sds.loadedStores -= storeId
    result match
      case Success(backend) if !registered || router.get(storeId).isDefined =>
        logger.warn(s"Not serving store $storeId opened at $storePath: its device was unloaded or another " +
                    s"backend serves it meanwhile. Closing it")
        if router.get(storeId).isEmpty then
          sds.loadedStores -= storeId
        if backendHasFailed(backend) then
          try ctx.rebuilds.markRebuildRequired(storePath)
          catch case t: Throwable => logger.error(s"Failed to mark store $storeId for rebuild at $storePath: $t")
        backend.close()
        created.foreach(_.tryFailure(new IllegalStateException(s"store $storeId was opened but not served")))
      case Success(backend) =>
        val served = serveOpened(sds, storeCfg, storePath, backend)
        created.foreach(_.completeWith(served))
        servedCallback = onServed
      case Failure(e: FsifRecovery.StoreLost) =>
        logger.error(s"Store $storeId cannot be loaded: FSIF recovery found its acknowledged state lost. " +
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
      case Failure(e) if FsifIo.isOutOfSpace(e) =>
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
        created.foreach(_.tryFailure(e))
    deferred.reverse.foreach(c => ctx.post(ShutdownStore(storeId, c)))
    // After the replay, so a callback that throws cannot lose a deferred shutdown's completion
    servedCallback.foreach: callback =>
      try callback()
      catch case NonFatal(t) => logger.warn(s"The served callback of store $storeId failed: $t")

  private[host] def isFsif(storeCfg: StoreConfig): Boolean = storeCfg.backend.isInstanceOf[StoreConfig.Fsif]

  /** Called once per device scan. Retries the load of every store in storesAwaitingSpace whose backoff has run
   *  out, whose device is still loaded here and not shutting down, and which is still offline and unloaded. Others
   *  are dropped, with their onServed callback unrun: whatever replaced or removed them has taken over. A retry's
   *  open takes the callback over from the entry (startFsifOpen). */
  def retryStoresAwaitingSpace(): Unit =
    storesAwaitingSpace.foreach: (storeId, wait) =>
      val deviceId = wait.sds.storageDeviceId
      if ! ctx.devices.devices.get(deviceId).exists(_ eq wait.sds) || router.get(storeId).isDefined ||
         ! wait.sds.offlineStores.contains(storeId) || fsifOpening.contains(storeId) ||
         ! os.exists(wait.storePath / StoreConfig.configFilename) then
        storesAwaitingSpace -= storeId
      else if wait.scansLeft > 1 then
        storesAwaitingSpace += storeId -> wait.copy(scansLeft = wait.scansLeft - 1)
      else if ! ctx.shutdown.isShuttingDown(deviceId) then
        ctx.tryLoadStore(wait.sds, wait.storePath, None)

  /** Opens (or, with `create`, creates) the backend `storeCfg` names at `storePath`. A RocksDB backend is opened on
   *  the control thread; an FSIF backend on fsifOpenEc (startFsifOpen), touching no Host state. */
  private[host] def openBackend(storeCfg: StoreConfig, storePath: os.Path, create: Boolean): Backend =
    val ref = new AtomicReference[Backend]()
    // Called once, on an FSIF io thread: it only posts BackendFailed to the control thread, so it never blocks there.
    val onFatal: Throwable => Unit = err => ctx.post(BackendFailed(storeCfg.storeId, ref, err))
    val backend = ctx.backends.open(storeCfg, storePath, create, onFatal)
    ref.set(backend)
    backend

  private[host] def backendHasFailed(backend: Backend): Boolean = backend match
    case b: FsifBackend => b.hasFailed
    case _ => false

  def closeStore(storeId: StoreId): Future[Unit] =
    val p = Promise[Unit]()
    ctx.post(ShutdownStore(storeId, p))
    p.future

  def handleLoad(e: LoadStore): Unit =
    val LoadStore(storageDeviceId, backend, maxSliceSize, p) = e
    // The save file is read before the Store exists: a store whose CRL state cannot be
    // recovered must never serve, not even briefly.
    val failed = backendHasFailed(backend)
    val savedState =
      if failed then
        // Failed-stop between its open and this event. Its BackendFailed found nothing to
        // unload, or will, so it is retired here.
        Left(new java.io.IOException(s"the backend of store ${backend.storeId} failed before it was served"))
      else if os.exists(backend.crlSaveFile) then
        CrashRecoveryLog.loadValidStoreState(backend.crlSaveFile, backend.storeId).map(Some(_))
      else
        Right(None)

    savedState match
      case Left(err) if failed =>
        logger.error(s"Store ${backend.storeId} cannot be loaded: its backend failed. Rebuilding it. Error: $err")
        ctx.rebuilds.retireUnserved(storageDeviceId, backend)
        p.failure(err)

      case Left(err) =>
        ctx.rebuilds.crlSaveFileLost(storageDeviceId, backend, err)
        p.failure(err)

      case Right(trs) =>
        // The store's transaction drivers run on the host's ec, not the control context; they post back as messages
        val store = new Store(storageDeviceId, ctx.ec, backend, ctx.objectCacheFactory(backend.storeId), ctx.net,
          ctx.backgroundTasks, ctx.crl, ctx.txStatusCache, ctx.finalizerFactory, ctx.txDriverFactory,
          Host.txHeartbeatTimeout(ctx.heartbeatPeriod), maxSliceSize)
        backend.setCompletionHandler(ctx.ioHandler)
        router.install(store)

        trs match
          case None => p.success(())
          case Some(trs) =>
            // Its transactions are recreated, and its transaction messages handled, once the CRL holds them
            store.frontend.awaitRecovery()
            ctx.crl.loadStore(backend.storeId, trs).onComplete:
              case Success(_) =>
                ctx.post(StoreCrlLoaded(backend.storeId, store, backend.crlSaveFile, p))
              case Failure(err) =>
                // The save file stays, so a restart tries again. Until then the store must
                // not serve without the state the file holds.
                logger.error(s"Failed to load the CRL state of store ${backend.storeId}. Taking it " +
                             s"offline. Error: $err")
                ctx.post(UnloadFailedStore(backend.storeId, store))
                p.failure(err)

  def handleCrlLoaded(e: StoreCrlLoaded): Unit =
    val StoreCrlLoaded(storeId, store, saveFile, p) = e
    // A store closed meanwhile keeps its save file, which its close rewrote, and recovers from it when next loaded
    if router.get(storeId).exists(_ eq store) then
      os.remove(saveFile)
      store.frontend.recover()
    p.success(())

  def handleUnloadFailed(e: UnloadFailedStore): Unit =
    val UnloadFailedStore(storeId, store) = e
    if router.get(storeId).exists(_ eq store) then
      router.remove(storeId)
      store.objectCache.clear()
      ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
        sds.offlineStores += storeId
      // Drops whatever part of the saved state did reach the CRL. The save file is not
      // rewritten from it: the file on disk is still the whole state.
      ctx.crl.closeStore(storeId).onComplete: _ =>
        store.close()

  def handleLoadById(e: LoadStoreById): Unit =
    ctx.devices.devices.get(e.sstorageDeviceId).foreach: sds =>
      ctx.tryLoadStore(sds, sds.devicePath / e.toreId.directoryName, None)

  def handleClose(e: ShutdownStore): Unit =
    val ShutdownStore(storeId, completion) = e
    router.get(storeId) match
      case None if fsifOpening.contains(storeId) =>
        // Closed once its open ends and it is served (fsifOpened puts this event again)
        fsifOpening += storeId -> (completion :: fsifOpening(storeId))
      case None => completion.success(())
      case Some(store) =>
        store.backend match
          case b: FsifBackend if b.isWriteFrozen =>
            // The close abandons the held writes as a crash would (Ruling 27): they were never
            // acknowledged, so the CRL saved below keeps their data, and acknowledged commits are
            // durable in the store's journal. It completes without waiting for space.
            logger.warn(s"Closing store $storeId while its device is out of space: its held writes are " +
                        s"abandoned, as in a crash, and redone from the CRL when it next loads")
          case _ =>
        router.remove(storeId)
        store.objectCache.clear()
        ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
          sds.offlineStores += storeId
        ctx.crl.closeStore(storeId).foreach: trs =>
          CrashRecoveryLog.saveStoreState(storeId, trs, store.backend.crlSaveFile)
          store.close().foreach: _ =>
            // An FSIF close completes even after a fail-stop during it, and this store is no
            // longer served, so its BackendFailed (if any) would find nothing to act on.
            if backendHasFailed(store.backend) then
              ctx.rebuilds.failedOnClose(store)
              completion.failure(StoreFailedOnClose(storeId))
            else
              completion.success(())
