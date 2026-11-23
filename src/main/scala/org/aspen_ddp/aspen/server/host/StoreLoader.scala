package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
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
 *  The exception is backend opens, which run on the device's management lane (`DeviceIo`), touch no host state and
 *  post their outcome back as BackendOpened. Every backend takes the same path; none
 *  is named here. Loads go back through `ctx.tryLoadStore`/`ctx.loadStore`, which call the overridable methods on
 *  Host. */
class StoreLoader(ctx: DeviceControl) extends Logging:
  import ctx.given

  private def router = ctx.router

  // A store whose load failed with its data intact (its device is out of space, the backend open failed for any
  // reason but a lost state (an I/O error, a lock or resource error), the config could not be read, no executor
  // slot was free (HostThreading.AlreadyAssigned), its Store failed to build (handleLoad), or the CRL cannot admit its
  // crl_save.log state now (crash-recovery-log 6.1)) is left offline, its copy AwaitingRetry in the copy registry
  // (HostCopies.awaitingRetry), the one home of a retry. ScanDevices (every deviceScanPeriod, 10 s) tries the load
  // again with an exponential backoff counted in scans: next scan, then 2, 4, ... scans later, capped at
  // LoadRetryCap; one waiting for CRL room is retried at every scan, as its CRL frees room as transactions conclude.
  // Each retry opens the store through startOpen, never blocking the control thread. An open takes the retry (Opening);
  // a retry that fails the same way records it again with a doubled wait, read from the retry the open took. Never
  // moved to lost/: nothing says its data is gone. Without the retry a store whose entry is Active would stay
  // unserved for the life of the process while its previous copy is released.
  //
  // An open is its copy's Opening activity, from startOpen until the open fails or, for one opened, until the
  // LoadStore that serveOpened queues is handled (handleLoad, which installs it or gives up within that one control
  // task). Kept through that LoadStore's wait in the queue, when the router has no entry for the store yet, so a
  // second load offer then (a duplicate LoadStoreById) starts no second open (isOpening), and a shutdown closes the
  // store (DeviceShutdown.storesToClose takes Opening copies).

  // The ShutdownStore events that arrived while their store was opening (handleClose), newest first. Put again once
  // the open fails or its LoadStore is handled, so the store is closed like any served one
  private var deferredCloses: Map[StoreId, List[ShutdownStore]] = Map()
  // The backend each store opened by startOpen awaits the install of, with the retry its open took (for a failed
  // build's backoff): from serveOpened until its LoadStore is handled
  private var awaitingInstall: Map[StoreId, (Backend, Option[LoadRetry])] = Map()
  // The id of the last open started in this process (startOpen): each open's Opening activity carries its own
  private var lastOpenId: Long = 0L

  // Stores whose backend this process created (startOpen with `create`: a new pool's store, StoreRebuilds.createNewStore),
  // from their open until their Store is built. Such a store has never served, so it cannot have joined and forgotten a
  // transaction: it records no load time (Frontend.loadedAt), and joins transactions started at once. Every other load
  // (a restart, an adoption, a transfer in) records its load, which raises its forget floor: by MaxStartLead, or by
  // RebuildingStore.AcceptorSinceMargin soon after the store arrived from another host (StoreLoader.ArrivedAtFile)
  private var newStores: Set[StoreId] = Set()

  /** LoadRetryCap in device scans, at least 1 */
  private val loadRetryCapScans: Int =
    val period = ctx.deviceScanPeriod
    if period.isFinite && period.toMillis > 0 then
      math.max(1L, Host.LoadRetryCap.toMillis / period.toMillis).toInt
    else
      1

  /** Records `retry` for `storeId`: its copy is AwaitingRetry (HostCopies.awaitingRetry) */
  private def awaitRetry(site: String, storeId: StoreId, retry: LoadRetry): Unit =
    ctx.copyRegistry.awaitingRetry(site, storeId, retry): Unit

  /** Drops the retry of `storeId`: its copy is Idle */
  private def dropRetry(storeId: StoreId, wait: LoadRetry): Unit =
    ctx.copyRegistry.retryDropped("retryFailedLoads", wait.sds, storeId)

  /** Whether a store awaiting a load retry holds a real onServed callback; None if it awaits none */
  private[server] def awaitingRetryHasCallback(storeId: StoreId): Option[Boolean] =
    ctx.copyRegistry.retryOf(storeId).map(_.onServed.isDefined)

  /** Whether a store awaiting a load retry awaits it on its copy's record (not parked while an unserved backend, or
   *  the store, still closes), so the next device scan considers it; None if it awaits none */
  private[server] def awaitingRetryReady(storeId: StoreId): Option[Boolean] =
    ctx.copyRegistry.retryOf(storeId).map(_ => ctx.copyRegistry.retryReady(storeId))

  /** The wait, in device scans, of a store awaiting a load retry; None if it awaits none */
  private[server] def awaitingRetryWaitScans(storeId: StoreId): Option[Int] =
    ctx.copyRegistry.retryOf(storeId).map(_.waitScans)

  // `onServed` runs, on the control thread, once the store is served: once the LoadStore its open queues has
  // installed it and it has recovered (its LoadStore's future succeeds), whether the backend opened in the caller
  // (RocksDB) or off the control thread. It never runs if the store is not served (skipped, already open or opening,
  // its open fails, or its LoadStore leaves it offline), except that a load that fails with its data intact (an open
  // error, an unreadable config, no executor slot, no CRL room yet) hands it to the store's next open (LoadRetry). It
  // runs at most once. A caller that must act once the store is served passes it rather than reading the copy
  // registry after the call.
  //
  // Every caller offers a Live copy's path (`live/<copy>`): its record's, or, at a device's load, the listing's. Any
  // other path is a caller's bug, logged at error and not loaded.
  def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: os.Path,
                   onServed: Option[() => Unit] = None): Unit =
    if !CopyLayout.isLivePath(potentialStoreFile) then
      logger.error(s"Not loading $potentialStoreFile: it is no Live copy's path (live/<copy>), a bug in the caller")
    else
      val storeCfgPath = potentialStoreFile / StoreConfig.configFilename
      // Every existence test here is FileStat's: a stat that fails is no absence, so a failed stat takes the
      // unreadable-config retry like a failed read
      val hasConfig =
        try Right(FileStat.exists(storeCfgPath))
        catch case NonFatal(t) => Left(t)
      hasConfig match
        case Left(t) => awaitUnreadRetry(sds, potentialStoreFile, onServed, t)
        case Right(true) =>
          try
            val storeCfg = StoreConfig.loadStoreConfig(storeCfgPath)
            val storeId = storeCfg.storeId
            ctx.copyRegistry.readAtLoad(sds, potentialStoreFile, storeId, Phase.Live)
            if isOpening(storeCfg.storeId) || router.get(storeCfg.storeId).isDefined then
              logger.debug(s"Not loading store ${storeCfg.storeId} again: it is already open or opening")
            else if ctx.moves.isPendingLost(sds, storeId) then
              // Its move to lost/ failed: held offline until a check of its device moves it (StoreRebuilds.moveToLost)
              logger.info(s"Not loading store $storeId: its move to lost/ failed and is retried. $potentialStoreFile")
            else if ctx.copyRegistry.isUnsynced(sds, storeId) then
              // A move a crash could undo (CopyMoves): the next check of its device forces its directories
              // and loads it (CopyDecision.Load)
              logger.info(s"Not loading store $storeId yet: a move of its copy is not durable. $potentialStoreFile")
            else
              startOpen(sds, storeCfg, potentialStoreFile, create = false, None, onServed)
          catch
            case t: Throwable => awaitUnreadRetry(sds, potentialStoreFile, onServed, t)
        // Only logged, so a directory whose stat fails is passed over quietly
        case Right(false) if Try(FileStat.isDir(potentialStoreFile)).getOrElse(false) =>
          // Never a half-made store (creates, transfers and rebuilds move a store into place whole), so a store whose
          // config was lost. An Active entry naming it is rebuilt by the reconcile's Missing Stores pass
          logger.error(s"Not loading $potentialStoreFile: it has no ${StoreConfig.configFilename}")
        case Right(false) => ()

  /** A load that threw before any open started: its config could not be read (an I/O error, a resource limit, a
   *  corrupt file), or the stat of its config failed (FileStat). Also a LoadStore whose save file or
   *  incarnation file could not be read or stat'd (handleLoad, `ofLoad`, whose own open is still Opening). Unless the
   *  store is otherwise open or opening (an offer repeated over a served store), it is left offline and AwaitingRetry,
   *  as a failed open is, so the device scans offer it to tryLoadStore again with backoff. Without this, a load from a
   *  pass that moved its copy Live first (Completed Transfers In, Completed Rebuilds) or that offers it once (device
   *  load) would leave an Active store unserved and untracked until a restart. `onServed` is kept for the load that
   *  serves it. A path that names no copy is only logged. */
  private def awaitUnreadRetry(sds: LocalStorageDeviceState, storePath: os.Path, onServed: Option[() => Unit],
                               err: Throwable, ofLoad: Boolean = false): Unit =
    storeIdOf(storePath) match
      case Failure(_) =>
        logger.warn(s"Failed to load store $storePath. Error: $err")
      case Success(storeId) if (!ofLoad && isOpening(storeId)) || router.get(storeId).isDefined =>
        logger.warn(s"Failed to load store $storeId again over its open or opening copy: $storePath. Error: $err")
      case Success(storeId) =>
        val previous = ctx.copyRegistry.retryOn(sds, storeId)
        val waitScans = nextWait(previous)
        if previous.exists(_.waitScans == loadRetryCapScans) then
          // Once per cap interval: a config that stays unreadable leaves the store unserved
          logger.error(s"Store $storeId still cannot be loaded: its directory could not be read. Retrying in " +
                       s"$waitScans device scans. $storePath. Error: $err")
        else if previous.isDefined then
          logger.debug(s"Store $storeId still cannot be loaded. Retrying in $waitScans device scans. Error: $err")
        else
          logger.warn(s"Store $storeId cannot be loaded: its directory could not be read. Leaving it offline and " +
                      s"retrying, with backoff. $storePath. Error: $err")
        awaitRetry("awaitUnreadRetry", storeId, LoadRetry(sds, storePath, waitScans, waitScans,
          composeServed(previous.flatMap(_.onServed), onServed)))

  /** The store a copy's path identifies, from its copy name (CopyLayout.parseName) */
  private def storeIdOf(storePath: os.Path): Try[StoreId] =
    CopyLayout.nameOf(storePath).map(_._1).toRight(
      IllegalArgumentException(s"$storePath names no store copy")).toTry

  /** A waiting load's callback and a new load's, composed only when both are real callbacks, so retries without one
   *  never nest */
  private def composeServed(waiting: Option[() => Unit], current: Option[() => Unit]): Option[() => Unit] =
    (waiting, current) match
      case (Some(w), Some(c)) => Some(() => { w(); c() })
      case _ => waiting.orElse(current)

  /** Whether the store's backend is being opened or created (startOpen), or, opened, awaits its install (handleLoad):
   *  its copy is Opening on a device of the host */
  private[host] def isOpening(storeId: StoreId): Boolean = ctx.copyRegistry.isOpening(storeId)

  /** Queues the LoadStore of an opened backend. Its copy stays Opening, so the store silent, until that LoadStore
   *  installs it or gives up (handleLoad). */
  private[host] def serveOpened(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                                  backend: Backend): Future[Unit] =
    logger.info(s"Loading store ${storeCfg.storeId}: $storePath")
    ctx.loadStore(sds.storageDeviceId, backend, storeCfg.effectiveMaxSliceSize.map(_.toLong), Some(storeCfg.ida),
      storeCfg.admissionTier)

  /** Opens (or, with `create`, creates) a store's backend through the BackendOpener, on the device's management
   *  lane (`sds.io`). The outcome is posted back as BackendOpened, so the control thread never waits on I/O or fsync.
   *  Meanwhile its copy is Opening, so a shutdown waits for it, no second open starts, and the store is silent rather
   *  than answered as not here.
   *
   *  The copy's Opening is a claim (HostCopies.opening), and a refused one opens nothing (I1). It is refused while the
   *  store has TransferOut work on the device (R12: the transfer's close and extraction need the store closed until
   *  its copy is moved to outbound/; asked first, and logged at debug), and for any move the table refuses (a
   *  copy Closing, Retiring or not Live; recorded as a violation). A refused open records a load retry (R23), with
   *  the waiting load's onServed and the usual backoff, so the device scans offer it again once whatever held the
   *  copy has ended; a refused create does not, as its entry, still Initializing, has the next check create again. */
  private[host] def startOpen(sds: LocalStorageDeviceState, storeCfg: StoreConfig, storePath: os.Path,
                              create: Boolean, created: Option[Promise[Unit]],
                              onServed: Option[() => Unit] = None): Unit =
    val storeId = storeCfg.storeId
    // A load that failed with its data intact handed its callback to this open, which now runs it, once, if it
    // serves the store. The open takes the retry: its copy goes AwaitingRetry -> Opening
    val previous = ctx.copyRegistry.retryOn(sds, storeId)
    val servedCallback = composeServed(previous.flatMap(_.onServed), onServed)
    def refused(why: String): Unit =
      created.foreach(_.tryFailure(new IllegalStateException(s"the open of store $storeId was refused: $why")))
      if !create then
        val waitScans = nextWait(previous)
        awaitRetry("refused open", storeId, LoadRetry(sds, storePath, waitScans, waitScans, servedCallback))
    val transferOut = ctx.copyRegistry.transferOutWork(sds, storeId)
    if ctx.shutdown.isStoppingServing(sds.storageDeviceId) then
      // A relinquish, Failed retirement or CRL failure is taking the device out of service, and unloads it whatever
      // is open once the closes it waits for have ended: a store opened now would stay served (after a relinquish,
      // beside the adopter's copy). Nothing is opened, and no retry recorded: the device is going
      logger.info(s"Not opening store $storeId on device ${sds.storageDeviceId}: the device is being taken out of " +
                  "service")
      created.foreach(_.tryFailure(new IllegalStateException(
        s"the open of store $storeId was refused: its device is being taken out of service")))
    else if transferOut.isDefined then
      logger.debug(s"Not opening store $storeId on device ${sds.storageDeviceId}: it has ${transferOut.get} there, " +
                   "which keeps it closed until its copy is moved to outbound/. Retrying at the device scans")
      refused(s"it has ${transferOut.get}")
    else
      lastOpenId += 1
      if !ctx.copyRegistry.opening(sds, storeId, lastOpenId, create, storePath) then
        logger.error(s"Not opening store $storeId on device ${sds.storageDeviceId}: the copy registry refused its " +
                     "Opening. Retrying at the device scans")
        refused("the copy registry refused its Opening")
      else
        val fopen =
          try openBackend(storeCfg, storePath, create, sds.devicePath / StoreCreation.CreatingDirectory, sds.io)
          catch case NonFatal(t) => Future.failed(t)
        // The open runs on the device's management lane and its result comes back as a posted BackendOpened, so
        // the control thread never waits on I/O or fsync. ExecutionContext.parasitic only posts, so nothing else
        // runs off the control thread.
        fopen.onComplete(result =>
          ctx.post(BackendOpened(sds, storeCfg, storePath, create, previous, result, created, servedCallback))
        )(using ExecutionContext.parasitic)

  /** The outcome of startOpen, on the control thread:
   *  - opened: the store is served (LoadStore, which retires a backend that failed-stop meanwhile), unless its
   *    device was unloaded meanwhile or another backend serves it, when it is closed unserved;
   *  - BackendStateLost: the store's acknowledged state is gone. It is moved to lost/ and left offline, as
   *    crlSaveFileLost leaves a store, and the reconcile then flips its entry to Rebuilding;
   *  - a failed create that left nothing at the path: left offline; its entry is still Initializing, so the next
   *    device check creates it again;
   *  - any other failure (out of space, Backend.isOutOfSpace, or an I/O, lock or resource error): nothing says the
   *    data is gone, so the store is left offline and unloaded, its copy AwaitingRetry, and ScanDevices retries it
   *    with backoff. So is one whose LoadStore could not be queued (serveOpened threw).
   *  ShutdownStore completions deferred meanwhile are then put again, after the LoadStore. The load's onServed
   *  callback, if any, waits for that LoadStore (awaitServed).
   *  Every outcome is a posted BackendOpened, handled after the control task that began the open has ended. */
  def handleBackendOpened(event: BackendOpened): Unit =
    val BackendOpened(sds, storeCfg, storePath, create, previous, result, created, onServed) = event
    val storeId = storeCfg.storeId
    // Set when the store is served: it stays opening until its LoadStore is handled, which puts its deferred closes
    // again
    var installing = false
    val registered = ctx.devices.isLoaded(sds)
    // The open's copy, unless served below: Opening -> Idle, or AwaitingRetry where a retry was recorded
    def openEnded(): Unit = ctx.copyRegistry.openEnded("open ended", sds, storeId)
    // A create renames its finished copy into the store's path before the open (StoreCreation), so the record its open
    // registered stays unless the create failed leaving nothing there. A stat that fails says nothing, and the disk
    // check skips it
    if create && result.isFailure && FileStat.existence(storePath).contains(false) then
      ctx.copyRegistry.createFailed(sds, storeId)
    result match
      case Success(backend) if !registered || router.get(storeId).isDefined =>
        logger.warn(s"Not serving store $storeId opened at $storePath: its device was unloaded or another " +
                    s"backend serves it meanwhile. Closing it")
        val failed = backend.hasFailed
        // Its device's unload waits for the close (StoreRebuilds.closeStarted), so the move below is made on it
        val tracked = Option.when(failed && registered)(ctx.rebuilds.closeStarted(sds.storageDeviceId))
        val closed =
          try backend.close()
          catch case NonFatal(t) => Future.failed(t)
        created.foreach(_.tryFailure(new IllegalStateException(s"store $storeId was opened but not served")))
        // A failed backend's copy is held Closing(Unserved) while it closes, as retireUnserved holds one: Idle, it
        // would be CopyDecision's Load state once the other backend is gone, and a load in that window would open a
        // second backend on the directory, which may serve, leaving the move below to find it Opening or Served
        val held = tracked.isDefined && ctx.copyRegistry.closingUnserved(sds.storageDeviceId, storeId)
        if !held then
          openEnded()
        if failed then
          // Moved to lost/ once its backend has closed: never a move of an open backend's directory (P5). A device
          // unloaded before this open ended is left as its unload left it: the copy is where it was, and a backend
          // still broken fails its next open again. The hold ends just before the move
          closed.onComplete: _ =>
            try
              if held then
                ctx.copyRegistry.closeEnded(sds.storageDeviceId, storeId)
              if ctx.devices.isLoaded(sds) then
                ctx.rebuilds.moveToLost("failed unserved backend", sds, storeId): Unit
                ctx.reconciler.checkStorageDevice(sds.storageDeviceId)
            finally tracked.foreach(ctx.rebuilds.closeDone(sds.storageDeviceId, _))
      case Success(backend) =>
        if create then
          newStores += storeId
        // Queuing the LoadStore may throw (the loadStore seam). The store is then not served and nothing else would
        // end its open: it is closed unserved and retried at the device scans as a failed open is, and the closes
        // deferred meanwhile are put again below
        Try(serveOpened(sds, storeCfg, storePath, backend)) match
          case Success(served) =>
            installing = true
            awaitingInstall += storeId -> (backend, previous)
            created.foreach(_.completeWith(served))
            onServed.foreach(awaitServed(storeId, served, _))
          case Failure(e) =>
            logger.error(s"Store $storeId opened, but its LoadStore could not be queued. Closing it unserved and " +
                         s"retrying at the device scans. $storePath. Error: $e")
            newStores -= storeId
            awaitRetry("serveOpened threw", storeId, LoadRetry(sds, storePath, nextWait(previous), nextWait(previous),
              onServed))
            // Its retry waits for this close (closeAbandoned)
            closeAbandoned(sds.storageDeviceId, backend, replaced = false)
            created.foreach(_.tryFailure(e))
      case Failure(e: BackendStateLost) =>
        logger.error(s"Store $storeId cannot be loaded: its backend found its acknowledged state lost. " +
                     s"Rebuilding it. $storePath. Error: ${e.getMessage}")
        openEnded()
        // No backend opened, so the copy is closed: moved to lost/ now. A move that fails holds it offline for the
        // checks to retry; a crash before the move recovers the same files at the next start and lands here again
        ctx.rebuilds.moveToLost("BackendStateLost", sds, storeId): Unit
        // A device still being loaded is checked once it is registered.
        if registered then
          ctx.reconciler.checkStorageDevice(sds.storageDeviceId)
        created.foreach(_.tryFailure(e))
      case Failure(e) if create && FileStat.existence(storePath).contains(false) =>
        // The create failed and left nothing at the path (StoreCreation), so the entry is still Initializing and the
        // next device check creates it again (DeviceReconciler, StoreRebuilds.createNewStore). A create that
        // succeeded and whose open failed left a complete store, which the next check flips without loading: it
        // takes the load retries below.
        logger.warn(s"Store $storeId cannot be created on device ${sds.storageDeviceId}. Leaving it offline; the " +
                    s"next device check creates it again. Error: $e")
        created.foreach(_.tryFailure(e))
      case Failure(e) =>
        val outOfSpace = Backend.isOutOfSpace(e)
        val waitScans = nextWait(previous)
        val why = if outOfSpace then "its device is out of space" else "its backend failed to open"
        if previous.isDefined then
          logger.debug(s"Store $storeId still cannot be loaded: $why. Retrying in $waitScans device scans. " +
                       s"Error: $e")
        else
          logger.warn(s"Store $storeId cannot be loaded: $why. Leaving it offline and retrying, with backoff" +
                      s"${if outOfSpace then ", until space returns" else ""}. $storePath. Error: $e")
        awaitRetry("failed open", storeId, LoadRetry(sds, storePath, waitScans, waitScans, onServed))
        created.foreach(_.tryFailure(e))
    if !installing then
      // Whatever ended it, the open is over: a retry whose claim was refused leaves no copy Opening for good
      openEnded()
      putDeferredCloses(storeId)

  /** The next wait, in device scans, after `previous`: doubled, up to the cap, or 1 for a first failure */
  private def nextWait(previous: Option[LoadRetry]): Int =
    previous.map(w => math.min(w.waitScans * 2, loadRetryCapScans)).getOrElse(1)

  /** Puts again the ShutdownStores that waited for the open of `storeId`, oldest first: they run after this task */
  private def putDeferredCloses(storeId: StoreId): Unit =
    val deferred = deferredCloses.getOrElse(storeId, Nil)
    deferredCloses -= storeId
    deferred.reverse.foreach(ctx.post)

  /** Runs a load's onServed `callback` on the control thread once its LoadStore has served the store (`served`
   *  succeeds). A LoadStore that leaves the store offline with its data intact leaves its copy AwaitingRetry
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
          ctx.copyRegistry.retryOf(storeId).foreach: wait =>
            awaitRetry("awaitServed", storeId, wait.copy(onServed = composeServed(wait.onServed, Some(callback))))

  /** Called once per device scan. Retries the load of every copy AwaitingRetry whose backoff has run out, whose
   *  device is still loaded here and not shutting down, and whose store is still unserved and not opening. Others are
   *  dropped, with their onServed callback unrun: whatever replaced or removed them has taken over. A retry's open
   *  takes the callback over from the copy (startOpen).
   *
   *  tryLoadStore reads no tree, so a retry is also held to what the device's latest reconcile read
   *  (`reconciledEntries`) and to the host's own work on the store (cross-document-review.md 2.2, load retries). It is dropped
   *  once that reconcile finds no entry for the store on the device: the Deleted Stores pass retires the copy. It
   *  waits, its backoff spent, while the entry is neither Active nor Offline (TransferringOut, Rebuilding: the
   *  transfer or the rebuild owns the copy; before the device's first reconcile no entry is known, and nothing the
   *  tree drives has started on the store but a transfer out, whose TransferOut work refuses the open: startOpen),
   *  while the Missing Stores pass has declared the store (declaredMissing) or while a rebuild of it runs here. Held
   *  rather than dropped, so a store whose transfer is aborted before its copy is moved to outbound/, or whose entry returns
   *  to Active, is still served. */
  def retryFailedLoads(): Unit =
    ctx.copyRegistry.retries.foreach: (storeId, wait) =>
      val deviceId = wait.sds.storageDeviceId
      val entry = wait.sds.reconciledEntries.map(_.get(storeId))
      if ! ctx.devices.isLoaded(wait.sds) || router.get(storeId).isDefined || isOpening(storeId) then
        dropRetry(storeId, wait)
      else if entry.contains(None) then
        logger.info(s"Store $storeId awaiting a load retry has no entry on device $deviceId any more. Dropping the " +
                    s"retry")
        dropRetry(storeId, wait)
      else if FileStat.existence(wait.storePath / StoreConfig.configFilename).contains(false) then
        // Nothing left to load. An Active entry naming it is rebuilt by the reconcile's Missing Stores pass. A failed
        // stat is retried as below: tryLoadStore takes it for an unreadable config
        logger.error(s"Store $storeId awaiting a load retry no longer has a ${StoreConfig.configFilename} at " +
                     s"${wait.storePath}. Dropping the retry")
        dropRetry(storeId, wait)
      else if wait.scansLeft > 1 then
        awaitRetry("retryFailedLoads", storeId, wait.copy(scansLeft = wait.scansLeft - 1))
      else if ! entry.forall(_.exists(e => e.status == StorageDeviceState.StoreStatus.Active ||
                                           e.status == StorageDeviceState.StoreStatus.Offline)) ||
              wait.sds.declaredMissing.contains(storeId) ||
              ctx.copyRegistry.rebuilding(HostCopies.RebuildHolds).contains(storeId) then
        logger.debug(s"Store $storeId awaiting a load retry is not loaded yet: its entry on device $deviceId is " +
                     s"${entry.flatten.map(_.status)}, or the host is rebuilding or has declared it")
      else if ! ctx.shutdown.isShuttingDown(deviceId) && ! ctx.shutdown.isStoppingServing(deviceId) then
        ctx.tryLoadStore(wait.sds, wait.storePath, None)

  /** Opens the backend `storeCfg` names at `storePath`, wherever the BackendOpener runs it; with `create`, first
   *  creates it there atomically (BackendOpener.create, scratch under `creatingRoot`). A backend's fail-stop is
   *  posted as BackendFailed. */
  private def openBackend(storeCfg: StoreConfig, storePath: os.Path, create: Boolean,
                          creatingRoot: os.Path, io: DeviceIo): Future[Backend] =
    val ref = new AtomicReference[Backend]()
    // Called once, on a backend thread: it only posts BackendFailed to the control thread, so it never blocks there.
    val onFatal: Throwable => Unit = err => ctx.post(BackendFailed(storeCfg.storeId, ref, err))
    val created =
      if create then ctx.backends.create(storeCfg, storePath, creatingRoot, io)
      else Future.unit
    // parasitic: the open is submitted from the create's completion on the lane, and queues behind it there
    created.flatMap(_ => ctx.backends.open(storeCfg, storePath, onFatal, io))(using ExecutionContext.parasitic).map { backend =>
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
    // The open that queued this LoadStore ends here: this task installs the store or gives up. Its deferred
    // ShutdownStores run after it, so a store installed is closed like any served one. A LoadStore queued by no
    // startOpen (Host.loadStore, a test's reload) leaves an open in progress alone
    ctx.copyRegistry.loadHandled(storageDeviceId, backend.storeId, backend.path)
    val fromOpen = awaitingInstall.get(backend.storeId).exists(_._1 eq backend)
    // The retry the open took, or, for a LoadStore no open queued, the one its copy awaits: a failed build's backoff
    // doubles it
    val previousRetry =
      if fromOpen then awaitingInstall.get(backend.storeId).flatMap(_._2)
      else ctx.devices.devices.get(storageDeviceId).flatMap(ctx.copyRegistry.retryOn(_, backend.storeId))
    if fromOpen then
      awaitingInstall -= backend.storeId
      putDeferredCloses(backend.storeId)
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
    // drop everything saved since. So the load is held back and retried instead
    val hasSaveFile = if failed then Success(false) else Try(FileStat.exists(backend.crlSaveFile))
    // The store's identity, read before anything is built. Never created here: every store directory gets one when it
    // is made (StoreCreation, RebuildingStore.finish). One that is missing is lost, which makes the store a new
    // incarnation, and one written fresh over a store keeping its data and CRL state would skip the commits it owes
    // (a recovered commit vote finding its UnackedCommits entry under the old incarnation)
    val incarnation =
      if failed then Failure(new IllegalStateException("not read")) else
        Try(StoreIncarnationFile.load(backend.path, backend.storeId))
    // A rebuilt store's acceptor start and inherited-vote gate. None only when each file is known absent: one whose
    // stat fails would load the store as a full acceptor, joining its old incarnation's transactions and voting over
    // their locks, so the load is held back and retried as for the save and incarnation files
    val rebuiltGates: Try[(Option[HLCTimestamp], Option[Long])] =
      if failed then Success((None, None)) else
        Try((RebuildingStore.readAcceptorSince(backend.path),
             RebuildingStore.readInheritedVotes(backend.path, ctx.transactionHorizon)))
    // The save file's content, or the error of a read that says nothing of it (CrashRecoveryLog.isUnreadable: EIO,
    // EMFILE, out of memory), which holds the load back as a failed stat does rather than declare the state lost
    val savedRead: Either[Throwable, Either[Exception, Option[StoreRecoveryState]]] =
      if failed || !hasSaveFile.getOrElse(false) then
        Right(Right(None))
      else
        try Right(CrashRecoveryLog.loadValidStoreState(backend.crlSaveFile, backend.storeId).map(Some(_)))
        catch case t: Throwable => Left(t)
    val savedState: Either[Exception, Option[StoreRecoveryState]] =
      if failed then
        // Failed-stop between its open and this event. Its BackendFailed found nothing to
        // unload, or will, so it is retired here.
        Left(new java.io.IOException(s"the backend of store ${backend.storeId} failed before it was served"))
      else
        savedRead.getOrElse(Right(None))

    // Why the saved state can never fit its device's CRL, if it cannot
    val neverFitsReason = (savedState, deviceCrl) match
      case (Right(Some(state)), Some(crl)) => neverFits(state, crl, admissionOf(storageDeviceId))
      case _ => None

    (savedState, deviceCrl) match
      case (Left(err), _) if failed =>
        logger.error(s"Store ${backend.storeId} cannot be loaded: its backend failed. Rebuilding it. Error: $err")
        ctx.rebuilds.retireUnserved(storageDeviceId, backend)
        p.failure(err)

      case _ if hasSaveFile.isFailure || savedRead.isLeft || rebuiltGates.isFailure ||
          incarnation.failed.toOption.exists(e => !e.isInstanceOf[StoreIncarnationFile.IncarnationLost]) =>
        // Not known to be gone, only unread: left offline, its files untouched, and retried with backoff as an
        // unreadable config is. A reload over a served store leaves that store serving
        val err = hasSaveFile.failed.toOption.orElse(savedRead.left.toOption).orElse(rebuiltGates.failed.toOption)
          .orElse(incarnation.failed.toOption).get
        val replaced = router.entry(backend.storeId).isDefined
        if replaced then
          logger.warn(s"Not reloading store ${backend.storeId}: its save, incarnation, acceptor-since or " +
                      s"inherited-votes file could not be read. " +
                      s"The loaded copy keeps serving. Error: $err")
        // The retry is recorded first, so the unserved backend's close parks it (abandonLoad)
        if !replaced then
          ctx.devices.devices.get(storageDeviceId).foreach(awaitUnreadRetry(_, backend.path, None, err,
            ofLoad = fromOpen))
        abandonLoad(storageDeviceId, backend, replaced, err, p)

      case (Left(err), _) =>
        ctx.rebuilds.crlSaveFileLost(storageDeviceId, backend, err)
        p.failure(err)

      case _ if incarnation.isFailure =>
        // Its incarnation file is gone: the store's identity is lost with it, so it is rebuilt as a new incarnation
        val err = incarnation.failed.get
        logger.error(s"Store ${backend.storeId} cannot be loaded: its incarnation file is missing. Rebuilding it. " +
                     s"Error: $err")
        ctx.rebuilds.retireUnserved(storageDeviceId, backend)
        p.failure(err)

      case (Right(Some(_)), Some(_)) if neverFitsReason.nonEmpty =>
        // No CRL of this device can ever take it (crash-recovery-log 6.1), as for a store moved from a device with
        // a larger CRL: retrying would keep the store offline for good, so it is rebuilt as one whose file is unusable
        val err = new IllegalStateException(s"the CRL state of store ${backend.storeId} can never fit its device's " +
          s"CRL: ${neverFitsReason.get}")
        ctx.rebuilds.crlSaveFileLost(storageDeviceId, backend, err)
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
              awaitRetry("no executor", storeId, LoadRetry(sds, backend.path, 1, 1))
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
                rebuiltGates.get._1,
                inheritedVotesAfter = rebuiltGates.get._2,
                heldVoteQuery = ctx.heldVoteQuery,
                incarnationOverride = Some(incarnation.get),
                poolIDA = poolIDA,
                admissionTier = admissionTier,
                onThisHost = id => router.get(id).isDefined,
                deviceIo = ctx.devices.devices.get(storageDeviceId).map(_.io),
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
                    awaitBuildRetry(sds, backend.path, storeId, previousRetry, err)
                abandonLoad(storageDeviceId, backend, replacing.isDefined, err, p)
              // A crl_save.log state is charged, checked, before any of it is saved (crash-recovery-log 6.1).
              // Retained for the store, the charge is swapped for its recovery's own count (Frontend.recoverState),
              // as what the device's open or its last close retained is: the store's records are never uncounted.
              // Charged here, after the build, so no later failure of the load leaves it charged: a failed
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
                    awaitRetry("no CRL room", storeId, LoadRetry(sds, backend.path, 1, 1))
                abandonLoad(storageDeviceId, backend, replacing.isDefined, err, p)
              case Success(entry) =>
                // A LoadStore no open queued (Host.loadStore, tests) takes the retry its copy awaits, as an open does
                if !fromOpen then
                  ctx.devices.devices.get(storageDeviceId).foreach(
                    ctx.copyRegistry.retryDropped("LoadStore", _, storeId))
                router.install(entry)
                awaitCrlLoad(entry.store, trs, p, previousRetry)
    // The open that queued this LoadStore ended unserved: its copy is Opening no more. Installed, it is Served
    if fromOpen && router.entry(backend.storeId).forall(_.store.backend ne backend) then
      ctx.devices.devices.get(storageDeviceId).foreach(ctx.copyRegistry.openEnded("load ended", _, backend.storeId))

  /** Records a store whose Store failed to build as AwaitingRetry: retried next scan, then 2, 4 ... scans later per
   *  failure in a row (`previous`, the retry its open took), capped at LoadRetryCap, and logged at error once per cap
   *  interval. That retry's onServed is kept for the load that serves it. */
  private def awaitBuildRetry(sds: LocalStorageDeviceState, storePath: os.Path, storeId: StoreId,
                              previous: Option[LoadRetry], err: Throwable): Unit =
    val waitScans = nextWait(previous)
    if previous.isEmpty || previous.exists(_.waitScans == loadRetryCapScans) then
      logger.error(s"Store $storeId cannot be loaded: its Store failed to build. Leaving it offline and retrying in " +
                   s"$waitScans device scans. $storePath. Error: $err")
    else
      logger.debug(s"Store $storeId still fails to build. Retrying in $waitScans device scans. Error: $err")
    val onServed = previous.flatMap(_.onServed)
    awaitRetry("failed build", storeId, LoadRetry(sds, storePath, waitScans, waitScans, onServed))

  /** The admission of a device's CRL, Unlimited for a device not loaded here (the test seam's CRL) */
  private def admissionOf(storageDeviceId: StorageDeviceId): CrlAdmission =
    ctx.devices.devices.get(storageDeviceId).map(_.admission).getOrElse(CrlAdmission.Unlimited)

  /** Why `state` can never be saved in `crl` counting with `admission`, if it cannot: its bytes exceed the most
   *  admission limit the CRL can reach under any device config (maxAdmissionLimitBytes) plus the per-transaction
   *  bound, the most a load is charged against (CrlAdmission.tryChargeLoaded) once the CRL has grown, or one of its
   *  transactions exceeds that bound (crash-recovery-log 10.20). Neither depends on the device config, so no config
   *  edit can declare a state lost. A state within that limit but above the one now is refused by tryChargeLoaded,
   *  which asks the CRL to grow and leaves the store waiting offline (crash-recovery-log 6.1) */
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
   *  earlier store is still served in its place (`replaced`), the store is left offline: its open's copy ends Opening
   *  (handleLoad). Its Live copy is held Closing(Unserved) until the close ends (HostCopies.closingUnserved), the
   *  retry its caller recorded parked meanwhile: the backend still holds the directory (a RocksDB lock, an FSIF
   *  backend's files), and a retry's open racing its close would open a second backend over it, or fail and double
   *  its backoff. Callers record the retry before calling this */
  private def abandonLoad(storageDeviceId: StorageDeviceId, backend: Backend, replaced: Boolean, err: Throwable,
                          p: Promise[Unit]): Unit =
    closeAbandoned(storageDeviceId, backend, replaced)
    p.tryFailure(err)

  /** abandonLoad's close of the unserved `backend`, its copy held while it runs unless `replaced` */
  private def closeAbandoned(storageDeviceId: StorageDeviceId, backend: Backend, replaced: Boolean): Unit =
    val storeId = backend.storeId
    val held = !replaced && ctx.copyRegistry.closingUnserved(storageDeviceId, storeId, "abandonLoad",
      keepRetry = true)
    val closed =
      try backend.close()
      catch case NonFatal(t) => Future.failed(t)
    closed.onComplete: result =>
      result.failed.foreach(t => logger.warn(s"Failed to close the unserved backend of store $storeId: $t"))
      if held then
        ctx.copyRegistry.closeEnded(storageDeviceId, storeId)

  /** The rest of a LoadStore once its store is installed: with no saved state, recovers it from the device CRL and
   *  completes `p` at once; otherwise completes `p` once the CRL holds `trs` and the store has recovered from it. Its
   *  transactions are recreated, and its transaction messages handled, only once it has recovered. `previous`: the
   *  load retry the load's open took, which a failed CRL load's retry backs off from (handleUnloadFailed) */
  private def awaitCrlLoad(store: Store, trs: Option[StoreRecoveryState], p: Promise[Unit],
                           previous: Option[LoadRetry]): Unit =
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
            ctx.post(UnloadFailedStore(storeId, store, previous))
            p.failure(err)

  /** Unloads a store whose CRL state failed to load, if it is still the one installed. Its close runs on its
   *  executor (closeUnloaded). */
  def handleUnloadFailed(e: UnloadFailedStore): Unit =
    val UnloadFailedStore(storeId, store, previous) = e
    router.entry(storeId).filter(_.store eq store).foreach: entry =>
      // Its copy stays where it is, to load again: no fail-stop. Closed with ClosePurpose.Reload it ends
      // AwaitingRetry, not Idle: retried at the device scans, 1, 2, 4 ... scans apart per failure in a row (from the
      // retry its open took), capped at LoadRetryCap, never at each device check by CopyDecision's Load
      ctx.copyRegistry.closingFor(storeId, ClosePurpose.Reload)
      router.remove(storeId)
      ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
        val waitScans = nextWait(previous)
        awaitRetry("CRL load failed", storeId, LoadRetry(sds, store.backend.path, waitScans, waitScans))
      // Drops whatever part of the saved state did reach the CRL. The save file is not
      // rewritten from it: the file on disk is still the whole state.
      // The save file on disk still holds the whole state, so the entries may go for good
      closeUnloaded(entry, dropDurably = true, ClosePurpose.Reload)

  /** Unloads a store whose crl_save.log could not be removed durably after its load (StoreRouter.crlLoaded), if it
   *  is still the one installed. It has not recovered, so it served nothing: its device-CRL entries, the whole of
   *  its state should the unlink have succeeded, are kept (closeUnloaded without dropDurably). It is left offline
   *  and unloaded, as abandonLoad leaves a store, and retried at every device scan; the retry's load removes the
   *  file durably, forcing the directory even if it is gone, before the store recovers (awaitCrlLoad). */
  def handleSaveFileRemovalFailed(e: Host.SaveFileRemovalFailed): Unit =
    val Host.SaveFileRemovalFailed(storeId, store) = e
    router.entry(storeId).filter(_.store eq store).foreach: entry =>
      ctx.copyRegistry.closingFor(storeId, ClosePurpose.Reload)
      router.remove(storeId)
      ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
        awaitRetry("save file removal failed", storeId, LoadRetry(sds, store.backend.path, 1, 1))
      closeUnloaded(entry, dropDurably = false, ClosePurpose.Reload)

  /** Closes the store of `entry`, already removed from the router, without saving its CRL state: clears its object
   *  cache on its executor, then closes it there, and releases its executor slot once the close completes. Nothing
   *  is awaited. With `dropDurably` its entries are deleted from the CRL (CrashRecoveryLog.dropStore), which only a
   *  caller whose state is recoverable elsewhere (a save file still on disk) may ask for; otherwise they stay live in
   *  the device CRL, and a restart recovers them. A close for `ClosePurpose.FailStop` moves the copy to lost/ when it
   *  ends (StoreRebuilds.failStopClosed, P5); any other leaves it where it is */
  private[host] def closeUnloaded(entry: StoreRouter.Entry, dropDurably: Boolean, purpose: ClosePurpose): Unit =
    val store = entry.store
    val closed = startClose(entry):
      store.objectCache.clear()
      if dropDurably then
        store.crl.dropStore(store.storeId).map(_ => true)(using ExecutionContext.parasitic)
      else
        Future.successful(false)
    .transformWith: dropped =>
      onExecutor(entry)(store.close()).transform(_ => Success(dropped == Success(true)))
    // Its device's unload waits for the close's end (StoreRebuilds.closeStarted). Taken last, once nothing before the
    // callback can throw, and done whatever the callback meets, so no close is left counted running for good
    val tracked = ctx.rebuilds.closeStarted(store.storageDeviceId)
    closed.onComplete: dropped =>
      try
        // Released once the close has retained them: dropped entries are no longer in the CRL
        if dropped == Success(true) then
          store.frontend.admission.releaseRetained(store.storeId)
        ctx.threading.release(store.storeId)
        ctx.copyRegistry.closeEnded(store.storageDeviceId, store.storeId)
        // The reconcile's passes leave a closing copy to its close (HostCopies.offlineCopies): its end checks the
        // device again, so a fail-stopped or unloaded copy is retired or rebuilt without waiting for the periodic check
        if purpose == ClosePurpose.FailStop then
          ctx.rebuilds.failStopClosed(store.storageDeviceId, store.storeId, store.backend.path)
        else if ctx.devices.devices.contains(store.storageDeviceId) then
          ctx.reconciler.checkStorageDevice(store.storageDeviceId)
      finally ctx.rebuilds.closeDone(store.storageDeviceId, tracked)

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
      // Only a Live copy is offered; a store with no Live copy on the device has nothing to load
      ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(e.toreId)).filter(_.phase == Phase.Live).foreach: _ =>
        ctx.copyRegistry.pathOf(sds, e.toreId).foreach(ctx.tryLoadStore(sds, _, None))

  /** Closes a store (spec section 5): removes it from the router, which marks it offline so new traffic gets the
   *  offline handling, then runs the close on its executor, behind any tasks already queued for it: the object cache
   *  cleared and, with `extractCrl`, `crl.readStoreState` reading the store's state, which stays live in the device
   *  CRL; then that state saved to its crl_save.log (extractCrl only) and `store.close()`. Without `extractCrl` no
   *  save file is written. Back on the control thread, its executor slot is released, whatever the outcome, and the
   *  fail-stop check decides the completion; a failed step fails it. Extracted entries are deleted from the device
   *  CRL (memory and files) only once the save file is durable, so a crash or a failed save before that leaves the
   *  device CRL holding them. A store whose backend failed-stop
   *  is moved to lost/ once closed (failedOnClose); its entries are dropped when the rebuild discards it
   *  (StoreRebuilds.discardLostStore), after that move is durable. */
  def handleClose(e: ShutdownStore): Unit =
    val ShutdownStore(storeId, extractCrl, completion) = e
    router.entry(storeId) match
      case None if isOpening(storeId) =>
        // Closed once its open ends and it is served (handleBackendOpened or handleLoad puts this event again)
        deferredCloses += storeId -> (e :: deferredCloses.getOrElse(storeId, Nil))
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
        // A close for a transfer out extracts; one asked for no other purpose (Host.closeStore, tests) is a shutdown's
        if extractCrl then
          ctx.copyRegistry.closingFor(storeId, ClosePurpose.TransferOut)
        else if !ctx.copyRegistry.closePurposeOf(storeId).isDefined then
          ctx.copyRegistry.closingFor(storeId, ClosePurpose.Shutdown)
        router.remove(storeId)
        val closed = startClose(entry):
          store.objectCache.clear()
          if extractCrl then
            store.crl.readStoreState(storeId).map(Some(_))
          else
            Future.successful(None)
        .transformWith: extractedTry =>
          // The save is a SaveFile job on the store's device lane, never on its executor (a hung device then stalls
          // only its own stores); the close follows it on the executor, whatever the CRL step or the save did, so the
          // store's backend and CRL admissions are released; a failed step then fails the close
          val saveFile = store.backend.crlSaveFile
          val savedFuture: Future[Unit] = extractedTry match
            case Failure(err) => Future.failed(err)
            case Success(None) => Future.unit
            case Success(Some(trs)) =>
              store.deviceIo match
                case Some(io) => io.submit(JobKind.SaveFile, Some(storeId))(StoreDisk.saveState(storeId, saveFile, trs)).future
                case None => Future.fromTry(Try(StoreDisk.saveState(storeId, saveFile, trs)))
          savedFuture.transformWith { saved =>
            onExecutor(entry):
              store.close().flatMap(_ => Future.fromTry(saved))(using ExecutionContext.parasitic)
          }(using ExecutionContext.parasitic)
          .flatMap: _ =>
            // The store is closed: its copy's close ends here, before the extraction's deletion, so an open
            // that installs during the deletion finds the copy Idle
            ctx.copyRegistry.closeEnded(store.storageDeviceId, storeId)
            val extracted = extractedTry.get
            // Deleted from the device CRL files only once the save file holding them is durable, so a restart does
            // not recover them on this device too. A crash between the two leaves them in both: harmless, as loading
            // the save file re-saves them under the same ids
            extracted.fold(Future.unit): state =>
              store.crl.deleteTransactions(storeId, state.entryIds).map: _ =>
                // Every entry of the closed store has left the device CRL
                store.frontend.admission.releaseRetained(storeId)
        // Its device's unload waits for the close's end, failedOnClose's move included (StoreRebuilds.closeStarted).
        // Taken last, once nothing before the callback can throw, and done whatever the callback meets
        val tracked = ctx.rebuilds.closeStarted(store.storageDeviceId)
        closed.onComplete: closeResult =>
          try
            // The close has ended: the backend and directory belong to the control side again. A throw here (a bug)
            // still completes the close's caller, failed, so nothing waits on it for good
            val ended = Try:
              ctx.threading.release(storeId)
              ctx.copyRegistry.closeEnded(store.storageDeviceId, storeId)
            ended.failed.foreach(t => logger.error(s"Failed to end the close of store $storeId, a bug: $t"))
            ended.flatMap(_ => closeResult) match
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
          finally ctx.rebuilds.closeDone(store.storageDeviceId, tracked)

object StoreLoader:
  /** In a store directory: this host's wall clock, in milliseconds, when the store arrived from another host, by a
   *  transfer in (StoreTransfers.bringArrivedLive) or an adoption of its device (DeviceDiscovery). The store
   *  may have joined, and forgotten, a transaction there started up to MaxStartLead ahead of that host's clock, which
   *  may lead this one's: a load less than RebuildingStore.AcceptorSinceMargin after the arrival raises its forget floor
   *  by that margin (Frontend.arrivedAt). Durable and written before the store can load here, so a crash between the
   *  arrival and the load, or a restart soon after it, keeps the margin. Kept for good: once the margin has passed it
   *  changes nothing, and the next arrival rewrites it. A network transfer may carry the source's copy, which the
   *  arrival here then replaces before the store loads */
  val ArrivedAtFile = "arrived-at"

  def writeArrivedAt(storePath: os.Path): Unit =
    DurableFiles.write(storePath / ArrivedAtFile, System.currentTimeMillis().toString)

  /** The store's last arrival from another host, if any. A file that cannot be stat'd or read counts as an arrival
   *  now: later than the truth, which only refuses more transactions. Only a known absence (FileStat) is none */
  def readArrivedAt(storePath: os.Path): Option[Long] =
    val file = storePath / ArrivedAtFile
    if FileStat.existence(file).contains(false) then
      None
    else
      Some(Try(os.read(file).trim.toLong).getOrElse(System.currentTimeMillis()))
