package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.AdoptOutcome
import org.aspen_ddp.aspen.server.{DeviceMarkers, Host, StorageDeviceConfig, StorageDeviceManager}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** How one Offline store of a Shutdown device was left, as adoptionClass reads it from its directory. */
private[aspen] enum AdoptionClass:
  /** Everything it needs is in its directory: adopted Active. */
  case Clean
  /** Closed, but its CRL save file is missing or unusable, so its transaction state is gone:
   *  adopted Rebuilding. */
  case CrlLost
  /** Its close did not finish: the device is refused. */
  case Unclean

/** Finds the storage devices under `storage-devices/` and decides whether to load, restart, or adopt each.
 *
 *  Everything here runs on the host's event thread under the host's lock. */
class DeviceDiscovery(ctx: DeviceControl) extends Logging:
  import ctx.given

  private def devices = ctx.devices.devices
  private def storageDevicesDir: os.Path = ctx.rootDir / StorageDeviceManager.StorageDevicesDirName

  // Directories under storage-devices/ that hold a loaded device. checkForNewDevices skips these
  // without reading anything in them, which keeps the frequent scan down to one directory
  // listing. An entry is dropped when its path stops being listed, so a disk pulled and
  // re-inserted on this host is checked again in full (and the duplicate guard in tryLoadDevice
  // finds it already loaded). Unloading a device must also remove its entry.
  private var loadedDirs: Map[os.Path, StorageDeviceId] = Map()

  // Directories whose problem has already been logged. The scan rechecks an unloaded directory
  // every deviceScanPeriod, so without this every unowned or broken device would produce a
  // warning every few seconds forever. In memory only: a restart logs each problem once more. A
  // path leaves the set when its device loads, so a later problem with it is reported again.
  private var reportedDirs: Set[os.Path] = Set()

  // Device directories carrying shutdown.state whose StorageDeviceState lookup, or the adoption
  // that lookup led to, is still in flight. One at a time per path: the scan reaches such a
  // directory every deviceScanPeriod, and without this a slow read would pile up a lookup per
  // scan, and a slow adoption a second transaction racing the first. A path leaves the set once
  // its outcome has been acted on, so the next scan decides afresh.
  private var pendingResolutions: Set[os.Path] = Set()

  private[server] def loadedDeviceDirs: Map[os.Path, StorageDeviceId] = loadedDirs
  private[server] def reportedDeviceDirs: Set[os.Path] = reportedDirs
  private[server] def pendingMarkerResolutions: Set[os.Path] = pendingResolutions

  /** Forgets the directory of a device that has been unloaded, so the next scan reads it again. */
  private[host] def forgetDevice(storageDeviceId: StorageDeviceId): Unit =
    loadedDirs = loadedDirs.filter((_, id) => id != storageDeviceId)

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
  def checkForNewDevices(): List[StorageDeviceId] =
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
          reportedDirs -= storageDevicesDir
          // A loaded device whose directory is gone is forgotten here, so it is read in full
          // again if it comes back. Its storageDevices entry is untouched: its stores are open.
          val listed = paths.toSet
          loadedDirs = loadedDirs.filter((path, _) => listed.contains(path))
          paths.filterNot(loadedDirs.contains).flatMap(tryLoadDevice).toList

  /** Logs `msg` unless a problem with `path` has already been logged by this process. */
  def warnOnce(path: os.Path, msg: => String): Unit =
    if ! reportedDirs.contains(path) then
      reportedDirs += path
      ctx.warn(path, msg)

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
    if ! pendingResolutions.contains(storageDevicePath) then
      pendingResolutions += storageDevicePath
      // A throw before the Future exists would register no callback, and the path would stay
      // pending -- and the device unresolved -- for the life of the process.
      val lookup =
        try ctx.metadata.lookupStorageDeviceState(sdCfg.storageDeviceId)
        catch case t: Throwable => Future.failed(t)
      lookup.onComplete: result =>
        ctx.post(Host.ShutdownMarkerResolved(storageDevicePath, sdCfg, markerHost, result))

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
  def handleMarkerResolved(e: Host.ShutdownMarkerResolved): Unit =
    val Host.ShutdownMarkerResolved(path, sdCfg, markerHost, result) = e
    import StorageDeviceState.DeviceStatus
    pendingResolutions -= path
    val deviceId = sdCfg.storageDeviceId
    try
      // The disk was read before the lookup went out. If it has been pulled, or its marker has
      // changed, since then, this result describes a directory that is no longer there to act
      // on; the next scan starts over from what the disk says now.
      val stillMarked = os.isDir(path) && DeviceMarkers.readShutdownState(path).contains(markerHost)
      if ! stillMarked || loadedDirs.contains(path) then
        ()
      else if devices.get(deviceId).exists(_.devicePath != path) then
        warnOnce(path, s"Storage device $deviceId is already loaded from " +
                       s"${devices(deviceId).devicePath}; ignoring duplicate at $path")
      else
        result match
          case Failure(err) =>
            warnOnce(path, s"Failed to read the state of shut-down storage device $deviceId at $path. " +
                           s"Not loading it. Will retry on the next scan. Error: $err")

          case Success(state) =>
            val ownedHere = state.hostId == ctx.hostId
            def refuse(why: String): Unit =
              warnOnce(path, s"Storage device $deviceId at $path was shut down by host $markerHost, " +
                             s"and the storage-devices tree records it as ${state.status} owned by " +
                             s"host ${state.hostId}. $why")

            if state.isFailed then
              refuse("It has been declared failed. Not loading it")
            else if markerHost == ctx.hostId then
              state.status match
                case DeviceStatus.Shutdown if ownedHere => ()
                case DeviceStatus.ShuttingDown if ownedHere =>
                  logger.info(s"Storage device $deviceId at $path was being shut down when this host " +
                              s"stopped. Loading it to finish the shutdown")
                  loadOwnedDevice(path, path / StorageDeviceConfig.configFilename, sdCfg)
                    .foreach(ctx.reconciler.checkStorageDevice)
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

  /** How the store at `storePath`, whose entry is Offline, can be put back into service by an
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
   *  A missing directory, or one with neither marker, is a store whose close did not finish, and
   *  adopting it would drop the transactions its CRL state was holding: Unclean.
   *
   *  A transfer-out marker with a missing or unusable CRL file is CrlLost. The device reads
   *  Shutdown, which the shutdown commits only once every close has finished and been forced to
   *  disk, so the save file was lost afterwards -- a disk pulled before it reached the platter, or
   *  later damage -- and the host that wrote it no longer holds the state either. Refusing the
   *  device would not bring the state back, so the store is adopted as Rebuilding instead.
   *
   *  A store carrying `rebuild-required` is CrlLost whatever else its directory holds: its host
   *  marked it because its state cannot be trusted (an FSIF backend that failed while closing).
   *
   *  The CRL file is parsed, not just found: loadValidStoreState rejects a file cut short, and one
   *  whose header names another store, so a file copied from another store's directory does not
   *  pass for this one's state.
   */
  private def adoptionClass(storeId: StoreId, storePath: os.Path): AdoptionClass =
    if !os.isDir(storePath) then
      AdoptionClass.Unclean
    else if os.exists(storePath / RebuildingStore.RebuildRequiredMarkerFile) then
      // Marked by the host that closed it (a backend that failed during its close): never loaded
      // again whatever else is present, so its state is lost and it is adopted as Rebuilding.
      AdoptionClass.CrlLost
    else if os.exists(storePath / StoreTransfer.TransferredInMarkerFile) then
      AdoptionClass.Clean
    else if os.exists(storePath / StoreTransfer.TransferOutMarkerFile) then
      CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storePath), storeId) match
        case Right(_) => AdoptionClass.Clean
        case Left(_) => AdoptionClass.CrlLost
    else
      AdoptionClass.Unclean

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
    // Checked here as well as inside adoptDevice, against the looked-up state, so a device that
    // is plainly not ready costs no transaction. The check that counts is adoptDevice's own.
    val unclean = uncleanStores(path, state)

    if unclean.nonEmpty then
      warnUnclean(path, deviceId, previousHostId, unclean)
    else
      logger.info(s"Adopting storage device $deviceId at $path, shut down by host $previousHostId")
      pendingResolutions += path
      val fadopt =
        try ctx.metadata.adoptDevice(deviceId, previousHostId, path, uncleanStores,
                                     (p, s) => offlineStoresIn(p, s, AdoptionClass.CrlLost),
                                     ctx.rebuilds.markRebuildRequired)
        catch case t: Throwable => Future.failed(t)
      fadopt.onComplete: result =>
        ctx.lock.synchronized:
          pendingResolutions -= path
          result match
            case Success(AdoptOutcome.Adopted(committed)) =>
              try bringIntoService(path, sdCfg, committed, rewriteOwner = true)
              catch case t: Throwable =>
                // The adoption is committed, so the next scan reaching this directory takes the
                // "Operational, owned here" row and runs these steps again.
                logger.error(s"Adopted storage device $deviceId but failed to load it from $path. " +
                             s"Will retry on the next scan. Error: $t")
            case Success(AdoptOutcome.Changed) =>
              logger.info(s"Storage device $deviceId changed state before it could be adopted. " +
                          s"The next scan will look at it again")
            case Success(AdoptOutcome.Unclean(stores)) =>
              warnUnclean(path, deviceId, previousHostId, stores)
            case Failure(t) =>
              warnOnce(path, s"Failed to adopt storage device $deviceId at $path. Will retry on the " +
                             s"next scan. Error: $t")

  /** The Offline entries of `state` in adoption class `cls`, by their directories under `path`. */
  def offlineStoresIn(path: os.Path,
                      state: StorageDeviceState,
                      cls: AdoptionClass): List[StoreId] =
    state.stores.collect {
      case (storeId, entry) if entry.status == StorageDeviceState.StoreStatus.Offline &&
                               adoptionClass(storeId, path / storeId.directoryName) == cls => storeId
    }.toList.sortBy(s => (s.poolId.uuid, s.poolIndex))

  /** The Offline entries of `state` whose directories under `path` were not closed cleanly. */
  def uncleanStores(path: os.Path, state: StorageDeviceState): List[StoreId] =
    offlineStoresIn(path, state, AdoptionClass.Unclean)

  private def warnUnclean(path: os.Path,
                          deviceId: StorageDeviceId,
                          previousHostId: HostId,
                          unclean: List[StoreId]): Unit =
    warnOnce(path, s"Storage device $deviceId at $path was shut down by host $previousHostId, but " +
                   s"stores ${unclean.mkString(", ")} were not closed cleanly (missing directory or " +
                   s"transfer-out marker). Not adopting it. It may have been " +
                   s"removed too early: return it to that host to finish the shutdown")

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
      DeviceMarkers.writeOwningHost(path, ctx.hostId)

    val active = state.stores.collect {
      case (storeId, entry) if entry.status == StorageDeviceState.StoreStatus.Active => storeId -> entry
    }
    active.foreach: (storeId, entry) =>
      val markerFile = path / storeId.directoryName / StoreTransfer.TransferOutMarkerFile
      if entry.transferDevice.isEmpty && !ctx.storeTransferService.isTransferringOut(storeId) &&
         os.exists(markerFile) then
        try os.remove(markerFile)
        catch case t: Throwable =>
          logger.error(s"Failed to remove the transfer-out marker for store $storeId at $markerFile. " +
                       s"The store stays offline until a later check of its device. Error: $t")

    if os.exists(path / DeviceMarkers.ShutdownStateFile) then
      DeviceMarkers.removeShutdownState(path)

    tryLoadDevice(path).foreach: deviceId =>
      ctx.reconciler.checkStorageDevice(deviceId)
      active.keysIterator.foreach(ctx.gossipStoreOnline)

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
        if sdCfg.aspenSystemId != ctx.aspenSystemId then
          warnOnce(storageDevicePath,
                   s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
          None
        else
          DeviceMarkers.readShutdownState(storageDevicePath) match
            case Some(markerHost) =>
              resolveShutdownMarker(storageDevicePath, sdCfg, markerHost)
              None
            case None =>
              DeviceMarkers.readOwningHost(storageDevicePath) match
                case Some(owner) if owner == ctx.hostId =>
                  loadOwnedDevice(storageDevicePath, sdCfgPath, sdCfg)
                case owner =>
                  val who = owner.map(h => s"host $h")
                    .getOrElse(s"no host (${DeviceMarkers.OwningHostFile} is missing)")
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
    devices.get(sdCfg.storageDeviceId) match
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
          loadedDirs += storageDevicePath -> sdCfg.storageDeviceId
        None
      case None =>
        // Nothing can be receiving onto a device that is not loaded yet, so whatever is in
        // its staging directory was left by an attempt that crashed or was abandoned. A copy
        // that had finished arriving was already moved to its final path.
        val staging = storageDevicePath / StoreTransfer.StagingDirectory
        try os.remove.all(staging)
        catch case t: Throwable =>
          logger.warn(s"Failed to remove stale transfer staging directory $staging: $t")
        // Likewise a store copy discardLostStore had moved aside but not yet deleted.
        val discarded = storageDevicePath / RebuildingStore.DiscardDirectory
        try os.remove.all(discarded)
        catch case t: Throwable =>
          logger.warn(s"Failed to remove discarded store directory $discarded: $t")
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
              ctx.tryLoadStore(sds, potentialStoreFile, None)
            // Registered only after every child has been offered to tryLoadStore. If
            // anything above throws, the catch in tryLoadDevice logs it and the device stays
            // unregistered, so the next scan retries the whole device rather than
            // skipping it as already loaded. That retry re-offers children whose
            // LoadStore events are already queued; tryLoadStore's own catch absorbs the
            // resulting duplicate-backend failures.
            ctx.devices.devices += sdCfg.storageDeviceId -> sds
            loadedDirs += storageDevicePath -> sdCfg.storageDeviceId
            reportedDirs -= storageDevicePath
            Some(sdCfg.storageDeviceId)
