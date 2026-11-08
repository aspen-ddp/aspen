package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.DeviceMetadata.AdoptOutcome
import org.aspen_ddp.aspen.server.{DeviceMarkers, DurableFiles, Host, StorageDeviceConfig, StorageDeviceManager,
  StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** How one store an adoption serves (an Offline or Active entry) was left, as adoptionClass reads it from its
 *  directory. */
private[aspen] enum AdoptionClass:
  /** Its state is on the device (the device CRL, or a network transfer's save file): adopted Active. */
  case Clean
  /** Its transaction state is gone (marked rebuild-required, or a transfer's save file missing or
   *  unusable): adopted Rebuilding. */
  case CrlLost
  /** Its directory is missing: the device is refused. */
  case Unclean

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Finds the storage devices under `storage-devices/` and decides whether to load, restart, or adopt each. */
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
  // its outcome has been acted on, so the next scan decides afresh. Each holds the stores found
  // in the directory, marked in the router as awaiting their device until then, so their
  // messages are dropped (and retransmitted) rather than answered as unknown while it is decided.
  private var pendingResolutions: Map[os.Path, Set[StoreId]] = Map()

  // The Active stores of a restarted or adopted device, by its directory, whose StoreMoved gossip waits for the
  // device to load. bringIntoService removes shutdown.state before loading, so a load that does not happen then (the
  // device's CRL is still shutting down, or the load throws) is left to a plain scan, which knows nothing of the
  // restart: without this, peers would keep the stores' StoreOffline. loadOwnedDevice gossips them and drops the entry.
  private var announceOnLoad: Map[os.Path, Set[StoreId]] = Map()

  // Directories of devices this process relinquished because the storage-devices tree records another host as
  // their owner (DeviceShutdown.relinquishDevice). Their owning-host file still names this host, so without this
  // the scan would load each again and the reconcile unload it, forever. Only a drive still attached here after
  // its adoption elsewhere reaches this, and it is never written to again. An entry is dropped when its path stops
  // being listed, as loadedDirs' are. In memory only: a restart loads it once more and relinquishes it again.
  private var disownedDirs: Set[os.Path] = Set()

  /** Keeps the scan from loading the device at `path` again (see disownedDirs). */
  private[host] def disown(path: os.Path): Unit =
    disownedDirs += path

  private[server] def disownedDeviceDirs: Set[os.Path] = disownedDirs

  private[server] def loadedDeviceDirs: Map[os.Path, StorageDeviceId] = loadedDirs
  private[server] def reportedDeviceDirs: Set[os.Path] = reportedDirs
  private[server] def pendingMarkerResolutions: Set[os.Path] = pendingResolutions.keySet

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
   *  Mutual exclusion: the handleEvent calls run on the control thread; the constructor call
   *  precedes start(), so no control thread runs yet.
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
          disownedDirs = disownedDirs.filter(listed.contains)
          paths.filterNot(loadedDirs.contains).flatMap(tryLoadDevice).toList

  /** Logs `msg` unless a problem with `path` has already been logged by this process. */
  def warnOnce(path: os.Path, msg: => String): Unit =
    if ! reportedDirs.contains(path) then
      reportedDirs += path
      ctx.warn(path, msg)

  /** Called instead of loading a device that carries `shutdown.state` naming `previousHost`
   *  (`fromShutdownMarker`), or whose `owning-host` names `previousHost`, another host.
   *
   *  Whether such a device stays shut down, is restarted in place, or is adopted by this host
   *  depends on its StorageDeviceState, which is a network read, so the load decision cannot be
   *  made here. The lookup is issued and its result queued as a DeviceOwnershipResolved event,
   *  which handleOwnershipResolved acts on from the event loop. Until then the directory is not
   *  in loadedDeviceDirs, so later scans reach it again; pendingMarkerResolutions keeps them from
   *  issuing a second lookup while this one is in flight. Meanwhile its stores are marked as
   *  awaiting their device, so the router drops their messages instead of answering UnknownStore.
   */
  private def resolveOwnership(storageDevicePath: os.Path,
                               sdCfg: StorageDeviceConfig,
                               previousHost: HostId,
                               fromShutdownMarker: Boolean): Unit =
    if ! pendingResolutions.contains(storageDevicePath) then
      val stores = storesIn(storageDevicePath)
      stores.foreach(ctx.router.markAwaitingDevice)
      pendingResolutions += storageDevicePath -> stores
      // A throw before the Future exists would register no callback, and the path would stay
      // pending -- and the device unresolved -- for the life of the process.
      val lookup =
        try ctx.metadata.lookupStorageDeviceState(sdCfg.storageDeviceId)
        catch case t: Throwable => Future.failed(t)
      lookup.onComplete: result =>
        ctx.post(Host.DeviceOwnershipResolved(storageDevicePath, sdCfg, previousHost, fromShutdownMarker, result))

  /** Acts on the StorageDeviceState looked up for a device this host does not load on its own. The previous host is
   *  the one named by `shutdown.state` when the device carries it, otherwise by `owning-host`:
   *
   *  | previous host | device state                         | action                                  |
   *  |---------------|--------------------------------------|-----------------------------------------|
   *  | this host     | Shutdown                             | not loaded, quietly: it awaits a restart or a move |
   *  | this host     | ShuttingDown                         | loaded: a crash before Shutdown committed. The reconcile finishes the shutdown |
   *  | this host     | Operational, owned here              | restarted while unloaded: brought back into service |
   *  | another host  | owned by the previous host           | adopted (tryAdoptDevice), shut down cleanly or not |
   *  | another host  | owned here                           | adopted earlier, crashed before the load: brought into service |
   *  | any           | Failed, or anything else             | warned about once, not loaded            |
   *  | lookup failed |                                      | warned about once, retried on the next scan |
   *
   *  "This host" arises only from `shutdown.state`: a device whose `owning-host` names this host is
   *  loaded without a lookup. Adoption needs no shutdown: the device's CRL is on the drive, so a disk
   *  pulled from a running or crashed host is in the state a crash of that host leaves it, and the
   *  adopter recovers its stores from the CRL as that host's restart would. A drive found here is
   *  taken to be out of the previous host's reach. Its CRL must be on it, though (tryAdoptDevice).
   *
   *  Every row that does nothing leaves the directory out of loadedDeviceDirs, so the next scan
   *  decides again from a fresh read: a restart or a finished shutdown is picked up that way.
   */
  def handleOwnershipResolved(e: Host.DeviceOwnershipResolved): Unit =
    val Host.DeviceOwnershipResolved(path, sdCfg, previousHost, fromShutdownMarker, result) = e
    import StorageDeviceState.DeviceStatus
    // The marks are cleared once the outcome has been acted on, so a store the outcome loads is
    // already marked loading by then; an adoption started here takes them over instead.
    val marked = pendingResolutions.getOrElse(path, Set())
    pendingResolutions -= path
    val deviceId = sdCfg.storageDeviceId
    try
      // The disk was read before the lookup went out. If it has been pulled, or the file naming
      // the previous host has changed, since then, this result describes a directory that is no
      // longer there to act on; the next scan starts over from what the disk says now.
      val unchanged = os.isDir(path) && (
        if fromShutdownMarker then DeviceMarkers.readShutdownState(path).contains(previousHost)
        else
          !os.exists(path / DeviceMarkers.ShutdownStateFile) &&
            DeviceMarkers.readOwningHost(path).contains(previousHost))
      if ! unchanged || loadedDirs.contains(path) then
        ()
      else if devices.get(deviceId).exists(_.devicePath != path) then
        warnOnce(path, s"Storage device $deviceId is already loaded from " +
                       s"${devices(deviceId).devicePath}; ignoring duplicate at $path")
      else
        result match
          case Failure(err) =>
            warnOnce(path, s"Failed to read the state of storage device $deviceId at $path. " +
                           s"Not loading it. Will retry on the next scan. Error: $err")

          case Success(state) =>
            val ownedHere = state.hostId == ctx.hostId
            def refuse(why: String): Unit =
              val how = if fromShutdownMarker then "was shut down by" else "was last owned by"
              warnOnce(path, s"Storage device $deviceId at $path $how host $previousHost, " +
                             s"and the storage-devices tree records it as ${state.status} owned by " +
                             s"host ${state.hostId}. $why")

            if state.isFailed then
              refuse("It has been declared failed. Not loading it")
            else if previousHost == ctx.hostId then
              state.status match
                case DeviceStatus.Shutdown if ownedHere =>
                  // Awaiting a restart or a move: silent, as unloadDevice leaves the stores of a
                  // device shut down while this host ran
                  marked.foreach(ctx.router.markOffline)
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
            else if state.hostId == previousHost then
              tryAdoptDevice(path, sdCfg, state, previousHost, marked)
            else if ownedHere then
              // Shutdown or ShuttingDown too: a shutdown requested before the load. The load's
              // reconcile carries it on.
              logger.info(s"Storage device $deviceId at $path was adopted by this host but not " +
                          s"yet loaded. Finishing the adoption")
              bringIntoService(path, sdCfg, state, rewriteOwner = true)
            else
              refuse("Not loading or adopting it")
    catch
      case t: Throwable =>
        warnOnce(path, s"Failed to resolve the ownership of storage device $deviceId at $path. " +
                       s"Will retry on the next scan. Error: $t")
    finally
      releaseMarks(path, marked)

  /** The stores whose directories are under the device directory `path`, by their store configs */
  private def storesIn(path: os.Path): Set[StoreId] =
    Try(os.list(path)).getOrElse(Nil).iterator.flatMap: dir =>
      val cfgPath = dir / StoreConfig.configFilename
      if os.isFile(cfgPath) then Try(StoreConfig.loadStoreConfig(cfgPath).storeId).toOption else None
    .toSet

  /** Clears the awaiting-device marks of `stores`, unless an adoption of `path` has taken them over */
  private def releaseMarks(path: os.Path, stores: Set[StoreId]): Unit =
    if ! pendingResolutions.contains(path) then
      stores.foreach(ctx.router.clearAwaitingDevice)

  /** How the store at `storePath`, whose entry is Offline or Active, can be put back into service by
   *  an adoption. The host that served it is not here, so its state must be on the device: in
   *  the device CRL, or, for a network transfer in flight, in the store's own crl_save.log.
   *
   *  - No directory: the store's close or creation did not finish (or its directory was lost), and
   *    adopting it would drop whatever it held: Unclean, and the device is refused.
   *  - `rebuild-required`: CrlLost whatever else the directory holds. Its host marked it because
   *    its state cannot be trusted (a backend that failed while closing, a lost CRL).
   *  - `transferred-in`: Clean. A transfer in that landed during the shutdown (PoolRepointedOffline);
   *    it was never opened on this device, so it has no CRL state to carry.
   *  - `transferring-out`: a network transfer out had taken its state out of the device CRL into
   *    crl_save.log. Clean with a valid save file. Without one the state is gone -- a disk pulled
   *    before the file reached the platter, or later damage -- and the host that wrote it no longer
   *    holds it either: refusing the device would not bring it back, so it is CrlLost and adopted as
   *    Rebuilding. The save file is parsed, not just found: loadValidStoreState rejects a file cut
   *    short, and one whose header names another store. A store rebuilt during or after the
   *    shutdown (keepRebuiltStoreOffline) carries the marker with an empty save file: Clean.
   *  - Any other directory: Clean. The shutdown closed it, or a crash or a pulled drive stopped it,
   *    with its state in the device CRL, which the adopting host opens before loading it.
   */
  private def adoptionClass(storeId: StoreId, storePath: os.Path): AdoptionClass =
    if !os.isDir(storePath) then
      AdoptionClass.Unclean
    else if os.exists(storePath / RebuildingStore.RebuildRequiredMarkerFile) then
      AdoptionClass.CrlLost
    else if os.exists(storePath / StoreTransfer.TransferredInMarkerFile) then
      AdoptionClass.Clean
    else if os.exists(storePath / StoreTransfer.TransferOutMarkerFile) then
      CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storePath), storeId) match
        case Right(_) => AdoptionClass.Clean
        case Left(_) => AdoptionClass.CrlLost
    else
      AdoptionClass.Clean

  /** Validates a device last owned by `previousHostId`, shut down or not, and adopts it if its CRL
   *  is on it and every store it serves has its directory. On success the post-commit steps run
   *  from the transaction's callback.
   *
   *  The CRL must be in `<device>/crl` whenever the device holds a store: the stores' transaction
   *  state is there, and a device without it must go back to the host that has it rather than
   *  have every store rebuilt. With CRLs kept on their device that is only damage, but a device
   *  whose CRL lives elsewhere would be shipped by a shutdown that moves its CRL onto it as the
   *  last step (Backlog.txt): one pulled before that step lacks the directory.
   *
   *  The directory stays in pendingMarkerResolutions until the adoption has been acted on, so a
   *  scan arriving meanwhile does not start a second one, and its stores stay marked as awaiting
   *  their device.
   */
  private def tryAdoptDevice(path: os.Path,
                             sdCfg: StorageDeviceConfig,
                             state: StorageDeviceState,
                             previousHostId: HostId,
                             marked: Set[StoreId]): Unit =
    val deviceId = sdCfg.storageDeviceId
    // Checked here as well as inside adoptDevice, against the looked-up state, so a device that
    // is plainly not ready costs no transaction. The check that counts is adoptDevice's own.
    val unclean = uncleanStores(path, state)

    if storesIn(path).nonEmpty && !os.isDir(CrashRecoveryLog.deviceCrlDir(path)) then
      warnOnce(path, s"Storage device $deviceId at $path, last owned by host $previousHostId, holds stores but its " +
                     s"CRL is not on it (${CrashRecoveryLog.deviceCrlDir(path)}). Not adopting it. Return it to " +
                     s"that host")
    else if unclean.nonEmpty then
      warnUnclean(path, deviceId, previousHostId, unclean)
    else
      logger.info(s"Adopting storage device $deviceId at $path, ${state.status} and owned by host $previousHostId")
      def handleAdoption(result: Try[AdoptOutcome]): Unit =
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

      pendingResolutions += path -> marked
      val fadopt =
        try ctx.metadata.adoptDevice(deviceId, previousHostId, path, uncleanStores,
                                     (p, s) => servedStoresIn(p, s, AdoptionClass.CrlLost),
                                     ctx.rebuilds.markRebuildRequired)
        catch case t: Throwable => Future.failed(t)
      fadopt.onComplete: result =>
        pendingResolutions -= path
        try handleAdoption(result)
        finally releaseMarks(path, marked)

  /** The entries of `state` an adoption serves -- Offline ones, and the Active ones of a device not
   *  shut down -- in adoption class `cls`, by their directories under `path`. */
  def servedStoresIn(path: os.Path,
                     state: StorageDeviceState,
                     cls: AdoptionClass): List[StoreId] =
    state.stores.collect {
      case (storeId, entry) if (entry.status == StorageDeviceState.StoreStatus.Offline ||
                                entry.status == StorageDeviceState.StoreStatus.Active) &&
                               adoptionClass(storeId, path / storeId.directoryName) == cls => storeId
    }.toList.sortBy(s => (s.poolId.uuid, s.poolIndex))

  /** The Offline and Active entries of `state` whose directories under `path` are missing. */
  def uncleanStores(path: os.Path, state: StorageDeviceState): List[StoreId] =
    servedStoresIn(path, state, AdoptionClass.Unclean)

  private def warnUnclean(path: os.Path,
                          deviceId: StorageDeviceId,
                          previousHostId: HostId,
                          unclean: List[StoreId]): Unit =
    warnOnce(path, s"Storage device $deviceId at $path, last owned by host $previousHostId, has no directory for " +
                   s"stores ${unclean.mkString(", ")}. Not adopting it. It may have been " +
                   s"removed too early: return it to that host to finish the shutdown")

  /** The local half of a restart or adoption, once `state` -- Operational and owned by this host
   *  -- has committed. Each step is idempotent, so a crash between any two of them is finished by
   *  the next scan, which finds shutdown.state still present and lands here again:
   *  1. `owning-host` is rewritten to name this host (adoption only), so once shutdown.state is
   *     gone the device loads here on its own.
   *  2. A stray transfer-out marker (a rebuilt store's, keepRebuiltStoreOffline, or an abandoned
   *     transfer's) is removed from every store now Active, except one whose entry still names a
   *     transfer or that this process is sending: there the marker belongs to a transfer. Offline
   *     entries -- none are expected -- keep theirs and stay unloaded. The shutdown itself leaves
   *     no marker. A transferred-in marker is left alone: the reconcile's completed-transfers-in
   *     pass clears it and loads the store, as it would after any crash.
   *  3. shutdown.state is removed.
   *  4. The device is loaded through tryLoadDevice and checked at once, so its Rebuilding and
   *     TransferringIn work resumes now rather than at the next periodic check.
   *  5. StoreMoved is gossiped for every Active store, read from the pool as for a completed
   *     transfer in: the shutdown announced them all offline, and after an adoption peers must
   *     also learn the new host. loadOwnedDevice does it, once the stores' LoadStore events are
   *     queued ahead of any message a peer sends in response.
   *
   *  A failed marker removal in step 2 only leaves that store offline: the reconcile's
   *  abandoned-transfer-out pass picks it up on a later check. A failed step 3 leaves the whole
   *  device to the next scan. A step 4 that does not load the device (its CRL is still shutting
   *  down, or the load throws) leaves it to a plain scan, shutdown.state being gone: the scan
   *  checks the device it loads, and announceOnLoad carries step 5 over to that load.
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

    announceOnLoad += path -> active.keySet
    if os.exists(path / DeviceMarkers.ShutdownStateFile) then
      DeviceMarkers.removeShutdownState(path)

    tryLoadDevice(path).foreach(ctx.reconciler.checkStorageDevice)

  /** Loads the device at `storageDevicePath` if this host owns it, returning its id when this
   *  call is what loaded it.
   *
   *  The load decision is made from device-local files before anything on the device is
   *  touched -- not even its stale staging directory is removed -- because a device that is
   *  not ours may still be needed exactly as it is by the host that does own it:
   *  - `shutdown.state` present: handed to resolveOwnership, which decides.
   *  - otherwise `owning-host` names this host: loaded, unless this process relinquished it to
   *    another host (disownedDirs).
   *  - otherwise `owning-host` names another host: handed to resolveOwnership too. A disk pulled
   *    from that host, running or crashed, carries its CRL, so it may be adopted here
   *    (handleOwnershipResolved).
   *  - otherwise (no file, malformed file): not loaded and never claimed.
   *  The decision for a device of ours needs no network, which matters at startup: a host may
   *  need its own devices loaded before it can read any metadata.
   *
   *  Nothing is decided for a path whose unloaded device's CRL is still shutting down
   *  (LocalDevices.closingCrls): a second CRL opened over the same files would corrupt them. The
   *  scan retries it once the shutdown has ended.
   */
  private def tryLoadDevice(storageDevicePath: os.Path): Option[StorageDeviceId] =
    val sdCfgPath = storageDevicePath / StorageDeviceConfig.configFilename

    if ctx.devices.closingCrls.contains(storageDevicePath) then
      logger.debug(s"Not loading the device at $storageDevicePath yet: its CRL is still shutting down")
      None
    else if os.isDir(storageDevicePath) && os.exists(sdCfgPath) then
      try
        val sdCfg = StorageDeviceConfig.loadStorageDeviceConfig(sdCfgPath)
        if sdCfg.aspenSystemId != ctx.aspenSystemId then
          warnOnce(storageDevicePath,
                   s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
          None
        else
          DeviceMarkers.readShutdownState(storageDevicePath) match
            case Some(markerHost) =>
              resolveOwnership(storageDevicePath, sdCfg, markerHost, fromShutdownMarker = true)
              None
            case None =>
              DeviceMarkers.readOwningHost(storageDevicePath) match
                case Some(owner) if owner == ctx.hostId =>
                  if disownedDirs.contains(storageDevicePath) then
                    None
                  else
                    loadOwnedDevice(storageDevicePath, sdCfgPath, sdCfg)
                case Some(owner) =>
                  resolveOwnership(storageDevicePath, sdCfg, owner, fromShutdownMarker = false)
                  None
                case None =>
                  warnOnce(storageDevicePath, s"Storage device ${sdCfg.storageDeviceId} at $storageDevicePath " +
                                              s"has no ${DeviceMarkers.OwningHostFile} file. Not loading or " +
                                              s"claiming it")
                  None
      catch
        case t: Throwable =>
          warnOnce(storageDevicePath, s"Failed to load storage device found at path $storageDevicePath. Error: $t")
          None
    else
      None

  /** Opens the device's CRL. It is lost when its directory is absent from a device that holds stores, or when
   *  opening it throws. The stores' transaction state was in it, so each is marked rebuild-required (the reconcile
   *  pass turns them into rebuilds), the old directory is kept aside as `crl.lost-<uuid>`, and a fresh CRL is opened.
   *  A device without stores simply gets its CRL created. */
  private def openDeviceCrl(devicePath: os.Path,
                            sdCfg: StorageDeviceConfig,
                            children: Seq[os.Path]): CrashRecoveryLog =
    val crlDir = CrashRecoveryLog.deviceCrlDir(devicePath)
    val storeDirs = children.filter(dir => os.isDir(dir) && os.isFile(dir / StoreConfig.configFilename))
    val lost =
      if storeDirs.nonEmpty && ! os.exists(crlDir) then
        logger.error(s"Storage device $devicePath holds stores but has no CRL directory. Its stores must be rebuilt")
        true
      else
        try
          return ctx.crlFactory.openCRL(devicePath, sdCfg)
        catch
          case t: Throwable =>
            if storeDirs.isEmpty then
              throw t
            logger.error(s"Failed to recover the CRL of storage device $devicePath. Its stores must be rebuilt: $t")
            true
    storeDirs.foreach(ctx.rebuilds.markRebuildRequired)
    if os.exists(crlDir) then
      os.move(crlDir, devicePath / s"crl.lost-${UUID.randomUUID()}")
    DurableFiles.syncDirectory(devicePath)
    ctx.crlFactory.openCRL(devicePath, sdCfg)

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
            // Opened before any store is offered, so each store recovers from it. Recovery blocks the control
            // thread, as each store's getFullRecoveryState does in handleLoad. A throw leaves the device unloaded.
            val crl = openDeviceCrl(storageDevicePath, sdCfg, storeFiles)
            val sds = new LocalStorageDeviceState(sdCfg.storageDeviceId, storageDevicePath, sdCfgPath, crl,
              new CrlAdmission(crl.admissionLimitBytes))
            logger.info(s"Loading storage device $storageDevicePath. StorageDeviceId ${sds.storageDeviceId}")
            try
              storeFiles.foreach: potentialStoreFile =>
                ctx.tryLoadStore(sds, potentialStoreFile, None)
            catch
              case t: Throwable =>
                // Shut down as an unload does, so the retry does not open the files a second time
                ctx.shutdown.shutDownCrl(storageDevicePath, crl)
                throw t
            // Registered only after every child has been offered to tryLoadStore. If
            // anything above throws, the catch in tryLoadDevice logs it and the device stays
            // unregistered, so the next scan retries the whole device rather than
            // skipping it as already loaded. That retry re-offers children whose
            // LoadStore events are already queued; tryLoadStore's own catch absorbs the
            // resulting duplicate-backend failures.
            ctx.devices.devices += sdCfg.storageDeviceId -> sds
            ctx.publishView()
            loadedDirs += storageDevicePath -> sdCfg.storageDeviceId
            reportedDirs -= storageDevicePath
            announceOnLoad.get(storageDevicePath).foreach: stores =>
              announceOnLoad -= storageDevicePath
              stores.toList.sortBy(s => (s.poolId.uuid, s.poolIndex)).foreach(ctx.gossipStoreOnline)
            Some(sdCfg.storageDeviceId)
