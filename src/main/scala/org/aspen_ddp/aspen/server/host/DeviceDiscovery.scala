package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.DeviceMetadata.AdoptOutcome
import org.aspen_ddp.aspen.server.{DeviceMarkers, DurableFiles, FileStat, Host, StorageDeviceConfig,
  StorageDeviceManager, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, CrlConfigInvalid, CrlCorrupt, CrlLocked,
  CrlMissing, CrlSizingMismatch}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.{Backend, StoreCreation}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** How one store an adoption serves (an Offline or Active entry) was left, as adoptionClass reads it from its
 *  directory. */
private[aspen] enum AdoptionClass:
  /** Its state is on the device (the device CRL, or a network transfer's save file): adopted Active. */
  case Clean
  /** Its transaction state is gone (in lost/, or a transfer's save file missing or
   *  unusable): adopted Rebuilding. */
  case CrlLost
  /** Its directory is missing: the device is refused. */
  case Unclean

object DeviceDiscovery:
  /** Device scans (ScanDevices, each deviceScanPeriod) in a row whose open of a device's CRL may fail with a
   *  transient error before it is treated as lost. Only a scan's open counts: the others (host start, every
   *  CheckAllDevices, a nudge naming an unloaded device) retry it uncounted, so a transient condition must outlast
   *  CrlOpenAttempts - 1 scan periods to be declared, however many opens it fails */
  val CrlOpenAttempts = 3

  /** An I/O error or a VM error (out of memory) may clear on a later attempt. Anything else, corruption, a sizing
   *  mismatch or a bug, recurs on every attempt over the same files */
  def isTransient(t: Throwable): Boolean = t match
    case _: java.io.IOException | _: java.io.UncheckedIOException | _: VirtualMachineError => true
    case _ => false

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

  // Consecutive device scans whose open of each device directory's CRL failed transiently (openDeviceCrl). In memory
  // only: a restart allows CrlOpenAttempts more
  private var transientCrlFailures: Map[os.Path, Int] = Map()

  // Set while a device scan runs checkForNewDevices: only then does a transient CRL open failure count
  private var countingCrlFailures = false

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
   *  storageDevices, so a device's LocalStorageDeviceState -- and with it the copy registry
   *  that reconcileDeviceState reads -- survives, and its children are not re-offered to
   *  tryLoadStore over already-open backends.
   *
   *  Returns the devices this call loaded, so ScanDevices can check them at once.
   *
   *  `scan` is true only for ScanDevices: its transient CRL open failures count towards CrlOpenAttempts, while the
   *  other callers' are retried uncounted (openDeviceCrl), so the allowance is one per scan, not one per open.
   *
   *  Mutual exclusion: the handleEvent calls run on the control thread; the constructor call
   *  precedes start(), so no control thread runs yet.
   */
  def checkForNewDevices(scan: Boolean = false): List[StorageDeviceId] =
    countingCrlFailures = scan
    try discoverDevices()
    finally countingCrlFailures = false

  private def discoverDevices(): List[StorageDeviceId] =
    // A stat that fails loads nothing, as an absent directory does
    if ! Try(FileStat.isDir(storageDevicesDir)).getOrElse(false) then
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
      ctx.copyRegistry.awaitDevice(storageDevicePath, stores)
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
      // A stat that fails throws (FileStat): the next scan starts over.
      val unchanged = FileStat.isDir(path) && (
        if fromShutdownMarker then DeviceMarkers.readShutdownState(path).contains(previousHost)
        else
          !FileStat.exists(path / DeviceMarkers.ShutdownStateFile) &&
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
              try DeviceMarkers.writeFailed(path)
              catch case t: Throwable => logger.warn(s"Failed to write the failed marker of $path: $t")
              refuse("It has been declared failed. Not loading it")
            else if previousHost == ctx.hostId then
              state.status match
                case DeviceStatus.Shutdown if ownedHere =>
                  // Awaiting a restart or a move: its stores are an unloaded device's, silent, as unloadDevice
                  // leaves the stores of a device shut down while this host ran
                  ctx.copyRegistry.recordUnloadedEntries(deviceId, state.stores.keySet ++ marked)
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

  /** The stores with a configured copy on the device directory `path` outside pending-delete/, by their copies'
   *  names (CopyLayout.list). A listing or a config stat that fails throws (FileStat): a store it cannot see is never
   *  taken for none, so the caller leaves the device to the next scan */
  private def storesIn(path: os.Path): Set[StoreId] =
    CopyLayout.list(path).copies.filter(f =>
      f.phase != Phase.Retired && FileStat.isFile(f.path / StoreConfig.configFilename)).map(_.storeId).toSet

  /** Clears the awaiting-device marks of `stores`, unless an adoption of `path` has taken them over */
  private def releaseMarks(path: os.Path, stores: Set[StoreId]): Unit =
    if ! pendingResolutions.contains(path) then
      ctx.copyRegistry.deviceDecided(path)

  /** How the store whose entry is Offline or Active, with `copy` its copy on the device, can be put back into service
   *  by an adoption: a lookup on the copy's phase alone (spec "Adoption"). The host that served it is not here, so its
   *  state must be on the device: in the device CRL, or, for a network transfer in flight, in the store's own
   *  crl_save.log.
   *
   *  - No copy in any phase: the store's close or creation did not finish (or its directory was lost), and
   *    adopting it would drop whatever it held: Unclean, and the device is refused.
   *  - In lost/: CrlLost. Its host moved it there because its state cannot be trusted (a backend that failed while
   *    closing, a lost CRL).
   *  - In arrived/: Clean. A transfer in that landed during the shutdown (PoolRepointedOffline);
   *    it was never opened on this device, so it has no CRL state to carry.
   *  - In outbound/: a network transfer out had taken its state out of the device CRL into
   *    crl_save.log. Clean with a valid save file. Without one the state is gone -- a disk pulled
   *    before the file reached the platter, or later damage -- and the host that wrote it no longer
   *    holds it either: refusing the device would not bring it back, so it is CrlLost and adopted as
   *    Rebuilding. The save file is parsed, not just found: loadValidStoreState rejects a file cut
   *    short, and one whose header names another store. One it cannot read (CrashRecoveryLog.isUnreadable)
   *    throws, failing the adoption for the next scan to retry, rather than be taken for lost.
   *  - In rebuilt/ or parked/: Clean. A finished rebuild awaiting its flip, or one whose flip wrote its entry Offline
   *    (keepRebuiltStoreOffline): it has never served, and its empty save file is its whole state.
   *  - In live/: Clean. The shutdown closed it, or a crash or a pulled drive stopped it,
   *    with its state in the device CRL, which the adopting host opens before loading it.
   *
   *  A save file that cannot be read throws, failing the adoption for the next scan to retry.
   */
  private def adoptionClass(storeId: StoreId, copy: Option[CopyLayout.Found]): AdoptionClass =
    copy.map(_.phase) match
      case None => AdoptionClass.Unclean
      case Some(Phase.Live | Phase.Arrived(_) | Phase.Rebuilt(_) | Phase.Parked) => AdoptionClass.Clean
      case Some(Phase.Outbound(_)) =>
        CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(copy.get.path), storeId) match
          case Right(_) => AdoptionClass.Clean
          case Left(_) => AdoptionClass.CrlLost
      case Some(Phase.Lost) => AdoptionClass.CrlLost
      // Not listed: servedStoresIn leaves pending-delete/ out
      case Some(Phase.Retired) => AdoptionClass.Unclean

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

    if storesIn(path).nonEmpty && !FileStat.isDir(CrashRecoveryLog.deviceCrlDir(path)) then
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
                                     moveToLostUnattached(path))
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
    // A failed listing throws, failing the adoption for the next scan
    val copies = CopyLayout.list(path).copies.filter(_.phase != Phase.Retired)
    state.stores.collect {
      case (storeId, entry) if (entry.status == StorageDeviceState.StoreStatus.Offline ||
                                entry.status == StorageDeviceState.StoreStatus.Active) &&
                               adoptionClass(storeId, copies.find(_.storeId == storeId)) == cls =>
        storeId
    }.toList.sortBy(s => (s.poolId.uuid, s.poolIndex))

  /** T10 for an adoption's CrlLost store on the device at `path`, not attached yet: its copy moved to lost/ unless it
   *  is there. A move that fails throws, failing the adoption's transaction for the next scan to retry; one not
   *  forced is held for the device's attach (CopyMoves.moveUnattached) */
  private def moveToLostUnattached(path: os.Path)(storeId: StoreId): Unit =
    CopyLayout.list(path).copies.find(f => f.storeId == storeId && f.phase != Phase.Retired) match
      case Some(found) if found.phase != Phase.Lost =>
        ctx.moves.moveUnattached("adoption CrlLost", found.path, storeId, found.copyId, Phase.Lost)._1 match
          case MoveResult.NotMoved(t) => throw t
          case _ => ()
      case _ => ()

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

  /** Whether a copy in the phase directory of `phase` may go Live once its device is in service: outbound/ (T7),
   *  rebuilt/ (T3) and parked/ (T5) */
  private def mayGoLive(phase: Phase): Boolean = phase match
    case Phase.Outbound(_) | Phase.Rebuilt(_) | Phase.Parked => true
    case _ => false

  /** The local half of a restart or adoption, once `state` -- Operational and owned by this host
   *  -- has committed. Each step is idempotent, so a crash between any two of them is finished by
   *  the next scan, which finds shutdown.state still present and lands here again:
   *  1. `owning-host` is rewritten to name this host (adoption only), so once shutdown.state is
   *     gone the device loads here on its own. Each store's arrival (StoreLoader.ArrivedAtFile) is
   *     recorded just before, for its load floor, in its live/, outbound/, rebuilt/ or parked/ copy.
   *  2. A copy in outbound/ (an abandoned transfer's, T7) or parked/ (a rebuilt store kept offline,
   *     keepRebuiltStoreOffline, T5) is moved to Live (CopyMoves.moveUnattached) for every store now Active,
   *     except one whose entry still names a transfer or that this process is sending: there it
   *     belongs to a transfer. Offline entries -- none are expected -- keep theirs and stay
   *     unloaded. The shutdown itself leaves none. An arrived/ copy is left alone: the
   *     reconcile's completed-transfers-in pass writes its arrival, moves it Live and loads the store, as it
   *     would after any crash.
   *  3. shutdown.state is removed.
   *  4. The device is loaded through tryLoadDevice and checked at once, so its Rebuilding and
   *     TransferringIn work resumes now rather than at the next periodic check.
   *  5. StoreMoved is gossiped for every Active store, read from the pool as for a completed
   *     transfer in: the shutdown announced them all offline, and after an adoption peers must
   *     also learn the new host. loadOwnedDevice does it, once the stores' LoadStore events are
   *     queued ahead of any message a peer sends in response.
   *
   *  A failed move in step 2 only leaves that store offline: the reconcile's
   *  abandoned-transfer-out pass picks it up on a later check. A failed step 3 leaves the whole
   *  device to the next scan. A step 4 that does not load the device (its CRL is still shutting
   *  down, or the load throws) leaves it to a plain scan, shutdown.state being gone: the scan
   *  checks the device it loads, and announceOnLoad carries step 5 over to that load.
   */
  private def bringIntoService(path: os.Path,
                               sdCfg: StorageDeviceConfig,
                               state: StorageDeviceState,
                               rewriteOwner: Boolean): Unit =
    // A failed listing throws, leaving the device to the next scan
    val listing = CopyLayout.list(path).copies
    if rewriteOwner then
      // Each store arrives from another host, whose clock may lead this one's: its load floor must cover that. Before
      // the owner is rewritten, so that a crash between the two finds the adoption still to finish. Each live/ copy of
      // a store the device lists, and each in outbound/ or parked/, which step 2 may reinstate, and in rebuilt/, which
      // goes Live at the Completed Rebuilds pass. A directory stat that fails throws (FileStat), leaving the owner
      // unwritten and the adoption to the next scan
      listing.filter(f => state.stores.contains(f.storeId) && (f.phase == Phase.Live || mayGoLive(f.phase)))
        .map(_.path).filter(FileStat.isDir(_)).foreach(StoreLoader.writeArrivedAt)
      DeviceMarkers.writeOwningHost(path, ctx.hostId)

    val active = state.stores.collect {
      case (storeId, entry) if entry.status == StorageDeviceState.StoreStatus.Active => storeId -> entry
    }
    val offline = listing.filter(f => f.phase.isInstanceOf[Phase.Outbound] || f.phase == Phase.Parked)
    active.foreach: (storeId, entry) =>
      if ctx.copyRegistry.mayReinstateOut(entry, storeId) then
        // T7, T5. A move that fails keeps the store offline where it is, until a later check of its device (logged by
        // the move); one not forced leaves the copy unsynced, which its attach takes and the device's check forces
        // and loads
        offline.filter(_.storeId == storeId).foreach: found =>
          ctx.moves.moveUnattached(s"adoption brings ${found.phase} into service", found.path, storeId, found.copyId,
            Phase.Live): Unit

    announceOnLoad += path -> active.keySet
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

    // A stat that fails throws (FileStat) and the device is left for the next scan, never loaded on a false absence:
    // of the failed marker above all, which would serve a tombstoned device's stores (rebuild-process B19)
    def isDevice = FileStat.isDir(storageDevicePath) && FileStat.exists(sdCfgPath)

    if ctx.devices.closingCrls.contains(storageDevicePath) then
      logger.debug(s"Not loading the device at $storageDevicePath yet: its CRL is still shutting down")
      None
    else
      try
        if !isDevice then
          None
        else if DeviceMarkers.isFailed(storageDevicePath) then
          warnOnce(storageDevicePath, s"Storage device at $storageDevicePath has been declared failed. Not loading " +
                                      s"or claiming it")
          None
        else
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

  /** Opens the device's CRL. It is lost when its directory is absent from a device that holds stores, or holds no
   *  stream files there (CrlMissing: stores exist only on a device whose CRL was opened), or when opening it throws
   *  anything but a transient error (DeviceDiscovery.isTransient) or a transient error on CrlOpenAttempts device
   *  scans in a row (an open outside a scan neither counts nor declares; a success anywhere resets). The stores'
   *  transaction state was in it, so each is moved to lost/ (the reconcile pass turns them into rebuilds),
   *  except one that held nothing in it (holdsOwnState: its valid crl_save.log is its whole state, or it is a
   *  finished rebuild awaiting its flip). The old directory is kept aside as
   *  `crl.lost-<uuid>`, and a fresh CRL is created in `crl.creating` (CrashRecoveryLog.replacementCrlDir), renamed
   *  to `crl/` durably once finished, and opened. A failed creation removes `crl.creating` and is rethrown, leaving no
   *  `crl/`. A transient error before then is rethrown: the device stays unloaded and the next scan retries.
   *  A corrupt CRL (CrlCorrupt) is lost too, logged under its own cause.
   *  A CRL another writer holds open (CrlLocked) is never lost: it is rethrown each time, uncounted.
   *  Nor is a CRL whose files do not match the sizing it recorded, or that holds a `.crl.tmp` that is no unfinished
   *  creation (CrlSizingMismatch): configuration cannot cause that,
   *  so it is a hand edit or damage of the directory, never by itself proof that the state is gone. The device is
   *  refused (left unloaded, an error logged once) until an operator repairs it (systemic-loss-protection.md).
   *  Nor is a CRL directory holding no CRL yet whose device config cannot size one (CrlConfigInvalid): the device is
   *  refused the same way until the config is fixed.
   *  A device without stores simply gets its CRL created. */
  private def openDeviceCrl(devicePath: os.Path,
                            sdCfg: StorageDeviceConfig,
                            listing: CopyLayout.Listing): CrashRecoveryLog =
    val crlDir = CrashRecoveryLog.deviceCrlDir(devicePath)
    // Stats that fail are no absence (FileStat). A copy whose config cannot be stat'd may be a store, so it counts as
    // one (CopyLayout.list): missing it could take an emptied crl/ for a new CRL. A failed stat of crl/ itself throws,
    // leaving the device unloaded for the next scan, rather than declare a crl/ that is there lost. Retired copies
    // keep their CRL state in their own files
    val storeDirs = listing.copies.filter(_.phase != Phase.Retired)
      .filter(f => FileStat.existence(f.path / StoreConfig.configFilename).forall(identity))
    val lost =
      if storeDirs.nonEmpty && ! FileStat.exists(crlDir) then
        logger.error(s"Storage device $devicePath holds stores but has no CRL directory. Its stores must be rebuilt")
        true
      else
        try
          val crl = ctx.crlFactory.openCRL(devicePath, sdCfg, holdsStores = storeDirs.nonEmpty)
          transientCrlFailures -= devicePath
          return crl
        catch
          case t: CrlLocked =>
            // Another writer has the files open: a double mount, or a host still running that this one adopted from.
            // Its CRL is in use, not lost, so nothing is marked or moved aside, and the next scan retries
            transientCrlFailures -= devicePath
            throw t
          case t: CrlSizingMismatch =>
            transientCrlFailures -= devicePath
            if !reportedDirs.contains(devicePath) then
              reportedDirs += devicePath
              logger.error(s"Refusing to load storage device $devicePath: its CRL does not match the sizing it was " +
                           s"created with, or holds stray .crl.tmp files. Its stores are not declared lost. Restore " +
                           s"the CRL directory's stream files and sizing file (removing any stray .crl.tmp), then " +
                           s"the next scan loads it: $t")
            throw t
          case t: CrlConfigInvalid =>
            // The directory holds no CRL yet and the config cannot size one: a refusal, like a sizing mismatch.
            // Nothing was created, so the next scan after the config is fixed opens it
            transientCrlFailures -= devicePath
            if !reportedDirs.contains(devicePath) then
              reportedDirs += devicePath
              logger.error(s"Refusing to load storage device $devicePath: its config cannot size a new CRL. Its " +
                           s"stores are not declared lost. Fix the device config, then the next scan loads it: $t")
            throw t
          case t: Throwable =>
            if storeDirs.isEmpty then
              throw t
            if DeviceDiscovery.isTransient(t) then
              if ! countingCrlFailures then
                logger.error(s"Failed to open the CRL of storage device $devicePath outside a device scan. " +
                             s"Retrying on the next scan: $t")
                throw t
              val attempts = transientCrlFailures.getOrElse(devicePath, 0) + 1
              if attempts < DeviceDiscovery.CrlOpenAttempts then
                transientCrlFailures += devicePath -> attempts
                logger.error(s"Failed to open the CRL of storage device $devicePath (scan $attempts of " +
                             s"${DeviceDiscovery.CrlOpenAttempts}). Retrying on the next scan: $t")
                throw t
            transientCrlFailures -= devicePath
            t match
              case _: CrlCorrupt =>
                logger.error(s"The CRL of storage device $devicePath is corrupt: an entry fails its checksums. Its " +
                             s"stores must be rebuilt: $t")
              case _: CrlMissing =>
                logger.error(s"Storage device $devicePath holds stores but its CRL directory has no stream files. " +
                             s"Its stores must be rebuilt")
              case _ =>
                logger.error(s"Failed to recover the CRL of storage device $devicePath. Its stores must be rebuilt: $t")
            true
    // Decided for every store before any is moved: a read that fails leaves all of them as they are. A copy already
    // in lost/ stays there
    val toMove =
      try storeDirs.filter(_.phase != Phase.Lost).filterNot(holdsOwnState)
      catch case t: Throwable =>
        logger.error(s"The CRL of storage device $devicePath is lost, but a store's save file or config could not " +
                     s"be read to tell whether it holds its own state. Retrying on the next scan: $t")
        throw t
    // T10: into lost/. A move that fails, or lands unforced, throws, leaving the device unloaded (and its CRL where it
    // is) for the next scan, which decides again: a crash undoing an unforced move would put the copy back in live/
    // over the empty replacement CRL. Then every copy in lost/, moved now or by an earlier scan whose move landed
    // unforced, is forced before the CRL is replaced
    val lostCopies = toMove.map: found =>
      ctx.moves.moveUnattached("lost device CRL", found.path, found.storeId, found.copyId, Phase.Lost) match
        case (MoveResult.NotMoved(t), _) => throw t
        case (MoveResult.Unsynced(t), _) => throw t
        case (MoveResult.Durable, path) => path
    ctx.moves.forceUnattached(devicePath, storeDirs.filter(_.phase == Phase.Lost).map(_.path) ++ lostCopies)
    // A stat that fails throws (FileStat), leaving the device unloaded for the next scan, rather than leave a crl/ in
    // the replacement's way
    if FileStat.exists(crlDir) then
      DurableFiles.move(crlDir, devicePath / s"crl.lost-${UUID.randomUUID()}")
    // A fresh CRL, which the copies now in lost/ recover nothing from. Created apart and renamed into place only once
    // finished, so a crash or a failure (ENOSPC while preallocating) leaves no crl/ beside the stores: the next load
    // declares the CRL lost again, moving nothing aside, and creates it afresh
    val replacement = CrashRecoveryLog.replacementCrlDir(devicePath)
    try
      os.remove.all(replacement)
      os.makeDir(replacement)
      ctx.crlFactory.createCRL(devicePath, sdCfg, replacement)
      DurableFiles.move(replacement, crlDir)
    catch case t: Throwable =>
      try os.remove.all(replacement)
      catch case r: Throwable => t.addSuppressed(r)
      logger.error(s"Failed to create the CRL replacing the lost CRL of storage device $devicePath. Retrying on " +
                   s"the next scan: $t")
      throw t
    ctx.crlFactory.openCRL(devicePath, sdCfg, holdsStores = true)

  /** Whether the store directory `storeDir` holds nothing in the device CRL, so losing that CRL loses nothing of its.
   *  Either its whole transaction state is in its own crl_save.log, which validates as that store's (an arrived
   *  transfer, a transfer out's source, a rebuilt store kept offline): its load drops whatever the device CRL holds
   *  for it and loads the file (StoreLoader.awaitCrlLoad). A directory whose config cannot be read, or whose file is
   *  missing, cut short or another store's, does not hold its state that way; one whose config or save file could
   *  not be read (CrashRecoveryLog.isUnreadable) is not known either way, so the read's error is thrown, leaving the
   *  device unloaded for the next scan before any copy is moved to lost/. Or it is in rebuilt/: a
   *  finished rebuild awaiting its flip, which has never served (StoreLoader.tryLoadStore leaves it offline), so its
   *  transaction state is empty. Its rebuild or the reconcile's Completed Rebuilds pass flips and loads it as it
   *  would have over the old CRL; one finished for an earlier episode is retired by retireForeignCopy as before. A
   *  failed stat throws (FileStat), leaving the device unloaded for the next scan. */
  private def holdsOwnState(copy: CopyLayout.Found): Boolean =
    val storeDir = copy.path
    val saveFile = Backend.crlSaveFile(storeDir)
    def configuredStoreId: Option[StoreId] =
      try Some(StoreConfig.loadStoreConfig(storeDir / StoreConfig.configFilename).storeId)
      catch
        case t: Throwable if CrashRecoveryLog.isUnreadable(t) => throw t
        case NonFatal(_) => None
    def ownSaveFile: Boolean =
      FileStat.exists(saveFile) && configuredStoreId.exists: storeId =>
        CrashRecoveryLog.loadValidStoreState(saveFile, storeId).isRight
    // By location
    copy.phase match
      case Phase.Rebuilt(_) => true
      case _ => ownSaveFile

  /** P2 (spec I1, every phase): a store with more than one copy outside pending-delete/ on a device being loaded. It
   *  is logged at error, and every one of its copies is moved to lost/ (T10); the one with the least copy id stays
   *  there, to be registered Lost and retired by the normal passes, and the others are moved on into pending-delete/
   *  whole (T11's move, their files, crl_save.log included, kept inside them), released as any retired copy is. None
   *  is served, and the store's entry is then rebuilt through the Lost CRL pass. A move that fails throws, leaving the
   *  device unloaded for the next scan, which decides again over what is left. Returns the listing after the moves */
  private def retireDuplicates(devicePath: os.Path, listing: CopyLayout.Listing): CopyLayout.Listing =
    val duplicated = listing.copies.filter(_.phase != Phase.Retired).groupBy(_.storeId).filter(_._2.size > 1)
    if duplicated.isEmpty then
      listing
    else
      def moved(r: (MoveResult, os.Path)): os.Path = r match
        case (MoveResult.NotMoved(t), _) => throw t
        case (_, path) => path
      duplicated.toList.sortBy((s, _) => (s.poolId.uuid, s.poolIndex)).foreach: (storeId, found) =>
        logger.error(s"Storage device $devicePath holds ${found.size} copies of store $storeId " +
                     s"(${found.map(f => s"${f.phase} at ${f.path}").mkString(", ")}). Retiring every one; the store " +
                     "is rebuilt")
        val ordered = found.sortBy(_.copyId)
        val lost = ordered.map: f =>
          if f.phase == Phase.Lost then
            f.path
          else
            moved(ctx.moves.moveUnattached("duplicate copies at load", f.path, storeId, f.copyId, Phase.Lost))
        ordered.zip(lost).drop(1).foreach: (f, path) =>
          moved(ctx.moves.moveUnattached("duplicate copies at load", path, storeId, f.copyId, Phase.Retired))
      CopyLayout.list(devicePath)

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
        // Likewise for store creates: a complete store was already renamed out of it (StoreCreation)
        val creating = storageDevicePath / StoreCreation.CreatingDirectory
        try os.remove.all(creating)
        catch case t: Throwable =>
          logger.warn(s"Failed to remove stale store-creation directory $creating: $t")
        // And whatever an interrupted delete left (StoreCopies.delete)
        StoreCopies.sweepDeleting(storageDevicePath)
        // And an unfinished replacement of a lost CRL (openDeviceCrl): it holds no entry
        val replacementCrl = CrashRecoveryLog.replacementCrlDir(storageDevicePath)
        try os.remove.all(replacementCrl)
        catch case t: Throwable =>
          logger.warn(s"Failed to remove the unfinished replacement CRL directory $replacementCrl: $t")
        // P6: a phase directory that cannot be listed, or a phase-param that cannot be read, leaves the device
        // unloaded, nothing on it moved, for the next scan
        Try(CopyLayout.list(storageDevicePath)) match
          case Failure(t) =>
            // Same hazard as the scan in checkForNewDevices: listing can fail with an IO
            // error even when isDir just succeeded. The catch in tryLoadDevice would absorb
            // the exception and leave the device unregistered and retryable just the same;
            // this guard exists to name the directory instead.
            warnOnce(storageDevicePath, s"Failed to list the copies of storage device $storageDevicePath. " +
                                        s"Will retry on the next scan. Error: $t")
            None
          case Success(listing) =>
            // Opened before any store is offered, so each store recovers from it. Recovery blocks the control
            // thread. A throw leaves the device unloaded.
            val crl = openDeviceCrl(storageDevicePath, sdCfg, listing)
            val sds = new LocalStorageDeviceState(sdCfg.storageDeviceId, storageDevicePath, sdCfgPath, crl,
              new CrlAdmission(crl.admissionLimitBytes), Some(ctx.deviceIos.forPath(storageDevicePath)))
            // A growth raises the limit the stores admit against; registered before any store loads
            crl.onAdmissionLimitRaised(sds.admission.raiseLimit)
            // Everything the CRL holds is counted, retained for its store, before any store loads: a store that loads
            // counts its own afresh, and the entries of one that does not are copied forward all the same
            // (crash-recovery-log 6.1)
            crl.recoveredBytesByStore.foreach((storeId, bytes) => sds.admission.chargeRetained(storeId, bytes))
            // On the control context. Ignored unless sds is still the device loaded (DeviceShutdown.crlFailed)
            crl.failed.foreach(t => ctx.shutdown.crlFailed(sds, t))
            logger.info(s"Loading storage device $storageDevicePath. StorageDeviceId ${sds.storageDeviceId}")
            try
              // Its copies registered before any store is offered (HostCopies). Listed again: a lost CRL's open moved
              // copies to lost/ since the listing above, and P2 may move more
              val copies = retireDuplicates(storageDevicePath, CopyLayout.list(storageDevicePath))
              ctx.copyRegistry.attach(sds, copies)
              // Only Live copies are offered to the loader: every other phase is its pass's
              copies.copies.filter(_.phase == Phase.Live).foreach: found =>
                ctx.tryLoadStore(sds, found.path, None)
            catch
              case t: Throwable =>
                // Shut down as an unload does, so the retry does not open the files a second time
                ctx.copyRegistry.detach(sds.storageDeviceId, Set())
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
