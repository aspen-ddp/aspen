package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.DeviceMetadata.AdoptOutcome
import org.aspen_ddp.aspen.server.{Host, StorageDeviceConfig, StorageDeviceManager}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, CrlConfigInvalid, CrlSizingMismatch}
import scribe.Logging

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/** How one store an adoption serves (an Offline or Active entry) was left, as DeviceStateDisk.adoptionClass reads it
 *  from its directory. */
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

  /** What asked for a discovery: `scan`, a ScanDevices, whose loads' transient CRL open failures count
   *  (CrlOpenAttempts); `check`, each device it loads is checked at once (DeviceReconciler.checkStorageDevice), or,
   *  for the `startup` scan, by a CheckLoadedDevice event posted as its load applies */
  final case class Trigger(scan: Boolean, check: Boolean, startup: Boolean = false):
    def |(o: Trigger): Trigger = Trigger(scan || o.scan, check || o.check, startup || o.startup)

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
  // re-inserted on this host is checked again in full (and the duplicate guard in loadOwnedDevice
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

  // Consecutive device scans whose open of each device directory's CRL failed transiently (openCrl). In memory
  // only: a restart allows CrlOpenAttempts more
  private var transientCrlFailures: Map[os.Path, Int] = Map()

  // The listing of storage-devices/ is outstanding on the host root's lane. A discovery asked for meanwhile is merged
  // into `relist` and runs once it has been applied, so at most one listing waits behind a slow host root
  private var listing = false
  private var relist: Option[DeviceDiscovery.Trigger] = None

  // The paths the last listing that succeeded returned. A job's result for a path no longer listed changes nothing
  private var listed: Set[os.Path] = Set()

  // The device directories with a discovery job outstanding on their own DeviceIo: a probe, or a load's sweep or P2 job.
  // A scan skips them (coalescing, so a hung device's lane holds one discovery job at most). A load or probe asked for
  // one explicitly (`again`) runs once its job has been applied. The entry is the token each of its jobs' results is
  // checked against: a result whose token is no longer the path's changes nothing
  private final class Busy(val io: DeviceIo):
    var again: Option[DeviceDiscovery.Trigger] = None
  private var busy: Map[os.Path, Busy] = Map()

  // The startup gate (startupScan): the paths the startup scan began a probe of whose decision is still to come, and
  // what to run once there are none, a listing no longer names them, or the first ScanDevices comes. None once run
  private var startupPaths: Option[Set[os.Path]] = None
  private var startupDecided: () => Unit = () => ()

  /** bringIntoService's step 3 (DeviceStateDisk.bringIntoService); a test seam */
  private[server] var removeShutdownState: os.Path => Unit = DeviceStateDisk.removeShutdownState

  /** Keeps the scan from loading the device at `path` again (see disownedDirs). */
  private[host] def disown(path: os.Path): Unit =
    disownedDirs += path

  private[server] def disownedDeviceDirs: Set[os.Path] = disownedDirs

  private[server] def loadedDeviceDirs: Map[os.Path, StorageDeviceId] = loadedDirs
  private[server] def reportedDeviceDirs: Set[os.Path] = reportedDirs
  private[server] def pendingMarkerResolutions: Set[os.Path] = pendingResolutions.keySet
  /** The device directories with a discovery job outstanding (a probe, or a load's sweep or P2 job) */
  private[server] def busyDeviceDirs: Set[os.Path] = busy.keySet
  /** Whether the startup scan has decided every path it listed */
  private[server] def startupScanDecided: Boolean = startupPaths.isEmpty

  /** Forgets the directory of a device that has been unloaded, so the next scan reads it again. */
  private[host] def forgetDevice(storageDeviceId: StorageDeviceId): Unit =
    loadedDirs = loadedDirs.filter((_, id) => id != storageDeviceId)

  /** The Host constructor's scan, before the threading starts: the same jobs as every scan, so it blocks on no device.
   *  Each device it loads is checked by a CheckLoadedDevice event posted as its load applies. `decided` runs once, on
   *  the control thread, at the first of: every path it listed decided (its device attached, or left unloaded, or
   *  handed to resolveOwnership, whose stores are marked awaiting their device; a path a later listing no longer
   *  names counts as decided), its listing failing or throwing, or the first ScanDevices (`endStartupGate`). So a
   *  path whose job never returns holds it back for one scan period at most: no timeout on its I/O, which stays
   *  outstanding */
  def startupScan(decided: () => Unit): Unit =
    startupDecided = decided
    startupPaths = Some(Set())
    discover(DeviceDiscovery.Trigger(scan = false, check = true, startup = true), startup = true)

  /** The first ScanDevices after startup: runs the startup gate's `decided` if it has not run */
  def endStartupGate(): Unit =
    if startupPaths.isDefined then
      startupPaths = None
      startupDecided()

  /** Scans storage-devices/ and loads any device found there that this host owns.
   *
   *  Called from the event loop: every ScanDevices (each deviceScanPeriod), every CheckAllDevices, and every
   *  CheckStorageDevice naming a device we have not loaded. Nothing here touches a disk on the control thread: the
   *  listing is one job on the host root's lane (DeviceIos.hostRoot), and each path it lists that holds no loaded
   *  device and has no discovery job outstanding (`busy`) gets one probe job on its own DeviceIo (DiscoveryDisk.probe:
   *  stat, config, markers), whose result is decided on its own (`decide`), so a hung device delays only its own load.
   *  Its problems are logged once (warnOnce) and then rechecked silently. A path gone from the listing, with no device
   *  loaded from it and no job outstanding, has its DeviceIo closed (DeviceIos.forget) once that drops nothing; a busy
   *  one is kept and asked again at the next listing.
   *
   *  Repeated scans are safe: a device already present in storageDevices is never loaded again, so a device's
   *  LocalStorageDeviceState -- and with it the copy registry that reconcileDeviceState reads -- survives, and its
   *  children are not re-offered to tryLoadStore over already-open backends.
   *
   *  Each device this loads is checked at once (DeviceReconciler.checkStorageDevice), as ScanDevices,
   *  CheckAllDevices and a nudge each want for a device found by their scan.
   *
   *  `scan` is true only for ScanDevices: its loads' transient CRL open failures count towards CrlOpenAttempts, while
   *  the other callers' are retried uncounted (openCrl), so the allowance is one per scan, not one per open.
   */
  def checkForNewDevices(scan: Boolean = false): Unit =
    discover(DeviceDiscovery.Trigger(scan, check = true), startup = false)

  private def discover(trigger: DeviceDiscovery.Trigger, startup: Boolean): Unit =
    if listing then
      relist = Some(relist.fold(trigger)(_ | trigger))
    else
      listing = true
      val job = ctx.deviceIos.hostRoot.submit(JobKind.DeviceListing, None)(
        DiscoveryDisk.listDevicePaths(storageDevicesDir))
      ctx.onControl("device listing", job, discovery = true): result =>
        listing = false
        try applyListing(result, trigger, startup)
        finally
          relist.foreach: t =>
            relist = None
            discover(t, startup = false)

  private def applyListing(result: Try[Seq[os.Path]], trigger: DeviceDiscovery.Trigger, startup: Boolean): Unit =
    var begun = Set[os.Path]()
    // Whatever this throws, the startup gate learns which of its paths are outstanding (none, if it threw first)
    try begun = listAndProbe(result, trigger)
    finally
      if startup then
        startupPaths = startupPaths.map(_ => begun)
      // A path a listing no longer names is decided for the startup gate: it holds no device to wait for
      startupPaths = startupPaths.map(_.filter(listed.contains))
      checkStartup()

  private def listAndProbe(result: Try[Seq[os.Path]], trigger: DeviceDiscovery.Trigger): Set[os.Path] =
    result match
      case Failure(_: DiscoveryDisk.NotADirectory) =>
        warnOnce(storageDevicesDir, s"Invalid storage devices directory: $storageDevicesDir")
        Set()
      // Listing can fail with an IO error even when isDir just succeeded: warn naming the directory
      case Failure(t) =>
        warnOnce(storageDevicesDir, s"Failed to list storage devices directory: $storageDevicesDir. Error: $t")
        Set()
      case Success(paths) =>
        reportedDirs -= storageDevicesDir
        // A loaded device whose directory is gone is forgotten here, so it is read in full
        // again if it comes back. Its storageDevices entry is untouched: its stores are open.
        listed = paths.toSet
        loadedDirs = loadedDirs.filter((path, _) => listed.contains(path))
        disownedDirs = disownedDirs.filter(listed.contains)
        forgetUnlisted()
        paths.filterNot(loadedDirs.contains).filter(probe(_, trigger, explicit = false)).toSet

  /** Closes the DeviceIo of each device directory gone from the listing that holds no loaded device and has no
   *  discovery job outstanding, once it is idle (DeviceIos.forget); one that is not stays, for the next listing */
  private def forgetUnlisted(): Unit =
    ctx.deviceIos.all.keys
      .filter(p => p / os.up == storageDevicesDir && !listed.contains(p) && !busy.contains(p))
      .filterNot(p => devices.valuesIterator.exists(_.devicePath == p))
      .foreach(ctx.deviceIos.forget(_): Unit)

  private def checkStartup(): Unit =
    startupPaths match
      case Some(paths) if paths.isEmpty =>
        startupPaths = None
        startupDecided()
      case _ => ()

  /** Marks `path` busy with a fresh token, unless a discovery job is outstanding for it */
  private def begin(path: os.Path): Option[Busy] =
    if busy.contains(path) then
      None
    else
      val b = new Busy(ctx.deviceIos.forPath(path))
      busy += path -> b
      Some(b)

  /** Asks for a probe of `path` once its outstanding discovery job has been applied */
  private def again(path: os.Path, trigger: DeviceDiscovery.Trigger): Unit =
    busy.get(path).foreach(b => b.again = Some(b.again.fold(trigger)(_ | trigger)))

  /** Ends `b`'s hold on `path`, if it still holds it, and runs any probe asked for meanwhile */
  private def end(path: os.Path, b: Busy): Unit =
    if busy.get(path).exists(_ eq b) then
      busy -= path
      startupPaths.foreach: paths =>
        startupPaths = Some(paths - path)
        checkStartup()
      b.again.foreach(probe(path, _, explicit = true): Unit)

  /** Runs `job` on `b`'s DeviceIo and applies its result on the control thread, if `b` still holds `path`, the path is
   *  still listed and its DeviceIo is still `b`'s. Otherwise, or on Cancelled or a failure, `abandon` (given the job's
   *  result) undoes what the steps before it, and a job that succeeded, did, and the path is released if `b` holds
   *  it; a failure is warned about once. `apply` ends the hold (`end`) or submits the next step as its last act */
  private def submitFor[T](path: os.Path, b: Busy, kind: JobKind, what: String)(job: => T)
                          (abandon: Try[T] => Unit)(apply: T => Unit): Unit =
    ctx.onControl(what, b.io.submit(kind, None)(job), discovery = true): result =>
      if busy.get(path).exists(_ eq b) then
        val current = listed.contains(path) && ctx.deviceIos.get(path).exists(_ eq b.io)
        result match
          case Success(v) if current =>
            try apply(v)
            catch case t: Throwable =>
              end(path, b)
              warnOnce(path, s"Failed to load storage device found at path $path. Error: $t")
          case Failure(t) if current && t != DeviceIo.Cancelled =>
            abandon(result)
            end(path, b)
            warnOnce(path, s"Failed to load storage device found at path $path. Error: $t")
          case _ =>
            abandon(result)
            end(path, b)
      else
        // Superseded: whatever the job opened is still undone
        abandon(result)

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
                               b: Busy,
                               sdCfg: StorageDeviceConfig,
                               previousHost: HostId,
                               fromShutdownMarker: Boolean): Unit =
    if pendingResolutions.contains(storageDevicePath) then
      end(storageDevicePath, b)
    else
      // The stores in the directory are listed by one DeviceOwnership job on its lane (DeviceStateDisk.storesIn), `b`
      // holding the path meanwhile; a listing that fails leaves the device to the next scan (submitFor warns)
      submitFor(storageDevicePath, b, JobKind.DeviceOwnership, "device ownership listing")(
        DeviceStateDisk.storesIn(storageDevicePath))(_ => ()): stores =>
        if pendingResolutions.contains(storageDevicePath) then
          end(storageDevicePath, b)
        else
          ctx.copyRegistry.awaitDevice(storageDevicePath, stores)
          pendingResolutions += storageDevicePath -> stores
          end(storageDevicePath, b)
          // A throw before the Future exists would register no callback, and the path would stay
          // pending -- and the device unresolved -- for the life of the process.
          val lookup =
            try ctx.metadata.lookupStorageDeviceState(sdCfg.storageDeviceId)
            catch case t: Throwable => Future.failed(t)
          lookup.onComplete: result =>
            ctx.post(Host.DeviceOwnershipResolved(storageDevicePath, sdCfg, previousHost, fromShutdownMarker, result))

  /** Runs `job` as one `kind` job on the lane of the device directory `path` (its DeviceIo, created on first sight),
   *  its result given to `apply` as a posted discovery event (DeviceControl.onControl). A submission that throws is
   *  given to `apply` as the failure */
  private def laneJob[T](path: os.Path, kind: JobKind, what: String)(job: => T)(apply: Try[T] => Unit): Unit =
    val io = ctx.deviceIos.forPath(path)
    val handle =
      try Right(io.submit(kind, None)(job))
      catch case t: Throwable => Left(t)
    handle match
      case Left(t) => apply(Failure(t))
      case Right(h) =>
        ctx.onControl(what, h, discovery = true): result =>
          // A DeviceIo forgotten meanwhile (the path left the listing) answers as a cancelled job: nothing happened
          apply(if ctx.deviceIos.get(path).exists(_ eq io) then result else Failure(DeviceIo.Cancelled))

  /** laneJob as a Future of `apply`'s value, completed on the control thread once the result is applied (a throw of
   *  `apply`, or the job's failure, fails it): the adoption transaction's disk callbacks */
  private def laneFuture[T, R](path: os.Path, kind: JobKind, what: String)(job: => T)(apply: T => R): Future[R] =
    val p = Promise[R]()
    laneJob(path, kind, what)(job): result =>
      p.complete(result.map(apply))
    p.future

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
    val Host.DeviceOwnershipResolved(path, sdCfg, previousHost, fromShutdownMarker, _) = e
    // The marks are cleared once the outcome has been acted on, so a store the outcome loads is
    // already marked loading by then; an adoption started here takes them over instead.
    val marked = pendingResolutions.getOrElse(path, Set())
    val deviceId = sdCfg.storageDeviceId
    // The disk was read before the lookup went out. If it has been pulled, or the file naming the previous host has
    // changed, since then, this result describes a directory that is no longer there to act on; the next scan starts
    // over from what the disk says now. The recheck is one DeviceOwnership job on the path's lane
    // (DeviceStateDisk.ownershipUnchanged); the path stays pending until it is applied, so no scan resolves it again
    // meanwhile. A stat that fails (FileStat), or a cancelled job, leaves it to the next scan
    laneJob(path, JobKind.DeviceOwnership, "adoption ownership recheck")(
      DeviceStateDisk.ownershipUnchanged(path, previousHost, fromShutdownMarker)): unchanged =>
      pendingResolutions -= path
      try
        unchanged match
          case Failure(t) =>
            if t != DeviceIo.Cancelled then
              warnOnce(path, s"Failed to resolve the ownership of storage device $deviceId at $path. " +
                             s"Will retry on the next scan. Error: $t")
          case Success(false) => ()
          case Success(true) => ownershipResolved(e, marked)
      finally
        releaseMarks(path, marked)

  /** handleOwnershipResolved's table, once the recheck found the directory unchanged */
  private def ownershipResolved(e: Host.DeviceOwnershipResolved, marked: Set[StoreId]): Unit =
    val Host.DeviceOwnershipResolved(path, sdCfg, previousHost, fromShutdownMarker, result) = e
    import StorageDeviceState.DeviceStatus
    val deviceId = sdCfg.storageDeviceId
    try
      if loadedDirs.contains(path) then
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
              // One Markers job, its outcome only logged: the device is not loaded either way
              laneJob(path, JobKind.DeviceOwnership, "adoption failed marker")(DeviceStateDisk.writeFailed(path)):
                _.failed.foreach: t =>
                  if t != DeviceIo.Cancelled then logger.warn(s"Failed to write the failed marker of $path: $t")
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
                  loadResolved(path, sdCfg)
                case DeviceStatus.Operational if ownedHere =>
                  logger.info(s"Storage device $deviceId at $path has been restarted. Loading it")
                  bringIntoService(path, sdCfg, state, rewriteOwner = false, marked)
                case _ =>
                  refuse("Not loading it")
            else if state.hostId == previousHost then
              tryAdoptDevice(path, sdCfg, state, previousHost, marked)
            else if ownedHere then
              // Shutdown or ShuttingDown too: a shutdown requested before the load. The load's
              // reconcile carries it on.
              logger.info(s"Storage device $deviceId at $path was adopted by this host but not " +
                          s"yet loaded. Finishing the adoption")
              bringIntoService(path, sdCfg, state, rewriteOwner = true, marked)
            else
              refuse("Not loading or adopting it")
    catch
      case t: Throwable =>
        warnOnce(path, s"Failed to resolve the ownership of storage device $deviceId at $path. " +
                       s"Will retry on the next scan. Error: $t")

  /** Clears the awaiting-device marks of `stores`, unless an adoption of `path` has taken them over */
  private def releaseMarks(path: os.Path, stores: Set[StoreId]): Unit =
    if ! pendingResolutions.contains(path) then
      ctx.copyRegistry.deviceDecided(path)

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
    // is plainly not ready costs no transaction. The check that counts is adoptDevice's own. Its reads are one
    // Adoption job on the device's lane (DeviceStateDisk.adoptionLook); the path stays pending meanwhile
    pendingResolutions += path -> marked
    laneJob(path, JobKind.Adoption, "adoption look")(DeviceStateDisk.adoptionLook(path, state)): look =>
      look match
        case Success(l) if !l.crlPresent || l.unclean.nonEmpty =>
          pendingResolutions -= path
          releaseMarks(path, marked)
          if !l.crlPresent then
            warnOnce(path, s"Storage device $deviceId at $path, last owned by host $previousHostId, holds stores but " +
                           s"its CRL is not on it (${CrashRecoveryLog.deviceCrlDir(path)}). Not adopting it. Return " +
                           s"it to that host")
          else
            warnUnclean(path, deviceId, previousHostId, l.unclean)
        case Success(_) =>
          adopt(path, sdCfg, state, previousHostId, marked)
        case Failure(t) =>
          pendingResolutions -= path
          releaseMarks(path, marked)
          if t != DeviceIo.Cancelled then
            warnOnce(path, s"Failed to resolve the ownership of storage device $deviceId at $path. " +
                           s"Will retry on the next scan. Error: $t")

  /** tryAdoptDevice's transaction (DeviceMetadata.adoptDevice), its disk callbacks each one Adoption job on the
   *  device's lane, applied on the control thread (laneFuture), so the transaction's order of disk steps and metadata
   *  rewrite is as it was: the CrlLost moves made before the commit */
  private def adopt(path: os.Path,
                    sdCfg: StorageDeviceConfig,
                    state: StorageDeviceState,
                    previousHostId: HostId,
                    marked: Set[StoreId]): Unit =
    val deviceId = sdCfg.storageDeviceId
    locally:
      logger.info(s"Adopting storage device $deviceId at $path, ${state.status} and owned by host $previousHostId")
      def handleAdoption(result: Try[AdoptOutcome]): Unit =
        result match
          case Success(AdoptOutcome.Adopted(committed)) =>
            try bringIntoService(path, sdCfg, committed, rewriteOwner = true, marked)
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

      val fadopt =
        try ctx.metadata.adoptDevice(deviceId, previousHostId, path,
                                     (p, s) => servedStoresOnLane(p, s, AdoptionClass.Unclean),
                                     (p, s) => servedStoresOnLane(p, s, AdoptionClass.CrlLost),
                                     moveToLostUnattached(path))
        catch case t: Throwable => Future.failed(t)
      fadopt.onComplete: result =>
        pendingResolutions -= path
        try handleAdoption(result)
        finally releaseMarks(path, marked)

  /** The entries of `state` an adoption serves -- Offline ones, and the Active ones of a device not shut down -- in
   *  adoption class `cls`, by their directories under `path` (DeviceStateDisk.servedStoresIn), read by one Adoption
   *  job on the device's lane. A failed listing or read fails it, failing the adoption for the next scan */
  private def servedStoresOnLane(path: os.Path, state: StorageDeviceState, cls: AdoptionClass): Future[List[StoreId]] =
    laneFuture(path, JobKind.Adoption, "adoption classes")(DeviceStateDisk.servedStoresIn(path, state, cls))(identity)

  /** T10 for an adoption's CrlLost stores on the device at `path`, not attached yet: each copy moved to lost/ unless it
   *  is there, in one Adoption job (DeviceStateDisk.moveToLost). A move that fails fails the Future, and with it the
   *  adoption's transaction for the next scan to retry; one not forced is held for the device's attach
   *  (CopyMoves.unattachedMoved) */
  private def moveToLostUnattached(path: os.Path)(stores: List[StoreId]): Future[Unit] =
    if stores.isEmpty then
      Future.unit
    else
      val sync = ctx.moves.syncDirectory
      val rename = ctx.moves.rename
      laneFuture(path, JobKind.Adoption, "adoption moves to lost")(
        DeviceStateDisk.moveToLost(path, stores, sync, rename)): moved =>
        moved.foreach(ctx.moves.unattachedMoved)
        moved.map(_.result).collectFirst { case MoveResult.NotMoved(t) => throw t }
        ()

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
   *  4. The device is probed and loaded as a scan loads it, and checked at once, so its Rebuilding and
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
                               rewriteOwner: Boolean,
                               marked: Set[StoreId]): Unit =
    val active = state.stores.collect {
      case (storeId, entry) if entry.status == StorageDeviceState.StoreStatus.Active => storeId -> entry
    }
    // Decided here, from control state: the stores whose outbound/ or parked/ copy may go Live (T7, T5)
    val reinstate = active.collect { case (storeId, entry) if ctx.copyRegistry.mayReinstateOut(entry, storeId) =>
      storeId }.toSet
    val sync = ctx.moves.syncDirectory
    val rename = ctx.moves.rename
    // Steps 1-3 are one Adoption job on the device's lane (DeviceStateDisk.bringIntoService), the path pending (its
    // stores still awaiting their device) until it is applied
    pendingResolutions += path -> marked
    laneJob(path, JobKind.Adoption, "adoption brings into service")(
      DeviceStateDisk.bringIntoService(path, state.stores.keySet, rewriteOwner, ctx.hostId, reinstate, sync, rename,
        removeShutdownState)):
      result =>
        pendingResolutions -= path
        try
          result match
            case Success(DeviceStateDisk.BroughtIn(moved, markerFailure)) =>
              // A move that failed keeps its store offline where it is, until a later check of its device; one not
              // forced leaves the copy unsynced, which its attach takes and the device's check forces and loads.
              // Held whether or not step 3 removed shutdown.state: a later bringIntoService finds the copy in live/
              // and makes no move to report
              moved.foreach(ctx.moves.unattachedMoved)
              markerFailure match
                case None =>
                  announceOnLoad += path -> active.keySet
                  probe(path, DeviceDiscovery.Trigger(scan = false, check = true), explicit = true): Unit
                case Some(t) =>
                  warnOnce(path, s"Failed to bring storage device ${sdCfg.storageDeviceId} at $path into service. " +
                                 s"Will retry on the next scan. Error: $t")
            case Failure(t) =>
              if t != DeviceIo.Cancelled then
                warnOnce(path, s"Failed to bring storage device ${sdCfg.storageDeviceId} at $path into service. " +
                               s"Will retry on the next scan. Error: $t")
        finally
          releaseMarks(path, marked)

  /** Probes the device directory `path` (DiscoveryDisk.probe, one job on its DeviceIo) and decides on the result
   *  (`decide`), unless a discovery job is outstanding for it: then a scan skips it, and an `explicit` request asks for
   *  a probe once that job has been applied. Returns whether a probe began.
   *
   *  Nothing is decided for a path whose unloaded device's CRL is still shutting down
   *  (LocalDevices.closingCrls): a second CRL opened over the same files would corrupt them. The
   *  scan retries it once the shutdown has ended. */
  private def probe(path: os.Path, trigger: DeviceDiscovery.Trigger, explicit: Boolean): Boolean =
    if ctx.devices.closingCrls.contains(path) then
      logger.debug(s"Not loading the device at $path yet: its CRL is still shutting down")
      false
    else
      begin(path) match
        case None =>
          if explicit then again(path, trigger)
          false
        case Some(b) =>
          submitFor(path, b, JobKind.DeviceProbe, "device probe")(DiscoveryDisk.probe(path))(_ => ()): result =>
            decide(path, b, result, trigger)
          true

  /** Loads the device whose probe found `result` at `storageDevicePath` if this host owns it.
   *
   *  The load decision is made from device-local files before anything on the device is
   *  touched -- not even its stale staging directory is removed -- because a device that is
   *  not ours may still be needed exactly as it is by the host that does own it:
   *  - `failed` present: never loaded or claimed. A failed stat of it is Unreadable (DiscoveryDisk.probe): the
   *    device is left for the next scan, never loaded on a false absence.
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
   *  A path loaded meanwhile (by another request's probe) is left as it is. */
  private def decide(storageDevicePath: os.Path, b: Busy, result: ProbeResult,
                     trigger: DeviceDiscovery.Trigger): Unit =
    def done(): Unit = end(storageDevicePath, b)
    result match
      case ProbeResult.NotADevice =>
        done()
      case ProbeResult.Unreadable(t) =>
        done()
        warnOnce(storageDevicePath, s"Failed to load storage device found at path $storageDevicePath. Error: $t")
      case ProbeResult.Failed =>
        done()
        warnOnce(storageDevicePath, s"Storage device at $storageDevicePath has been declared failed. Not loading " +
                                    s"or claiming it")
      case ProbeResult.Device(_, _) if loadedDirs.contains(storageDevicePath) =>
        done()
      case ProbeResult.Device(sdCfg, _) if sdCfg.aspenSystemId != ctx.aspenSystemId =>
        done()
        warnOnce(storageDevicePath,
                 s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
      case ProbeResult.Device(sdCfg, markers) =>
        try
          markers.shutdownState.get match
            case Some(markerHost) =>
              resolveOwnership(storageDevicePath, b, sdCfg, markerHost, fromShutdownMarker = true)
            case None =>
              markers.owningHost.get match
                case Some(owner) if owner == ctx.hostId =>
                  if disownedDirs.contains(storageDevicePath) then
                    done()
                  else
                    loadOwnedDevice(storageDevicePath, b, sdCfg, trigger)
                case Some(owner) =>
                  resolveOwnership(storageDevicePath, b, sdCfg, owner, fromShutdownMarker = false)
                case None =>
                  done()
                  warnOnce(storageDevicePath, s"Storage device ${sdCfg.storageDeviceId} at $storageDevicePath " +
                                              s"has no ${DeviceStateDisk.OwningHostFile} file. Not loading or " +
                                              s"claiming it")
        catch
          case t: Throwable =>
            done()
            warnOnce(storageDevicePath, s"Failed to load storage device found at path $storageDevicePath. Error: $t")

  /** Loads a device of ours that an ownership decision found (handleOwnershipResolved), or asks for a probe of its
   *  directory once its outstanding discovery job has been applied */
  private def loadResolved(path: os.Path, sdCfg: StorageDeviceConfig): Unit =
    val trigger = DeviceDiscovery.Trigger(scan = false, check = true)
    begin(path) match
      case Some(b) => loadOwnedDevice(path, b, sdCfg, trigger)
      case None => again(path, trigger)

  /** Whether a device with `sdCfg`'s id is loaded already. Usually that is just a rescan finding the same directory
   *  again -- a directory that briefly disappeared, since loaded ones are not otherwise read -- which must be a no-op
   *  so the device's state and open backends survive. A different directory claiming the same id -- a duplicate mount
   *  or a copied config file -- would otherwise replace the map entry and orphan the original's already-open stores,
   *  so it is reported and the original kept. */
  private def alreadyLoaded(storageDevicePath: os.Path, sdCfg: StorageDeviceConfig): Boolean =
    devices.get(sdCfg.storageDeviceId) match
      case Some(existing) =>
        if existing.devicePath != storageDevicePath then
          warnOnce(storageDevicePath, s"Storage device ${sdCfg.storageDeviceId} is already loaded from " +
                                      s"${existing.devicePath}; ignoring duplicate at $storageDevicePath")
        else
          loadedDirs += storageDevicePath -> sdCfg.storageDeviceId
        true
      case None => false

  /** The load half of a device decision, run only once the device is known to be ours, `b` holding its path
   *  throughout. Each step that touches the disk is one job on the device's own lane, its result applied on the
   *  control thread after the recheck submitFor makes (and, after each, whether the device id has been loaded from
   *  elsewhere meanwhile):
   *  1. DiscoveryDisk.sweepAtLoad (the scratch areas, nothing can be receiving onto or creating on a device that is not
   *     loaded yet) and the copy listing (P6: a phase directory that cannot be listed, or a phase-param that cannot be
   *     read, leaves the device unloaded, nothing on it moved, for the next scan).
   *  2. The device CRL's open (DeviceCrlDisk.open: recovery, and a lost CRL's moves and replacement, one CrlOpen
   *     job), before any store is offered, so each store recovers from it. Its result is applied by openCrl.
   *  3. P2 (DiscoveryDisk.retireDuplicates), over a fresh listing: a lost CRL's open moved copies to lost/ since the
   *     first one. Then, on the control thread, its copies are registered (HostCopies.attach) and its Live copies
   *     offered to tryLoadStore, and only then is the device registered, so a throw anywhere leaves it unregistered
   *     and the next scan retries it whole (its CRL shut down, as an unload does, so the retry does not open the files
   *     a second time). That retry re-offers children whose LoadStore events are already queued; tryLoadStore's own
   *     catch absorbs the resulting duplicate-backend failures. */
  private def loadOwnedDevice(storageDevicePath: os.Path,
                              b: Busy,
                              sdCfg: StorageDeviceConfig,
                              trigger: DeviceDiscovery.Trigger): Unit =
    val sdCfgPath = storageDevicePath / StorageDeviceConfig.configFilename
    if alreadyLoaded(storageDevicePath, sdCfg) then
      end(storageDevicePath, b)
    else
      submitFor(storageDevicePath, b, JobKind.LoadSweep, "device load sweep")(
        DiscoveryDisk.sweepAndList(storageDevicePath))(_ => ()):
        case _ if alreadyLoaded(storageDevicePath, sdCfg) =>
          end(storageDevicePath, b)
        case Failure(t) =>
          end(storageDevicePath, b)
          warnOnce(storageDevicePath, s"Failed to list the copies of storage device $storageDevicePath. " +
                                      s"Will retry on the next scan. Error: $t")
        case Success(listing) =>
          openCrl(storageDevicePath, b, sdCfgPath, sdCfg, listing, trigger)

  /** Step 2 of loadOwnedDevice: the device CRL's open, one CrlOpen job on the device's lane (DeviceCrlDisk.open), and
   *  the decisions on its result, made here once submitFor's recheck has passed. A result for a path no longer held,
   *  listed or on the same DeviceIo shuts the CRL it opened, as an unload does, and changes nothing else.
   *  A transient open failure on a device holding stores counts towards CrlOpenAttempts only on a device scan
   *  (`trigger.scan`); the open declares it lost (`transientIsLost`) when this scan would be the CrlOpenAttempts-th in
   *  a row. Anything else that declares a CRL lost is DeviceCrlDisk.open's: slowness never does (no timeout on it) */
  private def openCrl(storageDevicePath: os.Path,
                      b: Busy,
                      sdCfgPath: os.Path,
                      sdCfg: StorageDeviceConfig,
                      listing: CopyLayout.Listing,
                      trigger: DeviceDiscovery.Trigger): Unit =
    val counted = trigger.scan
    val transientIsLost =
      counted && transientCrlFailures.getOrElse(storageDevicePath, 0) + 1 >= DeviceDiscovery.CrlOpenAttempts
    val factory = ctx.crlFactory
    val earlierUnforced = ctx.moves.unforcedUnder(storageDevicePath)
    val sync = ctx.moves.syncDirectory
    val rename = ctx.moves.rename
    def shutDown(crl: CrashRecoveryLog): Unit = ctx.shutdown.shutDownCrl(storageDevicePath, crl)
    submitFor(storageDevicePath, b, JobKind.CrlOpen, "device CRL open")(
      DeviceCrlDisk.open(storageDevicePath, sdCfg, listing, factory, transientIsLost, earlierUnforced, sync, rename))(
      _.foreach(_.openedCrl.foreach(shutDown))):
      case CrlOpenResult.Failed(t, how, unsynced) =>
        // Held for the attach, as an unsynced move of a device being loaded is (HostCopies.unsynced)
        unsynced.foreach: (from, to) =>
          ctx.moves.heldUnforced(from, to)
          ctx.copyRegistry.unsynced(DeviceCrlDisk.LostSite, to)
        crlOpenFailed(storageDevicePath, t, how, counted)
        end(storageDevicePath, b)
        warnOnce(storageDevicePath, s"Failed to load storage device found at path $storageDevicePath. Error: $t")
      case opened =>
        transientCrlFailures -= storageDevicePath
        val crl = opened.openedCrl.get
        opened match
          case CrlOpenResult.Lost(_, _, forced) => ctx.moves.forcedUnattached(forced)
          case _ => ()
        if alreadyLoaded(storageDevicePath, sdCfg) then
          shutDown(crl)
          end(storageDevicePath, b)
        else
          try attachOpened(storageDevicePath, b, sdCfgPath, sdCfg, crl, trigger)
          catch case t: Throwable =>
            shutDown(crl)
            throw t

  /** The count and log of a CRL open that failed (CrlOpenResult.Failed). A CRL another writer holds open (CrlLocked)
   *  is in use, not lost: its count is reset, and the next scan retries. A sizing mismatch and a config that cannot
   *  size a new CRL are refusals, logged once at error, until an operator repairs the directory or the config */
  private def crlOpenFailed(devicePath: os.Path, t: Throwable, how: CrlOpenFailure, counted: Boolean): Unit =
    how match
      case CrlOpenFailure.Refused =>
        transientCrlFailures -= devicePath
        t match
          case _: CrlSizingMismatch =>
            if !reportedDirs.contains(devicePath) then
              reportedDirs += devicePath
              logger.error(s"Refusing to load storage device $devicePath: its CRL does not match the sizing it was " +
                           s"created with, or holds stray .crl.tmp files. Its stores are not declared lost. Restore " +
                           s"the CRL directory's stream files and sizing file (removing any stray .crl.tmp), then " +
                           s"the next scan loads it: $t")
          case _: CrlConfigInvalid =>
            // The directory holds no CRL yet and the config cannot size one. Nothing was created, so the next scan
            // after the config is fixed opens it
            if !reportedDirs.contains(devicePath) then
              reportedDirs += devicePath
              logger.error(s"Refusing to load storage device $devicePath: its config cannot size a new CRL. Its " +
                           s"stores are not declared lost. Fix the device config, then the next scan loads it: $t")
          case _ => ()
      case CrlOpenFailure.AfterLost =>
        transientCrlFailures -= devicePath
      case CrlOpenFailure.Undecided => ()
      case CrlOpenFailure.Transient =>
        if !counted then
          logger.error(s"Failed to open the CRL of storage device $devicePath outside a device scan. " +
                       s"Retrying on the next scan: $t")
        else
          val attempts = transientCrlFailures.getOrElse(devicePath, 0) + 1
          transientCrlFailures += devicePath -> attempts
          logger.error(s"Failed to open the CRL of storage device $devicePath (scan $attempts of " +
                       s"${DeviceDiscovery.CrlOpenAttempts}). Retrying on the next scan: $t")

  /** Step 3 of loadOwnedDevice, over the CRL step 2 opened */
  private def attachOpened(storageDevicePath: os.Path,
                           b: Busy,
                           sdCfgPath: os.Path,
                           sdCfg: StorageDeviceConfig,
                           crl: CrashRecoveryLog,
                           trigger: DeviceDiscovery.Trigger): Unit =
    val sds = new LocalStorageDeviceState(sdCfg.storageDeviceId, storageDevicePath, sdCfgPath, crl, b.io,
      new CrlAdmission(crl.admissionLimitBytes))
    // A growth raises the limit the stores admit against; registered before any store loads
    crl.onAdmissionLimitRaised(sds.admission.raiseLimit)
    // Everything the CRL holds is counted, retained for its store, before any store loads: a store that loads
    // counts its own afresh, and the entries of one that does not are copied forward all the same
    // (crash-recovery-log 6.1)
    crl.recoveredBytesByStore.foreach((storeId, bytes) => sds.admission.chargeRetained(storeId, bytes))
    logger.info(s"Loading storage device $storageDevicePath. StorageDeviceId ${sds.storageDeviceId}")
    // Shut down as an unload does, so the retry does not open the files a second time
    def abandon(): Unit =
      // Not the registry of a device with this id loaded from another directory meanwhile
      if !devices.contains(sds.storageDeviceId) then
        ctx.copyRegistry.detach(sds.storageDeviceId, Set())
      ctx.shutdown.shutDownCrl(storageDevicePath, crl)
    val sync = ctx.moves.syncDirectory
    val rename = ctx.moves.rename
    submitFor(storageDevicePath, b, JobKind.LoadDuplicates, "device load P2")(
      DiscoveryDisk.retireDuplicates(storageDevicePath, sync, rename))(_ => abandon()): dups =>
      // Held for the attach, as CopyMoves.moveUnattached holds an unsynced move of a device being loaded
      dups.unsynced.foreach: (from, to) =>
        ctx.moves.heldUnforced(from, to)
        ctx.copyRegistry.unsynced(DiscoveryDisk.DuplicatesSite, to)
      val attached =
        try
          if alreadyLoaded(storageDevicePath, sdCfg) then
            abandon()
            false
          else
            // Its copies registered before any store is offered (HostCopies)
            val copies = dups.outcome.get
            ctx.copyRegistry.attach(sds, copies)
            // Only Live copies are offered to the loader: every other phase is its pass's
            copies.copies.filter(_.phase == Phase.Live).foreach: found =>
              ctx.tryLoadStore(sds, found.path, None)
            true
        catch
          case t: Throwable =>
            abandon()
            throw t
      if attached then
        ctx.devices.devices += sdCfg.storageDeviceId -> sds
        // Its health probe runs while the device stays loaded here (DeviceShutdown.unloadDevice disarms it)
        sds.io.probe.arm()
        ctx.publishView()
        loadedDirs += storageDevicePath -> sdCfg.storageDeviceId
        reportedDirs -= storageDevicePath
        // On the control context. Ignored unless sds is still the device loaded (DeviceShutdown.crlFailed), so
        // registered only once it is: a failure meanwhile is acted on now
        crl.failed.foreach(t => ctx.shutdown.crlFailed(sds, t))
        announceOnLoad.get(storageDevicePath).foreach: stores =>
          announceOnLoad -= storageDevicePath
          stores.toList.sortBy(s => (s.poolId.uuid, s.poolIndex)).foreach(ctx.gossipStoreOnline)
      end(storageDevicePath, b)
      if attached && trigger.check then
        // The startup scan's check waits for the threading's start, as its events do
        if trigger.startup then ctx.post(Host.CheckLoadedDevice(sdCfg.storageDeviceId))
        else ctx.reconciler.checkStorageDevice(sdCfg.storageDeviceId)
