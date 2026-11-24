package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DeviceMarkers, FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.store.backend.Backend
import scribe.Logging

import scala.util.control.NonFatal

/** The disk work of a device's state decisions: its markers (DeviceShutdown, DeviceDiscovery: `failed`,
 *  `shutdown.state`, `owning-host`, each DurableFiles-written, so durable before the job ends), ownership
 *  resolution's reads and adoption's (DeviceDiscovery). Plain functions of paths: each runs as one job on the
 *  device's management lane, its result applied on the control thread. They touch no control state. */
object DeviceStateDisk extends Logging:

  /** The marker naming the host that shut a device down (DeviceMarkers) */
  val ShutdownStateFile: String = DeviceMarkers.ShutdownStateFile
  val OwningHostFile: String = DeviceMarkers.OwningHostFile

  /** The durable `failed` marker */
  def writeFailed(devicePath: os.Path): Unit = DeviceMarkers.writeFailed(devicePath)

  /** `shutdown.state` naming `hostId`, durably */
  def writeShutdownState(devicePath: os.Path, hostId: HostId): Unit =
    DeviceMarkers.writeShutdownState(devicePath, hostId)

  /** `shutdown.state` removed, durably */
  def removeShutdownState(devicePath: os.Path): Unit = DeviceMarkers.removeShutdownState(devicePath)

  /** The stores with a configured copy on the device directory `path` outside pending-delete/, by their copies'
   *  names (CopyLayout.list). A listing or a config stat that fails throws (FileStat): a store it cannot see is never
   *  taken for none */
  def storesIn(path: os.Path): Set[StoreId] =
    CopyLayoutDisk.list(path).copies.filter(f =>
      f.phase != Phase.Retired && FileStat.isFile(f.path / StoreConfig.configFilename)).map(_.storeId).toSet

  /** Whether the device directory `path` still names `previousHost` as the marker the ownership lookup was made for
   *  (handleOwnershipResolved's recheck): `shutdown.state` when `fromShutdownMarker`, else `owning-host` with no
   *  `shutdown.state`. A stat that fails throws (FileStat) */
  def ownershipUnchanged(path: os.Path, previousHost: HostId, fromShutdownMarker: Boolean): Boolean =
    FileStat.isDir(path) && (
      if fromShutdownMarker then DeviceMarkers.readShutdownState(path).contains(previousHost)
      else
        !FileStat.exists(path / DeviceMarkers.ShutdownStateFile) &&
          DeviceMarkers.readOwningHost(path).contains(previousHost))

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
   *  A save file that cannot be read throws, failing the adoption for the next scan to retry. Disk work: called only
   *  inside jobs on the device's lane
   */
  def adoptionClass(storeId: StoreId, copy: Option[CopyLayout.Found]): AdoptionClass =
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

  /** The entries of `state` an adoption serves (Offline ones, and the Active ones of a device not shut down) in
   *  adoption class `cls`, by their copies under `path`. A failed listing throws */
  def servedStoresIn(path: os.Path, state: StorageDeviceState, cls: AdoptionClass): List[StoreId] =
    val copies = CopyLayoutDisk.list(path).copies.filter(_.phase != Phase.Retired)
    state.stores.collect {
      case (storeId, entry) if (entry.status == StorageDeviceState.StoreStatus.Offline ||
                                entry.status == StorageDeviceState.StoreStatus.Active) &&
                               adoptionClass(storeId, copies.find(_.storeId == storeId)) == cls =>
        storeId
    }.toList.sortBy(s => (s.poolId.uuid, s.poolIndex))

  /** tryAdoptDevice's look before its transaction: the stores the device holds (storesIn), whether its CRL directory
   *  is there, and the unclean entries of `state` (servedStoresIn). Throws a failed listing, stat or read */
  final case class AdoptionLook(holdsStores: Boolean, crlPresent: Boolean, unclean: List[StoreId])

  def adoptionLook(path: os.Path, state: StorageDeviceState): AdoptionLook =
    val holds = storesIn(path).nonEmpty
    AdoptionLook(holds, !holds || FileStat.isDir(CrashRecoveryLog.deviceCrlDir(path)),
      servedStoresIn(path, state, AdoptionClass.Unclean))

  /** One move a job made: its site, from, target and result (CopyMoves logs and holds it on the control thread) */
  final case class Moved(site: String, from: os.Path, target: os.Path, result: MoveResult)

  /** T10 for an adoption's CrlLost stores on the device at `path`, not attached yet: each copy moved to lost/ unless it
   *  is there (CopyDisk.relocate through CopyMoves' seams). The moves made so far are returned with the first that
   *  failed to move, which stops the rest (the adoption's transaction then fails for the next scan) */
  def moveToLost(path: os.Path, stores: List[StoreId], syncDirectory: os.Path => Unit,
                 rename: (os.Path, os.Path) => Unit): List[Moved] =
    val listing = CopyLayoutDisk.list(path).copies
    var moved = List[Moved]()
    val it = stores.iterator
    var failed = false
    while !failed && it.hasNext do
      val storeId = it.next()
      listing.find(f => f.storeId == storeId && f.phase != Phase.Retired) match
        case Some(found) if found.phase != Phase.Lost =>
          val target = CopyLayout.path(path, storeId, found.copyId, Phase.Lost)
          val result = CopyDisk.relocate(found.path, target, Phase.Lost, syncDirectory, rename)
          moved :+= Moved("adoption CrlLost", found.path, target, result)
          failed = result.isInstanceOf[MoveResult.NotMoved]
        case _ => ()
    moved

  /** bringIntoService's disk steps for the device at `path`, in their order, each durable before the next:
   *  1. when `rewriteOwner` (an adoption), each store's arrival (StoreLoader.ArrivedAtFile) in every live/,
   *     outbound/, rebuilt/ or parked/ copy of a store in `stores`, then `owning-host` naming `hostId`;
   *  2. each outbound/ or parked/ copy of a store in `reinstate` moved to Live (T7, T5; CopyDisk.relocate through
   *     CopyMoves' seams): a failed move keeps that store offline where it is, and is only reported;
   *  3. `shutdown.state` removed.
   *  Throws a failed listing, stat, arrival or owner write (nothing after it is done). Returns the moves step 2 made
   *  with step 3's failure, if any: a failed marker removal still returns the moves, so an unsynced one is held for
   *  the device's attach (CopyMoves.unattachedMoved) whatever step 3 did */
  def bringIntoService(path: os.Path, stores: Set[StoreId], rewriteOwner: Boolean, hostId: HostId,
                       reinstate: Set[StoreId], syncDirectory: os.Path => Unit,
                       rename: (os.Path, os.Path) => Unit,
                       removeShutdownState: os.Path => Unit = DeviceMarkers.removeShutdownState): BroughtIn =
    val listing = CopyLayoutDisk.list(path).copies
    if rewriteOwner then
      listing.filter(f => stores.contains(f.storeId) && (f.phase == Phase.Live || mayGoLive(f.phase)))
        .map(_.path).filter(FileStat.isDir(_)).foreach(LoadDisk.writeArrivedAt)
      DeviceMarkers.writeOwningHost(path, hostId)
    val moved = listing.filter(f => reinstate.contains(f.storeId) &&
                                    (f.phase.isInstanceOf[Phase.Outbound] || f.phase == Phase.Parked)).map: found =>
      val target = CopyLayout.path(path, found.storeId, found.copyId, Phase.Live)
      val result =
        try CopyDisk.relocate(found.path, target, Phase.Live, syncDirectory, rename)
        catch case NonFatal(t) => MoveResult.NotMoved(t)
      Moved(s"adoption brings ${found.phase} into service", found.path, target, result)
    val markerFailure =
      try
        removeShutdownState(path)
        None
      catch case NonFatal(t) => Some(t)
    BroughtIn(moved, markerFailure)

  /** bringIntoService's result: the moves step 2 made, and step 3's failure to remove `shutdown.state` */
  final case class BroughtIn(moved: List[Moved], markerFailure: Option[Throwable])

  /** Whether a copy in the phase directory of `phase` may go Live once its device is in service: outbound/ (T7),
   *  rebuilt/ (T3) and parked/ (T5) */
  def mayGoLive(phase: Phase): Boolean = phase match
    case Phase.Outbound(_) | Phase.Rebuilt(_) | Phase.Parked => true
    case _ => false
