package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DeviceMarkers, FileStat, StorageDeviceConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.store.backend.StoreCreation
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import scala.util.Try

/** What one probe of a directory under `storage-devices/` found (DiscoveryDisk.probe) */
enum ProbeResult:
  /** Not a directory, or no device config in it (known absent) */
  case NotADevice
  /** It carries the `failed` marker: never loaded or claimed. Its config is not read */
  case Failed
  /** A stat of the directory, its config or its `failed` marker failed, or the config could not be read or parsed: no
   *  decision, the next scan probes it again */
  case Unreadable(err: Throwable)
  case Device(cfg: StorageDeviceConfig, markers: DeviceMarkers.State)

/** Device discovery's disk work (DeviceDiscovery), as plain functions of paths. Each runs as one job on a DeviceIo's
 *  management lane: the listing on the host root's lane (`DeviceIos.hostRoot`), everything else on its device path's.
 *  They touch no control state; their results are applied on the control thread. */
object DiscoveryDisk extends Logging:
  /** `storage-devices/` is not a directory, or its stat failed: nothing under it loads, as with an absent one */
  final case class NotADirectory(dir: os.Path) extends Exception(s"$dir is not a directory")

  /** The children of `storageDevicesDir`. Throws NotADirectory, or the listing's own error. */
  def listDevicePaths(storageDevicesDir: os.Path): Seq[os.Path] =
    // A stat that fails loads nothing, as an absent directory does
    if !Try(FileStat.isDir(storageDevicesDir)).getOrElse(false) then
      throw NotADirectory(storageDevicesDir)
    os.list(storageDevicesDir)

  /** Whether `path` holds a device, and if so its config and host-id markers, in the order the load decision reads them.
   *  A stat that fails is Unreadable, never an absence: of the failed marker above all, which would serve a tombstoned
   *  device's stores (rebuild-process B19) */
  def probe(path: os.Path): ProbeResult =
    try
      if !(FileStat.isDir(path) && FileStat.exists(path / StorageDeviceConfig.configFilename)) then
        ProbeResult.NotADevice
      else if DeviceMarkers.isFailed(path) then
        ProbeResult.Failed
      else
        val cfg = StorageDeviceConfig.loadStorageDeviceConfig(path / StorageDeviceConfig.configFilename)
        ProbeResult.Device(cfg, DeviceMarkers.read(path))
    catch case t: Throwable => ProbeResult.Unreadable(t)

  /** The scratch areas of a device being loaded, which nothing can be using yet: the transfer staging directory (a
   *  copy that had finished arriving was already moved to its final path), `creating/` (a complete store was already
   *  renamed out of it, StoreCreation), `deleting/` (an interrupted delete, CopyDisk.delete) and `crl.creating` (an
   *  unfinished replacement of a lost CRL, which holds no entry). A removal that fails is logged and left */
  def sweepAtLoad(devicePath: os.Path): Unit =
    def remove(dir: os.Path, what: String): Unit =
      try os.remove.all(dir)
      catch case t: Throwable => logger.warn(s"Failed to remove $what $dir: $t")
    remove(devicePath / StoreTransfer.StagingDirectory, "stale transfer staging directory")
    remove(devicePath / StoreCreation.CreatingDirectory, "stale store-creation directory")
    CopyDisk.sweepDeleting(devicePath)
    remove(CrashRecoveryLog.replacementCrlDir(devicePath), "the unfinished replacement CRL directory")

  /** retireDuplicates' outcome: the listing after its moves, or the error that stopped them, and every move whose
   *  rename landed unforced, (from, to) in order, for the caller to hold for the resync (CopyMoves.heldUnforced,
   *  HostCopies.unsynced) whatever the outcome */
  final case class Duplicates(outcome: Try[CopyLayout.Listing], unsynced: List[(os.Path, os.Path)])

  val DuplicatesSite = "duplicate copies at load"

  /** P2 (spec I1, every phase), on the device at `devicePath` being loaded: a store with more than one copy outside
   *  pending-delete/ is logged at error, and every one of its copies is moved to lost/ (T10); the one with the least
   *  copy id stays there, and the others are moved on into pending-delete/ whole. A move that fails stops the rest
   *  (Duplicates.outcome a Failure). Moves go through `syncDirectory` and `rename`, CopyMoves' seams. The listing is
   *  taken here, and again after any move */
  def retireDuplicates(devicePath: os.Path, syncDirectory: os.Path => Unit,
                       rename: (os.Path, os.Path) => Unit): Duplicates =
    var unsynced = List[(os.Path, os.Path)]()
    def move(from: os.Path, storeId: StoreId, copyId: java.util.UUID, to: Phase): os.Path =
      val target = CopyLayout.path(devicePath, storeId, copyId, to)
      CopyDisk.relocate(from, target, to, syncDirectory, rename) match
        case MoveResult.Durable =>
          logger.info(s"Moved the copy at $from to $target ($DuplicatesSite)")
        case MoveResult.Unsynced(t) =>
          logger.error(s"Moved the copy at $from to $target ($DuplicatesSite), but the move could not be forced. The " +
                       s"copy stays unsynced until a later check of its device forces it. Error: $t")
          unsynced :+= (from -> target)
        case MoveResult.NotMoved(t) =>
          logger.error(s"Failed to move the copy at $from to $target ($DuplicatesSite). It stays where it is. Error: $t")
          throw t
      target
    val outcome = Try:
      val listing = CopyLayout.list(devicePath)
      val duplicated = listing.copies.filter(_.phase != Phase.Retired).groupBy(_.storeId).filter(_._2.size > 1)
      if duplicated.isEmpty then
        listing
      else
        duplicated.toList.sortBy((s, _) => (s.poolId.uuid, s.poolIndex)).foreach: (storeId, found) =>
          logger.error(s"Storage device $devicePath holds ${found.size} copies of store $storeId " +
                       s"(${found.map(f => s"${f.phase} at ${f.path}").mkString(", ")}). Retiring every one; the " +
                       "store is rebuilt")
          val ordered = found.sortBy(_.copyId)
          val lost = ordered.map: f =>
            if f.phase == Phase.Lost then f.path
            else move(f.path, storeId, f.copyId, Phase.Lost)
          ordered.zip(lost).drop(1).foreach: (f, path) =>
            move(path, storeId, f.copyId, Phase.Retired)
        CopyLayout.list(devicePath)
    Duplicates(outcome, unsynced)
