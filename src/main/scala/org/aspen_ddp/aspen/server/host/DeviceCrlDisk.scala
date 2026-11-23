package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, CrlConfigInvalid, CrlCorrupt,
  CrlLocked, CrlMissing, CrlSizingMismatch}
import org.aspen_ddp.aspen.server.store.backend.Backend
import scribe.Logging

import java.util.UUID
import scala.util.control.NonFatal

/** How a device CRL open that failed (CrlOpenResult.Failed) counts towards DeviceDiscovery.CrlOpenAttempts */
enum CrlOpenFailure:
  /** CrlLocked, CrlSizingMismatch or CrlConfigInvalid: refused, never lost. The count is reset */
  case Refused
  /** A transient open failure (DeviceDiscovery.isTransient) on a device holding stores, not declared lost: a device
   *  scan counts it */
  case Transient
  /** Failed before anything was decided (a stat, or any open failure on a device holding no stores): the count is
   *  left as it is */
  case Undecided
  /** The CRL was declared lost and a later step (a save file read, a move, the replacement's creation) failed: the
   *  count is reset, and the next load decides again */
  case AfterLost

/** What one device CRL open (DeviceCrlDisk.open) did */
enum CrlOpenResult:
  case Opened(crl: CrashRecoveryLog)
  /** The CRL was lost: the copies that did not hold their own state were moved to lost/, the old directory kept
   *  aside at `movedTo` (None when there was no crl/), and a fresh CRL created and opened. `forced` are the lost/
   *  copies forced now: their earlier unforced moves' directories are done with (CopyMoves.forcedUnattached) */
  case Lost(movedTo: Option[os.Path], crl: CrashRecoveryLog, forced: List[os.Path])
  /** Nothing was opened. `unsynced` are the moves to lost/ whose rename landed unforced, (from, to), to be held for
   *  the resync as any unsynced move of a device being loaded is */
  case Failed(err: Throwable, how: CrlOpenFailure, unsynced: List[(os.Path, os.Path)])

  def openedCrl: Option[CrashRecoveryLog] = this match
    case Opened(c) => Some(c)
    case Lost(_, c, _) => Some(c)
    case Failed(_, _, _) => None

/** The device CRL's open at a device's load (DeviceDiscovery.loadOwnedDevice), recovery and the whole lost-CRL path
 *  included, as one job on the device's management lane. It touches no control state: what the control thread
 *  holds that it needs comes in as values, and what it decided goes back in its CrlOpenResult. */
object DeviceCrlDisk extends Logging:
  val LostSite = "lost device CRL"

  /** Opens the CRL of the device at `devicePath`, whose copies `listing` names. It is lost when its directory is
   *  absent from a device that holds stores, or holds no stream files there (CrlMissing: stores exist only on a
   *  device whose CRL was opened), or when opening it throws anything but a transient error
   *  (DeviceDiscovery.isTransient), or a transient error when `transientIsLost` (the caller's count of device scans in
   *  a row reached CrlOpenAttempts). The stores' transaction state was in it, so each is moved to lost/ (the
   *  reconcile pass turns them into rebuilds), except one that held nothing in it (holdsOwnState). Every copy in
   *  lost/ is then forced (with its directories in `earlierUnforced`, an earlier unforced move's), the old directory
   *  kept aside as `crl.lost-<uuid>`, and a fresh CRL created in `crl.creating` (CrashRecoveryLog.replacementCrlDir),
   *  renamed to `crl/` durably once finished, and opened. A failed creation removes `crl.creating`, leaving no
   *  `crl/`. A corrupt CRL (CrlCorrupt) is lost too, logged under its own cause.
   *  Never lost: a CRL another writer holds open (CrlLocked), one whose files do not match the sizing it recorded or
   *  hold a `.crl.tmp` that is no unfinished creation (CrlSizingMismatch: a hand edit or damage, never by itself
   *  proof that the state is gone), and a CRL directory holding no CRL yet whose device config cannot size one
   *  (CrlConfigInvalid). Each is Failed, Refused. A device without stores simply gets its CRL created.
   *
   *  Moves go through `syncDirectory` and `rename`, CopyMoves' seams. */
  def open(devicePath: os.Path,
           cfg: StorageDeviceConfig,
           listing: CopyLayout.Listing,
           factory: CrashRecoveryLogFactory,
           transientIsLost: Boolean,
           earlierUnforced: Map[os.Path, List[os.Path]],
           syncDirectory: os.Path => Unit,
           rename: (os.Path, os.Path) => Unit): CrlOpenResult =
    val crlDir = CrashRecoveryLog.deviceCrlDir(devicePath)
    try
      // Stats that fail are no absence (FileStat). A copy whose config cannot be stat'd may be a store, so it counts
      // as one (CopyLayout.list): missing it could take an emptied crl/ for a new CRL. A failed stat of crl/ itself
      // throws, leaving the device unloaded for the next scan, rather than declare a crl/ that is there lost. Retired
      // copies keep their CRL state in their own files
      val storeDirs = listing.copies.filter(_.phase != Phase.Retired)
        .filter(f => FileStat.existence(f.path / StoreConfig.configFilename).forall(identity))
      if storeDirs.nonEmpty && !FileStat.exists(crlDir) then
        logger.error(s"Storage device $devicePath holds stores but has no CRL directory. Its stores must be rebuilt")
        replaceLost(devicePath, cfg, storeDirs, factory, earlierUnforced, syncDirectory, rename)
      else
        try CrlOpenResult.Opened(factory.openCRL(devicePath, cfg, holdsStores = storeDirs.nonEmpty))
        catch
          case t @ (_: CrlLocked | _: CrlSizingMismatch | _: CrlConfigInvalid) =>
            CrlOpenResult.Failed(t, CrlOpenFailure.Refused, Nil)
          case t: Throwable if storeDirs.isEmpty =>
            CrlOpenResult.Failed(t, CrlOpenFailure.Undecided, Nil)
          case t: Throwable if DeviceDiscovery.isTransient(t) && !transientIsLost =>
            CrlOpenResult.Failed(t, CrlOpenFailure.Transient, Nil)
          case t: Throwable =>
            t match
              case _: CrlCorrupt =>
                logger.error(s"The CRL of storage device $devicePath is corrupt: an entry fails its checksums. Its " +
                             s"stores must be rebuilt: $t")
              case _: CrlMissing =>
                logger.error(s"Storage device $devicePath holds stores but its CRL directory has no stream files. " +
                             s"Its stores must be rebuilt")
              case _ =>
                logger.error(s"Failed to recover the CRL of storage device $devicePath. Its stores must be rebuilt: $t")
            replaceLost(devicePath, cfg, storeDirs, factory, earlierUnforced, syncDirectory, rename)
    catch case t: Throwable => CrlOpenResult.Failed(t, CrlOpenFailure.Undecided, Nil)

  /** The lost-CRL path of `open`: its moves, the old directory kept aside, the replacement created and opened */
  private def replaceLost(devicePath: os.Path,
                          cfg: StorageDeviceConfig,
                          storeDirs: List[CopyLayout.Found],
                          factory: CrashRecoveryLogFactory,
                          earlierUnforced: Map[os.Path, List[os.Path]],
                          syncDirectory: os.Path => Unit,
                          rename: (os.Path, os.Path) => Unit): CrlOpenResult =
    val crlDir = CrashRecoveryLog.deviceCrlDir(devicePath)
    var unsynced = List[(os.Path, os.Path)]()
    try
      // Decided for every store before any is moved: a read that fails leaves all of them as they are. A copy already
      // in lost/ stays there
      val toMove =
        try storeDirs.filter(_.phase != Phase.Lost).filterNot(holdsOwnState)
        catch case t: Throwable =>
          logger.error(s"The CRL of storage device $devicePath is lost, but a store's save file or config could not " +
                       s"be read to tell whether it holds its own state. Retrying on the next scan: $t")
          throw t
      // T10: into lost/. A move that fails, or lands unforced, throws, leaving the device unloaded (and its CRL where
      // it is) for the next scan, which decides again: a crash undoing an unforced move would put the copy back in
      // live/ over the empty replacement CRL. Then every copy in lost/, moved now or by an earlier scan whose move
      // landed unforced, is forced before the CRL is replaced
      val lostCopies = toMove.map: found =>
        val target = CopyLayout.path(devicePath, found.storeId, found.copyId, Phase.Lost)
        CopyDisk.relocate(found.path, target, Phase.Lost, syncDirectory, rename) match
          case MoveResult.Durable =>
            logger.info(s"Moved the copy at ${found.path} to $target ($LostSite)")
            target
          case MoveResult.Unsynced(t) =>
            logger.error(s"Moved the copy at ${found.path} to $target ($LostSite), but the move could not be forced. " +
                         s"The copy stays unsynced until a later check of its device forces it. Error: $t")
            unsynced :+= (found.path -> target)
            throw t
          case MoveResult.NotMoved(t) =>
            logger.error(s"Failed to move the copy at ${found.path} to $target ($LostSite). It stays where it is. " +
                         s"Error: $t")
            throw t
      val forced = storeDirs.filter(_.phase == Phase.Lost).map(_.path) ++ lostCopies
      // Every phase directory a copy may have left or entered (all but pending-delete/), then each lost/ copy with
      // the directories an earlier unforced move of it left. A sync that fails throws
      List(CopyLayout.LiveDirectory, CopyLayout.ArrivedDirectory, CopyLayout.OutboundDirectory,
           CopyLayout.RebuiltDirectory, CopyLayout.ParkedDirectory, CopyLayout.LostDirectory)
        .map(devicePath / _).filter(FileStat.exists(_)).foreach(syncDirectory)
      forced.foreach(path => (earlierUnforced.getOrElse(path, Nil) :+ path).foreach(syncDirectory))
      // A stat that fails throws (FileStat), leaving the device unloaded for the next scan, rather than leave a crl/
      // in the replacement's way
      val movedTo =
        if FileStat.exists(crlDir) then
          val aside = devicePath / s"crl.lost-${UUID.randomUUID()}"
          DurableFiles.move(crlDir, aside)
          Some(aside)
        else None
      // A fresh CRL, which the copies now in lost/ recover nothing from. Created apart and renamed into place only
      // once finished, so a crash or a failure (ENOSPC while preallocating) leaves no crl/ beside the stores: the next
      // load declares the CRL lost again, moving nothing aside, and creates it afresh
      val replacement = CrashRecoveryLog.replacementCrlDir(devicePath)
      try
        os.remove.all(replacement)
        os.makeDir(replacement)
        factory.createCRL(devicePath, cfg, replacement)
        DurableFiles.move(replacement, crlDir)
      catch case t: Throwable =>
        try os.remove.all(replacement)
        catch case r: Throwable => t.addSuppressed(r)
        logger.error(s"Failed to create the CRL replacing the lost CRL of storage device $devicePath. Retrying on " +
                     s"the next scan: $t")
        throw t
      CrlOpenResult.Lost(movedTo, factory.openCRL(devicePath, cfg, holdsStores = true), forced)
    catch case t: Throwable => CrlOpenResult.Failed(t, CrlOpenFailure.AfterLost, unsynced)

  /** Whether the copy `copy` holds nothing in the device CRL, so losing that CRL loses nothing of its.
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
  def holdsOwnState(copy: CopyLayout.Found): Boolean =
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
