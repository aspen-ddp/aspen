package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrlLocked
import org.aspen_ddp.aspen.server.crl.simple.CrlLock
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.fsif.{DeviceJournalFiles, DeviceJournalRecovery, FsifDeviceJournal,
  FsifLayout, FsifRecovery, JournalConfigInvalid, JournalCorrupt, JournalLocked, JournalSizingMismatch, NioFsifFiles}
import scribe.Logging

import java.io.IOException
import java.util.UUID
import scala.util.control.NonFatal

/** What one device-load open of the FSIF device journal (FsifJournalDisk.open) did, mirroring CrlOpenResult */
enum FsifJournalOpenResult:
  /** Recovered and replayed. `lost` are the copies whose own replay failed, each moved alone to lost/ (from, to); a
   *  staging directory whose replay failed was removed instead */
  case Opened(journal: FsifDeviceJournal, lost: List[(os.Path, os.Path)])
  /** No journal and no FSIF directory attached to one: nothing opened, nothing created (spec 4.1) */
  case Absent
  /** The journal was lost (corrupt, or missing under attached directories): every FSIF copy was moved to lost/, the
   *  FSIF staging directories removed, the old directory kept aside at `movedTo` (None when there was none), and a
   *  fresh journal created and opened. `forced` are the lost/ copies forced now: their earlier unforced moves'
   *  directories are done with (CopyMoves.forcedUnattached) */
  case Lost(movedTo: Option[os.Path], journal: FsifDeviceJournal, forced: List[os.Path])
  /** Nothing was opened. `unsynced` are the moves to lost/ whose rename landed unforced, (from, to), to be held for
   *  the resync as CrlOpenResult.Failed's are */
  case Failed(err: Throwable, how: CrlOpenFailure, unsynced: List[(os.Path, os.Path)])

  def openedJournal: Option[FsifDeviceJournal] = this match
    case Opened(j, _) => Some(j)
    case Lost(_, j, _) => Some(j)
    case _ => None

/** The device's FSIF journal on disk (`<device>/fsif-journal`, spec 4.1 and 4.7). Runs only as jobs on the device's
 *  lane: the device load's open (`openAtLoad`, the whole lost-journal path included, modelled step for step on
 *  DeviceCrlDisk.open), and the lazy creation ahead of the first FSIF store on a device that has none (`ensure`). It
 *  touches no control state: what it decided goes back in its result, or into the device's `fsifJournal` slot.
 *
 *  An open journal holds the directory's `lock` (CrlLock), released once its files are closed. */
object FsifJournalDisk extends Logging:
  val LostSite = "lost FSIF device journal"
  /** The site of a copy moved to lost/ alone because its own replay failed */
  val ReplayLostSite = "FSIF journal replay"

  /** An FSIF directory the journal open considers: a copy (`phase` Some) or a rebuild's staging directory (None) */
  private final case class Dir(path: os.Path, phase: Option[Phase], layout: FsifLayout)

  /** The device load's open: a journal still in `io`'s slot (a lane job queued before this load filled it after the
   *  device's unload cleared it) is closed first, then the device's copies are listed afresh (a lost CRL's open may have
   *  moved some since the load's first listing) and the journal opened (`open`). A listing that fails is
   *  Failed(Undecided), nothing moved */
  def openAtLoad(io: DeviceIo, devicePath: os.Path, cfg: StorageDeviceConfig, transientIsLost: Boolean,
                 earlierUnforced: Map[os.Path, List[os.Path]], syncDirectory: os.Path => Unit,
                 rename: (os.Path, os.Path) => Unit): FsifJournalOpenResult =
    io.fsifJournal.getAndSet(None).foreach: j =>
      logger.error(s"An FSIF journal of storage device $devicePath was opened after the device's unload. Closing it")
      j.close()
    try
      open(devicePath, cfg, CopyLayoutDisk.list(devicePath), io.journalRun, transientIsLost, earlierUnforced,
        syncDirectory, rename)
    catch case t: Throwable => FsifJournalOpenResult.Failed(t, CrlOpenFailure.Undecided, Nil)

  /** Opens the FSIF journal of the device at `devicePath`, whose copies `listing` names (spec 4.7), classifying every
   *  failure as DeviceCrlDisk.open does (Ruling R18).
   *
   *  The directories it serves are the FSIF copies in every phase but Lost and Retired, and the FSIF staging
   *  directories under `rebuilding/`, each FSIF by its config (`fsifDirectories`, Ruling R19). Deciding them reads the
   *  journal directory's stat, the configs and, with no journal, the newest indexes; any failure there is
   *  Failed(Undecided), nothing moved, whatever it is. With no journal directory, a device whose every such
   *  directory's newest index is detached is Absent; one with an undetached index (J2: it names the journal that is
   *  gone) has lost its journal. Otherwise the journal is locked (CrlLock), its sizing read, the ring recovered and
   *  replayed into every directory (DeviceJournalRecovery). Those steps' failures: a journal locked elsewhere
   *  (JournalLocked) is Failed(Transient), never lost (most often this host's own journal, closed with a write in
   *  flight, whose lock is released once that write ends), and Failed(Refused) once `transientIsLost` (the caller's
   *  count of device scans in a row reached FsifJournalOpenAttempts); on a device holding no FSIF directory any other
   *  failure replaces the journal through `replaceLost`, nothing to move, logged at warn (Ruling R22: nothing needs
   *  it); one whose files do not match its sizing (JournalSizingMismatch) is Failed(Refused), never lost; a transient
   *  one (DeviceDiscovery.isTransient) is Failed(Transient) unless `transientIsLost`; anything else, a corrupt ring
   *  (JournalCorrupt) logged as such, loses the journal (`replaceLost`). A directory whose replay fails, or whose
   *  undetached index names another journal (damage or a bug), is lost alone: a copy moved to lost/
   *  (CopyDisk.relocate, CopyMoves' seams), a staging directory removed; a move that fails or lands unforced is
   *  Failed(AfterLost). `run` is the journal's writer thread (DeviceIo.journalRun). */
  def open(devicePath: os.Path,
           cfg: StorageDeviceConfig,
           listing: CopyLayout.Listing,
           run: Runnable => Unit,
           transientIsLost: Boolean,
           earlierUnforced: Map[os.Path, List[os.Path]],
           syncDirectory: os.Path => Unit,
           rename: (os.Path, os.Path) => Unit): FsifJournalOpenResult =
    val dir = devicePath / DeviceJournalFiles.DirName
    try
      // Decided before anything else: a failed stat throws (FileStat), never a journal taken for absent that is there
      val present = FileStat.exists(dir)
      val dirs = fsifDirectories(devicePath, listing, journal = present)
      if !present then
        val attached = dirs.filter(d => undetached(d))
        if attached.isEmpty then
          FsifJournalOpenResult.Absent
        else
          logger.error(s"Storage device $devicePath has no FSIF device journal, but FSIF directories attached to one: " +
                       s"${attached.map(_.path).mkString(", ")}. Its FSIF copies must be rebuilt")
          replaceLost(devicePath, cfg, listing, dirs, run, earlierUnforced, syncDirectory, rename)
      else
        try openPresent(devicePath, dir, dirs, run, earlierUnforced, syncDirectory, rename)
        catch
          case t: JournalLocked =>
            // Usually this host's own journal, closed while a write was in flight, whose lock goes with that write.
            // One still held after FsifJournalOpenAttempts scans is another writer's: refused, as a locked CRL is
            val how = if transientIsLost then CrlOpenFailure.Refused else CrlOpenFailure.Transient
            FsifJournalOpenResult.Failed(t, how, Nil)
          case t: Throwable if dirs.isEmpty =>
            // Ruling R22: nothing needs this journal, so one that cannot be opened is replaced, nothing moved
            logger.warn(s"The FSIF device journal of storage device $devicePath cannot be opened, and no FSIF " +
                        s"directory needs it: replacing it. Error: $t")
            replaceLost(devicePath, cfg, listing, dirs, run, earlierUnforced, syncDirectory, rename)
          case t: JournalSizingMismatch =>
            FsifJournalOpenResult.Failed(t, CrlOpenFailure.Refused, Nil)
          case t: Throwable if DeviceDiscovery.isTransient(t) && !transientIsLost =>
            FsifJournalOpenResult.Failed(t, CrlOpenFailure.Transient, Nil)
          case t: Throwable =>
            t match
              case _: JournalCorrupt =>
                logger.error(s"The FSIF device journal of storage device $devicePath is corrupt. Its FSIF copies must " +
                             s"be rebuilt: ${t.getMessage}")
              case _ =>
                logger.error(s"Failed to recover the FSIF device journal of storage device $devicePath. Its FSIF " +
                             s"copies must be rebuilt: $t")
            replaceLost(devicePath, cfg, listing, dirs, run, earlierUnforced, syncDirectory, rename)
    catch case t: Throwable => FsifJournalOpenResult.Failed(t, CrlOpenFailure.Undecided, Nil)

  /** `open`'s steps over a journal directory that is there: lock, sizing, recovery and replay (whose failures `open`
   *  classifies), then each directory whose own replay found it lost taken out alone. The lock is released unless the
   *  journal opened holds it */
  private def openPresent(devicePath: os.Path, dir: os.Path, dirs: List[Dir], run: Runnable => Unit,
                          earlierUnforced: Map[os.Path, List[os.Path]],
                          syncDirectory: os.Path => Unit,
                          rename: (os.Path, os.Path) => Unit): FsifJournalOpenResult =
    val lock =
      try CrlLock.acquire(dir)
      catch case e: CrlLocked => throw new JournalLocked(s"the FSIF device journal $dir is open elsewhere: $e")
    var keep = false
    try
      val sizing = DeviceJournalFiles.readSizing(dir)
      val files = new NioFsifFiles(dir, dsync = true)
      val recovered = DeviceJournalRecovery.recover(files, sizing,
        dirs.map(d => DeviceJournalRecovery.Target(d.path, new NioFsifFiles(d.path), d.layout)))
      val lost = dirs.flatMap: d =>
        recovered.outcomes.get(d.path).collect:
          case DeviceJournalRecovery.Outcome.Lost(e) => d -> e.getMessage
          case DeviceJournalRecovery.Outcome.ForeignUndetached =>
            d -> "its newest index is not detached and names another journal"
      // Decided: a failure from here on is AfterLost, never a lost journal
      moveLostAlone(devicePath, lost, earlierUnforced, syncDirectory, rename) match
        case Left(failure) => failure
        case Right(moved) =>
          val journal = new FsifDeviceJournal(files, sizing, recovered.start, run, forceAfterWrite = false,
            onClosed = () => lock.release())
          keep = true
          FsifJournalOpenResult.Opened(journal, moved)
    finally
      if !keep then lock.release()

  /** The directories `lost` names, whose replay found them lost, each taken out alone: a copy moved to lost/, a
   *  staging directory removed (its rebuild starts over, as RebuildingStore.openStaging treats a lost staging store).
   *  Each moved copy is then forced with the directories an earlier unforced move of it left (`earlierUnforced`, by
   *  its path before or after this move), as replaceLost forces its lost/ copies, so a crash cannot bring it back
   *  where that earlier move took it from. Right with the copies moved (from, to); Left Failed(AfterLost) at the first
   *  move that fails or lands unforced, or a force that fails */
  private def moveLostAlone(devicePath: os.Path, lost: List[(Dir, String)],
                            earlierUnforced: Map[os.Path, List[os.Path]], syncDirectory: os.Path => Unit,
                            rename: (os.Path, os.Path) => Unit): Either[FsifJournalOpenResult, List[(os.Path, os.Path)]] =
    var moved = List[(os.Path, os.Path)]()
    try
      lost.foreach: (d, why) =>
        logger.error(s"The FSIF journal replay into ${d.path} failed. It must be rebuilt: $why")
        d.phase match
          case None =>
            os.remove.all(d.path)
            logger.info(s"Removed the rebuild staging directory ${d.path} ($ReplayLostSite)")
          case Some(_) =>
            val (from, to) = moveToLost(devicePath, d.path, ReplayLostSite, syncDirectory, rename, _ => ())
            (earlierUnforced.getOrElse(from, Nil) ++ earlierUnforced.getOrElse(to, Nil) :+ to).distinct
              .foreach(syncDirectory)
            moved :+= from -> to
      Right(moved)
    catch case t: Throwable => Left(FsifJournalOpenResult.Failed(t, CrlOpenFailure.AfterLost, unsyncedOf(t)))

  /** Carries a move whose rename landed unforced, so the caller's Failed holds it */
  private final class UnsyncedMove(val from: os.Path, val to: os.Path, cause: Throwable)
    extends IOException(s"the move of $from to $to could not be forced: $cause", cause)

  private def unsyncedOf(t: Throwable): List[(os.Path, os.Path)] = t match
    case u: UnsyncedMove => List(u.from -> u.to)
    case _ => Nil

  /** Moves the copy at `from` to lost/ (T10) through CopyDisk.relocate, as DeviceCrlDisk.replaceLost does: the lost/
   *  path once durable; a move that fails throws its error, and one that lands unforced throws UnsyncedMove */
  private def moveToLost(devicePath: os.Path, from: os.Path, site: String, syncDirectory: os.Path => Unit,
                         rename: (os.Path, os.Path) => Unit, unforced: ((os.Path, os.Path)) => Unit): (os.Path, os.Path) =
    val (storeId, copyId) = CopyLayout.parseName(from.last).getOrElse(
      throw new IllegalStateException(s"$from is no copy name"))
    val target = CopyLayout.path(devicePath, storeId, copyId, Phase.Lost)
    CopyDisk.relocate(from, target, Phase.Lost, syncDirectory, rename) match
      case MoveResult.Durable =>
        logger.info(s"Moved the copy at $from to $target ($site)")
        from -> target
      case MoveResult.Unsynced(t) =>
        logger.error(s"Moved the copy at $from to $target ($site), but the move could not be forced. The copy stays " +
                     s"unsynced until a later check of its device forces it. Error: $t")
        unforced(from -> target)
        throw new UnsyncedMove(from, target, t)
      case MoveResult.NotMoved(t) =>
        logger.error(s"Failed to move the copy at $from to $target ($site). It stays where it is. Error: $t")
        throw t

  /** The lost-journal path of `open`, step for step DeviceCrlDisk.replaceLost's: every move decided first (the FSIF
   *  copies, `dirs`, not in lost/), each moved with CopyDisk.relocate (a move that fails or lands unforced fails the
   *  open, the device left unloaded and the journal untouched for the next scan); every phase directory forced, then
   *  each lost/ copy with the directories an earlier unforced move of it left; the FSIF staging directories removed;
   *  the old directory kept aside as `fsif-journal.lost-<uuid>`; a fresh journal created in `fsif-journal.creating`,
   *  renamed into place durably and opened. A device config that cannot size the replacement is Failed, Refused,
   *  before anything is moved. A failure anywhere after is Failed(AfterLost), a partial replacement removed */
  private def replaceLost(devicePath: os.Path,
                          cfg: StorageDeviceConfig,
                          listing: CopyLayout.Listing,
                          dirs: List[Dir],
                          run: Runnable => Unit,
                          earlierUnforced: Map[os.Path, List[os.Path]],
                          syncDirectory: os.Path => Unit,
                          rename: (os.Path, os.Path) => Unit): FsifJournalOpenResult =
    val dir = devicePath / DeviceJournalFiles.DirName
    val sizing =
      try DeviceJournalFiles.sizingFor(cfg)
      catch case t: JournalConfigInvalid =>
        logger.error(s"The FSIF device journal of storage device $devicePath is lost, but its device config cannot size " +
                     s"a replacement. Nothing is moved: fix the device config, then the next scan replaces it: $t")
        return FsifJournalOpenResult.Failed(t, CrlOpenFailure.Refused, Nil)
    var unsynced = List[(os.Path, os.Path)]()
    try
      // Decided before any is moved: the copies (staging directories have no phase), none of them already in lost/
      val toMove = dirs.filter(_.phase.isDefined)
      val staging = dirs.filter(_.phase.isEmpty)
      val lostCopies = toMove.map(d => moveToLost(devicePath, d.path, LostSite, syncDirectory, rename,
        m => unsynced :+= m)._2)
      val forced = listing.copies.filter(_.phase == Phase.Lost).map(_.path) ++ lostCopies
      List(CopyLayout.LiveDirectory, CopyLayout.ArrivedDirectory, CopyLayout.OutboundDirectory,
           CopyLayout.RebuiltDirectory, CopyLayout.ParkedDirectory, CopyLayout.LostDirectory)
        .map(devicePath / _).filter(FileStat.exists(_)).foreach(syncDirectory)
      forced.foreach(path => (earlierUnforced.getOrElse(path, Nil) :+ path).foreach(syncDirectory))
      staging.foreach: d =>
        os.remove.all(d.path)
        logger.info(s"Removed the rebuild staging directory ${d.path} ($LostSite)")
      val movedTo =
        if FileStat.exists(dir) then
          val aside = devicePath / DeviceJournalFiles.lostName(UUID.randomUUID())
          DurableFiles.move(dir, aside)
          Some(aside)
        else None
      try createInPlace(devicePath, sizing)
      catch case t: Throwable =>
        logger.error(s"Failed to create the FSIF device journal replacing the lost journal of storage device " +
                     s"$devicePath. Retrying on the next scan: $t")
        throw t
      FsifJournalOpenResult.Lost(movedTo, openFresh(dir, run), forced)
    catch case t: Throwable => FsifJournalOpenResult.Failed(t, CrlOpenFailure.AfterLost, unsynced)

  /** A fresh journal of `sizing` at `<device>/fsif-journal`: made in `fsif-journal.creating` (any leftover removed
   *  first) and renamed into place durably once finished, so a crash or a failure leaves no partial journal; a
   *  failure removes the scratch directory. Zero-filling the whole ring (1 GiB by default) runs inside the calling lane
   *  job, so the creation's duration is logged */
  private def createInPlace(devicePath: os.Path, sizing: org.aspen_ddp.aspen.server.store.backend.fsif.JournalSizing): Unit =
    val creating = devicePath / DeviceJournalFiles.CreatingName
    try
      val started = System.nanoTime()
      os.remove.all(creating)
      DeviceJournalFiles.create(creating, sizing)
      DurableFiles.move(creating, devicePath / DeviceJournalFiles.DirName)
      logger.info(s"Created the FSIF device journal of storage device $devicePath (${sizing.files} files of " +
                  s"${sizing.fileSize} bytes) in ${(System.nanoTime() - started) / 1000000} ms")
    catch case t: Throwable =>
      try os.remove.all(creating)
      catch case r: Throwable => t.addSuppressed(r)
      throw t

  /** Opens the journal just created at `dir`: nothing to recover, the writer starts a fresh ring */
  private def openFresh(dir: os.Path, run: Runnable => Unit): FsifDeviceJournal =
    val lock =
      try CrlLock.acquire(dir)
      catch case e: CrlLocked => throw new JournalLocked(s"the FSIF device journal $dir is open elsewhere: $e")
    try
      val sizing = DeviceJournalFiles.readSizing(dir)
      new FsifDeviceJournal(new NioFsifFiles(dir, dsync = true), sizing, None, run, forceAfterWrite = false,
        onClosed = () => lock.release())
    catch case t: Throwable =>
      lock.release()
      throw t

  /** The FSIF directories a device-load open serves (Ruling R19). A directory is FSIF by its store config. One whose
   *  config is absent, does not parse or cannot be read is no FSIF directory here only while it holds no FSIF index
   *  file (index.a, index.b): its own open decides it. One holding an FSIF index file with an unusable config may be
   *  attached to a journal holding its acknowledged writes, so its state is unknown: this throws, leaving the device
   *  unloaded for the next scan, journal or not. A directory whose index files cannot even be stat'd (no search
   *  permission) throws too on a device with a journal (`journal`), where leaving out an attached directory would let
   *  its records be overwritten and its later open take it for replayed. On a device without one it is left out,
   *  logged, so a RocksDB copy that cannot be read keeps its device loading (X-W18): an attached FSIF copy there needs
   *  a journal that is already gone as well */
  private def fsifDirectories(devicePath: os.Path, listing: CopyLayout.Listing, journal: Boolean): List[Dir] =
    val copies = listing.copies.filter(c => c.phase != Phase.Lost && c.phase != Phase.Retired).flatMap: c =>
      layoutOf(c.path, journal).map(Dir(c.path, Some(c.phase), _))
    val stagingRoot = devicePath / RebuildingStore.RebuildDirectory
    val staging =
      if !FileStat.isDir(stagingRoot) then Nil
      else
        os.list(stagingRoot).toList.sorted.filter(FileStat.isDir(_)).flatMap: d =>
          layoutOf(d, journal).map(Dir(d, None, _))
    copies ++ staging

  /** `dir`'s FSIF layout, as fsifDirectories decides it */
  private def layoutOf(dir: os.Path, journal: Boolean): Option[FsifLayout] =
    val cfgFile = dir / StoreConfig.configFilename
    val read: Either[Throwable, Option[StoreConfig]] =
      try
        if !FileStat.exists(cfgFile) then Right(None)
        else Right(Some(StoreConfig.loadStoreConfig(cfgFile)))
      catch case NonFatal(t) => Left(t)
    read match
      case Right(Some(cfg)) => cfg.backend match
        case b: StoreConfig.Fsif => Some(FsifLayout(b.segmentPayloadSize, b.segmentCount))
        case _ => None
      case unusable =>
        val indexes =
          try FsifRecovery.IndexFiles.exists(n => FileStat.exists(dir / n))
          catch case NonFatal(t) if !journal =>
            logger.warn(s"Cannot read the directory $dir at the load of its device, which has no FSIF device " +
                        s"journal: it is not considered for one: $t")
            false
        if indexes then
          val why = unusable.left.toOption.fold("no store config")(t => s"a store config that cannot be used ($t)")
          throw new IOException(s"$dir holds FSIF index files but $why: whether it is attached to a journal is " +
                                s"unknown, so its device is left for the next scan")
        None

  /** Whether the directory `dir` is an FSIF directory (`layoutOf`, as a device load with a journal decides it) whose
   *  newest valid index is undetached (`undetached`): a transfer out's detach check (TransferDisk.moveOutbound). A
   *  directory with no FSIF config and no FSIF index file is not. Throws where layoutOf or the read does */
  def undetachedAt(dir: os.Path): Boolean =
    layoutOf(dir, journal = true).exists(layout => undetached(Dir(dir, None, layout)))

  /** Whether `d`'s newest valid index is undetached. One with no valid index says nothing (its own open finds it
   *  lost): false. A read that fails throws */
  private def undetached(d: Dir): Boolean =
    try !FsifRecovery.newestIndex(new NioFsifFiles(d.path), d.layout)._3.detached
    catch case _: FsifRecovery.StoreLost => false

  /** Lazy creation for the first FSIF store on a device (spec 4.1): `io.fsifJournal`'s journal, the device's journal
   *  at `devicePath` first created (sized by `cfg`, DeviceJournalFiles.sizingFor), opened and set there when the slot
   *  is empty and the device has no journal directory. A journal directory present while the slot is empty is the
   *  device load's to open (its replay may be due): an IOException, as is a journal that has failed */
  def ensure(io: DeviceIo, devicePath: os.Path, cfg: StorageDeviceConfig): FsifDeviceJournal =
    io.fsifJournal.get match
      case Some(j) => usable(io, j)
      case None =>
        val dir = devicePath / DeviceJournalFiles.DirName
        if FileStat.exists(dir) then
          throw new IOException(s"the FSIF device journal of $devicePath is not open: the device's load opens it")
        createInPlace(devicePath, DeviceJournalFiles.sizingFor(cfg))
        val journal = openFresh(dir, io.journalRun)
        if !io.fsifJournal.compareAndSet(None, Some(journal)) then
          journal.close()
          throw new IOException(s"the FSIF device journal of $devicePath was opened twice")
        journal

  /** The journal an FSIF store open on `io`'s device uses: the open one, or, on a device without a journal directory
   *  (loaded Absent: its FSIF copies were transferred in or placed there detached), one made by `ensure` with the
   *  device config at `io.path`. An IOException (a load retry) when the journal has failed, or a journal directory is
   *  there while none is open */
  def forOpen(io: DeviceIo): FsifDeviceJournal =
    io.fsifJournal.get match
      case Some(j) => usable(io, j)
      case None =>
        ensure(io, io.path,
          StorageDeviceConfig.loadStorageDeviceConfig(io.path / StorageDeviceConfig.configFilename))

  private def usable(io: DeviceIo, j: FsifDeviceJournal): FsifDeviceJournal =
    j.failed.foreach(t => throw new IOException(s"the FSIF device journal of ${io.path} has failed: $t", t))
    j
