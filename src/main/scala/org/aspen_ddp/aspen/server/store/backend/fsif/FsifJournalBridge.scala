package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException

import scala.util.control.NonFatal

import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.host.{CopyLayout, DeviceIo}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import scribe.Logging

/** TEMPORARY (Rulings R7, R14): fills `DeviceIo.fsifJournal` before the device load owns the journal. Task 10
  * replaces it with the device-load `FsifJournalOpen` job and the lazy create (`host.FsifJournalDisk.ensure`, for now
  * a stub over `ensure` here), and then this object goes.
  *
  * The journal is at `<device>/fsif-journal`, made on first use through `fsif-journal.creating` and a rename, over
  * DSYNC files, its writes on the device's journal thread (`DeviceIo.journalRun`), never forced. Filling an empty slot
  * recovers the ring and replays every FSIF directory under the phase directories a load can open (live/, arrived/,
  * outbound/, rebuilt/, parked/) and under rebuilding/ (spec 4.7 step 2); a directory whose replay finds it lost
  * makes its later opens through `journalFor` throw that StoreLost. A slot already filled is never replaced, even by a
  * failed journal: the opener refuses that one with an IOException (a load retry) until the device reloads. Sizing
  * when it creates: at the first open (`journalFor`), 3 files each holding at least two of that layout's largest
  * appends and at least 4 MiB; at a create (`ensure`), the device config's `fsif-journal-*` keys when either is set,
  * else 3 files of 4 MiB. What it does not do (Tasks 10-11): lock the journal, release a fail-stopped copy's
  * attachment on its move to lost/, or treat a lost journal as the spec does.
  */
object FsifJournalBridge extends Logging:
  private val lostByIo = new java.util.WeakHashMap[DeviceIo, Map[os.Path, FsifRecovery.StoreLost]]()

  /** The journal of `io`'s device, `io.fsifJournal` filled first when empty (`fill`); throws the StoreLost replay found
    * for `storePath`, or an IOException when no usable journal is open (a load retry)
    */
  def journalFor(io: DeviceIo, storePath: os.Path, layout: FsifLayout): FsifDeviceJournal =
    fill(io, io.path, sizingFor(layout))
    throwIfLost(io, storePath)
    usable(io)

  /** The journal of the device at `devicePath` (whose DeviceIo is `io`), `io.fsifJournal` filled first when empty
    * with a journal of `sizing` if none exists on disk; an IOException when no usable journal is open
    */
  def ensure(io: DeviceIo, devicePath: os.Path, sizing: => JournalSizing): FsifDeviceJournal =
    fill(io, devicePath, sizing)
    usable(io)

  /** When `io.fsifJournal` is empty: creates the device's journal if absent (of `sizing`), recovers and replays it,
    * and publishes it there. An IOException when it cannot (the slot stays empty)
    */
  def fill(io: DeviceIo, devicePath: os.Path, sizing: => JournalSizing): Unit = lostByIo.synchronized:
    if io.fsifJournal.get.isEmpty then
      val (journal, lost) = load(io, devicePath, sizing)
      lostByIo.put(io, lost)
      io.fsifJournal.set(Some(journal))

  /** Throws the StoreLost the replay that filled `io.fsifJournal` found for `storePath` */
  def throwIfLost(io: DeviceIo, storePath: os.Path): Unit =
    val lost = lostByIo.synchronized(Option(lostByIo.get(io)).getOrElse(Map.empty))
    lost.get(storePath).foreach(e => throw new FsifRecovery.StoreLost(e.getMessage))

  /** `io.fsifJournal`'s journal unless it is absent or has failed, which is an IOException (a load retry) */
  def usable(io: DeviceIo): FsifDeviceJournal = io.fsifJournal.get match
    case Some(j) =>
      j.failed.foreach(t => throw new IOException(s"FSIF journal of device ${io.path} has failed: $t", t))
      j
    case None => throw new IOException(s"FSIF journal of device ${io.path} is not open")

  private val Mib = 1L << 20

  /** A first open's sizing when the device has no journal */
  def sizingFor(layout: FsifLayout): JournalSizing =
    val one = FsifBackend.appendBound(layout.segmentPayloadSize) + FsifBackend.MappingAppendSize + 4096
    val size = math.min(1L << 30, math.max(4 * Mib, (2 * one + DeviceJournalFormat.HeaderSize + 4095) / 4096 * 4096))
    JournalSizing(java.util.UUID.randomUUID(), 3, size)

  /** A create's sizing when the device config sets no `fsif-journal-*` key */
  def defaultSizing: JournalSizing = JournalSizing(java.util.UUID.randomUUID(), 3, 4 * Mib)

  private def load(io: DeviceIo, device: os.Path,
                   sizing: => JournalSizing): (FsifDeviceJournal, Map[os.Path, FsifRecovery.StoreLost]) =
    val dir = device / DeviceJournalFiles.DirName
    try
      if !FileStat.exists(dir) then
        val creating = device / DeviceJournalFiles.CreatingName
        if FileStat.exists(creating) then
          os.remove.all(creating)
        DeviceJournalFiles.create(creating, sizing)
        DurableFiles.move(creating, dir)
      val recorded = DeviceJournalFiles.readSizing(dir)
      val files = new NioFsifFiles(dir, dsync = true)
      val result = DeviceJournalRecovery.recover(files, recorded, targets(device))
      val lost = result.outcomes.collect:
        case (path, DeviceJournalRecovery.Outcome.Lost(e)) =>
          logger.error(s"FSIF journal bridge: replay into $path found it lost: ${e.getMessage}")
          path -> e
      (new FsifDeviceJournal(files, recorded, result.start, io.journalRun, forceAfterWrite = false), lost.toMap)
    catch
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(s"FSIF journal of device $device cannot be opened: $t", t)
  /** Every FSIF directory a later open can reach, with its layout from its store config */
  private def targets(device: os.Path): Seq[DeviceJournalRecovery.Target] =
    val parents = Seq(CopyLayout.LiveDirectory, CopyLayout.ArrivedDirectory, CopyLayout.OutboundDirectory,
      CopyLayout.RebuiltDirectory, CopyLayout.ParkedDirectory, RebuildingStore.RebuildDirectory).map(device / _)
    for
      parent <- parents if FileStat.isDir(parent)
      dir <- os.list(parent) if FileStat.isDir(dir)
      layout <- fsifLayout(dir)
    yield DeviceJournalRecovery.Target(dir, new NioFsifFiles(dir), layout)

  private def fsifLayout(dir: os.Path): Option[FsifLayout] =
    val cfg = dir / StoreConfig.configFilename
    val hasIndex = FsifRecovery.IndexFiles.exists(n => FileStat.exists(dir / n))
    if !hasIndex || !FileStat.exists(cfg) then
      None
    else
      StoreConfig.loadStoreConfig(cfg).backend match
        case b: StoreConfig.Fsif => Some(FsifLayout(b.segmentPayloadSize, b.segmentCount))
        case _ => None
