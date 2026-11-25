package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException

import scala.util.control.NonFatal

import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.host.{CopyLayout, DeviceIo}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import scribe.Logging

/** TEMPORARY (Ruling R7): gives `DefaultBackendOpener.openFsifBackend` a device journal before the device load owns
  * one. Tasks 9-10 replace it with `DeviceIo.fsifJournal`, the device-load `FsifJournalOpen` job and the lazy create
  * ahead of the first FSIF store create, and then this object goes.
  *
  * One journal per `DeviceIo` instance (a new Host over the same device gets a fresh recovery), at
  * `<device>/fsif-journal`, made on first use through `fsif-journal.creating` and a rename. The first use recovers
  * the ring and replays every FSIF directory under the phase directories a load can open (live/, arrived/,
  * outbound/, rebuilt/, parked/) and under rebuilding/ (spec 4.7 step 2); a directory whose replay finds it lost
  * makes its later opens throw that StoreLost. A journal that has failed is recovered again on the next use. Its
  * writes run on the device's data pool, forced after each write (the files are not DSYNC). Sizing: 3 files, each
  * holding at least two of the first layout's largest appends and at least 4 MiB, since a `DeviceIo` carries no device
  * config. What it does not do (Tasks 9-11): lock the journal, release a fail-stopped copy's attachment on its move
  * to lost/, or treat a lost journal as the spec does.
  */
object FsifJournalBridge extends Logging:
  private final class Entry(val journal: FsifDeviceJournal, val lost: Map[os.Path, FsifRecovery.StoreLost])

  private val entries = new java.util.WeakHashMap[DeviceIo, Entry]()

  /** The journal of `io`'s device, recovered and replayed on first use; throws the StoreLost replay found for
    * `storePath`, or an IOException when the journal cannot be opened (a load retry)
    */
  def journalFor(io: DeviceIo, storePath: os.Path, layout: FsifLayout): FsifDeviceJournal =
    val entry = entries.synchronized:
      Option(entries.get(io)).filter(_.journal.failed.isEmpty).getOrElse:
        val e = load(io, layout)
        entries.put(io, e)
        e
    entry.lost.get(storePath).foreach(e => throw new FsifRecovery.StoreLost(e.getMessage))
    entry.journal

  private val Mib = 1L << 20

  private def sizingFor(layout: FsifLayout): JournalSizing =
    val one = FsifBackend.appendBound(layout.segmentPayloadSize) + FsifBackend.MappingAppendSize + 4096
    val size = math.min(1L << 30, math.max(4 * Mib, (2 * one + DeviceJournalFormat.HeaderSize + 4095) / 4096 * 4096))
    JournalSizing(java.util.UUID.randomUUID(), 3, size)

  private def load(io: DeviceIo, layout: FsifLayout): Entry =
    val device = io.path
    val dir = device / DeviceJournalFiles.DirName
    try
      if !FileStat.exists(dir) then
        val creating = device / DeviceJournalFiles.CreatingName
        if FileStat.exists(creating) then
          os.remove.all(creating)
        DeviceJournalFiles.create(creating, sizingFor(layout))
        DurableFiles.move(creating, dir)
      val sizing = DeviceJournalFiles.readSizing(dir)
      val files = new NioFsifFiles(dir)
      val result = DeviceJournalRecovery.recover(files, sizing, targets(device))
      val lost = result.outcomes.collect:
        case (path, DeviceJournalRecovery.Outcome.Lost(e)) =>
          logger.error(s"FSIF journal bridge: replay into $path found it lost: ${e.getMessage}")
          path -> e
      new Entry(new FsifDeviceJournal(files, sizing, result.start, r => io.dataEc.execute(r), forceAfterWrite = true),
        lost)
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
