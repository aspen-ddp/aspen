package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{FileStat, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.host.{DeviceIo, FsifJournalDisk, JobKind}
import org.aspen_ddp.aspen.server.store.backend.fsif.{DeviceJournalFormat, FsifBackend, FsifJournalBridge, FsifLayout,
  FsifRecovery}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend

import scala.concurrent.{ExecutionContext, Future}

/** Creates and opens store backends. The one place that names the concrete backends: the host's loads and creates
 *  (StoreLoader.startOpen) and a rebuild's staging store (RebuildingStore) treat every backend alike. */
trait BackendOpener:
  /** Creates `storeCfg`'s store at `storePath`, atomically (StoreCreation, with `creatingRoot` the device's
   *  `creating/` directory), without opening it. `storePath` must not exist. Fails with the backend's error, with
   *  nothing left at `storePath`; an out-of-space failure is `Backend.isOutOfSpace`. Runs on `io`'s management lane.
   *  An FSIF create first opens the device's journal (creating it if absent, sized by the device config at the parent
   *  of `creatingRoot`) and refuses a layout whose largest commit cannot fit one journal file
   *  (`IllegalArgumentException`). */
  def create(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path, io: DeviceIo): Future[Unit]

  /** Opens the existing backend `storeCfg` names at `storePath`. Never creates: a missing or blank backend is a lost
   *  one (`BackendStateLost`), since a store directory exists only once `create` has completed. The open runs on
   *  `io`'s management lane (JobKind.BackendOpen), never in the caller, touching no host state; an FSIF backend's
   *  later I/O runs on `io`'s data and serial pools. `onFatal` is called once, from a backend thread, when the backend fail-stops: it must only enqueue.
   *
   *  A failure that is `Backend.isOutOfSpace` leaves the store's data intact; a `BackendStateLost` means it must be
   *  rebuilt. */
  def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit, io: DeviceIo): Future[Backend]

class DefaultBackendOpener(ec: ExecutionContext) extends BackendOpener:
  override def create(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path, io: DeviceIo): Future[Unit] =
    io.submit(JobKind.BackendCreate, Some(storeCfg.storeId)):
      storeCfg.backend match
        case b: StoreConfig.Fsif =>
          val device = creatingRoot / os.up
          val deviceCfg = StorageDeviceConfig.loadStorageDeviceConfig(device / StorageDeviceConfig.configFilename)
          val journal = FsifJournalDisk.ensure(io, device, deviceCfg)
          val layout = FsifLayout(b.segmentPayloadSize, b.segmentCount)
          if !FsifBackend.fitsJournal(layout, journal.sizing) then
            throw new IllegalArgumentException(s"FSIF store ${storeCfg.storeId}: a commit of a " +
              s"${layout.segmentPayloadSize}-byte slice needs " +
              s"${FsifBackend.appendBound(layout.segmentPayloadSize) + FsifBackend.MappingAppendSize} bytes of a " +
              s"device journal file, which holds ${journal.sizing.fileSize - DeviceJournalFormat.HeaderSize} " +
              s"(file size ${journal.sizing.fileSize})")
        case _ => ()
      DefaultBackendOpener.createStore(storeCfg, storePath, creatingRoot)
    .future

  override def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit,
                    io: DeviceIo): Future[Backend] =
    val storeId = storeCfg.storeId
    io.submit(JobKind.BackendOpen, Some(storeId)):
      storeCfg.backend match
        case _: StoreConfig.RocksDB => new RocksDBBackend(storePath, storeId, io)
        case b: StoreConfig.Fsif =>
          openFsifBackend(storeId, storePath, FsifLayout(b.segmentPayloadSize, b.segmentCount), onFatal, io)
    .future

  /** Opens an existing FSIF store (FsifBackend.open: a missing or blank store is lost), its I/O on the device's data
   *  pool and its serial drains on the device's serial pool, against the device's journal, `io.fsifJournal`. A journal
   *  absent or failed is an IOException (a load retry), never a lost store; so is a layout whose largest commit cannot
   *  fit one journal file. While the slot is empty the temporary FsifJournalBridge fills it first (Task 10: the device
   *  load's job). */
  def openFsifBackend(storeId: StoreId,
                      storePath: os.Path,
                      layout: FsifLayout,
                      onFatal: Throwable => Unit,
                      io: DeviceIo): Backend =
    if !FileStat.isDir(storePath) then
      throw new FsifRecovery.StoreLost(s"FSIF store $storeId: its directory $storePath is missing")
    if io.fsifJournal.get.isEmpty then
      FsifJournalBridge.fill(io, io.path, FsifJournalBridge.sizingFor(layout))
    FsifJournalBridge.throwIfLost(io, storePath)
    val journal = FsifJournalBridge.usable(io)
    FsifBackend.open(storeId, storePath, layout, io.dataEc, journal, onFatal, serialEc = Some(io.serialEc),
      readGate = Some(io))

object DefaultBackendOpener:
  /** Creates `storeCfg`'s store at `storePath` atomically, blocking: its backend initialized empty and closed, then
   *  the directory renamed into place (StoreCreation). For BackendOpener.create and bootstrap. */
  def createStore(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path): Unit =
    StoreCreation.createAtomically(storeCfg, storePath, creatingRoot): dir =>
      storeCfg.backend match
        case _: StoreConfig.RocksDB => RocksDBBackend.initializeStore(dir)
        case b: StoreConfig.Fsif => FsifBackend.initializeStore(dir, FsifLayout(b.segmentPayloadSize, b.segmentCount))
