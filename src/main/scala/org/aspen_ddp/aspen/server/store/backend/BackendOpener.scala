package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifLayout}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Creates and opens store backends. The one place that names the concrete backends: the host's loads and creates
 *  (StoreLoader.startOpen) and a rebuild's staging store (RebuildingStore) treat every backend alike. */
trait BackendOpener:
  /** Creates `storeCfg`'s store at `storePath`, atomically (StoreCreation, with `creatingRoot` the device's
   *  `creating/` directory), without opening it. `storePath` must not exist. Fails with the backend's error, with
   *  nothing left at `storePath`; an out-of-space failure is `Backend.isOutOfSpace`. Runs where `open` would. */
  def create(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path): Future[Unit]

  /** Opens the existing backend `storeCfg` names at `storePath`. Never creates: a missing or blank backend is a lost
   *  one (`BackendStateLost`), since a store directory exists only once `create` has completed. The opener decides
   *  where the open runs: a quick one may run in the caller and return a completed Future; a slow one (recovery that
   *  reads and fsyncs a whole store) must run elsewhere, touching no host state, since the caller may be the host's
   *  control thread. `onFatal` is called once, from a backend thread, when the backend fail-stops: it must only enqueue.
   *
   *  A failure that is `Backend.isOutOfSpace` leaves the store's data intact; a `BackendStateLost` means it must be
   *  rebuilt. */
  def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit): Future[Backend]

class DefaultBackendOpener(ec: ExecutionContext) extends BackendOpener:
  override def create(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path): Future[Unit] =
    storeCfg.backend match
      case _: StoreConfig.RocksDB =>
        Future.fromTry(Try(DefaultBackendOpener.createStore(storeCfg, storePath, creatingRoot)))
      case _: StoreConfig.Fsif =>
        Future(DefaultBackendOpener.createStore(storeCfg, storePath, creatingRoot))(using fsifOpenEc)

  override def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit): Future[Backend] =
    val storeId = storeCfg.storeId
    storeCfg.backend match
      case _: StoreConfig.RocksDB => Future.fromTry(Try(new RocksDBBackend(storePath, storeId, ec)))
      case b: StoreConfig.Fsif =>
        val layout = FsifLayout(b.segmentPayloadSize, b.segmentCount)
        Future(openFsifBackend(storeId, storePath, layout, onFatal))(using fsifOpenEc)

  /** Opens an existing FSIF store (FsifBackend.open: a missing or blank store is lost) */
  def openFsifBackend(storeId: StoreId,
                      storePath: os.Path,
                      layout: FsifLayout,
                      onFatal: Throwable => Unit): Backend =
    FsifBackend.open(storeId, storePath, layout, FsifBackend.sharedIo, onFatal)

  /** Where FSIF stores are opened (recovered) and created */
  protected def fsifOpenEc: ExecutionContext = DefaultBackendOpener.FsifOpenEc

object DefaultBackendOpener:
  /** Creates `storeCfg`'s store at `storePath` atomically, blocking: its backend initialized empty and closed, then
   *  the directory renamed into place (StoreCreation). For BackendOpener.create and bootstrap. */
  def createStore(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path): Unit =
    StoreCreation.createAtomically(storeCfg, storePath, creatingRoot): dir =>
      storeCfg.backend match
        case _: StoreConfig.RocksDB => RocksDBBackend.initializeStore(dir)
        case b: StoreConfig.Fsif => FsifBackend.initializeStore(dir, FsifLayout(b.segmentPayloadSize, b.segmentCount))

  /** Host-wide pool for FSIF opens and creates (recovery reads and fsyncs whole stores): cached daemon threads
   *  named fsif-open-N */
  private[aspen] lazy val FsifOpenEc: ExecutionContext =
    val counter = new AtomicInteger(0)
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool: r =>
      val t = new Thread(r, s"fsif-open-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t)
