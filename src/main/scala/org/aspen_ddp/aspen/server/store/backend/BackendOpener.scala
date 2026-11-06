package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifLayout}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Opens store backends. The one place that names the concrete backends: the host's loads (StoreLoader.startOpen)
 *  and a rebuild's staging store (RebuildingStore) treat every open alike. */
trait BackendOpener:
  /** Opens (or, with `create`, creates) the backend `storeCfg` names at `storePath`. The opener decides where the
   *  open runs: a quick one may run in the caller and return a completed Future; a slow one (recovery that reads and
   *  fsyncs a whole store) must run elsewhere, touching no host state, since the caller may be the host's control thread.
   *  `onFatal` is called once, from a backend thread, when the backend fail-stops: it must only enqueue.
   *
   *  A failure that is `Backend.isOutOfSpace` leaves the store's data intact; a `BackendStateLost` means it must be
   *  rebuilt. */
  def open(storeCfg: StoreConfig, storePath: os.Path, create: Boolean, onFatal: Throwable => Unit): Future[Backend]

class DefaultBackendOpener(ec: ExecutionContext) extends BackendOpener:
  override def open(storeCfg: StoreConfig, storePath: os.Path, create: Boolean,
                    onFatal: Throwable => Unit): Future[Backend] =
    val storeId = storeCfg.storeId
    storeCfg.backend match
      case _: StoreConfig.RocksDB => Future.fromTry(Try(new RocksDBBackend(storePath, storeId, ec)))
      case b: StoreConfig.Fsif =>
        val layout = FsifLayout(b.segmentPayloadSize, b.segmentCount)
        Future {
          if create then FsifBackend.create(storeId, storePath, layout, FsifBackend.sharedIo, onFatal)
          else openFsifBackend(storeId, storePath, layout, onFatal)
        }(using fsifOpenEc)

  /** Opens an existing FSIF store (FsifBackend.open: a directory without the marker is initialized only if its
   *  files are blank, otherwise recovered). */
  def openFsifBackend(storeId: StoreId,
                      storePath: os.Path,
                      layout: FsifLayout,
                      onFatal: Throwable => Unit): Backend =
    FsifBackend.open(storeId, storePath, layout, FsifBackend.sharedIo, onFatal)

  /** Where FSIF stores are opened (recovered) and created */
  protected def fsifOpenEc: ExecutionContext = DefaultBackendOpener.FsifOpenEc

object DefaultBackendOpener:
  /** Host-wide pool for FSIF opens and creates (recovery reads and fsyncs whole stores): cached daemon threads
   *  named fsif-open-N */
  private[aspen] lazy val FsifOpenEc: ExecutionContext =
    val counter = new AtomicInteger(0)
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool: r =>
      val t = new Thread(r, s"fsif-open-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t)
