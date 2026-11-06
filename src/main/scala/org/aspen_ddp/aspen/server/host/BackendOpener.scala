package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifLayout}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext

trait BackendOpener:
  /** Opens (or, with `create`, creates) the backend `storeCfg` names at `storePath`. A RocksDB backend is opened
   *  in the caller; an FSIF backend is opened on fsifOpenEc by the caller, touching no host state. `onFatal` is
   *  called once, on an FSIF io thread, when an FSIF backend fail-stops: it must only enqueue. */
  def open(storeCfg: StoreConfig, storePath: os.Path, create: Boolean, onFatal: Throwable => Unit): Backend

  /** Where FSIF stores are opened (recovered) and created */
  def fsifOpenEc: ExecutionContext

class DefaultBackendOpener(ec: ExecutionContext) extends BackendOpener:
  override def open(storeCfg: StoreConfig, storePath: os.Path, create: Boolean, onFatal: Throwable => Unit): Backend =
    val storeId = storeCfg.storeId
    storeCfg.backend match
      case _: StoreConfig.RocksDB => new RocksDBBackend(storePath, storeId, ec)
      case b: StoreConfig.Fsif =>
        val layout = FsifLayout(b.segmentPayloadSize, b.segmentCount)
        if create then FsifBackend.create(storeId, storePath, layout, FsifBackend.sharedIo, onFatal)
        else openFsifBackend(storeId, storePath, layout, onFatal)

  /** Opens an existing FSIF store (FsifBackend.open: a directory without the marker is initialized only if its
   *  files are blank, otherwise recovered). */
  def openFsifBackend(storeId: StoreId,
                      storePath: os.Path,
                      layout: FsifLayout,
                      onFatal: Throwable => Unit): Backend =
    FsifBackend.open(storeId, storePath, layout, FsifBackend.sharedIo, onFatal)

  override def fsifOpenEc: ExecutionContext = DefaultBackendOpener.FsifOpenEc

object DefaultBackendOpener:
  /** Host-wide pool for FSIF opens and creates (recovery reads and fsyncs whole stores): cached daemon threads
   *  named fsif-open-N */
  private[aspen] lazy val FsifOpenEc: ExecutionContext =
    val counter = new AtomicInteger(0)
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool: r =>
      val t = new Thread(r, s"fsif-open-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t)
