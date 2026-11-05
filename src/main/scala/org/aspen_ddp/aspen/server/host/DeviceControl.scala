package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, CompletionHandler}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

/** What the classes extracted from Host share: the host's identity and services, the seams that tests replace, and
 *  the loaded devices. Each component takes the DeviceControl as its only constructor parameter and is added here as
 *  a lazy val, which lets components refer to each other without construction-order cycles.
 *
 *  `host` is the Host instance, which is also the lock, and `post` puts an event on its queue; both go away with the
 *  event queue. Components reach members of Host that have not been extracted yet through `host`.
 *
 *  `loadStore` and `tryLoadStore` are adapters that call the overridable methods on Host, so the test subclasses that
 *  replace them still take effect when a component loads a store. */
final class DeviceControl(val metadata: DeviceMetadata,
                          val scheduler: ControlScheduler,
                          val backends: BackendOpener,
                          val devices: LocalDevices,
                          val client: AspenClient,
                          val hostId: HostId,
                          val aspenSystemId: UUID,
                          val rootDir: os.Path,
                          val net: Messenger,
                          val backgroundTasks: BackgroundTaskManager,
                          val host: Host,
                          val post: Host.Event => Unit,
                          val crl: CrashRecoveryLog,
                          val txStatusCache: TransactionStatusCache,
                          val ioHandler: CompletionHandler,
                          val objectCacheFactory: StoreId => ObjectCache,
                          val finalizerFactory: TransactionFinalizer.Factory,
                          val txDriverFactory: TransactionDriver.Factory,
                          val heartbeatPeriod: Duration,
                          val deviceScanPeriod: Duration,
                          val loadStore: (StorageDeviceId, Backend, Option[Long]) => Future[Unit],
                          val tryLoadStore: (LocalStorageDeviceState, os.Path, Option[() => Unit]) => Unit)
                         (using val ec: ExecutionContext):
  def lock: AnyRef = host

  lazy val router: StoreRouter = new StoreRouter(this)
  lazy val loader: StoreLoader = new StoreLoader(this)
