package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.transfer.StoreTransferService
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.rebuild.StoreRebuildFactory
import org.aspen_ddp.aspen.server.store.UnackedCommitLookup
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendOpener, CompletionHandler}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  What the classes extracted from Host share: the host's identity and services, the seams that tests replace, and
 *  the loaded devices. Each component takes the DeviceControl as its only constructor parameter and is added here as
 *  a lazy val, which lets components refer to each other without construction-order cycles.
 *
 *  The only given context is `threading.control`, which every component imports (`import ctx.given`), so
 *  every Future callback in device management runs on the control thread. `ec` is the host's general-purpose
 *  context: it is never a given, and only work deliberately run elsewhere names it. `post` hands an event to the
 *  host's HostThreading.
 *
 *  `currentView` is the one piece of control state other threads may read: an immutable HostStoreView, republished
 *  by `publishView` after every change to what it reflects.
 *
 *  The function-valued parameters from `loadStore` on are the callbacks a test replaces through Host.Seams. */
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
                          val threading: HostThreading,
                          val post: Host.Event => Unit,
                          /** Opens each device's CRL as the device loads */
                          val crlFactory: CrashRecoveryLogFactory,
                          /** The CRL of stores loaded onto a device this host has not loaded (Host.Seams) */
                          val unloadedDeviceCrl: Option[CrashRecoveryLog],
                          val txStatusCache: TransactionStatusCache,
                          /** Wraps each backend's completion handler (a test seam; production: identity). */
                          val wrapCompletionHandler: CompletionHandler => CompletionHandler,
                          val objectCacheFactory: (StoreId, ExecutionContext) => ObjectCache,
                          val finalizerFactory: TransactionFinalizer.Factory,
                          val txDriverFactory: TransactionDriver.Factory,
                          val heartbeatPeriod: Duration,
                          val deviceScanPeriod: Duration,
                          val storeRebuildFactory: StoreRebuildFactory,
                          val maxConcurrentRebuilds: Int,
                          val loadStore: (StorageDeviceId, Backend, Option[Long]) => Future[Unit],
                          val tryLoadStore: (LocalStorageDeviceState, os.Path, Option[() => Unit]) => Unit,
                          val loadStoreById: (StorageDeviceId, StoreId) => Unit,
                          val warn: (os.Path, String) => Unit,
                          val storeTransferService: StoreTransferService,
                          val shutdownWorkTimeout: Duration,
                          val gossipStoreOnline: StoreId => Unit,
                          val closeStoreForTransferOut: StoreId => Future[Unit],
                          val closeStoreForShutdown: StoreId => Future[Unit],
                          val scheduleShutdownWorkTimeout: (StorageDeviceId, () => Unit) => Unit,
                          val scheduleEndpointRetry: (() => Unit) => Unit,
                          /** The host's general-purpose context: stores' transaction drivers and client
                           *  transactions run on it. Never a given. */
                          val ec: ExecutionContext,
                          /** Writes a store directory's rebuild-required marker (a test seam) */
                          val writeRebuildRequiredMarker: os.Path => Unit = StoreRebuilds.writeRebuildRequiredMarker,
                          /** Looks up recovered commit votes in their pools' UnackedCommits trees, for the stores */
                          val unackedLookup: UnackedCommitLookup = UnackedCommitLookup.Never):
  given ExecutionContext = threading.control

  lazy val router: StoreRouter = new StoreRouter(this)
  lazy val loader: StoreLoader = new StoreLoader(this)
  lazy val discovery: DeviceDiscovery = new DeviceDiscovery(this)
  lazy val transfers: StoreTransfers = new StoreTransfers(this)
  lazy val rebuilds: StoreRebuilds = new StoreRebuilds(this)
  lazy val shutdown: DeviceShutdown = new DeviceShutdown(this)
  lazy val reconciler: DeviceReconciler = new DeviceReconciler(this)
  lazy val services: HostServices = new HostServices(this)

  @volatile private var view: HostStoreView = HostStoreView.Empty

  /** The latest snapshot published by the control thread. Safe to read from any thread. */
  def currentView: HostStoreView = view

  /** Rebuilds the snapshot from the router's maps, the loaded devices, the rebuilds running and the stores
   *  transferring out, and publishes it. Called on the control thread after every change to any of them. */
  private[server] def publishView(): Unit =
    view = HostStoreView.of(router.loaded.keySet, router.offlineStores, rebuilds.rebuildingStoreIds,
      storeTransferService.transferringOutStores, devices.devices.map((id, sds) => id -> sds.devicePath))
