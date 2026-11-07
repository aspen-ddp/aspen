package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.store.FrontendSuite.{TestCrl, TestNet, poolId}
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionStatusCache}
import org.aspen_ddp.aspen.server.transfer.StoreTransferService
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, Promise}

class ControlContextSuite extends AnyFunSuite with Matchers:

  private val Bound = 10L // seconds

  private def withThreading(f: SingleThreadThreading => Unit): Unit =
    val t = new SingleThreadThreading()
    t.start()
    try f(t) finally t.shutdown()

  /** A DeviceControl with only what publishView reads: the router, the loaded devices, the rebuilds and the transfer
   *  service. Everything else is never touched. */
  private def deviceControl(threading: HostThreading): DeviceControl =
    new DeviceControl(
      metadata = null, scheduler = null, backends = null, devices = new LocalDevices(), client = null,
      hostId = HostId.BootstrapHostId, aspenSystemId = new UUID(0, 0), rootDir = os.root, net = null,
      backgroundTasks = BackgroundTaskManager.NoBackgroundTaskManager, threading = threading, post = _ => (),
      crlFactory = null, unloadedDeviceCrl = None, txStatusCache = null, wrapCompletionHandler = identity, objectCacheFactory = null,
      finalizerFactory = null,
      txDriverFactory = null, heartbeatPeriod = Duration(5, SECONDS), deviceScanPeriod = Duration(10, SECONDS),
      storeRebuildFactory = null, maxConcurrentRebuilds = 2, loadStore = null, tryLoadStore = null,
      loadStoreById = null, warn = null, storeTransferService = StoreTransferService.Disabled,
      shutdownWorkTimeout = Duration(60, SECONDS), gossipStoreOnline = null, closeStoreForTransferOut = null,
      closeStoreForShutdown = null, scheduleShutdownWorkTimeout = null, scheduleEndpointRetry = null,
      ec = ExecutionContext.parasitic)

  private def store(id: StoreId): Store =
    new Store(StorageDeviceId(new UUID(0, 99)), ExecutionContext.parasitic, new MapBackend(id), ObjectCache.NoCache,
      new TestNet, BackgroundTaskManager.NoBackgroundTaskManager, new TestCrl, new TransactionStatusCache(), null,
      TransactionDriver.noErrorRecoveryFactory, Duration(40, SECONDS), None)

  test("foreign-thread completion runs its callback on the control thread"):
    withThreading: t =>
      val p = Promise[Unit]()
      val ranOn = new AtomicReference[String]()
      val ran = new CountDownLatch(1)
      p.future.onComplete { _ =>
        ranOn.set(Thread.currentThread.getName)
        ran.countDown()
      }(using t.control)

      val completer = new Thread(() => { p.success(()); () }, "foreign-completer")
      completer.start()
      completer.join(TimeUnit.SECONDS.toMillis(Bound))

      ran.await(Bound, TimeUnit.SECONDS) should be(true)
      ranOn.get should be("aspen-host-control")

  test("repairable excludes offline, rebuilding and transferring-out stores"):
    val Seq(healthy, offline, rebuilding, transferring, unserved) = (10 to 14).map(i => StoreId(poolId, i.toByte))
    val view = HostStoreView.of(
      served = Set(healthy, offline, rebuilding, transferring),
      offline = Set(offline, unserved),
      rebuilding = Set(rebuilding),
      transferringOut = Set(transferring),
      devicePaths = Map())
    view.repairable should be(Set(healthy))
    view.served should be(Set(healthy, offline, rebuilding, transferring))
    view.offline should be(Set(offline, unserved))

  test("view is published to other threads"):
    withThreading: t =>
      val ctx = deviceControl(t)
      val storeId = StoreId(poolId, 3)
      val published = new CountDownLatch(1)
      val seen = new AtomicReference[Set[StoreId]]()

      val read: Runnable = () =>
        if published.await(Bound, TimeUnit.SECONDS) then
          seen.set(ctx.currentView.served)
      val reader = new Thread(read, "view-reader")
      reader.start()

      t.controlEvents.execute: () =>
        ctx.router.install(store(storeId), HostThreading.InlineStoreExecutor)
        ctx.publishView()
        published.countDown()

      reader.join(TimeUnit.SECONDS.toMillis(Bound))
      reader.isAlive should be(false)
      seen.get should be(Set(storeId))
