package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{TestDeviceIo, DurableFiles, Host, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, StoreRecoveryState}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{RebuildOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.transfer.TransferLane
import org.aspen_ddp.aspen.server.transfer.StoreTransferService
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendStateLost, DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Failure, Success, Try}

/** Stage 2, Task 2: Lost is the lost/ directory. Every T10 trigger moves a closed copy into lost/<copy> (a FailStop
 *  close moves it when the close ends, P5), retire moves lost/<copy> to pending-delete/<copy> (T11), and a copy whose
 *  move to lost/ fails is held offline until a device check moves it.
 *
 *  Task 3: Outbound is the outbound/ directory. A transfer out moves the closed copy there with its transfer id (T6)
 *  and ships it from there; an aborted or abandoned one moves it back to Live (T7).
 *
 *  Task 4: Rebuilt is rebuilt/ and Parked is parked/. A rebuild's finish renames its staging into rebuilt/ (T2); its
 *  flip moves the copy Live (T3) or to parked/ (T4), or deletes it (T13); a parked copy goes Live (T5) or is shipped
 *  (T6).
 *
 *  Task 6: Live is live/. Every copy is in a phase directory under a copy id; the loader is offered only live/ copies,
 *  a device holding a store in two copy directories retires both (P2), and an unreadable phase directory or
 *  phase-param leaves the device unloaded (P6). */
class PhaseDirectoriesSuite extends IntegrationTestSuite with CopyHostFixture:

  /** The copy of `sid` is in lost/ under a copy name, and nothing is left at its live/ path */
  private def inLost(dev: os.Path, sid: StoreId, flat: os.Path): org.scalatest.compatible.Assertion =
    CopyFixtures.phaseOf(dev, sid) should be(Some(Phase.Lost))
    val path = CopyFixtures.copyPath(dev, sid).get
    path / os.up should be(lostDir(dev))
    CopyLayout.parseName(path.last).map(_._1) should be(Some(sid))
    os.exists(flat) should be(false)

  atest("a served copy whose backend fails moves to lost when its close ends"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    val gate = Promise[Unit]()
    for
      _ <- pump(host, host.control.router.loaded.contains(s))
      served = host.control.router.loaded.contains(s)
      _ = opener.closeGate = Some(gate.future)
      _ = opener.onFatals.head(new java.io.IOException("injected"))
      _ <- pump(host, !host.control.router.loaded.contains(s))
      _ <- pump(host, false, rounds = 20)
      // The close is held: the copy is still Live in live/, its backend's directory never moved while open
      heldPhase = CopyFixtures.phaseOf(dev, s)
      heldPath = CopyFixtures.copyPath(dev, s)
      _ = gate.success(())
      _ <- pump(host, CopyFixtures.phaseOf(dev, s).contains(Phase.Lost))
    yield
      served should be(true)
      heldPhase should be(Some(Phase.Live))
      heldPath should be(Some(flat))
      inLost(dev, s, flat)
      host.control.router.loaded.contains(s) should be(false)

  /** A real host serving a RocksDB copy of `s` on device A, whose backend has failed with its FailStop close held by
   *  the returned gate: the copy is Closing, still Live in live/ */
  private def heldFailStop(hostRoot: os.Path, dev: os.Path): Future[(Host, os.Path, Promise[Unit])] =
    given ExecutionContext = executionContext
    val flat = rocksCopy(dev, s)
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    val gate = Promise[Unit]()
    for
      _ <- pump(host, host.control.router.loaded.contains(s))
      _ = opener.closeGate = Some(gate.future)
      _ = opener.onFatals.head(new java.io.IOException("injected"))
      _ <- pump(host, !host.control.router.loaded.contains(s))
    yield (host, flat, gate)

  private def closingFailStop(host: Host): Boolean =
    host.control.copyRegistry.copiesOf(loadedDevices(host)(deviceA)).flatMap(_.copies.get(s)).map(_.activity)
      .contains(Activity.Closing(ClosePurpose.FailStop))

  atest("a shutdown waits for a fail-stop close to end, and its copy is moved to lost before the unload"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val active = org.aspen_ddp.aspen.common.metadata.StorageDeviceState.StoreEntry(
      org.aspen_ddp.aspen.common.metadata.StorageDeviceState.StoreStatus.Active, None)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(s -> active)))
      _ <- waitForTransactionsToComplete()
      (host, flat, gate) <- heldFailStop(hostRoot, dev)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpTx(host, recorded(host).shutdownOutcomes.nonEmpty)
      _ <- pumpTx(host, false, rounds = 10)
      // Shut down in the tree, but not unloaded while the close runs: the copy is still Live where it was
      outcomes = recorded(host).shutdownOutcomes.toList
      loadedWhileHeld = loadedDevices(host).contains(deviceA)
      closingWhileHeld = closingFailStop(host)
      phaseWhileHeld = CopyFixtures.phaseOf(dev, s)
      _ = gate.success(())
      _ <- pumpTx(host, !loadedDevices(host).contains(deviceA))
    yield
      outcomes should be(List(DeviceMetadata.ShutdownOutcome.Completed))
      loadedWhileHeld should be(true)
      closingWhileHeld should be(true)
      phaseWhileHeld should be(Some(Phase.Live))
      loadedDevices(host).contains(deviceA) should be(false)
      // What an adopting host reads: CrlLost, never a clean copy
      inLost(dev, s, flat)

  atest("a relinquish waits for a fail-stop close to end, and its copy is moved to lost before the unload"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val other = org.aspen_ddp.aspen.common.metadata.HostId(new java.util.UUID(9, 9))
    for
      (host, flat, gate) <- heldFailStop(hostRoot, dev)
      // The tree names another owner: the device is relinquished
      armed = recorded(host).armLookup(deviceA)
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ = armed.success(deviceState(deviceA).copy(hostId = other))
      _ <- pump(host, false, rounds = 20)
      loadedWhileHeld = loadedDevices(host).contains(deviceA)
      closingWhileHeld = closingFailStop(host)
      phaseWhileHeld = CopyFixtures.phaseOf(dev, s)
      _ = gate.success(())
      _ <- pump(host, !loadedDevices(host).contains(deviceA))
    yield
      loadedWhileHeld should be(true)
      closingWhileHeld should be(true)
      phaseWhileHeld should be(Some(Phase.Live))
      loadedDevices(host).contains(deviceA) should be(false)
      inLost(dev, s, flat)

  // Final review M2.12: a throw in a close's end, before its closeDone, left the close counted running for good, and
  // every later unload of the device waited for it forever. Both close ends: closeUnloaded's (a fail-stop's) and
  // handleClose's (the relinquish's own close)
  for failStopFirst <- List(true, false) do
    val which = if failStopFirst then "a fail-stop close" else "a relinquish's close"
    atest(s"a throw in the end of $which does not wedge the device's unload"):
      given ExecutionContext = executionContext
      val (hostRoot, dev) = device()
      val other = org.aspen_ddp.aspen.common.metadata.HostId(new java.util.UUID(9, 9))
      rocksCopy(dev, s)
      val opener = new TestOpener
      @volatile var throws = 0
      val host = realHost(hostRoot, opener, threadingOf = ec => new LockstepThreading(ec):
        override def release(storeId: StoreId): Unit =
          if storeId == s && throws == 0 then
            throws += 1
            throw new IllegalStateException("injected release failure"))
      for
        _ <- pump(host, host.control.router.loaded.contains(s))
        _ <-
          if failStopFirst then
            opener.onFatals.head(new java.io.IOException("injected"))
            pump(host, !host.control.router.loaded.contains(s) && !host.control.rebuilds.closesRunning(deviceA))
          else Future.unit
        armed = recorded(host).armLookup(deviceA)
        _ = host.control.reconciler.checkStorageDevice(deviceA)
        _ = armed.success(deviceState(deviceA).copy(hostId = other))
        _ <- pump(host, !loadedDevices(host).contains(deviceA), rounds = 200)
      yield
        throws should be(1)
        host.control.router.loaded.contains(s) should be(false)
        loadedDevices(host).contains(deviceA) should be(false)

  // Final review I3: a relinquish or a CRL failure waits for a fail-stop close before its unload, which then unloads
  // whatever is open. A load retry firing during that wait must open nothing: after a relinquish, the adopter would
  // serve the same store
  for how <- List("relinquish", "CRL failure") do
    atest(s"a load retry during a $how's wait for a fail-stop close opens nothing before the unload"):
      given ExecutionContext = executionContext
      val (hostRoot, dev) = device()
      val other = org.aspen_ddp.aspen.common.metadata.HostId(new java.util.UUID(9, 9))
      val s1 = StoreId(s.poolId, 1)
      val active = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
      for
        (host, _, gate) <- heldFailStop(hostRoot, dev)
        local = loadedDevices(host)(deviceA)
        // A copy of s1 awaiting a load retry, due at the next scan
        retryPath = rocksCopy(dev, s1)
        _ = host.testingOnlyRescan(deviceA)
        _ = host.control.copyRegistry.awaitingRetry("test", s1, Host.LoadRetry(local, retryPath, 1, 1))
        _ = how match
          case "relinquish" =>
            // The tree names another owner: the device is relinquished
            val armed = recorded(host).armLookup(deviceA)
            host.control.reconciler.checkStorageDevice(deviceA)
            armed.success(deviceState(deviceA, Map(s -> active, s1 -> active)).copy(hostId = other))
          case _ =>
            local.reconciledEntries = Some(Map(s -> active, s1 -> active))
            host.control.shutdown.crlFailed(local, new Exception("injected CRL failure"))
        _ <- pump(host, false, rounds = 20)
        closingWhileHeld = closingFailStop(host)
        _ = host.control.loader.retryFailedLoads()
        _ <- pump(host, false, rounds = 20)
        s1WhileHeld = (host.control.router.loaded.contains(s1), recordOf(host, local, s1).map(_.activity))
        _ = gate.success(())
        _ <- pump(host, !loadedDevices(host).contains(deviceA))
      yield
        closingWhileHeld should be(true)
        s1WhileHeld._1 should be(false)
        s1WhileHeld._2 should not be(Some(Activity.Served))
        s1WhileHeld._2.exists(_.isInstanceOf[Activity.Opening]) should be(false)
        loadedDevices(host).contains(deviceA) should be(false)
        host.control.router.loaded.contains(s1) should be(false)

  atest("a crash after a fail-stop close and before its move reloads the copy from live"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    val gate = Promise[Unit]()
    for
      _ <- pump(host, host.control.router.loaded.contains(s))
      _ = opener.closeGate = Some(gate.future)
      _ = opener.onFatals.head(new java.io.IOException("injected"))
      _ <- pump(host, !host.control.router.loaded.contains(s))
      _ <- pump(host, false, rounds = 20)
      // The host crashes with the close's end held: the old host is dropped, and a new one starts over its directory
      restarted = newHost(hostRoot)
      _ = restarted.testingOnlyHandleEvents()
      attempts = recorded(restarted).storeLoadAttempts.toList
      phaseAfterCrash = CopyFixtures.phaseOf(dev, s)
      lostAfterCrash = if os.exists(lostDir(dev)) then os.list(lostDir(dev)).toList else Nil
    yield
      // Lets the dropped host's RocksDB close, for the suite's teardown
      gate.success(())
      attempts should contain((deviceA, flat))
      phaseAfterCrash should be(Some(Phase.Live))
      lostAfterCrash should be(Nil)

  atest("an unserved backend that fails at open moves to lost after its close"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    val opener = new TestOpener
    val gate = Promise[Unit]()
    // Failed between its open and its LoadStore: retired unserved (StoreRebuilds.retireUnserved), its close held
    opener.failing = true
    opener.closeGate = Some(gate.future)
    val host = realHost(hostRoot, opener)
    for
      _ <- pump(host, false, rounds = 20)
      heldPhase = CopyFixtures.phaseOf(dev, s)
      heldPath = CopyFixtures.copyPath(dev, s)
      _ = gate.success(())
      _ <- pump(host, CopyFixtures.phaseOf(dev, s).contains(Phase.Lost))
    yield
      host.control.router.loaded.contains(s) should be(false)
      heldPhase should be(Some(Phase.Live))
      heldPath should be(Some(flat))
      inLost(dev, s, flat)

  // Final review I1: while an unserved backend that must never serve closes, its copy is held Closing(Unserved), not
  // Idle, so a device check in the window does not load it (CopyDecision's Load) and open a second backend on the
  // directory the first still closes
  atest("a device check while an unserved failed backend closes opens no second backend"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    val opener = new TestOpener
    val gate = Promise[Unit]()
    opener.failing = true
    opener.closeGate = Some(gate.future)
    val host = realHost(hostRoot, opener)
    val active = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
    for
      _ <- pump(host, false, rounds = 20)
      local = loadedDevices(host)(deviceA)
      opensBefore = opener.opens.get
      _ <- check(host, Map(s -> active))
      _ <- pump(host, false, rounds = 20)
      opensAfterCheck = opener.opens.get
      heldActivity = recordOf(host, local, s).map(_.activity)
      heldPhase = CopyFixtures.phaseOf(dev, s)
      _ = gate.success(())
      _ <- pump(host, CopyFixtures.phaseOf(dev, s).contains(Phase.Lost))
    yield
      opensBefore should be(1)
      opensAfterCheck should be(1)
      heldActivity should be(Some(Activity.Closing(ClosePurpose.Unserved)))
      heldPhase should be(Some(Phase.Live))
      opener.opens.get should be(1)
      host.control.router.loaded.contains(s) should be(false)
      inLost(dev, s, flat)
      recordOf(host, local, s).map(_.activity) should be(Some(Activity.Idle))

  // As above, for an open that ends with a failed backend while another backend serves the store
  // (StoreLoader.handleBackendOpened): its copy is held Closing(Unserved) until the close ends, then moved to lost/
  atest("a failed backend opened while another serves the store holds its copy until its close ends"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    rocksCopy(dev, s)
    val devB = writeDevice(hostRoot, "dev1", deviceB)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devB))
    // A stale copy on B, not loaded while A's serves the store
    val flatB = rocksCopy(devB, s)
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    val gate = Promise[Unit]()
    for
      _ <- pump(host, host.control.router.loaded.contains(s) && loadedDevices(host).contains(deviceB))
      localB = loadedDevices(host)(deviceB)
      // B's copy opening when A's backend is already served
      cfg = StoreConfig.loadStoreConfig(flatB / StoreConfig.configFilename)
      _ = host.control.copyRegistry.opening(localB, s, 1000, false, flatB) should be(true)
      backend = new RocksDBBackend(flatB, s, TestDeviceIo.of(flatB)):
        override def hasFailed: Boolean = true
        override def close(): Future[Unit] = gate.future.flatMap(_ => super.close())
      _ = host.control.loader.handleBackendOpened(Host.BackendOpened(localB, cfg, flatB, false, None,
        Success(backend), None, None))
      _ <- pump(host, false, rounds = 20)
      heldActivity = recordOf(host, localB, s).map(_.activity)
      heldPhase = CopyFixtures.phaseOf(devB, s)
      _ = gate.success(())
      _ <- pump(host, CopyFixtures.phaseOf(devB, s).contains(Phase.Lost))
    yield
      heldActivity should be(Some(Activity.Closing(ClosePurpose.Unserved)))
      heldPhase should be(Some(Phase.Live))
      inLost(devB, s, flatB)
      recordOf(host, localB, s).map(_.activity) should be(Some(Activity.Idle))
      host.control.router.get(s).map(_.backend.path).exists(_.startsWith(dev)) should be(true)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // Final review M1: a copy closed for a failed CRL-state load (ClosePurpose.Reload) ends AwaitingRetry, not Idle, so
  // its reload is paced by the load retry's backoff on the device scans, never made at each device check
  atest("a failed CRL-state load is retried with backoff, not at every device check"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    CrashRecoveryLog.saveStoreState(s, StoreRecoveryState.Empty, Backend.crlSaveFile(flat))
    val factory = new FlakyCrlFactory
    factory.crl.failLoads.set(Int.MaxValue)
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, crlFactory = factory)
    val active = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
    def local = loadedDevices(host)(deviceA)
    def waitScans: Option[Int] = host.control.copyRegistry.retryOn(local, s).map(_.waitScans)
    def awaiting(scans: Int): Boolean = waitScans.contains(scans) && !host.control.loader.isOpening(s)
    for
      _ <- pump(host, awaiting(1))
      opensAtLoad = opener.opens.get
      firstWait = waitScans
      // Device checks leave it to its retry
      _ <- check(host, Map(s -> active))
      _ <- check(host, Map(s -> active))
      _ <- pump(host, false, rounds = 20)
      opensAfterChecks = opener.opens.get
      // Its retry, due at the next scan, fails again and doubles its wait
      _ = host.control.loader.retryFailedLoads()
      _ <- pump(host, awaiting(2))
      opensAfterRetry = opener.opens.get
      secondWait = waitScans
      // The next scan only counts down; the one after retries
      _ = host.control.loader.retryFailedLoads()
      _ <- pump(host, false, rounds = 20)
      opensWhileWaiting = opener.opens.get
      _ <- check(host, Map(s -> active))
      opensAfterCheckWhileWaiting = opener.opens.get
      _ = host.control.loader.retryFailedLoads()
      _ <- pump(host, awaiting(4))
    yield
      opensAtLoad should be(1)
      firstWait should be(Some(1))
      opensAfterChecks should be(1)
      opensAfterRetry should be(2)
      secondWait should be(Some(2))
      opensWhileWaiting should be(2)
      opensAfterCheckWhileWaiting should be(2)
      opener.opens.get should be(3)
      waitScans should be(Some(4))
      host.control.router.loaded.contains(s) should be(false)
      os.exists(Backend.crlSaveFile(flat)) should be(true)

  atest("a failed backend opened while another serves the store moves to lost after its close"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    host.testingOnlyHandleEvents()
    // Served from another device of the host; its copy on device A is opened, found failed, and closed unserved
    installServedElsewhere(host, s)
    val flat = rocksCopy(dev, s)
    host.testingOnlyRescan(deviceA)
    val sds = loadedDevices(host)(deviceA)
    val gate = Promise[Unit]()
    opener.failing = true
    opener.closeGate = Some(gate.future)
    host.control.loader.startOpen(sds, StoreConfig.loadStoreConfig(flat / StoreConfig.configFilename), flat,
      create = false, None)
    for
      _ <- pump(host, false, rounds = 20)
      heldPhase = CopyFixtures.phaseOf(dev, s)
      _ = gate.success(())
      _ <- pump(host, CopyFixtures.phaseOf(dev, s).contains(Phase.Lost))
    yield
      heldPhase should be(Some(Phase.Live))
      inLost(dev, s, flat)

  // Final review M2.1, as the failed unserved backend's hold (HostCopies.closingUnserved) leaves it: a new open of
  // the copy while the failed backend closes is refused (Closing(Unserved) -> Opening), not raced with the move. No
  // production path opens a held copy (CopyDecision's Load needs it Idle, and a retry is parked while it closes), so
  // the open is asked for by hand and its refusal is a registry violation. The copy moves to lost/ once the close ends
  atest("a new open of a failed unserved backend's copy while it closes is refused, and the copy moves to lost"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    host.testingOnlyHandleEvents()
    installServedElsewhere(host, s)
    val flat = rocksCopy(dev, s)
    host.testingOnlyRescan(deviceA)
    val sds = loadedDevices(host)(deviceA)
    val cfg = StoreConfig.loadStoreConfig(flat / StoreConfig.configFilename)
    val closing = Promise[Unit]()
    opener.failing = true
    opener.closeGate = Some(closing.future)
    host.control.loader.startOpen(sds, cfg, flat, create = false, None)
    for
      _ <- pump(host, false, rounds = 20)
      _ = opener.failing = false
      _ = opener.closeGate = None
      opensBefore = opener.opens.get
      _ = host.control.loader.startOpen(sds, cfg, flat, create = false, None)
      refusals = host.control.copyRegistry.takeRegistryErrors()
      opensAfter = opener.opens.get
      _ = closing.success(())
      _ <- pump(host, CopyFixtures.phaseOf(dev, s).contains(Phase.Lost))
    yield
      refusals.exists(_.contains("Closing(Unserved) -> Opening")) should be(true)
      opensAfter should be(opensBefore)
      host.control.moves.isPendingLost(sds, s) should be(false)
      inLost(dev, s, flat)

  atest("a BackendStateLost open moves the copy to lost"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    val opener = new TestOpener
    opener.stateLost = true
    val host = realHost(hostRoot, opener)
    for
      _ <- pump(host, CopyFixtures.phaseOf(dev, s).contains(Phase.Lost))
      opensBefore = opener.opens.get
      // Never loaded from lost/: a load offered there reads Lost and opens nothing
      _ = host.control.loader.tryLoadStore(loadedDevices(host)(deviceA), CopyFixtures.copyPath(dev, s).get)
      _ <- pump(host, false, rounds = 5)
    yield
      opensBefore should be(1)
      opener.opens.get should be(1)
      inLost(dev, s, flat)
      host.control.copyRegistry.copiesOf(loadedDevices(host)(deviceA)).flatMap(_.copies.get(s)).map(_.phase) should be(
        Some(Phase.Lost))

  atest("a lost device CRL moves every copy that does not hold its own state to lost"):
    val (hostRoot, dev) = device(withCrl = false)
    val s1 = StoreId(s.poolId, 1)
    val flat0 = writeStoreConfig(dev, s)
    val flat1 = writeStoreConfig(dev, s1)
    // s1's whole state is its own valid save file
    CrashRecoveryLog.saveStoreState(s1, StoreRecoveryState.Empty, Backend.crlSaveFile(flat1))
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val attempts = recorded(host).storeLoadAttempts.toList.map(_._2)
    CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
    CopyFixtures.phaseOf(dev, s1) should be(Some(Phase.Live))
    inLost(dev, s, flat0)
    CopyFixtures.copyPath(dev, s1) should be(Some(flat1))
    loadedDevices(host).keySet should be(Set(deviceA))
    // Neither lost/ nor any copy in it is ever offered for a load; the Live copy is
    attempts should contain(flat1)
    attempts.filter(p => p == lostDir(dev) || p / os.up == lostDir(dev)) should be(Nil)
    Future.successful(succeed)

  // Final review I1: a crash undoing an unforced move to lost/ would put the copy back in live/ over the empty
  // replacement CRL. The device stays unloaded, its CRL untouched, and the next scan forces the move before replacing it
  atest("a lost device CRL whose move to lost is not forced leaves the CRL and the device for the next scan"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val dev = writeDevice(hostRoot, "dev0", deviceA)
    writeStoreConfig(dev, s)
    val crlDir = CrashRecoveryLog.deviceCrlDir(dev)
    @volatile var failSyncs = 1
    // Each directory forced, and whether the device's CRL directory existed then
    @volatile var syncs = Vector.empty[(os.Path, Boolean)]
    host.control.moves.syncDirectory = d =>
      syncs :+= (d, os.exists(crlDir))
      if d == lostDir(dev) && failSyncs > 0 then
        failSyncs -= 1
        throw new java.io.IOException("injected sync")
      DurableFiles.syncDirectory(d)
    discoverNow(host)
    host.testingOnlyHandleEvents()
    val loadedFirst = loadedDevices(host).contains(deviceA)
    val rootAfterFirst = os.list(dev).map(_.last).toSet
    val phaseAfterFirst = CopyFixtures.phaseOf(dev, s)
    syncs = Vector.empty
    discoverNow(host)
    host.testingOnlyHandleEvents()
    Future.successful:
      failSyncs should be(0)
      loadedFirst should be(false)
      phaseAfterFirst should be(Some(Phase.Lost))
      rootAfterFirst.filter(_.startsWith("crl")) should be(Set())
      loadedDevices(host).contains(deviceA) should be(true)
      // The second scan forced lost/ and live/ before any CRL directory was there
      syncs.filter(_._1 == lostDir(dev)).map(_._2).headOption should be(Some(false))
      syncs.filter(_._1 == dev / CopyLayout.LiveDirectory).map(_._2).headOption should be(Some(false))
      os.exists(crlDir) should be(true)
      recordOf(host, loadedDevices(host)(deviceA), s).map(r => (r.phase, r.unsynced)) should be(
        Some((Phase.Lost, false)))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("retire moves lost to pending-delete under the copy's name"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val placed = CopyFixtures.placeCopy(dev, s, Phase.Lost)
    val copyId = CopyLayout.parseName(placed.last).get._2
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val registered = host.control.copyRegistry.copiesOf(loadedDevices(host)(deviceA)).flatMap(_.copies.get(s))
    for
      // Its entry has left the device: the Deleted Stores pass retires it
      _ <- check(host)
      _ <- pump(host, CopyFixtures.retiredCopies(dev, s).nonEmpty)
    yield
      registered.map(r => (r.phase, r.copyId)) should be(Some((Phase.Lost, copyId)))
      val retired = CopyFixtures.retiredCopies(dev, s)
      retired.map(_.last) should be(List(CopyLayout.copyName(s, copyId)))
      os.exists(retired.head / RebuildingStore.LostCrlStateFile) should be(true)
      CopyFixtures.copyPath(dev, s) should be(None)
      host.control.copyRegistry.copiesOf(loadedDevices(host)(deviceA)).flatMap(_.copies.get(s)) should be(None)

  atest("a crash at each retire step leaves the copy in lost and the next retire finishes"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    CopyFixtures.placeCopy(dev, s, Phase.Lost)
    val factory = new FlakyCrlFactory
    val host = newHost(hostRoot, crlFactory = factory)
    host.testingOnlyHandleEvents()
    @volatile var failRenames = 0
    host.control.moves.rename = (from, to) =>
      if (to / os.up).last == RebuildingStore.PendingDeleteDirectory && failRenames > 0 then
        failRenames -= 1
        throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    def phase = CopyFixtures.phaseOf(dev, s)
    def retired = CopyFixtures.retiredCopies(dev, s)
    // Step 2 fails: its CRL state cannot be read
    factory.crl.failReads.set(1)
    for
      _ <- check(host)
      afterRead = (phase, retired, os.exists(CopyFixtures.copyPath(dev, s).get / RebuildingStore.LostCrlStateFile))
      // Step 3 fails: its device-CRL entries cannot be dropped
      _ = factory.crl.failDrops.set(1)
      _ <- check(host)
      afterDrop = (phase, retired, os.exists(CopyFixtures.copyPath(dev, s).get / RebuildingStore.LostCrlStateFile))
      // Step 4 fails: the rename into pending-delete/
      _ = failRenames = 1
      _ <- check(host)
      afterRename = (phase, retired)
      _ <- check(host)
      _ <- pump(host, retired.nonEmpty)
    yield
      afterRead should be((Some(Phase.Lost), Nil, false))
      afterDrop should be((Some(Phase.Lost), Nil, true))
      afterRename should be((Some(Phase.Lost), Nil))
      failRenames should be(0)
      phase should be(None)
      retired should have size 1
      os.exists(retired.head / RebuildingStore.LostCrlStateFile) should be(true)

  atest("a failed move to lost holds the copy offline and the next check moves it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    val opener = new TestOpener
    opener.stateLost = true
    @volatile var failRenames = 1
    val rename: (os.Path, os.Path) => Unit = (from, to) =>
      if (to / os.up).last == CopyLayout.LostDirectory && failRenames > 0 then
        failRenames -= 1
        throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    val host = realHost(hostRoot, opener, Some(rename))
    host.testingOnlyHandleEvents()
    val sds = loadedDevices(host)(deviceA)
    val heldPhase = CopyFixtures.phaseOf(dev, s)
    val held = host.control.moves.pendingLost(sds)
    val opensAtLoad = opener.opens.get
    // Held offline: a load offered meanwhile opens nothing, no pass may act on it, and a retire refuses it
    host.control.loader.tryLoadStore(sds, flat)
    val opensAfterOffer = opener.opens.get
    val offline = host.control.copyRegistry.offlineCopies(sds)
    val active = Map(s -> org.aspen_ddp.aspen.common.metadata.StorageDeviceState.StoreEntry(
      org.aspen_ddp.aspen.common.metadata.StorageDeviceState.StoreStatus.Active, None))
    for
      retiredWhileHeld <- settled(host, host.control.copies.retire(sds, s))
      _ <- check(host, active)
    yield
      heldPhase should be(Some(Phase.Live))
      held should be(Set(s))
      opensAtLoad should be(1)
      opensAfterOffer should be(1)
      offline should not contain s
      retiredWhileHeld should be(false)
      failRenames should be(0)
      host.control.moves.pendingLost(sds) should be(Set())
      inLost(dev, s, flat)
      opener.opens.get should be(1)

  test("a move refuses a copy that is opening, served or closing, and keeps a copy unsynced that was"):
    val (hostRoot, dev) = device()
    writeStoreConfig(dev, s)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val sds = loadedDevices(host)(deviceA)
    val copies = host.control.copyRegistry.copiesOf(sds).get
    def record = copies.copies(s)
    for activity <- List(Activity.Opening(1), Activity.Served, Activity.Closing(ClosePurpose.FailStop)) do
      copies.applyActivity(s, activity)
      CopyFixtures.moveNow(host, "test", sds, s, Phase.Lost) shouldBe a[MoveResult.NotMoved]
      record.phase should be(Phase.Live)
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Live))
      copies.applyActivity(s, Activity.Idle)
    // Unsynced by an earlier change a crash could undo: a durable move forces only the directories naming the copy
    copies.setUnsynced(s, true)
    CopyFixtures.moveNow(host, "test", sds, s, Phase.Lost) should be(MoveResult.Durable)
    record.phase should be(Phase.Lost)
    record.unsynced should be(true)
    CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
    // The device check's resync forces the copy itself, and clears it
    host.control.moves.resync(sds)(())
    host.testingOnlyHandleEvents()
    record.unsynced should be(false)
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- Task 3: Outbound is outbound/ ----

  atest("a transfer out moves the closed copy to outbound with its transfer id and ships it from there"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(dev => writeStoreConfig(dev, shipped): Unit, sends)
      flat = CopyFixtures.livePathOf(dev, shipped)
      started <- askAndSettle(host)
    yield
      started should be(Success(()))
      sends.sources.map(_._1) should be(Vector(shipped))
      val from = sends.sources.head._2
      from / os.up should be(dev / CopyLayout.OutboundDirectory)
      CopyLayout.parseName(from.last).map(_._1) should be(Some(shipped))
      CopyLayout.readParam(from, Phase.Outbound("")) should be(tid)
      CopyFixtures.phaseOf(dev, shipped) should be(Some(Phase.Outbound(tid)))
      CopyFixtures.copyPath(dev, shipped) should be(Some(from))
      os.exists(flat) should be(false)
      recordOf(host, local, shipped).map(r => (r.phase, r.unsynced)) should be(Some((Phase.Outbound(tid), false)))
      host.control.copyRegistry.pathOf(local, shipped) should be(Some(from))

  atest("an aborted transfer out moves the copy back to live and serves it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    CopyFixtures.markCopy(dev, s, Phase.Outbound("t"))
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    host.testingOnlyHandleEvents()
    val outboundAtLoad = CopyFixtures.phaseOf(dev, s)
    val opensAtLoad = opener.opens.get
    val active = Map(s -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
    for
      _ <- check(host, active)
      _ <- pump(host, host.control.router.loaded.contains(s))
    yield
      outboundAtLoad should be(Some(Phase.Outbound("t")))
      opensAtLoad should be(0)
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Live))
      CopyFixtures.copyPath(dev, s) should be(Some(flat))
      os.exists(dev / CopyLayout.OutboundDirectory) && os.list(dev / CopyLayout.OutboundDirectory).nonEmpty should be(
        false)
      host.control.router.loaded.contains(s) should be(true)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("an Unconfirmed rebuilt copy transferred out and then reinstated ends Live and loads"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(dev => CopyFixtures.placeCopy(dev, shipped, Phase.Rebuilt("ep")): Unit,
                                              sends)
      flat = CopyFixtures.livePathOf(dev, shipped)
      started <- askAndSettle(host)
      outbound = CopyFixtures.phaseOf(dev, shipped)
      shippedFrom = sends.sources.head._2
      _ <- client.abortStoreTransfer(shipped)
      _ <- waitForTransactionsToComplete()
      _ = sends.endTransferOut(shipped)
      _ = host.control.transfersOut.reinstateAbandonedOut(local, shipped)
      _ = host.testingOnlyHandleEvents()
    yield
      started should be(Success(()))
      outbound should be(Some(Phase.Outbound(tid)))
      shippedFrom / os.up should be(dev / CopyLayout.OutboundDirectory)
      CopyFixtures.phaseOf(dev, shipped) should be(Some(Phase.Live))
      CopyFixtures.copyPath(dev, shipped) should be(Some(flat))
      recordOf(host, local, shipped).map(_.phase) should be(Some(Phase.Live))
      recorded(host).storeLoadAttempts.toList should contain((source, flat))

  atest("startOut refuses a copy that is not Live, Rebuilt or Parked"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, _) <- transferring(dev => CopyFixtures.placeCopy(dev, shipped, Phase.Arrived("t-in")): Unit,
                                            sends)
      started <- askAndSettle(host)
    yield
      started shouldBe a[Failure[?]]
      started.failed.get.getMessage should include("never shipped")
      sends.sources should be(Vector())
      CopyFixtures.phaseOf(dev, shipped) should be(Some(Phase.Arrived("t-in")))
      recordOf(host, local, shipped).map(_.phase) should be(Some(Phase.Arrived("t-in")))

  atest("a sync that fails after the move to outbound ships nothing until resynced"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    @volatile var failures = 0
    for
      (host, local, dev, tid) <- transferring(dev => writeStoreConfig(dev, shipped): Unit, sends)
      outboundDir = dev / CopyLayout.OutboundDirectory
      _ = failures = 1
      _ = host.control.moves.syncDirectory = d =>
            if d == outboundDir && failures > 0 then
              failures -= 1
              throw new java.io.IOException("injected sync")
            DurableFiles.syncDirectory(d)
      // The close's end checks the device, whose resync would force the copy: held, so the retransmit below meets
      // the copy unsynced
      _ = recorded(host).armLookup(source)
      first <- askAndSettle(host)
      checkedAfterFirst = host.control.reconciler.activeDeviceChecks.contains(source)
      shippedAfterFirst = sends.sources
      phaseAfterFirst = CopyFixtures.phaseOf(dev, shipped)
      unsyncedAfterFirst = recordOf(host, local, shipped).map(r => (r.phase, r.unsynced))
      // A retransmit while it is unsynced ships nothing either
      second <- askAndSettle(host)
      shippedAfterSecond = sends.sources
      _ = host.control.moves.resync(local)(())
      resyncSubmitted = host.control.moves.resyncRunning(dev)
      // The resync's job and its apply
      _ = host.testingOnlyHandleEvents()
      unsyncedAfterResync = recordOf(host, local, shipped).map(_.unsynced)
      third <- askAndSettle(host)
    yield
      first should be(Success(()))
      checkedAfterFirst should be(true)
      failures should be(0)
      shippedAfterFirst should be(Vector())
      phaseAfterFirst should be(Some(Phase.Outbound(tid)))
      unsyncedAfterFirst should be(Some((Phase.Outbound(tid), true)))
      second shouldBe a[Failure[?]]
      shippedAfterSecond should be(Vector())
      resyncSubmitted should be(true)
      unsyncedAfterResync should be(Some(false))
      third should be(Success(()))
      sends.sources.map(_._1) should be(Vector(shipped))
      sends.sources.head._2 should be(CopyFixtures.copyPath(dev, shipped).get)
      sends.sources.head._2 / os.up should be(outboundDir)
      recordOf(host, local, shipped).map(r => (r.phase, r.unsynced)) should be(Some((Phase.Outbound(tid), false)))

  atest("a copy whose move to lost failed is never shipped"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, _) <- transferring(dev => writeStoreConfig(dev, shipped): Unit, sends)
      _ = host.control.moves.holdLost(local, shipped, held = true)
      started <- askAndSettle(host)
    yield
      started shouldBe a[Failure[?]]
      started.failed.get.getMessage should include("Lost")
      sends.sources should be(Vector())
      CopyFixtures.phaseOf(dev, shipped).exists(_.isInstanceOf[Phase.Outbound]) should be(false)

  atest("a parked copy can be transferred out"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(dev => CopyFixtures.placeCopy(dev, shipped, Phase.Parked): Unit, sends)
      parkedAt = CopyFixtures.copyPath(dev, shipped).get
      started <- askAndSettle(host)
    yield
      parkedAt / os.up should be(dev / CopyLayout.ParkedDirectory)
      started should be(Success(()))
      sends.sources.map(_._1) should be(Vector(shipped))
      val from = sends.sources.head._2
      from / os.up should be(dev / CopyLayout.OutboundDirectory)
      // One copy, moved: the same copy id
      from.last should be(parkedAt.last)
      CopyLayout.readParam(from, Phase.Outbound("")) should be(tid)
      CopyFixtures.phaseOf(dev, shipped) should be(Some(Phase.Outbound(tid)))
      os.list(dev / CopyLayout.ParkedDirectory) should be(Nil)
      recordOf(host, local, shipped).map(r => (r.phase, r.unsynced)) should be(Some((Phase.Outbound(tid), false)))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- Task 3 fix round 1 ----

  /** Places `shipped` in outbound/ for transfer `t`, with the valid save file its transfer's close left */
  private def placeOutbound(t: String)(dev: os.Path): Unit =
    val dir = CopyFixtures.placeCopy(dev, shipped, Phase.Outbound(t))
    CrashRecoveryLog.saveStoreState(shipped, StoreRecoveryState.Empty, Backend.crlSaveFile(dir))

  atest("a copy outbound for an earlier transfer goes back to live and out again with the new transfer's id"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(placeOutbound("old"), sends)
      before = CopyFixtures.copyPath(dev, shipped).get
      flat = CopyFixtures.livePathOf(dev, shipped)
      renames = recordRenames(host)
      started <- askAndSettle(host)
    yield
      started should be(Success(()))
      renames() should be(Vector(before -> flat, flat -> before))
      sends.sources should be(Vector(shipped -> before))
      CopyLayout.readParam(before, Phase.Outbound("")) should be(tid)
      CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(before), shipped).isRight should be(true)
      recordOf(host, local, shipped).map(r => (r.phase, r.unsynced)) should be(Some((Phase.Outbound(tid), false)))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a copy outbound for an earlier transfer whose move back out fails is left live with its save file"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    @volatile var failOut = true
    for
      (host, local, dev, tid) <- transferring(placeOutbound("old"), sends)
      flat = CopyFixtures.livePathOf(dev, shipped)
      _ = recordRenames(host, (_, to) => failOut && (to / os.up).last == CopyLayout.OutboundDirectory)
      first <- askAndSettle(host)
      shippedAfterFirst = sends.sources
      pathAfterFirst = CopyFixtures.copyPath(dev, shipped)
      phaseAfterFirst = recordOf(host, local, shipped).map(_.phase)
      saveFileKept = os.exists(Backend.crlSaveFile(flat)) &&
        CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(flat), shipped).isRight
      // The receiver's retransmit: the Live copy, its state in its save file, ships
      _ = failOut = false
      second <- askAndSettle(host)
    yield
      first should be(Success(()))
      shippedAfterFirst should be(Vector())
      pathAfterFirst should be(Some(flat))
      phaseAfterFirst should be(Some(Phase.Live))
      saveFileKept should be(true)
      second should be(Success(()))
      sends.sources.map(_._1) should be(Vector(shipped))
      CopyLayout.readParam(sends.sources.head._2, Phase.Outbound("")) should be(tid)
      recordOf(host, local, shipped).map(_.phase) should be(Some(Phase.Outbound(tid)))

  atest("a copy that must move is not moved while an earlier send of its store is registered"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(placeOutbound("old"), sends)
      before = CopyFixtures.copyPath(dev, shipped).get
      // The earlier transfer's send, still running: its archiver may be reading the copy
      _ = sends.beginTransferOut(shipped, UUID.randomUUID(), before, "127.0.0.1", 1, HLCTimestamp.now)
      renames = recordRenames(host)
      whileSending <- askAndSettle(host)
      afterRefusal = (CopyFixtures.copyPath(dev, shipped), CopyFixtures.phaseOf(dev, shipped), renames())
      _ = sends.endTransferOut(shipped)
      afterEnd <- askAndSettle(host)
      // A retransmit of this transfer while its own send runs moves nothing, and ships again
      retransmit <- askAndSettle(host)
    yield
      whileSending shouldBe a[Failure[?]]
      whileSending.failed.get.getMessage should include("earlier send")
      afterRefusal should be((Some(before), Some(Phase.Outbound("old")), Vector()))
      afterEnd should be(Success(()))
      retransmit should be(Success(()))
      sends.sources should be(Vector.fill(3)(shipped -> before))
      CopyLayout.readParam(before, Phase.Outbound("")) should be(tid)
      renames().size should be(2)

  atest("a move out of live drops the copy's load retry, and a move into live retargets it"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(dev => writeStoreConfig(dev, shipped): Unit, sends)
      flat = CopyFixtures.livePathOf(dev, shipped)
      claimed = host.control.copyRegistry.awaitingRetry("test", shipped, Host.LoadRetry(local, flat, 1, 1))
      started <- askAndSettle(host)
      afterShip = (recordOf(host, local, shipped).map(_.activity), host.control.copyRegistry.retryOn(local, shipped))
      // Its send ends and the transfer is abandoned: a retry the outbound copy awaits follows it back to Live
      outbound = CopyFixtures.copyPath(dev, shipped).get
      _ = sends.endTransferOut(shipped)
      _ = host.control.copyRegistry.awaitingRetry("test", shipped, Host.LoadRetry(local, outbound, 1, 1))
      _ = host.control.transfersOut.reinstateAbandonedOut(local, shipped)
      _ = host.testingOnlyHandleEvents()
    yield
      claimed should be(true)
      started should be(Success(()))
      afterShip should be((Some(Activity.Idle), None))
      host.control.copyRegistry.retryOn(local, shipped).map(_.storePath) should be(Some(flat))
      CopyFixtures.copyPath(dev, shipped) should be(Some(flat))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- Task 4: Rebuilt is rebuilt/, Parked is parked/ ----

  atest("an Adopt flip moves the rebuilt copy to live and serves it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, rebuilds = new PlacingRebuilds(real = true))
    host.testingOnlyHandleEvents()
    val renames = recordRenames(host)
    def flat = CopyFixtures.livePathOf(dev, s)
    for
      outcome <- rebuildAndFlip(host, UUID.randomUUID())
      _ <- pump(host, host.control.router.loaded.contains(s))
    yield
      outcome should be(RebuildOutcome.Adopt)
      renames().map((from, to) => ((from / os.up).last, to)) should be(Vector(CopyLayout.RebuiltDirectory -> flat))
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Live))
      CopyFixtures.copyPath(dev, s) should be(Some(flat))
      inDir(dev, CopyLayout.RebuiltDirectory) should be(Nil)
      host.control.router.loaded.contains(s) should be(true)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  /** As PlacingRebuilds(real = true), but its first placement's rename is reported landed and not forced (as
   *  RebuildingStore.finish reports a MoveNotForced); a rebuild started over its own episode's Rebuilt copy completes
   *  with that copy at once, as RebuildingStore.start does. Counts its placements */
  private class UnforcedRebuilds extends StoreRebuildFactory:
    val placements = new AtomicInteger(0)
    def createRebuild(client: org.aspen_ddp.aspen.client.AspenClient, storeId: StoreId, deviceId: StorageDeviceId,
                      devicePath: os.Path, backends: org.aspen_ddp.aspen.server.store.backend.BackendOpener, io: org.aspen_ddp.aspen.server.host.DeviceIo,
                      episode: Option[UUID], existingCopy: Option[(Phase, os.Path)]): StoreRebuild =
      val id = storeId
      val rebuilt = Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode))
      val (copy, unforced) = existingCopy match
        case Some((phase, path)) if phase == rebuilt => (path, None)
        case _ =>
          val n = placements.incrementAndGet()
          val flat = rocksCopy(devicePath, id)
          RebuildingStore.writeEmptyCrlState(flat, id)
          val placed = CopyFixtures.markCopy(devicePath, id, rebuilt)
          // The staging area it was renamed from, which the resync forces
          os.makeDir.all(devicePath / RebuildingStore.RebuildDirectory)
          (placed, Option.when(n == 1)(new DurableFiles.MoveNotForced(
            devicePath / RebuildingStore.RebuildDirectory / id.directoryName, placed,
            new java.io.IOException("injected sync"))))
      new StoreRebuild:
        val storeId: StoreId = id
        def complete: Future[UUID] = Future.successful(CopyFixtures.copyIdOf(copy))
        override def placedUnforced: Option[DurableFiles.MoveNotForced] = unforced
        def cancel(): Unit = ()

  // Final review I2: a finish whose rename into rebuilt/ landed unforced is recorded unsynced, flipped only once a device
  // check has forced it, and never rebuilt a second time
  atest("a rebuilt copy placed unforced is recorded unsynced, forced, then flipped and served: one rebuild"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val opener = new TestOpener
    val rebuilds = new UnforcedRebuilds
    val host = realHost(hostRoot, opener, rebuilds = rebuilds)
    host.testingOnlyHandleEvents()
    val episode = UUID.randomUUID()
    val entry = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode))
    @volatile var syncs = Vector.empty[os.Path]
    host.control.moves.syncDirectory = d =>
      syncs :+= d
      DurableFiles.syncDirectory(d)
    def record = recordOf(host, loadedDevices(host)(deviceA), s)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(s, episode))
      _ <- waitForTransactionsToComplete()
      _ = host.control.rebuilds.startRebuild(loadedDevices(host)(deviceA), s, Some(episode), acceptLoss = false)
      _ <- pumpTx(host, !host.control.copyRegistry.rebuilding(HostCopies.RebuildHolds).contains(s), rounds = 50)
      afterFinish = (record.map(r => (r.phase, r.unsynced)), recorded(host).rebuildOutcomes.get((deviceA, s)))
      _ <- check(host, Map(s -> entry))
      _ <- pumpTx(host, host.control.router.loaded.contains(s))
    yield
      afterFinish should be((Some((Phase.Rebuilt(episode.toString), true)), None))
      syncs should contain(dev / CopyLayout.RebuiltDirectory)
      syncs should contain(dev / RebuildingStore.RebuildDirectory)
      recorded(host).rebuildOutcomes.get((deviceA, s)) should be(Some(RebuildOutcome.Adopt))
      rebuilds.placements.get should be(1)
      record.map(r => (r.phase, r.unsynced)) should be(Some((Phase.Live, false)))
      host.control.router.loaded.contains(s) should be(true)
      copyDirs(dev, s).size should be(1)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("an AdoptOffline flip moves the rebuilt copy to parked in one move"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuilds(real = false))
    host.testingOnlyHandleEvents()
    val renames = recordRenames(host)
    for
      outcome <- rebuildAndFlip(host, UUID.randomUUID(), StorageDeviceState.DeviceStatus.ShuttingDown)
    yield
      outcome should be(RebuildOutcome.AdoptOffline)
      renames().map((from, to) => ((from / os.up).last, (to / os.up).last)) should be(
        Vector(CopyLayout.RebuiltDirectory -> CopyLayout.ParkedDirectory))
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Parked))
      inDir(dev, CopyLayout.RebuiltDirectory) should be(Nil)
      inDir(dev, CopyLayout.OutboundDirectory) should be(Nil)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a parked copy goes live when its entry is Active with no transfer device"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val flat = rocksCopy(dev, s)
    RebuildingStore.writeEmptyCrlState(flat, s)
    CopyFixtures.markCopy(dev, s, Phase.Parked)
    val opener = new TestOpener
    val host = realHost(hostRoot, opener)
    host.testingOnlyHandleEvents()
    val parkedAtLoad = CopyFixtures.phaseOf(dev, s)
    val opensAtLoad = opener.opens.get
    val active = Map(s -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
    for
      _ <- check(host, active)
      _ <- pump(host, host.control.router.loaded.contains(s))
    yield
      parkedAtLoad should be(Some(Phase.Parked))
      opensAtLoad should be(0)
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Live))
      CopyFixtures.copyPath(dev, s) should be(Some(flat))
      inDir(dev, CopyLayout.ParkedDirectory) should be(Nil)
      host.control.router.loaded.contains(s) should be(true)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a Discard deletes the rebuilt copy rename-first"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    // The bootstrap device's entry does not list the store, so its flip discards the copy
    val dev = writeDevice(hostRoot, "dev0", source)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuilds(real = false))
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    @volatile var placedAt: Option[os.Path] = None
    host.control.rebuilds.startRebuild(local, s, Some(UUID.randomUUID()), acceptLoss = false)
    placedAt = CopyFixtures.copyPath(dev, s)
    for
      _ <- pumpTx(host, recorded(host).rebuildOutcomes.contains((source, s)) &&
                        !host.control.copyRegistry.rebuilding(HostCopies.RebuildHolds).contains(s))
    yield
      placedAt.map(_ / os.up) should be(Some(dev / CopyLayout.RebuiltDirectory))
      recorded(host).rebuildOutcomes((source, s)) should be(RebuildOutcome.Discard)
      CopyFixtures.copyPath(dev, s) should be(None)
      inDir(dev, CopyLayout.RebuiltDirectory) should be(Nil)
      inDir(dev, RebuildingStore.PendingDeleteDirectory) should be(Nil)
      // Renamed into deleting/ first, then removed there
      os.isDir(dev / StoreCopies.DeletingDirectory) should be(true)
      os.list(dev / StoreCopies.DeletingDirectory) should be(Nil)
      recordOf(host, local, s) should be(None)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a shipped rebuilt copy arrives and goes live as any arrival"):
    given ExecutionContext = executionContext
    val moving = StoreId(PoolId.BootstrapPoolId, 1)
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", net.secondDeviceId)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    val transfers = new StagedTransferService
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, transfers = transfers)
    host.testingOnlyHandleEvents()
    @volatile var seen = Vector.empty[Phase]
    host.control.copyRegistry.observer = (copies, sid) =>
      if sid == moving && copies.deviceId == net.secondDeviceId then
        copies.copies.get(sid).map(_.phase).foreach(p => if !seen.lastOption.contains(p) then seen :+= p)
    val tid = UUID.randomUUID().toString
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(moving, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      pstate <- client.getStoragePoolState(moving.poolId)
      _ = checkAllNow(host)
      _ <- pumpTx(host, transfers.startedTransfersIn.contains(moving))
      // The source's copy as it ships from outbound/ after a rebuild whose flip was Unconfirmed: a real store, its
      // Outbound transfer id and its episode in their phase-param files, and its empty save file
      _ = transfers.finishTransferIn(moving, extraFiles = CopyLayout.paramFile(Phase.Rebuilt("")).toList,
            prepare = staged =>
              DefaultBackendOpener.createStore(StoreConfig.forPool(moving, pstate), staged,
                staged / os.up / StoreCreation.CreatingDirectory))
      _ <- pumpTx(host, host.control.router.loaded.contains(moving))
    yield
      recorded(host).transferOutcomes.get(moving) should be(Some(TransferOutcome.PoolRepointed))
      seen.map { case Phase.Arrived(_) => "Arrived"; case p => p.toString } should be(Vector("Arrived", "Live"))
      CopyFixtures.phaseOf(dev, moving) should be(Some(Phase.Live))
      val live = CopyFixtures.copyPath(dev, moving).get
      live should be(CopyFixtures.livePathOf(dev, moving))
      // Inert here: the copy is Live in live/ and served
      CopyLayout.paramFile(Phase.Rebuilt("")).forall(f => os.exists(live / f)) should be(true)
      host.control.router.loaded.contains(moving) should be(true)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a lost device CRL leaves a rebuilt copy alone"):
    val (hostRoot, dev) = device(withCrl = false)
    val episode = UUID.randomUUID().toString
    val s1 = StoreId(s.poolId, 1)
    val s2 = StoreId(s.poolId, 2)
    CopyFixtures.placeCopy(dev, s, Phase.Rebuilt(episode))
    // A parked copy holds its own state only in a valid save file
    val parked = CopyFixtures.placeCopy(dev, s1, Phase.Parked)
    CrashRecoveryLog.saveStoreState(s1, StoreRecoveryState.Empty, Backend.crlSaveFile(parked))
    CopyFixtures.placeCopy(dev, s2, Phase.Parked)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    loadedDevices(host).keySet should be(Set(deviceA))
    CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Rebuilt(episode)))
    CopyFixtures.phaseOf(dev, s1) should be(Some(Phase.Parked))
    CopyFixtures.phaseOf(dev, s2) should be(Some(Phase.Lost))
    Future.successful(succeed)

  // ---- Task 5: Arrived is arrived/, and staging is per attempt ----

  /** A StagedTransferService that, at each beginTransferIn, snapshots what the receiving device `dev` holds of
   *  `moving`: its retired copies, and whether each holds its lost-crl-state.log */
  private class SnapshotTransfers(dev: => os.Path) extends StagedTransferService:
    @volatile var atBegin: Vector[List[(os.Path, Boolean)]] = Vector()
    override def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path,
                                 lane: TransferLane): Future[Unit] =
      atBegin :+= CopyFixtures.retiredCopies(dev, moving).map(p => p -> os.exists(p / RebuildingStore.LostCrlStateFile))
      super.beginTransferIn(storeId, transferUUID, destinationDirectory, lane)

  atest("a received copy is staged under its attempt and promoted into arrived with its transfer id"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    host.testingOnlyHandleEvents()
    recorded(host).holdTransferOutcomes()
    for
      tid <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      (_, attempt, staging) = transfers.transfersIn.head
      _ = transfers.finishTransferIn(moving, prepare = configured)
      _ <- awaitLoudly("the promotion"):
             host.testingOnlyHandleEvents()
             CopyFixtures.copyPath(dev, moving).isDefined
    yield
      staging should be(dev / "transferring-in" / attempt.toString)
      CopyFixtures.phaseOf(dev, moving) should be(Some(Phase.Arrived(tid.toString)))
      val arrived = CopyFixtures.copyPath(dev, moving).get
      arrived / os.up should be(dev / CopyLayout.ArrivedDirectory)
      CopyLayout.readParam(arrived, Phase.Arrived("")) should be(tid.toString)
      // The attempt's staging went with it
      os.exists(staging) should be(false)
      recordOf(host, loadedDevices(host)(net.secondDeviceId), moving).map(_.copyId) should be(
        Some(CopyFixtures.copyIdOf(arrived)))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a committed pool update writes arrived-at, then moves the copy live and serves it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, transfers = transfers)
    host.testingOnlyHandleEvents()
    // Each rename, and whether its source held arrived-at as it was renamed
    @volatile var renames = Vector.empty[(os.Path, os.Path, Boolean)]
    host.control.moves.rename = (from, to) =>
      renames :+= (from, to, os.exists(from / StoreLoader.ArrivedAtFile))
      DurableFiles.atomicRename(from, to)
    for
      _ <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      pstate <- client.getStoragePoolState(moving.poolId)
      _ = transfers.finishTransferIn(moving, prepare = staged =>
            DefaultBackendOpener.createStore(StoreConfig.forPool(moving, pstate), staged,
              staged / os.up / StoreCreation.CreatingDirectory))
      _ <- pumpTx(host, host.control.router.loaded.contains(moving))
    yield
      recorded(host).transferOutcomes.get(moving) should be(Some(TransferOutcome.PoolRepointed))
      val flat = CopyFixtures.livePathOf(dev, moving)
      renames.map((from, to, withArrival) => ((from / os.up).last, to, withArrival)).last should be(
        (CopyLayout.ArrivedDirectory, flat, true))
      CopyFixtures.phaseOf(dev, moving) should be(Some(Phase.Live))
      os.exists(flat / StoreLoader.ArrivedAtFile) should be(true)
      inDir(dev, CopyLayout.ArrivedDirectory) should be(Nil)
      host.control.router.loaded.contains(moving) should be(true)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  for (what, spoil) <- List[(String, os.Path => Unit)](
        "of another transfer" -> (copy =>
          os.write.over(copy / CopyLayout.paramFile(Phase.Arrived("")).get, UUID.randomUUID().toString)),
        "with an unusable save file" -> (copy => os.write.over(Backend.crlSaveFile(copy), Array[Byte](1, 2, 3))))
  do
    atest(s"an arrived copy $what is deleted and the transfer restarted"):
      given ExecutionContext = executionContext
      val (hostRoot, dev) = receivingDevice()
      val transfers = new StagedTransferService
      val host = newHost(hostRoot, transferService = transfers)
      host.testingOnlyHandleEvents()
      // Held, the first host stops where a crash after promotion would: Arrived, the pool still naming the source
      recorded(host).holdTransferOutcomes()
      for
        _ <- startMoving(host)
        _ <- awaitBegun(host, transfers)
        _ = transfers.finishTransferIn(moving, prepare = configured)
        _ <- awaitLoudly("the promotion"):
               host.testingOnlyHandleEvents()
               CopyFixtures.copyPath(dev, moving).isDefined
        _ = spoil(CopyFixtures.copyPath(dev, moving).get)
        transfers2 = new StagedTransferService
        host2 = newHost(hostRoot, transferService = transfers2)
        _ = checkAllNow(host2)
        _ <- awaitBegun(host2, transfers2)
      yield
        transfers2.transfersIn.size should be(1)
        // Deleted rename-first (T13): nothing in arrived/, nothing retired, deleting/ emptied
        CopyFixtures.copyPath(dev, moving) should be(None)
        inDir(dev, CopyLayout.ArrivedDirectory) should be(Nil)
        CopyFixtures.retiredCopies(dev, moving) should be(Nil)
        os.list(dev / StoreCopies.DeletingDirectory) should be(Nil)
        recorded(host2).transferOutcomes.get(moving) should be(None)
        recordOf(host2, loadedDevices(host2)(net.secondDeviceId), moving) should be(None)
        host2.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // Review Focus 2: an earlier tenancy's copy, in any phase but this transfer's Arrived, is retired before the receive:
  // never stuck behind a path, and never received while its CRL entries are unsaved
  for phase <- List(Phase.Lost, Phase.Outbound("old"), Phase.Rebuilt("old"), Phase.Parked) do
    atest(s"a transfer in retires an earlier tenancy's copy in each non-Live phase, then receives ($phase)"):
      given ExecutionContext = executionContext
      val (hostRoot, dev) = receivingDevice(dev => CopyFixtures.placeCopy(dev, moving, phase): Unit)
      val transfers = new SnapshotTransfers(dev)
      val host = newHost(hostRoot, transferService = transfers)
      host.testingOnlyHandleEvents()
      val atLoad = CopyFixtures.phaseOf(dev, moving)
      for
        _ <- startMoving(host)
        _ <- awaitBegun(host, transfers)
      yield
        atLoad should be(Some(phase))
        // Retired, with its CRL state saved, before beginTransferIn was called
        transfers.atBegin.size should be(1)
        transfers.atBegin.head.size should be(1)
        transfers.atBegin.head.head._2 should be(true)
        CopyFixtures.copyPath(dev, moving) should be(None)
        CopyFixtures.retiredCopies(dev, moving) should be(transfers.atBegin.head.map(_._1))
        host.control.transfers.receiving.keySet should be(Set(moving))
        host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a transfer in retires an earlier tenancy's idle Live copy once a fresh read confirms it, then receives"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice(dev => CopyFixtures.placeCopy(dev, moving, Phase.Live): Unit)
    val transfers = new SnapshotTransfers(dev)
    val host = newHost(hostRoot, transferService = transfers)
    host.testingOnlyHandleEvents()
    for
      _ <- startMoving(host)
      _ <- awaitBegun(host, transfers)
    yield
      transfers.atBegin.size should be(1)
      transfers.atBegin.head.map(_._2) should be(List(true))
      CopyFixtures.copyPath(dev, moving) should be(None)
      host.control.transfers.receiving.keySet should be(Set(moving))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // Review fix round 1: a reconcile whose tree read predates this transfer's pool update sees the entry TransferringIn
  // over the Live copy recordTransferIn has just moved there. Its startIn must not take that copy for an earlier
  // tenancy's: the fresh read of the device finds the entry Active
  atest("a Live copy its own transfer moved live is not retired by a startIn read before the pool update"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(net.secondDeviceId)
    for
      tid <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      _ = transfers.finishTransferIn(moving, prepare = configured)
      _ <- pumpTx(host, recorded(host).transferOutcomes.contains(moving) &&
                        !host.control.transfers.receiving.contains(moving))
      liveAfterUpdate = recordOf(host, local, moving).map(r => (r.phase, r.activity))
      // The stale pass: its snapshot read the entry TransferringIn for this transfer
      _ = host.control.transfers.startIn(moving, host.hostId, StorageDeviceId.BootstrapStorageDeviceId,
            net.secondDeviceId, Some(tid))
      // Claimed at once, held across the fresh read; ended, with the device check it asks for, once the read is in
      claimed = host.control.copyRegistry.copiesOf(local).flatMap(_.work.get(moving))
      _ <- awaitLoudly("the stale startIn's fresh read and the check it asks for to end"):
             host.testingOnlyHandleEvents()
             host.control.copyRegistry.copiesOf(local).flatMap(_.work.get(moving)).isEmpty &&
               host.control.reconciler.activeDeviceChecks.isEmpty
      st <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      recorded(host).transferOutcomes.get(moving) should be(Some(TransferOutcome.PoolRepointed))
      liveAfterUpdate should be(Some((Phase.Live, Activity.Idle)))
      claimed should be(Some(Work.TransferIn(None, TransferInStage.RetiringEarlierTenancy)))
      st.stores(moving).status should be(StorageDeviceState.StoreStatus.Active)
      CopyFixtures.phaseOf(dev, moving) should be(Some(Phase.Live))
      CopyFixtures.copyPath(dev, moving) should be(Some(CopyFixtures.livePathOf(dev, moving)))
      CopyFixtures.retiredCopies(dev, moving) should be(Nil)
      inDir(dev, CopyLayout.LostDirectory) should be(Nil)
      transfers.transfersIn.size should be(1)
      host.control.copyRegistry.copiesOf(local).flatMap(_.work.get(moving)) should be(None)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a transfer in takes an earlier tenancy's served Live copy out of service, then retires it and receives"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice(dev => rocksCopy(dev, moving): Unit)
    val transfers = new SnapshotTransfers(dev)
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, transfers = transfers)
    host.testingOnlyHandleEvents()
    val servedAtLoad = host.control.router.loaded.contains(moving)
    @volatile var seen = Vector.empty[Phase]
    @volatile var closings = Vector.empty[Activity]
    host.control.copyRegistry.observer = (copies, sid) =>
      if sid == moving && copies.deviceId == net.secondDeviceId then
        copies.copies.get(sid).map(_.phase).foreach(p => if !seen.lastOption.contains(p) then seen :+= p)
        copies.copies.get(sid).map(_.activity).collect { case c: Activity.Closing => c }.foreach: c =>
          if !closings.lastOption.contains(c) then closings :+= c
    for
      _ <- startMoving(host)
      // The checks that follow are the host's own: the close's end, then the retire's
      _ <- awaitLoudly("the transfer in to begin"):
             host.testingOnlyHandleEvents()
             transfers.transfersIn.nonEmpty
    yield
      servedAtLoad should be(true)
      host.control.router.loaded.contains(moving) should be(false)
      // Closed FailStop, moved to lost/ once closed, then retired with its CRL state before the receive began
      closings should be(Vector(Activity.Closing(ClosePurpose.FailStop)))
      seen should be(Vector(Phase.Live, Phase.Lost, Phase.Retired))
      transfers.atBegin.head.map(_._2) should be(List(true))
      CopyFixtures.copyPath(dev, moving) should be(None)
      host.control.transfers.receiving.keySet should be(Set(moving))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a transfer in waits while an earlier tenancy's Live copy is opening, and its recheck resumes it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice(dev => rocksCopy(dev, moving): Unit)
    val transfers = new SnapshotTransfers(dev)
    val opener = new TestOpener
    val gate = Promise[Unit]()
    opener.openGate = Some(gate.future)
    val host = realHost(hostRoot, opener, transfers = transfers)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(net.secondDeviceId)
    for
      _ <- startMoving(host)
      // The wait asks for its own recheck: the open's end checks nothing
      _ <- pumpTx(host, scheduler(host).pending.contains(Host.TransferOutRecheckDelay))
      recheckWhileOpening = scheduler(host).pending.contains(Host.TransferOutRecheckDelay)
      _ <- pumpTx(host, transfers.transfersIn.nonEmpty, rounds = 10)
      begunWhileOpening = transfers.transfersIn.size
      whileOpening = recordOf(host, local, moving).map(r => (r.phase, r.activity.isInstanceOf[Activity.Opening]))
      _ = gate.success(())
      // No periodic check: the wait scheduled its own device recheck (M5.8), which takes the served copy out of
      // service, and the close's end and the retire's end check the device again
      _ <- awaitLoudly("the transfer in to begin once the open ended, through the scheduled recheck alone"):
             runDeviceRechecks(host)
             transfers.transfersIn.nonEmpty
    yield
      begunWhileOpening should be(0)
      whileOpening should be(Some((Phase.Live, true)))
      recheckWhileOpening should be(true)
      transfers.atBegin.head.map(_._2) should be(List(true))
      CopyFixtures.copyPath(dev, moving) should be(None)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a failed move to live keeps the arrived copy offline and the next check retries it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    host.testingOnlyHandleEvents()
    @volatile var failing = true
    val renames = recordRenames(host, fail = (from, _) => failing && (from / os.up).last == CopyLayout.ArrivedDirectory)
    val local = loadedDevices(host)(net.secondDeviceId)
    for
      tid <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      _ = transfers.finishTransferIn(moving, prepare = configured)
      _ <- pumpTx(host, recorded(host).transferOutcomes.contains(moving) &&
                        !host.control.transfers.receiving.contains(moving))
      phaseAfterFailure = CopyFixtures.phaseOf(dev, moving)
      recordAfterFailure = recordOf(host, local, moving).map(r => (r.phase, r.activity))
      loadsAfterFailure = recorded(host).loadStoreByIdRequests.toList
      offlineAfterFailure = offlineCopies(host, local).contains(moving)
      arrivedAtWritten = CopyFixtures.copyPath(dev, moving).exists(p => os.exists(p / StoreLoader.ArrivedAtFile))
      _ = { failing = false }
      before = recorded(host).storeLoadAttempts.size
      st <- client.getStorageDeviceState(net.secondDeviceId)
      _ <- check2(host, st)
    yield
      recorded(host).transferOutcomes.get(moving) should be(Some(TransferOutcome.PoolRepointed))
      phaseAfterFailure should be(Some(Phase.Arrived(tid.toString)))
      recordAfterFailure should be(Some((Phase.Arrived(tid.toString), Activity.Idle)))
      loadsAfterFailure should be(Nil)
      offlineAfterFailure should be(true)
      arrivedAtWritten should be(true)
      st.stores(moving).status should be(StorageDeviceState.StoreStatus.Active)
      // The Completed Transfers In pass moved it and offered it to the loader at its Live path
      val flat = CopyFixtures.livePathOf(dev, moving)
      CopyFixtures.phaseOf(dev, moving) should be(Some(Phase.Live))
      renames().count((from, to) => to == flat) should be(2)
      recorded(host).storeLoadAttempts.toList.drop(before) should be(List((net.secondDeviceId, flat)))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a promotion whose sync fails starts no pool update until a device check forces it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    host.testingOnlyHandleEvents()
    val arrivedDir = dev / CopyLayout.ArrivedDirectory
    @volatile var failSyncs = 1
    host.control.moves.syncDirectory = d =>
      if d == arrivedDir && failSyncs > 0 then
        failSyncs -= 1
        throw new java.io.IOException("injected sync")
      DurableFiles.syncDirectory(d)
    // Each (phase, unsynced) the record takes, and whether the record was unsynced whenever the pool update ran
    @volatile var seen = Vector.empty[(Phase, Boolean)]
    @volatile var recordingUnsynced = Vector.empty[Boolean]
    host.control.copyRegistry.observer = (copies, sid) =>
      if sid == moving && copies.deviceId == net.secondDeviceId then
        copies.copies.get(sid).map(r => (r.phase, r.unsynced)).foreach(p => if !seen.lastOption.contains(p) then seen :+= p)
        copies.work.get(sid).foreach:
          case w: Work.TransferIn if w.stage == TransferInStage.Recording =>
            recordingUnsynced :+= copies.copies.get(sid).exists(_.unsynced)
          case _ => ()
    for
      tid <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      _ = transfers.finishTransferIn(moving, prepare = configured)
      _ <- pumpTx(host, host.control.router.loaded.contains(moving) ||
                        recorded(host).loadStoreByIdRequests.nonEmpty)
    yield
      failSyncs should be(0)
      val arrived = Phase.Arrived(tid.toString)
      seen.take(2) should be(Vector((arrived, true), (arrived, false)))
      seen.last should be((Phase.Live, false))
      recordingUnsynced should not be empty
      recordingUnsynced.forall(!_) should be(true)
      recorded(host).transferOutcomes.get(moving) should be(Some(TransferOutcome.PoolRepointed))
      transfers.transfersIn.size should be(1)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- Task 6: Live is live/; every copy has a copy id ----

  /** The copy directories of `sid` on `dev`, in every phase directory */
  private def copyDirs(dev: os.Path, sid: StoreId): List[os.Path] =
    List(CopyLayout.LiveDirectory, CopyLayout.ArrivedDirectory, CopyLayout.OutboundDirectory,
      CopyLayout.RebuiltDirectory, CopyLayout.ParkedDirectory, CopyLayout.LostDirectory,
      RebuildingStore.PendingDeleteDirectory).flatMap(d => inDir(dev, d))
      .filter(p => CopyLayout.parseName(p.last).exists(_._1 == sid)).toList

  atest("a new store is created into live under a fresh copy name"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val host = realHost(hostRoot, new TestOpener)
    host.testingOnlyHandleEvents()
    // A real pool, so the create reads its config
    val created = StoreId(PoolId.BootstrapPoolId, 0)
    val initializing = Map(created -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Initializing, None))
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = initializing))
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpTx(host, host.control.router.loaded.contains(created) && !host.control.copyRegistry.isCreating(created))
    yield
      val listing = CopyLayout.list(dev)
      listing.copies.map(f => (f.storeId, f.phase)) should be(List((created, Phase.Live)))
      val copy = listing.copies.head.path
      copy / os.up should be(dev / CopyLayout.LiveDirectory)
      recordOf(host, loadedDevices(host)(deviceA), created).map(r => (r.phase, r.copyId)) should be(
        Some((Phase.Live, CopyFixtures.copyIdOf(copy))))
      host.control.router.loaded(created).backend.path should be(copy)
      inDir(dev, StoreCreation.CreatingDirectory) should be(Nil)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("only live copies are offered to the loader"):
    val (hostRoot, dev) = device()
    val phases = List(Phase.Live, Phase.Arrived("t"), Phase.Outbound("t"), Phase.Rebuilt("e"), Phase.Parked,
      Phase.Lost, Phase.Retired)
    val placed = phases.zipWithIndex.map((phase, i) =>
      (StoreId(s.poolId, i.toByte), phase, CopyFixtures.placeCopy(dev, StoreId(s.poolId, i.toByte), phase)))
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    Future.successful:
      recorded(host).storeLoadAttempts.toList should be(List((deviceA, placed.head._3)))
      // Every copy but the Retired one is registered in its phase, under its copy id
      placed.filter(_._2 != Phase.Retired).foreach: (sid, phase, path) =>
        recordOf(host, local, sid).map(r => (r.phase, r.copyId)) should be(Some((phase, CopyFixtures.copyIdOf(path))))
      recordOf(host, local, placed.last._1) should be(None)

  // Review Focus 3 (P2): neither copy is served, every copy directory of the store ends in lost/ or pending-delete/
  // with its files, and the store is rebuilt
  atest("two copies of one store at load are both retired and the store rebuilt"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val live = rocksCopy(dev, s)
    os.write(live / "data-live", "live")
    val outbound = CopyFixtures.placeCopy(dev, s, Phase.Outbound("t-1"))
    RebuildingStore.writeEmptyCrlState(outbound, s)
    os.write(outbound / "data-outbound", "outbound")
    val ids = Set(CopyFixtures.copyIdOf(live), CopyFixtures.copyIdOf(outbound))
    val rebuilds = new HeldRebuilds
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, rebuilds = rebuilds)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val atLoad = recordOf(host, local, s).map(r => (r.phase, r.copyId))
    val inLostAtLoad = inDir(dev, CopyLayout.LostDirectory).toList
    val retiredAtLoad = CopyFixtures.retiredCopies(dev, s)
    val active = Map(s -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = active))
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- awaitLoudly("the store's rebuild to start"):
             host.testingOnlyHandleEvents()
             !rebuilds.created.isEmpty
      st <- client.getStorageDeviceState(deviceA)
    yield
      // At the load: the least copy id kept in lost/ and registered Lost, the other moved on into pending-delete/
      atLoad should be(Some((Phase.Lost, ids.min)))
      inLostAtLoad.map(CopyFixtures.copyIdOf) should be(List(ids.min))
      retiredAtLoad.map(CopyFixtures.copyIdOf) should be(List(ids.max))
      // Never served
      opener.opens.get should be(0)
      host.control.router.loaded.contains(s) should be(false)
      st.stores(s).status should be(StorageDeviceState.StoreStatus.Rebuilding)
      rebuilds.created.toArray.toList should be(List(s))
      // Each copy directory of the store, under its own name, in lost/ or pending-delete/, its files inside it
      val dirs = copyDirs(dev, s)
      dirs.map(CopyFixtures.copyIdOf).toSet should be(ids)
      dirs.foreach: d =>
        Set(CopyLayout.LostDirectory, RebuildingStore.PendingDeleteDirectory) should contain((d / os.up).last)
      dirs.exists(d => os.exists(d / "data-live") && os.exists(d / StoreConfig.configFilename)) should be(true)
      // The outbound copy's save file stays inside it: as crl_save.log, or as its retire's lost-crl-state.log
      dirs.exists(d => os.exists(d / "data-outbound") && (os.exists(Backend.crlSaveFile(d)) ||
        os.exists(d / RebuildingStore.LostCrlStateFile))) should be(true)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // Spec I1
  atest("two Live copies of one store at load are both retired"):
    val (hostRoot, dev) = device()
    val first = CopyFixtures.placeCopy(dev, s)
    val second = CopyFixtures.placeCopy(dev, s)
    val ids = List(first, second).map(CopyFixtures.copyIdOf)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    Future.successful:
      recorded(host).storeLoadAttempts.toList should be(Nil)
      recordOf(host, local, s).map(r => (r.phase, r.copyId)) should be(Some((Phase.Lost, ids.min)))
      inDir(dev, CopyLayout.LiveDirectory) should be(Nil)
      inDir(dev, CopyLayout.LostDirectory).map(CopyFixtures.copyIdOf) should be(List(ids.min))
      CopyFixtures.retiredCopies(dev, s).map(CopyFixtures.copyIdOf) should be(List(ids.max))
      copyDirs(dev, s).forall(d => os.exists(d / StoreConfig.configFilename)) should be(true)

  // Review Focus 4 (P6): the device stays unloaded with nothing on it moved, and the next scan loads it once the read
  // succeeds
  for (what, unreadable) <- List[(String, (os.Path, os.Path) => os.Path)](
      // The phase directory cannot be listed
      "phase directory" -> ((dev, _) => dev / CopyLayout.LiveDirectory),
      // An outbound copy's phase-param.outbound cannot be read
      "phase-param" -> ((_, sending) => sending / CopyLayout.paramFile(Phase.Outbound("")).get))
  do
    atest(s"an unreadable phase directory or phase-param leaves the device unloaded until a scan can read it ($what)"):
      val (hostRoot, dev) = device()
      val live = CopyFixtures.placeCopy(dev, s)
      val sending = CopyFixtures.placeCopy(dev, StoreId(s.poolId, 1), Phase.Outbound("t-1"))
      val before = CopyLayout.list(dev).copies.toSet
      val target = unreadable(dev, sending)
      val perms = os.perms(target)
      os.perms.set(target, if os.isDir(target) then "-wx------" else "---------")
      val host =
        try
          val h = newHost(hostRoot)
          h.testingOnlyHandleEvents()
          h
        finally os.perms.set(target, perms)
      val loadedWhileUnreadable = loadedDevices(host).contains(deviceA)
      val attemptsWhileUnreadable = recorded(host).storeLoadAttempts.toList
      val afterFailure = CopyLayout.list(dev).copies.toSet
      discoverNow(host)
      host.testingOnlyHandleEvents()
      Future.successful:
        loadedWhileUnreadable should be(false)
        attemptsWhileUnreadable should be(Nil)
        afterFailure should be(before)
        loadedDevices(host).contains(deviceA) should be(true)
        recorded(host).storeLoadAttempts.toList should be(List((deviceA, live)))
        recordOf(host, loadedDevices(host)(deviceA), StoreId(s.poolId, 1)).map(_.phase) should be(
          Some(Phase.Outbound("t-1")))

  // M5.8: a fresh read that fails is retried by the device recheck it schedules, not only by the periodic check
  atest("a transfer in whose fresh read of its device fails resumes at its scheduled recheck"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice(dev => CopyFixtures.placeCopy(dev, moving, Phase.Lost): Unit)
    val transfers = new SnapshotTransfers(dev)
    val host = newHost(hostRoot, transferService = transfers)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(net.secondDeviceId)
    for
      _ <- startMoving(host)
      // The next lookup of the device is the earlier tenancy's fresh read
      _ = recorded(host).armLookupThrow(net.secondDeviceId, new java.io.IOException("injected fresh read failure"))
      _ <- awaitLoudly("the earlier tenancy's retire to end on its failed fresh read"):
             host.testingOnlyHandleEvents()
             host.control.copyRegistry.copiesOf(local).flatMap(_.work.get(moving)).isEmpty &&
               host.control.reconciler.activeDeviceChecks.isEmpty
      _ <- pump(host, false, rounds = 20)
      begunBeforeRecheck = transfers.transfersIn.size
      phaseBeforeRecheck = CopyFixtures.phaseOf(dev, moving)
      _ <- awaitLoudly("the transfer in to begin through the scheduled recheck alone"):
             runDeviceRechecks(host)
             transfers.transfersIn.nonEmpty
    yield
      begunBeforeRecheck should be(0)
      phaseBeforeRecheck should be(Some(Phase.Lost))
      transfers.atBegin.head.map(_._2) should be(List(true))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- Task 6 fix round 1: an Initializing store with a Lost copy is created over it, then rebuilt ----

  private val creating = StoreId(PoolId.BootstrapPoolId, 0)
  private val initializing = Map(creating -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Initializing,
    None))

  /** With device A's tree entry of `creating` Initializing, checks `host`'s devices until its rebuild starts: the create
   *  takes the Lost copy for created and flips the entry, the Lost CRL pass flips it to Rebuilding, and the rebuild's
   *  discard retires the lost copy. Asserts that end */
  private def rebuildsOverLostCopy(host: Host, dev: os.Path, rebuilds: HeldRebuilds,
                                   createDevice: Boolean): Future[org.scalatest.compatible.Assertion] =
    given ExecutionContext = executionContext
    val lost = CopyFixtures.copyPath(dev, creating)
    for
      _ <- if createDevice then net.createDevice(deviceA) else Future.unit
      _ <- setDeviceState(deviceA)(_.copy(stores = initializing))
      _ <- waitForTransactionsToComplete()
      _ <- pumpTx(host, {
             if host.control.reconciler.activeDeviceChecks.isEmpty then checkAllNow(host)
             !rebuilds.created.isEmpty && !host.control.copies.isRetiring(creating)
           })
      st <- client.getStorageDeviceState(deviceA)
    yield
      lost.map(p => (p / os.up).last) should be(Some(CopyLayout.LostDirectory))
      st.stores(creating).status should be(StorageDeviceState.StoreStatus.Rebuilding)
      rebuilds.created.toArray.toList should be(List(creating))
      // The lost copy is retired under its own name, its CRL state saved; no copy of the store is left outside
      // pending-delete/
      val retired = CopyFixtures.retiredCopies(dev, creating)
      retired.map(_.last) should be(lost.map(_.last).toList)
      os.exists(retired.head / RebuildingStore.LostCrlStateFile) should be(true)
      CopyFixtures.copyPath(dev, creating) should be(None)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  /** A real host on device A, its tree entry absent, whose check reads `creating` Initializing: the create's copy is
   *  served, and its flip fails (the device is not in the tree). Then the served copy fail-stops, and its close moves
   *  it to lost/. Yields the host and its opener */
  private def createdThenFailStopped(hostRoot: os.Path, dev: os.Path, rebuilds: StoreRebuildFactory)
      : Future[(Host, TestOpener)] =
    given ExecutionContext = executionContext
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, rebuilds = rebuilds)
    host.testingOnlyHandleEvents()
    for
      _ <- check(host, initializing)
      _ <- pumpTx(host, host.control.router.loaded.contains(creating) && !host.control.copyRegistry.isCreating(creating))
      served = host.control.router.loaded.contains(creating)
      _ = opener.onFatals.head(new java.io.IOException("injected fail-stop before the flip"))
      _ <- pumpTx(host, CopyFixtures.phaseOf(dev, creating).contains(Phase.Lost))
    yield
      served should be(true)
      CopyFixtures.phaseOf(dev, creating) should be(Some(Phase.Lost))
      (host, opener)

  atest("a create whose served copy fail-stops before its failed flip is flipped and rebuilt"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val rebuilds = new HeldRebuilds
    for
      (host, _) <- createdThenFailStopped(hostRoot, dev, rebuilds)
      done <- rebuildsOverLostCopy(host, dev, rebuilds, createDevice = true)
    yield done

  atest("a create whose served copy fail-stops before its flip, then a restart, is flipped and rebuilt"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val rebuilds = new HeldRebuilds
    for
      _ <- createdThenFailStopped(hostRoot, dev, new HeldRebuilds)
      // The host crashes: a new one starts over its directory, and its device load registers the copy Lost
      restarted = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ = restarted.testingOnlyHandleEvents()
      atLoad = recordOf(restarted, loadedDevices(restarted)(deviceA), creating).map(_.phase)
      done <- rebuildsOverLostCopy(restarted, dev, rebuilds, createDevice = true)
    yield
      atLoad should be(Some(Phase.Lost))
      done

  atest("a create that crashed before its flip, then a lost device CRL, is flipped and rebuilt"):
    given ExecutionContext = executionContext
    // The create's complete copy, in live/ beside no CRL: the device's load declares its CRL lost and moves it to lost/
    val (hostRoot, dev) = device(withCrl = false)
    rocksCopy(dev, creating)
    val rebuilds = new HeldRebuilds
    val host = newHost(hostRoot, storeRebuildFactory = rebuilds)
    host.testingOnlyHandleEvents()
    val atLoad = recordOf(host, loadedDevices(host)(deviceA), creating).map(_.phase)
    for
      done <- rebuildsOverLostCopy(host, dev, rebuilds, createDevice = true)
    yield
      atLoad should be(Some(Phase.Lost))
      done

  // Review Focus 5: a file is never a copy, whatever its name
  atest("a file named like a copy beside a real copy in live is a stray: the copy loads and nothing is retired"):
    val (hostRoot, dev) = device()
    val real = CopyFixtures.placeCopy(dev, s)
    val stray = CopyFixtures.freshCopyPath(dev, s)
    os.write(stray, "an operator's file")
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    Future.successful:
      recorded(host).storeLoadAttempts.toList should be(List((deviceA, real)))
      recordOf(host, loadedDevices(host)(deviceA), s).map(r => (r.phase, r.copyId)) should be(
        Some((Phase.Live, CopyFixtures.copyIdOf(real))))
      os.isFile(stray) should be(true)
      inDir(dev, CopyLayout.LostDirectory) should be(Nil)
      CopyFixtures.retiredCopies(dev, s) should be(Nil)

  // ---- Stage 3, Task 2: the reconcile runs CopyDecision.decide per store ----

  // D2: each store's inputs are read at its turn, after the actions of the stores before it. A's request for its
  // rebuild (its Lost copy under Active) starts a receive attempt for B, which sorts after it: B's Arrived copy is
  // then held by that attempt and not brought live, until a check after the attempt ends
  atest("a reconcile reads each store's inputs at its turn"):
    given ExecutionContext = executionContext
    val a = s
    val b = StoreId(s.poolId, 1)
    val (hostRoot, dev) = device()
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(writeDevice(hostRoot, "dev1", deviceB)))
    CopyFixtures.placeCopy(dev, a, Phase.Lost)
    CopyFixtures.placeCopy(dev, b, Phase.Arrived(UUID.randomUUID().toString))
    @volatile var hostRef: Host = null
    @volatile var attemptStarted = false
    def receiving(attempt: Option[UUID]): Boolean =
      hostRef.control.copyRegistry.work("test receive attempt", deviceB, b, _.isInstanceOf[Work.TransferIn],
        attempt.map(id => Work.TransferIn(None, TransferInStage.Receiving, Some(id))))
    val host = newHost(hostRoot, adjustSeams = _.copy(rebuildRequestFailure = storeId =>
      if storeId == a && !attemptStarted then
        attemptStarted = true
        receiving(Some(UUID.randomUUID())) should be(true)
      Some(new Exception("injected: no rebuild request"))))
    hostRef = host
    host.testingOnlyHandleEvents()
    val active = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
    val entries = Map(a -> active, b -> active)
    val local = loadedDevices(host)(deviceA)
    def loadsOfB = recorded(host).storeLoadAttempts.toList.filter((_, p) => CopyLayout.parseName(p.last).exists(_._1 == b))
    for
      _ <- check(host, entries)
      startedInFirst = attemptStarted
      phaseAfterFirst = recordOf(host, local, b).map(_.phase)
      loadsAfterFirst = loadsOfB
      _ = receiving(None) should be(true)
      _ <- check(host, entries)
    yield
      startedInFirst should be(true)
      phaseAfterFirst.exists(_.isInstanceOf[Phase.Arrived]) should be(true)
      loadsAfterFirst should be(Nil)
      recordOf(host, local, b).map(_.phase) should be(Some(Phase.Live))
      loadsOfB should be(List((deviceA, CopyFixtures.livePathOf(dev, b))))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a tracked transfer in whose entry is gone is dropped"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    host.testingOnlyHandleEvents()
    for
      _ <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      trackedBefore = host.control.transfers.pendingStartTransfers.contains(moving)
      // The abort removed the device's entry for the store
      _ <- check2(host, deviceState(net.secondDeviceId))
    yield
      trackedBefore should be(true)
      host.control.transfers.pendingStartTransfers.contains(moving) should be(false)
      host.control.transfers.receiving.contains(moving) should be(false)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())
