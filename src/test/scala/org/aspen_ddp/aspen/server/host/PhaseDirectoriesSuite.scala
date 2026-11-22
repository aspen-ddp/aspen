package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, Host, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, StoreRecoveryState}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{RebuildOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
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
 *  (T6). */
class PhaseDirectoriesSuite extends IntegrationTestSuite with ControlFixture:

  private val s = storeId
  private val ida = Replication(3, 2)

  /** The default opener, whose RocksDB backends report hasFailed while `failing` is set, close only once `closeGate`
   *  (if set when the close is asked) completes, and whose opens fail with BackendStateLost while `stateLost` is set */
  private class TestOpener extends DefaultBackendOpener(executionContext):
    @volatile var failing = false
    @volatile var stateLost = false
    @volatile var closeGate: Option[Future[Unit]] = None
    @volatile var onFatals: List[Throwable => Unit] = Nil
    val opens = new AtomicInteger(0)
    override def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit): Future[Backend] =
      opens.incrementAndGet()
      onFatals = onFatal :: onFatals
      if stateLost then
        Future.failed(new BackendStateLost("injected: acknowledged state lost"))
      else
        Future.fromTry(Try(new RocksDBBackend(storePath, storeCfg.storeId, executionContext):
          override def hasFailed: Boolean = super.hasFailed || TestOpener.this.failing
          override def close(): Future[Unit] = closeGate match
            case None => super.close()
            case Some(gate) => gate.flatMap(_ => super.close())(using executionContext)))

  /** A CRL whose next `failReads` reads of a store's state and next `failDrops` drops fail */
  private class FlakyCrl extends TestNetwork.TestCRL:
    val failReads = new AtomicInteger(0)
    val failDrops = new AtomicInteger(0)
    override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
      if failReads.getAndUpdate(n => math.max(0, n - 1)) > 0 then Future.failed(new java.io.IOException("injected read"))
      else super.readStoreState(storeId)
    override def dropStore(storeId: StoreId): Future[Unit] =
      if failDrops.getAndUpdate(n => math.max(0, n - 1)) > 0 then Future.failed(new java.io.IOException("injected drop"))
      else super.dropStore(storeId)

  private class FlakyCrlFactory extends CrashRecoveryLogFactory:
    val crl = new FlakyCrl
    def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
      os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devicePath))
      crl
    def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit = ()

  /** A host root with device A, its CRL directory present (so its CRL is not lost) unless not `withCrl` */
  private def device(withCrl: Boolean = true): (os.Path, os.Path) =
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", deviceA)
    if withCrl then
      os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    (hostRoot, dev)

  /** A real RocksDB store of `sid`, Live on the device */
  private def rocksCopy(dev: os.Path, sid: StoreId): os.Path =
    val dir = CopyLayout.path(dev, sid, None, Phase.Live)
    DefaultBackendOpener.createStore(StoreConfig(sid, StoreConfig.RocksDB(None), None, ida), dir,
      dev / StoreCreation.CreatingDirectory)
    dir

  /** A ControlFixture host that loads its stores for real, through `opener` (its recording load seams, which reach the
   *  host only once it is built, left out: its device load opens and loads its stores) */
  private def realHost(hostRoot: os.Path, opener: TestOpener,
                       rename: Option[(os.Path, os.Path) => Unit] = None,
                       rebuilds: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                       transfers: StoreTransferService = StoreTransferService.Disabled): Host =
    newHost(hostRoot, storeRebuildFactory = rebuilds, transferService = transfers,
      adjustSeams = _.copy(tryLoadStore = None, loadStore = None, loadStoreById = None, backends = Some(opener),
        copyRename = rename))

  /** Drains `host` and yields until `condition`, `rounds` drains (when not negative) or 10 s. The caller asserts */
  private def pump(host: Host, condition: => Boolean, rounds: Int = -1): Future[Unit] =
    val deadline = System.nanoTime() + Duration(10, SECONDS).toNanos
    def loop(done: Int): Future[Unit] =
      host.testingOnlyHandleEvents()
      if condition || rounds == done || (rounds < 0 && System.nanoTime() - deadline > 0) then
        Future.unit
      else
        if done > 2000 then
          Thread.sleep(1)
        Future(())(using executionContext).flatMap(_ => loop(done + 1))(using executionContext)
    loop(0)

  private def lostDir(dev: os.Path): os.Path = dev / CopyLayout.LostDirectory

  /** The copy of `sid` is in lost/ under a copy name, and nothing is left at its flat path */
  private def inLost(dev: os.Path, sid: StoreId, flat: os.Path): org.scalatest.compatible.Assertion =
    CopyFixtures.phaseOf(dev, sid) should be(Some(Phase.Lost))
    val path = CopyFixtures.copyPath(dev, sid).get
    path / os.up should be(lostDir(dev))
    CopyLayout.parseName(path.last).map(_._1) should be(Some(sid))
    os.exists(flat) should be(false)

  /** A device check of device A reading `state`'s entries */
  private def check(host: Host, entries: Map[StoreId, org.aspen_ddp.aspen.common.metadata.StorageDeviceState.StoreEntry]
                    = Map()): Future[Unit] =
    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkStorageDevice(deviceA)
    armed.success(deviceState(deviceA, entries))
    pump(host, host.control.reconciler.activeDeviceChecks.isEmpty && !host.control.copies.isRetiring(s))

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
      // The close is held: the copy is still Live at its flat path, its backend's directory never moved while open
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

  /** As pump, also letting the client's transactions complete each round */
  private def pumpTx(host: Host, condition: => Boolean, rounds: Int = -1): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + Duration(10, SECONDS).toNanos
    def loop(done: Int): Future[Unit] =
      host.testingOnlyHandleEvents()
      if condition || rounds == done || (rounds < 0 && System.nanoTime() - deadline > 0) then
        Future.unit
      else
        waitForTransactionsToComplete().flatMap(_ => loop(done + 1))
    loop(0)

  /** A real host serving a RocksDB copy of `s` on device A, whose backend has failed with its FailStop close held by
   *  the returned gate: the copy is Closing, still Live at its flat path */
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
      _ = host.control.reconciler.checkAll()
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
      registered.map(r => (r.phase, r.copyId)) should be(Some((Phase.Lost, Some(copyId))))
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
      retiredWhileHeld <- host.control.copies.retire(sds, s)
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
      host.control.moves.move("test", sds, s, Phase.Lost) shouldBe a[MoveResult.NotMoved]
      record.phase should be(Phase.Live)
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Live))
      copies.applyActivity(s, Activity.Idle)
    // Unsynced by an earlier change a crash could undo: a durable move forces only the directories naming the copy
    copies.setUnsynced(s, true)
    host.control.moves.move("test", sds, s, Phase.Lost) should be(MoveResult.Durable)
    record.phase should be(Phase.Lost)
    record.unsynced should be(true)
    CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
    // The device check's resync forces the copy itself, and clears it
    host.control.moves.resync(sds) should be(List(s))
    record.unsynced should be(false)
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- Task 3: Outbound is outbound/ ----

  // A store of the bootstrap pool, whose pool entry names the bootstrap device: startOut reads both fresh
  private val shipped = StoreId(PoolId.BootstrapPoolId, 1)
  private val source = StorageDeviceId.BootstrapStorageDeviceId

  private def ask(host: Host): Future[Try[Unit]] =
    given ExecutionContext = executionContext
    host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, source, shipped,
      HLCTimestamp.now, UUID.randomUUID())).transform(Success(_))

  /** A host over the bootstrap device holding `shipped` as `place` leaves it, its sends staged, and the store's
   *  transfer to the second device started in the tree: yields the host, its device, the device path and the
   *  transfer's id */
  private def transferring(place: os.Path => Unit, sends: StagedSendService)
      : Future[(Host, LocalStorageDeviceState, os.Path, String)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", source)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    place(dev)
    val host = newHost(hostRoot, transferService = sends)
    host.testingOnlyHandleEvents()
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      st <- client.getStorageDeviceState(source)
    yield (host, loadedDevices(host)(source), dev, st.stores(shipped).transferId.get.toString)

  /** Asks `host` to ship `shipped`, and waits for the request's handling to end: its close's work too */
  private def askAndSettle(host: Host): Future[Try[Unit]] =
    given ExecutionContext = executionContext
    for
      started <- ask(host)
      _ <- awaitLoudly("the transfer out's work to end"):
             host.testingOnlyHandleEvents()
             !host.control.copyRegistry.transferringOut(shipped, TransferOutStage.Closing) &&
               !host.control.copyRegistry.transferringOut(shipped, TransferOutStage.AbortingLost)
    yield started

  private def recordOf(host: Host, local: LocalStorageDeviceState, storeId: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.copiesOf(local).flatMap(_.copies.get(storeId))

  atest("a transfer out moves the closed copy to outbound with its transfer id and ships it from there"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(dev => writeStoreConfig(dev, shipped): Unit, sends)
      flat = CopyLayout.path(dev, shipped, None, Phase.Live)
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
      flat = CopyLayout.path(dev, shipped, None, Phase.Live)
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
      first <- askAndSettle(host)
      shippedAfterFirst = sends.sources
      phaseAfterFirst = CopyFixtures.phaseOf(dev, shipped)
      unsyncedAfterFirst = recordOf(host, local, shipped).map(r => (r.phase, r.unsynced))
      // A retransmit while it is unsynced ships nothing either
      second <- askAndSettle(host)
      shippedAfterSecond = sends.sources
      resynced = host.control.moves.resync(local)
      third <- askAndSettle(host)
    yield
      first should be(Success(()))
      failures should be(0)
      shippedAfterFirst should be(Vector())
      phaseAfterFirst should be(Some(Phase.Outbound(tid)))
      unsyncedAfterFirst should be(Some((Phase.Outbound(tid), true)))
      second shouldBe a[Failure[?]]
      shippedAfterSecond should be(Vector())
      resynced should be(List(shipped))
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

  /** Records every rename of `host`'s moves, failing those `fail` names */
  private def recordRenames(host: Host, fail: (os.Path, os.Path) => Boolean = (_, _) => false)
      : () => Vector[(os.Path, os.Path)] =
    @volatile var seen = Vector.empty[(os.Path, os.Path)]
    host.control.moves.rename = (from, to) =>
      seen :+= (from, to)
      if fail(from, to) then throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    () => seen

  atest("a copy outbound for an earlier transfer goes back to live and out again with the new transfer's id"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    for
      (host, local, dev, tid) <- transferring(placeOutbound("old"), sends)
      before = CopyFixtures.copyPath(dev, shipped).get
      flat = CopyLayout.path(dev, shipped, None, Phase.Live)
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
      flat = CopyLayout.path(dev, shipped, None, Phase.Live)
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
      flat = CopyLayout.path(dev, shipped, None, Phase.Live)
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

  /** A rebuild that, once started, places its copy in rebuilt/ with its episode as RebuildingStore.finish does (a
   *  real RocksDB store with its empty save file when `real`, else a config only), and completes with its copy id */
  private class PlacingRebuilds(real: Boolean) extends StoreRebuildFactory:
    def createRebuild(client: org.aspen_ddp.aspen.client.AspenClient, storeId: StoreId, deviceId: StorageDeviceId,
                      devicePath: os.Path, backends: org.aspen_ddp.aspen.server.store.backend.BackendOpener,
                      episode: Option[UUID], existingCopy: () => Option[(Phase, os.Path)]): StoreRebuild =
      val id = storeId
      val rebuilt = Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode))
      val copy =
        if real then
          val flat = rocksCopy(devicePath, id)
          RebuildingStore.writeEmptyCrlState(flat, id)
          CopyFixtures.markCopy(devicePath, id, rebuilt)
        else
          CopyFixtures.placeCopy(devicePath, id, rebuilt)
      new StoreRebuild:
        val storeId: StoreId = id
        def complete: Future[UUID] = Future.successful(CopyFixtures.copyIdOf(copy))
        def cancel(): Unit = ()

  /** The children of the phase directory `name` on `dev`, none if it is absent */
  private def inDir(dev: os.Path, name: String): Seq[os.Path] =
    if os.isDir(dev / name) then os.list(dev / name) else Nil

  /** Starts the rebuild of `s` on device A of `host` for `episode`, its entry Rebuilding for it in the tree with the
   *  device in `status`, and waits for the rebuild's work to end */
  private def rebuildAndFlip(host: Host, episode: UUID,
                             status: StorageDeviceState.DeviceStatus = StorageDeviceState.DeviceStatus.Operational)
      : Future[RebuildOutcome] =
    given ExecutionContext = executionContext
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(s, episode).copy(status = status))
      _ <- waitForTransactionsToComplete()
      _ = host.control.rebuilds.startRebuild(loadedDevices(host)(deviceA), s, Some(episode), acceptLoss = false)
      _ <- pumpTx(host, recorded(host).rebuildOutcomes.contains((deviceA, s)) &&
                        !host.control.copyRegistry.rebuilding(HostCopies.RebuildHolds).contains(s))
    yield recorded(host).rebuildOutcomes((deviceA, s))

  atest("an Adopt flip moves the rebuilt copy to live and serves it"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val opener = new TestOpener
    val host = realHost(hostRoot, opener, rebuilds = new PlacingRebuilds(real = true))
    host.testingOnlyHandleEvents()
    val renames = recordRenames(host)
    val flat = CopyLayout.path(dev, s, None, Phase.Live)
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

  atest("a shipped rebuilt copy arrives and goes live with no rebuild-complete step"):
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
      _ = host.control.reconciler.checkAll()
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
      live should be(CopyLayout.path(dev, moving, None, Phase.Live))
      // Inert here: the copy is Live at the flat path and served
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
