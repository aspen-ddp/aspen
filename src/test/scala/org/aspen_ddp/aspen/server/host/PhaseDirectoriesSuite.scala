package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, Host, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, StoreRecoveryState}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendStateLost, DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.Try

/** Stage 2, Task 2: Lost is the lost/ directory. Every T10 trigger moves a closed copy into lost/<copy> (a FailStop
 *  close moves it when the close ends, P5), retire moves lost/<copy> to pending-delete/<copy> (T11), and a copy whose
 *  move to lost/ fails is held offline until a device check moves it. */
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
                       rename: Option[(os.Path, os.Path) => Unit] = None): Host =
    newHost(hostRoot, adjustSeams = _.copy(tryLoadStore = None, loadStore = None, loadStoreById = None,
      backends = Some(opener), copyRename = rename))

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
