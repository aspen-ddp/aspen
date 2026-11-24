package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.host.DeviceMetadata.RebuildOutcome
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.{DefaultBackendOpener, StoreCreation}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** The project goal of device I/O isolation (spec section 9, lockstep): a hung device stalls only its own stores.
 *  Device A's lane is held (`holdDevice`: its management jobs wait unrun in `heldJobs`, its data and serial tasks
 *  block) while the host keeps managing device B: a scan finds and loads it, a store on it loads and serves, a
 *  transfer in onto it completes, and a rebuild onto it completes beside a rebuild that waits on A. Stores load for
 *  real (RocksDB, CopyHostFixture.realHost) */
class HungDeviceSuite extends IntegrationTestSuite with CopyHostFixture:

  private val onB = StoreId(s.poolId, 1.toByte)
  // Stores of a pool the tree does not hold, rebuilt by held rebuilds (as RebuildIoSuite's): one onto each device
  private val rebuiltOnA = StoreId(PoolId(UUID.fromString("66666666-6666-6666-6666-666666666666")), 0)
  private val rebuiltOnB = StoreId(rebuiltOnA.poolId, 1.toByte)
  // A store of the bootstrap pool, transferred onto B
  private val transferred = StoreId(PoolId.BootstrapPoolId, 1)

  private def served(host: Host, sid: StoreId): Boolean = host.control.router.loaded.contains(sid)

  private def drainUntil(host: Host, what: String)(condition: => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    awaitLoudly(what):
      host.testingOnlyHandleEvents()
      condition

  /** drainUntil, letting the client's transactions complete between rounds (pool and device updates) */
  private def settleUntil(host: Host, what: String)(condition: => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + 20_000_000_000L
    def loop(): Future[Unit] =
      host.testingOnlyHandleEvents()
      if condition then Future.unit
      else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"Timed out waiting for $what"))
      else waitForTransactionsToComplete().flatMap(_ => loop())
    loop()

  private def recordOn(host: Host, id: StorageDeviceId, sid: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.device(id).flatMap(_.copies.get(sid))

  private def outcome(host: Host, id: StorageDeviceId, sid: StoreId): Option[RebuildOutcome] =
    recorded(host).rebuildOutcomes.get((id, sid))

  /** A real RocksDB store of `sid` in rebuilt/ on `dev` for `episode`, as a rebuild's finish leaves it: its copy id */
  private def placeRebuilt(dev: os.Path, sid: StoreId, episode: UUID): UUID =
    val phase = Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(Some(episode)))
    val dir = CopyFixtures.freshCopyPath(dev, sid, phase)
    os.makeDir.all(dir / os.up)
    DefaultBackendOpener.createStore(StoreConfig(sid, StoreConfig.RocksDB(None), None, ida), dir,
      dev / StoreCreation.CreatingDirectory)
    os.write.over(dir / CopyLayout.paramFile(phase).get, CopyLayout.param(phase).get)
    CopyFixtures.copyIdOf(dir)

  /** Starts the rebuild of `sid` on device `id` for `episode`, its entry Rebuilding in the latest reconcile's
   *  entries (as RebuildIoSuite does) */
  private def startRebuild(host: Host, id: StorageDeviceId, sid: StoreId, episode: UUID): Unit =
    val sds = loadedDevices(host)(id)
    sds.reconciledEntries = Some(sds.reconciledEntries.getOrElse(Map()) + (sid -> StorageDeviceState.StoreEntry(
      StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode))))
    host.control.rebuilds.startRebuild(sds, sid, Some(episode), acceptLoss = false)

  private def closeAll(host: Host): Future[Unit] =
    given ExecutionContext = executionContext
    Future.sequence(host.control.router.loaded.values.map(_.backend.close())).map(_ => ())

  atest("a held device stalls only its own stores"):
    given ExecutionContext = executionContext
    val (hostRoot, devA) = device()
    rocksCopy(devA, s)
    val factory = new HeldRebuildFactory
    val service = new StagedTransferService
    val host = newHost(hostRoot, storeRebuildFactory = factory, transferService = service, maxConcurrentRebuilds = 1,
      adjustSeams = _.copy(tryLoadStore = None, loadStore = None, loadStoreById = None,
        backends = Some(new TestOpener)))
    val (eA, eB) = (UUID.randomUUID(), UUID.randomUUID())
    var devB = os.root
    for
      _ <- drainUntil(host, "A's store served")(served(host, s))
      _ <- net.createDevice(deviceA)
      _ <- net.createDevice(deviceB)
      _ <- setDeviceState(deviceA)(_.setRebuilding(rebuiltOnA, eA)
             .setStoreEntry(s, StorageDeviceState.StoreStatus.Active, None))
      _ <- setDeviceState(deviceB)(_.setRebuilding(rebuiltOnB, eB)
             .setStoreEntry(onB, StorageDeviceState.StoreStatus.Active, None))
      _ <- waitForTransactionsToComplete()
      // A hangs. Its rebuild takes A's only rebuild slot and its finish waits on A's lane; its device check waits too
      _ = holdDevice(host, devA)
      _ = startRebuild(host, deviceA, rebuiltOnA, eA)
      _ <- drainUntil(host, "A's rebuild created")(factory.created.exists(_.storeId == rebuiltOnA))
      _ = factory.created.find(_.storeId == rebuiltOnA).get.placeAndComplete()
      _ = checkAllNow(host)
      _ <- drainUntil(host, "A's finish listing held")(heldJobs(host, devA).contains(JobKind.RebuildBookkeeping))
      // A scan finds device B and loads it, and its store loads and serves
      _ = devB = writeDevice(hostRoot, "dev1", deviceB)
      _ = os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devB))
      _ = rocksCopy(devB, onB)
      _ = scanNow(host)
      _ <- drainUntil(host, "B's store served")(loadedDevices(host).contains(deviceB) && served(host, onB))
      // A transfer in onto B completes: received, promoted, its pool updated, Live and served
      _ <- client.transferStore(transferred, deviceB)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- settleUntil(host, "the transfer in begun")(service.startedTransfersIn.contains(transferred))
      _ = service.finishTransferIn(transferred, prepare = staged =>
            DefaultBackendOpener.createStore(StoreConfig(transferred, StoreConfig.RocksDB(None), None, ida), staged,
              devB / StoreCreation.CreatingDirectory))
      _ <- settleUntil(host, "the transferred store served on B")(
             recorded(host).transferOutcomes.contains(transferred) && served(host, transferred))
      // A rebuild onto B starts (B's own slot) and completes: flipped, moved Live, loaded and served
      _ = startRebuild(host, deviceB, rebuiltOnB, eB)
      _ <- drainUntil(host, "B's rebuild created")(factory.created.exists(_.storeId == rebuiltOnB))
      _ = factory.created.find(_.storeId == rebuiltOnB).get.promise.success(placeRebuilt(devB, rebuiltOnB, eB))
      _ <- settleUntil(host, "B's rebuild served")(
             outcome(host, deviceB, rebuiltOnB).nonEmpty && served(host, rebuiltOnB))
      heldWhile = heldJobs(host, devA)
      rebuildAWhile = (outcome(host, deviceA, rebuiltOnA), host.control.rebuilds.rebuildSlotsTakenOn(deviceA))
      phaseAWhile = recordOn(host, deviceA, rebuiltOnA).map(_.phase)
      slotsBWhile = host.control.rebuilds.rebuildSlotsTakenOn(deviceB)
      servedWhile = List(s, onB, transferred, rebuiltOnB).filter(served(host, _))
      // Released, A's held work runs and its rebuild completes
      _ = releaseDevice(host, devA)
      _ <- settleUntil(host, "A's rebuild settled")(
             outcome(host, deviceA, rebuiltOnA).nonEmpty && !host.control.rebuilds.hasRunningRebuild(rebuiltOnA) &&
             heldJobs(host, devA).isEmpty)
      _ <- closeAll(host)
    yield
      // Everything A was asked for waited on A's lane, and nothing of it ran
      heldWhile should contain(JobKind.RebuildBookkeeping)
      withClue(heldWhile) {
        heldWhile.toSet.subsetOf(Set(JobKind.RebuildBookkeeping, JobKind.Move, JobKind.ReconcileInputs)) should be(true)
      }
      rebuildAWhile should be((None, 1))
      phaseAWhile should be(Some(Phase.Rebuilt(eA.toString)))
      // B's rebuild took B's slot, not A's, and gave it back
      slotsBWhile should be(0)
      outcome(host, deviceB, rebuiltOnB) should be(Some(RebuildOutcome.Adopt))
      recordOn(host, deviceB, rebuiltOnB).map(_.phase) should be(Some(Phase.Live))
      CopyFixtures.phaseOf(devB, transferred) should be(Some(Phase.Live))
      servedWhile should be(List(s, onB, transferred, rebuiltOnB))
      served(host, s) should be(true)
      outcome(host, deviceA, rebuiltOnA) should be(Some(RebuildOutcome.Adopt))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())
      host.testingOnlyVerifyCopyRegistry() should be(Nil)

  atest("a store with copies on both devices"):
    // Spec section 9 / review focus 2: store `both` has its old copy, Lost, on hung device A, and is rebuilt onto
    // healthy device B, the device its pool names (its owner). The new copy comes Active and is served while the old
    // copy's release waits on A's lane: never run early, never inferred done
    given ExecutionContext = executionContext
    val both = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    client.getStoragePoolState(PoolId.BootstrapPoolId).flatMap: pool =>
      val owner = pool.stores(both.poolIndex).storageDeviceId
      val (hostRoot, devA) = device()
      val devB = writeDevice(hostRoot, "dev1", owner)
      os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devB))
      CopyFixtures.placeCopy(devA, both, Phase.Lost)
      val factory = new HeldRebuildFactory
      val host = newHost(hostRoot, storeRebuildFactory = factory,
        adjustSeams = _.copy(tryLoadStore = None, loadStore = None, loadStoreById = None,
          backends = Some(new TestOpener)))
      val episode = UUID.randomUUID()
      // A hangs before any check of it: everything its checks ask for waits on its lane
      holdDevice(host, devA)
      def releasesRun: Long =
        loadedDevices(host).get(deviceA).flatMap(_.io.observer.completed.get(JobKind.Release.label)).getOrElse(0L)
      for
        _ <- net.createDevice(deviceA)
        _ <- setDeviceState(owner)(_.setRebuilding(both, episode))
        _ <- waitForTransactionsToComplete()
        _ = startRebuild(host, owner, both, episode)
        _ <- drainUntil(host, "the rebuild onto B created")(factory.created.exists(_.storeId == both))
        _ = factory.created.find(_.storeId == both).get.promise.success(placeRebuilt(devB, both, episode))
        _ <- settleUntil(host, "the rebuilt copy served on B")(
               outcome(host, owner, both).nonEmpty && served(host, both))
        // Both devices checked. A's checks go one held job at a time (passHeldJob) through the retire of its Lost
        // copy into pending-delete/, up to the release that would delete it: that one stays held
        _ = checkAllNow(host)
        _ <- settleUntil(host, "A's release of its retired copy held")(
               (heldJobs(host, devA).lastOption.contains(JobKind.Release) &&
                CopyFixtures.retiredCopies(devA, both).nonEmpty) || {
                 if heldJobs(host, devA).nonEmpty then passHeldJob(host, devA): Unit
                 else if !host.control.reconciler.activeDeviceChecks.contains(deviceA) then checkAllNow(host)
                 false
               })
        heldWhile = heldJobs(host, devA)
        retiredWhile = CopyFixtures.retiredCopies(devA, both).size
        oldWhile = CopyFixtures.phaseOf(devA, both)
        releasesWhile = releasesRun
        servedWhile = served(host, both)
        ownerWhile = recordOn(host, owner, both).map(r => (r.phase, r.activity))
        verifiedWhile = host.testingOnlyVerifyCopyRegistry()
        // Released, the release runs: the retired copy is deleted, its store being Active on its owner device
        _ = releaseDevice(host, devA)
        _ <- settleUntil(host, "A's old copy released")(
               releasesRun > releasesWhile && CopyFixtures.retiredCopies(devA, both).isEmpty)
        _ <- closeAll(host)
      yield
        heldWhile.lastOption should be(Some(JobKind.Release))
        retiredWhile should be(1)
        oldWhile should be(None)
        servedWhile should be(true)
        ownerWhile should be(Some((Phase.Live, Activity.Served)))
        verifiedWhile should be(Nil)
        outcome(host, owner, both) should be(Some(RebuildOutcome.Adopt))
        recordOn(host, deviceA, both) should be(None)
        CopyFixtures.phaseOf(devB, both) should be(Some(Phase.Live))
        host.control.copyRegistry.takeRegistryErrors() should be(Vector())
        host.testingOnlyVerifyCopyRegistry() should be(Nil)

