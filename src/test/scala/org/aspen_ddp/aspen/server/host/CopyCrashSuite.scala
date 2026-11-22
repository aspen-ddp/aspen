package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, Host, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{RebuildOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.{Backend, DefaultBackendOpener, StoreCreation}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

/** Spec Tests, "Crash at each step of each transition": a host stops after one step of a transition (a seam fails the
 *  next step, or holds it), and the host crashes there: dropped without shutdown, and a fresh host built over the same
 *  host directory. After the crash the copy is in exactly one phase (CopyLayout.list holds it once), and the next check
 *  finishes the transition or retries it.
 *
 *  A rename that landed but was not forced is the state a crash finds when the OS kept the rename: the restarted host
 *  sees the copy where the rename put it. The state after an OS crash that undid it is the step before's */
class CopyCrashSuite extends IntegrationTestSuite with CopyHostFixture:

  /** The copies of `sid` on the device, in every phase directory but pending-delete/ unless `retired` */
  private def copiesOf(dev: os.Path, sid: StoreId, retired: Boolean = false): List[CopyLayout.Found] =
    CopyLayout.list(dev).copies.filter(f => f.storeId == sid && (retired || f.phase != Phase.Retired))

  /** The one copy of `sid` on the device, in any phase directory pending-delete/ included: fails unless there is
   *  exactly one */
  private def onlyCopy(dev: os.Path, sid: StoreId): CopyLayout.Found =
    val all = copiesOf(dev, sid, retired = true)
    all should have size 1
    all.head

  /** Fails directory syncs of `dir` on `host`'s moves while `failing` says so: a rename that lands but is not forced */
  private def failSyncsOf(host: Host, dir: os.Path, failing: () => Boolean): Unit =
    host.control.moves.syncDirectory = d =>
      if d == dir && failing() then
        throw new java.io.IOException("injected sync")
      DurableFiles.syncDirectory(d)

  private def active(sid: StoreId): Map[StoreId, StorageDeviceState.StoreEntry] =
    Map(sid -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))

  // ---- T6: Live to Outbound, after the close's extraction and before the move ----

  atest("T6: a crash after the extraction and before the move to outbound leaves the copy live, and the retransmit " +
        "ships it"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", source)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    val live = rocksCopy(dev, shipped)
    val sends = new StagedSendService
    val host = realHost(hostRoot, new TestOpener, transfers = sends)
    val outboundDir = dev / CopyLayout.OutboundDirectory
    // Every rename into outbound/ fails: the copy is closed and extracted, then the host crashes before its move
    host.control.moves.rename = (from, to) =>
      if to / os.up == outboundDir then throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    for
      _ <- pump(host, host.control.router.loaded.contains(shipped))
      servedAtLoad = host.control.router.loaded.contains(shipped)
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      st <- client.getStorageDeviceState(source)
      tid = st.stores(shipped).transferId.get.toString
      first <- askAndSettle(host)
      // The extraction's state: closed, Live, its CRL state in its save file
      afterExtraction = (copiesOf(dev, shipped).map(f => (f.phase, f.path)), host.control.router.loaded.contains(shipped),
                         CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(live), shipped).isRight)
      shippedBeforeCrash = sends.sources
      // The crash: the old host dropped, a new one over the same directory
      sends2 = new StagedSendService
      restarted = realHost(hostRoot, new TestOpener, transfers = sends2)
      _ <- pump(restarted, restarted.control.router.loaded.contains(shipped))
      atRestart = copiesOf(dev, shipped).map(f => (f.phase, f.path))
      servedAtRestart = restarted.control.router.loaded.contains(shipped)
      // The receiver's retransmit: the next request closes it again and ships it from outbound/
      second <- askAndSettle(restarted)
    yield
      servedAtLoad should be(true)
      first should be(Success(()))
      afterExtraction should be((List((Phase.Live, live)), false, true))
      shippedBeforeCrash should be(Vector())
      // Reloaded from live/, from its save file
      atRestart should be(List((Phase.Live, live)))
      servedAtRestart should be(true)
      second should be(Success(()))
      val copy = onlyCopy(dev, shipped)
      copy.phase should be(Phase.Outbound(tid))
      copy.path / os.up should be(outboundDir)
      copy.copyId should be(CopyFixtures.copyIdOf(live))
      sends2.sources should be(Vector(shipped -> copy.path))
      CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(copy.path), shipped).isRight should be(true)
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("T6: a crash after the move to outbound and before its sync keeps the copy outbound, and the retransmit " +
        "ships it from there"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", source)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    val live = rocksCopy(dev, shipped)
    val sends = new StagedSendService
    val host = realHost(hostRoot, new TestOpener, transfers = sends)
    val outboundDir = dev / CopyLayout.OutboundDirectory
    failSyncsOf(host, outboundDir, () => true)
    for
      _ <- pump(host, host.control.router.loaded.contains(shipped))
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      st <- client.getStorageDeviceState(source)
      tid = st.stores(shipped).transferId.get.toString
      first <- askAndSettle(host)
      beforeCrash = onlyCopy(dev, shipped)
      recordBeforeCrash = recordOf(host, loadedDevices(host)(source), shipped).map(r => (r.phase, r.unsynced))
      shippedBeforeCrash = sends.sources
      sends2 = new StagedSendService
      restarted = realHost(hostRoot, new TestOpener, transfers = sends2)
      _ <- pump(restarted, false, rounds = 10)
      atRestart = onlyCopy(dev, shipped)
      servedAtRestart = restarted.control.router.loaded.contains(shipped)
      second <- askAndSettle(restarted)
    yield
      first should be(Success(()))
      beforeCrash.phase should be(Phase.Outbound(tid))
      recordBeforeCrash should be(Some((Phase.Outbound(tid), true)))
      shippedBeforeCrash should be(Vector())
      // Never loaded from outbound/: its state stays in its save file
      atRestart should be(beforeCrash)
      servedAtRestart should be(false)
      second should be(Success(()))
      // Shipped where it is, unmoved
      onlyCopy(dev, shipped) should be(beforeCrash)
      beforeCrash.copyId should be(CopyFixtures.copyIdOf(live))
      sends2.sources should be(Vector(shipped -> beforeCrash.path))
      CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(beforeCrash.path), shipped).isRight should be(true)
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- T11: Lost to Retired, at each step of the retire ----

  /** A retire's steps (StoreCopies.retire) after which the host crashes: each fails the step after it */
  private enum RetireStep:
    case MovedToLost, StateSaved, Dropped, RenamedNotForced

  for step <- RetireStep.values do
    atest(s"T11: a crash after each retire step leaves one copy, and the next check finishes the retire ($step)"):
      given ExecutionContext = executionContext
      val (hostRoot, dev) = device()
      // A Live copy whose entry has left the device: the Deleted Stores pass retires it
      val live = writeStoreConfig(dev, s)
      val copyId = CopyFixtures.copyIdOf(live)
      val factory = new FlakyCrlFactory
      val host = newHost(hostRoot, crlFactory = factory)
      host.testingOnlyHandleEvents()
      val pendingDir = dev / RebuildingStore.PendingDeleteDirectory
      step match
        case RetireStep.MovedToLost => factory.crl.failReads.set(1)
        case RetireStep.StateSaved => factory.crl.failDrops.set(1)
        case RetireStep.Dropped =>
          host.control.moves.rename = (from, to) =>
            if to / os.up == pendingDir then throw new java.io.IOException("injected rename")
            DurableFiles.atomicRename(from, to)
        case RetireStep.RenamedNotForced =>
          @volatile var failing = true
          failSyncsOf(host, pendingDir, () => failing)
      for
        _ <- check(host)
        beforeCrash = onlyCopy(dev, s)
        stateBeforeCrash = os.exists(beforeCrash.path / RebuildingStore.LostCrlStateFile)
        // The state the crashed retire saved, made distinct from anything a retire could save again: the CRLs here
        // hold no state, so a re-save over it would otherwise write the same bytes. After a real dropStore a re-save
        // would replace the saved entries with an empty state
        sentinel = s"saved before the crash: ${UUID.randomUUID()}"
        _ = if stateBeforeCrash then
              os.write.over(beforeCrash.path / RebuildingStore.LostCrlStateFile, sentinel)
        stateBytes = Option.when(stateBeforeCrash)(sentinel)
        restarted = newHost(hostRoot)
        _ = restarted.testingOnlyHandleEvents()
        atRestart = onlyCopy(dev, s)
        recordAtRestart = recordOf(restarted, loadedDevices(restarted)(deviceA), s).map(_.phase)
        _ <- check(restarted)
        _ <- pump(restarted, copiesOf(dev, s).isEmpty)
      yield
        // Before the crash: the copy where its last step left it, under its own copy id
        beforeCrash.copyId should be(copyId)
        (beforeCrash.phase, stateBeforeCrash) should be(step match
          case RetireStep.MovedToLost => (Phase.Lost, false)
          case RetireStep.StateSaved | RetireStep.Dropped => (Phase.Lost, true)
          case RetireStep.RenamedNotForced => (Phase.Retired, true))
        atRestart.path should be(beforeCrash.path)
        // A Lost copy is registered Lost and never loaded; a Retired one is not recorded (P3)
        recordAtRestart should be(if beforeCrash.phase == Phase.Lost then Some(Phase.Lost) else None)
        recorded(restarted).storeLoadAttempts.toList.map(_._2) should not contain beforeCrash.path
        // The next check's retire finished: one copy, retired under its own name with its CRL state, the state the
        // crashed retire saved kept as it was
        val retired = onlyCopy(dev, s)
        retired.phase should be(Phase.Retired)
        retired.path / os.up should be(pendingDir)
        retired.copyId should be(copyId)
        os.exists(retired.path / RebuildingStore.LostCrlStateFile) should be(true)
        stateBytes.foreach(b => os.read(retired.path / RebuildingStore.LostCrlStateFile) should be(b))
        os.exists(retired.path / StoreConfig.configFilename) should be(true)
        recordOf(restarted, loadedDevices(restarted)(deviceA), s) should be(None)
        restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- T3 and T9: into Live, after the rename and before its sync ----

  atest("T3: a crash after an Adopt flip's rename into live and before its sync reloads the copy from live"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val host = realHost(hostRoot, new TestOpener, rebuilds = new PlacingRebuilds(real = true))
    host.testingOnlyHandleEvents()
    @volatile var failing = true
    failSyncsOf(host, dev / CopyLayout.LiveDirectory, () => failing)
    for
      outcome <- rebuildAndFlip(host, UUID.randomUUID())
      _ <- pump(host, false, rounds = 10)
      beforeCrash = onlyCopy(dev, s)
      recordBeforeCrash = recordOf(host, loadedDevices(host)(deviceA), s).map(r => (r.phase, r.unsynced))
      servedBeforeCrash = host.control.router.loaded.contains(s)
      restarted = realHost(hostRoot, new TestOpener)
      _ <- pump(restarted, restarted.control.router.loaded.contains(s))
      servedAtRestart = restarted.control.router.loaded.contains(s)
      // The next check leaves it served there
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty)
    yield
      outcome should be(RebuildOutcome.Adopt)
      beforeCrash.phase should be(Phase.Live)
      recordBeforeCrash should be(Some((Phase.Live, true)))
      servedBeforeCrash should be(false)
      servedAtRestart should be(true)
      val copy = onlyCopy(dev, s)
      copy should be(beforeCrash)
      restarted.control.router.loaded(s).backend.path should be(copy.path)
      recordOf(restarted, loadedDevices(restarted)(deviceA), s).map(r => (r.phase, r.unsynced, r.activity)) should be(
        Some((Phase.Live, false, Activity.Served)))
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("T9: a crash after a committed pool update's rename into live and before its sync reloads the copy from live"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val host = realHost(hostRoot, new TestOpener, transfers = transfers)
    host.testingOnlyHandleEvents()
    @volatile var failing = true
    failSyncsOf(host, dev / CopyLayout.LiveDirectory, () => failing)
    for
      _ <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      pstate <- client.getStoragePoolState(moving.poolId)
      _ = transfers.finishTransferIn(moving, prepare = staged =>
            DefaultBackendOpener.createStore(StoreConfig.forPool(moving, pstate), staged,
              staged / os.up / StoreCreation.CreatingDirectory))
      _ <- pumpTx(host, recorded(host).transferOutcomes.contains(moving) &&
                        !host.control.transfers.receiving.contains(moving))
      beforeCrash = onlyCopy(dev, moving)
      recordBeforeCrash = recordOf(host, loadedDevices(host)(net.secondDeviceId), moving).map(r => (r.phase, r.unsynced))
      servedBeforeCrash = host.control.router.loaded.contains(moving)
      restarted = realHost(hostRoot, new TestOpener, transfers = new StagedTransferService)
      _ <- pump(restarted, restarted.control.router.loaded.contains(moving))
      servedAtRestart = restarted.control.router.loaded.contains(moving)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty)
    yield
      recorded(host).transferOutcomes.get(moving) should be(Some(TransferOutcome.PoolRepointed))
      beforeCrash.phase should be(Phase.Live)
      os.exists(beforeCrash.path / StoreLoader.ArrivedAtFile) should be(true)
      recordBeforeCrash should be(Some((Phase.Live, true)))
      servedBeforeCrash should be(false)
      servedAtRestart should be(true)
      val copy = onlyCopy(dev, moving)
      copy should be(beforeCrash)
      restarted.control.router.loaded(moving).backend.path should be(copy.path)
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- T8: into Arrived, after the promotion and before the pool update ----

  atest("T8: a crash after the promotion and before the pool update resumes the pool update and serves the copy"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = receivingDevice()
    val transfers = new StagedTransferService
    val host = realHost(hostRoot, new TestOpener, transfers = transfers)
    host.testingOnlyHandleEvents()
    // The pool update never runs on the first host: it crashes after the promotion
    recorded(host).holdTransferOutcomes()
    for
      tid <- startMoving(host)
      _ <- awaitBegun(host, transfers)
      pstate <- client.getStoragePoolState(moving.poolId)
      _ = transfers.finishTransferIn(moving, prepare = staged =>
            DefaultBackendOpener.createStore(StoreConfig.forPool(moving, pstate), staged,
              staged / os.up / StoreCreation.CreatingDirectory))
      _ <- awaitLoudly("the promotion"):
             host.testingOnlyHandleEvents()
             copiesOf(dev, moving).nonEmpty
      beforeCrash = onlyCopy(dev, moving)
      transfers2 = new StagedTransferService
      restarted = realHost(hostRoot, new TestOpener, transfers = transfers2)
      _ = restarted.testingOnlyHandleEvents()
      atRestart = onlyCopy(dev, moving)
      servedAtRestart = restarted.control.router.loaded.contains(moving)
      // The next check resumes the pool update (startIn: the copy is this transfer's), and moves the copy Live
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpTx(restarted, restarted.control.router.loaded.contains(moving))
      st <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      beforeCrash.phase should be(Phase.Arrived(tid.toString))
      recorded(host).transferOutcomes.get(moving) should be(None)
      atRestart should be(beforeCrash)
      servedAtRestart should be(false)
      recorded(restarted).transferOutcomes.get(moving) should be(Some(TransferOutcome.PoolRepointed))
      st.stores(moving).status should be(StorageDeviceState.StoreStatus.Active)
      // Not received again: the arrived copy itself went Live
      transfers2.transfersIn should be(Vector())
      val copy = onlyCopy(dev, moving)
      copy.phase should be(Phase.Live)
      copy.copyId should be(beforeCrash.copyId)
      restarted.control.router.loaded(moving).backend.path should be(copy.path)
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- T1: a create, after its rename into live and before its flip ----

  atest("T1: a crash after the create's rename into live and before its flip loads the copy and flips the entry"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val creating = StoreId(PoolId.BootstrapPoolId, 0)
    val initializing = Map(creating -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Initializing, None))
    // The create's open never completes: the host crashes once its copy is in live/, before the flip
    val opener = new TestOpener
    opener.openGate = Some(Promise[Unit]().future)
    val host = realHost(hostRoot, opener)
    host.testingOnlyHandleEvents()
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = initializing))
      _ <- waitForTransactionsToComplete()
      _ = host.control.reconciler.checkAll()
      _ <- pumpTx(host, copiesOf(dev, creating).nonEmpty && opener.opens.get > 0)
      beforeCrash = onlyCopy(dev, creating)
      stBeforeCrash <- client.getStorageDeviceState(deviceA)
      restarted = realHost(hostRoot, new TestOpener)
      _ <- pump(restarted, restarted.control.router.loaded.contains(creating))
      servedAtRestart = restarted.control.router.loaded.contains(creating)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty &&
                             !restarted.control.rebuilds.createFlipsPending.contains(creating))
      st <- client.getStorageDeviceState(deviceA)
    yield
      beforeCrash.phase should be(Phase.Live)
      stBeforeCrash.stores(creating).status should be(StorageDeviceState.StoreStatus.Initializing)
      servedAtRestart should be(true)
      // Counted as created: flipped over the copy, never created again beside it
      st.stores(creating).status should be(StorageDeviceState.StoreStatus.Active)
      onlyCopy(dev, creating) should be(beforeCrash)
      restarted.control.router.loaded(creating).backend.path should be(beforeCrash.path)
      inDir(dev, StoreCreation.CreatingDirectory) should be(Nil)
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  // ---- T4: Rebuilt to Parked, at an AdoptOffline flip ----

  /** Starts the rebuild of `s` on device A of `host` for `episode`, its entry Rebuilding with the device ShuttingDown,
   *  so its flip is AdoptOffline, and drains `host` until `crashPoint`: the host crashes there. Every device check of
   *  the host fails its read of the device's state, so its shutdown never starts: the flip's move is made on the
   *  loaded device, and nothing after the crash point is drained */
  private def adoptOfflineUntil(host: Host, episode: UUID)(crashPoint: => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    (1 to 100).foreach(_ => recorded(host).armLookupThrow(deviceA, new java.io.IOException("injected: no device check")))
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(s, episode).copy(status = StorageDeviceState.DeviceStatus.ShuttingDown))
      _ <- waitForTransactionsToComplete()
      _ = host.control.rebuilds.startRebuild(loadedDevices(host)(deviceA), s, Some(episode), acceptLoss = false)
      _ <- pumpTx(host, crashPoint)
    yield ()

  /** A fresh host over `hostRoot` after the crash: it loads the device (its owner, no shutdown marker) and checks
   *  nothing. Then the device is restarted, its entry Active again, and the host's checks run until the copy of `s` is
   *  Live. Yields the host, the record of the copy at the restart's load (its phase, whether a load was offered), and
   *  the copy then */
  private def restartDevice(hostRoot: os.Path, dev: os.Path)
      : Future[(Host, Option[(Phase, Boolean)], CopyLayout.Found)] =
    given ExecutionContext = executionContext
    val restarted = newHost(hostRoot)
    for
      _ <- pumpTx(restarted, false, rounds = 10)
      loadedBefore = loadedDevices(restarted).get(deviceA).flatMap(recordOf(restarted, _, s)).map(r =>
        (r.phase, recorded(restarted).storeLoadAttempts.nonEmpty))
      atRestart = onlyCopy(dev, s)
      _ <- client.restartStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ <- awaitLoudly("the restarted device's copy to go live"):
             restarted.testingOnlyHandleEvents()
             if restarted.control.reconciler.activeDeviceChecks.isEmpty then
               if loadedDevices(restarted).contains(deviceA) then restarted.control.reconciler.checkAll()
               else restarted.control.discovery.checkForNewDevices()
             copiesOf(dev, s).exists(_.phase == Phase.Live)
    yield (restarted, loadedBefore, atRestart)

  atest("T4: a crash after the move to parked and before the AdoptOffline flip's end keeps the copy parked until " +
        "its entry is Active"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuilds(real = false))
    host.testingOnlyHandleEvents()
    for
      _ <- adoptOfflineUntil(host, UUID.randomUUID())(copiesOf(dev, s).exists(_.phase == Phase.Parked))
      beforeCrash = onlyCopy(dev, s)
      st <- client.getStorageDeviceState(deviceA)
      (restarted, loadedBefore, atRestart) <- restartDevice(hostRoot, dev)
    yield
      recorded(host).rebuildOutcomes.get((deviceA, s)) should be(Some(RebuildOutcome.AdoptOffline))
      st.stores(s).status should be(StorageDeviceState.StoreStatus.Offline)
      beforeCrash.phase should be(Phase.Parked)
      // Loaded, the copy registered Parked and never offered to the loader
      loadedBefore should be(Some((Phase.Parked, false)))
      atRestart should be(beforeCrash)
      // Restarted, its entry Active: the parked copy went Live (T5) and was offered to the loader
      val copy = onlyCopy(dev, s)
      copy.phase should be(Phase.Live)
      copy.copyId should be(beforeCrash.copyId)
      recorded(restarted).storeLoadAttempts.toList.map(_._2) should contain(copy.path)
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("T4: a crash after the AdoptOffline flip and before its move keeps the copy rebuilt until its entry is Active"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuilds(real = false))
    host.testingOnlyHandleEvents()
    val parkedDir = dev / CopyLayout.ParkedDirectory
    // The move fails, standing for the crash between the flip's commit and the move
    @volatile var attempted = false
    host.control.moves.rename = (from, to) =>
      if to / os.up == parkedDir then
        attempted = true
        throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    val episode = UUID.randomUUID()
    for
      _ <- adoptOfflineUntil(host, episode)(attempted)
      beforeCrash = onlyCopy(dev, s)
      st <- client.getStorageDeviceState(deviceA)
      (restarted, loadedBefore, atRestart) <- restartDevice(hostRoot, dev)
    yield
      recorded(host).rebuildOutcomes.get((deviceA, s)) should be(Some(RebuildOutcome.AdoptOffline))
      st.stores(s).status should be(StorageDeviceState.StoreStatus.Offline)
      beforeCrash.phase should be(Phase.Rebuilt(episode.toString))
      loadedBefore should be(Some((Phase.Rebuilt(episode.toString), false)))
      atRestart should be(beforeCrash)
      // Restarted, its entry Active: the Completed Rebuilds pass moved it Live (T3) and offered it to the loader
      val copy = onlyCopy(dev, s)
      copy.phase should be(Phase.Live)
      copy.copyId should be(beforeCrash.copyId)
      recorded(restarted).storeLoadAttempts.toList.map(_._2) should contain(copy.path)
      restarted.control.copyRegistry.takeRegistryErrors() should be(Vector())
