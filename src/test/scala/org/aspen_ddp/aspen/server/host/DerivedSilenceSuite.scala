package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import org.aspen_ddp.aspen.server.store.backend.{DefaultBackendOpener, StoreCreation}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** The router's silence and the view's repairability, derived from the copy registry (store copy state machine,
 *  stage 1, Task 6): a store is served if a copy of it is Served on any device of the host, otherwise silent while
 *  any record, work, unloaded device's entry or pending device decision names it, otherwise unknown */
class DerivedSilenceSuite extends IntegrationTestSuite with ControlFixture:

  private def sid(index: Int): StoreId =
    StoreId(PoolId(UUID.fromString("22222222-2222-2222-2222-222222222222")), index.toByte)

  private val deviceC = StorageDeviceId(UUID.fromString("cccccccc-0000-0000-0000-000000000003"))

  /** A device directory holding its CRL directory, so the load does not take its CRL for lost */
  private def device(hostRoot: os.Path, name: String, deviceId: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, name, deviceId)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  /** A complete RocksDB store at its path on `dev`, as a create leaves it */
  private def realStore(dev: os.Path, storeId: StoreId): os.Path =
    val storePath = CopyFixtures.freshCopyPath(dev, storeId)
    DefaultBackendOpener.createStore(StoreConfig(storeId, StoreConfig.RocksDB(None), None, Replication(3, 2)),
      storePath, dev / StoreCreation.CreatingDirectory)
    storePath

  /** Pumps the test's queue, the network's transactions and `host`'s events until `condition` holds */
  private def pumpUntil(host: Host, condition: => Boolean, rounds: Int = 20): Future[Unit] =
    given ExecutionContext = executionContext
    if condition || rounds == 0 then
      Future.unit
    else
      host.testingOnlyHandleEvents()
      for
        _ <- yieldUntil(condition, 20)
        _ <- waitForTransactionsToComplete()
        _ <- pumpUntil(host, condition, rounds - 1)
      yield ()

  /** Closes `storeId` as Host.closeStore does, draining until it is closed */
  private def close(host: Host, storeId: StoreId): Future[Unit] =
    given ExecutionContext = executionContext
    val closed = host.closeStore(storeId)
    pumpUntil(host, closed.isCompleted).flatMap(_ => closed)

  /** A device check of `deviceId`, not loaded here, whose tree entry lists `stores` Active */
  private def checkUnloaded(host: Host, deviceId: StorageDeviceId, stores: Set[StoreId]): Future[Unit] =
    val armed = recorded(host).armLookup(deviceId)
    host.control.reconciler.checkStorageDevice(deviceId)
    armed.success(deviceState(deviceId, stores.map(_ ->
      StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)).toMap))
    pumpUntil(host, host.control.copyRegistry.unloadedEntries.contains(deviceId))

  private def rebuildWork(host: Host, deviceId: StorageDeviceId, storeId: StoreId, on: Boolean): Unit =
    host.control.copyRegistry.work("test", deviceId, storeId, _.isInstanceOf[Work.Rebuild],
      if on then Some(Work.Rebuild(None, RebuildStage.Queued)) else None)

  atest("served on one device dominates every other device's record"):
    given ExecutionContext = executionContext
    val s = sid(0)
    val hostRoot = newHostDir()
    val devA = device(hostRoot, "dev0", deviceA)
    val devB = device(hostRoot, "dev1", deviceB)
    val storePath = realStore(devA, s)
    // Device B holds a lost copy of the store
    CopyFixtures.placeCopy(devB, s, Phase.Lost)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    host.control.loader.tryLoadStore(loadedDevices(host)(deviceA), storePath)
    for
      _ <- pumpUntil(host, host.control.router.loaded.contains(s))
      // ... a rebuild of it, and an unloaded device listing it
      _ = rebuildWork(host, deviceB, s, on = true)
      _ <- checkUnloaded(host, deviceC, Set(s))
      answersUnknown = host.control.router.answersUnknown(s)
      delivered = scala.concurrent.Promise[Unit]()
      _ = host.control.router.post(s)(_ => delivered.trySuccess(()))
      _ <- pumpUntil(host, delivered.isCompleted)
      // Queued on B: a rebuild not yet reading, which keeps no repair scan from the served copy (R20)
      repairableQueued = host.isRepairable(s)
      _ = host.control.copyRegistry.work("test", deviceB, s, _.isInstanceOf[Work.Rebuild],
            Some(Work.Rebuild(None, RebuildStage.Running)))
      // Running on B: the rebuild reads everything, as rebuilding(RebuildReads) keeps the view from repairing it
      repairableRunning = host.isRepairable(s)
      // Abandoned on B: cancelled but still running past finish's latch, its slot not yet released
      _ = host.control.copyRegistry.work("test", deviceB, s, _.isInstanceOf[Work.Rebuild],
            Some(Work.Rebuild(None, RebuildStage.Abandoned)))
      repairableAbandoned = host.isRepairable(s)
      _ = rebuildWork(host, deviceB, s, on = false)
      repairableWithout = host.isRepairable(s)
      derived = host.control.copyRegistry.silence(s)
      _ <- close(host, s)
    yield
      host.control.copyRegistry.device(deviceB).flatMap(_.copies.get(s)).map(_.phase) should be(Some(Phase.Lost))
      answersUnknown should be(false)
      delivered.isCompleted should be(true)
      repairableQueued should be(true)
      repairableRunning should be(false)
      repairableAbandoned should be(false)
      repairableWithout should be(true)
      derived should be(Silence.Served)

  atest("an unloaded device's listed stores stay silent until it loads"):
    given ExecutionContext = executionContext
    val (served, rebuilt, listed) = (sid(1), sid(2), sid(3))
    val hostRoot = newHostDir()
    val dev = device(hostRoot, "dev0", deviceA)
    val storePath = realStore(dev, served)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    local.reconciledEntries = Some(Map(
      served -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None),
      rebuilt -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None),
      listed -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)))
    host.control.loader.tryLoadStore(local, storePath)
    for
      _ <- pumpUntil(host, host.control.router.loaded.contains(served))
      _ = rebuildWork(host, deviceA, rebuilt, on = true)
      _ <- close(host, served)
      // Unloaded with its copy, its rebuild's work and its listing: crlFailed, a shutdown, a relinquish
      _ = host.control.shutdown.unloadDevice(deviceA)
      unloaded = List(served, rebuilt, listed).map(host.control.copyRegistry.silence)
      unloadedAnswer = List(served, rebuilt, listed).map(host.control.router.answersUnknown)
      _ <- yieldUntil(!host.control.devices.closingCrls.contains(dev))
      reloaded = discoverNow(host)
      _ = host.testingOnlyHandleEvents()
      // Loaded, before its first reconcile: still silent by what its last tree entry listed
      beforeReconcile = List(served, rebuilt, listed).map(host.control.router.answersUnknown)
      // Its first reconcile reads only the served store's entry
      _ = loadedDevices(host)(deviceA).reconciledEntries = Some(Map(
            served -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)))
      afterReconcile = List(served, rebuilt, listed).map(host.control.copyRegistry.silence)
    yield
      unloaded should be(List.fill(3)(Silence.Silent(Set(SilenceReason.UnloadedDevice))))
      unloadedAnswer should be(List(false, false, false))
      host.control.copyRegistry.device(deviceA) should not be empty
      reloaded should be(List(deviceA))
      beforeReconcile should be(List(false, false, false))
      afterReconcile should be(List(Silence.Silent(Set(SilenceReason.CopyHere)), Silence.Unknown, Silence.Unknown))
      host.control.router.answersUnknown(listed) should be(true)

  test("a store whose last record and work go is Unknown"):
    val s = sid(4)
    val hostRoot = newHostDir()
    val dev = device(hostRoot, "dev0", deviceA)
    writeStoreConfig(dev, s)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    rebuildWork(host, deviceA, s, on = true)
    val withBoth = host.control.router.answersUnknown(s)
    host.control.copyRegistry.removed("test", local, s)
    val withWork = host.control.router.answersUnknown(s)
    rebuildWork(host, deviceA, s, on = false)
    withBoth should be(false)
    withWork should be(false)
    host.control.copyRegistry.silence(s) should be(Silence.Unknown)
    host.control.router.answersUnknown(s) should be(true)

  // The regression test of D2 (spec, Stage 1 audit findings, Step 3 addenda): a check of a device not loaded here
  // listed every store of its entry offline by store id, one served from another device of this host included
  atest("the unloaded-device branch does not silence a served store"):
    given ExecutionContext = executionContext
    val (served, elsewhere) = (sid(5), sid(6))
    val hostRoot = newHostDir()
    val dev = device(hostRoot, "dev0", deviceA)
    val storePath = realStore(dev, served)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    host.control.loader.tryLoadStore(loadedDevices(host)(deviceA), storePath)
    for
      _ <- pumpUntil(host, host.control.router.loaded.contains(served))
      _ <- checkUnloaded(host, deviceB, Set(served, elsewhere))
      servedAnswer = host.control.router.answersUnknown(served)
      repairable = host.isRepairable(served)
      derived = host.control.copyRegistry.silence(served)
      elsewhereAnswer = host.control.router.answersUnknown(elsewhere)
      _ <- close(host, served)
    yield
      servedAnswer should be(false)
      repairable should be(true)
      derived should be(Silence.Served)
      elsewhereAnswer should be(false)
      host.control.copyRegistry.silence(elsewhere) should be(Silence.Silent(Set(SilenceReason.UnloadedDevice)))

  // I3: the reconcile's passes select from the device's offline copies, which leave a copy whose store has work on
  // the device to that work. Its entry gone from the device (an abort, a re-placement), the Deleted Stores pass would
  // otherwise retire the copy under the work, which then discards or promotes the same path
  for (what, phase, work) <- List(
        ("an arrived copy while its transfer in's pool update is in flight", Phase.Arrived(""),
         Work.TransferIn(None, TransferInStage.Recording)),
        ("a rebuilt copy while its rebuild's flip is in flight", Phase.Rebuilt(""),
         Work.Rebuild(None, RebuildStage.Running)))
  do
    atest(s"the Deleted Stores pass leaves $what to its work"):
      given ExecutionContext = executionContext
      val s = sid(7)
      val hostRoot = newHostDir()
      val host = newHost(hostRoot)
      host.testingOnlyHandleEvents()
      // Injected, so no device load's own check is in flight
      val dev = hostRoot / os.up / "injected-device"
      os.makeDir.all(dev)
      CopyFixtures.placeCopy(dev, s, phase)
      val local = new LocalStorageDeviceState(deviceA, dev, dev / "dummy.file", new org.aspen_ddp.aspen.TestNetwork.TestCRL(), host.control.deviceIos.forPath(dev))
      injectLoadedDevice(host, local)
      host.control.copyRegistry.work("test", deviceA, s, w => w.ordinal == work.ordinal, Some(work))
      val armed = recorded(host).armLookup(deviceA)
      host.control.reconciler.checkStorageDevice(deviceA)
      // Its entry is gone from the device
      armed.success(deviceState(deviceA))
      for
        // The check ends, or a retire it started moves the copy away (and checks the device again)
        _ <- pumpUntil(host, !host.control.reconciler.activeDeviceChecks.contains(deviceA) ||
                             CopyFixtures.copyPath(dev, s).isEmpty)
        // A retire, had one started, completes within these rounds (its CRL calls answer at once)
        _ <- pumpUntil(host, CopyFixtures.copyPath(dev, s).isEmpty, rounds = 5)
      yield
        offlineCopies(host, local) should not contain s
        CopyFixtures.phaseOf(dev, s) should be(Some(phase))
        host.control.copyRegistry.device(deviceA).flatMap(_.copies.get(s)) should not be empty

