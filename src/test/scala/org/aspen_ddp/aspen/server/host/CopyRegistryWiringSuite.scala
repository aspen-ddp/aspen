package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StorageDeviceConfig}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{RebuildOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.BackendOpener
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

/** A rebuild that finishes at once, its copy placed at the store's path as RebuildingStore.finish places it: a store
 *  config and the rebuild-complete marker of its episode */
private class PlacingRebuildFactory(writeConfig: (os.Path, StoreId) => os.Path)(using ec: ExecutionContext)
    extends StoreRebuildFactory:
  def createRebuild(client: AspenClient, storeId: StoreId, storageDeviceId: StorageDeviceId, devicePath: os.Path,
                    backends: BackendOpener, episode: Option[UUID]): StoreRebuild =
    val id = storeId
    // Placed off the control thread, as finish's move is, and completed right after it
    val placed = Future:
      val dir = writeConfig(devicePath, id)
      os.write.over(dir / RebuildingStore.RebuildCompleteMarkerFile, RebuildingStore.rebuildCompleteContent(episode))
      ()
    new StoreRebuild:
      val storeId: StoreId = id
      def complete: Future[Unit] = placed
      def cancel(): Unit = ()

/** The copy registry in shadow mode (store copy state machine, stage 1): a device load registers each store directory
 *  with its phase, every disk mutation keeps it, and the drain check (Host.Seams.verifyCopyRegistry) finds no
 *  disagreement with the disk */
class CopyRegistryWiringSuite extends IntegrationTestSuite with ControlFixture:

  private def sid(index: Int): StoreId =
    StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), index.toByte)

  /** A device directory holding its CRL directory, so the load does not take its CRL for lost */
  private def device(hostRoot: os.Path, deviceId: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, "dev0", deviceId)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  private def phases(host: Host, deviceId: StorageDeviceId): Map[StoreId, Phase] =
    host.control.copyRegistry.device(deviceId).map(_.copies.view.mapValues(_.phase).toMap).getOrElse(Map())

  test("a device load registers each store directory with its phase"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    writeStoreConfig(dev, sid(0))
    os.write(writeStoreConfig(dev, sid(1)) / RebuildingStore.RebuildRequiredMarkerFile, "")
    os.write(writeStoreConfig(dev, sid(2)) / StoreTransfer.TransferOutMarkerFile, "t-out\n")
    os.write(writeStoreConfig(dev, sid(3)) / StoreTransfer.TransferredInMarkerFile, "t-in")
    os.write(writeStoreConfig(dev, sid(4)) / RebuildingStore.RebuildCompleteMarkerFile, "ep")
    // Not store directories: a scratch area and a directory whose name is no store's
    os.makeDir.all(dev / "creating")
    os.makeDir.all(dev / "not-a-store")

    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()

    phases(host, deviceA) should be(Map(
      sid(0) -> Phase.Live,
      sid(1) -> Phase.Lost,
      sid(2) -> Phase.Outbound("t-out"),
      sid(3) -> Phase.Arrived("t-in"),
      sid(4) -> Phase.Rebuilt("ep")))
    host.control.copyRegistry.device(deviceA).get.copies.values.map(_.activity).toSet should be(Set(Activity.Idle))
    host.control.copyRegistry.verifyAgainstDisk() should be(Nil)

  test("an unreadable copy registers Unread and stays silent"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val dir = writeStoreConfig(dev, sid(0))
    os.perms.set(dir, "rw-------")
    try
      val host = newHost(hostRoot, verifyCopyRegistry = true)
      // The drain's check skips what cannot be read: it neither throws nor reports
      host.testingOnlyHandleEvents()
      phases(host, deviceA) should be(Map(sid(0) -> Phase.Unread))
      host.control.copyRegistry.verifyAgainstDisk() should be(Nil)
      host.control.copyRegistry.takeShadowErrors() should be(Vector())
    finally os.perms.set(dir, "rwx------")

  atest("a restart rebuilds the same phases"):
    given ExecutionContext = executionContext
    // A store of the bootstrap pool, whose pool entry names the bootstrap device: startOut reads both fresh
    val shipped = StoreId(PoolId.BootstrapPoolId, 1)
    val source = StorageDeviceId.BootstrapStorageDeviceId
    val rebuiltId = sid(7)
    val hostRoot = newHostDir()
    val dev = device(hostRoot, source)
    writeStoreConfig(dev, shipped)
    writeStoreConfig(dev, sid(0))
    os.write(writeStoreConfig(dev, sid(1)) / StoreTransfer.TransferredInMarkerFile, "t-in")
    os.write(writeStoreConfig(dev, sid(2)) / RebuildingStore.RebuildCompleteMarkerFile, "kept")
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends,
                       storeRebuildFactory = new PlacingRebuildFactory(writeStoreConfig)(using executionContext),
                       verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    val episode = UUID.randomUUID()

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      started <- host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, source,
                   shipped, HLCTimestamp.now, UUID.randomUUID())).transform(Success(_))
      _ <- yieldUntil(sends.isTransferringOut(shipped))
      _ = host.testingOnlyHandleEvents()
      _ = host.control.rebuilds.startRebuild(local, rebuiltId, Some(episode), acceptLoss = false)
      _ <- yieldUntil(recorded(host).rebuildOutcomes.contains((source, rebuiltId)))
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyHandleEvents()
      before = phases(host, source)
      restarted = newHost(hostRoot, verifyCopyRegistry = true)
      _ = restarted.testingOnlyHandleEvents()
    yield
      started should be(Success(()))
      before(shipped) should be(a[Phase.Outbound])
      before(sid(0)) should be(Phase.Live)
      before(sid(1)) should be(Phase.Arrived("t-in"))
      before(sid(2)) should be(Phase.Rebuilt("kept"))
      // The bootstrap device's entry does not list the rebuilt store, so its flip discards the copy
      recorded(host).rebuildOutcomes((source, rebuiltId)) should be(DeviceMetadata.RebuildOutcome.Discard)
      before.contains(rebuiltId) should be(false)
      phases(restarted, source) should be(before)
      val copies = restarted.control.copyRegistry.device(source).get
      copies.copies.values.map(_.activity).toSet should be(Set(Activity.Idle))
      copies.work should be(Map())
      host.control.copyRegistry.verifyAgainstDisk() should be(Nil)
      restarted.control.copyRegistry.verifyAgainstDisk() should be(Nil)

  // ---- Fix round 1: attach is total (I1), learning and refreshing at load (I3), untracked records (risk 2), and one
  //      wiring test per transition class (I4) ----

  // A store of the bootstrap pool, whose pool entry names the bootstrap device
  private val shipped = StoreId(PoolId.BootstrapPoolId, 1)
  private val source = StorageDeviceId.BootstrapStorageDeviceId

  /** Pumps the test's queue, the network's transactions and `host`'s events until `condition` holds. Silent on
   *  exhaustion */
  private def pumpUntil(host: Host, condition: => Boolean, rounds: Int = 20, drain: Boolean = true): Future[Unit] =
    given ExecutionContext = executionContext
    if condition || rounds == 0 then
      Future.unit
    else
      if drain then
        host.testingOnlyHandleEvents()
      for
        _ <- yieldUntil(condition, 20)
        _ <- waitForTransactionsToComplete()
        _ <- pumpUntil(host, condition, rounds - 1, drain)
      yield ()

  /** As pumpUntil, never draining `host`: a rebuild's copy is placed off the control thread before its completion's
   *  callback registers it, and a drain check in between would see a directory with no record */
  private def awaitRebuild(host: Host, deviceId: StorageDeviceId, storeId: StoreId): Future[Unit] =
    pumpUntil(host, recorded(host).rebuildOutcomes.contains((deviceId, storeId)), drain = false)

  /** The registry agrees with the disk, no update threw, and no load had to learn a copy */
  private def consistent(host: Host): org.scalatest.Assertion =
    host.testingOnlyHandleEvents()
    host.control.copyRegistry.verifyAgainstDisk() should be(Nil)
    host.control.copyRegistry.takeShadowErrors() should be(Vector())
    host.control.copyRegistry.learned should be(Vector())

  private def ask(host: Host): Future[Try[Unit]] =
    given ExecutionContext = executionContext
    host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, source, shipped,
      HLCTimestamp.now, UUID.randomUUID())).transform(Success(_))

  test("attach registers a child whose stat fails as Unread, and never throws"):
    val hostRoot = newHostDir()
    device(hostRoot, deviceA)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    // A store directory name linking into a directory without search permission: its stat fails (EACCES)
    val other = hostRoot / os.up / "other-device"
    val closed = hostRoot / os.up / "closed"
    os.makeDir.all(closed / "inside")
    os.makeDir.all(other)
    val child = other / sid(0).directoryName
    os.symlink(child, closed / "inside")
    os.perms.set(closed, "rw-------")
    try
      val sds = new LocalStorageDeviceState(deviceB, other, other / StorageDeviceConfig.configFilename,
        new org.aspen_ddp.aspen.TestNetwork.TestCRL)
      noException should be thrownBy host.control.copyRegistry.attach(sds, List(child, other / "not-a-store"))
      phases(host, deviceB) should be(Map(sid(0) -> Phase.Unread))
      host.control.copyRegistry.takeShadowErrors() should be(Vector())
    finally os.perms.set(closed, "rwx------")

  test("an Unread copy that later reads cleanly is refreshed at its load"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val dir = writeStoreConfig(dev, sid(0))
    os.write(dir / RebuildingStore.RebuildCompleteMarkerFile, "ep")
    os.perms.set(dir, "rw-------")
    val (host, waiting) =
      try
        val h = newHost(hostRoot, verifyCopyRegistry = true)
        h.testingOnlyHandleEvents()
        // The device loaded, and only this store waits for a retry: the fixture's tryLoadStore only records, so the
        // loader's own is called, as a device scan's retry calls it
        h.control.loader.tryLoadStore(loadedDevices(h)(deviceA), dir)
        h.testingOnlyHandleEvents()
        (h, h.control.loader.awaitingRetryWaitScans(sid(0)))
      finally os.perms.set(dir, "rwx------")
    val local = loadedDevices(host)(deviceA)
    waiting should be(defined)
    phases(host, deviceA) should be(Map(sid(0) -> Phase.Unread))
    host.control.loader.tryLoadStore(local, dir)
    phases(host, deviceA) should be(Map(sid(0) -> Phase.Rebuilt("ep")))
    consistent(host)

  test("a copy placed after its device's load is learned when a load offers it"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val dir = writeStoreConfig(dev, sid(0))
    os.write(dir / StoreTransfer.TransferOutMarkerFile, "t")
    host.control.copyRegistry.verifyAgainstDisk().size should be(1)
    host.control.loader.tryLoadStore(local, dir)
    phases(host, deviceA) should be(Map(sid(0) -> Phase.Outbound("t")))
    host.control.copyRegistry.learned.size should be(1)
    host.control.copyRegistry.verifyAgainstDisk() should be(Nil)
    host.control.copyRegistry.takeShadowErrors() should be(Vector())

  atest("a store served from an unloaded device has an untracked record until the router removes it"):
    given ExecutionContext = executionContext
    val smgr = net.smgr
    val storeId = net.stores.head.storeId
    def record: Option[CopyRecord] =
      smgr.control.copyRegistry.device(net.storageDeviceId).flatMap(_.copies.get(storeId))
    val loaded = record
    for
      _ <- smgr.closeStore(storeId).transform(Success(_))
      _ = handleEvents()
    yield
      loaded.map(r => (r.phase, r.activity)) should be(Some((Phase.Live, Activity.Idle)))
      record should be(None)
      smgr.control.copyRegistry.verifyAgainstDisk() should be(Nil)

  atest("a create registers its copy Live"):
    given ExecutionContext = executionContext
    val created = StoreId(PoolId.BootstrapPoolId, 2)
    val hostRoot = newHostDir()
    device(hostRoot, source)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    val lockstep = host.threading.asInstanceOf[LockstepThreading]
    def isLoad(task: Runnable): Boolean = task match
      case e: Host.EventTask => e.event.isInstanceOf[Host.LoadStore]
      case _ => false
    host.control.rebuilds.createNewStore(local, created)
    for
      _ <- yieldUntil(lockstep.queuedItems.exists(isLoad))
      // Not installed: the bootstrap host serves this store. Its opened backend is closed instead
      loads = lockstep.removeQueued(isLoad)
      _ <- Future.sequence(loads.collect { case e: Host.EventTask => e.event }.collect {
             case l: Host.LoadStore => l.backend.close() })
    yield
      loads.size should be(1)
      phases(host, source) should be(Map(created -> Phase.Live))
      consistent(host)

  for (variant, deviceStatus, expected) <- List(
        ("Adopt", StorageDeviceState.DeviceStatus.Operational, Phase.Live),
        ("AdoptOffline", StorageDeviceState.DeviceStatus.ShuttingDown, Phase.Outbound("")))
  do
    atest(s"a rebuild's finish registers Rebuilt and its flip moves the copy ($variant)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      device(hostRoot, deviceA)
      val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuildFactory(writeStoreConfig)(using executionContext),
                         verifyCopyRegistry = true)
      host.testingOnlyHandleEvents()
      val local = loadedDevices(host)(deviceA)
      val episode = UUID.randomUUID()
      for
        // In the tree, so its flip reads the entry; only this device's own entries are reconciled after it
        _ <- net.createDevice(deviceA)
        _ <- setDeviceState(deviceA)(_.setRebuilding(sid(0), episode).copy(status = deviceStatus))
        _ <- waitForTransactionsToComplete()
        _ = host.control.rebuilds.startRebuild(local, sid(0), Some(episode), acceptLoss = false)
        _ <- awaitRebuild(host, deviceA, sid(0))
        _ <- pumpUntil(host, !host.control.rebuilds.rebuildingStoreIds.contains(sid(0)), drain = false)
        outcome = recorded(host).rebuildOutcomes((deviceA, sid(0)))
        // The device's own records: the check after an AdoptOffline flip finds the device shutting down and unloads it
        after = local.copies.copies.view.mapValues(_.phase).toMap
      yield
        outcome.toString should be(variant)
        after should be(Map(sid(0) -> expected))
        CopyPhase.read(local.devicePath / sid(0).directoryName) should be(expected)
        consistent(host)

  atest("a rebuild whose flip discards its copy unregisters it"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    device(hostRoot, source)
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuildFactory(writeStoreConfig)(using executionContext),
                       verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    // Not listed by the bootstrap device's entry
    host.control.rebuilds.startRebuild(local, sid(7), Some(UUID.randomUUID()), acceptLoss = false)
    for
      _ <- awaitRebuild(host, source, sid(7))
    yield
      recorded(host).rebuildOutcomes((source, sid(7))) should be(RebuildOutcome.Discard)
      phases(host, source) should be(Map())
      os.exists(local.devicePath / sid(7).directoryName) should be(false)
      consistent(host)

  atest("a rebuild over a foreign copy marks it Lost, retires it to pending-delete, and adopts its own"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    writeStoreConfig(dev, sid(0))
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuildFactory(writeStoreConfig)(using executionContext),
                       verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val episode = UUID.randomUUID()
    val before = phases(host, deviceA)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(sid(0), episode))
      _ <- waitForTransactionsToComplete()
      _ = host.control.rebuilds.startRebuild(local, sid(0), Some(episode), acceptLoss = false)
      // The foreign copy is marked, its discard retires it, and the slot passes to the rebuild
      _ <- awaitRebuild(host, deviceA, sid(0))
      outcome = recorded(host).rebuildOutcomes((deviceA, sid(0)))
      after = phases(host, deviceA)
    yield
      before should be(Map(sid(0) -> Phase.Live))
      outcome should be(RebuildOutcome.Adopt)
      after should be(Map(sid(0) -> Phase.Live))
      consistent(host)

  atest("markRebuildRequired moves a copy to Lost, and a retire takes it to pending-delete"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val dir = writeStoreConfig(dev, sid(0))
    writeStoreConfig(dev, sid(1))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    host.control.rebuilds.markRebuildRequired(dir)
    consistent(host)
    val lost = phases(host, deviceA)
    for
      retired <- host.control.copies.retire(local, sid(1))
    yield
      lost should be(Map(sid(0) -> Phase.Lost, sid(1) -> Phase.Live))
      retired should be(true)
      phases(host, deviceA) should be(Map(sid(0) -> Phase.Lost))
      os.list(dev / RebuildingStore.PendingDeleteDirectory).size should be(1)
      consistent(host)

  test("a lost device CRL registers its copies Lost, except one that holds its own state"):
    val hostRoot = newHostDir()
    // No crl/ beside its stores: the device's CRL is lost
    val dev = writeDevice(hostRoot, "dev0", deviceA)
    writeStoreConfig(dev, sid(0))
    os.write(writeStoreConfig(dev, sid(1)) / RebuildingStore.RebuildCompleteMarkerFile, "ep")
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    phases(host, deviceA) should be(Map(sid(0) -> Phase.Lost, sid(1) -> Phase.Rebuilt("ep")))
    consistent(host)

  for (variant, rebuilt) <- List(("live copy", false), ("Unconfirmed rebuilt copy", true)) do
    atest(s"a transfer out marks its copy Outbound and an abandoned one is reinstated ($variant)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      val dev = device(hostRoot, source)
      val dir = writeStoreConfig(dev, shipped)
      if rebuilt then
        os.write(dir / RebuildingStore.RebuildCompleteMarkerFile, "ep")
      val sends = new StagedSendService
      val host = newHost(hostRoot, transferService = sends, verifyCopyRegistry = true)
      host.testingOnlyHandleEvents()
      val local = loadedDevices(host)(source)
      val before = if rebuilt then Phase.Rebuilt("ep") else Phase.Live
      for
        _ <- net.createSecondDevice()
        _ <- waitForTransactionsToComplete()
        _ <- client.transferStore(shipped, net.secondDeviceId)
        _ <- waitForTransactionsToComplete()
        started <- ask(host)
        _ <- pumpUntil(host, sends.isTransferringOut(shipped))
        outbound = phases(host, source)(shipped)
        _ <- client.abortStoreTransfer(shipped)
        _ <- waitForTransactionsToComplete()
        _ = sends.endTransferOut(shipped)
        _ = host.control.transfersOut.reinstateAbandonedOut(local, shipped)
      yield
        started should be(Success(()))
        outbound should be(a[Phase.Outbound])
        phases(host, source) should be(Map(shipped -> before))
        consistent(host)

  for (variant, extra) <- List(("arrived copy", Nil), ("shipped rebuilt copy", List(RebuildingStore.RebuildCompleteMarkerFile)))
  do
    atest(s"a transfer in registers its promoted copy Arrived and its pool update makes it Live ($variant)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      val dev = device(hostRoot, net.secondDeviceId)
      val transfers = new StagedTransferService
      val host = newHost(hostRoot, transferService = transfers, verifyCopyRegistry = true)
      host.testingOnlyHandleEvents()
      for
        _ <- net.createSecondDevice()
        _ <- waitForTransactionsToComplete()
        _ <- client.transferStore(shipped, net.secondDeviceId)
        _ <- waitForTransactionsToComplete()
        _ = host.control.reconciler.checkAll()
        _ <- pumpUntil(host, transfers.startedTransfersIn.contains(shipped))
        // Held between the promotion and its pool update
        gate = recorded(host).holdTransferOutcomes()
        _ = transfers.finishTransferIn(shipped, extraFiles = extra)
        _ <- pumpUntil(host, phases(host, net.secondDeviceId).contains(shipped))
        arrived = phases(host, net.secondDeviceId)
        _ = consistent(host)
        _ = gate.success(())
        _ <- pumpUntil(host, recorded(host).transferOutcomes.contains(shipped))
        _ = host.testingOnlyHandleEvents()
      yield
        arrived(shipped) should be(a[Phase.Arrived])
        recorded(host).transferOutcomes(shipped) should be(TransferOutcome.PoolRepointed)
        phases(host, net.secondDeviceId) should be(Map(shipped -> Phase.Live))
        consistent(host)

  atest("a transfer in deletes an arrived copy of another transfer and unregisters it"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, net.secondDeviceId)
    os.write(writeStoreConfig(dev, shipped) / StoreTransfer.TransferredInMarkerFile, "another-transfer")
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val before = phases(host, net.secondDeviceId)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ = host.control.reconciler.checkAll()
      _ <- pumpUntil(host, transfers.startedTransfersIn.contains(shipped))
    yield
      before should be(Map(shipped -> Phase.Arrived("another-transfer")))
      transfers.startedTransfersIn should contain(shipped)
      phases(host, net.secondDeviceId) should be(Map())
      consistent(host)
