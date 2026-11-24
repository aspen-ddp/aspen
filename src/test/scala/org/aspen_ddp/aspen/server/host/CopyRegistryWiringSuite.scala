package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.{Host, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{RebuildOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.{BackendOpener, DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

/** A rebuild that finishes at once, its copy placed in rebuilt/ as RebuildingStore.finish places it: a store config
 *  and its episode's phase-param.rebuilt */
private class PlacingRebuildFactory(writeConfig: (os.Path, StoreId) => os.Path)(using ec: ExecutionContext)
    extends StoreRebuildFactory:
  def createRebuild(client: AspenClient, storeId: StoreId, storageDeviceId: StorageDeviceId, devicePath: os.Path,
                    backends: BackendOpener, io: org.aspen_ddp.aspen.server.host.DeviceIo, episode: Option[UUID],
                    existingCopy: Option[(Phase, os.Path)]): StoreRebuild =
    val id = storeId
    // Placed off the control thread, as finish's move is, and completed right after it with the copy's id
    val placed = Future:
      writeConfig(devicePath, id)
      CopyFixtures.copyIdOf(
        CopyFixtures.markCopy(devicePath, id, Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode))))
    new StoreRebuild:
      val storeId: StoreId = id
      def complete: Future[UUID] = placed
      def cancel(): Unit = ()

/** A device CRL whose deleteTransactions waits for the test once `hold` is set (an extraction's final deletion held
 *  up by a slow CRL) */
private class HeldDeleteCrl extends org.aspen_ddp.aspen.TestNetwork.TestCRL:
  @volatile var hold: Option[scala.concurrent.Promise[Unit]] = None

  override def deleteTransactions(storeId: StoreId,
                                  txids: List[org.aspen_ddp.aspen.common.transaction.TransactionId]): Future[Unit] =
    hold match
      case Some(p) => p.future.flatMap(_ => super.deleteTransactions(storeId, txids))(using ExecutionContext.parasitic)
      case None => super.deleteTransactions(storeId, txids)

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
    CopyFixtures.placeCopy(dev, sid(1), Phase.Lost)
    CopyFixtures.placeCopy(dev, sid(2), Phase.Outbound("t-out"))
    CopyFixtures.placeCopy(dev, sid(3), Phase.Arrived("t-in"))
    CopyFixtures.placeCopy(dev, sid(4), Phase.Rebuilt("ep"))
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
    host.testingOnlyVerifyCopyRegistry() should be(Nil)

  // A copy in live/ that cannot be read is Live by its location, and its load's
  // config read waits (StoreLoader.awaitUnreadRetry)
  test("an unreadable copy registers Live and stays silent"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val dir = writeStoreConfig(dev, sid(0))
    os.perms.set(dir, "rw-------")
    try
      val host = newHost(hostRoot, verifyCopyRegistry = true)
      // The drain's check skips what cannot be read: it neither throws nor reports
      host.testingOnlyHandleEvents()
      phases(host, deviceA) should be(Map(sid(0) -> Phase.Live))
      host.control.router.silentStores should contain(sid(0))
      host.testingOnlyVerifyCopyRegistry() should be(Nil)
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())
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
    CopyFixtures.placeCopy(dev, sid(1), Phase.Arrived("t-in"))
    CopyFixtures.placeCopy(dev, sid(2), Phase.Rebuilt("kept"))
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
      // The close's end checks the device; that check, and any after it, is held: the tree lists none of the
      // placed stores, and a reconcile would retire them
      _ = recorded(host).armLookup(source)
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
      host.testingOnlyVerifyCopyRegistry() should be(Nil)
      restarted.testingOnlyVerifyCopyRegistry() should be(Nil)

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
      else
        // A retire's or discard's lane jobs, which the rebuild waits on, run without the drain's disk check
        runCopyJobs(host)
      for
        _ <- yieldUntil(condition, 20)
        _ <- waitForTransactionsToComplete()
        _ <- pumpUntil(host, condition, rounds - 1, drain)
      yield ()

  /** As pumpUntil, never draining `host`: a rebuild's copy is placed off the control thread before its completion's
   *  callback registers it, and a drain check in between would see a directory with no record */
  private def awaitRebuild(host: Host, deviceId: StorageDeviceId, storeId: StoreId): Future[Unit] =
    // The flip's outcome, then its steps (an Adopt's move to Live is a lane job) through to the slot's release
    pumpUntil(host, recorded(host).rebuildOutcomes.contains((deviceId, storeId)) &&
      !host.control.rebuilds.hasRunningRebuild(storeId), drain = false)

  /** The registry agrees with the disk, no update threw, and no load had to learn a copy */
  private def consistent(host: Host): org.scalatest.Assertion =
    host.testingOnlyHandleEvents()
    host.testingOnlyVerifyCopyRegistry() should be(Nil)
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())
    host.control.copyRegistry.learned should be(Vector())

  private def ask(host: Host): Future[Try[Unit]] =
    given ExecutionContext = executionContext
    host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, source, shipped,
      HLCTimestamp.now, UUID.randomUUID())).transform(Success(_))

  test("attach registers a child whose stat fails, and never throws"):
    val hostRoot = newHostDir()
    device(hostRoot, deviceA)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    // A copy name in live/ linking into a directory without search permission: its stat fails (EACCES)
    val other = hostRoot / os.up / "other-device"
    val closed = hostRoot / os.up / "closed"
    os.makeDir.all(closed / "inside")
    os.makeDir.all(other / CopyLayout.LiveDirectory)
    val child = CopyFixtures.freshCopyPath(other, sid(0))
    os.symlink(child, closed / "inside")
    os.perms.set(closed, "rw-------")
    try
      val sds = new LocalStorageDeviceState(deviceB, other, other / StorageDeviceConfig.configFilename,
        new org.aspen_ddp.aspen.TestNetwork.TestCRL, host.control.deviceIos.forPath(other))
      os.makeDir.all(other / "not-a-store")
      noException should be thrownBy host.control.copyRegistry.attach(sds, CopyLayoutDisk.list(other))
      phases(host, deviceB) should be(Map(sid(0) -> Phase.Live))
      host.control.copyRegistry.takeRegistryErrors() should be(Vector())
    finally os.perms.set(closed, "rwx------")

  // A copy placed after its device's load whose first stat fails is learned, Live by its live/ path, by its load retry
  // (HostCopies.awaitingRetry); the load that reads it cleanly finds the record as it is (readAtLoad)
  test("a copy learned by a load retry that later reads cleanly is loaded under its record"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val dir = CopyFixtures.freshCopyPath(dev, sid(0))
    org.aspen_ddp.aspen.server.store.backend.DefaultBackendOpener.createStore(
      org.aspen_ddp.aspen.server.StoreConfig(sid(0), org.aspen_ddp.aspen.server.StoreConfig.RocksDB(None), None,
        org.aspen_ddp.aspen.common.ida.Replication(3, 2)),
      dir, dev / org.aspen_ddp.aspen.server.store.backend.StoreCreation.CreatingDirectory)
    os.perms.set(dir, "rw-------")
    val waiting =
      try
        // The fixture's tryLoadStore only records, so the loader's own is called, as a device scan's retry calls it
        // No drain until the learn is taken: a strict drain fails on any copy a load had to learn
        host.control.loader.tryLoadStore(local, dir)
        runLoadReads(host) // its config read, on the device's lane
        host.control.loader.awaitingRetryWaitScans(sid(0))
      finally os.perms.set(dir, "rwx------")
    val learnedAtRetry = phases(host, deviceA)
    host.control.copyRegistry.takeLearned().size should be(1)
    host.control.loader.tryLoadStore(local, dir)
    waiting should be(defined)
    learnedAtRetry should be(Map(sid(0) -> Phase.Live))
    phases(host, deviceA) should be(Map(sid(0) -> Phase.Live))
    consistent(host)

  test("a copy placed after its device's load is learned when a load offers it"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    // A live/ copy, the only kind a load learns at its path (a copy in any other phase is learned at its device's
    // attach or rescan): a real store, whose open succeeds
    val dir = CopyFixtures.freshCopyPath(dev, sid(0))
    org.aspen_ddp.aspen.server.store.backend.DefaultBackendOpener.createStore(
      org.aspen_ddp.aspen.server.StoreConfig(sid(0), org.aspen_ddp.aspen.server.StoreConfig.RocksDB(None), None,
        org.aspen_ddp.aspen.common.ida.Replication(3, 2)),
      dir, dev / org.aspen_ddp.aspen.server.store.backend.StoreCreation.CreatingDirectory)
    host.testingOnlyVerifyCopyRegistry().size should be(1)
    host.control.loader.tryLoadStore(local, dir)
    runLoadReads(host) // its config read, on the device's lane
    phases(host, deviceA) should be(Map(sid(0) -> Phase.Live))
    host.control.copyRegistry.takeLearned().size should be(1)
    host.testingOnlyVerifyCopyRegistry() should be(Nil)
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  atest("a store served from an unloaded device has an untracked record, Idle once the router removes it"):
    given ExecutionContext = executionContext
    val smgr = net.smgr
    val storeId = net.stores.head.storeId
    def current: Option[CopyRecord] =
      smgr.control.copyRegistry.device(net.storageDeviceId).flatMap(_.copies.get(storeId))
    val loaded = current
    for
      _ <- smgr.closeStore(storeId).transform(Success(_))
      _ = handleEvents()
    yield
      loaded.map(r => (r.phase, r.activity)) should be(Some((Phase.Live, Activity.Served)))
      // Kept, as a closed copy's record is kept on a loaded device: the store stays silent, as the router's mark says
      current.map(r => (r.phase, r.activity)) should be(Some((Phase.Live, Activity.Idle)))
      smgr.control.copyRegistry.silence(storeId) should be(Silence.Silent(Set(SilenceReason.CopyHere)))
      smgr.testingOnlyVerifyCopyRegistry() should be(Nil)

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
      _ <- yieldUntil { runUntilLoadQueued(host); lockstep.queuedItems.exists(isLoad) }
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
        ("AdoptOffline", StorageDeviceState.DeviceStatus.ShuttingDown, Phase.Parked))
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
        _ <- pumpUntil(host, !host.control.copyRegistry.rebuilding(HostCopies.RebuildReads).contains(sid(0)), drain = false)
        outcome = recorded(host).rebuildOutcomes((deviceA, sid(0)))
        // The device's own records: the check after an AdoptOffline flip finds the device shutting down and unloads it
        after = local.copies.copies.view.mapValues(_.phase).toMap
      yield
        outcome.toString should be(variant)
        after should be(Map(sid(0) -> expected))
        CopyFixtures.phaseOf(local.devicePath, sid(0)) should be(Some(expected))
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
      // The discard is a lane job its flip's outcome queues
      _ = runCopyJobs(host)
    yield
      recorded(host).rebuildOutcomes((source, sid(7))) should be(RebuildOutcome.Discard)
      phases(host, source) should be(Map())
      CopyFixtures.copyPath(local.devicePath, sid(7)) should be(None)
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

  atest("moveToLost moves a copy to Lost, and a retire takes it to pending-delete"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val dir = writeStoreConfig(dev, sid(0))
    writeStoreConfig(dev, sid(1))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val moved = scala.concurrent.Promise[MoveResult]()
    host.control.rebuilds.moveToLostOnLane("test", local, sid(0))(moved.success(_): Unit)
    for
      result <- settled(host, moved.future)
      _ = result should be(MoveResult.Durable)
      _ = consistent(host)
      lost = phases(host, deviceA)
      retired <- settled(host, host.control.copies.retire(local, sid(1)))
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
    CopyFixtures.placeCopy(dev, sid(1), Phase.Rebuilt("ep"))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    phases(host, deviceA) should be(Map(sid(0) -> Phase.Lost, sid(1) -> Phase.Rebuilt("ep")))
    consistent(host)

  for (variant, rebuilt) <- List(("live copy", false), ("Unconfirmed rebuilt copy", true)) do
    atest(s"a transfer out marks its copy Outbound and an abandoned one is reinstated ($variant)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      val dev = device(hostRoot, source)
      CopyFixtures.placeCopy(dev, shipped, if rebuilt then Phase.Rebuilt("ep") else Phase.Live)
      val sends = new StagedSendService
      val host = newHost(hostRoot, transferService = sends, verifyCopyRegistry = true)
      host.testingOnlyHandleEvents()
      val local = loadedDevices(host)(source)
      for
        _ <- net.createSecondDevice()
        _ <- waitForTransactionsToComplete()
        _ <- client.transferStore(shipped, net.secondDeviceId)
        _ <- waitForTransactionsToComplete()
        // The close's end checks the device: held, as the tree's bootstrap stores have no copies here and a
        // reconcile would rebuild them onto this device
        _ = recorded(host).armLookup(source)
        started <- ask(host)
        _ <- pumpUntil(host, sends.isTransferringOut(shipped))
        outbound = phases(host, source)(shipped)
        _ <- client.abortStoreTransfer(shipped)
        _ <- waitForTransactionsToComplete()
        _ = sends.endTransferOut(shipped)
        _ = host.control.transfersOut.reinstateAbandonedOut(local, shipped)
        // The move to Live is a lane job
        _ <- pumpUntil(host, phases(host, source).get(shipped).contains(Phase.Live))
      yield
        started should be(Success(()))
        outbound should be(a[Phase.Outbound])
        // Reinstated to Live either way: an Unconfirmed copy's episode is inert in outbound/, and the Active entry says
        // its flip committed (P8)
        phases(host, source) should be(Map(shipped -> Phase.Live))
        consistent(host)

  // A shipped rebuilt copy carries its source's phase-param.rebuilt, inert here
  for (variant, extra) <- List(("arrived copy", Nil), ("shipped rebuilt copy", CopyLayout.paramFile(Phase.Rebuilt("")).toList))
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
        _ = checkAllNow(host)
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
    CopyFixtures.placeCopy(dev, shipped, Phase.Arrived("another-transfer"))
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val before = phases(host, net.secondDeviceId)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(host, transfers.startedTransfersIn.contains(shipped))
    yield
      before should be(Map(shipped -> Phase.Arrived("another-transfer")))
      transfers.startedTransfersIn should contain(shipped)
      phases(host, net.secondDeviceId) should be(Map())
      consistent(host)

  // Task 8 M2: crlFailed cancels no transfer in, and the unload's detach forgets the receive attempt. Bytes that land
  // after that find the attempt not current; the device's StartStoreTransfer must stop retransmitting, since no
  // abandon will find the attempt to stop it
  atest("a receive whose attempt its device's unload forgot stops retransmitting when its bytes land"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    device(hostRoot, net.secondDeviceId)
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(host, transfers.startedTransfersIn.contains(shipped))
      retransmitting = host.control.transfers.pendingStartTransfers.contains(shipped)
      _ = host.control.shutdown.crlFailed(loadedDevices(host)(net.secondDeviceId), new Exception("injected CRL failure"))
      _ <- pumpUntil(host, !loadedDevices(host).contains(net.secondDeviceId))
      _ = transfers.finishTransferIn(shipped)
      _ = host.testingOnlyHandleEvents()
    yield
      retransmitting should be(true)
      host.control.transfers.pendingStartTransfers.contains(shipped) should be(false)

  // ---- Task 5: activity and work (shadow), and derived silence ----

  /** What the registry holds for one store on one device after a change: its record's phase and activity, if any,
   *  and its work */
  private final case class Sample(phase: Option[Phase], activity: Option[Activity], work: Option[Work])

  /** Records a Sample of `storeId` on each change the registry of `host` makes to it */
  private def watch(host: Host, storeId: StoreId): mutable.ArrayBuffer[Sample] =
    val seen = mutable.ArrayBuffer[Sample]()
    host.control.copyRegistry.observer = (copies, s) =>
      if s == storeId then
        val r = copies.copies.get(s)
        seen += Sample(r.map(_.phase), r.map(_.activity), copies.work.get(s))
    seen

  /** `xs` with each run of equal neighbours taken once */
  private def runs[T](xs: Iterable[T]): List[T] =
    xs.foldLeft(List[T]())((acc, x) => if acc.headOption.contains(x) then acc else x :: acc).reverse

  private def record(host: Host, deviceId: StorageDeviceId, storeId: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.device(deviceId).flatMap(_.copies.get(storeId))

  /** A complete RocksDB store at its path on `dev`, as a create leaves it */
  private def realStore(dev: os.Path, storeId: StoreId): os.Path =
    val storePath = CopyFixtures.freshCopyPath(dev, storeId)
    DefaultBackendOpener.createStore(StoreConfig(storeId, StoreConfig.RocksDB(None), None, Replication(3, 2)),
      storePath, dev / StoreCreation.CreatingDirectory)
    storePath

  /** Closes `storeId` as Host.closeStore does, draining until it is closed */
  private def close(host: Host, storeId: StoreId): Future[Unit] =
    given ExecutionContext = executionContext
    val closed = host.closeStore(storeId)
    pumpUntil(host, closed.isCompleted).flatMap(_ => closed)

  atest("a load moves Idle -> Opening -> Served"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val storePath = realStore(dev, sid(0))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val before = record(host, deviceA, sid(0)).map(_.activity)
    val seen = watch(host, sid(0))
    // RocksDB opens inline: its LoadStore is queued on return
    host.control.loader.tryLoadStore(local, storePath)
    val opening = record(host, deviceA, sid(0)).map(_.activity)
    val silentWhileOpening = host.control.copyRegistry.silence(sid(0))
    for
      _ <- pumpUntil(host, host.control.router.loaded.contains(sid(0)))
      served = record(host, deviceA, sid(0)).map(_.activity)
      repairable = host.control.copyRegistry.repairable(sid(0))
      _ = consistent(host)
      _ <- close(host, sid(0))
    yield
      before should be(Some(Activity.Idle))
      opening should be(Some(Activity.Opening(1)))
      silentWhileOpening should be(Silence.Silent(Set(SilenceReason.Opening)))
      served should be(Some(Activity.Served))
      repairable should be(true)
      runs(seen.map(_.activity)) should be(List(Some(Activity.Opening(1)), Some(Activity.Served),
        Some(Activity.Closing(ClosePurpose.Shutdown)), Some(Activity.Idle)))
      host.control.copyRegistry.silence(sid(0)) should be(Silence.Silent(Set(SilenceReason.CopyHere)))
      consistent(host)

  atest("a failed open leaves AwaitingRetry"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val storePath = realStore(dev, sid(0))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    // Opened elsewhere first: the load's own open fails on the backend's lock, its data intact
    val held = scala.concurrent.Await.result(new DefaultBackendOpener(executionContext).open(
      StoreConfig.loadStoreConfig(storePath / StoreConfig.configFilename), storePath, _ => (),
      org.aspen_ddp.aspen.server.TestDeviceIo.of(dev)), scala.concurrent.duration.Duration(10, "seconds"))
    val seen = watch(host, sid(0))
    host.control.loader.tryLoadStore(local, storePath)
    host.testingOnlyHandleEvents()
    val waiting = record(host, deviceA, sid(0)).map(_.activity)
    val waitScans = host.control.loader.awaitingRetryWaitScans(sid(0))
    for
      _ <- held.close()
      // The retry's load (retryFailedLoads offers it to the fixture's recording tryLoadStore), which now serves the
      // store
      _ = host.control.loader.tryLoadStore(local, storePath)
      _ <- pumpUntil(host, host.control.router.loaded.contains(sid(0)))
      served = record(host, deviceA, sid(0)).map(_.activity)
      _ <- close(host, sid(0))
    yield
      waitScans should be(Some(1))
      waiting.get should be(a[Activity.AwaitingRetry])
      runs(seen.map(_.activity.map(_.toString.takeWhile(_ != '(')))) should be(List(
        Some("Opening"), Some("AwaitingRetry"), Some("Opening"), Some("Served"), Some("Closing"), Some("Idle")))
      served should be(Some(Activity.Served))
      consistent(host)

  atest("a transfer out runs Closing(TransferOut) under Work.TransferOut and ends Outbound + Idle"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, source)
    val storePath = realStore(dev, shipped)
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    host.control.loader.tryLoadStore(local, storePath)
    for
      _ <- pumpUntil(host, host.control.router.loaded.contains(shipped))
      served = record(host, source, shipped).map(_.activity)
      seen = watch(host, shipped)
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      started <- ask(host)
      _ <- pumpUntil(host, sends.isTransferringOut(shipped))
      after = record(host, source, shipped)
      work = host.control.copyRegistry.device(source).get.work.get(shipped)
    yield
      started should be(Success(()))
      served should be(Some(Activity.Served))
      seen.exists(s => s.activity.contains(Activity.Closing(ClosePurpose.TransferOut)) &&
                       s.work.exists(_.isInstanceOf[Work.TransferOut])) should be(true)
      // The work starts before the close and ends after the move to outbound/
      runs(seen.map(_.work.map(_.toString.takeWhile(_ != '(')))) should be(List(Some("TransferOut"), None))
      // The move to outbound/ is a lane job, its copy DiskJob(Move) while it runs
      runs(seen.map(_.activity)) should be(List(Some(Activity.Served), Some(Activity.Closing(ClosePurpose.TransferOut)),
        Some(Activity.Idle), Some(Activity.DiskJob(JobKind.Move, Activity.Idle)), Some(Activity.Idle)))
      after.map(r => (r.phase, r.activity)).get._1 should be(a[Phase.Outbound])
      after.map(_.activity) should be(Some(Activity.Idle))
      work should be(None)
      consistent(host)

  atest("a rebuild holds Work.Rebuild from its first reconcile to its flip"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    device(hostRoot, deviceA)
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuildFactory(writeStoreConfig)(using executionContext),
                       verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val episode = UUID.randomUUID()
    val seen = watch(host, sid(0))
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(sid(0), episode))
      _ <- waitForTransactionsToComplete()
      // What the reconcile's Rebuilding pass calls
      _ = host.control.rebuilds.startRebuild(local, sid(0), Some(episode), acceptLoss = false)
      queued = host.control.copyRegistry.silence(sid(0))
      _ <- awaitRebuild(host, deviceA, sid(0))
      _ <- pumpUntil(host, !host.control.copyRegistry.rebuilding(HostCopies.RebuildReads).contains(sid(0)), drain = false)
    yield
      recorded(host).rebuildOutcomes((deviceA, sid(0))) should be(RebuildOutcome.Adopt)
      queued should be(Silence.Silent(Set(SilenceReason.Rebuilding)))
      runs(seen.map(_.work)) should be(List(Some(Work.Rebuild(Some(episode), RebuildStage.Queued)),
        Some(Work.Rebuild(Some(episode), RebuildStage.Running)), None))
      // Held through the finish and the flip's move Live, and ended by the flip's settling
      seen.filter(_.work.isDefined).last.phase should be(Some(Phase.Live))
      seen.last should be(Sample(Some(Phase.Live), Some(Activity.Idle), None))
      consistent(host)

  atest("a reload over a served store stays Served"):
    given ExecutionContext = executionContext
    val smgr = net.smgr
    val backend = net.stores.head
    val storeId = backend.storeId
    val before = record(smgr, net.storageDeviceId, storeId).map(r => (r.phase, r.activity))
    val seen = watch(smgr, storeId)
    val reloaded = net.restartStore(backend)
    for
      _ <- pumpUntil(smgr, reloaded.isCompleted)
      _ <- reloaded
    yield
      before should be(Some((Phase.Live, Activity.Served)))
      seen.map(_.activity).toSet should be(Set(Some(Activity.Served)))
      seen should not be empty
      record(smgr, net.storageDeviceId, storeId).map(r => (r.phase, r.activity)) should be(
        Some((Phase.Live, Activity.Served)))
      smgr.control.copyRegistry.repairable(storeId) should be(true)
      smgr.isRepairable(storeId) should be(true)

  // D2 of the old marks (spec, Stage 1 audit findings, Step 3 addenda): a check of a device not loaded here marked every
  // store its entry lists offline, by store id, one served from another device of this host included. Served
  // dominates the derived silence, so the store stays answered and repairable
  atest("a check of an unloaded device that lists a store served here leaves it served and repairable"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val storePath = realStore(dev, sid(0))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    host.control.loader.tryLoadStore(loadedDevices(host)(deviceA), storePath)
    for
      _ <- pumpUntil(host, host.control.router.loaded.contains(sid(0)))
      armed = recorded(host).armLookup(deviceB)
      _ = host.control.reconciler.checkStorageDevice(deviceB)
      _ = armed.success(deviceState(deviceB,
            Map(sid(0) -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- pumpUntil(host, host.control.copyRegistry.unloadedEntries.contains(deviceB))
      _ = host.testingOnlyHandleEvents()
      served = host.control.router.loaded.contains(sid(0))
      offline = host.control.router.isOffline(sid(0))
      viewRepairable = host.control.currentView.repairable.contains(sid(0))
      derived = host.control.copyRegistry.silence(sid(0))
      registryRepairable = host.control.copyRegistry.repairable(sid(0))
      _ <- close(host, sid(0))
    yield
      served should be(true)
      offline should be(false)
      viewRepairable should be(true)
      derived should be(Silence.Served)
      registryRepairable should be(true)

  // R12: an open that starts while the store has TransferOut work on its device is refused (Task 7): the copy stays
  // Idle, the close's end finds the store still closed, and the transfer starts
  atest("an open during a transfer out's close is refused, and the transfer starts"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, source)
    val storePath = realStore(dev, shipped)
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      gate = recorded(host).holdTransferOutCloses()
      request = ask(host)
      _ <- awaitLoudly("the transfer out's close to begin")(hasTransferOutWork(host, shipped))
      work = host.control.copyRegistry.device(source).get.work.get(shipped)
      _ = host.control.loader.tryLoadStore(local, storePath)
      _ = host.testingOnlyHandleEvents()
      refused = record(host, source, shipped).map(_.activity)
      loaded = host.control.router.loaded.contains(shipped)
      repairableDuringClose = host.isRepairable(shipped)
      _ = gate.success(())
      started <- request
      _ = host.testingOnlyHandleEvents()
      after = host.control.copyRegistry.device(source).get.work.get(shipped)
    yield
      work.get should be(a[Work.TransferOut])
      refused.get should be(a[Activity.AwaitingRetry])
      loaded should be(false)
      repairableDuringClose should be(false)
      started should be(Success(()))
      sends.transfersOut.map(_._1) should be(Vector(shipped))
      after should be(None)
      consistent(host)

  // A create that fails leaves nothing at its path and ends its Create work, but its entry stays Initializing and the
  // next check creates it again: the store stays silent, by its entry, as the router's offline mark keeps it
  atest("a failed create leaves its store silent while its entry is Initializing"):
    given ExecutionContext = executionContext
    val created = StoreId(PoolId.BootstrapPoolId, 2)
    val hostRoot = newHostDir()
    val dev = device(hostRoot, source)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    // The create's scratch area cannot be made: the create fails, leaving nothing at the store's path
    os.write.over(dev / StoreCreation.CreatingDirectory, "not a directory")
    // As the reconcile that starts the create reads it
    local.reconciledEntries = Some(Map(created ->
      StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Initializing, None)))
    host.control.rebuilds.createNewStore(local, created)
    for
      // Not drained before its open: a rebuild's copy and a create's are placed off the control thread
      _ <- pumpUntil(host, !host.control.rebuilds.isCreating(created), drain = false)
      _ = host.testingOnlyHandleEvents()
    yield
      host.control.router.isOffline(created) should be(true)
      CopyFixtures.copyPath(dev, created) should be(None)
      host.control.copyRegistry.silence(created) should be(Silence.Silent(Set(SilenceReason.Opening)))
      consistent(host)

  // R12, made real (Task 7): an open of a store with TransferOut work on its device is refused. The registry throws on
  // the move to Opening, and the open path asks first, so it refuses at debug and starts no open: the copy stays Idle,
  // the extraction's deletion finds the store still closed, and the transfer ships
  atest("an open started during a transfer-out extraction throws, and the extraction ships"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = hostRoot / os.up / "injected-device"
    os.makeDir.all(deviceDir)
    val storePath = realStore(deviceDir, shipped)
    val crl = new HeldDeleteCrl
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends, verifyCopyRegistry = true)
    val local = new LocalStorageDeviceState(source, deviceDir, deviceDir / StorageDeviceConfig.configFilename, crl, host.control.deviceIos.forPath(deviceDir))
    injectLoadedDevice(host, local)
    host.control.loader.tryLoadStore(local, storePath)
    val seen = watch(host, shipped)
    for
      _ <- pumpUntil(host, host.control.router.loaded.contains(shipped))
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      held = scala.concurrent.Promise[Unit]()
      _ = crl.hold = Some(held)
      request = ask(host)
      _ <- pumpUntil(host, !host.control.router.loaded.contains(shipped) &&
                           !seen.lastOption.exists(_.activity.exists(_.isInstanceOf[Activity.Closing])))
      copies = host.control.copyRegistry.device(source).get
      work = copies.work.get(shipped)
      // The registry refuses the move itself
      direct = Try(copies.setActivity(shipped, Activity.Opening(99)))
      // An open path asks first: refused, at debug, before any backend opens
      _ = host.control.loader.tryLoadStore(local, storePath)
      _ = host.testingOnlyHandleEvents()
      refusedOpen = record(host, source, shipped).map(_.activity)
      openingAfter = host.control.loader.isOpening(shipped)
      loadedAfter = host.control.router.loaded.contains(shipped)
      _ = crl.hold = None
      _ = held.success(())
      started <- request
      _ <- awaitLoudly("the transfer out of the extracted store to begin")(sends.isTransferringOut(shipped))
      _ = host.testingOnlyHandleEvents()
      errors = host.control.copyRegistry.takeRegistryErrors()
    yield
      work.get should be(a[Work.TransferOut])
      direct.failed.get should be(an[IllegalStateException])
      refusedOpen.get should be(a[Activity.AwaitingRetry])
      openingAfter should be(false)
      loadedAfter should be(false)
      started should be(Success(()))
      sends.isTransferringOut(shipped) should be(true)
      record(host, source, shipped).map(_.phase).get should be(a[Phase.Outbound])
      errors should be(Vector())

  // ---- Fix round 1 of Task 7: claims refuse and the caller backs off (I1, R23); facts apply (R22) ----

  // I1: the Opening claim is refused for a reason other than TransferOut work (here a record whose phase is not Live,
  // so Idle -> Opening is illegal). The open backs off: no backend opens, and the copy awaits a load retry (R23)
  atest("an Opening claim refused for a non-TransferOut reason opens no backend and leaves an AwaitingRetry"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val storePath = realStore(dev, sid(0))
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val copies = host.control.copyRegistry.copiesOf(local).get
    // The record disagrees with the disk (Lost, the disk Live): its Opening is illegal
    copies.setPhase(sid(0), Phase.Lost)
    val cfg = StoreConfig.loadStoreConfig(storePath / StoreConfig.configFilename)
    var served = false
    host.control.loader.startOpen(local, cfg, storePath, create = false, None, Some(() => served = true))
    val errors = host.control.copyRegistry.takeRegistryErrors()
    val activity = copies.copies.get(sid(0)).map(_.activity)
    // The record put back to what the disk reads, for the drain's disk check (Lost -> Live is no legal move)
    copies.applyPhase(sid(0), Phase.Live, Phase.Live)
    host.testingOnlyHandleEvents()
    Future:
      errors.size should be(1)
      errors.head should include("startOpen")
      recorded(host).loadStoreRequests should be(empty)
      host.control.loader.isOpening(sid(0)) should be(false)
      activity.get should be(a[Activity.AwaitingRetry])
      host.control.loader.awaitingRetryHasCallback(sid(0)) should be(Some(true))
      served should be(false)

  // R23: an open refused while the store has TransferOut work records a retry carrying its onServed. Once the
  // transfer's close fails (the work ends, nothing shipped), the device scan's retry serves the store and runs it
  atest("an open refused during TransferOut is retried and served once the transfer ends, with its onServed run"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, source)
    val storePath = realStore(dev, shipped)
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    var served = false
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      gate = recorded(host).holdTransferOutCloses()
      request = ask(host)
      _ <- awaitLoudly("the transfer out's close to begin")(hasTransferOutWork(host, shipped))
      _ = host.control.loader.tryLoadStore(local, storePath, Some(() => served = true))
      _ = host.testingOnlyHandleEvents()
      refused = record(host, source, shipped).map(_.activity)
      _ = gate.failure(new java.io.IOException("Simulated extraction failure"))
      _ <- request
      _ = host.testingOnlyHandleEvents()
      workAfter = hasTransferOutWork(host, shipped)
      attemptsBefore = recorded(host).storeLoadAttempts.size
      _ = host.control.loader.retryFailedLoads()
      retried = recorded(host).storeLoadAttempts.drop(attemptsBefore).toList
      // What the recording tryLoadStore seam stands in for
      _ = host.control.loader.tryLoadStore(local, storePath)
      _ <- awaitLoudly("the retried load to serve the store and run its onServed")(
             { host.testingOnlyHandleEvents(); host.control.router.loaded.contains(shipped) && served })
      _ <- close(host, shipped)
    yield
      refused.get should be(a[Activity.AwaitingRetry])
      workAfter should be(false)
      retried should be(List((source, storePath)))
      served should be(true)
      sends.transfersOut should be(empty)
      consistent(host)

  // R22: a fact (here a router removal of a copy the registry holds Idle, and a phase the registry holds against what
  // the disk reads) is applied whatever the table says; a phase is set to what the disk reads. A strict (test) host
  // records the violation for its drain; production only logs it, keeping nothing
  for strictMode <- List(true, false) do
    test(s"an illegal fact is applied, and recorded only when strict (strict = $strictMode)"):
      val hostRoot = newHostDir()
      val dev = device(hostRoot, deviceA)
      writeStoreConfig(dev, sid(0))
      writeStoreConfig(dev, sid(1))
      val host = newHost(hostRoot)
      host.testingOnlyHandleEvents()
      val registry = host.control.copyRegistry
      registry.strict = strictMode
      val local = loadedDevices(host)(deviceA)
      val copies = registry.copiesOf(local).get
      // Idle -> Closing is not in the table: applied anyway
      registry.removedFromRouter(deviceA, sid(0))
      // A copy placed over a record of another (P2) is applied anyway: the placed copy is recorded
      val placedId = java.util.UUID.randomUUID()
      registry.placed("test", local, sid(1), Phase.Arrived("u"), placedId, unsynced = false)
      val errors = registry.takeRegistryErrors()
      copies.copies(sid(0)).activity should be(Activity.Closing(ClosePurpose.FailStop))
      copies.copies(sid(1)).phase should be(Phase.Arrived("u"))
      copies.copies(sid(1)).copyId should be(placedId)
      if strictMode then
        errors.size should be(2)
        errors.exists(_.contains("router remove")) should be(true)
        errors.exists(_.contains("test")) should be(true)
      else
        errors should be(Vector())

  // I-2: production retries a refused claim at every check and may learn copies; a non-strict registry only logs
  // them, so nothing grows without bound
  test("in production mode, repeated refused claims and learned copies keep nothing"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    writeStoreConfig(dev, sid(0))
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val registry = host.control.copyRegistry
    registry.strict = false
    val local = loadedDevices(host)(deviceA)
    val path = CopyFixtures.copyPath(dev, sid(0)).get
    registry.opening(local, sid(0), 1L, create = false, path) should be(true)
    (1 to 100).foreach: i =>
      registry.opening(local, sid(0), 1L + i, create = false, path) should be(false)
      registry.registered("test", local, sid(0), Phase.Live, java.util.UUID.randomUUID()) should be(false)
    // A copy put at its path after the load, learned by its load
    val dir = writeStoreConfig(dev, sid(1))
    registry.readAtLoad(local, dir, sid(1), Phase.Live)
    registry.copiesOf(local).get.copies.contains(sid(1)) should be(true)
    registry.takeRegistryErrors() should be(Vector())
    registry.takeLearned() should be(Vector())
    registry.takeSilenceErrors() should be(Vector())

  // ---- The states of D1, D4, D5 and D6 of the old marks (spec, Stage 1 audit findings, Step 3 addenda), derived ----

  /** A second loaded device directory, dev1 */
  private def secondDevice(hostRoot: os.Path, deviceId: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, "dev1", deviceId)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  test("an Initializing entry alone, its create not started, is silent (D4's state)"):
    val hostRoot = newHostDir()
    device(hostRoot, deviceA)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    // Read Initializing by a reconcile, its create not started (the reconcile's read before createNewStore, or a
    // device shutting down): no Create work, no copy
    loadedDevices(host)(deviceA).reconciledEntries = Some(Map(sid(5) ->
      StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Initializing, None)))
    host.testingOnlyHandleEvents()
    host.control.copyRegistry.silence(sid(5)) should be(Silence.Silent(Set(SilenceReason.Opening)))
    host.control.router.answersUnknown(sid(5)) should be(false)

  atest("a copy retired into pending-delete leaves its store unknown at once (D5's state)"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    writeStoreConfig(dev, sid(1))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val before = host.control.router.answersUnknown(sid(1))
    for
      retired <- settled(host, host.control.copies.retire(local, sid(1)))
    yield
      before should be(false)
      retired should be(true)
      host.control.copyRegistry.silence(sid(1)) should be(Silence.Unknown)
      host.control.router.answersUnknown(sid(1)) should be(true)

  test("a copy on one device and a transfer in on another keep the store silent (D1's state)"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    secondDevice(hostRoot, deviceB)
    writeStoreConfig(dev, sid(2))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    host.control.copyRegistry.work("test", deviceB, sid(2), _.isInstanceOf[Work.TransferIn],
      Some(Work.TransferIn(None, TransferInStage.Receiving)))
    host.testingOnlyHandleEvents()
    host.control.copyRegistry.silence(sid(2)) should be(
      Silence.Silent(Set(SilenceReason.CopyHere, SilenceReason.TransferringIn)))
    host.control.router.answersUnknown(sid(2)) should be(false)

  test("a store directory without its config is silent from its device's load (D6's state)"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val other = secondDevice(hostRoot, deviceB)
    // Its config lost on device A; a whole copy on device B
    os.remove(CopyFixtures.placeCopy(dev, sid(3)) / StoreConfig.configFilename)
    writeStoreConfig(other, sid(3))
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    host.control.copyRegistry.silence(sid(3)) should be(Silence.Silent(Set(SilenceReason.CopyHere)))
    host.control.router.answersUnknown(sid(3)) should be(false)

  // R6: a move into live/ (Arrived's T9, Outbound's T7, Rebuilt's T3) whose directory sync fails after it
  // landed leaves the copy unsynced, and it is never loaded until a later device check forces the directory again,
  // which loads it
  for (name, phase) <- List(("Arrived, moved to Live", Phase.Arrived("x")), ("Outbound, moved to Live", Phase.Outbound("x")),
                            ("Rebuilt, moved to Live", Phase.Rebuilt("x"))) do
    atest(s"a failed sync after a move to live leaves the copy unsynced and unloaded, and the next check loads it ($name)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      val dev = device(hostRoot, deviceA)
      val placed = CopyFixtures.placeCopy(dev, sid(0), phase)
      // The directory whose sync fails: the one the move leaves
      val failing = placed / os.up
      val dir = CopyLayout.path(dev, sid(0), CopyFixtures.copyIdOf(placed), Phase.Live)
      val host = newHost(hostRoot, verifyCopyRegistry = true)
      var failSync = true
      var syncs = List.empty[os.Path]
      host.control.moves.syncDirectory = d =>
        syncs = d :: syncs
        if failSync && d == failing then throw new java.io.IOException("injected directory sync failure")
      host.testingOnlyHandleEvents()
      val local = loadedDevices(host)(deviceA)
      val active = deviceState(deviceA,
        Map(sid(0) -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)))
      def check(): Future[Unit] =
        val armed = recorded(host).armLookup(deviceA)
        host.control.reconciler.checkStorageDevice(deviceA)
        armed.success(active)
        awaitLoudly(s"the check of device $deviceA") {
          host.testingOnlyHandleEvents()
          !host.control.reconciler.activeDeviceChecks.contains(deviceA)
        }
      def loads: Int = recorded(host).storeLoadAttempts.count(_ == (deviceA, dir))
      val loadedAtStart = loads
      for
        _ <- check()
        removed = CopyFixtures.phaseOf(dev, sid(0)).contains(Phase.Live)
        unsynced = record(host, deviceA, sid(0)).map(_.unsynced)
        loadsAfterFailure = loads
        _ = failSync = false
        _ <- check()
      yield
        removed should be(true)
        unsynced should be(Some(true))
        loadsAfterFailure should be(loadedAtStart)
        syncs should contain(failing)
        record(host, deviceA, sid(0)).map(r => (r.phase, r.unsynced)) should be(Some((Phase.Live, false)))
        loads should be(loadedAtStart + 1)
        consistent(host)

  // One predicate answers "is a rebuild of this store registered here", by stage: the discard of its lost copy hands
  // its slot straight to the rebuild, with no queued gap between them
  atest("rebuilding answers the same through a discard and a run"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    writeStoreConfig(dev, sid(0))
    val host = newHost(hostRoot, storeRebuildFactory = new PlacingRebuildFactory(writeStoreConfig)(using executionContext),
                       verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val episode = UUID.randomUUID()
    val registry = host.control.copyRegistry
    // Each change: the store's work there, and what each view of `rebuilding` answers
    val seen = mutable.ArrayBuffer[(Option[RebuildStage], Boolean, Boolean, Boolean)]()
    registry.observer = (copies, s) =>
      if s == sid(0) then
        val stage = copies.work.get(s).collect { case w: Work.Rebuild => w.stage }
        seen += ((stage, registry.rebuilding().contains(s), registry.rebuilding(HostCopies.RebuildHolds).contains(s),
                  registry.rebuilding(HostCopies.RebuildReads).contains(s)))
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(sid(0), episode))
      _ <- waitForTransactionsToComplete()
      // The reconcile's entries, which the discard's handoff re-reads
      _ = local.reconciledEntries = Some(Map(sid(0) -> StorageDeviceState.StoreEntry(
            StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode))))
      _ = host.control.rebuilds.startRebuild(local, sid(0), Some(episode), acceptLoss = false)
      _ <- awaitRebuild(host, deviceA, sid(0))
    yield
      recorded(host).rebuildOutcomes((deviceA, sid(0))) should be(RebuildOutcome.Adopt)
      runs(seen.map(_._1)) should be(List(Some(RebuildStage.Queued), Some(RebuildStage.DiscardingLost),
        Some(RebuildStage.Running), None))
      seen.foreach: (stage, any, holds, reads) =>
        any should be(stage.isDefined)
        holds should be(stage.exists(HostCopies.RebuildHolds.contains))
        reads should be(stage.exists(HostCopies.RebuildReads.contains))
      consistent(host)

  // A copy whose move Live was forced again stays held for its load until a check that finds it loadable loads
  // it: a check whose entry is not Active, or whose copy other work holds, only defers it
  for variant <- List("entry not Active", "work holds the copy") do
    atest(s"a re-synced copy that the re-syncing check cannot load is loaded by a later check ($variant)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      val dev = device(hostRoot, deviceA)
      // An Outbound copy's move back to Live (T7) whose sync fails in the directory it leaves
      val placed = CopyFixtures.placeCopy(dev, sid(0), Phase.Outbound("x"))
      val dir = CopyLayout.path(dev, sid(0), CopyFixtures.copyIdOf(placed), Phase.Live)
      val host = newHost(hostRoot, verifyCopyRegistry = true)
      var failSync = true
      host.control.moves.syncDirectory = d =>
        if failSync && d == placed / os.up then throw new java.io.IOException("injected directory sync failure")
      host.testingOnlyHandleEvents()
      def entry(status: StorageDeviceState.StoreStatus) =
        deviceState(deviceA, Map(sid(0) -> StorageDeviceState.StoreEntry(status, None)))
      def check(state: StorageDeviceState): Future[Unit] =
        val armed = recorded(host).armLookup(deviceA)
        host.control.reconciler.checkStorageDevice(deviceA)
        armed.success(state)
        awaitLoudly(s"the check of device $deviceA") {
          host.testingOnlyHandleEvents()
          !host.control.reconciler.activeDeviceChecks.contains(deviceA)
        }
      def loads: Int = recorded(host).storeLoadAttempts.count(_ == (deviceA, dir))
      val held = Work.TransferIn(None, TransferInStage.RetiringEarlierTenancy)
      def hold(on: Boolean): Unit =
        host.control.copyRegistry.work("test", deviceA, sid(0), _.isInstanceOf[Work.TransferIn],
          if on then Some(held) else None): Unit
      val loadedAtStart = loads
      for
        _ <- check(entry(StorageDeviceState.StoreStatus.Active))
        unsynced = record(host, deviceA, sid(0)).map(_.unsynced)
        _ = failSync = false
        // The re-syncing check cannot load it
        _ = if variant == "work holds the copy" then hold(on = true)
        _ <- check(entry(if variant == "entry not Active" then StorageDeviceState.StoreStatus.Offline
                         else StorageDeviceState.StoreStatus.Active))
        synced = record(host, deviceA, sid(0)).map(_.unsynced)
        loadsWhileHeld = loads
        _ = if variant == "work holds the copy" then hold(on = false)
        _ <- check(entry(StorageDeviceState.StoreStatus.Active))
      yield
        unsynced should be(Some(true))
        synced should be(Some(false))
        loadsWhileHeld should be(loadedAtStart)
        loads should be(loadedAtStart + 1)
        consistent(host)

  // ---- Task 9: the disk check runs in every host test ----

  // A rebuild's finish places its copy off the control thread, ahead of its completion's handler: a drain in between
  // finds a directory with no record. Only that copy is exempt, recognised by its disk signature (the Rebuilt phase
  // of the running rebuild's episode), and only until the handler records it. Any other copy at the rebuilding
  // store's path or in rebuilt/ meanwhile is reported, as is a copy placed by hand at another store's path, until a rescan
  // registers it
  atest("a rebuild's copy placed ahead of its completion's handler is exempt from the disk check until recorded"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val factory = new HeldRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val episode = UUID.randomUUID()
    def mismatches(): List[String] = host.testingOnlyVerifyCopyRegistry()
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(sid(0), episode))
      _ <- waitForTransactionsToComplete()
      _ = host.control.rebuilds.startRebuild(local, sid(0), Some(episode), acceptLoss = false)
      _ <- pumpUntil(host, factory.created.nonEmpty)
      // Near misses, at the rebuilding store's path before finish: an unregistered Live copy, then one finished for
      // another episode. Neither is the rebuild's own, so both are reported (checked without draining)
      wrong = writeStoreConfig(dev, sid(0))
      liveCopy = mismatches()
      other = CopyFixtures.markCopy(dev, sid(0), Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(Some(UUID.randomUUID()))))
      otherEpisode = mismatches()
      _ = os.remove.all(other)
      // Where finish places the copy, before its future completes
      _ = CopyFixtures.placeCopy(dev, sid(0), Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(Some(episode))))
      placingPhase = host.control.rebuilds.finishPlacing(deviceA, sid(0))
      // The draining check passes: the copy is exempt
      _ = host.testingOnlyHandleEvents()
      exempt = mismatches()
      // A copy put at another store's path meanwhile is not exempt
      _ = writeStoreConfig(dev, sid(1))
      handPlaced = mismatches()
      rescanned = host.testingOnlyRescan(deviceA)
      _ = factory.created.head.completeFromDisk()
      _ <- awaitRebuild(host, deviceA, sid(0))
      recordedAfterFinish = host.control.rebuilds.finishPlacing(deviceA, sid(0))
    yield
      liveCopy.size should be(1)
      liveCopy.head should include(sid(0).directoryName)
      liveCopy.head should include("has no record")
      otherEpisode.size should be(1)
      otherEpisode.head should include(sid(0).directoryName)
      otherEpisode.head should include("has no record")
      placingPhase should be(Some(Phase.Rebuilt(episode.toString)))
      exempt should be(Nil)
      handPlaced.size should be(1)
      handPlaced.head should include(sid(1).directoryName)
      handPlaced.head should include("has no record")
      rescanned should be(List(sid(1)))
      recordedAfterFinish should be(None)
      recorded(host).rebuildOutcomes((deviceA, sid(0))) should be(RebuildOutcome.Adopt)
      phases(host, deviceA) should be(Map(sid(0) -> Phase.Live, sid(1) -> Phase.Live))
      consistent(host)
