package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.rebuild.RebuildDisk
import org.aspen_ddp.aspen.server.host.{CopyFixtures, JobKind, Phase}
import org.aspen_ddp.aspen.server.store.backend.BackendOpener
import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.server.host.{ControlFixture, DeviceMetadata, LocalStorageDeviceState, RebuildStage, StagedSendService, StagedTransferService, TransferInStage, Work}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.network.{StartStoreTransfer, StoreMoved, StoreOffline}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.DeviceMetadata.ShutdownOutcome
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** A StoreRebuild that runs until cancelled, then fails the way RebuildingStore does. */
private class CancellableRebuild(val storeId: StoreId) extends StoreRebuild:
  private val promise: Promise[UUID] = Promise()
  @volatile var cancelled: Boolean = false
  def complete: Future[UUID] = promise.future
  def cancel(): Unit =
    cancelled = true
    promise.tryFailure(RebuildingStore.RebuildCancelled(storeId))

private class CancellableRebuildFactory extends StoreRebuildFactory:
  val created: mutable.ListBuffer[CancellableRebuild] = mutable.ListBuffer()

  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: os.Path,
                    backends: BackendOpener, io: org.aspen_ddp.aspen.server.host.DeviceIo,
                    episode: Option[UUID],
                    existingCopy: Option[(Phase, os.Path)]): StoreRebuild = synchronized:
    val r = new CancellableRebuild(storeId)
    created += r
    r

class DeviceShutdownSuite extends IntegrationTestSuite with ControlFixture:
  import StorageDeviceState.{DeviceStatus, StoreEntry, StoreStatus}

  private def storeN(n: Int): StoreId =
    StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), n.toByte)

  private val s0 = storeN(0)
  private val s1 = storeN(1)

  private val active = StoreEntry(StoreStatus.Active, None)

  /** Alternates yields and transaction drains until `condition` holds. Silent on exhaustion, like
   *  yieldUntil: every caller asserts the end state afterwards. */
  private def pumpUntil(condition: => Boolean, rounds: Int = 50): Future[Unit] =
    given ExecutionContext = executionContext
    if condition || rounds == 0 then
      Future.unit
    else
      for
        _ <- yieldUntil(condition, 20)
        _ <- waitForTransactionsToComplete()
        _ <- pumpUntil(condition, rounds - 1)
      yield ()

  private def state(id: StorageDeviceId): Future[StorageDeviceState] = client.getStorageDeviceState(id)

  /** Pumps until `id`'s entry for `storeId` satisfies `p`, re-reading it each round. Silent on
   *  exhaustion, like pumpUntil. */
  private def pumpUntilEntry(id: StorageDeviceId, storeId: StoreId, rounds: Int = 50)
                            (p: Option[StoreEntry] => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    state(id).flatMap: st =>
      if p(st.stores.get(storeId)) || rounds == 0 then
        Future.unit
      else
        pumpUntil(false, rounds = 1).flatMap(_ => pumpUntilEntry(id, storeId, rounds - 1)(p))

  /** Lists `id` in the bootstrap host's HostState, as device creation does. net.createDevice
   *  registers the device and its set membership only. */
  private def addToBootstrapHost(id: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getHostPointer(HostId.BootstrapHostId)
        kvos <- client.read(ptr)
      yield
        val hs = HostState(kvos)
        tx.update(ptr, None, None,
          List(KeyRevision(HostState.StateKey, kvos.contents(HostState.StateKey).revision)),
          List(Insert(HostState.StateKey, hs.addStorageDevice(id).encode())))

  /** A host with deviceA registered, written to disk and loaded, carrying `stores` in the tree. The
   *  stores in `loaded` are given directories and recorded as open on the device, as tryLoadStore
   *  would have done. ControlFixture's RecordingSeams make tryLoadStore only record the path, so no
   *  backend is opened and that is done by hand. Their directories hold nothing unless `configured`, which
   *  gives each its config: a closed store is loaded again only with its config present (CopyDecision.Load). */
  private def setUp(stores: Map[StoreId, StoreEntry],
                    loaded: Set[StoreId] = Set(),
                    transfers: StagedTransferService = new StagedTransferService,
                    rebuilds: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                    configured: Boolean = false): Future[(Host, os.Path)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot, transferService = transfers, storeRebuildFactory = rebuilds)
    loaded.foreach(s => CopyFixtures.placeCopy(deviceDir, s, configured = configured))
    servedHere(host, loadedDevices(host)(deviceA), loaded.toSeq*)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = stores))
      _ <- waitForTransactionsToComplete()
    yield (host, deviceDir)

  /** Requests the shutdown, runs one check and waits for the device to be unloaded. */
  private def shutDown(host: Host): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(!loadedDevices(host).contains(deviceA))
    yield ()

  /** Neither of the things a network transfer leaves on the copy of `s`: its Outbound phase and its save file. A
   *  shutdown leaves a store's state in its device's CRL */
  private def noMoveFiles(deviceDir: os.Path, s: StoreId): Boolean =
    !outbound(deviceDir, s) && CopyFixtures.copyPath(deviceDir, s).forall(dir => !os.exists(Backend.crlSaveFile(dir)))

  /** Whether the copy of `s` is Outbound (in outbound/) */
  private def outbound(deviceDir: os.Path, s: StoreId): Boolean =
    CopyFixtures.phaseOf(deviceDir, s).exists(_.isInstanceOf[Phase.Outbound])

  /** A copy of `s` holding nothing, not even its config */
  private def bareCopy(deviceDir: os.Path, s: StoreId): os.Path =
    CopyFixtures.placeCopy(deviceDir, s, configured = false)

  /** Where a rebuild's finish places the copy of `s`: in rebuilt/, with no episode, holding nothing else */
  private def rebuiltCopy(deviceDir: os.Path, s: StoreId): os.Path =
    CopyFixtures.placeCopy(deviceDir, s, Phase.Rebuilt(""), configured = false)

  /** Whether the copy of `s` is parked (an AdoptOffline rebuild's) */
  private def parked(deviceDir: os.Path, s: StoreId): Boolean =
    CopyFixtures.phaseOf(deviceDir, s).contains(Phase.Parked)

  /** The path of the copy of `s` */
  private def copyAt(deviceDir: os.Path, s: StoreId): os.Path = CopyFixtures.copyPath(deviceDir, s).get

  atest("a ShuttingDown device has its stores closed and announced, then is Shutdown"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1))
      _ <- addToBootstrapHost(deviceA)
      _ = net.takeCapturedGossip()
      _ <- shutDown(host)
      st <- state(deviceA)
      hostState <- client.getHostState(HostId.BootstrapHostId)
    yield
      recorded(host).shutdownCloses.toSet should be(Set(s0, s1))
      recorded(host).shutdownCloses.size should be(2)
      noMoveFiles(deviceDir, s0) should be(true)
      noMoveFiles(deviceDir, s1) should be(true)
      DeviceMarkers.readShutdownState(deviceDir) should be(Some(HostId.BootstrapHostId))
      st.status should be(DeviceStatus.Shutdown)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Offline))
      net.takeCapturedGossip().collect { case g: StoreOffline => g.storeId }.toSet should be(Set(s0, s1))
      loadedDevices(host).contains(deviceA) should be(false)
      host.control.discovery.loadedDeviceDirs.values.toSet should not contain deviceA
      hostState.storageDevices should contain(deviceA)
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))

  atest("a store whose backend fails during its shutdown close does not stop the shutdown"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1))
      _ <- addToBootstrapHost(deviceA)
      _ = recorded(host).failingShutdownCloses.set(Set(s0))
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      recorded(host).shutdownCloses.toSet should be(Set(s0, s1))
      // The failed store is marked for rebuild, which is what an adopter reads; the clean one is plain
      CopyFixtures.phaseOf(deviceDir, s0).contains(Phase.Lost) should be(true)
      CopyFixtures.phaseOf(deviceDir, s1).contains(Phase.Lost) should be(false)
      noMoveFiles(deviceDir, s0) should be(true)
      noMoveFiles(deviceDir, s1) should be(true)
      st.stores(s0).status should be(StoreStatus.Offline)
      st.stores(s1).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))

  atest("shutdown cancels a running rebuild and does not restart it"):
    given ExecutionContext = executionContext
    val rebuilds = new CancellableRebuildFactory
    val rebuilding = StoreEntry(StoreStatus.Rebuilding, None)
    for
      (host, _) <- setUp(Map(s0 -> rebuilding), rebuilds = rebuilds)
      _ = checkAllNow(host)
      _ <- pumpUntil(rebuilds.created.nonEmpty)
      _ <- shutDown(host)
      // A check after the shutdown must not start the rebuild again either.
      _ = checkAllNow(host)
      _ <- pumpUntil(host.control.reconciler.activeDeviceChecks.isEmpty)
      st <- state(deviceA)
    yield
      rebuilds.created.size should be(1)
      rebuilds.created.head.cancelled should be(true)
      st.stores(s0) should be(rebuilding)
      st.status should be(DeviceStatus.Shutdown)

  atest("shutdown cancels transfers in and out of the device and leaves their entries alone"):
    given ExecutionContext = executionContext
    val transfers = new StagedTransferService
    val transferId = UUID.randomUUID()
    val inEntry = StoreEntry(StoreStatus.TransferringIn, Some(deviceB), Some(transferId))
    val outEntry = StoreEntry(StoreStatus.TransferringOut, Some(deviceB), Some(UUID.randomUUID()))
    for
      _ <- net.createDevice(deviceB)
      (host, _) <- setUp(Map(s0 -> inEntry, s1 -> outEntry), transfers = transfers)
      _ = transfers.markSending(s1)
      _ = checkAllNow(host)
      _ <- pumpUntil(transfers.startedTransfersIn.nonEmpty)
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      transfers.startedTransfersIn should be(Set(s0))
      transfers.cancelledTransfersIn.map(_._1) should be(Vector(s0))
      transfers.cancelledTransfersOut should be(Vector(s1))
      host.control.transfers.receiving.keySet should be(Set())
      st.stores should be(Map(s0 -> inEntry, s1 -> outEntry))
      st.status should be(DeviceStatus.Shutdown)

  atest("a store that becomes Active during shutdown is taken offline before Shutdown commits"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active), loaded = Set(s0))
      // Between the stores closing and the Shutdown commit, a late rebuild completion flips s1 to
      // Active and loads it.
      _ = recorded(host).beforeNextShutdownCommit: () =>
            bareCopy(deviceDir, s1)
            servedHere(host, loadedDevices(host)(deviceA), s1)
            setDeviceState(deviceA)(_.setStoreEntry(s1, StoreStatus.Active, None))
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.StoresReactivated, ShutdownOutcome.Completed))
      recorded(host).shutdownCloses.toList should be(List(s0, s1))
      noMoveFiles(deviceDir, s1) should be(true)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Offline))
      st.status should be(DeviceStatus.Shutdown)

  atest("repeated checks during a shutdown do not start a second one"):
    given ExecutionContext = executionContext
    for
      (host, _) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1))
      gate = recorded(host).holdShutdownCloses()
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).shutdownCloses.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty)
      _ = checkAllNow(host)
      _ <- pumpUntil(host.control.reconciler.activeDeviceChecks.isEmpty)
      closesWhileHeld = recorded(host).shutdownCloses.toList
      _ = gate.success(())
      _ <- pumpUntil(!loadedDevices(host).contains(deviceA))
      st <- state(deviceA)
    yield
      closesWhileHeld.size should be(1)
      recorded(host).shutdownCloses.toList.sortBy(_.poolIndex) should be(List(s0, s1))
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
      st.status should be(DeviceStatus.Shutdown)

  // Device I/O isolation (Task 15): shutdown.state is one Markers job on the device's lane, durable before Shutdown
  // commits
  atest("a shutdown's shutdown.state write waits on a held device, and Shutdown commits only once it is written"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active), loaded = Set(s0))
      _ <- addToBootstrapHost(deviceA)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = holdDevice(host, deviceDir)
      _ = checkAllNow(host)
      _ <- pumpUntil(heldJobs(host, deviceDir).contains(JobKind.Markers))
      _ <- pumpUntil(false, rounds = 2)
      heldWhile = heldJobs(host, deviceDir)
      whileHeld <- state(deviceA)
      markerWhile = os.exists(deviceDir / DeviceMarkers.ShutdownStateFile)
      loadedWhile = loadedDevices(host).contains(deviceA)
      _ = releaseDevice(host, deviceDir)
      _ <- pumpUntil(!loadedDevices(host).contains(deviceA))
      st <- state(deviceA)
    yield
      heldWhile should be(List(JobKind.Markers))
      whileHeld.status should be(DeviceStatus.ShuttingDown)
      markerWhile should be(false)
      loadedWhile should be(true)
      st.status should be(DeviceStatus.Shutdown)
      DeviceMarkers.readShutdownState(deviceDir) should be(Some(HostId.BootstrapHostId))

  atest("a shutdown's shutdown.state write waits on a held device while another device's shutdown completes"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val devA = writeDevice(hostRoot, "dev0", deviceA)
    val devB = writeDevice(hostRoot, "dev1", deviceB)
    val host = newHost(hostRoot, transferService = new StagedTransferService)
    CopyFixtures.placeCopy(devA, s0, configured = false)
    CopyFixtures.placeCopy(devB, s1, configured = false)
    servedHere(host, loadedDevices(host)(deviceA), s0)
    servedHere(host, loadedDevices(host)(deviceB), s1)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(s0 -> active)))
      _ <- net.createDevice(deviceB)
      _ <- setDeviceState(deviceB)(_.copy(stores = Map(s1 -> active)))
      _ <- addToBootstrapHost(deviceA)
      _ <- addToBootstrapHost(deviceB)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- client.shutdownStorageDevice(deviceB)
      _ <- waitForTransactionsToComplete()
      _ = holdDevice(host, devA)
      _ = checkAllNow(host)
      _ <- pumpUntil(heldJobs(host, devA).contains(JobKind.Markers) && !loadedDevices(host).contains(deviceB))
      heldWhile = heldJobs(host, devA)
      aWhile <- state(deviceA)
      bState <- state(deviceB)
      markerAWhile = os.exists(devA / DeviceMarkers.ShutdownStateFile)
      loadedAWhile = loadedDevices(host).contains(deviceA)
      _ = releaseDevice(host, devA)
      _ <- pumpUntil(!loadedDevices(host).contains(deviceA))
      aState <- state(deviceA)
    yield
      heldWhile should be(List(JobKind.Markers))
      // B's shutdown wrote its marker and committed while A's write waited
      bState.status should be(DeviceStatus.Shutdown)
      DeviceMarkers.readShutdownState(devB) should be(Some(HostId.BootstrapHostId))
      loadedDevices(host).contains(deviceB) should be(false)
      aWhile.status should be(DeviceStatus.ShuttingDown)
      markerAWhile should be(false)
      loadedAWhile should be(true)
      aState.status should be(DeviceStatus.Shutdown)
      DeviceMarkers.readShutdownState(devA) should be(Some(HostId.BootstrapHostId))

  atest("a shutdown.state write completing after its device's unload commits nothing"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active), loaded = Set(s0))
      _ <- addToBootstrapHost(deviceA)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = holdDevice(host, deviceDir)
      _ = checkAllNow(host)
      _ <- pumpUntil(heldJobs(host, deviceDir).contains(JobKind.Markers))
      _ = host.control.shutdown.unloadDevice(deviceA)
      _ = releaseDevice(host, deviceDir)
      _ <- pumpUntil(!host.control.shutdown.isShuttingDown(deviceA))
      st <- state(deviceA)
    yield
      host.control.shutdown.isShuttingDown(deviceA) should be(false)
      recorded(host).shutdownOutcomes should be(empty)
      st.status should be(DeviceStatus.ShuttingDown)

  atest("restart during shutdown cancels it"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1), configured = true)
      gate = recorded(host).holdShutdownCloses()
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).shutdownCloses.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty)
      _ <- client.restartStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = gate.success(())
      paths = List(s0, s1).map(copyAt(deviceDir, _))
      _ <- pumpUntil(recorded(host).shutdownOutcomes.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty &&
                     paths.forall(recorded(host).storeLoadAttempts.map(_._2).contains))
      st <- state(deviceA)
    yield
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Cancelled))
      st.status should be(DeviceStatus.Operational)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Active))
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      loadedDevices(host).contains(deviceA) should be(true)
      // The reconcile brought both stores back: no copy left live/ to bring back
      List(s0, s1).forall(noMoveFiles(deviceDir, _)) should be(true)
      recorded(host).storeLoadAttempts.map(_._2).toSet should contain allOf(copyAt(deviceDir, s0),
                                                                  copyAt(deviceDir, s1))

  // A closed copy that other work holds when the cancelled shutdown's reconcile runs is left to that work (I3), and a
  // later reconcile loads it once the work has ended (CopyDecision.Load). The work is an earlier tenancy's retire,
  // which no decision of an Active entry ends (a queued rebuild's would be ended by the reconcile's EndQueuedRebuild,
  // the entry not Rebuilding)
  atest("a cancelled shutdown's closed store held by other work is loaded once the work ends"):
    given ExecutionContext = executionContext
    val heldWork = Some(Work.TransferIn(None, TransferInStage.RetiringEarlierTenancy))
    for
      (host, deviceDir) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1), configured = true)
      gate = recorded(host).holdShutdownCloses()
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).shutdownCloses.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty)
      _ <- client.restartStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = host.control.copyRegistry.work("test", deviceA, s1, _.isInstanceOf[Work.TransferIn], heldWork)
      _ = gate.success(())
      paths = List(s0, s1).map(copyAt(deviceDir, _))
      attempts = () => recorded(host).storeLoadAttempts.map(_._2).toList
      _ <- awaitLoudly("the cancelled shutdown to reload s0")(recorded(host).shutdownOutcomes.nonEmpty &&
             host.control.reconciler.activeDeviceChecks.isEmpty && attempts().contains(paths(0)))
      heldBack = !attempts().contains(paths(1))
      _ = host.control.copyRegistry.work("test", deviceA, s1, _.isInstanceOf[Work.TransferIn], None)
      _ = checkAllNow(host)
      _ <- awaitLoudly("the next reconcile to reload s1")(attempts().contains(paths(1)))
    yield
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Cancelled))
      heldBack should be(true)
      attempts() should contain(paths(1))

  // A copy whose phase cannot be read is still offered to tryLoadStore, which reads it again and holds it back until it
  // answers: a config stat that fails is no absence (CopyDecision.Load offers it), so skipping it would leave it
  // unserved until a restart
  atest("a cancelled shutdown offers a store whose phase cannot be read"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1), configured = true)
      gate = recorded(host).holdShutdownCloses()
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).shutdownCloses.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty)
      // Its stats now fail (EACCES): its load cannot read it
      unreadable = copyAt(deviceDir, s0)
      _ = os.perms.set(unreadable, "rw-------")
      _ <- client.restartStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = gate.success(())
      paths = List(s0, s1).map(copyAt(deviceDir, _))
      _ <- pumpUntil(recorded(host).shutdownOutcomes.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty &&
                     paths.forall(recorded(host).storeLoadAttempts.map(_._2).contains))
      _ = os.perms.set(unreadable, "rwx------")
    yield
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Cancelled))
      recorded(host).storeLoadAttempts.map(_._2).toSet should contain allOf(unreadable, copyAt(deviceDir, s1))

  atest("an Initializing store is not created while the device is ShuttingDown"):
    given ExecutionContext = executionContext
    // A real pool, so createNewStore would get as far as creating the directory.
    val creating = StoreId(PoolId.BootstrapPoolId, 0)
    val initializing = StoreEntry(StoreStatus.Initializing, None)
    for
      (host, deviceDir) <- setUp(Map(creating -> initializing))
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      CopyFixtures.copyPath(deviceDir, creating) should be(None)
      st.stores(creating) should be(initializing)
      st.status should be(DeviceStatus.Shutdown)

  atest("StartStoreTransfer for a store on a ShuttingDown device is ignored"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val sourceId = StorageDeviceId.BootstrapStorageDeviceId
    val deviceDir = writeDevice(hostRoot, "dev0", sourceId)
    val sends = new StagedSendService
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)
    // Placed before the device's load, which records it: startOut ships only a recorded copy
    bareCopy(deviceDir, movingStore)
    val host = newHost(hostRoot, transferService = sends)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(sourceId)(_.copy(status = DeviceStatus.ShuttingDown))
      _ <- waitForTransactionsToComplete()
      refused <- host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId,
                   sourceId, movingStore, HLCTimestamp.now, UUID.randomUUID())).failed
    yield
      refused.getMessage should include("ShuttingDown")
      sends.transfersOut should be(Vector())
      outbound(deviceDir, movingStore) should be(false)

  atest("a transfer in from a Shutdown source is not started"):
    given ExecutionContext = executionContext
    val transfers = new StagedTransferService
    val inEntry = StoreEntry(StoreStatus.TransferringIn, Some(deviceB), Some(UUID.randomUUID()))
    for
      _ <- net.createDevice(deviceB)
      _ <- setDeviceState(deviceB)(_.copy(status = DeviceStatus.Shutdown))
      (host, _) <- setUp(Map(s0 -> inEntry), transfers = transfers)
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).transferSourceReads.contains(deviceB))
      // The record precedes Host's continuation on the serial ExecutionContext; these yields let
      // that continuation run before the assertion's snapshot.
      _ <- yieldUntil(false, 10)
      startedWhileShutdown = transfers.startedTransfersIn
      // Positive control: the same check starts the transfer once the source is back.
      _ <- setDeviceState(deviceB)(_.copy(status = DeviceStatus.Operational))
      _ = checkAllNow(host)
      _ <- pumpUntil(transfers.startedTransfersIn.nonEmpty)
    yield
      recorded(host).transferSourceReads.toList should be(List(deviceB, deviceB))
      startedWhileShutdown should be(Set())
      transfers.startedTransfersIn should be(Set(s0))

  atest("a transfer in whose pool update commits after the shutdown request leaves its entry Offline"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val destId = net.secondDeviceId
    val deviceDir = writeDevice(hostRoot, "dev0", destId)
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, destId)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(transfers.startedTransfersIn.contains(movingStore))
      // Hold the pool update between promotion and its transaction, then request the shutdown.
      gate = recorded(host).holdTransferOutcomes()
      _ = transfers.finishTransferIn(movingStore)
      _ <- pumpUntil(CopyFixtures.phaseOf(deviceDir, movingStore).exists(_.isInstanceOf[Phase.Arrived]))
      _ <- client.shutdownStorageDevice(destId)
      _ <- waitForTransactionsToComplete()
      _ = gate.success(())
      _ <- pumpUntil(recorded(host).transferOutcomes.contains(movingStore))
      entryAfterUpdate <- state(destId).map(_.stores.get(movingStore))
      _ = checkAllNow(host)
      _ <- pumpUntil(!loadedDevices(host).contains(destId))
      st <- state(destId)
      pool <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      recorded(host).transferOutcomes(movingStore) should be(DeviceMetadata.TransferOutcome.PoolRepointedOffline)
      entryAfterUpdate.map(_.status) should be(Some(StoreStatus.Offline))
      pool.stores(movingStore.poolIndex).storageDeviceId should be(destId)
      recorded(host).loadStoreByIdRequests.toList should be(Nil)
      // Kept as it arrived, in arrived/, and not served.
      CopyFixtures.phaseOf(deviceDir, movingStore).exists(_.isInstanceOf[Phase.Arrived]) should be(true)
      host.control.router.silentStores should contain(movingStore)
      st.stores(movingStore).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a rebuild completing after the shutdown request leaves its entry Offline"):
    given ExecutionContext = executionContext
    // RecordedRebuild ignores cancel(), as a rebuild whose finish had already begun does.
    val rebuilds = new RecordingRebuildFactory
    for
      (host, deviceDir) <- setUp(Map(s0 -> StoreEntry(StoreStatus.Rebuilding, None)), rebuilds = rebuilds)
      _ = checkAllNow(host)
      _ <- pumpUntil(rebuilds.created.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      // The rebuilt store lands in rebuilt/, with the empty save file finish writes.
      _ = rebuiltCopy(deviceDir, s0)
      _ = RebuildDisk.writeEmptyCrlState(copyAt(deviceDir, s0), s0)
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpUntil(recorded(host).rebuildFlipSeq.contains((deviceA, s0)))
      entryAfterFlip <- state(deviceA).map(_.stores.get(s0))
      _ = checkAllNow(host)
      _ <- pumpUntil(!loadedDevices(host).contains(deviceA))
      st <- state(deviceA)
    yield
      entryAfterFlip.map(_.status) should be(Some(StoreStatus.Offline))
      recorded(host).loadStoreByIdRequests.toList should be(Nil)
      parked(deviceDir, s0) should be(true)
      // Kept in the parked copy, so an adopter accepts it.
      os.exists(Backend.crlSaveFile(copyAt(deviceDir, s0))) should be(true)
      host.control.router.silentStores should contain(s0)
      st.stores(s0).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a store created as its device is shut down is flipped Offline, then closed"):
    given ExecutionContext = executionContext
    // A real pool, so createNewStore gets as far as loading the store.
    val creating = StoreId(PoolId.BootstrapPoolId, 0)
    for
      (host, deviceDir) <- setUp(Map(creating -> StoreEntry(StoreStatus.Initializing, None)))
      gate = recorded(host).holdStoreLoads()
      _ = checkAllNow(host)
      _ <- pumpUntil({ host.testingOnlyHandleEvents(); recorded(host).loadStoreRequests.nonEmpty })
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      // The load completes and the flip runs against the ShuttingDown device.
      _ = gate.success(())
      _ <- pumpUntilEntry(deviceA, creating)(!_.exists(_.status == StoreStatus.Initializing))
      entryAfterFlip <- state(deviceA).map(_.stores.get(creating))
      _ = checkAllNow(host)
      // The shutdown's close of the opening store waits for its LoadStore to install it, then closes it
      _ <- pumpUntil({ host.testingOnlyHandleEvents(); !loadedDevices(host).contains(deviceA) })
      st <- state(deviceA)
    yield
      recorded(host).loadStoreRequests.toList should be(List((deviceA, creating)))
      entryAfterFlip.map(_.status) should be(Some(StoreStatus.Offline))
      recorded(host).shutdownCloses.toList should be(List(creating))
      noMoveFiles(deviceDir, creating) should be(true)
      st.stores(creating).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a create that keeps failing does not block the shutdown"):
    given ExecutionContext = executionContext
    // No such pool: the create fails on every attempt.
    val creating = storeN(7)
    val initializing = StoreEntry(StoreStatus.Initializing, None)
    for
      (host, _) <- setUp(Map(creating -> initializing))
      _ = checkAllNow(host)
      _ <- pumpUntil(host.control.reconciler.activeDeviceChecks.isEmpty)
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      loadedDevices(host).contains(deviceA) should be(false)
      st.stores(creating) should be(initializing)
      st.status should be(DeviceStatus.Shutdown)

  private val rebuilding = StoreEntry(StoreStatus.Rebuilding, None)

  private val otherHostId = HostId(UUID.fromString("0b0b0b0b-0000-0000-0000-000000000001"))

  atest("a transfer finishing onto a device another host now owns writes nothing"):
    given ExecutionContext = executionContext
    val host = newHost(newHostDir())
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)
    val destId = net.secondDeviceId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, destId)
      _ <- waitForTransactionsToComplete()
      // The destination was shut down and adopted elsewhere while the bytes were in flight.
      _ <- setDeviceState(destId)(_.copy(hostId = otherHostId))
      poolBefore <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      srcBefore <- state(net.storageDeviceId)
      dstBefore <- state(destId)
      _ = net.takeCapturedGossip()
      outcome <- host.control.metadata.updateStateForTransferredStore(movingStore, net.storageDeviceId, destId,
                    host.gossipRetransmitter)
      _ <- waitForTransactionsToComplete()
      poolAfter <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      srcAfter <- state(net.storageDeviceId)
      dstAfter <- state(destId)
    yield
      outcome should be(DeviceMetadata.TransferOutcome.NotOwned)
      poolAfter.stores.toList should be(poolBefore.stores.toList)
      srcAfter should be(srcBefore)
      dstAfter should be(dstBefore)
      net.takeCapturedGossip().collect { case g: StoreMoved => g } should be(Nil)

  atest("a rebuild finishing on a device another host now owns writes nothing and keeps the copy"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      (host, deviceDir) <- setUp(Map(s0 -> rebuilding), rebuilds = rebuilds)
      _ = checkAllNow(host)
      _ <- pumpUntil(rebuilds.created.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty)
      _ <- setDeviceState(deviceA)(_.copy(hostId = otherHostId))
      _ = rebuiltCopy(deviceDir, s0)
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpUntil(recorded(host).rebuildOutcomes.contains((deviceA, s0)))
      st <- state(deviceA)
    yield
      recorded(host).rebuildOutcomes((deviceA, s0)) should be(DeviceMetadata.RebuildOutcome.NotOwned)
      st.stores(s0) should be(rebuilding)
      st.hostId should be(otherHostId)
      recorded(host).loadStoreByIdRequests.toList should be(Nil)
      // Left for the owner, whose rebuild finds the store in place and only flips it.
      CopyFixtures.copyPath(deviceDir, s0) should be(defined)
      CopyFixtures.phaseOf(deviceDir, s0) should be(Some(Phase.Rebuilt("")))

  /** setUp with s0 Rebuilding under a RecordedRebuild, which ignores cancel() as a rebuild whose
   *  walk is still reading does, and s1 Active and open. The shutdown is requested and checked,
   *  and has reached its wait for the rebuild. */
  private def shutDownDuringRebuild(): Future[(Host, os.Path, RecordingRebuildFactory)] =
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      (host, deviceDir) <- setUp(Map(s0 -> rebuilding, s1 -> active), loaded = Set(s1), rebuilds = rebuilds)
      _ = checkAllNow(host)
      _ <- pumpUntil(rebuilds.created.nonEmpty && host.control.reconciler.activeDeviceChecks.isEmpty)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).shutdownWorkTimeouts.nonEmpty)
    yield (host, deviceDir, rebuilds)

  atest("shutdown waits for a cancelled rebuild to stop before closing the device's stores"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir, rebuilds) <- shutDownDuringRebuild()
      closesWhileWaiting = recorded(host).shutdownCloses.toList
      outcomesWhileWaiting = recorded(host).shutdownOutcomes.toList
      // The walk ends and the rebuilt store lands, after the shutdown request.
      _ = rebuiltCopy(deviceDir, s0)
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpUntil(!loadedDevices(host).contains(deviceA))
      st <- state(deviceA)
    yield
      recorded(host).shutdownWorkTimeouts.map(_._1).toList should be(List(deviceA))
      rebuilds.created.head.cancelled should be(true)
      closesWhileWaiting should be(Nil)
      outcomesWhileWaiting should be(Nil)
      recorded(host).shutdownCloses.toList should be(List(s1))
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
      // Kept offline as the shutdown's own closes are, before Shutdown committed.
      parked(deviceDir, s0) should be(true)
      st.stores(s0).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a shutdown whose cancelled rebuild does not stop proceeds once its timeout passes"):
    given ExecutionContext = executionContext
    for
      (host, _, rebuilds) <- shutDownDuringRebuild()
      closesWhileWaiting = recorded(host).shutdownCloses.toList
      _ = recorded(host).fireShutdownWorkTimeouts()
      _ <- pumpUntil(!loadedDevices(host).contains(deviceA))
      st <- state(deviceA)
    yield
      closesWhileWaiting should be(Nil)
      rebuilds.created.head.promise.isCompleted should be(false)
      recorded(host).shutdownCloses.toList should be(List(s1))
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
      // Left as a crash leaves it, for the rebuild to resume after a restart or adoption.
      st.stores(s0) should be(rebuilding)
      st.status should be(DeviceStatus.Shutdown)

  atest("shutdown waits for a transfer in's pool update already in flight"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val destId = net.secondDeviceId
    val deviceDir = writeDevice(hostRoot, "dev0", destId)
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, destId)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(transfers.startedTransfersIn.contains(movingStore))
      gate = recorded(host).holdTransferOutcomes()
      _ = transfers.finishTransferIn(movingStore)
      _ <- pumpUntil(CopyFixtures.phaseOf(deviceDir, movingStore).exists(_.isInstanceOf[Phase.Arrived]))
      _ <- client.shutdownStorageDevice(destId)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).shutdownWorkTimeouts.nonEmpty)
      outcomesWhileWaiting = recorded(host).shutdownOutcomes.toList
      _ = gate.success(())
      _ <- pumpUntil(!loadedDevices(host).contains(destId))
      st <- state(destId)
    yield
      outcomesWhileWaiting should be(Nil)
      recorded(host).transferOutcomes(movingStore) should be(DeviceMetadata.TransferOutcome.PoolRepointedOffline)
      recorded(host).shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
      st.stores(movingStore).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a transfer out whose store fails during its close is never sent and goes back to be rebuilt"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val sourceId = StorageDeviceId.BootstrapStorageDeviceId
    val deviceDir = writeDevice(hostRoot, "dev0", sourceId)
    val sends = new StagedSendService
    val rebuilds = new RecordingRebuildFactory
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)
    // Placed before the device's load, which records it: startOut ships only a recorded copy
    bareCopy(deviceDir, movingStore)
    val host = newHost(hostRoot, transferService = sends, storeRebuildFactory = rebuilds)
    def ask() = host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId,
      sourceId, movingStore, HLCTimestamp.now, UUID.randomUUID()))

    for
      // The device's other stores, which the TestNetwork serves elsewhere: copies here, registered as a load would
      // find them, so the reconcile does not take them for lost ones
      sourceState <- state(sourceId)
      _ = sourceState.stores.keysIterator.filter(_ != movingStore).foreach(writeStoreConfig(deviceDir, _))
      _ = host.testingOnlyRescan(sourceId)
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      // The close fails as ShutdownStore's does for a backend that failed-stop: marked, offline, unloaded
      _ = recorded(host).failNextTransferOutClose.set(Some(() =>
            host.control.rebuilds.moveToLostOnLane("failedOnClose", loadedDevices(host)(sourceId), movingStore)(_ => ())))
      _ <- ask()
      _ <- pumpUntil(recorded(host).lostTransferAborts.nonEmpty)
      // A retransmit meanwhile is refused before anything is closed, written or sent
      retransmit <- ask().failed
      _ <- pumpUntil(false, rounds = 5)
      _ <- pumpUntilEntry(sourceId, movingStore)(_.exists(_.status == StoreStatus.Rebuilding))
      _ <- pumpUntil(rebuilds.created.nonEmpty)
      st <- state(sourceId)
      dest <- state(net.secondDeviceId)
    yield
      retransmit.getMessage should include("Lost")
      sends.transfersOut should be(Vector())
      recorded(host).transferOutCloses.toList should be(List(movingStore))
      outbound(deviceDir, movingStore) should be(false)
      st.stores(movingStore).status should be(StoreStatus.Rebuilding)
      dest.stores.get(movingStore) should be(None)
      rebuilds.created.map(_.storeId).toList should be(List(movingStore))

  atest("a transfer out whose store closes as its device starts shutting down does not begin"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val sourceId = StorageDeviceId.BootstrapStorageDeviceId
    val deviceDir = writeDevice(hostRoot, "dev0", sourceId)
    val sends = new StagedSendService
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)
    // Placed before the device's load, which records it: startOut ships only a recorded copy
    bareCopy(deviceDir, movingStore)
    val host = newHost(hostRoot, transferService = sends)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      // The request passes every check and starts closing the store, which is held open.
      transferGate = recorded(host).holdTransferOutCloses()
      _ <- host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId,
             sourceId, movingStore, HLCTimestamp.now, UUID.randomUUID()))
      closesRequested = recorded(host).transferOutCloses.toList
      // The device's shutdown begins meanwhile and is held in its own close of the store. The
      // bootstrap device holds the whole bootstrap pool, so the real request would refuse.
      shutdownGate = recorded(host).holdShutdownCloses()
      _ <- forceShuttingDown(sourceId)
      _ = checkAllNow(host)
      _ <- pumpUntil(recorded(host).shutdownCloses.nonEmpty)
      // The transfer's close finishes while the shutdown runs. Its continuation is queued on the
      // serial ExecutionContext ahead of these yields.
      _ = transferGate.success(())
      _ <- yieldUntil(false, 10)
      sendsWhileShuttingDown = sends.transfersOut
      _ = shutdownGate.success(())
      _ <- pumpUntil(!loadedDevices(host).contains(sourceId))
      st <- state(sourceId)
    yield
      closesRequested should be(List(movingStore))
      sendsWhileShuttingDown should be(Vector())
      sends.transfersOut should be(Vector())
      // Not marked: the transfer never began, and the shutdown marks no store it closes
      outbound(deviceDir, movingStore) should be(false)
      st.stores(movingStore).status should be(StoreStatus.TransferringOut)
      st.status should be(DeviceStatus.Shutdown)
