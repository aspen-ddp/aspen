package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.network.{StartStoreTransfer, StoreMoved, StoreOffline}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.ShutdownOutcome
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** A StoreRebuild that runs until cancelled, then fails the way RebuildingStore does. */
private class CancellableRebuild(val storeId: StoreId) extends StoreRebuild:
  private val promise: Promise[Unit] = Promise()
  @volatile var cancelled: Boolean = false
  def complete: Future[Unit] = promise.future
  def cancel(): Unit =
    cancelled = true
    promise.tryFailure(RebuildingStore.RebuildCancelled(storeId))

private class CancellableRebuildFactory extends StoreRebuildFactory:
  val created: mutable.ListBuffer[CancellableRebuild] = mutable.ListBuffer()

  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: os.Path): StoreRebuild = synchronized:
    val r = new CancellableRebuild(storeId)
    created += r
    r

class DeviceShutdownSuite extends IntegrationTestSuite with HostTestHarness:
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
   *  would have done. RecordingHost never opens a backend, so that is done by hand. */
  private def setUp(stores: Map[StoreId, StoreEntry],
                    loaded: Set[StoreId] = Set(),
                    transfers: StagedTransferService = new StagedTransferService,
                    rebuilds: StoreRebuildFactory = StoreRebuildFactory.Filesystem): Future[(RecordingHost, os.Path)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot, transferService = transfers, storeRebuildFactory = rebuilds)
    loaded.foreach(s => os.makeDir.all(deviceDir / s.directoryName))
    val local = host.loadedDevices(deviceA)
    local.loadedStores = local.loadedStores ++ loaded
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = stores))
      _ <- waitForTransactionsToComplete()
    yield (host, deviceDir)

  /** Requests the shutdown, runs one check and waits for the device to be unloaded. */
  private def shutDown(host: RecordingHost): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(!host.loadedDevices.contains(deviceA))
    yield ()

  atest("a ShuttingDown device has its stores closed, marked and announced, then is Shutdown"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1))
      _ <- addToBootstrapHost(deviceA)
      _ = net.takeCapturedGossip()
      _ <- shutDown(host)
      st <- state(deviceA)
      hostState <- client.getHostState(HostId.BootstrapHostId)
    yield
      host.shutdownCloses.toSet should be(Set(s0, s1))
      host.shutdownCloses.size should be(2)
      os.exists(deviceDir / s0.directoryName / StoreTransfer.TransferOutMarkerFile) should be(true)
      os.exists(deviceDir / s1.directoryName / StoreTransfer.TransferOutMarkerFile) should be(true)
      DeviceMarkers.readShutdownState(deviceDir) should be(Some(HostId.BootstrapHostId))
      st.status should be(DeviceStatus.Shutdown)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Offline))
      net.takeCapturedGossip().collect { case g: StoreOffline => g.storeId }.toSet should be(Set(s0, s1))
      host.loadedDevices.contains(deviceA) should be(false)
      host.testingOnlyLoadedDeviceDirs.values.toSet should not contain deviceA
      hostState.storageDevices should contain(deviceA)
      host.shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))

  atest("shutdown cancels a running rebuild and does not restart it"):
    given ExecutionContext = executionContext
    val rebuilds = new CancellableRebuildFactory
    val rebuilding = StoreEntry(StoreStatus.Rebuilding, None)
    for
      (host, _) <- setUp(Map(s0 -> rebuilding), rebuilds = rebuilds)
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(rebuilds.created.nonEmpty)
      _ <- shutDown(host)
      // A check after the shutdown must not start the rebuild again either.
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.testingOnlyActiveDeviceChecks.isEmpty)
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
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.nonEmpty)
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      transfers.startedTransfersIn should be(Set(s0))
      transfers.cancelledTransfersIn.map(_._1) should be(Vector(s0))
      transfers.cancelledTransfersOut should be(Vector(s1))
      host.testingOnlyTransferringIn should be(Set())
      st.stores should be(Map(s0 -> inEntry, s1 -> outEntry))
      st.status should be(DeviceStatus.Shutdown)

  atest("a store that becomes Active during shutdown is taken offline before Shutdown commits"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active), loaded = Set(s0))
      // Between the stores closing and the Shutdown commit, a late rebuild completion flips s1 to
      // Active and loads it.
      _ = host.beforeNextShutdownCommit: () =>
            os.makeDir.all(deviceDir / s1.directoryName)
            val local = host.loadedDevices(deviceA)
            local.loadedStores = local.loadedStores + s1
            setDeviceState(deviceA)(_.setStoreEntry(s1, StoreStatus.Active, None))
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      host.shutdownOutcomes.toList should be(List(ShutdownOutcome.StoresReactivated, ShutdownOutcome.Completed))
      host.shutdownCloses.toList should be(List(s0, s1))
      os.exists(deviceDir / s1.directoryName / StoreTransfer.TransferOutMarkerFile) should be(true)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Offline))
      st.status should be(DeviceStatus.Shutdown)

  atest("repeated checks during a shutdown do not start a second one"):
    given ExecutionContext = executionContext
    for
      (host, _) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1))
      gate = host.holdShutdownCloses()
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.shutdownCloses.nonEmpty && host.testingOnlyActiveDeviceChecks.isEmpty)
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.testingOnlyActiveDeviceChecks.isEmpty)
      closesWhileHeld = host.shutdownCloses.toList
      _ = gate.success(())
      _ <- pumpUntil(!host.loadedDevices.contains(deviceA))
      st <- state(deviceA)
    yield
      closesWhileHeld.size should be(1)
      host.shutdownCloses.toList.sortBy(_.poolIndex) should be(List(s0, s1))
      host.shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
      st.status should be(DeviceStatus.Shutdown)

  atest("restart during shutdown cancels it"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir) <- setUp(Map(s0 -> active, s1 -> active), loaded = Set(s0, s1))
      gate = host.holdShutdownCloses()
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.shutdownCloses.nonEmpty && host.testingOnlyActiveDeviceChecks.isEmpty)
      _ <- client.restartStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = gate.success(())
      markers = List(s0, s1).map(s => deviceDir / s.directoryName / StoreTransfer.TransferOutMarkerFile)
      _ <- pumpUntil(host.shutdownOutcomes.nonEmpty && host.testingOnlyActiveDeviceChecks.isEmpty &&
                     markers.forall(m => !os.exists(m)))
      st <- state(deviceA)
    yield
      host.shutdownOutcomes.toList should be(List(ShutdownOutcome.Cancelled))
      st.status should be(DeviceStatus.Operational)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Active))
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      host.loadedDevices.contains(deviceA) should be(true)
      // The reconcile's abandoned-transfer-out pass brought both stores back.
      markers.exists(os.exists) should be(false)
      host.storeLoadAttempts.map(_._2).toSet should contain allOf(deviceDir / s0.directoryName,
                                                                  deviceDir / s1.directoryName)

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
      os.exists(deviceDir / creating.directoryName) should be(false)
      st.stores(creating) should be(initializing)
      st.status should be(DeviceStatus.Shutdown)

  atest("StartStoreTransfer for a store on a ShuttingDown device is ignored"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val sourceId = StorageDeviceId.BootstrapStorageDeviceId
    val deviceDir = writeDevice(hostRoot, "dev0", sourceId)
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends)
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)
    os.makeDir.all(deviceDir / movingStore.directoryName)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(sourceId)(_.copy(status = DeviceStatus.ShuttingDown))
      _ <- waitForTransactionsToComplete()
      refused <- host.testingOnlyStartStoreTransferOut(StartStoreTransfer(host.hostId, client.clientId,
                   sourceId, movingStore, HLCTimestamp.now, UUID.randomUUID())).failed
    yield
      refused.getMessage should include("ShuttingDown")
      sends.transfersOut should be(Vector())
      os.exists(deviceDir / movingStore.directoryName / StoreTransfer.TransferOutMarkerFile) should be(false)

  atest("a transfer in from a Shutdown source is not started"):
    given ExecutionContext = executionContext
    val transfers = new StagedTransferService
    val inEntry = StoreEntry(StoreStatus.TransferringIn, Some(deviceB), Some(UUID.randomUUID()))
    for
      _ <- net.createDevice(deviceB)
      _ <- setDeviceState(deviceB)(_.copy(status = DeviceStatus.Shutdown))
      (host, _) <- setUp(Map(s0 -> inEntry), transfers = transfers)
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.transferSourceReads.contains(deviceB))
      // The record precedes Host's continuation on the serial ExecutionContext; these yields let
      // that continuation run before the assertion's snapshot.
      _ <- yieldUntil(false, 10)
      startedWhileShutdown = transfers.startedTransfersIn
      // Positive control: the same check starts the transfer once the source is back.
      _ <- setDeviceState(deviceB)(_.copy(status = DeviceStatus.Operational))
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.nonEmpty)
    yield
      host.transferSourceReads.toList should be(List(deviceB, deviceB))
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
    val arrived = deviceDir / movingStore.directoryName

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, destId)
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.contains(movingStore))
      // Hold the pool update between promotion and its transaction, then request the shutdown.
      gate = host.holdTransferOutcomes()
      _ = transfers.finishTransferIn(movingStore)
      _ <- pumpUntil(os.exists(arrived / StoreTransfer.TransferredInMarkerFile))
      _ <- client.shutdownStorageDevice(destId)
      _ <- waitForTransactionsToComplete()
      _ = gate.success(())
      _ <- pumpUntil(host.transferOutcomes.contains(movingStore))
      entryAfterUpdate <- state(destId).map(_.stores.get(movingStore))
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(!host.loadedDevices.contains(destId))
      st <- state(destId)
      pool <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      host.transferOutcomes(movingStore) should be(Host.TransferOutcome.PoolRepointedOffline)
      entryAfterUpdate.map(_.status) should be(Some(StoreStatus.Offline))
      pool.stores(movingStore.poolIndex).storageDeviceId should be(destId)
      host.loadStoreByIdRequests.toList should be(Nil)
      // Kept as it arrived, behind its transferred-in marker, and not served.
      os.exists(arrived / StoreTransfer.TransferredInMarkerFile) should be(true)
      host.testingOnlyOfflineStores should contain(movingStore)
      st.stores(movingStore).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a rebuild completing after the shutdown request leaves its entry Offline"):
    given ExecutionContext = executionContext
    // RecordedRebuild ignores cancel(), as a rebuild whose finish had already begun does.
    val rebuilds = new RecordingRebuildFactory
    for
      (host, deviceDir) <- setUp(Map(s0 -> StoreEntry(StoreStatus.Rebuilding, None)), rebuilds = rebuilds)
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(rebuilds.created.nonEmpty && host.testingOnlyActiveDeviceChecks.isEmpty)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      // The rebuilt store lands at its final path, as the walk leaves it.
      _ = os.makeDir.all(deviceDir / s0.directoryName)
      _ = rebuilds.created.head.promise.success(())
      _ <- pumpUntil(host.rebuildFlipSeq.contains((deviceA, s0)))
      entryAfterFlip <- state(deviceA).map(_.stores.get(s0))
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(!host.loadedDevices.contains(deviceA))
      st <- state(deviceA)
    yield
      entryAfterFlip.map(_.status) should be(Some(StoreStatus.Offline))
      host.loadStoreByIdRequests.toList should be(Nil)
      os.exists(deviceDir / s0.directoryName / StoreTransfer.TransferOutMarkerFile) should be(true)
      // Empty, as a fresh rebuild has no transaction state, but present so an adopter accepts it.
      os.exists(Backend.crlSaveFile(deviceDir / s0.directoryName)) should be(true)
      host.testingOnlyOfflineStores should contain(s0)
      st.stores(s0).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a store created as its device is shut down is flipped Offline, then closed"):
    given ExecutionContext = executionContext
    // A real pool, so createNewStore gets as far as loading the store.
    val creating = StoreId(PoolId.BootstrapPoolId, 0)
    for
      (host, deviceDir) <- setUp(Map(creating -> StoreEntry(StoreStatus.Initializing, None)))
      gate = host.holdStoreLoads()
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.loadStoreRequests.nonEmpty)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      // The load completes and the flip runs against the ShuttingDown device.
      _ = gate.success(())
      _ <- pumpUntilEntry(deviceA, creating)(!_.exists(_.status == StoreStatus.Initializing))
      entryAfterFlip <- state(deviceA).map(_.stores.get(creating))
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(!host.loadedDevices.contains(deviceA))
      st <- state(deviceA)
    yield
      host.loadStoreRequests.toList should be(List((deviceA, creating)))
      entryAfterFlip.map(_.status) should be(Some(StoreStatus.Offline))
      host.shutdownCloses.toList should be(List(creating))
      os.exists(deviceDir / creating.directoryName / StoreTransfer.TransferOutMarkerFile) should be(true)
      st.stores(creating).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a create that keeps failing does not block the shutdown"):
    given ExecutionContext = executionContext
    // No such pool: the create fails on every attempt.
    val creating = storeN(7)
    val initializing = StoreEntry(StoreStatus.Initializing, None)
    for
      (host, _) <- setUp(Map(creating -> initializing))
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.testingOnlyActiveDeviceChecks.isEmpty)
      _ <- shutDown(host)
      st <- state(deviceA)
    yield
      host.loadedDevices.contains(deviceA) should be(false)
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
      outcome <- host.testingOnlyUpdateStateForTransferredStore(movingStore, net.storageDeviceId, destId)
      _ <- waitForTransactionsToComplete()
      poolAfter <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      srcAfter <- state(net.storageDeviceId)
      dstAfter <- state(destId)
    yield
      outcome should be(Host.TransferOutcome.NotOwned)
      poolAfter.stores.toList should be(poolBefore.stores.toList)
      srcAfter should be(srcBefore)
      dstAfter should be(dstBefore)
      net.takeCapturedGossip().collect { case g: StoreMoved => g } should be(Nil)

  atest("a rebuild finishing on a device another host now owns writes nothing and keeps the copy"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      (host, deviceDir) <- setUp(Map(s0 -> rebuilding), rebuilds = rebuilds)
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(rebuilds.created.nonEmpty && host.testingOnlyActiveDeviceChecks.isEmpty)
      _ <- setDeviceState(deviceA)(_.copy(hostId = otherHostId))
      _ = os.makeDir.all(deviceDir / s0.directoryName)
      _ = rebuilds.created.head.promise.success(())
      _ <- pumpUntil(host.rebuildOutcomes.contains((deviceA, s0)))
      st <- state(deviceA)
    yield
      host.rebuildOutcomes((deviceA, s0)) should be(Host.RebuildOutcome.NotOwned)
      st.stores(s0) should be(rebuilding)
      st.hostId should be(otherHostId)
      host.loadStoreByIdRequests.toList should be(Nil)
      // Left for the owner, whose rebuild finds the store in place and only flips it.
      os.exists(deviceDir / s0.directoryName) should be(true)
      os.exists(deviceDir / s0.directoryName / StoreTransfer.TransferOutMarkerFile) should be(false)

  /** setUp with s0 Rebuilding under a RecordedRebuild, which ignores cancel() as a rebuild whose
   *  walk is still reading does, and s1 Active and open. The shutdown is requested and checked,
   *  and has reached its wait for the rebuild. */
  private def shutDownDuringRebuild(): Future[(RecordingHost, os.Path, RecordingRebuildFactory)] =
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      (host, deviceDir) <- setUp(Map(s0 -> rebuilding, s1 -> active), loaded = Set(s1), rebuilds = rebuilds)
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(rebuilds.created.nonEmpty && host.testingOnlyActiveDeviceChecks.isEmpty)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.shutdownWorkTimeouts.nonEmpty)
    yield (host, deviceDir, rebuilds)

  atest("shutdown waits for a cancelled rebuild to stop before closing the device's stores"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir, rebuilds) <- shutDownDuringRebuild()
      closesWhileWaiting = host.shutdownCloses.toList
      outcomesWhileWaiting = host.shutdownOutcomes.toList
      // The walk ends and the rebuilt store lands, after the shutdown request.
      _ = os.makeDir.all(deviceDir / s0.directoryName)
      _ = rebuilds.created.head.promise.success(())
      _ <- pumpUntil(!host.loadedDevices.contains(deviceA))
      st <- state(deviceA)
    yield
      host.shutdownWorkTimeouts.map(_._1).toList should be(List(deviceA))
      rebuilds.created.head.cancelled should be(true)
      closesWhileWaiting should be(Nil)
      outcomesWhileWaiting should be(Nil)
      host.shutdownCloses.toList should be(List(s1))
      host.shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
      // Kept offline as the shutdown's own closes are, before Shutdown committed.
      os.exists(deviceDir / s0.directoryName / StoreTransfer.TransferOutMarkerFile) should be(true)
      st.stores(s0).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)

  atest("a shutdown whose cancelled rebuild does not stop proceeds once its timeout passes"):
    given ExecutionContext = executionContext
    for
      (host, _, rebuilds) <- shutDownDuringRebuild()
      closesWhileWaiting = host.shutdownCloses.toList
      _ = host.fireShutdownWorkTimeouts()
      _ <- pumpUntil(!host.loadedDevices.contains(deviceA))
      st <- state(deviceA)
    yield
      closesWhileWaiting should be(Nil)
      rebuilds.created.head.promise.isCompleted should be(false)
      host.shutdownCloses.toList should be(List(s1))
      host.shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
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
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.contains(movingStore))
      gate = host.holdTransferOutcomes()
      _ = transfers.finishTransferIn(movingStore)
      _ <- pumpUntil(os.exists(deviceDir / movingStore.directoryName / StoreTransfer.TransferredInMarkerFile))
      _ <- client.shutdownStorageDevice(destId)
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.shutdownWorkTimeouts.nonEmpty)
      outcomesWhileWaiting = host.shutdownOutcomes.toList
      _ = gate.success(())
      _ <- pumpUntil(!host.loadedDevices.contains(destId))
      st <- state(destId)
    yield
      outcomesWhileWaiting should be(Nil)
      host.transferOutcomes(movingStore) should be(Host.TransferOutcome.PoolRepointedOffline)
      host.shutdownOutcomes.toList should be(List(ShutdownOutcome.Completed))
      st.stores(movingStore).status should be(StoreStatus.Offline)
      st.status should be(DeviceStatus.Shutdown)
