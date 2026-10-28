package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.network.{StartStoreTransfer, StoreOffline}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.ShutdownOutcome
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
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
