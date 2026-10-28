package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, StartStoreTransfer, StoreMoved}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.runSequentially
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBConfig}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Restart of a shut-down device on the host that shut it down, and adoption of one by another
 *  host. The adopting host is always the RecordingHost (BootstrapHostId); the "old host" is a
 *  second host registered with client.createHost. */
class DeviceAdoptionSuite extends IntegrationTestSuite with HostTestHarness:
  import StorageDeviceState.{DeviceStatus, StoreEntry, StoreStatus}

  private val offline = StoreEntry(StoreStatus.Offline, None)
  private val rebuilding = StoreEntry(StoreStatus.Rebuilding, None)

  /** Alternates yields and transaction drains until `condition` holds. Silent on exhaustion: every
   *  caller asserts the end state afterwards. */
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

  /** Waits for the shutdown-marker lookups issued by the last scan, handles their results, then
   *  waits for whatever those started (an adoption, the device check of a loaded device). */
  private def resolve(host: RecordingHost): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- pumpUntil(host.testingOnlyQueuedMarkerResolutions > 0)
      _ = host.testingOnlyHandleMarkerResolutions()
      _ <- pumpUntil(host.testingOnlyPendingMarkerResolutions.isEmpty &&
                     host.testingOnlyActiveDeviceChecks.isEmpty)
    yield ()

  private def scanAndResolve(host: RecordingHost): Future[Unit] =
    host.testingOnlyScanDevices()
    resolve(host)

  private def state(id: StorageDeviceId): Future[StorageDeviceState] = client.getStorageDeviceState(id)

  /** A fresh single-store pool. Its one store is placed on the bootstrap device; tests repoint it. */
  private def newPool(name: String): Future[StoreId] =
    given ExecutionContext = executionContext
    client.createNewStoragePool(name, Replication(1, 1), None, RocksDBConfig(),
                                StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      .map(StoreId(_, 0.toByte))

  private def setPoolEntry(storeId: StoreId, host: HostId, device: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStoragePoolPointer(storeId.poolId)
        kvos <- client.read(ptr)
      yield
        val cfg = StoragePoolState(kvos)
        cfg.stores(storeId.poolIndex) = StoragePoolState.StoreEntry(host, device)
        tx.update(ptr, None, None,
          List(KeyRevision(StoragePoolState.ConfigKey, kvos.contents(StoragePoolState.ConfigKey).revision)),
          List(Insert(StoragePoolState.ConfigKey, cfg.encode())))

  private def addToHost(hostId: HostId, id: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getHostPointer(hostId)
        kvos <- client.read(ptr)
      yield
        tx.update(ptr, None, None,
          List(KeyRevision(HostState.StateKey, kvos.contents(HostState.StateKey).revision)),
          List(Insert(HostState.StateKey, HostState(kvos).addStorageDevice(id).encode())))

  private def poolEntry(storeId: StoreId): Future[StoragePoolState.StoreEntry] =
    given ExecutionContext = executionContext
    client.getStoragePoolState(storeId.poolId).map(_.stores(storeId.poolIndex))

  /** A store directory as the shutdown leaves it: an empty transfer-out marker and a CRL save file. */
  private def writeClosedStore(deviceDir: os.Path, storeId: StoreId, withCrl: Boolean = true): os.Path =
    val dir = deviceDir / storeId.directoryName
    os.makeDir.all(dir)
    os.write.over(dir / StoreTransfer.TransferOutMarkerFile, "")
    if withCrl then
      CrashRecoveryLog.saveStoreState(storeId, Nil, Backend.crlSaveFile(dir))
    dir

  private def marker(dir: os.Path): os.Path = dir / StoreTransfer.TransferOutMarkerFile

  private def storeMoves(): List[StoreMoved] = net.takeCapturedGossip().collect { case g: StoreMoved => g }

  /** The metadata for a shut-down deviceA: `count` single-store pools whose entries name
   *  (`owner`, deviceA), the device in `status` owned by `owner` with `entries(stores)`, and the
   *  device listed in the old host's HostState when that host is registered. Returns the stores.
   *
   *  The old host is registered unless `registerOldHost` is false, in which case `owner` is an id
   *  with no HostState at all. */
  private def setUpMetadata(owner: HostId,
                            status: DeviceStatus,
                            count: Int,
                            registerOldHost: Boolean)
                           (entries: List[StoreId] => Map[StoreId, StoreEntry]): Future[List[StoreId]] =
    given ExecutionContext = executionContext
    for
      _ <-
        if registerOldHost then
          client.createHost("old-host", "10.0.0.9", 5750, 5751, 5752, owner).map(_ => ())
        else
          Future.unit
      _ <- net.createDevice(deviceA)
      // One at a time: pool creations write the same set and device objects, and would collide.
      stores <- runSequentially((0 until count).toList)(i => newPool(s"adopt-pool-$i")).map(_.toList)
      _ <- runSequentially(stores)(s => setPoolEntry(s, owner, deviceA))
      _ <- setDeviceState(deviceA)(_.copy(hostId = owner, status = status, stores = entries(stores)))
      _ <- if registerOldHost then addToHost(owner, deviceA) else Future.unit
      _ <- waitForTransactionsToComplete()
    yield stores

  /** The device directory as carried to the RecordingHost's root: `owning-host` and
   *  `shutdown.state` naming `markerHost` (owning-host `owningHost`). */
  private def writeCarriedDevice(markerHost: HostId,
                                 owningHost: HostId): (os.Path, os.Path) =
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA, owner = Some(owningHost))
    DeviceMarkers.writeShutdownState(deviceDir, markerHost)
    (hostRoot, deviceDir)

  private val oldHostId = HostId(UUID.fromString("0a0a0a0a-0000-0000-0000-000000000001"))

  atest("a properly shut down device from another host is adopted"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 3, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> rebuilding,
                      ss(2) -> StoreEntry(StoreStatus.TransferringOut, Some(deviceB), Some(UUID.randomUUID())))
      List(s0, s1, s2) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeClosedStore(deviceDir, s0)
      // A transfer out interrupted by the shutdown: its own marker, which adoption must leave.
      d2 = writeClosedStore(deviceDir, s2)
      _ = net.takeCapturedGossip()
      host = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ <- resolve(host)
      st <- state(deviceA)
      oldHostState <- client.getHostState(oldHostId)
      newHostState <- client.getHostState(HostId.BootstrapHostId)
      e0 <- poolEntry(s0)
      e1 <- poolEntry(s1)
      e2 <- poolEntry(s2)
    yield
      host.adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should be(StoreStatus.Active)
      st.stores(s1) should be(rebuilding)
      st.stores(s2).status should be(StoreStatus.TransferringOut)
      oldHostState.storageDevices should not contain deviceA
      newHostState.storageDevices should contain(deviceA)
      List(e0, e1, e2).map(_.hostId).toSet should be(Set(HostId.BootstrapHostId))
      List(e0, e1, e2).map(_.storageDeviceId).toSet should be(Set(deviceA))
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(HostId.BootstrapHostId))
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      os.exists(marker(d0)) should be(false)
      os.exists(marker(d2)) should be(true)
      host.loadedDevices.contains(deviceA) should be(true)
      host.storeLoadAttempts.map(_._2) should contain(d0)
      val moves = storeMoves()
      moves.map(_.storeId) should be(List(s0))
      moves.head.hostId should be(HostId.BootstrapHostId)

  atest("adoption resumes a Rebuilding store"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> rebuilding)
      List(s0, s1) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = writeClosedStore(deviceDir, s0)
      host = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ <- resolve(host)
      _ <- pumpUntil(rebuilds.created.nonEmpty)
    yield
      rebuilds.created.map(_.storeId).toList should be(List(s1))
      rebuilds.created.head.storageDeviceId should be(deviceA)

  atest("a device missing a store's CRL file is not adopted"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> offline)
      s0 = stores.head
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeClosedStore(deviceDir, s0, withCrl = false)
      host = newHost(hostRoot)
      _ <- resolve(host)
      _ <- scanAndResolve(host)
      stBefore <- state(deviceA)
      warningsBefore = host.deviceDirWarnings.count(_._1 == deviceDir)
      adoptsBefore = host.adoptCalls.size
      loadedBefore = host.loadedDevices.contains(deviceA)
      _ = CrashRecoveryLog.saveStoreState(s0, Nil, Backend.crlSaveFile(d0))
      _ <- scanAndResolve(host)
      stAfter <- state(deviceA)
    yield
      adoptsBefore should be(0)
      warningsBefore should be(1)
      loadedBefore should be(false)
      stBefore.status should be(DeviceStatus.Shutdown)
      stBefore.hostId should be(oldHostId)
      host.adoptCalls.toList should be(List((deviceA, oldHostId)))
      stAfter.status should be(DeviceStatus.Operational)
      stAfter.hostId should be(HostId.BootstrapHostId)
      host.loadedDevices.contains(deviceA) should be(true)

  // A disk pulled before the save reached it, or one whose save file was copied from another
  // store, must not be adopted: the CRL state the store needs is not in the file.
  for (what, corrupt) <- List[(String, (os.Path, StoreId) => Unit)](
        ("a truncated", (file, _) => os.write.over(file, "")),
        ("another store's", (file, s) =>
          CrashRecoveryLog.saveStoreState(StoreId(s.poolId, (s.poolIndex + 1).toByte), Nil, file))) do
    atest(s"a device with $what CRL save file is not adopted"):
      given ExecutionContext = executionContext
      for
        stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true): ss =>
                    Map(ss(0) -> offline)
        s0 = stores.head
        (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
        d0 = writeClosedStore(deviceDir, s0)
        _ = corrupt(Backend.crlSaveFile(d0), s0)
        host = newHost(hostRoot)
        _ <- resolve(host)
        st <- state(deviceA)
      yield
        host.adoptCalls.toList should be(Nil)
        host.loadedDevices.contains(deviceA) should be(false)
        host.deviceDirWarnings.count(_._1 == deviceDir) should be(1)
        st.status should be(DeviceStatus.Shutdown)
        st.hostId should be(oldHostId)

  atest("a device whose shutdown did not finish is refused by another host"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.ShuttingDown, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> offline)
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = writeClosedStore(deviceDir, stores.head)
      host = newHost(hostRoot)
      _ <- resolve(host)
      _ <- scanAndResolve(host)
      st <- state(deviceA)
    yield
      host.loadedDevices.contains(deviceA) should be(false)
      host.adoptCalls.toList should be(Nil)
      host.deviceDirWarnings.count(_._1 == deviceDir) should be(1)
      st.status should be(DeviceStatus.ShuttingDown)
      st.hostId should be(oldHostId)
      DeviceMarkers.readShutdownState(deviceDir) should be(Some(oldHostId))

  atest("a crash between the adoption commit and the load is finished on the next scan"):
    given ExecutionContext = executionContext
    for
      // The adoption committed: Operational, this host, the formerly Offline store Active.
      stores <- setUpMetadata(oldHostId, DeviceStatus.Operational, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> StoreEntry(StoreStatus.Active, None))
      s0 = stores.head
      _ <- setDeviceState(deviceA)(_.copy(hostId = HostId.BootstrapHostId))
      _ <- setPoolEntry(s0, HostId.BootstrapHostId, deviceA)
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeClosedStore(deviceDir, s0)
      _ = net.takeCapturedGossip()
      host = newHost(hostRoot)
      _ <- resolve(host)
    yield
      host.adoptCalls.toList should be(Nil)
      host.loadedDevices.contains(deviceA) should be(true)
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(HostId.BootstrapHostId))
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      os.exists(marker(d0)) should be(false)
      host.storeLoadAttempts.map(_._2) should contain(d0)
      storeMoves().map(_.storeId) should be(List(s0))

  atest("adoption proceeds when the previous host's state is missing"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = false): ss =>
                  Map(ss(0) -> offline)
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = writeClosedStore(deviceDir, stores.head)
      host = newHost(hostRoot)
      _ <- resolve(host)
      st <- state(deviceA)
      newHostState <- client.getHostState(HostId.BootstrapHostId)
    yield
      st.status should be(DeviceStatus.Operational)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(stores.head).status should be(StoreStatus.Active)
      newHostState.storageDevices should contain(deviceA)
      host.loadedDevices.contains(deviceA) should be(true)

  atest("a Shutdown device back on its own host is not loaded until restarted"):
    given ExecutionContext = executionContext
    val me = HostId.BootstrapHostId
    for
      stores <- setUpMetadata(me, DeviceStatus.Shutdown, 1, registerOldHost = false): ss =>
                  Map(ss(0) -> offline)
      s0 = stores.head
      (hostRoot, deviceDir) = writeCarriedDevice(me, me)
      d0 = writeClosedStore(deviceDir, s0)
      host = newHost(hostRoot)
      _ <- resolve(host)
      loadedWhileShutdown = host.loadedDevices.contains(deviceA)
      warningsWhileShutdown = host.deviceDirWarnings.count(_._1 == deviceDir)
      _ <- client.restartStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = net.takeCapturedGossip()
      _ = host.testingOnlyHandleHostMessage(CheckStorageDevice(me, client.clientId, deviceA))
      _ <- resolve(host)
      st <- state(deviceA)
    yield
      loadedWhileShutdown should be(false)
      warningsWhileShutdown should be(0)
      host.adoptCalls.toList should be(Nil)
      host.loadedDevices.contains(deviceA) should be(true)
      st.status should be(DeviceStatus.Operational)
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      os.exists(marker(d0)) should be(false)
      host.storeLoadAttempts.map(_._2) should contain(d0)
      storeMoves().map(_.storeId) should be(List(s0))

  atest("a host restarted mid-shutdown finishes it"):
    given ExecutionContext = executionContext
    val me = HostId.BootstrapHostId
    for
      stores <- setUpMetadata(me, DeviceStatus.ShuttingDown, 1, registerOldHost = false): ss =>
                  Map(ss(0) -> offline)
      (hostRoot, deviceDir) = writeCarriedDevice(me, me)
      _ = writeClosedStore(deviceDir, stores.head)
      host = newHost(hostRoot)
      _ <- resolve(host)
      loadedAfterResolve = host.loadedDevices.contains(deviceA)
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host.shutdownOutcomes.nonEmpty && !host.loadedDevices.contains(deviceA))
      st <- state(deviceA)
    yield
      loadedAfterResolve should be(true)
      host.shutdownOutcomes.toList should contain(Host.ShutdownOutcome.Completed)
      st.status should be(DeviceStatus.Shutdown)
      DeviceMarkers.readShutdownState(deviceDir) should be(Some(me))

  atest("a store that transferred in during the shutdown is adopted and then loaded"):
    given ExecutionContext = executionContext
    for
      // PoolRepointedOffline: the arrived copy sits behind its transferred-in marker, with no
      // transfer-out marker or CRL file of its own.
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> offline)
      List(s0, s1) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = writeClosedStore(deviceDir, s0)
      d1 = deviceDir / s1.directoryName
      _ = os.makeDir.all(d1)
      _ = os.write.over(d1 / StoreTransfer.TransferredInMarkerFile, "")
      host = newHost(hostRoot)
      _ <- resolve(host)
      st <- state(deviceA)
      markerKeptByAdoption = os.exists(d1 / StoreTransfer.TransferredInMarkerFile)
      attemptsAfterAdoption = host.storeLoadAttempts.map(_._2).toList
      // RecordingHost's tryLoadStore does not record the skipped store offline, as the real one
      // does on seeing the marker; done by hand so the completed-transfers-in pass sees it.
      _ = host.loadedDevices(deviceA).offlineStores += s1
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(!os.exists(d1 / StoreTransfer.TransferredInMarkerFile) &&
                     host.testingOnlyActiveDeviceChecks.isEmpty)
    yield
      host.adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.stores(s1).status should be(StoreStatus.Active)
      markerKeptByAdoption should be(true)
      attemptsAfterAdoption.count(_ == d1) should be(1)
      os.exists(d1 / StoreTransfer.TransferredInMarkerFile) should be(false)
      host.storeLoadAttempts.map(_._2).count(_ == d1) should be(2)

  atest("a store rebuilt after its device was shut down and unloaded is adopted"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Operational, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> rebuilding)
      s0 = stores.head
      // The old host loads the device, starts the rebuild, and is shut down while it runs.
      oldRoot = newHostDir()
      oldDir = writeDevice(oldRoot, "dev0", deviceA, owner = Some(oldHostId))
      oldHost = newHost(oldRoot, storeRebuildFactory = rebuilds, hostId = oldHostId)
      _ = oldHost.testingOnlyCheckAllDevices()
      _ <- pumpUntil(rebuilds.created.nonEmpty && oldHost.testingOnlyActiveDeviceChecks.isEmpty)
      // The pool lives on deviceA alone, so the real request would refuse this shutdown.
      _ <- forceShuttingDown(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = oldHost.testingOnlyCheckAllDevices()
      // The rebuild ignores its cancel, so the shutdown proceeds only once its wait times out.
      _ <- pumpUntil(oldHost.shutdownWorkTimeouts.nonEmpty)
      _ = oldHost.fireShutdownWorkTimeouts()
      _ <- pumpUntil(!oldHost.loadedDevices.contains(deviceA))
      // The rebuild, past its last read, lands its store and finishes on the unloaded device.
      d0Old = oldDir / s0.directoryName
      _ = os.makeDir.all(d0Old)
      _ = rebuilds.created.head.promise.success(())
      _ <- pumpUntil(os.exists(Backend.crlSaveFile(d0Old)))
      entryAfterRebuild <- state(deviceA).map(_.stores(s0))
      markerWritten = os.exists(marker(d0Old))
      crlWritten = os.exists(Backend.crlSaveFile(d0Old))
      // Carried to the new host.
      newRoot = newHostDir()
      newDir = StorageDeviceManager.deviceDirectory(newRoot, "dev0")
      _ = os.move(oldDir, newDir)
      host = newHost(newRoot)
      _ <- resolve(host)
      st <- state(deviceA)
    yield
      entryAfterRebuild.status should be(StoreStatus.Offline)
      markerWritten should be(true)
      crlWritten should be(true)
      host.adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should be(StoreStatus.Active)
      os.exists(marker(newDir / s0.directoryName)) should be(false)
      host.storeLoadAttempts.map(_._2) should contain(newDir / s0.directoryName)

  private val destHostId = HostId(UUID.fromString("0a0a0a0a-0000-0000-0000-000000000002"))

  private def startTransfers(): List[StartStoreTransfer] =
    net.takeCapturedHostMessages().collect { case m: StartStoreTransfer => m }

  /** deviceA, owned by the old host, sending its one store to deviceB on a third host whose
   *  RecordingHost (`dest`) has already asked the old host for it. deviceA is then shut down and
   *  carried to the adopting RecordingHost (`host`), which adopts it. Returns the store, the
   *  first request, and both hosts. */
  private def adoptTransferSource(transfers: StagedTransferService,
                                  sends: StagedSendService)
      : Future[(StoreId, StartStoreTransfer, RecordingHost, RecordingHost)] =
    given ExecutionContext = executionContext
    val transferId = UUID.randomUUID()
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Operational, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> StoreEntry(StoreStatus.TransferringOut, Some(deviceB), Some(transferId)))
      s0 = stores.head
      _ <- client.createHost("dest-host", "10.0.0.10", 5760, 5761, 5762, destHostId)
      _ <- net.createDevice(deviceB)
      _ <- setDeviceState(deviceB)(_.copy(hostId = destHostId,
             stores = Map(s0 -> StoreEntry(StoreStatus.TransferringIn, Some(deviceA), Some(transferId)))))
      _ <- waitForTransactionsToComplete()
      destRoot = newHostDir()
      _ = writeDevice(destRoot, "dev0", deviceB, owner = Some(destHostId))
      dest = newHost(destRoot, transferService = transfers, hostId = destHostId)
      _ = net.takeCapturedHostMessages()
      _ = dest.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.contains(s0))
      firstAsk = startTransfers()
      _ <- setDeviceState(deviceA)(_.copy(status = DeviceStatus.Shutdown))
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      // The transfer's own marker, as the old host's transfer out left it.
      _ = writeClosedStore(deviceDir, s0)
      host = newHost(hostRoot, transferService = sends)
      _ <- resolve(host)
      _ = net.takeCapturedHostMessages()
    yield (s0, firstAsk.head, dest, host)

  /** Hands `ask` to `host` as its event loop would and waits for the send to begin. */
  private def deliver(host: RecordingHost, sends: StagedSendService, ask: StartStoreTransfer): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- host.testingOnlyStartStoreTransferOut(ask)
      _ <- pumpUntil(sends.transfersOut.nonEmpty)
    yield ()

  atest("a retransmitted transfer request follows its source device to the adopting host"):
    given ExecutionContext = executionContext
    val transfers = new StagedTransferService
    val sends = new StagedSendService
    for
      (s0, firstAsk, dest, host) <- adoptTransferSource(transfers, sends)
      // The re-send goes out to the address held, then again once the source is re-read.
      _ = dest.retransmitPendingStartTransfers(force = true)
      _ <- pumpUntil(net.capturedHostMessages.exists:
             case m: StartStoreTransfer => m.toHost == HostId.BootstrapHostId
             case _ => false)
      resent = startTransfers()
      _ <- deliver(host, sends, resent.last)
    yield
      firstAsk.toHost should be(oldHostId)
      resent.map(_.toHost) should be(List(oldHostId, HostId.BootstrapHostId))
      resent.last.transferUUID should be(firstAsk.transferUUID)
      sends.transfersOut.map(_._1) should be(Vector(s0))

  atest("a retried transfer in asks the host that has since adopted its source"):
    given ExecutionContext = executionContext
    val transfers = new StagedTransferService
    val sends = new StagedSendService
    for
      (s0, firstAsk, dest, host) <- adoptTransferSource(transfers, sends)
      _ = transfers.failTransferIn(s0)
      _ <- pumpUntil(dest.testingOnlyScheduledTransferInRetries.contains(s0))
      _ = dest.testingOnlyRetryTransferIns()
      _ <- pumpUntil(net.capturedHostMessages.exists(_.isInstanceOf[StartStoreTransfer]))
      retried = startTransfers()
      _ <- deliver(host, sends, retried.head)
    yield
      firstAsk.toHost should be(oldHostId)
      retried.map(_.toHost) should be(List(HostId.BootstrapHostId))
      sends.transfersOut.map(_._1) should be(Vector(s0))

  /** A carried deviceA with Offline entries for `stores`, found by a scan whose lookup returns
   *  the state as it was before the last of them was added: the adoption must act on the state
   *  it re-reads, not on that one. `closeLast` decides whether the last store's directory was
   *  closed cleanly. */
  private def adoptWithStaleLookup(closeLast: Boolean): Future[(RecordingHost, os.Path, List[StoreId])] =
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> offline)
      List(s0, s1) = stores: @unchecked
      current <- state(deviceA)
      hostRoot = newHostDir()
      host = newHost(hostRoot)
      deviceDir = writeDevice(hostRoot, "dev0", deviceA, owner = Some(oldHostId))
      _ = DeviceMarkers.writeShutdownState(deviceDir, oldHostId)
      _ = writeClosedStore(deviceDir, s0)
      _ = if closeLast then writeClosedStore(deviceDir, s1)
      lookup = host.armLookup(deviceA)
      _ = net.takeCapturedGossip()
      _ = host.testingOnlyScanDevices()
      _ = lookup.success(current.copy(stores = current.stores - s1))
      _ <- resolve(host)
    yield (host, deviceDir, stores)

  atest("adoption re-validates the Offline stores of the state it re-reads"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir, stores) <- adoptWithStaleLookup(closeLast = false)
      st <- state(deviceA)
    yield
      host.adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Shutdown)
      st.hostId should be(oldHostId)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Offline))
      host.loadedDevices.contains(deviceA) should be(false)
      host.deviceDirWarnings.count(_._1 == deviceDir) should be(1)
      DeviceMarkers.readShutdownState(deviceDir) should be(Some(oldHostId))

  atest("adoption brings into service the stores of the state it committed"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir, stores) <- adoptWithStaleLookup(closeLast = true)
      st <- state(deviceA)
    yield
      st.status should be(DeviceStatus.Operational)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Active))
      host.loadedDevices.contains(deviceA) should be(true)
      stores.map(s => os.exists(marker(deviceDir / s.directoryName))) should be(List(false, false))
      storeMoves().map(_.storeId).toSet should be(stores.toSet)

  atest("adopting a device holding bootstrap-pool stores rewrites the bootstrap config for them only"):
    given ExecutionContext = executionContext
    val kept = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    val moved = List(StoreId(PoolId.BootstrapPoolId, 1.toByte), StoreId(PoolId.BootstrapPoolId, 2.toByte))
    def parse(yaml: String) = BootstrapConfig.parseBootstrapConfig(yaml)
    def storesOn(cfg: BootstrapConfig.Config, h: HostId) = cfg.hosts.find(_.hostId == h).map(_.stores.toSet)
    for
      _ <- client.createHost("old-host", "10.0.0.9", 5750, 5751, 5752, oldHostId)
      _ <- net.createDevice(deviceA)
      // Two of the bootstrap pool's three stores live on deviceA under the old host, and the
      // radicle's bootstrap config says so, as the transfers that put them there would have left it.
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             for
               ptr <- client.getStoragePoolPointer(PoolId.BootstrapPoolId)
               kvos <- client.read(ptr)
               cfg = StoragePoolState(kvos)
               _ = moved.foreach(s => cfg.stores(s.poolIndex) = StoragePoolState.StoreEntry(oldHostId, deviceA))
               _ <- BootstrapConfig.prepRadicleUpdate(client, moved.head, cfg, oldHostId)
             yield
               tx.update(ptr, None, None,
                 List(KeyRevision(StoragePoolState.ConfigKey, kvos.contents(StoragePoolState.ConfigKey).revision)),
                 List(Insert(StoragePoolState.ConfigKey, cfg.encode())))
      _ <- setDeviceState(deviceA)(_.copy(hostId = oldHostId, status = DeviceStatus.Shutdown,
                                          stores = moved.map(_ -> offline).toMap))
      _ <- addToHost(oldHostId, deviceA)
      _ <- waitForTransactionsToComplete()
      before <- client.getBootstrapConfig().map(parse)
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = moved.foreach(writeClosedStore(deviceDir, _))
      host = newHost(hostRoot)
      _ <- resolve(host)
      st <- state(deviceA)
      after <- client.getBootstrapConfig().map(parse)
      pool <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      st.status should be(DeviceStatus.Operational)
      storesOn(before, oldHostId) should be(Some(moved.toSet))
      storesOn(before, HostId.BootstrapHostId) should be(Some(Set(kept)))
      // Both moved stores now name the adopting host; the old host, left with none, is gone.
      storesOn(after, HostId.BootstrapHostId) should be(Some(Set(kept) ++ moved))
      after.hosts.map(_.hostId) should be(List(HostId.BootstrapHostId))
      // Nothing else changed: the adopting host's own entry, the IDA, the system id, the CA.
      after.hosts.head.copy(stores = Nil) should be(
        before.hosts.find(_.hostId == HostId.BootstrapHostId).get.copy(stores = Nil))
      after.bootstrapIDA should be(before.bootstrapIDA)
      after.aspenSystemId should be(before.aspenSystemId)
      after.caCertificate should be(before.caCertificate)
      pool.stores.toList.map(_.hostId) should be(List.fill(3)(HostId.BootstrapHostId))
      pool.stores(kept.poolIndex).storageDeviceId should be(StorageDeviceId.BootstrapStorageDeviceId)
