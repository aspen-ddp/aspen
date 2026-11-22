package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.host.{CopyFixtures, Phase}
import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.server.host.{ControlFixture, DeviceMetadata, LocalStorageDeviceState, StagedSendService, StagedTransferService, StoreLoader}
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
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBConfig}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Restart of a shut-down device on the host that shut it down, and adoption of one by another
 *  host. The adopting host is always the one ControlFixture's newHost builds (BootstrapHostId,
 *  with RecordingSeams); the "old host" is a second host registered with client.createHost. */
class DeviceAdoptionSuite extends IntegrationTestSuite with ControlFixture:
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
  private def resolve(host: Host): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- pumpUntil(queuedMarkerEvents(host) > 0)
      _ = handleMarkerEvents(host)
      _ <- pumpUntil(host.control.discovery.pendingMarkerResolutions.isEmpty &&
                     host.control.reconciler.activeDeviceChecks.isEmpty)
    yield ()

  /** Pumps until `condition` holds or `deadline` (System.nanoTime) passes, waiting in real time between rounds for
    * work the client schedules on its background timer, such as a retry's backoff */
  private def pollUntil(deadline: Long)(condition: => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    pumpUntil(condition, 1).flatMap: _ =>
      if condition || System.nanoTime() > deadline then Future.unit
      else
        val p = scala.concurrent.Promise[Unit]()
        client.backgroundTaskManager.schedule(scala.concurrent.duration.Duration(10, "ms"))(p.success(()))
        p.future.flatMap(_ => pollUntil(deadline)(condition))

  private def scanAndResolve(host: Host): Future[Unit] =
    host.control.reconciler.scan()
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

  /** A store directory as the shutdown leaves it: plain (its config only), its state in the device's CRL. */
  private def writeClosedStore(deviceDir: os.Path, storeId: StoreId): os.Path =
    writeStoreConfig(deviceDir, storeId)

  /** A store directory as a network transfer out leaves it: a transfer-out marker and a CRL save file. */
  private def writeTransferringOutStore(deviceDir: os.Path, storeId: StoreId): os.Path =
    val dir = CopyFixtures.placeCopy(deviceDir, storeId, Phase.Outbound(""))
    CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState.Empty, Backend.crlSaveFile(dir))
    dir

  /** Whether the copy of `storeId` on the device at `deviceDir` is Outbound (its transfer-out marker) */
  private def outbound(deviceDir: os.Path, storeId: StoreId): Boolean =
    CopyFixtures.phaseOf(deviceDir, storeId).exists(_.isInstanceOf[Phase.Outbound])

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

  /** The device directory as carried to the Host's root: `owning-host` and
   *  `shutdown.state` naming `markerHost` (owning-host `owningHost`). */
  private def writeCarriedDevice(markerHost: HostId,
                                 owningHost: HostId): (os.Path, os.Path) =
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA, owner = Some(owningHost))
    // The CRL its stores' state lives in, which every carried device that holds stores has
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
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
      d2 = writeTransferringOutStore(deviceDir, s2)
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
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
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
      outbound(deviceDir, s0) should be(false)
      outbound(deviceDir, s2) should be(true)
      // Adopted from another host, whose clock may lead this one's: their load floors take the cross-host margin
      List(d0, d2).foreach(d => os.exists(d / StoreLoader.ArrivedAtFile) should be(true))
      loadedDevices(host).contains(deviceA) should be(true)
      recorded(host).storeLoadAttempts.map(_._2) should contain(d0)
      val moves = storeMoves()
      moves.map(_.storeId) should be(List(s0))
      moves.head.hostId should be(HostId.BootstrapHostId)

  atest("an adoption whose commit is reported Unknown is still brought into service (Todo 5.10)"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> offline)
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeClosedStore(deviceDir, stores.head)
      devPtr <- client.getStorageDevicePointer(deviceA)
      // The adoption is the only transaction that writes deviceA from here on
      _ = net.reportCommitAsUnknown(_.allReferencedObjectsSet.exists(_.id == devPtr.id))
      host = newHost(hostRoot)
      _ <- resolve(host)
      // The retry follows the Unknown after the client's real-time backoff
      _ <- pollUntil(System.nanoTime() + 10_000_000_000L)(loadedDevices(host).contains(deviceA))
      st <- state(deviceA)
    yield
      net.pendingUnknownCommits should be(0)
      st.hostId should be(HostId.BootstrapHostId)
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(HostId.BootstrapHostId))
      loadedDevices(host).contains(deviceA) should be(true)
      recorded(host).storeLoadAttempts.map(_._2) should contain(d0)

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

  /** Adopts a carried deviceA with two Offline stores, s0 left by `leave` and s1 closed by the shutdown, and checks
   *  that s0 is adopted as Rebuilding and its directory discarded before the rebuild, and s1 adopted as usual. */
  private def adoptWithLostStore(leave: (os.Path, StoreId) => Unit): Future[org.scalatest.Assertion] =
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> offline)
      List(s0, s1) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeClosedStore(deviceDir, s0)
      d1 = writeClosedStore(deviceDir, s1)
      _ = leave(d0, s0)
      host = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ <- resolve(host)
      _ <- pumpUntil(rebuilds.created.nonEmpty)
      st <- state(deviceA)
      e0 <- poolEntry(s0)
    yield
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should be(StoreStatus.Rebuilding)
      // A transition into Rebuilding is a new rebuild episode
      st.stores(s0).rebuildEpisode should not be(None)
      // The rebuild runs for that episode
      rebuilds.created.map(r => r.storeId -> r.episode).toList should be(List(s0 -> st.stores(s0).rebuildEpisode))
      st.stores(s1).status should be(StoreStatus.Active)
      e0.hostId should be(HostId.BootstrapHostId)
      loadedDevices(host).contains(deviceA) should be(true)
      rebuilds.created.map(_.storeId).toList should be(List(s0))
      // Discarded before the rebuild began, so the rebuild cannot take it for a store in place.
      os.exists(d0) should be(false)
      recorded(host).storeLoadAttempts.map(_._2) should contain(d1)

  // Marked by the host that closed it (a backend that failed during its close, StoreRebuilds.failedOnClose): its
  // state cannot be trusted whatever else its directory holds.
  atest("a store marked rebuild-required is adopted as Rebuilding"):
    adoptWithLostStore((dir, s) => CopyFixtures.markCopy(dir / os.up, s, Phase.Lost))

  // A network transfer out extracted the store's state from the device CRL into its save file. A save file missing
  // or unusable now (a disk pulled before it reached the platter, or a file copied from another store) leaves no
  // copy of that state anywhere: refusing the device would not bring it back.
  for (what, corrupt) <- List[(String, (os.Path, StoreId) => Unit)](
        ("no", (file, _) => os.remove(file)),
        ("a truncated", (file, _) => os.write.over(file, "")),
        ("another store's", (file, s) =>
          CrashRecoveryLog.saveStoreState(StoreId(s.poolId, (s.poolIndex + 1).toByte), StoreRecoveryState.Empty, file))) do
    atest(s"a store whose network transfer left transferring-out with $what save file is adopted as Rebuilding"):
      adoptWithLostStore: (dir, s) =>
        writeTransferringOutStore(dir / os.up, s)
        corrupt(Backend.crlSaveFile(dir), s)

  // The transferred-in marker no longer outranks the transfer-out one: a copy carrying both is Outbound, so its save
  // file must be valid (it was adopted as Clean whatever its save file before CopyPhase)
  atest("a store carrying transferring-out and transferred-in with a truncated save file is adopted as Rebuilding"):
    adoptWithLostStore: (dir, s) =>
      writeTransferringOutStore(dir / os.up, s)
      os.write.over(dir / StoreTransfer.TransferredInMarkerFile, "t1")
      os.write.over(Backend.crlSaveFile(dir), "")

  atest("a store carrying transferring-out and transferred-in with a valid save file is adopted as Clean"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> offline)
      s0 = stores.head
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeTransferringOutStore(deviceDir, s0)
      _ = os.write.over(d0 / StoreTransfer.TransferredInMarkerFile, "t1")
      host = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ <- resolve(host)
      st <- state(deviceA)
    yield
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should not be(StoreStatus.Rebuilding)
      CopyFixtures.phaseOf(deviceDir, s0).contains(Phase.Lost) should be(false)
      rebuilds.created should be(empty)

  atest("a transferring-out store whose save file cannot be read fails the adoption for a later scan (SL-W18)"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> offline)
      s0 = stores.head
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeTransferringOutStore(deviceDir, s0)
      // Its stat succeeds and its open fails (EACCES): a read error that says nothing of its content
      _ = os.perms.set(Backend.crlSaveFile(d0), "---------")
      host = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ <- resolve(host)
      whileUnreadable <- state(deviceA)
      markedWhileUnreadable = CopyFixtures.phaseOf(deviceDir, s0).contains(Phase.Lost)
      _ = os.perms.set(Backend.crlSaveFile(d0), "rw-------")
      _ <- scanAndResolve(host)
      st <- state(deviceA)
    yield
      whileUnreadable.hostId should be(oldHostId)
      markedWhileUnreadable should be(false)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should not be(StoreStatus.Rebuilding)
      CopyFixtures.phaseOf(deviceDir, s0).contains(Phase.Lost) should be(false)
      rebuilds.created should be(empty)

  /** The device directory as pulled from its host without a shutdown: `owning-host` naming `owningHost`, and no
   *  `shutdown.state`. */
  private def writePulledDevice(owningHost: HostId): (os.Path, os.Path) =
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA, owner = Some(owningHost))
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
    (hostRoot, deviceDir)

  atest("a device pulled from its host without a shutdown is adopted"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Operational, 3, registerOldHost = true): ss =>
                  Map(ss(0) -> StoreEntry(StoreStatus.Active, None), ss(1) -> rebuilding,
                      ss(2) -> StoreEntry(StoreStatus.TransferringOut, Some(deviceB), Some(UUID.randomUUID())))
      List(s0, s1, s2) = stores: @unchecked
      (hostRoot, deviceDir) = writePulledDevice(oldHostId)
      d0 = writeClosedStore(deviceDir, s0)
      d2 = writeTransferringOutStore(deviceDir, s2)
      _ = net.takeCapturedGossip()
      _ = net.takeCapturedHostMessages()
      host = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ <- resolve(host)
      st <- state(deviceA)
      oldHostState <- client.getHostState(oldHostId)
      newHostState <- client.getHostState(HostId.BootstrapHostId)
      entries <- Future.sequence(stores.map(poolEntry))
    yield
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should be(StoreStatus.Active)
      st.stores(s1) should be(rebuilding)
      st.stores(s2).status should be(StoreStatus.TransferringOut)
      oldHostState.storageDevices should not contain deviceA
      newHostState.storageDevices should contain(deviceA)
      entries.map(_.hostId).toSet should be(Set(HostId.BootstrapHostId))
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(HostId.BootstrapHostId))
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      outbound(deviceDir, s2) should be(true)
      loadedDevices(host).contains(deviceA) should be(true)
      recorded(host).storeLoadAttempts.map(_._2) should contain(d0)
      storeMoves().map(_.storeId) should be(List(s0))
      net.takeCapturedHostMessages().collect { case m: CheckStorageDevice => m } should contain(
        CheckStorageDevice(oldHostId, client.clientId, deviceA))

  for withMarker <- List(true, false) do
    val how = if withMarker then "after writing shutdown.state" else "before writing shutdown.state"
    atest(s"a device pulled while shutting down, $how, is adopted"):
      given ExecutionContext = executionContext
      for
        // s0 was closed by the shutdown, s1 not yet: both have their state in the device CRL
        stores <- setUpMetadata(oldHostId, DeviceStatus.ShuttingDown, 2, registerOldHost = true): ss =>
                    Map(ss(0) -> offline, ss(1) -> offline)
        List(s0, s1) = stores: @unchecked
        (hostRoot, deviceDir) =
          if withMarker then writeCarriedDevice(oldHostId, oldHostId) else writePulledDevice(oldHostId)
        d0 = writeClosedStore(deviceDir, s0)
        d1 = writeClosedStore(deviceDir, s1)
        host = newHost(hostRoot)
        _ <- resolve(host)
        st <- state(deviceA)
      yield
        recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
        st.status should be(DeviceStatus.Operational)
        st.hostId should be(HostId.BootstrapHostId)
        st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Active))
        loadedDevices(host).contains(deviceA) should be(true)
        os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
        recorded(host).storeLoadAttempts.map(_._2) should contain allOf (d0, d1)

  atest("a pulled device whose store directory is missing is refused"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Operational, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> StoreEntry(StoreStatus.Active, None), ss(1) -> StoreEntry(StoreStatus.Active, None))
      (hostRoot, deviceDir) = writePulledDevice(oldHostId)
      _ = writeClosedStore(deviceDir, stores.head)
      host = newHost(hostRoot)
      _ <- resolve(host)
      _ <- scanAndResolve(host)
      st <- state(deviceA)
    yield
      loadedDevices(host).contains(deviceA) should be(false)
      recorded(host).adoptCalls.toList should be(Nil)
      recorded(host).deviceDirWarnings.count(_._1 == deviceDir) should be(1)
      st.hostId should be(oldHostId)
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(oldHostId))

  private val thirdHostId = HostId(UUID.fromString("0a0a0a0a-0000-0000-0000-000000000003"))

  for (what, status, owner) <- List(("declared failed", DeviceStatus.Failed, oldHostId),
                                    ("owned by a third host", DeviceStatus.Operational, thirdHostId)) do
    atest(s"a pulled device the tree records as $what is refused"):
      given ExecutionContext = executionContext
      for
        stores <- setUpMetadata(owner, status, 1, registerOldHost = false): ss =>
                    Map(ss(0) -> StoreEntry(StoreStatus.Active, None))
        (hostRoot, deviceDir) = writePulledDevice(oldHostId)
        _ = writeClosedStore(deviceDir, stores.head)
        host = newHost(hostRoot)
        _ <- resolve(host)
        _ <- scanAndResolve(host)
        st <- state(deviceA)
      yield
        loadedDevices(host).contains(deviceA) should be(false)
        recorded(host).adoptCalls.toList should be(Nil)
        recorded(host).deviceDirWarnings.count(_._1 == deviceDir) should be(1)
        st.hostId should be(owner)
        DeviceMarkers.readOwningHost(deviceDir) should be(Some(oldHostId))

  atest("a crash between a pulled device's adoption commit and its owning-host rewrite is finished on the next scan"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Operational, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> StoreEntry(StoreStatus.Active, None))
      s0 = stores.head
      _ <- setDeviceState(deviceA)(_.copy(hostId = HostId.BootstrapHostId))
      _ <- setPoolEntry(s0, HostId.BootstrapHostId, deviceA)
      (hostRoot, deviceDir) = writePulledDevice(oldHostId)
      d0 = writeClosedStore(deviceDir, s0)
      _ = net.takeCapturedGossip()
      host = newHost(hostRoot)
      _ <- resolve(host)
    yield
      recorded(host).adoptCalls.toList should be(Nil)
      loadedDevices(host).contains(deviceA) should be(true)
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(HostId.BootstrapHostId))
      recorded(host).storeLoadAttempts.map(_._2) should contain(d0)
      storeMoves().map(_.storeId) should be(List(s0))

  atest("an adoption whose store directory cannot be stat'd leaves the owner unwritten for the next scan (P-W10)"):
    // The store directory is a link into a directory this process cannot search once the lookup is out: its stat
    // fails (EACCES). Skipped, its arrival record would be missing when the rewritten owner loads it
    given ExecutionContext = executionContext
    val locked = os.temp.dir()
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Operational, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> StoreEntry(StoreStatus.Active, None))
      s0 = stores.head
      _ <- setDeviceState(deviceA)(_.copy(hostId = HostId.BootstrapHostId))
      _ <- setPoolEntry(s0, HostId.BootstrapHostId, deviceA)
      (hostRoot, deviceDir) = writePulledDevice(oldHostId)
      d0 = writeClosedStore(deviceDir, s0)
      _ = os.move(d0, locked / "store")
      _ = os.symlink(d0, locked / "store")
      host = newHost(hostRoot)
      _ <- pumpUntil(queuedMarkerEvents(host) > 0)
      _ = os.perms.set(locked, "rw-------")
      _ = handleMarkerEvents(host)
      _ <- pumpUntil(host.control.discovery.pendingMarkerResolutions.isEmpty &&
                     host.control.reconciler.activeDeviceChecks.isEmpty)
      ownerWhileUnknown = DeviceMarkers.readOwningHost(deviceDir)
      loadedWhileUnknown = loadedDevices(host).contains(deviceA)
      _ = os.perms.set(locked, "rwx------")
      _ <- scanAndResolve(host)
    yield
      ownerWhileUnknown should be(Some(oldHostId))
      loadedWhileUnknown should be(false)
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(HostId.BootstrapHostId))
      os.exists(d0 / StoreLoader.ArrivedAtFile) should be(true)
      loadedDevices(host).contains(deviceA) should be(true)

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
      recorded(host).adoptCalls.toList should be(Nil)
      loadedDevices(host).contains(deviceA) should be(true)
      DeviceMarkers.readOwningHost(deviceDir) should be(Some(HostId.BootstrapHostId))
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      outbound(deviceDir, s0) should be(false)
      recorded(host).storeLoadAttempts.map(_._2) should contain(d0)
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
      loadedDevices(host).contains(deviceA) should be(true)

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
      loadedWhileShutdown = loadedDevices(host).contains(deviceA)
      warningsWhileShutdown = recorded(host).deviceDirWarnings.count(_._1 == deviceDir)
      _ <- client.restartStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = net.takeCapturedGossip()
      _ = host.control.reconciler.handleCheckMessage(CheckStorageDevice(me, client.clientId, deviceA))
      _ <- resolve(host)
      st <- state(deviceA)
    yield
      loadedWhileShutdown should be(false)
      warningsWhileShutdown should be(0)
      recorded(host).adoptCalls.toList should be(Nil)
      loadedDevices(host).contains(deviceA) should be(true)
      st.status should be(DeviceStatus.Operational)
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      outbound(deviceDir, s0) should be(false)
      // Back on the host that shut it down, one clock: the load floor takes only MaxStartLead
      os.exists(d0 / StoreLoader.ArrivedAtFile) should be(false)
      recorded(host).storeLoadAttempts.map(_._2) should contain(d0)
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
      loadedAfterResolve = loadedDevices(host).contains(deviceA)
      _ = host.control.reconciler.checkAll()
      _ <- pumpUntil(recorded(host).shutdownOutcomes.nonEmpty && !loadedDevices(host).contains(deviceA))
      st <- state(deviceA)
    yield
      loadedAfterResolve should be(true)
      recorded(host).shutdownOutcomes.toList should contain(DeviceMetadata.ShutdownOutcome.Completed)
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
      d1 = CopyFixtures.placeCopy(deviceDir, s1, Phase.Arrived(""))
      host = newHost(hostRoot)
      _ <- resolve(host)
      st <- state(deviceA)
      // Its copy, registered Arrived at the device's load, is offline there, so the completed-transfers-in pass of
      // the adoption's own reconcile, or of the check below, completes it
      _ = host.control.reconciler.checkAll()
      _ <- pumpUntil(CopyFixtures.phaseOf(deviceDir, s1).contains(Phase.Live) &&
                     host.control.reconciler.activeDeviceChecks.isEmpty)
    yield
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.stores(s1).status should be(StoreStatus.Active)
      CopyFixtures.phaseOf(deviceDir, s1) should be(Some(Phase.Live))
      recorded(host).storeLoadAttempts.map(_._2).count(_ == d1) should be(2)

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
      _ = oldHost.control.reconciler.checkAll()
      _ <- pumpUntil(rebuilds.created.nonEmpty && oldHost.control.reconciler.activeDeviceChecks.isEmpty)
      // The pool lives on deviceA alone, so the real request would refuse this shutdown.
      _ <- forceShuttingDown(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = oldHost.control.reconciler.checkAll()
      // The rebuild ignores its cancel, so the shutdown proceeds only once its wait times out.
      _ <- pumpUntil(recorded(oldHost).shutdownWorkTimeouts.nonEmpty)
      _ = recorded(oldHost).fireShutdownWorkTimeouts()
      _ <- pumpUntil(!loadedDevices(oldHost).contains(deviceA))
      // The rebuild, past its last read, lands its store and finishes on the unloaded device.
      d0Old = writeStoreConfig(oldDir, s0)
      _ = RebuildingStore.writeEmptyCrlState(d0Old, s0)
      _ = rebuilds.created.head.promise.success(())
      _ <- pumpUntil(outbound(oldDir, s0))
      entryAfterRebuild <- state(deviceA).map(_.stores(s0))
      markerWritten = outbound(oldDir, s0)
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
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should be(StoreStatus.Active)
      outbound(newDir, s0) should be(false)
      recorded(host).storeLoadAttempts.map(_._2) should contain(CopyFixtures.copyPath(newDir, s0).get)

  private val destHostId = HostId(UUID.fromString("0a0a0a0a-0000-0000-0000-000000000002"))

  private def startTransfers(): List[StartStoreTransfer] =
    net.takeCapturedHostMessages().collect { case m: StartStoreTransfer => m }

  /** deviceA, owned by the old host, sending its one store to deviceB on a third host whose
   *  Host (`dest`) has already asked the old host for it. deviceA is then shut down and
   *  carried to the adopting Host (`host`), which adopts it. Returns the store, the
   *  first request, and both hosts. */
  private def adoptTransferSource(transfers: StagedTransferService,
                                  sends: StagedSendService)
      : Future[(StoreId, StartStoreTransfer, Host, Host)] =
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
      _ = dest.control.reconciler.checkAll()
      _ <- pumpUntil(transfers.startedTransfersIn.contains(s0))
      firstAsk = startTransfers()
      _ <- setDeviceState(deviceA)(_.copy(status = DeviceStatus.Shutdown))
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      // The transfer's own marker, as the old host's transfer out left it.
      _ = writeTransferringOutStore(deviceDir, s0)
      host = newHost(hostRoot, transferService = sends)
      _ <- resolve(host)
      _ = net.takeCapturedHostMessages()
    yield (s0, firstAsk.head, dest, host)

  /** Hands `ask` to `host` as its event loop would and waits for the send to begin. */
  private def deliver(host: Host, sends: StagedSendService, ask: StartStoreTransfer): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- host.control.transfersOut.startOut(ask)
      _ <- pumpUntil(sends.transfersOut.nonEmpty)
    yield ()

  atest("a retransmitted transfer request follows its source device to the adopting host"):
    given ExecutionContext = executionContext
    val transfers = new StagedTransferService
    val sends = new StagedSendService
    for
      (s0, firstAsk, dest, host) <- adoptTransferSource(transfers, sends)
      // The re-send goes out to the address held, then again once the source is re-read.
      _ = dest.control.transfers.retransmitPendingStarts(force = true)
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
      _ <- pumpUntil(dest.control.transfers.transferInRetries.keySet.contains(s0))
      _ = retryTransferIns(dest)
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
  private def adoptWithStaleLookup(closeLast: Boolean): Future[(Host, os.Path, List[StoreId])] =
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> offline)
      List(s0, s1) = stores: @unchecked
      current <- state(deviceA)
      hostRoot = newHostDir()
      host = newHost(hostRoot)
      deviceDir = writeDevice(hostRoot, "dev0", deviceA, owner = Some(oldHostId))
      _ = os.makeDir.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
      _ = DeviceMarkers.writeShutdownState(deviceDir, oldHostId)
      _ = writeClosedStore(deviceDir, s0)
      _ = if closeLast then writeClosedStore(deviceDir, s1)
      lookup = recorded(host).armLookup(deviceA)
      _ = net.takeCapturedGossip()
      _ = host.control.reconciler.scan()
      _ = lookup.success(current.copy(stores = current.stores - s1))
      _ <- resolve(host)
    yield (host, deviceDir, stores)

  atest("adoption re-validates the Offline stores of the state it re-reads"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir, stores) <- adoptWithStaleLookup(closeLast = false)
      st <- state(deviceA)
    yield
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Shutdown)
      st.hostId should be(oldHostId)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Offline))
      loadedDevices(host).contains(deviceA) should be(false)
      recorded(host).deviceDirWarnings.count(_._1 == deviceDir) should be(1)
      DeviceMarkers.readShutdownState(deviceDir) should be(Some(oldHostId))

  atest("adoption brings into service the stores of the state it committed"):
    given ExecutionContext = executionContext
    for
      (host, deviceDir, stores) <- adoptWithStaleLookup(closeLast = true)
      st <- state(deviceA)
    yield
      st.status should be(DeviceStatus.Operational)
      st.stores.values.map(_.status).toSet should be(Set(StoreStatus.Active))
      loadedDevices(host).contains(deviceA) should be(true)
      stores.map(outbound(deviceDir, _)) should be(List(false, false))
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
