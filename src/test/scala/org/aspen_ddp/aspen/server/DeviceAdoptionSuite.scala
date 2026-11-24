package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.host.{AdoptionClass, CopyFixtures, CopyLayout, DeviceStateDisk, JobKind, LockstepThreading, Phase}
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

  /** pumpUntil with discovery's jobs and results run each round (runDiscovery) */
  private def pumpWithDiscovery(host: Host, condition: => Boolean, rounds: Int = 30): Future[Unit] =
    given ExecutionContext = executionContext
    runDiscovery(host)
    if condition || rounds == 0 then Future.unit
    else pumpUntil(condition, 1).flatMap(_ => pumpWithDiscovery(host, condition, rounds - 1))

  /** Waits for the shutdown-marker lookups issued by the last scan, handles their results, then
   *  waits for whatever those started (an adoption, the device check of a loaded device). */
  private def resolve(host: Host): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- pumpUntil(queuedMarkerEvents(host) > 0)
      _ = handleMarkerEvents(host)
      _ <- pumpUntil(host.control.discovery.pendingMarkerResolutions.isEmpty)
      // An adoption's commit brings the device into service, whose load is discovery's jobs
      _ = runDiscovery(host)
      _ <- pumpUntil(host.control.reconciler.activeDeviceChecks.isEmpty)
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
    scanNow(host)
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

  /** A store copy as a network transfer out leaves it: in outbound/, with a CRL save file. */
  private def writeTransferringOutStore(deviceDir: os.Path, storeId: StoreId, transferId: String = ""): os.Path =
    val dir = CopyFixtures.placeCopy(deviceDir, storeId, Phase.Outbound(transferId))
    CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState.Empty, Backend.crlSaveFile(dir))
    dir

  /** Moves the copy of `storeId` already on the device into outbound/, as a transfer out leaves it, with a valid CRL
   *  save file: returns its path there */
  private def markTransferringOut(deviceDir: os.Path, storeId: StoreId): os.Path =
    val dir = CopyFixtures.markCopy(deviceDir, storeId, Phase.Outbound(""))
    CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState.Empty, Backend.crlSaveFile(dir))
    dir

  /** Whether the copy of `storeId` on the device at `deviceDir` is Outbound (in outbound/) */
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
                            registerOldHost: Boolean,
                            device: StorageDeviceId = deviceA,
                            createOldHost: Option[Boolean] = None)
                           (entries: List[StoreId] => Map[StoreId, StoreEntry]): Future[List[StoreId]] =
    given ExecutionContext = executionContext
    for
      _ <-
        if createOldHost.getOrElse(registerOldHost) then
          client.createHost("old-host", "10.0.0.9", 5750, 5751, 5752, owner).map(_ => ())
        else
          Future.unit
      _ <- net.createDevice(device)
      // One at a time: pool creations write the same set and device objects, and would collide.
      stores <- runSequentially((0 until count).toList)(i => newPool(s"adopt-pool-${device.uuid}-$i")).map(_.toList)
      _ <- runSequentially(stores)(s => setPoolEntry(s, owner, device))
      _ <- setDeviceState(device)(_.copy(hostId = owner, status = status, stores = entries(stores)))
      _ <- if registerOldHost then addToHost(owner, device) else Future.unit
      _ <- waitForTransactionsToComplete()
    yield stores

  /** The device directory as carried to the Host's root: `owning-host` and
   *  `shutdown.state` naming `markerHost` (owning-host `owningHost`). */
  private def writeCarriedDevice(markerHost: HostId,
                                 owningHost: HostId,
                                 into: Option[os.Path] = None,
                                 name: String = "dev0",
                                 device: StorageDeviceId = deviceA): (os.Path, os.Path) =
    val hostRoot = into.getOrElse(newHostDir())
    val deviceDir = writeDevice(hostRoot, name, device, owner = Some(owningHost))
    // The CRL its stores' state lives in, which every carried device that holds stores has
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
    DeviceMarkers.writeShutdownState(deviceDir, markerHost)
    (hostRoot, deviceDir)

  private val oldHostId = HostId(UUID.fromString("0a0a0a0a-0000-0000-0000-000000000001"))

  // Device I/O isolation (Task 15): the ownership decision's recheck of the directory is one job on the device's lane,
  // the path pending meanwhile, so no scan resolves it again; it reads the disk as it is when it runs
  // ---- Two devices: each adoption step held on A while B's adoption completes (device I/O isolation, Task 15) ----

  /** Drains `host` (every event, in FIFO order: a held device's jobs stay held) and the network's transactions, until
   *  `condition` holds or `rounds` run out (silent: callers assert) */
  private def drainUntil(host: Host, condition: => Boolean, rounds: Int = 60): Future[Unit] =
    given ExecutionContext = executionContext
    host.testingOnlyHandleEvents()
    if condition || rounds == 0 then Future.unit
    else
      for
        _ <- waitForTransactionsToComplete()
        _ <- yieldUntil(condition, 5)
        _ <- drainUntil(host, condition, rounds - 1)
      yield ()

  /** drainUntil, waiting in real time between rounds for the client's retry backoff (a transaction that collided
   *  with the other device's adoption retries), until `condition` or 20 s; callers assert the end state */
  private def drainPolling(host: Host, condition: => Boolean): Future[Unit] =
    val deadline = System.nanoTime() + 20_000_000_000L
    given ExecutionContext = executionContext
    def loop(): Future[Unit] =
      drainUntil(host, condition, rounds = 2).flatMap: _ =>
        if condition || System.nanoTime() > deadline then Future.unit
        else
          val p = scala.concurrent.Promise[Unit]()
          client.backgroundTaskManager.schedule(scala.concurrent.duration.Duration(10, "ms"))(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  /** Two devices of the old host, Shutdown, carried to one adopting host's root: A (dev0) holding an Offline store
   *  and a CrlLost one (an outbound/ copy with no save file, which the adoption moves to lost/), B (dev1) an Offline
   *  store. Both lanes are held from the host's construction. Returns the host, both directories and A's CrlLost
   *  store */
  private def twoCarried(): Future[(Host, os.Path, os.Path, StoreId)] =
    given ExecutionContext = executionContext
    for
      aStores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                   Map(ss(0) -> offline, ss(1) -> offline)
      bStores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true, device = deviceB,
                               createOldHost = Some(false))(ss => Map(ss(0) -> offline))
      (hostRoot, devA) = writeCarriedDevice(oldHostId, oldHostId)
      (_, devB) = writeCarriedDevice(oldHostId, oldHostId, into = Some(hostRoot), name = "dev1", device = deviceB)
      _ = writeClosedStore(devA, aStores(0))
      _ = CopyFixtures.placeCopy(devA, aStores(1), Phase.Outbound(""))
      _ = writeClosedStore(devB, bStores(0))
      host = newHost(hostRoot, threadingOf = ec =>
               val t = new LockstepThreading(ec)
               t.hold(devA)
               t.hold(devB)
               t)
    yield (host, devA, devB, aStores(1))

  /** Lets A's held jobs run one at a time until the `nth` job of `kind` is the first held, the others draining */
  private def advanceHeld(host: Host, dev: os.Path, kind: JobKind, nth: Int, rounds: Int = 80): Future[Unit] =
    given ExecutionContext = executionContext
    var passed = 0
    def loop(left: Int): Future[Unit] =
      host.testingOnlyHandleEvents()
      val held = heldJobs(host, dev)
      if (held.headOption.contains(kind) && passed == nth - 1) || left == 0 then
        Future.unit
      else
        if held.nonEmpty then
          passHeldJob(host, dev).foreach(k => if k == kind then passed += 1)
        drainUntil(host, false, rounds = 1).flatMap(_ => loop(left - 1))
    loop(rounds)

  for (step, kind, nth) <- List(("its ownership listing", JobKind.DeviceOwnership, 1),
                                ("its ownership recheck", JobKind.DeviceOwnership, 2),
                                ("its adoption look", JobKind.Adoption, 1),
                                ("its transaction's unclean classes", JobKind.Adoption, 2),
                                ("its transaction's CrlLost classes", JobKind.Adoption, 3),
                                ("its transaction's moves to lost/", JobKind.Adoption, 4),
                                ("its bringIntoService", JobKind.Adoption, 5))
  do
    atest(s"an adoption held at $step on one device waits while another device's adoption completes"):
      given ExecutionContext = executionContext
      for
        (host, devA, devB, lostOnA) <- twoCarried()
        _ = scanNow(host)
        // A to the step, B still held at its first job; then B's whole adoption runs while A waits there
        _ <- advanceHeld(host, devA, kind, nth)
        heldAt = heldJobs(host, devA).headOption
        _ = releaseDevice(host, devB)
        _ <- drainPolling(host, loadedDevices(host).contains(deviceB) &&
                                host.control.reconciler.activeDeviceChecks.isEmpty)
        bState <- state(deviceB)
        aWhile <- state(deviceA)
        aOwnerWhile = DeviceMarkers.readOwningHost(devA)
        aLoadedWhile = loadedDevices(host).contains(deviceA)
        _ = releaseDevice(host, devA)
        _ <- drainPolling(host, loadedDevices(host).contains(deviceA) &&
                                host.control.reconciler.activeDeviceChecks.isEmpty)
        aState <- state(deviceA)
      yield
        heldAt should be(Some(kind))
        // B, the same work on another device, completed: adopted, its owner rewritten, loaded
        bState.hostId should be(HostId.BootstrapHostId)
        DeviceMarkers.readOwningHost(devB) should be(Some(HostId.BootstrapHostId))
        os.exists(devB / DeviceMarkers.ShutdownStateFile) should be(false)
        // A waited at the held step: not loaded, its owner not rewritten; committed only once the transaction's last
        // disk step had run (bringIntoService follows the commit)
        aLoadedWhile should be(false)
        aOwnerWhile should be(Some(oldHostId))
        aWhile.hostId should be(if nth == 5 && kind == JobKind.Adoption then HostId.BootstrapHostId else oldHostId)
        // Released, A completes the same way, its CrlLost store moved to lost/ and adopted Rebuilding
        aState.hostId should be(HostId.BootstrapHostId)
        aState.stores(lostOnA).status should be(StoreStatus.Rebuilding)
        CopyFixtures.phaseOf(devA, lostOnA) should be(Some(Phase.Lost))
        DeviceMarkers.readOwningHost(devA) should be(Some(HostId.BootstrapHostId))
        os.exists(devA / DeviceMarkers.ShutdownStateFile) should be(false)
        loadedDevices(host).contains(deviceA) should be(true)

  /** Runs `job` (a queued lane task of `host`) with the interleaving of a path leaving the listing while it runs:
   *  `devA` moved away, a listing run (its result queued), `devA` moved back, then `job` run, so the listing's result
   *  (which forgets the path's DeviceIo, idle by then) is applied before the job's */
  private def leaveListingWhile(host: Host, devA: os.Path)(isJob: Runnable => Boolean): Unit =
    val ls = host.threading.asInstanceOf[LockstepThreading]
    val away = devA / os.up / os.up / "away"
    os.move(devA, away)
    // A scan's listing, run now (devA absent from it), its result queued behind
    host.control.reconciler.scan()
    ls.removeQueued:
      case t: LockstepThreading.DeviceTask => t.kind == JobKind.DeviceListing
      case _ => false
    .foreach(_.run())
    os.move(away, devA)
    // The job, run on the restored directory: its result is queued after the listing's
    ls.removeQueued(isJob).foreach(_.run())

  for (step, nth) <- List(("its adoption look", 1), ("its bringIntoService", 5))
  do
    atest(s"an adoption job ($step) completing after its path left the listing changes nothing"):
      given ExecutionContext = executionContext
      for
        // B stays held throughout: no other adoption's transaction collides with A's
        (host, devA, _, _) <- twoCarried()
        _ = scanNow(host)
        _ <- advanceHeld(host, devA, JobKind.Adoption, nth)
        heldAt = heldJobs(host, devA)
        adoptsBefore = recorded(host).adoptCalls.toList
        // The held job goes to the FIFO; it runs only inside the interleaving
        _ = releaseDevice(host, devA)
        _ = leaveListingWhile(host, devA):
              case t: LockstepThreading.DeviceTask => t.kind == JobKind.Adoption
              case _ => false
        // The listing's result, then the job's, in FIFO order
        _ = host.testingOnlyHandleEvents()
        forgotten = host.control.deviceIos.get(devA).isEmpty
        pendingAfter = host.control.discovery.pendingMarkerResolutions
        reportedAfter = host.control.discovery.reportedDeviceDirs.contains(devA)
        loadedAfter = loadedDevices(host).contains(deviceA)
        adoptsAfter = recorded(host).adoptCalls.toList
        // The directory is back: a later scan decides afresh from what its disk says
        _ = scanNow(host)
        _ <- drainPolling(host, loadedDevices(host).contains(deviceA) &&
                                host.control.reconciler.activeDeviceChecks.isEmpty)
        aState <- state(deviceA)
      yield
        heldAt should be(List(JobKind.Adoption))
        // The job's DeviceIo was forgotten as the path left the listing: its result is taken as Cancelled
        // (laneJob), no warning, nothing adopted or loaded, the path no longer pending
        forgotten should be(true)
        reportedAfter should be(false)
        pendingAfter should not contain devA
        loadedAfter should be(false)
        adoptsAfter should be(adoptsBefore)
        // The look's: no transaction began. bringIntoService's: its disk steps ran, so the later scan finds the
        // device owned here and loads it as its own; the look's later scan adopts it
        aState.hostId should be(HostId.BootstrapHostId)
        loadedDevices(host).contains(deviceA) should be(true)

  atest("an ownership recheck waits on a held device, then adopts it"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true)(ss => Map(ss(0) -> offline))
      List(s0) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = writeClosedStore(deviceDir, s0)
      host = newHost(hostRoot)
      _ <- pumpUntil(queuedMarkerEvents(host) > 0)
      _ = holdDevice(host, deviceDir)
      _ = handleMarkerEvents(host)
      _ <- pumpUntil(false, rounds = 2)
      heldWhile = heldJobs(host, deviceDir)
      pendingWhile = host.control.discovery.pendingMarkerResolutions
      adoptsWhile = recorded(host).adoptCalls.toList
      _ = scanNow(host)
      heldAfterScan = heldJobs(host, deviceDir)
      _ = releaseDevice(host, deviceDir)
      // The scan's probe results are discovery's: run with the pumps
      _ <- pumpWithDiscovery(host, loadedDevices(host).contains(deviceA) &&
                                   host.control.reconciler.activeDeviceChecks.isEmpty)
      st <- state(deviceA)
    yield
      heldWhile should be(List(JobKind.DeviceOwnership))
      pendingWhile should be(Set(deviceDir))
      adoptsWhile should be(Nil)
      // A scan meanwhile starts no second resolution (its probe of the path waits behind the recheck)
      heldAfterScan.count(_ == JobKind.DeviceOwnership) should be(1)
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.hostId should be(HostId.BootstrapHostId)
      loadedDevices(host).contains(deviceA) should be(true)

  atest("an ownership recheck reads the directory as it is when its held job runs"):
    given ExecutionContext = executionContext
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true)(ss => Map(ss(0) -> offline))
      List(s0) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = writeClosedStore(deviceDir, s0)
      host = newHost(hostRoot)
      _ <- pumpUntil(queuedMarkerEvents(host) > 0)
      _ = holdDevice(host, deviceDir)
      _ = handleMarkerEvents(host)
      // The marker the lookup was made for changes while the recheck waits
      _ = DeviceMarkers.writeShutdownState(deviceDir, HostId(UUID.randomUUID()))
      _ = releaseDevice(host, deviceDir)
      _ <- pumpUntil(host.control.discovery.pendingMarkerResolutions.isEmpty)
      _ = runDiscovery(host)
      _ <- pumpUntil(false, rounds = 2)
      st <- state(deviceA)
    yield
      // Nothing adopted: the next scan decides afresh from what the disk says
      recorded(host).adoptCalls.toList should be(Nil)
      st.hostId should be(oldHostId)
      loadedDevices(host).contains(deviceA) should be(false)

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
      // A transfer out interrupted by the shutdown: its copy in outbound/, which adoption must leave.
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
      // Its load, once the retry has committed, is discovery's jobs
      _ <- pollUntil(System.nanoTime() + 10_000_000_000L):
             runDiscovery(host)
             loadedDevices(host).contains(deviceA)
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

  // Moved to lost/ by the host that closed it (a backend that failed during its close, StoreRebuilds.failedOnClose):
  // its state cannot be trusted whatever else its directory holds.
  atest("a store in lost/ is adopted as Rebuilding"):
    adoptWithLostStore((dir, s) => CopyFixtures.markCopy(dir / os.up / os.up, s, Phase.Lost))

  // A network transfer out extracted the store's state from the device CRL into its save file. A save file missing
  // or unusable now (a disk pulled before it reached the platter, or a file copied from another store) leaves no
  // copy of that state anywhere: refusing the device would not bring it back.
  for (what, corrupt) <- List[(String, (os.Path, StoreId) => Unit)](
        ("no", (file, _) => os.remove(file)),
        ("a truncated", (file, _) => os.write.over(file, "")),
        ("another store's", (file, s) =>
          CrashRecoveryLog.saveStoreState(StoreId(s.poolId, (s.poolIndex + 1).toByte), StoreRecoveryState.Empty, file))) do
    atest(s"a store whose network transfer left it in outbound/ with $what save file is adopted as Rebuilding"):
      adoptWithLostStore: (dir, s) =>
        corrupt(Backend.crlSaveFile(markTransferringOut(dir / os.up / os.up, s)), s)

  // Stage 2 Task 3: an outbound/ copy is classed by its location and its save file; a Clean one whose entry the
  // adoption makes Active is reinstated (T7), the CrlLost one moved to lost/ (T10) and retired before its rebuild
  atest("an adopted outbound copy is Clean only with a valid save file"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    @volatile var renames = Vector.empty[(os.Path, os.Path)]
    val rename: (os.Path, os.Path) => Unit = (from, to) =>
      renames :+= (from, to)
      org.aspen_ddp.aspen.server.DurableFiles.atomicRename(from, to)
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> offline)
      List(s0, s1) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeTransferringOutStore(deviceDir, s0)
      d1 = writeTransferringOutStore(deviceDir, s1)
      _ = os.remove(Backend.crlSaveFile(d1))
      before <- state(deviceA)
      host = newHost(hostRoot, storeRebuildFactory = rebuilds, adjustSeams = _.copy(copyRename = Some(rename)))
      clean = DeviceStateDisk.servedStoresIn(deviceDir, before, AdoptionClass.Clean)
      crlLost = DeviceStateDisk.servedStoresIn(deviceDir, before, AdoptionClass.CrlLost)
      _ <- resolve(host)
      _ <- pumpUntil(rebuilds.created.nonEmpty)
      st <- state(deviceA)
    yield
      clean should be(List(s0))
      crlLost should be(List(s1))
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should be(StoreStatus.Active)
      st.stores(s1).status should be(StoreStatus.Rebuilding)
      // s0 is reinstated to Live, and loaded
      val flat0 = CopyFixtures.livePathOf(deviceDir, s0)
      CopyFixtures.phaseOf(deviceDir, s0) should be(Some(Phase.Live))
      CopyFixtures.copyPath(deviceDir, s0) should be(Some(flat0))
      recorded(host).storeLoadAttempts.map(_._2) should contain(flat0)
      // s1 went from outbound/ to lost/ under its copy name, then to pending-delete/ for its rebuild
      val lostName = d1.last
      renames.filter(r => r._1 == d1 || r._1.last == lostName) should be(Vector(
        d1 -> deviceDir / "lost" / lostName,
        deviceDir / "lost" / lostName -> deviceDir / RebuildingStore.PendingDeleteDirectory / lostName))
      renames.filter(_._1 == d0) should be(Vector(d0 -> flat0))
      rebuilds.created.map(_.storeId).toList should be(List(s1))

  // An arrived phase-param inside an outbound/ copy is inert (its location is its phase): it is Outbound, so its save
  // file must be valid
  atest("an outbound copy carrying an arrived phase-param with a truncated save file is adopted as Rebuilding"):
    adoptWithLostStore: (dir, s) =>
      val outbound = markTransferringOut(dir / os.up / os.up, s)
      os.write.over(outbound / CopyLayout.paramFile(Phase.Arrived("")).get, "t1")
      os.write.over(Backend.crlSaveFile(outbound), "")

  atest("an outbound copy carrying an arrived phase-param with a valid save file is adopted as Clean"):
    given ExecutionContext = executionContext
    val rebuilds = new RecordingRebuildFactory
    for
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 1, registerOldHost = true): ss =>
                  Map(ss(0) -> offline)
      s0 = stores.head
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      d0 = writeTransferringOutStore(deviceDir, s0)
      _ = os.write.over(d0 / CopyLayout.paramFile(Phase.Arrived("")).get, "t1")
      host = newHost(hostRoot, storeRebuildFactory = rebuilds)
      _ <- resolve(host)
      st <- state(deviceA)
    yield
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should not be(StoreStatus.Rebuilding)
      CopyFixtures.phaseOf(deviceDir, s0).contains(Phase.Lost) should be(false)
      rebuilds.created should be(empty)

  atest("an outbound store whose save file cannot be read fails the adoption for a later scan (SL-W18)"):
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
      _ = nudgeNow(host, CheckStorageDevice(me, client.clientId, deviceA))
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
      _ = checkAllNow(host)
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
      // PoolRepointedOffline: the arrived copy sits in arrived/, with no CRL file of its own.
      stores <- setUpMetadata(oldHostId, DeviceStatus.Shutdown, 2, registerOldHost = true): ss =>
                  Map(ss(0) -> offline, ss(1) -> offline)
      List(s0, s1) = stores: @unchecked
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      _ = writeClosedStore(deviceDir, s0)
      d1 = CopyFixtures.placeCopy(deviceDir, s1, Phase.Arrived(""))
      host = newHost(hostRoot)
      // Counted across checks: an offer opens the copy, as a real load does, so a later check does not offer it again
      _ = recorded(host).offersClaimOpening.set(true)
      _ <- resolve(host)
      st <- state(deviceA)
      // Its copy, registered Arrived at the device's load, is offline there, so the completed-transfers-in pass of
      // the adoption's own reconcile, or of the check below, completes it
      _ = checkAllNow(host)
      _ <- pumpUntil(CopyFixtures.phaseOf(deviceDir, s1).contains(Phase.Live) &&
                     host.control.reconciler.activeDeviceChecks.isEmpty)
    yield
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.stores(s1).status should be(StoreStatus.Active)
      CopyFixtures.phaseOf(deviceDir, s1) should be(Some(Phase.Live))
      // Never offered in arrived/ (only live/ copies are), and kept offline; loaded once, at its Live path, after its
      // move there (T9)
      recorded(host).storeLoadAttempts.map(_._2).count(_ == d1) should be(0)
      recorded(host).storeLoadAttempts.map(_._2).count(_ == CopyFixtures.livePathOf(deviceDir, s1)) should be(1)
      os.exists(CopyFixtures.livePathOf(deviceDir, s1) / StoreLoader.ArrivedAtFile) should be(true)

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
      _ = checkAllNow(oldHost)
      _ <- pumpUntil(rebuilds.created.nonEmpty && oldHost.control.reconciler.activeDeviceChecks.isEmpty)
      // The pool lives on deviceA alone, so the real request would refuse this shutdown.
      _ <- forceShuttingDown(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(oldHost)
      // The rebuild ignores its cancel, so the shutdown proceeds only once its wait times out.
      _ <- pumpUntil(recorded(oldHost).shutdownWorkTimeouts.nonEmpty)
      _ = recorded(oldHost).fireShutdownWorkTimeouts()
      _ <- pumpUntil(!loadedDevices(oldHost).contains(deviceA))
      // The rebuild, past its last read, lands its store and finishes on the unloaded device.
      flatOld = writeStoreConfig(oldDir, s0)
      _ = RebuildingStore.writeEmptyCrlState(flatOld, s0)
      _ = CopyFixtures.markCopy(oldDir, s0,
            Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(rebuilds.created.head.episode)))
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpUntil(CopyFixtures.phaseOf(oldDir, s0).contains(Phase.Parked))
      entryAfterRebuild <- state(deviceA).map(_.stores(s0))
      parked = CopyFixtures.phaseOf(oldDir, s0).contains(Phase.Parked)
      crlWritten = CopyFixtures.copyPath(oldDir, s0).exists(p => os.exists(Backend.crlSaveFile(p)))
      // Carried to the new host.
      newRoot = newHostDir()
      newDir = StorageDeviceManager.deviceDirectory(newRoot, "dev0")
      _ = os.move(oldDir, newDir)
      host = newHost(newRoot)
      _ <- resolve(host)
      st <- state(deviceA)
    yield
      entryAfterRebuild.status should be(StoreStatus.Offline)
      parked should be(true)
      crlWritten should be(true)
      recorded(host).adoptCalls.toList should be(List((deviceA, oldHostId)))
      st.status should be(DeviceStatus.Operational)
      st.hostId should be(HostId.BootstrapHostId)
      st.stores(s0).status should be(StoreStatus.Active)
      CopyFixtures.phaseOf(newDir, s0) should be(Some(Phase.Live))
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
      _ = checkAllNow(dest)
      _ <- pumpUntil(transfers.startedTransfersIn.contains(s0))
      firstAsk = startTransfers()
      _ <- setDeviceState(deviceA)(_.copy(status = DeviceStatus.Shutdown))
      (hostRoot, deviceDir) = writeCarriedDevice(oldHostId, oldHostId)
      // In outbound/ with its transfer's id, as the old host's transfer out left it.
      _ = writeTransferringOutStore(deviceDir, s0, transferId.toString)
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
      _ = scanNow(host)
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

  // Spec "Adoption": a lookup on the copy's phase alone. live, arrived, rebuilt and parked copies are Clean; an outbound
  // copy is Clean with a valid save file and CrlLost without; a lost copy is CrlLost; an entry whose store has no copy in
  // any phase (pending-delete/ is none) is Unclean
  atest("adoption classifies by phase"):
    val host = newHost(newHostDir())
    host.testingOnlyHandleEvents()
    val deviceDir = newHostDir() / "carried"
    val pool = PoolId(UUID.randomUUID())
    def sid(i: Int) = StoreId(pool, i.toByte)
    val placements = List(
      (Phase.Live, AdoptionClass.Clean),
      (Phase.Arrived("t"), AdoptionClass.Clean),
      (Phase.Rebuilt("e"), AdoptionClass.Clean),
      (Phase.Parked, AdoptionClass.Clean),
      (Phase.Outbound("t"), AdoptionClass.Clean),
      (Phase.Outbound("t"), AdoptionClass.CrlLost),
      (Phase.Lost, AdoptionClass.CrlLost),
      (Phase.Retired, AdoptionClass.Unclean))
    placements.zipWithIndex.foreach:
      case ((phase, cls), i) =>
        val copy = CopyFixtures.placeCopy(deviceDir, sid(i), phase)
        if phase.isInstanceOf[Phase.Outbound] && cls == AdoptionClass.Clean then
          RebuildingStore.writeEmptyCrlState(copy, sid(i))
    // No copy at all
    val absent = sid(placements.size)
    val expected = placements.zipWithIndex.map((p, i) => sid(i) -> p._2).toMap + (absent -> AdoptionClass.Unclean)
    // Offline and Active entries are both served by an adoption
    val state = deviceState(deviceA, expected.keys.zipWithIndex.map((s, i) =>
      s -> StoreEntry(if i % 2 == 0 then StoreStatus.Active else StoreStatus.Offline, None)).toMap)
    Future.successful:
      AdoptionClass.values.toList.map(cls => cls -> DeviceStateDisk.servedStoresIn(deviceDir, state, cls).toSet)
        .toMap should be(AdoptionClass.values.toList.map(cls =>
          cls -> expected.collect { case (s, c) if c == cls => s }.toSet).toMap)
