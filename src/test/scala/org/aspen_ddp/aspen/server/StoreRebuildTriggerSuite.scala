package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.store.backend.BackendOpener
import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.server.host.{ControlFixture, StoreRebuilds, DeviceMetadata, LocalStorageDeviceState, StagedSendService, StagedTransferService}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.network.{StoreMoved, StoreOffline}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.host.DeviceMetadata.RebuildOutcome
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.MapBackend

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/** A StoreRebuild that does nothing until the test resolves it. */
private class RecordedRebuild(val storeId: StoreId,
                              val storageDeviceId: StorageDeviceId,
                              val episode: Option[UUID] = None) extends StoreRebuild:
  val promise: Promise[Unit] = Promise()
  def complete: Future[Unit] = promise.future
  @volatile var cancelled: Boolean = false
  def cancel(): Unit = cancelled = true

private class RecordingRebuildFactory extends StoreRebuildFactory:
  val created: mutable.ListBuffer[RecordedRebuild] = mutable.ListBuffer()

  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: os.Path,
                    backends: BackendOpener,
                    episode: Option[UUID]): StoreRebuild =
    val r = new RecordedRebuild(storeId, storageDeviceId, episode)
    synchronized:
      created += r
    r

class StoreRebuildTriggerSuite extends IntegrationTestSuite with ControlFixture:

  private def rebuilding(storeId: StoreId): (StoreId, StorageDeviceState.StoreEntry) =
    storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None)

  private def storeN(n: Int): StoreId =
    StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), n.toByte)

  atest("a Rebuilding entry starts a rebuild"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)

    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    armed.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    yieldUntil(factory.created.nonEmpty).map: _ =>
      factory.created.map(_.storeId).toList should be(List(storeN(0)))
      factory.created.head.storageDeviceId should be(deviceA)

  atest("a store rebuilding on a device that never held it is silent, queued ones too (Rebuild F7)"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    armed.success(deviceState(deviceA, Map(rebuilding(storeN(0)), rebuilding(storeN(1)))))

    for
      _ <- yieldUntil(factory.created.nonEmpty)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0)))
      // Dropped rather than answered UnknownStore: the running rebuild and the one waiting for a slot
      host.control.router.offlineStores should contain allOf (storeN(0), storeN(1))

  atest("a failed rebuild pass keeps its store silent until the retry (Rebuild F7)"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)

    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    armed.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.nonEmpty)
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      _ <- yieldUntil(host.control.rebuilds.rebuildRetriesScheduled.contains(storeN(0)))
    yield
      host.control.router.offlineStores should contain(storeN(0))

  atest("a rebuild already running is not started twice"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)

    val first = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // A second check of the same device sees the same Rebuilding entry.
      second = recorded(host).armLookup(deviceA)
      _ = host.control.reconciler.checkAll()
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
    yield
      factory.created.size should be(1)

  atest("maxConcurrentRebuilds bounds how many start at once"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 2)

    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    armed.success(deviceState(deviceA,
      Map(rebuilding(storeN(0)), rebuilding(storeN(1)), rebuilding(storeN(2)))))

    for
      _ <- yieldUntil(factory.created.size == 2)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
    yield
      // Three entries, a bound of two: the third waits. A rebuild reads a whole store's worth
      // of data through the client, so an eight-store device must not start eight at once.
      factory.created.size should be(2)

  atest("a completed rebuild releases its slot and the next one starts"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    val twoRebuilding = deviceState(deviceA, Map(rebuilding(storeN(0)), rebuilding(storeN(1))))
    first.success(twoRebuilding)

    for
      _ <- yieldUntil(factory.created.size == 1)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
      // The first rebuild will complete and trigger a device check. Arm a second lookup
      // that returns a state where store 0 is now Active and store 1 is still Rebuilding.
      second = recorded(host).armLookup(deviceA)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      active = (storeN(0), StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
      oneRebuilding = deviceState(deviceA, Map(active, rebuilding(storeN(1))))
      _ = second.success(oneRebuilding)
      // deviceA is not in the tree, so the flip fails permanently: the re-check that frees the slot for store 1 is
      // the backoff retry
      _ <- yieldUntil(scheduler(host).pending.contains(Host.RebuildRetryInitialDelay))
      _ = runRebuildRetries(host)
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(1)))

  atest("a failed rebuild does not immediately restart itself"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // Armed but not consumed if the failure path behaves: the entry still reads Rebuilding and
      // the slot has just been freed, so a re-check of this device here would start the same
      // rebuild again -- and a rebuild that fails in under a millisecond would then spin forever
      // with no backoff and no give-up.
      second = recorded(host).armLookup(deviceA)
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      _ <- yieldUntil(factory.created.size == 2, 20)
      restartedImmediately = factory.created.size
      // The ordinary periodic check is what resumes it. The staging checkpoint survives the
      // failure, so that check resumes rather than restarting from the beginning.
      _ = host.control.reconciler.checkAll()
      _ <- yieldUntil(factory.created.size == 2)
    yield
      restartedImmediately should be(1)
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(0)))

  atest("a failed rebuild releases its slot for another device"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    writeDevice(hostRoot, "dev1", deviceB)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    // deviceB's store is queued behind the single slot deviceA's rebuild holds.
    val firstA = recorded(host).armLookup(deviceA)
    val firstB = recorded(host).armLookup(deviceB)
    host.control.reconciler.checkAll()
    firstA.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
    firstB.success(deviceState(deviceB, Map(rebuilding(storeN(1)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceB))
      // Pre-completed: the failure path issues this lookup itself, so there is no moment
      // between the check starting and the test completing it.
      _ = recorded(host).armLookup(deviceB).success(deviceState(deviceB, Map(rebuilding(storeN(1)))))
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      // rebuildingStores is host-wide, so the freed slot is what deviceB was waiting on. Without
      // this re-check it would wait for the hourly sweep with data under-replicated.
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(1)))
      factory.created.map(_.storageDeviceId).toList should be(List(deviceA, deviceB))

  atest("a successfully flipped rebuild is loaded, marked Active, and gossips StoreMoved"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)
    // A store of a pool that exists, so the StoreMoved gossip can read the pool's config
    val rebuilt = StoreId(PoolId.BootstrapPoolId, 0)

    // Register the device in the tree so the flip transaction can succeed
    for
      deviceSetId <- client.createStorageDeviceSet("test-set", 0, None)
      realDeviceId <- client.createStorageDevice(HostId.BootstrapHostId, deviceSetId)
      // Add a Rebuilding entry for the store
      ptr <- client.getStorageDevicePointer(realDeviceId)
      kvos <- client.read(ptr)
      state = StorageDeviceState(kvos)
      _ <- client.transactUntilSuccessful: tx =>
        Future.successful:
          val newState = state.setStoreEntry(rebuilt,
            StorageDeviceState.StoreStatus.Rebuilding, None)
          val reqs = List(KeyRevision(StorageDeviceState.StateKey,
            kvos.contents(StorageDeviceState.StateKey).revision))
          val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))
          tx.update(ptr, None, None, reqs, ops)
      _ <- waitForTransactionsToComplete()
      // Inject the device as loaded and trigger reconciliation
      _ = injectLoadedDevice(host, new LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir / StorageDeviceConfig.configFilename, new TestNetwork.TestCRL()))
      armed = recorded(host).armLookup(realDeviceId)
      _ = host.control.reconciler.checkAll()
      readState = deviceState(realDeviceId, Map(rebuilding(rebuilt)))
      _ = armed.success(readState)
      _ <- yieldUntil(factory.created.nonEmpty)
      // As finish leaves the copy: in place, carrying its rebuild-complete marker
      storeDir = deviceDir / rebuilt.directoryName
      _ = os.makeDir.all(storeDir)
      _ = os.write.over(storeDir / RebuildingStore.RebuildCompleteMarkerFile, "")
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(recorded(host).loadStoreByIdRequests.contains((realDeviceId, rebuilt)))
      // Verify the entry was flipped to Active
      finalKvos <- client.read(ptr)
      finalState = StorageDeviceState(finalKvos)
      _ <- yieldUntil(net.capturedGossip.exists:
        case m: StoreMoved => m.storeId == rebuilt
        case _ => false)
      (poolState, poolTimestamp) <- client.getStoragePoolStateAndTimestamp(rebuilt.poolId)
    yield
      recorded(host).loadStoreByIdRequests should contain((realDeviceId, rebuilt))
      // Removed once the flip committed, so the load serves it
      os.exists(deviceDir / rebuilt.directoryName / RebuildingStore.RebuildCompleteMarkerFile) should be(false)
      finalState.stores.get(rebuilt).map(_.status) should be(
        Some(StorageDeviceState.StoreStatus.Active))
      // The pool's current host for the store, as of the pool config's timestamp, and online
      // after the rebuild took it offline
      val gossip = net.takeCapturedGossip()
      val offline = gossip.collect { case m: StoreOffline if m.storeId == rebuilt => m }
      val moved = gossip.collect { case m: StoreMoved if m.storeId == rebuilt => m }
      offline should have size 1
      moved.map(m => (m.hostId, m.timestamp)) should be(
        List((poolState.stores(rebuilt.poolIndex).hostId, poolTimestamp)))
      moved.head.onlineTimestamp should be > offline.head.timestamp

  atest("an Active store marked rebuild-required is flipped to Rebuilding, discarded and rebuilt"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)
    val lost = storeN(0)
    val storeDir = deviceDir / lost.directoryName

    for
      deviceSetId <- client.createStorageDeviceSet("test-set", 0, None)
      realDeviceId <- client.createStorageDevice(HostId.BootstrapHostId, deviceSetId)
      ptr <- client.getStorageDevicePointer(realDeviceId)
      kvos <- client.read(ptr)
      _ <- client.transactUntilSuccessful: tx =>
        Future.successful:
          val newState = StorageDeviceState(kvos).setStoreEntry(lost,
            StorageDeviceState.StoreStatus.Active, None)
          tx.update(ptr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, newState.encode())))
      _ <- waitForTransactionsToComplete()
      // As crlSaveFileLost leaves it: marked, and offline. The recording tryLoadStore keeps
      // neither set, so the offline entry is made by hand.
      _ = os.makeDir.all(storeDir)
      _ = os.write.over(storeDir / RebuildingStore.RebuildRequiredMarkerFile, "")
      local = new LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir / StorageDeviceConfig.configFilename, new TestNetwork.TestCRL())
      _ = local.offlineStores = Set(lost)
      _ = injectLoadedDevice(host, local)
      _ = host.control.reconciler.checkAll()
      _ <- completeRebuildsStarted(factory)
      finalState <- client.getStorageDeviceState(realDeviceId)
    yield
      finalState.stores.get(lost).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      factory.created.map(_.storeId).toList should be(List(lost))
      os.exists(storeDir) should be(false)

  atest("a directory no rebuild produced at a Rebuilding store's path is retired before the rebuild starts"):
    // rebuild-process 4.1 (T12): a leftover of an earlier tenancy of the store on this device, with neither marker. It
    // must not be taken for a finished rebuild; it is kept in pending-delete like any lost copy, and the store rebuilt.
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)
    val stale = storeN(0)
    val storeDir = deviceDir / stale.directoryName
    os.makeDir.all(storeDir)
    os.write.over(storeDir / "data", "stale")

    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    armed.success(deviceState(deviceA, Map(rebuilding(stale))))

    def pending: List[os.Path] =
      val dir = deviceDir / RebuildingStore.PendingDeleteDirectory
      if os.exists(dir) then os.list(dir).toList else Nil

    yieldUntil(factory.created.nonEmpty).map: _ =>
      factory.created.map(_.storeId).toList should be(List(stale))
      os.exists(storeDir) should be(false)
      pending should have size 1
      os.exists(pending.head / "data") should be(true)
      os.exists(pending.head / RebuildingStore.RebuildRequiredMarkerFile) should be(true)

  atest("a served store its device's entry does not list is taken out of service and retired (RB-W5)"):
    // A copy put back at a store path by hand, loaded at startup without reading the tree. The reconcile takes it out
    // of service and keeps it in pending-delete; a served store its entry lists (Active) is left alone
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot)
    val unlisted = storeN(0)
    val listed = storeN(1)
    def backendAt(s: StoreId): MapBackend =
      val dir = deviceDir / s.directoryName
      os.makeDir.all(dir)
      new MapBackend(s):
        override def path: os.Path = dir
    val loads = List(unlisted, listed).map(s => host.loadStore(deviceA, backendAt(s), None))

    def served(s: StoreId): Boolean = host.control.router.loaded.contains(s)
    def pending: List[os.Path] =
      val dir = deviceDir / RebuildingStore.PendingDeleteDirectory
      if os.exists(dir) then os.list(dir).toList else Nil
    val active = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
    def answerChecks(): Boolean =
      host.testingOnlyHandleEvents()
      if !host.control.reconciler.activeDeviceChecks.contains(deviceA) then
        recorded(host).armLookup(deviceA).success(deviceState(deviceA, Map(listed -> active)))
        host.control.reconciler.checkStorageDevice(deviceA)
        host.testingOnlyHandleEvents()
      !os.exists(deviceDir / unlisted.directoryName)

    for
      _ <- yieldUntil({ host.testingOnlyHandleEvents(); loads.forall(_.isCompleted) && served(unlisted) && served(listed) })
      _ <- yieldUntil(answerChecks())
    yield
      served(unlisted) should be(false)
      loadedDevices(host)(deviceA).loadedStores should not contain unlisted
      pending should have size 1
      pending.head.last should startWith(unlisted.directoryName)
      os.exists(pending.head / RebuildingStore.RebuildRequiredMarkerFile) should be(true)
      served(listed) should be(true)
      os.exists(deviceDir / listed.directoryName / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

  private def rebuildingIn(storeId: StoreId, episode: UUID): (StoreId, StorageDeviceState.StoreEntry) =
    storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode))

  atest("a rebuild of an earlier Rebuilding episode is cancelled, and the current episode's rebuild started (RB-W3)"):
    // The store was re-placed away and back between two checks of the device: still Rebuilding here, under a new
    // episode. The running rebuild's copy may predate updates the store took while it lived elsewhere
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)
    val earlier = UUID.randomUUID()
    val current = UUID.randomUUID()

    val first = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    first.success(deviceState(deviceA, Map(rebuildingIn(storeN(0), earlier))))

    for
      _ <- yieldUntil(factory.created.nonEmpty)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
      // The same episode, checked again: left running
      same = recorded(host).armLookup(deviceA)
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ = same.success(deviceState(deviceA, Map(rebuildingIn(storeN(0), earlier))))
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
      keptRunning = !factory.created.head.cancelled
      replaced = recorded(host).armLookup(deviceA)
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ = replaced.success(deviceState(deviceA, Map(rebuildingIn(storeN(0), current))))
      _ <- yieldUntil(factory.created.head.cancelled)
      _ = recorded(host).armLookup(deviceA).success(deviceState(deviceA, Map(rebuildingIn(storeN(0), current))))
      _ = factory.created.head.promise.failure(RebuildingStore.RebuildCancelled(storeN(0)))
      _ <- yieldUntil(factory.created.size == 2)
    yield
      keptRunning should be(true)
      factory.created.map(r => r.storeId -> r.episode).toList should be(
        List(storeN(0) -> Some(earlier), storeN(0) -> Some(current)))
      factory.created(1).cancelled should be(false)

  atest("a copy finished for an earlier Rebuilding episode is retired before the rebuild starts (RB-W3)"):
    // Finished, and awaiting its flip, when the store was re-placed away; back here now under a new episode. It must
    // not be taken for done: it is kept in pending-delete like any lost copy, and the store rebuilt. A copy of the
    // current episode stays where it is, for the rebuild to flip
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 2)
    val current = UUID.randomUUID()
    def storeDir(n: Int): os.Path = deviceDir / storeN(n).directoryName
    List(0, 1).foreach(n => os.makeDir.all(storeDir(n)))
    os.write.over(storeDir(0) / RebuildingStore.RebuildCompleteMarkerFile, UUID.randomUUID().toString)
    os.write.over(storeDir(1) / RebuildingStore.RebuildCompleteMarkerFile, current.toString)

    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    armed.success(deviceState(deviceA, Map(rebuildingIn(storeN(0), current), rebuildingIn(storeN(1), current))))

    def pending: List[os.Path] =
      val dir = deviceDir / RebuildingStore.PendingDeleteDirectory
      if os.exists(dir) then os.list(dir).toList else Nil

    yieldUntil(factory.created.size == 2).map: _ =>
      factory.created.map(_.storeId).toSet should be(Set(storeN(0), storeN(1)))
      os.exists(storeDir(0)) should be(false)
      pending should have size 1
      os.exists(pending.head / RebuildingStore.RebuildRequiredMarkerFile) should be(true)
      os.read(storeDir(1) / RebuildingStore.RebuildCompleteMarkerFile) should be(current.toString)

  atest("a rebuild that finishes after its entry moved to a new Rebuilding episode is not promoted (RB-W3)"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)
    val rebuilt = StoreId(PoolId.BootstrapPoolId, 0)
    val earlier = UUID.randomUUID()
    val current = UUID.randomUUID()

    def setEntry(deviceId: StorageDeviceId, episode: UUID): Future[Unit] =
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
        _ <- client.transactUntilSuccessful: tx =>
          Future.successful:
            tx.update(ptr, None, None,
              List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
              List(Insert(StorageDeviceState.StateKey, StorageDeviceState(kvos).setRebuilding(rebuilt, episode).encode())))
        _ <- waitForTransactionsToComplete()
      yield ()

    for
      deviceSetId <- client.createStorageDeviceSet("test-set", 0, None)
      realDeviceId <- client.createStorageDevice(HostId.BootstrapHostId, deviceSetId)
      _ <- setEntry(realDeviceId, earlier)
      _ = injectLoadedDevice(host, new LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir / StorageDeviceConfig.configFilename, new TestNetwork.TestCRL()))
      armed = recorded(host).armLookup(realDeviceId)
      _ = host.control.reconciler.checkAll()
      _ = armed.success(deviceState(realDeviceId, Map(rebuildingIn(rebuilt, earlier))))
      _ <- yieldUntil(factory.created.nonEmpty)
      // Re-placed away and back before any check of this device saw it
      _ <- setEntry(realDeviceId, current)
      storeDir = deviceDir / rebuilt.directoryName
      _ = os.makeDir.all(storeDir)
      _ = os.write.over(storeDir / RebuildingStore.RebuildCompleteMarkerFile, earlier.toString)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(recorded(host).rebuildOutcomes.contains((realDeviceId, rebuilt)))
      finalState <- client.getStorageDeviceState(realDeviceId)
    yield
      recorded(host).rebuildOutcomes((realDeviceId, rebuilt)) should be(RebuildOutcome.Superseded)
      recorded(host).loadStoreByIdRequests should not contain((realDeviceId, rebuilt))
      finalState.stores(rebuilt).rebuildEpisode should be(Some(current))
      // Left for the current episode's rebuild to retire, and silent meanwhile
      os.read(storeDir / RebuildingStore.RebuildCompleteMarkerFile) should be(earlier.toString)
      host.control.rebuilds.silencedStores should contain(rebuilt)

  /** Pumps transactions until `factory` has created a rebuild. Silent on exhaustion: callers
   *  assert on it. */
  private def completeRebuildsStarted(factory: RecordingRebuildFactory, rounds: Int = 50): Future[Unit] =
    if factory.created.nonEmpty || rounds == 0 then
      Future.unit
    else
      for
        _ <- waitForTransactionsToComplete()
        _ <- yieldUntil(factory.created.nonEmpty, 20)
        _ <- completeRebuildsStarted(factory, rounds - 1)
      yield ()

  atest("a rebuild on a tombstoned device is discarded, not loaded"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)

    // Create and then tombstone the device
    for
      deviceSetId <- client.createStorageDeviceSet("test-set", 0, None)
      realDeviceId <- client.createStorageDevice(HostId.BootstrapHostId, deviceSetId)
      ptr <- client.getStorageDevicePointer(realDeviceId)
      kvos <- client.read(ptr)
      state = StorageDeviceState(kvos)
      // Add Rebuilding entry
      _ <- client.transactUntilSuccessful: tx =>
        Future.successful:
          val newState = state.setStoreEntry(storeN(0),
            StorageDeviceState.StoreStatus.Rebuilding, None)
          val reqs = List(KeyRevision(StorageDeviceState.StateKey,
            kvos.contents(StorageDeviceState.StateKey).revision))
          val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))
          tx.update(ptr, None, None, reqs, ops)
      _ <- waitForTransactionsToComplete()
      // Tombstone the device by zeroing both IDs
      kvos2 <- client.read(ptr)
      state2 = StorageDeviceState(kvos2)
      _ <- client.transactUntilSuccessful: tx =>
        Future.successful:
          val tombstoned = state2.copy(
            storageDeviceId = fixed_ids.FailedStorageDeviceId,
            hostId = fixed_ids.FailedHostId,
            status = StorageDeviceState.DeviceStatus.Failed)
          val reqs = List(KeyRevision(StorageDeviceState.StateKey,
            kvos2.contents(StorageDeviceState.StateKey).revision))
          val ops = List(Insert(StorageDeviceState.StateKey, tombstoned.encode()))
          tx.update(ptr, None, None, reqs, ops)
      _ <- waitForTransactionsToComplete()
      // Create the store directory that the rebuild would have placed
      storePath = deviceDir / storeN(0).directoryName
      _ = os.makeDir.all(storePath)
      _ = os.write(storePath / "marker.txt", "rebuilt store")
      _ = os.write(storePath / RebuildingStore.RebuildCompleteMarkerFile, "")
      // Inject device and trigger rebuild
      _ = injectLoadedDevice(host, new LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir / StorageDeviceConfig.configFilename, new TestNetwork.TestCRL()))
      armed = recorded(host).armLookup(realDeviceId)
      _ = host.control.reconciler.checkAll()
      readState = deviceState(realDeviceId, Map(rebuilding(storeN(0))))
      _ = armed.success(readState)
      _ <- yieldUntil(factory.created.nonEmpty)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(!os.exists(storePath), 200)
    yield
      // Store should NOT be loaded (no loadStoreById call for the real device)
      recorded(host).loadStoreByIdRequests.exists(_._1 == realDeviceId) should be(false)
      // Store directory should be discarded
      os.exists(storePath) should be(false)
      // Nothing puts the store here any more, so its offline mark is gone
      host.control.router.offlineStores should not contain storeN(0)
      host.control.rebuilds.silencedStores should not contain storeN(0)

  atest("a permanently failed flip is logged and releases its slot"):
    val hostRoot = newHostDir()
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    // Use synthetic IDs not registered in the tree - inject manually to avoid auto-scan
    val syntheticA = StorageDeviceId(UUID.fromString("cccccccc-0000-0000-0000-000000000001"))
    val syntheticB = StorageDeviceId(UUID.fromString("dddddddd-0000-0000-0000-000000000002"))
    val deviceDir = hostRoot / "storage-devices" / "synth-a"
    val deviceDir2 = hostRoot / "storage-devices" / "synth-b"
    os.makeDir.all(deviceDir)
    os.makeDir.all(deviceDir2)

    // syntheticA is NOT registered in the tree, so getStorageDevicePointer will throw NoSuchElementException
    // This exercises the Failure(t) branch in markRebuiltStoreActive's onComplete

    // Inject both devices manually (no writeDevice, so auto-scan won't find them)
    injectLoadedDevice(host, new LocalStorageDeviceState(
      syntheticA, deviceDir, deviceDir / "dummy.file", new TestNetwork.TestCRL()))
    injectLoadedDevice(host, new LocalStorageDeviceState(
      syntheticB, deviceDir2, deviceDir2 / "dummy.file", new TestNetwork.TestCRL()))

    val first = recorded(host).armLookup(syntheticA)
    host.control.reconciler.checkAll()
    first.success(deviceState(syntheticA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // Arm a second lookup for syntheticB before completing the first rebuild
      second = recorded(host).armLookup(syntheticB)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      // The flip transaction will fail with NoSuchElementException (syntheticA not in tree)
      // which should be logged, and the slot should be released so syntheticB's rebuild can start
      _ = second.success(deviceState(syntheticB, Map(rebuilding(storeN(1)))))
      _ <- yieldUntil(factory.created.size == 2, 200)
    yield
      // Store 0 should NOT be loaded (the flip failed)
      recorded(host).loadStoreByIdRequests.exists(_._1 == syntheticA) should be(false)
      // Store 1 should eventually be created (slot was released)
      factory.created.map(_.storeId).toSet should contain(storeN(1))

  test("the rebuild retry backoff doubles from its initial delay up to its cap"):
    import scala.concurrent.duration.*
    StoreRebuilds.rebuildRetryDelay(0) should be(Host.RebuildRetryInitialDelay)
    StoreRebuilds.rebuildRetryDelay(1) should be(Host.RebuildRetryInitialDelay * 2)
    StoreRebuilds.rebuildRetryDelay(2) should be(Host.RebuildRetryInitialDelay * 4)
    StoreRebuilds.rebuildRetryDelay(50) should be(Host.RebuildRetryMaxDelay)
    StoreRebuilds.rebuildRetryDelay(Int.MaxValue) should be(Host.RebuildRetryMaxDelay)

  atest("a failed rebuild pass is retried after a backoff that grows with each failure (Rebuild F5)"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)
    val rebuildingState = deviceState(deviceA, Map(rebuilding(storeN(0))))

    val first = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    first.success(rebuildingState)

    def retriesPending = scheduler(host).pending.filter(d =>
      d >= Host.RebuildRetryInitialDelay && d <= Host.RebuildRetryMaxDelay)

    for
      _ <- yieldUntil(factory.created.size == 1)
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      _ <- yieldUntil(retriesPending.nonEmpty)
      firstDelay = retriesPending
      _ = recorded(host).armLookup(deviceA).success(rebuildingState)
      _ = runRebuildRetries(host)
      _ <- yieldUntil(factory.created.size == 2)
      _ = factory.created(1).promise.failure(new Exception("disk still on fire"))
      _ <- yieldUntil(retriesPending.nonEmpty)
      secondDelay = retriesPending
    yield
      firstDelay should be(List(Host.RebuildRetryInitialDelay))
      secondDelay should be(List(Host.RebuildRetryInitialDelay * 2))
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(0)))

  atest("a permanently failed flip does not restart its rebuild at once, but after a backoff (Rebuild F11)"):
    val hostRoot = newHostDir()
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    // Not registered in the tree, so the flip's getStorageDevicePointer fails permanently
    val synthetic = StorageDeviceId(UUID.fromString("cccccccc-0000-0000-0000-000000000003"))
    val deviceDir = hostRoot / "storage-devices" / "synth-c"
    os.makeDir.all(deviceDir)
    injectLoadedDevice(host, new LocalStorageDeviceState(
      synthetic, deviceDir, deviceDir / "dummy.file", new TestNetwork.TestCRL()))
    val rebuildingState = deviceState(synthetic, Map(rebuilding(storeN(0))))

    val first = recorded(host).armLookup(synthetic)
    host.control.reconciler.checkAll()
    first.success(rebuildingState)

    for
      _ <- yieldUntil(factory.created.size == 1)
      // Pre-completed: a re-check of this device right after the failed flip would consume it and restart the store
      _ = recorded(host).armLookup(synthetic).success(rebuildingState)
      _ = factory.created.head.promise.success(())
      _ <- yieldUntil(scheduler(host).pending.contains(Host.RebuildRetryInitialDelay), 400)
      _ <- yieldUntil(factory.created.size == 2, 20)
      restartedImmediately = factory.created.size
      _ = runRebuildRetries(host)
      _ <- yieldUntil(factory.created.size == 2)
    yield
      restartedImmediately should be(1)
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(0)))
      recorded(host).loadStoreByIdRequests.exists(_._1 == synthetic) should be(false)

  atest("a rebuild whose entry leaves its device is cancelled and its staging copy deleted (Rebuild F12)"):
    val hostRoot = newHostDir()
    val devDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 2)
    def staging(n: Int): os.Path = devDir / RebuildingStore.RebuildDirectory / storeN(n).directoryName

    val first = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)), rebuilding(storeN(1)))))

    for
      _ <- yieldUntil(factory.created.size == 2)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
      _ = List(0, 1, 2).foreach(n => os.makeDir.all(staging(n)))
      // Store 0 re-placed elsewhere; store 2's staging copy was left by an earlier rebuild here
      reconciled = recorded(host).armLookup(deviceA)
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ = reconciled.success(deviceState(deviceA, Map(rebuilding(storeN(1)))))
      _ <- yieldUntil(factory.created.head.cancelled)
      runningCopyKept = os.exists(staging(0))
      _ = recorded(host).armLookup(deviceA).success(deviceState(deviceA, Map(rebuilding(storeN(1)))))
      _ = factory.created.head.promise.failure(RebuildingStore.RebuildCancelled(storeN(0)))
      _ <- yieldUntil(!os.exists(staging(0)))
    yield
      factory.created.head.cancelled should be(true)
      factory.created(1).cancelled should be(false)
      // Kept while its rebuild was still running, deleted by the check its cancel asked for
      runningCopyKept should be(true)
      os.exists(staging(0)) should be(false)
      os.exists(staging(1)) should be(true)
      os.exists(staging(2)) should be(false)
      // Not restarted: nothing lists it here
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(1)))

  atest("a rebuild whose entry leaves its device, running or queued, is no longer silenced (Rebuild F7)"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)
    val other = storeN(2)

    val first = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkAll()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)), rebuilding(storeN(1)), rebuilding(other))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
      silencedAtStart = host.control.router.offlineStores
      // Stores 0 (running) and 1 (queued) re-placed elsewhere; store 2 still queued here
      reconciled = recorded(host).armLookup(deviceA)
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ = reconciled.success(deviceState(deviceA, Map(rebuilding(other))))
      _ <- yieldUntil(factory.created.head.cancelled)
      _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
      queuedCleared = !host.control.router.offlineStores.contains(storeN(1))
      runningKept = host.control.router.offlineStores.contains(storeN(0))
      _ = recorded(host).armLookup(deviceA).success(deviceState(deviceA, Map(rebuilding(other))))
      _ = factory.created.head.promise.failure(RebuildingStore.RebuildCancelled(storeN(0)))
      _ <- yieldUntil(!host.control.router.offlineStores.contains(storeN(0)))
    yield
      silencedAtStart should contain allOf (storeN(0), storeN(1), other)
      queuedCleared should be(true)
      // Still silent while its cancelled rebuild winds down
      runningKept should be(true)
      host.control.router.offlineStores should not contain storeN(0)
      host.control.router.offlineStores should contain(other)
