package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.rebuild.{StoreRebuild, StoreRebuildFactory}

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/** A StoreRebuild that does nothing until the test resolves it. */
private class RecordedRebuild(val storeId: StoreId,
                              val storageDeviceId: StorageDeviceId) extends StoreRebuild:
  val promise: Promise[Unit] = Promise()
  def complete: Future[Unit] = promise.future

private class RecordingRebuildFactory extends StoreRebuildFactory:
  val created: mutable.ListBuffer[RecordedRebuild] = mutable.ListBuffer()

  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: Path): StoreRebuild =
    val r = new RecordedRebuild(storeId, storageDeviceId)
    synchronized:
      created += r
    r

class StoreRebuildTriggerSuite extends IntegrationTestSuite with HostTestHarness:

  private def rebuilding(storeId: StoreId): (StoreId, StorageDeviceState.StoreEntry) =
    storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None)

  private def storeN(n: Int): StoreId =
    StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), n.toByte)

  atest("a Rebuilding entry starts a rebuild"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)

    val armed = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()
    armed.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    yieldUntil(factory.created.nonEmpty).map: _ =>
      factory.created.map(_.storeId).toList should be(List(storeN(0)))
      factory.created.head.storageDeviceId should be(deviceA)

  atest("a rebuild already running is not started twice"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)

    val first = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // A second check of the same device sees the same Rebuilding entry.
      second = host.armLookup(deviceA)
      _ = host.testingOnlyCheckAllDevices()
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      _ <- yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceA))
    yield
      factory.created.size should be(1)

  atest("maxConcurrentRebuilds bounds how many start at once"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 2)

    val armed = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()
    armed.success(deviceState(deviceA,
      Map(rebuilding(storeN(0)), rebuilding(storeN(1)), rebuilding(storeN(2)))))

    for
      _ <- yieldUntil(factory.created.size == 2)
      _ <- yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceA))
    yield
      // Three entries, a bound of two: the third waits. A rebuild reads a whole store's worth
      // of data through the client, so an eight-store device must not start eight at once.
      factory.created.size should be(2)

  atest("a completed rebuild releases its slot and the next one starts"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()
    val twoRebuilding = deviceState(deviceA, Map(rebuilding(storeN(0)), rebuilding(storeN(1))))
    first.success(twoRebuilding)

    for
      _ <- yieldUntil(factory.created.size == 1)
      _ <- yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceA))
      // The first rebuild will complete and trigger a device check. Arm a second lookup
      // that returns a state where store 0 is now Active and store 1 is still Rebuilding.
      second = host.armLookup(deviceA)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      active = (storeN(0), StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
      oneRebuilding = deviceState(deviceA, Map(active, rebuilding(storeN(1))))
      _ = second.success(oneRebuilding)
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(1)))

  atest("a failed rebuild does not immediately restart itself"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // Armed but not consumed if the failure path behaves: the entry still reads Rebuilding and
      // the slot has just been freed, so a re-check of this device here would start the same
      // rebuild again -- and a rebuild that fails in under a millisecond would then spin forever
      // with no backoff and no give-up.
      second = host.armLookup(deviceA)
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      _ <- yieldUntil(factory.created.size == 2, 20)
      restartedImmediately = factory.created.size
      // The ordinary periodic check is what resumes it. The staging checkpoint survives the
      // failure, so that check resumes rather than restarting from the beginning.
      _ = host.testingOnlyCheckAllDevices()
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
    val firstA = host.armLookup(deviceA)
    val firstB = host.armLookup(deviceB)
    host.testingOnlyCheckAllDevices()
    firstA.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
    firstB.success(deviceState(deviceB, Map(rebuilding(storeN(1)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      _ <- yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceB))
      // Pre-completed: the failure path issues this lookup itself, so there is no moment
      // between the check starting and the test completing it.
      _ = host.armLookup(deviceB).success(deviceState(deviceB, Map(rebuilding(storeN(1)))))
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      // rebuildingStores is host-wide, so the freed slot is what deviceB was waiting on. Without
      // this re-check it would wait for the hourly sweep with data under-replicated.
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(1)))
      factory.created.map(_.storageDeviceId).toList should be(List(deviceA, deviceB))

  atest("a successfully flipped rebuild is loaded and marked Active"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)

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
          val newState = state.setStoreEntry(storeN(0),
            StorageDeviceState.StoreStatus.Rebuilding, None)
          val reqs = List(KeyRevision(StorageDeviceState.StateKey,
            kvos.contents(StorageDeviceState.StateKey).revision))
          val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))
          tx.update(ptr, None, None, reqs, ops)
      _ <- waitForTransactionsToComplete()
      // Inject the device as loaded and trigger reconciliation
      _ = host.injectLoadedDevice(new Host.LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir.resolve(StorageDeviceConfig.configFilename).toFile))
      armed = host.armLookup(realDeviceId)
      _ = host.testingOnlyCheckAllDevices()
      readState = deviceState(realDeviceId, Map(rebuilding(storeN(0))))
      _ = armed.success(readState)
      _ <- yieldUntil(factory.created.nonEmpty)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(host.loadStoreByIdRequests.contains((realDeviceId, storeN(0))))
      // Verify the entry was flipped to Active
      finalKvos <- client.read(ptr)
      finalState = StorageDeviceState(finalKvos)
    yield
      host.loadStoreByIdRequests should contain((realDeviceId, storeN(0)))
      finalState.stores.get(storeN(0)).map(_.status) should be(
        Some(StorageDeviceState.StoreStatus.Active))

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
            hostId = fixed_ids.FailedHostId)
          val reqs = List(KeyRevision(StorageDeviceState.StateKey,
            kvos2.contents(StorageDeviceState.StateKey).revision))
          val ops = List(Insert(StorageDeviceState.StateKey, tombstoned.encode()))
          tx.update(ptr, None, None, reqs, ops)
      _ <- waitForTransactionsToComplete()
      // Create the store directory that the rebuild would have placed
      storePath = os.Path(deviceDir) / storeN(0).directoryName
      _ = os.makeDir.all(storePath)
      _ = os.write(storePath / "marker.txt", "rebuilt store")
      // Inject device and trigger rebuild
      _ = host.injectLoadedDevice(new Host.LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir.resolve(StorageDeviceConfig.configFilename).toFile))
      armed = host.armLookup(realDeviceId)
      _ = host.testingOnlyCheckAllDevices()
      readState = deviceState(realDeviceId, Map(rebuilding(storeN(0))))
      _ = armed.success(readState)
      _ <- yieldUntil(factory.created.nonEmpty)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(!os.exists(storePath), 200)
    yield
      // Store should NOT be loaded (no loadStoreById call for the real device)
      host.loadStoreByIdRequests.exists(_._1 == realDeviceId) should be(false)
      // Store directory should be discarded
      os.exists(storePath) should be(false)

  atest("a permanently failed flip is logged and releases its slot"):
    val hostRoot = newHostDir()
    val factory = new RecordingRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    // Use synthetic IDs not registered in the tree - inject manually to avoid auto-scan
    val syntheticA = StorageDeviceId(UUID.fromString("cccccccc-0000-0000-0000-000000000001"))
    val syntheticB = StorageDeviceId(UUID.fromString("dddddddd-0000-0000-0000-000000000002"))
    val deviceDir = hostRoot.resolve("storage-devices").resolve("synth-a")
    val deviceDir2 = hostRoot.resolve("storage-devices").resolve("synth-b")
    Files.createDirectories(deviceDir)
    Files.createDirectories(deviceDir2)

    // syntheticA is NOT registered in the tree, so getStorageDevicePointer will throw NoSuchElementException
    // This exercises the Failure(t) branch in markRebuiltStoreActive's onComplete

    // Inject both devices manually (no writeDevice, so auto-scan won't find them)
    host.injectLoadedDevice(new Host.LocalStorageDeviceState(
      syntheticA, deviceDir, deviceDir.resolve("dummy.file").toFile))
    host.injectLoadedDevice(new Host.LocalStorageDeviceState(
      syntheticB, deviceDir2, deviceDir2.resolve("dummy.file").toFile))

    val first = host.armLookup(syntheticA)
    host.testingOnlyCheckAllDevices()
    first.success(deviceState(syntheticA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // Arm a second lookup for syntheticB before completing the first rebuild
      second = host.armLookup(syntheticB)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      // The flip transaction will fail with NoSuchElementException (syntheticA not in tree)
      // which should be logged, and the slot should be released so syntheticB's rebuild can start
      _ = second.success(deviceState(syntheticB, Map(rebuilding(storeN(1)))))
      _ <- yieldUntil(factory.created.size == 2, 200)
    yield
      // Store 0 should NOT be loaded (the flip failed)
      host.loadStoreByIdRequests.exists(_._1 == syntheticA) should be(false)
      // Store 1 should eventually be created (slot was released)
      factory.created.map(_.storeId).toSet should contain(storeN(1))
