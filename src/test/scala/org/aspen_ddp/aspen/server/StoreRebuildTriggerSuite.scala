package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.rebuild.{StoreRebuild, StoreRebuildFactory}

import java.nio.file.Path
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

class StoreRebuildTriggerSuite extends IntegrationTestSuite with StoreManagerTestHarness:

  private def rebuilding(storeId: StoreId): (StoreId, StorageDeviceState.StoreEntry) =
    storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None)

  private def storeN(n: Int): StoreId =
    StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), n.toByte)

  atest("a Rebuilding entry starts a rebuild"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory)

    val armed = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    armed.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    yieldUntil(factory.created.nonEmpty).map: _ =>
      factory.created.map(_.storeId).toList should be(List(storeN(0)))
      factory.created.head.storageDeviceId should be(deviceA)

  atest("a rebuild already running is not started twice"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory)

    val first = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // A second check of the same device sees the same Rebuilding entry.
      second = mgr.armLookup(deviceA)
      _ = mgr.testingOnlyCheckAllDevices()
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      _ <- yieldUntil(!mgr.testingOnlyActiveDeviceChecks.contains(deviceA))
    yield
      factory.created.size should be(1)

  atest("maxConcurrentRebuilds bounds how many start at once"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 2)

    val armed = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    armed.success(deviceState(deviceA,
      Map(rebuilding(storeN(0)), rebuilding(storeN(1)), rebuilding(storeN(2)))))

    for
      _ <- yieldUntil(factory.created.size == 2)
      _ <- yieldUntil(!mgr.testingOnlyActiveDeviceChecks.contains(deviceA))
    yield
      // Three entries, a bound of two: the third waits. A rebuild reads a whole store's worth
      // of data through the client, so an eight-store device must not start eight at once.
      factory.created.size should be(2)

  atest("a completed rebuild releases its slot and the next one starts"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    val twoRebuilding = deviceState(deviceA, Map(rebuilding(storeN(0)), rebuilding(storeN(1))))
    first.success(twoRebuilding)

    for
      _ <- yieldUntil(factory.created.size == 1)
      _ <- yieldUntil(!mgr.testingOnlyActiveDeviceChecks.contains(deviceA))
      // The first rebuild will complete and trigger a device check. Arm a second lookup
      // that returns a state where store 0 is now Active and store 1 is still Rebuilding.
      second = mgr.armLookup(deviceA)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      active = (storeN(0), StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
      oneRebuilding = deviceState(deviceA, Map(active, rebuilding(storeN(1))))
      _ = second.success(oneRebuilding)
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(1)))

  atest("a failed rebuild releases its slot"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      second = mgr.armLookup(deviceA)
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      // The same store is retried rather than being stuck behind a slot that never frees.
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(0)))

  atest("a successfully flipped rebuild is loaded and marked Active"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory)

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
      _ = mgr.injectLoadedDevice(new StoreManager.LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir.resolve(StorageDeviceConfig.configFilename).toFile))
      armed = mgr.armLookup(realDeviceId)
      _ = mgr.testingOnlyCheckAllDevices()
      readState = deviceState(realDeviceId, Map(rebuilding(storeN(0))))
      _ = armed.success(readState)
      _ <- yieldUntil(factory.created.nonEmpty)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(mgr.loadStoreByIdRequests.contains((realDeviceId, storeN(0))))
      // Verify the entry was flipped to Active
      finalKvos <- client.read(ptr)
      finalState = StorageDeviceState(finalKvos)
    yield
      mgr.loadStoreByIdRequests should contain((realDeviceId, storeN(0)))
      finalState.stores.get(storeN(0)).map(_.status) should be(
        Some(StorageDeviceState.StoreStatus.Active))

  atest("a rebuild on a tombstoned device is discarded, not loaded"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory)

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
      _ = mgr.injectLoadedDevice(new StoreManager.LocalStorageDeviceState(
        realDeviceId, deviceDir, deviceDir.resolve(StorageDeviceConfig.configFilename).toFile))
      armed = mgr.armLookup(realDeviceId)
      _ = mgr.testingOnlyCheckAllDevices()
      readState = deviceState(realDeviceId, Map(rebuilding(storeN(0))))
      _ = armed.success(readState)
      _ <- yieldUntil(factory.created.nonEmpty)
      _ = factory.created.head.promise.success(())
      _ <- waitForTransactionsToComplete()
      _ <- yieldUntil(!os.exists(storePath), 200)
    yield
      // Store should NOT be loaded (no loadStoreById call for the real device)
      mgr.loadStoreByIdRequests.exists(_._1 == realDeviceId) should be(false)
      // Store directory should be discarded
      os.exists(storePath) should be(false)
