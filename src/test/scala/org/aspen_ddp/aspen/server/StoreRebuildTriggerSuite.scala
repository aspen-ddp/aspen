package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
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
