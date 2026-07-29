package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

/** A StoreManager that records tryLoadStore calls instead of opening RocksDB backends, and
 *  exposes the protected device map to assertions.
 *
 *  The override records and, when `failFirstStoreLoad` is set, throws once before recording
 *  anything. It does nothing else. In particular it does not replicate the real
 *  tryLoadStore's bookkeeping: it never filters on `StoreConfig.configFilename`, never
 *  honours a TransferringOut marker, and never updates `sds.loadedStores`, `sds.offlineStores`
 *  or the manager's `offlineStores`. Those sets therefore stay empty here, so a test asserting
 *  on them would pass vacuously — assert on `storeLoadAttempts` instead.
 *
 *  Constructor parameters are deliberately named differently from StoreManager's own members
 *  (`client`, `ec`, `net`, `rootDir`, ...) so the superclass constructor call cannot
 *  accidentally resolve to an inherited, not-yet-initialized member.
 */
private class RecordingStoreManager(mgrClient: AspenClient,
                            systemId: UUID,
                            hostRootDir: Path,
                            execCtx: ExecutionContext,
                            cacheFactory: () => ObjectCache,
                            messenger: ServerMessenger,
                            finalizers: TransactionFinalizer.Factory,
                            failFirstStoreLoad: Boolean = false)
  extends StoreManager(
    mgrClient,
    HostId.BootstrapHostId,
    systemId,
    hostRootDir,
    execCtx,
    cacheFactory,
    messenger,
    BackgroundTaskManager.NoBackgroundTaskManager,
    TestNetwork.TestCRL,
    finalizers,
    TransactionDriver.noErrorRecoveryFactory,
    Duration(5, SECONDS),
    Duration(60, SECONDS)):

  /** (deviceId, path) for every path tryLoadStore was invoked with, in call order.
   *
   *  These are candidate paths, not confirmed stores. tryLoadDevice offers every child of the
   *  device directory, so a successfully loaded device always contributes at least one
   *  non-store entry: its own `aspen-storage-device-config.yaml`.
   *
   *  Lazy because StoreManager's constructor scans storage-devices/ and can therefore call
   *  the override below before this subclass's fields would otherwise be initialized. A
   *  plain val would still be null at that point, and tryLoadDevice's catch-all would
   *  silently swallow the resulting NPE.
   */
  lazy val storeLoadAttempts: mutable.ListBuffer[(StorageDeviceId, Path)] =
    mutable.ListBuffer[(StorageDeviceId, Path)]()

  /** True while a simulated store-load failure is still owed. The first tryLoadStore call to
   *  see it set clears it and throws, modelling a device that dies part-way through loading
   *  its stores (a yanked hot-plug disk, an IO error).
   *
   *  Boxed and lazy for the same initialization-order reason as storeLoadAttempts: a plain
   *  `var` in this body would still hold its default when StoreManager's constructor scan runs.
   */
  private lazy val storeLoadFailureOwed = new AtomicBoolean(failFirstStoreLoad)

  override protected def tryLoadStore(sds: StoreManager.LocalStorageDeviceState,
                                      potentialStoreFile: File): Unit =
    synchronized:
      if storeLoadFailureOwed.getAndSet(false) then
        throw new RuntimeException(s"Simulated store load failure for $potentialStoreFile")
      storeLoadAttempts += ((sds.storageDeviceId, potentialStoreFile.toPath))

  def loadedDevices: Map[StorageDeviceId, StoreManager.LocalStorageDeviceState] =
    synchronized(storageDevices)


class StoreManagerDeviceDiscoverySuite extends IntegrationTestSuite:

  /** The Aspen system that the manager under test and the device configs written to disk both
   *  belong to, so those devices are accepted. `foreignSystemId` is the rejection case: a
   *  device config carrying it must be ignored by the scan. */
  private val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val foreignSystemId = UUID.fromString("44444444-4444-4444-4444-444444444444")

  private val tempRoots = mutable.ListBuffer[Path]()

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach(deleteTree)
    tempRoots.clear()

  /** Removes a tree. Failures are ignored; these are temp dirs. */
  private def deleteTree(root: Path): Unit =
    try os.remove.all(os.Path(root))
    catch case _: Throwable => ()

  /** Creates `<tmp>/host/storage-devices` and returns the host root directory. Pass
   *  `withStorageDevicesDir = false` to leave the `storage-devices` child absent. */
  private def newHostDir(withStorageDevicesDir: Boolean = true): Path =
    val base = Files.createTempDirectory("aspen-device-discovery")
    tempRoots += base
    val hostDir = base.resolve("host")
    if withStorageDevicesDir then
      Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    else
      Files.createDirectories(hostDir)
    hostDir

  /** Creates `<hostRoot>/storage-devices/<name>` with a device config naming `deviceId`. */
  private def writeDevice(hostRoot: Path,
                          name: String,
                          deviceId: StorageDeviceId,
                          sysId: UUID = systemId): Path =
    val dir = StorageDeviceManager.deviceDirectory(hostRoot, name)
    Files.createDirectories(dir)
    writeDeviceConfig(dir, deviceId, sysId)
    dir

  /** Writes only the config file into an existing device directory. */
  private def writeDeviceConfig(deviceDir: Path,
                                deviceId: StorageDeviceId,
                                sysId: UUID = systemId): Unit =
    Files.write(
      deviceDir.resolve(StorageDeviceConfig.configFilename),
      StorageDeviceConfig(deviceId, sysId).yamlConfig.getBytes(StandardCharsets.UTF_8))

  /** Creates an empty device directory with no config file. */
  private def newDeviceDirWithoutConfig(hostRoot: Path, name: String): Path =
    val dir = StorageDeviceManager.deviceDirectory(hostRoot, name)
    Files.createDirectories(dir)
    dir

  private def newManager(hostRoot: Path, failFirstStoreLoad: Boolean = false): RecordingStoreManager =
    new RecordingStoreManager(client, systemId, hostRoot, executionContext,
                              net.objectCacheFactory, net, net.FinalizerFactory,
                              failFirstStoreLoad)

  private val deviceA = StorageDeviceId(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"))
  private val deviceB = StorageDeviceId(UUID.fromString("bbbbbbbb-0000-0000-0000-000000000002"))

  atest("constructor loads a device that already exists on disk"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)

    Future.successful:
      mgr.loadedDevices.keySet should be(Set(deviceA))
      mgr.loadedDevices(deviceA).devicePath should be(deviceDir)

      // Also pins the recording seam itself: tryLoadStore is offered every child of the
      // device directory, and dev0's only child is the device config file.
      mgr.storeLoadAttempts.toList should be(
        List((deviceA, deviceDir.resolve(StorageDeviceConfig.configFilename))))

  atest("a device created after construction is discovered on the next CheckAllDevices"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.loadedDevices should be(empty)

    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("a missing storage-devices directory does not throw"):
    val mgr = newManager(newHostDir(withStorageDevicesDir = false))

    noException should be thrownBy mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices should be(empty))

  atest("an already-loaded device is not reloaded"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    Files.createDirectories(deviceDir.resolve("some-store-dir"))

    val mgr = newManager(hostRoot)
    val firstState = mgr.loadedDevices(deviceA)
    val attemptsAfterConstruction = mgr.storeLoadAttempts.toList

    // tryLoadDevice offers every child of the device directory to tryLoadStore: here the
    // device config file and some-store-dir. The real tryLoadStore would load neither -- it
    // requires a store config file inside the candidate, and the config file is not a
    // directory while some-store-dir is empty -- but the recording override takes both.
    // Compared as a set because listFiles() ordering is unspecified.
    attemptsAfterConstruction.map(_._2).toSet should be(Set(
      deviceDir.resolve(StorageDeviceConfig.configFilename),
      deviceDir.resolve("some-store-dir")))

    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyCheckAllDevices()

    // Same instance: the device's loadedStores/offlineStores tracking survives a rescan.
    mgr.loadedDevices(deviceA) should be theSameInstanceAs firstState
    Future.successful(mgr.storeLoadAttempts.toList should be(attemptsAfterConstruction))

  atest("a device whose stores fail to load is retried on a later scan"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot, failFirstStoreLoad = true)

    // The load threw part-way through, so the device must not be recorded as loaded. Were it
    // recorded, the idempotency guard would skip it forever and its stores would never load.
    mgr.loadedDevices.keySet should be(empty)

    // A later scan retries it from scratch and succeeds.
    mgr.testingOnlyCheckAllDevices()
    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("a second directory claiming a loaded device id is ignored"):
    val hostRoot = newHostDir()
    val originalDir = writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val originalState = mgr.loadedDevices(deviceA)
    originalState.devicePath should be(originalDir)

    // A duplicate mount or a copied config file: same device id, different directory.
    writeDevice(hostRoot, "dev0-copy", deviceA)
    mgr.testingOnlyCheckAllDevices()

    mgr.loadedDevices.keySet should be(Set(deviceA))
    Future.successful(mgr.loadedDevices(deviceA) should be theSameInstanceAs originalState)
