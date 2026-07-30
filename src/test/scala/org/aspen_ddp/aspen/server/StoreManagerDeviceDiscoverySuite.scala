package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
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
import scala.concurrent.{ExecutionContext, Future, Promise}
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
   *  Boxed and lazy for the same initialization-order reason as storeLoadAttempts. A plain
   *  `var` in this body would still hold `false` during StoreManager's constructor scan and
   *  would then be assigned after it, arming the failure for the first rescan instead. The
   *  constructor parameter it reads is safe: parameters are assigned before the superclass
   *  constructor runs.
   */
  private lazy val storeLoadFailureOwed = new AtomicBoolean(failFirstStoreLoad)

  override protected def tryLoadStore(sds: StoreManager.LocalStorageDeviceState,
                                      potentialStoreFile: File): Unit =
    synchronized:
      if storeLoadFailureOwed.getAndSet(false) then
        throw new RuntimeException(s"Simulated store load failure for $potentialStoreFile")
      storeLoadAttempts += ((sds.storageDeviceId, potentialStoreFile.toPath))

  /** Storage device ids passed to lookupStorageDeviceState, in call order.
   *
   *  Lazy for the same reason as armedLookups below.
   */
  lazy val lookupAttempts: mutable.ListBuffer[StorageDeviceId] =
    mutable.ListBuffer[StorageDeviceId]()

  /** Promises queued by armLookup, consumed one per lookup of that device.
   *
   *  Lazy to match storeLoadAttempts. Unlike that field, nothing in StoreManager's constructor
   *  reaches this override today: the constructor's device scan calls tryLoadStore, but a
   *  device check only ever runs from handleEvent. The uniformity is deliberate insurance
   *  against that changing.
   */
  private lazy val armedLookups: mutable.Map[StorageDeviceId, mutable.Queue[Promise[StorageDeviceState]]] =
    mutable.Map[StorageDeviceId, mutable.Queue[Promise[StorageDeviceState]]]()

  /** Arms one lookup of `deviceId` to return a Future the test completes when it chooses.
   *
   *  Call once per lookup the test intends to control, in the order they will be issued.
   *  Lookups beyond the armed ones fall through to the real client, which is what keeps the
   *  tests that rely on a genuine lookup failure working unchanged.
   */
  def armLookup(deviceId: StorageDeviceId): Promise[StorageDeviceState] = synchronized:
    val p = Promise[StorageDeviceState]()
    armedLookups.getOrElseUpdate(deviceId, mutable.Queue[Promise[StorageDeviceState]]()).enqueue(p)
    p

  override protected def lookupStorageDeviceState(
      storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    val armed = synchronized:
      lookupAttempts += storageDeviceId
      armedLookups.get(storageDeviceId).filter(_.nonEmpty).map(_.dequeue())

    armed match
      case Some(p) => p.future
      case None    => super.lookupStorageDeviceState(storageDeviceId)

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

  /** Returns a Future that completes once `condition` holds, re-testing it after every yield
   *  back to the test's ExecutionContext, and giving up after `maxYields` of them.
   *
   *  ScalaTest's async ExecutionContext is single threaded and is the same thread that runs the
   *  test body: queued work only runs while the test is waiting on the Future it returned. A
   *  sleeping wait here would therefore starve the very callback being waited for; yielding
   *  lets everything queued ahead of us run instead.
   *
   *  Giving up is silent: the returned Future completes with unit either way, so an exhausted
   *  wait is indistinguishable from a satisfied one. That is intentional -- asserting the
   *  condition afterwards reports what was actually wrong far better than a timeout would --
   *  but it means callers MUST assert the condition themselves or they get a false pass.
   */
  private def yieldUntil(condition: => Boolean, maxYields: Int = 100): Future[Unit] =
    if condition || maxYields == 0 then
      Future.unit
    else
      Future(()).flatMap(_ => yieldUntil(condition, maxYields - 1))

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

  /** Overwrites the config file with bytes SnakeYAML cannot parse. */
  private def writeUnparseableDeviceConfig(deviceDir: Path): Unit =
    Files.write(deviceDir.resolve(StorageDeviceConfig.configFilename),
                "this is not: valid: device config yaml".getBytes(StandardCharsets.UTF_8))

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

  /** A store of pool 1111...:index 0. Used for its `directoryName`, which is the on-disk name of
   *  a store directory within a device directory. */
  private val storeId = StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), 0.toByte)

  private val deviceSetId = StorageDeviceSetId(UUID.fromString("55555555-5555-5555-5555-555555555555"))

  /** A StorageDeviceState for `deviceId` owned by this manager's host, carrying `stores`.
   *
   *  BootstrapHostId matches the manager's own hostId, which keeps check() off its
   *  host-migration branch. The sizes are arbitrary; nothing under test reads them.
   */
  private def deviceState(deviceId: StorageDeviceId,
                          stores: Map[StoreId, StorageDeviceState.StoreEntry] = Map()): StorageDeviceState =
    StorageDeviceState(deviceId, HostId.BootstrapHostId, 0L, 1024L, stores, deviceSetId)

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

    mgr.loadedDevices.keySet should be(empty)

    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("a missing storage-devices directory does not throw"):
    val mgr = newManager(newHostDir(withStorageDevicesDir = false))

    noException should be thrownBy mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(empty))

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

    // Same instance: the device's offlineStores tracking survives a rescan.
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

  atest("a device directory with no config is skipped, then loaded once the config appears"):
    val hostRoot = newHostDir()
    val deviceDir = newDeviceDirWithoutConfig(hostRoot, "dev0")

    val mgr = newManager(hostRoot)
    mgr.loadedDevices.keySet should be(empty)

    // createStorageDevice requires the directory to already exist and never creates one, so a
    // mounted-but-not-yet-registered directory is a normal steady state, not a brief window.
    // Every scan has to tolerate it and pick the device up whenever the config does appear.
    writeDeviceConfig(deviceDir, deviceA)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("CheckStorageDevice for an unknown but on-disk device triggers discovery"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.loadedDevices.keySet should be(empty)

    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("an unusable entry under storage-devices does not stop its siblings from loading"):
    val hostRoot = newHostDir()
    // .DS_Store and friends: a plain file where the scan expects a device directory.
    Files.write(StorageDeviceManager.deviceDirectory(hostRoot, ".DS_Store"),
                "not a device".getBytes(StandardCharsets.UTF_8))
    newDeviceDirWithoutConfig(hostRoot, "unconfigured")
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("a device belonging to another Aspen system is ignored"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA, foreignSystemId)
    writeDevice(hostRoot, "dev1", deviceB)

    val mgr = newManager(hostRoot)

    mgr.loadedDevices.keySet should be(Set(deviceB))

    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceB)))

  atest("an unparseable device config is skipped and retried on the next scan"):
    val hostRoot = newHostDir()
    val deviceDir = newDeviceDirWithoutConfig(hostRoot, "dev0")
    // Two ':' separators in one flow scalar, so SnakeYAML throws rather than returning a
    // config missing its required keys. This is the only test covering tryLoadDevice's
    // catch-all; keep the string genuinely unparseable or it silently becomes a duplicate of
    // the missing-config test above.
    writeUnparseableDeviceConfig(deviceDir)

    val mgr = newManager(hostRoot)
    mgr.loadedDevices.keySet should be(empty)

    // A config that stays broken must not poison later scans or half-register the device.
    mgr.testingOnlyCheckAllDevices()
    mgr.loadedDevices.keySet should be(empty)

    writeDeviceConfig(deviceDir, deviceA)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("stores on a newly discovered device are offered to tryLoadStore"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.storeLoadAttempts should be(empty)

    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val storeDir = deviceDir.resolve(storeId.directoryName)
    Files.createDirectories(storeDir)

    mgr.testingOnlyCheckAllDevices()

    mgr.loadedDevices.keySet should be(Set(deviceA))
    // tryLoadDevice offers every child of the device directory to tryLoadStore: here the
    // device config file and the store directory. The real tryLoadStore would load neither --
    // it requires a store config file inside the candidate, and the config file is not a
    // directory while the store directory is empty -- but the recording override takes both.
    Future.successful(mgr.storeLoadAttempts.toList should contain((deviceA, storeDir)))

  atest("a device check that fails does not wedge later checks of the same device"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    mgr.loadedDevices.keySet should be(Set(deviceA))

    // deviceA is on disk but absent from the StorageDevicesTree the TestNetwork bootstrapped,
    // so getStorageDeviceState fails with NoSuchElementException. In production that exact
    // state -- a device on disk with no entry in the storage-devices tree -- means a config
    // written out-of-band naming an id that was never registered, or a tree entry removed
    // after the fact; neither is something a command produces. What is routinely reachable
    // is not that state at all but a failed metadata read, which arrives here identically.
    // (A copied or moved config is not one of these: its device is registered, so the lookup
    // succeeds.) The failure must still release the activeDeviceChecks entry.
    mgr.testingOnlyCheckAllDevices()

    yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so this is the assertion that turns an exhausted wait
      // into a failure rather than a pass.
      mgr.testingOnlyActiveDeviceChecks should be(empty)

      // And the release lets a later check of the same device start rather than being skipped
      // forever by the in-progress guard.
      mgr.testingOnlyCheckAllDevices()
      mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

  atest("an armed lookup holds the device check open until the test completes it"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val p = mgr.armLookup(deviceA)

    mgr.testingOnlyCheckAllDevices()

    mgr.lookupAttempts.toList should be(List(deviceA))

    // Drain everything the check queued. A real client read of deviceA fails immediately --
    // deviceA is absent from the storage-devices tree -- so an unarmed check would have
    // released the guard by the time this wait exhausts. Still holding it is what proves the
    // armed promise, and not the client's read, is what the check is waiting on.
    yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).flatMap: _ =>
      mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

      p.failure(new RuntimeException("test-controlled lookup failure"))

      yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
        // yieldUntil gives up silently, so this is the assertion that turns an exhausted wait
        // into a failure rather than a pass.
        mgr.testingOnlyActiveDeviceChecks should be(empty)

  atest("a check started before its device loads does not mark the loaded device's stores offline"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.loadedDevices.keySet should be(empty)

    // Two arms: the lookup held in flight across the load, and the one the deferred request
    // issues once it completes.
    val p1 = mgr.armLookup(deviceA)
    // The lookup the re-dispatch issues. Armed so it does not fall through to the real client.
    val p2 = mgr.armLookup(deviceA)

    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    mgr.loadedDevices.keySet should be(empty)
    mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // The config appears and a later event loads the device while the lookup is outstanding.
    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyCheckAllDevices()
    mgr.loadedDevices.keySet should be(Set(deviceA))

    // That event's own check request collided with the outstanding lookup. It is the request
    // the guard used to discard outright, costing a full checkStorageDevicePeriod.
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // An Active store makes reconcileDeviceState a no-op in every one of its branches, so this
    // pins branch selection alone rather than dragging in store creation or transfers.
    p1.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(mgr.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert its condition first. This also proves the
      // first callback ran, without which the negative assertion below would pass vacuously.
      mgr.lookupAttempts.toList should be(List(deviceA, deviceA))
      mgr.testingOnlyDeferredDeviceChecks should be(empty)

      // The device was loaded before the lookup returned, so its stores must not be marked
      // offline by a decision taken back when it was not. In production nothing would clear
      // them afterwards: tryLoadStore and the LoadStore handler both ran on the way in, and
      // reconcileDeviceState's deleted-stores pass only removes ids recorded in the device's
      // own offlineStores set, which ids marked by this branch never enter.
      mgr.testingOnlyOfflineStores should not contain storeId

  atest("a check for a device that never loads marks its stores offline"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    val p = mgr.armLookup(deviceA)

    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    // Nothing was written under storage-devices/, so the check runs against a device this
    // manager has never loaded -- the case the offline marking exists for.
    mgr.loadedDevices.keySet should be(empty)

    p.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      mgr.testingOnlyActiveDeviceChecks should be(empty)

      // Suppresses TxUnknownStore and ReadResponse(StoreNotFound) for stores on a device that
      // is down. Deleting this marking is silent in production and, until this test, silent in
      // the suite too.
      mgr.testingOnlyOfflineStores should contain(storeId)

  atest("a check request arriving during an in-flight check is deferred, not dropped"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val p1 = mgr.armLookup(deviceA)
    // The lookup the re-dispatch issues. Armed so it does not fall through to the real client.
    val p2 = mgr.armLookup(deviceA)

    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))
    mgr.lookupAttempts.toList should be(List(deviceA))

    // Collides with the outstanding lookup. This is the create-storage-device nudge landing
    // during a periodic sweep, and dropping it costs a full checkStorageDevicePeriod.
    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))
    mgr.lookupAttempts.toList should be(List(deviceA))

    // A third request collapses into the same deferral. The existing lookupAttempts assertion
    // after the drain is what pins Set semantics: a queue or counter would issue two re-checks.
    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    p1.success(deviceState(deviceA))

    yieldUntil(mgr.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      mgr.lookupAttempts.toList should be(List(deviceA, deviceA))
      mgr.testingOnlyDeferredDeviceChecks should be(empty)

  atest("a deferred check still runs when the in-flight lookup fails"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val p1 = mgr.armLookup(deviceA)
    // The lookup the re-dispatch issues. Armed so it does not fall through to the real client.
    val p2 = mgr.armLookup(deviceA)

    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // A failed lookup never reaches the completion-time re-read, so the deferral is the only
    // thing that rescues the request that collided with it.
    p1.failure(new RuntimeException("test-controlled lookup failure"))

    yieldUntil(mgr.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      mgr.lookupAttempts.toList should be(List(deviceA, deviceA))
      mgr.testingOnlyDeferredDeviceChecks should be(empty)
