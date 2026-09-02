package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}
import org.aspen_ddp.aspen.server.transfer.StoreTransferFactory
import org.aspen_ddp.aspen.server.rebuild.StoreRebuildFactory

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

/** Shared test harness for StoreManager device discovery and rebuild tests.
 *
 *  Provides a RecordingStoreManager that logs tryLoadStore calls instead of opening RocksDB
 *  backends, plus helpers for writing device configs, arming lookups, and waiting for async
 *  conditions to hold.
 *
 *  Suites that mix this in must override subFixtureTeardown to call the cleanup:
 *  ```scala
 *  override def subFixtureTeardown(): Unit =
 *    tempRoots.foreach(deleteTree)
 *    tempRoots.clear()
 *  ```
 */
trait StoreManagerTestHarness:
  this: IntegrationTestSuite =>

  /** The Aspen system that the manager under test and the device configs written to disk both
   *  belong to, so those devices are accepted. */
  protected val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")

  protected val tempRoots = mutable.ListBuffer[Path]()

  /** Removes a tree. Failures are ignored; these are temp dirs. */
  protected def deleteTree(root: Path): Unit =
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
  protected def yieldUntil(condition: => Boolean, maxYields: Int = 100): Future[Unit] =
    if condition || maxYields == 0 then
      Future.unit
    else
      Future(()).flatMap(_ => yieldUntil(condition, maxYields - 1))

  /** Creates `<tmp>/host/storage-devices` and returns the host root directory. Pass
   *  `withStorageDevicesDir = false` to leave the `storage-devices` child absent. */
  protected def newHostDir(withStorageDevicesDir: Boolean = true): Path =
    val base = Files.createTempDirectory("aspen-device-discovery")
    tempRoots += base
    val hostDir = base.resolve("host")
    if withStorageDevicesDir then
      Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    else
      Files.createDirectories(hostDir)
    hostDir

  /** Creates `<hostRoot>/storage-devices/<name>` with a device config naming `deviceId`. */
  protected def writeDevice(hostRoot: Path,
                            name: String,
                            deviceId: StorageDeviceId,
                            sysId: UUID = systemId): Path =
    val dir = StorageDeviceManager.deviceDirectory(hostRoot, name)
    Files.createDirectories(dir)
    writeDeviceConfig(dir, deviceId, sysId)
    dir

  /** Writes only the config file into an existing device directory. */
  protected def writeDeviceConfig(deviceDir: Path,
                                  deviceId: StorageDeviceId,
                                  sysId: UUID = systemId): Unit =
    Files.write(
      deviceDir.resolve(StorageDeviceConfig.configFilename),
      StorageDeviceConfig(deviceId, sysId).yamlConfig.getBytes(StandardCharsets.UTF_8))

  protected def newManager(hostRoot: Path,
                           failFirstStoreLoad: Boolean = false,
                           ec: ExecutionContext = executionContext,
                           transferFactory: StoreTransferFactory = StoreTransferFactory.Filesystem,
                           storeRebuildFactory: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                           maxConcurrentRebuilds: Int = 2
                          ): RecordingStoreManager =
    new RecordingStoreManager(client, systemId, hostRoot, ec,
                              net.objectCacheFactory, net, net.FinalizerFactory,
                              failFirstStoreLoad, transferFactory, storeRebuildFactory,
                              maxConcurrentRebuilds)

  protected val deviceA = StorageDeviceId(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"))
  protected val deviceB = StorageDeviceId(UUID.fromString("bbbbbbbb-0000-0000-0000-000000000002"))

  /** A store of pool 1111...:index 0. Used for its `directoryName`, which is the on-disk name of
   *  a store directory within a device directory. */
  protected val storeId = StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), 0.toByte)

  protected val deviceSetId = StorageDeviceSetId(UUID.fromString("55555555-5555-5555-5555-555555555555"))

  /** A StorageDeviceState for `deviceId` owned by this manager's host, carrying `stores`.
   *
   *  BootstrapHostId matches the manager's own hostId, which keeps reconcileDeviceState off
   *  its host-migration branch. The sizes are arbitrary; nothing under test reads them.
   */
  protected def deviceState(deviceId: StorageDeviceId,
                            stores: Map[StoreId, StorageDeviceState.StoreEntry] = Map()): StorageDeviceState =
    StorageDeviceState(deviceId, HostId.BootstrapHostId, 0L, 1024L, stores, deviceSetId)

/** A StoreManager that records tryLoadStore calls instead of opening RocksDB backends, and
 *  exposes the protected device map to assertions.
 *
 *  The override records and, when `failFirstStoreLoad` is set, throws once before recording
 *  anything. It does nothing else. In particular it does not replicate the real
 *  tryLoadStore's bookkeeping: it never filters on `StoreConfig.configFilename`, never
 *  honours a TransferringOut marker, and never updates `sds.loadedStores`, `sds.offlineStores`
 *  or the manager's `offlineStores`. A test asserting that this override put something in them
 *  would therefore pass vacuously — assert on `storeLoadAttempts` instead. Assertions about
 *  what the *device check* writes to `offlineStores` are a different matter and are not
 *  vacuous: that write comes from startDeviceCheck's callback, which this override does not
 *  touch.
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
                            failFirstStoreLoad: Boolean = false,
                            transferFactory: StoreTransferFactory = StoreTransferFactory.Filesystem,
                            rebuildFactory: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                            maxRebuilds: Int = 2)
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
    Duration(60, SECONDS),
    transferFactory,
    rebuildFactory,
    maxRebuilds):

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

  /** One armed lookup: `Right` returns a Future the test completes, `Left` throws before any
   *  Future exists. */
  private type ArmedLookup = Either[Throwable, Promise[StorageDeviceState]]

  /** Lookups queued by armLookup and armLookupThrow, consumed one per lookup of that device.
   *
   *  One queue per device rather than a queue of promises plus a separate set of throws, so the
   *  order in which a test arms a success and a throw is the order the device check sees them.
   *
   *  Lazy to match storeLoadAttempts. Unlike that field, nothing in StoreManager's constructor
   *  reaches this override today: the constructor's device scan calls tryLoadStore, but a
   *  device check only ever runs from handleEvent. The uniformity is deliberate insurance
   *  against that changing.
   */
  private lazy val armedLookups: mutable.Map[StorageDeviceId, mutable.Queue[ArmedLookup]] =
    mutable.Map[StorageDeviceId, mutable.Queue[ArmedLookup]]()

  /** Caller holds this instance's lock. Write path only -- the read path in
   *  lookupStorageDeviceState must go through `armedLookups` directly, since getOrElseUpdate
   *  there would accumulate an empty queue per device looked up.
   */
  private def enqueueArmed(deviceId: StorageDeviceId, armed: ArmedLookup): Unit =
    armedLookups.getOrElseUpdate(deviceId, mutable.Queue[ArmedLookup]()).enqueue(armed)

  /** Arms one lookup of `deviceId` to return a Future the test completes when it chooses.
   *
   *  Call once per lookup the test intends to control, in the order they will be issued.
   *  Lookups beyond the armed ones fall through to the real client, which is what keeps the
   *  tests that rely on a genuine lookup failure working unchanged.
   */
  def armLookup(deviceId: StorageDeviceId): Promise[StorageDeviceState] = synchronized:
    val p = Promise[StorageDeviceState]()
    enqueueArmed(deviceId, Right(p))
    p

  /** Arms one lookup of `deviceId` to throw `error` instead of returning a Future at all.
   *
   *  This is the case no Promise can stage: a failed Promise still yields a Future, and it is
   *  the absence of the Future -- and so of the callback, and so of the callback's finally --
   *  that leaks startDeviceCheck's guard entry.
   */
  def armLookupThrow(deviceId: StorageDeviceId, error: Throwable): Unit = synchronized:
    enqueueArmed(deviceId, Left(error))

  override protected def lookupStorageDeviceState(deviceId: StorageDeviceId): Future[StorageDeviceState] =
    lookupAttempts += deviceId
    armedLookups.get(deviceId).flatMap(_.dequeueFirst(_ => true)) match
      case Some(Left(err)) => throw err
      case Some(Right(p)) => p.future
      case None => super.lookupStorageDeviceState(deviceId)

  def loadedDevices: Map[StorageDeviceId, StoreManager.LocalStorageDeviceState] =
    synchronized { storageDevices }

  /** Storage device ids passed to updateHostId, in call order.
   *
   *  Recorded rather than executed: the real updateHostId transacts against the host object,
   *  and the case under test is precisely the one where that host does not exist. Lazy for the
   *  same initialization-order reason as storeLoadAttempts.
   */
  lazy val hostIdClaims: mutable.ListBuffer[StorageDeviceId] =
    mutable.ListBuffer[StorageDeviceId]()

  /** (deviceId, storeId) for every loadStoreById call, in call order.
   *
   *  Recorded here rather than through storeLoadAttempts because loadStoreById only enqueues an
   *  event, and this suite never runs the manager's event loop -- so an assertion on
   *  storeLoadAttempts would pass vacuously whether the call was made or not. This records the
   *  decision itself. Lazy for the same initialization-order reason as storeLoadAttempts.
   */
  lazy val loadStoreByIdRequests: mutable.ListBuffer[(StorageDeviceId, StoreId)] =
    mutable.ListBuffer[(StorageDeviceId, StoreId)]()

  override def loadStoreById(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    synchronized:
      loadStoreByIdRequests += ((storageDeviceId, storeId))
    super.loadStoreById(storageDeviceId, storeId)

  /** The outcome of every completed post-transfer metadata update, by store.
   *
   *  Recorded rather than awaited directly: the failure mode these tests guard against is a
   *  Future that never completes, and a test that awaited one would hang the suite instead of
   *  failing it. Lazy for the same initialization-order reason as storeLoadAttempts.
   */
  lazy val transferOutcomes: mutable.Map[StoreId, StoreManager.TransferOutcome] =
    mutable.Map[StoreId, StoreManager.TransferOutcome]()

  override protected def updateStateForTransferredStore(
      storeId: StoreId,
      fromDeviceId: StorageDeviceId,
      toDeviceId: StorageDeviceId): Future[StoreManager.TransferOutcome] =
    val f = super.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceId)
    f.foreach: outcome =>
      synchronized:
        transferOutcomes += storeId -> outcome
    f

  override protected def updateHostId(storageDeviceId: StorageDeviceId): Future[Unit] =
    synchronized:
      hostIdClaims += storageDeviceId
    Future.unit

  /** Simulates injectLoadedDevice for tests that need a device already present. */
  def injectLoadedDevice(deviceId: StorageDeviceId, path: Path): Unit =
    synchronized:
      val configFile = path.resolve(StorageDeviceConfig.configFilename).toFile
      storageDevices += deviceId -> new StoreManager.LocalStorageDeviceState(
        deviceId, path, configFile)

  /** Installs `sds` in the protected device map without it ever having been on disk.
   *
   *  The callback-body throw test needs a LocalStorageDeviceState carrying values the real scan
   *  cannot produce -- a relative devicePath -- so it cannot go through writeDevice and a
   *  rescan.
   *
   *  Named for what it does to the load state, not the storage-devices tree: this registers
   *  nothing there, and a lookup of the injected device still resolves however the test arms
   *  it. Refuses to replace an existing entry, so a test that both writes a device to disk and
   *  injects one for the same id fails here rather than in a confusing assertion later.
   */
  def injectLoadedDevice(sds: StoreManager.LocalStorageDeviceState): Unit = synchronized:
    require(!storageDevices.contains(sds.storageDeviceId),
            s"${sds.storageDeviceId} is already loaded; injectLoadedDevice does not replace")
    storageDevices += (sds.storageDeviceId -> sds)
