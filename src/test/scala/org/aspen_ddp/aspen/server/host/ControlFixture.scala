package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.{DeviceMarkers, GossipRetransmitter, Host, StorageDeviceConfig, StorageDeviceManager}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{AdoptOutcome, RebuildOutcome, ShutdownOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.transfer.StoreTransferService

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

/** What a test host did, recorded by the seams it was built with, and the knobs that make it do something else.
 *
 *  Build the host with `seams(scheduler)`, then call `attach` with it: the seams that stand in front of the host's
 *  own behaviour (loadStore, closeForTransferOut and the like) reach it through that reference. They are only called
 *  once the host has been constructed. The metadata seams do not need it, so the host's own constructor scan is
 *  recorded too.
 *
 *  Replaces the recordings of the former RecordingHost, which overrode Host's protected methods. Nothing here loads
 *  a store: `tryLoadStore` records the candidate path and does nothing else, so it never touches the router, the
 *  loaded devices or the offline set. A test asserting that it put something there would pass vacuously; assert on
 *  `storeLoadAttempts` instead. */
final class RecordingSeams(ec: ExecutionContext, failFirstStoreLoad: Boolean = false):
  @volatile private var host: Host = null

  def attach(h: Host): Unit = host = h

  private def attached: Host =
    if host == null then throw new IllegalStateException("RecordingSeams used before attach")
    host

  /** Number of upcoming reconcileEndpoint reads to fail. */
  val failEndpointReads = new AtomicInteger(0)

  /** (deviceId, path) for every path tryLoadStore was invoked with, in call order.
   *
   *  These are candidate paths, not confirmed stores. tryLoadDevice offers every child of the device directory, so
   *  a successfully loaded device always contributes at least one non-store entry: its own
   *  `aspen-storage-device-config.yaml`. */
  val storeLoadAttempts = mutable.ListBuffer[(StorageDeviceId, os.Path)]()

  /** Every store gossipStoreOnline was called for, in call order. The call then goes on to the host's own. */
  val gossipedOnline = mutable.ListBuffer[StoreId]()

  private def gossipStoreOnline(storeId: StoreId): Unit =
    synchronized(gossipedOnline += storeId)
    attached.control.services.gossipStoreOnline(storeId)

  /** True while a simulated store-load failure is still owed. The first tryLoadStore call to see it set clears it
   *  and throws, modelling a device that dies part-way through loading its stores (a yanked hot-plug disk, an IO
   *  error). */
  private val storeLoadFailureOwed = new AtomicBoolean(failFirstStoreLoad)

  /** Storage device ids passed to lookupStorageDeviceState, in call order. */
  val lookupAttempts = mutable.ListBuffer[StorageDeviceId]()

  /** One armed lookup: `Right` returns a Future the test completes, `Left` throws before any Future exists. */
  private type ArmedLookup = Either[Throwable, Promise[StorageDeviceState]]

  /** Lookups queued by armLookup and armLookupThrow, consumed one per lookup of that device. One queue per device
   *  rather than a queue of promises plus a separate set of throws, so the order in which a test arms a success and
   *  a throw is the order the device check sees them. */
  private val armedLookups = mutable.Map[StorageDeviceId, mutable.Queue[ArmedLookup]]()

  private def enqueueArmed(deviceId: StorageDeviceId, armed: ArmedLookup): Unit = synchronized:
    armedLookups.getOrElseUpdate(deviceId, mutable.Queue[ArmedLookup]()).enqueue(armed)

  /** Arms one lookup of `deviceId` to return a Future the test completes when it chooses. Call once per lookup the
   *  test intends to control, in the order they will be issued. Lookups beyond the armed ones fall through to the
   *  real client. */
  def armLookup(deviceId: StorageDeviceId): Promise[StorageDeviceState] =
    val p = Promise[StorageDeviceState]()
    enqueueArmed(deviceId, Right(p))
    p

  /** Arms one lookup of `deviceId` to throw `error` instead of returning a Future at all. This is the case no
   *  Promise can stage: a failed Promise still yields a Future, and it is the absence of the Future -- and so of the
   *  callback, and so of the callback's finally -- that leaks startDeviceCheck's guard entry. */
  def armLookupThrow(deviceId: StorageDeviceId, error: Throwable): Unit =
    enqueueArmed(deviceId, Left(error))

  /** (path, message) for every device-directory warning the host actually raised, in order. warnOnce suppresses
   *  repeats before calling the seam, so this holds one entry per directory per problem episode. */
  val deviceDirWarnings = mutable.ListBuffer[(os.Path, String)]()

  /** Monotonic counter for stamping rebuild event sequences. Incremented each time an event (metadata flip or load
   *  decision) occurs. */
  val rebuildEventSeq = new AtomicInteger(0)

  /** Sequence number stamped when markRebuiltStoreActive's returned Future completes, by (deviceId, storeId). Used
   *  to verify "loading is downstream of the metadata decision": the flip's stamp must be strictly less than the
   *  load's. Stamped at completion, not at call, because the decision exists only once the Future completes. */
  val rebuildFlipSeq = mutable.Map[(StorageDeviceId, StoreId), Int]()

  /** Sequence number stamped at the FIRST loadStoreById call for each (deviceId, storeId). First-wins, not
   *  last-write-wins: an unconditional rogue load before the flip would overwrite its own stamp with a later
   *  legitimate one, hiding the inversion. */
  val loadStoreByIdSeq = mutable.Map[(StorageDeviceId, StoreId), Int]()

  /** (deviceId, storeId) for every loadStoreById call, in call order. Recorded here rather than through
   *  storeLoadAttempts because loadStoreById only enqueues an event that a test may not drain. */
  val loadStoreByIdRequests = mutable.ListBuffer[(StorageDeviceId, StoreId)]()

  /** The outcome of every completed markRebuiltStoreActive, by (deviceId, storeId). */
  val rebuildOutcomes = mutable.Map[(StorageDeviceId, StoreId), RebuildOutcome]()

  /** The outcome of every completed post-transfer metadata update, by store. Recorded rather than awaited directly:
   *  the failure mode these tests guard against is a Future that never completes, and a test that awaited one would
   *  hang the suite instead of failing it. */
  val transferOutcomes = mutable.Map[StoreId, TransferOutcome]()

  private val transferOutcomeGate = new AtomicReference[Option[Promise[Unit]]](None)

  /** Holds every later post-transfer metadata update until the returned promise is completed, holding a completed
   *  transfer between promotion and its metadata outcome. */
  def holdTransferOutcomes(): Promise[Unit] =
    val p = Promise[Unit]()
    transferOutcomeGate.set(Some(p))
    p

  /** (deviceId, storeId) for every loadStore call, in order. */
  val loadStoreRequests = mutable.ListBuffer[(StorageDeviceId, StoreId)]()

  private val storeLoadGate = new AtomicReference[Option[Promise[Unit]]](None)

  /** Makes every later loadStore return a Future completed by the returned promise, so a test can run work between
   *  createNewStore loading a store and its Initializing -> Active flip. The LoadStore event itself is still
   *  queued, as it is without the hold. */
  def holdStoreLoads(): Promise[Unit] =
    val p = Promise[Unit]()
    storeLoadGate.set(Some(p))
    p

  /** Every store the shutdown close was called for, in call order. */
  val shutdownCloses = mutable.ListBuffer[StoreId]()

  private val shutdownCloseGate = new AtomicReference[Option[Promise[Unit]]](None)

  /** Holds every later shutdown close open until the returned promise is completed. */
  def holdShutdownCloses(): Promise[Unit] =
    val p = Promise[Unit]()
    shutdownCloseGate.set(Some(p))
    p

  /** Stores whose shutdown close fails as a ShutdownStore does when the backend failed-stop during it, leaving the
   *  store's directory (on the host's loaded devices) marked rebuild-required as StoreRebuilds.failedOnClose does. */
  val failingShutdownCloses = new AtomicReference[Set[StoreId]](Set())

  /** Every store the transfer-out close was called for, in call order. */
  val transferOutCloses = mutable.ListBuffer[StoreId]()

  private val transferOutCloseGate = new AtomicReference[Option[Promise[Unit]]](None)

  /** Holds every later transfer-out close open until the returned promise is completed. */
  def holdTransferOutCloses(): Promise[Unit] =
    val p = Promise[Unit]()
    transferOutCloseGate.set(Some(p))
    p

  /** Makes the next transfer-out close fail as a ShutdownStore does when the backend failed-stop during it, after
   *  running the given function, which leaves the store as StoreRebuilds.failedOnClose would. */
  val failNextTransferOutClose = new AtomicReference[Option[() => Unit]](None)

  /** Every abortLostTransfer call, in order. */
  val lostTransferAborts = mutable.ListBuffer[StoreId]()

  /** Every outcome completeDeviceShutdown returned, in order. */
  val shutdownOutcomes = mutable.ListBuffer[ShutdownOutcome]()

  private val beforeShutdownCommit = new AtomicReference[Option[() => Future[Unit]]](None)

  /** Runs `hook` once, before the next completeDeviceShutdown reads the device state: a window in which a rebuild
   *  or transfer in could flip an entry to Active. */
  def beforeNextShutdownCommit(hook: () => Future[Unit]): Unit = beforeShutdownCommit.set(Some(hook))

  /** Every transfer-in source read that has completed, in order. Recorded before Host's own continuation runs, so on
   *  the suite's serial ExecutionContext a test that has seen the record and then yields has also seen whatever Host
   *  did with the read. */
  val transferSourceReads = mutable.ListBuffer[StorageDeviceId]()

  /** (deviceId, previousHostId) for every adoptDevice call, in order. */
  val adoptCalls = mutable.ListBuffer[(StorageDeviceId, HostId)]()

  /** Records each metadata call on the way to `base`, the metadata the host would have used. */
  private final class RecordingMetadata(base: DeviceMetadata) extends DeviceMetadata:
    given ExecutionContext = ec

    override def updateStateForTransferredStore(storeId: StoreId,
                                                fromDeviceId: StorageDeviceId,
                                                toDeviceid: StorageDeviceId,
                                                gossipRetransmitter: GossipRetransmitter): Future[TransferOutcome] =
      val f = transferOutcomeGate.get match
        case None => base.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid, gossipRetransmitter)
        case Some(gate) =>
          gate.future.flatMap: _ =>
            base.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid, gossipRetransmitter)
      f.foreach: outcome =>
        synchronized:
          transferOutcomes += storeId -> outcome
      f

    override def readHostStateForEndpoint(): Future[HostState] =
      if failEndpointReads.getAndUpdate(n => math.max(0, n - 1)) > 0 then
        Future.failed(new Exception("Simulated host state read failure"))
      else
        base.readHostStateForEndpoint()

    override def abortLostTransfer(storeId: StoreId): Future[Boolean] =
      synchronized:
        lostTransferAborts += storeId
      base.abortLostTransfer(storeId)

    override def markRebuiltStoreActive(storageDeviceId: StorageDeviceId,
                                        storeId: StoreId,
                                        gossipStoreOnline: StoreId => Unit): Future[RebuildOutcome] =
      base.markRebuiltStoreActive(storageDeviceId, storeId, gossipStoreOnline).map: outcome =>
        synchronized:
          rebuildFlipSeq += ((storageDeviceId, storeId) -> rebuildEventSeq.incrementAndGet())
          rebuildOutcomes += ((storageDeviceId, storeId) -> outcome)
        outcome

    override def adoptDevice(storageDeviceId: StorageDeviceId,
                             previousHostId: HostId,
                             devicePath: os.Path,
                             uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                             crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                             markRebuildRequired: os.Path => Unit): Future[AdoptOutcome] =
      synchronized:
        adoptCalls += ((storageDeviceId, previousHostId))
      base.adoptDevice(storageDeviceId, previousHostId, devicePath, uncleanStores, crlLostStores, markRebuildRequired)

    override def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
      val armed = synchronized:
        lookupAttempts += storageDeviceId
        armedLookups.get(storageDeviceId).filter(_.nonEmpty).map(_.dequeue())

      armed match
        case Some(Right(p))    => p.future
        case Some(Left(error)) => throw error
        case None              => base.lookupStorageDeviceState(storageDeviceId)

    override def readTransferSourceState(fromDeviceId: StorageDeviceId): Future[StorageDeviceState] =
      base.readTransferSourceState(fromDeviceId).map: state =>
        synchronized:
          transferSourceReads += fromDeviceId
        state

    override def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome] =
      val f = beforeShutdownCommit.getAndSet(None) match
        case None => base.completeDeviceShutdown(storageDeviceId)
        case Some(hook) => hook().flatMap(_ => base.completeDeviceShutdown(storageDeviceId))
      f.map: outcome =>
        synchronized:
          shutdownOutcomes += outcome
        outcome

  /** Every shutdown-work timeout a shutdown has scheduled, by device, in order. Recorded rather than timed: nothing
   *  fires them by itself, so a test fires them with `fireShutdownWorkTimeouts`. */
  val shutdownWorkTimeouts = mutable.ListBuffer[(StorageDeviceId, () => Unit)]()

  /** Runs every recorded shutdown-work timeout, as if shutdownWorkTimeout had elapsed. */
  def fireShutdownWorkTimeouts(): Unit =
    synchronized(shutdownWorkTimeouts.toList).foreach(_._2())

  private def scheduleShutdownWorkTimeout(storageDeviceId: StorageDeviceId, fn: () => Unit): Unit =
    synchronized:
      shutdownWorkTimeouts += ((storageDeviceId, fn))

  /** Records the call; does nothing else. A failure owed by `failFirstStoreLoad` is thrown before recording. */
  private def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: os.Path, onServed: Option[() => Unit]): Unit =
    synchronized:
      if storeLoadFailureOwed.getAndSet(false) then
        throw new RuntimeException(s"Simulated store load failure for $potentialStoreFile")
      storeLoadAttempts += ((sds.storageDeviceId, potentialStoreFile))

  private def loadStoreById(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    synchronized:
      loadStoreByIdRequests += ((storageDeviceId, storeId))
      val key = (storageDeviceId, storeId)
      if !loadStoreByIdSeq.contains(key) then
        loadStoreByIdSeq += (key -> rebuildEventSeq.incrementAndGet())
    attached.loadStoreById(storageDeviceId, storeId)

  private def loadStore(storageDeviceId: StorageDeviceId,
                        backend: org.aspen_ddp.aspen.server.store.backend.Backend,
                        maxSliceSize: Option[Long]): Future[Unit] =
    synchronized:
      loadStoreRequests += ((storageDeviceId, backend.storeId))
    val queued = attached.loadStore(storageDeviceId, backend, maxSliceSize)
    storeLoadGate.get.map(_.future).getOrElse(queued)

  /** Records the close and completes it (or holds it, see holdShutdownCloses) without touching the event queue. The
   *  real close queues a ShutdownStore event, and the stores the tests name were never opened anyway. Host does its
   *  own offline bookkeeping once the close completes, so nothing is lost by skipping it. */
  private def closeForShutdown(storeId: StoreId): Future[Unit] =
    synchronized:
      shutdownCloses += storeId
    if failingShutdownCloses.get.contains(storeId) then
      attached.control.devices.devices.valuesIterator.map(_.devicePath / storeId.directoryName).filter(os.isDir)
        .foreach(dir => os.write.over(dir / RebuildingStore.RebuildRequiredMarkerFile, ""))
      Future.failed(Host.StoreFailedOnClose(storeId))
    else
      shutdownCloseGate.get.map(_.future).getOrElse(Future.unit)

  private def closeForTransferOut(storeId: StoreId): Future[Unit] =
    synchronized:
      transferOutCloses += storeId
    failNextTransferOutClose.getAndSet(None) match
      case Some(leave) =>
        leave()
        Future.failed(Host.StoreFailedOnClose(storeId))
      case None =>
        transferOutCloseGate.get.map(_.future).getOrElse(attached.control.transfers.closeForTransferOut(storeId))

  /** The seams that make a Host record and, where a test asks, hold or fail what the old RecordingHost did. `None`
   *  fields keep the production behaviour; the schedule of retries and timeouts goes through `scheduler`. */
  def seams(scheduler: ControlScheduler): Host.Seams =
    Host.Seams(
      wrapMetadata = Some(base => new RecordingMetadata(base)),
      scheduler = Some(scheduler),
      warn = Some((path, msg) => synchronized(deviceDirWarnings += ((path, msg)))),
      closeForTransferOut = Some(closeForTransferOut),
      closeForShutdown = Some(closeForShutdown),
      loadStore = Some(loadStore),
      tryLoadStore = Some(tryLoadStore),
      loadStoreById = Some(loadStoreById),
      gossipStoreOnline = Some(gossipStoreOnline),
      scheduleShutdownWorkTimeout = Some(scheduleShutdownWorkTimeout))

/** Shared test fixture for the Host's components: builds Hosts with LockstepThreading, a ManualControlScheduler and
 *  RecordingSeams, plus helpers for writing device configs and waiting on async conditions. Temp directory cleanup is
 *  automatic via subFixtureTeardown.
 *
 *  A component is driven directly (`host.control.reconciler.checkAll()`) rather than through a queued event, so a
 *  test need not drain unrelated queued work, and read through the accessors on `host.control`. */
trait ControlFixture:
  this: IntegrationTestSuite =>

  /** The Aspen system that the host under test and the device configs written to disk both belong to, so those
   *  devices are accepted. */
  protected val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")

  protected val tempRoots = mutable.ListBuffer[os.Path]()

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach(deleteTree)
    tempRoots.clear()

  /** Removes a tree. Failures are ignored; these are temp dirs. */
  protected def deleteTree(root: os.Path): Unit =
    try os.remove.all(root)
    catch case _: Throwable => ()

  /** Returns a Future that completes once `condition` holds, re-testing it after every yield back to the test's
   *  ExecutionContext, and giving up after `maxYields` of them.
   *
   *  ScalaTest's async ExecutionContext is single threaded and is the same thread that runs the test body: queued
   *  work only runs while the test is waiting on the Future it returned. A sleeping wait here would therefore
   *  starve the very callback being waited for; yielding lets everything queued ahead of us run instead.
   *
   *  Giving up is silent: the returned Future completes with unit either way, so an exhausted wait is
   *  indistinguishable from a satisfied one. That is intentional -- asserting the condition afterwards reports what
   *  was actually wrong far better than a timeout would -- but it means callers MUST assert the condition
   *  themselves or they get a false pass. */
  protected def yieldUntil(condition: => Boolean, maxYields: Int = 100): Future[Unit] =
    if condition || maxYields == 0 then
      Future.unit
    else
      Future(()).flatMap(_ => yieldUntil(condition, maxYields - 1))

  /** Drives `host`'s device check until no store on `deviceId` is Rebuilding.
   *
   *  Rebuilds are started by the device-check machinery, which in production runs on an hourly timer. Tests do not
   *  wait an hour: this drives checks, yields to let the async rebuilds and their completion callbacks run, and
   *  repeats until nothing is Rebuilding any more.
   *
   *  The predicate is "nothing is Rebuilding", not "everything is Active": a store that lands in some other status
   *  is a result this helper should return so the caller's assertion can report it, not a condition to spin on until
   *  the round budget runs out.
   *
   *  Silent on exhaustion, like `yieldUntil`: the caller asserts the end state. */
  protected def completeRebuilds(host: Host,
                                 deviceId: StorageDeviceId,
                                 maxRounds: Int = 500): Future[Unit] =
    given ExecutionContext = executionContext
    def anyRebuilding(): Future[Boolean] =
      client.getStorageDeviceState(deviceId).map: state =>
        state.stores.values.exists(_.status == StorageDeviceState.StoreStatus.Rebuilding)

    def loop(rounds: Int): Future[Unit] =
      if rounds == 0 then
        Future.unit
      else
        anyRebuilding().flatMap:
          case false => Future.unit
          case true =>
            host.control.reconciler.checkAll()
            for
              // Yield so queued work -- the rebuild's own Future and its callbacks -- gets to run.
              _ <- Future(())
              _ <- waitForTransactionsToComplete()
              _ <- loop(rounds - 1)
            yield ()

    loop(maxRounds)

  /** Creates `<tmp>/host/storage-devices` and returns the host root directory. Pass
   *  `withStorageDevicesDir = false` to leave the `storage-devices` child absent. */
  protected def newHostDir(withStorageDevicesDir: Boolean = true): os.Path =
    val base = os.temp.dir(prefix = "aspen-device-discovery", deleteOnExit = false)
    tempRoots += base
    val hostDir = base / "host"
    if withStorageDevicesDir then
      os.makeDir.all(hostDir / StorageDeviceManager.StorageDevicesDirName)
    else
      os.makeDir.all(hostDir)
    hostDir

  /** Creates `<hostRoot>/storage-devices/<name>` with a device config naming `deviceId`. */
  protected def writeDevice(hostRoot: os.Path,
                            name: String,
                            deviceId: StorageDeviceId,
                            sysId: UUID = systemId,
                            owner: Option[HostId] = Some(HostId.BootstrapHostId)): os.Path =
    val dir = StorageDeviceManager.deviceDirectory(hostRoot, name)
    os.makeDir.all(dir)
    writeDeviceConfig(dir, deviceId, sysId, owner)
    dir

  /** Writes the config file, and the `owning-host` file when `owner` is given, into an existing device
   *  directory. */
  protected def writeDeviceConfig(deviceDir: os.Path,
                                  deviceId: StorageDeviceId,
                                  sysId: UUID = systemId,
                                  owner: Option[HostId] = Some(HostId.BootstrapHostId)): Unit =
    os.write.over(deviceDir / StorageDeviceConfig.configFilename, StorageDeviceConfig(deviceId, sysId).yamlConfig)
    owner.foreach(DeviceMarkers.writeOwningHost(deviceDir, _))

  private final class Rig(val seams: RecordingSeams, val scheduler: ManualControlScheduler)

  private val rigs = new java.util.IdentityHashMap[Host, Rig]()

  /** Builds a Host with LockstepThreading, a ManualControlScheduler and RecordingSeams.
   *
   *  Of the events the constructor posts, the initial CheckAllDevices is dropped unless `keepCheckAllDevices` (it
   *  would race a test's own device checks) and InitializeTaskExecutor unless `keepInitializeTaskExecutor` (draining
   *  it starts a real task-executor bootstrap). Everything else the constructor's device scan queued (store loads,
   *  marker resolutions, adoption continuations) stays queued for the test to drain. */
  protected def newHost(hostRoot: os.Path,
                        failFirstStoreLoad: Boolean = false,
                        ec: ExecutionContext = executionContext,
                        transferService: StoreTransferService = StoreTransferService.Disabled,
                        storeRebuildFactory: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                        maxConcurrentRebuilds: Int = 2,
                        hostId: HostId = HostId.BootstrapHostId,
                        keepCheckAllDevices: Boolean = false,
                        keepInitializeTaskExecutor: Boolean = false): Host =
    val threading = new LockstepThreading(ec)
    val scheduler = new ManualControlScheduler(threading.controlEvents)
    val recording = new RecordingSeams(ec, failFirstStoreLoad)
    val host = new Host(client, hostId, systemId, hostRoot, ec, threading,
                        net.objectCacheFactory, net, BackgroundTaskManager.NoBackgroundTaskManager,
                        TestNetwork.TestCRL, net.FinalizerFactory, TransactionDriver.noErrorRecoveryFactory,
                        Duration(5, SECONDS), Duration(60, SECONDS), transferService, storeRebuildFactory,
                        maxConcurrentRebuilds, seams = recording.seams(scheduler))
    recording.attach(host)
    threading.removeQueued:
      case t: Host.EventTask =>
        (!keepCheckAllDevices && t.event.isInstanceOf[Host.CheckAllDevices]) ||
          (!keepInitializeTaskExecutor && t.event.isInstanceOf[Host.InitializeTaskExecutor])
      case _ => false
    rigs.put(host, new Rig(recording, scheduler))
    host

  /** What `host` did, and the knobs that make it do something else. */
  protected def recorded(host: Host): RecordingSeams = rigs.get(host).seams

  /** The scheduler holding `host`'s delayed work. */
  protected def scheduler(host: Host): ManualControlScheduler = rigs.get(host).scheduler

  /** Installs `sds` as a loaded device without it ever having been on disk.
   *
   *  The callback-body throw test needs a LocalStorageDeviceState carrying values the real scan cannot produce -- a
   *  relative devicePath -- so it cannot go through writeDevice and a rescan.
   *
   *  Named for what it does to the load state, not the storage-devices tree: this registers nothing there, and a
   *  lookup of the injected device still resolves however the test arms it. Refuses to replace an existing entry, so
   *  a test that both writes a device to disk and injects one for the same id fails here rather than in a confusing
   *  assertion later. */
  protected def injectLoadedDevice(host: Host, sds: LocalStorageDeviceState): Unit =
    val devices = host.control.devices
    require(!devices.devices.contains(sds.storageDeviceId),
            s"${sds.storageDeviceId} is already loaded; injectLoadedDevice does not replace")
    devices.devices += (sds.storageDeviceId -> sds)
    host.control.publishView()

  /** The devices `host` has loaded. */
  protected def loadedDevices(host: Host): Map[StorageDeviceId, LocalStorageDeviceState] =
    host.control.devices.devices

  /** How many device directories have a shutdown-marker lookup or adoption in flight (or resolved and not yet
   *  acted on). */
  protected def queuedMarkerResolutions(host: Host): Int =
    host.control.discovery.pendingMarkerResolutions.size

  /** Runs every transfer-in retry waiting out its delay, then drains the events they post. */
  protected def retryTransferIns(host: Host): Unit =
    scheduler(host).runWhere(_ == StoreTransfers.TransferInRetryDelay)
    host.testingOnlyHandleEvents()

  /** Runs every device re-check waiting out its delay, then drains the events they post. */
  protected def runDeviceRechecks(host: Host): Unit =
    scheduler(host).runWhere(_ == Host.TransferOutRecheckDelay)
    host.testingOnlyHandleEvents()

  private def isMarkerResolution(task: Runnable): Boolean = task match
    case t: Host.EventTask => t.event.isInstanceOf[Host.DeviceOwnershipResolved]
    case _ => false

  private def lockstep(host: Host): LockstepThreading = host.threading match
    case t: LockstepThreading => t
    case other => throw new IllegalStateException(s"Needs LockstepThreading, not ${other.getClass.getSimpleName}")

  /** How many DeviceOwnershipResolved events are queued. */
  protected def queuedMarkerEvents(host: Host): Int =
    lockstep(host).queuedItems.count(isMarkerResolution)

  /** Removes the queued DeviceOwnershipResolved events and handles them, leaving every other event queued. */
  protected def handleMarkerEvents(host: Host): Unit =
    lockstep(host).removeQueued(isMarkerResolution).foreach(_.run())

  protected val deviceA = StorageDeviceId(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"))
  protected val deviceB = StorageDeviceId(UUID.fromString("bbbbbbbb-0000-0000-0000-000000000002"))

  /** A store of pool 1111...:index 0. Used for its `directoryName`, which is the on-disk name of a store directory
   *  within a device directory. */
  protected val storeId = StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), 0.toByte)

  protected val deviceSetId = StorageDeviceSetId(UUID.fromString("55555555-5555-5555-5555-555555555555"))

  /** A StorageDeviceState for `deviceId` owned by this host, carrying `stores`.
   *
   *  BootstrapHostId matches the host's own hostId, which keeps reconcileDeviceState off its host-migration branch.
   *  The sizes are arbitrary; nothing under test reads them. */
  protected def deviceState(deviceId: StorageDeviceId,
                            stores: Map[StoreId, StorageDeviceState.StoreEntry] = Map()): StorageDeviceState =
    StorageDeviceState(deviceId, HostId.BootstrapHostId, 0L, 1024L, stores, deviceSetId)
