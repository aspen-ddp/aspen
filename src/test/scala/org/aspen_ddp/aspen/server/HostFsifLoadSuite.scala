package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.host.{Activity, ClosePurpose, CopyFixtures, CopyLayout, DeviceIo, JobKind, Phase}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.host.{ControlFixture, DeviceMetadata, LockstepThreading, Silence, SilenceReason}
import org.aspen_ddp.aspen.server.store.backend.{DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.StoreIncarnationFile
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.store.backend.{CommitState, UpdateForm}
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifFile, FsifFiles, FsifLayout, FsifRecovery,
  FsifTuning, NioFsifFiles}
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.transfer.{StoreTransfer, StoreTransferService}

import java.io.IOException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

/** The seams of a real Host whose FSIF backends are opened through `openFsifBackend`, which it can make fail with
 *  out-of-space and whose `onFatal` callbacks it keeps, so a test can fire one. It also answers device state lookups
 *  from `deviceStates` and records `gossipStoreOnline` calls.
 */
private final class FsifSeams(execCtx: ExecutionContext) extends DefaultBackendOpener(execCtx):
  @volatile var outOfSpaceOpens: Int = 0
  /** Creates to fail with ENOSPC, after StoreCreation has made its scratch copy, as a full device fails one */
  @volatile var outOfSpaceCreates: Int = 0
  @volatile var creates: Int = 0
  /** Opens to fail with an error that is neither out of space nor a lost store */
  @volatile var failedOpens: Int = 0
  @volatile var opens: Int = 0
  @volatile var onFatals: List[Throwable => Unit] = Nil
  /** Backends opened from now on report hasFailed while this is set, as one that failed-stop does */
  @volatile var reportFailed: Boolean = false
  /** While set, every write, force and truncate of an FSIF store opened from now on fails with ENOSPC */
  @volatile var deviceFull: Boolean = false
  @volatile var lastBackend: Option[FsifBackend] = None
  /** LoadStores to fail by throwing from the loadStore seam, before anything is queued, as serveOpened then meets */
  @volatile var throwingLoadStores: Int = 0
  /** While set, an open is queued here instead of submitted to its device's lane, until a test runs it: the open
   *  then runs on the lane at the next drain. Unset (the default), opens are submitted at once. */
  @volatile var openQueue: Option[java.util.concurrent.ConcurrentLinkedQueue[Runnable]] = None
  /** While set, the close of each backend opened from now on starts only once this completes, as a slow device's does */
  @volatile var closeGate: Option[Future[Unit]] = None
  /** Closes started on backends opened under a closeGate */
  @volatile var gatedCloses: Int = 0

  override def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit,
                    io: DeviceIo): Future[Backend] =
    openQueue match
      case None => super.open(storeCfg, storePath, onFatal, io)
      case Some(q) =>
        val p = Promise[Backend]()
        q.add(() => p.completeWith(super.open(storeCfg, storePath, onFatal, io)))
        p.future

  /** Device states the metadata returns instead of reading the tree, which holds none of these devices */
  @volatile var deviceStates: Map[StorageDeviceId, StorageDeviceState] = Map()
  /** One-shot lookups the test completes itself, consumed before `deviceStates` is consulted */
  @volatile var heldLookups: Map[StorageDeviceId, Promise[StorageDeviceState]] = Map()
  /** Stores gossipStoreOnline was called for, most recent first. Recorded only: the test pool cannot be read */
  @volatile var gossipedOnline: List[StoreId] = Nil

  def gossipStoreOnline(storeId: StoreId): Unit =
    gossipedOnline = storeId :: gossipedOnline

  /** `base`, except that a lookup of a device in `deviceStates` returns that state */
  def withDeviceStates(base: DeviceMetadata): DeviceMetadata = new DeviceMetadata:
    override def updateStateForTransferredStore(storeId: StoreId, fromDeviceId: StorageDeviceId,
                                                toDeviceid: StorageDeviceId, gossipRetransmitter: GossipRetransmitter) =
      base.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid, gossipRetransmitter)
    override def readHostStateForEndpoint() = base.readHostStateForEndpoint()
    override def abortLostTransfer(storeId: StoreId) = base.abortLostTransfer(storeId)
    override def markRebuiltStoreActive(storageDeviceId: StorageDeviceId, storeId: StoreId, episode: Option[UUID],
                                        gossipStoreOnline: StoreId => Unit) =
      base.markRebuiltStoreActive(storageDeviceId, storeId, episode, gossipStoreOnline)
    override def adoptDevice(storageDeviceId: StorageDeviceId, previousHostId: HostId, devicePath: os.Path,
                             uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                             crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                             moveToLost: StoreId => Unit) =
      base.adoptDevice(storageDeviceId, previousHostId, devicePath, uncleanStores, crlLostStores, moveToLost)
    override def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
      heldLookups.get(storageDeviceId) match
        case Some(held) =>
          heldLookups -= storageDeviceId
          held.future
        case None =>
          deviceStates.get(storageDeviceId) match
            case Some(state) => Future.successful(state)
            case None => base.lookupStorageDeviceState(storageDeviceId)
    override def readTransferSourceState(fromDeviceId: StorageDeviceId) = base.readTransferSourceState(fromDeviceId)
    override def completeDeviceShutdown(storageDeviceId: StorageDeviceId) =
      base.completeDeviceShutdown(storageDeviceId)

  private def outOfSpace[A](op: => A): A =
    if deviceFull then throw new IOException("No space left on device") else op

  /** NioFsifFiles whose files fail every mutation with ENOSPC while `deviceFull` is set */
  private final class FullableFiles(path: os.Path) extends FsifFiles:
    private val under = new NioFsifFiles(path)
    def exists(name: String): Boolean = under.exists(name)
    def size(name: String): Long = under.size(name)
    def open(name: String): FsifFile =
      val f = under.open(name)
      new FsifFile:
        def size: Long = f.size
        def read(position: Long, length: Int): Array[Byte] = f.read(position, length)
        def write(position: Long, bytes: Array[Byte]): Unit = outOfSpace(f.write(position, bytes))
        def truncate(size: Long): Unit = outOfSpace(f.truncate(size))
        def force(): Unit = outOfSpace(f.force())
        def close(): Unit = f.close()

  override def create(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path, io: DeviceIo): Future[Unit] =
    creates += 1
    if outOfSpaceCreates > 0 then
      outOfSpaceCreates -= 1
      Future.fromTry(scala.util.Try(StoreCreation.createAtomically(storeCfg, storePath, creatingRoot): dir =>
        os.write(dir / "partial", "x")
        throw new IOException("No space left on device")))
    else
      super.create(storeCfg, storePath, creatingRoot, io)

  override def openFsifBackend(storeId: StoreId, path: os.Path, layout: FsifLayout,
                               onFatal: Throwable => Unit, io: DeviceIo): Backend =
    opens += 1
    if outOfSpaceOpens > 0 then
      outOfSpaceOpens -= 1
      throw new IOException("No space left on device")
    if failedOpens > 0 then
      failedOpens -= 1
      throw new IOException("injected open failure")
    onFatals = onFatal :: onFatals
    if !os.isDir(path) || FsifRecovery.isBlank(new NioFsifFiles(path)) then
      super.openFsifBackend(storeId, path, layout, onFatal, io) // StoreLost
    else
      val dataEc = io.dataEc
      val serial = Some(io.serialEc)
      val gate = closeGate
      val b = new FsifBackend(storeId, path, layout, new FullableFiles(path), dataEc, FsifTuning(),
          onFatal, org.aspen_ddp.aspen.server.store.backend.SharedRetryScheduler, serial):
        override def hasFailed: Boolean = super.hasFailed || FsifSeams.this.reportFailed
        override def close(): Future[Unit] = gate match
          case None => super.close()
          case Some(g) =>
            FsifSeams.this.gatedCloses += 1
            g.flatMap(_ => super.close())(using ExecutionContext.parasitic)
      lastBackend = Some(b)
      b

class HostFsifLoadSuite extends IntegrationTestSuite with ControlFixture:

  private val layout = FsifLayout(1024, 64)

  private val fsifSeams = new java.util.IdentityHashMap[Host, FsifSeams]()

  override def subFixtureTeardown(): Unit =
    fsifSeams.clear()
    super.subFixtureTeardown()

  /** What `host` did through its FSIF seams, and the knobs that make it do something else */
  private def fsif(host: Host): FsifSeams = fsifSeams.get(host)

  private def newSeamHost(hostRoot: os.Path, configure: FsifSeams => Unit = _ => ()): Host =
    val seams = new FsifSeams(executionContext)
    configure(seams)
    @volatile var built: Host = null
    def loadStore(id: StorageDeviceId, backend: Backend, maxSliceSize: Option[Long],
                  ida: Option[org.aspen_ddp.aspen.common.ida.IDA],
                  tier: org.aspen_ddp.aspen.server.crl.AdmissionTier): Future[Unit] =
      if seams.throwingLoadStores > 0 then
        seams.throwingLoadStores -= 1
        throw new IllegalStateException("Simulated LoadStore queueing failure")
      built.loadStore(id, backend, maxSliceSize, ida, tier)
    val host = new Host(client, HostId.BootstrapHostId, systemId, hostRoot, executionContext,
      new LockstepThreading(executionContext), net.objectCacheFactory, net,
      BackgroundTaskManager.NoBackgroundTaskManager, TestNetwork.TestCRL, net.FinalizerFactory,
      TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
      StoreTransferService.Disabled, StoreRebuildFactory.Filesystem, 2,
      seams = compared(Host.Seams(backends = Some(seams), wrapMetadata = Some(seams.withDeviceStates),
                         gossipStoreOnline = Some(seams.gossipStoreOnline), loadStore = Some(loadStore))))
    built = host
    runDiscovery(host)
    host
    fsifSeams.put(host, seams)
    host

  private val fsifStoreCfg = StoreConfig(storeId, StoreConfig.Fsif(layout.segmentPayloadSize, layout.segmentCount),
    None, org.aspen_ddp.aspen.common.ida.Replication(3, 2))

  /** Creates the empty FSIF store on the device, as the host's create does (StoreCreation) */
  private def writeFsifStoreConfig(deviceDir: os.Path): os.Path =
    val storeDir = CopyFixtures.freshCopyPath(deviceDir, storeId)
    DefaultBackendOpener.createStore(fsifStoreCfg, storeDir, deviceDir / StoreCreation.CreatingDirectory)
    storeDir

  /** A store directory holding only its config: a store whose backend files are gone */
  private def writeConfigOnly(deviceDir: os.Path): os.Path =
    val storeDir = CopyFixtures.freshCopyPath(deviceDir, storeId)
    os.makeDir.all(storeDir)
    os.write.over(storeDir / StoreConfig.configFilename, fsifStoreCfg.yamlConfig)
    storeDir

  /** Loads the store as tryLoadStore does after a restart, and runs the events that load queues. The copy the test
   *  placed after the device's load is registered first, as a restart's device load would find it */
  private def load(host: Host): Unit =
    host.testingOnlyRescan(deviceA)
    host.loadStoreById(deviceA, storeId)
    host.testingOnlyHandleEvents()

  atest("an FSIF store whose files are gone or empty is marked for rebuild, not served empty (SL D5)"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeConfigOnly(deviceDir)
    // As an incomplete restore leaves it: the config, an empty data file, nothing else
    os.write(storeDir / FsifRecovery.DataFile, Array[Byte]())
    load(host)

    Future.successful:
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)
      os.exists(storeDir / FsifRecovery.IndexFiles(0)) should be(false)

  atest("a create that runs out of space leaves nothing at the store path, and the next device check serves it"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    // A real pool, so createNewStore reads its config
    val created = StoreId(PoolId.BootstrapPoolId, 0)
    val initializing = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Initializing, None)
    fsif(host).outOfSpaceCreates = 1
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(created -> initializing)))
      _ <- waitForTransactionsToComplete()
      _ = host.control.rebuilds.createNewStore(loadedDevices(host)(deviceA), created)
      _ <- handleUntil(host)(fsif(host).creates == 1 && host.control.router.silentStores.contains(created))
      firstLeft = CopyFixtures.copyPath(deviceDir, created).isDefined
      scratchLeft = os.list(deviceDir / StoreCreation.CreatingDirectory).nonEmpty
      _ <- waitForTransactionsToComplete()
      stillInitializing <- client.getStorageDeviceState(deviceA).map(_.stores(created))
      // As the next device check does for an Initializing entry
      _ = host.control.rebuilds.createNewStore(loadedDevices(host)(deviceA), created)
      _ <- handleUntil(host)(host.getStoreIds.contains(created))
    yield
      firstLeft should be(false)
      scratchLeft should be(false)
      stillInitializing should be(initializing)
      fsif(host).creates should be(2)
      host.getStoreIds should contain(created)
      CopyFixtures.copyPath(deviceDir, created).exists(p => os.exists(p / StoreConfig.configFilename)) should be(true)

  atest("a device load removes what a crashed create left in creating/, and never loads it"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    // As a crash before the rename leaves it: a complete-looking store, config included, in the scratch area
    val scratch = deviceDir / StoreCreation.CreatingDirectory / s"${storeId.directoryName}-crashed"
    os.makeDir.all(scratch)
    os.write(scratch / StoreConfig.configFilename, fsifStoreCfg.yamlConfig)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    Future.successful:
      os.exists(deviceDir / StoreCreation.CreatingDirectory) should be(false)
      host.getStoreIds should not contain storeId
      fsif(host).opens should be(0)

  /** Drains `host` until its copy of the suite's store is in lost/, which a close's end moves it to (an FSIF close
   *  completes off the control thread), within 10 s */
  private def lostOnceClosed(host: Host, deviceDir: os.Path): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + 10_000_000_000L
    def loop(): Future[Unit] =
      host.testingOnlyHandleEvents()
      if CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) || System.nanoTime() > deadline then
        Future.unit
      else
        Future(Thread.sleep(1)).flatMap(_ => loop())
    loop()

  atest("an FSIF store whose files hold no valid index is marked for rebuild, not emptied"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeConfigOnly(deviceDir)
    os.write(storeDir / FsifRecovery.DataFile, Array.fill[Byte](10)(1))
    os.write(storeDir / FsifRecovery.JournalFiles(0), Array.fill[Byte](10)(2))
    load(host)

    Future.successful:
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)
      // Moved to lost/ whole, its files untouched
      os.read.bytes(CopyFixtures.copyPath(deviceDir, storeId).get / FsifRecovery.DataFile).toList should be(
        List.fill[Byte](10)(1))

  atest("an FSIF store whose recovery finds it lost is marked for rebuild and left offline"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      os.remove(storeDir / FsifRecovery.IndexFiles(0))
      load(host)
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)

  atest("an FSIF store that cannot open for lack of space stays offline and loads on a later scan"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      fsif(host).outOfSpaceOpens = 2
      load(host)
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)

      // Still full on the first retry, the next scan
      scanNow(host)
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(2)
      host.getStoreIds should not contain storeId

      // Backed off: the next retry waits two scans
      scanNow(host)
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(2)

      scanNow(host)
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(3)
      host.getStoreIds should contain(storeId)
      host.control.router.silentStores should not contain storeId
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)

      // Loaded: later scans no longer retry it
      scanNow(host)
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(3)

  atest("out-of-space load retries back off exponentially, capped at five minutes of scans"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      fsif(host).outOfSpaceOpens = Int.MaxValue
      load(host)
      val retriedAt = (1 to 100).filter: scan =>
        val before = fsif(host).opens
        scanNow(host)
        host.testingOnlyHandleEvents()
        fsif(host).opens > before
      // Waits of 1, 2, 4, 8, 16 scans, then the cap: 5 minutes of 10 s scans
      retriedAt.toList should be(List(1, 3, 7, 15, 31, 61, 91))
      host.getStoreIds should not contain storeId
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)

  atest("a served FSIF store that fails fatally is unloaded, marked for rebuild and left offline"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    load(host)
    host.getStoreIds should contain(storeId)

    fsif(host).onFatals.head(new IOException("Input/output error"))
    host.testingOnlyHandleEvents()

    // Moved to lost/ once its close has ended
    lostOnceClosed(host, deviceDir).map: _ =>
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)

  atest("a fatal failure of an FSIF backend closed and not replaced marks the store for rebuild"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    load(host)
    val closedOnFatal = fsif(host).onFatals.head
    val closed = host.closeStore(storeId)
    host.testingOnlyHandleEvents()
    closed.map: _ =>
      host.getStoreIds should not contain storeId
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)

      closedOnFatal(new IOException("Input/output error"))
      host.testingOnlyHandleEvents()
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)

  atest("a fatal failure of an FSIF backend replaced by a newer one is ignored"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    load(host)
    val staleOnFatal = fsif(host).onFatals.head
    val closed = host.closeStore(storeId)
    host.testingOnlyHandleEvents()
    closed.map: _ =>
      load(host)
      host.getStoreIds should contain(storeId)
      fsif(host).onFatals.size should be(2)

      staleOnFatal(new IOException("Input/output error"))
      host.testingOnlyHandleEvents()
      host.getStoreIds should contain(storeId)
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)

  atest("an FSIF store that fails during its close is marked for rebuild, not closed cleanly"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      load(host)
      host.getStoreIds should contain(storeId)
      // The close's final I/O fails: the backend reports failed by the time its close completes
      fsif(host).reportFailed = true
      val closed = host.closeStore(storeId)
      host.testingOnlyHandleEvents()
      closed.failed.map: err =>
        err should be(Host.StoreFailedOnClose(storeId))
        host.testingOnlyHandleEvents()
        host.getStoreIds should not contain storeId
        host.control.router.silentStores should contain(storeId)
        CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)
        os.exists(Backend.crlSaveFile(storeDir)) should be(false)
        CopyFixtures.phaseOf(deviceDir, storeId).exists(_.isInstanceOf[Phase.Outbound]) should be(false)

  atest("closing an FSIF store frozen by a full device completes without space returning, and it loads again"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      load(host)
      host.getStoreIds should contain(storeId)
      val backend = fsif(host).lastBackend.get
      fsif(host).deviceFull = true
      // A repair allocates an unmapped ext; its journal batch hits ENOSPC and freezes the store
      val id = ObjectId(new UUID(1, 2))
      val ptr = DataObjectPointer(id, storeId.poolId, java.nio.ByteBuffer.allocate(4).putInt(0).array())
      val md = Metadata(ObjectRevision(TransactionId(new UUID(3, 4))), ObjectRefcount(1, 1), HLCTimestamp(1))
      val repaired = Promise[Unit]()
      backend.repair(CommitState(ptr, md, ObjectType.Data, DataBuffer(Array[Byte](1, 2, 3)), form = UpdateForm.Full),
        repaired)
      // Transfer-out and device shutdown close a store this way
      val closed = host.closeStore(storeId)
      host.testingOnlyHandleEvents()
      closed.map: _ =>
        backend.isWriteFrozen should be(true)
        repaired.isCompleted should be(false) // abandoned, never acknowledged
        host.getStoreIds should not contain storeId
        CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)
        os.exists(Backend.crlSaveFile(storeDir)) should be(false)
        fsif(host).deviceFull = false
        load(host)
        host.getStoreIds should contain(storeId)

  atest("an FSIF store's recovery runs as a device lane job and its outcome is served by an event"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      // The load's config read, and the open after it, wait on the device's lane, not in the load that started it
      holdDevice(host, deviceDir)
      load(host)
      heldJobs(host, deviceDir) should be(List(JobKind.ReadConfig))
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      releaseDevice(host, deviceDir)
      host.testingOnlyHandleEvents() // the open, then BackendOpened and the LoadStore it queues
      host.getStoreIds should contain(storeId)
      host.control.router.silentStores should not contain storeId

  atest("a close that arrives while an FSIF store is still opening waits for the open, then closes it served"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
      fsif(host).openQueue = Some(queued)
      load(host)
      queued.size should be(1)
      load(host) // a second load while it opens starts no second open
      queued.size should be(1)
      val closed = host.closeStore(storeId)
      host.testingOnlyHandleEvents()
      closed.isCompleted should be(false) // deferred until the open ends
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, LoadStore, then the deferred ShutdownStore
      closed.map: _ =>
        host.getStoreIds should not contain storeId
        host.control.router.silentStores should contain(storeId)
        os.exists(Backend.crlSaveFile(storeDir)) should be(false)
        CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)

  // Task 1c's parked item: a serveOpened that throws (its LoadStore not queued) ended nothing, so the store stayed
  // opening for good and the closes deferred behind its open were never put again. The open now ends: the copy
  // awaits a load retry, the deferred close completes, and the retry serves the store
  atest("an opened FSIF store whose LoadStore cannot be queued awaits a retry, and the close deferred behind it completes"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      val queued = holdOpens(host)
      // The unserved backend's close is held, so the retry is seen waiting for it
      val gate = Promise[Unit]()
      fsif(host).closeGate = Some(gate.future)
      load(host)
      queued.size should be(1)
      val closed = host.closeStore(storeId)
      host.testingOnlyHandleEvents()
      closed.isCompleted should be(false) // deferred until the open ends
      fsif(host).throwingLoadStores = 1
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, whose serveOpened throws, then the deferred ShutdownStore
      fsif(host).closeGate = None
      def activity = host.control.copyRegistry.device(deviceA).flatMap(_.copies.get(storeId)).map(_.activity)
      val opening = host.control.copyRegistry.isOpening(storeId)
      val waitScans = host.control.loader.awaitingRetryWaitScans(storeId)
      closed.flatMap: _ =>
        opening should be(false)
        waitScans should be(Some(1))
        host.getStoreIds should not contain storeId
        host.control.router.silentStores should contain(storeId)
        // Its copy is held while the unserved backend closes, the retry parked: no second backend opens over it
        fsif(host).gatedCloses should be(1)
        activity should be(Some(Activity.Closing(ClosePurpose.Unserved)))
        host.control.loader.awaitingRetryReady(storeId) should be(Some(false))
        fsif(host).openQueue = None
        val opensBefore = fsif(host).opens
        host.control.loader.retryFailedLoads()
        host.testingOnlyHandleEvents()
        fsif(host).opens should be(opensBefore)
        gate.success(())
        awaitLoudly("the unserved backend's close to give the copy its retry back") {
          host.testingOnlyHandleEvents()
          host.control.loader.awaitingRetryReady(storeId).contains(true)
        }.flatMap: _ =>
          activity.get should be(a[Activity.AwaitingRetry])
          // The retry opens the store again (its first backend was closed) and serves it
          host.control.loader.retryFailedLoads()
          host.testingOnlyHandleEvents()
          awaitLoudly("the retried load to serve the store")(
            { host.testingOnlyHandleEvents(); host.getStoreIds.contains(storeId) }).map: _ =>
              host.control.loader.awaitingRetryWaitScans(storeId) should be(None)
              host.control.router.silentStores should not contain storeId

  // A load that fails with its data intact after its open (StoreLoader.abandonLoad) retries only once its unserved
  // backend has closed: a retry racing that close would open a second backend over the directory
  atest("a store whose LoadStore fails after its open retries only once its unserved backend has closed"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      // Read by the LoadStore, after the open: unparsable, so the load is retried, the file never replaced
      val file = storeDir / StoreIncarnationFile.FileName
      val original = os.read(file)
      os.write.over(file, "not a uuid")
      val gate = Promise[Unit]()
      fsif(host).closeGate = Some(gate.future)
      load(host)
      awaitLoudly("the failed LoadStore to start closing its unserved backend") {
        host.testingOnlyHandleEvents()
        fsif(host).gatedCloses == 1
      }.flatMap: _ =>
        fsif(host).closeGate = None
        def activity = host.control.copyRegistry.device(deviceA).flatMap(_.copies.get(storeId)).map(_.activity)
        host.getStoreIds should not contain storeId
        host.control.router.silentStores should contain(storeId)
        activity should be(Some(Activity.Closing(ClosePurpose.Unserved)))
        host.control.loader.awaitingRetryReady(storeId) should be(Some(false))
        os.write.over(file, original)
        // Its retry waits for the close: no second open
        val opensBefore = fsif(host).opens
        scanNow(host)
        host.testingOnlyHandleEvents()
        fsif(host).opens should be(opensBefore)
        activity should be(Some(Activity.Closing(ClosePurpose.Unserved)))
        gate.success(())
        awaitLoudly("the unserved backend's close to give the copy its retry back") {
          host.testingOnlyHandleEvents()
          host.control.loader.awaitingRetryReady(storeId).contains(true)
        }.flatMap: _ =>
          activity.get should be(a[Activity.AwaitingRetry])
          scanNow(host)
          awaitLoudly("the retried load to serve the store")(
            { host.testingOnlyHandleEvents(); host.getStoreIds.contains(storeId) }).map: _ =>
              fsif(host).opens should be(opensBefore + 1)
              host.control.loader.awaitingRetryReady(storeId) should be(None)

  // The abandoned load's close is one of the closes a device's unload waits for (StoreRebuilds.closeStarted): unloaded
  // before it ends, a reload of the device would offer its Idle Live copy to a second open over the closing backend
  atest("a device is not unloaded while a failed load's unserved backend is still closing"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      val file = storeDir / StoreIncarnationFile.FileName
      os.write.over(file, "not a uuid")
      val gate = Promise[Unit]()
      fsif(host).closeGate = Some(gate.future)
      load(host)
      awaitLoudly("the failed LoadStore to start closing its unserved backend") {
        host.testingOnlyHandleEvents()
        fsif(host).gatedCloses == 1
      }.flatMap: _ =>
        fsif(host).closeGate = None
        host.control.rebuilds.closesRunning(deviceA) should be(true)
        val local = host.control.devices.devices(deviceA)
        // Nothing is open on the device: only the unserved close holds its unload back
        host.control.shutdown.crlFailed(local, new Exception("injected CRL failure"))
        host.testingOnlyHandleEvents()
        host.control.devices.isLoaded(local) should be(true)
        gate.success(())
        awaitLoudly("the device's unload once the close has ended") {
          host.testingOnlyHandleEvents()
          !host.control.devices.isLoaded(local)
        }.map: _ =>
          host.control.rebuilds.closesRunning(deviceA) should be(false)
          host.getStoreIds should not contain storeId

  /** Holds every FSIF open of `host` until the test runs it */
  private def holdOpens(host: Host): java.util.concurrent.ConcurrentLinkedQueue[Runnable] =
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    fsif(host).openQueue = Some(queued)
    queued

  atest("a store is silent while its open runs, until its LoadStore installs it"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      val queued = holdOpens(host)
      load(host)
      queued.size should be(1)
      // Silent while its copy opens
      host.control.copyRegistry.silence(storeId) should be(Silence.Silent(Set(SilenceReason.Opening)))
      host.control.router.silentStores should contain(storeId)
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, then its LoadStore
      host.getStoreIds should contain(storeId)
      host.control.copyRegistry.silence(storeId) should be(Silence.Served)

  // X-W5: an ordinary open failure says nothing about the data, so the store waits offline for a retry as one out of
  // space does, rather than staying unserved and untracked until a restart
  for (what, fail) <- List[(String, FsifSeams => Unit)](
    "fails" -> (_.failedOpens = 2), "runs out of space" -> (_.outOfSpaceOpens = 2)) do
    atest(s"a store whose open $what stays silent, offline, and is retried with backoff until it is served"):
      val hostRoot = newHostDir()
      val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
      val host = newSeamHost(hostRoot)
      host.testingOnlyHandleEvents()
      val storeDir = writeFsifStoreConfig(deviceDir)
      FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
        fail(fsif(host))
        load(host)
        host.getStoreIds should not contain storeId
        host.control.copyRegistry.silence(storeId) should be(Silence.Silent(Set(SilenceReason.CopyHere)))
        host.control.router.silentStores should contain(storeId)
        CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(false)

        // Fails again on the first retry, the next scan, then waits two scans
        scanNow(host)
        host.testingOnlyHandleEvents()
        fsif(host).opens should be(2)
        scanNow(host)
        host.testingOnlyHandleEvents()
        fsif(host).opens should be(2)
        scanNow(host)
        host.testingOnlyHandleEvents()
        fsif(host).opens should be(3)
        host.getStoreIds should contain(storeId)
        host.control.router.silentStores should not contain storeId

  // X-W28: tryLoadStore reads no tree, so a retry waits while the device's latest reconcile found the store's entry
  // neither Active nor Offline, or the store declared missing, and is dropped once the entry has left the device
  atest("a load retry waits while the reconciled entry is not Active or Offline, and serves the store once it is (X-W28)"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      fsif(host).failedOpens = 1
      load(host)
      fsif(host).opens should be(1)
      val sds = host.control.devices.devices(deviceA)
      def scans(n: Int): Unit = (0 until n).foreach: _ =>
        scanNow(host)
        host.testingOnlyHandleEvents()
      def entry(status: StorageDeviceState.StoreStatus): Map[StoreId, StorageDeviceState.StoreEntry] =
        Map(storeId -> StorageDeviceState.StoreEntry(status, None))

      // A transfer out or a rebuild owns the copy: held, its backoff spent
      for status <- List(StorageDeviceState.StoreStatus.TransferringOut, StorageDeviceState.StoreStatus.Rebuilding) do
        sds.reconciledEntries = Some(entry(status))
        scans(3)
        fsif(host).opens should be(1)
        host.control.loader.awaitingRetryWaitScans(storeId) should be(defined)
      // Declared missing by the Missing Stores pass, its entry still Active
      sds.reconciledEntries = Some(entry(StorageDeviceState.StoreStatus.Active))
      sds.declaredMissing += storeId
      scans(1)
      fsif(host).opens should be(1)
      sds.declaredMissing -= storeId
      scans(1)
      fsif(host).opens should be(2)
      host.getStoreIds should contain(storeId)

  atest("a load retry is dropped once the reconciled entries no longer name the store (X-W28)"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      fsif(host).failedOpens = 1
      load(host)
      host.control.devices.devices(deviceA).reconciledEntries = Some(Map())
      scanNow(host)
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(1)
      host.control.loader.awaitingRetryWaitScans(storeId) should be(None)
      host.getStoreIds should not contain storeId

  /** Yields until device A's check has ended, running its lane's copy and reconcile-inputs jobs (runCopyJobs: their
   *  results applied only from the FIFO's head) on the way: this suite's hosts are not newHost's, so yieldUntil does
   *  not run them */
  private def checkEnded(host: Host, rounds: Int = 100): Future[Unit] =
    runCopyJobs(host)
    if !host.control.reconciler.activeDeviceChecks.contains(deviceA) || rounds == 0 then
      Future.unit
    else
      Future(()).flatMap(_ => checkEnded(host, rounds - 1))

  /** Handles `host`'s events, yielding between rounds, until `condition` holds. Silent on exhaustion: the caller
   *  asserts the end state. */
  private def handleUntil(host: Host, rounds: Int = 100)(condition: => Boolean): Future[Unit] =
    host.testingOnlyHandleEvents()
    if condition || rounds == 0 then
      Future.unit
    else
      Future(()).flatMap(_ => handleUntil(host, rounds - 1)(condition))

  /** A host whose device dev0 holds an FSIF store and carries shutdown.state naming this host, and whose lookup of
   *  that device's state is held: returns the host and the lookup to complete */
  private def shutDownDeviceHost(): Future[(Host, Promise[StorageDeviceState])] =
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
    val storeDir = writeFsifStoreConfig(deviceDir)
    DeviceMarkers.writeShutdownState(deviceDir, HostId.BootstrapHostId)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      val lookup = Promise[StorageDeviceState]()
      val host = newSeamHost(hostRoot, _.heldLookups = Map(deviceA -> lookup))
      host.testingOnlyHandleEvents()
      host.control.discovery.pendingMarkerResolutions.size should be(1)
      host.control.copyRegistry.silence(storeId) should be(Silence.Silent(Set(SilenceReason.DeviceUndecided)))
      host.control.router.silentStores should contain(storeId)
      (host, lookup)

  atest("the stores of a device awaiting its shutdown-marker lookup are silent, and stay so once it reads Shutdown"):
    shutDownDeviceHost().flatMap: (host, lookup) =>
      lookup.success(deviceState(deviceA).copy(status = StorageDeviceState.DeviceStatus.Shutdown))
      handleUntil(host)(host.control.discovery.pendingMarkerResolutions.isEmpty).map: _ =>
        host.control.discovery.pendingMarkerResolutions should be(empty)
        host.getStoreIds should not contain storeId
        host.control.copyRegistry.silence(storeId) should be(Silence.Silent(Set(SilenceReason.UnloadedDevice)))
        host.control.router.silentStores should contain(storeId)

  atest("the stores of a device awaiting its shutdown-marker lookup load once it reads restarted"):
    shutDownDeviceHost().flatMap: (host, lookup) =>
      val active = deviceState(deviceA,
        Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)))
      fsif(host).deviceStates = Map(deviceA -> active)
      lookup.success(active)
      handleUntil(host)(host.getStoreIds.contains(storeId)).map: _ =>
        host.getStoreIds should contain(storeId)
        host.control.copyRegistry.silence(storeId) should be(Silence.Served)
        host.control.router.silentStores should not contain storeId


  /** An FSIF store left offline in outbound/, whose device check then finds its entry Active with no transfer
   *  outstanding (an abandoned transfer out) and reinstates it. FSIF opens are queued, not run: returns once the
   *  reinstate has moved it back to Live and queued the store's open. */
  private def reinstateAbandoned(host: Host, deviceDir: os.Path, storeDir: os.Path,
                                 queued: java.util.concurrent.ConcurrentLinkedQueue[Runnable]): Future[Unit] =
    given ExecutionContext = executionContext
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      CopyFixtures.markCopy(deviceDir, storeId, Phase.Outbound(""))
      load(host) // skipped in outbound/: offline
      host.control.router.silentStores should contain(storeId)
      fsif(host).openQueue = Some(queued)
      fsif(host).deviceStates = Map(deviceA -> deviceState(deviceA,
        Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      checkAllNow(host)
      checkEnded(host).map: _ =>
        host.control.reconciler.activeDeviceChecks should not contain deviceA
        CopyFixtures.phaseOf(deviceDir, storeId).exists(_.isInstanceOf[Phase.Outbound]) should be(false)
        // The load's config read, on the device's lane, queues the open
        host.testingOnlyHandleEvents()
        queued.size should be(1)

  atest("an FSIF store reinstated after an abandoned transfer out gossips StoreMoved once its open completes"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, deviceDir, storeDir, queued).flatMap: _ =>
      // Still opening: not served, so nothing is gossiped yet
      host.control.router.silentStores should contain(storeId)
      fsif(host).gossipedOnline should be(Nil)
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, then its LoadStore
      host.getStoreIds should contain(storeId)
      host.control.router.silentStores should not contain storeId
      // Once the LoadStore's future, completed by its recovery, calls back
      yieldUntil(fsif(host).gossipedOnline.nonEmpty).map: _ =>
        fsif(host).gossipedOnline should be(List(storeId))

  atest("an FSIF store reinstated after an abandoned transfer out whose open fails is not gossiped"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, deviceDir, storeDir, queued).map: _ =>
      fsif(host).outOfSpaceOpens = 1
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened: out of space, left offline
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      fsif(host).gossipedOnline should be(Nil)

  atest("an FSIF store reinstated after an abandoned transfer out whose recovery finds it lost is not gossiped"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, deviceDir, storeDir, queued).map: _ =>
      os.remove(storeDir / FsifRecovery.IndexFiles(0))
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened: StoreLost, marked for rebuild and left offline
      host.getStoreIds should not contain storeId
      host.control.router.silentStores should contain(storeId)
      CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)
      fsif(host).gossipedOnline should be(Nil)

  // X-W15: the gossip waits for the LoadStore to serve the store, not for the open that queues it
  atest("an FSIF store reinstated after an abandoned transfer out whose LoadStore finds its save file lost is not gossiped"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, deviceDir, storeDir, queued).flatMap: _ =>
      // Read by the LoadStore, after the open has succeeded
      os.write.over(Backend.crlSaveFile(storeDir), Array[Byte](1, 2, 3))
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, then its LoadStore: the save file is unusable
      // Moved to lost/ once its unserved backend has closed. Time for any callback of the failed load to run
      lostOnceClosed(host, deviceDir).flatMap(_ => yieldUntil(fsif(host).gossipedOnline.nonEmpty, maxYields = 10)).map: _ =>
        CopyFixtures.phaseOf(deviceDir, storeId).contains(Phase.Lost) should be(true)
        host.getStoreIds should not contain storeId
        host.control.router.silentStores should contain(storeId)
        fsif(host).gossipedOnline should be(Nil)

  atest("a reinstated FSIF store whose LoadStore leaves it awaiting a retry gossips StoreMoved once, when a retry serves it"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, deviceDir, storeDir, queued).flatMap: _ =>
      CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState(Nil), Backend.crlSaveFile(storeDir))
      queued.poll().run()
      // The loaded crl_save.log cannot be unlinked: its LoadStore unloads the store and leaves it awaiting a retry.
      // Its incarnation file was written at its creation, so the read-only directory fails nothing before that removal
      os.perms.set(storeDir, "r-x------")
      val waited = Future.unit.flatMap: _ =>
        host.testingOnlyHandleEvents()
        // Its CRL load completes off the control context (CrashRecoveryLog.loadStore), then the failed removal
        // posts SaveFileRemovalFailed
        // The retry is parked until the unloaded store's close ends (closeUnloaded): the scan below considers it only
        // once its copy's record awaits it
        awaitLoudly("the failed save-file removal to leave the store awaiting a retry, its close ended") {
          host.testingOnlyHandleEvents()
          host.control.loader.awaitingRetryHasCallback(storeId).contains(true) &&
            host.control.loader.awaitingRetryReady(storeId).contains(true)
        }.map: _ =>
          host.getStoreIds should not contain storeId
          host.control.router.silentStores should contain(storeId)
          host.control.loader.awaitingRetryHasCallback(storeId) should be(Some(true))
          fsif(host).gossipedOnline should be(Nil)
      waited.andThen(_ => os.perms.set(storeDir, "rwx------")).flatMap: _ =>
        scanNow(host) // the retry
        host.testingOnlyHandleEvents() // its config read: its open is queued
        queued.size should be(1)
        queued.poll().run()
        host.testingOnlyHandleEvents()
        host.getStoreIds should contain(storeId)
        awaitLoudly("the retried load's gossip") {
          host.testingOnlyHandleEvents()
          fsif(host).gossipedOnline.nonEmpty
        }.map: _ =>
          os.exists(Backend.crlSaveFile(storeDir)) should be(false)
          fsif(host).gossipedOnline should be(List(storeId))

  atest("a RocksDB store reinstated after an abandoned transfer out gossips StoreMoved within the device check"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = CopyFixtures.freshCopyPath(deviceDir, storeId)
    DefaultBackendOpener.createStore(StoreConfig(storeId, StoreConfig.RocksDB(None), None, org.aspen_ddp.aspen.common.ida.Replication(3, 2)), storeDir,
      storeDir / os.up / os.up / StoreCreation.CreatingDirectory)
    CopyFixtures.markCopy(deviceDir, storeId, Phase.Outbound(""))
    load(host) // skipped in outbound/: offline
    host.control.router.silentStores should contain(storeId)
    fsif(host).deviceStates = Map(deviceA -> deviceState(deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
    checkAllNow(host)
    checkEnded(host).flatMap: _ =>
      host.control.reconciler.activeDeviceChecks should not contain deviceA
      // Opened synchronously, but not gossiped until its LoadStore has served it (X-W15): silent while it opens
      host.control.copyRegistry.silence(storeId) should be(Silence.Silent(Set(SilenceReason.Opening)))
      host.getStoreIds should not contain storeId
      fsif(host).gossipedOnline should be(Nil)
      host.testingOnlyHandleEvents()
      host.getStoreIds should contain(storeId)
      yieldUntil(fsif(host).gossipedOnline.nonEmpty).map: _ =>
        fsif(host).gossipedOnline should be(List(storeId))

  // X-W8: a config read failure must not leave the store unserved and untracked until a restart
  atest("a store whose config cannot be read is left offline and retried at the device scans until it loads"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val cfg = os.read.bytes(storeDir / StoreConfig.configFilename)
    os.write.over(storeDir / StoreConfig.configFilename, "not: [a store config")
    load(host)
    host.getStoreIds should not contain storeId
    host.control.router.silentStores should contain(storeId)
    host.control.loader.awaitingRetryHasCallback(storeId) should be(Some(false))

    // Still unreadable: kept waiting
    scanNow(host)
    host.testingOnlyHandleEvents()
    host.getStoreIds should not contain storeId
    host.control.loader.awaitingRetryHasCallback(storeId) should be(Some(false))

    os.write.over(storeDir / StoreConfig.configFilename, cfg)
    // The failed retry doubled the wait: the first scan only counts it down
    scanNow(host)
    host.testingOnlyHandleEvents()
    host.getStoreIds should not contain storeId
    scanNow(host)
    host.testingOnlyHandleEvents()
    Future.successful:
      host.getStoreIds should contain(storeId)
      host.control.router.silentStores should not contain storeId
      host.control.loader.awaitingRetryHasCallback(storeId) should be(None)

  atest("a reinstated store whose config cannot be read gossips StoreMoved once, when a retry serves it"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = CopyFixtures.freshCopyPath(deviceDir, storeId)
    DefaultBackendOpener.createStore(StoreConfig(storeId, StoreConfig.RocksDB(None), None, org.aspen_ddp.aspen.common.ida.Replication(3, 2)), storeDir,
      storeDir / os.up / os.up / StoreCreation.CreatingDirectory)
    val outboundDir = CopyFixtures.markCopy(deviceDir, storeId, Phase.Outbound(""))
    load(host) // skipped in outbound/: offline
    val cfg = os.read.bytes(outboundDir / StoreConfig.configFilename)
    os.write.over(outboundDir / StoreConfig.configFilename, "not: [a store config")
    fsif(host).deviceStates = Map(deviceA -> deviceState(deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
    checkAllNow(host)
    checkEnded(host).flatMap: _ =>
      // The load's config read, on the device's lane, fails
      host.testingOnlyHandleEvents()
      // Moved back to Live in live/ (T7), so only the retry can take the store up again
      CopyFixtures.phaseOf(deviceDir, storeId).exists(_.isInstanceOf[Phase.Outbound]) should be(false)
      CopyFixtures.copyPath(deviceDir, storeId) should be(Some(storeDir))
      host.control.router.silentStores should contain(storeId)
      host.control.loader.awaitingRetryHasCallback(storeId) should be(Some(true))
      fsif(host).gossipedOnline should be(Nil)

      os.write.over(storeDir / StoreConfig.configFilename, cfg)
      scanNow(host)
      host.testingOnlyHandleEvents()
      host.getStoreIds should contain(storeId)
      yieldUntil(fsif(host).gossipedOnline.nonEmpty).map: _ =>
        fsif(host).gossipedOnline should be(List(storeId))

  atest("a reinstated FSIF store whose first open is out of space gossips StoreMoved once, when a retry serves it"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, deviceDir, storeDir, queued).flatMap: _ =>
      fsif(host).outOfSpaceOpens = 1
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened: out of space, left offline awaiting space
      host.getStoreIds should not contain storeId
      fsif(host).gossipedOnline should be(Nil)
      host.control.loader.awaitingRetryHasCallback(storeId) should be(Some(true))

      scanNow(host) // the retry
      host.testingOnlyHandleEvents() // its config read: its open is queued
      queued.size should be(1)
      fsif(host).gossipedOnline should be(Nil)
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened served, then its LoadStore
      host.getStoreIds should contain(storeId)
      yieldUntil(fsif(host).gossipedOnline.nonEmpty).flatMap: _ =>
        fsif(host).gossipedOnline should be(List(storeId))

        // Exactly once: later scans and events gossip nothing more
        scanNow(host)
        host.testingOnlyHandleEvents()
        queued.size should be(0)
        yieldUntil(fsif(host).gossipedOnline.size > 1, maxYields = 10).map: _ =>
          fsif(host).gossipedOnline should be(List(storeId))

  atest("a reinstated FSIF store dropped from the space wait never runs its gossip, even if loaded later"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, deviceDir, storeDir, queued).map: _ =>
      fsif(host).outOfSpaceOpens = 1
      queued.poll().run()
      host.testingOnlyHandleEvents() // out of space, awaiting space
      val cfg = os.read.bytes(storeDir / StoreConfig.configFilename)
      os.remove(storeDir / StoreConfig.configFilename)
      scanNow(host) // its config is gone: dropped from the wait, no retry
      queued.size should be(0)
      fsif(host).gossipedOnline should be(Nil)

      // A later, unrelated load serves it without the dropped callback
      os.write(storeDir / StoreConfig.configFilename, cfg)
      scanNow(host)
      queued.size should be(0) // no longer waiting
      load(host)
      queued.poll().run()
      host.testingOnlyHandleEvents()
      host.getStoreIds should contain(storeId)
      fsif(host).gossipedOnline should be(Nil)

  atest("many out-of-space retries of an FSIF store build no callback chain, and a close deferred over its open completes"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      val retries = 10000
      fsif(host).outOfSpaceOpens = retries
      // Each load is an open that takes over the wait entry, as a retry's does, without the scans of backoff between
      (1 to retries).foreach(_ => load(host))
      fsif(host).opens should be(retries)
      host.getStoreIds should not contain storeId
      host.control.loader.awaitingRetryHasCallback(storeId) should be(Some(false))

      // The open that succeeds, with a close deferred over it
      val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
      fsif(host).openQueue = Some(queued)
      load(host)
      val closed = host.closeStore(storeId)
      host.testingOnlyHandleEvents()
      closed.isCompleted should be(false)
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, LoadStore, then the deferred ShutdownStore
      closed.map: _ =>
        host.getStoreIds should not contain storeId
        fsif(host).gossipedOnline should be(Nil)
