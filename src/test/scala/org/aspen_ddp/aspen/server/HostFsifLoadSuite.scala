package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.host.{ControlFixture, DeviceMetadata, LockstepThreading}
import org.aspen_ddp.aspen.server.store.backend.DefaultBackendOpener
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
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
  /** Opens to fail with an error that is neither out of space nor a lost store */
  @volatile var failedOpens: Int = 0
  @volatile var opens: Int = 0
  @volatile var onFatals: List[Throwable => Unit] = Nil
  /** Backends opened from now on report hasFailed while this is set, as one that failed-stop does */
  @volatile var reportFailed: Boolean = false
  /** While set, every write, force and truncate of an FSIF store opened from now on fails with ENOSPC */
  @volatile var deviceFull: Boolean = false
  @volatile var lastBackend: Option[FsifBackend] = None
  /** Where FSIF opens run. Synchronous by default, so the open completes, and its outcome is handled, within the
   *  load that starts it; a test may set the production pool, or one that holds the open, instead. */
  @volatile var openEc: ExecutionContext = ExecutionContext.parasitic
  /** Names of the threads FSIF opens ran on */
  @volatile var openThreads: List[String] = Nil

  override def fsifOpenEc: ExecutionContext = openEc

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
    override def markRebuiltStoreActive(storageDeviceId: StorageDeviceId, storeId: StoreId,
                                        gossipStoreOnline: StoreId => Unit) =
      base.markRebuiltStoreActive(storageDeviceId, storeId, gossipStoreOnline)
    override def adoptDevice(storageDeviceId: StorageDeviceId, previousHostId: HostId, devicePath: os.Path,
                             uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                             crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                             markRebuildRequired: os.Path => Unit) =
      base.adoptDevice(storageDeviceId, previousHostId, devicePath, uncleanStores, crlLostStores, markRebuildRequired)
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

  override def openFsifBackend(storeId: StoreId, path: os.Path, layout: FsifLayout,
                               onFatal: Throwable => Unit): Backend =
    opens += 1
    openThreads = Thread.currentThread.getName :: openThreads
    if outOfSpaceOpens > 0 then
      outOfSpaceOpens -= 1
      throw new IOException("No space left on device")
    if failedOpens > 0 then
      failedOpens -= 1
      throw new IOException("injected open failure")
    onFatals = onFatal :: onFatals
    if !os.exists(path / FsifBackend.InitializedMarkerFile) then
      super.openFsifBackend(storeId, path, layout, onFatal)
    else
      val b = new FsifBackend(storeId, path, layout, new FullableFiles(path), FsifBackend.sharedIo, FsifTuning(),
          onFatal):
        override def hasFailed: Boolean = super.hasFailed || FsifSeams.this.reportFailed
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
    val host = new Host(client, HostId.BootstrapHostId, systemId, hostRoot, executionContext,
      new LockstepThreading(executionContext), net.objectCacheFactory, net,
      BackgroundTaskManager.NoBackgroundTaskManager, TestNetwork.TestCRL, net.FinalizerFactory,
      TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
      StoreTransferService.Disabled, StoreRebuildFactory.Filesystem, 2,
      seams = Host.Seams(backends = Some(seams), wrapMetadata = Some(seams.withDeviceStates),
                         gossipStoreOnline = Some(seams.gossipStoreOnline)))
    fsifSeams.put(host, seams)
    host

  private def writeFsifStoreConfig(deviceDir: os.Path): os.Path =
    val storeDir = deviceDir / storeId.directoryName
    os.makeDir.all(storeDir)
    os.write.over(storeDir / StoreConfig.configFilename,
      StoreConfig(storeId, StoreConfig.Fsif(layout.segmentPayloadSize, layout.segmentCount), None).yamlConfig)
    storeDir

  /** Loads the store as tryLoadStore does after a restart, and runs the events that load queues */
  private def load(host: Host): Unit =
    host.loadStoreById(deviceA, storeId)
    host.testingOnlyHandleEvents()

  atest("an FSIF store whose creation was cut off before its marker loads as an empty store"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    // As a crash early in initialize leaves it: data and journal created but still empty, no index, no marker
    os.write(storeDir / FsifRecovery.DataFile, Array[Byte]())
    os.write(storeDir / FsifRecovery.JournalFiles(0), Array[Byte]())
    load(host)

    Future.successful:
      host.getStoreIds should contain(storeId)
      host.control.router.offlineStores should not contain storeId
      os.exists(storeDir / FsifBackend.InitializedMarkerFile) should be(true)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

  atest("an FSIF store without its marker whose files hold no valid index is marked for rebuild, not emptied"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    os.write(storeDir / FsifRecovery.DataFile, Array.fill[Byte](10)(1))
    os.write(storeDir / FsifRecovery.JournalFiles(0), Array.fill[Byte](10)(2))
    load(host)

    Future.successful:
      host.getStoreIds should not contain storeId
      host.control.router.offlineStores should contain(storeId)
      os.exists(storeDir / FsifBackend.InitializedMarkerFile) should be(false)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)
      os.read.bytes(storeDir / FsifRecovery.DataFile).toList should be(List.fill[Byte](10)(1))

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
      host.control.router.offlineStores should contain(storeId)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)

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
      host.control.router.offlineStores should contain(storeId)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

      // Still full on the first retry, the next scan
      host.control.reconciler.scan()
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(2)
      host.getStoreIds should not contain storeId

      // Backed off: the next retry waits two scans
      host.control.reconciler.scan()
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(2)

      host.control.reconciler.scan()
      host.testingOnlyHandleEvents()
      fsif(host).opens should be(3)
      host.getStoreIds should contain(storeId)
      host.control.router.offlineStores should not contain storeId
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

      // Loaded: later scans no longer retry it
      host.control.reconciler.scan()
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
        host.control.reconciler.scan()
        host.testingOnlyHandleEvents()
        fsif(host).opens > before
      // Waits of 1, 2, 4, 8, 16 scans, then the cap: 5 minutes of 10 s scans
      retriedAt.toList should be(List(1, 3, 7, 15, 31, 61, 91))
      host.getStoreIds should not contain storeId
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

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

    Future.successful:
      host.getStoreIds should not contain storeId
      host.control.router.offlineStores should contain(storeId)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)

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
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

      closedOnFatal(new IOException("Input/output error"))
      host.testingOnlyHandleEvents()
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)

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
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

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
        host.control.router.offlineStores should contain(storeId)
        os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)
        os.exists(Backend.crlSaveFile(storeDir)) should be(false)
        os.exists(storeDir / StoreTransfer.TransferOutMarkerFile) should be(false)

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
      // A repair allocates an unmapped ext (Ruling 18); its journal batch hits ENOSPC and freezes the store
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
        os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)
        os.exists(Backend.crlSaveFile(storeDir)) should be(false)
        fsif(host).deviceFull = false
        load(host)
        host.getStoreIds should contain(storeId)

  atest("an FSIF store's recovery runs off the event thread and its outcome is served by an event"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      // Runs the open on another thread, and completes `opened` once that task (which posts BackendOpened) has ended
      val opened = Promise[Unit]()
      val pool = ExecutionContext.global
      fsif(host).openEc = new ExecutionContext:
        def execute(r: Runnable): Unit = pool.execute: () =>
          try r.run()
          finally opened.trySuccess(())
        def reportFailure(t: Throwable): Unit = pool.reportFailure(t)
      val eventThread = Thread.currentThread.getName
      load(host)
      opened.future.map: _ =>
        fsif(host).openThreads.head should not be eventThread
        host.testingOnlyHandleEvents() // BackendOpened, then the LoadStore it queues
        host.getStoreIds should contain(storeId)
        host.control.router.offlineStores should not contain storeId

  atest("a close that arrives while an FSIF store is still opening waits for the open, then closes it served"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
      fsif(host).openEc = new ExecutionContext:
        def execute(r: Runnable): Unit = queued.add(r)
        def reportFailure(t: Throwable): Unit = ()
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
        host.control.router.offlineStores should contain(storeId)
        os.exists(Backend.crlSaveFile(storeDir)) should be(false)
        os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

  /** Holds every FSIF open of `host` until the test runs it */
  private def holdOpens(host: Host): java.util.concurrent.ConcurrentLinkedQueue[Runnable] =
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    fsif(host).openEc = new ExecutionContext:
      def execute(r: Runnable): Unit = queued.add(r)
      def reportFailure(t: Throwable): Unit = ()
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
      host.control.router.isLoading(storeId) should be(true)
      host.control.router.offlineStores should not contain storeId
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, then its LoadStore
      host.getStoreIds should contain(storeId)
      host.control.router.isLoading(storeId) should be(false)

  atest("a store whose open fails answers as unknown again; one out of space stays silent, offline"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      fsif(host).failedOpens = 1
      load(host)
      host.getStoreIds should not contain storeId
      host.control.router.isLoading(storeId) should be(false)
      host.control.router.offlineStores should not contain storeId

      fsif(host).outOfSpaceOpens = 1
      load(host)
      host.getStoreIds should not contain storeId
      host.control.router.isLoading(storeId) should be(false)
      host.control.router.offlineStores should contain(storeId)

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
      host.control.router.isAwaitingDevice(storeId) should be(true)
      host.control.router.offlineStores should not contain storeId
      (host, lookup)

  atest("the stores of a device awaiting its shutdown-marker lookup are silent, and stay so once it reads Shutdown"):
    shutDownDeviceHost().flatMap: (host, lookup) =>
      lookup.success(deviceState(deviceA).copy(status = StorageDeviceState.DeviceStatus.Shutdown))
      handleUntil(host)(host.control.discovery.pendingMarkerResolutions.isEmpty).map: _ =>
        host.control.discovery.pendingMarkerResolutions should be(empty)
        host.getStoreIds should not contain storeId
        host.control.router.isAwaitingDevice(storeId) should be(false)
        host.control.router.offlineStores should contain(storeId)

  atest("the stores of a device awaiting its shutdown-marker lookup load once it reads restarted"):
    shutDownDeviceHost().flatMap: (host, lookup) =>
      val active = deviceState(deviceA,
        Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)))
      fsif(host).deviceStates = Map(deviceA -> active)
      lookup.success(active)
      handleUntil(host)(host.getStoreIds.contains(storeId)).map: _ =>
        host.getStoreIds should contain(storeId)
        host.control.router.isAwaitingDevice(storeId) should be(false)
        host.control.router.isLoading(storeId) should be(false)
        host.control.router.offlineStores should not contain storeId


  /** An FSIF store left offline behind a transfer-out marker, whose device check then finds its entry Active with no
   *  transfer outstanding (an abandoned transfer out) and reinstates it. FSIF opens are queued, not run: returns
   *  once the reinstate has removed the marker and queued the store's open. */
  private def reinstateAbandoned(host: Host, storeDir: os.Path,
                                 queued: java.util.concurrent.ConcurrentLinkedQueue[Runnable]): Future[Unit] =
    given ExecutionContext = executionContext
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().flatMap: _ =>
      os.write.over(storeDir / StoreTransfer.TransferOutMarkerFile, Array.emptyByteArray)
      load(host) // skipped for its marker: offline
      host.control.router.offlineStores should contain(storeId)
      fsif(host).openEc = new ExecutionContext:
        def execute(r: Runnable): Unit = queued.add(r)
        def reportFailure(t: Throwable): Unit = ()
      fsif(host).deviceStates = Map(deviceA -> deviceState(deviceA,
        Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      host.control.reconciler.checkAll()
      yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA)).map: _ =>
        host.control.reconciler.activeDeviceChecks should not contain deviceA
        os.exists(storeDir / StoreTransfer.TransferOutMarkerFile) should be(false)
        queued.size should be(1)

  atest("an FSIF store reinstated after an abandoned transfer out gossips StoreMoved once its open completes"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, storeDir, queued).map: _ =>
      // Still opening: not served, so nothing is gossiped yet
      host.control.router.offlineStores should contain(storeId)
      fsif(host).gossipedOnline should be(Nil)
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, then its LoadStore
      host.getStoreIds should contain(storeId)
      host.control.router.offlineStores should not contain storeId
      fsif(host).gossipedOnline should be(List(storeId))

  atest("an FSIF store reinstated after an abandoned transfer out whose open fails is not gossiped"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, storeDir, queued).map: _ =>
      fsif(host).outOfSpaceOpens = 1
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened: out of space, left offline
      host.getStoreIds should not contain storeId
      host.control.router.offlineStores should contain(storeId)
      fsif(host).gossipedOnline should be(Nil)

  atest("an FSIF store reinstated after an abandoned transfer out whose recovery finds it lost is not gossiped"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, storeDir, queued).map: _ =>
      os.remove(storeDir / FsifRecovery.IndexFiles(0))
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened: StoreLost, marked for rebuild and left offline
      host.getStoreIds should not contain storeId
      host.control.router.offlineStores should contain(storeId)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)
      fsif(host).gossipedOnline should be(Nil)

  atest("a RocksDB store reinstated after an abandoned transfer out gossips StoreMoved within the device check"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = deviceDir / storeId.directoryName
    os.makeDir.all(storeDir)
    os.write.over(storeDir / StoreConfig.configFilename, StoreConfig(storeId, StoreConfig.RocksDB(None), None).yamlConfig)
    os.write.over(storeDir / StoreTransfer.TransferOutMarkerFile, Array.emptyByteArray)
    load(host) // skipped for its marker: offline
    host.control.router.offlineStores should contain(storeId)
    fsif(host).deviceStates = Map(deviceA -> deviceState(deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
    host.control.reconciler.checkAll()
    yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA)).map: _ =>
      host.control.reconciler.activeDeviceChecks should not contain deviceA
      // Opened synchronously, so gossiped by the check itself, before its LoadStore is handled
      host.control.router.offlineStores should not contain storeId
      fsif(host).gossipedOnline should be(List(storeId))
      host.testingOnlyHandleEvents()
      host.getStoreIds should contain(storeId)
      fsif(host).gossipedOnline should be(List(storeId))

  atest("a reinstated FSIF store whose first open is out of space gossips StoreMoved once, when a retry serves it"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, storeDir, queued).map: _ =>
      fsif(host).outOfSpaceOpens = 1
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened: out of space, left offline awaiting space
      host.getStoreIds should not contain storeId
      fsif(host).gossipedOnline should be(Nil)
      host.control.loader.awaitingSpaceHasCallback(storeId) should be(Some(true))

      host.control.reconciler.scan() // the retry: its open is queued
      queued.size should be(1)
      fsif(host).gossipedOnline should be(Nil)
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened served, then its LoadStore
      host.getStoreIds should contain(storeId)
      fsif(host).gossipedOnline should be(List(storeId))

      // Exactly once: later scans and events gossip nothing more
      host.control.reconciler.scan()
      host.testingOnlyHandleEvents()
      queued.size should be(0)
      fsif(host).gossipedOnline should be(List(storeId))

  atest("a reinstated FSIF store dropped from the space wait never runs its gossip, even if loaded later"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
    reinstateAbandoned(host, storeDir, queued).map: _ =>
      fsif(host).outOfSpaceOpens = 1
      queued.poll().run()
      host.testingOnlyHandleEvents() // out of space, awaiting space
      val cfg = os.read.bytes(storeDir / StoreConfig.configFilename)
      os.remove(storeDir / StoreConfig.configFilename)
      host.control.reconciler.scan() // its config is gone: dropped from the wait, no retry
      queued.size should be(0)
      fsif(host).gossipedOnline should be(Nil)

      // A later, unrelated load serves it without the dropped callback
      os.write(storeDir / StoreConfig.configFilename, cfg)
      host.control.reconciler.scan()
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
      host.control.loader.awaitingSpaceHasCallback(storeId) should be(Some(false))

      // The open that succeeds, with a close deferred over it
      val queued = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()
      fsif(host).openEc = new ExecutionContext:
        def execute(r: Runnable): Unit = queued.add(r)
        def reportFailure(t: Throwable): Unit = ()
      load(host)
      val closed = host.closeStore(storeId)
      host.testingOnlyHandleEvents()
      closed.isCompleted should be(false)
      queued.poll().run()
      host.testingOnlyHandleEvents() // BackendOpened, LoadStore, then the deferred ShutdownStore
      closed.map: _ =>
        host.getStoreIds should not contain storeId
        fsif(host).gossipedOnline should be(Nil)
