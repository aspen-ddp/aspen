package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, Host, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, StoreRecoveryState}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{RebuildOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.transfer.StoreTransferService
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendStateLost, DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Failure, Success, Try}

/** The hosts, devices, openers and drivers the copy phase suites share (PhaseDirectoriesSuite, CopyCrashSuite): a
 *  ControlFixture host that loads its stores for real, device A or the second device with a CRL directory, and the
 *  waits that drain a host until a condition holds. A crash is a fresh host built over the same host directory, the
 *  old one dropped without shutdown */
trait CopyHostFixture extends ControlFixture:
  this: IntegrationTestSuite =>

  protected val s = storeId
  protected val ida = Replication(3, 2)

  /** The default opener, whose RocksDB backends report hasFailed while `failing` is set, close only once `closeGate`
   *  (if set when the close is asked) completes, and whose opens fail with BackendStateLost while `stateLost` is set */
  protected class TestOpener extends DefaultBackendOpener(executionContext):
    @volatile var failing = false
    @volatile var stateLost = false
    @volatile var closeGate: Option[Future[Unit]] = None
    @volatile var openGate: Option[Future[Unit]] = None
    @volatile var onFatals: List[Throwable => Unit] = Nil
    val opens = new AtomicInteger(0)
    override def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit,
                       io: DeviceIo): Future[Backend] =
      opens.incrementAndGet()
      onFatals = onFatal :: onFatals
      def backend: Future[Backend] =
        io.submit(JobKind.BackendOpen, Some(storeCfg.storeId))(new RocksDBBackend(storePath, storeCfg.storeId, executionContext):
          override def hasFailed: Boolean = super.hasFailed || TestOpener.this.failing
          override def close(): Future[Unit] = closeGate match
            case None => super.close()
            case Some(gate) => gate.flatMap(_ => super.close())(using executionContext)).future
      if stateLost then
        Future.failed(new BackendStateLost("injected: acknowledged state lost"))
      else
        // An open held by `openGate` completes once it does
        openGate match
          case None => backend
          case Some(gate) => gate.flatMap(_ => backend)(using executionContext)

  /** A CRL whose next `failReads` reads of a store's state, next `failDrops` drops and next `failLoads` loads of a
   *  saved state fail */
  protected class FlakyCrl extends TestNetwork.TestCRL:
    val failReads = new AtomicInteger(0)
    val failDrops = new AtomicInteger(0)
    val failLoads = new AtomicInteger(0)
    override def loadStore(storeId: StoreId, state: StoreRecoveryState): Future[Unit] =
      if failLoads.getAndUpdate(n => math.max(0, n - 1)) > 0 then Future.failed(new java.io.IOException("injected load"))
      else super.loadStore(storeId, state)
    override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
      if failReads.getAndUpdate(n => math.max(0, n - 1)) > 0 then Future.failed(new java.io.IOException("injected read"))
      else super.readStoreState(storeId)
    override def dropStore(storeId: StoreId): Future[Unit] =
      if failDrops.getAndUpdate(n => math.max(0, n - 1)) > 0 then Future.failed(new java.io.IOException("injected drop"))
      else super.dropStore(storeId)

  protected class FlakyCrlFactory extends CrashRecoveryLogFactory:
    val crl = new FlakyCrl
    def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
      os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devicePath))
      crl
    def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit = ()

  /** A host root with device A, its CRL directory present (so its CRL is not lost) unless not `withCrl` */
  protected def device(withCrl: Boolean = true): (os.Path, os.Path) =
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", deviceA)
    if withCrl then
      os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    (hostRoot, dev)

  /** A real RocksDB store of `sid`, Live on the device */
  protected def rocksCopy(dev: os.Path, sid: StoreId): os.Path =
    val dir = CopyFixtures.freshCopyPath(dev, sid)
    DefaultBackendOpener.createStore(StoreConfig(sid, StoreConfig.RocksDB(None), None, ida), dir,
      dev / StoreCreation.CreatingDirectory)
    dir

  /** A ControlFixture host that loads its stores for real, through `opener` (its recording load seams, which reach the
   *  host only once it is built, left out: its device load opens and loads its stores) */
  protected def realHost(hostRoot: os.Path, opener: TestOpener,
                       rename: Option[(os.Path, os.Path) => Unit] = None,
                       rebuilds: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                       transfers: StoreTransferService = StoreTransferService.Disabled,
                       threadingOf: ExecutionContext => LockstepThreading = new LockstepThreading(_),
                       crlFactory: CrashRecoveryLogFactory = TestNetwork.TestCRL): Host =
    newHost(hostRoot, storeRebuildFactory = rebuilds, transferService = transfers, crlFactory = crlFactory,
      adjustSeams = _.copy(tryLoadStore = None, loadStore = None, loadStoreById = None, backends = Some(opener),
        copyRename = rename), threadingOf = threadingOf)

  /** Drains `host` and yields until `condition`, `rounds` drains (when not negative) or 10 s. The caller asserts */
  protected def pump(host: Host, condition: => Boolean, rounds: Int = -1): Future[Unit] =
    val deadline = System.nanoTime() + Duration(10, SECONDS).toNanos
    def loop(done: Int): Future[Unit] =
      host.testingOnlyHandleEvents()
      if condition || rounds == done || (rounds < 0 && System.nanoTime() - deadline > 0) then
        Future.unit
      else
        if done > 2000 then
          Thread.sleep(1)
        Future(())(using executionContext).flatMap(_ => loop(done + 1))(using executionContext)
    loop(0)

  protected def lostDir(dev: os.Path): os.Path = dev / CopyLayout.LostDirectory

  /** A device check of device A reading `state`'s entries */
  protected def check(host: Host, entries: Map[StoreId, org.aspen_ddp.aspen.common.metadata.StorageDeviceState.StoreEntry]
                    = Map()): Future[Unit] =
    val armed = recorded(host).armLookup(deviceA)
    host.control.reconciler.checkStorageDevice(deviceA)
    armed.success(deviceState(deviceA, entries))
    pump(host, host.control.reconciler.activeDeviceChecks.isEmpty && !host.control.copies.isRetiring(s))

  /** As pump, also letting the client's transactions complete each round */
  protected def pumpTx(host: Host, condition: => Boolean, rounds: Int = -1): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + Duration(10, SECONDS).toNanos
    def loop(done: Int): Future[Unit] =
      host.testingOnlyHandleEvents()
      if condition || rounds == done || (rounds < 0 && System.nanoTime() - deadline > 0) then
        Future.unit
      else
        waitForTransactionsToComplete().flatMap(_ => loop(done + 1))
    loop(0)

  // A store of the bootstrap pool, whose pool entry names the bootstrap device: startOut reads both fresh
  protected val shipped = StoreId(PoolId.BootstrapPoolId, 1)
  protected val source = StorageDeviceId.BootstrapStorageDeviceId

  protected def ask(host: Host): Future[Try[Unit]] =
    given ExecutionContext = executionContext
    host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, source, shipped,
      HLCTimestamp.now, UUID.randomUUID())).transform(Success(_))

  /** A host over the bootstrap device holding `shipped` as `place` leaves it, its sends staged, and the store's
   *  transfer to the second device started in the tree: yields the host, its device, the device path and the
   *  transfer's id */
  protected def transferring(place: os.Path => Unit, sends: StagedSendService)
      : Future[(Host, LocalStorageDeviceState, os.Path, String)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", source)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    place(dev)
    val host = newHost(hostRoot, transferService = sends)
    host.testingOnlyHandleEvents()
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      st <- client.getStorageDeviceState(source)
    yield (host, loadedDevices(host)(source), dev, st.stores(shipped).transferId.get.toString)

  /** Asks `host` to ship `shipped`, and waits for the request's handling to end: its close's work too */
  protected def askAndSettle(host: Host): Future[Try[Unit]] =
    given ExecutionContext = executionContext
    for
      started <- ask(host)
      _ <- awaitLoudly("the transfer out's work to end"):
             host.testingOnlyHandleEvents()
             !host.control.copyRegistry.transferringOut(shipped, TransferOutStage.Closing) &&
               !host.control.copyRegistry.transferringOut(shipped, TransferOutStage.AbortingLost)
    yield started

  protected def recordOf(host: Host, local: LocalStorageDeviceState, storeId: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.copiesOf(local).flatMap(_.copies.get(storeId))

  /** Records every rename of `host`'s moves, failing those `fail` names */
  protected def recordRenames(host: Host, fail: (os.Path, os.Path) => Boolean = (_, _) => false)
      : () => Vector[(os.Path, os.Path)] =
    @volatile var seen = Vector.empty[(os.Path, os.Path)]
    host.control.moves.rename = (from, to) =>
      seen :+= (from, to)
      if fail(from, to) then throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    () => seen

  /** A rebuild that, once started, places its copy in rebuilt/ with its episode as RebuildingStore.finish does (a
   *  real RocksDB store with its empty save file when `real`, else a config only), and completes with its copy id */
  protected class PlacingRebuilds(real: Boolean) extends StoreRebuildFactory:
    def createRebuild(client: org.aspen_ddp.aspen.client.AspenClient, storeId: StoreId, deviceId: StorageDeviceId,
                      devicePath: os.Path, backends: org.aspen_ddp.aspen.server.store.backend.BackendOpener, io: org.aspen_ddp.aspen.server.host.DeviceIo,
                      episode: Option[UUID], existingCopy: Option[(Phase, os.Path)]): StoreRebuild =
      val id = storeId
      val rebuilt = Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode))
      val copy =
        if real then
          val flat = rocksCopy(devicePath, id)
          RebuildingStore.writeEmptyCrlState(flat, id)
          CopyFixtures.markCopy(devicePath, id, rebuilt)
        else
          CopyFixtures.placeCopy(devicePath, id, rebuilt)
      new StoreRebuild:
        val storeId: StoreId = id
        def complete: Future[UUID] = Future.successful(CopyFixtures.copyIdOf(copy))
        def cancel(): Unit = ()

  /** The children of the phase directory `name` on `dev`, none if it is absent */
  protected def inDir(dev: os.Path, name: String): Seq[os.Path] =
    if os.isDir(dev / name) then os.list(dev / name) else Nil

  /** Starts the rebuild of `s` on device A of `host` for `episode`, its entry Rebuilding for it in the tree with the
   *  device in `status`, and waits for the rebuild's work to end */
  protected def rebuildAndFlip(host: Host, episode: UUID,
                             status: StorageDeviceState.DeviceStatus = StorageDeviceState.DeviceStatus.Operational)
      : Future[RebuildOutcome] =
    given ExecutionContext = executionContext
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setRebuilding(s, episode).copy(status = status))
      _ <- waitForTransactionsToComplete()
      _ = host.control.rebuilds.startRebuild(loadedDevices(host)(deviceA), s, Some(episode), acceptLoss = false)
      _ <- pumpTx(host, recorded(host).rebuildOutcomes.contains((deviceA, s)) &&
                        !host.control.copyRegistry.rebuilding(HostCopies.RebuildHolds).contains(s))
    yield recorded(host).rebuildOutcomes((deviceA, s))

  // A store of the bootstrap pool, transferred onto the second device
  protected val moving = StoreId(PoolId.BootstrapPoolId, 1)

  /** A host root with the second device, its CRL directory present, after `place` has put what it likes there */
  protected def receivingDevice(place: os.Path => Unit = _ => ()): (os.Path, os.Path) =
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", net.secondDeviceId)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    place(dev)
    (hostRoot, dev)

  /** Starts the transfer of `moving` onto the second device in the tree and checks `host`'s devices: yields the
   *  transfer's id */
  protected def startMoving(host: Host): Future[UUID] =
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(moving, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      st <- client.getStorageDeviceState(net.secondDeviceId)
      _ = host.control.reconciler.checkAll()
    yield st.stores(moving).transferId.get

  /** Gives a staged copy of `moving` its store config, as every shipped copy has: without one, an Active entry's
   *  Missing Stores pass would rebuild the store */
  protected def configured(staged: os.Path): Unit =
    os.makeDir.all(staged)
    os.write.over(staged / StoreConfig.configFilename,
      StoreConfig(moving, StoreConfig.RocksDB(None), None, ida).yamlConfig)

  /** Drains `host` until its transfer service has begun `n` transfers in */
  protected def awaitBegun(host: Host, transfers: StagedTransferService, n: Int = 1): Future[Unit] =
    awaitLoudly(s"transfer in number $n to begin"):
      host.testingOnlyHandleEvents()
      transfers.transfersIn.size >= n

  /** A device check of the second device reading `state` */
  protected def check2(host: Host, state: StorageDeviceState): Future[Unit] =
    val armed = recorded(host).armLookup(net.secondDeviceId)
    host.control.reconciler.checkStorageDevice(net.secondDeviceId)
    armed.success(state)
    pump(host, host.control.reconciler.activeDeviceChecks.isEmpty)

  /** A rebuild that never completes, recording the stores it is started for */
  protected class HeldRebuilds extends StoreRebuildFactory:
    val created = new java.util.concurrent.ConcurrentLinkedQueue[StoreId]()
    def createRebuild(client: org.aspen_ddp.aspen.client.AspenClient, storeId: StoreId, deviceId: StorageDeviceId,
                      devicePath: os.Path, backends: org.aspen_ddp.aspen.server.store.backend.BackendOpener, io: org.aspen_ddp.aspen.server.host.DeviceIo,
                      episode: Option[UUID], existingCopy: Option[(Phase, os.Path)]): StoreRebuild =
      created.add(storeId)
      val id = storeId
      new StoreRebuild:
        val storeId: StoreId = id
        def complete: Future[UUID] = Promise[UUID]().future
        def cancel(): Unit = ()
