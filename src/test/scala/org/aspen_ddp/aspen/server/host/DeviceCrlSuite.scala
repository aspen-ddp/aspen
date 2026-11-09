package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceState,
  StoragePoolState}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, TxPrepare}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Metadata, ObjectId, ObjectRefcount,
  ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription,
  TransactionId}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.{DeviceMarkers, GossipRetransmitter, Host, StorageDeviceConfig,
  StorageDeviceManager, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.crl.simple.{Recovery, SimpleCRL}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{AdoptOutcome, RebuildOutcome, ShutdownOutcome,
  TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.{Backend, DefaultBackendOpener, RocksDBConfig}
import org.aspen_ddp.aspen.server.RecordingRebuildFactory
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.transfer.{StoreTransfer, StoreTransferService}

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

/** Delegates to `inner`, recording the first completed save and every shutdown. Save completions run on
 *  `completions` (the suite's serial context), as lockstep hosts need: run on the CRL thread, the vote they send
 *  would drain the test network's hosts there. A shutdown waits on `gate`, if set, before shutting `inner` down. */
private class RecordedCrl(val devicePath: os.Path, val inner: CrashRecoveryLog, gate: Option[CountDownLatch],
                          completions: ExecutionContext) extends CrashRecoveryLog:
  val firstSave: Promise[Unit] = Promise()
  val shutdownCalled: Promise[Unit] = Promise()
  val shutdownCalls = new AtomicInteger(0)

  def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = inner.getFullRecoveryState(storeId)
  /** Completed when the store state is first read for an extraction */
  val readCalled: Promise[Unit] = Promise()
  /** While set, the next read of a store's state completes only once it completes: a host stopped there crashes in
   *  the window between the read and the save file's write. Later reads (a store's recovery) are not gated */
  @volatile var readGate: Option[Future[Unit]] = None
  override def readStoreState(storeId: StoreId): Future[List[TransactionRecoveryState]] =
    val read = inner.readStoreState(storeId)
    readCalled.trySuccess(())
    val gate = synchronized:
      val g = readGate
      readGate = None
      g
    gate match
      case None => read
      case Some(gate) => read.flatMap(trs => gate.map(_ => trs)(using ExecutionContext.parasitic))(
        using ExecutionContext.parasitic)
  override def loadStore(storeId: StoreId, trsList: List[TransactionRecoveryState]): Future[Unit] =
    inner.loadStore(storeId, trsList)
  def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
    inner.save(txid, state, () => completions.execute(() => { completionHandler(); firstSave.trySuccess(()) }))
  def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit =
    inner.dropTransactionObjectData(storeId, txid)
  def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = inner.deleteTransaction(storeId, txid)
  override def admissionLimitBytes: Long = inner.admissionLimitBytes
  override def deleteTransactions(storeId: StoreId, txids: List[TransactionId]): Future[Unit] =
    inner.deleteTransactions(storeId, txids)
  override def storesWithEntries(): Future[Set[StoreId]] = inner.storesWithEntries()

  /** Completed when dropStore is first called */
  val dropCalled: Promise[Unit] = Promise()
  /** While set, dropStore waits for it before dropping */
  @volatile var dropGate: Option[Future[Unit]] = None
  /** Completed when a dropStore has completed */
  val dropDone: Promise[Unit] = Promise()
  override def dropStore(storeId: StoreId): Future[Unit] =
    dropCalled.trySuccess(())
    val dropped = dropGate match
      case None => inner.dropStore(storeId)
      case Some(gate) => gate.flatMap(_ => inner.dropStore(storeId))(using ExecutionContext.parasitic)
    dropped.foreach(_ => dropDone.trySuccess(()))(using ExecutionContext.parasitic)
    dropped

  override def shutdown(): Unit =
    shutdownCalls.incrementAndGet()
    shutdownCalled.trySuccess(())
    gate.foreach(_.await())
    inner.shutdown()

/** Opens real SimpleCRLs and records each one it opened. */
private class RecordingCrlFactory(completions: ExecutionContext, gate: Option[CountDownLatch] = None)
  extends CrashRecoveryLogFactory:
  private val inner = SimpleCRL.DeviceFactory()
  private var crls: List[RecordedCrl] = Nil

  def openCRL(devicePath: os.Path, config: StorageDeviceConfig): CrashRecoveryLog =
    val crl = new RecordedCrl(devicePath, inner.openCRL(devicePath, config), gate, completions)
    synchronized { crls = crls :+ crl }
    crl

  def opened: List[RecordedCrl] = synchronized(crls)

  /** Shuts down every CRL opened that has not been shut down, so no CRL thread outlives the test. */
  def shutdownAll(): Unit = opened.foreach: crl =>
    if crl.shutdownCalls.get == 0 then
      crl.inner.shutdown()

/** Delegates to `base`, running `beforeComplete` before each completeDeviceShutdown: a window in which a restart can
 *  cancel the shutdown after its stores have closed. */
private class HookedMetadata(base: DeviceMetadata, beforeComplete: () => Future[Unit])(using ExecutionContext)
  extends DeviceMetadata:
  def updateStateForTransferredStore(storeId: StoreId, fromDeviceId: StorageDeviceId, toDeviceid: StorageDeviceId,
                                     gossipRetransmitter: GossipRetransmitter): Future[TransferOutcome] =
    base.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid, gossipRetransmitter)
  def readHostStateForEndpoint(): Future[HostState] = base.readHostStateForEndpoint()
  def abortLostTransfer(storeId: StoreId): Future[Boolean] = base.abortLostTransfer(storeId)
  def markRebuiltStoreActive(storageDeviceId: StorageDeviceId, storeId: StoreId,
                             gossipStoreOnline: StoreId => Unit): Future[RebuildOutcome] =
    base.markRebuiltStoreActive(storageDeviceId, storeId, gossipStoreOnline)
  def adoptDevice(storageDeviceId: StorageDeviceId, previousHostId: HostId, devicePath: os.Path,
                  uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                  crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                  markRebuildRequired: os.Path => Unit): Future[AdoptOutcome] =
    base.adoptDevice(storageDeviceId, previousHostId, devicePath, uncleanStores, crlLostStores, markRebuildRequired)
  def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    base.lookupStorageDeviceState(storageDeviceId)
  def readTransferSourceState(fromDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    base.readTransferSourceState(fromDeviceId)
  def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome] =
    beforeComplete().flatMap(_ => base.completeDeviceShutdown(storageDeviceId))

/** The default opener, whose RocksDB backends report hasFailed while `failing` is set, as one that failed-stop does */
private class FailingRocksOpener(ec: ExecutionContext) extends DefaultBackendOpener(ec):
  @volatile var failing: Boolean = false
  /** The fail-stop callback of each backend opened, most recent first */
  @volatile var onFatals: List[Throwable => Unit] = Nil
  override def open(storeCfg: StoreConfig, storePath: os.Path, create: Boolean,
                    onFatal: Throwable => Unit): Future[Backend] =
    onFatals = onFatal :: onFatals
    storeCfg.backend match
      case _: StoreConfig.RocksDB =>
        Future.fromTry(scala.util.Try(new RocksDBBackend(storePath, storeCfg.storeId, ec):
          override def hasFailed: Boolean = super.hasFailed || FailingRocksOpener.this.failing))
      case _ => super.open(storeCfg, storePath, create, onFatal)

class DeviceCrlSuite extends IntegrationTestSuite with ControlFixture:

  private val poolId = storeId.poolId
  private val oid = ObjectId(new UUID(9, 2))
  private val ptr = DataObjectPointer(oid, poolId, Array[Byte]())
  private val rev0 = ObjectRevision(TransactionId(new UUID(9, 3)))
  private val txid = TransactionId(new UUID(9, 4))
  private val ida = Replication(3, 2)

  private val factories = scala.collection.mutable.ListBuffer[RecordingCrlFactory]()

  override def subFixtureTeardown(): Unit =
    factories.foreach(_.shutdownAll())
    factories.clear()
    super.subFixtureTeardown()

  private def newFactory(gate: Option[CountDownLatch] = None): RecordingCrlFactory =
    val f = new RecordingCrlFactory(executionContext, gate)
    factories += f
    f

  /** A device whose CRL files are 1 MB each */
  private def writeSmallCrlDevice(hostRoot: os.Path, name: String, deviceId: StorageDeviceId,
                                  owner: HostId = HostId.BootstrapHostId): os.Path =
    val dir = writeDevice(hostRoot, name, deviceId, owner = Some(owner))
    os.write.over(dir / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(deviceId, systemId, crlFileSizeMb = 1).yamlConfig)
    dir

  /** A real Host, which opens RocksDB backends and its devices' CRLs through `crlFactory`, over `hostRoot` */
  private def newCrlHost(hostRoot: os.Path, crlFactory: CrashRecoveryLogFactory,
                         hostId: HostId = HostId.BootstrapHostId, seams: Host.Seams = Host.Seams.default,
                         rebuilds: StoreRebuildFactory = StoreRebuildFactory.Filesystem): Host =
    new Host(client, hostId, systemId, hostRoot, executionContext,
      new LockstepThreading(executionContext), net.objectCacheFactory, net,
      BackgroundTaskManager.NoBackgroundTaskManager, crlFactory, net.FinalizerFactory,
      TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
      StoreTransferService.Disabled, rebuilds, 2, seams = seams)

  /** Drains `host`'s events and yields until `condition` holds or `rounds` run out. The caller asserts it. */
  private def pumpUntil(host: Host, condition: => Boolean, rounds: Int = 2000): Future[Unit] =
    given ExecutionContext = executionContext
    host.testingOnlyHandleEvents()
    if condition || rounds == 0 then
      Future.unit
    else
      Future(()).flatMap(_ => pumpUntil(host, condition, rounds - 1))

  private def overwriteTxd(id: TransactionId, leader: Byte, pool: PoolId = poolId): TransactionDescription =
    val p = DataObjectPointer(oid, pool, Array[Byte]())
    TransactionDescription(id, HLCTimestamp.now, p, leader,
      List(DataUpdate(p, rev0, DataUpdateOperation.Overwrite)), Nil, None, Nil, Nil, ida, Map(pool -> ida))

  /** As pumpUntil, also letting the client's transactions complete each round */
  private def pumpWithTx(host: Host, condition: => Boolean, rounds: Int = 500): Future[Unit] =
    given ExecutionContext = executionContext
    host.testingOnlyHandleEvents()
    if condition || rounds == 0 then
      Future.unit
    else
      for
        _ <- yieldUntil(false, 5)
        _ <- waitForTransactionsToComplete()
        _ <- pumpWithTx(host, condition, rounds - 1)
      yield ()

  /** A RocksDB store `sid` in `deviceDir` holding the suite's object at rev0 */
  private def writeRocksStore(deviceDir: os.Path, sid: StoreId): Future[os.Path] =
    given ExecutionContext = executionContext
    val storeDir = deviceDir / sid.directoryName
    os.makeDir.all(storeDir)
    os.write.over(storeDir / StoreConfig.configFilename, StoreConfig(sid, StoreConfig.RocksDB(None), None).yamlConfig)
    // Not the suite's serial context: bootstrapAllocate blocks on work it runs there
    val initial = new RocksDBBackend(storeDir, sid, ExecutionContext.global)
    initial.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))
    initial.close().map(_ => storeDir)

  /** Sends `sid` a prepare of transaction `id` and waits for its vote to be saved in `crl` */
  private def prepareInFlight(host: Host, crl: RecordedCrl, sid: StoreId, id: TransactionId): Future[Unit] =
    given ExecutionContext = executionContext
    host.receiveTransactionMessage(TxPrepare(sid, net.storeId0, overwriteTxd(id, 1, sid.poolId),
      ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), Nil, true))
    pumpUntil(host, crl.firstSave.isCompleted).map(_ => crl.firstSave.isCompleted should be(true))

  /** Whether `host` serves `sid` with transaction `id` recovered and holding its lock on the object */
  private def recovered(host: Host, sid: StoreId, id: TransactionId): Boolean =
    host.control.router.loaded.get(sid).exists: s =>
      s.frontend.transactions.contains(id) && s.objectCache.get(oid).exists(_.lockedToTransaction.contains(id))

  private def noMoveFiles(storeDir: os.Path): Boolean =
    !os.exists(storeDir / StoreTransfer.TransferOutMarkerFile) && !os.exists(Backend.crlSaveFile(storeDir))

  private val oldHostId = HostId(UUID.fromString("0a0a0a0a-0000-0000-0000-00000000000c"))

  atest("a shut-down device adopted by another host recovers its in-flight transaction"):
    given ExecutionContext = executionContext
    val oldRoot = newHostDir()
    val oldDir = writeSmallCrlDevice(oldRoot, "dev0", deviceA, owner = oldHostId)
    createCrl(oldDir)
    val first = newFactory()
    val second = newFactory()
    for
      _ <- client.createHost("old-host", "10.0.0.9", 5750, 5751, 5752, oldHostId)
      _ <- net.createDevice(deviceA)
      pool <- client.createNewStoragePool("crl-adopt-pool", Replication(1, 1), None, RocksDBConfig(),
                StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      sid = StoreId(pool, 0.toByte)
      _ <- client.transactUntilSuccessful: tx =>
             for
               ptr <- client.getStoragePoolPointer(pool)
               kvos <- client.read(ptr)
             yield
               val cfg = StoragePoolState(kvos)
               cfg.stores(0) = StoragePoolState.StoreEntry(oldHostId, deviceA)
               tx.update(ptr, None, None,
                 List(KeyRevision(StoragePoolState.ConfigKey, kvos.contents(StoragePoolState.ConfigKey).revision)),
                 List(Insert(StoragePoolState.ConfigKey, cfg.encode())))
      _ <- setDeviceState(deviceA)(_.copy(hostId = oldHostId,
             stores = Map(sid -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- waitForTransactionsToComplete()
      oldStoreDir <- writeRocksStore(oldDir, sid)
      oldHost = newCrlHost(oldRoot, first, hostId = oldHostId)
      _ <- pumpUntil(oldHost, oldHost.control.router.loaded.contains(sid))
      _ <- prepareInFlight(oldHost, first.opened.head, sid, txid)
      // The pool lives on deviceA alone, so the real request would refuse this shutdown
      _ <- forceShuttingDown(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = oldHost.control.reconciler.checkAll()
      _ <- pumpWithTx(oldHost, !oldHost.control.devices.devices.contains(deviceA) &&
                               oldHost.control.devices.closingCrls.isEmpty)
      shutDown <- client.getStorageDeviceState(deviceA)
      filesAfterShutdown = noMoveFiles(oldStoreDir)
      // Carried to the new host
      newRoot = newHostDir()
      newDir = StorageDeviceManager.deviceDirectory(newRoot, "dev0")
      _ = os.move(oldDir, newDir)
      host = newCrlHost(newRoot, second)
      _ <- pumpWithTx(host, recovered(host, sid, txid))
      adopted <- client.getStorageDeviceState(deviceA)
      recoveredTx = recovered(host, sid, txid)
      _ <- host.control.router.loaded.get(sid).map(_.backend.close()).getOrElse(Future.unit)
    yield
      shutDown.status should be(StorageDeviceState.DeviceStatus.Shutdown)
      filesAfterShutdown should be(true)
      recoveredTx should be(true)
      adopted.status should be(StorageDeviceState.DeviceStatus.Operational)
      adopted.hostId should be(HostId.BootstrapHostId)
      adopted.stores(sid).status should be(StorageDeviceState.StoreStatus.Active)
      noMoveFiles(newDir / sid.directoryName) should be(true)
      DeviceMarkers.readOwningHost(newDir) should be(Some(HostId.BootstrapHostId))

  atest("a device pulled from its running host is adopted with its in-flight transaction and relinquished there"):
    given ExecutionContext = executionContext
    val oldRoot = newHostDir()
    val oldDir = writeSmallCrlDevice(oldRoot, "dev0", deviceA, owner = oldHostId)
    createCrl(oldDir)
    val first = newFactory()
    val second = newFactory()
    for
      _ <- client.createHost("old-host", "10.0.0.9", 5750, 5751, 5752, oldHostId)
      _ <- net.createDevice(deviceA)
      pool <- client.createNewStoragePool("crl-pull-pool", Replication(1, 1), None, RocksDBConfig(),
                StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      sid = StoreId(pool, 0.toByte)
      _ <- client.transactUntilSuccessful: tx =>
             for
               ptr <- client.getStoragePoolPointer(pool)
               kvos <- client.read(ptr)
             yield
               val cfg = StoragePoolState(kvos)
               cfg.stores(0) = StoragePoolState.StoreEntry(oldHostId, deviceA)
               tx.update(ptr, None, None,
                 List(KeyRevision(StoragePoolState.ConfigKey, kvos.contents(StoragePoolState.ConfigKey).revision)),
                 List(Insert(StoragePoolState.ConfigKey, cfg.encode())))
      _ <- setDeviceState(deviceA)(_.copy(hostId = oldHostId,
             stores = Map(sid -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- client.transactUntilSuccessful: tx =>
             for
               ptr <- client.getHostPointer(oldHostId)
               kvos <- client.read(ptr)
             yield
               tx.update(ptr, None, None,
                 List(KeyRevision(HostState.StateKey, kvos.contents(HostState.StateKey).revision)),
                 List(Insert(HostState.StateKey, HostState(kvos).addStorageDevice(deviceA).encode())))
      _ <- waitForTransactionsToComplete()
      oldStoreDir <- writeRocksStore(oldDir, sid)
      oldHost = newCrlHost(oldRoot, first, hostId = oldHostId)
      _ <- pumpUntil(oldHost, oldHost.control.router.loaded.contains(sid))
      _ <- prepareInFlight(oldHost, first.opened.head, sid, txid)
      // The drive is pulled while the old host runs: its files are released, as a vanished disk's are, and nothing
      // on it is written again. The old host still has the device and the store loaded.
      _ <- oldHost.control.router.loaded(sid).backend.close()
      _ = first.opened.head.inner.shutdown()
      newRoot = newHostDir()
      newDir = StorageDeviceManager.deviceDirectory(newRoot, "dev0")
      _ = os.move(oldDir, newDir)
      _ = net.takeCapturedHostMessages()
      host = newCrlHost(newRoot, second)
      _ <- pumpWithTx(host, recovered(host, sid, txid))
      adopted <- client.getStorageDeviceState(deviceA)
      oldHostState <- client.getHostState(oldHostId)
      newHostState <- client.getHostState(HostId.BootstrapHostId)
      poolEntry <- client.getStoragePoolState(pool).map(_.stores(0))
      recoveredTx = recovered(host, sid, txid)
      nudges = net.takeCapturedHostMessages().collect { case m: CheckStorageDevice => m }
      loadedBeforeNudge = oldHost.control.devices.devices.contains(deviceA)
      _ = nudges.filter(_.toHost == oldHostId).foreach(oldHost.control.reconciler.handleCheckMessage)
      _ <- pumpWithTx(oldHost, !oldHost.control.devices.devices.contains(deviceA))
      _ <- host.control.router.loaded.get(sid).map(_.backend.close()).getOrElse(Future.unit)
    yield
      recoveredTx should be(true)
      adopted.status should be(StorageDeviceState.DeviceStatus.Operational)
      adopted.hostId should be(HostId.BootstrapHostId)
      adopted.stores(sid).status should be(StorageDeviceState.StoreStatus.Active)
      oldHostState.storageDevices should not contain deviceA
      newHostState.storageDevices should contain(deviceA)
      poolEntry.hostId should be(HostId.BootstrapHostId)
      DeviceMarkers.readOwningHost(newDir) should be(Some(HostId.BootstrapHostId))
      os.exists(newDir / DeviceMarkers.ShutdownStateFile) should be(false)
      noMoveFiles(newDir / sid.directoryName) should be(true)
      nudges.map(_.toHost) should contain(oldHostId)
      loadedBeforeNudge should be(true)
      oldHost.control.devices.devices.contains(deviceA) should be(false)
      oldHost.control.router.loaded.contains(sid) should be(false)
      oldHost.control.router.isOffline(sid) should be(true)

  atest("a device holding stores without its CRL directory is not adopted until the CRL is on it"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA, owner = oldHostId)
    val factory = newFactory()
    for
      _ <- client.createHost("old-host", "10.0.0.9", 5750, 5751, 5752, oldHostId)
      _ <- net.createDevice(deviceA)
      pool <- client.createNewStoragePool("crl-missing-pool", Replication(1, 1), None, RocksDBConfig(),
                StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      sid = StoreId(pool, 0.toByte)
      _ <- setDeviceState(deviceA)(_.copy(hostId = oldHostId,
             stores = Map(sid -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- waitForTransactionsToComplete()
      storeDir <- writeRocksStore(deviceDir, sid)
      // As a shutdown copying an off-device CRL onto the drive leaves it when pulled before its final move
      _ = os.remove.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
      host = newCrlHost(hostRoot, factory)
      _ <- pumpWithTx(host, host.control.discovery.reportedDeviceDirs.contains(deviceDir) &&
                            host.control.discovery.pendingMarkerResolutions.isEmpty)
      refused <- client.getStorageDeviceState(deviceA)
      openedWhileRefused = factory.opened.size
      markedWhileRefused = os.exists(marker(storeDir))
      // The CRL arrives on the drive
      _ = createCrl(deviceDir)
      _ = host.control.reconciler.scan()
      _ <- pumpWithTx(host, host.control.router.loaded.contains(sid))
      adopted <- client.getStorageDeviceState(deviceA)
      _ <- host.control.router.loaded.get(sid).map(_.backend.close()).getOrElse(Future.unit)
    yield
      refused.hostId should be(oldHostId)
      openedWhileRefused should be(0)
      markedWhileRefused should be(false)
      adopted.hostId should be(HostId.BootstrapHostId)
      adopted.stores(sid).status should be(StorageDeviceState.StoreStatus.Active)
      os.exists(marker(storeDir)) should be(false)

  atest("a cancelled shutdown reloads its stores with their in-flight transactions"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    val factory = newFactory()
    // A restart lands after the stores have closed, just before the Shutdown commit
    val restart: () => Future[Unit] = () =>
      setDeviceState(deviceA)(_.withOfflineStoresActive.copy(status = StorageDeviceState.DeviceStatus.Operational))
    val seams = Host.Seams(wrapMetadata = Some(base => new HookedMetadata(base, restart)))
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(
             stores = Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- waitForTransactionsToComplete()
      storeDir <- writeRocksStore(deviceDir, storeId)
      host = newCrlHost(hostRoot, factory, seams = seams)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      firstStore = host.control.router.loaded(storeId)
      _ <- prepareInFlight(host, factory.opened.head, storeId, txid)
      _ <- forceShuttingDown(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, host.control.router.loaded.get(storeId).exists(_ ne firstStore) &&
                            recovered(host, storeId, txid))
      st <- client.getStorageDeviceState(deviceA)
      reloaded = host.control.router.loaded.get(storeId)
      recoveredTx = recovered(host, storeId, txid)
      _ <- reloaded.map(_.backend.close()).getOrElse(Future.unit)
    yield
      st.status should be(StorageDeviceState.DeviceStatus.Operational)
      st.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      reloaded.exists(_ ne firstStore) should be(true)
      recoveredTx should be(true)
      host.control.router.isOffline(storeId) should be(false)
      noMoveFiles(storeDir) should be(true)
      os.exists(deviceDir / DeviceMarkers.ShutdownStateFile) should be(false)
      host.control.devices.devices.contains(deviceA) should be(true)

  atest("each device gets its own CRL in its directory"):
    val hostRoot = newHostDir()
    val dirA = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val dirB = writeSmallCrlDevice(hostRoot, "dev1", deviceB)
    val host = newCrlHost(hostRoot, newFactory())

    Future.successful:
      val devices = host.control.devices.devices
      os.exists(CrashRecoveryLog.deviceCrlDir(dirA) / "0.crl") should be(true)
      os.exists(CrashRecoveryLog.deviceCrlDir(dirB) / "0.crl") should be(true)
      devices.keySet should be(Set(deviceA, deviceB))
      (devices(deviceA).crl ne devices(deviceB).crl) should be(true)

  atest("a restarted host recovers a store's in-flight transaction from its device's CRL"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    val storeDir = writeStoreDir(deviceDir)
    // Not the suite's serial context: bootstrapAllocate blocks on work it runs there
    val initial = new RocksDBBackend(storeDir, storeId, ExecutionContext.global)
    initial.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))

    val first = newFactory()
    val second = newFactory()

    for
      _ <- initial.close()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ = host.control.router.loaded.contains(storeId) should be(true)
      _ = host.receiveTransactionMessage(TxPrepare(storeId, net.storeId0, overwriteTxd(txid, 1),
        ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), Nil, true))
      _ <- pumpUntil(host, first.opened.head.firstSave.isCompleted)
      _ = first.opened.head.firstSave.isCompleted should be(true)
      // Crash-only: the store is not closed, so its state stays in the device's CRL
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      restarted = newCrlHost(hostRoot, second)
      _ <- pumpUntil(restarted, restarted.control.router.loaded.get(storeId).exists: s =>
        s.frontend.transactions.contains(txid) && s.objectCache.get(oid).exists(_.lockedToTransaction.nonEmpty))
      store = restarted.control.router.loaded(storeId)
      recovered = store.frontend.transactions.keySet
      lockHolder = store.objectCache.get(oid).flatMap(_.lockedToTransaction)
      _ <- store.backend.close()
    yield
      recovered should be(Set(txid))
      lockHolder should be(Some(txid))

  atest("unloading a device shuts its CRL down"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)
    host.control.devices.devices.keySet should be(Set(deviceA))
    val crl = factory.opened.head

    host.control.shutdown.unloadDevice(deviceA)

    crl.shutdownCalled.future.map: _ =>
      crl.shutdownCalls.get should be(1)
      host.control.devices.devices.contains(deviceA) should be(false)

  atest("a device is not reloaded while its CRL is closing"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val gate = new CountDownLatch(1)
    val factory = newFactory(Some(gate))
    val host = newCrlHost(hostRoot, factory)
    factory.opened.size should be(1)

    host.control.shutdown.unloadDevice(deviceA)
    val closing = host.control.devices.closingCrls(deviceDir)

    for
      _ <- factory.opened.head.shutdownCalled.future
      reloadedWhileClosing = host.control.discovery.checkForNewDevices()
      _ = reloadedWhileClosing should be(Nil)
      _ = factory.opened.size should be(1)
      _ = gate.countDown()
      _ <- closing
      _ <- yieldUntil(!host.control.devices.closingCrls.contains(deviceDir))
      reloaded = host.control.discovery.checkForNewDevices()
    yield
      host.control.devices.closingCrls.contains(deviceDir) should be(false)
      reloaded should be(List(deviceA))
      factory.opened.size should be(2)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened(1)

  private def writeStoreDir(deviceDir: os.Path): os.Path =
    val storeDir = deviceDir / storeId.directoryName
    os.makeDir.all(storeDir)
    os.write.over(storeDir / StoreConfig.configFilename,
      StoreConfig(storeId, StoreConfig.RocksDB(None), None).yamlConfig)
    storeDir

  /** Gives the device an empty CRL, as one that has run before has */
  private def createCrl(deviceDir: os.Path): Unit =
    newFactory().openCRL(deviceDir, StorageDeviceConfig.loadStorageDeviceConfig(
      deviceDir / StorageDeviceConfig.configFilename)).shutdown()

  private def marker(storeDir: os.Path): os.Path = storeDir / RebuildingStore.RebuildRequiredMarkerFile

  atest("a device whose CRL cannot be recovered marks its stores rebuild-required"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    val crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)
    // An existing CRL, whose contents must survive
    createCrl(deviceDir)
    val oldFiles = os.list(crlDir).map(_.last).toSet

    val inner = newFactory()
    val calls = new AtomicInteger(0)
    val failingFirst = new CrashRecoveryLogFactory:
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig): CrashRecoveryLog =
        if calls.incrementAndGet() == 1 then
          throw Recovery.BrokenEntryChain("unrecoverable")
        inner.openCRL(devicePath, config)
    val host = newCrlHost(hostRoot, failingFirst)

    Future.successful:
      os.exists(marker(storeDir)) should be(true)
      host.control.router.loaded.contains(storeId) should be(false)
      val lost = os.list(deviceDir).filter(_.last.startsWith("crl.lost-"))
      lost.size should be(1)
      os.list(lost.head).map(_.last).toSet should be(oldFiles)
      calls.get should be(2)
      inner.opened.size should be(1)
      os.exists(crlDir / "0.crl") should be(true)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  /** A device holding a store whose CRL opens fail with an I/O error the first `failures` times */
  private def transientlyFailingCrlDevice(failures: Int): (os.Path, os.Path, RecordingCrlFactory, AtomicInteger, Host) =
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val inner = newFactory()
    val calls = new AtomicInteger(0)
    val failing = new CrashRecoveryLogFactory:
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig): CrashRecoveryLog =
        if calls.incrementAndGet() <= failures then
          throw new java.io.IOException("transient")
        inner.openCRL(devicePath, config)
    (deviceDir, storeDir, inner, calls, newCrlHost(hostRoot, failing))

  atest("a transient CRL open failure leaves the stores untouched and a later scan loads the device"):
    val failures = DeviceDiscovery.CrlOpenAttempts - 1
    val (deviceDir, storeDir, inner, calls, host) = transientlyFailingCrlDevice(failures)
    // The startup scan failed once
    host.control.devices.devices.contains(deviceA) should be(false)
    os.exists(marker(storeDir)) should be(false)
    for _ <- 2 to failures do
      host.control.discovery.checkForNewDevices() should be(Nil)
    os.exists(marker(storeDir)) should be(false)

    host.control.discovery.checkForNewDevices() should be(List(deviceA))
    calls.get should be(failures + 1)
    Future.successful:
      os.exists(marker(storeDir)) should be(false)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  atest("a CRL open failing transiently CrlOpenAttempts times in a row is treated as lost"):
    val (deviceDir, storeDir, inner, calls, host) = transientlyFailingCrlDevice(DeviceDiscovery.CrlOpenAttempts)
    for _ <- 2 until DeviceDiscovery.CrlOpenAttempts do
      host.control.discovery.checkForNewDevices() should be(Nil)
    os.exists(marker(storeDir)) should be(false)

    // The last attempt fails too: the CRL is lost and a fresh one opened
    host.control.discovery.checkForNewDevices() should be(List(deviceA))
    Future.successful:
      os.exists(marker(storeDir)) should be(true)
      os.list(deviceDir).count(_.last.startsWith("crl.lost-")) should be(1)
      calls.get should be(DeviceDiscovery.CrlOpenAttempts + 1)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  atest("a device with stores but no crl dir marks them rebuild-required"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)
    host.control.shutdown.unloadDevice(deviceA)
    val storeDir = writeStoreDir(deviceDir)
    os.remove.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
    os.exists(marker(storeDir)) should be(false)

    factory.opened.head.shutdownCalled.future.flatMap(_ => yieldUntil(host.control.devices.closingCrls.isEmpty)).map: _ =>
      host.control.discovery.checkForNewDevices() should be(List(deviceA))
      os.exists(marker(storeDir)) should be(true)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
      os.exists(CrashRecoveryLog.deviceCrlDir(deviceDir) / "0.crl") should be(true)

  atest("a new device with no stores just creates its CRL"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val host = newCrlHost(hostRoot, newFactory())

    Future.successful:
      host.control.devices.devices.keySet should be(Set(deviceA))
      os.exists(CrashRecoveryLog.deviceCrlDir(deviceDir) / "0.crl") should be(true)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)

  /** Points the pool's entry for `sid` at `device` on the bootstrap host */
  private def setPoolEntry(sid: StoreId, device: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStoragePoolPointer(sid.poolId)
        kvos <- client.read(ptr)
      yield
        val cfg = StoragePoolState(kvos)
        cfg.stores(sid.poolIndex) = StoragePoolState.StoreEntry(HostId.BootstrapHostId, device)
        tx.update(ptr, None, None,
          List(KeyRevision(StoragePoolState.ConfigKey, kvos.contents(StoragePoolState.ConfigKey).revision)),
          List(Insert(StoragePoolState.ConfigKey, cfg.encode())))

  /** The transactions `crl` holds for `sid`. Blocks on the CRL thread, which never waits on this one */
  private def held(crl: CrashRecoveryLog, sid: StoreId): Set[TransactionId] =
    crl.getFullRecoveryState(sid).map(_.txd.transactionId).toSet

  /** The transactions the CRL files of `deviceDir` hold for `sid`, as a restart recovers them. The CRL that wrote
   *  them must be shut down first. */
  private def heldOnDisk(deviceDir: os.Path, sid: StoreId): Set[TransactionId] =
    val crl = newFactory().openCRL(deviceDir, StorageDeviceConfig.loadStorageDeviceConfig(
      deviceDir / StorageDeviceConfig.configFilename))
    try held(crl, sid)
    finally crl.shutdown()

  /** deviceA in the tree with `storeId` Active, and its directory under a new host root holding a RocksDB store */
  private def activeStoreDevice(): Future[(os.Path, os.Path, os.Path)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(
             stores = Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- waitForTransactionsToComplete()
      storeDir <- writeRocksStore(deviceDir, storeId)
    yield (hostRoot, deviceDir, storeDir)

  /** Drives `host`'s checks until its rebuild of `storeId` starts, lands a fresh copy at `storeDir` as the walk
   *  would and completes it, then waits for the copy to be served. Returns whether the old directory was discarded
   *  and the copy's device CRL held nothing for the store once the rebuild started. */
  private def rebuildAndServe(host: Host, rebuilds: RecordingRebuildFactory, deviceDir: os.Path,
                              storeDir: os.Path): Future[(Boolean, Set[TransactionId])] =
    given ExecutionContext = executionContext
    host.control.reconciler.checkAll()
    for
      _ <- pumpWithTx(host, rebuilds.created.nonEmpty)
      _ = rebuilds.created.map(_.storeId).toList should be(List(storeId))
      discarded = !os.exists(storeDir)
      // The drop is queued on the CRL thread ahead of this read
      leftInCrl = held(host.control.devices.devices(deviceA).crl, storeId)
      _ <- writeRocksStore(deviceDir, storeId)
      _ = rebuilds.created.head.promise.success(())
      _ <- pumpWithTx(host, host.control.router.loaded.contains(storeId))
    yield (discarded, leftInCrl)

  atest("a store that fails during its close is marked rebuild-required before its device-CRL entries go"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val rebuilds = new RecordingRebuildFactory
    val opener = new FailingRocksOpener(executionContext)
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory, seams = Host.Seams(backends = Some(opener)), rebuilds = rebuilds)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = host.control.devices.devices(deviceA).crl
      _ <- prepareInFlight(host, factory.opened.head, storeId, txid)
      _ = opener.failing = true
      closing = host.closeStore(storeId)
      _ <- pumpUntil(host, closing.isCompleted)
      closed <- closing.transform(r => scala.util.Success(r))
      _ = opener.failing = false
      markedAfterClose = os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile)
      heldAfterClose = held(crl, storeId)
      (discarded, leftInCrl) <- rebuildAndServe(host, rebuilds, deviceDir, storeDir)
      rebuilt = host.control.router.loaded(storeId)
      recoveredByCopy = rebuilt.frontend.transactions.keySet
      _ <- rebuilt.backend.close()
      _ = factory.shutdownAll()
    yield
      closed.failed.toOption should be(Some(Host.StoreFailedOnClose(storeId)))
      markedAfterClose should be(true)
      // Nothing dropped by the close itself: the marker comes first
      heldAfterClose should be(Set(txid))
      discarded should be(true)
      leftInCrl should be(Set())
      recoveredByCopy should be(Set())
      heldOnDisk(deviceDir, storeId) should be(Set())

  atest("a store discarded for rebuild after a restart does not get the discarded copy's transactions back"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    val rebuilds = new RecordingRebuildFactory
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      // Crash-only: the in-flight transaction stays in the device CRL. On the restart the store is found lost
      // (as BackendStateLost or a lost save file mark it)
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      _ = os.write.over(storeDir / RebuildingStore.RebuildRequiredMarkerFile, "")
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      heldAtRestart = held(restarted.control.devices.devices(deviceA).crl, storeId)
      (discarded, leftInCrl) <- rebuildAndServe(restarted, rebuilds, deviceDir, storeDir)
      rebuilt = restarted.control.router.loaded(storeId)
      recoveredByCopy = rebuilt.frontend.transactions.keySet
      _ <- rebuilt.backend.close()
      _ = second.shutdownAll()
    yield
      heldAtRestart should be(Set(txid))
      discarded should be(true)
      leftInCrl should be(Set())
      recoveredByCopy should be(Set())
      // Deleted from the files too, so a later restart does not recover them either
      heldOnDisk(deviceDir, storeId) should be(Set())

  atest("a discarded store is kept in pending-delete with its CRL entries until the store is Active again"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    val rebuilds = new RecordingRebuildFactory
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      _ = os.write.over(storeDir / RebuildingStore.RebuildRequiredMarkerFile, "")
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, rebuilds.created.nonEmpty)
      copies = os.list(deviceDir / RebuildingStore.PendingDeleteDirectory).toList
      saved = copies.map(c => CrashRecoveryLog.loadValidStoreState(c / RebuildingStore.LostCrlStateFile, storeId))
      leftInCrl = held(second.opened.head, storeId)
      // Checks while the entry is Rebuilding keep it
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty)
      keptWhileRebuilding = copies.forall(os.exists(_))
      _ <- writeRocksStore(deviceDir, storeId)
      _ = rebuilds.created.head.promise.success(())
      _ <- pumpWithTx(restarted, restarted.control.router.loaded.contains(storeId) && copies.forall(!os.exists(_)))
      deletedOnceActive = copies.forall(!os.exists(_))
      _ <- restarted.control.router.loaded(storeId).backend.close()
      _ = second.shutdownAll()
    yield
      copies.size should be(1)
      saved.map(_.map(_.map(_.txd.transactionId))) should be(List(Right(List(txid))))
      leftInCrl should be(Set())
      keptWhileRebuilding should be(true)
      deletedOnceActive should be(true)

  atest("a pending-delete copy of a store rebuilt on another device goes once that device has it Active"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val factory = newFactory()
    def checked(host: Host): Future[Unit] =
      host.control.reconciler.checkStorageDevice(deviceA)
      pumpWithTx(host, host.control.reconciler.activeDeviceChecks.isEmpty &&
                       host.control.reconciler.pendingDeleteLookupsInFlight.isEmpty)
    for
      _ <- net.createDevice(deviceA)
      _ <- net.createDevice(deviceB)
      pool <- client.createNewStoragePool("pending-delete-pool", Replication(1, 1), None, RocksDBConfig(),
                StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      elsewhere = StoreId(pool, 0.toByte)
      copy = deviceDir / RebuildingStore.PendingDeleteDirectory / s"${elsewhere.directoryName}-old"
      _ <- setPoolEntry(elsewhere, deviceB)
      _ <- setDeviceState(deviceB)(_.copy(stores = Map(
             elsewhere -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None))))
      _ <- waitForTransactionsToComplete()
      _ = os.makeDir.all(copy)
      _ = os.write.over(copy / StoreConfig.configFilename,
            StoreConfig(elsewhere, StoreConfig.RocksDB(None), None).yamlConfig)
      host = newCrlHost(hostRoot, factory)
      _ <- checked(host)
      keptWhileRebuilding = os.exists(copy)
      _ <- setDeviceState(deviceB)(_.copy(stores = Map(
             elsewhere -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- waitForTransactionsToComplete()
      _ <- checked(host)
      _ = factory.shutdownAll()
    yield
      keptWhileRebuilding should be(true)
      os.exists(copy) should be(false)

  atest("a device the tree records as failed is unloaded, marked, and not loaded again by a restart"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- setDeviceState(deviceA)(_.copy(status = StorageDeviceState.DeviceStatus.Failed))
      _ <- waitForTransactionsToComplete()
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ <- pumpWithTx(host, !host.control.devices.devices.contains(deviceA) &&
                            host.control.devices.closingCrls.isEmpty)
      unloaded = !host.control.devices.devices.contains(deviceA)
      storeServed = host.control.router.loaded.contains(storeId)
      restarted = newCrlHost(hostRoot, newFactory())
    yield
      unloaded should be(true)
      storeServed should be(false)
      os.exists(deviceDir / DeviceMarkers.FailedFile) should be(true)
      restarted.control.devices.devices.contains(deviceA) should be(false)
      restarted.control.router.loaded.contains(storeId) should be(false)
      // Nothing on it is deleted
      os.exists(storeDir / StoreConfig.configFilename) should be(true)

  atest("a network transfer out of an unloaded store ships its device-CRL entries in its crl_save.log"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = host.control.devices.devices(deviceA).crl
      _ <- prepareInFlight(host, factory.opened.head, storeId, txid)
      // Closed without extraction: its state stays in the device CRL
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      heldAfterClose = held(crl, storeId)
      saveFileAfterClose = os.exists(Backend.crlSaveFile(storeDir))
      _ <- host.control.transfers.closeForTransferOut(storeId)
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      heldAfterExtract = held(crl, storeId)
      _ = factory.shutdownAll()
    yield
      heldAfterClose should be(Set(txid))
      saveFileAfterClose should be(false)
      saved.map(_.map(_.txd.transactionId)) should be(Right(List(txid)))
      heldAfterExtract should be(Set())
      // Deleted from the files once the save file was durable: not recovered here on a restart as well
      heldOnDisk(deviceDir, storeId) should be(Set())

  atest("a discarded store's directory and marker stay until its device-CRL entries are durably dropped"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    val rebuilds = new RecordingRebuildFactory
    val gate = Promise[Unit]()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      _ = os.write.over(storeDir / RebuildingStore.RebuildRequiredMarkerFile, "")
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      crl = second.opened.head
      _ = crl.dropGate = Some(gate.future)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, crl.dropCalled.isCompleted)
      // More checks while the drop is held start nothing and discard nothing
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, false, rounds = 20)
      dropRequested = crl.dropCalled.isCompleted
      keptWhileDropping = os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile)
      startedWhileDropping = rebuilds.created.nonEmpty
      _ = gate.success(())
      _ <- pumpWithTx(restarted, rebuilds.created.nonEmpty)
      leftInCrl = held(crl, storeId)
      _ = second.shutdownAll()
    yield
      dropRequested should be(true)
      keptWhileDropping should be(true)
      startedWhileDropping should be(false)
      rebuilds.created.map(_.storeId).toList should be(List(storeId))
      os.exists(storeDir) should be(false)
      leftInCrl should be(Set())
      heldOnDisk(deviceDir, storeId) should be(Set())

  atest("a failed backend whose rebuild-required marker cannot be written keeps its CRL entries on disk"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val opener = new FailingRocksOpener(executionContext)
    // The marker's write fails and leaves nothing behind, as a failed write does. (Anything left at the marker's path
    // counts as a marker: a reconcile would then rebuild the store and drop its entries itself.)
    @volatile var markerWrites = 0
    val failingMarker: os.Path => Unit = _ =>
      markerWrites += 1
      throw new java.io.IOException("Input/output error")
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory,
               seams = Host.Seams(backends = Some(opener), writeRebuildRequiredMarker = Some(failingMarker)))
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, factory.opened.head, storeId, txid)
      _ = opener.onFatals.head(new java.io.IOException("Input/output error"))
      _ <- pumpUntil(host, !host.control.router.loaded.contains(storeId))
      unloaded = !host.control.router.loaded.contains(storeId)
      // Lets the close run on the store's executor, ahead of the shutdown below
      _ <- pumpUntil(host, false, rounds = 50)
      _ = factory.shutdownAll()
    yield
      unloaded should be(true)
      markerWrites should be >= 1
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)
      // Left in the device CRL: a restart, which may serve the unmarked store, recovers them
      heldOnDisk(deviceDir, storeId) should be(Set(txid))

  atest("a full device slows only its own stores"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dirA = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val dirB = writeSmallCrlDevice(hostRoot, "dev1", deviceB)
    createCrl(dirA)
    createCrl(dirB)
    val sidA = storeId
    val sidB = StoreId(storeId.poolId, 1.toByte)
    val txA = TransactionId(new UUID(9, 5))
    val txB = TransactionId(new UUID(9, 6))
    val factory = newFactory()
    // Led by a third store, so neither starts a driver
    def prepare(sid: StoreId, id: TransactionId) =
      TxPrepare(sid, net.storeId0, overwriteTxd(id, 2), ProposalId.initialProposal(2),
        List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), Nil, true)
    for
      _ <- writeRocksStore(dirA, sidA)
      _ <- writeRocksStore(dirB, sidB)
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(sidA) && host.control.router.loaded.contains(sidB))
      devices = host.control.devices.devices
      crlB = factory.opened.find(_.devicePath == dirB).get
      limitA = devices(deviceA).admission.limitBytes
      // Device A's CRL is past the mark
      _ = devices(deviceA).admission.tryAdmit(limitA) should be(true)
      _ = host.receiveTransactionMessage(prepare(sidA, txA))
      _ = host.receiveTransactionMessage(prepare(sidB, txB))
      _ <- pumpUntil(host, crlB.firstSave.isCompleted)
      storeA = host.control.router.loaded(sidA)
      storeB = host.control.router.loaded(sidB)
      droppedA = !storeA.frontend.transactions.contains(txA)
      answeredB = storeB.frontend.transactions.contains(txB)
      _ <- storeA.backend.close()
      _ <- storeB.backend.close()
    yield
      // 1 MB CRL files: half of one
      limitA should be(512L * 1024)
      (devices(deviceA).admission ne devices(deviceB).admission) should be(true)
      droppedA should be(true)
      factory.opened.find(_.devicePath == dirA).get.firstSave.isCompleted should be(false)
      crlB.firstSave.isCompleted should be(true)
      answeredB should be(true)
      devices(deviceB).admission.admittedBytes should be > 0L

  atest("two stores on one device receiving the same transaction are both counted"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dirA = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(dirA)
    val sid0 = storeId
    val sid1 = StoreId(storeId.poolId, 1.toByte)
    val factory = newFactory()
    // Led by a third store, so neither starts a driver
    def prepare(sid: StoreId) =
      TxPrepare(sid, net.storeId0, overwriteTxd(txid, 2), ProposalId.initialProposal(2),
        List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), Nil, true)
    for
      _ <- writeRocksStore(dirA, sid0)
      _ <- writeRocksStore(dirA, sid1)
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(sid0) && host.control.router.loaded.contains(sid1))
      admission = host.control.devices.devices(deviceA).admission
      store0 = host.control.router.loaded(sid0)
      store1 = host.control.router.loaded(sid1)
      _ = host.receiveTransactionMessage(prepare(sid0))
      _ <- pumpUntil(host, store0.frontend.transactions.contains(txid))
      one = admission.admittedBytes
      _ = host.receiveTransactionMessage(prepare(sid1))
      _ <- pumpUntil(host, store1.frontend.transactions.contains(txid))
      both = admission.admittedBytes
      _ <- store0.backend.close()
      _ <- store1.backend.close()
    yield
      one should be > 0L
      both should be(2 * one)

  /** Saves another store's transaction on `crl` twice. The second entry then needs nothing older than the first, so
   *  a recovery stops walking back there: only transactions still live in the CRL before these saves survive. */
  private def advanceLog(crl: CrashRecoveryLog, template: TransactionRecoveryState): Future[Unit] =
    given ExecutionContext = executionContext
    val other = template.copy(storeId = StoreId(storeId.poolId, 7.toByte))
    def saveOnce(): Future[Unit] =
      val p = Promise[Unit]()
      crl.save(TransactionId(new UUID(9, 77)), other, () => p.trySuccess(()))
      p.future
    saveOnce().flatMap(_ => saveOnce())

  /** Extracts `storeId`'s CRL state for a network transfer out, from the open store (`loaded`, a close) or from its
   *  device CRL once closed (extractUnloaded), with an in-flight transaction. With `crash`, the host stops between
   *  reading that state and writing the save file; otherwise the save file's write fails. Other stores' entries then
   *  move the log on, and the host crashes. Returns how the extraction ended, the transactions the device CRL held
   *  for the store once it had, and those a restart recovers. */
  private def interruptedExtraction(loaded: Boolean, crash: Boolean)
    : Future[(Option[scala.util.Try[Unit]], Set[TransactionId], Set[TransactionId], Boolean)] =
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      _ <- prepareInFlight(host, crl, storeId, txid)
      template = crl.getFullRecoveryState(storeId).head
      backend = host.control.router.loaded(storeId).backend
      _ <-
        if loaded then
          Future.unit
        else
          val closed = host.closeStore(storeId)
          pumpUntil(host, closed.isCompleted).flatMap(_ => closed)
      _ =
        if crash then
          crl.readGate = Some(Promise[Unit]().future)
        else
          // A non-empty directory where the save file's temporary goes: its write fails
          os.makeDir.all(storeDir / s".${Backend.crlSaveFile(storeDir).last}.tmp" / "blocker")
      extracting = host.control.transfers.closeForTransferOut(storeId)
      _ <- pumpUntil(host, if crash then crl.readCalled.isCompleted else extracting.isCompleted)
      outcome = extracting.value
      heldAfter = held(crl, storeId)
      _ <- advanceLog(crl, template)
      // A store stopped mid-close never closed its backend
      _ <- if loaded && crash then backend.close() else Future.unit
      _ = factory.shutdownAll()
      saveFile = os.exists(Backend.crlSaveFile(storeDir))
    yield (outcome, heldAfter, heldOnDisk(deviceDir, storeId), saveFile)

  atest("a closed store's live device-CRL entries stay counted by admission until they leave the CRL"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, _, _) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      admission = host.control.devices.devices(deviceA).admission
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      _ <- prepareInFlight(host, crl, storeId, txid)
      counted = admission.admittedBytes
      // Closed without extraction: its entries stay live in the device CRL
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      afterClose = admission.admittedBytes
      // Loaded again, it counts its recovered transaction in place of what its close kept
      _ = host.loadStoreById(deviceA, storeId)
      _ <- pumpUntil(host, recovered(host, storeId, txid))
      afterReload = admission.admittedBytes
      // A transfer out's extraction takes the entries out of the device CRL, and their count with them
      extracted = host.control.transfers.closeForTransferOut(storeId)
      _ <- pumpUntil(host, extracted.isCompleted)
      _ <- extracted
      afterExtraction = admission.admittedBytes
      _ = factory.shutdownAll()
    yield
      counted should be > 0L
      afterClose should be(counted)
      afterReload should be(counted)
      afterExtraction should be(0L)

  atest("an extraction during which its closed store loads again leaves the store's transactions in the device CRL"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      _ <- prepareInFlight(host, crl, storeId, txid)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      gate = Promise[Unit]()
      _ = crl.readGate = Some(gate.future)
      extracting = host.control.transfers.closeForTransferOut(storeId)
      _ <- pumpUntil(host, crl.readCalled.isCompleted)
      // The store loads again while the extraction's read is outstanding, recovering its transaction
      _ = host.loadStoreById(deviceA, storeId)
      _ <- pumpUntil(host, recovered(host, storeId, txid))
      _ = gate.success(())
      _ <- pumpUntil(host, extracting.isCompleted)
      outcome = extracting.value
      heldAfter = held(crl, storeId)
      served = recovered(host, storeId, txid)
      _ <- host.control.router.loaded.get(storeId).map(_.backend.close()).getOrElse(Future.unit)
      _ = factory.shutdownAll()
    yield
      outcome.exists(_.isFailure) should be(true)
      served should be(true)
      heldAfter should be(Set(txid))
      os.exists(Backend.crlSaveFile(storeDir)) should be(false)
      heldOnDisk(deviceDir, storeId) should be(Set(txid))

  atest("a transfer out of an open store whose save file fails keeps its transactions in the device CRL"):
    given ExecutionContext = executionContext
    interruptedExtraction(loaded = true, crash = false).map: (outcome, heldAfter, onDisk, saveFile) =>
      outcome.exists(_.isFailure) should be(true)
      saveFile should be(false)
      heldAfter should be(Set(txid))
      onDisk should be(Set(txid))

  atest("a transfer out of a closed store whose save file fails keeps its transactions in the device CRL"):
    given ExecutionContext = executionContext
    interruptedExtraction(loaded = false, crash = false).map: (outcome, heldAfter, onDisk, saveFile) =>
      outcome.exists(_.isFailure) should be(true)
      saveFile should be(false)
      heldAfter should be(Set(txid))
      onDisk should be(Set(txid))

  atest("a crash before an open store's transfer-out save file is written keeps its transactions in the device CRL"):
    given ExecutionContext = executionContext
    interruptedExtraction(loaded = true, crash = true).map: (outcome, _, onDisk, saveFile) =>
      outcome should be(None)
      saveFile should be(false)
      onDisk should be(Set(txid))

  atest("a crash before a closed store's transfer-out save file is written keeps its transactions in the device CRL"):
    given ExecutionContext = executionContext
    interruptedExtraction(loaded = false, crash = true).map: (outcome, _, onDisk, saveFile) =>
      outcome should be(None)
      saveFile should be(false)
      onDisk should be(Set(txid))

  atest("deleting a store transferred away drops its leftover device-CRL entries"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      _ <- prepareInFlight(host, crl, storeId, txid)
      // As a crash between a transfer out's save file and the deletion of its entries leaves them
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      // The transfer completed: the device's entry no longer names the store
      _ <- setDeviceState(deviceA)(_.copy(stores = Map()))
      _ <- waitForTransactionsToComplete()
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, !os.exists(storeDir) && crl.dropDone.isCompleted)
      deleted = !os.exists(storeDir)
      leftInCrl = held(crl, storeId)
      _ = factory.shutdownAll()
    yield
      deleted should be(true)
      leftInCrl should be(Set())
      heldOnDisk(deviceDir, storeId) should be(Set())

  atest("a restart drops the device-CRL entries a store transferred away left behind"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      // A crash after the reconcile deleted the directory of a store transferred away, before its drop was durable
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      _ = os.remove.all(storeDir)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map()))
      _ <- waitForTransactionsToComplete()
      heldBefore = heldOnDisk(deviceDir, storeId)
      restarted = newCrlHost(hostRoot, second)
      _ = restarted.control.reconciler.checkAll()
      crl = second.opened.head
      _ <- pumpUntil(restarted, crl.dropDone.isCompleted)
      leftInCrl = held(crl, storeId)
      _ = second.shutdownAll()
    yield
      heldBefore should be(Set(txid))
      leftInCrl should be(Set())
      heldOnDisk(deviceDir, storeId) should be(Set())

  /** Opens a device whose CRL was created with 3 streams of 1 MB, holding a store, with `config` instead */
  private def resizedCrlDevice(config: StorageDeviceConfig => StorageDeviceConfig): Future[org.scalatest.Assertion] =
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)
    val oldFiles = os.list(crlDir).map(_.last).toSet
    os.write.over(deviceDir / StorageDeviceConfig.configFilename,
      config(StorageDeviceConfig(deviceA, systemId, crlFileSizeMb = 1)).yamlConfig)
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)

    Future.successful:
      os.exists(marker(storeDir)) should be(true)
      host.control.router.loaded.contains(storeId) should be(false)
      val lost = os.list(deviceDir).filter(_.last.startsWith("crl.lost-"))
      lost.size should be(1)
      os.list(lost.head).map(_.last).toSet should be(oldFiles)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened.last

  atest("a device CRL opened with fewer streams than it was created with is treated as lost"):
    resizedCrlDevice(_.copy(crlNumStreams = 2))

  atest("a device CRL opened with more streams than it was created with is treated as lost"):
    resizedCrlDevice(_.copy(crlNumStreams = 4))

  atest("a device CRL opened with another file size than it was created with is treated as lost"):
    resizedCrlDevice(_.copy(crlFileSizeMb = 2))
