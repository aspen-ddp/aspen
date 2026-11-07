package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceState,
  StoragePoolState}
import org.aspen_ddp.aspen.common.network.TxPrepare
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
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{AdoptOutcome, RebuildOutcome, ShutdownOutcome,
  TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBConfig}
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
  def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]] = inner.closeStore(storeId)
  override def loadStore(storeId: StoreId, trsList: List[TransactionRecoveryState]): Future[Unit] =
    inner.loadStore(storeId, trsList)
  def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
    inner.save(txid, state, () => completions.execute(() => { completionHandler(); firstSave.trySuccess(()) }))
  def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit =
    inner.dropTransactionObjectData(storeId, txid)
  def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = inner.deleteTransaction(storeId, txid)

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
                         hostId: HostId = HostId.BootstrapHostId, seams: Host.Seams = Host.Seams.default): Host =
    new Host(client, hostId, systemId, hostRoot, executionContext,
      new LockstepThreading(executionContext), net.objectCacheFactory, net,
      BackgroundTaskManager.NoBackgroundTaskManager, crlFactory, net.FinalizerFactory,
      TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
      StoreTransferService.Disabled, StoreRebuildFactory.Filesystem, 2, seams = seams)

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
    TransactionDescription(id, HLCTimestamp(100), p, leader,
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
          throw new java.io.IOException("unrecoverable")
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
