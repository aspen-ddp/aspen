package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.TxPrepare
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription,
  TransactionId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.{Host, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.transfer.StoreTransferService

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
  private def writeSmallCrlDevice(hostRoot: os.Path, name: String, deviceId: StorageDeviceId): os.Path =
    val dir = writeDevice(hostRoot, name, deviceId)
    os.write.over(dir / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(deviceId, systemId, crlFileSizeMb = 1).yamlConfig)
    dir

  /** A real Host, which opens RocksDB backends and its devices' CRLs through `crlFactory`, over `hostRoot` */
  private def newCrlHost(hostRoot: os.Path, crlFactory: CrashRecoveryLogFactory): Host =
    new Host(client, HostId.BootstrapHostId, systemId, hostRoot, executionContext,
      new LockstepThreading(executionContext), net.objectCacheFactory, net,
      BackgroundTaskManager.NoBackgroundTaskManager, crlFactory, net.FinalizerFactory,
      TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
      StoreTransferService.Disabled, StoreRebuildFactory.Filesystem, 2)

  /** Drains `host`'s events and yields until `condition` holds or `rounds` run out. The caller asserts it. */
  private def pumpUntil(host: Host, condition: => Boolean, rounds: Int = 2000): Future[Unit] =
    given ExecutionContext = executionContext
    host.testingOnlyHandleEvents()
    if condition || rounds == 0 then
      Future.unit
    else
      Future(()).flatMap(_ => pumpUntil(host, condition, rounds - 1))

  private def overwriteTxd(id: TransactionId, leader: Byte): TransactionDescription =
    TransactionDescription(id, HLCTimestamp(100), ptr, leader,
      List(DataUpdate(ptr, rev0, DataUpdateOperation.Overwrite)), Nil, None, Nil, Nil, ida, Map(poolId -> ida))

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
