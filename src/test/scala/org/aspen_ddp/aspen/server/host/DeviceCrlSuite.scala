package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.server.store.backend.{DefaultBackendOpener, StoreCreation}

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceState,
  StoragePoolState}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, StartStoreTransfer, TxPrepare}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Metadata, ObjectId, ObjectRefcount,
  ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription,
  TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.{DeviceMarkers, DurableFiles, GossipRetransmitter, Host, StorageDeviceConfig,
  StorageDeviceManager, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{AdmissionTier, CrashRecoveryLog, CrashRecoveryLogFactory, CrlAdmission, CrlCorrupt,
  StoreRecoveryState, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.crl.simple.{Recovery, SimpleCRL}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{AdoptOutcome, RebuildOutcome, ShutdownOutcome,
  TransferOutcome}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.{Frontend, StoreIncarnationFile}
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
                          completions: ExecutionContext, readHold: Option[Future[Unit]] = None)
  extends CrashRecoveryLog:
  val firstSave: Promise[Unit] = Promise()
  val shutdownCalled: Promise[Unit] = Promise()
  val shutdownCalls = new AtomicInteger(0)

  def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = inner.getFullRecoveryState(storeId)
  /** Completed when the store state is first read for an extraction */
  val readCalled: Promise[Unit] = Promise()
  /** While set, the next read of a store's state completes only once it completes: a host stopped there crashes in
   *  the window between the read and the save file's write. Later reads (a store's recovery) are not gated */
  @volatile var readGate: Option[Future[Unit]] = None
  def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = inner.getAppliedStubs(storeId)
  def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
    inner.saveAppliedStub(storeId, txid, HLCTimestamp.now, completionHandler)
  def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit = inner.deleteAppliedStub(storeId, txid)
  override def whenDurable(storeId: StoreId, handler: () => Unit): Unit = inner.whenDurable(storeId, handler)
  /** Completed when a read of a store's state is first asked for, held or not */
  val readAsked: Promise[Unit] = Promise()
  override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
    readAsked.trySuccess(())
    readHold match
      // Asked of `inner` only once the hold completes: a read the store issued before its CRL died meets the death
      case Some(hold) if !hold.isCompleted =>
        hold.flatMap(_ => issueRead(storeId))(using ExecutionContext.parasitic)
      case _ => issueRead(storeId)
  private def issueRead(storeId: StoreId): Future[StoreRecoveryState] =
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
  /** While set, loadStore waits for it before saving the state */
  @volatile var loadGate: Option[Future[Unit]] = None
  override def loadStore(storeId: StoreId, state: StoreRecoveryState): Future[Unit] =
    loadGate match
      case None => inner.loadStore(storeId, state)
      case Some(gate) => gate.flatMap(_ => inner.loadStore(storeId, state))(using ExecutionContext.parasitic)
  def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
    inner.save(txid, state, () => completions.execute(() => { completionHandler(); firstSave.trySuccess(()) }))
  def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit =
    inner.dropTransactionObjectData(storeId, txid)
  def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = inner.deleteTransaction(storeId, txid)
  override def admissionLimitBytes: Long = inner.admissionLimitBytes
  override def maxTransactionBytes: Long = inner.maxTransactionBytes
  override def maxAdmissionLimitBytes: Long = inner.maxAdmissionLimitBytes
  override def requestGrowth(): Unit = inner.requestGrowth()
  override def onAdmissionLimitRaised(f: Long => Unit): Unit = inner.onAdmissionLimitRaised(f)
  override def deleteTransactions(storeId: StoreId, txids: List[TransactionId]): Future[Unit] =
    inner.deleteTransactions(storeId, txids)
  override def storesWithEntries(): Future[Set[StoreId]] = inner.storesWithEntries()
  override def recoveredBytesByStore: Map[StoreId, Long] = inner.recoveredBytesByStore
  override def failed: Future[Throwable] = inner.failed

  /** Completed when dropStore is first called */
  val dropCalled: Promise[Unit] = Promise()
  /** How many times dropStore has been called */
  val dropCalls = new AtomicInteger(0)
  /** While set, dropStore waits for it before dropping */
  @volatile var dropGate: Option[Future[Unit]] = None
  /** Completed when a dropStore has completed */
  val dropDone: Promise[Unit] = Promise()
  override def dropStore(storeId: StoreId): Future[Unit] =
    dropCalled.trySuccess(())
    dropCalls.incrementAndGet()
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
private class RecordingCrlFactory(completions: ExecutionContext, gate: Option[CountDownLatch] = None,
                                  readHold: Option[Future[Unit]] = None)
  extends CrashRecoveryLogFactory:
  private val inner = SimpleCRL.DeviceFactory()
  private var crls: List[RecordedCrl] = Nil

  def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
    val crl = new RecordedCrl(devicePath, inner.openCRL(devicePath, config, holdsStores), gate, completions, readHold)
    synchronized { crls = crls :+ crl }
    crl

  def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
    inner.createCRL(devicePath, config, dir)

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
  def markRebuiltStoreActive(storageDeviceId: StorageDeviceId, storeId: StoreId, episode: Option[UUID],
                             gossipStoreOnline: StoreId => Unit): Future[RebuildOutcome] =
    base.markRebuiltStoreActive(storageDeviceId, storeId, episode, gossipStoreOnline)
  def adoptDevice(storageDeviceId: StorageDeviceId, previousHostId: HostId, devicePath: os.Path,
                  uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                  crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                  moveToLost: StoreId => Unit): Future[AdoptOutcome] =
    base.adoptDevice(storageDeviceId, previousHostId, devicePath, uncleanStores, crlLostStores, moveToLost)
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
  override def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit): Future[Backend] =
    onFatals = onFatal :: onFatals
    storeCfg.backend match
      case _: StoreConfig.RocksDB =>
        Future.fromTry(scala.util.Try(new RocksDBBackend(storePath, storeCfg.storeId, ec):
          override def hasFailed: Boolean = super.hasFailed || FailingRocksOpener.this.failing))
      case _ => super.open(storeCfg, storePath, onFatal)

class DeviceCrlSuite extends IntegrationTestSuite with ControlFixture:

  private val poolId = storeId.poolId
  private val oid = ObjectId(new UUID(9, 2))
  private val ptr = DataObjectPointer(oid, poolId, Array[Byte]())
  private val rev0 = ObjectRevision(TransactionId(new UUID(9, 3)))
  private val txid = TransactionId(new UUID(9, 4))
  private val ida = Replication(3, 2)

  private val factories = scala.collection.mutable.ListBuffer[RecordingCrlFactory]()

  /** How long pumpUntil and pumpWithTx wait for their condition */
  private val PumpTimeout = Duration(10, SECONDS)
  /** Rounds pumpLoop runs before it starts sleeping between them */
  private val FastRounds = 2000

  override def subFixtureTeardown(): Unit =
    factories.foreach(_.shutdownAll())
    factories.clear()
    super.subFixtureTeardown()

  private def newFactory(gate: Option[CountDownLatch] = None,
                         readHold: Option[Future[Unit]] = None): RecordingCrlFactory =
    val f = new RecordingCrlFactory(executionContext, gate, readHold)
    factories += f
    f

  /** A device whose CRL files are 1 MB each */
  private def writeSmallCrlDevice(hostRoot: os.Path, name: String, deviceId: StorageDeviceId,
                                  owner: HostId = HostId.BootstrapHostId): os.Path =
    val dir = writeDevice(hostRoot, name, deviceId, owner = Some(owner))
    os.write.over(dir / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(deviceId, systemId, crlFileSizeMb = Some(1)).yamlConfig)
    dir

  /** A real Host, which opens RocksDB backends and its devices' CRLs through `crlFactory`, over `hostRoot` */
  private def newCrlHost(hostRoot: os.Path, crlFactory: CrashRecoveryLogFactory,
                         hostId: HostId = HostId.BootstrapHostId, seams: Host.Seams = Host.Seams.default,
                         rebuilds: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                         maxConcurrentRebuilds: Int = 2): Host =
    new Host(client, hostId, systemId, hostRoot, executionContext,
      new LockstepThreading(executionContext), net.objectCacheFactory, net,
      BackgroundTaskManager.NoBackgroundTaskManager, crlFactory, net.FinalizerFactory,
      TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
      StoreTransferService.Disabled, rebuilds, maxConcurrentRebuilds, seams = compared(seams))

  /** Drains `host`'s events and yields until `condition` holds. The caller asserts it.
   *
   *  The conditions wait on work done on other threads (RocksDB loads, CRL saves), so the wait is bounded by time
   *  (PumpTimeout), not by a count of yields: under load a count runs out before the work completes. `rounds` instead
   *  pumps exactly that many times at most, for callers showing that something does not happen.
   */
  private def pumpUntil(host: Host, condition: => Boolean, rounds: Int = -1): Future[Unit] =
    pumpLoop(host, condition, rounds, System.nanoTime() + PumpTimeout.toNanos)(Future(())(using executionContext))

  /** As pumpUntil, also letting the client's transactions complete each round */
  private def pumpWithTx(host: Host, condition: => Boolean, rounds: Int = -1): Future[Unit] =
    given ExecutionContext = executionContext
    pumpLoop(host, condition, rounds, System.nanoTime() + PumpTimeout.toNanos):
      yieldUntil(false, 5).flatMap(_ => waitForTransactionsToComplete())

  /** Runs `round` between drains of `host` until `condition`, `rounds` drains (when not negative) or `deadline`.
   *  Past the first FastRounds it also sleeps a millisecond a round, so a long wait does not spin a core
   */
  private def pumpLoop(host: Host, condition: => Boolean, rounds: Int, deadline: Long, done: Int = 0)
                      (round: => Future[Unit]): Future[Unit] =
    given ExecutionContext = executionContext
    host.testingOnlyHandleEvents()
    if condition || rounds == done || (rounds < 0 && System.nanoTime() - deadline > 0) then
      Future.unit
    else
      if rounds < 0 && done >= FastRounds then
        Thread.sleep(1)
      round.flatMap(_ => pumpLoop(host, condition, rounds, deadline, done + 1)(round))

  private def overwriteTxd(id: TransactionId, leader: Byte, pool: PoolId = poolId,
                           objectId: ObjectId = oid): TransactionDescription =
    val p = DataObjectPointer(objectId, pool, Array[Byte]())
    // MaxStartLead ahead of the host's clock now: no earlier than the forget floor of the stores this host loaded
    // before it was built (Frontend.forgetFloorMillis: a store refuses a transaction started before its load plus
    // MaxStartLead), and never further ahead of the host's clock than MaxStartLead, so not muted
    val start = HLCTimestamp((System.currentTimeMillis() + Frontend.MaxStartLead.toMillis) << 16)
    TransactionDescription(id, start, p, leader,
      List(DataUpdate(p, rev0, DataUpdateOperation.Overwrite)), Nil, None, Nil, Nil, ida, Map(pool -> ida))

  /** A RocksDB store `sid` in `deviceDir` holding the suite's object at rev0 */
  private def writeRocksStore(deviceDir: os.Path, sid: StoreId, at: Option[os.Path] = None): Future[os.Path] =
    given ExecutionContext = executionContext
    // In live/, under a fresh copy id unless put back `at` a path it had
    val storeDir = at.getOrElse(CopyFixtures.freshCopyPath(deviceDir, sid))
    DefaultBackendOpener.createStore(StoreConfig(sid, StoreConfig.RocksDB(None), None, org.aspen_ddp.aspen.common.ida.Replication(3, 2)), storeDir,
      deviceDir / StoreCreation.CreatingDirectory)
    // Not the suite's serial context: bootstrapAllocate blocks on work it runs there
    val initial = new RocksDBBackend(storeDir, sid, ExecutionContext.global)
    initial.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))
    initial.close().map(_ => storeDir)

  /** Sends `sid` a prepare of transaction `id` and waits for its vote to be saved in `crl` */
  private def prepareInFlight(host: Host, crl: RecordedCrl, sid: StoreId, id: TransactionId): Future[Unit] =
    given ExecutionContext = executionContext
    host.receiveTransactionMessage(TxPrepare(sid, net.storeId0, overwriteTxd(id, 1, sid.poolId),
      ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), true))
    pumpUntil(host, crl.firstSave.isCompleted).map(_ => crl.firstSave.isCompleted should be(true))

  /** Whether `host` serves `sid` with transaction `id` recovered and holding its lock on the object */
  private def recovered(host: Host, sid: StoreId, id: TransactionId): Boolean =
    host.control.router.loaded.get(sid).exists: s =>
      s.frontend.transactions.contains(id) && s.objectCache.get(oid).exists(_.lockedToTransaction.contains(id))

  private def noMoveFiles(deviceDir: os.Path, sid: StoreId): Boolean =
    !CopyFixtures.phaseOf(deviceDir, sid).exists(_.isInstanceOf[Phase.Outbound]) &&
      CopyFixtures.copyPath(deviceDir, sid).forall(dir => !os.exists(Backend.crlSaveFile(dir)))

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
      filesAfterShutdown = noMoveFiles(oldDir, sid)
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
      noMoveFiles(newDir, sid) should be(true)
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
      // The old host keeps the device loaded until it relinquishes it: its disk says nothing of its copies now
      _ = oldHost.testingOnlySkipDiskCheck(deviceA)
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
      noMoveFiles(newDir, sid) should be(true)
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
      markedWhileRefused = markedLost(deviceDir, sid)
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
      markedLost(deviceDir, sid) should be(false)

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
      noMoveFiles(deviceDir, storeId) should be(true)
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
        ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), true))
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

  /** A state for `sid` of a transaction overwriting the suite's object with `data` */
  private def recoveryState(sid: StoreId, id: TransactionId, data: Array[Byte]): TransactionRecoveryState =
    TransactionRecoveryState(sid, overwriteTxd(id, 1, sid.poolId).serialize(), List(ObjectUpdate(oid, DataBuffer(data))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState(None, None))

  atest("a device's admission counts, from its open, what its CRL holds for every store, loaded or not"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val unloaded = StoreId(poolId, 7.toByte) // No directory: never loads
    val loadedTrs = recoveryState(storeId, txid, Array[Byte](1, 2, 3))
    val unloadedTrs = recoveryState(unloaded, TransactionId(new UUID(9, 5)), new Array[Byte](1000))
    val first = newFactory()
    val crl = first.openCRL(deviceDir, StorageDeviceConfig.loadStorageDeviceConfig(
      deviceDir / StorageDeviceConfig.configFilename), holdsStores = false)
    val saved = Promise[Unit]()
    crl.save(txid, loadedTrs, () => ())
    crl.save(unloadedTrs.txd.transactionId, unloadedTrs, () => saved.trySuccess(()))
    val loadedBytes = CrlAdmission.recoveredBytes(loadedTrs)
    val unloadedBytes = CrlAdmission.recoveredBytes(unloadedTrs)
    for
      _ <- saved.future
      _ = first.shutdownAll()
      _ <- writeRocksStore(deviceDir, storeId)
      host = newCrlHost(hostRoot, newFactory())
      admission = host.control.devices.devices(deviceA).admission
      // Counted at the open, before any store loads
      atOpen = (admission.admittedBytes, admission.retainedBytes(storeId), admission.retainedBytes(unloaded))
      _ <- pumpUntil(host, host.control.router.loaded.get(storeId).exists(_.frontend.transactions.contains(txid)))
      afterLoad = (admission.admittedBytes, admission.retainedBytes(storeId), admission.retainedBytes(unloaded))
      _ <- host.control.router.loaded(storeId).backend.close()
    yield
      atOpen should be((loadedBytes + unloadedBytes, loadedBytes, unloadedBytes))
      // The loaded store counts its own afresh, in place of what the open retained for it: counted once
      afterLoad._2 should be(0L)
      afterLoad._1 should be(loadedBytes + afterLoad._3)

  atest("a device whose CRL fails is unloaded and not loaded again, and its stores are taken offline"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    val factory = newFactory()
    for
      _ <- writeRocksStore(deviceDir, storeId)
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      // An entry larger than a whole 1 MB stream: Stream.writeEntry's require kills the CRL thread
      _ = crl.inner.save(TransactionId(new UUID(9, 6)),
            recoveryState(storeId, TransactionId(new UUID(9, 6)), new Array[Byte](2 * 1024 * 1024)), () => ())
      _ <- crl.failed
      _ <- pumpUntil(host, !host.control.devices.devices.contains(deviceA) && crl.shutdownCalls.get > 0)
      rescanned = host.control.discovery.checkForNewDevices()
    yield
      host.control.devices.devices.contains(deviceA) should be(false)
      crl.shutdownCalls.get should be >= 1
      host.control.router.loaded.contains(storeId) should be(false)
      host.control.router.isOffline(storeId) should be(true)
      host.control.discovery.disownedDeviceDirs should contain(deviceDir)
      rescanned should be(Nil)
      factory.opened.size should be(1)

  atest("a store whose recovery read its CRL's death fails is closed with the device, unmarked (CRL-W12)"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    val hold = Promise[Unit]()
    val factory = newFactory(readHold = Some(hold.future))
    for
      storeDir <- writeRocksStore(deviceDir, storeId)
      host = newCrlHost(hostRoot, factory)
      crl = factory.opened.head
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId) && crl.readAsked.isCompleted)
      // An entry larger than a whole 1 MB stream: Stream.writeEntry's require kills the CRL thread
      _ = crl.inner.save(TransactionId(new UUID(9, 6)),
            recoveryState(storeId, TransactionId(new UUID(9, 6)), new Array[Byte](2 * 1024 * 1024)), () => ())
      _ <- crl.failed
      // The store's read meets the dead CRL before the device's unload has closed the store
      _ = hold.success(())
      _ <- pumpUntil(host, !host.control.devices.devices.contains(deviceA) && crl.shutdownCalls.get > 0)
    yield
      host.control.router.loaded.contains(storeId) should be(false)
      host.control.router.isOffline(storeId) should be(true)
      withClue("its state is intact in the CRL files, which a restart recovers: "):
        markedLost(deviceDir) should be(false)

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

  private def writeStoreDir(deviceDir: os.Path, sid: StoreId = storeId): os.Path =
    val storeDir = CopyFixtures.freshCopyPath(deviceDir, sid)
    DefaultBackendOpener.createStore(StoreConfig(sid, StoreConfig.RocksDB(None), None, org.aspen_ddp.aspen.common.ida.Replication(3, 2)), storeDir,
      storeDir / os.up / os.up / StoreCreation.CreatingDirectory)
    storeDir

  /** Gives the device an empty CRL, as one that has run before has */
  private def createCrl(deviceDir: os.Path): Unit =
    newFactory().openCRL(deviceDir, StorageDeviceConfig.loadStorageDeviceConfig(
      deviceDir / StorageDeviceConfig.configFilename), holdsStores = false).shutdown()

  /** Whether the copy of `sid` on the device at `deviceDir` is Lost (in lost/) */
  private def markedLost(deviceDir: os.Path, sid: StoreId = storeId): Boolean =
    CopyFixtures.phaseOf(deviceDir, sid).contains(Phase.Lost)

  atest("a device whose CRL cannot be recovered moves its stores to lost"):
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
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
        if calls.incrementAndGet() == 1 then
          throw Recovery.BrokenEntryChain("unrecoverable")
        inner.openCRL(devicePath, config, holdsStores)
      def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
        inner.createCRL(devicePath, config, dir)
    val host = newCrlHost(hostRoot, failingFirst)

    Future.successful:
      markedLost(deviceDir) should be(true)
      host.control.router.loaded.contains(storeId) should be(false)
      val lost = os.list(deviceDir).filter(_.last.startsWith("crl.lost-"))
      lost.size should be(1)
      os.list(lost.head).map(_.last).toSet should be(oldFiles)
      calls.get should be(2)
      inner.opened.size should be(1)
      os.exists(crlDir / "0.crl") should be(true)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  atest("a device whose CRL fails its checksums moves its stores to lost"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val inner = newFactory()
    val calls = new AtomicInteger(0)
    val corruptFirst = new CrashRecoveryLogFactory:
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
        if calls.incrementAndGet() == 1 then
          throw CrlCorrupt("corrupt")
        inner.openCRL(devicePath, config, holdsStores)
      def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
        inner.createCRL(devicePath, config, dir)
    val host = newCrlHost(hostRoot, corruptFirst)

    Future.successful:
      markedLost(deviceDir) should be(true)
      os.list(deviceDir).count(_.last.startsWith("crl.lost-")) should be(1)
      calls.get should be(2)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  atest("a lost device CRL does not mark a store whose valid save file holds its whole state"):
    given ExecutionContext = executionContext
    val inner = newFactory()
    val calls = new AtomicInteger(0)
    val corruptFirst = new CrashRecoveryLogFactory:
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
        if calls.incrementAndGet() == 1 then
          throw CrlCorrupt("corrupt")
        inner.openCRL(devicePath, config, holdsStores)
      def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
        inner.createCRL(devicePath, config, dir)
    for
      (hostRoot, deviceDir, saved) <- activeStoreDevice()
      // Its state is in its own save file (an arrived transfer, a transfer out's source, a rebuilt store kept offline)
      _ = CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState(), Backend.crlSaveFile(saved))
      // Its state was in the device CRL
      inCrl = writeStoreDir(deviceDir, StoreId(poolId, 1.toByte))
      // A save file that is not its own is no state of its own
      foreign = writeStoreDir(deviceDir, StoreId(poolId, 2.toByte))
      _ = CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState(), Backend.crlSaveFile(foreign))
      host = newCrlHost(hostRoot, corruptFirst)
      lost = os.list(deviceDir).count(_.last.startsWith("crl.lost-"))
      markers = List(storeId, StoreId(poolId, 1.toByte), StoreId(poolId, 2.toByte)).map(markedLost(deviceDir, _))
      // It loads from its file over the fresh CRL, as it would have over the old one. The file goes once its state
      // is in the CRL
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId) && !os.exists(Backend.crlSaveFile(saved)))
    yield
      lost should be(1)
      markers should be(List(false, true, true))
      host.control.router.loaded.contains(storeId) should be(true)
      os.exists(Backend.crlSaveFile(saved)) should be(false)

  // A read that fails says nothing of the file's content: deciding which stores hold their own state is left for the
  // next scan, before any store is marked
  for what <- List("save file", "store config") do
    atest(s"a lost device CRL whose store's $what cannot be read is left for the next scan, marking nothing (SL-W18)"):
      given ExecutionContext = executionContext
      val inner = newFactory()
      // Corrupt until it has been kept aside: the replacement opens
      val corruptUntilReplaced = new CrashRecoveryLogFactory:
        def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
          if !os.list(devicePath).exists(_.last.startsWith("crl.lost-")) then
            throw CrlCorrupt("corrupt")
          inner.openCRL(devicePath, config, holdsStores)
        def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
          inner.createCRL(devicePath, config, dir)
      for
        (hostRoot, deviceDir, saved) <- activeStoreDevice()
        // Its whole state is in its own save file
        _ = CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState(), Backend.crlSaveFile(saved))
        inCrl = writeStoreDir(deviceDir, StoreId(poolId, 1.toByte))
        // Its stat succeeds and its open fails (EACCES)
        unreadable = if what == "save file" then Backend.crlSaveFile(saved) else saved / StoreConfig.configFilename
        _ = os.perms.set(unreadable, "---------")
        host = newCrlHost(hostRoot, corruptUntilReplaced)
        _ = host.control.reconciler.scan()
        loadedWhileUnreadable = host.control.devices.devices.contains(deviceA)
        lostWhileUnreadable = os.list(deviceDir).count(_.last.startsWith("crl.lost-"))
        markedWhileUnreadable = List(storeId, StoreId(poolId, 1.toByte)).map(markedLost(deviceDir, _))
        _ = os.perms.set(unreadable, "rw-------")
        _ = host.control.reconciler.scan()
        _ <- pumpUntil(host, host.control.router.loaded.contains(storeId) && !os.exists(Backend.crlSaveFile(saved)))
      yield
        loadedWhileUnreadable should be(false)
        lostWhileUnreadable should be(0)
        markedWhileUnreadable should be(List(false, false))
        os.list(deviceDir).count(_.last.startsWith("crl.lost-")) should be(1)
        List(storeId, StoreId(poolId, 1.toByte)).map(markedLost(deviceDir, _)) should be(List(false, true))
        host.control.router.loaded.contains(storeId) should be(true)

  /** A factory over `inner` whose first open finds the CRL corrupt */
  private def corruptOnFirstOpen(inner: RecordingCrlFactory): CrashRecoveryLogFactory =
    val calls = new AtomicInteger(0)
    new CrashRecoveryLogFactory:
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
        if calls.incrementAndGet() == 1 then
          throw CrlCorrupt("corrupt")
        inner.openCRL(devicePath, config, holdsStores)
      def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
        inner.createCRL(devicePath, config, dir)

  /** A finished rebuild of `storeId` in rebuilt/ for `episode`, on a device whose entry for
   *  it is `entry` and whose CRL is lost at the host's start. Returns the store's directory, the host and its
   *  recorded rebuilds once the host's first check of the device has settled, with the device's directory first. */
  private def finishedRebuildOnLostCrl(entry: StorageDeviceState.StoreEntry,
                                       episode: Option[UUID]): Future[(os.Path, os.Path, Host, RecordingRebuildFactory)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    val rebuilds = new RecordingRebuildFactory
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(storeId -> entry)))
      _ <- waitForTransactionsToComplete()
      storeDir <- writeRocksStore(deviceDir, storeId)
      // As RebuildingStore.finish leaves it. It never served, so the lost CRL held nothing of it
      _ = landRebuiltCopy(deviceDir, episode)
      host = newCrlHost(hostRoot, corruptOnFirstOpen(newFactory()), rebuilds = rebuilds)
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, host.control.reconciler.activeDeviceChecks.isEmpty &&
                            (host.control.router.loaded.contains(storeId) || rebuilds.created.nonEmpty))
    yield (deviceDir, storeDir, host, rebuilds)

  private def pendingDeletes(storeDir: os.Path): List[os.Path] =
    val dir = storeDir / os.up / RebuildingStore.PendingDeleteDirectory
    if os.exists(dir) then os.list(dir).toList else Nil

  atest("a lost device CRL does not mark a finished rebuild whose flip committed: it loads as it would have"):
    given ExecutionContext = executionContext
    for
      (deviceDir, storeDir, host, rebuilds) <- finishedRebuildOnLostCrl(
        StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None), None)
      served = host.control.router.loaded.get(storeId).map(_.backend.path)
      _ <- host.control.router.loaded.get(storeId).fold(Future.unit)(_.backend.close())
    yield
      markedLost(deviceDir) should be(false)
      rebuilds.created should be(empty)
      pendingDeletes(storeDir) should be(Nil)
      served should be(Some(storeDir))
      CopyFixtures.phaseOf(deviceDir, storeId).exists(_.isInstanceOf[Phase.Rebuilt]) should be(false)

  atest("a lost device CRL does not mark a finished rebuild awaiting its flip: its rebuild flips that copy"):
    given ExecutionContext = executionContext
    val episode = UUID.randomUUID()
    for
      (deviceDir, storeDir, host, rebuilds) <- finishedRebuildOnLostCrl(
        StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode)), Some(episode))
      markedAtStart = markedLost(deviceDir)
      retired = pendingDeletes(storeDir)
      _ = rebuilds.created.map(r => (r.storeId, r.episode)).toList should be(List((storeId, Some(episode))))
      // The copy is the finished rebuild: start() takes it for done
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpWithTx(host, host.control.router.loaded.contains(storeId))
      served = host.control.router.loaded.get(storeId).map(_.backend.path)
      _ <- host.control.router.loaded.get(storeId).fold(Future.unit)(_.backend.close())
    yield
      markedAtStart should be(false)
      retired should be(Nil)
      served should be(Some(storeDir))
      CopyFixtures.phaseOf(deviceDir, storeId).exists(_.isInstanceOf[Phase.Rebuilt]) should be(false)

  /** A device holding a store whose CRL opens fail with an I/O error the first `failures` times */
  private def transientlyFailingCrlDevice(failures: Int): (os.Path, os.Path, RecordingCrlFactory, AtomicInteger, Host) =
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val inner = newFactory()
    val calls = new AtomicInteger(0)
    val failing = new CrashRecoveryLogFactory:
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
        if calls.incrementAndGet() <= failures then
          throw new java.io.IOException("transient")
        inner.openCRL(devicePath, config, holdsStores)
      def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
        inner.createCRL(devicePath, config, dir)
    (deviceDir, storeDir, inner, calls, newCrlHost(hostRoot, failing))

  atest("a transient CRL open failure leaves the stores untouched and a later scan loads the device"):
    // The constructor's open, then every scan but the last allowed
    val failures = DeviceDiscovery.CrlOpenAttempts
    val (deviceDir, storeDir, inner, calls, host) = transientlyFailingCrlDevice(failures)
    host.control.devices.devices.contains(deviceA) should be(false)
    markedLost(deviceDir) should be(false)
    for _ <- 2 to failures do
      host.control.reconciler.scan()
    host.control.devices.devices.contains(deviceA) should be(false)
    markedLost(deviceDir) should be(false)

    host.control.reconciler.scan()
    calls.get should be(failures + 1)
    Future.successful:
      markedLost(deviceDir) should be(false)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  atest("a CRL open failing transiently on CrlOpenAttempts device scans in a row is treated as lost"):
    // The constructor's open, uncounted, then CrlOpenAttempts scans
    val (deviceDir, storeDir, inner, calls, host) = transientlyFailingCrlDevice(1 + DeviceDiscovery.CrlOpenAttempts)
    for _ <- 1 until DeviceDiscovery.CrlOpenAttempts do
      host.control.reconciler.scan()
    markedLost(deviceDir) should be(false)

    // The last scan fails too: the CRL is lost and a fresh one opened
    host.control.reconciler.scan()
    Future.successful:
      markedLost(deviceDir) should be(true)
      os.list(deviceDir).count(_.last.startsWith("crl.lost-")) should be(1)
      calls.get should be(DeviceDiscovery.CrlOpenAttempts + 2)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  atest("only device scans count transient CRL open failures: host start and nudges never declare the CRL lost"):
    // Host start opens twice (constructor, startup CheckAllDevices), each round below twice, then the scans once each
    val uncounted = 2 + 2 * DeviceDiscovery.CrlOpenAttempts
    val (deviceDir, storeDir, inner, calls, host) =
      transientlyFailingCrlDevice(uncounted + DeviceDiscovery.CrlOpenAttempts)
    host.testingOnlyHandleEvents()
    for _ <- 1 to DeviceDiscovery.CrlOpenAttempts do
      host.control.reconciler.checkAll()
      host.control.reconciler.handleCheckMessage(CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))
    markedLost(deviceDir) should be(false)
    os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
    calls.get should be(uncounted)

    // Device scans count, CrlOpenAttempts in a row declaring it lost
    for _ <- 1 until DeviceDiscovery.CrlOpenAttempts do
      host.control.reconciler.scan()
    markedLost(deviceDir) should be(false)
    host.control.reconciler.scan()
    Future.successful:
      markedLost(deviceDir) should be(true)
      os.list(deviceDir).count(_.last.startsWith("crl.lost-")) should be(1)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head

  atest("a device whose CRL another opener holds is left unloaded, never lost, and loads once it is released"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    // A second writer of the same files: another host process, or a double mount
    val holder = newFactory().openCRL(deviceDir, StorageDeviceConfig.loadStorageDeviceConfig(
      deviceDir / StorageDeviceConfig.configFilename), holdsStores = false)
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)

    // Not a transient failure counting towards CrlOpenAttempts, nor a lost CRL, however often it is retried
    host.control.devices.devices.contains(deviceA) should be(false)
    for _ <- 1 to DeviceDiscovery.CrlOpenAttempts do
      host.control.discovery.checkForNewDevices(scan = true) should be(Nil)
    markedLost(deviceDir) should be(false)
    os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
    factory.opened should be(Nil)

    holder.shutdown()
    host.control.discovery.checkForNewDevices() should be(List(deviceA))
    Future.successful:
      markedLost(deviceDir) should be(false)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened.head

  atest("a device with stores but no crl dir moves them to lost"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)
    host.control.shutdown.unloadDevice(deviceA)
    val storeDir = writeStoreDir(deviceDir)
    os.remove.all(CrashRecoveryLog.deviceCrlDir(deviceDir))
    markedLost(deviceDir) should be(false)

    factory.opened.head.shutdownCalled.future.flatMap(_ => yieldUntil(host.control.devices.closingCrls.isEmpty)).map: _ =>
      host.control.discovery.checkForNewDevices() should be(List(deviceA))
      markedLost(deviceDir) should be(true)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
      os.exists(CrashRecoveryLog.deviceCrlDir(deviceDir) / "0.crl") should be(true)

  /** A device holding a store whose existing CRL directory then has `remove` applied to it: the CRL is lost, its
   *  remains kept aside, and a fresh CRL opened */
  private def crlDirLosesFiles(remove: os.Path => Unit): Future[org.scalatest.Assertion] =
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)
    remove(crlDir)
    val remains = os.list(crlDir).map(_.last).toSet
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)

    Future.successful:
      markedLost(deviceDir) should be(true)
      host.control.router.loaded.contains(storeId) should be(false)
      val lost = os.list(deviceDir).filter(_.last.startsWith("crl.lost-"))
      lost.size should be(1)
      os.list(lost.head).map(_.last).toSet should be(remains)
      os.exists(crlDir / "0.crl") should be(true)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened.last

  atest("a device with stores whose crl dir lost its stream files moves them to lost"):
    crlDirLosesFiles(dir => os.list(dir).filter(_.ext == "crl").foreach(os.remove(_)))

  atest("a device with stores whose crl dir lost its stream files and sizing moves them to lost"):
    crlDirLosesFiles(dir => os.list(dir).filter(f => f.ext == "crl" || f.last == SimpleCRL.SizingFile)
      .foreach(os.remove(_)))

  atest("a device with stores whose crl dir holds only unfinished stream files moves them to lost"):
    crlDirLosesFiles(dir => os.list(dir).filter(_.ext == "crl").foreach(f => os.move(f, dir / s"${f.last}.tmp")))

  atest("a new device with no stores just creates its CRL"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val host = newCrlHost(hostRoot, newFactory())

    Future.successful:
      host.control.devices.devices.keySet should be(Set(deviceA))
      os.exists(CrashRecoveryLog.deviceCrlDir(deviceDir) / "0.crl") should be(true)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)

  atest("a replacement CRL whose creation fails leaves no crl dir, and the next scan creates it without moving more aside"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val inner = newFactory()
    val opens = new AtomicInteger(0)
    val creates = new AtomicInteger(0)
    val factory = new CrashRecoveryLogFactory:
      def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
        if opens.incrementAndGet() == 1 then
          throw CrlCorrupt("corrupt")
        inner.openCRL(devicePath, config, holdsStores)
      def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit =
        if creates.incrementAndGet() == 1 then
          // A full device: one stream preallocated, the next refused
          os.write(dir / SimpleCRL.SizingFile, "partial")
          os.write(dir / "0.crl.tmp", "partial")
          throw new java.io.IOException("No space left on device")
        inner.createCRL(devicePath, config, dir)
    val host = newCrlHost(hostRoot, factory)
    val crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)

    host.control.devices.devices.contains(deviceA) should be(false)
    markedLost(deviceDir) should be(true)
    os.exists(crlDir) should be(false)
    os.exists(CrashRecoveryLog.replacementCrlDir(deviceDir)) should be(false)
    os.list(deviceDir).count(_.last.startsWith("crl.lost-")) should be(1)

    host.control.reconciler.scan()
    Future.successful:
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs inner.opened.head
      os.list(deviceDir).count(_.last.startsWith("crl.lost-")) should be(1)
      os.exists(crlDir / "0.crl") should be(true)
      os.exists(CrashRecoveryLog.replacementCrlDir(deviceDir)) should be(false)
      opens.get should be(2)
      creates.get should be(2)

  atest("an unfinished replacement CRL left by a crash is removed at device load and the CRL created afresh"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    // A crash during the renames of the replacement's stream files, after the old crl/ was moved aside
    createCrl(deviceDir)
    val replacement = CrashRecoveryLog.replacementCrlDir(deviceDir)
    DurableFiles.move(CrashRecoveryLog.deviceCrlDir(deviceDir), replacement)
    os.list(replacement).filter(f => f.ext == "crl" && f.last != "0.crl").foreach(f =>
      os.move(f, replacement / s"${f.last}.tmp"))
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)

    Future.successful:
      markedLost(deviceDir) should be(true)
      os.exists(replacement) should be(false)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
      os.list(CrashRecoveryLog.deviceCrlDir(deviceDir)).exists(_.last.endsWith(".tmp")) should be(false)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened.head

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
      deviceDir / StorageDeviceConfig.configFilename), holdsStores = false)
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
      discarded = CopyFixtures.copyPath(deviceDir, storeId).isEmpty
      // The drop is queued on the CRL thread ahead of this read
      leftInCrl = held(host.control.devices.devices(deviceA).crl, storeId)
      _ <- writeRocksStore(deviceDir, storeId)
      _ = landRebuiltCopy(deviceDir, rebuilds.created.head.episode)
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpWithTx(host, host.control.router.loaded.contains(storeId))
    yield (discarded, leftInCrl)

  atest("a store that fails during its close is moved to lost before its device-CRL entries go"):
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
      markedAfterClose = markedLost(deviceDir)
      heldAfterClose = held(crl, storeId)
      (discarded, leftInCrl) <- rebuildAndServe(host, rebuilds, deviceDir, storeDir)
      rebuilt = host.control.router.loaded(storeId)
      recoveredByCopy = rebuilt.frontend.transactions.keySet
      _ <- rebuilt.backend.close()
      _ = factory.shutdownAll()
    yield
      closed.failed.toOption should be(Some(Host.StoreFailedOnClose(storeId)))
      markedAfterClose should be(true)
      // Nothing dropped by the close itself: the move to lost/ comes first
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
      _ = CopyFixtures.markCopy(deviceDir, storeId, Phase.Lost)
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

  // rebuild-process 5.6: a rebuild over a path whose copy left without a retire (removed by hand) finds nothing to
  // discard, so the device CRL still holds the old copy's entries when the rebuilt copy first loads
  atest("a rebuilt copy's first load does not recover entries a copy removed without a retire left in the device CRL"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    val rebuilds = new RecordingRebuildFactory
    val episode = UUID.randomUUID()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      _ = os.remove.all(storeDir)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(storeId ->
             StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode)))))
      _ <- waitForTransactionsToComplete()
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      heldAtRestart = held(second.opened.head, storeId)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, rebuilds.created.nonEmpty)
      _ <- writeRocksStore(deviceDir, storeId)
      _ = landRebuiltCopy(deviceDir, Some(episode))
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpWithTx(restarted, restarted.control.router.loaded.contains(storeId) &&
                                 !os.exists(Backend.crlSaveFile(storeDir)))
      rebuilt = restarted.control.router.loaded(storeId)
      recoveredByCopy = rebuilt.frontend.transactions.keySet
      leftInCrl = held(second.opened.head, storeId)
      _ <- rebuilt.backend.close()
      _ = second.shutdownAll()
    yield
      heldAtRestart should be(Set(txid))
      recoveredByCopy should be(Set())
      leftInCrl should be(Set())
      heldOnDisk(deviceDir, storeId) should be(Set())

  /** Writes into the store directory at `storeDir` the files RebuildingStore.finish leaves beside the rebuilt
   *  backend, for a test whose rebuild only lands a store there: the copy of `storeId` on the device at `deviceDir` */
  private def landRebuiltCopy(deviceDir: os.Path, episode: Option[UUID]): Unit =
    RebuildingStore.writeEmptyCrlState(CopyFixtures.copyPath(deviceDir, storeId).get, storeId)
    CopyFixtures.markCopy(deviceDir, storeId, Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode)))

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
      _ = CopyFixtures.markCopy(deviceDir, storeId, Phase.Lost)
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
      _ = landRebuiltCopy(deviceDir, rebuilds.created.head.episode)
      _ = rebuilds.created.head.completeFromDisk()
      _ <- pumpWithTx(restarted, restarted.control.router.loaded.contains(storeId) && copies.forall(!os.exists(_)))
      deletedOnceActive = copies.forall(!os.exists(_))
      _ <- restarted.control.router.loaded(storeId).backend.close()
      _ = second.shutdownAll()
    yield
      copies.size should be(1)
      saved.map(_.map(_.transactions.map(_.txd.transactionId))) should be(List(Right(List(txid))))
      leftInCrl should be(Set())
      keptWhileRebuilding should be(true)
      deletedOnceActive should be(true)

  atest("a copy retired with an unusable crl_save.log keeps the device CRL's entries beside it"):
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
      // A crash between a transfer out's save file and the deletion of its entries, then the file found unusable
      // (D3): the device CRL's entries are a second copy of the state, and may be the only readable one
      _ = os.write.over(Backend.crlSaveFile(storeDir), "damaged")
      _ = CopyFixtures.markCopy(deviceDir, storeId, Phase.Lost)
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, rebuilds.created.nonEmpty)
      copies = pendingCopies(deviceDir)
      kept = copies.map(c => CrashRecoveryLog.loadValidStoreState(c / RebuildingStore.LostCrlEntriesFile, storeId))
      saveFileKept = copies.map(c => os.read(c / RebuildingStore.LostCrlStateFile))
      leftInCrl = held(second.opened.head, storeId)
      _ = second.shutdownAll()
    yield
      copies.size should be(1)
      saveFileKept should be(List("damaged"))
      kept.map(_.map(_.transactions.map(_.txd.transactionId))) should be(List(Right(List(txid))))
      leftInCrl should be(Set())

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
      copy = deviceDir / RebuildingStore.PendingDeleteDirectory / CopyLayout.copyName(elsewhere, UUID.randomUUID())
      _ <- setPoolEntry(elsewhere, deviceB)
      _ <- setDeviceState(deviceB)(_.copy(stores = Map(
             elsewhere -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None))))
      _ <- waitForTransactionsToComplete()
      _ = os.makeDir.all(copy)
      _ = os.write.over(copy / StoreConfig.configFilename,
            StoreConfig(elsewhere, StoreConfig.RocksDB(None), None, org.aspen_ddp.aspen.common.ida.Replication(3, 2)).yamlConfig)
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

  /** The retired copies in `deviceDir`'s pending-delete directory */
  private def pendingCopies(deviceDir: os.Path): List[os.Path] =
    val dir = deviceDir / RebuildingStore.PendingDeleteDirectory
    if os.exists(dir) then os.list(dir).toList else Nil

  atest("a lost copy whose rebuild was re-placed away is retired with its CRL entries, not deleted"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      // Declared lost and queued for a rebuild slot, then re-placed onto another device: the entry leaves this one
      _ = CopyFixtures.markCopy(deviceDir, storeId, Phase.Lost)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map()))
      _ <- waitForTransactionsToComplete()
      restarted = newCrlHost(hostRoot, second)
      crl = second.opened.head
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, pendingCopies(deviceDir).nonEmpty && !restarted.control.copies.isRetiring(storeId))
      copies = pendingCopies(deviceDir)
      saved = copies.map(c => CrashRecoveryLog.loadValidStoreState(c / RebuildingStore.LostCrlStateFile, storeId))
      leftInCrl = held(crl, storeId)
      // The store is not Active anywhere its pool names: later checks keep the copy
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty &&
                                 restarted.control.reconciler.pendingDeleteLookupsInFlight.isEmpty)
      kept = copies.forall(os.exists(_))
      _ = second.shutdownAll()
    yield
      os.exists(storeDir) should be(false)
      copies.size should be(1)
      copies.map(c => StoreCopies.pendingDeleteStore(c.last)) should be(List(Some(storeId)))
      saved.map(_.map(_.transactions.map(_.txd.transactionId))) should be(List(Right(List(txid))))
      leftInCrl should be(Set())
      heldOnDisk(deviceDir, storeId) should be(Set())
      kept should be(true)

  atest("a store that fails during a transfer-out close keeps its extracted CRL state in its retired copy"):
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
      closing = host.control.loader.closeStore(storeId, extractCrl = true)
      _ <- pumpUntil(host, closing.isCompleted)
      closed <- closing.transform(r => scala.util.Success(r))
      _ = opener.failing = false
      // Moved to lost/ once its close ended (failedOnClose), its save file with it
      _ <- pumpUntil(host, markedLost(deviceDir))
      saveFileKept = CopyFixtures.copyPath(deviceDir, storeId).exists(c => os.exists(Backend.crlSaveFile(c)))
      // The extraction deleted them from the device CRL: the save file is the only copy
      heldAfterClose = held(crl, storeId)
      // The rebuild starts once the copy is retired; the copy is kept until the store is Active again
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, rebuilds.created.nonEmpty)
      copies = pendingCopies(deviceDir)
      saved = copies.map(c => CrashRecoveryLog.loadValidStoreState(c / RebuildingStore.LostCrlStateFile, storeId))
      _ = factory.shutdownAll()
    yield
      closed.failed.toOption should be(Some(Host.StoreFailedOnClose(storeId)))
      saveFileKept should be(true)
      heldAfterClose should be(Set())
      os.exists(storeDir) should be(false)
      saved.map(_.map(_.transactions.map(_.txd.transactionId))) should be(List(Right(List(txid))))
      copies.forall(c => !os.exists(Backend.crlSaveFile(c))) should be(true)

  atest("a pending-delete copy is identified by its name, never by its store config"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, _) <- activeStoreDevice()
      pendingDir = deviceDir / RebuildingStore.PendingDeleteDirectory
      // storeId is Active on this device: its copy goes, though its store config does not parse
      released = pendingDir / CopyLayout.copyName(storeId, UUID.randomUUID())
      _ = os.makeDir.all(released)
      _ = os.write.over(released / StoreConfig.configFilename, "not: [a store config")
      // A name that identifies no store copy is kept, whatever it holds
      unknown = pendingDir / "unknown"
      _ = os.makeDir.all(unknown)
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, host.control.reconciler.activeDeviceChecks.isEmpty && !os.exists(released))
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = factory.shutdownAll()
    yield
      os.exists(released) should be(false)
      os.exists(unknown) should be(true)
      // Deleted rename-first: nothing is left behind in deleting/ either
      os.list(deviceDir / StoreCopies.DeletingDirectory) should be(empty)

  atest("whatever an interrupted delete left in deleting/ is removed when the device loads"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, _) <- activeStoreDevice()
      leftover = deviceDir / StoreCopies.DeletingDirectory / s"${storeId.directoryName}-${UUID.randomUUID()}"
      _ = os.makeDir.all(leftover / "sub")
      _ = os.write.over(leftover / "sub" / "data", "partial")
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = factory.shutdownAll()
    yield
      os.exists(leftover) should be(false)

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

  atest("a store directory carrying acceptor-since loads with that acceptor start, and one without with none"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val since = HLCTimestamp(HLCTimestamp.now.asLong + (60000L << 16))
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      _ = os.write.over(storeDir / RebuildingStore.AcceptorSinceFile, since.asLong.toString)
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      loaded = host.control.router.loaded(storeId).frontend.acceptorSince
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = factory.shutdownAll()
      _ = os.remove(storeDir / RebuildingStore.AcceptorSinceFile)
      second = newFactory()
      restarted = newCrlHost(hostRoot, second)
      _ <- pumpUntil(restarted, restarted.control.router.loaded.contains(storeId))
      reloaded = restarted.control.router.loaded(storeId).frontend.acceptorSince
      _ <- restarted.control.router.loaded(storeId).backend.close()
      _ = second.shutdownAll()
    yield
      loaded should be(Some(since))
      reloaded should be(None)

  atest("a store keeps its incarnation across a restart"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      first = host.control.router.loaded(storeId).frontend.incarnation
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = factory.shutdownAll()
      second = newFactory()
      restarted = newCrlHost(hostRoot, second)
      _ <- pumpUntil(restarted, restarted.control.router.loaded.contains(storeId))
      reloaded = restarted.control.router.loaded(storeId).frontend.incarnation
      _ <- restarted.control.router.loaded(storeId).backend.close()
      _ = second.shutdownAll()
    yield
      os.exists(storeDir / StoreIncarnationFile.FileName) should be(true)
      reloaded should be(first)
      first should not be StoreIncarnation.default(storeId)

  atest("a store whose save file cannot be stat'd at its load is held back and retried, never recovered without it (X-W18)"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val fileTx = TransactionId(new UUID(9, 43))
    val fileState = StoreRecoveryState(List(recoveryState(storeId, fileTx, Array[Byte](4, 5))))
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      // Its whole state is in its save file (an arrived transfer): the device CRL holds nothing for it. The file is
      // reached through a link into a directory without search permission, so its stat alone fails (EACCES) while
      // the rest of the store directory reads
      locked = hostRoot / "locked"
      _ = os.makeDir(locked)
      _ = CrashRecoveryLog.saveStoreState(storeId, fileState, locked / "state.log")
      _ = os.symlink(Backend.crlSaveFile(storeDir), locked / "state.log")
      _ = os.perms.set(locked, "rw-------")
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.loader.awaitingRetryHasCallback(storeId).isDefined)
      servedWhileUnreadable = host.control.router.loaded.contains(storeId)
      awaiting = host.control.loader.awaitingRetryHasCallback(storeId)
      _ = os.perms.set(locked, "rwx------")
      fileKept = os.exists(Backend.crlSaveFile(storeDir))
      _ = host.control.reconciler.scan()
      // Recovered once the CRL holds the file's state, after its install
      _ <- pumpUntil(host, host.control.router.loaded.get(storeId).exists(_.frontend.transactions.contains(fileTx)))
      store = host.control.router.loaded(storeId)
      recoveredFromFile = store.frontend.transactions.contains(fileTx)
      _ <- store.backend.close()
      _ = factory.shutdownAll()
    yield
      servedWhileUnreadable should be(false)
      awaiting should be(defined)
      fileKept should be(true)
      recoveredFromFile should be(true)
      os.exists(Backend.crlSaveFile(storeDir)) should be(false)

  for gate <- List(RebuildingStore.AcceptorSinceFile, RebuildingStore.InheritedVotesFile) do
    atest(s"a rebuilt store whose $gate file cannot be stat'd at its load is held back, never served as a full acceptor (X-W29)"):
      given ExecutionContext = executionContext
      val factory = newFactory()
      val since = HLCTimestamp(HLCTimestamp.now.asLong + (3600L * 1000 << 16))
      val gateAfter = System.currentTimeMillis() + 3600L * 1000
      for
        (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
        // A rebuilt store: both gates written by its finish. The one under test is reached through a link into a
        // directory without search permission, so its stat alone fails (EACCES) while the rest of the directory reads
        _ = RebuildingStore.writeAcceptorSince(storeDir, since)
        _ = RebuildingStore.writeInheritedVotes(storeDir, gateAfter)
        locked = hostRoot / "locked"
        _ = os.makeDir(locked)
        _ = os.move(storeDir / gate, locked / gate)
        _ = os.symlink(storeDir / gate, locked / gate)
        _ = os.perms.set(locked, "rw-------")
        host = newCrlHost(hostRoot, factory)
        _ <- pumpUntil(host, host.control.loader.awaitingRetryHasCallback(storeId).isDefined)
        servedWhileUnreadable = host.control.router.loaded.contains(storeId)
        _ = os.perms.set(locked, "rwx------")
        _ = host.control.reconciler.scan()
        _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
        store = host.control.router.loaded(storeId)
        loadedSince = store.frontend.acceptorSince
        loadedGate = store.frontend.inheritedVotesAfter
        _ <- store.backend.close()
        _ = factory.shutdownAll()
      yield
        servedWhileUnreadable should be(false)
        loadedSince should be(Some(since))
        loadedGate should be(Some(gateAfter))

  atest("a store whose save file cannot be read at its load is held back and retried, never declared lost (SL-W18)"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val fileTx = TransactionId(new UUID(9, 45))
    val fileState = StoreRecoveryState(List(recoveryState(storeId, fileTx, Array[Byte](4, 5))))
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      saveFile = Backend.crlSaveFile(storeDir)
      // Its whole state is in its save file (an arrived transfer), whose stat succeeds and whose open fails (EACCES)
      _ = CrashRecoveryLog.saveStoreState(storeId, fileState, saveFile)
      _ = os.perms.set(saveFile, "---------")
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.loader.awaitingRetryHasCallback(storeId).isDefined)
      servedWhileUnreadable = host.control.router.loaded.contains(storeId)
      awaiting = host.control.loader.awaitingRetryHasCallback(storeId)
      marked = markedLost(deviceDir)
      _ = os.perms.set(saveFile, "rw-------")
      _ = host.control.reconciler.scan()
      // Recovered once the CRL holds the file's state, after its install
      _ <- pumpUntil(host, host.control.router.loaded.get(storeId).exists(_.frontend.transactions.contains(fileTx)))
      store = host.control.router.loaded(storeId)
      recoveredFromFile = store.frontend.transactions.contains(fileTx)
      _ <- store.backend.close()
      _ = factory.shutdownAll()
    yield
      servedWhileUnreadable should be(false)
      awaiting should be(defined)
      marked should be(false)
      recoveredFromFile should be(true)
      markedLost(deviceDir) should be(false)

  atest("a store whose incarnation file is missing is rebuilt, never given a fresh incarnation (SL-W15)"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      _ = os.remove(storeDir / StoreIncarnationFile.FileName)
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, markedLost(deviceDir))
      served = host.control.router.loaded.contains(storeId)
      _ = factory.shutdownAll()
    yield
      served should be(false)
      markedLost(deviceDir) should be(true)
      os.exists(storeDir / StoreIncarnationFile.FileName) should be(false)

  atest("a store whose incarnation file cannot be parsed is retried, the file never replaced (SL-W15)"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      file = storeDir / StoreIncarnationFile.FileName
      original = os.read(file).trim
      _ = os.write.over(file, "not a uuid")
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.loader.awaitingRetryHasCallback(storeId).isDefined)
      servedWhileUnreadable = host.control.router.loaded.contains(storeId)
      kept = os.read(file)
      marked = markedLost(deviceDir)
      // Readable again (a release that reads its own format): the next retry serves it under its own incarnation
      _ = os.write.over(file, original)
      _ = host.control.reconciler.scan()
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      store = host.control.router.loaded(storeId)
      incarnation = store.frontend.incarnation
      _ <- store.backend.close()
      _ = factory.shutdownAll()
    yield
      servedWhileUnreadable should be(false)
      kept should be("not a uuid")
      marked should be(false)
      incarnation.uuid.toString should be(original)

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
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      heldAfterClose = held(crl, storeId)
      saveFileAfterClose = os.exists(Backend.crlSaveFile(storeDir))
      _ <- host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      heldAfterExtract = held(crl, storeId)
      _ = factory.shutdownAll()
    yield
      heldAfterClose should be(Set(txid))
      saveFileAfterClose should be(false)
      saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(txid)))
      heldAfterExtract should be(Set())
      // Deleted from the files once the save file was durable: not recovered here on a restart as well
      heldOnDisk(deviceDir, storeId) should be(Set())

  atest("a transfer out of a closed store with a valid save file ships the file unchanged and drops its CRL entries"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val fileTx = TransactionId(new UUID(9, 42))
    val fileState = StoreRecoveryState(List(recoveryState(storeId, fileTx, Array[Byte](4, 5))))
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = host.control.devices.devices(deviceA).crl
      admission = host.control.devices.devices(deviceA).admission
      _ <- prepareInFlight(host, factory.opened.head, storeId, txid)
      // Closed without extraction: txid stays in the device CRL
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      // The store came back holding a save file (transferred back, its load awaiting CRL room): the file is its whole
      // state, and txid an earlier tenancy's leftover
      _ = CrashRecoveryLog.saveStoreState(storeId, fileState, Backend.crlSaveFile(storeDir))
      retainedBefore = admission.retainedBytes(storeId)
      extracting = host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
      _ <- pumpUntil(host, extracting.isCompleted)
      _ <- extracting
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      heldAfter = held(crl, storeId)
      retainedAfter = admission.retainedBytes(storeId)
      _ = factory.shutdownAll()
    yield
      retainedBefore should be > 0L
      saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(fileTx)))
      heldAfter should be(Set())
      retainedAfter should be(0L)
      heldOnDisk(deviceDir, storeId) should be(Set())

  atest("a transfer out of a closed store whose save file cannot be stat'd fails, writing nothing over it (SL-W20)"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val fileTx = TransactionId(new UUID(9, 44))
    val fileState = StoreRecoveryState(List(recoveryState(storeId, fileTx, Array[Byte](4, 5))))
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = host.control.devices.devices(deviceA).crl
      _ <- prepareInFlight(host, factory.opened.head, storeId, txid)
      // Closed without extraction: txid stays in the device CRL, a leftover beside the save file below
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      // Its whole state is in its save file, reached through a link into a directory without search permission, so
      // the file's stat alone fails (EACCES) while the store directory reads and writes
      locked = hostRoot / "locked"
      _ = os.makeDir(locked)
      _ = CrashRecoveryLog.saveStoreState(storeId, fileState, locked / "state.log")
      _ = os.symlink(Backend.crlSaveFile(storeDir), locked / "state.log")
      _ = os.perms.set(locked, "rw-------")
      extracting = host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
      _ <- pumpUntil(host, extracting.isCompleted)
      outcome = extracting.value
      _ = os.perms.set(locked, "rwx------")
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      heldAfter = held(crl, storeId)
      _ = factory.shutdownAll()
    yield
      outcome.exists(_.isFailure) should be(true)
      saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(fileTx)))
      heldAfter should be(Set(txid))

  atest("a transfer out refuses an open of its closed store between its extraction and its move to outbound, and moves it (CRL-W17)"):
    given ExecutionContext = executionContext
    import StorageDeviceState.{StoreEntry, StoreStatus}
    val factory = newFactory()
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    var hostRef: Option[Host] = None
    // Once the extraction is done, and before startOut's move-to-outbound task runs, the store is offered an open, as
    // retryFailedLoads offers one awaiting a load retry: its TransferOut work refuses it (R12)
    val seams = Host.Seams(closeForTransferOut = Some((sid, sds) =>
      val host = hostRef.get
      host.control.transfersOut.closeForTransferOut(sid, sds).map: _ =>
        host.control.loader.tryLoadStore(sds, CopyFixtures.copyPath(sds.devicePath, sid).get)))
    for
      _ <- net.createDevice(deviceA)
      _ <- net.createDevice(deviceB)
      pool <- client.createNewStoragePool("transfer-out-pool", Replication(1, 1), None, RocksDBConfig(),
                StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      sid = StoreId(pool, 0.toByte)
      _ <- setPoolEntry(sid, deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(sid -> StoreEntry(StoreStatus.Active, None))))
      _ <- waitForTransactionsToComplete()
      storeDir <- writeRocksStore(deviceDir, sid)
      host = newCrlHost(hostRoot, factory, seams = seams)
      _ = hostRef = Some(host)
      _ <- pumpUntil(host, host.control.router.loaded.contains(sid))
      closed = host.closeStore(sid)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(
             sid -> StoreEntry(StoreStatus.TransferringOut, Some(deviceB), Some(UUID.randomUUID())))))
      _ <- waitForTransactionsToComplete()
      _ <- host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, deviceA, sid,
             HLCTimestamp.now, UUID.randomUUID()))
      _ <- pumpUntil(host, CopyFixtures.phaseOf(deviceDir, sid).exists(_.isInstanceOf[Phase.Outbound]))
      marked = CopyFixtures.phaseOf(deviceDir, sid).exists(_.isInstanceOf[Phase.Outbound])
      served = host.control.router.loaded.get(sid)
      opening = host.control.loader.isOpening(sid)
      _ <- served.map(_.backend.close()).getOrElse(Future.unit)
      _ = factory.shutdownAll()
    yield
      served should be(None)
      opening should be(false)
      marked should be(true)

  /** A closed store's transfer-out extraction with a second copy of it on another device of the host (an earlier
   *  tenancy's, kept offline in lost/). The source, deviceA, is the directory `sourceName`, the
   *  other copy's device (deviceB) the directory `otherName`, so the two calls put each first in the host's devices.
   *  Returns the source's save file state, what the source's device CRL holds afterwards, and whether the other copy
   *  got a save file */
  private def extractionBesideOtherCopy(sourceName: String, otherName: String)
    : Future[(Either[CrashRecoveryLog.InvalidSaveFile, StoreRecoveryState], Set[TransactionId], Boolean)] =
    given ExecutionContext = executionContext
    val factory = newFactory()
    val hostRoot = newHostDir()
    val sourceDir = writeSmallCrlDevice(hostRoot, sourceName, deviceA)
    val otherDir = writeSmallCrlDevice(hostRoot, otherName, deviceB)
    createCrl(sourceDir)
    createCrl(otherDir)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(
             stores = Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- waitForTransactionsToComplete()
      storeDir <- writeRocksStore(sourceDir, storeId)
      otherCopy <- writeRocksStore(otherDir, storeId)
      _ = CopyFixtures.markCopy(otherDir, storeId, Phase.Lost)
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      source = host.control.devices.devices(deviceA)
      crl = factory.opened.find(_.devicePath == sourceDir).get
      _ <- prepareInFlight(host, crl, storeId, txid)
      // Closed without extraction: txid stays in the source's device CRL
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      extracting = host.control.transfersOut.closeForTransferOut(storeId, source)
      _ <- pumpUntil(host, extracting.isCompleted)
      _ <- extracting
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      heldAfter = held(crl, storeId)
      otherSaveFile = os.exists(Backend.crlSaveFile(otherCopy))
      _ = factory.shutdownAll()
    yield (saved, heldAfter, otherSaveFile)

  atest("a closed store's transfer-out extraction runs on its source device, not another copy's (CRL-W16)"):
    given ExecutionContext = executionContext
    for
      sourceFirst <- extractionBesideOtherCopy("dev0", "dev1")
      otherFirst <- extractionBesideOtherCopy("dev1", "dev0")
    yield
      for (saved, heldAfter, otherSaveFile) <- List(sourceFirst, otherFirst) do
        saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(txid)))
        heldAfter should be(Set())
        otherSaveFile should be(false)
      succeed

  /** Extracts the store's CRL state for a network transfer out, from the open store (`loaded`) or from its device
   *  CRL once closed, holding an in-flight transaction and an applied stub. Returns the save file's state, the stubs
   *  the device CRL held afterwards, and those its files hold */
  private def extractionWithStub(loaded: Boolean)
    : Future[(Either[CrashRecoveryLog.InvalidSaveFile, StoreRecoveryState], Set[TransactionId], Set[TransactionId])] =
    given ExecutionContext = executionContext
    val factory = newFactory()
    val stubbed = TransactionId(new UUID(9, 41))
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      _ <- prepareInFlight(host, crl, storeId, txid)
      stubDurable = Promise[Unit]()
      _ = crl.saveAppliedStub(storeId, stubbed, HLCTimestamp.now, () => stubDurable.trySuccess(()))
      _ <- stubDurable.future
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
      _ <-
        if loaded then
          Future.unit
        else
          val closed = host.closeStore(storeId)
          pumpUntil(host, closed.isCompleted).flatMap(_ => closed)
      extracting = host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
      _ <- pumpUntil(host, extracting.isCompleted)
      _ <- extracting
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      heldAfter = crl.getAppliedStubs(storeId).keySet
      _ = factory.shutdownAll()
      onDisk = heldStubsOnDisk(deviceDir, storeId)
    yield (saved, heldAfter, onDisk)

  private def heldStubsOnDisk(deviceDir: os.Path, sid: StoreId): Set[TransactionId] =
    val crl = newFactory().openCRL(deviceDir, StorageDeviceConfig.loadStorageDeviceConfig(
      deviceDir / StorageDeviceConfig.configFilename), holdsStores = false)
    try crl.getAppliedStubs(sid).keySet
    finally crl.shutdown()

  atest("a network transfer out of an open store ships its applied stubs in its crl_save.log"):
    given ExecutionContext = executionContext
    extractionWithStub(loaded = true).map: (saved, heldAfter, onDisk) =>
      saved.map(_.appliedStubs.keySet) should be(Right(Set(TransactionId(new UUID(9, 41)))))
      saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(txid)))
      heldAfter should be(Set())
      onDisk should be(Set())

  atest("a network transfer out of an unloaded store ships its applied stubs in its crl_save.log"):
    given ExecutionContext = executionContext
    extractionWithStub(loaded = false).map: (saved, heldAfter, onDisk) =>
      saved.map(_.appliedStubs.keySet) should be(Right(Set(TransactionId(new UUID(9, 41)))))
      saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(txid)))
      heldAfter should be(Set())
      onDisk should be(Set())

  atest("a discarded store's directory stays in lost until its device-CRL entries are durably dropped"):
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
      _ = CopyFixtures.markCopy(deviceDir, storeId, Phase.Lost)
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      crl = second.opened.head
      _ = crl.dropGate = Some(gate.future)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, crl.dropCalled.isCompleted)
      // More checks while the drop is held start nothing and discard nothing
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, false, rounds = 20)
      dropRequested = crl.dropCalled.isCompleted
      keptWhileDropping = markedLost(deviceDir)
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

  atest("maxConcurrentRebuilds bounds the discards of lost stores, not only their rebuilds"):
    given ExecutionContext = executionContext
    val second = StoreId(poolId, 1)
    val first = newFactory()
    val rebuilds = new RecordingRebuildFactory
    val gate = Promise[Unit]()
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    createCrl(deviceDir)
    val rebuilding = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None)
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map(storeId -> rebuilding, second -> rebuilding)))
      _ <- waitForTransactionsToComplete()
      firstDir <- writeRocksStore(deviceDir, storeId)
      secondDir <- writeRocksStore(deviceDir, second)
      _ = CopyFixtures.markCopy(deviceDir, storeId, Phase.Lost)
      _ = CopyFixtures.markCopy(deviceDir, second, Phase.Lost)
      host = newCrlHost(hostRoot, first, rebuilds = rebuilds, maxConcurrentRebuilds = 1)
      crl = first.opened.head
      _ = crl.dropGate = Some(gate.future)
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, crl.dropCalled.isCompleted)
      // The single slot is held by the first store's discard: further checks discard nothing more
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, false, rounds = 20)
      dropsWhileHeld = crl.dropCalls.get
      secondKeptWhileHeld = markedLost(deviceDir, second)
      _ = gate.success(())
      // The slot passes from the first store's discard to its rebuild, with no gap a second discard could take
      _ <- pumpWithTx(host, rebuilds.created.nonEmpty)
      _ = host.control.reconciler.checkAll()
      _ <- pumpWithTx(host, false, rounds = 20)
    yield
      dropsWhileHeld should be(1)
      secondKeptWhileHeld should be(true)
      rebuilds.created.map(_.storeId).toList should be(List(storeId))
      CopyFixtures.copyPath(deviceDir, storeId) should be(None)
      crl.dropCalls.get should be(1)
      markedLost(deviceDir, second) should be(true)

  atest("a failed backend whose move to lost/ fails keeps its CRL entries on disk and is held offline"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val opener = new FailingRocksOpener(executionContext)
    // The rename into lost/ fails and moves nothing, as a failed rename does
    @volatile var lostRenames = 0
    val failingRename: (os.Path, os.Path) => Unit = (from, to) =>
      if (to / os.up).last == CopyLayout.LostDirectory then
        lostRenames += 1
        throw new java.io.IOException("Input/output error")
      DurableFiles.atomicRename(from, to)
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory,
               seams = Host.Seams(backends = Some(opener), copyRename = Some(failingRename)))
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, factory.opened.head, storeId, txid)
      _ = opener.onFatals.head(new java.io.IOException("Input/output error"))
      _ <- pumpUntil(host, !host.control.router.loaded.contains(storeId))
      unloaded = !host.control.router.loaded.contains(storeId)
      // Lets the close run on the store's executor, and its end's move to lost/ fail, ahead of the shutdown below
      _ <- pumpUntil(host, lostRenames > 0)
      _ <- pumpUntil(host, false, rounds = 50)
      held = host.control.moves.isPendingLost(host.control.devices.devices(deviceA), storeId)
      _ = factory.shutdownAll()
    yield
      unloaded should be(true)
      lostRenames should be >= 1
      markedLost(deviceDir) should be(false)
      CopyFixtures.copyPath(deviceDir, storeId) should be(Some(storeDir))
      // Held offline, never loaded, shipped or retired, until a check of its device moves it
      held should be(true)
      // Left in the device CRL: a restart, which may serve the copy where it is, recovers them
      heldOnDisk(deviceDir, storeId) should be(Set(txid))

  atest("a store that fail-stops is unloaded and moved to lost/"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ = host.control.router.loaded(storeId).frontend.failStop("test")
      _ <- pumpUntil(host, !host.control.router.loaded.contains(storeId))
      // Moved once its close has ended
      _ <- pumpUntil(host, markedLost(deviceDir))
      marked = markedLost(deviceDir)
      _ <- pumpUntil(host, false, rounds = 50)
      _ = factory.shutdownAll()
    yield
      marked should be(true)

  atest("the fail-stop of a store no longer served is ignored"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      store = host.control.router.loaded(storeId)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      _ = store.frontend.failStop("test")
      _ <- pumpUntil(host, false, rounds = 50)
      _ = factory.shutdownAll()
    yield
      markedLost(deviceDir) should be(false)

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
        List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), true)
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
      // 4 CRL files of 1 MB: (4 - 2) halves of one, less a sixteenth and 3 evacuation entries' fixed bytes
      limitA should be(15L * 1024 * 1024 / 16 - 3 * 118)
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
        List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), true)
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
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
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
      extracting = host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
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
      // A transfer out's extraction takes the entries out of the device CRL, and their count with them. Under its
      // TransferOut work, as startOut holds it, so no check loads the extracted copy again
      _ = transferOutWork(host, storeId, held = true)
      extracted = host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
      _ <- pumpUntil(host, extracted.isCompleted)
      _ <- extracted
      afterExtraction = admission.admittedBytes
      _ = factory.shutdownAll()
    yield
      counted should be > 0L
      afterClose should be(counted)
      afterReload should be(counted)
      afterExtraction should be(0L)

  /** The store's TransferOut work on deviceA, as startOut holds it from its close until its move-to-outbound task (R12) */
  private def transferOutWork(host: Host, storeId: StoreId, held: Boolean): Unit =
    host.control.copyRegistry.work("test", deviceA, storeId, _.isInstanceOf[Work.TransferOut],
      if held then Some(Work.TransferOut(None)) else None)

  atest("a load offered during an extraction's read is refused, and the extraction takes the store's transactions"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      _ <- prepareInFlight(host, crl, storeId, txid)
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      gate = Promise[Unit]()
      _ = crl.readGate = Some(gate.future)
      extracting = host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
      _ <- pumpUntil(host, crl.readCalled.isCompleted)
      // A load offered while the extraction's read is outstanding: refused, it opens nothing
      _ = host.loadStoreById(deviceA, storeId)
      _ = host.testingOnlyHandleEvents()
      opening = host.control.loader.isOpening(storeId)
      _ = gate.success(())
      _ <- pumpUntil(host, extracting.isCompleted)
      outcome = extracting.value
      _ = transferOutWork(host, storeId, held = false)
      heldAfter = held(crl, storeId)
      served = host.control.router.loaded.contains(storeId)
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      _ = factory.shutdownAll()
    yield
      opening should be(false)
      outcome should be(Some(scala.util.Success(())))
      served should be(false)
      heldAfter should be(Set())
      saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(txid)))

  atest("a load offered during an extraction from a save file is refused, and the file ships unchanged"):
    given ExecutionContext = executionContext
    val factory = newFactory()
    val fileTx = TransactionId(new UUID(9, 43))
    val fileState = StoreRecoveryState(List(recoveryState(storeId, fileTx, Array[Byte](6))))
    for
      (hostRoot, _, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, factory)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      crl = factory.opened.head
      // Under its TransferOut work from before its close, as startOut holds it, so no check loads the closed copy again
      _ = transferOutWork(host, storeId, held = true)
      closed = host.closeStore(storeId)
      _ <- pumpUntil(host, closed.isCompleted)
      _ <- closed
      _ = CrashRecoveryLog.saveStoreState(storeId, fileState, Backend.crlSaveFile(storeDir))
      gate = Promise[Unit]()
      _ = crl.dropGate = Some(gate.future)
      extracting = host.control.transfersOut.closeForTransferOut(storeId, host.control.devices.devices(deviceA))
      _ <- pumpUntil(host, crl.dropCalls.get == 1)
      // A load offered while the extraction's drop is outstanding: refused, it opens nothing and drops nothing
      _ = host.loadStoreById(deviceA, storeId)
      _ = host.testingOnlyHandleEvents()
      opening = host.control.loader.isOpening(storeId)
      _ = crl.dropGate = None
      _ = gate.success(())
      _ <- pumpUntil(host, extracting.isCompleted)
      outcome = extracting.value
      _ = transferOutWork(host, storeId, held = false)
      heldAfter = held(crl, storeId)
      served = host.control.router.loaded.contains(storeId)
      saved = CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(storeDir), storeId)
      _ = factory.shutdownAll()
    yield
      opening should be(false)
      outcome should be(Some(scala.util.Success(())))
      served should be(false)
      crl.dropCalls.get should be(1)
      heldAfter should be(Set())
      saved.map(_.transactions.map(_.txd.transactionId)) should be(Right(List(fileTx)))

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

  atest("an Active store whose directory is missing is silenced and rebuilt, its device-CRL entries kept (X-W11, RB-W16)"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    val rebuilds = new RecordingRebuildFactory
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      // The directory goes missing (a bad restore, a mount finishing late) while the entry still reads Active
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      aside = deviceDir / os.up / "aside"
      _ = os.move(storeDir, aside)
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      crl = second.opened.head
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, rebuilds.created.nonEmpty)
      // Later reconciles, the entry now Rebuilding, drop nothing either
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty)
      state <- client.getStorageDeviceState(deviceA)
      leftInCrl = held(crl, storeId)
      silenced = restarted.control.router.isOffline(storeId)
      _ = second.shutdownAll()
    yield
      rebuilds.created.map(_.storeId).toList should be(List(storeId))
      state.stores.get(storeId).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      silenced should be(true)
      // Kept: had the directory come back before the flip, a copy loaded without them would answer for a transaction
      // whose state and locks it lost. The rebuilt copy's empty crl_save.log drops them at its first load
      leftInCrl should be(Set(txid))
      crl.dropDone.isCompleted should be(false)

  atest("a missing store whose directory returns while its rebuild request fails is rebuilt by a later reconcile (RB-W18)"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    val rebuilds = new RecordingRebuildFactory
    val failRequests = new java.util.concurrent.atomic.AtomicBoolean(true)
    val requestsFailed = new java.util.concurrent.atomic.AtomicInteger(0)
    val seams = Host.Seams(rebuildRequestFailure = _ =>
      if failRequests.get then
        requestsFailed.incrementAndGet()
        Some(new RuntimeException("injected rebuild request failure"))
      else None)
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      aside = deviceDir / os.up / "aside"
      _ = os.move(storeDir, aside)
      restarted = newCrlHost(hostRoot, second, seams = seams, rebuilds = rebuilds)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, requestsFailed.get > 0 && restarted.control.reconciler.activeDeviceChecks.isEmpty)
      // The directory comes back (a restore finishing late) after the request failed
      _ = os.move(aside, storeDir)
      _ = restarted.testingOnlyRescan(deviceA)
      _ = failRequests.set(false)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, rebuilds.created.nonEmpty)
      state <- client.getStorageDeviceState(deviceA)
      silenced = restarted.control.router.isOffline(storeId)
      _ = second.shutdownAll()
    yield
      requestsFailed.get should be >= 1
      state.stores.get(storeId).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      rebuilds.created.map(_.storeId).toList should be(List(storeId))
      silenced should be(true)

  atest("an Active store whose config cannot be stat'd is not taken for missing: no rebuild, its entries kept (X-W18)"):
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
      // No search permission: the config's stat fails (EACCES) while the directory and its files are there
      _ = os.perms.set(storeDir, "rw-------")
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      crl = second.opened.head
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, restarted.control.reconciler.activeDeviceChecks.isEmpty)
      state <- client.getStorageDeviceState(deviceA)
      leftInCrl = held(crl, storeId)
      awaiting = restarted.control.loader.awaitingRetryHasCallback(storeId)
      _ = os.perms.set(storeDir, "rwx------")
      _ = second.shutdownAll()
    yield
      rebuilds.created should be(empty)
      state.stores.get(storeId).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Active))
      leftInCrl should be(Set(txid))
      // Its load waits for a stat that answers
      awaiting should be(defined)

  atest("an Active store whose directory lost its config is rebuilt, its copy retired with its CRL state (X-W11)"):
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
      _ = os.remove(storeDir / StoreConfig.configFilename)
      restarted = newCrlHost(hostRoot, second, rebuilds = rebuilds)
      crl = second.opened.head
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpWithTx(restarted, rebuilds.created.nonEmpty)
      state <- client.getStorageDeviceState(deviceA)
      leftInCrl = held(crl, storeId)
      pendingDir = deviceDir / RebuildingStore.PendingDeleteDirectory
      retired = if os.exists(pendingDir) then os.list(pendingDir).toList else Nil
      _ = second.shutdownAll()
    yield
      rebuilds.created.map(_.storeId).toList should be(List(storeId))
      state.stores.get(storeId).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      os.exists(storeDir) should be(false)
      retired should have size 1
      os.exists(retired.head / RebuildingStore.LostCrlStateFile) should be(true)
      leftInCrl should be(Set())

  atest("a leftover drop releases only the charge it was issued for, not that of a store landing meanwhile"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    val landedTx = TransactionId(new UUID(9, 6))
    val landedState = StoreRecoveryState(List(recoveryState(storeId, landedTx, Array[Byte](1, 2, 3))))
    val landedBytes = CrlAdmission.recoveredBytes(landedState)
    val dropGate = Promise[Unit]()
    val loadGate = Promise[Unit]()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      // A store whose directory went by hand, unlisted: its entries are left over, charged at the device's open
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      _ = os.remove.all(storeDir)
      _ <- setDeviceState(deviceA)(_.copy(stores = Map()))
      _ <- waitForTransactionsToComplete()
      restarted = newCrlHost(hostRoot, second)
      crl = second.opened.head
      sds = restarted.control.devices.devices(deviceA)
      leftoverBytes = sds.admission.retainedBytes(storeId)
      _ = crl.dropGate = Some(dropGate.future)
      _ = restarted.control.reconciler.checkAll()
      _ <- pumpUntil(restarted, crl.dropCalls.get == 1)
      // The store lands while the leftover drop runs: its crl_save.log state is charged, its own drop and its load
      // queue behind the leftover drop. Its device lists it again first, so no device check retires the copy as one
      // put back by hand (StoreRebuilds.retireUnlistedServed) before the test reads its charge
      _ <- setDeviceState(deviceA)(_.copy(
             stores = Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))
      _ <- writeRocksStore(deviceDir, storeId, Some(storeDir))
      _ = CrashRecoveryLog.saveStoreState(storeId, landedState, Backend.crlSaveFile(storeDir))
      _ = restarted.testingOnlyRescan(deviceA)
      _ = crl.loadGate = Some(loadGate.future)
      _ = restarted.control.loader.tryLoadStore(sds, storeDir)
      _ <- pumpUntil(restarted, crl.dropCalls.get == 2)
      retainedWhileLanding = sds.admission.retainedBytes(storeId)
      _ = dropGate.success(())
      _ <- pumpUntil(restarted, restarted.control.reconciler.leftoverCrlDropsRunning.isEmpty)
      // The landed records are saved into the CRL next, and must stay counted until the store's recovery counts them.
      // The load changed the charge, so the drop leaves all of it to that recovery: counted over, never under
      admittedAfterDrop = sds.admission.admittedBytes
      _ = loadGate.success(())
      _ <- pumpUntil(restarted, restarted.control.router.loaded.get(storeId).exists(
             _.frontend.transactions.contains(landedTx)) && sds.admission.retainedBytes(storeId) == 0)
      admittedAfterRecovery = sds.admission.admittedBytes
      leftInCrl = held(crl, storeId)
      _ = second.shutdownAll()
    yield
      leftoverBytes should be > 0L
      retainedWhileLanding should be(leftoverBytes + landedBytes)
      admittedAfterDrop should be(leftoverBytes + landedBytes)
      admittedAfterRecovery should be(landedBytes)
      leftInCrl should be(Set(landedTx))

  atest("a store loaded from its crl_save.log does not recover entries an earlier tenancy left in the device CRL"):
    given ExecutionContext = executionContext
    val first = newFactory()
    val second = newFactory()
    for
      (hostRoot, deviceDir, storeDir) <- activeStoreDevice()
      host = newCrlHost(hostRoot, first)
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      _ <- prepareInFlight(host, first.opened.head, storeId, txid)
      _ <- host.control.router.loaded(storeId).backend.close()
      _ = first.shutdownAll()
      // The copy that held txid left its path without a retire (removed by hand), and the store came back before any
      // reconcile saw it unlisted: a copy whose save file holds its whole current state, in which txid is long gone
      _ = os.remove.all(storeDir)
      _ <- writeRocksStore(deviceDir, storeId, Some(storeDir))
      _ = CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState(), Backend.crlSaveFile(storeDir))
      heldBefore = heldOnDisk(deviceDir, storeId)
      restarted = newCrlHost(hostRoot, second)
      // The save file goes once its state is in the CRL, just before the store recovers from it
      _ <- pumpUntil(restarted, restarted.control.router.loaded.contains(storeId) &&
                                !os.exists(Backend.crlSaveFile(storeDir)))
      _ <- pumpUntil(restarted, false, rounds = 50)
      crl = second.opened.head
      leftInCrl = held(crl, storeId)
      recoveredTx = restarted.control.router.loaded(storeId).frontend.transactions.contains(txid)
      _ = second.shutdownAll()
    yield
      heldBefore should be(Set(txid))
      recoveredTx should be(false)
      leftInCrl should be(Set())
      heldOnDisk(deviceDir, storeId) should be(Set())

  /** Opens a device whose CRL was created with 4 streams of 1 MB, holding a store, with `config` instead. A change of
   *  config (or of the code defaults it once took) must neither lose the CRL nor resize it */
  private def resizedCrlDevice(config: StorageDeviceConfig => StorageDeviceConfig): Future[org.scalatest.Assertion] =
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)
    def files = os.list(crlDir).filter(_.ext == "crl").map(f => f.last -> os.size(f)).toSet
    val oldFiles = files
    os.write.over(deviceDir / StorageDeviceConfig.configFilename,
      config(StorageDeviceConfig(deviceA, systemId, crlFileSizeMb = Some(1))).yamlConfig)
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)

    Future.successful:
      markedLost(deviceDir) should be(false)
      os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened.head
      factory.opened.head.admissionLimitBytes should be(15L * 1024 * 1024 / 16 - 3 * 118)
      files should be(oldFiles)

  atest("a device CRL opened with fewer configured streams than the minimum keeps its own sizing"):
    resizedCrlDevice(_.copy(crlNumStreams = Some(3)))

  atest("a device CRL opened with an invalid maximum stream count keeps its own sizing"):
    resizedCrlDevice(_.copy(crlMaxStreams = Some(2), crlMinFreePercent = Some(101)))

  atest("a device CRL opened with more configured streams than it was created with keeps its own sizing"):
    resizedCrlDevice(_.copy(crlNumStreams = Some(5)))

  atest("a device CRL opened with another configured file size than it was created with keeps its own sizing"):
    resizedCrlDevice(_.copy(crlFileSizeMb = Some(2)))

  /** A device holding a store whose CRL `damage` leaves disagreeing with the sizing it recorded: refused on every
   *  scan, never declared lost, and loaded once `repair` restores it */
  private def sizingMismatchDevice(damage: os.Path => Unit, repair: os.Path => Unit): Future[org.scalatest.Assertion] =
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    val storeDir = writeStoreDir(deviceDir)
    createCrl(deviceDir)
    val crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)
    damage(crlDir)
    def contents = os.list(crlDir).map(f => f.last -> os.read.bytes(f).toSeq).toMap
    val damaged = contents
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)

    host.control.devices.devices.contains(deviceA) should be(false)
    for _ <- 1 to DeviceDiscovery.CrlOpenAttempts do
      host.control.discovery.checkForNewDevices(scan = true) should be(Nil)
    markedLost(deviceDir) should be(false)
    os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
    factory.opened should be(Nil)
    contents should be(damaged)

    repair(crlDir)
    host.control.discovery.checkForNewDevices() should be(List(deviceA))
    Future.successful:
      markedLost(deviceDir) should be(false)
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened.head

  atest("a device CRL whose stream file disagrees with its recorded sizing is refused, never lost"):
    var saved = Array[Byte]()
    sizingMismatchDevice(
      crlDir =>
        saved = os.read.bytes(crlDir / "2.crl")
        os.write.over(crlDir / "2.crl", "truncated"),
      crlDir => os.write.over(crlDir / "2.crl", saved))

  atest("a device CRL with stream files but no recorded sizing is refused, never lost"):
    var saved = ""
    sizingMismatchDevice(
      crlDir =>
        saved = os.read(crlDir / SimpleCRL.SizingFile)
        os.remove(crlDir / SimpleCRL.SizingFile),
      crlDir => os.write(crlDir / SimpleCRL.SizingFile, saved))

  /** Removes the CRL's sizing file and gives the device `invalid` as its config: the open must be refused for the
   *  missing sizing file (CrlSizingMismatch), not the config, so the CRL is never declared lost. Restoring the
   *  sizing file opens it as recorded, the config notwithstanding */
  private def unsizedCrlWithInvalidConfig(invalid: StorageDeviceConfig): Future[org.scalatest.Assertion] =
    var saved = ""
    sizingMismatchDevice(
      crlDir =>
        saved = os.read(crlDir / SimpleCRL.SizingFile)
        os.remove(crlDir / SimpleCRL.SizingFile)
        os.write.over(crlDir / os.up / StorageDeviceConfig.configFilename, invalid.yamlConfig),
      crlDir => os.write(crlDir / SimpleCRL.SizingFile, saved))

  atest("a device CRL with stream files, no recorded sizing and too few configured streams is refused, never lost"):
    unsizedCrlWithInvalidConfig(StorageDeviceConfig(deviceA, systemId, crlFileSizeMb = Some(1),
      crlNumStreams = Some(3)))

  atest("a device CRL with stream files, no recorded sizing and a maximum below its streams is refused, never lost"):
    unsizedCrlWithInvalidConfig(StorageDeviceConfig(deviceA, systemId, crlFileSizeMb = Some(1),
      crlMaxStreams = Some(2)))

  atest("a new device whose config cannot size a CRL is refused, creating nothing in its CRL directory"):
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    os.write.over(deviceDir / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(deviceA, systemId, crlFileSizeMb = Some(1), crlNumStreams = Some(3)).yamlConfig)
    val crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)
    val factory = newFactory()
    val host = newCrlHost(hostRoot, factory)

    host.control.devices.devices.contains(deviceA) should be(false)
    for _ <- 1 to DeviceDiscovery.CrlOpenAttempts do
      host.control.discovery.checkForNewDevices(scan = true) should be(Nil)
    os.list(deviceDir).exists(_.last.startsWith("crl.lost-")) should be(false)
    os.list(crlDir) should be(Nil)
    factory.opened should be(Nil)

    os.write.over(deviceDir / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(deviceA, systemId, crlFileSizeMb = Some(1)).yamlConfig)
    host.control.discovery.checkForNewDevices() should be(List(deviceA))
    Future.successful:
      host.control.devices.devices(deviceA).crl should be theSameInstanceAs factory.opened.head
      os.exists(crlDir / SimpleCRL.SizingFile) should be(true)

  atest("A device whose CRL lane fills grows its CRL and admits more"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeSmallCrlDevice(hostRoot, "dev0", deviceA)
    // Streams of 1 MiB, a ring of 4 that may grow to 5, whatever the temp file system's free space
    os.write.over(deviceDir / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(deviceA, systemId, crlFileSizeMb = Some(1), crlMaxStreams = Some(5),
        crlMinFreePercent = Some(0)).yamlConfig)
    createCrl(deviceDir)
    val storeDir = writeStoreDir(deviceDir)
    // One object per transaction, so none collides with another
    val oids = (0 until 6).map(i => ObjectId(new UUID(77, i)))
    val data = new Array[Byte](150000)
    def prepare(host: Host, i: Int): Unit =
      host.receiveTransactionMessage(TxPrepare(storeId, net.storeId0,
        overwriteTxd(TransactionId(new UUID(78, i)), 1, objectId = oids(i)), ProposalId.initialProposal(1),
        List(ObjectUpdate(oids(i), DataBuffer(data))), true))
    val initial = new RocksDBBackend(storeDir, storeId, ExecutionContext.global)
    oids.foreach: o =>
      initial.bootstrapAllocate(o, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
        DataBuffer(Array[Byte](0)))
    for
      _ <- initial.close()
      host = newCrlHost(hostRoot, newFactory())
      _ <- pumpUntil(host, host.control.router.loaded.contains(storeId))
      admission = host.control.devices.devices(deviceA).admission
      ceiling = admission.ceiling(AdmissionTier.Data)
      before = admission.limitBytes
      // Five transactions take the data lane past its ceiling
      _ = (0 until 5).foreach(prepare(host, _))
      _ <- pumpUntil(host, admission.full(AdmissionTier.Data))
      _ = admission.full(AdmissionTier.Data) should be(true)
      admittedFull = admission.admittedBytes
      // The sixth is refused, which asks the CRL to grow
      _ = prepare(host, 5)
      _ <- pumpUntil(host, admission.limitBytes > before)
      crlDir = CrashRecoveryLog.deviceCrlDir(deviceDir)
      // The retransmission meets the raised limit
      _ = prepare(host, 5)
      _ <- pumpUntil(host, admission.admittedBytes > admittedFull)
      _ <- host.control.router.loaded(storeId).backend.close()
    yield
      admission.admittedBytes - admittedFull should be > 150000L
      // (n - 2) * S / 2 - S / 16 - 118 * (n - 1) for n = 5 streams of 1 MiB
      admission.limitBytes should be(3L * (1L << 20) / 2 - (1L << 20) / 16 - 4 * 118)
      admission.limitBytes should be > before
      admission.ceiling(AdmissionTier.Data) should be > ceiling
      os.exists(crlDir / "4.crl") should be(true)
      os.exists(crlDir / "5.crl") should be(false)
