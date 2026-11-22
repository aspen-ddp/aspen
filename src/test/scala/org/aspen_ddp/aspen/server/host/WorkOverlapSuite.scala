package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.{AspenClient, TransactionOutcomeUnknown}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.server.{FileStat, Host, StorageDeviceConfig}
import org.aspen_ddp.aspen.server.crl.StoreRecoveryState
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import org.aspen_ddp.aspen.server.host.DeviceMetadata.RebuildOutcome
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.{BackendOpener, DefaultBackendOpener, StoreCreation}

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Success, Try}

/** A rebuild that does nothing until the test resolves it, and ignores its cancel: a rebuild winding down, or one past
 *  finish's latch re-check */
private class HeldRebuild(val storeId: StoreId, val episode: Option[UUID]) extends StoreRebuild:
  val promise: Promise[Unit] = Promise()
  def complete: Future[Unit] = promise.future
  @volatile var cancelled: Boolean = false
  def cancel(): Unit = cancelled = true

private class HeldRebuildFactory extends StoreRebuildFactory:
  val created: mutable.ListBuffer[HeldRebuild] = mutable.ListBuffer()

  def createRebuild(client: AspenClient, storeId: StoreId, storageDeviceId: StorageDeviceId, devicePath: os.Path,
                    backends: BackendOpener, episode: Option[UUID]): StoreRebuild =
    val r = new HeldRebuild(storeId, episode)
    synchronized(created += r)
    r

/** A device CRL whose first dropStore of each store waits for the test (a retire held up by a slow CRL); later ones
 *  complete at once */
private class HeldDropCrl extends TestNetwork.TestCRL:
  val drops: mutable.Map[StoreId, Promise[Unit]] = mutable.Map()

  override def dropStore(storeId: StoreId): Future[Unit] = synchronized:
    drops.get(storeId) match
      case Some(p) if p.isCompleted => Future.unit
      case _ => drops.getOrElseUpdate(storeId, Promise[Unit]()).future

/** A device CRL whose first readStoreState of each store waits for the test (an extraction held up by a slow CRL);
 *  later ones answer at once. Counts every read */
private class HeldReadCrl extends TestNetwork.TestCRL:
  val reads: mutable.Map[StoreId, Promise[Unit]] = mutable.Map()
  val readCount: mutable.Map[StoreId, Int] = mutable.Map()

  override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] = synchronized:
    readCount(storeId) = readCount.getOrElse(storeId, 0) + 1
    val first = !reads.contains(storeId)
    val p = reads.getOrElseUpdate(storeId, Promise[Unit]())
    val answer = super.readStoreState(storeId)
    if first then p.future.flatMap(_ => answer)(using ExecutionContext.parasitic) else answer

/** I3: at most one piece of host work (Create, Rebuild, TransferIn, TransferOut) per (device, store) at a time. Each
 *  test is a counterexample of the Stage 1 audit (store-copy state machine design, "COUNTEREXAMPLES TO I3") and checks
 *  that the second piece of work waits for the first to end. */
class WorkOverlapSuite extends IntegrationTestSuite with ControlFixture:

  private val moving = StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), 3.toByte)
  private val source = StorageDeviceId.BootstrapStorageDeviceId

  private def rebuildingIn(episode: UUID): StorageDeviceState.StoreEntry =
    StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode))

  // Transferred back onto deviceA from the bootstrap device, which the tree records Operational
  private val transferringIn: StorageDeviceState.StoreEntry =
    StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.TransferringIn, Some(source), Some(UUID.randomUUID()))

  private def check(host: Host, deviceId: StorageDeviceId, stores: Map[StoreId, StorageDeviceState.StoreEntry])
      : Future[Unit] =
    val lookup = recorded(host).armLookup(deviceId)
    host.control.reconciler.checkStorageDevice(deviceId)
    lookup.success(deviceState(deviceId, stores))
    yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceId))

  /** Pumps the test's queue and the network's transactions until `condition` holds, and with `drain` the host's
   *  queued events too (a LoadStore). Silent on exhaustion */
  private def pumpUntil(condition: => Boolean, rounds: Int = 20, drain: Option[Host] = None): Future[Unit] =
    given ExecutionContext = executionContext
    if condition || rounds == 0 then
      Future.unit
    else
      drain.foreach(_.testingOnlyHandleEvents())
      for
        _ <- yieldUntil(condition, 20)
        _ <- waitForTransactionsToComplete()
        _ <- pumpUntil(condition, rounds - 1, drain)
      yield ()

  /** As pumpUntil, bounded by time rather than rounds, for waits on the client's real-time retry backoff: pumps until
   *  `condition` or 10 s, sleeping a millisecond a round past the first 50 so a long wait does not spin a core.
   *  Silent on the deadline; the caller asserts */
  private def pumpFor(condition: => Boolean, drain: Option[Host] = None): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + 10_000_000_000L
    def loop(round: Int): Future[Unit] =
      drain.foreach(_.testingOnlyHandleEvents())
      if condition || System.nanoTime() - deadline > 0 then
        Future.unit
      else
        if round >= 50 then
          Thread.sleep(1)
        for
          _ <- yieldUntil(condition, 20)
          _ <- waitForTransactionsToComplete()
          _ <- loop(round + 1)
        yield ()
    loop(0)

  /** C1: deviceA's entry was Rebuilding (e1) with its rebuild running here. The store was re-placed away, the nudge to
   *  this host was lost, and the store was transferred back: deviceA's next check reads TransferringIn. The rebuild is
   *  cancelled by that check, but it is still registered (winding down, or past finish's latch), so the receive waits
   *  for it to end. */
  for (variant, end) <- List[(String, HeldRebuild => Unit)](
        "running" -> (r => r.promise.failure(RebuildingStore.RebuildCancelled(r.storeId))),
        "cancelled past finish's latch" -> (r => r.promise.success(())))
  do
    atest(s"a transfer in waits while a rebuild of the store is registered on the device ($variant, C1)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      writeDevice(hostRoot, "dev0", deviceA)
      val factory = new HeldRebuildFactory
      val transfers = new StagedTransferService
      val host = newHost(hostRoot, transferService = transfers, storeRebuildFactory = factory)

      for
        // In the tree with no entry for the store, so a flip past the latch finds it gone (RebuildOutcome.Discard)
        _ <- net.createDevice(deviceA)
        _ <- waitForTransactionsToComplete()
        _ <- check(host, deviceA, Map(moving -> rebuildingIn(UUID.randomUUID())))
        _ <- yieldUntil(factory.created.nonEmpty)
        _ <- check(host, deviceA, Map(moving -> transferringIn))
        _ <- pumpUntil(recorded(host).transferSourceReads.nonEmpty)
        _ <- yieldUntil(!host.control.reconciler.activeDeviceChecks.contains(deviceA))
        cancelled = factory.created.head.cancelled
        receivedWhileRegistered = transfers.transfersIn.map(_._1)
        // The rebuild's end checks the device again, which starts the receive
        _ = recorded(host).armLookup(deviceA).success(deviceState(deviceA, Map(moving -> transferringIn)))
        _ = end(factory.created.head)
        _ <- pumpUntil(transfers.transfersIn.nonEmpty)
      yield
        cancelled should be(true)
        receivedWhileRegistered should be(Vector())
        host.control.rebuilds.rebuildingStoreIds should not contain moving
        transfers.transfersIn.map(_._1) should be(Vector(moving))
        factory.created.size should be(1)

  /** C2: a lost copy's discard (the retire) is held up by a slow CRL drop while the store is re-placed away and back
   *  (Rebuilding under episode e2). The discard finishing must not start the rebuild it began for (e1): the rebuild
   *  that starts is e2's, from the check the discard's end asks for. */
  atest("a discard finishing after the entry moved on starts no rebuild (C2)"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = hostRoot / os.up / "injected-device"
    os.makeDir.all(deviceDir)
    val storePath = writeStoreConfig(deviceDir, moving)
    os.write.over(storePath / RebuildingStore.RebuildRequiredMarkerFile, "")
    val crl = new HeldDropCrl
    val factory = new HeldRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory)
    val local = new LocalStorageDeviceState(deviceA, deviceDir, deviceDir / StorageDeviceConfig.configFilename, crl)
    injectLoadedDevice(host, local)
    val e1 = UUID.randomUUID()
    val e2 = UUID.randomUUID()

    for
      _ <- check(host, deviceA, Map(moving -> rebuildingIn(e1)))
      _ <- yieldUntil(crl.synchronized(crl.drops.contains(moving)))
      discarding = host.control.rebuilds.isRebuilding(moving)
      _ <- check(host, deviceA, Map(moving -> rebuildingIn(e2)))
      next = recorded(host).armLookup(deviceA)
      _ = next.success(deviceState(deviceA, Map(moving -> rebuildingIn(e2))))
      _ = crl.synchronized(crl.drops(moving)).success(())
      _ <- yieldUntil(factory.created.nonEmpty)
    yield
      discarding should be(true)
      os.exists(storePath) should be(false)
      factory.created.map(_.episode).toList should be(List(Some(e2)))

  /** C1, review Minor 1: a lost copy's discard fails (its CRL drop) after the store was re-placed away and transferred
   *  back. The transfer in held back while the discard ran is not left for the periodic check: the failed discard
   *  schedules its device's backoff re-check, as a failed rebuild pass does, and that check starts the transfer. */
  atest("a failed discard re-checks its device so a held transfer in proceeds (C1)"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = hostRoot / os.up / "injected-device"
    os.makeDir.all(deviceDir)
    val storePath = writeStoreConfig(deviceDir, moving)
    os.write.over(storePath / RebuildingStore.RebuildRequiredMarkerFile, "")
    val crl = new HeldDropCrl
    val factory = new HeldRebuildFactory
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers, storeRebuildFactory = factory)
    val local = new LocalStorageDeviceState(deviceA, deviceDir, deviceDir / StorageDeviceConfig.configFilename, crl)
    injectLoadedDevice(host, local)

    for
      _ <- check(host, deviceA, Map(moving -> rebuildingIn(UUID.randomUUID())))
      _ <- yieldUntil(crl.synchronized(crl.drops.contains(moving)))
      _ <- check(host, deviceA, Map(moving -> transferringIn))
      _ <- pumpUntil(recorded(host).transferSourceReads.nonEmpty)
      heldBack = transfers.transfersIn.isEmpty
      _ = crl.synchronized(crl.drops(moving)).failure(new Exception("CRL drop failed"))
      _ <- yieldUntil(!host.control.rebuilds.isRebuilding(moving))
      retryScheduled = host.control.rebuilds.rebuildRetriesScheduled.contains(moving)
      // The retry's check retires the copy as an earlier tenancy, and the check that follows starts the receive
      _ = List(1, 2).foreach(_ =>
        recorded(host).armLookup(deviceA).success(deviceState(deviceA, Map(moving -> transferringIn))))
      _ = runRebuildRetries(host)
      _ <- pumpUntil(transfers.transfersIn.nonEmpty)
    yield
      heldBack should be(true)
      retryScheduled should be(true)
      transfers.transfersIn.map(_._1) should be(Vector(moving))
      factory.created should be(empty)

  /** C4: a create's flip commits Active but is reported Unknown. Before its retry, a backend failure has the store
   *  requested for rebuild and a check queues the rebuild (silenced). The create's work ended when its copy was
   *  served, so the two never overlap; and while the flip is still pending a second create is not started. */
  atest("a rebuild requested while a create's flip is pending does not overlap Create (C4)"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new HeldRebuildFactory
    // No slot: the rebuild stays queued, its only work the silence
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 0)
    val tclient = client.asInstanceOf[TestNetwork.TClient]
    // A real pool, so the create gets as far as serving the store
    val creating = StoreId(PoolId.BootstrapPoolId, 0)
    @volatile var hooked = false
    @volatile var createWorkAtRetry = true
    @volatile var createWorkAfterSecondCreate = true
    @volatile var createWorkWhileQueued = true
    @volatile var queued = false

    def touches(ptr: KeyValueObjectPointer)(txd: TransactionDescription): Boolean =
      txd.allReferencedObjectsSet.exists(_.id == ptr.id)

    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setStoreEntry(creating, StorageDeviceState.StoreStatus.Initializing, None))
      _ <- waitForTransactionsToComplete()
      devPtr <- client.getStorageDevicePointer(deviceA)
      _ = net.reportCommitAsUnknown(touches(devPtr))
      _ = tclient.beforeRetry =
        case _: TransactionOutcomeUnknown if !hooked =>
          hooked = true
          val local = loadedDevices(host)(deviceA)
          createWorkAtRetry = host.control.rebuilds.isCreating(creating)
          // A check that read the entry Initializing before the flip committed
          host.control.rebuilds.createNewStore(local, creating)
          createWorkAfterSecondCreate = host.control.rebuilds.isCreating(creating)
          for
            _ <- setDeviceState(deviceA)(_.setRebuilding(creating, UUID.randomUUID()))
            _ = host.control.reconciler.checkStorageDevice(deviceA)
            _ <- yieldUntil(host.control.rebuilds.silencedStores.contains(creating) &&
                            !host.control.reconciler.activeDeviceChecks.contains(deviceA))
          yield
            queued = host.control.rebuilds.silencedStores.contains(creating)
            createWorkWhileQueued = host.control.rebuilds.isCreating(creating)
        case _ => Future.unit
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ <- pumpFor(hooked && !host.control.rebuilds.createFlipsPending.contains(creating), drain = Some(host))
      _ = tclient.beforeRetry = _ => Future.unit
    yield
      hooked should be(true)
      createWorkAtRetry should be(false)
      createWorkAfterSecondCreate should be(false)
      queued should be(true)
      createWorkWhileQueued should be(false)
      host.control.rebuilds.createFlipsPending should not contain creating
      recorded(host).loadStoreRequests.toList should be(List((deviceA, creating)))

  /** Ship x discard (audit review Minor 1): a lost copy (rebuild-required, offline, its entry Active again after its
   *  transfer was aborted) whose ship is still registered is neither flipped to Rebuilding nor retired until the ship
   *  ends: the retire would move the directory the archiver may still be reading. */
  atest("a lost copy still being shipped is not retired until the ship ends"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val storePath = writeStoreConfig(deviceDir, moving)
    os.write.over(storePath / RebuildingStore.RebuildRequiredMarkerFile, "")
    val sends = new StagedSendService
    val factory = new HeldRebuildFactory
    val host = newHost(hostRoot, transferService = sends, storeRebuildFactory = factory)
    loadedDevices(host)(deviceA).offlineStores += moving
    sends.beginTransferOut(moving, UUID.randomUUID(), storePath, "localhost", 1, org.aspen_ddp.aspen.common.HLCTimestamp.now)

    def entry(): Future[Option[StorageDeviceState.StoreStatus]] =
      client.getStorageDeviceState(deviceA).map(_.stores.get(moving).map(_.status))

    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setStoreEntry(moving, StorageDeviceState.StoreStatus.Active, None))
      _ <- waitForTransactionsToComplete()
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ <- pumpUntil(false, rounds = 3)
      whileShipping <- entry()
      keptWhileShipping = os.exists(storePath)
      _ = sends.endTransferOut(moving)
      _ = runDeviceRechecks(host)
      _ <- pumpUntil(factory.created.nonEmpty)
      afterShip <- entry()
    yield
      whileShipping should be(Some(StorageDeviceState.StoreStatus.Active))
      keptWhileShipping should be(true)
      afterShip should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      os.exists(storePath) should be(false)
      factory.created.map(_.storeId).toList should be(List(moving))

  /** Ship x discard, the entry already Rebuilding: its rebuild does not discard the copy while the ship runs */
  atest("a lost copy still being shipped is not discarded by its rebuild until the ship ends"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val storePath = writeStoreConfig(deviceDir, moving)
    os.write.over(storePath / RebuildingStore.RebuildRequiredMarkerFile, "")
    val sends = new StagedSendService
    val factory = new HeldRebuildFactory
    val host = newHost(hostRoot, transferService = sends, storeRebuildFactory = factory)
    loadedDevices(host)(deviceA).offlineStores += moving
    sends.beginTransferOut(moving, UUID.randomUUID(), storePath, "localhost", 1, org.aspen_ddp.aspen.common.HLCTimestamp.now)
    val rebuilding = Map(moving -> rebuildingIn(UUID.randomUUID()))

    for
      _ <- check(host, deviceA, rebuilding)
      // Long enough for a discard to have moved the copy
      _ <- yieldUntil(!os.exists(storePath))
      keptWhileShipping = os.exists(storePath)
      discardingWhileShipping = host.control.rebuilds.isRebuilding(moving)
      _ = sends.endTransferOut(moving)
      _ = recorded(host).armLookup(deviceA).success(deviceState(deviceA, rebuilding))
      _ = runDeviceRechecks(host)
      _ <- yieldUntil(factory.created.nonEmpty)
    yield
      keptWhileShipping should be(true)
      discardingWhileShipping should be(false)
      os.exists(storePath) should be(false)
      factory.created.map(_.storeId).toList should be(List(moving))

  // A store of the bootstrap pool, whose pool entry names the bootstrap device: startOut reads both fresh
  private val shipped = StoreId(PoolId.BootstrapPoolId, 1)

  private def ask(host: Host): Future[Try[Unit]] =
    given ExecutionContext = executionContext
    host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, source, shipped,
      HLCTimestamp.now, UUID.randomUUID())).transform(Success(_))

  private def queuedCloses(host: Host): List[Runnable] = host.threading match
    case t: LockstepThreading => t.queuedItems.filter:
      case e: Host.EventTask => e.event.isInstanceOf[Host.ShutdownStore]
      case _ => false
    case _ => Nil

  /** C5: a retransmitted StartStoreTransfer arrives while the first request's close (and CRL extraction) of the store
   *  is still running. It does nothing: the store is closed and extracted once, and its send begun once. Loaded: the
   *  close is the store's ShutdownStore, still queued. Unloaded: the extraction's read of the device CRL is held */
  for loaded <- List(true, false) do
    atest(s"two StartStoreTransfer requests close and extract once (${if loaded then "loaded" else "unloaded"}, C5)"):
      given ExecutionContext = executionContext
      val hostRoot = newHostDir()
      val deviceDir = hostRoot / os.up / "injected-device"
      os.makeDir.all(deviceDir)
      val storePath = writeStoreConfig(deviceDir, shipped)
      val crl = new HeldReadCrl
      val sends = new StagedSendService
      val host = newHost(hostRoot, transferService = sends)
      val local = new LocalStorageDeviceState(source, deviceDir, deviceDir / StorageDeviceConfig.configFilename, crl)
      injectLoadedDevice(host, local)
      if loaded then
        installServedElsewhere(host, shipped)
      else
        local.offlineStores += shipped

      // The first close, completed by hand once the second request is refused
      def finishFirstClose(): Unit =
        if loaded then
          host.threading match
            case t: LockstepThreading =>
              t.removeQueued:
                case e: Host.EventTask => e.event.isInstanceOf[Host.ShutdownStore]
                case _ => false
              .foreach:
                case e: Host.EventTask => e.event match
                  case Host.ShutdownStore(storeId, _, completion) =>
                    host.control.router.remove(storeId)
                    completion.success(())
                  case _ => ()
                case _ => ()
            case _ => ()
        else
          crl.synchronized(crl.reads(shipped)).success(())

      def firstCloseRunning: Boolean =
        if loaded then queuedCloses(host).nonEmpty else crl.synchronized(crl.reads.contains(shipped))

      for
        _ <- net.createSecondDevice()
        _ <- waitForTransactionsToComplete()
        _ <- client.transferStore(shipped, net.secondDeviceId)
        _ <- waitForTransactionsToComplete()
        first = ask(host)
        _ <- pumpFor(firstCloseRunning)
        second <- ask(host)
        closesWhileFirstRuns = recorded(host).transferOutCloses.toList
        queuedWhileFirstRuns = queuedCloses(host).size
        readsWhileFirstRuns = crl.synchronized(crl.readCount.getOrElse(shipped, 0))
        _ = finishFirstClose()
        _ <- pumpFor(sends.isTransferringOut(shipped))
        _ <- first
      yield
        closesWhileFirstRuns should be(List(shipped))
        if loaded then
          queuedWhileFirstRuns should be(1)
        else
          readsWhileFirstRuns should be(1)
        second.isFailure should be(true)
        sends.transfersOut.map(_._1) should be(Vector(shipped))
        FileStat.exists(storePath / StoreTransfer.TransferOutMarkerFile) should be(true)
        host.control.transfersOut.isClosingForTransfer(shipped) should be(false)

  /** C3: a rebuild's flip commits Active but is reported Unknown. Before its retry reads the entry, an operator's
   *  transfer makes it TransferringOut and the receiver's StartStoreTransfer arrives. The request is refused while
   *  the rebuild is registered (its retry still pending); the retry then finds the entry moved on (Unconfirmed) and
   *  keeps the copy offline, and the receiver's retransmit starts the transfer */
  atest("a transfer out waits for an outcome-unknown flip's retry (C3)"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", source)
    val factory = new HeldRebuildFactory
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends, storeRebuildFactory = factory)
    val tclient = client.asInstanceOf[TestNetwork.TClient]
    val episode = UUID.randomUUID()
    val rebuilding = Map(shipped -> rebuildingIn(episode))
    @volatile var hooked = false
    @volatile var registeredAtRequest = false
    @volatile var requestWhileRegistered: Option[Try[Unit]] = None
    @volatile var closesWhileRegistered: List[StoreId] = Nil

    def touches(ptr: KeyValueObjectPointer)(txd: TransactionDescription): Boolean =
      txd.allReferencedObjectsSet.exists(_.id == ptr.id)

    for
      _ <- net.createSecondDevice()
      _ <- setDeviceState(source)(_.setRebuilding(shipped, episode))
      _ <- waitForTransactionsToComplete()
      _ <- check(host, source, rebuilding)
      _ <- yieldUntil(factory.created.nonEmpty)
      // Where the rebuild placed its copy
      storePath = writeStoreConfig(deviceDir, shipped)
      devPtr <- client.getStorageDevicePointer(source)
      _ = net.reportCommitAsUnknown(touches(devPtr))
      _ = tclient.beforeRetry =
        case _: TransactionOutcomeUnknown if !hooked =>
          hooked = true
          for
            _ <- client.transferStore(shipped, net.secondDeviceId)
            _ = registeredAtRequest = host.control.rebuilds.rebuildingStoreIds.contains(shipped)
            r <- ask(host)
          yield
            requestWhileRegistered = Some(r)
            closesWhileRegistered = recorded(host).transferOutCloses.toList
        case _ => Future.unit
      // The checks the flip's outcome asks for read the entry the transfer wrote
      _ = List(1, 2).foreach(_ => recorded(host).armLookup(source).success(deviceState(source,
            Map(shipped -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.TransferringOut,
              Some(net.secondDeviceId), Some(UUID.randomUUID()))))))
      _ = factory.created.head.promise.success(())
      _ <- pumpFor(hooked && !host.control.rebuilds.rebuildingStoreIds.contains(shipped), drain = Some(host))
      _ = tclient.beforeRetry = _ => Future.unit
      outcome = recorded(host).rebuildOutcomes.get((source, shipped))
      // The receiver's retransmit
      retransmit <- ask(host)
      _ <- pumpFor(sends.isTransferringOut(shipped), drain = Some(host))
    yield
      hooked should be(true)
      registeredAtRequest should be(true)
      closesWhileRegistered should be(Nil)
      requestWhileRegistered.exists(_.isFailure) should be(true)
      outcome should be(Some(RebuildOutcome.Unconfirmed))
      retransmit.isSuccess should be(true)
      recorded(host).transferOutCloses.toList should be(List(shipped))
      sends.transfersOut.map(_._1) should be(Vector(shipped))
      FileStat.exists(storePath / StoreTransfer.TransferOutMarkerFile) should be(true)

  /** Double open (audit Step 4): a store's backend has opened and its LoadStore is queued, not yet handled. A second
   *  load offer meanwhile (a duplicate LoadStoreById, say) finds the store still opening and starts no second open,
   *  which would fail on the backend's lock and drop the store, served once the LoadStore installs it, from its
   *  device's loadedStores: the device's shutdown would then never close it */
  atest("a second load offer between open and install starts no second open, and the device's shutdown closes the served store"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot)
    val local = loadedDevices(host)(deviceA)
    val storePath = deviceDir / moving.directoryName
    DefaultBackendOpener.createStore(StoreConfig(moving, StoreConfig.RocksDB(None), None, Replication(3, 2)),
      storePath, deviceDir / StoreCreation.CreatingDirectory)

    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.setStoreEntry(moving, StorageDeviceState.StoreStatus.Active, None))
      _ <- waitForTransactionsToComplete()
      // RocksDB opens inline: the LoadStore is queued on return
      _ = host.control.loader.tryLoadStore(local, storePath)
      queued = recorded(host).loadStoreRequests.toList
      _ = host.control.loader.tryLoadStore(local, storePath)
      opens = host.control.loader.opensStarted(moving)
      _ = host.testingOnlyHandleEvents()
      _ <- yieldUntil(host.control.router.loaded.contains(moving))
      served = host.control.router.loaded.get(moving)
      listed = local.loadedStores.contains(moving)
      _ <- client.shutdownStorageDevice(deviceA)
      _ <- waitForTransactionsToComplete()
      _ = host.control.reconciler.checkAll()
      _ <- pumpFor(!loadedDevices(host).contains(deviceA), drain = Some(host))
      _ <- served.map(_.backend.close()).getOrElse(Future.unit)
    yield
      queued should be(List((deviceA, moving)))
      opens should be(1)
      served should be(defined)
      listed should be(true)
      recorded(host).shutdownCloses.toList should be(List(moving))
