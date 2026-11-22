package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StorageDeviceConfig}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.BackendOpener

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

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

/** A device CRL whose first dropStore of each store waits for the test: a retire held up by a slow CRL */
private class HeldDropCrl extends TestNetwork.TestCRL:
  val drops: mutable.Map[StoreId, Promise[Unit]] = mutable.Map()

  override def dropStore(storeId: StoreId): Future[Unit] = synchronized:
    drops.getOrElseUpdate(storeId, Promise[Unit]()).future

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
