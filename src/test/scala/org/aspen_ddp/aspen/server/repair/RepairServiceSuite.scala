package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, MINUTES, SECONDS}

class RepairServiceSuite extends IntegrationTestSuite:

  private val storeIds: List[StoreId] =
    (0 until 8).toList.map(i => StoreId(PoolId.BootstrapPoolId, i.toByte))

  /** Reports a fixed store list; never asked to repair anything, since scanStore is stubbed. */
  private class FixedTarget extends RepairTarget:
    def repairableStoreIds: List[StoreId] = storeIds
    def isRepairable(storeId: StoreId): Boolean = storeIds.contains(storeId)
    def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
      completion.success(())
    def repairDelete(sid: StoreId, oid: ObjectId,
                     sp: Array[Byte], completion: Promise[Unit]): Unit =
      completion.success(())

  /** Counts concurrent scans and optionally fails one store's scan. */
  private class CountingService(clock: AtomicLong, failFor: Option[StoreId])
    extends RepairService(client, HostId.BootstrapHostId, FixedTarget(),
                          BackgroundTaskManager.NoBackgroundTaskManager,
                          () => clock.get):

    val inFlight = AtomicInteger(0)
    val maxInFlight = AtomicInteger(0)
    val scanned = AtomicInteger(0)

    override protected def scanStore(storeId: StoreId,
                                     policy: RepairPolicy): Future[ScanResult] =
      if failFor.contains(storeId) then
        Future.failed(RuntimeException(s"scan of $storeId failed"))
      else
        val n = inFlight.incrementAndGet()
        maxInFlight.updateAndGet(m => math.max(m, n))
        Future:
          Thread.sleep(25)
          inFlight.decrementAndGet()
          scanned.incrementAndGet()
          ScanResult.Empty

  test("the first sweep admits stores without scanning them"):
    val clock = AtomicLong(0L)
    val svc = CountingService(clock, None)
    svc.sweep().map: _ =>
      svc.scanned.get shouldBe 0
      svc.testingOnlyScanStates.keySet shouldBe storeIds.toSet

  test("concurrent scans never exceed the host limit"):
    val clock = AtomicLong(0L)
    val svc = CountingService(clock, None)
    for
      _ <- svc.sweep()
      _ = clock.set(60_000L)
      _ <- svc.sweep()
    yield
      svc.scanned.get shouldBe 8
      svc.maxInFlight.get should be <= HostRepairLimits.Default.maxConcurrentStoreScans
      svc.maxInFlight.get should be > 1

  test("one failing store does not abort the sweep for the rest"):
    val clock = AtomicLong(0L)
    val svc = CountingService(clock, Some(storeIds(3)))
    for
      _ <- svc.sweep()
      _ = clock.set(60_000L)
      _ <- svc.sweep()
    yield
      svc.scanned.get shouldBe 7

  test("a departed store is dropped from the pacing map and re-admitted if it returns"):
    val clock = AtomicLong(0L)
    var hosted = storeIds
    val target = new RepairTarget:
      def repairableStoreIds: List[StoreId] = hosted
      def isRepairable(storeId: StoreId): Boolean = hosted.contains(storeId)
      def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
        completion.success(())
      def repairDelete(sid: StoreId, oid: ObjectId,
                       sp: Array[Byte], completion: Promise[Unit]): Unit =
        completion.success(())

    val svc = new RepairService(client, HostId.BootstrapHostId, target,
                                BackgroundTaskManager.NoBackgroundTaskManager, () => clock.get)
    for
      _ <- svc.sweep()
      _ = svc.testingOnlyScanStates.keySet shouldBe storeIds.toSet
      _ = hosted = storeIds.drop(1)
      _ = clock.set(60_000L)
      _ <- svc.sweep()
      _ = svc.testingOnlyScanStates.keySet shouldBe storeIds.drop(1).toSet
      _ = hosted = storeIds
      _ = clock.set(120_000L)
      _ <- svc.sweep()
    yield
      // Re-admitted at the floor, due inside the first floor interval from 120s.
      val readmitted = svc.testingOnlyScanStates(storeIds.head)
      readmitted.currentInterval shouldBe RepairPolicy.Default.scanIntervalFloor
      readmitted.nextDue should be >= 120_000L
      readmitted.nextDue should be < 120_000L + RepairPolicy.Default.scanIntervalFloor.toMillis

  test("a scan that finds entries snaps the store back to the floor"):
    val clock = AtomicLong(0L)

    class FindingService extends RepairService(client, HostId.BootstrapHostId, FixedTarget(),
                                               BackgroundTaskManager.NoBackgroundTaskManager,
                                               () => clock.get):
      override protected def scanStore(storeId: StoreId,
                                       policy: RepairPolicy): Future[ScanResult] =
        Future.successful(ScanResult(seen = 3, repaired = 2, deferred = 1))

    val svc = FindingService()
    for
      _ <- svc.sweep()
      _ = clock.set(60_000L)
      _ <- svc.sweep()
    yield
      // nextDue past the scan instant proves advance() ran at all; the interval still sitting at
      // the floor rather than doubled to 60s proves it took the found-entries branch.
      val s = svc.testingOnlyScanStates(storeIds.head)
      s.currentInterval shouldBe RepairPolicy.Default.scanIntervalFloor
      s.nextDue should be > 60_000L

  test("a scan that never completes does not wedge the sweep"):
    val clock = AtomicLong(0L)
    val bgTasks = BackgroundTaskManager(scala.concurrent.ExecutionContext.global)

    class HangingService extends RepairService(client, HostId.BootstrapHostId, FixedTarget(),
                                               bgTasks, () => clock.get):
      override protected def scanDeadline: FiniteDuration =
        FiniteDuration(50, MILLISECONDS)

      override protected def scanStore(storeId: StoreId,
                                       policy: RepairPolicy): Future[ScanResult] =
        if storeId == storeIds.head then Promise[ScanResult]().future
        else Future.successful(ScanResult.Empty)

    val svc = HangingService()
    val testFuture = for
      _ <- svc.sweep()
      _ = clock.set(60_000L)
      _ <- svc.sweep()
    yield
      // The sweep completing at all is the point. The hung store having advanced to a doubled
      // interval proves it went through the timeout and the recover rather than being skipped.
      val hung = svc.testingOnlyScanStates(storeIds.head)
      hung.currentInterval shouldBe Duration(60, SECONDS)
      hung.nextDue should be > 60_000L

    testFuture.andThen: _ =>
      svc.cancel()
      bgTasks.shutdown(FiniteDuration(2, SECONDS))

  test("a metadata read that never completes does not wedge the sweep"):
    val clock = AtomicLong(0L)
    val bgTasks = BackgroundTaskManager(scala.concurrent.ExecutionContext.global)

    class HangingMetadataService extends RepairService(client, HostId.BootstrapHostId,
                                                       FixedTarget(), bgTasks, () => clock.get):
      override protected def metadataDeadline: FiniteDuration =
        FiniteDuration(50, MILLISECONDS)

      override protected def readPolicy(poolId: PoolId): Future[RepairPolicy] =
        Promise[RepairPolicy]().future

      override protected def scanStore(storeId: StoreId,
                                       policy: RepairPolicy): Future[ScanResult] =
        Future.successful(ScanResult.Empty)

    val svc = HangingMetadataService()
    val testFuture = for
      _ <- svc.sweep()
      _ = clock.set(60_000L)
      _ <- svc.sweep()
    yield
      // The sweep completing at all is the core property. The stores having been scanned/paced
      // despite the hung policy read proves the timeout path worked and did not skip the stores.
      svc.testingOnlyScanStates.keySet shouldBe storeIds.toSet
      val representative = svc.testingOnlyScanStates(storeIds.head)
      representative.nextDue should be > 60_000L

    testFuture.andThen: _ =>
      svc.cancel()
      bgTasks.shutdown(FiniteDuration(2, SECONDS))

  test("the sweep tick period matches the default scan floor"):
    Future.successful:
      RepairService.TickPeriod shouldBe RepairPolicy.Default.scanIntervalFloor
      RepairService.StallAfter should be > scala.concurrent.duration.Duration(30, MINUTES)
