package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.store.{ReconcileResult, RepairSnapshot}

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}
import scala.util.{Success, Try}

class HostRepairTargetSuite extends IntegrationTestSuite:

  // The bootstrap pool has three stores at indices 0, 1 and 2. Index 7 is never hosted.
  private val unhostedStore = StoreId(PoolId.BootstrapPoolId, 7.toByte)

  test("repairableStoreIds lists the stores the host currently owns"):
    client.read(radicle, "warm up").map: _ =>
      val ids = net.smgr.repairableStoreIds
      ids should contain (StoreId(PoolId.BootstrapPoolId, 0.toByte))
      ids should not contain unhostedStore

  // The repair fence: a scan runs only while the pool's entry names this host and the device the store is loaded from
  test("localDevice names the device a loaded store is on, and nothing for an unknown store"):
    client.read(radicle, "warm up").map: _ =>
      net.smgr.localDevice(StoreId(PoolId.BootstrapPoolId, 0.toByte)) shouldBe
        Some(StorageDeviceId.BootstrapStorageDeviceId)
      net.smgr.localDevice(unhostedStore) shouldBe None

  // The repair scan leaves an entry alone while its store may still join the entry's transaction (StoreRepairer)
  test("mayJoinTransaction follows the store's forget floor"):
    client.read(radicle, "warm up").map: _ =>
      val store0 = StoreId(PoolId.BootstrapPoolId, 0.toByte)
      def at(offsetMillis: Long) = HLCTimestamp((System.currentTimeMillis() + offsetMillis) << 16)
      // The floor is at most the store's load plus MaxStartLead, or now less the horizon: never above now + 2 s
      withClue("a transaction starting after the floor may be joined: "):
        net.smgr.mayJoinTransaction(store0, at(5000)) shouldBe true
      withClue("one started more than the horizon ago may not: "):
        net.smgr.mayJoinTransaction(store0, at(-60000)) shouldBe false
      withClue("an unhosted store joins nothing: "):
        net.smgr.mayJoinTransaction(unhostedStore, at(5000)) shouldBe false

  test("a repair snapshot for an unhosted store fails rather than hanging"):
    val p = Promise[RepairSnapshot]()
    net.smgr.snapshot(unhostedStore, radicle, p)
    net.handleEvents()
    p.future.transform(t => Success(t)).map: result =>
      result.isFailure shouldBe true
      result.failed.get shouldBe a [StoreNotHosted]

  test("a reconcile for an unhosted store fails rather than hanging"):
    for
      kvos <- client.read(radicle, "unhosted repair")
      p = Promise[ReconcileResult]()
      _ = net.smgr.reconcile(unhostedStore, RepairSnapshot.missing(radicle.id), kvos, p)
      _ = net.handleEvents()
      result <- p.future.transform(t => Success(t))
    yield
      result.isFailure shouldBe true
      result.failed.get shouldBe a [StoreNotHosted]

  test("a repair deletion for an unhosted store fails rather than succeeding"):
    val objectId = ObjectId(new UUID(1, 2))
    val p = Promise[Unit]()
    net.smgr.repairDelete(unhostedStore, objectId, Array[Byte](), p)
    net.handleEvents()
    p.future.transform(t => Success(t)).map: result =>
      result.isFailure shouldBe true
      result.failed.get shouldBe a [StoreNotHosted]

  /** Runs `f`, handling the host's events until it completes. Under LockstepThreading a store's posted work runs only
    * when events are handled, so a scan's later steps would otherwise wait for some unrelated event to do it
    */
  private def pumpUntilComplete[T](f: Future[T]): Future[T] =
    if f.isCompleted then
      f
    else
      Future { Thread.sleep(1); net.handleEvents() }.flatMap(_ => pumpUntilComplete(f))

  test("a sweep drives the live host's own stores end to end"):
    val clock = AtomicLong(0L)
    val bgTasks = BackgroundTaskManager(scala.concurrent.ExecutionContext.global)
    val svc = new RepairService(client, net.smgr.hostId, net.smgr, bgTasks, () => clock.get)
    val store0 = StoreId(PoolId.BootstrapPoolId, 0.toByte)

    val testFuture = for
      // The first sweep only admits. The second, past the floor, is the one that scans: it walks
      // each store's real errorTree through the live host rather than a stubbed target.
      _ <- pumpUntilComplete(svc.sweep())
      _ = clock.set(120_000L)
      _ <- pumpUntilComplete(svc.sweep())
    yield
      svc.testingOnlyScanStates.keySet shouldBe net.smgr.repairableStoreIds.toSet
      // Store 0 is never the one TestNetwork makes miss updates, so its scan finds nothing and
      // backs off. The doubled interval is what proves the scan actually ran.
      val s = svc.testingOnlyScanStates(store0)
      s.currentInterval shouldBe Duration(60, SECONDS)
      s.nextDue should be > 120_000L

    // Sequenced with andThen rather than a bare onComplete so the background task manager is
    // shut down before ScalaTest moves on. A leaked manager has previously contaminated later
    // suites through test ordering.
    testFuture.andThen: _ =>
      svc.cancel()
      bgTasks.shutdown(FiniteDuration(2, SECONDS))
