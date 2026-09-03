package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Promise
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

  test("a repair for an unhosted store fails rather than hanging"):
    for
      kvos <- client.read(radicle, "unhosted repair")
      p = Promise[Unit]()
      _ = net.smgr.repair(unhostedStore, kvos, p)
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

  test("a sweep drives the live host's own stores end to end"):
    val clock = AtomicLong(0L)
    val bgTasks = BackgroundTaskManager(scala.concurrent.ExecutionContext.global)
    val svc = new RepairService(client, net.smgr.hostId, net.smgr, bgTasks, () => clock.get)
    val store0 = StoreId(PoolId.BootstrapPoolId, 0.toByte)

    val testFuture = for
      // The first sweep only admits. The second, past the floor, is the one that scans: it walks
      // each store's real errorTree through the live host rather than a stubbed target.
      _ <- svc.sweep()
      _ = clock.set(120_000L)
      _ <- svc.sweep()
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
