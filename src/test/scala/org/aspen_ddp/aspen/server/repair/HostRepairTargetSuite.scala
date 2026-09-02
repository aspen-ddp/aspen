package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.UUID
import scala.concurrent.Promise
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
