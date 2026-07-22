package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class RebalancingServiceStateSuite extends AnyFunSuite with Matchers:

  test("ActiveRebalancingTasks round-trips"):
    val entries = List(
      StorageDeviceSetId(UUID.randomUUID()) -> KeyValueObjectPointer(Radicle.objectId, Radicle.poolId),
      StorageDeviceSetId(UUID.randomUUID()) -> KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)
    )
    val bytes = RebalancingServiceState.encodeActiveTasks(entries)
    val decoded = RebalancingServiceState.decodeActiveTasks(bytes)
    decoded.map(_._1) shouldBe entries.map(_._1)
    decoded.map(_._2.id) shouldBe entries.map(_._2.id)

  test("empty list round-trips"):
    RebalancingServiceState.decodeActiveTasks(
      RebalancingServiceState.encodeActiveTasks(Nil)) shouldBe Nil
