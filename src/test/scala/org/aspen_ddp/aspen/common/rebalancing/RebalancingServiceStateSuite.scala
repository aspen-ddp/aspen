package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.{Duration, HOURS, MINUTES}

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

  test("an auto-rebalance period round-trips through minutes"):
    RebalancingServiceState.decodeAutoRebalancePeriod(
      RebalancingServiceState.encodeAutoRebalancePeriod(Duration(4, HOURS))
    ) shouldBe Duration(240, MINUTES)

  test("a zero auto-rebalance period round-trips"):
    RebalancingServiceState.decodeAutoRebalancePeriod(
      RebalancingServiceState.encodeAutoRebalancePeriod(Duration.Zero)
    ) shouldBe Duration(0, MINUTES)

  test("a last-auto-rebalance timestamp round-trips"):
    val ts = HLCTimestamp.now
    RebalancingServiceState.decodeLastAutoRebalance(
      RebalancingServiceState.encodeLastAutoRebalance(ts)) shouldBe ts

  test("HLCTimestamp.Zero round-trips as the last-auto-rebalance timestamp"):
    RebalancingServiceState.decodeLastAutoRebalance(
      RebalancingServiceState.encodeLastAutoRebalance(HLCTimestamp.Zero)) shouldBe HLCTimestamp.Zero

  test("initialServiceState populates all three keys with their defaults"):
    val s = RebalancingDurableService.initialServiceState
    s.keySet shouldBe Set(RebalancingServiceState.ActiveTasksKey,
                          RebalancingServiceState.AutoRebalancePeriodKey,
                          RebalancingServiceState.LastAutoRebalanceKey)
    RebalancingServiceState.decodeActiveTasks(
      s(RebalancingServiceState.ActiveTasksKey)) shouldBe Nil
    RebalancingServiceState.decodeAutoRebalancePeriod(
      s(RebalancingServiceState.AutoRebalancePeriodKey)) shouldBe Duration(8, HOURS)
    RebalancingServiceState.decodeLastAutoRebalance(
      s(RebalancingServiceState.LastAutoRebalanceKey)) shouldBe HLCTimestamp.Zero
