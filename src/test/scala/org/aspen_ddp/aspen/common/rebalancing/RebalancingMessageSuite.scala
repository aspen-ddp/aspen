package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class RebalancingMessageSuite extends AnyFunSuite with Matchers:

  test("NewSetRebalanceInitiated round-trips"):
    val m = NewSetRebalanceInitiated(StorageDeviceSetId(UUID.randomUUID()))
    RebalancingMessage.decode(RebalancingMessage.encode(m)) shouldBe m

  test("TransferComplete round-trips"):
    val m = TransferComplete(
      StorageDeviceSetId(UUID.randomUUID()),
      StoreId(PoolId(UUID.randomUUID()), 3.toByte),
      StorageDeviceId(UUID.randomUUID()),
      StorageDeviceId(UUID.randomUUID()))
    RebalancingMessage.decode(RebalancingMessage.encode(m)) shouldBe m

  test("AutoRebalancePeriodChanged round-trips"):
    RebalancingMessage.decode(
      RebalancingMessage.encode(AutoRebalancePeriodChanged)) shouldBe AutoRebalancePeriodChanged
