package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StorageDeviceSetStateCodecSuite extends AnyFunSuite with Matchers:

  private def dev(): StorageDeviceId = StorageDeviceId(UUID.randomUUID())

  test("round-trips with a non-empty pendingTransfers list"):
    val poolId = PoolId(UUID.randomUUID())
    val from = dev()
    val to = dev()
    val transfers = List(
      (StoreId(poolId, 0.toByte), from, to),
      (StoreId(poolId, 1.toByte), to, from)
    )
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "s",
      level = 0,
      parent = None,
      memberDevices = List(from, to),
      memberSets = Nil,
      assignedPools = List(poolId),
      pendingTransfers = transfers
    )
    val decoded = StorageDeviceSetState(original.toBytes)
    decoded.pendingTransfers shouldBe transfers

  test("round-trips with an empty pendingTransfers list"):
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "s",
      level = 0,
      parent = None,
      memberDevices = Nil,
      memberSets = Nil,
      assignedPools = Nil,
      pendingTransfers = Nil
    )
    StorageDeviceSetState(original.toBytes).pendingTransfers shouldBe Nil
