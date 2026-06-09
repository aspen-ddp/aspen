package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StorageDeviceSetStateSuite extends AnyFunSuite with Matchers:

  test("StorageDeviceSetState round-trips through codec (level 0, no parent)"):
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "leaf-set",
      level = 0,
      parent = None,
      memberDevices = List(StorageDeviceId(UUID.randomUUID()), StorageDeviceId(UUID.randomUUID())),
      memberSets = Nil,
      assignedPools = List(PoolId(UUID.randomUUID()))
    )

    val decoded = StorageDeviceSetState(original.toBytes)

    decoded should be(original)

  test("StorageDeviceSetState round-trips through codec (level 1, with parent)"):
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "upper-set",
      level = 1,
      parent = Some(StorageDeviceSetId(UUID.randomUUID())),
      memberDevices = Nil,
      memberSets = List(StorageDeviceSetId(UUID.randomUUID())),
      assignedPools = Nil
    )

    val decoded = StorageDeviceSetState(original.toBytes)

    decoded should be(original)
