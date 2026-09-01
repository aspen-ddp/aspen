package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.ExecutionContext

class PoolMigrationPlanningSuite extends IntegrationTestSuite:

  atest("getStateForPoolMigration holds one pool and the store devices plus the destination"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      planning <- State.getStateForPoolMigration(client, poolId, net.secondDeviceId)
      poolState <- client.getStoragePoolState(poolId)
    yield
      planning.pools.keySet should be(Set(poolId))
      planning.pools(poolId).ida should be(poolState.ida)
      planning.pools(poolId).stores.keySet should be(
        poolState.stores.indices.map(i => StoreId(poolId, i.toByte)).toSet)

      planning.devices.keySet should be(
        poolState.stores.map(_.storageDeviceId).toSet + net.secondDeviceId)

      // The destination is empty, so it carries none of the pool's stores.
      planning.devices(net.secondDeviceId).stores should be(empty)

  atest("getStateForPoolMigration attributes each store to its pool device of record"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      poolState <- client.getStoragePoolState(poolId)
      planning <- State.getStateForPoolMigration(client, poolId, net.secondDeviceId)
    yield
      poolState.stores.indices.foreach: i =>
        val storeId = StoreId(poolId, i.toByte)
        val deviceOfRecord = poolState.stores(i).storageDeviceId
        planning.devices(deviceOfRecord).stores.keySet should contain(storeId)
        planning.pools(poolId).stores(storeId).status should be(
          StorageDeviceState.StoreStatus.Active)
      succeed

  atest("getStateForPoolMigration lets TransferSafety approve a transfer to a fresh device"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      poolState <- client.getStoragePoolState(poolId)
      planning <- State.getStateForPoolMigration(client, poolId, net.secondDeviceId)
    yield
      val storeId = StoreId(poolId, 0.toByte)
      val source = poolState.stores(0).storageDeviceId
      // Replication(3, 2) with three Active stores: 3 - 1 == 2 >= writeThreshold.
      TransferSafety.isSafe(planning, Set.empty,
        Plan.Transfer(storeId, source, net.secondDeviceId)) should be(true)
