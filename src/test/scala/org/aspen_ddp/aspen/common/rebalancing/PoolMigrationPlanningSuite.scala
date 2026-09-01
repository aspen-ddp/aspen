package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision

import scala.concurrent.{ExecutionContext, Future}

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

  /** Write `migration` directly into a pool's config. Task 8 adds the real entry point; these
   *  tests predate it and only need the recorded state. */
  private def setMigration(poolId: PoolId,
                           migration: Option[StoragePoolState.Migration]): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        poolPtr <- client.getStoragePoolPointer(poolId)
        kvos <- client.read(poolPtr, "test migration write")
      yield
        val updated = StoragePoolState(kvos).copy(migration = migration)
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey,
          kvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, updated.encode()))
        tx.update(poolPtr, None, None, reqs, ops)

  atest("getStateForRebalancePlanning omits a pool whose migration is InProgress"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      setState <- client.getStorageDeviceSetState(setId)
      before <- State.getStateForRebalancePlanning(client, setState)

      _ <- setMigration(poolId, Some(StoragePoolState.Migration(
             StorageDeviceSetId(java.util.UUID.randomUUID()),
             StoragePoolState.MigrationStatus.InProgress)))
      _ <- waitForTransactionsToComplete()

      after <- State.getStateForRebalancePlanning(client, setState)
    yield
      before.pools.keySet should contain(poolId)
      after.pools.keySet should not contain poolId
      // Its stores also stop being movable candidates on every device.
      after.devices.values.flatMap(_.stores.keys).count(_.poolId == poolId) should be(0)
      // Device capacity is self-reported, so it is unaffected by the exclusion.
      after.devices.keySet should be(before.devices.keySet)

  atest("getStateForRebalancePlanning keeps a pool whose migration is Complete"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- setMigration(poolId, Some(StoragePoolState.Migration(
             StorageDeviceSetId(java.util.UUID.randomUUID()),
             StoragePoolState.MigrationStatus.Complete)))
      _ <- waitForTransactionsToComplete()
      setState <- client.getStorageDeviceSetState(setId)
      planning <- State.getStateForRebalancePlanning(client, setState)
    yield
      planning.pools.keySet should contain(poolId)
