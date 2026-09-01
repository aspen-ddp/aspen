package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.TypeFactories
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.compute.DurableTaskFactory
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future}

class MigratePoolToSetSuite extends IntegrationTestSuite:

  atest("MigratePoolToSetDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    // SystemTaskExecutorService reconstructs a task by looking its type UUID up here; an
    // unregistered factory makes prepareSystemDurableTask StopRetrying.
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      MigratePoolToSetDurableTask.typeUUID)
    factory should be(Some(MigratePoolToSetDurableTask))
    TypeFactories.factories should contain(MigratePoolToSetDurableTask)

  /** The task state objects currently enrolled with the system task executor. */
  private def enrolledTasks(): Future[List[(java.util.UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- tkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(vs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield enrolled

  atest("migratePoolToSet flips the pointer, swaps assignedPools, and enrolls one task"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      targetSetId <- client.createStorageDeviceSet("migration-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      target <- client.getStorageDeviceSetState(targetSetId)
      old <- client.getStorageDeviceSetState(oldSetId)
      enrolled <- enrolledTasks()
    yield
      poolState.storageDeviceSet should be(targetSetId)
      poolState.migration should be(Some(StoragePoolState.Migration(
        targetSetId, StoragePoolState.MigrationStatus.InProgress)))
      target.assignedPools should contain(poolId)
      old.assignedPools should not contain poolId
      enrolled.size should be(1)

  atest("migratePoolToSet to the pool's current set is a no-op"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- client.migratePoolToSet(poolId, setId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      set <- client.getStorageDeviceSetState(setId)
      enrolled <- enrolledTasks()
    yield
      poolState.migration should be(None)
      poolState.storageDeviceSet should be(setId)
      set.assignedPools.count(_ == poolId) should be(1)
      enrolled shouldBe empty

  atest("migratePoolToSet fails with NoSuchElementException for an unknown target set"):
    given ExecutionContext = executionContext
    val unknown = StorageDeviceSetId(java.util.UUID.randomUUID())
    recoverToSucceededIf[NoSuchElementException](
      client.migratePoolToSet(PoolId.BootstrapPoolId, unknown))

  atest("retargeting swaps assignedPools to the new target and enrolls no second task"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      firstId <- client.createStorageDeviceSet("first-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      secondId <- client.createStorageDeviceSet("second-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, firstId)
      _ <- waitForTransactionsToComplete()
      _ <- client.migratePoolToSet(poolId, secondId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      first <- client.getStorageDeviceSetState(firstId)
      second <- client.getStorageDeviceSetState(secondId)
      enrolled <- enrolledTasks()
    yield
      poolState.storageDeviceSet should be(secondId)
      poolState.migration.get.targetSet should be(secondId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.InProgress)
      second.assignedPools should contain(poolId)
      first.assignedPools should not contain poolId
      // The running task holds only the pool id and re-reads its target, so it is reused.
      enrolled.size should be(1)
