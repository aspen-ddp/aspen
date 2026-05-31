package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.client.{KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.{long2byte, byte2long}
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor

import scala.concurrent.{ExecutionContext, Future}

class StoragePoolUsageManagerSuite extends IntegrationTestSuite:

  private def setupUsageManager(): Future[(StoragePoolUsageManager, SimpleTaskExecutor)] =
    given ExecutionContext = executionContext
    for
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new SinglePoolObjectAllocator(client, pool, None)

      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      kvos0 <- client.read(radicle)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)
    yield
      val mgr = new StoragePoolUsageManager(client)
      mgr.setTaskExecutor(executor)
      (mgr, executor)

  private def readPoolState(): Future[(KeyValueObjectState, StoragePoolState)] =
    given ExecutionContext = executionContext
    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
    yield
      (poolKvos, StoragePoolState(poolKvos))

  atest("No update when size is unchanged"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0

      (kvos1, _) <- readPoolState()
      _ <- mgr.updateStoreSize(storeId, 1000L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
      _ <- mgr.updateStoreSize(storeId, 1000L)
      _ <- waitForTransactionsToComplete()

      (kvos3, _) <- readPoolState()
    yield
      val sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)
      kvos2.contents.contains(sizeKey) should be(true)
      byte2long(kvos2.contents(sizeKey).value.bytes) should be(1000L)
      kvos3.contents(sizeKey).revision should be(kvos2.contents(sizeKey).revision)

  atest("Update store size KV pair on first report"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0
      sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)

      (kvos1, _) <- readPoolState()
      _ = kvos1.contents.contains(sizeKey) should be(false)

      _ <- mgr.updateStoreSize(storeId, 5000L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
    yield
      kvos2.contents.contains(sizeKey) should be(true)
      byte2long(kvos2.contents(sizeKey).value.bytes) should be(5000L)

  atest("Update store size KV pair on significant change"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0
      sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)

      _ <- mgr.updateStoreSize(storeId, 10000L)
      _ <- waitForTransactionsToComplete()

      (kvos1, _) <- readPoolState()
      rev1 = kvos1.contents(sizeKey).revision

      _ <- mgr.updateStoreSize(storeId, 10200L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
    yield
      byte2long(kvos2.contents(sizeKey).value.bytes) should be(10200L)
      kvos2.contents(sizeKey).revision should not be rev1

  atest("No update when change is below threshold"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0
      sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)

      _ <- mgr.updateStoreSize(storeId, 10000L)
      _ <- waitForTransactionsToComplete()

      (kvos1, _) <- readPoolState()
      rev1 = kvos1.contents(sizeKey).revision

      _ <- mgr.updateStoreSize(storeId, 10040L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
    yield
      kvos2.contents(sizeKey).revision should be(rev1)

  atest("currentUsage updated when mean changes significantly"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()

      _ <- mgr.updateStoreSize(net.storeId0, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId1, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId2, 10000L)
      _ <- waitForTransactionsToComplete()

      (_, state1) <- readPoolState()
      _ = state1.currentUsage should be(10000L)

      _ <- mgr.updateStoreSize(net.storeId0, 15000L)
      _ <- waitForTransactionsToComplete()

      (_, state2) <- readPoolState()
    yield
      val expectedMean = (15000L + 10000L + 10000L) / 3
      state2.currentUsage should be(expectedMean)

  atest("Durable task prepared when allocationGroups non-empty"):
    given ExecutionContext = executionContext
    import java.util.UUID
    val allocGroup = UUID.randomUUID()

    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
      poolState = StoragePoolState(poolKvos)

      updatedState = poolState.copy(
        allocationGroups = List(allocGroup),
        maximumStoreSize = 100000L
      )
      tx0 = client.newTransaction()
      _ = tx0.update(poolPtr, None, None,
        List(KeyRevision(StoragePoolState.ConfigKey, poolKvos.contents(StoragePoolState.ConfigKey).revision)),
        List(Insert(StoragePoolState.ConfigKey, updatedState.encode())))
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      (mgr, _) <- setupUsageManager()

      _ <- mgr.updateStoreSize(net.storeId0, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId1, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId2, 10000L)
      _ <- waitForTransactionsToComplete()

      _ <- mgr.updateStoreSize(net.storeId0, 15000L)
      _ <- waitForTransactionsToComplete()

      (_, finalState) <- readPoolState()
    yield
      val expectedMean = (15000L + 10000L + 10000L) / 3
      finalState.currentUsage should be(expectedMean)

  atest("No durable task when allocationGroups is empty"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()

      _ <- mgr.updateStoreSize(net.storeId0, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId1, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId2, 10000L)
      _ <- waitForTransactionsToComplete()

      _ <- mgr.updateStoreSize(net.storeId0, 15000L)
      _ <- waitForTransactionsToComplete()

      (_, finalState) <- readPoolState()
    yield
      finalState.allocationGroups should be(Nil)
      val expectedMean = (15000L + 10000L + 10000L) / 3
      finalState.currentUsage should be(expectedMean)
