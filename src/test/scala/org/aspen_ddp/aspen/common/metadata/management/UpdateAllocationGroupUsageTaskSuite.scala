package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, StoragePoolState}
import org.aspen_ddp.aspen.common.util.byte2long
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor

import scala.concurrent.{ExecutionContext, Future}

class UpdateAllocationGroupUsageTaskSuite extends IntegrationTestSuite:

  private def setup(): Future[TaskExecutor] =
    given ExecutionContext = executionContext
    for
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)

      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      kvos0 <- client.read(radicle)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)
    yield executor

  private def readGroupState(groupId: AllocationGroupId): Future[AllocationGroupState] =
    given ExecutionContext = executionContext
    for
      groupPtr <- client.getAllocationGroupPointer(groupId)
      groupDos <- client.read(groupPtr)
    yield AllocationGroupState(groupDos)

  private def readPoolState(): Future[StoragePoolState] =
    given ExecutionContext = executionContext
    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
    yield StoragePoolState(poolKvos)

  atest("updates member usage in single parent group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      agsBefore <- readGroupState(groupId)
      memberBefore = agsBefore.members.find(_.uuid == Radicle.poolId.uuid).get

      newUsage = memberBefore.currentUsage + 50000L
      newMaxSize = memberBefore.maximumSize + 100000L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, newUsage, newMaxSize,
        List(groupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture

      agsAfter <- readGroupState(groupId)
      memberAfter = agsAfter.members.find(_.uuid == Radicle.poolId.uuid).get
    yield
      memberAfter.currentUsage should be(newUsage)
      memberAfter.maximumSize should be(newMaxSize)

  atest("updates member usage across multiple parent groups"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId1 <- client.createAllocationGroup("group-1", level = 0)
      _ <- waitForTransactionsToComplete()
      groupId2 <- client.createAllocationGroup("group-2", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId1, executor)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId2, executor)
      _ <- waitForTransactionsToComplete()

      newUsage = 75000L
      newMaxSize = 200000L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, newUsage, newMaxSize,
        List(groupId1.uuid, groupId2.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture

      ags1 <- readGroupState(groupId1)
      ags2 <- readGroupState(groupId2)
      member1 = ags1.members.find(_.uuid == Radicle.poolId.uuid).get
      member2 = ags2.members.find(_.uuid == Radicle.poolId.uuid).get
    yield
      member1.currentUsage should be(newUsage)
      member1.maximumSize should be(newMaxSize)
      member2.currentUsage should be(newUsage)
      member2.maximumSize should be(newMaxSize)

  atest("skips silently when member not found in group"):
    given ExecutionContext = executionContext
    val unknownChildUUID = java.util.UUID.randomUUID()
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      agsBefore <- readGroupState(groupId)

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        unknownChildUUID, 99999L, 99999L,
        List(groupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      result <- taskFuture

      agsAfter <- readGroupState(groupId)
    yield
      result should be(None)
      agsAfter.members should be(agsBefore.members)

  atest("cascades to grandparent when change exceeds threshold"):
    given ExecutionContext = executionContext
    for
      executor <- setup()

      childGroupId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentGroupId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childGroupId, executor)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childGroupId, parentGroupId, executor)
      _ <- waitForTransactionsToComplete()

      childBefore <- readGroupState(childGroupId)
      poolMember = childBefore.members.find(_.uuid == Radicle.poolId.uuid).get

      newUsage = poolMember.currentUsage + 500000L
      newMaxSize = poolMember.maximumSize + 1000000L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, newUsage, newMaxSize,
        List(childGroupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture
      _ <- waitForTransactionsToComplete()

      childAfter <- readGroupState(childGroupId)
      parentAfter <- readGroupState(parentGroupId)

      childMember = childAfter.members.find(_.uuid == Radicle.poolId.uuid).get
      parentMember = parentAfter.members.find(_.uuid == childGroupId.uuid).get
    yield
      childMember.currentUsage should be(newUsage)
      childMember.maximumSize should be(newMaxSize)
      parentMember.currentUsage should be(childAfter.currentUsage)
      parentMember.maximumSize should be(childAfter.maximumSize)

  atest("does not cascade when change is below threshold"):
    given ExecutionContext = executionContext
    // The cascade threshold is 0.5%. We first establish a large baseline so
    // that a subsequent +1 change falls below the threshold.
    val baselineUsage = 1000000L
    val baselineMaxSize = 2000000L
    for
      executor <- setup()

      childGroupId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentGroupId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childGroupId, executor)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childGroupId, parentGroupId, executor)
      _ <- waitForTransactionsToComplete()

      // Establish a large baseline usage so that later +1 is below threshold
      kvos0 <- client.read(radicle)
      tx0 = client.newTransaction()
      baselineTaskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, baselineUsage, baselineMaxSize,
        List(childGroupId.uuid), executor
      )(using tx0)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()
      _ <- baselineTaskFuture
      _ <- waitForTransactionsToComplete()

      // Now read the parent state after baseline cascade
      parentBefore <- readGroupState(parentGroupId)

      // Apply a tiny change (+1) that should be below the 0.5% threshold
      tinyUsageChange = baselineUsage + 1L
      tinyMaxSizeChange = baselineMaxSize + 1L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, tinyUsageChange, tinyMaxSizeChange,
        List(childGroupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture
      _ <- waitForTransactionsToComplete()

      childAfter <- readGroupState(childGroupId)
      parentAfter <- readGroupState(parentGroupId)

      childMember = childAfter.members.find(_.uuid == Radicle.poolId.uuid).get
      parentMemberBefore = parentBefore.members.find(_.uuid == childGroupId.uuid).get
      parentMemberAfter = parentAfter.members.find(_.uuid == childGroupId.uuid).get
    yield
      childMember.currentUsage should be(tinyUsageChange)
      childMember.maximumSize should be(tinyMaxSizeChange)
      parentMemberAfter.currentUsage should be(parentMemberBefore.currentUsage)
      parentMemberAfter.maximumSize should be(parentMemberBefore.maximumSize)

  atest("task completes when all groups result in skips"):
    given ExecutionContext = executionContext
    val unknownChildUUID = java.util.UUID.randomUUID()
    for
      executor <- setup()
      groupId1 <- client.createAllocationGroup("group-1", level = 0)
      _ <- waitForTransactionsToComplete()
      groupId2 <- client.createAllocationGroup("group-2", level = 0)
      _ <- waitForTransactionsToComplete()

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        unknownChildUUID, 99999L, 99999L,
        List(groupId1.uuid, groupId2.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      result <- taskFuture
    yield
      result should be(None)
