package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.TaskExecutor

import scala.concurrent.{ExecutionContext, Future}

class AllocationGroupStateSuite extends IntegrationTestSuite:

  private def setup(): Future[TaskExecutor] =
    given ExecutionContext = executionContext
    for
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new SinglePoolObjectAllocator(client, pool, pool.ida, None)

      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      kvos0 <- client.read(radicle)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)
    yield executor

  private def readPoolState(): Future[StoragePoolState] =
    given ExecutionContext = executionContext
    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
    yield StoragePoolState(poolKvos)

  private def readGroupState(groupId: AllocationGroupId): Future[AllocationGroupState] =
    given ExecutionContext = executionContext
    for
      groupPtr <- client.getAllocationGroupPointer(groupId)
      groupDos <- client.read(groupPtr)
    yield AllocationGroupState(groupDos)

  atest("addPool adds pool to allocation group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      ps <- readPoolState()
      ags <- readGroupState(groupId)
    yield
      ps.allocationGroups should contain(groupId.uuid)
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)

  atest("addPool sets correct member fields"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      psBefore <- readPoolState()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      ags <- readGroupState(groupId)
    yield
      val member = ags.members.find(_.uuid == Radicle.poolId.uuid).get
      member.memberType should be(AllocationGroupState.MemberType.Pool)
      member.maxObjectSize should be(psBefore.maxObjectSize)
      member.currentUsage should be(psBefore.currentUsage)
      member.maximumSize should be(psBefore.maximumStoreSize)

  atest("removePool removes pool from allocation group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.removePool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      ps <- readPoolState()
      ags <- readGroupState(groupId)
    yield
      ps.allocationGroups should not contain groupId.uuid
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(false)

  atest("addPool is idempotent"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      ps <- readPoolState()
      ags <- readGroupState(groupId)
    yield
      ps.allocationGroups.count(_ == groupId.uuid) should be(1)
      ags.members.count(_.uuid == Radicle.poolId.uuid) should be(1)

  atest("add and remove with multiple groups"):
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

      ps1 <- readPoolState()
      ags1 <- readGroupState(groupId1)
      ags2a <- readGroupState(groupId2)
      _ = ps1.allocationGroups should contain(groupId1.uuid)
      _ = ps1.allocationGroups should contain(groupId2.uuid)
      _ = ags1.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)
      _ = ags2a.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)

      _ <- AllocationGroupState.removePool(client, Radicle.poolId, groupId1, executor)
      _ <- waitForTransactionsToComplete()

      ps2 <- readPoolState()
      ags1b <- readGroupState(groupId1)
      ags2b <- readGroupState(groupId2)
    yield
      ps2.allocationGroups should not contain groupId1.uuid
      ps2.allocationGroups should contain(groupId2.uuid)
      ags1b.members.exists(_.uuid == Radicle.poolId.uuid) should be(false)
      ags2b.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)
