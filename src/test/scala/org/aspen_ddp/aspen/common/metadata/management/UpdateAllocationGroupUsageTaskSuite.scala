package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.common.metadata.IDAPolicy
import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor

import java.util.UUID
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

  private def readGroupRevision(groupId: AllocationGroupId) =
    given ExecutionContext = executionContext
    client.getAllocationGroupPointer(groupId).flatMap(client.read).map(_.revision)

  private def poolMember(ags: AllocationGroupState): AllocationGroupState.Member =
    ags.members.find(_.uuid == Radicle.poolId.uuid).get

  /** Sets the pool's usage and maximum store size, as StoragePoolUsageManager does */
  private def setPoolUsage(usage: Long, maxSize: Long)(using tx: Transaction): Future[Unit] =
    given ExecutionContext = executionContext
    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
    yield
      val ps = StoragePoolState(poolKvos)
      tx.update(poolPtr, None, None,
        List(KeyRevision(StoragePoolState.ConfigKey, poolKvos.contents(StoragePoolState.ConfigKey).revision)),
        List(Insert(StoragePoolState.ConfigKey, ps.copy(currentUsage = usage, maximumStoreSize = maxSize).encode())))

  private def commitPoolUsage(usage: Long, maxSize: Long): Future[Unit] =
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    setPoolUsage(usage, maxSize)(using tx).flatMap(_ => tx.commit()).flatMap(_ => waitForTransactionsToComplete())

  /** Enrolls a task for `child` in a transaction that also runs `change`, then waits for the task to finish */
  private def runTask(executor: TaskExecutor, child: UUID, groups: List[AllocationGroupId])
                     (change: Transaction ?=> Future[Unit]): Future[Option[AnyRef]] =
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    given Transaction = tx
    for
      _ <- change
      kvos <- client.read(radicle)
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(child, groups.map(_.uuid), executor)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      result <- taskFuture
      _ <- waitForTransactionsToComplete()
    yield result

  private def runPoolTask(executor: TaskExecutor, groups: List[AllocationGroupId], usage: Long, maxSize: Long) =
    runTask(executor, Radicle.poolId.uuid, groups)(setPoolUsage(usage, maxSize))

  private def newGroup(name: String, level: Int = 0): Future[AllocationGroupId] =
    given ExecutionContext = executionContext
    client.createAllocationGroup(name, level = level, idaPolicy = IDAPolicy.AllowAll)
      .flatMap(id => waitForTransactionsToComplete().map(_ => id))

  private def addPool(executor: TaskExecutor, groupId: AllocationGroupId): Future[Unit] =
    given ExecutionContext = executionContext
    AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor)).flatMap(_ => waitForTransactionsToComplete())

  // Concurrent tasks for one pool race to rewrite the group. One created before a later pool change can commit last:
  // it must record the pool's usage as it is then, not as it was when the task was created
  atest("a task records the pool's current usage, not the usage when it was created"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- newGroup("test-group")
      _ <- addPool(executor, groupId)

      _ <- runPoolTask(executor, List(groupId), 100000L, 200000L)
      // The pool moves on, and a task enrolled for an earlier change runs only now
      _ <- commitPoolUsage(105000L, 210000L)
      _ <- runTask(executor, Radicle.poolId.uuid, List(groupId))(Future.unit)

      ags <- readGroupState(groupId)
    yield
      poolMember(ags).currentUsage should be(105000L)
      poolMember(ags).maximumSize should be(210000L)

  atest("a task leaves the group unchanged when it already records the pool's usage"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- newGroup("test-group")
      _ <- addPool(executor, groupId)
      _ <- runPoolTask(executor, List(groupId), 75000L, 150000L)
      revisionBefore <- readGroupRevision(groupId)

      result <- runTask(executor, Radicle.poolId.uuid, List(groupId))(Future.unit)
      revisionAfter <- readGroupRevision(groupId)
    yield
      result should be(None)
      revisionAfter should be(revisionBefore)

  atest("updates member usage in single parent group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- newGroup("test-group")
      _ <- addPool(executor, groupId)

      agsBefore <- readGroupState(groupId)
      newUsage = poolMember(agsBefore).currentUsage + 50000L
      newMaxSize = poolMember(agsBefore).maximumSize + 100000L
      _ <- runPoolTask(executor, List(groupId), newUsage, newMaxSize)

      agsAfter <- readGroupState(groupId)
    yield
      poolMember(agsAfter).currentUsage should be(newUsage)
      poolMember(agsAfter).maximumSize should be(newMaxSize)

  atest("updates member usage across multiple parent groups"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId1 <- newGroup("group-1")
      groupId2 <- newGroup("group-2")
      _ <- addPool(executor, groupId1)
      _ <- addPool(executor, groupId2)

      _ <- runPoolTask(executor, List(groupId1, groupId2), 75000L, 200000L)

      ags1 <- readGroupState(groupId1)
      ags2 <- readGroupState(groupId2)
    yield
      poolMember(ags1).currentUsage should be(75000L)
      poolMember(ags1).maximumSize should be(200000L)
      poolMember(ags2).currentUsage should be(75000L)
      poolMember(ags2).maximumSize should be(200000L)

  atest("skips silently when member not found in group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- newGroup("test-group")
      _ <- addPool(executor, groupId)
      agsBefore <- readGroupState(groupId)

      result <- runTask(executor, UUID.randomUUID(), List(groupId))(Future.unit)

      agsAfter <- readGroupState(groupId)
    yield
      result should be(None)
      agsAfter.members should be(agsBefore.members)

  atest("cascades to grandparent when change exceeds threshold"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childGroupId <- newGroup("child-group")
      parentGroupId <- newGroup("parent-group", level = 1)
      _ <- addPool(executor, childGroupId)
      _ <- AllocationGroupState.addGroup(client, childGroupId, parentGroupId, Some(executor))
      _ <- waitForTransactionsToComplete()

      childBefore <- readGroupState(childGroupId)
      newUsage = poolMember(childBefore).currentUsage + 500000L
      newMaxSize = poolMember(childBefore).maximumSize + 1000000L
      _ <- runPoolTask(executor, List(childGroupId), newUsage, newMaxSize)

      childAfter <- readGroupState(childGroupId)
      parentAfter <- readGroupState(parentGroupId)
      parentMember = parentAfter.members.find(_.uuid == childGroupId.uuid).get
    yield
      poolMember(childAfter).currentUsage should be(newUsage)
      poolMember(childAfter).maximumSize should be(newMaxSize)
      parentMember.currentUsage should be(childAfter.currentUsage)
      parentMember.maximumSize should be(childAfter.maximumSize)

  atest("does not cascade when change is below threshold"):
    given ExecutionContext = executionContext
    // The cascade threshold is 0.5%. A large baseline makes a later +1 change fall below it
    val baselineUsage = 1000000L
    val baselineMaxSize = 2000000L
    for
      executor <- setup()
      childGroupId <- newGroup("child-group")
      parentGroupId <- newGroup("parent-group", level = 1)
      _ <- addPool(executor, childGroupId)
      _ <- AllocationGroupState.addGroup(client, childGroupId, parentGroupId, Some(executor))
      _ <- waitForTransactionsToComplete()

      _ <- runPoolTask(executor, List(childGroupId), baselineUsage, baselineMaxSize)
      parentBefore <- readGroupState(parentGroupId)

      _ <- runPoolTask(executor, List(childGroupId), baselineUsage + 1L, baselineMaxSize + 1L)

      childAfter <- readGroupState(childGroupId)
      parentAfter <- readGroupState(parentGroupId)
      parentMemberBefore = parentBefore.members.find(_.uuid == childGroupId.uuid).get
      parentMemberAfter = parentAfter.members.find(_.uuid == childGroupId.uuid).get
    yield
      poolMember(childAfter).currentUsage should be(baselineUsage + 1L)
      poolMember(childAfter).maximumSize should be(baselineMaxSize + 1L)
      parentMemberAfter.currentUsage should be(parentMemberBefore.currentUsage)
      parentMemberAfter.maximumSize should be(parentMemberBefore.maximumSize)

  atest("task completes when all groups result in skips"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId1 <- newGroup("group-1")
      groupId2 <- newGroup("group-2")
      result <- runTask(executor, UUID.randomUUID(), List(groupId1, groupId2))(Future.unit)
    yield
      result should be(None)
