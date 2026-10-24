package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{GroupObjectAllocatorId, ObjectAllocator}
import org.aspen_ddp.aspen.client.internal.ObjectAllocatorManager
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.management.UpdateAllocationGroupUsageTask
import org.aspen_ddp.aspen.common.ida.{ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{Key, KeyAlreadyExists}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.aspen_ddp.aspen.common.util.byte2uuid
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}
import org.aspen_ddp.aspen.compute.{ServiceEntry, TaskExecutor}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

class AllocationGroupStateSuite extends IntegrationTestSuite:

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

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
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

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
      _ <- waitForTransactionsToComplete()

      ags <- readGroupState(groupId)
    yield
      val member = ags.members.find(_.uuid == Radicle.poolId.uuid).get
      member.memberType should be(AllocationGroupState.MemberType.Pool)
      member.maxSliceSize should be(psBefore.maxSliceSize)
      member.maxObjectSize should be(psBefore.maxSliceSize.map(psBefore.ida.maxObjectSize(_)))
      member.currentUsage should be(psBefore.currentUsage)
      member.maximumSize should be(psBefore.maximumStoreSize)

  atest("removePool removes pool from allocation group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.removePool(client, Radicle.poolId, groupId, Some(executor))
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

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
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

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId1, Some(executor))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId2, Some(executor))
      _ <- waitForTransactionsToComplete()

      ps1 <- readPoolState()
      ags1 <- readGroupState(groupId1)
      ags2a <- readGroupState(groupId2)
      _ = ps1.allocationGroups should contain(groupId1.uuid)
      _ = ps1.allocationGroups should contain(groupId2.uuid)
      _ = ags1.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)
      _ = ags2a.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)

      _ <- AllocationGroupState.removePool(client, Radicle.poolId, groupId1, Some(executor))
      _ <- waitForTransactionsToComplete()

      ps2 <- readPoolState()
      ags1b <- readGroupState(groupId1)
      ags2b <- readGroupState(groupId2)
    yield
      ps2.allocationGroups should not contain groupId1.uuid
      ps2.allocationGroups should contain(groupId2.uuid)
      ags1b.members.exists(_.uuid == Radicle.poolId.uuid) should be(false)
      ags2b.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)

  atest("addGroup adds child group to parent group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      child <- readGroupState(childId)
      parent <- readGroupState(parentId)
    yield
      child.parentGroups.exists(_.uuid == parentId.uuid) should be(true)
      parent.members.exists(_.uuid == childId.uuid) should be(true)

  atest("addGroup sets correct member fields"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      child <- readGroupState(childId)
      parent <- readGroupState(parentId)
    yield
      val member = parent.members.find(_.uuid == childId.uuid).get
      member.memberType should be(AllocationGroupState.MemberType.Group)
      member.maxSliceSize should be(child.maxSliceSize)
      member.maxObjectSize should be(child.maximumObjectSize)
      member.currentUsage should be(child.currentUsage)
      member.maximumSize should be(child.maximumSize)

  atest("removeGroup removes child group from parent group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.removeGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      child <- readGroupState(childId)
      parent <- readGroupState(parentId)
    yield
      child.parentGroups.exists(_.uuid == parentId.uuid) should be(false)
      parent.members.exists(_.uuid == childId.uuid) should be(false)

  atest("addGroup is idempotent"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      child <- readGroupState(childId)
      parent <- readGroupState(parentId)
    yield
      child.parentGroups.count(_.uuid == parentId.uuid) should be(1)
      parent.members.count(_.uuid == childId.uuid) should be(1)

  atest("addGroup rejects equal level"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupA <- client.createAllocationGroup("group-a", level = 0)
      _ <- waitForTransactionsToComplete()
      groupB <- client.createAllocationGroup("group-b", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- recoverToSucceededIf[AllocationGroupState.InvalidLevel](
        AllocationGroupState.addGroup(client, groupA, groupB, Some(executor))
      )
    yield succeed

  atest("addGroup rejects lower-level parent"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 1)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- recoverToSucceededIf[AllocationGroupState.InvalidLevel](
        AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      )
    yield succeed

  atest("add and remove group with multiple parents"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId1 <- client.createAllocationGroup("parent-1", level = 1)
      _ <- waitForTransactionsToComplete()
      parentId2 <- client.createAllocationGroup("parent-2", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId1, Some(executor))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childId, parentId2, Some(executor))
      _ <- waitForTransactionsToComplete()

      child1 <- readGroupState(childId)
      parent1a <- readGroupState(parentId1)
      parent2a <- readGroupState(parentId2)
      _ = child1.parentGroups.exists(_.uuid == parentId1.uuid) should be(true)
      _ = child1.parentGroups.exists(_.uuid == parentId2.uuid) should be(true)
      _ = parent1a.members.exists(_.uuid == childId.uuid) should be(true)
      _ = parent2a.members.exists(_.uuid == childId.uuid) should be(true)

      _ <- AllocationGroupState.removeGroup(client, childId, parentId1, Some(executor))
      _ <- waitForTransactionsToComplete()

      child2 <- readGroupState(childId)
      parent1b <- readGroupState(parentId1)
      parent2b <- readGroupState(parentId2)
    yield
      child2.parentGroups.exists(_.uuid == parentId1.uuid) should be(false)
      child2.parentGroups.exists(_.uuid == parentId2.uuid) should be(true)
      parent1b.members.exists(_.uuid == childId.uuid) should be(false)
      parent2b.members.exists(_.uuid == childId.uuid) should be(true)

  atest("addPool no-op re-add does not change membership"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-noop", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-noop", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childId, Some(executor))
      _ <- waitForTransactionsToComplete()

      // Re-add: this is a no-op and must not duplicate membership.
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childId, Some(executor))
      _ <- waitForTransactionsToComplete()

      ps <- readPoolState()
      ags <- readGroupState(childId)
    yield
      ps.allocationGroups.count(_ == childId.uuid) should be(1)
      ags.members.count(_.uuid == Radicle.poolId.uuid) should be(1)

  atest("addPool with no executor enrolls a system usage task"):
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-sys", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-sys", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      // No executor supplied -> system durable task path.
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childId, None)
      _ <- waitForTransactionsToComplete()

      stateVs <- servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(stateVs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      taskKvos <- client.read(enrolled.head._2)

      ps <- readPoolState()
      ags <- readGroupState(childId)
    yield
      ps.allocationGroups should contain(childId.uuid)
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)
      enrolled.size should be >= 1
      byte2uuid(taskKvos.contents(SimpleTaskExecutor.TaskTypeKey).value.bytes) should be(UpdateAllocationGroupUsageTask.typeUUID)

  atest("addPoolToGroup resolves names and adds the pool"):
    given ExecutionContext = executionContext
    for
      groupId <- client.createAllocationGroup("named-group", level = 0)
      _ <- waitForTransactionsToComplete()

      // Radicle bootstrap pool is registered under a known name; resolve it dynamically.
      pools <- client.listStoragePools()
      poolName = pools.find(_._2 == Radicle.poolId).get._1

      _ <- client.addPoolToGroup(poolName, "named-group")
      _ <- waitForTransactionsToComplete()

      ps <- readPoolState()
      ags <- readGroupState(groupId)
    yield
      ps.allocationGroups should contain(groupId.uuid)
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)

  atest("addGroupToGroup resolves names and nests the source group"):
    given ExecutionContext = executionContext
    for
      childId <- client.createAllocationGroup("child-named", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-named", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- client.addGroupToGroup("child-named", "parent-named")
      _ <- waitForTransactionsToComplete()

      childState <- readGroupState(childId)
      parentState <- readGroupState(parentId)
    yield
      childState.parentGroups.exists(_.uuid == parentId.uuid) should be(true)
      parentState.members.exists(_.uuid == childId.uuid) should be(true)

  atest("addGroupToGroup fails with InvalidLevel when source level is not lower"):
    given ExecutionContext = executionContext
    for
      _ <- client.createAllocationGroup("src-same", level = 1)
      _ <- waitForTransactionsToComplete()
      _ <- client.createAllocationGroup("dst-same", level = 1)
      _ <- waitForTransactionsToComplete()

      err <- client.addGroupToGroup("src-same", "dst-same").failed
    yield
      err shouldBe a[AllocationGroupState.InvalidLevel]

  atest("createAllocationGroup fails with KeyAlreadyExists when the name is taken"):
    given ExecutionContext = executionContext
    for
      _      <- client.createAllocationGroup("dup-group", level = 0)
      _      <- waitForTransactionsToComplete()

      before <- client.listAllocationGroups()
      err    <- client.createAllocationGroup("dup-group", level = 1).failed
      _      <- waitForTransactionsToComplete()
      after  <- client.listAllocationGroups()
    yield
      // The type matters beyond the message: createAllocationGroup's onFail matches on it to
      // stop the retry loop, and create-allocation-group's reportError matches on it to print
      // "already exists". It is KeyAlreadyExists because the registration goes through
      // Registry.prepareRegister -- Registry.DuplicateRegistration belongs to the
      // non-transactional register() path.
      err shouldBe a[KeyAlreadyExists]
      // The allocation and the groups-tree insert are already staged when the registration
      // rejects, so this holds because the failed transaction is aborted wholesale.
      after should be(before)

  // ---- maxSliceSize membership rule ----

  private def createPool(name: String, maxSliceSize: Option[Int]): Future[PoolId] =
    client.createNewStoragePool(name, Replication(1, 1), maxSliceSize, RocksDBConfig(),
      StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)

  private def readPool(poolId: PoolId): Future[StoragePoolState] =
    client.getStoragePoolState(poolId)

  /** Adds a pool with `poolLimit` to a fresh group with `groupLimit` and returns the outcome
   *  plus the pool and group states read before and after the attempt. */
  private def tryAddPool(tag: String, poolLimit: Option[Int], groupLimit: Option[Int])
      : Future[(Option[Throwable], StoragePoolState, StoragePoolState, AllocationGroupState, AllocationGroupState)] =
    given ExecutionContext = executionContext
    for
      executor <- setup()
      poolId <- createPool(s"pool-$tag", poolLimit)
      _ <- waitForTransactionsToComplete()
      groupId <- client.createAllocationGroup(s"group-$tag", level = 0, maxSliceSize = groupLimit)
      _ <- waitForTransactionsToComplete()
      psBefore <- readPool(poolId)
      agsBefore <- readGroupState(groupId)
      result <- AllocationGroupState.addPool(client, poolId, groupId, Some(executor))
        .map(_ => None).recover { case e => Some(e) }
      _ <- waitForTransactionsToComplete()
      psAfter <- readPool(poolId)
      agsAfter <- readGroupState(groupId)
    yield (result, psBefore, psAfter, agsBefore, agsAfter)

  private def tryAddGroup(tag: String, childLimit: Option[Int], parentLimit: Option[Int])
      : Future[(Option[Throwable], AllocationGroupState, AllocationGroupState, AllocationGroupState, AllocationGroupState)] =
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup(s"child-$tag", level = 0, maxSliceSize = childLimit)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup(s"parent-$tag", level = 1, maxSliceSize = parentLimit)
      _ <- waitForTransactionsToComplete()
      childBefore <- readGroupState(childId)
      parentBefore <- readGroupState(parentId)
      result <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
        .map(_ => None).recover { case e => Some(e) }
      _ <- waitForTransactionsToComplete()
      childAfter <- readGroupState(childId)
      parentAfter <- readGroupState(parentId)
    yield (result, childBefore, childAfter, parentBefore, parentAfter)

  atest("addPool rejects a pool whose limit exceeds the group's and changes nothing"):
    given ExecutionContext = executionContext
    tryAddPool("too-big", Some(2000), Some(1000)).map: (result, psBefore, psAfter, agsBefore, agsAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitExceedsGroup]
      val err = result.get.asInstanceOf[AllocationGroupState.MemberLimitExceedsGroup]
      err.memberId should be(psBefore.poolId.uuid)
      err.memberLimit should be(Some(2000))
      err.groupLimit should be(1000)
      psAfter should be(psBefore)
      agsAfter should be(agsBefore)

  atest("addPool accepts a pool whose limit equals the group's"):
    given ExecutionContext = executionContext
    tryAddPool("equal", Some(1000), Some(1000)).map: (result, psBefore, _, _, agsAfter) =>
      result should be(None)
      agsAfter.members.exists(_.uuid == psBefore.poolId.uuid) should be(true)

  atest("addPool accepts a pool whose limit is below the group's"):
    given ExecutionContext = executionContext
    tryAddPool("smaller", Some(1000), Some(2000)).map: (result, psBefore, _, _, agsAfter) =>
      result should be(None)
      val m = agsAfter.members.find(_.uuid == psBefore.poolId.uuid).get
      m.maxSliceSize should be(Some(1000))
      m.maxObjectSize should be(Some(1000))
      agsAfter.maximumObjectSize should be(Some(1000))

  atest("addPool rejects an unlimited pool from a limited group"):
    given ExecutionContext = executionContext
    tryAddPool("unlimited-pool", None, Some(1000)).map: (result, psBefore, psAfter, agsBefore, agsAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitExceedsGroup]
      result.get.asInstanceOf[AllocationGroupState.MemberLimitExceedsGroup].memberLimit should be(None)
      psAfter should be(psBefore)
      agsAfter should be(agsBefore)

  atest("addPool accepts any pool into an unlimited group"):
    given ExecutionContext = executionContext
    for
      (r1, ps1, _, _, ags1) <- tryAddPool("any-limited", Some(5000), None)
      (r2, ps2, _, _, ags2) <- tryAddPool("any-unlimited", None, None)
    yield
      r1 should be(None)
      r2 should be(None)
      ags1.members.exists(_.uuid == ps1.poolId.uuid) should be(true)
      ags2.members.exists(_.uuid == ps2.poolId.uuid) should be(true)

  atest("addGroup rejects a child whose limit exceeds the parent's and changes nothing"):
    given ExecutionContext = executionContext
    tryAddGroup("too-big", Some(2000), Some(1000)).map: (result, childBefore, childAfter, parentBefore, parentAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitExceedsGroup]
      val err = result.get.asInstanceOf[AllocationGroupState.MemberLimitExceedsGroup]
      err.memberId should be(childBefore.groupId.uuid)
      err.memberLimit should be(Some(2000))
      err.groupLimit should be(1000)
      childAfter should be(childBefore)
      parentAfter should be(parentBefore)

  atest("addGroup accepts a child whose limit equals the parent's"):
    given ExecutionContext = executionContext
    tryAddGroup("equal", Some(1000), Some(1000)).map: (result, childBefore, _, _, parentAfter) =>
      result should be(None)
      parentAfter.members.exists(_.uuid == childBefore.groupId.uuid) should be(true)

  atest("addGroup accepts a child whose limit is below the parent's"):
    given ExecutionContext = executionContext
    tryAddGroup("smaller", Some(1000), Some(2000)).map: (result, childBefore, _, _, parentAfter) =>
      result should be(None)
      val m = parentAfter.members.find(_.uuid == childBefore.groupId.uuid).get
      m.maxSliceSize should be(Some(1000))
      parentAfter.members.exists(_.uuid == childBefore.groupId.uuid) should be(true)

  atest("addGroup rejects an unlimited child from a limited parent"):
    given ExecutionContext = executionContext
    tryAddGroup("unlimited-child", None, Some(1000)).map: (result, childBefore, childAfter, parentBefore, parentAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitExceedsGroup]
      result.get.asInstanceOf[AllocationGroupState.MemberLimitExceedsGroup].memberLimit should be(None)
      childAfter should be(childBefore)
      parentAfter should be(parentBefore)

  atest("addGroup accepts any child into an unlimited parent"):
    given ExecutionContext = executionContext
    for
      (r1, c1, _, _, p1) <- tryAddGroup("any-limited", Some(5000), None)
      (r2, c2, _, _, p2) <- tryAddGroup("any-unlimited", None, None)
    yield
      r1 should be(None)
      r2 should be(None)
      p1.members.exists(_.uuid == c1.groupId.uuid) should be(true)
      p2.members.exists(_.uuid == c2.groupId.uuid) should be(true)

  atest("createAllocationGroup records the declared maxSliceSize"):
    given ExecutionContext = executionContext
    for
      groupId <- client.createAllocationGroup("declared-limit", level = 0, maxSliceSize = Some(1000))
      _ <- waitForTransactionsToComplete()
      ags <- readGroupState(groupId)
    yield
      ags.maxSliceSize should be(Some(1000))

  test("a limited group with no members has no maximumObjectSize"):
    val ags = AllocationGroupState(AllocationGroupId(java.util.UUID.randomUUID()), 0, "empty-limited",
      Nil, Nil, maxSliceSize = Some(1000))
    ags.maximumObjectSize should be(None)

  test("Member.forPool derives maxObjectSize from the pool's IDA"):
    val ps = StoragePoolState(PoolId(java.util.UUID.randomUUID()), "rs-pool", ReedSolomon(14, 10, 12),
      Some(1000), Array.empty, RocksDBConfig(), StorageDeviceSetId.BootstrapStorageDeviceSetId)
    val m = AllocationGroupState.Member.forPool(ps)
    m.memberType should be(AllocationGroupState.MemberType.Pool)
    m.maxSliceSize should be(Some(1000))
    m.maxObjectSize should be(Some(9999))

  test("StoragePoolState.effectiveMaxSliceSize is the smaller of the pool and backend limits"):
    def ps(pool: Option[Int], backend: Option[Int]) =
      StoragePoolState(PoolId(java.util.UUID.randomUUID()), "p", ReedSolomon(5, 3, 4), pool, Array.empty,
        RocksDBConfig(backend), StorageDeviceSetId.BootstrapStorageDeviceSetId)
    ps(Some(2000), Some(1000)).effectiveMaxSliceSize should be(Some(1000))
    ps(Some(1000), Some(2000)).effectiveMaxSliceSize should be(Some(1000))
    ps(None, Some(1000)).effectiveMaxSliceSize should be(Some(1000))
    ps(Some(1000), None).effectiveMaxSliceSize should be(Some(1000))
    ps(None, None).effectiveMaxSliceSize should be(None)
    ps(Some(2000), Some(1000)).maxObjectSize should be(Some(ReedSolomon(5, 3, 4).maxObjectSize(1000)))
    ps(None, None).maxObjectSize should be(None)

  test("Member.forPool uses the pool's effective limit, including the backend's"):
    val ps = StoragePoolState(PoolId(java.util.UUID.randomUUID()), "rs-pool", ReedSolomon(14, 10, 12),
      Some(2000), Array.empty, RocksDBConfig(Some(1000)), StorageDeviceSetId.BootstrapStorageDeviceSetId)
    val m = AllocationGroupState.Member.forPool(ps)
    m.maxSliceSize should be(Some(1000))
    m.maxObjectSize should be(Some(9999))

  atest("addPool applies the membership rule to the pool's effective limit"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      poolId <- client.createNewStoragePool("backend-limited-member", Replication(1, 1), Some(2000),
        RocksDBConfig(Some(1000)), StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      _ <- waitForTransactionsToComplete()
      groupId <- client.createAllocationGroup("effective-limit-group", level = 0, maxSliceSize = Some(1000))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, poolId, groupId, Some(executor))
      _ <- waitForTransactionsToComplete()
      ags <- readGroupState(groupId)
    yield
      val m = ags.members.find(_.uuid == poolId.uuid).get
      m.maxSliceSize should be(Some(1000))
      m.maxObjectSize should be(Some(1000))

  // ---- group allocators compute maxObjectSize from live child group states ----

  /** A level-1 parent holding an empty, unlimited level-0 child, then a 1000-byte pool added to the child */
  private type BeforePoolAdded = (AllocationGroupId, AllocationGroupId) => Future[Unit]

  private def nestedGroupWithLatePool(tag: String, beforePoolAdded: BeforePoolAdded = (_, _) => Future.unit)
      : Future[(AllocationGroupId, AllocationGroupId)] =
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup(s"child-$tag", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup(s"parent-$tag", level = 1)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()
      _ <- beforePoolAdded(childId, parentId)
      poolId <- createPool(s"pool-$tag", Some(1000))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, poolId, childId, Some(executor))
      _ <- waitForTransactionsToComplete()
    yield (childId, parentId)

  atest("a parent group's maxObjectSize reflects a pool added to its child after the child joined"):
    given ExecutionContext = executionContext
    for
      (_, parentId) <- nestedGroupWithLatePool("late-pool")
      parent <- readGroupState(parentId)
      allocator <- client.getAllocator(GroupObjectAllocatorId(parentId))
    yield
      // The child's member entry in the parent is the snapshot taken when it joined, empty and unlimited
      parent.maximumObjectSize should be(None)
      allocator.maxObjectSize should be(Some(1000))

  atest("a cached parent group allocator picks up a pool added to its child once the child's state refreshes"):
    given ExecutionContext = executionContext
    // Refresh cached group states on every use so the test need not wait out the default interval
    val manager = new ObjectAllocatorManager(client, Duration.Zero)
    var allocator: Option[ObjectAllocator] = None

    def cacheParent(childId: AllocationGroupId, parentId: AllocationGroupId): Future[Unit] =
      manager.getAllocator(GroupObjectAllocatorId(parentId)).map: a =>
        allocator = Some(a)
        a.maxObjectSize should be(None)
        ()

    def awaitLimit(remaining: Int): Future[Option[Int]] =
      val current = allocator.get.maxObjectSize // also starts a background refresh of the stale cached states
      if current.isDefined || remaining == 0 then
        Future.successful(current)
      else
        Future(Thread.sleep(20))(using ExecutionContext.global).flatMap(_ => awaitLimit(remaining - 1))

    for
      _ <- nestedGroupWithLatePool("cached", cacheParent)
      limit <- awaitLimit(500)
    yield
      limit should be(Some(1000))

  test("maximumObjectSize is the smallest of the members' maxObjectSize"):
    import AllocationGroupState.{Member, MemberType}
    val ags = AllocationGroupState(AllocationGroupId(java.util.UUID.randomUUID()), 0, "mixed",
      List(
        Member(MemberType.Pool, java.util.UUID.randomUUID(), None, None, 0L, 0L),
        Member(MemberType.Pool, java.util.UUID.randomUUID(), Some(1000), Some(9999), 0L, 0L),
        Member(MemberType.Pool, java.util.UUID.randomUUID(), Some(500), Some(4999), 0L, 0L)),
      Nil)
    ags.maximumObjectSize should be(Some(4999))
