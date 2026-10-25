package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.GroupObjectAllocatorId
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.management.UpdateAllocationGroupUsageTask
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{Key, KeyAlreadyExists}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.aspen_ddp.aspen.common.util.byte2uuid
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}
import org.aspen_ddp.aspen.compute.{ServiceEntry, TaskExecutor}

import scala.concurrent.{ExecutionContext, Future}

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
      groupId <- client.createAllocationGroup("test-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
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
      groupId <- client.createAllocationGroup("test-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()

      psBefore <- readPoolState()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
      _ <- waitForTransactionsToComplete()

      ags <- readGroupState(groupId)
    yield
      val member = ags.members.find(_.uuid == Radicle.poolId.uuid).get
      member.memberType should be(AllocationGroupState.MemberType.Pool)
      member.currentUsage should be(psBefore.currentUsage)
      member.maximumSize should be(psBefore.maximumStoreSize)

  atest("removePool removes pool from allocation group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
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
      groupId <- client.createAllocationGroup("test-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
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
      groupId1 <- client.createAllocationGroup("group-1", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      groupId2 <- client.createAllocationGroup("group-2", level = 0, idaPolicy = IDAPolicy.AllowAll)
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
      childId <- client.createAllocationGroup("child-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1, idaPolicy = IDAPolicy.AllowAll)
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
      childId <- client.createAllocationGroup("child-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      child <- readGroupState(childId)
      parent <- readGroupState(parentId)
    yield
      val member = parent.members.find(_.uuid == childId.uuid).get
      member.memberType should be(AllocationGroupState.MemberType.Group)
      member.currentUsage should be(child.currentUsage)
      member.maximumSize should be(child.maximumSize)

  atest("removeGroup removes child group from parent group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1, idaPolicy = IDAPolicy.AllowAll)
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
      childId <- client.createAllocationGroup("child-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 1, idaPolicy = IDAPolicy.AllowAll)
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
      groupA <- client.createAllocationGroup("group-a", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      groupB <- client.createAllocationGroup("group-b", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()

      _ <- recoverToSucceededIf[AllocationGroupState.InvalidLevel](
        AllocationGroupState.addGroup(client, groupA, groupB, Some(executor))
      )
    yield succeed

  atest("addGroup rejects lower-level parent"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 1, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()

      _ <- recoverToSucceededIf[AllocationGroupState.InvalidLevel](
        AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      )
    yield succeed

  atest("add and remove group with multiple parents"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId1 <- client.createAllocationGroup("parent-1", level = 1, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId2 <- client.createAllocationGroup("parent-2", level = 1, idaPolicy = IDAPolicy.AllowAll)
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
      childId <- client.createAllocationGroup("child-noop", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-noop", level = 1, idaPolicy = IDAPolicy.AllowAll)
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
      childId <- client.createAllocationGroup("child-sys", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-sys", level = 1, idaPolicy = IDAPolicy.AllowAll)
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
      groupId <- client.createAllocationGroup("named-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
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
      childId <- client.createAllocationGroup("child-named", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-named", level = 1, idaPolicy = IDAPolicy.AllowAll)
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
      _ <- client.createAllocationGroup("src-same", level = 1, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      _ <- client.createAllocationGroup("dst-same", level = 1, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()

      err <- client.addGroupToGroup("src-same", "dst-same").failed
    yield
      err shouldBe a[AllocationGroupState.InvalidLevel]

  atest("createAllocationGroup fails with KeyAlreadyExists when the name is taken"):
    given ExecutionContext = executionContext
    for
      _      <- client.createAllocationGroup("dup-group", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _      <- waitForTransactionsToComplete()

      before <- client.listAllocationGroups()
      err    <- client.createAllocationGroup("dup-group", level = 1, idaPolicy = IDAPolicy.AllowAll).failed
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

  /** The IDA of every pool and limited group in these tests. Limited groups must be ExactMatch. */
  private val limitIda: IDA = Replication(1, 1)

  private def createPool(name: String, maxSliceSize: Option[Int]): Future[PoolId] =
    client.createNewStoragePool(name, limitIda, maxSliceSize, RocksDBConfig(),
      StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)

  private def createGroup(name: String, level: Int, maxSliceSize: Option[Int]): Future[AllocationGroupId] =
    client.createAllocationGroup(name, level, maxSliceSize, IDAPolicy.ExactMatch, Some(limitIda))

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
      groupId <- createGroup(s"group-$tag", level = 0, groupLimit)
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
      childId <- createGroup(s"child-$tag", level = 0, childLimit)
      _ <- waitForTransactionsToComplete()
      parentId <- createGroup(s"parent-$tag", level = 1, parentLimit)
      _ <- waitForTransactionsToComplete()
      childBefore <- readGroupState(childId)
      parentBefore <- readGroupState(parentId)
      result <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
        .map(_ => None).recover { case e => Some(e) }
      _ <- waitForTransactionsToComplete()
      childAfter <- readGroupState(childId)
      parentAfter <- readGroupState(parentId)
    yield (result, childBefore, childAfter, parentBefore, parentAfter)

  atest("addPool rejects a pool whose limit is below the group's and changes nothing"):
    given ExecutionContext = executionContext
    tryAddPool("too-small", Some(1000), Some(2000)).map: (result, psBefore, psAfter, agsBefore, agsAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitIncompatible]
      val err = result.get.asInstanceOf[AllocationGroupState.MemberLimitIncompatible]
      err.memberId should be(psBefore.poolId.uuid)
      err.memberLimit should be(Some(1000))
      err.groupLimit should be(Some(2000))
      psAfter should be(psBefore)
      agsAfter should be(agsBefore)

  atest("addPool accepts a pool whose limit equals the group's"):
    given ExecutionContext = executionContext
    tryAddPool("equal", Some(1000), Some(1000)).map: (result, psBefore, _, _, agsAfter) =>
      result should be(None)
      agsAfter.members.exists(_.uuid == psBefore.poolId.uuid) should be(true)

  atest("addPool accepts a pool whose limit is above the group's"):
    given ExecutionContext = executionContext
    tryAddPool("larger", Some(2000), Some(1000)).map: (result, psBefore, _, _, agsAfter) =>
      result should be(None)
      agsAfter.members.exists(_.uuid == psBefore.poolId.uuid) should be(true)
      agsAfter.maxObjectSize should be(Some(limitIda.maxObjectSize(1000)))

  atest("addPool accepts an unlimited pool into a limited group"):
    given ExecutionContext = executionContext
    tryAddPool("unlimited-pool", None, Some(1000)).map: (result, psBefore, _, _, agsAfter) =>
      result should be(None)
      agsAfter.members.exists(_.uuid == psBefore.poolId.uuid) should be(true)

  atest("addPool rejects a limited pool from an unlimited group and changes nothing"):
    given ExecutionContext = executionContext
    tryAddPool("limited-pool", Some(1000), None).map: (result, psBefore, psAfter, agsBefore, agsAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitIncompatible]
      val err = result.get.asInstanceOf[AllocationGroupState.MemberLimitIncompatible]
      err.memberLimit should be(Some(1000))
      err.groupLimit should be(None)
      psAfter should be(psBefore)
      agsAfter should be(agsBefore)

  atest("addPool accepts an unlimited pool into an unlimited group"):
    given ExecutionContext = executionContext
    tryAddPool("both-unlimited", None, None).map: (result, psBefore, _, _, agsAfter) =>
      result should be(None)
      agsAfter.members.exists(_.uuid == psBefore.poolId.uuid) should be(true)

  atest("addGroup rejects a child whose limit is below the parent's and changes nothing"):
    given ExecutionContext = executionContext
    tryAddGroup("too-small", Some(1000), Some(2000)).map: (result, childBefore, childAfter, parentBefore, parentAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitIncompatible]
      val err = result.get.asInstanceOf[AllocationGroupState.MemberLimitIncompatible]
      err.memberId should be(childBefore.groupId.uuid)
      err.memberLimit should be(Some(1000))
      err.groupLimit should be(Some(2000))
      childAfter should be(childBefore)
      parentAfter should be(parentBefore)

  atest("addGroup accepts a child whose limit equals the parent's"):
    given ExecutionContext = executionContext
    tryAddGroup("equal", Some(1000), Some(1000)).map: (result, childBefore, _, _, parentAfter) =>
      result should be(None)
      parentAfter.members.exists(_.uuid == childBefore.groupId.uuid) should be(true)

  atest("addGroup accepts a child whose limit is above the parent's"):
    given ExecutionContext = executionContext
    tryAddGroup("larger", Some(2000), Some(1000)).map: (result, childBefore, _, _, parentAfter) =>
      result should be(None)
      parentAfter.members.exists(_.uuid == childBefore.groupId.uuid) should be(true)

  atest("addGroup accepts an unlimited child into a limited parent"):
    given ExecutionContext = executionContext
    tryAddGroup("unlimited-child", None, Some(1000)).map: (result, childBefore, _, _, parentAfter) =>
      result should be(None)
      parentAfter.members.exists(_.uuid == childBefore.groupId.uuid) should be(true)

  atest("addGroup rejects a limited child from an unlimited parent and changes nothing"):
    given ExecutionContext = executionContext
    tryAddGroup("limited-child", Some(1000), None).map: (result, childBefore, childAfter, parentBefore, parentAfter) =>
      result.get shouldBe a[AllocationGroupState.MemberLimitIncompatible]
      val err = result.get.asInstanceOf[AllocationGroupState.MemberLimitIncompatible]
      err.memberLimit should be(Some(1000))
      err.groupLimit should be(None)
      childAfter should be(childBefore)
      parentAfter should be(parentBefore)

  atest("addGroup accepts an unlimited child into an unlimited parent"):
    given ExecutionContext = executionContext
    tryAddGroup("both-unlimited", None, None).map: (result, childBefore, _, _, parentAfter) =>
      result should be(None)
      parentAfter.members.exists(_.uuid == childBefore.groupId.uuid) should be(true)

  atest("createAllocationGroup records the declared maxSliceSize"):
    given ExecutionContext = executionContext
    for
      groupId <- createGroup("declared-limit", level = 0, Some(1000))
      _ <- waitForTransactionsToComplete()
      ags <- readGroupState(groupId)
    yield
      ags.maxSliceSize should be(Some(1000))

  atest("createAllocationGroup AllowAll with a maxSliceSize fails"):
    intercept[IllegalArgumentException]:
      client.createAllocationGroup("g", level = 0, maxSliceSize = Some(1000), idaPolicy = IDAPolicy.AllowAll)
    succeed

  atest("createAllocationGroup with a maxSliceSize below the IDA's minSliceSize fails"):
    val ida = RandomAccessReedSolomon(5, 3, 4, 4096)
    intercept[IllegalArgumentException]:
      client.createAllocationGroup("g", level = 0, maxSliceSize = Some(ida.minSliceSize - 1), ida = Some(ida))
    succeed

  atest("createAllocationGroup rejects a non-positive maxSliceSize"):
    intercept[IllegalArgumentException]:
      client.createAllocationGroup("g", level = 0, maxSliceSize = Some(0), ida = Some(limitIda))
    succeed

  test("a group's maxObjectSize is derived from its own maxSliceSize and IDA, not its members"):
    import AllocationGroupState.{Member, MemberType}
    val ida = ReedSolomon(14, 10, 12)
    val members = List(Member(MemberType.Pool, java.util.UUID.randomUUID(), 0L, 0L))
    def ags(limit: Option[Int]) = AllocationGroupState(AllocationGroupId(java.util.UUID.randomUUID()), 0, "g",
      members, Nil, maxSliceSize = limit, idaPolicy = IDAPolicy.ExactMatch, ida = Some(ida))
    ags(Some(1000)).maxObjectSize should be(Some(ida.maxObjectSize(1000)))
    ags(Some(1000)).maxObjectSize should be(Some(9999))
    ags(None).maxObjectSize should be(None)

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

  atest("addPool applies the membership rule to the pool's effective limit"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      // The pool's own limit meets the group's but its backend's does not
      poolId <- client.createNewStoragePool("backend-limited-member", limitIda, Some(2000),
        RocksDBConfig(Some(1000)), StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      _ <- waitForTransactionsToComplete()
      groupId <- createGroup("effective-limit-group", level = 0, Some(1500))
      _ <- waitForTransactionsToComplete()
      result <- AllocationGroupState.addPool(client, poolId, groupId, Some(executor))
        .map(_ => None).recover { case e => Some(e) }
      _ <- waitForTransactionsToComplete()
      ags <- readGroupState(groupId)
    yield
      result.get shouldBe a[AllocationGroupState.MemberLimitIncompatible]
      result.get.asInstanceOf[AllocationGroupState.MemberLimitIncompatible].memberLimit should be(Some(1000))
      ags.members should be(Nil)

  atest("a group allocator's maxObjectSize is the group's own, even when every member allows more"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- createGroup("child-own-limit", level = 0, Some(2000))
      _ <- waitForTransactionsToComplete()
      parentId <- createGroup("parent-own-limit", level = 1, Some(1000))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()
      poolId <- createPool("pool-own-limit", Some(4000))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, poolId, childId, Some(executor))
      _ <- waitForTransactionsToComplete()
      parent <- client.getAllocator(GroupObjectAllocatorId(parentId))
      child <- client.getAllocator(GroupObjectAllocatorId(childId))
    yield
      parent.maxObjectSize should be(Some(limitIda.maxObjectSize(1000)))
      child.maxObjectSize should be(Some(limitIda.maxObjectSize(2000)))

  // ---- IDA policy ----

  /** An IDA guaranteed to differ from `ida` in type, so it can never be equal to the bootstrap pool's */
  private def differentIda(ida: IDA): IDA = ida match
    case _: Replication => ReedSolomon(5, 4, 4)
    case _ => Replication(3, 2)

  private def expectMismatch(f: Future[Unit]): Future[org.scalatest.Assertion] =
    given ExecutionContext = executionContext
    f.map(_ => fail("expected MemberIDAMismatch")).recover:
      case _: AllocationGroupState.MemberIDAMismatch => succeed

  atest("createAllocationGroup ExactMatch without an ida fails"):
    intercept[IllegalArgumentException]:
      client.createAllocationGroup("g", level = 0)
    succeed

  atest("createAllocationGroup AllowAll with an ida fails"):
    intercept[IllegalArgumentException]:
      client.createAllocationGroup("g", level = 0, idaPolicy = IDAPolicy.AllowAll, ida = Some(Replication(3, 2)))
    succeed

  atest("ExactMatch group persists its policy and ida"):
    given ExecutionContext = executionContext
    val ida = RandomAccessReedSolomon(5, 3, 4, 4096)
    for
      groupId <- client.createAllocationGroup("exact", level = 0, ida = Some(ida))
      _ <- waitForTransactionsToComplete()
      ags <- readGroupState(groupId)
    yield
      ags.idaPolicy should be(IDAPolicy.ExactMatch)
      ags.ida should be(Some(ida))

  atest("addPool accepts a pool whose ida equals the group's"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      ps <- readPoolState()
      groupId <- client.createAllocationGroup("exact", level = 0, ida = Some(ps.ida))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
      _ <- waitForTransactionsToComplete()
      ags <- readGroupState(groupId)
    yield
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)

  atest("addPool rejects a pool whose ida differs and leaves state unchanged"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      psBefore <- readPoolState()
      groupId <- client.createAllocationGroup("exact", level = 0, ida = Some(differentIda(psBefore.ida)))
      _ <- waitForTransactionsToComplete()
      _ <- expectMismatch(AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor)))
      _ <- waitForTransactionsToComplete()
      psAfter <- readPoolState()
      ags <- readGroupState(groupId)
    yield
      psAfter.allocationGroups should not contain groupId.uuid
      ags.members should be(Nil)

  atest("addPool rejects a pool whose ida has the same type but different parameters"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      ps <- readPoolState()
      near = ps.ida match
        case r: Replication => Replication(r.width + 2, r.writeThreshold + 1)
        case r: ReedSolomon => ReedSolomon(r.width + 2, r.restoreThreshold + 1, r.writeThreshold + 1)
        case r: RandomAccessReedSolomon => r.copy(cellSize = r.cellSize * 2)
      groupId <- client.createAllocationGroup("exact", level = 0, ida = Some(near))
      _ <- waitForTransactionsToComplete()
      _ <- expectMismatch(AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor)))
    yield succeed

  atest("addPool to an AllowAll group accepts any ida"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("any", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, Some(executor))
      _ <- waitForTransactionsToComplete()
      ags <- readGroupState(groupId)
    yield
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)

  atest("addGroup accepts a child with an equal ida"):
    given ExecutionContext = executionContext
    val ida = Replication(3, 2)
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child", level = 0, ida = Some(ida))
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent", level = 1, ida = Some(Replication(3, 2)))
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()
      parent <- readGroupState(parentId)
    yield
      parent.members.exists(_.uuid == childId.uuid) should be(true)

  atest("addGroup rejects a child with a different ida"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child", level = 0, ida = Some(Replication(3, 2)))
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent", level = 1, ida = Some(ReedSolomon(5, 4, 4)))
      _ <- waitForTransactionsToComplete()
      _ <- expectMismatch(AllocationGroupState.addGroup(client, childId, parentId, Some(executor)))
      _ <- waitForTransactionsToComplete()
      child <- readGroupState(childId)
      parent <- readGroupState(parentId)
    yield
      child.parentGroups should be(Nil)
      parent.members should be(Nil)

  atest("addGroup rejects an AllowAll child in an ExactMatch parent"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child", level = 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent", level = 1, ida = Some(Replication(3, 2)))
      _ <- waitForTransactionsToComplete()
      _ <- expectMismatch(AllocationGroupState.addGroup(client, childId, parentId, Some(executor)))
    yield succeed

  atest("addGroup accepts an ExactMatch child in an AllowAll parent"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child", level = 0, ida = Some(Replication(3, 2)))
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent", level = 1, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()
      parent <- readGroupState(parentId)
    yield
      parent.members.exists(_.uuid == childId.uuid) should be(true)
