package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AllocationError, GroupObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.{Key, Value}
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor

import scala.concurrent.{ExecutionContext, Future}

class GroupObjectAllocatorSuite extends IntegrationTestSuite:

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

  atest("allocation from empty group fails with AllocationError for DataObject"):
    given ExecutionContext = executionContext
    for
      groupId <- client.createAllocationGroup("empty-group", level = 0)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(groupId))

      _ <- recoverToSucceededIf[AllocationError]:
        given tx: Transaction = client.newTransaction()
        allocator.allocateDataObject()
    yield succeed

  atest("allocation from empty group fails with AllocationError for KeyValueObject"):
    given ExecutionContext = executionContext
    for
      groupId <- client.createAllocationGroup("empty-group", level = 0)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(groupId))

      _ <- recoverToSucceededIf[AllocationError]:
        given tx: Transaction = client.newTransaction()
        allocator.allocateKeyValueObject()
    yield succeed

  atest("successful DataObject allocation with bootstrap pool in group"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("pool-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(groupId))

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](1, 2, 3)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      dos <- client.read(ptr)
    yield
      ptr.poolId should be(Radicle.poolId)
      dos.data.size should be(3)

  atest("successful KeyValueObject allocation with bootstrap pool in group"):
    given ExecutionContext = executionContext
    val key = Key(Array[Byte](42))
    val value = Value(Array[Byte](7, 8, 9))
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("pool-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(groupId))

      tx = client.newTransaction()
      ptr <- allocator.allocateKeyValueObject(Map(key -> value))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      kvos <- client.read(ptr)
    yield
      ptr.poolId should be(Radicle.poolId)
      kvos.contents.contains(key) should be(true)
      kvos.contents(key).value.bytes should be(value.bytes)

  atest("successful allocation from two-tier group hierarchy"):
    given ExecutionContext = executionContext
    for
      executor <- setup()

      group0Id <- client.createAllocationGroup("group0", level = 0)
      _ <- waitForTransactionsToComplete()
      group1Id <- client.createAllocationGroup("group1", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, group0Id, executor)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, group0Id, group1Id, executor)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(group1Id))

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](5, 6)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      dos <- client.read(ptr)
    yield
      ptr.poolId should be(Radicle.poolId)
      dos.data.size should be(2)

  atest("successful allocation from three-tier group hierarchy"):
    given ExecutionContext = executionContext
    for
      executor <- setup()

      group0Id <- client.createAllocationGroup("group0", level = 0)
      _ <- waitForTransactionsToComplete()
      group1Id <- client.createAllocationGroup("group1", level = 1)
      _ <- waitForTransactionsToComplete()
      group2Id <- client.createAllocationGroup("group2", level = 2)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, group0Id, executor)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, group0Id, group1Id, executor)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, group1Id, group2Id, executor)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(group2Id))

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](9)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      dos <- client.read(ptr)
    yield
      ptr.poolId should be(Radicle.poolId)
      dos.data.size should be(1)

  atest("multiple allocations from same group produce distinct objects"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("multi-alloc-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(groupId))

      tx1 = client.newTransaction()
      ptr1 <- allocator.allocateDataObject(DataBuffer(Array[Byte](1)))(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      tx2 = client.newTransaction()
      ptr2 <- allocator.allocateDataObject(DataBuffer(Array[Byte](2)))(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      dos1 <- client.read(ptr1)
      dos2 <- client.read(ptr2)
    yield
      ptr1.id should not be ptr2.id
      dos1.data.size should be(1)
      dos2.data.size should be(1)

  atest("allocated object is readable with correct content after commit"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("readback-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      allocator <- client.getAllocator(GroupObjectAllocatorId(groupId))

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](10, 20, 30)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      dos <- client.read(ptr)
    yield
      dos.data.size should be(3)
      dos.data.get(0) should be(10)
      dos.data.get(1) should be(20)
      dos.data.get(2) should be(30)
