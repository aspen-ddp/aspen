package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.internal.ObjectAllocatorManager
import org.aspen_ddp.aspen.client.{AllocationError, AspenClient, GroupObjectAllocatorId, ObjectAllocator, ObjectAllocatorId, PoolObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState.MemberType
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.{ExecutionContext, Future}

// Relies on the ObjectAllocatorManager having the corresponding AllocationGroupState present
// in its cache. Must only ever be constructed by the ObjectAllocationManager
class GroupObjectAllocator private[internal] (val groupId: AllocationGroupId,
                                              val manager: ObjectAllocatorManager) extends ObjectAllocator:

  given ExecutionContext = manager.client.clientContext

  val allocatorId: ObjectAllocatorId = GroupObjectAllocatorId(groupId)

  val client: AspenClient = manager.client

  override def maxObjectSize: Option[Int] = manager.getState(groupId).maximumObjectSize
  
  private def getSubAllocator(): Future[ObjectAllocator] =
    val state = manager.getState(groupId)

    state.selectMemberForAllocation() match
      case None => Future.failed(new AllocationError(s"Allocation Group ${state.name} is empty. Unable to allocate object"))
      case Some(member) =>
        val allocatorId: ObjectAllocatorId = member.memberType match
          case MemberType.Pool => PoolObjectAllocatorId(PoolId(member.uuid))
          case MemberType.Group => GroupObjectAllocatorId(AllocationGroupId(member.uuid))

        manager.client.getAllocator(allocatorId)

  override protected[client] def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] = 
    getSubAllocator().flatMap: allocator =>
      allocator.createDataObjectPointer()

  override protected[client] def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] =
    getSubAllocator().flatMap: allocator =>
      allocator.createKeyValueObjectPointer()

