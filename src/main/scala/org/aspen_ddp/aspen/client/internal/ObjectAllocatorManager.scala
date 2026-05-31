package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.allocation.{GroupObjectAllocator, PoolObjectAllocator}
import org.aspen_ddp.aspen.client.{AspenClient, GroupObjectAllocatorId, ObjectAllocator, ObjectAllocatorId, PoolObjectAllocatorId}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, Future}


class ObjectAllocatorManager(val client:AspenClient, groupRefreshDuration: Duration = Duration(20, SECONDS)):

  given ExecutionContext = client.clientContext

  private var allocators: Map[ObjectAllocatorId, ObjectAllocator] = Map()
  
  private var pointers: Map[AllocationGroupId, DataObjectPointer] = Map.empty
  private var refreshing: Set[AllocationGroupId] = Set.empty
  private var states: Map[AllocationGroupId, (Long, AllocationGroupState)] = Map.empty
  
  private def putState(ags: AllocationGroupState): Unit =
    synchronized:
      states += ags.groupId -> (System.currentTimeMillis(), ags)
      
  private[internal] def getState(groupId: AllocationGroupId): AllocationGroupState =
    synchronized:
      val (timestamp, state) = states(groupId)
      
      val now = System.currentTimeMillis()
      val isStale = (now - timestamp) > groupRefreshDuration.toMillis
      val isRefreshing = refreshing.contains(groupId)

      if isStale && !isRefreshing then
        // Fire-and-forget background refresh
        refreshing += groupId

        val fstate = pointers.get(groupId) match
          case Some(pointer) => client.read(pointer).map(AllocationGroupState(_))
          case None => client.getAllocationGroupState(groupId)

        fstate.onComplete:
          case scala.util.Success(newState) =>
            synchronized:
              states += groupId -> (System.currentTimeMillis(), newState)
              refreshing -= groupId
          case scala.util.Failure(_) =>
            synchronized:
              refreshing -= groupId
      
      state

  def getAllocator(allocId: ObjectAllocatorId): Future[ObjectAllocator] = synchronized(allocators.get(allocId)) match
    case Some(alloc) => Future.successful(alloc)
    case None =>
      val falloc = allocId match 
        case p: PoolObjectAllocatorId => client.getStoragePool(p.poolId).map: pool => 
          new PoolObjectAllocator(client, pool)
        case g: GroupObjectAllocatorId => client.getAllocationGroupState(g.groupId).map: ags =>
          putState(ags)
          
          // Cache the pointer to the group so subsequent reads don't need to navigate the TKVL
          client.getAllocationGroupPointer(g.groupId).foreach: ptr =>
            synchronized:
              pointers += g.groupId -> ptr
              
          new GroupObjectAllocator(g.groupId, this)
          
      falloc.foreach: alloc =>
        synchronized(allocators += allocId -> alloc)

      falloc
  
