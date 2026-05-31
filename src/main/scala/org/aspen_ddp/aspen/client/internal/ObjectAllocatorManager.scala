package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.allocation.{GroupObjectAllocator, PoolObjectAllocator}
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, Future}


class ObjectAllocatorManager(val client:AspenClient, groupRefreshDuration: Duration = Duration(20, SECONDS)):

  given ExecutionContext = client.clientContext

  private var allocators: Map[ObjectAllocatorId, ObjectAllocator] = Map()

  private var pointers: Map[AllocationGroupId, DataObjectPointer] = Map.empty
  private var refreshing: Set[AllocationGroupId] = Set.empty
  private var states: Map[AllocationGroupId, (Long, ObjectRevision, AllocationGroupState)] = Map.empty

  private def putState(revision: ObjectRevision, ags: AllocationGroupState): AllocationGroupState =
    synchronized:
      states += ags.groupId -> (System.currentTimeMillis(), revision, ags)
    ags

  private def refresh(groupId: AllocationGroupId, newDos: DataObjectState): Unit =
    synchronized:
      refreshing -= groupId
      val (_, currentRevision, currentState) = states(groupId)

      if currentRevision != newDos.revision then
        states += groupId -> (System.currentTimeMillis(), newDos.revision, AllocationGroupState(newDos))

  private[internal] def getState(groupId: AllocationGroupId): AllocationGroupState =
    synchronized:
      val (timestamp, revision, state) = states(groupId)

      val now = System.currentTimeMillis()
      val isStale = (now - timestamp) > groupRefreshDuration.toMillis
      val isRefreshing = refreshing.contains(groupId)

      if isStale && !isRefreshing then
        // Fire-and-forget background refresh
        refreshing += groupId

        client.read(pointers(groupId)).map(dos => refresh(groupId, dos)).failed.foreach: _ =>
          synchronized:
            refreshing -= groupId

      state

  private[internal] def get(allocatorId: ObjectAllocatorId): Option[ObjectAllocator] =
    synchronized(allocators.get(allocatorId))

  private[internal] def put(allocator: ObjectAllocator): Unit =
    allocator match
      case p: PoolObjectAllocator =>
        synchronized:
          allocators += p.allocatorId -> p
      case _ =>
        // Ignore all other allocator types


  def getAllocator(allocId: ObjectAllocatorId): Future[ObjectAllocator] = synchronized(allocators.get(allocId)) match
    case Some(alloc) => Future.successful(alloc)
    case None =>
      val falloc = allocId match
        case p: PoolObjectAllocatorId => client.getStoragePool(p.poolId).map: pool =>
          new PoolObjectAllocator(client, pool)
        case g: GroupObjectAllocatorId => client.getAllocationGroupPointer(g.groupId).flatMap: ptr =>
          synchronized(pointers += g.groupId -> ptr)
          client.read(ptr).map: dos =>
            putState(dos.revision, AllocationGroupState(dos))
            new GroupObjectAllocator(g.groupId, this)

      falloc.foreach: alloc =>
        synchronized(allocators += allocId -> alloc)

      falloc

