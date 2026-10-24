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
    val newState = synchronized:
      refreshing -= groupId
      val (_, currentRevision, currentState) = states(groupId)

      if currentRevision != newDos.revision then
        val ags = AllocationGroupState(newDos)
        states += groupId -> (System.currentTimeMillis(), newDos.revision, ags)
        Some(ags)
      else
        // Restart the staleness clock so the next refresh waits a full groupRefreshDuration
        states += groupId -> (System.currentTimeMillis(), currentRevision, currentState)
        None

    // A child group that joined since the last read must be cached for groupMaxObjectSize to see it live
    newState.foreach(loadChildGroups)

  /** Loads (and so caches, with their own children) the states of `ags`'s child groups that are not cached yet.
    * Failures are ignored: groupMaxObjectSize falls back to the member's snapshot for a child it cannot see.
    */
  private def loadChildGroups(ags: AllocationGroupState): Future[Unit] =
    val missing = ags.members.filter(_.memberType == AllocationGroupState.MemberType.Group)
      .map(m => AllocationGroupId(m.uuid))
      .filter(id => synchronized(!states.contains(id)))
    Future.sequence(missing.map(id => getAllocator(GroupObjectAllocatorId(id)).map(_ => ()).recover(_ => ())))
      .map(_ => ())

  /** Largest object that can be allocated through group `groupId`, computed from the live cached states: the
    * smallest limit among its members, where a pool member contributes its `maxObjectSize` (fixed for the pool's
    * lifetime, so the value recorded in the member entry is exact) and a child group member contributes this same
    * computation over the child's cached state. Cached states are refreshed in the background once older than
    * `groupRefreshDuration`, so a membership change anywhere below is seen within about that long. A child group
    * whose state has not been loaded yet contributes its member entry's snapshot, taken when it joined, while its
    * state is loaded in the background. `None` when nothing below the group has a limit.
    */
  private[internal] def groupMaxObjectSize(groupId: AllocationGroupId): Option[Int] =
    val state = getState(groupId)
    val memberLimits = state.members.flatMap: m =>
      m.memberType match
        case AllocationGroupState.MemberType.Pool => m.maxObjectSize
        case AllocationGroupState.MemberType.Group =>
          val childId = AllocationGroupId(m.uuid)
          if synchronized(states.contains(childId)) then
            groupMaxObjectSize(childId)
          else
            loadChildGroups(state)
            m.maxObjectSize
    memberLimits.minOption

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
          client.read(ptr).flatMap: dos =>
            val ags = putState(dos.revision, AllocationGroupState(dos))
            // Child groups are loaded before the allocator is handed out so its maxObjectSize is live at once
            loadChildGroups(ags).map(_ => new GroupObjectAllocator(g.groupId, this))

      falloc.foreach: alloc =>
        synchronized(allocators += allocId -> alloc)

      falloc

