package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

class AllocationGroupCache(client: AspenClient, refreshDuration: Duration = Duration(20, SECONDS)):

  given ec: ExecutionContext = client.clientContext

  private var pointers: Map[AllocationGroupId, DataObjectPointer] = Map.empty
  private var refreshing: Set[AllocationGroupId] = Set.empty
  private var states: Map[AllocationGroupId, (Long, AllocationGroupState)] = Map.empty

  def fetchState(groupId: AllocationGroupId): Future[AllocationGroupState] =
    synchronized:
      states.get(groupId) match
        // Case 1: State cached - return immediately, maybe refresh in background
        case Some((timestamp, cachedState)) =>
          val now = System.currentTimeMillis()
          val isStale = (now - timestamp) > refreshDuration.toMillis
          val isRefreshing = refreshing.contains(groupId)

          if isStale && !isRefreshing then
            refreshing += groupId
            // Fire-and-forget background refresh
            client.getAllocationGroupState(groupId).onComplete:
              case scala.util.Success(newState) =>
                synchronized:
                  states += groupId -> (System.currentTimeMillis(), newState)
                  refreshing -= groupId
              case scala.util.Failure(_) =>
                synchronized:
                  refreshing -= groupId

          Future.successful(cachedState)

        case None =>
          pointers.get(groupId) match
            // Case 2: No state, pointer cached - read via pointer
            case Some(pointer) =>
              client.read(pointer).map: dos =>
                val state = AllocationGroupState(dos)
                synchronized:
                  states += groupId -> (System.currentTimeMillis(), state)
                state

            // Case 3: No state, no pointer - fetch both
            case None =>
              // Fire-and-forget pointer caching
              client.getAllocationGroupPointer(groupId).foreach: ptr =>
                synchronized:
                  pointers += groupId -> ptr

              // Main path: getAllocationGroupState
              client.getAllocationGroupState(groupId).map: state =>
                synchronized:
                  states += groupId -> (System.currentTimeMillis(), state)
                state
