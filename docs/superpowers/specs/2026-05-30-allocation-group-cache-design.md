# AllocationGroupCache Design

## Overview

A cache for `AllocationGroupState` instances in `aspen.client.internal` that keeps state "relatively" up to date over long periods by periodically refreshing stale entries in the background.

## Location

`org.aspen_ddp.aspen.client.internal.AllocationGroupCache`

## Constructor

```scala
class AllocationGroupCache(client: AspenClient, refreshDuration: Duration = Duration(20, SECONDS))
```

- `client` — used for pointer lookups, object reads, and state fetching
- `refreshDuration` — how long a cached state is considered fresh before a background refresh is triggered

Uses `client.clientContext` as the implicit `ExecutionContext`.

## Internal State

All mutable state protected by `synchronized(this)` blocks:

- `pointers: Map[AllocationGroupId, DataObjectPointer]` — cached pointer lookups, optimization to avoid repeated `getAllocationGroupPointer` calls
- `refreshing: Set[AllocationGroupId]` — group IDs currently undergoing a background refresh, prevents duplicate concurrent refreshes
- `states: Map[AllocationGroupId, (Long, AllocationGroupState)]` — cached states keyed by group ID, paired with `System.currentTimeMillis()` timestamp of last update

## Public API

### `fetchState(groupId: AllocationGroupId): Future[AllocationGroupState]`

Single entry point. Three cases based on cache state:

### Case 1: State is cached

Always returns `Future.successful(cachedState)` immediately, regardless of staleness.

If the timestamp is older than `refreshDuration` and `groupId` is not in `refreshing`:
- Add `groupId` to `refreshing`
- Fire-and-forget: read the data object using the cached pointer, create a new `AllocationGroupState` from the result, update the states map entry with the new state and current timestamp, remove `groupId` from `refreshing`
- On failure: remove `groupId` from `refreshing` silently (stale cached state remains available)

### Case 2: No cached state, pointer is cached

- Read the data object using the cached pointer via `client.read(pointer)`
- Create `AllocationGroupState` from the `DataObjectState`
- Update the states map with the new state and current timestamp
- Return the state

### Case 3: No cached state, no cached pointer

- Call `client.getAllocationGroupPointer(groupId)` and cache the result in `pointers` (optimization for future calls)
- Concurrently call `client.getAllocationGroupState(groupId)` to obtain the state
- When the state future completes, update the states map with the new state and current timestamp
- Return the state

## Concurrency

- All reads and writes of `pointers`, `refreshing`, and `states` are protected by `synchronized(this)` blocks
- Locks are released before async I/O operations
- Background refreshes are fire-and-forget — they update state on completion but never block the caller

## Error Handling

- If a background refresh (case 1) fails, the failure is swallowed and `groupId` is removed from `refreshing`. The stale cached state remains available.
- If an initial fetch (cases 2 and 3) fails, the failure propagates to the caller since there is no cached state to fall back to.

## Testing

Unit tests should cover:
- Fetching state when cache is empty (case 3 flow)
- Returning cached state immediately when available (case 1)
- Background refresh triggering when state is stale
- No duplicate concurrent refreshes for the same group ID
- Error handling: background refresh failure preserves stale state
- Error handling: initial fetch failure propagates to caller
- Pointer caching optimization works for subsequent fetches
