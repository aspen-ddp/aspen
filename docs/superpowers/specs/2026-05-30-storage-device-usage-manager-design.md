# Storage Device Usage Manager Design

## Overview

Add a `StorageDeviceUsageManager` that tracks and updates `StorageDeviceState` objects when `currentUsage` or `totalSize` change by more than 0.5%. Follows the same pattern as the existing `StoragePoolUsageManager`. Integrated into `StoreManager` alongside the pool usage manager, sharing the same periodic update task.

## StorageDeviceUsageManager

**Package:** `org.aspen_ddp.aspen.server.usage`

**Constructor:** `StorageDeviceUsageManager(client: AspenClient)`

### Internal State

- `cachedUsage: Map[StorageDeviceId, Long]` — last committed `currentUsage` per device (initially empty; defaults to 0 on first lookup)
- `cachedTotalSize: Map[StorageDeviceId, Long]` — last committed `totalSize` per device (initially empty; defaults to 0 on first lookup)
- `cachedPointers: Map[StorageDeviceId, KeyValueObjectPointer]` — device KV object pointers, lazily populated

All mutable state is accessed under `synchronized`.

### Threshold Check

Reuses the same threshold logic as `StoragePoolUsageManager`:

```
exceedsThreshold(newValue, oldValue) = abs(newValue - oldValue) / max(oldValue, 1) > 0.005
```

### Update Method

```scala
def updateDeviceUsage(
  storageDeviceId: StorageDeviceId,
  currentUsage: Long,
  totalSize: Long
): Future[Unit]
```

1. Read cached `currentUsage` and `totalSize` for the device (default 0 if absent).
2. If neither value exceeds the 0.5% threshold vs. cached, return `Future.unit`.
3. Otherwise, execute `client.transactUntilSuccessfulWithRecovery`:
   a. Resolve device KV object pointer (cached or via `client.getStorageDevicePointer`).
   b. Read the device KV object.
   c. Deserialize `StorageDeviceState` from `StateKey`.
   d. Create updated state: `state.copy(currentUsage = currentUsage, totalSize = totalSize)`.
   e. Build requirement: `KeyRevision(StorageDeviceState.StateKey, revision)`.
   f. Build operation: `Insert(StorageDeviceState.StateKey, updatedState.encode())`.
   g. Call `tx.update(...)`.
4. On transaction success, update both cached values.

Error recovery follows the same pattern as `StoragePoolUsageManager`: `FatalReadError` triggers `StopRetrying`; other errors return `Future.unit` to allow retry.

### Pointer Caching

`getDevicePointer(storageDeviceId)` checks the synchronized cache first. On miss, calls `client.getStorageDevicePointer(storageDeviceId)` and caches the result.

## StoreManager Integration

### New Field

```scala
private val deviceUsageManager = new StorageDeviceUsageManager(client)
```

Declared alongside the existing `poolUsageManager`.

### Periodic Task Update

In `startUsageTracking`, the existing periodic task body is extended to iterate over local storage devices after iterating over stores:

```scala
usageUpdateTask = Some(backgroundTasks.schedulePeriodic(Duration(20, SECONDS)):
  synchronized:
    stores.valuesIterator.foreach: store =>
      poolUsageManager.updateStoreSize(store.storeId, store.estimateSize())
    storageDevices.valuesIterator.foreach: sds =>
      deviceUsageManager.updateDeviceUsage(sds.storageDeviceId, sds.currentUsage, sds.totalSize)
)
```

The `LocalStorageDeviceState` class already provides `currentUsage` and `totalSize` methods that read from the local filesystem via `configFile.getTotalSpace` and `configFile.getFreeSpace`.

### No TaskExecutor Needed

Unlike `StoragePoolUsageManager`, no `setTaskExecutor` method is needed since there are no downstream durable tasks to trigger.

### Shutdown

No additional shutdown logic needed. The single `usageUpdateTask` already covers both loops and is cancelled in `shutdown()`.

## Files Changed

1. **New:** `src/main/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManager.scala`
2. **Modified:** `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — add `deviceUsageManager` field and extend periodic task loop
