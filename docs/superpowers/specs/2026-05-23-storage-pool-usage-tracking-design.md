# Storage Pool Usage Tracking

Adds usage metrics tracking for DataStores and StoragePools. Each store periodically reports its estimated size to a centralized `StoragePoolUsageManager`, which updates the pool's KV object when sizes change significantly.

## StoragePoolState Changes

### Protobuf (`codec.proto`)

Add three fields to `StoragePoolState`:

```protobuf
message StoragePoolState {
  UUID poolId = 1;
  string name = 2;
  IDA ida = 3;
  uint32 maxObjectSize = 4;
  repeated PoolStoreEntry stores = 5;
  BackendConfig backendConfig = 6;
  int64 currentUsage = 7;
  int64 maximumStoreSize = 8;
  repeated UUID allocationGroups = 9;
}
```

- `currentUsage`: arithmetic mean of all store sizes in the pool
- `maximumStoreSize`: maximum allowed size per store
- `allocationGroups`: UUIDs of allocation groups to notify on usage change

### Scala Case Class

Add corresponding fields with defaults for backward compatibility:

```scala
final case class StoragePoolState(
  poolId: PoolId, name: String, ida: IDA, maxObjectSize: Option[Int],
  stores: Array[StoragePoolState.StoreEntry], backendConfig: BackendConfig,
  currentUsage: Long = 0,
  maximumStoreSize: Long = 0,
  allocationGroups: List[UUID] = Nil
)
```

### Companion Object

Add a method to generate the KV key for per-store usage:

```scala
def getStoreUsageKey(poolIndex: Byte): Key = Key(s"sz${poolIndex}")
```

### Codec.scala

Extend `encode(o: StoragePoolState)` and `decode(m: codec.StoragePoolState)` to handle the three new fields. `allocationGroups` uses existing `encodeUUID`/`decodeUUID`. Protobuf defaults (0 for int64, empty for repeated) provide backward compatibility.

## StoragePoolUsageManager

**File:** `src/main/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManager.scala`

### Constructor

```scala
class StoragePoolUsageManager(client: AspenClient)
```

A `setTaskExecutor(executor: TaskExecutor)` method is called once the executor is available.

### Internal State (synchronized)

- `cachedPointers: Map[PoolId, KeyValueObjectPointer]` — avoids re-resolving pool pointers
- `cachedSizes: Map[StoreId, Long]` — last-reported size per store

All access to these maps is synchronized since Future completions arrive on separate threads.

### `updateStoreSize(storeId: StoreId, currentEstimatedUsage: Long): Future[Unit]`

1. Check `cachedSizes` for this store's last-reported size. If delta < 0.5%, return immediately — no pool object read.
2. If threshold exceeded, use `transactUntilSuccessfulWithRecovery`:
   - The recovery function (`onCommitFailure`) checks whether the failure is a read error (e.g. the pool object no longer exists because the pool was deleted). If so, it fails the future to stop retries. For other errors (transaction collisions), it allows retries to continue.
   - Prepare function:
     a. Look up pool pointer (from `cachedPointers` or `client.getStoragePoolPointer`)
     b. Read pool KV object state
     c. Build store size KV pair update: `Insert(getStoreUsageKey(poolIndex), longToBytes(currentEstimatedUsage))`
        - Requirement: `DoesNotExist(key)` if key absent, `KeyRevision(key, revision)` if present
     d. Decode `StoragePoolState` from `ConfigKey`. Sum all store sizes from KV pairs (substituting new value for this store), compute arithmetic mean. If mean differs from `currentUsage` by > 0.5%, re-encode with updated `currentUsage`, add `Insert(ConfigKey, ...)` with `KeyRevision(ConfigKey, revision)`
     e. If updating `currentUsage` AND `allocationGroups.nonEmpty`, prepare `UpdateAllocationGroupUsageTask` via the `TaskExecutor`
3. On success, update `cachedSizes` (synchronized)

### Threshold Check

```
math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005
```

## StoreManager Integration

### TaskExecutor Initialization

Add a `TaskExecutorRootKey` to the host's KV object. On StoreManager startup:

1. Create a `Promise[TaskExecutor]`
2. Read the host KV object
3. If `TaskExecutorRootKey` exists: read the pointer, construct `SimpleTaskExecutor`, complete promise
4. If absent: use `transactUntilSuccessful` to allocate a new KV object and update the host KV object with the pointer. Only after the transaction commits, construct `SimpleTaskExecutor` and complete the promise.

Expose `getTaskExecutor(): Future[TaskExecutor]` that returns the promise's future.

### Periodic Usage Task

After the TaskExecutor promise completes:

1. Create a `StoragePoolUsageManager(client)` and call `setTaskExecutor(executor)`
2. Schedule a periodic task (every 20 seconds) that iterates all loaded stores, calls `estimateSize()`, and passes the result to `usageManager.updateStoreSize(storeId, size)`
3. Store the periodic task handle in an instance variable so it can be cancelled during shutdown/testing

## UpdateAllocationGroupUsageTask

**File:** `src/main/scala/org/aspen_ddp/aspen/server/usage/UpdateAllocationGroupUsageTask.scala`

Extends `DurableTask` directly (not `SteppedDurableTask`).

### Companion Object (DurableTaskFactory)

- `typeUUID`: unique UUID for this task type
- `createTask(...)`: reconstitutes from persisted state, returns immediately-completed task
- `prepareTask(timestamp, currentUsage, maximumStoreSize, allocationGroups, taskExecutor)`: persists arguments as KV pairs and prepares via `taskExecutor.prepareTask`

### Persisted State Keys

- `TimestampKey (1)` — transaction timestamp as bytes
- `CurrentUsageKey (2)` — new pool currentUsage as 8-byte Long
- `MaxStoreSizeKey (3)` — maximumStoreSize as 8-byte Long
- `AllocationGroupsKey (4)` — encoded list of allocation group UUIDs

### Current Implementation

Immediately completes with `None`. The real implementation will be provided later.

## Unit Tests

Integration tests using real in-memory AspenClient and stores.

### Test Cases

1. **No update when size unchanged** — call `updateStoreSize` with same size as cached, verify no transaction
2. **Update store size KV pair on first report** — first report for a store uses `DoesNotExist` requirement, verify KV pair created
3. **Update store size KV pair on significant change** — size differs by > 0.5%, verify KV pair updated with `KeyRevision`
4. **No update when change below threshold** — size differs by < 0.5%, verify no update
5. **currentUsage updated when mean changes significantly** — multi-store pool, one store's size shifts mean by > 0.5%, verify config updated
6. **Durable task prepared when allocationGroups non-empty** — configure pool with allocationGroups, trigger mean-affecting change, verify task prepared
7. **No durable task when allocationGroups empty** — same change but empty allocationGroups, verify no task
