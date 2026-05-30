# UpdateAllocationGroupUsageTask Implementation Design

## Overview

Implement `UpdateAllocationGroupUsageTask` to propagate usage and maximum size changes upward through the allocation group hierarchy. When a member's (pool or child group) usage or size changes, this task updates the corresponding `Member` entry in each parent `AllocationGroupState`. If the update causes the parent group's aggregated values to change by more than 0.5%, a cascading task is created for the parent's parents.

## Approach

Custom stepping logic extending `DurableTask` directly. Groups are processed sequentially with an index (`NextIndexKey`) that is advanced atomically in each transaction. This provides crash recovery — on restart, the task resumes from the last committed index.

## Signature Change: `DurableTaskFactory.createTask`

Add a `TaskExecutor` parameter to the `createTask` method:

```scala
def createTask(client: AspenClient,
               pointer: DurableTaskPointer,
               revision: ObjectRevision,
               state: Map[Key, KeyValueObjectState.ValueState],
               taskExecutor: TaskExecutor): DurableTask
```

`SimpleTaskExecutor` passes `this` at both call sites (recovery loop in constructor, and post-commit in `prepareTask`). All 6 existing implementations add the parameter, ignoring it where not needed.

### Files affected by signature change

- `DurableTaskFactory.scala` — trait definition
- `SimpleTaskExecutor.scala` — both call sites pass `this`
- `SteppedDurableTaskSuite.scala` (`TestSteppedTask`) — add ignored param
- `CreateFileTask.scala` — add ignored param
- `SimpleFileContent.scala` (`DeleteFileContentTask`) — add ignored param
- `SimpleDirectory.scala` (`DeleteDirectoryContentTask`) — add ignored param
- `UnlinkFileTask.scala` — add ignored param
- `UpdateAllocationGroupUsageTask.scala` — uses the param

## Task Class Design

### Constructor

```scala
class UpdateAllocationGroupUsageTask(
  val taskPointer: DurableTaskPointer,
  client: AspenClient,
  childUUID: UUID,
  currentUsage: Long,
  maximumSize: Long,
  allocationGroups: List[UUID],
  taskExecutor: TaskExecutor
) extends DurableTask with Logging
```

### Factory: `createTask`

Reconstitutes state from persisted KV entries:

- `ChildUUIDKey` → `byte2uuid` → `childUUID`
- `CurrentUsageKey` → `byte2long` → `currentUsage`
- `MaxSizeKey` → `byte2long` → `maximumSize`
- `AllocationGroupsKey` → `byte2uuids` → `allocationGroups`

Passes all fields plus `taskExecutor` to the constructor.

### Execution: `updateNextGroup()`

Called in the constructor body to begin processing. On each invocation:

1. Read the task's KV object to get the current `NextIndexKey` value.
2. If `nextIndex >= allocationGroups.length`, complete the promise with `None`.
3. Get the target group UUID from `allocationGroups(nextIndex)`.
4. Attempt to read the parent group's `AllocationGroupState` via `client.getAllocationGroupPointer` then `client.read`.
   - If pointer lookup fails (`NoSuchElementException`) or read fails (`ReadError`): skip — advance `NextIndexKey` in a standalone transaction (just the index update on the task's KV object), then continue.
5. Find the member with `uuid == childUUID` in the group's member list.
   - If not found: skip — advance `NextIndexKey` the same way, then continue.
6. Update the member's `currentUsage` and `maximumSize`. Produce a new `AllocationGroupState`.
7. Check threshold: compare the group's old aggregated `currentUsage`/`maximumSize` against the new values. If either exceeds 0.5% change **and** the group has `parentGroups`, prepare a cascading `UpdateAllocationGroupUsageTask` within the same transaction.
8. Build a transaction that atomically:
   - Overwrites the parent group's data object with the updated state.
   - Updates `NextIndexKey` to `nextIndex + 1` on the task's KV object (with a `KeyRevision` requirement on the current value).
   - If cascading: calls `UpdateAllocationGroupUsageTask.prepareTask` with the group's UUID as `childUUID`, the group's new aggregated `currentUsage` (sum of all members after update) as `currentUsage`, the group's new aggregated `maximumSize` (sum of all members after update) as `maximumStoreSize`, and the group's `parentGroups` UUIDs as `allocationGroups`.
9. Commit. On success, call `updateNextGroup()` again. On failure, schedule a retry.

### Threshold Check

```scala
private def exceedsThreshold(newValue: Long, oldValue: Long): Boolean =
  math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005
```

### Retry Logic

Mirrors `SteppedDurableTask`:

- Initial delay: 16ms
- Doubles on each retry, capped at 60 seconds
- Resets to 16ms on successful commit
- Uses `client.backgroundTaskManager.schedule()` to schedule retries

### Error Handling

- **Transaction abort** (conflict): retry the same index with backoff.
- **Unexpected error during transaction setup**: retry with backoff.
- **Group pointer not found** (`NoSuchElementException`/`ReadError`): skip — advance index in a standalone transaction, continue to next group.
- **Member not found in group**: skip — advance index, continue.
- **`StopRetrying`**: complete the promise with failure.
- All promise completions wrapped in `synchronized` + `if !promise.isCompleted`.

## Test Suite

New file: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`

Extends `IntegrationTestSuite`. Uses the same setup pattern as `AllocationGroupStateSuite` (create executor with `SimpleTaskExecutor`, helper methods for reading group/pool state).

### Test Cases

1. **"updates member usage in single parent group"** — Add a pool to a group, run the task with new usage/size values. Verify the member's `currentUsage` and `maximumSize` are updated.

2. **"updates member usage across multiple parent groups"** — Add a pool to two groups. Run the task targeting both. Verify both groups have updated member entries.

3. **"skips silently when member not found in group"** — Create a group, run the task with a childUUID not in the group's members. Verify the task completes and the group is unchanged.

4. **"cascades to grandparent when change exceeds threshold"** — 3-level hierarchy: pool → child group → parent group. Run the task with a large usage change on the pool targeting the child group. Verify both the child group and parent group are updated.

5. **"does not cascade when change is below threshold"** — Same hierarchy, tiny usage change. Verify only the child group is updated; parent group unchanged.

6. **"task completes when all groups result in skips"** — Run the task where no group contains the childUUID. Verify completion with `None`.
