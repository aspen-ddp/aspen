# Stoppable DurableTask Design

## Goal

Extend `DurableTask` to be stoppable so that async retry loops and step progression can be cancelled on lease loss or clean shutdown during unit testing.

## New Exception

`TaskStopped` in `org.aspen_ddp.aspen.compute`. A simple exception with no message — the type communicates intent.

```scala
class TaskStopped extends Exception
```

## DurableTask Trait Changes

Add three members to the `DurableTask` trait:

- `@volatile protected var isStopped: Boolean = false`
- `final def stop(): Unit` — sets `isStopped = true`, then calls `onStop()`
- `protected def onStop(): Unit = ()` — default no-op, subclasses override for custom cleanup

`stop()` is `final` so subclasses cannot break the flag-setting contract. Custom shutdown behavior goes in `onStop()`.

## SteppedDurableTask Changes

At the top of `doNextStep()`, check `isStopped`. If true, complete the promise with `TaskStopped` using the existing `synchronized` / `isCompleted` guard, and return without executing any step or scheduling any retry.

No changes to `scheduleRetry()`, step execution, or the constructor. The check at `doNextStep()` entry catches all re-entry paths (initial call, retry callbacks, and post-commit progression).

## UpdateAllocationGroupUsageTask Changes

Same pattern as `SteppedDurableTask`. At the top of `updateNextGroup()`, check `isStopped`. If true, complete the promise with `TaskStopped` using the synchronized guard, and return.

## SimpleTaskExecutor Changes

**Change `active` type** from `Set[DurableTaskPointer]` to `Map[DurableTaskPointer, DurableTask]` so task instances are reachable for stop signalling. Update all sites that add/remove from `active`:

- Constructor recovery (line 69): store the task returned by `createTask`
- `prepareTask` (line 128-129): store the task in the map
- `deallocateTask` (line 97): remove from map instead of set

**Add `shutdown()` method** that iterates all active task instances and calls `stop()` on each. Service implementations that hold a `SimpleTaskExecutor` call `executor.shutdown()` from their own `DurableService.shutdown()`.

## Files Changed

| File | Change |
|---|---|
| New: `compute/TaskStopped.scala` | New exception class |
| `compute/DurableTask.scala` | Add `isStopped`, `stop()`, `onStop()` |
| `compute/SteppedDurableTask.scala` | Check `isStopped` at top of `doNextStep()` |
| `common/metadata/management/UpdateAllocationGroupUsageTask.scala` | Check `isStopped` at top of `updateNextGroup()` |
| `compute/impl/SimpleTaskExecutor.scala` | `active` becomes Map, add `shutdown()` |

## Files Not Changed

- `DurableService.scala` — no changes needed
- `SimpleDurableServiceExecutor.scala` — already calls `service.shutdown()` on lease loss
- `CreateFileTask.scala`, `UnlinkFileTask.scala` — inherit behavior from `SteppedDurableTask`

## Propagation Chain (target end state)

```
SimpleDurableServiceExecutor.handleLeaseLoss(serviceUUID)
  -> service.shutdown()                    // DurableService.shutdown()
    -> taskExecutor.shutdown()             // new method on SimpleTaskExecutor
      -> task.stop()                       // new method on DurableTask
        -> isStopped = true                // volatile flag
        -> onStop()                        // subclass hook
          -> doNextStep() checks flag, completes promise with TaskStopped
```

## Design Decisions

- **Check at loop entry only** (not in `scheduleRetry`): matches `ExponentialBackoffRetryStrategy` pattern. Worst case is one extra scheduled callback that immediately aborts.
- **Promise completes with `TaskStopped` failure**: triggers `deallocateTask` in `SimpleTaskExecutor`, cleaning up the KV object. Next lease holder re-creates the task from scratch on recovery.
- **`stop()` is final**: prevents subclasses from accidentally breaking the flag-setting contract.
