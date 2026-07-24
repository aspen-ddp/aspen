# Design: `add-pool-to-group` CLI command

**Date:** 2026-07-24
**Status:** Approved
**Source:** Top item in `TODO.txt`

## Goal

Add a CLI command that adds a storage pool to an allocation group by name. This
requires supporting plumbing so an allocation-group membership change can create
its usage-cascade durable task **without** a locally-running `TaskExecutor` — the
CLI is a client-only process and has no local executor.

The membership change and the creation of the usage-cascade durable task must be
**atomic** (committed in a single transaction).

## Background

- `AllocationGroupState.addPool` / `removePool` / `addGroup` / `removeGroup`
  modify group membership inside a transaction driven by
  `transactUntilSuccessfulWithRecovery`. When the affected group has parent
  groups, they enroll an `UpdateAllocationGroupUsageTask` to cascade usage/size
  changes up the group hierarchy.
- Enrollment currently goes through a required `TaskExecutor`
  (`UpdateAllocationGroupUsageTask.prepareTask`), which stages the task-state
  object into the live transaction.
- All current callers of these four methods are tests. The new CLI command will
  be the first production caller.
- `AspenClient.createSystemDurableTask` already exists but is **not**
  transaction-scoped: it runs its own transactions (allocate task-state object,
  then separately enroll into the `SystemTaskExecutorService` task TKVL) plus a
  best-effort service wake-up message. It therefore cannot participate in a
  caller-supplied transaction.
- `SystemTaskServiceState` already has a `removeInTx(...)(using tx)` helper — the
  precedent for operating on the service task TKVL within a supplied transaction.
- `transact` (in `AspenClient`) awaits the future returned by `prepare` **before**
  calling `tx.commit()`. Any staging work that must land in the transaction has to
  be represented by a future that `prepare` returns (i.e. chained into its
  for-comprehension), not fired-and-forgotten.

### Latent race being fixed

The existing `modifyPool` / `modifyGroup` fire
`UpdateAllocationGroupUsageTask.prepareTask(...)` inside the `yield` block without
chaining the resulting future into the for-comprehension. Because `transact`
awaits only the returned future before committing, the task-staging work races
with commit. This design makes the task-preparation future awaited, closing the
race for both the executor path and the new system path.

## Design

Four layers, top to bottom.

### 1. CLI command (`Main.scala`)

Follow the existing `create-allocation-group` pattern:

- Add `poolName: String = ""` to the `Args` case class; reuse the existing
  `newGroupName` field for the group-name argument.
- Add a `cmd("add-pool-to-group")` parser block with three arguments: bootstrap
  config file, `<pool-name>`, `<group-name>`.
- Add dispatch case:
  `case "add-pool-to-group" => add_pool_to_group(bootstrapConfigPath, cfg.poolName, cfg.newGroupName)`.
- Implement `add_pool_to_group(...)`:
  - `configureLogging()`, `createAmoebaClient(...)`, `network.startIoThread(client)`.
  - Call `client.addPoolToGroup(poolName, groupName)`.
  - On success, print a confirmation; on failure translate `NoSuchElementException`
    into a "pool or group not found" message and fall back to a generic message
    otherwise.
  - `Await.ready(f, Duration(30, SECONDS))`.

### 2. `AspenClient.addPoolToGroup`

Declared on the `AspenClient` trait, implemented in `BaseAspenClient`, mirroring
`createAllocationGroup`:

```scala
def addPoolToGroup(poolName: String, groupName: String): Future[Unit] =
  given ExecutionContext = clientContext
  for
    poolId  <- getStoragePoolId(poolName)        // throws NoSuchElementException if absent
    groupId <- getAllocationGroupId(groupName)    // throws NoSuchElementException if absent
    _       <- AllocationGroupState.addPool(this, poolId, groupId, None)  // None -> system path
  yield ()
```

Name resolution via `getStoragePoolId` / `getAllocationGroupId` throws
`NoSuchElementException` when a name is not registered, satisfying the "if both
pool and group exist" requirement.

### 3. `AspenClient.prepareSystemDurableTask` (transaction-scoped primitive)

New abstract method on the `AspenClient` trait, implemented in `BaseAspenClient`:

```scala
def prepareSystemDurableTask(taskTypeUUID: UUID,
                             initialState: Map[Key, Array[Byte]])
                            (using tx: Transaction): Future[Unit]
```

It stages the following into the caller-supplied `tx`:

1. Allocation of the task-state KV object in the bootstrap pool
   (`PoolObjectAllocator.allocateKeyValueObject`, using the given `tx`). The task
   content is `initialState` plus `SimpleTaskExecutor.TaskTypeKey -> taskTypeUUID`
   (matching `createSystemDurableTask`).
2. Enrollment of the task pointer into the `SystemTaskExecutorService` task TKVL
   via a new helper `SystemTaskServiceState.enrollInTx(client, statePtr, taskId,
   taskStatePtr)(using tx)` — the sibling of the existing `removeInTx`.

The service state pointer is resolved by reading the services TKVL (as
`createSystemDurableTask` does today). The best-effort wake-up message is hooked
to `tx.result.foreach { _ => sendServiceMessage(SystemTaskExecutorService.ServiceUUID, ...) }`,
matching how `transferStore` fires `CheckStorageDevice` post-commit.

The method performs **no internal retry**. The returned future resolves once the
allocation and enrollment operations are staged into `tx`; commit and retry are
the caller's responsibility (the enclosing `transactUntilSuccessful*` handles
them). It validates that `taskTypeUUID` resolves to a `DurableTaskFactory` in the
type registry, failing with `StopRetrying(IllegalArgumentException)` otherwise
(same guard as `createSystemDurableTask`).

New helper in `SystemTaskServiceState`:

```scala
def enrollInTx(client: AspenClient,
               statePtr: KeyValueObjectPointer,
               taskId: UUID,
               taskStatePtr: KeyValueObjectPointer)(using tx: Transaction): Future[Unit] =
  tkvl(client, statePtr).set(Key(uuid2byte(taskId)), Value(taskStatePtr.toArray))
```

**Refactor:** `createSystemDurableTask` is re-expressed to delegate to
`prepareSystemDurableTask`:

```scala
override def createSystemDurableTask(taskTypeUUID: UUID,
                                     initialState: Map[Key, Array[Byte]]): Future[Unit] =
  given ExecutionContext = clientContext
  transactUntilSuccessful: tx =>
    given Transaction = tx
    prepareSystemDurableTask(taskTypeUUID, initialState)
```

This removes the current duplicate "allocate in one transaction, enroll in a
separate transaction" logic and makes `createSystemDurableTask` atomic as well.

### 3b. `UpdateAllocationGroupUsageTask.prepareSystemTask` (transaction-scoped)

Sibling of the existing `prepareTask`, routing through the new primitive:

```scala
def prepareSystemTask(client: AspenClient,
                      childUUID: UUID,
                      currentUsage: Long,
                      maximumStoreSize: Long,
                      allocationGroups: List[UUID])
                     (using tx: Transaction): Future[Unit] =
  require(allocationGroups.nonEmpty)
  client.prepareSystemDurableTask(typeUUID, Map(
    ChildUUIDKey        -> uuid2byte(childUUID),
    CurrentUsageKey     -> long2byte(currentUsage),
    MaxSizeKey          -> long2byte(maximumStoreSize),
    AllocationGroupsKey -> uuids2byte(allocationGroups),
    NextIndexKey        -> long2byte(0)))
```

(Same initial-state map that `prepareTask` builds; `prepareSystemDurableTask`
adds the task-type key itself.)

### 4. Optional executor in `modifyPool` / `modifyGroup`

Change the executor parameter on `addPool`, `removePool`, `addGroup`,
`removeGroup`, `modifyPool`, and `modifyGroup` from
`taskExecutor: TaskExecutor` to `taskExecutor: Option[TaskExecutor] = None`.

Two coupled changes inside `modifyPool` / `modifyGroup`:

1. **Await the task-staging future.** Restructure `prep` so the task-preparation
   future is chained into the for-comprehension (`_ <- prepareFuture`) rather than
   fired-and-forgotten in the `yield`. `transact` then awaits it before commit,
   giving genuine atomicity for both the executor and system paths.

2. **Fire the usage task only on real change.** The `mod` closures return
   `Option[(StoragePoolState, AllocationGroupState)]` — `Some((nps, nags))` when a
   change was staged, `None` on a no-op (pool/group already in the target state).
   The task is prepared only when the result is `Some` **and** the resulting
   group has parent groups:

```scala
mod(...) match
  case None =>
    Future.unit                                   // no-op: nothing changed, no task
  case Some((_, nags)) if nags.parentGroups.nonEmpty =>
    taskExecutor match
      case Some(exec) =>
        UpdateAllocationGroupUsageTask
          .prepareTask(id, nags.currentUsage, nags.maximumSize,
                       nags.parentGroups.map(_.uuid), exec).map(_ => ())
      case None =>
        UpdateAllocationGroupUsageTask
          .prepareSystemTask(client, id, nags.currentUsage, nags.maximumSize,
                             nags.parentGroups.map(_.uuid))
  case Some(_) =>
    Future.unit                                   // changed, but group has no parents
```

Here `id` is `poolId.uuid` for `modifyPool` and `childId.uuid` for `modifyGroup`,
matching the current code.

## Deliberate behavior changes

- **Executor path is now atomic.** Previously the usage-task staging raced with
  commit; it is now awaited before commit.
- **No-op re-add no longer creates a redundant usage task.** Previously the task
  fired whenever `parentGroups.nonEmpty`, even when membership did not change. Now
  it fires only when a change is actually staged.

Both changes are strictly more correct. Implementation must verify that
`UpdateAllocationGroupUsageTaskSuite` and related suites do not depend on the old
no-op-fires behavior.

## Scope

- Apply the optional-executor / atomic-system-task treatment to **both** the pool
  methods (`addPool` / `removePool` / `modifyPool`) and the group methods
  (`addGroup` / `removeGroup` / `modifyGroup`). This is mechanical and shared, and
  it tees up the next `TODO.txt` item ("add-group-to-group").
- Wire **only** the `add-pool-to-group` CLI command in this work. No
  `remove-pool-from-group` or `add-group-to-group` CLI commands yet, and no new
  `removePoolFromGroup` method on `AspenClient`.

## Files affected

Production:

- `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — `Args` field, parser
  block, dispatch case, `add_pool_to_group` handler.
- `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — declare
  `addPoolToGroup` and `prepareSystemDurableTask`; update the
  `createSystemDurableTask` doc if needed.
- `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` —
  implement `addPoolToGroup` and `prepareSystemDurableTask`; refactor
  `createSystemDurableTask` to delegate.
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupState.scala`
  — `Option[TaskExecutor]` params; `mod` closures return `Option[...]`; awaited
  task-preparation with executor/system branch.
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala`
  — add `prepareSystemTask`.
- `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala`
  — add `enrollInTx`.

Tests:

- `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala`
- `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`
- `src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/GroupObjectAllocatorSuite.scala`
- `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

## Testing

- **Mechanical call-site updates:** the ~30 existing call sites that pass
  `executor` to `addPool` / `removePool` / `addGroup` / `removeGroup` change to
  `Some(executor)`.
- **System-task path:** add a test in `AllocationGroupStateSuite` exercising
  `addPool(..., None)`. This requires a registered `SystemTaskExecutorService` in
  the test harness. If the harness does not provide one, cover the client
  method's name-resolution + delegation and rely on the existing executor-path
  tests for the membership logic; note the gap explicitly.
- **No-op behavior:** add/confirm a test that a no-op re-add does not create a
  usage task.
- **CLI:** add an `add-pool-to-group` test in `MainSuite.scala` (every CLI command
  has a corresponding test — strong test-mirror coupling).

## Out of scope

- `remove-pool-from-group`, `add-group-to-group`, and `move-device-to-set` CLI
  commands (subsequent `TODO.txt` items).
- Any change to how `UpdateAllocationGroupUsageTask` performs the cascade itself.
