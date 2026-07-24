# SystemTaskExecutorService — Distributed DurableTask Execution Strategy

**Date:** 2026-07-23
**Status:** Approved design; ready for implementation planning
**Scope:** Infrastructure only. Consumers (e.g. `MigratePoolToSetDurableTask`, passive
rebalancing, `add-pool-to-group`) are separate, later features that build on this.

## Problem

Aspen needs a reliable, system-level way to execute focused `DurableTask`s that are **not
tied to a specific host** and whose **results are not needed by any caller** — for example,
migrating a pool to a new device set. Execution should be **spread across hosts** so no
single node is overloaded, and it must be **crash-tolerant**: if the host running a task
dies, the task must be picked up and driven to completion elsewhere.

Today, the only durable-task machinery is:
- Per-host `SimpleTaskExecutor` — runs/resumes tasks persisted in a host-local executor
  object; tied to that host.
- `RebalancingDurableService` — a leased cluster-wide singleton `DurableService` that runs
  its tasks **on the owning host**.

Neither spreads load across hosts. This feature adds a coordinator that does.

## Goals

- A cluster-wide singleton service that dispatches system durable tasks to arbitrary hosts.
- Crash tolerance via reassignment when an executing host stops making progress.
- A simple client API to create-and-enroll a system task.
- No completion notification (fire-and-forget from the caller's perspective).

## Non-Goals

- Wiring up any production consumer of the service (separate specs).
- Returning task results to callers.
- Replacing the per-host `SimpleTaskExecutor` or `RebalancingDurableService`.

## Architecture Overview

A new **`SystemTaskExecutorService`** — a leased cluster-wide singleton (modeled on
`RebalancingDurableService`) that acts as a **dispatcher**. It does not run tasks itself;
it farms each task out to a randomly-selected host and monitors liveness, reassigning on
stall.

Three actors:

1. **Service** (runs on exactly one host, via the durable-service lease): owns the task
   registry, dispatches tasks, monitors progress, reassigns on stall.
2. **Executing host** (any host): on `ExecuteSystemTask`, transiently runs the task and
   heartbeats it. Chosen execution model: **transient run** — the host reconstructs the
   `DurableTask` from the durable task-state object and drives it, but does **not** add it
   to its persistent per-host `SimpleTaskExecutor`. The service is the single source of
   durability and reassignment; this avoids double-execution races on host restart.
3. **Client** (`createSystemDurableTask`): creates the durable task-state object, enrolls it
   in the task registry, and wakes the service.

### Package placement

New package **`org.aspen_ddp.aspen.compute.systemtask`** (the service is conceptually tied
to the compute package, alongside `DurableService`, `DurableTask`, `TaskExecutor`, and
`SimpleDurableServiceExecutor`).

Exception: the `ExecuteSystemTask` **`HostMessage`** case class lives in
`common/network/Message.scala` where all `HostMessage` subtypes live, with its proto/codec
in `common.network`, so `Codec.scala` does not depend on `compute`. The service,
`SystemTaskRunner`, state helpers, and the `ServiceMessage`-carried `SystemTaskMessage` ADT
all live in `compute.systemtask`.

## Data Model

- **Service state KV object**: allocated at Bootstrap, pointed to by the services TKVL
  entry (exactly like `RebalancingDurableService`). Holds one key whose value is the **root
  pointer of a task TKVL**.
- **Task TKVL**: `Key = task UUID` → `Value = encoded KeyValueObjectPointer` of that task's
  durable state object. A TKVL (not a single-key encoded list as rebalancing uses) is chosen
  because system tasks can be numerous; a TKVL scales. Managed via `KVObjectRootManager`.
- **Per-task durable state object**: a normal `SimpleTaskExecutor`-style task object,
  carrying `SimpleTaskExecutor.TaskTypeKey → typeUUID` plus the task's initial state, so any
  host can reconstruct the `DurableTask` through the `TypeRegistry`. Its **object revision
  doubles as the heartbeat/progress signal**.
- **Service in-memory tracking** (rebuilt on failover; not durable):
  `taskId → (assignedHost, lastObservedRevision, lastChangeTime)`.

## Messages & Routing

| Message | Wire type | From → To | Purpose |
|---|---|---|---|
| `NewSystemTaskAdded(taskId)` | `ServiceMessage` payload | client → service owner | wake service to pick up a new task fast |
| `SystemTaskComplete(taskId)` | `ServiceMessage` payload | executing host → service owner | fast completion signal |
| `ExecuteSystemTask(taskId, taskStatePtr)` | **new `HostMessage` subtype** | service → arbitrary host | tell a host to run a task |

- `ExecuteSystemTask` **must** be a new `HostMessage` (not a `ServiceMessage`): a
  `ServiceMessage` is delivered only to the service *owner* (`SimpleDurableServiceExecutor`
  routes by `ownedServices.get(serviceUUID)`), whereas this must reach an *arbitrary* host.
  It is routed via `sendHostMessage` (to `msg.toHost`) and dispatched in `StoreManager`'s
  `HostMsg` match to a per-host `SystemTaskRunner`.
- `NewSystemTaskAdded` and `SystemTaskComplete` travel inside `ServiceMessage.encodedContent`
  as a self-encoded `SystemTaskMessage` ADT (mirroring `RebalancingMessage`). They are
  **best-effort wake-ups**; correctness comes from the service's polling, not delivery.

## Task Lifecycle

### Enrollment — `createSystemDurableTask` (client)

1. Look up `taskTypeUUID` in the `TypeRegistry`; verify it resolves to a
   `DurableTaskFactory` (sanity check; otherwise fail with `StopRetrying`).
2. `transactUntilSuccessful`: allocate the durable task-state object
   (`TaskTypeKey → typeUUID` + initial state), and insert `taskId → taskStatePtr` into the
   task TKVL.
3. On commit, best-effort `sendServiceMessage(NewSystemTaskAdded(taskId))`. The returned
   `Future` completes here. There is no task-completion notification.

### Dispatch (service)

- On `reconcile()` (slow poll, and on `NewSystemTaskAdded`): scan the task TKVL. For any
  task not already tracked, pick a random host from the cached host list (excluding
  `offlineHosts()`), record `assignedHost`, send `ExecuteSystemTask(taskId, ptr)`, and
  schedule the first progress check ~30s out.

### Execution (host — `SystemTaskRunner`)

- On `ExecuteSystemTask`: if not already running that `taskId`, read the task-state object,
  reconstruct the `DurableTask` via the `TypeRegistry` factory, and drive it **transiently**
  (not added to the persistent per-host `SimpleTaskExecutor`).
- Immediately issue a `bumpVersion` transaction on the task-state object (claim + liveness
  signal).
- A single host-level background task every **15s** issues a `bumpVersion` on **all**
  task-state objects the host is currently running (the heartbeat).
- On the task's `completed` future: in one transaction, delete the task's TKVL entry and
  decrement the state object's refcount; drop it from the runner; then
  `sendServiceMessage(SystemTaskComplete(taskId))`.

### Monitoring & Reassignment (service)

- ~30s after dispatch, begin polling each tracked task's state-object revision
  (pileup-guarded).
- If the state object is **deleted / not found** → task complete; drop tracking.
- If the revision is **unchanged for 45s** → reassign: pick a new random host, send a fresh
  `ExecuteSystemTask`, reset timers.

### Safety property: duplicate execution is safe

Reassignment can transiently cause two hosts to run the same task (a slow-but-alive host
whose heartbeat lapsed, plus the reassigned host). This is **safe**:
`DurableTask`/`SteppedDurableTask` use revision-checked writes, so duplicate execution cannot
corrupt state — it only wastes work, which the heartbeat minimizes. Tasks created through
this service must therefore be idempotent, which is already a `DurableTask` requirement.
Whichever runner finishes first deletes the TKVL entry; the other's completion is a no-op.

## Client API

Add to `AspenClient` (abstract) and implement in `BaseAspenClient`:

```scala
def createSystemDurableTask(taskTypeUUID: UUID,
                            initialState: Map[Key, Array[Byte]]): Future[Unit]
```

Retries until the task-state object is created and enrolled in the task TKVL; the `Future`
completes at successful enrollment. No completion signal is provided.

## Bootstrap & Registration

- **`server/store/Bootstrap.scala`**: allocate the service's state KV object (initial content
  = an empty task TKVL root) and add an unclaimed `ServiceEntry` to the services tree,
  alongside `RebalancingDurableService`.
- **`server/TypeFactories.scala`**: add `SystemTaskExecutorService` so
  `SimpleDurableServiceExecutor` can resolve and claim it.
- No `common/TypeFactories.scala` change for production (infrastructure-only). The test-only
  task registers its factory through the test harness.

Note: `Bootstrap.initialize` runs once per cluster lifetime; this path covers new clusters.
Existing clusters would require `SimpleDurableServiceExecutor.registerService`, which is out
of scope here.

## Pileup Guards & Configurability

Periodic operations must guard against pileups, since the system can experience long periods
where objects are unreadable/unwritable:

- Service `reconcile` poll: `scheduleNonConcurrentPollingTask` (default ~2 min, matching
  rebalancing).
- Per-task monitoring poll and the host heartbeat: guarded with
  `ignoreExtraCallsWhileRunning` / non-concurrent scheduling so slow cycles cannot stack.
- Host-list cache refresh: every 30 min.

Tunables are exposed as overridable fields (test seams, like
`RebalancingDurableService.pollPeriod`): reconcile period, first-check delay (30s), stall
timeout (45s), heartbeat interval (15s), host-cache TTL (30 min).

## Testing Strategy

- A **test-only `DurableTask`** (e.g. a counter/marker task) with its factory registered via
  the test harness, exercising: enroll → dispatch → execute → complete; heartbeat keeps a
  slow task assigned; a stalled heartbeat triggers reassignment; duplicate execution is safe.
- Unit tests for `SystemTaskMessage` encode/decode and `ExecuteSystemTask` codec round-trip
  (mirror `RebalancingMessageSuite`).
- Integration test through `TestNetwork` (register the service factory), following existing
  rebalancing test patterns.

## Files to Change

**New (`compute/systemtask/`)**
- `SystemTaskExecutorService.scala` — service + `DurableServiceFactory` companion.
- `SystemTaskServiceState.scala` — task-TKVL root helpers / initial state.
- `SystemTaskMessage.scala` — `SystemTaskMessage` ADT (`NewSystemTaskAdded`,
  `SystemTaskComplete`) + encode/decode.
- `SystemTaskRunner.scala` — per-host transient executor + heartbeat.

**Edited**
- `common/network/Message.scala` — add `ExecuteSystemTask` `HostMessage`.
- `src/main/protobuf/codec.proto` — add proto messages for `ExecuteSystemTask` and
  `SystemTaskMessage`.
- `common/network/Codec.scala` — encode/decode for the new proto messages.
- `server/StoreManager.scala` — dispatch `ExecuteSystemTask`; construct/own a
  `SystemTaskRunner`; drive the 15s heartbeat.
- `server/store/Bootstrap.scala` — allocate service state + register service entry.
- `server/TypeFactories.scala` — register `SystemTaskExecutorService`.
- `client/AspenClient.scala` — abstract `createSystemDurableTask`.
- `client/internal/BaseAspenClient.scala` — implement `createSystemDurableTask`.
- `src/test/scala/.../TestNetwork.scala` — test wiring (register service + test task factory).

## Reference Patterns (existing code to mirror)

- `common/rebalancing/RebalancingDurableService.scala` — service structure, lease, reconcile
  loop, `initialServiceState`, enroll/remove transactions.
- `common/rebalancing/SetRebalanceDurableTask.scala` — driving a single task with a
  poll/wake loop and `Promise` completion.
- `common/rebalancing/RebalancingMessage.scala` + `RebalancingMessageSuite` — `ServiceMessage`
  payload ADT + codec + tests.
- `compute/impl/SimpleTaskExecutor.scala` — reconstructing a `DurableTask` from a task-state
  object via `TypeRegistry` (`TaskTypeKey`).
- `compute/impl/SimpleDurableServiceExecutor.scala` — lease/claim/renew, `deliverMessage`
  routing.
- `server/StoreManager.scala` — `HostMsg` dispatch, per-host background tasks,
  `taskExecutorPromise`/`serviceExecutorPromise`.
