# Rebalancing Active Flow Design

## Goal

Implement the on-demand rebalancing of a single storage device set: build a transfer
plan for the set and drive it to completion, one store transfer at a time. This is the
active plan-and-execute flow (TODO item "Rebalancing"). The passive background
rebalancer that continuously watches %usage is explicitly **out of scope** for this
iteration.

Built on the existing system-level durable service framework
(`SimpleDurableServiceExecutor`), the stoppable `DurableTask` framework, and the
`common.rebalancing` `State`/`Plan` modules.

## Scope

- `RebalancingDurableService` + factory — the lease-held singleton service.
- `SetRebalanceDurableTask` + factory — one custom `DurableTask` per in-flight set rebalance.
- `RebalancingDurableService.rebalanceStorageDeviceSet(setId)` companion entry point.
- A `rebalance <setId>` cmdline subcommand.
- A pluggable store-transfer mechanism so transfers are exercisable in the in-memory test framework.

Not in scope: passive/continuous rebalancing, set expansion, set reassignment, parallel transfers.

## Data Model

### `StorageDeviceSetState.pendingTransfers`

Add `pendingTransfers: List[(StoreId, StorageDeviceId, StorageDeviceId)]` (store, from-device, to-device).
This is the plan the task drains. Requires:

- New Scala field on the `StorageDeviceSetState` case class.
- A `PendingTransfer` message + repeated field on the `StorageDeviceSetState` message in `codec.proto`.
- `encode`/`decode` updates in `Codec.scala`.
- Default-to-empty for pre-existing data (old objects lack the field).
- `TestNetwork`'s `bootstrapSD` construction updated for the new field.

### `RebalancingDurableService` state object — `ActiveRebalancingTasks`

The service's KV state object (at its `statePointer`) holds one key/value pair,
`ActiveRebalancingTasks` = `List[(StorageDeviceSetId, KeyValueObjectPointer)]` — the second
element is the pointer to that task's (minimal) state object. This is the registry the
service polls to discover and resume tasks. Encoded/decoded with a small helper following
existing `Codec` patterns.

Each `SetRebalanceDurableTask` has a minimal KV state object (holding just its `setId`),
allocated by `rebalanceStorageDeviceSet`. Its purpose is to satisfy the `DurableTask`
`taskPointer` contract and to be the durable, refcount-tracked anchor the service points at;
all *progress* lives in the set's `pendingTransfers`, so the object's contents are otherwise
vestigial.

**Two lists, two purposes:** `pendingTransfers` on the *set* is the plan a task drains;
`ActiveRebalancingTasks` on the *service* is the registry the service uses to discover
and resume tasks.

## RebalancingDurableService

Implements `DurableService`. Constructed by `RebalancingDurableServiceFactory.createService`
when this host wins the lease inside `SimpleDurableServiceExecutor`.

On construction:

- Holds a `Map[StorageDeviceSetId, SetRebalanceDurableTask]` of the tasks it is driving.
- Reads `ActiveRebalancingTasks` and directly constructs a `SetRebalanceDurableTask` for each
  `(setId, taskStatePointer)` to resume in-flight work. (No `SimpleTaskExecutor`; the service
  manages task instances directly.)
- Starts a `scheduleNonConcurrentPollingTask` (~2 min) on `client.backgroundTaskManager`
  that re-reads state and reconciles: start tasks newly present in `ActiveRebalancingTasks`,
  and drop tasks whose completion has been observed.

`receiveMessage`:

- `NewSetRebalanceInitiated(setId)` — re-read state immediately and start any task not
  already running. The message is an early wake-up; the poll is the correctness fallback.
- `TransferComplete(setId, storeId, from, to)` — route to the matching running
  `SetRebalanceDurableTask` to trigger an immediate re-poll.

Task completion: each `SetRebalanceDurableTask.completed`, on success, causes the service
to remove that `(setId, taskPointer)` from `ActiveRebalancingTasks` (revision-checked
transaction) and drop the local instance. Using the completion future is simpler than
detecting deletion during polling.

`shutdown()` (lease loss / host shutdown): call `stop()` on every task in the map (they fail
with `TaskStopped`), cancel the poll task, and clear local state. The next lease winner
reconstructs everything from `ActiveRebalancingTasks` + each set's `pendingTransfers`.

`typeUUID`: a fresh, hard-coded UUID, registered in `server/TypeFactories.scala`, verified
not to collide with existing factory UUIDs.

## SetRebalanceDurableTask

A custom `DurableTask` (not `SteppedDurableTask` — the number of transfers is dynamic and
the durable state lives in the set's `pendingTransfers`, and the long async
poll-for-completion wait does not fit `SteppedDurableTask`'s per-step-transaction model).

Constructed by the service with its `taskPointer` (= `DurableTaskPointer(taskStatePointer)`),
the `client`, the `setId`, and a reference to the service (for polling helpers and message
wake-ups). Serial: one transfer in flight at a time.

Main loop (`processNext`):

1. If `isStopped` → `promise.failure(new TaskStopped)`, return.
2. Read `StorageDeviceSetState`. If `pendingTransfers` is empty → `promise.success(None)`, done.
3. Re-derive planning state via `State.getStateForRebalancePlanning` to evaluate current safety.
4. Pick the first pending transfer that is **safe now** (write-threshold OK — see below) and
   whose destination host is online.
   - If an entry already appears **in flight** (destination store `TransferringIn`, or source
     `TransferringOut`), treat it as the active one and jump to step 6 (crash-recovery/resume case).
   - If none are safe, schedule a re-check via the non-concurrent poller and return (skip/revisit).
5. Initiate `client.transferStore(storeId, toDevice)`.
6. Poll the destination `StorageDeviceState` until the store is `Active` and the source entry
   is gone. `TransferComplete` wakes this early; the poller is the fallback.
7. In one revision-checked transaction on the set state, remove that entry from `pendingTransfers`.
8. Loop to step 1.

`onStop()`: cancel any outstanding poll timer. In-flight transfers are **not** aborted — they
continue on the hosts; the resuming task re-detects them at step 4.

Idempotency / crash safety: an entry stays in `pendingTransfers` until its transfer fully
completes, so a crash at any point resumes by re-reading the list and re-detecting in-flight
work. Re-initiating an already-in-progress `transferStore` is avoided by the in-flight check
in step 4.

### Blocked-transfer policy: skip, continue, revisit

When a transfer cannot proceed (would drop the pool below its write threshold, or the
destination is temporarily offline), it is skipped for now; the task attempts other safe
pending transfers and revisits blocked ones on later cycles. The task completes only when
every transfer lands. Blockages are assumed transient.

### Write-threshold safety check

Before initiating a candidate transfer:

- Add stub `AspenClient.offlineHosts(): Set[HostId] = Set()`.
- A store is "usable" for its pool iff its status is `Active` **and** its host is not in
  `offlineHosts()`. Stores that are `TransferringIn` or otherwise non-`Active` count as offline.
- The candidate is safe iff, treating the to-be-moved store as momentarily offline during
  `TransferringOut`, `usableStores(pool) - 1 >= pool.ida.writeThreshold`.

## Entry Point

`RebalancingDurableService.rebalanceStorageDeviceSet(client, setId): Future[Unit]`:

1. Read `StorageDeviceSetState` (a DataObject). If `pendingTransfers` non-empty → return success (already running).
2. Locate the service state object: read the services TKVL (`Radicle.ServicesTreeKey`) entry for
   the fixed `RebalancingDurableService.ServiceUUID` → `ServiceEntry.decode` → `statePointer`. Read it and
   decode `ActiveRebalancingTasks`. If `setId` present → return success.
3. Build the plan: `State.getStateForRebalancePlanning` → `Plan.computePlan`. If empty → return success.
4. In a **single transaction**:
   - Overwrite the set (DataObject) with the plan written into `pendingTransfers` (revision requirement on the set state).
   - Allocate a minimal task state KV object (via `PoolObjectAllocator` on `Radicle.poolId`) holding `setId`.
   - Add `(setId, taskStatePointer)` to `ActiveRebalancingTasks` (revision requirement on the service state kv pair).
5. After commit, send `NewSetRebalanceInitiated(setId)` to the service (best-effort wake-up).

Revision requirements make concurrent callers safe: the loser's transaction aborts and
retries, sees the populated list, and returns success.

## Messages

Both carried via `ServiceMessage` (added to `common.network.Message` + `Codec`):

- `NewSetRebalanceInitiated(setId)` — companion/client → service.
- `TransferComplete(setId, storeId, fromDevice, toDevice)` — destination host → service.
  Requires wiring the destination transfer-completion path (`StoreManager`, after
  `updateStateForTransferredStore`) to emit it.

Both are latency optimizations only; correctness comes from polling.

## CLI

A `rebalance <setId>` subcommand in `cmdline` that resolves the client and calls
`rebalanceStorageDeviceSet`.

## Pluggable Transfer Mechanism (for testability)

`TransferringIn`/`TransferringOut` are built entirely on the real filesystem and `jar`
subprocesses and are the only such touchpoints, instantiated solely inside `StoreManager`.
The in-memory `TestNetwork` cannot run them. To make the real transfer flow exercisable
in tests, introduce a seam.

### Production seam

- Extract traits `StoreTransferIn` / `StoreTransferOut`. Members already exist on the concrete
  classes: `storeId`, `transferUUID`, `complete`, `dataReceived` (in), `abort` (out). The
  trait `complete` returns `Future[Unit]` (`StoreManager` ignores the resolved value).
- Introduce `StoreTransferFactory` with `createTransferIn(...)` / `createTransferOut(...)`,
  and an object `StoreTransferFactory.Filesystem` wrapping the current classes (behavior unchanged).
- `TransferringIn` / `TransferringOut` become the `Filesystem` implementation.
- `StoreManager` gains a constructor param `storeTransferFactory: StoreTransferFactory =
  StoreTransferFactory.Filesystem`, and replaces its two `new Transferring…` sites and the
  `transferringOut` / `transferringInUUIDs` field types with the traits. No production behavior change.

### In-memory implementation (test sources)

In the harness every store is a process-resident `MapBackend` on a single `StoreManager`, so
moving a store between devices requires **no byte movement** — only the metadata flip:

- `InMemoryStoreTransferOut`, on creation, sends a single empty `StoreTransferData` to the
  destination (reusing existing plumbing), then completes.
- `InMemoryStoreTransferIn.dataReceived(empty)` completes without any filesystem move.
- This drives `StoreManager`'s real `startStoreTransferIn` → `updateStateForTransferredStore`
  path unchanged — the flow tested is the real one minus disk/`jar`.

### TestNetwork changes

- Bootstrap a **second storage device** as a transfer target (with `loadStore`).
- Inject `InMemoryStoreTransferFactory` into `StoreManager`.
- **Loopback-deliver** the transfer-related host messages (`CheckStorageDevice`,
  `StartStoreTransfer`, `StoreTransferData`) to `smgr` (single host = source = dest) rather than
  only capturing them, without breaking existing tests that assert on `capturedHostMessages`
  (deliver transfer types, keep capturing the rest).

## Testing

**Unit:**

- `pendingTransfers` codec round-trip, including default-empty for old data.
- `ActiveRebalancingTasks` encode/decode.
- The write-threshold safety predicate against crafted `PlanningState`s.
- (`Plan.computePlan` is already pure and separately tested.)

**Integration** (following `DurableServiceSuite` + `StorageDeviceUsageManagerSuite` patterns):

- Register `RebalancingDurableService` via `SimpleDurableServiceExecutor` with short timings.
- Call `rebalanceStorageDeviceSet(setId)` on a set with a crafted imbalance across the two devices.
- Drive with `waitForTransactionsToComplete()`; assert `pendingTransfers` drains, each store lands
  `Active` on its target device, the task's `completed` resolves, and `ActiveRebalancingTasks` empties.
- A lease-loss / re-acquire case: assert a mid-flight rebalance resumes and finishes.

## Files Changed

| File | Change |
|---|---|
| New: `common/rebalancing/RebalancingDurableService.scala` | Service + factory + `rebalanceStorageDeviceSet` companion |
| New: `common/rebalancing/SetRebalanceDurableTask.scala` | Custom `DurableTask` + factory |
| New: `server/transfer/StoreTransferFactory.scala` | `StoreTransferIn`/`StoreTransferOut` traits, factory, `Filesystem` impl |
| `common/metadata/StorageDeviceSetState.scala` | Add `pendingTransfers` field |
| `common/network/Codec.scala` | Encode/decode `pendingTransfers`, `ActiveRebalancingTasks`, new messages |
| `common/network/Message.scala` | `NewSetRebalanceInitiated`, `TransferComplete` service messages |
| `src/main/protobuf/codec.proto` | `PendingTransfer` message + `StorageDeviceSetState` field; new messages |
| `server/transfer/TransferringIn.scala` | Implement `StoreTransferIn` trait |
| `server/transfer/TransferringOut.scala` | Implement `StoreTransferOut` trait |
| `server/StoreManager.scala` | Accept `StoreTransferFactory`; use factory; emit `TransferComplete` |
| `client/AspenClient.scala` (+ `BaseAspenClient`) | `offlineHosts()` stub |
| `server/TypeFactories.scala` | Register `RebalancingDurableServiceFactory` (service only; the task is managed directly, not via a factory) |
| `cmdline/...` | `rebalance <setId>` subcommand |
| `src/test/.../TestNetwork.scala` | Second device, in-memory transfer factory, loopback host-message delivery |
| New: `src/test/.../rebalancing/RebalancingServiceSuite.scala` | Integration tests |
| New: `src/test/.../rebalancing/` unit tests | Codec round-trips + write-threshold safety predicate |

## Design Decisions

- **Custom `DurableTask`, plan on the set** (Approach A): matches the data model, makes
  skip/continue/revisit and crash-resume fall out naturally, and avoids forcing the long
  poll-for-completion wait into `SteppedDurableTask`'s per-transaction step model.
- **Poll is the source of truth; messages are wake-ups**: robust to lost messages, bounded
  latency, no correctness dependency on delivery.
- **Skip/continue/revisit** rather than head-of-line blocking or permanent skip: honors the
  full plan while tolerating transient threshold/availability conditions.
- **Pluggable transfer mechanism** rather than a pure test-only metadata workaround: exercises
  the real `StoreManager` transfer flow (minus disk/`jar`) and is reusable, at the cost of a
  contained production refactor behind a default-valued constructor param.
- **Serial transfers**: one at a time; parallelization is explicitly deferred.
