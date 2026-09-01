# MigratePoolToSetDurableTask — Reassigning a Pool to a New StorageDeviceSet

**Date:** 2026-08-31
**Status:** Approved design; ready for implementation planning
**Scope:** Pool-to-set reassignment only. Passive background rebalancing (the second TODO.txt
item, which would call this machinery periodically) is a separate, later feature.

## Problem

A `StoragePoolState` records the `StorageDeviceSetId` it was created against, and the set
records the pool in `assignedPools`. Nothing in the codebase changes either after pool
creation: `assignedPools` is only ever appended, in the pool-creation transaction at
`BaseAspenClient.scala:415`.

An operator therefore cannot move a pool onto different physical storage — for example, to
migrate a pool off decommissioning hardware, or to relocate the upper tiers of a distributed
B-tree onto low-latency devices. This feature adds that capability as a crash-tolerant
durable task.

`SystemTaskExecutorService` was built for exactly this consumer; its design doc
(`2026-07-23-system-task-executor-service-design.md`) names `MigratePoolToSetDurableTask` by
name.

## Goals

- An operator-triggered migration of every store in a pool onto devices of a target set.
- Works uniformly for level-0 and level-1+ target sets, with as little level-specific code as
  possible.
- Crash-tolerant: a migration resumes wherever it left off, on any host.
- One store transfer in flight at a time, gated by the existing write-threshold safety check.
- Does not fight the rebalancing subsystem.

## Non-Goals

- **Abort/rollback.** Once started, a migration runs to completion or is retargeted. See
  "Retargeting" below for why this is acceptable.
- **Optimizing away unnecessary moves.** Per TODO.txt, every store is reassigned to a
  freshly-selected device. Preserving stores already well-placed within the target set is a
  later optimization.
- **Completion notification.** Fire-and-forget, consistent with `SystemTaskExecutorService`.

## Existing Machinery This Builds On

Verified during design:

- `StorageDeviceSetState.selectDevicesForPool` already recurses through `memberSets`, so
  level-0 and level-1+ device selection is a solved problem.
- `State.getStateForRebalancePlanning` derives a set's pools from **physical residency**
  (which stores sit on member devices), not from `assignedPools`. Rebalance planning stays
  self-consistent mid-migration regardless of what a pool's pointer says.
- At `StoreManager.scala:415`, transfer completion atomically rewrites
  `poolCfg.stores(poolIndex)` to the new `(hostId, deviceId)` in the same transaction that
  clears the source device entry and marks the destination `Active`.

That last point is the foundation of this design: **the pool's own `stores` array is an
authoritative progress record.** A store is migrated iff its device is a member of the target
set. The task needs no progress list of its own, and crash recovery is free.

## Design

### 1. Data model

`StoragePoolState` gains one optional field:

```scala
object StoragePoolState:
  enum MigrationStatus:
    case InProgress, Complete
  case class Migration(targetSet: StorageDeviceSetId, status: MigrationStatus)

final case class StoragePoolState(
  ...,
  allocationGroups: List[UUID] = Nil,
  migration: Option[Migration] = None    // new
)
```

`Complete` persists rather than reverting to `None`, so a pool records its last migration.
Defaulting to `None` means `Bootstrap.scala:105` and `AspenClient.scala:213` need no changes.

Protobuf adds a `PoolMigration` message at field 11 of `StoragePoolState` (1–10 are taken).
Lockstep sites: `codec.proto`, `Codec.scala` encode/decode, `CodecRoundTripSuite`, and the
`show-pool` formatter in `Main.scala`.

### 2. Entry point

`AspenClient.migratePoolToSet(poolId, targetSetId): Future[Unit]`, implemented in
`BaseAspenClient`, as one transaction via `transactUntilSuccessfulWithRecovery`:

- **No-op** if `poolState.storageDeviceSet == targetSetId`, whether or not a migration is
  `InProgress` (TODO.txt's same-set safety check; also makes a redundant retarget harmless).
- **`StopRetrying`** if the target set does not exist.
- Otherwise write, all in the one transaction — where `previousSet` is
  `poolState.storageDeviceSet`, which is the original set on a first call and the
  now-abandoned target on a retarget:
  - pool config: `storageDeviceSet = targetSetId`, `migration = Some(Migration(targetSetId,
    InProgress))`, KeyRevision-guarded on `ConfigKey`;
  - target set: `assignedPools` gains the pool, dedup-guarded;
  - `previousSet`: `assignedPools` loses the pool, self-healing if absent (mirrors
    `StorageDeviceSetState.moveDevice`);
  - `previousSet` and target set: the pool's stores are stripped from `pendingTransfers`
    (see §6).
- **Enroll a task only on a first call** — that is, when `poolState.migration` was not
  already `InProgress`: `prepareSystemDurableTask(MigratePoolToSetDurableTask.typeUUID,
  Map(PoolIdKey -> uuid2byte(poolId.uuid)))`. A retarget reuses the running task, which
  re-reads its target from pool state each pass.

Because task enrollment stages inside the same transaction, there is no window where a pool
is marked `InProgress` with no task driving it, or a task exists for a pool that never
flipped.

The single write path means a retarget is not a special case: it flips the pointer and swaps
`assignedPools` exactly as a first call does, just from the abandoned target rather than from
the pool's original set. Only task enrollment is conditional.

**The pointer flips at the start, not the end.** The pool belongs to the target set for the
whole migration while its stores physically catch up. The reason is `selectDeviceForRebuild`:
a store lost mid-migration is rebuilt into the set the pool points at, so flipping first
means a rebuilt store lands in the target set directly rather than being rebuilt into the old
set and then migrated again.

### 3. Device selection and membership

Three level-agnostic pieces.

**Shared free-space-aware selector.** `selectRebuildDevice` already implements the wanted
policy — hard exclusion, soft exclusion with fallback, free space as a hard requirement, lazy
random scan — but is level-0-only. Extract its body into a level-aware core:

```scala
private[metadata] def selectDeviceWithSpace(
    requiredSize: Long,
    hardExclude: Set[StorageDeviceId],
    softExclude: Set[StorageDeviceId],
    lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
    freeSpaceLookup: StorageDeviceId => Future[Long],
    rng: Random): Future[StorageDeviceId]
```

Level 0 behaves exactly as `selectRebuildDevice` does today; level 1+ shuffles `memberSets`
and recurses until one yields a device. `selectDeviceForRebuild` calls it with
`hardExclude = Set(failedDevice)` and **keeps its existing level-0 guard** — this is a
refactor beneath it, not a widening of it. Migration calls a new public
`selectDeviceForStore(requiredSize, exclude, client, rng)`.

`requiredSize` is the store's current size from `StoragePoolState.getStoreUsageKey(poolIndex)`.
`softExclude` is the devices of the pool's other stores — soft rather than hard for the same
reason `selectDevicesForPool` cycles its device list: a target set narrower than the IDA width
must still work.

**Membership.** "Is this store done?" is a downward walk from the target set (`memberDevices`
at level 0, recurse `memberSets` above), computed **once per pass** and reused for all stores,
with `runBoundedParallel` for the reads.

The cheaper upward walk — read the device, take its `storageDeviceSet`, follow `parent`
looking for the target — is rejected. `StorageDeviceSetState` documents `parent` as unenforced
("enforcement is out of scope for this pass") and `moveDevice` never maintains it. More
decisively: *selection* walks downward, so a done-check that walked upward could fail to
recognize a device selection had just placed a store on, which is an infinite migration loop
rather than a stale read.

**Pool-centric planning state** for `TransferSafety`, a new sibling in `State.scala`:

```scala
def getStateForPoolMigration(client: AspenClient,
                             poolId: PoolId,
                             destinationDevice: StorageDeviceId): Future[PlanningState]
```

Reads the pool plus the device states of the pool's store devices and the candidate
destination, and builds a `PlanningState` holding one `Pool` and those devices.
`TransferSafety.isSafe` works unchanged — it needs only `devices.get(toDevice)` for the
offline-host check and `pools.get(poolId)` for the write-threshold count. It never touches set
membership, so it works at any level. This is what makes migration level-agnostic where
`getStateForRebalancePlanning` (level-0-only) could not be.

Unlike `getStateForRebalancePlanning`, no `TransferringIn` filter is needed: devices come from
`poolState.stores`, which is authoritative and updated atomically at completion, so each store
maps to exactly one device by construction.

**Source == destination.** The done-check runs before selection, so a store already on a
target-set device is never selected for, and selection can only return target-set devices —
making source == destination structurally unreachable. The task skips it defensively anyway
rather than letting `transferStore` throw `InvalidDestination`, since the pool state driving
the check can be stale. This also covers TODO.txt's case cleanly: if the old level-0 set is
already a member of a level-1 target set, every store is already in the target and the
migration completes with zero transfers.

### 4. The task

`MigratePoolToSetDurableTask extends DurableTaskFactory` with a fresh `typeUUID` and a single
state key `PoolIdKey = Key(Array[Byte](0))` — below `DurableTask.ReservedFromKeyId` and
distinct from `SimpleTaskExecutor.TaskTypeKey`, which is what `prepareSystemDurableTask`
actually writes the type UUID under. Registered in `common/TypeFactories.scala`.

The task class holds only `poolId`; the target set is re-read from pool state every pass.

`processNext()`, wrapped in `ignoreExtraCallsWhileRunning` (from `common/util`) with
cancel-then-schedule on the poll timer — TODO.txt's "protect against pileups," covered on both
the timer side and the async-body side:

1. Read the pool. If `migration` is `None` or `Complete`, finish. A re-dispatched task after
   completion is a no-op.
2. Compute target-set membership once; partition `poolState.stores` into done and remaining.
3. Remaining empty → completion transaction → finish.
4. Any of the pool's stores `TransferringIn`/`TransferringOut` → `scheduleRecheck()`. This is
   the one-at-a-time serialization and the poll-for-completion TODO.txt asks for.
5. Otherwise take remaining stores in `poolIndex` order. For the first: select a destination,
   skip defensively if it equals the current device, build the pool-centric planning state,
   check `TransferSafety.isSafe`. Safe → `transferStore` then `scheduleRecheck()`. Not safe →
   try the next remaining store; if none is safe, `scheduleRecheck()`.
6. Any failure → log at warn and `scheduleRecheck()`.

There is no wake-up message, unlike `SetRebalanceDurableTask`: TODO.txt specifies polling, and
`SystemTaskExecutorService` has no `TransferComplete` routing. Cost is up to one poll period of
latency per store. A `@volatile var pollPeriod` test seam on the companion, as
`RebalancingDurableService` has, keeps integration tests fast.

The completion transaction is revision-checked and re-reads `migration`, writing only if it is
still `InProgress` for this target — so it is idempotent.

**Duplicate execution is safe.** `SystemTaskExecutorService` can re-dispatch a stalled task
while the original still runs. If two instances select destinations for the same store,
`transferStore`'s revision-checked writes let exactly one commit; the loser retries, re-reads,
finds the store no longer `Active`, and gets `StoreNotActive` → `StopRetrying`. Logged and
rechecked. They converge.

### 5. Retargeting

`migratePoolToSet` against a pool with an `InProgress` migration replaces the target rather
than rejecting the call.

This exists because flip-at-start plus no-abort would otherwise be a trap: a migration aimed
at a set that can never satisfy it — every device too full, or a set that ends up empty —
stalls forever, with the pool already pointing at that set and `InProgress` excluding it from
rebalancing. Rejecting a second call would leave an operator no recourse short of hand-editing
pool state.

Retargeting costs nothing structurally: the running task holds only the pool id and re-reads
the target every pass, so it picks up the new target on its next tick with no task-side change
at all. An in-flight transfer lands on a device in the old target, does not count toward the
new one, and gets moved again — slightly wasteful, correct.

This is preferred over an abort path, which flip-at-start makes awkward: aborting would mean
flipping back to a set the data has partly left.

### 6. Rebalancer coordination

Migration owns a pool's placement for the duration; rebalancing continues normally for every
other pool in both sets.

The exclusion goes in one place. `State.getStateForRebalancePlanning` already reads each
pool's KVOS, so it decodes `migration` and, when `InProgress`, omits that pool from `pools`
and its stores from every `Device.stores`. `Plan.computePlan` then skips those stores across
all three phases with no changes of its own. Device `currentUsage`/`totalSize` come from
`StorageDeviceState` rather than from summing stores, so balance-phase fill ratios stay
accurate — the stores simply stop being movable candidates.

**This has a consequence that must be handled.** `SetRebalanceDurableTask.drive` also calls
`getStateForRebalancePlanning`, for transfers *already enrolled* in a set's `pendingTransfers`.
Once a pool is excluded, `TransferSafety.isSafe` does `state.pools.get(poolId)` → `None` →
never safe. An already-enrolled transfer for a now-migrating pool would sit in that list
forever, wedging that set's entire rebalance.

Two changes, because one is not enough:

- `migratePoolToSet`'s start transaction strips the pool's stores from the **old** set's
  `pendingTransfers`, and the target set's defensively. Atomic with the flip.
- `SetRebalanceDurableTask.drive` treats a pending transfer whose pool is absent from planning
  state as droppable, removing it via the existing `removePending`.

The second is the backstop for a real race: the strip and `RebalancingDurableService.enroll`
are both revision-checked on the set object, so if `enroll` commits a fresh plan just after the
strip, that plan can still contain migrating stores.

### 7. CLI

`migrate-pool <target-set-name>` in `Main.scala`, shaped like `move-device-to-set`. It kicks
off the migration and returns rather than blocking; progress is observable via `show-pool`.

## Error Handling

Everything transient — read failures, transaction failures, `AllocationError` from selection,
an unsafe transfer — logs at warn and calls `scheduleRecheck()`. The task never fails itself;
it is durable and keeps trying. Only `onStop()` completes the promise exceptionally, with
`TaskStopped`, mirroring `SetRebalanceDurableTask`.

Terminal errors are confined to the entry point and surface to the caller as `StopRetrying`:
target set does not exist. Same-set is a silent no-op, not an error.

A migration blocked on safety (a host down, dropping the pool to its write threshold) stays
`InProgress` and resumes when the condition clears. A migration blocked on space is resolved by
retargeting.

## Testing

**Unit**, no client, following `StorageDeviceSetSelectionSuite`'s injected-lookup style:

- `selectDeviceWithSpace` — level-0 preferred/fallback ordering, free space as a hard bar,
  level-1+ recursion, exhaustion → `AllocationError`.
- The membership walk — flat, nested, device absent.
- `getStateForPoolMigration` — shape and store/device attribution.
- Codec round-trip for the `migration` field, added to `CodecRoundTripSuite`.

The existing `StorageDeviceSetSelectionSuite` is the regression guard on the
`selectRebuildDevice` refactor: it must pass unchanged, since that refactor is
behavior-preserving.

**Integration** on `TestNetwork`, following `StorageDeviceSetIntegrationSuite` and
`RebalancingServiceSuite`:

- Start transaction is atomic — pointer, both `assignedPools`, status, and task enrollment all
  land together.
- Same-set call is a no-op.
- Level-0 → level-0 migration moves every store and marks `Complete`.
- Level-1+ target works by the same path.
- Old level-0 set already a member of the level-1 target → completes with zero transfers.
- Crash resume — drop the task instance, recreate from state, verify it continues.
- Retarget mid-migration redirects the running task, swaps `assignedPools` from the abandoned
  target to the new one, and does not enroll a second task.
- Retarget to the pool's current set is a no-op.
- `getStateForRebalancePlanning` omits an `InProgress` pool.
- `SetRebalanceDurableTask` drops a pending transfer whose pool has left planning state.

`MainSuite` gains the new `migrate-pool` command.

## Files Touched

| File | Change |
|---|---|
| `common/metadata/StoragePoolState.scala` | `MigrationStatus`, `Migration`, `migration` field |
| `protobuf/codec.proto` | `PoolMigration` message; field 11 on `StoragePoolState` |
| `common/network/Codec.scala` | encode/decode for the new field |
| `common/metadata/StorageDeviceSetState.scala` | `selectDeviceWithSpace`, `selectDeviceForStore`, membership walk; `selectDeviceForRebuild` refactored onto the shared core |
| `common/rebalancing/State.scala` | `getStateForPoolMigration`; exclude `InProgress` pools from `getStateForRebalancePlanning` |
| `common/rebalancing/SetRebalanceDurableTask.scala` | drop pending transfers whose pool left planning state |
| `common/metadata/management/MigratePoolToSetDurableTask.scala` | new task + factory |
| `common/TypeFactories.scala` | register the factory |
| `client/AspenClient.scala` | `migratePoolToSet` declaration |
| `client/internal/BaseAspenClient.scala` | `migratePoolToSet` implementation |
| `cmdline/Main.scala` | `migrate-pool` command; `show-pool` migration status |
