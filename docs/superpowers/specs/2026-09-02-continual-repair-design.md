# Continual Repair Service

**Status:** approved design, not yet implemented
**Scope:** replaces the `TODO.txt` continual-repair entry

## Problem

When a store is offline or otherwise fails to process a transaction, the client's
`MissedUpdateFinalizationAction` records the failure in the enclosing pool's `errorTree`, keyed
by `poolIndex :: objectId` (17 bytes, big-endian) with the object's `storePointer` bytes as the
value. Nothing consumes those entries. `Main.repair` implements a full-pool repair pass and
self-reschedules every 30 seconds, but no caller ever starts it, so in practice missed updates
accumulate forever and every one of them is a replica the pool no longer has.

The fix is a service the Host runs continually over the stores it owns. The difficulty is load:
a host may hold hundreds or thousands of stores, and a repair pass costs reads whether or not it
finds anything. Aspen tolerates transient failures well, so repair need not be fast — but every
hour an out-of-date replica stays out of date raises the odds of losing data.

## Design

### Package layout

New package `org.aspen_ddp.aspen.server.repair`:

| Unit | Responsibility |
|---|---|
| `RepairPolicy` | Pool-scoped timing: `scanIntervalFloor`, `scanIntervalCap`, `minErrorEntryAgeForDeletion`. Companion holds `Default` and codec. |
| `HostRepairLimits` | Host-scoped load ceiling: `maxConcurrentStoreScans`. |
| `RepairTarget` | Trait: the three operations the service needs from a `Host`. |
| `StoreRepairer` | One store, one scan. Walks that store's errorTree range, repairs each entry, returns a `ScanResult`. |
| `RepairService` | Scheduling: deadline map, sweep tick, bounded parallelism, policy caching, start/cancel. |

`RepairTarget` exists to keep the service off the whole of `Host`:

```scala
trait RepairTarget:
  def repairableStoreIds: List[StoreId]
  def repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit
  def repairDelete(storeId: StoreId, objectId: ObjectId,
                   storePointer: Array[Byte], completion: Promise[Unit]): Unit
```

`Host` is a large class with an event loop and a device-discovery constructor. Depending on the
trait instead means `StoreRepairer` and `RepairService` test against a recording fake, and the
Host-side change is three small methods.

### Policy in system metadata

Policy lives in Aspen metadata rather than a config file, following the project's preference for
state that entities poll. It is split by cause: staleness risk is a property of the pool's IDA,
while the load ceiling is a property of the machine.

- Pool timing → new `StoragePoolState.RepairPolicyKey = Key(Array[Byte](5))`, a codec.proto
  message. There is precedent for auxiliary keys on the pool object outside the protobuf
  `ConfigKey` (the `sz<idx>` usage keys hold raw longs), but this record has three fields and is
  likely to grow, so it is encoded like the other metadata records.
- Host ceiling → new `HostState.RepairLimitsKey = Key(Array[Byte](2))`.

Two rules make this safe:

1. **An absent key is normal.** No existing pool or host has these keys and nothing is required
   to write them. Absent means `RepairPolicy.Default` / `HostRepairLimits.Default`, so the
   feature works with zero administration.
2. **An unreadable key never stops repair.** The service caches last-known-good policy per pool
   and falls back to defaults on a failed read, logging at debug. Repair is the service that
   must keep working while the system is unhealthy; the config read is advisory, never a gate.

   **"Unreadable" means a read that never returns, not only one that fails.** Aspen reads retry
   indefinitely — `SimpleReadDriver` settles only on success or a fatal `ObjectReader` error — so
   an unavailable object yields a future that never completes rather than one that fails. A
   `recover` block therefore does not implement this requirement on its own: it handles the case
   that does not happen. Every metadata read on the sweep's critical path must be bounded by a
   deadline, with the fallback taken on timeout. This applies to any future component that reads
   system metadata on a periodic path, not just to repair: an unbounded read inside a
   single-flight periodic task converts one unavailable object into a permanent, silent stall of
   that task for the life of the process.

Defaults: floor 30s, cap 15min, deletion age 60s (preserving today's
`Main.MinErrorEntryAgeForDeletion` exactly), `maxConcurrentStoreScans` 4.

### Pacing

Each store carries a `ScanState(nextDue, currentInterval)`.

- A tick at a fixed host-wide period, via
  `backgroundTasks.scheduleNonConcurrentPollingTask("repair-sweep", ...)`. The period is a
  constant equal to `RepairPolicy.Default.scanIntervalFloor` (30s), *not* a per-pool value:
  `schedulePeriodic` fixes its period at construction, and pool policies differ. A pool whose
  floor is set below the tick period is therefore effectively rounded up to it; floors above it
  work exactly as configured, which is the direction that matters, since the knob exists to slow
  repair down. `stallAfter` must be generous: a sweep of a thousand stores through a concurrency
  cap of four is legitimately slow and must not be reported to an operator as a stall.
- Each tick snapshots `repairableStoreIds`, drops states for departed stores, admits new ones,
  selects the due set, groups it by pool so one pool read serves several stores, then runs it
  through `runBoundedParallel(due, maxConcurrentScans)`.
- A clean scan sets `interval = min(interval * 2, cap)`. A scan that finds any entries sets
  `interval = floor`. The next due time gets ±25% jitter.
- A newly admitted store starts at the floor, due at a random offset within the first floor
  interval — eager, because a store that just came online is the likeliest to have missed
  updates, but spread, so a host restart with a thousand stores does not make them all due on
  the same instant.
- `repairableStoreIds` excludes offline stores, stores being rebuilt, and stores transferring
  out. A store mid-rebuild is already having everything read; a store transferring out is about
  to stop being ours.

Two implementation traps:

- `runBoundedParallel` **fails fast**. Each store's scan must be individually recovered or a
  single bad store aborts the whole sweep.
- The sweep must stay on the `BackgroundTaskManager` execution context. `Host.repair` enqueues
  to the Host event loop, so calling it from that loop would deadlock.

### Per-entry repair

The flow is today's `Main.repairOne`, with its semantics unchanged:

1. Decode the `ObjectId` from key bytes `[1,17)`, skipping the `poolIndex` byte.
2. `pool.allocationTree.get(Key(objectId.toBytes))`.
3. **Allocation present** → decode the `ObjectPointer`, `client.read` it, call
   `target.repair(storeId, os, promise)`, await, then delete the errorTree entry guarded by
   `KeyValueUpdate.TimestampLessThan(key, os.timestamp)`. The guard is retained deliberately: a
   bumped timestamp means another update was missed between the read and the write, and letting
   the delete fail is how the next pass learns to try again.
4. **Allocation absent** → apply the age guard. Eligible: call
   `target.repairDelete(storeId, objectId, entry.value.bytes, promise)`, await, then delete the
   entry unconditionally. Not eligible: leave the entry, log, count it deferred. An absent
   allocation means either the object was deleted or its `AllocationFinalizationAction` has not
   landed; only entry age tells them apart, and dropping the guard silently costs replicas. A
   future-dated entry yields a negative age and stays ineligible — clock skew is not evidence of
   a deletion.

An empty errorTree value means "delete by ObjectId alone" and is passed through to
`repairDelete` as such.

Three changes on the way across from `Main`:

- **Both delete transactions gain `tx.disableMissedUpdateTracking()`.** Today's
  `deleteErrorEntry` and `deleteErrorEntryByTimestamp` do not call it. Under a one-shot CLI pass
  that was survivable; under a service that runs forever it is not — a delete whose own commit
  misses a store writes a fresh errorTree entry for the errorTree node, which the next sweep
  repairs, which writes another. This is a live bug being fixed, not merely relocated code.
- `println` becomes `logger`: info on a completed repair, debug on a deferral.
- The walk tallies seen / repaired / deferred into `ScanResult`, which drives the pacing
  decision.

Per-entry failures need no new handling: `KeyValueListNode.foreachInRange` already logs a
failing callback and continues to the next key.

**Scan-level failure.** When the walk itself fails (a failed tree read fails its future), the
store backs off as if the scan were clean, and the failure is logged at warn. A failed read says
nothing about whether the store is damaged, and retrying hard against a pool that cannot be read
is exactly the load this design exists to avoid. The cost is that a damaged store behind a flaky
pool object drifts out to the cap. A separate, shorter error cap was considered and rejected as
more knobs than the case warrants.

### Store departure mid-scan

A store can leave a host at any moment — `ShutdownStore` (which a transfer-out triggers) removes
it from `stores` and adds it to `offlineStores`. A scan already walking that store's errorTree
range must not keep trying to write to it, and must not delete entries it did not actually
repair. Excluding departed stores at admission time is not enough, because a sweep can be
minutes long.

**Completion must not hang.** Today `Host.handleEvent` does:

```scala
case Repair(storeId, os, completion) => stores.get(storeId).foreach: store =>
  store.repair(os, completion)
```

If the store is gone the promise is never completed. `StoreRepairer` awaits it, that entry's
callback never completes, `foreachInRange` never advances, the store's scan future never
completes, and `boundedSingleFlight` holds it in flight forever — with `maxInFlight = 1` the
`repair-sweep` tick is dropped from then on. A single transfer-out at the wrong instant would
silently stop repair for every store on the host until the process restarts. This is latent only
because nothing calls `Host.repair` today.

**Both events fail the promise instead.** Add a `StoreNotHosted` error; `Repair` and
`RepairDelete` both complete with `Failure(StoreNotHosted(storeId))` when the store is absent
from `stores`. `StoreRepairer` then leaves the errorTree entry in place, which is the point: the
store still exists, it just belongs to another host now, and that host's sweep is what will
repair it. `foreachInRange` already logs a failing callback and continues, so no new per-entry
handling is needed.

This changes existing behavior. `RepairDelete` currently *succeeds* the promise for an unhosted
store, with a comment explaining that the alternative would block the caller forever. Succeeding
also causes `StoreRepairer` to delete the errorTree entry, so no host ever deletes the object
from the moved store and the slice leaks. Failing the promise resolves the hang the comment was
addressing without that cost.

**Cooperative abandonment.** `StoreRepairer` re-checks ownership before each entry and no-ops the
remainder of the walk once the store has departed, rather than doing a per-object read and two
transactions apiece for entries it cannot repair. It cannot exit the walk outright:
`foreachInRange` has no early termination, so the remaining tree-node reads still happen. Those
are cheap next to what is being skipped, and the sweep is bounded either way.

**Scan state.** The departed store's `ScanState` is dropped at the next tick, so if it ever
returns it is re-admitted at the floor — correct, since a store that has been away is a store
likely to have missed updates.

### Host integration

- `Repair` gains the unhosted-store branch described above, mirroring `RepairDelete`.
- New `Host.repairableStoreIds: List[StoreId]` — `stores.keySet` less `offlineStores`,
  `rebuildingStores.keySet`, and `transferringOut.keySet`, under `synchronized`. `Host`
  implements `RepairTarget`; its existing `repair` and `repairDelete` already have the right
  shapes.
- `RepairService` is constructed alongside `heartbeatTask` and `checkStorageDeviceTask` rather
  than waiting on task-executor initialization. Early sweeps simply find few stores, and the
  policy fallback covers metadata reads that are not ready yet.
- Its `ScheduledTask` is cancelled in `Host.shutdown()` with the others.

### Removals

`Main.repair`, `Main.errorEntryMayBeDeleted`, `Main.MinErrorEntryAgeForDeletion`, and their
orphaned imports are deleted; the logic moves into `aspen.server.repair`. `MainSuite`'s three
`errorEntryMayBeDeleted` tests move to the new package's suite. There is no CLI repair command:
the service is the only driver, so there is one code path and nothing to drift. The `TODO.txt`
entry is removed in the same commit.

## Testing

Test-driven, following the existing suites.

- `RepairPolicySuite` — codec round-trip; an absent key yields `Default`; a failed read yields
  last-known-good, then `Default`.
- Pacing, with an injected clock and no I/O: clean scans double to the cap and stop; a scan that
  finds entries snaps back to the floor; jitter stays within ±25%; a newly admitted store lands
  inside the first floor interval; departed stores are dropped from the map.
- `StoreRepairerSuite` over `TestNetwork` with a recording `RepairTarget` — the update path, the
  aged-deletion path, the fresh-deferral path, and empty-storePointer reaching `repairDelete` as
  empty bytes. Entries are seeded directly into the errorTree (through a transaction with
  missed-update tracking disabled) rather than by provoking a real missed update, since
  `TestNetwork`'s three stores always answer.
- The recursion fix gets an observable assertion: after a sweep drains the tree, a second sweep
  finds it still empty. Were the delete transactions tracking missed updates, the tree would
  refill.
- `RepairServiceSuite` — a counting fake proves concurrent scans never exceed the cap, and one
  store whose scan fails does not abort the sweep for the rest.
- Store departure, the case most likely to regress:
  - `Host` completes a `Repair` for an unhosted store with `Failure(StoreNotHosted)` rather than
    leaving the promise open. Asserted directly against a `Host`, since the hang it prevents is
    invisible from `StoreRepairer`'s side.
  - Same for `RepairDelete`, replacing the current assertion of success if one exists.
  - A failed repair leaves the errorTree entry in place — the entry is still there after the
    scan, for both the update and the deletion path.
  - A store removed mid-scan causes the remaining entries to be skipped: the recording
    `RepairTarget` sees no further `repair` calls after the store leaves.
  - The sweep survives it — a subsequent tick still runs, which is what proves the
    single-flight slot was released.

## Out of scope

No CLI trigger. No metrics beyond logs. No per-pool concurrency cap. No store-side "I know I
missed something" hint.
