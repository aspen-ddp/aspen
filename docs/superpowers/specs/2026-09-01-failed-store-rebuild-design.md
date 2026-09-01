# Failed Store Rebuild — Tombstoning a Device and Reconstructing Its Stores

**Date:** 2026-09-01
**Status:** Approved design; ready for implementation planning
**Scope:** Operator-triggered failure of a whole storage device, and reconstruction of every
store that lived on it. Automatic failure detection, partial-device failure, and background
scrubbing of stale slices are out of scope.

## Problem

A storage device can die. Nothing in the system handles that today.

The only reconstruction code that exists is `Main.rebuild` (`cmdline/Main.scala:1255`), marked
`OBSOLETE: not reachable from the CLI` in its own comment. It hardcodes the demo bootstrap
host's device path, is single-shot, has no crash recovery, and prints to stdout. It does
demonstrate the essential mechanic — walk the pool's allocation tree, read each object through
the client, ask the resulting `ObjectState` for this store's slice, and hand it to
`Backend.rebuildWrite` — but nothing around that mechanic is usable.

Meanwhile `StorageDeviceState.StoreStatus` already has a `Rebuilding` case. It round-trips
through the codec and the rebalancer already treats it as immovable, but nothing in the
codebase ever produces or consumes it. This feature fills that hole from both ends.

## Goals

- An operator can declare a device dead with one CLI command and walk away.
- Every store on the dead device is reconstructed onto a live device in the pool's set, one at
  a time per device, crash-tolerantly.
- A rebuild that dies partway resumes near where it stopped rather than from the beginning.
- Nothing can be transferred to, or allocated on, a device that has been declared dead.
- Reconstruction reuses the transfer-in machinery's shape so there is one way to think about
  "a store is arriving on this device."

## Non-Goals

- **Automatic failure detection.** Declaring a device dead is an operator decision. The
  system offers no opinion about when a device has failed.
- **Un-failing a device.** The tombstone is one-way. A recovered device is re-introduced as a
  new device with a new id.
- **Partial failure.** A device is entirely alive or entirely dead. There is no per-store or
  per-object failure declaration.
- **Healing objects written during the rebuild.** See "Residual staleness" below.
- **Reassigning a rebuild whose destination fills up.** See error handling.

## Existing Machinery This Builds On

Verified during design:

- `StorageDeviceSetState.selectDeviceForRebuild` (`StorageDeviceSetState.scala:142`) already
  implements replacement-device selection: free space as a hard requirement, the failed device
  hard-excluded, the pool's other devices soft-excluded. It was written in anticipation of this
  feature and currently has no callers.
- `Backend.rebuildWrite` / `Backend.rebuildFlush` are the store-side write path, already
  present on every backend.
- `ObjectState.getRebuildDataForStore(storeId)` computes one store's slice from a
  client-side read, so reconstruction needs no store-to-store protocol.
- `StoreManager`'s device-check machinery — `checkStorageDevice`, the
  `activeDeviceChecks`/`deferredDeviceChecks` coalescing, the `CheckStorageDevice` network
  nudge, and `reconcileDeviceState`'s three existing clauses — is exactly the polling loop
  TODO.txt asks the rebuild to hang off.
- The transfer-in staging pattern: work lands in a subdirectory of the device that is not a
  store directory, then `os.move`s into place. `tryLoadStore` (`StoreManager.scala:307`) only
  descends into directories holding a `StoreConfig.configFilename`, so a staging directory is
  invisible to normal loading for free.
- `MigratePoolToSetDurableTask` is the structural template for the driving task, including its
  hard-won single-flight and never-throw discipline.

## Design

Two halves that meet at one piece of shared state — a `Rebuilding` entry in a
`StorageDeviceState`. The **producer** (a durable task) tombstones the dead device and drains
its stores onto live devices, marking each `Rebuilding`. The **consumer** (`StoreManager` on
the receiving host) notices the `Rebuilding` entry and reconstructs the data.

The two halves never talk. Either can crash and restart independently.

### 1. The contract: a tombstoned device

A device is **tombstoned** when both its `hostId` and its `storageDeviceId` are the zero UUID.

Two named constants in `common/metadata/fixed_ids.scala`, alongside the existing bootstrap
ids. Both slots are free — `BootstrapStorageDeviceId` is `UUID(0, 2)` and `BootstrapHostId` is
`UUID(0, 3)`:

```scala
val FailedHostId: HostId = new HostId(new UUID(0, 0))
val FailedStorageDeviceId: StorageDeviceId = new StorageDeviceId(new UUID(0, 0))
```

and one predicate on `StorageDeviceState`, so no site open-codes the comparison:

```scala
def isFailed: Boolean =
  hostId == fixed_ids.FailedHostId || storageDeviceId == fixed_ids.FailedStorageDeviceId
```

`||` rather than `&&`: a half-written tombstone should read as failed, not as healthy.

A tombstoned device object still exists and is still readable — it has to be, because it holds
the list of stores still awaiting reconstruction. What it no longer is: a member of any device
set, a member of any host's `storageDevices`, or a legal destination for anything.

### 2. The producer: `FailedStorageDeviceDurableTask`

CLI entry point `fail-storage-device <device-name>` in `Main.scala`, shaped like
`move-device-to-set`. It refuses at pre-flight if the device is already tombstoned, then
enrolls the task and returns; progress is observable via `show-device`.

The task lives in `common/metadata/management/`, registered in `common/TypeFactories.scala`,
and holds two state keys: the `StorageDeviceId` and the `StorageDeviceSetId` that contained it.
The set id must be captured at enrollment because step 1 destroys the device's own record of
it.

It reuses `MigratePoolToSetDurableTask`'s skeleton verbatim: `ignoreExtraCallsWhileRunning`
around `processNext()`, `scheduleRecheck()` with cancel-then-schedule, a `@volatile var
pollPeriod` test seam, and the discipline that **`processNext` must never throw and never
return a failed Future** — `ignoreExtraCallsWhileRunning` clears its flag from
`Future.foreach`, which does not run on failure, so either would wedge the single-flight guard
permanently.

**Step 1 — tombstone**, one transaction, skipped if the device already reads as failed:

- remove the device from `memberDevices` of its enclosing set;
- remove it from its owning `HostState.storageDevices`;
- rewrite the device state with `hostId = FailedHostId` and
  `storageDeviceId = FailedStorageDeviceId`.

All three re-read inside the transaction and are self-healing if the removal has already
happened, which makes the step idempotent and makes concurrent invocations harmless.

**Step 2 — drain**, one store at a time, one transaction per store. For the lowest-indexed
store still recorded on the tombstone:

- Select a destination with `selectDeviceForRebuild(poolId, poolIndex, client)`, called on
  **the pool's own set** (`poolState.storageDeviceSet`), not the failed device's former set.
  This is what `BaseAspenClient.scala:500` already assumes, and it is what makes a store lost
  during a pool migration rebuild directly into the migration's target rather than into the set
  it is leaving. The selector derives the store's size and the failed device from pool state
  itself, so the caller passes nothing else. This is why the drain selects *before* it
  repoints: `poolState.stores(poolIndex).storageDeviceId` must still name the dead device for
  the selector's hard exclusion to mean anything.
- In one transaction: add `StoreEntry(Rebuilding, None)` to the destination device, remove the
  store's entry from the tombstone, and repoint `poolState.stores(poolIndex)` to the
  destination's `(hostId, storageDeviceId)`.
- Send a `CheckStorageDevice` nudge to the destination host so it starts promptly rather than
  waiting up to `Main.CheckStorageDevicesPeriod` (an hour). The nudge is an optimization; the
  poll is the guarantee.

The pool is repointed **now**, at the start of the rebuild rather than at its end. Reads of a
rebuilding store fail until it is reconstructed, but that is equally true of a store on a dead
device, and the pool must stop naming the dead device before anything can route around it. The
rebalancer already excludes non-`Active` stores from movement and from the write-threshold
count, so the interval is safe from its point of view.

**Bootstrap pools need one more write in that transaction.** `StoreManager`'s
`prepUpdateBootstrapConfig` regenerates the radicle's `BootstrapConfigKey` whenever a
bootstrap-pool store changes device, keyed on `storeId.poolId == PoolId.BootstrapPoolId`. The
drain does exactly that change, so it must do the same regeneration or every host's bootstrap
config keeps pointing at the dead device. `prepUpdateBootstrapConfig` is currently a nested
`def` inside `updateStateForTransferredStore` and must be lifted to a shared helper both call.

**Step 3 — finish.** When the tombstone's `stores` map is empty, the task completes. The
device object is left in place as a permanent record.

`selectDeviceForRebuild` needs one change: its level-0 guard is lifted by passing
`client.getStorageDeviceSetState` as the recursion `lookup` instead of the throwing stub, so a
pool on a level-1+ set can be rebuilt. Everything else about it stays as written.

### 3. The guards

Three code paths write a `StoreEntry` into a `StorageDeviceState`. Each must reject a
tombstoned destination, because a store placed on a dead device is silently lost:

- pool creation (`BaseAspenClient.scala:383`, the `Initializing` entries),
- `transferStore` — which also covers `MigratePoolToSetDurableTask` and the rebalancer, since
  both reach the device through it,
- the drain transaction above, defensively: selection cannot return a tombstoned device once
  step 1 has removed it from its set, but the pool state driving selection can be stale.

A fourth guard is a bug fix rather than a new safety check. `reconcileDeviceState` compares
`remote.hostId != hostId` and, on a mismatch, calls `updateHostId` to claim the device. If a
failed device's directory still mounts — a dead controller, not a dead platter — that mismatch
is against the zero host id. `updateHostId` then calls `getHostPointer` on a nonexistent host,
which fails inside `transactUntilSuccessful` and retries forever, never releasing the device's
entry in `activeDeviceChecks`. **`reconcileDeviceState` must test `remote.isFailed` first and
return without reconciling anything.**

### 4. The consumer: `org.aspen_ddp.aspen.server.rebuild`

A new package holding `StoreRebuild`, a `StoreRebuildFactory` test seam mirroring
`StoreTransferFactory`, and a `RebuildState` case class for the checkpoint file.

**Layout.** A rebuild stages into
`<device>/rebuilding/<storeId.directoryName>/`, holding the backend's files, a `StoreConfig`
written up front, and `rebuild-state.yaml`. The staging directory is invisible to
`tryLoadStore` for the same reason `transferring-in/` is — there is no `StoreConfig` at the
`rebuilding/` level — so no marker file and no new `tryLoadStore` clause are needed. TODO.txt
proposed `rebuild-state.yaml` as a do-not-load marker in the device root; the staging directory
subsumes that role, and the file keeps its other job as the checkpoint.

**Trigger.** A fourth clause in `reconcileDeviceState`, alongside Deleted / New / Transferring
In, selecting entries with `status == Rebuilding` and calling `startStoreRebuild`. A
`rebuildingStores` map guards against double-starts exactly as `creatingStores` does for
`createNewStore`, and a `maxConcurrentRebuilds` constructor parameter (default 2) bounds
concurrency — a rebuild reads a full store's worth of data through the client, so an
eight-store device must not start eight at once. Every completion calls
`checkStorageDevice` on its own device, which re-runs the clause and starts the next queued
rebuild.

`startStoreRebuild` runs under `StoreManager`'s instance lock, so it may only create the
staging directory, open the backend, and launch the future. The walk itself runs on the
client's execution context. Opening a backend under that lock is what `createNewStore` already
does.

**The walk.** `pool.allocationTree.foreach(fn)` on a fresh rebuild;
`foreachFrom(lastRestoredKey, fn)` on a resume. `fn` filters to
objects in this store's pool, reads each through the client, takes
`getRebuildDataForStore(storeId)`, and calls `rebuildWrite`. This is `Main.rebuild`'s body,
minus the hardcoded paths and the printlns.

The resume range is inclusive of the checkpointed key, so the last checkpointed object is
re-restored. `rebuildWrite` is an overwrite, so that is free.

**Checkpointing** every 1000 objects: `rebuildFlush()` **first**, then write
`rebuild-state.yaml`. The reverse order would let a crash between the two produce a checkpoint
claiming objects that never reached stable storage. `rebuild-state.yaml` is never deleted on
restart — reading it is what makes a restart a resume.

**Completion:** flush, close the backend, delete `rebuild-state.yaml`, `os.move` the staging
directory to `<device>/<storeId.directoryName>`, then a transaction flipping the entry from
`Rebuilding` to `Active` — re-reading and writing only if the status is still `Rebuilding`,
exactly as `createNewStore` does for `Initializing`. Then load the store and re-check the
device.

If the final directory already exists on startup (a crash between the move and the flip), the
rebuild skips straight to the flip.

### 5. Residual staleness

A store being rebuilt is not `Active`, so it does not participate in transactions and receives
no writes during the walk. Objects the walk has not yet reached are therefore read at their
current value and are correct. The gap is the other side: an object the walk has already
copied, which is then written before the rebuild finishes. The rebuilt store holds a stale
slice of it.

This is exactly the condition of a store that missed writes while briefly offline, and the
existing repair-on-read path (`SimpleOpportunisticRebuildManager` +
`PreTransactionOpportunisticRebuild`) heals it — but only for objects something reads. A
background scrubber that heals cold objects is real work and is deliberately deferred; this
design documents the gap rather than closing it.

### 6. The `foreach` fix, and `foreachFrom`

`KeyValueListNode.foreach` and `foreachInRange` are both wrong once a tier-0 list has more than
one node, which makes them unusable for the walk as written.

In `foreach` (`KeyValueListNode.scala:154`), the recursion constructs `nextNode` from the
newly-read KVOS and then sorts `node.contents` — the node it just finished — instead of
`nextNode.contents`. The result: the first node's contents are visited twice, every subsequent
node's contents are visited paired with the wrong node, and the last node's contents are never
visited at all.

`foreachInRange` has the same slip, plus a second: its termination test compares `maxKey`
against the *current* node's `minimum` rather than `nodeTail.minimum`, so it stops one node
late or early depending on the range. The fix is to compare against `nodeTail.minimum` — the
minimum key of the node about to be read — with `<= 0`, since the range is `[minKey, maxKey)`
and a next node whose minimum equals `maxKey` holds nothing in range.

Both bugs are entirely within `KeyValueListNode`. `TieredKeyValueList.foreach` and
`foreachInRange` only descend to the containing tier-0 node and delegate, so they need no fix.

The existing coverage cannot catch either. `TKVLSuite:95` inserts 100 tiny keys, the tree never
splits, `tail` is `None`, and the buggy branch never runs.

**`foreachFrom(minKey, fn)`** is new, at both levels. A resume needs "from this key to the
end," and `foreachInRange` cannot express it: keys are arbitrary-length byte arrays, so there
is no maximum key to pass as the upper bound. At the `TieredKeyValueList` level it is
`foreach` with `minKey` in place of `Key.AbsoluteMinimum` in the `fetchContainingNode` call; at
the node level it is `foreach` with the head node's contents filtered to `>= minKey`.

Both functions also swallow a failing `fn` — they log and continue. That is left as-is; the
rebuild copes with it explicitly (see error handling), and changing it would alter behavior for
the other callers.

Blast radius of the fix: `Registry.scala:35`, `Main.scala:1034` (`errorTree.foreachInRange`,
the repair command), and `SystemTaskBootstrapSuite`. All three are currently exercised only on
single-node trees, so all three are getting less than they asked for today.

## Error Handling

**Per-object read failures.** Because `foreach` swallows them, `StoreRebuild` tracks them
itself: failed `ObjectId`s go into `rebuild-state.yaml` and the checkpoint advances past them,
so one unreadable object cannot wedge the walk forever. At the end of the walk the recorded
list is retried; anything still failing leaves the store `Rebuilding` for the next device check
to pick up. The list is capped at 10,000 — beyond that the pass aborts with the checkpoint
intact, because something systemic is wrong and continuing only burns I/O.

**Pool below read threshold.** Every read fails, the cap trips, the pass aborts, the checkpoint
holds, the next device check retries. Self-limiting; no special handling.

**Destination fills up mid-rebuild.** `selectDeviceForRebuild` checks free space at *placement*
time against the pool's recorded store size, which can be stale and can grow. If `rebuildWrite`
runs out of space the pass aborts and the store is stuck `Rebuilding` on a device that cannot
hold it. There is no automatic recovery in this scope: log it distinctly enough to alert on,
and document that the operator's remedy is to add capacity. Reassigning a stuck rebuild is
follow-on work.

**No device with room during the drain.** `selectDeviceForRebuild` fails with
`AllocationError`; the drain logs at warn, skips to the next store, and leaves this one on the
tombstone. The task's next poll retries, so it recovers on its own when capacity appears.

**Concurrent `fail-storage-device` on the same device.** The second invocation reads a tombstone
and refuses at pre-flight. If two land simultaneously, both enroll: step 1 is idempotent, and
racing drain transactions lose on their `KeyRevision` requirement, retry, and find the store
already moved. No locking is needed.

**Everything else** — read failures, transaction failures — logs at warn and calls
`scheduleRecheck()`, matching `MigratePoolToSetDurableTask`. The task never fails itself.

Terminal errors are confined to the CLI pre-flight: no such device, device already tombstoned.

## Testing

**The `foreach` fix lands first and standalone.** A test that forces multiple tier-0 nodes —
the existing suite's 100 tiny keys never split one — asserting `foreach` visits every key
exactly once and passes the owning node. The same for `foreachInRange` across a node boundary,
including a `maxKey` that lands inside a later node. Both confirmed failing before the fix.
`foreachFrom` gets the same splitting-tree treatment, with `minKey` landing inside the first
node, on a node boundary, and inside a later node.

**Selector unit tests.** `StorageDeviceSetSelectionSuite:258` currently asserts
`selectRebuildDevice` rejects a level-1+ set; that case is replaced with recursion coverage.
Every other case in the suite must pass unchanged.

**Producer**, on `TestNetwork`, following `StorageDeviceSetIntegrationSuite`:

- Step 1 removes the device from its set and its host and zeroes both ids, atomically.
- Step 1 against an already-tombstoned device is a no-op.
- The drain places each store `Rebuilding` on a device from the pool's set and repoints the
  pool in the same transaction.
- Resumption after a simulated crash mid-drain.
- The task completes when the tombstone's store map empties.
- A bootstrap-pool store triggers bootstrap-config regeneration.

**Guards.** Each of the three write paths rejects a tombstoned device. `reconcileDeviceState`
against a tombstone reconciles nothing and never calls `updateHostId` — that one is a
regression test for the infinite-retry wedge.

**Consumer.** `StoreManagerDeviceDiscoverySuite` already subclasses `StoreManager` with a
recording `tryLoadStore`; the same pattern with a recording `StoreRebuildFactory` covers: a
`Rebuilding` entry starts a rebuild; an entry beyond `maxConcurrentRebuilds` does not start;
completion triggers `checkStorageDevice`; a pre-existing final directory skips to the flip.

**`StoreRebuild` itself**, against a real backend on a temp directory: a full walk restores
every object; the checkpoint is written after the flush; a resume skips already-restored
objects; a failed object is recorded and does not advance the rebuild to completion; completion
moves the staging directory and deletes the checkpoint.

**End-to-end** on `TestNetwork`: allocate objects across a pool, fail a device, drive both
halves, and assert the rebuilt store's backend holds the same slices as the original.
`TestNetwork` already has `completeTransfersIn`; a `completeRebuilds` helper sits alongside it.

## Files Touched

| File | Change |
|---|---|
| `client/tkvl/KeyValueListNode.scala` | fix `foreach` and `foreachInRange`; add `foreachFrom` |
| `client/tkvl/TieredKeyValueList.scala` | add `foreachFrom` |
| `common/metadata/fixed_ids.scala` | `FailedHostId`, `FailedStorageDeviceId` |
| `common/metadata/StorageDeviceState.scala` | `isFailed` predicate |
| `common/metadata/StorageDeviceSetState.scala` | lift `selectDeviceForRebuild`'s level-0 guard |
| `common/metadata/management/FailedStorageDeviceDurableTask.scala` | new task + factory |
| `common/TypeFactories.scala` | register the factory |
| `client/AspenClient.scala` | `failStorageDevice` declaration |
| `client/internal/BaseAspenClient.scala` | `failStorageDevice`; tombstone guard at pool creation and in `transferStore` |
| `server/rebuild/StoreRebuild.scala` | new — the walk, checkpointing, completion |
| `server/rebuild/StoreRebuildFactory.scala` | new — test seam |
| `server/rebuild/RebuildState.scala` | new — `rebuild-state.yaml` model |
| `server/StoreManager.scala` | `isFailed` short-circuit; `Rebuilding` clause; `rebuildingStores`; `maxConcurrentRebuilds`; lift `prepUpdateBootstrapConfig` to a shared helper |
| `cmdline/Main.scala` | add `fail-storage-device`; delete the obsolete `rebuild` and its commented parser entries |

## Implementation Order

Three independently landable pieces, in order:

1. **The TKVL work**: the `foreach`/`foreachInRange` fixes and `foreachFrom`, with the
   splitting-tree tests. The consumer depends on all three, and none of it depends on anything
   else here.
2. **The producer**: constants, `isFailed`, the guards, the selector change, the task, the CLI
   command. Testable end-to-end without any consumer, since the observable result is a
   `Rebuilding` entry in a device state.
3. **The consumer**: the `rebuild` package, the `StoreManager` clause, deletion of the obsolete
   CLI `rebuild`.
