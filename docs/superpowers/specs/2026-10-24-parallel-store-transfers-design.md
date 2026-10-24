# Parallel Store Transfers and Transfer Error Handling

## Intent

Rebalancing (`SetRebalanceDurableTask`) and pool migration (`MigratePoolToSetDurableTask`) move
stores between storage devices with `AspenClient.transferStore`. Both are deliberately
conservative today: one transfer in flight at a time. That was a good first step, but it is slow
and, worse, it cannot make forward progress when a Host or StorageDevice fails mid-transfer,
because the single in-flight slot stays occupied (see the top two items of `Backlog.txt`).

This work (1) allows several transfers to run concurrently, bounded by a rule that never puts
the ability to read or write objects in a pool at risk, and (2) audits and fixes how the
initiating/monitoring code reacts to each way a transfer can end abnormally.

## Goals

- Multiple stores of one pool (and of many pools) may be transferring at once, as long as the
  pool keeps enough online stores.
- Every abnormal transfer ending (abort, source device death, destination device death) leaves
  the initiator able to make progress, never permanently wedged.
- Rebalance and migration share one admission rule and, where it stays simple, other code.

## Non-goals / anti-goals

- No change to the transfer protocol itself (`transferStore`, `Host` transfer in/out handling).
- No time-based "transfer is taking too long" abort logic. Waiting for a transfer to complete is
  the default correct behavior.
- Do not trade clarity for sharing: duplication is preferable to a convoluted shared abstraction.
- Must never reduce online stores of a pool to where reads or writes are at risk.

## Terminology

A store is **online** only if its status on its device is `Active` AND its host is not offline
(`client.offlineHosts()`). **Anything that is not Active is considered offline**: `Initializing`,
`Rebuilding`, `TransferringIn`, `TransferringOut`. This wording should be used in code comments
and in the implementation logic.

## 1. Admission rule (parallelism)

Replace both "only one transfer at a time" gates and the current check in
`TransferSafety.isSafe` (`usable - 1 >= writeThreshold`) with a single rule:

> A transfer may start only if, after the store being moved goes offline, the pool still has at
> least `floor` online stores, where `floor = min(writeThreshold + 2, width - 1)`.

The clamp means that for IDAs where `writeThreshold + 2 >= width`, the floor is `width - 1`, so
at least one store can always be transferred (one at a time) when the rest are online. The rule
counts stores already `TransferringOut` or otherwise non-Active as offline, so concurrent
transfers in one pool add up correctly. The destination host must still be online (existing
behavior of `isSafe`).

Counting is **per store id, not per device entry**. A mid-transfer store has two entries (source
`TransferringOut`, destination `TransferringIn`) but is one store of the pool, attributed to its
source device as `State.ownedStores` already does (it filters `TransferringIn` out). It counts as
one offline store. The candidate being moved is subtracted once from the online count; a store
that is already non-Active is not a candidate. *Designer's call.*

### Known race, and how it is contained (decision by the user)

The check and the start are **not atomic**. `transferStore` revision-checks only the source and
destination device objects, not the other stores of the pool, so two initiators (rebalance and
migration, or two tasks) can both pass the rule against the same state and together take the pool
below the floor. This is accepted, with these requirements:

- Re-read current device state immediately before each start.
- **Verify after starting:** once `transferStore` commits, re-read the state of all the pool's
  stores. If the pool is now below the floor, immediately abort the transfer just started (flip the
  store back to `Active` on the source and clear the destination's `TransferringIn` entry), then
  retry the start with exponential backoff. The pool is then only ever below the floor very
  briefly, and service interruption stays minimal even with several initiators competing.
- **The verification must use the same clamped floor as admission** (`min(writeThreshold + 2,
  width - 1)`), never the raw write threshold or `writeThreshold + 2`. Example: a pool whose
  `writeThreshold == width` admits exactly one transfer (floor clamps to `width - 1`), and that
  one transfer leaves `width - 1` online, which must pass verification. Checking against the raw
  threshold would abort it, and the retry would be admitted, start, fail verification and abort
  again, forever. Admission and verification must call one shared floor computation so they cannot
  drift apart. A test must cover `writeThreshold == width`: one transfer starts and stays started;
  a second concurrent one in the same pool is aborted by verification.
- This requires a client-level abort operation (e.g. `AspenClient.abortStoreTransfer`), which does
  not exist today. The same operation is what a CLI abort would use. Check how `Host` reacts to an
  aborted transfer in flight (`cancelTransferIn`, `StoreTransferManager`) and test it.
- **The race and its mitigation must be documented clearly and prominently in the code**, at the
  admission rule (`TransferSafety`) and at each call site that starts a transfer, so a reader
  cannot miss that the rule is best-effort at the instant of the start.

Consequences:
- `SetRebalanceDurableTask.drive` no longer stops at "any transfer InFlight". It still removes
  completed transfers from `pendingTransfers`, and starts every not-started transfer that passes
  the rule, re-evaluating the rule after each start (the planning state must reflect the
  transfers just initiated, either by re-reading or by updating a local snapshot). It waits
  (rechecks on poll or wake) when nothing is admissible.
- `MigratePoolToSetDurableTask.step` drops its "any store of this pool is TransferringIn/Out"
  gate and uses the same rule per store. Destination selection per store is unchanged.
- Rebalance and migration may now run against the same pool; the rule evaluated on current
  device state is what keeps that safe, so it must read current state (not a cached snapshot).
- Within the rebalance task, `Future.traverse(pending)(classify)` already reads every pending
  transfer; bounding concurrent reads (`runBoundedParallel`, `State.DefaultMaxConcurrentReads`)
  should be considered since in-flight counts are now larger.

## 2. Error-handling audit

Audit everything that initiates or monitors transfers: `SetRebalanceDurableTask`,
`MigratePoolToSetDurableTask`, `RebalancingDurableService`, `FailedStorageDeviceDurableTask`,
`Host` transfer handling, and the CLI paths that start transfers. For each case below, verify the
behavior with a test and fix if wrong.

| Event | Observable state | Required behavior |
|---|---|---|
| Abort (CLI or future logic) | Store flipped back to `Active` on source; no entry (or no `TransferringIn`) on destination | Initiator treats the transfer as not started and **retries it**. |
| Source device dies | Source device tombstoned; `FailedStorageDeviceDurableTask` rebuilds the store onto another device | Keep waiting. The rebuild is effectively the transfer. Monitors must not misclassify the rebuild's entries (`Rebuilding`) as stuck or as an abort to retry in a conflicting way. |
| Destination device dies | Destination tombstoned | **Existing behavior (to verify, not rewrite):** `FailedStorageDeviceDurableTask` drops the disowned `TransferringIn` entry and `Host.updateStateForTransferredStore` restores the source to `Active`. Add tests confirming this end to end, then confirm the initiator retries with a new destination (below). |

### Retry with a new destination (decision by the user: "retry it, and pick a new destination")

- **Migration task:** already selects a destination on every attempt; verify it retries cleanly
  after an abort or destination death and stops treating a stale `TransferringIn`/`Out` as
  permanently in flight.
- **Rebalance task:** `pendingTransfers` holds a fixed `(storeId, from, to)` tuple. A not-started
  transfer whose `to` is no longer usable (device failed/tombstoned, or its host offline) must
  have its destination replaced, written back into the set's `pendingTransfers` (revision-checked,
  like `removePending`). *Designer's call:* if the planned destination is still healthy, retry
  the same destination, since an arbitrary reselection could undo the balance the plan computed.
  Selection uses the set's existing device-selection facility
  (`StorageDeviceSetState.selectDeviceForStore`) with the pool's devices soft-excluded, as the
  migration task does.
- If no destination can be selected, leave the entry as is and wait for the next poll; never drop
  it. "Usable" means: device not failed, host not offline. Free-space and device-set criteria are
  whatever `selectDeviceForStore` already applies. *Designer's call.*
- The abort/source-death/never-started classification is the existing `classify` (Completed /
  InFlight / NotStarted); the audit must check it against a source-death `Rebuilding` entry and fix
  it if it misclassifies. *Designer's call* on where to start.
- Distinguishing an abort from "never started" is not required: both are "not started".

## 3. Shared code

Share the admission rule (`TransferSafety`) between both tasks. Share destination reselection
and "classify transfer phase" logic only if the result is simpler than two copies. Secondary to
clarity.

## 4. Backlog

Out of scope (decision by the user): the unbounded `transferStore` retry inside the migration
task's pass. It stays in `Backlog.txt`, rewritten to explain the actual mechanism (what blocks,
why nothing recovers it). The second Backlog item (source host death blocks migration forever)
is resolved by this work and should be removed. Do not bound the retry loop here.

## Constraints

- Follow `CLAUDE.md` development rules (Scala 3 quiet syntax, os-lib, etc.).
- Crash-only: all task state must remain recoverable from durable state; no new in-memory-only
  progress.
- `TransferSafety` is a pure predicate over a `PlanningState` snapshot plus an offline-host set;
  keep it pure and unit-testable (`TransferSafetySuite`).

## Test infrastructure

Existing suites to extend: `FailedStorageDeviceSuite`, `RebalancingServiceSuite`,
`PoolMigrationPlanningSuite`, `TransferSafetySuite`. Reuse their harnesses; building a new
end-to-end harness is not expected.

## Left to the builder

- Exact shape of the planning snapshot update after each start in a pass.
- Whether to bound `classify` parallelism.
- How the shared pieces are factored.
- Test design: unit tests for the rule (including clamp cases for Replication, ReedSolomon,
  AppendingReedSolomon widths), and service-level tests for abort, source death, destination
  death, and concurrent transfers.
