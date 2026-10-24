# Parallel Store Transfers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let rebalancing and pool migration run several store transfers concurrently under a safety floor, and make both recover from aborted or failed transfers.

**Architecture:** One pure admission predicate (`TransferSafety`) with a single floor computation. A shared `TransferStarter` re-reads state, starts a transfer, verifies the floor afterwards and rolls back through a new `AspenClient.abortStoreTransfer` if breached; callers retry with exponential backoff. Both durable tasks drop their one-in-flight gates and use the starter.

**Tech Stack:** Scala 3 (quiet syntax, indented if/then/else), ScalaTest (`AnyFunSuite`, `IntegrationTestSuite` with `atest`), os-lib. Run one suite with `sbt 'testOnly *SuiteName'`.

**Spec:** `docs/superpowers/specs/2026-10-24-parallel-store-transfers-design.md`

## Global Constraints

- Floor is `min(writeThreshold + 2, width - 1)` everywhere; admission and post-start verification share one function.
- Online = store status `Active` AND host not in `client.offlineHosts()`. "Anything that is not Active is considered offline" (use this wording in comments).
- Counting is per store id; a mid-transfer store counts once, as offline.
- The check/start race is accepted (option a) and must be documented prominently at `TransferSafety` and at every call site that starts a transfer.
- No time-based abort logic. Do not bound the `transferStore` retry loop in migration (stays in `Backlog.txt`).
- If no destination can be selected, leave the pending entry and wait for the next poll; never drop it.
- No client abort exists today; the CLI command is out of scope (YAGNI).
- Follow `CLAUDE.md`: Scala 3 quiet syntax, os-lib, crash-only (all progress recoverable from durable state; backoff state may be in-memory).

## Review Focus

- `writeThreshold == width` pool: one transfer starts and stays started; a second concurrent one is rolled back (Task 1, Task 3).
- `width == 1` pool: floor is 0, transfer admitted (Task 1).
- Abort run against an already-completed transfer must not undo it (Task 2).
- Source device dies mid-transfer: `Rebuilding` entries are `InFlight`, not an abort to restart (Task 5).
- Planned destination fails: rebalance entry gets a new destination; nothing usable: entry kept (Task 5).
- Two initiators race on one pool: pool dips below floor only briefly, rolled back store retried after backoff (Task 3).

---

### Task 1: Shared floor and admission rule

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafety.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafetySuite.scala`

**Interfaces:**
- Produces: `TransferSafety.floor(ida: IDA): Int`; `TransferSafety.onlineCount(state: State.PlanningState, offlineHosts: Set[HostId], poolId: PoolId): Int`; `TransferSafety.isSafe(state, offlineHosts, transfer: Plan.Transfer): Boolean` (same signature, new rule); `TransferSafety.isBelowFloor(state, offlineHosts, poolId): Boolean` (post-start verification).

- [ ] **Step 1: Write failing tests** in `TransferSafetySuite` (extend `build` to take an IDA and the number of stores): `floor` for Replication(3,2) = 2, Replication(5,3) = 4 (clamped), Replication(3,3) = 2, Replication(1,1) = 0; `isSafe` true for 5 wide wt 2 with two concurrent `TransferringOut` stores already (online 3 -> after move 2... assert exactly at the floor boundary both sides); `isSafe` false when a `Rebuilding` store plus an offline-host store push below floor; `isSafe` true for width == writeThreshold with all Active; `isBelowFloor` false for that pool with one `TransferringOut` store, true with two; an existing-test update: destination host offline still false.
- [ ] **Step 2: Run** `sbt 'testOnly *TransferSafetySuite'`. Expected: FAIL (new members missing).
- [ ] **Step 3: Implement** the four members. `onlineCount` counts pool stores with status `Active` whose owning device's host is not offline (keep `storeHosts`). `isSafe` = destination device known and host online, pool known, and `onlineCount - 1 >= floor` (the candidate must itself be Active; otherwise false). `isBelowFloor` = `onlineCount < floor`. Put the race documentation in a prominent scaladoc block on the object: check and start are not atomic, `transferStore` revision-checks only the two devices, `TransferStarter` verifies afterwards and rolls back.
- [ ] **Step 4: Run** the suite. Expected: PASS.
- [ ] **Step 5: Commit** `git add -A && git commit -m "Shared transfer floor and admission rule"`.

### Task 2: `AspenClient.abortStoreTransfer`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (next to `transferStore`, ~line 363)
- Test: the existing suite that tests `transferStore` (find with `grep -rln "transferStore" src/test`); add tests there.

**Interfaces:**
- Produces: `def abortStoreTransfer(storeId: StoreId): Future[Unit]` — in one transaction, reads the pool state for the source device, the source device state and the destination named by its `TransferringOut` entry; sets the source entry to `Active` with `transferDevice = None` and removes the destination's `TransferringIn` entry for that store. Revision-checks both device state keys.

- [ ] **Step 1: Write failing tests:** (a) after `transferStore`, `abortStoreTransfer` leaves source `Active`/no `transferDevice` and destination without the store; (b) calling it on an `Active` store with no transfer completes successfully and changes nothing; (c) calling it after a completed transfer (pool now names the destination) changes nothing.
- [ ] **Step 2: Run** the suite. Expected: FAIL (method missing).
- [ ] **Step 3: Implement** with `transactUntilSuccessfulWithRecovery`, modelled on `transferStore`'s `onFail` (same `StopRetrying` set; "no transfer outstanding" is success, not an error). Re-read the pool state inside the transaction. Do not send a host message.
- [ ] **Step 4: Run** the suite. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 3: Host reaction to an aborted transfer

**Files:**
- Modify (only if the tests show a gap): `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` (destination-side receive near `dropAbandonedTransferIn`/`abandonTransferIn`; source-side `reinstateAbandonedTransferOut`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/` (extend the existing transfer-related Host suite; see `HostTestHarness`, `HostDeviceDiscoverySuite`)

**Interfaces:**
- Consumes: `abortStoreTransfer` (Task 2).

- [ ] **Step 1: Write failing tests:** with a transfer in flight, abort it; assert (a) the destination host cancels its pending receive and discards staged data when its `TransferringIn` entry disappears, (b) the source reinstates the store and serves it again (the `Active` + no `transferDevice` + transfer-out marker path), (c) a subsequent `transferStore` of the same store to a new destination succeeds.
- [ ] **Step 2: Run** them. Expected: any gap shows as FAIL.
- [ ] **Step 3: Fix the gaps** found. Destination side: on each device check, any locally-tracked incoming transfer whose entry is no longer `TransferringIn` from that source calls the existing `dropAbandonedTransferIn`. If the tests already pass, record that in the commit message and change no production code.
- [ ] **Step 4: Run** the suites. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 4: `TransferStarter` and backoff

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/TransferStarter.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/TransferStarterSuite.scala` (integration, `IntegrationTestSuite`, modelled on `PoolMigrationPlanningSuite`)

**Interfaces:**
- Consumes: `TransferSafety.isSafe`, `TransferSafety.isBelowFloor`, `State.getStateForPoolMigration(client, poolId, destination)`, `AspenClient.transferStore`, `AspenClient.abortStoreTransfer`.
- Produces: `enum StartResult { Started, Rejected, RolledBack }`; `TransferStarter.start(client: AspenClient, storeId: StoreId, destination: StorageDeviceId): Future[StartResult]`; `class TransferBackoff(now: () => Long, base: Duration, max: Duration)` with `ready(storeId): Boolean`, `failed(storeId): Unit`, `succeeded(storeId): Unit` (exponential, in-memory).

- [ ] **Step 1: Write failing tests:** `start` returns `Rejected` when the rule fails on fresh state and starts nothing; `Started` leaves source `TransferringOut`; a `writeThreshold == width` pool returns `Started` and stays started; with the post-start read forced below floor (start a second transfer in the same pool first), the second returns `RolledBack` and its source entry is `Active` again; `TransferBackoff` (inject a fake clock): not ready after `failed`, ready after `base`, delay doubles per failure up to `max`, `succeeded` resets.
- [ ] **Step 2: Run** the suite. Expected: FAIL.
- [ ] **Step 3: Implement.** `start`: read fresh state, `isSafe` else `Rejected`; `transferStore`; re-read state; `isBelowFloor` -> `abortStoreTransfer` then `RolledBack`, else `Started`. The scaladoc must state the race prominently (best-effort at the instant of the start; bounded dip via rollback; callers must back off on `RolledBack`).
- [ ] **Step 4: Run** the suite. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 5: Rebalance task — parallel starts, retry, new destinations

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala` (and `RebalancingServiceStateSuite` if state shape matters)

**Interfaces:**
- Consumes: `TransferStarter.start`, `TransferBackoff`, `StorageDeviceSetState.selectDeviceForStore(requiredSize: Long, exclude: Set[StorageDeviceId], client: AspenClient, rng)`.
- Produces: private `replacePending(old: Transfer, replacement: Transfer): Future[Unit]` (revision-checked like `removePending`).

- [ ] **Step 1: Write failing tests:** (a) a set with several admissible `pendingTransfers` has more than one `TransferringOut` at once after one pass; (b) a transfer aborted via `abortStoreTransfer` is started again and the set's `pendingTransfers` drains; (c) a pending transfer whose `toDevice` is then tombstoned gets its destination replaced in `pendingTransfers` and completes; (d) when no replacement is selectable, the entry stays in `pendingTransfers`; (e) `classify` returns `InFlight` for a store whose source device is failed and whose entry is `Rebuilding`.
- [ ] **Step 2: Run** these tests. Expected: FAIL (a, c, d; b and e may already pass — keep them as regression pins).
- [ ] **Step 3: Implement.** In `drive`, drop the "any InFlight -> wait" branch: completed transfers are still removed first; then, for each `NotStarted` transfer whose backoff is `ready`: if its pool left planning state, drop it (existing logic); if `toDevice` is unusable (device failed or host offline) call `selectDeviceForStore` with the pool's devices soft-excluded and `replacePending`, or skip when none; else `TransferStarter.start`, recording `failed`/`succeeded` on the backoff. Start sequentially so each start re-reads state. Keep `scheduleRecheck()` at the end. Adjust the class comment ("serially"). Fix `classify` only if test (e) fails. Add the race-documentation comment at the `start` call site.
- [ ] **Step 4: Run** the rebalancing suites. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 6: Migration task — drop the in-flight gate

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetDurableTask.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala`

**Interfaces:**
- Consumes: `TransferStarter.start`, `TransferBackoff`.

- [ ] **Step 1: Write failing tests:** (a) a pool wide enough to admit two concurrent transfers has two `TransferringOut` stores after one pass; (b) after an abort of an in-flight migration transfer, the next pass starts a transfer for that store to a freshly selected destination; (c) a `writeThreshold == width` pool still migrates, one store at a time.
- [ ] **Step 2: Run** the suite. Expected: FAIL (a).
- [ ] **Step 3: Implement.** Remove the `inFlight` early return in `step`. Stores already `TransferringOut`/`TransferringIn` are skipped as candidates in `tryStores`; continue through all remaining candidates instead of stopping at the first start. Replace the direct `TransferSafety.isSafe` + `transferStore` with `TransferStarter.start` (destination still chosen per store via `selectDeviceForStore`). Update the class comment ("serially"). Leave `tryStores` awaiting the call as is (Backlog item). Add the race-documentation comment at the call site.
- [ ] **Step 4: Run** the suite. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 7: Failure-path audit tests

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala`, `MigratePoolToSetSuite.scala`, `RebalancingServiceSuite.scala` (use `FailedStorageDeviceTestHarness`)
- Modify: production code only where a test exposes a defect.

- [ ] **Step 1: Write tests** for: destination device fails mid-transfer -> source restored to `Active`, and the rebalance entry is retried to a new destination and the migration continues; source device fails mid-transfer -> rebuild proceeds, neither task starts a competing transfer for that store, and both tasks finish once the rebuild completes.
- [ ] **Step 2: Run** them. Expected: pass where existing behavior is correct; each FAIL is a defect to fix in Step 3.
- [ ] **Step 3: Fix** any defect found, keeping fixes small and local.
- [ ] **Step 4: Run** `sbt 'testOnly *Rebalanc* *MigratePool* *FailedStorageDevice* *TransferSafety* *TransferStarter*'` then the full `sbt test`. Expected: PASS.
- [ ] **Step 5: Commit.** Update `CLAUDE.md` only if it describes the one-at-a-time rule (grep first).
