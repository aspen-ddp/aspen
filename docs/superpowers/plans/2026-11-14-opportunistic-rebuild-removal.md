# Opportunistic Rebuild Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete both opportunistic rebuild paths and the client object cache, record repair entry deletions'
missed updates through `limitTracking`, and heal the bootstrap pool's own error trees with a sweep in its stores'
repair scans.

**Architecture:** Removal first (read-triggered path, then pre-transaction path with the client cache). Then
three small building blocks (`ReconcileResult.changed`, a TKVL walk over every node of every tier, tracked
deletions), then `StoreRepairer.sweepBootstrapTrees`, then its wiring into `RepairService.scanOne` with its own
deadline.

**Tech Stack:** Scala 3, sbt, ScalaTest, protobuf (`src/main/protobuf/codec.proto`).

**Spec:** `docs/superpowers/specs/2026-11-14-opportunistic-rebuild-removal-design.md`

## Global Constraints

- Backward compatibility with persisted data or mixed-version messages is not required: delete proto fields and
  messages outright (renumbering is fine; reserve nothing).
- Server threading rules (`server/CLAUDE.md`): nothing in repair blocks the control thread; store work goes through
  `RepairTarget`, which posts to the store's executor.
- Healing goes through `Reconcile` only. No new durable state. The sweep creates and deletes no entries and runs no
  proofs.
- `newRepairTransaction` stays, for proofs only.
- The server-side `server.store.cache.ObjectCache` and `ObjectState.getRebuildDataForStore` stay
  (`RebuildingStore` uses the latter).
- Tests are deterministic (TestNetwork/LockstepThreading), with no flakiness and no needless waits. Full runs use a
  70 s timeout to detect hangs.
- Quiet-mode Scala 3 syntax, indented if/then/else.
- Commit directly on main.
- Each task updates the module CLAUDE.md for the behavior it changes.

## Review Focus

1. **A join deleting a node mid-sweep.** The walk of that tier stops, the sweep reports incomplete, and the next
   scan sweeps again. It never fails the scan or hangs. Covered by Task 4's join test plus Task 6's incomplete
   result.
2. **A root reduction or root split between reading the root and walking.** The walk visits what it can, reports
   incomplete when a read hits `InvalidObject`, and never throws out of `scanOne`. Task 4 test.
3. **A metadata-pool (non-bootstrap) store's scan.** No sweep runs and no bootstrap reads are issued. Task 7 test.
4. **A deadline cutting the sweep off.** The entry scan's result is still applied, the store stays at the floor,
   and an abandoned sweep never shares a DeferralState with the next one. Task 7 test.
5. **Late read responses after removing the read hold.** No warning flood: a response for a finished read is
   dropped quietly. Task 1 test.

---

### Task 1: Remove the read-triggered opportunistic rebuild

**Files:**
- Delete: `OpportunisticRebuild` from `common/network/Message.scala`, its `Codec.encode`/decode, its
  `codec.proto` message and any `oneof` case, its `QuicMessageCodec` case
- Modify: `server/host/StoreRouter.scala:165` (drop the dispatch), `server/store/Frontend.scala`
  (`readObjectForOpportunisticRebuild`, `opportunisticRebuild`, `ReadKind.OpportuneRebuild` and the branch at ~1497)
- Modify: `client/internal/read/BaseReadDriver.scala` (`sendOpportunisticRebuild`, the `rereadCandidates` send,
  the `disableOpportunisticRebuild` constructor parameter), `ReadDriver.scala` (Factory signature),
  `SimpleReadDriver.scala`, `ReadManager.scala` (`read`'s `disableOpportunisticRebuild` parameter,
  `pruneStaleReadsTask`, `completionTimes`), and every caller of `ReadManager.read` that passes the flag
- Tests: delete `FrontendSuite` tests at ~3197-3260 and `CodecRoundTripSuite` "OpportunisticRebuild round-trip";
  update `BaseReadDriverSuite` (`rebuildsSent` and the tests using it, and the `disableOpportunisticRebuild` argument)
- Docs: `server/store/CLAUDE.md`, `client/internal/CLAUDE.md`, `Architecture.md`

`markRepairNeeded` and the `opportunisticRebuildManager` member stay until Task 2. Their only caller in
`BaseReadDriver` (the `repairNeeded` block at ~231) goes in this task.

**Interfaces:**
- Produces: `ReadDriver.Factory = (AspenClient, ObjectPointer, IDA, UUID, String, ReadType) => ReadDriver`
  (the Boolean removed) and `ReadManager.read(objectPointer, ida, comment, readType = FullObject())`.

- [ ] **Step 1: Write the failing test** in `client/internal/read/ReadManagerSuite.scala` (create it if absent,
  using the `BaseReadDriverSuite` recording client):
  `test("a read is released when it completes, and a late response for it is dropped without a warning")`.
  Assert that after the read's future completes on a threshold of responses, `outstandingReadCount == 0`
  (add `private[read] def outstandingReadCount: Int`), and that delivering the last store's response logs nothing
  at warn (capture with the scribe test handler other suites use, or assert no exception and the count stays 0).
- [ ] **Step 2: Run it.** `sbt 'testOnly *ReadManagerSuite'`. Expected: FAIL, because the read is still
  outstanding (held for 5 s).
- [ ] **Step 3: Implement.** In `ReadManager.deliver`, remove the read from `outstandingReads` as soon as
  `driver.readResult` completes. Log an unknown-read response at trace, not warn. Delete `pruneStaleReadsTask`
  and `completionTimes`. First confirm nothing in `BaseReadDriver` acts on responses after `promise.isCompleted`
  apart from the opportunistic code being deleted. If something does, stop and report it instead of deleting the
  hold.
- [ ] **Step 4: Delete the rest of the path** as listed under Files, then fix compile errors in main and test.
- [ ] **Step 5: Run.** `sbt Test/compile`, then `sbt 'testOnly *ReadManagerSuite *BaseReadDriverSuite *FrontendSuite *CodecRoundTripSuite *Quic*Codec*'`.
  Expected: PASS.
- [ ] **Step 6: Docs.** Remove opportunistic rebuild from the module guides and `Architecture.md`.
- [ ] **Step 7: Commit.** `git commit -m "Remove read-triggered opportunistic rebuild (Repair F1, N2); Todo 2.1 part 1"`

### Task 2: Remove the pre-transaction rebuild and the client object cache

**Files:**
- Delete: `common/transaction/PreTransactionOpportunisticRebuild.scala`,
  `client/internal/OpportunisticRebuildManager.scala`, `client/internal/SimpleOpportunisticRebuildManager.scala`,
  `client/ObjectCache.scala`, `client/internal/SimpleObjectCache.scala`
- Modify: `TxPrepare` in `Message.scala:257` (drop `preTxRebuilds`), `Codec.scala:480` and the proto field,
  `QuicMessageCodec`; `server/transaction/Tx.scala:42` (constructor parameter and its use in `completeLoad`) and
  every `Tx` construction site (`Frontend`, `Store`, `StoreLoader`, `DeviceControl`, `Host` as the compiler finds
  them); `TransactionBuilder.buildTranaction` (drop the `OpportunisticRebuildManager` parameter and the
  `preTransactionRebuilds` result element at :35 and :146); `TransactionImpl.scala:366`;
  `ReadDriverClient.scala` (`opportunisticRebuildManager`, `objectCache` and their `None`/`NoCache` defaults);
  `BaseAspenClient.scala:53`; `SimpleAspenClient.scala:33,39`; `BaseReadDriver.scala:201` (the `put`);
  `cmdline/Main.scala` if it references either
- Tests: drop the argument and overrides from `TestNetwork`, `RangedWriteIntegrationSuite`, `UncheckedTransaction`,
  `TransactionBuilderSuite`, `TransactionRangedWriteSuite`, `TransactionMaxSizeSuite`,
  `ClientTransactionDriverSuite`, `TransactionOutcomeUnknownSuite`, `BaseReadDriverSuite`,
  `StreamFrameDecoderSuite:49`; delete any test that exists only for pre-tx rebuild
- Docs: `client/internal/CLAUDE.md`, `client/internal/transaction/CLAUDE.md`, `server/transaction/CLAUDE.md`,
  `server/store/CLAUDE.md`; mark Repair F1, F2, N1, N2 and Req F3 FIXED (dated 2026-11-14, "path deleted") in
  `docs/safety/repair-process.md` and `transaction-requirements.md`, plus their README ranked rows

**Interfaces:**
- Produces: `TransactionBuilder.buildTranaction(poolIDAMap, maxSizes, ...)` returning a 2-tuple, or keep the
  3-tuple shape minus the rebuild list (the builder's call; update every caller); `TxPrepare` without
  `preTxRebuilds`.

- [ ] **Step 1: Delete** as listed, then fix compile errors in main and test (`sbt Test/compile`).
- [ ] **Step 2: Check** that `grep -rn "Opportunistic\|preTxRebuild\|SimpleObjectCache\|client.ObjectCache" src`
  returns nothing apart from safety-doc references.
- [ ] **Step 3: Run** the full suite: `sbt test` with a 70 s hang timeout. Expected: PASS.
- [ ] **Step 4: Docs** as listed.
- [ ] **Step 5: Commit.** `git commit -m "Remove pre-transaction rebuild and client object cache (Repair F2, N1, Req F3); Todo 2.1"`

### Task 3: `ReconcileResult.changed`

**Files:**
- Modify: `server/store/Reconcile.scala:63`; `server/store/Frontend.scala` (~1387, where the plan is applied)
- Test: `server/store/FrontendSuite.scala` (the existing reconcile tests)

**Interfaces:**
- Produces: `final case class ReconcileResult(deferredKeys: Set[Key], objectLevelDeferred: Boolean, stale: Boolean, changed: Boolean = false)`.
  `complete` is unchanged. `changed` is true when the store installed anything, i.e. the applied plan's
  `!changesNothing`.

- [ ] **Step 1: Write the failing tests** in `FrontendSuite`:
  `test("reconciling a store that matches the read reports changed = false")` and
  `test("reconciling a store missing a key reports changed = true")`. Assert on `result.changed` and that
  `result.complete` is true in both.
- [ ] **Step 2: Run.** `sbt 'testOnly *FrontendSuite -- -z "reports changed"'`. Expected: compile failure.
- [ ] **Step 3: Implement.** Set `changed = !plan.changesNothing` on the result Frontend returns after a
  successful install. A refused or stale install reports false.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "ReconcileResult reports whether the install changed the store"`

### Task 4: TKVL walk over every node of every tier

**Files:**
- Modify: `client/tkvl/TierEngine.scala`, `client/tkvl/TieredKeyValueList.scala`
- Test: `client/tkvl/` (the suite holding the `foreach` join-race tests; add the new tests there)
- Docs: `client/tkvl/CLAUDE.md` (one bullet)

**Interfaces:**
- Produces: `TieredKeyValueList.foreachNode(fn: (Int, KeyValueObjectPointer) => Future[Unit]): Future[NodeWalk]`,
  backed by `TierEngine.foreachNode(fn: (Int, ObjectPointer) => Future[Unit]): Future[NodeWalk]`, with
  `final case class NodeWalk(visited: Int, complete: Boolean)` in `client/tkvl`.
  The walk reads a fresh root (`rootManager.getFreshRootNode()`), finds each tier's leftmost node by following
  `Key.AbsoluteMinimum` down from the root (as `deleteTree.fetchRootNodes` does), and walks each tier left to right
  through `link`, calling `fn(tier, node.pointer)` once per node. An empty tree yields `NodeWalk(0, true)`. Any
  `InvalidObject` on a read (a join, tail removal or root reduction deleted the node) ends that tier's walk and
  makes the result `complete = false`. Other tiers are still walked. A failing `fn` is logged and the walk
  continues, as in `foreach`. Other read failures fail the future.

- [ ] **Step 1: Write the failing tests:**
  - `test("foreachNode visits every node of every tier exactly once")`: build a tree of at least 2 tiers (large
    values, as the StoreRepairerSuite join test does). Assert that the visited `(tier, pointer.id)` set equals the
    one from walking each tier by hand, that every tier from 0 to the root's tier is present, that
    `visited == set.size`, and that `complete == true`.
  - `test("foreachNode of an empty tree visits nothing")`: `NodeWalk(0, true)`.
  - `test("foreachNode reports incomplete when a join deletes a node mid-walk")`: from inside `fn`, on the first
    tier-0 node, delete entries through the tree so the next node is joined away. Assert that the future succeeds
    with `complete == false`.
- [ ] **Step 2: Run** `sbt 'testOnly *<suite> -- -z "foreachNode"'`. Expected: compile failure.
- [ ] **Step 3: Implement** as described under Interfaces.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Docs, then commit.** `git commit -m "TKVL foreachNode: walk every node of every tier"`

### Task 5: Repair entry deletions record their misses

**Files:**
- Modify: `server/repair/StoreRepairer.scala:427` (`deleteEntryIfUnchanged`) and its two callers;
  `client/AspenClient.scala:89-102` (scaladoc: proofs only); `client/Transaction.scala:171-178` (scaladoc)
- Test: `server/repair/StoreRepairerSuite.scala`
- Docs: `server/repair/CLAUDE.md`, `client/internal/transaction/CLAUDE.md` if it describes these; Repair F7 FIXED
  (dated) in `docs/safety/repair-process.md` and its README row

**Interfaces:**
- Produces: `private[repair] def deleteEntryIfUnchanged(entryPool: PoolId, tree: TieredKeyValueList, entryRevision: ObjectRevision, key: Key): Future[Unit]`,
  and a constructor seam on `StoreRepairer`:
  `testingOnlyBeforeEntryDelete: (PoolId, Transaction) => Unit = (_, _) => ()`, called just before `tx.commit()`.
  Callers pass `storeId.poolId`. The body uses `client.newTransaction()`, then `tree.deleteIfPresent(...)`, then
  `MissedUpdateFinalizationAction.limitTracking(entryPool, tx)`, then commit.

- [ ] **Step 1: Write the failing tests:**
  - `test("an entry deletion about another pool keeps missed-update tracking")`: seed an entry in the bootstrap
    errorTree. Call `deleteEntryIfUnchanged(PoolId(UUID.randomUUID()), tree, rev, key)` with the seam recording
    `tx.missedUpdateTrackingEnabled`. Assert true, and that the entry is gone.
  - `test("an entry deletion in the bootstrap pool's own trees runs untracked")`: the same with
    `PoolId.BootstrapPoolId`. Assert false.
- [ ] **Step 2: Run** `sbt 'testOnly *StoreRepairerSuite -- -z "entry deletion"'`. Expected: compile failure.
- [ ] **Step 3: Implement** as under Interfaces. Update both scaladocs to say why the chain ends: an entry about P
  lives one level closer to the bootstrap pool, and B's own trees stay untracked through `limitTracking`.
- [ ] **Step 4: Run** `sbt 'testOnly *StoreRepairerSuite *NewRepairTransactionSuite *RepairProofsSuite'`. Expected:
  PASS.
- [ ] **Step 5: Docs, then commit.** `git commit -m "Repair entry deletions record their misses via limitTracking (Repair F7)"`

### Task 6: `StoreRepairer.sweepBootstrapTrees`

**Files:**
- Modify: `server/repair/StoreRepairer.scala`
- Test: `server/repair/StoreRepairerSuite.scala` (live-host section, using `missedBy`, `settle`, `pump`, `stored`)

**Interfaces:**
- Consumes: `TieredKeyValueList.foreachNode` and `NodeWalk` (Task 4), `ReconcileResult.changed` (Task 3).
- Produces:
  - `final case class SweepResult(nodes: Int, reconciled: Int, incomplete: Int, walkComplete: Boolean)` with
    `def needsAnotherPass: Boolean = reconciled > 0 || incomplete > 0 || !walkComplete`.
  - `def sweepBootstrapTrees(storeId: StoreId, policy: RepairPolicy, deferral: DeferralState): Future[SweepResult]`.
    It requires `storeId.poolId == PoolId.BootstrapPoolId` (otherwise `IllegalArgumentException`) and does
    nothing (an empty, complete result) unless `ownedHere(storeId)`. For each node of
    `pool.errorTree` then `pool.unackedCommitsTree` of the bootstrap pool it calls a no-proof variant of
    `repairUpdate` with `minTimestamp = HLCTimestamp.Zero` and the deferral key
    `sweepDeferralKey(pointer.id) = Key(Array[Byte](2) ++ pointer.id.toBytes)`. It counts `changed` results as
    reconciled and `!complete` results as incomplete, and stops visiting (counting the rest as nothing) once
    `abandoned(storeId)`. Sweep keys are added to `counts.touch`-equivalent retention so `deferral.retain` in
    `scan` keeps them. The simplest way: `DeferralState.retain` keeps every key with prefix byte 2.
  - Refactor `repairUpdate` to take `allowProof: Boolean`. The sweep passes false; entry repair passes true.

- [ ] **Step 1: Write the failing tests:**
  - `test("a bootstrap store that missed an untracked errorTree write is reconciled by a sweep")`: using
    `missedBy(2)`, commit a `client.newRepairTransaction()` (untracked) that inserts a key into the bootstrap
    errorTree for store 1's range. Assert store 2's tier-0 node copy lacks the key and `errorEntries(2)` is empty.
    Run `sweepBootstrapTrees(sid(2), RepairPolicy.Default, DeferralState())` through the live host (`net.smgr`),
    then assert that store 2's copy holds the key and that `result.reconciled >= 1`.
  - `test("a sweep reconciles a stale upper-tier node")`: with `missedBy(2)`, seed enough 400 KiB-pointer
    entries untracked that the tree reaches tier 1. Run `dropErrorEntries(2)` so that only the sweep can heal.
    Sweep store 2 until `!needsAnotherPass` (at most 5 passes). Assert that for every node pointer from
    `foreachNode`, `stored(2, id)` has the same kv keys and revision as `stored(0, id)`.
  - `test("a sweep of a converged store changes nothing and needs no further pass")`: after
    `scanAllUntilQuiet()`, two sweeps in a row. The second has `reconciled == 0` and
    `needsAnotherPass == false`.
  - `test("a sweep runs no proofs")`: hold a key of a bootstrap node locked (as the
    `"one permanently locked key is deferred..."` test does) for `proofAfterDeferrals + 1` sweeps. Assert that
    `testingOnlyOnProof` is never called and `incomplete >= 1` on each.
  - `test("sweeping a store of another pool is rejected")`: `IllegalArgumentException`.
- [ ] **Step 2: Run** `sbt 'testOnly *StoreRepairerSuite -- -z "sweep"'`. Expected: compile failure.
- [ ] **Step 3: Implement** as under Interfaces.
- [ ] **Step 4: Run** `sbt 'testOnly *StoreRepairerSuite'`. Expected: PASS, including every existing test
  (`scan` itself is unchanged).
- [ ] **Step 5: Commit.** `git commit -m "Repair: sweep the bootstrap pool's own error trees (Rebuild F8)"`

### Task 7: Run the sweep from `RepairService`

**Files:**
- Modify: `server/repair/RepairService.scala`
- Test: `server/repair/RepairServiceSuite.scala`
- Docs: `server/repair/CLAUDE.md`, `server/rebuild/CLAUDE.md` (the F8 hole is closed by the first scan after
  promotion); safety docs: Rebuild F8 FIXED, X9 narrowed with its residual (the 15-minute window, citing the
  `Backlog.txt` entry), Repair F7 already done; README rows; `Todo.txt`: remove 2.1 and 2.10, and in other items
  remove references to opportunistic rebuild healing (2.1's "This also removes the healing that 2.10 currently
  leans on")

**Interfaces:**
- Consumes: `StoreRepairer.sweepBootstrapTrees`, `SweepResult` (Task 6).
- Produces: `protected def sweepStore(storeId: StoreId, policy: RepairPolicy): Future[SweepResult]` (overridable,
  like `scanStore`), `protected def sweepDeadline: FiniteDuration = SweepDeadline`, and
  `val SweepDeadline: FiniteDuration = FiniteDuration(90, SECONDS)` in `object RepairService`, plus
  `case class SweepTimedOut(storeId: StoreId, after: Duration) extends Exception`.
  In `scanOne`, after the entry scan settles (success or failure), stores of `PoolId.BootstrapPoolId` run
  `withDeadline(sweepDeadline, SweepTimedOut(...))(sweepStore(...))`. The value passed to `ScanState.advance` is
  `foundEntries || sweep.needsAnotherPass`. A failed or timed-out sweep counts as needing another pass
  (`true`, unlike a failed entry scan). Both the sweep and the entry scan use the store's `DeferralState`. On
  `SweepTimedOut`, drop the store's DeferralState as `ScanTimedOut` does.

- [ ] **Step 1: Write the failing tests** in `RepairServiceSuite`, using its existing overriding pattern:
  - `test("a bootstrap store's scan also sweeps; a metadata pool store's does not")`: override `sweepStore` to
    record calls. Assert it is called for `StoreId(PoolId.BootstrapPoolId, 0)` and not for a store of another
    PoolId.
  - `test("a sweep that needs another pass keeps the store at the floor")`: `scanStore` returns no entries,
    `sweepStore` returns `needsAnotherPass = true`. Assert that the ScanState interval stays at
    `scanIntervalFloor` over three ticks.
  - `test("a clean sweep lets the store back off")`: both clean. Assert that the interval doubles.
  - `test("a sweep cut off by its deadline keeps the store at the floor and drops its deferrals")`: `sweepStore`
    never completes and `sweepDeadline` is overridden short. Assert floor interval and no DeferralState entry.
  - `test("a failed entry scan still runs the sweep")`.
- [ ] **Step 2: Run** `sbt 'testOnly *RepairServiceSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement** as under Interfaces.
- [ ] **Step 4: Run** `sbt 'testOnly *RepairServiceSuite *StoreRepairerSuite *ScanStateSuite'`, then the full
  `sbt test` (70 s hang timeout). Expected: PASS. Run the full suite twice to check for flakes.
- [ ] **Step 5: Docs** as listed.
- [ ] **Step 6: Commit.** `git commit -m "Repair service sweeps bootstrap error trees each scan (Rebuild F8, X9 narrowed); Todo 2.1, 2.10"`
