# Replay Pins Addendum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two problems the final review left open:
- **Applied stubs:** a store that applied T never re-applies it through a late commit.
- **Prefer-abort driver prepares:** a re-driven aborted transaction can only abort again.

Also document the one accepted corner case.

**Architecture:**
- **Stubs.** A store writes a minimal durable stub record in its device CRL when it forgets a committed T that it
  applied while an UnackedCommits entry might name it. Late-commit application checks the stub. The repair scan
  deletes stubs along with entries.
- **Prefer-abort.** A driver's prepare (`fromClient == false`) never makes a store check requirements. A store with
  no vote proves if it can, otherwise waits for the client's prepare and votes abort if it never comes.

**Tech Stack:** Scala 3, sbt, ScalaTest (`AnyFunSuite`, `IntegrationTestSuite`/`TestNetwork`).

**Spec:**
- `docs/superpowers/specs/2026-11-09-replay-pins-addendum-design.md` (the addendum, authoritative for this plan);
- the base design `docs/superpowers/specs/2026-11-09-replay-pins-design.md`, including its "Amendments during
  implementation".

## Global Constraints

- **Branch:** work and commit on `pin_refactor`.
- **Tests:** write the test before the code for every step that changes behavior.
- **Full `sbt test`:** 70 s hang bound, no flaky tests, no new "Hung Test" prints, CPU may be at 100%. Use a bash
  timeout of about 600000 ms. Leave no background runs going.
- **Compatibility:** backward compatibility with persisted data and wire formats is not required.
- **Style:** Scala 3 quiet syntax, indented if/then/else.
- **Threading:** Store, Frontend and Tx run on the store executor and take no locks.
- **Prefer-abort is `TxPrepare.fromClient == false`.** Add no new prepare field.
- **Stub semantics:** stub (S, T) means "S holds T's update", so an UnackedCommits entry naming S for T is stale.
  - A stub is written before T's CRL state is deleted.
  - A Tx with a stub for its transaction never applies through an entry.
  - Stubs never outlive their entry by more than one repair scan.
- **`TxFinalized` gains `owed: Set[StoreId]`**, the stores the finalizer's marker wrote UnackedCommits entries for.
  It is empty for late and abort finalizations.
- **Commit trailer:** every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A driver prepare that overtakes the client's prepare on the fast path must not abort an ordinary transaction.**
   The store waits for the client's prepare. Pinned in Task 1.
2. **A crash between writing a stub and deleting T's CRL state leaves both.** A recovered commit vote that has a stub
   applies nothing. Pinned in Task 4.
3. **Stubs survive restart and travel with a store transfer** (`extractCrl` / `crl_save.log`). They are dropped with
   the store on rebuild (`dropStore`). Pinned in Task 2.
4. **CRL backpressure (`CrlAdmission`) never drops or blocks a stub write.** A stub is not a prepare. Pinned in
   Task 2.
5. **A stub whose entry was already deleted is still removed by the next scan.** Pinned in Task 5.

---

### Task 1: Prefer-abort driver prepares

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala`
  - `awaitingData` (~:180)
  - the vote decision (`determineDisposition` / `completeLoad` path)
  - `receivePrepare` handling of `fromClient` (~:758-790)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/ReplayIntegrationSuite.scala`

**Interfaces:**
- **Produces:** the rule. A Tx with no vote (Undetermined, not recovered with a vote, no client prepare seen) that
  receives only driver prepares:
  - never checks requirements;
  - proves at once if `mayProve && appliedHere.nonEmpty`;
  - otherwise waits for a client prepare up to `frontend.missingDataWait`, then votes abort.
- `awaitingData` loses its `missingObjectData` condition, so it covers data-less transactions too. Rename it to fit,
  e.g. `awaitingClientPrepare` (builder's choice).
- The abort-on-expiry carries no requirement errors. Record it in `AbortVoteReport` under a clear reason. A new
  non-object `RequirementError` such as `NoClientPrepare` is fine.

- [ ] **Step 1: Write the failing tests**

```scala
// FrontendSuite
test("a data-less transaction prepared only by a driver gets an abort vote after the wait, requirements unchecked")
  // RefcountUpdate whose requirement holds; deliver only a fromClient=false prepare; advance past missingDataWait;
  // the vote is VoteAbort and no requirement check ran (e.g. no lock taken on the object)
test("a client prepare arriving within the wait is checked normally and can vote commit")   // Review Focus 1
test("a driver prepare reaching a store whose object shows the update proves at once")
test("a recovered commit vote answers a driver prepare with its vote")
// ReplayIntegrationSuite
test("an aborted data-less transaction re-driven by a recovered commit vote aborts again")
  // refcount change aborts on a collision; one commit voter's vote is recovered after every store forgot T and
  // the collision cleared; T aborts again and the object's refcount is unchanged on every store
```

- [ ] **Step 2: Run them to verify they fail.** Run `sbt 'testOnly *FrontendSuite *ReplayIntegrationSuite'`.
  Expected: the new tests FAIL.
- [ ] **Step 3: Implement the rule.**
- [ ] **Step 4: Run the tests.** Run the same command, then the replay suite 3 more times, then `sbt test`.
  Expected: all PASS.
- [ ] **Step 5: Commit.** `git commit -am "prefer-abort: driver prepares never gather fresh commit votes"`

---

### Task 2: CRL applied-stub records and the Frontend stub set

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/CrashRecoveryLog.scala`, and the implementation(s) it
  delegates to under `server/crl/`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala`, `Store.scala`,
  `host/StoreRouter.scala` (`readAndRecover`).
- Modify: the transfer path that builds `crl_save.log` (`StoreTransfers.closeForTransferOut`, `extractCrl`).
- Modify: `TestNetwork.RecordingCRL` and `NoCRL` (`src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`).
- Test: the CRL suites under `src/test/scala/org/aspen_ddp/aspen/server/crl/`, `FrontendSuite`, and the store
  transfer suite.

**Interfaces:**
- Produces on `CrashRecoveryLog`:
  - `def saveAppliedStub(storeId: StoreId, txid: TransactionId, completionHandler: () => Unit): Unit`, which replaces
    any state the store holds for `txid`;
  - `def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit`;
  - stubs are returned on recovery separately from transaction state, e.g.
    `readStoreState` → `(List[TransactionRecoveryState], Set[TransactionId])` or a separate
    `readAppliedStubs(storeId): Future[Set[TransactionId]]` (builder's choice; state it in the report).
- Produces on `Frontend`:
  - `def hasAppliedStub(txid: TransactionId): Boolean`;
  - `def recordAppliedStub(txid: TransactionId)(then: => Unit): Unit`, which runs `then` once the stub is durable;
  - `def dropAppliedStub(txid: TransactionId): Unit`.

  The stub set is loaded at recovery.

- [ ] **Step 1: Confirm the premise.** A CRL record survives stream rotation (copy-forward), transfer extraction and
  recovery loading. Write this as a CRL-suite test first. If the CRL cannot hold such a record, stop and report.
- [ ] **Step 2: Write the failing tests**

```scala
test("an applied stub replaces a transaction's state and is recovered as a stub, not a transaction")
test("an applied stub survives stream rotation")
test("deleteAppliedStub removes it durably")
test("stubs travel with a store transfer and are dropped by dropStore")        // Review Focus 3
test("a stub write is not subject to CrlAdmission backpressure")              // Review Focus 4
// FrontendSuite
test("a store's stub set is loaded on recovery")
```

- [ ] **Step 3: Run them to verify they fail.**
- [ ] **Step 4: Implement.**
- [ ] **Step 5: Run the touched suites, then `sbt test`.** Expected: PASS.
- [ ] **Step 6: Commit.** `git commit -am "crl: applied-stub records; Frontend stub set"`

---

### Task 3: `owed` on TxFinalized

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationAction.scala`. Once
  its `mark(...)` future succeeds, it records the store ids it wrote UnackedCommits entries for.
- Modify: `client/FinalizationAction.scala`, `server/transaction/TransactionFinalizer.scala` and
  `server/RegisteredTransactionFinalizerFactory.scala`. Expose the owed set from the completed finalizer.
- Modify: `server/transaction/TransactionDriver.scala` (`onFinalized`) and `common/network/Message.scala` + `Codec.scala`
  + `codec.proto`. This adds `TxFinalized.owed: Set[StoreId]`. Every other construction site passes `Set()`.
- Test:
  - `MissedUpdateFinalizationActionSuite`
  - `RegisteredTransactionFinalizerSuite`
  - `TransactionDriverSuite`
  - `CodecRoundTripSuite`

**Interfaces:**
- Produces `TxFinalized(to, from, transactionId, committed, late, owed: Set[StoreId])`. `owed` is non-empty only for
  a non-late commit whose finalizer wrote entries.
- Produces `TransactionFinalizer.owedStores: Set[StoreId]`, valid once `complete` has succeeded.

- [ ] **Step 1: Confirm the premise.** `MissedUpdateMarker.mark`'s future completes only after its batches are
  committed. Add a test if none pins it. If the premise fails, stop and report.
- [ ] **Step 2: Write the failing tests**

```scala
test("a finalizer that wrote entries for stores 2 and 3 reports owedStores == {2, 3}")
test("a driver's TxFinalized carries the finalizer's owed set; a late or abort TxFinalized carries none")
test("TxFinalized owed round-trips through the codec")
```

- [ ] **Step 3: Run them to verify they fail.**
- [ ] **Step 4: Implement.**
- [ ] **Step 5: Run the touched suites, then `sbt test`.** Expected: PASS.
- [ ] **Step 6: Commit.** `git commit -am "TxFinalized carries the stores owed an UnackedCommits entry"`

---

### Task 4: Writing and honouring stubs

**Files:**
- Modify: `server/transaction/Tx.scala`: `receiveFinalized` / `finalized`, `unackedCommitFound`, `lateCommit`, and the
  forget paths.
- Modify: `server/store/Frontend.scala`: the no-Tx TxFinalized path.
- Test: `FrontendSuite`, `ReplayIntegrationSuite`, `UnackedCommitsIntegrationSuite`.

**Interfaces:**
- **Consumes:** Task 2's Frontend stub API, and Task 3's `TxFinalized.owed`.
- **Write points** (spec A, cases 1 to 3). In every case the stub is durable before T's CRL state is deleted.
  - **(1)** A `TxFinalized(committed = true)` whose `owed` contains this store writes a stub. This holds both for a
    Tx that applied T and for a store that has no Tx for T.
  - **(2)** Applying T through an entry writes a stub. That covers `unackedCommitFound`, a late commit with its entry
    found, and the lingering Tx of Ruling 9.
  - **(3)** A committed T applied here and forgotten without a listing TxFinalized first runs the existing
    UnackedCommits lookup.
    - On a find, write a stub.
    - On absent, forget T.
    - On failure, hold and retry at heartbeat.
- **Check:** before applying through an entry, `frontend.hasAppliedStub(txid)` leads to `applyNothing()`.

- [ ] **Step 1: Write the failing tests**

```scala
// FrontendSuite
test("a TxFinalized naming this store in owed writes a stub before deleting the transaction's state")
test("a TxFinalized naming this store with no Tx writes a stub")
test("applying through a found entry writes a stub")
test("a Tx forgotten after a late resolution looks up its entry and writes a stub when found")
test("a late commit whose entry is found applies nothing when a stub exists")
test("a recovered commit vote with a stub applies nothing")                     // Review Focus 2
// ReplayIntegrationSuite
test("a replay at a pool whose stores all acknowledged late leaves every store unchanged")
  // T: pin in pool A + DoesNotExist insert of k in pool B; every pool-B store's TxCommitted delayed past
  // errorTimeout (entries written); k later deleted; replay T's client prepares after the cache expired;
  // k stays absent on every pool-B store (the R-N1 quorum scenario)
```

- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the touched suites (the replay suite at least 3 times), then `sbt test`.** Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -am "applied stubs: a store that applied T never re-applies it"`

---

### Task 5: Stub cleanup in the repair scan

**Files:**
- Modify: `server/repair/StoreRepairer.scala` (`repairUnackedEntry` and the scan) and `server/repair/RepairTarget.scala`.
- Modify: `Host.scala` and `host/StoreRouter.scala` (the RepairTarget implementation).
- Test: `src/test/scala/org/aspen_ddp/aspen/server/repair/StoreRepairerSuite.scala`.

**Interfaces:**
- **Consumes:** Task 2's Frontend stub API.
- **Produces** on `RepairTarget`:
  - `def appliedStubs(storeId: StoreId): Future[Set[TransactionId]]`
  - `def dropAppliedStub(storeId: StoreId, txid: TransactionId): Unit`

  Both run on the store's executor.
- **Behavior:**
  - Deleting entry (S, T) also drops S's stub for T.
  - After walking S's UnackedCommits range, the scan drops every stub of S whose txid had no entry in that walk.

- [ ] **Step 1: Write the failing tests**

```scala
test("deleting an UnackedCommits entry drops the store's stub for it")
test("a stub with no entry is dropped by the next scan")                        // Review Focus 5
test("a stub whose entry is still present is kept")
```

- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run `sbt 'testOnly *StoreRepairerSuite *UnackedCommitsIntegrationSuite'`, then `sbt test`.**
  Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -am "repair scan cleans applied stubs"`

---

### Task 6: Documentation

**Files:**
- **`src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`:** scaladoc on `commit()` and `result`, stating the
  accepted corner case verbatim in meaning (addendum section C). Required.
- **`Architecture.md`, "Transaction Model":** extend the paragraph "It's worth pointing out that the transaction
  recovery process is pessimistic…". Say that recovery and other store-side drivers send prefer-abort prepares, so
  they never gather fresh commit votes. Then add one sentence for the corner case.
- **`UsageGuide.md`, "What to take away":** add one bullet after the `TransactionAborted` bullets. An abort is final
  except in that corner case, and a successful retry closes it.
- **`CLAUDE.md`, Transaction Design:** add one terse line for prefer-abort and one for applied stubs, in the existing
  style.
- **`docs/superpowers/specs/2026-11-09-replay-pins-design.md`:** add a pointer to the addendum in "Amendments during
  implementation".
- **`docs/safety/*`:**
  - P-N4: narrowed to the duplicate-client-prepare corner case;
  - R-N1/X11: narrowed to a self-healing minority, per the addendum's residual;
  - P-N1: re-check against prefer-abort;
  - README tier tables;
  - any finding whose cited lines moved.

- [ ] **Step 1: Edit.** Verify each statement against the code.
- [ ] **Step 2: Run `sbt -batch compile`.** Expected: success. This step covers scaladoc only.
- [ ] **Step 3: Commit.** `git commit -am "docs: prefer-abort, applied stubs, and the accepted abort corner case"`
