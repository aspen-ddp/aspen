# Replay Pins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the wall-clock horizon with transactions that cannot be replayed: a client-enforced one-pin rule,
per-write checks by default, and owed-commit entries so that a late (proven) commit is applied only where a commit is
owed.

**Architecture:**
- **Server.** The TracelessCommits tree becomes UnackedCommits, with an entry for every committed transaction's
  unacknowledged commit voters. Drivers classify commits learned after forgetting as `late`. A Tx applies a late
  commit only if it holds its own commit vote and its entry exists. The horizon is deleted.
- **Client.** `TransactionBuilder` refuses pinless transactions unless `replaySafe(reason)` is called. TKVL planners
  derive per-key checks. Product sites (core, amoebafs, vdisk) are converted.

**Tech Stack:** Scala 3, sbt, ScalaTest (`AnyFunSuite`, `AsyncFunSuite` via `IntegrationTestSuite`), Scaffeine.

**Spec:** `docs/superpowers/specs/2026-11-09-replay-pins-design.md`. Call-site inventory with file and line:
`docs/superpowers/specs/2026-11-09-replay-pin-rule-analysis.md`.

## Global Constraints

- **Branch:** work and commit on `pin_refactor` (not main).
- **Testing:** test-first for every step that changes behavior.
- **Full runs:** `sbt test` with a 70 s hang bound, no flaky tests, no new "Hung Test" prints. CPU may be at 100%.
- **Compatibility:** backward compatibility with persisted data and wire formats is not required.
- **Style:** Scala 3 quiet syntax, indented if/then/else. Use os-lib for filesystem work.
- **Threading:** `Store`, `Frontend` and `Tx` run on the store's executor and take no locks. `TransactionDriver`
  keeps its own `synchronized`.
- **Pin definition** (exact, from the spec): a pin is any of
  - a `DataUpdate` at a required revision other than `ObjectRevision.Allocating`;
  - a `VersionBump`;
  - a `KeyValueUpdate` with `requiredRevision = Some(_)`;
  - a `KeyRevision(k, _)` with an `Insert(k, _)` or `Delete(k)` in the same update;
  - a `RefcountUpdate` with `newRefcount != requiredRefcount`.

  Allocations are never pins.
- **Late commit** (exact, from the spec): a commit resolution reached when the driver holds a proof and has prepare
  responses from a Paxos prepare quorum of acceptors, none carrying an accepted value; or when a store reported T
  finalized committed. A driver holding a proof never proposes abort.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A proof in the live window** (an acceptor quorum still remembers T) must give a *normal* commit with
   finalization actions run, never a late one. A genuine voter that missed messages must apply. Pinned in Task 6.
2. **A driver that holds a proof and cannot reach a prepare quorum** (acceptors offline) must neither propose abort
   nor resolve. It keeps retrying with its normal round timer, and its Tx keeps its locks. Pinned in Task 6.
3. **A Tx with its own commit vote whose entry lookup fails** (metadata pool unreachable) must hold the late commit and
   retry at heartbeat, never drop it. Pinned in Task 7.
4. **Derived per-write checks on a contended TKVL key** must turn a lost blind race into an abort and a successful
   retry, never a stuck retry loop or a `KeyAlreadyExists` surfaced to callers that never asked for it. Pinned in
   Task 9.
5. **amoebafs restarting with pending tasks** must resume them, never reuse their task objects. A task of an unknown
   type must be left alone and logged, never treated as inactive. Pinned in Task 1.

---

### Task 1: Register amoebafs types and repair duplicate detection

**Root cause, found during planning:**
- `amoebafs.impl.StaticTypeRegistry.types` is never passed to any client. `run_amoebafs` builds its client with
  `Nil` (`cmdline/Main.scala:1251`), so `TypeRegistry.apply` never sees the duplicate `c1fb782f-…`.
- `SimpleTaskExecutor` (`compute/impl/SimpleTaskExecutor.scala:66-68`) files a task of an unknown type as `inactive`.
  `prepareTask` then reuses it blindly. So after an amoebafs restart, its pending tasks are overwritten.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/StaticTypeRegistry.scala`. Replace `types` with
  `val factories: List[RegisteredTypeFactory]`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`. `run_amoebafs` passes
  `StaticTypeRegistry.factories` as `userTypeFactories`; add a `createAspenClient(session, onetworkCacheDir,
  userTypeFactories)` overload or parameter.
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileContent.scala:283`. Give
  `DeleteFileContentTask` the fresh UUID `6f1c2a9e-3b7d-4e58-9a41-d2c0b8e7f310`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala:66-68`. Unknown type: log at
  error with the task pointer and type UUID, and add it to neither `active` nor `inactive`.
- Modify: the amoebafs integration suites under `src/test/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/`. Override
  `userTypeFactories = StaticTypeRegistry.factories`, preferably in one shared base trait if the suites have one.
- Test: `src/test/scala/org/aspen_ddp/aspen/client/TypeRegistrySuite.scala` (create).
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/SimpleTaskExecutorSuite.scala` (create, an `IntegrationTestSuite`).

**Interfaces:**
- Produces: `StaticTypeRegistry.factories: List[RegisteredTypeFactory]`.

- [ ] **Step 1: Write the failing tests**

```scala
// TypeRegistrySuite
test("the built-in and amoebafs type lists register without duplicates"):
  noException should be thrownBy TypeRegistry(common.TypeFactories.factories, client.TypeFactories.factories,
    server.TypeFactories.factories, StaticTypeRegistry.factories)

test("two factories sharing a UUID fail registration naming both classes"):
  val e = intercept[TypeRegistry.DuplicateTypeRegistration](TypeRegistry(List(fa), List(fb))) // fa, fb: same UUID
  e.getMessage should (include (fa.getClass.getTypeName) and include (fb.getClass.getTypeName))

// SimpleTaskExecutorSuite
test("a task of an unregistered type is neither resumed nor reused"):
  // enroll a task whose factory is registered, then build a second executor over the same root with a client
  // lacking that factory; prepareTask on it allocates a new task object instead of reusing the pending one,
  // and the pending task's contents are unchanged
```

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *TypeRegistrySuite *SimpleTaskExecutorSuite'`
Expected: the first test FAILS with `DuplicateTypeRegistration: c1fb782f-7f13-4921-8ddf-155123445730` (after
`factories` exists). The executor test FAILS because the pending task's object is reused.

- [ ] **Step 3: Implement**

The `factories` rename, the `run_amoebafs` wiring, the suites' `userTypeFactories` overrides, and the unknown-type
handling. Then give `DeleteFileContentTask` its fresh UUID.

- [ ] **Step 4: Run them to verify they pass**

Run: `sbt 'testOnly *TypeRegistrySuite *SimpleTaskExecutorSuite *amoebafs*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "amoebafs: register its types with the client; unknown task types are not reused"
```

---

### Task 2: Replay test harness and premise tests

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`, near `dropTxMessage` (`:243`). Add:
  - `@volatile var recordTxMessage: TxMessage => Boolean`, default false;
  - `def takeRecordedTxMessages(): List[TxMessage]`;
  - `def deliverTransactionMessage(msg: TxMessage): Unit`, which delivers through `hostFor(msg.to)` the way
    `cliMessenger.sendTransactionMessage` does, with `handleEvents()` before and after.

  Messages from both the client messenger and the store messenger are recorded.
- Create: `src/test/scala/org/aspen_ddp/aspen/server/ReplayIntegrationSuite.scala`. It extends
  `IntegrationTestSuite` with `testIda = ReedSolomon(5, 3, 4)`, `recordCrl = true`, `recoveryDrivers = true`, and
  heartbeat 300 ms, as `TracelessCommitsIntegrationSuite.testHostOptions` does. Reuse its `pump`/`eventually`/
  `settle` pattern.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/RequirementsCheckerSuite.scala` (premise).

**Interfaces:**
- Produces: `TestNetwork.recordTxMessage`, `takeRecordedTxMessages()` and `deliverTransactionMessage(msg)`, used by
  Tasks 7 and 13.
- Produces: in `ReplayIntegrationSuite`, `private def replayClientPrepares(txid: TransactionId): Future[Unit]`, which
  waits until `net.smgr.control.txStatusCache.getStatus(txid).isEmpty`, then delivers every recorded `TxPrepare` with
  `fromClient = true` for `txid`.

- [ ] **Step 1: Write the replay test (expected to fail today)**

```scala
test("a replayed pinned transaction does not re-apply its blind delete over a re-inserted key"):
  // T: overwrite data object X at its revision (the pin) + blind Delete(k) on KV object Y (tx.update(y, None, None,
  //    Nil, List(Delete(k)))). Record T's client prepares. Commit, settle.
  // T2: Insert(k, v2) on Y with DoesNotExist(k). Commit, settle.
  // replayClientPrepares(T.id); settle(clock = true)
  // every store of Y still holds k with value v2; client.read(y) shows k -> v2
```

Wrap the body in `pendingUntilFixed` so the suite stays green. Task 7 removes the wrapper.

- [ ] **Step 2: Verify the premise "it fails today"**

Run it once without `pendingUntilFixed`: `sbt 'testOnly *ReplayIntegrationSuite'`.
Expected: FAIL, with k absent on the stores of Y. If it *passes*, stop: the premise is wrong and the user must be told
before continuing.

- [ ] **Step 3: Write the premise tests**

```scala
// RequirementsCheckerSuite
test("a revision requirement on an object absent from the store fails"):
  // RequirementsChecker.check with a RevisionLock (and a DataUpdate) for an object id with no ObjectState
  // in `objects`: the result's object error map contains that id
```

**Ordering premise:** `TxFinalized` is sent only after the finalizer completes (`TransactionDriver.onResolution` →
`f.complete foreach onFinalized`), and a store forgets a committed Tx only on `TxFinalized` or the TracelessCommits
local apply. Record this as a scaladoc paragraph on `TransactionDriver.onResolution`, citing
`Frontend.receiveFinalized` and `Tx.tracelessCommitFound`. If reading shows any other forget path for a committed
transaction (grep `deleteTransactionState`, `forget`), stop and report it.

- [ ] **Step 4: Run**

Run: `sbt 'testOnly *ReplayIntegrationSuite *RequirementsCheckerSuite'`
Expected: PASS, with the replay test reported as pending.

- [ ] **Step 5: Commit**

```bash
git commit -am "test: replay harness, failing replay scenario (pending), and premise tests"
```

---

### Task 3: Rename TracelessCommits to UnackedCommits

This is a mechanical rename with no behavior change.

**Files** (every hit of `grep -rln "Traceless" src`):

| Old | New |
|---|---|
| `StoragePoolState.TracelessCommitsTreeKey` | `UnackedCommitsTreeKey` |
| `StoragePool.tracelessCommitsTree` | `unackedCommitsTree` |
| `TracelessCommitEntry` (file and object) | `UnackedCommitEntry` |
| `TracelessCommitLookup` (file and trait) | `UnackedCommitLookup` |
| `Host.tracelessLookup` | `unackedLookup` |
| `Frontend.TracelessLookupTimeout` | `UnackedLookupTimeout` |
| `Frontend.requestTracelessLookup` / `startTracelessLookup` / `tracelessLookupComplete` / `retryTracelessLookups` | `…Unacked…` |
| `Tx.needsTracelessCheck` | `needsUnackedCheck` |
| `Tx.tracelessCommitFound` / `tracelessCommitAbsent` | `unackedCommitFound` / `unackedCommitAbsent` |
| `StoreRepairer.repairTracelessEntry` | `repairUnackedEntry` |
| `MissedUpdateMarker.MissedTracelessCommit` | `MissedUnackedCommit` |
| `TestNetwork.HostOptions.wrapTracelessLookup` | `wrapUnackedLookup` |
| `TracelessCommitsIntegrationSuite` | `UnackedCommitsIntegrationSuite` |

**Kept:** `TransactionDescription.tracelessObjects` and `KeyValueUpdate.deletesKeys`. They still describe traceless
changes.

- [ ] **Step 1: Rename** with the IDE-style edits above. Update the log text in `RepairService.scala:190`.
- [ ] **Step 2: Run** `sbt test`. Expected: PASS, the same as before the rename.
- [ ] **Step 3: Commit:** `git commit -am "rename TracelessCommits to UnackedCommits"`

---

### Task 4: Entries for every committed transaction; lookup for every recovered commit vote

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationAction.scala:88-98`.
  The entry is written for every voter that gets one under today's voter rule, whatever `tracelessObjects` says.
  Entry objects are the objects T changes on that store (`RequirementsApplyer.changedObjects` restricted to the
  store's pool).
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala:126-135`. `needsUnackedCheck` drops its
  traceless condition: `recovered && disposition == VoteCommit && txStatus == Unresolved`.
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationActionSuite.scala`.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/UnackedCommitsIntegrationSuite.scala`.

**Interfaces:**
- Produces: the entry meaning used by Task 7. Entry `(store, txid)` exists ⇒ the store owes T.

- [ ] **Step 1: Write the failing tests**

```scala
// MissedUpdateFinalizationActionSuite
test("a voter without TxCommitted gets an UnackedCommits entry for a data overwrite too"):
  // transaction with only an overwrite; one counted voter sends no TxCommitted -> entry (store, txid) whose objects
  // contain the overwritten object; voters that sent TxCommitted get none

// FrontendSuite
test("a recovered commit vote of a non-traceless transaction applies on load when its entry exists"):
  // recovered VoteCommit/Unresolved for an overwrite; the lookup answers found -> the update is applied, CRL state
  // deleted, the Tx forgotten
```

In `UnackedCommitsIntegrationSuite`, the test "a recovered commit vote for a committed data overwrite is proven and
applied" asserts `marked shouldBe empty` (`:261`). Change it to `marked.keySet should contain (2)`.

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *MissedUpdateFinalizationActionSuite *FrontendSuite *UnackedCommitsIntegrationSuite'`
Expected: the new tests and the amended assertion FAIL.

- [ ] **Step 3: Implement** the two changes.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -am "unacked commits: entries for every transaction, lookup for every recovered vote"`

---

### Task 5: The `late` flag on the wire and in the status cache

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala`.
  - Add `late: Boolean` (last field, after `committed`) to `TxResolved` and `TxFinalized`.
  - Add `late: Boolean` to `TxStatusResponse.TxStatus`.
  - Include it in `toString`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`. Encode and decode the new fields.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionStatusCache.scala`.
  - `Entry(status, finalized, late: Boolean = false)`.
  - `updateStatus(txid, status, finalized = false, late = false)`.
- Modify: every constructor call of these messages. Pass `late = false` for now (`grep -rn "TxResolved(\|TxFinalized("
  src`). Tasks 6 and 7 set it.
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/transaction/TransactionStatusCacheSuite.scala`.

**Interfaces:**
- Produces:
  - `TxResolved(to, from, transactionId, committed, late)`;
  - `TxFinalized(to, from, transactionId, committed, late)`;
  - `TxStatusResponse.TxStatus(status, finalized, late)`;
  - `TransactionStatusCache.Entry.late`.

- [ ] **Step 1: Write the failing tests.** Round-trip `TxResolved` and `TxFinalized` with `late = true` and
  `late = false`, and `TxStatusResponse` with a late status. In the cache: `updateStatus(id, Committed, finalized =
  true, late = true)`, then `getStatus(id).get.late` is true.
- [ ] **Step 2: Run** `sbt 'testOnly *CodecRoundTripSuite *TransactionStatusCacheSuite'`. Expected: compile failure,
  then FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `sbt test`. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -am "late flag on TxResolved, TxFinalized and status entries"`

---

### Task 6: Driver: proofs never abort, late commits run no finalization actions

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriver.scala`.
  - `receiveTxResolved` (`:345`): a `TxResolved(committed = true)` from a store with `late = false` that this driver
    did not send is a **proof**. Today proofs are the only `TxResolved` a driver receives from a participant store.
    Confirm this by reading `Tx.sendProvenResolution` and the store's `TxResolved` sends. If it is not so, add a
    `proof: Boolean` instead of inferring.
  - Add `private var holdsProof = false`. A proof sets it; it no longer calls `onResolution` directly.
  - `receiveTxResolved` with `late = true` resolves late at once.
  - `receiveTxPrepareResponse` (`:230`), after `proposer.receivePromise`: if `holdsProof`, and the promises received
    in the current round from valid acceptors reach `proposer.prepareQuorumReached` with every `lastAccepted ==
    None`, resolve late. If `holdsProof` and any promise carries an accepted value, let normal Paxos proceed.
  - `setLocalProposal`: never `false` while `holdsProof`. If the vote tally says abort, leave the proposal unset and
    wait for the next round or more responses.
  - `TxStatusResponse` handling: a status `Committed` with `finalized = true`, or with `late = true`, resolves late.
  - **Late resolution:** set `late = true` on the driver. Run **no** finalizer. Call `onFinalized(true)`. Every
    `TxResolved` and `TxFinalized` the driver sends afterwards (`onResolution`'s messages, `onFinalized`,
    `sendPrepareMessages`, `sendPrepareMessage`) carries `late = true`. The client notification is unchanged.
  - Record the ordering premise from Task 2 in the scaladoc of the late path.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriverSuite.scala`.

**Interfaces:**
- Consumes: Task 5's message fields.
- Produces: the guarantee that drivers send `late = true` on every outcome message for a late commit. Task 7 relies on
  it.

- [ ] **Step 1: Write the failing tests** (in the suite's existing driver-harness style)

```scala
test("a proof with a quorum of memoryless promises resolves late, runs no finalizer, and sends late everywhere")
test("a proof with an accepted commit among the promises resolves normally and runs the finalizer")  // Review Focus 1
test("a driver holding a proof never proposes abort, even when votes say abort")
test("a driver holding a proof without a prepare quorum neither resolves nor proposes")              // Review Focus 2
test("a finalized-committed status response resolves late")
test("a late TxResolved resolves late at once")
```

Each asserts on the messages captured from the test messenger:
- the `late` flag on every `TxResolved` and `TxFinalized`;
- whether `finalizerFactory.create` was called (use a counting factory);
- that no `TxAccept` with value `false` was sent.

- [ ] **Step 2: Run** `sbt 'testOnly *TransactionDriverSuite'`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `sbt 'testOnly *TransactionDriverSuite *UnackedCommitsIntegrationSuite *FrontendSuite'`.
  Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -am "driver: proofs never abort; late commits skip finalization"`

---

### Task 7: Tx: late commits apply only where a commit is owed

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala`.
  - `receiveResolved` and `receiveFinalized`: when `m.committed && m.late`, call a new `private def
    lateCommit(): Unit`.
    - If `disposition == VoteCommit`, recovered or not: request the unacked lookup (Task 3's names; today's
      `needsUnackedCheck` hold machinery). Found → `unackedCommitFound` (apply, log at info, forget). Absent →
      `applyNothing()`. A failed or unanswered lookup holds and retries at heartbeat (`Frontend.UnackedLookupTimeout`).
    - Otherwise: `applyNothing()`.
  - `private def applyNothing(): Unit` does the following, with no `doCommit` and no `TxCommitted`:
    - `frontend.deleteTransactionState(transactionId)`;
    - unlock and release objects;
    - `statusCache.updateStatus(transactionId, Committed, finalized = true, late = true)`;
    - forget the Tx as finalized.
  - `proveCommitted` (`:530-536`): send `TxResolved(lastProposer, storeId, txid, committed = true, late = false)` (a
    proof). Instead of `resolved(true)`, call `applyNothing()`. A prover voted abort, so it applies nothing.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala`.
  - A `TxResolved` or `TxFinalized` with `late = true` for an unknown transaction is recorded in the status cache with
    `late = true`, and finalized for `TxFinalized` (`:629-640`).
  - Status answers carry `late`.
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/ReplayIntegrationSuite.scala`. Remove `pendingUntilFixed`.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`.

**Interfaces:**
- Consumes: Task 5's fields; Task 6's late messages; Task 4's entries.

- [ ] **Step 1: Write the failing tests** (FrontendSuite)

```scala
test("a live Tx with its own commit vote applies a late commit when its entry exists")
test("a live Tx with its own commit vote applies nothing on a late commit when its entry is absent")
test("a Tx with its own commit vote holds a late commit while the lookup fails, and applies once it succeeds") // RF 3
test("a Tx without a commit vote applies nothing on a late commit and sends no TxCommitted")
test("a prover applies nothing to its other objects")
test("a late TxFinalized for an unknown transaction is cached as finalized, committed and late")
```

- [ ] **Step 2: Run** `sbt 'testOnly *FrontendSuite *ReplayIntegrationSuite'`. Expected: the new tests and the
  replay test FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `sbt 'testOnly *FrontendSuite *ReplayIntegrationSuite *UnackedCommitsIntegrationSuite
  *TransactionDriverSuite'`. Expected: PASS. Then run `sbt test`. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -am "late commits apply only on stores owed them; provers apply nothing"`

---

### Task 8: The client pin check (not yet enforced by default)

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/Pins.scala` with
  `object Pins { def hasPin(requirements: List[TransactionRequirement], kvOps: Map[ObjectId, List[KeyValueOperation]]):
  Boolean }`, implementing the Global Constraints definition exactly.
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/TransactionError.scala`. Add `final case class
  UnpinnedTransaction(transactionId: TransactionId) extends TransactionCreationError`. Its scaladoc states the rule
  and `replaySafe`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`. Add `def replaySafe(reason: String): Unit`,
  whose scaladoc says it records that replaying this transaction is harmless, and that the reason goes into the notes.
- Modify: `TransactionImpl.scala` and `TransactionBuilder.scala`. Store the flag, and add `note(s"replay-safe: $reason")`.
  In `buildTranaction`, after the automatic allocation requirements and before the `require(requirements.nonEmpty)`
  line: if `TransactionBuilder.enforcePins && !replaySafe && !Pins.hasPin(...)`, throw `UnpinnedTransaction`.
- Add: `object TransactionBuilder { @volatile var enforcePins: Boolean = false }`. This is a temporary switch: Task 12
  removes it and enforces always.
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilderSuite.scala`.

**Interfaces:**
- Produces: `Transaction.replaySafe(reason: String): Unit`, `UnpinnedTransaction`, `Pins.hasPin`, and
  `TransactionBuilder.enforcePins` (until Task 12).

- [ ] **Step 1: Write the failing tests** (`enforcePins = true` in the suite)

```scala
// one test per pin form -> buildTranaction succeeds:
//   overwrite at a revision; bumpVersion; update with Some(revision); KeyRevision(k) + Insert(k);
//   KeyRevision(k) + Delete(k); setRefcount with a different refcount
// one test per non-pin form -> intercept[UnpinnedTransaction]:
//   allocation only; lockRevision only; DoesNotExist(k) + Insert(k); Exists(k) + Delete(k);
//   KeyRevision(k) + Insert(other key); setRefcount(r, r)
test("replaySafe lets a pinless transaction through and records the reason in its notes")
test("replaySafe on a pinned transaction changes nothing")
```

- [ ] **Step 2: Run** `sbt 'testOnly *TransactionBuilderSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `sbt 'testOnly *TransactionBuilderSuite'`. Expected: PASS.
- [ ] **Step 5: Produce the empirical inventory.** Run `sbt test` with `enforcePins` defaulting to `true` locally (do
  not commit that default). Record every `UnpinnedTransaction` origin (stack frame in product code, or test name) in
  `docs/superpowers/plans/2026-11-09-replay-pins-inventory.md`. Compare it with the analysis's inventory and add any
  site the analysis missed to Task 10 or 11's list in that file.
- [ ] **Step 6: Commit** (with the default `false`):
  `git commit -am "client pin check and replaySafe (enforcement off until conversions land)"`

---

### Task 9: TKVL per-write checks by default, with a blind opt-in

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/PutRequirement.scala`:

```scala
enum PutRequirement:
  case Checked                         // default: KeyRevision if present, DoesNotExist if absent
  case MustNotExist                    // DoesNotExist; fails with KeyAlreadyExists if present (old Left(true))
  case AtRevision(rev: ObjectRevision) // KeyRevision(rev) (old Right(rev))
  case Blind                           // no key requirement (old None); for idempotent upserts only
```

- Modify: replace `requirement: Option[Either[Boolean, ObjectRevision]]` with `requirement: PutRequirement =
  PutRequirement.Checked` in:
  - `TieredKeyValueList.set`;
  - `TierEngine.set`;
  - `KeyValueListNode.insert` (both);
  - `KeyValueListNode.planInsert` (`:491`);
  - `BatchOp.Put`.
- Modify `TKVLInsert`: `requireDoesNotExist: Boolean` stays, and `blind: Boolean = false` is added. Mapping:
  `requireDoesNotExist` → `MustNotExist`, `blind` → `Blind`, else `Checked`.
- **`Checked` and the existing-key retry rule.** With `Checked`, a put of a key that exists in the node read gets
  `KeyRevision(k, thatRevision)`. One that is absent gets `DoesNotExist(k)`. That `DoesNotExist` failing must surface
  as an ordinary abort (retry), not `KeyAlreadyExists`: only `MustNotExist` maps to `KeyAlreadyExists`.
- Modify `KeyValueListNode` delete planning (`:624`, `:874`, `:944`, `:957-958`, rename `:540-547`,
  `deleteOneAtATime :1114`): `Exists(k)` becomes `KeyRevision(k, rev)`, with `rev` from the node contents already
  held. A delete of a key absent from the node read keeps today's not-found behavior.
- Modify the rename new key: `KeyRevision` if present, else `DoesNotExist`.
- Modify `DataObjectNodeEncoding`: `Checked` and `Blind` add nothing (the node revision covers them).
  `AtRevision` keeps raising `UnsupportedRequirement`.
- Modify callers using `Some(Left(true))` / `Some(Right(rev))` / `None` (`grep -rn "Left(true)\|Right(.*revision)"
  src/main`) to the new cases. Today's `None` callers become `Checked`, except `MissedUpdateMarker` (Task 10 sets
  `Blind`).
- Modify the tkvl CLAUDE.md: the put/delete requirement rules.
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueNodePlanSuite.scala`.
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLBatchSuite.scala`.

**Interfaces:**
- Produces: `PutRequirement` and `TKVLInsert.blind`, used by Tasks 10 and 11.

- [ ] **Step 1: Write the failing tests**

```scala
// KeyValueNodePlanSuite
test("a Checked put of a present key requires its KeyRevision")
test("a Checked put of an absent key requires DoesNotExist")
test("a Blind put adds no key requirement")
test("a planned delete requires the key's KeyRevision, not Exists")
test("rename requires KeyRevision on the old key and KeyRevision or DoesNotExist on the new key")
// TKVLBatchSuite (integration)
test("a Checked put that loses a race to another writer of the key aborts and succeeds on retry")   // Review Focus 4
test("a Checked put racing a concurrent insert of the same absent key retries, never KeyAlreadyExists")
```

- [ ] **Step 2: Run** `sbt 'testOnly *KeyValueNodePlanSuite *TKVLBatchSuite'`. Expected: FAIL (compile, then
  assertions).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `sbt test`. Expected: PASS. Note any new "Hung Test" prints in TKVL suites. If they appear,
  investigate before committing.
- [ ] **Step 5: Commit:** `git commit -am "tkvl: per-write checks by default; explicit blind upsert"`

---

### Task 10: Core site conversions

**Files** (lines from the analysis inventory):
- **`MissedUpdateMarker.writeBatch`:** `TKVLInsert(..., blind = true)`, and
  `tx.replaySafe("missed-update marks are idempotent")`.
- **`RepairProofs.proveKey` / `proveObjectLevel` / `proveRefcount`:** `tx.replaySafe("repair proof changes nothing")`.
- **`StoreRepairer.deleteEntry` (`:355-361`):** delete with `KeyRevision` from the entry it read.
- **`SplitFinalizationAction.insertIntoExistingTier` (`:66-67`)** and **`AllocationFinalizationAction.execute`
  (`:28`):**
  - Read the new node or object. If it is absent, complete without a transaction.
  - If its entry is already present with this value, complete.
  - Otherwise use a `Checked` insert plus `tx.lockRevision(ptr, readRevision)`, and
    `tx.replaySafe("cannot re-add once the node/object is gone: its revision lock fails")`.
- **`JoinFinalizationAction.deleteFromExistingTier` (`:83`) and `DeletionFinalizationAction` (`:28`):** already
  `KeyRevision` through Task 9. Verify. If an entry is already gone, complete.
- **`SystemTaskServiceState.enroll` / `remove`:** `Checked`. If the transaction then has no pin, add the task state
  object's revision (`tx.bumpVersion(taskState, rev)`).
- **`SimpleTaskExecutor.prepareTask` / `deallocateTask`:** `tx.update(taskPtr, Some(readRevision), ...)`. The executor
  reads the task object (cache the revision from its scan and its own commits).
- **`SimpleTaskExecutor.allocateTask`:** `tx.update(executorObject, Some(executorRevision), ...)` (node-revision pin).
- **Node-revision pin** at `Registry.prepareRegister`, `MetadataTree.preparePut` and
  `SimpleDurableServiceExecutor.registerService`. The TKVL node holding the key gets `requiredRevision`: add
  `TieredKeyValueList.prepareSetPinned(key, value, requirement)` (KV trees only). It locates the containing node and
  stages the update with `requiredRevision = Some(node.revision)`.
- **Every other core site** listed in the Task 8 inventory file.
- Test: one test per converted site in its existing suite:
  - `MissedUpdateFinalizationActionSuite`;
  - `StoreRepairerSuite`;
  - `JoinFinalizationActionSuite`;
  - `SimpleTaskExecutorSuite`;
  - `DurableServiceSuite`;
  - the registries suites under `client/registries`.

  Each asserts the pin, or the `replaySafe` mark, through `Pins.hasPin` on the built description, using
  `TransactionBuilder.enforcePins = true` in that suite.
- Add the finalization-action tests:

```scala
test("a split FA run a second time after the first committed completes without change")
test("a split FA run after its node was updated completes")
test("a split FA run after its node was deleted completes without writing")
// the same three for AllocationFinalizationAction
```

**Interfaces:**
- Consumes: `PutRequirement`, `TKVLInsert.blind` (Task 9), and `replaySafe` (Task 8).
- Produces: `TieredKeyValueList.prepareSetPinned(key: Key, value: Value, requirement: PutRequirement)(using
  Transaction): Future[Unit]`.

- [ ] **Step 1: Write the failing tests** above.
- [ ] **Step 2: Run** the named suites. Expected: FAIL.
- [ ] **Step 3: Convert** the sites.
- [ ] **Step 4: Run** `sbt test`. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -am "core: every transaction pinned or marked replay-safe"`

---

### Task 11: amoebafs and vdisk conversions, and the move fix

**Files:**
- **`amoebafs/impl/simple/SimpleDirectory.scala`:**
  - `prepareInsert` (`:79-80`): `MustNotExist` when absent, `AtRevision(vs.revision)` when present.
  - `prepareDelete`: `KeyRevision` (Task 9).
  - `prepareRename` (`:129-170`): the new key gets `AtRevision`/`MustNotExist`.
  - `freeResources` and `DeleteDirectoryContentTask` enrollment: through the task executor, which is pinned by Task 10.
- **`amoebafs/nfs/AmoebaNFS.scala:366-380`:** sequence `s.prepareDelete`, `d.prepareInsert` and
  `prepareSetParentDirectory` into the body's returned future. Do not return `Future.unit`.
- **`amoebafs/impl/simple/UnlinkFileTask.scala`, `SimpleFileContent.scala`, `SimpleInodeTable.scala`:** each site in
  the Task 8 inventory file, converted per the spec's section 4.
- **`vdisk/DiskCatalog.scala`:**
  - `removeKey` (`:241-249`): `KeyRevision` from its `tree.get`.
  - `create` (`:55-78`): the registry put through `prepareSetPinned`.
- Test: `src/test/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/DirectorySuite.scala`.
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/DiskCatalogSuite.scala` (or the suite holding `DiskCatalog` tests).

**Interfaces:**
- Consumes: Tasks 9 and 10.

- [ ] **Step 1: Write the failing tests**

```scala
// DirectorySuite
test("inserting an absent name requires that it does not exist: a concurrent create of the name is not overwritten")
test("a cross-directory move stages its delete and insert before committing")
//   the move's transaction description contains the source delete and the destination insert
// DiskCatalogSuite
test("create pins the registry node and delete requires the entry's revision")
```

- [ ] **Step 2: Run** `sbt 'testOnly *DirectorySuite *DiskCatalogSuite'`. Expected: FAIL.
- [ ] **Step 3: Convert.**
- [ ] **Step 4: Run** `sbt test`. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -am "amoebafs, vdisk: pinned transactions; await move's prepares"`

---

### Task 12: Enforce pins always

**Files:**
- Modify: `TransactionBuilder.scala`. Delete `enforcePins` and enforce unconditionally.
- Modify: every test that still builds a pinless transaction. Give it a natural pin, or call
  `tx.replaySafe("test")` where the test is not about replay. The Task 8 inventory lists them.
- Modify: suites that set `enforcePins` (remove those lines).

- [ ] **Step 1: Delete the switch.**
- [ ] **Step 2: Run** `sbt test`. Expected: some `UnpinnedTransaction` failures. Each one is a test, or a product
  site missed by Tasks 10 and 11. Fix product sites as in Tasks 10 and 11, with a test.
- [ ] **Step 3: Run** `sbt test` until it passes. Expected: PASS, with no "Hung Test" prints beyond those listed in
  `Todo.txt`.
- [ ] **Step 4: Commit:** `git commit -am "enforce the pin rule for every client transaction"`

---

### Task 13: Remove the horizon

**Files:**
- **`server/Host.scala:69-73`:** delete `txHorizon`. At `:192-193`, construct `TransactionStatusCache` without an
  early-eviction age.
- **`server/transaction/TransactionStatusCache.scala`:** delete `Entry.insertedAt`, `EarlyEvictionWarnEvery`,
  `earlyEvictionAge`, the counters, `evicted`, the listener branch and `earlyEvictions`.
- **`server/store/Store.scala`, `Frontend.scala`, `host/StoreLoader.scala:336`:** delete the `horizon` parameter.
  `Frontend.receivePrepare` (`:665-686`) computes `tooOld = muted` only. Rename `tooOld` to `muted` throughout `Tx`.
- **`common/transaction/RequirementError.scala:26-30`:** delete `TransactionTooOld`, and its codec case.
- **`Tx.determineDisposition` (`:458-463`):** the branch applies to muted Txs only. Its abort reason becomes a new
  `StoreMuted` non-object error, or reuse an existing one if a fitting one exists.
- Tests: delete the horizon tests (`FrontendSuite:718-786, 870-934`, `TransactionStatusCacheSuite:29-45`). Keep the
  muted tests.
- Add to `ReplayIntegrationSuite`, using Task 2's harness:

```scala
test("a replayed pinned KV insert after a later delete of its key changes nothing")
test("a replayed pinned data overwrite after a later overwrite changes nothing")
test("a store that missed T's prepare and applied a later write to the key skips T's stale checked write")
//   per-write checked T; drop T's client prepare to store i, deliver T2 to all, then deliver T's prepare and
//   outcome to store i; store i keeps T2's value and gets an ErrorTree entry; repair leaves it consistent
```

- [ ] **Step 1: Write the three tests.** Run `sbt 'testOnly *ReplayIntegrationSuite'`. Expected: PASS already, since
  pins, late commits and per-write checks are in place. If any test fails, fix that before removing anything.
- [ ] **Step 2: Remove** the horizon code and its tests.
- [ ] **Step 3: Run** `sbt test`. Expected: PASS. Then `grep -rn "txHorizon\|TransactionTooOld\|earlyEviction" src`.
  Expected: no output.
- [ ] **Step 4: Commit:** `git commit -am "remove the horizon"`

---

### Task 14: Project docs

**Files:**
- **`CLAUDE.md`, Transaction Design:**
  - Replace the "Horizon (`Host.txHorizon` …)" paragraph with the pin rule, `replaySafe` and `UnpinnedTransaction`.
  - Rewrite the proof-of-commit paragraph: proofs never decide abort, and late commits apply only where an
    UnackedCommits entry is owed.
  - Rewrite the "Changes that leave no trace…" paragraph for UnackedCommits: entries for every transaction's
    unacknowledged counted voters.
  - Update the "Metadata layout" line's tree name.
  - Update the TKVL bullet on `prepareUpdates`/`requireDoesNotExist` with `PutRequirement` and `TKVLInsert.blind`.
  - Remove the status-cache sentence that ties it to the horizon.
- **`Todo.txt`:** replace the first item with a one-line pointer to the design. Add the unverified findings:
  - `UnlinkFileTask` step 2 running unpinned transactions during step preparation;
  - DataObject `deleteList` using `lockRevision` rather than `bumpVersion`;
  - `JoinFinalizationAction.setNewRoot` deleting the old root with only a refcount update;
  - joins triggered through `StoreRepairer.deleteEntry` registering no `JoinFinalizationAction`;
  - `LocalTimeRequirement` `Equals` looking inverted.
- **`docs/superpowers/specs/2026-11-08-traceless-commits-design.md`:** a header note on what this design superseded
  (the horizon section and the tree name).

- [ ] **Step 1: Edit.**
- [ ] **Step 2: Check** with `grep -rn "txHorizon\|TracelessCommits\|TransactionTooOld" CLAUDE.md src/main`. Expected:
  hits only in historical notes.
- [ ] **Step 3: Commit:** `git commit -am "docs: pin rule, late commits, UnackedCommits"`

---

### Task 15: Refresh `docs/safety/*`

**Files:** every file in `docs/safety/`:
- `README.md`
- `paxos-transactions.md`
- `transaction-recovery.md`
- `transaction-requirements.md`
- `repair-process.md`
- `crash-recovery-log.md`
- `rebuild-process.md`
- `systemic-loss-protection.md`
- `cross-document-review.md`

The refresh is against the final code on this branch, not the plan.

- [ ] **Step 1: Remove what no longer exists:** the horizon, `TransactionTooOld`, the TracelessCommits name and
  proof-first ordering. Describe the pin rule, per-write checks, late commits and UnackedCommits where each document
  covers that ground.
- [ ] **Step 2: Re-assess every existing finding** against the code, and mark each **fixed**, **narrowed** or
  **unchanged** with a one-line reason and file:line evidence. Expected, to be confirmed and not assumed:
  - Paxos F3 / Recovery F7 and X3: fixed.
  - Paxos F2: narrowed.
  - Requirements F1 and Recovery F4: narrowed where per-write checks apply.
- [ ] **Step 3: Look for new weaknesses** this change introduces, and record each as a new finding with a verdict
  (CONFIRMED / PLAUSIBLE / refuted), in each document's existing finding format. At minimum examine:
  - the late-commit evidence rule (quorum memorylessness, finalized-status reports, the X3 status downgrade's
    interaction with `late`);
  - the liveness of the "no abort while holding a proof" rule (a driver that never gets a quorum);
  - UnackedCommits growth, lookups, and repair-scan cleanup for every transaction;
  - `replaySafe` claims at each marked site;
  - TKVL derived checks;
  - finalization actions that read before writing;
  - the spec's residual risk (a recovering acceptor plus a replayed client prepare).
- [ ] **Step 4: Update `README.md`'s tier tables** to match the documents.
- [ ] **Step 5: Commit:** `git commit -am "docs/safety: refresh for replay pins"`
