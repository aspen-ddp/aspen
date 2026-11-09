# Traceless Commits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A recovered commit vote for a committed transaction is never dropped. Missed-update repair of KeyValue
objects and refcounts is exact. A prepare that arrives after its transaction left the status cache is never applied
as a new transaction.

**Architecture:**
- **New tree:** a per-pool `TracelessCommits` TKVL, written by `MissedUpdateFinalizationAction` through
  `MissedUpdateMarker` only when a store misses a commit that leaves no trace. A recovered `VoteCommit` looks its
  entry up and applies the update locally.
- **Repair:** `StoreRepairer` walks both trees in one scan and repairs through an exact reconciliation. Each
  reconciliation takes a store snapshot, then a lock-reporting read, then a checked install. Keys that stay locked
  fall back to proof transactions.
- **Horizon:** `Frontend` votes `TransactionTooOld` on unknown prepares older than the status cache duration minus
  1 s.

**Tech Stack:** Scala 3, sbt, ScalaTest, ScalaPB (`src/main/protobuf/codec.proto`), Scaffeine.

**Spec:** `docs/superpowers/specs/2026-11-08-traceless-commits-design.md`. Read it first: it holds the argument for
why each rule is safe. This plan does not repeat it.

## Global Constraints

- **Branching and commits:** commit directly on main. Every commit message ends with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Testing:**
  - TDD: watch each new test fail before writing the code that passes it.
  - Flaky tests are not permitted. Poll for conditions; never sleep for a fixed time and assume.
  - Full runs: `timeout 70 sbt test` under normal load is the hang bound.
- **No migration:** persisted formats may change without migration (no backward compatibility).
- **Store threading:** everything touching a `Store`, `Frontend` or `Tx` runs on that store's executor. Results from
  other threads come back through `StoreRouter.onStore` / `route` (CLAUDE.md, Host threading model).
- **Simplicity:** prefer simple and correct code over optimized recovery on rare paths.
- **Values fixed by the spec:**
  - fallback after **10** consecutive deferring passes;
  - horizon = `Host.txStatusCacheDuration(heartbeatPeriod)` − **1 s**;
  - the recovery apply logs at **info**.

## Review Focus

1. **The metadata pool stays unreachable during recovery.** A recovered `VoteCommit` whose lookups keep failing must
   hold every abort indefinitely and keep its locks. Test in Task 5.
2. **One key of a big KV object is permanently locked.** Every other key is reconciled in the first pass. Only that
   key waits, then goes to the proof transaction. Test in Task 8.
3. **A commit lands on the store between the snapshot and the install.** It must survive, and the key is retried on
   the next pass. Test in Task 7.
4. **A transaction touches objects in two pools.** Each missed store gets its entry in its own pool's tree, naming
   only the objects it hosts. Test in Task 4.
5. **A late client prepare carrying data, 6 to 7 s after start but still in the status cache,** is answered from the
   cache, not voted `TransactionTooOld`. Test in Task 9.

---

### Task 1: Guard the lock-retention invariant

The spec's trust argument needs a resolved-committed transaction to keep its write locks until its commit is applied
in memory, including while the commit is held.

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`. Add a section
  `"Write locks are held until a commit is applied"` before
  `"Requirement locks held between a commit vote and resolution"`.
- Modify, only if a test fails: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala`.

**Interfaces:**
- Consumes: existing FrontendSuite helpers (`withDataless`, `mkPrepare`, `clientPrepare`).
- Produces: nothing new.

- [ ] **Step 1: Write the tests**

```scala
test("a commit held for missing data keeps its write locks until applied"):
  // Driver prepare without data -> VoteCommit (data arrives later); TxResolved(committed=true) arrives first.
  // Before the client's data prepare arrives:
  assert(frontend.objectStateFor(oid).lockedWriteTransactions.contains(txid))
  // After the data prepare arrives and the commit is applied:
  assert(!frontend.objectStateFor(oid).lockedWriteTransactions.contains(txid))

test("a commit held behind an in-flight commit of its object keeps its write locks until applied"):
  // T1 commits with its backend commit held incomplete; T2 on the same object resolves committed.
  assert(frontend.objectStateFor(oid).lockedWriteTransactions.contains(t2))   // while T1's commit is in flight
  // complete T1's backend commit
  assert(!frontend.objectStateFor(oid).lockedWriteTransactions.contains(t2))
```

Use whatever accessor FrontendSuite already uses to inspect object state. Add a `private[store]` one on `Frontend`
only if none exists.

- [ ] **Step 2: Run**

Run: `sbt 'testOnly *FrontendSuite -- -z "keeps its write locks"'`
Expected: PASS. If either test fails, stop and fix `Tx` so locks are released only in `releaseObjects` after the
commit is applied, then rerun.

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala
git commit -m "test: write locks of a held commit last until it is applied"
```

---

### Task 2: The `deletesKeys` flag

**Files:**
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionRequirement.scala` (`KeyValueUpdate`)
  - `src/main/protobuf/codec.proto` (`message KeyValueUpdate`)
  - `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (KeyValueUpdate encode/decode)
  - `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilder.scala` (where `KVUpdate`
    becomes a `KeyValueUpdate` requirement)
  - `src/main/scala/org/aspen_ddp/aspen/common/transaction/RequirementError.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/store/RequirementsChecker.scala` (the `case r: KeyValueUpdate`
    branch)
- Test:
  - `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`
  - `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilderSuite.scala`
  - `src/test/scala/org/aspen_ddp/aspen/server/store/RequirementsCheckerSuite.scala`

**Interfaces:**
- Produces:
  - `KeyValueUpdate(objectPointer, requiredRevision, contentLock, requirements, deletesKeys: Boolean = false)`.
    Scaladoc: set when the operations contain a key `Delete`. `DeleteMin`/`DeleteMax`/`DeleteLeft`/`DeleteRight` do
    not set it, and a use of them without a required revision is a traceless change this flag does not cover.
  - `case class UnflaggedDelete() extends RequirementError`, purely local like `RequirementCheckFailure`.
  - Proto: `bool deletes_keys = 5;` in `message KeyValueUpdate`.

- [ ] **Step 1: Write the failing tests**

```scala
// CodecRoundTripSuite
test("KeyValueUpdate round-trips deletesKeys"):
  val r = KeyValueUpdate(kvPtr, None, None, Nil, deletesKeys = true)
  assert(roundTrip(r) == r)

// TransactionBuilderSuite
test("an update with a key Delete sets deletesKeys"):
  b.update(kvPtr, None, None, Nil, List(Delete(Key("a")), Insert(Key("b"), Array(1))))
  assert(kvRequirement(b, kvPtr).deletesKeys)
test("an update without a key Delete leaves deletesKeys false"):
  b.update(kvPtr, None, None, Nil, List(Insert(Key("b"), Array(1)), DeleteRight()))
  assert(!kvRequirement(b, kvPtr).deletesKeys)

// RequirementsCheckerSuite
test("a key Delete without deletesKeys is rejected"):
  val (objErrors, _) = check(KeyValueUpdate(kvPtr, None, None, Nil, deletesKeys = false), ops = List(Delete(k)))
  assert(objErrors(kvPtr.id) == UnflaggedDelete())
test("a flagged key Delete passes"):
  assert(check(KeyValueUpdate(kvPtr, None, None, Nil, deletesKeys = true), ops = List(Delete(k)))._1.isEmpty)
```

Use each suite's existing helpers for building descriptions, extracting requirements and calling the checker.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *CodecRoundTripSuite *TransactionBuilderSuite *RequirementsCheckerSuite -- -z deletesKeys'`
Expected: compilation failure (no `deletesKeys`).

- [ ] **Step 3: Implement**

- Add the field, the proto field and the codec mapping.
- `TransactionBuilder` sets `deletesKeys = operations.exists(_.isInstanceOf[Delete])` (the key `Delete` class from
  `common.objects`).
- In `RequirementsChecker`, after `ops` is decoded: if
  `ops.exists(_.exists(_.isInstanceOf[Delete])) && !r.deletesKeys`, throw `ObjectErr(r.objectPointer.id, UnflaggedDelete())`.
- Check every other construction of `KeyValueUpdate` in `src/main` and `src/test` with
  `grep -rn "KeyValueUpdate(" src`. The default `false` keeps them compiling. Any that pass a `Delete` outside
  `TransactionBuilder` must set `true`.

- [ ] **Step 4: Run tests**

Run: `sbt 'testOnly *CodecRoundTripSuite *TransactionBuilderSuite *RequirementsCheckerSuite *FrontendSuite'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src/main src/test
git commit -m "tx: KeyValueUpdate carries a deletesKeys flag, checked by stores"
```

---

### Task 3: Create the TracelessCommits tree

**Files:**
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala` (both pool objects built near lines 175-195)
  - `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (`createPoolObj`, near line 500)
  - `src/main/scala/org/aspen_ddp/aspen/client/StoragePool.scala`
  - `src/main/scala/org/aspen_ddp/aspen/client/internal/pool/SimpleStoragePool.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/BootstrapMetadataSuite.scala`, plus the suite that covers
  `createNewStoragePool` (find it with `grep -rln createNewStoragePool src/test`).

**Interfaces:**
- Produces:
  - `StoragePoolState.TracelessCommitsTreeKey = Key(Array[Byte](6))`
  - `StoragePool.tracelessCommitsTree: TieredKeyValueList` (`private[aspen]`, like `errorTree`)

- [ ] **Step 1: Write the failing tests**

```scala
test("the bootstrap and initial metadata pools have a TracelessCommits tree"):
  for poolId <- List(bootstrapPoolId, initialMetadataPoolId) do
    val pool = await(client.getStoragePool(poolId))
    await(insertAndGet(pool.tracelessCommitsTree, Key(Array[Byte](1, 2)), Value(Array[Byte](3))))

test("a new storage pool has a TracelessCommits tree"):
  val pool = await(client.getStoragePool(await(createPool())))
  await(insertAndGet(pool.tracelessCommitsTree, Key(Array[Byte](1, 2)), Value(Array[Byte](3))))
```

`insertAndGet` commits one insert and asserts that `get` returns the value.

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *BootstrapMetadataSuite -- -z TracelessCommits'`
Expected: compilation failure.

- [ ] **Step 3: Implement**

Allocate one more tree root wherever an ErrorTree root is allocated. Use the same allocator and
`Root(0, ByteArrayKeyOrdering, ...)` shape, and store it under `TracelessCommitsTreeKey`. Open it in
`SimpleStoragePool` exactly as `errorTree` is opened.

- [ ] **Step 4: Run tests**

Run: `sbt 'testOnly *BootstrapMetadataSuite'` and the pool-creation suite.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "pool: each pool gets a TracelessCommits tree"
```

---

### Task 4: The finalizer writes TracelessCommits entries

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TracelessCommitEntry.scala`
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionDescription.scala`
  - `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationAction.scala`
  - `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateMarker.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationActionSuite.scala`

**Interfaces:**
- Produces:
  - `TransactionDescription.tracelessObjects: Set[ObjectId]`: objects with a `RefcountUpdate`, plus objects with a
    `KeyValueUpdate` where `requiredRevision.isEmpty && deletesKeys`.
  - In `TracelessCommitEntry.scala`:

    ```scala
    final case class TracelessCommitEntry(timestamp: HLCTimestamp, objects: List[ObjectPointer]):
      def encode: Array[Byte]
    object TracelessCommitEntry:
      def decode(bytes: Array[Byte]): TracelessCommitEntry
      /** poolIndex byte then the transaction UUID, big-endian: 17 bytes, like MissedUpdateMarker.errorKey */
      def key(store: StoreId, txid: TransactionId): Key
      def decodeTransactionId(key: Key): TransactionId
    ```

    Pointers are encoded with `ObjectPointer.toArray` (length-prefixed), so store pointers survive.
  - `MissedUpdateMarker.mark(updates: List[MissedUpdate], traceless: List[MissedTracelessCommit] = Nil): Future[Unit]`
  - `final case class MissedTracelessCommit(store: StoreId, transactionId: TransactionId, entry: TracelessCommitEntry)`
    in `object MissedUpdateMarker`.

- [ ] **Step 1: Write the failing tests**

```scala
test("a store that sent no TxCommitted for a refcount change gets a TracelessCommits entry"):
  // txd: RefcountUpdate on dataPtr hosted by stores 0..2; commitErrors from stores 0,1 only
  runMark(txd, commitErrors = Map(s0 -> Nil, s1 -> Nil))
  assert(tracelessEntries(pool) == Map(TracelessCommitEntry.key(s2, txid) -> TracelessCommitEntry(txd.startTimestamp, List(dataPtr))))

test("a flagged KV delete without a required revision gets an entry; an insert-only update does not"):
  ...
  assert(tracelessEntries(pool).keySet == Set(TracelessCommitEntry.key(s2, deleteTx)))

test("a store that sent TxCommitted with object errors gets no TracelessCommits entry"):
  runMark(txd, commitErrors = Map(s0 -> Nil, s1 -> Nil, s2 -> List(dataPtr.id)))
  assert(tracelessEntries(pool).isEmpty)

test("a transaction spanning two pools writes each store's entry to its own pool's tree, naming only its objects"):
  assert(tracelessEntries(poolA).values.forall(_.objects.forall(_.poolId == poolA.poolId)))
  assert(tracelessEntries(poolB).values.forall(_.objects.forall(_.poolId == poolB.poolId)))

test("TracelessCommits entries are written in the same transaction as the pool's ErrorTree marks"):
  // Count commits through the MissedUpdateMarker.commitErrorTreeTransaction seam: one per pool
```

`tracelessEntries(pool)` reads the pool's `tracelessCommitsTree` over the full key range and decodes the values.

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *MissedUpdateFinalizationActionSuite -- -z TracelessCommits'`
Expected: compilation failure.

- [ ] **Step 3: Implement**

- In `markMissedUpdates`, for each store in the `commitErrors.get(storeId) == None` case, compute
  `txd.allHostedObjects(storeId).filter(p => txd.tracelessObjects.contains(p.id))`. If that is non-empty, add a
  `MissedTracelessCommit`.
- `MissedUpdateMarker` queues traceless entries per pool beside the ErrorTree entries, merged by key (keys are unique
  per transaction).
- `writeBatch` calls `pool.tracelessCommitsTree.prepareUpdates(tracelessInserts, Nil)` in the same `tx` as the
  ErrorTree inserts, before `limitTracking` and the commit.
- `mark`'s future completes when both kinds of entry are written.

- [ ] **Step 4: Run tests**

Run: `sbt 'testOnly *MissedUpdateFinalizationActionSuite *MissedUpdateReportSuite'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src && git commit -m "finalizer: record missed traceless commits in the pool's TracelessCommits tree"
```

---

### Task 5: Recovery looks up TracelessCommits and applies locally

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/store/TracelessCommitLookup.scala`
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (constructor, lookup bookkeeping, heartbeat
    retry)
  - `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala` (constructor passes it to Frontend; `heartbeat`
    calls `frontend.retryTracelessLookups()`)
  - `src/main/scala/org/aspen_ddp/aspen/server/host/StoreLoader.scala` (passes the loader context's lookup)
  - `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` (builds the host implementation)
  - the `StoreLoader` context class that carries `txStatusCache` / `finalizerFactory`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`, new section
  `"A recovered commit vote learns its commit from the TracelessCommits tree"`.

**Interfaces:**
- Produces:

```scala
/** Looks up a store's TracelessCommits entry for a transaction. The answer is delivered on the store's executor by
  * calling Frontend.tracelessLookupComplete(transactionId, result); result is Success(found) or a Failure. */
trait TracelessCommitLookup:
  def lookup(storeId: StoreId, transactionId: TransactionId): Unit
object TracelessCommitLookup:
  /** For tests and stores without a host: never answers */
  val Never: TracelessCommitLookup
```

- `Frontend(..., tracelessLookup: TracelessCommitLookup = TracelessCommitLookup.Never)`
- `Frontend.tracelessLookupComplete(txid: TransactionId, result: Try[Boolean]): Unit`
- `Frontend.retryTracelessLookups(): Unit`, called each store heartbeat. It re-issues failed lookups.
- `Tx.needsTracelessCheck: Boolean` = `recovered && disposition == VoteCommit && status == Unresolved && oresolution.isEmpty`
- Host implementation:
  - reads `client.getStoragePool(storeId.poolId)`, then
    `pool.tracelessCommitsTree.get(TracelessCommitEntry.key(storeId, txid))`;
  - delivers the result through `deviceControl.router` `route(storeId, ...)` to
    `store.frontend.tracelessLookupComplete`;
  - drops the result if the store is gone.

- [ ] **Step 1: Write the failing tests** (FrontendSuite, with a `ManualLookup` fake that records requests and lets
  the test answer them)

```scala
test("a recovered commit vote with a TracelessCommits entry applies its update and is forgotten"):
  // CRL holds T: VoteCommit, Unresolved, a KV Delete of k on kvPtr. Store state still has k.
  lookup.answer(txid, Success(true))
  assert(!kvState(kvPtr).content.contains(k))
  assert(!frontend.transactions.contains(txid))
  assert(crl.deleted.contains(txid))
  assert(logged(Level.Info, s"applied committed transaction $txid"))

test("a recovered commit vote without an entry accepts an abort"):
  lookup.answer(txid, Success(false))
  deliver(TxFinalized(storeId, driver, txid, committed = false))
  assert(kvState(kvPtr).content.contains(k)); assert(!frontend.transactions.contains(txid))

test("an abort arriving before the entry is visible waits for a later lookup and then commits"):
  lookup.answer(txid, Success(false))
  deliver(TxResolved(storeId, driver, txid, committed = false))
  assert(lookup.requests.count(_ == txid) == 2)      // a fresh lookup started after the abort
  assert(frontend.transactions(txid).status == TransactionStatus.Unresolved)
  lookup.answer(txid, Success(true))
  assert(!kvState(kvPtr).content.contains(k))

test("a failing lookup holds the abort and is retried at heartbeat"):
  lookup.answer(txid, Failure(new Exception("metadata pool down")))
  deliver(TxFinalized(storeId, driver, txid, committed = false))
  frontend.retryTracelessLookups(); lookup.answer(txid, Failure(new Exception("still down")))
  assert(frontend.transactions(txid).status == TransactionStatus.Unresolved)
  assert(lockedWriteTransactions(kvPtr).contains(txid))

test("an object whose requirements fail on the local apply is skipped and the Tx still completes"):
  // store's k is at a different revision than T's KeyRevision requires
  lookup.answer(txid, Success(true))
  assert(!frontend.transactions.contains(txid))
```

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *FrontendSuite -- -z TracelessCommits'`
Expected: compilation failure.

- [ ] **Step 3: Implement**

- **Starting the lookup:** in `Tx.completeLoad`, when `needsTracelessCheck`, call
  `frontend.requestTracelessLookup(this)`. Frontend records the txid as pending, then calls
  `tracelessLookup.lookup`.
- **Holding an abort:** in `Tx.receiveResolved` and `Tx.receiveFinalized`, when `needsTracelessCheck` and the message
  says abort, stash the message instead of acting on it. Then request a fresh lookup; a lookup already in flight does
  not count.
- **`tracelessLookupComplete`:**
  - `Success(true)`: log at info
    `s"Store $storeId applied committed transaction $txid learned from the TracelessCommits tree (objects: ...)"`.
    Drop any stashed abort. Run the same path `receiveFinalized(committed = true)` runs (`resolved(true)`, set
    `ofinalized`, `frontend.transactionFinalized` or `finalizeDeferred`, status cache finalized), without a message.
  - `Success(false)`: replay the stashed abort, if any, through the normal handler.
  - `Failure`: keep the stashed abort and mark the txid for `retryTracelessLookups`.
- **The driver:** the heartbeat driver is untouched. If it learns commit from a status cache, the Tx resolves as
  today and the pending lookup result is ignored (`needsTracelessCheck` is false).

- [ ] **Step 4: Run tests**

Run: `sbt 'testOnly *FrontendSuite *StoreSuite *TransactionDriverSuite'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src && git commit -m "tx: a recovered commit vote applies a commit recorded in the TracelessCommits tree"
```

---

### Task 6: Read responses report locked keys

**Files:**
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/server/store/ObjectState.scala`
  - `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala` (`ReadResponse.CurrentState`, including
    `equals`)
  - `src/main/protobuf/codec.proto` (`message ReadResponse`)
  - `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (ReadResponse encode/decode)
  - `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (where `CurrentState` is built, about line 765)
  - `src/main/scala/org/aspen_ddp/aspen/client/internal/read/StoreState.scala`
  - `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseObjectReader.scala`
  - `src/main/scala/org/aspen_ddp/aspen/client/ObjectState.scala`
- Test: `CodecRoundTripSuite`, `ObjectStateSuite` (server), and a reader test in the existing client read suites
  (find one with `grep -rln KeyValueObjectReader src/test`).

**Interfaces:**
- Produces:
  - server `ObjectState.lockedKeys: Set[Key]` (keys whose `ValueState.lockedToTransaction` is set, plus
    `absentKeyLocks` keys)
  - server `ObjectState.objectLevelLocked: Boolean` (`lockedToTransaction`, `contentLocked` or `rangeLocks`)
  - `CurrentState(..., lockedWriteTransactions, lockedKeys: Set[Key] = Set(), objectLocked: Boolean = false)`
  - proto `repeated bytes locked_keys = 14; bool object_locked = 15;`
  - client `final case class ReadLocks(keys: Set[Key], objectLevel: Boolean)` with
    `object ReadLocks { val None: ReadLocks = ReadLocks(Set(), false) }`
  - client `ObjectState.readLocks: ReadLocks`, a `private[aspen] var` defaulting to `ReadLocks.None`. The reader sets
    it, in `attemptRestore`, to the union over every successful store response, not only the matching ones.

- [ ] **Step 1: Write the failing tests**

```scala
// ObjectStateSuite (server)
test("lockedKeys reports locked present keys and absent-key locks; objectLevelLocked reports object locks"):
  kvs.content(k1).lockedToTransaction = Some(t1); kvs.absentKeyLocks += (k2 -> t2)
  assert(os.lockedKeys == Set(k1, k2)); assert(!os.objectLevelLocked)
  os.lockedToTransaction = Some(t3); assert(os.objectLevelLocked)

// CodecRoundTripSuite
test("ReadResponse round-trips lockedKeys and objectLocked"):
  assert(roundTrip(rr(lockedKeys = Set(k1, k2), objectLocked = true)) == rr(lockedKeys = Set(k1, k2), objectLocked = true))

// reader suite
test("a restored object's readLocks is the union of every responder's locks"):
  // responses: store0 locks k1, store1 locks k2 + object, store2 none
  assert(state.readLocks == ReadLocks(Set(k1, k2), objectLevel = true))
```

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *ObjectStateSuite *CodecRoundTripSuite -- -z ocked'`
Expected: compilation failure.

- [ ] **Step 3: Implement** the fields, the codec mapping, the server fill-in and the reader union.

- [ ] **Step 4: Run tests**

Run: `sbt 'testOnly *ObjectStateSuite *CodecRoundTripSuite *FrontendSuite'` and the reader suite.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src && git commit -m "read: responses report locked keys and object-level locks"
```

---

### Task 7: Exact reconciliation, replacing timestamp-gated repair

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/store/Reconcile.scala`. It holds the pure plan and the snapshot
  type, so the arithmetic can be tested without a running system, as with the TKVL `plan*` functions.
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala`: replace `readObjectForRepair`/`repair` with
    snapshot and reconcile. `RepairRead` becomes `SnapshotRead` and `ReconcileRead`.
  - `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/host/StoreRouter.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/Host.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairTarget.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/repair/StoreRepairer.scala` (`repairUpdate`)
- Test:
  - Create `src/test/scala/org/aspen_ddp/aspen/server/store/ReconcileSuite.scala` (pure plan)
  - `StoreRepairerSuite` (integration)
  - `HostRepairTargetSuite`
  - the `RecordingTarget` fakes in `StoreRepairerSuite` and `RepairServiceSuite`

**Interfaces:**
- Produces, in `Reconcile.scala`:

```scala
final case class RepairSnapshot(objectId: ObjectId, revision: ObjectRevision, refcount: ObjectRefcount,
                                timestamp: HLCTimestamp, keyRevisions: Map[Key, ObjectRevision],
                                objectLevel: Option[KVObjectLevel])          // None for DataObjects / missing objects
final case class KVObjectLevel(min: Option[Key], max: Option[Key], left: Option[Value], right: Option[Value])
final case class ReconcileResult(deferredKeys: Set[Key], objectLevelDeferred: Boolean, stale: Boolean):
  def complete: Boolean = deferredKeys.isEmpty && !objectLevelDeferred && !stale
object Reconcile:
  /** What to change on the store. `read` is the store's IDA slice view of the restored object. Keys in `locks`, and
    * object-level parts when `locks.objectLevel`, are deferred; anything differing from `snapshot` in `current` is
    * left alone and reported stale. */
  def plan(snapshot: RepairSnapshot, current: RepairSnapshot, read: ClientObjectState, storeId: StoreId,
           locks: ReadLocks): ReconcilePlan
```

  `ReconcilePlan` holds: the key installs `Map[Key, ValueState slice]`, the key deletes `Set[Key]`, an optional new
  refcount, optional object-level parts and revision, an optional new data slice (DataObjects), and the resulting
  `ReconcileResult`.
- `RepairTarget` replaces `repair` with:

```scala
def snapshot(storeId: StoreId, pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit
def reconcile(storeId: StoreId, snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult]): Unit
```

  Both fail with `StoreNotHosted` exactly as `repair` does. `repairDelete` is unchanged.
- Rules `plan` implements:
  - **Refcount:** take the read's refcount when its serial is higher than the store's, on any object type.
    `locks.objectLevel` defers it.
  - **DataObjects:** install data and revision when `read.timestamp > store timestamp` (today's rule), under the
    snapshot check.
  - **KeyValueObjects:**
    - for every key in the union of store keys and read keys that is not in `locks.keys` and is unchanged from
      `snapshot`: install the read's version when it differs or the store lacks the key, and delete when the read
      lacks it;
    - make the object-level parts and the revision equal to the read's unless `locks.objectLevel`;
    - the object timestamp becomes `max(store, read)`.

- [ ] **Step 1: Write the failing integration test that shows the gap** (StoreRepairerSuite)

```scala
test("a store that missed a KV insert and then applied a later update to another key gets the missed key"):
  // Commit insert k1 with store s2 offline (it misses it and is marked). Bring s2 back, commit insert k2 (s2 applies it).
  scanUntilQuiet(s2)
  assert(storeKeys(s2, kvPtr) == Set(k1, k2)); assert(errorEntries(s2).isEmpty)

test("a store that missed a KV delete and then applied a later update to another key loses the deleted key"):
  ...
  assert(!storeKeys(s2, kvPtr).contains(k1))

test("a store that missed a refcount change on a DataObject and then applied a later data write gets the refcount"):
  assert(storeRefcount(s2, dataPtr) == expected)
```

Run: `sbt 'testOnly *StoreRepairerSuite -- -z "and then applied a later"'`
Expected: FAIL. The missed key, deleted key or refcount is still wrong after the scan, which confirms the gap the
spec describes.

- [ ] **Step 2: Write the failing pure tests** (ReconcileSuite)

```scala
test("a key the store lacks is installed from the read")
test("a key the read lacks is deleted from the store")
test("a key whose read version differs is replaced")
test("a key changed since the snapshot is left alone and the result is stale")
test("a locked key is deferred and every other key is reconciled")
test("an object-level lock defers revision, min, max, left, right and refcount")
test("refcount follows the higher serial and never decreases")
test("a DataObject keeps the timestamp rule for its data, while its refcount follows the serial")
test("the object timestamp becomes the larger of the two")
```

Each asserts on the `ReconcilePlan` fields and on `ReconcileResult.complete`.

- [ ] **Step 3: Run them to verify they fail**

Run: `sbt 'testOnly *ReconcileSuite'`
Expected: compilation failure.

- [ ] **Step 4: Implement `Reconcile.plan`**

Compute the store's slice of each read value the way `KeyValueObjectState.getRebuildDataForStore` does for the whole
object, but per key: `ida.encode(value)(storeId.poolIndex)`.

- [ ] **Step 5: Wire it into the store**

- `Frontend.snapshotForRepair` builds a `RepairSnapshot` from the loaded state.
- `Frontend.reconcileForRepair` loads the state, takes the current snapshot, calls `Reconcile.plan` and applies the
  plan to the `ObjectState`. It commits through `backend.repair` (the same path `repair` uses today) and completes
  `p` with the result after the backend completes. A missing object uses an empty `RepairSnapshot`, as
  `repairMissing` does today.
- `StoreRepairer.repairUpdate`:
  1. `snapshot`;
  2. `client.read`;
  3. `reconcile`, using `read.readLocks`;
  4. delete the ErrorTree entry only when `result.complete && read.timestamp >= missedTimestamp`.

  It returns the `ReconcileResult`, so Task 8 can track deferrals.

- [ ] **Step 6: Write the snapshot-race test** (Review Focus 3, StoreRepairerSuite)

```scala
test("a commit applied between the snapshot and the install survives and is retried next pass"):
  // RecordingTarget-free: a real host; a test seam in StoreRepairer pauses between read and reconcile while a
  // transaction inserting k3 commits on s2.
  assert(storeKeys(s2, kvPtr).contains(k3)); assert(errorEntries(s2).nonEmpty)
  scanUntilQuiet(s2); assert(errorEntries(s2).isEmpty)
```

- [ ] **Step 7: Run tests**

Run: `sbt 'testOnly *ReconcileSuite *StoreRepairerSuite *HostRepairTargetSuite *RepairServiceSuite *FrontendSuite'`
Expected: PASS, including Step 1's gap tests.

- [ ] **Step 8: Commit**

```bash
git add -A src && git commit -m "repair: reconcile KV objects key by key and refcounts by serial"
```

---

### Task 8: One scan for both trees, deferral, and the proof fallback

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairProofs.scala`
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/server/repair/StoreRepairer.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairService.scala` (owns the deferral state across scans)
  - `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairPolicy.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairTarget.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/Host.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/host/StoreRouter.scala`
- Test: `StoreRepairerSuite`, `RepairServiceSuite`, `RepairPolicySuite`; create `RepairProofsSuite` (integration).

**Interfaces:**
- Consumes: `TracelessCommitEntry` (Task 4), `ReconcileResult`/`ReadLocks` (Tasks 6-7).
- Produces:
  - `RepairTarget.holdsTransaction(storeId: StoreId, txid: TransactionId): Boolean`. It reads the store's
    `transactions` key set through a `@volatile` snapshot, like `hasTransactions`.
  - `RepairPolicy(..., proofAfterDeferrals: Int)`, default **10**.
  - `final class DeferralState`, owned by `RepairService` and passed to each `StoreRepairer`:
    - `record(entryKey: Key, result: ReconcileResult): Int` returns consecutive deferring passes;
    - `clear(entryKey: Key)`;
    - `deferredKeys(entryKey: Key): Option[Set[Key]]`.
  - `ScanResult` gains `tracelessSeen: Int`.
  - In `RepairProofs`, each returns `Future[Boolean]`, where true means committed. Every proof calls
    `disableMissedUpdateTracking()`.

```scala
object RepairProofs:
  def proveKey(client: AspenClient, ptr: KeyValueObjectPointer, key: Key, readRevision: Option[ObjectRevision]): Future[Boolean]
  def proveObjectLevel(client: AspenClient, read: KeyValueObjectState): Future[Boolean]    // FullContentLock + required revision, no ops
  def proveRefcount(client: AspenClient, ptr: ObjectPointer, refcount: ObjectRefcount): Future[Boolean] // RefcountUpdate(r -> r)
```

- [ ] **Step 1: Write the failing tests**

```scala
// StoreRepairerSuite
test("a TracelessCommits entry for a transaction the store holds live is skipped"):
  target.live += (s2 -> txid); scan(s2); assert(tracelessEntries(s2).nonEmpty)
test("a TracelessCommits entry is reconciled and deleted once the store no longer holds it"):
  scanUntilQuiet(s2); assert(tracelessEntries(s2).isEmpty); assert(!storeKeys(s2, kvPtr).contains(k1))
test("a TracelessCommits entry for a deleted object deletes it from the store"):
  assert(!storeHasObject(s2, dataPtr.id)); assert(tracelessEntries(s2).isEmpty)
test("after a rebuild, the first scan clears the store's TracelessCommits entries"):
  ...
test("one permanently locked key is deferred while every other key of the object is reconciled at once"):  // Review Focus 2
  assert(storeKeys(s2, kvPtr) == expectedWithout(kLocked)); assert(deferral.deferredKeys(entryKey).contains(Set(kLocked)))
test("a key deferred for proofAfterDeferrals passes is proven and reconciled"):
  // lock released only to the proof (test seam), 10 passes, then:
  assert(storeKeys(s2, kvPtr) == expected)
test("an aborted proof leaves the key deferred for a later pass"):
  ...
test("a refcount under an object lock is deferred and then proven"):
  ...

// RepairPolicySuite
test("proofAfterDeferrals defaults to 10")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *StoreRepairerSuite *RepairPolicySuite -- -z "TracelessCommits|deferred|proven|proofAfterDeferrals"'`
Expected: compilation failure.

- [ ] **Step 3: Implement**

- `scan` walks `pool.tracelessCommitsTree.foreachInRange(min, max, ...)` after the ErrorTree range, with the same
  `isRepairable` early-out.
- Per entry:
  - skip when `holdsTransaction`;
  - otherwise, for each object, use `repairDeletion`'s path when the allocation is gone (no age wait is needed:
    the transaction committed and is finalized), and run Task 7's reconciliation otherwise;
  - delete the entry with a revision-guarded delete, like `deleteErrorEntryIfUnchanged`, once every object is
    `complete`.
- On a later pass, an entry with `DeferralState.deferredKeys` reconciles only those keys. Pass the key filter into
  `reconcile`, or ignore the others in `plan`; the implementer picks one.
- When `record` reaches `proofAfterDeferrals`, run the matching `RepairProofs` call for each deferred part. On true,
  reconcile those parts from the read the proof used.

- [ ] **Step 4: Run tests**

Run: `sbt 'testOnly *StoreRepairerSuite *RepairServiceSuite *RepairPolicySuite *RepairProofsSuite *HostRepairTargetSuite'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src && git commit -m "repair: one scan walks ErrorTree and TracelessCommits, with proofs for keys that stay locked"
```

---

### Task 9: The horizon rule

**Files:**
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/server/Host.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (`receivePrepare`, `case None`)
  - `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala` (`decideDisposition`)
  - `src/main/scala/org/aspen_ddp/aspen/common/transaction/RequirementError.scala`
  - `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionStatusCache.scala`
  - `Store.scala` and `StoreLoader.scala` (pass the horizon through)
- Test: `FrontendSuite`; create `TransactionStatusCacheSuite` if none exists.

**Interfaces:**
- Produces:
  - `Host.txHorizon(heartbeatPeriod: Duration): FiniteDuration = txStatusCacheDuration(heartbeatPeriod) - 1.second`
  - `Frontend(..., horizon: Duration = Duration.Inf)`. Infinite means off, which keeps existing tests unchanged.
  - `case class TransactionTooOld() extends RequirementError`, local. It is recorded as a non-object error.
  - `Tx(..., tooOld: Boolean = false)`
  - `TransactionStatusCache(cacheDuration, maxEntries, earlyEvictionAge: Option[FiniteDuration] = None)`. With an age
    set, a size eviction of an entry younger than that age logs at warn, at most once a minute, with the number of
    such evictions since the last warning.

- [ ] **Step 1: Write the failing tests**

```scala
test("an unknown prepare older than the horizon gets a TransactionTooOld abort vote and takes no locks"):
  // horizon 6 s; txd.startTimestamp = now - 7 s; nothing in the status cache
  assert(votes(net) == List(TransactionDisposition.VoteAbort)); assert(lockedWriteTransactions(oid).isEmpty)
test("an unknown prepare older than the horizon whose update an object shows is answered TxResolved(committed)"):
  assert(resolvedMessages(net).map(_.committed) == List(true))
test("a late client prepare with data inside the status cache is answered from the cache"):  // Review Focus 5
  // startTimestamp = now - 6.5 s, finalized entry in the cache
  assert(votes(net).isEmpty); assert(!frontend.transactions.contains(txid))
test("an unknown prepare younger than the horizon is handled as today"):
  assert(votes(net) == List(TransactionDisposition.VoteCommit))
test("Host.txHorizon is the status cache duration minus one second"):
  assert(Host.txHorizon(1.second) == 6.seconds)

// TransactionStatusCacheSuite
test("a size eviction of an entry younger than the early-eviction age is counted and warned"):
  val c = new TransactionStatusCache(7.seconds, maxEntries = 1, earlyEvictionAge = Some(6.seconds))
  c.updateStatus(t1, Committed); c.updateStatus(t2, Committed); c.cleanUpForTest()
  assert(c.earlyEvictions == 1)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `sbt 'testOnly *FrontendSuite *TransactionStatusCacheSuite -- -z "horizon|TransactionTooOld|early-eviction|status cache"'`
Expected: compilation failure.

- [ ] **Step 3: Implement**

- In `receivePrepare`'s `case None`, compute `tooOld = HLCTimestamp.now - m.txd.startTimestamp > horizon` and pass
  it to the new `Tx`.
- In `Tx.decideDisposition`, when `tooOld && oresolution.isEmpty && !outcomeKnown`:
  - if `appliedHere.nonEmpty`, call `proveCommitted()`;
  - otherwise set the disposition to `VoteAbort`, recording `TransactionTooOld()` as a non-object error, without
    running requirement checks or `lock()`.
- The host passes `Host.txHorizon(heartbeatPeriod)` and builds its status cache with
  `earlyEvictionAge = Some(Host.txHorizon(heartbeatPeriod))`.
- Eviction warnings come from a Scaffeine `removalListener` that checks `RemovalCause.SIZE` against an insertion
  time stored in `Entry`. `earlyEvictions` and `cleanUpForTest` are `private[aspen]`.

- [ ] **Step 4: Run tests**

Run: `sbt 'testOnly *FrontendSuite *TransactionStatusCacheSuite *TransactionDriverSuite *StoreSuite'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src && git commit -m "tx: unknown prepares older than the status cache horizon vote TransactionTooOld"
```

---

### Task 10: End-to-end test, docs, full run

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/TracelessCommitsIntegrationSuite.scala`, using `TestNetwork`
  through `IntegrationTestSuite`.
- Modify:
  - `CLAUDE.md`:
    - Transaction Design: a bullet for the TracelessCommits tree and local apply; a bullet for the horizon; amend the
      proof-of-commit bullet;
    - the AllocationGroups "Metadata layout" line, which should name the third tree;
    - the "A store that misses `errorTimeout`" bullet, which should describe reconciliation.
  - `Todo.txt`: replace the "Recovered commit votes of forgotten transactions" paragraph with a closed note pointing
    at the spec.

- [ ] **Step 1: Write the failing integration tests**

```scala
test("two stores recovering commit votes for a committed TKVL delete both apply it"):
  // width 5 / write 4 / read 3 pool. Commit a TKVL delete of k with stores s2 and s3 holding their commit votes
  // (drop their TxResolved/TxFinalized via a TestNetwork message filter, then crash them). Finalize, let the
  // status cache expire (short heartbeat in the test host), restart s2 and s3.
  eventually { assert(allStores.forall(s => !storeKeys(s, nodePtr).contains(k))) }
  eventually { assert(tracelessEntries(pool).isEmpty && errorEntries(pool).isEmpty) }
  assert(await(tree.get(k)).isEmpty)

test("a recovered commit vote for a committed data overwrite is proven and applied"):  // proof-of-commit end to end
  eventually { assert(storeRevision(s2, dataPtr) == ObjectRevision(txid)) }
```

`eventually` polls; there are no fixed sleeps.

- [ ] **Step 2: Run them**

Run: `sbt 'testOnly *TracelessCommitsIntegrationSuite'`
Expected: PASS, since Tasks 1-9 implement it. If a test fails, debug with superpowers:systematic-debugging before
changing code.

- [ ] **Step 3: Update `CLAUDE.md` and `Todo.txt`** as listed under Files.

- [ ] **Step 4: Full run**

Run: `timeout 70 sbt test`, then the same once more to check for flakes.
Expected: 0 failures, no new "Hung Test" prints. Report any failure by name.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "traceless commits: end-to-end tests and docs"
```
