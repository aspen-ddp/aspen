# Host Object Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the host-wide `synchronized` object cache with per-store LRUs under one host byte budget, keep
committed states in memory until the backend has them, and keep a transaction's CRL state until its backend commits
succeed.

**Architecture:** `HostObjectCache` becomes a lock-free coordinator (an `AtomicLong` host total plus a registry of
per-store views). Each `StoreObjectCache` is owned by its store's executor and holds an access-ordered LRU of evictable
states plus the pinned states the Frontend holds. An over-budget store evicts from itself, or posts one shed request to
the store whose oldest evictable entry is oldest. `Frontend` pins states for transactions and in-flight commits, and
tracks per-transaction commit outcomes to defer CRL deletion.

**Tech Stack:** Scala 3, ScalaTest (`AnyFunSuite` + `Matchers`), sbt.

**Spec:** `docs/superpowers/specs/2026-11-06-host-object-cache-design.md`

## Global Constraints

- No lock shared between store threads, or between a store thread and the control thread, on the cache path. Cross-store
  interaction is a task posted to the victim's executor, plus volatile/atomic published values only.
- No store touches another store's entries or ObjectStates.
- Soft limit: `HostConfig.objectCacheBytes` (unchanged). Brief overshoot is allowed.
- `HostObjectCache.estimateSize`, `EntryOverhead` (256) and `KeyValueEntryOverhead` (128) are unchanged.
- Lockstep (`LockstepThreading`) ordering must not change; single-store behaviour stays as today except the commit
  window and failed-commit rules below.
- No flaky tests. Tests are deterministic (inject the clock; no sleeps).
- Scala 3 quiet syntax; indented if/then/else (CLAUDE.md).
- Precondition: the working tree has uncommitted work from the previous change (`Host.scala`, `DeviceControl.scala`,
  `StoreLoader`-adjacent files, `CLAUDE.md`, `Todo.txt`, ...). It must be committed or set aside by the user before
  Task 1. Every `git add` below names explicit files; never `git add -A` or a directory that holds unrelated edits.
- Commit on main (memory: commit directly on main). Each commit message ends with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A state pinned by a transaction that aborts must go back to the LRU at the most-recently-used end, never dropped
   (user: a retry follows at once). Test in Task 3: "an aborted transaction leaves its objects cached".
2. An allocating transaction that aborts must not leave its `Allocating` state cached as if the object existed.
   Test in Task 3: "an aborted allocation is not cached".
3. Two successive transactions committing the same object while the first commit is still in the backend: reads
   serve the newest state, and the first completion does not unpin the second's. Test in Task 3.
4. A store closing with shed requests queued for it, then reloading: the old view's queued shed does nothing and the
   new view starts empty with the host total correct. Test in Task 2: "a shed queued for a cleared view does nothing".
5. A finalized transaction whose commit is still outstanding must not be driven again by `Store.heartbeat`: it is not
   in `Frontend.transactions`. Test in Task 4.

---

### Task 1: Per-store LRU with pinning and a host total

Rewrites the cache as per-store views with local eviction only (cross-store eviction is Task 2). Changes the
`ObjectCache` trait.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/cache/ObjectCache.scala`
- Rewrite: `src/main/scala/org/aspen_ddp/aspen/server/store/cache/HostObjectCache.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/server/store/cache/StoreObjectCache.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala:165-195` (only to compile: `insert` now
  returns `Unit`)
- Rewrite: `src/test/scala/org/aspen_ddp/aspen/server/store/HostObjectCacheSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/store/LRUCacheIdentitySuite.scala` (still passes; no API it
  uses changes beyond `insert`'s result)

**Interfaces:**
- Produces:
  - `trait ObjectCache`: `clear(): Unit`, `get(objectId: ObjectId): Option[ObjectState]`,
    `insert(state: ObjectState): Unit`, `remove(objectId: ObjectId): Unit`, `pin(state: ObjectState): Unit`,
    `unpin(state: ObjectState): Unit`. `ObjectCache.NoCache` implements all as no-ops (`get` = `None`).
  - `class HostObjectCache(val maxBytes: Long, clock: () => Long = () => System.nanoTime())` with
    `def sizeInBytes: Long` (host total), and
    `def forStore(storeId: StoreId, executor: ExecutionContext = ExecutionContext.parasitic): StoreObjectCache`.
    Companion keeps `EntryOverhead`, `KeyValueEntryOverhead`, `estimateSize` unchanged.
  - `final class StoreObjectCache extends ObjectCache` (constructor `private[cache]`) with `val storeId: StoreId`,
    `def sizeInBytes: Long` (this store's resident bytes: evictable + pinned), `def entryCount: Int` (evictable
    entries), `def pinnedCount: Int`.

**Semantics (all `StoreObjectCache` methods run on its store's executor):**
- Evictable entries: access-ordered `java.util.LinkedHashMap[ObjectId, Entry]`, `Entry(state, size, lastAccess)`.
  `get` returns an evictable entry's state and refreshes `lastAccess` from `clock()`; it also returns a pinned state
  that is marked cached (below). `insert` adds or re-measures (if the state is pinned, re-measures the pinned record
  and marks it cached instead of adding an LRU entry).
- Pinned states: a `java.util.IdentityHashMap[ObjectState, Pinned(size, cached: Boolean)]`. `pin(s)`: if the LRU
  entry for `s.objectId` is `s` itself, move it out of the LRU (bytes stay counted, `cached = true`); otherwise start
  counting it (`cached = false`). Pinning an already-pinned state is a no-op (the Frontend pins once per state, at its
  first reference). `unpin(s)`: if `cached && !s.reloadRequired` and the LRU has no entry for its id, put it back at
  the most-recently-used end with `lastAccess = clock()`; otherwise stop counting it.
- `remove(id)` drops only the LRU entry for `id`.
- Every byte change goes through one private `adjust(delta: Long)` that updates the view's own total and
  `HostObjectCache.total` (`AtomicLong.addAndGet`). After `insert`/`unpin`, if the host total exceeds `maxBytes`,
  the view evicts its own LRU head until the host total is within `maxBytes` or the LRU is empty; never the entry
  just inserted.
- `clear()` uncounts everything (LRU and pinned), unregisters the view, and marks it cleared: every later call on it
  is a no-op and `get` returns `None`. (It is only called when the store closes, `StoreLoader.scala:383` and `:445`.)
- `forStore` registers the new view in a `ConcurrentHashMap[StoreId, StoreObjectCache]`. If an uncleared view for that
  store is still registered, log a warning, subtract that view's last published byte count from the host total, and
  mark it cleared through a `@volatile` flag (the old store is no longer running).
- Scaladoc on both classes states the ownership rules above and replaces the stale "Host serializes every store
  operation" argument.

- [ ] **Step 1: Write the failing tests** in `HostObjectCacheSuite` (replace the file; keep `mkobj`/sizing helpers).
  Use `Overhead = HostObjectCache.EntryOverhead` and a manual clock `var now = 0L; val clock = () => { now += 1; now }`.

```scala
test("evicts least recently used first")                       // cap 3*Overhead; insert a,b,c; get(a); insert d
  // => get(b) shouldBe None; a, c, d present; hc.sizeInBytes shouldBe 3 * Overhead
test("evicts as many as needed to fit a large one")             // cap 4*Overhead; 4 small, then one 3*Overhead-data obj
test("an object larger than the capacity is kept until the next insert")
test("re-inserting re-measures")                                 // data grows; sizeInBytes reflects new size
test("key-value objects count their decoded content")            // keep existing estimateSize assertion
test("pinned states are counted but never evicted")              // cap 2*Overhead; pin a (cached); insert b, c
  // => a still returned by get; c present; b evicted; sizeInBytes shouldBe 2 * Overhead
test("unpinning returns a cached state at the most recently used end")
  // cap 3; insert a,b,c; pin a; unpin a; insert d => b evicted, a kept
test("unpinning an uncached state stops counting it")            // pin x never inserted; sizeInBytes == Overhead; unpin => 0
test("unpinning a discarded state stops counting it")            // insert a; pin a; a.reloadRequired = true; unpin a
  // => get(a.objectId) shouldBe None; sizeInBytes shouldBe 0
test("stores share one host total but not objects")              // two views, same ObjectId; each get returns its own
test("clearing a view uncounts it and ends it")                  // after clear: host total drops by view's bytes; get None; insert no-op
test("a new view replacing an uncleared one uncounts the old one")
```

- [ ] **Step 2: Run them to see them fail**

Run: `sbt 'testOnly *HostObjectCacheSuite'`
Expected: compilation errors (`pin`/`unpin` undefined, `insert` result type).

- [ ] **Step 3: Implement** `ObjectCache` changes, `HostObjectCache`, `StoreObjectCache` as specified. In
  `Frontend.scala` nothing changes yet except that the ignored `List` result of `insert` is gone.

- [ ] **Step 4: Run the cache suites and the Frontend suite**

Run: `sbt 'testOnly *HostObjectCacheSuite *LRUCacheIdentitySuite *FrontendSuite *StoreSuite'`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/store/cache/*.scala src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala src/test/scala/org/aspen_ddp/aspen/server/store/HostObjectCacheSuite.scala src/test/scala/org/aspen_ddp/aspen/server/store/LRUCacheIdentitySuite.scala
git commit -m "cache: per-store LRU views with pinning under one host byte total"
```

---

### Task 2: Cross-store eviction and host wiring

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/cache/HostObjectCache.scala`,
  `StoreObjectCache.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala:139`, `server/host/DeviceControl.scala:51`,
  `server/host/StoreLoader.scala:307`, `cmdline/Main.scala:1715`
- Modify tests that build the factory: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala:104`,
  `server/host/StoreRouterSuite.scala:140` (`(_, _) => ObjectCache.NoCache`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/HostObjectCacheSuite.scala`

**Interfaces:**
- Consumes: Task 1's classes.
- Produces: the factory type `objectCacheFactory: (StoreId, ExecutionContext) => ObjectCache` on `Host` and
  `DeviceControl`; `StoreLoader` passes the store's `executor` (a `StoreExecutor`, which is an `ExecutionContext`).
  `Main` and `TestNetwork` pass `new HostObjectCache(bytes).forStore` (eta-expands to the two-argument function).

**Semantics:**
- Each view publishes `@volatile oldestAccess: Long`: the LRU head's `lastAccess`, or `Long.MaxValue` when its LRU is
  empty. It is republished after every mutation.
- Replace Task 1's local-only eviction with `HostObjectCache.rebalance(from: StoreObjectCache)`, called by a view
  after `insert`/`unpin` when the host total exceeds `maxBytes`:
  - The victim is the registered view with the smallest `oldestAccess`; `from` wins ties. No view with
    `oldestAccess < Long.MaxValue` means nothing to do.
  - If the victim is `from`, it evicts locally (as in Task 1).
  - Otherwise, if the victim's `shedPending: AtomicBoolean` flips from false to true, post
    `victim.shed()` to `victim.executor`. If a shed is already pending, do nothing.
- `shed()` (on the victim's executor) clears `shedPending`; if the view is cleared, returns. Otherwise it evicts its
  own LRU head while the host total exceeds `maxBytes` and its LRU is non-empty. If the total is still over afterwards,
  it calls `rebalance(this)`, which picks the next victim (its own `oldestAccess` is now `Long.MaxValue`).
- Lockstep note: `LockstepThreading`'s store executors run tasks inline, so a shed runs inside the inserting store's
  call, deterministically. Under `StoreThreadPool` it runs on the victim's mailbox.

- [ ] **Step 1: Write the failing tests** in `HostObjectCacheSuite`. For deterministic cross-store tests, give each view
  a recording executor: `class Queue extends ExecutionContext { var tasks = Vector[Runnable](); def execute(r) = tasks :+= r; def runAll() = ...; def reportFailure(t) = throw t }`.

```scala
test("a hot store's inserts evict a cold store's oldest entries")
  // cap 4*Overhead; cold view (Queue executor) inserts c1,c2 at clock 1,2; hot view (parasitic) inserts h1..h4
  // => coldQueue.tasks.size shouldBe 1 (one shed posted); runAll; cold.get(c1) shouldBe None;
  //    hc.sizeInBytes shouldBe 4 * Overhead; all h1..h4 present
test("a store evicts its own entries when they are the oldest")   // no task posted to the other view's queue
test("only one shed is pending per victim")                       // several hot inserts before runAll => 1 task
test("a shed that arrives after the pressure passed evicts nothing")  // free bytes (hot.clear()) before runAll
test("a shed queued for a cleared view does nothing")             // cold.clear() then runAll; total unchanged
test("a victim with too few entries passes the rest of the pressure on")  // three views; second-oldest also shed
test("pinned states never make a store a victim")                 // cold view's only state pinned => no shed posted
test("stores on their own threads keep the host total consistent")
  // two views on two single-thread executors (Executors.newSingleThreadExecutor wrapped in
  // ExecutionContext.fromExecutor), each inserting 10_000 distinct objects from its own thread; await both and drain
  // with a final no-op task on each executor; then hc.sizeInBytes shouldBe (a.sizeInBytes + b.sizeInBytes) and
  // hc.sizeInBytes should be <= cap + Overhead * 2. Deterministic assertions only (no timing).
```

- [ ] **Step 2: Run to see them fail**

Run: `sbt 'testOnly *HostObjectCacheSuite'`
Expected: FAIL (no shed is posted; the hot store evicts its own entries).

- [ ] **Step 3: Implement** `oldestAccess`, `shedPending`, `rebalance`, `shed`, and change the factory type and its
  callers listed above.

- [ ] **Step 4: Run cache and host suites**

Run: `sbt 'testOnly *HostObjectCacheSuite *StoreRouterSuite *ThreadedHostSuite *AdaptiveThreadedHostSuite *HostCrlSaveLoadSuite'`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/store/cache/*.scala src/main/scala/org/aspen_ddp/aspen/server/Host.scala src/main/scala/org/aspen_ddp/aspen/server/host/DeviceControl.scala src/main/scala/org/aspen_ddp/aspen/server/host/StoreLoader.scala src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala src/test/scala/org/aspen_ddp/aspen/server/host/StoreRouterSuite.scala src/test/scala/org/aspen_ddp/aspen/server/store/HostObjectCacheSuite.scala
git commit -m "cache: over-budget stores evict the host's oldest entries, posting to other stores' executors"
```

---

### Task 3: Frontend pins states for transactions and in-flight commits

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/ObjectState.scala:12-16`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (`commit`, `retain`, `release`,
  `discard`, `backendOperationComplete`, `readObjectForNetwork`, `readObjectForTransaction`,
  `readObjectForRepair`, `readObjectForOpportunisticRebuild`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`

**Interfaces:**
- Consumes: `ObjectCache.pin`/`unpin` (Task 1).
- Produces (used by Task 4): `Frontend.commit(os: ObjectState, cs: CommitState, txid: TransactionId,
  forTransaction: Boolean = false): Unit`. `Tx.doCommit` passes `forTransaction = true` in Task 4; the rebuild paths
  (`Tx.completeLoad` pre-transaction rebuilds, `Frontend.opportunisticRebuild`) keep the default.

**Semantics:**
- Rename `ObjectState.transactionReferences` to `references`: "transactions and in-flight backend commits holding this
  state. Touched only on the owning store's executor." `retain(os, shared)` calls `objectCache.pin(os)` on 0 -> 1;
  `release(os)` calls `objectCache.unpin(os)` on 1 -> 0 (after the existing `referenced` removal).
- `commit(...)`: as today (discard if refcount is 0, else `objectCache.insert(os)`), then `retain(os, shared = true)`,
  record the state under `(cs.objectId, txid)` in `inFlight: mutable.HashMap[(ObjectId, TransactionId), List[InFlight]]`
  where `InFlight(os: ObjectState, start: Long, deletes: Boolean)` (this replaces `commitStarts`). A deletion also
  increments `deleting: mutable.HashMap[ObjectId, Int]`.
- `backendOperationComplete(c: Commit)`: pop one `InFlight` for the key; record timing as today.
  - Success: `release(os)`; for a deletion decrement `deleting` (remove at 0).
  - Failure: keep it pinned and keep its `deleting` count: never released while the store is loaded. Log at error with
    the object and transaction. Remove the `discard(c.objectId)` and replace its comment with the reason (spec: the
    store may have voted commit at write-threshold; `Backlog.txt` covers proper handling).
  - Then call `tx.commitComplete` as today.
- Reads during a deletion: wherever a read would otherwise call `backend.read` (no servable/transaction state in
  memory), if `deleting.contains(objectId)` answer as the backend will after the delete instead:
  network read -> `ReadResponse(..., Left(ReadError.ObjectNotFound))`; transaction read ->
  `objectLoadFailed(objectId, ReadError.ObjectNotFound)`; repair read -> the existing read-error repair path
  (zeroed state); opportunistic rebuild -> ignored.
- Update the `referenced` scaladoc: it now also holds states of in-flight commits.

- [ ] **Step 1: Write the failing tests** in `FrontendSuite`. Add a test backend that holds commit completions:
  `class DeferredCommitBackend extends MapBackend(storeId)` that applies the write to its map only on
  `deliverCommits(result: Either[Unit, CommitError.Value] = Left(()))`, and counts `reads: Int` in `read`.
  Use a small `HostObjectCache` (e.g. `2 * EntryOverhead`) so eviction pressure is real.

```scala
test("a committed state is served from memory until its backend commit completes")
  // commit tx1 (overwrite oid1 with byte 1) with completion held; insert 3 other objects through reads to push
  // oid1 out of the LRU; network read of oid1 => data == Array(1) and backend.reads unchanged;
  // deliverCommits(); evict pressure again; next read goes to the backend and returns Array(1)
test("a read during a deletion's commit gets ObjectNotFound without reaching the backend")
  // refcount -> 0 commit held; network read => Left(ObjectNotFound); backend.reads unchanged
test("a failed backend commit keeps serving the committed state")   // replaces "a backend commit error evicts the state the commit cached"
  // for each CommitError: after failure, read => new data; eviction pressure => still served; backend.reads unchanged
test("two in-flight commits of one object serve the newest state")  // tx1 then tx2 (requires tx1's revision); hold both;
  // deliver one completion => read returns tx2's data; deliver second => still tx2's data
test("an aborted transaction leaves its objects cached")             // load oid1 via tx1 prepare; resolve aborted;
  // eviction pressure smaller than one entry evicts an older object, not oid1 (cache.get(oid1) nonEmpty)
test("an aborted allocation is not cached")                          // allocating tx resolves aborted => cache.get(newId) None
test("a pinned state counts toward the store's bytes")               // cache.sizeInBytes includes the in-flight state after eviction
```

  Update `"a failed backend commit does not drop another transaction's shared lock"` (`FrontendSuite.scala:888`): its
  last step expected the discarded state to be reloaded from the backend; it now expects the failed commit's state
  (rev txid2) to stay in force, so an overwrite requiring `rev0` votes abort and one requiring txid2's revision votes
  commit.

- [ ] **Step 2: Run to see them fail**

Run: `sbt 'testOnly *FrontendSuite'`
Expected: the new tests fail (backend reads during the window; discard after failure).

- [ ] **Step 3: Implement** the semantics above.

- [ ] **Step 4: Run**

Run: `sbt 'testOnly *FrontendSuite *StoreSuite *HostObjectCacheSuite'`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/store/ObjectState.scala src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala
git commit -m "store: keep committed states in memory until the backend has them; failed commits keep serving"
```

---

### Task 4: Keep CRL state until the store's backend commits succeed

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala` (`doCommit` :415-421,
  `allObjectCommitsComplete` :429-435, `receiveFinalized` :582-591, `commitComplete` :603-612)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`

**Interfaces:**
- Consumes: `Frontend.commit(..., forTransaction = true)` (Task 3).
- Produces: `Frontend.transactionFinalized(txid: TransactionId): Unit`.

**Semantics:**
- Frontend keeps `txCommits: mutable.HashMap[TransactionId, TxCommits]`,
  `final class TxCommits(var outstanding: Int, var failed: Boolean, var finalized: Boolean)`, for commits made with
  `forTransaction = true` only. Each such commit increments `outstanding`; each completion decrements it and sets
  `failed` on an error. When `outstanding == 0` and not `failed`: if `finalized`, call
  `crl.deleteTransaction(storeId, txid)`; either way remove the record. A `failed` record is never removed while the
  store is loaded.
- `transactionFinalized(txid)`: no record -> `crl.deleteTransaction` now (today's behaviour, also for aborts and
  transactions that committed nothing here); record present -> set `finalized` (deletion deferred).
- `Tx.receiveFinalized`: call `frontend.transactionFinalized(transactionId)` instead of `crl.deleteTransaction`. All
  else unchanged (it is still removed from `Frontend.transactions`, so `Store.heartbeat` never drives it).
- `Tx`: a new `backendCommitFailed` flag set in `commitComplete` on `Right(err)`. In `allObjectCommitsComplete`,
  only when it is false: `saveObjectUpdates = false` and `crl.dropTransactionObjectData(...)`. `TxCommitted` is sent
  as today, with the errors.
- The abort path in `resolvedAndAllObjectsLoaded` keeps `crl.deleteTransaction`.
- Scaladoc on `transactionFinalized` and `TxCommits` states the rule: the store deletes a committed transaction's CRL
  state only once every backend commit it issued for it succeeded; a reloaded store recovers the rest and its
  transaction driver concludes them.

- [ ] **Step 1: Write the failing tests** in `FrontendSuite` (use `DeferredCommitBackend` from Task 3 and `TestCrl`;
  extend `TestCrl` to record `deleted: List[TransactionId]` and `dropped: List[TransactionId]`).

```scala
test("finalization before the backend commit completes defers the CRL deletion")
  // prepare+resolve committed tx1 with completion held; TxFinalized => crl.deleted shouldBe Nil,
  // frontend.transactions.contains(txid1) shouldBe false; deliverCommits() => crl.deleted shouldBe List(txid1)
test("a finalized transaction with an outstanding commit is not driven again")
  // Store-level: build a Store (StoreSuite's helper) with DeferredCommitBackend; finalize with the commit held;
  // store.heartbeat() after the heartbeat timeout => no TransactionDriver created (driver factory records creations)
test("a failed backend commit keeps the transaction's CRL entry and its object data")
  // completion fails; then TxFinalized => crl.deleted shouldBe Nil and crl.dropped shouldBe Nil
test("a failed commit's CRL entry is in the store's saved state")
  // with SimpleCRL (see SimpleCRLSuite for setup): failed commit + TxFinalized; crl.closeStore(storeId) result
  // contains txid1 with its objectUpdates
test("a store loaded with such an entry recreates the transaction")
  // new Frontend over a CRL returning that entry => frontend.transactions.contains(txid1)
test("locks are released at finalization while a commit is outstanding")
  // tx1 commit held + finalized; tx2 requiring tx1's revision votes commit
```

  The existing `"finalization deletes the CRL entry ..."` tests (`FrontendSuite.scala:499`, `:515`) must still pass.

- [ ] **Step 2: Run to see them fail**

Run: `sbt 'testOnly *FrontendSuite *StoreSuite'`
Expected: the deferral and failure tests fail (CRL deleted at finalization; object data dropped).

- [ ] **Step 3: Implement** the semantics above.

- [ ] **Step 4: Run**

Run: `sbt 'testOnly *FrontendSuite *StoreSuite *SimpleCRLSuite *HostCrlSaveLoadSuite'`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala src/test/scala/org/aspen_ddp/aspen/server/store/StoreSuite.scala
git commit -m "store: keep a transaction's CRL state until its backend commits succeed"
```

---

### Task 5: Documentation and full verification

**Files:**
- Modify: `CLAUDE.md` (Host threading model: add one line on the cache, e.g. "Each store's object cache is its own
  LRU, owned by its executor, under one host byte budget (`HostObjectCache`): an over-budget store evicts its own
  oldest entries or posts a shed to the store holding the host's oldest. Frontend pins states held by transactions
  and in-flight commits; a failed backend commit stays pinned, and a committed transaction's CRL state is kept until
  its backend commits succeed")
- Modify: `Todo.txt` (remove the first item, lines 4-5)

- [ ] **Step 1: Update the docs** as above; check no comment still says the cache is `synchronized` or that the Host
  serializes all stores (`grep -rn "serializes every store\|synchronized" src/main/scala/org/aspen_ddp/aspen/server/store`).

- [ ] **Step 2: Full test run**

Run: `timeout 1500 sbt test 2>&1 | tee <scratchpad>/full.log; grep -E "\*\*\* FAIL|Tests:|Hung" <scratchpad>/full.log`
Expected: `Tests: succeeded N, failed 0`. Any failure is investigated (superpowers:systematic-debugging), not retried.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md Todo.txt
git commit -m "docs: per-store object caches under one host budget"
```
