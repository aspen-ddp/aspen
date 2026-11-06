# Host object cache: per-store LRUs under one host budget

Date: 2026-11-06. Follows the host refactor (`2026-11-05-host-refactor-design.md`), which listed this as follow-up
work and recorded it as the top item of `Todo.txt`.

## Intent

`HostObjectCache` (`server/store/cache/HostObjectCache.scala`) is a single `synchronized` LinkedHashMap shared by
every store on a host. Now that stores run on their own threads (`StoreThreadPool`), every cache `get`, `insert` and
`remove` from every store takes one host-wide lock, and an insert that is over capacity walks the LRU list while
holding it. The cache is not a measured bottleneck yet. This change is to get ahead of it, and to make the cache obey
the host threading rules: no lock is taken by two store threads, and each piece of state has a single owner thread.

The cache exists to make reads fast. It must:

1. **Count every in-memory ObjectState toward one soft limit for the whole host**, including states that are held only
   because a transaction references them.
2. **Evict approximately least-recently-used states first, across the whole host.** Stores do not get equal load, and
   the share each one uses shifts at runtime, so an insert by store A may need to evict entries owned by store C on
   another thread.

Keeping a transaction's ObjectStates in memory while it is active is not the cache's job (see the verification
below). The Frontend already does that, and this change extends it to cover the backend commit window.

Verifying that window exposed a durability bug, folded into this change: a store drops a transaction's CRL state
before its backend commits have succeeded (see "Store-side transaction completion").

## Verification of the current code (2026-11-06)

The question was whether the threading change made the shared cache unsafe. It did not:

- A transaction's states do not depend on the cache to stay in memory. Each `Tx` holds its objects in its `objects`
  map, and `Frontend.retain` (`Frontend.scala:177`) also records shared states in `Frontend.referenced`
  (`Frontend.scala:80`), both owned by the store's thread. Lookups for transactions and reads consult `referenced`
  before the cache (`stateForTransaction`, `servableState`), and a fresh load never replaces a referenced state
  (`backendReadComplete`).
- `HostObjectCache.insert` reads `ObjectState.transactionReferences`, a plain `var` written by the owning store's
  thread, from whichever store thread is inserting. That is a data race, but a harmless one: an `Int` read is not
  torn, and a wrong value either evicts a referenced state (harmless: it stays in `referenced` and the `Tx`, but its
  bytes stop counting toward the limit) or skips an unreferenced one (a brief overshoot of the limit).
- The scaladoc's safety argument (`HostObjectCache.scala:17-19`, "the Host serializes every store operation on its
  event loop") is stale.
- Gaps against the requirements: referenced states that are not in the cache are not counted (evicted ones,
  discarded ones still holding locks, and allocated objects before their commit inserts them).
- Commit window: `Tx.releaseObjects` runs right after `doCommit`, before the backend reports the write complete. If
  the state is then evicted or discarded, a read loads it from the backend, which may not have it yet. RocksDB
  happens to serve its own buffered writes (`BufferedConsistentRocksDB`), but the system must not rely on that
  (below).

## Goals

- One soft byte limit per host (`HostConfig.objectCacheBytes`, unchanged) covering every ObjectState a store holds
  in memory: cached states and states held by transactions or in-flight commits. Each state counts once.
- Approximately-LRU eviction across all stores on the host. Cross-store eviction is required.
- No lock shared between store threads, or between a store thread and the control thread, on the cache path. Cache
  state is owned by its store's thread, actor-style.
- Frontend keeps an object's state in memory from the moment a commit is handed to the backend until the backend
  reports that commit complete, and serves reads and transactions from it during that window. Backends are never
  required to serve reads of a write they have not finished.
- Correct the stale documentation: `HostObjectCache`'s scaladoc (or its replacement's), `ObjectState.transactionReferences`'
  doc comment ("not allowed to exit the cache"), the `CLAUDE.md` threading section if it mentions the cache, and
  `Todo.txt` (remove the item when done).

## Non-goals

- Exact global LRU. Approximate is fine.
- A hard limit. Brief overshoot is acceptable, including while a cross-store eviction request is in flight and while
  too few unreferenced states exist to get back under the limit (as today: the cache grows rather than drop state a
  transaction needs).
- Changing the cache size configuration, the size estimate (`HostObjectCache.estimateSize` stays as is), or what is
  cached.
- Changing the transaction protocol, Paxos, the CRL's format, or backends. (When the store deletes CRL state does
  change: see "Store-side transaction completion".)
- Read-only mode for stores with failing commits, and bounding CRL growth from them (`Backlog.txt`).
- Backward compatibility of persisted data (none is involved).
- An adaptive or tunable eviction policy. One fixed, simple policy.

## Anti-goals (a failure even if it works)

- Any flaky test. Correctness, then reliability, then speed (`CLAUDE.local.md`).
- A lock taken by two store threads, or by a store thread and the control thread, on the cache path. Cross-thread
  interaction is by posting a task to the owning store, plus read-only published values (volatile fields or atomics
  whose single writer is the owning store; an atomic shared host total is acceptable).
- Any store touching another store's cache entries or ObjectStates directly.
- A read served from the backend while the store holds a newer state in memory for an outstanding commit.
- Changed message ordering under `LockstepThreading`. The integration suites are sensitive to ordering; single-store
  behaviour should be identical to today's, and any posted eviction request must go through the existing posting
  paths so lockstep orders it deterministically.
- A flood of posted eviction requests under sustained pressure (e.g. one per insert to the same victim).

## Decisions (made with the user)

- **Option 2: one LRU per store, owned by its thread, balanced by messages.** Chosen over a lock-striped shared
  cache, which would keep store threads sharing locks, contrary to the threading model.
  - Each store has its own LRU of its cached ObjectStates, touched only on its executor, with no locks.
  - Each store accounts its own resident bytes, cached plus referenced, and publishes that total and the age of its
    least-recently-used evictable entry, for other threads to read.
  - When the host total exceeds the limit, the inserting store picks the store whose oldest evictable entry is
    oldest, itself included. If that is itself, it evicts locally and synchronously. Otherwise it posts a request to
    the victim's executor (through `StoreRouter`) asking it to free bytes; the victim evicts from its own LRU.
- **Approximate LRU is fine.**
- **Every read and every transaction refreshes recency.** A network read, a repair read, and a transaction taking an
  object all mark its state most-recently-used, including when the state is found in `referenced` or an outstanding
  commit rather than through the cache lookup. A referenced state that becomes evictable again does so at the
  most-recently-used end.
- **Aborted transactions never evict.** A transaction that aborts (or otherwise ends without committing) leaves the
  states it used cached and recently used: a re-read and retry by the client very probably follows at once.
- **Commit window is the Frontend's job.** The Frontend retains the state from `Frontend.commit` until the backend's
  commit completion (`Frontend.backendOperationComplete`, `case c: Commit`), and skips backend reads for that object
  during the window. A retained-for-commit state counts toward the limit like any referenced state.

## Store-side transaction completion (folded in by the user)

**Rule:** from a store's perspective, a committed transaction is not complete until every backend commit it issued
for that transaction has succeeded. Until then the store keeps the transaction's CRL state, including its object
data, and the committed ObjectStates stay pinned in memory. A crash with a commit in flight must not lose the CRL
state: on the next load the store recovers the transaction from it, and the existing recovery path (a `Tx` idle for
`txHeartbeatTimeout` gets a `TransactionDriver`) drives it to a conclusion. If that concludes commit, the update is
applied; if it concludes abort, discarding the state is safe, since the store is then known to be behind and in need
of repair. No extra CRL write is added (user decision). Making that driver reliably reach the commit decision is the
"Potential hole in transaction recovery" item in `Todo.txt`, out of scope here.

**What the code does today (verified 2026-11-06), both bugs under this rule:**

1. *Failed backend commit.* `Tx.commitComplete` (`Tx.scala:603`) counts a failure like a success. When the last
   completion arrives, `allObjectCommitsComplete` (`Tx.scala:429-432`) calls `crl.dropTransactionObjectData`, and
   `Frontend.backendOperationComplete` discards the state, so reads return the pre-commit backend state and a crash
   leaves recovery nothing to work from.
2. *TxFinalized during an in-flight commit.* The finalizer waits at most `MissedUpdateFinalizationAction.errorTimeout`
   (2 s) for each store's `TxCommitted`, then finalizes. `Tx.receiveFinalized` (`Tx.scala:582-590`) calls
   `crl.deleteTransaction` unconditionally and `Frontend` forgets the `Tx` (`Frontend.scala:237`). A crash before the
   slow store's write lands loses its update. (`receiveFinalized` can also start a held commit via
   `releaseHeldCommit` and delete the CRL entry in the same step.)

The present safety net is the ErrorTree: the finalizer usually marks such stores (`markMissedUpdates`) and repair
rewrites them later. It has a hole: when missed-update tracking is disabled (`limitTracking`, ErrorTree writes about
the bootstrap pool) nothing is marked and the store's copy is lost outright. Even when marked, the store serves
pre-commit state until repair.

**Required behaviour:**

- `crl.deleteTransaction` and `crl.dropTransactionObjectData` for a committed transaction run only once all of that
  store's backend commits for it have succeeded. TxFinalized arriving earlier records the finalization (status
  cache, `ofinalized`, releasing locks/holds as today) but defers the CRL deletion until the last successful commit.
  For an aborted transaction, or one that committed nothing on this store, deletion is unchanged.
- The Frontend must keep enough of the transaction (or a per-commit record) after TxFinalized to perform that deferred
  deletion when the completion arrives; today the `Tx` is forgotten at once.
- A failed backend commit leaves the transaction incomplete for the life of the store: its CRL entry and object data
  stay, and the state stays pinned and served. It is not retried in-process; the next load of the store recovers the
  transaction from the CRL and the transaction driver concludes it, as above. The failure is still reported in
  `TxCommitted`, so the ErrorTree mark stays as an extra safety net.
- Store close/save (`crl.closeStore`, `CrashRecoveryLog.saveStoreState`) must include such incomplete transactions, so
  a planned shutdown or transfer carries them too.
- Lock/requirement behaviour after TxFinalized is unchanged: an incomplete transaction must not keep requirement locks
  or block other transactions; only its states stay pinned and its CRL state stays.

- The transaction is not kept in `Frontend.transactions` after TxFinalized (the heartbeat would otherwise drive an
  already-finalized transaction); the deferred deletion is tracked separately.

## Design constraints for the builder

- **Accounting.** A state is resident for a store if it is in that store's LRU or the Frontend holds it (in
  `referenced`, for a transaction that allocates it, or for an outstanding commit). It is counted once while resident
  and uncounted when it stops being resident. Sizes are re-measured when content changes (today `commit` re-inserts
  to re-measure). Referenced and committing states are never eviction candidates; eviction removes only states the
  store holds for no other reason.
- **Cross-store requests.** At most one outstanding eviction request per victim store at a time (or an equivalent
  bound). A request that arrives after the pressure has passed does nothing, or frees only what is still needed. A
  request for a store that has closed is dropped (`StoreRouter.post` already does this).
- **Store lifecycle.** A store's bytes leave the host total when its cache is cleared or the store closes, and it
  stops being a victim candidate. A reloaded store starts empty (as `forStore` does today). This includes the close
  paths in `StoreLoader` (`store.objectCache.clear()` at `StoreLoader.scala:383` and `:445`).
- **Commit window details.**
  - The retention is per outstanding backend commit, independent of whether the `Tx` still exists: the completion
    may arrive after the transaction is forgotten (`transactions.get(c.transactionId)` may be empty).
  - Several outstanding commits of one object (from successive transactions) must keep the newest state served.
  - **A failed backend commit no longer discards the state (user decision).** Today `backendOperationComplete`
    discards it (`Frontend.scala:283-287`); that discard is removed. The store keeps serving the committed in-memory
    state, and it stays pinned (never evicted) until the store closes, exactly like a state whose commit is still
    outstanding: from the store's view that transaction never completed (see "Store-side transaction completion"
    below). The failure is still reported to the `Tx` and in `TxCommitted` as today. Reason: the store may have voted
    to commit at write-threshold, so it must not lose that state or return the pre-commit state. Flipping the store
    read-only and bounding the CRL growth this causes is future work in `Backlog.txt`, not part of this change.
    Update the comment there and any test that asserts the discard.
  - A deletion (refcount 0, `Frontend.commit` calls `discard`) must not let a read reach the backend during the
    window either; such reads get the answer the store will give once the delete is applied.
- **`ObjectCache` interface.** The `StoreId => ObjectCache` factory seam (`Host`, `DeviceControl`, `Main.scala:1715`,
  `TestNetwork.objectCacheFactory`) may change as needed. `ObjectCache.NoCache` must keep working for tests that use
  it. The eviction list `insert` returns today is unused and may go.
- **Wiring cross-store requests.** The cache is created before the `StoreRouter` exists (`Main.scala:1715`); the
  builder chooses how the host-wide coordinator gets a way to post to a store.
- **Tests.** Deterministic unit tests for the per-store LRU and accounting; victim selection; cross-store eviction
  under lockstep (two stores, one hot, one cold: the hot store's inserts evict the cold store's entries); request
  deduplication; store close releasing its bytes; the commit window (no backend read is issued while a commit of the
  object is outstanding, including after eviction pressure, after a delete, and after the `Tx` is gone); and a
  threaded test (`ThreadedHostSuite`-style) exercising cross-store eviction on real store threads. Store-side
  completion: TxFinalized before the backend commit completes keeps the CRL entry until the completion arrives; a
  failed backend commit keeps the CRL entry (object data included) and the served, pinned state, and the entry is in
  the store's saved CRL state at close; a reloaded store recreates that transaction; and requirement locks are
  released at finalization as today. The existing
  `HostObjectCacheSuite`, `LRUCacheIdentitySuite` and `FrontendSuite` cache tests are updated or replaced.

## Left to the builder

- Data structures for the per-store LRU (an access-ordered LinkedHashMap is fine) and how referenced states are kept
  out of eviction (e.g. moved out of the evictable list while referenced, returned at the most-recently-used end on
  release).
- The age metric (e.g. `System.nanoTime` of last access) and how stores publish it.
- How the host total is kept (an `AtomicLong` / `LongAdder`, or the sum of per-store published totals).
- How much a victim frees per request (e.g. the overshoot at request time plus some slack) and the slack amount.
- Class and file names, and whether `HostObjectCache` is reshaped or replaced.
- Whether `ObjectState.transactionReferences` stays, or reference counting moves wholly into the Frontend.
