# Traceless commits: recovery, exact KV repair, and late-prepare replay

Date: 2026-11-08. Follows `2ad0f326` (proof-of-commit, `Tx.proveCommitted`), which closed the recovered-commit-vote
hole for changes that leave a trace, and the Todo.txt note on what it left open.

> **Superseded in part by `2026-11-09-replay-pins-design.md`.** The horizon section (`Host.txHorizon`,
> `TransactionTooOld`) was removed and replaced by the client pin rule. The TracelessCommits tree is now UnackedCommits
> (`UnackedCommitEntry`, `UnackedCommitLookup`) and holds an entry for every committed transaction's unacknowledged
> counted voters, not only traceless ones. Names below are the historical ones.

## Why

A store that votes commit, crashes before learning the outcome, and recovers after the transaction T was finalized
and forgotten starts a recovery driver. The other stores no longer know T, so they check T's requirements against
current state. Proof-of-commit makes a store that already shows T's update answer `TxResolved(committed)`, but some
changes leave nothing to show:

- a KeyValue key `Delete` in an update without a required revision (absence carries no revision);
- any `RefcountUpdate` (the serial is not unique to one transaction, and a refcount of 0 deletes the object).

For those the driver can decide abort, and the recovering store drops an update that counted toward the write
threshold. With two stores recovering at once in a width 5 / write 4 / read 3 pool, a committed TKVL delete is undone
for every reader: the three stores lacking the delete form a read quorum, repair copies the resurrected key to the
marked stores and clears their ErrorTree entries, and the key is then stuck, since every `Exists`/`KeyRevision` write
gets enough abort votes from the two stores that applied the delete.

Two related problems surfaced while designing the fix, and the user chose to fold both in:

1. **ErrorTree repair is not exact for KeyValue objects.** `Frontend.repair` installs the read state only when it is
   newer than the store's (`current.timestamp > os.metadata.timestamp`), and `StoreRepairer.repairUpdate` clears the
   entry once the read's timestamp reaches the missed one. A store that misses T and then applies an unrelated later
   KV update (another key, no required revision) has a timestamp past T's, so repair leaves T's change missing and
   deletes the entry. The same gate hides a missed refcount change behind a later data write on a DataObject. This is
   believed from reading the code. The first implementation step confirms it with a failing test.
2. **Late prepares replay forgotten transactions.** After the 7 s status cache expires, a prepare for a finalized
   transaction creates a fresh `Tx` (`Frontend.receivePrepare`, `case None`) that checks requirements against current
   state. A blind write, a `DoesNotExist` insert after a later delete (ABA), or an `Exists(k)` delete after k was
   re-inserted can then commit again and overwrite newer data.

## Goals

- A store holding a commit vote for a committed transaction it never applied applies it, however long it was offline,
  and never lets a recovery driver's abort drop it. It logs the local apply at info.
- Every change that leaves no trace is covered, refcount decrements included (user's decision).
- Missed-update repair of KeyValue objects is exact: after repair the store's copy matches the committed state, key by
  key. Refcount repair is exact on every object.
- Entries never pile up: the repair scan, which already walks each store's ErrorTree range, also walks and cleans the
  new tree (user's decision).
- A prepare arriving after its transaction's status-cache entry is gone is never applied as a new transaction.

## Non-goals

- Changing how concurrent per-key KV writes behave on the normal path. No new whole-object locks, no new revision
  requirements on deletes. (The `KeyRevision` switch for TKVL deletes was considered and dropped: the horizon rule
  makes it unnecessary.)
- Retaining transaction state on hosts or in CRLs beyond today's lifetimes. All new durable state lives in Aspen
  trees, which are replicated.
- Protecting the bootstrap pool's own ErrorTree writes. `limitTracking` turns tracking off only on a marker
  transaction (the `MissedUpdateMarker` batch, and with this design its TracelessCommits entries) that writes entries
  about a pool the transaction itself touches, which in practice means the bootstrap pool. Ordinary transactions on
  bootstrap-pool or metadata-pool objects are tracked and covered like any other. Losing one of those untracked
  marker deletes only resurrects an error entry, which costs one extra, idempotent repair pass.

## Anti-goals

- Any new work on the normal commit path when no store misses. The new tree is written only when a store misses a
  commit.
- A repair that overwrites or deletes a value the store applied after repair read its snapshot.
- A recovering store that commits a transaction which did not commit.
- Retaining per-store state that grows with message loss and is never cleaned.

## Background: facts the design relies on

- `TxCommitted` means durably applied: `Tx.allObjectCommitsComplete` sends it only after every backend commit
  completes. A store that sent it never comes back without the update.
- The CRL persists a transaction's status (`TransactionRecoveryState.status`). A store that learned the outcome before
  crashing recovers with it and needs no driver. Only a recovered `VoteCommit` with status `Unresolved` is at risk.
- `MissedUpdateFinalizationAction` runs as a finalization action, so it completes before `TxFinalized` and before any
  store forgets T. It marks every store that sent no `TxCommitted` within `errorTimeout` (2 s), plus stores reporting
  commit errors, through `MissedUpdateMarker`, which writes one batched transaction per pool.
- A pool's `ErrorTree` and `AllocationTree` are TKVLs whose roots are keys of the pool's state object
  (`StoragePoolState.ErrorTreeKey`, `AllocationTreeKey`, opened in `SimpleStoragePool`). Their nodes live in the
  pool's metadata group.
- Stores hold a transaction's requirement locks from its commit vote until its commit is applied in memory, and read
  responses report every transaction holding a write lock on the object (`ObjectState.lockedWriteTransactions`:
  object locks, per-key locks, absent-key locks, content and range locks). Step 1 of the implementation confirms that
  a resolved-committed transaction whose commit is held (awaiting data, awaiting its turn behind an in-flight commit)
  keeps its locks until applied.
- Backward compatibility with persisted data is not required, so formats can change without migration.

## Design

### 1. The TracelessCommits tree

A new per-pool TKVL, `TracelessCommits`, next to the ErrorTree and AllocationTree. Its root is a new key in the pool
state object (`StoragePoolState.TracelessCommitsTreeKey`), created wherever the ErrorTree root is created (bootstrap
and `createNewStoragePool` / `createNewMetadataPool`). Nodes go in the same metadata group as the ErrorTree.

- **Key:** the store's pool index byte followed by the 16-byte transaction id, so each store's entries form the
  half-open range `[poolIndex, poolIndex + 1)`, the same way ErrorTree keys do.
- **Value:** T's start timestamp and the pointers of the objects T changes on that store (with their store pointers,
  so a deleted object can still be located, as the ErrorTree does).

**Who gets an entry.** When `MissedUpdateFinalizationAction.markMissedUpdates` marks a store that sent no
`TxCommitted` (the `commitErrors.get(storeId) == None` case), it also writes a TracelessCommits entry for that store
if any object T changes on that store has a traceless change:

- a `RefcountUpdate`, for any refcount, allocations included;
- a `KeyValueUpdate` with `requiredRevision == None` whose new `deletesKeys` flag is set (section 2).

Stores that did send `TxCommitted`, even with object errors, get no entry: they applied T durably and cannot recover
a pending vote for it.

Only commit voters get one. When the deciding driver counted commit votes reaching the write threshold of every object,
it hands those voters to the finalizer (`FinalizationAction.commitVoters`), and only they get entries. A store that
never voted, such as one offline throughout, holds no vote to recover, and its ErrorTree marks repair it. A voter the
driver did not count is not one the commit relied on: if it drops T it is no different from a store that missed T.
When the driver learned the outcome without counting (another driver's `TxResolved`, a recovered accepted value), it
passes None and every store that sent no `TxCommitted` gets an entry, as before. (Amended after implementation: an
offline store otherwise grew the tree by one entry per traceless transaction for the length of its outage.)

The entries go into the same per-pool batch transaction `MissedUpdateMarker` already writes, so marking stays one
transaction per pool. Tracking disabled by `limitTracking` disables these entries too.

### 2. The `deletesKeys` flag

The finalizer sees only the transaction description, which today cannot tell an insert-only KV update from one that
deletes. `KeyValueUpdate` gains `deletesKeys: Boolean`, carried in the description and its codec.

- `TransactionBuilder` sets it automatically when the update's operations contain a `Delete`. No public API changes.
- The store rejects an update whose operations contain a `Delete` while the flag is false. The rejection is an object
  error, so the store votes abort.
- `DeleteMin`/`DeleteMax`/`DeleteLeft`/`DeleteRight` do not set it. Every current use carries a required revision,
  which leaves a trace. A future use without one is a traceless change this design does not cover, so the flag's
  scaladoc must say so.

### 3. Recovery: a recovered commit vote applies a committed transaction locally

A Tx recovered from the CRL with disposition `VoteCommit` and status `Unresolved` looks up its own entry
(its pool index followed by its transaction id) in its pool's TracelessCommits tree:

- **On recovery,** as soon as the Tx has loaded.
- **Again before accepting an abort.** When such a Tx receives an abort resolution (from its own recovery driver or
  another), it holds the abort until a lookup started after the abort arrived completes. The entry may have been
  written moments after the first lookup.

**Entry found:** T committed and this store missed it. The Tx resolves committed and applies its saved update through
the normal commit path. The update comes from the CRL record (`TransactionRecoveryState.objectUpdates`, which is kept
until the commit persists). Requirements are re-checked with `applying = true`, so nothing is applied twice. It logs at
info that it applied a committed transaction learned from the TracelessCommits tree, naming the transaction and
objects. No `TxFinalized` will arrive, so once the backend commits persist, the store deletes the transaction's CRL
state and forgets it as if finalized. The store does not delete the tree entry; the repair scan does.

The local apply always completes. An object whose requirements no longer hold on this store is skipped and logged,
exactly as a skipped object in any commit is. Once the Tx is forgotten, the repair scan reconciles that object from
the entry, so the scan's skip of live transactions cannot stall. A failed backend commit follows the existing
`BackendCommitFailed` handling.

**Entry absent:** either T did not commit, or this store applied it durably. The existing paths (heartbeat, recovery
driver, proof-of-commit) continue unchanged, and an abort is accepted.

**Lookup unavailable** (the metadata pool cannot be reached): the Tx keeps waiting and retries with backoff, holding
its locks as recovered transactions do today. It never accepts an abort without a completed lookup.

A bootstrap-pool store looks up the bootstrap pool's own tree. After a whole-cluster restart, its lookup waits until a
read threshold of bootstrap stores serves reads, which is the same dependency its recovery driver has today. Recovered
transactions do not block reads (a lock only matters to the reader when a key cannot be restored), so there is no
circularity. Retrying with backoff is enough (my call).

Stores have no client, so the host provides the lookup through a small seam passed to stores, in the style of
`RepairTarget`. Results come back to the store's executor as a posted task (Host threading model).

### 4. Repair: one scan, exact reconciliation

`StoreRepairer.scan` walks the store's TracelessCommits range after its ErrorTree range, in the same pass and under
the same `RepairPolicy` pacing. Both kinds of entry are repaired by the same reconciliation.

**ErrorTree entry (store, object):** reconcile the object; delete the entry as today, when the read state is at least
as new as the missed timestamp (`deleteErrorEntryIfUnchanged`).

**TracelessCommits entry (store, T):**

- Skip it if the store still holds T live (recovered or active). That Tx applies T itself through section 3. This
  needs a new `RepairTarget` query.
- Otherwise reconcile each object in the entry, then delete the entry once every object reconciled. Objects whose
  allocation is gone (refcount reached 0) go through the existing `repairDeletion` path.

**Lifecycle of entries for stores that never come back.** Entries live as long as ErrorTree entries do. A store
offline for days keeps its range untouched until it returns. A store that never returns is eventually rebuilt at the
same pool index. Rebuilding drops its CRL, so it holds no live transactions, and its state is rebuilt from current
reads. The first scan after the rebuild therefore reconciles every entry in the range (they pass at once) and deletes
it. No hook in rebuild or store removal is needed.

**Reconciliation of one object on one store:**

1. **Snapshot.** On the store's executor, record the store's copy: per key its revision (absent keys included), the
   object's revision, refcount, min/max/left/right and timestamp.
2. **Read.** A normal consistent read of the object. Read responses report, per store, which keys are under a write
   lock and whether an object-level lock is held (section 5). The reported state is the union over responders.
3. **Install,** on the store's executor:
   - **Refcount:** install the read's refcount if its serial is higher than the store's. Never lower it. This applies
     to every object type, with no timestamp gate. A `RefcountUpdate` takes an exclusive object lock
     (`RequirementsLocker`), so a refcount change a voter has not yet applied shows up as a reported object-level lock.
     The refcount is then deferred like the other object-level parts, and its fallback is the same proof (below).
   - **DataObjects:** contents and revision are installed when the read's revision differs from the store's and the
     read's timestamp is at least the store's. The object timestamp becomes the larger of the two. HLC timestamps
     are per transaction, so a read no older than the store reflects every transaction the store applied. A
     `RefcountUpdate` advances the timestamp without changing the revision, so equal timestamps with differing
     revisions mean the store missed a data write. A strict "read is newer" gate would leave that write missing.
   - **KeyValueObjects:** for every key not reported locked, make the store's key equal to the read's: install the
     read's version (the store's IDA slice of it) when it differs or the store lacks the key, and delete the store's
     key when the read lacks it. Object-level parts (revision, min, max, left, right) are made equal to the read's
     unless an object-level lock was reported. The store's object timestamp becomes the larger of the two.
   - **Snapshot check:** each key, and the object-level parts, is changed only if it is still exactly as the
     snapshot recorded. Anything that changed since was written by a newer committed transaction, which either reached
     this store or will mark it again. It is left for a later pass.
4. **Locked keys are deferred.** The object is reconciled except for keys (or object-level parts) reported locked,
   and its entry is kept. The next pass reconciles only the deferred keys. Deferral state is kept in memory on the
   repairing host, per entry: a host restart only means the next pass reconciles the whole object again (my call).

**Why the read can be trusted.** The read is an ordinary read of every store of the pool, the store being repaired
included. A committed transaction collected commit votes from at least a write threshold of stores, and a read
restores from at least a read threshold, so at least one responder voted commit for any committed transaction. If the
store being repaired is that responder, it voted, so it either applied the transaction or holds its locks. That responder has either applied it, in which case the read cannot restore the state the transaction
replaced, or still holds its locks, in which case the key is reported locked and deferred. So an unlocked key in the
read reflects every committed transaction. Recovered `VoteCommit` transactions keep their locks too, so a key a
recovering store has not yet applied is deferred rather than reconciled to the old state.

**Fallback for a key that stays locked.** After a key has been deferred for 10 consecutive passes (a new
`RepairPolicy` setting; my call), the scan stops waiting for a quiet moment and proves the key's state with a small
transaction from the repairing host's client:

- `KeyRevision(key, revision the read saw)`, or `DoesNotExist(key)` if the read saw it absent, on an update with no
  operations;
- missed-update tracking disabled, so it cannot create entries.

If it commits, a write threshold of stores agreed on that key at that moment, and the scan reconciles the key from the
read (still under the snapshot check). If it aborts, the key was changing; the scan retries on later passes with the
transaction's normal backoff. Until reconciled, the store is only behind on that key, and reads are served by the
other stores.

Deferred object-level parts use the same fallback, with a different proof:
- KV revision, min, max, left and right: a `RevisionLock` of the read's revision. Its exclusive object lock collides
  with every lock an object-level or key write holds, and it changes nothing (no revision, timestamp or backend
  commit), so like the other proofs it runs with missed-update tracking disabled. Like the earlier `FullContentLock`
  form, it compares the revision but not min, max, left or right.
- A refcount, on either object type: a `RefcountUpdate` that requires the read's refcount and sets that same refcount.

Every proof is an ordinary transaction from the repairing host's client (`StoreRepairer`'s `AspenClient`). An update
with no operations is supported, since TKVL already uses `update(..., Nil, Nil)` as a lock. `KeyRevision` and
`DoesNotExist` take the same per-key lock a real write does. The pass counter lives in memory, so a host restart
restarts the 10-pass count. That is acceptable (my call).

### 5. Read responses report locked keys

`ReadResponse.CurrentState` reports which keys are under a write lock (keys whose `ValueState.lockedToTransaction` is
set, and `absentKeyLocks` keys) and whether an object-level write lock is held (`lockedToTransaction`,
`contentLocked`, `rangeLocks`), alongside the existing `lockedWriteTransactions`. Only repair consumes the new fields.
The wire format changes; persisted data is unaffected.

### 6. The horizon rule for late prepares

When `Frontend.receivePrepare` gets a prepare for a transaction it does not know (not live, not recovered, not in the
status cache), and the transaction started more than the **horizon** ago:

1. It loads the objects and checks proof-of-commit (`RequirementsApplyer.appliedBy`) first. If proven, it answers
   `TxResolved(committed)` exactly as `Tx.proveCommitted` does today.
2. Otherwise it votes abort with a new non-object requirement error, `TransactionTooOld`, without checking
   requirements and without taking locks. The vote is otherwise handled as any abort vote.

- **Horizon:** the host's status cache duration minus a 1 s clock-skew margin (6 s against today's 7 s; my call),
  derived from `Host.txStatusCacheDuration` so the two cannot drift apart. A transaction is always finalized after it
  starts, so any prepare that misses the cache is older than the cache duration.
- Prepares younger than the horizon are handled as today, including proof-of-commit only on the abort path.
- Paxos keeps the rule safe for undecided and decided transactions alike: an abort vote never overturns a value a
  quorum accepted. In the recovery case, section 3's lookup runs before any abort is accepted.
- **Status cache evictions.** The cache is capped at `TransactionStatusCache.DefaultMaxEntries`. An entry evicted for
  size before it is older than the horizon reopens the window for its transaction, so the host logs a rate-limited
  warning when that happens (Scaffeine removal listener with cause SIZE; entries record their insertion time).
- **Cost:** a legitimate transaction whose prepare reaches a store more than about 6 s after it started gets an abort
  vote from that store. Under normal load this never happens. Under extreme CPU starvation some slow transactions
  abort and their clients retry.
- **Orphaned transactions** (the user accepted this, to be revisited if needed): when a client dies mid-transaction,
  the store recovery driver that takes over sends its prepares at least 5 s after the last activity. Stores that
  never got the client's prepare are then usually past the horizon and vote abort. Such a transaction can commit only
  through stores that already had the client's prepare, and a store reloading for more than 6 s mid-transaction is
  affected the same way. Correctness is unaffected. If the extra aborts matter later, the status cache duration and
  the horizon can be raised together.
- The horizon is measured from the client-assigned start timestamp in the transaction description
  (`txd.startTimestamp`), so a client clock running behind makes transactions look older.

## Implementation order

1. Confirm the facts this design depends on, with tests:
   - the ErrorTree gap: a store that misses a KV insert or delete and then applies a later KV update on another key
     keeps the missed change after repair (expected to fail today);
   - a resolved-committed transaction with a held commit keeps its write locks until applied.
2. `deletesKeys` flag (description, codec, builder, store check).
3. TracelessCommits tree creation and the finalizer entries.
4. Recovery lookup and local apply (section 3).
5. Read responses report locked keys (section 5).
6. Exact reconciliation and the combined scan, including the deferral fallback (section 4).
7. The horizon rule (section 6).
8. Docs: CLAUDE.md (Transaction Design, and the pool metadata layout line), Todo.txt (replace the open note on
   traceless changes).

Each step is test-first, per the project's rules.

## Testing

Unit-level, in the style of `FrontendSuite`, `TransactionDriverSuite` and the repair suites:

- **Finalizer:** a store with no `TxCommitted` for a transaction with a refcount change or a flagged KV delete gets a
  TracelessCommits entry in the same batch as its ErrorTree marks. Insert-only KV updates, data updates, stores
  that sent `TxCommitted` with errors, and stores outside known commit voters get none.
- **Flag:** the builder sets `deletesKeys` for any `Delete`; a store votes abort on a `Delete` with the flag false.
- **Recovery:** a recovered `VoteCommit` with an entry applies its update, logs at info, deletes its CRL state, and
  is forgotten. Without an entry an abort is accepted. An abort arriving before the entry is visible is held until a
  later lookup finds the entry, and then the Tx commits. A failing lookup holds the abort.
- **Reconciliation:**
  - the step-1 gap test passes;
  - a key changed on the store after the snapshot is left alone;
  - locked keys are deferred and reconciled on a later pass;
  - the proof transaction runs after the bound, and an abort retries;
  - refcounts never decrease;
  - a deleted object goes through `repairDeletion`.
- **Scan:** TracelessCommits entries for transactions the store holds live are skipped. The others are reconciled and
  deleted. After a rebuild, the first scan reconciles and deletes the rebuilt store's entries.
- **Refcount deferral:** a refcount under a reported object lock is deferred, and the proof transaction then installs
  it.
- **Horizon:** a prepare older than the horizon and missing from the cache gets a `TransactionTooOld` abort vote with
  no locks; one with proof gets `TxResolved`; one inside the cache is answered from the cache; one younger than the
  horizon is handled as today. A size eviction of a young entry logs the warning.
- **Integration:** one test through `TestNetwork` covering the two-recovery delete scenario from "Why", ending with the
  key deleted on every store and no entries left. The proof-of-commit integration test skipped in `2ad0f326` can be
  written alongside it.
- Full `sbt test`, 70 s hang bound, no new "Hung Test" prints, no flaky tests.

## Decisions

Made by the user:

- Cover every traceless change, refcount decrements included.
- A recovering store applies a committed update locally and logs it at info.
- The repair scan that walks the ErrorTree also walks and cleans the new tree.
- Fold the ErrorTree repair gap for KV objects into the same reconciliation.
- A fallback is needed for keys that stay locked.
- Include the horizon rule; drop the `KeyRevision` switch for TKVL deletes.
- Accept the 6 s horizon and the extra aborts it causes for orphaned transactions; expand later if needed.

My calls, for review:

- The tree's name, `TracelessCommits`.
- A one-bit `deletesKeys` flag set automatically by `TransactionBuilder`, rather than recording every KV update without
  a required revision.
- Entries only for commit voters (all stores when the voters are unknown) that sent no `TxCommitted`.
- Deferral state kept in memory on the repairing host.
- A fallback bound of 10 passes, which is 5 minutes at the 30 s scan floor.
- A horizon of the cache duration minus 1 s.
- Proof-of-commit before requirements only for transactions older than the horizon, to keep the hot path cheap (the
  KV proof scans the object's values).
- `DeleteMin`/`DeleteMax`/`DeleteLeft`/`DeleteRight` stay outside the flag because every use carries a required
  revision.

## Left to the builder

- Encoding of the tree value and the new read-response fields.
- How the recovery lookup seam and the new `RepairTarget` queries are shaped and wired into `Host`.
- How per-key installs compute the store's IDA slice (`getRebuildDataForStore` today builds the whole object's).
- Where the warning for early evictions is rate-limited.
- Whether the step-1 lock-retention test finds a held-commit path that releases early; if so, fix it so locks are
  kept, since section 4's argument depends on it.

## Residual risks

- A key written continuously can stay unreconciled on a lagging store until writes pause. Reads are unaffected.
- An undersized status cache under extreme volume can evict entries before the horizon. This is visible through the
  warning, and fixed by raising the cap.
- The bootstrap pool's own ErrorTree writes stay untracked (see Non-goals).
