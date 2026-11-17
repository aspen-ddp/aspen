# Transaction Requirements, Locking and the Commit Window

This document specifies how a store checks a transaction's requirements, which locks a commit vote takes and how
long it holds them, how votes are tallied against each pool's thresholds, and how commits of one object are ordered
at a store. Code paths are relative to `src/main/scala/org/aspen_ddp/aspen/` and cite the class and symbol.

Sibling specifications (not repeated here): [paxos-transactions.md](paxos-transactions.md) (Paxos roles, proofs,
late commits, the transaction horizon), [crash-recovery-log.md](crash-recovery-log.md),
[transaction-recovery.md](transaction-recovery.md) (driver and participant recovery, UnackedCommits, expiry),
[repair-process.md](repair-process.md), [rebuild-process.md](rebuild-process.md),
[systemic-loss-protection.md](systemic-loss-protection.md).

## 1. Scope and model

A transaction (`common/transaction/TransactionDescription`) carries a list of `TransactionRequirement`s, the Paxos
metadata (primary object, designated leader, `primaryObjectIDA`, `poolIDAMap`), the objects it allocates
(`allocatingObjects`), the `replaySafe` flag, and finalization actions. Object data travels separately, one
`ObjectUpdate` per updated object per store, in the client's prepare (`TxPrepare.objectUpdates`, `fromClient = true`).
A driver's prepare carries no data.

Every store of every referenced pool receives the whole description and acts only on the objects it hosts:
`Tx.requirements` keeps the object requirements whose pointer is in `TransactionDescription.allHostedObjects(storeId)`
(same pool, index below the pool's width) plus every non-object requirement. The store:

1. checks the description itself (`DescriptionValidator`, section 2.5), then the requirements against its in-memory
   `ObjectState`s (`RequirementsChecker.check`);
2. if everything passes, votes commit and takes locks (`Tx.lock`, `RequirementsLocker.lock`) in the same executor
   step, then persists the vote in its device CRL; the prepare response, and the `TransactionVote` to the client, are
   sent only from the completion handler of that save (`Tx.sendPrepareResponse`);
3. holds the locks until the transaction resolves. On commit, `Tx.doCommit` releases them, re-checks the requirements
   with `applying = true` (no lock is enforced), applies the update in memory (`RequirementsApplyer.apply`) and issues
   one backend commit per changed object (`Frontend.commit`), ordered per object by the in-flight rule (section 8).

The driver (and, for commits, the client) tallies votes per object against that object's pool's write threshold
(section 7). The safety argument is a quorum-intersection argument plus a lock argument:

* **Quorum.** Every IDA satisfies `2 * writeThreshold > width` (section 7). A commit needs at least `writeThreshold`
  commit votes for every referenced object, so two transactions that conflict on an object share at least one voting
  store for it.
* **Lock.** A store that voted commit for a transaction votes commit for no conflicting transaction until the first
  one is resolved and applied (or aborted). Locks live in memory and are re-taken from the CRL after a crash
  (section 10.4).

Vocabulary: a "vote" is a store's `TransactionDisposition` (`VoteCommit` / `VoteAbort`; `Undetermined` is no vote),
fixed once decided. "Voter" is a store, "holder" a transaction holding a lock. `t`, `w` and `r` are a pool's write
threshold, width and read threshold (`IDA.consistentRestoreThreshold`).

## 2. Requirements and operations

`TransactionRequirement` is sealed (`common/transaction/TransactionRequirement`). "Changes object" means the
requirement is in `RequirementsApplyer.changedObjects` (an exhaustive match with no wildcard, so a new requirement
must be classified): such objects get a backend commit and take part in the in-flight rule (section 8). "Trace" is what
`RequirementsApplyer.appliedBy` accepts as proof that a store already holds the update (paxos-transactions.md, proofs).

### 2.1 Object requirements

| Requirement | Check (`RequirementsChecker.check`) | Lock (`RequirementsLocker.lock`) | Apply (`RequirementsApplyer.apply`) | Changes object | Trace |
|---|---|---|---|---|---|
| `DataUpdate(ptr, rev, Overwrite)` | `getState` (whole object), `checkRevision`: revision == `rev` and object timestamp <= tx timestamp (`RevisionMismatch`, `ObjectTimestampError`); the object's data must be in the transaction (`MissingObjectUpdate`); `checkSliceSize` unless the update does not grow the slice | X | revision := txid, timestamp := max(current, tx), data := update | yes | revision == txid |
| `DataUpdate(.., Append \| WriteRange)` | as Overwrite, plus the `EncodedAppendDelta` must match the current slice (`AppendMismatch`; an undecodable delta is a mismatch); the size check uses the delta's `newSliceLength` | X | as Overwrite, data := `delta.applyTo(data)` | yes | revision == txid |
| `RefcountUpdate(ptr, required, new)` | `getState`, `checkRefcount`: refcount (count and update serial) == `required`. No revision or timestamp check | X | refcount := new, timestamp := max(current, tx), revision unchanged. A count of 0 makes `Frontend.commit` a deletion | yes (also when `new == required`) | none |
| `VersionBump(ptr, rev)` | `getState`, `checkRevision` | X | revision := txid, timestamp := max | yes | revision == txid |
| `ExclusiveRevisionLock(ptr, rev)` | `getState`, `checkRevision` | X | nothing | no | none |
| `SharedRevisionLock(ptr, rev)` | `getSharedState`: collides with X, C, any part lock (K, N, R) and refuses while the exclusive-preference window is open; then `checkRevision` | S | nothing | no | none |
| `KeyValueUpdate(ptr, requiredRevision, contentLock, requirements, deletesKeys, pinned)` | section 2.2 | section 2.2 | section 2.3 | yes | section 2.3 |

Every object requirement first goes through `loaded`: an object the store could not load fails `MissingObject`; an
allocated object whose load found an existing state fails `ObjectExists` (section 9.4); a refcount-0 object fails
`MissingObject` on every backend (MapBackend keeps refcount-0 states); a discarded state (`reloadRequired`) fails
`StaleObjectState`. An exception inside a check is recorded as `RequirementCheckFailure` against the requirement's
object (never as a non-object error, which `apply` would ignore), so it is an abort vote, never a silent pass.

`checkRevision` also enforces `object timestamp <= tx timestamp`, so a transaction never moves an object's revision
"back in time" relative to its HLC. Note that this check applies to lock-only requirements too, at vote and at the
apply re-check (see Q-W1).

### 2.2 `KeyValueUpdate`: key requirements and what a KV update locks

The update's operations are decoded from its object data (`KeyValueOperation.decode`). Order of checks in the KV
branch of `RequirementsChecker.check`:

1. a key `Delete` without `deletesKeys` fails `UnflaggedDelete`;
2. a `pinned` flag that the operations do not justify (`Pins.writesCheckedKey`) fails `FalsePin`;
3. `refuseBlindWrites` (section 2.5) fails `BlindWrite`;
4. the update is "whole object" when it has a `requiredRevision`, a `contentLock`, a `KeyObjectRevision`, or any
   object-level operation (`KVWrites.objectLevel`); `getState(oid, wholeObject)` then checks object locks (and part
   locks when whole-object) and notes a held shared lock;
5. `checkKVRequirements` (with `ignoreLocks = applying`);
6. `MissingObjectUpdate` if the data is absent, so a KV update never votes commit without its operations;
7. unless applying, every key an operation writes is checked like a key lock: the present key's holder, or else the
   absent-key holder, must be none or this transaction;
8. `checkSliceSize` against `KVObjectState.encodedSizeAfter`, which uses the same last-writer-wins rule as apply.

| Item | Check | Lock taken | Notes |
|---|---|---|---|
| `requiredRevision = Some(r)` | revision == r, object timestamp <= tx timestamp | X | the only way a KV update moves the object revision |
| `contentLock = Some(FullContentLock(keys))` | set of (key, revision) equals the object's content (`ContentMismatch`); no other holder of C or of any present key | C (`kvs.contentLocked`) | does not lock each value; other value writers collide through C |
| `KeyRevision(k, rev)` | key present (`KeyExistenceError`), no other K on it and no other C, revision == rev, key timestamp <= tx timestamp (`KeyTimestampError`) | K on k | |
| `KeyObjectRevision(k, rev)` | object revision == rev; no timestamp check | X | whole-object |
| `WithinRange(k, ordering)` | k within `[min, max]` (`WithinRangeError`) | R (`kvs.rangeLocks`, a set: many holders) | checks no key lock |
| `Exists(k)` | key present, no other K, no other C | K | |
| `MayExist(k)` | present: no other K or C; absent: no other N | K if present, else N | |
| `DoesNotExist(k)` | absent (`KeyExistenceError` if present), no other N | N (`absentKeyLocks(k)`) | |
| `TimestampEquals/LessThan/GreaterThan(k, ts)` | present, key timestamp ==, <, > ts, no other K or C | K | no production caller |
| `Insert(k, v)` | written-key lock check (step 7) | `lockWrites`: K if present, else N | a blind insert (no requirement on k) still locks k |
| `Delete(k)` | as Insert; needs `deletesKeys` and a `KeyRevision(k, _)`, a required revision or a content lock (step 3) | as Insert | leaves no trace |
| `SetMin/SetMax/SetLeft/SetRight`, `DeleteMin/Max/Left/Right` | `objectLevel`, so whole-object; needs a required revision (step 3) | X (`lockWrites`) | |

`KVWrites.from` decodes each KV update's writes in `Tx.lock` and keeps them in `Tx.lockedWrites`, so `unlock` releases
exactly what `lock` took even if the data is later dropped. An undecodable update is skipped there (its check already
failed). Keys compare by content (`common/objects/Key`). `unlock` releases a lock only if this transaction still holds
it: an applied Insert replaces a key's `ValueState`, dropping any lock on it, and another transaction may have locked
it since.

### 2.3 How a KV update is applied, and what it leaves

`RequirementsApplyer.apply`: object timestamp := max(current, tx timestamp) for every KV update; revision := txid only
with a `requiredRevision`. Operations apply in order: `Insert` replaces the key's `ValueState` (revision txid,
timestamp tx, unlocked) only when it wins `KVObjectState.insertWins` (later transaction timestamp, ties broken by
revision UUID; equal is the same transaction re-applying), otherwise it is a no-op, not a skip; `Delete` removes the key;
the eight object-level operations set or clear `min/max/left/right`. Then `state.data := kvs.encode()`.

Last-writer-wins makes sequential blind Inserts of one key converge whatever order a store applies them in. A checked
Insert (`KeyRevision`, `DoesNotExist`, a required revision) always wins in practice: its check rejects a key written
later, and the client's HLC places it after the state it read. Deletes and object-level fields carry no timestamp, so
they cannot be ordered this way; that is why blind ones are refused (section 2.5).

Traces (`appliedBy`): object revision == txid for `DataUpdate`, `VersionBump` and a KV update with a required
revision; for a KV update without one, any current key carrying revision txid; an allocated object (`allocatedOver`)
at revision txid. Traceless: `RefcountUpdate`, the locks, a KV update that only deletes or changes object-level
fields, and one whose Inserts were all superseded. The UnackedCommits fallback does not depend on traces: the
finalizer writes an entry for every counted commit voter that did not acknowledge, whatever its change
(`client/internal/transaction/MissedUpdateFinalizationAction`). `TransactionDescription.tracelessObjects` still exists
but no code calls it.

`apply` itself runs `check(applying = true)` first and skips every object with an error (`Result.skippedReasons`).
An exception while applying an object marks it `failed`: the caller discards its in-memory state (`Frontend.discard`)
and commits nothing for it.

### 2.4 The non-object requirement

`LocalTimeRequirement(timestamp, Equals | LessThan | GreaterThan)` holds when the checking store's `HLCTimestamp.now`
equals, is less than, or is greater than `timestamp` (`RequirementsChecker.checkLocalTime`). It takes no lock and
applies nothing. Every store hosting any object of the transaction checks it against its own clock, so stores may
disagree; a failure is a non-object error, an abort vote for all of that store's objects. It is test-only: no
production code constructs it and `TransactionBuilder` has no method for it. Shared decisions should use HLC object
timestamps instead.

### 2.5 Structural rules: builder and store

The client builder (`client/internal/transaction/TransactionBuilder`) refuses: a second content update of an object
(`MultipleDataUpdatesToObject`), a second refcount update (`MultipleRefcountUpdatesToObject`), a lock on an updated
object or a shared/exclusive mix (`ConflictingRequirements`), a blind KV Delete or object-level change
(`refuseBlindWrites`, `BlindKeyValueWrite`, even when replay-safe), and an unpinned transaction not marked
`replaySafe(reason)` (`UnpinnedTransaction`). A `RefcountUpdate` may accompany a content update of the same object
(allocation does this).

**Pin rule** (`common/transaction/Pins`): a transaction must carry a write that invalidates its own requirement once
applied: a `DataUpdate` at a required revision other than `Allocating`, a `VersionBump`, a KV update with a required
revision, a `KeyRevision(k, _)` on a key the same update Inserts or Deletes, or a `RefcountUpdate` whose new refcount
differs from the required one. The builder records the key pin in the description as `KeyValueUpdate.pinned`
(`Pins.writesCheckedKey`) and the client's claim as `TransactionDescription.replaySafe`.

**Store-side validation.** Stores do not trust the builder. Before checking requirements, a client-prepared Tx runs
`DescriptionValidator.validate` once (`Tx.invalidDescription`, lazily, at vote only); a refusal is the non-object error
`InvalidDescription` (whole vote abort, never waited out) and is logged at warn:

* the store's own pool entry in `poolIDAMap` must equal its pool's IDA (`StoreConfig.ida`, passed as
  `Frontend.poolIDA`; `None` skips this, tests only), and every referenced pool must have an entry. Each pool's stores
  check their own entry, so a lowered `writeThreshold` for any pool draws abort votes from every honest store of
  that pool and cannot commit;
* `primaryObjectIDA` is the primary pool's entry, the primary object is referenced, no referenced pool tolerates more
  failures than the primary's (the builder picks the most tolerant), and the designated leader index is below the
  primary pool's width;
* per object at most one content update and one `RefcountUpdate`, no lock beside an update, no shared lock beside
  another lock (duplicate `ExclusiveRevisionLock`s, harmless, are allowed as the builder allows them);
* the pin rule from the description alone (`Pins.declared`, taking `pinned` as the key pin), unless `replaySafe`.

The stores that hold a `pinned` object check the claim against the operations (`FalsePin`); a store that does not hold
the object relies on the flag, which is sound because a false flag draws abort votes on that object from every honest
store of its pool. The blind-write rule is enforced at every check, vote and apply re-check (`refuseBlindWrites`):
outside an allocated object's initial content, a key Delete needs a `KeyRevision` on that key, a required revision or
a content lock, and an object-level change needs a required revision (a content lock does not move the revision that a
repair proof's `ExclusiveRevisionLock` locks).

The validation runs at vote only. It depends on the description and the store's pool alone, so a committed
transaction passed it at a write threshold of every pool's stores. The `replaySafe` bit is the one claim stores must
trust: no store can prove a replay harmless. A malicious client is outside the trust model.

TKVL planners derive per-write checks (`client/tkvl/KeyValueListNode.putRequirements` / `deleteRequirements`): a put
gets `KeyRevision(k, rev)` for a key the node read holds and `DoesNotExist(k)` otherwise (`PutRequirement.Checked`, the
default), or exactly `DoesNotExist` (`MustNotExist`) or `KeyRevision(k, rev)` (`AtRevision`); a delete gets the key's
`KeyRevision`. `PutRequirement.Blind` (`TKVLInsert.blind`) has no production caller. `MissedUpdateMarker` writes
conditional checked inserts (`TKVLInsert.replaces`).

## 3. Lock classes

`ObjectState` (`server/store/ObjectState`) and `KVObjectState` hold six kinds of lock:

| Class | Field | Holders | Taken by |
|---|---|---|---|
| X | `ObjectState.lockedToTransaction` | one | DataUpdate, RefcountUpdate, VersionBump, ExclusiveRevisionLock, KV with `requiredRevision`, `KeyObjectRevision`, an object-level KV operation |
| S | `ObjectState.sharedLocks` | many | SharedRevisionLock |
| C | `KVObjectState.contentLocked` | one | `FullContentLock` |
| R | `KVObjectState.rangeLocks` | many | `WithinRange` |
| K | `ValueState.lockedToTransaction` | one per present key | key requirements on a present key; Insert/Delete of a present key |
| N | `KVObjectState.absentKeyLocks` | one per absent key | `DoesNotExist`, `MayExist` on an absent key; Insert/Delete of an absent key |

`ObjectState.lockedWriteTransactions` is the union of X, K, N, C and R (not S); whole-object checks test it
(`checkPartLocks`), and parked reads wait on it. `lockHolders` adds S.

A single-holder lock is taken by `RequirementsLocker.take`. For a live commit vote the preceding check guarantees no
other holder (finding one on X is logged as an error). Only recovered commit votes, which re-take locks without a
check, can find one (section 10.5). The holder found is not dropped: it is kept in `ObjectState.displacedLocks`
(slot X, C, K(k) or N(k), plus txid), which is empty outside recovery. A release by the slot's current holder
(`RequirementsLocker.give`) passes the slot to the newest holder it displaced; a release by a displaced holder only
removes its entry. So a slot stays locked until every holder has released it, whatever order the holders took and
release it in. `lockedWriteTransactions`, `lockedKeys` and `objectLevelLocked` count displaced holders.

Locks are attached to the one `ObjectState` instance transactions use while any references it (`Frontend.referenced`,
`retain`, `stateForTransaction`). A discarded state still holding locks stays the referenced one, marked
`reloadRequired`, so its locks keep being honoured while every check against it fails `StaleObjectState`.

## 4. Lock-compatibility matrix

Row = the request being checked, column = a lock already held by another transaction. `x` = collision
(`TransactionCollision`, an abort vote unless waited out, section 8.4), `-` = compatible. Only non-`applying` checks
are shown: `applying = true` enforces no lock and no window.

| Request \ Held | X | S | C | R | K (same k) | K (other k) | N (same k) | N (other k) |
|---|---|---|---|---|---|---|---|---|
| Whole-object (DataUpdate, RefcountUpdate, VersionBump, ExclusiveRevisionLock, KV with `requiredRevision`/`contentLock`/`KeyObjectRevision`/object-level op) | x | x | x | x | x | x | x | x |
| `SharedRevisionLock` | x | - | x | x | x | x | x | x |
| Key-level KV (key requirement, Insert, Delete of present k) | x | x | x | - | x | - | - | - |
| Key-level KV on absent k | x | x | x | - | - | - | x | - |
| `WithinRange` only | x | x | x | - | - | - | - | - |

Additional rules:

* **Exclusive-preference window** (`SharedRevisionLock.ExclusivePreferenceWindow`, 250 ms,
  `ObjectState.exclusiveWaitUntil`). A non-shared request that meets a held S lock records the collision
  (`sharedCollision`) and raises it only after the requirement's own checks pass, so a requirement that also fails
  otherwise reports that error. After the requirement loop the window opens on every such object only if the
  transaction's only errors are those shared collisions and it has no non-object error (`onlySharedBlocked`). While it
  is open, new shared requests collide with their own transaction id: the waiting writer is not tracked, and a
  self-id never looks like a completed holder to the driver. Residual: a writer that opens the window and then aborts
  for reasons at other stores, or never retries, still makes shared lockers collide for up to 250 ms.
* A key-level KV update without a required revision does not change the revision yet collides with S (every non-shared
  request tests `sharedLocks`). Conservative.
* `WithinRange` conflicts only with whole-object requests (through R).
* A transaction never collides with itself (`collide` skips its own id; `otherLock` likewise).

## 5. Per-operation conflict summary

| Operation | Takes | Collides with held | Ordering of committed writes |
|---|---|---|---|
| DataUpdate / VersionBump | X | anything on the object | revision chain |
| RefcountUpdate | X | anything on the object | refcount (count, serial) chain |
| ExclusiveRevisionLock | X | anything on the object | applies nothing |
| SharedRevisionLock | S | X, C, R, K, N, an open preference window | applies nothing |
| KV with `requiredRevision` | X, plus C/K/N/R for its items | anything on the object | revision chain |
| KV with key items only | K/N on named and written keys, R | X, S, C, same-key K/N | key requirement chain |
| Blind `Insert` | K or N on the key | X, S, C, same-key K/N | last writer wins on (timestamp, revision) |
| `FullContentLock` | C | X, S, C, any K/N/R | content set |
| `WithinRange` | R | X, S, C | none needed |
| Append / WriteRange | X | anything on the object | revision chain |

## 6. Invariants

"Holds" means holds in the code at HEAD assuming the quorum property of section 7; exceptions name the weakness.

**R1 (per-object serializability).** Two committed transactions that both move an object's revision are ordered by the
revision chain: each requires the revision it read, a store accepts a revision requirement only against the revision
it holds and only when no other transaction holds a conflicting lock (R4), and with R2 at most one transaction commits
per required revision. Refcount updates are chained the same way by (count, serial); checked key writes by their
key's revision or absence.

**R2 (quorum overlap).** A commit decision implies, for every referenced object, at least its pool's `writeThreshold`
commit votes from that pool's stores (driver tally and client `ClientTransactionDriver.commitCertain`, section 7), and
every IDA has `2 * writeThreshold > width`. Two commits on one object share a voting store.

**R3 (vote implies lock until resolution).** A store that decides a commit vote takes its locks in the same executor
step (`determineDisposition` then `lock()`, in `completeLoad`, `endCollisionWait` and `answerPrepare`) and sends the
vote only after the CRL save holding it completes. It holds them until the transaction resolves: on abort until
`resolvedAndAllObjectsLoaded`, on commit until `doCommit` (or, for a commit held for its turn, until it is issued).
After a restart the locks of recovered commit votes are re-taken before any message is handled (section 10.4), and two
recovered votes on one lock both hold it (section 3, displaced holders).

**R4 (conflicting requests are refused while a lock is held).** While T holds a lock, a request of a conflicting class
(section 4) by another transaction gets `TransactionCollision`, and the store votes abort or withholds its vote
(section 8.4). Check and lock happen in one executor step, and a Tx touches state only on its store's executor.

**R5 (no vote on stale local state).** A vote is decided only when no commit of any of the transaction's hosted
objects is in flight or held (section 8), so it is checked against exactly the state a recovery would produce. An
object whose backend commit failed, or that is quarantined, gets an abort vote (`BackendCommitFailed`,
`ObjectQuarantined`).

**R6 (apply is idempotent where requirements pin a version).** Applying re-checks the requirements; a failing object
is skipped and reported in `TxCommitted.objectCommitErrors`, which makes the finalizer mark the store for repair.
Revision, refcount and checked key requirements make a transaction apply at most once per store and never over a
later chained write. A blind Insert re-applies harmlessly (last writer wins). Exception: a `DoesNotExist` or blind
Insert re-applied after a later delete of its key (Q-W3).

**R7 (no divergence between replicas).** Committed transactions not ordered by R1 either touch disjoint keys and no
common object-level field (K/N/X locks at the common store), or are blind Inserts of one key (last writer wins), so
their application order does not change the result. The object timestamp is the max of every applied transaction's,
so it is order-independent too. A store that applies them in an order its re-check rejects skips and reports, and
repair restores it.

**R8 (lock lifetime).** Locks are released when: the Tx resolves abort; the Tx resolves commit and its commit is not
held (`doCommit` releases them before applying); a held commit is finally issued; the Tx expires (`Tx.expire`) or is
forgotten without resolution (`releaseObjects`). They are not held across the backend write: followers are ordered
by the in-flight marker (R9). Every release answers the reads parked on the objects (`Frontend.answerParkedReads`),
after the update is applied in memory.

**R9 (one in-flight commit per object).** A transaction touching an object with a transaction or repair commit in
flight, or a held commit queued, decides no vote and holds no locks until it persists. A committed transaction does
not issue its commit while an earlier commit of any object it changes is in flight or queued ahead of it.

**R10 (a failed backend commit poisons the object).** After a backend commit failure (or `poisonObject`), the object is
in `failedCommits` until the store reloads: every later transaction on it votes abort, and committed transactions do
not apply to it and report it.

**R11 (votes are final).** A disposition is decided only while `Undetermined`; a recovered Tx keeps its persisted
vote (`completeLoad` decides only when it has none).

**R12 (waiting is safe).** A Tx waiting for a lock holder or for in-flight commits holds no locks and gives no vote,
so waits form no cycle and cannot make two transactions both commit.

**R13 (no locks once the outcome is known).** `lock()` is skipped when the outcome is known (`outcomeKnown`, a
resolution restored from the CRL, or `txStatus` resolved).

**R14 (a commit voter that cannot apply stops counting).** A commit voter has dropped its locks before it applies,
so one that cannot hold the update it voted for must not keep serving as if it had: a live voter fail-stops the store,
a recovered one quarantines the object (section 8.5). Exception: the live fail-stop also fires when no invariant was
violated (Q-W1).

## 7. Vote tallying against the IDA threshold

**Driver** (`TransactionDriver.receiveTxPrepareResponse`). Only a driver that may propose (primary-pool store,
`mayPropose`) and holds no proof tallies, and only once its current round has a prepare quorum. Replies to another
round are ignored; `nextRound` clears `peerDispositions` and stores resend their fixed dispositions. Per referenced
object, over its own pool's `width` indexes (`txd.poolIDAMap(ptr.poolId)`): `VoteCommit` counts as commit,
`Undetermined` as nothing, any other disposition as abort. Then:

* **abort-certain** if `width - nAbort < writeThreshold` for any object: the driver proposes abort at once;
* **commit** if every object has `nCommit >= writeThreshold`;
* otherwise **undecided**: the driver waits. It never proposes abort on an undecided tally (a client may learn commit
  from the votes), but it may propose an accepted value its promise quorum reports. Expiry ends a Tx that stays
  undecided (transaction-recovery.md).

A later round skips the tally: its local proposal is sticky, since votes never change. A store's vote is one bit for
the whole transaction, so a failure on one of its objects is an abort vote for all of them (conservative). One
object's votes come only from its own pool, so another pool's abort votes never lower its count.

**Client** (`ClientTransactionDriver.receive(TransactionVote)`, the 1-RTT commit). The client records each store's
first vote and completes `Committed` once every referenced object has its pool's `writeThreshold` commit votes
(`commitCertain`). Abort is learned only from the leader's resolution. The vote reaches the client only after the CRL
save that holds it.

IDA constraints, checked by every IDA constructor (`common/ida/IDA`) and so by every decoder, which builds IDAs
through them:

| IDA | Constraint | Consequence |
|---|---|---|
| `Replication(width, t)` | `t >= width/2 + 1` | `2t > width` |
| `ReedSolomon(width, k, t)` | `k >= width/2 + 1`, `t >= k` | `2t > width` |
| `RandomAccessReedSolomon(width, k, t, ..)` | same | same |

Edge cases:

* **Exactly the threshold.** `t` commit votes decide commit whatever the unreplied stores later say: those `t` votes
  are durable and their stores hold the locks.
* **One store lagging** (`w = 3, t = 2`): two commit replies decide commit; one commit and one abort wait for the
  third.
* **Mixed pools.** Each pool needs its own threshold; one pool with too many aborts aborts the transaction.
* **Replies beyond the width** are ignored (the loop is over `0 until width`).
* **Thresholds come from the description.** Stores refuse a description that misstates their own pool's IDA (2.5), so
  a lowered threshold cannot gather a commit.

## 8. The commit window

### 8.1 What the window is

From a commit decision at a store until the backend reports the commit done, the store has applied the update in
memory, released its locks (`Tx.doCommit`) and handed the state to the backend (`Frontend.commit`). Without further
rules a following transaction would see no lock and the new in-memory state, and could vote while the backend still
lacked the earlier change. The store therefore keeps a total order of backend commits per object: one in-flight
commit per object.

### 8.2 State (`Frontend`)

* `txCommitsInFlight: ObjectId -> count`: outstanding backend commits of transactions (deletions included) and repair
  writes (`reconcileSettled`).
* `heldCommits: ObjectId -> List[Tx]`: committed transactions waiting their turn, oldest first.
* `commitWaiters: ObjectId -> List[Tx]`: transactions withholding their vote.
* `commitInFlight(oid) = txCommitsInFlight.contains(oid) || heldCommits.contains(oid)`.
* `failedCommits`: never cleared; the store reloads with a new Frontend.
* `quarantined`: section 8.5.

### 8.3 Behaviour

1. **Vote path** (`Tx.determineDisposition`, in this order): a frozen, unresolved Tx (past expiry) decides nothing; a
   muted Tx votes abort without checking (it proves the commit if an object shows it); a Tx that has seen no client
   prepare votes abort with `NoClientPrepare` without checking (prefer-abort; it is called only once the wait for the
   client's prepare is over or the outcome is known); an invalid description votes `InvalidDescription`; a failed or
   quarantined hosted object votes abort; a hosted object with `commitInFlight` and an unknown outcome makes the Tx
   wait (`awaitingCommits`, `Frontend.awaitCommitPersisted`), no timer and no locks, until `commitPersisted` has
   arrived for each; otherwise the requirements are checked, and a collision may be waited out (8.4). The in-flight
   test covers every hosted object, lock-only ones included; a lock-only object has a commit in flight only when another
   transaction changes it.
2. **Commit path** (`Tx.resolvedAndAllObjectsLoaded`): if the commit decision arrived before the object data
   (`commitAwaitsData`: no client prepare and data missing, or adopted data not yet saved to the CRL), the commit is
   held, without locks, since a commit vote requires the data. Else if `holdForTurn()` finds a changed object in
   flight or held, the Tx joins every changed object's queue in one `holdCommit` call and keeps its locks and
   references. Queues are appended to together, so the oldest held commit heads every queue it is in, and held commits
   cannot wait on each other in a cycle.
3. **Issue** (`doCommit`): release the locks, apply (`issueCommits`), answer parked reads, one `Frontend.commit` per
   changed object that applied, then `heldCommitIssued` removes the Tx from the queues and `commitTurnFree` wakes the
   next. A store holding an applied stub for the transaction, or owed nothing through its UnackedCommits entry
   (`notOwedHere`), applies nothing and reports its changed objects (transaction-recovery.md).
4. **Complete** (`Frontend.backendOperationComplete`): the count drops; at zero `commitTurnFree` runs last, so a
   synchronous backend completes inside the next transaction's commit safely. It first retries a parked repair (the
   one settled moment), then tells the head held commit, or else every waiting voter. On failure the object enters
   `failedCommits`; the state stays pinned and served (the store may have voted at the threshold and must neither lose
   nor revert it), a deletion keeps answering `ObjectNotFound`, the transaction's CRL state is kept, and the counter
   still drops so followers wake and vote abort.
5. **Followers see persisted state.** A voter waits for the persisted state, so it checks exactly what a recovery
   would produce.

A wait for in-flight commits has no timer: a backend commit always completes or fails. A hung device stalls every
transaction on its objects at that store (other stores still vote); this is tracked in Backlog.txt ("Isolate every
device's I/O, and detect and handle hung devices", including why there is no per-commit watchdog).

### 8.4 `CollisionWait`: waiting out a holder the transaction is built on

A transaction whose check fails only with collisions against holders it is **built on** (`Tx.buildsOn`: one of its
`DataUpdate`, `VersionBump`, `ExclusiveRevisionLock`, `SharedRevisionLock` or KV `requiredRevision` requirements on that
object requires the revision the holder writes) and that still hold locks here (`Frontend.holdsLocks`) does not vote
(`Tx.awaitableHolders`). It registers with each holder (`awaitLocksReleased`), holds no locks, and decides when the
holders release (`locksReleased`, called at the end of `releaseObjects`, after the commit was issued, so the waiter
then sees the commit in flight and waits for it to persist) or after `Tx.CollisionWait` (500 ms), checked at the 1 s
store heartbeat (`Store.heartbeat` -> `Tx.checkCollisionWait`), so a wait lasts up to about 1.5 s. On expiry the check
runs once more and a remaining collision is an ordinary abort vote. Recovered Txs, and any non-object error, never
wait. A collision with a holder the transaction does not build on is an immediate abort vote. `objectErrors` keeps
one error per object, so a later error for an object masks an earlier one; the re-check after the wait only costs
time.

### 8.5 A commit voter that cannot apply: fail-stop, quarantine, poison

`Tx.issueCommits`, for a commit voter (`disposition == VoteCommit`), after the apply:

* an apply that threw (`result.failed`) fail-stops the store (`Frontend.failStop`), live or recovered;
* a **live** voter with any skipped object, other than one that already shows the update (`appliedBy`) or one whose
  earlier backend commit failed or that is quarantined, fail-stops the store with an `INVARIANT VIOLATION` naming the
  transaction each such object now shows. This test is over every skipped object, lock-only ones included (Q-W1);
* a **recovered** voter's skipped changed objects, except those already applied and allocations the backend missed
  (`allocatedHere`), are quarantined (`Frontend.quarantine`), and an already quarantined changed object is
  quarantined by this vote too. Its re-check may fail legitimately (section 10.5a), which the store cannot tell from a
  state invalidated under recovered locks.

A fail-stopped store drops every transaction message, read, repair and heartbeat, and `Store.onFailStop` has its host
mark it rebuild-required and unload it. Every fail-stop is a lost-store declaration (Backlog.txt, "System-level
protection against correlated lost store declarations"; Backlog 5.8 on whether a whole-store fail-stop is overkill).

A quarantined object votes abort for every transaction (`ObjectQuarantined`); committed transactions apply nothing to
it and report it. The store has its host verify it (`QuarantineVerifier` -> `RepairService.verifyObject` ->
`StoreRepairer.verifyObject`, bounded by `VerifyDeadline`), and it clears once the store's copy is complete against a
read at least as new as the quarantining transaction's start: what repair requires before dropping an ErrorTree entry
for it. Retries back off from 1 to `MaxQuarantineRetryInterval` (60) heartbeats. The quarantine is in memory only;
the quarantining Tx's CRL state, object data included, is kept until it clears (`concludeTransactionState` defers a
finalization or stub to `verificationComplete`), so a reloaded store quarantines again.

A recovered commit vote whose object fails to load with anything but `ObjectNotFound` fail-stops the store
(`Tx.objectLoadFailed`), because a later load could succeed and find no lock. On a write-frozen store
(`Backend.isWriteFrozen`, a full FSIF device) it poisons the object instead (`Frontend.poisonObject`, the R10 rule):
out of space must never lead to a rebuild. `ObjectNotFound` is permanent (the object is gone or being deleted).

## 9. Behaviour in specific situations

### 9.1 How each pool checks only its objects

`Tx.requirements` keeps only hosted objects' requirements plus non-object ones, so a store never locks or votes for
another pool's objects, and each pool's votes are tallied only against that pool's width and threshold. The
description checks (2.5) are per store too: each pool guards its own entry.

### 9.2 When locks are released

See R8. A commit held for its turn keeps its locks and references (`releaseObjects` is a no-op while `commitHeld`). A
recovered commit vote whose object did not load takes no lock on it (`RequirementsLocker.lock` skips unloaded objects),
which is why such a load failure fail-stops the store (8.5). A Tx forgotten before all its objects loaded releases
through the same `releaseObjects`.

### 9.3 Why stores that apply in different orders converge

1. Different-object updates commute.
2. Same-object revision-, refcount- or key-chained updates: a store that receives the later transaction's commit
   first re-checks at apply and skips it, reports it in `TxCommitted.objectCommitErrors`, and the finalizer marks it;
   repair installs the state (repair-process.md).
3. Concurrent key-level KV updates of one object hold disjoint key sets, or they would have collided at the common
   store (R2, R4).
4. Sequential blind Inserts of one key apply last-writer-wins, so a lagging store or a crash re-apply converges.
5. Object timestamps are the max of all applied updates, so they agree once the same set is applied.

A non-voter that applies a revision-chained update after a later unrelated update raised the object's timestamp
(a refcount change or blind Insert with a later start) fails `ObjectTimestampError` at the re-check, skips and is
repaired. The same order at a commit voter is Q-W1. The argument stops at a `DoesNotExist` or blind Insert whose
store re-applies it after a later delete of its key (Q-W3).

### 9.4 Deletion and allocation

Deletion is a `RefcountUpdate` to count 0. `Frontend.commit` sees the deletion, calls `discard` (the referenced state is
marked `reloadRequired`, the cache entry dropped) and counts it in `deleting`; reads, snapshots and transaction loads
answer `ObjectNotFound` meanwhile. Transactions referencing the old state fail `StaleObjectState`, later ones
`MissingObject`, and a transaction built on the deletion waits for it in flight (R9) and then fails to load. A failed
deletion keeps answering `ObjectNotFound` while the store is loaded. A deletion's backend commit waits until no
allocation's CRL state durably holds the object's update (`awaitAllocationsDurable`, `Tx.persistWithoutUpdateOf`):
otherwise a recovered allocating vote would recreate it (transaction-recovery.md).

An allocation is read like any other object (`Frontend.readObjectForTransaction`): the in-memory state, `ObjectNotFound`
while a deletion is in flight, else the backend. Only a backend miss gives the fresh `Allocating` state
(`Tx.allocationLoaded`), retained unshared, so only its Tx sees it. An existing state goes into `Tx.allocatedOver` and
every requirement on that object fails `ObjectExists`, at vote and at the apply re-check, so a replayed allocation
never overwrites a live object. (The requirements alone would not catch it: a KV allocation is a revision-less
`KeyValueUpdate` plus `RefcountUpdate(Allocating -> ..)`.) `appliedBy` counts an allocated object at the transaction's
own revision as applied, which covers a recovered allocating vote whose commit persisted before the crash. Each
allocation costs one backend read per store. No backend keeps tombstones, so an id whose deletion has persisted can be
allocated again by a replay that reaches a store at all; the transaction horizon keeps replays to stores that never saw
the transaction (paxos-transactions.md, horizon).

While a store holds a commit vote on an allocation it has not applied (`Tx.allocationUnapplied`), a client read that
misses in the backend is parked and answered once the allocation applies or aborts, or `AllocationPending` at
`ParkedReadWait` (`Frontend.allocationPending`, `parkAllocationRead`), never `ObjectNotFound`. So a read straight after
an allocation commits cannot prove the object deleted: at most `w - t` stores answer `ObjectNotFound`.

## 10. Failure scenarios

### 10.1 Concurrent conflicting transactions

A and B both require revision R of object o. At every store the first checked takes X; the second collides (or, if
built on the first, waits). With `2t > w` at most one can gather `t` commit votes; the other aborts when its abort
votes exceed `w - t`, or expires. A store that voted abort for the winner (it saw the loser first) still applies the
winner, ignoring the loser's lock, and the loser then fails its revision check there. Safe by R1, R2, R4.

Variants: exclusive vs shared (S holders block the writer, which opens the preference window); two key-level writers on
different keys (both commit) or the same key (collide); `WithinRange` vs `SetMin` (collide through R);
`FullContentLock` vs a value writer (collide through C or K); `DoesNotExist(k)` vs `Insert(k)` (collide through N).

### 10.2 Partial vote arrival

The driver and the client act on the votes in hand (section 7). Missing votes only postpone a decision. A store that
withholds its vote (waiting for data, a client prepare, a collision or in-flight commits) is a missing reply; Paxos
allows no reply and drivers retransmit. A Tx that has seen only driver prepares withholds its promise and vote until a
client prepare arrives, and after `Frontend.MissingDataWait` (5 s from its creation) votes abort with `NoClientPrepare`
without checking (paxos-transactions.md, prefer-abort). An abort vote is always sound: R2 and the lock argument rest
on checked commit votes only.

### 10.3 Stale stores voting

*Behind on a previous commit A.* (a) S voted commit for A: it holds A's locks, so B collides; B built on A waits up to
about 1.5 s and then checks the post-A state. (b) S voted abort for A or never saw it: its state is pre-A. B built on A
fails `RevisionMismatch`. B built on the pre-A revision (a stale client) may get S's commit vote, but at least `t`
stores voted for A and either hold A's locks or applied it, leaving at most `w - t < t` stale stores. B cannot commit.

*Blind requirements.* A requirement that does not name the previous value (a blind Insert, `MayExist`, `WithinRange`)
passes on stale and fresh stores alike; its meaning is the application's.

*An update awaiting repair.* A store with a pending ErrorTree entry is stale in the same sense. Repair installs only
what the store still shows as at the snapshot, deferring locked keys and objects with a commit in flight
(repair-process.md).

### 10.4 Store crashes while holding locks

Locks are in memory, the commit vote is in the device CRL. A crash before the CRL save completes loses both the lock
and the vote, which was never sent. After a restart, transaction messages are held (`Frontend.holding`: CRL not yet
read, recovered transactions still loading, or undecided recovered ones not yet completed), and client reads are
dropped meanwhile. Recovered transactions are registered before any read starts (`recoverTransactions`). Each with a
persisted vote completes its load as soon as its objects load, re-taking a commit vote's locks without a check
(`completeLoad` -> `lock()`); recovered transactions without a vote complete only after every recovered vote has
(`recoveredTransactionLoaded`). A recovered commit vote whose outcome is unknown looks up its UnackedCommits entry and
holds any abort until a later lookup finds none (transaction-recovery.md). If the device CRL is lost, every store of the
device is rebuilt and its votes are lost (systemic-loss-protection.md, rebuild-process.md).

### 10.5 Recovered transactions re-acquiring locks

(a) Two recovered commit votes that conflict can coexist: A committed, applied and persisted; B, built on A, voted
after A's commit persisted (R9); A's CRL state stays until its `TxFinalized`. Both re-take their locks in load-completion order, which is not controlled (CRL record order
is no guide either: a re-save of A moves it). Whichever takes a shared slot second displaces the first, which keeps
its hold (section 3), so A's release at its resolution leaves B locked, whichever order they loaded in. A's re-check
finds its update already applied (`appliedBy`), so it neither applies nor quarantines, and no Insert of A replaces
a key's `ValueState` (which would drop a K lock) under B.

(b) A recovered commit vote whose object data the CRL no longer holds (a re-save after `persistWithoutUpdateOf`) has a
missing update: `MissingObjectUpdate` at the re-check, and the allocation rule of 8.5 applies.

(c) A recovered commit vote re-applies its update when its outcome is learned. Revision, refcount and `KeyRevision`
requirements fail on a store that applied a later chained write, which quarantines the object until repair
verifies it. A blind Insert re-applies harmlessly. A `DoesNotExist` or blind Insert after a later delete of the key
re-applies: Q-W3.

(d) A recovered vote whose saved status is resolved (a prepare or accept after the resolution re-saved it) restores the
resolution, takes no locks (R13) and applies the commit or deletes the abort's state once loaded.

### 10.6 Failed backend commit

See 8.3 step 4 and R10. The store may be one of the `t` commit voters, so the committed state exists in memory and in
the CRL but not in the backend until reload; on reload the Tx is recovered from the CRL and re-applies. New votes on
the object are aborts and it keeps serving the committed state, so it neither reverts nor lets a conflicting commit in.
A repair write that fails discards the in-memory state (`repairWriteComplete`), so it is reloaded from the backend;
repair writes are not votes and do not enter `failedCommits`.

### 10.7 Key deleted, then a stale insert

(a) The insert carries `KeyRevision(k, r)` or `Exists(k)`: stores that applied the delete fail it (`KeyExistenceError`),
the stale ones (at most `w - t`) pass. Abort. (b) `DoesNotExist(k)` with an old view: the fresh stores pass, stores
still holding the key fail, and the commit needs `t` fresh stores: correct. (c) A blind Insert resurrects the key by
design, on every store alike. (d) A delete's commit reaching a lagging store before the insert it is built on: its
`KeyRevision` fails there, the store skips and reports it, then applies the insert: repaired. (e) Re-apply of an insert
from a recovered CRL record after a later delete: Q-W3.

### 10.8 Lock-deferral and holder lifetime

On `CollisionWait` expiry a still-held lock is an abort vote and the client retries. No lock is ever force-broken. A
holder that never resolves (dead client, dead driver) is taken over by a recovery driver once its Tx has been idle for
`txHeartbeatTimeout` (5 heartbeats, `Store.heartbeat`), and ends by expiry past the horizon plus the expiry margin
(transaction-recovery.md), which releases its locks (`Tx.expire`). Repair has its own lock deferrals
(repair-process.md).

### 10.9 Other situations

* **Commit decision before the data:** the store holds the commit until the data, a client prepare, or `TxFinalized`
  arrives, then applies and sends `TxCommitted` (`commitAwaitsData`). Adopted data is saved to the CRL first.
* **Commit while another commit of the object is in flight:** held and queued (R9). A `TxFinalized` for a held commit
  defers concluding the CRL state until the commit is issued (`finalizeDeferred`, `Frontend.finalizationDeferred`).
* **Prepare after finalization:** ignored when the host's status cache holds T finalized.
* **Prepare for a transaction the store does not hold:** refused outright below the forget floor (no Tx, no reply);
  above it a Tx is created, muted if T started before a rebuilt store's `acceptorSince` or too far ahead of the
  host's clock. A cached resolved outcome makes the new Tx take no locks (`outcomeDecided`). See
  paxos-transactions.md (horizon) and `server/store/CLAUDE.md`.
* **Proofs:** a memoryless Tx whose objects already show the update sends a proof instead of an abort vote
  (`Tx.proveCommitted`, paxos-transactions.md).
* **An object that fails to load** is absent from `objects`: a fresh vote fails `MissingObject`.
* **A frozen Tx** (past expiry by the store's clock) decides no vote and sends no delayed prepare response.

## 11. Answer: `ExclusiveRevisionLock` is exclusive

It goes through `getState(oid)` (whole object), the same path as `DataUpdate`, `VersionBump` and `RefcountUpdate`:
it collides with any other X holder, any S holder, and every part lock (K, N, C, R). It takes X
(`lockExclusive`). It applies nothing and is not in `changedObjects`, so it never starts a backend commit. The builder
throws if the object is also updated or shared-locked. Unlike `VersionBump` it leaves the revision unchanged, so a
later transaction can still require that revision. `SharedRevisionLock` is the non-exclusive sibling. Repair's
object-level proof is an `ExclusiveRevisionLock` (`server/repair/RepairProofs.proveObjectLevel`).

## 12. Accepted limitations

* Exclusive-preference window residual (section 4): a writer that aborts elsewhere still makes shared lockers collide
  for up to 250 ms.
* Per-write TKVL checks turn concurrent puts of one key into an abort and retry (randomized exponential backoff), and
  a stale cached node costs an extra abort.
* A hung backend commit stalls the object's transactions at that store (Backlog.txt, hung devices).
* Every allocation costs one backend read per store (9.4).
* A deleted id can be re-allocated by a replay at a store that never saw the transaction; no backend keeps
  tombstones (9.4).
* A `TxPrepare.fromClient` or `replaySafe` claim forged by a non-client component is outside the trust model.

## Open weaknesses

### Q-W1. A live commit voter fail-stops when a later, non-conflicting commit is applied first (CONFIRMED; systemic loss, spurious lost-store declaration)

Mechanism: `Tx.issueCommits` fail-stops the store with `INVARIANT VIOLATION` whenever a live commit voter's apply
re-check skips any object (`unapplied = result.skipped -- alreadyApplied -- notApplied`), over every skipped object,
lock-only ones included (unlike the recovered branch, which filters to `changed`). The re-check
(`RequirementsChecker.check(applying = true)`) re-evaluates the voter's requirements against the store's current
state, including `checkRevision`'s object-timestamp test. A committed transaction applies over other transactions'
locks (`applying` ignores them) and is not held behind an unresolved lock holder (`Tx.holdForTurn` looks only at
`commitInFlight`). So a later transaction T, committed by other stores after an earlier transaction U committed and
released its locks there, can be applied at a store S that still holds U's locks (U's resolution not yet received),
and U's re-check then fails at S although no invariant was violated: T was voted on post-U state elsewhere and does
not depend on U.

Trigger / scenario (`w = 3, t = 2`, stores S, A, B): U holds a lock on object O at S and A (for example a vdisk block
commit's `SharedRevisionLock` on the fence, `vdisk/BlockCommitter`; an `AllocationFinalizationAction`'s
`SharedRevisionLock` on the new object; a repair proof's `ExclusiveRevisionLock`; or a `DataUpdate` while T is a
`RefcountUpdate`). U commits; A applies U and releases. T, which does not build on U (a fence takeover bumping the
fence at the same revision; the allocator's follow-up `DataUpdate` at the allocation revision; a writer after a repair
proof; a refcount change), votes commit at A and B. At S it collides with U's lock and votes abort (`buildsOn` is false,
so no wait). T commits. U's `TxResolved` to S is delayed or lost (S learns U's outcome later from `TxFinalized` or a
recovery driver), while T's arrives: S applies T (revision or object timestamp moves past U's). U's commit then reaches
S: its `SharedRevisionLock`/`ExclusiveRevisionLock`/`DataUpdate` re-check fails `RevisionMismatch` or
`ObjectTimestampError`, and S fail-stops. A deletion T (refcount 0) gives `MissingObject` the same way. The cases are
common precisely when a store is slow or partitioned: vdisk takeover after a writer's network trouble, and repair,
which runs because a store missed messages.

Consequence: a healthy store is declared lost and rebuilt. Up to `w - t` stores of a pool can be hit by one (U, T)
pair (the stores holding U's lock that voted abort on T), which for `w = 5, t = 3` leaves the pool at its write
threshold during the rebuilds. The final state needs no fail-stop: for a lock-only object there is nothing to apply; a
changed object would only need repair. (FrontendSuite "a commit held behind an in-flight commit ..." had to switch
to a non-conflicting fence write when the fail-stop was introduced, which suggests the same shape.)

Suggested fix: never fail-stop on a lock-only object (it has no update to lose; skip it as the recovered branch does).
For a changed object of a live voter, quarantine it as for a recovered voter (8.5) instead of fail-stopping, at least
when the object was applied over this voter's lock while it held it (record the overriding txid at apply time in the
rare `applying`-over-a-lock case, which costs nothing on the hot path). Keep the fail-stop for an apply that throws.
Add a FrontendSuite test for each trigger above (deterministic: deliver T's `TxResolved` to S before U's).

Effort: M   Priority: 3

### Q-W3. A recovered `DoesNotExist` or blind Insert re-applies after a later delete of its key (PLAUSIBLE; resurrection, permanent replica divergence)

Mechanism: `SimpleCRL.dropTransactionObjectData` only flags the in-memory record (`crl/CLAUDE.md`), so a committed
transaction's object data stays durable until its CRL state is deleted at `TxFinalized`. A later transaction on the
object may vote and commit once the first's backend commit persisted (R9), before the first is finalized. After a
crash (or a store transfer, which moves the CRL state with `readStoreState`), the first is recovered as a commit vote
and, if its peers have not yet finalized it, learns a non-late commit and re-applies through `issueCommits`. Its
re-check passes where its key requirement is `DoesNotExist(k)` (or there is none) and a later transaction deleted k:
k is absent again. `insertWins(None, ..)` is true, so the Insert writes k back. `appliedBy` finds no trace (k is gone),
but nothing consults it because the re-check passed, so the store reports success in `TxCommitted` and nothing is
marked. `Tx.persistWithoutUpdateOf` closes the same hole for a deleted allocated object, not for a deleted key.

Trigger / scenario: T1 inserts a new key k with the TKVL default (`PutRequirement.Checked` gives `DoesNotExist(k)` for
an absent key), for example the allocate-and-insert pattern whose `AllocationFinalizationAction` lengthens T1's
finalization. T2 deletes k (`KeyRevision(k, T1)`) and is applied, persisted and finalized at store S while T1's
finalization is still running. S crashes, or is transferred, and recovers before T1's `TxFinalized` is sent. T1's
recovered vote is driven, learns commit from its peers' status caches, and re-inserts k at S only. If T2's state is also
still in the CRL and re-applies after T1, the stores converge; if T2 was finalized first, they do not. A second path
needs no crash: a replayed prepare of T1 at a store that never saw it votes commit (k is absent there too) and, given
a late commit, applies T1 through an UnackedCommits entry naming no incarnation (transaction-recovery.md section 7),
after repair has already brought that store to T2's state.

Consequence: S alone holds a deleted key, with no ErrorTree or UnackedCommits entry: permanent divergence of one
replica until a later write of k fails its check at S and gets S marked. Reads use a majority and are unaffected, but
S's votes on k are wrong (a `DoesNotExist(k)` insert votes abort there), and a second such event at another store
would make a read return the resurrected key.

Suggested fix: on the recovery path only, have a recovered commit vote whose changed KV object's timestamp is later
than T's start (some later transaction applied after it, which R9 allows only after T's own commit persisted, or over
its lock) quarantine the object instead of applying, so repair verifies it; or record durably, when a key Delete is
applied while an unfinalized Tx's CRL state holds an Insert of that key, that the Insert must not re-apply (the
`persistWithoutUpdateOf` analog). Add a FrontendSuite test: commit T1 (`DoesNotExist` insert), commit and finalize
T2 (delete), restart the store with T1's CRL record, resolve T1 committed, expect the key absent or the object
quarantined.

Effort: M   Priority: 2
