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
apply re-check (section 8.5).

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
8. `checkKVSliceSize`: the update's growth (`KVObjectState.growth`, which uses the same last-writer-wins rule as
   apply) plus the growth the commit votes holding locks on the object have reserved and not yet applied, against the
   KeyValue limit (`Frontend.maxKVSliceSize`, R16).

| Item | Check | Lock taken | Notes |
|---|---|---|---|
| `requiredRevision = Some(r)` | revision == r, object timestamp <= tx timestamp | X | the only way a KV update moves the object revision |
| `contentLock = Some(FullContentLock(keys))` | set of (key, revision) equals the object's content (`ContentMismatch`); no other holder of C or of any present key | C (`kvs.contentLocked`) | does not lock each value; other value writers collide through C |
| `KeyRevision(k, rev)` | key present (`KeyExistenceError`), no other K on it and no other C, revision == rev, key timestamp <= tx timestamp (`KeyTimestampError`) | K on k | |
| `KeyObjectRevision(k, rev)` | object revision == rev; no timestamp check | X | whole-object |
| `WithinRange(k, ordering)` | k within `[min, max]` (`WithinRangeError`) | R (`kvs.rangeLocks`, a set: many holders) | checks no key lock |
| `Exists(k)` | key present, no other K, no other C | K | no production caller |
| `MayExist(k)` | present: no other K or C; absent: no other N (C through `getState`) | K if present, else N | no production caller |
| `DoesNotExist(k)` | absent (`KeyExistenceError` if present), no other N | N (`absentKeyLocks(k)`) | |
| `TimestampEquals/LessThan/GreaterThan(k, ts)` | present, key timestamp ==, <, > ts, no other K or C | K | no production caller |
| `Insert(k, v)` | written-key lock check (step 7); needs a check on k, a required revision or a content lock (step 3) | `RequirementsLocker.plannedLocks` from `KVWrites.keys`: K if present, else N | |
| `Delete(k)` | as Insert; needs `deletesKeys` and a `KeyRevision(k, _)` or a content lock (step 3) | as Insert | leaves no trace |
| `SetMin/SetMax/SetLeft/SetRight`, `DeleteMin/Max/Left/Right` | `objectLevel`, so whole-object; needs a required revision (step 3) | X (`plannedLocks`, `KVWrites.objectLevel`) | |

`KVWrites.from` decodes each KV update's writes in `Tx.lock` and keeps them in `Tx.lockedWrites`, so `unlock` releases
exactly what `lock` took even if the data is later dropped. An undecodable update is skipped there (its check already
failed). Keys compare by content (`common/objects/Key`). `unlock` releases a lock only if this transaction still holds
it: an applied Delete removes a key with any lock on it, and another transaction may have locked the key since. An
applied Insert keeps the lock of the `ValueState` it replaces.

### 2.3 How a KV update is applied, and what it leaves

`RequirementsApplyer.apply`: object timestamp := max(current, tx timestamp) for every KV update; revision := txid only
with a `requiredRevision`. Operations apply in order: `Insert` replaces the key's `ValueState` (revision txid, timestamp
tx, unlocked) only when it wins `KVObjectState.insertWins` (later transaction timestamp, ties broken by revision UUID;
equal is the same transaction re-applying), otherwise it is a no-op, not a skip; `Delete` removes the key; the eight
object-level operations set or clear `min/max/left/right`. Then `state.data := kvs.encode()`.

Last-writer-wins makes Inserts of one key checked only for presence (`Exists`, the timestamp requirements) converge
whatever order a store applies them in; such an Insert passes its check over a later write of its key and is then a
no-op. An Insert checked by `KeyRevision`, `DoesNotExist`, a content lock or a required revision always wins in practice: its check
rejects a key written later, and the client's HLC places it after the state it read. Deletes and object-level fields
carry no timestamp, so they cannot be ordered this way; that is why unchecked ones are refused (section 2.5).

Traces (`appliedBy`): object revision == txid for `DataUpdate`, `VersionBump` and a KV update with a required
revision; for a KV update without one, any current key carrying revision txid; an allocated object (`allocatedOver`)
at revision txid. Traceless: `RefcountUpdate`, the locks, a KV update without a required revision whose operations
are all key Deletes, and one whose Inserts were all superseded (an object-level change always carries a required
revision outside an allocation's initial content, 2.5, so it leaves the revision as its trace). The UnackedCommits fallback does not depend on traces: the
finalizer writes an entry for every counted commit voter that did not acknowledge, whatever its change
(`client/internal/transaction/MissedUpdateFinalizationAction`).

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
(`MultipleDataUpdatesToObject`, through `claimForUpdate`), a second refcount update
(`MultipleRefcountUpdatesToObject`), a lock on an object it gives a content update (`DataUpdate`, `KeyValueUpdate`,
`VersionBump`), a second shared lock or a shared/exclusive mix (`ConflictingRequirements`), a blind KV write (an
unchecked Insert, an unchecked Delete or an unrevisioned object-level change; `refuseBlindWrites`,
`BlindKeyValueWrite`, even when replay-safe), a `RefcountUpdate` that does not advance the update serial
(`RefcountSerialNotAdvanced`, below), and an unpinned transaction not marked `replaySafe(reason)`
(`UnpinnedTransaction`). The blind-write rule is one function, `common/transaction/BlindWrites.violation`, which the
builder and the stores' `RequirementsChecker.refuseBlindWrites` share. A
`RefcountUpdate` may accompany a content update of the same object (allocation does this) or a lock on it: both take
X for the same transaction, and a transaction never collides with itself.

**Pin rule** (`common/transaction/Pins`): a transaction must carry a write that invalidates its own requirement once
applied: a `DataUpdate` at a required revision other than `Allocating`, a `VersionBump`, a KV update with a required
revision, a `KeyRevision(k, _)` on a key the same update Inserts or Deletes, or a `RefcountUpdate` whose new refcount
differs from the required one. The refcount pin relies on `ObjectRefcount.updateSerial` never returning to an earlier
value, and both the builder (`RefcountSerialNotAdvanced`) and the stores refuse a `RefcountUpdate` whose new serial is
not above the required one (`RefcountUpdate.advancesSerial`), except an allocation of the object (from `Allocating`)
and one that sets the refcount it requires (a repair proof's rc -> rc, which writes no new value).
`ObjectRefcount.Allocating` equals `ObjectRefcount.Default` (serial 0, count 1), so the `RefcountUpdate` the builder
adds for an allocation is itself an rc -> rc and no pin. Production callers
derive the new refcount with `update`, `increment`, `decrement` or `setCount`, which add one to the serial, or name a
higher serial (the amoebafs unlink). The builder records the key pin in the description as
`KeyValueUpdate.pinned` (`Pins.writesCheckedKey`) and the client's claim as `TransactionDescription.replaySafe`.

**Store-side validation.** Stores do not trust the builder. Before checking requirements, a client-prepared Tx runs
`DescriptionValidator.validate` once (`Tx.invalidDescription`, lazily, at vote only); a refusal is the non-object error
`InvalidDescription` (whole vote abort, never waited out) and is logged at warn:

* the store's own pool entry in `poolIDAMap` must equal its pool's IDA (`StoreConfig.ida`, passed as
  `Frontend.poolIDA`; `None` skips this, tests only), and every referenced pool must have an entry. Each pool's stores
  check their own entry, so a lowered `writeThreshold` for any pool draws abort votes from every honest store of
  that pool and cannot commit;
* `primaryObjectIDA` is the primary pool's entry, the primary object is referenced, no pool named in `poolIDAMap`
  tolerates more failures than the primary's (the builder picks the most tolerant), and the designated leader index is
  below the primary pool's width;
* per object at most one content update and one `RefcountUpdate`, no lock beside a content update (a lock beside a
  `RefcountUpdate` passes, as in the builder), no shared lock beside another lock (duplicate `ExclusiveRevisionLock`s,
  harmless, are allowed as the builder allows them);
* every `RefcountUpdate` advances the update serial, unless the description allocates its object or it keeps the
  required refcount (`RefcountUpdate.advancesSerial`), so a recovered vote's passing refcount re-check means its update
  is not applied (10.5(c));
* the pin rule from the description alone (`Pins.declared`, taking `pinned` as the key pin), unless `replaySafe`.

The stores that hold a `pinned` object check the claim against the operations (`FalsePin`); a store that does not hold
the object relies on the flag, which is sound because a false flag draws abort votes on that object from every honest
store of its pool. The blind-write rule is enforced at every check, vote and apply re-check (`refuseBlindWrites`):
outside an allocated object's initial content, a key Delete needs a `KeyRevision` on that key or a content lock, an
object-level change needs a required revision (a content lock does not move the revision that a repair proof's
`SharedRevisionLock` locks), and a key Insert needs a required revision, a content lock, or a requirement that
checks its key's state (`KeyValueUpdate.checkedKeys`: `KeyRevision`, `Exists`, `DoesNotExist`, the timestamp
requirements; not `MayExist`, `WithinRange` or `KeyObjectRevision`). A required revision does not cover a Delete: an
Insert moves no revision, so a Delete checked only by it passes its re-check at a store that applied an Insert of its
key first, and removes the key unreported (R15, 9.3). A `KeyRevision` or a content lock fails there once the Insert
changed the key. An Insert that checks nothing of its key would pass at a store that applied a later Delete of the key
first, and be a no-op (superseded) at one that applied a later write of the key first, so the two orders diverge with
nothing to report (R15). With its key checked, the Insert fails after the Delete, or holds the key's lock so that the
later write comes after it. In this document, as in the code (`Frontend.blindInserts`), a *blind* Insert means a
key-level one, in a `KeyValueUpdate` without a required revision: it is checked, but moves no object revision.

The validation runs at vote only. It depends on the description and the store's pool alone, so a committed
transaction passed it at a write threshold of every pool's stores. The `replaySafe` bit is the one claim stores must
trust: no store can prove a replay harmless. A malicious client is outside the trust model.

TKVL planners derive per-write checks (`client/tkvl/KeyValueListNode.putRequirements` / `deleteRequirements`): a put
gets `KeyRevision(k, rev)` for a key the node read holds and `DoesNotExist(k)` otherwise (`PutRequirement.Checked`, the
default), or exactly `DoesNotExist` (`MustNotExist`) or `KeyRevision(k, rev)` (`AtRevision`); a delete gets the key's
`KeyRevision`. There is no unchecked put. `MissedUpdateMarker` writes conditional checked inserts
(`TKVLInsert.replaces`).

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
| Key-level `Insert` checked only for presence | K on the key | X, S, C, same-key K/N | last writer wins on (timestamp, revision) |
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
object whose backend commit failed, or that a rebuilt store's inherited votes may lock, gets an abort vote
(`BackendCommitFailed`, `InheritedVoteLock`).

**R6 (apply is idempotent where requirements pin a version).** Applying re-checks the requirements; a failing object
is skipped and reported in `TxCommitted.objectCommitErrors`, which makes the finalizer mark the store for repair.
Revision, refcount and checked key requirements make a transaction apply at most once per store and never over a
later chained write. A blind Insert re-applies harmlessly (last writer wins). A recovered commit vote whose re-check
fails holds the update already (8.5). A recovered commit vote's Insert of a key now absent applies: a key's deletion
reaches the backend only once no CRL state holding a blind Insert of it remains (9.4), so the Insert was never
applied (10.5c).

**R7 (no divergence between replicas).** Committed transactions not ordered by R1 either touch disjoint keys and no
common object-level field (K/N/X locks at the common store), or are Inserts of one key checked only for its presence
(last writer wins), so
their application order does not change the result, the slice-size check included, which each vote makes against
the growth of every vote held beside it (R16). The object timestamp is the max of every applied transaction's,
so it is order-independent too. A store that applies them in an order its re-check rejects skips and reports, and
repair restores it, provided a read threshold of stores applied the skipped update. A commit voter of T never applies in
such an order: a committed transaction that holds no locks at a store waits to apply while a transaction there holds
locks it collides with on an object both change (8.3, lock order). So only stores that did not vote commit for T skip it
(at most `w - t`), and T's `t >= r` voters hold it.

**R8 (lock lifetime).** Locks are released when: the Tx resolves abort; the Tx resolves commit and its commit is not
held (`doCommit` releases them before applying); a held commit is finally issued; the Tx expires (`Tx.expire`) or is
forgotten without resolution (`releaseObjects`). They are not held across the backend write: followers are ordered
by the in-flight marker (R9). Every release answers the reads parked on the objects (`Frontend.answerParkedReads`),
after the update is applied in memory.

**R9 (one in-flight commit per object).** A transaction touching an object with a transaction or repair commit in
flight, a held commit queued, or a committed transaction waiting to apply for a lock holder (8.3), decides no vote and
holds no locks until it persists or applies. A committed transaction does not issue its commit while an earlier commit
of any object it changes is in flight or queued ahead of it.

**R15 (a commit voter applies in its lock order).** A committed transaction holding no locks at a store does not apply
while another transaction there holds locks its requirements or writes on an object it changes would collide with
(section 4), when that holder changes one of the objects it changes (8.3). If both commit, a store in both write
quorums ordered them. Where that store voted for the holder first, it held the holder's lock until it applied and
only then voted for the newcomer, so the holder precedes it; applying the newcomer first would let it break the
holder's re-check and leave a counted commit vote standing for neither its lock nor its update. The common store
may instead have voted for the newcomer first (the holder's prepare reached it only after the newcomer applied
there, while it reached this store first): the newcomer then precedes the holder, and the wait applies them here in
the reverse order. That is harmless when the newcomer's apply re-check fails after the holder's update (skipped and
reported at a store that did not vote for it, a repair pass) or the two commute. Every key Delete carries a
`KeyRevision` on its key or a content lock, and every Insert a check on its key (2.5). So a holder's Insert changes
its key here: a write of the key that would supersede it waits for it, since the holder holds the key's lock, and the
newcomer Delete's check then fails after it and the Delete is skipped and reported. With the Delete as the holder, the
newcomer Insert's check fails after it (`KeyRevision`, `Exists`) or, a `DoesNotExist`, passes and writes the key back
here, which is reported where the Insert did not vote commit (9.4). At recovery every recovered commit vote relocks, an unresolved one and one whose committed
resolution was restored alike (R13), and a Tx that does not relock (an abort vote, no vote, a restored resolution
without a commit vote) completes its load only after every relocking commit vote has relocked, so it never misses
such a holder (transaction-recovery.md 7.2).

**R16 (commit votes held together fit the slice limit together).** A commit vote whose KeyValue update grows its
object (`SliceGrowth.grows`: it adds bytes or entries) reserves that growth on the object with its locks
(`ObjectState.sliceReservations`, computed by `RequirementsChecker.sliceGrowth` in `Tx.lock` and taken by
`RequirementsLocker.lock`, released by `unlock`; live and recovered votes alike, only when the store has a slice limit,
`Frontend.maxSliceSize`). A Tx that takes no locks reserves nothing; a recovered commit vote whose committed
resolution was restored relocks, and reserves, until it applies (R13).
Compatible K/N locks let several votes on disjoint keys hold at once, and their growths add up, since they change
disjoint entries. So the size check (`RequirementsChecker.checkKVSliceSize`, at vote and at the apply re-check) fails a
growing update unless the worst state any apply order reaches fits: the current slice plus every positive growth
reserved by another transaction plus its own, with the entry-count varint taken at the summed count. Invariant: the
current slice plus every reservation fits. A growing vote is admitted only if it holds afterwards; an apply moves its
reservation into the slice, at no more than the reserved size, and a release drops it, so neither breaks it. A repair
install, the one other change to a store's slice, keeps it too: a plan that grows a slice holding reservations is
measured the same way and, if it does not fit, writes nothing and answers stale (repair-process.md 6.2, M6a). So a
commit voter's apply re-check never fails on size. An update that grows nothing passes beside any reservation, so an
oversized object can always be shrunk; one that removes bytes but adds entries counts as growing. A committed
transaction that holds no reservation here (the store did not vote commit for it, so at most `w - t` such stores) is
checked the same way at apply, and if the reserved votes leave it no room it is skipped and reported, as any
non-voter's failed re-check is (R7): a repair pass, not margin. An abort vote on size beside reservations costs the
client a retry; a TKVL writer then reads the full node and splits it.
The limit these checks use is not `maxSliceSize` but the KeyValue limit
`L = floor(t * (maxSliceSize - 4) / w)` (`IDA.maxKVSliceSize`, `Frontend.maxKVSliceSize`; `t` the write threshold,
`w` the width), and the client's check of a new object's initial content and the TKVL node limit
(`NodeAllocator.nodeSizeLimit`) use it too. A store checks against the slice it holds, which lacks the committed
updates it did not vote for and has not yet applied, so the union of committed updates that a read restores, and that
repair (`Reconcile.planKeyValue`) and rebuild (`RebuildingStore.restore`) install unchecked, can be larger than any
store's slice: three inserts with pairwise-overlapping voter sets, each store voting two, hold three growths that
each store checked only two of. The bound: split the union into its parts (each entry at its latest value, and the
object-level fields), each written by a committed update whose `t` commit voters applied it. At a store's latest
commit vote among the writers of the parts it voted for, every earlier such part is in its slice or reserved (a
voter holds its reservation until it applies, and a later write of that part would itself be the part's writer),
so those parts total at most `L - 1` (the store's count varint is at least a byte). Each part is counted at `t` of
the `w` stores, so the parts total at most `w (L - 1) / t`, and the union's own count varint adds at most 5 bytes:
within `maxSliceSize`. Value slices are the same length at every store (replicated, or equal IDA shards), so the
measure is the same at each. The argument holds whatever messages are late or lost, and needs no ordering between
the voter sets. The cost is capacity: a KeyValue object grows only to `t / w` of the slice (2/3 for Replication(3, 2)).
DataObjects need no such limit: every data update requires the object's revision or matches the current slice
(appends, ranged writes), so committed data updates form a chain each voter checked in full.
The bound counts each store's votes as one copy, which a rebuilt store would break: its new incarnation holds what
its rebuild read, which can lack a growth T its lost incarnation voted commit for and that committed afterwards (the
inherited-vote lock ends at T's resolution, which carries no data; the update arrives only through repair). A
growing vote it granted then would be checked without T while T is counted at the old incarnation. So until a
complete whole-object repair from a snapshot taken after the guard was set (a read after T's commit, which reflects
T), a rebuilt store votes abort (`InheritedGrowthLock`) on any update that grows a KeyValue object its old
incarnation may have voted to grow (`Frontend.inheritedGrowthLock`): the objects of the transactions its first
held-vote round reports, and every KeyValue object with a missed-update entry against it, which covers a T finalized
before the round (cross-document-review.md 2.3). It needs a store loss, a transaction left unresolved across the
rebuild's read and concurrent growth of one object near `L` together; the guard costs nothing elsewhere.

**R10 (a failed backend commit poisons the object).** After a backend commit failure (or `poisonObject`), the object is
in `failedCommits` until the store reloads: every later transaction on it votes abort, and committed transactions do
not apply to it and report it.

**R11 (votes are final).** A disposition is decided only while `Undetermined`; a recovered Tx keeps its persisted
vote (`completeLoad` decides only when it has none).

**R12 (waiting is safe).** A Tx waiting for a lock holder or for in-flight commits holds no locks and gives no vote,
so waits form no cycle and cannot make two transactions both commit.

**R13 (no locks once the outcome is known, except a restored commit voter's until it applies).** `lock()` is skipped
when the outcome is known (`outcomeKnown`, a restored abort, or `txStatus` resolved), with one exception: a recovered
commit vote whose saved status restores Committed (re-saved by a prepare or accept after the resolution, possibly
before its backend commit completed) relocks, locks and slice reservation without a check, as an unresolved
recovered vote does, and holds them until it applies (`doCommit` releases them as for a live voter). Without them a
committed transaction with no vote here could apply first (R15 would find no holder; a recovered one applies nothing,
8.5, but one whose prepare reaches the store after the restart applies), fail the voter's re-check, and the
recovered-voter rule (8.5) would take the skipped object as applied; a new vote could check the state before the
update; and reads and repair would see no lock for an update the store has yet to apply, and take the state it
replaces as current.

**R14 (a commit voter that cannot apply stops counting).** A commit voter has dropped its locks before it applies,
so one that cannot hold the update it voted for must not keep serving as if it had: an apply that threw fail-stops
the store (section 8.5). A failed re-check has no cause within the margin: for a live voter it is an invariant
violation, logged and reported; for a recovered voter it means the update is applied and superseded (8.5).

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
| `Replication(width, t)` | `1 <= width <= IDA.MaxWidth` (127), `width/2 + 1 <= t <= width` | `2t > width` |
| `ReedSolomon(width, k, t)` | `width <= IDA.MaxWidth`, `width/2 + 1 <= k <= width`, `k <= t <= width` | `2t > width` |
| `RandomAccessReedSolomon(width, k, t, ..)` | same | same |

The width bound keeps every store index within the signed-byte pool index of a `StoreId`, so the tallies' `0 until
width` loops address every store of the pool.

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

* `txCommitsInFlight` (an `ObjectCounts`, per object): outstanding backend commits of transactions (deletions
  included) and repair writes (`reconcileSettled`). `deleting` counts the deletions among them, and repair deletes.
* `heldCommits: ObjectId -> List[Tx]`: committed transactions waiting their turn, oldest first.
* `inFlightCommitWaiters: ObjectId -> List[Tx]`: transactions withholding their vote, for a commit in flight or held, or for
  a committed transaction waiting there in lock order (`Tx.lockOrderBlocked`).
* `lockReleaseWaiters: TransactionId -> List[Tx]`, by holder: transactions waiting out a lock holder (8.4,
  `awaitLocksReleased`) and committed transactions waiting for it in lock order (8.3, `Frontend.awaitLockOrder`
  registers them here too); `locksReleased` tells both.
* `hasPendingCommit(oid) = txCommitsInFlight.contains(oid) || heldCommits.contains(oid)`.
* `lockOrderWaiters: ObjectId -> List[Tx]`: committed transactions waiting to apply in lock order (8.3), by object
  they change (`Frontend.lockOrderWaitersOf`). Not part of `hasPendingCommit`.
* `adoptedDataSaves: ObjectId -> List[Tx]`: transactions saving object data adopted from a later prepare
  (`Tx.saveAdoptedData`), by object they change, from the save's issue until it completes and a commit waiting for it
  has applied or moved to one of the lists above. Not part of `hasPendingCommit`; only repair consults it.
* `failedCommits`: never cleared; the store reloads with a new Frontend.
* `allocations` and `blindInserts`, two `CrlStateRecords` (by txid and by object): transactions whose CRL state may
  still hold an allocation or a blind KeyValue update of an object here (9.4).

### 8.3 Behaviour

1. **Vote path** (`Tx.determineDisposition`, in this order): a frozen, unresolved Tx (past expiry) decides nothing; a
   muted Tx votes abort without checking (it proves the commit if an object shows it); a Tx that has seen no client
   prepare votes abort with `NoClientPrepare` without checking (prefer-abort; it is called only once the wait for the
   client's prepare is over or the outcome is known); an invalid description votes `InvalidDescription`; a hosted object
   that is failed (`commitFailed`) or covered by a rebuilt store's inherited votes (`Frontend.inheritedVoteLock`,
   cross-document-review.md 2.3) votes abort, one error per such object; a hosted object with a commit in flight or held
   (`hasPendingCommit`), or whose locks a committed transaction waiting there to apply in lock order would collide with
   (`Tx.lockOrderBlocked`, below), while the outcome is unknown makes the Tx wait (`awaitingInFlightCommits`,
   `Frontend.awaitInFlightCommits`), with no locks and, for a commit in flight, no timer, until `commitPersisted` has
   arrived for each (from `commitTurnFree`, at a backend completion, a held commit's issue, or the end of a lock-order
   wait, `Frontend.lockOrderWaitWakeVoters`); otherwise the requirements are checked, and a collision may be waited out
   (8.4). After the check, a rebuilt store holding inherited growth guards adds `InheritedGrowthLock` for every growing
   KeyValue update of a guarded object (R16). The in-flight test covers every hosted object, lock-only ones included; a
   lock-only object has a commit in flight only when another transaction changes it.
2. **Commit path** (`Tx.resolvedAndAllObjectsLoaded`): if the commit decision arrived before the object data
   (`commitAwaitsData`: no client prepare and data missing, or adopted data not yet saved to the CRL), the commit is
   held, without locks, since a commit vote requires the data. Else if `holdForTurn()` finds a changed object in flight
   or held, the Tx joins every changed object's queue in one `holdCommit` call and keeps its locks and references.
   Queues are appended to together, so the oldest held commit heads every queue it is in, and held commits cannot wait
   on each other in a cycle. Before that, a committed Tx that holds no locks here waits in lock order
   (`Tx.awaitLockOrder`, R15): while another Tx holds locks (`Frontend.holdsLocks`) that its requirements or writes on
   an object it changes collide with (`RequirementsChecker.lockHolders` over `changedHere`, displaced holders included),
   and that holder itself changes one of the objects it changes (`Frontend.changes`), it is held (`commitHeld`, no
   locks, references kept, `Frontend.awaitLockOrder`) until each such holder has released them
   (`Frontend.locksReleased`, after the holder applied or aborted). The holder set is then computed again, and with none
   left it takes the ordinary path, queueing behind the holder's in-flight commit. A lock-only holder is not waited for:
   it has no update to lose. A Tx holding its own locks never waits: two live votes never hold conflicting locks (R4),
   and recovered votes sharing a lock (section 3) are ordered by what persisted before the crash (the later one voted
   after the earlier one's update persisted, 10.5a). A recovered Tx that does not relock completes its load only after
   every recovered commit vote has relocked (transaction-recovery.md 7.2), so it finds every holder it must wait for.
   The wait is not counted by `hasPendingCommit`, so the holder's own commit never queues behind it. A new vote waits for
   it only if the waiter would wait for it as a holder: the vote changes an object the waiter changes, and the waiter's
   requirements or writes collide with the locks the vote would take on one of its objects (`Tx.lockOrderBlocked`:
   `RequirementsLocker.plannedLocks` seen through `RequirementsChecker.PlannedLocks`, tested by the waiter's
   `Tx.wouldWaitFor` with the same `lockHoldersIn` rule and `Frontend.changes` filter as its own holder set), so it
   cannot be starved. A vote that is lock-only on the waiter's objects is not withheld. Any other new vote decides at
   once, on the state before the waiter's apply: what the waiter changes is nothing that vote's requirements or writes
   depend on, as for two votes holding compatible locks (R4). A waiter may itself wait for a holder whose transaction
   can decide only with a vote withheld at another store, by a waiter there that waits for this vote's transaction: four
   contending multi-key writers and two delayed prepares suffice. So a vote is never withheld for a waiter it does not
   collide with, and a colliding one at most `Tx.CollisionWait` (`Tx.checkCollisionWait` at the 1 s store heartbeat, so
   up to about 1.5 s, `lockOrderBlockStart`): it then votes abort, a `TransactionCollision` naming the waiter, on the
   state before the waiter's apply. A Tx whose such wait expired (`lockWaitBlockExpired`) never waits for a lock-order
   waiter again: it votes abort at once if one still collides, and otherwise waits for real in-flight commits as usual.
   An abort vote is always safe, and it ends any such cycle: the holder its transaction would have needed decides
   without it. A holder that never resolves holds the waiter as long as its own locks (section 12).
3. **Issue** (`doCommit`): release the locks, apply (`issueCommits`), answer parked reads, one `Frontend.commit` per
   changed object that applied, then `heldCommitIssued` removes the Tx from the queues and `commitTurnFree` wakes the
   next. A store holding an applied stub for the transaction, owed nothing through its UnackedCommits entry
   (`notOwedHere`), or holding a recovered Tx without a commit vote (8.5), applies nothing and reports its changed
   objects (transaction-recovery.md). That is decided before the waits, by one predicate shared with `doCommit`
   (`Tx.appliesNothing`): such a commit writes nothing a vote or an earlier commit could be run over by, so it neither
   waits in lock order nor for its turn (`awaitLockOrder`, `holdForTurn`), and withholds no vote and unsettles nothing
   for repair meanwhile. One that became such while already waiting leaves the lock-order registration and the queue
   when it is next woken.
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
device's I/O, and detect and handle hung devices", including why there is no per-commit watchdog). A vote withheld
by a colliding committed transaction waiting in lock order is bounded instead (`Tx.CollisionWait`, then an abort
vote): that wait ends only when a holder's transaction resolves at other stores, which may need this vote.

### 8.4 `CollisionWait`: waiting out a holder the transaction is built on

A transaction whose check fails only with collisions against holders it is **built on** (`Tx.buildsOn`: one of its
`DataUpdate`, `VersionBump`, `ExclusiveRevisionLock`, `SharedRevisionLock` or KV `requiredRevision` requirements on that
object requires the revision the holder writes; a key requirement such as `KeyRevision` is never built on) and that
still hold locks here (`Frontend.holdsLocks`: a Tx in `transactions` holding locks, a commit held for its turn
included; one finalized while held has left `transactions`, but the objects it changes stay pending in `heldCommits`, so
a vote on them waits for its commit first) does not vote (`Tx.awaitableHolders`). It
registers with each holder (`awaitLocksReleased`), holds no locks, and decides when the holders release
(`locksReleased`, called at the end of `releaseObjects`, after the commit was issued, so the waiter then sees the commit
in flight and waits for it to persist) or after `Tx.CollisionWait` (500 ms), checked at the 1 s store heartbeat
(`Store.heartbeat` -> `Tx.checkCollisionWait`), so a wait lasts up to about 1.5 s. On expiry the check runs once more
and a remaining collision is an ordinary abort vote; a Tx whose wait expired never waits again (`collisionWaitExpired`).
Recovered Txs, and any non-object error, never wait. A collision raised by the exclusive preference window names the Tx
itself, which holds no locks, so it is never waited out. A collision with a holder the transaction does not build on is
an immediate abort vote. `objectErrors` keeps one error per object, so a later error for an object masks an earlier one;
the re-check after the wait only costs time.

### 8.5 A commit voter that cannot apply: fail-stop, poison

`Tx.issueCommits`, for a commit voter (`disposition == VoteCommit`):

* an apply that threw (`result.failed`) fail-stops the store (`Frontend.failStop`): its in-memory state is corrupt;
* a live voter's skipped changed object, except one already applied (`appliedBy`) or an allocation the backend missed
  (`allocatedHere`), is an invariant violation. A committed write that does not depend on T, a deletion included,
  never applies over T's locks before T: it waits for T's vote in lock order (8.3, R15). So the causes left are a
  state discarded under the locks (`Frontend.discard`, `StaleObjectState`) and two conflicting commits, which the
  quorum overlap excludes (a Tx that decides its vote with the outcome known, from the status cache or a
  resolution that beat its object loads, votes commit without taking locks, so its skip is no violation, Q-W20). A slice-size re-check is no cause: the votes a store holds together fit together (R16). It
  is logged at error (`INVARIANT VIOLATION`), naming the transaction each object now
  shows, then skipped and reported in `objectCommitErrors`, so the finalizer marks the store; no state is kept and
  the store is not fail-stopped. An allocation the backend missed is reported: the store holds no copy, so no lock
  was lost, and every other transaction on it misses and votes abort;
* a recovered voter (recovered from the CRL, whether it learns the outcome by resolution or applies through its
  UnackedCommits entry) takes every skipped changed object as already applied: not reported, logged at info. The vote
  held its locks until it applied and relocked them at recovery, and a Tx that does not relock completes its load only
  after every recovered commit vote has relocked (transaction-recovery.md 7.2), so only T's update and the writes after
  it in lock order changed the object. A failed re-check therefore means the store applied T before the crash: a later
  transaction superseded it (10.5a), the update leaves no trace for `appliedBy` (a `RefcountUpdate`, a KV update whose
  operations are all key Deletes), or its data was dropped from T's CRL state once applied (`MissingObjectUpdate` after
  `Tx.persistWithoutUpdateOf`: an allocation then a deletion, or a blind Insert then a deletion of its key, 9.4;
  `FalsePin` instead for a pinned KeyValue update, whose pin check finds no operations first). The
  rule does not filter by reason. It holds for a voter whose committed resolution was restored too: it relocks until it
  applies (R13). Proofs never use this rule: a store that never voted on T has no such bound, and proofs see only
  `appliedBy`. A recovered vote whose object is not found (`ObjectNotFound`) applies nothing and reports it;
* a skipped lock-only object has no update to lose: it is skipped, logged and reported, never fail-stopped. The usual
  cause is a later commit applied first over the lock: a fence takeover after a vdisk block
  commit's `SharedRevisionLock`, an allocator's follow-up write after an `AllocationFinalizationAction`'s
  `SharedRevisionLock`. Repair's object-level and key proofs change the object (the object-level proof's
  `RefcountUpdate` beside its `SharedRevisionLock`), so they wait for a vote holding it in lock order like any
  committed write. A rebuilt store votes abort on the
  objects its lost incarnation's votes may back (`InheritedVoteLock`), so a lock lost to a rebuild is not a cause.

A recovered Insert of a key now absent passes its re-check and applies: a key's deletion reaches the backend only once
no CRL state still holding a blind Insert of the key that the store applied remains (9.4), so such an Insert was never
applied (10.5c).

A fail-stopped store drops every transaction message, read, repair and heartbeat, and `Store.onFailStop` has its host
move its copy to `lost/` (when its backend closes) and unload it. Every fail-stop is a lost-store declaration (Backlog.txt, "System-level
protection against correlated lost store declarations"), so on the transaction path only a thrown apply and a failed
load fail-stop (`Frontend.failStop`'s two callers, `Tx.issueCommits` and `Tx.objectLoadFailed`); the FSIF backend's
own fail-stops on I/O failures are outside this document.

Objects whose earlier backend commit failed (`failedHere`) are left out of the apply for every Tx, voter or not, and
reported. Skipped objects of a non-voter are reported only. A recovered Tx that did not vote commit applies nothing on a commit and reports every object it changes
(`Tx.doCommit`): it may have applied the update before the crash as a non-voter and acknowledged it, so T's finalizer
left no mark, and with no vote it held no key lock keeping a later deletion of a key it Inserts out, which nothing in
its CRL state shows (its update is dropped from the state only once it voted commit or applied in this incarnation,
9.4); a re-apply could write that key back with no repair to follow. While T's marker has not written, the report
marks the store and repair installs the object's current state; once T is finalized, the store either acknowledged T
in its earlier life, so its backend holds the update, or the finalizer marked it. A non-voter is not counted, so this
costs no margin.

A failed backend commit keeps the Tx's CRL state, object data included, for the life of the loaded store
(`Tx.keptForReplay`): its `TxCommitted` says so, so the finalizer gives a counted voter an UnackedCommits entry even
though it reported, and a reloaded store's recovered vote, learning the outcome late, re-applies through it rather
than apply nothing.

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
4. Sequential Inserts of one key checked only for presence apply last-writer-wins, so a lagging store or a crash
   re-apply converges. Every other Insert's check fails over a later write of its key.
5. Object timestamps are the max of all applied updates, so they agree once the same set is applied.

A non-voter that applies a revision-chained update after a later unrelated update raised the object's timestamp (a
refcount change or blind Insert with a later start) fails `ObjectTimestampError` at the re-check, skips and is repaired.
A commit voter never applies in that order: the later update waits for its vote in lock order (8.3, R15), so T's `t`
voters hold T and repair restores the at most `w - t` stores that skipped it. A recovered `DoesNotExist` or blind Insert
whose key a later transaction deleted no longer holds its update in its CRL state (9.4), so it never writes the key back
(10.5c). Every key Delete is checked by a `KeyRevision` or a content lock (2.5), so it fails at a store that lacks the Insert
it follows (it checks the key at the Insert's revision), and that store skips and reports it.

### 9.4 Deletion and allocation

Deletion is a `RefcountUpdate` to count 0. `Frontend.commit` sees the deletion, calls `discard` (the referenced state is
marked `reloadRequired`, the cache entry dropped) and counts it in `deleting`; reads, snapshots and transaction loads
answer `ObjectNotFound` meanwhile. Transactions referencing the old state fail `StaleObjectState`, later ones
`MissingObject`, and a transaction built on the deletion waits for it in flight (R9) and then fails to load. A failed
deletion keeps answering `ObjectNotFound` while the store is loaded. A deletion's backend commit waits until no
allocation's CRL state durably holds the object's update (`awaitAllocationsDurable` over `allocations`,
`Tx.persistWithoutUpdateOf`):
otherwise a recovered allocating vote would recreate it (transaction-recovery.md).

A key's deletion is ordered the same way after a blind Insert of the key (`Frontend.awaitBlindInsertsDurable`). The
Frontend indexes, beside the allocation records (`allocations`) and at both Tx creations (`registerReplayHazards`),
every Tx with a `KeyValueUpdate` without a required revision on a hosted object (`blindInserts`, a `CrlStateRecords`
by txid and by object; by shape alone, no decoding), until its CRL state's deletion or stub is durable
(`stateConcluded`, `CrlStateRecords.concluded`); a later Tx of the same transaction replaces the record's Tx. A
transaction commit that
deletes keys of such an object (`Frontend.commit`, whose `deletedKeys` is decoded only when the index covers the
object) and a repair write whose plan removes keys (`reconcileSettled`, `plan.deletes`) reach the backend only after
each other covering transaction T whose update Inserts one of those keys is handled:

* T is concluded (its deletion or stub queued, not known durable): the write waits for `crl.whenDurable`;
* T holds its update of the object here (`Tx.holdsUpdateOf`: it issued its commit of the object, or it voted commit;
  a commit voter holds the key's lock from its vote until it applies, so a key deletion applies before it only as a
  recovered vote sharing the lock, cast after T's update persisted, 10.5a; a Tx that decides its vote with the outcome known votes
  commit without locks, Q-W20): T's state is saved again without the
  object's update (`persistWithoutUpdateOf`, after `CrlStateRecords.dropObject`), and the write waits for that save;
* otherwise (a non-voter that has not applied): nothing waits. Its update is one the store does not hold and may yet
  apply. When the deletion follows T in the serial order (a Delete checked against a later write of the key that
  superseded T's Insert at a store voting for both), that later apply writes the key back at this store alone, and
  reports it (below).

So a recovered commit vote that still holds a blind Insert of an absent key never applied it, and applies it (10.5c).
The cost is one index entry per Tx with a blind KeyValue update, and a decode only for a key deletion soon after a
blind write of the same object; nothing on the ordinary commit path.

A Tx that did not vote commit here held no key lock that kept a later deletion of its key out, so a deletion ordered
after T may apply here first. Its apply of a blind Insert of a key absent before the apply reports the object in
`objectCommitErrors` (`Tx.issueCommits`) when a committed Delete of that key, started at or after T's start, applied
here (`Frontend.keyDeletions`, `keyDeletedSince`): one that removed a value that superseded T's Insert, or a no-op
over the absent key under a content lock (which lists only present keys). A deletion started before T's start is never ordered after T: where T's Insert applied first, the object
timestamp is at least T's start, and the deletion's check requires its own timestamp to be at least that. The record
keeps the latest deleting start per object and key, written by every apply that deletes keys (the keys are decoded
only for an update flagged `deletesKeys`), and is kept while a transaction started before it may still apply here:
above the forget floor, or while any Tx in `transactions` started at or before it (a held Tx applies below the floor
too); it is pruned at `expiryHeartbeat`. A commit held here past its TxFinalized (in lock order or for its turn,
`Frontend.deferredFinalizations`) is not in `transactions` and keeps no record alive, so its apply may miss a pruned
deletion. It needs no report: it was finalized before it applied here, so no TxCommitted reached the finalizer within
`errorTimeout` and the finalizer marked this store, and that mark's repair finds the object unsettled while the
commit is held, queued, or waiting for the save of object data it adopted from a late client prepare
(`settledForRepair`, `Frontend.adoptedDataSaves`, repair-process 8.2) and installs the current state after it
applies. A deletion that reached the store through repair is covered by `repairInstalls` (below).
The finalizer marks the store, and the mark's repair installs the object's current state. A non-voter is not counted
toward the write threshold, so the report costs a repair pass, not margin. A non-voter's Insert of a key no deletion
reached reports nothing, so the routine non-voter of pipelined inserts into one leaf (its vote withheld behind an
in-flight commit, a collision or lock-order wait, or its client prepare) leaves its transaction's finalizer on the
clean fast path. The record is empty at load: the load's floor refuses every transaction started before it, and a
recovered non-voter applies nothing and reports every object it changes.

A store can also hold a key's deletion it never applied: repair installs it from a read (a whole-object reconcile), at a
store that may never have held the Insert either. A transaction T that reaches such a store later (a delayed client
prepare at a store that never saw T, its vote passing T's pin on another object; or a non-voter applying a commit
decided elsewhere) passes its re-check on the absent key and writes it back there alone. Its TxCommitted could reach the
finalizer within `errorTimeout`, leaving no mark, and T's own mark of a store that missed it is repaired only once the
store can no longer join T (repair-process.md 5.4), after any such apply. A reconcile that changes nothing can leave the
same state behind, the store lacking a deletion it never applied as the read does, and still delete the store's mark for
it. So the Frontend records the latest object timestamp each repair write installs, and for a complete reconcile that
changes nothing the entry's minimum timestamp, which no deletion whose mark the entry's delete removes started after
(`RepairSnapshot.minTimestamp`; the bootstrap sweep, deleting no entries, records nothing) (`repairInstalls`, kept
while a transaction started before it may still
join: pruned below the forget floor at `expiryHeartbeat`), and tells every Tx in `transactions` holding the object
started before it, and every Tx created for the object later with an earlier start (`Tx.repairInstalled`). A commit
held past its TxFinalized has left `transactions` and is not told: while it is held the object is unsettled for
repair (`settledForRepair`), so no repair installs under it. An apply of a blind Insert of a key
absent before the apply, on an object so marked, applies and reports the object in `objectCommitErrors`
(`Tx.issueCommits`): the finalizer marks the store, and the mark's repair, once T can no longer join here, installs the
object's current state. A recovered commit vote, whose marks were lost with the crash, reports the same apply when any
write newer than T's start reached the object. The map is empty except soon after a repair, so an ordinary commit voter
checks nothing; a counted voter that reports only adds a repair pass.

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

*Blind requirements.* A requirement that does not name the previous value (`Exists`, `MayExist`, `WithinRange`)
passes on stale and fresh stores alike; its meaning is the application's. An Insert needs more than `MayExist` or
`WithinRange` on its key (2.5).

*An update awaiting repair.* A store with a pending ErrorTree entry is stale in the same sense. Repair installs only
what the store still shows as at the snapshot, deferring locked keys and objects with a commit in flight
(repair-process.md).

### 10.4 Store crashes while holding locks

Locks are in memory, the commit vote is in the device CRL. A crash before the CRL save completes loses both the lock and
the vote, which was never sent. After a restart, transaction messages are held (`Frontend.holding`: CRL not yet read,
recovered transactions still loading, or undecided recovered ones not yet completed), and client reads are dropped
meanwhile. Recovered transactions are registered before any read starts (`recoverTransactions`). Each recovered
commit vote completes its load as soon as its objects load, re-taking its locks without a check (`completeLoad` ->
`lock()`); one whose saved status restores Committed relocks the same way and then applies (R13). Every other
recovered Tx (an abort vote, no vote, a restored abort, a restored commit without a commit vote) completes only after
every relocking recovered commit vote has (`recoveredTransactionLoaded`), those with a persisted vote first, each group
in the order its objects loaded. A recovered commit vote whose outcome is unknown looks up its UnackedCommits entry and
holds any abort until a later lookup finds none (transaction-recovery.md). If the device CRL is lost, every store of the
device is rebuilt and its votes are lost (systemic-loss-protection.md, rebuild-process.md).

### 10.5 Recovered transactions re-acquiring locks

(a) Two recovered commit votes that conflict can coexist: A committed, applied and persisted; B, built on A, voted after
A's commit persisted (R9); A's CRL state stays until its `TxFinalized`. Both re-take their locks in load-completion
order, which is not controlled (CRL record order is no guide either: a re-save of A moves it). Whichever takes a shared
slot second displaces the first, which keeps its hold (section 3), so A's release at its resolution leaves B locked,
whichever order they loaded in. A re-applies only on a non-late outcome or a found UnackedCommits entry; a late commit
with no entry applies nothing (lateCommit, transaction-recovery.md). When it does re-apply, its re-check fails where its
update was chained (a revision, refcount or checked key requirement), and the recovered vote takes it as applied
(8.5): it held its locks until it applied and relocked them at recovery, so only its update and the writes after it in
lock order changed the object, whether or not the update leaves a trace for `appliedBy` (2.3). An Insert checked only
for its key's presence (`Exists`) passes its re-check and applies again: its Insert ties with the
entry it wrote (`insertWins`), and the `ValueState` it installs keeps the key's lock (`RequirementsApplyer.apply`), so B
stays locked. A key Delete is never blind, so its re-check fails once applied and it never removes a key under B.

(b) A recovered commit vote whose object data the CRL no longer holds (a re-save after `persistWithoutUpdateOf`) has a
missing update: `MissingObjectUpdate` at the re-check (`FalsePin` for a pinned KeyValue update, checked first). The
update was applied before it was dropped (9.4), so the recovered vote takes it as applied (8.5). The data stays
missing: a commit voter adopts no object data from a later prepare (`Tx.adoptObjectUpdates`), since it held all its
data when it voted (a vote without it fails `MissingObjectUpdate`), so a client retransmission reaching the recovered
vote with the dropped data does not bring it back. Only a Tx without a commit vote adopts late data.

(c) A recovered commit vote re-applies its update when its outcome is learned. Revision, refcount and `KeyRevision`
requirements fail on a store that applied a later chained write (a refcount because every refcount
change advances `ObjectRefcount.updateSerial`, which the builder and the stores enforce, 2.5, so no later write returns
it to the required value), and the vote takes the object as applied (8.5). An
`Exists`-checked Insert over a present key re-applies harmlessly (last writer wins). A `DoesNotExist` or blind Insert of a key now
absent passes its re-check again, and applies: T never applied it. Had T applied it and a later transaction deleted the
key, the deletion would have reached the backend only once T's CRL state no longer held the update
(`Frontend.awaitBlindInsertsDurable`, 9.4), so the recovered vote would find it missing and take it as applied. This
holds whatever later writes of other keys raised the object's timestamp. The same holds for a recovered vote applying
through its UnackedCommits entry. Where a write newer than T's start reached the object, the apply also reports it,
since the marks of repair installs are lost with the crash (9.4). A live voter, a replay's included, has held the
key's lock since its own check; it reports the apply when a repair installed a state newer than T's start on the
object (9.4).

(d) A recovered vote whose saved status is resolved (a prepare or accept after the resolution re-saved it) restores the
resolution and applies the commit or deletes the abort's state once loaded. A restored commit vote relocks first and
holds its locks until it applies (R13), so a committed transaction without a vote here waits for it in lock order
(R15) and cannot overtake it; a restored commit without a vote here applies nothing at all (8.5); a restored abort
takes no locks.

### 10.6 Failed backend commit

See 8.3 step 4 and R10. The store may be one of the `t` commit voters, so the committed state exists in memory and in
the CRL but not in the backend until reload; on reload the Tx is recovered from the CRL and re-applies. New votes on
the object are aborts and it keeps serving the committed state, so it neither reverts nor lets a conflicting commit in.
A repair write that fails restores the fields it changed to the settled pre-repair state, which is the backend's
(`repairWriteComplete`); the instance stays the one lock holders reference, so their committed apply still holds. A
repair of an object the store did not hold discards its state instead. Repair writes are not votes and do not enter
`failedCommits`.

### 10.7 Key deleted, then a stale insert

(a) The insert carries `KeyRevision(k, r)` or `Exists(k)`: stores that applied the delete fail it (`KeyExistenceError`),
the stale ones (at most `w - t`) pass. Abort. (b) `DoesNotExist(k)` with an old view: the fresh stores pass, stores
still holding the key fail, and the commit needs `t` fresh stores: correct. (c) An Insert with no check on its key is
refused (2.5), so no insert resurrects a key without a check that sees the delete. (d) A delete's commit reaching a lagging store before the insert it is built on: its
`KeyRevision` fails there, the store skips and reports it, then applies the insert: repaired. (e) Re-apply of an insert
from a recovered CRL record (or through an UnackedCommits entry) after a later delete: the delete reached the backend
only once the insert's update was dropped from the CRL state, so the re-apply finds it missing and inserts nothing
(10.5c, 9.4).

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
  host's clock. A cached resolved outcome makes the new Tx take no locks (`outcomeDecided`); with a client prepare it
  still checks its requirements and may vote commit, a vote no outcome rests on (Q-W20). See
  paxos-transactions.md (horizon) and `server/store/CLAUDE.md`.
* **Proofs:** a memoryless Tx whose objects already show the update sends a proof instead of an abort vote
  (`Tx.proveCommitted`, paxos-transactions.md).
* **An object that fails to load** is absent from `objects`: a fresh vote fails `MissingObject`.
* **A frozen Tx** (past expiry by the store's clock) decides no vote and sends no delayed prepare response.

## 11. Answer: `ExclusiveRevisionLock` is exclusive

It goes through `getState(oid)` (whole object), the same path as `DataUpdate`, `VersionBump` and `RefcountUpdate`: it
collides with any other X holder, any S holder, and every part lock (K, N, C, R). It takes X (the object slot of
`RequirementsLocker.plannedLocks`, taken by `take`). It applies nothing and is not in `changedObjects`, so it never
starts a backend commit. The builder throws if the object is also updated or shared-locked. Unlike `VersionBump` it
leaves the revision unchanged, so a later transaction can still require that revision. `SharedRevisionLock` is the
non-exclusive sibling. Repair's object-level proof (`server/repair/RepairProofs.proveObjectLevel`) takes one beside a
`RefcountUpdate(rc -> rc)` of the same object: the update's X lock already excludes every writer, and the shared lock
adds the revision check.

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
* A commit voter whose transaction never resolves (an undecidable expiry tally, logged `UNDECIDABLE TRANSACTION`) keeps
  every committed transaction that waits for it in lock order (8.3) held at its store, with that transaction's CRL
  state and admission, as long as it keeps its own locks. An undecidable tally needs more failures within one
  transaction's lifetime than the write threshold's margin covers, and is alarmed; there is no way out of the wait.

## Open weaknesses

**Q-W20 A Tx that decides its vote with the outcome known votes commit without locks, and is treated as a locking
commit voter.** CONFIRMED, effort S, priority 2. Mechanism: two paths give a `VoteCommit` that `Tx.lock` never locks.
(a) `Frontend.createTx` calls `Tx.outcomeDecided` for a prepare whose transaction the host's status cache holds
resolved but unfinalized; with a client prepare, `Tx.determineDisposition` skips the in-flight wait (`!outcomeKnown`)
and checks the requirements, while `lock` takes nothing (`!outcomeKnown`). (b) A Tx whose `TxResolved` arrives before
its objects load decides its vote in `Tx.completeLoad` with `oresolution` set (the in-flight wait skipped likewise),
then `lock` takes nothing because `txStatus` is no longer `Unresolved`. Either vote is sent (`Tx.sendPrepareResponse`),
but T is already decided, so no outcome rests on it. Later code keys on the disposition alone:
`Tx.checkVoteCommitApplied` treats it as a live voter, and `Tx.holdsUpdateOf` as a Tx whose update is applied. Trigger
(first effect): the Tx votes commit and, before it applies (its resolution not yet delivered, held for its turn, or
waiting in lock order behind a holder that took its locks after the lockless vote, beside a later committed lockless
transaction U, which `Frontend.locksReleased` wakes first, newest waiter first), U applies, nothing ordering it behind
T, and raises the object timestamp, so T's re-check fails (`ObjectTimestampError`). Consequence: a false
`INVARIANT VIOLATION` error, plus the skip-and-report repair pass a non-voter would cost anyway. Trigger (second
effect): a committed D ordered before T that deletes a key T Inserts applies here first, T holding no lock to make it
wait. Every key check tests the key's presence, so the one D whose check passes on the state T voted on and that
commits before T is a Delete of the absent key under a content lock; `Frontend.awaitBlindInsertsDurable` still counts
it (`deletedKeys` is every Delete of the update) and, `holdsUpdateOf` being true, saves T's state without its whole
update of the object (`persistWithoutUpdateOf`). A crash after that save and before T's backend commit is durable
recovers a commit vote without the data, which takes the object as applied (8.5, `recoveredApplied`) and
acknowledges without reporting. Consequence: that store lacks T's whole update of the object, unmarked, if its
recovered `TxCommitted` reaches T's current finalizer before the marker's `errorTimeout` (2 s from that finalizer's
start; a takeover driver's finalizer starts its own), so a permanent replica divergence that costs one unit of `w - t`
until a later write or repair of the object happens to cover it. Only that timing guards it, hence the priority.
Fix direction: a Tx whose outcome is known when it decides gives no commit vote (vote abort unchecked, as for
`NoClientPrepare`, still proving if an object shows the update), so every `VoteCommit` holds its locks until it
applies; that removes both paths.
