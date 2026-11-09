# Transaction Requirements, Locking and the Commit Window

Status: derived from the code at commit `c3ad5493` (2026-11-09). Every mechanism below was read in the source; line
numbers are `path:line` relative to `src/main/scala/org/aspen_ddp/aspen/`. Where `CLAUDE.md` or a scaladoc disagrees
with the code, the code is described and the disagreement is called out.

Sibling specifications (not repeated here): [paxos-transactions.md](paxos-transactions.md),
[crash-recovery-log.md](crash-recovery-log.md), [transaction-recovery.md](transaction-recovery.md),
[repair-process.md](repair-process.md), [rebuild-process.md](rebuild-process.md),
[systemic-loss-protection.md](systemic-loss-protection.md).

## 1. Scope and model

A transaction (`TransactionDescription`, `common/transaction/TransactionDescription.scala:21`) carries a list of
`TransactionRequirement`s, one data blob per updated object per store (`ObjectUpdate`), and the Paxos metadata. Each store
that hosts at least one referenced object receives the whole description and acts only on the objects it hosts
(`Tx.scala:59-61`: object requirements are filtered to `pointersById`, non-object requirements are kept). The store:

1. checks the requirements against its in-memory `ObjectState` (`RequirementsChecker.check`);
2. if all pass, takes locks (`RequirementsLocker.lock`) and persists a **commit vote** in its CRL before the vote is sent
   (`Tx.scala:244-249`, `Tx.scala:420-431`, `Tx.scala:739-745`);
3. holds the locks until the transaction resolves, then (on commit) re-checks the requirements without locks
   (`applying = true`), applies the update in memory (`RequirementsApplyer.apply`), releases the locks, and issues the
   backend commit (`Tx.scala:527-602`).

The transaction driver tallies votes per object, per pool, against the pool's write threshold
(`server/transaction/TransactionDriver.scala:254-306`) and runs Paxos over the primary object's pool.

The whole safety argument of this document is a quorum-intersection argument plus a lock argument:

* **Quorum.** Every pool IDA satisfies `2 * writeThreshold > width` (section 7). A commit decision needs at least
  `writeThreshold` commit votes for every referenced object. Two transactions that conflict on an object therefore share
  at least one voting store for that object.
* **Lock.** A store that voted commit for a transaction never votes commit for a conflicting transaction until the first
  one is resolved and applied (or aborted). The lock lives in memory but is re-created from the CRL after a crash
  (section 9.5).

Anything that weakens either leg is listed in section 11.

Vocabulary: "vote" is a store's `TransactionDisposition` (`VoteCommit` / `VoteAbort`), fixed once given
(`Tx.scala:717-719` only decides while `Undetermined`). "Voter" is a store, "holder" is a transaction holding a lock.

## 2. Requirements and operations

Every `TransactionRequirement` subclass (sealed, `common/transaction/TransactionRequirement.scala:48`) and every update
operation. "Changes object" means it is in `RequirementsApplyer.changedObjects` (`RequirementsApplyer.scala:32-42`): such
objects get a backend commit and take part in the in-flight rule (section 8).

### 2.1 Object requirements

| Requirement | Check semantics (`RequirementsChecker.scala`) | Lock taken (`RequirementsLocker.scala`) | Applied by (`RequirementsApplyer.scala`) | Changes object | Proof of commit (`appliedBy`) |
|---|---|---|---|---|---|
| `DataUpdate(ptr, rev, Overwrite)` | `getState` (exclusive, whole object), `checkRevision`: revision equals `rev` and object timestamp <= tx timestamp (`:97-99`, `:220-226`); the object's data must be in the transaction (`MissingObjectUpdate`, `:100-101`); slice-size limit unless shrinking (`:113-121`) | object exclusive lock `lockedToTransaction` (`:47`, `lockExclusive` `:15-21`) | revision := txid, timestamp := tx timestamp, data := update (`:89-96`) | yes | object revision == txid (`:57`) |
| `DataUpdate(.., Append)` and `DataUpdate(.., WriteRange)` | as Overwrite, plus the encoded `EncodedAppendDelta` must match the store's current slice (expected length; trailer for encoding index k-1) else `AppendMismatch` (`:103-111`); size check uses the delta's `newSliceLength` (`:115-120`) | same exclusive lock | revision, timestamp as above; `data := delta.applyTo(data)` XOR (`:98-99`) | yes | revision == txid |
| `RefcountUpdate(ptr, required, new)` | `getState` (exclusive), `checkRefcount`: refcount (count and updateSerial) equals `required` (`:160`, `:228-231`). No revision or timestamp check | object exclusive lock (`:54`) | refcount := new, timestamp := tx timestamp, revision unchanged (`:135-139`). A count of 0 makes `Frontend.commit` a deletion (`Frontend.scala:507-511`) | yes | none (traceless) |
| `VersionBump(ptr, rev)` | `getState`, `checkRevision` (`:162`) | exclusive (`:56`) | revision := txid, timestamp (`:141-145`) | yes | revision == txid |
| `RevisionLock(ptr, rev)` | `getState`, `checkRevision` (`:164`) | exclusive (`:58`) | nothing (`:147`) | no | none |
| `SharedRevisionLock(ptr, rev)` | `getSharedState` (shared): collides with exclusive object locks, content lock, any part lock, and refuses while the exclusive-preference window is open (`:78-87`); then `checkRevision` (`:166-167`) | adds txid to `sharedLocks` (`:60`) | nothing (`:149`) | no | none |
| `KeyValueUpdate(ptr, requiredRevision, contentLock, requirements, deletesKeys)` | see 2.2 | see 2.2 | see 2.3 | yes | see 2.3 |

### 2.2 `KeyValueUpdate` key requirements and what a KV update locks

A `KeyValueUpdate` is checked as one requirement (`RequirementsChecker.scala:123-158`). Decoded operations are examined
first: a key `Delete` without `deletesKeys` fails `UnflaggedDelete` (`:127-128`). The check is "whole object"
(`wholeObject = true`) when any of: `requiredRevision` defined, `contentLock` defined, a `KeyObjectRevision` requirement,
or an object-level operation (`:132-134`). Otherwise it is "key level".

| Item | Check | Lock taken | Notes |
|---|---|---|---|
| `requiredRevision = Some(r)` | revision == r, object timestamp <= tx timestamp (`:241-247`) | object exclusive (`lockKV` `:74`) | The only way a KV update moves the object revision (`Applyer:109-112`) |
| `contentLock = Some(FullContentLock)` | set of (key, revision) equals the object's content (`ContentMismatch`), no other holder on the content or any value (`:271-281`) | `kvs.contentLocked` (single holder, `:80`) | |
| `KeyRevision(k, rev)` | key exists (`KeyExistenceError`), no other lock on it, revision == rev, key timestamp <= tx timestamp (`:285-293`) | present-key lock `ValueState.lockedToTransaction` (`:87-89`) | exclusive per key |
| `KeyObjectRevision(k, rev)` | object revision == rev, no timestamp check (`:295-297`) | object exclusive (`:91`) | whole-object |
| `WithinRange(k, ordering)` | k within `[min, max]` (`:299-307`) | adds txid to `kvs.rangeLocks` (`:85`), a **set**: many holders | Does not check key locks or other range locks |
| `Exists(k)` | key present, not locked by another (`:309-312`) | present-key lock (`:93-95`) | |
| `MayExist(k)` | present: not locked; absent: no absent-key lock by another (`:314-319`) | present-key lock, or `absentKeyLocks(k)` if absent (`lockKey` `:118-121`) | |
| `DoesNotExist(k)` | absent and no absent-key lock by another (`:321-326`) | `absentKeyLocks(k) = tx` (`:99`) | |
| `TimestampEquals(k, ts)` | present, key timestamp == ts, not locked (`:328-334`) | present-key lock | |
| `TimestampLessThan(k, ts)` | present, error if `key.timestamp < ts`, i.e. passes when `key.timestamp >= ts` (`:336-342`) | present-key lock | The name reads "ts is less than the key's timestamp" |
| `TimestampGreaterThan(k, ts)` | present, error if `key.timestamp > ts`, i.e. passes when `key.timestamp <= ts` (`:344-350`) | present-key lock | Not used by any non-test, non-codec code |
| Operation `Insert(k, v)` | after requirements, every key an operation writes is checked like a key lock: present key's lock holder, else `absentKeyLocks` holder, must be none or this tx (`:145-152`, not when `applying`) | `lockWrites`: present-key lock or absent-key lock (`:131-134`, `lockKey`) | A blind insert (no requirement on `k`) still locks `k` |
| Operation `Delete(k)` | same as Insert; requires `deletesKeys` | same as Insert | Leaves no trace (see `appliedBy`) |
| Operation `SetMin/SetMax/SetLeft/SetRight/DeleteMin/DeleteMax/DeleteLeft/DeleteRight` | `KVWrites.objectLevel = true` (`KVWrites.scala:22-28`), so the update is whole-object (`Checker:134`) | `lockWrites`: object exclusive (`Locker:131-133`) | Do not set `deletesKeys` |

`KVWrites` (`server/store/KVWrites.scala:15`) is computed from the decoded operations in `Tx.lock()` (`Tx.scala:248`) and
stored in `lockedWrites` so `unlock` releases exactly what `lock` took even if the data is later dropped (`:84-85`,
`:335-340`). Keys are compared by content (`common/objects/Key.scala:12-16`).

The check order inside the KV branch matters: `getState` first (object-level collisions, then part-level if whole-object),
then `checkKVRequirements`, then `MissingObjectUpdate` if the update's data is absent (`:139-140`, so a KV requirement
without its operations never votes commit), then the per-key write locks.

### 2.3 How a KV update is applied, and what it leaves behind

`RequirementsApplyer.apply` (`:102-133`): timestamp := tx timestamp for every KV update; revision := txid only when
`requiredRevision` is defined. Operations are applied in order: `Insert` replaces the `ValueState` with a new one
(revision = txid, timestamp = tx timestamp, lock cleared); `Delete` removes it; the eight object-level operations set or
clear `min/max/left/right`. `state.data := kvs.encode()` afterwards.

`appliedBy` (`:50-63`) proves a commit for: `DataUpdate`, `VersionBump`, `KeyValueUpdate` with a required revision (object
revision == txid), and `KeyValueUpdate` without one when any key currently carries revision txid. `RefcountUpdate`, lock
requirements and a KV update whose effect left no key with that revision (delete-only, object-level-only, or inserts
since overwritten) are traceless. The traceless fallback (`TracelessCommitEntry`) is only recorded for `RefcountUpdate`
and for `KeyValueUpdate` with no required revision and `deletesKeys` (`TransactionDescription.scala:94-98`); see flaw F8.

### 2.4 Non-object requirements

`LocalTimeRequirement(timestamp, Equals|LessThan|GreaterThan)` is the only non-object requirement
(`TransactionRequirement.scala:50`). It is checked by every store hosting any object of the transaction against that
store's `HLCTimestamp.now` (`RequirementsChecker.scala:203-218`). It takes no lock and applies nothing
(`Locker:45`, `Applyer:41`). Semantics: `Equals` **fails when equal** (`now == ts` throws), `LessThan` fails when
`now < ts`, `GreaterThan` fails when `now > ts` (so `GreaterThan(ts)` means "ts is greater than now"). The tests encode this
(`RequirementsCheckerSuite.scala:604-640`). No code outside tests constructs it (grep: only codec, checker, locker,
applyer), and there is no `TransactionBuilder` method for it. Because each store reads its own clock at its own time, the
votes of different stores need not agree, which is harmless to safety (only commit votes counted per object matter) but
makes the requirement non-deterministic; a non-object failure aborts the store's vote for all of its objects
(`Tx.scala:489`).

### 2.5 Client-side structure that the store relies on

The client builder rejects (`client/internal/transaction/TransactionBuilder.scala`): two updates to one object
(`MultipleDataUpdatesToObject`, `:246-249`, `:273`, `:287`, `:307`, `:336`), a lock combined with an update or a
shared/exclusive lock mix on one object (`ConflictingRequirements`, `:346-358`), two refcount updates (`:319`). A
`RefcountUpdate` may accompany a data or KV update of the same object (allocation does this, `:96-107`). **The stores do
not enforce any of this.** See flaw F7.

## 3. Lock classes

`ObjectState` (`server/store/ObjectState.scala`) and `KVObjectState` (`:20-22`) hold six kinds of lock:

| Class | Field | Holders | Taken by |
|---|---|---|---|
| X | `ObjectState.lockedToTransaction` (`:17`) | one (a second writer logs an error and overwrites, `Locker:15-21`) | DataUpdate, RefcountUpdate, VersionBump, RevisionLock, KV with `requiredRevision`, `KeyObjectRevision`, object-level KV operation |
| S | `ObjectState.sharedLocks` (`:26`) | many | SharedRevisionLock |
| C | `KVObjectState.contentLocked` | one | `FullContentLock` |
| R | `KVObjectState.rangeLocks` | many | `WithinRange` |
| K | `ValueState.lockedToTransaction` | one per present key | key requirements, Insert/Delete of a present key |
| N | `KVObjectState.absentKeyLocks` | one per absent key | `DoesNotExist`, `MayExist` and Insert/Delete of an absent key |

`ObjectState.lockedWriteTransactions` (`:42-54`) is the union of X, K, N, C, R (not S). It is what a whole-object
requirement tests (`checkPartLocks`, `Checker:49-50`) and what readers are told about.

Locks are attached to the single `ObjectState` instance for the object. `Frontend.referenced` (`Frontend.scala:223`)
guarantees every transaction uses that instance while any references it, and a discarded instance is marked
`reloadRequired` rather than replaced, so checks against it fail with `StaleObjectState` (`Checker:52-58`,
`Frontend.scala:578-598`).

## 4. Lock-compatibility matrix

Row = the request being checked, column = a lock already held by a different transaction. `x` = collision
(`TransactionCollision`, abort vote unless awaited, section 8.4); `-` = compatible. "same k" / "other k" refer to the key.
Only non-`applying` checks are shown; `applying = true` takes no locks and enforces none (`Checker:67`, `:81`, `:146`,
`checkKVRequirements(ignoreLocks)` `:250-251`).

| Request \ Held | X | S | C | R | K (same k) | K (other k) | N (same k) | N (other k) |
|---|---|---|---|---|---|---|---|---|
| Whole-object exclusive (DataUpdate, RefcountUpdate, VersionBump, RevisionLock, KV with `requiredRevision`/`contentLock`/`KeyObjectRevision`/object-level op) | x | x | x | x | x | x | x | x |
| `SharedRevisionLock` | x | - | x | x | x | x | x | x |
| Key-level KV (key requirements, Insert, Delete on key k) | x | x | x | - | x | - | - | - |
| Key-level on an absent key k | x | x | x | - | - | - | x | - |
| `WithinRange` only | x | x | x | - | - | - | - | - |
| Applying a committed tx | - | - | - | - | - | - | - | - |

Additional rules:

* `getState` sets `exclusiveWaitUntil = now + 250 ms` whenever an exclusive-style request meets a held S lock
  (`Checker:69-72`). While it is open, new `SharedRevisionLock` requests collide with the transaction's own id
  (`:83-86`), so a writer cannot be starved. Any exclusive collision opens it, even if that writer later aborts (documented
  at `TransactionRequirement.scala:85-91`).
* A key-level KV update that holds no `requiredRevision` does not change the revision yet still collides with S
  (`getState` tests `sharedLocks` for every non-shared request). This is conservative.
* `WithinRange` and the object-level KV operations (`SetMin` ...) are the only pair that conflict through R, because the
  operations make the update whole-object and so test `lockedWriteTransactions`, which contains R.
* Holders of the same class on the same key/object by the same transaction never collide with themselves
  (`collide` skips `transactionId`, `Checker:38-41`; `otherLock` `:250-251`).

## 5. Per-operation conflict table (summary)

| Operation | Takes | Collides with held | Conflicts only through |
|---|---|---|---|
| DataUpdate / VersionBump / RefcountUpdate / RevisionLock | X | anything on the object (row 1 above) | |
| `RevisionLock` | X | same as above; applies nothing | |
| `SharedRevisionLock` | S | X, C, R, K, N, exclusive preference window | |
| KV update with `requiredRevision` | X (+ C/K/N/R for its other items) | anything on the object | |
| KV update with only key items | K/N on named and written keys | X, S, C, K/N on the same keys | |
| KV blind `Insert`/`Delete` | K or N on the key | X, S, C, same-key K/N | **Ordering of two sequential blind writes is not constrained** (F1) |
| `FullContentLock` | C + K on every value | X, S, any K/N/R/C | |
| `WithinRange` | R | X, S, C | |
| `Append` / `WriteRange` | X | anything on the object | revision chain orders them |

## 6. Invariants

Numbered so a reviewer can attack them individually. "Holds" means holds in the code as read, assuming the quorum
property of section 7; exceptions are cross-referenced to flaws in section 11.

**R1 (per-object serializability).** Two transactions that both change the revision of an object (`DataUpdate`,
`VersionBump`, `KeyValueUpdate` with required revision) and both reach commit must be ordered by the revision chain: the
later one requires the earlier one's revision. Reason: each requires the revision it read; a store accepts a revision
requirement only against the revision it holds and only when no other transaction holds X. With R3 this yields at most
one commit per revision.

**R2 (quorum overlap).** For every object `o` in pool `P`, a commit decision implies at least `P.writeThreshold` commit
votes from `P`'s stores (`TransactionDriver.scala:259-296`, `countedCommitVoters` `:464-470`), and
`2 * writeThreshold > width` (`IDA.scala:203`, `:408-415`, `:586-596`). Two commit decisions on the same object share a
store.

**R3 (vote implies lock until resolution).** A store that persists a commit vote holds the locks of that vote in memory
from before the vote leaves the store (`Tx.scala:468-478` then `lock()`; the response is sent only after `crl.save`
completes, `:739-745`) until the transaction resolves and, for a commit, until it is applied or its turn arrives
(`doCommit` `:527-530`, `releaseObjects` `:640-645`). The lock is re-created after a restart before any other message is
handled (`Frontend.scala:279-312`, `Tx.scala:403`).

**R4 (conflicting requests are refused while a lock is held).** While a lock of class L is held by T, another transaction
requesting a conflicting class (section 4) gets `TransactionCollision` and the store votes abort, or withholds its vote
under R12. The check and the lock are performed in one executor step (`Tx.scala:468-478` followed by `lock()`).

**R5 (no commit on stale state).** A commit vote is cast only against the current in-memory state, after every earlier
commit of the same object has been persisted by the backend or has failed (section 8). A failed commit makes the store
vote abort for the object forever (`BackendCommitFailed`, `Tx.scala:450-453`).

**R6 (apply is idempotent where requirements pin a version).** Applying a committed transaction re-checks the
requirements against the store's state (`Applyer:73-74`); an object that fails is skipped and reported in `TxCommitted`
for repair (`Tx.scala:561-568`, `:611`). Because `DataUpdate`, `VersionBump`, `RefcountUpdate` and KV updates with a
required revision pin revision or refcount, they apply at most once per store and never out of order. **Blind KV writes
do not pin anything** (F1, F2).

**R7 (no divergence between replicas).** Two committed transactions that are not ordered by R1 touch disjoint keys of a
KV object (K/N locks, sections 2.2 and 4) and no common object-level field, so their application order does not matter,
except for the object timestamp (F4). Sequential blind writes of the same key are *not* covered by the lock argument
(F1).

**R8 (lock lifetime).** Locks are released exactly when: (a) the Tx resolves abort (`Tx.scala:627-631`); (b) the Tx
resolves commit and its commit is not held (`doCommit` calls `unlock()` before applying, `:530`); (c) the held commit is
finally issued (`:615-632`, `releaseHeldCommit` `:167-171`); (d) the Tx is released without resolution
(`releaseObjects`, called from `TxFinalized` handling `Frontend.scala:616-622`). Locks are **not** held across the backend
write: ordering of followers is done by the in-flight marker (R9).

**R9 (one in-flight commit per object).** A transaction touching an object with a transaction commit in flight, or with a
held commit queued, does not vote (and holds no locks) until the commit persists. A committed transaction does not issue
its commit while an earlier commit of any object it changes is in flight or queued ahead of it
(`Frontend.scala:131`, `:150-176`; `Tx.scala:178-188`).

**R10 (a failed backend commit poisons the object).** After a backend commit failure, `failedCommits` contains the object
until the store is reloaded; later transactions on it vote abort and later committed transactions do not apply to it
(`Frontend.scala:684-688`, `Tx.scala:441`, `:536-541`).

**R11 (votes are final).** A disposition once set is never changed; a recovered Tx keeps the persisted vote
(`Tx.scala:394-401`).

**R12 (waiting is safe).** A Tx waiting for a lock holder, or for in-flight commits, holds no locks and gives no vote, so
waits cannot form a cycle and cannot make two transactions both commit (`Tx.scala:262-281`, `:301-320`, `:440-478`).

**R13 (a resolved transaction needs no lock).** `lock()` is skipped when the outcome is already known
(`Tx.scala:245-246`, `:254`).

## 7. Vote tallying against the IDA threshold

`TransactionDriver.receiveTxPrepareResponse` (`:230-312`), per prepare quorum on the primary pool:

For each referenced object pointer, over the pool's `width` indexes (`txd.poolIDAMap(ptr.poolId)`, `:267-279`):
count replies, commit votes and abort votes (any non-`VoteCommit` is an abort vote). Then:

* `nReplies < writeThreshold` -> cannot attempt, wait (`:281-284`).
* `nReplies != width && nCommitVotes < writeThreshold && width - nAbortVotes >= writeThreshold` -> still undecided, wait
  (`:286-289`).
* `width - nAbortVotes < writeThreshold` -> this object cannot commit (`:291-292`).
* The transaction commits only if every object can commit (`:295`).

Net effect, per object: commit iff `nCommitVotes >= T`; abort iff `nAbortVotes > width - T`; otherwise wait. Because the
objects are tallied from their own pool's stores, one object's abort votes do not lower another pool's count, but the
transaction as a whole aborts if any object aborts. A store's vote is one bit for the whole transaction, so a failure on
one hosted object is an abort vote for all of that store's objects (conservative). Votes are collected for the current
proposal only; `nextRound` clears `peerDispositions` (`:402-406`), and stores resend their fixed dispositions.

IDA thresholds (all checked at construction):

| IDA | Constraint | Consequence |
|---|---|---|
| `Replication(width, T)` | `T >= width/2 + 1` (`IDA.scala:203-204`) | `2T > width` |
| `ReedSolomon(width, k, T)` | `k >= width/2 + 1`, `T >= k` (`:408-415`) | `2T >= 2k > width` |
| `RandomAccessReedSolomon(width, k, T, ..)` | same (`:586-596`) | same |

Edge cases (see also section 10):

* **Exactly the threshold.** `nCommit == T`, `nAbort = width - T` (all replied) -> commit. A store that has not replied
  is not counted; the decision is made on the replies present. The unreplied stores may later vote anything, which cannot
  matter: `T` commit votes have been persisted.
* **One store lagging.** With `nReplies == T` and all commit -> commit; with `nReplies == T`, `nCommit == T-1` -> wait
  (`:286`); with one lagging store out of `width = 3, T = 2`: two commit replies decide commit, one commit plus one abort
  waits for the third.
* **`2T > width` is the only protection against two commit decisions.** The driver never verifies that a pool's IDA in
  the description equals the real pool's (`txd.poolIDAMap` is supplied by the client; `TransactionDriver.scala:267`).

## 8. The commit window

### 8.1 What the window is

From a commit decision at a store until `backend.commit` reports completion, the store has applied the update in memory,
released the locks (`Tx.doCommit` `:527-530`) and handed the state to the backend (`Frontend.commit`
`Frontend.scala:506-535`). Without further rules, a following transaction would see no lock and the new in-memory state
and could commit while the CRL held an older uncommitted change of the same object. The design
(`docs/superpowers/specs/2026-11-07-per-device-crl-and-commit-ordering-design.md`) therefore enforces a total order of
backend commits per object.

### 8.2 State

* `txCommitsInFlight: ObjectId -> count` of counted commits (transaction commits and `ordered` rebuild writes;
  reconcile writes too), `Frontend.scala:119`, incremented in `commit` (`:529`) and `reconcileSettled` (`:952`).
* `heldCommits: ObjectId -> List[Tx]` committed transactions waiting their turn, oldest first (`:122`).
* `commitWaiters: ObjectId -> List[Tx]` transactions withholding their vote (`:125`).
* `commitInFlight(oid) = txCommitsInFlight.contains(oid) || heldCommits.contains(oid)` (`:131`).
* `failedCommits` (`:128`), never cleared.

### 8.3 Behaviour

1. **Vote path** (`Tx.determineDisposition` `:440-478`): objects with `commitFailed` -> abort vote `BackendCommitFailed`;
   else if any of the transaction's hosted objects has `commitInFlight` and the outcome is unknown -> `awaitingCommits`,
   registered with `awaitCommitPersisted`; no timer, no locks; the vote is decided when `commitPersisted(oid)` has been
   received for each (`:304-311`, `endCollisionWait` `:325-333`). Otherwise the requirements are checked and locks are taken.
   Objects that the transaction only locks (`RevisionLock`, `SharedRevisionLock`) never have a commit in flight unless
   another transaction changes them (`changedObjects`); the check is on `commitInFlight`, whatever the requirement.
2. **Commit path** (`resolvedAndAllObjectsLoaded` `:615-632`): if the commit decision arrived before the object data
   (`commitAwaitsData`, `:165`), the commit is held without locks (a commit vote requires the data, so only a transaction
   that voted abort can be here). Else if `holdForTurn()` (`:178-188`): some changed object has a commit in flight or held,
   the Tx joins every changed object's queue in one `holdCommit` call (`Frontend.scala:150-151`) and keeps its locks. Queues are appended
   to atomically, so the oldest held commit heads every queue it is in and held commits cannot wait on each other in a
   cycle (`Frontend.scala:146-149`).
3. **Issue** (`doCommit`): `unlock()`, apply, then one `frontend.commit` per changed object; `heldCommitIssued` removes the Tx
   from the queues and `commitTurnFree` wakes the next (`Frontend.scala:160-176`, `Tx.scala:590-596`).
4. **Complete** (`backendOperationComplete` `Frontend.scala:671-718`): on success the count drops; when it reaches zero
   `commitTurnFree` is called last (so a synchronous backend completes inside the next transaction's commit safely).
   On failure the object is added to `failedCommits`, the state stays pinned and served (the store may have voted at the
   write threshold and must not lose it nor revert), and the counter still drops so followers wake and then vote abort.
5. **Followers see persisted state.** Because the voter waits for the persisted state, it checks against exactly the state
   the next recovery would produce.

### 8.4 `CollisionWait`: waiting out a holder you are built on

A transaction whose requirement check fails only with collisions against holders that it is **built on** (it requires the
revision that the holder writes: `DataUpdate`, `VersionBump`, `RevisionLock`, `SharedRevisionLock`, or a KV update's
`requiredRevision`, `Tx.scala:284-292`) and that still hold locks on this store (`frontend.holdsLocks`) does not vote
abort. It registers with each holder (`awaitLocksReleased`, `Frontend.scala:98-100`), holds no locks, and votes when the
holder releases (`locksReleased`, called at the end of `releaseObjects`, after the commit was issued so the follower then
sees the commit in flight, `Tx.scala:640-645`) or after `Tx.CollisionWait` = 500 ms, checked at the 1 s store heartbeat
(`Store.scala:71`, `Tx.scala:316-320`), so a wait lasts up to roughly 1.5 s. On expiry the requirements are checked again
and the collision, if still there, is an ordinary abort vote. A recovered Tx never waits (`:275`). A collision with a holder
that the transaction does not build on is an immediate abort vote. `objectErrors` keeps one error per object
(`Checker:170`), so a later error for the same object masks an earlier one; the wait then re-checks, which only
costs time.

## 9. Behaviour in specific situations

### 9.1 How each pool checks only its objects

`Tx.requirements` is `txd.requirements` filtered to object requirements for pointers in `allHostedObjects(storeId)`
(`TransactionDescription.scala:108-113`: same pool and index below the pool width) plus all non-object requirements
(`Tx.scala:59-61`). A store therefore never locks or votes for another pool's objects. Each pool's votes are tallied
only against that pool's width and threshold (section 7).

### 9.2 When locks are released

See R8. Also: a commit held for its turn keeps the locks and the references (`releaseObjects` is a no-op while
`commitHeld`, `Tx.scala:640`); a recovered commit vote whose objects did not load holds no lock for them
(`Locker:41-42`, F6); a `TxFinalized` for a Tx that has not loaded all objects releases through the same function.

### 9.3 Why stores that apply in different orders cannot diverge (and where the argument stops)

1. Different-object updates trivially commute.
2. Same-object updates that change the revision are ordered by the revision chain. A store that receives the later
   transaction's resolution first re-checks the revision requirement at apply time and skips the object (`Applyer:73-82`,
   `Tx.scala:561-568`); the store reports the object in `TxCommitted`, the finalizer records the miss and repair
   installs the state (see [repair-process.md](repair-process.md)).
3. KV updates on the same object that both hold only key-level locks must have disjoint key sets, otherwise the second
   check would have collided with the first one at the common store (R2, R4). Key operations on disjoint keys and the
   four object-level fields commute.
4. **Gap.** (3) argues about transactions concurrent at some store. Two *sequential* blind writes of the same key (the
   second voted after the first's locks were released at its quorum) can still be applied in the opposite order at a lagging
   store, because `apply` never compares the existing key's revision or timestamp with the incoming one and ignores locks
   (F1).

### 9.4 Deleted object, later transaction on the same id

Deletion is `RefcountUpdate(.., count 0)`: `Frontend.commit` sees `deletes`, calls `discard` (marks the referenced state
`reloadRequired`, drops it from the cache), records `deleting(oid)` and later reads answer `ObjectNotFound`
(`Frontend.scala:507-511`, `:521-522`, `:829-851`, `:982-985`). Transactions that referenced the old state fail with
`StaleObjectState` (`Checker:56-57`); transactions that load later fail to load and get `MissingObject`. A transaction
that built on the deleting one waits for the in-flight deletion (R9) then fails to load. A failed deletion commit keeps
`deleting` set, so the object keeps answering `ObjectNotFound` while the store is loaded.
A stale `DataUpdate`/`KeyValueUpdate` with an old required revision on a store that missed the deletion is an
ordinary stale-state vote (section 10.3): fewer than `T` such stores exist.
Object ids are UUIDs and are never reallocated by design; an allocation transaction has no existence check, see F11.

## 10. Failure scenarios

For each scenario: outcome, then why it is safe, or "NOT SAFE" with the flaw id.

### 10.1 Concurrent conflicting transactions

*Setup:* A and B both require revision R of object o (exclusive). *Outcome:* on every store the first to be checked takes
X; the second gets `TransactionCollision` (or, if it is built on the holder, waits). Because votes require `T` stores and
`2T > width`, at most one of A, B can collect `T` commit votes; the other is aborted when abort votes exceed `width - T`
or when it times out into a retry. A store that votes abort for the winner (it saw the loser first) still applies the
winner when it commits, ignoring the loser's lock (`applying`), after which the loser, if still unresolved there, fails
its revision check. Safe by R1, R2, R4.

*Variants.* A: exclusive vs shared: S holders block the writer (`getState`), the writer opens the 250 ms window and
new shared lockers back off. B: two key-level writers on different keys: both commit (no common lock); on the same key:
collide. C: `WithinRange` holders vs a `SetMin` writer: collide through R. D: `FullContentLock` vs any value writer:
collide through C/K. E: `DoesNotExist(k)` vs `Insert(k)`: collide through N. All safe by R4.

### 10.2 Partial vote arrival

The driver acts on the votes in hand (section 7). Abort needs `> width - T` abort votes, commit needs `T` commit votes.
Missing votes can only postpone a decision. A store that withholds its vote (waiting for data, collision, or in-flight
commits) is simply a missing reply; Paxos allows no reply, and the driver retransmits. The vote withheld for object data
times out into an abort vote after `Frontend.MissingDataWait` = 5 s (`Tx.scala:144-147`). Safe; liveness depends on
retransmission.

### 10.3 Stale stores voting on requirements

*Behind on a previous commit.* A committed; store S has not applied A.
(a) S voted commit for A: it holds A's locks, so B collides; if B is built on A, B waits up to 1.5 s for A's resolution
and then checks the post-A state (`awaitableHolders`). (b) S voted abort for A or never saw A: S's state is pre-A. B,
built on A, requires A's revision: `RevisionMismatch`, abort vote. B, built on the pre-A revision (a stale client): S
votes commit, but at least `T` stores hold A's commit votes, those stores either hold A's locks (collision) or have
applied A (revision mismatch), leaving at most `width - T < T` stale stores. B cannot commit. Safe by R2.

*Blind requirements.* A requirement that does not name the previous value (blind insert, `MayExist`, a delete, `WithinRange`)
passes on stale and fresh stores alike; its safety is the application's. See F1.

*Missing an update awaiting repair* (ErrorTree entry exists): S is stale in the same sense. Repair installs only what the
store still shows as at the snapshot, deferring locked keys and objects with a commit in flight, and skips transactions the
store holds live ([repair-process.md](repair-process.md)). A store with a pending repair is just a stale store here.
Because it votes on its stale state, its commit votes are always sound only when the argument above holds; and its abort
votes are always sound.

### 10.4 Store crashes while holding locks

Locks are in memory, the commit vote is in the device CRL (`Tx.scala:739-745`). After restart the Frontend recreates each
recovered Tx **before** handling any new message (`Frontend.scala:279-312`, messages held while `holding`, `:600-603`),
and a recovered commit vote takes its locks without re-checking (`Tx.scala:394-403`, `Locker:41-65`). Recovered
transactions without a persisted vote decide only after all recovered commit votes have taken their locks
(`Frontend.scala:302-312`). Reads are dropped meanwhile (`Frontend.scala:730`). A crash before the CRL save completes
loses the lock but also loses the vote (never sent). Safe if the CRL is intact; if it is lost, every store of the device
is rebuilt and the votes are lost: see [systemic-loss-protection.md](systemic-loss-protection.md) and F5.

### 10.5 Recovered transactions re-acquiring locks

(a) Two recovered commit votes that conflict can exist when A committed and was applied and persisted, B built on it voted
afterwards, and both are still in the CRL. `lock()` re-locks both; the second exclusive lock logs an error and overwrites
the first (`Locker:15-21`). This is harmless because A's outcome is committed; A's apply is a no-op (revision mismatch) and
is reported via `TxCommitted` as a missed object for repair, which reconciles exactly. (b) A recovered commit vote on a
KV update that has no required revision and whose data was already dropped from the CRL after the backend commit is
skipped with `MissingObjectUpdate` (data missing) rather than replayed (`Tx.scala:604-613`). (c) If the data was not yet
dropped, a blind write is replayed: F2. (d) A recovered object that fails to load holds no lock on it: F6.

### 10.6 Failed backend commit

The state stays in memory and is served, the CRL entry is kept (`Frontend.scala:209-215`, `:684-698`), `failedCommits`
poisons the object (R10): every later transaction on it votes abort; committed transactions skip it and report it in
`TxCommitted` (`Tx.scala:536-541`), which makes the finalizer mark a missed update. The store may have been counted among
the `T` commit voters, so the committed state exists in memory (and in the CRL) but not in the backend until reload; on
reload the Tx is recovered from the CRL and replays. Safe for new votes (abort). While the store is loaded it serves the
committed state (no revert). A failed *deletion* leaves the object answering `ObjectNotFound`. Not covered: a repair or
`ordered` rebuild write that fails does not enter `failedCommits` (`Frontend.scala:965-971`).

### 10.7 Deleted object followed by a transaction on the same id

See 9.4. Outcome: abort (`MissingObject` or `StaleObjectState`). The ordering is by R9: a Tx touching a deleting object
waits for the deletion to persist. An allocation transaction for the same id would create a fresh state: F11.

### 10.8 Key deleted, then a stale insert

(a) The insert carries `KeyRevision(k, r)` or `Exists(k)`: `KeyExistenceError` on stores that applied the delete; stale
stores pass; at most `width - T` of them. Abort. (b) `DoesNotExist(k)` (the allocate-and-insert pattern,
`TKVLInsert.requireDoesNotExist`) with an old view: the stores that applied the delete pass, the stale stores that still
have the key fail, so commit needs the `T` fresh stores; correct. (c) A **blind** insert resurrects the key by design,
on every store alike. (d) A delete's `TxResolved` reaching a lagging store after a later blind insert of the same key: that
store ends up without the key while the others have it: the mirror of F1. (e) Replay of an old blind insert from a
recovered CRL entry: F2.

### 10.9 Lock-deferral timeout

`Tx.CollisionWait` (8.4): on expiry `collisionWaitExpired = true` and the check is run once more; a still-held lock is
`TransactionCollision` -> abort vote, and the client retries. No lock is ever force-broken. A holder that never resolves
(dead client, dead driver) is taken over by a recovery driver when the Tx has been idle for `Host.txHeartbeatTimeout`
= 5 heartbeats (`Store.scala:75`, `Host.scala:60`), so locks are bounded by that plus recovery time. The `repair` side has
its own deferral count (`RepairPolicy.proofAfterDeferrals`, [repair-process.md](repair-process.md)).

### 10.10 IDA threshold edge cases

* **Exactly the threshold** commit votes, rest unknown: commit; the `T` voters hold locks, so a conflicting transaction
  needs `T` commit votes from at most `width - T` unlocked stores: impossible.
* **One store lagging, rest voted:** `width=3, T=2`: two commit votes suffice. If the third store is stale/behind it either
  votes abort on A (then applies A when resolved, ignoring locks) or votes late. No divergence for revision-chained updates;
  F1 for blind KV.
* **Mixed pools:** transaction on a `width=3,T=2` pool and a `width=5,T=3` pool: each pool needs its own threshold; one
  pool missing `T` replies blocks the decision (`:281-284`), one pool with `> width - T` abort votes aborts it.
* **RS pools:** `T >= k > width/2`, stronger than replication.
* **Replies exceeding width** are ignored: the loop is over `0 until width` (`:269`).

### 10.11 Other situations covered by the code

* **Commit decision before the data:** the store waits for the client's prepare or `TxFinalized` and then applies, sending
  `TxCommitted` only afterwards (`Tx.scala:160-171`).
* **Commit arrives while another commit on the object is in flight:** held and queued (R9). A `TxFinalized` for a held
  commit is deferred until the commit is issued (`Tx.scala:790-800`).
* **Prepare arriving after finalization:** ignored, answered from the status cache (`Frontend.scala:638-652`).
* **Prepare for an unknown transaction older than the horizon:** abort vote `TransactionTooOld` unless an object already
  shows the update, in which case the store proves the commit (`Tx.scala:444-449`, `:491-493`, `:516-522`).
* **An object that fails to load** (`CorruptedObject` etc.) is absent from `objects`: `MissingObject` abort for a fresh vote
  (`Checker:52-55`).

## 11. Suspected flaws and open questions

Confidence is my confidence that the issue exists as described (High = the code path is plainly there; Medium = plausible
under reordering/crash; Low = needs an unusual precondition or a trust-model violation). "Impact" is what can go wrong.

### F1. Sequential blind KV writes of one key (or object-level field) can be applied in opposite orders by a lagging store; permanent divergence with no repair entry. Confidence: Medium.

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: stale-replica (permanent divergence of one replica until rewritten).** Blind KeyValueUpdate has no state-dependent requirement and applying ignores locks (RequirementsChecker.scala:123-158, `ignoreLocks = applying`), so RequirementsApplyer.scala:126-133 overwrites the key with whatever transaction applies last; nothing compares ValueState revision/timestamp. Order inversion trigger on a lagging store S: A's commit is held by commitAwaitsData (Tx.scala:~120, data lost/dropped, e.g. CrlAdmission drop or QUIC budget drop) or A's TxResolved is delayed, while B (issued after the client saw A commit) votes, commits and applies at S; A's data/resolution then arrives (client retransmit) and applies over B. If S's TxCommitted for A reaches the finalizer within errorTimeout (2 s) no ErrorTree entry is written, so repair never runs. B's per-store commit queue only orders transactions present at that store (Frontend.holdCommit), A's held commit is not in it.
> *Fix sketch:* Make blind KV apply last-writer-wins on (ValueState.timestamp, revision) with tombstones for deletes, or forbid blind writes to keys without a revision/lock.


*Scenario.* A = `KeyValueUpdate(o, None, None, Nil)` with `Insert(k, v1)` and B = the same with `Insert(k, v2)`, no
requirements on `k`. A commits with `T` stores; they apply and release the key lock. B is checked by those stores (no
collision), reaches `T` commit votes and commits. Store S3 voted abort on A (or had A's prepare, but A's resolution is
delayed or dropped; e.g. a message dropped over budget, or S3's thread behind). S3 receives B's resolution first and
applies B (`Applyer:126-128`, locks ignored, `Checker:146`). A's resolution (or `TxFinalized`, which also calls
`resolved`, `Tx.scala:790-792`) arrives later and `apply` overwrites `k` with `v1`, revision A. Nothing compares the existing
key's revision/timestamp with the incoming one. If S3 answered A's `TxCommitted` within `errorTimeout` (2 s) there is no
ErrorTree entry, no repair, and the object diverges until the key is next rewritten. For `Delete` the mirror is a resurrected key.
*Why the claimed protection does not apply:* `KVWrites` only prevents **concurrent** conflicts at a common store; ordering
of sequential blind writes is whatever order each store receives resolutions in. *Code:* `RequirementsApplyer.scala:126-129`,
`RequirementsChecker.scala:146-152`, `Tx.scala:527-543`, `Frontend.scala:157-176` (the queue orders only transactions
already present at this store). *Possible direction:* last-writer-wins on `(ValueState.timestamp, revision)` at apply
time for blind writes, with tombstones for deletes, or forbid blind writes.

### F2. A blind KV write can be replayed over newer state after a crash between "backend commit persisted" and "CRL object data dropped". Confidence: Low-Medium.

> **Verification 2026-11-09: CONFIRMED (mechanism, narrow window). Severity: stale-replica.** SimpleCRL.dropTransactionObjectData only flips in-memory state (SimpleCRL.scala:296-299, LogContent.scala:206-209); the on-disk log entry keeps the data until the entry is deleted at TxFinalized. A crash after A persisted and B (blind, same key) overwrote it, before A's finalization, recovers A with its data; on resolution doCommit re-checks with applying=true and a blind Insert passes (RequirementsChecker.scala:123-158), so k := v1 over v2 on this store only. Needs a crash within the finalization window (ms to ~2 s).
> *Fix sketch:* Make the data drop durable before B's commit may proceed (or re-check ValueState timestamp/revision on apply).


*Scenario.* A (blind `Insert(k, v1)`) commits and persists; B (blind `Insert(k, v2)`) is checked only after A's commit
persisted (R9), commits and persists. The CRL's drop of A's object data (`crl.dropTransactionObjectData`,
`Tx.scala:604-613`) is not part of R9's ordering. If the store crashes before that drop is durable and before A is
finalized, A is recovered with its data, receives the resolution, and `doCommit` re-applies it: the check passes (no
requirement, no timestamp check for blind KV) and `k := v1` over `v2`. *Code:* `Tx.scala:604-613`,
`RequirementsChecker.scala:123-158`, `RequirementsApplyer.scala:102-133`. Also needs A's finalization to be pending, which
is the normal state of a just-committed transaction.

### F3. The pre-transaction opportunistic rebuild bypasses the checks that the live opportunistic rebuild has. Confidence: Low-Medium.

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: stale-replica.** Tx.completeLoad assigns os.metadata = r.requiredMetadata and os.data = r.data when only revision equals and !reloadRequired (Tx.scala:380-392): no commitInFlight guard, no refcount-serial max, no lock check, `frontend.commit(os, cs, txid)` not ordered. KeyValue objects do reach it (KeyValueObjectState.getRebuildDataForStore exists, client ObjectState.scala:~295). The client cache can lag blind KV writes/refcount changes (neither moves the revision), so the store's newer content/serial/timestamp is overwritten and persisted. Gated only by the client's 10 s repairCache (SimpleOpportunisticRebuildManager.scala:14-16) and by the object being flagged knownBehind (revision-based).
> *Fix sketch:* Route both rebuild paths through one guarded implementation (commitInFlight, refcount max, content compare) or through Reconcile.


`Tx.completeLoad` (`:380-392`) assigns `os.metadata := r.requiredMetadata` (the **client's cached** revision, refcount and
timestamp) and `os.data := r.data` whenever the revision matches, under any locks other transactions hold, without the
`commitInFlight` guard and without the refcount-serial max that `Frontend.opportunisticRebuild` applies (`Frontend.scala:869-882`),
and not counted as in-flight. A refcount change or a key-level KV update does not change the revision, so a stale client
cache can lower the store's refcount serial, timestamp or (for KV) data below the store's committed state, then persist it
(`frontend.commit`). It is gated by the client's 10 s repair cache and by `getRebuildDataForStore`, so exposure is
limited; I did not trace whether KV objects can reach it.

### F4. The object timestamp is last-applied-wins, not monotone, and differs by apply order. Confidence: Medium (certain behaviour, uncertain impact).

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (repair livelock) / stale metadata.** RequirementsApplyer sets timestamp := txd.startTimestamp unconditionally for blind KV and RefcountUpdate (RequirementsApplyer.scala:109-112, 137-139), without max. See repair F3 for the consequence.
> *Fix sketch:* Use max(existing, tx) timestamp for blind updates.


`RequirementsApplyer` sets `timestamp := tx timestamp` unconditionally for `RefcountUpdate` (`:137-139`), blind KV
updates (`:109-112`, they hold no revision requirement and so no `ObjectTimestampError` guard), and, via the revision
chain, DataUpdates (guarded). Two committed transactions with different start timestamps applied in different orders on
two stores leave different timestamps; a late transaction with an earlier timestamp lowers it. Consequences: spurious
`ObjectTimestampError` aborts on the store with the higher value; repair's caught-up test compares a read timestamp to an
entry timestamp (`server/repair/StoreRepairer.scala:270`) and can loop or terminate early; `Reconcile` raises the timestamp
only when the read is higher (`Reconcile.scala:162`, `:212`). Safety of data is not affected, but replicas' metadata
diverges by design.

### F5. A commit voter that does not apply the committed update drops its lock and becomes, effectively, a lost commit vote that still counts toward `T`. Confidence: Low.

> **Verification 2026-11-09: PLAUSIBLE. Severity: stale-replica.** doCommit unlocks before applying (Tx.scala:527-533) and a failed apply re-check is only logged (Tx.scala:549-575); needs a state change between vote and apply (F3 or repair/rebuild writes can cause it).
> *Fix sketch:* Keep the lock until the apply succeeded, or abort the store (rebuild-required) on a failed apply of a voted-commit.


`doCommit` unlocks first (`Tx.scala:530`) and then applies. If the apply is skipped for the voting store (the re-check
fails; `RequirementsApplyer` throws and `frontend.discard` forces a reload from a backend that lacks the update
`Tx.scala:549-552`; the state was marked `reloadRequired`, `Checker:52-58`), the store no longer has the lock **and** does not
hold the update. A conflicting transaction can then vote commit on the pre-update state at this store. Only the
`failedCommits` case is closed (that store votes abort for every transaction, R10). If enough committed voters are in this
state, a conflicting transaction can collect `T` commit votes from the stores that did not apply. The same shape applies to
a lost device CRL (rebuild) as treated in [systemic-loss-protection.md](systemic-loss-protection.md). Needs an exception
during apply or a state invalidation between vote and apply on several stores.

### F6. A recovered commit vote whose object fails to load takes no lock on it; a later load can succeed. Confidence: Low.

> **Verification 2026-11-09: PLAUSIBLE. Severity: stale-replica (low).** RequirementsLocker.lock skips objects that are not loaded; transient read errors are not distinguished.
> *Fix sketch:* Retry loads of a recovered vote's objects; never serve until all loaded.


`RequirementsLocker.lock` skips unloaded objects "there is nothing to lock on it" (`Locker:41-42`, `:141-143`). A failure
other than `ObjectNotFound` (`CorruptedObject`, `StoreNotFound`, `ObjectMismatch`) can be transient. After a later
successful load, a conflicting transaction B finds no lock and may vote commit while the recovered A's persisted commit
vote also stands. Needs a transient read error on the recovery read and success on a later one, both within A's
unresolved lifetime.

### F7. The stores trust the client's description. Confidence: Low (trust model).

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only (trust model).** Invariants enforced only in TransactionBuilder; stores take thresholds from the description (TransactionDriver.scala:88-90, 259-262).
> *Fix sketch:* Validate txd against pool state at the store.


(a) The "one update per object" and "lock xor update" rules are enforced only in `TransactionBuilder`; a hand-built
description with two `KeyValueUpdate`s on one object has `KVWrites.from` keep only the last (`toMap`,
`KVWrites.scala:37-47`), so the first update's keys are not locked; with `SharedRevisionLock` plus an update on one object
the shared lock is subsumed. (b) The driver uses the description's `poolIDAMap` for thresholds and `width`
(`TransactionDriver.scala:267`); a description with a smaller `writeThreshold` breaks R2 for that transaction. (c)
`primaryObject` defines the Paxos acceptors; a description that names the wrong primary object picks an arbitrary acceptor set.

### F8. Traceless coverage is narrower than "KV with required revision"/"delete" suggests. Confidence: Medium-Low.

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica (repair coverage).** TransactionDescription.tracelessObjects only covers RefcountUpdate and revision-less KV with deletesKeys (TransactionDescription.scala:93-98).
> *Fix sketch:* Treat all revision-less KV updates as traceless, or write TracelessCommits for them.


`tracelessObjects` (`TransactionDescription.scala:94-98`) covers `RefcountUpdate` and no-revision KV updates with
`deletesKeys`. A no-revision KV update that only sets or deletes `min/max/left/right`, and one whose inserts have since been
overwritten or deleted by later transactions, also leave no revision trace (`Applyer:60-61` looks for the txid on a
current key). The scaladoc on `deletesKeys` admits this for `DeleteMin/Max/Left/Right` (`TransactionRequirement.scala:104-107`).
The effect: a forgotten, committed transaction cannot be proved by `appliedBy` and has no TracelessCommits entry, so
a recovered commit vote for it relies on the recovery driver (see [transaction-recovery.md](transaction-recovery.md)).

### F9. `LocalTimeRequirement` and the `Timestamp*` key requirements have inverted or ambiguous semantics and no non-test callers. Confidence: High (code reading), low impact.

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only.** RequirementsChecker.checkLocalTime throws on Equals when equal and inverts LessThan/GreaterThan (RequirementsChecker.scala:~203-217); no non-test callers (grep).
> *Fix sketch:* Fix or delete.


`Equals` throws when equal (`Checker:207-209`), the other two compare `now` to `ts` in the opposite direction from the
names. `TimestampLessThan/GreaterThan` pass when `key.timestamp >= ts` / `<= ts`. `LocalTimeRequirement` is evaluated by
every store against its own clock, so two stores can disagree and the commit decision depends on which stores' replies
arrive first. Dead code today; a hazard if wired up.

### F10. Applying a committed transaction ignores locks held by transactions that already persisted commit votes. Confidence: Low (argued safe under R2, listed for review).

> **Verification 2026-11-09: PLAUSIBLE. Severity: doc-only (argued safe under R2).** Applying ignores locks (ignoreLocks = applying).
> *Fix sketch:* Log an invariant violation when an apply overrides a commit-voter's lock.


`Checker:67-82`, `:146` skip all lock checks when `applying`. The argument that a lock holder `B` then cannot commit relies on
R2. If R2 is violated (F5, F7(b), or a systemic loss), both A and B can commit and the lock-ignoring apply turns the
violation into a silent overwrite instead of a detected conflict, since the loser's apply re-check is just a revision check
(and none for blind KV, F1). There is no store-side detection of "B holds a commit vote conflicting with a committed A".
Open question: should an apply that overrides a held lock of a *commit* voter be logged as an invariant violation?

### F11. An allocating object has no existence check, so a replayed allocation can overwrite a live or deleted object. Confidence: Low.

> **Verification 2026-11-09: PLAUSIBLE. Severity: resurrection (needs horizon failure).** Frontend.readObjectForTransaction builds a fresh Allocating ObjectState for allocatingObjects without consulting backend/cache (Frontend.scala:834-845); protection is the cache/horizon only (see paxos F2/F3).
> *Fix sketch:* Have the backend reject allocating over an existing object.


`readObjectForTransaction` creates a fresh `ObjectState` with `ObjectRevision.Allocating`/`ObjectRefcount.Allocating` for
every object in `allocatingObjects` without consulting the backend or the cache (`Frontend.scala:833-843`), so the
`RefcountUpdate(Allocating -> Default)` and `DataUpdate(Allocating, Overwrite)` requirements always pass. The only guards
against a forgotten, already-committed allocation being voted and applied again are the status cache and
`Frontend.horizon` (`Tx.scala:444-449`), which depend on synchronized HLC clocks and on the cache not evicting early.
A retransmitted allocation of an object that was since modified or deleted, arriving within the horizon on a store without a
cache entry, would vote commit and, if resolved by a recovery driver, overwrite the object. The horizon is protected by an
early-eviction warning (`Host.scala:192-193`) but not enforced.

### F12. A hung backend commit stalls every transaction on the object indefinitely. Confidence: High (by design), liveness only.

> **Verification 2026-11-09: CONFIRMED (by design). Severity: liveness.** awaitingCommits has no timer (Tx.scala checkCollisionWait comment; Frontend.commitInFlight/awaitCommitPersisted). Concrete trigger: an FSIF store frozen by ENOSPC never completes held commits 'with an error or otherwise' (FsifBackend.scala:621-651), so every later transaction on the object at that store withholds its vote; other stores still vote, so quorum is unaffected unless many stores freeze.
> *Fix sketch:* Add a long watchdog that votes abort (BackendCommitFailed) for a store whose commit has been in flight longer than a bound.


`awaitingCommits` has no timer (`Tx.scala:454-460`, `Frontend.scala:172-176`) and `txCommitsInFlight` is cleared only by
the backend's completion. A backend that never completes (stuck I/O, a lost completion) leaves the object in flight and
every later transaction on it silently withholds its vote; the driver sees missing replies and the client retries
forever. The deliberate design; recorded because a lost completion has no recovery path.

### F13. Exclusive-preference window can abort unrelated shared lockers. Confidence: High, liveness only.

> **Verification 2026-11-09: PLAUSIBLE (not re-derived). Severity: liveness.** Accepted as documented behaviour; not re-derived.
> *Fix sketch:* Only set exclusiveWaitUntil when the colliding exclusive transaction survives.


`exclusiveWaitUntil` is set by any exclusive collision with an S holder, including one by a transaction that then aborts,
and it makes new `SharedRevisionLock`s collide for 250 ms (`Checker:69-72`, `:83-86`). The collision names the requester
itself (`:86`), so the driver never treats it as a stale race. Under repeated aborted writers on a fence object (vdisk) this
can starve shared lockers. Documented behaviour.

### Open questions

1. Is the `2T > width` property enforced anywhere other than IDA construction (e.g. when a pool is created from a
   deserialized IDA, or in a description arriving over the network)?
2. Should the apply path detect a committed transaction overriding a held commit-voter lock and report it (F10)?
3. Should `Frontend.opportunisticRebuild` and `Tx.completeLoad`'s pre-transaction rebuild share one guarded implementation (F3)?
4. Should object-level KV operations without a required revision be flagged like `deletesKeys` for the traceless path (F8)?
5. Should the key `ValueState.timestamp` take part in apply (F1, F2), so that blind writes become last-writer-wins by
   transaction timestamp?

## 12. Todo question: is `RevisionLock` exclusive?

**Verdict: yes, `RevisionLock` is exclusive, and it is a pure lock (it changes nothing).** The existing name does not say
"exclusive", and `SharedRevisionLock` is the non-exclusive sibling, so the Todo.txt rename to `ExclusiveRevisionLock`
would be accurate.

Evidence:

* Check: `RevisionLock` goes through `getState(oid)` with `wholeObject = true` (`RequirementsChecker.scala:164`), the same
  path as `DataUpdate`, `VersionBump` and `RefcountUpdate`. That path collides with any other holder of X
  (`checkObjectLocks`, `:44-46`), with any other holder of S (`:69-72`), and with every part lock including key, absent-key,
  content and range locks (`:73-74`, `ObjectState.lockedWriteTransactions` `:42-54`).
* Lock: it calls `lockExclusive` on the object (`RequirementsLocker.scala:58`), the same as `DataUpdate`; released by
  `release` (`:185`).
* Not a write: `RequirementsApplyer` has an empty case (`:147`) and `changedObjects` excludes it (`:39`), so it never starts a
  backend commit and never appears in the in-flight machinery.
* Builder: `lockRevision` throws if the object is also updated or shared-locked in the same transaction
  (`TransactionBuilder.scala:346-347`), and updates throw if the object is revision-locked (`:246-249` etc.).
* Other code states it: `server/repair/RepairProofs.scala:36` ("It takes the exclusive object lock, which collides with
  every lock a ..."), and the tests `RequirementsCheckerSuite` "RevisionLock failure on tx collision" (`:365`).
* Contrast `SharedRevisionLock`: `getSharedState` (`:78-87`), `sharedLocks += txid` (`Locker:60`), compatible with other
  shared locks (test "two SharedRevisionLocks on one object both pass", `RequirementsCheckerSuite.scala:237`).
* One difference from `VersionBump`: a `RevisionLock` leaves the revision unchanged, so a later transaction can still
  require the same revision; a `VersionBump` invalidates it.

### New findings from the adversarial verification (2026-11-09)

* **N1.** An opportunistic rebuild that survives its revision check can replace a KeyValue object's content with an older snapshot and leaves `kvState` unchanged; see repair-process.md F1, F2 and N1. This enlarges F3 here: the pre-transaction path is the same bug without the `commitInFlight` guard.
* **N2.** A replayed (already applied) `RefcountUpdate` or `DataUpdate` after a crash is skipped by its revision or refcount check and then reported in `TxCommitted.objectCommitErrors`, producing a spurious but harmless ErrorTree mark (see transaction-recovery.md F4).
