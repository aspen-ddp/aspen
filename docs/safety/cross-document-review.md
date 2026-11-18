# Cross-document safety review

This document covers what no single subsystem document can: the invariants each subsystem relies on from the others,
contradictions between the documents (and between a document and the code), and flaws that arise only from the
interaction of subsystems. Per-subsystem mechanisms and their own weaknesses are in the seven subsystem documents
(see [README.md](README.md)); they are referred to here by file name and section topic.

Paths are relative to `src/main/scala/org/aspen_ddp/aspen/`. Code is cited as file plus symbol. `w`, `t`, `r` are a
pool's width, write threshold and read threshold (`IDA.consistentRestoreThreshold`), `majority <= r <= t <= w`, so
`t + r > w`.

Contents: 1 the dependency map; 2 cross-subsystem invariants and the guards that hold them; 3 capacity; 4 clocks;
5 contradictions; open weaknesses (last).

---

## 1. The dependency map

| Subsystem | Relies on | For |
|---|---|---|
| Paxos transactions | CRL | Votes, promises, accepts and object data durable before any message depends on them (CRL contract C1, C2) |
| Paxos transactions | Transaction recovery | Drivers that can be rebuilt from any store's Tx; finalization before forgetting |
| Paxos transactions | Rebuild | A rebuilt store is muted for transactions started before its `acceptor-since` and answers expiry as `amnesiac` |
| Transaction requirements | CRL, recovery | Recovered commit votes re-take their locks before any message is handled |
| Transaction recovery | Repair | ErrorTree and UnackedCommits entries are consumed only once the store can no longer join T, and only by the store the pool names |
| Transaction recovery | Metadata pools | Marker batches commit (finalization waits for them) |
| Repair | Paxos, requirements | A read's unlocked parts reflect every committed transaction (quorum overlap plus lock reporting) |
| Repair | Rebuild, transfers | The ownership fence (host and device) keeps superseded copies from consuming entries |
| Rebuild | Transactions | `CurrentAllocationsRequest` lists every unfinalized allocation; reads of the pool's objects complete |
| Rebuild | Repair | Everything the store missed while rebuilding is marked and repaired after promotion |
| CRL | Store lifecycle | Entries leave the device CRL only after another durable copy exists or the store is unservable on disk |
| Systemic-loss protection | All of the above | Declarations of loss are independent and true (they are not checked; see that document) |

## 2. Cross-subsystem invariants and the guards that hold them

### 2.1 Forgetting a transaction

A store forgets T in exactly these ways: a committed T at `TxFinalized` (after its backend commits), an aborted T's
CRL record at resolution and its Tx at `TxFinalized`, a late outcome, an expiry discard or conclusion, a muted Tx once
its objects load, and a host restart, transfer or adoption (memory lost). Every one is covered by the forget floor:
once a store has forgotten T it refuses every prepare for T (paxos-transactions.md, transaction horizon). A store
never rejoins a transaction it forgot, so Paxos sees a forgetful acceptor as an offline one.

The premises other subsystems take from this:

* **Finalization precedes forgetting a Paxos-chosen commit.** `TxFinalized(commit)` is sent only after every
  finalization action completed, the missed-update marker last, so every ErrorTree mark and UnackedCommits entry of
  T is durable before any acceptor forgets T (transaction-recovery.md, finalizer). Late resolution
  (`TransactionDriver.resolveLate`) runs no actions on the strength of this.
* **An expiry conclusion of a known commit is sound by counting, not by that premise.** A Tx that knows its outcome
  concludes once more than `w - t` primary stores are non-holders (`ExpiryTally.decideKnown`). Every expiry commit
  rule leaves at least `t` primary stores that are commit voters or amnesiac (A: `vc >= t`; B: an accepted commit
  traces back to a commit-certain tally; F: `vc + k >= t`), and both kinds count as holders. So more than `w - t`
  non-holders need one of them to have forgotten T first, which only `TxFinalized`, a late outcome or a discard does.
  Late condition 1 is the one inference that does rely on "a memoryless quorum means T was finalized", which holds
  only for a commit chosen by Paxos: it is taken only inside the horizon by the driver's clock, and an expiry commit
  exists only past the horizon plus the expiry margin by the decider's clock, a margin that covers the inter-host
  skew (paxos-transactions.md, late condition 1).
* **A recovered or expiring commit vote is not dropped silently.** It accepts an abort, or discards itself, only after
  an UnackedCommits lookup that began after the abort (or the discard decision) found no entry.
* **Repair waits until the store cannot join T.** An entry is left alone while T's start is at or above the store's
  forget floor (`RepairTarget.mayJoinTransaction`) and while the store holds a Tx for T or is loading its CRL
  (`Frontend.mayHoldTransaction`), so no Tx can apply T after a reconcile and have its entry deleted under it.
* **Applied stubs outlive every Tx that could consult them.** A stub is dropped only once T is below the floor and
  the store holds no Tx for T (`Frontend.expireStubs`).

### 2.2 Store identity: restarts, transfers, adoption, rebuild

* Every restart, adoption and transfer-in builds a new Frontend, whose load floor (load plus `MaxStartLead`, the
  host's join ceiling, or the arrival margin of `AcceptorSinceMargin` after a move between hosts) covers every T the
  store may have joined and forgotten in its previous life. `StoreLoader.ArrivedAtFile` is written durably before a
  moved store can load.
* A rebuilt store has a fresh `incarnation` and an `acceptor-since` (finish plus 10 s), both written durably before
  the rename that makes the directory appear, and both carried by transfers. Before `acceptor-since` it promises,
  votes and accepts nothing and answers expiry queries `amnesiac`, never as a non-holder
  (`Frontend.mayHaveForgottenUndecided`). UnackedCommits entries naming another incarnation apply nothing at it; their
  reconcile installs the committed state.
* ErrorTree marks carry no incarnation on purpose: a marker has no authoritative incarnation (only a possibly stale
  pool state), ignoring a wrongly attributed mark would leave a real miss unrepaired, and an exact reconcile makes an
  inherited mark harmless.
* Only the store the pool entry names, on the device it names, consumes entries (`StoreRepairer.OwnershipFence`:
  host and device, failing closed, re-checked before each delete). A superseded copy (a failed device returning, a
  copy left on another device of the same host) never consumes the current copy's entries.
* A failed device is retired under a durable `failed` marker (`DeviceShutdown.retireFailedDevice`), and no host loads
  a device carrying it. Until a restarted or partition-healed old host's first reconcile, it may still answer peers
  holding a stale store-to-host mapping (the no-fencing trade-off accepted for adoption).
* `Store.close` and a reload over the same backend stop every driver of the store (`Store.shutdownDrivers`); a
  finalizer already running completes its idempotent actions and does not restart.

### 2.3 Locks across crashes and copies

* Recovered commit votes re-take their locks before the store handles any transaction message or client read
  (`Frontend.holding`; transaction-requirements.md, store crashes). Two recovered votes on one single-holder lock
  both hold it until each releases (`ObjectState.displacedLocks`; transaction-requirements.md, lock classes).
* Reads report the write locks of every voted, unapplied transaction (shared revision locks excepted); repair defers
  locked parts, and a read that needs a voted, unapplied allocation parks rather than answering `ObjectNotFound`
  (`Frontend.parkAllocationRead`), so no read in an allocation's apply window proves a deletion.
* A rebuild copies state from ordinary reads and ignores the locks they report, and the rebuilt store holds no lock
  for any transaction its old incarnation voted in. Those votes still count: a driver keeps a vote until a nack or a
  proof starts a new round (a muted store sends neither), a client's vote count is final, and expiry rule B places no
  condition on lock-only pools. So the rebuilt store holds **inherited votes** instead
  (`Frontend.inheritedVoteLock`, rebuild-process.md, the rebuilt store): it votes abort (`InheritedVoteLock`) on every
  object until a query of its peers' held votes (`HeldVote`, in `CurrentAllocationsReply`) completes past
  `RebuildingStore.inheritedVotesAfter` (the pass start plus the horizon and twice `AcceptorSinceMargin`, after which
  no store casts a vote on a transaction the old incarnation joined), then on the objects of the transactions they
  report, until a later query no longer reports one or its `TxResolved`/`TxFinalized` reaches the store. A
  transaction that can still commit with the old vote counted needs `t - 1` live commit voters among the other stores,
  so `w - t + 1` replies include one. Without this, in Replication(3,2) the rebuilt store plus T's abort voter would
  commit a T' over T's lock on a lock-only object, and T could then commit after it (RebuiltVoterLockIntegrationSuite).
  The rebuild marks a copy whose read reported a write lock, so a copy predating such a T is repaired
  (rebuild-process.md 5.2, locked copies).

### 2.4 Repair and transactions on one object

* A repair install is an in-flight write of the object (`Frontend.reconcileSettled`): transactions wait behind it as
  behind a commit, and it is refused while a commit is in flight, held, failed or being deleted.
* Repair proofs are untracked, `replaySafe` transactions; every requirement on a refcount-0 or missing object fails
  `MissingObject`, so no proof revives a deleted object. The refcount proof (`RefcountUpdate(rc -> rc)`) is a real
  write: it changes the object (a backend commit, the object timestamp raised to the proof's start) and, once
  committed, applies over other transactions' locks at the stores that voted abort on it.
* A repair write never overwrites a different occupant of an FSIF slot (`RepairSlotOccupied`); the repairer drops a
  dead occupant with an id-checked `repairDelete`, only when the AllocationTree does not list it.
* A failed repair write restores the fields it changed to the settled pre-repair state, the backend's
  (`Frontend.repairWriteComplete`), keeping the instance lock holders reference, so their committed apply holds. A
  repair of an object the store did not hold discards its state.

### 2.5 Allocations

* No allocation can be built without its `AllocationFinalizationAction`, so the AllocationTree eventually lists every
  committed allocation; a listed object may already be deleted.
* Repair never takes absence from the tree as a deletion: only `InvalidObject.provesDeletion` drops a slice.
* A rebuild covers allocations the lost store acknowledged but the tree does not yet list by asking `w - t + 1`
  peers for their unfinalized allocations and checkpointing the union (rebuild-process.md, pending allocations).
* A replayed allocation never overwrites a live object (`ObjectExists`). With no tombstones, an id whose deletion
  persisted can be allocated again by a store that never saw T (transaction-requirements.md, deletion and
  allocation).

### 2.6 Client-held state

A client keeps no object cache. Every allocation takes a fresh random id, so a stale pointer never names a newer
object, and FSIF answers `ObjectNotFound`/`ObjectMismatch` to a stale locator. Every requirement on a missing or
refcount-0 object fails, so a stale pointer cannot revive a deleted object through a zombie slice; the zombies are
fewer than the write threshold. TKVL upper-tier caches and application caches write under revisions. The status
cache holds outcomes, never object state, and is monotone (`TransactionStatusCache.updateStatus`); a size eviction
raises the host's eviction watermark, so an evicted entry's absence is never trusted.

## 3. Capacity

**CRL admission lanes.** Every committed but unfinalized transaction keeps its admitted CRL bytes until its
finalization actions complete, and finalization needs a marker batch to commit in the metadata tier above. Admission
therefore has lanes by the receiving store's pool, copied into its store config at creation (`AdmissionTier`,
`CrlAdmission.ceiling`): a device admits a new data-pool transaction only below 3/4 of its limit `L`
(crash-recovery-log.md section 6; it rises with each CRL growth), a metadata-pool one below 7/8 `L`, a bootstrap-pool
one up to `L`. A data transaction's marker batch writes trees in a metadata pool, a metadata transaction's in the
bootstrap pool, and the bootstrap pool's own tree writes track nothing
(`MissedUpdateFinalizationAction.limitTracking`). So each higher tier keeps about `L/8` of headroom on every device,
and lower-tier load cannot prevent a transaction's finalization. `TxFinalized` is a message and never admitted.
Charges made without a check (one transaction crossing its ceiling, data arriving after admission, records recovered
at load, applied stubs) are bounded by the transactions already admitted, and one transaction's bytes on a store by
`maxTransactionBytes`, past which its prepare is dropped (crash-recovery-log.md section 6, Oversized). What can still
fill a lane is the tier's own traffic, including same-tier finalizers (TKVL split, join, tail removal): ordinary
back-pressure that clears as the tier's transactions finalize upward. Data-only devices give up a quarter of their
headroom (Backlog.txt).

**Repair backlog.** ErrorTree and UnackedCommits entries cost backend space in the metadata pools, not CRL bytes; an
entry occupies CRL admission only while the batch writing it is in flight. The backlog drains one entry at a time per
store, a timed-out scan that saw entries stays at the scan floor and keeps its deferrals, and a silent voter's
UnackedCommits entry costs one delete. No argument bounds the metadata-pool space the backlog holds during a long
outage.

## 4. Clocks

Every clock comparison that crosses subsystems, and what bounds it:

| Comparison | Clocks | Bound |
|---|---|---|
| Forget floor time leg (`now - horizon`) | the store's own wall clock against T's start | same host only; a backward step of more than about 8 s reopens the window (paxos-transactions.md) |
| `MaxStartLead` mute and the join ceiling | host wall clock | caps every accepted start, and so every object timestamp and client HLC, at the fastest host plus 2 s |
| Arrival margin, rebuilt store's `acceptor-since` | old host against new host | inter-host skew below 8 s |
| Expiry (`Frontend.pastExpiry`, freeze) | each store's own clock | stores freeze at different moments under skew; the 8 s margin covers the inter-host skew, so an expiry commit exists only past the horizon on every clock (late condition 1) |
| Repair age gate (60 s) | repairing host's HLC against the marker's start | paces only; the read decides a deletion |
| Repair `mayJoinTransaction` | the store's own floor | same host |
| Repair completion gate | object timestamps, no clock | object timestamps never decrease |
| Rebuild pending-allocation coverage | none | the checkpointed union of listed allocations |

---

## 5. Contradictions

### 5.1 "An acceptor having forgotten T implies every finalization action completed"
Resolved. The premise holds for a commit chosen by Paxos. An expiry commit (`Frontend.decideExpiry`) is accepted by
no acceptor, so a memoryless post-proof quorum is reachable while its finalizer runs; late condition 1 is therefore
taken only inside the horizon by the driver's clock, where no expiry commit can exist yet (paxos-transactions.md,
late condition 1; transaction-recovery.md, "Finalization gates forgetting").

### 5.2 When a prover is forgotten
paxos-transactions.md (proof of commit on an unknown transaction) says a prover "on any real outcome ... applies
nothing ... and is forgotten (`Tx.applyNothing`)", and transaction-recovery.md (finalizer) says "a muted Tx and a
prover are dropped without `TxFinalized`". The code (`Tx.receiveResolved`, `Tx.receiveFinalized`) ignores a non-late
`TxResolved` at a prover and forgets it only on `TxFinalized` or a late outcome, as paxos-transactions.md P9 and
transaction-recovery.md (how a driver learns the outcome) say. The latter statements are right.

### 5.3 "Expiry never discards a committed, unfinalized Tx"
rebuild-process.md (pending allocations) rests the `w - t + 1` coverage argument on this. paxos-transactions.md rule D
discards a T that may have committed when a changed pool has `vc + s < r` (logged `dataLost`). paxos-transactions.md
is right; the rebuild argument holds within the data
tolerance (`t - r` lost commit voters), which is the only regime in which the allocation is restorable anyway.

### 5.4 Re-applied Inserts "are healed by reconcile"
Resolved. A re-apply (a recovered commit vote, or one applying through its UnackedCommits entry) that would insert a
key now absent, on an object a later transaction reached, quarantines the object instead of writing the key back
(transaction-requirements.md 10.5c). transaction-recovery.md section 9 no longer lists Inserts after a later delete
among the residual classes. A re-allocation at a store that never saw T is still healed only if the deletion's ErrorTree
mark for that store is unconsumed when the zombie appears.

### 5.5 "Nothing of a lost store is destroyed before its rebuild succeeds"
Resolved. Every copy that stops being its store's owner copy is retired into `pending-delete/` with its CRL state
(`StoreCopies.retire`), and released only once the store is Active on the device its pool names
(`StoreCopies.release`). That covers a re-placed queued rebuild's copy, which the Deleted Stores pass now retires, and a
transfer-out close's save file, which the retire keeps. A copy is identified by its name, not its store config, and
deletes are rename-first. An accepted rebuild loss ends in that Active flip by design: the acceptance is the
operator's release. rebuild-process.md B3, systemic-loss-protection.md 4.4 and `server/host/CLAUDE.md` all state it.

### 5.6 Expiry rule F's "live commit voters holding T's locks"
paxos-transactions.md (rule F and the abort-paths audit) says F commits only where every pool has more than `w - t`
live commit voters holding T's locks. A Tx created from a resolved, unfinalized cache entry (`Frontend.receivePrepare`
-> `Tx.outcomeDecided`) still votes, takes no locks, keeps `txStatus = Unresolved`, and reports
`votedCommit = true` to status queries (`Tx.statusForExpiry`). This is harmless: such a store is never among the
fixed abort voters of an abort-certain object, so it cannot lift an aborted T to a commit count (L1), and counting it
for a T that really committed commits what was committed. But the statement is not literally true, and such a Tx
hides its host's cached outcome from status queries (the reply is `Unresolved`, so other observers do not adopt it).

### 5.7 The stale rebuilt copy's example lay outside the tolerance it assumed
Resolved. The weakness (a walk copying an object while a commit the lost store applied is unapplied elsewhere) was
described with Replication(3,2), where `t = r` and losing one of T's two holders before the other applies is beyond
tolerance anyway. It is real for `t > r` (for example Replication(5,4)), where rebuild-process.md 5.2 (locked copies)
closes it: a copy read under a write lock is marked in the ErrorTree.

---

## Open weaknesses

None.
