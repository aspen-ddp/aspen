# Cross-document safety review

This document covers what no single subsystem document can: the invariants each subsystem relies on from the others,
contradictions between the documents (and between a document and the code), and flaws that arise only from the
interaction of subsystems. Per-subsystem mechanisms and their own weaknesses are in the seven subsystem documents
(see [README.md](README.md)); they are referred to here by file name and section topic.

Paths are relative to `src/main/scala/org/aspen_ddp/aspen/`. Code is cited as file plus symbol. `w`, `t`, `r` are a
pool's width, write threshold and read threshold (`IDA.consistentRestoreThreshold`), `majority <= r <= t <= w`, so
`t + r > w`. A pool's **data tolerance** for a committed T is `t - r` of T's commit voters losing their copy before
the others apply it: beyond it, T's update is held by fewer than `r` stores and is lost whatever any subsystem does.

Contents: 1 the dependency map; 2 cross-subsystem invariants and the guards that hold them; 3 capacity; 4 clocks;
5 contradictions; open weaknesses (last).

---

## 1. The dependency map

| Subsystem | Relies on | For |
|---|---|---|
| Paxos transactions | CRL | Votes, promises, accepts and object data durable before any message depends on them (CRL contract C1, C2) |
| Paxos transactions | Transaction recovery | Drivers that can be rebuilt from any store's Tx; finalization before forgetting |
| Paxos transactions | Rebuild | A rebuilt store is muted for transactions started before its `acceptor-since` and answers expiry as `amnesiac` |
| CRL | Paxos, recovery | A save skipped after a final deletion (C1, C9) belongs to a transaction the store has concluded (2.1) |
| CRL | Store lifecycle | Entries leave the device CRL only after another durable copy exists or the store is unservable on disk |
| Transaction requirements | CRL, recovery | Recovered commit votes re-take their locks before any message is handled |
| Transaction requirements | CRL, recovery | A quarantine survives a reload through the quarantining Tx's kept CRL state and its UnackedCommits entry |
| Client reads | Requirements | Every commit voter applies a committed update or keeps its lock, applying in lock order (transaction-requirements.md 8.3); at most `w - r` copies lack it |
| Client reads | Requirements, rebuild | `InvalidObject.provesDeletion`: a commit voter of an unapplied allocation answers `AllocationPending` (2.5) |
| Transaction recovery | Repair | ErrorTree and UnackedCommits entries are consumed only once the store can no longer join T, and only by the store the pool names |
| Transaction recovery | Metadata pools | Marker batches commit (finalization waits for them) |
| Allocation and split finalizers | Client reads | A read proving deletion means the object is not live, so it needs no tree entry (2.5) |
| Repair | Paxos, requirements | A read's unlocked parts reflect every committed transaction (quorum overlap plus lock reporting) |
| Repair | Rebuild, transfers | The ownership fence (host and device) keeps superseded copies from consuming entries |
| Quarantine verification | Repair, client reads | A read at least as new as the quarantining T shows T if T committed: T's commit voters apply it before any later write over its locks |
| Rebuild | Transactions | `CurrentAllocationsRequest` lists every unfinalized allocation and every held commit vote (`HeldVote`); reads of the pool's objects complete |
| Rebuild | Repair | Everything the store missed while rebuilding is marked and repaired after promotion |
| Systemic-loss protection | All of the above | Declarations of loss are independent and true (they are not checked; see that document) |

## 2. Cross-subsystem invariants and the guards that hold them

### 2.1 Forgetting a transaction

A store forgets T in exactly these ways: a committed T at `TxFinalized` (after its backend commits), an aborted T's
CRL record at resolution and its Tx at `TxFinalized`, a late outcome, an expiry discard or conclusion, a muted Tx once
its objects load, and a host restart, transfer or adoption (memory lost). Every one is covered by the forget floor:
once a store has forgotten T it refuses every prepare for T (paxos-transactions.md, transaction horizon), and a
prepare for a T whose finalized outcome the host caches creates no Tx (`Frontend.receivePrepare`). A store never
rejoins a transaction it forgot, so Paxos sees a forgetful acceptor as an offline one.

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
  an UnackedCommits lookup that began after the abort (or the discard decision) found no entry, and a late commit
  with no entry applies nothing (`Tx.unackedCommitAbsent` -> `applyNothing`). The premise is that a counted voter
  without an entry acknowledged T. A voter that acknowledged with `objectCommitErrors` and keeps T's state for a
  replay (a failed backend commit, `Frontend.TxCommits.failed`; a quarantine, `quarantinedConclusions`) says so in
  `TxCommitted.keptForReplay`, and `MissedUpdateFinalizationAction.writeMissedUpdates` gives it an entry, so a reload
  after `TxFinalized` re-applies the kept state through it (retrying the commit, or quarantining again).
* **A save acknowledged without being written belongs to a concluded transaction.** The CRL skips a save of an id
  that an unwritten entry deletes finally and acknowledges it once the deletion is durable (crash-recovery-log.md C1,
  C9; `SimpleCRL.skippedAfterFinalDeletion`). A store queues a final deletion of T (`crl.deleteTransaction` in
  `Frontend.concludeTransactionState` or `deleteTransactionState`) only once it has concluded T: aborted, finalized,
  a late outcome, an expiry discard, or a quarantine cleared after one of those. A Tx of T that saves after that
  point (the live Tx with `keepAdmission`) works on a transaction whose outcome this store has already concluded. If
  the store then crashes, its reload floor refuses T, which makes it an offline acceptor of a decided transaction;
  the promise, accept or vote it acknowledged decides nothing that is not decided already.
* **Repair waits until the store cannot join T.** An entry is left alone while T's start is at or above the store's
  forget floor (`RepairTarget.mayJoinTransaction`) and while the store holds a Tx for T or is loading its CRL
  (`Frontend.mayHoldTransaction`), so no Tx can apply T after a reconcile and have its entry deleted under it.
* **Applied stubs outlive every Tx that could consult them.** A stub is dropped only once T is below the floor and
  the store holds no Tx for T (`Frontend.expireStubs`).

### 2.2 Store identity: restarts, transfers, adoption, rebuild

* Every restart, adoption and transfer-in builds a new Frontend, whose load floor (load plus `MaxStartLead`, the
  host's join ceiling, or the arrival margin of `AcceptorSinceMargin` after a move between hosts) covers every T the
  store may have joined and forgotten in its previous life. `StoreLoader.ArrivedAtFile` is written durably before a
  moved store can load. Every in-memory store state (`failedCommits`, quarantines, inherited-vote progress) is lost
  with the Frontend; only what the CRL state, the save file and the store directory's markers carry survives.
* A rebuilt store has a fresh `incarnation` and an `acceptor-since` (finish plus `AcceptorSinceMargin`, 10 s), both
  written durably before the rename that makes the directory appear, and both carried by transfers. Before
  `acceptor-since` it promises, votes and accepts nothing and answers expiry queries `amnesiac`, never as a non-holder
  (`Frontend.mayHaveForgottenUndecided`). UnackedCommits entries naming another incarnation apply nothing at it; their
  reconcile installs the committed state.
* ErrorTree marks carry no incarnation on purpose: a marker has no authoritative incarnation (only a possibly stale
  pool state), ignoring a wrongly attributed mark would leave a real miss unrepaired, and an exact reconcile makes an
  inherited mark harmless.
* Only the store the pool entry names, on the device it names, consumes entries (`StoreRepairer.OwnershipFence`:
  host and device, failing closed, re-checked before each delete with one read shared for `OwnershipRecheck`, 1 s).
  The fence is not part of the delete's transaction; what keeps a superseded copy from consuming entries is that a
  copy stops being served before its successor is owner (a transfer closes its source first, a lost or retired copy
  is not loaded), with the accepted exception of the no-fencing adoption trade-off below.
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
  (`Frontend.parkAllocationRead`, `Frontend.allocationPending`).
* A committed apply ignores other transactions' locks (`RequirementsChecker.check` with `applying = true`), but a
  committed transaction holding no locks at a store waits to apply while another transaction there holds locks it
  collides with on an object both change (`Tx.awaitLockHolders`, transaction-requirements.md 8.3, R15). So a commit
  voter of T whose outcome is late applies T before a later commit T2 that does not chain on T's revision (a
  `RefcountUpdate`, a key-level write, a repair proof, 2.4), and T2's raised object timestamp never fails T's
  re-check. Reads choose the revision by count alone (`BaseObjectReader.getStoreStates`) and a quarantine
  verification counts the quarantined copy itself among the `r`; both rely on T's `t` commit voters holding T, which
  the lock order guarantees without any store failure.
* A rebuild copies state from ordinary reads and ignores the locks they report, and the rebuilt store holds no lock
  for any transaction its old incarnation voted in. Those votes still count: a driver keeps a vote until a nack or a
  proof starts a new round (a muted store sends neither), a client's vote count is final, and expiry rule B places no
  condition on lock-only pools. So the rebuilt store holds **inherited votes** instead
  (`Frontend.inheritedVoteLock`, rebuild-process.md 6.4): it votes abort (`InheritedVoteLock`) on every object until a
  query of its peers' held votes (`HeldVote`, in `CurrentAllocationsReply`, built by `Frontend.heldVotes`) completes
  past `RebuildingStore.inheritedVotesAfter` (the pass start plus the horizon and twice `AcceptorSinceMargin`, after
  which no store casts a vote on a transaction the old incarnation joined), then on the objects of the transactions
  they report, until a later query no longer reports one or its `TxResolved`/`TxFinalized` reaches the store
  (`Frontend.inheritedVoteDecided`). A transaction that can still commit with the old vote counted needs `t - 1` live
  commit voters among the other stores, so `w - t + 1` replies include one. Without this, in Replication(3,2) the
  rebuilt store plus T's abort voter would commit a T' over T's lock on a lock-only object, and T could then commit
  after it (RebuiltVoterLockIntegrationSuite). The rebuild marks a copy whose read reported a write lock, so a copy
  predating such a T is repaired (rebuild-process.md 5.2).

### 2.4 Repair and transactions on one object

* A repair install is an in-flight write of the object (`Frontend.reconcileSettled`): transactions wait behind it as
  behind a commit, and it is refused while a commit is in flight, held, failed or being deleted.
* Repair proofs are untracked, `replaySafe` transactions; every requirement on a refcount-0 or missing object fails
  `MissingObject`, so no proof revives a deleted object. The object-level proof (`RepairProofs.proveObjectLevel`, an
  `ExclusiveRevisionLock`) changes nothing. The refcount proof (`RefcountUpdate(rc -> rc)`) and the key proof (a
  KeyValue update with no operations, `KeyRevision` or `DoesNotExist`) are real writes: each changes the object (a
  backend commit, the object timestamp raised to the proof's start) and, once committed, applies at the stores that
  voted abort on it once the commit votes it collides with there have resolved. A proof is attempted precisely when an object stays
  locked across passes (`proofAfterDeferrals`), so where the lock is a commit vote of a revision-chained T that is
  applied at some stores and not yet resolved at the lock holder, the proof is a T2 of 2.3: if its outcome reaches
  the holder first, it waits for the holder's vote in lock order (2.3), so T applies first.
* A repair write never overwrites a different occupant of an FSIF slot (`RepairSlotOccupied`); the repairer drops a
  dead occupant with an id-checked `repairDelete`, only when the AllocationTree does not list it.
* A failed repair write restores the fields it changed to the settled pre-repair state, the backend's
  (`Frontend.repairWriteComplete`), keeping the instance lock holders reference, so their committed apply holds. A
  repair of an object the store did not hold discards its state.
* A data copy is installed only from a read no older than the store's copy, or of equal timestamp and a different
  revision (`Reconcile.planData`): a refcount or key-level write moves the timestamp without the revision, so the
  revision decides. That rule overwrites a store's copy only with what a read majority holds, which includes every
  committed T because its commit voters apply in lock order (2.3).

### 2.5 Allocations and proofs of deletion

* No allocation can be built without its `AllocationFinalizationAction`, so the AllocationTree eventually lists every
  committed allocation that is not proven deleted when the action reads it; a listed object may already be deleted.
* `InvalidObject.provesDeletion` (`BaseObjectReader.restore`) needs more than `w - r` error replies and more than
  `w - t` of them absences (`ObjectNotFound`, `ObjectMismatch`); `AllocationPending` and `StoreNotFound` replies are
  re-read, never counted. A commit voter of an unapplied allocation that still holds its vote answers
  `AllocationPending`; one that lost its vote (a rebuilt store, a lost or recreated CRL) answers `ObjectNotFound`.
  With the `w - t` non-voters absent too, a false proof needs more than `t - r` voters without their vote: beyond
  the data tolerance, where the object is unrestorable anyway. Within it the proof is sound for every consumer:
  repair (`StoreRepairer.repairAbsent`, `repairListed`), the rebuild's pending sweep, and the allocation and TKVL
  split finalizers (`AllocationFinalizationAction.execute`, `SplitFinalizationAction`), which record nothing for an
  object proven deleted.
* Repair never takes absence from the tree as a deletion: only `InvalidObject.provesDeletion` drops a slice.
* A rebuild covers allocations the lost store acknowledged but the tree does not yet list by asking `w - t + 1`
  peers for their unfinalized allocations and checkpointing the union (rebuild-process.md 5.3).
* A replayed allocation never overwrites a live object (`ObjectExists`). With no tombstones, an id whose deletion
  persisted can be allocated again by a store that never saw T (transaction-requirements.md, deletion and
  allocation).

### 2.6 Client-held state

A client keeps no object cache. Every allocation takes a fresh random id, so a stale pointer never names a newer
object, and FSIF answers `ObjectNotFound`/`ObjectMismatch` to a stale locator. Every requirement on a missing or
refcount-0 object fails, so a stale pointer cannot revive a deleted object through a zombie slice; the zombies are
fewer than the write threshold. TKVL upper-tier caches and application caches write under revisions. The status
cache holds outcomes, never object state, and is monotone (`TransactionStatusCache.updateStatus`); a size eviction
raises the host's eviction watermark, so an evicted entry's absence is never trusted. A relay from the cache
(`Store.handleTransactionMessage`) runs before the forget-floor check and is trusted as far as the entry is: a
non-finalized `Committed` entry for a T every peer forgot makes the relay answer non-late (R-W5).

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
at load, applied stubs) are bounded by the transactions already admitted, plus 41 bytes per unoffset stub
(crash-recovery-log.md, Lemma C), and one transaction's bytes on a store by `maxTransactionBytes`, past which its
prepare is dropped (crash-recovery-log.md section 6, Oversized). What can still fill a lane is the tier's own traffic,
including same-tier finalizers (TKVL split, join, tail removal): ordinary back-pressure that clears as the tier's
transactions finalize upward. Data-only devices give up a quarter of their headroom (Backlog.txt).

**State kept outside admission's turnover.** A transaction's CRL state kept for a failed backend commit or a
quarantine stays charged for the life of the loaded store, or until the quarantine clears; a quarantine that never
clears (a deleted object, RP-W4) holds it indefinitely.

**Repair backlog.** ErrorTree and UnackedCommits entries cost backend space in the metadata pools, not CRL bytes; an
entry occupies CRL admission only while the batch writing it is in flight. The backlog drains one entry at a time per
store, a timed-out scan that saw entries stays at the scan floor and keeps its deferrals, and a silent voter's
UnackedCommits entry costs one delete. No argument bounds the metadata-pool space the backlog holds during a long
outage.

## 4. Clocks

Every clock comparison that crosses subsystems, and what bounds it:

| Comparison | Clocks | Bound |
|---|---|---|
| Forget floor time leg (`now - horizon`) | the store's own wall clock against T's start | same host only; a backward step of more than about 8 s reopens the window (paxos-transactions.md, accepted limitations) |
| `MaxStartLead` mute and the join ceiling | host wall clock | caps every accepted start, and so every object timestamp and client HLC, at the fastest host plus 2 s |
| Arrival margin, rebuilt store's `acceptor-since` (10 s) | old host against new host | inter-host skew below 8 s |
| Inherited-vote gate (`inheritedVotesAfter`: pass start + horizon + 20 s) | rebuilding host against every voter's host | the lead plus one skew, and a second skew |
| Expiry (`Frontend.pastExpiry`, freeze) | each store's own clock | stores freeze at different moments under skew; the 8 s margin covers the inter-host skew, so an expiry commit exists only past the horizon on every clock (late condition 1). A backward step after a freeze thaws the Tx, breaking the tally's "reported states are final" premise (accepted, paxos-transactions.md) |
| Repair age gate (60 s) | repairing host's HLC against the marker's start | paces only; the read decides a deletion |
| Repair `mayJoinTransaction` | the store's own floor | same host |
| Repair completion gate, quarantine verification | object timestamps, no clock | object timestamps never decrease; a timestamp past T's start does not show T was applied, so the gates rely on T's commit voters applying T before any later write over its locks (2.3) |
| Rebuild pending-allocation coverage | none | the checkpointed union of listed allocations |

---

## 5. Contradictions

None.

---

## Open weaknesses

None.
