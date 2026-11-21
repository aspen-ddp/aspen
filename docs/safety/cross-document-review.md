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
| CRL | Store lifecycle | Entries leave the device CRL only after another durable copy exists or the store is unservable on disk; a store whose entry still names the device keeps them (2.2) |
| Transaction requirements | CRL, recovery | Recovered commit votes, unresolved or with a restored committed resolution, re-take their locks and slice reservations before any message is handled, and a restored one holds them until it applies (transaction-requirements.md R13) |
| Transaction requirements | CRL, recovery | A key's deletion (commit or repair write) reaches the backend only once no CRL state holding an applied blind Insert of the key remains (`Frontend.awaitBlindInsertsDurable`), so a recovered Insert of an absent key was never applied |
| Client reads | Requirements | Every commit voter applies a committed update or keeps its lock, applying in lock order (transaction-requirements.md 8.3); at most `w - r` copies lack it. A recovered voter whose re-check fails holds it already (2.3) |
| Transaction requirements | Repair | Every repair write, and every complete reconcile pass that changes nothing (at its entry's minimum timestamp), tells every Tx of the object started before the timestamp it records (`Frontend.recordRepairInstall`, `Tx.repairInstalled`), so a blind Insert of an absent key applied after it reports the object; a non-voter's blind Insert of an absent key reports its object also where a committed Delete of the key started at or after its start applied here (`Frontend.keyDeletedSince`), and only while the Tx is in `Frontend.transactions` and its marker has not written; a later apply relies on the store's ErrorTree mark, which no reconcile of an object it changes consumes while its commit is held, waits in lock order, or waits for the save of object data it adopted from a late client prepare (`Frontend.settledForRepair`); a recovered Tx without a commit vote applies nothing on a commit and reports every object it changes (`Tx.doCommit`), since it may have acknowledged T before its crash and left no mark |
| Requirements (lock order) | Recovery | Every recovered commit vote (unresolved, or with a restored committed resolution) has relocked before a recovered Tx that does not relock applies (`Frontend.recoveredTransactionLoaded`, transaction-recovery.md 7.2) |
| Requirements (slice limit) | Repair, rebuild | The commit votes a store holds on a KeyValue object fit the KeyValue limit together (`Frontend.maxKVSliceSize`, `ObjectState.sliceReservations`, R16); a repair install keeps them fitting (`Frontend.repairFitsReservations`, repair-process.md M6a), and the union of every committed update, which repair and rebuild install unchecked, fits `maxSliceSize` because the KeyValue limit is `t / w` of it (R16), a rebuilt store admitting no growth of an object it may hold without an inherited commit until repaired (2.3) |
| Client reads | Requirements, rebuild | `InvalidObject.provesDeletion`: a commit voter of an unapplied allocation answers `AllocationPending` (2.5) |
| Transaction recovery | Repair | ErrorTree and UnackedCommits entries are consumed only once the store can no longer join T, and only by the store the pool names |
| Transaction recovery | Metadata pools | Marker batches commit (finalization waits for them) |
| Allocation and split finalizers | Client reads | A read proving deletion means the object is not live, so it needs no tree entry (2.5) |
| Repair | Paxos, requirements | A read's unlocked parts reflect every committed transaction (quorum overlap plus lock reporting) |
| Repair | Rebuild, transfers | The ownership fence (host and device) keeps superseded copies from consuming entries |
| Repair proofs | Requirements | A committed proof's requirements cover every part the reconcile then treats as proven: the object level (refcount and revision) by one transaction requiring both (repair-process.md 8.3) |
| Rebuild | Transactions | `CurrentAllocationsRequest` lists every unfinalized allocation and every held commit vote (`HeldVote`); reads of the pool's objects complete |
| Rebuild | Repair | Everything the store missed while rebuilding is marked and repaired after promotion |
| Rebuild (reassignment) | Host device lifecycle | The tombstoned device's former host stops serving its stores before the drain rebuilds them elsewhere: one best-effort nudge, else its hourly check; a running host that misses the nudge serving through the rebuild is an accepted limitation (rebuild-process.md B19) |
| Transfer out (closed store) | Host loader | The store stays closed from its extraction until its `transferring-out` marker is written (an open meanwhile fails the extraction, or stops `startOut` in the task that would write the marker), and the extraction fails on a failed stat of the directory or save file rather than take it for an absence (crash-recovery-log.md 3.5) |
| Rebuild completion | Transfers | A flip that finds the entry already Active adopts the copy without checking its episode (`DeviceMetadata.markRebuiltStoreActive`); no transfer starts onto an occupied store path, so the copy is this episode's (2.2). A flip retried after `TransactionOutcomeUnknown` that finds the entry moved on (TransferringOut, absent) keeps the copy offline rather than discarding it (`Unconfirmed`, rebuild-process.md 5.7) |
| Rebuild, CRL | Store lifecycle | A rebuilt copy's first load recovers nothing an earlier copy left under its id in the device CRL: it is a save-file load of the empty state `RebuildingStore.finish` writes, which drops those entries first (rebuild-process.md 5.6) |
| Lost device CRL | Rebuild | A finished rebuild awaiting its flip (`rebuild-complete`) has never served, so it holds nothing in the device CRL and is not marked (`DeviceDiscovery.holdsOwnState`) |
| Status-cache relay | Finalization actions | Every shipped action is safe to run again after T was finalized and forgotten, which a second driver's resent `TxResolved` makes happen (paxos-transactions.md 10.5; 2.6) |
| Expiry adoption | Recovery | An outcome a status reply reports non-late stems from Paxos, a relay or an expiry commit; a tree-learned resolution is reported `Unresolved` until finalized (`Tx.statusForExpiry`) |
| Expiry tallies | Recovery | A reported `votedCommit` stands for a voter holding T's locks, or for a committed T (a tree-learned or not-owed resolution, whose locks are released) (2.1) |
| Host load paths | Store lifecycle | A store whose entry names the device is served, offline under a pass, or retried (2.2); one whose directory or config is missing is rebuilt, its device-CRL entries kept (2.2). A device load serves every store directory its markers do not hold back without reading the tree, so the markers, the save file and the incarnation file are read with stats that tell a failure from an absence (`server.FileStat`), and an incarnation file is never written by a load (2.2) |
| CRL admission | Host reconcile | A retained charge is released only once the records it covers have left the CRL or been counted again; the leftover drop releases only the charge it was issued for, unchanged since |
| Systemic-loss protection | All of the above | Declarations of loss are independent and true (they are not checked; see that document) |

## 2. Cross-subsystem invariants and the guards that hold them

### 2.1 Forgetting a transaction

A store forgets T in exactly these ways: a committed T at `TxFinalized` (after its backend commits), an aborted T's
CRL record at resolution and its Tx at `TxFinalized`, a late outcome, an expiry discard or conclusion, a muted Tx once
its objects load, and a host restart, transfer or adoption (memory lost). Every one is covered by the forget floor:
once a store has forgotten T it refuses every prepare for T (paxos-transactions.md, transaction horizon), and a
prepare for a T whose finalized outcome the host caches creates no Tx (`Frontend.receivePrepare`). A store never
rejoins a transaction it forgot, so Paxos sees a forgetful acceptor as an offline one. The one Tx created below the
floor is a rebuilt store's muted Tx for a T started before its acceptor (`acceptorSince`): it promises, votes and
applies nothing (`Tx.muted`), at most proves the commit to its driver, and is forgotten once its objects load unless it
proved it (`Frontend.forgetMuted`); a prover is forgotten at `TxFinalized`, a late outcome, or its expiry discard
(`Frontend.expireTransaction`), so repair's skip of T's UnackedCommits entry while it exists (`holdsTransaction`)
ends.

The premises other subsystems take from this:

* **Finalization precedes forgetting a Paxos-chosen commit.** `TxFinalized(commit)` is sent only after every
  finalization action completed, the missed-update marker last, so every ErrorTree mark and UnackedCommits entry of
  T is durable before any acceptor forgets T (transaction-recovery.md, finalizer). Late resolution
  (`TransactionDriver.resolveLate`) runs no actions on the strength of this, and finalizes with empty `owed` even when
  it cancels a running marker that may still write entries: the first `TxFinalized`'s `owed` is the authoritative stub
  list, a store outside it acknowledged that driver and holds no Tx to consume an extra entry, and repair's reconcile
  of a copy that holds T is a no-op. A non-late commit can still reach a driver after T was finalized and forgotten,
  from a second driver's resent `TxResolved` (paxos-transactions.md 10.5), and its finalizer then runs T's actions again until a late answer
  cancels it; every shipped action tolerates that (2.6). A `TxResolved` reaching a store after its `TxFinalized` (an
  abort's broadcast is queued after it) creates no Tx and only refreshes a monotone status-cache entry
  (`TransactionStatusCache.updateStatus`).
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
  replay (a failed backend commit, `Frontend.TxCommits.failed`) says so in `TxCommitted.keptForReplay`, and
  `MissedUpdateFinalizationAction.writeMissedUpdates` gives it an entry, so a reload after `TxFinalized` re-applies
  the kept state through it, retrying the commit.
* **A reported commit vote holds its locks, or its transaction committed.** Expiry rules A and F count each
  `votedCommit` as a voter holding T's locks (paxos-transactions.md, expiry). Two reporters hold none: a Tx resolved
  from its UnackedCommits entry (`resolved(record = false)`, `resolvedFromTree`), which reports its saved status
  (`Unresolved`, with its vote and accept) until finalized, and a recovered or expiring vote whose entry names another
  incarnation (`Tx.unackedCommitNotOwed`, `notOwedHere`), which takes the same path; both released their locks on
  resolving. Both exist only for a committed T (the entry exists), so a tally they push toward commit decides rightly.
  Such a Tx answers before it is frozen, the one exception to "reported states are final": its vote is fixed, and
  since no abort is ever accepted for a committed T, its accept can only move toward commit. An observer that counts
  it and still discards T is a commit voter whose own lookup found no entry, or a non-voter, which the finalizer's
  marks repair (transaction-recovery.md, expiry).
* **A save acknowledged without being written belongs to a concluded transaction.** The CRL skips a save of an id
  that an unwritten entry deletes finally and acknowledges it once the deletion is durable (crash-recovery-log.md C1,
  C9; `SimpleCRL.skippedAfterFinalDeletion`). A store queues a final deletion of T (`crl.deleteTransaction` in
  `Frontend.concludeTransactionState` or `deleteTransactionState`) only once it has concluded T: aborted, finalized,
  a late outcome or an expiry discard. A Tx of T that saves after that
  point (the live Tx with `keepAdmission`) works on a transaction whose outcome this store has already concluded. If
  the store then crashes, its reload floor refuses T, which makes it an offline acceptor of a decided transaction;
  the promise, accept or vote it acknowledged decides nothing that is not decided already.
* **Repair waits until the store cannot join T.** An entry is left alone while T's start is at or above the store's
  forget floor (`RepairTarget.mayJoinTransaction`) and while the store holds a Tx for T or is loading its CRL
  (`Frontend.mayHoldTransaction`), so no Tx can apply T after a reconcile and have its entry deleted under it. A Tx
  finalized while its commit is held has left `transactions` (`Frontend.finalizationDeferred`), so
  `mayHoldTransaction` is false for it, and a reloaded store's floor covers it; repair still cannot lose its update. A
  commit held for its turn is in `heldCommits`, part of `commitInFlight`, and a commit waiting in lock order is in
  `lockWaitingCommits`, each under every object it changes wherever the locks or commits it waits for lie, so those
  objects are unsettled (`Frontend.settledForRepair`) and no reconcile installs or completes there until it has
  applied. A committed non-voter waiting for object data it adopted from a late client prepare to be saved
  (`Tx.commitAwaitsData`, `adoptedDataUnsaved`), which `TxFinalized` can also defer, is in `adoptedDataSaves` under
  every object it changes from the save's issue until the save completes and the commit has applied or taken one of
  the other two holds (`Tx.saveAdoptedData`, `Frontend.adoptedDataSaved`), so its objects are unsettled meanwhile
  too. A recovered vote with a restored resolution is safe either way: its kept CRL state applies it again after a
  reload without the entry.
* **Kept state after finalization is concluded only with T decided.** `Frontend.concludeTransactionState` with
  `keepAdmission` (a replay's Tx is live) replaces or deletes the CRL record that live Tx shares, so its later
  promises and accepts are acknowledged unwritten (C9). It runs only for a finalized T (after its backend commits),
  so the live Tx's acceptor state chooses nothing new: while the store runs it is in memory, and after a crash the
  reload floor refuses T, a crash-stop acceptor of a decided instance.
* **Applied stubs outlive every Tx that could consult them.** A stub is dropped only once T is below the floor and
  the store holds no Tx for T (`Frontend.expireStubs`).

### 2.2 Store identity: restarts, transfers, adoption, rebuild

* Every restart, adoption and transfer-in builds a new Frontend, whose load floor (load plus `MaxStartLead`, the
  host's join ceiling, or the arrival margin of `AcceptorSinceMargin` after a move between hosts) covers every T the
  store may have joined and forgotten in its previous life. `StoreLoader.ArrivedAtFile` is written durably before a
  moved store can load. The new load floor may sit below the old Frontend's floor (an arrival margin just past, a
  stalled ceiling renewal), which is sound: it bounds every T the previous life joined, and the old Frontend refused
  everything below its own floor, so it joined nothing in the gap; repair's `mayJoinTransaction` consumes only entries
  of transactions the store joined. Every in-memory store state (`failedCommits`, inherited-vote progress) is lost
  with the Frontend; only what the CRL state, the save file and the store directory's markers carry survives.
* A rebuilt store has a fresh `incarnation` and an `acceptor-since` (finish plus `AcceptorSinceMargin`, 10 s), both
  written durably before the rename that makes the directory appear, and both carried by transfers. Before
  `acceptor-since` it promises, votes and accepts nothing and answers expiry queries `amnesiac`, never as a non-holder
  (`Frontend.mayHaveForgottenUndecided`). UnackedCommits entries naming another incarnation apply nothing at it; their
  reconcile installs the committed state. That rule needs a store that was not rebuilt to keep its incarnation:
  every store directory gets its file when it is made (`StoreCreation`; a rebuild's `finish` replaces it), and a load
  only reads it (`StoreIncarnationFile.load`). One known absent has the store rebuilt (`retireUnserved`); one that
  cannot be stat'd, read or parsed holds the load back for a retry and is never replaced.
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
  holding a stale store-to-host mapping (the no-fencing trade-off accepted for adoption). A running, reachable old
  host that misses the tombstone's single nudge is outside that trade-off: it serves the device's stores, voting and
  acknowledging, until its hourly check while the drain rebuilds them elsewhere, and peers keep routing to it until
  the flip's `StoreMoved`. This is an accepted limitation, deliberately not addressed in code
  (rebuild-process.md B19). The rebuild's inherited-vote gate and `acceptor-since` assume
  the old incarnation stopped joining at the rebuild's start; there it does not.
* A device whose CRL fails is disowned and unloaded until the host restarts, its rebuilds left running
  (`DeviceShutdown.crlFailed`). A rebuild that finishes meanwhile flips its entry Active (`Adopt`) and its load finds
  no device, so that store is Active and unserved like every other store of the device, which the pool already
  counts as unserved since the device's stores were closed and gossiped offline; the restart loads the copy, whose
  empty `crl_save.log` drops any earlier entries. No state is lost and nothing is served twice: the cost is the
  device's outage, which the rebuild does not widen.
* `Store.close` and a reload over the same backend stop every driver of the store (`Store.shutdownDrivers`); a
  finalizer already running completes its idempotent actions and does not restart. A fail-stopped store's drivers run
  until its host's unload closes it, and `SimpleDriverRecoveryMixin` may start more meanwhile (`Store.driveTransaction`
  has no `failStopped` check, unlike `Store.heartbeat`): a proposer holds no acceptor state, so it decides only what
  the other stores' votes and accepts allow, its own store drops every message, and its finalizer marks that store.
* A store whose entry names this device is served, or is in the device's `offlineStores`, where a reconcile pass (lost
  CRL, abandoned transfer out, cancelled shutdown, completed transfer in or rebuild) or a load retry takes it up. A
  backend open that fails for anything but a lost state (out of space, an I/O, lock or resource error) leaves the store
  offline and retries the load at the device scans with backoff (`StoreLoader.storesAwaitingRetry`, `retryFailedLoads`,
  capped at `Host.LoadRetryCap`); a create that left nothing at the path is created again by the next device check
  instead; a retry whose store config cannot be read keeps its entry and tries again at the next scan. A rebuilt copy
  whose `rebuild-complete` marker cannot be removed after its flip is kept in `offlineStores`
  (`keepUnpromotedCopyOffline`) and in `StoreRebuilds.pendingMarkerRemovals`, so the Completed Rebuilds pass retries the
  removal and the load at each device check, including after an unlink whose directory sync failed. A transferred-in
  copy's marker is removed by a plain unlink after a durable `ArrivedAtFile`
  (`StoreTransfers.clearTransferredInMarker`): a crash may bring the marker back, and the Completed Transfers In pass
  then loads the copy again. A failed `crl.loadStore` (`UnloadFailedStore`) is not retried, but it fails only with the
  CRL itself, whose failure unloads the device (`DeviceShutdown.crlFailed`). A store whose `config.yaml` cannot be read
  by `StoreLoader.tryLoadStore` (`awaitUnreadRetry`), or whose load finds its executor slot still held
  (`HostThreading.AlreadyAssigned`), is left offline and in `storesAwaitingRetry` like a failed open, its `onServed`
  kept for the load that serves it. A load's `onServed` (the `StoreMoved` gossip of `DeviceShutdown.reloadClosedStores`
  and `StoreTransfers.reinstateAbandonedOut`) runs only once its LoadStore has installed and recovered the store
  (`StoreLoader.awaitServed`); a LoadStore that leaves it offline with its data intact (no CRL room, no executor slot, a
  save file whose removal failed, a Store whose build throws) hands the callback to its `storesAwaitingRetry` entry,
  and any other failure drops it, so peers never clear their offline mark for a store left offline. An Active entry on a loaded
  device with no `config.yaml` at its store path (the directory lost to a bad restore, a partial mount or a hand
  removal, or its config gone), not served, opening or retiring, is found by the reconcile's Missing Stores pass
  (`DeviceReconciler.reconcileDeviceState`): the store is marked offline and a rebuild requested
  (`StoreRebuilds.requestRebuild`). Its device-CRL entries stay (below).
  `tryLoadStore` logs a store directory without a config at error, and an unreadable config is logged at error once per
  retry cap. This matters because `StoreCopies.release` deletes the superseded pending-delete copy once the entry is
  Active, whether or not the new copy is served: a store left in neither state would consume one unit of the pool's `w -
  t` margin for good.
* A store's device-CRL entries never leave while its entry names the device and no retire has saved them: the leftover
  drop (`dropLeftoverCrlState`) takes only a store its device entry does not name. A store the Missing Stores pass
  declares has nothing durable of that declaration, and its directory may come back (a restore or mount finishing late)
  before its Rebuilding flip commits, or after it and before the first reconcile's `retireForeignCopy`; a device load
  opens it then (`DeviceDiscovery.loadOwnedDevice` offers every child to `tryLoadStore`, which reads no tree), and it
  recovers its own state. Its entries go when the rebuilt copy's empty `crl_save.log` is first loaded (5.6 of
  rebuild-process.md), when a retire saves them into a copy, or as leftovers once a re-placement takes the entry off the
  device.
* Everything a device load serves is decided by files in the store directory alone: the markers `rebuild-required`,
  `transferring-out`, `transferred-in`, `rebuild-complete` (`StoreLoader.tryLoadStore`), the save file and the
  incarnation file (`StoreLoader.handleLoad`). Each is tested with `server.FileStat`, which answers absent only for a
  stat that does (ENOENT, or a parent that is no directory) and throws otherwise, so a failed stat holds the load back
  (`awaitUnreadRetry`, with backoff) instead of serving a store without its markers or state. The passes that act on an
  absence use it too: the Missing Stores pass and the leftover drop skip a store whose stat fails, a retire fails
  rather than report a clear path, the Deleted Stores pass keeps the store offline, and the device CRL's open leaves the
  device unloaded for the next scan rather than declare a `crl/` lost (a child whose config cannot be stat'd counts as
  a store). Existence tests outside the load still use `os.exists`/`os.isDir`, which answer false on a failed stat.
  Most only delay: a marker a reconcile pass or `reloadClosedStores` looks for (the load they lead to tests again with
  `FileStat`), `adoptionClass` (an `Unclean` refusal, or a marked copy classed `Clean` that its load then keeps
  offline), `retireForeignCopy`/`discardLostStore` (a rebuild whose finish cannot move onto the occupied path),
  `startIn` (a transfer whose promotion fails on the occupied path), and `openDeviceCrl`'s move of `crl/` (a
  replacement whose rename fails on it). `StoreTransfers.extractUnloaded` tests the directory and the save file with
  `FileStat`, failing the extraction on a failed stat. One acts on the absence: `StoreLoader.readArrivedAt`, whose
  false absence drops the arrival margin from a moved store's load floor (X-W26).
* A lost device CRL marks every store directory except those that held nothing in it (`DeviceDiscovery.holdsOwnState`):
  a store whose valid `crl_save.log` is its whole state, and a finished rebuild awaiting its flip (`rebuild-complete`),
  which `tryLoadStore` never serves, so its flip or the Completed Rebuilds pass loads it over the fresh CRL. A copy
  whose marker removal left it offline with no marker holds nothing either but is marked; that only costs a rebuild.
* A rebuild's flip that finds the entry already Active returns `Adopt` without checking the episode
  (`DeviceMetadata.markRebuiltStoreActive`), and `AdoptOffline` likewise for an Offline entry. That is sound because a
  rebuild still running for a superseded episode is cancelled when the reconcile sees its entry move, and no transfer
  can bring the store back onto this device while a copy occupies its path: `StoreTransfers.startIn` refuses an
  existing path, except an earlier tenancy's `rebuild-complete` or `rebuild-required` copy, which it retires before
  asking for any bytes, and not while a rebuild of the store still runs here (rebuild-process.md FS18). So an Active
  entry here over this rebuild's copy was made Active by this episode. The retire waits for a copy served from the
  destination's path or a store opening, never for the transfer's source served from another device of this host,
  which closes only for the StartStoreTransfer sent past the retire; a marked copy's load leaves the store served there
  unsilenced (`StoreLoader.keepMarkedCopyOffline`), and a removed copy clears the router's offline mark only when no
  other device here holds the store offline (`StoreRouter.clearOfflineUnlessHeld`). A rebuilt copy carries an empty
  `crl_save.log` (`RebuildingStore.writeEmptyCrlState`), so its first load (by the flip, the Completed Rebuilds pass,
  a restart, a reinstatement or an adoption) drops whatever device-CRL entries an earlier copy left under its id
  (`StoreLoader.awaitCrlLoad`), and a transfer out of the unloaded copy ships the file and drops them
  (`StoreTransfers.extractUnloaded`). Such a transfer (an `Unconfirmed` copy whose entry went TransferringOut) ships
  `rebuild-complete` too, which the destination removes when its pool update commits, before the load
  (`StoreTransfers.recordTransferIn`), since its entry is then Active; a failed removal leaves the copy offline for the
  Completed Rebuilds pass, as for `Adopt`.
* Host-side client work (UnackedCommits lookups, held-vote queries, the repair service) runs
  on the host's `ec`, not a store executor or the control thread. It keeps no host state of its own beyond what it
  locks (`DeferralState`, `StoreRepairer`'s counters), reads store state only through `@volatile` snapshots
  (`Frontend.mayHoldTransaction`, `RepairTarget.mayJoinTransaction`, `localDevice`), and reaches a store only by a
  request posted to its executor whose answer completes a promise (`Frontend.post`, `RepairTarget.snapshot`,
  `reconcile`), so the single-owner rule of `server/CLAUDE.md` holds.

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
  re-check. Reads choose the revision by count alone (`BaseObjectReader.getStoreStates`, per key in
  `KeyValueObjectReader.resolve`, where only a lock held by the transaction of a revision present defers the answer)
  and rely on T's `t` commit voters holding T or its lock. The lock order guarantees that for a live voter. For a
  recovered voter too: every recovered commit vote relocks, one whose committed resolution was restored included, which
  holds its locks and reservation until it applies (transaction-requirements.md R13), and a recovered Tx that does not
  relock (an abort vote, no vote, a restored resolution without a commit vote) completes its load only after every
  relocking commit vote has relocked (`Frontend.recoveredTransactionLoaded`), so its apply waits for them in lock
  order, and reads see the lock of a voter that has yet to apply. Where the store in both write quorums
  saw the newcomer first, the order is reversed here; every key Delete carries a `KeyRevision` or a content lock and
  every Insert a check on its key, so the newcomer's re-check fails after the holder's change and it is reported
  (transaction-requirements.md R15). A commit voter whose commit is held for its
  turn keeps its locks after a `TxFinalized` takes it out of `Frontend.transactions` (`finalizationDeferred`); a
  lock-order waiter that then skips it (`Frontend.holdsLocks` is false) is still queued behind it on every object both
  change (`heldCommits`, `holdForTurn`), so it applies first. A prepare built on it never meets its locks: it builds on
  an object the held commit changes, which is in `heldCommits` and so in `commitInFlight`, and a vote touching such an
  object waits, its requirements unchecked, until the commit persists, by when the held commit has applied and
  released its locks (`Tx.determineDisposition`). A recovered voter whose re-check fails therefore
  holds T already (transaction-requirements.md 8.5); for a refcount the converse, that a re-check passing means T is
  not applied, holds only because every shipped refcount change advances `ObjectRefcount.updateSerial`, so no later
  write returns the refcount to T's required value (X-W27). One
  whose Insert of an absent key passes never applied it: a key's deletion reaches the backend only once no CRL state
  holding an applied blind Insert of the key remains (`Frontend.awaitBlindInsertsDurable`, transaction-requirements.md
  9.4). A live voter's failed re-check is an invariant violation, logged and reported.
* Taking a recovered vote's failed re-check as applied assumes nothing wrote the object at this store between the
  crash and the relock. Nothing can: the backend is unserved until the new Frontend loads, and while it is `holding`
  it handles no transaction message, drops client reads, and parks every repair (`settledForRepair` is false); a
  transfer closes its source before the copy ships, and an adopted device's stores load the same way.
* `awaitBlindInsertsDurable` takes a covering Tx that voted commit as holding its update (`Tx.holdsUpdateOf`) and saves
  its state without the update, although it may not have applied yet. That is sound because no deletion of the key
  applies at this store before that vote's own apply: a transaction deleting it votes abort on the vote's key lock
  here, and its commit, holding no locks here, waits in lock order (`Tx.awaitLockHolders`); a repair defers every key
  the store's own locks cover (`ObjectState.lockedKeys`, `Frontend.reconcileSettled`); a recovered deletion waits for
  the recovered vote to relock. The one exception, a recovered vote sharing the lock cast after T's update persisted
  (transaction-requirements.md 10.5a), is one whose update is applied.
* A lock-order wait has no timer, and while it lasts every new vote whose locks the waiter collides with waits
  (`Tx.lockWaitBlocked`); other votes decide at once. A colliding vote waits at most `Tx.CollisionWait`, then votes
  abort naming the waiter, so waits at two stores that withhold the votes each other's holders need end in about
  1.5 s: each holder's driver then sees an abort vote and decides (transaction-requirements.md 8.3). A holder that
  cannot decide otherwise ends by expiry (`ExpiryTally.decideUnknown`), and one whose tally is undecidable holds the
  waiter as long as its own locks (transaction-requirements.md section 12).
* A vote is withheld only for a waiter that would wait for it: one changing none of the waiter's objects (lock-only
  there, such as a repair proof or a vdisk fence lock) decides at once (transaction-requirements.md 8.3). That also
  guards a restored commit voter: once the store stops `holding`, it is issued, held for its turn (`commitInFlight`
  withholds every new vote on its objects and unsettles them for repair) or waits in lock order (`lockWaitBlocked`), so
  no new transaction, repair proofs included, commits over it at this store, and its relocked locks (R13) show reads
  and repair that it has yet to apply.
* A lock-only vote is not waited for, and a repair install may change an object under one: shared revision locks are
  outside `ObjectState.objectLevelLocked`. It has no update to lose; its skipped re-check is reported
  (transaction-requirements.md 8.5), and the shared lock did its work during the vote.
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
* The same votes count in the slice-limit bound (transaction-requirements.md R16), which counts each store once: a
  KeyValue growth T the old incarnation voted for that commits after the rebuild read its object is counted at the
  old incarnation, and the rebuilt copy lacks it until repair (a resolution carries no data). So a rebuilt store
  holds **inherited growth guards** (`Frontend.inheritedGrowthLock`): it votes abort (`InheritedGrowthLock`) on any
  update that grows a guarded KeyValue object, until a complete whole-object repair from a snapshot taken after the
  guard was set (`RepairSnapshot.takenAt`), whose read followed T's commit and so reflects it (repair-process.md
  E1). The first held-vote round finds what to guard: the objects of the transactions its peers report unresolved
  (guarded once released) or committed and not finalized (`HeldVote.committed`; each commit voter holds a committed
  Tx until TxFinalized), and every KeyValue object with an ErrorTree or UnackedCommits entry against the store, read
  after the peers answered (`StoreRepairer.inheritedGrowthScan`): a T finalized before the round had its finalizer
  mark the store, which never acknowledged it, first. The scan marks the reported transactions' objects itself, so
  every guarded object has an entry whatever T's outcome. A repair that cannot release a guard, or of a KeyValue
  object under the inherited-vote lock (every object until the round completes), answers stale, so the entry
  outlives the guard; the inherited-votes file stays while any guard does, and a reload repeats the round
  (RebuiltVoterLockIntegrationSuite, FrontendSuite). A guarded object whose deletion persists (committed, or a
  repaired missed deletion) has its guard end then, since no repair of it follows. The scan's reads of the trees
  have no deadline (the held-vote query has `Host.HeldVoteQueryDeadline`); every object stays under the
  inherited-vote lock until it answers, which it does within the write-threshold margin: the trees' reads complete
  with `r` stores of the pool above, its marks are `MissedUpdateMarker` batches written to commit in that pool, and
  any failure retries the round. The scan's key range `[poolIndex, poolIndex + 1)` is never empty: every IDA's width
  is at most `IDA.MaxWidth` (127), so the bound never wraps, and no two stores of a pool share an id.

### 2.4 Repair and transactions on one object

* A repair install is an in-flight write of the object (`Frontend.reconcileSettled`): transactions wait behind it as
  behind a commit, and it is refused while a commit is in flight, held, failed or being deleted.
* Repair proofs are untracked, `replaySafe` transactions; every requirement on a refcount-0 or missing object fails
  `MissingObject`, so no proof revives a deleted object. The object-level proof
  (`RepairProofs.proveObjectLevel`: `RefcountUpdate(rc -> rc)` beside a `SharedRevisionLock` of the read's revision,
  repair-process.md 8.3) and the key proof (a KeyValue update with no operations,
  `KeyRevision` or `DoesNotExist`) are real writes: each changes the object (a backend commit, the object timestamp raised to the proof's start) and, once committed, applies at the stores that
  voted abort on it once the commit votes it collides with there have resolved. A proof is attempted precisely when
  an object stays locked across passes (`proofAfterDeferrals`), so where the lock is a commit vote of a
  revision-chained T that is applied at some stores and not yet resolved at the lock holder, the proof is a T2 of
  2.3: if its outcome reaches the holder first, it waits for the holder's vote in lock order (2.3), so T applies
  first.
* A repair write never overwrites a different occupant of an FSIF slot (`RepairSlotOccupied`); the repairer drops a
  dead occupant with an id-checked `repairDelete`, only when the AllocationTree does not list it.
* A failed repair write restores the fields it changed to the settled pre-repair state, the backend's
  (`Frontend.repairWriteComplete`), keeping the instance lock holders reference, so their committed apply holds. A
  repair of an object the store did not hold discards its state.
* A repair write that removes keys waits, as a commit's key deletion does, for every covering blind Insert's CRL
  state (`awaitBlindInsertsDurable`, 2.3), and every repair write records the object timestamp it installs
  (`Frontend.recordRepairInstall`) for the Txs of the object started before it, so a non-voter's later blind Insert
  of a key the install removed reports the object (transaction-requirements.md 9.4). A pass that finds the store's
  copy already equal to the read writes nothing but, when complete, records its entry's minimum timestamp all the
  same, since the store may lack a deletion the read lacks too, and that deletion started no later (repair-process
  6.2). A non-voter still holding T, which held no key lock that
  kept a deletion out, also reports a blind Insert of an absent key where a committed Delete of the key started at or
  after T's start applied here (`Frontend.keyDeletedSince`), and a recovered non-voter (after a restart, a transfer or
  an adoption) applies nothing on a commit and reports every object it changes (`Tx.doCommit`). These notices reach
  only Txs in `Frontend.transactions`, and a report only a finalizer still running: a commit applied after T's
  finalization relies on the store's ErrorTree mark, which no reconcile of an object it changes consumes while the
  commit is held, waits in lock order or waits for its adopted data's save (`Frontend.settledForRepair`). A recovered
  non-voter needs no mark: once T is finalized it either acknowledged T in its earlier life, so its backend holds the
  update, or the finalizer marked it.
* Rebuild installs a KeyValue object's read state with no size check (`RebuildingStore.restore`); repair checks it
  only beside held votes' reservations (`Frontend.repairFitsReservations`). A committed apply never exceeds the limit
  at a commit voter, since the votes a store holds fit together (R16) and a repair install that would break that is
  stale (repair-process.md M6a). The read state is the union of updates committed by different voter sets, each
  checked only at the stores that voted it; it fits `maxSliceSize` because every store admits KeyValue growth only
  within `t / w` of it (`IDA.maxKVSliceSize`, R16), a rebuilt store included (inherited growth guards, 2.3). FSIF
  refuses a slice
  above its segment payload (`CommitError.SliceTooLarge`, `rebuildWrite`).
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
non-finalized `Committed` entry stems only from a Paxos, relayed or expiry-commit resolution: a tree-learned
resolution records none, saves `Unresolved` and reports `Unresolved` in status replies until finalized
(`Tx.statusForExpiry`), so no expiry adoption records one either (transaction-recovery.md 7.2). A self-relay is
delivered after the Tx the prepare creates and any driver started for it exist, so both learn the outcome; below the
floor the Frontend refuses the prepare and nothing is created but a rebuilt store's muted Tx (2.1). The entry may be
the store's own Tx's, so no self-relay is sent while that Tx is resolved: after a lost `TxFinalized` its idle driver
would take it as a non-late commit and run T's finalization actions again; it learns the outcome from its peers. A
sibling store of T on the same host shares the cache, so it relays no non-finalized entry to a sender on its host
(`Store.onThisHost`). T's actions can still run after T was finalized and forgotten, from a second driver's resent
`TxResolved` (paxos-transactions.md 10.5). Each shipped action tolerates it: the allocation action finds its entry, or records a live object under a shared revision lock that fails
once the object is deleted, and records nothing for one proven deleted; the deletion action removes the entry of an id
never allocated again; the split action indexes the new node only if it still reads and its entry is absent, under
its revision lock; the tail removal acts only on a node still present and empty; the join action removes the entry
at its key only while it names the joined node, under the key's revision as read, so a later split's entry at the
same minimum stays; the missed-update marker
writes marks and entries for stores that no longer hold T, which repair consumes as no-ops (below the floor, no Tx),
and the stubs its `TxFinalized` has stores write expire (`Frontend.expireStubs`). No compute or durable-task code
registers a finalization action. A recovered commit voter that re-applies on such a non-late outcome is safe by the
re-check rules of 2.3; a recovered non-voter applies nothing and reports its objects (`Tx.doCommit`). A prepare dropped by CRL admission
(`Store.receiveTransactionMessage`, lane full) gets no relay either; every store still holding T answers itself, so
that only delays the sender.

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
prepare is dropped (crash-recovery-log.md section 6, Oversized). The limit charges the rotations' evacuation headers, so live bytes within it
always leave room for them, at every stream and ring size (crash-recovery-log.md 6.1, "The limit"). The device reconciler's leftover drop
(`DeviceReconciler.dropLeftoverCrlState`) releases only the charge retained for the store when it was issued, and only
if it is unchanged when the drop completes, so a store landing meanwhile keeps its loaded state counted until its
recovery counts it (crash-recovery-log.md 3.5). What can still fill a lane is the tier's own traffic,
including same-tier finalizers (TKVL split, join, tail removal): ordinary back-pressure that clears as the tier's
transactions finalize upward. Data-only devices give up a quarter of their headroom (Backlog.txt).

**State kept outside admission's turnover.** A transaction's CRL state kept for a failed backend commit stays charged
for the life of the loaded store.

**Slice limit.** KeyValue growth is checked per store against what that store holds: the slice plus the growth its
held commit votes reserve (`RequirementsChecker.checkKVSliceSize`), within the KeyValue limit
`floor(t * (maxSliceSize - 4) / w)` (`IDA.maxKVSliceSize`). Each part of the union of committed updates is held by
its `t` voters and each store's voted parts fit the KeyValue limit, so the union fits `maxSliceSize` (R16), and repair
and rebuild can always install what the read restores (2.4); a rebuilt store guards what its old incarnation may
have voted to grow until repaired (2.3).

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
| Repair completion gate | object timestamps, no clock | object timestamps never decrease; a timestamp past T's start does not show T was applied, so the gate relies on T's commit voters applying T before any later write over its locks (2.3) |
| Rebuild pending-allocation coverage | none | the checkpointed union of listed allocations |

---

## 5. Contradictions

Terms the documents share mean the same in each:

* A transaction that "holds" or "has applied" a blind Insert's update at a store, for a key deletion's wait, includes
  a commit voter that has not applied yet (`Tx.holdsUpdateOf`; crash-recovery-log.md 3.4, transaction-requirements.md
  9.4, 2.3 above). A recovered non-voter is not one, and applies nothing (`Tx.doCommit`).
* A "recovered" commit vote is one rebuilt from the CRL at load (`Tx.recovered`). A live voter applying through its
  UnackedCommits entry past expiry is not one: its failed re-check is a live voter's invariant violation.
* The collision wait is `Tx.CollisionWait`, 500 ms checked at the 1 s store heartbeat, so up to about 1.5 s, for a
  lock holder a vote builds on and for a colliding lock-order waiter alike.
* "Below the floor no Tx is created" excepts, everywhere it is said, a rebuilt store's muted Tx for a transaction
  started before its acceptor (2.1; repair-process.md 5.4, paxos-transactions.md 10.10, transaction-recovery.md 4.2).

Between the documents, each disagreement found is corrected where it stood:

* systemic-loss-protection.md S10 described `fail-storage-device` as only for a drive whose data is already gone,
  while rebuild-process.md 4.1 uses it to move a rebuild off suspect media. S10 now says that nothing checks that the
  device is unserved, and points to the accepted limitation in rebuild-process.md B19.
* transaction-recovery.md 4.2 (a resolved, unfinalized cache entry) and 2.6 here said a prepare below the floor
  creates nothing; the rebuilt store's muted Tx is now excepted in both.
* transaction-requirements.md 10.5(c) says a refcount requirement fails at a store that applied a later chained
  write. That rests on the `updateSerial` convention (X-W27), which it now names.

Between a document, scaladoc or guide outside docs/safety and the code:

* `server/store/backend/fsif/DESIGN.md` 4.2 (L18 and "What a false trigger looks like") counted any failed compaction
  move read as loss, although its stated reason for loss (a failed write or fsync leaves the page cache
  untrustworthy) covers writes only. A failed move read now abandons the move and retries it after a backoff, as a
  client read retries a failure that is not evidence of damage, and both passages say so.

---

## Open weaknesses

### X-W26: A failed stat of a moved store's arrival file drops its arrival margin
Verdict PLAUSIBLE. Priority 5. Effort S.
Mechanism: `StoreLoader.readArrivedAt` tests `arrived-at` with `os.exists`, which answers false on a failed stat, and
the load then passes no arrival (`handleLoad`). `Frontend.loadFloorMillis` uses the load plus `MaxStartLead`, or the
host's join ceiling when lower, instead of the arrival plus `AcceptorSinceMargin`, so a store loaded within the margin
of its arrival may accept a prepare for a transaction it joined and forgot on its previous host, started ahead of this
host's clock by up to the skew the margin covers. An unreadable file already counts as an arrival now; only the stat
is unguarded. The other `os.exists`/`os.isDir` decisions outside the load only delay (2.2).
Consequence: one store rejoins a transaction it forgot. That T was concluded there, so the other acceptors also refuse
it below their floors or ignore it from their caches, and a lone rejoined acceptor decides nothing: hygiene. Needs a
failing stat of that one file during the margin and a replayed prepare.
Fix: `FileStat.exists` in `readArrivedAt`, a failure counting as an arrival now; and `FileStat` for the remaining
existence tests listed in 2.2, so absence means one thing everywhere.

### X-W27: A recovered vote's refcount re-check rests on an unenforced update-serial convention
Verdict PLAUSIBLE. Priority 5. Effort S.
Mechanism: a recovered commit vote takes a failed re-check as applied and a passing one as not yet applied
(`Tx.issueCommits`, `recoveredApplied`). A `RefcountUpdate`'s check is equality (`RequirementsChecker.checkRefcount`)
and its apply sets the new refcount (`RequirementsApplyer.apply`), so a later T2 that returns the refcount to T's
required value lets T's re-check pass after the store applied T and T2, and T applies again. Every shipped caller
derives the new refcount by `update`, `increment`, `decrement` or `setCount`, all of which advance
`ObjectRefcount.updateSerial`, so the value never repeats. Nothing enforces that: `Pins` and
`DescriptionValidator` accept any `newRefcount != requiredRefcount`, such as (5, 2) -> (5, 1), and
`TransactionBuilder.setRefcount` takes any `ObjectRefcount`.
Consequence: none with shipped code. A caller that writes a refcount by hand without advancing the serial can have a
recovered vote re-apply an old count over a later change at one store, unreported.
Fix: refuse, in `TransactionBuilder` and `DescriptionValidator`, a `RefcountUpdate` whose new `updateSerial` is not
above the required one, except an allocation's (`Allocating` to the initial refcount, guarded by `ObjectExists`) and a
`replaySafe` repair proof (rc -> rc).
