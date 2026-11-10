# Cross-document safety review (interactions between the seven docs in docs/safety/)

Scope: only flaws that arise from combining mechanisms of different documents. Per-document flaws were not re-verified.
Refreshed 2026-11-09 against `0d08e3c6` (branch `pin_refactor`, replay pins): the horizon and `TransactionTooOld` are
gone, TracelessCommits is UnackedCommits (entries for every committed transaction's unacknowledged voters), and
forgotten transactions are guarded by the client pin rule and late commits (paxos-transactions.md 10.5). Each item
below carries a refresh status; X11 and X12 are new.
Paths are relative to src/main/scala/org/aspen_ddp/aspen/ unless noted. Verdicts: CONFIRMED (mechanism read in code, no guard
found; trigger may still be rare), PLAUSIBLE (mechanism real, trigger/reachability uncertain), REFUTED (guard found).

## 1. Interaction flaws

### X1. Failed-device zombie replica consumes the new store's ErrorTree entries (rebuild F1 x repair x metadata)

> **Refresh (replay pins): unchanged (still FIXED).**

> **FIXED (2026-11-09).** `DeviceShutdown.retireFailedDevice` writes a durable `failed` marker and unloads a device the
> reconcile reads Failed; `tryLoadDevice` never loads a device carrying it; the tombstone sends the former host a
> `CheckStorageDevice`; `StoreRepairer.scan` runs only while the pool's entry for the store names the scanning host.
> Residual: until its first successful reconcile, a restarted or partition-healed old host may answer peers with a
> stale store-to-host mapping. Tests: DeviceCrlSuite "a device the tree records as failed is unloaded, marked, and
> not loaded again by a restart", StoreRepairerSuite "a store whose pool records it on another host is not scanned,
> so its entries stay for that host", FailedStorageDeviceSuite "step 1 wakes the device's former host".
Docs: rebuild-process F1, repair-process 4.4/12.6, systemic-loss-protection X11.  Verdict: CONFIRMED (mechanism); trigger = operator
`fail-storage-device` on a healthy device, or a partition that heals, then a restart of the old host.  Severity: stale-replica,
split-decision (two acceptors for one index).
1. Operator fails D1 (host H1 up). Tombstone; drain rebuilds store S on D2/H2 and repoints the pool entry; entries for index i stay in the ErrorTree.
2. H1 reconcile sees `remote.isFailed` and only logs (`server/host/DeviceReconciler.scala:123-127`). Loaded stores keep serving.
   On H1 restart, `owning-host == this host` takes the `loadOwnedDevice` branch directly (`server/host/DeviceDiscovery.scala:474-478`);
   the `state.isFailed` refusal exists only inside `resolveOwnership` (`:215-219`), which that branch bypasses.
3. H1's zombie S_old is `served` and not `rebuilding`, so `HostStoreView.of` marks it repairable (`server/host/HostStoreView.scala:27-34`).
   `RepairService` scans the ErrorTree range for index i (the key is `poolIndex || objectId`, no host in it), reconciles S_old, and deletes the
   entries via `deleteEntryIfUnchanged`.
4. S_new (promoted blank) then finds no entry for the gaps recorded for it. They are never repaired, and nothing records it.
   Also S_old votes/accepts as index i with its own old CRL while S_new has blank acceptor state: two acceptors for one index, quorum
   intersection broken.
Fix sketch: store-load and repair must check the pool entry (`stores(i).hostId/deviceId`) against the local host/device before serving or
scanning (fencing by pool entry, not by network); on `isFailed` relinquish like `relinquishDevice`; make `owning-host` load do one tree lookup
when the tree is reachable.

### X2. Blank rebuilt acceptor vs a transaction that is stuck unresolved (rebuild F14 x CRL F18 x paxos P9 x transaction-recovery)

> **Refresh (replay pins): unchanged (still FIXED).** The mute now also carries the late-commit premise (paxos P-N2).

> **FIXED.** Fixed 2026-11-09. A rebuilt store's acceptor covers only transactions started at or after the HLC time in its
> `acceptor-since` file (`RebuildingStore.AcceptorSinceFile`), written when the rebuild finishes: finish time plus a
> 10 s margin for client clocks running fast. `Frontend.acceptorSince` mutes a prepare for an unknown older transaction
> (`Tx.muted`): it is still proven committed if an object shows its update, and otherwise forgotten with no promise,
> vote, accept or CRL save. The store is then offline for every transaction its old incarnation might have joined, so
> any later quorum meets the surviving holder of a chosen value. Accepted costs: such a transaction cannot be decided
> if it needs the rebuilt store for its quorum, which is correct Paxos behaviour; a split tally waits on the muted
> store, a stall and not a split (paxos F8); new transactions within the margin after promotion are muted there too.
> Not covered: a zombie old copy on a partitioned host (X1 residual), or a client clock fast by more than the rebuild's
> duration plus the margin. Tests: FrontendSuite "a rebuilt store ..." (6), RebuildingStoreSuite "a rebuilt store
> records ...", DeviceCrlSuite "a store directory carrying acceptor-since ..."
Verdict: PLAUSIBLE.  Severity: split-decision (commit vs abort).
Rebuild F14 says amnesia "is safe only within the pool's failure tolerance". That is not a Paxos property: a chosen value held by exactly
`t` acceptors survives a later quorum only through the `2t-w` intersection. For Replication(3,2), `2t-w = 1`, so ONE amnesiac acceptor can be
the entire intersection. The amnesiac store is silent for the whole rebuild (offline marker, `StoreLoader.scala:73`; `HostStoreView`),
which hides the problem: during the rebuild the other holder of the accept answers every promise quorum. The hole opens at promotion
for any transaction still unresolved then:
 - T accepted commit at S0,S1 (chosen). S0's CRL is lost (D1/D2/T3-T8), S1 stalls (e.g. T cannot finalize: recovery F2 permanent marker failure,
   F14 ErrorTree unavailable, paxos F8 tally stall), but S1 keeps the Tx (resolved or accepted).
 - Rebuild of a small/empty store takes seconds to minutes; S0' promotes with no promises or accepts.
 - A new driver reaching {S0', S2} sees no accepted value; a young T gets fresh votes; S0'/S2 re-check requirements (S0' on rebuilt data) and may vote abort
   (e.g. T already applied at S1 and S0' was rebuilt after that, so its revision check fails). Abort is accepted by S0',S2; S1 still has commit.
   `Tx.resolved` keeps the first outcome per store (paxos P19, F11), so the pool diverges silently.
Mitigating guards found: stalled T normally resolves at S1 and S1's cache/Tx answers drivers; horizon makes old T vote `TooOld` abort (can
itself cause the abort); the window needs T unresolved beyond the rebuild duration.
Fix sketch: a rebuilt/CRL-lost store must refuse to promise/accept for any transaction whose start timestamp is older than its rebuild
completion (or than "now minus horizon" at promotion), answering only TxUnknown/silence, not a fresh vote.

### X3. Stray TxResolved writes the host-wide status cache; finalized flag can be downgraded -> re-apply of blind updates (paxos F9 x transaction-recovery F10/F8 x requirements F1/F2)

> **Refresh (replay pins): NARROWED, not fixed.** `Frontend`'s no-Tx `TxResolved` path now leaves a finalized entry
> alone and ignores proofs (`server/store/Frontend.scala:636-637`). But `Tx.resolved` still writes
> `(status, finalized = false, late = lateOutcome)` unconditionally (`server/transaction/Tx.scala:737-738`), so step 1
> still happens when store B on the same host resolves after store A finalized. Path at `0d08e3c6`: a client
> retransmit or replay reaches A in that window; `Store.handleTransactionMessage` sees Committed, not finalized, and
> answers `TxResolved(committed, late = false)` (`Store.scala:158-168`); if A is the designated leader it also starts a
> driver (`:182-184`), which takes that message as an ordinary resolution and re-runs T's finalization actions
> (`TransactionDriver.scala:376-377,568-596`); A's fresh Tx (created with `outcomeDecided`, `Frontend.scala:683-689`,
> carrying the client data) then applies on that driver's non-late `TxFinalized(true)` (`Tx.scala:883-890` ->
> `finalized` -> `resolved(true)` -> `doCommit`). Per-write checked writes are
> skipped at apply; blind writes are applied over a newer value. The window is B's resolution-to-finalization time, so
> the trigger needs B's messages delayed. Fix: make `updateStatus` monotone (never clear `finalized` or `late`).
Verdict: PLAUSIBLE.  Severity: stale-replica / resurrection (blind KV insert/delete, overwritten value).
Code: `Frontend.handleTransactionMessage` for `TxResolved` of an unknown Tx calls `statusCache.updateStatus(txid, statusOf(committed))` unless the
entry is `finalized` (`server/store/Frontend.scala:614-619`). `Tx.resolved` writes `(status, finalized=false)` (`server/transaction/Tx.scala:662`),
including from a second store on the same host that resolves later than the first one finalized (host-wide cache, expireAfterWrite resets).
1. T (blind `Insert(k,v1)`, or a Delete) commits; store A on host H applies and finalizes; the cache says finalized. Store B of the same txn on H (another pool, or
   a co-located store of the same pool, rebuild F12) resolves later and downgrades the entry to finalized=false.
2. In that window a client retransmit (`fromClient`, with data) reaches A. `Frontend.receivePrepare` only ignores a prepare when the entry is `finalized` (`:632-640`);
   otherwise it creates a new Tx (`known = Committed`, `outcomeDecided`, locks skipped) with the data.
3. The new Tx learns commit (peer caches, its idle recovery driver at 5-6 s) and applies with `applying=true`; a blind KV Insert/Delete has no revision to
   compare (requirements F1), so it overwrites a newer blind write T2 of the same key committed in between at A only (key resurrected/overwritten, no marker: A reported TxCommitted both times).
Docs call the downgrade "hygiene" (paxos F9) and "not unsafe given T7" (recovery F10); neither considers a re-apply.
Fix sketch: never downgrade `finalized` (keep max of flags per txid); a Tx created from a prepare for a transaction with known outcome should apply only if
the store has no trace of the transaction (needs the 'applied' record) or compare key revision/timestamp at apply for blind writes.

### X4. Stores' drivers are never stopped on close/reload/rebuild/relinquish: zombie proposer with an immortal Accept (recovery F12 x paxos F4/F5 x rebuild/adoption)

> **Refresh (replay pins): UNCHANGED, consequence changed.** `Store.close` still only closes the frontend
> (`Store.scala:65`). The zombie's periodic prepares no longer meet `TooOld`: they create fresh Txs checked against
> current state, which may take commit-vote locks, and for a data-less transaction whose abort was forgotten can
> re-decide it commit (paxos P-N4).
Verdict: CONFIRMED (mechanism); safety impact PLAUSIBLE-low.  Severity: liveness (leak), split-decision only with F5.
`Store.close()` calls only `frontend.close()` (`server/store/Store.scala:63`); `transactionDrivers` is touched nowhere else (grep: only Store.scala).
A driver stops only on `receiveTxFinalized` for its own store id (`SimpleTransactionDriver.scala:50-52, 83`; `TransactionDriver.scala:363-396`). After the store is reloaded
under the same `StoreId` (rebuild, backend fail-stop, device shutdown/restart, relinquish) replies go to the NEW `Store`, whose `transactionDrivers` map lacks it
(`Store.scala:162-167` just caches the response), so the old driver never finishes. It rebroadcasts `Accept((1,L), v_old)` / `TxResolved` forever every `maxDelay`, and every 3rd round
re-sends prepares to every store, resurrecting `Tx`s that were legitimately forgotten (they vote `TooOld`, cost CRL space and admission).
This turns paxos F5 (equal id `(1,L)` re-accepted, `Acceptor.scala:31`) from "a message delayed in the network" into "an indefinitely repeated message" for any non-crash reload of the leader's store.
Fix sketch: `Store.close` must `shutdown()` all drivers; a driver should stop when its store instance is gone.

### X5. Admission back-pressure x finalization needing ErrorTree writes x shared device CRL (CRL F15 x recovery F14 x paxos F8 x repair)

> **Refresh (replay pins): UNCHANGED, amplified.** Every marker batch now also carries one UnackedCommits entry per
> silent counted voter per transaction (transaction-recovery R-N4), so the batches the loop waits on are larger.
Verdict: PLAUSIBLE.  Severity: liveness (system-wide stall, positive feedback).
`CrlAdmission` is per device and "nothing is exempt". A store whose device CRL is past half a stream silently drops prepares for unseen transactions.
Every dropped prepare makes that store "silent" for `errorTimeout` (2 s), which forces the finalizer to write a marker batch (`MissedUpdateFinalizationAction.markMissedUpdates`)
and holds `TxFinalized` until it succeeds (recovery F2/F14). Marker batches are transactions whose prepares go to the metadata pool's stores; if those share a device with
data-pool stores (default: metadata pool members are ordinary devices), their prepares are dropped too. Unfinalized transactions keep their admitted bytes, so the
CRL stays full and more prepares drop: the loop only breaks when a metadata quorum of devices has room. Also each drop adds an ErrorTree entry (more repair load, repair F8/F9).
Fix sketch: exempt (or reserve a lane for) transactions whose pool is a metadata pool / marker batches / TxFinalized-path; or finalize with the miss recorded in memory first.

### X6. Missed delete marker is a prerequisite for repairing a missed allocation into a reused FSIF slot (FSIF Ruling 18 x repair x rebuild F9 x recovery F2)

> **Refresh (replay pins): unchanged.**
Verdict: PLAUSIBLE.  Severity: stale-replica (permanently missing slice); resurrection of the old object's zombie slice in the same event.
Code: `FsifBackend.updateExt` (`server/store/backend/fsif/FsifBackend.scala:1273-1295`): a repair of object N at ext x never overwrites a different occupant; it returns
`ObjectMismatch` and relies on "that missed delete heals first through repairDelete". `startRepairDelete` is id- and ext-checked (`:1457-1478`), so slot aliasing itself is REFUTED (see
section 4). But the heal needs the ErrorTree entry E(S,O,x) for the delete. That entry can be missing: marker batch give-up on `UnknownStoragePool`/`TransactionCreationError`
(`client/internal/transaction/MissedUpdateMarker.scala:127-157`), repair F5 (unguarded delete of an entry on allocation absence), or `deleteEntry` by a zombie (X1).
Then O's slice stays on S (zombie, never read by quorums but occupies the ext) and N's repair returns `ObjectMismatch` forever; S never gets N. Only a full rebuild heals
(`rebuildWrite` writes the ext directly, `:390-413`).
Fix sketch: repair of N over a foreign occupant should drop the occupant if the allocation tree no longer lists it (the repairer already can look up O).

### X7. Rebuild-written timestamps feed the repair completion gate that is not monotone (rebuild 5.2 x repair F3/F13 x requirements F4)

> **Refresh (replay pins): unchanged.**
Verdict: PLAUSIBLE.  Severity: liveness / silent early completion.
`rebuildWrite` stores the read's revision/refcount/timestamp. Repair declares an entry complete when the read timestamp is at least the entry's `missedTimestamp`
(`StoreRepairer.scala:266-271`), and the DataObject path silently skips if `read.timestamp < current.timestamp` and counts it complete (`Reconcile.scala:151-153`, repair F13).
After a rebuild the store's timestamps are the walk-time reads', and blind writes/refcount updates move timestamps non-monotonically (requirements F4). So an entry written for a
transaction during the rebuild can (a) never complete (repair F3), or (b) be dropped as complete without the store having the content when the store's timestamp is higher
than the read's (F13) -- the rebuilt copy can be lower or higher than peers' depending on apply order. Neither doc analyses the rebuilt-store case, where EVERY post-walk
entry depends on the gate.
Fix sketch: gate on content equality after install (as repair F3 suggests), mandatory before relying on the ErrorTree to close the rebuild gap.

### X8. Mixed-version rolling upgrade and finalizer types (recovery F1 x systemic SC1-SC7 x repair x user jars)

> **Refresh (replay pins): unchanged.** `RegisteredTransactionFinalizerFactory.scala:66-70` still throws on an unknown type.
Verdict: PLAUSIBLE.  Severity: liveness, then stale-replica.
`RegisteredTransactionFinalizerFactory.create` throws `NoSuchElementException` on an unknown type and the driver never finalizes (recovery F1). In a rolling upgrade, a new host
can start transactions with new FinalizationAction types while old hosts, whose stores hold the CRL state, become recovery drivers for them (idle 5 s). Each wedged
transaction keeps CRL state and admission bytes and, because finalization (hence ErrorTree marks) never runs, the lagging store gets NO entry. Combined with X5 this is
a correlated trigger across all stores of a pool at once. Neither recovery nor systemic-loss mention version skew of finalizer types.
Fix sketch: refuse to drive a transaction whose types are unknown; carry a protocol/feature version in the txd and have old hosts not become drivers.

### X9. Metadata recursion: bootstrap-pool tracking hole composes with repair-entry deletion tracking disabled (rebuild F8 x repair F7 x systemic)

> **Refresh (replay pins): UNCHANGED.** `limitTracking` is unchanged; the untracked bootstrap batches now also hold
> UnackedCommits entries.
Verdict: PLAUSIBLE.  Severity: stale-replica of the repair metadata itself.
`limitTracking` disables missed-update tracking on an ErrorTree write only when the transaction references the pool the entry is about
(`MissedUpdateFinalizationAction.limitTracking`, verified `:130`). Ordinary pools: fine. Bootstrap pool B: its ErrorTree/UnackedCommits nodes and all pool-state objects live in B, so any miss
of a marker/entry-deletion transaction by a B store is not recorded, and repair's own entry deletions are never tracked at all (repair F7). A B store that lags on an ErrorTree node
is healed only by opportunistic rebuild on a read (itself unsafe: repair F1/F2). Since B also holds the trees describing M's stores, a B store rebuilt or briefly offline can miss entries
about OTHER pools' stores (the entries are stored in B nodes) without any record. If two B stores each lag on different nodes, a read quorum for a node can resolve to a revision lacking
a just-written entry, and the data-store gap is lost permanently. Documents treat the hole as "redundancy -1"; the combination with rolling restarts of the 3-5 bootstrap hosts makes it correlated.
Fix sketch: track misses of B-internal writes in an in-memory/CRL-backed list on the finalizer host until a full B node scan confirms convergence, or give B a periodic ErrorTree-node anti-entropy (compare all node revisions).

### X10. UnackedCommits / ErrorTree entries for a store are not bound to the store's incarnation (traceless design x rebuild x transfer x repair 12.6)

> **Refresh (replay pins): unchanged; matters more.** An UnackedCommits entry now means "this store is owed T", and a late commit reads it (R-N1).
Verdict: PLAUSIBLE-low.  Severity: stale-replica.
Keys are `(poolIndex, objectId)` / `(poolIndex, txid)`; values carry no incarnation. After an in-place rebuild or T12 move the new store consumes entries written for the old
incarnation: harmless (reconcile is exact), but the opposite order is unprotected: entries consumed (deleted as complete) by the old incarnation shortly before its loss mean
the rebuilt store's later zombie slices (rebuild reading an object that is deleted a moment later; the entry E for the delete was written before the restore) rely on E surviving.
Verified OK in the normal flow (E is written 2 s after commit and kept until the store is repairable), listed as a place where X1 turns it into data loss.

### X11. Late-acknowledged entries x replayed allocation (transaction-recovery R-N1 x requirements F11 x repair 5.3)
Verdict: PLAUSIBLE.  Severity: resurrection / overwrite of a live object.
1. T allocates object O in pool P (its pin is elsewhere, say a pointer stored at a required revision) and commits; P's
   stores acknowledge after `errorTimeout`, so each counted voter gets an UnackedCommits entry
   (`MissedUpdateFinalizationAction.scala:98-106`). They apply, finalize and forget T; entries wait for each store's scan.
2. O is written by later transactions. A replayed client prepare of T arrives before the scans. P's stores build a fresh
   `Allocating` state without reading the backend (requirements F11) and vote commit; the pin object's stores prove T if
   it is unchanged; the driver resolves late (`TransactionDriver.scala:539-552`).
3. Each P store with an entry finds it (`Tx.scala:926-930,1003-1004`) and applies the allocation again, overwriting O's
   current state with T's initial content and refcount. With entries at a write threshold of P's stores the overwrite is
   what reads return, and the scans then reconcile to it and delete the entries.
Fix sketch: as R-N1; and have an allocating update fail at apply when the backend already holds the object.

### X12. Recovering acceptor x replay x finalization re-run (paxos P-N3 x recovery R-N6 x requirements Q-N5)
Verdict: PLAUSIBLE.  Severity: resurrection (blind writes), stale-replica (orphan allocation entries, regressed marks).
A store offline beyond the status-cache window that holds T's accepted commit makes any post-proof quorum it joins
non-memoryless, so a driver adopts commit normally (`TransactionDriver.scala:264-268`) and re-runs T's finalization
actions long after the original ran (Ruling 13): the allocation action may re-insert an entry for a deleted object
(orphan), the marker re-marks stores and re-creates deleted UnackedCommits entries, and a replayed client prepare in the
same window applies its blind writes on the non-late commit. Each step is accepted in isolation by a ruling; together
they let one long outage plus one delayed duplicate leave three different kinds of residue.

## 2. Coverage gaps (failure scenarios no document covers)

G1. Fast rebuild or lost CRL vs a long-unresolved transaction (X2): no document states how long a transaction can stay unresolved or tests promotion of a blank acceptor against it.
G2. Mixed-version/rolling-upgrade protocol skew (message formats, finalizer types, CRL entry format, txd fields): systemic doc covers detectors only; recovery covers F1 only for jars.
G3. Zombie drivers/timers after a non-crash store reload, device relinquish and transfer-out (X4). Only recovery F12 mentions it, with no interaction analysis.
G4. A shared, in-memory host-wide status cache is the anchor for finalization relays and, since the refresh, late-commit evidence (condition 2); no document analyses host restart (cache wiped) combined with rolling restarts of the stores' peers (paxos F6 touches it for one restart only). The horizon and TooOld no longer depend on it.
G5. ErrorTree/UnackedCommits entry-key incarnation (X10) and the time-line of entry production vs consumption during a T12 move are described only per-document.
G6. Allocation-tree dependency: rebuild F10/F17, repair F5 and requirements F11 each mention an allocation recorded late or not at all, but nobody analyses the total effect (missing slice, stale deletion decision, replayed allocation) across rebuild, repair and horizon.
G7. Client object caches/pointers (stale `ObjectPointer`, DataObject read caches) are outside all seven; resurrection by a client reading a zombie slice with a cached pointer is not discussed.
G8. Clock: HLC skew no longer affects replay (the horizon is gone, paxos F3 fixed), but still affects the rebuilt store's `acceptorSince` mute, on which the late-commit rule now also rests (paxos P-N2), repair's 60 s age gate (entry timestamp is the marker transaction's), and rebuild timestamps; no document looks at them together.
G9. Marker-batch size/limit: `TransactionCreationError` is treated as permanent (StopRetrying) and wedges finalization (recovery F2); no document asks which creation errors an oversized batch (256 entries x many objects) can hit.
G10. Capacity interaction: ErrorTree growth during long outages (repair 13.3-6) x CRL admission (X5) x rebuild in-progress counts as failure budget (rebuild 6.3): no combined capacity argument.

G11. Replay ordering: the replay integration tests deliver the leader's copy of the replayed client prepare last (the TestNetwork leader driver does not retransmit), so the ordering where a driver's data-less prepare reaches stores before the replayed client prepare is not exercised end to end. Reading the code (`Tx.awaitingData`, `Tx.scala:180-183`) it withholds the vote up to 5 s or until a proof exists; untested.
G12. amoebafs pre-existing bugs found during the refactor (cross-node rename whose delete joins into the target node updates it twice; stale `iptr` in `prepareMove`; `prepareDelete` able to end unpinned) are outside these documents' scope and are recorded in Todo.txt.

## 3. Inter-document contradictions

C1. systemic-loss-protection X11 says a Failed device is "refused (DeviceDiscovery.scala:219)" and "by design never reintroduced"; rebuild-process F1 says it is loaded again. Code: both true in different branches. The refusal is in `resolveOwnership`
    (`:215-219`) used when `owning-host` names another host or a shutdown marker exists; the `owning-host == this host` branch calls `loadOwnedDevice` with no tree check (`:474-478`). Rebuild F1 is right for a device whose owning-host is the local host.
C2. repair-process 12.8 says marker inserts use `retryUntilSuccessful` and "No entry is lost"; rebuild F9 and transaction-recovery F2 say the batch gives up. Code: both: transient errors retry, but `UnknownStoragePool` and every `TransactionCreationError` throw `StopRetrying`
    and `finished` only logs "Gave up" (`MissedUpdateMarker.scala:127-157`); repair 12.8 is wrong on "no entry is lost".
C3. rebuild-process F14 ("Paxos acceptor amnesia is safe only within the pool's failure tolerance") contradicts paxos-transactions P8/P9 (state of a possibly chosen value must be retained) and crash-recovery-log F18 (open question). Quorum arithmetic says tolerance
    does not cover amnesia (X2). The three documents give three different answers.
C4. transaction-recovery F10 and paxos F9 call the status-cache downgrade low impact/"hygiene" and "not unsafe"; requirements F1/F2 and X3 show a re-apply path for blind writes. *(Refresh: resolved in the documents: all three now say the downgrade is narrowed, still present in `Tx.resolved`, and costs late-commit evidence.)* The documents disagree on whether the cache is a safety mechanism (paxos P17 says assumption) or an optimization (recovery F10).
C5. paxos F2 / recovery F3,F5 say the false-abort is "repaired through the ErrorTree/UnackedCommits entries recorded by the finalizer", but transaction-recovery F13 and requirements F8 say stores that were not counted voters, or traceless changes not covered by `tracelessObjects`, get no entry; the repair-process documents assume an entry always exists. *(Refresh: the traceless half is gone, requirements F8 fixed; the uncounted-voter half, recovery F13, stands.)*
C6. rebuild-process 8.2 says ErrorTree repair "closes the residual staleness gap"; repair-process 13.2 F1/F2/F3/F4/F13 each describe cases where repair itself leaves the store stale or declares completion early. The rebuild guarantee rests on the repair guarantee that is not established.
C7. transaction-recovery F6 (persisted `Committed` status is not restored into the recovered Tx; relies on the cache) vs crash-recovery-log 3.1 line 149 ("No decision is saved"): the CRL doc says the resolution is not a record, but recovery F6 says it can be persisted by a re-save; both are true depending on whether a prepare/accept re-saved the Tx after resolution. Should be stated once.

## 4. Candidates examined and REFUTED (do not redo)

R1. Stale allocation-tree lookup causing a false "absent -> repairDelete": `pool.allocationTree.get` reads the tier-0 leaf fresh (only tier >= 1 comes from `LruTierCache`; `TierEngine` navigate/fetchContainingNode) and a stale upper tier is corrected by following links. No false absence from the cache.
R2. FSIF slot reuse aliasing a new object with a stale one: delete of an occupant mismatch returns `ObjectMismatch` ("not deleting it", `updateExt` `:1279-1285`), `startRepairDelete` checks `index.ids(x) == objectId`, `readExt`/`finishRead` check id, allocation over a missed-delete occupant is an explicit overwrite (Ruling 18). Remaining issue is only X6.
R3. Rebuild walk resurrecting a traceless-deleted key: a read quorum with one stale and one current store cannot resolve either state; the reader waits (matching+potential), so a rebuild read fails/retries rather than restoring the key. Requires more laggards than the failure budget.
R4. Quorum of {rebuilt-stale S, current S2} committing on stale requirements: revision-based requirements abort at S2 or S; `DoesNotExist/Exists` are held by S2 (the fact committed at t >= 2 stores, any t-quorum includes one holder). Only blind writes pass, by design.
R5. ErrorTree entries consumed by a rebuilding staging store: staging is a bare `Backend`, never a `Store`; `HostStoreView.repairable` excludes `rebuilding`; in-place rebuild keeps the store offline. Entries written during the rebuild wait for promotion. (Only the F4 crash window and X1 zombie bypass this.)
R6. Metadata recursion deadlock (marker txn needing its own marker): `limitTracking` stops the recursion at the bootstrap pool; the marker for a pool's entries references the metadata pool only through different pools. No infinite recursion; residual issue is X9.
R7. Device-CRL loss dropping commit votes that the other stores need: the lost store is offline for the rebuild duration, other holders answer, and the finalizer marks the silent store; confirmed except for X2's window.
