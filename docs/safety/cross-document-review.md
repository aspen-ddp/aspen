# Cross-document safety review (interactions between the seven docs in docs/safety/)

Scope: only flaws that arise from combining mechanisms of different documents. Per-document flaws were not re-verified.
Refreshed 2026-11-09 against `0d08e3c6` (branch `pin_refactor`, replay pins): the horizon and `TransactionTooOld` are
gone, TracelessCommits is UnackedCommits (entries for every committed transaction's unacknowledged voters), and
forgotten transactions are guarded by the client pin rule and late commits (paxos-transactions.md 10.5). Each item
below carries a refresh status; X11 and X12 are new. Refreshed again 2026-11-10 against `c181375b` for the replay-pins
addendum (prefer-abort driver prepares, applied stubs, `TxFinalized.owed`): X6, X10, X11 and X12 carry an addendum
status, X13 and X14 are new.
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

> **Refresh (2026-11-13): the fast-client residual is closed.** Every store mutes an unknown transaction whose start
> leads its host's wall clock by more than `Frontend.MaxStartLead` (2 s), so an old incarnation joined only
> transactions started before its rebuilt store's acceptor-since time, whatever the client's clock (paxos P-N2). The
> "not covered" clause below now reads: a zombie old copy, or host clocks skewed by more than 8 s less the rebuild's
> duration.

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
 - T accepted commit at S0,S1 (chosen). S0's CRL is lost (D1/D2/T3-T8), S1 stalls (e.g. T cannot finalize: recovery F2 permanent marker failure (since 2026-11-11 one that repeats on every retry),
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

> **Refresh (Ruling 30): FIXED.** `TransactionStatusCache.updateStatus` is monotone, by an atomic compute: `finalized`
> and `late` are sticky and a resolved status never reverts to Unresolved. Store B's `Tx.resolved` and a new Tx's
> constructor can no longer downgrade A's finalized late entry, so the finalized relay keeps answering late
> (`StoreSuite` "a sibling store's delayed resolution does not downgrade a finalized late entry").

> **Refresh (horizon, 2026-11-13).** The split decisions that produced opposite outcomes (paxos F2, P-N4) are closed
> by the transaction horizon (paxos 10.10), so opposite outcomes should never reach the cache. `updateStatus` keeps
> the merge rule below and now logs an error and counts any that does (`splitDecisions`; TransactionStatusCacheSuite
> "an opposite outcome is logged as a split decision").

> **Refresh (post-addendum): opposite outcomes.** The monotone merge ORed `finalized`/`late` across opposite statuses,
> reachable through split decisions (paxos F2, P-N4): a stray abort over a finalized late commit became a finalized
> abort that the relay spread, and a commit over a finalized abort became a finalized late commit whose drivers skip
> the finalization actions. Now Committed wins (an abort never replaces a commit) and a commit replacing an abort
> takes only its own flags (TransactionStatusCacheSuite "a false abort never replaces a finalized late commit", "a
> commit replacing a finalized abort takes none of the abort's flags").

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

> **Refresh (Ruling 31): FIXED for closed stores.** `Store.close` now shuts down the store's drivers and clears its
> map, and a `SimpleTransactionDriver` timer already firing neither sends nor reschedules, so no zombie driver outlives
> a close, reload, rebuild or relinquish that goes through it. P-N4's delayed duplicate client prepare remains.
> *2026-11-13:* P-N4 is closed by the transaction horizon (paxos 10.10), so nothing of this chain remains.
>
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

> **2026-11-15 (Todo 4.3): FIXED for the admission leg.** Admission has lanes by the receiving store's pool
> (`AdmissionTier`, `CrlAdmission.ceiling`): data-pool stores admit a new transaction only while the device's total is
> below 3/4 of the limit, metadata-pool stores below 7/8, bootstrap-pool stores up to the limit. The tier is copied
> into the store's `aspen-store-config.yaml` from its pool when the store is created (`StoreConfig.forPool`), so no
> claim in the description is trusted. Finalization only depends upward: a data transaction's marker batch writes
> trees in a metadata pool, a metadata transaction's in the bootstrap pool, and a bootstrap transaction's marker about
> the bootstrap pool tracks no missed updates (`MissedUpdateFinalizationAction.limitTracking`). Data load on a shared
> device therefore cannot take the room the marker batches it waits for need. `TxFinalized` is a message, never
> gated. The capacity argument is G10.
> Residual: (a) finalizers in the same tier as their transaction (TKVL split, join and tail removal) can still stall
> if that tier's own traffic fills its lane on every device of the pool; lower-tier load can no longer cause it.
> (b) Finalization still needs the marker write to be *available*: a metadata quorum that is unreachable still holds
> transactions unfinalized (recovery F14; accepted 2026-11-15 with the `FINALIZATION STALLED` alarm, Todo 4.9). (c) Data-only devices give up 1/4 of their admission headroom
> (Backlog.txt).
> Tests: CrlAdmissionSuite "each tier is admitted below its own ceiling"; FrontendSuite "past the data lane a data
> store drops a new prepare while a metadata store on the device admits one", "past the metadata lane only a
> bootstrap store admits a new prepare"; StoreConfigSuite "forPool copies the pool's settings and derives its
> admission tier"; MainSuite (the CLI's bootstrap and initial metadata stores). Not reproduced end to end: the
> single-host TestNetwork runs with unlimited admission, and the loop's only mechanism, which lane a store's
> admission checks, is exercised directly at the Store.

> **Refresh (replay pins): UNCHANGED, amplified.** Every marker batch now also carries one UnackedCommits entry per
> silent counted voter per transaction (transaction-recovery R-N4), so the batches the loop waits on are larger.
Verdict: PLAUSIBLE.  Severity: liveness (system-wide stall, positive feedback).
`CrlAdmission` is per device and "nothing is exempt". A store whose device CRL is past half a stream silently drops prepares for unseen transactions.
Every dropped prepare makes that store "silent" for `errorTimeout` (2 s), which forces the finalizer to write a marker batch (`MissedUpdateFinalizationAction.markMissedUpdates`)
and holds `TxFinalized` until it succeeds (recovery F14; since the F2 fix a write that gives up restarts the finalizer rather than hanging it, which does not change this loop). Marker batches are transactions whose prepares go to the metadata pool's stores; if those share a device with
data-pool stores (default: metadata pool members are ordinary devices), their prepares are dropped too. Unfinalized transactions keep their admitted bytes, so the
CRL stays full and more prepares drop: the loop only breaks when a metadata quorum of devices has room. Also each drop adds an ErrorTree entry (more repair load, repair F8/F9).
Fix sketch: exempt (or reserve a lane for) transactions whose pool is a metadata pool / marker batches / TxFinalized-path; or finalize with the miss recorded in memory first.

### X6. Missed delete marker is a prerequisite for repairing a missed allocation into a reused FSIF slot (FSIF Ruling 18 x repair x rebuild F9 x recovery F2)

> **2026-11-14 (Todo 2.6): FIXED.** A repair no longer depends on the delete entry. FSIF `updateExt` in repair mode
> fails over a foreign occupant with `RepairSlotOccupied(N, O)`. The repairer has just read N live, and two live
> objects never share an ext, so once the AllocationTree does not list O it drops O through the id-checked
> `repairDelete` and leaves N's entry for the next pass, which installs N. An O the tree still lists is logged as an
> inconsistency and left. The F5 leg is gone too: an entry is dropped on absence only after a read proves the deletion
> (repair-process F5).
> Residual: a zombie O on a slot never reused stays until an entry for O is repaired. It costs space only, since
> quorum reads of O answer not found.
> Tests: FsifBackendSuite "repair at an ext held by a different object fails naming the occupant, and succeeds once
> it is dropped"; StoreRepairerSuite "a repair over a foreign occupant the allocation tree does not list drops the
> occupant and keeps the entry", "... still lists leaves the occupant".

> **Refresh (replay pins): unchanged.**
> **Refresh (addendum): unchanged; one more consequence.** The same give-up, in the second driver's handed-off write,
> now also leaves the committed pools' UnackedCommits entries listed in no `owed` (X13).
> **Refresh (2026-11-11): the recovery F2 leg is fixed.** A marker write that gives up fails the finalizer and its driver
> re-runs it, so the delete mark is written before the transaction finalizes, or the transaction stays unfinalized
> (a logged stall). A second driver finalized by another's `TxFinalized` cancels its marker and does not re-run it; its
> lost entries are the first driver's to write (X13 unchanged).
Verdict: PLAUSIBLE.  Severity: stale-replica (permanently missing slice); resurrection of the old object's zombie slice in the same event.
Code: `FsifBackend.updateExt` (`server/store/backend/fsif/FsifBackend.scala:1273-1295`): a repair of object N at ext x never overwrites a different occupant; it returns
`ObjectMismatch` and relies on "that missed delete heals first through repairDelete". `startRepairDelete` is id- and ext-checked (`:1457-1478`), so slot aliasing itself is REFUTED (see
section 4). But the heal needs the ErrorTree entry E(S,O,x) for the delete. That entry can be missing: marker batch give-up on `UnknownStoragePool`/`TransactionCreationError`
(`client/internal/transaction/MissedUpdateMarker.scala:127-157`), repair F5 (unguarded delete of an entry on allocation absence), or `deleteEntry` by a zombie (X1).
Then O's slice stays on S (zombie, never read by quorums but occupies the ext) and N's repair returns `ObjectMismatch` forever; S never gets N. Only a full rebuild heals
(`rebuildWrite` writes the ext directly, `:390-413`).
Fix sketch: repair of N over a foreign occupant should drop the occupant if the allocation tree no longer lists it (the repairer already can look up O).

### X7. Rebuild-written timestamps feed the repair completion gate that is not monotone (rebuild 5.2 x repair F3/F13 x requirements F4)

> **2026-11-15 (Todo 4.5): CLOSED.** (a) cannot happen: object timestamps never decrease, and a rebuilt or repaired
> store's copied timestamp is a read's, the max over stores holding the read's revision, so once a read reflects the
> entry's transaction the gate passes (repair F3, FIXED, with the argument and tests). (b) was repair F13, fixed
> 2026-11-14: a DataObject read older than the store with a differing revision is stale, not complete.

> **2026-11-14 (Todo 2.2): NARROWED.** Requirements F4 is fixed: applies no longer lower an object timestamp, so blind
> writes and refcount updates cannot move timestamps backwards. Part (b), the DataObject skip on
> `read.timestamp < current.timestamp` (repair F13), and the gate's reliance on copied timestamps remain (Todo 2.5,
> 4.5).

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
> **Refresh (2026-11-11): narrowed.** Recovery F1 is fixed: a creation failure is logged at error and retried with
> backoff (`TransactionDriver.startFinalizer`), so a host that loads the type later (upgrade completes) finalizes the
> transaction, marks included. While the skew lasts the transaction still stalls with its CRL state, now visibly.
Verdict: PLAUSIBLE.  Severity: liveness, then stale-replica.
`RegisteredTransactionFinalizerFactory.create` throws `NoSuchElementException` on an unknown type and the driver never finalizes (recovery F1). In a rolling upgrade, a new host
can start transactions with new FinalizationAction types while old hosts, whose stores hold the CRL state, become recovery drivers for them (idle 5 s). Each wedged
transaction keeps CRL state and admission bytes and, because finalization (hence ErrorTree marks) never runs, the lagging store gets NO entry. Combined with X5 this is
a correlated trigger across all stores of a pool at once. Neither recovery nor systemic-loss mention version skew of finalizer types.
Fix sketch: refuse to drive a transaction whose types are unknown; carry a protocol/feature version in the txd and have old hosts not become drivers.

### X9. Metadata recursion: bootstrap-pool tracking hole composes with repair-entry deletion tracking disabled (rebuild F8 x repair F7 x systemic)

> **NARROWED (2026-11-14).** Opportunistic rebuild is removed, repair F7 is fixed (entry deletions are tracked except in
> the bootstrap pool's own trees), and every scan of a bootstrap-pool store now sweeps its copy of every node of the
> bootstrap pool's ErrorTree and UnackedCommits trees (rebuild F8 FIXED). Residual: after a missed untracked write the
> store stays stale until its next scan, a worst-case window of about one `scanIntervalCap` (15 min) plus a scan, and
> inside that window two bootstrap stores stale on different nodes could still let a quorum read lose a just-written
> entry. Accepted for now; see the "Bootstrap tree sweep staleness window" entry in `Backlog.txt` (suggested fix: a
> separate, short sweep interval in `RepairPolicy`).

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
> **Refresh (addendum): unchanged; one stub-specific case.** A rebuilt store has no stubs for the entries it inherits;
> the mute protects it except with a fast client clock (rebuild-process N3, paxos P-N2).
Verdict: PLAUSIBLE-low.  Severity: stale-replica.
Keys are `(poolIndex, objectId)` / `(poolIndex, txid)`; values carry no incarnation. After an in-place rebuild or T12 move the new store consumes entries written for the old
incarnation: harmless (reconcile is exact), but the opposite order is unprotected: entries consumed (deleted as complete) by the old incarnation shortly before its loss mean
the rebuilt store's later zombie slices (rebuild reading an object that is deleted a moment later; the entry E for the delete was written before the restore) rely on E surviving.
Verified OK in the normal flow (E is written 2 s after commit and kept until the store is repairable), listed as a place where X1 turns it into data loss.
> **2026-11-14 (Todo 2.12): NARROWED.** *The N3 leg is closed, without a clock.* Each store directory keeps an
> `incarnation` UUID (`server/store/StoreIncarnationFile`): load reads it or creates it durably before the store
> serves, `RebuildingStore.finish` writes a fresh one before its `rebuild-complete` marker, a transfer carries it
> with the archived directory, and an adoption takes over the device, the directory and its file unchanged. Every
> prepare reply carries it; the driver records it for every store it hears from, a takeover driver included (a store
> reporting two different incarnations is recorded as unknown), and `MissedUpdateFinalizationAction` writes it into each
> UnackedCommits entry (unknown only for a store that never answered the finalizing driver, or that answered it with two). A store applies T
> through an entry only when the entry names its own incarnation (`Frontend.unackedLookupComplete`, then
> `Tx.unackedCommitFound`). Another or an unknown incarnation concludes T committed with nothing applied
> (`Tx.unackedCommitNotOwed`), never as an abort, and the entry is left for the scan, whose exact reconcile heals the
> store (rebuild-process N3, FIXED). *The reverse direction is narrowed.* An old incarnation consuming entries
> written for the new one is still kept out by the repair fence, not by the entry, and the fence is tighter:
> `StoreRepairer.ownedHere` requires the pool entry to name both this host and the device the store is loaded from
> (`HostStoreView.storeDevices`, `RepairTarget.localDevice`), failing closed on an unknown device or a failed read, so
> a same-host device move is fenced too; and a per-scan `OwnershipFence` re-checks ownership before each ErrorTree or
> UnackedCommits delete, reusing an answer at most one second old (`StoreRepairer.OwnershipRecheck`). A negative answer
> abandons the rest of the scan and leaves the remaining entries (and their stubs) for the owner. Residual: a delete
> that commits inside the re-check window (about one second plus the delete's latency) after the pool entry moved.
> Reaching it needs the old incarnation still serving after its move, that is, a mistaken failure of a live device.
> Such a device keeps serving until its host reads it Failed and retires it (X1). Closing it outright would put a requirement on the
> pool state object into every repair delete, adding the bootstrap pool to each. *ErrorTree entries carry no
> incarnation, by design.* Their writer has no authoritative source: a store is marked because it was silent, so the
> only source would be a possibly stale cached `StoragePoolState`, and ignoring a mark it got wrong would leave a real
> miss unrepaired forever. ErrorTree reconcile is exact, so marks a new incarnation inherits cost one reconcile each
> and are harmless. The production and consumption timeline is G5.

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
Ruling 32 corrects Ruling 17's "one store, transient" wording to this quorum reach, for exactly T's writes in a pool
where T has no pin of its own (blind or `replaySafe` writes, ABA-prone `DoesNotExist` inserts, allocations in another
pool; KeyRevision-checked writes are pins and fail), within each store's scan window of 30 s to 15 min. No closure is
implemented.
Fix sketch: as R-N1; and have an allocating update fail at apply when the backend already holds the object.
> **Refresh (addendum): NARROWED.** Each P store that forgot T on a `TxFinalized` listing it in `owed` holds an applied
> stub and applies nothing on step 3 (`Tx.scala:998-1000`), so the quorum overwrite needs entries without stubs: the
> second driver's entries after its host crashed or its write gave up (transaction-recovery R-N7, X13), or a lost
> `TxFinalized` of a re-run finalizer (R-N6). The never-saw-T minority (R-N9) reaches fewer than a quorum and is
> reconciled from the majority. The end-to-end form of steps 1-3 (with a `DoesNotExist` insert instead of an
> allocation) is ReplayIntegrationSuite's headline test, which now passes. The `Allocating` state is unchanged, so the
> overwrite remains wherever a commit does reach a stubless store.
> **Refresh 2026-11-14 (Todo 2.3): the allocation leg is CLOSED for live objects.** An allocation reads the store
> first and refuses an object the store holds (`ObjectExists`, requirements F11), so step 3 applies nothing for an
> allocation. What remains of X11 is T's other unpinned writes (blind or `replaySafe` writes, ABA-prone
> `DoesNotExist` inserts) and an allocation whose object was deleted, which comes back as an orphan slice (Todo 2.9).
> **2026-11-14 (Todo 2.9): CLOSED at a quorum by the transaction horizon.** Step 2 needs a fresh Tx at a P store that
> applied and forgot T; such a store never rejoins T (finalized cache entry, then the forget floor). Only stores that
> never saw T (below a quorum) can apply it late, and their entry's reconcile heals them, now always after the apply
> (transaction-recovery, re-assessment "replays through entries with no stub", 2026-11-14, Todo 2.9).

### X12. Recovering acceptor x replay x finalization re-run (paxos P-N3 x recovery R-N6 x requirements Q-N5)
Verdict: PLAUSIBLE.  Severity: resurrection (blind writes), stale-replica (orphan allocation entries, regressed marks).
A store offline beyond the status-cache window that holds T's accepted commit makes any post-proof quorum it joins
non-memoryless, so a driver adopts commit normally (`TransactionDriver.scala:264-268`) and re-runs T's finalization
actions long after the original ran (Ruling 13): the allocation action may re-insert an entry for a deleted object
(orphan), the marker re-marks stores and re-creates deleted UnackedCommits entries, and a replayed client prepare in the
same window applies its blind writes on the non-late commit. Each step is accepted in isolation by a ruling; together
they let one long outage plus one delayed duplicate leave three different kinds of residue.
> **Refresh (addendum): NARROWED.** The re-created UnackedCommits entries come with a non-late `TxFinalized` listing
> them, so every store with no Tx writes a stub (recovery R-N6); stores holding a stub apply nothing on the non-late
> commit and are marked (paxos P-N3). Blind writes at stores that acknowledged in time, the regressed marks and the
> orphan allocation entry remain.
> **2026-11-14 (Todo 2.4): NARROWED.** The marker leg is closed: a re-run marker never regresses a mark or rewrites a
> held UnackedCommits entry, and an entry it re-creates after the scan deleted it changes nothing (repair-process F10,
> recovery R-N6). Blind writes at stores that acknowledged in time and the orphan allocation entry remain (Todo 2.9,
> 5.5).
> **2026-11-14 (Todo 2.9): NARROWED to the orphan allocation entry (Todo 5.5).** Stores that acknowledged in time saw
> T and forgot it, so they hold no replay Tx for the non-late commit to resolve. A re-adoption is possible only inside
> the horizon, and then T applies only at stores that never saw T (transaction-recovery, re-assessment "replays through entries with no stub", 2026-11-14, Todo 2.9).

### X13. Second driver's entries x marker give-up x late commit (recovery R-N7 x X6 x R-N1; addendum)

Verdict: PLAUSIBLE.  Severity: resurrection (unpinned-pool writes, possibly at a quorum).
Ruling 3 makes a second driver D1, finalized by D0's non-late commit `TxFinalized`, wait for the entries its cancelled
marker already handed off and list them in its own `TxFinalized` (`TransactionDriver.scala:414-437`). The stores forgot
T on D0's message, so they get stubs for D1's entries only from D1's (`Frontend.scala:763-773`). D1's write can give up
on a permanent error in one pool after another pool's batch committed (`MissedUpdateMarker.scala:153,157`); D1 then
finalizes with D0's `owed` only (`TransactionDriver.scala:432`), and the committed pool's entries are listed nowhere.
D1's host crashing between its write and its message has the same effect. D1 marks every store that did not report to
it (voters unknown), which with `TxCommitted` going to `lastProposer` (paxos F10) can be a whole pool, so a replay
before the scans re-applies at a quorum (X11 step 3). Each piece is accepted on its own (Ruling 3's residual, the
deferred Task 3 minor, X6's give-up); together they reopen X11 for one pool.
Fix sketch: as recovery R-N7; at least, on a give-up D1 could list the stores of the batches that did commit (the
marker would have to report per-pool results instead of one all-or-nothing future).
> **Refresh (final fix wave, Rulings 7 and 8): NARROWED.** D1 no longer waits: on D0's message it cancels its finalizer
> and finalizes at once with D0's `owed` union the planned set its marker recorded at the hand-off
> (`MissedUpdateFinalizationAction.scala:53-55,141`; `TransactionDriver.scala:411-430`). The give-up leg is closed: the
> planned set lists every store whatever the write's outcome, the committed pools' stores included, without per-pool
> results. The crash leg shrinks to D1's host crashing between its hand-off and its prompt `TxFinalized`, or that
> message lost at a store. A stub can now precede its entry, so the scan drops an orphan stub only after two
> consecutive successful scans found no entry (Ruling 8, `StoreRepairer.scala:95-104,187-200`); a D1 batch still in
> flight across two whole successful scans of a store would leave that store an entry without a stub (residual).
> **2026-11-14 (Todo 2.9): CLOSED by the transaction horizon.** Every store D1's entries name forgot T on D0's
> message and never rejoins T, so no replay Tx exists there to apply through an entry. The crash leg and the two-scan
> leg no longer matter (transaction-recovery, re-assessment "replays through entries with no stub", 2026-11-14, Todo 2.9).

### X14. Ruling 3's delayed `TxFinalized` x status-cache expiry x replay (recovery R-N8 x paxos P-N3 x P17; addendum)

Verdict: PLAUSIBLE (mechanism CONFIRMED).  Severity: resurrection (blind writes at stores that acknowledged in time).
D1's non-late commit `TxFinalized` waits for its handed-off write, which retries without a deadline. Once the stores'
finalized cache entries expire (7 s then; 30 s since the horizon, past which the stores refuse the replay, paxos P17 and 10.10), a replayed client prepare creates fresh Txs, and D1's message resolves
each of them committed (`Tx.scala:945-946`), applying the replay's data where no stub exists. The cache window that
made D1's prompt `TxFinalized` harmless before Ruling 3 no longer covers it.
Fix sketch: as recovery R-N8: send the delayed message late but keep its `owed`, and have stores honour `owed` on late
messages (an owed store with a Tx writes the stub and applies nothing; one without writes it on the no-Tx path). A
plain `late = true`, or a bounded wait, empties `owed` and reopens X13.
> **Refresh (final fix wave, Ruling 7): FIXED** by a different route than the sketch: D1 does not wait at all. It
> finalizes non-late on D0's `TxFinalized` at once, inside the stores' cache window, listing its planned stores, and a
> finalized driver sends no `TxResolved` and is unaffected by later late messages (recovery R-N8's refresh). Ruling 4
> stays: late and abort messages carry an empty `owed`.

## 2. Coverage gaps (failure scenarios no document covers)

G1. Fast rebuild or lost CRL vs a long-unresolved transaction (X2): no document states how long a transaction can stay unresolved or tests promotion of a blank acceptor against it.
G2. Mixed-version/rolling-upgrade protocol skew (message formats, finalizer types, CRL entry format, txd fields): systemic doc covers detectors only; recovery covers F1 only for jars.
G3. Zombie drivers/timers after a non-crash store reload, device relinquish and transfer-out (X4). Only recovery F12 mentions it, with no interaction analysis.
G4. A shared, in-memory host-wide status cache is the anchor for finalization relays and, since the refresh, late-commit evidence (condition 2); no document analyses host restart (cache wiped) combined with rolling restarts of the stores' peers (paxos F6 touches it for one restart only). The horizon and TooOld no longer depend on it. *(2026-11-13: the safety half is covered. A restart, transfer-in or size eviction raises the affected stores' forget floor (load + `MaxStartLead`, or the eviction watermark) at the moment memory is lost, so a store whose host lost its cache never rejoins a transaction it may have forgotten, however many peers restart (paxos 10.10; HorizonIntegrationSuite "a host restart does not let its stores rejoin a transaction they forgot"). Left: the liveness cost across rolling restarts, where each reload refuses the transactions it does not hold started before its load plus `MaxStartLead`, and too many refusing stores in a pool end those transactions Unknown.)*
G5. ErrorTree/UnackedCommits entry-key incarnation (X10) and the time-line of entry production vs consumption during a T12 move are described only per-document.
    *2026-11-14 (Todo 2.12): written.* Entries for a pool slot are keyed by its pool index; since Todo 2.12 an
    UnackedCommits entry also records the incarnation of the store that cast the counted commit vote (X10). I0 is the
    old incarnation, I1 the rebuilt one.
    *In-place rebuild* (T1-T11: lost or corrupt CRL, lost backend; same host, same device, rebuild-process 4.1):
    (1) *I0 serving.* Finalizers of each T in which I0 cast a counted commit vote and sent no `TxCommitted` write
    UnackedCommits entries tagged I0 (unknown only when the finalizing driver never heard from I0, typically after a takeover).
    Transactions I0 was silent for write ErrorTree marks. I0's scans consume both.
    (2) *Marked and Rebuilding.* I0 is discarded with its CRL, stubs included (rebuild-process N3). The store is offline
    and not repairable, so nothing scans the slot. Finalizers still running for T that I0 voted in keep writing entries
    tagged I0 (or unknown). Transactions in the pool mark the slot in the ErrorTree, since the store is silent. I1
    casts no vote yet: it does not serve, and once it does it is muted for every T started before its `acceptorSince`.
    (3) *Finish.* `RebuildingStore.finish` writes a fresh incarnation I1 before the `rebuild-complete` marker, so a
    finished rebuild never keeps I0's identity, and the flip makes the store Active. From here on, entries for new T
    are tagged I1.
    (4) *I1 serving.* I1 inherits every entry of stages 1-2. An inherited UnackedCommits entry names I0 or nothing, so
    no Tx of I1's (a replay's fresh Tx; a recovered vote of I1's own always matches I1) applies through it: the lookup
    concludes T committed with nothing applied (`Tx.unackedCommitNotOwed`), and I1's scan reconciles the entry's objects
    exactly and deletes it. Inherited ErrorTree marks are reconciled exactly, costing one reconcile each.
    *Move to a new device* (T12, `FailedStorageDeviceDurableTask.moveStore`, onto device D1, on the same host or
    another): stages 1 and 2 are as above, except that the pool entry is rewritten to the destination (host, D1) in
    the same transaction that records the store Rebuilding on D1. Stages 3 and 4 run on D1 and are unchanged. The difference is that
    I0 may still exist: if D0 was failed by mistake while still working, I0 serves on D0 until its host reads D0 Failed
    and retires it (X1).
    *Who may consume entries.* Only the store the pool entry names on the device it names
    (`StoreRepairer.ownedHere`: host and device both, failing closed on an unknown device), and only repairable
    (served, not Rebuilding) stores are scanned. Ownership is re-checked before each ErrorTree or UnackedCommits delete
    (`OwnershipFence`, answers at most one second old), and a negative answer abandons the scan. So in an in-place
    rebuild only I1 ever consumes the slot's entries after stage 1, and in a move I0 is fenced out from the flip,
    except a delete committing within about one second plus its latency of the flip (X10's residual).
    *Why ErrorTree marks carry no incarnation.* A mark's writer has no authoritative incarnation (only a possibly stale
    cached `StoragePoolState`), and ignoring a mark it got wrong would leave a real miss unrepaired forever; an exact
    reconcile makes an inherited mark harmless.
    *Transfers and adoptions* are not rebuilds: a transfer's archived directory carries the incarnation file, and an
    adoption takes over the device with the directory unchanged, so the store keeps its identity and keeps applying
    through its own entries.
G6. Allocation-tree dependency: rebuild F10/F17, repair F5 and requirements F11 each mention an allocation recorded late or not at all, but nobody analyses the total effect (missing slice, stale deletion decision, replayed allocation) across rebuild, repair and horizon.
    *2026-11-14 (Todo 2.11): analysed.* A missing entry cannot occur: `TransactionBuilder.addAllocatingObject` adds
    the allocation action, and bootstrap lists what it creates (rebuild F17, closed). A late entry has three effects:
    (1) *Missing slice.* A rebuild walk that passed the key never restores an allocation the lost store acknowledged.
    The delta walk restores entries written since the walk started, and the allocations still pending are listed by a
    quorum of the pool's other stores (`CurrentAllocationsRequest`) and restored directly (rebuild F10, fixed).
    Residual: the pool lost the Paxos state of `writeThreshold` stores while T was unfinalized (beyond its tolerance).
    (2) *Stale deletion decision.* Repair no longer takes absence from the tree as a deletion. A read decides
    (`InvalidObject.provesDeletion`, repair F5 fixed), so a late entry costs only deferral. Residual, unrelated to
    lateness: an object already below its write threshold reads as deleted.
    (3) *Replayed allocation.* It never overwrites a live object (requirements F11). Under the horizon it reaches only
    stores that never saw T, where a deleted id becomes an orphan slice that repair drops. A re-run allocation action
    no longer leaves an orphan entry (Todo 5.5, fixed 2026-11-15: it writes the entry only under the object's revision
    lock and skips a proven deletion).
    The one cross-process interaction: an orphan entry names an object that reads as deleted, so a rebuild records it
    as failed and `retryFailures` finds the entry still present. That rebuild cannot finish (rebuild F15, Todo 4.8),
    so orphan entries are a liveness cost on rebuild, never lost data. Todo 5.5 (no orphan entry, done 2026-11-15)
    and 4.8 (an operator command to finish past lost objects) together close it.
G7. Client object caches/pointers (stale `ObjectPointer`, DataObject read caches) are outside all seven; resurrection by a client reading a zombie slice with a cached pointer is not discussed.
G8. Clock: HLC skew no longer affects replay (the horizon is gone, paxos F3 fixed), but still affects the rebuilt store's `acceptorSince` mute, on which the late-commit rule now also rests (paxos P-N2), repair's 60 s age gate (entry timestamp is the marker transaction's), and rebuild timestamps; no document looks at them together.
    *2026-11-13, partial:* the `acceptorSince` leg is settled (P-N2 FIXED). It now depends only on host-to-host skew
    under 8 s, and `Frontend.MaxStartLead` caps every accepted start timestamp, and so every client HLC, at the fastest
    host's wall clock plus 2 s. Still open: the repair age gate and rebuild timestamps under host skew.
G9. Marker-batch size/limit: `TransactionCreationError` is treated as permanent (StopRetrying) and wedges finalization (recovery F2); no document asks which creation errors an oversized batch (256 entries x many objects) can hit. *(2026-11-11: no longer a wedge. The give-up fails the actions and their drivers re-run the finalizers with backoff, so a transient creation error heals; one that repeats forever is a logged stall. An oversized UnackedCommits entry surfaced as TKVL `NodeSizeExceeded`, retried in place forever: about 90 objects on one store with 4096-byte FSIF metadata segments. Fixed: such entries are written in chunks of at most half a leaf node (repair-process 13.0b), and a `NodeSizeExceeded` left over is permanent, taking the finalizer failure path. ErrorTree entries are fixed-size and small.)*
G10. Capacity interaction: ErrorTree growth during long outages (repair 13.3-6) x CRL admission (X5) x rebuild in-progress counts as failure budget (rebuild 6.3): no combined capacity argument.
    *2026-11-15 (Todo 4.3), the CRL admission leg:* ErrorTree and UnackedCommits growth costs backend space in the
    metadata pools, not CRL bytes: an entry occupies a device's CRL admission only while the marker batch writing it is
    in flight, and batching (`MissedUpdateMarker`) bounds those batches per pool. Every committed but unfinalized
    transaction holds its admitted bytes until its finalization actions complete. Lanes keep them from starving each
    other: tier t admits while the device total is below c(t), c(Data) = 3/4 L < c(Metadata) = 7/8 L < c(Bootstrap)
    = L (L = half a CRL stream). A transaction's marker batch is one tier up (the bootstrap tier's own markers track
    nothing). Lower tiers stop being admitted at their ceiling, so each higher tier keeps about L/8 of headroom on every
    device. The exceptions are charges made without a check: one transaction crossing its ceiling, object data
    arriving after admission, records recovered at load, and applied stubs. The first three are bounded by the
    transactions already admitted. Stubs are not bounded while repair scans stall (CRL F22, Todo 4.6). A
    transaction of tier t then finalizes once a write threshold of the next tier's stores can admit one batch, which
    lower-tier load cannot prevent. What can still fill a lane is the tier's own traffic, including same-tier
    finalizers (TKVL split, join, tail removal): that is ordinary back-pressure, and it clears as the tier's own
    transactions finalize upward. Still open: the rebuild leg (in-progress rebuilds counted against the failure
    budget, rebuild 6.3) and UnackedCommits growth (Todo 4.7).

G11. Replay ordering: the replay integration tests deliver the leader's copy of the replayed client prepare last (the TestNetwork leader driver does not retransmit), so the ordering where a driver's data-less prepare reaches stores before the replayed client prepare is not exercised end to end. *(Addendum: the headline stub test delivers the pool-B leader's copy before pool A's, because the default order dropped the provers' proofs at a leader with no driver yet (paxos P-N1). Prefer-abort makes a driver-first store withhold everything until the client's prepare arrives, then check it normally, or prove at once (`Tx.awaitingClientPrepare`, `Tx.scala:205-208`); FrontendSuite covers both at unit level, end to end is still untested.)*
G13. (addendum) No document or test covers a second driver's handed-off entries across its host's crash (X13), nor a `TxFinalized` delivered after the stores' cache entries expired (X14). *(Final fix wave: X14's delayed `TxFinalized` no longer exists; the unit suites cover D1's prompt finalization with the planned set, its silence afterwards, and the two-scan stub drop. The crash leg of X13 is still untested.)* *(2026-11-14, Todo 2.9: closed. X13 is closed by the horizon, and the state
the crash leaves, every store with an entry and no stub after forgetting T, is tested end to end: ReplayIntegrationSuite
"a replay at a pool whose stores hold entries and no stubs applies nothing, inside and past the caches". The
single-host TestNetwork cannot run the second driver on another host, and the outcome does not depend on it.)*
G12. amoebafs pre-existing bugs found during the refactor (cross-node rename whose delete joins into the target node updates it twice; stale `iptr` in `prepareMove`; `prepareDelete` able to end unpinned) are outside these documents' scope and are recorded in Todo.txt. **Fixed (2026-11-11), the cross-node rename:** `SimpleDirectory.prepareRename` stages both keys as one `TieredKeyValueList.prepareUpdates` batch, so the joined node is updated once; the old key is deleted at the revision it was read (`deletesAtRevision`, else `KeyNotAtRevision` aborts) and the new key written under `MustNotExist` or `AtRevision` of the entry it replaces. **Fixed (2026-11-11), the stale `iptr`:** `Directory.prepareMove` no longer takes the pointer from its caller; `prepareMoveOut` reads the source entry inside the attempt and deletes it under that read's `KeyRevision`, returning the pointer it held, which is what is moved (and, for a directory, whose inode's parent pointer is rewritten at its freshly read revision). `Directory.move` holds the retry loop that `AmoebaNFS.move` now calls. **Fixed (2026-11-09, `29e94080`), the unpinned `prepareDelete`:** the delete goes through `TieredKeyValueList.deleteIfPresent`, and when the node it plans against no longer holds the entry (nothing staged, so no pin) it fails with `DirectoryEntryDoesNotExist` as if the entry had been absent from the start (DirectorySuite "a delete whose entry a concurrent unlink removed after it was read fails as an absent entry"). `prepareMoveOut` shares the path. All three G12 items are now fixed.

## 3. Inter-document contradictions

C1. systemic-loss-protection X11 says a Failed device is "refused (DeviceDiscovery.scala:219)" and "by design never reintroduced"; rebuild-process F1 says it is loaded again. Code: both true in different branches. The refusal is in `resolveOwnership`
    (`:215-219`) used when `owning-host` names another host or a shutdown marker exists; the `owning-host == this host` branch calls `loadOwnedDevice` with no tree check (`:474-478`). Rebuild F1 is right for a device whose owning-host is the local host.
C2. repair-process 12.8 says marker inserts use `retryUntilSuccessful` and "No entry is lost"; rebuild F9 and transaction-recovery F2 say the batch gives up. Code: both: transient errors retry, but `UnknownStoragePool` and every `TransactionCreationError` throw `StopRetrying` *(2026-11-11: resolved. `UnknownStoragePool` is no longer handled (nothing throws it); a `TransactionCreationError` give-up fails the actions and their drivers re-run the finalizers, which mark again, so "No entry is lost" now holds: rebuild F9 and recovery F2 fixed.)*
    and `finished` only logs "Gave up" (`MissedUpdateMarker.scala:127-157`); repair 12.8 is wrong on "no entry is lost".
C3. rebuild-process F14 ("Paxos acceptor amnesia is safe only within the pool's failure tolerance") contradicts paxos-transactions P8/P9 (state of a possibly chosen value must be retained) and crash-recovery-log F18 (open question). Quorum arithmetic says tolerance
    does not cover amnesia (X2). The three documents give three different answers.
C4. transaction-recovery F10 and paxos F9 call the status-cache downgrade low impact/"hygiene" and "not unsafe"; requirements F1/F2 and X3 show a re-apply path for blind writes. *(Refresh: resolved in the documents. Since Ruling 30 the downgrade is fixed: the cache is monotone.)* The documents disagree on whether the cache is a safety mechanism (paxos P17 says assumption) or an optimization (recovery F10).
C5. paxos F2 / recovery F3,F5 say the false-abort is "repaired through the ErrorTree/UnackedCommits entries recorded by the finalizer", but transaction-recovery F13 and requirements F8 say stores that were not counted voters, or traceless changes not covered by `tracelessObjects`, get no entry; the repair-process documents assume an entry always exists. *(Refresh: the traceless half is gone, requirements F8 fixed; the uncounted-voter half, recovery F13, stands.)* *(2026-11-13: the false abort itself is gone, paxos F2 and recovery F3, F5 closed by the horizon; an expiring commit voter discards only when its lookup finds no entry. An uncounted voter that discards relies on its ErrorTree mark, as before, so recovery F13's gap is unchanged but no longer meets a false abort.)* *(2026-11-14: RESOLVED. Recovery F13/F15 are closed by design: entries stay limited to unacknowledged counted voters, a write threshold of which holds T or owes it, and every other store without `TxCommitted` is repaired by its ErrorTree mark. repair-process 9.2 now states which stores get which entry.)*
C6. rebuild-process 8.2 says ErrorTree repair "closes the residual staleness gap"; repair-process 13.2 F1/F2/F3/F4/F13 each describe cases where repair itself leaves the store stale or declares completion early. The rebuild guarantee rests on the repair guarantee that is not established.
C7. transaction-recovery F6 (persisted `Committed` status is not restored into the recovered Tx; relies on the cache) vs crash-recovery-log 3.1 line 149 ("No decision is saved"): the CRL doc says the resolution is not a record, but recovery F6 says it can be persisted by a re-save; both are true depending on whether a prepare/accept re-saved the Tx after resolution. Should be stated once. *(2026-11-14: RESOLVED. Recovery F6 is fixed: a re-saved resolved status is restored into `oresolution`, and crash-recovery-log 3.1 now says that no save is made because of a resolution, but any later save persists the status, which recovery restores.)*

## 4. Candidates examined and REFUTED (do not redo)

R1. Stale allocation-tree lookup causing a false "absent -> repairDelete": `pool.allocationTree.get` reads the tier-0 leaf fresh (only tier >= 1 comes from `LruTierCache`; `TierEngine` navigate/fetchContainingNode) and a stale upper tier is corrected by following links. No false absence from the cache.
R2. FSIF slot reuse aliasing a new object with a stale one: delete of an occupant mismatch returns `ObjectMismatch` ("not deleting it", `updateExt` `:1279-1285`), `startRepairDelete` checks `index.ids(x) == objectId`, `readExt`/`finishRead` check id, allocation over a missed-delete occupant is an explicit overwrite (Ruling 18). Remaining issue is only X6.
R3. Rebuild walk resurrecting a traceless-deleted key: a read quorum with one stale and one current store cannot resolve either state; the reader waits (matching+potential), so a rebuild read fails/retries rather than restoring the key. Requires more laggards than the failure budget.
R4. Quorum of {rebuilt-stale S, current S2} committing on stale requirements: revision-based requirements abort at S2 or S; `DoesNotExist/Exists` are held by S2 (the fact committed at t >= 2 stores, any t-quorum includes one holder). Only blind writes pass, by design.
R5. ErrorTree entries consumed by a rebuilding staging store: staging is a bare `Backend`, never a `Store`; `HostStoreView.repairable` excludes `rebuilding`; in-place rebuild keeps the store offline. Entries written during the rebuild wait for promotion. (Only the F4 crash window and X1 zombie bypass this.)
R6. Metadata recursion deadlock (marker txn needing its own marker): `limitTracking` stops the recursion at the bootstrap pool; the marker for a pool's entries references the metadata pool only through different pools. No infinite recursion; residual issue is X9.
R7. Device-CRL loss dropping commit votes that the other stores need: the lost store is offline for the rebuild duration, other holders answer, and the finalizer marks the silent store; confirmed except for X2's window.
R8. (addendum) Applied-stub loss across rotation, transfer or adoption: stubs are copied forward by anchor, carried in `crl_save.log` and in the device CRL (crash-recovery-log F23). Only rebuild and CRL loss drop them (rebuild-process N3).
R9. (addendum) The repair scan dropping a stub while a replay's late commit at that store looks up its entry: a stub holder never looks up, and the drop follows the entry's deletion (repair-process RP-N8).
R10. (addendum) Prefer-abort turning CRL backpressure into abort votes for live clients: a known transaction's retransmit is admitted without a check, within the 5 s wait (paxos P-N9).
R11. (addendum) A misdirected `owed`: every store of the transaction receives the same set and checks its own `StoreId`; a moved store keeps its `StoreId`, and a rebuilt one is muted for T, so a stub written there is harmless.
