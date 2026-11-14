# Safety specifications

These documents state, as checkable specifications, the correctness guarantees that were previously embedded only in
the code. They were derived from the code on 2026-11-09 and refreshed the same day against `0d08e3c6` (branch
`pin_refactor`), after the replay horizon was replaced by pinned transactions
(`docs/superpowers/specs/2026-11-09-replay-pins-design.md`). Each document has a "Re-assessment for replay pins" table
marking every earlier finding fixed, narrowed or unchanged, and a section of new findings. They were refreshed again on
2026-11-10 against `c181375b` for the replay-pins addendum
(`docs/superpowers/specs/2026-11-09-replay-pins-addendum-design.md`: prefer-abort driver prepares, applied stubs in the
device CRL, `TxFinalized.owed`), with a "Re-assessment for the replay-pins addendum" table and new findings in each.
The final fix wave of that addendum (amendments 7 to 9: a second driver finalizes at once with its planned set, an
orphan stub is dropped only by the second scan to find it so, and a deferred finalization is concluded only by its own
Tx) is recorded as "Refresh (final fix wave)" notes on the findings it changes (R-N7, R-N8, R-N11, P-N10, X13, X14,
RP-N6, RP-N7) and in the rows below. The 2026-11-11 follow-ups (the finalization failure path, chunked
UnackedCommits entries, explicit ownership of deferred finalizations, and a refresh of every line citation in
paxos-transactions.md) are recorded as dated notes on the findings they change and in the rows below. The
2026-11-13 transaction horizon (`docs/superpowers/specs/2026-11-13-transaction-horizon-design.md`: past 20 s no store
joins a transaction it does not hold, outcomes are remembered 30 s, stuck Txs expire from their peers' status, and the
client reports `TransactionOutcomeUnknown`) has a "Re-assessment for the transaction horizon" table in
paxos-transactions.md and transaction-recovery.md, its mechanism and safety argument in paxos-transactions.md 10.10,
and is reflected in the rows below. Each has its own "Suspected flaws" section, annotated with adversarial verification verdicts:

- **CONFIRMED**: the mechanism was traced in the code, or reproduced.
- **PLAUSIBLE**: the mechanism is real, but the trigger is uncertain.
- **REFUTED**: a guard was found.

| Document | Scope |
|---|---|
| [paxos-transactions.md](paxos-transactions.md) | Paxos roles, message flow, votes, rounds, persistence ordering, the pin rule, proof-of-commit and late commits |
| [crash-recovery-log.md](crash-recovery-log.md) | CRL contract, its place in the workflow, SimpleCRL design, on- and off-device trade-offs |
| [transaction-requirements.md](transaction-requirements.md) | Requirements and operations, locks, vote tallying, commit-in-flight ordering |
| [transaction-recovery.md](transaction-recovery.md) | Driver and participant crash recovery, the missed-update feed into the ErrorTree and UnackedCommits |
| [repair-process.md](repair-process.md) | ErrorTree and UnackedCommits schemas, scan, exact reconcile, races with transactions |
| [rebuild-process.md](rebuild-process.md) | Lost-store triggers, the allocation-tree walk, placement, promotion |
| [systemic-loss-protection.md](systemic-loss-protection.md) | Correlated false "store lost" declarations. **No solution has been identified yet.** |
| [cross-document-review.md](cross-document-review.md) | Flaws that arise only from interactions between the mechanisms above, coverage gaps, and contradictions between documents |

## Answers to questions raised in Todo.txt

- **1RTT commit:** not implemented. Only the designated leader's driver sends `TxAccept`
  (`TransactionDriver.scala:521-531` at `c181375b`). Round 1 carries no commit-only rule, and aborts are decided in
  round 1. In the error-free case the client learns the outcome after 5 one-way hops and 2 serial CRL writes. Todo.txt
  and Architecture.md describe a protocol the code does not implement (paxos F1). Either the protocol needs
  implementing or the description needs correcting.
- **Is RevisionLock exclusive?** Yes. It takes the same exclusive object lock as a DataUpdate and changes nothing.
  **Fixed (2026-11-10):** renamed to `ExclusiveRevisionLock` (transaction-requirements §12).
- **Two failure-recovery modes?** Driver loss and participant loss are the roots. A lost `TxFinalized`, a recovered
  commit vote whose peers forgot the transaction, and missed updates handed to repair each need separate treatment
  (transaction-recovery §1).

## Ranked findings

The ranking is by worst plausible consequence, then by confidence. IDs refer to each document's flaw section; `X*` ids
refer to cross-document-review.md. New findings from the replay-pins refresh carry `-N` ids (`P-N*` paxos, `R-N*`
recovery, `Q-N*` requirements, `RP-N*` repair). Findings from the addendum refresh continue those series (P-N7 to
P-N10, R-N7 to R-N12, Q-N7, Q-N8, RP-N6 to RP-N8, CRL F22 to F24, rebuild N3, cross-document X13 and X14; not systemic-loss X13/X14, which are discard-inventory ids).

### Tier 1: lost durable state, split decisions, systemic loss

| Id | Verdict | Finding |
|---|---|---|
| CRL F1 | **FIXED** 2026-11-09 (was CONFIRMED, reproduced) | `StreamWriter.scala:74` ignored the partial `write(ByteBuffer[])` (IOV_MAX = 1024): large entries acknowledged but unreadable |
| CRL F2 / F20 | **FIXED** 2026-11-09 (was CONFIRMED, reproduced) | Recovery gave the active stream the oldest visited entry's UUID; a second restart after a rotation threw `BrokenEntryChain` |
| CRL F6 | **FIXED** 2026-11-09 for transient errors (was CONFIRMED) | I/O and VM errors are retried on 3 scans before the CRL is declared lost; other failures still declare it lost, by decision |
| Rebuild F2 | **FIXED** 2026-11-09 (was CONFIRMED) | The old copy and its saved CRL entries stay in `pending-delete/` until the store is Active again |
| X2 | **FIXED** 2026-11-09 (was PLAUSIBLE) | A rebuilt store mutes prepares for transactions started before its `acceptor-since` time |
| X1 / Rebuild F1 | **FIXED** 2026-11-09 (was CONFIRMED) | A Failed device is retired (marker plus unload), never reloaded, its former host nudged, repair scans fenced on the pool entry |
| CRL F3 | **FIXED** 2026-11-09 (was PLAUSIBLE, reproduced by a torn-write test) | Rotation copy-forward overwrote the stream holding the only copy of live data |
| CRL F4 | **FIXED** 2026-11-13 for the phantom (was CONFIRMED, reproduced) | A save after a delete in the same unwritten entry left a live, never-written `Tx` that pinned `oldestEntryNeeded`: after the ring wrapped, recovery threw `BrokenEntryChain` (CRL lost, every store on the device rebuilt). Residual: the skipped save is still acknowledged, which no load path reaches |
| CRL F10 | **FIXED** 2026-11-14 for lock-sharing openers (was CONFIRMED) | A second opener of a device's CRL files (double mount, adoption while the old process still writes) interleaved entries. `SimpleCRL` now holds an exclusive lock on `crl/lock`; a refused open (`CrlLocked`) leaves the device unloaded and is never a lost CRL. Residual: shared-storage double mounts need fencing (SL M9) |
| CRL F5 | CONFIRMED | There are no checksums on CRL entries. Bit rot or reordered sectors can become Paxos state or object data |
| Paxos P-N4 | **FIXED** 2026-11-13 by the transaction horizon (was NARROWED to the accepted corner case, PLAUSIBLE) | Prefer-abort closed the driver paths; the delayed duplicate of the client's own prepare is closed by the forget floor: every store that took part in the abort remembers it for longer than it would join T, and refuses T past its floor. `TransactionAborted` is final; the corner case is gone from the `Transaction.commit` scaladoc (HorizonIntegrationSuite "a delayed client prepare of a forgotten abort does not commit inside or past the horizon") |
| Paxos P-N2 | **FIXED** 2026-11-13 (was PLAUSIBLE) | Late condition 1 assumed a rebuilt store is always muted, but the mute trusted the client's clock. Every store now mutes an unknown transaction whose start leads its host's wall clock by more than `Frontend.MaxStartLead` (2 s), so the mute depends only on host clock skew (< 8 s). Residual: larger host skew, zombie old copies |

### Tier 2: resurrection, overwritten updates, permanently stale replicas

| Id | Verdict | Finding |
|---|---|---|
| Paxos F3 / Recovery F7 | **FIXED** (replay pins) (was CONFIRMED) | The horizon trusted the client clock. It is gone; replay is refused by the client pin rule and the late-commit rule |
| Recovery R-N1 / X11 | **NARROWED** (addendum) | Applied stubs: a store that forgot T on a `TxFinalized` listing it in `owed`, or applied T through its entry, applies nothing on a replay's commit; the whole-pool scenario is tested end to end. Residual: stores with an entry and no stub (R-N7, R-N9, R-N6's lost `TxFinalized`) |
| Recovery R-N7 / X13 | PLAUSIBLE (new, addendum; accepted residual; **narrowed**, Rulings 7-8) | A second driver D1 finalized by another driver's `TxFinalized` now finalizes at once, listing the planned set its marker recorded at the hand-off whatever the write's outcome (the give-up leg is closed). Left: D1's host crashing between its hand-off and that prompt `TxFinalized`, or the message lost at a store, and a D1 batch in flight across two whole successful scans (Ruling 8's two-scan stub drop), each leaving entries with no stub, possibly for a whole pool |
| Recovery R-N8 / Paxos P-N10 / cross-document X14 | **FIXED** (Ruling 7) (was PLAUSIBLE, mechanism CONFIRMED) | Ruling 3's wait is gone: the second driver finalizes non-late on the other driver's `TxFinalized` at once, inside the stores' cache window, and once finalized sends no `TxResolved` and ignores late messages, so no delayed non-late outcome reaches a replay's fresh Txs and no late message empties its `owed` |
| Paxos P-N3 / X12 | PLAUSIBLE (new; **narrowed**, addendum) | A recovering acceptor holding T's accepted commit makes a post-proof quorum adopt a *non-late* commit even when a proof exists (broader than the design's residual): finalization actions re-run and a concurrent replayed client prepare applies blind writes, `DoesNotExist` inserts after a later delete, and allocations. Since the addendum stub holders apply nothing and are marked; stores that acknowledged in time still apply |
| X3 | **FIXED** (Ruling 30) (was PLAUSIBLE, then NARROWED) | `TransactionStatusCache.updateStatus` is monotone (finalized and late sticky, no revert to Unresolved), so no store's write downgrades a finalized entry and the finalized relay stays late; opposite outcomes of a split decision resolve to Committed, flags from that outcome only |
| Recovery R-N6 | PLAUSIBLE (new; **narrowed**, addendum) | A driver adopting an accepted commit long after finalization re-runs every finalization action (Ruling 13): the marker's blind upsert can regress newer ErrorTree entries; a re-run allocation action can leave an orphan entry. The re-created UnackedCommits entries are now listed in its `TxFinalized`, so stores write stubs for them, unless that message is lost |
| Repair RP-N4 | PLAUSIBLE (new; **narrowed**, addendum) | An UnackedCommits entry outlives the store's own late application of T until its scan deletes it; since the addendum a stub holder applies nothing meanwhile, so the window stays open only at stores with an entry and no stub (R-N1's residual) |
| Recovery R-N9 | PLAUSIBLE (new, addendum; accepted residual) | A store named in `owed` that never saw T and missed that `TxFinalized` has an entry and no stub: a replay re-applies T there, at most `w - t` of a pool, repaired from the majority by the entry's own reconcile |
| Rebuild N3 | PLAUSIBLE-low, NARROWED 2026-11-13 | A rebuilt store has no stubs; only its `acceptorSince` mute keeps a replay from applying through an old incarnation's entry. A fast client clock no longer defeats the mute (P-N2 fixed); residual: host skew > 8 s, and entries carry no incarnation (X10). One store, normally reconciled |
| Recovery R-N11 | **FIXED** (M2; ownership explicit 2026-11-11) (was PLAUSIBLE, very narrow) | `deferredFinalizations` lists every Tx of the txid that deferred its finalization; each removes only itself and the last concludes the state, whatever order their held commits issue in. Before: only the Tx that registered a held finalization removed it or concluded the state: a replay's Tx of the same txid, forgotten meanwhile, changes neither (FrontendSuite "a replay's Tx forgotten while a finalized commit is held for its turn leaves the held Tx registered"); a skipped call's owed stub passes to the registered Tx |
| Requirements Q-N4 | CONFIRMED (new; trust model, by design) | The pin rule is enforced only by the client: a client built without the check sends replayable transactions, which get fresh commit votes and commit normally |
| CRL F13 / Requirements F2 / Recovery F4 | **NARROWED** (was CONFIRMED, narrow) | A crash re-apply of a TKVL write now fails its per-write check; blind writes and `DoesNotExist` inserts after a later delete still re-apply |
| Requirements F1 | **NARROWED** (was CONFIRMED) | A lagging store skips a stale per-write checked write and is marked; blind writes are still last-arrival-wins with no entry |
| Requirements F11 | **NARROWED** (was PLAUSIBLE; again by the addendum) | A replayed allocation can no longer commit by fresh votes (its pin fails), nor apply at a store holding an applied stub; it still applies at stubless stores where X11, X13 or P-N3 deliver the commit (X14 fixed) |
| Repair F4 | **NARROWED** (was CONFIRMED, narrow) | TKVL deletes now carry `KeyRevision`, so the invisible no-op delete fails at apply and is marked; blind deletes unchanged |
| Requirements F5 / F6 | **FIXED** 2026-11-14 (was PLAUSIBLE) | A commit voter that cannot hold the update it voted for fail-stops the store (`Frontend.failStop`, rebuilt as a failed backend is): an apply that throws, a live voter's failed apply re-check (F5: its locks are already gone), a recovered commit vote whose object fails to load with anything but `ObjectNotFound` (F6: no lock on it). Never on a full device: a write-frozen store poisons the object (abort votes until reload) and keeps serving (FrontendSuite "a commit voter whose state is invalidated under its locks ...", "a recovered commit vote whose object fails to load ..."). Residual: a recovered vote's failed re-check is not a fail-stop, since a superseded update is indistinguishable from an invalidated one; a fail-stop is a lost-store declaration (Backlog: correlated declarations) |
| Repair F1, F2, N1, N2 / Requirements F3 | CONFIRMED | Opportunistic and pre-transaction rebuilds replace KV `data` without refreshing `kvState` and skip the commit-in-flight and refcount guards |
| Paxos F2 / Recovery F3, F5, F8 | **FIXED** 2026-11-13 by the transaction horizon (was CONFIRMED, narrowed) | No store joins a transaction it may have forgotten (forget floor), so no fresh instance re-decides a forgotten commit as abort: the client gets `TransactionOutcomeUnknown` past 22 s instead of a false abort, and a live commit voter left behind ends by expiry, applying T through its UnackedCommits entry if owed. The takeover margin is 30 s against 5-6 s (F5). Residual: Unknown is retried by default, so a non-idempotent retry of a T that had committed can duplicate work (Todo audit entry); post-load refusal (below) |
| Paxos P-N1 / F10 | **CLOSED BY THE HORIZON** 2026-11-13, not fixed directly (was CONFIRMED, narrowed) | The proof is still sent once to `lastProposer`, and client retransmits still redirect it (F10), but a driver without the proof can no longer decide abort: inside the window every store that knew T answers from its `Tx` or cache, past its floor it refuses. Todo 1.3a dropped. F10's redirected `TxCommitted` still costs an extra repair mark |
| Paxos F6 | **RE-ASSESSED** 2026-11-13: no longer a split path | An abort still drops its record at resolution, but a forgotten abort cannot be re-decided (forget floor). Residual: the floor's timing relies on each host's wall clock not stepping back by more than about 8 s |
| Repair F5 | PLAUSIBLE | An allocation-tree record more than 60 s late is treated as a delete |
| Requirements Q-N5 | PLAUSIBLE (new) | `replaySafe` audit: proofs and the split action hold; the allocation action's orphan entry is less rare than stated; the marker's "idempotent" blind batch can regress newer entries and re-create deleted UnackedCommits entries |
| X6, X7, X9, C5, C6 | PLAUSIBLE | Repair coverage holes compose: marker-batch give-up, FSIF slot reuse depending on a delete entry, a non-monotone completion gate after a rebuild, untracked bootstrap-pool ErrorTree writes |

### Tier 3: liveness (hangs, stalls, leaks)

| Id | Verdict | Finding |
|---|---|---|
| Paxos 10.10 (horizon cost) | accepted residual, by design | After a reload a store refuses, for good, every T it does not hold that started before load + `MaxStartLead`; the cost beyond the horizon is the Ts started in `[load, load + MaxStartLead)`. On a single host, or after most of a pool's stores restart, such Ts end `TransactionOutcomeUnknown` at about 22 s and are retried. Stores written by bootstrap are not exempt (only `StoreRebuilds.createNewStore`'s are), so on a fresh single-host system every transaction started in the first 2 s after load ends Unknown. A durable high-water mark is the upgrade path |
| Recovery F2 / R-N2 | **FIXED** / **NARROWED** 2026-11-11 (was CONFIRMED) | Finalization has a failure path: a failed finalizer is logged and started again with backoff, never finalized without its marks. Residual (R-N2): an action failing on every attempt keeps the marks unwritten with the transaction unfinalized, a logged stall |
| Recovery F14 / X5 | CONFIRMED | Finalization still needs the marker's write; under a metadata outage or CRL admission pressure transactions pile up unfinalized |
| Recovery R-N3 | CONFIRMED (new) | Every recovered commit vote waits, holding its locks and with no driver, for its UnackedCommits lookup; a metadata-group outage stalls them all |
| Recovery R-N4 / Repair RP-N3 | CONFIRMED (new) | UnackedCommits entries are one per (store, transaction), never merge, and are now written for every transaction with a silent voter: tree growth, larger marker batches, double reconciles |
| Cross G9 | **FIXED** 2026-11-11 (was a coverage gap) | An UnackedCommits entry naming more objects than half a metadata-tree node (about 90 with 4 KiB FSIF segments) failed its insert with `NodeSizeExceeded`, retried forever; the marker now writes it as chunks under consecutive keys (repair-process 13.0b), and a node too small for one object gives up into the finalizer failure path |
| Recovery F1 / X8 | **FIXED** 2026-11-11 (was CONFIRMED) | An unknown finalizer type threw after resolution and the transaction never finalized; creation now takes the finalizer failure path (logged, retried with backoff) |
| Recovery F12 / X4 | **FIXED** (Ruling 31) (was CONFIRMED) | `Store.close` shuts down the store's drivers, so no zombie driver keeps re-preparing; P-N4's delayed duplicate client prepare is unaffected |
| CRL F9 | CONFIRMED | Death of the writer thread only logs, and all dependent futures hang |
| Requirements F12 | CONFIRMED | `awaitingCommits` has no timer, so an FSIF ENOSPC freeze stalls the object forever |
| Paxos F8, Rebuild F6 | CONFIRMED | The tally stalls on a silent store, holding locks |
| Repair F3, F8, F9 | CONFIRMED / PLAUSIBLE | Repair livelock: non-monotone timestamps, `stale` outcomes that never escalate, scan timeouts that discard progress |
| Requirements Q-N3, Q-N6 | CONFIRMED (new, by design) | Derived per-write checks turn blind wins into abort-and-retry; the allocation action's lock collides with the allocator's follow-up writes; task allocation collides under bursts |
| Paxos P-N7, P-N8 | CONFIRMED (new, addendum, by design) | The prefer-abort wait withholds promises as well as votes (up to 5 s more for recovery and late resolution); orphaned data-less transactions, and those whose client prepares to a store are lost for 5 s, now abort |
| Recovery R-N10 | CONFIRMED (new, addendum, by design) | The case-3 fallback holds an applied, committed Tx until its UnackedCommits lookup answers; no locks, but it waits on the metadata group like R-N3 |
| CRL F22 / Repair RP-N6 | PLAUSIBLE (new, addendum) | Stubs live as long as their entries plus two scans (an orphan is dropped only by the second scan to find it so, Ruling 8); a stalled scan lets them accumulate against the device's CRL admission (33 B each, about 4.5 M per device at the default size) |
| Repair RP-N7 | CONFIRMED (new, addendum, performance) | Each orphan stub costs its own sequential point lookup per scan |

The full lists include lower-severity and documentation-only items, as well as refuted candidates recorded so they are
not re-investigated (cross-document-review §4, CRL F17, rebuild F13, repair F14, paxos P-N5/P-N6, recovery R-N5,
repair RP-N5; from the addendum refresh: paxos P-N9, recovery R-N12, requirements Q-N7/Q-N8, CRL F23/F24, repair RP-N8).

## Coverage gaps

Per cross-document-review §2, no document yet covers:

- mixed-version rolling upgrades;
- drivers and timers outliving a store reload;
- a host restart wiping the status cache (now late-commit evidence), combined with rolling restarts (2026-11-13: the
  safety half is covered by the forget floor's load component, paxos 10.10; the liveness cost of rolling restarts,
  post-load refusals ending Unknown, is stated there but not analysed across many hosts);
- the incarnation of ErrorTree and UnackedCommits entries;
- a combined analysis of delayed allocation-tree records;
- client-side caches;
- clock skew across `acceptorSince`, repair age gates and rebuild timestamps (no longer the horizon);
- the size limits of marker batches;
- a combined capacity argument covering the ErrorTree, UnackedCommits growth, CRL admission and rebuilds;
- replay where a driver's data-less prepare precedes the replayed client prepare (untested end to end; since the
  addendum the store waits for the client's prepare or proves, covered at unit level only);
- a second driver's handed-off entries across its host's crash (cross-document G13; X13). The `TxFinalized` delivered
  after the stores' cache entries expired (X14) no longer exists.
