# Safety specifications

These documents state, as checkable specifications, the correctness guarantees that were previously embedded only in
the code. They were derived from the code on 2026-11-09 and refreshed the same day against `0d08e3c6` (branch
`pin_refactor`), after the replay horizon was replaced by pinned transactions
(`docs/superpowers/specs/2026-11-09-replay-pins-design.md`). Each document has a "Re-assessment for replay pins" table
marking every earlier finding fixed, narrowed or unchanged, and a section of new findings. Each has its own "Suspected flaws" section, annotated with
adversarial verification verdicts:

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
  (`TransactionDriver.scala:438-447`). Round 1 carries no commit-only rule, and aborts are decided in round 1. In the
  error-free case the client learns the outcome after 5 one-way hops and 2 serial CRL writes. Todo.txt and
  Architecture.md describe a protocol the code does not implement (paxos F1). Either the protocol needs implementing or
  the description needs correcting.
- **Is RevisionLock exclusive?** Yes. It takes the same exclusive object lock as a DataUpdate and changes nothing.
  Renaming it to `ExclusiveRevisionLock` would be accurate (transaction-requirements, RevisionLock verdict).
- **Two failure-recovery modes?** Driver loss and participant loss are the roots. A lost `TxFinalized`, a recovered
  commit vote whose peers forgot the transaction, and missed updates handed to repair each need separate treatment
  (transaction-recovery §1).

## Ranked findings

The ranking is by worst plausible consequence, then by confidence. IDs refer to each document's flaw section; `X*` ids
refer to cross-document-review.md. New findings from the replay-pins refresh carry `-N` ids (`P-N*` paxos, `R-N*`
recovery, `Q-N*` requirements, `RP-N*` repair).

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
| CRL F5 | CONFIRMED | There are no checksums on CRL entries. Bit rot or reordered sectors can become Paxos state or object data |
| Paxos P-N4 | PLAUSIBLE (new) | Pins protect committed transactions only. With the horizon gone, a forgotten *abort* can be re-decided commit by a delayed duplicate prepare (a zombie driver's, X4, no longer: Ruling 31) once the requirement that failed has cleared |
| Paxos P-N2 | PLAUSIBLE (new) | Late condition 1 assumes no acceptor of the choosing quorum forgets a live T silently; a rebuilt store left unmuted by a fast client clock (X2 residual) can make a live T resolve late, skipping finalization and marks |

### Tier 2: resurrection, overwritten updates, permanently stale replicas

| Id | Verdict | Finding |
|---|---|---|
| Paxos F3 / Recovery F7 | **FIXED** (replay pins) (was CONFIRMED) | The horizon trusted the client clock. It is gone; replay is refused by the client pin rule and the late-commit rule |
| Recovery R-N1 / X11 | PLAUSIBLE (new) | Ruling 17's residual is not one store (Ruling 32, docs corrected, no closure): every counted voter that acknowledged after `errorTimeout` keeps an UnackedCommits entry until its repair scan (30 s to 15 min), and a replay's late commit re-applies T's writes in pools where T has no pin of its own (blind or `replaySafe` writes, ABA `DoesNotExist` inserts, allocations in another pool) at all of them, a quorum when the whole pool acked late; reconcile then follows the corrupted quorum |
| Paxos P-N3 / X12 | PLAUSIBLE (new) | A recovering acceptor holding T's accepted commit makes a post-proof quorum adopt a *non-late* commit even when a proof exists (broader than the design's residual): finalization actions re-run and a concurrent replayed client prepare applies blind writes, `DoesNotExist` inserts after a later delete, and allocations |
| X3 | **FIXED** (Ruling 30) (was PLAUSIBLE, then NARROWED) | `TransactionStatusCache.updateStatus` is monotone (finalized and late sticky, no revert to Unresolved), so no store's write downgrades a finalized entry and the finalized relay stays late |
| Recovery R-N6 | PLAUSIBLE (new) | A driver adopting an accepted commit long after finalization re-runs every finalization action (Ruling 13): the marker's blind upsert can regress newer ErrorTree entries and re-create deleted UnackedCommits entries (re-arming R-N1); a re-run allocation action can leave an orphan entry |
| Repair RP-N4 | PLAUSIBLE (new) | An UnackedCommits entry outlives the store's own late application of T until its scan deletes it; a replay's late commit applies T's unpinned-pool writes there meanwhile (the window of R-N1) |
| Requirements Q-N4 | CONFIRMED (new; trust model, by design) | The pin rule is enforced only by the client: a client built without the check sends replayable transactions, which get fresh commit votes and commit normally |
| CRL F13 / Requirements F2 / Recovery F4 | **NARROWED** (was CONFIRMED, narrow) | A crash re-apply of a TKVL write now fails its per-write check; blind writes and `DoesNotExist` inserts after a later delete still re-apply |
| Requirements F1 | **NARROWED** (was CONFIRMED) | A lagging store skips a stale per-write checked write and is marked; blind writes are still last-arrival-wins with no entry |
| Requirements F11 | **NARROWED** (was PLAUSIBLE) | A replayed allocation can no longer commit by fresh votes (its pin fails), but still applies where X11 or P-N3 deliver the commit |
| Repair F4 | **NARROWED** (was CONFIRMED, narrow) | TKVL deletes now carry `KeyRevision`, so the invisible no-op delete fails at apply and is marked; blind deletes unchanged |
| Repair F1, F2, N1, N2 / Requirements F3 | CONFIRMED | Opportunistic and pre-transaction rebuilds replace KV `data` without refreshing `kvState` and skip the commit-in-flight and refcount guards |
| Paxos F2 / Recovery F3, F8 | **NARROWED** (was CONFIRMED, narrowed) | A driver holding a proof never decides abort and resolves late; the false abort remains without a proof (refcount or overwritten pin, or the single-shot proof missed, P-N1) |
| Paxos P-N1 | CONFIRMED (new, mechanism) | The proof is sent once to `lastProposer`; any other driver may decide abort |
| Repair F5 | PLAUSIBLE | An allocation-tree record more than 60 s late is treated as a delete |
| Requirements Q-N5 | PLAUSIBLE (new) | `replaySafe` audit: proofs and the split action hold; the allocation action's orphan entry is less rare than stated; the marker's "idempotent" blind batch can regress newer entries and re-create deleted UnackedCommits entries |
| X6, X7, X9, C5, C6 | PLAUSIBLE | Repair coverage holes compose: marker-batch give-up, FSIF slot reuse depending on a delete entry, a non-monotone completion gate after a rebuild, untracked bootstrap-pool ErrorTree writes |

### Tier 3: liveness (hangs, stalls, leaks)

| Id | Verdict | Finding |
|---|---|---|
| Recovery F2, F14 / X5 / R-N2 | CONFIRMED | Finalization has no failure path. Since Ruling 8 a failed action also stops the marker, so no ErrorTree marks or UnackedCommits entries are written (R-N2) |
| Recovery R-N3 | CONFIRMED (new) | Every recovered commit vote waits, holding its locks and with no driver, for its UnackedCommits lookup; a metadata-group outage stalls them all |
| Recovery R-N4 / Repair RP-N3 | CONFIRMED (new) | UnackedCommits entries are one per (store, transaction), never merge, and are now written for every transaction with a silent voter: tree growth, larger marker batches, double reconciles |
| Recovery F1 / X8 | CONFIRMED | An unknown finalizer type throws after resolution and the transaction never finalizes |
| Recovery F12 / X4 | **FIXED** (Ruling 31) (was CONFIRMED) | `Store.close` shuts down the store's drivers, so no zombie driver keeps re-preparing; P-N4's delayed duplicate client prepare is unaffected |
| CRL F9 | CONFIRMED | Death of the writer thread only logs, and all dependent futures hang |
| Requirements F12 | CONFIRMED | `awaitingCommits` has no timer, so an FSIF ENOSPC freeze stalls the object forever |
| Paxos F8, Rebuild F6 | CONFIRMED | The tally stalls on a silent store, holding locks |
| Repair F3, F8, F9 | CONFIRMED / PLAUSIBLE | Repair livelock: non-monotone timestamps, `stale` outcomes that never escalate, scan timeouts that discard progress |
| Requirements Q-N3, Q-N6 | CONFIRMED (new, by design) | Derived per-write checks turn blind wins into abort-and-retry; the allocation action's lock collides with the allocator's follow-up writes; task allocation collides under bursts |

The full lists include lower-severity and documentation-only items, as well as refuted candidates recorded so they are
not re-investigated (cross-document-review §4, CRL F17, rebuild F13, repair F14, paxos P-N5/P-N6, recovery R-N5,
repair RP-N5).

## Coverage gaps

Per cross-document-review §2, no document yet covers:

- mixed-version rolling upgrades;
- drivers and timers outliving a store reload;
- a host restart wiping the status cache (now late-commit evidence), combined with rolling restarts;
- the incarnation of ErrorTree and UnackedCommits entries;
- a combined analysis of delayed allocation-tree records;
- client-side caches;
- clock skew across `acceptorSince`, repair age gates and rebuild timestamps (no longer the horizon);
- the size limits of marker batches;
- a combined capacity argument covering the ErrorTree, UnackedCommits growth, CRL admission and rebuilds;
- replay where a driver's data-less prepare precedes the replayed client prepare (untested end to end).
