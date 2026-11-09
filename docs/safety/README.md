# Safety specifications

These documents state, as checkable specifications, the correctness guarantees that were previously embedded only in
the code. They were derived from the code on 2026-11-09. Each has its own "Suspected flaws" section, annotated with
adversarial verification verdicts:

- **CONFIRMED**: the mechanism was traced in the code, or reproduced.
- **PLAUSIBLE**: the mechanism is real, but the trigger is uncertain.
- **REFUTED**: a guard was found.

| Document | Scope |
|---|---|
| [paxos-transactions.md](paxos-transactions.md) | Paxos roles, message flow, votes, rounds, persistence ordering, horizon, proof-of-commit |
| [crash-recovery-log.md](crash-recovery-log.md) | CRL contract, its place in the workflow, SimpleCRL design, on- and off-device trade-offs |
| [transaction-requirements.md](transaction-requirements.md) | Requirements and operations, locks, vote tallying, commit-in-flight ordering |
| [transaction-recovery.md](transaction-recovery.md) | Driver and participant crash recovery, the missed-update feed into the ErrorTree and TracelessCommits |
| [repair-process.md](repair-process.md) | ErrorTree and TracelessCommits schemas, scan, exact reconcile, races with transactions |
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
refer to cross-document-review.md.

### Tier 1: lost durable state, split decisions, systemic loss

| Id | Verdict | Finding |
|---|---|---|
| CRL F1 | **FIXED** 2026-11-09 (was CONFIRMED, reproduced) | `StreamWriter.scala:74` ignores the partial `write(ByteBuffer[])` (IOV_MAX = 1024). An entry carrying more than about 340 transactions is acknowledged but cannot be read, and every later entry is lost. The open-time re-save of every recovered transaction triggers it on any device with that many live transactions |
| CRL F2 / F20 | **FIXED** 2026-11-09 (was CONFIRMED, reproduced) | Recovery assigns the oldest visited entry's stream UUID to the active stream (`Recovery.scala:81`), so saves acknowledged after a recovery that spans two streams are lost. A second restart after a rotation throws `BrokenEntryChain`, which triggers a device-wide rebuild |
| CRL F6 | **FIXED** 2026-11-09 for transient errors (was CONFIRMED) | I/O and VM errors are now retried on 3 scans before the CRL is declared lost; other failures still declare it lost, by decision, leaving fleet-wide protection to a higher level. Was: `openDeviceCrl` treats any `Throwable` as a lost CRL and marks every store on the device rebuild-required (`DeviceDiscovery.scala:510-515`). CRL F1, F2 and F4 all feed it: this is the in-product generator of the systemic-loss scenario |
| Rebuild F2 | **FIXED** 2026-11-09 (was CONFIRMED) | The old copy and its saved CRL entries now stay in `pending-delete/` until the store is Active again. Was: `discardLostStore` destroys the old store's data and CRL entries before checking that the pool can still supply it. The concurrency cap does not pace discards |
| X2 | **FIXED** 2026-11-09 (was PLAUSIBLE) | A rebuilt store now mutes prepares for transactions started before its `acceptor-since` time (rebuild finish plus 10 s): proof of commit only, no promise, vote or accept. Was: A rebuilt (blank) acceptor promoted while a transaction is still unresolved can let a quorum choose a different value. For Replication(3,2) the quorum intersection is one store. Rebuild F14's "safe within failure tolerance" is wrong as stated |
| X1 / Rebuild F1 | **FIXED** 2026-11-09 (was CONFIRMED) | A Failed device is now retired (marker plus unload), never reloaded, its former host nudged, and repair scans fenced on the pool entry; residual stale-mapping window documented. Was: A device marked Failed is never unloaded or fenced. A restarted owner reloads it without a tree check (`DeviceDiscovery.scala:474-478`). The resulting zombie replica serves as a second acceptor for the same pool index and consumes the rebuilt store's ErrorTree entries |
| CRL F3 | **FIXED** 2026-11-09 (was PLAUSIBLE, reproduced by a torn-write test) | The rotation copy-forward overwrites the stream that holds the only copy of live data. A torn write there leads to `BrokenEntryChain` |
| CRL F5 | CONFIRMED | There are no checksums on CRL entries. Bit rot or reordered sectors can become Paxos state or object data |

### Tier 2: resurrection, overwritten updates, permanently stale replicas

| Id | Verdict | Finding |
|---|---|---|
| Paxos F3 / Recovery F7 | CONFIRMED | The horizon trusts the client-chosen `startTimestamp`, and no server forwards its HLC from received prepares. A client clock more than about 1 s fast lets a replayed blind KV insert or delete re-commit |
| CRL F13 / Requirements F2 / Recovery F4 | CONFIRMED (narrow) | `dropTransactionObjectData` is memory-only. After a crash, a recovered commit vote re-applies a blind KV Insert, an `Exists`-guarded Delete, or an ABA refcount over newer state |
| Requirements F1 | CONFIRMED | A lagging store applies blind KV writes last-arrival-wins. A late commit overwrites a newer one, and no ErrorTree entry is written if `TxCommitted` arrived within 2 s |
| X3 | PLAUSIBLE | A `finalized` status-cache flag can be downgraded (`Tx.scala:662`, `Frontend.scala:614-619`), which lets a client retransmit re-apply a blind update |
| Repair F4 | CONFIRMED (narrow) | The snapshot compares key revisions only, so a blind delete in the window is invisible and repair re-creates the key |
| Repair F1, F2, N1, N2 / Requirements F3 | CONFIRMED | Opportunistic and pre-transaction rebuilds replace KV `data` without refreshing `kvState` and skip the commit-in-flight and refcount guards. As written, they almost never fire for the store they target, and when they do they can install an older snapshot. Repair's own reads can trigger them |
| Paxos F2 / Recovery F3, F8 | CONFIRMED (narrowed) | After the 7 s status cache expires, a stale retransmit starts a fresh instance that resolves abort for a committed transaction. Store state does not split, but the client can be told "aborted" and live voters drop the commit, relying on repair |
| Repair F5 | PLAUSIBLE | An allocation-tree record more than 60 s late is treated as a delete. `repairDelete` removes a live slice and drops the entry unguarded |
| X6, X7, X9, C5, C6 | PLAUSIBLE | Repair coverage holes compose: a marker-batch give-up (`MissedUpdateMarker.scala:127-157`), FSIF slot reuse that depends on a delete entry, a non-monotone timestamp completion gate after a rebuild, and untracked bootstrap-pool ErrorTree writes |

### Tier 3: liveness (hangs, stalls, leaks)

| Id | Verdict | Finding |
|---|---|---|
| Recovery F2, F14 / X5 | CONFIRMED | Finalization has no failure path, so a permanent marker failure hangs it forever and no ErrorTree entries are written. CRL admission can feed back into this |
| Recovery F1 / X8 | CONFIRMED | An unknown finalizer type throws after resolution and the transaction never finalizes. Under a rolling upgrade this hits a whole pool at once |
| Recovery F12 / X4 | CONFIRMED | `Store.close` never stops the store's drivers, which keep re-sending `Accept((1,L))` forever after a non-crash reload. This turns paxos F5 into a persistent hazard |
| CRL F9 | CONFIRMED | Death of the writer thread only logs, and all dependent futures hang |
| Requirements F12 | CONFIRMED | `awaitingCommits` has no timer, so an FSIF ENOSPC freeze stalls the object forever |
| Paxos F8, Rebuild F6 | CONFIRMED | The tally stalls on a silent store, holding locks |
| Repair F3, F8, F9 | CONFIRMED / PLAUSIBLE | Repair livelock: non-monotone timestamps, `stale` outcomes that never escalate to a proof, and scan timeouts that discard progress |

The full lists include lower-severity and documentation-only items, as well as refuted candidates recorded so they are
not re-investigated (cross-document-review §4, CRL F17, rebuild F13, repair F14).

## Coverage gaps

Per cross-document-review §2, no document yet covers:

- mixed-version rolling upgrades;
- drivers and timers outliving a store reload;
- a host restart wiping the status cache, combined with rolling restarts;
- the incarnation of ErrorTree entries;
- a combined analysis of delayed allocation-tree records;
- client-side caches;
- clock skew across the horizon, repair age gates and rebuild timestamps;
- the size limits of marker batches;
- a combined capacity argument covering the ErrorTree, CRL admission and rebuilds.
