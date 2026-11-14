# Opportunistic rebuild removal, bootstrap tree sweep, tracked repair deletions

Todo items 2.1 and 2.10. Findings: Repair F1, F2, N1, N2, F7, Req F3, Rebuild F8, X9.

## Intent

Opportunistic rebuild (`Frontend.opportunisticRebuild`, driven by `OpportunisticRebuild` messages from read
drivers) and the pre-transaction rebuild (`PreTransactionOpportunisticRebuild` carried in `TxPrepare` and applied in
`Tx.completeLoad`) exist for two reasons:

1. They are the only thing that heals a store's copy of a metadata-tree node written by a transaction that has
   missed-update tracking disabled.
2. An optimization: when a client already knows a store is behind and holds the data, sending it is cheap.

Both paths are unsafe (they replace KV content without refreshing kvState, move timestamps backwards, and the
pre-tx path skips the commitInFlight guard, the refcount max and ordered commit), and they duplicate what the
ErrorTree/UnackedCommits scan and `Reconcile` already do. The system is simpler without them. The owner accepts
removing them provided a bullet-proof mechanism keeps the error trees repaired.

## Goals

- Delete both opportunistic paths, their messages, codec entries, client-side manager and tests.
- After removal, every stale replica of an ErrorTree/UnackedCommits node is healed by a mechanism that does not
  depend on a client happening to read the node.
- Shrink the set of writes that run with missed-update tracking disabled to the minimum the recursion requires.

## Non-goals

- Hardening the opportunistic paths. They are deleted, not fixed.
- Keeping the "cheap repair when the data is in hand" optimization. Repair latency for a lagging store becomes
  errorTimeout (2 s) plus the store's next repair scan. If that latency matters later, tune the scan.
- Anti-entropy for metadata pools' trees or for ordinary pools. Their writes are all tracked after this change.
- Other Todo items that touch the same code (2.4 marker monotonicity, 2.5 reconcile fixes, 2.12 incarnations).

## Anti-goals

- A second write path into stores that bypasses the Tx apply rules. Healing goes through `Reconcile` only.
- New durable state (a miss list, a CRL-backed queue) whose own recovery needs a design.
- Unbounded recursion of marker writes. Tracking may be turned on only where the chain provably ends.

## Background: which writes are untracked today

| Writer | Mechanism | Trees written |
|---|---|---|
| `MissedUpdateMarker` writes for the bootstrap pool B's own trees | `limitTracking(entryPool, tx)` disables tracking when `tx` references `entryPool`; only B's own trees live in the pool they describe | B.errorTree, B.unackedCommitsTree |
| Repair entry deletions, every pool | `AspenClient.newRepairTransaction` disables tracking unconditionally (Repair F7) | every pool's errorTree/unackedCommitsTree |
| Repair proofs | `newRepairTransaction` | data objects; change no content, at most the timestamp |

A store that misses an untracked write keeps a stale copy and gets no entry. For B's trees nothing heals it except
opportunistic rebuild on a read. A B store that is rebuilding, or briefly offline, misses every such write
(Rebuild F8), and two B stores lagging on different nodes can let a quorum read lose a just-written entry, which
loses the record of another pool's missed update permanently (X9).

## Approaches considered

1. **Bootstrap tree sweep plus tracked repair deletions (chosen).** Turn tracking on for repair deletions wherever
   `limitTracking` allows it, which leaves writes to B's own error trees as the only untracked content writes. Then
   periodically reconcile every B store's copy of every node of those two trees. Small, bounded cost (B's trees
   describe only B and the metadata pools, and are drained by repair); no new durable state; independent of reads.
2. **Sweep every metadata tree, leave deletions untracked.** Smaller code change, but cost grows with metadata
   pools' trees, which are large exactly during a failure burst.
3. **A tracked miss list on the finalizer host until a full B node scan confirms convergence (X9's sketch).** New
   durable state with its own recovery story, for the same coverage.
4. **A second rebuild walk over B's nodes (F8's sketch).** Covers rebuilds only, not stores that were briefly
   offline.

The owner picked approach 1.

## Design

### Part A: delete the opportunistic paths

Remove, with their tests:

- `common.network.OpportunisticRebuild`, its codec entry and protobuf message, and its `StoreRouter` dispatch.
- `Frontend.readObjectForOpportunisticRebuild`, `Frontend.opportunisticRebuild`, `ReadKind.OpportuneRebuild` and
  any read path that exists only to serve them.
- `PreTransactionOpportunisticRebuild`, the `preTxRebuilds` field of `TxPrepare` (Message, Codec, proto), the
  `Tx` constructor parameter and its application in `Tx.completeLoad`.
- `OpportunisticRebuildManager`, `SimpleOpportunisticRebuildManager`, the `ReadDriverClient` member,
  `TransactionBuilder.buildTranaction`'s manager parameter and pre-tx rebuild collection, `BaseReadDriver`'s
  `sendOpportunisticRebuild` and `markRepairNeeded` call, and the `disableOpportunisticRebuild` parameter threaded
  through `ReadManager`/`ReadDriver`/`SimpleReadDriver` and every caller.
- The client object cache: `client.ObjectCache` (trait and `NoCache`), `client.internal.SimpleObjectCache`, the
  `objectCache` member of `ReadDriverClient`/`BaseAspenClient` and its construction in `SimpleAspenClient`, and the
  `put` in `BaseReadDriver`. Its only reader is `SimpleOpportunisticRebuildManager` (verified 2026-11-14); reads
  never consult it. The server-side `server.store.cache.ObjectCache` is unrelated and stays.
- Keep `ObjectState.getRebuildDataForStore`: `server.rebuild.RebuildingStore` uses it.
- `ReadManager.pruneStaleReadsTask` holds completed reads for `slowReadReplyDuration` (5 s) only so late responses
  can trigger opportunistic rebuild (its scaladoc says so). Release a read when it completes instead. Builder to
  confirm that a response for an unknown read is dropped harmlessly and that nothing else (slow-read reporting,
  lock detection) consumes late responses; if something does, keep the holding and fix its comment.

Backward compatibility with persisted data or mixed-version messages is not required.

### Part B: tracked repair deletions

`StoreRepairer.deleteEntryIfUnchanged` builds its transaction with `client.newTransaction()` and applies
`MissedUpdateFinalizationAction.limitTracking(storeId.poolId, tx)` (the pool the entry describes) after the tree's
`deleteIfPresent` has added its updates and before commit, the way `MissedUpdateMarker` does.

Why the chain ends: a deletion of an entry about pool P writes P's tree, which lives in P's metadata pool M (or in
B when P is a metadata pool). A miss by one of those stores is marked as an entry about M (or B) in M's (or B's)
tree, which lives one level closer to B. Deleting an entry in B's own trees references B, so `limitTracking`
disables tracking there and the chain stops. Depth is at most two below the original deletion. The concern in
`newRepairTransaction`'s scaladoc (a deletion's miss writes an entry, whose repair writes another, forever) holds
only for B's own trees, which stay untracked.

`newRepairTransaction` remains for proofs only. Update its scaladoc to say so, and why proofs are safe untracked
(they change no content; a missed proof leaves at most an older object timestamp on one store).

Join and tail-removal finalization actions registered by the deletion already run in ordinary tracked
transactions (repair CLAUDE.md); unchanged.

### Part C: the bootstrap tree sweep

After Part B the only untracked content writes are writes to nodes of B.errorTree and B.unackedCommitsTree (all
tiers: tier-0 entries and upper-tier (minimum, pointer) entries written by split/join actions are tracked, but the
marker and deletion writes to those nodes are not, so treat every node of both trees as possibly stale).

The sweep reconciles one B store's copy of every node of both trees:

- **Where:** in `StoreRepairer.scan`, for a store of the bootstrap pool, after the ErrorTree and UnackedCommits
  walks and orphan-stub handling. It therefore runs only where scans run: on the host the pool entry names, for a
  repairable store (not offline, rebuilding or transferring out), under the scan deadline.
- **When:** every scan of a B store. A store becomes repairable on load and on rebuild promotion, so its first
  scan after either sweeps it; that closes Rebuild F8 within one scan interval of promotion. Periodic scans
  cover transient misses (X9).
- **Interval:** a sweep that reconciled anything, or left anything not complete, counts as "found entries", so
  the store stays at `scanIntervalFloor` (30 s) until its trees converge. A clean sweep does not, so the store backs
  off to `scanIntervalCap` (15 min) as today. The worst-case staleness window for a missed untracked write is
  therefore one cap interval plus a scan. The owner accepted this window for now (2026-11-14): no new policy knob.
  The residual X9 exposure and the suggested fix (a separate, short sweep interval in `RepairPolicy`) are recorded
  in `Backlog.txt` for a later revisit. Document the window as X9's residual when updating its status.
- **Enumeration:** every node of every tier of each tree, by walking each tier left to right from its leftmost
  node (tier navigation left to right is consistent). `TierEngine.foldLeft` covers tier 0 only; add a node walk
  over all tiers to `TierEngine`/`TieredKeyValueList` (builder's call how). Nodes deleted by a join while the
  sweep runs are skipped, not errors; a node created after the sweep passed its position is covered by the next
  sweep.
- **Reconcile:** each node through the existing repair path (`repairUpdate`: snapshot, quorum read, install what
  still matches), keyed in the store's `DeferralState` so lock deferrals and proofs work as for entries. The
  deferral key needs a distinct prefix (beside the existing 0 and 1 of `errorDeferralKey`/`unackedDeferralKey`) so
  sweep state survives `deferral.retain`. The `minTimestamp` argument has no missed-update timestamp to come from;
  use none (or the minimum) so completion means "the store's copy matches the read".
- **No entries are created or deleted** by the sweep.
- **No proofs for sweep nodes** (my call). B's nodes are written by marker traffic, and a proof on them is an
  untracked write that collides with it. Reconcile installs key by key, so only keys locked at every attempt wait,
  and they are retried on the next sweep. A node left deferred counts as "not complete" (the store stays at the
  floor); for B's small trees a sweep every 30 s is cheap.
- **Deadline** (my call): the sweep runs as its own step with its own deadline, so the entry walks cannot starve
  it. A sweep cut off by its deadline, or a node that cannot be read (B degraded), counts as "not complete" and the
  next scan sweeps from the start. No resume cursor: B's trees describe only B and the metadata pools.
- **Completeness of the untracked set:** `disableMissedUpdateTracking` has exactly two callers, `limitTracking`
  and `newRepairTransaction` (verified 2026-11-14). Split, join and tail-removal actions on B's trees run in
  ordinary tracked transactions; their misses are marked by (untracked) marker writes, which the sweep covers.

### Documentation and tests

- Tests (deterministic, LockstepThreading where possible), each reproducing the finding before the fix:
  - Rebuild F8 / X9: a B store misses a marker write to a B tree node (offline or rebuilding), and after the store
    is repairable again its scan reconciles the node, with no read of the node by any client.
  - A stale B store copy of an upper-tier node is reconciled.
  - Repair F7: a metadata-pool store misses an entry deletion of a data pool's errorTree, and an ErrorTree entry
    for that metadata-pool store is written; a deletion in B's own trees still runs untracked.
  - Removal: the existing opportunistic-rebuild tests are deleted; no test depends on reads healing stores.
- Update the statuses of Repair F1, F2, N1, N2, F7, Req F3, Rebuild F8 and X9 (dated notes), the README ranked rows,
  and `Todo.txt` (drop 2.1 and 2.10; strike "This also removes the healing that 2.10 currently leans on").
- Module guides: `server/repair/CLAUDE.md` (sweep, tracked deletions), `client/internal/CLAUDE.md` and
  `server/store/CLAUDE.md` (drop opportunistic rebuild), `client/internal/transaction/CLAUDE.md` if it describes
  `newRepairTransaction` or `limitTracking`.

## Left to the builder

- How to walk every tier's nodes in TKVL, and its name.
- How `repairUpdate` expresses "no minimum timestamp".
- The exact counting of sweep results into `ScanResult` (a new field is fine) and log lines.
- The order of removal commits.
