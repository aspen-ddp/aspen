### Repair
- Inputs: ErrorTree entries for stores that missed `errorTimeout` and UnackedCommits entries
  (`client/internal/transaction/CLAUDE.md`), both kept in the pool's metadata trees
- Repair reconciles exactly (`server.store.Reconcile`): it snapshots the store's copy, reads the object, then
  installs on the executor only what still matches the snapshot: KV objects key by key, refcounts by the higher
  serial (never lowered), DataObject data by revision when the read's timestamp is at least the store's. Keys and
  object-level parts a read reports locked (`ReadLocks`) are deferred; nothing is written while the object has a
  commit in flight
- One `StoreRepairer` scan walks the store's ErrorTree range, then its UnackedCommits range (skipping transactions
  the store holds live). Each chunk of a chunked UnackedCommits entry is repaired as an entry of its own. It drops an
  applied stub with its entry (for a chunked one, with its head, the chunk the lookups name), and an orphan stub only
  once two consecutive scans found it so (`server/transaction/CLAUDE.md`)
- An entry is deleted through its tree (`TieredKeyValueList.deleteIfPresent`, under the entry's `KeyRevision`), not
  through the node the walk handed over, so a delete that empties a node registers the join/tail-removal finalization
  actions and the trees shrink as repair drains them. Those actions run in ordinary (tracked) transactions, as the
  splits of errorTree inserts already do; the entry delete itself is tracked too, with `MissedUpdateFinalizationAction.limitTracking(storeId.poolId, tx)` turning
  tracking off only for the bootstrap pool's own trees (an entry about P lives one level closer to the bootstrap pool, so
  the chain ends). `newRepairTransaction` is for repair proofs only
- `StoreRepairer.sweepBootstrapTrees` reconciles one bootstrap store's copy of every node (all tiers,
  `TieredKeyValueList.foreachNode`) of the bootstrap pool's own errorTree and unackedCommitsTree, whose writes run
  untracked so a miss leaves no entry. Each node goes through `repairUpdate` with no minimum timestamp and no proofs
  (they would collide with marker traffic), keyed `sweepDeferralKey` (prefix 2, kept by `DeferralState.retain`;
  pruned by `retainSweep` after a complete walk). It creates and deletes no entries; an unreadable node counts as
  incomplete. `SweepResult.needsAnotherPass` is true when anything was reconciled or left incomplete, or a walk
  missed nodes
- After `RepairPolicy.proofAfterDeferrals` (10) lock-deferred passes in a row, `RepairProofs` commits a no-op
  transaction proving the read state, and reconciles from it
- A repair scan runs only while the store's pool entry names this host, so a superseded copy never consumes the
  current copy's ErrorTree entries
