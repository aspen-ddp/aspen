### Repair
- Inputs: ErrorTree entries for stores that missed `errorTimeout` and UnackedCommits entries
  (`client/internal/transaction/CLAUDE.md`), both kept in the pool's metadata trees
- Repair reconciles exactly (`server.store.Reconcile`): it snapshots the store's copy, reads the object, then
  installs on the executor only what still matches the snapshot: KV objects key by key, refcounts by the higher
  serial (never lowered), DataObject data by revision when the read's timestamp is at least the store's (an older
  read with a differing revision is stale, so the entry stays: repair F13). Keys and
  object-level parts a read reports locked (`ReadLocks`) are deferred; nothing is written while the object has a
  commit in flight
- One `StoreRepairer` scan walks the store's ErrorTree range, then its UnackedCommits range (skipping transactions
  the store holds live). Each chunk of a chunked UnackedCommits entry is repaired as an entry of its own. Both kinds
  of entry are left for a later pass while the store may still join the entry's transaction
  (`RepairTarget.mayJoinTransaction`: T's start, which both carry, at or above the store's forget floor). Otherwise a
  replay's Tx created after the `holdsTransaction` check could apply T after the reconcile, and the entry would be
  deleted under it (transaction-recovery R-N9). The delay is at most the horizon plus `MaxStartLead` (about 22 s). It drops an
  applied stub with its entry (for a chunked one, with its head, the chunk the lookups name), and an orphan stub only
  once two consecutive scans found it so (`server/transaction/CLAUDE.md`)
- An entry is deleted through its tree (`TieredKeyValueList.deleteIfPresent`, under the entry's `KeyRevision`), not
  through the node the walk handed over, so a delete that empties a node registers the join/tail-removal finalization
  actions and the trees shrink as repair drains them. Those actions run in ordinary (tracked) transactions, as the
  splits of errorTree inserts already do; the entry delete itself is tracked too, with
  `MissedUpdateFinalizationAction.limitTracking(storeId.poolId, tx)` turning tracking off only for the bootstrap
  pool's own trees (an entry about P lives one level closer to the bootstrap pool, so the chain ends).
  `newRepairTransaction` is for repair proofs only
- `StoreRepairer.sweepBootstrapTrees` reconciles one bootstrap store's copy of every node (all tiers,
  `TieredKeyValueList.foreachNode`) of the bootstrap pool's own errorTree and unackedCommitsTree, whose writes run
  untracked so a miss leaves no entry. Each node goes through `repairUpdate` with no minimum timestamp and no proofs
  (they would collide with marker traffic), always over the whole node (never only the keys a lock deferred: a later
  missed untracked write records nothing), keyed `sweepDeferralKey` (prefix 2, kept by `DeferralState.retain`;
  pruned by `retainSweep` after a complete walk). It creates and deletes no entries; an unreadable node counts as
  incomplete. `SweepResult.needsAnotherPass` is true when anything was reconciled or left incomplete, or a walk
  missed nodes
- `RepairService.scanOne` runs the sweep for a store of the bootstrap pool right after its entry scan settles (success
  or failure), under its own deadline (`SweepDeadline`, 90 s; `SweepTimedOut` drops the store's DeferralState as
  `ScanTimedOut` does). The store's pacing is advanced with `foundEntries || sweep.needsAnotherPass`: a failed or
  timed-out sweep counts as needing another pass (unlike a failed entry scan), so the store stays at
  `scanIntervalFloor` until its trees converge and a clean sweep lets it back off to `scanIntervalCap`. A node
  skipped because the store stopped being repairable here counts as incomplete too. A missed untracked write is
  therefore healed within about one cap interval (Backlog.txt "Bootstrap tree sweep staleness window"). While the
  bootstrap pool is unreadable, a bootstrap store's scan and sweep each run to their 90 s deadline, so it holds a scan
  slot most of each floor interval (spec-mandated)
- After `RepairPolicy.proofAfterDeferrals` (10) lock-deferred passes in a row, `RepairProofs` commits a no-op
  transaction proving the read state, and reconciles from it
- Absence from the AllocationTree is not a deletion (repair F5): it may be an AllocationFinalizationAction running
  late. For an entry older than `RepairPolicy.minErrorEntryAgeForDeletion` (60 s), `repairAbsent` reads the object
  through its pool, using the entry's pointer (an UnackedCommits entry's own, or one rebuilt from an ErrorTree value,
  `client/internal/transaction/CLAUDE.md`). Only `InvalidObject.provesDeletion` deletes it from the store: more than
  `width - writeThreshold` `ObjectNotFound`/`ObjectMismatch` replies, or a consistent majority at refcount 0 (a live
  object is on at least writeThreshold ≥ consistentRestoreThreshold stores). A restored read means the object is
  live: it is reconciled as if listed. Any other failure keeps the entry. An ErrorTree entry of unknown type is acted
  on only for a proven deletion
- A repair write that meets a foreign occupant of the object's slot (`backend.RepairSlotOccupied`, FSIF slot reuse
  over a missed delete) drops the occupant with an id-checked `repairDelete` once the AllocationTree does not list it.
  The object was just read live, and two live objects never share a slot. The entry stays (stale), and the next pass
  installs the object. A listed occupant is logged as an error and left (cross-document-review X6)
- A deletion the store refuses because the object is in use (`Frontend.RepairDeleteDeferred`) counts as deferred: the
  entry stays for a later pass
- A repair scan runs only while the store's pool entry names this host, so a superseded copy never consumes the
  current copy's ErrorTree entries
