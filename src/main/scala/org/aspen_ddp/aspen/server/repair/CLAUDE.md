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
  the store holds live). Each chunk of a chunked UnackedCommits entry is repaired as an entry of its own. An entry's
  object that the same scan's ErrorTree pass completed, with the ErrorTree entry's delete committed, at a
  `missedTimestamp` at or after the entry's timestamp is complete without another read (`Counts.completedAt`;
  repair-process 5.3): a silent voter's entry then costs only its delete. A failed delete records nothing: the entry
  was rewritten, possibly by a mark for a transaction that committed after the read. Both kinds
  of entry are left for a later pass while the store may still join the entry's transaction
  (`RepairTarget.mayJoinTransaction`: T's start, which both carry, at or above the store's forget floor). Otherwise a
  replay's Tx created after the `holdsTransaction` check could apply T after the reconcile, and the entry would be
  deleted under it (transaction-recovery R-N9). The delay is at most the horizon plus `MaxStartLead` (about 22 s).
  Applied stubs are not repair's: each store expires its own (`server/transaction/CLAUDE.md`, Todo 4.6)
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
- A scan or sweep cut off by its deadline (`ScanTimedOut`, `SweepTimedOut`) has its `ScanControl` cancelled: it starts
  no further entry or node, and skips `retain`/`retainSweep` and the orphan pass's `confirmOrphans`, so the store
  keeps its DeferralState (repair F9). A timed-out scan counts as having found entries if it had seen any
  (`ScanControl.sawEntries`): a backlog keeps the store at the floor, while a scan that reached no entry backs off. A
  reconcile in flight at the cancel still records, which can bring a proof one pass early
- `RepairService.scanOne` runs the sweep for a store of the bootstrap pool right after its entry scan settles (success
  or failure), under its own deadline (`SweepDeadline`, 90 s). The store's pacing is advanced with
  `foundEntries || sweep.needsAnotherPass`: a failed or timed-out sweep counts as needing another pass (unlike a
  failed entry scan; a timed-out one counts as found if it had seen entries), so the store stays at
  `scanIntervalFloor` until its trees converge and a clean sweep lets it back off to `scanIntervalCap`. A node
  skipped because the store stopped being repairable here counts as incomplete too. A missed untracked write is
  therefore healed within about one cap interval (Backlog.txt "Bootstrap tree sweep staleness window"). While the
  bootstrap pool is unreadable, a bootstrap store's scan and sweep each run to their 90 s deadline, so it holds a scan
  slot most of each floor interval (spec-mandated)
- `StoreRepairer.verifyObject` (through `RepairService.verifyObject`, `VerifyDeadline` 30 s) verifies an object a
  store quarantined (`server/store/CLAUDE.md`, Quarantine): `repairUpdate` with the quarantining transaction's start as
  `minTimestamp`, proofs allowed, keyed `verifyDeferralKey` (prefix 3, kept by `DeferralState.retain`). It reads and
  deletes no entry, so the ownership fence and `mayJoinTransaction` do not apply. `complete` clears the quarantine
- After `RepairPolicy.proofAfterDeferrals` (10) lock-deferred passes since the last complete one, `RepairProofs` commits a no-op
  transaction proving the read state, and reconciles from it
- A proof cannot help a `stale` pass (the store's own copy is busy or changed), so stale passes escalate by waiting
  (repair F8): `DeferralState.recordStale` counts them per entry, and from `StoreRepairer.ParkAfterStalePasses` (3)
  on the repair sets `RepairSnapshot.awaitSettle`, so the store parks the reconcile until the object settles (up to
  1 s; `server/store/CLAUDE.md`). Every `proofAfterDeferrals` stale passes log a warning. Only a complete pass resets
  either count: a stale pass neither counts toward nor resets the lock-deferral count, and a lock-deferred pass keeps
  the stale count, so parking continues once started. A hot object alternating busy and locked passes so reaches a
  proof (repair-process 8.1). The bootstrap sweep (no proofs) neither counts nor parks
- The completion gate (`read.timestamp >= missedTimestamp`) is sound (repair F3, X7): object timestamps never
  decrease, and a copied one (Reconcile, rebuild) is a read's, the max over the stores matching its revision, so a
  read that reflects the missed transaction passes it. A whole-object KeyValue pass that defers a locked key still
  takes the read's object timestamp, as a caught-up store would hold it, since key requirements check per-key
  timestamps. The entry stays incomplete until the key is reconciled (repair 13.3-3)
- Absence from the AllocationTree is not a deletion (repair F5): it may be an AllocationFinalizationAction running
  late. For an entry older than `RepairPolicy.minErrorEntryAgeForDeletion` (60 s), `repairAbsent` reads the object
  through its pool, using the entry's pointer (an UnackedCommits entry's own, or one rebuilt from an ErrorTree value,
  `client/internal/transaction/CLAUDE.md`). Only `InvalidObject.provesDeletion` deletes it from the store: more than
  `width - writeThreshold` `ObjectNotFound`/`ObjectMismatch` replies, or a consistent majority at refcount 0 (a live
  object is on at least writeThreshold ≥ consistentRestoreThreshold stores). A restored read means the object is
  live: it is reconciled as if listed. Any other failure keeps the entry. The age gate only paces (cross-document
  G8): a commit voter that has not applied the allocation answers `AllocationPending`, which proves nothing, so a
  clock skewed enough to open the gate early cannot turn a fresh allocation into a deletion. An ErrorTree entry of unknown type is acted
  on only for a proven deletion. A listed object's repair read can find it deleted too (its DeletionFinalizationAction
  not yet run): `repairListed` drops it on the same proof, with no age gate, and defers on an `InvalidObject` short of
  one (repair F15)
- A repair write that meets a foreign occupant of the object's slot (`backend.RepairSlotOccupied`, FSIF slot reuse
  over a missed delete) drops the occupant with an id-checked `repairDelete` once the AllocationTree does not list it.
  The object was just read live, and two live objects never share a slot. The entry stays (stale), and the next pass
  installs the object. A listed occupant is logged as an error and left (cross-document-review X6)
- A deletion the store refuses because the object is in use (`Frontend.RepairDeleteDeferred`) counts as deferred: the
  entry stays for a later pass
- Repair fence: a repair scan runs only while the store's pool entry names both this host and the device the store
  is loaded from here (`RepairTarget.localDevice`, from `HostStoreView.storeDevices`; a store with no device here is
  not owned), so a superseded copy, even one on another device of the same host, never consumes the current copy's
  entries. Each entry delete re-checks it first: one cached ownership read per scan, redone when older than
  `StoreRepairer.OwnershipRecheck` (1 s, constructor seam `ownershipRecheck`). A check that fails leaves that entry and
  abandons the rest of the scan. The bootstrap sweep checks only at its start (it deletes nothing)
