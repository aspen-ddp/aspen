### Allocation
- Pool/group rules and the metadata layout: `common/metadata/CLAUDE.md`
- FSIF pools use `FsifIndexAllocationStrategy`: a per-pool bitmap KeyValue object in the metadata group; bits are
  set/cleared in the transaction, or by a system DurableTask after `Transaction.deferAllocationRelease()`
  (`FsifReleaseIndicesTask`: one task per transaction, covering all its FSIF pools). Metadata pools can't use FSIF
- A pool's allocation strategy lives on its `StoragePool`, and `AspenClient.getStoragePool` caches the load's
  Future, so concurrent callers (the commit hooks included) share one instance. Building a second `StoragePool` for
  the same pool would give it a second strategy that the commit hooks never call
- `AllocationFinalizationAction` records each allocated object in its pool's AllocationTree. Every insert carries a
  `SharedRevisionLock` on the object as read, so a replay or a run after its deletion fails its lock and leaves no
  orphan. It skips the entry only when the read proves the deletion (`InvalidObject.provesDeletion`). Any other read
  failure is retried. Read straight after resolution, the object's commit voters answer `AllocationPending` until they
  apply it (transaction-requirements Q-N5); the read waits for that (`client/internal/CLAUDE.md`). Residual: an
  object already below writeThreshold live copies reads as deleted and gets no entry
