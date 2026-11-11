### Allocation
- Pool/group rules and the metadata layout: `common/metadata/CLAUDE.md`
- FSIF pools use `FsifIndexAllocationStrategy`: a per-pool bitmap KeyValue object in the metadata group; bits are
  set/cleared in the transaction, or by a system DurableTask after `Transaction.deferAllocationRelease()`
  (`FsifReleaseIndicesTask`: one task per transaction, covering all its FSIF pools). Metadata pools can't use FSIF
- A pool's allocation strategy lives on its `StoragePool`, and `AspenClient.getStoragePool` caches the load's
  Future, so concurrent callers (the commit hooks included) share one instance. Building a second `StoragePool` for
  the same pool would give it a second strategy that the commit hooks never call
