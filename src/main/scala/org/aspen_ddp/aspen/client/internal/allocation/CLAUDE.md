### Allocation
- Pool/group rules and the metadata layout: `common/metadata/CLAUDE.md`
- FSIF pools use `FsifIndexAllocationStrategy`: a per-pool bitmap KeyValue object in the metadata group; bits are
  set/cleared in the transaction, or by a system DurableTask after `Transaction.deferAllocationRelease()`
  (`FsifReleaseIndicesTask`: one task per transaction, covering all its FSIF pools). Metadata pools can't use FSIF
