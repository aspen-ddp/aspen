### Client API
- `AspenClient` is the primary interface for applications: object allocation, transaction building, reading objects
- Clients start from a local `aspen-client-config.yaml` (system id, credentials, and a cached list of bootstrap hosts)
  and fetch the current bootstrap yaml (bootstrap StoragePool and network endpoints) from any host via `GET /bootstrap`
  (`BootstrapFetcher`). From this the Radicle object (zeroed UUID) is loaded; it embeds pointers to the roots of all
  system metadata structures
- Transaction rules every caller must follow (pin rule, one update per object, store new pointers) are in the root
  CLAUDE.md; builder internals in `internal/transaction/CLAUDE.md`

### IDA data operations
- RandomAccessReedSolomon lays data out in rows of `k * cellSize` bytes with an 8-byte length trailer, so
  DataObjects in such pools support efficient appends (`Transaction.append` with an `AppendPosition`) and
  byte-range/tail reads (`readRange`, `readAppendPosition`, `readRanges`), all served by a single `ByteRanges`
  read request (ranges plus an optional tail). `readRanges` and `readRangedWriteState` therefore read one
  consistent revision. They also support ranged writes
  (`Transaction.rangedWrites`): the caller supplies a `RangedWriteState` (from `RangedWriteState.fromObject` or
  `ObjectReader.readRangedWriteState`) and stores receive only sparse XOR deltas of the changed bytes
- Replication supports all of them too. Classic ReedSolomon does not: those operations fail with
  `RangeReadNotSupported` / `AppendNotSupported` / `RangeWriteNotSupported`
- Store side of the deltas (`EncodedAppendDelta`): see `server/store/CLAUDE.md`

### Transaction outcomes
- A commit fails with `TransactionAborted` (final: it did not and will not commit) or, past the transaction horizon
  (`common.transaction.TransactionHorizon`, 20 s from `startTimestamp`), `TransactionOutcomeUnknown`: it may have
  committed or aborted and the client can no longer find out. The two are siblings, not subtypes. Unknown records
  nothing in the client's status cache
- Retry is the default for both: `StopRetrying.unlessCollision` retries aborted and unknown, and
  `SimpleTaskExecutor.allocateTask` treats unknown like an abort (a committed-but-unknown attempt leaves a duplicate
  task, as a false abort did). Its allocations run one at a time per executor, so they do not collide on the
  executor object's cached revision (Q-N6). `DiskCatalog.deleteContents` maps unknown to `Fenced`
