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
