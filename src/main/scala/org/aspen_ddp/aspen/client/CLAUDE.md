### Client API
- `AspenClient` is the primary interface for applications: object allocation, transaction building, reading objects
- Clients start from a local `aspen-client-config.yaml` (system id, credentials, and a cached list of bootstrap hosts)
  and fetch the current bootstrap yaml (bootstrap StoragePool and network endpoints) from any host via `GET /bootstrap`
  (`BootstrapFetcher`). From this the Radicle object (zeroed UUID) is loaded; it embeds pointers to the roots of all
  system metadata structures
- Transaction rules every caller must follow (pin rule, one update per object, store new pointers) are in the root
  CLAUDE.md; builder internals in `internal/transaction/CLAUDE.md`
- Reads are untimed: they retry until they restore the object or fail fatally. `withReadTimeout(d)` returns an
  `ObjectReader` (`internal/TimedObjectReader`) whose every read method fails with `ReadTimeout` after `d` and stops
  retrying. `ReadTimeout` is not an `InvalidObject` (it proves nothing about existence) and `readOptional` does not
  swallow it. `Duration.Inf` returns the client; non-positive or non-finite durations are refused. The timeout is one
  deadline over IDA resolution (an uncached pool's load) and the object read; only the caller's wait on the pool
  load is bounded, since `getStoragePool` shares that load with untimed callers and must never fail it with a
  timeout (`BaseAspenClient.resolveIdaWithin`). For callers that must not wait forever (repair scans, rebuild walks); the
  usage example is `UsageGuide.md` "Timed reads", kept compiling by `examples/TimedReadsSuite`

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
- Retry is the default for both: `StopRetrying.unlessCollision` retries aborted and unknown. An Unknown may have
  committed, so a retry re-runs `prepare` against what that attempt wrote. **A retry closure must recognize its own
  committed attempt** rather than redo the work or fail on it (Todo 5.10, paxos-transactions 10.11): use
  `StopRetrying.collisionRetry()` and, only when `mayHaveCommitted`, accept this call's own post-state (identified by
  an id or key chosen outside the closure) as success, staging nothing so the empty transaction succeeds. Post-commit
  announcements made from `tx.result` must then be made by the attempt that finds the work done, since the committed
  attempt's `tx.result` failed with Unknown. Creates go through `BaseAspenClient.runIdempotentCreate`; durable task
  enrollments that a caller's own guards would not stop pass a fixed `taskId` to `prepareSystemDurableTask`
- `SimpleTaskExecutor.allocateTask` uses one key per allocation, so a retry after an Unknown returns the committed
  attempt's task object. Its allocations run one at a time per executor, so they do not collide on the executor
  object's cached revision (Q-N6). `DiskCatalog.deleteContents` maps unknown to `Fenced`
