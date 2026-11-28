# System event log

Design, data layout and the surprising behaviors (duplicates, dropped events, best-effort observers, storage
optimizations) are in `DESIGN.md` next to this file: read it before changing anything here.

## Invariants
- **Key encoding**: `loggedEvents` keys are always `LogKey(ts)` (sign bit flipped, big-endian); never `Key` from a raw
  timestamp. State keys are the single-byte keys of `EventLogState`, all present from Bootstrap on.
- **One full object per transaction**: a transaction updates each object at most once. A move transaction writes each
  tail once (one frame) and the state object once; a late rewrite overwrites one full object; every `loggedEvents`
  change of a plan goes into one `prepareUpdates` batch. `loggedEvents` must have a root node from Bootstrap, or the
  first insert would write the root into the state object a second time.
- **Late is `<= tailFloor`**: tails only hold records strictly after `tailFloor`, so a compaction never files an
  object under an existing key.
- **Reader header checks**: every full object read is checked (`filedKey` == tree key, `upperBound` == next tree key,
  first visited key == the floor key) and the read restarts, boundedly, when a check fails. Read order is incoming,
  state/tails, tree. Anything that moves records must keep that forward-only order, or the reader can omit events.
- **Reader reads are settled**: every read the reader makes (incomingEvents and loggedEvents nodes, state, tails, full
  objects) goes through `client.settledReader` (the trees through `KVObjectRootManager(readWith = ...)`), so a move
  seen applied in one read shows in every later one even across pools (`DESIGN.md` 6.1). A new read in the reader
  must be settled too
- **Pin rule**: the move transaction's pin is the revision-checked delete of the moved incoming keys. Client inserts
  are `requireDoesNotExist` (no pin), so `logSystemEvent` is `replaySafe` (hence duplicates, `DESIGN.md` section 7);
  observer and allocator edits are pinned by `KeyRevision` on the key they write.
- **Size checks use zstd's compress bound** plus `FullObject.overhead`, so a written object never exceeds its cap.
- **No static test seams**: tests configure the service and reader through per-client or per-instance hooks (the
  per-client weak maps in `EventLogDurableService`, the reader's instance `var`s), never through global mutable state,
  because suites run in parallel in one JVM.
- Observers and messages are best-effort; correctness must never depend on delivery.
