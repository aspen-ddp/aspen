# Design: FixedSizeIndexedFile (FSIF) store backend

**Date:** 2026-10-29
**Status:** Draft

## Intent

Aspen needs a store backend built for the workload that RandomAccessReedSolomon (RARS) pools exist for: small
writes and appends into large DataObjects, such as the iSCSI block devices planned next. RocksDB is a poor
match. It rewrites the whole value on every update, and an LSM tree compacting multi-MiB values adds yet more
write amplification.

FSIF stores each slice in a fixed-size segment of one flat file, located by an index embedded in the
ObjectPointer's `storePointer`. Lookups need no search. A small write costs one sequential journal append plus
a later in-place write of the changed bytes.

Correctness is the overriding priority. Every acknowledged commit must survive a crash at any point, including
a correlated crash of every store in a pool at once (a rack power loss). Because all stores of a pool write the
same transaction at nearly the same moment, any window in which a crash can tear a slice could tear the same
object on more stores than the IDA tolerates. That is data loss.

## Goals

- A `store.backend.fsif` backend implementing `Backend`, usable as a pool's backend on any host.
- Small writes and appends (RARS `Append` / `WriteRange`) cost about the size of the change, not the slice.
- Whole-object writes are supported and crash-safe.
- Space freed by deletions is returned to the filesystem (tail compaction), because stores are deliberately
  over-allocated on a device and rebalanced as they grow.
- Recovers on its own from any crash. The store is declared lost (and rebuilt) only when no consistent on-disk
  state exists.

## Non-goals

- **The allocation strategy** (the bitmap of allocated external indices and its release). Specified separately in
  `2026-10-30-fsif-allocation-strategy-design.md`. This design defines only the `storePointer` contract it must
  satisfy.
- **Efficient KeyValue objects.** They must work correctly, but putting them in an FSIF pool is usually a
  mistake. Every KV update is treated as a whole-object write.
- **Bootstrap pool support.** The bootstrap pool stays RocksDB.
- **System-level handling of full devices** (abort votes, write steering, repair afterward). This is in
  `Backlog.txt`. FSIF implements only the backend side: freeze writes and keep serving reads.
- **Preallocating the data file.** Explicitly rejected: stores are over-allocated per device, and the fix must
  be system-wide.
- **Hole punching.** Rejected in favor of tail compaction: it's easier to reason about and works off Linux.
- **Backward compatibility** with any persisted data.

## Anti-goals (failure even if it "works")

- An acknowledged commit that is lost or torn after a crash.
- A read that returns another object's data, or a `repairDelete` that removes another object's slice, because
  an external index was reused.
- Declaring a store lost (forcing a rebuild) when a valid index map and journal pair still exists on disk.
- Blocking the host event thread on I/O or fsync.
- Any flaky test (see `CLAUDE.local.md`).

## Constraints from the existing system

- **Commit completion means durable.** `Tx.allObjectCommitsComplete` drops the CRL object data
  (`crl.dropTransactionObjectData`) as soon as every backend commit completes. A completion must therefore be
  sent only after everything needed to reproduce the commit is fsynced.
- **CRL recovery cannot repair a torn slice.** After a restart, `Frontend` re-reads each object of an
  unfinished transaction. A slice that fails its CRC comes back as `CorruptedObject`, `Tx.objectLoadFailed`
  skips the object, and it is never re-committed. RARS deltas can't be replayed without the old bytes anyway.
  The backend has to be crash-safe on its own.
- **Completions are funneled through the host event thread** (`Host.IOHandler` → `host.events`). Backend
  interface methods are called on that thread.
- **The object cache lets a second transaction commit to an object** before the first one's backend write has
  finished. The backend may see several commits to the same object in flight at once.
- **Every store in a pool shares one external index space** (the pointer is the same for every slice), so
  segment geometry is pool-wide.
- **`maxSliceSize` is the pool's source of truth** (see CLAUDE.md). FSIF's segment payload size *is* its
  backend `maxSliceSize`.
- **Linux is the supported platform**, but nothing here may depend on Linux-only calls. Tail compaction uses
  only truncate.

## Decided design

### Configuration

- New `BackendConfig` case `FsifConfig(segmentPayloadSize: Int, segmentCount: Int)`, with
  `maxSliceSize = Some(segmentPayloadSize)`. Add a Codec entry beside `BACKEND_CONFIG_ROCKS_DB`, plus a
  matching `StoreConfig.Fsif` storage backend.
- Pool-creation tooling (cmdline) takes the desired IDA and maximum object size and computes
  `segmentPayloadSize = ida.minSliceSizeFor(maxObjectSize)`. `segmentCount` is the number of external
  indices (the allocation bitmap's size).
- Every place that hard-codes RocksDB must handle the new case: `Host.scala` (store load around line 935 and
  store creation around 1773–1779), `RebuildingStore.scala` (around 186–191), `Codec.scala` (around 1190 and
  1231) and `cmdline/Main.scala` pool creation.

### storePointer contract

- `storePointer` is exactly 4 bytes, a big-endian `Int` external index `X` with `0 <= X < segmentCount`.
  *(Claude's call. The allocation-strategy design must produce this.)*
- Anything else is invalid. A commit fails with `CommitError.InvalidPointer`, and a read fails with
  `ReadError.ObjectNotFound`.

### Files (all in the store directory)

- `data`: the segments.
- `index.a`, `index.b`: index map checkpoints.
- `journal.a`, `journal.b`: journals.

The journal always lives next to the data file. `crl_save.log` and the transfer, shutdown and rebuild markers
work exactly as they do for RocksDB stores. The directory is closed before transfer, so the archive-based
`StoreTransferManager` needs no change.

### Segment format

Slot `i` (an internal index) starts at byte `i * stride`, where `stride = roundUp(HeaderSize + segmentPayloadSize, 4096)`.

Header *(the ObjectId and alignment are Claude's calls; the rest follows the original plan)*:

| Field | Size | Notes |
|---|---|---|
| object type | 1 | Data / KeyValue |
| ObjectId | 16 | **Required.** External indices are reused after deletion; see below |
| external index | 4 | |
| slice length | 4 | |
| Metadata | 32 | `Metadata.EncodedSize`: revision, refcount, timestamp |
| CRC32 | 4 | Covers every other header byte plus the slice bytes |

Why the ObjectId is required: suppose store A misses the deletion of object O at index X. The bit is cleared, X
is reallocated to P, and P's slice is written to A. The repairer then calls `repairDelete(O, X)` on A, which
would delete P's slice. A client holding a stale pointer to O would likewise read P's data under O's identity.
Every read and `repairDelete` therefore checks that the header's ObjectId matches. A mismatch on a read is
`ObjectNotFound`. A mismatch on `repairDelete` is a no-op success.

### Journal

- **File header:** `<magic><8-byte sequence><4-byte CRC32>`. A journal's header is (re)written only by
  truncating the file to zero length, writing the header and fsyncing. No record is appended until that fsync
  completes. At load:
  - If the header is invalid and the file is no longer than a header, the crash happened mid-rewrite, and no
    record was ever written: the journal is **empty**.
  - If the header is invalid and the file is longer, this is corruption: the **store is lost** (rebuild).
- **Body:** a sequence of batches. Each batch is `<4-byte length><8-byte batch number><records…><4-byte CRC32>`.
  Batch numbers start at 0 for each journal and increase by one per batch. A batch is one write plus one
  fsync, and a record's commit completes when its batch is durable.
- **Torn tail versus corruption.** Batches are appended and fsynced strictly one after another, so only the last
  batch can be torn. Replay stops at the first invalid batch.
  - If no valid batch with the next batch number follows anywhere after it, it is the torn tail. **Truncate it
    away before appending anything**; otherwise new batches written after the garbage would look like
    corruption mid-journal after the next crash.
  - If a valid later batch exists, this is corruption mid-journal: the **store is lost**.
  - *(Claude's call. This matches the original plan's stance that a bad index CRC means a lost store.)*
- Record types:
  - **Map(X, i)**: external index X now maps to slot i. `i = -1` means unmapped (deleted).
  - **Delta(X, i, ObjectId, header, ranges)**: the complete new segment header, plus a list of
    `(offset, bytes)` ranges, for slot `i`. **The target slot is explicit**, and replay applies only Deltas that
    belong to a slot's final occupant (see Replay).
  - Metadata-only commits (refcount or revision changes) are a Delta with no ranges.
- **Delta ranges hold absolute bytes, never XOR deltas.** An `AppendDelta` arrives as XOR regions, but the
  backend journals the resulting bytes, taken from the new full slice, so that replaying a record twice is
  harmless. The ranges are each XOR region's span, plus `[oldLength, newLength)` when the slice grows, because a
  reused slot may hold an old occupant's bytes in the zero-extended area.
- Records are buffered while a batch flush is pending and go out in the next batch.

### Write paths

Each write takes one of two paths depending on the bytes that must reach disk. The size threshold is left to
the builder.

1. **Journaled (small writes, appends, small whole objects, metadata-only).** Append a Delta record for X's
   current slot. An allocation into a free slot `f` appends `Map(X, f)` followed by a Delta covering the whole
   slice and header. The commit completes when the batch is fsynced. The in-place write of the ranges and header
   then happens in the background. The header, including its CRC over the full new slice, is computed at commit
   time and journaled, so applying it in place needs only the journaled bytes, not the full slice.
2. **Copy-on-write (large whole-object writes).** Write the full segment to a free slot `f`, fsync the data file,
   then append `Map(X, f)` and complete when that batch is durable. The old slot is freed per the slot-freeing
   rule below.

Whole-object writes include `Overwrite`, every KV update, `repair`, opportunistic rebuilds and pre-transaction
rebuilds. For KV objects this is the planned behavior, not a deficiency.

**Durable-before-map rule.** A `Map(X, f)` record with `f >= 0` may be appended only after a data-file fsync
that follows *every* write into `f` for the new occupant. The one exception is a journaled allocation, whose
`Map` is immediately followed by a Delta in the same batch that rewrites the whole slot on replay.

**Allocation versus update.** `CommitState` carries an `allocating` flag (from `txd.allocatingObjects`).
- Allocating at an X that is unmapped takes the lowest free slot.
- Allocating at an X that already maps to a different ObjectId (a missed delete on this store) overwrites that
  occupant and logs a warning. It is not an error.
- A non-allocating commit whose X is unmapped, or maps to a different ObjectId, fails with a new
  `CommitError.ObjectMismatch`. With the `Tx` fix below, that error is recorded as a missed update and repaired.
  In practice `Frontend` already prevents this, since a non-allocating commit requires the object to have been
  read successfully, which checks the ObjectId. The error is a backstop.

**CRL re-commit after a crash** needs nothing special. If the commit's journal batch was durable, replay
restores the new revision, and on reload the transaction's requirements no longer match. `RequirementsChecker`
skips the object and the CRL entry is dropped, the same as with RocksDB today. If the batch wasn't durable,
the commit was never acknowledged and is re-applied normally.

**Delete** (a commit with refcount 0) appends `Map(X, -1)`.

**Slot-freeing rule.** A slot becomes reusable only after all three hold:
- the record that unmapped it is durable;
- no read of it is in flight;
- no in-place write to it is pending. Pending writes for a dead occupant are cancelled.

**Ordering per external index.** Physical writes to the slot that X maps to are serialized. Later commits
coalesce: only the latest bytes need writing, but every journal record is still written in commit order. Until
X's pending in-place writes are applied, the slot on disk is stale, so a backend read of X overlays the pending
Delta bytes onto what it reads.

**Backpressure.** Journaled-but-unapplied delta bytes are held in memory until they are written in place.
When they exceed a bound (left to the builder), new commit completions are delayed until in-place writes catch
up. *(Claude's call.)*

### Read path

Look up X's slot, read the header and slice, and check the CRC, ObjectId and external index. A CRC failure is
`CorruptedObject`, and a mismatch is `ObjectNotFound`. Reading the whole segment versus header-first, and when
to copy into a right-sized array, are left to the builder. Every read must complete, including on I/O errors.
If X has pending (unapplied) Deltas, their bytes and the newest pending header are overlaid onto what was read,
and the CRC check uses that header.

### Index map and checkpoints

- An index file is `<magic><8-byte seq><segmentCount × 4-byte Int><4-byte CRC32 over everything before it>`,
  using the same a/b scheme. A journal with sequence `s` holds the records made after the map with sequence `s`
  was snapshotted.
- **Checkpoints chain journals instead of writing to both** *(Claude's call: changed from the original plan.
  Now that the journal carries data deltas, writing every record to both journals during a checkpoint would
  double write bandwidth during that window.)* The procedure:
  1. On the executor, increment the sequence to `s+1`, snapshot the in-memory map, and rewrite the other
     journal's header with sequence `s+1` (truncate, write, fsync). All new records now go there.
  2. Wait until every Delta in journal `s` has been applied in place (or cancelled because its occupant is
     dead), then fsync the data file.
  3. Write the other index file with sequence `s+1` and fsync it.
  4. Truncate journal `s` and index `s` to zero length, and fsync. An empty file is an invalid index and an
     empty journal, both of which recovery handles.
- **Recovery rule.** An index file is valid if its size is exactly right and its CRC matches. Take the valid
  index with the highest sequence `m`, then replay, in sequence order, every non-empty journal with sequence
  `>= m`. Only if no index file is valid is the store lost: throw so the host's rebuild path takes over. A crash
  during step 3 therefore falls back to index `s` plus journals `s` and `s+1`, which is correct. The original
  plan's "full size but bad CRC = lost" rule is replaced, because that is exactly the state a crash during
  step 3's fsync can leave behind.
- **Interrupted checkpoints are finished at startup.** If recovery used two journals, the backend first
  re-runs steps 2–4 for sequence `s+1`, so that one of each file is free for the next checkpoint.
- **Trigger.** Checkpoint when the active journal exceeds `max(indexFileSize / 2, journalBound)`. The bound
  (on the order of tens of MiB) is left to the builder. *(Claude's call: with data deltas in the journal, half
  the index file alone would checkpoint every few dozen writes.)*

### Replay

Replay is a simulation followed by a single application pass, never "apply each record as you go":
1. Starting from the index map, walk the records in order, maintaining the mapping. For each slot, note the
   position of the last `Map(X, i)` that installed its current occupant.
2. Apply a Delta(X, i, …) only if `X → i` is X's **final** mapping and the Delta comes after the `Map` that
   installed X in `i` (or X was already in `i` in the index map and no later `Map` for `i` exists). Apply such
   Deltas in journal order.

This prevents three ways of clobbering live data:
- a dead occupant's old Deltas landing on a slot that was reused by a copy-on-write write;
- pre-move Deltas landing on a freed tail slot;
- old Deltas overwriting a newer copy-on-write version.

Every Delta that is applied writes absolute bytes, so a crash during replay just replays again.

### Startup

After replay:
- **Slot count:** `max(dataFileLength / stride, 1 + highest mapped slot)`. Replaying a Delta past the end of the
  file extends it.
- **Validity:** a mapped slot past the end of the file that no applied Delta covers, or a slot mapped twice,
  means corruption, so fail the load and let the host rebuild.
- **Free slots:** the unmapped slots below the slot count.
- Replay rewrites any torn segment before the store serves anything.

### Tail compaction

- Free slots are allocated lowest first. Whenever the tail slots are free (per the slot-freeing rule), the file
  is truncated to the highest occupied slot.
- **One move:** take the highest occupied slot `t` (external index X) and the lowest free slot `f < t`.
  1. Reserve `f` so no allocation can take it, and mark X as moving.
  2. Apply X's pending in-place writes to `t`.
  3. Copy `t` to `f`.
  4. Commits to X that arrive from step 1 onward are journaled with target `t` and acknowledged normally.
     Their bytes are also written to `f` (after the copy) and to `t`.
  5. Fsync the data file **after the last write into `f`**, then append `Map(X, f)` (the durable-before-map
     rule). Commits to X that arrive after that fsync but before `Map(X, f)` force another fsync before the
     `Map`. Commits after the `Map` target `f`.
  6. Once `Map(X, f)` is durable, free `t` per the slot-freeing rule.
- On replay, Deltas journaled with target `t` are skipped once `X → f` is final, which is correct because `f` was
  fsynced holding them. If the crash came before `Map(X, f)`, they are applied to `t` and `f` stays free.
- A copy-on-write overwrite or a delete of X during a move cancels the move and frees `f`.
- Compaction is driven passively, as in the original plan: each operation completion starts the next move if
  none is outstanding and one is possible. The first operations after startup resume it. There is no dedicated
  thread.

### Threading

- Each backend owns a **single serial executor** for all slot, index, journal and compaction state. Interface
  methods hand work to it and return at once.
- A **host-wide shared I/O pool** performs the reads, writes, fsyncs and truncates, and posts completions back
  to the owning backend's executor.
- This makes "compaction start is atomic with respect to allocation" and per-index ordering hold by
  construction.

### Out of space: freeze writes, keep serving reads

A full device must never take the store offline. If every device filled at once, the whole pool would go
offline. On ENOSPC from any journal append, data write or file growth, the backend:
- **Freezes writes.** Commits whose journal batch isn't durable are held, not completed, so `Tx` keeps their CRL
  data. Acknowledged commits are already safe in the journal, and their in-place writes are held. Checkpoints
  and compaction moves pause.
- **Keeps serving reads.** Reads overlay any held, unapplied Delta bytes onto the slot data read from disk
  rather than waiting for the in-place write, which may be held indefinitely. *(Claude's call: reads always
  overlay pending Deltas, frozen or not, so there is one read path.)*
- **Retries the held I/O** periodically (interval left to the builder) and resumes normally once it succeeds.
  Space returns when a rebalance moves the store or other stores migrate off its device.
- **Exposes its full state** (for example `isWriteFrozen`) for the system-level work to use.

Voting abort on full devices, steering writes away from full stores and relying on the missed-update tree are
system-wide and out of scope. See the "Full storage devices" item in `Backlog.txt`.

### Other I/O failures are fail-stop

After an I/O error other than ENOSPC on a write or fsync of the data file or a journal, page-cache state can't
be trusted. The backend stops accepting writes and completes everything outstanding with an error. Commits fail
with a new `CommitError` value; reads fail with `CorruptedObject`. The store is then marked for rebuild through
the existing `rebuild-required` path. The details of the hookup are left to the builder.

### Rebuild and bootstrap entry points

- `rebuildWrite` gets the pointer (see API changes). The store isn't live during a rebuild, so there is no
  journaled delta:
  - If X is unmapped, take the lowest free slot.
  - If X is mapped (the walk revisits the checkpointed key on resume), overwrite that slot.
  - Write the segment in place and buffer the `Map` record.
- `rebuildFlush` fsyncs the data file, then writes and fsyncs the buffered `Map` records, then runs a
  checkpoint.
- **Crash during a rebuild:** `RebuildingStore` resumes from its last checkpoint, and every object after that
  checkpoint is written again, which covers any slot torn by the crash. Everything up to the checkpoint was
  durable at `rebuildFlush`.
- `bootstrapAllocate` and `bootstrapOverwrite` throw `UnsupportedOperationException`.

### Required changes outside the backend

1. **Pass the pointer and the update's form to the backend.** `CommitState` gains the `ObjectPointer` and an
   update form, either `Full` or `Ranges(oldLength, ranges)`, derived from the `DataUpdateOperation` and the
   `AppendDelta`. It still carries the full new slice for the CRC. `repair` and `rebuildWrite` gain the pointer.
   Call sites: `Frontend.commit`, `Tx` (both `CommitState` constructions), `Frontend.opportunisticRebuild`,
   `Frontend.repair` and `RebuildingStore.restore`, which already has `ptr`. RocksDB and MapBackend ignore the
   new fields.
2. **Stop ignoring commit errors.** `Tx.commitComplete` (`Tx.scala:304`) discards `result`. An error result
   must add the object to `objectCommitErrors`, so `TxCommitted` reports it and `MissedUpdateFinalizationAction`
   records a missed update that repair will heal.
3. **Always complete RocksDB reads.** `RocksDBBackend.read` must complete with an error on Future failure; it
   currently only logs, which leaves `pendingReads` hanging.
4. **Simplify `MissedUpdateFinalizationAction`.** Since backward compatibility isn't required, drop the
   empty-`storePointer` ambiguity comment and handling where it no longer applies. FSIF treats an empty
   `storePointer` on `repairDelete` as a no-op success, with a warning.
5. **Report actual disk usage.** `estimateSize` returns the sum of the data, journal and index file sizes.

## Testing requirements

- **Crash consistency is the core of the test suite.** The backend performs file I/O through a thin interface
  so that tests can inject a crash at every write and fsync boundary, tear writes partway, and drop un-fsynced
  writes. After every injected crash, recovery must reproduce exactly the acknowledged state.
- **A model-based randomized test** checks sequences of allocate, small write, append, overwrite, delete, read,
  compaction and checkpoint against `MapBackend`, with crashes injected at random points. Seeds must be fixed
  or logged, and every run must be deterministic. Flaky tests are not permitted.
- **Concurrency tests** cover concurrent commits to one object, reads during in-place writes, writes and deletes
  during moves, and checkpoints under load.
- **One integration test** runs a RARS pool on FSIF stores through the client: appends, ranged writes, range
  reads, a host restart and a rebuild.
- Tests must be fast (see `CLAUDE.local.md`). Use small segments and small segment counts.

## Left to the builder

- The journaled/copy-on-write size threshold, the journal bound, the unapplied-delta memory bound and the
  batch sizing.
- Whether to read the whole segment or the header first, and when to copy reads into right-sized arrays.
- Whether the segment CRC is recomputed over the whole slice on each write (simplest, about 0.1 ms/MiB with a
  hardware CRC) or kept per block.
- How deltas pending their in-place write are held in memory, and how in-place writes are coalesced.
- The shared I/O pool size and its configuration knob.
- The retry interval while writes are frozen on ENOSPC.
- How the fail-stop state is surfaced to the host so that the store gets rebuilt.
- Whether pool creation warns when KV objects are allocated in an FSIF pool (optional).
