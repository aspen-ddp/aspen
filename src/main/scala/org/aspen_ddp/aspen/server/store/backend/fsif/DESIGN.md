# FSIF (FixedSizeIndexedFile) store backend

This document describes the FSIF backend as implemented. The original design is
`docs/superpowers/specs/2026-10-29-fsif-backend-design.md`. Where the two differ, this document follows the code.
Paths below are relative to `src/main/scala/org/aspen_ddp/aspen/server/` unless they start with `src/`. Line
numbers are as of the commit this file was written against and will drift.

Terms used throughout:

- **ext** (external index) `X`: the 4-byte big-endian `Int` carried in the `ObjectPointer`'s `storePointer`, in
  `[0, segmentCount)`. The value is the same on every store of the pool.
- **slot** (internal index) `i`: the position of a segment in the `data` file. Slot `i` starts at `i * stride`.
- **Delta**: a journal record that carries a new segment header plus absolute byte ranges for one slot.
- **Mapping**: a journal record `Mapping(X, i, ObjectId)` that maps ext X to slot i (`-1` = unmapped).

---

## 1. Goals and non-goals

### Goals

- **Small writes and appends cost about the size of the change.** A RandomAccessReedSolomon `Append` or
  `WriteRange` reaches the backend as `UpdateForm.Ranges` (`store/backend/UpdateForm.scala:of`). FSIF journals
  only those spans as a Delta and later writes them in place. A refcount or revision change is a Delta with no
  ranges (`UpdateForm.MetadataOnly`). Whole-slice writes above `cowThreshold` take the copy-on-write path, so they
  never pass through the journal.
- **Correctness under a correlated, pool-wide crash.** Every store of a pool writes the same transaction at nearly
  the same moment. If a crash could tear a slice on one store, it could tear the same object on every store and
  exceed what the IDA tolerates. So every acknowledged commit must survive a crash at any point, with no help from
  the CRL (section 3.1).
- **A full device never takes a store offline.** ENOSPC or EDQUOT freezes writes. Reads continue, and the held
  I/O is retried until it succeeds (section 2.9). A store that cannot finish recovery for lack of space is retried
  with backoff and is never moved to `lost/` for a rebuild (section 2.12).
- **Space freed by deletes is returned to the filesystem** through tail compaction and truncation (section 2.7).

### Non-goals

- **The allocation strategy.** It lives outside the backend: `FsifIndexAllocationStrategy` produces the 4-byte
  storePointers (see `docs/superpowers/specs/2026-10-30-fsif-allocation-strategy-design.md`).
- **Efficient KeyValue objects.** They work, but every KV update is a `Full` write.
- **Bootstrap pool.** `bootstrapAllocate` and `bootstrapOverwrite` throw `UnsupportedOperationException`.
- **Hole punching.** Rejected in favour of tail compaction, which uses only `truncate`.
- **Preallocating the data file.** Rejected. Stores are over-allocated per device, and the fix for full devices
  belongs to the whole system.
- **System-level handling of full devices** (abort votes, write steering). See the "Full storage devices" item in
  `Backlog.txt`. FSIF provides only the backend side and exposes `isWriteFrozen`.
- **Backward compatibility** with persisted data.

---

## 2. Design

### 2.1 Configuration

- `store/backend/BackendConfig.scala`: `FsifConfig(segmentPayloadSize, segmentCount)`, with
  `maxSliceSize = Some(segmentPayloadSize)`. `FsifConfig.forObjectSize` sizes segments with
  `ida.minSliceSizeFor(maxObjectSize)`. `segmentCount` is bounded by `FsifLayout.MaxSegmentCount`
  (`(MaxLoadSize - 20) / 20`), which keeps the index file within `FsifRecovery.MaxLoadSize` (`Int.MaxValue - 16`)
  so recovery can read it into a single array. `segmentPayloadSize` is bounded by
  `FsifLayout.MaxSegmentPayloadSize` (about 2 GiB). A store's largest commit must also fit one device journal file
  (`FsifBackend.fitsJournal`, 2.3).
- `common/network/Codec.scala:decodeBackendConfig` and `src/main/protobuf/codec.proto`: `BACKEND_CONFIG_FSIF = 1`.
  `backendMaxSliceSize` carries the payload size and `fsifSegmentCount` (field 13) the count. A decoded FSIF config
  with either value `<= 0` is an `EncodingError`.
- `StoreConfig.Fsif(segmentPayloadSize, segmentCount)` is the per-store config (`storage-engine: fsif`).
- `FsifTuning` defaults: `cowThreshold = 64 KiB`, `unappliedBound = 64 MiB`, `enospcRetry = 1 s`, plus the read and
  move retry delays (2.6, 2.8). The host always uses the defaults. The device journal's sizing comes from the device
  config (2.3).

### 2.2 storePointer contract

`FsifBackend.decodeStorePointer` accepts exactly 4 bytes holding a big-endian `Int` in `[0, segmentCount)`.
Anything else gives `CommitError.InvalidPointer` on commit and `ReadError.ObjectNotFound` on read. A
`repairDelete` with an empty or invalid pointer is a warning and a no-op success. `rebuildWrite` throws
`IllegalArgumentException`.

### 2.3 Files and on-disk formats

The store directory holds `data`, `index.a` and `index.b`, plus the generic files every store has:
`aspen-store-config.yaml`, `crl_save.log`, and the files a copy's phase may add (`phase-param.<phase>`, `arrived-at`;
the phase itself is the directory the copy is in). A store has no journal file of its own: its journal records go to
its device's FSIF journal (below; design `docs/superpowers/specs/2026-11-24-fsif-device-journal-design.md`). Integers
are big-endian. The index and segment CRCs are CRC32, the device journal's CRC32C.

**Data file and segment.** `FsifLayout.stride = roundUp(61 + segmentPayloadSize, 4096)`. A segment is a 61-byte
header followed by `sliceLength` slice bytes (`SegmentHeader.scala`):

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | object type: 0 = Data, 1 = KeyValue (anything else fails to decode) |
| 1 | 16 | ObjectId (msb, lsb) |
| 17 | 4 | ext |
| 21 | 4 | slice length |
| 25 | 32 | Metadata (revision, refcount, timestamp) |
| 57 | 4 | CRC over header bytes 0..56 followed by the slice bytes |

**Index file** (`IndexFile.scala`), `20 * segmentCount + 61` bytes:

| Size | Field |
|---|---|
| 8 | magic `FSIFINDX` |
| 8 | seq |
| 16 | journal id: the device journal the index is attached to (zero for none) |
| 16 | attachment id (4.2 of the spec; below) |
| 8 | covered serial: the highest journal batch serial whose records for this attachment the index and forced data reflect |
| 1 | detached (0 or 1): the directory needs nothing from that journal |
| 20 per ext | slot (4, `-1` = unmapped) then the occupant's ObjectId (16, zero UUID when unmapped) |
| 4 | CRC over every preceding byte |

An index is valid only if its size is exact, the magic matches, the CRC matches, the detached byte is 0 or 1, and no
slot is below `-1`. `IndexMeta.None` (zero ids, covered -1, detached) is a fresh store's.

**Records** (`JournalFormat.scala`), each tagged with its attachment id in the device journal:

| Record | Layout | Size |
|---|---|---|
| Record | len(4), type(1), payload; len counts type + payload | |
| Mapping (type 1) | ext(4), slot(4), ObjectId msb(8), lsb(8) | 29 |
| Delta (type 2) | ext(4), slot(4), SegmentHeader(61), rangeCount(4), then per range offset(4), length(4), bytes | 78 + Σ(8 + length) |

Delta ranges hold absolute bytes of the new slice, never XOR deltas, so replaying a Delta twice is harmless. Parsing
rejects ranges that fall outside `sliceLength`, negative lengths, unknown types and trailing bytes.

**Device journal** (`<device>/fsif-journal/`; `DeviceJournalFormat`, `DeviceJournalFiles`, `DeviceJournalScan`):

| Part | Layout |
|---|---|
| `sizing` | magic `FSIFDJSZ`(8), journal id(16), file count(4), file size(8), CRC(4); written durably once, at creation |
| `<n>.fj`, `n < count` | exactly the file size each, zero-filled with real writes and forced at creation (never `fallocate`d) |
| File header (first 4096 bytes) | magic `FSIFDJHD`(8), journal id(16), salt(8), first serial(8), oldest needed serial(8), CRC(4) |
| Batch (at a 4096 boundary, zero-padded to one) | len(4), serial(8), record count(4), then per record attachment id(16) and a record, CRC(4) |
| `lock` | `CrlLock`'s lock file, held while the journal is open |

- A batch's `len` counts the serial through the last record. Its CRC covers the journal id and the file's salt
  (neither stored in the batch), then `len` through the last record. The salt is a fresh `SecureRandom` long each time
  the writer enters the file. So a stale batch of an earlier pass, or another journal's bytes, never passes as a batch.
- Serials are device-wide, rising by one per batch across files and passes. A file with a valid header holds this
  pass's batches `first`, `first + 1`, ... contiguously after its header, up to the next file's `first - 1`; only the
  newest file may end in a torn tail. A file with an invalid header holds nothing.
- Sizing: at least 3 files of 1 MiB to 1 GiB (a multiple of 4096), default 4 x 256 MiB
  (`DeviceJournalFiles.sizingFor`; device config keys `fsif-journal-files`, `fsif-journal-file-size-mb`, read only when
  a journal is created). A store whose largest commit plus one Mapping cannot fit one file less its header
  (`FsifBackend.fitsJournal`) is refused at create (`IllegalArgumentException`) and at open (`IOException`).
- **Creation is lazy and atomic** (`FsifJournalDisk.ensure`): just before the device's first FSIF store create (and,
  on a device loaded without one, before an FSIF open, `forOpen`), in `fsif-journal.creating`, renamed into place
  durably once every file is written and forced; the device load sweeps a leftover `fsif-journal.creating`.
  RocksDB-only devices never get one. 4 x 256 MiB took 0.5 to 1.1 s to create on NVMe/ext4.

**Creation of a store** (`FsifRecovery.initialize`, run by `server.store.backend.StoreCreation` in a scratch
directory under the device's `creating/`): index.a emptied first if present, index.b emptied if present, data emptied,
and index.a written last with seq 0, `IndexMeta.None` and an empty map. So a valid index implies the rest of the store
exists. The scratch directory, config included, is forced and renamed to the store's path only once `initialize` has
returned; a failure or crash before that leaves nothing at the path, and the scratch copy is discarded. A store path
therefore never holds a partly initialized store, and `open` never initializes (2.13).

**Attach and detach** (spec 4.2, 4.6). Each open is an attachment with a fresh UUID: before its first append the
constructor writes index s+1 naming the journal, the attachment and the journal's tail serial (covered), undetached,
forces it, empties index s, and attaches. Records reach a directory only by attachment id. A close detaches (2.7): its
last checkpoint writes the index detached, and the journal releases the attachment. A directory whose newest index is
detached needs nothing from any journal.

### 2.4 Threading model

- **One `SerialExecutor` per backend** owns all mutable state: index map, slot states, checkpoint and its drain
  buffer, journal room (`capHeld`), compaction and freeze. Every interface method only posts to it (`FsifBackend.post`). Every posted
  task runs inside `guarded`, which catches internal errors (they fail-stop the store) and then runs the epilogue:
  `advanceCheckpoint`, `maybeCheckpoint`, `compaction.drive`, `releaseThrottled`, `maybeForceData`, and
  `checkQuiesce`. The executor is lock-free and has no thread of its own (`SerialExecutor.scala`). Each drain runs
  at most 64 tasks before it yields.
- **I/O runs on `io`.** On a host this is the device's `DeviceIo.dataEc` (threads `dev-<label>-io-N`) and the serial
  executor drains on its `serialEc` (`dev-<label>-serial-N`), so a device hung in fsync stalls only its own stores.
  Results are posted back to the serial executor. `FsifBackend.sharedIo`/`sharedSerial` (`fsif-io-N`,
  `fsif-serial-N`) remain for tests, which may also pass one deterministic EC for both. `close()` closes the channels
  on `io`.
- **The device journal** (`FsifDeviceJournal`) is shared by every FSIF store of the device and safe to call from
  any thread: one lock guards its state and is never held across I/O or a store callback. Each write (a file's
  header when the writer enters it, then one batch) is one DSYNC write, one task on the device's journal thread
  `dev-<label>-fjournal-0` (`DeviceIo.journalRun`, behind the device's gate and recorded in its observer as
  `fsif-journal`, so hang detection sees a stuck journal write). One batch is in flight; appends made meanwhile, from
  every store, form the next batch. Each append's Future completes on its store's serial executor
  (`Owner.complete`), and `roomAvailable`, `checkpointRequested` and `journalFailed` are posted there too. The data
  pool carries no journal writes. Tests give each machine one journal over `SimulatedFsifFiles` (`SharedJournal`),
  its writes on a test EC.
- **ENOSPC retries** are scheduled on `backend.SharedRetryScheduler` (one `backend-retry` thread, shared with RocksDB's read retry).
- **The open's recovery (`openDirectory`) and attach index write run synchronously in the constructor.** The host therefore runs the open or create as a job on
  the device's management lane (`DeviceIo.submit(BackendOpen|BackendCreate, ...)`), never on its control thread:
  `DefaultBackendOpener.open` returns that job's Future, and `StoreLoader.startOpen`, called on the control thread,
  gets the outcome back as a posted `BackendOpened`.
- **Rebuild writes post and return too:** `rebuildWrite` and `rebuildFlush` return Futures and never block their
  caller. They run one at a time in call order (`RebuildSequence`): `rebuildWrite` takes the slot on serial and
  writes the segment on `io` (`IoKind.RebuildWrite`), completing once the write returns (unforced);
  `rebuildFlush` chains the data force, the Mappings' appends and the checkpoint on serial, completing once all are
  durable. **Exception to "post and return":** compaction's truncate of free tail slots runs inline on the serial
  executor (section 5).

### 2.5 Write paths

`FsifBackend.update` handles `fatal`, the pointer, and `SliceTooLarge`. It then runs `updateExt` through
`withExt(x)`, which queues the operation behind any copy-on-write in progress for X. `updateExt` is the decision
table:

| Condition | Result |
|---|---|
| refcount 0, X mapped to a different ObjectId | `ObjectMismatch`, nothing deleted |
| refcount 0 otherwise | `deleteExt`: `Mapping(X, -1)`. If X is already unmapped, append nothing and complete once any pending unmap is durable |
| not allocating, X unmapped or held by a different ObjectId | `ObjectMismatch` |
| allocating over a different occupant (a missed delete) | warning, then overwrite |
| `form == Full` and `data.size > cowThreshold` | copy-on-write |
| otherwise | journaled |

For `repair`, `allocating` is `slot < 0`. A repair allocates an unmapped X and never overwrites a different
occupant. Over a different occupant it fails with `RepairSlotOccupied(objectId, occupant)` instead of a
plain `ObjectMismatch`, so the repairer can drop the occupant once the AllocationTree no longer lists it and the
repaired object is shown still live (repair-process 6.5; cross-document-review 2.4).

**Journaled** (`journaled`, `appendDelta`).
- **Update of the current occupant:** append `Delta(X, slot, header, ranges)`. The ranges come from `rangesOf`:
  `Full` gives the whole slice, `MetadataOnly` gives none, and `Ranges` gives the spans. Spans that do not fit
  fall back to the whole slice, with a warning. So do spans whose encoding (8 bytes each plus their bytes) would be
  larger than the whole slice's, so a Delta is never larger than `appendBound` allows (2.9).
- **New occupant:** X gets the lowest free slot, or keeps its slot when it overwrites a missed delete. Then
  `Mapping(X, slot, id)` and a full Delta are appended in **one** call, so they go into one batch.
- The header, including the CRC over the full new slice, is computed at commit time. Applying the Delta needs
  only the journaled bytes.
- The commit completes when its batch is durable in the device journal (one DSYNC write per batch, one batch in
  flight per device, later appends of every store of the device batched into the next). Only then is the `PendingDelta` marked `durable` and
  eligible for its in-place write.

**Copy-on-write** (`copyOnWrite`, `cowWritten`, `cowMap`).
1. X is gated (`gated(x)`), and any compaction move of X is cancelled.
2. The lowest free slot f is reserved. If f grew the slot count it goes into `tailCows`.
3. Header plus slice are written to f, then `forceData`.
4. Only after a successful force: `assign(X, f)`, `kill` the old slot, and append `Mapping(X, f, id)`. The commit
   completes when that batch is durable.
5. The gate is released, and the operations queued on X run in order against f.

Reads are not gated, because X's index entry does not change until step 4.

### 2.6 In-place writes, read path and overlay

**Slot state** (`SlotState.scala`). Each slot with pending work has a list of `PendingDelta`s in commit order,
split into four runs: forced (kept only while a read is in flight), written but not forced, being written, and
queued.

**In-place writes** (`kick`, `inPlaceDone`).
- At most one write task runs per slot. It coalesces the run of durable queued Deltas, writes every range, then
  writes the newest header last.
- A completed write is tagged with `nextForceId` and waits in `unforced`. The Delta is finished (no longer
  unapplied, no longer holding back a checkpoint) only when a data force with that id or higher succeeds
  (`forcedUpTo`).
- `maybeForceData` issues a background force when something waits on the written Deltas (a quiesce, a throttled
  reply, a checkpoint in `Applying`) or when unforced bytes reach half of `unappliedBound`.
- Data forces run one at a time through `forceData` / `startDataForce`.

**Read path** (`startRead`, `readExt`, `finishRead`, `assemble`).
1. Look up X. Unmapped, or `ids(X) != pointer.id`, gives `ObjectNotFound`.
2. Otherwise register a read on the slot. This blocks pruning and freeing of the slot.
3. Read the header plus `max(newest pending sliceLength, disk sliceLength)` bytes.
4. Overlay **every** Delta in the slot's list, in order (`SlotState.overlay`).
5. Check the CRC against the newest pending header, or the disk header if none is pending. A mismatch gives
   `CorruptedObject`. A header ObjectId or ext mismatch gives `ObjectNotFound`.
6. Retry instead of completing in two cases: X moved or changed while the read was in flight, or the result is
   `CorruptedObject` and the slot's `version` changed (pread and pwrite are not atomic, so the header read could
   tear).

Reads never wait for in-place writes, frozen or not. Only evidence of damage (no valid header, a CRC mismatch) or a
fail-stopped backend gives `CorruptedObject` at once. A failure that is not evidence of damage (an I/O error from the
disk read, a rejected io task, an internal exception while issuing or assembling) is retried after each of
`tuning.readRetryDelays` (default `RetryScheduler.ReadRetryDelays`: 10 ms, 100 ms, 1 s) on `scheduler`, issued from
step 1 again, and gives `CorruptedObject` only once they are spent, or at once when the backend is closing (a retry
never touches closed files). A recovered commit vote fail-stops its store on `CorruptedObject` (systemic-loss D7), so
one transient host condition must not produce it. A read failure does **not** fail-stop the backend.

### 2.7 Checkpoints

A checkpoint writes an index covering the store's applied records, so the device journal can reuse the files holding
them. `maybeCheckpoint` starts one when the journal asks (`Owner.checkpointRequested`: this store's oldest needed
record pins the ring's oldest file under pressure, 2.9), or when an operation waits for journal room while the store
has records no checkpoint covers. `rebuildFlush`, `close` (the detach) and the test hook `checkpointNow` start one too.
Only one runs at a time, none starts while frozen or failed, and a closing store still checkpoints. It moves from seq
s to s+1 (`startCheckpoint`, `advanceCheckpoint`, `finish`):

1. **Draining.** Snapshot the index as of the last record appended (`oldTail`). `appendSeq` becomes s+1, and new
   appends wait in order in `drainBuffer` until `oldTail` is durable; its batch serial is the covered serial `S`. The
   buffered appends are then issued in order. The journal merges one attachment's queued appends into one batch, so
   without the wait a post-snapshot record could share S's batch and be taken as covered (Ruling R8). Draining does
   no I/O and runs while frozen.
2. **Applying.** Wait until no epoch-s Delta is unapplied (`unappliedBySeq`). "Applied" means forced in place, that
   is, covered by a successful data force, or cancelled.
3. **Barrier.** Wait until everything appended so far is durable (`lastAppend`). A cancelled epoch-s Delta may still
   be mapped in the snapshot, and the record that cancelled it came after the snapshot.
4. **Finishing.** Write index s+1 (covered `S`, detached for a close) into the other index file and force it, then
   truncate and force index s. Then tell the journal: `covered(attachment, S)`, or, for a detach, `release`.

Each I/O step runs through `checkpointIo`. ENOSPC holds the step and re-runs it from its start. Any other
failure is fail-stop. While frozen the checkpoint pauses where it stands. A checkpoint needs no journal space.

**Detach on close** (spec 4.6). Once quiesced, `close` runs a checkpoint with `detach` set (or marks a running one
that covers every append), so the index it writes covers every append and is detached, and the attachment is
released; only then are the files closed. A frozen, abandoned or failed store does not detach: it stays attached
(`Backend.closedAttached`), its records stay in the ring for the next device load to replay, and the host releases the
attachment once the copy is durably out of replay's scope (`server/host/CLAUDE.md`).

### 2.8 Tail compaction and truncation

`Compaction.scala` is passive: `drive()` runs after every serial task.

**Truncation** (`truncate`). The slot count is lowered to `used.length()` (highest used slot + 1). A longer
`data` is cut to that many strides. It only ever shrinks. The truncate is not forced; recovery is correct whether
or not it survives a crash. "Used" covers mapped slots, reserved slots (copy-on-write, rebuild and move targets),
and dead slots still waiting for the slot-freeing rule.

**One move** (`start`, `copied`, `pump`, `written`, `forced`, `appendMap`) of ext X from the highest used slot t
to the lowest free slot f < t:
1. Reserve f. Read slot t raw, registered as a read on t.
2. Overlay t's pending Deltas and newest header (`assemble`), then write the image to f.
3. Deltas that arrive for t after the image was assembled (`onDelta`) are queued as writes into f as well. Their
   records are still journaled against t.
4. When no write into f is in flight or queued, force data.
5. Append `Mapping(X, f, id)` only if no write into f was issued after that force started. Otherwise force again
   (durable-before-map).
6. `commitMove` assigns X to f and kills t in the same serial task. t is freed once the Mapping is durable.

A move does not start for an object that is gated, being rebuilt, or dead. A copy-on-write, delete,
re-allocation or rebuild write of X before step 5 cancels the move (`cancel`), and f is released once the move's
I/O has settled. A failed read of t in step 1 (an I/O error, a rejected io task) ends the move the same way: nothing
was written into f, so the store's state is not in doubt. No move starts until the next of
`FsifTuning.moveRetryDelays` (10 ms, 100 ms, 1 s, then 10 s repeating; a successful move read starts over) has
passed, and none starts once the backend is closing. A damaged t reads successfully and is copied as it is; a
client read of f then reports the damage (`CorruptedObject`), as a read of t would have (tests: `FsifFaultSuite`
"a failed compaction move read abandons the move, ...", "a rejected compaction move read task ...", "a move retry
delay that ends after close starts no move"). On ENOSPC, writes and forces into f are held and re-run. A data force failure re-queues every
write into f that no good force has covered (`dataLost`).

### 2.9 Backpressure and journal room

A commit that arrives while `unappliedBytes > unappliedBound` is journaled normally, but its successful reply is
held in `throttled` until unapplied bytes drop to the bound or below (`ack`, `releaseThrottled`). Error replies
are never held.

**Journal room** (`admitted`, `releaseCapHeld`; `FsifDeviceJournal.tryReserve`). Every append the store issues must
fit the device journal without waiting, so that every checkpoint's Barrier completes:
- Every commit, repair, repairDelete and `rebuildFlush` chunk reserves an upper bound on the bytes it can append before
  it touches any state: `appendBound(min(sliceSize, segmentPayloadSize))` (a Mapping plus a one-range Delta of the whole
  slice, each with its 16-byte attachment tag; Ruling R13(b): an oversized slice reserves the largest bound and replies
  `SliceTooLarge`), or one Mapping (`MappingAppendSize`, 45 bytes) for a repairDelete. A `rebuildFlush` chunk is at
  most `RebuildMappingsPerAppend` (4096) Mappings, fewer when that would not fit one journal file (Ruling R11).
  Copy-on-write and delete commits are covered by the same bound. A compaction move reserves its own Mapping
  (`journalHasRoomForMove`), one move at a time.
- The journal charges a reservation of padded size `c` as `2c - 4096` against its room: what is left in the current
  file plus every free file after it, less what is already reserved. That covers the unusable tail a batch may leave
  in a file, so a reserved append is always written without waiting for a file to free. One larger than a file less
  its header is refused for good; `fitsJournal` keeps every store's largest commit below that.
- An operation is admitted if nothing is held before it and the reservation succeeds. Otherwise it waits in `capHeld`,
  in arrival order, neither completed nor failed, and the epilogue starts a checkpoint if the store has uncovered
  records. The journal calls `Owner.roomAvailable` once room covers a refused charge, and asks the stores pinning the
  oldest file to checkpoint (pressure: no free file after the current one and the current one 3/4 full, a refused
  reservation, or a batch waiting for a file). Reads go on and do not see held operations.
- A reservation is returned when the operation replies (its appends are written) or if it never appends.
- Fail-stop fails every held operation with `IOFailure`. A close of a frozen store abandons them as it abandons held
  I/O (2.10); a later open of that directory releases the old attachment first, returning its reservations (Ruling
  R13(a)).
- **Exception: `rebuildFlush` chunks.** `rebuildFlush` calls `assign` for every buffered Mapping before its chunks
  are admitted, so a held chunk has mutated the index. This is safe: the rebuild store is not live; `data` is forced
  before any assign, so durable-before-map holds for any index snapshot; no Delta's cancellation depends on these
  Mappings; and Barrier waits only on real appends.

**Why journal room cannot deadlock a checkpoint.** A held operation has mutated nothing (rebuild chunks excepted, as
above) and appended nothing, and no admitted operation waits for one. Every checkpoint condition looks only at
admitted records: Draining waits for `oldTail`, an admitted append, which fits and is written; the appends it buffers
belong to admitted operations and are issued once it completes; Applying waits for epoch-s Deltas; Barrier waits for
`lastAppend`, the last admitted append. Writing the index is not an append. So every checkpoint finishes and its
`covered` lets the writer reuse the files it pinned. While frozen nothing starts a checkpoint, so a frozen store holds
the ring (2.10), and every FSIF store of the device then waits for room as its commits fill it.

### 2.10 ENOSPC freeze

`FsifIo.isOutOfSpace` matches "no space left", "disk full", "insufficient space", "disk quota exceeded" or
"edquot" anywhere in the cause chain. Each I/O source that fails this way calls `hold(key, kind, retry, abort)`.
The sources are: in-place write, data force, checkpoint step, copy-on-write, move write or force, and truncate. A
device journal write is never one: it overwrites preallocated space, and any failure of it is J4 (4.1).

While anything is held, the store is **frozen**:
- In-place writes, checkpoints, compaction moves and truncation start nothing new, except their own held retries.
- Commits are still accepted and journaled. Checkpoints do not start, so the store's records pin the device journal's
  ring; once it is full, new operations of every FSIF store on the device wait for room (2.9) until space returns.
  Decided: no evacuation (spec 4.5); out of space is per file system, so the neighbours are almost always frozen too.
  A held operation is never failed, so `Tx` keeps its data in the CRL.
- Reads go on and overlay pending Deltas.
- `retryRound` re-issues every held operation every `enospcRetry`. When the last one succeeds the store unfreezes
  and paused work resumes.

**Data force failure (fsyncgate).** A failed fsync on Linux marks the failed pages clean, so retrying the force
would succeed without writing them. A data force that fails with ENOSPC therefore bumps `dataEpoch` and calls
`dataLost`, and every write that no good force has covered is issued again:
- in-place Deltas written but not forced are re-queued (`requeueWritten`), and a write still in flight is
  re-queued when it completes (stale epoch);
- a move's writes into f are re-queued (`Compaction.dataLost`, `written`);
- a copy-on-write whose segment write completed in a stale epoch, or whose force waiter failed, rewrites the
  segment (`segmentWritten`, `cowWritten`);
- rebuild writes not yet mapped cannot be re-issued, so `rebuildLost` makes this incarnation refuse further
  `rebuildWrite` and `rebuildFlush` (section 2.11).

A data force failure that is **not** ENOSPC is fail-stop.

**Closing a frozen store** (`close`, `abandonHeld`). Held I/O is abandoned exactly as a crash would
abandon it, and the close waits only for I/O already in flight. Abandoned commits never complete. That is safe:
- held commits were never acknowledged, and Tx keeps them in the CRL;
- acknowledged Deltas are in the device journal and replay at the next device load (the store does not detach);
- held checkpoint, move and copy-on-write steps re-run, or are moot, at recovery.

### 2.11 Fail-stop

`failStop(t)` runs at most once:
- sets `fatal`;
- fails every append waiting in `drainBuffer`, every operation waiting for journal room, every held operation's
  abort, and every throttled reply, each with `IOFailure` (appends already issued complete as the journal writes or
  fails them);
- calls `onFatal` once.

Afterwards commits complete with `IOFailure`, reads with `CorruptedObject`, rebuild entry points throw, and
`hasFailed` is true. The attachment stays live (no detach): its records stay in the ring until the host releases it,
once the copy's move to `lost/` is durable. A device journal write failure fails the journal: every pending append
fails, and each unreleased attachment's owner gets `journalFailed`, which fail-stops its store (J4). The full list of
sources is in section 4.2.

### 2.12 Rebuild entry points

- `rebuildWrite` (rebuild only; the store is not live):
  1. cancels any move of X;
  2. reuses X's mapped slot, or the slot already reserved for X in this rebuild, or allocates the lowest free
     slot;
  3. writes header plus slice in place on the calling thread;
  4. buffers `(X -> slot, id)` in `rebuildMaps`.

  If the write fails, a slot taken for this write alone is released.
- `rebuildFlush` forces data, appends and forces the buffered Mappings (durable-before-map), then runs a
  checkpoint.
- Both throw an out-of-space `IOException` when the store is frozen, or becomes frozen before a step completes
  (`throwIfFrozen`, `freezeWaiters`). They also throw after `rebuildLost` (2.12). `RebuildingStore` latches
  that as `REBUILD OUT OF SPACE`. It never waits for space.

### 2.13 Host integration

- **Open and create** (`StoreLoader.startOpen`, `handleBackendOpened`, shared by every backend) run on
  the device's management lane. A create (`BackendOpener.create`, `StoreCreation`) builds the store in `<device>/creating/` and
  renames it into place, then opens it as any load does; `open` never creates. While an open runs, its copy is Opening in the copy registry, which keeps
  the store silent until it is installed. A `ShutdownStore` that arrives meanwhile is deferred and
  replayed after the store is served.
- **Outcomes** (`handleBackendOpened`; FsifRecovery.StoreLost is a `BackendStateLost`):

  | Outcome | Host action |
  |---|---|
  | success | `serveOpened` and `LoadStore` |
  | `StoreLost` | move the copy to `lost/`, leave the store offline, `checkStorageDevice` |
  | a create that left nothing at the path | leave offline, not retried here: its entry is still Initializing and the next device check creates it again |
  | any other failure (out of space, I/O, lock or resource error) | leave offline, its copy left where it is; it awaits a load retry (AwaitingRetry), offered every device scan with exponential backoff capped at `LoadRetryCap` (5 min) |

- **The device journal** (`DefaultBackendOpener`). A create first makes sure the device has a journal
  (`FsifJournalDisk.ensure`: created if absent, sized by the device config) and refuses a layout that does not fit it.
  An open runs against `io.fsifJournal` (`FsifJournalDisk.forOpen`; a device loaded with no journal directory gets one
  first, Ruling R17); a failed journal, or a journal directory present while none is open, is an `IOException`, a load
  retry. The device load opens and replays the journal before any store opens (`FsifJournalOpen`, step 2b of
  `DeviceDiscovery.loadOwnedDevice`; `server/host/CLAUDE.md`), so every store opens detached or attached to an
  attachment that needs no replay (Ruling R12); any other undetached index naming the journal is an `IOException`.
- **No create on open** (systemic-loss-protection 4.1, row D5; 2.3 creation). A missing
  store directory, or one whose FSIF files are all absent or empty (`FsifRecovery.isBlank`), is `StoreLost` (L0):
  creation is atomic, so such a directory lost its store. Every other directory is recovered. A stat that fails
  (`server.FileStat`) is an I/O error, retried, not an absence.
- **onFatal** posts `BackendFailed`. The routes are listed in section 4.2.
- **Frozen close.** `ShutdownStore` logs and closes without waiting for space.
- **`estimateSize`** is the sum of the data and index file sizes (the device journal is the device's), kept in memory by `SizeTrackedFiles` (statted once at open, then updated by every write and truncate through its handles), so it does no I/O on the store's executor.

---

## 3. How correctness is guaranteed

Each invariant gives the failure it prevents, where it is enforced, and the tests that pin it. Test files are
under `src/test/scala/org/aspen_ddp/aspen/server/store/backend/fsif/` unless named otherwise.

### 3.1 Commit completion means durable
- **Why:** `Tx.allObjectCommitsComplete` drops the CRL's object data as soon as every backend commit completes, and
  the CRL cannot repair a torn slice after restart. A completion sent before durability loses an acknowledged
  write.
- **Where:**
  - `appendDelta`, `cowMap` and `deleteExt` reply only from `onDurable(appendRecords(...))`.
  - `FsifDeviceJournal.written` completes a batch's promises only after its DSYNC write (in tests, write and force)
    returns.
  - The in-place write is never required for durability, because the journal holds the bytes.
  - `kick` writes only `durable` Deltas, so a slot is never changed ahead of its journal record.
- **Tests:** `FsifDeviceJournalSuite` "crash at every op of 20 appends and 2 file switches scans Valid and holds every
  completed append"; `FsifBackendSuite` "two stores sharing one journal both survive a crash with every acknowledged
  write" and "100 appends of 37 bytes
  read back byte-exactly, before and after a crash"; `FsifModelSuite`.

### 3.2 Durable-before-map
- **Why:** a `Mapping(X, f)` that is durable while f's bytes are not would, after a crash, map X to a torn or stale
  segment. Replay drops every earlier Delta for X, so nothing could repair it.
- **Where:**
  - `copyOnWrite` and `cowWritten` append the Mapping only after `forceData` succeeds.
  - `Compaction.pump` appends `Map(X, f)` only when `!m.dirty` after a successful force.
  - `rebuildFlush` forces before appending.
  - A journaled allocation is the one exception: its Mapping and a full Delta share one batch, and replay rewrites
    the whole slot.
- **Tests:** `FsifBackendSuite` "a copy-on-write overwrite cut short by a crash is old or new, never torn";
  `FsifCompactionSuite` "a commit whose write into f runs after the force before Map(X, f) forces data again
  first" and "a crash at every file op of one move with an in-flight Delta keeps the acknowledged state".

### 3.3 Per-object journal order
- **Why:** replay selection (3.4) depends on the journal order of X's records matching commit order. A Delta
  journaled before a Mapping it logically follows would be dropped. One journaled after a later Mapping would land
  on the wrong incarnation.
- **Where:**
  - Everything goes through `appendRecords`, which keeps order while a checkpoint drains (`drainBuffer`). The device
    journal's queue is FIFO and each batch takes a prefix of it, so one attachment's records stay in append order.
  - `withExt` and `gated` hold every operation on X behind a copy-on-write until its Mapping is appended
    (`releaseExt`).
  - A journaled allocation that must wait for a tail copy-on-write gates X the same way (`journaled`,
    `afterTailCows`).
- **Tests:** `FsifBackendSuite` "a small write right behind a copy-on-write survives a crash once acknowledged
  (Ruling 14)" and "two commits to one object issued back to back leave the second's data"; `FsifCheckpointSuite`
  "Ruling R8: a commit made while a checkpoint drains shares no batch with the snapshot's last append".

### 3.4 Replay selection rule
- **Rule:** `Replay.run` keeps `Delta(X, i)` at position p iff `finalSlot(X) == i` and p is after X's last Mapping
  (any slot). Kept Deltas are applied in journal order.
- **Why:** this stops three kinds of damage: a dead occupant's Deltas landing on a reused slot, pre-move Deltas
  landing on a freed tail slot, and old Deltas overwriting a newer copy-on-write version.
- **Range checks:** slots have no upper bound in replay (recovery bounds them separately). An ext outside
  `[0, segmentCount)` throws `InvalidRecord`, which recovery turns into `StoreLost`.
- **Tests:** all of `ReplaySuite`, for example "dead occupant's delta is not applied to a reused slot", "pre-move
  deltas are not applied to the freed tail", "old delta does not overwrite a newer copy-on-write version",
  "reallocation to the same slot applies only the new incarnation", "a move that never completed", and "an ext out
  of range in a Mapping throws InvalidRecord".

### 3.5 Slot-freeing rule
- **Rule:** a dead slot (deleted, replaced by copy-on-write, or moved from) becomes free only once its unmapping
  record is durable, no read is in flight on it, and no in-place write is in flight.
- **Why:** if the slot were freed earlier, a crash before the unmap is durable could replay X back into a slot
  already overwritten by a new occupant, and a read in flight could return another object's bytes.
- **Where:** `kill` (cancels queued and unforced Deltas, marks `dead`), `unmapDurable`, `tryFree`.
- **Tests:** `FsifBackendSuite` "a slot freed by a delete is not reused before the delete is durable" and "a slot
  with a read in flight is not reused, and the read completes cleanly".

### 3.6 Lowest-free-slot allocation and the recovery bound
- **Rule:** `allocateSlot` always returns `used.nextClearBit(0)`. Recovery rejects any kept Delta slot or final
  mapped slot above `max(D0 / stride, 1 + highest slot in index m) + (number of replayed Mappings with slot >= 0)`.
  D0 is the forced data size before replay (`FsifRecovery.replayDirectory` step 2).
- **Why:** a CRC-valid but bogus slot number must not make recovery grow `data` enormously. The bound holds
  because each Mapping can grow the slot count by at most one.
- **Tail copy-on-write exception:** a copy-on-write that grew the slot count is invisible to every term of the
  bound until it is forced. So a journaled allocation above an unforced `tailCows` slot waits for it
  (`afterTailCows`, `tailCowDone`). Without that wait, a crash would put a legitimate slot above the bound and
  declare the store lost.
- **Tests:** `FsifBackendSuite` "allocations above in-flight copy-on-write tail slots never make recovery lose the
  store"; `FsifRecoverySuite` "a Mapping beyond the reachable slot bound -> StoreLost" and "a chain of allocations
  each taking the next new top slot is accepted".

### 3.7 Checkpoint Draining and Barrier
- **Why:**
  - Index s+1's covered serial S must not cover a record left out of the snapshot. Replay applies only records above
    S, so such a record would be lost. The device journal batches one attachment's queued appends together, so a
    record appended after the snapshot could share S's batch.
  - Index s+1 must not be written while it maps a slot whose cancelling record (appended after the snapshot) is not
    yet durable. Otherwise a crash would pair index s+1 with a journal that lacks that record.
  - The ring must never reuse a file holding a record above a live attachment's covered serial.
- **Where:**
  - `appendRecords` holds every append in `drainBuffer` while a checkpoint is Draining; `advanceCheckpoint` issues
    them, in order, only once `oldTail` has completed with S (Ruling R8).
  - `Applying` requires `unappliedBySeq` to have no seq below s+1; `Barrier` waits for `lastAppend`.
  - `finish` tells the journal `covered(attachment, S)` only after index s+1 is forced. `FsifDeviceJournal.nextWrite`
    enters a file only when every unreleased attachment's oldest needed serial is above that file's last serial of its
    previous pass (`freeRun`, `lastOf`, `minNeeded`).
- **Tests:** `FsifCheckpointSuite` "Ruling R8: a commit made while a checkpoint drains shares no batch with the
  snapshot's last append", "a journal-s Delta cancelled by a later delete holds the checkpoint until that delete is
  durable", "a crash at any file op during a checkpoint with commits in flight keeps every acknowledged write" and
  "two back-to-back checkpoints (a to b to a) read back correctly after a crash"; `FsifDeviceJournalSuite` "the writer
  enters the next file only when no unreleased attachment needs it", "an idle attachment pins nothing" and "a released
  attachment pins nothing".

### 3.8 Force coverage and fsyncgate
- **Rule:** a Delta is resolved only by a successful data force that started after its write completed, with no failed
  data force since that write completed. The tag is `nextForceId` at completion, `forcedUpTo(id)` resolves tags
  `<= id`, and forces are serialized. A failed force drops the tags of every written but unforced Delta (`dataLost`),
  including a slot's whose next in-place write is still in flight, so no later force counts them.
- **ENOSPC on a force:** every uncovered write is re-issued (2.10).
- **Other force failures:** fail-stop.
- **Why:** after a failed fsync the kernel may report later forces as successful although the pages never reached
  disk. A checkpoint covering a record on the strength of such a force would lose an acknowledged Delta: its record is
  never replayed again.
- **Tests:** `FsifFaultSuite`:
  - "ENOSPC on the data force of a checkpoint's step 2 re-issues the in-place writes before the journal is
    emptied";
  - "ENOSPC on a data force while the slot's next in-place write is in flight: the next force does not count the
    slot's earlier dropped writes as forced" (the regression found by the three-store model: a force issued before
    that write completed counted the dropped Deltas as forced);
  - "ENOSPC on a copy-on-write's data force re-issues the segment and another object's unforced in-place write";
  - "ENOSPC on a compaction move's data force re-issues its copy into f before Map(X, f)";
  - "after a data force fails with a rebuild write unmapped, that incarnation refuses further rebuild work".

  Also `SimulatedFsifFilesSuite` "a faulted force discards the pending ops: ..." and the model suite's
  `dataForceFaults` coverage floor.

### 3.9 Torn tail versus corruption, with the salt
- **Rule** (`DeviceJournalScan.scan`, spec 4.7 step 1):
  - Every file header is read; the valid ones are ordered by `first`. The needed range runs from the newest header's
    oldest needed serial to the tail. Batches below it are not read; where one fails, the walk resumes at the later
    4096 boundary holding the oldest needed serial (none there is corrupt).
  - Every needed serial must be present, CRC-valid and in order in the file 4.1 assigns it to. A failed batch in a
    file other than the newest is corrupt, as is an oldest needed serial below the oldest valid file's `first`.
  - In the newest file, the first batch that fails or has the wrong serial ends the journal: a torn tail unless a
    CRC-valid batch with the next serial exists at a later 4096 boundary of that file (`findLater`), which is
    corrupt.
  - A CRC-valid batch whose records do not parse is corrupt. No valid header at all is Empty, which is corrupt if any
    attached directory has a covered serial >= 0 (`DeviceJournalRecovery.recover`).
- **Why it holds:** one batch is in flight, so only the newest file's last batch can be torn. The writer rewrites a
  file's header (fresh salt) with a DSYNC write before its first batch of the pass, and every batch CRC covers the
  journal id and that salt, so an earlier pass's batches and another journal's bytes end the file instead of passing
  as batches. A file being entered when a crash tore its header holds nothing needed, which the needed range decides.
  After recovery the writer enters the next file before its first batch, so it never appends after a torn tail
  (Ruling R4). User data inside a torn batch cannot pose as the next batch: it would need the salt.
- **Tests:** `DeviceJournalScanSuite` (all of it, e.g. "torn last batch in the newest file is a torn tail", "a rotted
  batch followed by a valid next batch is Corrupt", "a torn header rewrite of the file being entered is ignored", "a
  rotted header of a file inside the needed range is Corrupt", "stale batches of an earlier pass (old salt) after the
  current pass's last batch end the file, not Corrupt"); `DeviceJournalFormatSuite` "truncation at every byte of a
  batch is Invalid, never Unparseable" and "a batch is Invalid under another salt or journal id";
  `DeviceJournalRecoverySuite` "an empty ring with an attached directory whose covered serial is >= 0 throws
  JournalCorrupt and writes nothing".

### 3.10 ObjectId checks against reused exts
- **Why:** an ext is reused after deletion. A store that missed O's delete may later hold P at the same ext. A
  stale pointer to O must never read P's data, and `repairDelete(O, X)` must never delete P.
- **Where:**
  - The index carries each occupant's ObjectId.
  - `readExt` compares it with the pointer, and `assemble` compares the header's ObjectId and ext.
  - `updateExt` returns `ObjectMismatch` for a refcount-0 or non-allocating commit to a different occupant.
  - `startRepairDelete` deletes only when `ids(X)` matches. Otherwise it is a no-op success.
- **Tests:** `FsifBackendSuite` "appends into a reused slot never expose the previous occupant", "a non-allocating
  commit to an unmapped or differently occupied ext is ObjectMismatch", "allocating over a missed delete replaces
  the occupant, journaled or copy-on-write", "repairDelete of a stale ObjectId leaves the new occupant readable",
  and "repair at an ext held by a different object fails naming the occupant, and succeeds once it is dropped".

### 3.11 Idempotent recovery
- **Rule:** recovery is split in two (`FsifRecovery`). At device load, `DeviceJournalRecovery.recover` scans the ring
  without writing it, then runs `replayDirectory` for each directory whose newest index names this journal and is not
  detached, with that attachment's records above its covered serial. Every step of `replayDirectory` is one of three
  kinds:
  - it only reads (step 1: load the newest index, forcing each index file before reading it; step 2: replay and
    validate, L7-L10);
  - it rewrites identical bytes (step 3: absolute Deltas, then force, then L11 and L12);
  - it moves the directory to a state whose own recovery yields the same objects (step 4: index s+1 written
    detached, covering the ring's tail, forced, then index s emptied).

  Until step 4's index is valid every rerun sees the same inputs; once it is, the directory is detached, so a device
  load no longer replays it, and data already holds every kept Delta. The writer starts only after every directory is
  replayed, entering the next file, so the ring a rerun scans is unchanged. A crash inside the device load followed by
  another load therefore yields the same state. At store open, `openDirectory` only reads (L1, L11, L12) and the
  constructor's attach index is an ordinary index switch.
- Steps decide on whether files exist (`FsifFiles.exists`/`size`); `NioFsifFiles` answers them with `server.FileStat`,
  so a file is absent only when known absent and a stat that fails throws an `IOException` (4.6).
- **Tests:** `FsifRecoverySuite` "replayDirectory is idempotent across crashes inside replayDirectory (30 seeds)" and
  "a torn index s+1 is ignored and replay starts from index s"; `DeviceJournalRecoverySuite` "crash at every op of
  recover, then recover again, yields the same objects (20 seeds)"; `FsifModelSuite` "crash during recovery" (its
  `inReplay` floor).

Validation order differs slightly from "validate everything before writing anything". The sliceLength,
double-mapping and bound checks run before step 3. The "mapped slot past EOF" check and the slot-count overflow
check run after step 3 has written and forced the kept Deltas, because applied Deltas extend the file. These are
idempotent writes, so a rerun sees the same inputs, but a directory declared lost by those two checks has already
been modified.

### 3.12 Testing approach

- **`SimulatedFsifFiles`** is deliberately harsher than a real disk:
  - each file keeps its durable image plus the ops pending since its last force;
  - `crash()` keeps each pending op independently with probability 1/2, which also models reordering;
  - a kept write may tear, applying a random subset of its 512-byte sectors;
  - a write beyond EOF zero-fills the gap;
  - a faulted force **discards** every op pending on that file, as Linux does after failed writeback: reads still
    see them, but no later force makes them durable;
  - faulted writes and truncates have no effect;
  - creating a file is durable, matching `NioFsifFiles.open`, which syncs the directory;
  - `failNext` / `failWhile` inject `OutOfSpace` or `IoError` by file and op.
  Tests run the device journal over the same simulated machine (`SharedJournal`, `PrefixedFsifFiles`), with
  `forceAfterWrite` standing in for DSYNC.
- **crashAfterOps sweeps.** `crashAfterOps(n)` stops the simulated machine after n more ops, and `crash()` then
  yields the durable image. Sweeps cover:
  - every op of one move (`FsifCompactionSuite` "a crash at every file op of one move ...");
  - random points inside checkpoints and compactions with commits in flight (50 seeds each);
  - every op of 20 appends and two file switches of the device journal (`FsifDeviceJournalSuite`);
  - every op inside device-load replay (`DeviceJournalRecoverySuite`, `FsifRecoverySuite`).
- **Model-based suite** (`FsifModelSuite`):
  - three stores sharing one device journal on one simulated machine, so every crash covers all of them and each
    recovery is a device-load replay followed by each store's open;
  - 400 random operations over 16 exts each: allocate, small write, append, overwrite, delete, read, checkpoint,
    compaction, ENOSPC windows and crashes;
  - checked against a per-ext history: after each crash, every ext must read as its last acknowledged state or a
    later issued one, other ObjectIds as `ObjectNotFound`, and nothing as `CorruptedObject` unless the store
    fail-stopped;
  - 30 seeds for "random operations match the model across crashes", and 30 for "crash during recovery";
  - coverage floors of at least 3 across the seeds for: crashes inside a checkpoint, inside a move and while
    frozen; mid-checkpoint images; later-index images; data-force faults; and, with interrupted recovery,
    crashes inside recovery, inside recovery's index switch and inside device-load replay;
  - at least 1 stale-index image.
- **Determinism.** io is a seeded `ShuffleEc` that runs on the test thread, and ENOSPC retries run on a
  `ManualScheduler`. The first two seeds of each model run are re-run and their digests compared.
- **Host and integration:**
  - `src/test/scala/org/aspen_ddp/aspen/server/HostFsifLoadSuite.scala`: open off the event thread, StoreLost and
    fail-stop marking, the out-of-space backoff, frozen close, and the device journal's load paths (R19, R20, runtime
    failure);
  - `src/test/scala/org/aspen_ddp/aspen/server/host/FsifJournalLostSuite.scala`: the lost-journal path (J1, J2), a
    failed move, a locked journal, the open's classification (R18);
  - `src/test/scala/org/aspen_ddp/aspen/server/host/FsifJournalReleaseSuite.scala`: releases of attachments a close
    left live (a durable move to lost/, deletes of pending-delete and discarded copies) and the refused undetached
    transfer;
  - `DeviceShutdownSuite`: fail-stop during shutdown and transfer closes;
  - `DeviceAdoptionSuite`: CrlLost adoption, including a failed-on-close store;
  - `src/test/scala/org/aspen_ddp/aspen/FsifIntegrationSuite.scala`: a RARS pool through the client covering
    appends, ranged writes, restart, and a lost store rebuilt.

---

## 4. Automatic lost store

**The dominant real-world cause of data loss in a system like this is not hardware.** It is a systemic bug that
wrongly fires one of the automatic paths below. A store then decides, from local evidence alone, that its content
cannot be trusted. Its copy is moved to `lost/`, then retired, and it is rebuilt from its peers.
One store wrongly declared lost costs a rebuild. The same bug firing on many stores at once, for example on every
host after an upgrade, loses the whole pool, because the peers the rebuild needs are the stores being thrown
away. This cannot be solved inside the backend: each store sees only its own files. See the `Backlog.txt` item
"System-level protection against correlated 'lost store' declarations".

Every case below runs without an operator. Code is named by class and method; a recovery check is located by its
step in `FsifRecovery.replayDirectory` (its numbered steps) and the `StoreLost` message it throws.

### 4.1 Detected by FSIF recovery (`FsifRecovery.StoreLost`)

Recovery runs in two places (3.11). At a store open, `FsifBackend.open` and `FsifRecovery.openDirectory` throw from
`FsifBackend`'s constructor: L0, L1, L11, L12 and J5. The host reaction is the same for each:
`StoreLoader.handleBackendOpened` logs, leaves the store offline, moves the copy to `lost/`
(`StoreRebuilds.moveToLostOnLane`), and calls `DeviceReconciler.checkStorageDevice`. At the device load,
`FsifRecovery.replayDirectory` (inside `DeviceJournalRecovery.recover`, in the `FsifJournalOpen` job) finds L1 and
L7-L12 for a directory, which is then lost alone (J3). A copy in `lost/` is acted on as described in 4.5. A plain
`IOException` from either is not a loss (4.6). Every absence below is a known one: the directory and file tests are
`server.FileStat`'s, so a stat that fails is such an `IOException`, never L0 or a missing index or journal.

| # | Trigger | Where (`FsifRecovery` unless noted) | Why it is believed to mean loss | What a false trigger from a systemic bug looks like |
|---|---|---|---|---|
| L0 | The store directory is missing, or every FSIF file is absent or empty | `FsifBackend.open`; `DefaultBackendOpener.openFsifBackend` | A store directory appears at its path only complete, by StoreCreation's rename, so its files were lost (an incomplete restore or mount, a wiped directory) | A code path that makes a store directory outside StoreCreation (a new create path, a test helper copied into production), or a transfer or rebuild that moves a directory before its files are written |
| L1 | Neither index file is valid (size, magic, CRC, detached byte, or a slot < -1) | `newestIndex`, "no valid index file" (open and replay) | `initialize` writes index.a last, before the store is renamed into place, and every index switch empties index s only after index s+1 is forced. So a store always has one valid index unless something damaged it | An index size or CRC computation that changes with the code: e.g. a change to `IndexFile.EntrySize`, the meta fields or `sizeFor`, or a `segmentCount` read differently from the pool config. Every store would fail on first open after the upgrade |
| L2 | An index file is larger than `MaxLoadSize` (`Int.MaxValue - 16`) bytes | `load`, "is too large to load" | Not reachable: the index is at most `IndexFile.sizeFor(MaxSegmentCount)` | A change to `sizeFor` or `MaxSegmentCount` |
| L7 | A Mapping or Delta carries an ext outside `[0, segmentCount)` (`Replay.InvalidRecord`) | `Replay.checkExt` via `replay`, "invalid journal record" (replay) | Only a writer bug or corruption past the CRC produces one | A `segmentCount` that shrinks or is read from the wrong source at load: every store of the pool would fail on any record above the new count |
| L8 | A kept Delta has sliceLength > segmentPayloadSize | `replayChecked`, "has slice length" (replay) | `update` rejects such slices with `SliceTooLarge` | A `segmentPayloadSize` computed differently after an upgrade (e.g. from `forObjectSize` instead of the stored config) |
| L9 | After replay, two exts map to one slot | `replayChecked`, "is mapped by ext" (replay) | Allocation marks the slot used, and freeing waits for the unmap to be durable | A bug in `tryFree` or `assign` that frees or double-assigns a slot; replay selection or Mapping order changed |
| L10 | A kept Delta's slot, or a final mapped slot, is above `max(D0/stride, startTop+1) + mappings` | `replayChecked`, `checkBound`, "beyond the reachable bound" (replay) | Lowest-free allocation makes each Mapping grow the slot count by at most one (3.6) | The highest-risk check. An allocator that skips a slot (e.g. a new reservation kind), a new write path that maps a slot not covered by `tailCows`, or a miscalculated bound would fire on every store after an upgrade, typically only on stores that had a crash with the right pattern in flight, which makes it look like scattered hardware loss |
| L11 | A mapped slot starts at or beyond the end of `data` (after applying and forcing the kept Deltas, at replay) | `checkedSlotCount`, "beyond the data file" (open and replay) | A mapped slot was either written in place (forced before the index that maps it) or its Delta was replayed | A truncation that is too aggressive (compaction's `used` missing a mapped slot) that became durable; a `stride` change |
| L12 | The computed slot count exceeds `Int.MaxValue` | `checkedSlotCount`, after L11, "slot count ... is too large" (open and replay) | Not reachable with a bounded data file | A `stride` computed as 0 or very small |

L3-L6 (the per-store journals' seq rules and corruption) and L2's journal half went with those journals; their
device-level counterparts are J1 and J2.

**Device journal triggers** (spec section 5; `host/FsifJournalDisk.scala`, device load). J1 and J2 move every FSIF
copy on the device to `lost/` and remove its FSIF staging directories (the lost-journal path, modelled on the lost
CRL's; `server/host/CLAUDE.md`); RocksDB copies are untouched. J3 and J5 lose one copy (or staging directory).

| # | Trigger | Where | What a false trigger from a systemic bug looks like |
|---|---|---|---|
| J1 | The ring is corrupt: a needed serial missing or out of order (a gap, a rotted header hiding batches, a failed batch before the newest file), a CRC-valid batch whose records do not parse, a valid batch after an invalid one in the newest file, or no valid header while an attached directory has covered >= 0; also any other non-transient failure of the recovery once the device holds FSIF directories, and a transient one on `FsifJournalOpenAttempts` scans in a row | `DeviceJournalScan.scan`, `DeviceJournalRecovery.recover` (`JournalCorrupt`); `FsifJournalDisk.open` | A change to the record or batch encoding, the CRC's inputs or the serial arithmetic: every device's FSIF copies after an upgrade, at once |
| J2 | No journal directory on a device holding an FSIF directory whose newest index is undetached | `FsifJournalDisk.open` | A journal directory renamed by new code, or an index's detached flag or journal id read wrongly |
| J3 | One directory's replay fails (L1, L7-L12) | `FsifJournalDisk.openPresent`, `moveLostAlone` | As for L1 and L7-L12, now found at device load |
| J4 | A device journal write fails at runtime | `FsifDeviceJournal.fail` → each attached store's `journalFailed` (4.2, L13) | An error classifier or an I/O path bug that fails the device's journal: every FSIF store on the device fail-stops, and the device is taken out of service until the host restarts (`DeviceShutdown.fsifJournalFailed`) |
| J5 | A newest index undetached but naming another journal (Rulings R6, R20) | `DeviceJournalRecovery` `ForeignUndetached` (device load); `openDirectory` (store open) | A transfer or move path that ships a directory without detaching it (every legitimate foreign index is detached: a close detaches, `TransferDisk.moveOutbound` refuses an undetached copy, a create writes `IndexMeta.None`), or a journal id read wrongly |

Refusals, never a loss: a journal locked elsewhere (`JournalLocked`) or whose files do not match its sizing
(`JournalSizingMismatch`), and a device config that cannot size a replacement, refuse the device (Failed, Refused)
until fixed.

Related, for staging only: `RebuildingStore.openStaging` (`rebuild/RebuildingStore.scala`) opens the rebuild's
staging store through the host's `BackendOpener` and catches `BackendStateLost`, which `StoreLost` extends. It
deletes the staging directory and its checkpoint (`cleanupStaging`), so the next pass starts over. The live store is
already Rebuilding, so this costs only rebuild progress. A recovery bug of the L0-L12 kind would make every rebuild
restart from scratch forever. A staging store that closes attached (fail-stopped or frozen) is discarded the same way.

### 4.2 Detected at runtime by FSIF fail-stop (`onFatal`)

Every fail-stop calls `onFatal` once. The host's `onFatal` posts `BackendFailed` (`StoreLoader.openBackend`).

| # | Trigger | Where |
|---|---|---|
| L13 | The device journal failed (a write failed, J4: every attached store), or the append a checkpoint drains behind failed | `FsifBackend.owner.journalFailed`; `advanceCheckpoint` (Draining) |
| L14 | An in-place write fails with a non-ENOSPC error | `FsifBackend.inPlaceDone` |
| L15 | A data force fails with a non-ENOSPC error | `FsifBackend.dataForceDone` |
| L16 | A checkpoint step (index write, index truncate) fails with a non-ENOSPC error | `FsifBackend.checkpointIo` |
| L17 | A copy-on-write segment write or force fails with a non-ENOSPC error | `FsifBackend.cowWritten` |
| L18 | Compaction: a move write or force fails with a non-ENOSPC error; the truncate of free tail slots fails with a non-ENOSPC error. (A failed move read is not a trigger: it abandons the move, 2.8.) | `Compaction.written`, `forced`; the truncate (`Compaction.truncate`) fails through `FsifBackend.guarded` |
| L19 | Any `NonFatal` exception escapes a serial task, checkpoint advance or compaction drive (an internal error), for example `allocateSlot`'s `IllegalStateException`, a rejected io task, or an NPE | `FsifBackend.guarded` |

**Why these are believed to mean loss.** After a write or fsync error other than ENOSPC, the page cache no longer
says what is on disk, so the store's state cannot be trusted. The recovery a restart would run might well succeed,
but the backend does not assume it.

**What a false trigger looks like.** L19 is the broad one. Any bug that throws inside the serial executor (an
assertion, an index out of range, a new `require`) fail-stops the store. A bug on a common path (every commit,
every checkpoint) fires on every FSIF store on every host running that version. Reads trigger none of them: a
failed client read is retried and only then returns `CorruptedObject`, and a failed compaction move read abandons
the move and retries it after a backoff, so a host-wide EMFILE, ENOMEM or saturated io executor stalls compaction
but fail-stops no store. L13-L17 would fire together if an error classifier misses ENOSPC (for example a filesystem or
JDK that words it differently from `FsifIo.isOutOfSpace`'s strings), which turns every full device into a
correlated loss. L13 fires on every FSIF store of a device at once (J4), and the device then stays out of service until
the host restarts.

**Host routes for `BackendFailed` and other failed-backend checks.** Each of these moves the copy to `lost/` once it is closed (`StoreRebuilds.moveToLostOnLane`):

- **Served store** (`StoreRebuilds.handleBackendFailed`, then `backendFailed`). The store is unloaded, moved to `lost/` when its close ends, and left
  offline, and the device is checked. Its CRL entries stay live in the device CRL (`closeUnloaded(dropDurably =
  false)`) until the rebuild's retire saves them with the copy it keeps and drops them.
- **Not served, not replaced, directory on a loaded device** (`StoreRebuilds.backendFailed`).
- **Failed before its `LoadStore` ran** (`StoreLoader.handleLoad`, then `StoreRebuilds.retireUnserved`).
- **Opened but not served** (device unloaded or another backend serves it) **and failed**
  (`StoreLoader.handleBackendOpened`).
- **Failed during a `ShutdownStore` close** (`StoreLoader.handleClose`, then `StoreRebuilds.failedOnClose`). The
  store is moved to `lost/`, so an adopter treats the store as CrlLost (L22), and nothing loads it to read its save
  file. A `crl_save.log` a transfer-out close just wrote is kept, since its entries have left
  the device CRL; the rebuild's retire renames it to `lost-crl-state.log` in the retired copy. Entries left in the
  device CRL are saved and dropped by that retire, once the copy is in `lost/`.
- **Transfer out** (`StoreTransfersOut.startOut`):
  - a store whose copy is already in `lost/` and whose entry is TransferringOut is returned to rebuild
    (`StoreTransfersOut.returnLostTransferToRebuild`);
  - a close that fails with `StoreFailedOnClose` is never shipped, and its transfer is aborted;
  - a store moved to `lost/` while it closed is likewise never shipped.

### 4.3 Host-level, backend-independent paths that apply to FSIF stores

| # | Trigger | Where | What happens | Why believed | False trigger looks like |
|---|---|---|---|---|---|
| L20 | `crl_save.log` exists but `CrashRecoveryLog.loadValidStoreState` rejects it (truncated, unparsable, or naming another store), or its state can never fit the device's CRL (`StoreLoader.neverFits`: a transaction above `maxTransactionBytes`, or the whole above the admission limit at the most streams any config allows plus that bound) | `StoreLoader.handleLoad`, then `StoreRebuilds.crlSaveFileLost` and `retireUnserved` | Closed unserved, moved to `lost/`, offline, device checked | The file is the only copy of the store's transaction state | A CRL save format change: every store with a save file (every store closed by a network transfer out) is declared lost on its next load; for never-fits, a change to `CrlAdmission.recoveredBytes` or the CRL bounds in a release, or a transfer onto a device with a smaller CRL. **Generic: all backends** |
| L21 | Adoption of a device: an Offline or Active store's copy is in `outbound/` (a network transfer out) but has a missing or unusable `crl_save.log` | `DeviceDiscovery.adoptionClass`; the copy moved to `lost/` in `DeviceMetadata.adoptDevice`; entry written Rebuilding in the same transaction | Adopted as Rebuilding, copy moved to `lost/` | The transfer's close forced the file, so it was lost afterwards | A save-file read path that fails on the adopting host's version (format, or a path change in `Backend.crlSaveFile`): every store on every moved device. **Generic** |
| L22 | Adoption: an Offline or Active store's copy is in `lost/` | `DeviceDiscovery.adoptionClass` | CrlLost, so adopted Rebuilding | The previous host already moved it to `lost/` | Inherits whatever falsely declared it lost on the previous host. **Generic** |

At LoadStore a *missing* `crl_save.log` is not a loss: it loads with no saved state (`StoreLoader.handleLoad`). A missing file
counts as loss only at adoption (L21), where a copy in `outbound/` shows that a transfer's close extracted the state.

### 4.4 Count

There are 22 automatic cases:
- 9 found by FSIF recovery (L0, L1, L2, L7-L12), at store open or device-load replay;
- 3 more found at device load or store open (J1, J2, J5; J3 is L1/L7-L12 at device load, J4 is L13 at device scope);
- 7 FSIF fail-stop sources (L13-L19), reaching `lost/` through the host routes listed in 4.2;
- 3 generic host paths (L20-L22);
- J1 and J2 each lose every FSIF copy on their device.

In addition, the rebuild staging store is discarded on `StoreLost`, on a replay failure, on a lost journal, and when
it closes attached (4.1, related).

### 4.5 What happens after the move to `lost/`

1. `StoreLoader.tryLoadStore` never loads a copy in `lost/`. Its copy stays offline on its
   device (the copy registry's Lost record), which keeps the store silent.
2. The reconcile's Lost CRL State pass (`DeviceReconciler.reconcileDeviceState`) finds offline stores whose copy is in `lost/` and whose
   entry is Active and calls `StoreRebuilds.requestRebuild`. That flips the entry to Rebuilding in a transaction, but only if the device is
   not failed and is owned by this host.
3. `StoreRebuilds.startRebuild` calls `discardLostStore` first, which retires the copy (`StoreCopies.retire`): its CRL
   state saved into `lost-crl-state.log`, its device-CRL entries dropped, then the directory moved, durably, to
   `<device>/pending-delete/<copy>`. The copy is deleted (rename-first, through `<device>/deleting/`) only
   once the store is Active on the device its pool names (`StoreCopies.release`).
4. `RebuildingStore` rebuilds into `<device>/rebuilding/...` and moves it into place.

So the store's content is gone as soon as its rebuild succeeds. The Backlog item proposes keeping it longer, in
`<storage-device>/suspected-loss/<store-dir>-<uuid>`, together with the reason, time, software version and the
check that fired, and leaving the decision to a system-level check that looks for correlation across stores,
pools and hosts.

### 4.6 Deliberate non-cases

These look like loss but never trigger a rebuild:

- **Out of space at load** (`StoreLoader.handleBackendOpened`). The store is left offline and retried with backoff. A
  failed create leaves nothing at the path and is created again by the next device check.
- **Out of space at runtime.** ENOSPC or EDQUOT freezes writes (2.10). It is never fail-stop, including during
  retries. Only a non-ENOSPC error during a retry is fail-stop.
- **A crash or failure during creation.** It happens in `creating/`, which the device's next load deletes; nothing
  reaches the store's path, so nothing is opened or moved to `lost/`.
- **A torn tail in the device journal's newest file.** The scan ends there, and the writer starts in the next file.
- **Batches below the needed range, and invalid headers outside it** (an earlier pass, a torn header rewrite of the
  file being entered). They are not read.
- **A device with no journal and no FSIF directory attached to one.** Loaded Absent; nothing is created until an FSIF
  store needs it.
- **A journal locked elsewhere, one whose files do not match its sizing, or a device config that cannot size a
  replacement.** The device is refused until fixed (Ruling R18); nothing is moved.
- **An FSIF copy with an unreadable config that holds index files** (Ruling R19). The device stays unloaded for the
  next scan.
- **An undetached index naming this journal at a store open** that the running journal does not know as replay-free.
  An `IOException`, a load retry (Ruling R12).
- **One invalid index file** while the other is valid (a torn index s+1 falls back to index s).
- **A CRC mismatch on a read.** `CorruptedObject` goes to the repair path. The store stays served.
- **An `IOException` during open that is not out of space.** It is logged ("its backend failed to open"), and the
  store is left offline, its copy where it is. Like out of space at load, it is retried at device scans with backoff
  doubling up to `Host.LoadRetryCap` (the copy's AwaitingRetry, `StoreLoader.retryFailedLoads`).
- **A failed `crl.loadStore`** after a valid save file. `UnloadFailedStore` takes the store offline and keeps the
  file. It is not moved to `lost/`.
- **A failed removal of the loaded save file** (`StoreRouter.crlLoaded`). `SaveFileRemovalFailed` takes the store
  offline with its device-CRL entries kept and retries its load every device scan; the retry removes the file
  durably, forcing the directory even if it is gone, before the store recovers. It is not moved to `lost/`.
- **A transferred-in copy with an unusable save file.** It is not promoted, and the transfer retries
  (generic, `HostDeviceDiscoverySuite` "a transferred copy with an unusable CRL save file is not promoted and the
  transfer restarts").
- **`rebuildLost` and staging fail-stop.** The rebuild pass aborts and resumes from its checkpoint in a new
  incarnation.

Tests for the automatic paths:
- `HostFsifLoadSuite` "an FSIF store whose recovery finds it lost is marked for rebuild and left offline", "a
  served FSIF store that fails fatally is unloaded, marked for rebuild and left offline", "a fatal failure of an
  FSIF backend closed and not replaced marks the store for rebuild", and "an FSIF store that fails during its
  close is marked for rebuild, not closed cleanly";
- `DeviceShutdownSuite` "a store whose backend fails during its shutdown close does not stop the shutdown" and "a
  transfer out whose store fails during its close is never sent and goes back to be rebuilt";
- `DeviceAdoptionSuite` "a store in lost/ is adopted as Rebuilding" and "a store whose network transfer
  left it in outbound/ with ... save file is adopted as Rebuilding";
- `HostDeviceDiscoverySuite` truncated-CRL tests;
- `StoreRebuildTriggerSuite` "an Active store whose copy is in lost/ is flipped to Rebuilding, discarded and
  rebuilt";
- `FsifIntegrationSuite` "an FSIF store whose recovery finds it lost is rebuilt through the rebuild path".

Tests for the non-cases: `DeviceJournalScanSuite` "torn last batch in the newest file is a torn tail", "a rotted
batch below oldestNeeded is not read" and "a torn header rewrite of the file being entered is ignored";
`FsifJournalLostSuite` "a locked journal refuses the device and moves nothing", "a journal open that fails before
anything is decided never loses the journal, however many scans" and "Review Focus 2: a device whose FSIF copies all
came from another host loads Absent"; `FsifRecoverySuite` "openDirectory refuses an undetached index of its own journal
with an IOException"; `FsifInitializationSuite` "a store holding objects is
recovered on open", "an initialized store is recovered as the empty store, not re-initialized" and "a directory with
non-empty files and no valid index is lost and left untouched"; `StoreCreationSuite` (a failed create leaves nothing at
the store path); `HostFsifLoadSuite` "an FSIF store whose files hold no valid index is marked for rebuild, not
emptied", "a create that runs out of space leaves nothing at the store path, and the next device check serves it" and
"a device load removes what a crashed create left in creating/, and never loads it". L0: `FsifInitializationSuite` "a
missing directory is lost, ..." and "a directory whose FSIF files are all absent or empty is lost, ... (SL D5)";
`HostFsifLoadSuite` "an FSIF store whose files are gone or empty is marked for rebuild, not served empty (SL D5)".

---

## 5. Known limitations and open items

- **Index files are read whole during recovery.** `FsifRecovery.load` reads each into one array and throws
  `StoreLost` above `MaxLoadSize` (`Int.MaxValue - 16`) bytes; the index stays below it through `MaxSegmentCount`.
  Device journal recovery reads the headers, then one file at a time (at most 1 GiB each).
- **One frozen store holds its whole device.** A store frozen by ENOSPC cannot checkpoint, so its records pin the
  ring, and once the ring is full every FSIF store on the device waits for room; reads go on. So does a fail-stopped
  copy until its move to `lost/` is durable (`retryPendingLost`). Accepted: no evacuation (spec 4.5).
- **Journal room counts conservatively.** A reservation is charged `2c - 4096` and counts until its operation
  replies, even once its records are written, and a commit's bound assumes a Mapping plus the whole slice. Operations
  can therefore wait while the ring is still well below full.
- **A frozen or failed close leaves an attachment live until it is released** (a reopen of the same directory
  releases it, Ruling R13(a); otherwise the host's move or delete does). A frozen FSIF staging store restarts its
  rebuild from scratch.
- **Truncate runs inline on the serial executor** (`Compaction.truncate`). It is the one blocking file call made
  there, so a hung device can stall that store's serial work during a truncate.
- **A moving object's Map can livelock.** `Compaction.pump` appends `Map(X, f)` only if no write into f started
  after the last force began. A continuous stream of commits to X keeps `dirty` set, so the move never completes.
  It still holds f reserved and keeps t from being truncated. Nothing breaks: commits to X are acknowledged
  normally, and a copy-on-write, delete or re-allocation cancels the move.
- **`bringIntoService` gossips early.** `DeviceDiscovery.bringIntoService` gossips `StoreMoved`
  (`gossipStoreOnline`) for every Active store right after `tryLoadDevice`. For FSIF stores the open is still
  running on the device's lane, and it may yet find the store lost or out of space. The reinstated-transfer path waits
  for `onServed`, but this restart and adoption path does not.
- **StoreRepairer leaves errorTree entries behind** after a successful repair (pre-existing; `Backlog.txt`).
  Found during the FSIF integration test.
- **FSIF metadata pools are refused.** `create-pool --backend fsif` works; `createNewMetadataPool` and
  `create-metadata-pool` refuse FSIF, since the strategy's bitmap lives in the metadata group.
- **Correlated lost-store declarations** (section 4) need system-level protection (`Backlog.txt`). A missing marker
  is no longer destructive (4.6), but a crash during a fresh store's `initialize` can still declare that new store
  lost.
