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
  with backoff and is never marked for rebuild (section 2.12).
- **Space freed by deletes is returned to the filesystem** through tail compaction and truncation (section 2.7).

### Non-goals

- **The allocation strategy.** Nothing produces 4-byte storePointers yet. `cmdline/Main.scala` refuses
  `create-pool --backend fsif` (`FsifPoolsEnabled = false`), and tests construct FSIF pools directly.
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
  `FsifLayout.MaxSegmentPayloadSize` (about 2 GiB), so one commit's records always fit a loadable journal (2.9).
- `common/network/Codec.scala:decodeBackendConfig` and `src/main/protobuf/codec.proto`: `BACKEND_CONFIG_FSIF = 1`.
  `backendMaxSliceSize` carries the payload size and `fsifSegmentCount` (field 13) the count. A decoded FSIF config
  with either value `<= 0` is an `EncodingError`.
- `StoreConfig.Fsif(segmentPayloadSize, segmentCount)` is the per-store config (`storage-engine: fsif`).
- `FsifTuning` defaults: `cowThreshold = 64 KiB`, `journalBound = 64 MiB`, `unappliedBound = 64 MiB`,
  `enospcRetry = 1 s`, `journalHardLimit = 1 GiB`. The host always uses the defaults.

### 2.2 storePointer contract

`FsifBackend.decodeStorePointer` accepts exactly 4 bytes holding a big-endian `Int` in `[0, segmentCount)`.
Anything else gives `CommitError.InvalidPointer` on commit and `ReadError.ObjectNotFound` on read. A
`repairDelete` with an empty or invalid pointer is a warning and a no-op success. `rebuildWrite` throws
`IllegalArgumentException`.

### 2.3 Files and on-disk formats

The store directory holds `data`, `index.a`, `index.b`, `journal.a`, `journal.b` and `fsif-initialized`, plus the
generic files every store has: `aspen-store-config.yaml`, `crl_save.log`, and the transfer and rebuild markers. All
integers are big-endian, and every CRC is CRC32.

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

**Index file** (`IndexFile.scala`), `20 * segmentCount + 20` bytes:

| Size | Field |
|---|---|
| 8 | magic `FSIFINDX` |
| 8 | seq |
| 20 per ext | slot (4, `-1` = unmapped) then the occupant's ObjectId (16, zero UUID when unmapped) |
| 4 | CRC over every preceding byte |

An index is valid only if its size is exact, the magic matches, the CRC matches, and no slot is below `-1`.

**Journal** (`JournalFormat.scala`):

| Part | Layout | Size |
|---|---|---|
| Header | magic `FSIFJRNL`(8), seq(8), salt(8), CRC(4) over the first 24 bytes | 28 |
| Batch | len(4), batchNo(8), records..., crc(4) | 16 + records |
| Record | len(4), type(1), payload; len counts type + payload | |
| Mapping (type 1) | ext(4), slot(4), ObjectId msb(8), lsb(8) | 29 |
| Delta (type 2) | ext(4), slot(4), SegmentHeader(61), rangeCount(4), then per range offset(4), length(4), bytes | 78 + Σ(8 + length) |

- A batch's `len` counts `batchNo` plus the records. Its `crc` covers the salt (8 bytes, **not stored in the
  batch**) followed by `len`, `batchNo` and the records. The salt is a fresh `SecureRandom` long, written into the
  header each time the header is rewritten.
- Batch numbers start at 0 in each journal and rise by one per batch.
- Delta ranges hold absolute bytes of the new slice, never XOR deltas, so replaying a Delta twice is harmless.
  Parsing rejects ranges that fall outside `sliceLength`, negative lengths, unknown types and trailing bytes.
- **A header is rewritten** (`FsifRecovery.writeJournalHeader`) as truncate(0), force, write header, force. The
  first force means an invalid header on a file longer than 28 bytes can never be a crash artifact.

**`fsif-initialized`** is written durably (`DurableFiles.write`) after `FsifRecovery.initialize` has finished. A
fresh store is: index.a emptied first if present, index.b and journal.b emptied if present, data emptied,
journal.a given header seq 0, and index.a written last with seq 0 and an empty map. So a valid index implies the
rest of the store exists. An `initialize` that fails (ENOSPC, say) truncates every file back to empty before it
rethrows, best effort, so the retry finds a blank directory (2.13).

### 2.4 Threading model

- **One `SerialExecutor` per backend** owns all mutable state: index map, slot states, journal writer,
  checkpoint, compaction and freeze. Every interface method only posts to it (`FsifBackend.post`). Every posted
  task runs inside `guarded`, which catches internal errors (they fail-stop the store) and then runs the epilogue:
  `advanceCheckpoint`, `maybeCheckpoint`, `compaction.drive`, `releaseThrottled`, `maybeForceData`, and
  `checkQuiesce`. The executor is lock-free and has no thread of its own (`SerialExecutor.scala`). Each drain runs
  at most 64 tasks before it yields.
- **I/O runs on `io`.** On a host this is `FsifBackend.sharedIo`, `max(4, cpus)` daemon threads named
  `fsif-io-N`. Results are posted back to the serial executor. A store on `sharedIo` drains its serial executor on
  `sharedSerial`, `max(2, cpus)` threads named `fsif-serial-N`, so a device hung in fsync ties up io threads and
  not every store's serial work. Tests pass one deterministic EC for both.
- **ENOSPC retries** are scheduled on `SharedRetryScheduler` (one `fsif-retry` thread).
- **Recovery runs synchronously in the constructor.** The host therefore runs the open or create on
  `Host.FsifOpenEc` (cached `fsif-open-N` threads), never on its event thread (`Host.startFsifOpen`).
- **Exceptions to "post and return":** `rebuildWrite` writes the segment on the calling thread and uses
  `onSerialSync` (`Await`) for its bookkeeping. `rebuildFlush` blocks the calling thread as well. Compaction's
  truncate of free tail slots runs inline on the serial executor (section 5).

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
occupant (Ruling 18).

**Journaled** (`journaled`, `appendDelta`).
- **Update of the current occupant:** append `Delta(X, slot, header, ranges)`. The ranges come from `rangesOf`:
  `Full` gives the whole slice, `MetadataOnly` gives none, and `Ranges` gives the spans. Spans that do not fit
  fall back to the whole slice, with a warning. So do spans whose encoding (8 bytes each plus their bytes) would be
  larger than the whole slice's, so a Delta is never larger than `appendBound` allows (2.9).
- **New occupant:** X gets the lowest free slot, or keeps its slot when it overwrites a missed delete. Then
  `Mapping(X, slot, id)` and a full Delta are appended in **one** call, so they go into one batch.
- The header, including the CRC over the full new slice, is computed at commit time. Applying the Delta needs
  only the journaled bytes.
- The commit completes when the batch is forced (`JournalWriter`: one batch in flight, later appends buffered
  into the next batch, one write plus one force per batch). Only then is the `PendingDelta` marked `durable` and
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

Reads never wait for in-place writes, frozen or not. A read I/O failure gives `CorruptedObject` and does **not**
fail-stop the store.

### 2.7 Checkpoints

A checkpoint is triggered by `maybeCheckpoint` when the active journal exceeds
`max(indexFileSize / 2, journalBound)`, or when an operation is held by the journal hard limit and the active
journal holds records (2.9). It can also be started by `rebuildFlush` or the test hook `checkpointNow`. Only one runs at a time, and none starts while frozen or failed. It moves from seq s to s+1
(`startCheckpoint`, `advanceCheckpoint`, `writeNextHeader`, `finish`):

1. **Draining.** Snapshot the index as of the last record appended to journal s. `appendSeq` becomes s+1, and new
   records wait in `switchBuffer`.
2. **Header.** Once the last journal-s append is durable and the writer is idle, write and force header s+1
   (`writeJournalHeader`) into the other journal file. Only then does a new `JournalWriter` take the buffered
   records, in order.
3. **Applying.** Wait until no journal-s Delta is unapplied (`unappliedBySeq`). "Applied" means forced in place,
   that is, covered by a successful data force, or cancelled.
4. **Barrier.** Wait until everything appended so far is durable (`lastAppend`). A cancelled journal-s Delta may
   still be mapped in the snapshot, and the record that cancelled it is in journal s+1.
5. **Finishing.** Write index s+1 into the other index file and force it. Then truncate and force journal s, then
   index s.

Each I/O step runs through `checkpointIo`. ENOSPC holds the step and re-runs it from its start. Any other
failure is fail-stop. While frozen the checkpoint pauses where it stands.

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
I/O has settled. On ENOSPC, writes and forces into f are held and re-run. A data force failure re-queues every
write into f that no good force has covered (`dataLost`).

### 2.9 Backpressure and the journal hard limit

A commit that arrives while `unappliedBytes > unappliedBound` is journaled normally, but its successful reply is
held in `throttled` until unapplied bytes drop to the bound or below (`ack`, `releaseThrottled`). Error replies
are never held.

**Journal hard limit** (`admitted`, `journalHasRoom`, `releaseCapHeld`). The active journal never grows past
`journalHardLimit`, whether or not the store is frozen, so recovery can always load it (L2):
- Every commit, repair, repairDelete and `rebuildFlush` chunk (at most `RebuildMappingsPerAppend` = 4096
  Mappings) is admitted before it touches any state, with an upper bound on the bytes it can append:
  `appendBound(sliceSize)` (a batch of a Mapping plus a one-range Delta of the whole slice), or one Mapping batch
  (`MappingAppendSize`, 45 bytes) for a repairDelete. Copy-on-write and delete commits are covered by the same
  bound.
- It is admitted if nothing is held before it and `activeJournalLength + capOutstanding + MappingAppendSize +
  bound <= journalHardLimit`. `activeJournalLength` is the writer's length (durable, in flight and buffered), or,
  during a switch, the header plus the switch buffer. `capOutstanding` sums the bounds of admitted operations that
  have not yet replied. An otherwise empty journal (header only, nothing outstanding) admits any one operation.
- Otherwise it waits in `capHeld`, in arrival order, neither completed nor failed, and `maybeCheckpoint` starts a
  checkpoint (never while frozen). Held operations are admitted once the switch makes room. Reads go on and do not
  see them.
- A copy-on-write's Mapping is covered by its commit's bound. A compaction move's Mapping is covered by the
  `MappingAppendSize` headroom: one move at a time, started only while that headroom is free
  (`journalHasRoomForMove`).
- Fail-stop fails every held operation with `IOFailure`. A close of a frozen store abandons them as it abandons
  held I/O (2.10).

So no journal the backend writes is longer than `journalLengthBound = max(journalHardLimit, 28 +
appendBound(segmentPayloadSize) + 45)`, which `FsifLayout.MaxSegmentPayloadSize` keeps within
`FsifRecovery.MaxLoadSize`. With the defaults it is 1 GiB.

**Why the limit cannot deadlock a checkpoint.** A held operation has mutated nothing (no index, slot or pending
Delta change) and appended nothing, and no admitted operation waits for one. Every checkpoint condition looks only
at admitted records: Draining waits for `oldTail`, the last append to journal s; Applying waits for journal-s
Deltas; Barrier waits for `lastAppend`, the last admitted append. Writing header s+1 is not an append. So the switch
always proceeds, and afterwards the active journal holds only the header and the switch buffer, which was itself
admitted under the limit. Admitted operations reply independently of held ones, so `capOutstanding` drains; the
empty-journal rule then admits any head, however large. While frozen, nothing starts a checkpoint, so held
operations wait for space exactly as held I/O does.

### 2.10 ENOSPC freeze

`FsifIo.isOutOfSpace` matches "no space left", "disk full", "insufficient space", "disk quota exceeded" or
"edquot" anywhere in the cause chain. Each I/O source that fails this way calls `hold(key, kind, retry, abort)`.
The sources are: journal batch, in-place write, data force, checkpoint step, copy-on-write, move write or force,
and truncate.

While anything is held, the store is **frozen**:
- In-place writes, checkpoints, compaction moves and truncation start nothing new, except their own held retries.
- Commits are still accepted, and their batches buffer behind a held one. A commit whose bytes are not durable is
  held, not failed, so `Tx` keeps its data in the CRL. Checkpoints do not start, so once the active journal reaches
  `journalHardLimit` new commits are held before they run (2.9) until space returns.
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

**Closing a frozen store** (`close`, `abandonHeld`, Ruling 27). Held I/O is abandoned exactly as a crash would
abandon it, and the close waits only for I/O already in flight. Abandoned commits never complete. That is safe:
- held commits were never acknowledged, and Tx keeps them in the CRL;
- acknowledged Deltas are in the journal and replay at recovery;
- held checkpoint, move and copy-on-write steps re-run, or are moot, at recovery.

### 2.11 Fail-stop

`failStop(t)` runs at most once:
- sets `fatal`;
- fails every journal append (`JournalWriter.failAll`), every buffered switch record, every held operation's
  abort, and every throttled reply, each with `IOFailure`;
- calls `onFatal` once.

Afterwards commits complete with `IOFailure`, reads with `CorruptedObject`, rebuild entry points throw, and
`hasFailed` is true. The full list of sources is in section 4.2.

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
  (`throwIfFrozen`, `freezeWaiters`). They also throw after `rebuildLost` (Ruling 21). `RebuildingStore` latches
  that as `REBUILD OUT OF SPACE`. It never waits for space.

### 2.13 Host integration

- **Open and create** (`Host.startFsifOpen`, `fsifOpened`) run on `fsifOpenEc`. While an open runs, the store is
  in its device's `loadedStores` and in `fsifOpening`. A `ShutdownStore` that arrives meanwhile is deferred and
  replayed after the store is served.
- **Outcomes** (`fsifOpened`):

  | Outcome | Host action |
  |---|---|
  | success | `serveOpened` and `LoadStore` |
  | `StoreLost` | mark `rebuild-required`, leave the store offline, `checkStorageDevice` |
  | out of space | leave offline; `storesAwaitingSpace` retries every device scan with exponential backoff capped at `SpaceRetryCap` (5 min). A failed create is handed to the same retries (no marker, and the failed `initialize` emptied its files, so the retry initializes again) |
  | any other failure | logged; the store is not loaded and not marked |

- **`fsif-initialized`** (Ruling 23, revised). A marked store is always recovered. For a directory without the marker,
  `FsifBackend.openUnmarked` initializes it only if every FSIF file is absent or zero-length
  (`FsifRecovery.isBlank`), which keeps creation re-runnable. Otherwise it recovers the store, writes the marker
  durably and serves it, with a warning; a `StoreLost` propagates and is marked `rebuild-required` like any other.
  A crash part way through a fresh store's `initialize` therefore opens as the empty store or as `StoreLost` (4.6).
- **onFatal** posts `BackendFailed`. The routes are listed in section 4.2.
- **Frozen close.** `ShutdownStore` logs and closes without waiting for space.
- **`estimateSize`** is the sum of the data, journal and index file sizes.

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
  - `JournalWriter.completed` completes a batch's promises only after its write and force succeed.
  - The in-place write is never required for durability, because the journal holds the bytes.
  - `kick` writes only `durable` Deltas, so a slot is never changed ahead of its journal record.
- **Tests:** `JournalWriterSuite` "every acknowledged record survives crash" and "random faults, retries and
  crashes never lose an acknowledged record or corrupt the journal"; `FsifBackendSuite` "100 appends of 37 bytes
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
  - Everything goes through `appendRecords`, which preserves order across the checkpoint switch via
    `switchBuffer`.
  - `withExt` and `gated` hold every operation on X behind a copy-on-write until its Mapping is appended
    (`releaseExt`).
  - A journaled allocation that must wait for a tail copy-on-write gates X the same way (`journaled`,
    `afterTailCows`).
- **Tests:** `FsifBackendSuite` "a small write right behind a copy-on-write survives a crash once acknowledged
  (Ruling 14)" and "two commits to one object issued back to back leave the second's data"; `FsifCheckpointSuite`
  "Ruling 19: a commit arriving while journal s is still being forced is not acknowledged before it".

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
  D0 is the forced data size before replay (`FsifRecovery.recover` step 2).
- **Why:** a CRC-valid but bogus slot number must not make recovery grow `data` enormously. The bound holds
  because each Mapping can grow the slot count by at most one.
- **Tail copy-on-write exception:** a copy-on-write that grew the slot count is invisible to every term of the
  bound until it is forced. So a journaled allocation above an unforced `tailCows` slot waits for it
  (`afterTailCows`, `tailCowDone`). Without that wait, a crash would put a legitimate slot above the bound and
  declare the store lost.
- **Tests:** `FsifBackendSuite` "allocations above in-flight copy-on-write tail slots never make recovery lose the
  store"; `FsifRecoverySuite` "a Mapping beyond the reachable slot bound -> StoreLost" and "a chain of allocations
  each taking the next new top slot is accepted".

### 3.7 Checkpoint switch and Barrier
- **Why:**
  - Recovery replays journal s+1 only on top of a complete journal s. A durable s+1 record with an incomplete s
    behind it would replay a suffix without its prefix.
  - Index s+1 must not be written while it maps a slot whose cancelling record (in s+1) is not yet durable.
    Otherwise a crash would pair index s+1 with a journal that lacks that record.
- **Where:**
  - `advanceCheckpoint` `Draining` requires `oldTail.isCompleted && journal.idle` before `writeNextHeader`.
  - The new writer exists only after header s+1 is forced (Ruling 19).
  - `Applying` requires `unappliedBySeq` to have no seq below s+1.
  - `Barrier` waits for `lastAppend`.
- **Tests:** `FsifCheckpointSuite` "Ruling 19: ...", "a journal-s Delta cancelled by a later delete holds the
  checkpoint until that delete is durable", "a crash at any file op during a checkpoint with commits in flight
  keeps every acknowledged write", and "two back-to-back checkpoints (a to b to a) read back correctly after a
  crash".

### 3.8 Force coverage and fsyncgate
- **Rule:** a Delta is resolved only by a successful data force that started after its write completed. The tag is
  `nextForceId` at completion, `forcedUpTo(id)` resolves tags `<= id`, and forces are serialized.
- **ENOSPC on a force:** every uncovered write is re-issued (2.10).
- **Other force failures:** fail-stop.
- **Why:** after a failed fsync the kernel may report later forces as successful although the pages never reached
  disk. Emptying journal s on the strength of such a force would lose acknowledged Deltas.
- **Tests:** `FsifFaultSuite`:
  - "ENOSPC on the data force of a checkpoint's step 2 re-issues the in-place writes before the journal is
    emptied";
  - "ENOSPC on a copy-on-write's data force re-issues the segment and another object's unforced in-place write";
  - "ENOSPC on a compaction move's data force re-issues its copy into f before Map(X, f)";
  - "after a data force fails with a rebuild write unmapped, that incarnation refuses further rebuild work".

  Also `SimulatedFsifFilesSuite` "a faulted force discards the pending ops: ..." and the model suite's
  `dataForceFaults` coverage floor.

### 3.9 Torn tail versus corruption, with the salt
- **Rule** (`JournalFormat.scan`):
  - Batches are parsed in order while each is CRC-valid with the expected number.
  - At the first invalid position, `findLater` checks **every** byte offset after it for a CRC-valid batch numbered
    n+1, with no extent skip, because a rotted `len` could otherwise hide later batches.
  - If one is found, the journal is `Corrupt`. Otherwise the remainder is a torn tail, which recovery truncates
    away.
  - A CRC-valid batch whose records do not parse is `Corrupt`.
- **Why the salt:** batch CRCs include the per-header random salt, which is not stored in the batch. User data
  inside a torn batch, such as a slice that happens to contain an encoded batch copied from another journal or
  crafted by a client, cannot pass as a later batch, so it cannot make a torn tail look like corruption.
- **Tests:** `JournalFormatSuite` "truncation at every offset inside the last batch", "flip in a middle batch is
  corrupt, in the last batch is a torn tail", "torn batch embedding a later batch (different salt) in user data
  stays a torn tail", "mid-journal batch with a rotted len (larger, in bounds) is corrupt", and "scan never throws
  on random mutations"; `FsifRecoverySuite` "a torn tail is truncated to the valid length" and "corruption in the
  middle of the active journal -> StoreLost".

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
  and "repair at an ext held by a different object fails with the mismatch".

### 3.11 Idempotent recovery
- **Rule:** every step of `FsifRecovery.recover` falls into one of three kinds:
  - it only reads (step 1: load, which forces each file before reading it; step 2: classify, replay and validate);
  - it rewrites identical bytes (step 3: absolute Deltas, then force; step 4: torn-tail truncate);
  - it moves the store to a state whose own recovery yields the same objects (step 5: finish the interrupted
    checkpoint by writing index m+1 and emptying journal m and index m, or empty stale files).

  A crash inside recovery followed by another recovery therefore yields the same state.
- **Tests:** `FsifRecoverySuite` "recover is idempotent across crashes inside recover (30 seeds)", which crashes at
  every op k and then again at a random op; `FsifModelSuite` "crash during recovery"; `FsifRecoverySuite` "index s
  valid, index s+1 torn, journals s and s+1 recover the union and finish the checkpoint".

Validation order differs slightly from "validate everything before writing anything". The sliceLength,
double-mapping and bound checks run before step 3. The "mapped slot past EOF" check and the slot-count overflow
check run after step 3 has written and forced the kept Deltas, because applied Deltas extend the file. These are
idempotent writes, so a rerun sees the same inputs, but a store declared lost by those two checks has already
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
- **crashAfterOps sweeps.** `crashAfterOps(n)` stops the simulated machine after n more ops, and `crash()` then
  yields the durable image. Sweeps cover:
  - every op of one move (`FsifCompactionSuite` "a crash at every file op of one move ...");
  - random points inside checkpoints and compactions with commits in flight (50 seeds each);
  - every op inside recovery (`FsifRecoverySuite`).
- **Model-based suite** (`FsifModelSuite`):
  - 400 random operations over 16 exts: allocate, small write, append, overwrite, delete, read, checkpoint,
    compaction, ENOSPC windows and crashes;
  - checked against a per-ext history: after each crash, every ext must read as its last acknowledged state or a
    later issued one, other ObjectIds as `ObjectNotFound`, and nothing as `CorruptedObject` unless the store
    fail-stopped;
  - 30 seeds for "random operations match the model across crashes", and 30 for "crash during recovery";
  - coverage floors of at least 3 across the seeds for: crashes inside a checkpoint, inside a move and while
    frozen; mid-checkpoint images; later-index images; data-force faults; and, with interrupted recovery,
    crashes inside recovery and inside recovery's checkpoint completion;
  - at least 1 stale-index image.
- **Determinism.** io is a seeded `ShuffleEc` that runs on the test thread, and ENOSPC retries run on a
  `ManualScheduler`. The first two seeds of each model run are re-run and their digests compared.
- **Host and integration:**
  - `src/test/scala/org/aspen_ddp/aspen/server/HostFsifLoadSuite.scala`: open off the event thread, StoreLost and
    fail-stop marking, the out-of-space backoff, and frozen close;
  - `DeviceShutdownSuite`: fail-stop during shutdown and transfer closes;
  - `DeviceAdoptionSuite`: CrlLost adoption, including a failed-on-close store;
  - `src/test/scala/org/aspen_ddp/aspen/FsifIntegrationSuite.scala`: a RARS pool through the client covering
    appends, ranged writes, restart, and a lost store rebuilt.

---

## 4. Automatic lost store

**The dominant real-world cause of data loss in a system like this is not hardware.** It is a systemic bug that
wrongly fires one of the automatic paths below. A store then decides, from local evidence alone, that its content
cannot be trusted. It marks itself `rebuild-required`, its directory is deleted, and it is rebuilt from its peers.
One store wrongly declared lost costs a rebuild. The same bug firing on many stores at once, for example on every
host after an upgrade, loses the whole pool, because the peers the rebuild needs are the stores being thrown
away. This cannot be solved inside the backend: each store sees only its own files. See the `Backlog.txt` item
"System-level protection against correlated 'lost store' declarations".

Every case below runs without an operator. Line numbers are as of now.

### 4.1 Detected by FSIF recovery (`FsifRecovery.StoreLost`)

All of these are thrown from `FsifBackend`'s constructor through `FsifBackend.open`. The host reaction is the same
for each: `Host.fsifOpened` (Host.scala:1089-1100) logs, leaves the store offline, writes `rebuild-required`
(`markRebuildRequired`, :2366), and calls `checkStorageDevice`. The `rebuild-required` marker is acted on as
described in 4.5. A plain `IOException` from the open is not a loss (4.6).

| # | Trigger | Where (`store/backend/fsif/FsifRecovery.scala:recover` unless noted) | Why it is believed to mean loss | What a false trigger from a systemic bug looks like |
|---|---|---|---|---|
| L1 | Neither index file is valid (size, magic, CRC, or a slot < -1) | :168 | `initialize` writes index.a last and the marker after it, and a checkpoint empties index s only after index s+1 is forced. So a marked store always has one valid index unless something damaged it. An unmarked, non-blank store reaches it after a crash during its own `initialize` (4.6), having never held data | An index size or CRC computation that changes with the code: e.g. a change to `IndexFile.EntrySize` or `sizeFor`, or a `segmentCount` read differently from the pool config. Every store would fail on first open after the upgrade |
| L2 | An index or journal file is larger than `MaxLoadSize` (`Int.MaxValue - 16`) bytes | `load`, :112 | Not reachable from a state the backend produced. The index is at most `IndexFile.sizeFor(MaxSegmentCount)`. The active journal is at most `journalLengthBound` (1 GiB by default), because operations past `journalHardLimit` are held, frozen or not (2.9); a stale journal is one that was active | A bug in admission: a write path that appends without being admitted or covered by headroom, an `appendBound` that undercounts (a new record type or field), or a writer whose batches outgrow their records. The journal would then grow past the limit only on stores that keep taking writes while checkpoints cannot finish, e.g. a long freeze |
| L3 | A journal with a valid scan has seq > m + 1 | :179 | A journal s+1 header is written only while index s is the newest, so s+2 cannot exist | Seq arithmetic off by one in the checkpoint switch or in step 5's finish; a misread of the header seq |
| L4 | A journal is `Scan.Corrupt` and is not a stale journal (a valid header with seq < m). Corrupt means: (a) invalid header on a file longer than 28 bytes; (b) a CRC-valid batch whose records do not parse (`parseRecords` / `parseRecord`, `JournalFormat.scala` :121-169); (c) an invalid batch followed later by a CRC-valid batch numbered n+1 (`findLater`, :200) | :182-185; `JournalFormat.scan` | (a) the header rewrite forces after truncate(0); (b) the batch CRC covers the records; (c) batches are written strictly in sequence, so only the last can be torn, and the salt keeps user data from posing as a batch | (b) is the most dangerous: any change to the record encoding (a new record type, a `SegmentHeader` layout or type-byte change, a stricter parse check) makes **every** CRC-valid journal "corrupt" after an upgrade. (c) would fire on every store if the writer reused batch numbers, or if a retry path re-sent a batch at a different offset without truncating first |
| L5 | No journal has seq m | :189 | Index m is written only after journal m's header is forced, and journal m is emptied only after index m+1 is forced | Step 5 or `finish` emptying the wrong journal; a journal naming scheme that changed |
| L6 | Both journals have seq m | :190 | The header rewrite always targets the non-active journal | A checkpoint that writes header s+1 into the active file |
| L7 | A Mapping or Delta carries an ext outside `[0, segmentCount)` (`Replay.InvalidRecord`) | `Replay.checkExt` (`Replay.scala`:13) via `replay`, :136 | Only a writer bug or corruption past the CRC produces one | A `segmentCount` that shrinks or is read from the wrong source at open: every store of the pool would fail on any record above the new count |
| L8 | A kept Delta has sliceLength > segmentPayloadSize | :196-199 | `update` rejects such slices with `SliceTooLarge` | A `segmentPayloadSize` computed differently after an upgrade (e.g. from `forObjectSize` instead of the stored config) |
| L9 | After replay, two exts map to one slot | :200-205 | Allocation marks the slot used, and freeing waits for the unmap to be durable | A bug in `tryFree` or `assign` that frees or double-assigns a slot; replay selection or Mapping order changed |
| L10 | A kept Delta's slot, or a final mapped slot, is above `max(D0/stride, startTop+1) + mappings` | :206-219 | Lowest-free allocation makes each Mapping grow the slot count by at most one (3.6) | The highest-risk check. An allocator that skips a slot (e.g. a new reservation kind), a new write path that maps a slot not covered by `tailCows`, or a miscalculated bound would fire on every store after an upgrade, typically only on stores that had a crash with the right pattern in flight, which makes it look like scattered hardware loss |
| L11 | After applying the kept Deltas and forcing, a mapped slot starts at or beyond the end of `data` | :230-236 | A mapped slot was either written in place (forced before index m+1) or its Delta was replayed now | A truncation that is too aggressive (compaction's `used` missing a mapped slot) that became durable; a `stride` change |
| L12 | The computed slot count exceeds `Int.MaxValue` | :263-264 | Not reachable with a bounded data file | A `stride` computed as 0 or very small |

Related, for staging only: `RebuildingStore.openFsifStaging` (`rebuild/RebuildingStore.scala`:228) catches
`StoreLost` for the rebuild's staging store. It deletes the staging directory and its checkpoint
(`cleanupStaging`), so the next pass starts over. The live store is already Rebuilding, so this costs only
rebuild progress. A recovery bug of the L1-L12 kind would make every rebuild restart from scratch forever.

### 4.2 Detected at runtime by FSIF fail-stop (`onFatal`)

Every fail-stop calls `onFatal` once. The host's `onFatal` posts `BackendFailed` (Host.scala:1165).

| # | Trigger | Where |
|---|---|---|
| L13 | A journal batch write or force fails with a non-ENOSPC error (including during an ENOSPC retry) | `FsifBackend.journalError`, :635 |
| L14 | An in-place write fails with a non-ENOSPC error | `inPlaceDone`, :895-899 |
| L15 | A data force fails with a non-ENOSPC error | `dataForceDone`, :966-968 |
| L16 | A checkpoint step (open the other journal, header rewrite, index write, truncates) fails with a non-ENOSPC error | `checkpointIo`, :1215-1217 |
| L17 | A copy-on-write segment write or force fails with a non-ENOSPC error | `cowWritten`, :1418-1420 |
| L18 | Compaction: the move's raw read of slot t fails with **any** error; a move write or force fails with a non-ENOSPC error; the truncate of free tail slots fails with a non-ENOSPC error | `Compaction.copied` :152, `written` :221, `forced` :242; truncate via `guarded` :500-504 |
| L19 | Any `NonFatal` exception escapes a serial task, checkpoint advance or compaction drive (an internal error), for example `allocateSlot`'s `IllegalStateException`, a rejected io task, or an NPE | `FsifBackend.guarded`, :484-505 |

**Why these are believed to mean loss.** After a write or fsync error other than ENOSPC, the page cache no longer
says what is on disk, so the store's state cannot be trusted. The recovery a restart would run might well succeed,
but the backend does not assume it.

**What a false trigger looks like.** L19 is the broad one. Any bug that throws inside the serial executor (an
assertion, an index out of range, a new `require`) fail-stops the store. A bug on a common path (every commit,
every checkpoint) fires on every FSIF store on every host running that version. L18 is broader than the
others: a failed compaction read fail-stops the store, while a failed client read only returns
`CorruptedObject`. L13-L17 would fire together if an error classifier misses ENOSPC (for example a filesystem or
JDK that words it differently from `FsifIo.isOutOfSpace`'s strings), which turns every full device into a
correlated loss.

**Host routes for `BackendFailed` and other failed-backend checks.** Each of these writes `rebuild-required`:

- **Served store** (`Host.backendFailed`, :2437-2459, marker at :2449). The store is unloaded, its CRL state is
  dropped (`crl.closeStore`), it is marked and left offline, and the device is checked.
- **Not served, not replaced, directory on a loaded device** (`backendFailed`, :2460-2471).
- **Failed before its `LoadStore` ran** (`LoadStore` handler, :3814-3829, then `retireUnservedStore`, :2389).
- **Opened but not served** (device unloaded or another backend serves it) **and failed** (`fsifOpened`,
  :1076-1083).
- **Failed during a `ShutdownStore` close** (:3928-3932, then `failedOnClose`, :2413-2427). The store is marked,
  and the `crl_save.log` the close just wrote is **deleted**. `closeStoresForShutdown` (:2952-2966) then writes no
  transfer-out marker, so an adopter sees `rebuild-required` and treats the store as CrlLost (L22).
- **Transfer out** (`startStoreTransferOut` region, :1935-1974):
  - a store already marked whose entry is TransferringOut is returned to rebuild (Ruling 25,
    `returnLostTransferToRebuild`, :2001);
  - a close that fails with `StoreFailedOnClose` is never shipped, and its transfer is aborted (:1949-1952);
  - a store marked while it closed is likewise never shipped (:1969-1973).

### 4.3 Host-level, backend-independent paths that apply to FSIF stores

| # | Trigger | Where | What happens | Why believed | False trigger looks like |
|---|---|---|---|---|---|
| L20 | `crl_save.log` exists but `CrashRecoveryLog.loadValidStoreState` rejects it (truncated, unparsable, or naming another store) | `LoadStore` handler, Host.scala:3818-3833, then `crlSaveFileLost` :2379 and `retireUnservedStore` :2389 | Closed unserved, marked, offline, device checked | The file is the only copy of the store's transaction state | A CRL save format change: every store with a save file (every store closed by a shutdown or transfer) is declared lost on its next load. **Generic: all backends** |
| L21 | Adoption of a Shutdown device: an Offline store has a transfer-out marker but a missing or unusable `crl_save.log` | `adoptionClass`, :750-754; marked in `adoptDevice` :2655-2659; entry written Rebuilding in the same transaction (:2661-2663) | Adopted as Rebuilding, marked | Shutdown commits only after every close is forced, so the file was lost afterwards | A save-file read path that fails on the adopting host's version (format, or a path change in `Backend.crlSaveFile`): every store on every moved device. **Generic** |
| L22 | Adoption: an Offline store's directory has `rebuild-required` | `adoptionClass`, :745-748 | CrlLost, so adopted Rebuilding | The previous host already marked it | Inherits whatever falsely marked it on the previous host. **Generic** |

At LoadStore a *missing* `crl_save.log` is not a loss: it loads with no saved state (:3819-3822). A missing file
counts as loss only at adoption (L21), where a transfer-out marker shows that a close completed.

### 4.4 Count

There are 22 automatic cases:
- 12 found by FSIF recovery (L1-L12);
- 7 FSIF fail-stop sources (L13-L19), reaching the marker through the host routes listed in 4.2;
- 3 generic host paths (L20-L22).

In addition, the rebuild staging store is discarded on `StoreLost` (4.1, related).

### 4.5 What happens after the mark

1. `tryLoadStore` (Host.scala:989) never loads a directory with `rebuild-required`. It goes into
   `offlineStores`.
2. The reconcile's Lost CRL State pass (:3073-3081) finds marked offline stores whose entry is Active and calls
   `requestStoreRebuild` (:2483). That flips the entry to Rebuilding in a transaction, but only if the device is
   not failed and is owned by this host.
3. `startStoreRebuild` (:2127) calls `discardLostStore` (:2526) first. It moves the directory to
   `<device>/discarded/<store>-<uuid>`, syncs the device directory, and **deletes it at once**
   (`os.remove.all`). If the delete fails, the copy is removed by `tryLoadDevice` at the next device load, which
   empties `discarded/` (:945-950).
4. `RebuildingStore` rebuilds into `<device>/rebuilding/...` and moves it into place.

So the store's content is gone as soon as the rebuild starts. The Backlog item proposes keeping it instead in
`<storage-device>/suspected-loss/<store-dir>-<uuid>`, together with the reason, time, software version and the
check that fired, and leaving the decision to a system-level check that looks for correlation across stores,
pools and hosts.

### 4.6 Deliberate non-cases

These look like loss but never trigger a rebuild:

- **Out of space at load** (`fsifOpened`, :1104-1123). The store is left offline and retried with backoff. A
  failed create is retried the same way.
- **Out of space at runtime.** ENOSPC or EDQUOT freezes writes (2.10). It is never fail-stop, including during
  retries. Only a non-ENOSPC error during a retry is fail-stop.
- **A missing `fsif-initialized` marker.** `FsifBackend.openUnmarked` (:1724-1742) re-initializes the directory
  only if every FSIF file is absent or zero-length (`FsifRecovery.isBlank`): nothing there can hold an object, so
  nothing is lost. Any other marker-less directory goes through normal recovery. If that succeeds, the marker is
  written durably and the store served, with a warning, so a bug that loses or fails to carry the marker (a
  transfer, adoption or rebuild path that does not carry the file, or a renamed constant) costs nothing. If it
  throws `StoreLost`, that is L1-L12 and the store is marked like any other. A crash part way through a fresh
  store's `initialize` (no marker yet) leaves non-blank files once journal.a's header has been written and until
  index.a is forced; those crash points open as `StoreLost` and the brand-new store, which never held data, is
  rebuilt. The others open as the empty store (`FsifInitializationSuite` crash sweep: 4 of initialize's 10 crash
  points, 100 of 320 seeded crashes, are lost).
- **A torn journal tail.** It is truncated in recovery step 4.
- **A journal with a stale seq** (< m), whether valid or corrupt with a valid header. It is ignored and emptied.
- **One invalid index file** while the other is valid (a torn index s+1 falls back to index s).
- **A CRC mismatch on a read.** `CorruptedObject` goes to the repair path. The store stays served.
- **An `IOException` during open that is not out of space.** It is logged ("Failed to load store"), and the store
  is not loaded and not marked. A restart tries again.
- **A failed `crl.loadStore`** after a valid save file. `UnloadFailedStore` takes the store offline and keeps the
  file. It is not marked.
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
- `DeviceShutdownSuite` "a store whose backend fails during its shutdown close gets no transfer-out marker" and "a
  transfer out whose store fails during its close is never sent and goes back to be rebuilt";
- `DeviceAdoptionSuite` "a store with ... CRL save file is adopted as Rebuilding and rebuilt";
- `HostDeviceDiscoverySuite` truncated-CRL tests;
- `StoreRebuildTriggerSuite` "an Active store marked rebuild-required is flipped to Rebuilding, discarded and
  rebuilt";
- `FsifIntegrationSuite` "an FSIF store whose recovery finds it lost is rebuilt through the rebuild path".

Tests for the non-cases: `FsifRecoverySuite` "a non-empty journal with seq < m is stale and ignored" and "an
invalid header no longer than a header is an empty journal"; `FsifInitializationSuite` "without its marker, a store
holding objects is recovered, not re-initialized, and gains the marker", "without its marker, a directory whose
FSIF files are all absent or empty is initialized", "a fully initialized store without its marker is recovered as
the empty store, not re-initialized", "without its marker, a directory with non-empty files and no valid index is
lost and left untouched" and "a crash at any op of initialize on a fresh directory opens as the empty store or
StoreLost, nothing else"; `HostFsifLoadSuite` "an FSIF store without its marker whose files hold no valid index is
marked for rebuild, not emptied"; `FsifFaultSuite` "with data writes frozen, commits past journalHardLimit are
held, not failed, and complete once it clears" (L2 cannot be reached).

---

## 5. Known limitations and open items

- **Index and journal files are read whole during recovery.** `FsifRecovery.load` reads each into one array and
  throws `StoreLost` above `MaxLoadSize` (`Int.MaxValue - 16`) bytes. Both stay below it by construction: the index
  through `MaxSegmentCount`, the journal through `journalHardLimit` (2.9). The cost moves elsewhere: a store frozen
  long enough to fill its journal to the limit (1 GiB) stops acknowledging commits until space returns, and
  recovery of such a journal reads and replays up to 1 GiB.
- **The hard limit counts conservatively.** An admitted operation's bound counts until it replies, even once its
  records are in the journal, and a commit's bound assumes a Mapping plus the whole slice. Commits can therefore be
  held while the journal is still well below the limit, most while a frozen journal batch keeps commits from
  replying. Operations held only by outstanding copy-on-writes can make checkpoints run back to back without
  admitting anything until those finish.
- **Truncate runs inline on the serial executor** (`Compaction.truncate`). It is the one blocking file call made
  there, so a hung device can stall that store's serial work during a truncate.
- **A moving object's Map can livelock.** `Compaction.pump` appends `Map(X, f)` only if no write into f started
  after the last force began. A continuous stream of commits to X keeps `dirty` set, so the move never completes.
  It still holds f reserved and keeps t from being truncated. Nothing breaks: commits to X are acknowledged
  normally, and a copy-on-write, delete or re-allocation cancels the move.
- **`bringIntoService` gossips early.** `Host.bringIntoService` (:870-872) gossips `StoreMoved`
  (`gossipStoreOnline`) for every Active store right after `tryLoadDevice`. For FSIF stores the open is still
  running on `fsifOpenEc`, and it may yet find the store lost or out of space. The reinstated-transfer path waits
  for `onServed`, but this restart and adoption path does not.
- **Cross-pool transactions always abort** (pre-existing; `Todo.txt` "Transactions spanning objects in more than
  one StoragePool always abort"). `FsifIntegrationSuite` relies on the allocation tree rather than linking its
  objects from the radicle.
- **StoreRepairer leaves errorTree entries behind** after a successful repair (pre-existing; `Backlog.txt`).
  Found during the FSIF integration test.
- **create-pool is gated.** `cmdline/Main.scala` has `FsifPoolsEnabled = false` until an allocation strategy
  produces 4-byte storePointers (`Todo.txt`: FixedSizedIndexFileAllocationStrategy).
- **Correlated lost-store declarations** (section 4) need system-level protection (`Backlog.txt`). A missing marker
  is no longer destructive (4.6), but a crash during a fresh store's `initialize` can still declare that new store
  lost.
