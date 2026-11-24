# FSIF Device Journal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace each FSIF store's `journal.a`/`journal.b` with one preallocated journal per storage device, shared
by its FSIF stores and written by one thread, so FSIF throughput no longer needs a data thread per store.

**Architecture:** A new device-level component, `FsifDeviceJournal`, owns a ring of zero-filled files under
`<device>/fsif-journal/` and batches every attached backend's records into one DSYNC write. Backends attach at open:
they write an index naming the journal, their attachment id and a covered serial. They detach at close. At device
load, a lane job replays every attached directory's records into its own files before any store opens. A lost journal
moves every FSIF copy to `lost/`, following the lost-CRL procedure.

**Tech Stack:** Scala 3, java.nio `FileChannel` (DSYNC), os-lib, ScalaTest, the existing `FsifFiles`/`SimulatedFsifFiles`
abstractions.

**Spec:** `docs/superpowers/specs/2026-11-24-fsif-device-journal-design.md`. Read it before any task. Section
numbers below (§4.5 etc.) are the spec's.

## Global Constraints

- Paths: main code under `src/main/scala/org/aspen_ddp/aspen/server/`, tests under
  `src/test/scala/org/aspen_ddp/aspen/server/`. New FSIF files go in `store/backend/fsif/`.
- Persisted compatibility: none (old FSIF stores need not open).
- Journal files are zero-filled with real writes and forced. Never `fallocate` / `server.FilePreallocation` (§4.1).
- Defaults: 4 files of 256 MiB. Minimum 3 files. The file size is a multiple of 4096. The header is 4096 bytes.
  Batches are padded to 4096 bytes and never span files.
- Device config keys: `fsif-journal-files` and `fsif-journal-file-size-mb`. They apply only when a journal is
  created; an existing journal opens with its recorded `sizing`.
- Batch CRC covers the journal id and the file's pass salt (neither stored in the batch), then length, serial and
  records.
- Disk work under `host/`, `rebuild/` and `transfer/` runs only as `DeviceIo` lane jobs (`DeviceIoUsageSuite`).
  Existence tests use `server.FileStat`. Filesystem work uses os-lib except DSYNC channels.
- No hot-path cost for rare cases (memory): release, loss and refusal logic stays off the per-commit path.
- Tests: no flakiness. Full runs: `timeout 80 sbt test` under normal load. Single suite:
  `sbt 'testOnly *Suite -- -z "substring"'`.
- Commit on main after each task (memory: commit-to-main), ending the message with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Correction to spec §4.5 (found while planning): the "file size must exceed `appendBound(MaxSegmentPayloadSize)`"
  rule would need 2 GiB files. Instead a store's layout is checked against the journal at create and open:
  `appendBound(layout.segmentPayloadSize) + MappingAppendSize <= fileSize - HeaderSize`. A create fails with
  `IllegalArgumentException`. An open fails with an `IOException` naming both sizes, which is a load retry and never
  a loss.

## Review Focus

1. **A crash after a store's attach index and before its first append.** The index names an attachment with no
   records. The next device load must replay nothing, write a detached index, and declare nothing lost (test in
   Task 6).
2. **A device whose only FSIF copies were transferred in.** Their indexes name a foreign journal and there is no
   local journal. The load must be Absent, not J2 lost (test in Task 10).
3. **A store too large for the device's journal file size.** The create fails cleanly. An open logs an error and
   retries without declaring a loss (test in Task 9).
4. **An idle store whose covered serial is laps behind.** After the ring wraps several times around it, a reload must
   not see a false gap (test in Task 6).
5. **A fail-stopped copy whose move to `lost/` fails.** The ring stays pinned and commits on the device hold. Once
   the device check's retry moves it durably, commits resume (test in Task 11).

---

### Task 1: Device journal format

**Files:**
- Create: `store/backend/fsif/DeviceJournalFormat.scala`
- Test: `store/backend/fsif/DeviceJournalFormatSuite.scala`

**Interfaces:**
- Produces:
  - `final case class JournalSizing(journalId: UUID, files: Int, fileSize: Long)`, with `require` for files >= 3,
    fileSize % 4096 == 0 and fileSize > HeaderSize.
  - `final case class FileHeader(journalId: UUID, salt: Long, first: Long, oldestNeeded: Long)`.
  - `final case class TaggedRecord(attachment: UUID, record: JournalRecord)`.
  - `final case class Batch(serial: Long, records: Vector[TaggedRecord])`.
  - `object DeviceJournalFormat` with:
    - `val HeaderSize = 4096`
    - `def encodeSizing(s: JournalSizing): Array[Byte]`
    - `def decodeSizing(b: Array[Byte]): Option[JournalSizing]`
    - `def encodeHeader(h: FileHeader): Array[Byte]` (exactly HeaderSize bytes)
    - `def decodeHeader(b: Array[Byte], journalId: UUID): Option[FileHeader]`
    - `def encodeBatch(journalId: UUID, salt: Long, b: Batch): Array[Byte]` (padded to 4096)
    - `def decodeBatchAt(file: Array[Byte], pos: Int, journalId: UUID, salt: Long): BatchRead`, where
      `enum BatchRead { case Ok(batch: Batch, next: Int); case Invalid; case Unparseable(reason: String) }`
    - `def batchSize(records: Seq[TaggedRecord]): Int` (padded)
    - `def taggedRecordSize(r: JournalRecord): Int` (`JournalFormat.recordSize` plus 16 bytes)
- Consumes: `JournalRecord`, `JournalFormat.recordSize` and the record codec (make `encodeRecord`/`parseRecord`
  `private[fsif]` and reuse them).

- [ ] **Step 1: Write failing tests** in `DeviceJournalFormatSuite`:
  - `"sizing round-trips and rejects a flipped byte"`: decode(encode(s)) == Some(s); any single flipped byte → None.
  - `"a header round-trips only under its own journal id"`.
  - `"a batch round-trips at a 4096 boundary"`: encodeBatch length % 4096 == 0, and decodeBatchAt returns `Ok`
    with equal serial and records, `next` == pos + length.
  - `"a batch is Invalid under another salt or journal id"`.
  - `"truncation at every byte of a batch is Invalid, never Unparseable"`.
  - `"a CRC-valid batch with an unknown record type is Unparseable"` (build the bytes by hand, CRC included).
  - `"all-zero bytes are Invalid"`.
- [ ] **Step 2:** Run `sbt 'testOnly *DeviceJournalFormatSuite'`. Expected: compile failure.
- [ ] **Step 3:** Implement. The batch layout is length(4), serial(8), record count(4), records (each
  attachment(16) plus today's record encoding), crc(4), zero padding to 4096. The CRC is CRC32C over journalId(16),
  salt(8), then the bytes from length through the last record.
- [ ] **Step 4:** Run the suite. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF device journal: format`.

### Task 2: Ring scan (recovery read side)

**Files:**
- Create: `store/backend/fsif/DeviceJournalScan.scala`
- Test: `store/backend/fsif/DeviceJournalScanSuite.scala`

**Interfaces:**
- Consumes: Task 1.
- Produces:
  - `object DeviceJournalScan`:
    `def scan(files: FsifFiles, sizing: JournalSizing): Scan`
  - `enum Scan`:
    - `case Empty` (no valid header anywhere)
    - `case Valid(batches: Vector[Batch], tail: Tail)` with
      `final case class Tail(file: Int, nextPos: Long, nextSerial: Long, salt: Long)`
    - `case Corrupt(reason: String)`
  - File names: `DeviceJournalScan.fileName(n: Int): String = s"$n.fj"`.

Rules (§4.1 "What a file holds", §4.7 step 1, exactly):
- Read every file header. A file whose header is invalid holds nothing.
- Order valid headers by `first`. The newest is the greatest `first`. The needed range runs from the newest header's
  `oldestNeeded` to the tail.
- In each non-newest file, the batches from `first` to the next file's `first - 1` must all be `Ok`. A missing,
  `Invalid` or `Unparseable` batch is `Corrupt`, but only when its serial is within the needed range. Batches below
  it are not read.
- In the newest file, the first non-`Ok` position ends the scan. It is a torn tail when no `Ok` batch with the next
  serial exists at any later 4096 boundary of that file. Otherwise it is `Corrupt`. An `Unparseable` batch is always
  `Corrupt`.
- `oldestNeeded` below the oldest valid `first` (a rotted or overwritten file held needed batches) is `Corrupt`.
- `batches` holds only those at or above `oldestNeeded`.

- [ ] **Step 1: Write failing tests** with `SimulatedFsifFiles` and a test writer helper
  `writeRing(files, sizing, plan: Seq[(fileIdx, first, oldestNeeded, batches)])`:
  - `"empty ring is Empty"`.
  - `"one file, three batches, Valid with tail after the third"`.
  - `"clean switch: the batches end before the next file's first, Valid"`.
  - `"torn last batch in the newest file is a torn tail"`.
  - `"a valid later batch with the next serial after an invalid one is Corrupt"`.
  - `"a torn header rewrite of the file being entered is ignored"` (the newest file's header is zero bytes, the
    previous file is intact: Valid, tail in the previous file).
  - `"a rotted header of a file inside the needed range is Corrupt"`.
  - `"a rotted header of a file wholly below oldestNeeded is ignored"`.
  - `"stale batches of an earlier pass (old salt) after the current pass's last batch end the file, not Corrupt"`.
  - `"an Unparseable batch in the needed range is Corrupt"`.
- [ ] **Step 2:** Run. Expected: compile failure.
- [ ] **Step 3:** Implement `scan`. Load each file through `FsifFiles.open` and force it before reading, as
  `FsifRecovery.load` does.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF device journal: ring scan`.

### Task 3: Journal directory: create, open, lock, sizing

**Files:**
- Create: `store/backend/fsif/DeviceJournalFiles.scala`
- Modify: `StorageDeviceConfig.scala` (add `fsifJournalFiles: Option[Int]` and `fsifJournalFileSizeMb: Option[Int]`,
  keys `fsif-journal-files` and `fsif-journal-file-size-mb`, in `yamlConfig`, `attrs`, `create` and the comment
  block)
- Test: `store/backend/fsif/DeviceJournalFilesSuite.scala`, and the `StorageDeviceConfig` round-trip test where it
  exists today (`grep -rn "crl-num-streams" src/test`)

**Interfaces:**
- Produces:
  - `object DeviceJournalFiles`:
    - `val DirName = "fsif-journal"`
    - `val CreatingName = "fsif-journal.creating"`
    - `def lostName(id: UUID) = s"fsif-journal.lost-$id"`
    - `def sizingFor(cfg: StorageDeviceConfig): JournalSizing` (defaults 4 × 256 MiB, fresh random id; throws
      `JournalConfigInvalid` for files < 3, or a size not giving a 4096 multiple)
    - `def create(dir: os.Path, sizing: JournalSizing): Unit`: writes `sizing` durably, then each `<n>.fj` as
      `fileSize` zero bytes in 1 MiB writes, forced, then forces the directory. Used only on an empty scratch
      directory.
    - `def readSizing(dir: os.Path): JournalSizing`: throws `JournalSizingMismatch` when it is unreadable, or when
      the files present do not match it in count or size.
    - `def exists(dir: os.Path): Boolean` (FileStat)
  - Exceptions in the same file: `class JournalConfigInvalid(msg) extends Exception`,
    `class JournalSizingMismatch(msg) extends java.io.IOException`, `class JournalLocked(msg) extends Exception`.
  - Locking: reuse `crl.simple.CrlLock.acquire(dir)` (it locks `<dir>/lock`, and the registry is keyed by file
    identity, so it is safe for a second directory). Map `CrlLocked` to `JournalLocked` at the call site in Task 10.

- [ ] **Step 1: Write failing tests:**
  - `"create writes count files of exactly fileSize zero bytes and a readable sizing"`.
  - `"readSizing refuses a missing file, an extra file and a short file"` (`JournalSizingMismatch`).
  - `"sizingFor uses the defaults and the config keys; refuses 2 files"`.
  - `"StorageDeviceConfig round-trips the two new keys"`.
- [ ] **Step 2:** Run. Expected: compile failure.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF device journal: directory creation and sizing`.

### Task 4: `FsifDeviceJournal` runtime

**Files:**
- Create: `store/backend/fsif/FsifDeviceJournal.scala`
- Test: `store/backend/fsif/FsifDeviceJournalSuite.scala`

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces:

```scala
/** `run` executes one blocking journal write task: production passes the DeviceIo's journal thread (Task 9), tests a
 *  ManualEc or a direct runner. `files` holds `<n>.fj`; `start` comes from DeviceJournalScan (Valid's tail, or
 *  file 0 at serial 0 for a fresh ring) */
final class FsifDeviceJournal(files: FsifFiles, val sizing: JournalSizing, start: DeviceJournalScan.Tail,
                              run: Runnable => Unit):
  def attach(attachment: UUID, directory: os.Path, owner: FsifDeviceJournal.Owner): Unit
  /** Serial of the last batch written (the tail), for an attach index */
  def tailSerial: Long
  /** Any thread. Reserves room for `bytes` of tagged records, or returns false; `Owner.roomAvailable` is called
   *  once room returns after a refusal */
  def tryReserve(attachment: UUID, bytes: Long): Boolean
  def unreserve(attachment: UUID, bytes: Long): Unit
  /** Any thread. Completes with the batch serial once written; completion callbacks run through
   *  Owner.complete (the store's serial executor) */
  def append(attachment: UUID, records: List[JournalRecord]): Future[Long]
  /** The attachment's records up to `serial` are covered by its index (a finished checkpoint) */
  def covered(attachment: UUID, serial: Long): Unit
  /** Detached (index written, detached flag) or released by the host: it pins nothing more */
  def release(attachment: UUID): Unit
  def releaseDirectory(directory: os.Path): Unit
  def failed: Option[Throwable]
  def close(): Unit

object FsifDeviceJournal:
  trait Owner:
    def complete(task: Runnable): Unit
    def roomAvailable(): Unit
    def checkpointRequested(): Unit
    def journalFailed(t: Throwable): Unit
```

Behaviour (§4.3, §4.5, §4.8):
- **One batch in flight.** Appends queued meanwhile form the next batch, up to the room left in the current file;
  the rest waits for the next batch.
- **Entering the next file.** Allowed only when no unreleased attachment has a record above its covered serial in
  that file's previous pass. Before the first batch, rewrite the file's header (fresh salt,
  `first` = next serial, `oldestNeeded` = the minimum oldest-needed over unreleased attachments, else `first`) with a
  DSYNC write.
- **Room** is the space left in the current file plus every free file, less what is reserved. `tryReserve` never
  over-commits. When the next file is not free and the current file is at least 3/4 full, call
  `checkpointRequested` on every owner whose oldest needed serial lies in the oldest pinned file.
- **Writes** are positional writes through `files.open(name)` handles opened once; the production `FsifFiles` opens
  them DSYNC (Task 9). Each write is followed by `force()` when the `FsifFiles` is not DSYNC (the simulated one), so
  crash tests see durability.
- **Failure.** Any write or header failure fails the journal: `failed = Some(t)`, every pending append Future fails
  with it, and every owner's `journalFailed(t)` is called once. Later appends fail at once.

- [ ] **Step 1: Write failing tests** (ManualEc from `JournalWriterSuite`; `SimulatedFsifFiles`; 3 files of 64 KiB):
  - `"appends from two attachments made meanwhile share one batch"`: two appends before the run, one write, both
    Futures complete with the same serial.
  - `"serials rise by one per batch and survive a scan"`: 50 appends, then `DeviceJournalScan.scan` returns them in
    order, tagged.
  - `"the writer enters the next file only when no unreleased attachment needs it"`: attachment A appends into file
    0 and never covers; B fills files 1-2; B's next tryReserve is false until A `covered`, then `roomAvailable` fires.
  - `"an idle attachment pins nothing"`: A attaches, never appends; B wraps the ring 3 times.
  - `"checkpointRequested goes to the owners pinning the oldest file"`.
  - `"a write failure fails every pending append and calls journalFailed once per owner"` (`failNext`).
  - `"crash at every op of 20 appends and 2 file switches scans Valid and holds every completed append"`: crash
    sweep with `crashAfterOps(k)` for k in 0 until opCount.
- [ ] **Step 2:** Run. Expected: compile failure.
- [ ] **Step 3:** Implement. The state is guarded by one lock, and the write task runs through `run`.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF device journal: runtime writer, reservations and attachments`.

### Task 5: Index v2 and the recovery split

**Files:**
- Modify: `store/backend/fsif/IndexFile.scala`, `store/backend/fsif/FsifRecovery.scala`, `Replay.scala` (unchanged
  API), `JournalFormat.scala` (remove the per-store journal header and `scan`; keep the record codec and sizes)
- Delete: `store/backend/fsif/JournalWriter.scala` and `JournalWriterSuite.scala` in Task 7, not here. This task
  leaves `FsifBackend` compiling by keeping the old `recover` until Task 7 removes it.
- Test: `store/backend/fsif/FsifRecoverySuite.scala` (rewritten cases), `FormatsSuite.scala`

**Interfaces:**
- Produces:
  - `final case class IndexMeta(journalId: UUID, attachment: UUID, covered: Long, detached: Boolean)`, where
    `IndexMeta.None` = zero UUIDs, -1, detached = true.
  - `IndexFile.encode(seq: Long, meta: IndexMeta, map: IndexMap): Array[Byte]`
  - `IndexFile.decode(bytes, segmentCount): Option[(Long, IndexMeta, IndexMap)]`, with `sizeFor` grown by 41 bytes.
  - `FsifRecovery.AllFiles = IndexFiles :+ DataFile`.
  - `FsifRecovery.newestIndex(files, layout): (String, Long, IndexMeta, IndexMap)` (L1 `StoreLost` "no valid index
    file").
  - `FsifRecovery.replayDirectory(files: FsifFiles, layout: FsifLayout, records: Vector[JournalRecord], journalId:
    UUID, tailSerial: Long): Unit`. These are today's steps 2, 3 and the L7-L12 checks, with the bound computed as
    today, and the records in place of journals m and m+1. Then it writes index seq+1 into the other index file with
    `IndexMeta(journalId, meta.attachment, tailSerial, detached = true)`, forces it, and empties the old index. It
    throws `StoreLost` as today.
  - `FsifRecovery.openDirectory(files, layout, journalId: UUID): Opened`, where
    `final case class Opened(index: IndexMap, seq: Long, activeIndex: String, slotCount: Int)`. It loads the
    newest index. Not detached and naming `journalId` → `IOException("... not replayed ...")`. It runs L11 and L12
    against `data`'s size.
  - `FsifRecovery.initialize(files, layout)`: empty data, then index.a seq 0 with `IndexMeta.None`, forced.
  - `FsifRecovery.isBlank` over the new `AllFiles`.

- [ ] **Step 1: Rewrite the tests:**
  - `FormatsSuite`: `"index v2 round-trips its meta"`.
  - `FsifRecoverySuite`: port every case that fed journals to recover so it feeds `records` to `replayDirectory`
    (torn-tail cases move to Task 2). Keep:
    - `"a Mapping beyond the reachable slot bound -> StoreLost"`;
    - `"a chain of allocations each taking the next new top slot is accepted"`;
    - `"recover is idempotent across crashes inside recover (30 seeds)"`, now over `replayDirectory`.
  - Add: `"replayDirectory writes a detached index covering tailSerial"`.
  - Add: `"openDirectory refuses an undetached index of its own journal with an IOException"`.
  - Add: `"openDirectory accepts an index naming another journal"`.
- [ ] **Step 2:** Run `sbt 'testOnly *FsifRecoverySuite *FormatsSuite'`. Expected: FAIL / compile failure.
- [ ] **Step 3:** Implement. Keep `recover` as a deprecated shim, used only by `FsifBackend` until Task 7.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF: index names its journal attachment; recovery split into replay and open`.

### Task 6: Device-load recovery over directories

**Files:**
- Create: `store/backend/fsif/DeviceJournalRecovery.scala`
- Test: `store/backend/fsif/DeviceJournalRecoverySuite.scala`
- Test helper: `store/backend/fsif/PrefixedFsifFiles.scala` (test tree): `final class PrefixedFsifFiles(under:
  FsifFiles, prefix: String) extends FsifFiles`. It gives several "directories" on one simulated machine, so one
  `crash()` covers the journal and every store.

**Interfaces:**
- Consumes: Tasks 2, 5.
- Produces:

```scala
object DeviceJournalRecovery:
  final case class Target(key: os.Path, files: FsifFiles, layout: FsifLayout)
  enum Outcome:
    case Replayed(records: Int)   // also 0: nothing above its covered serial, index rewritten detached
    case NotAttached              // newest index detached, or names another journal: untouched
    case Lost(err: FsifRecovery.StoreLost)
  final case class Result(start: DeviceJournalScan.Tail, outcomes: Map[os.Path, Outcome])
  /** @throws JournalCorrupt when the scan is Corrupt; IOExceptions propagate (transient) */
  def recover(journalFiles: FsifFiles, sizing: JournalSizing, targets: Seq[Target]): Result
final class JournalCorrupt(msg: String) extends Exception(msg)
```

- Per target: `newestIndex`. If it is detached or names another journal → `NotAttached`. Otherwise replay that
  attachment's records with a serial above `covered` via `replayDirectory(..., tailSerial = scan tail - 1)`.
  `StoreLost` → `Lost`, and the others continue.
- `Empty` scan with an attached target that has records needed (covered < any serial) cannot happen. An attached
  target over an `Empty` ring is replayed with no records.

- [ ] **Step 1: Write failing tests** (3 stores via `PrefixedFsifFiles` on one `SimulatedFsifFiles`, real
  `FsifDeviceJournal` from Task 4 to produce rings):
  - `"replay applies each attachment's records only to its own directory"` (two directories of the same store
    id, different attachments).
  - `"Review Focus 1: an attach index with no records replays nothing and becomes detached"`.
  - `"Review Focus 4: an idle directory whose covered serial is three laps behind is not Corrupt"`.
  - `"one directory's bad Mapping is Lost, the others Replayed"`.
  - `"a corrupt ring throws JournalCorrupt and writes nothing"`.
  - `"crash at every op of recover, then recover again, yields the same objects"` (20 seeds, model check: each
    directory reads as replayed).
- [ ] **Step 2:** Run. Expected: compile failure.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF device journal: device-load replay`.

### Task 7: `FsifBackend` on the device journal: write path, attach, checkpoints, admission

**Files:**
- Modify: `store/backend/fsif/FsifBackend.scala`, `FsifTuning.scala` (remove `journalBound` and `journalHardLimit`),
  `FsifLayout.scala` (`MaxSegmentPayloadSize` no longer depends on the journal hard limit: `Int.MaxValue - 16 -
  FsifBackend.appendBound(0) - FsifBackend.MappingAppendSize`), `Compaction.scala` (`journalHasRoomForMove` meaning
  only)
- Delete: `JournalWriter.scala`, `JournalWriterSuite.scala`, `FsifRecovery.recover` (the Task 5 shim)
- Test helper: `store/backend/fsif/SharedJournal.scala` (test tree). Given one `SimulatedFsifFiles` machine, it
  builds a `FsifDeviceJournal` under prefix `fj/`, and `open(storeIdx)` runs `DeviceJournalRecovery` and then opens
  `FsifBackend` under prefix `s<idx>/`. `crash()` crashes the machine and rebuilds everything.
- Test: port `FsifBackendSuite`, `FsifCheckpointSuite`, `FsifCompactionSuite`, `FsifInitializationSuite` to
  `SharedJournal`

**Interfaces:**
- Consumes: Tasks 4-6.
- Produces:
  - Constructor: `class FsifBackend(storeId, path, layout, rawFiles, io, journal: FsifDeviceJournal, tuning,
    onFatal, scheduler, serialEc, readGate)`.
  - `FsifBackend.open(storeId, path, layout, io, journal: FsifDeviceJournal, onFatal, tuning, serialEc, readGate)`.
  - The constructor's open sequence:
    1. `openDirectory(files, layout, journal.sizing.journalId)`.
    2. Check the layout's fit (Global Constraints correction): `IOException`.
    3. Choose a fresh attachment.
    4. Write and force index seq+1 with `IndexMeta(journalId, attachment, journal.tailSerial, detached = false)`.
    5. `journal.attach(attachment, path, owner)`, where the owner's `complete` = `serial.execute`.
  - `appendRecords(records): Future[Long]` → `journal.append(attachment, records)`.
  - **Admission:** `admitted(bound)` reserves through `journal.tryReserve(attachment, bound)`. A refusal queues into
    `capHeld` as today. `Owner.roomAvailable` posts `releaseCapHeld`. `startHeld`'s release calls `unreserve` for the
    bytes not used. `journalHasRoomForMove` = `capHeld.isEmpty && journal.tryReserve(attachment, MappingAppendSize)`,
    the reservation held by the move until its Mapping is written.
  - **Checkpoint phases** (§4.4): Draining (wait for the snapshot's last append; its serial is `S`) → Applying
    (unchanged) → Barrier (`lastAppend` durable) → Finishing (index seq+1 with covered `S`, not detached; then
    `journal.covered(attachment, S)`). `Header`, `switchBuffer`, `switchBytes`, `activeJournal`, `journalFile` and
    `oldFile` go. `appendSeq`/`unappliedBySeq` stay as the checkpoint epoch.
  - **Trigger:** `maybeCheckpoint` starts one when `Owner.checkpointRequested` has arrived, or when `capHeld` is
    non-empty and there are unapplied records.
  - `estimateSize` covers data plus the index files.

- [ ] **Step 1: Port the tests.** In `FsifBackendSuite`, keep every case's name and assertions, built on
  `SharedJournal` with one store. Add:
  - `"two stores sharing one journal both survive a crash with every acknowledged write"`.
  - `"a commit held for journal room has mutated nothing: a read during the hold sees the old state"`.
  - `"checkpoint completes while another store's appends fill the ring"` (deadlock regression for the
    builder-check item 1).
  - `FsifCheckpointSuite`: drop the Ruling 19 switch cases; keep and port `"a crash at any file op during a
    checkpoint with commits in flight keeps every acknowledged write"`, sweeping over the shared machine with two
    stores.
- [ ] **Step 2:** Run `sbt 'testOnly *store.backend.fsif.*'`. Expected: compile failure.
- [ ] **Step 3:** Implement the changes above in `FsifBackend`. Remove `newJournal`, `journalError`, `writeNextHeader`
  and the switch logic, and route `Owner.journalFailed` to `post(failStop(t))`.
- [ ] **Step 4:** Run `sbt 'testOnly *store.backend.fsif.*'`. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF: backends append to the shared device journal`.

### Task 8: Detach, close, journal failure; fault and model suites

**Files:**
- Modify: `store/backend/fsif/FsifBackend.scala`
- Test: `FsifFaultSuite.scala`, `FsifModelSuite.scala`

**Interfaces:**
- Produces:
  - `close()` (§4.6). When not frozen, not fatal and not abandoned, it quiesces, runs a checkpoint covering every
    append, writes index seq+1 with `detached = true` and covered = last serial, then calls
    `journal.release(attachment)`, then closes the channels on `io`. When frozen, it does `abandonHeld`, no detach
    and no release. When fatal, it does not detach.
  - `private[fsif] def detachedForTest: Boolean`.
  - `Owner.journalFailed(t)`: `post(failStop(t))` (L13 at device scope, J4).

- [ ] **Step 1: Write failing tests:**
  - `FsifFaultSuite`: port every journal ENOSPC and failure case. Journal ENOSPC no longer exists; replace with
    `"a journal write failure fail-stops every attached store, each through its own onFatal"`.
  - `FsifFaultSuite`: `"a clean close leaves a detached index and the directory reopens under another journal id
    with nothing to replay"`.
  - `FsifFaultSuite`: `"a frozen close does not detach, and the next device-load replay restores its acknowledged
    writes"`.
  - `FsifModelSuite`: run the 400-operation model across 3 stores on one `SharedJournal`, with each store's history
    checked separately after each crash. Keep the 30 seeds, the coverage floors, and the digest re-run of the first
    two seeds. Add a coverage floor of >= 3 for "crash inside device-load replay".
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `sbt 'testOnly *store.backend.fsif.*'`. Expected: PASS.
- [ ] **Step 5:** Commit: `FSIF: detach on close; device journal failure fail-stops its stores`.

### Task 9: DeviceIo integration and the opener

**Files:**
- Modify: `host/DeviceIo.scala`, `host/ThreadedDeviceIo.scala`, `host/LockstepDeviceIo.scala`,
  `store/backend/BackendOpener.scala`, `store/backend/fsif/FsifFile.scala` (`NioFsifFiles(dir, dsync: Boolean =
  false)`)
- Test: `host/ThreadedDeviceIoSuite.scala` (or the existing DeviceIo suite: `grep -rln "ThreadedDeviceIo" src/test`),
  `store/backend/BackendOpenerSuite.scala`

**Interfaces:**
- Produces:
  - In `DeviceIo`: `def journalRun: Runnable => Unit`. In `ThreadedDeviceIo` it is a single-thread pool `"fjournal"`
    (`pool("fjournal", 1)`), each task passing the `gate` and recorded in the observer as kind `fsif-journal`. In
    `LockstepDeviceIo` it is the internal threaded one's, as `dataEc` is.
  - In `DeviceIo`: `val fsifJournal: java.util.concurrent.atomic.AtomicReference[Option[FsifDeviceJournal]]`
    (initially None). It is set by the device load (Task 10) or a lazy create, and cleared and closed at unload.
  - `DefaultBackendOpener.create`: for `StoreConfig.Fsif`, inside the same `BackendCreate` job and before
    `createStore`, call `FsifJournalDisk.ensure(io, deviceDirOf(storePath), cfg)` (Task 10 defines it; this task
    stubs its signature). Refuse a layout that does not fit (`IllegalArgumentException`).
  - `openFsifBackend` reads `io.fsifJournal.get`. With None, it throws `IOException("FSIF journal of device ... is
    not open")` (a load retry).

- [ ] **Step 1: Write failing tests:**
  - `"journal tasks run on one dev-<label>-fjournal thread, pass the gate, and show in observer.inFlight as
    fsif-journal"`.
  - `"opening an FSIF store with no journal open fails with an IOException, not BackendStateLost"`.
  - `"Review Focus 3: an FSIF create whose slice cannot fit a journal file fails with nothing at the path; an open
    of such a store fails with an IOException naming both sizes"`.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the two suites plus `*DeviceIoUsageSuite`. Expected: PASS.
- [ ] **Step 5:** Commit: `DeviceIo: FSIF journal thread and slot; opener uses the device journal`.

### Task 10: Device load: journal open, replay, lost journal

**Files:**
- Create: `host/FsifJournalDisk.scala`
- Modify: `host/DeviceDiscovery.scala` (`loadOwnedDevice`: a new step after `openCrl` and before P2), the device
  unload paths that shut the CRL (`DeviceShutdown`: also close and clear `io.fsifJournal`), and `host/JobKind`
  (`case FsifJournalOpen` in `DeviceIo.scala`'s enum)
- Test: `server/HostFsifLoadSuite.scala`, plus a new `host/FsifJournalLostSuite.scala` on `CopyHostFixture`

**Interfaces:**
- Consumes: Tasks 3, 6, 9; `DeviceCrlDisk` as the model; `CopyLayoutDisk.list`, `CopyDisk.relocate`,
  `CopyMoves.unforcedUnder`, `StoreConfig.loadStoreConfig`.
- Produces:

```scala
enum FsifJournalOpenResult:
  case Opened(journal: FsifDeviceJournal, lost: List[(os.Path, os.Path)])   // per-copy replay losses moved (from, to)
  case Absent
  case Lost(movedTo: Option[os.Path], journal: FsifDeviceJournal, forced: List[os.Path])
  case Failed(err: Throwable, how: CrlOpenFailure, unsynced: List[(os.Path, os.Path)])

object FsifJournalDisk:
  val LostSite = "lost FSIF device journal"
  def open(devicePath: os.Path, cfg: StorageDeviceConfig, listing: CopyLayout.Listing, run: Runnable => Unit,
           earlierUnforced: Map[os.Path, List[os.Path]], syncDirectory: os.Path => Unit,
           rename: (os.Path, os.Path) => Unit): FsifJournalOpenResult
  /** Lazy creation for the first FSIF create (Task 9): scratch dir, create, durable rename, open, set io.fsifJournal */
  def ensure(io: DeviceIo, devicePath: os.Path, cfg: StorageDeviceConfig): FsifDeviceJournal
```

Behaviour (§4.7, mirroring `DeviceCrlDisk.open`/`replaceLost` step for step):
- **Targets.** The FSIF copies in every phase except Lost and Retired (their `StoreConfig.backend` is `Fsif`; a
  config that cannot be read throws, so the device stays unloaded) plus FSIF staging directories under
  `rebuilding/`.
- **No journal directory.** If every target's newest index is detached (`IndexMeta.None` included), the result is
  Absent (Review Focus 2). If any target's index is undetached while no journal directory exists, that is J2 → the
  lost path.
- **Undetached index naming another journal id** (with or without a local journal). This can only come from damage
  or a bug, since Task 11 refuses such transfers. That copy alone is treated as a per-copy `Lost`.
- **Refusals.** `CrlLock` → `JournalLocked`, and `JournalSizingMismatch` → `Failed(Refused)`, with nothing moved.
- **Transient IOExceptions** (`DeviceDiscovery.isTransient`) → `Failed(Transient)`.
- **Per-copy `Outcome.Lost`.** Move that copy alone with `CopyDisk.relocate` to Lost. A staging directory is
  removed instead. A failed or unforced move → `Failed(AfterLost, unsynced)`.
- **`JournalCorrupt` or J2 → lost path.** Decide every move first. Move each FSIF copy (not already Lost) to
  `lost/`. Force every phase directory, then each lost copy with its earlier unforced directories. Remove the FSIF
  staging directories. Move the old directory to `lostName(uuid)` with `DurableFiles.move`. Create the replacement in
  `CreatingName`, then `DurableFiles.move` it to `DirName`, then open it. Log at error per cause, as `DeviceCrlDisk`
  does. A failure anywhere → `Failed(AfterLost, unsynced)`, with a partial replacement removed.
- **The `DeviceDiscovery` apply** mirrors `openCrl`'s:
  - Failed: held unsynced moves, the device left unloaded, the CRL shut.
  - Lost: `ctx.moves.forcedUnattached(forced)`.
  - Opened or Lost: set `io.fsifJournal`, then continue to P2 and `attachOpened`.
  - Opened's per-copy losses are recorded like P2's moves.

- [ ] **Step 1: Write failing tests** (`FsifJournalLostSuite`, real loading hosts):
  - `"a corrupt journal moves every FSIF copy to lost/, leaves RocksDB copies, keeps fsif-journal.lost-*, and the
    reconcile rebuilds them"`.
  - `"a move to lost/ that fails leaves the device unloaded and the journal untouched; the next scan completes it"`
    (`Host.Seams.copyRename`).
  - `"one copy's replay failure moves only that copy to lost/"`.
  - `"a locked journal refuses the device and moves nothing"`.
  - `"Review Focus 2: a device whose FSIF copies all came from another host loads Absent"`.
  - `HostFsifLoadSuite`: `"a host restart replays acknowledged FSIF writes before the stores open"` (kill -9
    equivalent: drop the host and build a fresh one over the directory).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the new suites, `*HostFsifLoadSuite`, `*DeviceIoUsageSuite`, `*FileStatUsageSuite` and
  `*CopyCrashSuite`. Expected: PASS.
- [ ] **Step 5:** Commit: `Host: open and replay the FSIF device journal at device load; lost journal path`.

### Task 11: Release hooks and the transfer-out detach check

**Files:**
- Modify: `host/CopyMoves.scala` (`moveOnLane`'s apply: when `current`, `result == MoveResult.Durable` and `to` is
  `Phase.Lost` or `Phase.Retired`, call `sds.io.fsifJournal.get.foreach(_.releaseDirectory(from))`),
  `host/StoreCopies.scala`/`CopyDisk` delete apply (release the deleted path likewise), `host/StoreTransfersOut.scala`
  (`closedForTransfer`'s extraction job: read the copy's newest index for an FSIF store; not detached and naming this
  device's journal → refuse, logged at error, the work ended as a NotMoved does), `transfer/TransferDisk.scala` (the
  index read, inside the existing `SaveFile` job)
- Test: `host/FsifJournalReleaseSuite.scala`

**Interfaces:**
- Consumes: `FsifDeviceJournal.releaseDirectory`, `FsifRecovery.newestIndex`.

- [ ] **Step 1: Write failing tests:**
  - `"a fail-stopped FSIF copy pins the ring until its move to lost/ is durable"`.
  - `"Review Focus 5: when that move fails, commits on the device hold; the device check's retry moves it and they
    resume"`.
  - `"a transfer out of an undetached FSIF copy is refused"` (seam a close that skips detach).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the suite and `*WorkOverlapSuite`, `*StoreTransfer*`. Expected: PASS.
- [ ] **Step 5:** Commit: `Host: release FSIF journal attachments with their copies; refuse undetached transfers`.

### Task 12: Measure, set the default, document

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/bench/FsifDeviceBench.scala`. Copy it from
  `/tmp/claude-1000/-home-rakis-devel-aspen/6dc4a5b3-53e4-4944-bd8a-6530da645acc/scratchpad/bench/FsifDeviceBench.scala`
  and adapt it to `FsifJournalDisk.ensure` and the new `FsifBackend.open`.
- Modify: `host/DeviceIo.scala` (`defaultDataThreads`), `store/backend/fsif/DESIGN.md` (2.3, 2.4, 2.7, 2.9, 2.13, 3,
  4: the L table gains J1-J4, and L2's journal half, L4, L5 and L6 go), `server/store/CLAUDE.md` (Backends),
  `server/host/CLAUDE.md` (device load step, lost journal, Host threading model), `server/CLAUDE.md` (BackendTiming
  stages: `fsif journal write`, `fsif journal batch (records)`), `docs/host-threading-measurement.md`, `Todo.txt`

- [ ] **Step 1:** Run the benchmark against an ext4 directory such as `target/fsifbench` (not `/tmp`, which is
  tmpfs): `java -cp "$(sbt -batch 'export Test/fullClasspath' | tail -1)"
  org.aspen_ddp.aspen.bench.FsifDeviceBench $PWD/target/fsifbench <threads> 1 16 8 10`, for threads 1, 2, 4, 8.
  - Success: >= 767 ops/s at 1 or 2 threads.
  - On failure, stop and report the numbers to the user. Do not tune silently.
- [ ] **Step 2:** Set `DeviceIo.defaultDataThreads` to the smallest count within noise of the best, and its scaladoc
  to the measured numbers.
- [ ] **Step 3:** Update the documents listed. In `Todo.txt`, mark the entry done and list the fio re-measure (it
  needs root/TCMU, so the user runs it) as the remaining step.
- [ ] **Step 4:** Run `timeout 80 sbt test`. Expected: all pass, and no "Hung Test" reports from the new suites.
- [ ] **Step 5:** Commit: `FSIF device journal: measured default data threads; docs`.
