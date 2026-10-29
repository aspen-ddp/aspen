# FixedSizeIndexedFile Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A crash-safe flat-file store backend (`store.backend.fsif`) in which small writes and appends cost about the size of the change.

**Architecture:** Slices live in fixed-size segments of one `data` file, located through an external→internal index map that is checkpointed to a/b index files. Small writes are journaled (one sequential fsync) and applied in place later. Large whole-object writes are copy-on-write. All state belongs to one serial executor per backend, with the I/O itself running on a shared pool. File I/O goes through a thin `FsifFile` interface so tests can inject crashes, torn writes, ENOSPC and I/O errors.

**Tech Stack:** Scala 3, os-lib, `java.nio.channels.FileChannel`, `java.util.zip.CRC32`, ScalaTest (`AnyFunSuite` + `Matchers`, `FileBasedTests` for temp dirs), protobuf (`src/main/protobuf/codec.proto`).

**Spec:** `docs/superpowers/specs/2026-10-29-fsif-backend-design.md`. Read it before any task. This plan cites its section names rather than restating them.

## Global Constraints

- Package `org.aspen_ddp.aspen.server.store.backend.fsif`; sources in `src/main/scala/org/aspen_ddp/aspen/server/store/backend/fsif/`, tests in the mirror path under `src/test`.
- Scala 3 quiet syntax; indented if/then/else; braces only where they clearly help.
- os-lib for filesystem operations; java.nio only for `FileChannel` positional I/O and `force`.
- Every checksum is `java.util.zip.CRC32`, stored as a 4-byte big-endian `Int`. All integers are big-endian.
- No backward compatibility with persisted data is needed.
- Tests: correctness first, then no flakiness (no sleeps used for ordering; await `quiesce()` or futures instead), then speed. Use small layouts (e.g. `segmentPayloadSize = 1024`, `segmentCount = 64`).
- Randomized tests take an explicit seed, print it on failure (`withClue`), and are deterministic for a given seed.
- Run a suite with `sbt 'testOnly *SuiteName'`. Full test runs use a 70 s timeout.
- Commit directly to `main` after each task, ending the message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

### Refinement of the spec, decided in this plan

The design checks ObjectIds on every commit and on `repairDelete`. To do that without disk reads, the ObjectId is also kept in the index: **index entries are `slot(4) + ObjectId(16)`, and Map records carry the ObjectId** (the zero UUID when `slot = -1`). Segment headers still carry the ObjectId, and reads still verify it.

### Constants fixed by this plan

| Name | Value |
|---|---|
| `SegmentHeader.Size` | 61 (type 1, ObjectId 16, ext 4, length 4, Metadata 32, CRC 4) |
| stride | `roundUp(61 + segmentPayloadSize, 4096)` |
| journal magic | ASCII `FSIFJRNL` (8 bytes) |
| index magic | ASCII `FSIFINDX` (8 bytes) |
| journal header | magic 8 + seq 8 + CRC 4 = 20 bytes (CRC over magic + seq) |
| batch | `len(4) batchNo(8) records… crc(4)`, where `len` = bytes of `batchNo + records`, and the CRC covers `len + batchNo + records` |
| record | `len(4) type(1) payload`. Type 1 = Mapping: `ext 4, slot 4, objectId 16`. Type 2 = Delta: `ext 4, slot 4, header 61, rangeCount 4, then (offset 4, length 4, bytes)*` |
| index file | magic 8 + seq 8 + `segmentCount × 20` + CRC 4 (over everything before it) |
| storePointer | exactly 4 bytes, big-endian Int `X`, `0 <= X < segmentCount` |
| `FsifTuning` defaults | `cowThreshold = 64 KiB`, `journalBound = 64 MiB`, `unappliedBound = 64 MiB`, `enospcRetry = 1.second` |
| shared I/O pool | fixed, `max(4, availableProcessors)` daemon threads named `fsif-io-N` |

## Review Focus

1. **Malformed `storePointer`** (empty, 3 or 5 bytes, negative, `>= segmentCount`): a commit completes with `CommitError.InvalidPointer`, a read completes with `ObjectNotFound`, and nothing crashes. Test in Task 9.
2. **A slice larger than `segmentPayloadSize`** arriving through `repair`, opportunistic rebuild or `rebuildWrite` (which bypass `checkSliceSize`): it is rejected (`CommitError.SliceTooLarge`, a failed repair promise, an exception from `rebuildWrite`) and never written past its segment into a neighbor. Test in Task 9.
3. **Growing an object by appends in a reused slot** whose previous occupant was larger: bytes beyond the old length must read back as the new data, never the old occupant's bytes. Test in Task 9.
4. **A crash during recovery itself** (mid-replay, mid-way through finishing an interrupted checkpoint, mid torn-tail truncation): recovering again yields the same acknowledged state. Test in Task 13.
5. **Reading an object while its slot is being compacted or the tail truncated:** the read returns the correct data, never zeros or `ObjectNotFound`. Test in Task 11.

---

### Task 1: Thread the pointer, the allocation flag and the update form through `CommitState`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/UpdateForm.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/CommitState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/CommitError.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/Backend.scala` (`rebuildWrite`)
- Modify: `MapBackend.scala`, `rocksdb/RocksDBBackend.scala`, `server/store/Frontend.scala` (`opportunisticRebuild`, `repair`), `server/transaction/Tx.scala` (both `CommitState` constructions), `server/rebuild/RebuildingStore.scala:326`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/backend/UpdateFormSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  enum UpdateForm:
    case Full
    case MetadataOnly
    /** spans: sorted, non-overlapping (offset, length) pairs in the NEW slice; includes [oldLength, newLength) when it grows */
    case Ranges(oldLength: Int, spans: List[(Int, Int)])
  object UpdateForm:
    def of(objectId: ObjectId, requirements: List[TransactionRequirement],
           objectUpdates: Map[ObjectId, DataBuffer], allocating: Boolean): UpdateForm
  case class CommitState(pointer: ObjectPointer, metadata: Metadata, objectType: ObjectType.Value,
                         data: DataBuffer, allocating: Boolean = false, form: UpdateForm = UpdateForm.Full):
    def objectId: ObjectId = pointer.id
  // CommitError gains: ObjectMismatch, IOFailure, SliceTooLarge
  // Backend: def rebuildWrite(pointer: ObjectPointer, objectType: ObjectType.Value, metadata: Metadata, data: DataBuffer): Unit
  ```

- [ ] **Step 1: Write the failing tests in `UpdateFormSuite`**
  - `"allocation is Full"`: `of(oid, List(DataUpdate(ptr, rev, DataUpdateOperation.Append)), updates, allocating = true) shouldBe Full`.
  - `"overwrite is Full"`.
  - `"append spans include the zero-extended area"`: an `AppendDelta(expectedSliceLength = 10, newSliceLength = 30, regions = List(XorRegion(20, 5 bytes)), None)` gives `Ranges(10, List((10, 20)))`. The region `[20, 25)` merges into the extension `[10, 30)`.
  - `"write-range with no growth keeps region spans"`: `AppendDelta(100, 100, List(XorRegion(4, 2 bytes), XorRegion(50, 8 bytes)), None)` gives `Ranges(100, List((4, 2), (50, 8)))`.
  - `"key-value update is Full"`.
  - `"refcount-only is MetadataOnly"` (a `RefcountUpdate` and no data requirement for the object). Same for `VersionBump`.
- [ ] **Step 2:** Run `sbt 'testOnly *UpdateFormSuite'`. Expected: compile failure (`UpdateForm` is not defined).
- [ ] **Step 3: Implement `UpdateForm` and change `CommitState`, `CommitError` and `Backend.rebuildWrite`.** Update every call site:
  - `Tx.doCommit`: take `pointer` from `objectPointers` by id, set `allocating = txd.allocatingObjects.contains(id)`, and compute `form = UpdateForm.of(id, txd.requirements, ou, allocating)`.
  - `Tx` pre-transaction rebuild: look up the pointer the same way; `Full`.
  - `Frontend.opportunisticRebuild`: `op.pointer`; `Full`.
  - `Frontend.repair`: `current.pointer`; `Full`.
  - `RebuildingStore.restore`: pass `ptr`.
  - `MapBackend` and `RocksDBBackend` use `state.objectId` and ignore the new fields.
- [ ] **Step 4:** Run `sbt 'testOnly *UpdateFormSuite *FrontendSuite *RebuildEndToEndSuite *RocksDB*'`. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Pass pointer, allocation flag and update form to store backends".

### Task 2: Stop ignoring commit errors; always complete RocksDB reads

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala:176-188,304-308`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/rocksdb/RocksDBBackend.scala` (`read`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/server/store/backend/rocksdb/RocksDBBackendResourcesSuite.scala`

**Interfaces:**
- Consumes: `CommitError.IOFailure` (Task 1).

- [ ] **Step 1: Write the failing tests.**
  - In `FrontendSuite`: `"backend commit errors are reported in TxCommitted"`. Use a `MapBackend` subclass whose `commit` completes `Commit(..., Right(CommitError.IOFailure))`. Drive a transaction updating `oid1` to commit. Assert the `TxCommitted` sent has `objectCommitErrors` containing `oid1`.
  - In `RocksDBBackendResourcesSuite`: `"a read that fails in the database still completes"`. Close the backend's database, call `read(ptr)`, and assert the completion handler receives a `Read` whose result is `Right(ReadError.CorruptedObject)`.
- [ ] **Step 2:** Run both suites. Expected: both new tests FAIL (the first sees an empty error list; the second times out).
- [ ] **Step 3: Implement.**
  - Assign `objectCommitErrors = uncommittable.toList` *before* the commit loop, because a synchronous backend can complete inside it. In `commitComplete`, prepend `objectId` on a `Right(_)` result.
  - In `RocksDBBackend.read`, complete with `ReadError.CorruptedObject` on Future failure, and also catch any synchronous throw.
  - In `MissedUpdateFinalizationAction` (lines 45–52), drop the comment about entries written before this change (backward compatibility isn't required). Keep "an empty value means delete by ObjectId alone", which RocksDB stores still rely on.
- [ ] **Step 4:** Rerun both suites. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Report backend commit errors and always complete RocksDB reads".

### Task 3: File abstraction with crash and fault simulation

**Files:**
- Create: `fsif/FsifFile.scala` (the trait and the NIO implementation)
- Create (test): `fsif/SimulatedFsifFiles.scala`, `fsif/SimulatedFsifFilesSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  trait FsifFile:
    def size: Long
    def read(position: Long, length: Int): Array[Byte]   // fewer bytes if it reaches EOF
    def write(position: Long, bytes: Array[Byte]): Unit   // extends the file as needed
    def truncate(size: Long): Unit
    def force(): Unit                                     // data + size durable (FileChannel.force(false))
    def close(): Unit
  trait FsifFiles:
    def open(name: String): FsifFile                      // creates if missing
    def exists(name: String): Boolean
  final class NioFsifFiles(dir: os.Path) extends FsifFiles  // creating a file syncs the directory via DurableFiles.syncDirectory
  object FsifIo:
    def isOutOfSpace(t: Throwable): Boolean   // "no space left" in any cause message, as RebuildingStore.isOutOfSpace does
  // test only:
  final class SimulatedFsifFiles(rnd: scala.util.Random) extends FsifFiles:
    def crash(): SimulatedFsifFiles            // new instance holding only the durable image
    def failNext(fault: Fault): Unit           // the next write/force/truncate throws
    def failWhile(fault: Fault)(p: => Boolean): Unit   // persistent fault until p is false
  enum Fault:
    case OutOfSpace   // IOException("No space left on device")
    case IoError      // IOException("Input/output error")
  ```
- Crash model: each file keeps its durable bytes plus the ops pending since its last `force`. `crash()` applies each pending op independently with probability 1/2. A pending write that is applied may be torn: a random subset of its 512-byte sectors is applied. A pending truncate is applied or skipped as a whole. `force()` makes everything pending durable.

- [ ] **Step 1: Write the failing tests in `SimulatedFsifFilesSuite`.**
  - Forced writes survive `crash()`.
  - Over 200 seeded crashes, un-forced writes are sometimes applied, sometimes absent and sometimes torn at a 512-byte boundary; every outcome occurs at least once.
  - A truncate followed by force is durable.
  - `failNext(OutOfSpace)` makes the next write throw an `IOException`, and `FsifIo.isOutOfSpace` returns true for it.
  - `NioFsifFiles` round-trips a write, read, truncate and force under `tdir` (a `FileBasedTests` suite).
- [ ] **Step 2:** Run it. Expected: compile failure.
- [ ] **Step 3: Implement** `FsifFile`, `NioFsifFiles` (one `FileChannel` per file, `READ`/`WRITE`/`CREATE`) and `SimulatedFsifFiles`.
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF file abstraction with crash simulation".

### Task 4: Segment header, layout and index file formats

**Files:**
- Create: `fsif/FsifLayout.scala`, `fsif/SegmentHeader.scala`, `fsif/IndexFile.scala`
- Test: `fsif/FormatsSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  final case class FsifLayout(segmentPayloadSize: Int, segmentCount: Int):
    val stride: Long                       // roundUp(SegmentHeader.Size + segmentPayloadSize, 4096)
    def slotOffset(slot: Int): Long
  final case class SegmentHeader(objectType: ObjectType.Value, objectId: ObjectId, externalIndex: Int,
                                 sliceLength: Int, metadata: Metadata, crc: Int)
  object SegmentHeader:
    val Size = 61
    def forSlice(objectType: ObjectType.Value, objectId: ObjectId, externalIndex: Int,
                 metadata: Metadata, slice: DataBuffer): SegmentHeader   // computes the CRC
    def encode(h: SegmentHeader): Array[Byte]
    def decode(bytes: Array[Byte]): Option[SegmentHeader]   // None: bad type byte or short
    def verify(h: SegmentHeader, slice: Array[Byte]): Boolean
  /** slots(ext) = internal slot or -1; ids(ext) = occupant (ObjectId of zero UUID when unmapped) */
  final class IndexMap(val slots: Array[Int], val ids: Array[ObjectId]):
    def copy(): IndexMap
  object IndexMap:
    def empty(segmentCount: Int): IndexMap
  object IndexFile:
    def encode(seq: Long, map: IndexMap): Array[Byte]
    def decode(bytes: Array[Byte], segmentCount: Int): Option[(Long, IndexMap)]  // None: wrong size, magic or CRC
  ```

- [ ] **Step 1: Write the failing tests.**
  - `FsifLayout(1024, 8).stride == 4096` and `FsifLayout(4096, 8).stride == 8192`.
  - Header encode/decode round trip; `encode(h).length == 61`.
  - `verify` fails after flipping one slice byte, and after flipping one header byte (re-decoded).
  - Index round trip; `decode` returns `None` for: one byte short, one byte long, a flipped entry byte, a bad magic, and an empty array.
- [ ] **Step 2:** Run `sbt 'testOnly *fsif.FormatsSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement**, using the constants table.
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF segment and index formats".

### Task 5: Journal format and scan

**Files:**
- Create: `fsif/JournalFormat.scala`
- Test: `fsif/JournalFormatSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  final case class DeltaRange(offset: Int, bytes: Array[Byte])
  enum JournalRecord:
    case Mapping(ext: Int, slot: Int, objectId: ObjectId)
    case Delta(ext: Int, slot: Int, header: SegmentHeader, ranges: List[DeltaRange])
  object JournalFormat:
    val HeaderSize = 20
    def encodeHeader(seq: Long): Array[Byte]
    def encodeBatch(batchNumber: Long, records: List[JournalRecord]): Array[Byte]
    enum Scan:
      case Empty                                   // invalid header and length <= HeaderSize
      case Valid(seq: Long, records: Vector[JournalRecord], validLength: Long, nextBatch: Long)
      case Corrupt(reason: String)                 // invalid header with a longer file, or corruption mid-journal
    def scan(bytes: Array[Byte]): Scan
  ```
- Scan rule (spec, "Journal"): batches must number 0, 1, 2… If the batch at offset `o` with expected number `n` is short or fails its CRC, search the rest of the file for a valid batch numbered `n + 1`. If one is found, return `Corrupt`. Otherwise return `Valid` with `validLength = o`. To keep the search cheap, match the 8-byte `n + 1` pattern first and check the CRC only on a match.

- [ ] **Step 1: Write the failing tests.**
  - Round trip of three batches with mixed records.
  - Truncating the file at every byte offset inside the last batch gives `Valid` with exactly the earlier batches' records and a `validLength` equal to the start of the last batch.
  - Flipping a byte in batch 1 of 3 gives `Corrupt`.
  - Flipping a byte in the last batch gives `Valid`.
  - A file of 0, 7 or 20 garbage bytes gives `Empty`; 21+ garbage bytes give `Corrupt`.
  - A header with seq 5 and no batches gives `Valid(5, Vector(), 20, 0)`.
- [ ] **Step 2:** Run it. Expected: compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF journal format with torn-tail detection".

### Task 6: Replay

**Files:**
- Create: `fsif/Replay.scala`
- Test: `fsif/ReplaySuite.scala`

**Interfaces:**
- Consumes: `IndexMap`, `JournalRecord`.
- Produces:
  ```scala
  object Replay:
    final case class Result(index: IndexMap, apply: Vector[JournalRecord.Delta])  // apply in this order
    def run(start: IndexMap, records: Iterable[JournalRecord]): Result
  ```
- Algorithm (spec, "Replay"): pass 1 walks the records, maintaining the mapping, and records for each slot the position of the last `Mapping` that installed its current occupant (`-1` when the occupant came from `start`). Pass 2 keeps a `Delta(X, i)` only if X's final slot is `i` and the Delta's position is greater than that installing position for `i`.

- [ ] **Step 1: Write the failing tests.** Each is named for the spec case it covers.
  - `"dead occupant's delta is not applied to a reused slot"`: `Delta(X,3)`, `Mapping(X,-1)`, `Mapping(Y,3)` → no deltas.
  - `"pre-move deltas are not applied to the freed tail"`: start with `X→7`; `Delta(X,7)`, `Mapping(X,2)`, `Mapping(Y,7)` → no deltas, X→2, Y→7.
  - `"old delta does not overwrite a newer copy-on-write version"`: start with `X→1`; `Delta(X,1)`, `Mapping(X,4)` → none applied.
  - `"deltas for the live occupant apply in order"`: start with `X→1`; `Delta(X,1,a)`, `Delta(X,1,b)` → `Vector(a, b)`.
  - `"reallocation to the same slot applies only the new incarnation"`: start with `X→1`; `Delta(X,1,old)`, `Mapping(X,-1)`, `Mapping(X,1,P)`, `Delta(X,1,new)` → `Vector(new)`, and `ids(X) == P`.
  - `"journaled allocation"`: `Mapping(X,5)`, `Delta(X,5)` → applied.
- [ ] **Step 2:** Run it. Expected: compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF journal replay".

### Task 7: Store initialization and recovery

**Files:**
- Create: `fsif/FsifRecovery.scala`
- Test: `fsif/FsifRecoverySuite.scala`

**Interfaces:**
- Consumes: Tasks 3–6.
- Produces:
  ```scala
  object FsifRecovery:
    final class StoreLost(msg: String) extends Exception(msg)
    final case class Recovered(index: IndexMap, seq: Long, activeJournal: String,  // "journal.a" | "journal.b"
                               journalLength: Long, nextBatch: Long, slotCount: Int)
    /** Fresh store: index.a = (seq 0, empty map), journal.a = header(seq 0), empty data; each forced. */
    def initialize(files: FsifFiles, layout: FsifLayout): Unit
    def recover(files: FsifFiles, layout: FsifLayout): Recovered
  ```
- `recover`, following the spec's "Recovery rule", "Replay" and "Startup":
  1. Decode both index files and take the valid one with the highest seq `m`. If neither is valid, throw `StoreLost`.
  2. Scan both journals. Any `Corrupt` journal with seq `>= m` throws `StoreLost`.
  3. Replay the journals with seq `>= m` in seq order.
  4. Write each kept Delta's ranges and header into the slot, then force `data`.
  5. Validate: a mapped slot beyond the data file that no applied Delta covers, or a slot mapped twice, throws `StoreLost`.
  6. Truncate the active journal's torn tail to `validLength` and force it.
  7. If two journals were replayed, finish the interrupted checkpoint (spec, "Checkpoints" steps 3–4 for seq `m+1`, using the replayed map at the end of journal `m`) before returning.
  8. `slotCount = max(dataSize / stride, 1 + highest mapped slot)`.

- [ ] **Step 1: Write the failing tests** (all on `SimulatedFsifFiles`).
  - `initialize` then `recover` gives an empty map with seq 0.
  - A journal holding `Mapping` and `Delta` records for 3 objects recovers those objects' segments byte-exactly.
  - Index `s` valid, index `s+1` torn, journals `s` and `s+1` both holding records → recovers the union. Afterwards index `s+1` is valid and journal `s` is empty.
  - Both index files invalid → `StoreLost`.
  - Corruption in the middle of the active journal → `StoreLost`.
  - A torn tail is truncated: `files.open(active).size == validLength` after `recover`.
  - A slot mapped twice in the index → `StoreLost`.
- [ ] **Step 2:** Run it. Expected: compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF store initialization and recovery".

### Task 8: Serial executor and journal writer

**Files:**
- Create: `fsif/SerialExecutor.scala`, `fsif/JournalWriter.scala`
- Test: `fsif/JournalWriterSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  /** Runs submitted tasks one at a time, in submission order, on `underlying` */
  final class SerialExecutor(underlying: ExecutionContext) extends ExecutionContext
  /** Owned by the backend's SerialExecutor; every method is called on it and every Future completes on it. */
  final class JournalWriter(file: FsifFile, startLength: Long, nextBatch: Long,
                            io: ExecutionContext, serial: SerialExecutor):
    def append(records: List[JournalRecord]): Future[Unit]  // completes when its batch is forced
    def length: Long                                        // bytes durable or in flight
    def idle: Boolean
  ```
- Behavior: at most one batch is in flight. Records appended while a batch is in flight are buffered and become the next batch, so one write and one `force` cover all of them. On a failed write or force, the writer truncates back to the last durable length and forces before retrying the same batch, which keeps a torn tail from ever preceding later batches. It fails the returned futures with the original exception; the backend decides between retrying and failing stop (Task 12).

- [ ] **Step 1: Write the failing tests.**
  - `SerialExecutor` runs 10,000 tasks submitted from 8 threads one at a time (an `AtomicInteger` in-task counter never exceeds 1).
  - Ten `append`s issued while the first batch is held (use a gate-controlled `io` EC) produce exactly 2 batches, by `JournalFormat.scan`.
  - Every acknowledged record survives `crash()`.
  - After an injected `IoError` on a force, the file is truncated back to the last durable length.
- [ ] **Step 2:** Run it. Expected: compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF serial executor and batching journal writer".

### Task 9: `FsifBackend`: read, commit, delete, repair and rebuild

**Files:**
- Create: `fsif/FsifBackend.scala`, `fsif/FsifTuning.scala`
- Test: `fsif/FsifBackendSuite.scala`

**Interfaces:**
- Consumes: Tasks 1, 3–8.
- Produces:
  ```scala
  final case class FsifTuning(cowThreshold: Int = 64 * 1024, journalBound: Long = 64L << 20,
                              unappliedBound: Long = 64L << 20, enospcRetry: FiniteDuration = 1.second)
  class FsifBackend(val storeId: StoreId, val path: os.Path, layout: FsifLayout, files: FsifFiles,
                    io: ExecutionContext, tuning: FsifTuning = FsifTuning(),
                    onFatal: Throwable => Unit = _ => ()) extends Backend:
    def isWriteFrozen: Boolean
    def quiesce(): Future[Unit]   // completes when no journal batch, in-place write, move or checkpoint is pending
  object FsifBackend:
    lazy val sharedIo: ExecutionContext
    def decodeStorePointer(sp: Array[Byte], segmentCount: Int): Option[Int]
    def create(storeId: StoreId, path: os.Path, layout: FsifLayout, io: ExecutionContext,
               onFatal: Throwable => Unit): FsifBackend       // os.makeDir.all, initialize, then construct
  ```
- The constructor runs `FsifRecovery.recover` synchronously, as `RocksDBBackend` opens its database. Interface methods only post to the serial executor, and completions call `handler.complete` from it.
- In-memory state: `IndexMap`; a `java.util.BitSet` of used slots (mapped, reserved, or waiting for the slot-freeing rule), whose `nextClearBit(0)` gives the lowest free slot; per-ext pending Deltas (header + ranges) awaiting their in-place write; per-slot in-flight read counts.
- Commit decision table (spec, "Write paths"):
  - `storePointer` invalid → `InvalidPointer`.
  - `data.size > segmentPayloadSize` → `SliceTooLarge`.
  - `refcount == 0` → `Mapping(X, -1, zero)`.
  - Not allocating and (`slots(X) == -1` or `ids(X) != objectId`) → `ObjectMismatch`.
  - Allocating and `ids(X)` is a different live id → log a warning and treat it as an overwrite of that slot.
  - `Full` with `data.size > cowThreshold` → copy-on-write (write the segment to a free slot, force data, then `Mapping`).
  - Otherwise journaled: `Full` → one range `(0, data)`, `Ranges` → the spans' bytes from `data`, `MetadataOnly` → no ranges, each Delta carrying `SegmentHeader.forSlice(...)` for the full new slice; with `Mapping(X, f, id)` first when X is unmapped.
- The read path overlays pending Deltas (spec, "Read path"). `repair` uses the same decision table, completing its promise and failing it on an error. `repairDelete` is a no-op success unless `ids(X) == objectId`; an empty pointer logs a warning and succeeds. `rebuildWrite` and `rebuildFlush` follow the spec's "Rebuild and bootstrap entry points"; `rebuildWrite` throws `IllegalArgumentException` on an invalid pointer or an oversized slice. The bootstrap methods throw `UnsupportedOperationException`. `estimateSize` sums the sizes of `data`, both journals and both index files.

- [ ] **Step 1: Write the failing tests** (on `SimulatedFsifFiles` with an inline `io` EC; await completions through a recording `CompletionHandler`).
  - Allocate, read back, overwrite, read back, delete, and the read then gives `ObjectNotFound`.
  - Append 100 × 37 bytes to one object and read back byte-exactly. Then `crash()`, reopen and read back byte-exactly.
  - `"appends into a reused slot never expose the previous occupant"` (Review Focus 3): fill slot 0 with 1000 bytes of `0xAA`, delete, `quiesce()`, allocate a new object at another X (it reuses slot 0), and append 10 bytes at a time up to 1000. Every read shows only the new bytes, before and after `crash()` and reopen.
  - `"malformed store pointers fail cleanly"` (Review Focus 1): for each pointer in `Array()`, 3 bytes, 5 bytes, `-1`, `segmentCount`, a commit gives `InvalidPointer` and a read gives `ObjectNotFound`.
  - `"oversized slices are rejected on every entry point"` (Review Focus 2): a commit gives `SliceTooLarge`, the repair promise fails, and `rebuildWrite` throws; the neighbor slot's object still reads back intact.
  - A non-allocating commit to an unmapped X gives `ObjectMismatch`.
  - `repairDelete(O, X)` with X now holding P leaves P readable.
  - A commit larger than `cowThreshold` goes through copy-on-write: after `quiesce()` its old slot is reusable. For 20 seeds, issue the overwrite and `crash()` without quiescing: the object reads back as either the old version or (if the commit was acknowledged) the new one, never torn.
  - Two commits to the same object issued back to back (the second before the first completes) leave the second's data after `quiesce()` and after `crash()`.
  - A read issued while an in-place write is pending returns the new data.
- [ ] **Step 2:** Run `sbt 'testOnly *FsifBackendSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FsifBackend read, commit, delete, repair and rebuild paths".

### Task 10: Checkpoints

**Files:**
- Modify: `fsif/FsifBackend.scala`
- Test: `fsif/FsifCheckpointSuite.scala`

**Interfaces:**
- Produces: `private[fsif] def checkpointNow(): Future[Unit]` (a test hook; checkpoints are otherwise triggered after commit and compaction completions when the active journal exceeds `max(indexFileSize / 2, tuning.journalBound)`). Also `def journalSeq: Long` for tests.
- Steps exactly as the spec's "Checkpoints" 1–4: step 2 waits for every pending Delta from journal `s` to be applied or cancelled, then forces `data`.

- [ ] **Step 1: Write the failing tests.**
  - After `checkpointNow()`, `journalSeq` increments, the old journal and old index are empty files, and the objects read back correctly.
  - With `journalBound = 4096`, 200 small writes trigger at least one checkpoint without the test calling it.
  - For 50 seeds, crash during a checkpoint (inject `crash()` after a random number of file ops via a `SimulatedFsifFiles` op-count hook), recover, and every acknowledged write reads back. Add the hook `crashAfterOps(n: Int)` to `SimulatedFsifFiles`.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF index checkpoints".

### Task 11: Tail compaction

**Files:**
- Modify: `fsif/FsifBackend.scala`
- Test: `fsif/FsifCompactionSuite.scala`

**Interfaces:**
- Produces: `def slotCount: Int` (for tests). A move follows the spec's "Tail compaction" steps 1–6, including the durable-before-map rule and the extra force for commits that arrive after the first force. A move starts after any completion when none is outstanding. Truncation waits for the slot-freeing rule.

- [ ] **Step 1: Write the failing tests.**
  - Allocate 10 objects, delete the first 9, `quiesce()`: `slotCount == 1`, the data file is exactly one stride long, and the survivor reads back.
  - `"reads during a move or truncate see correct data"` (Review Focus 5): with a gate-controlled `io` EC, park a move at each of its steps, issue a read of the moving object and of the tail object, release, and both return correct data.
  - Small writes to the moving object at each parked step: correct data after `quiesce()` and after `crash()`.
  - A copy-on-write overwrite and a delete of the moving object each cancel the move; the reserved slot becomes free.
  - For 50 seeds, `crashAfterOps(random)` during compaction, recover, and acknowledged state is intact.
  - Compaction resumes after reopening: delete without quiescing, crash, reopen, `quiesce()`, and the file shrinks.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Add FSIF tail compaction".

### Task 12: Out-of-space freeze and fail-stop

**Files:**
- Modify: `fsif/FsifBackend.scala`
- Test: `fsif/FsifFaultSuite.scala`

**Interfaces:**
- Consumes: `FsifIo.isOutOfSpace`, `SimulatedFsifFiles.failWhile`, `onFatal`.
- Behavior per the spec's "Out of space" and "Other I/O failures" sections. Retries are scheduled every `tuning.enospcRetry` on a shared daemon `ScheduledExecutorService` in `object FsifBackend`. Backpressure: while unapplied Delta bytes exceed `tuning.unappliedBound`, commit completions are held until in-place writes catch up.
- Fail-stop: further commits complete with `IOFailure`, reads with `CorruptedObject`, and `onFatal(err)` is called exactly once.

- [ ] **Step 1: Write the failing tests** (`enospcRetry = 10.millis`).
  - `failWhile(OutOfSpace)` on the journal: a commit does not complete and `isWriteFrozen` is true; reads of earlier objects still succeed. Clearing the fault lets the commit complete, and its data reads back after `crash()`.
  - `failWhile(OutOfSpace)` on `data` with acknowledged deltas pending: reads return the new data through the overlay, and the in-place writes finish after the fault clears.
  - `failNext(IoError)` on a journal force: the pending commit completes with `IOFailure`, `onFatal` is called once, and later reads give `CorruptedObject`.
  - With `unappliedBound = 2048` and `data` writes gated, the 3rd 1 KiB commit's completion is held until the gate opens.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Freeze FSIF writes on ENOSPC and fail-stop on I/O errors".

### Task 13: Model-based randomized crash test

**Files:**
- Test: `fsif/FsifModelSuite.scala`

**Interfaces:**
- Consumes: `FsifBackend`, `SimulatedFsifFiles.crashAfterOps`, `MapBackend` (the model of acknowledged state).

- [ ] **Step 1: Write the tests.**
  - `"random operations match the model across crashes"`: 30 fixed seeds. Each runs 400 random operations (allocate, small write, append, overwrite, metadata-only, delete, read, `checkpointNow`, wait for a compaction) over 16 external indices, with 3–5 crashes at random op counts. After each recovery, every acknowledged object matches the model byte for byte, including metadata, and every deleted one gives `ObjectNotFound`. Operations issued but not acknowledged before a crash may match either the old or the new state.
  - `"crash during recovery"` (Review Focus 4): 30 seeds. Crash, then crash again partway through `recover` (`crashAfterOps(random)`), recover a second time, and the state matches the model.
- [ ] **Step 2:** Run `sbt 'testOnly *FsifModelSuite'`. Expected: PASS. Any failure is a bug in Tasks 7–12: fix it there, and add the failing seed as its own named test in the owning suite.
- [ ] **Step 3:** Run it 5 times in a row. Expected: identical results every run.
- [ ] **Step 4: Commit** with the message "Add FSIF model-based crash test".

### Task 14: Configuration, codec and host wiring

**Files:**
- Modify: `src/main/protobuf/codec.proto` (`BackendConfig` enum: `BACKEND_CONFIG_FSIF = 1`; `StoragePoolState`: `uint32 fsifSegmentCount = 13;`)
- Modify: `server/store/backend/BackendConfig.scala`, `common/network/Codec.scala:1189-1194,1230-1232` and the decode path, `server/StoreConfig.scala`
- Modify: `server/Host.scala:934-935` (load), `:1772-1779` (create), and the failure handling
- Modify: `server/rebuild/RebuildingStore.scala:182-191`
- Modify: `cmdline/Main.scala` `create_pool`, plus its argument parsing
- Test: the existing Codec and StoreConfig suites (find them with `grep -rln "decodeBackendConfig\|StoreConfig" src/test`), adding cases there

**Interfaces:**
- Produces:
  ```scala
  case class FsifConfig(segmentPayloadSize: Int, segmentCount: Int) extends BackendConfig:
    val backendName = "fsif"
    def maxSliceSize: Option[Int] = Some(segmentPayloadSize)
  object FsifConfig:
    def forObjectSize(ida: IDA, maxObjectSize: Int, segmentCount: Int): FsifConfig   // ida.minSliceSizeFor
  // StoreConfig:
  case class Fsif(segmentPayloadSize: Int, segmentCount: Int) extends StorageBackend   // name "fsif"
  //   yaml keys: segment-payload-size, segment-count; maxSliceSize = Some(segmentPayloadSize)
  //   StoreConfig.backend becomes Required[StorageBackend] with Choice "rocksdb" | "fsif"
  ```
- Host:
  - Construct with `FsifBackend.create` (on store creation) or `new FsifBackend(... NioFsifFiles(path), FsifBackend.sharedIo ...)` (on load).
  - On load, a `FsifRecovery.StoreLost` from the constructor calls `markRebuildRequired(path)` and leaves the store offline. That is the same outcome `crlSaveFileLost` produces, and the reconcile then flips the entry to Rebuilding.
  - `onFatal` enqueues a new host event `BackendFailed(storeId, err)`. Its handler closes the store unserved, calls `markRebuildRequired`, and marks the store offline, reusing `crlSaveFileLost`'s steps.
- cmdline: `create-pool` gains `--backend rocksdb|fsif` (default `rocksdb`), plus `--max-object-size <bytes>` and `--segment-count <n>`, which are required with `fsif`.

- [ ] **Step 1: Write the failing tests.** Codec round trip of a `StoragePoolState` with `FsifConfig(4097, 1000)`; StoreConfig yaml round trip of `Fsif(4097, 1000)`; `FsifConfig.forObjectSize(Replication(3,2), 10 << 20, 10).segmentPayloadSize == 10 << 20`.
- [ ] **Step 2:** Run those suites. Expected: compile failure.
- [ ] **Step 3: Implement** all the wiring above.
- [ ] **Step 4:** Run the suites, then the full test suite (70 s timeout). Expected: PASS.
- [ ] **Step 5: Commit** with the message "Wire FSIF backend into pool config, codec, host and rebuild".

### Task 15: End-to-end integration

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/FsifIntegrationSuite.scala`, plus a test-only `FsifTestIndexStrategy` in the same directory

**Interfaces:**
- Consumes: Task 14. Model pool creation on `DeviceAdoptionSuite.newPool` and the append and ranged-write helpers on `AppendIntegrationSuite` and `RangedWriteIntegrationSuite`.
- `FsifTestIndexStrategy`: a `StoragePoolAllocationStrategy.Factory` whose strategy hands out 4-byte big-endian indices from an `AtomicInteger`, with a no-op `deleteObject`. Register it in the type registry the test network builds (find where the test hosts and clients construct their `TypeRegistry`). Set it on the new pool by writing its UUID under `StoragePoolState.AllocationStrategyKey` in the pool's KV object, in one transaction after creation.

- [ ] **Step 1: Write the tests** on a `RandomAccessReedSolomon` pool with `FsifConfig.forObjectSize(ida, 64 * 1024, 256)`:
  - Allocate, append 50 chunks, and `readRange` and `readAppendPosition` match.
  - `Transaction.rangedWrites` updates a middle range, and the bytes match.
  - Restart a host (crash-only: drop it and start it from the same directories), and the data still matches.
  - Rebuild one store through the existing rebuild path, and the data still matches.
- [ ] **Step 2:** Run `sbt 'testOnly *FsifIntegrationSuite'`. Expected: PASS. A failure is a bug in Tasks 9–14: fix it there.
- [ ] **Step 3:** Run the full test suite (70 s timeout). Expected: PASS.
- [ ] **Step 4: Commit** with the message "Add FSIF end-to-end integration tests".
