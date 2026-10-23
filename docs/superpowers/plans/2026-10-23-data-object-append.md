# Data Object Append and Byte-Range Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let clients append to DataObjects and read byte ranges of them in Replication pools and a new `AppendingReedSolomon` IDA pool, sending roughly `appended × width / k` bytes per append instead of the whole object.

**Architecture:** A new `AppendingReedSolomon` IDA lays objects out as rows of k cells of `w` bytes (cell i to data store i), with a zero-filled final row ending in an 8-byte big-endian logical length trailer. Because Reed-Solomon is linear, the client computes a per-store XOR delta for each append. Stores apply "zero-extend, then XOR at offset", guarded by the revision plus checks on slice length and trailer. Range reads are new slice-coordinate read types that the client decodes through an `AppendableIDA` trait, which Replication also implements.

**Tech Stack:** Scala 3, Backblaze JavaReedSolomon (`com.backblaze.erasure.ReedSolomon`), ScalaPB protobuf (`src/main/protobuf/codec.proto`), ScalaTest (`AnyFunSuite` / `AsyncFunSuite` + `Matchers`).

**Spec:** `docs/superpowers/specs/2026-10-23-data-object-append-design.md`

## Global Constraints

- The existing `ReedSolomon` IDA's behavior and on-disk format must not change. `ReedSolomonSuite` and `ReedSolomonIntegrationSuite` stay green unmodified.
- `AppendingReedSolomon` validation: `1 ≤ k ≤ width ≤ 127`, `k ≤ writeThreshold ≤ width`, `cellSize ≥ 8`. The default `cellSize` is **64**.
- Trailer: 8 bytes, big-endian, holding the logical length. It occupies the last 8 bytes of the final row, which are the last 8 bytes of data store k-1's slice.
- Append and range reads are supported for `Replication` and `AppendingReedSolomon` only. Classic `ReedSolomon` is rejected client-side before anything is sent.
- `append` never takes a raw length. It takes an `AppendPosition` that callers can't construct (its constructor is `private[aspen]`).
- Partial (range/tail) results are never cached, never used for opportunistic rebuild, and never yield rebuild data.
- Code style: Scala 3 quiet syntax, indented if/then/else, and os-lib for any filesystem work. Array-holding case classes compare and hash by content (see commit 85e1090).
- Run single tests with `sbt 'testOnly *SuiteName -- -z "substring"'`.

## Review Focus

1. **The trailer spills into a new row**: appending when the data leaves fewer than 8 bytes free in the final row, or exactly 0 free, or exactly 8 free. Expect the delta to clear the old trailer in the old final row and write the new one in the new final row. Pinned in Task 2 ("trailer boundary appends").
2. **Many tiny appends** (1 byte each, hundreds of times) must decode to the concatenation. Pinned in Task 2 ("long sequences of 1-byte appends") and Task 8.
3. **A range read ending exactly at, or past, the logical end** (offset + length > L, or offset == L) returns only real bytes and never zeros or trailer bytes. Pinned in Task 3 and Task 7.
4. **Appending from a stale AppendPosition** (another appender won) must abort, not corrupt. Pinned in Task 5 (checker) and Task 8 (concurrent appenders).
5. **A store left behind by an append** must vote abort on the next append and recover through repair. Later reads stay correct. Pinned in Task 8.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/common/ida/IDA.scala` (modify) | Adds `AppendingReedSolomon` (the sealed IDA hierarchy must stay in one file), the `AppendableIDA` trait, its Replication implementation, and the `RestoredRange` result type. |
| `src/main/scala/org/aspen_ddp/aspen/common/ida/AppendDelta.scala` (create) | The per-store append payload: the store-side `matches` and `applyTo` checks, and binary encode/decode. |
| `src/main/protobuf/codec.proto`, `common/network/Codec.scala` (modify) | Wire encoding for the new IDA and the new read types. |
| `common/metadata/BootstrapConfig.scala`, `cmdline/Main.scala` (modify) | Selecting the new IDA in bootstrap YAML and in `create-pool`/`bootstrap`. |
| `common/objects/ReadType.scala`, `server/store/PartialRead.scala` (modify) | Slice-coordinate range reads on stores. |
| `server/store/RequirementsChecker.scala`, `RequirementsApplyer.scala`, `common/transaction/RequirementError.scala`, `DataUpdateOperation.scala` (modify) | Store-side append. |
| `client/AppendPosition.scala` (create), `client/Transaction.scala`, `client/TransactionError.scala`, `client/internal/transaction/{TransactionImpl,TransactionBuilder}.scala` (modify) | Client-side append. |
| `client/ObjectState.scala`, `client/ReadError.scala`, `client/ObjectReader.scala`, `client/internal/BaseAspenClient.scala`, `client/internal/read/{BaseReadDriver,DataObjectRangeReader}.scala` (modify/create) | Client-side range reads. |
| `src/test/scala/org/aspen_ddp/aspen/AppendIntegrationSuite.scala` (create) | End-to-end tests on Replication and AppendingReedSolomon pools. |

---

### Task 1: AppendingReedSolomon encode/restore

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/ida/IDA.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/common/ida/AppendingReedSolomonSuite.scala`

**Interfaces:**
- Produces:
  - `case class AppendingReedSolomon(width: Int, restoreThreshold: Int, writeThreshold: Int, cellSize: Int = AppendingReedSolomon.DefaultCellSize) extends IDA`
  - `object AppendingReedSolomon { val DefaultCellSize: Int = 64; val TrailerSize: Int = 8 }`
  - On the class: `name == "appending-reed-solomon"`, `consistentRestoreThreshold == restoreThreshold`, `def rowBytes: Int = restoreThreshold * cellSize`, `def rowsFor(nbytes: Int): Int` (smallest r with `nbytes + 8 ≤ r × rowBytes`), and `calculateEncodedSegmentLength(n) == rowsFor(n) * cellSize`.
  - `IDA.AppendingReedSolomonCode: Byte = 2`. `serializeIDAType` writes `<code><width><k><writeThreshold>` followed by `cellSize` as a 4-byte int, so `getSerializedIDATypeLength == 8`. `deserializeIDAType` handles code 2.

Layout (from the spec): logical stream = content, then zeros, then an 8-byte BE length in the last 8 bytes of row `rowsFor(n) - 1`. Stream byte `p` goes to data shard `(p % rowBytes) / cellSize` at offset `(p / rowBytes) * cellSize + p % cellSize`. Parity comes from one `ReedSolomon.codecFor(k, width - k).encodeParity(shards, 0, sliceLength)` call when `width > k`. Restore recovers missing data shards exactly as `ReedSolomon.restoreToArray` does (validation, duplicate handling, `decodeMissing`). You may extract that recovery into a `private[ida]` helper shared by both classes, as long as `ReedSolomon`'s behavior is unchanged. It then reads the trailer, validates it, and gathers the content.

- [ ] **Step 1: Write the failing tests** in `AppendingReedSolomonSuite` (AnyFunSuite + Matchers), reusing `ReedSolomonSuite.Sizes`/`Generators`. Configs: `(1,1,1,8)`, `(3,1,2,8)`, `(3,2,3,8)`, `(4,4,4,16)`, `(5,3,4,64)`, `(6,3,5,8)`, `(8,4,6,64)`, `(10,7,9,8)`.
  - `"construction rejects invalid parameters"`: `cellSize = 7`, `k = 0`, `k > width`, `width = 128`, and `writeThreshold < k` each throw `IllegalArgumentException`.
  - `"segment length is rows times cellSize"`: for `(5,3,4,8)`, `calculateEncodedSegmentLength(0) == 8`, `(16) == 8`, `(17) == 16`, `(40) == 16`, `(41) == 24`. (rowBytes = 24; n + 8 ≤ 24 rows.)
  - `"encode then restore round trips"`: every config × size × generator; `restoreArray(all shards) sameElements content`.
  - `"restores from every k-subset of slices"`: for configs with `width ≤ 6` and sizes `{0, 1, rowBytes-9, rowBytes-8, rowBytes-7, 3*rowBytes+5, 1000}`.
  - `"data store k-1 holds the trailer in plain bytes"`: for each config and size n, `ByteBuffer.wrap(shards(k-1)).getLong(shards(k-1).length - 8) == n`.
  - `"layout places stream byte p at its row and cell"`: for `(5,3,4,8)` with content `0 until 100` (as bytes), the slice of shard `(p % 24) / 8` at `(p / 24) * 8 + p % 8` equals `p.toByte` for every `p < 100`.
  - `"restore rejects a corrupt trailer"`: setting shard k-1's trailer to `rows × rowBytes` (too large) and to a value whose `rowsFor` differs from the slice's rows throws `IDARestoreError`.
  - `"encodeInto matches encode"`: KV values travel through `encodeInto` (`KeyValueOperation.encodeGenericIda`). For each config and size, buffers of `calculateEncodedSegmentLength(n)` filled by `encodeInto` equal `encode`.
  - `"serializeIDAType round trips including cellSize"`: `IDA.deserializeIDAType` after `serializeIDAType` equals the original for `(5,3,4,64)` and `(3,2,3,4096)`.
- [ ] **Step 2: Run** `sbt 'testOnly *AppendingReedSolomonSuite'`. Expected: compilation FAILS (`AppendingReedSolomon` not found).
- [ ] **Step 3: Implement** `AppendingReedSolomon` and the serialize/deserialize code in `IDA.scala` as specified above.
- [ ] **Step 4: Run** `sbt 'testOnly *AppendingReedSolomonSuite *ReedSolomonSuite'`. Expected: all PASS.
- [ ] **Step 5: Commit** `git commit -m "Add AppendingReedSolomon IDA with row-interleaved layout"`.

---

### Task 2: AppendDelta and AppendableIDA append computation

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/ida/AppendDelta.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/ida/IDA.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/ida/AppendDeltaSuite.scala`

**Interfaces:**
- Consumes: `AppendingReedSolomon` (Task 1).
- Produces:
  ```scala
  final case class AppendDelta(expectedSliceLength: Int, newSliceLength: Int, xorOffset: Int,
                               xorBytes: Array[Byte], expectedTail: Option[Array[Byte]]):
    def matches(slice: DataBuffer): Boolean   // slice.size == expectedSliceLength && tail bytes equal expectedTail when present
    def applyTo(slice: DataBuffer): DataBuffer // require(matches); copy zero-extended to newSliceLength, XOR xorBytes in at xorOffset
    def encode: DataBuffer
  object AppendDelta:
    def decode(db: DataBuffer): AppendDelta    // throws IllegalArgumentException on malformed input
  ```
  `equals`/`hashCode` compare arrays by content.
  ```scala
  // in IDA.scala
  sealed trait AppendableIDA:
    this: IDA =>
    /** One delta per encoding index, for appending `data` (non-empty) to an object of logical length `currentLength` */
    def appendDeltas(currentLength: Int, data: Array[Byte]): Array[AppendDelta]
  ```
  Both `Replication` and `AppendingReedSolomon` mix in `AppendableIDA`. Classic `ReedSolomon` does not.

Delta rules (from the spec):
- **Replication:** every index gets `AppendDelta(L, L + m, L, data, None)`.
- **AppendingReedSolomon:** `oldRows = rowsFor(L)`, `newRows = rowsFor(L + m)`, `startRow = L / rowBytes`, `base = startRow × rowBytes`. Start from a zero delta stream of `(newRows - startRow) × rowBytes` bytes. XOR in `data` at `L - base`, the BE bytes of `L` at `oldRows × rowBytes - 8 - base`, and the BE bytes of `L + m` at `newRows × rowBytes - 8 - base`. Scatter the stream into k data cells using the Task 1 layout and compute parity. Index i then gets `AppendDelta(oldRows × cellSize, newRows × cellSize, startRow × cellSize, shard_i, expectedTail)`, where `expectedTail = Some(BE bytes of L)` for index k-1 and `None` otherwise.

- [ ] **Step 1: Write the failing tests** in `AppendDeltaSuite`. Use the Task 1 configs plus `Replication(3,2)` and the helper `appendAll(ida, initial, appends): Array[Array[Byte]]`, which encodes `initial`, then for each append applies `ida.appendDeltas(len, a)(i).applyTo` to every slice.
  - `"delta-applied slices equal a fresh encode"`: for every config, each initial size in `{0, 1, 15, 16, 17, 100, 1000}`, and append sizes in `{1, 7, 8, 9, rowBytes, 1024}`, `appendAll(...)` equals `ida.encode(initial ++ append)` slice by slice.
  - `"trailer boundary appends"`: for `(5,3,4,8)` (rowBytes 24), initial lengths 15, 16, 17, and 24 each followed by appends of 1, 8, and 9 bytes match a fresh encode. This exercises a trailer that fits exactly, one that spills into a new row, and one in an already-new row.
  - `"long sequences of 1-byte appends"`: 300 one-byte appends onto an empty object for `(5,3,4,8)`, `(3,1,2,8)`, and `Replication(3,2)` restore to the 300 bytes.
  - `"random append sequences"`: a seeded `Random(0xA99E)`, 50 runs of 1-40 appends of 1-3000 bytes on random configs, restoring from a random k-subset.
  - `"delta only touches rows from the old final row onward"`: for `(5,3,4,64)` with L = 10000 and m = 1024, `xorOffset == (10000 / 192) * 64` and `xorBytes.length == newSliceLength - xorOffset`.
  - `"matches rejects wrong slice length and wrong trailer"`: the delta for index k-1 doesn't match a slice one row shorter, nor a correct-length slice whose last byte has been flipped. For indices other than k-1, a flipped trailer byte still matches.
  - `"applyTo throws when the delta does not match"`.
  - `"encode/decode round trips"` with and without `expectedTail`, and `"decode rejects truncated input"`.
- [ ] **Step 2: Run** `sbt 'testOnly *AppendDeltaSuite'`. Expected: compilation FAILS.
- [ ] **Step 3: Implement** `AppendDelta` (binary layout is your call; use a leading format byte) and `appendDeltas` for both IDAs.
- [ ] **Step 4: Run** `sbt 'testOnly *AppendDeltaSuite *AppendingReedSolomonSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Compute per-store append deltas for appendable IDAs"`.

---

### Task 3: AppendableIDA range mapping and range restore

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/ida/IDA.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/ida/AppendableRangeSuite.scala`

**Interfaces:**
- Consumes: Tasks 1-2.
- Produces, on `AppendableIDA`:
  ```scala
  /** Row-aligned slice offset at which logical byte `offset` begins */
  def sliceOffsetFor(offset: Int): Int
  /** Row-aligned slice range [start, end) covering logical [offset, offset + length) */
  def sliceRangeFor(offset: Int, length: Int): (Int, Int)
  /** Bytes from the end of each slice that are needed to learn the logical length: cellSize, or 0 for Replication */
  def lengthProbeBytes: Int
  /** Decodes equal-length slice segments that start at slice offset `sliceStart`, where `sliceSize` is the full slice length */
  def restoreRange(sliceStart: Int, sliceSize: Int, segments: List[(Byte, Array[Byte])]): RestoredRange
  ```
  `final case class RestoredRange(logicalOffset: Int, data: Array[Byte], objectLength: Option[Int])` (content equality).
  - **Replication:** the slice range equals the logical range, `restoreRange` returns `(sliceStart, segment, Some(sliceSize))`, and `lengthProbeBytes == 0`.
  - **AppendingReedSolomon:** `logicalOffset = (sliceStart / cellSize) × rowBytes`. If `sliceStart + segmentLength == sliceSize`, the rows include the final row: read the trailer L, validate it as in Task 1, truncate data to `max(0, L - logicalOffset)`, and set `objectLength = Some(L)`. Otherwise return all gathered bytes with `objectLength = None`. `sliceStart` must be a multiple of `cellSize`; anything else throws `IDARestoreError`.

- [ ] **Step 1: Write the failing tests** in `AppendableRangeSuite`, slicing fully encoded fragments to simulate stores:
  - `"every row-aligned window decodes to the matching logical bytes"`: `(5,3,4,8)` and `(3,2,3,16)`, content sizes `{0, 23, 24, 100, 777}`, every `(startRow, endRow)` pair. The result's data equals `content.slice(logicalOffset, logicalOffset + data.length)`. `objectLength == Some(n)` exactly when `endRow` is the last row.
  - `"windows ending past the logical end return only real bytes"`: the final-row window for n = 100 returns `data.length == 100 - logicalOffset`.
  - `"length probe of the final cellSize bytes yields the length"`: restoring only the last `lengthProbeBytes` of each slice gives `objectLength == Some(n)` for every config and size.
  - `"sliceRangeFor covers the requested logical range"`: for random offset/length, the decoded window contains `[offset, offset+length)`.
  - `"Replication range restore is the identity"`.
  - `"restoreRange rejects a misaligned start"`.
- [ ] **Step 2: Run** `sbt 'testOnly *AppendableRangeSuite'`. Expected: compilation FAILS.
- [ ] **Step 3: Implement** the four methods for both IDAs.
- [ ] **Step 4: Run** `sbt 'testOnly *AppendableRangeSuite *AppendDeltaSuite *AppendingReedSolomonSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Map and restore byte ranges for appendable IDAs"`.

---

### Task 4: IDA and read-type plumbing (codec, bootstrap, CLI, store reads)

**Files:**
- Modify: `src/main/protobuf/codec.proto`, `common/network/Codec.scala`, `common/metadata/BootstrapConfig.scala`, `cmdline/Main.scala`, `common/objects/ReadType.scala`, `server/store/PartialRead.scala`
- Test: `common/network/CodecRoundTripSuite.scala`, `common/metadata/BootstrapConfigSuite.scala`, `cmdline/MainSuite.scala`, `server/store/PartialReadSuite.scala`

**Interfaces:**
- Produces:
  - proto: `message AppendingReedSolomon { uint32 width = 1; uint32 restore_threshold = 2; uint32 write_threshold = 3; uint32 cell_size = 4; }` with `AppendingReedSolomon appending_reed_solomon = 3;` in the `IDA` oneof. Two new `ReadType` enum values: `READ_TYPE_BYTE_RANGE_TO_END = 8` and `READ_TYPE_SLICE_TAIL = 9`, both reusing `Read.offset`/`Read.length`.
  - `case class ByteRangeToEnd(offset: Int) extends ReadType` and `case class SliceTail(length: Int) extends ReadType`. All three byte-range read types are in **slice coordinates**.
  - Bootstrap YAML type `appending-reed-solomon` with the same attrs as `reed-solomon` plus optional `cell-size` (default 64). `generateBootstrapConfig` writes `read-threshold` and `cell-size` for it.
  - CLI: `validateIdaType` accepts `appending-reed-solomon`. New `opt[Int]("cell-size")` on `bootstrap` and `create-pool` (default 64), and `createIDA` builds `AppendingReedSolomon`.
  - `PartialRead.filter` for `ObjectType.Data` returns the clipped slice. `ByteRange(o, l)` gives `[min(o,size), min(o+l,size))`, `ByteRangeToEnd(o)` gives `[min(o,size), size)`, and `SliceTail(n)` gives the last `min(n,size)` bytes. KeyValue objects with these types are unchanged (full data).

- [ ] **Step 1: Write the failing tests:**
  - CodecRoundTripSuite: `"AppendingReedSolomon round-trip"` (including `cellSize = 4096`), and the `Read` round-trip for `ByteRange(5, 10)`, `ByteRangeToEnd(64)`, and `SliceTail(64)`.
  - BootstrapConfigSuite: `"appending-reed-solomon bootstrap IDA round-trips including cell-size"` and `"cell-size defaults to 64"`.
  - MainSuite: `"createIDA builds AppendingReedSolomon with the requested cell size"`, plus `"validateIdaType accepts appending-reed-solomon"`.
  - PartialReadSuite: `"ByteRange clips to the slice"` (data of 100 bytes; `ByteRange(90, 20)` returns bytes 90-99; `ByteRange(200, 5)` returns empty), `"ByteRangeToEnd returns the suffix"`, and `"SliceTail returns the last n bytes, or the whole slice when shorter"`.
- [ ] **Step 2: Run** `sbt 'testOnly *CodecRoundTripSuite *BootstrapConfigSuite *MainSuite *PartialReadSuite'`. Expected: compilation FAILS.
- [ ] **Step 3: Implement** the proto, codec, bootstrap format, CLI, read types, and `PartialRead` cases. Update the `<ida-type>` help text to list all three types.
- [ ] **Step 4: Re-run the same command.** Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Wire AppendingReedSolomon and slice byte-range reads through codec, config, CLI and stores"`.

---

### Task 5: Store-side append

**Files:**
- Modify: `server/store/RequirementsChecker.scala`, `server/store/RequirementsApplyer.scala`, `common/transaction/RequirementError.scala`, `common/transaction/DataUpdateOperation.scala`
- Test: `server/store/RequirementsCheckerSuite.scala`, `server/store/RequirementsApplyerSuite.scala`

**Interfaces:**
- Consumes: `AppendDelta.decode`, `matches`, and `applyTo` (Task 2).
- Produces: `case class AppendMismatch() extends RequirementError`, which replaces `UnsupportedOperation` (delete it, since it has no other users). For a `DataUpdate` with `Append`, the checker checks revision exactly as for Overwrite and requires `objectUpdates` to contain the object. It then decodes the payload, records `AppendMismatch` when decoding fails or `!delta.matches(state.data)`, and the applyer sets `s.data = delta.applyTo(s.data)` along with the revision and timestamp, as for Overwrite.

- [ ] **Step 1: Write the failing tests** (replacing the existing "Append is rejected" tests in both suites):
  - Checker: `"Append with matching revision and delta passes"`, `"Append with wrong revision fails with RevisionMismatch"`, `"Append whose expected slice length differs fails with AppendMismatch"`, `"Append whose expected trailer differs fails with AppendMismatch"`, `"Append with an undecodable payload fails with AppendMismatch"`, and `"Append without an object update fails with MissingObjectUpdate"`.
  - Applyer: `"Append XORs the delta into the zero-extended slice and bumps the revision"`. Build state from `AppendingReedSolomon(5,3,4,8).encode(content)(i)`, apply `appendDeltas(n, more)(i)`, and expect data equal to `encode(content ++ more)(i)` and revision `ObjectRevision(txid)`. Also `"a mismatched Append leaves state untouched"`.
- [ ] **Step 2: Run** `sbt 'testOnly *RequirementsCheckerSuite *RequirementsApplyerSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement**, and rewrite the `DataUpdateOperation.Append` doc comment to describe the delta semantics: the payload is an encoded `AppendDelta`, and the operation is valid only under appendable IDAs.
- [ ] **Step 4: Re-run.** Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Apply append deltas on stores"`.

---

### Task 6: Client-side append

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/AppendPosition.scala`
- Modify: `client/Transaction.scala`, `client/TransactionError.scala`, `client/internal/transaction/TransactionImpl.scala`, `client/internal/transaction/TransactionBuilder.scala`
- Test: `client/internal/transaction/TransactionBuilderSuite.scala`

**Interfaces:**
- Consumes: `AppendableIDA.appendDeltas`, `AppendDelta.encode`.
- Produces:
  - `final case class AppendPosition private[aspen] (pointer: DataObjectPointer, revision: ObjectRevision, length: Int)`
  - `Transaction.append(position: AppendPosition, data: DataBuffer): AppendPosition` returns the position that holds once this transaction commits, `AppendPosition(position.pointer, this.revision, position.length + data.size)`. It throws `IllegalArgumentException` on empty data.
  - `TransactionBuilder.append(position: AppendPosition, data: DataBuffer): Unit`. It applies the same `MultipleDataUpdatesToObject`/`ConflictingRequirements` checks as `overwrite` and adds `DataUpdate(pointer, position.revision, DataUpdateOperation.Append)`. At build time it computes `ida.appendDeltas(position.length, data)` per store and passes their encodings to `addUpdate`.
  - `AppendNotYetSupported` is renamed to `final case class AppendNotSupported(objectPointer: ObjectPointer, ida: IDA) extends TransactionCreationError`. It's thrown at build time when the pool's IDA isn't an `AppendableIDA`.

- [ ] **Step 1: Write the failing tests** in TransactionBuilderSuite (replacing the AppendNotYetSupported test):
  - `"append adds an Append DataUpdate with the position's revision"`.
  - `"append sends each store its own encoded delta"`: under `AppendingReedSolomon(5,3,4,8)` with L = 30, every store's `ObjectUpdate` decodes to `appendDeltas(30, data)(i)`.
  - `"append and overwrite of the same object conflict"`.
  - `"building an append for a classic ReedSolomon pool throws AppendNotSupported"`.
  - In a small unit test against `TransactionImpl`, or wherever revision is accessible: `"append returns the post-commit position"`, with `length == L + m` and `revision == tx.revision`.
- [ ] **Step 2: Run** `sbt 'testOnly *TransactionBuilderSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement.** Update every caller of the old `append` signature, if any (compile errors will show them).
- [ ] **Step 4: Re-run.** Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Build append transactions from AppendPosition"`.

---

### Task 7: Client range reads and append-position reads

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/DataObjectRangeReader.scala`
- Modify: `client/ObjectState.scala`, `client/ReadError.scala`, `client/ObjectReader.scala`, `client/internal/BaseAspenClient.scala`, `client/internal/read/BaseReadDriver.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/DataObjectRangeReaderSuite.scala` (unit), with end-to-end coverage in Task 8.

**Interfaces:**
- Consumes: `AppendableIDA.sliceOffsetFor`, `sliceRangeFor`, `lengthProbeBytes`, and `restoreRange` (Task 3), the read types (Task 4), and `AppendPosition` (Task 6).
- Produces:
  - In `ObjectState.scala`: `class DataObjectRangeState(pointer: DataObjectPointer, revision, refcount, timestamp, readTimestamp, val ida: IDA, val offset: Int, val data: DataBuffer, val objectLength: Option[Int]) extends ObjectState`, with `getRebuildDataForStore = None` and `def appendPosition: Option[AppendPosition] = objectLength.map(AppendPosition(pointer, revision, _))`.
  - `class RangeReadNotSupported(val pointer: ObjectPointer, val ida: IDA) extends FatalReadError("RangeReadNotSupported")`.
  - On `ObjectReader` (client trait), each with a no-comment overload:
    - `def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int], comment: String): Future[DataObjectRangeState]`. A `length` of None means to the end. The result's `offset == offset` and `data` is exactly the logical bytes `[offset, min(offset + length, L))`, where L is known.
    - `def readAppendPosition(pointer: DataObjectPointer, comment: String): Future[AppendPosition]`.
  - `BaseAspenClient` resolves the IDA and fails with `RangeReadNotSupported` unless it's an `AppendableIDA`. Otherwise it issues `ByteRange(start, end - start)` (bounded), `ByteRangeToEnd(sliceOffsetFor(offset))`, or `SliceTail(lengthProbeBytes)` (for the position), then trims the `DataObjectRangeState` to the requested window.
  - `BaseReadDriver`: `isPartial` also covers `ByteRange | ByteRangeToEnd | SliceTail`, and `(p: DataObjectPointer, ByteRange | ByteRangeToEnd | SliceTail)` maps to `new DataObjectRangeReader(p, ida, readUUID, readType)`.
  - `DataObjectRangeReader` extends `BaseObjectReader[DataObjectPointer, DataObjectStoreState]` with `metadataOnly = false`. In `restoreObject`, `sliceSize` is the matching states' `sizeOnStore` and `sliceStart` comes from the read type (`SliceTail(n)` gives `sliceSize - min(n, sliceSize)`, the others their offset clipped to `sliceSize`). It calls `restoreRange` and builds a `DataObjectRangeState`. An `IDARestoreError` becomes `NotRestorable`.
- [ ] **Step 1: Write the failing tests** in `DataObjectRangeReaderSuite`. Feed hand-built `ReadResponse.CurrentState`s (sliced from `AppendingReedSolomon(5,3,4,8).encode(content)`, same revision) into a reader. Cover:
  - `"ByteRangeToEnd restores the tail and the object length"`.
  - `"SliceTail of cellSize yields the object length with no data beyond L"`.
  - `"bounded ByteRange not reaching the final row has no objectLength"`.
  - `"responses at differing revisions restore only from the matching set"`: two stores at an older revision with a shorter `sizeOnStore`, three at the newest.
  - `"range results never provide rebuild data"`.
  Mirror the response construction in existing reader tests, if any; otherwise build `ReadResponse` directly.
- [ ] **Step 2: Run** `sbt 'testOnly *DataObjectRangeReaderSuite'`. Expected: compilation FAILS.
- [ ] **Step 3: Implement** the reader, state, error, driver wiring, and client methods. Any `AspenClient` test doubles that fail to compile get delegating stubs.
- [ ] **Step 4: Run** `sbt 'testOnly *DataObjectRangeReaderSuite *BasicIntegrationSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Add byte-range and append-position reads for data objects"`.

---

### Task 8: End-to-end integration, KV coverage, and docs

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/AppendIntegrationSuite.scala`
- Modify: `CLAUDE.md` (IDA bullet: "Currently only Replication and ReedSolomon are supported" becomes a mention of AppendingReedSolomon and its append/range-read support), `Todo.txt` (remove the top item)

**Interfaces:**
- Consumes: the whole client API from Tasks 6-7. Use the `IntegrationTestSuite` harness (`testIda`, `atest`, `net.waitForTransactionsToComplete()`, `net.stores(i).m`, and the read-silencing helpers used by `ReedSolomonIntegrationSuite`).
- Produces: `trait AppendIntegrationTests { this: IntegrationTestSuite => ... }` and the concrete suites `ReplicationAppendIntegrationSuite` (default `Replication(3,2)`), `AppendingReedSolomonAppendIntegrationSuite` (`AppendingReedSolomon(5,3,4,64)`), and `NarrowAppendingReedSolomonAppendIntegrationSuite` (`AppendingReedSolomon(4,2,3,8)`). Also `class AppendingReedSolomonBasicIntegrationSuite extends BasicIntegrationSuite { override def testIda = AppendingReedSolomon(5,3,4,64) }` to prove KV objects (including KV partial reads) work in these pools. Finally, `class ClassicReedSolomonAppendRejectionSuite extends IntegrationTestSuite` with `testIda = ReedSolomon(5,3,4)`.

- [ ] **Step 1: Write the tests** in the trait. Each one allocates a DataObject and links it to the radicle, as `ReedSolomonIntegrationTests` does.
  - `"single writer carries the position across 50 appends"`: start from `readAppendPosition`, then for each of 50 appends of 1-2000 bytes (seeded), call `tx.append(pos, chunk)`, commit, and use the returned position. A full `read` then equals the concatenation.
  - `"independent appender learns the position with a tail read"`: interleave appends that each call `readAppendPosition` first. The final content matches.
  - `"long sequence of 1-byte appends"`: 200 single-byte appends; a full read matches.
  - `"range reads return exact logical bytes"`: after building a 20 KiB object by appends, `readRange` checks offsets 0, 1, rowBytes-1, rowBytes, L-1, L, and L+100, with lengths None, 0, 1, 500, and past-end. Each data must equal `content.slice(offset, min(offset+len, L))`. `objectLength`/`appendPosition` must be defined whenever the range reaches the end.
  - `"tailing follows new appends"`: `readRange(pointer, lastSeen, None)` after each append returns exactly the new bytes, and its `appendPosition` equals what the append returned.
  - `"stale position aborts and leaves content intact"`: two transactions append from the same position. Exactly one commits, the other's `commit()` fails, and a full read equals base plus the winner's chunk.
  - `"a store behind on appends is repaired and later appends still succeed"`: after 20 appends, do a full `read` (which drives opportunistic rebuild), then `waitForTransactionsToComplete`. Assert eventually every store holds the newest revision (retry briefly using the harness's existing polling style, if any). Then do 5 more appends and check a degraded read over every silenceable subset.
  - ClassicReedSolomonAppendRejectionSuite: `"readAppendPosition fails with RangeReadNotSupported"` and `"committing an append fails with AppendNotSupported"` (it constructs `AppendPosition` directly, since it's in `org.aspen_ddp.aspen`).
- [ ] **Step 2: Run** `sbt 'testOnly *AppendIntegrationSuite *AppendingReedSolomonBasicIntegrationSuite *ClassicReedSolomonAppendRejectionSuite'`. Expected: PASS. If something fails, debug it with superpowers:systematic-debugging; don't weaken assertions.
- [ ] **Step 3: Update** `CLAUDE.md` and `Todo.txt` as listed above.
- [ ] **Step 4: Run the full suite** with `sbt test`. Expected: all PASS, apart from the known-flaky QUIC tests listed in `Todo.txt`. Re-run any QUIC failure once and report it if it persists.
- [ ] **Step 5: Commit** `git commit -m "End-to-end tests for data object append and range reads"`.
