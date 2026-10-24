# Ranged Writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a transaction overwrite byte ranges of a DataObject in Replication and AppendingReedSolomon pools, sending only the changed bytes.

**Architecture:** Same XOR-delta technique as append. The client computes, per store, a delta of `old XOR new` through the pool's IDA (Reed-Solomon is linear, so XORing the encoded delta into each slice yields the new object's slices). The caller supplies the old bytes up front as a typed `RangedWriteState`, obtained from the full object or from a client helper, so `Transaction` stays synchronous. The delta is sparse: a list of XOR regions per store, so distant edits never send the bytes between them. Stores apply it with an extended `AppendDelta` behind a new `DataUpdateOperation.WriteRange`.

**Tech Stack:** Scala 3, sbt, protobuf codec, Scaffeine/os-lib per CLAUDE.md. Follow the "quiet" Scala 3 syntax and indented if/then/else rules in `CLAUDE.md`.

**Spec:** `docs/superpowers/specs/2026-10-23-ranged-writes-design.md` (append background: `docs/superpowers/specs/2026-10-23-data-object-append-design.md`)

All source paths below are relative to `src/main/scala/org/aspen_ddp/aspen/` and test paths to `src/test/scala/org/aspen_ddp/aspen/`.

## Global Constraints

- Classic `ReedSolomon` is unsupported: fails at build time with `RangeWriteNotSupported` (new, modelled on `AppendNotSupported` in `client/TransactionError.scala:19`).
- `zeroFill` defaults to `false`; it is a parameter of the call, not of each write.
- Overlapping writes in one call throw immediately from `rangedWrites`. Touching (adjacent) writes are not overlapping.
- An object may be modified once per transaction: use the same `updatingObjects` / `revisionLocks` checks as `append` (`TransactionBuilder.scala:183-193`).
- `maxObjectSize` (on `StoragePoolState` / `StoragePool`) is enforced on the client at build time only; the resulting length may equal but not exceed it.
- For AppendingReedSolomon keep the bound `newLength <= Int.MaxValue - rowBytes`.
- All `Transaction` methods stay synchronous. No new backend operations.
- Deltas are multi-region (sparse). Bytes between distant edits must not be sent. For AppendingReedSolomon the unit is the affected rows' cells per store (at most `cellSize` bytes per affected row per store, trimmed to the touched span where convenient); for Replication it is exactly the edited bytes.
- No alignment-expanded old-data reads: parity deltas come from the data deltas alone (linearity), so only the old bytes under each write plus the object length are needed.

## Review Focus

- Two far-apart edits: each store's delta contains no bytes from the untouched rows between them (assert on encoded delta size).
- Mixed-revision fetch: the helper's parallel reads return different revisions. Expected: retry, then fail; never build a state whose old bytes belong to a different revision than the one claimed (that would silently corrupt parity).
- Write starting exactly at the current length (`offset == length`) is an extension, not a gap: allowed with `zeroFill = false`.
- Object of length 0 (bare allocation): first ranged write at offset 0 works in both IDAs.
- Extension that moves the trailer into a new AppendingReedSolomon row, and a write ending exactly on a row boundary.
- `newLength == maxObjectSize` succeeds; `maxObjectSize + 1` fails.

---

### Task 1: Multi-region AppendDelta

**Files:**
- Modify: `common/ida/AppendDelta.scala` (whole file)
- Test: `common/ida/AppendDeltaSuite.scala`

**Interfaces:**
- Produces: `final case class XorRegion(offset: Int, bytes: Array[Byte])` and `AppendDelta(expectedSliceLength: Int, newSliceLength: Int, regions: List[XorRegion], expectedTail: Option[Array[Byte]])`. Invariants (`require`): regions sorted by offset, non-overlapping, non-empty bytes, offsets `>= 0`, every region ends `<= newSliceLength`, `0 <= expectedSliceLength <= newSliceLength`, tail no longer than `expectedSliceLength`. A region may begin beyond `expectedSliceLength` (it lands in the zero-extended area).
- Produces: a companion `AppendDelta.apply(expectedSliceLength, newSliceLength, xorOffset, xorBytes, expectedTail)` single-region overload so existing append callers (`Replication.appendDeltas`, `AppendingReedSolomon.appendDeltas`) and tests compile unchanged.
- Wire format: encode as `FormatVersion = 2` (region count + per-region offset/length + bytes + tail). `decode` still accepts version 1 (becomes one region), so already-written CRL data and old tests keep working. Decode failures stay `IllegalArgumentException`.
- `matches` and `applyTo` semantics unchanged apart from applying every region after zero-extending to `newSliceLength`.

- [ ] **Step 1: Write failing tests:** (a) two regions at distant offsets with `newSliceLength == expectedSliceLength` XOR only those bytes in `applyTo`; (b) v2 encode/decode round trip, including a region starting beyond `expectedSliceLength`; (c) decoding a hand-built version 1 payload yields one region; (d) overlapping, unsorted, empty-region and region-beyond-`newSliceLength` inputs throw `IllegalArgumentException`; (e) all existing tests still pass.
- [ ] **Step 2:** Run `sbt 'testOnly *AppendDeltaSuite'`. Expected: FAIL (compile).
- [ ] **Step 3:** Implement per the Interfaces block. Update `equals`/`hashCode` for the region list and the class doc.
- [ ] **Step 4:** Run `sbt 'testOnly *AppendDeltaSuite *RequirementsApplyerSuite *RequirementsCheckerSuite *AppendingReedSolomonSuite'`. Expected: PASS.
- [ ] **Step 5:** Commit `Make AppendDelta multi-region`.

### Task 2: IDA ranged-write deltas

**Files:**
- Modify: `common/ida/IDA.scala` (`AppendableIDA` trait ~line 45; `Replication` ~151; `AppendingReedSolomon` ~437-680)
- Create: `common/ida/RangeEdit.scala`
- Test: `common/ida/AppendingReedSolomonSuite.scala`, a Replication case in the same package (new `RangedWriteDeltaSuite.scala` is fine)

**Interfaces:**
- Produces: `final case class RangeEdit(offset: Int, newBytes: Array[Byte], oldBytes: Array[Byte])` where `oldBytes.length == max(0, min(newBytes.length, currentLength - offset))` (the overlap with existing content; empty beyond the end).
- Produces: on `AppendableIDA`: `def rangedWriteDeltas(currentLength: Int, edits: List[RangeEdit]): Array[AppendDelta]` — one delta per encoding index. Preconditions (`require`): edits non-empty, sorted-or-not but non-overlapping, offsets `>= 0`, `offset <= currentLength` is NOT required (a gap is zero-filled implicitly because the extended slice region is already zero and the delta carries zeros there). New length is `max(currentLength, max(offset + newBytes.length))`.

- [ ] **Step 1: Write failing tests.** Core property test for ARS (several `(k, nparity, cellSize)` combos including `cellSize` small like 4 so rows are tiny): for random object content and random edit sets (within a row, spanning rows, ending exactly on a row boundary, extending past the end, extending so the trailer moves to a new row, starting at `offset == length`, starting beyond length with a gap, `currentLength == 0`), assert `applyTo`-ing each delta (via `AppendDelta.decode(delta.encode)`) onto the slices of `encode(oldContent)` yields exactly the slices of `encode(expectedNewContent)` for every encoding index. Same property for `Replication`. Plus: deltas for a non-extending write have `newSliceLength == expectedSliceLength`; two edits many rows apart produce deltas whose encoded size is bounded by `affectedRows * cellSize + header` per store (no bytes from the rows between); Replication deltas carry exactly the edited bytes.
- [ ] **Step 2:** Run `sbt 'testOnly *RangedWriteDeltaSuite *AppendingReedSolomonSuite'`. Expected: FAIL (method missing).
- [ ] **Step 3: Implement `Replication.rangedWriteDeltas`:** one region per edit (merge regions that touch), `expectedSliceLength = currentLength`, `newSliceLength = newLength`, no tail. Region bytes are `old ^ new` over the overlap with existing content and `new` beyond `currentLength`. A gap between the old end and an edit needs no bytes (it is zero after extension).
- [ ] **Step 4: Implement `AppendingReedSolomon.rangedWriteDeltas`** sparsely, row by row. Find the set of affected rows (rows containing an edited byte, plus the old and new trailer rows when the length changes). For each affected row build that row's `rowBytes` XOR stream (edits' `old ^ new`, `xorLong` of the old and new trailer where they fall), `scatter` + `addParity` it into `cellSize`-byte cells per encoding index, and give each store a `XorRegion(row * cellSize, cell)` (optionally trimmed to the nonzero span; a whole cell is fine). Merge regions that are adjacent in slice offset. Each store gets `AppendDelta(oldRows*cellSize, newRows*cellSize, regions, tail = if i == k-1 then Some(oldTrailerBytes) else None)` (the trailer tail is always sent for shard k-1; it is cheap and keeps the store-side length check). Rows between distant edits contribute nothing. Keep the overflow `require`. `appendDeltas` may be reimplemented on top of this if it falls out naturally, but its output must stay byte-for-byte compatible with the existing append tests.
- [ ] **Step 5:** Run the suites. Expected: PASS, including existing append tests.
- [ ] **Step 6:** Commit `Add rangedWriteDeltas to Replication and AppendingReedSolomon`.

### Task 3: WriteRange operation on the wire and in the stores

**Files:**
- Modify: `common/transaction/DataUpdateOperation.scala`, `src/main/protobuf/codec.proto:84-87`, `common/network/Codec.scala:166,170`, `server/store/RequirementsChecker.scala:~44-65`, `server/store/RequirementsApplyer.scala:~47-60`
- Test: `common/network/CodecRoundTripSuite.scala` (~103, ~199), `server/store/RequirementsCheckerSuite.scala` (~90), `server/store/RequirementsApplyerSuite.scala` (~144)

**Interfaces:**
- Consumes: multi-region `AppendDelta` (Task 1).
- Produces: `DataUpdateOperation.WriteRange`; stores treat it exactly like `Append` (decode `AppendDelta`, `matches`, `applyTo`), mismatch -> existing `AppendMismatch`.

- [ ] **Step 1: Write failing tests:** codec round trip of `DataUpdate(ptr, rev, WriteRange)`; checker accepts a matching `WriteRange` payload and returns `AppendMismatch` for a wrong slice length and `MissingObjectUpdate` for no payload; applier applies a mid-slice delta (length unchanged) and leaves state untouched on mismatch.
- [ ] **Step 2:** Run `sbt 'testOnly *CodecRoundTripSuite *RequirementsCheckerSuite *RequirementsApplyerSuite'`. Expected: FAIL (compile).
- [ ] **Step 3:** Add the enum value, proto `WRITE_RANGE = 2`, both `Codec` mappings, and extend the `Append` cases in checker and applier to `Append | WriteRange`. Update the `DataUpdateOperation` doc comment. The compiler flags any non-exhaustive match; fix each.
- [ ] **Step 4:** Run those suites plus `sbt compile`. Expected: PASS.
- [ ] **Step 5:** Commit `Add DataUpdateOperation.WriteRange`.

### Task 4: Client types and Transaction.rangedWrites

**Files:**
- Create: `client/RangedWrite.scala` (holds `RangedWrite` and `RangedWriteState`)
- Modify: `client/TransactionError.scala` (add `RangeWriteNotSupported`, plus errors for gap-without-zeroFill and exceeding max size, each carrying the pointer), `client/Transaction.scala`, `client/internal/transaction/TransactionImpl.scala:61-67`, `client/internal/transaction/TransactionBuilder.scala` (new `rangedWrites`, build step near line 145, new `rangedUpdates` map), and every other `Transaction` implementer (grep `extends Transaction` / `with Transaction`, including test fakes)
- Test: `client/internal/transaction/TransactionRangedWriteSuite.scala` (model on `TransactionAppendSuite.scala` and `TransactionBuilderSuite.scala`)

**Interfaces:**
- Produces: `final case class RangedWrite(offset: Int, data: Array[Byte])`.
- Produces: `final class RangedWriteState private[aspen] (val pointer: DataObjectPointer, val revision: ObjectRevision, val length: Int, ...)` with `def oldBytes(offset: Int, length: Int): Array[Byte]` returning the existing bytes in `[offset, offset+length)` clipped to the object length, and throwing `IllegalArgumentException` if the state does not cover that range. Companion constructors: `RangedWriteState.fromObject(state: DataObjectState)` (full content, no extra read) and `RangedWriteState.fromRanges(...)` used by Task 5. `DataObjectRangeState` instances whose `objectLength` is known and whose range covers the whole object may also feed `fromObject`-style construction; builder's call.
- Produces: `Transaction.rangedWrites(state: RangedWriteState, writes: List[RangedWrite], zeroFill: Boolean = false): Unit` and `Transaction.rangedWrite(state: RangedWriteState, write: RangedWrite, zeroFill: Boolean = false): Unit = rangedWrites(state, List(write), zeroFill)`. Pointer and revision come from the state, mirroring `append(position, data)`.

- [ ] **Step 1: Write failing tests** (names are the contract):
  - "overlapping writes throw immediately" (e.g. `(0,"abcd")` and `(3,"x")`), and "touching writes are accepted".
  - "empty write list, empty write data and negative offset throw IllegalArgumentException".
  - "second update to the same object throws MultipleDataUpdatesToObject".
  - "building with a classic ReedSolomon pool fails with RangeWriteNotSupported".
  - "gap without zeroFill fails at build; with zeroFill builds"; "offset equal to length builds without zeroFill".
  - "result longer than maxObjectSize fails; exactly maxObjectSize builds".
  - "the requirement is DataUpdate(ptr, state.revision, WriteRange)".
  - "state missing old bytes for a written range throws".
- [ ] **Step 2:** Run `sbt 'testOnly *TransactionRangedWriteSuite'`. Expected: FAIL (compile).
- [ ] **Step 3: Implement.** `rangedWrites` in the builder validates (overlap via sort by offset, then `prev.end <= next.offset`) and applies the `updatingObjects`/`revisionLocks` checks, records into a `rangedUpdates` map, and prepends the `DataUpdate(..., WriteRange)` requirement. In the build step, mirror the `appendUpdates` branch: pattern-match `IDA & AppendableIDA` else throw `RangeWriteNotSupported`; run the gap, max-size and (ARS) overflow checks; call `rangedWriteDeltas(state.length, edits)` with `RangeEdit`s from `state.oldBytes`; `addUpdate(ptr, deltas.map(_.encode))`. Plumb the pool's `maxObjectSize` into the builder next to `poolIDAMap` (inspect where the builder is constructed and how `poolIDAMap` is populated, and add a parallel lookup; `Option[Int]`). `TransactionImpl.rangedWrites` delegates inside `modify` like `append`.
- [ ] **Step 4:** Run the suite and `sbt compile`/`Test/compile`. Expected: PASS.
- [ ] **Step 5:** Commit `Add Transaction.rangedWrites`.

### Task 5: Multi-range read and state-fetching helper

**Files:**
- Modify: `client/ObjectReader.scala:44-52` (add `readRanges` with the same comment/no-comment overload pair as `readRange`), `client/AspenClient.scala` if the helper lives there, `client/internal/BaseAspenClient.scala:137-171`, any other `ObjectReader`/`AspenClient` implementers (grep; includes test fakes)
- Test: `client/internal/BaseAspenClientRangedSuite.scala` or a case in `AppendIntegrationSuite`'s style (needs a test network; see `AppendIntegrationSuite.scala:93`)

**Interfaces:**
- Produces: `ObjectReader.readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)], comment: String): Future[List[DataObjectRangeState]]` — `(offset, length)` pairs, results in the same order, all reads issued concurrently. Fails with `RangeReadNotSupported` for non-appendable IDAs.
- Produces: `ObjectReader.readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite], comment: String): Future[RangedWriteState]` (plus no-comment overload). It issues one concurrent round: a `readRange` per write plus the object length (via the `SliceTail` path used by `readAppendPosition`). The caller never computes alignment; the helper does whatever the IDA needs. If the revisions of the pieces differ, retry the whole fetch (up to 3 attempts), then fail the future; never combine pieces from different revisions.

- [ ] **Step 1: Write failing tests:** `readRanges` returns each range's exact bytes in order on Replication and ARS pools; a range past the end returns the clipped/empty result; `readRanges` on a classic ReedSolomon pool fails with `RangeReadNotSupported`; `readRangedWriteState` followed by `rangedWrite` and commit works; a mixed-revision fetch (inject a commit between pieces using the test hooks in the existing append integration tests, or unit-test the combiner function with fabricated `DataObjectRangeState`s of differing revisions) retries and finally fails rather than combining.
- [ ] **Step 2:** Run the new suite. Expected: FAIL (compile).
- [ ] **Step 3: Implement** `readRanges` as concurrent `readRange` calls combined with `Future.sequence`; implement the helper with a small pure function that checks revisions agree, so the mixed-revision rule is unit-testable without a network.
- [ ] **Step 4:** Run the suite. Expected: PASS.
- [ ] **Step 5:** Commit `Add readRanges and readRangedWriteState`.

### Task 6: End-to-end integration

**Files:**
- Test: `RangedWriteIntegrationSuite.scala` (model on `AppendIntegrationSuite.scala`, same fixtures and `atest` style)

- [ ] **Step 1: Write the tests** for Replication and AppendingReedSolomon pools: overwrite inside a row then read back full object; write spanning rows; extension past end; zero-fill gap (bytes between old end and offset read as zero); gap without zeroFill fails and leaves the object unchanged; multiple writes in one call; state from a full-object read versus from `readRangedWriteState` give identical results; stale state aborts and leaves content intact; a store lagging behind is repaired and later ranged writes succeed (model on the append repair test at line 269); classic ReedSolomon: `readRangedWriteState` fails with `RangeReadNotSupported` and committing a ranged write fails with `RangeWriteNotSupported`; also verify each store's slice (read via the pool's stores) equals a fresh `encode` of the expected content so parity is proven correct, then read back with one data store down.
- [ ] **Step 2:** Run `sbt 'testOnly *RangedWriteIntegrationSuite'`. Expected: PASS (if a case fails, fix the owning task's code, not the test).
- [ ] **Step 3:** Run the full suite `sbt test`. Expected: all pass.
- [ ] **Step 4:** Commit `Add ranged write integration tests`.

### Task 7: Documentation housekeeping

**Files:**
- Modify: `Todo.txt` (remove the RangedWrites item), `CLAUDE.md` (extend the AppendingReedSolomon bullet: ranged writes via `Transaction.rangedWrites`, `readRanges`/`readRangedWriteState`; classic ReedSolomon fails with `RangeWriteNotSupported`)

- [ ] **Step 1:** Make the edits.
- [ ] **Step 2:** Commit `Document ranged writes and clear Todo item`.
