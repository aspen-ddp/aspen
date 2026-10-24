# Multi-Range Data Object Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `ByteRanges` read type that fetches several byte ranges plus the length trailer of a data object in one request, make it the only data-object partial-read type, and rebuild `readRange`, `readRanges`, `readAppendPosition` and `readRangedWriteState` on it.

**Architecture:** Stores clip each requested slice range and the slice tail and return them concatenated in request order. The client reader knows the slice size from the response, recomputes each piece's clipped length, splits every store's buffer, and runs `restoreRange` per piece. The result is one state holding all pieces at a single revision. `ByteRange`, `ByteRangeToEnd`, `SliceTail` and the `RangedWriteFetch` retry loop are deleted.

**Tech Stack:** Scala 3, ScalaPB/protobuf (`src/main/protobuf/codec.proto`), ScalaTest (`AnyFunSuite` with `Matchers`), sbt.

**Spec:** `docs/superpowers/specs/2026-10-23-multi-range-read-design.md`

## Global Constraints

- Scala 3 quiet syntax, indented if/then/else; braces only where they clearly help (`CLAUDE.md`).
- os-lib for filesystem work (not expected to be needed here).
- No wire compatibility with the removed read types is required; delete them outright (no `reserved`).
- Ranges are in slice coordinates and are `(offset: Int, length: Int)` pairs. `tail` is `Int`.
- Store layout is a pure function of `(ranges, tail, sliceSize)`: ranges in request order then the tail, each clipped to `[0, sliceSize]`, no dedup or merge.
- `ByteRanges` is a partial read: never cached in `objectCache`, never triggers opportunistic rebuild.
- Classic ReedSolomon still fails with `RangeReadNotSupported`.
- Public signatures of `readRange`, `readRanges`, `readRangedWriteState`, `readAppendPosition` do not change.
- Run a single suite with `sbt 'testOnly *SuiteName'`.

## Review Focus

1. Replication object, tail-only read with `tail = 0` and no ranges (what `readAppendPosition` sends): must still yield the object length. (Task 2 test)
2. A range starting at or past the end of the object, and a zero-length range: empty piece, no exception, same as `readRange` today. (Task 2, Task 3 tests)
3. Overlapping and duplicate requested ranges: each returned piece holds its own correct bytes. (Task 2, Task 3 tests)
4. `length = Int.MaxValue` / huge ranges: no `Int` overflow in clipping or in `sliceRangeFor`. (Task 1 test)
5. Stores answering at different revisions: the reader restores only from a threshold at one revision and never mixes. (Task 2 test)

---

### Task 1: `ByteRanges` model, codec and store clipping (additive)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/objects/ReadType.scala`
- Modify: `src/main/protobuf/codec.proto` (`ReadType` enum, `Read` message)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (`encode(o: Read)`, `decode(m: codec.Read)`, ~lines 700-775)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/PartialRead.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala` (near line 392), `src/test/scala/org/aspen_ddp/aspen/server/store/PartialReadSuite.scala` (near line 48)

**Interfaces:**
- Produces: `case class ByteRanges(ranges: List[(Int, Int)], tail: Int) extends ReadType` in `org.aspen_ddp.aspen.common.objects`. Proto: `READ_TYPE_BYTE_RANGES = 10`; `Read.range_offsets = 13` and `Read.range_lengths = 14` (`repeated uint32`); `tail` travels in the existing `length` field. `PartialRead.filter(ObjectType.Data, data, ByteRanges)` returns `Some(concatenation)`.

- [ ] **Step 1: Write failing tests.** In `CodecRoundTripSuite`: `Codec.decode(Codec.encode(Read(..., ByteRanges(List((0, 8), (40, 16)), 8)))).readType shouldBe ByteRanges(List((0, 8), (40, 16)), 8)`, and the same for `ByteRanges(Nil, 0)` and `ByteRanges(Nil, 64)`. In `PartialReadSuite` (slice is bytes `0 until 100`, reuse its `bytesOf`): `bytesOf(ByteRanges(List((10, 5), (90, 20)), 8)) shouldBe (10 until 15) ++ (90 until 100) ++ (92 until 100)`; `bytesOf(ByteRanges(List((200, 5)), 0)) shouldBe Nil`; `bytesOf(ByteRanges(List((10, 5), (12, 5)), 0)) shouldBe (10 until 15) ++ (12 until 17)` (overlap repeats bytes); `bytesOf(ByteRanges(List((95, Int.MaxValue)), 0)) shouldBe (95 until 100)`; `bytesOf(ByteRanges(List((0, 0)), 500)) shouldBe (0 until 100)` (zero-length range contributes nothing; tail longer than slice returns the whole slice); `bytesOf(ByteRanges(Nil, 0)) shouldBe Nil`; a KeyValue-typed object read with `ByteRanges` is returned unchanged (mirrors the existing `ByteRange` KV test at line 68).
- [ ] **Step 2: Run** `sbt 'testOnly *CodecRoundTripSuite *PartialReadSuite'`. Expected: compile failure, `ByteRanges` not found.
- [ ] **Step 3: Implement.** Add the case class with a doc comment (ranges in slice coordinates, optional last `tail` bytes). Add the proto enum value and two repeated fields. In `Codec`, encode `ByteRanges` (offsets/lengths into the repeated fields, `tail` into `length`) and decode `READ_TYPE_BYTE_RANGES`. In `PartialRead.filter` add the `(ObjectType.Data, rt: ByteRanges)` case: clip each range with the existing private `clip` using `Long` arithmetic (`offset.toLong + length.toLong`, so `Int.MaxValue` lengths cannot overflow), clip the tail as `clip(data, size - tail, size)`, and concatenate. Update the class doc comment.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** `git add -A && git commit -m "Add ByteRanges read type, codec and store clipping"`.

---

### Task 2: Multi-range reader and read-driver wiring (additive)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ObjectState.scala` (add the new state class beside `DataObjectRangeState`, ~line 95)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/DataObjectRangeReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala` (`isPartial` ~line 28, reader selection ~line 39)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/DataObjectRangeReaderSuite.scala`

**Interfaces:**
- Consumes: `ByteRanges` (Task 1); `AppendableIDA.restoreRange(sliceStart: Int, sliceSize: Int, segments: List[(Byte, Array[Byte])]): RestoredRange`.
- Produces: `class DataObjectRangesState(pointer, revision, refcount, timestamp, readTimestamp, val ida: IDA, val ranges: List[DataObjectRangeState], val objectLength: Option[Int]) extends ObjectState(...)` with `appendPosition: Option[AppendPosition]` like `DataObjectRangeState`. `ranges` has one entry per requested range, in request order, each restored (row-aligned, untrimmed) and sharing the state's revision. `DataObjectRangeReader` keeps its existing constructor and, for `ByteRanges`, returns a `DataObjectRangesState`. The reader still serves the old three read types until Task 4.

- [ ] **Step 1: Write failing tests** in `DataObjectRangeReaderSuite` (AppendingReedSolomon(5, 3, 4, 8), 24-byte rows, `response` helper takes `from`/`until`; add a helper that builds a concatenated response for a `ByteRanges`): 
  - `"ByteRanges restores every range and the object length from one response set"`: content(100), `ByteRanges(List((0, 16), (48, 16)), 8)` using `ida.sliceRangeFor` for slice coordinates; assert `ranges(0).offset`/`data` equal the row-aligned content windows, `ranges(1)` likewise, `objectLength shouldBe Some(100)`.
  - `"ByteRanges with overlapping and duplicate ranges restores each independently"`.
  - `"ByteRanges range past the end yields an empty piece"` and `"zero-length range yields an empty piece"`: no exception, empty `data`.
  - `"ByteRanges tail-only read on Replication yields the object length"` (separate reader built with `Replication(3, 2)` and tail 0, no ranges): `objectLength shouldBe Some(sliceSize)`.
  - `"ByteRanges ignores responses from a minority revision"`: two stores at rev2 and three at rev1 (mirror an existing mixed-revision test in this suite if present); the result is restored from rev1 only and `revision shouldBe rev1`.
- [ ] **Step 2: Run** `sbt 'testOnly *DataObjectRangeReaderSuite'`. Expected: FAIL (reader throws "not a byte range read").
- [ ] **Step 3: Implement.** In `DataObjectRangeReader.restoreObject`, add a `ByteRanges(ranges, tail)` branch: compute each piece's clipped `[start, end)` against `sliceSize` exactly as the store does (Long arithmetic; tail piece is `[sliceSize - min(tail, sliceSize), sliceSize)` and is **always** restored, even when empty, so Replication still reports its length); split each store's `objectData` by those lengths; call `restoreRange(start, sliceSize, segmentsForPiece)` per piece; the state's `objectLength` is the tail piece's. Throw `NotRestorable("Below Threshold")` as today when fewer than `threshold` segments. Wrap `IDARestoreError` as today. In `BaseReadDriver` add `ByteRanges` to `isPartial` and to the `DataObjectRangeReader` case.
- [ ] **Step 4: Run** `sbt 'testOnly *DataObjectRangeReaderSuite'`. Expected: PASS, existing tests still pass.
- [ ] **Step 5: Commit** `git commit -am "Add multi-range data object reader"`.

---

### Task 3: Move the client API onto `ByteRanges`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (`readRange`, `readRanges`, `readRangedWriteState`, `readAppendPosition`, ~lines 133-190)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/RangedWriteFetch.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ReadError.scala` (remove `UnstableRangedWriteState`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ObjectReader.scala` (doc comments on `readRanges`, `readRangedWriteState`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/RangedWriteFetchSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/AppendIntegrationSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/RangedWriteIntegrationSuite.scala`

**Interfaces:**
- Consumes: `DataObjectRangesState` (Task 2).
- Produces: `RangedWriteFetch.combine(pointer, writes, position: AppendPosition, reads: List[DataObjectRangeState]): RangedWriteState` (no longer an `Option`; no revision-mismatch case). `RangedWriteFetch.fetch`, `MaxAttempts` and `UnstableRangedWriteState` are gone. A private helper in `BaseAspenClient` (e.g. `readByteRanges(pointer, ranges, tail, comment): Future[DataObjectRangesState]`) is the single read path.

- [ ] **Step 1: Update/write tests first.**
  - `RangedWriteFetchSuite`: delete the three revision-mismatch tests and the three `fetch` tests; change the remaining `combine` tests to expect a `RangedWriteState` directly (drop `.get`). Keep the "does not cover its write" `IllegalStateException` test and the over-long-read clipping test.
  - `AppendIntegrationSuite`: add tests for `readRange` with `Some(len)` and `None`, `readAppendPosition`, and `readRanges` including an empty list, a range at/after the end, a zero-length range, overlapping ranges and duplicate ranges; assert each result equals the corresponding slice of the written content (compare against `readRange` results from before the change if easiest).
  - `RangedWriteIntegrationSuite`: add `"readRangedWriteState never fails with a mixed revision while a writer appends"`: run an appender loop in the background and call `readRangedWriteState` repeatedly; every call must succeed and its `revision`/`length` must be consistent with `oldBytes` (use the pattern of content this suite already writes). Add `"readRanges returns every range at one revision"` using the same loop and asserting all returned states share `revision`.
- [ ] **Step 2: Run** `sbt 'testOnly *RangedWriteFetchSuite *AppendIntegrationSuite *RangedWriteIntegrationSuite'`. Expected: FAIL (compile errors / old behaviour).
- [ ] **Step 3: Implement.** Add the private single-read helper: resolve the appendable IDA first (so an empty list on classic ReedSolomon fails with `RangeReadNotSupported`), validate, build `ByteRanges` via `ida.sliceRangeFor` per range, call `rmgr.read(..., readType = ByteRanges(...))`, cast to `DataObjectRangesState`. `readRange` → one-element list, `tail = 0`, then `trimRange` the head (the `None` length case uses `(offset, Int.MaxValue - offset)` mapped through `sliceRangeFor`, or `(ida.sliceOffsetFor(offset), Int.MaxValue)` directly; builder's choice, but the end must not overflow). `readRanges` → one request, `tail = 0`, trim each piece. `readAppendPosition` → `Nil`, `tail = ida.lengthProbeBytes`, `appendPosition` from the state (`CorruptedObject` if `None`). `readRangedWriteState` → ranges for `writes`, `tail = ida.lengthProbeBytes`, then `RangedWriteFetch.combine` with the state's `appendPosition` and `ranges`. Strip `fetch`/`MaxAttempts` and the mismatch branch from `RangedWriteFetch`; remove `UnstableRangedWriteState`; fix the `ObjectReader` doc comments (single revision, no retries).
- [ ] **Step 4: Run** the Step 2 command. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -am "Build range reads on a single ByteRanges request"`.

---

### Task 4: Delete the old read types and finish

**Files:**
- Modify: `ReadType.scala`, `codec.proto`, `Codec.scala`, `PartialRead.scala`, `BaseReadDriver.scala`, `DataObjectRangeReader.scala` (all under `src/main/...` as in Tasks 1-2)
- Modify: `CLAUDE.md` (IDA paragraph, lines ~28-34), `Todo.txt`
- Test: `CodecRoundTripSuite.scala`, `PartialReadSuite.scala`, `DataObjectRangeReaderSuite.scala`

**Interfaces:**
- Consumes: everything above. Produces: `ReadType` with no `ByteRange`, `ByteRangeToEnd` or `SliceTail`; proto enum without values 2, 8, 9; `DataObjectRangeReader` with only the `ByteRanges` path.

- [ ] **Step 1: Migrate tests.** Rewrite every test that constructs a removed type to use `ByteRanges` with the mapping `ByteRange(o,l)` → `ByteRanges(List((o,l)),0)`, `ByteRangeToEnd(o)` → `ByteRanges(List((o, Int.MaxValue)),0)`, `SliceTail(n)` → `ByteRanges(Nil,n)`; expectations unchanged except that reader tests now read pieces from `DataObjectRangesState.ranges` / `objectLength`. Delete the now-duplicate codec cases (lines ~392-404) and fold unique assertions into the `ByteRanges` ones.
- [ ] **Step 2: Delete the types.** Remove the three case classes, their proto values, `Codec` encode/decode arms, `PartialRead` branches, the `BaseReadDriver` references, and the old single-range branches and `sliceStart` match in `DataObjectRangeReader`. Fix the `"Read type $rt is not a byte range read"` guard to reject anything but `ByteRanges`.
- [ ] **Step 3: Docs.** In `CLAUDE.md` note that range and tail reads are served by one `ByteRanges` request and that `readRanges`/`readRangedWriteState` read a single revision. Remove the top item (the `readRangedWriteState` paragraph) from `Todo.txt`.
- [ ] **Step 4: Run** `sbt compile` (Expected: no references to the removed types: `grep -rn "ByteRangeToEnd\|SliceTail\|ByteRange(" src` finds none), then `sbt test`. Expected: PASS. Report any failure with its output rather than skipping.
- [ ] **Step 5: Commit** `git commit -am "Remove single-range read types; update docs and Todo"`.
