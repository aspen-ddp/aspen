# CRL Growth and Oversized-Transaction Drops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** No client request can kill a device's CRL: oversized transactions are dropped silently, and the CRL grows
(crash-safely, within a cap and a free-space floor) to raise the device's admission limit.

**Architecture:** SimpleCRL's ring order moves into `crl/sizing`, which is also the commit point of a growth. Records
track per-piece locations, and a live-piece index replaces the record queue for `oldestEntryNeeded`. A rotation writes
an evacuation-only entry first, and pending saves are cut into entries that fit, so the admission limit can scale as
`(n − 2) · S / 2`. Frontend drops prepares above `crl.maxTransactionBytes` and asks the CRL to grow when its lane is
full.

**Tech Stack:** Scala 3, sbt, ScalaTest, os-lib, existing `DurableFiles` / `FilePreallocation`.

**Spec:** `docs/superpowers/specs/2026-11-17-crl-growth-design.md` (read it first; this plan argues from it).

## Global Constraints

- Read `server/crl/CLAUDE.md`, `server/store/CLAUDE.md`, `server/CLAUDE.md` (threading) before coding.
- Defaults: `crl-num-streams` 4 (minimum 4 in device config), `crl-max-file-size-mb` default `capacity / 2048`
  rounded down to 4 KiB, at least 64 MiB, `crl-max-streams` 20, `crl-min-free-percent` 5.
- Per-transaction bound `T = S/2 − EntryOverheadReserve` (S = stream size). Target admission limit `(n − 2) · S / 2`.
- Growth commit point: the atomic rewrite of `crl/sizing` listing the ring. The CRL never shrinks.
- Oversized prepares are dropped with no reply, no vote, no CRL write; logged at warn, rate-limited.
- No hot-path cost beyond one comparison in `admitPrepare`. Rare paths (growth, evacuation, drops) stay simple.
- No compatibility with persisted CRL files is required (CLAUDE.local.md).
- Tests: deterministic, fast, no flakes. Full runs under a 70 s timeout. Commit on main after each task.
- Every code change updates the module CLAUDE.md it affects in the same task.
- *Plan's call:* `SimpleCRL.apply` itself accepts 3 or more streams (the limit formula is positive at 3, giving today's
  `S/2`). The minimum of 4 is enforced where device CRLs are created (`CrlSizingDefaults.resolve`). This keeps the 143
  SimpleCRLSuite tests that use 3-stream CRLs unchanged. The 2-stream tests move to 3.

## Review Focus

1. A prepare whose data arrives after a data-less admission and pushes the transaction over `T`. Expected: that
   prepare is dropped and the earlier admission is untouched (Task 1 test `oversizedLateData`).
2. A crash between the growth file's rename and the `crl/sizing` commit, then a second growth to the same number.
   Expected: the open removes the orphan and the next growth recreates it cleanly (Task 6 test
   `growthOrphanThenRegrow`).
3. `dropTransactionObjectData` on a record whose update pieces lie in the oldest stream, followed by a rotation.
   Expected: the pieces stay pinned until the record's static record is rewritten. Recovery never names a location
   outside the verified entries (Task 4 test `droppedPiecesStayPinnedUntilRewrite`).
4. A save larger than the room left in the active stream, while the next stream is dense with live pieces. Expected:
   the writer rotates as many times as needed, all saves become durable, and `require` never fires (Task 5 stress test).
5. Growth requested while the CRL is shutting down or has died. Expected: no file is left except a `.tmp` or an
   unlisted `<n>.crl`, both removed at the next open. The shutdown does not hang (Task 6 test
   `growthDuringShutdown`).

---

### Task 1: Drop oversized transactions (reproduce CRL-W2 first)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/CrashRecoveryLog.scala` (trait)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/SimpleCRL.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala:1258-1284` (`admitPrepare`, `chargeData`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala:266` (drop + log)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/crl/simple/SimpleCRLSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`

**Interfaces:**
- Produces: `CrashRecoveryLog.maxTransactionBytes: Long` (default `Long.MaxValue`).
  `SimpleCRL.EntryOverheadReserve: Long = 8192`. SimpleCRL's value is `maxSizeInBytes / 2 - EntryOverheadReserve`.
- Produces: `enum PrepareAdmission { case Admitted, Full, Oversized }` in `server/store/Frontend.scala`, with
  `Frontend.admitPrepare(m: TxPrepare): PrepareAdmission` (was `Boolean`).

- [ ] **Step 1: Write the reproduction** `test("An entry larger than a stream kills the CRL: the backstop admission must keep it from (CRL-W2)")`
  in SimpleCRLSuite. Open `SimpleCRL(tdir, 3, 4096 * 4)`, save a trs whose one object update is `4096 * 5` bytes, and
  assert `Await.result(i.crl.failed, 5.seconds)` completes. This pins the backstop.
- [ ] **Step 2: Run it** with `sbt 'testOnly *SimpleCRLSuite -- -z "CRL-W2"'`. Expected: PASS, which confirms the
  mechanism. If it does not die, stop and report.
- [ ] **Step 3: Write the failing Frontend tests** in FrontendSuite, with a CRL stub whose `maxTransactionBytes` is 10 000:
  - `oversizedPrepare`: a prepare with 20 000 bytes of object data returns `PrepareAdmission.Oversized`, and
    `admission.admittedBytes` is unchanged.
  - `oversizedLateData`: a data-less prepare is `Admitted`. A second prepare of the same txid carrying 20 000 bytes
    returns `Oversized`, and `admittedBytes` equals the value after the first.
  - `fullLaneStillFull`: with the admission at the ceiling, a small new prepare returns `Full`.
- [ ] **Step 4: Run** `sbt 'testOnly *FrontendSuite -- -z "versized"'`. Expected: compile failure (no `PrepareAdmission`).
- [ ] **Step 5: Implement.** Compare against `crl.maxTransactionBytes` before charging: the new transaction's counted
  bytes, or for an admitted one `a.bytes + added`. `Store.receiveTransactionMessage` drops both non-Admitted results
  without a reply. `Oversized` logs at warn with txid, store and bytes, rate-limited to one line per store per 10 s.
- [ ] **Step 6: Run** both suites. Expected: PASS.
- [ ] **Step 7: Docs.** In `server/store/CLAUDE.md` (admission bullet), document the oversized drop. In
  `server/crl/CLAUDE.md`, document `maxTransactionBytes`.
- [ ] **Step 8: Commit** with the message `"An oversized transaction is dropped, not saved: the CRL survives it (CRL-W2, Todo 4.1)"`.

### Task 2: Capacity-derived CRL sizing defaults

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/crl/CrlSizingDefaults.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceConfig.scala` (keys become `Optional`, new keys)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/SimpleCRL.scala` (`DeviceFactory.openCRL`)
- Test: create `src/test/scala/org/aspen_ddp/aspen/server/crl/CrlSizingDefaultsSuite.scala`; modify
  `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceConfigSuite.scala`

**Interfaces:**
- Produces: `case class CrlSizing(numStreams: Int, streamBytes: Long, maxStreams: Int, minFreePercent: Int)` and
  `object CrlSizingDefaults { def resolve(config: StorageDeviceConfig, capacityBytes: Long): CrlSizing }`.
- Produces: `StorageDeviceConfig(storageDeviceId, aspenSystemId, crlNumStreams: Option[Int] = None,
  crlFileSizeMb: Option[Int] = None, crlMaxStreams: Option[Int] = None, crlMinFreePercent: Option[Int] = None)`.
  Keys: `crl-num-streams`, `crl-max-file-size-mb`, `crl-max-streams`, `crl-min-free-percent`. `yamlConfig` writes only
  the keys that are set.

- [ ] **Step 1: Failing tests** in `CrlSizingDefaultsSuite`:
  - A 1 TiB (`1L << 40`) capacity resolves to `CrlSizing(4, 512L << 20, 20, 5)`.
  - A 10 GiB capacity gives `streamBytes == 64L << 20`.
  - A capacity of `(1L << 40) + 12345` gives a `streamBytes` that is a multiple of 4096 and at most capacity/2048.
  - Explicit `crlFileSizeMb = Some(2)` gives `2L << 20`.
  - `crlNumStreams = Some(3)` throws `IllegalArgumentException` whose message contains `"at least 4"`.
  - `crlMaxStreams = Some(3)` with the default 4 streams throws.
  - `crlMinFreePercent = Some(101)` throws.

  In `StorageDeviceConfigSuite`, a yaml without any `crl-*` key parses with all four `None`. Replace the old test that
  required both keys.
- [ ] **Step 2: Run** `sbt 'testOnly *CrlSizingDefaultsSuite *StorageDeviceConfigSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement.** `DeviceFactory.openCRL` resolves sizing with capacity `devicePath.toIO.getTotalSpace` and
  passes `numStreams` and `streamBytes` to `SimpleCRL.apply`. Growth bounds are passed in Task 7. Update DeviceCrlSuite
  call sites, `crlFileSizeMb = 1` → `Some(1)`, and the `resizedCrlDevice` tests (2 → 4, 4 → 5).
- [ ] **Step 4: Run** both suites plus `*DeviceCrlSuite`. Expected: PASS.
- [ ] **Step 5: Docs.** Update the `StorageDeviceConfig` sample yaml comment, and the sizing bullet in `server/crl/CLAUDE.md`.
- [ ] **Step 6: Commit** with the message `"CRL sizing defaults scale with the device's capacity"`.

### Task 3: The ring is recorded in `crl/sizing`

**Files:**
- Modify: `SimpleCRL.scala` (`Sizing`, `recordedSizing`, `checkSizing`, `createStreamsIfNew`, rotation's successor)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/Recovery.scala` (the next-stream check)
- Test: `SimpleCRLSuite.scala`

**Interfaces:**
- Produces: `Sizing(ring: Vector[StreamId], maxSizeInBytes: Long, crlId: UUID)` with `numStreams = ring.size` and
  `def successor(s: StreamId): StreamId`. Encoding: the existing lines plus `ring: 0,1,2,3`. `decode` returns None
  unless the ring is a permutation of `0 until num-streams`.
- Produces: `Recovery.recover(files, crlId, ring: Vector[StreamId])`. The torn-write check of "the next stream" uses
  `ring` successor.
- Produces: `SimpleCRL.apply` takes an optional `ringForTest: Option[Vector[StreamId]]`, used only when creating a
  CRL, so later tasks can test non-numeric rings before growth exists.

- [ ] **Step 1: Failing tests:**
  - `"A CRL created with a ring out of numeric order rotates and recovers in ring order"`: ring `0,2,1`, 4 KiB × 3
    streams. Save until `currentStreamNumber` goes 0 → 2 → 1 → 0. Reopen after each step and compare `trsList`.
  - `"Sizing without a ring line is unreadable"`: expect `CrlSizingMismatch`.
  - `"A stream file numbered past the ring is an uncommitted growth: removed at open"`: write `3.crl`, the stream size,
    beside a 3-stream CRL. The open succeeds, logs, and the file is gone. Same for a `3.crl.tmp`.
  - `"Any other unlisted stream file is a sizing mismatch"`: `5.crl` beside a 3-stream CRL.
- [ ] **Step 2: Run** `sbt 'testOnly *SimpleCRLSuite -- -z "ring"'`. Expected: FAIL.
- [ ] **Step 3: Implement.** Rotation uses `sizing.successor(currentStream)` and its successor in place of `+ 1 % n`.
  `checkSizing` removes `<ringSize>.crl` / `<ringSize>.crl.tmp` with a warning before comparing sets. The existing
  creation-`.tmp` rule (no stream files) is unchanged.
- [ ] **Step 4: Run** the full `*SimpleCRLSuite`. Expected: PASS.
- [ ] **Step 5: Commit** with the message `"SimpleCRL records its ring order in crl/sizing"`.

### Task 4: Per-piece locations and a live-piece index

Behavior-preserving except that copy-forward moves pieces, not whole records.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/LogContent.scala` (`Record`, `Tx`, `Stub`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/LogEntry.scala` (`createEntryBuffers` allocation)
- Modify: `SimpleCRL.scala` (remove `queueHead`/`queueTail`/`addToQueueHead`/`moveToQueueHead`/`removeFromQueue`)
- Modify: `Recovery.scala` (`RecoveredTx` carries the serial of each piece)
- Create: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/LivePieces.scala`
- Test: `SimpleCRLSuite.scala`, create `src/test/scala/org/aspen_ddp/aspen/server/crl/simple/LivePiecesSuite.scala`

**Interfaces:**
- Produces: `case class Piece(location: StreamLocation, serial: Long)`.
  `Tx.txdPiece: Option[Piece]`, `Tx.updatePieces: Vector[Option[Piece]]` (one per `state.objectUpdates`, by index).
  `Stub.anchor: Option[Piece]`. `Record.piecesIn(stream: StreamId): Boolean`. `Record.unlocate(stream: StreamId): Unit`
  forgets only the pieces in that stream.
- Produces: `final class LivePieces { def add(serial: Long): Unit; def remove(serial: Long): Unit;
  def oldest: Option[Long] }`, a counted `TreeMap`. `SimpleCRL.oldestEntryNeeded = livePieces.oldest.getOrElse(current serial)`.
- Rule: a piece's serial is added when the entry writing it is built, and removed when the piece is relocated (after
  the new location is assigned) or its record is deleted. Pieces dropped by `dropTransactionObjectData` stay in the
  index until the record's next static rewrite, then are removed. A record's `entrySerialNumber` field goes away.
- Rule: `LogEntry.oldestEntryNeeded` is stamped when the entry's buffers are created, not when the entry object is made.

- [ ] **Step 1: Failing tests:**
  - LivePiecesSuite: add 5, 3, 3, then remove 3. `oldest == Some(3)`. Remove 3 again: `oldest == Some(5)`.
  - SimpleCRLSuite `"A rotation copies forward only the pieces in the evacuated stream"`: tx A's txd is in stream 1,
    and its updates are saved later into stream 2. Rotate so stream 1 is evacuated. Assert the evacuation entry's
    dynamic size equals A's txd size, not txd + updates. Use `runOnCrlThread` to read `currentLogEntry`. Recovery
    returns A whole.
  - `droppedPiecesStayPinnedUntilRewrite` (Review Focus 3): the updates are in the oldest stream, then
    `dropTransactionObjectData`, then rotations up to the wrap. Each reopen recovers A's txd without
    `BrokenEntryChain`. After A's next save, `oldestEntryNeeded` advances past the old update entry.
- [ ] **Step 2: Run** `sbt 'testOnly *LivePiecesSuite *SimpleCRLSuite'`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement.** The update-count change path (`objectUpdateLocations = None` today) becomes "all update
  pieces unlocated, their serials removed". Recovery builds `Piece`s with `entryHolding(loc)` per piece, and SimpleCRL's
  constructor adds them to `LivePieces`.
- [ ] **Step 4: Run** the full `*SimpleCRLSuite` and `*DeviceCrlSuite`. Expected: PASS (all existing rotation, wrap,
  torn-write and reopen tests).
- [ ] **Step 5: Commit** with the message `"SimpleCRL relocates pieces, not records; a live-piece index sets oldestEntryNeeded"`.

### Task 5: Evacuation-first rotations, bounded entries, and the proven admission limit

**Files:**
- Modify: `SimpleCRL.scala` (`writeCurrentLogEntry`, `startWrite`, a FIFO of sealed entries, `admissionLimitBytes`)
- Modify: `docs/safety/crash-recovery-log.md` (the proof, in the design text)
- Test: `SimpleCRLSuite.scala`, create `src/test/scala/org/aspen_ddp/aspen/server/crl/simple/SimpleCRLStressSuite.scala`

**Interfaces:**
- Consumes: Task 4's `Record.piecesIn` / `unlocate`, and `LivePieces`.
- Produces: `SimpleCRL.MaxEntryBytes = maxSizeInBytes / 2`. `admissionLimitBytes = (numStreams - 2) * maxSizeInBytes / 2`.
- Rules (from the spec, section 4):
  - Adding a record that would push the current entry past `MaxEntryBytes` seals it. Sealed entries are written FIFO,
    one at a time. Barriers and completion handlers attach to the newest unsealed entry, as today.
  - When the next entry does not fit in the active stream, the writer rotates into `successor(active)`. That stream
    holds nothing needed by the invariant. The first write there is an evacuation-only entry carrying every live piece
    in `successor(new active)`. Then the pending entry is written. If it does not fit beside the evacuation, the writer
    rotates again.
  - `Stream.writeEntry`'s `require` stays.

- [ ] **Step 1: Write the proof first.** In crash-recovery-log.md (sections 6 and 10), prove:
  - (a) An evacuation entry always fits in an empty stream. Its pieces plus one static record each, plus one header
    and padding, are no larger than what the evacuated stream held. Bound the static-record growth from an update-count
    change, and show it is covered.
  - (b) With live bytes ≤ `(n − 2)·S/2 + T + unchecked charges`, a sealed entry (≤ S/2) is written within at most
    `n − 1` rotations.

  If (b) fails, lower the formula (never below `S/2` at n = 4, never by lowering T) and record the formula used. Get
  the proof reviewed in the task's review before coding.
- [ ] **Step 2: Failing tests:**
  - `"Pending saves larger than half a stream are cut into entries that each fit"`: 4 KiB × 64 KiB streams. Queue 40
    saves of 2 KiB on a held CRL thread (`runOnCrlThread` + latch). All complete and recover.
  - `"A rotation writes its evacuation before the pending saves"`.
  - `admissionLimitBytes` of a 4-stream CRL of size S is `S`, and of a 20-stream one is `9 * S`.
  - `SimpleCRLStressSuite` (Review Focus 4): a seeded random workload against a real SimpleCRL of 4 streams × 256 KiB.
    It keeps a model of admission, saves only below the limit and up to T per transaction, randomly delays deletions,
    and makes dense placements (bursts held live across a full wrap). After every N operations it shuts down, reopens,
    and compares recovered state to the model. It asserts `failed` never completes. 20 seeds. Must run under 10 s.
- [ ] **Step 3: Run** `sbt 'testOnly *SimpleCRLSuite *SimpleCRLStressSuite'`. Expected: FAIL.
- [ ] **Step 4: Implement** the rules.
- [ ] **Step 5: Run** the suites, plus `*DeviceCrlSuite *FrontendSuite`. Expected: PASS. Run the stress suite 10
  times in a loop under load (CLAUDE.local.md permits 100% CPU). There must be no failures.
- [ ] **Step 6: Commit** with the message `"SimpleCRL evacuates before saving and bounds its entries: admission scales with the ring"`.

### Task 6: Crash-safe growth in SimpleCRL

**Files:**
- Modify: `CrashRecoveryLog.scala` (trait methods)
- Modify: `SimpleCRL.scala` (growth thread, `GrowthReady` request, ring insert, limit listener)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/StreamWriter.scala` (`addStream`)
- Test: `SimpleCRLSuite.scala`

**Interfaces:**
- Produces, on the trait:
  - `def requestGrowth(): Unit = ()`. Idempotent, callable from any thread.
  - `def onAdmissionLimitRaised(f: Long => Unit): Unit = ()`. `f` runs on the CRL thread with the new limit after each
    committed growth.
- Produces: `case class GrowthPolicy(maxStreams: Int, minFreePercent: Int, capacity: () => Long, freeSpace: () => Long,
  retryAfter: FiniteDuration = 10.seconds)` and `GrowthPolicy.Disabled`. `SimpleCRL.apply(..., growth: GrowthPolicy =
  GrowthPolicy.Disabled)`.
- Produces: `StreamWriter.addStream(id: StreamId, path: os.Path): Unit`. It is queued to the writer thread, ahead of
  any later write.
- Sequence (the spec, section 3):
  1. The growth thread, one per SimpleCRL and stopped by its shutdown/death, checks the policy:
     `ringSize < maxStreams` and `freeSpace() - S >= capacity() * minFreePercent / 100`. It then preallocates
     `<n>.crl.tmp` with n = ringSize, forces it, renames it to `<n>.crl`, syncs the directory, and puts
     `GrowthReady(n)`.
  2. The CRL thread: if alive and `n == ringSize`, it writes `crl/sizing` with n inserted after `currentStream`
     (`DurableFiles.write`), calls `writer.addStream`, adds a `Stream`, and calls listeners with the new
     `admissionLimitBytes`.
  3. A failure at any step logs, removes the `.tmp`/unlisted file where possible, and retries no sooner than
     `retryAfter`. A failure never fails the CRL.

- [ ] **Step 1: Failing tests:**
  - `"A growth inserts a stream after the active one and raises the limit"`: 4 streams, writer in stream 2. The ring
    becomes `0,1,2,4,3`, the listener sees `3 * S / 2`, the next rotation enters 4, and reopens recover everything.
  - `"Growth stops at maxStreams"` and `"Growth is refused below the free-space floor"` (fake `freeSpace`).
  - Crash at each step. Simulate by leaving files as each step would and reopening without a shutdown:
    `.tmp` only → removed; `<n>.crl` unlisted → removed; ring committed and the stream unwritten → it opens and the
    next rotation enters it; first entry in the new stream torn → the state before it recovers (reuse the
    torn-at-every-sector helper of `"A rotation write torn at any sector..."`).
  - `growthOrphanThenRegrow` (Review Focus 2).
  - `growthDuringShutdown` (Review Focus 5): request growth with a `freeSpace` that blocks on a latch, call
    `shutdown()`, and release the latch. Shutdown returns, and the reopen opens with the original ring.
  - `"A failed preallocation leaves the CRL unchanged and alive"` (`capacity` throws).
- [ ] **Step 2: Run** `sbt 'testOnly *SimpleCRLSuite -- -z "rowth"'`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the full `*SimpleCRLSuite`. Expected: PASS.
- [ ] **Step 5: Commit** with the message `"SimpleCRL grows by one stream at a time, committed by crl/sizing"`.

### Task 7: Wire growth into devices and admission

**Files:**
- Modify: `CrlAdmission.scala` (mutable limit)
- Modify: `Frontend.scala` (`requestGrowth` on `Full`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceDiscovery.scala:632-637`
- Modify: `SimpleCRL.scala` (`DeviceFactory.openCRL` builds the `GrowthPolicy` from `CrlSizing`)
- Test: `CrlAdmissionSuite.scala`, `FrontendSuite.scala`, `DeviceCrlSuite.scala`

**Interfaces:**
- Consumes: Task 6's trait methods, Task 2's `CrlSizing`.
- Produces: `CrlAdmission.limitBytes: Long` becomes a getter over an `AtomicLong`, and
  `CrlAdmission.raiseLimit(bytes: Long): Unit` only ever increases it. `ceiling(tier)` reads the current limit.

- [ ] **Step 1: Failing tests:**
  - CrlAdmissionSuite: after `raiseLimit(2 * L)`, `ceiling(Data) == 2 * L - 2 * L / 4`, and `raiseLimit(L)` leaves it
    at `2 * L`.
  - FrontendSuite: a `Full` prepare calls `crl.requestGrowth()` once (recording stub). `Admitted` and `Oversized`
    prepares do not call it.
  - DeviceCrlSuite `"A device whose CRL lane fills grows its CRL and admits more"`: a device with `crlFileSizeMb =
    Some(1)`, `crlMaxStreams = Some(5)`. Hold transactions until admission refuses. The ring grows to 5 streams, the
    `sds.admission.limitBytes` rises, and a retransmitted prepare is admitted.
- [ ] **Step 2: Run** `sbt 'testOnly *CrlAdmissionSuite *FrontendSuite *DeviceCrlSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement.** DeviceDiscovery registers `crl.onAdmissionLimitRaised(sds.admission.raiseLimit)` right
  after constructing `sds`. The growth policy's capacity and free space come from
  `devicePath.toIO.getTotalSpace` / `getUsableSpace`.
- [ ] **Step 4: Run** the full test suite: `sbt test` with a 70 s hang watch. Expected: PASS.
- [ ] **Step 5: Commit** with the message `"Devices grow their CRL when admission fills, raising the limit"`.

### Task 8: Documentation and work lists

**Files:** `Architecture.md`, `docs/safety/crash-recovery-log.md`, `src/main/scala/org/aspen_ddp/aspen/server/crl/CLAUDE.md`,
`src/main/scala/org/aspen_ddp/aspen/client/internal/CLAUDE.md`, `Todo.txt`, `Backlog.txt`

- [ ] **Step 1: Architecture.md, CRL section** (after the "circular ring buffer" paragraph). Add a paragraph covering:
  - The CRL grows by whole files, up to a cap and within a free-space floor, and never shrinks.
  - A transaction whose CRL bytes on one store exceed about half a CRL file is silently dropped and can never commit.
  - The recommendation: "Applications should not write objects whose slice on any one store exceeds 10 MiB."
  - Also fix the copy-forward description ("second-to-last file") to match the ring.
- [ ] **Step 2: crash-recovery-log.md.** Remove CRL-W2 from Open weaknesses. Present-tense design text for the ring,
  growth steps and open rules, the invariant, the live-piece index, and the drop rule (the proof is already in from
  Task 5). Fix the references to CRL-W2 at sections 2 (C11), 6 and 10.5.
- [ ] **Step 3: server/crl/CLAUDE.md.** Rewrite the circular-buffer, sizing, preallocation and admission bullets for
  the ring, growth, `maxTransactionBytes` and the limit formula.
- [ ] **Step 4: client/internal/CLAUDE.md.** Read `ClientTransactionDriver` and document what an application sees
  when every store drops its transaction until retransmits stop.
- [ ] **Step 5: Todo.txt.** Remove item 4.1, keeping the other numbers. **Backlog.txt:** remove the "CRL expansion."
  entry (its paragraph at about lines 190-192, with the blank line after it).
- [ ] **Step 6: Commit** with the message `"Docs: CRL growth, oversized drops and the 10 MiB slice guidance"`.
