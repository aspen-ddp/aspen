# Store Incarnation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A store applies a transaction through an UnackedCommits entry only when the entry names the store's own
incarnation, and repair scans stop deleting entries the moment the pool no longer names this host and device.

**Architecture:** Each store directory carries an `incarnation` UUID file (fresh on every rebuild). Prepare replies
carry it; drivers pass every incarnation they heard to the finalizer; `MissedUpdateFinalizationAction` writes it into
the entry. The UnackedCommits lookup returns the entry's incarnation, and the Frontend applies only on an exact
match, otherwise concluding the Tx committed with nothing applied (reconcile heals). The repair fence adds a device
check and a once-per-second ownership re-check before each entry delete.

**Tech Stack:** Scala 3, sbt, ScalaTest (`LockstepThreading`), protobuf (`codec.proto`), os-lib.

**Spec:** `docs/superpowers/specs/2026-11-14-store-incarnation-design.md`

## Global Constraints

- Backward compatibility with persisted entries, CRL contents and message formats is not required.
- An unknown or mismatched incarnation never applies, and never turns a committed T into an abort at the store.
- No metadata-pool read at store load to learn the incarnation.
- ErrorTree entries and `StoragePoolState` get no incarnation.
- Tests are deterministic (`LockstepThreading` where possible); no flaky tests; each as fast as possible.
- Full test runs: `timeout 70` is enough to detect hangs under normal load.
- Scala 3 quiet syntax, indented if/then/else; os-lib for filesystem work.
- Each task updates the module CLAUDE.md of the subsystem whose behaviour it changes.
- Commit directly on `main`. End commit messages with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Package root below: `src/main/scala/org/aspen_ddp/aspen/` (main), `src/test/scala/org/aspen_ddp/aspen/` (tests).

## Review Focus

1. A restart keeps a store's incarnation: a recovered commit vote must still match its own entries, or every crash
   recovery would silently stop applying (Task 5, loader test reloads twice and compares).
2. An absent or zero incarnation on the wire never overwrites a known one in the driver (Task 3 test).
3. Chunked entries: every chunk carries the incarnation and the head decodes it (Task 1 test).
4. A store missing from the host's device map is not scanned (fails closed), rather than scanned unfenced (Task 6
   test).
5. A held late commit at a store that holds an applied stub still takes the stub path (apply nothing) whatever the
   entry's incarnation, and a mismatch never writes or drops a stub wrongly (Task 4 test).

---

### Task 1: `StoreIncarnation` type and the entry format

**Files:**
- Create: `common/store/StoreIncarnation.scala`
- Modify: `client/internal/transaction/UnackedCommitEntry.scala`
- Modify: call sites of `UnackedCommitEntry(` (main: `MissedUpdateFinalizationAction.scala`; tests:
  `MissedUpdateFinalizationActionSuite`, `StoreRepairerSuite`, `ReplayIntegrationSuite`,
  `UnackedCommitsIntegrationSuite`) — pass `None` for now
- Test: create `client/internal/transaction/UnackedCommitEntrySuite.scala`

**Interfaces:**
- Produces: `final case class StoreIncarnation(uuid: UUID)` with `object StoreIncarnation { def random():
  StoreIncarnation; def default(storeId: StoreId): StoreIncarnation }` (`default` = `UUID.nameUUIDFromBytes` of
  the pool UUID bytes plus the pool index; stable, never the zero UUID).
- Produces: `UnackedCommitEntry(timestamp: HLCTimestamp, incarnation: Option[StoreIncarnation],
  objects: List[ObjectPointer])`. Encoding: timestamp (8), incarnation UUID (16, zero UUID = `None`), count, pointers.
  `encodedSize` includes the 16 bytes; `chunks` copies `incarnation` into every chunk.

- [ ] **Step 1: Write the failing tests** in `UnackedCommitEntrySuite`:
  - `"an entry round-trips its incarnation, and an unknown one decodes as None"`: encode/decode with
    `Some(StoreIncarnation.random())` and with `None`; `decode(e.encode) shouldBe e`; `e.encode.length shouldBe
    e.encodedSize`.
  - `"every chunk of a split entry carries the entry's incarnation and fits the limit"`: an entry of 50 pointers,
    `chunks(maxEncodedSize(4096))` has more than one element, each `incarnation shouldBe e.incarnation`, each
    `encodedSize <= maxEncodedSize(4096)`, objects concatenated equal the original.
  - `"a store's default incarnation is stable and differs between stores"`.
- [ ] **Step 2: Run** `sbt 'testOnly *UnackedCommitEntrySuite'` — expect compile failure.
- [ ] **Step 3: Implement** the type and the format change; update every call site to pass `None`.
- [ ] **Step 4: Run** `sbt 'testOnly *UnackedCommitEntrySuite *MissedUpdateFinalizationActionSuite *StoreRepairerSuite'`
  — expect PASS.
- [ ] **Step 5: Update** `client/internal/transaction/CLAUDE.md` (UnackedCommits bullet: value format) and commit
  ("UnackedCommits entries carry a store incarnation (Todo 2.12)").

### Task 2: Prepare replies carry the store's incarnation

**Files:**
- Modify: `src/main/protobuf/codec.proto` (`TxPrepareResponse`: `UUID incarnation = 11;`)
- Modify: `common/network/Message.scala` (`TxPrepareResponse` gains trailing `incarnation: Option[StoreIncarnation]
  = None`), `common/network/Codec.scala` (zero/absent UUID decodes to `None`)
- Modify: `server/store/Frontend.scala` (constructor `incarnation: StoreIncarnation`, default
  `StoreIncarnation.default(storeId)`), `server/store/Store.scala` (pass-through, same default)
- Modify: `server/transaction/Tx.scala:1028` (the one construction site sets `Some(frontend.incarnation)`)
- Test: `common/network/CodecRoundTripSuite.scala`, `server/store/FrontendSuite.scala`

**Interfaces:**
- Consumes: `StoreIncarnation` (Task 1).
- Produces: `Frontend.incarnation: StoreIncarnation`; `TxPrepareResponse.incarnation: Option[StoreIncarnation]`;
  `Store(..., incarnation: StoreIncarnation = StoreIncarnation.default(storeId))`.

- [ ] **Step 1: Write the failing tests:**
  - CodecRoundTripSuite `"TxPrepareResponse round-trips its incarnation, and none decodes as none"`.
  - FrontendSuite `"a prepare reply carries the store's incarnation"`: Frontend built with
    `incarnation = StoreIncarnation(new UUID(7, 7))`; after `clientPrepare(dataUpdateTxd)` the sent
    `TxPrepareResponse.incarnation shouldBe Some(StoreIncarnation(new UUID(7, 7)))`.
- [ ] **Step 2: Run** `sbt 'testOnly *CodecRoundTripSuite *FrontendSuite -- -z incarnation'` — expect FAIL.
- [ ] **Step 3: Implement** the field, codec and plumbing.
- [ ] **Step 4: Run** the same command — expect PASS.
- [ ] **Step 5: Commit** ("Prepare replies carry the store's incarnation (Todo 2.12)").

### Task 3: Drivers pass incarnations; the marker records them

**Files:**
- Modify: `server/transaction/TransactionDriver.scala` (record incarnations in `receiveTxPrepareResponse` for both
  nack and promise, before the match; pass them at `:648`)
- Modify: `client/FinalizationAction.scala`, `server/transaction/TransactionFinalizer.scala`,
  `server/RegisteredTransactionFinalizerFactory.scala`, `client/internal/transaction/MissedUpdateFinalizationAction.scala`
- Test: `server/transaction/TransactionDriverSuite.scala`, `client/internal/transaction/MissedUpdateFinalizationActionSuite.scala`,
  `server/RegisteredTransactionFinalizerSuite.scala` (signature only)

**Interfaces:**
- Consumes: `TxPrepareResponse.incarnation` (Task 2), `UnackedCommitEntry.incarnation` (Task 1).
- Produces: `def commitVoters(voters: Option[Set[StoreId]], incarnations: Map[StoreId, StoreIncarnation]): Unit` on
  `FinalizationAction` and `TransactionFinalizer` (default no-op kept). `incarnations` holds every store the driver
  heard from in any prepare response, whatever `voters` is. A response with `incarnation = None` leaves a known value
  in place.

- [ ] **Step 1: Write the failing tests:**
  - TransactionDriverSuite `"a driver that counted a write threshold of commit votes gives the finalizer their
    incarnations"` (extend the existing `:592` pattern: responses carry distinct incarnations; the recording
    finalizer receives `Some(voters)` and a map with exactly those incarnations).
  - TransactionDriverSuite `"a driver that adopted the commit without counting votes still reports the incarnations
    it heard"` (the `:608` pattern: voters `None`, map has each responding store's incarnation and no entry for a
    store that never answered).
  - TransactionDriverSuite `"a reply without an incarnation does not erase one already heard"`: same store answers
    twice, first with `Some(i)`, then `None`; map has `i`.
  - MissedUpdateFinalizationActionSuite `"an UnackedCommits entry records the voter's incarnation, and unknown for a
    store the driver never heard"`: `commitVoters(None, Map(storeId0 -> i0))`; written entries: storeId0 has
    `Some(i0)`, the other silent stores `None`.
- [ ] **Step 2: Run** `sbt 'testOnly *TransactionDriverSuite *MissedUpdateFinalizationActionSuite -- -z incarnation'`
  — expect FAIL.
- [ ] **Step 3: Implement** the plumbing; update existing `commitVoters` callers and overrides (tests included).
- [ ] **Step 4: Run** `sbt 'testOnly *TransactionDriverSuite *MissedUpdateFinalizationActionSuite *RegisteredTransactionFinalizerSuite'`
  — expect PASS.
- [ ] **Step 5: Update** `client/internal/transaction/CLAUDE.md` (entries record the incarnation from
  `commitVoters`) and commit ("Finalizers record each voter's incarnation in its UnackedCommits entry (Todo 2.12)").

### Task 4: The lookup returns the incarnation; only a match applies

**Files:**
- Modify: `server/store/UnackedCommitLookup.scala` (answer type), `server/Host.scala:219` (decode the head entry)
- Modify: `server/store/Frontend.scala` (`unackedLookupComplete`)
- Modify: `server/transaction/Tx.scala` (new `unackedCommitNotOwed()`, next to `unackedCommitFound` `:1233`)
- Test: `server/store/FrontendSuite.scala` (`ManualLookup` answer type; existing `Success(true)` answers become
  `Success(Found(Some(<frontend's incarnation>)))`, `Success(false)` becomes `Success(Absent)`),
  `server/UnackedCommitsIntegrationSuite.scala`

**Interfaces:**
- Consumes: `Frontend.incarnation` (Task 2), entries with incarnations (Tasks 1, 3).
- Produces: `enum UnackedLookupResult { case Absent; case Found(incarnation: Option[StoreIncarnation]) }` in
  `server/store/UnackedCommitLookup.scala`; `UnackedCommitLookup.lookup(..., answer: Try[UnackedLookupResult] => Unit)`.
- `Frontend.unackedLookupComplete`: `Found(Some(own))` → `tx.unackedCommitFound()` (unchanged); `Found(other|None)`
  → log at info with both incarnations, then `tx.unackedCommitNotOwed()`; `Absent` → as today's `Success(false)`.
- `Tx.unackedCommitNotOwed()`: in the `forgetLookupHeld && ofinalized.isEmpty` case, same as `unackedCommitFound`
  (stub, finalize, forget: a stub only suppresses). Otherwise `applyNothing(committed = true, finalMessage = true,
  late = true)`: an entry is durable only after every other finalization action, so T is committed and finalized;
  held aborts are dropped, never accepted. No stub is written. The entry stays for the scan.

- [ ] **Step 1: Write the failing tests** in FrontendSuite (use `lateFixture`; build the Frontend with a known
  `incarnation`):
  - `"a replay at a rebuilt store applies nothing through an entry for the store's earlier incarnation"` (the N3
    reproduction): Frontend with `incarnation = B` and `acceptorSince` earlier than T's start (mute does not apply);
    `clientPrepare(dataUpdateTxd)` → VoteCommit; late `TxResolved`; answer `Found(Some(A))`. Assert: object data
    unchanged (`size 0`), no `TxCommitted`, Tx forgotten, no stub saved, status cache Committed/finalized/late, the
    Tx's locks released.
  - `"a live Tx with its own commit vote applies nothing on a late commit when its entry's incarnation is unknown"`:
    same shape, answer `Found(None)`.
  - `"a recovered commit vote whose entry names no incarnation resolves committed, applies nothing and never
    accepts its held abort"`: recovered VoteCommit Tx (pattern of the `:2568`/`:2609` tests), an abort `TxResolved`
    held, answer `Found(None)`. Assert: no abort applied, object unchanged, status Committed, Tx forgotten.
  - `"a held late commit at a store holding a stub applies nothing whatever its entry's incarnation"`: stub set
    contains txid1; answer `Found(Some(other))`; stub not dropped, nothing applied.
  - Existing `"a live Tx with its own commit vote applies a late commit when its entry exists"` and `"a recovered
    commit vote with a UnackedCommits entry applies its update..."` keep passing with `Found(Some(own))`.
- [ ] **Step 2: Run** `sbt 'testOnly *FrontendSuite -- -z incarnation'` — expect FAIL (compile).
- [ ] **Step 3: Implement** the result type, `Host.unackedLookup` decoding `UnackedCommitEntry.decode(value).incarnation`,
  the Frontend dispatch and `Tx.unackedCommitNotOwed`.
- [ ] **Step 4: Run** `sbt 'testOnly *FrontendSuite *UnackedCommitsIntegrationSuite *ReplayIntegrationSuite'` — expect
  PASS. `UnackedCommitsIntegrationSuite "a recovered commit vote for a committed data overwrite is proven and
  applied"` must pass end to end (the real driver now records the matching incarnation); entries seeded directly in
  that suite pass the store's `StoreIncarnation.default(storeId)` or the loaded store's `frontend.incarnation`.
- [ ] **Step 5: Update** `server/transaction/CLAUDE.md` (Applied stubs: the apply decision; the "Residual: a store
  that never saw T applies T late" sentence becomes "...gets T from reconcile"; UnackedCommits section) and commit
  ("A store applies through an UnackedCommits entry only for its own incarnation (Todo 2.12, Rebuild N3)").

### Task 5: The incarnation file

**Files:**
- Create: `server/store/StoreIncarnationFile.scala`
- Modify: `server/host/StoreLoader.scala:~355` (pass the loaded incarnation to `Store`)
- Modify: `server/rebuild/RebuildingStore.scala` (`finish` `:651-665`: write a fresh incarnation into staging before
  the `rebuild-complete` marker, overwriting any present)
- Test: create `server/store/StoreIncarnationFileSuite.scala`; `server/host/DeviceCrlSuite.scala`;
  `server/rebuild/RebuildingStoreSuite.scala`; `server/transfer/StoreTransferManagerSuite.scala`

**Interfaces:**
- Produces: `object StoreIncarnationFile { val FileName = "incarnation"; def loadOrCreate(backendPath: os.Path,
  storeId: StoreId): StoreIncarnation; def writeFresh(storeDir: os.Path): StoreIncarnation }`. `loadOrCreate` returns
  `StoreIncarnation.default(storeId)` when `backendPath == os.root` (`MapBackend`: no directory, never touch it);
  otherwise reads the file, or, if absent or unreadable, writes a random one with `DurableFiles.write` and logs at
  info. Writes go through `DurableFiles.write` (durable before the store serves).

- [ ] **Step 1: Write the failing tests:**
  - StoreIncarnationFileSuite `"a missing file is created durably and read back unchanged"`,
    `"an unreadable file is replaced by a fresh incarnation"`, `"a backend with no directory gets the store's default
    and nothing is written"`.
  - DeviceCrlSuite (pattern of `:1013` "a store directory carrying acceptor-since loads..."): `"a store keeps its
    incarnation across a restart"`: first load creates the file; `frontend.incarnation` after a restart equals the
    first.
  - RebuildingStoreSuite (pattern of `:326`): `"a rebuild gives the store a fresh incarnation"`: a staging directory
    pre-seeded with an `incarnation` file; after finish, the final directory's incarnation differs from the seeded one.
  - StoreTransferManagerSuite `"beginTransferIn completes when header, archive and trailer arrive"`: add the
    assertion that the destination directory's `incarnation` file equals the source's.
- [ ] **Step 2: Run** `sbt 'testOnly *StoreIncarnationFileSuite *DeviceCrlSuite *RebuildingStoreSuite *StoreTransferManagerSuite -- -z incarnation'`
  — expect FAIL. (The transfer assertion may already pass: the transfer archives the whole directory.)
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same suites without `-z` — expect PASS.
- [ ] **Step 5: Update** `server/store/CLAUDE.md` (store identity bullet), `server/rebuild/CLAUDE.md` (finish writes a
  fresh incarnation), `server/host/CLAUDE.md` (load reads or creates it) and commit ("Stores keep an incarnation file;
  a rebuild writes a fresh one (Todo 2.12)").

### Task 6: Tighter repair fence

**Files:**
- Modify: `server/host/HostStoreView.scala` (`storeDevices: Map[StoreId, StorageDeviceId]`),
  `server/host/DeviceControl.scala:107` (`router.loaded.map((id, s) => id -> s.storageDeviceId)`)
- Modify: `server/repair/RepairTarget.scala` (`def localDevice(storeId: StoreId): Option[StorageDeviceId] = None`),
  `server/Host.scala` (implements it from `deviceControl.currentView.storeDevices`)
- Modify: `server/repair/StoreRepairer.scala` (`ownedHere` `:289`, `abandoned` `:299`, delete sites `:327`, `:432`;
  constructor seam `ownershipRecheck: FiniteDuration = StoreRepairer.OwnershipRecheck` with
  `OwnershipRecheck = 1.second`)
- Test: `server/repair/StoreRepairerSuite.scala` (`RecordingTarget.localDevice` returns the device the test pool's
  entry names, settable per test), `server/repair/HostRepairTargetSuite.scala`

**Interfaces:**
- `ownedHere(storeId)`: with `hostId` given, true only if the pool entry's `hostId == h` and
  `target.localDevice(storeId).contains(entry.storageDeviceId)`; a `None` device is not owned (log at warn).
- Per scan, one cached ownership `Future`, refreshed when older than `ownershipRecheck`. Each delete site awaits it
  before `deleteEntryIfUnchanged`; `false` sets a scan-local flag, the entry is left, and `abandoned` returns true for
  the rest of the scan. The bootstrap sweep uses the device check via `ownedHere` only (it deletes nothing).

- [ ] **Step 1: Write the failing tests** in StoreRepairerSuite (pattern of `:191`):
  - `"a store whose pool records it on this host but another device is not scanned"`.
  - `"a store with no device in the host's view is not scanned"`.
  - `"a scan stops deleting entries once the pool moves the store elsewhere"`: two seeded ErrorTree entries,
    `ownershipRecheck = Duration.Zero`, `testingOnlyBeforeReconcile` rewrites the pool's entry for the store to another
    host after the first object; result: first entry deleted, second kept.
  - HostRepairTargetSuite: `localDevice` returns the loaded store's device and `None` for an unknown store.
- [ ] **Step 2: Run** `sbt 'testOnly *StoreRepairerSuite *HostRepairTargetSuite'` — expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `sbt 'testOnly *StoreRepairerSuite *HostRepairTargetSuite *RepairServiceSuite'` — expect PASS.
- [ ] **Step 5: Update** `server/repair/CLAUDE.md` (fence: host and device, re-checked before each delete) and commit
  ("Repair fence checks the device and re-checks ownership before each entry delete (Todo 2.12, X10)").

### Task 7: Full run, safety records, G5 timeline

**Files:**
- Modify: `docs/safety/cross-document-review.md` (X10 status note dated 2026-11-14; G5 timeline section),
  `docs/safety/rebuild-process.md` (N3 → FIXED, dated note), `docs/safety/README.md` (ranked rows for N3 and X10),
  `Todo.txt` (remove 2.12; numbering of other items unchanged)

- [ ] **Step 1: Run** the full suite: `timeout 70 sbt test` — expect all PASS. Investigate any failure; a flaky test
  is a failure.
- [ ] **Step 2: Write** the records:
  - X10: N3 leg closed clock-free (incarnation match); reverse direction narrowed to the ownership re-check window
    (about one second plus a delete's latency) and requires a mistaken failure of a live device; ErrorTree needs no
    incarnation (exact reconcile; stale-cache hazard).
  - G5 timeline, for an in-place rebuild and for a move to a new device: entries written for the slot (UnackedCommits
    by finalizers of T the old incarnation voted in, tagged with that incarnation; ErrorTree marks while the store is
    Rebuilding); who may consume them (only the store the pool entry names on the device it names, re-checked before
    each delete); what the new incarnation does with inherited entries (UnackedCommits: never applies, reconciles;
    ErrorTree: reconciles exactly).
  - Rebuild N3: FIXED, residual none for the apply; unknown-incarnation entries are healed by reconcile.
- [ ] **Step 3: Commit** ("Safety records: store incarnation closes Rebuild N3, narrows X10, writes G5 (Todo 2.12)").
