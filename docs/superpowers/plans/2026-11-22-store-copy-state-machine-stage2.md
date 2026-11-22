# Store Copy State Machine, Stage 2 (Phase Directories) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the four marker files with phase directories, so a copy's directory is its phase, every phase
change is one durable rename, and every copy has a unique `copyId` name.

**Architecture:** A pure `CopyLayout` maps phases to directories and names. `CopyMoves` replaces `CopyMarkers`:
it makes every phase change as a rename, records it in the copy registry as a fact, and sets `unsynced` when a
directory sync fails. The migration converts one phase at a time: Lost, Outbound, Rebuilt and Parked, Arrived, then
Live. Until a phase is converted it stays a marker on a flat `<device>/<storeDir>` copy, so each task leaves the
full suite green. The last conversion removes the flat path, the markers, `StoreDirMarkers`, `CopyMarkers` and the
marker reader in `CopyPhase`.

**Tech Stack:** Scala 3, os-lib, ScalaTest, sbt. RocksDB and FSIF backends.

**Spec:** `docs/superpowers/specs/2026-11-21-store-copy-state-machine-design.md`. Binding sections: Device layout,
Phases and transitions, Uniform move and `Unsynced`, Registry (including "Facts and claims"), the stage 2 plan
outline, Tests, Documentation. Stage 1 is done: plan `docs/superpowers/plans/2026-11-21-store-copy-state-machine-stage1.md`.

## Global Constraints

- Run on the current branch (`state_refactor`). No worktree. Never use `git stash`: read an earlier revision with
  `git show <rev>:<path>`.
- Persisted-data compatibility is not required (spec Non-goals): there is no migration of flat copies or markers,
  and devices are recreated.
- Every existence test under `server/` uses `server.FileStat`. A listing of a phase directory distinguishes a
  failed listing from an empty one (spec Non-goals). `FileStatUsageSuite` must keep passing.
- Use os-lib for filesystem work. java.nio only where os-lib has no equivalent (DSYNC channels, directory fsync).
- Every phase change is one `CopyMoves.move`. It writes `phase-param` durably first when the target phase carries
  a parameter, then renames through `DurableFiles.move`. No other code renames a copy between phases.
- No move is ever made of an installed store's directory. Every phase change happens while the copy is closed
  (spec Device layout; Step 1 amendment on T10).
- Each layout name, verbatim: `live/`, `arrived/`, `outbound/`, `rebuilt/`, `parked/`, `lost/`, `pending-delete/`;
  scratch areas `creating/`, `deleting/`, `transferring-in/<id>/`, `rebuilding/<id>/`; copy name
  `<storeId.directoryName>.<copyId>`; parameter file `phase-param`.
- Registry updates stay facts or claims (R22). A move's registry update is a fact. Tests fail on any violation
  (`Host.Seams.strictCopyRegistry` / `verifyCopyRegistry`). Production logs it at error.
- Scala 3 quiet syntax, indented if/then/else, no one-line `if a then b` unless clearer.
- Test priorities: correctness, then reliability (no flaky tests), then speed. Every new test is deterministic
  (`LockstepThreading`, ControlFixture) unless it names a real-time hop and waits on it with `awaitLoudly`.
- Full suite: run `sbt Test/compile` first, then `sbt test` with a 70 s Bash timeout. It takes about 50 s idle. A
  task is done only when the full suite passes.
- When a subsystem's behaviour changes, update its module CLAUDE.md in the same task (Task 8 does the full rewrite).

## Decisions this plan makes (the spec leaves them open or must be amended)

Task 8 copied these into the spec ("Stage 2 decisions"), which also holds the rulings made during execution. The
single `phase-param` file named below was superseded by R-T1: each phase keeps its own `phase-param.<phase directory>`.

- **P1. Retire before placement stays.** `retireEarlierTenancy`, `discardLostStore` and `retireForeignCopy` still
  retire an earlier copy before a new copy of the store is placed on the device. Unique names remove the path
  collision, but not the ordering need. An earlier copy's device-CRL entries must be saved (retire step 2) before
  the new copy's first load, which is a save-file load that drops them (`StoreLoader.awaitCrlLoad`). Only the path
  checks go: the "store path already exists" refusal, and path clearing as the reason to retire.
- **P2. At most one copy of a store per device outside `pending-delete/` (I1, strengthened).** The registry stays
  keyed by `StoreId` per device (`DeviceCopies.copies: Map[StoreId, CopyRecord]`), and `CopyRecord` gains `copyId`.
  P1 keeps well-behaved code within this rule. A device load that finds a store in more than one copy directory
  logs an error and moves every one of them to `lost/` (T10). It registers the one with the least `copyId` (Lost),
  which the normal passes retire, and moves the others into `pending-delete/` whole. They are released like any
  retired copy. Their files, `crl_save.log` included, stay inside them. The entry is then rebuilt. This is the
  spec's I1 rule (both retired, store rebuilt), extended to every phase.
- **P3. Retired copies are not recorded.** As in stage 1, a copy leaves the registry when it reaches
  `pending-delete/`. `Phase.Retired` exists only as a move target. `StoreCopies.release` lists `pending-delete/`
  itself, so I4 holds by construction.
- **P4. A copy gets its `copyId` when it first enters a phase directory.** That happens at the create's rename into
  `live/`, a rebuild's finish into `rebuilt/`, or a transfer's promotion into `arrived/`. Scratch directories are
  not copies. During Tasks 2–5 a flat copy gets a fresh `copyId` at its first move into a phase directory. From
  Task 6 every copy has one (`copyId: UUID`).
- **P5. A move to Lost waits for the close.** Today `backendFailed` (served), `retireForeignCopy`,
  `retireUnlistedServed` and `StoreLoader`'s failed unserved backend write `rebuild-required` before the backend
  closes. Stage 2 moves the copy to `lost/` when its close ends. A failed close moves it too. Purpose
  `ClosePurpose.FailStop` means "move to Lost when closed". A crash in between leaves the copy in `live/`, and the
  restart loads it as after any crash: the CRL holds what the backend had not applied, and a backend that is still
  broken fails its open (`BackendStateLost`) or fails again. Cost if wrong: a fail-stopped copy serves once more
  after a crash in that window.
- **P6. A failed listing or `phase-param` read at device load leaves the device unloaded** for the next scan, as a
  failed CRL open does. A `phase-param` known absent reads as `""`, which matches no transfer or episode.
  `Phase.Unread` is removed (Task 6).
- **P7. Transfer staging is per attempt.** `StoreTransfers` passes `<device>/transferring-in/<transferUUID>` as the
  service's destination directory. The service still extracts into `<that>/<storeDir>`, so `StoreTransferManager`
  is unchanged apart from paths. Promotion renames `<staging>/<storeDir>` to `arrived/<storeDir>.<newCopyId>`.
- **P8. Open question 1 is resolved: Outbound does not carry the episode.** An Outbound copy goes back to Live
  (T7) only for an Active entry with no `transferDevice` (`mayReinstateOut`). Active means the flip committed, so
  the source needs nothing from the old `rebuild-complete`.
- **P9. Open question 2 is resolved: Parked stays a phase.** It is the layout's `parked/`, and `reloadClosedStores`
  stays a Live-copy rule.
- **P10. Only Live, Rebuilt and Parked copies are T6 sources.** `startOut` refuses, at debug, a copy in any other
  phase. An Arrived copy whose move to Live failed waits for the Completed Transfers In pass. This replaces the
  stage-1 TI+TO combination and its transitions (Arrived→Outbound, Outbound→Arrived, Outbound→Rebuilt,
  Arrived→Rebuilt), which Tasks 4–5 delete.
- **P11. `DurableFiles.move` reports a rename that landed but was not forced.** It throws
  `DurableFiles.MoveNotForced(source, target, cause)` when the rename succeeded and a directory sync failed. Any
  other throw means nothing moved.

## Review Focus

1. **A crash between a FailStop close and its move to `lost/` (P5).** A reasonable operator expects the restarted
   host to recover the copy from its CRL as after any crash, losing no committed transaction, and to rebuild it
   only if it fails again. Test: Task 2, "a crash after a fail-stop close and before its move reloads the copy from
   live".
2. **A transfer back onto a device that still holds an earlier tenancy's copy in `outbound/`, `lost/` or
   `rebuilt/`.** Expected: the copy is retired, then the transfer runs. It is never stuck behind a path, and never
   received while the earlier copy's CRL entries are unsaved. Test: Task 5, "a transfer in retires an earlier
   tenancy's copy in each non-Live phase, then receives".
3. **A device holding two copies of one store across phase directories (P2).** Expected: neither is served, both
   end in `lost/` or `pending-delete/` with their files, and the store is rebuilt. Test: Task 6, "two copies of one
   store at load are both retired and the store rebuilt".
4. **A phase directory or `phase-param` that cannot be read at device load (P6).** Expected: the device stays
   unloaded and nothing on it is moved, and the next scan loads it once the read succeeds. Test: Task 6, "an
   unreadable phase directory or phase-param leaves the device unloaded until a scan can read it".
5. **A name in a phase directory that is not a copy name** (an operator's stray directory or file). Expected: it is
   logged once, never loaded, moved or deleted, and the device loads. Test: Task 1, `CopyLayoutSuite` "a child
   whose name is not a copy name is reported, not listed".

---

### Task 1: Layout, moves and path indirection (no format change)

The foundation. Code and tests stop computing `devicePath / storeId.directoryName` and ask the layout or the
registry instead. The layout maps every phase to the flat path for now, so nothing on disk changes.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/CopyLayout.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/CopyMoves.scala` (unused by production until Task 2)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/DurableFiles.scala` (`MoveNotForced`)
- Modify: `server/host/CopyRegistry.scala` (`CopyRecord.copyId: Option[UUID]`), `HostCopies.scala` (`pathOf`,
  `attach`/`rescan`/`verifyAgainstDisk`/`storeDirectories` through `CopyLayout.list`), `DeviceControl.scala`
  (`moves: CopyMoves`)
- Modify every main-code site of the inventory: `grep -rn "devicePath / .*directoryName\|path / storeId.directoryName" src/main/scala`
  (StoreRebuilds, DeviceReconciler, DeviceShutdown, DeviceMetadata, DeviceDiscovery, LocalDevices, StoreTransfers,
  StoreTransfersOut, StoreCopies, StoreLoader, CopyMarkers, RebuildingStore). Each reads `CopyLayout.path(...)` or
  `HostCopies.pathOf(...)`.
- Create: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyFixtures.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyLayoutSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/server/host/CopyMovesSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/server/store/backend/BackendRelocationSuite.scala`
- Port: every test that builds a store path or writes or reads a marker by hand (`grep -rln "directoryName\|MarkerFile\|StoreDirMarkers" src/test/scala`),
  to `CopyFixtures`.

**Interfaces:**
- Produces (`server/host/CopyLayout.scala`):
  ```scala
  object CopyLayout:
    val PhaseParamFile = "phase-param"
    /** The directory of `phase` under the device root, None while the phase is still a flat marker copy */
    def dirOf(phase: Phase): Option[String]
    def copyName(storeId: StoreId, copyId: UUID): String           // s"${storeId.directoryName}.$copyId"
    def parseName(name: String): Option[(StoreId, UUID)]           // inverse; the store id is from the name only
    def path(devicePath: os.Path, storeId: StoreId, copyId: Option[UUID], phase: Phase): os.Path
    def param(phase: Phase): Option[String]                        // Arrived/Outbound tid, Rebuilt episode
    /** Reads `phase-param` of the copy at `copyPath`: "" when known absent; throws a failed stat or read */
    def readParam(copyPath: os.Path): String
    final case class Found(storeId: StoreId, copyId: Option[UUID], phase: Phase, path: os.Path)
    /** Every copy on the device: flat store directories (phase from CopyPhase.read) and children of each converted
     *  phase directory. Throws a failed listing or param read. A child whose name is not a copy name is skipped,
     *  and returned in `strays` */
    def list(devicePath: os.Path): Listing
    final case class Listing(copies: List[Found], strays: List[os.Path])
  ```
  In Task 1 `dirOf` returns `None` for every phase except `Retired` (`Some("pending-delete")`), and
  `path(..., Retired)` is not used yet.
- Produces (`server/DurableFiles.scala`): `final class MoveNotForced(val source: os.Path, val target: os.Path, cause: Throwable) extends java.io.IOException(cause)`,
  thrown by `DurableFiles.move` when the rename landed and a sync failed. Its `syncDirectory` calls inside `move`
  go through a `sync: os.Path => Unit = syncDirectory` parameter.
- Produces (`server/host/CopyMoves.scala`):
  ```scala
  enum MoveResult:
    case Durable
    case Unsynced(cause: Throwable)   // renamed; a directory sync failed; the record takes the phase, unsynced
    case NotMoved(cause: Throwable)   // nothing changed; the issuing pass retries
  final class CopyMoves(ctx: DeviceControl):
    private[server] var syncDirectory: os.Path => Unit   // test seam, as CopyMarkers.syncDirectory
    private[server] var rename: (os.Path, os.Path) => Unit // test seam; default os.move(atomicMove = true)
    /** Moves the copy of `storeId` on the attached `sds` to `to`, recording the fact (HostCopies.moved) */
    def move(site: String, sds: LocalStorageDeviceState, storeId: StoreId, to: Phase): MoveResult
    /** As `move`, for a copy of a device being loaded (lost CRL, adoption): returns the copy's new path, and an
     *  unsynced result is held for the device's attach, as stage 1's `unsyncedPaths` */
    def moveUnattached(site: String, from: os.Path, storeId: StoreId, copyId: Option[UUID], to: Phase): (MoveResult, os.Path)
    /** Forces again each unsynced copy's source and target directories (the device check). Returns the stores
     *  synced now */
    def resync(local: LocalStorageDeviceState): List[StoreId]
  ```
  `move` assigns a fresh `copyId` when the record has none (P4).
- Produces (`HostCopies`): `def pathOf(sds: LocalStorageDeviceState, storeId: StoreId): Option[os.Path]` (the
  record's path, from `CopyLayout.path`), and the fact
  `def moved(site: String, sds: LocalStorageDeviceState, storeId: StoreId, to: Phase, copyId: UUID, unsynced: Boolean): Unit`.
  `moved` to `Retired` unregisters the record.
- Produces (`CopyFixtures`, test tree; every later task changes only its bodies):
  ```scala
  object CopyFixtures:
    /** Places a configured copy of `storeId` in `phase` on the device at `deviceDir`, returning its path. Writes
     *  the store config (as ControlFixture.writeStoreConfig) and the phase's representation (marker or directory
     *  and phase-param) */
    def placeCopy(deviceDir: os.Path, storeId: StoreId, phase: Phase = Phase.Live): os.Path
    /** The path of the one copy of `storeId` on the device, wherever its phase puts it; None if absent */
    def copyPath(deviceDir: os.Path, storeId: StoreId): Option[os.Path]
    /** The phase of the one copy of `storeId` on the device, None if absent */
    def phaseOf(deviceDir: os.Path, storeId: StoreId): Option[Phase]
    /** The retired copies of `storeId` in pending-delete/ */
    def retiredCopies(deviceDir: os.Path, storeId: StoreId): List[os.Path]
  ```

- [ ] **Step 1: Write the failing layout tests** in `CopyLayoutSuite`:
  - `"copy names round-trip and carry the store id"`: `parseName(copyName(s, u)) == Some((s, u))`;
    `parseName(s.directoryName) == None`; `parseName(s"${s.directoryName}.not-a-uuid") == None`.
  - `"every phase maps to the spec's directory once converted"`: a table of (phase, expected name) for `Live`
    "live", `Arrived("t")` "arrived", `Outbound("t")` "outbound", `Rebuilt("e")` "rebuilt", `Lost` "lost",
    `Retired` "pending-delete". In Task 1 only `Retired` is asserted `Some`; each later task flips its row to
    `Some` (and Task 4 adds `Parked` "parked").
  - `"param is the phase's parameter"`: `param(Arrived("t")) == Some("t")`, `param(Outbound("")) == Some("")`,
    `param(Rebuilt("e")) == Some("e")`, `param(Live) == None`, `param(Lost) == None`.
  - `"readParam reads an absent file as empty and throws a failed stat"`: absent → `""`; a parent without search
    permission → throws.
  - `"a child whose name is not a copy name is reported, not listed"` (Review Focus 5): a directory `stray` and a
    file `notes.txt` placed under `pending-delete/` (and, from each later task, under that task's phase directory)
    appear in `strays` and not in `copies`.
  - `"a failed listing throws"`: a phase directory without read permission → `list` throws.

- [ ] **Step 2: Write the failing move tests** in `CopyMovesSuite`, through a ControlFixture host with one loaded
  device, `verifyCopyRegistry = true`:
  - `"a move writes phase-param before the rename and records the phase"`: with `rename` wrapped to assert the
    source's `phase-param` already reads the target's parameter, then delegating; the result is `Durable`, and the
    record's phase is the target.
  - `"a sync that fails after the rename leaves the record unsynced and the next resync clears it"`:
    `syncDirectory` throws once for the target directory; result `Unsynced`; `record.unsynced == true`; after
    `moves.resync(local)` the result list holds the store and `record.unsynced == false`.
  - `"a rename that fails changes nothing"`: `rename` throws; result `NotMoved`; the directory and the record are
    unchanged; no registry violation is recorded.
  - `"DurableFiles.move throws MoveNotForced only after a landed rename"`: a unit test of `DurableFiles.move` with
    a failing `sync`: target exists, source gone, exception is `MoveNotForced`.
  Until Task 2 converts a phase, these tests drive `move` to `Retired`, from a Lost copy placed by `placeCopy`
  (stage-1 flat copy with the `rebuild-required` marker). Each later task adds its phase to the move test.

- [ ] **Step 3: Write the backend relocation test** `BackendRelocationSuite`: `"a closed RocksDB and FSIF store
  opens again after its directory is renamed"`. For each backend, create the store with
  `DefaultBackendOpener.createStore`, open it, write and commit one object, close it, rename the directory with
  `DurableFiles.move`, then open it at the new path and read the object back. This is the spec's first
  implementation step (no backend records its absolute path). If it fails, stop and report: the layout depends on
  it.

- [ ] **Step 4: Run the new suites to verify they fail**

  Run: `sbt 'testOnly *CopyLayoutSuite *CopyMovesSuite *BackendRelocationSuite'`
  Expected: compile errors naming `CopyLayout`, `CopyMoves`, `MoveNotForced`.

- [ ] **Step 5: Implement `CopyLayout`, `MoveNotForced`, `CopyMoves`, `CopyRecord.copyId`, `HostCopies.pathOf`
  and `HostCopies.moved`.** `CopyLayout.list` replaces `DeviceDiscovery.storesIn`, `HostCopies.storeDirectories`
  and the listing in `attach`/`rescan`/`verifyAgainstDisk`. A flat child counts as a copy only when it has a
  `config.yaml`, as today. The phase directories, `creating/`, `deleting/`, `transferring-in/`, `rebuilding/`,
  `crl*` and the device markers are never taken for flat copies. Log each stray at warn, once per device load.
  `CopyMoves.resync` takes over `CopyMarkers.resync`'s call site at the top of `reconcileDeviceState`, and forces
  marker-unsynced copies too: `CopyMarkers` keeps writing markers for unconverted phases and records through the
  same `unsynced` flag.

- [ ] **Step 6: Route every main-code store path through the layout.** Replace each `devicePath /
  storeId.directoryName` with `ctx.copyRegistry.pathOf(sds, storeId)` where a record must exist, or with
  `CopyLayout.path(devicePath, storeId, None, Phase.Live)` where the site names the flat path of a copy not yet
  recorded (create target, rebuild final path, transfer promotion target). After this step,
  `grep -rn "directoryName" src/main/scala | grep -v "CopyLayout\|StoreId.scala\|StoreCreation"` lists only staging
  paths (`rebuilding/`, `transferring-in/`).

- [ ] **Step 7: Write `CopyFixtures` and port the tests.** Each test that builds `deviceDir / storeId.directoryName`
  or writes, reads or asserts a marker file uses `placeCopy`, `copyPath`, `phaseOf` or `retiredCopies`. A test that
  asserts a marker's presence asserts `phaseOf(...)` instead (e.g. `os.exists(dir / TransferOutMarkerFile)` becomes
  `phaseOf(dev, s) == Some(Phase.Outbound(tid))`). `ControlFixture.writeStoreConfig` stays for a Live copy and
  calls `placeCopy`. Keep each test's assertions otherwise as they are: this step must not change what any test
  proves. After it, `grep -rn "MarkerFile\|StoreDirMarkers" src/test/scala` lists only `CopyFixtures`,
  `CopyPhaseSuite` and `StoreTransferManagerSuite` (the service suite, which moves raw directories).

- [ ] **Step 8: Run the full suite**

  Run: `sbt Test/compile`, then `sbt test` (70 s Bash timeout).
  Expected: PASS, with the new suites included.

- [ ] **Step 9: Commit**

  ```bash
  git add -A src docs
  git commit -m "Stage 2 Task 1: CopyLayout, CopyMoves and MoveNotForced; every store path through the layout; tests through CopyFixtures"
  ```

---

### Task 2: Lost becomes `lost/`

`rebuild-required` is no longer written. Every T10 trigger moves the copy into `lost/<copy>`, and retire step 4
moves `lost/<copy>` to `pending-delete/<copy>`.

**Files:**
- Modify: `CopyLayout.dirOf(Lost) = Some("lost")`; `CopyPhase.read` stops reading `rebuild-required`.
- Modify: `StoreRebuilds` (`markRebuildRequired` → `moveToLost(site, sds, storeId): MoveResult`;
  `backendFailed`, `retireUnserved`, `failedOnClose`, `retireForeignCopy`, `retireUnlistedServed`,
  `discardLostStore`, `crlSaveFileLost`), `StoreLoader` (the failed unserved backend at `handleBackendOpened`, the
  `BackendStateLost` open, the close ends of `handleClose`/`closeUnloaded` for `ClosePurpose.FailStop`),
  `StoreCopies.retire`/`retirePresent`/`moveToPendingDelete` (step 1 is the move to `lost/`, step 4 a move to
  `Retired`; pending-delete names are copy names; `pendingDeleteStore` parses them with `CopyLayout.parseName`),
  `DeviceDiscovery` (lost device CRL: `moveUnattached(..., Lost)` for each copy not `holdsOwnState`),
  `DeviceMetadata` (adoption `CrlLost`), `DeviceReconciler` (Lost CRL pass selects `Phase.Lost` records, not
  `hasMarker(RebuildRequired)`), `StoreTransfersOut` (`startOut`'s and `closedForTransfer`'s rebuild-required
  checks read the record's phase).
- Modify: `Host.Seams.writeRebuildRequiredMarker` is replaced by `Host.Seams.copyRename: Option[(os.Path, os.Path) => Unit]`,
  installed as `CopyMoves.rename` before the first device load. Port `DeviceCrlSuite`'s `failingMarker` test to it.
- Modify: `Transitions.phaseLegal`: `Lost → Retired` legal; `Lost → anything else` stays illegal.
- Modify: `RebuildingStore.RebuildRequiredMarkerFile` deleted; `StoreDirMarkers.Marker.RebuildRequired` deleted.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/PhaseDirectoriesSuite.scala` (new, ControlFixture), plus
  `CopyFixtures` bodies.

**Interfaces:**
- Consumes: `CopyMoves.move`/`moveUnattached`, `HostCopies.moved`, `CopyLayout` (Task 1).
- Produces: `StoreRebuilds.moveToLost(site: String, sds: LocalStorageDeviceState, storeId: StoreId): MoveResult`
  (the one T10 entry point for a closed copy of an attached device), and
  `ClosePurpose.FailStop` meaning "move to Lost when the close ends" (P5), and
  `CopyMoves.pendingLost(local: LocalStorageDeviceState): Set[StoreId]` (copies whose move to Lost failed, held
  offline and retried at each device check). `StoreCopies.retire` keeps its signature and result.

- [ ] **Step 1: Write the failing tests** in `PhaseDirectoriesSuite`:
  - `"a served copy whose backend fails moves to lost when its close ends"`: a served copy, a `BackendFailed`
    event, the close held (the fixture's close hold). While it is held, `phaseOf == Some(Live)` and the copy is
    still at its flat path. After release, `phaseOf == Some(Phase.Lost)`, the copy is under `lost/` with a copy
    name, and nothing remains at the flat path.
  - `"a crash after a fail-stop close and before its move reloads the copy from live"` (Review Focus 1): hold the
    close's end, rebuild the host from the same directory (a fresh `newHost` over `hostRoot`, the old host
    dropped), and assert the copy is loaded (`storeLoadAttempts` names its path) and nothing is in `lost/`.
  - `"an unserved backend that fails at open moves to lost after its close"` and
    `"a BackendStateLost open moves the copy to lost"`.
  - `"a lost device CRL moves every copy that does not hold its own state to lost"`: two Live copies, one with a
    valid own `crl_save.log`; the CRL removed; after the load, `phaseOf` is `Lost` for one and `Live` for the other.
  - `"retire moves lost to pending-delete under the copy's name"`: a Lost copy (placed) whose entry left the
    device; after the check, `retiredCopies(dev, s)` holds one path named `copyName(s, copyId)` that contains
    `lost-crl-state.log`, and `copyPath(dev, s) == None`.
  - `"a crash at each retire step leaves the copy in lost and the next retire finishes"`: fail
    `local.crl.readStoreState`, then `dropStore`, then the final rename (`copyRename`), each once; after each failure
    `phaseOf == Some(Lost)`; after the next check the copy is retired.
  - `"a failed move to lost holds the copy offline and the next check moves it"`: `copyRename` throws once on a
    target under `lost/`. `phaseOf` is unchanged, and no load of the copy is attempted (`storeLoadAttempts`); the
    next check moves it.

- [ ] **Step 2: Run them to verify they fail**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite'`
  Expected: FAIL. The copies keep the flat path with a `rebuild-required` marker.

- [ ] **Step 3: Implement.** Every former `markRebuildRequired(storePath)` caller calls `moveToLost` once the copy
  is closed. For a served copy, the FailStop close's end calls it (`StoreLoader.handleClose` and `closeUnloaded`),
  and so does a failed close (`failedOnClose`). `retireUnserved` awaits the backend's close before it moves. A
  `NotMoved` result is logged at error, and the copy is held in `CopyMoves.pendingLost(local): Set[StoreId]`
  (in memory, per device). A held copy is never loaded, shipped or retired (`HostCopies.heldOffline` includes it).
  Each device check retries its move first, beside `resync`. A crash forgets the hold, and the copy then reloads
  from `live/`, as P5 accepts. Every reader of "is it marked" reads the record's phase.

- [ ] **Step 4: Run the new tests, then the full suite**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite *CopyMovesSuite *CopyLayoutSuite'`, then `sbt Test/compile` and
  `sbt test` (70 s).
  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git commit -am "Stage 2 Task 2: Lost is lost/; T10 moves a closed copy; retire moves lost/ to pending-delete/ by copy name"
  ```

---

### Task 3: Outbound becomes `outbound/` (Parked rides along as `Outbound("")`)

The `transferring-out` marker is no longer written. T6 moves the closed copy into `outbound/<copy>` with the
transfer id as `phase-param`, and the service ships from there. T7 moves it back to Live.

**Files:**
- Modify: `CopyLayout.dirOf(Outbound(_)) = Some("outbound")`; `CopyPhase.read` stops reading `transferring-out`.
- Modify: `StoreTransfersOut` (`closedForTransfer`: move to `Outbound(tid)`, then `beginTransferOut` with the
  outbound path; `reinstateAbandonedOut`: move to Live; `extractUnloaded`/`extractFromDirectory`: the record's path;
  `startOut`: P10's source check), `DeviceDiscovery.bringIntoService` (T7 by move for a `mayReinstateOut` entry,
  through `moveUnattached`), `StoreRebuilds.keepRebuiltStoreOffline` (T4 is for now a move to `Outbound("")`, a
  single move with no intermediate TO+RC state), `DeviceReconciler.reinstateAbandonedTransfersOut` (selects
  `Outbound` records), `DeviceDiscovery.adoptionClass` (Outbound by location), `StoreTransfers` staging (the
  shipped copy carries `phase-param`, not `transferring-out`: promotion no longer removes a TO marker).
- Modify: `StoreTransfer.TransferOutMarkerFile` and `StoreDirMarkers.Marker.TransferOut` deleted.
- Modify: `Transitions.phaseLegal`: Live/Rebuilt/`Outbound("")` → `Outbound(tid)`; Outbound → Live / Lost.
  Delete Outbound → Rebuilt, Outbound → Arrived, Arrived → Outbound (P10).
- Test: `PhaseDirectoriesSuite`, `StoreTransferManagerSuite` (ships a directory under `outbound/`), the ported
  transfer suites.

**Interfaces:**
- Consumes: Task 2's `moveToLost`; `CopyMoves`.
- Produces: none new. `beginTransferOut(storeId, transferUUID, sourceDirectory, ...)` receives the outbound path.

- [ ] **Step 1: Write the failing tests** in `PhaseDirectoriesSuite`:
  - `"a transfer out moves the closed copy to outbound with its transfer id and ships it from there"`: through
    the fixture's transfer-out close, the recorded `beginTransferOut` source is under `outbound/`, and
    `CopyLayout.readParam(source) == tid.toString`.
  - `"an aborted transfer out moves the copy back to live and serves it"`: an Outbound copy, entry Active with no
    `transferDevice`; after the check `phaseOf == Some(Live)` and the copy is loaded.
  - `"an adopted outbound copy is Clean only with a valid save file"`: two adoptions, with and without a valid
    `crl_save.log`; the classes are Clean and CrlLost, and the CrlLost copy is moved to `lost/`.
  - `"startOut refuses a copy that is not Live, Rebuilt or Parked"`: an Arrived copy (placed) on an Active entry;
    `startOut` logs at debug and ships nothing.
  - `"a sync that fails after the move to outbound ships nothing until resynced"`.

- [ ] **Step 2: Run to verify they fail**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite'`
  Expected: FAIL. The copy stays flat with `transferring-out`.

- [ ] **Step 3: Implement.** `closedForTransfer` moves the copy only after the extraction has made
  `crl_save.log` durable (T6's order today). A `NotMoved` or `Unsynced` result ships nothing. It ends the
  TransferOut work, and the receiver's retransmit retries. An `Unsynced` copy stays unshipped until the next device
  check resyncs it.

- [ ] **Step 4: Run the new tests, then the full suite**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite *StoreTransferManagerSuite'`, then `sbt Test/compile` and
  `sbt test` (70 s).
  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git commit -am "Stage 2 Task 3: Outbound is outbound/; T6 and T7 are moves; the transfer service ships from outbound/"
  ```

---

### Task 4: Rebuilt becomes `rebuilt/`, and Parked becomes `parked/`

`rebuild-complete` is no longer written. A rebuild's `finish` renames staging into `rebuilt/<copy>` with the
episode as `phase-param`. AdoptOffline moves Rebuilt to `parked/` (T4). `Phase.Parked` replaces `Outbound("")`.

**Files:**
- Modify: `CopyPhase.scala` (`Phase.Parked`; `Phase.Retired` if Task 1 did not add it), `CopyLayout`
  (`Rebuilt` → `"rebuilt"`, `Parked` → `"parked"`), `CopyPhase.read` stops reading `rebuild-complete`.
- Modify: `rebuild/RebuildingStore.scala`. `finish` writes `phase-param` (the episode,
  `rebuildCompleteContent(episode)`) in staging in place of the marker, then renames staging to
  `CopyLayout.path(devicePath, storeId, Some(freshCopyId), Rebuilt(ep))`. `start()` asks a lookup it is given
  (`existingCopy: () => Option[(Phase, os.Path)]`, passed by `StoreRebuildFactory.createRebuild` from the registry
  or, off the control thread, from `CopyLayout.list`), not `finalPath`. `isRebuildCompleteFor` becomes
  `isRebuiltFor(phase: Phase, episode: Option[UUID]): Boolean`. `RebuildCompleteMarkerFile` is deleted.
- Modify: `StoreRebuilds` (`keepRebuiltStoreOffline` → one move to Parked; `clearRebuildCompleteMarker` →
  `bringRebuiltLive(sds, storeId): MoveResult`, a move Rebuilt → Live; `RebuildOutcome.Discard` deletes the Rebuilt
  copy (T13, through `StoreCopies.delete` of its path); `retireForeignCopy` keys off the record's phase;
  `finishPlacing` exemption reads the `rebuilt/` copy's `phase-param`), `DeviceReconciler.completeRebuilds` (selects
  Rebuilt records), the Parked → Live paths (Abandoned Transfers Out pass and `bringIntoService` for Parked, T5),
  `DeviceDiscovery.holdsOwnState` (Rebuilt by location), `StoreTransfers.recordTransferIn` (no RC removal: a
  shipped Rebuilt copy arrives as a plain Arrived copy), `HostCopies.finishInFlight`.
- Modify: `Transitions.phaseLegal`: Rebuilt → Live / Parked / Outbound / Lost; Parked → Live / Outbound / Lost;
  delete `Outbound("") → Outbound(tid)` and Arrived → Rebuilt.
- Test: `PhaseDirectoriesSuite`, `RebuildingStoreSuite` (ported), `RebuildEndToEndSuite`, the ported rebuild suites.

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: `Phase.Parked`; `StoreRebuilds.bringRebuiltLive(sds: LocalStorageDeviceState, storeId: StoreId): MoveResult`;
  `RebuildingStore.isRebuiltFor(phase: Phase, episode: Option[UUID]): Boolean`; `StoreRebuildFactory.createRebuild`
  gains `existingCopy: () => Option[(Phase, os.Path)]`.

- [ ] **Step 1: Write the failing tests** in `PhaseDirectoriesSuite` and `RebuildingStoreSuite`:
  - `"finish renames staging into rebuilt with the episode as phase-param"` (RebuildingStoreSuite): after a
    finished pass, `CopyLayout.list(dev)` holds one copy of the store, phase `Rebuilt(ep.toString)`, and
    `rebuilding/<storeDir>` is gone.
  - `"start takes a rebuilt copy of its own episode for done, and fails on any other copy"` (RebuildingStoreSuite):
    `Rebuilt(ep)` → completes without a pass; `Rebuilt(other)` or `Live` → fails with the "holds a store directory
    that no rebuild finished" error.
  - `"an Adopt flip moves the rebuilt copy to live and serves it"`.
  - `"an AdoptOffline flip moves the rebuilt copy to parked in one move"`: `phaseOf == Some(Phase.Parked)`, and no
    copy is in `rebuilt/` or `outbound/`.
  - `"a parked copy goes live when its entry is Active with no transfer device"` (T5).
  - `"a parked copy can be transferred out"` (T6 from Parked): its ship source is under `outbound/`.
  - `"a Discard deletes the rebuilt copy rename-first"` (T13): nothing in `rebuilt/`, nothing in `pending-delete/`.
  - `"a shipped rebuilt copy arrives and goes live with no rebuild-complete step"` (spec note "A shipped Rebuilt
    copy"): port `HostDeviceDiscoverySuite`'s `finishTransferIn(movingStore, extraFiles = List(RebuildCompleteMarkerFile))`
    test. The staged copy now carries the source's `phase-param` (its Outbound tid) and an empty `crl_save.log`.
    Assert the destination's record goes Arrived → Live with no Rebuilt phase between, and that the live copy's
    `phase-param`, if any, is inert (the copy loads).
  - `"a lost device CRL leaves a rebuilt copy alone"`: Rebuilt copy, CRL removed; `phaseOf == Some(Rebuilt(ep))`.

- [ ] **Step 2: Run to verify they fail**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite *RebuildingStoreSuite'`
  Expected: FAIL.

- [ ] **Step 3: Implement.** `finish` runs off the control thread, as now, and the control-thread handler records
  the placed copy (`HostCopies.placed` / `rebuilt`) with the `copyId` the finish chose. `finish` reports it in its
  completion. A failed rename leaves staging intact for the next pass, as today.

- [ ] **Step 4: Run the new tests, then the full suite**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite *RebuildingStoreSuite *RebuildEndToEndSuite'`, then
  `sbt Test/compile` and `sbt test` (70 s).
  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git commit -am "Stage 2 Task 4: Rebuilt is rebuilt/ and Parked is parked/; finish, T3, T4, T5 and Discard are moves"
  ```

---

### Task 5: Arrived becomes `arrived/`, and staging is per attempt

`transferred-in` is no longer written. Promotion renames `transferring-in/<transferUUID>/<storeDir>` into
`arrived/<copy>` with the transfer id as `phase-param` (T8). The pool update moves it to Live (T9), after
`arrived-at` is written.

**Files:**
- Modify: `CopyLayout` (`Arrived` → `"arrived"`), `CopyPhase.read` stops reading `transferred-in`.
- Modify: `StoreTransfers` (`startIn`: the destination directory is `<device>/transferring-in/<transferUUID>` (P7),
  the promotion is a move to `Arrived(tid)`, `clearTransferredInMarker` → `bringArrivedLive(sds, storeId): MoveResult`
  writing `arrived-at` first, the stale check reads the record (`Arrived(tid)` with another tid, an unusable save
  file → delete, T13), `discardArrivedTransferIn` deletes the Arrived copy's path, the staging cleanups at `:229`,
  `:466`, `:495` use the per-attempt path), `DeviceReconciler.completeTransfersIn` (selects Arrived records),
  `DeviceDiscovery` (staging sweep at load removes `transferring-in/` whole, as today), `transfer/StoreTransferManager.scala`
  (`storeDirectory` unchanged; only the caller's directory changes).
- Modify: `startIn`'s earlier-tenancy branch (P1): a copy of the store on the device in any phase other than
  `Arrived(tid)` of this transfer is retired before the receive. A Live, Outbound, Rebuilt or Parked one is moved
  to Lost first (T10, "a copy that stopped being the owner's"), unless served or opening: then it waits, as today.
  The "store path already exists" refusal is deleted.
- Modify: `StoreTransfer.TransferredInMarkerFile` and `StoreDirMarkers.Marker.TransferredIn` deleted.
- Modify: `Transitions.phaseLegal`: Arrived → Live / Lost (T13 is a removal, `unregister`).
- Test: `PhaseDirectoriesSuite`, ported `HostDeviceDiscoverySuite` transfer tests, `StagedTransferServices`.

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: `StoreTransfers.bringArrivedLive(sds: LocalStorageDeviceState, storeId: StoreId): MoveResult`.

- [ ] **Step 1: Write the failing tests** in `PhaseDirectoriesSuite`:
  - `"a received copy is staged under its attempt and promoted into arrived with its transfer id"`: the
    `beginTransferIn` destination is `transferring-in/<transferUUID>`, and after promotion `phaseOf ==
    Some(Arrived(tid.toString))` and the staging attempt directory is gone.
  - `"a committed pool update writes arrived-at, then moves the copy live and serves it"`: `arrived-at` exists in
    the live copy, and `phaseOf == Some(Live)`.
  - `"an arrived copy of another transfer, or with an unusable save file, is deleted and the transfer restarted"`.
  - `"a transfer in retires an earlier tenancy's copy in each non-Live phase, then receives"` (Review Focus 2): for
    each of `Lost`, `Outbound("old")`, `Rebuilt("old")`, `Parked`, one test case. A copy placed before the transfer
    is retired (`retiredCopies` holds it, with its `lost-crl-state.log`) before `beginTransferIn` is called.
  - `"a transfer in waits while an earlier tenancy's Live copy is served, then retires it"`.
  - `"a failed move to live keeps the arrived copy offline and the next check retries it"`.

- [ ] **Step 2: Run to verify they fail**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite'`
  Expected: FAIL.

- [ ] **Step 3: Implement.**

- [ ] **Step 4: Run the new tests, then the full suite**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite *HostDeviceDiscoverySuite'`, then `sbt Test/compile` and
  `sbt test` (70 s).
  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git commit -am "Stage 2 Task 5: Arrived is arrived/; staging per attempt; T8, T9 and T13 are moves; an earlier tenancy is retired whatever its phase"
  ```

---

### Task 6: Live becomes `live/`; markers and the flat path removed

Every copy is now in a phase directory, with a mandatory `copyId`. The loader is offered only `live/` copies.

**Files:**
- Modify: `CopyLayout` (`Live` → `"live"`, `dirOf` total, `path` takes `copyId: UUID`, the flat branch of `list`
  deleted), `CopyRecord.copyId: UUID`, `HostCopies.Found`/records accordingly.
- Delete: `server/host/StoreDirMarkers.scala`, `server/host/CopyMarkers.scala`, `CopyPhase.read`/`known` (the `Phase`
  enum stays, in `CopyPhase.scala` or renamed `Phase.scala`), `Phase.Unread` and every match arm on it
  (`HostCopies.applyPhase`'s fallback re-reads the copy's location: `CopyLayout.locate(devicePath, storeId, copyId): Option[Phase]`).
- Modify: `StoreCreation.createAtomically` and `BackendOpener.create` (target is `live/<copy>`: the caller names a
  fresh `copyId`), `StoreLoader.startOpen(create = true)` and `StoreRebuilds.createNewStore` (the create's record
  takes the `copyId` before the create), `cmdline/Main.scala:1907` and `:1920` (bootstrap creates into
  `live/<copy>` with `CopyLayout.copyName`), `StoreLoader.tryLoadStore` and every load caller (they pass a Live
  record's path, and the stage-1 "non-Live → keep offline" branch is deleted), `DeviceDiscovery` (device load:
  `CopyLayout.list`, P2 and P6; `adoptionClass` by the spec's table: live, arrived, rebuilt, parked → Clean;
  outbound → Clean iff its save file validates, else moved to Lost; lost → CrlLost; an entry with no copy in any
  phase → Unclean), `DeviceShutdown.reloadClosedStores` (Live records only; the "phase cannot be read" branch goes
  with `Unread`), `StoreLoader.storeIdOf` (from the copy name), `TestNetwork` and `ControlFixture` store creation.
- Modify: `Transitions.phaseLegal` to the spec's table T1–T13 plus P10 and Task 4's T13 (Rebuilt → gone). Every
  remaining stage-1-only edge is deleted.
- Test: `PhaseDirectoriesSuite`, `CopyRegistrySuite`, `CopyPhaseSuite` (reduced to the enum or deleted),
  `DeviceAdoptionSuite`, `HostDeviceDiscoverySuite`.

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces: `CopyLayout.locate(devicePath: os.Path, storeId: StoreId, copyId: UUID): Option[Phase]`;
  `BackendOpener.create(storeCfg, storePath, creatingRoot)` unchanged in signature, called with a `live/<copy>`
  path.

- [ ] **Step 1: Write the failing tests** in `PhaseDirectoriesSuite`:
  - `"a new store is created into live under a fresh copy name"`: after `createNewStore`, `CopyLayout.list(dev)`
    holds one copy, phase Live, under `live/`, and nothing remains in `creating/`.
  - `"only live copies are offered to the loader"`: one copy placed in each phase. `storeLoadAttempts` names only
    the Live copy's path.
  - `"two copies of one store at load are both retired and the store rebuilt"` (Review Focus 3): a Live and an
    Outbound copy of one store; after the load and checks, neither is served, the entry is Rebuilding, and every
    copy directory of the store is in `lost/` or `pending-delete/` with its files intact.
  - `"two Live copies of one store at load are both retired"` (spec I1).
  - `"an unreadable phase directory or phase-param leaves the device unloaded until a scan can read it"` (Review
    Focus 4): one case per failure. `loadedDevices(host)` lacks the device, and no copy has moved; after the
    permission is restored, the next scan loads it.
  - Adoption classes, `"adoption classifies by phase"`: one copy per phase and one entry with no copy, asserting
    the spec's table.
  - Bootstrap: `"bootstrap creates its stores under live"`, in the cmdline suite that covers bootstrap, or a
    narrow unit test of the helper `Main` calls.

- [ ] **Step 2: Run to verify they fail**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite *DeviceAdoptionSuite'`
  Expected: FAIL.

- [ ] **Step 3: Implement.** Check `grep -rn "rebuild-required\|transferring-out\|transferred-in\|rebuild-complete\|StoreDirMarkers\|CopyMarkers\|Phase.Unread" src`
  returns nothing outside comments that describe the change. Then read each remaining comment hit and correct it.

- [ ] **Step 4: Run the new tests, then the full suite, then `ThreadedHostSuite` alone**

  Run: `sbt 'testOnly *PhaseDirectoriesSuite *DeviceAdoptionSuite *HostDeviceDiscoverySuite'`, then
  `sbt Test/compile`, `sbt test` (70 s), and `sbt 'testOnly *ThreadedHostSuite'`.
  Expected: PASS. `ThreadedHostSuite` passes unchanged (spec Tests).

- [ ] **Step 5: Commit.** The message lists, by name, every test deleted because its starting state is no longer
  representable (spec Tests, "Port"), e.g. the TI+TO, TO+RC and RR+X combination tests.

  ```bash
  git commit -am "Stage 2 Task 6: Live is live/; every copy has a copyId; markers, StoreDirMarkers, CopyMarkers and Unread removed

  Deleted, no representable starting state: <test names>"
  ```

---

### Task 7: Crash-at-each-step and transition-table suites

The spec's crash and table tests, over the finished layout.

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyCrashSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyRegistrySuite.scala` (table)

**Interfaces:**
- Consumes: everything above. A "crash" is a fresh host built over the same host directory after the step (the
  old host dropped without shutdown), as `CopyRegistryWiringSuite`'s restart tests do.

- [ ] **Step 1: Write the table test** in `CopyRegistrySuite`: `"every (from, to) pair is legal exactly as the
  spec's table says"`. Use the full cross product of `Phase` values (parameters drawn from `""` and one id), with
  the expected legal set written out as the spec's T1–T13 rows (plus P10 and the Rebuilt removal). Claim moves
  refuse every illegal pair and record a violation. Fact moves apply every pair and record a violation for each
  illegal one.

- [ ] **Step 2: Write the crash tests** in `CopyCrashSuite`. Each asserts that after the crash the copy is in
  exactly one phase (`CopyLayout.list` holds it once) and the next check finishes or retries the transition:
  - T6 after the extraction and before the move (`copyRename` throws on the `outbound/` target, then the crash).
  - T11 at each step: after the move to `lost/`, after the lost-state save, after `dropStore`, after the final
    rename and before its sync.
  - T3 and T9 after the rename and before the sync (`syncDirectory` throws, then the crash): the restarted host
    loads the copy from `live/`.
  - T8 after the promotion rename and before the pool update.
  - T1 after the create's rename and before the flip.
  - T4 after the move to `parked/` and before the AdoptOffline flip's end.

- [ ] **Step 3: Run them**

  Run: `sbt 'testOnly *CopyCrashSuite *CopyRegistrySuite'`
  Expected: PASS. A failure is a defect in Tasks 2–6: fix it in this task, and name the fix in the commit message.

- [ ] **Step 4: Run the full suite**

  Run: `sbt Test/compile`, then `sbt test` (70 s).
  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add -A src
  git commit -m "Stage 2 Task 7: crash-at-each-step and transition-table suites"
  ```

---

### Task 8: Documentation, spec status and amendments

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/CLAUDE.md`. Replace the `StoreDirMarkers`, `CopyMarkers`
  and `CopyPhase` paragraphs, the marker-precedence prose and every "keeps it offline" marker case with the
  layout, `CopyLayout`, `CopyMoves` (`MoveResult`, `Unsynced`, resync), the transition table by reference to the
  spec, and P1–P11. Shorten the store-copies section to "every retire goes through Lost": step 1 is the move to
  `lost/`, and step 4 the move to `pending-delete/<copy>`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/rebuild/CLAUDE.md`: `rebuild-complete` becomes the Rebuilt
  phase; `finish` renames staging into `rebuilt/` with the episode as `phase-param`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/CLAUDE.md`, `docs/safety/rebuild-process.md`,
  `docs/safety/crash-recovery-log.md`, `docs/safety/systemic-loss-protection.md`: marker references become phases.
  Add P5's crash window to `systemic-loss-protection.md`'s backend fail-stop row.
- Modify: `docs/superpowers/specs/2026-11-21-store-copy-state-machine-design.md`. Update the status line (stage 2
  done, this plan, its commits). Add a "Stage 2 decisions" block holding P1–P11, with an amendment pointer where
  each changes the spec: P1 under "What disappears", P2 under I1, P3 under Phases, P5 under T10, and P8/P9 closing
  the open questions.
- Modify: `Todo.txt`: close SCM-2 and keep SCM-3.
- Modify: this plan: mark the Decisions block as copied into the spec.

- [ ] **Step 1: Make the edits.** Check every file named above with
  `grep -n "rebuild-required\|transferring-out\|transferred-in\|rebuild-complete\|StoreDirMarkers\|CopyMarkers"`.
  A hit that remains is history, described as such.
- [ ] **Step 2: Run the full suite** (doc-only, as a last gate): `sbt test` (70 s). Expected: PASS.
- [ ] **Step 3: Commit**

  ```bash
  git add -A docs src Todo.txt
  git commit -m "Stage 2 Task 8: documentation, spec status and stage 2 decisions"
  ```
