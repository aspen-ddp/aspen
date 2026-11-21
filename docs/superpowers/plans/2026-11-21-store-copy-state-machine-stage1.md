# Store Copy State Machine, Stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every store copy one in-memory record (phase, activity, work), derive router silence and
repairability from it, and delete the per-concern sets that encode a copy's state. The on-disk format stays as it
is: marker files.

**Architecture:**
- One function reads a copy's phase from its markers. It replaces the five precedence lists.
- A per-device registry (`DeviceCopies`) is updated at every site that changes a copy on disk or in memory. Its
  transitions are checked against one table.
- It first runs in shadow mode, with test-only checks that compare it to disk and to the old sets. Then readers
  switch to it and the old sets are deleted, cluster by cluster.

**Tech Stack:** Scala 3, sbt, ScalaTest. Host control thread, `LockstepThreading` in tests. `server.FileStat` and
`DurableFiles`.

**Spec:** `docs/superpowers/specs/2026-11-21-store-copy-state-machine-design.md`. Stage 1 is "Registry over today's
disk". Read the spec's Phases and transitions, Registry and Router-silence sections first.

## Global Constraints

- Stage 1 changes no on-disk format. Markers, paths and file contents are untouched, and so is
  `<device>/<storeId.directoryName>`. `copyId`, phase directories and `Parked` are stage 2.
- Copies are keyed by `StoreId` per device in stage 1: one path per store per device means at most one copy per
  device.
- Every existence test uses `server.FileStat` (`FileStatUsageSuite` enforces it). A failed stat is never an absence.
- Registry state is control-thread only (`server/CLAUDE.md` threading rules). The router's silent set stays a
  concurrent set written only by the control thread.
- An illegal transition throws `IllegalStateException`. Production code must not catch it in order to continue.
- Scala 3 quiet syntax, indented if/then/else.
- Tests are deterministic (`LockstepThreading`), and none may be flaky. Run full suites with `sbt test` under a
  70 s timeout (CLAUDE.local.md).
- Commit directly on main, one commit per task at least. End messages with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Every task that changes host behaviour updates `server/host/CLAUDE.md` in the same commit (CLAUDE.md
  Development Rules).

## Review Focus

1. **Failed stat or read of a marker while building or refreshing a record.** The copy becomes `Phase.Unread`,
   stays silent, is retried through the existing load backoff, and is never treated as Live. Test owner: Task 2,
   `CopyPhaseSuite` "a failed stat throws"; Task 4, `CopyRegistryWiringSuite` "an unreadable copy registers
   Unread and stays silent".
2. **A store served from device A while device B lists it.** Device B may be unloaded, retiring an old copy, or a
   rebuild destination. The store stays served and repairable. Test owner: Task 6, `DerivedSilenceSuite` "served
   on one device dominates every other device's record".
3. **Restart.** A registry rebuilt from disk after `TestNetwork.restartHost` equals the pre-crash registry with
   activities reset to Idle and work dropped. Test owner: Task 4, `CopyRegistryWiringSuite` "a restart rebuilds the
   same phases".
4. **Reload over a loaded store** (`TestNetwork.restartStore`, a reload reusing the backend). It must be a legal
   activity transition and must not leave the copy Idle while installed. Test owner: Task 7,
   `CopyRegistryWiringSuite` "a reload over a served store stays Served".
5. **A device unloaded while its copies have activity or work** (shutdown, relinquish, `crlFailed`, Failed
   retirement). Its tree-listed stores stay silent, and no record of it answers Served. Test owner: Task 6,
   `DerivedSilenceSuite` "an unloaded device's listed stores stay silent until it loads".

---

### Task 1: Audit and fix the stage-1 tables

This task produces no code, only a findings section appended to the spec. It gates every later task: Tasks 3–9
use the tables it fixes.

**Files:**
- Modify: `docs/superpowers/specs/2026-11-21-store-copy-state-machine-design.md` (append
  `## Stage 1 audit findings`)

- [ ] **Step 1: Disk mutation sites.**
  - List every production call that creates a store directory, moves one into or out of `<device>/<storeDir>`,
    writes or removes one of the four markers, or deletes a copy.
  - Start from `StoreCreation`, `RebuildingStore.finish`, `StoreTransfers` (promotion, `clearTransferredInMarker`,
    `discardArrivedTransferIn`), `StoreTransfersOut` (TO write and remove), `StoreRebuilds`
    (`writeRebuildRequiredMarker`, `keepRebuiltStoreOffline`, `clearRebuildCompleteMarker`), `StoreCopies`
    (`retire`, `delete`, `moveToPendingDelete`), `LocalDevices.discardStoreDirectory` and `DeviceDiscovery`
    (adoption's TO removal, lost-CRL marking).
  - Give each site its `file:line`, the phase before and after, and whether it runs on the control thread. Sites
    off it, such as `RebuildingStore.finish`, report through an outcome the control thread handles; name that
    handler.
- [ ] **Step 2: I3, at most one Work per (device, store).** For each pair among {create, rebuild (queued, discard,
  running), transfer in (retire earlier tenancy, receive, record, abandon), transfer out (close, extract, ship),
  retire}, say whether both can be in flight for one store on one device, citing the guard that prevents it.
  Retire is listed as activity `Retiring`, not as Work; record any overlap with Work anyway. If a pair can
  overlap, write down the counterexample and stop. The spec is amended before Task 3.
- [ ] **Step 3: Silence readers.** Confirm that the router's `offline`, `loading` and `awaitingDevice` marks are read
  only by `StoreRouter.answersUnknown`, `StoreRouter.isOffline` (`DeviceShutdown.storesToClose`) and
  `router.offlineStores` (`HostStoreView.of`, i.e. `repairable`), plus tests. Record any other reader and what it
  needs.
- [ ] **Step 4: Reload paths.** List the production and test paths that install a store over a loaded one
  (`StoreRouter.install` with `replaced != null`). They fix whether `Served -> Opening` is legal (Task 3's
  table).
- [ ] **Step 5: Marker combinations.**
  - For each pair of markers, say whether production can produce it, with a citation. Known reachable:
    TI+RC (a shipped Rebuilt copy) and TO+RC (the crash window in `keepRebuiltStoreOffline`).
  - Check that the single precedence RR > TO > TI > RC gives, at all five current sites, the same decision as
    today for every reachable combination. Sites: `tryLoadStore` (`StoreLoader.scala:109`), `adoptionClass`
    (`DeviceDiscovery.scala:330`), `holdsOwnState` (`DeviceDiscovery.scala:681`), `reloadClosedStores`
    (`DeviceShutdown.scala:355`), `startIn` (`StoreTransfers.scala:372-430`).
  - List each behaviour change. Known: an adopted TI+TO copy becomes Outbound and so needs a valid save file.
- [ ] **Step 6: Commit** `Stage 1 audit: disk mutation sites, I3, silence readers, reload paths, marker combinations`.

### Task 2: One phase reader

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/CopyPhase.scala`
- Modify: `StoreLoader.scala` (`tryLoadStore`), `DeviceDiscovery.scala` (`adoptionClass`, `holdsOwnState`),
  `DeviceShutdown.scala` (`reloadClosedStores`), `StoreTransfers.scala` (`startIn` marker tests)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyPhaseSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  enum Phase:
    case Live
    case Arrived(transferId: String)            // transferred-in content, trimmed
    case Outbound(transferId: Option[UUID])     // transferring-out; empty content -> None
    case Rebuilt(episode: Option[UUID])         // rebuild-complete; empty content -> None
    case Lost                                   // rebuild-required
    case Unread                                 // stage 1 only: never returned by read; the registry's record of a copy whose read threw
  object CopyPhase:
    def read(storePath: os.Path): Phase         // precedence RR > TO > TI > RC; throws a failed stat or read
    def known(storePath: os.Path): Option[Phase]// None where read throws
  ```

- [ ] **Step 1: Write `CopyPhaseSuite`.** Each test builds a store directory in a temp dir.
  - "no marker is Live".
  - "each single marker maps to its phase with its content". Cases: TI `"t1"` → `Arrived("t1")`; TO `""` →
    `Outbound(None)`; TO with a UUID → `Outbound(Some(u))`; RC with a UUID → `Rebuilt(Some(u))`; RR → `Lost`.
  - "precedence is RR > TO > TI > RC". Cases: RR+TO → `Lost`; TO+TI → `Outbound`; TI+RC → `Arrived`; TO+RC →
    `Outbound(None)`.
  - "a failed stat throws and known answers None". Use a directory without search permission, as the existing
    FileStat tests do.
- [ ] **Step 2: Run** `sbt 'testOnly *CopyPhaseSuite'`. Expected: compile failure (`CopyPhase` not found).
- [ ] **Step 3: Implement `CopyPhase`** with `StoreDirMarkers.has` for the stats and `os.read(...).trim` for content.
- [ ] **Step 4: Replace the five sites** with a `match` on `CopyPhase.read` (or `known`, where the site uses
  `knownPresent` today). Apply exactly the decisions and behaviour changes from Task 1 Step 5. Add one test per
  listed behaviour change to the suite that covers that site (`DeviceAdoptionSuite`, `StoreTransferSuite`, ...).
  `StoreDirMarkers.firstPresent` is deleted.
- [ ] **Step 5: Run** `sbt 'testOnly *CopyPhaseSuite *DeviceAdoptionSuite *HostDeviceDiscoverySuite
  *DeviceShutdownSuite'`, then the full suite. Expected: PASS.
- [ ] **Step 6: Commit** `CopyPhase is the one reader of a store directory's markers; the five precedence lists go`.

### Task 3: Registry types and the transition table

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/CopyRegistry.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyRegistrySuite.scala`

**Interfaces:**
- Consumes: `Phase` (Task 2).
- Produces:
  ```scala
  final case class CopyRecord(storeId: StoreId, phase: Phase, unsynced: Boolean, activity: Activity)
  enum ClosePurpose: case TransferOut, Shutdown, Relinquish, FailStop
  enum Activity:
    case Idle
    case AwaitingRetry(retry: StoreLoader.LoadRetry)
    case Opening(openId: Long)
    case Served
    case Closing(purpose: ClosePurpose)
    case Retiring
  enum RebuildStage: case Queued, DiscardingLost, Running, Abandoned
  enum TransferInStage: case RetiringEarlierTenancy, Receiving, Recording, Abandoning
  enum Work:
    case Create
    case Rebuild(episode: Option[UUID], stage: RebuildStage)
    case TransferIn(transferId: Option[UUID], stage: TransferInStage)
    case TransferOut(transferId: Option[UUID])
  object Transitions:
    def phaseLegal(from: Phase, to: Phase): Boolean
    def activityLegal(phase: Phase, from: Activity, to: Activity): Boolean
  final class DeviceCopies(val deviceId: StorageDeviceId, val devicePath: os.Path, onChange: StoreId => Unit):
    def copies: Map[StoreId, CopyRecord]
    def work: Map[StoreId, Work]
    def missing: Set[StoreId]
    def register(storeId: StoreId, phase: Phase): Unit     // throws if a record exists (I1)
    def setPhase(storeId: StoreId, to: Phase): Unit
    def setActivity(storeId: StoreId, to: Activity): Unit
    def setUnsynced(storeId: StoreId, unsynced: Boolean): Unit
    def unregister(storeId: StoreId): Unit                 // the copy left its path: pending-delete or deleted
    def startWork(storeId: StoreId, w: Work): Unit         // throws if work exists (I3)
    def updateWork(storeId: StoreId, w: Work): Unit        // throws if absent or of another case
    def endWork(storeId: StoreId): Unit
    def declareMissing(storeId: StoreId): Unit
    def clearMissing(storeId: StoreId): Unit
  ```
  Every mutator calls `onChange(storeId)` after it succeeds.

The stage-1 phase table (`phaseLegal`) comes from Task 1 Step 1.
- Expected legal moves:
  - `Live -> Lost | Outbound`
  - `Outbound -> Live | Lost`
  - `Arrived -> Rebuilt | Live | Lost`
  - `Rebuilt -> Live | Outbound | Lost`
  - `Unread -> any`
  - `any but Lost -> Unread`
- `Lost` moves only by `unregister`.
- `Outbound(a) -> Outbound(b)` is illegal.

Activity table (`activityLegal`):
- `Idle -> Opening` only when the phase is `Live`.
- `Opening -> Served | Idle | AwaitingRetry`.
- `Idle | AwaitingRetry -> AwaitingRetry`, and `AwaitingRetry -> Opening | Idle`.
- `Served -> Closing(_)`, and `Closing(_) -> Idle`.
- `Idle -> Retiring` only when the phase is `Lost`, and `Retiring -> Idle`.
- Plus `Served -> Opening` if Task 1 Step 4 found a reload path.
- `setActivity(_, Opening)` also throws while the store has `Work.TransferOut`.

- [ ] **Step 1: Write `CopyRegistrySuite`.**
  - "every legal phase move succeeds and every other pair throws". Enumerate all pairs over representative values
    of each case.
  - "every legal activity move succeeds and every other pair throws", per phase.
  - "register over an existing record throws".
  - "startWork over existing work throws".
  - "Opening is refused during TransferOut work".
  - "onChange runs once per successful mutation and not on a throw".
- [ ] **Step 2: Run** `sbt 'testOnly *CopyRegistrySuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `CopyRegistry.scala`.** Use immutable maps in `var`s. `Transitions` is two plain `match`
  expressions.
- [ ] **Step 4: Run** the suite. Expected: PASS.
- [ ] **Step 5: Commit** `Copy registry types and the stage-1 transition table`.

### Task 4: Wire the registry in shadow mode, with a disk check

The registry is kept up to date everywhere but nothing reads it yet.

**Files:**
- Modify: `DeviceControl.scala` (add `lazy val copyRegistry: HostCopies`), `LocalDevices.scala`
  (`LocalStorageDeviceState` gains `val copies: DeviceCopies`), `DeviceDiscovery.scala` (device load registers
  every store directory, using `CopyPhase.known`, with `Unread` for `None`), every site in Task 1 Step 1's list,
  `Host.scala` (`Host.Seams.verifyCopyRegistry: Boolean`, default false; `testingOnlyHandleEvents` runs the check
  after each drain when it is set), and `TestNetwork.scala` (sets it true).
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/HostCopies.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyRegistryWiringSuite.scala`, using `ControlFixture`;
  `injectLoadedDevice` registers through the same path as device load.

**Interfaces:**
- Consumes: Task 3's `DeviceCopies`.
- Produces:
  ```scala
  final class HostCopies(ctx: DeviceControl):
    def device(id: StorageDeviceId): Option[DeviceCopies]
    def attach(sds: LocalStorageDeviceState): Unit                 // at load; registers the scanned directories
    def detach(id: StorageDeviceId, listed: Set[StoreId]): Unit    // at unload; keeps `listed` as unloaded entries
    def recordUnloadedEntries(id: StorageDeviceId, listed: Set[StoreId]): Unit // DeviceReconciler's None branch
    def awaitDevice(path: os.Path, stores: Set[StoreId]): Unit     // replaces router.markAwaitingDevice
    def deviceDecided(path: os.Path): Unit                         // replaces clearAwaitingDevice
    def verifyAgainstDisk(): List[String]                          // test-only; empty when consistent
  ```
- `verifyAgainstDisk` checks, for every loaded device:
  - each record's phase equals `CopyPhase.known(path)`, except records that are `Unread` or `unsynced`;
  - each store directory on disk with a config has a record;
  - no record lacks a directory.

  It returns the mismatches; the seam fails the drain with them.

- [ ] **Step 1: Write `CopyRegistryWiringSuite`.**
  - "a device load registers each store directory with its phase": one directory each of Live, Lost, Outbound,
    Arrived and Rebuilt.
  - "an unreadable copy registers Unread and stays silent": a directory without search permission.
  - "a restart rebuilds the same phases". After a TestNetwork transfer and a rebuild, `restartHost` gives equal
    phases, all activities `Idle`, and no work.
- [ ] **Step 2: Run** the suite. Expected: FAIL (no registration).
- [ ] **Step 3: Wire registration and every disk mutation site.** At each site from Task 1 Step 1, call
  `register`, `setPhase`, `unregister` or `setUnsynced` right after the durable step succeeds. Activity and work
  are not wired yet: every record stays `Idle`.
- [ ] **Step 4: Run** the suite, then the full suite. Every TestNetwork suite runs the disk check after each drain.
  Expected: PASS. A mismatch means a site is missing from Task 1's list; add it to the list and to the wiring.
- [ ] **Step 5: Commit** `Copy registry tracks every store directory's phase (shadow), checked against disk after
  each test drain`.

### Task 5: Wire activity and work in shadow mode, and compare silence

**Files:**
- Modify: `StoreLoader.scala` (Opening / Served / AwaitingRetry / Closing), `StoreRouter.scala` (`install`,
  `remove` report to the registry), `StoreRebuilds.scala` (`Work.Rebuild`, `Work.Create`), `StoreTransfers.scala`
  (`Work.TransferIn`), `StoreTransfersOut.scala` (`Work.TransferOut`, `Closing(TransferOut)`), `StoreCopies.scala`
  (`Retiring`), `DeviceShutdown.scala` (`Closing(Shutdown | Relinquish)`), and `HostCopies.scala` (`silence`,
  `repairable` and `compareWithRouter`).
- Test: `CopyRegistryWiringSuite`, more cases.

**Interfaces:**
- Produces:
  ```scala
  enum SilenceReason: case CopyHere, Opening, Rebuilding, TransferringIn, Missing, UnloadedDevice, AwaitingDevice
  enum Silence:
    case Served
    case Silent(reasons: Set[SilenceReason])
    case Unknown
  // HostCopies
  def silence(storeId: StoreId): Silence
  def repairable(storeId: StoreId): Boolean
  def compareWithRouter(): List[String]   // test-only
  ```
- `silence` follows the spec's "Router silence is derived":
  1. It is `Served` if any loaded device has the store's record with activity `Served`.
  2. Otherwise it is `Silent` if any of these holds:
     - a loaded device has a record (`CopyHere`, or `Opening` when that is its activity);
     - a loaded device has `Work.Rebuild` (`Rebuilding`) or `Work.TransferIn` (`TransferringIn`);
     - a loaded device has the store in `missing` (`Missing`);
     - unloaded entries list it (`UnloadedDevice`);
     - `awaitDevice` lists it (`AwaitingDevice`).
  3. Otherwise it is `Unknown`.
- `repairable(s)` is true when:
  - `silence(s) == Served`;
  - the Served record's activity is not `Closing`;
  - no `Work.Rebuild` or `Work.TransferOut` exists for `s` on any device;
  - `storeTransferService.transferringOutStores` does not contain `s`.
- `compareWithRouter` lists each store where the two answers to "is it silent" disagree:
  - the old answer is `router.isOffline || loading || awaitingDevice`, for a store not installed;
  - the new answer is `silence` being `Silent`.

  It does the same for the view's `repairable`.

- [ ] **Step 1: Write the tests.**
  - "a load moves Idle -> Opening -> Served".
  - "a failed open leaves AwaitingRetry".
  - "a transfer out runs Closing(TransferOut) under Work.TransferOut and ends Outbound + Idle".
  - "a rebuild holds Work.Rebuild from its first reconcile to its flip".
  - "a reload over a served store stays Served". Use `TestNetwork.restartStore`.
- [ ] **Step 2: Run** them. Expected: FAIL.
- [ ] **Step 3: Wire activity and work** at each site that today writes the corresponding old set (the mapping table
  below). The old sets stay.
- [ ] **Step 4: Add the comparison to the drain check** (same seam as Task 4). Run the full suite. For each
  disagreement, either fix the wiring, or record it under Task 1's findings as a defect of the old marks, with the
  scenario. A recorded defect is whitelisted in `compareWithRouter`, by test name, until Task 6 removes the old
  marks. Expected known defect: `DeviceReconciler`'s unloaded-device branch silences a store served from another
  device.
- [ ] **Step 5: Commit** `Copy registry tracks activity and work (shadow); derived silence compared with the router's
  marks after each test drain`.

Mapping from old sets to registry state. This table is the reference for Tasks 5–8:

| Old state | Registry |
|---|---|
| router `offline`, device `offlineStores`, `silenced`, `markStoreOffline`, `markCopyOffline`, `clearOffline*` | derived `silence` |
| router `loading`, `StoreLoader.opening`, `openCounts`, `opensStarted` | `Activity.Opening(openId)` |
| router `awaitingDevice` | `HostCopies.awaitDevice` |
| `storesAwaitingRetry`, `buildRetryWaits` | `Activity.AwaitingRetry(retry)` |
| `LocalStorageDeviceState.loadedStores` | records with activity `Opening` or `Served` |
| `declaredMissing` | `DeviceCopies.missing` |
| `rebuildingStores`, `rebuildDevices`, `rebuildEpisodes`, `abandonedRebuilds`, `droppingCrlState` | `Work.Rebuild(episode, stage)` |
| `creatingStores` | `Work.Create` |
| `transferringIn`, `recordingTransfersIn`, `abandoningTransferIns` | `Work.TransferIn(id, stage)` |
| `closingForTransfer`, `abortingLostTransfers` | `Work.TransferOut` |
| `StoreCopies.retiring` | `Activity.Retiring` |
| `closedForShutdown` | stays (device-level, `DeviceShutdown`) |
| `pendingMarkerRemovals` | `CopyRecord.unsynced` (Task 8) |

These are kept as they are, being retry scheduling and not copy state: `rebuildFailures`, `scheduledRebuildRetries`,
`rebuildsSettled`, `pendingRebuildRequests`, `pendingStartTransfers`, `transferInRetries`, `releaseLookups`, and
`DeviceReconciler`'s and `DeviceDiscovery`'s check bookkeeping.

### Task 6: Switch the router to derived silence; delete the offline marks

**Files:**
- Modify:
  - `StoreRouter.scala`: delete `offline`, `loading`, `awaitingDevice`, `markOffline`, `clearOffline`,
    `clearOfflineUnlessHeld`, `markLoading`, `clearLoading` and the awaiting-device methods. Add
    `private val silent` and `private[host] def setSilent(storeId, silent: Boolean)`. `answersUnknown` reads
    `silent`, and `isOffline` becomes `copyRegistry.silence(_) != Unknown`.
  - `DeviceControl.scala`: delete `markStoreOffline` and `markCopyOffline`. `publishView` takes `repairable` from
    `HostCopies`, and `HostStoreView.of` takes `repairable` directly.
  - `HostCopies.onChange`: recomputes `silence(storeId)`, calls `router.setSilent`, then `publishView`.
  - `LocalDevices.scala`: delete `offlineStores`.
  - `StoreRebuilds.scala`: delete `silenced`, `silencedStores` and `unsilence`.
  - The `DeviceReconciler` unloaded-device branch calls `recordUnloadedEntries`.
  - `DeviceShutdown.storesToClose`: records with activity `Served` (not `Closing`).
  - Every reconcile pass that iterates `offlineStores` iterates the device's records whose activity is not
    `Served`/`Opening` instead.
  - Delete the Task 5 comparison and its whitelist.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/DerivedSilenceSuite.scala`. Port the tests listed by the
  grep in Step 3.

- [ ] **Step 1: Write `DerivedSilenceSuite`.**
  - "served on one device dominates every other device's record". Device A serves s, and device B holds a Lost
    copy of s, a Rebuild of s and an unloaded entry for s. Assert `answersUnknown(s)` is false, the message is
    delivered, and `isRepairable(s)` is false only while B's Rebuild exists.
  - "an unloaded device's listed stores stay silent until it loads".
  - "a store whose last record and work go is Unknown".
  - "the unloaded-device branch does not silence a served store". This is the regression test for the Task 5
    defect.
- [ ] **Step 2: Run** it. Expected: FAIL (the old marks still decide).
- [ ] **Step 3: Make the switch.** `grep -rn "offlineStores\|markOffline\|clearOffline\|markCopyOffline\|markStoreOffline\|silencedStores\|markLoading\|AwaitingDevice" src`
  must return no code hits afterwards. Port test assertions on `offlineStores` to `copyRegistry.device(id).copies`
  or `silence`.
- [ ] **Step 4: Run** `DerivedSilenceSuite`, then the full suite. Expected: PASS.
- [ ] **Step 5: Update `server/host/CLAUDE.md`.** Replace the offline-mark paragraphs (threading model, "The
  router's offline mark is by store id ...", `markStoreOffline`) with derived silence and the registry.
- [ ] **Step 6: Commit** `Router silence and repairability are derived from the copy registry; the offline, loading
  and awaiting-device marks and offlineStores go`.

### Task 7: Delete the load and open sets

**Files:**
- Modify:
  - `StoreLoader.scala`: delete `storesAwaitingRetry`, `buildRetryWaits`, `opening`, `openCounts` and
    `opensStarted`. `retryFailedLoads` iterates `AwaitingRetry` records, and `isOpening` reads the activity.
  - `StoreTransfersOut.scala`: the extraction gate is `Work.TransferOut` refusing `Opening` (Task 3), so its
    `opensStarted` comparisons and `isClosingForTransfer` go.
  - `LocalDevices.scala`: delete `loadedStores`.
  - The callers of all of these.
- Test: existing `HostFsifLoadSuite`, `StoreRebuildTriggerSuite` and transfer-out suites, ported. Add to
  `CopyRegistryWiringSuite`: "an open started during a transfer-out extraction throws, and the extraction ships".

- [ ] **Step 1: Write the new test.**
- [ ] **Step 2: Run** it. Expected: FAIL, because the old gate allows the open and fails the extraction instead.
- [ ] **Step 3: Delete the sets and port their readers and tests.** The backoff arithmetic in `awaitUnreadRetry`
  and `awaitBuildRetry` is unchanged: it now reads the previous `AwaitingRetry`.
- [ ] **Step 4: Run** the full suite. Expected: PASS.
- [ ] **Step 5: Update** `server/host/CLAUDE.md` (store creation and load retry paragraphs; the transfer-out "must stay
  closed" paragraph).
- [ ] **Step 6: Commit** `Load retries and opens are copy activities; storesAwaitingRetry, opening, opensStarted and
  loadedStores go`.

### Task 8: Delete the work sets, unify the predicates, and make unsynced uniform

**Files:**
- Modify:
  - `StoreRebuilds.scala`: delete `rebuildingStores`, `rebuildDevices`, `rebuildEpisodes`, `abandonedRebuilds`,
    `droppingCrlState`, `creatingStores` and `pendingMarkerRemovals`. Replace `isRebuilding` and
    `rebuildingStoreIds` with one `HostCopies.rebuilding: Set[StoreId]` (any `Work.Rebuild`, any stage).
  - `StoreTransfers.scala`: delete `transferringIn`, `recordingTransfersIn` and `abandoningTransferIns`.
  - `StoreTransfersOut.scala`: delete `closingForTransfer` and `abortingLostTransfers`.
  - `StoreCopies.scala`: delete `retiring`.
  - Create `CopyMarkers.scala`:
    `write(sds, storeId, marker, content): Unit` and `remove(sds, storeId, marker): Boolean`, both through
    `DurableFiles`. A failed directory sync after a successful unlink or write sets `unsynced`. Each device check
    re-syncs unsynced records and clears the flag.
  - Every marker write and remove goes through `CopyMarkers`. That includes `clearTransferredInMarker`'s plain
    `os.remove`, the adoption TO removal and `clearRebuildCompleteMarker`.
  - One `HostCopies.mayReinstateOut(entry, storeId): Boolean` replaces the three copies of that predicate
    (`DeviceReconciler`, `DeviceDiscovery.bringIntoService`, `DeviceShutdown.reloadClosedStores`).
- Test: `CopyRegistryWiringSuite`, more cases. Port the existing `pendingMarkerRemovals` tests (`syncMarkerDirectory`
  seam, renamed `CopyMarkers.syncDirectory`).

- [ ] **Step 1: Write the tests.**
  - "a failed sync after removing any marker leaves the copy unsynced and unloaded, and the next check loads it".
    Run it for TI, TO and RC.
  - "rebuilding answers the same through a discard and a run". A discard hands its slot to the rebuild.
- [ ] **Step 2: Run** them. Expected: FAIL. TI and TO have no forced sync today.
- [ ] **Step 3: Delete the sets** and port the readers to `work` and `activity`. Route marker I/O through
  `CopyMarkers`.
- [ ] **Step 4: Run** the full suite. Expected: PASS.
- [ ] **Step 5: Update** `server/host/CLAUDE.md` and `server/rebuild/CLAUDE.md` (rebuild slot, silence, the
  rebuild-complete removal paragraph).
- [ ] **Step 6: Commit** `Rebuild, transfer and retire work live in the copy registry; marker removal is uniformly
  durable, with unsynced copies held back`.

### Task 9: Keep the registry check permanent, and close out the docs

**Files:**
- Modify: `Host.scala` (`verifyCopyRegistry` defaults to true in `ControlFixture` seams too), test suites that hand-place
  copies (each calls `host.testingOnlyRescan(device)`, a new test hook that registers hand-placed directories as a
  device scan would), the spec (stage 1 marked done, audit findings kept), and `Todo.txt` (close the items stage 1
  subsumes; add stage 2 and 3 entries).

- [ ] **Step 1: Turn the check on for every suite.** Run the full suite and fix each suite that hand-places copies
  without a rescan.
- [ ] **Step 2: Run** the full suite twice under load (a CPU-saturating load alongside, e.g. `stress-ng --cpu 0` if installed; CLAUDE.local.md allows it).
  Expected: PASS both times.
- [ ] **Step 3: Commit** `Copy registry disk and transition checks run in every host test; stage 1 complete`.
