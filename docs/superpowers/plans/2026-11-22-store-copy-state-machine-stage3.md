# Store copy state machine, stage 3: the reconcile as a decision function — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `DeviceReconciler`'s per-store reconcile passes with one pure function,
`CopyDecision.decide(StoreInputs): List[Action]`, executed by one match. Then test that function over every
combination of entry × copy × work.

**Architecture:** A new pure object, `server/host/CopyDecision.scala`, holds the inputs, the closed `Action`
enum, `decide`, and the pending-delete release rule. It reads no `ctx` and touches no disk. `DeviceReconciler`
handles one store at a time, in store order. For each store it builds that store's inputs from the registry and
the host's other state, calls `decide`, and runs the actions in order through one match onto the existing
handlers. The handlers keep their own claims and guards (R22). The device-level steps (the failed, relinquish and
shutdown branches, resync, pending-lost retries, dropping leftover CRL entries, the rebuild-staging sweep, release)
stay where they are. Task 3 then replaces the two remaining reload marks (`closedForShutdown`, `heldForSync`) with
one rule in `decide`.

**Tech Stack:** Scala 3, ScalaTest `AnyFunSuite`, sbt.

**Spec:** `docs/superpowers/specs/2026-11-21-store-copy-state-machine-design.md`: "Reconcile as a decision
function", "Plan outline" item 3, and "Stage 2 decisions", which governs where it differs from the text above it.
`Todo.txt` SCM-3.

## Global Constraints

- Run on the current branch (`state_refactor`). No worktree. Never use `git stash`. To read an earlier revision,
  use `git show <rev>:<path>`.
- `CopyDecision` is pure. It does not import `DeviceControl`, `Host`, `LocalStorageDeviceState` or `os`. It does
  no I/O and holds no mutable state. Its inputs are plain values built by the caller.
- Registry updates stay facts or claims (R22). `decide` never mutates anything. A refused claim inside a handler
  still backs off as today. Tests fail on any registry violation (`Host.Seams.strictCopyRegistry` /
  `verifyCopyRegistry`).
- The reconcile visits stores in `(poolId.uuid, poolIndex)` order. That covers every store in entries ∪ copies ∪
  work ∪ tracked transfers in, and its actions run in `decide`'s order.
- Scala 3 quiet syntax. Use indented if/then/else. No one-line `if a then b` unless that is clearer.
- Every existence test under `server/` goes through `server.FileStat`; `FileStatUsageSuite` must keep passing. Use
  os-lib for file work.
- Test priorities: correctness, then reliability (no flaky tests), then speed. New tests are pure (`CopyDecision`)
  or deterministic (`LockstepThreading`, ControlFixture).
- Full suite: run `sbt Test/compile` first, then `sbt test` with a 70 s Bash timeout. It takes about 50 s idle. A
  task is done only when the full suite passes.
- A pass's rationale comment (the "why" blocks in `DeviceReconciler`) moves onto its case in `decide` and is
  shortened only where the case's code now says it. It is not dropped.
- When a subsystem's behaviour changes, update its module CLAUDE.md in the same task.

## Decisions

- **D1. Inputs are values, not the registry.** `decide` takes `StoreInputs`. Its `CopyState` carries an
  `ActivityKind` (the `Activity` cases without their payloads), so tests need no `LoadRetry` or
  `LocalStorageDeviceState`.
- **D2. Store-major order replaces pass-major order.** Today every pass runs over all stores before the next pass
  starts. Now each store's actions run before the next store's, and each store's inputs are read fresh at its
  turn. Within one store, the cases come in today's pass order. Across stores, the only shared resources are the
  rebuild slots and the receive attempts, and their handlers guard them. The order in which stores take rebuild
  slots changes from `poolIndex` alone to `(poolId.uuid, poolIndex)`. Cost if wrong: one reconcile starts a
  different store's rebuild first.
- **D3. `decide` enforces I3 itself.** It emits a work-starting action (`Create`, `StartRebuild`,
  `StartTransferIn`) only when the store has no work left after this reconcile's own `EndQueuedRebuild`, or has
  work of the same kind. Today the handler's claim refuses the other combinations and logs them at error as
  registry violations. Now they back off silently in `decide`. Cost if wrong: a state that ought to be reported is
  not logged.
- **D4. `decide` ends a queued rebuild first.** Work `Rebuild(_, Queued)` under an entry that is not Rebuilding
  produces `EndQueuedRebuild`, and the rest of `decide` sees no work. This replaces `endUnlistedWork`'s run
  before the passes.
- **D5. Missing Stores becomes one case.** `DeclareMissing(first)` means "declared (newly if `first`), request its
  rebuild". The executor sets `local.declaredMissing` to exactly the stores given that action in this reconcile.
- **D6. What stays outside `decide`:**
  - the device-status branches (Failed, other host, shutting down);
  - `CopyMoves.resync` and `retryPendingLost`;
  - `dropLeftoverCrlState`, whose stores are known only once the CRL answers;
  - the `rebuilding/` staging sweep (the rest of `abandonUnlisted`), which lists a directory;
  - `StoreCopies.release`, which lists `pending-delete/`. Its per-copy rule becomes the pure
    `CopyDecision.release`.

  `reloadClosedStores` runs after the store loop until Task 3 removes it.
- **D7 (Task 3; settles the Todo's question). `closedForShutdown` and `heldForSync` both become one Live-copy rule,
  `Load`.** An Idle, synced Live copy with no work, whose entry is Active and whose store is not being sent, is
  loaded. The store must also not be served, opening, or held by a rebuild anywhere on the host, its config must
  be present, and it must not be declared missing. Every way of reaching that state today is a copy that should be
  served:
  - a cancelled shutdown's close;
  - a move into Live that landed unforced;
  - a dropped load retry whose reason has gone.

  A copy deliberately held offline is never in it: it is pendingLost, has work, is AwaitingRetry, or sits on a
  device that is stopping serving, whose opens `tryLoadStore` refuses. Cost if wrong: a copy loads one check
  earlier than today, or loads under an Active entry that names a transfer destination. In that case the transfer
  out closes it as it closes any served store.

## Review Focus

1. **A store whose inputs change during the reconcile.** For example, store A's retire callback runs, or a load
   ends while store B is being decided. Because inputs are read per store at its turn (D2), B's decision sees the
   new state. Task 2's test "a reconcile reads each store's inputs at its turn" pins this.
2. **A copy that is Retired and unsynced, or pendingLost.** Neither is offline, so no case may act on it. Task 1's
   exhaustive property A5 pins this.
3. **A tracked transfer in for a store with no entry on this device.** The tracked attempt must still be dropped,
   so the store set includes tracked stores. Task 2's test "a tracked transfer in whose entry is gone is dropped"
   pins this.
4. **A Rebuilding entry over a running rebuild of an earlier episode.** It needs `CancelRebuild` and then
   `StartRebuild`, in that order, in one reconcile, as today. The handler then waits for the cancel. Task 1's test
   "a rebuild of an earlier episode is cancelled before the current one is started" pins this.
5. **A Live copy that is AwaitingRetry under an Active entry after Task 3.** `Load` must leave it to its retry's
   backoff. Task 3's test "a copy awaiting its load retry is left to the retry" pins this.

---

### Task 1: `CopyDecision`: inputs, actions, `decide`, `release`, and the exhaustive test

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/CopyDecision.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/server/host/CopyDecisionSuite.scala`

**Interfaces:**
- Consumes: `Phase`, `Activity`, `Work`, `RebuildStage`, `TransferInStage`, `TransferOutStage`
  (`CopyRegistry.scala`, `CopyPhase.scala`); `StorageDeviceState.StoreEntry` and `StoreStatus`; `StorageDeviceId`;
  `StoreId`.
- Produces (all in `object CopyDecision`):

```scala
enum ActivityKind:
  case Idle, AwaitingRetry, Opening, Served, Closing, Retiring
object ActivityKind:
  def of(a: Activity): ActivityKind

final case class CopyState(phase: Phase, unsynced: Boolean, activity: ActivityKind, pendingLost: Boolean)
final case class TrackedIn(fromDevice: StorageDeviceId, toDevice: StorageDeviceId)

/** What decide cannot see in the device's own state: the rest of the host, and stats of the copy */
final case class StoreFacts(
  servedOnHost: Boolean,        // ctx.router.get(storeId).isDefined
  openingOnHost: Boolean,       // ctx.loader.isOpening(storeId)
  retiringOnHost: Boolean,      // ctx.copies.isRetiring(storeId)
  sending: Boolean,             // ctx.storeTransferService.isTransferringOut(storeId)
  receivingOnHost: Boolean,     // ctx.transfers.receiving.contains(storeId)
  rebuildReadsOnHost: Boolean,  // registry.rebuilding(HostCopies.RebuildReads).contains(storeId)
  rebuildHoldsOnHost: Boolean,  // registry.rebuilding(HostCopies.RebuildHolds).contains(storeId) (used by Task 3)
  rebuildHandle: Boolean,       // StoreRebuilds.runningRebuilds.contains(storeId)
  dirPresent: Option[Boolean],  // registry.existence(local, storeId)
  configPresent: Option[Boolean], // registry.existence(local, storeId, Some(StoreConfig.configFilename))
  declaredMissing: Boolean,     // local.declaredMissing.contains(storeId)
  heldForSync: Boolean,         // registry.heldForSyncOn(local).contains(storeId) (removed by Task 3)
  tracked: List[TrackedIn])     // this store's pendingStartTransfers and transferInRetries, any device
object StoreFacts:
  val none: StoreFacts          // every Boolean false, both existences Some(false), tracked Nil

final case class StoreInputs(deviceId: StorageDeviceId, storeId: StoreId,
                             entry: Option[StorageDeviceState.StoreEntry], copy: Option[CopyState],
                             work: Option[Work], facts: StoreFacts)

/** Declared in pass order: decide emits actions in ordinal order */
enum Action:
  case EndQueuedRebuild
  case TakeOutOfService
  case ForgetGoneCopy
  case Retire
  case DeclareMissing(first: Boolean)
  case RequestRebuild
  case Recheck
  case ReinstateOut
  case BringArrivedLive
  case BringRebuiltLive
  case LoadResynced
  case Create
  case DropTransferIn(fromDevice: StorageDeviceId, toDevice: StorageDeviceId)
  case CancelRebuild
  case StartTransferIn(fromDevice: StorageDeviceId, transferId: Option[UUID])
  case StartRebuild(episode: Option[UUID], acceptLoss: Boolean)

def decide(in: StoreInputs): List[Action]

enum ReleaseDecision:
  case Release, Keep, AskOwner
/** A pending-delete copy of a store whose entry here is `entryHere`, the device holding a Lost or pendingLost copy of
 *  it iff `lostHere` (StoreCopies.release's rule) */
def release(entryHere: Option[StorageDeviceState.StoreEntry], lostHere: Boolean): ReleaseDecision
```

The cases of `decide` transcribe the passes in `DeviceReconciler.reconcileDeviceState`, in this order. In what
follows:
- `work'` is `work` after D4.
- `offline` means the copy exists, is not Retired and not pendingLost, its activity is Idle or AwaitingRetry, and
  `work'` is empty (`HostCopies.offlineCopies`).
- `open` means the copy's activity is Opening or Served (`HostCopies.openCopies`, by `isOffline`: a Closing or
  Retiring copy is not open).
- `active` means the entry's status is Active.
- `noTransferOut` means Active with no `transferDevice`.

| Rule | When | Emits |
|---|---|---|
| D4 | `work` is `Rebuild(_, Queued)`, entry not Rebuilding | `EndQueuedRebuild` |
| Deleted (served) | entry None, copy activity Served | `TakeOutOfService` |
| Deleted (offline) | entry None, `offline` | `ForgetGoneCopy` if `dirPresent == Some(false)`; else `Retire` if `dirPresent.isDefined && !retiringOnHost` |
| Missing | `unserved = !servedOnHost && !open && !openingOnHost && !retiringOnHost`; `active && unserved && (declaredMissing \|\| configPresent == Some(false))` | `DeclareMissing(first = !declaredMissing)` |
| Lost CRL | `offline`, phase Lost, `active` | `Recheck` if `sending`, else `RequestRebuild` |
| Abandoned out | `offline`, phase Outbound(_) or Parked, `noTransferOut` | `Recheck` if `sending`, else `ReinstateOut` |
| Completed in | `offline`, phase Arrived(_), `active`, `!receivingOnHost` | `BringArrivedLive` |
| Completed rebuild | `offline`, phase Rebuilt(_), `active`, `!rebuildReadsOnHost` | `BringRebuiltLive` |
| Resynced | `heldForSync`, `offline`, `!unsynced`, phase Live, `active` | `LoadResynced` |
| New store | entry Initializing, `work'` None or Create | `Create` |
| Aborted in | each `t` in `tracked`: `t.toDevice == deviceId` and not (entry TransferringIn with `transferDevice == Some(t.fromDevice)`); or `t.toDevice != deviceId` and entry TransferringIn | `DropTransferIn(t.fromDevice, t.toDevice)` |
| Unlisted rebuild | `work'` is `Rebuild(ep, Running)`, not (entry Rebuilding with `rebuildEpisode == ep`), `rebuildHandle` | `CancelRebuild` |
| Transfer in | entry TransferringIn with `transferDevice = Some(from)`, `work'` None or TransferIn | `StartTransferIn(from, entry.transferId)` |
| Rebuild | entry Rebuilding, `work'` None or Rebuild | `StartRebuild(entry.rebuildEpisode, entry.acceptLoss)` |

`release`:
- `Some(entry)` that is Active with `!lostHere` → `Release`;
- `Some(_)` otherwise → `Keep`;
- `None` → `AskOwner`.

- [ ] **Step 1: Write the per-rule tests in `CopyDecisionSuite`** (`AnyFunSuite with Matchers`). Use a builder
  `in(entry, copy, work, facts = StoreFacts.none)` over fixed `deviceId`, `otherDevice` and `storeId`. Write one
  positive test per row, plus these guards. Each asserts the exact `List[Action]`.
  - "a queued rebuild under a non-Rebuilding entry ends first, and the copy is then offline": Rebuild(Queued)
    work, entry None, Live Idle copy, `dirPresent = Some(true)` → `List(EndQueuedRebuild, Retire)`.
  - "a served copy with no entry is taken out of service": → `List(TakeOutOfService)`.
  - "an offline copy whose directory is gone is forgotten, one whose stat fails waits" → `List(ForgetGoneCopy)`.
    With `dirPresent = None`, the result is `Nil`.
  - "a copy being retired anywhere on the host is not retired again" (`retiringOnHost`) → `Nil`.
  - "an Active entry with no config is declared missing once, and stays declared while unserved":
    `List(DeclareMissing(true))`, then with `declaredMissing = true` and the config present,
    `List(DeclareMissing(false))`; with `servedOnHost`, `Nil`.
  - "a lost copy under an Active entry is flipped, or rechecked while sending": `List(RequestRebuild)` and
    `List(Recheck)`.
  - "an outbound or parked copy under Active with no transfer device is reinstated, or rechecked while sending";
    an entry with `transferDevice` set → `Nil`.
  - "an arrived copy under Active is brought live unless a receive attempt holds the store" (`receivingOnHost` →
    `Nil`).
  - "a rebuilt copy under Active is brought live unless a rebuild reads the store".
  - "a held Live copy loads only once synced" (`unsynced = true` → `Nil`).
  - "an Initializing entry is created; other work holds it" (`work = TransferIn(..)` → `Nil`).
  - "a tracked transfer in that its entry no longer names is dropped, and one targeted at another device is
    dropped when this device's entry is TransferringIn": two `DropTransferIn` cases.
  - "a rebuild of an earlier episode is cancelled before the current one is started": work
    `Rebuild(Some(e1), Running)`, entry Rebuilding with episode `e2`, `rebuildHandle` →
    `List(CancelRebuild, StartRebuild(Some(e2), false))`.
  - "a running rebuild without its handle is not cancelled" → `List(StartRebuild(..))` only.
  - "a TransferringIn entry starts its transfer in; Rebuild work holds it" → `Nil` with Rebuild(Running) work for
    the same episode.
  - "release: Active and no lost copy here releases; a lost copy keeps it; no entry asks the owner" — every
    StoreStatus × `lostHere`.

- [ ] **Step 2: Write the exhaustive test** "decide keeps I1-I5 and never loads a non-Live copy over every
  combination". Enumerate:
  - entries: None, plus each `StoreStatus`, with `transferDevice` ∈ {None, Some(otherDevice)} and `transferId` /
    episode ∈ {None, Some(e1)};
  - copies: None, plus every phase in `CopyRegistrySuite.phases` × every `ActivityKind` × `unsynced` ×
    `pendingLost`;
  - works: None, Create, `Rebuild(ep, stage)` for every stage with ep ∈ {None, Some(e1), Some(e2)},
    `TransferIn(t, stage)` for every stage, `TransferOut(t, stage)` for every stage;
  - facts: `StoreFacts.none`; every Boolean field true; each Boolean field true alone; `dirPresent` and
    `configPresent` each ∈ {None, Some(true), Some(false)} with the rest none; `tracked` ∈ {Nil,
    `List(TrackedIn(otherDevice, deviceId))`, `List(TrackedIn(otherDevice, thirdDevice))`}.

  Take the product of entries × copies × works, and for each element every facts value. Assert, with a clue
  naming the failing input:
  - **A1 (no load of a non-Live copy):** `LoadResynced` only for phase Live and `!unsynced`; `BringArrivedLive`
    only for Arrived; `BringRebuiltLive` only for Rebuilt; `ReinstateOut` only for Outbound or Parked.
  - **A2 (I2):** none of `LoadResynced`, `BringArrivedLive`, `BringRebuiltLive`, `ReinstateOut`, `Retire`,
    `ForgetGoneCopy`, `RequestRebuild` when the copy is Opening, Served, Closing or Retiring.
  - **A3 (I3):** at most one of `Create`, `StartTransferIn`, `StartRebuild`. Each only when `work'` is None or the
    same kind.
  - **A4 (I5):** `Retire`, `ForgetGoneCopy` and `TakeOutOfService` only when the entry is None.
  - **A5:** no copy-touching action (A2's list) for a copy that is Retired or pendingLost, or that has work after
    D4.
  - **A6:** actions are in non-decreasing `ordinal` order. No action appears twice except `DropTransferIn` with
    distinct arguments.
  - **A7:** `decide` is deterministic. Two calls on equal inputs give equal lists.

  The test must run in under 2 s. If the product is too large, keep the full entries × copies × works product
  and reduce only the facts list.

- [ ] **Step 3: Run the suite and see it fail to compile.**
  Run: `sbt 'testOnly *CopyDecisionSuite'`. Expected: compile errors naming `CopyDecision`.

- [ ] **Step 4: Implement `CopyDecision.scala`** as specified in the Interfaces block and the table. Move each
  pass's rationale comment from `DeviceReconciler` onto its case, as the "why" for that case. In Task 1, leave
  copies of the comments in `DeviceReconciler`; Task 2 deletes the passes.

- [ ] **Step 5: Run the suite and see it pass.**
  Run: `sbt 'testOnly *CopyDecisionSuite'`. Expected: all pass, with the exhaustive test under 2 s.

- [ ] **Step 6: Commit.**
  `git add` the two files and commit: `Stage 3 Task 1: CopyDecision, the reconcile's per-store decision as a
  pure function, with its exhaustive test`.

### Task 2: The reconcile runs `decide`; the passes go

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceReconciler.scala`. Delete
  `reconcileDeletedStores`, `requestMissingStoresRebuild`, `flipLostCrlStores`, `reinstateAbandonedTransfersOut`,
  `completeTransfersIn`, `completeRebuilds`, `loadResynced`, `dropAbortedTransfersIn`, `startTransfersIn`,
  `startRebuilds` and `retireUnowned`'s pass role; add `storeInputs`, `reconcileStores` and `run`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/StoreRebuilds.scala`. Split `abandonUnlisted` and
  `endUnlistedWork`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/StoreCopies.scala`. `release` asks
  `CopyDecision.release`.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/PhaseDirectoriesSuite.scala` (or the lockstep host suite
  whose fixture fits best), for the two integration tests below.
- Docs: `src/main/scala/org/aspen_ddp/aspen/server/host/CLAUDE.md`, the reconcile paragraph.

**Interfaces:**
- Consumes: Task 1's `CopyDecision` API.
- Produces:
  - `StoreRebuilds.endQueuedRebuild(local: LocalStorageDeviceState, storeId: StoreId): Unit`: `endUnlistedWork`'s
    body for one store, unconditionally, since `decide` checked.
  - `StoreRebuilds.cancelRebuild(local: LocalStorageDeviceState, storeId: StoreId): Unit`: `abandonUnlisted`'s
    cancel for one store: work to Abandoned, then `r.sr.cancel()`, with its log line.
  - `StoreRebuilds.sweepUnlistedStaging(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit`: the
    rest of `abandonUnlisted`.
  - `endUnlistedWork` and `abandonUnlisted` are deleted.
  - `private def storeInputs(local, remote, storeId): CopyDecision.StoreInputs` in `DeviceReconciler`, which fills
    every `StoreFacts` field from the source named in Task 1's comments.

- [ ] **Step 1: Write the two integration tests** (lockstep, failing before the rewire only where noted):
  - "a reconcile reads each store's inputs at its turn". Two stores, A and B, on one device: A's copy is Lost under
    an Active entry, and B's is Arrived under Active. Use a seam or a stub so A's `RequestRebuild` handler (via
    `Host.Seams` / `rebuildRequestFailure`, or the fixture's own hook) synchronously starts a receive attempt for
    B. Assert that B is not brought live in that reconcile, and is brought live at the next one once the attempt
    ends. Under the pass-major code this passes too, because B's pass runs after A's. Its job is to pin the
    behaviour across the rewire. If no handler can be hooked synchronously, write the equivalent with a
    `CopyDecision`-level stub of `storeInputs` and say so in the report.
  - "a tracked transfer in whose entry is gone is dropped". A pending start transfer is tracked for a store with
    no entry on the device. One device check drops it (`pendingStartTransfers` no longer has the store).

- [ ] **Step 2: Split `StoreRebuilds.abandonUnlisted` and `endUnlistedWork`** into the three methods in Produces.
  `sweepUnlistedStaging` keeps its `rebuilding(HostCopies.RebuildReads)` check.

- [ ] **Step 3: Rewrite `reconcileDeviceState`'s Operational branch** as below. The device-status branches above
  it are unchanged.

```scala
      dropLeftoverCrlState(local, remote)
      reconcileStores(local, remote)
      ctx.shutdown.reloadClosedStores(local, remote)   // Task 3 removes it
      ctx.rebuilds.sweepUnlistedStaging(local, remote)
      ctx.copies.release(local, remote)
```

  `reconcileStores` takes the union of `remote.stores.keySet`, the device's copy and work keys, and the stores of
  tracked transfers in (`pendingStartTransfers` and `transferInRetries` on any device). It sorts them by
  `(poolId.uuid, poolIndex)`. For each store in turn it builds `storeInputs`, calls `CopyDecision.decide`, and
  runs each action through `run(local, remote, storeId, action)`, one match with one case per `Action`, onto
  today's handler calls:
  - `TakeOutOfService` → `ctx.rebuilds.retireUnlistedServed`.
  - `ForgetGoneCopy` → `registry.removed("retireUnowned: no directory", …)`.
  - `Retire` → today's `retireUnowned` body after its guards, with its log line and the re-check on a clear path.
  - `DeclareMissing(first)` → add the store to `local.declaredMissing`, call `missingIs`, log today's error line
    when `first`, then `requestRebuild`.
  - `Recheck` → `scheduleDeviceRecheck`.
  - `RequestRebuild` → `requestRebuild`.
  - `ReinstateOut` → `reinstateAbandonedOut`.
  - `BringArrivedLive` / `BringRebuiltLive` → today's warn line, the bring call, then `tryLoadStore` on `Durable`.
  - `LoadResynced` → today's info line and `tryLoadStore` with the online gossip.
  - `Create` → `createNewStore`.
  - `DropTransferIn` → `dropAbandonedTransferIn`.
  - `CancelRebuild` → `cancelRebuild`.
  - `StartTransferIn` → today's `startTransfersIn` body for one store, as a private method.
  - `StartRebuild` → `startRebuild`.
  - `EndQueuedRebuild` → `endQueuedRebuild`.

  Before the loop, the executor clears `local.declaredMissing` of every store. Each `DeclareMissing` re-adds its
  store, and `missingIs` runs once after the loop as well. Delete the pass methods listed under Files. Update the
  scaladoc of `reconcileDeviceState` to describe the loop.

- [ ] **Step 4: Make `StoreCopies.release` ask `CopyDecision.release(remote.stores.get(storeId), lostHere)`** and
  match on the three outcomes. Its lookup body for `AskOwner` is unchanged.

- [ ] **Step 5: Run the targeted suites.**
  Run: `sbt 'testOnly *CopyDecisionSuite *PhaseDirectoriesSuite *DeviceShutdownSuite *StoreRebuildTriggerSuite *StoreCopiesSuite *WorkOverlapSuite *DerivedSilenceSuite'`.
  Expected: all pass.

- [ ] **Step 6: Run the full suite.**
  Run: `sbt Test/compile`, then `sbt test` (70 s timeout). Expected: `Tests: succeeded N, failed 0`. A test that
  depended on pass-major order across two stores is a D2 finding. Report it, and do not reorder the loop to hide
  it.

- [ ] **Step 7: Update `server/host/CLAUDE.md`'s reconcile description.** The passes become `CopyDecision`'s
  cases, store-major (D2). Name the device-level steps that stay (D6).

- [ ] **Step 8: Commit.** `Stage 3 Task 2: the device reconcile runs CopyDecision.decide per store; the passes go`.

### Task 3: One Live-copy reload rule replaces `closedForShutdown` and `heldForSync` (D7)

**Files:**
- Modify: `CopyDecision.scala`: `LoadResynced` becomes `Load`, by D7's rule; delete `StoreFacts.heldForSync`.
- Modify: `DeviceReconciler.scala`: `run`'s `Load` case is today's `LoadResynced` call with the gossip; delete the
  `reloadClosedStores` call.
- Modify: `DeviceShutdown.scala`: delete `closedForShutdown`, its writes and `reloadClosedStores`.
- Modify: `HostCopies.scala`: delete `heldForSync`, `heldForSyncOn` and every write of them. Keep `unsyncedPaths`
  and the `unsynced` flag.
- Test: `CopyDecisionSuite`, `DeviceShutdownSuite`.
- Docs: `server/host/CLAUDE.md` (cancelled shutdown, unsynced loads), the `closedForShutdown` bullet in the spec's
  "Stage 1 status".

**Interfaces:**
- Consumes: Task 2's executor.
- Produces: `Action.Load` (same ordinal position as `LoadResynced`). `StoreFacts` loses `heldForSync`.

`Load` fires when every one of these holds:
- the copy is `offline` and its activity is Idle;
- the phase is Live and `!unsynced`;
- the entry is `active` and `!sending`;
- `configPresent == Some(true)`;
- `!declaredMissing`, `!servedOnHost`, `!openingOnHost` and `!rebuildHoldsOnHost`.

- [ ] **Step 1: Write the tests.**
  - In `CopyDecisionSuite`, "an idle synced Live copy under Active loads" (→ `List(Load)`), plus one negative case
    per conjunct of the rule above, each → `Nil`.
  - "a copy awaiting its load retry is left to the retry" (AwaitingRetry → `Nil`).
  - Extend the exhaustive test's A1: `Load` only for phase Live, `!unsynced`, activity Idle.
  - In `DeviceShutdownSuite`, keep "restart during shutdown cancels it", "a cancelled shutdown's closed store held
    by other work is loaded once the work ends" and "a cancelled shutdown offers a store whose phase cannot be
    read" unchanged. The last one's comment at line 335 names `closedForShutdown`; reword it, not its assertions.
  - In `StoreRebuildTriggerSuite`, "a flipped rebuild whose move to live landed but whose directory sync failed is
    loaded by a later check" stays unchanged. So does `PhaseDirectoriesSuite`'s "a rebuilt copy placed unforced is
    recorded unsynced, forced, then flipped and served: one rebuild".

- [ ] **Step 2: Run `sbt 'testOnly *CopyDecisionSuite'`.** Expected: the new tests fail to compile (no
  `Action.Load`).

- [ ] **Step 3: Implement D7** as listed under Files.

- [ ] **Step 4: Run the targeted suites, then the full suite.**
  Run: `sbt 'testOnly *CopyDecisionSuite *DeviceShutdownSuite *StoreRebuildTriggerSuite *PhaseDirectoriesSuite *CopyMovesSuite *CopyCrashSuite'`,
  then `sbt Test/compile` and `sbt test` (70 s). Expected: all pass.
  - If an existing test fails because a copy now loads that it expected offline, stop. Report the test and the
    state, and do not weaken the assertion: that is a counterexample to D7.

- [ ] **Step 5: Update the docs** listed under Files.

- [ ] **Step 6: Commit.** `Stage 3 Task 3: one Live-copy load rule in CopyDecision replaces closedForShutdown and
  heldForSync`.

### Task 4: Spec, Todo, and the stage's decisions

**Files:**
- Modify: `docs/superpowers/specs/2026-11-21-store-copy-state-machine-design.md`:
  - the status line (stage 3 done, with plan path and commit range);
  - a "Stage 3 decisions" section with D1–D7 and the execution rulings;
  - an amendment pointer in "Reconcile as a decision function". The signature now takes one `StoreInputs` (D1,
    and P2's one copy per store), and `HostFacts` is `StoreFacts`.
- Modify: `Todo.txt`: close SCM-3.
- Modify: `docs/safety/*.md` only where a section names a deleted pass method by name. Grep for the method names
  deleted in Tasks 2–3.

- [ ] **Step 1: Make the edits.**
- [ ] **Step 2: Run `sbt test`** (70 s). Expected: it passes. This is a docs-only change, so the run checks that
  nothing else moved.
- [ ] **Step 3: Commit.** `Stage 3 Task 4: spec status, stage 3 decisions, Todo`.
