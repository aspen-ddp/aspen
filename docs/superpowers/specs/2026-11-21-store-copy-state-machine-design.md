# Store copy state machine

**Status:** stage 1 (registry over today's disk) is done, 2026-11-22 (plan
`docs/superpowers/plans/2026-11-21-store-copy-state-machine-stage1.md`, commits `cde3d375..` on main). Stage 2 (phase
directories replacing the per-copy marker files) is done, 2026-11-22 (plan
`docs/superpowers/plans/2026-11-22-store-copy-state-machine-stage2.md`, commits `e104a53d..59c39437` on branch
`state_refactor`, plus test-only flake fixes `44e08ebe`, `34739243` and `ad7eedb1`); its decisions are in "Stage 2
decisions" below, with an amendment pointer where each changes the text. Stage 3 (the reconcile as a decision
function) is done, 2026-11-22 (plan `docs/superpowers/plans/2026-11-22-store-copy-state-machine-stage3.md`, commits
`b46456b4..836428bb` on branch `state_refactor`, then this status line); its decisions are in "Stage 3 decisions"
below. The stage 1 audit findings below are kept as history. The sections below describe the design as drafted; the
four marker files they name are history after stage 2.

## Intent

A store copy's lifecycle (served, closed, lost, retiring, retired, rebuilt, transferring in or out) is currently
spread across three representations that must agree:

- four marker files in the store directory (`rebuild-required`, `transferring-out`, `transferred-in`,
  `rebuild-complete`), whose combinations are read by at least five sites with their own precedence:
  `tryLoadStore` (RR, TO, TI, RC), `adoptionClass` (RR, TI, TO; RC ignored), `holdsOwnState`,
  `reloadClosedStores`, `StoreTransfers.startIn`;
- the tree entry's `StoreStatus`;
- about forty control-thread collections. Those that encode a copy's state are the router's `offline`, `loading`
  and `awaitingDevice`, the device's `offlineStores` and `declaredMissing`, and `storesAwaitingRetry`,
  `buildRetryWaits`, `silenced`, `pendingMarkerRemovals`, `droppingCrlState`, `closingForTransfer`, `retiring`,
  `closedForShutdown`, `transferringIn`, `recordingTransfersIn`, `rebuildingStores` and `abandonedRebuilds`.

An audit of 44 recent lifecycle fixes (Nov 2026; roughly 90 of the 288 commits in `host/`, `rebuild/`, `crl/` and
`store/backend` are of this kind) found:

- 12 caused by in-memory sets disagreeing with each other or with disk (e.g. RB-W23, X-W31, X-W13: an offline mark
  keyed by store id hiding or unhiding another device's copy);
- 7 by markers misread or given the wrong precedence (92a5c2e9, SL-W10, X-W19);
- 4 by two copies of a store on one host, or one path reused across tenancies and episodes (RB-W3, RB-W13);
- the rest by crash windows, failed stats, races and tree-vs-disk disagreement.

The code says outright that the structure is the problem: `StoreTransfersOut` calls two sets and a marker "three
representations of one fact [that] must not be allowed to disagree". "Not served here" is derived in seven places
and "served or opening" in about ten.

This change gives every store copy exactly one persisted phase, represented by which directory the copy is in. The
phase changes only by an atomic, durable rename. It gives each copy exactly one in-memory record, from which every
other view (router silence, the published `HostStoreView`, "is X running here") is derived. A copy that is not in
a loadable phase is not in the directory the loader reads, so it cannot be loaded by mistake.

## Goals

- One persisted phase per copy. No combination of markers exists, so no precedence rule exists.
- Legal transitions are listed in one table (below) and checked by one function. An illegal transition is a
  programming error, but production never stops on one (R19, R22; "Facts and claims" under Registry): a fact the
  table refuses is applied anyway, and a claim it refuses is refused while its caller backs off. Each violation is
  logged at error in production, and fails the test that made it.
- Every phase change is one `DurableFiles.move`. Every move has the same durability policy, including the handling
  of a rename whose directory sync failed (`Unsynced`, below), which replaces `pendingMarkerRemovals` and the three
  current marker-removal policies (`os.remove` with a directory sync, plain `os.remove`, `DurableFiles.remove`).
- Every copy has a unique directory name, so a copy left by an earlier tenancy or episode never sits at the path a
  new copy needs (the RB-W3, RB-W13, `retireEarlierTenancy` and `retireForeignCopy` path collisions).
- One in-memory registry of copies and host work per device. Router silence is computed from it, never set directly.
- The reconcile's decision per store is a pure function of (tree entry, copies, work), testable over every
  combination.

## Non-goals

- Device-level markers (`owning-host`, `shutdown.state`, `failed`), device CRL loss detection and `crl.lost-*`.
  They are unchanged.
- `FileStat` semantics. Listing a phase directory still distinguishes a failed listing from an empty one.
- CRL contents, admission accounting, extraction and the save-file format.
- Tree-vs-disk detection (Missing Stores) and the retry backoffs. Both stay, as rules in the decision function and
  as data in the registry.
- Compatibility with existing device directories. Persisted-data compatibility is not required. Devices are
  recreated.

## Anti-goals

- A phase file (`state.yaml`) in place of directories. It keeps the "found by scanning the device root, then
  filtered" structure that let marked copies load, and a crash between writing the file and acting on it is again a
  combination of state.
- A generic state-machine library or DSL. The table is plain Scala: an enum and one match.
- Optimized recovery for states that well-behaved code cannot produce (two Live copies of one store on one device).
  Those copies are retired and the store rebuilt (simplicity over corner-case optimization).

## Approaches considered

1. **Keep markers; centralize the reads.** Put one precedence function in `StoreDirMarkers` and make every site use
   it. Cheap, but combinations stay representable, the in-memory sets stay, and the commit history shows that
   shared precedence lists drift (92a5c2e9).
2. **A single phase file per copy.** Removes combinations, but the loader still finds unloadable copies and must
   filter them. Rejected (anti-goals).
3. **Phase directories plus one in-memory registry (chosen).** The location of a copy is its phase. A copy can only
   be found by code that looks in its phase's directory.

## Design

### Device layout

```
<device>/
  storage-device-config.yaml, owning-host, shutdown.state, failed   (unchanged)
  crl/ crl.creating/ crl.lost-<uuid>/                                 (unchanged)
  live/<copy>/          Live: may be loaded and served
  arrived/<copy>/       Arrived: transferred in, awaiting its pool update
  outbound/<copy>/      Outbound: closed for transfer out; state in crl_save.log
  rebuilt/<copy>/       Rebuilt: finished rebuild awaiting its entry's flip out of Rebuilding
  parked/<copy>/        Parked: complete copy whose entry is Offline (rebuild flipped AdoptOffline)
  lost/<copy>/          Lost: its CRL state is gone or unusable; awaits retire
  pending-delete/<copy>/  Retired: kept until the store is Active on its owner device
  creating/ deleting/ transferring-in/<id>/ rebuilding/<id>/          scratch (unchanged)
```

- `<copy>` is `<storeId.directoryName>.<copyId>`. `copyId` is a fresh UUID assigned when the copy's directory is
  first made on this device: a create in `creating/`, a transfer's staging and a rebuild's staging. It never changes
  across moves on the device. A copy shipped to another device gets a new `copyId` there. The store id comes from
  the name, never from parsing the contents (as `StoreCopies.pendingDeleteStore` already does).
- A phase that carries a parameter holds it in its own file, `phase-param.<phase directory>` (`phase-param.arrived`,
  `phase-param.outbound`, `phase-param.rebuilt`), written durably into the copy before the move into that phase.
  Outbound and Arrived carry the transfer id, and Rebuilt carries the episode. Only the phase the location names
  reads its file, and a move writes only its target's file. A file of another phase is therefore inert, and a failed
  rename or a crash before it leaves the source's parameter as it was. (As first drafted, one file `phase-param` was
  overwritten by the next move, which was claimed inert; that is false for a move between two parameter phases, such
  as Rebuilt(e) to Outbound(t): a failed rename would leave `rebuilt/` reading t. Amended by R-T1, "Stage 2
  decisions".)
- The files that are not phase (`aspen-store-config.yaml`, `incarnation`, `acceptor-since`, `inherited-votes`,
  `arrived-at`, `crl_save.log`, `lost-crl-state.log`, `lost-crl-entries.log`) keep their meaning and move with the
  copy.
- The four marker files and `StoreDirMarkers` are removed.

Backends open at the path the registry gives them. Every phase change happens while the copy is closed (no move is
ever made of an installed store's directory), so no backend sees its directory move. The first implementation step
checks that neither RocksDB nor FSIF records its own absolute path.

### Phases and transitions

`enum Phase: Live, Arrived(transferId), Outbound(transferId), Rebuilt(episode), Parked, Lost, Retired`

| # | From | To | Trigger (today's code) | Precondition |
|---|------|----|------------------------|--------------|
| T1 | `creating/` | Live | `createNewStore` (`StoreCreation`) | no Live copy of the store on the device |
| T2 | `rebuilding/<id>` | Rebuilt(ep) | `RebuildingStore.finish` | episode is the entry's |
| T3 | Rebuilt | Live | flip committed (`Adopt`), or the Completed Rebuilds pass on an Active entry | no Live copy |
| T4 | Rebuilt | Parked | flip committed `AdoptOffline` (today an empty `transferring-out` written by `keepRebuiltStoreOffline`) | none |
| T5 | Parked | Live | entry Active with no `transferDevice` (Abandoned Transfers Out pass, adoption) | no Live copy |
| T6 | Live / Rebuilt / Parked | Outbound(tid) | `StoreTransfersOut.closedForTransfer`, after the extraction into `crl_save.log` (Parked, an empty TO, is also a source: a parked rebuilt copy transferred out; stage 1 `Outbound("") -> Outbound(tid)`) | copy closed, its work is `TransferOut(tid)` |
| T7 | Outbound | Live | transfer aborted or abandoned: entry Active, no `transferDevice` (`reinstateAbandonedOut`, adoption) | no Live copy |
| T8 | `transferring-in/<id>` | Arrived(tid) | promotion after validating the save file | no Arrived copy for another transfer (else delete it first) |
| T9 | Arrived | Live | pool update committed (`recordTransferIn`, `completeTransfersIn`); `arrived-at` written first | no Live copy |
| T10 | Live, Outbound, Arrived, Rebuilt, Parked | Lost | backend failure or fail-stop, failed close, missing incarnation, unloadable save file, lost device CRL (except copies that hold their own state), or a copy that stopped being the owner's | copy closed (P5: a served copy's close ends first, so the move never happens while its backend is open) |
| T11 | Lost | Retired | retire: save CRL state into `lost-crl-state.log`, `dropStore`, move | the store has no running retire on this device |
| T12 | Retired | gone | `StoreCopies.release`, rename-first via `deleting/` | store Active on its owner device |
| T13 | Arrived | gone | unusable or stale arrival, rename-first (source still owns the state) | none |

Amendments from stage 2 (see "Stage 2 decisions"): a retired copy is not recorded, and `Phase.Retired` is only a move
target (P3, R-P1); T6's sources are Live, Rebuilt, Parked and Outbound of the same or another transfer, and the stage
1 combination of a transfer in with a transfer out is gone (P10, as amended); T10 is made only when the copy's close
ends (P5, extended); T8's staging is per attempt (P7).

Notes on the table:

- **Every retire goes through Lost.** This is what writing `rebuild-required` as step 1 does today. A crash
  anywhere in T11 leaves the copy in `lost/`, and the next retire redoes the remaining steps. A Live, Rebuilt,
  Parked or Outbound copy that is no longer the owner's (Deleted Stores, foreign copy, earlier tenancy) is moved to
  Lost first. A copy in `lost/` is never loaded, transferred or promoted.
- **A shipped Rebuilt copy.** A Rebuilt copy whose flip was Unconfirmed can be shipped (today it travels with
  `rebuild-complete`). The destination treats it as any arrival: T8, then T9. Today's destination already removes
  the marker at pool-update commit before loading, so this keeps the behavior and removes a marker from the
  arrival path.
- **Adoption** classifies by phase alone:
  - live, arrived, rebuilt and parked copies are Clean;
  - an outbound copy is Clean if its save file validates and goes Lost (T10) otherwise;
  - a lost copy is CrlLost;
  - a tree entry whose store has no directory in any phase is Unclean.

  This is a table lookup on the phase, replacing `adoptionClass`'s precedence list.
- **A lost device CRL** moves every live, outbound and parked copy to Lost (T10), except one that holds its own
  state. That exception is a copy whose `crl_save.log` validates as its own, which can be Live after T3/T9 or
  Outbound. Rebuilt copies are left alone, as `holdsOwnState` does today for `rebuild-complete`.
- **Cancelled shutdown.** A shutdown's closed stores stay Live on disk with their state in the device CRL, so
  "closed for shutdown" is work in memory, not a phase (as now: nothing on disk says so).

### Uniform move and `Unsynced`

`CopyMoves.move(copy, to: Phase, param: Option[String])`:

1. write the target phase's `phase-param.<phase directory>` durably if `param` is set;
2. `DurableFiles.move(from, to)`, which forces both directories.

If step 2's rename succeeds but a directory sync fails (P11: `DurableFiles.move` throws `MoveNotForced` for exactly this,
and any other throw means nothing moved), the copy's record takes the new phase with
`unsynced = true`. An unsynced copy is never loaded, shipped or released. Each device check retries the sync of
both directories, which `DurableFiles` already makes idempotent, and clears the flag on success. If the rename
itself fails, the copy stays in its old phase and the action is retried by the pass that issued it. This one rule
replaces `pendingMarkerRemovals`, the TO and TI removal paths' missing sync, and the "marker whose removal failed
keeps it offline" special cases in `completeTransfersIn`, `completeRebuilds` and `bringIntoService`.

### Registry (control thread)

```scala
final case class CopyRecord(storeId: StoreId, copyId: UUID, phase: Phase, unsynced: Boolean, activity: Activity)

enum Activity:   // volatile; Idle after every restart
  case Idle
  case AwaitingRetry(retry: LoadRetry)                  // storesAwaitingRetry, buildRetryWaits
  case Opening(openId: Long)                            // loading, opening, opensStarted
  case Served(store: Store)                             // installed in the router
  case Closing(purpose: ClosePurpose)                   // TransferOut(tid) | Shutdown | Relinquish | FailStop | Reload | Unserved
  case Retiring                                         // retiring, droppingCrlState

enum Work:       // per (device, store); at most one
  case Rebuild(episode, stage)                          // rebuildingStores, rebuildEpisodes, abandonedRebuilds, silenced
  case TransferIn(tid, stage)                           // transferringIn, recordingTransfersIn, abandoningTransferIns
  case TransferOut(tid)                                 // closingForTransfer, abortingLostTransfers
  case Create                                           // creatingStores
```

`DeviceCopies` (one per loaded device) holds `copies: Map[UUID, CopyRecord]` and `work: Map[StoreId, Work]`, plus
the last reconciled tree entries (`reconciledEntries`) and `declaredMissing` (as a derived fact: an Active entry
with no Live copy and no work).

- It is built at device load by listing the phase directories. A listing that fails leaves the device unloaded for
  the next scan, as a failed CRL open does.
- Every phase or activity change goes through the registry's moves (stage 1: `DeviceCopies.setPhase` /
  `setActivity` / `startWork` for claims, `applyPhase` / `applyActivity` for facts). They check the table and the
  per-phase invariants, and the change republishes the view.
- Opens are started only by an Opening claim (stage 1: `HostCopies.opening`) on an Idle Live copy whose store has
  no `TransferOut` work. That one check replaces the `opensStarted` counter and `isClosingForTransfer`.

**Facts and claims (R19, R22).** Every registry update is one of two kinds, and an illegal one is handled by kind:

- A **fact** records something that already happened: an install, a router removal, the end of a close, open,
  retire or retry, a retire's start, a phase a disk mutation reached, a copy leaving its path, a copy a load found.
  The registry mirrors what is true, so a fact is applied even when the table refuses it. A refused phase is not
  taken on trust: the record is set to what the copy's directory reads (`CopyPhase.known`, Unread where it cannot be
  read).
- A **claim** takes ownership of something about to start that another actor may hold: Opening, AwaitingRetry, a
  Work's start or update, a new record. A claim the table refuses is refused (`false`), and the caller backs off: it
  opens nothing and starts no work, and a later check asks again. Forcing a claim through could hide a served store
  from a shutdown, or open a copy that is being retired, so a claim is never applied over a refusal.
- Each violation, of either kind, is a bug. Tests fail on any recorded violation: a strict host
  (`Host.Seams.strictCopyRegistry`, implied by `verifyCopyRegistry`) fails its next lockstep drain. Production logs
  it at error, with its throwable, and keeps nothing: a refused claim is retried at every check, so a kept record
  would grow without bound.
- The silence path is neither. A throw while deriving a store's silence (below) leaves the store silent, the
  conservative answer, since clients retransmit; it is logged at error in production, and a strict host's
  propagates (`SilenceFailure`, which neither a fact nor a claim swallows).

Invariants checked by the registry's moves. A violation is handled as above (applied for a fact, refused for a
claim), and tests assert it never happens:

- I1. At most one Live copy of a store per device. A device load that finds two retires both (T10) and logs an
  error. The entry is then rebuilt through the normal path. **Amended by P2:** at most one copy of a store per device
  outside `pending-delete/`, in any phase, with the retire as P2 describes.
- I2. Only a Live copy has activity Opening or Served.
- I3. A store has at most one Work per device, and a Work's copy (if any) is on that device.
- I4. A Retired copy is released only by T12.
- I5. No copy that is Live and whose device entry is Active (the owner's copy) ever moves to Lost, except by the
  failure triggers of T10.

Step 1 of the plan verifies I3 against today's code. In particular, a discard handing its slot to the rebuild
(`discardLostStore`) and `retireEarlierTenancy` before a transfer in become stages of one Work, not two. If the
check finds a real counterexample, the spec is amended rather than the invariant weakened silently.

### Router silence is derived

The router keeps its installed-store map and one `silent: Set[StoreId]`, which the registry recomputes after every
transition, for the affected store only. A store is:

- served if a copy of it is Served on any device of this host;
- otherwise silent if any of these holds:
  - some loaded device has a non-Retired copy of it, or Rebuild, TransferIn or Create work for it (a create's copy
    is silent from its claim until it is served);
  - a loaded device's latest reconcile read its entry Initializing (its create not started, or the device shutting
    down before it starts);
  - a loaded device has declared it missing (an Active entry with no `config.yaml` at its path, being rebuilt by
    the Missing Stores pass: the device's `missing` mark);
  - some unloaded device's last tree entry lists it;
  - its device's adoption or shutdown-marker decision is pending (`awaitingDevice`);
- otherwise unknown.

`markOffline`, `clearOffline`, `clearOfflineUnlessHeld`, `markStoreOffline`, `markCopyOffline`, the device's
`offlineStores`, `loading` and `awaitingDevice` as separately written sets, and `silenced` are removed.

Being served dominates every other condition. That fixes, by construction, the unguarded `router.markOffline` for
an unloaded device's entries in the device check (`DeviceReconciler`, the `None` branch after the tree lookup),
which can silence a store served from another device of the host. It also covers the per-store-id offline-mark
class (RB-W23, X-W31, X-W13). Where the differences among today's offline, loading and awaitingDevice marks matter
to a reader (`isRepairable`, `HostStoreView`), the derivation returns a reason alongside `silent`. Step 1
inventories those readers.

### Reconcile as a decision function

```scala
def decide(entry: Option[StoreEntry], copies: List[CopyRecord], work: Option[Work], facts: HostFacts): List[Action]
```

**Amended by stage 3 (D1):** `decide` takes one `StoreInputs` (the entry, the store's one copy per P2, its work and
`StoreFacts`) and returns `List[Action]`; `HostFacts` is `StoreFacts`. See "Stage 3 decisions".

`HostFacts` holds what the function cannot see in the device's own state: the store is served or opening on
another device of this host, the pool's owner device is Active, the rebuild slot is free, and the retry holds.
`Action` is a closed enum (`Load`, `Move(copy, phase)`, `Retire`, `Release`, `RequestRebuild`, `StartRebuild`,
`StartTransferIn`, `ReinstateOut`, `FlipLostToRebuilding`, ...). `reconcileDeviceState` becomes: for each store in
entries ∪ copies ∪ work, `decide`, then run the actions through the registry. The thirteen passes become cases of
one match, ordered by phase, then entry status.

### What disappears

- `StoreDirMarkers`, the four marker constants and their 18 direct uses, and the five precedence lists.
- `keepMarkedCopyOffline`, `keepUnpromotedCopyOffline`, `keepRebuiltStoreOffline` (replaced by T4) and the
  overloaded empty `transferring-out` marker.
- `pendingMarkerRemovals`, `clearRebuildCompleteMarker`'s forced-sync special case and `clearTransferredInMarker`'s
  plain remove.
- The two "is a rebuild running" predicates (`isRebuilding` vs `rebuildingStoreIds`, which disagree during a
  discard), and the three copies of the "may reinstate an abandoned transfer out" predicate.
- The path-collision branches of `startIn` (`retireEarlierTenancy`'s path clearing, the "store path already exists"
  refusal; **amended by P1:** the retire itself stays, before a new copy is placed, only the path checks go) and `retireForeignCopy`'s staging-checkpoint dance. The latter is kept only to the extent that a foreign
  copy may have served since a first pass began: that copy is Lost, and the checkpoint rule stays.

**Stage 1 status.** Stage 1 kept the markers (the phase is read from them by `CopyPhase`, the one ranking,
RR > TO > TI > RC), so the items tied to the on-disk format wait for stage 2.
- Gone: `pendingMarkerRemovals` and both marker-removal special cases (every marker write and removal goes through
  `CopyMarkers`, durable, with `unsynced` for a failed sync); the two "is a rebuild running" predicates (one
  `HostCopies.rebuilding(stages)`, each caller naming its stages); the three reinstate predicates (one
  `HostCopies.mayReinstateOut`); `keepMarkedCopyOffline` and `keepUnpromotedCopyOffline`; and every in-memory set the
  Intent lists except those below (the router's `offline`, `loading` and `awaitingDevice`, `offlineStores`,
  `storesAwaitingRetry`, `buildRetryWaits`, `silenced`, `droppingCrlState`, `closingForTransfer`, `retiring`,
  `transferringIn`, `rebuildingStores`, `abandonedRebuilds`, and `opensStarted`). Silence is derived; opens, retries,
  closes and retires are a copy's Activity; Create, Rebuild, TransferIn and TransferOut are its Work.
- Kept until stage 2: `StoreDirMarkers` and the marker constants (the reconcile passes' single-marker tests),
  `keepRebuiltStoreOffline` and its empty `transferring-out` marker (Parked), `startIn`'s path-collision
  branches, and `retireForeignCopy`'s staging-checkpoint dance (`StoreRebuilds`: a foreign or rebuild-required copy
  at a rebuilding store's path may have served since the first pass began, so the checkpoint is dropped durably
  before the copy is retired). Stage 2's unique copy directories remove the path collision; the checkpoint rule
  stays for a foreign copy that may have served.
- Error handling: stage 1 ends with the fact/claim split above (R19, R22), not with throws on production paths.
- Kept, because Work and Activity cannot hold what they carry:
  - `pendingCreateFlips` (`StoreRebuilds`): `Work.Create` ends when the copy is served, on purpose, so a rebuild or
    transfer out of the served store never overlaps the create (I3). The flip out of Initializing that follows
    touches no copy, and only stops a check meanwhile from creating the store again.
  - `recordingTransfersIn` (`StoreTransfers`): it holds the promise a device shutdown waits on, and an abandon's stage
    can overlay the Recording stage.
  - `runningRebuilds` (`StoreRebuilds`): it holds the `StoreRebuild` handle a cancel needs and the slot's settled
    promise. It also counts slots, and keeps a rebuild single flight across its device's unload, which drops the
    device's Work.
  - `closedForShutdown` (`DeviceShutdown`): a cancelled shutdown leaves the stores it closed offline with nothing on
    disk to say so, and the record of a closed copy is Idle like any other offline copy. **Removed in stage 3
    (D7)**, with `HostCopies.heldForSync`: one Live-copy rule, `CopyDecision.Load`, loads an Idle, synced Live copy
    with no work under an Active entry, not being sent, whose store is not served, opening or held by a rebuild on
    the host, whose config is not known absent (R9) and which is not declared missing. A copy held offline on purpose
    is never in that state.
  - `declaredMissing` (mirrored as the device's `missing`), `transferInRetries`, and the retry backoffs, which are not
    copy state.

### What stays

`FileStat` everywhere a phase directory is listed; the Missing Stores rule (Active entry, no Live copy, no work →
`RequestRebuild`); load, build and rebuild backoffs (now `Activity.AwaitingRetry` and the existing
`HeldRebuild`); the retire's four durable steps; the CRL save-file handling; the forget floor and `arrived-at`; and
the device-level markers.

## Plan outline

Each stage leaves the full suite green and is reviewable on its own.

1. **Registry over today's disk.** Done (2026-11-22). The registry is authoritative; every lockstep test host fails
   its drain on a fact or claim violation and on any disagreement with the disk (`Host.Seams.verifyCopyRegistry`).
   What it removed and kept: "Stage 1 status" under "What disappears".
   - Introduce `Phase`, `CopyRecord`, `Activity`, `Work` and `DeviceCopies`. Phase is computed from the marker
     files by one function, the only one that reads markers.
   - Derive router silence. Delete the in-memory sets listed above.
   - Verify I3 and the readers of the silence reason.
   - Expected effect: most of the in-memory bug class is gone, with no format change.
2. **Phase directories.** Done (2026-11-22; Task 8 is this documentation). The decisions are in "Stage 2 decisions".
   - Replace markers with the layout and `CopyMoves`, and give copies `copyId` names. Delete `StoreDirMarkers` and
     the marker-reading phase function.
   - Update bootstrap (`cmdline`, `DefaultBackendOpener.createStore`) to create into `live/`, and update the
     transfer service to ship from `outbound/` and stage into `transferring-in/`.
3. **Decision function.** Done (2026-11-22). The decisions are in "Stage 3 decisions".
   - Extract `decide` and replace the passes.
   - Add an exhaustive test over every (entry status × up to two copies × each phase × activity × work)
     combination, asserting I1–I5, the store-copies release rule, and "no Action loads a non-Live copy".

## Tests

All deterministic (LockstepThreading) unless noted.

- **Table:** every illegal (from, to) pair is refused by the claim moves (they throw, and `claim` turns that into a
  refusal) and recorded by the fact moves, and every legal one moves the directory and updates the record.
- **Crash at each step of each transition** (a fresh `DeviceCopies` built from the disk after the step): the copy
  is in exactly one phase, and the next check finishes or retries the transition. This covers T6 after the
  extraction and before the move, T11 at each step, and T3/T9 after the rename and before the sync.
- **`Unsynced`:** a failed sync (`syncMarkerDirectory`'s seam generalized to `CopyMoves`) keeps the copy unloaded
  and the next check loads it.
- **I1:** two Live copies of a store found at load are both retired, and the store is rebuilt.
- **Derived silence:** a store served from device A stays served while device B, unloaded or retiring a copy, lists
  it. This is the regression test for the `DeviceReconciler` unloaded-device branch and RB-W23/X-W31.
- **Adoption:** the classification by phase for each phase, with an outbound copy with and without a valid save
  file.
- **Port:** existing suites for transfers, rebuilds, adoption, lost CRLs and store copies are ported by replacing
  marker writes with phase placement. A test that no longer has a representable starting state is deleted, with its
  name listed in the stage's commit message.
- `ThreadedHostSuite` passes unchanged.

## Documentation

- `server/host/CLAUDE.md`:
  - replace the `StoreDirMarkers` paragraph, the marker-precedence prose and the "keeps it offline" special cases
    with the layout, the transition table (by reference to this spec), the registry and derived silence;
  - shorten the store-copies section to "every retire goes through Lost".
- `server/rebuild/CLAUDE.md`: `rebuild-complete` becomes the Rebuilt phase. `finish` moves the staging copy into
  `rebuilt/` with the episode as `phase-param.rebuilt` (R-T1).
- `server/crl/CLAUDE.md`, `docs/safety/rebuild-process.md`, `crash-recovery-log.md`, `systemic-loss-protection.md`:
  rename the marker references to phases. No safety argument changes, since each marker maps to one phase.
- `Todo.txt`: close the items this subsumes.

## Open questions

Both questions were resolved in stage 2 (see "Stage 2 decisions").

- Whether `Outbound` needs to keep a Rebuilt copy's episode in its parameter file so the source side can still
  recognize an Unconfirmed flip after a crash mid-transfer. **Resolved by P8: no.** An Outbound copy goes back to Live
  (T7) only for an Active entry with no `transferDevice` (`mayReinstateOut`), and Active means the flip committed, so
  the source needs nothing from the old `rebuild-complete`.
- Whether `Parked` merges into Live with an Offline entry, if step 1 shows that `reloadClosedStores` and T5 are the
  same rule. **Resolved by P9: Parked stays a phase** (the layout's `parked/`), and `reloadClosedStores` stays a
  Live-copy rule. Stage 3 may revisit it.

## Stage 2 decisions

Decisions the stage 2 plan made where this spec left a choice open or had to be amended, with the controller rulings
made during execution. Each carries an amendment pointer in the section it changes: P1 under "What disappears", P2
under I1, P3 and P10 under "Phases and transitions", P5 under T10, R-T1 under "Device layout" and "Uniform move", and
P8 and P9 close the open questions above. Where this block and the text above disagree, this block governs.

### Constraints

- Run on the current branch (`state_refactor`). No worktree. Never use `git stash`: read an earlier revision with
  `git show <rev>:<path>`.
- Persisted-data compatibility is not required (spec Non-goals): there is no migration of flat copies or markers,
  and devices are recreated.
- Every existence test under `server/` uses `server.FileStat`. A listing of a phase directory distinguishes a
  failed listing from an empty one (spec Non-goals). `FileStatUsageSuite` must keep passing.
- Use os-lib for filesystem work. java.nio only where os-lib has no equivalent (DSYNC channels, directory fsync).
- Every phase change is one `CopyMoves.move`. It writes the target phase's parameter file (`phase-param.<phase directory>`, R-T1) durably first when
  that phase carries a parameter, then renames through `DurableFiles.move`. No other code renames a copy between phases.
- No move is ever made of an installed store's directory. Every phase change happens while the copy is closed
  (spec Device layout; Step 1 amendment on T10).
- Each layout name, verbatim: `live/`, `arrived/`, `outbound/`, `rebuilt/`, `parked/`, `lost/`, `pending-delete/`;
  scratch areas `creating/`, `deleting/`, `transferring-in/<id>/`, `rebuilding/<id>/`; copy name
  `<storeId.directoryName>.<copyId>`; parameter files `phase-param.arrived`, `phase-param.outbound` and `phase-param.rebuilt` (R-T1).
- Registry updates stay facts or claims (R22). A move's registry update is a fact. Tests fail on any violation
  (`Host.Seams.strictCopyRegistry` / `verifyCopyRegistry`). Production logs it at error.
- Scala 3 quiet syntax, indented if/then/else, no one-line `if a then b` unless clearer.
- Test priorities: correctness, then reliability (no flaky tests), then speed. Every new test is deterministic
  (`LockstepThreading`, ControlFixture) unless it names a real-time hop and waits on it with `awaitLoudly`.
- Full suite: run `sbt Test/compile` first, then `sbt test` with a 70 s Bash timeout. It takes about 50 s idle. A
  task is done only when the full suite passes.
- When a subsystem's behaviour changes, update its module CLAUDE.md in the same task (Task 8 does the full rewrite).

### Decisions

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
- **P6. A failed listing or `phase-param.*` read at device load leaves the device unloaded** for the next scan, as a
  failed CRL open does. A parameter file known absent reads as `""`, which matches no transfer or episode.
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

### Rulings made during execution

- **R-P1. `Phase.Retired` is a move target only.** Only Lost to Retired is legal. A copy leaves the registry when its
  move to `pending-delete/` is durable (P3).
- **R-P2. Hybrid period.** During Tasks 1-5 (before the flat path was removed) marker files inside a copy in a phase
  directory were inert, and `CopyMoves` stripped them. Stage 2 ended with no marker anywhere (Task 6).
- **R-T1. One parameter file per phase (amends "Device layout").** Each phase keeps its parameter in
  `phase-param.<phase directory>` (`CopyLayout.paramFile(phase)`, `readParam(copyPath, phase)`). A move writes only the
  target phase's file, durably, before the rename; files of other phases are inert. The spec's single `phase-param`,
  "overwritten by the next move, so inert", is false for a move between two parameter phases (Rebuilt(e) to
  Outbound(t)): a failed rename or a crash would leave `rebuilt/` reading t. The single-file wording is replaced
  everywhere in this spec.
- **P5 extended.** Every device unload (shutdown, relinquish, Failed retirement, CRL failure) waits for the device's
  closes whose end moves a copy to `lost/`, so T10 is always made while the device is attached.
  `ClosePurpose.Reload` (a failed CRL load, a failed save-file removal) closes without moving the copy to Lost. A move
  to Lost that fails holds the copy offline in memory (`CopyMoves.pendingLost`) and is retried at each device check;
  `CopyMoves.move` refuses (NotMoved) a record that is Opening, Served or Closing.
- **P10 as amended.** The T6 ship-source set is Live, Rebuilt, Parked and Outbound(_). Outbound(tid) of the same
  transfer is a reship without a move; Outbound(other) goes T7 (back to Live) and then T6. No copy moves while an
  earlier send of its store is still registered (`startOut` refuses at debug; the receiver retransmits). A move out of
  Live drops the copy's load retry; a move into Live retargets it.
- **Earlier tenancy.** `startIn` acts on an earlier tenancy's copy (retires it, or takes a served Live one out of
  service as `retireUnlistedServed` does: FailStop close, then Lost, then retire) only after a fresh read of the
  device's own `StorageDeviceState` still shows the store TransferringIn with this transfer id, holding the
  TransferIn work's RetiringEarlierTenancy stage across the read. Against the reconcile's pre-commit snapshot the
  transfer's own copy, just moved Live by its pool update, would look like an earlier tenancy. Waits on an opening
  copy, an open begun during the read, or a failed read schedule a bounded device re-check
  (`scheduleDeviceRecheck`).
- **Initializing over Lost.** An Initializing entry whose store has a Lost copy on the device counts as created:
  `createNewStore` flips it out of Initializing and the Lost CRL pass rebuilds it (the copy may have served before its
  flip, and a fresh create could drop what it acknowledged).
- **Release hold.** `StoreCopies.release` keeps a `pending-delete/` copy while the device holds a Lost copy of the same
  store: an Active entry over a Lost copy is not "Active on its owner device" in substance (P2's extra copies would
  otherwise be released at the first check).
- **Copies are directories.** A child of a phase directory is a copy only if it is a directory (a failed stat counts as
  a copy). A non-directory, or a name that is not a copy name, is a stray: logged once, never touched.
- **`RebuildingStore.start`** runs on the control thread; its existing-copy lookup comes from the registry, not from a
  listing.
- **Deletions.** Per-attempt staging (P7) and the deletion of `StoreDirMarkers` and `CopyMarkers` were done in Task 5;
  `CopyPhase.read`, `CopyPhase.known` and `Phase.Unread` were deleted in Task 6.
- **Final review fixes.** A lost device CRL's move to `lost/` that lands unforced leaves the device unloaded and its
  CRL untouched, as a failed one does, and every `lost/` copy and phase directory is forced before the CRL is replaced
  (`CopyMoves.forceUnattached`). A rebuild's finish whose rename into `rebuilt/` lands unforced completes with its copy
  id (`StoreRebuild.placedUnforced`): the copy is recorded unsynced, and the flip waits for a device check to force it
  (its restarted rebuild takes the copy for done; there is never a second rebuild). No store opens on a device being
  relinquished, retired as Failed or unloaded for a CRL failure (`DeviceShutdown.isStoppingServing`), since those
  unload whatever is open once their closes have ended. A move to Lost refused because the copy is Opening, Served or
  Closing again is not held in `pendingLost`.

## Stage 1 audit findings

Audit of HEAD `cde3d375`, code reading only. Paths are relative to `src/main/scala/org/aspen_ddp/aspen/server/`
unless they start with `src/test` or `cmdline/`. "CT" means the call runs on the control thread. Stage-1 phases
are read from markers with the precedence RR > TO > TI > RC (Task 2's `CopyPhase`), so "Parked" is
`Outbound(None)`. Today a TO marker with empty content is written only by `keepRebuiltStoreOffline`: every
TransferringOut entry carries `Some(transferId)` (`AspenClient.scala:645-648`).

### COUNTEREXAMPLES TO I3 (read first)

Ratified by the controller after review: continuing past the I3 failure, and Step 5's `startIn` decision Outbound → `retireEarlierTenancy`. The fixes for C1–C6 and the double open are plan Tasks 1b/1c.

I3 does not hold in today's code. Each case below has two Works for one (device, store) in flight at once. The
invariant is still the right one. Each counterexample comes from a missing guard or a pass-ordering problem, and
the fix is to add the guard rather than to weaken I3. Stage 1 has to make the change listed in each row, or the
shadow registry will throw (Task 5 must refuse there, not throw).

| # | Works | How it happens | Guard missing | Spec/plan change |
|---|-------|----------------|---------------|------------------|
| C1 | Rebuild (queued or running, episode e1) × TransferIn (receive) on device D | D's entry is Rebuilding(e1) and a rebuild runs (`rebuildingStores`). The entry is re-placed to E (`replaceRebuild`), and the nudge to D is lost (nudges are best effort, and the periodic check is 1 h). E finishes and becomes Active, then is transferred back to D. D's first reconcile after that sees TransferringIn. `startTransfersIn` (`host/DeviceReconciler.scala:191`) runs **before** `startRebuilds`/`abandonUnlisted` (`:192`, `:433`), and `startIn`'s path check and fresh-receive branch (`host/StoreTransfers.scala:433`, `:436-542`) check neither `rebuildingStoreIds` nor `silenced`. So the receive starts while the e1 rebuild is still registered, and is cancelled only later in the same task (or never, past finish's latch, `host/StoreRebuilds.scala:94-100`). The queued variant: `silenced` (Rebuild queued) is cleared by `abandonUnlisted:460-462`, which also runs after `startTransfersIn` | `startIn` has a rebuild guard only on the retire-earlier-tenancy branch (`host/StoreTransfers.scala:560`) | Run `abandonUnlisted` before `startTransfersIn`. `startIn` refuses (waits) while the store has any Rebuild work (any stage) on the device, as `retireEarlierTenancy` already does |
| C2 | Rebuild (discard→running) for a stale episode × TransferIn, or a Rebuild with no Rebuilding entry | `discardLostStore`'s callback (`host/StoreRebuilds.scala:855-858`) calls `startRebuild(local, storeId, episode, acceptLoss)` with the episode captured when the discard began, and without re-reading `reconciledEntries`. A retire held up by a slow CRL (`readStoreState`/`dropStore`) can finish after the entry has moved on, and then starts a rebuild for an entry that is no longer Rebuilding(e1). If the entry is TransferringIn by then, `startIn` waits only while the retire runs (`isRetiring`, `:563`) and then starts the receive (C1) | The handoff does not check the entry | When the discard→running handoff runs, check `local.reconciledEntries` for Rebuilding with the same episode. Otherwise end the Work and re-check the device |
| C3 | Rebuild (awaiting flip outcome) × TransferOut (close, extract, ship) | A flip attempt commits Active and is reported `TransactionOutcomeUnknown`. Before the retry reads the entry, an operator's `transfer-store` makes it TransferringOut. The receiver's StartStoreTransfer then reaches `startOut` (`host/StoreTransfersOut.scala:119`), which closes or extracts the RC copy and writes TO (`:253`) while `rebuildingStores` still holds the store. The retry then gives `Unconfirmed` (`host/StoreRebuilds.scala:360-368`). This is the designed path ("kept for the transfer"), but the two Works overlap | `startOut` has no rebuild check | Either `startOut` refuses while Rebuild work exists (the receiver retransmits, so the transfer only starts later), or the Rebuild work ends at its first flip attempt and its outcome handler then runs as a stage-less "flip settle". The refusal is the simpler fix |
| C4 | Create (flip pending) × Rebuild, or × TransferOut | `createNewStore` keeps `creatingStores` until `fflip` completes (`host/StoreRebuilds.scala:189-193`). After the load and before that callback runs, a backend failure marks RR. Once the flip has committed, a reconcile can request a rebuild and start it (`startRebuild` does not check `creatingStores`). The same window allows a TransferringOut request (`startOut`). Only timing orders these: the callback has to lag a whole requestRebuild transaction | None in memory | End Create work when the copy is Served, or when its open fails, not at the flip. Track the flip in its own set: it touches no copy |
| C6 "ship×discard" | TransferOut (ship) × Rebuild (discard) | A ship is running. RR is then written on the closed copy (`backendFailed`, unserved backend, `host/StoreRebuilds.scala:765`). A retransmitted request reaches `returnLostTransferToRebuild` (`host/StoreTransfersOut.scala:185-191`), and `abortLostTransfer` makes the entry Active again. The Lost CRL pass then requests the rebuild, and `discardLostStore` → `retire` moves the directory into pending-delete while the archiver may still be reading it. Narrow, and probably benign, since the transfer was aborted | `flipLostCrlStores` (`host/DeviceReconciler.scala:269-273`) and `startRebuild` (`host/StoreRebuilds.scala:237-240`) do not check `isTransferringOut`, which the abandoned-out pass beside them does (`:305-310`) | The discard waits while a ship of the store is registered on the device |
| C5 | TransferOut(tid) twice (two closes of one store) | `startOut` adds to `closingForTransfer` (`host/StoreTransfersOut.scala:200`) without checking it first. Two requests (a retransmit, `retransmitPendingStarts`, or a readdress) that both finish their reads before the first close is handled both call `closeStoreForTransferOut`. The second `ShutdownStore` finds the store gone (`host/StoreLoader.scala:803`) and succeeds at once. So the second `closedForTransfer` writes TO and begins the ship while the first close is still extracting, and its `finally` (`:207`) clears `closingForTransfer` under the first. A second variant: if B's reads finish after A's `ShutdownStore` was handled (the router entry is gone), `closeForTransferOut` takes `extractUnloaded` (`host/StoreTransfersOut.scala:44-45`), which reads and saves the device CRL while A's extraction on the executor is still running | No single-flight check at `startOut` entry | `startOut` refuses while TransferOut work exists for the store on the device. This is not a two-kind pair, but the registry's `Work` would be set twice |

C1, C2 and C6 can all happen in production (C6 needs a backend failure reported after the store closed). C1 needs one lost nudge, and C2 needs a stalled CRL during a re-placement.
C3 needs an outcome-Unknown flip plus an operator transfer. C4 and C5 need slow callbacks or retransmits.

### Step 1: disk mutation sites

Phase columns use stage-1 phases. `—` is no copy at a store path: scratch, or gone. "Handler" is the CT handler
of a site that runs off the control thread.

**Copy creation and promotion into `<device>/<storeDir>`**

| Site | file:line | Before → after | CT? / handler |
|------|-----------|----------------|---------------|
| Store create: scratch in `creating/`, then `os.move` | `store/backend/StoreCreation.scala:31-47` (move `:45`), called from `store/backend/BackendOpener.scala:32-37` ← `host/StoreLoader.scala:395` ← `startOpen(create = true)` `:218` ← `host/StoreRebuilds.scala:161` (`createNewStore`) | — → Live (T1) | RocksDB: CT (inline in `startOpen`). FSIF: `FsifOpenEc` → `BackendOpened` → `StoreLoader.handleBackendOpened:258` |
| Bootstrap create | `cmdline/Main.scala:1907`, `:1920` (`DefaultBackendOpener.createStore`) | — → Live | No host running. Outside the registry |
| Rebuild finish: RC written in staging, then move | `rebuild/RebuildingStore.scala:801` (RC), `:807` (`DurableFiles.move`) | `rebuilding/<id>` → Rebuilt(ep) (T2) | Rebuild's own Future (client `ec`). Handler `host/StoreRebuilds.scala:312` (`completeFuture.onComplete`) → `markRebuiltStoreActive` → outcome cases `:319-398` |
| Rebuild start finds its own finished copy | `rebuild/RebuildingStore.scala:315-320` | Rebuilt(ep), unchanged (staging cleaned `:337`) | Same handler |
| Transfer-in promotion: TO removed and TI written in staging, then move | `host/StoreTransfers.scala:506` (TO `os.remove`), `:509` (TI), `:511` (move) | `transferring-in/<storeDir>` → Arrived(tid) (T8) | CT (`fcomplete.onComplete` `:459`). The bytes are unpacked off CT by `transfer/StoreTransferManager.scala:363-364` (network thread) into staging only |

**Marker writes and removals at the store path**

| Site | file:line | Before → after | CT? |
|------|-----------|----------------|-----|
| `StoreTransfersOut.closedForTransfer`: TO written | `host/StoreTransfersOut.scala:253` | Live / Rebuilt (Unconfirmed RC copy) → Outbound(tid) (T6). TI+? at the source gives TI+TO (Step 5) | CT (`fclosed.onComplete` `:205`) |
| `reinstateAbandonedOut`: TO removed (plain `os.remove`, no directory sync) | `host/StoreTransfersOut.scala:321` ← `host/DeviceReconciler.scala:312` | Outbound → Live (T7). TO+RC → Rebuilt. TI+TO → Arrived | CT |
| `bringIntoService`: TO removed for each Active entry with no `transferDevice` and not sending (no sync) | `host/DeviceDiscovery.scala:466-469` | Outbound → Live / Rebuilt / Arrived (T5/T7) | CT (adoption or restart callback) |
| `keepRebuiltStoreOffline` (AdoptOffline): TO "" written, then RC `DurableFiles.remove` | `host/StoreRebuilds.scala:545`, `:547` ← `:336` | Rebuilt → (TO+RC) Outbound(None) → Outbound(None) "Parked" (T4). A crash or failure between the two leaves TO+RC | CT |
| `clearRebuildCompleteMarker`: RC `os.remove` + `syncMarkerDirectory` | `host/StoreRebuilds.scala:560-571`. Callers: Adopt `:324`, Completed Rebuilds `host/DeviceReconciler.scala:351`, `recordTransferIn` `host/StoreTransfers.scala:302` | Rebuilt → Live (T3). TO+RC → Outbound (phase unchanged). A failure records `pendingMarkerRemovals` | CT |
| `clearTransferredInMarker`: `arrived-at` written, then TI `os.remove` (no sync) | `host/StoreTransfers.scala:243-252`. Callers: `recordTransferIn` `:294`, Completed Transfers In `host/DeviceReconciler.scala:332` | Arrived → Live (T9). TI+RC → Rebuilt, and then the RC removal `:302` → Live: **two transitions**, not one (see amendments) | CT |
| RR `writeRebuildRequiredMarker` (all through `markRebuildRequired` `host/StoreRebuilds.scala:667` → `ctx.writeRebuildRequiredMarker`, default `:877-878`; the `Host.Seams.writeRebuildRequiredMarker` seam, `Host.scala:161`, `:335`) | Callers below | X → Lost (T10/T11) | CT, every caller |
| ↳ an opened, unserved backend that failed | `host/StoreLoader.scala:277` | Live → Lost | CT |
| ↳ `BackendStateLost` at open | `host/StoreLoader.scala:292` | Live → Lost | CT |
| ↳ `retireUnserved` (closes the backend, not awaited, then marks) | `host/StoreRebuilds.scala:695`. Callers: `host/StoreLoader.scala:463` (failed before served), `:492` (incarnation lost), `crlSaveFileLost` `host/StoreRebuilds.scala:682` ← `host/StoreLoader.scala:483`, `:501` | Live (with save file) → Lost | CT |
| ↳ `failedOnClose` | `host/StoreRebuilds.scala:719` ← `host/StoreLoader.scala:846` | Live → Lost (keeps a transfer-out `crl_save.log`) | CT |
| ↳ `backendFailed`, served: marked **before** the close (`closeUnloaded` `:751`, async) | `host/StoreRebuilds.scala:743`. Callers: `handleBackendFailed:103`, `handleFailStopped:113`, `retireForeignCopy:619`, `retireUnlistedServed:662` | Live (Served→Closing) → Lost | CT |
| ↳ `backendFailed`, a backend no longer served | `host/StoreRebuilds.scala:765` | Any non-Lost copy at the path (Live closed, Outbound, ...) → Lost | CT |
| ↳ `retireForeignCopy` | `host/StoreRebuilds.scala:625` | Live (closed) / Outbound / Arrived / Rebuilt(other ep) at a Rebuilding store's path → Lost | CT |
| ↳ `StoreCopies.retirePresent` step 1 | `host/StoreCopies.scala:75-76` | Any → Lost (T11 step 1) | CT |
| ↳ lost device CRL: every store dir but those that `holdsOwnState` | `host/DeviceDiscovery.scala:641` (decided `:636`, `holdsOwnState:674-683`) | Live / Outbound / Arrived without a valid own save file → Lost | CT (device load) |
| ↳ adoption `CrlLost` (inside the transaction's prepare, before commit) | `host/DeviceMetadata.scala:597-601` ← `host/DeviceDiscovery.scala:392-394` | Outbound without a valid save file → Lost (RR copies already Lost) | CT (control context, `host/DeviceMetadata.scala:163-166`) |

**Moves out of the store path, and deletes**

| Site | file:line | Before → after | CT? |
|------|-----------|----------------|-----|
| `StoreCopies.moveToPendingDelete` | `host/StoreCopies.scala:108-122` (move `:116`), from retire's `transform` `:94-105` | Lost → Retired (`pending-delete/<storeDir>-<uuid>`) (T11 step 4) | CT |
| `StoreCopies.retire` callers | Deleted Stores `host/DeviceReconciler.scala:448`; discard `host/StoreRebuilds.scala:855`; earlier tenancy `host/StoreTransfers.scala:566` | — | CT |
| `StoreCopies.release` → `delete` | `host/StoreCopies.scala:151`, `:168` → `:209-223` (move into `deleting/` `:217`, then remove) | Retired → gone (T12) | CT |
| `LocalDevices.discardStoreDirectory` → `StoreCopies.delete` | `host/LocalDevices.scala:61-78`. Callers: `RebuildOutcome.Discard` `host/StoreRebuilds.scala:343`; `discardArrivedTransferIn` `host/StoreTransfers.scala:136-139` ← `:325` (SourceRestored/Aborted) | Rebuilt → gone (**not in the table**); Arrived → gone (T13) | CT |
| `startIn`: unusable or stale arrived copy | `host/StoreTransfers.scala:400` | Arrived (wrong tid, invalid save) or Lost (RR+TI) → gone | CT |

**Scratch only (no copy phase; listed for Task 4 completeness)**

| Area | Sites | CT? |
|------|-------|-----|
| `creating/` | `store/backend/StoreCreation.scala:50` (failure cleanup); `host/DeviceDiscovery.scala:713` (load sweep) | Create: as the create above. Sweep: CT |
| `transferring-in/` | `host/DeviceDiscovery.scala:708`; `host/StoreTransfers.scala:229`, `:466`, `:495`; `transfer/StoreTransferManager.scala:363-364`, `:437` | Manager: network thread, result → `startIn` `:459`. Others: CT |
| `rebuilding/<id>` | `host/StoreRebuilds.scala:477` (`abandonUnlisted`), `:641-643` (`dropStagingCheckpoint`); `rebuild/RebuildingStore.scala:337` (`cleanupStaging`), `:421` (`createStaging` + `backends.create`) | `RebuildingStore`: rebuild Future, result → `host/StoreRebuilds.scala:312`. Others: CT |
| `deleting/` | `host/StoreCopies.scala:226-234`, `host/DeviceDiscovery.scala:717` | CT |

Non-phase files written into copies (unchanged by stage 1): `arrived-at` (`host/StoreLoader.scala:866`, via
`host/StoreTransfers.scala:245` and `host/DeviceDiscovery.scala:458-459`), `crl_save.log` (`host/StoreLoader.scala:826-827`
on the store executor, `host/StoreTransfersOut.scala:113`, removed at `host/StoreRouter.scala:282` on the executor),
`lost-crl-*.log` (`host/StoreCopies.scala:85-91`), the `finish` gates (`rebuild/RebuildingStore.scala:792-799`), and the `inherited-votes` unlink from a live copy (`RebuildingStore.removeInheritedVotes`, `rebuild/RebuildingStore.scala:153-155`, called from `store/Frontend.scala:467` on the store executor).

Task 4 addendum: the drain check (run with ControlFixture verification forced on, exploratory) found no missing
production site; its 13 mismatches were all copies a test put at a store path after the device's load, or fake rebuilds
that complete without placing a copy. A copy put at a store path by hand after its device's load is not known to the
registry.

**Amendments implied by Step 1**

- T13 must also cover Rebuilt → gone, for `RebuildOutcome.Discard` (`host/StoreRebuilds.scala:340-346`).
- T9 for a shipped Rebuilt copy (TI+RC) is two stage-1 transitions, Arrived → Rebuilt → Live, at
  `host/StoreTransfers.scala:294`/`:302` and at Completed Transfers In followed by Completed Rebuilds in the same
  reconcile (`host/DeviceReconciler.scala:180-181`). Task 3 needs Arrived → Rebuilt (stage 1 only).
- T10's "copy closed" precondition is broken on purpose. `backendFailed` (`:742-751`) marks RR while the store is
  still closing, and `StoreLoader.scala:277` marks before `backend.close()` (`:279`). Stage 1 must allow phase Lost with
  activity `Closing(FailStop)`. Stage 2's move must wait for the close, so the backend directory is never moved
  while it is open.
- T10 also has to cover `retireForeignCopy` taking Arrived and Outbound copies to Lost at a Rebuilding store's
  path (`host/StoreRebuilds.scala:589-630`).
- Removing TI or TO does not sync the directory (`host/StoreTransfers.scala:246`, `host/StoreTransfersOut.scala:321`,
  `host/DeviceDiscovery.scala:469`), and removing RC does (`:563`). Stage 1 keeps this as it is. `Unsynced`
  belongs to stage 2.

### Step 2: I3 pair table

Work tracking today, by store id **host-wide** unless a device is noted:

| Work | Collections |
|------|-------------|
| Create | `creatingStores` (`host/StoreRebuilds.scala:34`), from `createNewStore:139` until `fflip` completes `:193` |
| Rebuild | queued `silenced` (`:64`, by store → device), discard `droppingCrlState` (`:54`), running `rebuildingStores`/`rebuildDevices`/`rebuildEpisodes` (`:36-40`, through the flip outcome), cancelled `abandonedRebuilds` (`:42`) |
| TransferIn | earlier-tenancy retire (`StoreCopies.retiring` only, **not** `transferringIn`), receive `transferringIn` + `pendingStartTransfers` (`host/StoreTransfers.scala:58-60`), retry `transferInRetries` (`:63`), record `recordingTransfersIn` (`:55`), abandon `abandoningTransferIns` (`:59`) |
| TransferOut | close/extract `closingForTransfer` (`host/StoreTransfersOut.scala:32`), ship `storeTransferService` active (`transfer/StoreTransferManager.scala:183-185`), abort-lost `abortingLostTransfers` (`:26`) |
| Retire (activity) | `StoreCopies.retiring` (`host/StoreCopies.scala:29`, by store id) |

The main guard is the tree. One entry per (device, store) has one status, and each Work starts only from its own
status (Initializing / Rebuilding / TransferringIn / TransferringOut). Overlaps come from Work that outlives the
status it started from, or from a pass that runs before the pass that ends the other Work.

| Pair | Overlap? | Guard, or counterexample |
|------|----------|--------------------------|
| create × rebuild (any stage) | **Yes (C4)**, timing only | Initializing vs Rebuilding. A rebuild needs Active first (`requestRebuild:803-808`), and Active comes from the create's own flip |
| create × transfer in | No | TransferringIn needs no entry on the device. The Initializing entry is the device's |
| create × transfer out | **Yes (C4)**, timing only | TransferringOut needs Active (`AspenClient.scala:641`) |
| create × retire | Not found | Deleted Stores needs the entry gone. Nothing removes an Initializing entry |
| rebuild queued × discard × running | No (stages of one Work) | `startRebuild:237-240` (`!rebuildingStores`, slot count includes `droppingCrlState`); `discardLostStore:853`; the handoff `:855-858` |
| rebuild × transfer in (retire earlier tenancy) | No | `retireEarlierTenancy:560` (`rebuildingStoreIds`) and `:563` (`isRetiring` covers a discard, which is a retire) |
| rebuild × transfer in (receive, record) | **Yes (C1, C2)** | No rebuild guard on `startIn:433-542` (receive from `:436`). Reconcile order `DeviceReconciler.scala:191-192` |
| rebuild × transfer in (abandon) | No | `abandonIn` needs a failed source and TransferringIn. It writes the tree only |
| rebuild × transfer out (close, extract, ship) | **Yes (C3)** | No rebuild guard in `startOut` |
| rebuild × retire | **Overlap with Retiring** (not Work×Work) | A rebuild past `finish` (copy at the path, flip pending) whose entry was re-placed away. The Deleted Stores pass (`DeviceReconciler.scala:217-223`, `retireUnowned:439-451`) retires the Rebuilt copy, because the store is in `offlineStores` from its lost copy, and nothing removes it after the discard. The flip then gives `Discard`, which deletes the path (`StoreRebuilds.scala:343`) while the retire may still be running. `retireUnowned` checks `isRetiring`, not `rebuildingStoreIds`. Fix: the Deleted Stores pass skips a store with Rebuild work, and the Discard handler skips a copy that is Lost or Retiring |
| transfer in: retire earlier tenancy × receive × record × abandon | No (stages) | Retire → re-check → `startIn` (`:571`). Receive and record single-flight on `transferringIn` (`:356`, `:420`, `finished:271-276`). Abandon cancels a receive by design (`dropAbandonedTransferIn:212-234`) |
| transfer in × transfer out (one device) | Tail only | Exclusive statuses. A ship's registration can outlive the commit that removed the source entry (`StoreTransferManager` `finally` `:250-253`). If the store transfers back to D before the ship ends, `startIn` refuses the old TO copy at the path (see Step 5 site 5). Benign |
| transfer out × retire | Tail only | Deleted Stores retires the source copy after the commit (`DeviceReconciler.scala:448`) while the ship is still registered. Benign: all bytes have been sent. Theoretical: a request whose tree reads came before the completing commit still closes or extracts (`StoreTransfersOut.scala:73-114`) while the retire reads and drops the same CRL entries. That needs a request still in flight after completion |
| transfer out close × extract × ship × abort-lost | **Yes (C5)**, one Work twice | Not single-flight at `startOut:200` |
| transfer out (ship) × rebuild (discard) | **Yes (C6)**, tail | No `isTransferringOut` check in `flipLostCrlStores` or `startRebuild` |
| retire × retire | No | `StoreCopies.retire:55` (`retiring`, host-wide) |

**Open paths that can race an extraction** (for the controller's shadow-mode ruling). An open of the same store
that starts during `closingForTransfer` fails the extraction today. `opensStarted` is host-wide, by store
(`StoreTransfersOut.scala:83-87`, `:236-237`). The paths:

| Open path | file:line | Same device? |
|-----------|-----------|--------------|
| `reinstateAbandonedOut` → `tryLoadStore` (a retransmitted request on an Outbound copy whose transfer was aborted meanwhile) | `host/StoreTransfersOut.scala:332` ← `host/DeviceReconciler.scala:312` | yes |
| `reloadClosedStores` (a cancelled shutdown, entry Active) | `host/DeviceShutdown.scala:360` | yes |
| Completed Transfers In / Completed Rebuilds | `host/DeviceReconciler.scala:333`, `:352` | other device of the host (same store id) |
| `handleLoadById` (from `recordTransferIn` `host/StoreTransfers.scala:304`, Adopt `host/StoreRebuilds.scala:325`) | `host/StoreLoader.scala:783-785` | other device |
| Device load | `host/DeviceDiscovery.scala:749` | other device |
| `retryFailedLoads` | `host/StoreLoader.scala:379` | guarded (`isClosingForTransfer`) |
| `createNewStore` → `startOpen(create)` | `host/StoreRebuilds.scala:161` | no (needs Initializing) |

A per-device `Work.TransferOut` refusal covers only the first two. Today's `opensStarted` also fails the
extraction for an open on **another** device of the host. That open uses its own device's CRL, so it cannot
corrupt the extraction, but it installs that copy under the store id, which `closedForTransfer`'s
`router.loaded.contains` (`:236`) then treats as "opened". Task 5/7 should keep this host-wide check until
stage 2, or state that it has been dropped on purpose.

### Step 3: silence readers

| Mark | Production readers | What the reader needs |
|------|--------------------|-----------------------|
| `offline`, `loading`, `awaitingDevice` | `StoreRouter.answersUnknown` (`host/StoreRouter.scala:86-87`; used by `deliverTx:148` and `deliverClient` `Read:156`) | "silent" for any reason, combined with `ready`. No reason needed |
| `offline` | `StoreRouter.isOffline` (`:309`) → only `DeviceShutdown.storesToClose` (`host/DeviceShutdown.scala:331-333`), which feeds `closeStoresForShutdown` (call at `:216`), `closeStoresToRelinquish` (`:319`), `closeThenUnload` (`:372`) | **Not silence.** It needs "a copy on this device that is Opening or Served": `loadedStores` minus those already closed. A by-id offline mark set for another reason skips a store this device serves. By-id writers that bypass `markCopyOffline`'s served/opening check, and so can mark a served store: `host/DeviceReconciler.scala:597` (unloaded-device branch), `host/DeviceDiscovery.scala:260` (Shutdown device), `host/StoreRebuilds.scala:231` (`silenced`), `:540` (`keepRebuiltStoreOffline`), `:689` (`retireUnserved`), `:714` (`failedOnClose`), `host/StoreLoader.scala:265` (`leaveOffline`, including the double-open case in Step 4), and `markStoreOffline` (`host/DeviceControl.scala:118-120`) with all its callers (`host/StoreLoader.scala:179`, `host/DeviceReconciler.scala:258`, `host/DeviceShutdown.scala:225`, `:325`, `host/StoreTransfers.scala:306`, `:310`). Tasks 6/8 use this full list. Derive it from activity on the device's copies |
| `offline` | `router.offlineStores` (`:336`) → `DeviceControl.publishView` (`host/DeviceControl.scala:136`) → `HostStoreView.of` (`host/HostStoreView.scala:29-35`) | `repairable = served -- offline -- rebuilding -- transferringOut`. Read by `Host.repairableStoreIds` (`Host.scala:444`) → `repair/RepairService.scala:131`, and `Host.isRepairable` (`Host.scala:450`) → `repair/StoreRepairer.scala:291`. Since served dominates silence, `served -- silent = served`, so derived silence drops the `-- offline` term. That term today removes only stores the buggy unguarded marks above have marked. No reason needed. `HostStoreView.offline` itself has no production reader (tests: `src/test/.../host/ControlContextSuite.scala:79`, `StoreRouterSuite.scala:471`) |
| `loading` | `isLoading` (`:311`): tests only (`HostFsifLoadSuite` 509, 514, 530, 642; `StoreRouterSuite` 485) | Activity Opening of some copy |
| `awaitingDevice` | `isAwaitingDevice` (`:313`): tests only (`HostFsifLoadSuite` 620, 630, 641) | Pending adoption or shutdown-marker decision for the path (`DeviceDiscovery.pendingResolutions`) |

Test readers: `router.isOffline` ×11 and `router.offlineStores` ×52 across `CommittedUnknownRetrySuite`,
`DeviceShutdownSuite`, `DeviceCrlSuite`, `HostDeviceDiscoverySuite`, `HostFsifLoadSuite`, `StoreRouterSuite` and
`StoreRebuildTriggerSuite`. `QuicNet.isStoreOffline` (`common/network/implementations/quic/QuicNet.scala:615`) is the
client's `StoreAvailabilityTracker`, unrelated. The device sets `sds.offlineStores` and `loadedStores` are read by
the reconcile passes to select stores (`host/DeviceReconciler.scala:217`, `:269`, `:305`, `:324`, `:342`;
`host/DeviceShutdown.scala:345`; `host/StoreLoader.scala:363`; `host/StoreRouter.scala:398`). They are not silence
readers. Tasks 7/8 handle them.

**Conclusion.** No production reader needs the reason. The derivation can return `silent: Boolean`, plus
predicates kept for the test accessors. `storesToClose` has to move off the silence mark (Task 6).

Task 5 addenda (derived silence compared with the router's marks after each test drain; each is whitelisted by test
name in `HostCopies.KnownDefects` until Task 6 removes the marks):

- D1: a transfer in's store has no router mark on its destination while it is received, promoted (Arrived) and its
  pool update runs, so once the pool names the destination the router answers UnknownStore until the load. The
  registry is right (silent: TransferIn work, the Arrived copy).
- D2: the device check of an unloaded device (`DeviceReconciler`, the `None` branch) marks every store its entry lists
  offline by id, one served from another device of this host included, which drops it from `repairable`. The registry
  is right (Served dominates).
- D3: the view counts a store repairable while a transfer out is closing it, when an open during the close (R1) serves
  it. The registry is right (TransferOut work); Task 7's refusal of that open removes the case.
- Not a defect of the marks: `startRebuild` queued (silenced) a store while a transfer out was closing it, a second
  Work for the (device, store). Fixed in Task 5: the store is not silenced until the close ends and re-checks the
  device.
- D4 (fix round 1, every ControlFixture suite compared): a store being created has no router mark until its open
  starts (`createNewStore` reads its pool first), so the router answers UnknownStore meanwhile. The registry is right
  (silent: Work.Create).
- D5: the router's offline mark of a retired copy trails its move into pending-delete by one control task
  (`StoreCopies.retire`, then its caller's `clearOfflineUnlessHeld`). The registry is right (no copy, Unknown).
- D6 (fix round 2): a store directory whose config is gone is not silenced at its device's load (`tryLoadStore`
  only logs it); only the reconcile's Missing Stores pass silences it. The registry is right (silent: its copy).
- D1, D4, D5 and D6 arise in windows whose timing varies from run to run, so `compareWithRouter` recognises them by
  their state (`HostCopies.knownDefect`) rather than by test name; D2 and D3 stay whitelisted by test name.
  Each state is bounded to its window: D1 to copies on the receiving device (or Arrived), D4 to Create work before
  its copy exists, D5 to before the retiring caller's clear (a device still lists the store offline), D6 to stores
  every copy of which lacks its config.
- Derivation extension: Work.Create, and an entry a loaded device's latest reconcile read Initializing, are
  `Opening` reasons, so a failed create (nothing at its path, its Create work ended, its entry still Initializing
  for the next check to create again) stays silent as the router's offline mark keeps it.

### Step 4: reload paths (`StoreRouter.install` with `replaced != null`)

`install` is reached only from `StoreLoader.handleLoad:611` and from tests (`ControlFixture.scala:533`,
`installServedElsewhere`, a device that is not loaded). `handleLoad` replaces when `router.entry(storeId)` is
defined (`:514`).

| Path | file:line | Goes through `startOpen`? | Device in the registry? |
|------|-----------|---------------------------|-------------------------|
| `TestNetwork.restartStore` (`ReplayIntegrationSuite:474`, `UnackedCommitsIntegrationSuite:182`, `:383`, `:406`, `StoreRepairerSuite:1743`) | `src/test/.../TestNetwork.scala:637` → `Host.loadStore` (`Host.scala:465-469`) | no, `LoadStore` directly | no: TestNetwork's device is never loaded (`unloadedDeviceCrl`, `TestNetwork.scala:619-626`) |
| `TestNetwork.moveStore` | `TestNetwork.scala:730` | no | no |
| `StoreRouterSuite` reload tests (`:531`, `:592`, `:727`, `:893`) | `fx.load` `StoreRouterSuite.scala:170-173` | no | injected `LocalStorageDeviceState` (`:187-193`, and inline) |
| `StoreRebuildTriggerSuite:578`, `:617`; `HostCrlSaveLoadSuite:98`, `:120`, `:142`, `:165`, `:201`, `:272`, `:279`, `:287`, `:325`, `:341`, `:388`; `HostDeviceDiscoverySuite:2108`; `host/ControlFixture.scala:302` | `Host.loadStore` | no | **yes**: loaded, tracked devices. Loads only (no replace) |
| `LearnOnlyDriverIntegrationSuite:78`, `FsifIndexAllocationStrategySuite:44`, `StorageDeviceSetIntegrationSuite:210` | `Host.loadStore` on TestNetwork's host | no | no. Loads only |
| Direct `router.install`: `host/ControlFixture.scala:533` (`installServedElsewhere`), `host/ControlContextSuite.scala:95` | `StoreRouter.install(store, executor)` `host/StoreRouter.scala:339` | no | no |
| Production | none intended. `handleBackendOpened` closes an opened backend when the store is served (`StoreLoader.scala:270-280`), and `tryLoadStore` skips one that is served or opening (`:133`) | — | — |

Window found: between `serveOpened`'s post of `LoadStore` (`:210`) and `handleLoad`, `opening` is empty and the
router has no entry. A second `tryLoadStore` (from a `LoadStoreById` queued meanwhile, `:785`) passes `:133` and
starts a second open. With RocksDB or FSIF the second open fails on the backend's own lock, and then
`leaveOffline` (`:264-268`) marks the router offline, clears `loading`, adds the store to `sds.offlineStores` and
drops it from `sds.loadedStores` (a separate bug). The first LoadStore's install then clears the router marks again
(`host/StoreRouter.scala:371-372`), and `retryFailedLoads` drops the retry because the store is served (`:362`).
The lasting residue is a **served store that is missing from `loadedStores` and present in `offlineStores`**, so
`DeviceShutdown.storesToClose` (`:331-333`) never closes it. If the second backend opens and reports `hasFailed`,
`:276-277` writes RR on the served copy's path. Task 4/5 must record this as a defect to fix (plan Task 1c), not
only as an `Opening → Opening` refusal. With MapBackend it
would reload. Every pass that calls `tryLoadStore` selects stores from `offlineStores`, which `serveOpened`
clears, so in practice only a duplicate `LoadStoreById` reaches the window.

**Decision for Task 3.** `Served → Opening` is **illegal**: no path opens a Served copy. A reload is
`LoadStore` without `startOpen`, so the registry must also accept `Idle → Served` (the test seam
`Host.loadStore`) and `Served → Served` (a replace), or skip loads onto devices it does not track. Every test
reload targets a device with no `DeviceCopies` (TestNetwork) or an injected one (StoreRouterSuite,
`ControlFixture.injectLoadedDevice`). Task 4 needs a rule for injected devices. `Idle → Served` without `startOpen` must also be legal on **tracked** devices (StoreRebuildTriggerSuite, HostCrlSaveLoadSuite, HostDeviceDiscoverySuite, ControlFixture). `Opening → Opening` (the window
above) is refused, not thrown, per the R1 ruling.

### Step 5: marker combinations

Writers and removers are as in Step 1. RR is never removed (only moved away with its directory).

| Pair | Reachable? | How (citation) |
|------|------------|----------------|
| RR+TO | yes | Retire of a shipped source copy: TO, then RR at `host/StoreCopies.scala:76`, with a crash before the move `:116`. Lost CRL on a TO copy whose save file is invalid (`host/DeviceDiscovery.scala:641`). Adoption `CrlLost` (`host/DeviceMetadata.scala:601`). `backendFailed` on a closed backend (`host/StoreRebuilds.scala:765`) |
| RR+TI | yes | Lost CRL on an arrived copy whose save file does not validate (`:641`; named at `host/StoreTransfers.scala:364-365`). `retireForeignCopy` on an Arrived copy (`host/StoreRebuilds.scala:625`) |
| RR+RC | yes | `retireForeignCopy` on an earlier episode's copy (`:625`). A retire of an unpromoted, Superseded or Unconfirmed copy (`host/StoreCopies.scala:76`). `retireEarlierTenancy` (`host/StoreTransfers.scala:427`) |
| TO+TI | yes, narrow | At a source: the arrived copy's TI removal failed (`host/StoreTransfers.scala:246`, entry Active, store offline `:310`), then `transfer-store` → `startOut` writes TO (`host/StoreTransfersOut.scala:253`) before Completed Transfers In retries. In staging (TO removed `:506`, then TI `:509`), the directory sync of `:509` persists the unlink too, so it never reaches a store path |
| TO+RC | yes | `keepRebuiltStoreOffline` between `:545` and `:547`, or a failed `:547`. `startOut` on an Unconfirmed rebuilt copy, which today keeps RC (`host/StoreTransfersOut.scala:253`) |
| TI+RC | yes | A shipped Rebuilt copy arrives with RC (`host/StoreTransfers.scala:296-302`) |
| triples | TI+TO+RC: only if a TI+RC copy's TI removal fails and it is then shipped. RR+(any) as RR+X | — |

**Precedence RR > TO > TI > RC at the five sites, for every reachable combination**

| Site | Today | With `CopyPhase` | Changes |
|------|-------|------------------|---------|
| 1 `tryLoadStore` (`host/StoreLoader.scala:108-132`) | RR, TO, TI, RC. Any → `keepMarkedCopyOffline`. A failed stat → `awaitUnreadRetry` | `read`. Non-Live → keep offline. A throw → `awaitUnreadRetry` | None (only which log line is chosen) |
| 2 `adoptionClass` (`host/DeviceDiscovery.scala:325-338`) | RR → CrlLost. TI → Clean. TO → Clean iff `loadValidStoreState`. RC ignored → Clean. A stat throws | Lost → CrlLost. Outbound → Clean iff valid save file, else CrlLost. Arrived, Rebuilt, Live → Clean. Throws | **TI+TO**: today Clean whatever its save file; now Outbound, so it needs a valid save file, or it is CrlLost and adopted Rebuilding. In the reachable case (above) the source kept a valid file (extraction ships a valid one unchanged, `host/StoreTransfersOut.scala:98-105`), so this matters only for later damage. TO+RC: same as today (Outbound; the rebuilt copy's empty save file is valid) |
| 3 `holdsOwnState` (`host/DeviceDiscovery.scala:674-683`) | RC present → true; else a valid own save file | Rebuilt → true. Lost → irrelevant (RR is already written, and marking again is a no-op). Others → valid own save file | TO+RC: today true through RC, now through the save file. Same for reachable states: an RC copy always has `finish`'s empty save file and is never loaded, so nothing removes it. TI+RC (Arrived): the save file was validated at promotion, so same unless damaged |
| 4 `reloadClosedStores` (`host/DeviceShutdown.scala:352-360`) | Offered to `tryLoadStore` unless RR, TO or TI is known present. RC ignored, then refused by `tryLoadStore`. A failed stat counts as absent → offered | Live → offer. Others → skip. `known` = None → **offer anyway** (today's behaviour: `tryLoadStore` stats again and waits) | RC-only: today offered and then kept offline, now skipped. Unreachable: a copy closed for shutdown was served, and RC appears only through a rebuild's move into an empty path. **Task 2 must offer on None**: the store has already left `closedForShutdown` (`:346-351`), so skipping it would leave it unserved until a restart |
| 5 `startIn` (`host/StoreTransfers.scala:372-435`) | TI present → stale check (wrong tid, RR, invalid save → **delete** `:400`; unreadable → wait), else resume. Else RC or RR known → `retireEarlierTenancy`. Else path exists or stat fails → refuse ("store path already exists"). Else receive | Arrived(tid) → stale check as today. Lost → `retireEarlierTenancy`. Rebuilt → `retireEarlierTenancy`. Outbound → see below. Live → refuse. `known` = None → wait | **RR+TI**: today deleted (and the transfer restarted); now Lost wins → retired to `pending-delete/` (and restarted). Both are safe: the copy was never the owner's. **TO+RC** (a same-device copy left by an earlier tenancy): today `retireEarlierTenancy`; now Outbound. **TO-only / TI+TO**: today refused forever (TO-only), or deleted as stale (TI+TO). **Decision: Outbound → `retireEarlierTenancy`**. This keeps TO+RC as it is today and fixes the TO-only case, where a store transferred back onto its old source before that device's Deleted Stores pass ran is stuck behind "store path already exists". A TO copy at a TransferringIn path is never the current source (`require(fromDeviceId != toDeviceid)` `:351`) and never the owner's. TI+TO: deleted today, retired now |

Other marker readers outside the five sites. Stage 1 keeps them as single-marker tests, or Task 2 moves them to
`CopyPhase` with the same decisions:

- `DeviceReconciler.hasMarker` passes: Lost CRL RR (`:269-270`), abandoned out TO (`:305-309`), completed in TI
  (`:324-327`), completed rebuilds RC (`:342-346`). Their order (`:174-181`) takes TI+RC, TO+RC and TI+TO
  to Live within one reconcile, one marker per pass.
- `retireForeignCopy` (`host/StoreRebuilds.scala:595-600`): RC of this episode or RR.
- `discardLostStore:846`: RR.
- `StoreCopies.retirePresent:75`: RR.
- `startOut` (`host/StoreTransfersOut.scala:187`, `:225`): RR.
- `recordTransferIn:301`: RC.
- `RebuildingStore.isRebuildCompleteFor` (`rebuild/RebuildingStore.scala:78-80`, and `start:315`): RC.

**Behaviour changes, in full:**

1. An adopted TI+TO copy is Outbound and needs a valid save file (CrlLost otherwise).
2. At `startIn`, an RR+TI copy is retired, not deleted.
3. At `startIn`, a TO copy at the path (TO-only or TI+TO) is retired as an earlier tenancy, where today it is
   refused forever (TO-only) or deleted (TI+TO). TO+RC is unchanged (retired).
4. `reloadClosedStores` skips an RC-only copy (unreachable), and must offer the copy when its phase cannot be
   read.

## Stage 3 decisions

Decisions the stage 3 plan made where this spec left a choice open, with the rulings made during execution. Each is
stated as what holds now. Where this block and the text above disagree, this block governs.

### Decisions

- **D1. Inputs are values, not the registry.** `CopyDecision.decide` takes one `StoreInputs` (entry, the store's copy,
  work, `StoreFacts`). Its `CopyState` carries an `ActivityKind` (the `Activity` cases without payloads), so tests need
  no `LoadRetry` or device state. `HostFacts` is `StoreFacts`.
- **D2. Store-major order replaces pass-major order.** Each store's actions run before the next store's, and each
  store's inputs are read fresh at its turn. Within a store the cases keep the old pass order. Rebuild slots are taken
  in `(poolId.uuid, poolIndex)` order.
- **D3. `decide` enforces I3.** It emits `Create`, `StartRebuild` or `StartTransferIn` only when the store has no work
  left after this reconcile's own `EndQueuedRebuild`, or has work of the same kind. The other combinations back off
  silently there instead of being refused and logged by the handler's claim.
- **D4. `decide` ends a queued rebuild first.** Work `Rebuild(_, Queued)` under an entry that is not Rebuilding gives
  `EndQueuedRebuild`, and the rest of `decide` sees no work.
- **D5. Missing Stores is one case.** `DeclareMissing(first)`; the executor sets `local.declaredMissing` to exactly the
  stores given it in this reconcile.
- **D6. Outside `decide`:** the device-status branches (Failed, other host, shutting down), `CopyMoves.resync` and
  `retryPendingLost`, `dropLeftoverCrlState`, the `rebuilding/` staging sweep, and `StoreCopies.release`'s listing of
  `pending-delete/` (its per-copy rule is the pure `CopyDecision.release`).
- **D7. One Live-copy rule, `Load`, replaces `closedForShutdown`, `heldForSync` and `reloadClosedStores`.** An Idle,
  synced Live copy with no work, under an Active entry, not being sent, whose store is not served, opening or held by a
  rebuild anywhere on the host, whose config is not known absent (a stat that fails is offered: R9) and which is not
  declared missing, is loaded. A copy held offline on purpose (pendingLost, work, AwaitingRetry, an unserved backend
  closing, held `Closing(Unserved)` by `retireUnserved`, a device that stops serving) is never in that state. This
  settles the Todo's question: `closedForShutdown` became a case of the Live-copy rule.

The exhaustive test (`CopyDecisionSuite`) runs every entry status, copy phase, activity and work and asserts I1-I5, the
store-copies release rule and "no Action loads a non-Live copy".

### Rulings made during execution

- **R1. `declaredMissing` is snapshotted before the loop.** `storeInputs` reads the snapshot; after the loop the executor
  assigns the new set, then calls `missingIs` once.
- **R2. `StoreRebuilds.hasRunningRebuild(storeId)`** exists for `StoreFacts.rebuildHandle`.
- **R4. Every transfer-out close's end re-checks the source device,** whatever its outcome (StoreTransfersOut), since a
  reconcile is a re-read, not a delta. `rebuildWaitsForClose` (a predicate) replaces `deferRebuildUntilClosed` and
  `rebuildsAwaitingClose` is gone. Parked (pre-existing, unchanged): a rebuild of a store on another device of the host
  that waits for a transfer-out close is prompted only by the periodic device check.
- **R6 (superseded by R9). A Live copy whose config stat fails waits** until the stat answers (it is not offered); a
  Live copy with no config is a fixture-only state, since creates and moves are atomic. The configured-copy fixture
  (`setUp(configured = ...)`) stays; the skip does not.
- **R7. Test fixtures hold the work production holds.** A real load claims Opening at once, and production closes for
  transfer out only under TransferOut work; fixtures that closed a copy for transfer out hold that work, and release
  it at the end.
- **R8. Reload copies are reloaded by their load retry.** A copy closed with `ClosePurpose.Reload` (a failed CRL-state
  load, `handleUnloadFailed`; a failed save-file removal, `handleSaveFileRemovalFailed`) is not left Idle for `Load`:
  each records a load retry, so the copy ends AwaitingRetry and `retryFailedLoads` reloads it under the retry's
  backoff (a failed CRL load's doubles from the retry its open took), never at each device check.
- **R9 (final review; supersedes R6). A config stat that fails does not stop `Load`.** Its conjunct is
  `configPresent != Some(false)`: a copy whose stat fails is offered, and `tryLoadStore`, reading it again, holds it
  back with its load retry (`awaitUnreadRetry`), retried on the device scans with backoff, as before stage 3. R6's skip
  left it unserved until the next device check (hourly in production). `Some(false)` is `DeclareMissing`.
- **R10 (final review). An unserved backend's close holds its copy.** `StoreRebuilds.retireUnserved` (a CRL save file
  lost, a lost incarnation, never-fits, a backend failed before served) takes the copy's record to `Closing(Unserved)`
  (a new `ClosePurpose`) before `backend.close()` (`HostCopies.closingUnserved`; `Transitions.activityLegal` allows
  Idle, AwaitingRetry and Opening to `Closing(Unserved)` under Live, and nothing else to it but Served's
  `Closing(_)`, so a router removal of an unserved copy is still a violation) and ends it (`closeEnded`) just before `moveClosedToLost`. Left Idle, the
  copy was in `Load`'s state while the close ran, and a device check in that window opened a second backend on its
  directory, which for a failed backend could serve, leaving the move to give up on a fail-stopped copy. The close is
  counted by `closeStarted` as before, so every unload still waits for it. `StoreLoader.handleBackendOpened` holds
  the same way a failed backend it closes unserved because another backend serves the store (from Opening, on a
  registered device), ending the hold before its move to lost/.
- R3 and R5 were review carry-overs with no design effect.
