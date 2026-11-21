# Store copy state machine

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
- Legal transitions are listed in one table (below) and enforced by one function. An illegal transition is a
  programming error (thrown), not a state to recover from.
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
- A phase that carries a parameter holds it in one file, `phase-param`, written durably into the copy before the
  move into that phase. Outbound and Arrived carry the transfer id, and Rebuilt carries the episode. Only the phase
  the location names reads it, and the next move that needs a parameter overwrites it. A stale `phase-param` is
  therefore inert.
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
| T6 | Live / Rebuilt | Outbound(tid) | `StoreTransfersOut.closedForTransfer`, after the extraction into `crl_save.log` | copy closed, its work is `TransferOut(tid)` |
| T7 | Outbound | Live | transfer aborted or abandoned: entry Active, no `transferDevice` (`reinstateAbandonedOut`, adoption) | no Live copy |
| T8 | `transferring-in/<id>` | Arrived(tid) | promotion after validating the save file | no Arrived copy for another transfer (else delete it first) |
| T9 | Arrived | Live | pool update committed (`recordTransferIn`, `completeTransfersIn`); `arrived-at` written first | no Live copy |
| T10 | Live, Outbound, Arrived, Rebuilt, Parked | Lost | backend failure or fail-stop, failed close, missing incarnation, unloadable save file, lost device CRL (except copies that hold their own state), or a copy that stopped being the owner's | copy closed |
| T11 | Lost | Retired | retire: save CRL state into `lost-crl-state.log`, `dropStore`, move | the store has no running retire on this device |
| T12 | Retired | gone | `StoreCopies.release`, rename-first via `deleting/` | store Active on its owner device |
| T13 | Arrived | gone | unusable or stale arrival, rename-first (source still owns the state) | none |

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

1. write `phase-param` durably if `param` is set;
2. `DurableFiles.move(from, to)`, which forces both directories.

If step 2's rename succeeds but a directory sync fails, the copy's record takes the new phase with
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
  case Closing(purpose: ClosePurpose)                   // TransferOut(tid) | Shutdown | Relinquish | FailStop | Reload
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
- Every phase or activity change goes through `DeviceCopies.transition(copyId, ...)`. That function checks the
  table and the per-phase invariants, and it publishes the view.
- Opens are started only by `transition(_, Activity.Opening)` from an Idle Live copy whose store has no
  `TransferOut` work. That one check replaces the `opensStarted` counter and `isClosingForTransfer`.

Invariants checked by `transition`. Violating one throws, and tests assert it never happens:

- I1. At most one Live copy of a store per device. A device load that finds two retires both (T10) and logs an
  error. The entry is then rebuilt through the normal path.
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
  - some loaded device has a non-Retired copy of it, or Rebuild or TransferIn work for it;
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
  refusal) and `retireForeignCopy`'s staging-checkpoint dance. The latter is kept only to the extent that a foreign
  copy may have served since a first pass began: that copy is Lost, and the checkpoint rule stays.

### What stays

`FileStat` everywhere a phase directory is listed; the Missing Stores rule (Active entry, no Live copy, no work →
`RequestRebuild`); load, build and rebuild backoffs (now `Activity.AwaitingRetry` and the existing
`HeldRebuild`); the retire's four durable steps; the CRL save-file handling; the forget floor and `arrived-at`; and
the device-level markers.

## Plan outline

Each stage leaves the full suite green and is reviewable on its own.

1. **Registry over today's disk.**
   - Introduce `Phase`, `CopyRecord`, `Activity`, `Work` and `DeviceCopies`. Phase is computed from the marker
     files by one function, the only one that reads markers.
   - Derive router silence. Delete the in-memory sets listed above.
   - Verify I3 and the readers of the silence reason.
   - Expected effect: most of the in-memory bug class is gone, with no format change.
2. **Phase directories.**
   - Replace markers with the layout and `CopyMoves`, and give copies `copyId` names. Delete `StoreDirMarkers` and
     the marker-reading phase function.
   - Update bootstrap (`cmdline`, `DefaultBackendOpener.createStore`) to create into `live/`, and update the
     transfer service to ship from `outbound/` and stage into `transferring-in/`.
3. **Decision function.**
   - Extract `decide` and replace the passes.
   - Add an exhaustive test over every (entry status × up to two copies × each phase × activity × work)
     combination, asserting I1–I5, the store-copies release rule, and "no Action loads a non-Live copy".

## Tests

All deterministic (LockstepThreading) unless noted.

- **Table:** every illegal (from, to) pair throws, and every legal one moves the directory and updates the record.
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
  `rebuilt/` with the episode as `phase-param`.
- `server/crl/CLAUDE.md`, `docs/safety/rebuild-process.md`, `crash-recovery-log.md`, `systemic-loss-protection.md`:
  rename the marker references to phases. No safety argument changes, since each marker maps to one phase.
- `Todo.txt`: close the items this subsumes.

## Open questions

- Whether `Outbound` needs to keep a Rebuilt copy's episode in its `phase-param` so the source side can still
  recognize an Unconfirmed flip after a crash mid-transfer. Today it learns this from the shipped `rebuild-complete`
  still on its own copy.
- Whether `Parked` merges into Live with an Offline entry, if step 1 shows that `reloadClosedStores` and T5 are the
  same rule.
