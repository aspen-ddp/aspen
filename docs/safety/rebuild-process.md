# Store Rebuild Process: Safety Specification

Code paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`. Mechanisms are cited
as `path/File.scala` plus the symbol. Thresholds follow `CLAUDE.md`: `t` is a pool's `writeThreshold`, `r` its read
threshold `IDA.consistentRestoreThreshold` (a majority for every IDA), `w` its width.

Related specifications: [paxos-transactions.md](paxos-transactions.md) (expiry rules, acceptor amnesia),
[crash-recovery-log.md](crash-recovery-log.md), [transaction-requirements.md](transaction-requirements.md),
[transaction-recovery.md](transaction-recovery.md) (UnackedCommits, missed-update marking),
[repair-process.md](repair-process.md) (how ErrorTree entries are consumed), and
[systemic-loss-protection.md](systemic-loss-protection.md), which covers the question "what if the code wrongly
declares stores lost" and is the other half of this document.

---

## 1. Scope and vocabulary

A **store** is one slice-holder of a storage pool, `StoreId(poolId, poolIndex)`. A store is **lost** when the state
that makes it servable is gone or can no longer be trusted: its device-CRL entries, its `crl_save.log`, or its backend
(RocksDB/FSIF) files. A lost store cannot be repaired from its own media. **Rebuild** reconstructs the store's whole
slice set from the other stores of the pool: it walks the pool's allocation tree, reads each object through the
ordinary client read path (which needs `r` consistent replies from the peers), takes this store's encoded slice, and
writes it into a fresh staging backend. The rebuild writes no object of its own pool; its only write anywhere is the
ErrorTree marks of an operator-accepted loss (section 5.5).

Rebuild does **not** reconstruct transaction state (Paxos promises and accepts, commit votes, requirement locks). The
rebuilt store starts with an empty `Frontend`, a fresh incarnation, and an `acceptor-since` time before which it takes
no part in any transaction (section 6.4). What happened to the pool's objects while the store was away is reconciled
afterwards by the missed-update mechanism (ErrorTree), section 8 and [repair-process.md](repair-process.md), plus the
pending-allocation sweep for allocations the lost store had acknowledged (section 5.3).

| Term | Meaning |
|---|---|
| tree entry | `StorageDeviceState.stores(storeId)`: `StoreEntry(status, transferDevice, transferId, acceptLoss)` in the storage-devices tree (`common/metadata/StorageDeviceState.scala`) |
| pool entry | `StoragePoolState.stores(poolIndex) = StoreEntry(hostId, storageDeviceId)`: which device/host the pool says holds the store |
| `rebuild-required` | marker in a store directory: never load it, rebuild it (`RebuildingStore.RebuildRequiredMarkerFile`) |
| `rebuild-complete` | marker in a directory a rebuild produced, from just before its move into place until its entry leaves Rebuilding (`RebuildingStore.RebuildCompleteMarkerFile`) |
| staging | `<device>/rebuilding/<storeId.directoryName>/` (`RebuildingStore.RebuildDirectory`) |
| final path | `<device>/<storeId.directoryName>/` |
| pending-delete | `<device>/pending-delete/<dir>-<uuid>`: a retired copy (a discarded lost copy, a re-placed or transferred-away one), kept until the store is Active on its owner device (`RebuildingStore.PendingDeleteDirectory`, `StoreCopies`) |
| deleting | `<device>/deleting/`: every delete of a copy renames it here first, then removes it; leftovers are removed at device load and each reconcile (`StoreCopies.DeletingDirectory`) |
| checkpoint | `rebuild-state.yaml` in staging (`server/rebuild/RebuildState.scala`) |
| device check | one run of `DeviceReconciler.reconcileDeviceState` for a device: hourly (`cmdline/Main.scala` `CheckStorageDevicesPeriod`), on `CheckStorageDevice` nudges, and on the rebuild's own re-checks |

---

## 2. State machines

### 2.1 Tree-entry state (`StoreStatus`)

`StoreStatus` is `Initializing, Active, TransferringIn, TransferringOut, Rebuilding, Offline`; a device has
`DeviceStatus` `Operational, ShuttingDown, Shutdown, Failed`. Transitions involving `Rebuilding`:

| # | From | To | Writer | Condition |
|---|---|---|---|---|
| R1 | Active | Rebuilding (same device) | `StoreRebuilds.requestRebuild`, called by the reconcile's Lost CRL State pass | store in the device's `offlineStores` with a `rebuild-required` marker; device not Failed and owned by this host; entry Active. A retry after `TransactionOutcomeUnknown` that finds Rebuilding takes it as its own commit and checks the device at once |
| R2 | any | Rebuilding (same device) | `DeviceMetadata.adoptDevice` | adopted device; store's `DeviceDiscovery.adoptionClass` is `CrlLost` (a `rebuild-required` marker, or `transferring-out` with an unusable `crl_save.log`) |
| R3 | absent on destination | Rebuilding (another device) | `FailedStorageDeviceDurableTask.moveStore` | operator ran `fail-storage-device`; one store per drain pass; the same transaction removes the store from the tombstone and repoints the pool entry |
| R4 | Rebuilding on D1 | Rebuilding on D2 | `AspenClient.replaceRebuild` (`transfer-store` on a Rebuilding store) | operator re-placement; one transaction removes the D1 entry, writes D2's, repoints the pool (and the radicle's bootstrap config for a bootstrap-pool store) |
| R5 | Rebuilding | Active | `DeviceMetadata.markRebuiltStoreActive` returning `RebuildOutcome.Adopt` | device not Failed, owned by this host, entry Rebuilding, device not ShuttingDown/Shutdown (`StorageDeviceState.serviceStatus`) |
| R6 | Rebuilding | Offline | same method, `AdoptOffline` | device ShuttingDown or Shutdown |
| R7 | Rebuilding | unchanged | same method, `Discard` / `NotOwned` / `Superseded` | device Failed, or entry absent or of another status (`Discard`); device owned by another host (`NotOwned`); entry Rebuilding under another episode than the rebuild's (`Superseded`) |
| R8 | Rebuilding | unchanged | device shutdown (`DeviceShutdown.stopDeviceWork`) | `withActiveStoresOffline` touches only Active entries; the rebuild is cancelled with its checkpoint kept, and resumed after restart or adoption |
| R9 | Rebuilding on a tombstone | moved | `moveStore` | the drain moves a Rebuilding entry like any other |

Each of R1-R4 writes the entry through `StorageDeviceState.setRebuilding` with a fresh UUID in its `transferId`, the
rebuild **episode** (`StoreEntry.rebuildEpisode`). R1 and R4 draw it once per call, outside the retried transaction,
so a retry after `TransactionOutcomeUnknown` recognizes its own commit by the episode. A rebuild runs for the
episode of the entry the reconcile read. The episode is the identity of everything the rebuild leaves on disk: its
checkpoint and its `rebuild-complete` marker carry it, and a store re-placed away and back is a new episode, so
nothing an earlier episode left is resumed or promoted (sections 5.1, 5.7).

`acceptLoss` is set only on a Rebuilding entry, by `AspenClient.acceptRebuildLoss`, which keeps the episode. The
rebuild honours it only on an entry of its own episode. `setStoreEntry` writes a fresh entry, so the promoting flip
clears it and an acceptance covers one rebuild.

Rebuilding stores are excluded from the shutdown write-quorum check (`AspenClient.checkPoolsStayWritable`: only Active
entries on other Operational devices count), from the repairable set (`HostStoreView.of`), and from rebalancing
(`common/rebalancing/SetRebalanceDurableTask.scala`); `StorageDeviceState.incomingStores` reserves their full size
against free space.

### 2.2 Host-local store state (directory and markers)

The host decides from files, not from the tree (`StoreLoader.tryLoadStore`). Markers are checked in this order:
`rebuild-required`, `transferring-out`, `transferred-in`, `rebuild-complete`; any of them leaves the store offline in
the router and in the device's `offlineStores`. A directory carrying `rebuild-required` is therefore treated as lost
whatever else it carries.

| Local state | Evidence on disk | Router | Messages for the store |
|---|---|---|---|
| Served | `<store>/config.yaml`, no markers | installed | handled |
| Opening/loading | `startOpen` ran | `loading` | dropped |
| Lost, awaiting rebuild | `<store>/rebuild-required` | offline | dropped |
| Discarding | marker present, CRL entries being saved and dropped (`droppingCrlState`) | offline | dropped |
| Discarded | directory moved to `pending-delete/` | offline (silenced) | dropped |
| Staging | `rebuilding/<id>/` with config, backend files, checkpoint | not installed; silenced (`StoreRebuilds.silenced`) | dropped |
| Finished, awaiting flip | final path with `rebuild-complete` (content: the episode) | offline | dropped |
| Active again | loaded after the flip | installed | handled |

`rebuilding/`, `pending-delete/` and `deleting/` hold no `config.yaml` at their own level, so `DeviceDiscovery` never offers their
contents to `tryLoadStore`.

### 2.3 One rebuild pass (`RebuildingStore`)

```
start
  ├─ final path carries this episode's rebuild-complete ► delete staging, complete (flip only)
  ├─ final path exists without it                      ► fail (defence; StoreRebuilds retires such a copy first)
  └─ runPass
        load checkpoint (total: unreadable = none; another episode's = none)
        ► read pool state ► first pass: createStaging (discard staging, BackendOpener.create)
        ► openStaging (BackendStateLost: discard staging and checkpoint, fail)
        ► queryPeers: CurrentAllocationsRequest until w - t + 1 peers answer; merge with the checkpoint's list
        ► walk allocation tree: foreach | foreachFrom(lastRestoredKey) (inclusive), stopWhen = aborted
              per entry: restoreObject ► client.read ► rebuildWrite; every 1000: flush THEN checkpoint
        ► sweepPending: restore the listed allocations directly, re-querying in rounds
        ► abort latch check (out of space, > MaxFailedObjects failures, cancel, staging fail-stop)
        ► retryFailures (re-read tree entry; absent = deleted; still failing ► acceptLoss? mark : fail)
        ► finish: flush, close, re-check latch, remove checkpoint, acceptor-since, incarnation,
                  rebuild-complete (the episode), os.move(staging ► final), fsync device dir
  ► complete ► StoreRebuilds completion handler ► markRebuiltStoreActive
             ► Adopt | AdoptOffline | Discard | NotOwned | Superseded
```

---

## 3. Triggers: how a store becomes rebuild-required or Rebuilding

A detection site writes `rebuild-required` durably (`StoreRebuilds.writeRebuildRequiredMarker`, `DurableFiles.write`),
takes the store offline, and asks for a device check; the reconcile flips the entry (R1); the Rebuilding pass then
discards the old directory (section 4.3) and starts the rebuild. Adoption (R2) and the operator paths (R3, R4) write
the entry straight to Rebuilding.

| # | Trigger | Detection | Effect |
|---|---|---|---|
| T1 | `<device>/crl` missing from a device that holds store directories | `DeviceDiscovery.openDeviceCrl` | every store directory gets the marker; a fresh CRL is opened |
| T2 | the device CRL fails to open with anything but `CrlLocked`, `CrlSizingMismatch` (both refuse the device and retry each scan) or a transient error (retried up to `CrlOpenAttempts` = 3 scans in a row); `CrlCorrupt` is logged as such | same | every store directory gets the marker; the old CRL is kept as `crl.lost-<uuid>` |
| T3 | `crl_save.log` exists but `CrashRecoveryLog.loadValidStoreState` fails | `StoreLoader.handleLoad` ► `StoreRebuilds.crlSaveFileLost` ► `retireUnserved` | backend closed unserved, marker, offline, device checked |
| T4 | backend open throws `BackendStateLost` (FSIF `FsifRecovery.StoreLost`; RocksDB `RocksDBStateLost` for a directory with no database) | `StoreLoader.handleBackendOpened` | marker, offline, device checked |
| T5 | backend fail-stop while served (`Host.BackendFailed`; only FSIF's `onFatal` raises it) | `StoreRebuilds.backendFailed` | marker first, then `closeUnloaded(dropDurably = false)`: the CRL entries stay for the discard to save |
| T6 | backend failed between open and install | `StoreLoader.handleLoad` ► `retireUnserved` | marker, offline |
| T7 | backend fail-stop during a clean close | `StoreLoader.handleClose` ► `StoreRebuilds.failedOnClose` | marker; a transfer-out close's `crl_save.log` kept, as the copy's lost CRL state at its retire (4.3) |
| T8 | `Host.StoreFailStopped`: CRL recovery failed (`StoreRebuilds.RecoveryFailed`) or the Frontend fail-stopped (a commit voter that cannot hold the update it voted for) | `StoreRebuilds.handleFailStopped` ► `backendFailed`, only while that Store is still installed | as T5 |
| T9 | a `transferring-out` store with an unusable `crl_save.log`, found by adoption | `DeviceDiscovery.adoptionClass`; `DeviceMetadata.adoptDevice` | marker written in the transaction's prepare closure; entry written Rebuilding |
| T10 | a marked store on an adopted device | same | as T9 |
| T11 | transfer out requested for a marked store | `StoreTransfers.returnLostTransferToRebuild` | transfer aborted, entry back to Active, then R1 |
| T12 | a directory at a Rebuilding store's path that carries neither `rebuild-complete` of the entry's episode nor `rebuild-required` | `StoreRebuilds.retireForeignCopy` | marked (a served one is first fail-stopped through `backendFailed`), then discarded like any lost copy |
| T13 | operator declares the device dead | `fail-storage-device`, `FailedStorageDeviceDurableTask` | tombstone, then R3 per store |

The marker written in adoption's prepare closure (T9, T10) is correct whatever the transaction's outcome: the set of
CRL-lost stores is derived from the disk's own contents, so the state is gone whichever host ends up owning it.

Not triggers: other backend open failures are logged and retried at the next load; an out-of-space open leaves the
store intact and retries (`StoreLoader.retryStoresAwaitingSpace`); a failed `crl_save.log` load by a **transfer-in**
destination restarts the transfer, since the source still holds the state. Nothing declares a *device* Failed
automatically; only `fail-storage-device` does.

---

## 4. Placement and the discard of the old copy

### 4.1 In place (T1-T12)

Automatic triggers rebuild the store on the same device and host: the pool entry already names it. If the media is the
problem, the rebuild writes onto the same media; only an operator moves it (`fail-storage-device`, or `transfer-store`
on the Rebuilding store, R4).

### 4.2 Reassignment (T13, R4)

`failStorageDevice` enrolls `FailedStorageDeviceDurableTask` (poll period `DefaultPollPeriod`, 2 min). Its two
idempotent steps:

1. `tombstone`: one transaction removes the device from its set and its host's `HostState`, and writes the device
   state `Failed` with both ids zeroed. The store map is kept: it is the work list. The former host is nudged.
2. `drain`, one store per pass (`nextStore`, sorted). `isDisowned` (TransferringIn, or the pool no longer names this
   device for the index) drops stale bookkeeping without a rebuild, restoring an abandoned transfer source to Active
   in the same transaction. Otherwise `StorageDeviceSetState.selectDeviceForRebuild` picks a destination from the
   pool's set **before** the pool is repointed (it derives the failed device from the pool entry), then `moveStore` in
   one transaction removes the entry from the tombstone, writes the destination entry Rebuilding (refusing a Failed or
   non-Operational destination), rewrites the pool entry, and updates the radicle's bootstrap config for a
   bootstrap-pool store. All writes carry a `KeyRevision`; a concurrent drain loses and sees `StoreAlreadyMoved`.

Selection (`selectRebuildDevice`): the failed device is hard-excluded; devices holding the pool's other stores are
soft-excluded (used only if nothing else fits, which logs `REBUILD CO-LOCATED` at error); the candidate must be
Operational with free space at least the store's recorded usage (`StoragePoolState.getStoreUsageKey`; absent, the
size check is skipped with a warning) after the space already promised to `incomingStores`. A selection failure
(`AllocationError`) leaves the store on the tombstone for the next poll.

`replaceRebuild` (R4) refuses a store whose entry on its pool-named device is not Rebuilding, the same device, a
destination that already lists the store, and a Failed or non-Operational destination. It does not require the
source device or its host to be up. Both hosts are nudged; the old host's reconcile (`StoreRebuilds.abandonUnlisted`)
cancels a rebuild still running for a store no longer Rebuilding on that device, or Rebuilding there under another
episode, and deletes a staging copy for which no rebuild is running. A completion racing the move reads the entry
absent and gets `Discard`, or `Superseded` if the store was moved back meanwhile. The old host may be down throughout,
and the store may finish elsewhere, serve, and be re-placed back (by the operator, or by a drain choosing the device):
its returning reconcile then sees a Rebuilding entry whose episode none of its staging, checkpoint or finished copy
carries, and starts over (sections 5.1, 5.7). The remedy is manual by
design; automating it is in `Backlog.txt` ("Automate re-placement of a rebuild whose destination is full").

### 4.3 The discard of a marked store (nothing destroyed)

`StoreRebuilds.startRebuild` first silences the store (section 6.2), then proceeds only if all hold, in order: the
store is not already rebuilding here; `rebuildingStores.size + droppingCrlState.size < maxConcurrentRebuilds`
(default 2, `Host`); the device is not shutting down; `retireForeignCopy` clears the path (T12); and
`discardLostStore` reports the path clear. For a directory with `rebuild-required`, `discardLostStore` retires the
copy (`StoreCopies.retire`, the one path by which any store copy that is no longer its owner's leaves its store path):

1. the `rebuild-required` marker, already durable here;
2. the copy's CRL state saved into `<storeDir>/lost-crl-state.log`: kept if the file exists from an earlier attempt,
   else the copy's `crl_save.log` renamed to it (a transfer-out close that fail-stopped, T7, already deleted the
   entries from the device CRL), else the store's device-CRL entries read and saved (`CrashRecoveryLog.saveStoreState`);
3. the entries dropped from the device CRL durably (`CrashRecoveryLog.dropStore`; a rebuild onto the same device would
   recover them otherwise), releasing their admission charge;
4. the directory renamed to `pending-delete/<dir>-<uuid>` durably (`DurableFiles.move`);

and then starts the rebuild in the same control-thread callback.

While the retire runs, the store holds a rebuild slot through `droppingCrlState`, and the slot passes straight to the
rebuild, so one reconcile cannot discard every marked store on a device at once. A failure at any step frees the slot
and leaves everything for a later check; a crash leaves the marker, so the next attempt saves (unless saved) and drops
again. The store is never served meanwhile, so the entries cannot change under the save.

Nothing is deleted until the store is Active on its owner device, the device its pool names: `StoreCopies.release`
deletes a `pending-delete/` copy then, on this device by its own entry, elsewhere by the pool's entry and that
device's state; a failed read keeps it. The store is named by the copy's directory name, never by its contents, and a
name that identifies no store is kept. Deletes are rename-first through `<device>/deleting/`, so an interrupted one
leaves nothing partial behind. A lost copy whose rebuild is re-placed onto another device before it ran (section 4.2)
is retired the same way by the reconcile's Deleted Stores pass, once its entry has left this device, and released when
the destination's flip makes the store Active there. An accepted loss (5.5) ends in that flip too: the old copy goes
with it. What may be done with
suspected-loss content is the subject of [systemic-loss-protection.md](systemic-loss-protection.md) and
`Backlog.txt` ("System-level protection against correlated lost store declarations").

---

## 5. Rebuild execution

### 5.1 Start (`RebuildingStore.start`)

Constructed on the control thread by `StoreRebuildFactory.createRebuild`, with the episode of the entry the reconcile
read; everything expensive runs on the client's execution context. A final path carrying `rebuild-complete` of this
episode (`RebuildingStore.isRebuildCompleteFor`) is a finished rebuild awaiting its flip: staging is deleted and only
the flip remains. A final path without it fails the pass (a defence: `retireForeignCopy` has already retired such a
copy, a finished copy of an earlier episode included). Otherwise `runPass`, which resumes the checkpoint only if it
names this episode (`RebuildState.episode`); one of an earlier episode counts as none, so staging is discarded and
the walk starts over. A store re-placed away and back may have taken updates elsewhere, and repair consumed their
ErrorTree entries there, so the earlier episode's copy of every object is stale with nothing to mark it.

### 5.2 The walk

* `RebuildState.load` is total: an unreadable or torn checkpoint means "no checkpoint" and a full re-walk; a
  checkpoint without `pending-allocations` does not load either. A first pass (no checkpoint) discards whatever staging
  holds and creates a fresh staging store atomically (`BackendOpener.create` under `<device>/creating/`); a resume
  only opens it. `BackendStateLost` on that open discards staging, checkpoint included, and fails the pass; a
  fail-stop of the staging backend latches the abort.
* The walk is `pool.allocationTree.foreach` or `foreachFrom(lastRestoredKey)` (inclusive), on `TierEngine.walk`,
  which calls `fn` sequentially, logs and steps past a failing `fn`, and checks `stopWhen = aborted` before every
  entry and node read.
* `restoreObject` decodes the stored `ObjectPointer` inside one `try`, so an undecodable pointer is a failed object,
  not a silently skipped one. Entries of another pool are skipped; keys that are not 16 bytes are logged and skipped.
  `restore` does a full read through `client.withReadTimeout(ObjectReadTimeout)` (30 s), then `ObjectState.getRebuildDataForStore(storeId)` (re-encodes the object and
  takes this store's slice; a partial KV state yields none) and `Backend.rebuildWrite` with the read's revision,
  refcount and timestamp.
* Locked copies: a read ignores the write locks of transactions it does not know committed, and the one store that
  applied such a T may be the lost one, whose acknowledgment left T's finalizer nothing to mark. So when any reply of
  the read reported a write lock (`ObjectState.readLocks`), `restore` marks the object in the ErrorTree against this
  store (`MissedUpdateMarker.mark`, missed at `HLCTimestamp.Zero`) and completes only once the mark is written: the
  walk never advances past an unrecorded copy, and a failed mark is a failed object (retried by 5.4). This covers
  every stale copy of that kind: the rebuilding store is silent, and at most `w - t < r` stores hold T's old revision
  without a lock of T (its non-commit-voters), so `r` matching old-revision replies include an unapplied commit voter
  reporting T's lock. The mark needs no transaction timestamp: after promotion, at most `w - t + 1` stores hold the old
  revision without the lock (the rebuilt store added), still below `r` whenever `t > r` (a majority `r` gives
  `2r >= w + 1`), so repair's read of a stale copy reports the lock and defers rather than completing the entry.
  For `t <= r`, losing a store that held T before its peers applied it is beyond the pool's tolerance (`CLAUDE.md`,
  read threshold). Marks also land for locks that change nothing (an exclusive lock-only requirement) or for
  transactions that abort; each costs one repair read, on the rebuild's path, not the transaction's.
* A per-object read failure is recorded in `failed` and the checkpoint advances past it. A read that gets no answer
  (its pool below `r` with the missing stores silent, an allocation a commit voter parks, `Frontend.parkAllocationRead`)
  fails with `ReadTimeout` after `ObjectReadTimeout` and stops its driver: it is one such failure, so it holds the
  walk, the host's rebuild slot (`rebuildingStores`) and a cancel for at most that long. The same bound covers the
  sweep's reads (5.3: the object stays pending for the next round) and the retries (5.4). More than
  `RebuildState.MaxFailedObjects` (10,000) failures latch an abort. An out-of-space error (`Backend.isOutOfSpace`,
  ENOSPC or EDQUOT) latches an abort and logs `REBUILD OUT OF SPACE`. A cancel latches `RebuildCancelled`.
* Checkpoint: every `CheckpointInterval` (1,000) restored objects, `backend.rebuildFlush()` **then**
  `RebuildState.save` (temp file and atomic rename). Contents: store id, last restored key (hex), failed object ids,
  and `pending-allocations`, the union of the allocations listed at every pass's query so far. A checkpoint that
  fails is logged (and an out-of-space one latches the abort); losing one costs a re-walk from the previous one.

### 5.3 Pending allocations

An allocation becomes visible in the allocation tree only when its `AllocationFinalizationAction` runs, after commit
(section 7). The walk would miss an allocation that the lost store had acknowledged (so it has no ErrorTree mark) and
whose entry lands behind the walk's position. Two mechanisms close that gap:

* `queryPeers`, before each pass's walk, sends `CurrentAllocationsRequest` to the pool's other stores
  (`AspenClient.currentAllocations`, `client/internal/CurrentAllocationsQuery.scala`, resending at 1 s backing off to
  10 s) until `w - t + 1` answer. Each lists the allocations in the pool held by transactions it has not seen
  finalized or aborted (`Frontend.currentAllocations`); a store still loading its CRL does not answer. A committed
  transaction is held by at least `t` of its commit voters in the pool until `TxFinalized`, which comes only after
  its AllocationFinalizationAction recorded its objects, and expiry never discards a committed, unfinalized Tx. The
  lost store may have been one voter, so `w - t + 1` answers from the other `w - 1` include a holder. That count never
  exceeds `r` (Replication: `t >= r`; Reed-Solomon variants: `t >= restoreThreshold`, and `r` is a majority), so it
  adds no availability requirement beyond the reads the rebuild needs anyway.
* The listed set is merged into `listedAllocations` and carried by every checkpoint, so the walk never advances
  durably past an allocation it has not yet swept. A resumed pass sweeps what earlier passes listed: one finalized
  meanwhile may have its tree entry behind the checkpoint, and no timestamp filter could find it (its tree-writing
  transaction may prepare long before it is decided).
* `sweepPending`, after the walk, restores the listed objects directly, not through the tree, in rounds that query
  the peers again (delays 1, 2, 4, 8, 16, then 30 s; progress logged every 10 rounds): still listed and unresolved,
  wait; still listed and committed (by any replier), restore, retrying any read failure (an unapplied commit reads
  like a deletion); no longer listed (finalized, or aborted and forgotten), restore, or settle on a read with
  `InvalidObject.provesDeletion`. Transactions first listed in a later round are ignored. A cancel abandons the wait
  (`CurrentAllocationsQuery.Abandoned`). The sweep never waits on a finalization, which could need this very store
  in a pool with `t = w`, and it does not wait on resolution past `PendingWaitLimit` (horizon plus
  `Frontend.DefaultExpiryMargin` plus `MaxStartLead`, about 30 s, from the sweep's start). Whatever is still unsettled
  then is marked in the ErrorTree against this store (`markUnsettled`, missed at `HLCTimestamp.Zero`), durably before
  `finish`, and the sweep ends; a failed mark fails the pass. The wait cannot be unbounded: a transaction whose live
  voters disagree is undecided, and past expiry `ExpiryTally` rule G holds it while any store of it is silent, which
  this store is until the rebuild finishes. Nor can the sweep drop such a transaction unmarked: it may be decided
  commit, applied and acknowledged by the lost incarnation while the replying peers do not know the outcome yet, and
  then no finalizer marks this store. The mark makes the outcome irrelevant: repair restores the slice if the
  allocation commits and settles the entry on `InvalidObject.provesDeletion` if it aborts. The limit is liveness
  only, and a pass that fails after marking waits and marks again (marks only move forward).

Coverage: an allocation the lost store acknowledged committed before the rebuild, so it is either unfinalized at the
first query (listed, and on disk with every advance of the checkpoint) or finalized before it (in the tree ahead of
every walk). No clock enters. Residuals: at least `t` stores of the pool lost their Paxos state while T was
unfinalized (beyond the pool's acceptor tolerance); or a peer rebuilt after T started, which answers without holding T
and still counts. The latter needs T to start inside the window between that peer's own query and its
`acceptor-since`, since its rebuild's query listed everything pending before then.

### 5.4 Retries

`retryFailures` re-reads each failed object's allocation-tree entry: absent means deleted since, skipped; present is
read and restored again. Anything still failing either fails the pass ("incomplete", checkpoint saved) or, with
`acceptLoss`, is recorded (5.5).

### 5.5 Accepted loss

`RebuildingStore.lossAccepted` reads the store's entry on its device: Rebuilding with `acceptLoss` (a failed read
counts as no). With it, `recordAcceptedLoss` marks each still-listed object in the pool's ErrorTree against this store
(`MissedUpdateMarker.mark`, missed at `HLCTimestamp.Zero`, so it never displaces a newer mark), durably before
`finish`, and logs `REBUILD ACCEPTED LOSS`. Repair restores a slice if the object becomes readable again; the entry is
the record of the loss until then. A failed mark fails the pass. The flip that follows makes the store Active, which
releases the old copy (4.3), slices of the accepted objects included: the acceptance is the operator's release, and
`accept-rebuild-loss` warns of it. The `MaxFailedObjects` abort is not lifted by an
acceptance: a pool below its read threshold is not papered over.

### 5.6 Finish (`RebuildingStore.finish`)

`rebuildFlush`, close the backend, **re-check the abort latch** (a cancel or fail-stop during the close fails the
pass), remove the checkpoint and temp file, write `acceptor-since` (`writeAcceptorSince`: now plus
`AcceptorSinceMargin`, 10 s), write `inherited-votes` (`writeInheritedVotes`: the pass start plus the horizon plus
twice the margin; section 6.4, inherited votes), write a fresh `incarnation` (`StoreIncarnationFile.writeFresh`), write
`rebuild-complete` durably with the episode as its content, `DurableFiles.move(staging, final)` (both directories forced). All four files are written durably
in staging before the move, so the directory at the final path carries them from the instant it appears.

### 5.7 Completion and promotion (`StoreRebuilds.startRebuild`)

`markRebuiltStoreActive` reads the device state inside a transaction (`KeyRevision` on its state key; only collisions
retried) and decides: `Discard` if the device is Failed, or the entry is absent or neither Rebuilding, Active nor
Offline; `NotOwned` if another host owns the device; `Superseded` if the entry is Rebuilding under another episode
than the rebuild's; Rebuilding of its episode is flipped to `serviceStatus` (`Adopt` for Active,
`AdoptOffline` for Offline on a shutting-down device); an entry already Active is `Adopt` and one already Offline is
`AdoptOffline` (a concurrent completion, or this call's own attempt reported `TransactionOutcomeUnknown`, which then
gossips the store online itself). The host acts only after the decision:

* `Adopt`: removes `rebuild-complete` durably (`clearRebuildCompleteMarker`), then `loadStoreById`. The committed flip
  gossips `StoreMoved` (`HostServices.gossipStoreOnline`), clearing the `StoreOffline` gossiped at the start.
* `AdoptOffline`: `keepRebuiltStoreOffline` writes a `transferring-out` marker and an empty `crl_save.log`, then
  removes `rebuild-complete`; an adopter classes it Clean and a restart's reinstatement loads it.
* `Discard`: deletes the final directory (`LocalDevices.discardStoreDirectory`).
* `NotOwned`: leaves the copy for the new owner, whose rebuild finds the marked final path and only flips.
* `Superseded`: leaves the copy at the final path, silent. Its marker names the earlier episode, so the current
  episode's `startRebuild` retires it (`retireForeignCopy`: marked `rebuild-required`, kept in `pending-delete/`).
  An Active entry from another episode cannot meet a running rebuild: every transition out of Rebuilding on this
  device passes a reconcile whose `abandonUnlisted` cancels it.

Every settled outcome releases the slot and re-checks all devices; `NotOwned` re-checks the others only. A failed pass
or a permanently failed flip releases the slot, re-checks the **other** devices (re-checking its own would restart the
same store at once) and schedules a backoff re-check of its own (`StoreRebuilds.scheduleRebuildRetry`:
`Host.RebuildRetryInitialDelay` 30 s, doubled per consecutive failure of the store, capped at `RebuildRetryMaxDelay`
30 min, reset when a flip settles). A cancel for a shutdown re-checks the other devices; a cancel because the entry
left the device (`abandonedRebuilds`) re-checks all, so the next check deletes the staging copy.

---

## 6. Transactions while a store is being rebuilt

### 6.1 What the rebuilding store does

Nothing. The staging backend is never wrapped in a `Store`/`Frontend`/`Tx`, never installed in `StoreRouter`, never
given CRL entries, and never offered to `tryLoadStore`. Votes, locks and Paxos acceptor state are neither collected nor
produced for the store. In an in-place rebuild the old device-CRL entries were saved into the pending-delete copy and
dropped before the rebuild (4.3), so nothing on the device recovers them.

### 6.2 What the host does with messages addressed to the store

`StoreRouter` answers "unknown" only for a store that is neither installed nor `offline`, `loading` or
`awaitingDevice`. `startRebuild` marks the store offline (`router.markOffline`, recorded in `silenced`) on every
reconcile that sees its Rebuilding entry, queued for a slot or running, so the host drops its messages. The mark ends
with the outcome: an adopted store's install clears it; `AdoptOffline` and `NotOwned` keep it; `Discard`, a cancel
because the entry left the device, and `abandonUnlisted` for a queued store clear it unless a device's
`offlineStores` holds the store for another path. A failed pass keeps it until the retry. A device that is not loaded
has all its stores marked offline by `startDeviceCheck`. After a host restart a destination store answers unknown
until its device's first reconcile returns; the answer costs only a re-send.

### 6.3 Quorum arithmetic

The Rebuilding store is an ordinary **absent acceptor**; nothing in the transaction code knows about rebuilds.

* Every object's votes are tallied from its own pool's stores (`TransactionDriver`); a rebuilding store in a metadata
  pool therefore also constrains transactions that merely touch that pool's objects. One rebuilding store consumes one
  unit of `w - t` for the whole rebuild.
* A tally is commit-certain (`c >= t`), abort-certain (`w - a < t`) or undecided. An undecided tally only waits; no
  driver proposes abort on it. With the rebuilding store silent, a transaction whose live voters disagree (or one of
  which never voted) stays undecided, and past expiry the expiry tally **holds** (rule G of
  [paxos-transactions.md](paxos-transactions.md)): the silent store's vote may have been commit. Its commit voters
  keep their locks for as long as the store is silent. Once it replies it reports `amnesiac` (transaction started
  before its `acceptor-since`) or "unknown", and rules D/E discard T if the commit could not have been chosen.
* Designated-leader choice skips stores gossiped offline (`client/internal/transaction/DesignatedLeader.scala`), which
  is advisory; a leader chosen on the rebuilding store never answers, and recovery drivers take over after
  `Host.txHeartbeatTimeout` (5 heartbeats).
* Reads need `r` consistent replies, which the pool must supply without the rebuilding store; this is also the
  rebuild's own read requirement. A read whose replies cannot reach `r` because stores are silent (not erroring)
  retransmits forever: `SimpleReadDriver` has no timeout.
* `checkPoolsStayWritable` refuses the shutdown of another device that would leave a pool with fewer than `t`
  writable stores; a Rebuilding store is not writable.

### 6.4 The rebuilt store's acceptor state

A rebuilt store is a blank acceptor, and quorum arithmetic gives amnesia no tolerance: in Replication(3,2) one
amnesiac acceptor can be the whole `2t - w` intersection that carries a chosen value. Safety therefore rests on the
mute, not on the failure budget:

* It takes no part in any transaction started before its `acceptor-since` (rebuild finish plus 10 s, durable in the
  store directory and carried by transfers): no promise, vote, accept or CRL save; only a proof from an object that
  shows the update (`Frontend.acceptorSince`, `Tx.muted`). It answers expiry status requests as `amnesiac`, never as
  a non-holder (`Frontend.mayHaveForgottenUndecided`).
* Every store mutes an unknown transaction whose start leads its host's wall clock by more than `MaxStartLead` (2 s),
  or that reaches the host's join ceiling (`JoinCeiling`), so the old incarnation joined only transactions started
  before its host's clock at its stop plus 2 s. The 10 s margin covers that lead plus host-to-host clock skew; client
  clocks do not enter. Residual: if the old incarnation's host clock led the rebuilding host's by more than 8 s plus
  the rebuild's duration, a transaction it joined can start after `acceptor-since`.
* On load the store also records its load time (`Frontend.loadedAt`), refusing for good each unknown transaction that
  started before the load plus `MaxStartLead`.
* A fresh `incarnation` means UnackedCommits entries naming the old incarnation never apply through the rebuilt
  store (`Frontend.unackedLookupComplete` applies only an entry naming its own incarnation; another one calls
  `Tx.unackedCommitNotOwed`); the entry's reconcile installs the committed state instead. A rebuild also discards the
  old incarnation's applied stubs (`dropStore`), which is harmless for the same reason.
* A late commit reaching the rebuilt store for a muted transaction applies nothing; its owed state comes back through
  the ErrorTree or UnackedCommits entries the finalizer wrote for it.
* **Inherited votes.** A commit vote the old incarnation cast still counts for its transaction T (a driver keeps it
  until a nack or proof starts a new round, which a muted store never causes; a client's vote count is final; expiry
  rule B does not look at lock-only pools), but the rebuilt store holds none of T's locks. A transaction T' started
  after `acceptor-since` must not get its commit vote on an object T may lock, or T' commits (in Replication(3,2) with
  T's abort voter) and T can still commit after it. So `finish` writes `inherited-votes`
  (`RebuildingStore.writeInheritedVotes`, durable, carried by transfers) holding the pass start plus the horizon plus
  twice `AcceptorSinceMargin`: the old incarnation voted only before the pass began, in transactions started at most
  `MaxStartLead` after its clock, and no store joins a transaction past its start plus the horizon, so from then on
  every vote on such a T has been cast; one margin covers the lead and the old host's skew, the other the skew of the
  querying and answering hosts. Until a query after that time completes, the Frontend votes abort
  (`InheritedVoteLock`) on every object. The query (`HeldVoteQuery`, Host → `client.heldVotes`) asks `w - t + 1` of
  the other stores for the transactions they hold with outcome unknown and a vote that is commit or undecided
  (`Frontend.heldVotes`, carried in `CurrentAllocationsReply`); T can still commit with the old vote counted only with
  `t - 1` live commit voters among them, so the replies include one. The store then votes abort only on the objects
  of the reported transactions started before its `acceptor-since`, repeats the query at heartbeat (1 s doubling to
  30 s), and releases a transaction once a query no longer reports it or its `TxResolved`/`TxFinalized` reaches the
  store (one of T's stores): T is then decided and cannot commit after a later T'. Once none is held the file is
  removed; a reload that still finds it queries again. An abort vote, not a withheld one: a single store's abort does
  not abort a transaction its peers commit (RebuiltVoterLockIntegrationSuite).

---

## 7. Allocation tree as the work list

The walk visits every `(ObjectId -> ObjectPointer)` entry of `pool.allocationTree`, the only enumeration of a pool's
objects; the pointer carries the store-pointer bytes FSIF and RocksDB stores need.

* Allocation: `TransactionBuilder.addAllocatingObject` adds an `AllocationFinalizationAction` for every allocated
  object, so no allocation can be built without one. After commit the action runs a second transaction that
  `allocationTree.set`s the entry, retried until it succeeds. The object exists before it is listed.
* Deletion: the transaction that sets the refcount to zero carries a `DeletionFinalizationAction`, which later deletes
  the entry, so an entry can name an object already gone.
* Bootstrap creates every object through its local `allocate`/`allocateData`, which list it in the tree root written
  last. Tree nodes are objects allocated the same way. Metadata layout: an ordinary pool's trees live in a metadata
  pool, the metadata pools' trees in the bootstrap pool, the bootstrap pool's in itself.
* Walk and concurrent modification: a split moves entries to a new right node, which a walk that already read the old
  node has seen. A join has the emptied left node absorb its right neighbour, deleting the neighbour with its version
  bumped, so a walk following a stale link gets `InvalidObject`; `TierEngine.walk` then descends again to the node
  holding the last visited key and continues after it, failing the pass only after `MaxJoinRaceRestarts` restarts in a
  row without visiting a key. Entries are filtered only by the walk's bounds and the last visited key, so none is
  skipped silently.

---

## 8. Missed updates and deletes during the rebuild

### 8.1 Marking

`TransactionDescription.allDataStores` covers every store of every pool a transaction references; a Rebuilding store
is a member like any other. A store that sends no `TxCommitted` within `MissedUpdateFinalizationAction.errorTimeout`
(2 s) is marked in its pool's ErrorTree for every object it hosts that the transaction changes (lock-only objects
excluded): key `poolIndex || objectId`, value the timestamp and the object's store pointer
(`MissedUpdateFinalizationAction.markMissedUpdates`, written by `MissedUpdateMarker` one batch per pool at a time).
A store that never voted gets no UnackedCommits entry, only the ErrorTree mark. Deletes are covered the same way: the
refcount update is a change to a hosted object, and the entry keeps the store pointer, the only copy once the
allocation-tree entry is gone. The marker runs in parallel with T's other finalization actions; a marker that fails
fails the finalizer, which `TransactionDriver.startFinalizer` restarts after a backoff, so the transaction stays
unfinalized (its stores keep it) until its marks land.

### 8.2 Processing

Entries are processed only for repairable stores (served, and neither offline, rebuilding nor transferring out,
`HostStoreView.of`), and only while the store's pool entry names the scanning host (`StoreRepairer`), so a superseded
copy never consumes the current copy's entries. Entries written during the rebuild therefore wait until the rebuilt
store is loaded, and are then reconciled by its repair scans ([repair-process.md](repair-process.md)):

* allocation-tree entry present: reconcile the store's copy with a full read and delete the entry once complete;
* absent: if the entry is older than `minErrorEntryAgeForDeletion` (60 s), read the object: a read proving deletion
  drops the slice (FSIF frees a slot only if it still maps this object id), a read that restores it reconciles it,
  anything else defers. Younger entries are deferred (an allocation whose tree entry has not landed yet).

So an entry for the rebuilt store is deleted only once its copy is complete. Not closed by repair: an object whose
copy changes under every install, or never settles, stays stale and is warned about; and a staleness nothing marked
is never repaired (5.2's locked copies are marked for this reason).

### 8.3 Coverage limits

* `MissedUpdateFinalizationAction.limitTracking` disables tracking for an ErrorTree write only when that transaction
  references the pool the entry is about: in practice the bootstrap pool, whose trees live in itself. A rebuilt
  bootstrap-pool store may therefore hold stale copies of those tree nodes with no entry. Its first repair scan after
  load sweeps every node of both trees (`StoreRepairer.sweepBootstrapTrees`) and stays at the scan floor until they
  converge. The staleness window before the sweep runs is accepted (`Backlog.txt`, "Bootstrap tree sweep staleness
  window").

---

## 9. IDA thresholds and insufficient peers

* The rebuild has no admission check against pool health; it starts however many peers are Active. Pool-wide
  protection against correlated loss declarations is `Backlog.txt` ("System-level protection against correlated lost
  store declarations").
* Each restore needs `r` consistent replies from the other stores. The rebuilt slice is the decoded object
  re-encoded, exactly what a normal encode produces.
* Peers that **reply** with errors make reads fail: failures accumulate to `MaxFailedObjects`, then abort with the
  checkpoint intact; a smaller pool's walk finishes and `retryFailures` fails the pass. Either way the staging copy,
  checkpoint and Rebuilding entry stay, and the backoff re-check resumes. `accept-rebuild-loss` lets a pass finish
  past objects still unreadable at its end (5.5).
* Peers that are **silent** make reads time out (`ObjectReadTimeout`, 5.2): each is a failed object, so the walk goes
  on, `MaxFailedObjects` and the backoff re-check engage as for error replies, and a cancel ends the pass within one
  timeout. A pool whose objects all time out costs one timeout per object until the abort; the host's other
  rebuilds wait at most that long for its slot. The allocation-tree reads (the walk's node reads, the retries'
  `allocationTree.get`) and the pool lookup are not timed: they read the metadata pool, and a metadata pool short of
  `r` stalls the rebuild (FS14).
* Two stores of one pool rebuilding at once each read the other as missing. With Replication(3,2) neither can read
  anything (`r = 2` of one live store), so both wait until a store with data returns. With Reed-Solomon, both complete
  while `w - 2 >= r`. The concurrency bound is per host (`maxConcurrentRebuilds`), not per pool.

---

## 10. Crash and resumption matrix

| Crash point | State on restart | Behaviour |
|---|---|---|
| after the marker, before the flip | marker, entry Active | device load leaves it offline; reconcile R1 flips; proceeds |
| after R1, during the save/drop | marker, entry Rebuilding, entries in CRL, maybe `lost-crl-state.log` | the retire saves (unless saved) and drops again |
| after the drop, before the move | marker, no entries | the move runs; rebuild starts |
| mid-walk | staging + checkpoint (at most 1,000 objects behind) | resume from `lastRestoredKey` inclusive; `rebuildWrite` is an overwrite |
| between flush and checkpoint save | staging, previous checkpoint | resume from the older key (flush precedes checkpoint) |
| torn checkpoint | temp file or unreadable yaml | `load` returns none: full re-walk from a fresh staging store |
| in `finish` after checkpoint removal, before the move | staging, no checkpoint | full re-walk (wasteful, safe) |
| after the move, before the directory sync | staging or final path | re-walk, or the "already in place" path |
| after the move, before the flip | final path with `rebuild-complete`, entry Rebuilding | load leaves it offline; `start` sees the marker and flips |
| after the flip, before the marker's removal | final path with `rebuild-complete`, entry Active | loaded offline; the reconcile's Completed Rebuilds pass removes the marker and loads it |
| flip fails permanently | final path, entry Rebuilding | logged; backoff re-check re-enters `start`, which flips again |
| device shutdown mid-walk | staging + checkpoint, entry Rebuilding | `cancelFor` latches the cancel; the shutdown waits up to `shutdownWorkTimeout` (60 s); resume after restart or adoption |
| device adopted elsewhere mid-rebuild | staging + checkpoint on the moved disk | the new owner's reconcile sees Rebuilding (`adoptDevice` leaves it, episode and all) and resumes |
| host down mid-walk or awaiting the flip; store re-placed away (and maybe back) | staging + checkpoint, or a final path with `rebuild-complete`, of the earlier episode | not Rebuilding here: `abandonUnlisted` deletes the staging copy. Rebuilding here under a new episode: the checkpoint counts as none (fresh staging, full walk) and a finished copy is retired to `pending-delete/` |

---

## 11. Invariants

* **B1 No serving while rebuilding.** The staging backend is unreachable by the network and the router; a finished
  copy at the final path carries `rebuild-complete` from the instant it appears, and `tryLoadStore` leaves it offline
  until the flip commits.
* **B2 Marker before discard.** A store directory is moved aside only after its `rebuild-required` marker is durable,
  its CRL entries are saved into it, and the entries are durably dropped.
* **B3 Nothing destroyed before the store is Active on its owner device.** The pending-delete copy is deleted only
  once a tree read shows the store Active on the device its pool names, on every path that retires a copy (4.3).
* **B4 No load of a marked store.** `tryLoadStore` checks `rebuild-required` first.
* **B5 Flush before checkpoint.** The checkpoint never names an object that has not been flushed.
* **B6 Resume is inclusive and idempotent.** Re-restoring an object is an overwrite.
* **B7 Sequential walk.** `lastRestoredKey` advances only for objects already written, because the walk awaits each
  `fn` before the next.
* **B8 Listed allocations outlive the walk's progress.** Every checkpoint carries every allocation listed so far.
* **B9 Promotion is atomic and complete.** The final path appears only by renaming staging after the abort latch is
  re-checked and `acceptor-since`, `inherited-votes`, `incarnation` and `rebuild-complete` are durable.
* **B10 Load follows the metadata decision.** The store is loaded only after `markRebuiltStoreActive` returned
  `Adopt`, or after a tree read shows its entry Active.
* **B11 Metadata flips are guarded.** Every tree-entry change in this subsystem is a transaction carrying a
  `KeyRevision` on the device state key (and the pool config key where the pool is rewritten).
* **B12 Selection precedes repointing.**
* **B13 The tombstone is one-way and is the work list.**
* **B14 Every transaction that changes the pool's objects during the rebuild marks the store**, except writes to the
  bootstrap pool's own ErrorTree/UnackedCommits nodes (8.3).
* **B15 Missed-update processing waits until the store is repairable and named by its pool entry.**
* **B16 A rebuilt copy is discarded if its device is Failed or its entry has left it.**
* **B17 Rebuilding survives shutdown, adoption and restart.**
* **B18 Work belongs to one episode.** A rebuild resumes only a checkpoint of its entry's episode, takes only a
  finished copy whose `rebuild-complete` names it, and is flipped only while the entry still names it; a running
  rebuild whose entry moved to another episode is cancelled.
* **B18 A failed device's stores are not served after the tombstone.** The reconcile retires a device it reads Failed
  (`DeviceShutdown.retireFailedDevice`: a durable `failed` marker, then unloaded like a relinquished device), and no
  host loads a device carrying the marker. Residual: until its first reconcile, a restarted or partition-healed old
  host may still answer peers holding a stale store-to-host mapping (the no-fencing trade-off accepted for adoption).
* **B19 A rebuilt store never joins a transaction its old incarnation may have joined** (6.4), up to the clock-skew
  residual.
* **B20 A rebuilt store grants no commit vote over a lock its old incarnation's vote may still back** (6.4, inherited
  votes), up to the same clock-skew residual.
* **B21 A copy read under a write lock is recorded before the walk passes it.** Its ErrorTree mark is written before
  the restore completes (5.2, locked copies).

Not an invariant: the rebuild validates neither the decision that declared the store lost nor the pool's ability to
supply it; see [systemic-loss-protection.md](systemic-loss-protection.md).

---

## 12. Failure scenarios

**FS1 Crash mid-walk.** The staging backend recovers (RocksDB WAL or FSIF journal); the checkpoint gives the last
flushed key and its failed list; the pass resumes there. If staging recovery throws `BackendStateLost`, staging is
discarded with the checkpoint and the next pass starts over.

**FS2 Source peer fails mid-walk.** With `r` peers still answering, only latency changes. With fewer, a read whose
missing peers are silent times out and one whose peers answer with errors fails; either is retried at the end of the
pass.
No wrong data is written either way.

**FS3 A second store of the pool is lost during the rebuild.** Replication(3,2): one live store, below `r`; every
restore waits; the pool is also unwritable. The stores stay Rebuilding until a store with data returns. Reed-Solomon
pools tolerate `w - r` silent peers. No wrong data is produced.

**FS4 Transactions committing to objects already rebuilt.** The store is silent, so after 2 s the finalizer marks it
for every object the transaction changed on it. After promotion the repair scan installs the newer state. Safe subject
to B14.

**FS5 Transactions committing to objects not yet rebuilt.** The walk reads the newer state; the mark is still
written, and repair finds nothing to install and removes it.

**FS6 Object deleted after it was rebuilt.** The deletion marks the store with the saved store pointer; once the
allocation-tree entry is gone and the mark is older than 60 s, repair reads the object, and on a proven deletion drops
the slice. FSIF's repair delete is id-checked, so a slot reused by a later allocation is not clobbered.

**FS7 Object deleted after the walk saw its key but before the read.** The read fails and is retried at the end of the
pass; by then the tree entry is usually gone and the object skipped. If the deletion's finalization has not landed,
the pass ends incomplete and is retried after the backoff.

**FS8 Allocation racing the walk.** (a) The entry lands after the walk passed its key, by a transaction the store did
not vote in: it was marked, and repair installs the slice; a young mark is deferred until the entry lands. (b) The
entry lands before the walk reaches it: restored at its then-current state. (c) The lost store acknowledged the
allocation before it was lost: listed by the peers and restored by the sweep (5.3).

**FS9 Tree restructuring during the walk.** Splits are benign; joins and tail removals make a stale link fail, and the
walk resumes after the last visited key (section 7).

**FS10 The old device returns after its stores were reassigned.** (i) Same host: the reconcile retires the Failed
device under a durable `failed` marker and `tryLoadDevice` refuses it thereafter. (ii) Carried to another host:
`owning-host` names the old host, the new host looks it up and refuses a Failed device. (iii) A rebuild already running
on the old device when it is tombstoned completes and gets `Discard`.

**FS11 Two hosts rebuilding the same store.** (a) Adoption mid-rebuild: the old host's rebuild is cancelled by
`relinquishDevice` (or lost with its crash); the new owner resumes from the checkpoint; an old completion gets
`NotOwned`. (b) A tombstone repointing a store whose in-place rebuild runs: the destination builds a fresh copy; the old
completion gets `Discard`. (c) Concurrent drains: `KeyRevision`s make one lose (`StoreAlreadyMoved`). (d) The
double-start guard is host-local (`rebuildingStores`); across hosts it is the tree entry and the guarded flip.

**FS12 Destination runs out of space.** The abort latches, staging and checkpoint stay, `REBUILD OUT OF SPACE` names the
remedy (`transfer-store` on the Rebuilding store); the old device's reconcile then cancels the rebuild and deletes its
staging copy, and a pending-delete copy there goes once the store is Active elsewhere.

**FS13 Destination shut down or failed mid-rebuild.** Shutdown: cancel, resume later. Failed: the copy is discarded and
the drain moves the Rebuilding entry again.

**FS14 The pool's metadata pool, or the bootstrap pool, has lost stores.** The walk reads the allocation tree through
the metadata pool, and the bootstrap pool for the metadata pools' trees and every pool state. Losing enough bootstrap
stores makes every rebuild impossible (and every client start fail). Nothing mitigates this; it motivates
[systemic-loss-protection.md](systemic-loss-protection.md).

**FS15 The pool cannot supply an object.** It stays in the failed list and every pass ends incomplete until an
operator accepts the loss; the next pass records it in the ErrorTree and finishes.

**FS16 Rebuild finishes while the device shuts down.** The flip writes Offline, and the store is kept offline behind a
transfer-out marker and an empty save file; an adopter classes it Clean, and a restart of the device reinstates it.

**FS17 A copy left at the store's path by an earlier tenancy.** `retireForeignCopy` marks it and the discard keeps it in
pending-delete, so the store is rebuilt from the pool; a copy being served is first fail-stopped. Before the first
reconcile reaches the Rebuilding entry, a device load may serve it (RB-W5).

**FS18 A store re-placed away and back.** Its host is down mid-rebuild; `replaceRebuild` moves it, the copy finishes
elsewhere and serves, and the store later comes back Rebuilding (a drain choosing the device, or the operator). The
entry carries a new episode, so the host's returning reconcile resumes neither the old checkpoint nor the old finished
copy (5.1); a rebuild still running for the old episode is cancelled (4.2), and one that finishes first is
`Superseded` (5.7). The new episode walks the whole tree.

---

## Open weaknesses

### RB-W5. A markerless directory at a store path the tree does not list is loaded and served (PLAUSIBLE; stale replica)
Mechanism: `StoreLoader.tryLoadStore` loads any directory with a `config.yaml` and no marker, without reading the tree,
and nothing unloads a served store whose device entry is absent. The code no longer makes such a directory: every
copy that stops being its owner's is retired with a `rebuild-required` marker (4.3), and every delete is rename-first
through `<device>/deleting/` (`StoreCopies.delete`), so a partial delete cannot strip a marker and leave the rest at
the store path. What remains is a directory put at a store path by hand (a restored backup, a copy moved back from
`pending-delete/` without its marker removed deliberately). `retireForeignCopy` retires such a copy only when the
store is Rebuilding on that device, and only from the first reconcile after the load.
Trigger / scenario: an operator copies an old store directory back onto a device, then restarts its host. If S is
Rebuilding on that device, the copy serves until the first reconcile (seconds); if the device has no entry for S, the
copy serves indefinitely (its repair scans are fenced by the pool entry, but it answers reads and prepares from peers
holding a stale mapping).
Consequence: an old incarnation of S, with stale slices and no `acceptor-since`, answers reads and votes. The load
floor (`Frontend.loadedAt`) keeps it out of transactions started before the load; it can still vote on newer ones from
stale state.
Suggested fix: have the reconcile close (and retire) a loaded store whose device entry is absent.
Effort: S   Priority: 5
