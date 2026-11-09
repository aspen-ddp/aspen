# Store Rebuild Process: Safety Specification

Status: derived from the code at commit `c3ad5493` (2026-11-09). Every mechanism below is cited as `path:line`
relative to `src/main/scala/org/aspen_ddp/aspen/` unless the path starts with `docs/`. Statements were checked against
the code, not against `CLAUDE.md` or the design documents; where they disagree the code wins and the disagreement is
called out.

Related specifications: [paxos-transactions.md](paxos-transactions.md), [crash-recovery-log.md](crash-recovery-log.md),
[transaction-requirements.md](transaction-requirements.md), [transaction-recovery.md](transaction-recovery.md),
[repair-process.md](repair-process.md), and the companion [systemic-loss-protection.md](systemic-loss-protection.md),
which covers the question "what if the code wrongly declares stores lost" and is the other half of this document.

Confidence marks used in the flaw list: **High** (traced end to end, low chance of a missed guard), **Medium** (the
path is real but a guard or an environmental assumption could make it unreachable), **Low** (plausible, needs a test).

---

## 1. Scope and vocabulary

A **store** is one slice-holder of a storage pool, `StoreId(poolId, poolIndex)`. A store is **lost** when the state
that makes it servable is gone or can no longer be trusted: its device CRL entries, its `crl_save.log`, or its backend
(RocksDB/FSIF) files. A lost store cannot be repaired from its own media. **Rebuild** reconstructs the store's whole
slice set from the other stores of the pool: it walks the pool's allocation tree, reads each object through the
ordinary client read path (which needs the pool's IDA read quorum from the peers), takes this store's encoded slice,
and writes it into a fresh backend. Rebuild is a read-only consumer of peers; it never writes to them.

Rebuild deliberately does **not** reconstruct transaction state (Paxos promises/accepts, commit votes, requirement
locks). The rebuilt store starts with an empty `Frontend`. Everything that happened to the pool's objects while the
store was away is reconciled afterwards by the ordinary missed-update mechanism (ErrorTree), see section 8 and
[repair-process.md](repair-process.md).

Terms:

| Term | Meaning |
|---|---|
| tree entry | `StorageDeviceState.stores(storeId)`: `StoreEntry(status, transferDevice, transferId)` in the storage-devices tree (`common/metadata/StorageDeviceState.scala:37,52`) |
| pool entry | `StoragePoolState.stores(poolIndex) = StoreEntry(hostId, storageDeviceId)`: which device/host the pool says holds the store (`common/metadata/StoragePoolState.scala:24`) |
| marker | the empty file `rebuild-required` in a store directory (`server/rebuild/RebuildingStore.scala:27`) |
| staging | `<device>/rebuilding/<storeId.directoryName>/` (`server/rebuild/RebuildingStore.scala:22,73`) |
| final path | `<device>/<storeId.directoryName>/` (`server/rebuild/RebuildingStore.scala:74`) |
| checkpoint | `rebuild-state.yaml` in staging (`server/rebuild/RebuildState.scala:30,87`) |
| device check | one run of `DeviceReconciler.reconcileDeviceState` for a device, run hourly (`cmdline/Main.scala:82`) and on nudges |

---

## 2. State machines

### 2.1 Tree-entry state (`StoreStatus`)

`StoreStatus` is `Initializing, Active, TransferringIn, TransferringOut, Rebuilding, Offline`
(`common/metadata/StorageDeviceState.scala:37`). A device additionally has `DeviceStatus`
`Operational, ShuttingDown, Shutdown, Failed` (`:44`). The transitions that involve `Rebuilding`:

| # | From | To | Writer / code | Condition |
|---|---|---|---|---|
| R1 | Active | Rebuilding (same device) | `StoreRebuilds.requestRebuild` `server/host/StoreRebuilds.scala:448-485`; called by the reconcile "Lost CRL State" pass `server/host/DeviceReconciler.scala:166-176` | store is in `offlineStores` with a `rebuild-required` marker, device not Failed, `state.hostId == ctx.hostId`, entry is Active |
| R2 | Active/other | Rebuilding (same device) | `DeviceMetadata.adoptDevice` `server/host/DeviceMetadata.scala:471,540-553` | adopted device; store's `adoptionClass` is `CrlLost` (marker, or `transferring-out` marker with an unusable `crl_save.log`) |
| R3 | (absent on dest) | Rebuilding (other device, usually other host) | `FailedStorageDeviceDurableTask.moveStore` `common/metadata/management/FailedStorageDeviceDurableTask.scala:386-` (dest write near the end of `moveStore`) | operator ran `failStorageDevice`; the tombstone is drained one store per pass; the same transaction removes the store from the tombstone and repoints the pool entry |
| R4 | Rebuilding | Active | `DeviceMetadata.markRebuiltStoreActive` `server/host/DeviceMetadata.scala:381-` returning `RebuildOutcome.Adopt` (`:95-`) | device not Failed, owned by this host, entry still Rebuilding, device not ShuttingDown/Shutdown |
| R5 | Rebuilding | Offline | same method, `AdoptOffline` | device ShuttingDown/Shutdown (`StorageDeviceState.serviceStatus` `:88`) |
| R6 | Rebuilding | (unchanged) | same method, `Discard` / `NotOwned` | device Failed, or the entry vanished or has another status (`Discard`); device now owned by another host (`NotOwned`) |
| R7 | Rebuilding | (stays) | device shutdown, `DeviceShutdown.stopDeviceWork` `server/host/DeviceShutdown.scala:146-` | `withActiveStoresOffline` touches only Active entries; the rebuild is cancelled with its checkpoint kept and resumed after restart or adoption |
| R8 | Rebuilding | (removed from tombstone) | `moveStore` when the device is later failed | drain moves the Rebuilding entry like any other (`isDisowned` is false while the pool still names the device) |

Rebuilding stores are excluded from: the shutdown write-quorum check (`client/AspenClient.scala:644`, only Active
stores on Operational devices count as writable), the repairable set (`server/host/HostStoreView.scala:27`),
rebalancing (`common/rebalancing/SetRebalanceDurableTask.scala:54`), and `incomingStores` space accounting
(`common/metadata/StorageDeviceState.scala:116` counts them as space still to be filled).

### 2.2 Host-local store state (directory and markers)

A store on a device is in exactly one of these local states. The state is encoded by directory layout and marker
files, not by one variable, so the host decides from files (`server/host/StoreLoader.scala:67-97`).

| Local state | Evidence on disk | Router | Messages for the store |
|---|---|---|---|
| Served | `<store>/config.yaml`, no markers, loaded | installed in `StoreRouter` | handled |
| Opening/loading | `startOpen` ran (`StoreLoader.scala:120`) | `loading` mark (`StoreRouter.scala:338`) | dropped silently |
| Lost, awaiting rebuild | `<store>/rebuild-required` | `markOffline` (`StoreLoader.scala:78`, `StoreRebuilds.scala:356`) | dropped silently |
| Discarding | marker present, device-CRL entries being dropped (`droppingCrlState`) | offline | dropped silently |
| Cleared | store directory gone (renamed into `<device>/discarded/` then deleted, `StoreRebuilds.scala:524-540`) | offline | dropped silently |
| Staging | `<device>/rebuilding/<id>/` with `config.yaml`, backend files, `rebuild-state.yaml` | **not in router** | see section 6.2: silent if the router had marked it offline earlier, otherwise answered "unknown store" |
| Promoted | `<device>/<id>/` present (no markers) | not installed until the load | as above, until loaded |
| Active again | loaded after the tree flip | installed | handled |

Markers that interact with rebuild: `rebuild-required` (never load, rebuild it), `transferring-out` and
`transferred-in` (transfer state; `server/host/StoreLoader.scala:80-91`). `rebuild-required` is checked first, so a
store carrying both is treated as lost.

### 2.3 One rebuild pass (`RebuildingStore`)

```
start (131)                      ┌── finalPath exists ──► cleanupStaging, complete (flip only)
  │                              │
  └─ no finalPath ► runPass (168)┘
        load checkpoint (171)  ► mkdir staging ► read pool state (183) ► write config.yaml (185-187)
        ► openStaging (188, 214)  ► read pool (191)
        ► walk allocation tree: foreach / foreachFrom(lastRestoredKey) (193-198)
              per entry: restoreObject (237) ► attemptRestore ► restore (326) ► recordRestored (344)
                          every 1000: flush THEN checkpoint (385)
        ► abort latch check (201)         [out of space, >10000 failures, cancel]
        ► retryFailures (204, 420)         [re-read allocation-tree entry; deleted ► skip]
        ► finish (205, 455): flush, close, remove checkpoint, os.move(staging ► final), sync dir
  ► complete promise ► StoreRebuilds.startRebuild completion handler (224)
        ► markRebuiltStoreActive (230)  ► Adopt: loadStoreById | AdoptOffline | Discard | NotOwned
```

---

## 3. Triggers: how a store becomes rebuild-required or Rebuilding

The rule: a *detection* site writes the `rebuild-required` marker (durable, `DurableFiles.write`,
`server/host/StoreRebuilds.scala:544`), takes the store offline in the router, and asks for a device check; the
reconcile then flips the tree entry (R1); the Rebuilding pass starts the rebuild and, first, discards the old
directory (section 4.3). Sites that skip the marker (R2, R3) put the entry straight to Rebuilding.

| # | Trigger | Detection code | Effect |
|---|---|---|---|
| T1 | Device CRL directory missing from a device that holds store directories | `DeviceDiscovery.openDeviceCrl` `server/host/DeviceDiscovery.scala:498-520` (`:504`) | every store directory on the device gets the marker; `<device>/crl` is moved to `crl.lost-<uuid>` if present; fresh CRL created |
| T2 | Device CRL fails to open (any `Throwable`) on a device that holds store directories, e.g. `SimpleCRL.SizingMismatch`, broken entry chain | same method `:508-515`; `server/crl/simple/SimpleCRL.scala:53,84-93` | as T1 |
| T3 | Store's `crl_save.log` exists but `loadValidStoreState` returns Left (missing header, truncated, wrong store id, any `Throwable`) | `StoreLoader.handleLoad` `server/host/StoreLoader.scala:293-296`; `server/crl/CrashRecoveryLog.scala:105-114` ► `StoreRebuilds.crlSaveFileLost` `:344` ► `retireUnserved` `:354` | backend closed unserved, marker written, offline, device checked |
| T4 | Backend open throws `BackendStateLost` (only FSIF: `FsifRecovery.StoreLost`; RocksDB never throws it) | `StoreLoader.handleBackendOpened` `:188-202`; `server/store/backend/Backend.scala:28`; `server/store/backend/fsif/FsifRecovery.scala:16,168-190` | marker, offline, device checked |
| T5 | Backend fail-stop while served (`BackendFailed`; only FSIF's `onFatal` posts it) | `Host.scala:453` ► `StoreRebuilds.backendFailed` `:396-438` | marker written first, then `closeUnloaded(dropDurably = marked)` drops the store's device-CRL entries durably |
| T6 | Backend failed between open and install | `StoreLoader.handleLoad` `:287-291` ► `retireUnserved`; `handleBackendOpened` `:173-183` marks if `backend.hasFailed` | marker, offline |
| T7 | Backend fail-stop during a clean close | `StoreLoader.handleClose` `:525-527` ► `StoreRebuilds.failedOnClose` `:374-388` | marker; `crl_save.log` removed |
| T8 | Failure to read or recover the store's transactions from the device CRL while served | `StoreRouter.readAndRecover` `:222-256` ► `Host.StoreRecoveryFailed` ► `StoreRebuilds.handleRecoveryFailed` `:66` ► `backendFailed` | as T5 (fail-stop, marker, CRL entries dropped durably) |
| T9 | `transferring-out` store with an unusable `crl_save.log`, found during **adoption** | `DeviceDiscovery.adoptionClass` `:286-298`; `DeviceMetadata.adoptDevice` `:540-553` | marker written in the transaction's prepare closure; entry written Rebuilding in the adoption transaction |
| T10 | Store carries the marker when its device is adopted (host marked it, or T1-T8 happened before the drive was pulled) | `adoptionClass` `:289-290` | as T9 |
| T11 | Transfer out requested for a store that is marked | `StoreTransfers` `server/host/StoreTransfers.scala:604-611,636-641,668-` `returnLostTransferToRebuild` ► `client.abortStoreTransfer`, then device check | transfer aborted, entry back to Active, then R1 |
| T12 | Operator declares the device dead (`fail-storage-device`) | `client/internal/BaseAspenClient.scala:719`; task `FailedStorageDeviceDurableTask` | tombstone, then R3 for each store, onto other devices |

Not triggers (checked): RocksDB open failures are only logged (`DefaultBackendOpener`
`server/store/backend/BackendOpener.scala:29` returns a failed Future; `handleBackendOpened` `:223` logs it); an
out-of-space open leaves the store intact and retries (`StoreLoader.scala:203-222`); a failed `crl_save.log` load by a
**transfer-in** destination restarts the transfer instead (`StoreTransfers.scala:471-476`). There is no automatic
declaration of a *device* as Failed; only the operator command (`client/AspenClient.scala:325`, sole caller
`cmdline/Main.scala:2207`).

---

## 4. Placement and the destructive prelude

### 4.1 In place (T1-T11)

All automatic triggers rebuild the store **on the same device and host**: the pool entry already names it, so nothing
in the pool changes (`StoreRebuilds.scala:440-447`). If the device itself is the problem (bad media) the rebuild
writes onto the bad media; there is no automatic path from "store lost" to "move the store to another device". The
only reassignment is the operator command.

### 4.2 Reassignment (T12)

`failStorageDevice` enrolls `FailedStorageDeviceDurableTask` (`BaseAspenClient.scala:719-`; task poll period 2 min,
`FailedStorageDeviceDurableTask.scala:30`). Its two idempotent steps:

1. `tombstone` (`:186`): one transaction removes the device from its set and from its host's `HostState`, and writes
   the device state with `status = Failed` and both ids zeroed. The store map is preserved; it is the work list.
   The tombstone is one-way (`AspenClient.scala:312-324`).
2. `drain` (`:317`) moves one store per pass: `isDisowned` (`:270`) drops stale entries (TransferringIn, or the pool
   no longer names this device) without a rebuild; otherwise `set.selectDeviceForRebuild` (`common/metadata/
   StorageDeviceSetState.scala:178`) picks a destination **before** the pool is repointed, then `moveStore` (`:386`)
   in one transaction: removes the entry from the tombstone, writes the destination entry `Rebuilding`, rewrites the
   pool entry to `(dest host, dest device)` (and the radicle bootstrap config for bootstrap-pool stores). Nudge sent
   best-effort; the destination host's hourly check is the guarantee.

Selection (`StorageDeviceSetState.selectRebuildDevice` `:398`): the failed device is hard-excluded; devices that already
hold other stores of the pool are soft-excluded (used only when nothing else exists, so two stores of a pool may share a
device); the candidate must be Operational and have free space of at least the store's recorded usage
(`StoragePoolState.getStoreUsageKey`, 0 if absent) minus space already promised (`incomingStores`). Selection failure
(`AllocationError`) leaves the store on the tombstone; the task retries every poll.

### 4.3 The destructive prelude to every rebuild of a marked store

> **Changed 2026-11-09 (F2 fix).** The prelude is no longer destructive. `discardLostStore` saves the store's
> device-CRL entries into `<storeDir>/lost-crl-state.log`, drops them from the device CRL (a rebuild onto the same
> device would recover them otherwise), and moves the directory to `<device>/pending-delete/<dir>-<uuid>`. Nothing
> is deleted there. The device's reconcile deletes the copy only once the store is Active again, wherever it was
> rebuilt; a failed read keeps it. A fail-stopped backend no longer drops its entries at close: the discard saves
> them. With nothing destroyed before reconstruction, the cap's gap below no longer matters. The text below
> describes the code before the fix.

`StoreRebuilds.startRebuild` evaluates, in order, `!rebuildingStores.contains(storeId)`,
`rebuildingStores.size < maxConcurrentRebuilds` (default 2, `server/Host.scala:179`), device not shutting down, and
`discardLostStore` (`StoreRebuilds.scala:167-169`). `discardLostStore` (`:499-521`), if the directory has the marker:

1. `local.crl.dropStore(storeId)` - durable removal of every device-CRL entry of the store (`server/crl/
   CrashRecoveryLog.scala:159`); returns false (start deferred) until done;
2. then `removeLostStoreDirectory` (`:524-540`): `os.move` to `<device>/discarded/<dir>-<uuid>`, fsync the device
   directory, `os.remove.all` the moved copy (failures only warn; `loadOwnedDevice` deletes `discarded/` at the next
   load, `DeviceDiscovery.scala:549-552`);
3. only then does the device check run again and start the rebuild.

Consequences stated as fact: the old backend files and the store's Paxos/vote state are destroyed **before** the rebuild
has read a single object, and nothing checks that the pool can supply the data. The concurrency cap applies to rebuilds
that have registered, not to the discard step: `rebuildingStores` is only populated after `discardLostStore` returns
true, so a single reconcile pass over many marked Rebuilding stores starts the discard of all of them.

---

## 5. Rebuild execution

### 5.1 Start (`RebuildingStore.start`, `server/rebuild/RebuildingStore.scala:131`)

The constructor starts the work from the control thread but everything expensive is inside the Future. If a directory
exists at the final path, the rebuild is treated as already done: staging is deleted and only the metadata flip
remains (`:134-139`). Otherwise `runPass`.

### 5.2 Pass

* The checkpoint is read (`RebuildState.load` `server/rebuild/RebuildState.scala:107`, total: unreadable means "no
  checkpoint", a full re-walk), staging is created, the pool state read (`client.getStoragePoolState`), a
  `StoreConfig` for the pool's backend written up front, the staging backend opened through `BackendOpener`.
  `BackendStateLost` while opening staging discards staging (checkpoint included) and fails the pass (`:224-228`).
* The walk is `pool.allocationTree.foreach` or `foreachFrom(lastRestoredKey)` (inclusive) on the pool's TKVL
  (`:193-198`). `TierEngine.walk` (`client/tkvl/TierEngine.scala:121-`) descends to the first tier-0 node and follows
  right links, calling `fn` **sequentially**, one entry at a time, logging and stepping past a failing `fn`.
  `stopWhen = aborted` is checked before every entry and every node read.
* `restoreObject` (`:237-281`) decodes the stored `ObjectPointer`; entries for a different pool, and keys that are not
  16-byte ObjectIds, are skipped (`:255`, `:304-310`). `restore` (`:326`) does `client.read(ptr)` (a full read through
  the normal read driver, IDA-quorum consistent), then `os.getRebuildDataForStore(storeId)` (re-encodes the restored
  object and takes this store's slice, `client/ObjectState.scala:94`) and `backend.rebuildWrite(ptr, type, metadata,
  slice)` with the read's revision, refcount and timestamp. Partial/range states are never used
  (`ObjectState.scala:136`).
* Per-object read failure: recorded in `failed`, the checkpoint advances past it, retried at the end of the pass
  (`:277-278`, `:360`, `:420`). More than `MaxFailedObjects` = 10,000 failures latches an abort (`RebuildState.scala:59`).
  Out-of-space latches an abort and logs `REBUILD OUT OF SPACE` (`:263-276,288`). A cancel latches `RebuildCancelled`
  (`:117`).
* Checkpoint: every 1,000 restored objects (`:37`), `backend.rebuildFlush()` **then** atomic temp+rename save of
  `rebuild-state.yaml` (`:385-416`; `RebuildState.save` `:87`). Contents: store id, last restored key (hex), failed
  object ids.
* `retryFailures` (`:420-452`): for each failed id re-reads the allocation-tree entry; absent means deleted since,
  skipped; present is re-read and restored; any remaining failure fails the pass ("incomplete") with the failure list
  checkpointed.
* `finish` (`:455-471`): `rebuildFlush`, close the backend, **re-check the abort latch** (a cancel or fail-stop during
  close fails the pass), remove the checkpoint and temp, `os.move(staging, final)`, fsync the device directory.

### 5.3 Completion and promotion (host side)

The completion handler (`StoreRebuilds.scala:224-302`) calls `markRebuiltStoreActive`, which reads the device state
inside a transaction with a `KeyRevision` on the device state key and decides: `Discard` if the device is Failed
(`DeviceMetadata.scala:395`) or the entry is absent/not Rebuilding-Active-Offline; `NotOwned` if the device now belongs
to another host; otherwise writes `serviceStatus` (Active, or Offline on a device shutting down). Only after the
transaction decides does the host act: `Adopt` calls `loadStoreById` (the store is served only now), `AdoptOffline`
keeps it offline behind a `transferring-out` marker plus an empty `crl_save.log` (`keepRebuiltStoreOffline`
`StoreRebuilds.scala:316`), `Discard` deletes the directory (`LocalDevices.discardStoreDirectory`
`server/host/LocalDevices.scala:51`), `NotOwned` leaves the copy for the new owner (its rebuild finds the final path
and only flips the entry). On `Adopt` the host then gossips `StoreMoved` (`HostServices.scala:215`), which clears the
`StoreOffline` gossiped at start (`StoreRebuilds.scala:222`).

A walk failure re-checks all devices **except** the failing one (`:288-302`), so a failed pass is retried at the next
periodic device check (an hour, `cmdline/Main.scala:82`) unless a nudge arrives; this is deliberate, to avoid an
unbounded spin.

---

## 6. Transactions while a store is being rebuilt

### 6.1 What the rebuilding store does

Nothing. The rebuilt data lives in a bare `Backend` opened by `RebuildingStore.openStaging`; it is never wrapped in a
`Store`/`Frontend`/`Tx`, never registered with `StoreRouter`, never given a CRL entry, and its staging directory has no
`config.yaml` at the level `tryLoadStore`/`storesIn` look (`RebuildingStore.scala:19-22`;
`DeviceDiscovery.storesIn` `:255`). There is no code path in which a prepare, accept, heartbeat, status request or
read reaches the staging backend. No attempt is made to process active transactions. Votes, locks and Paxos acceptor
state are neither collected nor produced for the store. Additionally the store's old device-CRL entries were dropped
before the rebuild (section 4.3), and the in-place rebuild's old directory deleted, so there is nothing to recover.

### 6.2 What the host does with messages addressed to the store

`StoreRouter` looks the store up in `stores`; if absent it calls `missing`, which replies "unknown" only when
`answersUnknown` holds: the host is ready, and the store is not `offline`, `loading` or `awaitingDevice`
(`server/host/StoreRouter.scala:82-84,142-163`).

| Case | Offline mark set? | Behaviour |
|---|---|---|
| In-place rebuild after T1-T11 | yes (`StoreLoader.scala:78`, `StoreRebuilds.scala:356,379`, `DeviceDiscovery` awaiting marks for adoption) | messages dropped; peers retransmit/time out |
| Rebuild on a destination device after T12 | **no** | `startRebuild` only gossips `StoreOffline` (`StoreRebuilds.scala:222`); the router never marks it offline, so the host answers `TxUnknownStore` to tx messages and `ReadError.StoreNotFound` to reads (`StoreRouter.scala:142-163`) |
| Host restart during a rebuild (either) | in-place: yes through the marker/offline path; destination: no | same as the rows above |
| Device unloaded or not yet loaded | `startDeviceCheck` marks all of the device's stores offline (`DeviceReconciler.scala:482`) | dropped |

Receiver behaviour on "unknown": a transaction driver drops its cached store mapping (`server/transaction/
TransactionDriver.scala:216-224`); a read driver drops the mapping and **immediately re-sends** the read to the same
store (`client/internal/read/BaseReadDriver.scala:160-178`), for a bootstrap-pool store `dropStoreMapping` triggers a
bootstrap-config refresh (`common/network/MetadataManager.scala:281-284`). See flaw F7.

### 6.3 Quorum arithmetic

The Rebuilding store is an ordinary **absent acceptor**; nothing in the transaction code knows about rebuilds.

* The Paxos participants of a transaction are all `ida.width` stores of the primary object's pool
  (`TransactionDriver.scala:88-90,144`: `Proposer(index, width, writeThreshold)`, `Learner(width, writeThreshold)`),
  quorum = `writeThreshold` (`IDA.scala:106`), at least a majority for both IDAs (`IDA.scala:203`, `:408`). Fault
  tolerance is `IDA.failureTolerance = width - writeThreshold` (`IDA.scala:144`). One rebuilding store consumes one
  unit of that budget for the whole length of the rebuild, however long (hours).
* Per object, the driver needs `nReplies >= writeThreshold` (`TransactionDriver.scala:281`). If some stores have not
  replied, commit is still reachable (`nCommitVotes < writeThreshold && width - nAbortVotes >= writeThreshold`) it
  **waits** (`:286`) rather than proposing abort. With a permanently silent rebuilding store this makes a transaction
  whose live voters disagree unable to proceed (flaw F6).
* Every pool in the transaction contributes its own width: a transaction touching two pools tallies each object's
  votes from its own pool's stores (`TransactionDriver.scala:268-289`), so a rebuilding store in the metadata pool also
  constrains transactions that merely touch pool-state objects.
* Designated leader choice skips stores gossiped offline (`client/internal/transaction/DesignatedLeader.scala:22`),
  which is advisory only (`common/network/StoreAvailabilityTracker.scala:18`). A leader chosen on the rebuilding store
  never answers; recovery drivers take over after `Host.txHeartbeatTimeout` (5 heartbeats, `server/Host.scala:60`).
* Reads: the read driver needs `consistentRestoreThreshold` consistent responses (`IDA.scala:103,243,419,603`:
  majority for Replication, `restoreThreshold` for the Reed-Solomon variants). With a rebuilding store absent the pool
  must have that many live Active stores for any read to complete; this is also the rebuild's own read requirement.
* Admission: the shutdown of another device is refused when it would leave a pool with fewer writable stores than its
  `writeThreshold`; a Rebuilding store does not count as writable (`AspenClient.scala:644-`).

Nothing special is done to keep a rebuilding store from being re-counted as a quorum member after it loads: once
loaded and Active it participates fully with blank acceptor state and a possibly stale copy of objects changed since
the walk read them.

---

## 7. Allocation tree as the work list

The walk visits every `(ObjectId -> ObjectPointer)` entry of `pool.allocationTree`. The tree is the **only**
enumeration of a pool's objects (`client/internal/pool/SimpleStoragePool.scala:34`); ObjectPointers contain the
store-pointer bytes needed by FSIF/RocksDB stores.

How the tree is maintained (this defines what the walk can see):

* Allocation: the allocating transaction carries an `AllocationFinalizationAction` (`client/ObjectAllocator.scala:31,38,
  57,82`). After the transaction commits, the action runs a **second transaction** that `allocationTree.set`s the entry
  (`client/internal/allocation/AllocationFinalizationAction.scala:23-36`). The object therefore exists before it is
  listed; the listing is eventually consistent, retried until success.
* Deletion: the transaction that sets the refcount to zero carries a `DeletionFinalizationAction`, which later
  `allocationTree.delete`s the entry (`DeletionFinalizationAction.scala:23-33`). The entry can therefore name an object
  that is already gone.
* The bootstrap pool's tree is seeded with the bootstrap objects, including the Radicle
  (`server/store/Bootstrap.scala:133,166`). Tree nodes are themselves objects allocated through pool allocators and so
  recorded in the same way (metadata layout: an ordinary pool's trees live in a metadata pool, the metadata pools' trees
  in the bootstrap pool, the bootstrap pool's in itself).
* Walk and concurrent modification: `TierEngine.walk` reads each right neighbour on demand. A split moves entries to a
  new right node, so a walk that already read the old node saw them all. A join is performed by the **emptied left**
  node absorbing its right neighbour, deleting the neighbour in the same transaction with its version bumped
  (`client/tkvl/CLAUDE.md`; `ListBatch`), so a walk that follows a stale link to the deleted node gets `InvalidObject` and
  the pass **fails** (`readRight` failure fails the walk, `TierEngine.scala:158`) rather than skipping entries
  silently. I found no path where a join makes the walk skip live entries without an error (analysis, not a test;
  see F5 for the cost).

---

## 8. Missed updates and deletes during the rebuild

### 8.1 Marking

`txd.allDataStores` is every store of every pool the transaction references, by IDA width
(`common/transaction/TransactionDescription.scala:103`); a Rebuilding store is a member like any other. When a store
does not report a commit within `errorTimeout` (2 s, `client/internal/transaction/MissedUpdateFinalizationAction.scala:138`),
`markMissedUpdates` (`:69-`) writes, for **every object the transaction changes on that store's pool**
(`txd.allHostedObjects`, minus lock-only objects), a `MissedUpdate(store, objectId, startTimestamp, storePointer)` into
the pool's ErrorTree: key `errorKey(store, objectId)` = `poolIndex || objectId` (17 bytes,
`client/internal/transaction/MissedUpdateMarker.scala:30`), value = timestamp + the object's store-pointer bytes
(`MissedUpdateFinalizationAction.encodeErrorEntry`). The writes are batched one transaction per pool at a time
(`MissedUpdateMarker.scala:46-`). A store that never voted gets no TracelessCommits entry, only the ErrorTree mark
(`MissedUpdateFinalizationAction.scala:89-100`). Deletes are covered by the same mark: the deletion
transaction (a refcount update) is a change to a hosted object, and the entry preserves the storePointer, which is the
only copy once the allocation-tree entry has been deleted (`MissedUpdateFinalizationAction.scala:62-68`).

### 8.2 Processing

Entries are processed only for **repairable** stores: served, and neither offline, rebuilding nor transferring out
(`server/host/HostStoreView.scala:27`, `server/repair/RepairService.scala:115`, `StoreRepairer.scala:143`). So the
entries written during the rebuild are untouched until the store is loaded. A newly admitted store is first scanned at a
random offset within the scan floor (30 s default, `server/repair/ScanState.scala:30`, `RepairPolicy.scala:44`). The
scan (`StoreRepairer.scan` `:121`) walks the store's ErrorTree range then its TracelessCommits range:

* Allocation-tree entry present: reconcile the store's copy with a full read (snapshot the store, read, install only
  what still matches the snapshot; refcount by higher serial; data by revision when the read's timestamp is at least the
  store's; keys/parts reported locked are deferred) and delete the entry if complete (`repairErrorEntry` `:149-`).
* Allocation-tree entry absent: if the ErrorTree entry is older than `minErrorEntryAgeForDeletion` (60 s,
  `RepairPolicy.scala:46`) it is treated as a deletion and `repairDelete` removes the slice using the saved
  storePointer; younger entries are deferred to a later pass (covers an allocation whose tree entry has not landed yet,
  `StoreRepairer.scala:184-201,380-402`).

This is the mechanism that closes the "residual staleness" gap described in
`docs/superpowers/specs/2026-09-01-failed-store-rebuild-design.md` section 5 (which says only repair-on-read heals it);
the design document predates the ErrorTree.

### 8.3 Known coverage limits

* Tracking is disabled for an ErrorTree write only when that transaction references the pool the entry is about
  (`MissedUpdateFinalizationAction.limitTracking` `:130`, wired at `MissedUpdateMarker.scala:164`). For the bootstrap
  pool, whose trees live in itself, misses of writes to its own ErrorTree and TracelessCommits nodes are never recorded
  (F8).
* A batch that "gave up" (`UnknownStoragePool`, `TransactionCreationError` from `onFail`,
  `MissedUpdateMarker.scala:148-157`) is logged and the entries are lost (F9).

---

## 9. IDA threshold and insufficient peers

* The rebuild has no admission check. It starts regardless of how many peers of the pool are Active
  (`StoreRebuilds.scala:167-169`).
* Each object restore needs the read quorum of the pool's IDA from peers other than the store being rebuilt: majority
  for Replication (`IDA.scala:243`), `restoreThreshold` for the Reed-Solomon variants. The `rebuildWrite` input is the
  *decoded and re-encoded* object, so the rebuilt slice is exactly what a normal encode would produce.
* If too few peers are readable, every read fails: `recordFailure` accumulates up to 10,000 ids, then aborts with the
  checkpoint intact (`RebuildingStore.scala:360-381`); for a smaller pool the walk finishes and `retryFailures` fails
  the pass ("incomplete", `:442-452`). In both cases the staging copy and checkpoint stay, the Rebuilding entry stays,
  and the next device check (hourly) resumes. There is no escalation, no give-up and no operator override that
  accepts the loss of unreadable objects; the store stays Rebuilding (F15).
* Two stores of one pool rebuilding at once need the *other* stores' reads; with Replication(3,2) they deadlock each
  other (each is the other's missing peer), see scenario FS6.
* The comment in `RebuildingStore.scala:233-235` records that a read that never returns can stall the walk and hold a
  rebuild slot; with two slots per host, two such reads stop all rebuilds on that host (F19).

---

## 10. Crash and resumption matrix

| Crash point | State on restart | Behaviour |
|---|---|---|
| after marker written, before entry flipped | marker, entry Active | device load puts store offline (`StoreLoader.scala:73`); reconcile R1 flips; proceeds |
| after R1, before `dropStore` completes | marker, entry Rebuilding, entries partly in CRL | `discardLostStore` runs `dropStore` again (idempotent read-then-delete) |
| after `dropStore`, before rename | marker, no entries | rename+delete runs; rebuild starts |
| after rename, before delete finishes | `discarded/` exists | `loadOwnedDevice` deletes `discarded/` (`DeviceDiscovery.scala:549-552`) |
| mid-walk | staging + checkpoint (<= 1,000 objects behind) | `runPass` resumes from `lastRestoredKey` inclusive (`:193-198`); lost work re-done; `rebuildWrite` is an overwrite |
| between flush and checkpoint save | staging, previous checkpoint | resume from the older key; safe (flush-before-checkpoint, B4) |
| torn checkpoint | `.tmp` or unreadable yaml | `load` returns None: full re-walk, safe (`RebuildState.scala:107-122`) |
| inside `finish` after checkpoint removal, before move | staging, no checkpoint | full re-walk (wasteful, safe) |
| after move, before dir sync | staging or final (rename may revert) | either re-walk or "final exists" path |
| after move+sync, before tree flip | final path present, entry Rebuilding, no markers | **the device load serves the store** (`tryLoadStore` has no check against the tree, `StoreLoader.scala:67-97`); `start()` later sees final exists and flips (F4) |
| after flip committed, before `loadStoreById` | final path, entry Active | normal load at restart |
| flip transaction fails permanently | final path, entry Rebuilding | logged; `release()` and re-check of all devices including this one (`StoreRebuilds.scala:263-276`) re-enter `start()` which flips again (spin bounded only by the device lookup, F11) |
| device shutdown mid-walk | staging + checkpoint, entry Rebuilding | `cancelFor` latches `RebuildCancelled` (`StoreRebuilds.scala:53`), pass fails, resumes after restart or adoption |
| host crash with device adopted elsewhere | staging + checkpoint on the moved disk | new owner's reconcile sees Rebuilding entry (adopt leaves it, `DeviceMetadata.scala:471-`) and `startRebuild` resumes |

---

## 11. Invariants

Each is a property the code is intended to maintain and is checked by reading; "holds" / "does not hold" is my finding.

* **B1 No serving while rebuilding.** The staging backend is never reachable by the network, the router or a `Store`.
  Holds (`RebuildingStore.scala:19-22`; `DeviceDiscovery.scala:255-259`). The final directory becomes loadable the
  moment `os.move` completes, before the tree decision, which weakens B1 across a crash (F4).
* **B2 Marker before destruction.** A store directory is cleared only after its `rebuild-required` marker is durable
  and its device-CRL entries have been durably dropped. Holds (`StoreRebuilds.scala:493-521,544`;
  `DurableFiles.write`).
* **B3 No load of a marked store.** `tryLoadStore` checks the marker first. Holds (`StoreLoader.scala:73`).
* **B4 Flush before checkpoint.** The checkpoint never names an object that has not been flushed. Holds
  (`RebuildingStore.scala:383-387`; trace test hook `:88`).
* **B5 Resume is inclusive and idempotent.** Re-restoring an object is an overwrite. Holds (`:193-198`).
* **B6 Sequential, order-preserving walk.** `lastRestoredKey` is only advanced for objects already written because the
  walk awaits each `fn` before the next. Holds (`TierEngine.scala:135-153`).
* **B7 Promotion is atomic and complete.** The final path appears only by renaming the staging directory after the abort
  latch is re-checked and the checkpoint removed. Holds modulo the crash windows in section 10.
* **B8 Load follows the metadata decision.** In the normal (no-crash) flow the store is loaded only after
  `markRebuiltStoreActive` returned `Adopt`. Holds in-process (`StoreRebuilds.scala:224-236`); the crash window in B1
  is the exception.
* **B9 Metadata flips are guarded.** Every tree-entry change in this subsystem is a transaction carrying a
  `KeyRevision` on the device state key. Holds (`StoreRebuilds.scala:464-467`, `DeviceMetadata.scala:408`,
  `FailedStorageDeviceDurableTask.scala`).
* **B10 Selection precedes repointing.** The destination is chosen from the pool as it stands before the failed device
  is replaced. Holds (`FailedStorageDeviceDurableTask.scala:345-353`).
* **B11 Tombstone is one-way and the work list is the tombstone.** Holds (`:186`, `nextStore` `:251`).
* **B12 Every transaction that touches the pool during a rebuild marks the store.** Holds for ordinary pools; does not
  hold for ErrorTree/TracelessCommits nodes of the bootstrap pool (F8) or when the marker batch gives up (F9).
* **B13 Missed-update processing is deferred until the store is repairable.** Holds (`HostStoreView.scala:27`).
* **B14 Rebuild never writes to peers.** Holds (read path only).
* **B15 A rebuilt copy is discarded if its device is Failed.** Holds (`DeviceMetadata.scala:395`).
* **B16 Rebuilding survives shutdown, adoption and restart.** Holds (`DeviceShutdown.scala:146`; `adoptDevice` comment
  `:462-468`).
* **B17 The failed device's stores are not served after the tombstone.** **Does not hold** (F1): the tombstone changes
  only metadata; a loaded or reloaded device on its old host is not unloaded.
* **B18 A rebuild needs a feasibility check against the pool's IDA before the old state is destroyed.** **Does not
  hold** (section 4.3, F2).
* **B19 Loss declaration is validated against redundancy.** **Does not hold**; see
  [systemic-loss-protection.md](systemic-loss-protection.md).

---

## 12. Failure scenarios

For each: outcome, and why it is safe or whether it is not.

**FS1 Crash mid-walk.** The staging backend is recovered (RocksDB WAL or FSIF journal); the checkpoint gives the last
flushed key; restart resumes there. Objects between the checkpoint and the crash are re-read. Safe: B4, B5. The
`failed` list persisted in the checkpoint is merged back (`RebuildingStore.scala:175-181`). If staging recovery throws
`BackendStateLost` it is discarded with the checkpoint and the pass restarts from the beginning (`:224-228`).

**FS2 Source peer fails mid-walk.** The client read driver treats a missing peer as any other: it needs the IDA read
quorum from whoever is up. If quorum still exists, nothing is visible beyond latency. If not, the read stays pending or
fails; failures are recorded and retried at the end of the pass. A read that never completes stalls the sequential walk
and holds the rebuild slot (known gap noted at `RebuildingStore.scala:234-235`, F19). Safe (no wrong data written),
but liveness depends on the peers returning.

**FS3 A second store of the same pool is lost during the rebuild.** The surviving read quorum shrinks. Replication(3,2):
the other two stores are the rebuilding one (silent) and one live store; majority read (2) cannot be met; all restores
fail; the pool is also unwritable (writeThreshold 2) and every transaction on it stalls (F6). If the second store is
also being rebuilt, each rebuild is the other's missing peer (deadlock until a store with data returns). For
Reed-Solomon pools the margin is `width - restoreThreshold`. The system cannot restore even data that survives on one
store (consistent read needs the majority). Not safe in the availability sense; no wrong data is produced, the stores
stay Rebuilding.

**FS4 Transactions committing to objects already rebuilt.** The store is silent for the transaction, so after 2 s the
finalizer writes ErrorTree entries for every object the transaction changed on that store
(`MissedUpdateFinalizationAction.scala:69-`). The staged copy has the pre-transaction revision. After promotion the
repair scan reconciles it (`StoreRepairer.repairErrorEntry`), installing the newer state or detecting that the staged
copy is already current. Safe subject to B12 (F8, F9 break it).

**FS5 Transactions committing to objects not yet rebuilt.** The walk reads the object later at its newer state; the
ErrorTree entry is still written (the store was silent) and the repair finds nothing to install and removes it. Safe.

**FS6 Two stores of one pool rebuilding concurrently.** See FS3. With Reed-Solomon `k`-of-`n` and `n-k >= 2` both can
complete. Each store's walk reads the other's pre-rebuild state as missing, so correctness is unaffected; both rely on
ErrorTree repair afterwards. Concurrency is bounded at 2 per host (not host-wide across the cluster).

**FS7 Object deleted after it was rebuilt but before the ErrorTree is processed.** The deletion transaction marks the
store's slice (the refcount update is a hosted object change, `allHostedObjects`) with the saved storePointer. The
deletion's finalization removes the allocation-tree entry. After promotion the scan finds the ErrorTree entry, sees the
object absent from the allocation tree and, once the entry is older than 60 s, calls `repairDelete` with the stored
storePointer (`StoreRepairer.scala:184-201`). FSIF `startRepairDelete` only frees the slot if the slot still maps this
object id (`server/store/backend/fsif/FsifBackend.scala:1478`), so slot reuse by a later allocation is not clobbered.
Safe, with a window in which the stale slice exists but no pointer can reach it. If the mark was lost (F9) the slice
leaks (space only).

**FS8 Object deleted after the walk saw its key but before the read.** The read fails (deleted object); recorded as a
failure; `retryFailures` re-reads the allocation-tree entry, which is by then absent (deletion finalization landed) and
the object is skipped (`RebuildingStore.scala:433-435`). If the deletion finalization has not landed yet, the entry is
present, the read fails again, the pass ends "incomplete" and is retried one device check later (an hour). Safe but slow.

**FS9 Allocation racing the walk.** (a) The object's tree entry lands after the walk passed its position: the walk never
sees it. The allocating transaction was one the rebuilding store did not vote in, so it was marked in the ErrorTree
(the object is a hosted object of the transaction); after promotion the scan finds the allocation-tree entry and installs
the slice. While the tree entry has not landed, a young ErrorTree entry is deferred, not deleted
(`StoreRepairer.scala:184-201,380-402`). Safe. (b) The entry lands before the walk reaches it: the walk restores it at
its then current state; ErrorTree repair completes any later change. Safe. (c) The allocation committed **before** the
store was lost, with the store having acknowledged it, and its tree entry lands after the walk passed that key: there is
no ErrorTree mark (the store reported success), the walk skipped it, and the slice is never rebuilt (F10, low
probability).

**FS10 Tree restructuring during the walk.** Splits are benign (entries move to the right, the walk's read node already
held them). Joins and tail removals delete nodes with a version bump; a stale link read raises `InvalidObject` and the
pass fails and resumes at the next device check from the checkpoint (`TierEngine.scala:158`). No silent skip found.
Under heavy delete churn the pass can fail repeatedly; each retry waits for the next device check (F5).

**FS11 Old device comes back after the rebuild started elsewhere (stale owner, data resurrection).**
(i) *Same host, operator declared the device failed*: the tombstone zeroes the ids in the tree but the host decides
whether to load a device from local files; `owning-host` names this host, so `tryLoadDevice` loads it without reading
the tree (`DeviceDiscovery.scala:474-478`), `tryLoadStore` loads its stores without consulting the pool entry
(`StoreLoader.scala:67-97`), and the reconcile for a Failed device only logs (`DeviceReconciler.scala:123-127`). A
running host is never told to unload it either. The old store therefore serves under the same StoreId while the
pool names the rebuilt replacement: two live replicas of one acceptor index, and the zombie's repair scans consume
ErrorTree entries meant for the replacement (F1). Not safe.
(ii) *Device carried to another host*: `owning-host` names the old host, so the new host looks the device up
(`resolveOwnership` `:145-159`) and `handleOwnershipResolved` refuses a Failed device (`:219-220`). Safe.
(iii) *Rebuild started elsewhere for a non-Failed reason*: not possible; only the operator reassigns.
(iv) *Stale rebuild completion*: a rebuild already running on the old device when it is tombstoned completes and the
`markRebuiltStoreActive` transaction returns `Discard` for a Failed device (`DeviceMetadata.scala:395`), deleting the
copy. Safe.

**FS12 Two hosts both rebuilding the same store.** Possible sources: (a) adoption mid-rebuild: the old host's rebuild
is cancelled by `relinquishDevice` (`DeviceShutdown.scala:242`) or lost with the crash; the new owner resumes from the
checkpoint on the physical disk; if the old host finishes first its completion gets `NotOwned` and leaves the copy
(`StoreRebuilds.scala:253-261`). (b) T12 repointing a store that already has an in-place rebuild running: the
destination host builds a fresh copy; the old host's pass completes and is discarded because the old device is Failed.
(c) Two concurrent `fail-storage-device` calls: both enroll, idempotent; `moveStore` carries a `KeyRevision` on the
tombstone and pool state, the loser re-reads and sees `StoreAlreadyMoved` (`FailedStorageDeviceDurableTask.scala:404`).
(d) `rebuildingStores` is per host, so the guard against double start is host-local; across hosts the guard is the
tree entry plus the `KeyRevision` flip. Safe, under the assumption in (b) that the old host really stops (F1 shows it
need not).

**FS13 Promotion crash.** See section 10 rows. The one row that changes behaviour is "after move, before the flip": the
store is loaded and served while the tree says Rebuilding (F4).

**FS14 Destination device runs out of space.** `isOutOfSpace` latches an abort, the pass fails, staging and checkpoint
stay, the entry stays Rebuilding "until capacity is added" (`RebuildingStore.scala:288-294`). There is no
re-placement; the failed-device task only places once (F12). Safe, not live.

**FS15 Destination device shut down or failed mid-rebuild.** Shutdown: cancel, resume later. Failed: copy discarded and
the drain moves the Rebuilding entry again from the new tombstone. Safe.

**FS16 The allocation tree's own metadata pool has lost stores.** The walk reads the pool's allocation tree through the
metadata pool; rebuilding any store therefore requires the metadata pool, and the bootstrap pool for the metadata pool's
trees and the Radicle, to be readable. Rebuilding a bootstrap-pool store reads the bootstrap pool's own tree and the
pool state from the remaining bootstrap stores. A loss of enough bootstrap stores makes every rebuild in the system
impossible (and every client start fail). No code mitigates this; it motivates [systemic-loss-protection.md](systemic-loss-protection.md).

**FS17 Crash while discarding the old directory.** See section 10. Safe: marker first, drop second, rename third.
Destructive and irreversible (section 4.3).

**FS18 Pool cannot supply the data (permanent loss of an object).** The object stays in the failed list forever; the
pass ends incomplete on every device check; the store never becomes Active (F15).

**FS19 Rebuild finishes while a transfer or shutdown is in flight on the device.** The flip writes Offline on a
shutting-down device (`serviceStatus`) and the store is kept offline behind a marker and an empty save file
(`keepRebuiltStoreOffline` `StoreRebuilds.scala:316`); an adopter classes it Clean (`adoptionClass` `:296`). Safe.

---

## 13. Suspected flaws and open questions

Each item carries a **Verification** paragraph from an independent adversarial pass against the code (verdicts CONFIRMED, PLAUSIBLE, REFUTED, OPEN).

Each item: scenario, reference, confidence.

> **F1 FIXED (2026-11-09)** together with cross-document X1: the reconcile retires a device it reads Failed (a durable
> `failed` marker, then unloaded like a relinquished device), a device with the marker is never loaded, the tombstone
> nudges the former host, and a repair scan runs only while the pool's entry names the scanning host. Residual: until
> its first successful reconcile, a restarted or partition-healed old host may still answer peers holding a stale
> store-to-host mapping (the no-fencing trade-off accepted for adoption).

**F1 (Medium-High) A device declared Failed is never taken out of service on its host; a stale replica of the same
StoreId can run beside the rebuilt one.**
Scenario: the operator runs `fail-storage-device D1` on a device whose host H1 is up and whose media is healthy (a
mistake, a flapping controller, or a network partition that heals). The tombstone zeroes the ids; the drain rebuilds
store S on D2/H2 and repoints the pool. H1's reconcile reads the Failed state and only logs
(`server/host/DeviceReconciler.scala:123-127`); its loaded stores keep serving. If H1 restarts, `tryLoadDevice` trusts
`owning-host` and loads the device without a network check (`server/host/DeviceDiscovery.scala:474-478`);
`tryLoadStore` has no pool-ownership check (`server/host/StoreLoader.scala:67-97`; a repo-wide search for a
`stores(...).hostId` comparison in `server/` finds none). Messages are routed by `(HostId, StoreId)`; a peer with a
stale mapping, or any gossip lag, reaches H1's S. The pool then has two acceptors for index `i`, defeating the
quorum-intersection argument, and H1's repair service (the zombie is repairable) deletes ErrorTree entries for
index `i` that the rebuilt store needs (`server/repair/StoreRepairer.scala:143`, `deleteEntryIfUnchanged`). The
adoption design states the lack of fencing as a conscious decision for adoption
(`docs/superpowers/specs/2026-11-08-unclean-device-adoption-design.md:25`) but the failed-device design has no such
statement and its anti-goals (`:36-40`) list only adoption of a Failed device.

**Verification (adversarial pass): CONFIRMED mechanism; trigger is an operator `fail-storage-device` (or a partition) on a live, healthy device; severity stale-replica / split-decision risk.** `DeviceReconciler.reconcileDeviceState` tests `remote.isFailed` first (`:123-127`) and only logs, so neither the relinquish path (`:128-129`) nor any unload runs; grep shows `unloadDevice`/`relinquishDevice` have no caller on the Failed path. On restart `tryLoadDevice` loads by `owning-host` with no lookup (`DeviceDiscovery.scala:471-478`), and `tryLoadStore` (`StoreLoader.scala:67-97`) checks only markers. The zombie's store stays in `HostStoreView.repairable` (only offline/rebuilding/transferring-out are removed), so its `StoreRepairer.scan` consumes ErrorTree entries for the shared pool index (`StoreRepairer.scala:142-165`, `deleteEntryIfUnchanged` after its own repair), which the rebuilt copy then never sees. Message routing reaches it whenever a peer or client still maps the StoreId to the old HostId (the envelope check is on HostId, not store ownership). Fix sketch: when the reconcile reads Failed, unload the device and its stores (`relinquishDevice`-style) and delete/keep the `owning-host` marker so a restart does not load it; longer term a per-store epoch on messages.

> **F2 FIXED (2026-11-09).** The old copy and its saved CRL entries are kept in `pending-delete/` until the store
> is Active again (section 4.3). Covered by the DeviceCrlSuite tests "a discarded store is kept in pending-delete with
> its CRL entries until the store is Active again" and "a pending-delete copy of a store rebuilt on another device
> goes once that device has it Active".

**F2 (High) The old store state is destroyed before any check that the pool can supply the replacement.**
Scenario: a transient detection (see systemic-loss-protection.md) marks a store; `startRebuild` runs `discardLostStore`
(`server/host/StoreRebuilds.scala:167-169,499-540`), deleting the backend directory and dropping the CRL entries; the
peers turn out to be below read quorum (a rolling restart is in progress). The rebuild then wedges (section 9) and the
only copy on this store, which may have been recoverable, is gone. Fix direction: stage the old directory aside (see the
companion document) and gate the discard on a pool-health read.

**Verification: CONFIRMED, severity data-loss (only in a correlated-loss situation, but that is exactly what systemic-loss-protection.md is about).** `startRebuild` (`StoreRebuilds.scala:167-169`) calls `discardLostStore` (`:499-520`) which does `crl.dropStore` and then `removeLostStoreDirectory` (rename to `discarded/`, then `os.remove.all`, `:522-540`) with no read of the pool state. Because `rebuildingStores` is filled only after `discardLostStore` returns true, a reconcile pass starts the discard of every marked store on a device regardless of `maxConcurrentRebuilds`. No retention of the old bytes. Fix sketch: keep the discarded directory (and CRL entries or a copy) until the rebuilt store has been promoted, or gate the discard on a pool-health read.

**F3 (Medium-Low) `RebuildingStore.start` treats any directory at the final path as a finished rebuild.**
Scenario: the failed-device drain places store S on D3, where an earlier tenancy of S left a directory (a transferred-away
copy whose delete failed and whose `transferring-out` marker was lost, or an operator copy). `start()` sees the path,
deletes staging and completes (`server/rebuild/RebuildingStore.scala:134-139`); the host flips the entry and loads a
stale store that no ErrorTree entry will ever repair. `discardLostStore` only clears directories that carry the marker
(`StoreRebuilds.scala:501-502`). No identity or freshness check on the directory.

**Verification: PLAUSIBLE, severity stale-replica.** `RebuildingStore.start` (`:134-139`) does treat any existing `finalPath` as complete. Whether the drain can place a store on a device that still has a directory for it depends on leftover directories (`DeviceReconciler` deletes transferred-away directories and retries on failure), so the trigger is a failed cleanup plus a later placement on that device. Fix sketch: the rebuild writes a `rebuild-complete` marker inside the staging dir before the move and `start()` requires it.

**F4 (Medium) A rebuilt store can serve while the tree still says Rebuilding.**
Scenario: crash after `os.move(staging, final)` (`RebuildingStore.scala:467-470`) and before `markRebuiltStoreActive`.
On restart `loadOwnedDevice` offers every store directory to `tryLoadStore` (`DeviceDiscovery.scala:565-571`), which
loads it (no marker). It votes in transactions and answers reads with ErrorTree entries unprocessed (`rebuildingStoreIds`
removes it from the repairable set only once a rebuild object is registered, `HostStoreView.scala:27`) and no
`StoreMoved` gossip. Stale slices vote according to revision checks, so this looks like an ordinary lagging store, but
it differs from B1/B8 and from `RebuildingStore.scala:135-137`'s claim that only a flip is left.

**Verification: PLAUSIBLE, severity stale-replica (low).** After `os.move` (`RebuildingStore.scala:467-470`) a crash before `markRebuiltStoreActive` leaves a plain store directory with no marker; the restart loads it (`DeviceDiscovery.scala:565-571`, `tryLoadStore` has no tree check). It serves and votes with ErrorTree entries unprocessed until the first reconcile finds the tree entry Rebuilding, runs `startRebuild`, and `start()` sees `finalPath` and just flips. It then behaves like any lagging replica, which the revision checks tolerate, and the repairable set (`HostStoreView.of`) includes it once rebuilding is not registered. Also a blank Paxos acceptor (F14). Fix sketch: load a store whose tree entry is Rebuilding as offline until the flip commits.

**F5 (Medium) A walk that fails on a tree read waits for the next hourly device check; under delete churn it may never
finish.**
Scenario: heavy deletion near the walk cursor keeps joining/removing nodes; `readRight` raises `InvalidObject`
(`client/tkvl/TierEngine.scala:158`), the pass fails, `recheckOtherDevices` excludes this device
(`StoreRebuilds.scala:288-302`), the retry occurs after up to `CheckStorageDevicesPeriod` = 1 h
(`cmdline/Main.scala:82`) and re-walks from the last checkpoint (at most 1,000 objects behind). On a single-device
host siblings also wait. No backoff schedule, no in-pass retry of the right-neighbour read.

**Verification: PLAUSIBLE, severity liveness.** `CheckStorageDevicesPeriod` = 1 hour (`cmdline/Main.scala:82`), the failure path deliberately does not re-check its own device (`StoreRebuilds.scala:288-302`), and `readRight` (`TierEngine.scala:158`) can raise on a concurrently joined node. Whether the pass can fail repeatedly depends on delete churn; not executed. Fix sketch: retry the failed read inside the pass with a bounded count and backoff, and schedule a delayed self-recheck with exponential backoff.

**F6 (Medium) A Rebuilding (silent) store can stall transactions whose live voters disagree.**
Scenario: Replication(3,2), S0 rebuilding. A transaction's prepare gets commit from S1 and abort from S2 (S2 lags).
`nReplies = 2 >= writeThreshold`, but `nReplies != width && nCommitVotes (1) < writeThreshold && width - nAbortVotes (2)
>= writeThreshold` sets `cantAttempt` (`server/transaction/TransactionDriver.scala:281-288`); the driver returns and
waits for S0, which is silent for the whole rebuild. `nextRound` runs only on a NACK. The transaction keeps the
objects' locks on S1/S2. The code comment at `:285` itself calls the rule not smart. Sibling analysis belongs in
[paxos-transactions.md](paxos-transactions.md).

**Verification: CONFIRMED (logic read at `TransactionDriver.scala:257-298`), severity liveness.** For Replication(3,2) with one commit vote, one abort vote and the third store silent: `nReplies (2) >= writeThreshold`, `nReplies != width`, `nCommitVotes (1) < writeThreshold`, `width - nAbortVotes (2) >= writeThreshold` set `cantAttempt` and the driver returns; nothing else re-triggers a decision except a NACK or the silent store's vote. Applies to any silent third store (offline as well as rebuilding), and the locks taken by the commit voters remain until it answers, so the object stays blocked for the whole rebuild. Fix sketch: after a bounded wait, propose abort when the prepare quorum reports no accepted value (a legitimate Paxos proposal), so a silent store cannot hold the commit voters' locks.

**F7 (Medium-Low) A rebuild on a new device answers "unknown store" instead of staying silent.**
Scenario: T12 rebuild on host H2. `startRebuild` gossips `StoreOffline` but never calls `router.markOffline`
(`StoreRebuilds.scala:222`; `markOffline` callers are only in lost-CRL paths). `answersUnknown` is true
(`StoreRouter.scala:82-84`), so every read for S gets `StoreNotFound`; the read driver immediately re-sends to S
(`client/internal/read/BaseReadDriver.scala:160-178`) after `dropStoreMapping`; for a bootstrap-pool store each reply
refreshes the bootstrap config (`MetadataManager.scala:281-284`). The loop ends only when the read completes from other
stores, so it is bounded for reads that have a quorum without S. Not incorrect, wasteful and inconsistent with the
in-place path.

**Verification: CONFIRMED (code), severity liveness/efficiency only.** `startRebuild` gossips `StoreOffline` but never calls `router.markOffline` (the callers are all in lost-CRL, transfer and shutdown paths), and `StoreRouter.answersUnknown` (`:82-84`) is true for a store that is neither offline, loading nor awaiting its device. Fix sketch: `router.markOffline(storeId)` in `startRebuild` on the new-device path and `clearOffline` on promotion.

**F8 (Medium) Bootstrap-pool stores rebuilt with untracked stale nodes.**
The bootstrap pool's ErrorTree and TracelessCommits nodes live in the bootstrap pool; writes to them have missed-update
tracking disabled (`MissedUpdateFinalizationAction.limitTracking` `:130`). Scenario: a bootstrap-pool store S is
rebuilding; the miss-recording transactions (which write these nodes) cannot record S's misses of those same nodes. The
promoted S has a stale slice of each such node and no ErrorTree entry; it is only repaired if a read triggers
`OpportunisticRebuild`. Redundancy of those nodes is silently reduced by one after every bootstrap rebuild.

**Verification: CONFIRMED mechanism, severity stale-replica.** `MissedUpdateFinalizationAction.limitTracking` (`:130-133`) disables missed-update tracking for the whole ErrorTree-writing transaction whenever it references the pool the entry describes; only the bootstrap pool's own trees live in the bootstrap pool, so this applies there only. Any bootstrap-pool store that is silent while such a write commits (offline, or rebuilding) gets no entry for those tree nodes, and the walk may already have passed them. The damage is bounded by client reads of those nodes (quorum reads detect the stale slice and OpportunisticRebuild repairs it), so the effect is redundancy silently reduced until read. Fix sketch: run a second pass of the walk over objects of the bootstrap pool's metadata trees after the first, or write the entries for bootstrap-pool nodes in a follow-up transaction that does not reference the pool's own nodes.

**F9 (Medium-Low) The missed-update batch can give up.**
`MissedUpdateMarker.onFail` converts `UnknownStoragePool` and `TransactionCreationError` into `StopRetrying`
(`client/internal/transaction/MissedUpdateMarker.scala:148-157`); the batch's entries are dropped with a warning
(`:133`). A rebuilding store then keeps a stale slice (or a leaked slice of a deleted object) with no record.

**Verification: PLAUSIBLE-low, severity stale-replica.** `MissedUpdateMarker.writeBatch.onFail` (`:143-154`) converts `UnknownStoragePool` (benign: pool deleted) and `TransactionCreationError` ("always permanent" per its comment) into `StopRetrying`; the batch is dropped with the warning at `:133`. Trigger needs a permanent creation error, which was not found to occur in normal operation.

**F10 (Low) A committed allocation whose tree entry lands after the walk passed the key, on a store that had
acknowledged it before being lost, is never restored.**
Needs the AllocationFinalizationAction to be later than the walk's arrival at that key (it can be delayed by retries,
`AllocationFinalizationAction.scala:23-36`). No ErrorTree mark exists for it. Mitigation would be a second pass over
tree entries newer than the walk start.

**Verification: PLAUSIBLE-low, not traced.** The walk reads the allocation tree once in key order; an allocation committed after the cursor passed its key on a store that had acknowledged it earlier is not in the ErrorTree. Plausible by construction; no experiment.

**F11 (Low) Flip failure re-enters `start()` through a recheck that includes the failing device.**
`StoreRebuilds.scala:263-276` calls `recheckAll()` for a permanently failed `markRebuiltStoreActive`; the
walk-failure path explicitly avoids this to prevent spinning (`:288-302`). The device lookup at the head of every
check fails for most of the plausible permanent errors, which stops the loop, so the spin appears bounded in practice;
needs a test with a persistently failing flip that leaves the lookup healthy.

**Verification: PLAUSIBLE-low, not traced end to end.** Code as described at `StoreRebuilds.scala:263-276`; boundedness depends on the device lookup failing for a persistent flip error.

**F12 (Medium) Placement can co-locate stores and cannot be revised.**
`selectRebuildDevice` soft-excludes the pool's other devices (`StorageDeviceSetState.scala:398-412`), so a set with one
other device puts two stores of one pool on it. `requiredSize` falls back to 0 when the usage key is absent
(`:198-205`). After placement the destination is final: ENOSPC leaves the store Rebuilding "until capacity is added"
(`RebuildingStore.scala:288-294`); there is no re-placement and no alarm beyond the log line. In-place rebuilds never
move off suspect media.

**Verification: CONFIRMED (code), severity availability.** `selectRebuildDevice` passes `poolDevices` as `softExclude` and `requiredSize` falls back to 0 (`StorageDeviceSetState.scala:195-205, 398-412`).

**F13 (Low) `adoptDevice` writes rebuild-required markers in the transaction's prepare closure.**
`DeviceMetadata.scala:540-546` runs `markRebuildRequired` for each `crlLost` store before the transaction is known to
commit. A collision retry that then returns `Changed` or `Unclean` (the device's state moved on) leaves markers on a
device this host did not adopt; its rightful owner then reads them as lost stores. Narrow window; destructive.

**Verification: REFUTED as a harm.** The marker is written in the prepare closure (`DeviceMetadata.scala:540-546`) but `crlLostStores(devicePath, state)` is derived from the disk's own contents, so the CRL state really is gone whichever host ends up owning the device; a retry that returns Changed/Unclean leaves markers that are true statements about the disk. The comment at `:541-542` says exactly that. (It is still impure code in a retried closure, doc-only.) Strike from the active list; systemic-loss-protection.md D4 note updated accordingly.

**F14 (Low) The rebuilt store is a blank acceptor.**
Promotion creates a `Frontend` with no transactions (`StoreLoader.scala:304-359`); `TxAccept`/`TxHeartbeat` for
unknown transactions are ignored and a `TxPrepare` creates a fresh `Tx` (`server/store/Frontend.scala:608-650`),
with transactions older than the horizon voting abort (`:655`). Paxos acceptor amnesia is safe only within the pool's
failure tolerance; the rebuild path does not check that no in-flight transaction chose a value with this store's
acceptance. See [paxos-transactions.md](paxos-transactions.md) and [transaction-recovery.md](transaction-recovery.md).

**Verification: OPEN (Paxos-level), PLAUSIBLE only under correlated loss.** Rebuilt stores are blank acceptors (`StoreLoader.scala:304-359`). Safe in practice because rebuilds take far longer than the lifetime of any transaction in flight when the state was lost and old prepares vote `TransactionTooOld` (`Frontend.scala:655`); not a proof.

**F15 (Medium) No way to finish a rebuild past a permanently unreadable object.**
`retryFailures` fails the pass while any object is unreadable (`RebuildingStore.scala:442-452`); a stale
allocation-tree entry for a vanished object, or a genuinely lost object, keeps the store Rebuilding forever and the
pool permanently one store short. There is no "accept loss" decision, escalation or counter.

**Verification: CONFIRMED, severity liveness (store stuck in Rebuilding).** `retryFailures` (`RebuildingStore.scala:442-452`) fails the pass while any object is unreadable; there is no skip/accept-loss path. Fix sketch: an operator command that records the objects as lost (ErrorTree entries) and lets the rebuild finish.

**F16 (Open question) Is `minErrorEntryAgeForDeletion` (60 s) long enough for the rebuild case?** A store offline for
hours needs entries for deletions to be older than the cap on its allocation-tree race, which they are; but the
allocation-tree entry of a *recreated* ObjectId cannot occur (UUIDs). Question closed unless deletion finalization can
be delayed beyond 60 s while the ErrorTree entry was written first (it is written 2 s after commit, the deletion
finalization runs at commit): a delayed deletion finalization (retries) with an older-than-60 s ErrorTree entry would
cause `repairDelete` of a live-looking object only if the tree entry is absent, i.e. not live. Not a flaw as far as
traced; listed so a reviewer can check the reasoning.

**Verification: Not a flaw (agreed with the document).**

**F17 (Open question) Walk coverage vs. objects absent from the allocation tree by construction.** Bootstrap seeds the
tree with its objects (`Bootstrap.scala:133,166`); every other allocation passes through `ObjectAllocator`. Objects
created by other paths (tests, FSIF bitmap preparation `FsifIndexAllocationStrategy.prepareBitmap`, pool creation) were
not each verified to carry an `AllocationFinalizationAction`; a missing one means a rebuild never restores the object.

**Verification: OPEN, not verified; belongs with the allocation-tree audit.**

**F18 (Medium-Low) FSIF slot reuse versus rebuild writes.** FSIF rebuild writes into the slot named by the pointer's
storePointer (`FsifBackend.scala:390-415`). An object deleted after the walk read it, whose slot is reallocated to a new
object, leaves the ErrorTree to reconcile two entries for the same slot (a repair-delete of the old id, a repair-update
of the new one). `startRepairDelete` is id-checked (`:1478`); whether the repair-update install over a slot still holding
the old id succeeds was not traced. See [repair-process.md](repair-process.md).

**Verification: OPEN, not verified (FSIF slot reuse); see repair-process.md.**

**New (verification pass) N1 (Medium) Replay of a recovered commit vote can regress a newer blind write.** A store that crashes between a transaction's backend commit and its CRL record deletion recovers the record and re-applies it; revision requirements stop the replay but blind KV inserts, Exists-guarded deletes and ABA refcounts are applied over newer committed values. Details and fix in crash-recovery-log.md F13. Relevant here because a rebuilt or adopted store recovers through the same path.

**New N2. Crash-recovery-log F2/F4 can raise `BrokenEntryChain` at a restart, which marks every store of the device rebuild-required and so feeds this process (see crash-recovery-log.md F2, Exp3).**
