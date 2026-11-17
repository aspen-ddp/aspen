# Systemic Loss Protection (problem statement only)

Paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`.

> **No solution to the problem described here has been designed.** The work is tracked in `Backlog.txt`, "System-level
> protection against correlated 'lost store' declarations" (it carries the requirements S11-S15 below). This
> document states the problem and inventories every code path that can automatically declare a store lost or
> destroy store state. It classifies each path as destructive or recoverable, describes what the current code
> retains and for how long, and lists candidate directions as ideas only. Nothing in the current code detects or
> limits a correlated burst of declarations.

Related: [rebuild-process.md](rebuild-process.md) (the mechanism a false declaration sets off),
[crash-recovery-log.md](crash-recovery-log.md) (the CRL, whose failure to open is the widest trigger),
[repair-process.md](repair-process.md), [paxos-transactions.md](paxos-transactions.md),
[transaction-recovery.md](transaction-recovery.md), [transaction-requirements.md](transaction-requirements.md),
and `server/store/backend/fsif/DESIGN.md` section 4 ("Automatic lost store", the FSIF-specific triggers).

---

## 1. The problem

Aspen treats a store as expendable when its local state cannot be trusted. It takes the store out of service and
rebuilds it from the rest of its pool. That is the right response to a failed disk, and it is cheap for one store
at a time, because the pool tolerates the loss of some stores (`IDA.failureTolerance = width - writeThreshold`;
reads need `IDA.consistentRestoreThreshold`, `common/ida/IDA.scala`).

The mechanism is safe only if declarations of loss are **independent** and **true**. A *software* fault can make the
detector itself wrong, and then it is wrong on every node running the same code at the same moment. For example, a
release that misreads a valid CRL makes every device's CRL look corrupt. On one device that is a normal rebuild. In a
rolling upgrade it marks every store on every upgraded device lost. Each store is taken out of service at once
(section 3, S4), and each rebuild then needs a read quorum of peers that were themselves just taken out of service.
Whole pools become unreadable, including the metadata pools and the bootstrap pool that every rebuild and every flip
to Rebuilding depends on.

The current code retains what it takes out of service (section 4.4). A false burst therefore causes a cluster-wide
outage with the old copies still on disk, but not immediate data loss. There are three gaps. Nothing stops the burst
(S8, S9). There is no tool or procedure for putting retained copies back. And a few exits destroy the retained
copies or their transaction state (section 4.3, the destructive rows, and the open weaknesses).

Why this is hard here:

* **The detector and the repairer are the same code.** The evidence for a declaration is "my local CRL, save file or
  backend does not look right". Locally, a software bug in reading those files looks the same as corruption.
* **Declarations are local and unilateral.** No declaration site consults the pool, the IDA, other stores or other
  hosts (S1).
* **Rebuild needs healthy peers**, and the peers are exposed to the same fault.
* **Rollouts correlate faults in time.** Detection runs at host start, at device load and adoption, at store load,
  and at every commit (fail-stop). A rolling upgrade exercises it on each host in turn, faster than rebuilds complete.

---

## 2. What is at stake: the arithmetic

Let a pool have width `w`, write threshold `wt` and consistent read threshold `kc` (`IDA.consistentRestoreThreshold`:
`w/2+1` for Replication, `restoreThreshold` for ReedSolomon and RandomAccessReedSolomon). Let `d` be the number of
stores of **one pool** that are out of service at the same time because they were declared lost and not yet rebuilt.
A store is out of service from the moment it is marked: a marked directory is never loaded (S4).

* Writes stall once `d > w - wt`: Paxos needs `wt` stores (`TransactionDriver`'s `Proposer`/`Learner` use
  `ida.writeThreshold`).
* Reads fail once `d > w - kc`, including for objects whose slices survive on some live store: a consistent read
  needs `kc` stores that agree on the revision.
* A rebuild reads each object through the normal read path, so it needs `w - d >= kc`. Before its walk it also asks
  the pool's other stores for pending allocations and waits for `w - wt + 1` answers (`RebuildingStore`'s
  `queryPeers`). That count never exceeds `kc`, so `kc` is the binding constraint. Peers that are themselves being
  rebuilt are silent and do not answer.
* The same rule applies to the **metadata pool** that holds a pool's AllocationTree, ErrorTree and UnackedCommits
  tree, and to the **bootstrap pool**, which holds the metadata pools' trees, the pool state objects, the
  storage-device state objects and the Radicle. A rebuild reads its pool's state and walks the pool's
  AllocationTree. The flip to Rebuilding (`StoreRebuilds.requestRebuild`) and every device reconcile read the
  device's state object in the bootstrap pool. If the bootstrap pool falls below `kc`, no flip, rebuild or reconcile
  can run anywhere, and clients cannot start.

A burst of declarations correlated across hosts therefore breaks these inequalities for most pools at once. The
rebuild concurrency cap (`Host` `maxConcurrentRebuilds`, default 2 per host) bounds rebuild work. It does not bound
`d`, because a store leaves service when it is marked, not when its rebuild slot is taken.

---

## 3. Observed properties of the current system

These are facts about the code at HEAD. They define the problem; none of them is a goal.

* **S1 Declarations are local and consult no redundancy information.** Every site in section 4.1 decides from files
  on one disk, or from one store's own failure. None of them reads the pool state, the IDA, other devices' state or
  other hosts. The flip (`requestRebuild`) checks only that the device is not Failed, is owned by this host, and has
  the entry Active.
* **S2 One decision can flag a whole device.** `DeviceDiscovery.openDeviceCrl` marks every store directory of a device
  after one failed CRL open. A host restart reopens every device's CRL, so it can do this to every device of the host
  in one event.
* **S3 Pacing applies to rebuild work, per host.** `StoreRebuilds.startRebuild` admits a store only while
  `rebuildingStores.size + droppingCrlState.size < maxConcurrentRebuilds`. A store holds its slot from the start of
  its discard (`discardLostStore`, which saves and drops its CRL entries) and passes it straight to its rebuild in the
  same callback. So at most `maxConcurrentRebuilds` stores per host are being discarded or rebuilt at once. The cap
  is per host, not per pool, and nothing coordinates it across hosts.
* **S4 Removal from service is immediate; destruction is deferred.** A marked store is never served:
  `StoreLoader.tryLoadStore` leaves a directory with `rebuild-required` offline, and every runtime declaration
  unloads the store before or as it marks it. The discard renames the directory, with its device-CRL entries saved
  into `lost-crl-state.log`, into `<device>/pending-delete/`. The copy is deleted only once the store is Active again
  (section 4.4). Nothing checks whether the pool can supply the rebuild before the store is taken out of service.
* **S5 Errors are classified coarsely.** The CRL open distinguishes four cases. `CrlLocked` (another writer) is never
  lost. `CrlSizingMismatch` refuses the device and never declares it lost. A transient error (`IOException`,
  `UncheckedIOException` or `VirtualMachineError`, `DeviceDiscovery.isTransient`) is retried on later scans, up to
  `CrlOpenAttempts` (3) in a row; the count is kept in memory, so it resets at restart. Anything else, including
  `CrlCorrupt` and `Recovery.BrokenEntryChain`, declares at once. Elsewhere there is no classification:
  `CrashRecoveryLog.loadValidStoreState` turns any `Throwable` into `InvalidSaveFile`. A failed CRL read or recovery
  of one store fail-stops it. So do a commit voter whose apply throws, and a recovered commit vote whose object fails
  to load with anything but ObjectNotFound (`Frontend.failStop`, from `Tx`). Every FSIF error other than ENOSPC in a
  write, force, checkpoint or compaction, and any internal exception in its serial executor, fail-stops the backend.
  Persistent and version causes are handled the same way as damage.
* **S6 Configuration cannot change an existing CRL's sizing.** A CRL records its sizing in `crl/sizing` when it is
  created and always opens with it (`SimpleCRL`). The device config keys `crl-num-streams` and
  `crl-max-file-size-mb` are required. Stream files that disagree with the recorded sizing throw
  `CrlSizingMismatch`, which refuses the device. A changed default or config loader cannot turn a CRL into a lost
  one. A changed CRL *format* or recovery check still can (SC2).
* **S7 Retained artefacts are never read by code.** `crl.lost-<uuid>` (D2) is kept forever: no code reads, expires
  or references it. A `pending-delete/` copy is deleted automatically (section 4.4) but is never reopened. No tool
  reinstates a retained copy, and there is no documented procedure for doing it by hand.
* **S8 There is no brake and no escalation.** No declaration site has a counter, threshold, circuit breaker,
  operator confirmation or alarm, other than log lines.
* **S9 Every host runs the same detection at the same lifecycle events** (start, device load, adoption, store load,
  close, every commit), with no staggering and no version awareness.
* **S10 `fail-storage-device` is unguarded by design.** `failStorageDevice` checks only that the device is not
  already Failed, then enrolls `FailedStorageDeviceDurableTask`. That task writes the tombstone (ids zeroed, device
  removed from its set and host) and moves every store to Rebuilding elsewhere. The command is for a drive that has
  physically failed and whose data is already gone. A pool-health guard could save nothing there and would only delay
  rebuilding the data elsewhere, so it has none. A device that may still hold its data goes through
  `shutdownStorageDevice`, which refuses when a pool would drop below its write threshold.

### Protections that would be needed (tracked in Backlog.txt)

* **S11** A store is taken out of service, or its retained state destroyed, only after it is shown that the rest of
  the pool can reconstruct it.
* **S12** The number of stores of any one pool (and of the metadata and bootstrap pools) that are out of service for
  a declaration at the same time is bounded below the pool's tolerance, and the bound is enforced across hosts.
* **S13** Transient, configuration and version conditions are never treated as corruption.
* **S14** Discarded state stays recoverable for a retention period that is long compared with detection and rollout
  times, and is released only by an explicit decision.
* **S15** A burst of declarations (across stores, devices or hosts) is detected as systemic and halts automatic
  declaration and destruction until an operator confirms.

The current code satisfies S14 only partly: copies are retained until the rebuild succeeds, which is not a retention
period, and there are exceptions (section 4.4). It satisfies none of the others.

---

## 4. Inventory: every path that can declare a store lost or discard state

### 4.1 Declarations (decide that a store is lost)

Each declaration writes the durable `rebuild-required` marker (`StoreRebuilds.writeRebuildRequiredMarker`) into the
store directory and leaves the store offline.

| ID | Trigger | Code | Immediate local effect | False positives |
|---|---|---|---|---|
| D1 | `<device>/crl` absent from a device that holds store directories | `DeviceDiscovery.openDeviceCrl` | marker on **every** store directory of the device; fresh empty CRL created | a partial restore or mount; a path or layout change in a release |
| D2 | CRL open throws a non-transient error (`CrlCorrupt`, `BrokenEntryChain`, anything not `isTransient`), or a transient one `CrlOpenAttempts` times in a row, on a device that holds stores | `DeviceDiscovery.openDeviceCrl` | as D1, and `crl/` renamed to `crl.lost-<uuid>` | a CRL format or recovery change; a bug in the CRL writer that the next recovery rejects; a persistent I/O condition (EMFILE in a restart storm, a flaky controller) lasting three scans |
| D3 | `crl_save.log` present but `loadValidStoreState` rejects it (truncated, unparsable, another store's, any `Throwable`) | `StoreLoader.handleLoad` then `StoreRebuilds.crlSaveFileLost`/`retireUnserved` | closed unserved, marked, offline, device checked | a save-file format change: every store whose directory holds a save file. A crash between the save and `deleteTransactions` leaves the same entries in the device CRL, and the discard saves those |
| D4 | Adoption: an Offline or Active store whose directory has `transferring-out` without a valid save file, or has `rebuild-required` | `DeviceDiscovery.adoptionClass` (`CrlLost`); `DeviceMetadata.adoptDevice` writes the marker in its prepare closure and the entry Rebuilding in the adoption transaction | adopted as Rebuilding | a save-file read path that fails on the adopting host's version; it inherits any false mark the previous host wrote. A marker left by an adoption transaction that does not commit states a fact about the disk and is harmless |
| D5 | `BackendStateLost` at backend open: FSIF `StoreLost` (missing directory or blank files, no valid index, journal sequence or corruption checks, replay bounds; `FsifRecovery`, fsif/DESIGN.md 4.1), RocksDB `RocksDBStateLost` (`CURRENT` missing) | `StoreLoader.handleBackendOpened` | marked, offline, device checked | a format or validation change in a release; an incomplete mount or restore. An open never creates: stores are created atomically (`StoreCreation`), so a store path holds nothing or a complete store, and a missing or blank backend is declared rather than silently served empty |
| D6 | Backend fail-stop while served (`Host.BackendFailed`; FSIF `onFatal`, fsif/DESIGN.md 4.2) | `StoreRebuilds.handleBackendFailed`/`backendFailed` | marked, then `closeUnloaded(dropDurably = false)`: its device-CRL entries stay live until the discard saves them | any I/O error FSIF treats as fatal; any internal exception in its serial executor; an ENOSPC that the classifier fails to recognize |
| D7 | Store fail-stop (`Host.StoreFailStopped`): its CRL read or recovery failed (`StoreRouter.readAndRecover`, `recoverOrFail`), or `Frontend.failStop` (a commit voter whose apply throws, a recovered commit vote whose object fails to load, an invariant violation; `Tx`) | `StoreRebuilds.handleFailStopped` then `backendFailed`, only while that Store is still the one installed | as D6 | a decoding change in `TransactionRecoveryState`; a bug in an applier or requirement checker, which fires on **every replica of the object** in the same transaction |
| D8 | Backend failed before it was served: at `LoadStore` (`hasFailed`), opened but not served, or failed after it stopped being served (not replaced, directory on a loaded device) | `StoreLoader.handleLoad` then `retireUnserved`; `StoreLoader.handleBackendOpened`; `StoreRebuilds.backendFailed` | marked, offline | as D6 |
| D9 | Backend fail-stop during a clean close (`ShutdownStore`) | `StoreLoader.handleClose` then `StoreRebuilds.failedOnClose` | marked; `crl_save.log` removed (X4) | as D6, and any close-path bug |
| D10 | A directory at the path of a store being rebuilt here that carries neither `rebuild-complete` nor `rebuild-required` | `StoreRebuilds.retireForeignCopy` | marked (a served one goes through `backendFailed`) | a leftover of an earlier tenancy, or a copy put back by hand |
| D11 | A transfer out of a marked store, or one whose close fails with `StoreFailedOnClose` | `StoreTransfers` (`returnLostTransferToRebuild`) | transfer aborted; the entry goes back to Active and is then flipped | inherits D6/D9 |
| D12 | Operator `fail-storage-device` | `BaseAspenClient.failStorageDevice`; `FailedStorageDeviceDurableTask` | tombstone; every store Rebuilding elsewhere | not automatic; unguarded by design (S10) |

**The flip.** For every offline store of a device that has the marker and an Active entry, the reconcile's Lost CRL
State pass (`DeviceReconciler.reconcileDeviceState`) calls `StoreRebuilds.requestRebuild`. That flips the entry to
Rebuilding in a transaction guarded only by the device state object's `KeyRevision` (S1). The device is then checked
again, and the Rebuilding pass calls `startRebuild`, which discards (X2) and rebuilds as slots allow (S3). A marker
whose entry is Offline (the device is shutting down) is not flipped.

### 4.2 Time from detection to removal from service

Removal from service is immediate (S4). Every runtime declaration also requests `checkStorageDevice`, so the flip
commits within one round trip of the metadata pools, and the discard follows at the next reconcile that has a free
slot. On a healthy control plane this takes seconds. Destruction (X3) waits for the rebuild to succeed, which takes
minutes to hours depending on store size. A failed pass is retried with backoff, from 30 s doubling up to 30 min
(`StoreRebuilds.scheduleRebuildRetry`).

### 4.3 Discards and deletions (destroy state)

Classification: **destructive** means no copy of the destroyed bytes is retained. **Recoverable** means the bytes are
retained, or are a derived copy of data that still exists elsewhere.

| ID | Action | Code | Class | What survives |
|---|---|---|---|---|
| X1 | Device CRL directory renamed to `crl.lost-<uuid>` (D2) | `DeviceDiscovery.openDeviceCrl` | Recoverable by hand; never used or reclaimed by code (S7) | the whole old CRL (votes, Paxos state and pending updates of every store of the device) |
| X2 | `discardLostStore`: marker already durable; the store's device-CRL entries read and saved into `lost-crl-state.log` (kept if a crash left it); `CrashRecoveryLog.dropStore` durable; directory renamed to `pending-delete/<store>-<uuid>`, durably | `StoreRebuilds.discardLostStore`, `moveLostStoreToPendingDelete` | Recoverable | data plus saved entries in `pending-delete/` until X3. After D1/D2 the entries are in `crl.lost-<uuid>` instead: the fresh CRL holds none |
| X3 | Pending-delete copy deleted: its store's entry on this device is Active, or its entry is gone and the pool's holder device has it Active; or its store config does not parse | `DeviceReconciler.clearPendingDeletes` | **Destructive**, the final step for every declared store | the rebuilt store, whose content came from the peers. Exceptions: SL-W2, SL-W4 |
| X4 | `crl_save.log` removed after a fail-stop during close. For a transfer-out close (`extractCrl`), the entries were already deleted from the device CRL | `StoreRebuilds.failedOnClose` | **Destructive** for the store's transaction state (SL-W3) | the backend files |
| X5 | Reconcile "Deleted Stores" pass: directory of an offline store whose entry is absent from the device's tree state | `DeviceReconciler.reconcileDeviceState` | **Destructive** for the local copy. Intended for a completed transfer out, where the destination holds the data | the destination's copy, if the transfer completed. Also reached by a re-placed rebuild whose lost copy was not yet discarded (SL-W1) |
| X6 | `dropLeftoverCrlState`: device-CRL entries of stores with no entry, no directory and not loaded | `DeviceReconciler.dropLeftoverCrlState` | **Destructive** for those entries | nothing local |
| X7 | `UnloadFailedStore`: entries dropped from the device CRL after a failed `crl.loadStore` | `StoreLoader.handleUnloadFailed` | Recoverable | the save file holds the full state |
| X8 | Entries deleted from the device CRL after a transfer out's save file is durable | `StoreLoader.handleClose`, `StoreTransfers.extractUnloaded` | Recoverable (moved) | the save file (but see X4) |
| X9 | Derived copies deleted: rebuilt copy on `RebuildOutcome.Discard` (`LocalDevices.discardStoreDirectory`); rebuild staging (first pass, staging `BackendStateLost`, `abandonUnlisted`); transfer staging and stale promoted copies (`StoreTransfers`, `StoreTransferManager`); `transferring-in` staging and `creating/` at device load (`DeviceDiscovery.loadOwnedDevice`) | as listed | Derived | peers, or the transfer source |
| X10 | Tombstone: device removed from its set and host, ids zeroed, stores rebuilt elsewhere | `FailedStorageDeviceDurableTask` | One-way **metadata** change; the media is untouched | the physical data. The old host retires the device under a durable `failed` marker (`DeviceShutdown.retireFailedDevice`, also written by `DeviceDiscovery.handleOwnershipResolved`), and `tryLoadDevice` never loads or claims a device with it |

### 4.4 Retention

Every store declared lost through D1-D11 keeps its last local copy until its rebuild succeeds. The marked directory
stays at its path, offline, until its discard. The discard (X2) moves it, with its saved device-CRL entries, into
`pending-delete/`. A rebuild that cannot finish (too few readable peers, an unreadable object, out of space) leaves
the store Rebuilding, so the copy is kept for as long as that lasts. A failed lookup in `clearPendingDeletes` also
keeps it. For a correlated false positive, this turns data loss into an outage: pools fall below `kc`, rebuilds stall
on unreadable objects, and the copies stay on disk.

What is not retained, or not for long:

* The copy is deleted at the first reconcile after the store is Active again (X3). That can be minutes after the
  declaration, which is short compared with a rollout. It does not wait for repair or for any check that the
  declaration was isolated.
* `crl.lost-<uuid>` is retained, but nothing ties it to the pending-delete copies whose entries it holds, and nothing
  reads it.
* The exits in SL-W1 to SL-W4 destroy a copy, or its transaction state, before the store's rebuild succeeds or
  regardless of it.
* No code path reinstates a retained copy. Clearing a false declaration by hand means removing the marker, moving
  the directory back from `pending-delete/`, and, after D1/D2, restoring the CRL from `crl.lost-<uuid>` over a fresh
  CRL that may by then hold new entries. If the entry has already flipped, it must also be put back to Active. None of
  this has a defined safe procedure. In particular, a store fail-stopped for D6/D7 may genuinely be missing an update
  it voted to commit.

---

## 5. Concrete systemic-failure scenarios from the code

These are places where one software change or environmental fault produces declarations on many stores at once.

* **SC1 CRL on-disk format or recovery code change.** A release whose `Recovery.recover` throws for a previously
  valid file is D2 for every device of every upgraded host: every store is marked and taken out of service when its
  host restarts. Format skew and damage look the same to the code. Configuration and default changes are not a cause
  (S6).
* **SC2 CRL writer bug.** A writer bug that produces a log the next recovery rejects fails the CRL at runtime
  (`CrashRecoveryLog.failed`, `DeviceShutdown.crlFailed`). That unloads the device until the host restarts, which is
  an outage on every host running the bug. The device is deliberately not reloaded in-process, which would loop into
  D2. The natural operator response, restarting the hosts, then runs `openDeviceCrl` over the damaged logs: D2 on
  every affected device.
* **SC3 FSIF recovery or validation change.** A stricter check in `FsifRecovery.recover`, or a changed record
  encoding, turns a store that older code opened into `StoreLost`: D5 on every FSIF store at upgrade
  (fsif/DESIGN.md 4.1 lists the false-trigger shape of each check).
* **SC4 Fail-stop on a common path.** An internal exception in FSIF's serial executor (D6), or an applier or
  requirement-checker bug (D7), fires on every store that runs the path. In D7 it fires on every replica of the
  object in the same transaction, so `d` for that pool jumps to the number of voters at once.
* **SC5 Resource exhaustion at start** (file descriptors, memory, threads in a restart storm). The CRL open retries
  `isTransient` errors for three scans (about 20-30 s at the 10 s device scan) and then declares. The save-file load
  (`loadValidStoreState`) declares at once on any `Throwable`.
* **SC6 Incomplete mount or restored backup.** `crl/` absent while store directories exist is D1, with no retry.
  Absent FSIF files or a missing RocksDB `CURRENT` is D5.
* **SC7 Operator error.** `fail-storage-device` on the wrong device, or on two devices of one pool. It is one-way and
  unguarded by design (S10).
* **SC8 Rolling restart faster than rebuild.** Even a correct system under a legitimate fault pattern is exposed. If
  any check has a false-positive rate p per store, the expected number of stores of one pool out of service at once
  grows with the restart rate and the rebuild duration (hours for a large store, with failed passes retried after up
  to 30 min).

---

## 6. Interaction with other subsystems

* **Rebuild** ([rebuild-process.md](rebuild-process.md)). A rebuilt store is a new incarnation whose Paxos acceptor
  state covers only transactions started after its `acceptor-since`. So losing the old CRL state cannot let a rebuilt
  store vote inconsistently. A transaction that needed the lost votes stalls rather than splitting. A rebuild that
  cannot read an object fails its pass and keeps the store Rebuilding, which keeps it out of the pool's failure
  budget indefinitely, until `accept-rebuild-loss` (see SL-W2). Placement may co-locate a pool's stores on one device
  (`REBUILD CO-LOCATED`), which raises the correlation of later real failures.
* **Retired devices.** A tombstoned device is retired by its old host under a durable `failed` marker and never
  loaded again (X10). Until its first successful reconcile, a restarted or partition-healed old host may still answer
  peers that hold a stale store-to-host mapping.
* **UnackedCommits and repair** ([transaction-recovery.md](transaction-recovery.md),
  [repair-process.md](repair-process.md)). A store's unacknowledged commit votes also have UnackedCommits entries in
  its pool's metadata group, which let repair reconcile the rebuilt store. A correlated CRL loss still destroys the
  votes themselves, so the arithmetic of section 2 is unchanged, and the reconcile needs a read quorum that still
  holds the transaction.
* **Recovery depends on the metadata group.** A recovered unresolved commit vote accepts no abort before its
  UnackedCommits lookup answers. A systemic event that also takes the metadata pool below `kc` leaves such a vote
  holding its locks until the pool returns. That is a stall, not a declaration.
* **Applied stubs** ([crash-recovery-log.md](crash-recovery-log.md)). Stubs live in the device CRL beside the
  transactions and go with them in X1, X2, X6 and X7, and in X8 into the save file. Losing them costs no data: a
  rebuilt store is muted for older transactions. A long repair stall, which a systemic event can cause, lets stubs
  accumulate against each device's CRL admission. That is a stall, not a declaration.
* **Prefer-abort** ([paxos-transactions.md](paxos-transactions.md)). A systemic client outage aborts orphaned
  data-less transactions rather than committing them, which is the intended direction.

---

## 7. Candidate mitigation directions (ideas only, none selected)

These are listed to seed design work. They are neither requirements nor an ordering.

* **M1 Long retention of discarded state.** Keep `pending-delete/` copies (and `crl.lost-<uuid>`, linked to them)
  for a retention period measured in days. Release them only on an explicit instruction through the object-based
  admin channel, or under space pressure with an alarm. Record why, when, on which software version, and which
  check fired (the Backlog item's `suspected-loss/` proposal). Salvage tooling could reopen a retained copy.
* **M2 Rebuild before taking out of service.** For declarations whose store still opens (D3, D7 recovery failures,
  version-suspect D2/D5), keep serving reads, or at least keep the store's votes counted, until a rebuild into
  staging has completed. Then swap. This would make feasibility visible before availability is lost (S11).
* **M3 Redundancy-aware admission at the flip.** `requestRebuild`'s transaction could read the pool state and the
  device states of the pool's stores. It would refuse to flip when the number of the pool's stores already
  Rebuilding, Offline or marked, plus this one, exceeds a budget derived from the IDA (for example `w - wt`). The
  pool object's `KeyRevision` would serialize concurrent hosts. A refused store stays offline with its marker and
  its local state intact, with an alarm. It does not help availability, since the store is already offline at the
  mark.
* **M4 Per-pool and per-host rate limits on declarations**, kept in shared state, independent of rebuild
  concurrency.
* **M5 Circuit breaker for bursts.** If more than a fraction of a host's stores, or of a pool's stores, or more than
  a cluster-wide number of declarations in a window, are declared lost, assume a systemic cause. Stop automatic
  destruction cluster-wide, keep the markers, and require an operator command to proceed.
* **M6 Classify the cause before declaring.** Separate transient errors (retried with backoff across restarts, not
  three scans in one process), version or format mismatches (refuse to open and alert, as `CrlSizingMismatch` does),
  and content corruption (checksum failure).
* **M7 Version-aware safety.** Hosts publish their software and format versions. Destructive actions are disabled for
  a store whose on-disk format version differs from what the binary last wrote, for a period after any version
  change, or while hosts disagree on version. Pair this with a canary stage of the upgrade process (Backlog,
  "Mixed-Version Support / Upgrade Process").
* **M8 Independent verification before release.** Before deleting a retained copy, compare it with the rebuilt store.
  If the old slices agree with what the pool produced, the fault was in the detector, not the data: alert. This uses
  the pool, which does not run the suspect detector, as the independent witness.
* **M9 Alarming.** A durable record (counter, tree entry) of every declaration with its cause, visible in `show-*`
  commands and delivered through the planned operator alerts (Backlog, "Operator alerts").

---

## 8. Why none of this is a solution yet

* M1 protects data but not availability. A systemic false positive still takes every affected store out of service
  at once (S4), and the rebuilds still consume read load across the cluster.
* M2 keeps availability only for detectors that leave the store usable, and a store that fail-stopped on a real
  missed update must not keep voting.
* M3 and M4 bound the number of simultaneous flips per pool, but a store is already offline at the mark. They do not
  detect a false positive spread over time (a bug that flags one store per restart across a long rollout). They also
  need a trustworthy, available shared counter, and that counter lives in the same metadata pools a burst can take
  down.
* M5 needs cluster-wide visibility during an event that may take the metadata pools down. A fail-safe default (do
  nothing destructive when the control plane cannot be read) is possible, but then a real double failure is not
  repaired while the control plane is unhealthy.
* M6 and M7 rely on classifying causes in the code that has the bug. A release that misclassifies defeats them, and
  a version-aware guard is itself new code in the same rollout.
* M8 requires reading the old state with code that may be the buggy part.
* All of the above leave the fundamental tension open. The only independent evidence that a store's local state is
  bad lies outside the code that decides it is bad: peers, external tooling, operators. The design must choose what
  authority overrides a local declaration, and how long data is kept before that authority is trusted.

The Backlog item's suggested first step is a test that injects a fleet-wide false detector (for example, forcing
`openCRL` to throw on every device during a simulated rolling restart) and shows what survives. With the current
code, every pool whose number of simultaneously marked stores exceeds `w - kc` is expected to become unreadable,
with its copies retained in `pending-delete/` or at their paths, and nothing to bring them back.

---

## 9. Open questions

1. What is the intended retention of `crl.lost-<uuid>`, and who may use it (S7)?
2. What is a safe manual procedure for reinstating a store that was falsely declared lost (section 4.4)? It is
   different for D1/D2 (CRL moved aside), D3 (save file), D5 (backend refused) and D6/D7 (the store may really lack
   an update it voted for).
3. What distinguishes a transient CRL read failure from damage for `StoreRouter.readAndRecover`? Today any failure
   fail-stops the store (D7).
4. Is the daily random-slot technique used for network snapshots (`NetworkSnapshotReader`) a model for staggering
   destructive checks, or only for read load?
5. Can the bootstrap pool, whose trees live in itself, be rebuilt at all once more than `w - kc` of its stores are
   out of service? Every flip and reconcile reads it, so no.

---

## Open weaknesses

These go beyond the tracked Backlog item. Each is an exit that destroys retained state the design otherwise keeps
until the store's rebuild succeeds.

### SL-W1. Re-placing a queued rebuild deletes the lost copy and its CRL entries before any rebuild (CONFIRMED; systemic loss)
Mechanism: A store marked `rebuild-required` stays at its path, offline (`StoreLoader.tryLoadStore` adds it to
`offlineStores`), until `StoreRebuilds.startRebuild` gets a slot and `discardLostStore` moves it to `pending-delete/`.
While it waits for a slot (S3: two per host), its entry is Rebuilding. `AspenClient.replaceRebuild` (`transfer-store`
on a Rebuilding store) removes the entry from the source device (`srcState.removeStore`) and repoints the pool. At
the source's next reconcile, the "Deleted Stores" pass of `DeviceReconciler.reconcileDeviceState` takes every offline
store whose entry is absent for a completed transfer out and runs `os.remove.all(storePath)`. In the same reconcile,
`dropLeftoverCrlState` then finds the store with no entry, no directory and not loaded, and drops its device-CRL
entries (`CrashRecoveryLog.dropStore`). The same happens when `discardLostStore` failed and left the directory in
place.
Trigger / scenario: D2 marks every store of a device lost, say 10 stores. Two take the slots; eight are queued with
their directories at their paths. The operator, seeing `REBUILD CO-LOCATED`, `REBUILD OUT OF SPACE` or slow
progress, re-places several queued stores onto other devices, the documented remedy. The source host's reconcile
(nudged by `replaceRebuild`'s `CheckStorageDevice`) deletes each re-placed store's only local copy and its CRL
entries at once, before the destination's rebuild has read anything. In a correlated event, where the destination
rebuild cannot finish, that copy was the last one.
Consequence: This contradicts the retention guarantee (section 4.4; `server/host/CLAUDE.md`, "Nothing of a lost store
is destroyed before its rebuild succeeds"). Under a systemic false declaration it is permanent loss of data the
system had retained. The existing test (`RebuildEndToEndSuite` "a rebuild re-placed on another device ...") re-places
a tombstoned device's rebuild, where no local copy exists.
Suggested fix: In the Deleted Stores pass, treat a directory carrying `rebuild-required` (or any marker other than a
completed transfer's) as a lost copy. Run it through `discardLostStore`'s save, drop and move into `pending-delete/`,
which `clearPendingDeletes` already releases once the pool's holder has the store Active. Alternatively, have
`replaceRebuild`'s source-side handling discard before the entry goes. Add a test: mark three stores on a
one-slot host, re-place a queued one, and assert that its copy and its entries are in `pending-delete/` until the
destination is Active.
Effort: S   Priority: 3

### SL-W2. An accepted rebuild loss deletes the retained copy that may hold the accepted-lost objects (CONFIRMED; systemic loss)
Mechanism: `accept-rebuild-loss` (`AspenClient.acceptRebuildLoss`) sets `acceptLoss` on a Rebuilding entry.
`RebuildingStore.retryFailures` then records each still-unreadable object in the ErrorTree
(`recordAcceptedLoss`) and finishes, and the entry becomes Active. `DeviceReconciler.clearPendingDeletes` deletes a
`pending-delete/` copy as soon as its store is Active, on this device or on the pool's holder device. It does not
consider whether the rebuild accepted losses. The ErrorTree marks let repair restore an object only "if it ever
becomes readable" from the pool. The deleted copy is often the only remaining source of exactly those slices.
Trigger / scenario: A correlated false declaration takes `d > w - kc` stores of a pool out of service; their copies
sit in `pending-delete/`. Rebuilds fail on unreadable objects. To restore service, the operator accepts the loss on
one store. Its rebuild finishes, the entry goes Active, and the next reconcile deletes that store's old copy, which
held valid slices of the accepted objects. The other stores' rebuilds may then also be accepted, and each deletes its
copy in turn.
Consequence: Objects that an operator accepted as lost only for availability become lost for good, although their
data was on disk. Acceptance is the escape hatch most likely to be used in a systemic event.
Suggested fix: Keep a `pending-delete/` copy whose store's rebuild accepted any loss until an explicit release. For
example, record the acceptance in the rebuilt directory or the copy, or keep the copy while the pool's ErrorTree holds
`HLCTimestamp.Zero` marks against the store. Warn in `accept-rebuild-loss`'s output that the old copy is retained and
must be released by hand. Add a test extending `RebuildEndToEndSuite`'s accepted-loss case: assert that the copy
survives the reconcile after the flip.
Effort: S   Priority: 3

### SL-W3. A fail-stop during a transfer-out close deletes the only copy of the store's transaction state (CONFIRMED; systemic loss)
Mechanism: `StoreLoader.handleClose` with `extractCrl` saves the store's CRL state to `crl_save.log`, closes the
store, and then deletes the entries from the device CRL (`deleteTransactions`). Only after that does it check
`backend.hasFailed`, and if the backend failed it calls `StoreRebuilds.failedOnClose`. That marks the store and then
removes `crl_save.log` (`DurableFiles.remove`). The save file was then the only copy of the store's transactions,
applied stubs and their pending updates. The later `discardLostStore` reads the device CRL, which now holds nothing
for the store, and writes an empty `lost-crl-state.log`. The removal protects nothing: `tryLoadStore` and
`DeviceDiscovery.adoptionClass` both test `rebuild-required` before they look at the save file.
Trigger / scenario: A release with a bug on FSIF's close path, or an I/O error during close, fail-stops the backend
of each store being closed for a transfer out (for example, during a rebalance of many stores). Each such store is
marked and its save file deleted. Its `pending-delete/` copy then holds data without the CRL state it needs for any
salvage.
Consequence: This is a retention gap (S14). The retained copy of a falsely declared store lacks its transaction
state, so it cannot be reinstated as it was. Safety is unaffected, since the rebuild treats the store as a new
incarnation.
Suggested fix: In `failedOnClose`, rename `crl_save.log` to `RebuildingStore.LostCrlStateFile` (durably) instead of
removing it. `discardLostStore` already keeps an existing `lost-crl-state.log` rather than reading the device CRL.
Add a test: a store whose backend fails during a transfer-out close has its extracted entries in the
`pending-delete/` copy.
Effort: S   Priority: 3

### SL-W4. A pending-delete copy whose store config does not parse is deleted (CONFIRMED; systemic loss)
Mechanism: `DeviceReconciler.clearPendingDeletes` runs `Try(StoreConfig.loadStoreConfig(copy / store.cfg))` on every
copy, and on any failure calls `delete(copy, "it holds no store config, as an interrupted delete leaves it")`. A
parse failure, a decoding change, or a transient read error is taken as an interrupted delete.
Trigger / scenario: A release changes the `store.cfg` format or its loader so that it rejects files written by the
previous version. On the first reconcile of each device after the upgrade, every retained `pending-delete/` copy is
deleted, including the copies of stores whose rebuilds are still stuck. The same release makes `tryLoadStore` fail
to load those stores ("Failed to load store", not marked), so the outage and the loss of retained copies arrive
together.
Consequence: A correlated software fault destroys exactly the retained state meant to survive one.
Suggested fix: Delete only a copy whose config file is absent, or better, make the delete itself recognizable: rename
the copy to a `deleting-` prefix before `os.remove.all`, and delete without parsing only copies carrying that prefix.
Keep and log anything else. Add a test with a copy whose `store.cfg` is unparsable.
Effort: S   Priority: 3
