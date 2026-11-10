# Systemic Loss Protection (placeholder, problem statement only)

Status: derived from the code at commit `c3ad5493` (2026-11-09); refreshed 2026-11-09 against `0d08e3c6` (branch
`pin_refactor`, replay pins), see section 11. Paths are relative to
`src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`.

> **A solution to the problem described here has NOT been identified.** This document states the problem, inventories
> every code path found while writing [rebuild-process.md](rebuild-process.md) that can automatically declare a store
> lost or discard store state, classifies each as destructive or recoverable, and lists candidate directions as ideas
> only. None of the ideas is a design, and each has an unresolved hole (section 8). Nothing in the current code protects
> the system from the failure mode below.

Related: [rebuild-process.md](rebuild-process.md) (the mechanism being protected from misfiring),
[crash-recovery-log.md](crash-recovery-log.md) (the CRL whose corruption is the most likely false trigger),
[repair-process.md](repair-process.md), [paxos-transactions.md](paxos-transactions.md),
[transaction-recovery.md](transaction-recovery.md), [transaction-requirements.md](transaction-requirements.md).

---

## 1. The problem

Aspen treats a store as expendable if its local state cannot be trusted: it deletes the local state and rebuilds the
store from the rest of its pool. That is the right response to a failed disk, and it is cheap for one store at a time,
because the pool tolerates the loss of some stores (`IDA.failureTolerance = width - writeThreshold`,
`common/ida/IDA.scala:144`; reads need `consistentRestoreThreshold`, `:103`).

The mechanism is only safe if declarations of loss are **independent** and **true**. A *software* fault can make the
detector itself wrong, and it will be wrong on every node running the same code at the same moment. Example: a bug (or
a changed default) makes a device's CRL look corrupt. One device doing this is a normal rebuild. A rolling upgrade
doing this on every host marks every store on every device lost; each host discards its stores' local state
(section 4); each rebuild then needs the read quorum of peers that were themselves just discarded. Whole pools, and
the metadata pools and bootstrap pool that every rebuild depends on, are gone with no way to recover them. The
system cannot tolerate this, and it must not be left to chance whether it happens.

Why this is hard here:

* **The detector and the repairer are the same code.** The evidence that triggers a declaration is "my local CRL, save
  file or backend does not look right". A software bug in reading those files is indistinguishable, locally, from
  corruption.
* **Declarations are local and unilateral.** No declaration site consults the pool, the IDA, other stores or other
  hosts (section 3, S1).
* **Destruction comes before reconstruction.** The old state is deleted before a single peer has been read
  ([rebuild-process.md](rebuild-process.md) section 4.3, F2).
* **Rebuild needs healthy peers**, and the peers are subject to the same fault (rebuild-process FS3, FS16).
* **Rollouts correlate faults in time.** The automatic detection runs at host start and at device load; a rolling
  upgrade exercises it on each host in turn, faster than rebuilds complete.

---

## 2. What is at stake: the arithmetic

Let a pool have width `w`, write threshold `wt`, consistent read threshold `kc` (`IDA.consistentRestoreThreshold`:
Replication `w/2+1`, `common/ida/IDA.scala:243`; Reed-Solomon `restoreThreshold`, `:419,603`). A rebuild reads each
object through the normal read path, so it needs `kc` intact stores that hold the object's current revision. The state
that matters is how many stores of **one pool** are simultaneously discarded and not yet rebuilt, `d`.

* Rebuild can proceed only if `w - d >= kc`. Replication(3,2): `d <= 1`. A Reed-Solomon pool with `w=5,k=3`: `d <= 2`.
* Writes stall once `d > w - wt` (the Paxos quorum is `wt`, `server/transaction/TransactionDriver.scala:88`).
* Beyond `d > w - kc` the data is gone, including objects that survive on a single store (a consistent read needs the
  majority).
* The same rule applies to the **metadata pool** holding a pool's allocation/error trees and the **bootstrap pool**
  holding the metadata pools' trees, the pool state objects, the device state objects and the Radicle. Rebuilding any
  store walks its pool's allocation tree and reads pool state (`server/rebuild/RebuildingStore.scala:183-198`). If
  the bootstrap pool falls below `kc`, no rebuild anywhere can run and clients cannot start.

A declaration burst that is correlated across hosts therefore breaks the inequality for most pools at once. The
concurrency cap of 2 rebuilds per host (`server/Host.scala:179`) does not help: it limits rebuilds that have started, not
the discards that precede them (section 3, S3).

---

## 3. Observed properties of the current system

These are facts about the code, each checked. They define the problem; none is a goal.

* **S1 Declarations are local and consult no redundancy information.** Every site in section 4 decides from files on
  one disk. None reads the pool state, the IDA, other devices' state or other hosts.
* **S2 One decision can flag a whole device.** `DeviceDiscovery.openDeviceCrl` marks every store directory of a device
  from a single failed CRL open (`server/host/DeviceDiscovery.scala:498-520`); the next host restart can do it for
  every device of the host in the same event.
* **S3 Pacing applies to rebuilds, not to destruction.** `startRebuild` tests
  `rebuildingStores.size < maxConcurrentRebuilds` and then calls `discardLostStore`
  (`server/host/StoreRebuilds.scala:167-169`); `rebuildingStores` is populated only after `discardLostStore` returns
  true, so one reconcile pass starts the discard of every marked Rebuilding store on the device at once.
* **S4 Destruction precedes reconstruction.** *Fixed 2026-11-09:* the discard now moves the copy, with its saved CRL
  entries, to `pending-delete/`, deleted only once the store is Active again (rebuild-process.md F2). Was: `discardLostStore` drops the store's device-CRL entries and deletes its
  directory before the rebuild reads anything (`StoreRebuilds.scala:499-540`). There is no feasibility check.
* **S5 Errors are classified coarsely.** CRL open: `catch case t: Throwable` flags the device
  (`DeviceDiscovery.scala:508-515`). Save file: `catch case t: Throwable => Left(InvalidSaveFile)`
  (`server/crl/CrashRecoveryLog.scala:113-114`). Recovery read failure fail-stops the store
  (`server/host/StoreRouter.scala:222-256`). Transient (EMFILE, EIO, timeouts, interrupts), persistent, and
  configuration/version causes all lead to the same conclusion. There is no retry before declaring.
  *Update 2026-11-09:* the CRL open now retries transient errors (I/O and VM errors) on 3 scans before declaring
  (crash-recovery-log.md F6). Persistent and configuration causes still declare at once, by decision: protection
  against correlated declarations is to be built at a higher level, which this document still leaves unsolved.
* **S6 Configuration and version skew are treated as corruption.** `SimpleCRL.SizingMismatch` is thrown when the
  stream files on disk differ in count or size from the device config (`server/crl/simple/SimpleCRL.scala:53,84-93`);
  the keys `crl-num-streams` and `crl-max-file-size-mb` are optional and default in code
  (`server/StorageDeviceConfig.scala:11-12,43-44`), so changing a default, or a config loader change, turns every
  existing CRL "unrecoverable" (`DeviceDiscovery.scala:508-515`).
* **S7 The only retained artefact is the old CRL directory.** `crl.lost-<uuid>` (`DeviceDiscovery.scala:518`) is never
  read, expired or referenced by any other code (repository search), and without the store directories it is useless.
  No store directory is ever quarantined.
* **S8 There is no brake and no escalation.** I found no counter, threshold, circuit breaker, operator confirmation or
  alarm other than log lines (`logger.error`) at any declaration site.
* **S9 Every host runs the same detection at the same lifecycle events** (start, device load, adoption, close), with
  no staggering and no version awareness.
* **S10 Operator commands are unguarded in the same way.** `failStorageDevice` checks only that the device is not
  already failed and then enrolls the drain (`client/internal/BaseAspenClient.scala:719-`), unlike `shutdownStorageDevice`,
  which refuses when a pool would drop below its write threshold (`client/AspenClient.scala:644`). The tombstone is
  one-way (`client/AspenClient.scala:312-324`).

### Protections that would be needed (not satisfied)

* **S11 [NOT SATISFIED]** Irreversible discard of store state happens only after it is shown that the rest of the
  pool can reconstruct it.
* **S12 [NOT SATISFIED]** The number of stores of any one pool (and of the metadata/bootstrap pools) that may be
  simultaneously in the discarded-or-rebuilding state is bounded below the pool's tolerance, enforced across hosts.
* **S13 [NOT SATISFIED]** Declarations caused by transient, configuration or version conditions are not treated as
  corruption.
* **S14 [NOT SATISFIED]** Discarded state remains recoverable for a retention period that is long compared with
  detection and rollout times.
* **S15 [NOT SATISFIED]** A burst of declarations (across stores, devices or hosts) is detected as systemic and halts
  automatic destruction until an operator confirms.

---

## 4. Inventory: every path that can declare a store lost or discard state

### 4.1 Declarations (decide that a store is lost)

| ID | Trigger | Code | Immediate local effect | Notes on false positives |
|---|---|---|---|---|
| D1 | `<device>/crl` absent from a device that holds store directories | `server/host/DeviceDiscovery.scala:504` | marker on **every** store dir of the device; fresh empty CRL created | a partially restored or re-mounted device; a path/layout change in a release |
| D2 | CRL open throws anything (`Throwable`) on a device that holds stores | `DeviceDiscovery.scala:508-515` | as D1, plus `crl/` renamed to `crl.lost-<uuid>` (`:517-518`) | `SizingMismatch` from changed defaults (S6); `Recovery.BrokenEntryChain` parse changes (`server/crl/simple/Recovery.scala:63-75`); EMFILE, ENOMEM, EIO, interrupts |
| D3 | `crl_save.log` present but `loadValidStoreState` fails (missing/truncated/wrong store/any `Throwable`) at load | `server/host/StoreLoader.scala:293-296` ► `StoreRebuilds.crlSaveFileLost` `:344` | store closed unserved, marker, offline | the file is treated as the only copy (`StoreRebuilds.scala:336-343`) even if the device CRL still holds the same entries (the window between the save and `deleteTransactions`, `StoreLoader.scala:514-517`) |
| D4 | Same, for a `transferring-out` store, during **adoption** | `DeviceDiscovery.adoptionClass` `:294-296`; `server/host/DeviceMetadata.scala:540-553` | marker written inside the adoption transaction's prepare closure; entry written Rebuilding | marker persists if the transaction later returns Changed/Unclean; verified harmless, since the marker states a fact about the disk (rebuild-process F13, refuted) |
| D5 | `BackendStateLost` from backend open (FSIF `StoreLost`: no valid index, journal seq mismatch, corrupt journal, missing files) | `StoreLoader.scala:188-202`; `server/store/backend/fsif/FsifRecovery.scala:138-264` | marker, offline | a format/version change in a release; a file that is missing because a mount is incomplete (`FsifRecovery.load` returns None for an absent file `:106-109`) |
| D6 | Backend fail-stop while served (`BackendFailed`; FSIF only) | `server/Host.scala:453` ► `StoreRebuilds.backendFailed` `:396-438` | marker first, then `closeUnloaded(dropDurably = marked)`: device-CRL entries dropped durably (`StoreLoader.scala:421-434`) | any I/O error the FSIF layer treats as fatal |
| D7 | Failure to read or recover the store's transactions from the device CRL | `server/host/StoreRouter.scala:222-256` ► `StoreRebuilds.handleRecoveryFailed` `:66` | as D6 | a transient CRL read failure or a decoding change in `TransactionRecoveryState` |
| D8 | Backend failed between open and install | `StoreLoader.scala:287-291`, `:173-183` | marker, offline | |
| D9 | Backend fail-stop during a clean close | `StoreLoader.scala:525-527` ► `StoreRebuilds.failedOnClose` `:374-388` | marker; `crl_save.log` removed (`:385`) | |
| D10 | Store marked when its device is adopted | `DeviceDiscovery.scala:289-290`, `DeviceMetadata.scala:540-553` | store comes in as Rebuilding | inherits any false positive that wrote the marker |
| D11 | Transfer out requested for a marked store | `server/host/StoreTransfers.scala:604-611,636-641,668-` | transfer aborted, store sent down the rebuild path | |
| D12 | Operator `fail-storage-device` | `client/internal/BaseAspenClient.scala:719`; `common/metadata/management/FailedStorageDeviceDurableTask.scala:186,317,386` | tombstone (ids zeroed, device removed from set and host), every store moved to Rebuilding elsewhere | not automatic; unguarded (S10) |

The reconcile step that follows D1-D11 is also unguarded: for every offline store with a marker whose entry is Active,
`requestRebuild` flips it to Rebuilding with a transaction that checks only the device state
(`server/host/DeviceReconciler.scala:172-176`, `StoreRebuilds.scala:448-485`).

### 4.2 Discards and deletions (destroy state)

Classification: **destructive** = no copy of the destroyed bytes is retained by the code; **recoverable** = the bytes
are retained or are a derived copy of data that still exists elsewhere (and the code, or a person, can use that).

| ID | Action | Code | Class | What survives |
|---|---|---|---|---|
| X1 | Device CRL directory renamed to `crl.lost-<uuid>` | `DeviceDiscovery.scala:517-518` | **Recoverable (by hand)**; never used or reclaimed by code | the whole old CRL (votes, Paxos state of every store of the device). Not useful without the store directories (X3) |
| X2 | `dropStore` on fail-stop of a served store: device-CRL entries removed durably | `StoreRebuilds.scala:421` ► `StoreLoader.scala:421-434`, `server/crl/CrashRecoveryLog.scala:159` | **Destructive** for the store's transaction state | the store's backend files (until X3) |
| X3 | `discardLostStore` (before 2026-11-09): `dropStore` again, rename the store directory to `discarded/`, `os.remove.all` it. Now: entries saved into the directory, dropped, the directory moved to `pending-delete/` and deleted only once the store is Active again | `StoreRebuilds.scala:499-540` | **Destructive** for the store's object slices and transaction state | nothing local. Peers hold the data if their count permits (section 2) |
| X4 | `discarded/` and the transfer staging directory deleted at device load | `DeviceDiscovery.scala:544-552` | Destructive for already-discarded copies; staging is derived | |
| X5 | `crl_save.log` removed after a fail-stop during close | `StoreRebuilds.scala:385` | **Destructive** (it may be the only copy of the transfer-time state) | |
| X6 | Reconcile "Deleted Stores": directory of an offline store whose entry is absent from the tree | `server/host/DeviceReconciler.scala:146-161` | **Destructive** local copy; intended after a completed transfer, where the destination holds the data | the destination's copy, only if the transfer really completed. Triggered by the tree not listing the store; a wrong tree read or a wrong tree write would delete a live offline store's directory |
| X7 | `dropLeftoverCrlState`: CRL entries of stores that have no tree entry, no directory and are not loaded | `DeviceReconciler.scala:76-104` | **Destructive** for those entries | |
| X8 | Rebuilt or transferred-in copy deleted on `Discard` | `server/host/LocalDevices.scala:51-66`, `StoreRebuilds.scala:245-251` | Derived (rebuilt from peers; or source still holds it) | |
| X9 | Rebuild staging deleted when staging's backend is `BackendStateLost`, or the final path already exists | `RebuildingStore.scala:224-228,148-152` | Derived | peers |
| X10 | Transfer receive/staging directories deleted | `server/transfer/StoreTransferManager.scala:263,362,436`; `StoreTransfers.scala:279,407,458,485` | Derived while the source still holds the store | |
| X11 | Tombstone: device removed from its set and host, ids zeroed; its stores abandoned in favour of rebuilds | `FailedStorageDeviceDurableTask.scala:186-228` | One-way **metadata** change; the old media is not touched but is refused (`DeviceDiscovery.scala:219`) and by design never reintroduced | the physical data on the old device, unusable by the system |
| X12 | CRL entries removed after a transfer-out's save file is durable | `StoreLoader.scala:514-517` | Recoverable: moved to `crl_save.log` first | the save file |
| X13 | `UnloadFailedStore`: CRL entries dropped, save file kept | `StoreLoader.scala:405-414` | Recoverable | the save file holds the full state |
| X14 | Silent empty initialization of a blank FSIF directory that also lacks its marker | `server/store/backend/fsif/FsifBackend.scala:1739-1745` | **Destructive but not declared**: the store is "healthy" and empty | nothing; no rebuild is triggered |

The temporal window to intervene is short: the marker is written, `checkStorageDevice` is requested immediately
(`StoreRebuilds.scala:368,388,422`), the reconcile flips the entry, and the next reconcile runs X3. On a healthy
control plane this takes seconds to minutes.

---

## 5. Concrete systemic-failure scenarios from the code

These are places where a software change or environmental fault would produce D-declarations on many stores at once.

* **SC1 CRL sizing defaults change in an upgrade.** `DefaultCrlNumStreams` / `DefaultCrlFileSizeMb` apply to devices
  whose config omits the keys (`StorageDeviceConfig.scala:11-12,43-44`). Existing 3 x 300 MB CRLs no longer match;
  `SimpleCRL.apply` throws `SizingMismatch`; D2 fires on every device at every upgraded host. Each device's
  `crl.lost-*` (900 MB) is kept, every store directory is deleted by X3. Every pool loses `d` = (stores per upgraded
  host) as each host is restarted.
* **SC2 CRL on-disk format or recovery code change.** `Recovery.recover` throwing for a previously valid file is D2 for
  all devices; the code cannot tell format skew from damage.
* **SC3 FSIF recovery change.** Any stricter validation in `FsifRecovery.recover` (`:138-264`) turns a store that older
  code opened into `StoreLost`; D5 on every FSIF store at upgrade.
* **SC4 Resource exhaustion at start** (file descriptors, memory, threads during a restart storm): `openCRL` and
  `loadValidStoreState` catch `Throwable`.
* **SC5 Incomplete mount or restored backup.** `crl/` absent while store directories exist (D1); FSIF files absent so
  `load` returns None (D5).
* **SC6 Operator error.** `fail-storage-device` on the wrong id, or on two devices of one pool: no pool-health check,
  one-way (S10).
* **SC7 Rolling restart faster than rebuild.** A correct system under a legitimate fault pattern: every host restart
  performs D-checks; if any check has a false-positive rate p per store, the expected number of simultaneously
  discarded stores per pool grows with the restart rate and the rebuild duration (hours for a large store, with
  failed passes retried only hourly, rebuild-process F5).

---

## 6. Interaction with the rebuild flaws

* [rebuild-process.md](rebuild-process.md) F2 (destroy first) is what turns every false positive into a loss.
* F1 (a Failed device's stores are not stopped) means a mistaken `fail-storage-device` is not even cleanly one-way
  physically; the old replica may keep running beside the new.
* F15 (no way to complete a rebuild past an unreadable object) means a pool already below threshold leaves its stores
  Rebuilding forever, which consumes the failure budget (rebuild-process section 6.3) indefinitely.
* F12 (placement may co-locate a pool's stores) increases correlation of later real failures.

---

## 7. Candidate mitigation directions (ideas only, none selected)

These are listed to seed design work. They are not requirements and not an ordering.

* **M1 Quarantine instead of delete.** In X3, rename the store directory (and keep its CRL entries or a copy of its
  save-state) into a quarantine area with a retention period measured in days; release it only on operator command,
  after the rebuilt store has been Active and repaired for a long time, or under space pressure with an alarm. A false
  positive then costs disk space, not data. Salvage tooling could re-open a quarantined store.
* **M2 Rebuild into staging first, discard last ("build then swap").** Do not remove the old state until the rebuild
  has completed (or at least verified that peers can supply it); on completion, swap. The old directory becomes the
  quarantine. This also makes feasibility visible before destruction (S11).
* **M3 Redundancy-aware admission at the flip.** `requestRebuild`'s transaction could read the pool state and the
  device states of the pool's stores and refuse to flip when the number of the pool's stores already Rebuilding,
  Offline or lost (plus this one) would exceed a budget derived from the IDA (for example `w - kc`, or `w - wt`);
  the pool object's `KeyRevision` serializes concurrent hosts. Refused stores stay offline with their marker and local
  state intact, plus an alarm.
* **M4 Per-pool and per-host rate limits on declarations**, kept in shared state (a token bucket in the pool or host
  object), independent of rebuild concurrency, with a hard cap on declared-lost stores per pool per time window.
* **M5 Circuit breaker for bursts.** If more than a fraction of a host's stores (or of a pool's stores, or the number
  of D-events cluster-wide in a window) are declared lost, assume a systemic cause: stop all automatic destruction
  cluster-wide, keep markers, require an operator command to proceed. The counters would live in a replicated object
  (e.g. under the Radicle).
* **M6 Classify the cause before declaring.** Separate transient errors (retry with backoff across restarts before
  concluding), configuration/version mismatches (never treated as corruption: refuse to open and alert), and content
  corruption (checksum failure). `SizingMismatch` in particular should refuse the device, not declare its stores lost.
* **M7 Version-aware safety.** Hosts publish their software version/format version; destructive actions are disabled for
  a store whose on-disk format version differs from what the binary last wrote, or for a period after any version
  change, or while hosts disagree on version. Pair with a canary stage: upgrade a minority of hosts per pool and
  require a health check before continuing.
* **M8 Independent verification before discard.** Rebuild into staging and compare with the old files: if the old
  state validates against the peers (slices agree with what the pool would produce), the fault is the detector, not the
  data; keep the old state and alert. This uses the pool, which does not run the suspect detector, as the independent
  witness.
* **M9 Store generations / fencing.** A per-store epoch recorded in the pool entry and carried on messages, bumped by
  each rebuild or tombstone. Stale replicas (rebuild-process F1) and half-rebuilt stores are then rejected by
  peers rather than relying on the old host to stop.
* **M10 Reversible tombstone and operator preconditions.** `fail-storage-device` refuses when the move would drop a pool
  below tolerance (as shutdown does, `AspenClient.scala:644`), and allows un-failing while the old media is intact and
  no destination rebuild has completed.
* **M11 Alarming.** A durable record (counter, tree entry) of every declaration with its cause, visible in `show-*`
  commands, so a burst is visible even without log aggregation.

---

## 8. Why none of this is a solution yet

* M1/M2 protect against loss but cost disk space and delay, and do not stop the rebuild from running on false
  declarations (a quarantined store is still taken out of service; the rebuild still consumes failure budget and read
  load across the cluster). A systemic false positive still takes every store offline at once until quarantine is
  released.
* M3/M4 bound the number of simultaneous declarations per pool but do not detect a false positive that is spread out
  over time (a bug that flags one store per restart, across a long rollout), and need a trustworthy, available shared
  counter. That counter lives in the same metadata pools that a burst can damage.
* M5 needs cluster-wide visibility during an event that may take the metadata pools down. A fail-safe default (do
  nothing destructive when the control plane cannot be read) is possible but then a real double failure is not
  repaired while the control plane is unhealthy.
* M6/M7 rely on classifying causes in the code that has the bug. A mis-classifying release defeats them, and a
  version-aware guard is itself new code in the same rollout.
* M8 requires reading the old state with code that may be the buggy part, and an expensive staging rebuild before any
  decision.
* M9 fixes zombies, not false declarations.
* All of the above leave open the fundamental tension: the only independent evidence that a store's local state is
  bad is outside the code that decides it is bad (peers, external tooling, operators). The design must choose what
  authority overrides a local declaration and how long data is kept before that authority is trusted.

**A solution has yet to be identified.** The first step proposed is to write the requirements (S11-S15) as a test
suite that injects a fleet-wide false detector (for example forcing `openCRL` to throw on every device during a
simulated rolling upgrade) and shows what survives; the current system is expected to lose every pool whose
simultaneous discard count exceeds `w - kc`.

---

## 9. Open questions

1. What is the intended retention of `crl.lost-<uuid>` and who may use it? Nothing consumes it today (S7).
2. Should `failStorageDevice` be allowed to proceed when it would leave a pool below `kc` readable stores (it is
   today)?
3. Is `SizingMismatch` ever a real corruption signal, or always configuration drift?
4. Can the bootstrap pool, whose trees live in itself, ever be rebuilt if more than `w - kc` of its stores are
   discarded? (No: rebuild-process FS16.)
5. What distinguishes a transient CRL read failure from damage for the purposes of `handleRecoveryFailed`? Today a
   failed read fail-stops the store and drops its entries durably (D7, X2).
6. Is the daily random-slot technique used for network snapshots (`CLAUDE.md`, NetworkSnapshotReader) a model for
   staggering destructive checks, or only for read load?

---

## 10. Verification notes (adversarial pass)

Each claim in sections 3 to 6 was re-derived from the code.

* **S2, S3, S4, S5, S6, S7, SC1, D1, D2: CONFIRMED.** `openDeviceCrl` catches `Throwable` (`DeviceDiscovery.scala:510-515`);
  `StorageDeviceConfig` keys are optional with code defaults (`:31-40`); `startRebuild` checks the concurrency cap
  before `discardLostStore`, and `rebuildingStores` is filled only afterwards (`StoreRebuilds.scala:167-169, 247`), so one
  pass starts the destructive discard of every marked store on a device; `crl.lost-<uuid>` is created at
  `DeviceDiscovery.scala:518` and referenced by nothing but tests.
* **D4 note corrected.** The adoption-time marker (rebuild-process F13) is REFUTED as a harm: it is derived from the
  disk's own contents.
* **New in-product generators of D2 found by the verification (SC2 is not hypothetical).** crash-recovery-log.md F2
  (the active stream gets another stream's UUID after a recovery that crossed a stream boundary) makes the second
  restart after a rotation throw `BrokenEntryChain` (reproduced, `verify-crl/Exp3.scala`); F4 (phantom live transaction)
  can do the same; F1 (writes truncated at 1024 buffers) corrupts the log without throwing and so loses state
  silently. All three end in D2 on the device and then X3 for every store on it. They need no version skew or fault.
* **Section 6 cross-checks.** rebuild-process F1 (a Failed device keeps running) CONFIRMED in mechanism; F2 (destroy
  first) CONFIRMED; F15 (no escape from an unreadable object) CONFIRMED; F12 (co-location) CONFIRMED.
* **S10** (`failStorageDevice` has no pool-health guard) was not re-read in `BaseAspenClient`; the reconcile side
  confirmed that nothing stops a Failed device's host from continuing to serve (rebuild-process F1).

## 11. Refresh for replay pins (2026-11-09, `0d08e3c6`)

The replay-pins change adds no declaration (D1-D7) or discard (X1-X13) path: `git diff db789ab0..0d08e3c6` touches no
code in `server/host/DeviceDiscovery`, `StoreRebuilds`, `StoreLoader` (beyond the horizon parameter) or `server/crl/`.
The problem statement and sections 3 to 9 stand. Two interactions change:

* **Owed commits now live in a tree, not only in CRLs.** A store's unacknowledged counted commit votes have
  UnackedCommits entries in its pool's metadata group (transaction-recovery T-M2). A correlated CRL loss still destroys
  the votes themselves, so section 2's arithmetic is unchanged; the entries only let repair reconcile the rebuilt stores
  (repair-process 2.3), which needs a read quorum that still holds T.
* **Recovery now depends on the metadata group.** Every recovered unresolved commit vote waits for its UnackedCommits
  lookup before its store drives it (transaction-recovery R-N3). A systemic event that also takes the metadata pool
  below a read quorum leaves recovered votes holding their locks with no driver until it returns: a stall, not a
  declaration, so it does not feed D1-D7.

## 12. Refresh for the replay-pins addendum (2026-11-10, `c181375b`)

The addendum adds no declaration or discard path. It adds one kind of durable state, the applied stub, kept in the
device CRL beside the transactions (crash-recovery-log.md 3.6), and destroyed by exactly the discards that already
destroy CRL state (`dropStore`, a lost CRL, X1-X13 here). Losing stubs in a correlated event costs no data: a rebuilt
store is muted for older transactions, and its owed state comes back through the entries' reconcile (rebuild-process.md
N3 for the clock caveat). A long repair stall, which a systemic event can cause, lets stubs accumulate against each
device's CRL admission (crash-recovery-log.md F22): a stall, not a declaration. Prefer-abort makes a systemic client
outage abort orphaned data-less transactions rather than commit them (paxos-transactions.md P-N8), which is the
intended direction.
