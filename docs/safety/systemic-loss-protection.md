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
(S8, S9). There is no tool for putting retained copies back, only the manual procedure of section 4.5. And a few
exits destroy the retained copies or their transaction state (section 4.3, the destructive rows, and the open
weaknesses).

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

Let a pool have width `w`, write threshold `t` and consistent read threshold `r` (`IDA.consistentRestoreThreshold`:
`w/2+1` for Replication, `restoreThreshold` for ReedSolomon and RandomAccessReedSolomon). Let `d` be the number of
stores of **one pool** that are out of service at the same time because they were declared lost and not yet rebuilt.
A store is out of service from the moment it is marked: a marked directory is never loaded (S4).

* Writes stall once `d > w - t`: Paxos needs `t` stores (`TransactionDriver`'s `Proposer`/`Learner` use
  `ida.writeThreshold`).
* Reads fail once `d > w - r`, including for objects whose slices survive on some live store: a consistent read
  needs `r` stores that agree on the revision.
* A rebuild reads each object through the normal read path, so it needs `w - d >= r`. Before its walk it also asks
  the pool's other stores for pending allocations and waits for `w - t + 1` answers
  (`PendingAllocationSweep.queryPeers`). That count never exceeds `r`, so `r` is the binding constraint. Peers that are themselves being
  rebuilt are silent and do not answer. A query short of that many answers fails its pass after
  `RebuildingStore.PeerQueryTimeout` (60 s), and a walk whose reads time out `MaxConsecutiveReadTimeouts` (100) times
  in a row aborts its pass, each into the host's backoff retry (section 4.2). A pool below `r` therefore holds no
  rebuild slot for long, but its stores stay Rebuilding.
* The same rule applies to the **metadata pool** that holds a pool's AllocationTree, ErrorTree and UnackedCommits
  tree, and to the **bootstrap pool**, which holds the metadata pools' trees, the pool state objects, the
  storage-device state objects and the Radicle. A rebuild reads its pool's state and walks the pool's
  AllocationTree. The flip to Rebuilding (`StoreRebuilds.requestRebuild`) and every device reconcile read the
  device's state object in the bootstrap pool. If the bootstrap pool falls below `r`, no flip, rebuild or reconcile
  can run anywhere, and clients cannot start. The bootstrap pool's own rebuilds read it too, so once more than
  `w - r` of its stores are out of service it cannot be rebuilt at all.
* A transaction whose voters were declared lost is decided by expiry once past the horizon
  ([paxos-transactions.md](paxos-transactions.md) 10.10, `ExpiryTally.decideUnknown`). A declared store not yet
  rebuilt counts as silent, which holds (rule G) only a decision that its vote could change; a rebuilt one counts as
  amnesiac. Once the stores are rebuilt, a changed pool left with `vc + s < r` discards the transaction under rule D,
  with `dataLost` logged when it may have committed. A correlated burst therefore ends in rule-D discards, not in
  split decisions.

A burst of declarations correlated across hosts therefore breaks these inequalities for most pools at once. The
rebuild concurrency cap (`Host` `maxConcurrentRebuilds`, default 2 per host) bounds rebuild work. It does not bound
`d`, because a store leaves service when it is marked, not when its rebuild slot is taken.

---

## 3. Observed properties of the current system

These are facts about the current code. They define the problem; none of them is a goal.

* **S1 Declarations are local and consult no redundancy information.** Every site in section 4.1 decides from files
  on one disk, or from one store's own failure. None of them reads the pool state, the IDA, other devices' state or
  other hosts. The flip (`requestRebuild`) checks only that the device is not Failed, is owned by this host, and has
  the entry Active.
* **S2 One decision can flag a whole device.** `DeviceDiscovery.openDeviceCrl` marks every store directory of a device
  after one failed CRL open, except one whose valid `crl_save.log` is its whole state (`holdsOwnState`). A host restart
  reopens every device's CRL, so it can do this to every device of the host in one event.
* **S3 Pacing applies to rebuild work, per host.** `StoreRebuilds.startRebuild` admits a store only while
  its running rebuilds plus discards (Rebuild work `DiscardingLost`) `< maxConcurrentRebuilds`. A store holds its slot from the start of its
  discard (`discardLostStore`, whose retire saves and drops its CRL entries) and passes it straight to its rebuild in
  the same callback. So at most `maxConcurrentRebuilds` stores per host are being discarded or rebuilt at once. The cap
  is per host, not per pool, and nothing coordinates it across hosts. Retires of copies that are not the owner's (X5,
  and a transfer in's retire of an earlier tenancy's copy) take no slot.
* **S4 Removal from service is immediate; destruction is deferred.** A marked store is never served:
  `StoreLoader.tryLoadStore` leaves a directory with `rebuild-required` offline, and every runtime declaration
  unloads the store before or as it marks it. The discard retires the copy: its CRL state is saved into
  `lost-crl-state.log` and the directory is renamed into `<device>/pending-delete/`. The copy is deleted only once
  the store is Active on its owner device (section 4.4). Nothing checks whether the pool can supply the rebuild
  before the store is taken out of service.
* **S5 Errors are classified coarsely.** The CRL open of a device that holds stores distinguishes these cases.
  `CrlLocked` (another writer) is never lost. `CrlSizingMismatch` refuses the device and never declares it lost, and so
  does `CrlConfigInvalid` (no CRL yet, and the config cannot size one). A transient error (`IOException`,
  `UncheckedIOException` or `VirtualMachineError`, `DeviceDiscovery.isTransient`) is rethrown, leaving the device
  unloaded, until it has failed on `CrlOpenAttempts` (3) device scans in a row. Only a `ScanDevices` open counts
  (`checkForNewDevices(scan = true)`); the host start's opens, every `CheckAllDevices` and a `CheckStorageDevice` naming
  an unloaded device retry it uncounted, and a success anywhere resets the count. The count is per device directory, in
  memory, so it resets at restart. Anything else, including `CrlMissing`, `CrlCorrupt` and `Recovery.BrokenEntryChain`,
  declares at once. A store's save file is classified too: `CrashRecoveryLog.loadValidStoreState` returns
  `InvalidSaveFile` only for a file that is missing, cut short, unparsable or another store's, and throws a read
  error that says nothing of the content (`CrashRecoveryLog.isUnreadable`: an `IOException`, `UncheckedIOException` or
  `VirtualMachineError` other than `NoSuchFileException` or `InvalidProtocolBufferException`). Its callers retry on
  that throw rather than declare: `StoreLoader.handleLoad` holds the load back through `awaitUnreadRetry` (D3),
  `holdsOwnState`, which reads the store config the same way, leaves the device unloaded for the next scan before any
  store is marked (D1/D2), and `adoptionClass` fails the adoption for the next scan (D4). A
  failed CRL read or recovery of one store fail-stops it. So do a commit voter whose apply throws, and a recovered
  commit vote whose object fails to load with anything but ObjectNotFound (`Frontend.failStop`, from `Tx.issueCommits`
  and `Tx.objectLoadFailed`), except on a store whose writes are frozen, which poisons the object instead
  (`Frontend.poisonObject`). Both backends classify that load and report `CorruptedObject` at once only on
  evidence of damage: for RocksDB (`RocksDBBackend.read`) a Corruption status or a value that does not decode, for
  FSIF (`FsifBackend.assemble`, fsif/DESIGN.md 2.6) a CRC mismatch or no valid header, or a backend already
  fail-stopped. Any other failure (an IOError, Busy or TryAgain status, EIO, EMFILE or ENOMEM, a rejected io task, a
  throw while issuing) is retried after each of `RetryScheduler.ReadRetryDelays` (about 1.1 s in all), so only a
  failure that outlasts them declares. Every FSIF error other than ENOSPC in a write, force, checkpoint or compaction
  write, and any internal exception in its serial executor, fail-stops the backend. A compaction move's failed read
  (`Compaction.copied`, a rejected io task included) wrote nothing, so it abandons the move, and no move starts until
  the next of `FsifTuning.moveRetryDelays` (10 ms, 100 ms, 1 s, then 10 s repeating) has passed; it never declares.
  A damaged slot reads successfully and is copied as it is, so its damage is reported by a client read of the copy. Persistent and version causes are handled
  the same way as damage. A commit vote whose apply re-check fails does not declare (section 4.6). The existence
  checks the host makes to decide D1 (`<device>/crl` present, and which children are store directories), D13
  (`config.yaml` at a store path) and D14 (the `incarnation` file) take only a stat that answers absent for absence
  (`server.FileStat`): a failed stat leaves the device unloaded for the next scan (D1; a child whose config cannot be
  stat'd counts as a store), skips the store until a later reconcile (D13), or holds the load back for a retry (D14).
  So do the tests inside the opens they lead to: the CRL open's stream-directory, sizing-file and stream-file tests
  (`CrlDirectory.streamFiles`, `CrlDirectory.recordedSizing`, `StreamReader`) and the backend open's
  (`FsifBackend.open`, `NioFsifFiles.exists`/`size`, `RocksDBBackend.openDatabase`): a failed stat there throws an
  `IOException`, which the device load (D1/D2) or the store load (D5) retries as transient.
* **S6 Configuration cannot change an existing CRL's sizing.** A CRL records its sizing and id in `crl/sizing` when it
  is created and always opens with them (`CrlDirectory.recordedSizing`), whatever the device config says; a config that
  differs is only logged. The device config keys `crl-num-streams` and `crl-max-file-size-mb` are optional and size only
  a new CRL (`CrlSizingDefaults`). Its growth bounds (`crl-max-streams`, `crl-min-free-percent`) apply to an existing
  CRL too, and an invalid one only disables growth for that open (`CrlSizingDefaults.resolveExisting`, which never
  throws). Stream files that disagree with the recorded sizing, stream files without a readable sizing file, and an
  uncommitted growth beside anything but the listed streams throw `CrlSizingMismatch`, which refuses the device. A
  changed default or config loader cannot turn a CRL into a lost one. A changed CRL *format*, recovery check or
  stream-file naming still can (SC1).
* **S7 Retained artefacts are never read by code.** `crl.lost-<uuid>` (D1, D2) is kept forever: no code reads, expires
  or references it. A `pending-delete/` copy is deleted automatically (section 4.4) but is never reopened. No tool
  reinstates a retained copy; section 4.5 is the procedure for doing it by hand.
* **S8 There is no brake and no escalation.** No declaration site has a counter, threshold, circuit breaker,
  operator confirmation or alarm, other than log lines.
* **S9 Every host runs the same detection at the same lifecycle events** (start, device load, adoption, store load,
  close, every commit, every device reconcile), with no staggering and no version awareness.
* **S10 `fail-storage-device` is unguarded by design.** `failStorageDevice` checks only that the device is not
  already Failed, then enrolls `FailedStorageDeviceDurableTask`. That task writes the tombstone (ids zeroed, device
  removed from its set and host) and moves every store to Rebuilding elsewhere. The command is for a drive that has
  physically failed and whose data is already gone. A pool-health guard could save nothing there and would only delay
  rebuilding the data elsewhere, so it has none. A device that may still hold its data goes through
  `shutdownStorageDevice`, which refuses when a pool would drop below its write threshold. Nothing checks that the
  device is no longer served: the command is also the way to move a rebuild off suspect media (rebuild-process 4.1),
  and a host still serving the device keeps answering for its stores until it reconciles the device, which the
  tombstone's single nudge may not trigger for up to an hour: an accepted limitation, not addressed in code
  (rebuild-process B19).

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

The current code satisfies S14 only partly: copies are retained until the store is Active on its owner device, which
is not a retention period (section 4.4). It satisfies none of the others.

---

## 4. Inventory: every path that can declare a store lost or discard state

### 4.1 Declarations (decide that a store is lost)

Each automatic declaration D1-D11 and D14 writes the durable `rebuild-required` marker
(`StoreRebuilds.writeRebuildRequiredMarker`) into the store directory and leaves the store offline. D13 finds no
store to mark: it silences the store and requests the flip directly.

| ID | Trigger | Code | Immediate local effect | False positives |
|---|---|---|---|---|
| D1 | `<device>/crl` absent from, or without stream files on (`CrlMissing`), a device that holds store directories (directories directly under the device holding a store config) | `DeviceDiscovery.openDeviceCrl`, at every load of the device | marker on **every** store directory of the device except one that held nothing in the CRL (`holdsOwnState`): one whose `crl_save.log` validates as its own (an arrived transfer, a transfer out's source, a rebuilt store kept offline), which loads from that file over the fresh CRL as it would over the old one (`StoreLoader.awaitCrlLoad` drops its device-CRL entries and loads the file), or a finished rebuild awaiting its flip (`rebuild-complete`), which has never served (`StoreLoader.tryLoadStore` leaves it offline), so its rebuild flips it, or the Completed Rebuilds pass loads it, as over the old CRL; an existing `crl/` renamed to `crl.lost-<uuid>`; a fresh empty CRL created in `crl.creating/` and renamed to `crl/` durably once finished, then opened. The replacement is sized by the device config as it is now (`CrashRecoveryLogFactory.createCRL`), not by the lost CRL's recorded sizing. A crash or failure of the creation leaves no `crl/` (the next load sweeps `crl.creating/` and runs D1 again, which re-marks idempotently and moves nothing aside). A config that cannot size it fails the creation with `CrlConfigInvalid`, after the stores are marked: the device stays unloaded, retried at each scan, until the config is fixed. A device without room for it stays unloaded (Backlog.txt, "System-level protection against correlated lost store declarations") | a partial restore or mount (one that copies directories but not the large stream files included); a path or layout change in a release; never a read of a store's `crl_save.log` or store config that fails with `isUnreadable` (`holdsOwnState` throws, leaving the device for the next scan before any store is marked), nor a failed stat of `<device>/crl`, which throws and leaves the device for the next scan (`FileStat`). An emptied `crl/` (neither sizing file nor stream files) under a config that cannot size a new CRL is refused instead (`CrlConfigInvalid`, which the open raises before its `CrlMissing` check); an absent `crl/` is declared without an open. A failed stat inside the open is neither: the open's tests are `server.FileStat`'s, so it throws an `IOException`, transient (D2's count), never `CrlMissing` |
| D2 | CRL open throws a non-transient error (`CrlCorrupt`, `BrokenEntryChain`, anything not `isTransient`), or a transient one on the `CrlOpenAttempts`th device scan in a row, on a device that holds stores | `DeviceDiscovery.openDeviceCrl` | as D1 | a CRL format or recovery change; a bug in the CRL writer that the next recovery rejects; a persistent I/O condition (EMFILE in a restart storm, a flaky controller) outlasting two scan periods (about 20 s; opens outside a scan do not count) |
| D3 | `crl_save.log` present but `loadValidStoreState` rejects it (missing, truncated, unparsable, another store's; a read that fails with `isUnreadable` holds the load back through `awaitUnreadRetry` instead), or its state can never fit the device's CRL (`StoreLoader.neverFits`: a transaction above `maxTransactionBytes`, or the whole above the admission limit at the most streams any config allows, `maxAdmissionLimitBytes`, plus that bound: neither follows the device config, so a lowered or invalid `crl-max-streams` declares nothing) | `StoreLoader.handleLoad` then `StoreRebuilds.crlSaveFileLost`/`retireUnserved` | closed unserved, marked, offline, device checked | a save-file format change: every store whose directory holds a save file; a change to `CrlAdmission.recoveredBytes` or the CRL bounds in a release (never-fits); a transfer onto a device with a smaller CRL (Backlog, "Transfers do not check the destination's CRL room"), or a store D1/D2 spared whose save file meets a replacement CRL sized smaller by a changed config. A crash between the save and `deleteTransactions` leaves the same entries in the device CRL, and the retire saves those beside the file (`lost-crl-entries.log`). A state that does not fit *yet*, including one that fits only once the CRL grows, is not declared: the store waits offline, asking the CRL to grow, retried at each device scan |
| D4 | Adoption: an Offline or Active store whose directory has `transferring-out` without a valid save file, or has `rebuild-required` | `DeviceDiscovery.adoptionClass` (`CrlLost`); `DeviceMetadata.adoptDevice` writes the marker in its prepare closure and the entry Rebuilding in the adoption transaction | adopted as Rebuilding | a save-file read path that fails on the adopting host's version (a read failing with `isUnreadable` throws instead, and the adoption is retried at the next scan); it inherits any false mark the previous host wrote. A marker left by an adoption transaction that does not commit is harmless when the classification is right, as the state is lost whatever the outcome; one inherited from the previous host stays a false mark |
| D5 | `BackendStateLost` at backend open: FSIF `StoreLost` (missing directory or blank files, no valid index, journal sequence or corruption checks, replay bounds; `FsifRecovery`, fsif/DESIGN.md 4.1), RocksDB `RocksDBStateLost` (`CURRENT` missing) | `StoreLoader.handleBackendOpened` | marked, offline, device checked | a format or validation change in a release; an incomplete mount or restore; only a known absence: the backends' existence tests are `server.FileStat`'s, so a failed stat of the store directory, an FSIF file or `CURRENT` throws an `IOException`, which the load retries. An open never creates: stores are created atomically (`StoreCreation`), so a store path holds nothing or a complete store, and a missing or blank backend is declared rather than silently served empty |
| D6 | Backend fail-stop while served (`Host.BackendFailed`; FSIF `onFatal`, fsif/DESIGN.md 4.2) | `StoreRebuilds.handleBackendFailed`/`backendFailed` | marked, then `closeUnloaded(dropDurably = false)`: its device-CRL entries stay live until the discard saves them | any I/O error FSIF treats as fatal (a failed compaction move read is not one: it abandons and retries the move); any internal exception in its serial executor; an ENOSPC that the classifier fails to recognize |
| D7 | Store fail-stop (`Host.StoreFailStopped`): its CRL read failed other than by the CRL's own death or shutdown (`CrlFailed`, `CrlClosed`: the device's failure, left to `DeviceShutdown.crlFailed`), or its recovery failed (`StoreRouter.readAndRecover`, `recoverOrFail`), or `Frontend.failStop` (a commit voter whose apply throws, `Tx.issueCommits`; a recovered commit vote whose object fails to load with other than ObjectNotFound on a store whose writes are not frozen, `Tx.objectLoadFailed`; for RocksDB a Corruption status, an undecodable value, or a read failure outlasting `RetryScheduler.ReadRetryDelays`; for FSIF a CRC mismatch, no valid header, or a read failure outlasting the same delays) | `StoreRebuilds.handleFailStopped` then `backendFailed`, only while that Store is still the one installed | as D6 | a decoding change in `TransactionRecoveryState`; a bug in an applier or requirement checker, which fires on **every replica of the object** in the same transaction |
| D8 | Backend failed before it was served: at `LoadStore` (`hasFailed`), opened but not served, or failed after it stopped being served (not replaced, directory on a loaded device) | `StoreLoader.handleLoad` then `retireUnserved`; `StoreLoader.handleBackendOpened`; `StoreRebuilds.backendFailed` | marked, offline | as D6 |
| D9 | Backend fail-stop during a clean close (`ShutdownStore`) | `StoreLoader.handleClose` then `StoreRebuilds.failedOnClose` | marked; a transfer-out close's `crl_save.log` kept, saved as the copy's lost CRL state by its retire (X2) | as D6, and any close-path bug |
| D10 | A directory at the path of a store being rebuilt here that carries neither `rebuild-complete` of the entry's episode nor `rebuild-required` | `StoreRebuilds.retireForeignCopy` | marked (a served one goes through `backendFailed`) | a leftover of an earlier tenancy (a copy finished for an earlier Rebuilding episode included), a copy put back by hand, or a directory a D13 store's restore brought back (D13). A failed stat or read of the path or a marker (`FileStat`, `RebuildingStore.isRebuildCompleteFor`) waits for a later check, so a finished copy is never retired as foreign |
| D11 | A transfer out of a marked store, or one whose close fails with `StoreFailedOnClose`, or one marked while it closed | `StoreTransfersOut.startOut` (`returnLostTransferToRebuild`, `DeviceMetadata.abortLostTransfer`) | transfer aborted; the entry goes back to Active and is then flipped (Offline on a device shutting down, which is not flipped) | inherits D6/D9. A failed stat of the marker refuses the transfer (`FileStat`): before the close it fails the request, after it the store is left closed and unmarked for a retransmitted request, so a marked store is never shipped |
| D12 | Operator `fail-storage-device` | `BaseAspenClient.failStorageDevice`; `FailedStorageDeviceDurableTask` | tombstone; every store Rebuilding elsewhere | not automatic; unguarded by design (S10) |
| D13 | An Active entry on a loaded device with no `config.yaml` at its store path (directory or config gone), not served, opening, loaded or retiring | `DeviceReconciler.reconcileDeviceState` (Missing Stores pass), at every reconcile | silenced and recorded offline, and recorded declared (`LocalStorageDeviceState.declaredMissing`, in memory); `StoreRebuilds.requestRebuild` at once and again at each reconcile while the entry is Active and the store unserved, its directory back or not (no marker: there is nothing to mark). Its device-CRL entries stay. A directory that comes back is not loaded in this process: no pass offers it, so the flip goes ahead, and the rebuild's `retireForeignCopy` (D10) retires it, saving the entries into the copy. Only a restart before the flip commits loads a returned directory, with its entries. The rebuilt copy's empty `crl_save.log` drops what is left at its first load | a partial restore or mount that lacks store directories or configs, the directories arriving later. Never a failed stat of `config.yaml` (`FileStat`): the store is skipped until a stat answers |
| D14 | A store's `incarnation` file known absent at load (every store directory gets one when made: `StoreCreation`, `RebuildingStore.finish`) | `StoreLoader.handleLoad`, before the Store is built | `retireUnserved`: marker, offline, device checked; rebuilt as a new incarnation. A file that cannot be stat'd, read or parsed is never replaced: the load is held back and retried (`awaitUnreadRetry`) | a hand removal or a partial restore of the directory. Not a failed stat, a transient read error or a format change: those wait |

**The flip.** For every offline store of a device that has the marker and an Active entry, the reconcile's Lost CRL
State pass (`DeviceReconciler.reconcileDeviceState`) calls `StoreRebuilds.requestRebuild`. That flips the entry to
Rebuilding in a transaction guarded only by the device state object's `KeyRevision` (S1). The device is then checked
again, and the Rebuilding pass calls `startRebuild`, which retires the copy (X2) and rebuilds as slots allow (S3). A
marker whose entry is Offline (the device is shutting down) is not flipped. D13 requests the same flip from the
Missing Stores pass, repeated at each reconcile until it commits.

### 4.2 Time from detection to removal from service

Removal from service is immediate (S4). Every runtime declaration of a store on a loaded device also requests
`checkStorageDevice`, except a backend found failed after it stopped being served (`StoreRebuilds.backendFailed`, not
served) and one that failed while opened but not served (`StoreLoader.handleBackendOpened`), which wait for the next
check; D1/D2 run inside a device load, whose scan checks the device it loads. So the flip commits within one round
trip of the metadata pools, and the discard follows at the next reconcile that has a free slot. On a healthy control
plane this takes seconds. Destruction (X3) waits for the store to be Active on its owner device, which takes minutes
to hours depending on store size. A failed pass is retried with backoff, from 30 s doubling up to 30 min
(`StoreRebuilds.scheduleRebuildRetry`).

### 4.3 Discards and deletions (destroy state)

Classification: **destructive** means no copy of the destroyed bytes is retained. **Recoverable** means the bytes are
retained, or are a derived copy of data that still exists elsewhere.

| ID | Action | Code | Class | What survives |
|---|---|---|---|---|
| X1 | Device CRL directory renamed to `crl.lost-<uuid>` (D1, D2) | `DeviceDiscovery.openDeviceCrl` | Recoverable by hand; never used or reclaimed by code (S7) | the whole old CRL (votes, Paxos state and pending updates of every store of the device) |
| X2 | Retire: the `rebuild-required` marker durable; the copy's CRL state saved into `lost-crl-state.log` (an existing one kept, else its `crl_save.log` renamed to it, with any device-CRL entries the store still has saved beside it as `lost-crl-entries.log`, else its device-CRL entries read); `CrashRecoveryLog.dropStore` durable; directory renamed to `pending-delete/<store>-<uuid>`, durably. Run for a lost copy before its rebuild (`StoreRebuilds.discardLostStore`), by X5, and by a transfer in for an earlier tenancy's `rebuild-complete` or `rebuild-required` copy at the store's path, before any bytes are asked for (`StoreTransfers.startIn`, `retireEarlierTenancy`; it waits while a rebuild of the store runs here, the store is opening, or a copy is served from that path) | `StoreCopies.retire` | Recoverable | data plus saved CRL state in `pending-delete/` until X3. After D1/D2 the entries are in `crl.lost-<uuid>` instead: the fresh CRL holds none, so only a renamed `crl_save.log` is a state |
| X3 | Release: a pending-delete copy deleted once its store, named by the copy's directory name, is Active on its owner device (this device by its own entry, else the device the pool names, by that device's state). Rename-first through `<device>/deleting/` (`StoreCopies.delete`), whose leftovers are removed at device load and each reconcile | `StoreCopies.release` | **Destructive**, the final step for every declared store | the store's owner copy, whose content came from the peers or the transfer source. After an accepted rebuild loss it lacks the accepted objects: the acceptance is the operator's release (section 4.4) |
| X5 | Reconcile "Deleted Stores" pass: an offline store whose entry is absent from the device's tree state (a completed transfer out, a re-placed rebuild) is retired (X2); it stays offline until its path is clear. A served store the entry does not list (a copy put back by hand) is first taken out of service as a failed backend is (`StoreRebuilds.retireUnlistedServed`) | `DeviceReconciler.reconcileDeviceState` | Recoverable | the copy and its CRL state in `pending-delete/` until X3 |
| X6 | `dropLeftoverCrlState`: device-CRL entries of stores with no directory (a stat that answers absent), no retire running and not loaded, whose entry is absent from the device (a safety net: a retire drops them before the copy leaves its path) | `DeviceReconciler.dropLeftoverCrlState` | **Destructive** for those entries, once no entry on the device names the store | nothing local |
| X7 | `UnloadFailedStore`: entries dropped from the device CRL after a failed `crl.loadStore` | `StoreLoader.handleUnloadFailed` | Recoverable | the save file holds the full state |
| X8 | Entries deleted from the device CRL after a transfer out's save file is durable; for a closed store that already has a valid save file, its device-CRL entries dropped (`dropStore`) and the file shipped unchanged | `StoreLoader.handleClose`, `StoreTransfersOut.extractUnloaded` (on the transfer's source device only) | Recoverable (moved); the dropped entries are derived (a copy of part of the file, or an earlier tenancy's leftover). Whether the directory and a save file are there is taken from `FileStat`: a stat that fails fails the extraction, which a retransmitted request retries, rather than write the entries over a valid file | the save file, which a retire keeps as the copy's lost CRL state |
| X9 | Derived copies deleted (copies that never were the owner's): rebuilt copy on `RebuildOutcome.Discard` and an arrived copy on `TransferOutcome.SourceRestored` or `Aborted` (`LocalDevices.discardStoreDirectory`, via `StoreTransfers.discardArrivedTransferIn` for the latter), and a promoted copy whose pool update has not committed and which is stale (its `transferred-in` marker names another transfer) or unusable (marked `rebuild-required`, or its save file no longer validates) (`StoreTransfers.startIn`), all rename-first; rebuild staging (first pass `createStaging`, staging `BackendStateLost`, a finished rebuild's `cleanupStaging`, `abandonUnlisted`); transfer staging: a staged copy with an unusable save file, one landed after its attempt was dropped, an abandoned one (`StoreTransfers.startIn`, `dropAbandonedTransferIn`), a receive's clearing of its staging path and a partial receive (`StoreTransferManager.receive`, `removeOwnedDirectory`); `transferring-in` staging and `creating/` at device load (`DeviceDiscovery.loadOwnedDevice`) | as listed | Derived. `StoreCopies.delete` reports gone only a copy known absent (`FileStat`): a failed stat is a failed delete, and the promoted-copy case waits for a later check | peers, or the transfer source |
| X10 | Tombstone: device removed from its set and host, ids zeroed, stores rebuilt elsewhere | `FailedStorageDeviceDurableTask` | One-way **metadata** change; the media is untouched | the physical data. The old host retires the device under a durable `failed` marker (`DeviceShutdown.retireFailedDevice`, also written by `DeviceDiscovery.handleOwnershipResolved`), and `tryLoadDevice` does not load or claim a device with it. That test is `DeviceMarkers.isFailed` (`FileStat`): a failed stat throws and the scan leaves the device unloaded, as with the marker present |
| X11 | CRL stream files removed: by a growth that fails at runtime, its own `<n>.crl.tmp` and unlisted `<n>.crl` (`SimpleCRL.attemptGrowth`); when the CRL is opened, an uncommitted growth's `<n>.crl` or `<n>.crl.tmp` (`CrlDirectory.removeUncommittedGrowth`), and an unfinished creation: when every remaining `*.crl.tmp` is a listed stream's `<n>.crl.tmp` with no `<n>.crl` beside it, and the device holds no stores, every `*.crl.tmp` and every stream file, the CRL then created afresh (`CrlDirectory.createStreamsIfNew`). Any other `*.crl.tmp` is `CrlSizingMismatch` and nothing is removed. The creation step that follows runs only when `CrlDirectory.streamFiles` knows of no stream file (a failed stat throws, `FileStat`) and never on a device holding stores (`CrlMissing` instead), so its renames never replace a stream file | `SimpleCRL.apply` | Derived: neither holds an entry. Leftovers are taken for an unfinished creation only on a device holding no store directory; the fresh CRL of D1/D2 is created apart, in `crl.creating/`, whose leftovers device load and each creation remove (`DeviceDiscovery.openDeviceCrl`, `loadOwnedDevice`) | nothing; no store is marked |
| X12 | Load from a save file: the store's device-CRL entries dropped, durably, before its `crl_save.log` state is saved into the device CRL; the file removed, durably, once the CRL holds it and before the store recovers | `StoreLoader.awaitCrlLoad`, `StoreRouter.crlLoaded` | Derived: the file is the store's whole state (it has served nowhere since it was written), so the dropped entries are a copy of part of it or an earlier tenancy's leftover | the file, then its entries in the device CRL. A failed save keeps the file (X7); a store closed before the removal keeps it too, and its next load repeats the drop. A failed removal unloads the store with its entries kept and retries its load, which removes the file durably, forcing the directory even if the file is gone, before it recovers |
| X13 | A store's `incarnation` file replaced by a fresh random one | `RebuildingStore.finish` only (`StoreIncarnationFile.writeFresh`, in staging); a load never writes it (`StoreIncarnationFile.load` throws for a missing, unreadable or unparsable file) | the rebuilt copy is a new incarnation, as it must be | nothing |

### 4.4 Retention

A store has one owner: the device its pool names. Ownership moves by a transfer or a rebuild and takes effect when the
new device's entry becomes Active. Every other copy of the store may then go, and only then. So every store declared
lost through D1-D11 or D14 keeps its last local copy until the store is Active on its owner device. The marked directory stays
at its path, offline, until its retire (X2) moves it, with its saved CRL state, into `pending-delete/`. The same holds
for a copy whose rebuild was re-placed onto another device and for a transfer out's source, both retired by X5, and
for an earlier tenancy's copy at the path of a store transferring back onto the device, retired by the transfer in. A
rebuild that cannot finish (too few readable peers, an unreadable object, out of space) leaves the store Rebuilding, so
the copy is kept for as long as that lasts. A failed lookup in `StoreCopies.release` also keeps it, as does a copy whose
name identifies no store. A device tombstoned by `fail-storage-device` (X10) is never loaded again, so nothing on it,
`pending-delete/` included, is ever released. Deletes are rename-first, so an interrupted delete leaves nothing partial
at a store path or in `pending-delete/`. For a correlated false positive, this turns data loss into an outage: pools
fall below `r`, rebuilds stall on unreadable objects, and the copies stay on disk.

What is not retained, or not for long:

* The copy is deleted at the first reconcile after the store is Active on its owner device (X3). That can be minutes
  after the declaration, which is short compared with a rollout. It does not wait for repair or for any check that the
  declaration was isolated. Nor does it look at the owner copy itself: an Active entry is enough, even when that copy
  has since been marked `rebuild-required` and its flip is still pending, so the reconcile whose Lost CRL State pass
  requests that flip may release the older copy in its Pending Deletes pass.
* An accepted rebuild loss (`accept-rebuild-loss`) ends with the store Active, so the old copy goes too, with any
  slices of the accepted objects it held. The acceptance is the operator's release, and the command says so.
* `crl.lost-<uuid>` is retained, but nothing ties it to the pending-delete copies whose entries it holds, and nothing
  reads it.
* A D13 store with no directory at its path keeps its device-CRL entries only while an entry on the device names it.
  The rebuilt copy's first load on the device drops them (its empty `crl_save.log`), and a re-placement that moves
  the entry off the device leaves them to X6, both without any check that the declaration was isolated. A directory
  that reappears, before the flip or during the rebuild, is retired by D10, which saves them into the copy.
* No code path reinstates a retained copy. Section 4.5 gives the manual procedure and its preconditions.

### 4.5 Manual reinstatement of a falsely declared store

**Why it can be safe.** While a declared store is out of service, the rest of the system sees it only as silent:
its marked copy is never served, and a rebuild's copy is not served before its flip (the `rebuild-complete` marker
keeps it offline while the entry is Rebuilding). Paxos is safe with an offline acceptor, an expiry tally counts a
silent store as possibly holding anything (rule G), and its missed updates are marked in the ErrorTree. Putting
back **exactly the copy and CRL state the store last held**, under the same incarnation, is therefore the same as a
long outage ended by a restart, which crash-only operation already handles. The load records a new load time, so the
forget floor keeps it out of every transaction it does not hold that started before its return. Its recovered
transactions decide as any held across an outage do: inside the horizon by Paxos, past it by expiry, and its
recovered commit votes apply again or look up their UnackedCommits entries, which name its incarnation. Repair then
brings it up to date.

**Preconditions.** All must hold, or the store must be left to its rebuild:

1. **No other incarnation ever served.** The store's device entry is still Rebuilding under the episode its
   declaration started, the pool still names this device, and no loss was accepted (`acceptLoss`). An entry that
   has flipped Active, or been re-placed (rebuild-process 4.2, R4), means a rebuilt copy may have joined
   transactions. Two incarnations' states are then both live, and the old one must never return.
2. **The data copy is whole.** It is the copy that was declared, from its path or from
   `pending-delete/<store-dir>-<uuid>`, untouched since. Its `incarnation` file must be the copy's own.
3. **Its complete CRL state is in hand**, and the software that will load it can read it (the fixed release, when the
   declaration came from a bug):
   - D1/D2 (CRL lost): the store's entries are only in `crl.lost-<uuid>`. The fresh CRL holds none, so the retire's
     `lost-crl-state.log`, read from it, is empty and must not be used. The entries must be read out of the old CRL
     by code that can open it. If no release can, the state is gone: not reinstatable. The exception is a store
     whose directory held a valid `crl_save.log` when it was marked (an arrived transfer, a transfer out's source, a
     rebuilt store kept offline): that file was its whole state, and the retire renamed it to `lost-crl-state.log`,
     which is then the state.
   - D3 (save file rejected): the rejected `crl_save.log` itself, now in `lost-crl-state.log`, read by a release that
     parses it. If it is truly damaged, the state is gone, unless a crash before the extraction's deletion left the
     entries in the device CRL too: the retire then saved them as `lost-crl-entries.log`, which is the state. That
     holds only on the device whose close wrote the file (a transfer out's source). On any other device (an arrived
     transfer) the device CRL never held the file's entries, so a `lost-crl-entries.log` there is an earlier
     tenancy's leftover (`StoreLoader.awaitCrlLoad` drops such entries for the same reason) and must never be used.
   - D5, D6, D7, D8, D9 (backend refused, or a fail-stop): `lost-crl-state.log`, saved by the retire from the device
     CRL, or from the transfer-out save file for D9. A fail-stop keeps the store's entries live until that save, so a
     commit vote whose apply failed is in it with its update. A store fail-stopped for D6/D7 may really lack an update
     it voted to commit, and that vote's state is what re-applies it at recovery, so the file must not be thinned.
     D5 needs a release whose backend opens the files: the backend refusing them is the declaration.
   - D13 (no store at the path): a directory that comes back is retired by D10, which saves its entries into
     `lost-crl-state.log`. While none is back, the entries are still in the device CRL, until the rebuilt copy's first
     load drops them or a re-placement leaves them to X6; they must be read out of the device CRL before either, by
     code that can (none exists). After either, the state is gone: not reinstatable.
   - D14 (incarnation file absent): the copy's identity is gone, so precondition 2 cannot hold: not reinstatable.
4. **The cause is gone.** The fixed release is running, or the transient condition (a mount, a restore) is corrected.
   Otherwise the store is declared again at load, which is harmless but pointless.

**Steps.** With the device's host stopped (crash-only: killing it is fine), so no reconcile acts in between:

1. Write the device's tree entry back to Active, as a transaction that requires the entry to be unchanged (still
   Rebuilding under the declaration's episode). If it fails, precondition 1 may no longer hold: start over.
   Once the entry is not Rebuilding, the reconcile abandons the rebuild (`StoreRebuilds.abandonUnlisted`) and
   deletes its `rebuilding/<id>` staging copy.
2. At the store's path, remove a finished rebuild's copy (one carrying `rebuild-complete`). It never served, so it is
   derived, and the path must be clear.
3. Put the declared copy at the store's path. Write its complete CRL state (precondition 3) as the copy's
   `crl_save.log`, in the save-file format `CrashRecoveryLog.saveStoreState` writes (`lost-crl-state.log` already is
   one), and remove `lost-crl-state.log`, `lost-crl-entries.log` and the `rebuild-required` marker, in that order. A
   crash between leaves the marker, which keeps the copy unserved. A `lost-crl-entries.log` left behind would be kept in
   place of fresh entries by the store's next retire (`StoreCopies.retire` writes it only when absent). A
   `transferring-out` marker the copy still carries (D4, or D1/D2 over a transfer out's source) may stay: the
   reconcile's Abandoned Transfers Out pass removes it and loads the store once its entry is Active with no transfer
   named.
4. Start the host. The load reads `crl_save.log` as the store's whole state: it drops whatever the device CRL holds
   for the store, then saves the file (`StoreLoader.awaitCrlLoad`, crash-recovery-log.md 3.3), so leftovers of the
   store in the device CRL are never merged in. Then the store recovers and serves.

After D1/D2 every store of the device was marked: each is reinstated by these steps, its own entries read out of the
one `crl.lost-<uuid>`, which stays until all are done or rebuilt.

**Before the flip.** A declared store whose entry is still Active (the flip has not committed, for example while the
metadata pools are unreachable) or Offline (its device is shutting down, so it is never flipped) has not been retired:
the copy is at its path, and its CRL state is where the declaration left it: the device CRL for D5-D8, the device CRL
or, after a transfer out's close, the copy's `crl_save.log` for D9, its rejected `crl_save.log` for D3,
`crl.lost-<uuid>` for D1/D2 (or the copy's own valid `crl_save.log`). Preconditions 2-4 apply. With the host stopped,
the entry is left as it is, the state is put in place as in step 3 (where it is already in the device CRL or a valid
save file, only the marker is removed), and the host is started. D4 and D10 have no such window: their entry is already
Rebuilding.

**Not implemented:** nothing reads a store's entries out of `crl.lost-<uuid>` (S7), and no command performs these
steps or checks the preconditions. Both are part of the Backlog item.

### 4.6 Conditions that declare nothing

These look like loss, or take a store or device out of service, but write no marker and destroy nothing:

* **A failed apply re-check** (`Tx.issueCommits`). A live commit voter's failed re-check on a changed object is an
  invariant violation: logged at error, skipped and reported, so repair restores the object. A recovered commit vote
  takes a failed re-check as an update already applied (transaction-requirements.md 8.5). Neither fail-stops.
* **Failed backend commit and poisoned objects** (`Frontend.commitFailed`, `poisonObject`): every later transaction
  on the object votes abort until the store reloads. A recovered commit vote whose object fails to load on a
  write-frozen store poisons it instead of fail-stopping the store.
* **Out of space, and any other failed backend open.** ENOSPC freezes writes (FSIF) and keeps the store serving. At
  load, every open failure other than `BackendStateLost` (out of space, an I/O, lock or resource error) leaves the
  store offline and unmarked, retried at device scans with backoff doubling up to `Host.LoadRetryCap` (5 min)
  (its copy AwaitingRetry, holding a `LoadRetry`). So does a load that throws before any open starts (an unreadable
  store config, `StoreLoader.awaitUnreadRetry`) and a `LoadStore` that finds the store's executor slot still held
  (`HostThreading.AlreadyAssigned`). A failed create that left nothing at the path is created again by the next
  device check instead. A `crl_save.log` state that does not fit the device's CRL admission yet waits in the
  same retry, at every scan, and asks the CRL to grow.
* **CRL refusals.** `CrlLocked`, `CrlSizingMismatch` and `CrlConfigInvalid` leave the device unloaded and retried (S5);
  a transient open failure does too, until it has failed on `CrlOpenAttempts` (3) device scans in a row. A failing CRL
  open on a device holding no store directory is rethrown, uncounted: nothing is marked.
* **A failed CRL at runtime** (`CrashRecoveryLog.failed`, `DeviceShutdown.crlFailed`): the device's stores are closed
  without extraction and the device is unloaded until the host restarts, its files untouched (SC2).
* **A failed `crl.loadStore`** of a valid save file (`UnloadFailedStore`, X7): offline, the file kept, not marked,
  and not retried in this process. It fails only with the CRL (`SimpleCRL.tracked`: `CrlFailed`, or `CrlClosed` for
  a device unloading), so the whole device is out of service with it and the store loads at the device's next load.
* **A failed Store build** in `StoreLoader.handleLoad` (the `Store` constructor, the object cache or the backend's
  completion handler throws, for example with EMFILE): `abandonLoad` closes the backend and leaves the store offline
  and unmarked, and `awaitBuildRetry` leaves its copy AwaitingRetry, so each device scan retries it with backoff
  (1, 2, 4 ... scans per build failure in a row, capped at `LoadRetryCap`, logged at error once per cap interval), as
  a failed open is. A new store's failed `created` promise fails `createNewStore`'s flip; the next check finds the
  directory and flips the entry Active, and the retry then serves it. A failed build of a reload over a served store
  leaves that store serving and starts no retry. (The `incarnation` file is read before the build: one that
  cannot be stat'd, read or parsed waits AwaitingRetry, and one known absent is D14.)
* **A transferred copy with an unusable save file** is not promoted (X9) and the transfer restarts; the source still
  holds the state. The same holds for a promoted copy found lost before its pool update commits: a resume
  (`StoreTransfers.startIn`, the copy behind its `transferred-in` marker and the entry still TransferringIn) first
  checks the copy, and one marked `rebuild-required` (a D1/D2 declaration whose `holdsOwnState` read failed) or whose
  save file no longer validates (`CrashRecoveryLog.loadValidStoreState`) is deleted (`StoreCopies.delete`) and the
  transfer starts over, so the pool is never repointed onto a lost copy and the source's intact copy never released
  for it.
* **A transfer out's extraction that finds an unusable save file, or cannot stat it or the store directory**
  (`StoreTransfersOut.extractUnloaded`, `FileStat`) fails the close; nothing is written over the file, and the
  retransmitted request retries.
* **A failed staging store** during a rebuild (fail-stop, or `BackendStateLost` at its open) fails or restarts the
  pass; the live store is already Rebuilding.

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
* **SC5 Resource exhaustion at start** (file descriptors, memory, threads in a restart storm). The CRL open declares on
  the third device scan in a row whose open fails with an `isTransient` error. The host start's opens (the
  constructor's discovery and the `CheckAllDevices` posted right after it) and nudges do not count, so the first
  counted failure is at the first 10 s scan and the declaration at the third: a condition must outlast about 20 s, two
  scan periods, to be declared. A save-file or store-config read that fails with `isUnreadable` is retried, never declared, and an
  FSIF compaction move's failed read, a rejected io task included, abandons the move and retries it after a backoff.
  A failed stat inside the CRL open or a backend open is an `IOException` like any other: retried, and declared only
  as the CRL open's counted transient failures are.
* **SC6 Incomplete mount or restored backup.** `crl/` absent, or present without stream files, while store
  directories exist is D1, with no retry. Absent FSIF files or a missing RocksDB `CURRENT` is D5. Store directories
  or configs missing beside Active entries are D13 at the first reconcile; their device-CRL entries stay. A directory
  the restore brings back is not loaded while the host runs: its rebuild is requested at each reconcile, and the
  rebuild retires the returned copy with its state saved into it. Only a host restart before the flip commits loads it
  whole.
* **SC7 Operator error.** `fail-storage-device` on the wrong device, or on two devices of one pool. It is one-way and
  unguarded by design (S10).
* **SC8 Rolling restart faster than rebuild.** Even a correct system under a legitimate fault pattern is exposed. If
  any check has a false-positive rate p per store, the expected number of stores of one pool out of service at once
  grows with the restart rate and the rebuild duration (hours for a large store, with failed passes retried after up
  to 30 min).
* **SC9 Incarnation file format or read path change.** A release that writes or reads the `incarnation` file
  differently (`StoreIncarnationFile.load` parses it as a bare UUID) leaves every store of every upgraded host offline,
  retried at each scan and logged at error once per retry cap, until the format is fixed: an outage, never a fresh
  incarnation over a store's data and CRL state (D14 only for a file known absent).

---

## 6. Interaction with other subsystems

* **Rebuild** ([rebuild-process.md](rebuild-process.md)). A rebuilt store is a new incarnation whose Paxos acceptor
  state covers only transactions started after its `acceptor-since`. So losing the old CRL state cannot let a rebuilt
  store vote inconsistently. A transaction that needed the lost votes is decided by expiry, counting the rebuilt
  store as amnesiac: it holds, commits by the votes that remain, or is discarded (rule D when its update can no
  longer be restored), never split (section 2). A rebuild that
  cannot read an object fails its pass and keeps the store Rebuilding, which keeps it out of the pool's failure
  budget indefinitely, until `accept-rebuild-loss` (section 4.4). Placement may co-locate a pool's stores on one device
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
  UnackedCommits lookup answers. A systemic event that also takes the metadata pool below `r` leaves such a vote
  holding its locks until the pool returns. That is a stall, not a declaration.
* **Applied stubs** ([crash-recovery-log.md](crash-recovery-log.md)). Stubs live in the device CRL beside the
  transactions and go with them in X1, X2, X6 and X7, and move with them in X8 and X12. Losing them with a rebuild costs
  no data: a rebuilt store is muted for older transactions. A long repair stall, which a systemic event can cause, lets
  stubs accumulate against each device's CRL admission. That is a stall, not a declaration.
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
  Rebuilding, Offline or marked, plus this one, exceeds a budget derived from the IDA (for example `w - t`). The
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
code, every pool whose number of simultaneously marked stores exceeds `w - r` is expected to become unreadable,
with its copies retained in `pending-delete/` or at their paths, and nothing to bring them back.

---

## 9. Open questions

* **Q1** What is the intended retention of `crl.lost-<uuid>`, and who may use it (S7)?
* **Q3** What distinguishes a transient CRL read failure from damage for `StoreRouter.readAndRecover`? The CRL's
  death or shutdown (`CrlFailed`, `CrlClosed`) is the device's failure and leaves the store unmarked; any other
  failure fail-stops the store (D7). SimpleCRL reads from memory, so it has no other failure.
* **Q4** Is the daily random-slot technique used for network snapshots (`NetworkSnapshotReader`) a model for
  staggering destructive checks, or only for read load?

---

## Open weaknesses

(none)
