### Code organization
- `Host` is a thin shell. `DeviceControl` wires the control-thread components, each a lazy val taking the
  `DeviceControl` (`ctx`) and importing its control context: `DeviceDiscovery` (device scan, load, adoption, device
  CRL open), `DeviceReconciler` (device checks: `reconcileDeviceState` dispatches one method per pass over the
  tree's entries for the device), `DeviceShutdown` (shutdown, relinquish, CRL failure, Failed retirement),
  `DeviceMetadata` (the tree and pool transactions), `StoreLoader` (loads, opens, closes, load retries), `StoreRebuilds`
  (rebuilds, backend failures), `StoreTransfers` (the receiving end of a transfer), `StoreTransfersOut` (the sending
  end: close with CRL extraction, answering a receiver's request, reinstating an abandoned transfer), `StoreCopies`
  (retire, release and rename-first delete of store copies) and `LocalDevices` (the loaded devices). `StoreRouter`,
  `StoreThreadPool`/`StoreMailbox`/`StoreBalancer` and `HostStoreView` are the store-side and cross-thread pieces
- `StoreDirMarkers` is the single source for the marker files a store directory carries (`rebuild-required`,
  `transferred-in`, `transferring-out`, `rebuild-complete`). Its tests are FileStat's: `has` throws on a failed stat,
  `knownPresent` answers false for a pass that waits for a later one
- `CopyPhase` (`Phase`: Live, Arrived, Outbound, Rebuilt, Lost) is the one reader of a copy's markers and the one
  place that ranks them: RR > TO > TI > RC, first present wins (`read` throws a failed stat or read, `known`
  answers None for a pass that waits). `tryLoadStore`, `adoptionClass` (Outbound needs a valid save file, else
  CrlLost), `holdsOwnState` (Rebuilt), `reloadClosedStores` (offers Live, and a copy whose phase cannot be read) and
  `startIn` (Arrived: stale check; Lost, Rebuilt and Outbound: `retireEarlierTenancy`; Live: refuse or receive; None:
  wait) match on it. The single-marker tests of the reconciler passes, `retire` and the rebuilds stay `StoreDirMarkers`
- Copy registry (store copy state machine, stage 1, shadow mode): `HostCopies` (`DeviceControl.copyRegistry`) holds
  one `DeviceCopies` per loaded device, the device state's `LocalStorageDeviceState.copies`. `attach` at device load
  (before any store is offered; `ControlFixture.injectLoadedDevice` too) registers every store directory, by name,
  with its `CopyPhase` (`Unread` where it cannot be read); `detach` at unload. Every disk mutation site of a store
  path (create, rebuild finish's handler, transfer-in promotion, the four markers' writes and removals, retire into
  pending-delete, discards) updates it right after its durable step, through `HostCopies.shadow`, which logs and
  records a throw instead of failing production. Nothing reads it yet. Activity and work are mirrored from the old
  sets (spec mapping table), also through `shadow`: `startOpen` sets Opening (a create's record, registered at its
  open's outcome, too), an open's unserved end Idle, each `storesAwaitingRetry` write AwaitingRetry and a dropped
  retry Idle; `StoreRouter.install` Served (Served -> Served for a reload) and `remove` Closing, its purpose from
  `closingFor` (DeviceShutdown: Shutdown / Relinquish; `handleClose` with `extractCrl`: TransferOut; else FailStop),
  until `handleClose`/`closeUnloaded` end it (Idle, or AwaitingRetry when the loader holds a retry); `StoreCopies.retire`
  Retiring. Work is synced from each owner's sets at every write: Create (`creatingStores`), Rebuild
  (`silenced` Queued, `droppingCrlState` DiscardingLost, `rebuildDevices` Running / Abandoned), TransferIn
  (`transferringIn` Receiving, `recordingTransfersIn` Recording, `abandoningTransferIns` Abandoning,
  `retireEarlierTenancy` RetiringEarlierTenancy), TransferOut (`closingForTransfer`, `abortingLostTransfers`); a second
  Work for a (device, store) is a shadow error (I3), so `startRebuild` does not even queue (silence) a store a transfer
  out is closing. `declaredMissing` is mirrored as `missing`. An open during TransferOut work is not refused yet (R1,
  Task 7): its Opening is skipped and listed (`opensDuringTransferOut`). `silence` derives the router's answer
  (Served if any copy is Served; else Silent with its reasons: a record, Opening, Rebuild / TransferIn / Create work,
  missing, an unloaded device's entries, awaiting device; else Unknown) and `repairable` (Served, not Closing, no
  Rebuild or TransferOut work, not being sent). With
  `Host.Seams.verifyCopyRegistry` (TestNetwork; ControlFixture's `newHost(verifyCopyRegistry = ...)`, default off)
  each lockstep drain throws on a recorded throw, on any `verifyAgainstDisk` mismatch (phase, unrecorded config'd
  directory, record without directory) or on any `compareWithRouter` disagreement (silence or repairability against
  the router's marks), except for the tests `HostCopies.KnownDefects` lists, whose disagreement is a defect of the old
  marks (spec, Step 3 addenda D1-D3). A ControlFixture host so checked holds each load its recording seam skips
  offline (`RecordingSeams.holdLoadsOffline`), as a production load holding a copy back does. `attach` never throws: a child whose stat fails is registered `Unread`, and
  so is one whose markers cannot be read. `tryLoadStore` reports each phase it reads (`readAtLoad`): an `Unread`
  record takes it, a copy with no record (put at its path after the device's load, by hand or by a test) is
  registered and listed in `learned` (reported by the drain check, not failed on), and any other disagreement is a
  shadow error. Stores loaded onto a device this host has not loaded, through `Host.Seams.unloadedDeviceCrl`
  (TestNetwork), get an untracked record (Live) at install, Served there and Closing then Idle once the router
  removes the store, kept as a closed copy's record is kept on a loaded device, so the store stays silent;
  `HostCopies.device` returns them like any other, and the disk check skips them. The drain check can land between
  an off-thread step and its control-thread handler (a rebuild's copy placed before its completion callback runs),
  so a test does not drain while such a step is in flight
- `DeviceControl.markStoreOffline` keeps the router's offline mark and the device's
  `offlineStores` together; `LocalDevices.isLoaded(sds)` says whether a device state is still the loaded one

### Host threading model
- `HostThreading` decides where a host's work runs. It is a `Host` constructor parameter, chosen in
  code. The default strategy, which `Main` passes, is `StoreThreadPool`: each store's executor is its own
  `StoreMailbox` (a FIFO with at most one drain at a time, so the store's tasks never overlap and keep their order),
  scheduled onto one of the `aspen-store-N` daemon threads. By default the pool is fixed: up to
  `aspen host --store-threads` threads (default a quarter of the CPUs, at most 4), created lazily, each new store on
  the thread with the fewest stores; stores never move. Fewer busy threads beat many idle ones: waking a parked thread
  for each task multiplied CPU per operation, and fixed pools of 2 to 4 threads beat one per CPU and both adaptive
  policies tried (docs/host-threading-measurement.md). `StoreThreadPool.Adaptive` (code only, parked until revisited)
  runs `StoreBalancer` instead: it packs stores by load, grows on drain-wait congestion and moves stores between threads;
  a move only changes where the mailbox's next drain is queued. Its known problems are in `StoreThreadPool`'s scaladoc
  and `Todo.txt`
- The hot path (network messages, I/O completions, heartbeats, repairs) goes through `StoreRouter` straight to the
  store's executor: a concurrent map lookup on the calling thread, then the task, which runs only if that store is
  not closed when the task runs (tasks queued ahead of a close still run). Unknown stores are answered from the
  calling thread; offline ones are silent, as are all unknown ones until the startup scan's LoadStores are handled.
  A store whose load is under way is silent too: `loading` from `StoreLoader.startOpen` until install, and
  `awaitingDevice` while its device's shutdown-marker lookup (or adoption) decides. Neither mark is `offline`.
  The router's `offline` mark is by store id, but two devices of one host can each hold a copy of a store (a transfer
  between them, an earlier tenancy's copy at the destination's path): `tryLoadStore` keeps a marked copy offline on
  its device without silencing a store served or opening here (`keepMarkedCopyOffline`), as does a rebuild's
  unpromoted copy (`StoreRebuilds.keepUnpromotedCopyOffline`; both through `DeviceControl.markCopyOffline`, never
  `markStoreOffline`, which marks unconditionally), and a pass that removes a
  copy clears the mark only if no other loaded device holds the store offline and no Rebuilding entry here silences
  it (`StoreRebuilds.silencedStores`: a rebuild's destination never held the store, so lists it in no
  `offlineStores`; `StoreRouter.clearOfflineUnlessHeld`)
- Backend opens are backend-neutral: `BackendOpener.open` (`server.store.backend`) returns a Future and decides where
  the open runs (RocksDB inline, FSIF on `FsifOpenEc`); `StoreLoader.startOpen` handles an already-completed one
  inline and otherwise posts `BackendOpened`. A store stays opening (`StoreLoader.isOpening`) from `startOpen` until
  its open fails or the LoadStore `serveOpened` queued is handled (`handleLoad` installs it or gives up in that one
  task), so a second load offer in between (a duplicate `LoadStoreById`) starts no second open, and a ShutdownStore
  then waits for the install and closes the served store. A rebuild's staging store opens through the same opener (passed by
  `StoreRebuildFactory.createRebuild`). The host and the rebuild see only `Backend.hasFailed`/`isWriteFrozen`,
  `BackendStateLost` and `Backend.isOutOfSpace`; only `DefaultBackendOpener` names concrete backends
- Store creation is atomic (`BackendOpener.create`, `server.store.backend.StoreCreation`,
  systemic-loss-protection 4.1): `startOpen` with `create` (only `StoreRebuilds.createNewStore`) builds the
  store in `<device>/creating/` and renames it to its path before opening it, so a store directory exists only
  complete and `open` never creates. The create's work (`creatingStores`) ends when its copy is served, its open
  fails or the create is skipped, not at the flip out of Initializing, which touches no copy: once the flip commits,
  a rebuild or transfer out of the served store may start, and must not overlap the create (I3). The pending flip
  is tracked apart (`pendingCreateFlips`), so a check meanwhile does not create the store again.
  A failed create, out of space included, leaves nothing at the path and is not put in `storesAwaitingRetry`: its
  entry stays Initializing and the next device check creates it again. Any other failed open but a lost state (out of
  space, an I/O, lock or resource error) leaves the store offline, unmarked, and in `storesAwaitingRetry`: each
  device scan retries it with backoff (`StoreLoader.retryFailedLoads`, 1, 2, 4 ... scans, capped at
  `Host.LoadRetryCap` 5 min), so an Active store is never left unserved and untracked until a restart. A load that
  throws before its open (an unreadable `config.yaml`, `StoreLoader.awaitUnreadRetry`), one whose executor slot
  an earlier close still holds (`HostThreading.AlreadyAssigned`) and one whose Store fails to build in `handleLoad`
  (`awaitBuildRetry`, backoff per build failure in a row, kept in `buildRetryWaits` until an install) are left
  offline and retried the same way. `tryLoadStore` reads no tree, so a retry is held to the entries the device's
  latest reconcile read (`LocalStorageDeviceState.reconciledEntries`, set by `reconcileDeviceState`) and to the
  host's own work: it waits while that entry is neither Active nor Offline, the store is `declaredMissing`,
  `StoreTransfersOut.isClosingForTransfer` (from `startOut`'s close until its marker task has run) or
  `StoreRebuilds.isRebuilding`, and is dropped once the entry has left the device (before a first reconcile only the
  host-work checks apply). A retry
  keeps the waiting load's `onServed` (e.g. its `StoreMoved` gossip) for the open that serves it. A load's `onServed`
  runs only once its LoadStore has installed and recovered the store (`StoreLoader.awaitServed`), never at backend
  open; a LoadStore that leaves the store awaiting a retry (no CRL room, no executor slot, a failed save-file removal,
  a failed build) hands the callback to that entry, and any other failure drops it. An unreadable config
  is logged at error once per retry cap. An Active entry on a loaded device with no `config.yaml` at its store path
  (directory or config lost), not served, opening or retiring, is rebuilt by the reconcile's Missing Stores pass:
  marked offline, recorded in the device's `declaredMissing`, and `StoreRebuilds.requestRebuild`; the request is made
  again at each reconcile while the entry stays Active and the store unserved, even once its directory is back, so a
  failed request never leaves it offline until a restart. Its device-CRL entries stay (nothing durable records the
  declaration, and the directory may come back before the flip commits): the rebuild's retire of a copy that is back
  saves them, the rebuilt copy's empty `crl_save.log` drops what is left, and a re-placement's entry leaving the
  device makes them leftovers. Every load path
  starts from a directory, so nothing else would notice it while the pool counted it writable.
  `DeviceDiscovery` deletes `creating/` when it loads a device. Bootstrap (`cmdline`) creates its stores the same way (`DefaultBackendOpener.createStore`)
- Store lifecycle: the control thread builds the `Store` and calls `router.install`, where ownership passes to the
  executor; a close removes it from the router (marking it offline) and then runs the close on its executor. Before
  install and after close the backend and directory belong to the control side, which otherwise reads only
  `storeId`, `path`, `crlSaveFile` and the thread-safe `hasFailed`/`isWriteFrozen`. `release` frees the slot.
  Every exit (transfer out, shutdown, relinquish, fail-stop, failed CRL load) runs `Store.close`, which stops the
  store's drivers; a reload over a loaded store reusing its backend skips the close but calls
  `Store.shutdownDrivers`. A new exit path must do one or the other
- Store identity: `StoreLoader.handleLoad` reads the `incarnation` file (`StoreIncarnationFile.load`) before building
  the Store and never writes it: `StoreCreation` writes it into every new store directory (bootstrap included) and a
  rebuild's `finish` replaces it (`server/store/CLAUDE.md`). A file known absent has the store retired unserved
  (`retireUnserved`: marked `rebuild-required`, rebuilt as a new incarnation); one that cannot be stat'd, read or
  parsed is left alone and the load retried through `storesAwaitingRetry` (`awaitUnreadRetry`), so a format change
  in a release is an outage, never a fresh identity over a store's data and CRL state
- Every existence test under `server/` uses `server.FileStat`, never `os.exists`/`os.isDir`/`os.isFile`
  (`Files.exists`, false on any stat error; `FileStatUsageSuite` fails on one): absent only on ENOENT (or a parent
  that is no directory), otherwise it throws. A new site decides whether a failed stat waits, throws or is safe
  either way, and says so where it stands. `tryLoadStore`'s config and marker checks and `handleLoad`'s `crl_save.log`, `acceptor-since` and
  `inherited-votes` checks hold a load whose stat fails back through `awaitUnreadRetry` (a rebuilt store loaded
  without its gates would be a full acceptor of its old incarnation's transactions); the Missing Stores pass, `dropLeftoverCrlState` and the Deleted Stores
  pass skip such a store; `StoreCopies.retire` fails rather than report a clear path, and `StoreCopies.delete`
  reports gone only a copy known absent; `DeviceDiscovery.openDeviceCrl`
  throws (device left for the next scan; a child whose config cannot be stat'd counts as a store directory) and so
  does `holdsOwnState`. `tryLoadDevice` leaves a device whose directory, config or `failed` marker cannot be stat'd
  for the next scan, and `DeviceMarkers`' host-id reads and `storesIn` throw too. `StoreLoader.readArrivedAt` counts
  a failed stat as an arrival now; `adoptionClass` throws (adoption retried next scan), and so does
  `bringIntoService` on a store directory it cannot stat, before it rewrites `owning-host`, so no adopted store loads
  without its `ArrivedAtFile`; `RebuildingStore.isRebuildCompleteFor` throws on a marker it cannot stat or read;
  `startOut` never ships a store whose `rebuild-required` stat fails; `retireForeignCopy`/`discardLostStore` and `startIn` wait for a later check (`startIn`
  also when the promoted copy's `transferred-in` marker cannot be read: it says nothing of which transfer the copy
  belongs to); reconcile
  passes act on a marker only when a stat finds it. Tests fail a stat with a directory lacking search permission, or
  a link into one
- Load time and the forget floor (`server/store/CLAUDE.md`): every Store `StoreLoader` builds records its load
  (`Frontend.loadedAt`), so after a restart, adoption or transfer in it refuses, for good, each transaction it does
  not hold that started before the load plus `MaxStartLead`. A store that arrived from another host had joined by that
  host's clock: `StoreTransfers.clearTransferredInMarker` (before the marker goes) and an adopting
  `DeviceDiscovery.bringIntoService` (before `owning-host` is rewritten) durably write `StoreLoader.ArrivedAtFile`, and
  a load less than `RebuildingStore.AcceptorSinceMargin` (10 s) after it adds that margin instead (`Frontend.arrivedAt`).
  A same-host restart past the margin adds only the lead. The exception is a store this process just created
  (`StoreLoader.newStores`: a `startOpen` with `create`, made only by `StoreRebuilds.createNewStore` for a new pool's
  store with no directory yet). It has never served, so it records no load and joins at once. Tests set load times
  through `Host.Seams.storeLoadedAt`
- Join ceiling (`JoinCeiling`, paxos-transactions 10.10): `Host` keeps a durable upper bound on the start of every
  transaction its stores joined. It is one file, `<host-root>/join-ceiling`, written with `DurableFiles`. The bound is
  renewed once at construction, then every `RenewPeriod` (250 ms) on the host's `ec`, to now + `MaxStartLead` +
  `Step` (1 s), and only ever rises. Stores join only below the persisted value (`mayJoin`), so a same-host reload
  needs a load floor no higher than the ceiling its previous life ran under (`boundAtLoad`). That is the value the
  previous process left (`atStart`) for a store's first load in this process, or the value now for a reload. A failed
  write removes the file and stops gating for the process; an unreadable file bounds nothing. A host with
  `NoBackgroundTaskManager` cannot renew, so it has `JoinCeiling.Unbounded` unless `Host.Seams.joinCeiling` supplies a
  persistence. TestNetwork does, in memory, and renews in `handleEvents` (`Host.Seams.joinCeilingStep`, 100 ms there).
  `TestNetwork.crashHost`/`restartHost` model a host whose ceiling outlives its process
- A loading store asks for its CRL state with the non-blocking `readStoreState` from its executor
  (`StoreRouter.readAndRecover`), holding its messages until it recovers, so no store thread blocks on a CRL. A
  read failed by the CRL's own death or shutdown (`CrlFailed`, `CrlClosed`) is the device's failure: the store is
  not fail-stopped but left, unrecovered and unmarked, for `DeviceShutdown.crlFailed` or the unload to close
- Device management (the classes `DeviceControl` wires) runs on one control thread, `aspen-host-control`,
  with no locks. Rules (scaladoc of `HostThreading.scala`): the only `given ExecutionContext` is the control context;
  never block the control thread on a Future; other threads read control state only through the `@volatile`
  `HostStoreView` (an immutable snapshot the control thread republishes on every change: `containsStore`,
  `getStoreIds`, `getDevicePath`, `isRepairable`, `storeDevices`: the device each served store is loaded from, read by
  `Host.localDevice` for the repair fence's `RepairTarget.localDevice`) or the router's concurrent map
  (`hasTransactions`, `logTransactionStatus`); store and control state stay apart: control posts store work to the
  executor (`StoreRouter.onStore`) and gets results back through a Future whose callbacks name the control context
- Callbacks name the control context. Work run elsewhere (backend opens, client transactions on the host's `ec`) names
  its context; timers and other non-Future callbacks post to `controlEvents` before touching control state
- Tests use `LockstepThreading`: everything posted waits on one FIFO drained by `testingOnlyHandleEvents`, stores run
  inline, and the control context is the suite's serial `ec`, so integration suites keep their exact ordering.
  `ThreadedHostSuite` runs the TestNetwork's host on a fixed `StoreThreadPool` (`IntegrationTestSuite.testThreading`),
  and `AdaptiveThreadedHostSuite` on an adaptive one that moves stores between threads mid-test

### Host and StorageDevice Management
- Most administration is done by reading and writing host and device objects containing the desired state
- Hosts will poll these instances and act on changes
  - This approach is preferred over sending messages directly to the host, since those messages could be lost
  - An example is seeing that a DataStore is marked for transfer in or out. When seen, the host will start the process
- Each storage device has its own CRL (`<device>/crl`, opened when the device loads; `server/crl/CLAUDE.md`) shared by
  its stores. An unloaded device is not loaded again until its CRL has shut down (`LocalDevices.closingCrls`). A
  CRL that fails (`CrashRecoveryLog.failed`) takes its device out of service (`DeviceShutdown.crlFailed`): the
  directory is disowned, the stores closed and gossiped offline, the device unloaded, and nothing written. The device
  stays unloaded until the host restarts: it is not reloaded, which could loop into a lost-CRL declaration
  (`CrlOpenAttempts`), and it is not marked Failed in the tree. Its admission is charged at load for every store's CRL
  records, retained per store (`CrlAdmission.chargeRetained`). A loading store releases what is retained for it only
  after its recovery has counted its records (`Frontend.recoverState`); a reload over a loaded store retains the old
  store's admissions on the shared executor (`StoreRouter.install`), and the new store's CRL read is issued behind
  every save the old one made (crash-recovery-log 6.1)
- `DeviceDiscovery` registers `crl.onAdmissionLimitRaised(sds.admission.raiseLimit)` right after constructing the device
  state, before any store loads: a CRL growth raises the device's admission limit (raise-only, thread-safe) for all its
  stores
- A store loaded from its `crl_save.log` first drops whatever the device CRL holds for it (`StoreLoader.awaitCrlLoad`):
  the file is its whole state, and anything else there is a copy of part of it or an earlier tenancy's leftover,
  never to be recovered as its own (crash-recovery-log 3.3 step 4). A rebuilt copy carries an empty one
  (`RebuildingStore.writeEmptyCrlState`), so its first load drops what an earlier copy left; a late save by a
  fail-stopped earlier copy's close precedes that drop, since the new store cannot install until the close releases
  its executor slot (`HostThreading.AlreadyAssigned`). Once the CRL holds the file's state, `StoreRouter.crlLoaded`
  removes the file durably and only then recovers. A failed removal (`SaveFileRemovalFailed`) unloads the store
  keeping its device-CRL entries (after a successful unlink they are its whole state) and retries the load every
  device scan; the router records the file (`saveFileRemovalPending`), so the retry removes it again, forcing the
  directory even if it is gone, before recovering
- A `crl_save.log` state is charged before `loadStore` saves it (`CrlAdmission.tryChargeLoaded`, at most the limit
  plus one transaction), after the Store is built and before install, retained for the store. Refused, the store
  is left offline, its file kept, and retried every device scan through `storesAwaitingRetry` (one warning per
  minute), and the CRL is asked to grow (`requestGrowth`). A state that can never fit (above the most limit any
  config allows, `maxAdmissionLimitBytes`, plus T, or a transaction above T) takes `crlSaveFileLost`: rebuilt; one
  that fits only after growth, even past this open's `crl-max-streams`, waits offline as above. Neither bound follows
  the device config, so a config edit never declares a state lost
- Planned physical moves: `shutdownStorageDevice` makes Active stores Offline and the device ShuttingDown; its host
  closes the stores, leaving their CRL state live in the device CRL (no per-store marker or save file), writes
  `shutdown.state` (its HostId) in the device dir, and commits Shutdown. Each device dir also has `owning-host`
  (`server.DeviceMarkers`), so the 10 s scan decides without the network; only a dir with `shutdown.state` needs a
  device-state lookup. On the same host a Shutdown device waits for `restartStorageDevice`; a restart that cancels a
  shutdown in flight has the reconcile load the closed stores again (`DeviceShutdown.reloadClosedStores`). Another host
  adopts it, in one transaction rewriting the device, its pool entries and both HostStates, then rewrites
  `owning-host` and loads it: it opens the device CRL and each store recovers its transactions from it
- Adoption needs no shutdown: a drive pulled from a running or crashed host is in the state that host's crash leaves
  it in. A device whose `shutdown.state` (or, absent that, `owning-host`) names another host is looked up and adopted
  when the tree records it as owned by that host and not Failed, `<device>/crl` exists (if it holds stores), and
  every Offline or Active entry has a directory. Physical presence is trusted: there is no fencing of the old host.
  A running old host whose reconcile finds the tree naming another owner relinquishes the device
  (`DeviceShutdown.relinquishDevice`): it closes the stores, unloads the device, writes nothing, and keeps the
  directory disowned in memory. The adopter nudges it with `CheckStorageDevice`
- `crl_save.log`, `transferring-out` and `transferred-in` are for network transfers only: a transfer out closes the
  store with `extractCrl`, copying its state from the device CRL into `crl_save.log` to travel with it. Such a file is
  then the only copy of that state: if it fails to load (missing, truncated, unparsable, or naming another store) the
  store is never served. A read that fails without saying anything of the content (`CrashRecoveryLog.isUnreadable`:
  EIO, EMFILE, out of memory; `loadValidStoreState` throws it rather than return `Left`) is never taken for that:
  `handleLoad` retries through `awaitUnreadRetry`, `holdsOwnState` (which reads the store config the same way) throws
  before any store is marked, `adoptionClass` fails the adoption for the next scan, and `StoreTransfers` neither
  resumes nor discards a promoted copy it cannot read. It gets a `rebuild-required` marker, its entry is flipped to Rebuilding by the reconcile, and the
  copy is retired (below) before the rebuild. Adoption of a device does the same for a `transferring-out` store
  without a valid save file instead of refusing the device, and adopts any `rebuild-required` directory as Rebuilding
  whatever its other markers. A network transfer validates the staged copy's save file before promotion and retries on
  failure, since the source still holds the state; a resume of a promoted copy's pool update (`StoreTransfers.startIn`)
  checks it again, and a copy marked `rebuild-required` or whose save file no longer validates is deleted and the
  transfer restarted, as is one whose marker names another transfer
- A lost device CRL does the same to every store on the device: if `<device>/crl` is absent from a device holding store
  dirs, or holds no stream files there (`openCRL(..., holdsStores = true)` throws `CrlMissing`: a device's stores exist
  only once its CRL was opened, so an emptied `crl/` is never taken as a new CRL), or `openCRL` throws (a corrupt
  CRL, `CrlCorrupt`, is logged as such), each store dir gets `rebuild-required` except one that held nothing in it
  (`holdsOwnState`): one whose `crl_save.log` validates as its own (its whole state is the file, which its load uses
  over any CRL), or a finished rebuild carrying `rebuild-complete` (never served, so its state is empty; its flip and
  load proceed as over the old CRL), the old
  directory is kept as `crl.lost-<uuid>`, and a fresh CRL is created in `crl.creating/` (`CrashRecoveryLogFactory.createCRL`),
  renamed to `crl/` durably once finished, and opened (`DeviceDiscovery.openDeviceCrl`). A crash or failed creation
  (ENOSPC) leaves no `crl/`: device load sweeps `crl.creating/` and the next load declares the CRL lost again, moving
  nothing aside. A CRL another writer holds open (`CrlLocked`) is never
  lost: the device stays unloaded and each scan retries. Nor is a CRL whose files disagree with its recorded sizing
  (`CrlSizingMismatch`), nor a `crl/` holding no CRL yet whose device config cannot size one (`CrlConfigInvalid`):
  the device is refused, an error is logged once, and each scan retries. A transient error (I/O or VM error,
  `DeviceDiscovery.isTransient`) leaves the device unloaded for the next scan instead, until it has failed on
  `CrlOpenAttempts` (3) device scans in a row, when the CRL is lost. Only a `ScanDevices` open counts
  (`checkForNewDevices(scan = true)`): host start, `CheckAllDevices` and nudges retry uncounted, so start-up's burst of
  opens cannot use up the allowance.
- `Host.StoreFailStopped` (a store's CRL read failed other than by the CRL's death or shutdown, its recovery threw,
  or its Frontend fail-stopped, `server/store/CLAUDE.md`) is handled as a backend failure (`StoreRebuilds.handleFailStopped`), but only while that Store is still the one
  installed under its id: a late fail-stop of a closed or replaced store is ignored, so its directory is not marked
- A backend fail-stop during a store's close marks it `rebuild-required` instead of closing it cleanly. A
  `crl_save.log` a transfer-out close wrote stays: its entries have left the device CRL, so it is the only copy of
  the store's transaction state, and the retire keeps it as the copy's lost CRL state. Every reader of a store
  directory checks `rebuild-required` before its save file. Such a store is never transferred: its transfer is
  aborted so it is rebuilt
- Store copies (`StoreCopies`). A store has one owner: the device its pool names. Ownership moves by a transfer or a
  rebuild and takes effect when the new device's entry becomes Active. From then on every other copy, whatever it
  is, may go, and only then. So a copy that stops being the owner's is never deleted where it stands. This covers a
  completed transfer out's source and a lost copy, whether its rebuild runs here or was re-placed away. Instead it is
  **retired** (`StoreCopies.retire`), each step durable before the next:
  1. the `rebuild-required` marker is written;
  2. its CRL state is saved into the copy's `lost-crl-state.log`. A file already there is kept. Otherwise the copy's
     `crl_save.log` is renamed to that name (with any device-CRL entries the store still has, after a crash before
     a transfer out deleted them, saved beside it first as `lost-crl-entries.log`: the file may be what was found
     unusable), or else the store's device-CRL entries are read and saved;
  3. `CrashRecoveryLog.dropStore` removes the entries from the device CRL;
  4. the copy is renamed to `<device>/pending-delete/<store-dir>-<uuid>`.

  A crash at any step leaves the marker, so the next retire redoes what remains. Three passes retire:
  - The Rebuilding pass, for a `rebuild-required` copy at the path of a store rebuilt here
    (`StoreRebuilds.discardLostStore`). It holds a rebuild slot from its start (`maxConcurrentRebuilds` counts
    `droppingCrlState`), and the slot passes straight to the rebuild in the same callback, so one reconcile cannot
    retire every marked store at once. The handoff re-reads `reconciledEntries`: the rebuild starts only if the entry
    is still Rebuilding under the episode the discard began for; otherwise the slot is released and the device checked
    again (a retire held up by a slow CRL can outlast a re-placement). A failed retire is a failed pass: it re-checks
    the other devices and schedules this device's backoff re-check (`scheduleRebuildRetry`), which also starts a
    transfer in the discard held back. Neither the Lost CRL pass
    (`DeviceReconciler.flipLostCrlStores`) nor `startRebuild` acts on a store while
    `storeTransferService.isTransferringOut` (host-wide, by store id) holds it (a transfer aborted while its send runs, its copy then marked):
    the discard would move the directory the send may still read. Each schedules a device re-check instead, as the
    Abandoned Transfers Out pass does; `startRebuild` asks only when no rebuild of the store is registered, since a
    registered one's end re-checks the device anyway. `startRebuild` also waits while a transfer out is still closing
    the store (`StoreTransfersOut.deferRebuildUntilClosed`, i.e. `isClosingForTransfer`): the close's end, whatever its outcome,
    checks the device again, which starts the rebuild.
  - `StoreTransfers.startIn`, for a `rebuild-complete` or `rebuild-required` copy an earlier tenancy left at the
    path of a store transferring onto the device (re-placed away while the host was down, then transferred back;
    rebuild-process). It retires before asking for bytes, waits while a rebuild of the store runs here, the
    store is opening, or a copy is served from this path (not while the transfer's source is served from another
    device of this host: it closes only for the StartStoreTransfer sent once the path is clear), and checks the
    device again once the path is clear, which starts the transfer.
  - The Deleted Stores pass, for an offline store whose entry has left the device. The store stays offline until its
    path is clear, so a failed retire is retried.
    A store served from the device that its entry does not list (a copy put back by hand: `tryLoadStore` reads no
    tree) is first taken out of service as a failed backend is (`StoreRebuilds.retireUnlistedServed`: marked,
    closed, offline), so the check that follows retires it.

  The reconcile **releases** (`StoreCopies.release`) a `pending-delete/` copy once its store is Active on its owner
  device: on this device by its own entry, elsewhere by the pool's entry and that device's state. This is the only
  release condition. A rebuild that cannot finish, a failed read, or a name that identifies no store keeps the copy.
  An accepted rebuild loss is no exception: the operator's acceptance is the release, and `accept-rebuild-loss` says
  so. The store comes from the copy's name, never from parsing anything inside it.

  Every delete of a copy is **rename-first** (`StoreCopies.delete`). The copy is renamed into `<device>/deleting/`,
  durably, then removed, and whatever is left there is removed at device load and at each reconcile. So an
  interrupted delete never strips a marker from a copy at a store path or leaves a partial copy in `pending-delete/`.
  A copy that never was the owner's and holds nothing its source lacks is deleted directly (still rename-first):
  `LocalDevices.discardStoreDirectory` and a stale or unusable arrived copy in `StoreTransfers.startIn`. Scratch areas
  are never loaded, so they are deleted in place: `creating/`, transfer staging and `deleting/` are swept whole at
  device load (`DeviceDiscovery.loadOwnedDevice`); `rebuilding/<id>` holds a resumable checkpoint and is deleted
  only by `StoreRebuilds.abandonUnlisted` or a first pass's `createStaging`.

  Every rename of a copy, of a store's CRL state, or of a lost device CRL goes through `DurableFiles.move`, which
  forces both directories. Without the marker on disk, a store's entries stay in the device CRL. A transfer out reads
  them (`readStoreState`, leaving them live) into `crl_save.log`, whether the store is loaded or not
  (`StoreTransfersOut.closeForTransferOut`), and deletes them from the device CRL (`deleteTransactions`) only once that
  file is durable: a crash or failed save before that leaves the device CRL holding them. A closed store that already
  has a valid `crl_save.log` ships it unchanged: the file is its whole state, so its device-CRL entries are copies or
  an earlier tenancy's leftovers, dropped (`dropStore`) as its load would drop them (`extractUnloaded`). A closed
  store is extracted on the transfer's source device only (`startOut` passes `sourceDs`), never on another copy of it
  another device of the host holds. Its directory and save file are tested with `FileStat`: a failed stat fails the
  extraction rather than read as an absence (which would write the CRL's entries over a valid file). The store must
  stay closed from the extraction to the `transferring-out` marker: a load retry waits throughout
  (`isClosingForTransfer`), an open started meanwhile by any other path (`opensStarted`, checked as each CRL call
  answers) fails the extraction, and a load of the save file queues its saves behind the extraction's final deletions
  (its empty `dropStore` is a CRL barrier, `server/crl/CLAUDE.md`), so they create records rather than being skipped, and `startOut` re-checks loaded / opening / `opensStarted` in the
  control task that writes the marker. The extraction releases only the charge retained when it began
  (`releaseRetained(storeId, charge)`). `startOut` is single flight per store (I3): a request (a retransmit, a
  readdress) that arrives while `closingForTransfer` holds the store, its close or `extractUnloaded` still running,
  does nothing (logged at debug); the receiver's retransmit tries again if the first fails (`WorkOverlapSuite`).
  `DeviceReconciler.dropLeftoverCrlState` drops the entries of a store with no entry on the device, no directory
  (a stat that answers absent) and no retire running, as a safety net for a directory removed by hand; a store its
  entry names keeps them, even one the Missing Stores pass is rebuilding. It releases only the admission charge
  retained for the store when it was issued, and only if unchanged at completion (`CrlAdmission.retainedCharge` /
  `releaseRetained(storeId, charge)`, by identity): a store landing meanwhile charges records the drop does not
  remove, and its recovery releases the whole charge
- A rebuilt store's directory carries `rebuild-complete` from its move into place until its entry leaves
  Rebuilding; `tryLoadStore` leaves such a store offline (`server/rebuild/CLAUDE.md`). An `Adopt` whose marker
  removal fails records the copy in `offlineStores` (`keepUnpromotedCopyOffline`) and in
  `StoreRebuilds.pendingMarkerRemovals`, so the Completed Rebuilds pass retries the removal and the load at each device
  check. The pass selects by the pending record as well as the marker, and `clearRebuildCompleteMarker` forces the
  directory even with the marker absent: an unlink whose directory sync failed leaves no marker to select by. Tests
  fail the sync through `StoreRebuilds.syncMarkerDirectory`
- A failed rebuild pass or a permanently failed flip never re-checks its own device at once, which would restart the
  same store in a spin: it re-checks the other devices (the slot is host-wide) and schedules a backoff re-check of its
  own (`StoreRebuilds.scheduleRebuildRetry`: `Host.RebuildRetryInitialDelay` 30 s, doubled per failure of the store in
  a row up to `RebuildRetryMaxDelay` 30 min, reset when a flip settles; rebuild-process 5.7). Until it runs,
  `startRebuild` skips the store for what failed (`HeldRebuild`: device, episode, `acceptLoss`), so a failing store
  cannot retake the slot from those queued behind it; an operator's remedy since (a re-placement to another device or
  episode, an `accept-rebuild-loss`) starts at once. Tests drive it with
  `ControlFixture.runRebuildRetries`
- Re-placement (rebuild-process 4.2): `AspenClient.replaceRebuild` (`transfer-store` on a Rebuilding store) moves a
  Rebuilding entry to another device and repoints the pool, under a new rebuild episode
  (`server/rebuild/CLAUDE.md`). The reconcile's `StoreRebuilds.abandonUnlisted` cancels a rebuild running for a store
  no longer Rebuilding on its device, or Rebuilding there under another episode (`rebuildEpisodes`; its completion
  checks the device again; the slot release clears its `abandonedRebuilds` mark whatever the outcome, since a rebuild
  past finish's latch re-check ignores the cancel), and deletes a `rebuilding/<id>` staging copy only when no rebuild of it is running.
  The reconcile runs it before the transfers-in pass, and `StoreTransfers.startIn` receives nothing while rebuild work
  of the store is registered on the device (`StoreRebuilds.hasRebuildWork`: queued in `silenced`, discarding in
  `droppingCrlState`, or running, a cancelled one included until its slot is released): the store re-placed away and
  transferred back must not be received while its earlier rebuild still runs here. The rebuild's end checks the device
  again, which starts the receive (I3, `WorkOverlapSuite`). A
  flip of a rebuild whose entry has since moved to another episode is `RebuildOutcome.Superseded`: nothing is
  written, and its copy, marked for the earlier episode, is retired by the next `startRebuild`. That copy, like one
  whose flip failed permanently, is added to the device's `offlineStores` (`keepUnpromotedCopyOffline`, as a
  restart's load would), so the Deleted Stores pass retires it if its entry leaves the device first. A flip retried
  after `TransactionOutcomeUnknown` never discards its copy on a device not Failed: its earlier attempt may have made
  the copy the owner's before a transfer out or removal moved the entry on, so an entry it would otherwise `Discard`
  for is `RebuildOutcome.Unconfirmed`, kept the same way for the transfer, Abandoned Transfers Out, Completed Rebuilds
  or Deleted Stores pass (rebuild-process 5.7). A transfer ships it with its `rebuild-complete`, which the destination
  removes (`clearRebuildCompleteMarker`) when its pool update commits, before `loadStoreById`
  (`StoreTransfers.recordTransferIn`); a failed removal keeps it offline for the destination's Completed Rebuilds pass.
  `StoreTransfersOut.startOut` refuses (logged at debug) while a rebuild of the store is registered
  (`StoreRebuilds.isRebuilding`, the flip's retry after an Unknown attempt included), so a transfer made meanwhile
  never closes or ships the copy under it (I3); the receiver's retransmit (`StoreTransfers.retransmitPendingStarts`,
  every 30 s) starts it once the flip settles. Tests run a write between an Unknown attempt and its retry with
  `TestNetwork.TClient.beforeRetry`, and `TClient.immediateRetries` fires the retry without its real-time backoff
- Nothing of a store's previous copy is destroyed before the store is Active on its owner device (the store copies
  rule above). A fail-stopped backend's CRL entries likewise stay live until the retire saves them
- A device the tree records Failed is retired by its host's reconcile (`DeviceShutdown.retireFailedDevice`): a
  durable `failed` marker (`DeviceMarkers`), then unloaded as a relinquished device is. A device with the marker is
  never loaded or claimed; nothing on it is deleted. The tombstone (`FailedStorageDeviceDurableTask`) nudges the
  former host with `CheckStorageDevice`. A repair scan runs only while the store's pool entry names this host
  (`StoreRepairer`), so a superseded copy never consumes the current copy's ErrorTree entries
- Metadata transactions retried after a `TransactionOutcomeUnknown` recognize their own commit (`client/CLAUDE.md`,
  paxos-transactions 10.11): `requestRebuild` checks the device at once on finding its Rebuilding entry, `adoptDevice`
  returns `Adopted` for a device it finds owned here, the executor root init uses a root already present, and the
  transfer-complete and rebuild-complete updates send the `StoreMoved`/`StoreOffline`/`gossipStoreOnline`
  announcements and nudges that the committed attempt's `tx.result` could not
- `DurableFiles` (Linux-only directory durability): `server/CLAUDE.md`
