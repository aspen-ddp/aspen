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
- No copy carries a marker file (store copy state machine stage 2, Task 5): `StoreDirMarkers`, `CopyMarkers` and
  `transferred-in` are gone, and nothing writes `transferring-out`, `rebuild-complete` or `rebuild-required`. Every
  phase but Live is a directory (below); the flat path is Live (`CopyPhase.read`, which Task 6 deletes with the flat
  path, reads nothing)
- Unsynced copies: a move that lands but whose directory sync fails sets the record's `unsynced` (`CopyMoves`,
  `HostCopies.unsynced`; a copy of a device not attached yet, an adoption's or a lost CRL's moves, is held in
  `unsyncedPaths` until its attach). An unsynced copy is never loaded (`tryLoadStore`), shipped (`startOut`
  refuses), retired (`StoreCopies.retire` answers false) or received over (`startIn` waits). Each device check
  first forces every unsynced copy again (`CopyMoves.resync`, at the top of `reconcileDeviceState`). A copy moved
  Live unsynced stays held for its load (`HostCopies.heldForSyncOn`) until a load opens or installs it, or it leaves
  its path or device, and every operational check loads each held copy that is synced, Live, offline, free of work
  and Active (`DeviceReconciler.loadResynced`, with its online gossip): the pass that moved it will not see it again,
  and a check that finds the entry not Active or work holding the copy only defers it
- Copy layout (store copy state machine, stage 2; spec "Device layout"): `CopyLayout` says where each copy lives.
  A converted phase is a directory under the device root holding copies named `<storeId.directoryName>.<copyId>`
  (`copyName`/`parseName`; the store id comes from the name only), its parameter in the copy's own file for that
  phase, `phase-param.<phase directory>` (`paramFile`, `param`, `readParam(copy, phase)`: "" when known absent, a
  failed stat throws). Each location reads only its own phase's file and a move writes only its target's, so a
  failed move or a crash before the rename leaves the source's parameter as it was; an earlier phase's file is
  inert. So far Arrived (`arrived/`, `CopyLayout.ArrivedDirectory`, its transfer id in `phase-param.arrived`), Outbound
  (`outbound/`, `CopyLayout.OutboundDirectory`, its transfer id in `phase-param.outbound`),
  Rebuilt (`rebuilt/`, `RebuiltDirectory`, its episode in `phase-param.rebuilt`), Parked (`parked/`,
  `ParkedDirectory`), Lost (`lost/`, `CopyLayout.LostDirectory`) and Retired (`pending-delete/`) are directories
  (`dirOf`);
  Live is the flat path `<device>/<storeId.directoryName>`. A copy in a phase directory has its phase by location
  (`CopyLayout.phaseAt`; `knownPhase`/`readPhase` for any copy path, which `tryLoadStore` and `reloadClosedStores`
  read).
  `CopyLayout.list` is the one listing of a device's copies (device load and its CRL-lost decision, `storesIn`,
  `HostCopies.attach`/`rescan`/`verifyAgainstDisk`): flat store-named directories (config or not; a failed stat
  counts) and each converted directory's children; a child that is no copy name is a stray, logged at warn at the
  device's load and never touched. Code never builds a copy's path: it asks `HostCopies.pathOf` (the record's,
  `CopyLayout.path` over its `copyId` and phase), `pathOrLive` where the copy may have no record, or
  `CopyLayout.path(..., None, Phase.Live)` for a copy not recorded yet (a create's target). `CopyRecord.copyId` is None for a flat copy. Every phase change into or out of a phase
  directory is one `CopyMoves.move` (`DeviceControl.moves`; `moveUnattached` for a device being loaded): the target
  phase's parameter file written durably, then `DurableFiles.move` through the `rename` and `syncDirectory` seams,
  recorded as a fact (`HostCopies.moved`, a fresh `copyId` on a copy's first move; a load retry the copy awaited is
  retargeted to its new path for a move into Live and dropped for any other phase). `DurableFiles.move` throws
  `DurableFiles.MoveNotForced` for a rename that landed but was not forced: `MoveResult.Unsynced`, the record takes
  the phase with `unsynced` (above) and `resync` forces both directories again; any
  other failure is `NotMoved`, nothing changed. A durable move to Retired unregisters the record (P3); an unsynced one
  stays recorded, Retired, out of `offlineCopies`, and `StoreCopies.release` skips it, until the resync. A copy
  placed from a scratch area into a phase directory (a transfer's promotion) is one `CopyMoves.place`, the same
  steps under a fresh copy id, recorded as a fact (`HostCopies.placed`, unsynced for a rename not forced). Tests place, find and read copies only through `CopyFixtures`
  (`placeCopy`, `markCopy`, `copyPath`, `phaseOf`, `retiredCopies`). `CopyMoves.move` refuses (NotMoved, logged at
  error as a bug) a record that is Opening, Served or Closing, and a copy unsynced before a move stays unsynced after
  it (its earlier directories still to force move with it), until the resync. A move whose target is the copy's own
  path is NotMoved (no move changes only a parameter). A failed read of an `arrived/`, `outbound/` or `rebuilt/` copy's
  parameter file fails `CopyLayout.list` (the device stays unloaded for the next scan, P6) and makes `knownPhase` answer None
- Outbound is `outbound/<copy>` (T6, T7); `transferring-out` is never written or read. `startOut` refuses at
  debug a source with no record or whose phase is not Live, Rebuilt, Parked or Outbound (P10: an Arrived copy waits
  for the Completed Transfers In pass), and an unsynced one. `closedForTransfer`, once the close's extraction has made
  `crl_save.log` durable, moves the copy to `Outbound(tid)` (`StoreTransfersOut.moveToOutbound`: one already
  Outbound for `tid` stays, a retransmit's; one Outbound for another transfer goes back to Live first; a copy that
  must move is refused at debug, in `startOut` and again here, while a send of the store is still registered,
  `isTransferringOut`: the supersession abort runs only inside the next `beginTransferOut`, after the move) and begins the
  send from the outbound path; a NotMoved or Unsynced move ships nothing and ends the work, the receiver's
  retransmit retries (an unsynced copy once a device check's resync has forced it). The service ships whatever
  directory it is given, so a shipped copy carries `phase-param.outbound`, inert at the destination, whose promotion
  writes `phase-param.arrived`. T7 is a move to Live: `reinstateAbandonedOut` (the Abandoned Transfers Out pass
  selects offline records whose phase is Outbound or Parked) and `bringIntoService` (`moveUnattached` for each
  `outbound/` and `parked/` copy of an Active entry that `mayReinstateOut`; the adoption's arrived-at is written into
  flat, `outbound/`, `rebuilt/` and `parked/` copies; an `arrived/` copy's is written when it goes Live). A reinstated copy is Live (P8: an Active entry means the flip
  committed)
- Rebuilt is `rebuilt/<copy>` and Parked is `parked/<copy>` (store copy state machine stage 2, Task 4). A rebuild's
  `finish` writes its episode as `phase-param.rebuilt` in staging, last, then renames staging into `rebuilt/` under a
  fresh copy id (T2, off the control thread; a failed rename leaves staging for the next pass), and its completion
  (`StoreRebuild.complete: Future[UUID]`) reports that id: the control-thread handler records the copy with it
  (`HostCopies.rebuilt`). `RebuildingStore.start` asks the lookup it is given (`existingCopy`, built by
  `StoreRebuilds` over `CopyLayout.list`, `RebuildingStore.copyOnDisk`, since it must not read control-thread state):
  a Rebuilt copy of its own episode (`isRebuiltFor`) completes at once with its copy id; any other copy fails it.
  The flip then moves the copy: `Adopt` Rebuilt -> Live (`StoreRebuilds.bringRebuiltLive`, T3: a NotMoved copy stays
  Rebuilt for the Completed Rebuilds pass, which selects offline Rebuilt records of Active entries; an Unsynced one
  is Live and held for `loadResynced`), `AdoptOffline` Rebuilt -> Parked in one move (`keepRebuiltStoreOffline`, T4;
  `moveUnattached` at its `rebuilt/` path when the device is already unloaded), `Discard` deletes it rename-first
  (`discardStoreDirectory`, `StoreCopies.delete`, T13), `Superseded`/`Unconfirmed`/`NotOwned` leave it Rebuilt. A
  Parked copy goes Live (T5) as an abandoned Outbound one does, and is a T6 source (Parked -> Outbound(tid), a real
  move). `retireForeignCopy` keys off the record's phase (a flat copy with no record, put there by hand, by its
  stat; an Unread record waits). A lost device CRL leaves Rebuilt copies alone (`holdsOwnState`, by location) and
  treats Parked ones as Live or Outbound (kept with a valid own save file). A shipped Rebuilt copy arrives as any
  other: Arrived, then Live at its pool update, its `phase-param.*` files inert. `finishPlacing`'s exemption
  (`HostCopies.finishInFlight`) is a `rebuilt/` copy of the store whose parameter reads the running rebuild's
  episode
- Lost is `lost/<copy>` (T10). Nothing writes `rebuild-required`. Every T10 trigger moves a closed copy there through
  `StoreRebuilds.moveToLost(site, sds, storeId): MoveResult` (a copy already Lost is left, Durable): a served copy's
  close for `ClosePurpose.FailStop` (`backendFailed`, so `handleFailStopped`, `retireForeignCopy`'s and
  `retireUnlistedServed`'s served copies) moves it when the close ends (`StoreLoader.closeUnloaded`,
  `StoreRebuilds.failStopClosed`; P5, the move never made while the backend is open); `failedOnClose` once
  `handleClose` has ended the close; `retireUnserved` (failed before served, lost incarnation, `crlSaveFileLost`) once
  the unserved backend's close completes; `handleBackendOpened`'s failed unserved backend likewise; a
  `BackendStateLost` open at once (no backend opened); `backendFailed` for a backend no longer served, unless its copy
  is Opening or Closing (its LoadStore or its close's end retires it); a foreign copy `retireForeignCopy` finds
  closed, and retire step 1. A crash between a FailStop close and its move leaves the copy where it was, and the
  restart loads it as after any crash (its CRL holds what the backend had not applied; a backend still broken fails
  again). Every unload of a device (a shutdown's `closeThenUnload`, a relinquish or a Failed retirement's
  `stopServing`, `crlFailed`) waits for the closes on it whose end may move a copy to lost/
  (`StoreRebuilds.closeStarted`/`closeDone`: `handleClose`, `closeUnloaded`, `retireUnserved`, a failed unserved
  backend's close in `handleBackendOpened`) through `DeviceShutdown.unloadOnceClosed`, so every such move is made on
  the attached device and an adopter never reads a fail-stopped copy as clean. A close that ends after its device's
  unload is a bug: logged at error, and its copy moved with `moveUnattached` all the same. `ClosePurpose.Reload` (a failed CRL load, `handleUnloadFailed`; a failed save-file removal,
  `handleSaveFileRemovalFailed`) leaves the copy where it is, to load again. A move that fails is logged at error and
  the copy held offline in `CopyMoves.pendingLost(local)` (in memory, per device, dropped at detach): never loaded
  (`tryLoadStore`), shipped (`startOut`, `closedForTransfer`: treated as Lost), retired (`StoreCopies.retire` answers
  false) or acted on by a pass (`offlineCopies` excludes it; `heldOffline` includes it); each device check retries the
  move first, beside `resync` (`StoreRebuilds.retryPendingLost`). A crash forgets the hold. Every reader of "is it
  lost" reads the record's phase: the Lost CRL pass (`flipLostCrlStores`), `discardLostStore`, `retireForeignCopy`,
  `startOut`/`closedForTransfer`. A device being loaded moves copies with `CopyMoves.moveUnattached`: a lost device
  CRL moves every copy that does not hold its own state (a failed move throws, leaving the device unloaded for the next
  scan), and adoption's `CrlLost` stores move through the `DeviceMetadata.adoptDevice` callback `moveToLost:
  StoreId => Unit` (a failed move fails the adoption's transaction). The device load offers no phase directory, and
  no copy in `lost/` or `pending-delete/`, to `tryLoadStore`. Tests fail renames with `Host.Seams.copyRename`
  (installed as `CopyMoves.rename` before the first device load)
- Arrived is `arrived/<copy>` (T8, T9, T13; stage 2, Task 5). Each receive attempt stages in its own directory,
  `<device>/transferring-in/<transferUUID>` (`StoreTransfers.stagingOf`, P7), which `beginTransferIn` is given; the
  service extracts into `<that>/<storeDir>` (`StoreTransferManager.storeDirectory`, unchanged: no two attempts share
  a path, so `directoryOwners` never sees two). An attempt's directory is removed whole when its attempt ends:
  promoted, a failed or cancelled receive, bytes landing after it stopped being current, an unusable save file, a
  failed promotion, an abandon's drop; one left by a crash is swept with `transferring-in/` at device load. The only
  exception is a promotion whose rename landed unforced: its staging directory is one the resync forces, so it stays
  until the device's next load. Promotion (T8) is one `CopyMoves.place` of `<staging>/<storeDir>` into
  `arrived/<copy>` under a fresh copy id, `phase-param.arrived` (the transfer id, "" for none) written first. A
  NotMoved promotion restarts the transfer after its delay; an Unsynced one ends the attempt and checks the device,
  whose resync forces it before `startIn` resumes the pool update (no pool update ever runs over an unsynced copy).
  The pool update's PoolRepointed moves the copy Live (T9, `StoreTransfers.bringArrivedLive(sds, storeId):
  MoveResult`, `arrived-at` written durably first, while it is Arrived); a NotMoved move leaves it Arrived and
  offline for the Completed Transfers In pass (offline Arrived records of Active entries, no receive attempt), an
  Unsynced one leaves it Live for `loadResynced`. `PoolRepointedOffline` leaves it Arrived. `startIn` decides from the
  record: none, receive; Unread, Retired or unsynced, wait; `Arrived(tid)` of this transfer with a valid save file,
  resume the pool update; Arrived of another transfer, or with an unusable save file, deleted rename-first (T13,
  `StoreCopies.delete`, the record removed) and received; any other phase, Live included, the earlier tenancy's,
  retired (`retireEarlierTenancy`, below; the "store path already exists" refusal is gone). `startOut` refuses an
  Arrived copy (P10). `discardArrivedTransferIn` (SourceRestored, Aborted) deletes the record's path
- `CopyPhase` (`Phase`: Live, Arrived, Outbound, Rebuilt, Parked, Lost, Retired, and stage 1's Unread) names the
  phases. `CopyPhase.read` reads no marker: a flat copy is Live, and `Unread` no longer arises from a listing.
  `tryLoadStore`, `adoptionClass` (arrived/ is Clean; Outbound, by location, needs a valid save file, else CrlLost),
  `holdsOwnState` (Rebuilt), `reloadClosedStores` (offers Live) and `startIn` (by the record, above) match on the
  phase
- Copy registry (store copy state machine, stage 1): `HostCopies` (`DeviceControl.copyRegistry`) holds one
  `DeviceCopies` per loaded device, the device state's `LocalStorageDeviceState.copies`. `attach` at device load
  (before any store is offered; `ControlFixture.injectLoadedDevice` too) registers every store directory, by name,
  with its `CopyPhase` (`Unread` where it cannot be read); `detach` at unload. Every disk mutation site of a store
  path (create, rebuild finish's handler, transfer-in promotion, every move, retire into pending-delete, discards)
  updates it right after its durable step. Every update is one of two kinds (R22):
  a **fact** (`HostCopies.fact`: something that already happened, i.e. an install, a router removal, the end of a
  close, an open, a retire or a retry, a retire's start, a phase a move reached, a copy leaving its path) is
  applied even where the transition table refuses it (`DeviceCopies.applyActivity`/`applyPhase`; a refused phase is
  set to what the directory reads, `CopyPhase.known`), and the violation recorded. A **claim** (`HostCopies.claim`:
  Opening, AwaitingRetry, a work's start or update, a new record) that the table refuses is refused, recorded and
  returned `false`, and its caller backs off: `startOpen` opens nothing; `createNewStore`, `startRebuild`'s queuing,
  `startOut`, `returnLostTransferToRebuild` and `startIn`'s receive, earlier-tenancy retire and abandon start nothing
  (each claims its work before touching its own sets). Production logs each violation at error; tests fail on it
  (below). Activity is the one home of the loader's opens and load retries:
  `startOpen` sets Opening (a create's record is registered Live there, before its create places the copy, and
  unregistered if the create fails leaving nothing; the disk check skips a record under Create work that has no
  directory yet), an open's unserved end Idle, each load retry AwaitingRetry (`HostCopies.awaitingRetry`, holding the
  `LoadRetry`: parked while the copy is Closing or Retiring, `parkedRetries`, and taken once it is Idle again;
  refused for a Served copy) and a dropped retry Idle (as is one whose copy a retire takes); `StoreRouter.install`
  Served (Served -> Served for a reload; a LoadStore no open queued first drops the copy's retry) and `remove` Closing, its purpose from `closingFor` (DeviceShutdown:
  Shutdown / Relinquish; `handleClose` with `extractCrl`: TransferOut; else FailStop), until
  `handleClose`/`closeUnloaded` end it (Idle, or AwaitingRetry when a retry was parked while it closed; a
  transfer-out close ends once the store is closed, before the extraction's deletion);
  `StoreCopies.retire` Retiring (`isRetiring`, over loaded devices: a second retire of the store does nothing). Work is the
  only record of each owner's work, set where it starts and ends: Create (`createNewStore`, until the copy is served,
  its open fails or the create is skipped; the flip that follows touches no copy and is kept apart,
  `pendingCreateFlips`), Rebuild (`server/rebuild/CLAUDE.md`: Queued, DiscardingLost, Running, Abandoned; the running
  rebuild's handle and settled promise stay in `StoreRebuilds.runningRebuilds`), TransferIn (RetiringEarlierTenancy,
  Receiving, Recording, Abandoning, and `attempt`, the receive attempt's transfer UUID: `StoreTransfers.receiving`; an
  abandon overlays a receive or a pool update and ends back at whichever still runs; the pool update's settled promise
  a shutdown waits on stays in `recordingTransfersIn`; a receive, a resume or an earlier tenancy's retire starts only
  where the device has no TransferIn work, and an abandon waits out a retire), TransferOut (stage Closing from
  `startOut`'s close until its move-to-outbound task, AbortingLost while `returnLostTransferToRebuild` aborts; an abort claimed
  during the close takes its work over, and each end ends only its own stage). A second Work for a (device, store) is
  a refused claim (I3), so `startRebuild` does not even queue a store a transfer out is closing. The transfer-in
  promotion's record is a fact (`HostCopies.placed`), as the copy is in arrived/ whatever the registry held.
  `declaredMissing` is mirrored as `missing`. An open of a copy with TransferOut work on its device is refused (R12):
  `DeviceCopies.setActivity` throws on the move to Opening, and `StoreLoader.startOpen` asks first
  (`HostCopies.transferOutWork`), logs at debug and opens nothing. Any refused open (that, or an Opening claim the
  table refuses, I1) records a load retry with the waiting load's `onServed` and the usual backoff (R23), so the
  device scans serve the store once whatever held the copy has ended; a refused create records none (its entry,
  still Initializing, has the next check create it again). An install on a loaded device with no record learns the
  copy (`learned`), as `readAtLoad` does, and so do a LoadStore and a load retry that find none (copies put at a path
  after the device's load). Stores loaded onto a device this host has not loaded, through
  `Host.Seams.unloadedDeviceCrl` (TestNetwork), get an untracked record (Live) at install, Served there and Closing
  then Idle once the router removes the store, kept so the store stays silent; `HostCopies.device` returns them like
  any other, and the disk check skips them
- Derived silence (authoritative): the router keeps its installed-store map and one `silent` set, which only
  `HostCopies.onChange` writes (`StoreRouter.setSilent`), after every change to what `silence` reads (a record, a
  work, a missing mark, an unloaded device's or awaiting device's listing, a reconcile's entries), then republishes
  the view. `silence(storeId)`: Served if any copy is Served on a device of the host (or the router has it
  installed); else Silent with its reasons: a record of any phase or activity (CopyHere, or Opening), Rebuild /
  TransferIn / Create work, missing, an unloaded device's last tree entry (`detach`, `recordUnloadedEntries`: the
  device check of a device not loaded here, a Shutdown device's decision), a device directory awaiting its
  ownership decision (`awaitDevice`/`deviceDecided`), an entry a loaded device's latest reconcile read Initializing;
  else Unknown. Served dominates, so a device check of an unloaded device never silences a store served from another
  device here. A reloaded device keeps its unloaded listing until its first reconcile writes `reconciledEntries`
  (`LocalStorageDeviceState.onReconciled`). `StoreRouter.remove` marks the store silent before it leaves the map;
  `answersUnknown` is `ready`, not installed, not silent. The view's `repairable` is `HostCopies.repairable`
  (Served, not Closing, no Rebuild work Running or Abandoned, `HostCopies.rebuilding(RebuildReads)`, and no TransferOut
  work on any device, not being sent: R20; a queued or discarding rebuild reads nothing, and a flip that keeps failing
  keeps its work queued). `reloadClosedStores` keeps a closed copy still closing, retiring or held by other work for
  a later reconcile. A throw of the
  silence path is a bug: in production it is logged at error and the store left silent (clients retransmit); with
  `HostCopies.strict` (`Host.Seams.strictCopyRegistry`, every ControlFixture host; implied by `verifyCopyRegistry`)
  it propagates as `SilenceFailure`, which `fact` and `claim` never swallow. Every lockstep drain of a strict host
  fails on any violation the registry recorded, a fact's or a refused claim's (R21, R22); production logs them at
  error, applies the fact, and backs off the claim. With `verifyCopyRegistry` (every lockstep test host:
  TestNetwork, every ControlFixture host, `ControlFixture.compared` seams, FsifIntegrationSuite)
  each drain also throws on any `verifyAgainstDisk` mismatch (phase, unrecorded config'd directory, record without
  directory) and on any copy a load, retry, LoadStore or `markedLost` learned since the last drain
  (`HostCopies.takeLearned`): a learn is a site that placed a copy without recording it. The disk check skips only:
  an Unread or unsynced record's phase, and stats that fail; a create's record before its copy is placed
  (`Work.Create`); the copy a running rebuild's finish placed off the control thread before its completion's
  handler records it, recognised as a `rebuilt/` copy of the store reading that rebuild's `Rebuilt(episode)` phase
  (`StoreRebuilds.finishPlacing`; any other copy of the rebuilding store is checked); untracked records; and
  a device a test pulled from under its running host (`host.testingOnlySkipDiskCheck(deviceId)`; a device directory
  that is merely missing is reported). A test that places a copy by hand after its device's load registers it
  before the next drain with `host.testingOnlyRescan(deviceId)` (`HostCopies.rescan`: each unrecorded store
  directory with its phase, as the load registers them) or `ControlFixture.placedCopies`; a fake rebuild places its
  copy as finish does (in `rebuilt/` with a config and its episode's `phase-param.rebuilt`) before completing with the
  copy's id (`CopyFixtures.copyIdOf`/`copyIdOn`)
- The reconcile passes select from the device's offline copies (`HostCopies.offlineCopies`: records Idle or awaiting
  a load retry, with no work on the device): the Deleted Stores, Lost CRL, Abandoned Transfers Out, Completed
  Transfers In and Completed Rebuilds passes and `reloadClosedStores`. A copy that a transfer in, rebuild, create or
  transfer out owns, or that is closing or retiring, is left to that work (I3), whose end checks the device again:
  `closeUnloaded` (fail-stop, failed CRL load, failed save-file removal) checks it once the close ends, and the
  reconcile first ends the queued Rebuild work of stores no longer Rebuilding there (`StoreRebuilds.endUnlistedWork`).
  `retryFailedLoads` keeps a retry while its copy is neither served nor opening, whatever work holds it
  (`heldOffline`). Whether a copy a transfer out left offline may go back in service is one predicate,
  `HostCopies.mayReinstateOut(entry, storeId)` (Active, no destination named, not being sent), asked by the
  Abandoned Transfers Out pass (which re-checks the device for one still sending), `bringIntoService` and
  `reloadClosedStores`. A shutdown or relinquish closes the device's
  copies that are Served or Opening (`openCopies`, plus a create's open, whose copy has no record yet;
  `DeviceShutdown.storesToClose`). Tests place copies after a device's load with `ControlFixture.placedCopies`, and
  fake a served copy with `servedHere` (the recording shutdown close ends it); `ControlFixture.awaitLoudly` waits on
  work that crosses real-time hops and fails, naming what it waited for, when its time runs out. `LocalDevices.isLoaded(sds)` says
  whether a device state is still the loaded one

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
  calling thread; silent ones (derived, above) are not answered, nor are any until the startup scan's LoadStores
  are handled. A store whose load is under way is silent (its copy Opening), as are the stores of a device whose
  shutdown-marker lookup (or adoption) is deciding whether to load it
- Backend opens are backend-neutral: `BackendOpener.open` (`server.store.backend`) returns a Future and decides where
  the open runs (RocksDB inline, FSIF on `FsifOpenEc`); `StoreLoader.startOpen` handles an already-completed one
  inline and otherwise posts `BackendOpened`. A store stays opening (`StoreLoader.isOpening`: its copy's Opening
  activity on any device of the host) from `startOpen` until its open fails or the LoadStore `serveOpened` queued is
  handled (`handleLoad` installs it or gives up in that one task), so a second load offer in between (a duplicate
  `LoadStoreById`) starts no second open, and a ShutdownStore then waits for the install (`deferredCloses`) and
  closes the served store. A `serveOpened` that throws (its LoadStore not queued) ends the open as a failed one: the
  backend is closed unserved, the copy AwaitingRetry, and the deferred closes put again. A rebuild's staging store opens through the same opener (passed by
  `StoreRebuildFactory.createRebuild`). The host and the rebuild see only `Backend.hasFailed`/`isWriteFrozen`,
  `BackendStateLost` and `Backend.isOutOfSpace`; only `DefaultBackendOpener` names concrete backends
- Store creation is atomic (`BackendOpener.create`, `server.store.backend.StoreCreation`,
  systemic-loss-protection 4.1): `startOpen` with `create` (only `StoreRebuilds.createNewStore`) builds the
  store in `<device>/creating/` and renames it to its path before opening it, so a store directory exists only
  complete and `open` never creates. The create's work (`Work.Create`) ends when its copy is served, its open
  fails or the create is skipped, not at the flip out of Initializing, which touches no copy: once the flip commits,
  a rebuild or transfer out of the served store may start, and must not overlap the create (I3). The pending flip
  is tracked apart (`pendingCreateFlips`), so a check meanwhile does not create the store again.
  A failed create, out of space included, leaves nothing at the path and awaits no retry: its record goes, its
  entry stays Initializing and the next device check creates it again. Any other failed open but a lost state (out of
  space, an I/O, lock or resource error) leaves the store offline, unmarked, and its copy AwaitingRetry (the retry
  lives only there): each device scan retries it with backoff (`StoreLoader.retryFailedLoads`, over
  `HostCopies.retries`, 1, 2, 4 ... scans, capped at `Host.LoadRetryCap` 5 min), so an Active store is never left
  unserved and untracked until a restart. The next wait doubles the retry the copy awaited before (an open takes it,
  and carries it to its outcome and, through `awaitingInstall`, to its LoadStore). A load that throws before its open
  (an unreadable `config.yaml`, `StoreLoader.awaitUnreadRetry`), one whose executor slot an earlier close still holds
  (`HostThreading.AlreadyAssigned`) and one whose Store fails to build in `handleLoad` (`awaitBuildRetry`, backoff
  per failure in a row) are left offline and retried the same way. `tryLoadStore` reads no tree, so a retry is held
  to the entries the device's latest reconcile read (`LocalStorageDeviceState.reconciledEntries`, set by
  `reconcileDeviceState`) and to the host's own work: it waits while that entry is neither Active nor Offline, the
  store is `declaredMissing` or a rebuild of it discards or runs (`HostCopies.rebuilding(RebuildHolds)`), its copy is
  unsynced (CopyMarkers), its open is refused while the store has TransferOut
  work on the device (from `startOut`'s close until its move-to-outbound task has run), and it is dropped once the entry has
  left the device (before a first reconcile only the host-work checks apply). A retry
  keeps the waiting load's `onServed` (e.g. its `StoreMoved` gossip) for the open that serves it. A load's `onServed`
  runs only once its LoadStore has installed and recovered the store (`StoreLoader.awaitServed`), never at backend
  open; a LoadStore that leaves the store awaiting a retry (no CRL room, no executor slot, a failed save-file removal,
  a failed build) hands the callback to that entry, and any other failure drops it. An unreadable config
  is logged at error once per retry cap. An Active entry on a loaded device with no `config.yaml` at its store path
  (directory or config lost), not served, opening or retiring, is rebuilt by the reconcile's Missing Stores pass:
  recorded in the device's `declaredMissing` (silent: `missing`), and `StoreRebuilds.requestRebuild`; the request is made
  again at each reconcile while the entry stays Active and the store unserved, even once its directory is back, so a
  failed request never leaves it offline until a restart. Its device-CRL entries stay (nothing durable records the
  declaration, and the directory may come back before the flip commits): the rebuild's retire of a copy that is back
  saves them, the rebuilt copy's empty `crl_save.log` drops what is left, and a re-placement's entry leaving the
  device makes them leftovers. Every load path
  starts from a directory, so nothing else would notice it while the pool counted it writable.
  `DeviceDiscovery` deletes `creating/` when it loads a device. Bootstrap (`cmdline`) creates its stores the same way (`DefaultBackendOpener.createStore`)
- Store lifecycle: the control thread builds the `Store` and calls `router.install`, where ownership passes to the
  executor; a close removes it from the router (its copy Closing, so silent) and then runs the close on its executor. Before
  install and after close the backend and directory belong to the control side, which otherwise reads only
  `storeId`, `path`, `crlSaveFile` and the thread-safe `hasFailed`/`isWriteFrozen`. `release` frees the slot.
  Every exit (transfer out, shutdown, relinquish, fail-stop, failed CRL load) runs `Store.close`, which stops the
  store's drivers; a reload over a loaded store reusing its backend skips the close but calls
  `Store.shutdownDrivers`. A new exit path must do one or the other
- Store identity: `StoreLoader.handleLoad` reads the `incarnation` file (`StoreIncarnationFile.load`) before building
  the Store and never writes it: `StoreCreation` writes it into every new store directory (bootstrap included) and a
  rebuild's `finish` replaces it (`server/store/CLAUDE.md`). A file known absent has the store retired unserved
  (`retireUnserved`: moved to `lost/`, rebuilt as a new incarnation); one that cannot be stat'd, read or
  parsed is left alone and the load retried (`awaitUnreadRetry`, the copy AwaitingRetry), so a format change
  in a release is an outage, never a fresh identity over a store's data and CRL state
- Every existence test under `server/` uses `server.FileStat`, never `os.exists`/`os.isDir`/`os.isFile`
  (`Files.exists`, false on any stat error; `FileStatUsageSuite` fails on one): absent only on ENOENT (or a parent
  that is no directory), otherwise it throws. A new site decides whether a failed stat waits, throws or is safe
  either way, and says so where it stands. `tryLoadStore`'s config check and `handleLoad`'s `crl_save.log`, `acceptor-since` and
  `inherited-votes` checks hold a load whose stat fails back through `awaitUnreadRetry` (a rebuilt store loaded
  without its gates would be a full acceptor of its old incarnation's transactions); the Missing Stores pass, `dropLeftoverCrlState` and the Deleted Stores
  pass skip such a store; `StoreCopies.retire` fails rather than report a clear path, and `StoreCopies.delete`
  reports gone only a copy known absent; `DeviceDiscovery.openDeviceCrl`
  throws (device left for the next scan; a child whose config cannot be stat'd counts as a store directory) and so
  does `holdsOwnState`. `tryLoadDevice` leaves a device whose directory, config or `failed` marker cannot be stat'd
  for the next scan, and `DeviceMarkers`' host-id reads and `storesIn` throw too. `StoreLoader.readArrivedAt` counts
  a failed stat as an arrival now; `adoptionClass` throws (adoption retried next scan), and so does
  `bringIntoService` on a store directory it cannot stat, before it rewrites `owning-host`, so no adopted store loads
  without its `ArrivedAtFile`; `RebuildingStore.copyOnDisk` throws on a `phase-param.rebuilt` it cannot stat or read;
  `startOut` never ships a Lost copy; `retireForeignCopy` and `startIn` wait for a later check (an arrived copy's
  `phase-param.arrived` that cannot be read leaves its device unloaded, P6: it says nothing of which transfer the copy
  belongs to). Tests fail a stat with a directory lacking search permission, or
  a link into one
- Load time and the forget floor (`server/store/CLAUDE.md`): every Store `StoreLoader` builds records its load
  (`Frontend.loadedAt`), so after a restart, adoption or transfer in it refuses, for good, each transaction it does
  not hold that started before the load plus `MaxStartLead`. A store that arrived from another host had joined by that
  host's clock: `StoreTransfers.bringArrivedLive` (before the move out of arrived/) and an adopting
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
  is left offline, its file kept, and retried every device scan (its copy AwaitingRetry; one warning per
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
- `crl_save.log`, `outbound/` and `arrived/` are for network transfers only: a transfer out closes the
  store with `extractCrl`, copying its state from the device CRL into `crl_save.log` to travel with it. Such a file is
  then the only copy of that state: if it fails to load (missing, truncated, unparsable, or naming another store) the
  store is never served. A read that fails without saying anything of the content (`CrashRecoveryLog.isUnreadable`:
  EIO, EMFILE, out of memory; `loadValidStoreState` throws it rather than return `Left`) is never taken for that:
  `handleLoad` retries through `awaitUnreadRetry`, `holdsOwnState` (which reads the store config the same way) throws
  before any store is marked, `adoptionClass` fails the adoption for the next scan, and `StoreTransfers` neither
  resumes nor discards a promoted copy it cannot read. It is moved to `lost/`, its entry is flipped to Rebuilding by the reconcile, and the
  copy is retired (below) before the rebuild. Adoption of a device does the same for an `outbound/` copy
  without a valid save file instead of refusing the device, and adopts any copy in `lost/` as Rebuilding
  whatever its other markers. A network transfer validates the staged copy's save file before promotion and retries on
  failure, since the source still holds the state; a resume of a promoted copy's pool update (`StoreTransfers.startIn`)
  checks it again, and a copy whose save file no longer validates is deleted and the
  transfer restarted, as is one whose `phase-param.arrived` names another transfer
- A lost device CRL does the same to every store on the device: if `<device>/crl` is absent from a device holding store
  dirs, or holds no stream files there (`openCRL(..., holdsStores = true)` throws `CrlMissing`: a device's stores exist
  only once its CRL was opened, so an emptied `crl/` is never taken as a new CRL), or `openCRL` throws (a corrupt
  CRL, `CrlCorrupt`, is logged as such), each store copy is moved to `lost/` (one there stays) except one that held nothing in it
  (`holdsOwnState`): one whose `crl_save.log` validates as its own (its whole state is the file, which its load uses
  over any CRL), or a finished rebuild in `rebuilt/` (never served, so its state is empty; its flip and
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
- A backend fail-stop during a store's close moves it to `lost/` instead of closing it cleanly. A
  `crl_save.log` a transfer-out close wrote stays: its entries have left the device CRL, so it is the only copy of
  the store's transaction state, and the retire keeps it as the copy's lost CRL state. A copy in `lost/` is never
  loaded, so its save file is never read as its state. Such a store is never transferred: its transfer is aborted
  so it is rebuilt
- Store copies (`StoreCopies`). A store has one owner: the device its pool names. Ownership moves by a transfer or a
  rebuild and takes effect when the new device's entry becomes Active. From then on every other copy, whatever it
  is, may go, and only then. So a copy that stops being the owner's is never deleted where it stands. This covers a
  completed transfer out's source and a lost copy, whether its rebuild runs here or was re-placed away. Instead it is
  **retired** (`StoreCopies.retire`), each step durable before the next:
  1. the copy is moved to `lost/` (`StoreRebuilds.moveToLost`), unless it is there;
  2. its CRL state is saved into the copy's `lost-crl-state.log`. A file already there is kept. Otherwise the copy's
     `crl_save.log` is renamed to that name (with any device-CRL entries the store still has, after a crash before
     a transfer out deleted them, saved beside it first as `lost-crl-entries.log`: the file may be what was found
     unusable), or else the store's device-CRL entries are read and saved;
  3. `CrashRecoveryLog.dropStore` removes the entries from the device CRL;
  4. the copy is moved from `lost/` to `<device>/pending-delete/<copy>` under its copy name (`CopyMoves.move` to
     Retired; T11).

  A crash at any later step leaves the copy in `lost/`, so the next retire redoes what remains. Three passes retire:
  - The Rebuilding pass, for a Lost copy of a store rebuilt here
    (`StoreRebuilds.discardLostStore`). It holds a rebuild slot from its start (`maxConcurrentRebuilds` counts its
    Rebuild work's DiscardingLost stage), and the slot passes straight to the rebuild in the same callback
    (DiscardingLost -> Running), so one reconcile cannot retire every Lost store at once. Discards count only on
    loaded devices (`HostCopies.discards`), as does `isRetiring`: a device unloaded mid-retire (`crlFailed`, a
    relinquish) drops its discard work and frees the slot while the orphaned retire's CRL calls finish. That is safe:
    the orphan's only remaining destructive step, the move to `pending-delete/`, is skipped once its device is not
    loaded, and its CRL is being shut down; running rebuilds, by contrast, count by handle (`runningRebuilds`). The handoff re-reads `reconciledEntries`: the rebuild starts only if the entry
    is still Rebuilding under the episode the discard began for; otherwise the slot is released and the device checked
    again (a retire held up by a slow CRL can outlast a re-placement). A failed retire is a failed pass: it re-checks
    the other devices and schedules this device's backoff re-check (`scheduleRebuildRetry`), which also starts a
    transfer in the discard held back. Neither the Lost CRL pass
    (`DeviceReconciler.flipLostCrlStores`) nor `startRebuild` acts on a store while
    `storeTransferService.isTransferringOut` (host-wide, by store id) holds it (a transfer aborted while its send runs, its copy then Lost):
    the discard would move the directory the send may still read. Each schedules a device re-check instead, as the
    Abandoned Transfers Out pass does; `startRebuild` asks only when no rebuild of the store is registered, since a
    registered one's end re-checks the device anyway. `startRebuild` also waits while a transfer out is still closing
    the store (`StoreTransfersOut.deferRebuildUntilClosed`: TransferOut work Closing on any device): the close's end, whatever its outcome,
    checks the device again, which starts the rebuild.
  - `StoreTransfers.startIn`, for a copy in any phase but this transfer's Arrived (Live, Rebuilt, Parked,
    Outbound or Lost) an earlier tenancy left on a device a store is transferring onto (re-placed away while the
    host was down, then transferred back; a store transferred back onto its old source; rebuild-process, P1). Its
    retire moves it to Lost first (T10, "stopped being the owner's"). It retires before asking for bytes, so the
    earlier copy's device-CRL entries are saved before the new copy's first, save-file load drops them; it waits
    while a rebuild of the store runs here, the store is opening, or the copy here is served, opening or closing
    (not while the transfer's source is served from another device of this host: it closes only for the
    StartStoreTransfer sent once the copy is retired), and checks the device again once it is retired, which
    starts the transfer.
  - The Deleted Stores pass, for an offline store whose entry has left the device. The store stays offline until its
    path is clear, so a failed retire is retried.
    A store served from the device that its entry does not list (a copy put back by hand: `tryLoadStore` reads no
    tree) is first taken out of service as a failed backend is (`StoreRebuilds.retireUnlistedServed`: closed,
    then moved to `lost/`, offline), so the check that follows retires it.

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
  are never loaded, so they are deleted in place: `creating/`, transfer staging (`transferring-in/`, every attempt's
  directory) and `deleting/` are swept whole at device load (`DeviceDiscovery.loadOwnedDevice`); `rebuilding/<id>` holds a resumable checkpoint and is deleted
  only by `StoreRebuilds.abandonUnlisted` or a first pass's `createStaging`.

  Every rename of a copy, of a store's CRL state, or of a lost device CRL goes through `DurableFiles.move`, which
  forces both directories. Until a retire drops them, a store's entries stay in the device CRL. A transfer out reads
  them (`readStoreState`, leaving them live) into `crl_save.log`, whether the store is loaded or not
  (`StoreTransfersOut.closeForTransferOut`), and deletes them from the device CRL (`deleteTransactions`) only once that
  file is durable: a crash or failed save before that leaves the device CRL holding them. A closed store that already
  has a valid `crl_save.log` ships it unchanged: the file is its whole state, so its device-CRL entries are copies or
  an earlier tenancy's leftovers, dropped (`dropStore`) as its load would drop them (`extractUnloaded`). A closed
  store is extracted on the transfer's source device only (`startOut` passes `sourceDs`), never on another copy of it
  another device of the host holds. Its directory and save file are tested with `FileStat`: a failed stat fails the
  extraction rather than read as an absence (which would write the CRL's entries over a valid file). The store must
  stay closed from the extraction to its move into `outbound/`. Its TransferOut work on the source device, from
  `startOut`'s close until the control task that moves it, refuses every open of the copy (R12,
  `StoreLoader.startOpen`; a load retry included), so the extraction and the move task need no re-check. An open of
  the store's copy on another device of the host is not refused: it reads that device's CRL, never this extraction's
  (the host-wide open count that failed the extraction for it was dropped on purpose). A load of a save file still
  queues its saves behind a device CRL's earlier deletions (its empty `dropStore` is a CRL barrier,
  `server/crl/CLAUDE.md`), so they create records rather than being skipped. The extraction releases only the charge retained when it began
  (`releaseRetained(storeId, charge)`). `startOut` is single flight per store (I3): a request (a retransmit, a
  readdress) that arrives while TransferOut work Closing holds the store (any device), its close or `extractUnloaded` still running,
  does nothing (logged at debug); the receiver's retransmit tries again if the first fails (`WorkOverlapSuite`).
  `DeviceReconciler.dropLeftoverCrlState` drops the entries of a store with no entry on the device, no directory
  (a stat that answers absent) and no retire running, as a safety net for a directory removed by hand; a store its
  entry names keeps them, even one the Missing Stores pass is rebuilding. It releases only the admission charge
  retained for the store when it was issued, and only if unchanged at completion (`CrlAdmission.retainedCharge` /
  `releaseRetained(storeId, charge)`, by identity): a store landing meanwhile charges records the drop does not
  remove, and its recovery releases the whole charge
- A rebuilt store's copy is in `rebuilt/` from its finish until its entry leaves Rebuilding; `tryLoadStore` leaves
  such a store offline (`server/rebuild/CLAUDE.md`). An `Adopt` whose move to Live fails leaves the copy offline on
  its device (its Rebuilt record), so the Completed Rebuilds pass retries the move and the load at each device check.
  A move whose directory sync failed leaves the copy Live and unsynced (CopyMoves), and the next device check forces
  it and loads it (`loadResynced`). Tests fail the rename through `CopyMoves.rename` and the sync through
  `CopyMoves.syncDirectory`
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
  no longer Rebuilding on its device, or Rebuilding there under another episode (its Running work's episode; its
  completion checks the device again; the slot release clears its Abandoned stage whatever the outcome, since a
  rebuild past finish's latch re-check ignores the cancel), and deletes a `rebuilding/<id>` staging copy only when no rebuild of it is running.
  The reconcile runs it before the transfers-in pass, and `StoreTransfers.startIn` receives nothing while rebuild work
  of the store is registered on the device (`StoreRebuilds.hasRebuildWork`: its Rebuild work in any stage, queued,
  discarding, or running, a cancelled one included until its slot is released): the store re-placed away and
  transferred back must not be received while its earlier rebuild still runs here. The rebuild's end checks the device
  again, which starts the receive (I3, `WorkOverlapSuite`). A
  flip of a rebuild whose entry has since moved to another episode is `RebuildOutcome.Superseded`: nothing is
  written, and its copy, Rebuilt for the earlier episode, is retired by the next `startRebuild`. That copy, like one
  whose flip failed permanently, stays offline on its device (its Rebuilt record, as a restart's load would leave
  it), so the Deleted Stores pass retires it if its entry leaves the device first. A flip retried
  after `TransactionOutcomeUnknown` never discards its copy on a device not Failed: its earlier attempt may have made
  the copy the owner's before a transfer out or removal moved the entry on, so an entry it would otherwise `Discard`
  for is `RebuildOutcome.Unconfirmed`, kept the same way for the transfer, Abandoned Transfers Out, Completed Rebuilds
  or Deleted Stores pass (rebuild-process 5.7). A transfer ships it from `outbound/` (Rebuilt -> Outbound, T6); the
  destination receives it as any arrival (Arrived, then Live once its pool update commits, `recordTransferIn`), its
  `phase-param.*` files inert there.
  `StoreTransfersOut.startOut` refuses (logged at debug) while a rebuild of the store discards or runs
  (`HostCopies.rebuilding(RebuildHolds)`, the flip's retry after an Unknown attempt included), so a transfer made meanwhile
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
