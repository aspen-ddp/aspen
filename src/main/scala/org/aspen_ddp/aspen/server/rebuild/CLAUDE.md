### Store rebuilds
- Files: `RebuildingStore.scala` (the pass: walk, restore, retry, finish; the files a rebuilt copy carries),
  `PendingAllocationSweep.scala` (the peer query for allocations held unfinalized and the direct restore of those
  objects, reaching the pass through callbacks), `RebuildState.scala` (checkpoint format), `RebuildDisk.scala` (all
  staging disk work: the checkpoint's `save`/`load`/`removeAll`, `cleanup`, finish's `complete`), `StoreRebuild.scala`.
  `RebuildingStore.missMark` builds the ErrorTree mark (missed at `HLCTimestamp.Zero`) every rebuild record uses
- Staging I/O runs on the destination device (`io`, the destination's `DeviceIo`): every `RebuildDisk` call is a
  `JobKind.Staging` job on its management lane (`RebuildingStore.staging`), and the staging backend's writes, flushes
  and close are Futures on its data/serial pools (`Backend.rebuildWrite`/`rebuildFlush`, `server/store/CLAUDE.md`).
  Nothing waits (no `Await`); `client.clientContext` only composes the pass and runs the peer reads, so a hung
  destination stalls only its rebuild, never the client pool or the control thread (the constructor's `start` does
  no disk work: the checkpoint load, an in-place copy's staging cleanup and everything after are jobs). The walk
  waits for each checkpoint (`recordRestored`'s Future), and a checkpoint reads the walk's position before its flush,
  so it never names an object the flush does not cover. Every best-effort checkpoint before a failed pass completes
  before the failure. `RebuildingStoreIoSuite` holds the destination in lockstep (`LockstepThreading.hold`) to show
  the checkpoint waiting there and a staging write waiting on the data pool while client transactions commit
- A rebuilt store lost its Paxos acceptor state. `RebuildingStore.finish` writes `acceptor-since` (finish time plus
  `AcceptorSinceMargin`, 10 s) into its directory; how the Frontend enforces it: `server/store/CLAUDE.md`. The
  margin covers `Frontend.MaxStartLead` (2 s) plus host-to-host clock skew; client clocks do not enter into it.
  The gate files' reads and writes are `RebuildDisk`'s (`writeAcceptorSince`, `readAcceptorSince`, `writeInheritedVotes`,
  `readInheritedVotes`, `writeEmptyCrlState`), run only inside device jobs (finish's completion job, a load's
  `LoadDisk.readLoadInputs`). `readAcceptorSince` and `readInheritedVotes` stat with `server.FileStat`: None only for a file known absent; a
  failed stat throws, and `StoreLoader.handleLoad` holds the load back. An unreadable file reads late (only mutes more)
- `finish` also writes `inherited-votes` (`writeInheritedVotes`): the pass start (`passStartMillis`, reset by each
  `runPass`) plus the `horizon` parameter (default `TransactionHorizon.Horizon`, the longest a host uses) plus twice
  `AcceptorSinceMargin`. From then on every vote on a transaction the old incarnation joined has been cast, so the
  rebuilt store's first query of its peers' held votes after it is complete (`Frontend.inheritedVoteLock`,
  `server/store/CLAUDE.md`). The same round finds the KeyValue objects the rebuilt copy may hold without a growth the
  old incarnation voted for, and guards them against growth until repaired (Inherited growth, same guide); the
  rebuild itself does nothing for it
- `finish` also writes a fresh `incarnation` file (`StoreIncarnationFile.writeFresh`) into staging, overwriting any
  present, after `acceptor-since` and before `phase-param.rebuilt`: a rebuilt store is a new incarnation
- `finish` also writes an empty `crl_save.log` (`RebuildDisk.writeEmptyCrlState`) before `phase-param.rebuilt`: the
  rebuilt store's whole transaction state. Its first load, by any path, is then a save-file load, which drops whatever
  the device CRL holds under its id (an earlier copy's leftovers: one removed without a retire, or a fail-stopped copy's
  close saving behind its retire's drop) before loading it (`server/host/CLAUDE.md`). `keepRebuiltStoreOffline` relies
  on the file being there
- Rebuild episode (rebuild-process.md 2.1, B18): every transition into Rebuilding writes a fresh UUID into the
  entry's `transferId` (`StorageDeviceState.setRebuilding`, `StoreEntry.rebuildEpisode`), and `RebuildingStore`
  runs for the episode the reconcile read (`StoreRebuildFactory.createRebuild`'s `episode`). The checkpoint carries
  it (`RebuildState.episode`); `runPass` treats one of another episode as no checkpoint, so staging is discarded
  and the walk starts over. `lossAccepted` honours `acceptLoss` only on an entry of its episode
- Completion identity (rebuild-process 2.2, 5.6): `finish` writes the episode (empty for None) durably as
  `phase-param.rebuilt` in staging, the last write, then renames staging into `<device>/rebuilt/` under a fresh copy id
  (T2), and `complete` reports that id (`StoreRebuild.complete: Future[UUID]`), with which the host records the copy. A
  rename that landed but was not forced (`DurableFiles.MoveNotForced`; the sync is the `syncDirectory` constructor
  seam) completes all the same, reported by `placedUnforced`: the host records the copy unsynced and flips it only once
  a device check has forced it, by restarting the rebuild, which takes the copy for done (never a second rebuild). A
  copy in `rebuilt/` is a finished rebuild awaiting its flip: `start()`, run by the constructor on the control thread
  (no disk work there: the checkpoint's load and any staging cleanup are its own `Staging` jobs, so `StoreRebuilds`
  constructs it directly), reads its `existingCopy: Option[(Phase, os.Path)]` (`StoreRebuildFactory.createRebuild`'s, built by
  `StoreRebuilds.startRebuild` from the store's copy record on the device; nothing lists the device) and takes a copy
  for done only when it is Rebuilt for its own episode (`RebuildingStore.isRebuiltFor`), failing on any other copy of
  the store outside `pending-delete/`. Tests build it with `CopyFixtures.existingCopy`. `StoreLoader.tryLoadStore`
  leaves a Rebuilt copy offline, and `StoreRebuilds` moves it once the flip commits: `Adopt` to Live before the load
  (`bringRebuiltLiveOnLane`, T3), `AdoptOffline` to `parked/` in one move (`keepRebuiltStoreOffline`, T4), each a
  `Move` job on the device's lane whose applied result releases the rebuild's slot, `Discard` deletes
  it rename-first (T13). The reconcile's Completed Rebuilds case moves an offline Rebuilt copy of an Active entry Live
  after a crash. A copy of a Rebuilding store that is not this episode's Rebuilt copy (a copy finished for an earlier
  episode included), told by its record's phase, is retired as a lost copy (`StoreRebuilds.retireForeignCopy`: moved to
  `lost/`, then retired into `pending-delete/`, rebuilt). Such a copy, or one already in `lost/`, may have served since
  the first pass began, so the store's staging checkpoint is first removed durably (`dropStagingCheckpoint`, a `RebuildBookkeeping` job on
  the device's lane whose apply continues the start) and the next pass is a first pass: a resume would leave the
  walked objects' updates stale. The record is read again from the
  disk first (`HostCopies.reread`), so a copy back since the device's load is retired with its device-CRL entries. One
  back while the rebuild runs is found by its completion, which lists the store's copies before the flip (a
  `RebuildBookkeeping` job, its slot held until the outcome releases it) and discards the rebuilt copy unflipped, so the next pass retires the returned copy and rebuilds again (`host/CLAUDE.md`)
- Pending allocations (rebuild-process 5.3): before each pass's walk, `queryPeers` asks the pool's other stores
  (`CurrentAllocationsRequest`) and waits, resending, for `width - writeThreshold + 1` answers, at most
  `peerQueryTimeout` (default `PeerQueryTimeout`, 60 s, noticed at the next resend): then the pass fails with
  `PeerQueryTimedOut` into the host's backoff retry, after a best-effort checkpoint in the sweep. That many answers
  include a holder of every committed, unfinalized allocation the lost store voted for, and the count never exceeds
  the consistent read threshold. The listed set is merged into `RebuildState.pendingAllocations`, which every
  checkpoint carries (a checkpoint without it does not load): a resume sweeps what earlier passes listed, since one
  finalized meanwhile may have its tree entry behind the checkpoint. That is the only cover for such an entry; no
  timestamp filter can be (its writer may prepare long before it is decided: rebuild-process 5.3). An allocation the
  lost store acknowledged is listed at the first query or already in the tree. After the walk, `sweepPending` restores
  the listed objects directly, re-querying in rounds (1 s doubling to 30 s): unresolved waits; committed restores,
  retrying any read failure; no longer listed restores, or settles on `InvalidObject.provesDeletion`. It never waits
  on a finalization, which could need this store. A cancel abandons the wait (`CurrentAllocationsQuery.Abandoned`).
  Nor does it wait on resolution past `pendingWaitLimit` (default `PendingWaitLimit`: horizon plus
  `Frontend.DefaultExpiryMargin` plus `MaxStartLead`) from the sweep's start: a transaction undecided between its live
  voters is held by rule G while this (silent) store is rebuilding. What is still unsettled then is marked in the
  ErrorTree against this store (`markUnsettled`, missed at `HLCTimestamp.Zero`) before `finish`, never dropped
  unmarked (the lost incarnation may have applied and acknowledged a commit its peers do not know yet); repair
  settles each entry whatever the outcome. A failed mark fails the pass
- Locked copies (rebuild-process.md 5.2): `restore` copies from an ordinary read, which ignores write locks of
  transactions not known committed. When any reply reported one (`ObjectState.readLocks`), the object is marked in the
  ErrorTree against this store (missed at `HLCTimestamp.Zero`) before the restore completes, so a commit the lost
  store applied and acknowledged while a peer had not is repaired; repair defers while the lock is reported. A failed
  mark is a failed object
- Unreadable objects (rebuild-process 5.5): `retryFailures` fails the pass while any object is still unreadable, unless
  the store's Rebuilding entry carries `acceptLoss` (set by `accept-rebuild-loss`). Then each one still in the
  AllocationTree is marked in the ErrorTree against this store (`MissedUpdateMarker.mark`, missed at
  `HLCTimestamp.Zero`), durably before `finish`, and the pass finishes with a `REBUILD ACCEPTED LOSS` error line; repair
  restores the slice if the object becomes readable. A failed mark fails the pass. The `MaxFailedObjects` abort is not
  lifted by an acceptance. An out-of-space retry latches the abort (`latchOutOfSpace`, `REBUILD OUT OF SPACE`); the
  latch is checked after the retries, before `lossAccepted`, so the pass fails with the latched cause and a full
  destination never becomes an accepted loss
- Timed object reads (rebuild-process.md 5.2): every object read of the walk, the sweep and `retryFailures` goes
  through `client.withReadTimeout(objectReadTimeout)` (default `ObjectReadTimeout`, 30 s). A `ReadTimeout` is a
  failed object like any other read failure: the walk records it and moves on (toward `MaxFailedObjects`, and
  `maxConsecutiveReadTimeouts`, default `MaxConsecutiveReadTimeouts` 100 in a row with no restore between, which
  latches an abort with the checkpoint kept), the sweep
  keeps it pending for the next round, and a cancel takes effect within one timeout. Tree-node reads (the walk itself,
  `allocationTree.get`) and the pool lookup stay untimed: they read the metadata pool, whose loss stalls the rebuild
- Placement alarms (rebuild-process 4.2, 5.2): `REBUILD CO-LOCATED` (the selection had to share a device with the pool),
  `REBUILD OUT OF SPACE` (from any write of the walk, the sweep, a checkpoint, or `finish`'s flush and completion
  files), and a warning for a store with no recorded usage. The remedy for either is re-placement,
  `transfer-store` on the Rebuilding store (`server/host/CLAUDE.md`)
- Rebuild work lives only in the copy registry: `Work.Rebuild(episode, stage)` on its device, stage Queued (waiting for
  a slot), DiscardingLost (its lost copy's retire), Running, or Abandoned (cancelled by `cancelRebuild`, still running
  until its slot is released). `StoreRebuilds` keeps only what Work cannot hold: each running rebuild's handle and
  settled promise (`runningRebuilds`), the pending flips, requests and retry backoff. One predicate answers "is a
  rebuild registered", `HostCopies.rebuilding(stages)`, each caller naming its stages (R20): `RebuildReads` (Running,
  Abandoned: repairability, the Completed Rebuilds case, an earlier tenancy's retire, a second start), `RebuildHolds`
  (plus DiscardingLost: a load retry waits, `startOut` refuses), all stages for silence; `hasRebuildWork(store, device)`
  is any stage on one device. `startRebuild`'s host-wide "a rebuild of the store runs" also counts a running handle, so
  a rebuild still running for a device unloaded under it (crlFailed cancels nothing, and a cancel past the abort latch completes; its Work went with
  the device's records) blocks a second rebuild of the store until it releases its slot. Slots (`maxConcurrentRebuilds`)
  are per destination device (`StoreRebuilds.slotsTaken(deviceId)`): each device holds at most that many, counting the
  rebuilds running onto it by handle and the discards on it by work. Every slot holder waits only on its own device's
  lane (its staging jobs, its finish's listing, its flip's move, its discard's retire), so a hung device holds only its
  own slots and a rebuild onto a healthy device always starts (spec 7a); within a device, queued rebuilds wait for a
  slot there; a discard hands its slot to the rebuild with no gap
  (`startRebuild(handoff = true)`: DiscardingLost goes straight to Running, the discard's own slot not counted), and a
  rebuild that does not start leaves the work Queued. An outcome that ends the work ends it at the slot's release, after
  the outcome's steps (an Adopt's move to Live and load), so the store stays Rebuilding-silent throughout
- Silence (rebuild-process 6.2): `StoreRebuilds.startRebuild` registers the store's Rebuild work on its device at the
  first reconcile that sees its Rebuilding entry, and the work keeps the store silent
  (derived silence, `server/host/CLAUDE.md`), so its host drops its messages rather than answering UnknownStore. No
  other path can unsilence it while the work is held (another device's Deleted Stores case after a same-host
  re-placement only removes that device's copy). `Adopt`, `AdoptOffline`, `Discard`, `Unconfirmed`, `NotOwned`, a
  cancel for an entry that left the device and `endQueuedRebuild` for a queued one end the work; whatever else names
  the store (the finished copy, until its load serves it; an unloaded device's entry) keeps it silent. Other outcomes
  (Superseded, a failed flip, pass or a shutdown's cancel) keep the work, queued, for the retry, until the entry
  leaves Rebuilding: each reconcile first ends such work (`EndQueuedRebuild`, decide's first case), so the rest of its decision finds
  the copy free (a
  copy with work on its device is left to that work). A queued or discarding rebuild never makes a store served
  elsewhere here unrepairable; a running one, or an abandoned one still running, does (R20)
- One piece of host work per (device, store) at a time (I3): while a rebuild of a store is registered on a device
  (queued, discarding its lost copy, running, or cancelled and not yet released), a transfer in of that store onto the
  device waits (`StoreRebuilds.hasRebuildWork`, checked by `StoreTransfers.startIn`; `server/host/CLAUDE.md`). A lost
  copy's discard hands its slot to the rebuild only for the episode it began for, re-read from the device's latest
  reconcile (`discardLostStore`); an entry moved on meanwhile gets a fresh device check instead, and a failed one the
  backoff re-check of a failed pass. A new store's create work ends once its copy is served, before its flip commits, so
  a rebuild of a store whose backend failed right after its create never overlaps the create
  (`StoreRebuilds.createNewStore`). A rebuild is neither requested (Lost CRL case) nor started (`startRebuild`) while a
  send of the store is still registered (`isTransferringOut`, host-wide by store): its discard would move the directory
  being shipped. A device re-check follows. Nor is one started while a transfer out is still closing the store
  (`StoreTransfersOut.rebuildWaitsForClose`); the close's end, whatever its outcome, re-checks the device. Conversely,
  `StoreTransfersOut.startOut` refuses while a rebuild of the store is registered, its flip's retry included; the
  receiver's retransmit starts the transfer once it ends
- A rebuild's staging store opens through the same `BackendOpener` as a normal load (passed by
  `StoreRebuildFactory.createRebuild`). A first pass (no checkpoint) discards whatever staging holds and creates a
  fresh staging store atomically (`BackendOpener.create`); a resume only opens it, and one whose backend is lost
  (missing or blank files included) is discarded, checkpoint and all, so the next pass starts over
- A promoted bootstrap-pool store may hold stale copies of the bootstrap pool's own ErrorTree/UnackedCommits nodes
  (their writes are untracked, so the rebuild recorded no misses). Its first bootstrap tree sweep after promotion
  (within one scan floor) reconciles every node of both trees (`server/repair/CLAUDE.md`), closing the staleness window of rebuild-process 8.3
- What triggers a rebuild (a copy in `lost/`, lost CRLs, failed transfers) and when the old copy in
  `pending-delete/` may be deleted: `server/host/CLAUDE.md`
