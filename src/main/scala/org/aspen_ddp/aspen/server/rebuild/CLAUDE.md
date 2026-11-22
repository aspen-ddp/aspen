### Store rebuilds
- Files: `RebuildingStore.scala` (the pass: walk, restore, retry, finish; the on-disk marker files), `PendingAllocationSweep.scala` (the peer query for allocations held unfinalized and the direct restore of those objects, reaching the pass through callbacks), `RebuildState.scala` (checkpoint), `StoreRebuild.scala`. `RebuildingStore.missMark` builds the ErrorTree mark (missed at `HLCTimestamp.Zero`) every rebuild record uses
- A rebuilt store lost its Paxos acceptor state. `RebuildingStore.finish` writes `acceptor-since` (finish time plus
  `AcceptorSinceMargin`, 10 s) into its directory; how the Frontend enforces it: `server/store/CLAUDE.md`. The
  margin covers `Frontend.MaxStartLead` (2 s) plus host-to-host clock skew; client clocks do not enter into it.
  `readAcceptorSince` and `readInheritedVotes` stat with `server.FileStat`: None only for a file known absent; a
  failed stat throws, and `StoreLoader.handleLoad` holds the load back. An unreadable file reads late (only mutes more)
- `finish` also writes `inherited-votes` (`writeInheritedVotes`): the pass start (`passStartMillis`, reset by each
  `runPass`) plus the `horizon` parameter (default `TransactionHorizon.Horizon`, the longest a host uses) plus twice
  `AcceptorSinceMargin`. From then on every vote on a transaction the old incarnation joined has been cast, so the
  rebuilt store's first query of its peers' held votes after it is complete (`Frontend.inheritedVoteLock`,
  `server/store/CLAUDE.md`). The same round finds the KeyValue objects the rebuilt copy may hold without a growth the
  old incarnation voted for, and guards them against growth until repaired (Inherited growth, same guide); the
  rebuild itself does nothing for it
- `finish` also writes a fresh `incarnation` file (`StoreIncarnationFile.writeFresh`) into staging, overwriting any
  present, after `acceptor-since` and before the marker: a rebuilt store is a new incarnation
- `finish` also writes an empty `crl_save.log` (`RebuildingStore.writeEmptyCrlState`) before the marker: the rebuilt
  store's whole transaction state. Its first load, by any path, is then a save-file load, which drops whatever the
  device CRL holds under its id (an earlier copy's leftovers: one removed without a retire, or a fail-stopped copy's
  close saving behind its retire's drop) before loading it (`server/host/CLAUDE.md`). `keepRebuiltStoreOffline`
  relies on the file being there
- Rebuild episode (rebuild-process.md 2.1, B18): every transition into Rebuilding writes a fresh UUID into the
  entry's `transferId` (`StorageDeviceState.setRebuilding`, `StoreEntry.rebuildEpisode`), and `RebuildingStore`
  runs for the episode the reconcile read (`StoreRebuildFactory.createRebuild`'s `episode`). The checkpoint carries
  it (`RebuildState.episode`); `runPass` treats one of another episode as no checkpoint, so staging is discarded
  and the walk starts over. `lossAccepted` honours `acceptLoss` only on an entry of its episode
- Completion identity (rebuild-process 2.2, 5.6): `finish` writes a durable `rebuild-complete` marker in staging just
  before the move; its content is the episode (empty for None). A store directory carrying it is a finished rebuild
  awaiting its flip: `start()` takes the final path for done only with the marker of its own episode
  (`RebuildingStore.isRebuildCompleteFor`), `StoreLoader.tryLoadStore` leaves it offline, and `StoreRebuilds` removes it
  once the flip commits (before the load, or after the transfer-out pair for `AdoptOffline`); the reconcile's Completed
  Rebuilds pass removes it for an Active entry after a crash. A directory at a Rebuilding store's path with neither this
  episode's marker nor `rebuild-required` (a copy finished for an earlier episode included) is retired as a lost copy
  (`StoreRebuilds.retireForeignCopy`: marked, kept in `pending-delete/`, rebuilt). Such a copy, or a `rebuild-required`
  one, may have served since the first pass began, so the store's staging checkpoint is first removed durably
  (`dropStagingCheckpoint`) and the next pass is a first pass: a resume would leave the walked objects' updates stale
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
- Silence (rebuild-process 6.2): `StoreRebuilds.startRebuild` marks a store offline in the router at the first
  reconcile that sees its Rebuilding entry, queued or running (`silenced`), so its host drops its messages rather than
  answering UnknownStore. While it is in `silenced`, no other path clears the mark (`StoreRouter.clearOfflineUnlessHeld`
  keeps it, e.g. for another device's Deleted Stores pass after a same-host re-placement). An adopted store's load
  clears it; `Discard`, a cancel for an entry that left the device, and `abandonUnlisted` for a queued one clear it
  unless a device's `offlineStores` holds it; other outcomes keep it
- One piece of host work per (device, store) at a time (I3): while a rebuild of a store is registered on a device
  (queued, discarding its lost copy, running, or cancelled and not yet released), a transfer in of that store onto
  the device waits (`StoreRebuilds.hasRebuildWork`, checked by `StoreTransfers.startIn`; `server/host/CLAUDE.md`).
  A lost copy's discard hands its slot to the rebuild only for the episode it began for, re-read from the device's
  latest reconcile (`discardLostStore`); an entry moved on meanwhile gets a fresh device check instead, and a failed one the backoff re-check of a failed pass.
  A new store's create work ends once its copy is served, before its flip commits, so a rebuild of a store whose
  backend failed right after its create never overlaps the create (`StoreRebuilds.createNewStore`).
  A rebuild is neither requested (Lost CRL pass) nor started (`startRebuild`) while a send of the store is still
  registered (`isTransferringOut`, host-wide by store): its discard would move the directory being shipped. A device re-check follows.
  Nor is one started while a transfer out is still closing the store (`isClosingForTransfer`); the close's end
  re-checks the device.
  Conversely, `StoreTransfersOut.startOut` refuses while a rebuild of the store is registered, its flip's retry
  included; the receiver's retransmit starts the transfer once it ends
- A rebuild's staging store opens through the same `BackendOpener` as a normal load (passed by
  `StoreRebuildFactory.createRebuild`). A first pass (no checkpoint) discards whatever staging holds and creates a
  fresh staging store atomically (`BackendOpener.create`); a resume only opens it, and one whose backend is lost
  (missing or blank files included) is discarded, checkpoint and all, so the next pass starts over
- A promoted bootstrap-pool store may hold stale copies of the bootstrap pool's own ErrorTree/UnackedCommits nodes
  (their writes are untracked, so the rebuild recorded no misses). Its first repair scan after promotion sweeps and
  reconciles every node of both trees (`server/repair/CLAUDE.md`), closing the staleness window of rebuild-process 8.3
- What triggers a rebuild (`rebuild-required`, lost CRLs, failed transfers) and when the old copy in
  `pending-delete/` may be deleted: `server/host/CLAUDE.md`
