### Store rebuilds
- A rebuilt store lost its Paxos acceptor state. `RebuildingStore.finish` writes `acceptor-since` (finish time plus
  `AcceptorSinceMargin`, 10 s) into its directory; how the Frontend enforces it: `server/store/CLAUDE.md`. The
  margin covers `Frontend.MaxStartLead` (2 s) plus host-to-host clock skew; client clocks do not enter into it
- Completion identity (Rebuild F3, F4): `finish` writes a durable `rebuild-complete` marker in staging just before
  the move. A store directory carrying it is a finished rebuild awaiting its flip: `start()` takes the final path
  for done only with the marker, `StoreLoader.tryLoadStore` leaves it offline, and `StoreRebuilds` removes it once
  the flip commits (before the load, or after the transfer-out pair for `AdoptOffline`); the reconcile's Completed
  Rebuilds pass removes it for an Active entry after a crash. A directory at a Rebuilding store's path with neither
  this marker nor `rebuild-required` is retired as a lost copy (`StoreRebuilds.retireForeignCopy`: marked, kept in
  `pending-delete/`, rebuilt)
- Delta walk (Rebuild F10): after the main walk, `RebuildingStore.deltaWalk` walks the AllocationTree again and
  restores entries whose timestamp is at or after `walkStartedAt - DeltaMargin` (horizon + `AcceptorSinceMargin`,
  30 s): allocations the lost store acknowledged whose entry landed behind the walk. `walk-started-at` is the first
  pass's, kept in the checkpoint across resumes (a checkpoint without it does not load). The delta walk does not move
  the checkpoint. Residual: an entry landing after the delta walk passed it (a finalization stalled for longer than
  both walks)
- A rebuild's staging store opens through the same `BackendOpener` as a normal load (passed by
  `StoreRebuildFactory.createRebuild`)
- A promoted bootstrap-pool store may hold stale copies of the bootstrap pool's own ErrorTree/UnackedCommits nodes
  (their writes are untracked, so the rebuild recorded no misses). Its first repair scan after promotion sweeps
  and reconciles every node of both trees (`server/repair/CLAUDE.md`), closing Rebuild F8
- What triggers a rebuild (`rebuild-required`, lost CRLs, failed transfers) and when the old copy in
  `pending-delete/` may be deleted: `server/host/CLAUDE.md`
