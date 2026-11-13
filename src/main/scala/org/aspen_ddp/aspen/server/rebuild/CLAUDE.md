### Store rebuilds
- A rebuilt store lost its Paxos acceptor state. `RebuildingStore.finish` writes `acceptor-since` (finish time plus
  `AcceptorSinceMargin`, 10 s) into its directory; how the Frontend enforces it: `server/store/CLAUDE.md`. The
  margin covers `Frontend.MaxStartLead` (2 s) plus host-to-host clock skew; client clocks do not enter into it
- A rebuild's staging store opens through the same `BackendOpener` as a normal load (passed by
  `StoreRebuildFactory.createRebuild`)
- What triggers a rebuild (`rebuild-required`, lost CRLs, failed transfers) and when the old copy in
  `pending-delete/` may be deleted: `server/host/CLAUDE.md`
