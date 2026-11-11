### Repair
- Inputs: ErrorTree entries for stores that missed `errorTimeout` and UnackedCommits entries
  (`client/internal/transaction/CLAUDE.md`), both kept in the pool's metadata trees
- Repair reconciles exactly (`server.store.Reconcile`): it snapshots the store's copy, reads the object, then
  installs on the executor only what still matches the snapshot: KV objects key by key, refcounts by the higher
  serial (never lowered), DataObject data by revision when the read's timestamp is at least the store's. Keys and
  object-level parts a read reports locked (`ReadLocks`) are deferred; nothing is written while the object has a
  commit in flight
- One `StoreRepairer` scan walks the store's ErrorTree range, then its UnackedCommits range (skipping transactions
  the store holds live). It drops an applied stub with its entry, and an orphan stub only once two consecutive scans
  found it so (`server/transaction/CLAUDE.md`)
- After `RepairPolicy.proofAfterDeferrals` (10) lock-deferred passes in a row, `RepairProofs` commits a no-op
  transaction proving the read state, and reconciles from it
- A repair scan runs only while the store's pool entry names this host, so a superseded copy never consumes the
  current copy's ErrorTree entries
