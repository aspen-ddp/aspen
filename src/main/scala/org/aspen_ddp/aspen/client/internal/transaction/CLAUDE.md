### Client transaction internals
- Server-side protocol (votes, proofs, finalization, UnackedCommits semantics): `server/transaction/CLAUDE.md`
- Pin rule: a client transaction must contain a pin, a write that invalidates its own requirement once applied (a
  `DataUpdate`/`VersionBump` at a required revision, a `KeyValueUpdate` with a required revision, a `KeyRevision` on a
  key the update writes, a `RefcountUpdate` that changes the count). Allocations, locks and `DoesNotExist` are not
  pins. `TransactionBuilder` fails an unpinned one with `UnpinnedTransaction` unless `Transaction.replaySafe(reason)`
  marked it (reason logged in `notes`; replay must be provably harmless); the retry helpers treat it as permanent.
  So a forgotten transaction replayed by a delayed prepare cannot gather a write threshold of commit votes, and there
  is no time horizon. A refcount-0 object is `MissingObject` for every requirement. Design:
  `docs/superpowers/specs/2026-11-09-replay-pins-design.md`
- UnackedCommits (`StoragePoolState.UnackedCommitsTreeKey`, `UnackedCommitEntry.key(store, txid)`): for every
  committed transaction, the finalizer writes `UnackedCommitEntry(timestamp, objects)` for each counted commit voter
  that sent no TxCommitted (`FinalizationAction.commitVoters`; all stores that sent none when voters are unknown),
  in the ErrorTree batch. `MissedUpdateFinalizationAction` runs last, so an entry durable means every other
  finalization action is done
- `MissedUpdateFinalizationAction` disables missed-update tracking on an ErrorTree write only when the transaction
  references the pool the entry is about (in practice the bootstrap pool)
- A store that misses `errorTimeout` (2 s) is marked in its pool's ErrorTree for every object it hosts that the
  transaction changes (a lock changes nothing, so there is nothing to repair and the object's timestamp would not
  catch up). A client's `MissedUpdateMarker` writes one batch per pool at a time and merges waiting entries by key,
  so a slow spell makes batches larger rather than adding colliding writes. Repair: `server/repair/CLAUDE.md`
