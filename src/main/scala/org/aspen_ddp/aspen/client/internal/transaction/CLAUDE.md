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
- Blind writes (`TransactionBuilder.refuseBlindWrites`, transaction-requirements F1): an update with neither a required
  revision nor a content lock may not delete a key without that key's `KeyRevision` (`Exists` is not enough), or
  change min/max/left/right, except in an allocated object's initial content. It fails with `BlindKeyValueWrite`,
  even when replay-safe. Blind Inserts are allowed: stores apply them last-writer-wins, but keep nothing to order
  a delete or an object-level change by
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
- An UnackedCommits entry larger than half a leaf node of its tree (`UnackedCommitEntry.maxEncodedSize`) is written as
  chunks, each a complete entry for some of the objects: chunk 0 under `UnackedCommitEntry.key(store, txid)`, chunk `i`
  under that plus `i` as 2 bytes. Lookups name the head only
- A marker write that gives up (a `TransactionCreationError` or `NodeSizeExceeded`) fails every entry of its batch, and so the
  `MissedUpdateFinalizationAction`s waiting on them; their drivers re-run the finalizers, which mark again
  (`server/transaction/CLAUDE.md`, finalization failure path). No entry is dropped
