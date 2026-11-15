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
- Blind writes (`TransactionBuilder.refuseBlindWrites`, transaction-requirements F1), except in an allocated object's
  initial content: a key Delete needs that key's `KeyRevision` (`Exists` is not enough), a required revision or a
  content lock; a min/max/left/right change needs a required revision (a content lock does not move the revision a
  repair proof locks, repair F11). It fails with `BlindKeyValueWrite`, even when replay-safe, and stores refuse the
  same writes (`BlindWrite`). Blind Inserts are allowed: stores apply them last-writer-wins, but keep nothing to order
  a delete or an object-level change by
- `TransactionBuilder.addAllocatingObject` adds the object's `AllocationFinalizationAction` itself, once per object, so
  every allocation records its AllocationTree entry, which repair and rebuild find objects by (rebuild F17).
  `ObjectAllocator` does not add it
- UnackedCommits (`StoragePoolState.UnackedCommitsTreeKey`, `UnackedCommitEntry.key(store, txid)`): for every
  committed transaction, the finalizer writes `UnackedCommitEntry(timestamp, incarnation, objects)` for each counted commit voter
  that sent no TxCommitted (`FinalizationAction.commitVoters`; all stores that sent none when voters are unknown),
  in the ErrorTree batch. The entry's incarnation comes from `commitVoters`' `incarnations` map, which the driver fills
  from every prepare reply it got (promise or nack, any round, whether or not it counted votes; a reply without one
  never erases a known value); a store that never answered the driver gets `None`, as does one that reported two
  different incarnations (`TransactionDriver.recordIncarnation`: recorded as unknown for good). Value:
  `[timestamp:8][incarnation:16][count:4][pointers]`; the incarnation is the store's
  `StoreIncarnation` UUID, the zero UUID meaning `None` (unknown), copied into every chunk. `MissedUpdateFinalizationAction` runs last, so an entry durable means every other
  finalization action is done
- `MissedUpdateFinalizationAction` disables missed-update tracking on an ErrorTree write only when the transaction
  references the pool the entry is about (in practice the bootstrap pool)
- A store that misses `errorTimeout` (2 s) is marked in its pool's ErrorTree for every object it hosts that the
  transaction changes (a lock changes nothing, so there is nothing to repair and the object's timestamp would not
  catch up). A client's `MissedUpdateMarker` writes one batch per pool at a time and merges waiting entries by key,
  so a slow spell makes batches larger rather than adding colliding writes. Repair: `server/repair/CLAUDE.md`
- ErrorTree entry: key `[poolIndex:1][objectId:16]`, value `[missedTimestamp:8][objectType:1][storePointer]`
  (`encodeErrorEntry`; type 0 Data, 1 KeyValue, 2 unknown). With the tree's pool these rebuild the object's pointer
  (`ErrorEntry.pointer`), which repair needs to read an object the AllocationTree no longer lists (repair F5). The type
  is unknown only for a commit error on an object the transaction does not reference (not reachable today)
- Marker entries only move forward (repair F10): its puts are conditional checked inserts (`TKVLInsert.replaces`). An
  ErrorTree mark replaces a held one only for a later `missedTimestamp`; an UnackedCommits entry is written only
  where none is held, so a re-write never moves its revision under the scan's delete. Two clients marking one key at
  once abort one batch, which retries; a batch with nothing newer to write commits nothing. Still `replaySafe`
  ("missed-update marks only move forward"): a replay writes a key only where none is held, which costs one more
  repair pass. An UnackedCommits entry re-created after the scan deleted it is harmless: the scan deletes one only
  while its store holds no Tx for T, and a store never rejoins a transaction it forgot
- An UnackedCommits entry larger than half a leaf node of its tree (`UnackedCommitEntry.maxEncodedSize`) is written as
  chunks, each a complete entry for some of the objects: chunk 0 under `UnackedCommitEntry.key(store, txid)`, chunk `i`
  under that plus `i` as 2 bytes. Lookups name the head only
- A marker write that gives up (a `TransactionCreationError` or `NodeSizeExceeded`) fails every entry of its batch, and so the
  `MissedUpdateFinalizationAction`s waiting on them; their drivers re-run the finalizers, which mark again
  (`server/transaction/CLAUDE.md`, finalization failure path). No entry is dropped
