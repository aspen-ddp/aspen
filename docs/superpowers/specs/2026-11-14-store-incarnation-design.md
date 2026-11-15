# Store incarnation on UnackedCommits entries, and a tighter repair fence

Todo item 2.12. Findings: cross-document X10, G5; rebuild-process N3.

## Intent

UnackedCommits entries are keyed `(poolIndex, txid)`: they name a pool slot, not the incarnation of the store that
occupied it. A rebuild (in place after a lost or corrupt CRL, or onto a new device) creates a new incarnation in the
same slot and discards the old one's CRL, applied stubs included. The new incarnation inherits the slot's entries but
not the stubs that told the old one "you already hold T".

Today only the `acceptorSince` mute keeps a replayed client prepare of T from applying at the rebuilt store through
an entry the old incarnation was owed (rebuild-process N3). The mute compares T's start timestamp with a time taken
from the rebuilding host's clock, so a host clock slow by more than about 8 s defeats it. The consequence is one
store's copy overwritten with T's update over newer state; quorum reads mask it, and the entry's reconcile restores it.

The goal is to remove the clock dependency from that decision: a store applies a transaction through an
UnackedCommits entry only when the entry was written for its own incarnation.

Separately, X10's reverse direction (an old incarnation consuming entries written while the new one was being
rebuilt) is defended by process, not by the entry: `StoreRepairer.ownedHere` checks that the pool's entry names the
scanning host. Two gaps remain in that fence and are closed here. With opportunistic rebuild removed (Todo 2.1), a
consumed entry's stale replica is never healed except by a later write to the object or another rebuild, so the
fence is worth tightening.

## Goals

1. A store applies T through an UnackedCommits entry only if the entry records the incarnation of the store that
   cast the counted commit vote, and that incarnation is the store's own. Clock-free.
2. A found entry is still evidence that T committed, whatever incarnation it names. A mismatch never turns a
   committed T into an abort at the store.
3. Close the two gaps in the repair fence: same-host device moves, and ownership changing mid-scan.
4. Write the G5 timeline (entry production vs consumption across a rebuild or move) once, in the cross-document
   review, including why ErrorTree entries need no incarnation.

## Non-goals

- Incarnations on ErrorTree entries. Their writer has no authoritative source (a store is marked because it was
  silent, and the only source would be a possibly stale cached `StoragePoolState`); ignoring a mismatched mark would
  leave a real miss unrepaired forever. ErrorTree reconcile is exact, so inheriting old marks is harmless.
- Incarnations in `StoragePoolState`. The store must not need a metadata read at load to learn its own identity.
- Replacing the `acceptorSince` mute. The mute protects Paxos acceptor state (X2), which is a different problem.
- Backward compatibility with persisted entries, CRL contents or message formats (not required in this project).

## Anti-goals

- Any path where a mismatch makes a store skip a repair or treat a committed transaction as aborted. Every mismatch
  must fall back to "committed, apply nothing here, reconcile heals".
- A metadata-pool dependency at store load.

## Approaches considered

1. **`incarnation` UUID file in the store directory** (chosen). Written at store creation and by
   `RebuildingStore.finish`; moves with the directory on transfer and adoption. Explicit, single-meaning, no metadata
   dependency. Cost: one more durable file and plumbing to the Frontend and votes.
2. Reuse the `acceptor-since` value as the identity (0 for a never-rebuilt store). No new file, and an equality
   comparison is still clock-free, but it gives one field two meanings across the safety docs.
3. Incarnation counter in `StoragePoolState.StoreEntry`, bumped by the rebuild flip. Rejected: the store would read
   pool state at load to learn its identity (metadata dependency, stale-cache hazard).

Scope was chosen as "UnackedCommits only, plus fence tightening" over "both trees" because the ErrorTree version has
the stale-cache hazard described in Non-goals, and the reverse direction it would close is very unlikely and better
closed by the fence.

## Design

### Store identity

- `StoreIncarnation`: a UUID. Kept in `<storeDir>/incarnation`, written with `DurableFiles.write`.
- `RebuildingStore.finish` writes a fresh one into the staging directory before the `rebuild-complete` marker,
  overwriting any present (a resumed or leftover staging directory must never keep an old identity).
- Load (`StoreLoader`, where `acceptor-since` is read today): read the file; if absent, generate one, persist it
  durably before the store serves, and log at info. This covers new pool stores, bootstrap stores and any
  pre-existing directory. Generating a new identity is always safe: its only effect is that older entries stop
  applying here, which falls back to reconcile.
- Passed to `Store` and `Frontend` like `acceptorSince` (`Frontend.incarnation`).
- Transfers and adoptions move the directory unchanged, so the identity moves with it. Checked: the transfer sends
  the whole directory as a jar archive (`StoreTransferManager.send`, `jar --create .` in the store directory), and a
  rebuild's `finish` moves the whole staging directory into place, so the file travels with both. A rebuild
  interrupted after the marker but before the file exists cannot happen (the file is written first); if the file is
  missing for any other reason, load mints a fresh one, which is the safe direction.
- Stores with no real directory (`MapBackend.path` is `os.root`): never read or write the file there. *(Call:)* such a
  store takes an incarnation from whoever builds it, defaulting to a value derived from its `StoreId`, so it is stable
  across reloads in a test (a recovered commit vote still matches its entries). A test of a rebuilt incarnation passes
  an explicit different value. The `Frontend` parameter has the same default for unit tests that build one directly.

### Votes carry the incarnation

- `TxPrepareResponse` gains `incarnation` (protobuf field, `Message`, `Codec`). Every store sets its own on every
  response, promise or nack. Checked: there is one construction site, `Tx.scala` (the prepare reply), so the
  Frontend's value is always at hand.
- `TransactionDriver` records `msg.from -> incarnation` alongside `peerDispositions`. The latest response from a store
  wins (an incarnation cannot change within one T's voting: a rebuilt store is muted for every T started before its
  `acceptorSince`). *(Call:)* an absent or zero incarnation on the wire decodes as unknown and never overwrites a
  known one.
- `countedCommitVoters` becomes `Option[Map[StoreId, StoreIncarnation]]`: the counted voters and the incarnation each
  voted in. `FinalizationAction.commitVoters`, `TransactionFinalizer.commitVoters` and
  `RegisteredTransactionFinalizerFactory` change signature accordingly.
- When the voters are unknown (the outcome came from another driver or a recovered accepted value), the marker writes
  entries for every silent store as today. **Decided:** the driver still passes the incarnation of every store it
  heard from in any prepare response (a takeover driver hears from at least a quorum in its own round), so
  `commitVoters` takes the voters (`Option[Set[StoreId]]`, as today) plus `incarnations: Map[StoreId,
  StoreIncarnation]` for every store the driver heard from, in both cases. An entry records the store's incarnation
  when the map has it, and unknown only for a store that never answered this driver.
- **Decided:** an unknown incarnation never applies (see below). The accepted cost: a store legitimately owed T that
  was silent to the finalizing driver gets T from the repair scan's reconcile (typically about 22 s to 15 min) instead
  of from its entry; that one replica is stale meanwhile and reads mask it. This is a rare failure path (driver
  takeover plus a store silent to the new driver), and it keeps the decision clock-free everywhere.

### Entry format

- `UnackedCommitEntry(timestamp, incarnation: Option[StoreIncarnation], objects)`. Encoded after the timestamp;
  `None` encodes as the zero UUID. `encodedSize`, `chunks` and `maxEncodedSize` account for the 16 bytes. Every chunk
  carries the same incarnation; lookups read only the head, as today.
- The marker's "write only where none is held" rule is unchanged: an existing entry keeps its incarnation.

### Lookup and the apply decision

- `UnackedCommitLookup.answer` returns `Try[UnackedLookupResult]`: `Absent`, or `Found(incarnation: Option[...])`.
  `Host.unackedLookup` decodes the head entry instead of returning `isDefined`.
- `Frontend.unackedLookupComplete` maps the result to the Tx:
  - `Absent`: as today (`Tx.unackedCommitAbsent`).
  - `Found(Some(own))`: as today (`Tx.unackedCommitFound`): apply, stub, conclude.
  - `Found(other or None)`: **committed, owed nothing here**. The Tx concludes committed exactly as it does today when
    the store holds an applied stub: a held late commit calls `applyNothing(committed = true, ...)`; a recovered or
    expiring commit vote resolves committed and applies nothing (its locks and held aborts are released, held aborts
    are never accepted, since T committed); the case-3 `forgetLookupHeld` path writes the stub as it does for any found
    entry (a stub only ever suppresses an apply). The entry is left for the repair scan, whose reconcile installs the
    committed state exactly.
  - Log the mismatch at info with both incarnations.
- Why this is complete: an exact match means the vote that counted was cast by this incarnation. A store that saw T
  never rejoins it (forget floor), so a fresh Tx here with a matching entry is the original voter still holding its
  Tx, or its CRL-recovered successor, which is what the entry is for. Any other Tx (a replay at a rebuilt store, or at
  a store that never saw T) finds a mismatch or unknown and applies nothing.
- Consequence for an existing residual: a store that never saw T and has an entry (only possible through the unknown-
  voters case) no longer applies T late; reconcile heals it instead (the transaction CLAUDE.md "Residual" under
  Applied stubs). Strictly safer, a little more repair work.

### Repair fence

- `StoreRepairer.ownedHere` checks that the pool's entry names both this host and the device the store is loaded
  from (`StoreEntry.storageDeviceId`). *(Call:)* the local device comes from the host's published view:
  `HostStoreView` gains `storeDevices: Map[StoreId, StorageDeviceId]`, built by `DeviceControl.publishView` for served
  stores (each `Store` already knows its `storageDeviceId`), and `RepairTarget` exposes `localDevice(storeId)`. Only
  repairable stores are scanned, and those are served, so rebuilding stores and devices not yet loaded never reach the
  check. A store with no device in the view is treated as not owned here (fails closed, logged).
- Ownership is re-checked before each entry delete (ErrorTree and UnackedCommits), not only at scan start.
  *(Call:)* each scan holds one cached ownership `Future`, refreshed when older than one second; every delete site
  awaits it, and a negative answer sets a scan-local flag that makes `abandoned` true for the rest of the scan,
  leaving the remaining entries for the owning host. The read is fresh: `getStoragePoolState` reads the pool object
  through the client, and the client object cache was removed (opportunistic-rebuild removal design). Cost: one
  bootstrap-pool read per second per scan, against a residual window of at most a second plus the delete's latency.
  Making it airtight would need a requirement on the pool state object in every delete transaction, adding the
  bootstrap pool to every repair delete; not worth it for a race that already needs a mistaken failure of a live
  device.
- The bootstrap sweep deletes nothing and needs only the device check.

### Documentation

- Cross-document review: X10 status (N3 leg closed, reverse direction narrowed to the 1 s window), and the G5 timeline
  for an in-place rebuild and a move to a new device: who writes entries for the slot at each stage (finalizers of
  T the old incarnation voted in; ErrorTree marks while the store is Rebuilding), who may consume them (only the
  incarnation the pool entry names, on the device it names), and why ErrorTree marks inherited by the new incarnation
  are harmless (exact reconcile).
- rebuild-process N3 → FIXED; README ranked rows; Todo 2.12 removed.
- Module CLAUDE.md files: `server/transaction` (apply decision, Applied stubs residual), `client/internal/transaction`
  (entry format, voters with incarnations), `server/store` (Frontend incarnation), `server/rebuild` (finish writes
  the identity), `server/repair` (fence), `server/host` (load generates the file if absent).

## Tests (deterministic, LockstepThreading)

1. N3 reproduction: a rebuilt store (new incarnation) holding an inherited UnackedCommits entry for T, with the
   `acceptorSince` mute defeated (inject a host clock skew, or construct the Frontend with an `acceptorSince` earlier
   than T's start), receives a replayed client prepare of T that resolves late. Before the change T applies over newer
   state; after, it applies nothing and the scan's reconcile leaves the store matching its peers.
2. Same-incarnation late commit still applies through its entry (regression for the legitimate path), and a recovered
   commit vote with a matching entry still applies.
3. Recovered commit vote finding an entry of unknown incarnation: resolves committed, applies nothing, never accepts
   its held abort; the scan reconciles the object.
3a. Takeover: the original driver dies after the commit is chosen; a second driver adopts the accepted commit and
   finalizes. A store that answered the second driver's prepare and sent no TxCommitted gets an entry with its own
   incarnation and applies through it; a store silent to the second driver gets an unknown entry and is healed by the
   scan.
4. Entry encode/decode round trip, chunks carry the incarnation, size bounds hold.
5. Store load without an `incarnation` file creates one durably; a rebuild's finish writes a fresh one; a transfer
   keeps it.
6. Repair fence: a store whose pool entry names this host but another device is not scanned; ownership flipping mid-
   scan stops further deletes.

## Left to the builder

- Type and module placement of `StoreIncarnation` and `UnackedLookupResult`.
- How the repair target exposes the store's local device.
- Exact log wording and levels beyond what is stated.
