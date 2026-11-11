### Store and Frontend
- The transaction protocol the Frontend takes part in (votes, proofs, late resolution, UnackedCommits, applied
  stubs, status cache): `server/transaction/CLAUDE.md`. Read both when changing `Frontend` or `Tx`
- Every referenced pool's stores receive the prepare and check only the requirements of the objects they host (plus
  non-object requirements)

#### Requirement locks and collisions
- Stores hold requirement locks from a commit vote until the transaction resolves. `SharedRevisionLock` is a
  non-exclusive requirement on an object's revision: many transactions may hold it, but it collides with any write
  to that object
- A KeyValue update's operations lock what they write, as requirements do (`KVWrites`): each key an
  Insert or Delete names (an absent key through `absentKeyLocks`, shared with `DoesNotExist`/`MayExist`), and the
  whole object for a min/max/left/right change. Blind writes of one key collide, so stores applying committed
  transactions in different orders cannot diverge
- A prepare that reaches a store after its transaction was finalized is not applied there: the store relies on
  missed-update repair. A collision is an ordinary abort vote, even with an already-completed transaction, except
  one with a lock holder the transaction is built on (it requires the revision the holder writes): the store then
  withholds its vote until the holder releases its locks, or for `Tx.CollisionWait` (500 ms, checked at heartbeat),
  so a single writer's next transaction need not abort while a store is behind on the previous one's outcome
- Rebuilt stores: `Frontend.acceptorSince` (from the `acceptor-since` file `RebuildingStore.finish` writes) mutes a
  prepare for an unknown transaction started earlier (`Tx.muted`): proven committed if an object shows its update,
  otherwise forgotten with no promise, vote, accept or CRL save, as if the store were offline. Its old incarnation
  may have accepted in such a transaction, and a fresh promise could let a quorum choose against the chosen value

#### Commits
- A store keeps a committed transaction's CRL state until its backend commits succeed; a failed commit keeps it (and
  the pinned object state, still served) while the store is loaded
- One in-flight transaction commit per object (`Frontend.commitInFlight`): a transaction touching an object whose
  commit is in flight, whether or not it builds on it, withholds its vote with no locks and no timer until the commit
  persists, then checks its requirements against the state as it is then. A follower resolved committed meanwhile
  holds its commit (queued per object, oldest first) until earlier commits of its objects persist. Locks still
  release when a commit is applied in memory: the in-flight marker orders followers. After a failed backend commit
  every transaction on that object votes abort (`BackendCommitFailed`) until the store reloads. Objects a
  transaction only checks or locks get no backend commit, so concurrent `SharedRevisionLock` holders never wait
- Ranged-write/append deltas are read through `EncodedAppendDelta`, which parses only the header and region table
  and XORs straight from the encoded buffer, so checking and applying never copy the delta

#### Backpressure (`CrlAdmission`, one per device, shared by its stores)
- A store admits a prepare for a transaction it has not seen (not admitted or recovered by its Frontend, nor
  finalized in the host-wide status cache) for its description, object data and CRL record, and drops it silently
  while the device's admitted bytes are at or past half a CRL stream (`SimpleCRL.admissionLimitBytes`). Object data
  arriving later is charged once per object without a check. Everything else (known transactions' prepares, accepts,
  resolutions, finalizations, heartbeats, status requests) is handled. Nothing is exempt. Bytes are released when
  the entry is deleted, a Tx is forgotten without one, or the store closes; a Tx waiting on an in-flight commit keeps
  its admission. Recovered transactions are counted too, without a check. Accepted gap: a retransmit past the mark
  while a store awaits its crl_save.log is dropped

#### Object cache (`cache/`)
- Each store's object cache is its own LRU, owned by its executor, under one host byte budget (`HostObjectCache`): an
  over-budget store evicts its own oldest entries down to the budget, or posts a shed to the store holding the host's
  oldest; a shed frees to `maxBytes / 32` below the budget. `Frontend` pins states held by transactions and
  in-flight commits; a failed backend commit stays pinned and served

#### Backends (`backend/`)
- Pluggable: RocksDB, in-memory Map, and FSIF (`backend/fsif`, a flat file of fixed-size segments located by a
  4-byte storePointer, with a journal and checkpointed index; design in `backend/fsif/DESIGN.md`). FSIF freezes
  writes on ENOSPC and keeps serving reads. FSIF pools allocate through `FsifIndexAllocationStrategy`
  (`client/internal/allocation/CLAUDE.md`)
- Opening is backend-neutral (`BackendOpener`; only `DefaultBackendOpener` names concrete backends): see
  `server/host/CLAUDE.md`
- `Reconcile` (exact repair install): `server/repair/CLAUDE.md`
