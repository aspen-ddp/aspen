### Store and Frontend
- `Frontend.incarnation` (a `StoreIncarnation`; default `StoreIncarnation.default(storeId)`, overridden through `Store`'s and `Frontend`'s `incarnationOverride`) is carried by every prepare reply (`TxPrepareResponse.incarnation`)
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
  whole object for a min/max/left/right change. Concurrent blind writes of one key collide
- Apply is order-independent for what the builder lets through (transaction-requirements F1, F2, F4): every key
  Insert is last-writer-wins on (transaction timestamp, revision) (`KVObjectState.insertWins`; `encodedSizeAfter`
  uses the same rule). A superseded Insert is a no-op, not a skip: no report and no fail-stop. The object timestamp
  is the max of its value and the transaction's. No tombstones, so `TransactionBuilder` refuses blind key deletes
  and blind object-level ops (`BlindKeyValueWrite`), and stores refuse them too (`RequirementsChecker.refuseBlindWrites`,
  `BlindWrite`, at vote and apply re-check): unless the transaction allocates the object, a key Delete needs a
  `KeyRevision` on its key, a required revision or a content lock, and a min/max/left/right change a required revision
  (a content lock does not move the revision a repair proof locks; repair F4, F11). Residual: a late or replayed
  Insert after a later delete of its key resurrects it, like a `DoesNotExist` ABA (Todo 2.9)
- Allocations (transaction-requirements F11): an object in `txd.allocatingObjects` is loaded like any other (cache,
  `deleting`, backend read). Only a backend miss gives the fresh `Allocating` state (`Tx.allocationLoaded`). An
  existing state goes into `Tx.allocatedOver`, and every requirement on it fails with `ObjectExists`, at vote and at
  the apply re-check, so a replayed allocation never overwrites a live object. `appliedBy` counts such an object at
  T's own revision as applied. Residual: no tombstones, so an id whose deletion has persisted can be allocated again
  (an orphan slice, Todo 2.9)
- A prepare that reaches a store after its transaction was finalized is not applied there: the store relies on
  missed-update repair. A collision is an ordinary abort vote, even with an already-completed transaction, except
  one with a lock holder the transaction is built on (it requires the revision the holder writes): the store then
  withholds its vote until the holder releases its locks, or for `Tx.CollisionWait` (500 ms, checked at heartbeat),
  so a single writer's next transaction need not abort while a store is behind on the previous one's outcome
- Store identity: `StoreIncarnationFile` keeps the store's incarnation UUID in its directory's `incarnation` file (written
  with `DurableFiles.write`). It survives restarts and transfers (the archive is the whole directory) and changes only on
  a rebuild. A backend with no directory (`MapBackend`) gets `StoreIncarnation.default(storeId)` and no file.
- Rebuilt stores: `Frontend.acceptorSince` (from the `acceptor-since` file `RebuildingStore.finish` writes) mutes a
  prepare for an unknown transaction started earlier (`Tx.muted`): proven committed if an object shows its update,
  otherwise forgotten with no promise, vote, accept or CRL save, as if the store were offline. Its old incarnation
  may have accepted in such a transaction, and a fresh promise could let a quorum choose against the chosen value
- Start-time lead (`Frontend.MaxStartLead`, 2 s): every store mutes, the same way, a prepare for an unknown transaction
  whose client-chosen start timestamp leads the host's wall clock (`wallClockMillis`, not the JVM-wide HLC, which the
  host's own client ratchets) by more; the designated leader does not drive it (`Store`). The client's retransmission
  is answered once the clock catches up. This is what makes the `acceptorSince` mute independent of client clocks
  (paxos P-N2), and it caps how far stored timestamps, and so client HLCs, run ahead of the hosts
- Current allocations (`CurrentAllocationsRequest`, Rebuild F10; `StoreRouter` routes it to the store's executor):
  `Frontend.currentAllocations` lists, for every held Tx neither finalized nor known aborted, the objects it allocates
  in this store's pool, and whether it is known committed. Silent while the CRL loads (`awaitingCrl`): it may hold
  transactions it does not know yet. A committed Tx is held until TxFinalized, after its AllocationFinalizationAction,
  so every allocation whose AllocationTree entry may still be missing is listed by each store holding it
- Status query (`TxStatusRequest`, carrying the transaction's `startTimestamp`; horizon design section 2): every
  store answers. A store with a Tx replies with its status, finalized, late and `holdsAcceptorState`, false only for a Tx
  muted for good (before `acceptorSince`, or below the floor): one muted only for starting too far ahead may yet join. With no
  Tx but a non-Unresolved status-cache entry it replies with that entry (`holdsAcceptorState = false`). With neither it
  replies `status = None` ("unknown") only when the start is below its forget floor (`belowForgetFloor(HLCTimestamp)`),
  which makes the answer permanent; above the floor it stays silent, as it could still join the transaction
- Forget floor (`Frontend.forgetFloorMillis`, transaction horizon design section 1): a prepare for a transaction the
  store does not hold (no Tx, no non-Unresolved status-cache entry) whose start is below the floor is refused: no Tx,
  no promise, vote, reply or CRL save, its admission released, a warning with the lag for a client's prepare (at most
  once per `RefusalLogPeriod`, 10 s, with the count held back). The floor
  is the latest of `wallClockMillis() - horizon` (`TransactionHorizon.Horizon`, 20 s, threaded Host → DeviceControl →
  StoreLoader → Store → Frontend), the store's load on this host plus `MaxStartLead` (`loadedAtMillis`, taken when
  the Frontend is built: every restart, adoption and transfer-in builds a new Store through `StoreLoader`; the lead
  because before its load the store may have joined, and forgotten, a transaction started up to `MaxStartLead` ahead
  of its clock; a new pool's store the host just created, `StoreRebuilds.createNewStore`, has never served and records
  no load; a load less than `RebuildingStore.AcceptorSinceMargin` after the store's arrival from another host, recorded
  durably in `StoreLoader.ArrivedAtFile` by a transfer in or an adoption and passed as `arrivedAt`, adds that 10 s
  margin instead, since it joined there by a clock that may lead this host's by up to the P-N2 skew), and the status
  cache's `evictionWatermarkMillis` (which adds the lead too). A Tx the store holds
  (recovered ones included) is unaffected, as is a prepare whose outcome the cache knows (finalized: the relay;
  resolved: a Tx that learns the outcome). An `acceptorSince` mute is checked first and keeps its muted, possibly
  proving Tx. The designated leader starts no driver for a prepare below the floor it does not hold (`Store`). A
  store therefore never rejoins a transaction it may have forgotten; for up to the horizon after a load it looks
  offline to transactions started before the load plus `MaxStartLead`. Accepted cost: on a single host, or after most
  of a pool's stores restart, such transactions end `TransactionOutcomeUnknown` at about 22 s and are retried. Stores
  written by bootstrap are not exempt from the load floor. A durable high-water mark is the upgrade path
- Expiry (`server/transaction/CLAUDE.md`): `Store.heartbeat` calls `Frontend.expiryHeartbeat`, which queries the
  peers of each Tx unfinalized past the horizon plus `expiryMargin` and decides from their `TxStatusResponse`s
  (`ExpiryTally`): adopt an outcome, keep holding, or discard (`expireTransaction`: locks and objects released, CRL
  state deleted, status cache untouched; a commit vote only once a later UnackedCommits lookup finds no entry; a
  resolved Tx concludes as finalized). Then
  `removeExpiredDrivers` stops each driver past the horizon with no outcome whose store holds no Tx for it. Store
  takes `heartbeatPeriod` and a `wallClockMillis` test seam for this

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
- Fail-stop (`Frontend.failStop`, transaction-requirements F5/F6): a commit voter has dropped its locks before it
  applies, so one that cannot hold the update it voted for fail-stops the store. That covers an apply that throws, a
  live voter's failed apply re-check (a recovered voter's may legitimately fail: superseded before the crash), and a
  recovered commit vote whose object fails to load with anything but `ObjectNotFound`. A fail-stopped store drops
  every transaction message, read, repair and heartbeat. `Store.onFailStop` posts `Host.StoreFailStopped`, and the
  store is rebuilt as a failed backend is. Never for a full device: a write-frozen store poisons the object instead
  (`poisonObject`, the `BackendCommitFailed` rule) and keeps serving. Every fail-stop is a lost-store declaration
  (Backlog: correlated declarations)
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
  writes on ENOSPC and keeps serving reads. A repair write never overwrites a different occupant of its slot: it
  fails with `backend.RepairSlotOccupied(objectId, occupant)`, which the repairer uses to drop a dead occupant
  (cross-document-review X6). FSIF pools allocate through `FsifIndexAllocationStrategy`
  (`client/internal/allocation/CLAUDE.md`)
- Opening is backend-neutral (`BackendOpener`; only `DefaultBackendOpener` names concrete backends): see
  `server/host/CLAUDE.md`
- `Reconcile` (exact repair install): `server/repair/CLAUDE.md`; `ReconcileResult.changed` is true only when the
  install was applied (every deferred, stale or no-op path reports false)
- `deleteObjectForRepair` refuses (`Frontend.RepairDeleteDeferred`) while the store holds for recovered transactions or
  the object has a pending backend read, a commit, repair write or deletion in flight, or a transaction referencing
  it (repair F6); while its backend removal is outstanding the object counts in `deleting`, so reads, snapshots and
  transactions get `ObjectNotFound`
