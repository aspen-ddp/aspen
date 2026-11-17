### Store and Frontend
- `Frontend.incarnation` (a `StoreIncarnation`; default `StoreIncarnation.default(storeId)`, overridden through `Store`'s and `Frontend`'s `incarnationOverride`) is carried by every prepare reply (`TxPrepareResponse.incarnation`)
- The transaction protocol the Frontend takes part in (votes, proofs, late resolution, UnackedCommits, applied
  stubs, status cache): `server/transaction/CLAUDE.md`. Read both when changing `Frontend` or `Tx`
- Every referenced pool's stores receive the prepare and check only the requirements of the objects they host (plus
  non-object requirements)
- The description itself is checked before the vote (`DescriptionValidator`, transaction-requirements F7, Q-N4). The
  store's own pool entry in `poolIDAMap` must be its pool's IDA (`StoreConfig.ida`, written at store creation, passed
  as `Frontend.poolIDA`; `None` skips the check, tests only), so each pool's stores guard their own thresholds. The
  primary object must be referenced, with its pool's IDA, the most failure-tolerant pool, and a leader among its
  stores. Per object: one content update, one `RefcountUpdate`, no lock on an updated object, no shared lock beside
  another. And the pin rule must hold from the description alone (`Pins.declared`), or the description must be
  `replaySafe` (a trusted client claim). A key pin is the update's `pinned` flag. The stores holding that object
  refuse a false flag (`FalsePin`, in `RequirementsChecker` beside `UnflaggedDelete`), so a store that does not hold
  it may rely on the flag

#### Requirement locks and collisions
- Stores hold requirement locks from a commit vote until the transaction resolves. `SharedRevisionLock` is a
  non-exclusive requirement on an object's revision: many transactions may hold it, but it collides with any write
  to that object
- Exclusive preference window (`SharedRevisionLock.ExclusivePreferenceWindow`, 250 ms, `ObjectState.exclusiveWaitUntil`):
  a writer that collides with held shared locks makes new shared lockers collide meanwhile, so it gets in on its
  retry. `RequirementsChecker.check` opens it only for a transaction blocked by nothing but shared holders at this
  store; the shared collision is raised after the requirement's own checks, so another failure (a stale revision)
  is what gets reported, and such a writer opens no window (transaction-requirements F13)
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
- A fresh `Allocating` state is known only to its Tx (`retain(shared = false)`), so a client read misses it. On a
  backend miss for a client read, a store that holds a Tx with a commit vote on the object's allocation, not yet
  applied (`Tx.allocationUnapplied`: no `doCommit` yet, not known aborted), answers `AllocationPending`, not
  `ObjectNotFound` (`Frontend.allocationPending`; it scans the held transactions, misses only). A read straight after
  the allocation commits therefore cannot prove the object deleted (transaction-requirements Q-N5). The read is
  parked rather than answered (`parkAllocationRead`, Parked reads below), and the client re-reads a store that answers
  `AllocationPending` anyway. Once `doCommit` starts, the in-flight state is served, and a client read's backend
  miss that completes after that (its load was in flight when the commit applied) serves that state rather than
  answering `ObjectNotFound`: the miss predates it
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
  store with a final state answers. A store with a Tx replies with `Tx.statusForExpiry`: status, finalized, late,
  `holdsAcceptorState` (false only for a Tx muted for good below the floor), its latest `accepted` value and
  `votedCommit`. An unresolved Tx answers only once frozen (past expiry by its own clock: it promises and accepts
  nothing more, `Tx.frozen`) and its objects are loaded, or muted for good, so the state it reports is final: a frozen
  Tx decides no vote and sends none (`Tx.determineDisposition`, `Tx.sendDelayedPrepare`), so one still undecided then
  (waiting on an in-flight commit, a collision or its client's prepare) is a final non-voter; before that, and for a Tx muted only for starting too far ahead (it may yet join), it is silent. With no
  Tx but a non-Unresolved status-cache entry it
  replies with that entry. With neither it replies `status = None` ("unknown") only when the start is below its forget
  floor (`belowForgetFloor(HLCTimestamp)`), which makes the answer permanent; above the floor it stays silent, as it
  could still join the transaction. A rebuilt store, for T started before its `acceptorSince`
  (`mayHaveForgottenUndecided`), replies `amnesiac` (with a cached outcome if any), never "unknown": its old
  incarnation may have accepted T (cross-document G1). How the replies decide: `server/transaction/CLAUDE.md`, Expiry
- Forget floor (`Frontend.forgetFloorMillis`, transaction horizon design section 1): a prepare for a transaction the
  store does not hold (no Tx; a finalized status-cache entry has it ignored instead) whose start is below the floor is
  refused, a resolved but unfinalized cache entry included (Todo 4.6: the host's cache is shared, so another store's Tx
  may have written it after this store forgot T, whose applied stub may have expired): no Tx, no promise, vote, reply or
  CRL save, its admission released, a warning with the lag for a client's prepare (at most once per `RefusalLogPeriod`,
  10 s, with the count held back). The floor is the latest of `wallClockMillis() - horizon`
  (`TransactionHorizon.Horizon`, 20 s, threaded Host → DeviceControl → StoreLoader → Store → Frontend), the store's load
  on this host plus `MaxStartLead` (`loadedAtMillis`, taken when the Frontend is built: every restart, adoption and
  transfer-in builds a new Store through `StoreLoader`; the lead because before its load the store may have joined, and
  forgotten, a transaction started up to `MaxStartLead` ahead of its clock; a new pool's store the host just created,
  `StoreRebuilds.createNewStore`, has never served and records no load; a load less than
  `RebuildingStore.AcceptorSinceMargin` after the store's arrival from another host, recorded durably in
  `StoreLoader.ArrivedAtFile` by a transfer in or an adoption and passed as `arrivedAt`, adds that 10 s margin instead,
  since it joined there by a clock that may lead this host's by up to the P-N2 skew; otherwise the load part is
  lowered to the host's join ceiling the store's previous life ran under, if lower, but never below a past arrival
  plus the margin: `Frontend.joinCeiling`, `server/host/CLAUDE.md`), and the status cache's
  `evictionWatermarkMillis` (which adds the lead too). A Tx the store holds (recovered ones included) is unaffected; a
  prepare whose outcome the cache knows creates a Tx that learns it only above the floor (resolved), and is ignored when
  finalized (the relay). An `acceptorSince` mute is checked first and keeps its muted, possibly proving Tx. The
  designated leader starts no driver for a prepare below the floor it does not hold (`Store`), but does start one when
  the cache holds an outcome not yet finalized: the fast takeover of a dead remote driver (R-F16, by design). A store therefore never
  rejoins a transaction it may have forgotten; for up to the horizon after a load it looks offline to transactions
  started before the load plus `MaxStartLead`. Accepted cost: on a single host, or after most of a pool's stores
  restart, such transactions end `TransactionOutcomeUnknown` at about 22 s and are retried. With the join ceiling,
  after a restart longer than about 3 s, that covers only what started before the host went down plus about 3 s.
  Stores written by bootstrap are not exempt from the load floor (and a first load on a host has no ceiling)
- Join ceiling gate (`Frontend.startsTooFarAhead`, `JoinCeiling.mayJoin`): an unknown transaction started at or above
  the host's persisted join ceiling is muted as one started too far ahead (warned as a renewal fallen behind), and its
  designated leader does not drive it. It joins once a renewal passes its start
- Expiry (`server/transaction/CLAUDE.md`): `Store.heartbeat` calls `Frontend.expiryHeartbeat`, which queries the
  peers of each Tx unfinalized past the horizon plus `expiryMargin` and decides from their `TxStatusResponse`s
  (`ExpiryTally`): adopt an outcome, decide commit (resolve it at every store, apply it, and have the store's driver
  finalize it: `onExpiryCommit`), keep holding, or discard (`expireTransaction`: locks and objects released, CRL
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
  live voter's failed apply re-check, and a recovered commit vote whose object fails to load with anything but
  `ObjectNotFound`. A live voter's failed re-check is logged as an `INVARIANT VIOLATION` naming the transaction
  whose update each object now shows: a committed apply ignores other transactions' locks, which alone is normal (the
  holder usually aborts) and not reported, so this is where two conflicting commits surface (transaction-requirements
  F10; Backlog 5.8: repair the object instead). A recovered voter's failed re-check quarantines the object instead
  (below). A fail-stopped store drops every transaction message, read, repair and heartbeat. `Store.onFailStop` posts `Host.StoreFailStopped`, and the
  store is rebuilt as a failed backend is. Never for a full device: a write-frozen store poisons the object instead
  (`poisonObject`, the `BackendCommitFailed` rule) and keeps serving. Every fail-stop is a lost-store declaration
  (Backlog: correlated declarations)
- Deletion after allocation (transaction-recovery R-N13, Todo 2.17): a recovered allocating commit vote treats a
  backend miss as "not yet applied" and creates the object from its CRL data, so a deletion must never reach the
  backend while an allocation's CRL state still durably holds the object's update. `allocationRecords`/
  `allocationsOf` index the Txs that allocate objects here (registered at both Tx creations) until their CRL state's
  deletion or stub is durable (`allocationConcluded`, at `concludeTransactionState`, `deleteTransactionState`,
  `recordAppliedStub`, `forgetMuted`, then a `whenDurable`). `awaitAllocationsDurable` holds a deletion's
  `backend.commit` (`commit` with refcount 0) and `backend.repairDelete` until each covering allocation's state is
  saved without the object's update (`Tx.persistWithoutUpdateOf`, which also covers a forgotten Tx whose state the
  CRL keeps for a failed commit) or, once concluded, its deletion is durable. The deletion's bookkeeping is
  synchronous, so it is served as in flight meanwhile; a store stopped or closed first never issues it and keeps the
  deleting transaction's CRL state. Inline for every object no unconcluded allocation covers. MapBackend keeps a
  refcount-0 state rather than removing it, unlike RocksDB and FSIF, so its tests see no miss: the R-N13 tests use a
  removing MapBackend
- Quarantine (`Frontend.quarantine`, transaction-requirements F5, Todo 2.16): a recovered commit vote whose apply
  re-check fails may have been superseded before the crash (10.5a) or invalidated under its locks, and the store does
  not tell these apart. Each changed object it could not apply is quarantined, except one the backend missed and the
  Tx allocates (`allocatedHere`: no copy, no lost lock; one whose data was dropped was applied and then deleted, which
  no read verifies), which is reported (R-F17). Every transaction on a quarantined object votes abort
  (`ObjectQuarantined`), and committed ones apply nothing to it and report it. Repair is not held off, unlike
  `commitFailed`. The store asks its host to verify the object (`QuarantineVerifier`, Host →
  `RepairService.verifyObject`, bounded by `VerifyDeadline`, 30 s). It clears once `StoreRepairer.verifyObject` reports the copy complete against a
  read at least the transaction's start. Otherwise it retries at heartbeat, 1 doubling to
  `MaxQuarantineRetryInterval` (60) heartbeats. In memory only: the quarantining transaction's CRL state, object data
  included, is kept until it clears (`concludeTransactionState` defers a finalization or stub to
  `verificationComplete`), so a reloaded store quarantines again. Nothing is written for it
- Ranged-write/append deltas are read through `EncodedAppendDelta`, which parses only the header and region table
  and XORs straight from the encoded buffer, so checking and applying never copy the delta

#### Backpressure (`CrlAdmission`, one per device, shared by its stores)
- A store admits a prepare for a transaction it has not seen (not admitted or recovered by its Frontend, nor
  finalized in the host-wide status cache) for its description, object data and CRL record, and drops it silently
  while the device's admitted bytes are at or past its lane's ceiling (`Frontend.admissionTier`, from
  `StoreConfig.admissionTier`): 3/4 of the limit (half a CRL stream, `SimpleCRL.admissionLimitBytes`) for data pools,
  7/8 for metadata pools, the whole limit for the bootstrap pool. Marker batches are always a tier above what they
  finalize, so lower-tier load never blocks them (safety X5, G10). Object data arriving later is charged once per
  object without a check. Everything else (known transactions' prepares, accepts, resolutions, finalizations,
  heartbeats, status requests) is handled. Bytes are released when
  the entry is deleted, a Tx is forgotten without one, or the store closes; a Tx waiting on an in-flight commit keeps
  its admission. Recovered transactions are counted too, without a check. Accepted gap: a retransmit past the mark
  while a store awaits its crl_save.log is dropped

#### Object cache (`cache/`)
- Each store's object cache is its own LRU, owned by its executor, under one host byte budget (`HostObjectCache`): an
  over-budget store evicts its own oldest entries down to the budget, or posts a shed to the store holding the host's
  oldest; a shed frees to `maxBytes / 32` below the budget. `Frontend` pins states held by transactions and
  in-flight commits; a failed backend commit stays pinned and served

#### Backends (`backend/`)
- A backend's commit completion must mean durable: the CRL deletes a committed transaction's record once the backend
  commit completes. RocksDB writes synced, FSIF completes at the journal force. `MapBackend` is memory only and gives
  no durability (crash-recovery-log F17); it is for tests, and no opener constructs it
- Pluggable: RocksDB, in-memory Map, and FSIF (`backend/fsif`, a flat file of fixed-size segments located by a
  4-byte storePointer, with a journal and checkpointed index; design in `backend/fsif/DESIGN.md`). FSIF freezes
  writes on ENOSPC and keeps serving reads. A repair write never overwrites a different occupant of its slot: it
  fails with `backend.RepairSlotOccupied(objectId, occupant)`, which the repairer uses to drop a dead occupant
  (cross-document-review X6). FSIF pools allocate through `FsifIndexAllocationStrategy`
  (`client/internal/allocation/CLAUDE.md`)
- Opening is backend-neutral (`BackendOpener`; only `DefaultBackendOpener` names concrete backends): see
  `server/host/CLAUDE.md`
- Creation is atomic for every backend (`StoreCreation`, safety SL X14): `BackendOpener.create` builds the store,
  backend initialized and closed plus its config, in a scratch directory under `<device>/creating/`, forces the
  tree and renames it to the store's path. Open never creates: FSIF throws `StoreLost` for a missing directory or
  blank files, RocksDB `RocksDBStateLost` when `CURRENT` is missing, so a store that lost its files is rebuilt, never
  served empty. A new backend must keep both rules: create only in `init`, and treat an absent or empty store as lost
- `Reconcile` (exact repair install): `server/repair/CLAUDE.md`; `ReconcileResult.changed` is true only when the
  install was applied (every deferred, stale or no-op path reports false)
- Parked repairs (repair F8): a reconcile whose `RepairSnapshot.awaitSettle` is set, finding its object unsettled (a
  commit or repair write in flight or held, a failed commit, a deletion, holding for recovery), waits in
  `parkedRepairs` (at most one per object, a newer one answers the older stale) instead of answering stale.
  `commitTurnFree` runs it before telling any waiting transaction, so at a settled moment before the next commit
  starts, and `releaseHeldMessages` runs all of them when holding ends. It runs once, through `reconcileForRepair`
  with `awaitSettle` off. Past `Frontend.RepairSettleWait` (1 s, `expireParkedRepairs` on the store heartbeat) it
  answers stale. Fail-stop and close fail it
- Parked reads (client 1-RTT commit design section 5): a client read whose `Read.awaitCommit` names a transaction
  holding a write lock on the object's in-memory state (`lockedWriteTransactions`) is parked (`parkRead`, keyed by
  object and `readUUID`: a resent read replaces its entry) instead of answered: the client knows that transaction
  committed, and the reply would only show its lock again. Every `Tx.unlock` calls `answerParkedReads(objects)`, which
  answers each parked read on them none of whose named transactions still holds a lock there; so commit, abort,
  expiry discard (`Tx.expire`) and forget all release them. `doCommit` drops its locks before applying, but calls
  `answerParkedReads` only after `issueCommits` applied the update in memory, so the answer shows it. Past
  `Frontend.ParkedReadWait` (1 s; a test seam, `parkedReadWait`; `expireParkedReads` on the store heartbeat) a parked read is answered with the
  state as it is then. At `Frontend.MaxParkedReads` (1024) parked a read is answered at once. A read reaching the
  backend is never parked (an uncached object holds no locks). Fail-stop and close drop parked reads unanswered; the
  client retransmits. Safety is unaffected: a parked read only delays its own answer
- Parked allocation reads: a client read that misses in the backend while `allocationPending` holds is parked too
  (`parkAllocationRead`, `ParkedRead.allocation`; `NetworkRead` carries the pointer for it), with no `awaitCommit`: the
  store knows its own unapplied commit vote. `answerParkedReads` answers it once no held Tx has the allocation
  pending (the allocating Tx's unlock: after `doCommit` applied it in memory, so the state is served, or on abort or
  discard, so `ObjectNotFound`). At `parkedReadWait` it is answered `AllocationPending` if still pending, which the
  client re-reads at once; at `MaxParkedReads` it is answered `AllocationPending` at once
- `deleteObjectForRepair` refuses (`Frontend.RepairDeleteDeferred`) while the store holds for recovered transactions or
  the object has a pending backend read, a commit, repair write or deletion in flight, or a transaction referencing
  it (repair F6); while its backend removal is outstanding the object counts in `deleting`, so reads, snapshots and
  transactions get `ObjectNotFound`
