### Store and Frontend
- `Frontend.incarnation` (a `StoreIncarnation`; default `StoreIncarnation.default(storeId)`, overridden through `Store`'s and `Frontend`'s `incarnationOverride`) is carried by every prepare reply (`TxPrepareResponse.incarnation`)
- The transaction protocol the Frontend takes part in (votes, proofs, late resolution, UnackedCommits, applied
  stubs, status cache): `server/transaction/CLAUDE.md`. Read both when changing `Frontend` or `Tx`
- Every referenced pool's stores receive the prepare and check only the requirements of the objects they host (plus
  non-object requirements)
- The description itself is checked before the vote (`DescriptionValidator`, transaction-requirements 2.5). The
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
- Single-holder lock slots (X, content, present key, absent key) are taken by `RequirementsLocker.take`. A holder
  found there (only a recovered commit vote, which locks without a check, can meet one) is kept in
  `ObjectState.displacedLocks`, and `give` hands the slot back to it when the current holder releases, so two
  recovered votes on one lock both hold it until each releases (transaction-requirements.md 10.5a). An applied
  Insert keeps the lock of the key entry it replaces, so a recovered blind write applying again does not unlock a
  later vote on the key
- Exclusive preference window (`SharedRevisionLock.ExclusivePreferenceWindow`, 250 ms, `ObjectState.exclusiveWaitUntil`):
  a writer that collides with held shared locks makes new shared lockers collide meanwhile, so it gets in on its
  retry. `RequirementsChecker.check` opens it only for a transaction blocked by nothing but shared holders at this
  store; the shared collision is raised after the requirement's own checks, so another failure (a stale revision)
  is what gets reported, and such a writer opens no window (transaction-requirements 4)
- A KeyValue update's operations lock what they write, as requirements do (`KVWrites`): each key an
  Insert or Delete names (an absent key through `absentKeyLocks`, shared with `DoesNotExist`/`MayExist`), and the
  whole object for a min/max/left/right change. Concurrent blind writes of one key collide
- Slice-size reservations (transaction-requirements R16): compatible K/N locks let commit votes on disjoint keys of one
  KeyValue object hold at once, so a vote's size check alone could admit votes that fit only one at a time, and the
  later apply would fail its re-check. A commit vote whose update grows its object (`SliceGrowth.grows`, from
  `KVObjectState.growth`: bytes apart from the count varint, and entries) reserves the growth with its locks
  (`ObjectState.sliceReservations`; `Tx.lock` -> `RequirementsChecker.sliceGrowth` -> `RequirementsLocker.lock`,
  dropped by `unlock`, only when the store has a slice limit). `RequirementsChecker.checkKVSliceSize`, at vote and at
  the apply re-check, fails a growing update unless the current slice plus every other transaction's positive growth
  plus its own fits, the count varint taken at the summed count. The limit is the KeyValue limit
  `Frontend.maxKVSliceSize` = `IDA.maxKVSliceSize(maxSliceSize)`, `floor(t * (maxSliceSize - 4) / w)` (allocations
  included; DataObjects keep `maxSliceSize`): a store checks only the slice it holds, which lacks commits it did not
  vote for, and this keeps the union of committed updates, which repair and rebuild install unchecked, within
  `maxSliceSize` (transaction-requirements R16; a rebuilt store also guards inherited growth, below). Without a
  `poolIDA` (tests) it is `maxSliceSize`. So a commit voter's apply never fails on size; a committed non-voter that
  no longer fits is skipped and reported. A non-growing update always passes. A repair install that grows a slice
  holding reservations is measured the same way, against `maxKVSliceSize` (`Frontend.repairFitsReservations`), and if
  it does not fit, writes nothing and answers stale (repair-process M6a). The apply-time check stays: FSIF's slice limit is its segment payload, so an oversized slice would fail the backend commit
- Apply is order-independent for what the builder lets through (transaction-requirements 9.3): every key Insert is
  last-writer-wins on (transaction timestamp, revision) (`KVObjectState.insertWins`; `growth` and `encodedSizeAfter` use the same
  rule). A superseded Insert is a no-op, not a skip: no report and no fail-stop. The object timestamp is the max of its
  value and the transaction's. No tombstones, so `TransactionBuilder` refuses blind key deletes and blind object-level
  ops (`BlindKeyValueWrite`), and stores refuse them too (`RequirementsChecker.refuseBlindWrites`, `BlindWrite`, at vote
  and apply re-check): unless the transaction allocates the object, a key Delete needs a `KeyRevision` on its key, a
  required revision or a content lock, and a min/max/left/right change a required revision (a content lock does not move
  the revision a repair proof locks; repair-process 1.2, Q8). A re-apply (a recovered commit vote, or one applying
  through its UnackedCommits entry) never writes back a key deleted since: see Key deletion after a blind insert
- Allocations (transaction-requirements 9.4): an object in `txd.allocatingObjects` is loaded like any other (cache,
  `deleting`, backend read). Only a backend miss gives the fresh `Allocating` state (`Tx.allocationLoaded`). An
  existing state goes into `Tx.allocatedOver`, and every requirement on it fails with `ObjectExists`, at vote and at
  the apply re-check, so a replayed allocation never overwrites a live object. `appliedBy` counts such an object at
  T's own revision as applied. Residual: no tombstones, so an id whose deletion has persisted can be allocated again
  (an orphan slice)
- A fresh `Allocating` state is known only to its Tx (`retain(shared = false)`), so a client read misses it. On a
  backend miss for a client read, a store that holds a Tx with a commit vote on the object's allocation, not yet
  applied (`Tx.allocationUnapplied`: no `doCommit` yet, not known aborted), answers `AllocationPending`, not
  `ObjectNotFound` (`Frontend.allocationPending`; it scans the held transactions, misses only). A read straight after
  the allocation commits therefore cannot prove the object deleted (transaction-requirements 9.4). The read is
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
- Inherited votes (`Frontend.inheritedVoteLock`, cross-document-review.md section 2.3): the old incarnation's commit
  votes still count for their transactions, with none of their locks held here. While the `inherited-votes` file
  (`RebuildingStore.InheritedVotesFile`, passed as `inheritedVotesAfter`) is present, `Tx.determineDisposition` votes
  abort (`InheritedVoteLock`, beside `BackendCommitFailed`) on every object until a query of the peers' held votes
  (`HeldVoteQuery`, Host → `client.heldVotes`, `Store.heartbeat` → `retryHeldVoteQuery`, from the file's time on,
  1 s doubling to 30 s) completes, then on the objects of the reported transactions started before `acceptorSince`.
  A transaction is released when a later query no longer reports it or its `TxResolved`/`TxFinalized` reaches the
  store; with none left, and no inherited growth guard held, the file is removed. Muted Txs are decided first, so only
  transactions started after `acceptorSince` are affected
- Inherited growth (`Frontend.inheritedGrowthLock`, the "Inherited growth" block in Frontend; transaction-requirements
  R16, cross-document-review.md 2.3). Exists only for a rare case: the lost incarnation voted commit for a KeyValue
  growth T, T committed after the rebuild read the object, and another growth of it near the KeyValue limit comes
  before repair; a vote for that would count this store twice in R16's bound, so the union could overfill the slice.
  The first held-vote round, after the peers answer (unresolved votes, and committed unfinalized ones,
  `HeldVote.committed`), runs `HeldVoteQuery.growthScan` (the Host's: `StoreRepairer.inheritedGrowthScan`), which
  marks the reported transactions' KeyValue objects and lists every KeyValue object with an ErrorTree or
  UnackedCommits entry against the store. Those are guarded at once; a reported unresolved T's objects (if listed)
  from its release (`setInheritedVotes`). A guarded object's growing update votes abort (`InheritedGrowthLock`, after
  `RequirementsChecker.check` in `Tx.determineDisposition`, computed with `sliceGrowth` only while a guard is held); a
  non-growing one passes. `reconcileForRepair`: a KeyValue object under the inherited-vote lock (all of them before
  the first round) answers stale; a guarded one's complete result is stale unless the pass is whole-object and its
  snapshot is later than the guard (`RepairSnapshot.takenAt`, numbered by `snapshotForRepair`), which ends the guard
  (`inheritedGrowthRepaired`). A failed scan keeps every object locked and repeats the round. Nothing is persisted
  beyond the inherited-votes file: a reload repeats the round, the guarded objects' entries still there
- Start-time lead (`Frontend.MaxStartLead`, 2 s): every store mutes, the same way, a prepare for an unknown transaction
  whose client-chosen start timestamp leads the host's wall clock (`wallClockMillis`, not the JVM-wide HLC, which the
  host's own client ratchets) by more; the designated leader does not drive it (`Store`). The client's retransmission
  is answered once the clock catches up. This is what makes the `acceptorSince` mute independent of client clocks
  (paxos-transactions 10.10), and it caps how far stored timestamps, and so client HLCs, run ahead of the hosts
- Current allocations (`CurrentAllocationsRequest`, rebuild-process 5.3; `StoreRouter` routes it to the store's
  executor): `Frontend.currentAllocations` lists, for every held Tx neither finalized nor known aborted, the objects it
  allocates in this store's pool, and whether it is known committed. Silent while the CRL loads (`awaitingCrl`): it may
  hold transactions it does not know yet. A committed Tx is held until TxFinalized, after its
  AllocationFinalizationAction, so every allocation whose AllocationTree entry may still be missing is listed by each
  store holding it. The reply also carries `Frontend.heldVotes`: each held Tx with outcome unknown, not muted, whose
  vote is commit or undecided, with its objects in this pool (a rebuilt store's inherited votes), and each held Tx
  known committed and not finalized, flagged `committed` (a rebuilt store's inherited growth)
- Status query (`TxStatusRequest`, carrying the transaction's `startTimestamp`; horizon design section 2): every store
  with a final state answers. A store with a Tx replies with `Tx.statusForExpiry`: status, finalized, late,
  `holdsAcceptorState` (false only for a Tx muted for good below the floor), its latest `accepted` value and
  `votedCommit` (only once the vote's CRL save is durable; a commit voter whose save is in flight is silent, in its
  own expiry round too: `Tx.ownExpiryStatus`). An
  unresolved Tx answers only once frozen (past expiry by its own clock: it promises and accepts nothing more,
  `Tx.frozen`) and its objects are loaded, or muted for good, so the state it reports is final: a frozen Tx decides no
  vote and sends none (`Tx.determineDisposition`, `Tx.sendDelayedPrepare`), so one still undecided then (waiting on an
  in-flight commit, a collision or its client's prepare) is a final non-voter; before that, and for a Tx muted only for
  starting too far ahead (it may yet join), it is silent. A Tx created from a cached outcome (`Tx.outcomeDecided`)
  answers at once with the outcome as the cache now holds it, and keeps it in its own expiry round's reply, so it is
  adopted. With no Tx but a non-Unresolved status-cache entry it replies with that entry. With neither it replies
  `status = None` ("unknown") only when the start is below its forget floor (`belowForgetFloor(HLCTimestamp)`), which
  makes the answer permanent; above the floor it stays silent, as it could still join the transaction. A rebuilt store,
  for T started before its `acceptorSince` (`mayHaveForgottenUndecided`), replies `amnesiac` (with a cached outcome if
  any), never "unknown": its old incarnation may have accepted T (cross-document-review 2.2). How the replies decide:
  `server/transaction/CLAUDE.md`, Expiry
- Forget floor (`Frontend.forgetFloorMillis`, transaction horizon design section 1): a prepare for a transaction the
  store does not hold (no Tx; a finalized status-cache entry has it ignored instead) whose start is below the floor is
  refused, a resolved but unfinalized cache entry included (paxos-transactions 10.2: the host's cache is shared, so
  another store's Tx may have written it after this store forgot T, whose applied stub may have expired): no Tx, no
  promise, vote, reply or CRL save, its admission released, a warning with the lag for a client's prepare (at most once
  per `RefusalLogPeriod`, 10 s, with the count held back). The floor is the latest of `wallClockMillis() - horizon`
  (`TransactionHorizon.Horizon`, 20 s, threaded Host → DeviceControl → StoreLoader → Store → Frontend), the store's load
  on this host plus `MaxStartLead` (`loadedAtMillis`, taken when the Frontend is built: every restart, adoption and
  transfer-in builds a new Store through `StoreLoader`; the lead because before its load the store may have joined, and
  forgotten, a transaction started up to `MaxStartLead` ahead of its clock; a new pool's store the host just created,
  `StoreRebuilds.createNewStore`, has never served and records no load; a load less than
  `RebuildingStore.AcceptorSinceMargin` after the store's arrival from another host, recorded durably in
  `StoreLoader.ArrivedAtFile` by a transfer in or an adoption and passed as `arrivedAt`, adds that 10 s margin instead,
  since it joined there by a clock that may lead this host's by up to the inter-host skew (paxos-transactions 10.10);
  otherwise the load part is lowered to the host's join ceiling the store's previous life ran under, if lower, but never
  below a past arrival plus the margin: `Frontend.joinCeiling`, `server/host/CLAUDE.md`), and the status cache's
  `evictionWatermarkMillis` (which adds the lead too). A Tx the store holds (recovered ones included) is unaffected; a
  prepare whose outcome the cache knows creates a Tx that learns it only above the floor (resolved), and is ignored when
  finalized (the relay). An `acceptorSince` mute is checked first and keeps its muted, possibly proving Tx. The
  designated leader starts no driver for a prepare below the floor it does not hold (`Store`), but does start one when
  the cache holds an outcome not yet finalized: the fast takeover of a dead remote driver (paxos-transactions 10.2, by
  design). A store therefore never rejoins a transaction it may have forgotten; for up to the horizon after a load it
  looks offline to transactions started before the load plus `MaxStartLead`. Accepted cost: on a single host, or after
  most of a pool's stores restart, such transactions end `TransactionOutcomeUnknown` at about 22 s and are retried. With
  the join ceiling, after a restart longer than about 3 s, that covers only what started before the host went down plus
  about 3 s. Stores written by bootstrap are not exempt from the load floor (and a first load on a host has no ceiling)
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
  release when a commit is applied in memory: the in-flight marker orders followers. A committed transaction holding
  no locks here applies in lock order: it waits while another transaction holds locks it collides with on an object
  both change (`Tx.awaitLockHolders`, `RequirementsChecker.lockHolders`; `server/transaction/CLAUDE.md`, Lock order),
  and new votes it would wait for as holders (changing an object it changes, with colliding locks) wait for it, at most `Tx.CollisionWait` before voting abort
  (`lockWaiters`, `Tx.lockWaitBlocked`; other votes decide at once; the wait is not in `commitInFlight`, which orders held commits). `RequirementsLocker.plannedLocks` is the one
  source of the locks a vote takes, for both `lock` and that test. After a failed backend commit
  every transaction on that object votes abort (`BackendCommitFailed`) until the store reloads. Objects a
  transaction only checks or locks get no backend commit, so concurrent `SharedRevisionLock` holders never wait
- Fail-stop (`Frontend.failStop`, transaction-requirements.md 8.5): a commit voter has dropped its locks before it
  applies, so one that cannot hold the update it voted for must not serve as if it did. An apply that throws, and a
  recovered commit vote whose object fails to load with anything but `ObjectNotFound`, fail-stop the store. A failed
  apply re-check does not (transaction-requirements.md 8.5). A live voter's is an invariant violation (the
  state was invalidated under its locks, or two conflicting commits happened; a later commit over the vote's locks
  waits for it in lock order, so it is no cause): logged at error as `INVARIANT VIOLATION`, naming the transaction
  each object now shows, then skipped and reported. A recovered commit vote's (one applying through its UnackedCommits
  entry included) is taken as already applied, neither reported nor kept: the vote held its locks until it applied
  and relocked them at recovery, so only its update and the writes after it in lock order changed the object
  (superseded, traceless, or its data dropped from the CRL by `persistWithoutUpdateOf`). A recovered Tx that did not
  vote commit and applies a committed update reports every object it applies, since it may have applied before the
  crash. A lock-only object that fails the re-check is skipped and reported only. A fail-stopped store drops every transaction message, read, repair and heartbeat. `Store.onFailStop` posts `Host.StoreFailStopped`, and the
  store is rebuilt as a failed backend is. Never for a full device: a write-frozen store poisons the object instead
  (`poisonObject`, the `BackendCommitFailed` rule) and keeps serving. Every fail-stop is a lost-store declaration
  (Backlog: correlated declarations)
- Deletion after allocation (crash-recovery-log 3.4): a recovered allocating commit vote treats a backend miss as "not
  yet applied" and creates the object from its CRL data, so a deletion must never reach the backend while an
  allocation's CRL state still durably holds the object's update. `allocationRecords`/ `allocationsOf` index the Txs
  that allocate objects here (registered at both Tx creations) until their CRL state's deletion or stub is durable
  (`allocationConcluded`, at `concludeTransactionState`, `deleteTransactionState`, `recordAppliedStub`, `forgetMuted`,
  then a `whenDurable`). `awaitAllocationsDurable` holds a deletion's `backend.commit` (`commit` with refcount 0) and
  `backend.repairDelete` until each covering allocation's state is saved without the object's update
  (`Tx.persistWithoutUpdateOf`, which also covers a forgotten Tx whose state the CRL keeps for a failed commit) or, once
  concluded, its deletion is durable. The deletion's bookkeeping is synchronous, so it is served as in flight meanwhile;
  a store stopped or closed first never issues it and keeps the deleting transaction's CRL state. Inline for every
  object no unconcluded allocation covers. MapBackend keeps a refcount-0 state rather than removing it, unlike RocksDB
  and FSIF, so its tests see no miss: the deletion-after-allocation tests use a removing MapBackend
- Key deletion after a blind insert (transaction-requirements 9.4, 10.5c): a recovered commit vote re-applies an Insert
  of an absent key, which is right only if no deletion of that key followed the update its CRL state holds. So
  `blindInsertRecords`/`blindInsertsOf` index the Txs whose KeyValue update of an object here has no required
  revision (by shape only, registered beside the allocations by `registerReplayHazards`, dropped once the state's
  deletion or stub is durable, `stateConcluded`), and `awaitBlindInsertsDurable` holds a key deletion's backend write
  (`commit`, whose `deletedKeys` thunk is asked only when the index covers the object, and a repair write whose plan
  removes keys) for each other covering Tx whose update Inserts a deleted key: a concluded one until its deletion is
  durable; one that holds its update of the object here (`Tx.holdsUpdateOf`: it issued its commit, or voted commit,
  whose lock puts the deletion after it) until its state is saved without that update (`persistWithoutUpdateOf`;
  its recovered vote then finds the update missing and takes it as applied). A non-voter that has not applied holds
  an update the store lacks: nothing waits for it, and its later apply of a blind Insert of a key absent before it
  reports the object (`Tx.issueCommits`; the deletion may have applied first, e.g. a required-revision Delete, a
  no-op on the absent key). Inline for every object no recent blind write covers
- Late blind Insert after a repair install (transaction-requirements 9.4): a repair write can install a later
  transaction's key deletion at a store that never held the Insert, which a delayed T would then write back at this
  store alone, possibly with no mark (its TxCommitted inside `errorTimeout`). `repairInstalls` keeps the latest object
  timestamp each repair write installed, while a transaction started before it may still join (pruned below the
  forget floor at `expiryHeartbeat`), and tells every held Tx of the object, and every later-created one, started
  before it (`Tx.repairInstalled`). An apply of a blind Insert of a key absent before it, on an object so marked,
  applies and reports the object (`Tx.issueCommits`), so the finalizer marks the store. A recovered commit vote
  (its marks lost with the crash) reports the same apply when any write newer than T's start reached the object
- Ranged-write/append deltas are read through `EncodedAppendDelta`, which parses only the header and region table
  and XORs straight from the encoded buffer, so checking and applying never copy the delta

#### Backpressure (`CrlAdmission`, one per device, shared by its stores)
- A store admits a prepare for a transaction it has not seen (not admitted or recovered by its Frontend, nor finalized
  in the host-wide status cache) for its description, object data and CRL record, and drops it silently while the
  device's admitted bytes are at or past its lane's ceiling (`Frontend.admissionTier`, from
  `StoreConfig.admissionTier`): 3/4 of the limit (`SimpleCRL.admissionLimitBytes`) for data pools, 7/8 for metadata
  pools, the whole limit for the bootstrap pool. Marker batches are always a tier above what they finalize, so
  lower-tier load never blocks them (crash-recovery-log 6.1; cross-document-review 3, capacity). Object data arriving
  later for an admitted transaction is charged once per object, only below the lane's ceiling (`tryAdmit`); past it the
  prepare is dropped as `Full` and the client retransmits (crash-recovery-log 6.1, A1). Everything else (data-less
  prepares of known transactions, accepts, resolutions, finalizations, heartbeats, status requests) is handled. Bytes
  are released when the entry is deleted or a Tx is forgotten without one; a Tx waiting on an in-flight commit keeps its
  admission. A closing or replaced store moves its bytes into what the device retains for it
  (`Frontend.retainAdmissions`). Recovered transactions are counted without a check (`admitRecovered`, data included),
  and only then is what was retained for the store released (`recoverState`, A2: a swap, never a gap). Accepted gap: a
  retransmit past the mark while a store awaits its crl_save.log is dropped
- No save without a charge (A4): an abort whose CRL state was deleted at resolution (`Tx.stateDeleted`) stays in
  `transactions` until `TxFinalized` with no admission; it saves nothing more, answering a prepare or accept with a
  `TxResolved` of the abort, and adopts no late data. `Tx.saveState` requires it
- `Full` asks the CRL to grow: `admitPrepare` calls `crl.requestGrowth()` for both Full cases (a new transaction past the
  tier ceiling, late data past it, A1), never for `Admitted` or `Oversized`. `CrlAdmission`'s limit is an `AtomicLong`
  raised only upward (`raiseLimit`, from the CRL thread after a growth commit); `limitBytes`, `ceiling`, `tryAdmit`,
  `full` and `tryChargeLoaded` read the current value, so the retransmitted prepare meets the raised lane
- Oversized prepares (safety crash-recovery-log 10.20): `Frontend.admitPrepare` returns `PrepareAdmission`
  (`Admitted`, `Full`, `Oversized`) and `Store.receiveTransactionMessage` drops both non-admitted kinds the same way: no reply, no vote, no CRL write.
  `Oversized`: the transaction's counted bytes (for an admitted one, its bytes plus the uncounted data the prepare
  brings) exceed `crl.maxTransactionBytes`. Nothing is charged; data arriving after a data-less prepare is dropped
  likewise, leaving the store as one that never received it (a commit elsewhere reaches it through missed-update
  repair; the data-less admission is released by the usual paths). Warned once per `OversizedLogPeriod` (10 s) per
  store, naming the transaction and its bytes. Checked after `Full`, so a full lane never serializes

#### Object cache (`cache/`)
- Each store's object cache is its own LRU, owned by its executor, under one host byte budget (`HostObjectCache`): an
  over-budget store evicts its own oldest entries down to the budget, or posts a shed to the store holding the host's
  oldest; a shed frees to `maxBytes / 32` below the budget. `Frontend` pins states held by transactions and
  in-flight commits; a failed backend commit stays pinned and served

#### Backends (`backend/`)
- A backend's commit completion must mean durable: the CRL deletes a committed transaction's record once the backend
  commit completes. RocksDB writes synced, FSIF completes at the journal force. `MapBackend` is memory only and gives
  no durability (crash-recovery-log 3.4); it is for tests, and no opener constructs it
- Pluggable: RocksDB, in-memory Map, and FSIF (`backend/fsif`, a flat file of fixed-size segments located by a
  4-byte storePointer, with a journal and checkpointed index; design in `backend/fsif/DESIGN.md`). FSIF freezes
  writes on ENOSPC and keeps serving reads. A repair write never overwrites a different occupant of its slot: it
  fails with `backend.RepairSlotOccupied(objectId, occupant)`, which the repairer uses to drop a dead occupant
  (cross-document-review 2.4, repair and transactions). FSIF pools allocate through `FsifIndexAllocationStrategy`
  (`client/internal/allocation/CLAUDE.md`)
- Opening is backend-neutral (`BackendOpener`; only `DefaultBackendOpener` names concrete backends): see
  `server/host/CLAUDE.md`
- Creation is atomic for every backend (`StoreCreation`, systemic-loss-protection 4.1, row D5): `BackendOpener.create`
  builds the store, backend initialized and closed plus its config, in a scratch directory under `<device>/creating/`,
  forces the tree and renames it to the store's path. Open never creates: FSIF throws `StoreLost` for a missing
  directory or blank files, RocksDB `RocksDBStateLost` when `CURRENT` is missing, so a store that lost its files is
  rebuilt, never served empty. A new backend must keep both rules: create only in `init`, and treat an absent or empty
  store as lost
- `Reconcile` (exact repair install): `server/repair/CLAUDE.md`; `ReconcileResult.changed` is true only when the
  install was applied (every deferred, stale or no-op path reports false)
- Parked repairs (repair-process 8.2): a reconcile whose `RepairSnapshot.awaitSettle` is set, finding its object
  unsettled (a commit or repair write in flight or held, a failed commit, a deletion, holding for recovery), waits in
  `parkedRepairs` (at most one per object, a newer one answers the older stale) instead of answering stale.
  `commitTurnFree` runs it before telling any waiting transaction, so at a settled moment before the next commit starts,
  and `releaseHeldMessages` runs all of them when holding ends. It runs once, through `reconcileForRepair` with
  `awaitSettle` off. Past `Frontend.RepairSettleWait` (1 s, `expireParkedRepairs` on the store heartbeat) it answers
  stale. Fail-stop and close fail it
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
  the object has a pending backend read, a commit, repair write or deletion in flight, or a transaction referencing it
  (repair-process 9.2); while its backend removal is outstanding the object counts in `deleting`, so reads, snapshots
  and transactions get `ObjectNotFound`
