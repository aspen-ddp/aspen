# Missed-Update Repair: ErrorTree, UnackedCommits, Reconcile

Paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`. Code is cited as
`File.scala` plus the symbol. Related documents: [paxos-transactions.md](paxos-transactions.md),
[crash-recovery-log.md](crash-recovery-log.md), [transaction-requirements.md](transaction-requirements.md),
[transaction-recovery.md](transaction-recovery.md) (how a recovered or late commit vote consumes an UnackedCommits
entry), [rebuild-process.md](rebuild-process.md) (whole-store rebuild),
[systemic-loss-protection.md](systemic-loss-protection.md).

Contents: 1 Scope and model, 2 Tree schemas, 3 Entry production, 4 Scheduling and limits, 5 The scan, 6 Reconcile,
7 Race prevention, 8 Deferral, parking and proofs, 9 Deletes, 10 Other users of the reconcile path, 11 Invariants,
12 Failure scenarios, 13 Accepted limitations, 14 Open weaknesses.

---

## 1. Scope and model

### 1.1 What repair is for

A transaction commits when a write threshold of the stores hosting each of its objects voted commit. The other stores
may not hear the outcome (message loss, host down, slow, admission refusal). Such a store keeps an older copy: a
*missed update*. Repair brings the store's copy back into agreement with what a read of the rest of the system
returns. It is a background, eventually consistent process; reads and writes never wait for it. Reads are correct
without it because they restore from a read threshold (`IDA.consistentRestoreThreshold`, a majority) of agreeing
stores. Repair restores redundancy.

Repair never decides what a committed state is. It copies the state a normal read returns onto one store, under
guards that make the copy safe against concurrent transactions at that store.

### 1.2 Facts the design relies on

| # | Fact | Code |
|---|------|------|
| Q1 | For every IDA the read threshold `r` is a majority and `r <= w <= width` (`w` the write threshold), so `r + w > width`: any read set of `r` stores contains at least one commit voter of any committed transaction. | `common/ida/IDA.scala`; CLAUDE.md |
| Q2 | A store that voted commit holds the transaction's locks until it applies the outcome or the transaction resolves abort. A read reply reports them: locked keys (present keys locked, plus absent-key locks) and an object-level flag (exclusive object lock, content lock, range locks). A write locks every key it writes, and an object-level write takes the exclusive lock. Shared revision locks are not reported (they never change the object). | `server/store/ObjectState.scala` `lockedKeys`, `objectLevelLocked`; `RequirementsLocker.lock`, `lockWrites`; `Frontend.readForNetwork` |
| Q3 | The reader picks the revision held by at least `r` replying stores, whatever its timestamp (at most one revision can reach a majority). It returns once one does. Its read locks are the union over every store that replied with a state. Its timestamp is the maximum over the matching stores. | `client/internal/read/BaseObjectReader.scala` `getStoreStates`, `attemptRestore` |
| Q4 | A KeyValue key is restored from the stores holding a revision of it that `r` segments share. It is declared deleted only when no revision of it can still reach `r` and no store holds the lock of any segment's revision; otherwise the read waits (`NotRestorable`). | `client/internal/read/KeyValueObjectReader.scala` `resolve` |
| Q5 | A restored object with refcount 0 is `InvalidObject(provesDeletion = true)`. A read fails `InvalidObject` when more than `width - r` stores answered `ObjectNotFound`/`ObjectMismatch`, and `provesDeletion` is set when more than `width - w` did. A store that applied a deletion answers `ObjectNotFound`. | `BaseObjectReader.scala`; `client/ReadError.scala` `InvalidObject` |
| Q6 | Every apply sets the object's timestamp to the later of its current timestamp and the transaction's start (`later`), so object timestamps never decrease. Only revision-bearing requirements (`checkRevision`, a `KeyValueUpdate` with a required revision) check `state.timestamp > txTimestamp`. A `RefcountUpdate` and a key-level `KeyValueUpdate` do not, so two transactions on one object may commit in the opposite order of their starts. | `server/store/RequirementsApplyer.scala`; `RequirementsChecker.checkRevision`, `checkKVRequirements` |
| Q7 | A KeyValue update moves the object revision only when it has a required revision. | `RequirementsApplyer.scala` |
| Q8 | Stores refuse blind writes as `TransactionBuilder` does: a key Delete needs a `KeyRevision` on that key, a required revision or a content lock; a min/max/left/right change needs a required revision (except an allocation's initial content). So every key delete fails at a store lacking the key (and is marked there), and every object-level change moves the revision. | `RequirementsChecker.refuseBlindWrites`; `TransactionBuilder.refuseBlindWrites` |
| Q9 | A store holding recovered transactions (`Frontend.holding`: CRL still loading, a recovered Tx still loading its objects, or one deciding its vote) drops every client read, as a silent store would. A recovered commit vote retakes its locks before holding ends. | `Frontend.readObjectForNetwork`; `Tx.completeLoad` |

Q1 with Q2 and Q9 is why an unlocked part of a read reflects every transaction committed before the read began
(invariant E1, section 11).

### 1.3 Terms

- **Store**: `StoreId(poolId, poolIndex)`. **Slice**: the store's IDA-encoded part of an object.
- **Entry**: a record in the ErrorTree ("this store may be behind on this object") or the UnackedCommits tree ("this
  store cast a commit vote on this transaction and had not acknowledged applying it").
- **Snapshot**: the store's copy of an object, recorded on its executor before the repair reads (`RepairSnapshot`,
  `server/store/Reconcile.scala`).
- **Install**: writing the repaired state into the store's memory and backend (`Frontend.reconcileSettled`).
- **Deferred**: a part the read reports locked, or the store itself holds locked. Left alone.
- **Stale** (result flag): something the repair would change differs from the snapshot, the object is unsettled on
  the store, or the read cannot be used (or is older than the entry's missed timestamp). Left for a later pass.

---

## 2. Tree schemas

Both trees are Tiered Key Value Lists (`TieredKeyValueList`, KeyValue-object nodes, unsigned lexicographic key
order) rooted in keys of the pool's state object.

### 2.1 Where they live

| Item | Value | Code |
|------|-------|------|
| Pool state keys | `ErrorTreeKey=1`, `AllocationTreeKey=2`, `RepairPolicyKey=5`, `UnackedCommitsTreeKey=6` (others: config, allocation strategy) | `common/metadata/StoragePoolState.scala` |
| Pool state object | Always in the bootstrap pool | CLAUDE.md; `common/metadata/CLAUDE.md` |
| Tree handles | `pool.errorTree`, `pool.allocationTree`, `pool.unackedCommitsTree` | `client/internal/pool/SimpleStoragePool.scala` |
| Node placement | Ordinary pool: the pool's metadata group. Metadata pool: the bootstrap pool. Bootstrap pool: itself | `common/metadata/CLAUDE.md`; `server/store/Bootstrap.scala` |
| One tree per pool | A store's entries are the key range `[poolIndex, poolIndex+1)`; keys compare unsigned, and `poolIndex` is a byte, so `poolIndex+1` never wraps below 255 | `StoreRepairer.scan` |

Repair of pool P reads P's trees from a metadata pool M: an unavailable or degraded M stops repair of P (12.7).
Repair of M reads M's trees from the bootstrap pool. Repair of the bootstrap pool reads its own trees, which are also
swept node by node (10.1).

### 2.2 ErrorTree

| Field | Format | Code |
|-------|--------|------|
| Key | 17 bytes: `poolIndex`, `objectId` (msb, lsb, big-endian). One entry per (store, object). | `MissedUpdateMarker.errorKey`; `StoreRepairer.decodeObjectId` |
| Value | `missedTimestamp` (8, `HLCTimestamp.asLong`), object type (1: 0 Data, 1 KeyValue, 2 unknown), `storePointer` bytes (possibly empty) | `MissedUpdateFinalizationAction.encodeErrorEntry`, `decodeErrorEntry`, `ErrorEntry.pointer` |
| `missedTimestamp` | The start timestamp of the newest missed transaction recorded for (store, object). A store that applies it gets at least that object timestamp (Q6), so a read at least that new can reflect the miss (6.4). The key's own tree timestamp belongs to the marker transaction. | same |
| Tree metadata used | `ValueState.revision` (the marker transaction's id) as the compare-and-delete token and part of the deferral key; `ValueState.timestamp` (its start) as the entry's age | `StoreRepairer.repairErrorEntry` |
| Type and store pointer | With the tree's pool and the key's id they rebuild the object's pointer, which repair needs to read an object the AllocationTree no longer lists, and to delete its slice (FSIF needs the store pointer; an empty one means "by ObjectId", RocksDB). Deletion removes the AllocationTree entry, so the entry is then the only copy. The type is unknown only for a commit error on an object the transaction does not reference, which `RequirementsChecker` never reports. | `MissedUpdateFinalizationAction.writeMissedUpdates` |

### 2.3 UnackedCommits tree

| Field | Format | Code |
|-------|--------|------|
| Key | 17 bytes: `poolIndex`, `transactionId`. One entry per (store, transaction). An entry larger than `UnackedCommitEntry.maxEncodedSize` (half the leaf node size less 64 bytes) is written as chunks, each a complete entry for some of the objects: chunk 0 under the 17-byte key, chunk `i` under it plus `i` as 2 big-endian bytes. Lookups name the head only. | `UnackedCommitEntry.key`, `chunks`, `isHeadKey` |
| Value | `timestamp` (8, T's start), `incarnation` (16, the store's `StoreIncarnation` as the driver heard it, the zero UUID meaning unknown), `count` (4), then `count` x (`length` (4), `ObjectPointer` bytes) | `UnackedCommitEntry.encode`, `decode` |
| Objects | Every object of that store that T changes, traced or traceless | `MissedUpdateFinalizationAction.writeMissedUpdates` (`RequirementsApplyer.changedObjects`) |
| Meaning | The store cast a commit vote the commit may have relied on and had not acknowledged applying T when T was finalized: it is owed T. A recovered or late commit vote applies T through the entry only when the head names the store's own incarnation (`server/transaction/CLAUDE.md`, UnackedCommits; transaction-recovery.md). Otherwise, or when the store has forgotten T, the entry is left for repair, whose reconcile installs the committed state. | `Frontend.unackedLookupComplete`; `Host.unackedLookup` |

Repair ignores the incarnation: it reconciles the objects themselves, which is right for any incarnation.

### 2.4 AllocationTree (dependency, not repaired)

Key `objectId.toBytes`, value `ObjectPointer.toArray`. Written by `AllocationFinalizationAction`, which
`TransactionBuilder.addAllocatingObject` adds for every allocated object; removed by `DeletionFinalizationAction`.
Repair uses it as a hint: listed means "read it with this pointer"; absent means "deleted, or its allocation not yet
recorded", and a read decides (section 9).

### 2.5 Policy records

| Record | Key | Default | Code |
|--------|-----|---------|------|
| `RepairPolicy` | pool state `RepairPolicyKey` | `scanIntervalFloor` 30 s, `scanIntervalCap` 15 min, `minErrorEntryAgeForDeletion` 60 s, `proofAfterDeferrals` 10. An absent key gives the default; a non-positive field gives that field's default | `server/repair/RepairPolicy.scala` |
| `HostRepairLimits` | host state `RepairLimitsKey` | `maxConcurrentStoreScans` 4; non-positive gives the default | `server/repair/HostRepairLimits.scala` |

### 2.6 Who writes, who deletes

| Operation | By whom | When |
|-----------|---------|------|
| Insert or advance an ErrorTree entry | `MissedUpdateMarker.writeBatch`, from the finalizing client's `MissedUpdateFinalizationAction` | `errorTimeout` (2 s) after the action executes, for each store that did not report a clean commit |
| Insert an UnackedCommits entry | same batch | same, for each counted commit voter that sent no `TxCommitted` (every such store when the voters are unknown) and hosts an object T changes |
| Delete either kind | `StoreRepairer.deleteEntryIfUnchanged` only | after the object(s) are reconciled completely or proven deleted |
| Point lookup of an UnackedCommits head | `Host.unackedLookup` (`server/store/UnackedCommitLookup.scala`) | a recovered, expiring or late commit vote |
| Applied stubs | not repair's: each store writes and expires its own (`Frontend.expireStubs`; `server/transaction/CLAUDE.md`) | |

---

## 3. Entry production

Details are in [transaction-recovery.md](transaction-recovery.md); what repair relies on:

1. `TransactionBuilder` adds a `MissedUpdateFinalizationAction` to every transaction unless tracking is disabled
   (`disableMissedUpdateTracking`; `AspenClient.newRepairTransaction` for repair proofs; `limitTracking` for writes
   to a pool's own trees, which happens only for the bootstrap pool).
2. The marker action runs once every other finalization action of T has settled, succeeded or failed
   (`RegisteredTransactionFinalizerFactory.Finalizer`).
3. `updateCommitErrors`: when every data store replied and no error names a non-lock-only object, the action completes
   with no marks. Otherwise `execute` schedules `markMissedUpdates` after `errorTimeout` (2 s); stores replying
   meanwhile are not marked.
4. `writeMissedUpdates` builds:
   - `MissedUpdate(store, objectId, type, storePointer, txd.startTimestamp)` for every object a silent store hosts,
     except lock-only objects (`TransactionDescription.lockOnlyObjects`), and for every object a store reported a
     commit error on;
   - `MissedUnackedCommit(store, txid, UnackedCommitEntry(start, incarnation, changedObjectsOfStore))` for every store
     that sent no `TxCommitted`, is a counted voter (or the voters are unknown) and hosts an object T changes.
   Uncounted voters and non-voters are repaired by their ErrorTree marks alone.
5. `MissedUpdateMarker.mark` queues per pool, at most one write outstanding per pool. The queue merges ErrorTree marks
   by (poolIndex, objectId), keeping the later `missedTimestamp`, and UnackedCommits entries by (poolIndex, txid).
   A batch holds at most 256 entries, each kind guaranteed half.
6. `writeBatch` is one `replaySafe` transaction over both trees, retried until it succeeds except on
   `TransactionCreationError` or `NodeSizeExceeded` (then the batch fails and the driver re-runs the finalizer with
   backoff, `TransactionDriver.startFinalizer`). Its puts are conditional (`TKVLInsert.replaces`): an ErrorTree mark
   replaces a held one only for a later `missedTimestamp`, and otherwise is dropped from the batch with no write and
   no requirement; an UnackedCommits entry is written only where none is held. A replaced or new key carries a
   key requirement, so two markers racing on one key abort one batch, which retries.
7. The action completes only when the batch commits; T is not finalized (and its stores do not forget it) before.

Properties used later: every entry is written after T committed and at least 2 s after its finalizer started; an
ErrorTree entry's `missedTimestamp` never moves backward; an UnackedCommits entry is never rewritten in place, so its
revision changes only by delete and re-create. Section 14 (RP-W1) examines a consequence of the dropped mark.

---

## 4. Scheduling and limits

### 4.1 RepairService

One `RepairService` per host (`server/Host.scala`). `sweep()` is a non-concurrent polling task with a fixed 30 s
tick (`RepairService.TickPeriod`; stall report after 6 h).

```
sweep():
  repairable = target.repairableStoreIds   -- served, not offline / rebuilding / transferring out (HostStoreView.of)
  refreshLimits()                          -- 15 s deadline (MetadataDeadline); keeps the last good value
  due = selectDue(repairable, now)         -- drops state of departed stores; admits new ones (not due this tick)
  refreshPolicies(pools of due)            -- per pool, 15 s deadline, last good or default
  runBoundedParallel(due, maxConcurrentStoreScans)(scanOne)
scanOne(store):
  found = withDeadline(90 s)(StoreRepairer.scan(store, policy, deferralOf(store), control)).foundEntries
    on ScanTimedOut: control.cancel(); found = control.sawEntries
    on any other failure: found = false
  if bootstrap pool: found ||= withDeadline(90 s)(sweepBootstrapTrees(...)).needsAnotherPass  -- failure or timeout: true
  scanStates[store] = ScanState.advance(prev, now, found, policy)
```

A newly seen store is admitted at the floor, due at a random time inside the first floor interval. Because the tick
is 30 s, a floor below 30 s acts as 30 s.

### 4.2 Pacing (`ScanState`)

| Outcome | Next interval |
|---------|---------------|
| Any entry seen in either tree (repaired, deferred or skipped), or a bootstrap sweep needing another pass | floor |
| Clean scan, or a failed one, or one timed out before seeing any entry | `min(2 * current, cap)`, at least floor |

The next due time is `now + interval` jittered by +/-25 %.

### 4.3 Bounds

| Bound | Value | Code |
|-------|-------|------|
| Concurrent store scans per host | 4 by default | `HostRepairLimits` |
| Scan deadline, bootstrap sweep deadline | 90 s each (`ScanDeadline`, `SweepDeadline`): a read of an unavailable object never fails, so without a bound one sick pool would wedge the single-flight sweep | `RepairService.withDeadline` |
| Metadata reads (policy, limits) | 15 s | `MetadataDeadline` |
| Quarantine verification | 30 s | `VerifyDeadline` |
| Per-entry work | strictly sequential within a store: the walk hands one entry at a time; an UnackedCommits entry's objects go one at a time | `TierEngine.walk`; `StoreRepairer.repairUnackedObjects` |
| A failing entry callback | logged at error by the walk, which continues | `TierEngine.walk` |

A timed-out scan keeps running in the background (a hung read cannot be interrupted). Its `ScanControl` is
cancelled, so it starts no further entry (the walk still reads the rest of the range's nodes), skips
`DeferralState.retain`, and the store keeps its DeferralState for the next scan. A reconcile in flight at the cancel
still records its result, which can bring a proof one pass early (safe).

### 4.4 Ownership fence and store departure

- **Fence** (`StoreRepairer.OwnershipFence`, `ownedHere`): a scan runs only while the pool's state entry for the store
  names this host and the device the store is loaded from here (`RepairTarget.localDevice`, from
  `HostStoreView.storeDevices`). Each entry delete re-checks it, sharing one read for `OwnershipRecheck` (1 s). A
  failed check leaves that entry and abandons the rest of the scan. So a superseded copy (left on a device declared
  failed, or on another device of the same host) never consumes entries the current copy needs. The bootstrap sweep
  checks once at its start; it deletes nothing.
- **Departure**: every entry callback first checks `abandoned` (cancelled, fence lost, or `!isRepairable`). The
  `RepairTarget` operations (`snapshot`, `reconcile`, `repairDelete`) fail with `StoreNotHosted` for a store that has
  left (`StoreRouter.snapshotForRepair` etc.), and with `IllegalStateException` for a fail-stopped store, so a
  departed store is never treated as repaired and its entries stay for its new host.

---

## 5. The scan

### 5.1 Per store

```
scan(store, policy, deferral, control):          -- StoreRepairer.scan
  if !fence.stillOwned(): return empty result (warned)
  pool = client.getStoragePool(store.poolId)
  pool.errorTree.foreachInRange([poolIndex], [poolIndex+1], repairErrorEntry)              -- phase A
  pool.unackedCommitsTree.foreachInRange([poolIndex], [poolIndex+1], repairUnackedEntry)   -- phase B
  if !control.cancelled: deferral.retain(deferral keys touched this scan)
  return ScanResult(seen, repaired, deferred, unackedSeen)
```

The walk hands each callback the entries of the node it just read, so the `ValueState` the callback holds may be
older than the live value; deletion is therefore guarded by revision (5.5). Entries added during the walk may or may
not be visited. A join that deletes a node the walk was about to read makes it resume from the last key it visited
(`TierEngine.walk`, `MaxJoinRaceRestarts`).

### 5.2 ErrorTree entry (phase A): `repairErrorEntry`

```
if abandoned: return
seen += 1; decode objectId, (missedTs, type, storePointer); entryKey = errorDeferralKey(key, value.revision)
if target.mayJoinTransaction(store, missedTs): deferred += 1; return                    -- 5.4
alloc = pool.allocationTree.get(objectId)
listed:  done = repairListed(store, pointer(alloc), missedTs, entryKey)                 -- 6, 9.2
absent:  if age(value.timestamp) >= minErrorEntryAgeForDeletion (60 s):
             done = repairAbsent(store, entry's pointer, missedTs, entryKey, typeKnown)  -- 9.2
         else deferred += 1; return
if done: counts.recordCompleted(objectId, missedTs); deferral.clear(entryKey)
         deleteIfStillOwned(errorTree, value.revision, key)                              -- 5.5
else:    deferred += 1
```

Any exception in the callback (a decode failure, a read that fails with something other than `InvalidObject`,
`StoreNotHosted`, a failed delete) leaves the entry and is logged by the walk.

### 5.3 UnackedCommits entry (phase B): `repairUnackedEntry`, `repairUnackedObjects`

```
if abandoned: return
unackedSeen += 1; txid = decodeTransactionId(key)
if target.holdsTransaction(store, txid): deferred += 1; return     -- live Tx, or the store is still loading its CRL
entry = decode(value)
if target.mayJoinTransaction(store, entry.timestamp): deferred += 1; return                  -- 5.4
for each ptr in entry.objects, one at a time:
    if counts.completedAt(ptr.id) >= entry.timestamp: complete           -- phase A completed it this scan
    else alloc = allocationTree.get(ptr.id)
         absent and value.timestamp old enough: repairAbsent(store, ptr, entry.timestamp, unackedDeferralKey)
         absent, too young:                     incomplete
         listed:                                repairListed(store, ptr, entry.timestamp, unackedDeferralKey)
if all complete: clear deferral keys; deleteIfStillOwned(unackedCommitsTree, value.revision, key)
else deferred += 1
```

`holdsTransaction` is `Frontend.mayHoldTransaction` (`awaitingCrl || transactions.contains(txid)`), read from the scan
thread through `@volatile` fields (`StoreRouter.holdsTransaction`). While the store holds T, its Tx applies T itself
(through the entry when it names the store's incarnation). Each chunk of a chunked entry is repaired and deleted as an
entry of its own; all are skipped while the store holds T.

The phase-A shortcut: phase A records each object it completed with the entry's `missedTimestamp`
(`Counts.recordCompleted`); phase B treats an object recorded at or after the UnackedCommits entry's timestamp as
complete with no allocation-tree read, snapshot or reconcile. A silent voter has both kinds of entry for every
object, so its UnackedCommits entry then costs only its delete. Section 14 (RP-W1) shows where this argument is weaker
than it looks.

### 5.4 Waiting until the store can no longer join the transaction

Both kinds of entry are left for a later pass while `RepairTarget.mayJoinTransaction(store, start)` holds: the entry's
transaction started at or above the store's forget floor (`StoreRouter.mayJoinTransaction` =
`!Frontend.belowForgetFloor(start)`). The floor (`Frontend.forgetFloorMillis`) is the latest of `now - horizon`
(`TransactionHorizon.Horizon`, 20 s), the store's load floor (`loadFloorMillis`: its load plus `MaxStartLead`, or plus
`RebuildingStore.AcceptorSinceMargin` soon after an arrival) and the status cache's eviction watermark. At or above
it, a replayed prepare could still create a Tx for T at the store after the scan's `holdsTransaction` check, apply T
after the reconcile, and the entry would be deleted under that apply. Below it the store refuses every prepare for a
transaction it does not hold (`Frontend.receivePrepare`), and a store never rejoins a transaction it forgot. The wait
is at most the horizon plus `MaxStartLead` (2 s) plus clock skew. For an ErrorTree entry the check uses
`missedTimestamp`, the newest merged miss; older merged misses started earlier and are below the floor too.

### 5.5 Entry deletion and its guard

`deleteIfStillOwned` re-checks the fence (4.4) and then runs `deleteEntryIfUnchanged`: an ordinary transaction
(`client.newTransaction()`) that deletes the key through the tree (`TieredKeyValueList.deleteIfPresent`) with the
requirement `KeyRevision(key, entryRevision)`, the revision the walk saw. The `KeyRevision` is also the transaction's
pin. Deleting through the tree, not the node, lets an emptied node be joined or removed (the join and tail-removal
finalization actions), so the trees shrink as repair drains them. The delete is tracked, with
`MissedUpdateFinalizationAction.limitTracking(entryPool, tx)` disabling tracking only when the delete touches the
entry's own pool, which is the bootstrap pool's own trees (a store of the metadata pool that misses it gets an entry
one level closer to the bootstrap pool, so the chain ends there).

If any writer replaced the entry since the walk read it, the requirement fails and the delete aborts; the next pass
sees the new entry. A delete and a concurrent marker write of the same key collide (both require the key); one
orders first. A key already gone makes `deleteIfPresent` false and the callback fails (logged), costing nothing.

---

## 6. Reconcile: the per-object algorithm

`StoreRepairer.repairUpdate` runs the steps; `Reconcile.plan` is the pure decision; `Frontend.reconcileForRepair` and
`reconcileSettled` apply it on the store's executor.

### 6.1 Steps

```
repairUpdate(store, ptr, minTimestamp, entryKey, policy, deferral, allowProof):
  snap = target.snapshot(store, ptr)              -- Frontend.snapshotForRepair, on the executor
  snap = snap.copy(only = deferral.deferredKeys(entryKey)            (only with allowProof),
                   awaitSettle = staleCount(entryKey) >= ParkAfterStalePasses (3)  (only with allowProof))
  os   = client.read(ptr)                         -- a full read: only full reads report locks
  first = reconcile(snap, os); if os.timestamp < minTimestamp: first.stale = true
          (RepairSlotOccupied: dropOccupant, then stale)                              -- 6.5
  passes = deferral.record(entryKey, first)
  if allowProof && passes >= policy.proofAfterDeferrals:
      proven = prove(os, first)                    -- section 8
      if proven nonEmpty: result = reconcile(snap.copy(proven = proven), os); deferral.afterProof(entryKey, result)
  if allowProof: deferral.recordStale(entryKey, result)   -- warns every proofAfterDeferrals stale passes in a row
```

`snapshotForRepair` gives `RepairSnapshot.of(state)` for a served state, `missing` for an object being deleted, and
otherwise loads it from the backend (`SnapshotRead`). A snapshot is a fresh copy each pass.

### 6.2 What the executor does at install

`Frontend.reconcile` refuses to write unless the object is settled:

```
unsettled = holding || commitInFlight(obj) || commitFailed(obj) || committing.contains(obj) || deleting.contains(obj)
         || another in-memory state of the object is the one served or referenced
if unsettled && snapshot.awaitSettle: park (8.2)
elif unsettled: stale, nothing written
else reconcileSettled
```

`commitInFlight` covers outstanding transaction commits, repair writes and committed transactions holding their
commit for their turn (`heldCommits`). `commitFailed` includes objects poisoned by `poisonObject`.

`reconcileSettled`, all in one executor task:

1. `current` = the store's copy now (`RepairSnapshot.of`, or `missing`).
2. `locks` = the read's reported locks, less the parts a proof covered (`snapshot.proven`), plus the store's own
   (`os.lockedKeys`, `os.objectLevelLocked`): a store that did not answer the read may hold locks of its own.
3. `plan = Reconcile.plan(snapshot, current, read, storeId, locks)`.
4. If the plan changes nothing, return its result. Otherwise mutate the in-memory state (key installs and deletes,
   min/max/left/right, re-encode, data, metadata), insert it in the object cache, retain it, add it to `committing`
   and count it in `txCommitsInFlight`, then `backend.repair(CommitState)`. Transactions on the object wait behind the
   repair write as behind a commit. On completion `repairWriteComplete` releases it and passes the object's turn on
   (`commitTurnFree`). A failed write discards the in-memory state (the next use reloads the backend's) and fails the
   promise, so the entry stays; the object is not marked failed, since no transaction relied on the repaired state.

A `quarantine`d object is repaired like any other (its verification uses this path, 10.2).

### 6.3 `Reconcile.plan`

Early exits: the read is of another pool: stale. `current` missing but the snapshot was not (deleted since): stale,
since installing would resurrect the slice. A partial or metadata-only read, or a `poolIndex` outside the read's IDA
width: stale. Final rule: a copy is created only together with its refcount; otherwise stale.

**Refcount** (`planRefcount`):

```
wanted = current.isMissing || read.refcount.updateSerial > current.refcount.updateSerial
if !wanted: none
elif current.refcount != snapshot.refcount: stale
else: install read.refcount
```

The serial decides (a decrement with a higher serial is installed). A refcount whose serial is not strictly higher is
never installed unless the store has no copy.

**DataObject** (`planData`):

```
if current.isMissing && locks.objectLevel: objectLevelDeferred (the copy cannot be created without its refcount)
refcount = locks.objectLevel ? none : planRefcount
data = locks.objectLevel || read.revision == current.revision:                 none
       read.timestamp < current.timestamp || current.revision != snapshot.revision: none, stale
       else: the store's slice of read.data, with read.revision, timestamp max(read, current)
result = (no keys, objectLevelDeferred = locks.objectLevel, stale = refcountStale || dataStale)
```

A read older than the store with a differing revision is stale, not caught up: the store may have missed a data
write and applied a later refcount update the read's responders have yet to apply.

**KeyValueObject** (`planKeyValue`):

```
if current.isMissing && locks.objectLevel: defer everything (keys = locks.keys, objectLevel)
keys = (current.keys ∪ read.keys ∪ snapshot.keys) ∩ snapshot.only (when set)
for each key not in locks.keys:
    if current.rev(key) != read.rev(key):
        if current.rev(key) != snapshot.rev(key): stale              -- the store changed it since the snapshot
        elif read has it: install (slice of the read's value, its revision and timestamp)
        else: delete it
wholeObject = snapshot.only.isEmpty; objectLevelLeft = locks.objectLevel || !wholeObject
refcount = objectLevelLeft ? none : planRefcount
min/max/left/right + revision: objectLevelLeft: none
    equal to the read's and the store has a copy: none
    current differs from snapshot on them or on the revision: stale
    else install the read's (slices of left/right) and read.revision
timestamp = wholeObject && read.timestamp > current.timestamp ? read.timestamp : none
result.deferredKeys = locks.keys (∩ only); objectLevelDeferred = locks.objectLevel && wholeObject
```

A pass narrowed to `only` (the keys an earlier pass deferred) leaves the refcount, object-level parts, timestamp and
other keys alone: they were reconciled by that earlier pass. A whole-object pass that defers some keys still takes the
read's object timestamp: a caught-up store would hold it, key requirements check per-key timestamps, and the entry
stays incomplete until the deferred keys are reconciled. A narrowed pass never touches the timestamp, because its read
also reflects keys outside its scope.

### 6.4 Result and the completion gate

`ReconcileResult.complete = deferredKeys.isEmpty && !objectLevelDeferred && !stale`. Only a complete result lets the
caller treat the object as done. `repairUpdate` also marks the result stale when the read's timestamp is older than
`minTimestamp` (the entry's `missedTimestamp`, or T's start).

The gate is sound in the direction it needs: every object timestamp on a store was set by an apply (`later`, Q6) or
copied from a read (Reconcile installs `read.timestamp` only when higher; a rebuild copies one into its staging store),
and a read's timestamp is the maximum over stores that match its revision, so it is one some store really holds. When
a read reflects the missed transaction, some matching store applied it, so the read's timestamp is at least its start
and the gate passes. A post-install equality test would be weaker: equality with a read says nothing about whether the
read includes the miss. The gate does not show that the read reflects every merged miss; that rests on E1 and on the
read following each miss's commit (section 14, RP-W1).

### 6.5 A foreign occupant of the slot

A repair write that finds another object in the slot (`backend.RepairSlotOccupied`, FSIF slot reuse over a missed
delete) drops the occupant (`StoreRepairer.dropOccupant`) with an id-checked `repairDelete`, but only when the
AllocationTree does not list it: the object was just read live, and two live objects never share a slot. The repair
is stale, so the entry stays and the next pass installs. A listed occupant is logged as an error and left.

---

## 7. Race prevention: every mechanism, and what it prevents

| # | Mechanism | Code | Prevents |
|---|-----------|------|----------|
| M1 | Quorum read plus lock reporting (Q1, Q2, Q9) | `BaseObjectReader`; `ObjectState.lockedKeys` | Installing a state older than a committed transaction on a part not reported locked |
| M2 | Snapshot before read; install compares | `StoreRepairer.repairUpdate` | Overwriting anything the store applied after the snapshot |
| M3 | Install only what still matches the snapshot: per key revision, object revision, refcount, min/max/left/right; a mismatch is stale | `Reconcile.plan*` | Overwriting a newer committed change, including one the read did not see |
| M4 | Install on the executor in one task; refuse while the object is unsettled | `Frontend.reconcile` | A repair write interleaving with a commit's backend write (backend writes of one object are unordered) |
| M5 | The repair write is itself an in-flight commit (retained, `committing`, `txCommitsInFlight`) | `Frontend.reconcileSettled` | A transaction or read seeing the pre-repair backend state while the write is outstanding |
| M6 | The store's own locks defer parts as the read's do | `Frontend.reconcileSettled` | Repairing over a part the store's own voted transaction is about to write |
| M7 | Refcount only with a strictly higher `updateSerial`, or when the store has no copy | `Reconcile.planRefcount` | Lowering a refcount; un-deleting |
| M8 | DataObject data only when revisions differ, the read is not older than the store, and the store's revision still equals the snapshot's | `Reconcile.planData` | Installing an older revision over a newer one (revisions are transaction ids, so no ABA) |
| M9 | A copy is never created without its refcount, never while the object level is locked, never if the store deleted it after the snapshot | `Reconcile.plan`, `planData`, `planKeyValue` | Resurrecting a deleted slice |
| M10 | No repair write for an object being deleted on the store; `repairDelete` refused while the object is in use | `Frontend.reconcileForRepair`, `deleteObjectForRepair` | Writing after a deletion; a delete racing a read, commit or transaction |
| M11 | Skip an UnackedCommits entry while the store holds T (or is loading its CRL) | `Frontend.mayHoldTransaction` | Repair racing the Tx that applies T; deleting an entry a recovered vote still needs to look up |
| M12 | Leave entries while the store may still join their transaction | `RepairTarget.mayJoinTransaction` | A replay's Tx applying T after the reconcile and the entry being deleted under it |
| M13 | Compare-and-delete on the entry's revision | `deleteEntryIfUnchanged` | Dropping an entry a newer mark rewrote |
| M14 | Completion requires the read to be at least as new as the entry's timestamp | `repairUpdate` | Dropping an entry when the read cannot reflect the miss |
| M15 | Proof transactions for persistently locked parts | `RepairProofs` | Waiting forever on a hot lock |
| M16 | Parking a persistently stale repair until the object settles | `Frontend.parkRepair` | Retrying forever an object always busy at install |
| M17 | Absence from the AllocationTree is not a deletion; only `InvalidObject.provesDeletion` drops a slice | `StoreRepairer.repairAbsent`, `repairListed` | Deleting the slice of a live object whose allocation is recorded late |
| M18 | Ownership fence; `StoreNotHosted`; `abandoned` | `OwnershipFence`; `StoreRouter` | A departed or superseded copy consuming entries |
| M19 | A failed backend write keeps the entry | `Frontend.repairWriteComplete` | Dropping an entry when the install did not reach disk |
| M20 | Proofs run untracked and replay-safe; entry deletes run tracked except in the bootstrap pool's own trees | `AspenClient.newRepairTransaction`; `limitTracking` | Entry feedback loops |

---

## 8. Deferral, parking and proofs

### 8.1 DeferralState

In memory, one per store, kept across scans by `RepairService` and dropped when the store leaves the repairable set
(`StoreRepairer.DeferralState`). Keys: `errorDeferralKey(key, entry revision)` (prefix 0), `unackedDeferralKey(key,
objectId)` (prefix 1), `sweepDeferralKey(nodeId)` (prefix 2), `verifyDeferralKey(objectId)` (prefix 3).

- `record(entryKey, result)` counts consecutive passes that deferred something by a lock (`deferredKeys` non-empty or
  `objectLevelDeferred`). A pass that defers nothing by a lock (complete, or only stale) forgets the entry and returns
  0. When the pass was neither object-level deferred nor stale, `only` is set to the deferred keys, and the next pass
  reconciles only those.
- `recordStale` counts consecutive stale passes separately; a non-stale pass resets it.
- `retain` (a completed scan) forgets entry keys the scan did not touch; sweep and verification keys are kept.
  `retainSweep` (a complete sweep walk) prunes sweep keys. A host restart forgets everything; the next pass reconciles
  whole objects and restarts the counts. Because an ErrorTree deferral key includes the entry's revision, a rewritten
  entry starts over with a whole-object pass.

### 8.2 Parking (persistently stale objects)

A proof cannot help a stale result (the store's own copy is busy or changed). After `ParkAfterStalePasses` (3) stale
passes in a row, the repair sets `RepairSnapshot.awaitSettle`. An unsettled store then parks the reconcile
(`Frontend.parkRepair`; at most one per object, a newer one replacing an older, which answers stale) instead of
answering stale. `commitTurnFree` runs it first, at the one moment the object is settled, before any waiting
transaction is told; the end of holding retries every parked repair too. It runs once, through the ordinary
snapshot-checked reconcile, so a part the completing commit changed still comes back stale. One that never sees its
object settle answers stale after `Frontend.RepairSettleWait` (1 s), checked on the store heartbeat
(`expireParkedRepairs`); fail-stop and close fail it. The commit path pays one empty-map check while nothing is parked.

### 8.3 When a proof runs

At `passes >= proofAfterDeferrals` (default 10, about 5 minutes at the 30 s floor), `prove(read, result)` runs proofs
one at a time (parallel proofs of one object would collide), each a `replaySafe` transaction from
`client.newRepairTransaction()` (tracking off):

| Deferred part | Proof | Code |
|---------------|-------|------|
| each deferred key (KeyValue) | an update with no operations requiring `KeyRevision(key, readRevision)`, or `DoesNotExist(key)` when the read lacked it | `RepairProofs.proveKey` |
| object level, KeyValue | `RefcountUpdate(required = rc, new = rc)`, then `ExclusiveRevisionLock(read.revision)` | `proveRefcount`, `proveObjectLevel` |
| object level, DataObject | `RefcountUpdate(required = rc, new = rc)` | `proveRefcount` |

Each completes true only on commit; an abort or error is false. If any object-level proof fails, the object level is
unproven. A committed proof means a write threshold of stores agreed with the read on that part at that moment and
none held a conflicting lock (the requirements take the same locks a real write would). Any other committed write of
that part needed a vote from a store in the proof's set, which would have collided or mismatched, so the read was
current on it at proof time. The scan then reconciles from the same read with `snapshot.proven` set: the read's locks
on proven parts are ignored, the store's own locks still defer, and the snapshot check still applies.

The object-level proof compares the revision only, which suffices because every min/max/left/right change moves the
revision (Q8). Every requirement on a refcount-0 or missing object fails `MissingObject`
(`RequirementsChecker.loaded`), so no proof revives a deleted object: a proof from a pre-deletion read aborts, the entry
stays, and the next pass's read proves the deletion.

---

## 9. Deletes

### 9.1 How a deletion reaches a store

An object is deleted by a transaction setting its refcount to 0 (a `RefcountUpdate`, traceless), whose
`DeletionFinalizationAction` later removes the AllocationTree entry. A store that applies it removes the object and
answers reads `ObjectNotFound`. A store that missed it keeps a positive refcount. A missed deletion is marked like any
other miss (ErrorTree for silent or erroring stores, UnackedCommits for unacknowledged counted voters).

### 9.2 How a missed delete is repaired

- **Listed** (`repairListed`): the repair read may find the object deleted (its DeletionFinalizationAction has not run
  yet). A read failing `InvalidObject.provesDeletion` drops the slice (`repairDeletion`) with no age gate; an
  `InvalidObject` short of a proof defers the entry.
- **Absent** (`repairAbsent`): absence may be a late `AllocationFinalizationAction`. Once the entry is at least
  `minErrorEntryAgeForDeletion` (60 s) old by the repairing host's HLC (`errorEntryMayBeDeleted`; a future-dated entry
  is ineligible), the object is read through its pool with the entry's pointer. A proven deletion drops the slice; a
  restored read means the object is live and it is reconciled as if listed (logged); anything else keeps the entry.
  An ErrorTree entry of unknown type is acted on only for a proven deletion.
- `provesDeletion` needs more than `width - w` `ObjectNotFound`/`ObjectMismatch` replies, or a majority at refcount 0
  (Q5). A committed live object is on at least `w` stores, and `w >= r`. A commit voter that has not yet applied an
  allocation does not answer `ObjectNotFound`: the read parks there until the allocation applies or aborts
  (`Frontend.parkAllocationRead`, `allocationPending`), so a read inside an allocation's apply window proves nothing,
  and the 60 s gate only paces. Clock skew moves the gate by at most the skew plus `MaxStartLead`; safety does not rest
  on it.
- `repairDeletion` (`Frontend.deleteObjectForRepair`) is refused (`RepairDeleteDeferred`, counted deferred) while the
  store is holding, or the object has a pending backend read, a commit, a repair write or a deletion in flight, or a
  transaction references it. Otherwise it evicts the cache, counts the object in `deleting` (reads, snapshots and
  transactions get `ObjectNotFound`), waits for allocations to be durable (`awaitAllocationsDurable`), then
  `backend.repairDelete`: RocksDB deletes the key; FSIF deletes the extent only if its slot still holds that id. Both
  are idempotent.

### 9.3 Can a store that missed a delete resurrect the object?

By reads: no. A stale store is at most `width - w` stores, fewer than `r` (Q1), so it cannot form a matching set, and
more than `width - r` deleters make the read `InvalidObject`. A deleter that has only voted holds its object lock,
which defers repair and makes readers wait.

By repair: a read of a deleted object fails before reconcile runs. A store with no copy gets one only from a read with
a live refcount and no object-level lock (M9). If the store deleted the object after the snapshot, the plan is stale,
and `reconcileForRepair` answers stale for an object being deleted.

Window: a store with no copy can install a pre-deletion read if the deletion commits after the read and had not been
voted at any responder at read time. The deletion cannot apply at that store (no object: `MissingObject`), so the
store reports an error, is marked, and the next pass proves the deletion and drops the slice. The slice is a zombie
until then; reads ignore it (Q4/Q5).

### 9.4 Can a missed key delete resurrect a key?

On reads: no (Q4). On repair: the store is made to match the read key by key; a key the read lacks is deleted at the
store if its revision still equals the snapshot's. Every key delete carries a `KeyRevision`, a required revision or
a content lock (Q8), each of which fails at a store lacking the key, so a delete never applies there as an invisible
no-op that the snapshot check would miss: it is reported and marked instead.

---

## 10. Other users of the reconcile path

### 10.1 Bootstrap tree sweep

Writes to the bootstrap pool's own ErrorTree and UnackedCommits nodes run untracked (`limitTracking`), so a bootstrap
store that misses one gets no entry. `StoreRepairer.sweepBootstrapTrees` heals it: after each entry scan of a
bootstrap store (success or failure), it reconciles the store's copy of every node of both trees, every tier
(`TieredKeyValueList.foreachNode`), through `repairUpdate` with no minimum timestamp, no proofs (they would collide
with marker traffic), never narrowed to deferred keys (a later missed untracked write records nothing), keyed
`sweepDeferralKey`. A node deleted under the walk, an unreadable node, or one skipped because the store stopped being
repairable or the sweep was cancelled counts as incomplete. `SweepResult.needsAnotherPass` (anything reconciled or
incomplete, or a walk that missed nodes) keeps the store at the floor; a clean sweep lets it back off. Its staleness
window is an accepted limitation (13).

### 10.2 Quarantine verification

A store quarantines an object a recovered commit vote could not apply (`Frontend.quarantine`): every transaction on it
votes abort and committed ones apply nothing to it, until `QuarantineVerifier` (through `RepairService.verifyObject`,
30 s deadline) shows the store's copy matches a read at least as new as the quarantining transaction's start.
`StoreRepairer.verifyObject` is `repairUpdate` with that `minTimestamp`, proofs allowed, keyed `verifyDeferralKey`. It
reads and deletes no entry, so the fence and `mayJoinTransaction` do not apply. A failed or incomplete verification
is retried at store heartbeat with doubling backoff.

### 10.3 No opportunistic rebuild

There is no read-driven repair path: the read-triggered and pre-transaction opportunistic rebuilds were removed
because their same-revision guard could overwrite blind writes and left `kvState` stale
(`docs/superpowers/specs/2026-11-14-opportunistic-rebuild-removal-design.md`). Every repair goes through Reconcile.

---

## 11. Invariants

| ID | Invariant | Enforced by |
|----|-----------|-------------|
| E1 | An unlocked part of a read reflects every transaction committed before the read began. | Q1, Q2, Q9; a voter that lost its vote state: [crash-recovery-log.md](crash-recovery-log.md) |
| E2 | Repair writes a store only on its executor, only when the object is settled, and the write is itself an in-flight write. | M4, M5 |
| E3 | Repair never changes a part of the store's copy that differs from the snapshot taken before the read. | M2, M3, Q8 |
| E4 | Repair never installs a part the read or the store reports locked (unless a proof covered the read's lock). | M6, `Reconcile.planKeyValue` |
| E5 | Repair never lowers a refcount's `updateSerial`, and installs no refcount while the object level is locked. | M7 |
| E6 | Repair never installs an older DataObject revision over a newer one. | M8, Q6 |
| E7 | Repair never creates a copy without its refcount, while the object level is locked, or after the store deleted it. | M9 |
| E8 | An entry is removed only after every object it names was reconciled completely (or was completed by phase A of the same scan at a timestamp at least the entry's), or was proven deleted and dropped. | 5.2, 5.3, M17 |
| E9 | An entry is removed only if it has not been rewritten since the walk read it, and only while the store's pool entry names this host and device. | M13, M18 |
| E10 | An entry whose repair fails, is deferred or is stale stays. | 5.2, 5.3 |
| E11 | An UnackedCommits entry is not acted on while the store holds T or is loading its CRL; no entry is acted on while the store may still join its transaction. | M11, M12 |
| E12 | Repair proofs never create entries; entry deletes create entries only one level closer to the bootstrap pool. | M20 |
| E13 | A complete result implies a read at least as new as the entry's timestamp. | M14 |
| E14 | Missing or failed metadata, policy or limits never stops a scan. | `RepairService.refreshPolicies`, `refreshLimits` |
| E15 | Progress: an entry whose object is readable, not locked at every attempt, and settles within `RepairSettleWait` is removed within a bounded number of passes. | M15, M16; limits in 13 and 14 |

Assumptions: (A1) a store reports the locks of every transaction it voted commit on and has not applied, for every
locking requirement; (A2) it reports them for the whole interval between vote and apply, across restarts (Q9);
(A3) object timestamps never decrease (Q6); (A4) a store completes a backend commit before it forgets the
transaction; (A5) every merged miss of an ErrorTree entry committed before the read that completes it (see RP-W1).

---

## 12. Failure scenarios

### 12.1 Repair racing with a commit C at store S

| When | Event | Outcome |
|------|-------|---------|
| Before snapshot | C applied at S | Snapshot includes it; the read reflects it (E1). Safe. |
| Snapshot to install | C applies at S | S's key/revision/refcount/object level differs from the snapshot: stale. Safe. |
| Snapshot to install | S voted C (holds locks), not yet applied | S's own locks defer those parts. Safe. |
| During the read | C's voter among the responders applied it or holds its locks | Read reflects C or reports its locks. Safe. |
| Read to install | C commits elsewhere; S has not voted | The install writes the read's state, a state the system had. C's apply at S, or C's mark for S, follows; its requirements then check the repaired state. Safe, but see RP-W1 for the entry. |
| At install | A commit's backend write outstanding, or a commit held for its turn | Unsettled: stale or parked. Safe. |
| After install, before the backend completes | A new transaction arrives | It waits behind the repair write. Safe. |
| After install | Backend write fails | State discarded and reloaded; promise fails; entry kept. Safe. |

### 12.2 Repair reading from lagging peers

Stale peers are a minority of matching responders. A read returns only when `r` responders share a revision (Q3); for
any committed C at least one of them voted commit (Q1) and either shows C or reports its locks (deferring C's parts).
S itself may answer the read; a matching set made only of stores that missed C would need `r` stores outside C's
`w` voters, which do not exist. A read waits while no revision has a majority among the replies. Reads from a store
that is rebuilding or lost its CRL state are covered by [rebuild-process.md](rebuild-process.md) and
[crash-recovery-log.md](crash-recovery-log.md).

### 12.3 Crash mid-repair

| Crash point | After restart | Safe? |
|-------------|---------------|-------|
| Before install | Nothing changed; entry present; DeferralState lost | Yes: the next pass reconciles the whole object |
| After the in-memory mutation, before the backend write completes | Memory lost; the backend has the old or the new state (one backend commit) | Yes: the entry was not deleted |
| After the backend write, before the entry delete | The next pass finds the store in sync; no-op; entry deleted | Yes |
| During or after `repairDelete`, before the entry delete | The backend delete is idempotent; the next pass proves the deletion again | Yes |
| During the entry delete | It commits or not; guarded by revision | Yes |
| The marker crashes after T committed | Finalization re-runs (at-least-once); entries are written later | Yes |
| The host crashes; the store is adopted elsewhere | The new host's service starts with fresh ScanState and DeferralState; the old copy is fenced (4.4) | Yes: entries are the only durable state |

### 12.4 Entry deleted while a new mark for the same key is written

Delete D (requires `KeyRevision(key, Rv)`) and mark I:
- I rewrites the entry before D commits: D fails its requirement; the next pass repairs the new entry.
- D commits before I reads the key: I creates a fresh entry.
- D and I concurrent and both writing: they collide; one aborts and retries (the marker retries until success; the
  delete costs one more pass).
- I declined (its `missedTimestamp` is not later than the held one): I writes nothing and requires nothing, so it
  orders against D in no way. This is safe only if D's completing read followed I's transaction's commit: RP-W1.

### 12.5 Entry for an object that has since been deleted

Listed: the repair read proves the deletion and the slice is dropped, or the entry waits for a proof. Absent: after
60 s a read proves it and the slice is dropped. An object already below `r` live copies through some other loss reads
`InvalidObject`; if more than `width - w` stores lack it, that proves "deletion" and the remaining slices are dropped,
but the object was unreadable already.

### 12.6 Entry whose store has been rebuilt or transferred

- Rebuilding: not repairable; entries stay. After promotion the next scan reconciles them (no-ops, since the rebuild
  copied current state) and removes them. Entries are keyed by pool index, so a rebuilt store inherits them harmlessly.
- Transferred or adopted: repairable only on the host the pool entry names, on the named device (fence). An in-flight
  `snapshot`/`reconcile`/`repairDelete` fails with `StoreNotHosted`. The entry stays for the new host.

### 12.7 Trees unavailable (metadata pool degraded)

- A tree walk or `allocationTree.get` hangs: the 90 s deadline cancels the scan. A scan that had seen no entry backs
  off; one cut off while working through entries stays at the floor and keeps its DeferralState.
- A tree walk fails: logged, treated as clean, the interval doubles toward the cap.
- A marker write cannot commit: `retryUntilSuccessful`; the transaction stays unfinalized and its stores keep its
  state (the `FINALIZATION STALLED` alarm after 60 s). A permanent error fails the batch and the finalizer is re-run.
  No entry is lost.
- An entry delete fails: one more pass. Policy or limits unreadable: last good or defaults.

### 12.8 An object whose read never completes

A read of an object below its read threshold, or of a key that stays `NotRestorable`, or of an allocation held
indefinitely (Backlog.txt "Repair scan stalls on an allocation held past expiry"), never completes. The store's scan
stops at that entry until the deadline cancels it; the next scan stops at the same entry (the walk always starts at
the range's head), so later entries of the store are not repaired, and each scan leaves one read retrying in the
background. Tracked in Backlog.txt (the fix there, timed reads mapped to deferral, covers every cause above).

### 12.9 More scenarios

| Scenario | Outcome |
|----------|---------|
| Two scans of one store overlap (a timed-out scan's in-flight entry still running) | They share the store's DeferralState; both are guarded by M3/M4/M13; at most one reconcile parks per object. Duplicated work only. |
| The AllocationTree's pointer differs from the entry's | The listed pointer is used for the read; the entry's only when absent. |
| A malformed entry | The callback fails; logged at error each pass; the entry stays forever. |
| An entry for a pool index beyond the IDA width | Stale every pass; stays forever. Width changes do not exist. |
| FSIF has no free extent at install | `backend.repair` fails; the entry stays. |
| A proof aborts every time | `prove` returns nothing; the pass's result stands; each later pass retries every deferred proof. Bounded by one object's deferred set per pass; a lock held at every attempt starves the part. |
| Clock skew | The age gate uses this host's HLC against the marker's; `mayJoinTransaction` uses this host's wall clock against T's start. The reconcile guards use revisions and transaction ids. |

---

## 13. Accepted limitations

- **Bootstrap tree sweep staleness window** (Backlog.txt): a bootstrap store whose scans come back clean backs off to
  the 15 min cap, so a missed untracked write to the bootstrap pool's own trees can stay unhealed that long.
- **Head-of-line blocking on an unreadable object** (Backlog.txt "Repair scan stalls on an allocation held past
  expiry"; 12.8).
- **Traceless follow-ups** (Backlog.txt "Traceless commits follow-ups"): UnackedCommits entries are per transaction,
  so a voter that keeps missing `errorTimeout` grows the tree.
- **Large UnackedCommits entries**: a `NodeSizeExceeded` for a node too small for one object's chunk is permanent; the
  transaction stays unfinalized by design (finalizing without the entry would leave its store unrepaired), raised by
  `FINALIZATION STALLED`.
- **Tree space during a long outage**: one ErrorTree entry per (store, object) and one UnackedCommits entry per
  (store, transaction) accumulate in metadata-pool backend space; no bound against a long outage is argued (see
  [systemic-loss-protection.md](systemic-loss-protection.md) and the cross-document capacity argument). The drain is
  sequential per store: one entry's read, install and delete at a time.
- **Proof starvation**: a part locked at every proof attempt is never proven (12.9).
- **Persistently stale objects**: an object whose copy changes under every install, or never settles within
  `RepairSettleWait`, stays stale, warned every `proofAfterDeferrals` stale passes; a parked repair holds its scan up
  to the wait plus one heartbeat.

---

## 14. Open weaknesses

### RP-W1. A declined ErrorTree mark does not order against the repair that deletes the held entry (CONFIRMED; permanent replica divergence)
Mechanism: the marker drops an ErrorTree mark whose `missedTimestamp` is not later than the held entry's, with no
write and no requirement (`MissedUpdateMarker.writeBatch`, `TKVLInsert.replaces`, `BatchOp.Put.appliesTo`). The held
entry then stands for both misses, and its completion gate (`StoreRepairer.repairUpdate`: `os.timestamp >=
minTimestamp`) only says the read is at least as new as the held miss's start. Two transactions on one object can
commit in the opposite order of their starts (Q6: a `RefcountUpdate` and a key-level `KeyValueUpdate` check no object
timestamp, `RequirementsChecker.checkRefcount`, `checkKVRequirements`). Nothing makes the repair's delete
(`deleteEntryIfUnchanged`, guarded only by the entry's `KeyRevision`) fail when a declined mark arrived after the
repair's read.
Trigger / scenario: store S misses T2 (start t2) on KeyValue object X; entry (S, X)@t2 is written. A repair pass
snapshots S and reads X at time r. Transaction T (start t1 < t2; it read X before T2 applied and stalled more than
`errorTimeout` before preparing, or its client's HLC lagged), writing key k1 under `KeyRevision`, prepares after r at
no read responder, commits, and S misses it (silent, refused admission, or voted abort on its stale k1). After 2 s
T's finalizer marks (S, X)@t1: the node holds t2 >= t1, so nothing is written. The repair's delete commits after that
read (its read-to-delete interval exceeded T's commit plus 2 s: proofs, a parked reconcile, a slow fence read, a delete
transaction retrying collisions). Wider variant: the pass at r deferred only some locked key k0, so the next passes are
narrowed to `only = {k0}` and never look at k1; T may then commit at any time until the narrowed pass completes, which
for a hot k0 is many scan periods. The same hole reaches UnackedCommits entries through the phase-A shortcut
(`Counts.recordCompleted` / `completedAt`): phase A records X complete at t2 from a read that predates T, and phase B
of the same scan treats T's UnackedCommits entry for X as complete. That matters only where S can forget T without
applying it (a recovered vote whose entry names another incarnation, which "leaves the entry for the repair scan",
`Frontend.unackedLookupComplete`).
Consequence: S's copy of X never receives T's update of k1, and no entry records it. Reads stay correct (quorum), but
X's redundancy is one store short until something else rewrites k1 at S, S misses a later update of X, or S is rebuilt.
For a cold object that is permanent.
Suggested fix: make a declined mark move the held entry's revision: write the held value again (or `max` of the two,
which equals the held one) instead of dropping the put, so a repair that read before the mark fails its
`KeyRevision` delete and its next pass, keyed by the new revision, reconciles the whole object. Record
`Counts.recordCompleted` only after the phase-A delete committed, so a rewritten entry also denies the phase-B
shortcut. Cost: one extra write per declined mark, rare since the queue merges by key. Test: StoreRepairerSuite with
`testingOnlyBeforeEntryDelete` holding the delete while a declined mark for an earlier-started, later-committed
transaction is written; the entry must survive and the next pass must install k1.
Effort: S   Priority: 2

### RP-W2. A stale pass resets the lock-deferral count, so a hot object can alternate and never reach a proof (PLAUSIBLE; liveness)
Mechanism: an unsettled object answers `ReconcileResult(Set(), false, stale = true)` before any locks are computed
(`Frontend.reconcile`), and `DeferralState.record` forgets the entry on any result without lock deferral, resetting the
proof count to 0. `recordStale` resets the stale count on any non-stale pass. Parking (`awaitSettle`, after 3 stale
passes) produces a settled reconcile, which on a hot object usually reports locks (count 1) and resets the stale count;
the next pass, not parked, meets a commit in flight again and resets the lock count.
Trigger / scenario: S is behind on a KeyValue object written continuously (a hot TKVL node, a counter): each pass meets
either a commit in flight at install (stale) or a key or object lock in the read (deferred). Sequence: stale, stale,
stale, parked -> locked (1), stale (0), stale, stale, parked -> locked (1), ... `proofAfterDeferrals` (10) consecutive
lock-deferred passes never occur.
Consequence: the entry is never removed while the object stays hot. Parked passes do install unlocked parts, so S's
copy converges except for parts locked at those instants, but the entry costs a full read and a reconcile at the
30 s floor indefinitely, and parts locked at every parked pass stay behind.
Suggested fix: do not reset the lock-deferral count on a stale pass, only on a complete or lock-free settled pass;
or, once parking has started for an entry, keep parking (do not reset `awaitSettle`) until a pass is complete. Test:
FrontendSuite/StoreRepairerSuite driving an object with a commit in flight on alternate passes and a lock on the rest;
the entry must reach a proof.
Effort: S   Priority: 4
