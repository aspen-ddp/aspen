# Missed-Update Repair: ErrorTree, UnackedCommits, Reconcile

Paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`. Code is cited as
`File.scala` plus the symbol. Thresholds follow [README.md](README.md): `w` width, `t` write threshold, `r` read
threshold. Related documents: [paxos-transactions.md](paxos-transactions.md),
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
| Q1 | For every IDA the read threshold `r` is a majority and `r <= t <= w` (`t` the write threshold, `w` the width), so `r + t > w`: any read set of `r` stores contains at least one commit voter of any committed transaction. | `common/ida/IDA.scala`; CLAUDE.md |
| Q2 | A store that voted commit holds the transaction's locks until it applies the outcome or the transaction resolves abort. A read reply reports them: locked keys (present keys locked, plus absent-key locks) and an object-level flag (exclusive object lock, content lock, range locks), each including a slot a recovered commit vote took over from another holder (`displacedLocks`). A write locks every key it writes, and an object-level write takes the exclusive lock. Shared revision locks are not reported (they never change the object). | `server/store/ObjectState.scala` `lockedKeys`, `objectLevelLocked`; `RequirementsLocker.lock`, `plannedLocks`; `Frontend.readForNetwork` |
| Q3 | The reader picks the revision held by at least `r` replying stores, whatever its timestamp (at most one revision can reach a majority). It returns once one does. Its read locks are the union over every store that replied with a state. Its timestamp is the maximum over the matching stores. | `client/internal/read/BaseObjectReader.scala` `getStoreStates`, `attemptRestore` |
| Q4 | A KeyValue key is restored from the stores holding a revision of it that `r` segments share. It is declared deleted only when no revision of it can still reach `r` and no store holds the lock of any segment's revision; otherwise the read waits (`NotRestorable`). | `client/internal/read/KeyValueObjectReader.scala` `resolve` |
| Q5 | A restored object with refcount 0 is `InvalidObject(provesDeletion = true)`. A read fails `InvalidObject` when more than `w - r` stores answered `ObjectNotFound`/`ObjectMismatch`, and `provesDeletion` is set when more than `w - t` did. A store that applied a deletion answers `ObjectNotFound`. | `BaseObjectReader.scala`; `client/ReadError.scala` `InvalidObject` |
| Q6 | Every apply sets the object's timestamp to the later of its current timestamp and the transaction's start (`later`), so object timestamps never decrease. Only revision-bearing requirements (`checkRevision`, a `KeyValueUpdate` with a required revision) check `state.timestamp > txTimestamp`. A `RefcountUpdate` and a key-level `KeyValueUpdate` do not, so two transactions on one object may commit in the opposite order of their starts. | `server/store/RequirementsApplyer.scala`; `RequirementsChecker.checkRevision`, `checkKVRequirements` |
| Q7 | A KeyValue update moves the object revision only when it has a required revision. | `RequirementsApplyer.scala` |
| Q8 | Stores refuse blind writes as `TransactionBuilder` does: a key Delete needs a `KeyRevision` on that key or a content lock; a key Insert a check on its key, a required revision or a content lock; a min/max/left/right change needs a required revision (except an allocation's initial content). So every key delete fails at a store lacking the key (and is marked there), and every object-level change moves the revision. | `RequirementsChecker.refuseBlindWrites`; `TransactionBuilder.refuseBlindWrites` |
| Q9 | A store holding recovered transactions (`Frontend.holding`: CRL still loading, a recovered Tx still loading its objects, a recovered Tx that does not relock waiting for every recovered commit vote to relock, or one deciding its vote) drops every client read, as a silent store would. A recovered commit vote retakes its locks before holding ends. | `Frontend.readObjectForNetwork`, `recoveredTransactionLoaded`; `Tx.completeLoad` |

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
| Meaning | The store cast a commit vote the commit may have relied on and had not acknowledged applying T when T was finalized (it sent no `TxCommitted`, or one with `keptForReplay`: a failed backend commit keeps T's CRL state for a reload to replay): it is owed T. A recovered, expiring or late commit vote applies T through the entry when the head names the store's own incarnation or none (every Tx that awaits a lookup holds its own commit vote); one naming another incarnation concludes T committed and applies nothing (`Frontend.unackedLookupComplete`; transaction-recovery.md). Then, or when the store has forgotten T, the entry is left for repair, whose reconcile installs the committed state. Chunk keys keep their first writer's value, so chunks of one entry may differ in incarnation; only the head is looked up. | `Frontend.unackedLookupComplete`; `Host.unackedLookup` |

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
| Insert an UnackedCommits entry | same batch | same, for each counted commit voter (every store when the voters are unknown) that sent no `TxCommitted`, or one with `keptForReplay`, and hosts an object T changes |
| Insert an ErrorTree entry at `HLCTimestamp.Zero` (or rewrite a held one unchanged) | a host, through `MissedUpdateMarker.mark`: a rebuild (`RebuildingStore.markUnsettled`, a restore whose read reported a write lock, `recordAcceptedLoss`) or a rebuilt store's inherited-growth scan (`StoreRepairer.inheritedGrowthScan`, 10.3) | see section 3, marks that name no transaction |
| Delete either kind | `StoreRepairer.deleteEntryIfUnchanged` only | after the object(s) are reconciled completely or proven deleted |
| Point lookup of an UnackedCommits head | `Host.unackedLookup` (`server/store/UnackedCommitLookup.scala`) | a recovered, expiring or late commit vote |
| Applied stubs | not repair's: each store writes and expires its own (`Frontend.expireStubs`; `server/transaction/CLAUDE.md`) | |

---

## 3. Entry production

Details are in [transaction-recovery.md](transaction-recovery.md); what repair relies on:

1. `TransactionBuilder` adds a `MissedUpdateFinalizationAction` to every transaction unless tracking is disabled
   (`disableMissedUpdateTracking`; `AspenClient.newRepairTransaction` for repair proofs; `limitTracking` for writes
   to a pool's own trees, which happens only for the bootstrap pool).
2. The marker action executes when T's finalizer is created, in parallel with T's other finalization actions
   (`RegisteredTransactionFinalizerFactory.Finalizer`). `cancel` (T finalized another way, whose finalization wrote
   the same marks) stops a mark not yet handed to the marker; a handed-off one is written regardless
   (`handedOffStores`).
3. `updateCommitErrors`: when every data store replied and no error names a non-lock-only object, the action completes
   with no marks. Otherwise `execute` schedules `markMissedUpdates` after `errorTimeout` (2 s); stores replying
   meanwhile are not marked.
4. `writeMissedUpdates` builds:
   - `MissedUpdate(store, objectId, type, storePointer, txd.startTimestamp)` for every object a silent store hosts,
     except lock-only objects (`TransactionDescription.lockOnlyObjects`), and for every object a store reported a
     commit error on;
   - `MissedUnackedCommit(store, txid, UnackedCommitEntry(start, incarnation, changedObjectsOfStore))` for every store
     that sent no `TxCommitted` or one with `keptForReplay` (`updateCommitErrors`), is a counted voter (or the voters
     are unknown) and hosts an object T changes.
   Uncounted voters and non-voters are repaired by their ErrorTree marks alone.
5. `MissedUpdateMarker.mark` queues per pool, at most one write outstanding per pool. The queue merges ErrorTree marks
   by (poolIndex, objectId), keeping the later `missedTimestamp`, and UnackedCommits entries by (poolIndex, txid).
   A batch holds at most 256 entries, each kind guaranteed half; a queued key keeps its place when merged. So one
   transaction's ErrorTree marks and UnackedCommits entries can land in different batches (written one after
   another, either kind first).
6. `writeBatch` is one `replaySafe` transaction over both trees, retried until it succeeds except on
   `TransactionCreationError` or `NodeSizeExceeded` (then the batch fails and the driver re-runs the finalizer with
   backoff, `TransactionDriver.startFinalizer`). Its puts are conditional (`TKVLInsert.replaces`): an ErrorTree mark
   replaces a held one only for a later `missedTimestamp`, and otherwise writes the held value again, which moves
   its revision; an UnackedCommits entry is written only where none is held (otherwise it is dropped from the batch
   with no write and no requirement). A replaced or new key carries a
   key requirement, so two markers racing on one key abort one batch, which retries.
7. The action completes only when the batch commits; T is not finalized (and its stores do not forget it) before.

Properties used later: every entry is written after T committed and at least 2 s after its finalizer started; an
ErrorTree entry's `missedTimestamp` never moves backward; every ErrorTree mark moves the entry's revision, even one
older than the held entry (12.4); an UnackedCommits entry is never rewritten in place, so its revision changes only
by delete and re-create.

Marks that name no transaction: a rebuild and a rebuilt store's inherited-growth scan record objects as missed at
`HLCTimestamp.Zero` (2.6). Such a mark never displaces a newer one (it is written as the held value again, which still
moves the entry's revision), `mayJoinTransaction(Zero)` is false at once, and the completion gate passes for any read.
The entry then says only "this store's copy may be behind a committed state", which a complete pass from a later read
satisfies by E1. The first two properties above hold for transaction marks only.

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
seen += 1; decode objectId, (missedTs, type, storePointer); entryKey = errorDeferralKey(key); deferral.observeEntry(entryKey, value.revision)
if target.mayJoinTransaction(store, missedTs): deferred += 1; return                    -- 5.4
alloc = pool.allocationTree.get(objectId)
listed:  done = repairListed(store, pointer(alloc), missedTs, entryKey)                 -- 6, 9.2
absent:  if age(value.timestamp) >= minErrorEntryAgeForDeletion (60 s):
             done = repairAbsent(store, entry's pointer, missedTs, entryKey, typeKnown)  -- 9.2
         else deferred += 1; return
if done: deferral.clear(entryKey)
         deleted = deleteIfStillOwned(errorTree, value.revision, key)                    -- 5.5
         if deleted: counts.recordCompleted(objectId, missedTs); repaired += 1
         else (fence lost): deferred += 1
else:    deferred += 1
```

`repairListed` turns `InvalidObject` and `ReadTimeout` into "not done" (or a deletion, 9.2); `repairAbsent` turns any
failure of its deciding read into "not done". Any other exception in the callback (a decode failure, a
`CorruptedObject` read, `StoreNotHosted`, a fail-stopped store, a failure of `repairAbsent`'s follow-up
`repairUpdate`, a delete that aborts on its revision or finds the key gone) leaves the entry, records nothing and is
logged at error by the walk.

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
object, so its UnackedCommits entry then costs only its delete. Phase A records an object only once the entry's
delete committed: a mark written between the walk's read of the entry and the delete (including one for an
earlier-started transaction that committed after phase A's object read, 12.4) moves the entry's revision and fails
that delete. A mark may also land after the delete: T's UnackedCommits entry and its ErrorTree marks can be in
different marker batches (section 3, step 5), so phase B can find T's entry while T's mark for an object is still
queued, and the shortcut can then complete the object for T from a phase-A read that preceded T's commit. That mark
still follows: every object of an UnackedCommits entry that the store did not apply also gets an ErrorTree mark from
the same finalization (a store that sent no TxCommitted is marked for every hosted object that is not lock-only, and a
changed object is never lock-only; a `keptForReplay` store reported every object it did not apply as a commit error,
and an object it applied owes nothing), T is not finalized before both are written, and the mark creates a fresh
entry that a later pass repairs from a read after T's commit. Deleting the UnackedCommits entry early costs nothing
else: S cannot join T any more (5.4), so no Tx of S looks it up.

### 5.4 Waiting until the store can no longer join the transaction

Both kinds of entry are left for a later pass while `RepairTarget.mayJoinTransaction(store, start)` holds: the entry's
transaction started at or above the store's forget floor (`StoreRouter.mayJoinTransaction` =
`!Frontend.belowForgetFloor(start)`). The floor (`Frontend.forgetFloorMillis`) is the latest of `now - horizon`
(`TransactionHorizon.Horizon`, 20 s), the store's load floor and the status cache's eviction watermark. The load floor
(`loadFloorMillis`) is the load plus `MaxStartLead`, or the join ceiling the store's previous life on this host ran
under when lower (`JoinCeiling.boundAtLoad`), and at least an arrival from another host plus
`RebuildingStore.AcceptorSinceMargin` (10 s); a load within that margin of the arrival uses the load plus the margin.
At or above the floor, a replayed prepare could still create a Tx for T at the store after the scan's
`holdsTransaction` check, apply T after the reconcile, and the entry would be deleted under that apply. Below it the
store refuses every prepare for a transaction it does not hold (`Frontend.receivePrepare`, `refusePrepare`), and a
store never rejoins a transaction it forgot. The one Tx created below the floor is a rebuilt store's muted Tx for a
transaction started before its acceptor (`acceptorSince`, `Frontend.createTx`): it takes no part and applies nothing
(`Tx.muted`), at most proving the commit to its driver, and it is forgotten once its objects load unless proven
committed (`Frontend.forgetMuted`); while it exists, `holdsTransaction` skips the UnackedCommits entry. The wait is at most the horizon plus `MaxStartLead` (2 s) plus clock skew (the margin is
shorter than the horizon). For an ErrorTree entry the check uses
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
  if allowProof: deferral.recordStale(entryKey, result)   -- warns every proofAfterDeferrals stale passes
```

`snapshotForRepair` gives `RepairSnapshot.of(state)` for a served state, `missing` for an object being deleted, and
otherwise loads it from the backend (`SnapshotRead`). A snapshot is a fresh copy each pass.

### 6.2 What the executor does at install

`Frontend.reconcile` refuses to write unless the object is settled:

```
unsettled = holding || commitInFlight(obj) || lockWaitingCommits.contains(obj) || adoptedDataSaves.contains(obj)
         || commitFailed(obj) || committing.contains(obj) || deleting.contains(obj)
         || another in-memory state of the object is the one served or referenced
if unsettled && snapshot.awaitSettle: park (8.2)
elif unsettled: stale, nothing written
else reconcileSettled
```

`commitInFlight` covers outstanding transaction commits, repair writes and committed transactions holding their
commit for their turn (`heldCommits`). `lockWaitingCommits` lists committed transactions waiting to apply in lock order
(`Tx.awaitLockHolders`) under every object they change, wherever the locks they wait for lie. `adoptedDataSaves` lists
transactions saving object data adopted from a late client prepare (`Tx.saveAdoptedData`), which a commit waits for,
until the save completes and that commit has applied or taken one of the holds above. A committed transaction
finalized before it applies here relies on the store's ErrorTree mark for its object, and a completed pass would
consume it: its later blind Insert of an absent key could then write back a key a later transaction deleted. `commitFailed` includes objects poisoned by `poisonObject`.

A rebuilt store also answers stale, before any of this, for a KeyValue object under its inherited-vote lock
(`Frontend.inheritedVoteLock`: every object until its first held-vote round completes, then the objects of inherited
transactions not yet released), and turns a complete result into a stale one for an object it
guards against growth unless the pass is whole-object and its snapshot was taken after the guard was set
(`RepairSnapshot.takenAt`), when the guard ends (`Frontend.inheritedGrowthRepaired`; 10.3,
cross-document-review.md 2.3). The entry so outlives the guard, which only a repair can end. A stale result defers
nothing by a lock, so the next pass is whole-object.

`reconcileSettled`, all in one executor task:

1. `current` = the store's copy now (`RepairSnapshot.of`, or `missing`).
2. `locks` = the read's reported locks, less the parts a proof covered (`snapshot.proven`), plus the store's own
   (`os.lockedKeys`, `os.objectLevelLocked`): a store that did not answer the read may hold locks of its own.
3. `plan = Reconcile.plan(snapshot, current, read, storeId, locks)`.
4. If the plan changes nothing, record it as an install when its result is complete (below) and return its result. If a KeyValue object holds
   commit votes' slice reservations (`ObjectState.sliceReservations`, transaction-requirements.md R16) and the plan
   changes it, measure the planned state as a vote is measured (`Frontend.repairFitsReservations`,
   `RequirementsChecker.checkKVSliceSize`: the planned slice plus every reservation, the count varint at the summed
   count). If it exceeds the KeyValue limit (`Frontend.maxKVSliceSize`, which the votes' apply re-checks use), write
   nothing and answer stale: a later pass installs once the votes have applied or been released. Otherwise mutate the
   in-memory state (key installs and deletes, min/max/left/right, re-encode, data, metadata), insert it in the object
   cache, retain it, add it to `committing` and count it in `txCommitsInFlight`, then `backend.repair(CommitState)`.
   Transactions on the object wait behind the repair write as behind a commit. On completion `repairWriteComplete`
   releases it and passes the object's turn on (`commitTurnFree`). A failed write restores the fields the plan changed
   to the loaded state the plan was built on (`PreRepairState`: metadata, data, min/max/left/right and the touched
   keys), keeping the same instance, since lock holders reference it and discarding it would fail their committed
   apply (`StaleObjectState`). That state was settled, so it is the backend's, and no transaction locked, checked or
   applied the object while the write was in flight. A repair of an object the store did not hold discards its state
   instead (the next use reloads the backend's). Either way the promise fails, so the entry stays; the object is not
   marked failed, since no transaction relied on the repaired state. While the write is outstanding, reads are served
   the repaired state from memory, a state a quorum read returned; a failed write leaves the store as stale as before
   the repair (12.2).

A repair write whose plan removes keys (`plan.deletes`) reaches the backend only once no CRL state the store holds of
another transaction still holds a blind Insert of one of them that the store applied
(`Frontend.awaitBlindInsertsDurable`, transaction-requirements.md 9.4): such a state is saved again without the
object's update first, or its conclusion made durable. A recovered commit vote therefore never writes back a key
repair removed. The object stays held in memory and counted in flight meanwhile, as for any repair write.

Every repair write records the object timestamp it installs (`Frontend.recordRepairInstall`, `repairInstalls`, kept
until it falls below the forget floor). So does every pass whose plan changes nothing and whose result is complete
(its read at least the entry's minimum timestamp, nothing deferred), at that minimum timestamp
(`RepairSnapshot.minTimestamp`: the newest merged miss's start, or the UnackedCommits transaction's start): such a
pass's entry delete can remove the store's mark for a deletion D of a key the store never held and the read lacks
too, and D's mark, merged into the entry or the entry itself, started no later than the entry's minimum timestamp.
An incomplete pass deletes no entry, and the bootstrap sweep (minimum timestamp zero) deletes none, so neither is
recorded. Every Tx on the object started before the recorded
timestamp, held now or created while the record lasts (`registerReplayHazards`), is told (`Tx.repairInstalled`), and
its apply of a blind Insert of a key absent here reports the object, whether or not it voted commit here
(`Tx.issueCommits`), since the installed or matched state may hold a deletion of the key this store never applied
(transaction-requirements.md 9.4). A committed Delete the store applied as a no-op over the absent key is recorded
apart (`Frontend.keyDeletions`), so a non-voter's blind Insert of a key also reports after a deletion of it applied
here. A Tx a no-op pass's record reports writes a mark carrying its own start, below the recorded timestamp, so the
next pass of that mark records no higher: on a hot KeyValue object under concurrent inserts the reports die out
rather than renew the entry, and an inserter started after the entry's newest miss reports nothing.

The recorded timestamp of a write is the installed object's: a pass narrowed to deferred keys (6.3) does not raise
it, so a deletion such a pass installs can be recorded below the deleting transaction D's start. That loses nothing:
the store holds the key at the snapshot's revision and the read lacks it, so the store has not applied D (a key
delete applied there would have removed it, and one it cannot apply fails, Q8), and D's mark of the store exists or
follows. Written before the whole-object pass that narrowed this one read the entry, it set that pass's
`minTimestamp` to at least D's start, and that pass, not stale, raised the store's timestamp to a read at least that
new. Written between the passes, it moved the entry's revision, and `DeferralState.observeEntry` made this pass
whole-object. Written after the entry's delete, it is a fresh entry, repaired whole-object from a read reflecting D,
which removes a key written back meanwhile. A narrowed pass that changes nothing needs no such argument: it records
the entry's minimum timestamp, the same for every pass of the entry, and D's start is at most that.

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
       else: the store's slice of read.data, with read.revision; read.timestamp when later than current's
result = (no keys, objectLevelDeferred = locks.objectLevel, stale = refcountStale || dataStale)
```

A read older than the store with a differing revision is stale, not caught up: the store may have missed a data
write and applied a later refcount update the read's responders have yet to apply. A DataObject's timestamp moves only
with its data: a pass that installs only the refcount leaves the store's timestamp below the read's. That only makes
`checkRevision` at this store more permissive than at its caught-up peers (a minority vote); the completion gate uses
the read's timestamp, not the store's.

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
read following each miss's commit. Two transactions on one object can commit in the opposite order of their starts
(Q6: a `RefcountUpdate` and a key-level `KeyValueUpdate` check no object timestamp), so a miss older than the held one
can commit after the read. Its mark is written after its commit and moves the entry's revision (section 3, step 6),
so the entry's delete fails (12.4) and the next pass, seeing the new revision (`DeferralState.observeEntry`),
reconciles the whole object from a later read.

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
| M6a | A plan that grows a KeyValue slice installs only if the planned slice plus the held votes' reservations fits the KeyValue limit (`Frontend.maxKVSliceSize`); otherwise stale | `Frontend.repairFitsReservations` | A counted commit voter's apply re-check failing on size (R16) after the install grew the slice under its reservation |
| M7 | Refcount only with a strictly higher `updateSerial`, or when the store has no copy | `Reconcile.planRefcount` | Lowering a refcount; un-deleting |
| M8 | DataObject data only when revisions differ, the read is not older than the store, and the store's revision still equals the snapshot's | `Reconcile.planData` | Installing an older revision over a newer one (revisions are transaction ids, so no ABA) |
| M9 | A copy is never created without its refcount, never while the object level is locked, never if the store deleted it after the snapshot | `Reconcile.plan`, `planData`, `planKeyValue` | Resurrecting a deleted slice |
| M10 | No repair write for an object being deleted on the store; `repairDelete` refused while the object is in use | `Frontend.reconcileForRepair`, `deleteObjectForRepair` | Writing after a deletion; a delete racing a read, commit or transaction |
| M11 | Skip an UnackedCommits entry while the store holds T (or is loading its CRL) | `Frontend.mayHoldTransaction` | Repair racing the Tx that applies T; deleting an entry a recovered vote still needs to look up |
| M12 | Leave entries while the store may still join their transaction | `RepairTarget.mayJoinTransaction` | A replay's Tx applying T after the reconcile and the entry being deleted under it |
| M13 | Compare-and-delete on the entry's revision; every mark rewrites the entry | `deleteEntryIfUnchanged`, `MissedUpdateMarker.writeBatch` | Dropping an entry a mark rewrote after the read, older marks included |
| M14 | Completion requires the read to be at least as new as the entry's timestamp | `repairUpdate` | Dropping an entry when the read cannot reflect the miss |
| M15 | Proof transactions for persistently locked parts | `RepairProofs` | Waiting forever on a hot lock |
| M16 | Parking a persistently stale repair until the object settles, until a complete pass; stale passes keep the lock-deferral count | `Frontend.parkRepair`, `DeferralState.record`/`recordStale` | Retrying forever an object always busy at install, or alternating busy and locked |
| M17 | Absence from the AllocationTree is not a deletion; only `InvalidObject.provesDeletion` drops a slice | `StoreRepairer.repairAbsent`, `repairListed` | Deleting the slice of a live object whose allocation is recorded late |
| M18 | Ownership fence; `StoreNotHosted`; `abandoned` | `OwnershipFence`; `StoreRouter` | A departed or superseded copy consuming entries |
| M19 | A failed backend write restores the pre-repair state (or discards a new copy) and keeps the entry | `Frontend.repairWriteComplete` | Dropping an entry when the install did not reach disk; memory left ahead of the backend |
| M20 | Proofs run untracked and replay-safe; entry deletes run tracked except in the bootstrap pool's own trees | `AspenClient.newRepairTransaction`; `limitTracking` | Entry feedback loops |

---

## 8. Deferral, parking and proofs

### 8.1 DeferralState

In memory, one per store, kept across scans by `RepairService` and dropped when the store leaves the repairable set
(`StoreRepairer.DeferralState`). Keys: `errorDeferralKey(key)` (prefix 0), `unackedDeferralKey(key,
objectId)` (prefix 1), `sweepDeferralKey(nodeId)` (prefix 2).

- `record(entryKey, result)` counts the passes since the entry's last complete pass that deferred something by a lock
  (`deferredKeys` non-empty or `objectLevelDeferred`). A complete pass forgets the entry and returns 0. A pass that is
  only stale returns 0 and keeps the count as it is: on a hot object stale and lock-deferred passes alternate, and
  only a complete pass shows the locks are gone. It sets `only` to None, so the next pass reconciles the whole
  object. When the pass was neither object-level deferred nor stale, `only` is set to the deferred keys, and the next
  pass reconciles only those. `afterProof` follows the same rules, keeping the count.
- `recordStale` counts stale passes separately. Only a complete pass resets it: a lock-deferred pass keeps it, so
  once parking (8.2) has started for an entry it continues until a complete pass. A parked pass that meets a lock is
  then followed by another parked pass, not by a stale one, and an object busy at every unparked install reaches a
  proof in about `ParkAfterStalePasses + proofAfterDeferrals` passes.
- `retain` (a completed scan) forgets entry keys the scan did not touch; sweep keys are kept.
  `retainSweep` (a complete sweep walk) prunes sweep keys. A host restart forgets everything; the next pass reconciles
  whole objects and restarts the counts. An ErrorTree deferral key is the entry's key alone, so its counts survive a
  rewrite of the entry: every mark rewrites it (section 3, step 6), and a store marked again between passes, as a
  commit error on a hot key marks it, still reaches a proof or parking. `observeEntry` records the revision each pass
  reads the entry at; when it has moved, it drops only the `only` narrowing, since the later mark may name a miss of
  keys the last pass did not defer, and the next pass reconciles the whole object. A proof is safe at any pass, so a
  count carried over a rewrite costs nothing in correctness.

### 8.2 Parking (persistently stale objects)

A proof cannot help a stale result (the store's own copy is busy or changed). After `ParkAfterStalePasses` (3) stale
passes since the entry's last complete pass, the repair sets `RepairSnapshot.awaitSettle`, and keeps setting it until a
complete pass (8.1). An unsettled store then parks the reconcile
(`Frontend.parkRepair`; at most one per object, a newer one replacing an older, which answers stale) instead of
answering stale. `commitTurnFree` runs it first, at the one moment the object is settled, before any waiting
transaction is told; the end of holding retries every parked repair too, as does the completion of an adopted-data
save (`Frontend.adoptedDataSaved`) for the objects it changes. It runs once, through the ordinary
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
| object level (either type) | one transaction: `RefcountUpdate(required = rc, new = rc)` and `SharedRevisionLock(read.revision)` | `RepairProofs.proveObjectLevel` |

Each completes true only on commit; an abort or error is false. A committed proof means a write threshold of stores
agreed with the read on that part at that moment and none held a conflicting lock (the requirements take the same
locks a real write would). Any other committed write of
that part needed a vote from a store in the proof's set, which would have collided or mismatched, so the read was
current on it at proof time. The scan then reconciles from the same read with `snapshot.proven` set: the read's locks
on proven parts are ignored, the store's own locks still defer, and the snapshot check still applies. An object's
object level is its refcount and its revision (a DataObject's data, a KeyValue object's min, max, left and right), and
neither requirement covers the other: a refcount change moves no revision, and a write that moves the revision leaves
the refcount as it was. One transaction requires both, so both are proven as of one moment. That fits the structural
rules: a lock beside an update of one object is refused, but a `RefcountUpdate` is not an update in that sense
(`TransactionBuilder`, `DescriptionValidator`). The `RefcountUpdate`'s X lock already collides with every write lock on
the object, so the revision lock need not be exclusive: the shared one adds only the revision check, and beside the
transaction's own X lock an exclusive one would exclude nothing more.

The revision lock compares the revision only, which suffices because every data write and every min/max/left/right
change moves the revision (Q8). Every requirement on a refcount-0 or missing object fails `MissingObject`
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
- `provesDeletion` needs more than `w - t` `ObjectNotFound`/`ObjectMismatch` replies, or a majority at refcount 0
  (Q5). A committed live object is on at least `t` stores, and `t >= r`. A commit voter that has not yet applied an
  allocation does not answer `ObjectNotFound`: the read parks there until the allocation applies or aborts
  (`Frontend.parkAllocationRead`, `allocationPending`), so a read inside an allocation's apply window proves nothing,
  and the 60 s gate only paces. Clock skew moves the gate by at most the skew plus `MaxStartLead`; safety does not rest
  on it.
- `repairDeletion` (`Frontend.deleteObjectForRepair`) is refused (`RepairDeleteDeferred`, counted deferred) while the
  store is holding, or the object has a pending backend read, a commit, a repair write or a deletion in flight, or a
  transaction references it. Otherwise it evicts the cache, counts the object in `deleting` (reads, snapshots and
  transactions get `ObjectNotFound`), waits for allocations to be durable (`awaitAllocationsDurable`), then
  `backend.repairDelete`: RocksDB deletes the key; FSIF deletes the extent only if its slot still holds that id. Both
  are idempotent. A stopped or closed store fails it. A `commitFailed` object is not refused: it is gone from the
  rest of the system.

### 9.3 Can a store that missed a delete resurrect the object?

By reads: no. The stores that missed the deletion are at most `w - t`, fewer than `r` (Q1), so they cannot form a
matching set, and more than `w - r` deleters make the read `InvalidObject`. A deleter that has only voted holds its
object lock, which defers repair and makes readers wait.

By repair: a read of a deleted object fails before reconcile runs. A store with no copy gets one only from a read with
a live refcount and no object-level lock (M9). If the store deleted the object after the snapshot, the plan is stale,
and `reconcileForRepair` answers stale for an object being deleted.

Window: a store with no copy can install a pre-deletion read if the deletion commits after the read and had not been
voted at any responder at read time. The deletion cannot apply at that store (no object: `MissingObject`), so the
store reports an error, is marked, and the next pass proves the deletion and drops the slice. The slice is a zombie
until then; reads ignore it (Q4/Q5).

### 9.4 Can a missed key delete resurrect a key?

On reads: no (Q4). On repair: the store is made to match the read key by key; a key the read lacks is deleted at the
store if its revision still equals the snapshot's. Every key delete carries a `KeyRevision` or a content lock (Q8),
each of which fails at a store lacking the key, so a delete never applies there as an invisible
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

### 10.2 No read-driven repair

There is no read-driven repair path: a read never writes a store's copy, and every repair goes through Reconcile.

### 10.3 A rebuilt store's inherited-growth scan and guards

A rebuilt store guards each KeyValue object it may hold without a committed growth its lost incarnation voted for
(`Frontend` "Inherited growth", cross-document-review.md 2.3): a growing update of it votes abort
(`InheritedGrowthLock`) until a repair shows the store holds that growth. Its first held-vote round, past the
inherited-votes gate, finds the objects through `StoreRepairer.inheritedGrowthScan` (the host's
`HeldVoteQuery.growthScan`):

1. Mark, at `HLCTimestamp.Zero` (section 3), every object of the transactions the peers reported that the
   AllocationTree lists in the store's pool and that is a KeyValue object, and wait for the marks to commit. Each such
   object then has an entry whatever its transaction's outcome. An unlisted one is deleted, or an allocation the
   rebuild's pending sweep covers.
2. Walk the store's range of both trees and list every KeyValue object with an entry: an ErrorTree entry typed
   KeyValue (a typeless one is left out: repair acts on it only for a proven deletion, so no repair would end its
   guard), or an object of an UnackedCommits entry. An entry that cannot be decoded, a failed read or a failed mark
   fails the scan; the round is retried.

The listed objects are guarded at once (`growthScanAnswered`). An inherited transaction released later guards those
of its objects the scan listed. Until the first round's scan answers, every object is under the inherited-vote lock,
so every KeyValue reconcile answers stale (6.2) and no KeyValue entry is consumed by a reconcile while the guards are
being found. A guard ends only at a complete whole-object reconcile from a snapshot the store numbered after the guard
was set (`repairSnapshotSeq`, `RepairSnapshot.takenAt`, `inheritedGrowthRepaired`): the repair read began after that
snapshot, so after the transaction committed, and by E1 reflects it. Any other complete result for a guarded object is
turned stale, so its entry stays for a pass that can end the guard. The release does not wait for the entry's
completion gate (6.4), which concerns the entry's own misses, not the guard's transaction. The inherited-votes file
stays while any guard does: a reload repeats the round and finds what is still guarded through the entries the guards
kept. A guarded object whose deletion persists here, a committed refcount-zero update or a missed deletion repaired
by `repairDeletion` (`Frontend.deleteObjectForRepair`), is never reconciled again (its reads fail), so its guard ends
at the deletion instead (`inheritedGrowthDeleted`): a deleted object cannot grow, and the store can hold no copy of it
lacking T's update. It is also dropped from what a later release guards, and a deletion that persists before the first
round's scan answers is kept out of what the scan lists, since the entry the scan saw may be the one the deletion
consumed. The work is three tests per deletion on a store never rebuilt.

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
| E15 | Progress: an entry that is not marked again, whose object is readable, not locked at every attempt, and settles within `RepairSettleWait`, is removed within a bounded number of passes. | M15, M16; limits in 13 and 14 |

Assumptions: (A1) a store reports the locks of every transaction it voted commit on and has not applied, for every
locking requirement; (A2) it reports them for the whole interval between vote and apply, across restarts (Q9);
(A3) object timestamps never decrease (Q6); (A4) a store completes a backend commit before it forgets the
transaction. That every merged miss of a deleted ErrorTree entry committed before the read that completed it is not
assumed: a later commit's mark moves the entry's revision and fails the delete (5.3, 12.4).

---

## 12. Failure scenarios

### 12.1 Repair racing with a commit C at store S

| When | Event | Outcome |
|------|-------|---------|
| Before snapshot | C applied at S | Snapshot includes it; the read reflects it (E1). Safe. |
| Snapshot to install | C applies at S | S's key/revision/refcount/object level differs from the snapshot: stale. Safe. |
| Snapshot to install | S voted C (holds locks), not yet applied | S's own locks defer those parts. Safe. |
| During the read | C's voter among the responders applied it or holds its locks | Read reflects C or reports its locks. Safe. |
| Read to install | C commits elsewhere; S has not voted | The install writes the read's state, a state the system had. C's apply at S, or C's mark for S, follows; its requirements then check the repaired state. The mark, even one older than the held entry, moves the entry's revision, so the entry survives (12.4). Safe. |
| At install | A commit's backend write outstanding, or a commit held for its turn or waiting in lock order | Unsettled: stale or parked. Safe. |
| After install, before the backend completes | A new transaction arrives | It waits behind the repair write. Safe. |
| After install | Backend write fails | Pre-repair state restored in memory (a new copy discarded); promise fails; entry kept. Safe. |

### 12.2 Repair reading from lagging peers

Stale peers are a minority of matching responders. A read returns only when `r` responders share a revision (Q3); for
any committed C at least one of them voted commit (Q1) and either shows C or reports its locks (deferring C's parts).
S itself may answer the read; a matching set made only of stores that missed C would need `r` stores outside C's
`t` voters, which do not exist. A read waits while no revision has a majority among the replies. Reads from a store
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
- I older (its `missedTimestamp` is not later than the held one): I writes the held value again, under the key's
  revision as I read it, so it orders against D exactly as a newer mark does (one of the cases above). A mark for an
  earlier-started transaction that committed after D's read therefore fails D, though the entry's timestamp does not
  change; the next pass reads after that commit.

### 12.5 Entry for an object that has since been deleted

Listed: the repair read proves the deletion and the slice is dropped, or the entry waits for a proof. Absent: after
60 s a read proves it and the slice is dropped. An object already below `r` live copies through some other loss reads
`InvalidObject`; if more than `w - t` stores lack it, that proves "deletion" and the remaining slices are dropped,
but the object was unreadable already.

### 12.6 Entry whose store has been rebuilt or transferred

- Rebuilding: not repairable; entries stay. After promotion the next scans reconcile them (mostly no-ops, since the
  rebuild copied current state) and remove them, together with the entries the rebuild itself wrote at
  `HLCTimestamp.Zero` (2.6). Entries are keyed by pool index, so a rebuilt store inherits them harmlessly. A rebuilt
  store's KeyValue reconciles answer stale until its first held-vote round has found its growth guards, and a guarded
  object's until a pass that ends its guard (10.3).
- Transferred or adopted: repairable only on the host the pool entry names, on the named device (fence). An in-flight
  `snapshot`/`reconcile`/`repairDelete` fails with `StoreNotHosted`. The entry stays for the new host.

### 12.7 Trees unavailable (metadata pool degraded)

- A tree walk or `allocationTree.get` hangs: the 90 s deadline cancels the scan. A scan that had seen no entry backs
  off; one cut off while working through entries stays at the floor and keeps its DeferralState.
- A tree walk fails (a node read fails other than by a join race): the scan fails, is logged, and is paced as a clean
  scan even if it had seen entries: the interval doubles toward the cap.
- A marker write cannot commit: `retryUntilSuccessful`; the transaction stays unfinalized and its stores keep its
  state (the `FINALIZATION STALLED` alarm after 60 s). A permanent error fails the batch and the finalizer is re-run.
  No entry is lost.
- An entry delete fails: one more pass. Policy or limits unreadable: last good or defaults.

### 12.8 An object whose read never completes

A read of an object below its read threshold, or of a key that stays `NotRestorable`, or of an allocation held
indefinitely (ExpiryTally rule G; the store parks the `AllocationPending` re-read, `Frontend.parkAllocationRead`),
gets no answer. The scan's object reads are timed (`client.withReadTimeout(StoreRepairer.ObjectReadTimeout)`, 15 s):
such a read fails with `ReadTimeout` and stops its driver (`ReadManager.read`, `BaseReadDriver.timeOut`; the one
deadline also covers resolving the pool's IDA when it is not cached, `BaseAspenClient.resolveIdaWithin`), and the
entry is deferred as for an `InvalidObject` short of a deletion proof (`repairListed`; `repairAbsent` defers on any
failure of its deciding read that proves nothing; a timeout of its follow-up `repairUpdate` fails the callback,
logged, with the same effect). The scan goes on to
the store's later entries, so each stuck object costs one bounded wait per scan and leaves no read behind. More than
`ScanDeadline / ObjectReadTimeout` (6) stuck objects at the head of a store's range still use up its scan before the
later entries; the tree reads (`foreachInRange`, `allocationTree.get`) are not timed, and a metadata pool short of
its read threshold stalls the scan until the deadline.

### 12.9 More scenarios

| Scenario | Outcome |
|----------|---------|
| Two scans of one store overlap (a timed-out scan's in-flight entry still running) | They share the store's DeferralState; both are guarded by M3/M4/M13; at most one reconcile parks per object. Duplicated work only. |
| The AllocationTree's pointer differs from the entry's | An ErrorTree entry reads with the listed pointer, and with its own only when absent. An UnackedCommits entry always reads with its own pointer; the tree only decides listed or absent. |
| A malformed entry | The callback fails; logged at error each pass; the entry stays forever. |
| An entry for a pool index beyond the IDA width | Stale every pass; stays forever. Width changes do not exist. |
| FSIF has no free extent at install | `backend.repair` fails; the entry stays. |
| A proof aborts every time | `prove` returns nothing; the pass's result stands; each later pass retries every deferred proof. Bounded by one object's deferred set per pass; a lock held at every attempt starves the part. |
| Clock skew | The age gate uses this host's HLC against the marker's; `mayJoinTransaction` uses this host's wall clock against T's start. The reconcile guards use revisions and transaction ids. |

---

## 13. Accepted limitations

- **Bootstrap tree sweep staleness window** (Backlog.txt): a bootstrap store whose scans come back clean backs off to
  the 15 min cap, so a missed untracked write to the bootstrap pool's own trees can stay unhealed that long.
- **Head-of-line blocking on many unreadable objects** (12.8): each costs one `ObjectReadTimeout` per scan, so more
  than six at the head of a store's range use up its scan; tree reads are untimed and bounded only by the deadline.
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

(none)
