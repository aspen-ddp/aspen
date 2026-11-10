# Missed-Update Repair: ErrorTree, UnackedCommits, Reconcile

Status: derived from the code at commit c3ad5493 (branch main); refreshed 2026-11-09 against `0d08e3c6` (branch
`pin_refactor`, replay pins: `docs/superpowers/specs/2026-11-09-replay-pins-design.md`). The TracelessCommits tree is now
the UnackedCommits tree: same schema and scan, but entries are written for every committed transaction's
unacknowledged counted voters (2.3, 3), an entry means "this store is owed T", and late commits read it (5.3). Line
numbers in passages the refresh did not touch are for c3ad5493. Every mechanism cites `path:line` relative to
`src/main/scala/org/aspen_ddp/aspen/` unless the path starts with `docs/`. Claims in `CLAUDE.md` and the two design
documents (`docs/superpowers/specs/2026-09-02-continual-repair-design.md`,
`docs/superpowers/specs/2026-11-08-traceless-commits-design.md`) were checked against the code; where the code
differs, the code is described.

Related documents (written in parallel): [paxos-transactions.md](paxos-transactions.md),
[crash-recovery-log.md](crash-recovery-log.md), [transaction-requirements.md](transaction-requirements.md),
[transaction-recovery.md](transaction-recovery.md) (how entries get written by finalization and how a recovered commit
vote consumes a UnackedCommits entry), [rebuild-process.md](rebuild-process.md) (whole-store rebuild),
[systemic-loss-protection.md](systemic-loss-protection.md).

Contents: 1 Scope and model, 2 Tree schemas, 3 Entry production, 4 Scheduling, 5 Scan state machine, 6 Reconcile,
7 Race prevention, 8 Deferral and proofs, 9 Deletes, 10 Opportunistic rebuild, 11 Invariants, 12 Failure scenarios,
13 Suspected flaws and open questions.

---

## 1. Scope and model

### 1.1 What repair is for

A transaction commits when a write threshold of the stores hosting its objects voted commit. The remaining stores may
not hear about the outcome (message loss, host down, slow). Such a store keeps an older copy of the object: this is a
*missed update*. Repair brings the store's copy back into agreement with what a read of the rest of the system
returns. It is a background, eventually-consistent process; reads and writes never wait for it. Reads are correct
without it because they restore from a read threshold of stores. Repair restores redundancy.

Repair never decides what a committed state is. It only copies the state a normal read returns, onto one store, under
guards that make the copy safe against concurrent transactions at that store.

### 1.2 Facts the design relies on (verified)

| # | Fact | Code |
|---|------|------|
| Q1 | Replication: read (consistent restore) threshold = `width/2+1`; write threshold is in `[width/2+1, width]`. | `common/ida/IDA.scala:203,243` |
| Q2 | Reed-Solomon and RandomAccessReedSolomon: restore threshold `k >= width/2+1` (a majority), write threshold in `[k, width]`. | `common/ida/IDA.scala:411-415, 591-595` |
| Q3 | Hence `readThreshold + writeThreshold > width` for every IDA: any read set intersects the commit-vote set of any committed transaction in at least one store. | derived from Q1, Q2 |
| Q4 | A store that voted commit holds the transaction's locks until the outcome is applied (or the transaction resolves abort). Locks are reported in read responses: locked keys, absent-key locks, and an object-level flag. | `server/store/ObjectState.scala:59-68`; `server/store/Frontend.scala:1011` (`NetworkRead` reply), `client/internal/read/BaseObjectReader.scala:104` |
| Q5 | The reader restores from stores sharing the "most recent" revision, ranked by (max timestamp, count). It returns only when at least `threshold` stores match. Read locks are the union over all responders. | `client/internal/read/BaseObjectReader.scala:139-207` |
| Q6 | A KeyValue key is restored from the stores holding its highest-timestamp revision. If fewer than `threshold` match and the missing ones could still match, or any responder holds a lock of that revision, the read waits (`NotRestorable`). Otherwise the key is deleted. | `client/internal/read/KeyValueObjectReader.scala:109-151` |
| Q7 | A refcount is the one with the highest `updateSerial` among matching responders. A restored object with `count == 0` is `InvalidObject`, so a client read of a deleted object fails. A store that applied a deletion answers `ObjectNotFound`. | `client/internal/read/BaseObjectReader.scala:176,197`; `server/store/Frontend.scala:1010-1020` |
| Q8 | Applying a transaction sets the object's timestamp to the transaction's start timestamp unconditionally. Only requirements that name a revision check `state.timestamp > txTimestamp`; refcount updates and KeyValue updates without a required revision do not. So an object's timestamp is *not* monotone in commit order. | `server/store/RequirementsApplyer.scala:105-145`; `server/store/RequirementsChecker.scala:220-226,245`, `:213` |
| Q9 | A KeyValue update moves the object revision only when it has a required revision. A blind insert or delete changes only the key's revision and the object's timestamp. | `server/store/RequirementsApplyer.scala:105-111` |

Q3 is the reason an unlocked part of a read reflects every committed transaction. Section 11 states that as E1.

### 1.3 Terms

- **Store**: `StoreId(poolId, poolIndex)`. **Slice**: the store's IDA-encoded part of an object.
- **Entry**: a record in the ErrorTree or UnackedCommits tree that says "this store may be behind on this object /
  this transaction".
- **Snapshot**: the store's copy of an object recorded on its executor before the repair reads (`RepairSnapshot`,
  `server/store/Reconcile.scala:22`).
- **Install**: writing the repaired state into the store's memory and backend (`Frontend.reconcileSettled`).
- **Deferred**: a part of the object the read reports locked, or the store itself holds locked. It is left alone.
- **Stale** (result flag): something the repair would have changed differs from the snapshot, or the read could not be
  used. It is left for a later pass. Not the same as deferred.

---

## 2. Tree schemas

Both trees are ordinary Tiered Key Value Lists (`TieredKeyValueList`, `TreeKind.KeyValueObjects`, `ByteArrayKeyOrdering`
= unsigned lexicographic, `common/objects/KeyOrdering.scala:25-29`). Each is rooted in a key of the pool's
*pool state* KeyValue object.

### 2.1 Where they live

| Item | Value | Code |
|------|-------|------|
| Pool state keys | `ConfigKey=0, ErrorTreeKey=1, AllocationTreeKey=2, AllocationStrategyKey=3, AllocationStrategyConfigKey=4, RepairPolicyKey=5, UnackedCommitsTreeKey=6` | `common/metadata/StoragePoolState.scala:14-20` |
| Pool state object | Always in the bootstrap pool | CLAUDE.md; `client/internal/BaseAspenClient.scala:~527` |
| Tree handles | `pool.errorTree`, `pool.allocationTree`, `pool.unackedCommitsTree` over `KVObjectRootManager(client, key, poolStatePointer)` | `client/internal/pool/SimpleStoragePool.scala:34-41` |
| Tree node placement, ordinary pool | Root and later nodes allocated from the pool's *metadata group* (default `aspen-default-storage-pool-metadata`) | `client/internal/BaseAspenClient.scala:~485-505` |
| Tree node placement, metadata pool | Nodes in the bootstrap pool | `server/store/Bootstrap.scala:~187-199`; `BaseAspenClient.scala:~485` |
| Tree node placement, bootstrap pool | In the bootstrap pool itself | `server/store/Bootstrap.scala:~167-186` |
| One tree per pool | Not per store. A store's entries are the key range `[poolIndex, poolIndex+1)`. | `server/repair/StoreRepairer.scala:124-127` |

So repair of pool P reads P's trees from metadata pool M (an unavailable or degraded M stops repair of P, see 12.9).
Repair of M's stores reads M's trees from the bootstrap pool. Repair of the bootstrap pool reads trees from itself.

### 2.2 ErrorTree

| Field | Format | Code |
|-------|--------|------|
| Key | 17 bytes: `poolIndex` (1 byte), `objectId.msb` (8, big-endian), `objectId.lsb` (8, big-endian). One entry per (store, object). | `client/internal/transaction/MissedUpdateMarker.scala:259-266`; decode `server/repair/StoreRepairer.scala:376-382` |
| Value | `missedTimestamp` (8 bytes, big-endian `HLCTimestamp.asLong`) then the object's `storePointer` bytes (possibly empty). | `client/internal/transaction/MissedUpdateFinalizationAction.scala:203-217` |
| Meaning of `missedTimestamp` | The *start timestamp of the newest missed transaction known to the writer*. A store that applies that transaction sets the object's timestamp to this value (Q8), so a read at least this new "has caught up past the miss". The key's own tree timestamp belongs to the transaction that wrote the entry and is not used for this. | same, `:196-202` |
| Entry's tree-level metadata | The `ValueState.revision` (txid of the writing transaction) and `.timestamp` (its start timestamp). Repair uses the revision as a compare-and-delete token and the timestamp as the entry's age. | `server/repair/StoreRepairer.scala:164,184` |
| Empty `storePointer` | Means "delete by ObjectId alone" (RocksDB). FSIF needs the pointer. | `Backend.scala:74`; `FsifBackend.scala:1463-1466` |

The `storePointer` in the value is *the only surviving copy* once an object is deleted, because deletion removes the
AllocationTree entry (`client/internal/allocation/DeletionFinalizationAction.scala:28`).

### 2.3 UnackedCommits tree

| Field | Format | Code |
|-------|--------|------|
| Key | 17 bytes: `poolIndex` then `transactionId` (msb, lsb big-endian). One entry per (store, transaction). | `client/internal/transaction/UnackedCommitEntry.scala:40-46` |
| Value | `timestamp` (8) = the transaction's start timestamp; `count` (4); then `count` x (`length` (4), `ObjectPointer` bytes). The pointers keep their store pointers and are only the objects of this store with a traceless change. | `UnackedCommitEntry.scala:16-37` |
| Which objects | Since the refresh: every object the transaction changes on that store, traced or traceless. Before, only `RefcountUpdate`s and revision-less `KeyValueUpdate`s with `deletesKeys`. | `MissedUpdateFinalizationAction.scala:98-106` |
| Meaning | The store cast a commit vote the commit may have relied on and had not acknowledged applying T when T was finalized: it is owed T. A recovered commit vote, and any commit voter receiving a late commit, applies T only if its entry exists. | `Tx.scala:158-160,920-932,997-1041` |

### 2.4 AllocationTree (dependency, not repaired)

Key = `objectId.toBytes`, value = `ObjectPointer.toArray`. Written by `AllocationFinalizationAction`
(`client/internal/allocation/AllocationFinalizationAction.scala:28`), deleted by `DeletionFinalizationAction`
(`:28`). Repair uses it as the *oracle for "is this object live"*: present means live (its pointer is used for the
read), absent means "deleted, or its allocation has not been recorded yet" (section 9).

### 2.5 Policy records

| Record | Key | Default | Code |
|--------|-----|---------|------|
| `RepairPolicy` | pool state `RepairPolicyKey` (5) | floor 30 s, cap 15 min, `minErrorEntryAgeForDeletion` 60 s, `proofAfterDeferrals` 10; absent key or non-positive field gives the default | `server/repair/RepairPolicy.scala:77-127` |
| `HostRepairLimits` | host state `RepairLimitsKey` | `maxConcurrentStoreScans = 4` | `server/repair/HostRepairLimits.scala:201-225` |

### 2.6 Who writes, who deletes

| Operation | By whom | When |
|-----------|---------|------|
| Insert ErrorTree entry | `MissedUpdateMarker.writeBatch`, called from `MissedUpdateFinalizationAction.markMissedUpdates` on the client that finalized the transaction | `errorTimeout` (2 s) after the finalizer started, for each store that did not report a clean commit |
| Insert UnackedCommits entry | same | same, for each counted commit voter (every store if the voters are unknown) that sent no `TxCommitted` and hosts an object T changes |
| Overwrite ErrorTree entry | same path; plain insert, no `requireDoesNotExist`, no timestamp comparison | a later miss of the same (store, object) |
| Delete either kind | `StoreRepairer` only (`deleteEntryIfUnchanged`, `deleteEntry`). No other code deletes entries (grep of `errorTree`/`unackedCommitsTree` over the tree). | after the object is verified or found deleted |
| Read | `StoreRepairer.scan`; `Host.unackedLookup` (`Host.scala:~210`, point lookup for a recovered commit vote) | |

---

## 3. Entry production (summary; details in transaction-recovery.md)

`MissedUpdateFinalizationAction` is added to every transaction unless tracking is disabled
(`client/internal/transaction/TransactionBuilder.scala:55,122,234`). When the finalizer runs it:

1. `updateCommitErrors(commitErrors)`: if every data store of the transaction replied and no error names a
   non-lock-only object, it completes with no marks (`MissedUpdateFinalizationAction.scala:45-54`).
2. Otherwise `execute()` schedules `markMissedUpdates` after `errorTimeout` = 2 s (`:56-60`, `:138`). Stores that
   reply in that window are not marked.
3. `markMissedUpdates` (`:69-107`) builds
   - `MissedUpdate(store, objectId, txd.startTimestamp, ptr.storePointer)` for every store with no reply (all hosted
     objects except lock-only ones, `:76`) and for every object a store reported an error on (`:82`);
   - `MissedUnackedCommit(store, txid, UnackedCommitEntry(txd.startTimestamp, changedObjectsOfStore))` for each
     store that sent no `TxCommitted`, is in the voter set (or the set is unknown), and hosts an object T changes,
     traced or not (`:98-106` at `0d08e3c6`). A store outside a known voter set gets no entry.
   Since the refresh the marker runs only after every other finalization action of T completed
   (`RegisteredTransactionFinalizerFactory.scala:37-42`, Ruling 8).
   Lock-only objects (`RevisionLock`/`SharedRevisionLock` without a change) are never marked (`:36-39`).
4. `MissedUpdateMarker.mark` (`MissedUpdateMarker.scala:302-309`) queues entries per pool. Per pool at most one write
   is outstanding. Entries arriving meanwhile wait; the queue is a `LinkedHashMap` keyed by (poolIndex, objectId) for
   updates and (poolIndex, txid) for traceless entries, so a store missing one object repeatedly produces one write of
   the newest miss (`:316-327`, higher `missedTimestamp` wins *within the queue only*). Batches hold at most 256 entries
   of both kinds with a half-and-half guarantee (`:335-353`).
5. `writeBatch` (`:369-397`) is one transaction on the pool's ErrorTree and UnackedCommits trees, retried until
   success except `UnknownStoragePool`/`TransactionCreationError` (`StopRetrying`, `:377-386`). If the transaction
   references the pool the entry is about, missed-update tracking is disabled for it
   (`MissedUpdateFinalizationAction.limitTracking`, `:130-132`), preventing a circular write. The FA completes only
   after the batch commits; the transaction cannot be forgotten before that (`:101-106`).

Important properties used later: entries are written *after* the transaction committed and *at least 2 s later*;
the entry value's timestamp is the miss's start timestamp, not its commit time; an entry write can overwrite an
existing entry with an older `missedTimestamp` (flaw F10).

---

## 4. Scheduling and limits

### 4.1 RepairService

One `RepairService` per host (`server/Host.scala:304`). Its `sweep()` is a non-concurrent polling task with a fixed
30 s tick (`RepairService.scala:48-49,219`; `StallAfter` 6 h).

```
sweep():
  now = clock()
  refreshLimits()                       -- 15 s deadline, keeps last good on failure          (:148-158)
  repairable = target.repairableStoreIds   -- served, not offline/rebuilding/transferring-out  (host/HostStoreView.scala:15)
  due = selectDue(repairable, now)      -- drop states of departed stores; admit new ones      (:166-177)
  refreshPolicies(pools of due)         -- per-pool RepairPolicy, 15 s deadline, last good     (:133-146)
  runBoundedParallel(due, maxConcurrentStoreScans)(scanOne)
scanOne(store):
  result = withDeadline(90 s)(StoreRepairer.scan(store, policy, deferralOf(store)))            (:182-210)
  on ScanTimedOut: discard store's DeferralState, found = false
  on any other failure: found = false
  scanStates[store] = ScanState.advance(prev, now, found, policy)
```

A newly seen store is admitted at the floor with a random due time inside the first floor interval and is *not due on
the tick that admits it* (`ScanState.scala:30-32`; `RepairService.scala:172-176`). Because the tick is 30 s, a floor
below 30 s is rounded up (`:214-219`).

### 4.2 ScanState (pacing)

`ScanState(nextDue, currentInterval)` (`ScanState.scala:12`). After a scan:

| Outcome | Next interval |
|---------|---------------|
| Any entry seen in either tree (repaired, deferred or skipped), `foundEntries = seen>0 || unackedSeen>0` | floor |
| Clean scan | `min(2*current, cap)`, at least floor |
| Failed or timed-out scan (`foundEntries=false`) | treated as clean: doubles toward cap (`ScanState.scala:34-38`) |

Next due time is `now + interval` jittered by +/-25 % (`:16-24`).

### 4.3 Bounds

| Bound | Value | Why | Code |
|-------|-------|-----|------|
| Concurrent store scans per host | 4 default | host load | `HostRepairLimits.scala:208` |
| Scan deadline | 90 s | A read of an unavailable object retries forever; without a bound one sick pool wedges the single-flight sweep | `RepairService.scala:235,91-102` |
| Metadata read deadline | 15 s | policy and limits are advisory | `:241` |
| Per-entry work | strictly sequential within a store (one `foreachInRange` callback at a time) | bounded memory, no self-collision | `client/tkvl/TierEngine.scala:135-145` |
| Failure of one entry | logged, walk continues | `TierEngine.visit` | `TierEngine.scala:139-142` |

### 4.4 Store departure

`abandoned(storeId)` is checked at the top of every entry callback. If the store is no longer repairable the callback
returns without touching the entry (`StoreRepairer.scala:142-147,152,213`). `RepairTarget.snapshot/reconcile/
repairDelete` fail the promise with `StoreNotHosted` for a store that has left (`StoreRouter.scala:184-204`), so the
caller never treats a departed store as repaired and the entry survives for the new host's sweep.

---

## 5. Scan state machine

### 5.1 Per store

```
scan(store, policy, deferral):
  pool  = client.getStoragePool(store.poolId)
  range = [Key(poolIndex), Key(poolIndex+1))                                  -- StoreRepairer.scala:124-127
  pool.errorTree.foreachInRange(range, repairErrorEntry)                      -- phase A
  pool.unackedCommitsTree.foreachInRange(range, repairUnackedEntry)       -- phase B (after A completes)
  deferral.retain(keys touched this scan)      -- forget deferral state of entries not seen (gone or rewritten)
  return ScanResult(seen, repaired, deferred, unackedSeen)
```

The tree walk hands each callback the entries of the node it just read, so the `ValueState.revision` the callback holds
may be older than the live value. That is why deletions are guarded by revision (5.4). Entries added during the walk
may or may not be visited. The walk ends at the end of the range.

### 5.2 ErrorTree entry (phase A), `repairErrorEntry` (`StoreRepairer.scala:149-200`)

```
repairErrorEntry(node, key, value):
  if not repairable(store): return
  seen += 1
  objectId = decode(key); (missedTs, storePointer) = decodeErrorEntry(value)
  entryKey = errorDeferralKey(key, value.revision)
  alloc = allocationTree.get(objectId)
  if alloc present:
      r = repairUpdate(store, pointer(alloc), missedTs, entryKey, policy, deferral)       -- section 6
      if r.complete: deferral.clear(entryKey); deleteEntryIfUnchanged(value.revision)      -- guarded
      else           deferred += 1                                                        -- entry stays
  else (absent):
      if now - value.timestamp >= minErrorEntryAgeForDeletion (60 s):
          repairDelete(store, objectId, storePointer); deleteEntry(key)                    -- UNguarded
      else deferred += 1                                                                   -- too young
```

Any exception inside the callback (including the read failing with `InvalidObject` for an object whose refcount is 0
but whose allocation entry is not yet removed) leaves the entry untouched; the walk logs and continues.

### 5.3 UnackedCommits entry (phase B), `repairUnackedEntry` (`:210-252`)

```
repairUnackedEntry(node, key, value):
  txid = decode(key)
  if target.holdsTransaction(store, txid):            -- live Tx, or store still loading CRL state (awaitingCrl)
      deferred += 1; return                           -- Tx applies T itself (Tx.needsUnackedCheck, Tx.lateCommit)
  entry = decode(value)
  allComplete = true
  for ptr in entry.objects (sequentially):
      alloc = allocationTree.get(ptr.id)
      if absent and entry age >= minAge:  repairDelete(store, ptr.id, ptr.storePointer)
      elif absent:                        allComplete = false
      else:  allComplete &= repairUpdate(store, ptr, entry.timestamp, unackedDeferralKey(key, ptr.id), ...).complete
  if allComplete: clear deferral keys; deleteEntryIfUnchanged(value.revision)
  else deferred += 1
```

`holdsTransaction` is `Frontend.mayHoldTransaction = awaitingCrl || transactions.contains(txid)`, a volatile read from
the scan thread (`server/store/Frontend.scala:96`, `Store.scala:114`, `StoreRouter.scala:279`). Since the refresh a
recovered vote that finds its entry applies T but stays live until `TxFinalized` (Ruling 9), so the scan keeps skipping;
a Tx that applied a late commit is forgotten at once. The entry remains either way, and the next scan reconciles (a
no-op) and removes it (`StoreRepairer.scala:228-268`). Until then the entry still says the store is owed T, so a replay
of T that resolves late is applied there again (Ruling 17; transaction-recovery R-N1).

### 5.4 Entry deletion and its guard

- `deleteEntryIfUnchanged(entryRevision, node, key)`: a repair transaction (`client.newRepairTransaction()`, tracking
  disabled, `client/AspenClient.scala:~89-100`) that `node.delete(key)` with requirement
  `KeyValueUpdate.KeyRevision(key, entryRevision)` (`StoreRepairer.scala:351-361`). If any writer replaced the entry
  since the scan read it, the revision differs, the requirement fails, the transaction aborts, and the next pass sees
  the new entry. The comment at `:345-350` states this is "the point".
- `deleteEntry(node, key)` (`:337-343`): same transaction but no requirement. Used only after `repairDelete`.
- Delete and a concurrent entry insert both lock the key (key requirement vs insert), so they collide at the stores:
  exactly one orders first. If the delete commits first, the later insert creates a fresh entry. If the insert commits
  first, the delete fails its `KeyRevision` requirement (`RequirementsChecker.scala:~278-285`).

---

## 6. Reconcile: the per-object algorithm

`repairUpdate` (`StoreRepairer.scala:261-299`) runs the three steps; `Reconcile.plan` (`server/store/Reconcile.scala:103`)
is the pure decision.

### 6.1 Steps

```
repairUpdate(store, ptr, minTimestamp, entryKey, policy, deferral):
  snap  = target.snapshot(store, ptr)           -- on the store's executor (Frontend.snapshotForRepair, :787)
  snap  = snap.copy(only = deferral.deferredKeys(entryKey))
  os    = client.read(ptr)                      -- FULL read: only full reads report locks (:275)
  first = reconcile(snap, os)                   -- on the store's executor (Frontend.reconcileForRepair, :799)
          result.stale |= (os.timestamp < minTimestamp)           -- :270
  passes = deferral.record(entryKey, first)
  if passes < policy.proofAfterDeferrals: return first
  proven = prove(os, first)                     -- RepairProofs (section 8)
  if proven nonEmpty: second = reconcile(snap.copy(proven = proven), os); deferral.afterProof(entryKey, second); return second
  return first
```

`snapshotForRepair` returns `RepairSnapshot.of(state)` for a servable state, `missing` for an object being deleted, or
loads from the backend (`SnapshotRead`) (`Frontend.scala:787-795,1030,1053`). The snapshot is taken on the executor, so
it is consistent with all in-flight work of that store.

### 6.2 What the executor does at install

`Frontend.reconcile` (`:888-906`) refuses to write unless the store is quiescent for that object:

```
unsettled = holding                          -- recovering transactions from the CRL (awaitingCrl, recovering, undecided)
         || commitInFlight(obj) || commitFailed(obj) || committing.contains(obj) || deleting.contains(obj)
         || the loaded copy is not the one served or referenced (another in-memory state is the real one)
if unsettled: return ReconcileResult(Set(), false, stale = true)      -- nothing written
else reconcileSettled
```

`reconcileSettled` (`:908-964`):

1. `current` = the store's copy now (`RepairSnapshot.of`, or `missing`).
2. `locks` = the read's reported locks (minus parts `snapshot.proven` covers) UNION the store's own locks
   (`os.lockedKeys`, `os.objectLevelLocked`). The store adds its own because a store that did not answer the read may
   hold locks of its own, and a recovering store's locks may not all be taken yet.
3. `plan = Reconcile.plan(snapshot, current, read, storeId, locks)`.
4. If the plan changes nothing: return its result. Else mutate the in-memory state (`kvs.content`, `min/max/left/right`,
   `data`, `metadata`), insert it in the object cache, **retain it as an in-flight commit** (`committing`,
   `txCommitsInFlight`), call `backend.repair(CommitState)` and complete the promise when the backend write finishes.
   Transactions on the object wait behind the repair write exactly as behind a transaction commit. On a failed write
   the state is discarded and reloaded (`repairWriteComplete`, `:968-985`); the promise fails so the entry stays.

All of step 1-4 runs in one task on the store's executor, so nothing else touches the object between `current` and
the in-memory mutation.

### 6.3 `Reconcile.plan`

Early exits (`Reconcile.scala:105-109`): wrong pool: stale. `current` missing but `snapshot` was not (deleted since):
stale ("installing the read would resurrect the slice"). Partial or metadata-only reads, or a poolIndex outside the
IDA width: stale (`:111-118`). Final rule: a copy is created only together with its refcount; otherwise stale
(`:119-123`).

**Refcount** (any object type) (`planRefcount`, `:128-136`):

```
wanted = current.isMissing || read.refcount.updateSerial > current.refcount.updateSerial
if !wanted: no change
elif current.refcount != snapshot.refcount: stale (store changed it since the snapshot)
else: install read.refcount
```

The comparison is on `updateSerial` only (so a count can go down when the serial is higher, e.g. a decrement). Repair
never installs a refcount whose serial is not strictly higher unless the store has no copy.

**DataObject** (`planData`, `:138-163`):

```
if current.isMissing && locks.objectLevel: defer object level (cannot create the copy without its refcount)
refcount = if locks.objectLevel: none else planRefcount
data = if locks.objectLevel || read.revision == current.revision || read.timestamp < current.timestamp:  none, NOT stale
       elif current.revision != snapshot.revision:  none, STALE
       else: Some(read.ida.encode(read.data)(poolIndex))     -- install data, revision, and timestamp = max
result = ReconcileResult(deferredKeys = {}, objectLevelDeferred = locks.objectLevel, stale = refcountStale || dataStale)
```

The silent "read.timestamp < current.timestamp" skip is justified only by a timestamp argument: the read reflects every
transaction the store applied if it is "no older" than the store (`Reconcile.scala:147-150`). Section 13 (F3, F13)
examines that assumption.

**KeyValueObject** (`planKeyValue`, `:165-216`):

```
if current.isMissing && locks.objectLevel: defer everything (keys = locks.keys, objectLevel = true)
keys = (current.keys ∪ read.keys ∪ snapshot.keys), intersected with snapshot.only if set
for key in keys, key not in locks.keys:
    have = current.keyRevisions(key); want = read.contents(key)
    if have != want.revision:
        if have != snapshot.keyRevisions(key): stale = true           -- changed since snapshot; leave it
        elif want is Some: install (IDA slice of the read's value, its revision, its timestamp)
        else: delete key                                              -- the read lacks it
wholeObject = snapshot.only.isEmpty
objectLevelLeft = locks.objectLevel || !wholeObject
refcount = if objectLevelLeft: none else planRefcount
objectLevel+revision (min,max,left,right,revision):
    if objectLevelLeft: none
    elif equal to read and revision equal and have a copy: none
    elif current.objectLevel != snapshot.objectLevel || current.revision != snapshot.revision: stale
    else: install read's (slices of left/right) and read.revision
timestamp = if wholeObject && read.timestamp > current.timestamp: read.timestamp
result.deferredKeys = locks.keys (∩ only); objectLevelDeferred = locks.objectLevel && wholeObject
```

Notes: keys the read reports locked are never installed or deleted; keys deferred stay in `deferredKeys`; the object
timestamp is advanced even when keys are deferred (F-note in 13.4).

### 6.4 Result and completion

`ReconcileResult.complete = deferredKeys.isEmpty && !objectLevelDeferred && !stale` (`Reconcile.scala:63-64`). Only a
complete result lets the caller delete the entry. Additionally `repairUpdate` marks the result stale if the read's
timestamp is older than the entry's `missedTimestamp` (`StoreRepairer.scala:270`).

---

## 7. Race prevention: every mechanism, and what it prevents

| # | Mechanism | Code | Prevents |
|---|-----------|------|----------|
| M1 | **Quorum read plus lock reporting.** A committed transaction was voted commit by a write-threshold of stores; any read-threshold set contains one voter, which has applied the transaction (read shows it) or holds its locks (read reports them). | Q3, Q4; `Reconcile.scala:89-97` | Installing a state older than a committed transaction, as long as the part is not reported locked |
| M2 | **Snapshot before read.** Snapshot is taken, then the read is issued, then install compares. | `StoreRepairer.scala:263-280` | Any change the store applied after the snapshot is detected at install |
| M3 | **Install only what still matches the snapshot.** Per key revision; object revision; refcount; min/max/left/right. A mismatch gives `stale`. | `Reconcile.scala:128-136,179-189,198-210,152-157` | Overwriting a newer committed change, including one the read did not yet see |
| M4 | **Install on the executor in one task; refuse while the object has any commit in flight** (`commitInFlight`, `commitFailed`, `committing`, `deleting`, recovery `holding`, state not the served one). | `Frontend.scala:888-906` | Repair write interleaving with a transaction's backend commit (backend writes of one object are unordered) |
| M5 | **Repair write is itself counted as an in-flight commit**: retained, `committing`, `txCommitsInFlight`. | `Frontend.scala:947-962` | A transaction reading the pre-repair state from the backend while the repair write is outstanding; transactions on the object wait behind it |
| M6 | **Store's own locks defer parts as read locks do.** | `Frontend.scala:915-916` | A store that did not answer the read, or a recovering store, being repaired over a part its own locked transaction is about to write |
| M7 | **Refcount: install only with a strictly higher `updateSerial`**, or when the store has no copy. | `Reconcile.scala:128-136` | Lowering a refcount, un-deleting |
| M8 | **DataObject: install data only when revisions differ, the read's timestamp is not older than the store's, and the store's revision still equals the snapshot's.** | `Reconcile.scala:151-157` | Installing an older revision over a newer one; ABA is excluded because revisions are transaction ids |
| M9 | **A copy is never created without its refcount; never created while the object-level part is locked; never "re-created" if the store deleted it since the snapshot.** | `Reconcile.scala:107-109,119-123,140-142,167-168` | Resurrecting a slice of a deleted object |
| M10 | **No repair write for an object being deleted on the store**, and `repairDelete` evicts the cache first. | `Frontend.scala:803-805,822-828` | Writing after the deletion |
| M11 | **Skip a UnackedCommits entry while the store holds the transaction live (or is loading CRL state).** | `StoreRepairer.scala:219-222`; `Frontend.scala:96` | Two writers (the Tx applying T and repair) racing, and repair acting before a recovered commit vote has had its chance to apply T |
| M12 | **Entry deletion compare-and-delete on the entry's revision.** | `StoreRepairer.scala:351-361` | Dropping an entry a newer miss has rewritten |
| M13 | **Completion requires the read to be at least as new as the entry's `missedTimestamp`.** | `StoreRepairer.scala:270` | Dropping an entry when the read does not yet include the missed transaction (belt and braces for M1) |
| M14 | **Proof transactions** make a persistently locked part safe to reconcile (section 8). | `RepairProofs.scala` | Waiting forever on a hot key |
| M15 | **Allocation-absence needs an entry older than 60 s** before treating the object as deleted. | `StoreRepairer.scala:399-402` | Deleting the store's slice (and the entry) of an object whose allocation has not been recorded |
| M16 | **Store left the host: promises fail with `StoreNotHosted`; callbacks check `abandoned`.** | `StoreRouter.scala:184-204`; `StoreRepairer.scala:142` | Dropping entries for a store now served elsewhere |
| M17 | **Backend write failure keeps the entry.** | `Frontend.scala:968-985` | Dropping an entry when the install did not reach disk |
| M18 | **Repair transactions have missed-update tracking disabled.** | `AspenClient.scala:~89-100`; `RepairProofs.scala:276` | Entry feedback loops |

The race analysis at each step is in section 12.1.

---

## 8. Deferral and proofs

### 8.1 DeferralState

In-memory, per store, kept across scans (`StoreRepairer.scala:45-92`). Keyed by `errorDeferralKey(key, entry revision)`
(prefix 0) or `unackedDeferralKey(entryKey, objectId)` (prefix 1). `record(entryKey, result)` counts consecutive
passes that deferred something *by a lock* (`deferredKeys` non-empty or `objectLevelDeferred`). A pass that defers
nothing by a lock (complete, or only `stale`) removes the entry and returns 0. `only` is set when the previous pass
deferred only keys; the next pass then reconciles only those keys and leaves refcount/object level/timestamp alone.
`retain` drops state for entries not seen in a completed scan. A host restart, or a `ScanTimedOut`, forgets everything
(`RepairService.scala:198-200`); the next pass reconciles the whole object and restarts the count.

### 8.2 When a proof runs

`passes >= proofAfterDeferrals` (default 10, about 5 minutes at the 30 s floor). `prove(read, result)` (`:304-329`)
runs, one at a time (parallel proofs of one object would collide):

| Deferred part | Proof | Code |
|---------------|-------|------|
| each deferred key (KeyValue only) | `update(ptr, None, None, [KeyRevision(key, readRevision)] or [DoesNotExist(key)], Nil)` | `RepairProofs.scala:254-259` |
| object level, KeyValue | `RefcountUpdate(required=rc, new=rc)` then `lockRevision(ptr, read.revision)` | `:265-273`; `StoreRepairer.scala:317-319` |
| object level, DataObject | `RefcountUpdate(required=rc, new=rc)` | `StoreRepairer.scala:320-321` |

All use `client.newRepairTransaction()` (tracking off) and complete `true` only on commit; an abort or any error is
`false` and logged at debug/info.

A committed proof means a write-threshold of stores agreed with the read on that part *at that moment* and none held a
conflicting lock (the proof's requirements take the same locks as a real write). Combined with Q3 (any other committed
write needed a vote from a store in the proof's set, which would have collided or mismatched), the read was current for
that part at proof time. The scan then re-runs `reconcile(snap.copy(proven = ...), os)`: the read's locks on proven
parts are ignored, but the store's own locks still defer (`Frontend.scala:913-916`), and the snapshot check still
applies. If any proof of a group fails, the object-level group is entirely unproven (`StoreRepairer.scala:326-327`).

### 8.3 What a proof cannot do

- It cannot help a `stale` outcome (`DeferralState` comment, `:36-43`): `stale` means the store's own copy is busy or
  moved. Nothing escalates a persistently `stale` object (F8).
- It proves revision equality, not equality of min/max/left/right (F11).

---

## 9. Deletes

### 9.1 How a deletion reaches a store

An object is deleted by a transaction setting its refcount to 0 (`RefcountUpdate`, traceless) with a
`DeletionFinalizationAction` that removes the AllocationTree entry afterwards. A store that applies it removes the
object from its backend, and answers reads `ObjectNotFound` (Q7). A store that missed it keeps the object with a
positive refcount.

### 9.2 How a missed delete is repaired

1. The finalizer records the miss: an ErrorTree entry (store never replied, or replied with an error) or a
   UnackedCommits entry (replied, but no `TxCommitted`), both carrying the `storePointer`.
2. Scan: the entry is found. `allocationTree.get(objectId)`:
   - **present** (DeletionFinalizationAction not yet run): `client.read` fails with `InvalidObject` (refcount 0,
     Q7). The callback fails and logs, the entry stays. The next pass retries.
   - **absent**: if the entry is at least 60 s old, `repairDelete(store, objectId, storePointer)`
     (`Frontend.deleteObjectForRepair`, `:822-828`) evicts the cache and calls `backend.repairDelete`: RocksDB deletes
     the key; FSIF deletes the extent only if its index slot still holds that object id
     (`FsifBackend.scala:1459-1476`). Then the entry is deleted *unguarded* (`StoreRepairer.scala:190-191`).
3. Younger than 60 s: deferred; retried each pass.

### 9.3 Can a store that missed a delete resurrect the object?

By reads: no. A read needs `threshold` matching responders; deleters answer `ObjectNotFound`. With `errors > width -
threshold` the read is `InvalidObject` (`BaseObjectReader.scala:119-132`). A stale store can contribute at most
`width - writeThreshold` Right responses, fewer than `threshold` (Q3). Otherwise the read waits. A deleter that has only
voted holds its object-level lock, which defers repair.

By repair (the store itself is behind):
- The refcount of the read is the highest serial among matching responders (Q7). A read of a deleted object fails
  before reconcile runs.
- A store with no copy never receives one unless the read has a live refcount and no object-level lock
  (`Reconcile.scala:119-123,140-142,167-168`).
- **Window**: a store with no copy, whose snapshot is "missing", can install a *pre-deletion* read if the deletion
  commits after the read and has not yet been voted at any responder at read time, while the store itself does not hold
  the object. The deletion transaction cannot apply at that store (no object, `MissingObject` error), so the store
  replies with an error, gets an ErrorTree entry for the object and is cleaned up by the missed-delete path above
  (S: after the allocation entry is gone and 60 s old, `repairDelete`). The slice is a zombie for at least that
  long, and the cleanup depends on the marker having been written (see F5/F14).
- If the store deleted the object after the snapshot, `current.isMissing && !snapshot.isMissing` gives stale
  (`Reconcile.scala:107-109`), and `Frontend.reconcileForRepair` returns stale for `deleting` objects (`:803-805`).

### 9.4 Can a missed key delete resurrect a key?

On reads: no, by Q6. A key present only at stores that missed the delete is dropped (all responders in, fewer than
`threshold` match, none locked) or waits (some responders could still make it restorable).

On repair, the store is made to match the read key by key (`Reconcile.scala:179-189`); a key the read lacks is deleted
at the store provided the store's key revision still equals the snapshot's. The reverse hazard is in F4: a key the
read has (taken before the delete), that the store lacks, and a delete that the store applies as a no-op between
snapshot and install; the install then re-creates the key at that store only. It cannot reappear in reads (Q6) but it
is a zombie slice and can make reads of the object depend on all stores (fewer than `threshold` match, others "could
still match").

KeyValue deletes with a required revision change the revision and are detected by M3. Only blind key deletes
(`deletesKeys`, no required revision), the traceless kind, can be silent.

---

## 10. Opportunistic rebuild

Opportunistic rebuild is a second, *read-driven* repair path that bypasses the ErrorTree. It is not guarded by
snapshots.

### 10.1 Flow

1. A completed full read (not `isPartial`) leaves `rereadCandidates` = stores whose last response was behind the
   restored state: a revision other than the winning revision, or (KeyValue) a store holding a lower-revision segment
   for some key (`BaseObjectReader.scala:42,160`; `KeyValueObjectReader.scala:121-124`).
2. `BaseReadDriver.receiveReadResponse` sends an `OpportunisticRebuild(toStore, pointer, revision, refcount, timestamp,
   data = os.getRebuildDataForStore(store))` message to each such store, once per read (`BaseReadDriver.scala:139-146,
   243-246`). `getRebuildDataForStore` encodes the whole object (all keys, min/max/left/right) as that store's slices
   (`client/ObjectState.scala:~119-139`).
3. The read driver also calls `opportunisticRebuildManager.markRepairNeeded(os, repairNeeded)`
   (`BaseReadDriver.scala:231-236`). `SimpleOpportunisticRebuildManager` keeps it for 10 s (`expireAfterWrite`, max 5000
   entries) (`SimpleOpportunisticRebuildManager.scala:15-22`). When the same client later builds a transaction touching
   that object, `TransactionBuilder.addUpdate` attaches a `PreTransactionOpportunisticRebuild(objectId, Metadata,
   data)` for each behind store to that store's `TxPrepare` (`TransactionBuilder.scala:132-153`), built from the
   *client object cache's* current state (`SimpleOpportunisticRebuildManager.scala:24-39`).
4. At the store: `Frontend.readObjectForOpportunisticRebuild` (`:766`) loads the object if needed, then
   `opportunisticRebuild` (`:867-881`):
   ```
   if os.revision == op.revision && !commitInFlight(obj):
        refcount = higher serial of (op.refcount, os.refcount)
        os.metadata = Metadata(op.revision, refcount, op.timestamp); os.data = op.data
        commit(os, ..., ordered = true)        -- counted as an in-flight commit
   ```
   An object not present on the store is not rebuilt (`:766-777`, and the backend not-found case ignores it,
   `:1051`). A different revision is ignored. So opportunistic rebuild *only repairs the same revision* (a KeyValue
   object whose keys differ, or a refcount).
5. In a prepare, `Tx.completeLoad` applies `preTxRebuilds` the same way when `os.revision == r.requiredMetadata.revision
   && !os.reloadRequired`, before the transaction determines its disposition (`server/transaction/Tx.scala:381-391`).
   It calls `frontend.commit(os, cs, txid)` with the defaults (not `ordered`).

### 10.2 Properties

- It is best effort and unrecorded: no entry is written or removed. It never completes an ErrorTree entry; the scan
  still reconciles.
- It makes the *same-revision* guard its only race protection (plus `commitInFlight` for the first path). It does not
  compare timestamps, key revisions, or locks, and does not check that the client's data is not older than the
  store's current state (flaws F1, F2).
- `os.data` is replaced but `os.kvState` is not (flaw F1). `Reconcile`'s own install does update `kvs.content` and
  re-encodes (`Frontend.scala:927-940`).

### 10.3 Not used by `RepairService`

`StoreRepairer` never calls it. The repair read itself is a normal read with opportunistic rebuild enabled; the read
driver of the repair's `client.read` may therefore *also* send rebuild messages to the very store being repaired,
concurrently with the snapshot/read/install sequence. The snapshot check at install still detects a change but cannot
tell an opportunistic rebuild apart from a transaction (both are "changes since snapshot"), so the result is stale
rather than wrong, except for the issues in F1/F2.

---

## 11. Invariants

Numbered for review; the code that enforces each is in section 7. "Holds" means by the code read here.

| ID | Invariant | Status |
|----|-----------|--------|
| E1 | An unlocked part of a read reflects every transaction committed before the read began. (Quorum intersection Q3 + lock reporting Q4; recovered commit votes retake their locks, and a loading store drops reads.) | Holds (needs the bounds of Q1/Q2; a voter that lost its vote state is covered in [crash-recovery-log.md](crash-recovery-log.md)) |
| E2 | Repair writes to a store only on its executor, only when the object has no other write in flight, and the write is itself an in-flight write. | Holds (M4, M5) |
| E3 | Repair never installs a part of an object that differs from the snapshot taken before the read. | Holds, with the exception of changes that leave no trace in the compared fields (F4) |
| E4 | Repair never installs a part the read or the store reports locked. | Holds (M6, `Reconcile.scala:179`) |
| E5 | Repair never lowers a refcount's `updateSerial`; it never installs a refcount while an object-level lock is reported. | Holds (M7) |
| E6 | Repair never installs an older DataObject revision over a newer one the store has applied: data requires `current.revision == snapshot.revision`, `read.revision != current.revision`, `read.timestamp >= current.timestamp`. | Holds if object timestamps order revisions: true for DataObjects, which check `timestamp > txTimestamp` on every revision-bearing update (Q8) |
| E7 | Repair never creates a copy of an object without installing its refcount, never while the object-level part is locked, never if the store deleted it after the snapshot. | Holds (M9) |
| E8 | An ErrorTree/UnackedCommits entry is removed only if (a) the object was reconciled completely (`complete`), or (b) the object was absent from the AllocationTree for an entry at least `minErrorEntryAgeForDeletion` old and `repairDelete` succeeded. | Holds in the code; (b) is a heuristic (F5) |
| E9 | An entry is removed only if it has not been rewritten since the scan read it (ErrorTree/UnackedCommits via `deleteEntryIfUnchanged`). | Holds except branch (b) of E8, which uses unguarded `deleteEntry` (F5) |
| E10 | An entry whose repair fails (read fails, promise fails, store left, backend write fails, `stale`, any deferral) stays. | Holds |
| E11 | A UnackedCommits entry is not acted on while the store holds that transaction live or is loading CRL state. | Holds (M11) |
| E12 | Repair transactions never create new entries. | Holds for proofs and entry deletions; the cost is F7 |
| E13 | A complete result implies the read was not older than the entry's `missedTimestamp`. | Holds, but the converse (an in-sync store always completes) does not (F3) |
| E14 | Missing/failed metadata, policy, or limits never stops a scan: last-good or defaults are used. | Holds (`RepairService.scala:133-158`) |
| E15 | Progress: for each entry whose object is reconcilable (no hot lock, no persistent stale), the scan removes it within a bounded number of passes. | Does not hold in general (F3, F8, F9) |

Assumptions the invariants rest on, to be checked by the reviewer: (A1) the lock set a store reports includes every
transaction that has voted commit and not yet applied, for every locking requirement; (A2) a store that holds a
transaction's locks reports them for the whole interval between vote and apply, across restarts; (A3) object timestamps
of DataObjects are monotone with the revision chain; (A4) a store completes a backend commit before it forgets the
transaction.

---

## 12. Failure scenarios

For each: outcome, and why it is safe, or "NOT SAFE" with the finding in section 13.

### 12.1 Repair racing with a commit

| Step | Event | Outcome |
|------|-------|---------|
| Before snapshot | Commit applied at S before the snapshot | The snapshot includes it; the read (taken after) reflects it by E1; nothing to change on that part. Safe. |
| Between snapshot and read | Commit C applies at S | Install sees S's key/revision/refcount != snapshot: stale for that part; entry kept. Safe. |
| Between snapshot and read | C commits on others, not yet applied at S, S voted (S holds C's locks) | Install adds S's own locks (`Frontend.scala:915`) and defers those parts. Safe. |
| During the read | C commits; C's voter among the read set applies or holds locks | Read reflects C or reports its locks (E1). Safe. |
| During the read | C commits entirely **after** the read's responders answered, and S applies C before install | Detected by M3 for revisions/keys/refcount/data. **Not detected** for a no-op effect of C at S (a delete of a key S lacks): F4. |
| Between read and install | C commits on others, S has not voted | The install writes the read's state, which is older than C. S's version after install is still older than quorum; C's miss is marked for S (S has no vote, or the commit decision reaches S later and S applies it on top of the repair: its requirements now compare against the repaired state). The ErrorTree entry for C (marked by C's finalizer) or C's apply at S keeps the system convergent. Safe, as the installed state is a state the system once had, and E3 covers S's own changes. |
| Between read and install | C is voted at S (locks taken) after the snapshot | `unsettled` (commit in flight only after apply) does not cover a held lock; install uses `os.lockedKeys` (`Frontend.scala:915-916`) so the keys C locks are deferred; for object-level locks `objectLevelLocked` defers. Safe. |
| At install | A commit's backend write is outstanding | `unsettled` -> stale, nothing written (M4). Safe. |
| After install, before backend completes | A new transaction arrives for the object | It waits behind the repair write (M5). Safe. |
| After install | Backend write fails | State discarded and reloaded from backend; promise fails, entry kept (M17). Safe. |
| Install of data object | Newer revision applied at S after snapshot | `current.revision != snapshot.revision`: stale. Safe. |
| Install of a refcount | S's refcount changed since snapshot | stale. Safe. |

### 12.2 Repair reading from lagging peers (could it install a stale revision?)

The repair read is an ordinary read; stale peers can only be a minority of matching responders.
- A read returns when `>= threshold` responders match the most recent revision (ranked by max timestamp, then count).
  For any committed transaction C at least one responder voted commit (Q3); it either shows C (and then the stale
  responders are not in the matching set, or are fewer than threshold) or reports C's locks (then C's parts are
  deferred and, with `locks.objectLevel`, the data/refcount/object level too).
- A lagging S itself can be in the read set. If S and `threshold-1` other stale stores are the *only* responders, they
  can only be a matching set if C has no voter among them, contradicting Q3.
- **Residual risks.** (1) Ranking is by timestamp before count (`BaseObjectReader.scala:146-153`): a refcount update
  that bumped a stale store's timestamp can make the stale revision rank first; the read then simply fails to reach
  `threshold` matching and waits for more responders; a store that is down can make that wait indefinite (F12,
  availability only). (2) E1 depends on A1 and A2. (3) Reads from a store that is itself Rebuilding or has lost its
  CRL state are the domain of [rebuild-process.md](rebuild-process.md) and
  [crash-recovery-log.md](crash-recovery-log.md).
- Per key: `resolve` takes the highest-timestamp revision among segments and requires `threshold` matches or declares the
  key deleted only if no responder could still match and none holds a lock of that revision (`KeyValueObjectReader.scala:
  135-147`). Keys with blind-write timestamps out of order (Q8) are ranked by timestamp, not commit order: the highest
  timestamp is the highest *start* timestamp, which may be an older commit. This is a read-path issue shared with
  all reads (F12), not specific to repair.

### 12.3 Crash mid-repair

| Crash point | State after restart | Safe? |
|-------------|---------------------|-------|
| Before install | Nothing changed; entry present | Yes; DeferralState lost, the next pass reconciles the whole object again |
| After in-memory mutation, before backend completes | The host crashed: memory lost; backend has the old state (or the new, atomically by the backend's commit) | Yes. The repair write is a normal backend commit, and the entry was not deleted. |
| After backend write, before entry delete | Next pass finds the store in sync; reconcile is a no-op; entry deleted | Yes (idempotent) |
| During `repairDelete` | The delete is idempotent (`Backend.scala:80`); entry still present | Yes |
| After `repairDelete`, before `deleteEntry` | Next pass: allocation still absent, repair delete again, then delete the entry | Yes |
| During entry delete transaction | Transaction either commits or not; the delete is guarded by the revision | Yes |
| Marker crashes after the transaction committed but before the entry is written | The finalization action is re-run by recovery; entry written later. If the store's repair already happened earlier the entry is found as in sync. | Yes. Entries are only recorded by at-least-once finalization actions. |
| Host owning the store crashes, store adopted by another host | `StoreNotHosted` failures or the old host's scan dies; the new host's service has fresh `ScanState` and `DeferralState` | Yes. Entries are the only durable state. |

### 12.4 Entry deleted while a new miss for the same object is written

Case analysis on the order of the entry-delete transaction D (requires `KeyRevision(key, Rv)`) and the marker insert
I (blind insert of the same key):
- I commits before D reads the key: the scan's `value.revision` is older, D fails its requirement, the new entry is
  repaired on the next pass.
- D commits before I: I creates a new entry (key absent). A fresh entry always results; D has deleted only the old one.
- D and I concurrent: both lock the key; one aborts and retries (the marker retries until success; the delete is
  best-effort, one more pass).
- The repair that D concluded read a state after the *old* miss. The *new* miss (transaction C2) was committed before
  I was written (I follows the finalizer by >= 2 s) and so it is covered by E1 for any read started after C2 committed.
  A read started before C2 committed cannot make D's precondition `complete` with respect to C2's entry since C2's
  entry is independent. So the interleavings are safe.
- **Exception:** the unguarded delete `deleteEntry` after `repairDelete` (`StoreRepairer.scala:191`) and the
  marker's `missedTimestamp` regression on overwrite (F10), see section 13.

### 12.5 Entry for an object that has since been deleted

- Deleted and AllocationTree entry removed: absent, delete path after 60 s (9.2).
- Deleted but AllocationTree entry still present: the read fails `InvalidObject` each pass; the entry stays until the
  DeletionFinalizationAction removes the allocation. Noisy (error log per pass per entry) but safe.
- Deleted, entry younger than 60 s: deferred.
- Deleted, then the object's allocation never recorded (an allocating transaction whose AllocationFinalizationAction
  is delayed past 60 s): see F5.

### 12.6 Entry whose target store has been rebuilt or transferred

- Store being rebuilt: not `repairable` while Rebuilding (`HostStoreView.scala:15`); its entries remain. After
  promotion to Active, the next scan reconciles each entry; since the rebuild read current state they are no-ops
  and are removed (traceless-commits design, section 4). See [rebuild-process.md](rebuild-process.md) for whether a
  rebuilt store can answer reads for state it has not yet copied.
- Store transferred away: `repairable` false; `abandoned` skips callbacks; any in-flight `snapshot/reconcile/
  repairDelete` fails with `StoreNotHosted` (M16). The entry stays for the new host. Safe.
- Store rebuilt at the same pool index while a repair is in flight on the old host: the old host's `isRepairable` is
  false after the store leaves; promises fail. The ErrorTree entry is keyed by pool index, not by store incarnation,
  so a new store inherits the old store's entries. They are harmless: the new store's copy is fresh, reconcile is a
  no-op, entries are removed.
- Rebuild-required directory discarded (`StoreRebuilds.discardLostStore`): entries are unaffected; same argument.

### 12.7 UnackedCommits entry whose transaction is still live

`holdsTransaction` true (or `awaitingCrl`): the entry is counted as deferred and skipped; DeferralState is not
touched. The Tx applies T (recovered commit vote after the lookup finds the entry), stays until `TxFinalized` or its
driver's late resolution, and the entry is reconciled and removed at a later pass. A Tx that stays live forever (e.g. waiting on a driver) delays the entry
indefinitely, but the heartbeat gives it a driver (CLAUDE.md "lost TxFinalized"). `awaitingCrl` makes the skip global
for the store: while loading a `crl_save.log`, every UnackedCommits entry of the store is skipped, the ErrorTree
phase is unaffected. Safe; a liveness dependency on [transaction-recovery.md](transaction-recovery.md).

Race: the scan checks `holdsTransaction` once at the start of the entry, then reconciles objects one at a time. The
Tx may be created (recovered) after the check. Reconcile is safe regardless (E2, E3): `holding` makes install
`stale` while recovery is in progress (`Frontend.scala:895`), and the Tx's later apply goes through the normal commit
path. Safe.

### 12.8 ErrorTree unavailable (metadata pool degraded)

- The tree walk or an `allocationTree.get` hangs: `withDeadline` fails the scan after 90 s (`ScanTimedOut`). The
  abandoned walk keeps running in the background, DeferralState is discarded, the store's interval doubles (F9).
- The tree walk fails: logged, scan failed, treated as clean, interval doubles toward the 15 min cap.
- Entry insert (marker): `retryUntilSuccessful`; the finalization action cannot complete, so the transaction is not
  forgotten: servers keep its state ("clog up", per CLAUDE.md). No entry is lost.
- Entry delete fails: best-effort, one more pass.
- Policy or limits read fails: last-good or defaults (E14).
- Net effect: no entry is dropped, no state is installed. Repair delay grows until the metadata pool recovers, up to
  the cap.

### 12.9 A repair proof transaction that aborts repeatedly

`prove` returns `false`; `proven` empty: the scan returns `first` (deferred), the entry stays, and `passes` keeps
growing. Each later pass re-reads, re-reconciles, and re-attempts every deferred proof (up to one transaction per key
plus two for object level). Every transaction is an ordinary transaction with the client's normal backoff. No limit
on the proof attempts exists besides the pass interval (floor 30 s). Safe (nothing is installed), and the cost is bounded
by one object's deferred set per pass, but a hot key can starve forever (residual risk in the traceless design).
Escalation, such as switching the proof to a lock-wait, does not exist.

### 12.10 More scenarios

| Scenario | Outcome |
|----------|---------|
| Two scans of one store overlap (a timed-out scan still running when the next starts) | Both are guarded by M3/M4 and M12; duplicated work and read load. Safe. The second scan has a fresh DeferralState (RepairService.scala:198-200). |
| Repair reads an object whose pointer in the AllocationTree differs from the entry's store pointer | Entry's pointer is used only for deletion; update uses the AllocationTree's. Fine. |
| Entry key decode throws (malformed) | `Future.unit.flatMap` converts to a failed Future; the walk logs and continues (`StoreRepairer.scala:155-160`). The entry stays forever. |
| Entry for a (store, object) the store never hosted, e.g. a width change | `poolIndex >= ida.width` -> stale (`Reconcile.scala:112-118`), entry stays forever. No width change exists today. |
| ErrorTree read returns the repairing store's own entries only | `[poolIndex, poolIndex+1)`; keys `>= 0x80` compare unsigned. MaxWidth 127. `(poolIndex+1).toByte` overflow does not occur below 255. |
| An FSIF store with no free extent while repair installs | `backend.repair` fails; promise fails; entry stays. FSIF refuses to overwrite a different occupant (`FsifBackend.scala:~1280`). |
| Read quorum includes the repairing store's own stale answer | Same as any read (12.2). |
| Clock skew | Entry age uses `HLCTimestamp.now - value.timestamp`; a future-dated entry is ineligible (`:396`). Skew does not affect the reconcile guards, which use revisions and tx ids. |

---

## 13. Suspected flaws and open questions

Confidence is my confidence that the described behaviour is real and matters, from reading the code alone (no
experiments run).

### 13.0 Re-assessment for replay pins (2026-11-09, `0d08e3c6`)

The refactor touched entry production (`MissedUpdateFinalizationAction`, `MissedUpdateMarker`), the scan's names and
entry deletion (`StoreRepairer`), and proofs (`RepairProofs`, now `replaySafe`); reconcile, opportunistic rebuild and the
completion gate are unchanged.

| Id | Status | Reason | Evidence |
|---|---|---|---|
| F1, F2 | unchanged | Opportunistic rebuild paths untouched | `Frontend.scala` rebuild path, `Tx.scala:429-441` |
| F3 | unchanged | Blind/refcount timestamps still unconditional | `RequirementsApplyer.scala` (not in the diff) |
| F4 | **narrowed** | TKVL deletes now carry `KeyRevision(k, rev)`, so a delete of k at a store where k is absent fails at apply, is reported and marked instead of applying as an invisible no-op; blind deletes unchanged | `KeyValueListNode.scala:505-506`; `RequirementsChecker.scala:141` |
| F5 | unchanged | Absence heuristic unchanged; the allocation action never skips an entry now (Ruling 28), which does not shorten its delay | `StoreRepairer.scala:249-256` |
| F6 | unchanged | `deleteObjectForRepair` untouched | |
| F7 | unchanged | Entry deletions still untracked; `deleteEntry` now pins on `KeyRevision` of the value read | `StoreRepairer.scala` |
| F8, F9 | unchanged (load larger) | More entries per store (R-N4 in transaction-recovery) | |
| F10 | unchanged (reach widened) | Blind upsert kept (`PutRequirement.Blind`), and the batch is `replaySafe`, so a replayed or re-run batch also regresses entries (transaction-requirements Q-N5) | `MissedUpdateMarker.scala:145-148,165` |
| F11-F15 | unchanged | Not in the diff | |
| N1, N2 | unchanged | Not in the diff | |

### 13.1 Summary table

| ID | Title | Severity | Confidence |
|----|-------|----------|------------|
| F1 | Opportunistic rebuild replaces `os.data` of a KeyValue object but not `os.kvState` | high if real | medium-high |
| F2 | Opportunistic and pre-transaction rebuild overwrite whole KeyValue content when revisions are equal, regressing blind writes applied since the read | high if real | medium |
| F3 | Completion gate `read.timestamp >= missedTimestamp` assumes monotone timestamps; blind KV writes and refcount updates break that | medium (livelock of one entry) | medium |
| F4 | A blind key delete applied as a no-op at the store between snapshot and install is invisible to the snapshot check, so repair re-creates the deleted key at that store | medium | medium |
| F5 | `repairDelete` on allocation absence: object live but not yet in the AllocationTree for over 60 s loses the store's slice and the entry (unguarded delete) | medium impact, low probability | low-medium |
| F6 | `repairDelete` has no in-flight/pending-read guard | low | low |
| F7 | Repair's entry-deletion transaction has tracking disabled unconditionally, so a store missing it is never marked | low | medium |
| F8 | `stale` outcomes never escalate: a hot or perpetually busy object is retried forever | medium (resource), low-medium | medium |
| F9 | `ScanTimedOut` throws away deferral counts and doubles the interval while a backlog exists | low-medium | medium |
| F10 | Marker overwrite can replace a newer `missedTimestamp` with an older one | low | medium |
| F11 | Object-level proof is a `RevisionLock`; blind SetMin/SetMax/SetLeft/SetRight do not move the revision | low | low |
| F12 | Reader ranks revisions by timestamp before count; timestamp bumps from refcount updates can make a stale revision win the ranking and stall reads | low (availability) | low |
| F13 | DataObject `read.timestamp < current.timestamp` is a silent skip that counts as complete | low | low |
| F14 | A zombie slice installed from a pre-deletion read is cleaned only if the delete's own marker is written | low | low |
| F15 | Entry callbacks that fail repeatedly (InvalidObject read of a deleted object whose allocation entry is slow to disappear) log at error each pass | cosmetic | high |

### 13.2 Details

> **Verification 2026-11-09: CONFIRMED (mechanism); doc scenario premise is WRONG. Severity: stale-replica.** ObjectState.kvState is a var initialised once from data (ObjectState.scala:34-37) and no code reassigns it (grep); Frontend.opportunisticRebuild writes os.metadata/os.data only (Frontend.scala:867-881) and the next KV apply does state.data = kvs.encode() from kvState (RequirementsApplyer.scala:133), so the rebuilt content is served and persisted, then silently replaced by stale kvState on the next write; requirement checks and locks also use the stale kvState. Premise error: the client marks a store knownBehind only when its REVISION differs from the chosen one (BaseObjectReader.getStoreStates, :112-130; KeyValueObjectReader.scala:122), while the store applies the rebuild only if its revision EQUALS the read's (Frontend.scala:869). A store that merely missed blind writes (same revision) is never targeted, and a revision-lagging store is skipped; the rebuild fires only when the store caught up between the read and the message. So the 'S missed blind insert of k, then rebuilt' scenario cannot occur as written.
> *Fix sketch:* After replacing os.data for a KeyValue object rebuild kvState from it (or route through Reconcile and delete the opportunistic path).

**F1. Opportunistic rebuild leaves `kvState` stale** (`server/store/Frontend.scala:867-881`; `server/store/ObjectState.scala:
34-37`; `server/transaction/Tx.scala:381-390`).
`opportunisticRebuild` and the pre-transaction path assign `os.metadata` and `os.data` and commit. `ObjectState.kvState`
(the structure requirement checks, locks and the next apply use) is a `var` initialised once from `data` at
construction; nothing refreshes it (grep: only `reconcileSettled` mutates `kvs` and re-encodes). For a KeyValue object
the rebuilt `data` is therefore served to reads and written to the backend, but requirement checks still see the old
content, and the next transaction's apply does `state.data = kvs.encode()` (`RequirementsApplyer.scala:~138`), which
re-encodes the *old* content plus that transaction's change, silently undoing the rebuild. A requirement such as
`KeyRevision(k, newRev)` that the rebuilt data satisfies would abort at this store. Scenario: store S missed an
insert of k (blind). A read triggers an opportunistic rebuild of S (same revision). S's reads now show k; the next
insert of k2 at S encodes `kvState` (without k) + k2; k is gone again and nothing records it (the transaction replied OK).
`FrontendSuite` tests (lines 2002-2060) check ordering with the backend write, not the `kvState` content. Confidence:
medium-high; the fix is to rebuild `kvState` from the new data, or to route through `reconcile`.

> **Verification 2026-11-09: CONFIRMED (mechanism, race trigger). Severity: stale-replica (durable regression of one replica until next write).** Guard is only revision equality (+ !commitInFlight on the message path): os.data = op.data replaces the whole KV content with the read's snapshot and os.metadata.timestamp goes back to op.timestamp (Frontend.scala:867-881). Blind writes applied after the read do not change the revision (RequirementsApplyer.scala:105-112), so they are dropped from data (and persisted that way: crash reloads the regression), although the stale kvState still holds them and re-encodes them on the next write (interplay with F1). Pre-transaction path additionally has no commitInFlight guard and no refcount max (Tx.scala:380-392, requirements F3).
> *Fix sketch:* Do not replace content on equal revision for KeyValue objects; use Reconcile with snapshot compare.

**F2. Whole-content overwrite when revisions are equal** (`Frontend.scala:867-881`; `Tx.scala:381-390`;
`SimpleOpportunisticRebuildManager.scala:24-39`).
The only guard is `os.metadata.revision == op.revision` (plus `!commitInFlight` for the message path). For KeyValue
objects, blind writes do not change the revision (Q9), so equality says nothing about keys. `op.data` is the whole
object as of a read that may be seconds old (the message path), or as of the *client's cached state* (the pre-transaction
path, whose manager keeps entries 10 s but reads the client object cache with its own lifetime). Scenario: S is behind
on key a. Read R1 (t0) restores a@r. S then applies blind insert of key b (t1, replied OK). The rebuild message from
R1 arrives (t2): `os.data = op.data` has no b; `os.metadata.timestamp` goes back to R1's timestamp. S lost a committed
key (b) that nothing records; durability of b then rests on fewer than the intended number of stores. Even with F1's
repair, the guard does not compare the store's content or timestamp (`op.timestamp` < `os.timestamp` is accepted).
Confidence: medium (depends on a blind-write workload and on the timing; the code has no check).

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: liveness (one repair entry retried forever).** Gate `os.timestamp >= minTimestamp` (StoreRepairer.scala:~262-266); blind KV and refcount updates assign timestamp := txd.startTimestamp with no max (RequirementsApplyer.scala:109-112, 137-139), so two blind writes applied out of start-timestamp order leave a timestamp below the entry's missedTimestamp; every pass is stale.
> *Fix sketch:* Gate on post-install equality with the read, or apply max() to blind updates.

**F3. Completion gate assumes monotone timestamps** (`StoreRepairer.scala:266-271`; Q8).
`stale` is set when `os.timestamp < minTimestamp`, with `os.timestamp` = max over matching stores of the *last applied*
transaction's start timestamp. Updates without a required revision (blind KeyValue inserts/deletes) never check the
object timestamp (`RequirementsChecker.scala:220-226` is called only for revision-bearing requirements;
`checkKVRequirements` only when `requiredRevision` is set), and `RequirementsApplyer` assigns it unconditionally.
Scenario: blind insert T1 (start 30) and blind insert T0 (start 20, slow client) on different keys; T1 applies first,
T0 later, on every store; the object's timestamp is 20. Store S missed T1; the entry has `missedTimestamp` 30. S is
repaired (install ran), but every read says 20 < 30, so `complete` is false forever, until another update with start
>= 30 reaches the object. Cold objects keep an entry, a full read and a reconcile, every 30 s. `stale` does not count
toward proofs, so nothing ends it. Confidence: medium. Mitigation ideas for the fix stage: make the gate depend on
the store's post-install equality with the read (all unlocked parts equal), not on a timestamp.

> **Verification 2026-11-09: CONFIRMED (mechanism, narrow race). Severity: resurrection (zombie key on one store).** Reconcile.planKeyValue compares only key revisions (Reconcile.scala:179-190) and snapshot vs current (Reconcile.scala:172-190); a blind Delete(k) applied as a no-op at S (RequirementsApplyer.scala:131 `kvs.content -= key`) changes S's timestamp only, so current == snapshot and the read's k@I is installed, while the lock check (os.lockedKeys at install) misses a delete that already applied and released. Needs the delete to vote and apply between the repair's read and install.
> *Fix sketch:* Add the store's timestamp (or a change counter) to RepairSnapshot and treat a change as stale.

> **Refresh (replay pins): NARROWED.** Deletes planned by TKVL now require `KeyRevision(k, rev)`
> (`KeyValueListNode.scala:505-506`); at S, where k is absent, the delete fails its check at apply, is reported in
> `TxCommitted` and marked, so the next scan repairs S. Unchanged for blind deletes.

**F4. No-op blind delete is invisible to the snapshot check** (`Reconcile.scala:179-189`; `RequirementsApplyer.scala:~126`).
The snapshot compares key revisions. A blind `Delete(k)` applied at S where k is absent changes S's timestamp only.
Scenario: key k absent at S (S missed insert I). Snapshot at t0. Read at t1 from B, C returns k@I with no locks. A
delete D of k then votes, commits and applies at B, C and S (a no-op at S) at t2. Install at t3: `have = None ==
snapshot None`, `want = Some(k@I)`, so k@I is installed at S. Final: B and C lack k, S has k@I; a zombie slice that
reads ignore (Q6: matching 1 < threshold, none locked, but *a read that includes S and one store from {B,C} only* has
`matching + potential >= threshold`, so reads of the object wait for the third store; with one store down they hang).
`DoesNotExist(k)` requirements abort at S and mark S in the ErrorTree, so the next write heals it. The entry for this
repair was deleted as complete. The same hole exists for a blind object-level change that sets the same value. A fix:
compare `current.timestamp` (or a store change counter) with the snapshot's in addition to key revisions. Confidence:
medium (needs the delete inside the read-to-install window, and a blind delete on that key).

> **Verification 2026-11-09: PLAUSIBLE. Severity: data-loss (one replica slice).** Mechanism verified: absence from the AllocationTree + entry value timestamp older than minErrorEntryAgeForDeletion (60 s) => repairDeletion + unguarded deleteEntry (StoreRepairer.scala:182-195, 230-236; Frontend.deleteObjectForRepair :822-828 just discards and deletes). Bootstrap objects are registered in the tree (Bootstrap.scala:128-166), so only an allocation recorded >60 s late (stuck AllocationFinalizationAction, e.g. paired with recovery F2) triggers it.
> *Fix sketch:* Confirm deletion through the pool (refcount read / deletion log) before removing a slice; keep the entry on doubt.

**F5. Allocation-absence heuristic** (`StoreRepairer.scala:184-199,233-239,399-402`).
Absent from the AllocationTree plus an entry older than 60 s is taken as "deleted". If the allocating transaction's
`AllocationFinalizationAction` is delayed past 60 s (allocation tree in a degraded metadata pool; a recovered
finalizer; retry storm), then for an object that S **already holds** (S missed a later update U): `repairDelete`
removes S's live slice, and `deleteEntry` removes the entry unguarded (`:190-191`), without any record that S is short a
slice. The slice returns only through a full store rebuild (an opportunistic rebuild ignores an object not present on
the store, `Frontend.scala:1051`). For a store that missed the *allocation* (no copy), the entry is removed before the
store is ever given the slice (the comment at `:386-398` acknowledges the cost: "guessing wrong costs a replica").
Also, the clock the age uses is the entry value's HLC timestamp; the AllocationFinalizationAction timing is
unbounded. Confidence: low-medium (needs a >60 s allocation recording delay). Open question: should repair confirm the
deletion through the pool's state (e.g. the object's refcount in a read, which returns `InvalidObject` for a deleted
object but also for "never allocated")?

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica (low).** Frontend.deleteObjectForRepair has no commitInFlight/pendingReads guard (Frontend.scala:822-828).
> *Fix sketch:* Refuse while reads/commits are pending.

**F6. `repairDelete` has no in-flight guard** (`Frontend.scala:822-828`).
`discard(objectId)` then `backend.repairDelete`. It does not check `commitInFlight`, `committing`, or pending reads
(`pendingReads`), unlike `reconcile`. A read in flight from the backend can return the old object after the delete
request was issued and re-insert it into the cache. For a truly deleted object the effect is a stale cache entry
(served until evicted: a store answering reads with a zombie). Confidence: low.

> **Verification 2026-11-09: CONFIRMED (documented trade-off). Severity: stale-replica (low).** AspenClient.newRepairTransaction disables missed-update tracking for every entry deletion (AspenClient.scala:96-99).
> *Fix sketch:* Use limitTracking for entry deletions as for marker writes.

**F7. Entry deletion never records its own misses** (`client/AspenClient.scala:~89-100`; `StoreRepairer.scala:337-361`).
`newRepairTransaction` disables tracking for every entry deletion, although `limitTracking` (used for marker writes)
is selective by pool. A store of the metadata pool that misses the deletion's commit keeps an ErrorTree node (or
UnackedCommits node) holding an entry that was removed. Reads are correct (quorum), and the next write to the node
that has a required revision aborts at the stale store and marks it. A cold node stays stale until the quorum is
degraded; then a removed entry can reappear and cause extra repairs (harmless). The same applies to the proofs. This
is the documented trade-off (`AspenClient.scala:89-99`; the continual-repair design). Confidence: medium that it is
as described; low impact.

> **Verification 2026-11-09: PLAUSIBLE. Severity: liveness.** unsettled/stale paths in Frontend.reconcile (Frontend.scala:~895-905) return stale without escalation; trigger needs a perpetually busy object.
> *Fix sketch:* Count stale passes toward proofs, add backoff.

**F8. `stale` never escalates** (`StoreRepairer.scala:36-43,58-61`; `Frontend.scala:888-906`).
`unsettled` and snapshot mismatches yield `stale`, which neither counts toward `proofAfterDeferrals` nor triggers a
proof. For a DataObject the store's revision can only change by the store applying the chain, so staleness is
transient. For a hot KeyValue object, the combination "S is behind on key set A" and "S is continually applying
other keys" works key by key, so progress is made; but if the object is written often enough that `commitInFlight` or
a held transaction is present at nearly every install attempt, installs are refused every time. The traceless design
admits this for keys ("residual risks"). Each pass costs a full object read and a reconcile attempt; no back-off applies
to the entry (the scan stays at the floor because `foundEntries` is true). Confidence: medium.

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (slower recovery).** ScanTimedOut is recovered as foundEntries=false, which doubles the interval, and DeferralState is dropped (RepairService.scala:189-203, ScanState.advance).
> *Fix sketch:* Treat a timeout as 'found entries' and keep deferral counts.

**F9. Timed-out scans** (`RepairService.scala:194-200,235`; `ScanState.scala:34-38`).
A scan that exceeds 90 s is counted as "found nothing", doubles the store's interval (toward 15 min), and its
DeferralState is dropped so proof counts restart. A store with a large backlog (long outage) needs many sequential
read+install+delete cycles; 90 s may allow only a few hundred. The abandoned scan continues in the background
(concurrently with the next), and each timeout delays the next scan more. The effect is slower recovery of exactly
the stores that need it most; an entry with persistent locks never accumulates 10 deferrals across timed-out scans.
Confidence: medium (depends on backlog size and latency).

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (defence in depth only).** MissedUpdateMarker.writeBatch does a blind TKVLInsert (MissedUpdateMarker.scala:140-146); merging by newest is only within one queue (:97-106), so a later batch with an older timestamp overwrites.
> *Fix sketch:* Read-modify-write with max(), or use requireRevision.

> **Refresh (replay pins): UNCHANGED, reach widened.** The batch is now `replaySafe` (`MissedUpdateMarker.scala:165`): a
> replay of an old batch, or the marker re-run by a driver adopting an old accepted commit (Ruling 13), regresses entries
> the same way and re-creates UnackedCommits entries already deleted (transaction-recovery R-N6).

**F10. Marker overwrite can regress `missedTimestamp`** (`MissedUpdateMarker.scala:316-327,369-376`).
Within a pool queue the newest `missedTimestamp` wins, but across batches a late-arriving older miss overwrites the
tree value without comparing. Scenario: transactions T2 (newer) and T1 (older) both missed S for object X; T2's
finalizer wins the race to the queue and its entry is written; T1's marker (scheduled by its own `errorTimeout`) later
writes the entry with T1's smaller timestamp. The completion gate M13 is weakened to T1. Given E1 this is
defence-in-depth only, so it is low impact; but it shows that the entry value is not a monotone max. Confidence:
medium that it happens; low impact. Also affects which `storePointer` bytes are kept (identical, so no effect).

> **Verification 2026-11-09: PLAUSIBLE. Severity: stale-replica (low).** Blind object-level KV ops do not move the revision (RequirementsApplyer.scala:105-112); proof is a RevisionLock.
> *Fix sketch:* Include a timestamp in the proof's requirements.

**F11. Object-level proof compares revision only** (`RepairProofs.scala:261-268`; `RequirementsApplyer.scala:105-111`).
A blind `SetMin`/`SetMax`/`SetLeft`/`SetRight` (no required revision) changes the object-level parts but not the
revision. If the read saw the object-level part locked (reason for the deferral) and the lock holder committed between
the read and the proof, the `RevisionLock(readRevision)` still commits; the scan then reconciles object-level parts
from the older read (if S is unchanged since the snapshot, F4-style). The same note appears in the continual-repair
design ("compares the revision but not min, max, left or right"). Whether TKVL/system code issues blind object-level
writes is for the reviewer; the code permits them. Confidence: low.

> **Verification 2026-11-09: PLAUSIBLE. Severity: liveness (availability).** Reader ranks by (max timestamp, count) (BaseObjectReader.scala:112-130 area).
> *Fix sketch:* Rank by revision chain where available.

**F12. Revision ranking by timestamp first** (`BaseObjectReader.scala:142-153`).
A stale store whose timestamp was raised by a later refcount update can outrank (same timestamp, more up-to-date but
lower count) or exceed the up-to-date revision's timestamp. With fewer than `threshold` stores at that revision the
read waits (never returns wrong data), but with an unavailable store it may wait indefinitely, and the stale stores
are marked `knownBehind` for the wrong revision (triggering an opportunistic rebuild message to an up-to-date store,
which is then ignored by the revision check). Availability only. Confidence: low.

> **Verification 2026-11-09: PLAUSIBLE. Severity: stale-replica (narrow).** Reconcile.planData treats read.timestamp < current.timestamp as complete with no change (Reconcile.scala:151-153). A store that missed data write D but applied a later refcount update R2 that the read quorum has not yet applied has a higher timestamp than the read, so the miss is treated as caught up and the entry is dropped. Needs R2 committed but unapplied at the read quorum.
> *Fix sketch:* Mark stale when the revision differs but timestamp is newer, and retry.

**F13. DataObject silent skip** (`Reconcile.scala:151-153`).
`read.timestamp < current.timestamp` skips the data without marking stale, so the result is `complete` and the entry is
dropped. I argued above (E6) that this cannot drop a needed repair for DataObjects, because DataUpdate/VersionBump check
timestamps and refcount updates form a causal chain. The argument depends on A1-A3 and on `timestamp` being the max
over matching stores; it should be confirmed or the branch should mark `stale`. Confidence: low.

> **Verification 2026-11-09: REFUTED as a flaw. Severity: doc-only.** S errors on a refcount update of an absent object, so a marker is produced, as the item itself concludes; no unsound step found.
> *Fix sketch:* None.

**F14. Zombie slice from a pre-deletion read** (9.3). The slice is cleaned up only if the deleting transaction wrote a
marker for S; the marker is written from `commitErrors`/silence. If S replied OK to the delete (it cannot: no object)
it would not be marked. S always errors on a refcount update of an absent object, so a marker is produced. The
dependency is real but sound. Confidence: low.

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only.** Cosmetic log. *Fix sketch:* Downgrade the log level.
**F15.** Entries whose object is deleted but whose AllocationTree entry has not been removed log an error from
`TierEngine` each pass (`TierEngine.scala:139-142`). No data effect.

### 13.3 Other open questions for the reviewer

1. **Recovered commit votes and E1.** Section 11 relies on a recovered `VoteCommit` Tx retaking its locks before the
   store answers reads (`Frontend.holding`). Confirm in [crash-recovery-log.md](crash-recovery-log.md) that no window
   exists where a store answers a read between CRL recovery and lock retake.
2. **Object timestamp semantics.** Whether the system intends object timestamps to be monotone (the design says "HLC
   timestamps are per transaction, so a read no older than the store reflects every transaction the store applied",
   `Reconcile.scala:147-150`). Q8 says it is not for blind writes and refcount updates. F3 and F13 follow.
3. **Timestamp advance with deferred keys.** `planKeyValue` sets the object timestamp to the read's even if keys were
   deferred (`:212`). The only readers of the store's timestamp for KeyValue objects are the reader's revision ranking
   and `checkRevision`, so this has no visible effect today; if a future change uses the store's timestamp as a
   "caught up" indicator it would be wrong.
4. **Proof on a refcount-0 or deleted object.** A proof of a refcount for an object that a concurrent delete has set
   to zero aborts on the required refcount. Fine, but not exercised in tests I saw.
5. **`maxConcurrentStoreScans` applies to metadata reads too** (`RepairService.scala:134`). Harmless.
6. **ErrorTree growth during a long outage.** One entry per (store, object), plus, since the refresh, one
   UnackedCommits entry per (store, transaction) for every committed transaction with that store as a silent voter
   (transaction-recovery R-N4). A store down for days across a workload that touches many objects makes a large tree; the 256-entry
   batch cap and TKVL splitting keep individual writes bounded, but the scan is sequential and F9 limits progress per
   scan.
7. **Interaction of repair's own read with opportunistic rebuild.** The repair's `client.read` is an ordinary read and
   may trigger rebuild messages to the store being repaired (10.3), which F1/F2 make unsafe; confirm whether the read
   in `repairUpdate` should disable opportunistic rebuild (`disableOpportunisticRebuild`).

### 13.4 New findings from the adversarial verification (2026-11-09)

* **N1 (stale-replica, doc correction for F1/F2).** The opportunistic rebuild is nearly dead code for the store it targets: the client flags a store `knownBehind` only when its revision differs from the chosen revision (`BaseObjectReader.scala` getStoreStates, `KeyValueObjectReader.scala:122`), while `Frontend.opportunisticRebuild` applies only when the store's revision EQUALS the read's (`Frontend.scala:869`). It fires only when the store caught up between the read and the message arriving, and then it replaces content with the read's older snapshot (F2) leaving `kvState` stale (F1). Real repair of a revision-lagging or same-revision-lagging store is left entirely to the ErrorTree path. Fix: delete the path or turn it into a Reconcile call.
* **N2 (stale-replica).** The client-side read that feeds repair (`repairUpdate`'s `client.read`) is an ordinary read and may itself send `OpportunisticRebuild` to the store under repair (open question 7): with F1/F2 this can overwrite the very install the repair just made. `disableOpportunisticRebuild` exists on the read manager (`ReadManager.scala:113`) but `repairUpdate` does not set it.

### 13.5 New findings from the replay-pins refresh (2026-11-09)

* **RP-N3 (capacity; CONFIRMED).** Every UnackedCommits entry is reconciled object by object
  (`StoreRepairer.scala:246-261`) even when the same (store, object) also has an ErrorTree entry, as it does for a silent
  store: twice the snapshots, reads and installs per missed object. See transaction-recovery R-N4 for growth.
* **RP-N4 (resurrection window; PLAUSIBLE).** An entry outlives the store's own late application of T until the scan
  deletes it (`deleteEntryIfUnchanged`, `StoreRepairer.scala:262-266`). Meanwhile a late commit of a replay applies T's
  pin-only blind writes there again (Ruling 17; transaction-recovery R-N1). Faster scans shrink the window; nothing
  closes it.
* **RP-N5 (REFUTED).** Deleting an entry while a recovered vote still needs it: the scan skips while
  `mayHoldTransaction` (`awaitingCrl || transactions.contains`, `Frontend.scala:96`), and a found entry's Tx stays live
  until `TxFinalized` (Ruling 9), so a recovered vote has looked its entry up before the scan can delete it.
