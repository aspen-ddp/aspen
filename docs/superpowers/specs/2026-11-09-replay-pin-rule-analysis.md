# Replay without the horizon: the one-pin rule (impact analysis)

Date: 2026-11-09. Answers the first item of `Todo.txt` (revisit traceless operations). This is an impact analysis to
decide from, not a design. **Superseded for what gets built by `2026-11-09-replay-pins-design.md`**, which replaces
this document's "recovered commit voters only" rule with owed-commit entries (UnackedCommits) and late commits. Facts come from reading the code; nothing here has been run. Items marked *unverified* need
a test before a design relies on them.

## The question and why it matters

A late prepare for a transaction that every store has already finalized and forgotten can commit a second time. The
user's example: a blind delete of key A commits; a new A is written; a late replay of the delete removes the new A.
From the end user's point of view the system is corrupted.

Today the horizon (`Host.txHorizon`, `TransactionTooOld`) stops this by wall-clock age: a prepare for an unknown
transaction older than 6 s gets an abort vote unchecked. It trusts the client-chosen `startTimestamp`, so a client
clock more than about 1 s fast defeats it (docs/safety/README.md, Paxos F3 / Recovery F7, CONFIRMED). The goal is to
remove the horizon and make replay impossible by construction.

## Two different problems, only one of which is in scope

- **Replay**: a forgotten transaction's requirements hold again, so it is decided committed again. Caused by blind
  writes and by requirements that can become true again (ABA). This is the target.
- **No trace**: after a commit nothing on a store proves that *this* transaction made the change (KV key deletes
  without a required revision, every `RefcountUpdate`). This is what the TracelessCommits tree handles, for a store
  that recovers an unresolved commit vote after the others forgot the transaction.

They overlap in name only. A `KeyRevision`-checked delete cannot be replayed yet is still traceless. Removing traceless
operations would need backend tombstones for deleted objects (FSIF and RocksDB formats, repair, rebuild, FSIF index
release) and either per-key delete tombstones or whole-node locking on every hot KV tree (ErrorTree, AllocationTree,
directories, registries). That is very expensive and does nothing for replay. **Recommendation: keep TracelessCommits
as it is; address replay only.**

## The one-pin rule

**Rule (user's decision):** every transaction contains at least one *pin*. The client never sends a transaction
without one unless the application explicitly marks it as not needing one. The ErrorTree marking batch is the model
exemption: re-inserting an ErrorTree entry only causes one more idempotent repair pass, and preventing it is not worth
the overhead.

### What counts as a pin

A write that permanently invalidates its own requirement once applied:

| Pin | Why it cannot hold again |
|---|---|
| `DataUpdate` (overwrite, append, ranged write) at a required revision other than `ObjectRevision.Allocating` | revision becomes this transaction's id |
| `VersionBump` | same |
| `KeyValueUpdate` with `requiredRevision = Some(r)` | moves the object revision (`RequirementsApplyer.scala:107-112`) |
| `KeyRevision(k, r)` on a key the same update inserts or deletes | the key's revision becomes the txid, or the key is gone |
| `RefcountUpdate` whose new refcount differs from the required one | the serial only grows (`ObjectRefcount.scala`) |

**Not pins:**
- allocations (the store fabricates a fresh state without reading the backend, `Frontend.scala:854-865`);
- `RevisionLock` and `SharedRevisionLock` (they change nothing);
- `DoesNotExist`, `Exists`, `MayExist`, `WithinRange` and `FullContentLock` (they can become true again);
- the timestamp key requirements and `LocalTimeRequirement`;
- identity refcount updates (`RepairProofs.proveRefcount`, allocation's `Allocating -> Default`).

### Why one pin protects the whole transaction

A replay commits only if every object it touches reaches its write threshold of commit votes. After T commits, the
stores of the pinned object that still hold the pre-T state *and* do not know T are at most width − writeThreshold:

- a commit voter either applied T or holds it live from its CRL, and a live Tx is not replayed;
- a rebuilt store is muted (`acceptorSince`).

Every IDA enforces writeThreshold > width / 2 (`IDA.scala:203`, `:408`, `:588`), so those stores cannot supply a
write threshold. The pinned object always gets enough abort votes, and the requirements on the transaction's other
objects no longer matter.

### Enforcement

- **Client side** (user's decision): `TransactionBuilder` refuses to send a transaction with no pin unless it is
  marked, for example `tx.allowUnpinned()` (the name is open). It sees every requirement and operation, so the check
  is local and cheap.
- **Store side** (user's decision): none. The client check is sufficient, so the marker is a client-side flag only and
  the wire format does not change.

## Finding: proof-of-commit re-applies replayed transactions

*Believed from reading the code; unverified.* This affects the rule directly, and the horizon today.

`Tx.proveCommitted` (`Tx.scala:523-539`) fires when a store does not know a transaction and one of its objects already
shows the transaction's update. It answers `TxResolved(committed)`, and per its scaladoc "the proposer finalizes it,
and every store applies what it lacks". Every other store that created a fresh Tx from the replayed prepare then runs
`doCommit` and applies its update, re-checking requirements with `applying = true`.

Walk the user's scenario with a pin:

1. T overwrites object X at revision r (the pin) and blind-deletes key A on object Y. T commits everywhere and is
   forgotten.
2. T2 inserts a new A on Y. X is untouched.
3. T's prepare is replayed. X's stores show revision T, so they prove T committed. Y's stores have no failing
   requirement for a blind delete. On the committed resolution they delete the new A.

So the one-pin rule stops a replay from being *voted* committed, but proof-of-commit *declares* it committed whenever
the pin still shows T. The pin's self-invalidation is exactly what triggers the proof. Today's horizon has the same
gap: past the horizon the proof comes first (`Tx.scala:458-463`, then `decideDisposition`), and the `tooOld` Tx on Y
applies the committed outcome like any other.

**Fix (user's decision):** a commit learned by proof is applied only by a Tx recovered from the CRL with its own
commit vote (the case proof-of-commit exists for). The case is exceedingly rare, so correctness alone decides it; its
performance does not matter. A fresh Tx created from a prepare for a transaction it
did not know drops its update and is forgotten. This is safe because any store that had not applied T when T was
finalized was marked in the ErrorTree by `MissedUpdateFinalizationAction`, so repair covers it (bootstrap-pool marker
batches excepted, as today). It is also simpler than today's behavior, which fits the project's preference for simple
recovery on rare paths. The resolution message (or the proving store's reply) has to carry "proven" so receivers can
tell.

**First implementation step:** a test showing the scenario above re-applies the delete today.

## What one pin does not cover

These corrupt state without any replayed *decision*. The pin rule leaves them as they are:

- **Lagging-store order** (Requirements F1, CONFIRMED): a store that never saw T's prepare applies T2's blind insert of
  A, then T's late data and commit, and deletes the new A on that store only. Reads use a quorum, so this is usually
  masked, and an ErrorTree entry is written only if `TxCommitted` came more than 2 s late.
- **CRL re-apply** (Recovery F4, CONFIRMED narrow): after a crash, a recovered commit vote re-applies a blind KV write
  over newer state, because the applied-data drop is in memory only.

Both close if every *write* is individually checked. A per-write check states the exact prior state of what it
changes:
- `KeyRevision(k, rev)` for an insert or overwrite of an existing key, `DoesNotExist(k)` for a new one;
- `KeyRevision(k, rev)` for a delete;
- objects already require a revision.

When a store applies a committed transaction it re-checks requirements (`doCommit` → `RequirementsApplyer.apply`), and
a stale write fails its own check. It is then skipped and reported, the store is marked in the ErrorTree, and repair
restores it from the other stores.

**Decided (user): per-write checks are the default wherever they are free**, which is most sites, since the code
already read the node or object. The one-pin rule stays the minimum. Pin-only is allowed where per-write checks cost
something, such as the ErrorTree batch, which deliberately overwrites the newest miss and is harmless when stale.
The two layers are distinct: the pin stops a replay from being *decided* committed, and per-write checks stop a stale
write from being *applied* on any one store.

## Removing the horizon

Deleted (about 60 main lines, 150 test lines):
- `Host.txHorizon`;
- the `horizon` parameter of Store and Frontend (`StoreLoader.scala:336`);
- the age test in `Frontend.receivePrepare` (`:665-686`);
- `TransactionTooOld`;
- the early-eviction warning in `TransactionStatusCache` (about 40 lines; it exists only to protect the horizon);
- the horizon tests in `FrontendSuite` (`:718-786`, `:870-934`) and `TransactionStatusCacheSuite` (`:29-45`).

Kept:
- the `tooOld` path in Tx, because `muted` (rebuilt stores, `acceptorSince`) uses it;
- the status cache, still needed to recover a lost `TxFinalized`.

**Cost of removal:** a replayed prepare is checked normally. Where it passes (unpinned objects, lagging stores) it takes
locks until a recovery driver resolves it as an abort, roughly `Host.txHeartbeatTimeout` plus one round. Live
transactions on those objects collide during that window. Replays are rare (late network messages, client
retransmits), so this is a small cost.

**Decided (user):** the horizon is removed entirely. Keeping the age test as a lock-free early abort was considered
and rejected: it would keep a timestamp-based branch, and its proof-first ordering would need the proof-of-commit fix
too, for little gain on a rare path.

## Inventory: pinless transactions today

"Fix" names the cheapest pin. "Exempt?" is whether replay is harmless enough to mark, as the user did for the
ErrorTree batch. Sites already pinned are not listed: DataObject TKVL, root managers, durable task steps, the FSIF
bitmap, host, device and pool state, vdisk data, fence and grow.

### Core (client, compute, server)

| Site | Today | Replay harm | Fix | Exempt? |
|---|---|---|---|---|
| `MissedUpdateMarker.writeBatch` ErrorTree and TracelessCommits inserts | blind upsert | an extra repair pass | none | **yes** (user) |
| `RepairProofs` proveKey / proveObjectLevel / proveRefcount | no writes, or an identity refcount update | none: no change | none | yes (my call) |
| `StoreRepairer.deleteEntry` (`:355-361`) | `Exists` delete of an ErrorTree key | deletes a newer mark for the same object, so a missed update is never repaired | `KeyRevision` from the value it just read | no |
| `SplitFinalizationAction.insertIntoExistingTier` (`:66-67`) | blind upper-tier insert | after a join, re-inserts a pointer to a deleted node | `KeyRevision`/`DoesNotExist` plus a lock on the new node's revision (fails once the node is gone) | no |
| `JoinFinalizationAction.deleteFromExistingTier` (`:83`) | `Exists` delete | deletes a re-added entry for the same minimum | `KeyRevision` | no |
| `AllocationFinalizationAction` (`:28`) | blind `set(id, ptr)` | resurrects an entry for a deleted object; repair and rebuild then chase it | `lockRevision` on the new object (fails once it is deleted) | no |
| `DeletionFinalizationAction` (`:28`) | `Exists` delete | object ids are never reused, so reachable only through an AllocationFA replay | `KeyRevision` | decide |
| `SystemTaskServiceState.enroll` / `remove` | blind set / `Exists` delete | re-enrolls a finished system task | `KeyRevision`/node pin, or the task state object's revision | no |
| `SimpleTaskExecutor.prepareTask` / `deallocateTask` (`:95-124`) | blind writes to a recycled task object | overwrites a task now in use, or wipes it | the task object's revision (single writer, no contention) | no |
| `SimpleTaskExecutor.allocateTask`, `SimpleDurableServiceExecutor.registerService`, `Registry.prepareRegister`, `MetadataTree.preparePut` | `DoesNotExist` plus allocations | resurrects a registration after a later delete (registries have no delete today; services and vdisk do) | see "Absent-key pins" | decide per site |
| KV TKVL `set` with requirement `None` (`KeyValueListNode.scala:803-805`), `deleteOneAtATime` (`:1114`) | blind | depends on the caller | the planner derives `KeyRevision` from the node it read; absent keys see below | no |

### amoebafs

Every fix uses state that is already read. Pins added by the fixes in brackets.

- **Directory insert of an absent name** (`SimpleDirectory.scala:79-80`): blind. Pinned only when the incref inode
  overwrite rides along [inode revision].
- **Unlink** (`prepareDelete`, `:101-127`): an `Exists` delete; regular files carry no other pin. Needs `KeyRevision`
  on the entry.
- **Rename in one directory** (`:129-170`): the new key is a blind insert. Needs `KeyRevision` on the old key, and
  `KeyRevision` or an absent-key pin on the new key.
- **Cross-directory move** (`AmoebaNFS.move`): no pin at all for files. Needs `KeyRevision` on the source entry.
- **Directory and file content deletion tasks, and task enrollment**: blind enrollment. Needs the task object's
  revision.
- **Already pinned**: create (durable task step key), inode attribute updates, file writes, truncate.

### vdisk

Single owner plus fence: everything is pinned except two sites.
- `DiskCatalog.delete` → `removeKey` (`DiskCatalog.scala:241-249`): an `Exists` delete on the registry. Needs
  `KeyRevision` from the `tree.get` it already does.
- `DiskCatalog.create`: `DoesNotExist` on the name plus allocations, and no pin. Replaying it after a delete would
  re-register a name that points at freed objects. Needs an absent-key pin.

### Absent-key pins

**Decided (user): option 1, node revision**, at the sites with no other pin: `Registry.prepareRegister`,
`MetadataTree.preparePut`, `DiskCatalog.create`, `registerService` and `allocateTask`. Their write rates make the
serialization harmless. amoebafs needs none:
- rename and move delete the old entry under `KeyRevision`, which is a pin;
- create is pinned by its durable task step;
- hard link is pinned by the inode overwrite.

Inserting a *new* key into a KV tree has no cheap pin today. `DoesNotExist` can become true again, and the node's
`requiredRevision` serializes every writer on that node. Most callers already carry another pin (amoebafs create, the
FSIF bitmap). For the rest (registries, `DiskCatalog.create`, service registration, the KV `set` default) the options
are:

1. **Node revision.** Simple. Fine for low-rate trees such as registries and the disk catalog.
2. **An ABA-free `DoesNotExist`.** The store keeps, per KV object, the timestamp of its latest key delete, and
   `DoesNotExist` also requires it ≤ the transaction's start. This relies on HLC causality, not wall clocks: a
   `KeyRevision`-checked delete read the key, so it is ordered after the insert it removes. Cost: an insert that
   started before an unrelated delete on the same node aborts and retries. It is a server change (object metadata,
   checker, repair).
3. **Mark unpinned** where replay is harmless.

## Incidental bugs found during the survey

Separate from this question. Worth fixing either way.

- **`AmoebaNFS.move` discards its prepare futures** (`amoebafs/nfs/AmoebaNFS.scala:370-379`): `prepareDelete` and
  `prepareInsert` are not awaited and the body returns `Future.unit`, so the move can commit half-staged. *Verified by
  reading.*
- **Shared typeUUID:** `DeleteDirectoryContentTask` (`SimpleDirectory.scala:194`) and `DeleteFileContentTask`
  (`SimpleFileContent.scala:283`) both use `c1fb782f-7f13-4921-8ddf-155123445730`. *Verified.*
- **Concurrent create overwrite:** directory insert of an absent name is blind, so a concurrent create of the same name
  is overwritten and its inode leaks. *Unverified.*
- **`UnlinkFileTask` step 2:** runs `freeResources()` transactions while the step is being prepared, so a step retry
  enrolls duplicate delete tasks. *Verified, worse than stated, fixed.* The separate transaction was committed before
  the task enrollment was staged, so it was usually empty and "succeeded" with nothing in it: unlinked content was
  never freed, and a task object leaked each time. `DeleteDirectoryContentTask` also read its root from the inode the
  same step deletes, and unlinked each entry in a transaction separate from the entry's removal (a crash between them
  unlinked it twice). Step 2 now enrolls the content task in its own transaction, at the inode revision it read; the
  directory task holds the root in its task object and removes each entry in the transaction that unlinks it.
- **Unguarded node deletes:** DataObject TKVL `deleteList` deletes nodes with `lockRevision` + `setRefcount` rather
  than `bumpVersion`, against the tkvl rule that deleted nodes get a version bump. `JoinFinalizationAction.setNewRoot`
  deletes the old root with only a refcount update. *Unverified.*
- **Join through repair:** `StoreRepairer.deleteEntry` uses `KeyValueListNode#delete` with the default no-op
  `prepareForJoin`, so a join triggered by repair registers no `JoinFinalizationAction`. *Unverified.*
- **`LocalTimeRequirement` `Equals`** fails when the times are equal, which looks inverted
  (`RequirementsChecker.scala:203-218`). *Unverified.*

## Decisions needed

1. ~~Proof-of-commit~~ **Decided:** proven commits are applied only by stores that recover their own commit vote.
2. ~~Store-side enforcement~~ **Decided:** the client check is sufficient; stores do not enforce the rule and the
   marker stays out of `TransactionDescription`.
3. ~~The horizon~~ **Decided:** removed entirely, including the age test; no lock-free early abort is kept.
4. ~~Absent-key pins~~ **Decided:** node revision at the sites with no other pin (`Registry.prepareRegister`,
   `MetadataTree.preparePut`, `DiskCatalog.create`, `registerService`, `allocateTask`). No ABA-free `DoesNotExist`
   server change. A generic KV `set` of a new key relies on another pin in its transaction, or fails the client check
   unless marked.
5. ~~Per-write checks~~ **Decided:** per-write checks are the default wherever they are free. Pin-only is allowed
   where they are not, such as the ErrorTree batch.
6. **The "decide" rows** in the inventory: the registries are covered by decision 4. `DeletionFinalizationAction`
   follows from decision 5: `KeyRevision` is free there (the TKVL delete reads the node), so it gets one (my call).
