# Replay pins: replacing the horizon with checked transactions

Date: 2026-11-09. Branch: `pin_refactor` (a sweeping change, so not on main). The background, the full call-site
inventory and the decision record are in `docs/superpowers/specs/2026-11-09-replay-pin-rule-analysis.md` (the
analysis). This document says what to build.

## Why

A late prepare for a transaction that every store has finalized and forgotten can commit it again. The user's example:
a blind delete of key A commits, a new A is written, and a late replay of the delete removes the new A. To the end
user, the system is corrupted.

The horizon (`Host.txHorizon`, `TransactionTooOld`) guards against this by wall-clock age. It trusts the
client-chosen `startTimestamp`, so clock skew or long delays defeat it (docs/safety/README.md, Paxos F3 / Recovery
F7). The user wants the horizon gone and replay made impossible by the structure of the transactions themselves.

## Goals

- **No replayed decisions.** A forgotten transaction is never decided committed again, whatever its age and whatever
  the clocks say, unless the application explicitly marked it safe to replay.
- **No late commits applied by stores that already have them.** When a commit is learned only after the transaction
  was forgotten, it is applied solely by a store that holds its own commit vote and is recorded as still owing that
  commit. Every other store applies nothing.
- **No stale writes applied.** Wherever it costs nothing, a stale write cannot be applied on a lagging or recovering
  store, because it fails its own check.
- **The horizon is removed entirely.**

## Non-goals

- **Making traceless operations leave a trace.** KV deletes and `RefcountUpdate`s stay as they are. The
  TracelessCommits tree is kept and generalized (section 1), not removed.
- **Store-side enforcement of the pin rule** (user's decision: the client check is sufficient). The wire format
  carries no "unpinned" marker.
- **An ABA-free `DoesNotExist` server change** (user's decision: node revision at the few sites that need it).
- **Performance of the late-commit path.** It is exceedingly rare, so only correctness matters (user).

## Anti-goals

- **Node-level serialization on hot trees.** No hot tree (ErrorTree, AllocationTree, amoebafs directories, the inode
  table) gains whole-node locking as a side effect.
- **Time-based tests in the prepare path.** No wall-clock or timestamp-age test of any kind is left there.
- **Silently allowing pinless transactions.** A transaction that is pinless and unmarked must fail at the client, not
  be sent.
- **Normal-path cost.** The commit path when no store misses gains no new work. New tree entries are written only
  when a store misses its acknowledgement, which is when ErrorTree marks are written today.

## Definitions

### Pins

A **pin** is a write that permanently invalidates its own requirement once applied:

| Pin | Why it cannot hold again |
|---|---|
| `DataUpdate` (overwrite, append, ranged write) at a required revision other than `ObjectRevision.Allocating` | the revision becomes this transaction's id |
| `VersionBump` | the same |
| `KeyValueUpdate` with `requiredRevision = Some(_)` | it moves the object revision (`RequirementsApplyer.scala:107-112`) |
| `KeyRevision(k, _)` where the same update has an `Insert(k, _)` or `Delete(k)` | the key's revision becomes the txid, or the key is gone |
| `RefcountUpdate` with `newRefcount != requiredRefcount` | the serial only grows |

These are **not** pins:
- allocations;
- `RevisionLock` and `SharedRevisionLock`;
- `DoesNotExist`, `Exists`, `MayExist`, `WithinRange` and `FullContentLock`;
- the timestamp key requirements and `LocalTimeRequirement`;
- identity refcount updates.

**Why one pin is enough for the decision.** After T commits, at most width − writeThreshold stores of the pinned
object still hold the pre-T state *and* do not know T:
- a commit voter either applied T or holds it live from its CRL;
- rebuilt stores are muted (`acceptorSince`).

Every IDA enforces writeThreshold > width / 2 (`IDA.scala:203`, `:408`, `:588`). So a replay never collects a write
threshold of commit votes on the pinned object.

### Per-write checks

A **per-write check** gives each individual write the exact prior state of what it changes:
- `KeyRevision(k, rev)` for an insert or overwrite of an existing key;
- `DoesNotExist(k)` for a new key;
- `KeyRevision(k, rev)` for a delete;
- objects already require a revision.

When a store applies a commit it re-checks requirements (`doCommit` → `RequirementsApplyer.apply`). A stale write
fails that check and is skipped and reported. The store is then marked in the ErrorTree, and repair restores it.

### Late commits

A **late commit** is a commit resolution that a driver reaches only after the transaction was forgotten. It is defined
precisely in section 1.

## Design

### 1. Late commits are applied only where a commit is owed

#### The problem

*Believed from reading the code; step 1 confirms it.*

`Tx.proveCommitted` (`Tx.scala:523-539`) answers `TxResolved(committed)` when a store does not know a transaction and
one of its objects shows the transaction's update. From there:
1. The receiving driver resolves committed (`TransactionDriver.receiveTxResolved` → `onResolution`).
2. It runs the finalization actions.
3. It sends `TxFinalized(committed)` to every store.
4. Every store holding a fresh Tx from the replayed prepare applies its update.

A blind or `Exists`-guarded write passes the re-check, so in the user's scenario the new A is deleted. The pin's own
trace is exactly what triggers the proof.

A store cannot tell a vote it cast for the original transaction from a vote it cast for a replay:
- **A genuine vote must be applied.** Take a store that voted commit and then missed both resolution messages.
  Dropping its update can leave fewer stores holding the commit than a read needs. Repair then reads the old state
  back, which is the resurrection bug proof-of-commit was built to fix.
- **A replay vote must not be applied.**

The fix records, durably and per store, which stores still owe a commit (the user's choice, option 1 in the decision
record).

#### 1a. The UnackedCommits tree (the generalized TracelessCommits tree)

Today `MissedUpdateFinalizationAction` writes a TracelessCommits entry `(store, txid) → (timestamp, objects)` for each
commit voter that sent no `TxCommitted`, but only for transactions with traceless changes
(`TransactionDescription.tracelessObjects`). The voters are the counted ones (`FinalizationAction.commitVoters`), or
every store without `TxCommitted` when they are unknown.

**Change:** write the entry for every committed transaction, not only traceless ones, with the same voter rule.
- Entry objects are the objects T changes on that store.
- The batch is the same: `MissedUpdateMarker` writes these entries in the per-pool batch that carries the ErrorTree
  marks.
- `limitTracking` disables them as before.
- Everything else about the tree is unchanged: its root key in the pool state, nodes in the metadata group, the repair
  scan that reconciles and deletes entries (`StoreRepairer.repairTracelessEntry`), and the `holdsTransaction` skip.

**Rename** the tree to UnackedCommits (my call), since entries no longer concern only traceless changes:
- `StoragePoolState.TracelessCommitsTreeKey`;
- `TracelessCommitEntry`;
- `TracelessCommitLookup`;
- the `Frontend` and `Tx` lookup members;
- the repair scan names.

Backward compatibility is not required.

**Meaning of an entry:** this store cast a commit vote that the commit relied on (or might have), and had not
acknowledged applying T when T was finalized. It is the durable record that the store is owed T.

**Ordering fact the design relies on (amended: see Amendments, Ruling 8 - the marker now runs last):** entries are written by a finalization action, so they are durable before
`TxFinalized` is sent, and so before any store forgets T. `TransactionDriver.onResolution` completes the finalizer
before `onFinalized`.

#### 1b. When a commit is late

A driver's commit resolution is **late** when the driver has conclusive evidence that T was already finalized:
1. **A quorum of acceptors with no memory of T.** The driver received prepare responses from a Paxos quorum of T's
   acceptors (the quorum it uses to choose a value), none of which carries an accepted value for T, and it holds a
   proof (`TxResolved` from a prover).
2. **A status report of finalization.** A store reported T *finalized committed* from its status cache.

**Why case 1 is conclusive:**
- A proof means some store shows T's update, so T was applied somewhere, so T was resolved committed, so its commit
  was chosen. A Paxos quorum Q accepted it.
- Acceptors keep their state (CRL) until `TxFinalized`.
- Rebuilt acceptors are muted and do not respond.
- Two quorums intersect. So a quorum of responses with no accepted value means some acceptor in Q forgot T, which it
  did only on `TxFinalized`.
- Finalization had therefore completed, and the UnackedCommits entries are durable.

**Why case 2 is conclusive:** the finalized status is set only on `TxFinalized`.

**The live window.** While T is not yet finalized, Q still remembers. Every quorum of responses then contains an
accepted commit, so the driver learns commit normally: not late, finalization actions as usual. A proof that arrives
in the live window, for example from a store that got T's state through a rebuild without ever seeing T, is only a
reason not to decide abort. The driver keeps running Paxos until a quorum answers.

**Behavior on a proof.** A driver that receives a proof no longer resolves on it immediately:
- It never proposes abort while it holds a proof.
- It resolves late commit once it has condition 1 or 2.
- If it learns an accepted or chosen value first, it resolves normally.

Proof-based resolutions already skip the accept phase, and late commits keep doing so. No acceptor accepts a commit
from a replay round.

#### 1c. What a late commit does

**Driver:** runs no finalization actions (the original finalization completed, per 1b) and goes straight to
`onFinalized(committed = true)`. Every `TxResolved` and `TxFinalized` it sends carries `late = true`. That includes the
resolutions `sendPrepareMessages`/`sendPrepareMessage` send once the value is known. Re-running finalization actions
would itself be a replay, and not a harmless one:
- `AllocationFinalizationAction` re-inserting an entry for an object deleted since;
- `MissedUpdateFinalizationAction` marking stores against a long-gone transaction.

**A Tx receiving a late commit:**
- **It holds its own commit vote** (recovered from the CRL or live): it looks up its UnackedCommits entry. If found, it
  applies its update (`doCommit` with `applying = true`), logs at info, and is forgotten as finalized. This is today's
  `tracelessCommitFound` path. If absent, it applies nothing (below). If the lookup fails or gets no answer, it holds
  and retries at heartbeat (`Frontend.TracelessLookupTimeout`), as today.
- **Otherwise** (an abort vote, no vote, a prover, a muted Tx) it applies nothing.

**Applying nothing** means:
- delete the transaction state and release its locks and objects;
- send no `TxCommitted`;
- record T as finalized committed and late in the status cache;
- forget the Tx.

A store that applies nothing yet was owed T is covered: the original finalizer marked it in the ErrorTree, since it
sent no `TxCommitted` then (bootstrap-pool marker batches excepted, as today).

**The prover itself (amended: see Amendments, Rulings 10-12, 15)** (`proveCommitted` → `resolved(true)` → `doCommit`) today applies whatever its other objects lack.
It now applies nothing, per the rule above: a prover voted no commit.

**Status cache:** entries record `late`. A status answer for a late commit reports it as late, so a driver learning
from it resolves late. The status cache is not a source of non-late commits for a forgotten transaction.

#### 1d. Recovered commit votes

The existing recovery lookup (`Tx.needsTracelessCheck`, on load and again before accepting an abort) applies to every
recovered unresolved commit vote, not only transactions with traceless changes, since entries now exist for all of
them. When an entry is found, the Tx applies its update and is forgotten, as today. When none is found, the
existing paths continue, and a late commit then follows 1c (absent entry, so it applies nothing).

The non-late paths are unchanged:
- normal resolution;
- learning an accepted value;
- the missing-data and commit-overtakes-data holds.

### 2. The client pin check

- `TransactionBuilder.buildTranaction` computes whether the transaction has a pin (Definitions) from `requirements`
  and `keyValueUpdates`, before building the description.
- If there is none and the transaction is not marked, it throws a new `TransactionCreationError`,
  `UnpinnedTransaction(transactionId)`. Its scaladoc states the rule and how to mark a transaction.
- The commit then fails with that error and nothing is sent.

**Marking:** `Transaction.replaySafe(reason: String)` (name is my call). It records that the application accepts this
transaction being replayed.
- The reason is added to the transaction's `notes`, so it shows in logs.
- Calling it on a transaction that has a pin is allowed and has no effect.

**Scope:** the check covers transactions built through `TransactionBuilder`, which is every client transaction.
Descriptions built directly (some tests) are out of scope. The marker is not in `TransactionDescription`.

**Allocation never counts as a pin.** The automatic allocation requirements added in `buildTranaction` are added
before the check and are not pins.

### 3. Per-write checks as the default

The user's decision: per-write checks wherever they are free; pin-only where they are not.

**TKVL KV planners (`KeyValueListNode`), the main lever:**
- **Puts.** A put with no requirement (today `None`, which gets only `WithinRange`) gets a derived per-write check from
  the node contents the planner already holds: `KeyRevision(k, existingRev)` if k is present, `DoesNotExist(k)` if it
  is absent. This applies to `planInsert`, `planBatch.putRequirements` and the InPlace path.
- **Deletes.** `Exists(k)` becomes `KeyRevision(k, rev)` everywhere a delete is planned: `remove`, batch removes,
  `delete`'s default overload, rename's old key, and `deleteOneAtATime`.
- **Rename.** The new key gets `KeyRevision` if present, `DoesNotExist` if absent.
- **Blind upsert.** The TKVL APIs get an explicit opt-in to blind upsert for callers that want it
  (`MissedUpdateMarker`).

These checks are per key, so they add no node-level serialization (anti-goal).

**DataObject TKVL** is already fully checked through its node-revision rewrites. No change.

**Retrying callers.** A derived check can now abort a write that used to win blindly. Callers already retry on abort
(`transactUntilSuccessful` and the TKVL retry loops), so the effect is a retry.

**At-least-once finalization actions must treat "my write is already there" as done.** Each converted FA reads current
state before writing and returns success without writing when its effect is present.

### 4. Site conversions

The analysis's inventory has file and line for each site.

**Core:**
- **`MissedUpdateMarker.writeBatch`** (ErrorTree and UnackedCommits entries): blind upsert kept, marked
  `replaySafe("missed-update marks are idempotent")` (user's example).
- **`RepairProofs`** proveKey, proveObjectLevel and proveRefcount: `replaySafe`, because they change nothing.
- **`StoreRepairer.deleteEntry`**: `KeyRevision` from the value it read.
- **`SplitFinalizationAction.insertIntoExistingTier` and `AllocationFinalizationAction`**:
  - Read the new node or object first. If it is gone, the action is done: there is nothing to record.
  - If it is present, lock its revision as just read (`lockRevision`) together with the per-write checked insert. A
    replay after deletion then fails, and a late first run cannot fail forever, since it reads the current revision.
  - A lock is not a pin, so both are marked `replaySafe`. The reasoning goes in the reason string and the scaladoc.
  - A deleted object fails a revision requirement on its stores. Step 1 confirms this; if it does not, stop and bring
    it back.
- **`JoinFinalizationAction.deleteFromExistingTier`**: `KeyRevision` (a pin).
- **`DeletionFinalizationAction`**: `KeyRevision` (a pin; my call, recorded in the analysis).
- **`SystemTaskServiceState.enroll` / `remove`**: per-write checks. If that leaves no pin, use the task state object's
  revision.
- **`SimpleTaskExecutor.prepareTask` / `deallocateTask`**: the task object's revision. Single writer, so there is no
  contention.
- **Node revision as the pin** (user's decision), where nothing else pins: `Registry.prepareRegister`,
  `MetadataTree.preparePut`, `SimpleTaskExecutor.allocateTask` and `SimpleDurableServiceExecutor.registerService`.
- **Anything else the check finds** (step 3).

**amoebafs:**
- **Directory insert** (`SimpleDirectory.prepareInsert`): `DoesNotExist` for an absent name, `KeyRevision` for a
  present one. A transaction without the incref inode overwrite gets its pin from its other writes.
- **Unlink** (`prepareDelete`): `KeyRevision` on the entry, which is the pin.
- **Rename in one directory**: `KeyRevision` on the old key (the pin), and `KeyRevision` or `DoesNotExist` on the new
  key.
- **Cross-directory move** (`AmoebaNFS.move`): the same, and its prepare futures are awaited (section 6).
- **Content and directory deletion tasks, and task enrollment**: the task object's revision.

**vdisk:**
- **`DiskCatalog.delete` → `removeKey`**: `KeyRevision` from its `tree.get`.
- **`DiskCatalog.create`**: the registry node's revision.

Nothing else in vdisk changes.

### 5. Removing the horizon

**Delete:**
- `Host.txHorizon`;
- the `horizon` parameter of `Store` and `Frontend`, and its passing in `StoreLoader`;
- the age test in `Frontend.receivePrepare`;
- `TransactionTooOld`;
- the early-eviction warning machinery in `TransactionStatusCache` (`Entry.insertedAt`, `EarlyEvictionWarnEvery`, the
  counters, `evicted()`, the listener branch);
- their tests.

**`txStatusCacheDuration`** is unchanged. It still has to outlast `Host.txHeartbeatTimeout`.

**Keep the `tooOld` path in `Tx`** only as far as `muted` (rebuilt stores, `acceptorSince`) needs it, and rename it
(builder's choice). A muted Tx still proves or forgets. Its proof is a proof like any other, so 1b and 1c apply.

**Proof ordering.** Proof-first-before-requirements existed only past the horizon. It now runs only on the abort path,
as it does for young prepares today.

**Accepted cost:** a replayed prepare is checked normally. Where it passes, it holds locks until a recovery driver
resolves it.

### 6. Incidental fixes

These sit in code this change touches anyway (my call):
- **`AmoebaNFS.move`** (`amoebafs/nfs/AmoebaNFS.scala:370-379`): await `prepareDelete`/`prepareInsert` before the
  body's future completes.
- **Duplicate typeUUID:** give `DeleteFileContentTask` or `DeleteDirectoryContentTask` a fresh one. They share
  `c1fb782f-…`.

**The type registry missed the duplicate** (user). `TypeRegistry.apply` (`client/TypeRegistry.scala`) throws
`DuplicateTypeRegistration` for two different factories with one UUID. Even so, the shared UUID in amoebafs's
`StaticTypeRegistry` was not caught when the system initialized. Find out why. One possibility is that
`StaticTypeRegistry.types` never passes through `TypeRegistry.apply`, or not on every path that loads amoebafs types.
Repair the detection so any duplicate among the registered type lists fails initialization, naming both classes, the
way `UserJarLoader` reports jar duplicates. Write a test that registers the amoebafs types and would have caught it.
Fix the detection first, see it report the existing duplicate, then give one task a fresh UUID.

The analysis's unverified findings go to `Todo.txt`, not this change:
- `UnlinkFileTask` step 2;
- DataObject `deleteList` using `lockRevision`;
- `JoinFinalizationAction.setNewRoot`;
- joins triggered through repair;
- `LocalTimeRequirement` `Equals`.

## Implementation order

Each step is test-first.

1. **Confirm the premises.**
   - A test showing that a replayed client prepare of a pinned transaction with a blind KV delete, delivered after
     the key was re-inserted and the status cache expired, deletes the new key through proof-of-commit. It is
     expected to fail today. Use the `TestNetwork` style, or `FrontendSuite`/`TransactionDriverSuite` if simpler.
   - Establish that a store forgets a committed transaction only after `TxFinalized`, and that `TxFinalized` follows
     the finalizer's completion.
   - Establish that a deleted object fails a revision requirement on its stores.
   - If any premise is false, stop and bring it back.
2. **UnackedCommits** (1a): the rename, entries for every committed transaction, and the recovery lookup for every
   recovered commit vote (1d).
3. **Late commits** (1b, 1c): the driver's late conditions and its no-abort-on-proof rule, the `late` flag on
   `TxResolved`, `TxFinalized` and the status cache, the receiver rule, the prover applying nothing, and no
   finalization actions on a late commit. The step-1 test passes.
4. **The pin check** (section 2). Turn it on and run the full suite.
   - Each `UnpinnedTransaction` failure is a pinless site in product code or a test.
   - Product sites are converted in steps 5-6.
   - Test transactions get a natural pin, or `replaySafe("test")` where the test is not about replay.
   - Keep the list and compare it with the analysis's inventory.
5. **TKVL per-write defaults** (section 3), with the blind opt-in.
6. **Site conversions** (section 4): core, then amoebafs, then vdisk.
7. **Horizon removal** (section 5).
8. **Incidental fixes** (section 6).
9. **Docs:**
   - **CLAUDE.md, Transaction Design:**
     - replace the horizon paragraph with the pin rule and `replaySafe`;
     - rewrite the proof-of-commit and TracelessCommits paragraphs for UnackedCommits and late commits;
     - update the pool metadata layout line (the tree name);
     - update the TKVL note on `requireDoesNotExist`/`set` defaults.
   - **The tkvl CLAUDE.md**, if it describes put/delete requirements.
   - **`Todo.txt`**: replace the first item; add the unverified findings.
   - **The traceless-commits design and the analysis:** note them as superseded where this design changes them.
10. **Refresh `docs/safety/*`** (user), as the last step, once the code is final. Revise every document in the
    directory to match the system as built:
    - `README.md`
    - `paxos-transactions.md`
    - `transaction-recovery.md`
    - `transaction-requirements.md`
    - `repair-process.md`
    - `crash-recovery-log.md`
    - `rebuild-process.md`
    - `systemic-loss-protection.md`
    - `cross-document-review.md`

    The refresh does three things:
    - **Remove what no longer exists**: the horizon, `TransactionTooOld`, the TracelessCommits name and proof-first
      ordering.
    - **Re-assess each existing finding against the new code**, and mark it fixed, narrowed or unchanged with the
      reason. Expected: Paxos F3 / Recovery F7 and X3 addressed. Paxos F2 narrowed, because the driver no longer
      decides abort while it holds a proof. Requirements F1 and Recovery F4 narrowed wherever per-write checks apply.
      These must be confirmed against the code, not assumed.
    - **Look for new weaknesses this design introduces**, each recorded as a new finding with a verdict. Candidates:
      - the late-commit evidence rule (quorum memorylessness and finalized-status reports);
      - the no-abort-while-holding-a-proof rule and its liveness;
      - UnackedCommits growth and lookups;
      - `replaySafe` claims;
      - derived per-write checks on TKVL;
      - finalization actions that read before writing;
      - the residual risk named below.

## Testing

- **Late commits:**
  - The step-1 scenario leaves the new key on every store.
  - A driver holding a proof does not decide abort. It resolves late with a quorum of memoryless responses or a
    finalized status report, and normally when it learns an accepted value.
  - In the live window (an acceptor quorum remembers) a proof leads to a normal commit with finalization actions run.
  - A late commit runs no finalization actions and carries `late = true` on everything after.
  - A Tx with its own commit vote applies a late commit when its entry exists, and applies nothing when it does not.
    A failing lookup holds and retries.
  - A Tx with no commit vote, and the prover, apply nothing, send no `TxCommitted`, and are forgotten.
  - A status answer for a late commit leads the asking driver to a late resolution.
- **UnackedCommits:**
  - Entries are written for non-acknowledging voters of non-traceless transactions too.
  - The existing TracelessCommits tests pass under the new names.
  - A recovered commit vote of a non-traceless transaction with an entry applies on load.
- **Pin check:**
  - Each pin form is sent.
  - Each non-pin form fails with `UnpinnedTransaction`: allocation only, `RevisionLock` only, `DoesNotExist` +
    insert, `Exists` + delete, and an identity refcount update.
  - `replaySafe` passes a pinless transaction and records its reason in the notes.
- **TKVL:**
  - Puts get derived `KeyRevision`/`DoesNotExist`, and deletes get `KeyRevision`.
  - A stale derived check aborts and the retry succeeds.
  - The blind opt-in still upserts.
- **Finalization actions:**
  - A second execution after the first committed succeeds with no change.
  - A late first run after the object was updated still completes.
  - A run after the object was deleted completes without writing.
- **Replay integration** (`TestNetwork`): for one pinned transaction of each kind (KV delete, KV insert, data
  overwrite), re-deliver its client prepare after the status cache expired and after a conflicting later write. State
  is unchanged everywhere. This replaces the horizon's integration coverage.
- **Lagging store:** a store that missed T's prepare and applied a later T2 to the same key skips T's stale per-write
  checked write, is marked, and is repaired.
- **Horizon removal:** its tests go, and no test depends on a time-based abort.
- **Full `sbt test`:** 70 s hang bound, no flaky tests, no new "Hung Test" prints. Watch for new collision retries in
  TKVL suites.

## Decisions

Made by the user:
- **The pin rule:** one pin per transaction, enforced by the client. An explicit mark is needed to send a pinless
  transaction; the ErrorTree batch is the model exemption.
- **Client-side enforcement only.**
- **Owed commits (option 1):** a late commit is applied only by a store that holds its own commit vote and has an entry
  recording that it owes the commit. Correctness over performance on this path.
- **The horizon:** removed entirely, with no age-based early abort.
- **Node-revision pins** for registries, metadata trees, `DiskCatalog.create`, service registration and task
  allocation. No ABA-free `DoesNotExist`.
- **Per-write checks** are the default wherever they are free.
- **Traceless operations stay**, and their tree is generalized rather than removed.
- **The `pin_refactor` branch.**
- **Repair the type registry's duplicate detection**, which missed the shared amoebafs UUID.
- **Refresh `docs/safety/*`** at the end: re-assess every finding, and look for new weaknesses.

My calls, for review:
- **When a commit is late** (1b): a quorum of memoryless acceptors plus a proof, or a finalized status report. A
  driver holding a proof never proposes abort and keeps running Paxos until one of those holds.
- **No finalization actions** on a late commit.
- **The `late` flag** on `TxResolved`, `TxFinalized` and status cache entries.
- **Renaming** TracelessCommits to UnackedCommits.
- **The recovery lookup** for every recovered commit vote, not only traceless ones.
- **The `replaySafe(reason)` name**, with the reason in the notes.
- **The split and allocation finalization actions:** read first, lock the current revision, marked `replaySafe`.
- **`KeyRevision` for `DeletionFinalizationAction`.**
- **The incidental fixes:** two folded in, the rest to Todo.

## Left to the builder

- The name and shape of the blind opt-in in the TKVL APIs.
- What the `tooOld`/`muted` path is renamed to.
- How each finalization action recognises its own already-committed effect.
- Whether a test transaction gets a pin or `replaySafe("test")`.
- How "memoryless" is reported in prepare responses (an accepted value is already carried; whether more is needed).
- How the `late` flag threads through `TransactionDriver`, including recovery drivers and `sendPrepareMessage`.

## Residual risks

- **A recovering acceptor plus a replay.** An acceptor that still holds T's accepted commit, such as one offline
  longer than the status cache window and recovering, can let a driver learn a *non-late* commit from that accepted
  value when no other evidence exists (no proof, because the pin object changed since T). A replayed client prepare
  arriving during that recovery then applies its writes over newer state. Per-write checked writes are skipped by the
  apply-time re-check. Pin-only blind writes and `DoesNotExist` inserts after a later delete are not. This requires a
  long outage, a delayed duplicate client prepare, and a pin object changed since, all at once.
- **Marked transactions stay replayable.** A `replaySafe` mark is a claim the code cannot check; a wrong one brings
  replay back for that site. The reason string keeps such claims reviewable.
- **Lagging-store order and CRL re-apply** remain wherever a write is pin-only rather than per-write checked.
- **More retries on contended keys**, from derived per-write checks.
- **Untracked bootstrap-pool marker batches** get no entries, as today. Losing one costs an extra repair pass.

## Amendments during implementation

Decisions made while building that changed or refined the text above. Where they conflict, these win. Only
Ruling 4 is omitted (superseded by 12); Ruling 16 is included as read by Ruling 19.

- **1 (proof field).** `TxResolved` carries an explicit `proof: Boolean`, set only by `Tx.proveCommitted`. Inferring a
  proof from `(committed, late=false)` misclassifies ordinary resolutions from other drivers.
- **2 (proof vs Paxos; refined by 29).** "Never propose abort while holding a proof" constrains only the driver's local
  proposal. An accepted value adopted from promises is not overridable without breaking safety; an adopted abort
  resolves normally. Ruling 29 narrows this: a proof holder never adopts an accepted abort.
- **3 (muted path).** The muted path keeps its behavior (prove or forget, no vote); no new error type.
- **5 (late via relay).** Late condition 2 ("a store reported T finalized") is the existing finalized relay: a
  `TxFinalized` relayed from a finalized cache entry carries `late = true`. No status-request path was added.
- **6 (late condition 1).** Late condition 1 counts only promises to prepares sent after the proof arrived. On a proof
  while unresolved the driver starts a new, higher-proposal round and evaluates the memoryless-quorum condition on that
  round alone, since earlier promises could predate another driver's later commit choice.
- **7 (adopted value; refined by 29).** A proof-holding driver with a prepare quorum whose promises carry an accepted
  commit sends accepts for commit without waiting for the vote tally, since provers never vote.
- **8 (finalizer order; amends 1a).** The finalizer runs `MissedUpdateFinalizationAction` only after every other
  finalization action of T, so a durable UnackedCommits entry implies all other actions are done. Forgetting on a found
  entry and skipping actions on a late commit are then safe.
- **9 and 12 (found entry; amend 1c and 1d; 12 supersedes 4).** `unackedCommitFound` applies the update and resolves
  committed but does not forget: the Tx stays until TxFinalized (or a later abort/late outcome), and writes no status
  cache entry. This avoids cross-pool atomic batches (marker batches are per pool) and stops a sibling store on the host
  relaying a recovery-time Committed entry to a fresh replay.
- **10, 11 and 14 (prover; amend 1c).** A prover applies nothing, records nothing in the status cache, casts no commit
  vote, and stays a live Tx: its first answer is the proof; later prepares get a memoryless promise plus an abort vote
  saved to the CRL (muted provers stay proof-only). On any real outcome it applies nothing, sends no TxCommitted and is
  forgotten, and only that message writes the cache. `Tx.receiveResolved` and Frontend's no-Tx path ignore
  `proof = true`. A held post-proof promise sent after load carries an abort vote, which stopped a TestNetwork hang.
- **13 (accepted cost).** A lingering found Tx whose heartbeat driver adopts an accepted commit re-runs T's finalization
  actions; they are at-least-once and read-first or replay-safe.
- **15, 20 and 21 (found entry as proof; lookup ordering).** A found entry is a proof for its own Tx's driver (never
  proposes abort, runs a post-proof round, resolves late on a memoryless quorum), which also covers an original driver
  that crashed mid-marker. No driver starts for a Tx before its first lookup answers, nor while the store's CRL state
  loads (`Frontend.recoveryPending`). `Store.driveTransaction` delivers the proof before replaying cached prepare
  responses.
- **16 and 19 (when a Tx proves).** A Tx proves only with no accepted value and no recovered Paxos state; a promise it
  gave itself in this incarnation does not block. A prover forgets only on TxFinalized or a late outcome.
- **18 (late cached finalized).** A late `TxResolved` for an unknown transaction is cached `finalized = true,
  late = true`. The invariant: finalized comes only from a real TxFinalized or a late outcome.
- **22 (retry helpers).** `transactUntilSuccessful*`, `retryUntilSuccessful` and `ExponentialBackoffRetryStrategy`
  treat `UnpinnedTransaction` as permanent, so a missed site fails instead of hanging.
- **23 (unlisted sites).** Sites outside the analysis get a real pin where one is free or a node-revision pin
  (`prepareSetPinned`); `replaySafe` only where replay is provably harmless, with the reason recorded.
- **24 (refcount 0).** `RequirementsChecker` treats a refcount-0 object as `MissingObject` for every requirement, on
  every backend, which the allocation-FA existence-lock argument (section 4) relies on.
- **25, 26 and 27 (system tasks).** `SystemTaskServiceState.enroll` pins on the system-task tree node revision, not a
  version bump of the task state (a bump would break a running task's steps on a replayed enroll). The services and
  system-task trees get node-revision pins too. `enrollInTx` stays a Checked set; the node pin is applied only by admin
  creators lacking another pin.
- **28 (allocation FA; amends section 4).** `AllocationFinalizationAction` never concludes "gone" from a read: if the
  object reads, Checked insert plus `lockRevision`; if not, Checked insert without the lock, `replaySafe`. The split
  action keeps read-first-gone. A read right after resolution can precede the stores applying the allocation.

- **29 (proof holder adopts only commit; refines 2 and 7).** A proof means commit was chosen at some ballot c, and
  every value accepted at c or higher is commit. So a proof holder adopts only an accepted commit reported in the
  current, post-proof round (`Proposer.commitProven`: the proposal is then commit or none; neither the local proposal
  nor an earlier round's value counts). A post-proof quorum reporting no accepted commit, an older accepted abort
  included, is memoryless and resolves late. Adopting such an abort resolved a committed T as abort.
- **30 (monotone status cache).** `TransactionStatusCache.updateStatus` is monotone, by an atomic compute: `finalized`
  and `late` are sticky, and a resolved status never reverts to Unresolved. A sibling store's delayed resolution, or a
  newly created Tx, can no longer downgrade a finalized entry and make the finalized relay answer non-late (X3).
- **31 (closed store's drivers).** `Store.close` shuts down the store's transaction drivers, so their timers stop
  re-preparing transactions for a store that is gone. Other stores' heartbeat recovery takes over.
- **Addendum.** Applied stubs (closing residual 17's late-acknowledged re-apply to a self-healing minority) and
  prefer-abort driver prepares are specified in `2026-11-09-replay-pins-addendum-design.md`, with its own amendments.
  Where it conflicts with residual 17 or the missing-data wait above, it wins.

Residuals recorded by the amendments:
- **17 (corrected by 32).** A store that applied T but acknowledged after errorTimeout keeps its UnackedCommits entry
  until its repair scan reconciles it, 30 s to 15 min later. A replayed client prepare reaching it meanwhile, with a
  passing commit vote, late-commits there and applies T's writes again, but only writes in a pool where T has no pin
  of its own: blind or `replaySafe` writes, ABA-prone `DoesNotExist` inserts, and allocations in another pool.
  Per-write KeyRevision-checked writes are pins in their pool and fail. When the whole pool acked after errorTimeout,
  every store holds an entry, so a quorum is exposed, and reconcile then follows the corrupted quorum. Not transient
  and not masked by quorum reads in that case. No closure is implemented; the choice is left to the user.
- **28.** A doubly-rare re-run of the allocation action after the object's deletion can leave an orphan allocation-tree
  entry.
