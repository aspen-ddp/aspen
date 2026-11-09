# Transaction Recovery

Status: derived from the code at commit `c3ad5493` (main). Every mechanism below was checked against the source; line
numbers are for that commit. Paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`.

Sibling documents (not duplicated here): [paxos-transactions.md](paxos-transactions.md),
[crash-recovery-log.md](crash-recovery-log.md), [transaction-requirements.md](transaction-requirements.md),
[repair-process.md](repair-process.md), [rebuild-process.md](rebuild-process.md),
[systemic-loss-protection.md](systemic-loss-protection.md). Design background for the traceless-commit machinery:
`docs/superpowers/specs/2026-11-08-traceless-commits-design.md` (consistent with the code except where noted).

## 1. Scope and the "two failure modes" claim

`Todo.txt` says there are two (to be verified) primary recovery modes: (1) crash of the transaction driver, (2) crash of
a transaction participant. That is a correct first cut but **not complete**. The code has these distinct recovery
situations, each with its own mechanism:

| # | Situation | Detector | Mechanism |
|---|-----------|----------|-----------|
| R1 | Driver (the Paxos proposer plus finalizer runner) is gone or silent, before or after resolution | A store's `Tx` sees no non-client event for `txHeartbeatTimeout` (5 heartbeats) | The store starts its own `SimpleTransactionDriver` (a "recovery driver") |
| R2 | Participant (store/host) crashes or restarts | Store load | CRL read, `Tx` rebuilt with `recovered = true`, messages held until loaded |
| R3 | `TxFinalized` is lost (driver and all other stores are fine) | Same idle detector as R1 | Same recovery driver; it learns the outcome from peers' status caches. Not a separate code path, but a separate failure with a separate timing constraint (`txStatusCacheDuration`) |
| R4 | Client crashes or goes silent, before or after sending prepares | Same idle detector, plus the data-wait timers | Same as R1; the client is only a data carrier and result recipient |
| R5 | Participant recovered a commit vote after the transaction was finalized and forgotten | `Tx.needsTracelessCheck` / horizon / proof-of-commit | `Tx.proveCommitted`, `TracelessCommitLookup`, `TransactionTooOld` |
| R6 | Participant missed the commit entirely (never applied, lost data, backend failure, slow) | Finalizer: no `TxCommitted` within `errorTimeout`, or `commitErrors` | `MissedUpdateFinalizationAction` writes ErrorTree and TracelessCommits entries; the repair scan applies them |
| R7 | Committed-but-unapplied updates held in the CRL | Store load | Replay of the recovered update through the normal commit path (resolution comes from a driver, a peer or the TracelessCommits tree) |

Verdict: R1 and R2 are the two *roots*; R3 and R4 are triggers that reuse R1's machinery; R5, R6, R7 are the
consequences of R2 and of forgetting transactions, and are where most of the subtle safety reasoning lives. The Todo
text should say "two primary modes (driver loss, participant loss) plus the consequences of forgetting a transaction
(missed updates and recovered commit votes)".

One structural fact makes R1 simple: there is **no persistent driver state**. A driver is a volatile object on a store
(`server/store/Store.scala:61`). All durable consensus state is the acceptor state in each store's CRL entry
(`server/crl/TransactionRecoveryState.scala:5`). A driver can be discarded and rebuilt from scratch at any time; Paxos
makes that safe.

## 2. Vocabulary and roles

* **Client**: builds the `TransactionDescription` (txd), sends `TxPrepare(fromClient = true)` to every store of every
  referenced pool, retransmits every `retransmitDelay` until it hears `TransactionResolved`/`TransactionFinalized`
  (`client/internal/transaction/SimpleClientTransactionDriver.scala:121`, `ClientTransactionDriver.scala:65`). The client
  has no role in consensus or recovery: it supplies per-store object data and is told the result.
* **Store/Tx**: each store keeps one `Tx` per live transaction (`server/transaction/Tx.scala`). It checks requirements,
  votes (commit/abort), persists its vote and Paxos acceptor state in the device CRL, is an acceptor if it is in the
  *primary object's pool*, holds requirement locks from commit vote to resolution, and applies committed updates.
* **Driver** (`server/transaction/TransactionDriver.scala`, `SimpleTransactionDriver.scala`): a Paxos proposer plus
  learner for one transaction, running on one store. It collects votes, proposes, learns the outcome, runs the
  finalizer, and broadcasts `TxResolved`/`TxFinalized`. The **designated leader** is the primary-pool store named by
  `txd.designatedLeaderUID`; it is the nominal driver and is started when it receives any `TxPrepare` for the
  transaction (`Store.scala:156`). Any other store may become a **recovery driver**.
* **Finalizer** (`server/RegisteredTransactionFinalizerFactory.scala`): created by a driver when it learns *commit*; it
  instantiates and executes every `FinalizationAction` named in the txd. `TxFinalized` is sent only after all actions'
  `complete` futures succeed (`TransactionDriver.scala:496`).
* **Status cache** (`server/transaction/TransactionStatusCache.scala`): one per **host**, shared by all its stores,
  keyed by transaction id only; `Entry(status, finalized)`; `expireAfterWrite(txStatusCacheDuration)`, max 4,000,000
  entries.
* **Horizon**: `Host.txHorizon` (`Host.scala:73`); the age (HLC now minus `txd.startTimestamp`) past which a store refuses
  to evaluate a prepare for a transaction it does not know.

## 3. Timers and their required relationships

Defaults in production (`cmdline/Main.scala:1724-1749`): `txHeartbeatPeriod` = 1 s; driver `initialDelay` 100 ms,
`maxDelay` ("txRetryCap") 3 s. In tests (`TestNetwork.scala:326`) recovery drivers use 10 ms / 100 ms.

| Timer | Value (prod) | Defined at | Role |
|-------|--------------|-----------|------|
| Store heartbeat period | 1 s | `Host.scala:284`, `Store.heartbeat` `Store.scala:67` | Drives driver `TxHeartbeat`/`TxFinalized` rebroadcast, collision-wait expiry, traceless lookup retry, idle-Tx check |
| `txHeartbeatTimeout` | 5 s (5 periods) | `Host.scala:60`, used `Store.scala:75` | Idle `Tx` (no non-client prepare, accept, resolved, finalized, heartbeat) gets a recovery driver. Observed with 1 s granularity, so actual start is 5-6 s after the last event |
| `failedDriverDuration` | 6 s (`2 * maxDelay`) | `SimpleTransactionDriver.scala:48`, `SimpleDriverRecoveryMixin.scala:18-28` | Second, redundant trigger (500 ms poll). Because 6 s > 5 s the heartbeat path fires first; this mixin is effectively a backstop |
| `txStatusCacheDuration` | 7 s (7 periods) | `Host.scala:66` | Lifetime of an outcome entry after the last write |
| `txHorizon` | 6 s (cache - 1 s) | `Host.scala:73`, used `Frontend.scala:655` | Unknown prepare older than this, with no cached outcome: proven committed or abort `TransactionTooOld` |
| Driver rebroadcast | `maxDelay` (3 s) | `SimpleTransactionDriver.scala:113`, backoff `:116-127` | Retransmits prepare/accept/resolved; Paxos-contention backoff is randomized in `[0, backoff)` doubling to `maxDelay` |
| `Driver.heartbeat` | every store heartbeat | `TransactionDriver.scala:184` | `TxHeartbeat` to all stores while not finalized (first tick sends none); `TxFinalized` while finalized |
| `errorTimeout` | 2 s | `MissedUpdateFinalizationAction.scala:138` | Finalizer waits this long for `TxCommitted` before marking silent stores |
| `Tx.CollisionWait` | 500 ms (+ up to 1 s heartbeat granularity) | `Tx.scala:26` | Votes withheld for a lock holder the Tx builds on |
| `Frontend.MissingDataWait` | 5 s | `Frontend.scala:27`, used `Tx.scala:146` | Vote withheld waiting for the client's per-store object data |
| `Frontend.TracelessLookupTimeout` | 5 s | `Frontend.scala:32`, `:396-406` | Re-issues an unanswered TracelessCommits lookup |
| `Tx.StuckReportAfter/Every` | 2 s / 5 s | `Tx.scala:20` | Logging only |

Required relationships (the code states the first three; the rest I derived):

* **TR1** `txStatusCacheDuration > txHeartbeatTimeout + heartbeat granularity + message latency`. Stated in the
  `Host.scala:62-66` comment. Actual margin with defaults: 7 s vs 5-6 s + RTT, about 1 s. See flaw F5.
* **TR2** `txHorizon < txStatusCacheDuration`, margin 1 s "for clock skew" (`Host.scala:69-73`). A prepare that misses
  the cache is older than the cache duration *since finalization*, and finalization is after start, so the age since
  start is strictly larger than 7 s, hence above the 6 s horizon, if clocks agree to 1 s. See F7.
* **TR3** `errorTimeout < txHeartbeatTimeout`. The comment at `MissedUpdateFinalizationAction.scala:134-137` says so.
  The reason is weaker than stated: while a driver is alive its `TxHeartbeat`s keep peers' `lastEvent` fresh
  (`Tx.scala:854`), so a long finalizer does not by itself provoke takeovers. The real constraint is that a *dead*
  driver's finalizer is restarted by a recovery driver after 5-6 s, and the original `errorTimeout` timer must not be
  the thing that delays the takeover's finalizer. With 2 s there is room for one full retry of the mark.
* **TR4** `CollisionWait` (0.5-1.5 s) `< MissingDataWait` (5 s) `< ` the client's patience; both are vote-withholding
  timers, bounded so that a driver never waits on them past `txHeartbeatTimeout`.
* **TR5** Driver silence must be shorter than `txHeartbeatTimeout`. The driver comment (`TransactionDriver.scala:181-183`)
  claims at most two periods of silence; `maxDelay` (3 s) rebroadcasts are independent of heartbeats.
* **TR6** Status-cache entries must survive until every store that could still be idle has taken over: this is TR1.
  Early size evictions (`TransactionStatusCache.scala:209-224`) reopen the window and only log, rate-limited.

## 4. Driver failure and takeover (R1, R3, R4)

### 4.1 How a recovery driver is created

1. `Store.heartbeat()` (`Store.scala:67-78`), once per heartbeat period on the store's executor, walks
   `frontend.transactions`. A `Tx` whose `durationSinceLastEvent > txHeartbeatTimeout` and which has no driver in
   `transactionDrivers` gets `driveTransaction(tx.txd)`.
2. `SimpleDriverRecoveryMixin.handleRecoveryEvent` (`SimpleDriverRecoveryMixin.scala:18-28`), every 500 ms on the control
   thread, does the same with `failedDriverDuration` (6 s), via `onEachStore`.
3. `Store.driveTransaction` (`Store.scala:84-96`) is idempotent per store (`!transactionDrivers.contains`). It creates the
   driver through `txDriverFactory`, and replays any `TxPrepareResponse`s cached in `prepareResponseCache`
   (10 s, 1000 entries) for that transaction.
4. The designated leader's driver is created by the first `TxPrepare` it receives for a transaction that is not
   finalized in the status cache (`Store.scala:156`); this includes client retransmissions, so a restarted leader gets a
   driver back the next time the client retransmits.

`Tx.lastEvent` is updated by: non-client `TxPrepare` (`Tx.scala:692`), `TxAccept`, `TxResolved`, `TxFinalized`,
`TxHeartbeat`, and a traceless-found conclusion. It is **not** updated by client prepares (`Tx.scala:689-693`: otherwise
a transaction whose leader is offline would never look idle), nor by `TxCommitted`/`TxStatusRequest`. A new `Tx` starts
with `lastEvent = now`.

Driver start differs by role (`SimpleTransactionDriver.scala:77`): the designated leader starts at proposal
`(1, leaderIndex)`, the same id the client's initial prepare carries (`ClientTransactionDriver.scala:77`), so the client
prepare doubles as the leader's Phase 1. Any other driver calls `nextRound()` immediately, taking a proposal id above
anything it has seen, and delays its first send by a random `[0, backoff)`.

Suppression of duplicate recovery drivers is probabilistic, not exclusive. The first recovery driver's non-client
`TxPrepare`s reset the other stores' idle timers (`Tx.scala:692`), so usually one driver emerges. If several start, Paxos
arbitrates (4.3). The extra drivers cost messages and each runs its own finalizer if it learns commit (TODO at
`TransactionDriver.scala:482`).

A driver is discarded only when its store receives a `TxFinalized` for the transaction (`Store.scala:181-184`;
`onFinalized` also calls `shutdown()` which cancels its timer, `TransactionDriver.scala:392`). There is no timeout, no
cap, and no cancel on store close (see F12).

### 4.2 How the outcome is learned (without having been the proposer)

A recovery driver sends `TxPrepare(fromClient=false)` with a fresh proposal id to every data store of the transaction
(`TransactionDriver.scala:408-422`). Each receiving store answers in one of these ways, in this order
(`Store.scala:131-158`, `Frontend.scala:630-666`):

1. **Status cache hit, finalized** (`Store.scala:144`): reply `TxResolved(outcome)` and `TxFinalized(outcome)`, and also
   `TransactionFinalized` to the originating client. The driver then runs `receiveTxFinalized` and finishes
   (`TransactionDriver.scala:363-372`) **without running any finalization action** (see invariant T6).
2. **Status cache hit, resolved not finalized**: reply `TxResolved(outcome)`. The driver treats it as resolution
   (`onResolution(sendResolutionMessage=false)`) and, if commit, runs the finalizer itself.
3. **Live `Tx`** (`Frontend.scala:632-633`): the existing `Tx.receivePrepare` answers with its stored vote and acceptor
   promise (`Tx.scala:730-749`; the response is sent only after the CRL save of the acceptor state is durable). A `Tx`
   that already applied the commit also resends `TxCommitted` to the proposer (`Tx.scala:695-699`).
4. **Unknown to the store, no cache entry**: a fresh `Tx` is created (`Frontend.scala:649-665`). If
   `HLC.now - startTimestamp > horizon` (`tooOld`) it never checks requirements: it answers with
   `TxResolved(committed)` if an object it hosts already shows the transaction's update (`RequirementsApplyer.appliedBy`,
   `Tx.proveCommitted`, `Tx.scala:516-522`), otherwise votes abort with `TransactionTooOld` (`Tx.scala:444-449`). Younger
   than the horizon it evaluates requirements normally.
5. **Unknown, cache says resolved-unfinalized**: a fresh `Tx` is created with `outcomeDecided()`; it takes no locks and
   learns the real outcome from the TxResolved already answered by `Store.handleTransactionMessage` or from `TxFinalized`.

Whichever route, a driver learns commit/abort one of three ways: (a) Paxos `Accepted` quorum (`learner.finalValue`,
`TransactionDriver.scala:335-337`), (b) a peer's `TxResolved` (`receiveTxResolved`, `:345-353`), (c) a peer's
`TxFinalized`. A recovery driver that has to run Paxos reports the previous highest accepted value from promises
(`Proposer.proposalValue`, `common/paxos/Proposer.scala:27-35`), so a value chosen before the takeover is preserved.

### 4.3 Proposal-id competition

Proposal ids are `(number, proposerPoolIndex)` (`common/paxos/ProposalId.scala`). A driver that receives a `TxPrepare`
from a competitor calls `proposer.updateHighestProposalId` (`TransactionDriver.scala:226-228`); a `Nack` triggers
`onNack` -> `nextRound()` with a randomized exponential backoff capped at `maxDelay`
(`SimpleTransactionDriver.scala:116-128`). Acceptors are the primary pool's stores only (`isValidAcceptor`,
`TransactionDriver.scala:144`); `TxAccept` goes only to those (`:443`), prepares go to all data stores of all pools so
that every store votes and takes locks. The Paxos layer is covered by paxos-transactions.md. Recovery-specific points:

* Vote values (`VoteCommit`/`VoteAbort`) are fixed once given and are persisted before the response is sent
  (`Tx.scala:742`), so two drivers tallying the same votes compute the same proposal (the commit rule requires
  `width - nAbort >= writeThreshold` for every object and a commit-vote quorum or all replies, `TransactionDriver.scala:256-304`).
  The decision function is monotone and cannot produce commit in one view and abort in another of the same fixed votes
  (nCommit_true + nAbort_view >= width + 1 would be needed). This is what makes reuse of the leader's initial proposal id
  `(1, leader)` after a leader restart harmless in practice, although it is not textbook Paxos.
* A `TxPrepareResponse` for a stale proposal id is ignored (`:235`). A `Nack` to an accept also restarts the round.
* `sendPeerMessages` (`SimpleTransactionDriver.scala:83-114`) re-sends prepares periodically even while accepting, so
  stores that finalized and forgot the transaction, and therefore silently ignore accepts, restart it.

### 4.4 The finalizer and at-least-once semantics

`onResolution(committed=true)` (`TransactionDriver.scala:471-503`): creates the finalizer (executing every action's
`execute()` in the constructor, `RegisteredTransactionFinalizerFactory.scala:271`), hands it `commitVoters`
(`countedCommitVoters`, `:464-469`) and the `commitErrors` collected so far, and `f.complete foreach onFinalized`.
`onFinalized` sends `TxFinalized(outcome)` to every data store and `TransactionFinalized` to the originating client
(`:376-400`). Abort resolution runs no finalizer and finalizes immediately (`:502-503`).

Guarantees:

* **At-least-once**: `TxFinalized` follows only a successful `complete` of every action in some driver. Two drivers that
  both learn commit each run the actions (TODO at `:482`), and a driver that dies mid-finalizer is replaced by a
  recovery driver that restarts them from scratch. Actions must be idempotent (`CLAUDE.md`, and each shipped action
  is: allocation-tree insert/delete, TKVL split/join, ErrorTree upserts).
* **Not at-most-once**, and **no failure path**: `f.complete foreach` ignores a failed future, and a thrown exception in
  `finalizerFactory.create` escapes `onResolution` after `resolved = true` (F1, F2).
* **Finalization is the gate for forgetting**: stores delete CRL state and the status-cache `finalized` flag only on
  `TxFinalized` (`Tx.scala:790-800`, `Frontend.scala:537-540`). So the finalization actions, including the missed-update
  marking, complete before any store may forget the transaction, except for the cases in which a store forgets via the
  TracelessCommits path or a status-cache entry that expires (7 s).

## 5. Feeding missed updates into repair (R6)

### 5.1 Inputs the finalizer sees

* `updateCommitErrors(commitErrors)` is called with the accumulated map `store -> objects that store could not apply`
  each time a `TxCommitted` arrives (`TransactionDriver.scala:355-361`) and once at creation (`:494`). `TxCommitted` is
  sent by a store only after **every** backend commit for the transaction on that store completed
  (`Tx.scala:604-613`, via `Frontend.backendOperationComplete`, `Frontend.scala:671`), or immediately if nothing needed
  committing; its payload is `objectCommitErrors` = objects uncommittable at apply time (`Tx.scala:580`) plus objects
  whose backend commit failed (`Tx.scala:867`). It is re-sent on every later prepare (`Tx.scala:695-699`).
* `commitVoters(voters)` (`TransactionDriver.scala:490`): `Some(set)` if the driver itself counted commit votes reaching
  the write threshold for **every** referenced object (`countedCommitVoters`), `None` if it learned the outcome from
  another driver or an accepted value.

### 5.2 `MissedUpdateFinalizationAction`

Added automatically to every transaction (`TransactionBuilder.scala:122`) unless `disableMissedUpdateTracking`
(`MissedUpdateFinalizationAction.limitTracking`, `:130-133`, used only on marker transactions that reference the pool
the entry is about).

* `execute()` schedules `markMissedUpdates` after `errorTimeout` = 2 s (`:56-60`).
* `updateCommitErrors` completes the action immediately and cancels the timer if **every** data store has reported and no
  report names a non-lock-only object (`:45-54`). This is the fast path: no ErrorTree or TracelessCommits write at all.
* `markMissedUpdates` (`:69-107`) builds:
  * ErrorTree `MissedUpdate(store, object, txd.startTimestamp, storePointer)` for every store with no `TxCommitted`
    (all its hosted objects) and for each object a store reported as an error; lock-only objects are never marked (`:36-39`).
  * TracelessCommits `MissedTracelessCommit(store, txid, TracelessCommitEntry(startTimestamp, objects))` for each store
    that (a) sent no `TxCommitted`, (b) is in `voters` if `voters` is known, and (c) hosts at least one `tracelessObjects`
    object (`RefcountUpdate`, or a `KeyValueUpdate` without required revision whose `deletesKeys` is set,
    `common/transaction/TransactionDescription.scala:94-98`).
* `MissedUpdateMarker.mark` (`client/internal/transaction/MissedUpdateMarker.scala:302`) groups by pool, merges entries
  by `(poolIndex, object)` keeping the newest timestamp (`:316-327`) and by `(poolIndex, txid)` (`:330-332`), and writes
  one transaction per pool at a time (`nextBatch`, `:335-353`) with at most 256 entries across both trees, ErrorTree
  entries through `pool.errorTree.prepareUpdates` and traceless entries through `pool.tracelessCommitsTree.prepareUpdates`
  in the same transaction (`:388-397`). The write is retried with exponential backoff until success; only
  `UnknownStoragePool` and `TransactionCreationError` give up (`:377-386`).
* The action completes (and so the transaction can finalize) only when the marker's future succeeds
  (`MissedUpdateFinalizationAction.scala:101-106`). If the marker gives up, the action never completes (F2).

### 5.3 What this guarantees, and what it depends on

* **T-M1** Every store that did not report a successful, error-free commit in time has an ErrorTree entry for every
  non-lock-only object it hosts, written durably (in a committed transaction) before `TxFinalized` is sent, therefore
  before any peer forgets the transaction. The entry's value is the transaction's start timestamp plus the object's
  store pointer, which is what a repair compares against (`encodeErrorEntry`, `:203`).
* **T-M2** Stores whose commit changes leave no trace and that may hold a counted saved commit vote have a TracelessCommits
  entry keyed `(poolIndex, txid)`, written in the same batch (so atomically with that batch's ErrorTree entries).
* A store that applied durably but whose `TxCommitted` was late/lost is still marked (a harmless extra repair pass:
  reconcile is exact and a store already at the committed state is left alone, `server/store/Reconcile`).
* The scan side (repair-process.md): `StoreRepairer.scan` skips TracelessCommits entries for transactions the store still
  holds live (`server/repair/StoreRepairer.scan` at `StoreRepairer.scala:219` via `Frontend.mayHoldTransaction`).
* Dependency inversion to be aware of: **finalization of any transaction in which a store goes quiet depends on a
  successful write to that pool's ErrorTree/TracelessCommits trees**, which needs a write quorum of the metadata pool.
  While that write is retried, `TxFinalized` is withheld, every store keeps the CRL entry and the host-wide
  status cache keeps no `finalized` entry.

## 6. Participant recovery (R2, R5, R7)

### 6.1 Load-time sequence

```
StoreLoader (control thread)                    store executor
---------------------------------------------   -------------------------------------------------
build Store with deferRecovery=true             (not yet owned)
  Frontend.awaitRecovery()  [awaitingCrl=true]
router.install(entry)                           ownership passes
no save file:  router.recoverDeferred(store)  ->  readAndRecover(store)
crl_save.log:  crl loaded -> router.crlLoaded ->  readAndRecover(store)
                                                  crl.readStoreState(storeId)  (non-blocking Future)
                                                  on success, on executor, if still installed:
                                                    Frontend.recover(trs)
                                                      recoverTransactions: for each trs, new Tx(recovered=true)
                                                        registered in `transactions`, then objects read
                                                      awaitingCrl=false
                                                      releaseHeldMessages() only when nothing is holding
```
Code: `StoreLoader.scala:327-395`, `StoreRouter.scala:209-262`, `Frontend.scala:279-329`.

While `holding` (`Frontend.scala:241`) the store: (a) queues transaction messages in arrival order and handles them after
(`:600-604`, `:408-413`); (b) drops client reads (`:729-733`, because a read before a recovered commit vote has taken its
locks would report the pre-commit state unlocked); (c) the host still answers `TxPrepareResponse`/`Accept`/`Finalized` etc.
for **drivers** because `Store.handleTransactionMessage` routes driver messages before `frontend.receiveTransactionMessage`
(`Store.scala:128-193`); (d) `Store.heartbeat` runs, so a recovered `Tx` with `lastEvent` = construction time can acquire a
driver 5-6 s later even if still loading.

A failed read or recovery fail-stops the store as a failed load (`StoreRouter.scala:222-252`,
`Host.StoreRecoveryFailed`), which ends in a rebuild (rebuild-process.md) - discarding the store's counted commit votes.

### 6.2 How a recovered Tx is rebuilt

`new Tx(trs, txd, ..., recovered = true)` (`Frontend.scala:289`) restores: the serialized txd, the saved object updates
(`trs.objectUpdates`), the persisted **disposition** (vote), the persisted **status**, and the **Paxos acceptor state**
(`Tx.scala:52,64,66,74`). It does *not* restore `oresolution` or `ofinalized` (both `None`, `Tx.scala:67-68`); the
persisted `status` only seeds the status cache (`:114`) and the `Unresolved` guards at `:128` and `:245`.

Ordering rules (`Frontend.scala:302-312`, `Tx.scala:368-418`):

1. Every recovered `Tx` is registered before any object read starts (so a synchronously loaded one cannot release the
   held messages early).
2. When all of a recovered `Tx`'s hosted objects have loaded (or failed to load), `Frontend.recoveredTransactionLoaded`
   runs `completeLoad`:
   * with a persisted vote: immediately. A recovered vote is kept as is, not recomputed (`Tx.scala:394-401`): "the vote may
     already have been counted, and recomputing it against state that changed since could contradict it".
   * without a persisted vote: only after every recovered `Tx` that has a vote has loaded (`:308-311`), so that
     recomputing cannot take locks that conflict with a recovered commit vote.
3. `completeLoad` calls `lock()` (`:244-249`): a recovered `VoteCommit` with `status == Unresolved` re-takes its
   requirement locks (`RequirementsLocker.lock`), so new transactions collide with it exactly as before the crash.
4. Held messages are released only once nothing is loading (`releaseHeldMessages`, `:408`).

### 6.3 Held messages and locks

* Messages for a recovered `Tx` that arrive before it has loaded are held (above). Messages for unknown transactions
  during recovery are held too, so a new transaction cannot load a shared object and take conflicting locks first.
* Locks live on shared `ObjectState` instances kept by `Frontend.referenced` so every transaction sees the same lock table
  regardless of cache behavior (`Frontend.scala:214-223,553-562`).
* The recovered `Tx` holds its locks until it learns the outcome (`unlock()` in `resolvedAndAllObjectsLoaded`, `Tx.scala:629`)
  or is forgotten; locks are released when a commit is applied in memory, while the *in-flight-commit marker* orders
  followers (`Frontend.scala:106-176`).

### 6.4 The replay of committed-but-unapplied updates

A recovered `VoteCommit` `Tx` that is later resolved committed (by a driver's `TxResolved`, by `TxFinalized`, or by
`tracelessCommitFound`) goes through the ordinary path: `resolved(true)` -> `resolvedAndAllObjectsLoaded(true)` ->
`doCommit()` (`Tx.scala:615-632`, `:527-602`). `doCommit` re-checks requirements with `applying = true` (locks ignored,
`RequirementsChecker.scala:62-70`) and skips objects that fail:

* `DataUpdate`/`VersionBump`/KV update with required revision: a required-revision mismatch skips the object. If the
  object already shows this transaction's revision it is logged as "already holds the committed update" and counted as
  applied (`Tx.scala:556-566`).
* Blind KV updates (no required revision, no key requirements) and `RefcountUpdate` do not self-protect against replay
  where the revision check cannot tell (F4).
* `saveObjectUpdates` is cleared when the store's backend commits complete (`Tx.scala:608-610`). `dropTransactionObjectData`
  is only an in-memory change to the CRL's in-memory entry (`server/crl/simple/SimpleCRL.scala:299,324`,
  `LogContent.scala:206-208`), so the on-disk record may still contain the data after the backend committed.
* A backend commit that fails keeps the CRL entry and the pinned state, marks the object `failedCommits`, and makes every
  later transaction on it vote abort (`BackendCommitFailed`) until the store reloads (`Frontend.scala:127-134,686-709`).

### 6.5 Recovered commit vote whose peers have forgotten the transaction (R5)

Hazard: store S voted commit (counted toward the write threshold), crashed before learning the outcome, and recovers
after the other stores finalized and forgot. S's recovery driver prepares them; they treat it as a new transaction and
would check requirements against *current* state. Two defenses, both on the **answering** side or the **recovering**
side:

**Answering side (peers).**
* Horizon (`Frontend.scala:651-656`): if no non-`Unresolved` cache entry exists and the txd is older than
  `Host.txHorizon`, the new `Tx` is `tooOld`: it never checks or locks. It does a *proof of commit*: if an object it hosts
  shows the transaction's update (`RequirementsApplyer.appliedBy`: revision equals the txid for data, version bump and
  KV-with-required-revision; a key whose revision is the txid for blind KV, `RequirementsApplyer.scala:50-63`), it replies
  `TxResolved(committed)` to the proposer (`Tx.scala:516-522`, `sendProvenResolution`) and gives no vote. Otherwise it
  votes abort `TransactionTooOld` (`Tx.scala:444-449`). A fresh abort vote of this kind can join the abort quorum, so the
  Paxos result can contradict a long-forgotten commit (see 6.6).
* Proof of commit is also tried on the abort path for younger prepares (`Tx.scala:491-493`).

**Recovering side (S itself).** `Tx.needsTracelessCheck` (`Tx.scala:127-129`): `recovered && disposition == VoteCommit &&
txStatus == Unresolved && oresolution.isEmpty`. Such a `Tx`:
* Looks up its own entry in its pool's TracelessCommits tree as soon as it loads (`Tx.scala:416-417`,
  `Frontend.requestTracelessLookup`, `:336-345`). The host performs the lookup with its client:
  `client.getStoragePool(pool).tracelessCommitsTree.get(TracelessCommitEntry.key(store, txid))` (`Host.scala:208-212`).
* Holds any **abort** resolution or abort `TxFinalized` until a lookup *started after the abort arrived* completes
  (`holdAbort`, `Tx.scala:808-818`; `Frontend.tracelessLookupComplete`, `:365-391`): `firstCounting` ensures that an
  in-flight lookup that began before the abort cannot clear it, because the entry may have been written after that read.
  A commit resolution is never held (`Tx.scala:771`).
* Entry found: `tracelessCommitFound` (`Tx.scala:825-842`): the `Tx` concludes as committed and finalized
  (`finalized(true)`): applies the saved update through `doCommit`, drops its CRL state once backend commits persist, and the
  Frontend forgets it (`Frontend.scala:374-378`). The tree entry is left for the repair scan.
* Entry absent: held aborts are accepted (`tracelessCommitAbsent`, `Tx.scala:847-852`; `abortsAccepted` stays set).
* Lookup failure: the latest lookup is marked failed and retried at the next heartbeat; an unanswered one is re-issued after
  `TracelessLookupTimeout` (5 s) (`Frontend.scala:380-406`). The `Tx` holds aborts for as long as it takes. A commit
  resolution is never held.

The TracelessCommits entry exists only for commit voters that sent no `TxCommitted` and whose transaction has traceless
changes (5.2). For changes with a trace, defense is peer proof-of-commit plus the ErrorTree mark and repair.

### 6.6 What happens if a forgotten commit is aborted anyway

If peers vote abort (`TransactionTooOld`) and S has no traceless entry (non-traceless change that has since been
overwritten, so no proof exists), the driver decides abort and S drops its update. S is safe only because S sent no
`TxCommitted`, hence the finalizer marked it in the ErrorTree before `TxFinalized` was sent, hence repair will bring S's
copy to the committed state. This is the key reason ErrorTree marking must precede finalization (T-M1).

## 7. State machines

### 7.1 `Tx` (store side), recovery-relevant view

```
            new Tx (prepare)  /  new Tx(recovered=true) from CRL
                    |
        load objects (pendingObjectLoads)  --- recovered: Frontend orders completions
                    |
     [Undetermined] --determineDisposition--> [VoteCommit | VoteAbort | withheld]
        withheld while: awaitingData (<5 s, no client data) / waitingOn holders (<0.5 s) / awaitingCommits (in-flight commit)
                    |
     CRL.save(vote + acceptor)  ---durable--->  TxPrepareResponse sent
                    |
        (VoteCommit: lock() requirement locks)
                    |
   TxAccept -> CRL.save -> TxAcceptResponse      (acceptor role)
                    |
   TxResolved(c) / driver / proof -> resolved(c): txStatus, statusCache(c, finalized=false)
        abort:  Frontend.deleteTransactionState (CRL entry dropped now)
        commit: commitAwaitsData? holdForTurn? -> doCommit -> backend commits -> TxCommitted -> (CRL data drop in memory)
                    |
   TxFinalized(c) -> finalized(c): statusCache(c, finalized=true); CRL entry deleted once backend commits ok
                    |
              Frontend forgets Tx
 recovered + VoteCommit + Unresolved: lookup TracelessCommits; abort held until a later lookup is absent;
                    entry found -> finalized(true) path above
```

### 7.2 Driver

```
 created (leader on first prepare | any store on idle Tx)
   -> PREPARE phase: send TxPrepare(proposalId) to stores that have not answered (every maxDelay)
   -> votes from all pools; tally per object (threshold rules at TransactionDriver.scala:256-304)
   -> local proposal fixed; ACCEPT phase to primary-pool stores (+ periodic re-prepare, sendCount % 3)
   -> learner: Accepted quorum   ==> resolved(c)         [or TxResolved from peer ==> resolved(c)]
   resolved(c): commit => finalizer.create (actions execute) ; abort => finalized immediately
   -> send TxResolved (all data stores + notifyOnResolution + client) ; keep re-sending TxResolved to stores not known resolved
   -> commit: wait finalizer.complete ==> finalized => TxFinalized to stores, TransactionFinalized to client
   discarded when this store receives any TxFinalized for the txid (Store.scala:181)
   Nack at any phase => nextRound() with randomized backoff
```

### 7.3 Finalizer (commit only)

```
 create: each action.execute()  ->  commitVoters(voters?) -> updateCommitErrors(accumulated)
   MissedUpdate: all data stores reported error-free? -> complete (no writes)
                 else after errorTimeout: markMissedUpdates -> MissedUpdateMarker (ErrorTree + TracelessCommits, one txn/pool)
                      -> complete on success; on permanent marker failure: never completes
   others (alloc/dealloc/TKVL): retryUntilSuccessful; Deletion action never completes on StopRetrying
 all complete -> driver.onFinalized
 cancel() on any TxFinalized received: actions stop retrying (best effort)
```

## 8. Invariants and guarantees

* **T1 (single-writer consensus)** A transaction has at most one decided outcome *while any acceptor of the primary pool
  still holds Paxos state for it*. Votes are fixed and persisted before being released (`Tx.scala:742,759-765`; a response
  is sent only after the CRL save completes). Therefore driver crash or takeover cannot flip an outcome that any
  acceptor quorum has chosen. This is paxos-transactions.md territory; recovery relies on it.
* **T2 (driver disposability)** No driver state is durable; a driver can be recreated by any store from the txd alone
  (the txd is in every prepare and in each CRL entry). A transaction with a quorum of live stores in every referenced pool
  and live primary-pool acceptors will eventually get a driver (T3).
* **T3 (liveness of takeover)** A `Tx` with no non-client event for `txHeartbeatTimeout` gets a driver on its store within
  one heartbeat period. Weakness: only stores that hold a `Tx` can drive; a transaction no store has heard of cannot
  be recovered and is simply never started (client crash before any prepare reaches a store: safe, nothing committed).
* **T4 (vote persistence)** A commit vote that counted toward a decision is in the voter's CRL until either the voter
  learns the outcome and finishes any backend commits, or the voter is rebuilt. It survives host and store restarts, and is
  re-locked on load before any new transaction can touch the same objects (6.2, 6.3).
* **T5 (recovered commit vote is not dropped silently)** A *recovered* `VoteCommit` `Tx` in `Unresolved` status cannot be
  aborted without a TracelessCommits lookup that began after the abort arrived and found no entry; if an entry exists it
  commits. Scope: recovered Txs only (see F3).
* **T6 (no finalization without finalizers)** `TxFinalized` is sent only by a driver whose finalizer completed, or
  re-sent by a store that answers from a `finalized` status-cache entry, which was recorded only after receiving a
  `TxFinalized`. Therefore a finalization action's work is complete (at least once) before any store may forget the
  transaction, except through cache expiry (7 s) or a store that was down.
* **T7 (finalization actions are at-least-once)** See 4.4. Idempotency is the action's duty.
* **T8 (missed-update recording precedes forgetting)** Every store that did not report an error-free commit before
  `errorTimeout` has an ErrorTree entry per non-lock-only object (and, for counted commit voters with traceless changes,
  a TracelessCommits entry) durably recorded before `TxFinalized`. The fast path (all stores report error-free) writes
  nothing.
* **T9 (forgotten transactions are not replayed)** A prepare for an unknown transaction, with no cached outcome, older
  than the horizon, is never checked or locked. Holds if (i) client and store HLC/clock skew < 1 s (TR2) and (ii) the
  status cache did not evict entries younger than the horizon for size (TR6).
* **T10 (missed commits are repairable)** A store that does not apply a committed update either recorded it in the ErrorTree/
  TracelessCommits trees (T8) or can only have stayed behind because the tree write failed permanently (F2).
* **T11 (apply exactly once, mostly)** A committed transaction's update is applied to an object at most once on a store
  provided the requirements include a revision check on that object; blind KV updates and refcount updates rely on
  other mechanisms (F4).
* **T12 (client independence)** The client can crash at any time without affecting safety. The only client-dependent
  quantity is object data: a store without it withholds its vote up to `MissingDataWait`, votes abort afterwards, and holds
  a commit decided without it until data arrives or `TxFinalized` (`commitAwaitsData`, `Tx.scala:165`).

## 9. Failure scenarios

Notation: L = designated leader store/driver; S = an arbitrary participant store; D2 = a recovery driver.

### 9.1 Client crashes

| Crash point | Outcome | Why safe / not |
|-------------|---------|----------------|
| Before sending any prepare | Transaction does not exist. Allocated objects were never created | Nothing durable on stores. Client's local pointers are lost: leaks nothing on stores |
| After prepare to a subset of stores, not to L | The stores that got it create `Tx`s; L starts no driver. After 5-6 s idle (client prepares do not count as events) the first idle store starts a recovery driver, whose data-less prepares reach L and the rest | Safe: T3. Stores that never got data withhold votes for up to 5 s then vote abort (`awaitingData`); the transaction may abort or commit with fewer stores; a commit decided without a store's data is held there until data/TxFinalized, then repair (R6) |
| After prepare to all stores, before result | Normal completion by L; client never learns | Safe. A later retry by a new client instance would be a new txid |
| After result (committed), before using it | Committed | Safe. The caller's state (e.g. the pointer to an allocated object stored in another object by the same transaction) is whatever the transaction wrote (guideline in CLAUDE.md) |
| Client survives but its TransactionFinalized/Resolved messages are lost | It retransmits prepares forever (logs "HUNG Client Transaction" every 3 retries). Stores that know the txid answer from cache/`Tx`. After the cache expires, those prepares are ignored or answered from `Frontend.receivePrepare` (finalized cache) or treated as unknown+tooOld (abort vote) - the client can get `TxResolved(abort)` for a committed transaction (F8) |

### 9.2 Store (participant) crashes at each step

| Crash point | State on restart | Outcome |
|-------------|------------------|---------|
| Before it created a `Tx` | none | Treated as an unreachable store. Others commit with threshold; finalizer marks it after `errorTimeout`; repair later |
| After creating `Tx`, before the CRL vote save is durable | none (CRL save callback not run, so no `PrepareResponse` was sent either) | As above. Safe because the vote was never released |
| After CRL vote save, before the response was sent | recovered `Tx` with vote | Vote re-sent on retransmitted prepare (`Tx.receivePrepare` -> `respondToPrepare`), locks re-taken on load. Safe |
| After accept saved (acceptor), before `AcceptResponse` | acceptor state in CRL | Paxos safe: the accepted value is reported in any later promise |
| After commit resolution learned, before applying | CRL: vote + saved object data; `Tx.status` possibly `Unresolved` (resolution is not persisted unless a later prepare/accept re-saved it, `Tx.scala:425,741,759`) | Recovered `VoteCommit`/`Unresolved`: lookup in TracelessCommits if the store was marked; its recovery driver or peers' `TxFinalized` supply the outcome; peers that forgot: proof of commit, or `TransactionTooOld` abort -> store drops its update and is repaired from the ErrorTree entry |
| After applying in memory, before backend durable | same as above | The backend has nothing for it. The store never sent `TxCommitted` (it is sent only after backend completion), so it is marked after `errorTimeout`. Recovery replays from the CRL data; requirements re-checked, so no double apply. Safe |
| After backend durable, before `TxCommitted` | backend has the update; CRL entry has data (drop is in-memory only) | May be marked (extra repair pass, harmless) and replays: requirement checks skip it (revision shows) except blind updates (F4) |
| After `TxCommitted`, before `TxFinalized` | backend has update; CRL still has the entry (data may still be on disk) | Recovered as `VoteCommit/Unresolved`. Driver or peers (cache) tell it committed; if abort decision comes from `TooOld` voters, it drops a Tx whose effect it already holds: harmless; no repair needed |
| After `TxFinalized` received, CRL delete queued, not durable | possibly the old entry | Same as the previous row |
| Rebuild instead of recovery (`StoreRecoveryFailed`, `rebuild-required`) | CRL entries dropped (rebuild-process.md) | Counted commit votes are lost; the store is rebuilt from peers. Safe only if the system never loses more than the write-threshold's tolerance (systemic-loss-protection.md) |

### 9.3 Driver-side failures

* **Designated leader crashes before resolving**: idle stores start recovery drivers after 5-6 s. Their prepares reset others'
  idle timers. D2 runs Paxos with a higher proposal id; the acceptors report any previously accepted value (`Proposer`).
  Outcome preserved (T1). The client keeps retransmitting; the restarted leader gets a driver on its next client
  prepare, competing harmlessly.
* **Leader crashes after resolving, before finalizer**: stores that learned `TxResolved` apply the commit. Idle timers
  expire, D2 starts, learns commit from `TxResolved`/accepted state, creates a **new** finalizer and runs it. Safe (T7).
  Stores that have not applied yet get applied by `TxResolved`.
* **Leader crashes during the finalizer / mark write**: D2 re-runs all actions. A partially written ErrorTree batch is a
  committed transaction, so is either entirely there or not; re-inserting the same keys is idempotent (the marker
  merges and overwrites with the same timestamp).
* **Host of a recovery driver crashes**: same as above; another idle store takes over after another 5-6 s.
* **Multiple simultaneous recovery drivers**: Paxos arbitrates; duplicate finalizers (T7). The extra drivers are
  discarded when the first `TxFinalized` reaches their store. If the finalizer of D1 is slow, D2 may start its own copy
  (extra ErrorTree writes, one per driver).
* **Driver lives but is partitioned from some stores**: stores it cannot reach go idle (no heartbeat), start drivers
  that can reach the others. Two live drivers for one transaction is normal.
* **Driver on a store that gets closed/transferred**: the driver is not shut down (F12).

### 9.4 Status-cache, horizon and time

* **Expired status cache** (>7 s since last write) at the stores that finalized: a late prepare creates a fresh `Tx`.
  - Older than horizon (6 s) and no cache: `tooOld`. Proven committed if an object shows the update; otherwise abort vote.
  - Younger than horizon but cache missing (possible only after a size eviction): requirements re-evaluated; may vote commit and
    replay. The cache warns. See F9.
* **Lost TxFinalized (R3)**: the idle `Tx` starts a driver at 5-6 s; the other stores answer from their cache (written at
  their finalization, 7 s lifetime) with `TxResolved+TxFinalized`; the driver finishes without finalizers. Margin about
  1 s (F5). If the margin is lost, the outcome is re-decided from scratch (proof-of-commit / abort votes), which may
  produce an abort for a committed transaction that only this store is waiting to forget (F5, F8).
* **Clock skew**: `tooOld` uses `HLCTimestamp.now` (the host's HLC, which only advances forward and is raised by observed
  timestamps, `common/HLCTimestamp.scala`) minus the client-assigned `startTimestamp`.
  - Client clock *behind* by more than the margin (or slow): fresh unknown prepares look older: spurious `TooOld` aborts
    (liveness only, client retries).
  - Client clock *ahead* by more than 1 s: a prepare 6 s + skew old looks younger than the horizon; if its cache entries
    expired (7 s) it is evaluated afresh: replay possible (F7).
  - Host HLC ahead (raised by observing a far-future object timestamp) while a client hasn't seen it: all that client's
    new transactions look old at that host until the client observes the later timestamp (self-healing via
    `ObjectTimestampError` retries).
* **Partition that heals after outcomes are forgotten**: a minority store S that holds a (live, not recovered) `VoteCommit`
  `Tx` and rejoins after more than 7 s: its recovery driver asks peers; they proof-of-commit or vote `TooOld` abort; S
  accepts the abort (no TracelessCommits protection since `recovered` is false, F3) and is repaired from its ErrorTree
  entry (S sent no `TxCommitted`). For a traceless change, S's TracelessCommits entry exists and the repair scan applies it
  after S forgets the `Tx` (6.5); the lookup-based protection is not used. This relies on repair being exact.
* **Finalization action that keeps failing**: the action retries itself with backoff forever (unless `StopRetrying`), the
  driver keeps heartbeating, stores keep the `Tx`, CRL entry and admission bytes; a crash of the driver's host hands the
  work to D2. If an action's future *fails* (not retries), `onResolution`'s `foreach` ignores it: the transaction is
  never finalized, ever (F2). Locks are already released (they release on apply), so other transactions proceed; CRL space is
  consumed (`CrlAdmission`) until finalization.
* **ErrorTree write that fails**: transient failures retry forever (exponential backoff); finalization of the transaction
  is withheld meanwhile and so is forgetting; permanent failures (`UnknownStoragePool`, `TransactionCreationError`) end the
  future with failure and the action never completes (F2). During the wait a store that is S-missed remains unrepaired.
* **Store missed `errorTimeout` but did apply**: it is marked (extra repair pass) - and gets a TracelessCommits entry
  if it was a counted voter. If it crashes before `TxFinalized`, its recovered `Tx` finds the entry and "applies" via
  `doCommit`; data updates are skipped by revision; blind KV/refcount may re-apply (F4). If it does not crash, the
  repair scan sees the store is current and deletes the entry.
* **Store applied but crashed before the backend commit**: covered in 9.2 (no `TxCommitted` sent, marked, replayed from
  CRL or repaired).

## 10. Suspected flaws and open questions

Confidence: H = high (the code path is directly readable), M = medium, L = low (needs a particular interleaving
or an assumption I could not verify).

> **Verification 2026-11-09: CONFIRMED. Severity: liveness.** RegisteredTransactionFinalizerFactory.create does getType(...).get (server/RegisteredTransactionFinalizerFactory.scala, create); it runs inside TransactionDriver.onResolution after `resolved = true` and before `finalizer =` and before queueTxMessages (TransactionDriver.scala:471-520), the exception escapes the handler; periodic TxResolved (SimpleTransactionDriver.scala:42-47) still tells stores, but no finalizer ever exists. Needs a host lacking a registered type (version skew, partial --load-jars).
> *Fix sketch:* Catch creation failure, leave resolved=false so the driver retries, and surface an error; or refuse to drive txds with unknown types.

**F1 (M-H) A host that cannot instantiate a finalization action wedges the transaction permanently.**
`RegisteredTransactionFinalizerFactory.create` does `client.typeRegistry.getType[FinalizationActionFactory](...).get`
(`RegisteredTransactionFinalizerFactory.scala:292`). With user-jar types (`--load-jars`) or a version skew, a host
lacking the type throws `NoSuchElementException` inside `TransactionDriver.onResolution` (`TransactionDriver.scala:488`),
after `resolved = true` was set (`:474`) and before `finalizer` is assigned and before the resolution messages are
queued (`:505-518`). The exception propagates out of the message handler, and the driver never retries creation
(`resolved` stays true). The commit is applied via later periodic `TxResolved` (`SimpleTransactionDriver.scala:89-92`) but
nothing ever finalizes: stores keep CRL entries, and every other driver that learns commit may hit the same exception if
it too lacks the type. Fix direction: treat creation failure as "not finalized yet, retry", or refuse to become a
driver for a txd whose types are unknown.

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (permanent hang; replica stays stale).** MissedUpdateFinalizationAction.markMissedUpdates completes its promise only in `mark(...).foreach` (MissedUpdateFinalizationAction.scala:~100-106); MissedUpdateMarker.writeBatch throws StopRetrying for UnknownStoragePool and every TransactionCreationError (MissedUpdateMarker.scala:140-160) and finished() completes the Queued promises with Failure, which `foreach` ignores. Finalizer.complete = Future.sequence of actions (server/RegisteredTransactionFinalizerFactory.scala), driver uses `f.complete foreach` (TransactionDriver.scala:496), so onFinalized never runs: stores keep CRL state and admission bytes indefinitely. Same for DeletionFinalizationAction.
> *Fix sketch:* Handle failure: on permanent marker failure log loudly, record the loss, and still complete the action so TxFinalized goes out.

**F2 (H) Finalization has no failure path: any failed finalizer future, including the missed-update marker's permanent
failure, hangs the transaction forever.** `f.complete foreach { ... onFinalized }` (`TransactionDriver.scala:496`) ignores
failure. `MissedUpdateFinalizationAction.markMissedUpdates` completes its promise only on success
(`:101-106`), and `MissedUpdateMarker.writeBatch` throws `StopRetrying` for `UnknownStoragePool` and every
`TransactionCreationError` (`MissedUpdateMarker.scala:377-386`); `DeletionFinalizationAction.execute` also drops the
failure (`DeletionFinalizationAction.scala:24-34`). Result: committed transaction, never finalized, every store keeps the
CRL entry and admission bytes forever (until rebuild), and the missed stores' ErrorTree entries are never written
(a replica stays stale). Restarting the driver reruns the same failing action. Also, a store whose pool is being deleted
(`UnknownStoragePool`) is exactly the permanent-failure case. Possible fix direction: separate "give up the mark but still
finalize" for missed-update marking, with the loss recorded.

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: stale-replica (repaired via the finalizer ErrorTree/TracelessCommits entry).** needsTracelessCheck requires `recovered` (Tx.scala:128). A live store S that was frozen/partitioned > ~10 s while the others committed, finalized and forgot: S's idle driver (Store.heartbeat, Store.scala:73-77) prepares peers; with no cache entry they are TooOld, vote abort unless an object shows the update (traceless changes cannot), S's driver tallies abort and its own live Tx accepts it (Tx.receiveResolved, Tx.scala:768-776), dropping an update that counted toward the write threshold. S was silent past errorTimeout so the finalizer marked it (ErrorTree + TracelessCommits entry), so repair restores it; the window is a stale replica serving reads, which for key deletes is the resurrection hazard of the traceless design doc.
> *Fix sketch:* Apply the traceless hold to live commit-voters too (needsTracelessCheck without `recovered`) and hold aborts until the lookup completes.

**F3 (M) Traceless protection applies only to `recovered` transactions, not to a live store that held a commit vote through a
long partition or pause.** `needsTracelessCheck` requires `recovered` (`Tx.scala:128`). A store S that voted commit and
stays up but is partitioned or paused longer than the 7 s cache window while the others commit, finalize and forget
will, when its own idle driver reaches them, receive `TransactionTooOld` aborts for a transaction with a traceless change
(no proof is possible), accept the resulting abort (`receiveResolved`, `Tx.scala:771`) and drop an update that counted
toward the write threshold. The design relies on ErrorTree/TracelessCommits repair for S. The original hazard described in
`docs/superpowers/specs/2026-11-08-traceless-commits-design.md` (a read quorum of stores lacking the delete resurrects the
key before repair) applies equally to this non-crash case. Trigger: GC pause, VM freeze, or a one-sided partition of
>6 s.

> **Verification 2026-11-09: PARTLY CONFIRMED / PARTLY REFUTED. Severity: stale-replica (blind KV only).** Confirmed for blind KV: see requirements F2 (SimpleCRL.scala:296-299). REFUTED for refcount and DataUpdate: checkRefcount compares the whole ObjectRefcount including updateSerial (RequirementsChecker.scala:checkRefcount) and checkRevision the revision, so a replay is skipped, which instead lands in objectCommitErrors -> TxCommitted with errors -> a spurious (harmless) ErrorTree mark. Answers the doc's open question: the on-disk data is NOT guaranteed removed before TxCommitted is released.
> *Fix sketch:* Make the data drop durable, or compare ValueState timestamps on apply.

**F4 (L-M) Replay of a blind KV write or refcount change from a CRL entry that was already applied.** The applied-data
drop is only in memory (`SimpleCRL.scala:299`, `LogContent.scala:206`), so a crash after the backend commit but before the
transaction's CRL entry is rewritten leaves the update data on disk. Recovered T is `VoteCommit/Unresolved`; if a later
transaction T2 overwrote the same key on that store before the crash (allowed as soon as T's commit persisted,
`Frontend.scala:106-176`) and T is then resolved committed by a driver within the status-cache window, `doCommit`
re-checks with `applying = true` and a blind `Insert`/`Delete` passes (`RequirementsChecker.scala:62-70` skips locks;
`KeyValueUpdate` without required revision has nothing to compare), so T's value overwrites T2's on this store only
(`RequirementsApplyer.scala:112-124`). Open question: is the on-disk data guaranteed removed (or the entry guaranteed to
be deleted) before TxCommitted is released? I could not find it in this scope; crash-recovery-log.md should answer.
Also applies to a TracelessCommits hit by a store that already applied (`tracelessCommitFound`).

> **Verification 2026-11-09: PLAUSIBLE. Severity: stale-replica.** Margin is heartbeat-timeout 5 s (Host.scala:60) + driver start + RTT vs 7 s cache (Host.scala:66-73); a stall (GC, executor backlog) of ~2 s defeats it, after which paxos F2 mechanics apply. No defense in code.
> *Fix sketch:* Make the idle takeover prefer asking peers' status caches before running Paxos, and widen the cache vs timeout gap.

**F5 (M) The takeover margin (cache 7 s vs. idle-driver start 5-6 s + RTT) is about 1 s and has no defense against a stall.**
`TR1` rests on timer granularity (heartbeat check at `Store.scala:75` fires on the next 1 s tick) and on the cache being
written at finalization time, not at the idle store's last event. A heartbeat tick delayed by GC, a slow executor (all
the store's work runs on a shared store thread), or a slow network can push the first recovery prepare beyond 7 s.
Then the outcome of a committed transaction is re-derived: proof of commit if an object still shows the update, otherwise
`TransactionTooOld` aborts. The committed outcome would be changed to abort in the driver (F8), though replicas that
applied remain applied and the lagging store drops its update (repaired via ErrorTree). No defense beyond the logs.

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: stale-replica (low).** Tx constructor sets txStatus = trs.status but oresolution = None (Tx.scala:66-67, 114); resolvedAndAllObjectsLoaded is never run for a recovered Committed Tx, lock() and needsTracelessCheck require Unresolved, so such a Tx neither locks nor applies until its idle driver (5-6 s) learns the outcome from the cache, which its own constructor seeded.
> *Fix sketch:* Restore oresolution from a persisted resolved status at recovery.

**F6 (L-M) A persisted resolved status is not restored into the recovered `Tx`.** `txStatus = trs.status` but
`oresolution = None` (`Tx.scala:66-67`); a recovered `Tx` with persisted `Committed` (saved by a retransmitted-prepare
re-save after resolution) is excluded from the traceless check (`:128`) and from locking (`:245`), and relies on its own
status-cache seeding (`:114`, 7 s lifetime) for its recovery driver (started 5-6 s after load) to learn commit from the
store's own `Store.handleTransactionMessage` (`Store.scala:131-158`). If the cache entry is gone (size eviction or a slow
start) the driver may re-decide through peers' `TooOld` votes and the store drops an update it knew was committed.
Fix direction: restore `oresolution` from `trs.status` on recovery.

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: resurrection (see paxos F3).** Same as paxos F3 (Frontend.scala:655; no server-side HLC update).
> *Fix sketch:* As paxos F3.

**F7 (M) Clock-skew margin is 1 s and nothing validates it.** `tooOld` compares the host's HLC with the client's
`startTimestamp` (`Frontend.scala:655`); the safe-forgetting argument (TR2) assumes <1 s skew. A client whose clock is ahead of a
host by >1 s turns the 7 s expiry into a window in which a >7 s-old unknown prepare (client retransmission, delayed
driver prepare) evaluates as new: blind writes, `DoesNotExist` after a later delete (ABA) or `Exists`-delete after
re-insert commit again. HLC corrections go only forward through object timestamps; hosts have no skew monitor.

> **Verification 2026-11-09: CONFIRMED. Severity: split-decision (see paxos F2: client-visible false abort; no applied store rolled back).** Same as paxos F2: TransactionDriver.scala:471-521 sends TxResolved(false)/TxFinalized(false) and TransactionFinalized(client,false) after a TooOld-only tally; Frontend.scala:614-624 caches Aborted. Applied stores stay applied.
> *Fix sketch:* As paxos F2.

**F8 (L-M) A re-decided abort can overwrite the cached finalized-commit entries and tell the client "aborted" after
commit.** When F3, F5 or F7 produce an abort decision after a commit, the deciding driver sends `TxResolved(false)` and
`TxFinalized(false)` to every data store and `TransactionFinalized(client, false)` (`TransactionDriver.scala:386,505-517`).
Stores that forgot cache `Aborted` (`Frontend.scala:614-615,624`), replacing a finalized-commit entry for up to 7 s;
a client that missed the original result and keeps retransmitting then sees an abort for a committed transaction and may retry
non-idempotently (e.g. allocate again).

> **Verification 2026-11-09: PLAUSIBLE. Severity: resurrection (low).** Eviction reopens the window (TransactionStatusCache); only a rate-limited warning.
> *Fix sketch:* Enforce rather than warn: refuse unknown prepares when early evictions occurred recently.

**F9 (L) Status-cache size eviction reopens the replay window.** Cap 4,000,000 entries / 7 s (about 570k tx/s). An
entry evicted early (`TransactionStatusCache.scala:209-224`) behaves as expired: a late prepare with age < horizon is
treated as a fresh transaction. Only a rate-limited warning exists. Practically unlikely.

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica / hygiene.** Same as paxos F9 (TransactionStatusCache.scala:64-66; Tx.scala:114, 662, 800).
> *Fix sketch:* Make updateStatus monotone.

**F10 (L) Shared host-wide status cache is overwritten by weaker information.** Cache is keyed by txid; each store's `Tx`
writes `(status, finalized=false)` when it resolves (`Tx.scala:662`), which can overwrite a `finalized=true` entry just
written by another store's `Tx` on the same host (`:800`). Effect: a late prepare gets `TxResolved` without `TxFinalized`,
so the late driver re-runs the finalizers instead of finishing (extra work and delay, not unsafe given T7), and prepares are not
dropped as finalized (`Frontend.scala:639`) so they create a fresh `Tx` that takes no locks.

> **Verification 2026-11-09: PLAUSIBLE. Severity: split-decision (benign in practice).** Same as paxos F4: the tally is a pure function of immutable votes, so colliding ids propose the same value.
> *Fix sketch:* As paxos F4.

**F11 (L) Proposal-id peer is the store's pool index, not (pool, index).** `Proposer(storeId.poolIndex, ...)`
(`TransactionDriver.scala:88`). A recovery driver on a store of a non-primary pool (any store holding a `Tx` can drive,
`Store.scala:75`) can mint the same `(number, index)` as a primary-pool driver. I verified that all drivers computing a
proposal from the same fixed votes compute the same value (section 4.3), so the collision looks benign, but the property is
implicit; document or assert it. Same argument covers the leader's reuse of proposal `(1, leader)` after restart.

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (leak, extra traffic).** Store.close is frontend.close() only (Store.scala:63) and transactionDrivers is never shut down or cleared (Store.scala:61-90, only `-=` on TxFinalized, :181-183); the driver's timer (SimpleTransactionDriver.scala:42-47, 73-85) keeps firing until it sees a TxFinalized that the new Store instance, with an empty driver map, will not deliver to it.
> *Fix sketch:* Shut down and clear all drivers in Store.close/transfer/unload.

**F12 (M) Zombie drivers after a store closes or moves.** `Store.close` closes the backend only (`Store.scala:63`);
`transactionDrivers` is never shut down on store close, transfer, unload or rebuild. A driver's timer
(`SimpleTransactionDriver.scala:73,113`) keeps rebroadcasting until it sees a `TxFinalized` for its own store, but replies
now go to the replacement store instance, so it can run forever sending prepares/accepts/`TxResolved` as the old
`StoreId` (an unbounded message and memory leak, and a second proposer under the same id as the new store's driver).
Confidence is medium because I did not find a shutdown hook, only absence of one in `Store`, `Frontend`, `StoreRouter`.

> **Verification 2026-11-09: PLAUSIBLE. Severity: stale-replica (low).** MissedUpdateFinalizationAction.scala:~91-98 limits traceless entries to counted voters; a late voter has only its ErrorTree mark.
> *Fix sketch:* Write traceless entries for every store without TxCommitted, not only counted voters.

**F13 (L-M) Late-uncounted commit voter gets no TracelessCommits entry.** `commitVoters` is `Some(voters)` only for votes
the driver tallied (`countedCommitVoters`), and entries are limited to those voters (`MissedUpdateFinalizationAction.scala:91-98`).
A store that voted commit after the driver's tally and then crashes recovers a `VoteCommit` Tx with no traceless
entry, so an abort from peers' `TooOld` votes is accepted at once. The design argues such a store is "no different from one
that missed T" and relies on its ErrorTree mark and exact reconciliation. That argument fails for any case in which the
repair read quorum can include stores that have not applied the traceless change (the 5-wide, write-4 example in the design doc
with two such stores).

> **Verification 2026-11-09: CONFIRMED (by design). Severity: liveness.** Finalization needs the marker write (see F2).
> *Fix sketch:* As F2.

**F14 (M) Finalization (and therefore forgetting) depends on the availability of the ErrorTree write; no escape hatch.**
Any transaction in which one store is silent for `errorTimeout` needs a successful marker transaction, whose own
commit may be slowed by the same overload that made the store silent (the `MissedUpdateMarker` batching mitigates it but
does not remove the dependency). Under a partition that leaves a write-threshold of the metadata pool unreachable,
transactions pile up unfinalized with their CRL state. Whether CRL admission (`CrlAdmission`) then throttles new
prepares across the board is outside this scope.

> **Verification 2026-11-09: PLAUSIBLE. Severity: doc-only/open.** Design choice (MissedUpdateFinalizationAction.scala traceless comment).
> *Fix sketch:* Cross-check against repair doc.

**F15 (L) Stores that never held a `Tx` for a committed transaction are not recoverable by lookup at all.** The traceless
design deliberately excludes non-voters (their `ErrorTree` marks suffice). This is only sound if repair always reaches
the correct quorum state; see repair-process.md.

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (wasted work).** Store.scala:156 `notFinalized` is true for a cached non-finalized Aborted/Committed entry, so the leader starts a driver for a resolved transaction.
> *Fix sketch:* Drive only when the cache has no outcome.

**F16 (L) The designated-leader trigger ignores abort-in-status-cache.** `Store.handleTransactionMessage` starts a leader
driver when `notFinalized` (`Store.scala:156`), which is true for a cached `Aborted/Committed` non-finalized entry. The driver
then runs Paxos for an already-resolved transaction (answers come back as `TxResolved` quickly); wasteful rather than unsafe,
but combined with F10 can produce duplicate finalizer runs.

> **Verification 2026-11-09: OPEN (not verified). Severity: stale-replica.** Not re-derived.
> *Fix sketch:* Add a test.

**F17 (open) Recovered `Tx` objects that fail to load.** `objectLoadFailed` (`Tx.scala:359`) leaves the object out of
`objects`; for a recovered commit vote the apply is skipped as "uncommittable"/`MissingObject` and reported in
`TxCommitted`, which makes the finalizer mark it. Is a persistent read failure (ObjectNotFound for a store that lost the object) ever
reconciled by repair for a recovered Tx that is applying a *creation*? Not verified.

> **Verification 2026-11-09: OPEN (not verified). Severity: liveness.** Not re-derived; Store.handleTransactionMessage runs drivers while Frontend holds messages (Store.scala:128-193) as the doc says.
> *Fix sketch:* Add a test.

**F18 (open) `holding` blocks reads, but not driver traffic**: during recovery the store drops client reads and holds
transaction messages, yet `Store.handleTransactionMessage` lets drivers on this store process votes and respond
(`Store.scala:128-193`) and recovery drivers on the *same* store can start (heartbeat) before the `Tx` has loaded. Since drivers
only produce messages to peers and the `Tx` holds its own responses until loaded, I did not find a violation, but the
combination (recovery driver for a transaction whose local `Tx` is still loading) is untested here.

### 10.1 Mapping to the requested hazard classes

| Hazard | Flaws |
|--------|-------|
| Lose a committed update | F3, F5, F6, F7, F13 (all on a lagging store, rescued only by repair) |
| Apply an aborted update | none found; the traceless path applies only after an entry that exists only for committed transactions (finalizer writes it after commit resolution) |
| Replay a forgotten transaction | F4, F7, F9, F10 |
| Fail to record a missed update | F2 (permanent marker failure), F14; F13 for traceless |
| Hang forever | F1, F2, F12; F14 (long) |
