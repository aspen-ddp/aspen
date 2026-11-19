# Transaction Recovery

How a transaction reaches its outcome, gets finalized, and is applied everywhere it is owed when drivers,
participants, clients or messages fail. Paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start
with `docs/`.

Sibling documents (not duplicated here): [paxos-transactions.md](paxos-transactions.md) (the Paxos layer, proofs,
late resolution, the transaction horizon and expiry), [crash-recovery-log.md](crash-recovery-log.md) (CRL format,
applied stubs on disk), [transaction-requirements.md](transaction-requirements.md) (requirement checks, replay classes
of each write kind), [repair-process.md](repair-process.md) (how ErrorTree and UnackedCommits entries are consumed),
[rebuild-process.md](rebuild-process.md), [systemic-loss-protection.md](systemic-loss-protection.md). Module guides:
`server/transaction/CLAUDE.md`, `server/store/CLAUDE.md`, `client/internal/transaction/CLAUDE.md`,
`server/repair/CLAUDE.md`. Designs: `docs/superpowers/specs/2026-11-13-transaction-horizon-design.md`,
`2026-11-09-replay-pins-design.md` and its addendum, `2026-11-14-store-incarnation-design.md`.

Thresholds: for a pool of width `w`, write threshold `t` and read threshold `r = IDA.consistentRestoreThreshold`
(a majority), `r <= t <= w`. A committed update is readable only while at least `r` stores hold it.

## 1. Recovery situations

There are two root failures, a lost driver and a lost participant, plus the consequences of forgetting a transaction
once it is finalized. Each situation has its own mechanism:

| # | Situation | Detector | Mechanism |
|---|-----------|----------|-----------|
| 1 | Driver (Paxos proposer, learner and finalizer runner) gone or silent, before or after resolution | A store's `Tx` sees no driver activity for `txHeartbeatTimeout` | The store starts its own `SimpleTransactionDriver` (a recovery driver), section 4 |
| 2 | Participant (store or host) crashes or restarts | Store load | CRL read, `Tx` rebuilt with `recovered = true`, messages held until loaded, section 7 |
| 3 | `TxFinalized` lost (driver and stores fine) | Same idle detector as 1 | Same recovery driver; it learns the outcome from peers' status caches (relay, 4.2) |
| 4 | Client crashes or goes silent, before or after its prepares | Same idle detector, plus the prefer-abort wait | As 1; the client is only a data carrier and result recipient (10.1) |
| 5 | Participant holds a commit vote whose peers have finalized and forgotten the transaction | `Tx.needsUnackedCheck`, late commits, expiry | UnackedCommits lookup, proofs, late commits, expiry (7.5, section 8) |
| 6 | Participant missed the commit (never applied, failed apply, slow, down) | Finalizer: no `TxCommitted` within `errorTimeout`, or `objectCommitErrors` | `MissedUpdateFinalizationAction` writes ErrorTree and UnackedCommits entries; repair applies them (section 5) |
| 7 | Committed but unapplied update held in the CRL | Store load | Replay through the ordinary commit path (7.4) |
| 8 | Transaction still unfinalized past the horizon (its peers may have forgotten it) | `Frontend.expiryHeartbeat` | Status queries and `ExpiryTally` (section 8) |

One structural fact keeps driver loss simple: there is **no persistent driver state**. A driver is a volatile object
on a store (`Store.transactionDrivers`). All durable consensus state is each store's acceptor state and vote in its
CRL record (`server/crl/TransactionRecoveryState`). A driver can be discarded and rebuilt from the description at any
time; Paxos makes that safe.

## 2. Vocabulary and roles

* **Client** (`client/internal/transaction/SimpleClientTransactionDriver`, `ClientTransactionDriver`): builds the
  `TransactionDescription` (txd), sends `TxPrepare(fromClient = true)` with each store's object data to every store
  of every referenced pool, and retransmits every `retransmitDelay` (1 s in `cmdline/Main`) until it hears the
  leader's `TransactionResolved`/`TransactionFinalized` or passes `startTimestamp + horizon + MaxStartLead`
  (`TransactionHorizon.clientGiveUp`, 22 s), when it completes `Unknown` (`TransactionOutcomeUnknown`). It also learns
  commit from the stores' `TransactionVote`s (a write threshold of `VoteCommit` per object). It has no role in
  consensus or recovery.
* **Store / Tx** (`server/transaction/Tx`): one `Tx` per live transaction per store. It checks the requirements of
  the objects it hosts, votes, persists its vote and acceptor state in the device CRL before answering, is a Paxos
  acceptor if it is in the primary object's pool, holds requirement locks from a commit vote until resolution, and
  applies committed updates.
* **Driver** (`server/transaction/TransactionDriver`, `SimpleTransactionDriver`): proposer plus learner for one
  transaction on one store. It collects votes, proposes, learns the outcome, runs the finalizer, and sends
  `TxResolved`/`TxFinalized`. Only drivers on the primary pool's stores propose (`TransactionDriver.mayPropose`); a
  driver on any other store is learn-only. The **designated leader** (`txd.designatedLeaderUID` in the primary pool)
  starts a driver on a client prepare (`Store.handleTransactionMessage`); any store holding a `Tx` may start a
  **recovery driver**.
* **Finalizer** (`server/RegisteredTransactionFinalizerFactory.Finalizer`): created when a driver resolves commit; it
  runs every `FinalizationAction` the txd names. `TxFinalized` follows only a finalizer whose `complete` succeeded.
* **Status cache** (`server/transaction/TransactionStatusCache`): one per host, shared by its stores, keyed by txid,
  `Entry(status, finalized, late)`, `expireAfterWrite(txStatusCacheDuration)`, at most `DefaultMaxEntries`
  (4,000,000). Monotone: `finalized` and `late` are sticky, a resolved status never reverts, Committed wins over an
  opposite outcome (logged and counted as a split decision). It is safety evidence: a finalized entry makes a store
  ignore a prepare for a transaction it does not hold and lets the relay report the commit late.
* **Pin rule** (`common/transaction/Pins`, `TransactionBuilder`): every client transaction carries a write that
  invalidates its own requirement once applied, or is marked `replaySafe`, so a replayed prepare of a committed,
  forgotten transaction cannot gather a write threshold of commit votes on its pinned object.
* **Late commit**: a commit a driver learns only after the transaction was finalized and forgotten
  (`TransactionDriver.resolveLate`). It runs no finalization actions, and stores apply it only where an UnackedCommits
  entry says they are owed it (`Tx.lateCommit`).
* **Forget floor** (`Frontend.forgetFloorMillis`): the latest of now minus the horizon (20 s), the store's load plus a
  lead, and the status cache's eviction watermark. A prepare for a transaction a store does not hold, started below
  its floor, is refused outright. **A store never rejoins a transaction it forgot.**

## 3. Timers and their required relationships

Production defaults (`cmdline/Main`): heartbeat period 1 s; driver `initialDelay` 100 ms, `maxDelay` 3 s; client
retransmit 1 s. `TestNetwork` builds recovery drivers with 10 ms / 100 ms.

| Timer | Value (prod) | Defined at | Role |
|-------|--------------|-----------|------|
| Store heartbeat | 1 s | `Host.heartbeatPeriod`, `Store.heartbeat` | Driver heartbeats, stall report, collision waits, lookup and held-vote query retries, parked-read/repair expiry, stub expiry and expiry rounds (`Frontend.expiryHeartbeat`), expired-driver removal, idle-Tx check |
| `txHeartbeatTimeout` | 5 s (5 heartbeats) | `Host.txHeartbeatTimeout`, used in `Store.heartbeat` | An idle `Tx` with no driver on its store gets one. Checked each heartbeat, so 5-6 s after the last event |
| `failedDriverDuration` | 6 s (`2 * maxDelay`) | `SimpleTransactionDriver.factory`, `SimpleDriverRecoveryMixin` (500 ms poll) | Redundant backstop to the heartbeat check (`driveTransaction` is idempotent) |
| `txStatusCacheDuration` | 30 s | `TransactionHorizon.statusCacheDuration` (horizon + `MaxStartLead` + 8 heartbeats), asserted by `Host.checkStatusCacheDuration` | Lifetime of an outcome entry after its last write |
| Transaction horizon | 20 s | `TransactionHorizon.Horizon` | Forget floor's time leg (on `startTimestamp` against the host's wall clock) |
| `MaxStartLead` | 2 s | `TransactionHorizon.MaxStartLead` | A store mutes an unknown transaction started further ahead of its clock; added to the load floor |
| Expiry margin | 8 s | `Frontend.expiryMargin` (`MaxStartLead` + `txHeartbeatTimeout` + one heartbeat) | A Tx unfinalized past horizon + margin (28 s) queries its peers: at once, then after 1, 2, 4 and every 8 heartbeats; the same point freezes its acceptor (`Tx.frozen`) |
| Driver retransmit | `maxDelay` (3 s) | `SimpleTransactionDriver.sendPeerMessages` | Re-sends prepares (to stores not yet answered), accepts (with prepares every third send) or `TxResolved` (to stores not known resolved). Contention backoff `nextRound`: random in `[0, backoff)`, doubling to `maxDelay` |
| `TransactionDriver.heartbeat` | every store heartbeat | `TransactionDriver.heartbeat` | `TxHeartbeat` to all data stores while unfinalized (none on the first tick), `TxFinalized` while finalized |
| `errorTimeout` | 2 s | `MissedUpdateFinalizationAction.errorTimeout` | Marker waits this long after it executes for `TxCommitted`s before marking |
| Finalizer restart | 1 s doubling to 60 s | `TransactionDriver.finalizerRetryDelay` | Backoff of a failed finalizer |
| `FinalizationStallAfter` | 60 s | `TransactionDriver.finalizationStall`, `FinalizationStallReport` | `FINALIZATION STALLED` logged once a minute per store |
| `Tx.CollisionWait` | 500 ms (+ up to 1 s granularity) | `Tx.checkCollisionWait` | Vote withheld for a lock holder the Tx builds on |
| `Frontend.MissingDataWait` | 5 s | `Tx.awaitingClientPrepare` | A Tx that has seen no client prepare withholds promise and vote this long (from its creation), then votes abort (`NoClientPrepare`), requirements unchecked |
| `Frontend.UnackedLookupTimeout` | 5 s | `Frontend.retryUnackedLookups` | Logs an UnackedCommits lookup still unanswered (it is not re-issued); a failed one is retried at the next heartbeat |
| `prepareResponseCache` | 10 s, 1000 entries | `Store` | Prepare responses arriving before the store's driver exists, replayed into it |
| `Tx.StuckReportAfter/Every` | 2 s / 5 s | `Tx.reportIfStuck`, `TransactionDriver.reportIfStuck` | Logging only |

Required relationships:

* **Takeover within the cache.** `txStatusCacheDuration >= horizon + MaxStartLead + txHeartbeatTimeout + heartbeat`,
  enforced by a `require` in `Host.checkStatusCacheDuration`: a Tx first created near the horizon still finds its
  peers' entries when its idle driver starts (30 s against a 5-6 s takeover). Missing the window decides nothing
  wrongly: past the horizon the peers refuse the driver and the Tx ends by expiry.
* **Entries outlast joining.** An entry written when its store joined T (its clock then at least
  `start - MaxStartLead`) expires only once T is below that store's floor: `30 - 2 = 28 s > 20 s`. This relies on the
  host's wall clock not stepping back by more than the 8 s slack (paxos-transactions.md, transaction horizon).
* **`errorTimeout < txHeartbeatTimeout`.** While a driver lives its `TxHeartbeat`s keep peers' Txs fresh, so a long
  finalizer does not provoke takeovers. The constraint is that marking must not delay a finalization so much that
  every store starts its own driver; 2 s leaves room.
* **Prefer-abort waits.** `CollisionWait` (0.5-1.5 s) `< MissingDataWait` (5 s); the client retransmit (1 s) is
  well inside `MissingDataWait`, so a live client's retransmit reaches a store that missed its first prepare before
  that store votes abort.
* **Driver silence.** A live driver is silent to its stores for at most about two heartbeat periods (no heartbeat on
  the first tick, then one per heartbeat), well inside `txHeartbeatTimeout`; its `maxDelay` retransmits are
  independent of heartbeats.

## 4. Driver failure and takeover (situations 1, 3, 4)

### 4.1 How a driver is created

1. `Store.heartbeat`, once per heartbeat on the store's executor, drives every `Tx` whose `durationSinceLastEvent`
   exceeds `txHeartbeatTimeout` and that has no driver in `transactionDrivers`. A fail-stopped store does nothing.
2. `SimpleDriverRecoveryMixin.handleRecoveryEvent` (production hosts) does the same every 500 ms with
   `failedDriverDuration` (6 s), posted to each store's executor. It fires after the heartbeat check, as a backstop.
   It does not skip a fail-stopped store; such a store drops every reply (`Store.receiveTransactionMessage`), so a
   driver started there only retransmits until the host unloads the store (`StoreRebuilds.handleFailStopped`).
3. The designated leader starts a driver on any `TxPrepare` for a transaction not finalized in the status cache,
   unless the transaction starts too far ahead of the host's clock (`Frontend.startsTooFarAhead`) or is below the
   forget floor and not held (`Frontend.belowForgetFloor`). This includes client retransmissions, so a restarted
   leader gets a driver back at the next retransmit. An outcome cached but not finalized is driven on purpose: its
   deciding driver ran on another host, and this takes over at once if that driver died.
4. `Store.driveTransaction` is idempotent per store and starts nothing while the store's CRL state is still being
   loaded (`Frontend.recoveryPending`), since a recovered Tx may be among it and the driver must start above its
   acceptor's promises. It creates the driver through `txDriverFactory`, gives it a proof if the store's Tx proves
   the commit (`Store.giveProof`, `Tx.provesCommitToItsDriver`), then replays cached prepare responses.

Driver start (`TransactionDriver.Start`): only the designated leader's first driver, created by the client prepare
that creates the Tx (`initialProposal = true`), keeps the initial proposal `(1, L)` the client's prepare carries, so
that prepare doubles as Phase 1. Every other driver calls `nextRound()` at once, above `Tx.highestProposal` (the
highest id its own acceptor promised or accepted), and delays its first send by a random part of the backoff.

`Tx.lastEvent` is refreshed by a non-client `TxPrepare` or a `TxHeartbeat` from a primary-pool sender
(`Tx.fromProposer`), by `TxAccept`, a non-proof `TxResolved`, `TxFinalized`, and a found UnackedCommits entry. It is
not refreshed by client prepares (otherwise a transaction whose leader is offline would never look idle) or by
learn-only drivers' prepares and heartbeats (otherwise a secondary-pool driver would keep primary stores from taking
over), nor by `TxCommitted` or status requests. A new or recovered `Tx` starts with `lastEvent = now`.

Suppression of duplicate recovery drivers is probabilistic: the first recovery driver's prepares refresh the other
stores' idle timers, so usually one emerges. If several start, Paxos arbitrates (4.3); each that learns commit runs
its own finalizer (4.4).

A driver is removed from its store's map when:
- the store receives a `TxFinalized` for the transaction that leaves the driver finalized
  (`Store.handleTransactionMessage`, `TransactionDriver.isFinalized`); a driver that ignored it (it resolved commit and
  the message is an abort) stays until its own. A driver that is itself finalized sends `TxFinalized` to every data
  store, its own included, and its heartbeat re-sends it until its store gets it;
- the store closes, or is replaced by a reload over the same backend (`Store.close`, `Store.shutdownDrivers`); a
  finalizer already running completes its idempotent actions;
- the transaction is past the horizon plus the expiry margin, the driver has no outcome and the store holds no Tx for
  it (`Store.removeExpiredDrivers`; not while the CRL state loads). A driver with an outcome is kept until its
  finalization completes.

### 4.2 How a driver learns the outcome

A driver's prepare (`TxPrepare(fromClient = false)`) goes to every data store of every referenced pool. A store
answers in this order:

1. **Finalized in the host's status cache** (`Store.handleTransactionMessage`, the finalized relay): the store sends
   itself `TxResolved(outcome)`, committed ones with `late = true` (for a Tx it holds and its own driver), sends the
   prepare's sender `TxFinalized(outcome, late = committed, owed = {})`, and sends `TransactionFinalized` to the
   originating client. If the store holds no Tx, the Frontend then ignores the prepare (no Tx is created and its
   admission is released). The sender's driver finishes without running finalization actions: a commit through
   `resolveLate`, an abort through `onFinalized`.
2. **Resolved, not finalized, in the cache**: the store sends `TxResolved(outcome)`, a commit carrying the entry's
   `late` flag (set only by a late outcome recorded on this host), to the prepare's sender and to itself, delivered
   after the Tx the prepare creates and any driver started for it exist, so all of them learn it. A non-late one a
   driver treats as resolution (`onResolution(sendResolutionMessage = false)`) and, for a commit, runs the finalizer
   itself. If the store holds no Tx and the transaction is above its floor, the Frontend creates one marked
   `outcomeDecided` (no locks; status queries get the cached outcome); below the floor it refuses the prepare.
3. **Live `Tx`** (`Tx.receivePrepare`): it answers with its fixed vote and an acceptor promise, sent only after the
   CRL save of that state completes (or after its objects load, then the save). A Tx that already applied the commit
   also resends `TxCommitted` to the proposer.
4. **Unknown, below the forget floor** (and not muted by `acceptorSince`): refused, no Tx, no reply
   (`Frontend.refusePrepare`).
5. **Unknown, muted**: a rebuilt store's prepare for a transaction started before its `acceptorSince`, or one started
   more than `MaxStartLead` ahead of the host's clock (or at or above its join ceiling). The muted Tx checks nothing:
   it proves the commit if an object already shows the update, and is otherwise forgotten with no promise, vote or
   CRL save.
6. **Unknown, otherwise**: a fresh Tx. A driver's prepare is prefer-abort: a Tx that has seen no client prepare never
   checks requirements. Once its objects load it proves the commit if an object shows the update (`Tx.proveCommitted`,
   `RequirementsApplyer.appliedBy`), and otherwise withholds promise and vote until a client prepare, the outcome or
   `MissingDataWait` (5 s, from its creation), then votes abort (`NoClientPrepare`). A client prepare is checked
   normally.

A **proof** (`TxResolved(proof = true)`, sent to the Tx's last proposer) comes only from a memoryless acceptor
(`Tx.mayProve`: nothing accepted and no recovered Paxos state) whose decided vote is abort and whose object shows the
update. The prover applies nothing, records nothing in the status cache, stays live as an ordinary acceptor, and is
forgotten only on `TxFinalized` or a late outcome (`Tx.applyNothing`). A driver holding a proof never proposes abort,
starts a fresh round with its first proof, adopts only an accepted commit, and resolves late once a prepare quorum of
that post-proof round reports no accepted commit (`Proposer.prepareQuorumWithoutAcceptedCommit`). A recovered or
expiring commit vote that found its UnackedCommits entry proves the commit to its own store's driver the same way.

A driver therefore learns the outcome by: an `Accepted` quorum (`learner.finalValue`); a peer's non-proof
`TxResolved`; a peer's `TxFinalized`; a late message (`late = true`) or, inside the horizon by the driver's clock, a
proof plus a memoryless post-proof quorum (both `resolveLate`). A recovery driver that runs Paxos proposes the highest accepted value its promise quorum reports
(`Proposer.proposalValue`), so a value chosen before the takeover is kept.

### 4.3 Proposal-id competition

Proposal ids are `(number, poolIndex)` (`common/paxos/ProposalId`); only primary-pool drivers propose, so ids are
unique per proposer. A `Nack` (`onNack`) raises the highest seen id and starts a new round with backoff. Acceptors are
the primary pool's stores (`isValidAcceptor`); `TxAccept` goes only to them, while prepares go to every data store so
that every store votes and takes locks.

* Votes are fixed once given and persisted before release, so every driver tallying them computes the same
  proposal. Each object is classified: commit-certain (`c >= t`), abort-certain (`w - a < t`) or undecided.
  Undetermined (a Tx resolved before voting) is no vote. Any abort-certain object proposes abort; all commit-certain
  proposes commit; an undecided tally only waits (a client may already have learned commit from the votes), proposing
  an accepted value its quorum reports if any. The local proposal is sticky across rounds (`acceptQueuedThisRound`).
* A response for a stale proposal id is ignored.
* `sendPeerMessages` sends prepares along with accepts every third send, so a store that finalized and forgot the
  transaction, and therefore ignores accepts, answers the prepare (from its cache, 4.2).

### 4.4 The finalizer and at-least-once semantics

`TransactionDriver.onResolution(committed = true)` records the resolution time and calls `startFinalizer`, which
creates the finalizer (every action executes in `Finalizer`'s constructor), hands it
`commitVoters(countedCommitVoters, peerIncarnations)` and the `commitErrors` collected so far, and on success calls
`onFinalized(true, handedOffStores)`. `onFinalized` queues `TxFinalized(outcome, late, owed)` to every data store and
`TransactionFinalized` to the client. An abort finalizes at once with no finalizer. A resolution the driver reached by
Paxos (`sendResolutionMessage = true`) also queues `TxResolved` to every data store, `notifyOnResolution` stores and
the client.

* **Action order.** `RegisteredTransactionFinalizerFactory.Finalizer` executes every action at once, the
  `MissedUpdateFinalizationAction` (the marker) included, so no other action, failing or retrying for as long as an
  outage lasts, keeps the missed stores' marks unwritten. A durable UnackedCommits entry therefore implies only that T
  committed; every path that concludes T finalized rests on acceptors having forgotten T, which happens only on
  TxFinalized, sent once a finalizer completed every action. `complete` fails with the first failure once all have
  settled. An action whose type cannot be loaded, or whose factory throws, is a failed action (`Finalizer.of`), so the
  marker still runs. The marker is the first action (`TransactionBuilder.buildTranaction` prepends it). An action
  whose `execute()` throws is a failed action too: `Finalizer` catches the throw as that action's failure, so the
  finalizer is still created, the actions after it still execute, and the marker still receives `commitVoters` and
  `updateCommitErrors` and is cancelled with the others; no marker is left running outside a finalizer. The shipped
  actions run their work inside `retryStrategy.retryUntilSuccessful`, which turns a synchronous throw into a failed
  attempt.
* **Failure path.** A finalizer that cannot be created, or whose `complete` fails, is logged at error and started
  again from scratch after `finalizerRetryDelay` (1 s doubling to 60 s), until one completes, the driver finalizes
  another way, or it shuts down (`finalizerFailed`). The transaction is never finalized without its actions: without
  the marker's entries a missed store would stay stale and an owed store could not find its entry. A failure that
  repeats forever is a stall, alarmed by `FINALIZATION STALLED`, never a lost mark.
* **At-least-once, not at-most-once.** Two drivers that both learn commit each run the actions, and a driver that dies
  mid-finalizer is replaced by one that runs them again from scratch. Actions must be idempotent; the shipped ones are
  (allocation and deletion tree entries, TKVL split, join and tail removal, missed-update marks that only move
  forward).
* **Second driver.** A driver reached by another driver's non-late commit `TxFinalized` cancels its own finalizer,
  reads the stores its marker had already handed entries off for (`handedOffStores`, failed finalizers' included via
  `failedHandedOff`), and finalizes at once with `owed` = the received `owed` plus that set. It never waits for its own
  marker's write, so it finalizes while the stores that forgot T still hold their finalized cache entries. Its entries
  may land after its `TxFinalized`; a stub may then precede its entry, which is harmless (5.4).
* **A driver that resolved commit ignores an abort `TxFinalized`** (Committed wins; reported as a split decision).
  Its store keeps it until it is finalized (`TransactionDriver.isFinalized`), so heartbeats, stall reports and
  `Store.close` still reach it.
* **Finalization gates forgetting.** An acceptor forgets a committed transaction only on `TxFinalized`, on a late
  outcome (which follows one), or past the horizon by expiry once more than `w - t` primary acceptors no longer hold
  it (which a committed T allows only after its finalization). Finding an UnackedCommits entry applies the update but
  keeps the Tx until one of those. So an acceptor having forgotten T implies every finalization action completed. A
  durable UnackedCommits entry implies only that T committed: the other actions may still be failing. This is the
  premise `resolveLate` relies on when it runs no actions (a memoryless quorum shows it only for a commit chosen by
  Paxos, so late condition 1 is taken only inside the horizon by the driver's clock, before any expiry commit, which
  no acceptor accepts, can exist: paxos-transactions.md, late condition 1), and the late branches of `Tx.unackedCommitFound`,
  `Tx.unackedCommitNotOwed` and `Tx.concludeOwedCommit` rely on it too. A muted Tx is dropped without `TxFinalized`
  once its objects load (`Frontend.forgetMuted`), but it never voted commit; a prover, like any acceptor, is forgotten
  only on `TxFinalized` or a late outcome (`Tx.applyNothing`); `unackedCommitFound`, `unackedCommitNotOwed` (for a recovered or expiring
  vote) and `proveCommitted` record nothing in the status cache, so a finalized cache entry, and the late relay from
  it, always stems from a real `TxFinalized` or late outcome. Nor does a restart seed one from a tree-learned resolution:
  its saves record `Unresolved` (7.2, `Tx.savedStatus`).

## 5. Recording missed updates (situation 6)

### 5.1 Inputs

* `updateCommitErrors(commitErrors, keptForReplay)`: the map `store -> objects it could not apply`, and the stores
  whose `TxCommitted` set `keptForReplay`, updated on every `TxCommitted` (`TransactionDriver.receiveTxCommitted`) and
  passed once when the finalizer starts. A store sends `TxCommitted` to
  its Tx's last proposer only after every backend commit it issued for T has completed (`Tx.allObjectCommitsComplete`),
  or at once if nothing needed committing, and again on every later prepare. The last proposer is the sender of the
  latest prepare it saw (initially the designated leader); a `TxResolved` does not change it, so a store that never
  got the finalizing driver's prepare acknowledges another driver, and the finalizing one marks it (an extra repair
  pass). Its `objectCommitErrors` are the objects uncommittable at apply time (not those a recovered commit vote
  takes as applied), plus every object a recovered Tx that did not vote commit applies, plus those whose backend commit
  failed; a store that applies nothing (it holds an applied stub, or its entry is not owed
  to it) reports every object it changes.
* `commitVoters(voters, incarnations)`: `voters` is `Some(set)` if this driver itself counted commit votes reaching
  the write threshold for every referenced object (`countedCommitVoters`), else `None` (the outcome came from another
  driver, a relay or an accepted value). `incarnations` is every store's incarnation heard in any prepare reply of any
  round; a store that reported two different incarnations is recorded as unknown for good (`recordIncarnation`).

### 5.2 `MissedUpdateFinalizationAction`

Added to every transaction by `TransactionBuilder.buildTranaction` unless `disableMissedUpdateTracking`
(`MissedUpdateFinalizationAction.limitTracking`, used only for writes about the pool they touch: in practice the
bootstrap pool's own trees, healed by the bootstrap tree sweep instead, repair-process.md).

* `execute()` (when the finalizer is created) schedules `markMissedUpdates` after `errorTimeout` (2 s).
* Fast path: `updateCommitErrors` completes the action, writing nothing, once every data store has reported and no
  report names an object other than a lock-only one. It applies only before the marks are handed off.
* `writeMissedUpdates` builds:
  * ErrorTree `MissedUpdate(store, object, type, storePointer, txd.startTimestamp)` for every hosted, non-lock-only
    object of each store with no `TxCommitted`, and for each non-lock-only object a store reported as an error;
  * UnackedCommits `MissedUnackedCommit(store, txid, UnackedCommitEntry(startTimestamp, incarnation, objects))` for
    each store that sent no `TxCommitted`, is in `voters` when `voters` is known, and hosts an object T changes, with
    the store's incarnation from `incarnations` (`None`, encoded as the zero UUID, if the driver never heard it).
    A store whose `TxCommitted` names objects gets their ErrorTree marks; it also gets an entry, under the same voter
    rule, when its `TxCommitted` sets `keptForReplay`: it keeps T's CRL state for a reload to replay (a failed backend
    commit, `Tx.keptForReplay`). The driver passes the set of such stores with the commit errors
    (`updateCommitErrors`).
* It then hands the marks to `client.missedUpdateMarker` (`handedOff`), recording the stores of the planned entries
  (`handedOffStores`) whatever the write's outcome. From then on only the write's outcome completes the action; a
  failed write fails it (the driver restarts the finalizer). `cancel` shares the action's lock with the hand-off, so a
  caller that cancels and then reads `handedOffStores` sees every store that may still get an entry.

`MissedUpdateMarker.mark` groups entries by pool, merges ErrorTree entries by `(poolIndex, object)` keeping the newest
miss and UnackedCommits entries by `(poolIndex, txid)`, and writes one transaction per pool at a time (`nextBatch`)
with at most 256 entries of both kinds (each kind gets up to half first). Writes are checked inserts
(`TKVLInsert.replaces`): an ErrorTree mark replaces a held one only for a later `missedTimestamp`, and an older one
writes the held value again, moving the entry's revision so that a repair whose read predates this miss's commit
fails its delete (`StoreRepairer.deleteEntryIfUnchanged`). An UnackedCommits entry is written only where none is held,
so the first writer's entry (and incarnation) stands and a re-write never moves its revision under the scan's delete;
a batch whose UnackedCommits entries are all held and that has no ErrorTree marks commits nothing. An entry too large
for half a leaf node (`UnackedCommitEntry.maxEncodedSize`) is written in chunks under consecutive keys, head first
(`UnackedCommitEntry.chunks`); each chunk keeps its first writer's value, so a takeover marker that heard no
incarnation may leave `None` on the head beside chunks the original marker tagged. Lookups read the head only. The
write retries with backoff, without limit on other errors; a `TransactionCreationError` or `NodeSizeExceeded` gives
up and fails every entry of the batch. The marker transaction is `replaySafe` (marks only move forward).

### 5.3 What this guarantees

* **Missed stores are marked before forgetting.** Every store that did not report an error-free commit in time has an
  ErrorTree entry for every non-lock-only object it hosts, durable before `TxFinalized` is sent and so before any
  acceptor forgets T. The value is T's start timestamp plus the object's type and store pointer, which repair compares
  against and rebuilds the pointer from (`encodeErrorEntry`).
* **Owed voters have entries.** Every store whose counted commit vote may be saved and which sent no `TxCommitted` has
  an UnackedCommits entry keyed `UnackedCommitEntry.key(store, txid)`, written in the same pool batch as its ErrorTree
  marks. Entries are one per (store, transaction), never merged: the recovered-vote lookup names that key.
* **Coverage argument.** For each changed object the counted voters reach its write threshold; each either sent
  `TxCommitted` or has an entry, through which a recovered or expiring voter applies T, and repair leaves the entry
  alone while the store may still hold or join T. A `TxCommitted` naming no changed object means its backend commits
  persisted, so at least a write threshold of stores holds T. One naming objects comes from a store that holds T
  already (a stub), is not owed it (another incarnation), or keeps T's CRL state for a failed backend commit; the
  last two keep the object out of the write-threshold count until that state is replayed or repair reconciles the
  copy. Such a store set `keptForReplay`, so a counted voter among them has an entry, and a reload after
  T's finalization replays the state through it (an uncounted one's kept state applies nothing on reload; its
  ErrorTree marks repair it). An uncounted voter or non-voter that sent no `TxCommitted` gets only ErrorTree marks, which repair keeps
  until the store's copy reaches T's timestamp, read from the holders above. An owed voter's entry names its
  incarnation, or none when the finalizing driver did not count the votes and never heard from it; either way the
  voter applies through it (section 7).
* A store that applied durably but whose `TxCommitted` was late or lost is marked too: a harmless extra repair pass
  (reconcile is exact, `server/store/Reconcile`), and, if it was a counted voter, an entry whose late apply its applied
  stub suppresses (5.4).
* **Dependency inversion.** Finalization of any transaction in which a store is silent for `errorTimeout` depends on
  a successful write to that pool's ErrorTree/UnackedCommits trees, which needs a write quorum of the metadata pool
  holding them. Meanwhile every store keeps T's CRL state and admission, and no `finalized` cache entry exists. Marker
  batches run in a higher CRL admission lane than what they finalize (`AdmissionTier`), so data-pool load cannot
  block them.

### 5.4 Applied stubs and `TxFinalized.owed`

An **applied stub** (S, T) is a durable CRL record meaning "S holds T's update (or must never take it through its
entry)", charged to `CrlAdmission` and carrying T's start (crash-recovery-log.md). The Frontend keeps the set in memory
(`Frontend.stubs`).

* **`owed`.** A non-late commit `TxFinalized` lists the stores its driver's markers handed entries off for; aborts,
  late commits and the relay carry none (`TransactionDriver.onFinalized` sets it in the same critical section as the
  finalization). Heartbeat re-sends carry it.
* **Where a stub is written** (only where the record would otherwise be deleted, after the Tx's backend commits
  persist: `Frontend.recordAppliedStub` requires no outstanding or failed commits):
  1. a non-late commit `TxFinalized` naming S: the Tx sets `stubDue` and finalizes with it; with no Tx the Frontend
     writes it directly (`Frontend.handleTransactionMessage`), or passes it to every Tx whose commit is held for its
     turn (`deferredFinalizations`; the call that empties the list concludes the state, `Frontend.transactionFinalized`);
  2. S applied T through its own entry (`Tx.unackedCommitFound`);
  3. a Tx resolved committed and reached by a late commit with neither (its `TxFinalized` was lost) looks its entry up
     (`forgetLookupHeld`), writes a stub if found (any incarnation), and is forgotten either way; a lookup still
     unanswered once T is past the horizon plus the expiry margin is dropped and the Tx forgotten without a stub
     (`Tx.forgetWithoutStubPastExpiry`).
* **The check.** `doCommit` at a store with a stub applies nothing and reports T's changed objects; a late commit
  reaching a commit vote with a stub applies nothing without a lookup; a recovered non-late commit vote with a stub
  resolves committed through `doCommit` and stays until `TxFinalized`.
* **Expiry.** The store drops its own stub once T is past the horizon plus the expiry margin and it holds no Tx for T
  (live, deferred or awaiting backend commits) (`Frontend.expireStubs`, a queue ordered by expiry plus
  `heldStubs`). Below the floor no Tx for T can arise, so the stub could suppress nothing. Repair neither reads nor
  drops stubs.
* **Entries without stubs.** An entry with no stub is harmless at a store that saw T: it never rejoins T, so no Tx
  exists there to apply through it. A store that never saw T can hold a replay's Tx only above its floor; its entry,
  if any, comes from the unknown-voters case and names no incarnation. A replay's commit vote there passed T's pin, so
  a late commit applies T through the entry, which is the update the store lacks. Its vote has held its locks since,
  so its apply is a live voter's; a blind Insert of a key a later transaction deleted, installed there by repair
  before the replay voted, is written back unmarked (transaction-requirements.md Q-W11). A replay that voted abort applies nothing, and the entry's
  reconcile installs T; repair leaves the entry until the store can no longer join T
  (`RepairTarget.mayJoinTransaction`).

## 6. Expiry (situation 8)

Mechanism and proofs: paxos-transactions.md (transaction horizon), `server/transaction/CLAUDE.md` (Expiry),
`server/transaction/ExpiryTally`. Recovery-relevant points:

* A Tx unfinalized past the horizon plus the expiry margin (`Tx.mayExpire`: objects loaded, no late commit or
  forget-lookup held) begins expiry and sends `TxStatusRequest` to every other store of T, at once and then after 1,
  2, 4 and every 8 heartbeats; each round counts only its own replies plus its own frozen state. Past the same point
  its acceptor is frozen (`Tx.frozen`): it promises, accepts and votes nothing more, so the states the tallies count
  are final.
* A store with no Tx answers a cached outcome, or "unknown" only below its floor (a permanent answer); a rebuilt store
  answers `amnesiac` for T started before its `acceptorSince`. A Tx created from a cached outcome (`Tx.outcomeDecided`)
  answers that outcome as the cache now holds it (`Tx.statusForExpiry`), so an observer adopts it. An unresolved Tx
  answers only once frozen with its objects loaded (a commit vote only once durable, `Tx.voteDurable`), or muted for
  good; until then it is silent.
* `ExpiryTally.decide` adopts an outcome, decides commit (`Frontend.onExpiryCommit` -> `Store.finalizeExpiryCommit`:
  the deciding store resolves T at every store and its driver finalizes it, so its marks repair the stores that never
  applied it), holds, or discards. A Tx that knows its outcome concludes once more than `w - t` primary stores are
  non-holders (`Tx.concludeKnownOutcome`); one that does not holds while a store is silent.
* A discard of a commit vote (`needsUnackedCheck`) first waits for an UnackedCommits lookup started after the
  decision: no entry discards it, an entry applies T as a late commit (own or unknown incarnation) or concludes it
  with nothing applied (another incarnation), a failed lookup keeps it holding. Discarding releases locks and objects,
  deletes the CRL state, and leaves the status cache alone; the floor then refuses every later prepare for T.

## 7. Participant recovery (situations 2, 5, 7)

### 7.1 Load-time sequence

```
StoreLoader (control thread)                    store executor
---------------------------------------------   -------------------------------------------------
build Store with deferRecovery=true             (not yet owned)
  Frontend.awaitRecovery()  [awaitingCrl=true]
crl_save.log only: admission.tryChargeLoaded
  (no room: not loaded, save file kept, retried
  at the device scans)
router.install(entry)                           ownership passes
no save file:  router.recoverDeferred(store)  ->  readAndRecover(store)
crl_save.log:  crl.dropStore, then loadStore(trs)
               (failed: UnloadFailedStore, the
               entries dropped, the file kept)
               -> router.crlLoaded            ->  remove save file durably, readAndRecover(store)
                                                  crl.readStoreState(storeId)  (non-blocking Future)
                                                  on success, on executor, if still installed:
                                                    Frontend.recover(state)
                                                      applied stubs loaded and charged
                                                      recoverTransactions: new Tx(recovered=true) for each,
                                                        all registered before any object read starts
                                                      awaitingCrl = false
                                                      releaseHeldMessages() once nothing is holding
```
Code: `server/host/StoreLoader` (`awaitCrlLoad`, `handleUnloadFailed`), `StoreRouter.recoverDeferred`,
`readAndRecover`, `recoverOrFail`, `crlLoaded`; `Frontend.recover`, `recoverState`, `recoverTransactions`,
`recoveredTransactionLoaded`. A save file is the store's whole state, so whatever the device CRL holds for the store
is dropped, durably, before the file is loaded into it (crash-recovery-log.md). The recovered state is counted in the
device's admission before what was retained for the store is released (`Frontend.recoverState`).

While `Frontend.holding` (CRL awaited, recovered transactions loading, or undecided recovered ones waiting) the store
queues every transaction message in arrival order and handles them after (`receiveTransactionMessage`,
`releaseHeldMessages`), and drops client reads (a read before a recovered commit vote retook its locks would report
the pre-commit state unlocked). `Store.handleTransactionMessage` still routes driver messages (prepare responses,
accept responses, resolutions, `TxCommitted`, `TxFinalized`) to the store's own drivers first, and the finalized
relay still answers from the cache. No driver starts while the CRL state loads; once it has, a recovered Tx whose
objects are still loading may be driven at heartbeat, starting above its recovered promise, while its own replies
wait in the held queue.

A failed CRL read or recovery fail-stops the store (`Host.StoreFailStopped`, `StoreRebuilds.RecoveryFailed`), which
ends in a rebuild: its counted commit votes are lost with it (rebuild-process.md).

### 7.2 How a recovered Tx is rebuilt

`new Tx(trs, txd, ..., recovered = true)` restores the serialized txd, the saved object updates, the persisted vote,
the persisted status and the acceptor state. A saved status of Committed or Aborted (re-saved by a prepare or accept
after the resolution, before an abort's state is deleted; one deleted saves nothing more and answers with the abort,
`Tx.stateDeleted`, crash-recovery-log 6.1 A4; the CRL persists it) is restored into `oresolution`: once its objects load such a Tx applies the
commit with the recovered re-check rules, or deletes the abort's state, takes no locks and needs no lookup, then waits
for `TxFinalized` as a live resolved Tx does. The constructor also seeds the host status cache with the saved status.
A Tx resolved through its UnackedCommits entry (`unackedCommitFound`, `unackedCommitNotOwed`) saves `Unresolved` on
every later prepare or accept (`Tx.savedStatus`): restored as resolved, it would seed the host status cache with a
non-finalized `Committed` for a T every peer may have forgotten, whose relay answers non-late and would make the
restored Tx's own driver re-run finalization, and it would lose `notOwedHere`, applying T from a state with no object
data. Restored unresolved, it looks its entry up again and takes the same path, seeding nothing.

Ordering (`Frontend.recoveredTransactionLoaded`, `Tx.completeLoad`):

1. Every recovered Tx is registered before any object read starts, so a synchronously loaded one cannot release the
   held messages early.
2. A recovered `VoteCommit` with unresolved status completes its load as soon as its objects load. Every other
   recovered Tx completes only once every such vote has: first those with a persisted vote (an abort vote, or a
   restored resolution), then those without, in the order their objects loaded. A recovered vote is kept, never
   recomputed (it may already have been counted). A Tx without a vote promises and votes abort to any driver at once,
   but stays undecided after load so a client retransmit that arrives first is still checked; completing last, it
   cannot take locks that conflict with a recovered commit vote.
3. `completeLoad` calls `lock()`: a recovered `VoteCommit` with unresolved status retakes its requirement locks
   (`RequirementsLocker.lock`), so new transactions collide with it exactly as before the crash. A Tx with a restored
   commit resolution takes no locks and applies in the same `completeLoad`, waiting in lock order
   (`Tx.awaitLockHolders`) for the holders it collides with. Every recovered commit vote has relocked by then (step 2),
   so it never applies over a recovered vote's update before that vote applies.
4. `completeLoad` requests the UnackedCommits lookup if `needsUnackedCheck` (7.5).
5. Held messages are released only once nothing is holding.

A recovered commit vote whose object fails to load with anything but `ObjectNotFound` fail-stops the store (a later
load would find no lock), or, on a write-frozen store, poisons the object. `ObjectNotFound` for an object it does not
allocate means the store applied T and a later transaction deleted it: the object is left out, T applies nothing to it
and reports it. An allocation the backend misses gets a fresh state (`Tx.allocationLoaded`) and is created from the
saved data.

### 7.3 Held messages and locks

* Messages for a recovered Tx that arrive before it has loaded are held, and so are messages for unknown
  transactions, so a new transaction cannot load a shared object and take conflicting locks first.
* Locks live on shared `ObjectState` instances kept by `Frontend.referenced`, so every transaction sees the same lock
  table whatever the cache does.
* A commit voter holds its locks until it learns the outcome or is forgotten; `doCommit` drops them before applying,
  and the in-flight-commit marker (`Frontend.commitInFlight`) orders the transactions that follow.
* A committed Tx holding no locks does not apply while another Tx holds locks it collides with on an object both
  change (`Tx.awaitLockHolders`, transaction-requirements.md R15): it is held (`commitHeld`) until each holder
  releases (`Frontend.locksReleased`), and new votes on its objects wait meanwhile (`Frontend.voteBlocked`). A
  `TxFinalized` reaching it while held is deferred like one for a commit held for its turn
  (`Frontend.finalizationDeferred`), its CRL state kept until it applies.

### 7.4 Replay of committed but unapplied updates

A recovered `VoteCommit` Tx later resolved committed (a driver's `TxResolved`, a `TxFinalized`, its own entry, a late
commit whose entry it finds, or an expiry commit) goes through the ordinary path: `resolved(true)` ->
`resolvedAndAllObjectsLoaded(true)` -> `doCommit` -> `issueCommits`, which re-checks the requirements with
`applying = true` (locks ignored) and skips objects that fail:

* An object that already shows T's update (`RequirementsApplyer.appliedBy`) is logged and counted as applied.
* Writes guarded by revision or per-key checks fail their re-check on replay over a newer state. A blind Insert
  over a newer write of its key is a no-op (last writer wins on timestamp and revision); blind deletes and blind
  object-level operations are refused by the builder and the stores. The remaining replay classes are in
  transaction-requirements.md.
* A recovered commit voter whose re-check fails on an object it changes takes it as applied: it held its locks until
  it applied and relocked them at recovery, so only its update and the writes after it in lock order changed the
  object (superseded, traceless, or its data dropped once applied). A live voter's failed re-check is an invariant
  violation, logged at error, skipped and reported; an allocation the backend missed is reported (no copy, no lost
  lock); any apply that throws fail-stops the store (transaction-requirements.md 8.5). A recovered Tx that did not vote
  commit reports every object it applies: it may have applied before the crash.
* The CRL's drop of applied object data (`Tx.allObjectCommitsComplete`, `CrashRecoveryLog.dropTransactionObjectData`)
  is in memory only, so a crash can recover a Tx with data it already applied. For allocations this matters: a
  deletion of an object an unconcluded Tx allocated here waits (`Frontend.awaitAllocationsDurable`) until that Tx's
  state is saved again without the object's update (`Tx.persistWithoutUpdateOf`), or until its concluded state's
  deletion or stub is durable (`CrashRecoveryLog.whenDurable`), so a restart never re-creates a deleted object. A
  key's deletion (a commit or a repair write) waits the same way for another Tx's state holding an applied blind
  Insert of the key (`Frontend.awaitBlindInsertsDurable`, transaction-requirements.md 9.4), so a restart never writes
  a deleted key back, and a recovered Insert of an absent key is one never applied.
* A backend commit that fails keeps the CRL state and the pinned object state, marks the object `failedCommits`, and
  makes every later transaction on it vote abort (`BackendCommitFailed`) until the store reloads. The kept state is
  recovered on reload as a recovered commit vote, usually with status `Unresolved`, and re-applied if it learns a
  non-late commit or finds an entry. Its store's `TxCommitted` set `keptForReplay`, so a counted voter has an entry:
  once T is finalized, its late commit (relay, or a late resolution) finds it and re-applies through it, retrying the
  backend commit, as does expiry past the horizon after the same lookup. The entry lists the store in
  `TxFinalized.owed`, but a failed commit's state is never concluded while the store is loaded, so no stub replaces
  it and a reload finds the state, not a stub. Repair deletes the entry only after reconciling every
  object it names, which leaves the store holding T either way. The state is never re-saved as `Committed` for this:
  a tree-learned resolution saves `Unresolved` (`Tx.savedStatus`).

### 7.5 A commit vote whose peers may have forgotten the transaction

Hazard: S voted commit (possibly counted), then crashed or was cut off before learning the outcome, while the others
committed, finalized and forgot T.

**What S's peers do.** Inside the horizon a peer that forgot T answers S's driver from its status cache (the finalized
relay reports the commit late; 4.2). Below its floor it refuses the driver outright. A peer that never saw T and is
above its floor creates a prefer-abort Tx: it proves if an object shows T's update, otherwise votes abort after
`MissingDataWait`; it can never be a fresh commit vote. So every promise a driver counts comes from real acceptor
state, and Paxos, safe with offline acceptors, never chooses abort once commit was chosen.

**What S does** (`Tx.needsUnackedCheck`: recovered or expiring, `VoteCommit`, unresolved, no restored resolution):

* It looks up its own entry once loaded (`Frontend.requestUnackedLookup`; the Host reads
  `unackedCommitsTree.get(UnackedCommitEntry.key(store, txid))` through its client and decodes the incarnation,
  `Host.unackedLookup`). An expiring live vote looks up only when an abort or a discard needs it.
* It is driven at once, like any recovered Tx (no driver only while the CRL state loads).
* It holds any abort resolution or abort `TxFinalized` until a lookup started after the first held abort finds no
  entry (`Tx.holdAbort`, `Frontend.unackedLookupComplete`, `firstCounting`); a later abort replaces the held message
  but does not move the wait. Then the held aborts are handled again and accepted (`Tx.unackedCommitAbsent`).
* **Entry found naming its own incarnation, or none** (`Tx.unackedCommitFound`): it resolves committed without
  writing the status cache and applies its saved update; it keeps its CRL and acceptor state and waits for
  `TxFinalized` (or its driver, which now holds the proof, or expiry); a stub replaces its state when it is forgotten.
  An entry names none when the finalizing driver did not count the votes (it learned commit from an accepted value, a
  `TxResolved` or a relay) and never heard from the store. That store may hold the counted vote the commit relied on:
  for a pool with `t = r` (Replication(3, 2)), the original driver dying before its marker wrote, plus one lost prepare,
  leaves T at one other store, below the read threshold, so reconcile could never restore it. Every Tx that looks up
  holds its own commit vote, so an unknown entry is applied. If that vote did not count, it is a replay's vote, which
  passed T's pin: the pinned object is before T here, and T's committed update is what the store lacks. A rebuilt store
  whose `acceptorSince` mute a skewed clock defeated is in the same position. A recovered vote's failed re-check there
  is an update already applied, and its Insert of a key now absent one never applied (transaction-requirements.md
  10.5c); a replay's live vote has held its locks since it voted (residual: transaction-requirements.md Q-W11).
* **Entry found naming another incarnation** (`Tx.unackedCommitNotOwed`): T committed, but nothing is applied
  through the entry. A recovered or expiring vote resolves committed without writing the cache, drops its held aborts
  unaccepted, releases its locks, proves the commit to its driver, applies nothing (`notOwedHere`, reporting its
  objects in `objectCommitErrors`) and stays until `TxFinalized` or expiry concludes it (`concludeOwedCommit`, no
  stub). It cannot be forgotten earlier: an entry is durable before `TxFinalized` may have been sent, and a finalized
  cache entry here would let the relay report T late while other stores' entries are not yet durable. The entry is
  left for the repair scan, whose reconcile installs the committed state. Another incarnation means the store was
  rebuilt since its vote, so the vote that counted is lost and this incarnation's copy comes from its rebuild.
* **Lookup failure or no answer**: a failed lookup is retried at heartbeat; the Tx keeps holding aborts (and its
  locks) meanwhile, and is driven. Each lookup has its own request id: an entry found by any of them is taken, while
  no entry counts only from a lookup started after the latest held abort or late commit (`firstCounting`); a
  non-counting absent answer of the latest lookup starts another. The lookup is an untimed read of the tree through
  the host's client, so one still unanswered is never re-issued (one past `UnackedLookupTimeout` is logged once):
  through a metadata outage each Tx keeps at most one read in flight. Its answer cannot be lost: lookups start only
  after the store is installed (recovery is deferred until then), and `Store`'s `post` drops an answer only once
  another store replaced the one that asked.
* A found entry of any incarnation is never taken as absent. When a lookup finds an entry while a driver already runs,
  the Frontend hands the proof to it (`Frontend.onCommitProven` -> `Store.giveProof`).

**Late commit at any commit voter** (`Tx.lateCommit`, recovered or live): held for a lookup started then; an own entry
applies T and the Tx is forgotten as finalized with a stub, as does an entry naming no incarnation; another
incarnation, or no entry, applies nothing. A
stub already held applies nothing without a lookup. A Tx already resolved committed looks its entry up only to decide
its stub (5.4, case 3). A Tx without its own commit vote (abort vote, no vote, prover, muted) applies nothing and
sends no `TxCommitted`.

**Past the horizon** the peers that forgot T refuse S's driver, and S ends by expiry (section 6): it adopts a cached
outcome (a commit through the late-commit path, so S looks its entry up), concludes a known one, or discards after a
lookup that finds no entry.

## 8. State machines

### 8.1 `Tx` (store side), recovery-relevant view

```
            new Tx (prepare)  /  new Tx(recovered=true) from CRL
                    |
        load objects (pendingObjectLoads)  --- recovered: Frontend orders completions
                    |
     [Undetermined] --determineDisposition--> [VoteCommit | VoteAbort | withheld | proof]
        withheld while: no client prepare yet (< MissingDataWait, promise withheld too; not for a recovered Tx)
                        / waiting on lock holders it builds on (< CollisionWait) / an in-flight commit
        frozen past expiry: no vote is decided, no promise or accept given
                    |
     CRL.save(vote + acceptor)  ---durable--->  TxPrepareResponse (+ TransactionVote to the client)
                    |
        (VoteCommit: lock() requirement locks)
                    |
   TxAccept -> CRL.save -> TxAcceptResponse      (acceptor role)
                    |
   TxResolved(c) / proof-holding driver / own entry -> resolved(c): statusCache(c) unless from the tree
        abort:  CRL state deleted at once (held for a lookup if needsUnackedCheck)
        commit: commitAwaitsData? awaitLockHolders? holdForTurn? -> doCommit -> backend commits -> TxCommitted -> data dropped in memory
                    |
   TxFinalized(c) -> finalized(c): statusCache finalized; CRL state deleted once backend commits succeed
                     (or replaced by an applied stub: owed names the store, or it applied T
                     through its entry)
                    |
              Frontend forgets Tx
 commit at a store holding a stub, or notOwedHere: applies nothing, TxCommitted reports T's objects
 recovered/expiring + VoteCommit + Unresolved: lookup; aborts held until a later lookup finds none;
                    own entry -> resolved(true), applied, kept until TxFinalized; other/none -> resolved(true), nothing applied
 late commit: own commit vote -> lookup -> own entry: apply + stub + forget / other, none or absent: applyNothing;
              stub held -> applyNothing; already resolved committed -> lookup -> found: stub / absent: no stub; forget
              no commit vote or prover -> applyNothing
 past horizon + margin, unfinalized: expiry rounds -> adopt / commit / conclude known / discard (after lookup) / hold
```

### 8.2 Driver

```
 created (leader on a client prepare | any store on an idle Tx | expiry commit | a primary or learn-only store)
   -> PREPARE: TxPrepare(proposalId) to stores that have not answered (every maxDelay)
   -> votes per object (commit-certain / abort-certain / undecided); proof: never abort, post-proof round
   -> local proposal fixed; ACCEPT to primary-pool stores (+ prepares every third send); learn-only: no Accept
   -> learner: Accepted quorum   ==> resolved(c)         [or TxResolved / TxFinalized from a peer]
      proof + memoryless post-proof quorum (inside the horizon), or late message ==> resolveLate: finalized, no actions
   resolved(c): commit => startFinalizer ; abort => finalized at once
   -> TxResolved to all data stores + notifyOnResolution + client; re-sent to stores not known resolved
   -> commit: finalizer.complete ==> TxFinalized(owed) to stores, TransactionFinalized to client
      finalizer failed ==> restart after 1 s .. 60 s; unfinalized 60 s after resolution ==> stall alarm
   another driver's TxFinalized ==> cancel finalizer, finalize at once with owed = received ∪ handed off
   removed: any TxFinalized at its store | store close or reload | past expiry with no outcome and no Tx
   Nack ==> nextRound() with randomized backoff
```

### 8.3 Finalizer (commit only)

```
 create: every action executes  ->  commitVoters(voters?, incarnations) -> updateCommitErrors
   others (allocation/deletion/TKVL): retry until success; a give-up fails the action
   marker: all data stores reported error-free? -> complete (no writes)
           executed at create -> after errorTimeout: markMissedUpdates
                -> MissedUpdateMarker (ErrorTree + UnackedCommits, one transaction per pool)
                -> complete on success; fails on a permanent marker failure
 all settled, none failed -> driver.onFinalized
 any failed               -> complete fails -> logged; a fresh finalizer after 1 s doubling to 60 s
 cancel() on another driver's TxFinalized or a late resolution: a marker not yet handed off writes nothing
```

## 9. Invariants and guarantees

* **Single decided outcome.** While any primary acceptor holds Paxos state for T, T has at most one decided outcome.
  Votes are fixed and persisted before release; a response is sent only after its CRL save completes. Below the floor
  no store that forgot T joins it again, so a forgotten transaction is never decided again
  (paxos-transactions.md).
* **Driver disposability.** No driver state is durable; any store can recreate a driver from the txd (in every
  prepare and every CRL record).
* **Liveness of takeover.** A Tx with no driver activity for `txHeartbeatTimeout` gets a driver on its store within a
  heartbeat (unless its store's CRL state is loading). Only stores holding a Tx can drive: a transaction no store has
  heard of is never started (a client crash before any prepare arrives: nothing committed). Past the horizon a Tx
  that cannot learn its outcome ends by expiry, or holds if undecidable.
* **Vote persistence.** A counted commit vote is in the voter's CRL until the voter learns the outcome, its backend
  commits succeed, or the voter is rebuilt. It survives host and store
  restarts and is relocked on load before any new transaction can touch the same objects. A state kept past
  finalization for a failed commit has an UnackedCommits entry (`TxCommitted.keptForReplay`), through
  which a reload replays it.
* **A recovered or expiring commit vote is not dropped silently.** It accepts an abort, or discards itself, only after
  an UnackedCommits lookup that began after the abort (or the discard decision) found no entry.
* **No finalization without finalizers.** `TxFinalized` is sent only by a driver whose finalizer completed, by a
  driver finalized by another's `TxFinalized` or by a late outcome, or relayed from a `finalized` cache entry, recorded
  only after a real `TxFinalized` or late outcome. An expiring Tx concludes a known outcome only when more than `w - t`
  primary acceptors no longer hold T.
* **At-least-once actions.** 4.4; idempotency is each action's duty.
* **Missed-update recording precedes forgetting.** Every store that did not report an error-free commit within
  `errorTimeout` of the marker's execution has an ErrorTree entry per non-lock-only object it hosts (and, if a counted
  voter or the voters are unknown, an UnackedCommits entry) durable before `TxFinalized`. All stores error-free: nothing
  is written.
* **Forgotten transactions are not replayed.** A replayed prepare of a committed, forgotten, pinned transaction is
  refused below every floor; inside the window it cannot collect `t` commit votes on its pinned object, and with a
  proof it resolves late and applies only where an entry names the store's own incarnation. A store holding a stub
  never applies T again. Residual classes (replay-safe transactions, allocations after a persisted deletion at a store
  that never saw T) are in transaction-requirements.md; a re-allocation is healed by reconcile only while the
  deletion's ErrorTree mark for that store is unconsumed. A recovered vote never writes back a key deleted since: the
  deletion reached the backend only once its CRL state no longer held the Insert (transaction-requirements.md 10.5c).
* **Missed commits are repairable.** A store that does not apply a committed update either has its ErrorTree marks
  and, if owed, its entry, or T is not finalized yet (its finalizer is being retried, alarmed after 60 s).
* **Apply at most once per store**, given a revision or per-key check on the object; an applied stub covers a replay
  through an entry.
* **Client independence.** The client can crash at any time without affecting safety. Its only input is its prepare,
  which carries the object data: a store that has seen none votes abort after `MissingDataWait`, and a commit decided
  without a store's data is held there until the data, a client prepare or `TxFinalized` arrives
  (`Tx.commitAwaitsData`); after `TxFinalized` repair restores that store.

## 10. Failure scenarios

Notation: L = designated leader store/driver; S = a participant store; D2 = a recovery driver.

### 10.1 Client crashes

| Crash point | Outcome | Why safe |
|-------------|---------|----------|
| Before any prepare | The transaction does not exist | Nothing durable on stores |
| After prepares to some stores, not L | Those stores create Txs; L starts no driver. 5-6 s later (client prepares are no driver activity) an idle primary store starts D2, whose data-less prepares reach L and the rest | Stores without the client's prepare withhold their response for 5 s, then vote abort with requirements unchecked; the transaction aborts, or commits with fewer stores and a store whose data never came is marked and repaired |
| After prepares to all stores, before the result | Normal completion by L; the client never learns | A retry by a new client instance is a new txid; retry sites recognize their own committed attempt (paxos-transactions.md) |
| After the result | Committed | The caller's state is whatever the transaction wrote |
| Client alive, its result messages lost | It retransmits until horizon + `MaxStartLead` (22 s); stores answer from their Txs or caches (the 30 s cache covers the window); then `TransactionOutcomeUnknown`. A store that forgot T and is past its floor refuses, so no new instance forms | Unknown is retried by default, and every retry site recognizes its own committed attempt |

### 10.2 Store (participant) crashes

| Crash point | State on restart | Outcome |
|-------------|------------------|---------|
| Before it created a Tx | none | An unreachable store: others commit with the threshold; the finalizer marks it; repair |
| After creating the Tx, before the vote save is durable | none (no response was sent) | As above: the vote was never released |
| After the vote save, before the response | recovered Tx with its vote | Answered on a retransmitted prepare; locks retaken on load |
| After an accept save, before `AcceptResponse` | acceptor state | Reported in any later promise |
| After learning commit, before applying | vote + saved data; status Unresolved unless a later prepare or accept re-saved it | Unresolved: lookup, driven. Peers still holding T or its cache entry tell it committed (non-late, or late through the relay, which needs its entry). Own entry: applies. Past the horizon: expiry |
| After applying in memory, before the backend is durable | as above | No `TxCommitted` was sent (only after backend completion), so it is marked; recovery replays from the CRL; re-checks skip what shows T |
| After the backend is durable, before `TxCommitted` | backend has T; CRL still has the data (drop is in memory) | Marked (an extra repair pass); replay skips objects showing T |
| After `TxCommitted`, before `TxFinalized` | backend has T; CRL entry | Recovered Unresolved; no entry (it acknowledged). Driver or caches tell it committed; a late commit applies nothing |
| After `TxFinalized`, CRL delete or stub not durable | possibly the old record | As the previous row; for an allocation a later deletion waited for the record's removal to be durable (7.4) |
| After `TxFinalized`, state kept for a failed backend commit | the kept record, usually `Unresolved` | Recovered Unresolved; an entry (its `TxCommitted` set `keptForReplay`). The late commit, or expiry, finds it and re-applies through it: the backend commit is retried |
| Rebuild instead of recovery (failed load, fail-stop) | CRL entries dropped; new incarnation | Counted votes are lost; the store is rebuilt from peers; old-incarnation entries apply nothing. Safe while losses stay within the write threshold's tolerance (systemic-loss-protection.md) |

### 10.3 Driver-side failures

* **Leader crashes before resolving**: idle stores start D2 after 5-6 s; their prepares refresh other stores' idle
  timers. D2 runs Paxos above every id its own acceptor saw; acceptors report any accepted value. The outcome is kept.
  The client keeps retransmitting; a restarted leader gets a driver on its next client prepare, starting above round 1.
* **Leader crashes after resolving, before finalizing**: stores that received `TxResolved` apply. Idle timers expire,
  D2 learns commit from `TxResolved`, accepted state or caches, creates a new finalizer (voters unknown) and runs it.
* **Leader crashes during the finalizer or marker write**: D2 re-runs every action. A marker batch is a transaction,
  so it is entirely there or not; re-inserting the same keys keeps the held entries.
* **Host of a recovery driver crashes**: the same again, after another 5-6 s.
* **Several recovery drivers**: Paxos arbitrates; each that learns commit runs the actions. A driver reached by
  another's `TxFinalized` finalizes at once (4.4).
* **Driver partitioned from some stores**: those stores go idle and start drivers that can reach the others.
* **Driver on a store that closes or reloads**: shut down (`Store.shutdownDrivers`); other stores' heartbeat recovery
  takes over.
* **Finalization action that keeps failing or retrying**: the finalizer is restarted with backoff, the driver keeps
  heartbeating, stores keep T's Tx, CRL state and admission (locks were released on apply). The marker runs in
  parallel with it, so missed stores are marked during the stall. A host lacking an action's type stalls T, alarmed,
  until a driver that has the type finalizes it (mixed-version support is in Backlog.txt).
* **Marker write that cannot succeed** (metadata pool without a write quorum): retried with backoff; finalization and
  forgetting wait, alarmed after 60 s. A permanent failure restarts the finalizer.

### 10.4 Status cache and time

* **Forget floor**: a store refuses any prepare for a transaction it does not hold whose start is below the latest of
  now - 20 s, its load + `MaxStartLead` (or the arrival margin, or the join ceiling), and the eviction watermark.
* **Expired cache entry** (30 s after its last write): the transaction is then below every such store's floor, so a
  late prepare is refused and no fresh Tx is created.
* **Size eviction**: logged as an error and raises the host's eviction watermark (a floor component), so the evicted
  entry's transaction is refused rather than joined afresh.
* **Lost `TxFinalized`**: the idle Tx starts a driver after 5-6 s; the peers answer from their 30 s entries with
  `TxResolved`+`TxFinalized(late)`; the driver resolves late and runs no actions; a commit voter that already applied
  looks its entry up only for its stub. If even that window passes, nothing is re-decided: the Tx ends by expiry,
  concluding its known outcome once more than `w - t` primary acceptors no longer hold T.
* **Clock skew**: no clock decides replay. Clocks remain in the forget floor's time leg (a step back of more than about
  8 s can let a store rejoin T, paxos-transactions.md), in the `acceptorSince` and `MaxStartLead` mutes, and in repair's
  age gate.
* **Partition that heals after the outcome was forgotten**: a minority store S holding a live commit vote: inside the
  window its driver learns the outcome from the caches; past it the peers refuse S's driver and S ends by expiry, which
  discards a commit vote only after a lookup finds no entry and applies T through an own-incarnation entry.
* **Store missed `errorTimeout` but did apply**: marked (an extra repair pass) and, if a counted voter, given an entry.
  If it forgot T on a `TxFinalized` listing it, it holds a stub and applies nothing again; otherwise it never rejoins
  T, so the entry is consumed by repair only.

## 11. Accepted limitations

* Finalization needs the marker's write: a metadata outage leaves transactions unfinalized, holding CRL state and
  admission, alarmed by `FINALIZATION STALLED` (`Store.stalledFinalizations`). Finalizing without the marks would leave
  missed stores unrepaired.
* An action failing on every attempt (a deterministic bug, a type no host can load) stalls its transaction for good,
  alarmed.
* Every unacknowledged counted voter gets one UnackedCommits entry per transaction (one per silent store when the
  voters are unknown), costing a marker batch slot, a tracked delete at repair, and a stub until the horizon plus the
  margin.
* During a metadata-group outage a recovered vote whose transaction aborted keeps its locks while its abort is held
  for a lookup; a held late commit and an expiry discard of a commit vote also wait, since only the tree says whether
  the store is owed T.
* No driver starts while a store's CRL state loads, costing a client retransmit of latency.
* Duplicate recovery drivers each run the finalization actions once.
* A store that never saw T, whose entry names no incarnation (the unknown-voters case), applies T through it if a
  replayed prepare reached it and voted commit. A blind Insert of a key deleted since, when repair installed the
  deletion there before the replay voted, is written back unmarked (transaction-requirements.md Q-W11).
* A transaction undecidable at expiry (too many rebuilt or forgetting primary stores, ExpiryTally rule G) holds its
  locks and CRL state, logged `UNDECIDABLE TRANSACTION`; a repair read of an allocation it holds times out and defers
  its entry at every scan (repair-process.md 12.8).
* Mixed-version finalizer types (Backlog.txt, "Mixed-Version Support").

## Open weaknesses

None.
