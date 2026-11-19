# Paxos-based transaction resolution: safety specification

This document specifies how a transaction's outcome is decided, delivered and remembered: the store votes, the
Paxos instance among the primary pool's stores, the client's commit learned from votes, the transaction horizon and
forget floor, and expiry. It states the invariants each mechanism establishes and the safety argument that rests on
them. Paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`. Code is cited as
`path/File.scala` plus the symbol.

Designs: `docs/superpowers/specs/2026-11-16-client-1rtt-commit-design.md` (votes to the client, abort-certain tallies,
expiry rules A-G), `2026-11-13-transaction-horizon-design.md` (horizon, forget floor, status cache, expiry),
`2026-11-09-replay-pins-design.md` and its addendum (pins, late commits, prefer-abort, applied stubs),
`2026-11-08-traceless-commits-design.md`, `2026-11-07-per-device-crl-and-commit-ordering-design.md`.

Related safety documents (not duplicated here): `crash-recovery-log.md` (the CRL interface, applied stubs, admission),
`transaction-requirements.md` (requirement checks, locks, commit-in-flight ordering, description validation),
`transaction-recovery.md` (driver and participant recovery, finalizers, the missed-update marker and UnackedCommits
entries), `repair-process.md`, `rebuild-process.md`, `systemic-loss-protection.md`.

Contents: 1 model; 2 protocol overview; 3 invariants; 4 messages; 5 normal-case flow; 6 voting; 7 the driver;
8 persistence ordering; 9 commit application and finalization; 10 edge cases, the horizon and expiry; 11 failure
scenarios; 12 accepted limitations; open weaknesses (last).

---

## 1. Model

### 1.1 Participants

| Role | Who | Notes |
|---|---|---|
| Client | `client/internal/transaction/ClientTransactionDriver`, `SimpleClientTransactionDriver` | Builds the `TransactionDescription` (txd) and each store's object data, sends prepares to every store of every referenced pool, retransmits until the leader's outcome or the give-up point. Learns a commit from the stores' votes (P-VC), never an abort. Not a Paxos participant. |
| Primary pool | the pool of `txd.primaryObject` | Chosen in `TransactionBuilder.buildTranaction` as the object requirement whose pool IDA is greatest (`maxBy(poolIDAMap)`). Its `w` stores are the Paxos acceptors; its IDA's `writeThreshold` is the quorum. |
| Secondary pools | the other pools the txd references | Their stores vote on their own objects and run an `Acceptor` like any Tx, but are not acceptors: the driver feeds only primary-pool promises to its `Proposer` (`TransactionDriver.isValidAcceptor`) and sends `TxAccept` only to `txd.primaryObjectDataStores`. |
| Designated leader | primary-pool store `txd.designatedLeaderUID` | Chosen by the client among stores believed online (`client/internal/transaction/DesignatedLeader`). Its first driver, created by the client's prepare, is the normal-case proposer and learner. |
| Recovery driver | any store whose `Tx` is idle | `Store.heartbeat` drives a Tx idle for more than `Host.txHeartbeatTimeout` (5 heartbeats); `server/SimpleDriverRecoveryMixin` drives one idle for more than the factory's `failedDriverDuration` (`2 * maxDelay`, 6 s in production). Same `TransactionDriver` class. A driver on a secondary-pool store is learn-only (7.6). |
| Store-side participant | `server/transaction/Tx`, one per transaction per store, inside the store's `Frontend` | Owns the acceptor, the vote, the requirement locks and commit application. |

Every `Store`, `Frontend` and `Tx` method runs on that store's executor (`server/CLAUDE.md`); `TransactionDriver`s keep
their own lock and run on timer threads.

### 1.2 Identifiers and values

* `ProposalId(number: Int, peer: Byte)`, ordered by number then peer (`common/paxos/ProposalId`). `peer` is the
  proposing store's pool index (`TransactionDriver.proposer`). The client's prepare carries
  `ProposalId.initialProposal(designatedLeaderUID) = (1, L)`.
* Paxos values are `Boolean` (true = commit). Store votes are `TransactionDisposition` {`Undetermined`, `VoteCommit`,
  `VoteAbort`}; outcomes are `TransactionStatus` {`Unresolved`, `Committed`, `Aborted`}.
* `StoreIncarnation` names one life of a store's data (`Frontend.incarnation`); prepare replies carry it
  (`TxPrepareResponse.incarnation`), and the driver records it per store for the finalizer's UnackedCommits entries
  (`TransactionDriver.recordIncarnation`; a store reporting two incarnations is recorded as unknown for good).

### 1.3 Quorum arithmetic

The acceptors are the primary pool's stores `0..w-1`, quorum `t = writeThreshold`. `Proposer` and `Learner` require
`t >= w/2 + 1`, and the IDA constructors enforce a majority write threshold for Replication, ReedSolomon and
RandomAccessReedSolomon (`common/ida/IDA`). Each changed pool also has a read threshold `r =
consistentRestoreThreshold` (a majority, never `restoreThreshold`), and `majority <= r <= t <= w`, so `t + r > w`.
Any two write quorums intersect, which both Paxos and the "two conflicting transactions cannot both reach `t` commit
votes on one object" argument rely on.

### 1.4 Timers and constants

| Constant | Value | Where |
|---|---|---|
| Horizon | 20 s on `txd.startTimestamp.wallTime` against the host's wall clock | `common/transaction/TransactionHorizon.Horizon` |
| `MaxStartLead` | 2 s | `TransactionHorizon.MaxStartLead` (= `Frontend.MaxStartLead`) |
| Client give-up | horizon + `MaxStartLead` (22 s) on the client's HLC wall time | `TransactionHorizon.clientGiveUp`, `SimpleClientTransactionDriver.pastGiveUp` |
| Status cache life | horizon + `MaxStartLead` + 8 heartbeats (30 s), after the last write; at most 4 M entries | `TransactionHorizon.statusCacheDuration`, `Host.txStatusCacheDuration` (asserted by `Host.checkStatusCacheDuration` to be at least horizon + `MaxStartLead` + `txHeartbeatTimeout` + one heartbeat), `TransactionStatusCache.DefaultMaxEntries` |
| Expiry margin | `MaxStartLead` + `txHeartbeatTimeout` + one heartbeat (8 s) | `Frontend.expiryMargin` |
| Host heartbeat / `txHeartbeatTimeout` | 1 s / 5 s | `cmdline/Main`, `Host.txHeartbeatTimeout` |
| Driver retransmit | first send after 100 ms, then every 3 s (`maxDelay`); backoff on a new round doubles up to 3 s, random delay below it | `SimpleTransactionDriver`, `cmdline/Main` |
| Client retransmit | 1 s | `cmdline/Main`, `SimpleAspenClient` |
| Prefer-abort wait | 5 s from the Tx's creation | `Frontend.MissingDataWait` |
| Collision wait | 500 ms, checked at the store heartbeat | `Tx.CollisionWait` |
| UnackedCommits lookup timeout | 5 s | `Frontend.UnackedLookupTimeout` |
| Prepare-response cache (no driver yet) | 10 s, 1000 transactions | `Store.prepareResponseCache` |
| Acceptor-since margin | 10 s | `server/rebuild/RebuildingStore.AcceptorSinceMargin` |
| Join ceiling | renewed every 250 ms to now + `MaxStartLead` + 1 s | `server/host/JoinCeiling` |
| Finalizer retry | 1 s doubling to 60 s; stall alarm after 60 s | `TransactionDriver.finalizerRetryDelay`, `FinalizationStallAfter` |
| Missed-update `errorTimeout` | 2 s | `client/internal/transaction/MissedUpdateFinalizationAction` |

---

## 2. Protocol overview

The client sends `TxPrepare((1, L), from = L, fromClient = true)` with each store's object slices to every store of
every referenced pool. Each store loads its objects, decides its vote, takes the requirement locks of a commit vote,
saves the txd, its data, its vote and its acceptor state (`promised = (1, L)`) to its device's CRL, and only when that
save is durable sends a `TxPrepareResponse` (promise plus vote) to the leader and a `TransactionVote` to the client.

* **Client commit in one round trip.** The client completes `Committed` once every referenced object has its pool's
  `t` `VoteCommit`s (`ClientTransactionDriver.commitCertain`). Votes are durable and immutable, so commit is then the
  only possible outcome (P-VC). The client never learns abort from votes.
* **Stores learn by Paxos.** The leader tallies the votes; on a decisive tally it sends `TxAccept((1, L), v)` to the
  primary pool, its `Learner` sees `t` `Accepted`, and it sends `TxResolved` to every store and `TransactionResolved`
  to the client. On commit it runs the FinalizationActions and then sends `TxFinalized` and `TransactionFinalized`;
  on abort it finalizes at once.
* **Recovery.** Any store whose Tx goes idle drives it. Store-side drivers send data-less, prefer-abort prepares
  (6.2), propose abort only on an abort-certain tally (P7a), and otherwise wait or re-propose a value their promise
  quorum reports.
* **Bounded memory.** Stores forget a transaction (an abort at resolution, a commit at `TxFinalized`, the host status
  cache after 30 s). The forget floor makes a store refuse every prepare for a transaction it may have forgotten
  (10.10), so no transaction is ever decided twice. A Tx left without an outcome past the horizon plus the margin
  decides from its peers' status (expiry, rules A-G).

Why not self-generated Accepts and a commit-only round 1: votes to the client give the same one-round-trip commit
without changing Paxos; acceptors accept only a driver's `TxAccept`, so P2 and P8 are ordinary single-decree Paxos.
`Acceptor.receiveAccept` accepts any value at any id at or above its promise; nothing restricts a round to one value,
and safety does not rest on any round rule. Do not reason with "aborts need round 2" or "recovery always aborts":
neither is true. Use P-VC and P7a.

---

## 3. Invariants and guarantees

Each is a property that must hold, with where the code establishes it. Items marked **(assumption)** rest on timing or
state retention rather than on the Paxos machinery.

**P1 Quorum intersection.** The acceptor set is the primary pool's `w` stores and every quorum has size `t >= w/2+1`
(1.3).

**P2 Proposal-id uniqueness.** Two proposers never use one `ProposalId` with different values. Only primary-pool stores
propose (`TransactionDriver.mayPropose`), so the pool index identifies the proposer. Only the designated leader's first
driver, the one its client's prepare creates the Tx with (`Store.driveTransaction(initialProposal = true)`), uses
`(1, L)`. Every other driver calls `nextRound` in its constructor (`SimpleTransactionDriver`), starting at round 2 or
above the highest id its own store's acceptor promised or accepted (`TransactionDriver.Start.highestSeen`,
`Tx.highestProposal`). Residual: a restarted store-side driver of round 2 or more may repeat an id it used in an earlier
life that its own acceptor never saw. Two such proposers tally the same immutable votes (7.2), so they propose the same
value, and a restarted host refuses every transaction it may have forgotten (10.10). P2 is load-bearing because an
acceptor accepts an Accept whose id equals its promise.

**P3 Acceptor durability.** A `Promise`, `Nack` or `Accepted` is sent only after the acceptor state that justifies it
(`promised`, `accepted`) is durable in the device CRL together with the txd, the store's object data and its vote:
`Tx.answerPrepareAsAcceptor`, `Tx.sendDelayedPrepare` (a prepare answered once the objects load) and
`Tx.receiveAccept` send from the completion handler of `crl.save`, which runs only once the entry is durable
(`crash-recovery-log.md`). Handlers run on the CRL's completion thread and only send messages built before the save;
the one state change a save completes (`Tx.voteDurable`) is posted to the store's executor (`Tx.saveState`). A CRL that
fails or shuts down runs no handler of an unwritten entry (`CrlFailed`, `CrlClosed`), so no response follows it.

**P4 Vote immutability.** A store's non-`Undetermined` disposition is decided once (`Tx.determineDisposition` decides
only while `Undetermined`) and never recomputed while the Tx lives. A recovered Tx keeps its persisted vote
(`Tx.completeLoad` skips the decision for a recovered Tx with a vote). A Tx frozen past expiry decides no vote at all
(`Tx.undecidedFrozen`).

**P5 Vote and promise are one durable record.** The disposition travels in the same `TxPrepareResponse` as the promise
and is saved in the same `TransactionRecoveryState`. A vote any driver or client has seen survives any crash of its
store.

**P6 Every commit rests on durable commit votes, a proof, or an expiry rule.** Commit is concluded in four ways:
* a driver's tally: every object has at least its pool's `t` `VoteCommit`s in the current round (7.2);
* a proof holder adopting an accepted commit of its post-proof round (10.5), when T's update was already applied;
* an expiry commit (10.10, rules A, B, F), which resolves T at every store without a Paxos round, its driver then
  finalizing it;
* the client's conclusion from votes (P-VC), on the same test the tally uses.

**P7 Abort is a valid free choice for Paxos.** If no value has been chosen any proposer may propose abort: an abort
applies nothing.

**P7a Abort is proposed only on an abort-certain tally.** P7 is not enough once clients learn commit from votes: P-VC
also forbids an abort while every object may still reach `t` commit votes. A driver's local proposal is abort only
when some object is abort-certain (`w - a < t`, `TransactionDriver.receiveTxPrepareResponse`). Otherwise
`Proposer.proposalValue` re-proposes an accepted value its promise quorum reports, or the driver waits. An undecided
tally never proposes abort, past expiry included; such a T ends by expiry (10.10). TransactionDriverSuite "an
undecided tally never proposes abort", "an abort-certain tally still proposes abort".

**P-VC Vote-decided commit.** If every object of T has `t` commit votes (its pool's write threshold), commit is T's
only possible outcome; recovery and expiry abort or discard T only where that cannot be the case. The client relies on
it: it completes `Committed` on those votes alone (`ClientTransactionDriver.receive(vote)`, `commitCertain` over
`txd.allReferencedObjectsSet`, the set the driver tallies). Why it holds:
* Votes are durable before they are sent, to the leader or to the client: `Tx.sendPrepareResponse` runs only in the
  completion handler of the CRL save that holds the vote. They are immutable (P4) and recovered with the Tx. A rebuilt
  store loses its vote but is muted for T and reports itself `amnesiac`, never a non-voter (10.10).
* A store never casts a second vote: while it holds T's Tx its vote stands, and once it forgot T it refuses every
  prepare for T (the rejoin invariant, 10.10).
* A store has one vote, so `c >= t` commit votes leave at most `w - t` abort votes on that object and no tally is
  abort-certain. By P7a no driver proposes abort, so no abort is ever accepted (lemma L1, 10.10); Paxos can choose only
  commit.
* Expiry discards only by rules C, D and E, each of which rules out a commit-certain tally or loses the data (10.10).
  10.12 audits every other way T can end as abort.
* Two conflicting transactions cannot both have `t` commit votes on one object: commit votes take locks (P12) and `t`
  is a majority.

**P8 Single chosen value.** At most one of {commit, abort} is chosen by the primary pool's acceptors, given P1-P3 and
that the acceptor state of a possibly chosen value is still held by a quorum-intersecting set (P9). The proposer
proposes the highest accepted value reported in its promise quorum before it falls back to its local proposal
(`Proposer.proposalValue`). `highestAccepted` is kept across rounds; that is safe because every value it holds was
really accepted at a ballot below the current one, and taking the maximum over a superset of a quorum's reports picks
the chosen value whenever one exists.

**P9 Acceptor-state retention (assumption).** The state of a possibly chosen value is retained while it can matter. A
committed Tx's CRL record and in-memory acceptor live until `TxFinalized` (or a late outcome). An aborted Tx's CRL
record is deleted at resolution (`Tx.resolvedAndAllObjectsLoaded` -> `Frontend.deleteTransactionState`), releasing
its CRL admission, but its in-memory Tx lives until `TxFinalized` (`Tx.stateDeleted`). It saves nothing more: a
prepare, an accept or a held prepare response is answered with `TxResolved(committed = false)` to the proposer
(`Tx.answerResolved`), never a promise or acceptance. That is sound because the abort is the chosen value, and an
acceptor that promises and accepts nothing can help choose no other. After a store forgot T, safety is carried by the forget floor: the
store refuses every prepare for T it may have forgotten (10.10), so no driver ever counts the fresh state of an
acceptor that forgot T. A committed T's record may be replaced by an applied stub, which carries no Paxos state, only
"this store holds T's update" (9.1).

**P10 Only the chosen value is announced.** A driver sends `TxResolved`, `TransactionResolved` and `TxFinalized` only
after its `Learner` saw a quorum accept one proposal id (`TransactionDriver.receiveTxAcceptResponse`,
`onResolution`), or after another party's resolution reached it (`receiveTxResolved`, `receiveTxFinalized`), or on a
late commit (`resolveLate`, 10.5). Three further sources exist: a status-cache relay (`Store.handleTransactionMessage`),
which repeats an outcome a real resolution recorded; a Tx whose aborted state was deleted (`Tx.answerResolved`, P9),
which repeats the abort it learned; and an expiry commit (`Frontend.decideExpiry`), which sends `TxResolved(commit)`
under rules A, B, F. A store may send a `TxResolved(proof = true)` (P15), which is not an outcome.
Receivers trust every non-proof `TxResolved`.

**P11 Application only on resolution.** A store applies T's update only on a non-late `TxResolved(true)`, a non-late
`TxFinalized(true)`, a found UnackedCommits entry naming its incarnation or none (`Tx.unackedCommitFound`), or a late
commit reaching its own undecided commit vote whose entry is found so (`Tx.lateCommit`). Never on its own vote, an Accept or a
proof: a prover applies nothing (`Tx.applyNothing`). Never at a store holding an applied stub for T or owed nothing by
its entry (`notOwedHere`): `Tx.doCommit` applies nothing and reports T's changed objects in `TxCommitted`.

**P12 Commit-vote locks.** From the moment a store votes commit until it learns the outcome, T's requirements hold
locks on the store's object states (`Tx.lock`), including after a restart: a recovered commit vote re-takes them, and
the Frontend holds all transaction messages until every recovered vote has loaded its objects
(`Frontend.recoverTransactions`, `recoveredTransactionLoaded`, `holding`). A Tx created after its outcome is known
(`Tx.outcomeDecided`) or recovered with a resolved status takes none. Details: `transaction-requirements.md`.

**P13 Object data lives in the CRL until the backend commit succeeds.** `crl.dropTransactionObjectData` runs only once
every issued backend commit completed without error and no object of T is quarantining (`Tx.allObjectCommitsComplete`).
A failed backend commit keeps the record, with its data, for the life of the loaded store. Data adopted from a later
prepare is saved before a commit applies it (`Tx.adoptedDataUnsaved`, `saveAdoptedData`).

**P14 Finalization ordering.** A driver sends `TxFinalized(commit)` and `TransactionFinalized` only after every
FinalizationAction completed (`TransactionDriver.startFinalizer`; a failed finalizer is restarted, never skipped).
`TxFinalized(abort)` is sent at resolution. A store deletes a committed T's CRL record only after `TxFinalized` and its
backend commits (`Frontend.transactionFinalized`). A commit `TxFinalized` carries `owed`: the stores the finalizer's
marker handed UnackedCommits entries off for, set with the finalization and empty for aborts and late commits
(`TransactionDriver.onFinalized(committed, owedStores)`). A store named there replaces its record by an applied stub.
A driver reached by another driver's non-late commit `TxFinalized` cancels its own finalizer and finalizes at once with
the received `owed` plus the stores its own markers had handed off (`handedOffStores`, `failedHandedOff`). Its entries
may then become durable after its `TxFinalized`; the ordering holds for the first finalizer to complete, and the
repair scan allows for the rest.

**P15 Proof of commit is sound.** If an object a store hosts shows revision `ObjectRevision(txid)` (or, for a
KeyValueUpdate without a required revision, some key carries it), T committed, because only an applied commit writes
it (`server/store/RequirementsApplyer.appliedBy`). A Tx on the abort path whose acceptor is memoryless
(`Tx.mayProve`: nothing accepted and no Paxos state recovered) and whose objects show the update sends its proposer a
`TxResolved(committed, late = false, proof = true)` instead of a vote (`Tx.proveCommitted`). The proof is not an
outcome: the driver stops proposing abort and starts a post-proof round (10.5); stores ignore proofs. A proof is not
available for traceless changes (refcount-only, key deletions) or when later transactions overwrote the revision.

**P16 Pin rule.** Every client transaction carries a pin, a write that invalidates its own requirement once applied,
or is marked `Transaction.replaySafe(reason)`; `TransactionBuilder` fails one without with `UnpinnedTransaction`
(`common/transaction/Pins`), and stores check the same rule from the description (`DescriptionValidator`, 6.5). After
a pinned T commits, at most `w - t` stores of the pinned object both hold the pre-T state and do not know T, so a
replay cannot gather `t` commit votes on it. Under the forget floor a replayed or delayed client prepare reaches a fresh
Tx only inside the window in which every store that knew T still remembers it (10.10); the pin rule decides replays
inside that window, for stores that never saw T.

**P17 Status cache (assumption on retention only).** The host-wide `TransactionStatusCache` remembers each transaction's
`Entry(status, finalized, late)` for 30 s after its last write, at most 4 M entries. Stores answer prepares for
transactions found there with `TxResolved`/`TxFinalized` instead of running Paxos (`Store.handleTransactionMessage`,
the finalized relay), and ignore a prepare for a finalized transaction they do not hold (`Frontend.receivePrepare`).
What an entry says is relied on:
* as late-commit evidence: a finalized committed entry is relayed as a late `TxResolved` and a late `TxFinalized`
  (`late = entry.late || entry.finalized`), so a replay's driver runs no finalization actions and stores apply the
  commit only where it is owed (10.5);
* as finalization evidence: a finalized entry makes a store ignore a prepare for a transaction it does not hold, so no
  fresh Tx takes locks or re-applies a blind write.

Both are sound because an entry becomes finalized only from a real `TxFinalized` or a late outcome (proofs and found
UnackedCommits entries record nothing), and `updateStatus` is monotone, atomically: `finalized` and `late` are sticky,
a resolved status never reverts to `Unresolved`, an abort never replaces a commit, and a commit replacing an abort
takes only its own flags. An opposite outcome is logged as a split decision and counted (`splitDecisions`). The cache
is not relied on for retention: an entry that expires, is evicted or dies with its host belongs to a transaction
below the forget floor, whose prepares are refused (10.10). A wrong entry is a safety fault; a missing one is not.

**P18 Client outcome.** The client completes with the first of: a `TransactionResolved` or `TransactionFinalized`,
the commit-certain vote set (P-VC), or `Unknown` at the give-up point. A store's first vote is kept; a different later
one is logged and ignored. Abort and `Undetermined` votes complete nothing. A disagreeing later outcome is logged as a
split decision while the driver is registered: `TransactionManager` keeps a driver until it is `stopped` (the leader's
outcome or the give-up), so a leader's abort after a vote completion is logged. After a vote completion
`SimpleClientTransactionDriver` keeps retransmitting, so a leader that missed the client's prepare still gets a driver
started. A client can see `Committed` before any store has accepted or applied anything.

**P19 First resolution wins.** `Tx.resolved`, `Tx.receiveFinalized`, `TransactionDriver.onResolution` and the client
keep the first outcome. A conflicting second one is logged as an error and counted in the host status cache's
`splitDecisions` (`Tx.reportConflictingOutcome`, `TransactionDriver.splitDecision`, `TransactionStatusCache.updateStatus`).
A driver that resolved commit ignores an abort `TxFinalized` and keeps finalizing (Committed wins, as in the cache).
Its store keeps it: `Store.handleTransactionMessage` drops a driver on `TxFinalized` only once the driver is finalized
(`TransactionDriver.isFinalized`), so heartbeats, stall reports and `Store.close` still reach it, and its own
`TxFinalized`, which it also sends to its store, removes it.

---

## 4. Messages

| Message | From -> to | Carries | Notes |
|---|---|---|---|
| `TxPrepare` | client or driver -> every store of every referenced pool | txd, proposal id, this store's object updates (client only), `fromClient` | `from` always names the proposer store, the leader for a client prepare. |
| `TxPrepareResponse` | store -> `m.from` | `Left(Nack(promised))` or `Right(Promise(lastAccepted))`, disposition, collisions, incarnation | Sent only after the save holding it is durable. |
| `TransactionVote` | store -> originating client | `VoteCommit` or `VoteAbort` | Sent with every prepare response, promise or nack, from the same save handler (`Tx.sendPrepareResponse`). Never by a muted or frozen Tx (they send no prepare response), never for `Undetermined` or a proof. |
| `TxAccept` | driver -> primary-pool stores | proposal id, value | Built only in `TransactionDriver.sendAcceptMessages`; never by a learn-only driver. |
| `TxAcceptResponse` | acceptor -> driver | `Nack` or `Accepted(value)` | After the save. |
| `TxResolved` | driver -> every store plus `notifyOnResolution`; store -> proposer (proof, a deleted abort's answer); store -> itself (relay of a resolved, unfinalized cache entry, 10.2); expiring store -> every store (expiry commit) | `committed`, `late`, `proof` | A proof is meant for drivers only. |
| `TransactionResolved` / `TransactionFinalized` | driver (or a store's finalized relay) -> originating client | `committed` | |
| `TxCommitted` | store -> its `lastProposer` | object ids this store could not apply | After the store's backend commits complete. |
| `TxFinalized` | driver -> every store; store relay -> proposer | `committed`, `late`, `owed`, `startTimestamp` | `owed` empty for aborts, late commits and relays; `startTimestamp` lets a store with no Tx write a stub. |
| `TxHeartbeat` | driver -> every store | | Resets a Tx's idle timer only from a primary-pool sender (`Tx.fromProposer`). |
| `TxUnknownStore` | router -> sender | | The addressed store is not on this host; the driver drops its endpoint cache (`TransactionDriver.receiveTxUnknownStore`). |
| `TxStatusRequest` / `TxStatusResponse` | expiring Tx -> every other store of T, and back | request id, `startTimestamp`; reply `Option[TxStatus]` (status, finalized, late, `holdsAcceptorState`, `accepted`, `votedCommit`, `amnesiac`), `None` = "unknown" | 10.10. |

The network may lose, delay, duplicate and reorder messages (the QUIC plugin drops messages over its send budget);
Paxos and the retransmit loops tolerate all of it.

---

## 5. Normal-case flow

Notation: client `C`; primary pool stores `P0..P4`, leader `L = P2`, `t = 3`; one secondary-pool store `Q0`.

```
 C                P0   P1   P2(L)  P3   P4        Q0
 |-- TxPrepare(1,L) data_i, fromClient, from=L -->  (to every store, in parallel)           (hop 1)
 |                 each store: load objects, check requirements, take locks (commit vote),
 |                 crl.save{txd, data, vote, promised=(1,L)}; ON DURABLE ->
 |                 +----+----+----->L<---+----+----+   TxPrepareResponse(Promise(None), vote)  (hop 2)
 |<----------------+----+----+------+----+---------+   TransactionVote(vote), same handler      (hop 2)
 |  C: every object has t VoteCommits -> Committed (abort is never learned here)
 |                                  L: the client's prepare creating the Tx at L started the
 |                                     driver; tally votes; setLocalProposal(v)
 |                 <-- TxAccept((1,L), v) ---- L ----->  (primary pool stores only)           (hop 3)
 |                 each acceptor: receiveAccept; crl.save{..., accepted=(1,L,v)}; ON DURABLE ->
 |                 +----+----+----->L   TxAcceptResponse(Accepted(v))                          (hop 4)
 |                                  L: Learner has t Accepted at (1,L) -> resolved(v)
 |<--- TransactionResolved(v) ------ L                                                         (hop 5)
 |                 <-- TxResolved(v) -- L ----> all stores incl. Q0
 |                 commit: unlock, apply in memory, backend commit, drop CRL data, then
 |                 +----+----+----->L   TxCommitted(errors)
 |                                  L (commit): run FinalizationActions; when all complete:
 |<--- TransactionFinalized -------- L
 |                 <-- TxFinalized(v, owed) -- L ----> all stores: forget Tx, delete CRL record or stub it
```

| # | Step | Code |
|---|---|---|
| 1 | Client builds the txd: `startTimestamp = HLCTimestamp.now`, primary object, designated leader, `originatingClient`. | `TransactionBuilder.buildTranaction` |
| 2 | Client sends the prepares and retransmits all of them every 1 s until the leader's outcome or the give-up point; a transaction already past give-up at `begin()` is never sent. | `ClientTransactionDriver.sendPrepareMessages`, `SimpleClientTransactionDriver.tick` |
| 3 | Store entry: `Store.receiveTransactionMessage` applies CRL admission to prepares of unseen transactions and to prepares bringing uncounted data (`Frontend.admitPrepare`), dropping a `Full` or `Oversized` one without a reply (10.2). `Store.handleTransactionMessage` answers from the status cache (10.6) and, at the designated leader, starts a driver unless T is finalized in the cache, starts too far ahead, or is refused below the floor. The Frontend then finds or creates the Tx. | `Store.handleTransactionMessage`, `Frontend.receivePrepare` |
| 4 | New Tx: reads its hosted objects; once loaded, `completeLoad` decides the vote and locks. | `Frontend.createTx`, `Tx.completeLoad` |
| 5 | The prepare is answered: `acceptor.receivePrepare` in memory, `crl.save`, then `TxPrepareResponse` to `m.from` and `TransactionVote` to the client. Before the objects load the response is held (`delayedPrepare`). | `Tx.answerPrepareAsAcceptor`, `sendDelayedPrepare`, `sendPrepareResponse` |
| 5a | Client keeps each store's first vote; completes `Committed` on a commit-certain vote set. | `ClientTransactionDriver.receive(vote)` |
| 6 | Leader driver takes responses for its current id only, records each vote, feeds primary-pool promises to its `Proposer`, and at a promise quorum with a decisive tally sets its local proposal and queues the Accept. | `TransactionDriver.receiveTxPrepareResponse` |
| 7 | `TxAccept` to primary-pool stores not yet in `acceptedPeers`. | `sendAcceptMessages` |
| 8 | Acceptor: `receiveAccept`, `crl.save`, then `TxAcceptResponse`. A muted or frozen Tx ignores the Accept; one for an unknown Tx is dropped. | `Tx.receiveAccept` |
| 9 | `Learner.receiveAccepted` counts distinct peers at the highest id; at `t` it fixes the value. | `common/paxos/Learner` |
| 10 | `onResolution`: commit starts the finalizer; abort finalizes at once. `TxResolved` to every store plus `notifyOnResolution`; `TransactionResolved` to the client. | `TransactionDriver.onResolution` |
| 11 | Stores apply or abort (9.1). | `Tx.resolved`, `resolvedAndAllObjectsLoaded` |
| 12 | The finalizer completes: `onFinalized(true, handedOffStores)` sends `TxFinalized` to every store and `TransactionFinalized` to the client; the driver shuts down. | `TransactionDriver.startFinalizer`, `onFinalized` |
| 13 | Store `TxFinalized`: the Tx is forgotten, the cache entry marked finalized, the CRL record deleted or stubbed once the backend commits are done. | `Tx.receiveFinalized`, `Frontend.transactionFinalized` |

The abort path sends `TxFinalized` (and `TransactionFinalized`) before the `TxResolved` messages, since `onFinalized`
runs inside `onResolution`; the order does not matter, because a `TxResolved` for a transaction the store no longer
holds only updates the monotone cache. In the commit case a fast finalizer's `TxFinalized` may replace the queued but
unsent `TxResolved` (`queueTxMessages` logs it); `TxFinalized(commit)` implies resolution at the store.

---

## 6. Voting

### 6.1 What a store votes on

A Tx considers only the requirements of objects it hosts plus non-object requirements (`Tx.requirements`). Its vote is
decided in `Tx.determineDisposition`, in this order:

0. Frozen (T past expiry by the store's clock) with the outcome unknown here and not muted: nothing is decided, and any
   wait ends. The Tx stays a non-voter for good.
1. Muted (a rebuilt store and T started before its `acceptorSince`, or T started too far ahead of the host's clock,
   10.10): `VoteAbort`, never sent. Requirements are not checked and no locks are taken. It proves the commit if an
   object shows the update, and is otherwise forgotten unanswered once its objects load (`Frontend.forgetMuted`).
2. No client prepare seen (prefer-abort, 6.2): `VoteAbort` with `NoClientPrepare`, requirements unchecked, once the
   wait for the client's prepare is over or the outcome is known. It still proves the commit if an object shows it.
3. Invalid description (6.5): `VoteAbort` with `InvalidDescription` for every object.
4. A hosted object whose earlier backend commit failed or that was poisoned (`BackendCommitFailed`), that is
   quarantined (`ObjectQuarantined`), or that a rebuilt store's inherited votes may lock (`InheritedVoteLock`, 10.10):
   `VoteAbort`.
5. A hosted object with a commit in flight or held for its turn (`Frontend.commitInFlight`), outcome unknown: the vote
   is withheld and the Tx registers to be told when the commit persists (`Frontend.awaitCommitPersisted`).
6. Otherwise `RequirementsChecker.check`. If every failure is a collision with a lock holder this transaction builds on
   (`Tx.awaitableHolders`: it requires the revision the holder writes, and the holder still holds its locks), the vote
   is withheld for up to `Tx.CollisionWait`. Otherwise `VoteCommit` (no errors) or `VoteAbort`; an abort whose cause is
   only that objects already show T proves the commit instead (6.3).

"Withheld" means no response at all, which Paxos allows; drivers and the client retransmit. A withheld Tx holds no
locks, so waits cannot form cycles. `lock()` runs right after the vote is decided and before it is durable; that is
safe because locks are volatile and a crash before durability discards the vote with them (P3, P5). A Tx whose outcome
was already known when it was created (`outcomeDecided`) still votes, but takes no locks.

### 6.2 Prefer-abort driver prepares

A driver's prepare carries no object data and has `fromClient = false`. It is prefer-abort: it never gathers a fresh
commit vote. A Tx with no vote, not muted, that has seen no client prepare (`clientPrepared`, set only by a
`fromClient` prepare) does not check its requirements (`Tx.awaitingClientPrepare`):

1. **Proof first.** Once its objects load, if it may prove and an object shows T's update, the wait ends at once and it
   proves to `lastProposer`, the sender of the latest prepare, so the driver whose prepare created the Tx gets the
   proof.
2. **Otherwise it withholds everything**, promise and vote, until the client's prepare arrives (then it votes as
   usual), the outcome is known, or `MissingDataWait` (5 s from the Tx's creation) passes. It then votes `NoClientPrepare`
   abort, requirements unchecked. The check runs as prepares arrive (`Tx.answerPrepare`), so the abort goes out with the
   first driver retransmit after the wait.
3. A Tx recovered without a vote does not withhold its answer from a driver: the driver's prepare decides its
   `NoClientPrepare` abort at once and the promise carries whatever its acceptor accepted. Once loaded it stays
   undecided, so a retransmit of its client's prepare that arrives first is checked as usual. A Tx recovered with a vote
   answers with it.

Why this matters: a driver may re-drive a transaction that aborted and was forgotten. A driver decides abort only when
some object's pool has more than `w - t` non-commit votes in one round, and that object's commit votes can never exceed
`t - 1` afterwards: votes are fixed, and a store that forgot T can give a driver only an abort vote (and refuses T
altogether once T is below its floor). The remaining replay source, a delayed duplicate of the client's own prepare,
is closed by the forget floor: inside the window every store that took part still holds its Tx or a finalized cache
entry (an abort is finalized at once) and answers with the abort; past the floor it refuses (HorizonIntegrationSuite
"a delayed client prepare of a forgotten abort does not commit inside or past the horizon").

Effect on healthy transactions: none for a live client, which retransmits every 1 s; a known transaction's retransmit
is admitted without an admission check (`Frontend.admitPrepare`). A client prepare that arrives later is still used:
its data is adopted so a commit decided by the others can be applied (`Tx.adoptObjectUpdates`), and the commit waits
until that data is durable (`commitAwaitsData` includes `adoptedDataUnsaved`). Orphaned data-less transactions
(refcount changes, version bumps, lock-only) abort where a store got no client prepare; so do a live client's
transactions whose prepares to some store are all lost for 5 s. The wait runs from the Tx's creation because a store
never sees the retransmits that are lost, and the first one it sees ends the wait.

### 6.3 Proof of commit on an unknown transaction

If a vote would be abort, the outcome is unknown here, the acceptor is memoryless (`Tx.mayProve`) and an object shows
T's update, the store proves the commit (`Tx.proveCommitted`): one `TxResolved(proof = true)` to `lastProposer`; no
save, no locks, nothing in the status cache, no local resolution. A response already held for the objects to load still
goes out, as the abort vote it now is. The prover stays a live Tx and an ordinary acceptor: later prepares get a
promise with no accepted value plus its abort vote, saved first (`Tx.answerPrepare` -> `answerPrepareAsAcceptor`); a
muted prover re-sends only the proof. Like any acceptor it is forgotten only on `TxFinalized` or a late outcome (a
non-late `TxResolved` changes nothing at it, `Tx.receiveResolved`); then it applies nothing, sends no `TxCommitted` and
records nothing (`Tx.applyNothing`): it holds no commit vote, so it is owed nothing. Its saved abort vote is a true vote (its
requirements fail) and cannot make a tally abort-certain over `t` commit votes: it is not one of the commit voters, so
it counts in `a <= w - c`. A recovered prover has Paxos state, so it no longer proves and answers with its saved vote.

### 6.4 Recovered votes

A Tx recovered from the CRL keeps its persisted disposition and acceptor state and restores a resolution its saved
status records (`Tx.oresolution`; status is saved only incidentally, by a prepare or accept after the resolution). A
recovered resolved Tx takes no locks and applies its commit, or deletes its abort's state, once its objects load.

A recovered unresolved commit vote, of any transaction, looks up its UnackedCommits entry once loaded and again before
accepting any abort (`Tx.needsUnackedCheck`, `holdAbort`, `Frontend.requestUnackedLookup`). Its store drives it at once
(`Store.driveTransaction`), except while the store's CRL state is still loading (`Frontend.recoveryPending`). That is
safe because, under the forget floor, no store that forgot T answers a driver, so Paxos cannot choose abort over a
chosen commit. A found entry naming this incarnation applies the saved update and resolves committed without
forgetting the Tx or writing a cache entry (`Tx.unackedCommitFound`); its driver gets the proof
(`Store.giveProof`, at start or through `Frontend.onCommitProven`). A found entry naming no incarnation (the driver did
not count the voters, or heard two incarnations from the store) applies too (`Frontend.unackedLookupComplete`): the
Tx holds its own commit vote, which may be the counted one, and a vote that did not count is a replay's, which passed
T's pin. Such a re-apply leaves out and quarantines every object on which it would insert a key now absent
(`RequirementsApplyer.mayReinsertDeletedKeys`, 9.1). A found entry of another incarnation resolves committed applying
nothing (`Tx.unackedCommitNotOwed`; `notOwedHere`), its objects reported, its driver given the proof. An absent entry from a lookup started after the latest
held abort releases the held aborts. A failed or unanswered lookup keeps them held and is retried at heartbeat
(`Frontend.retryUnackedLookups`, `UnackedLookupTimeout`). Details: `transaction-recovery.md`.

A recovered commit vote whose object fails to load with an error other than `ObjectNotFound` fail-stops the store, or
poisons the object if the store's writes are frozen (`Tx.objectLoadFailed`), since its lock would be missing.

### 6.5 Description validation

A client-prepared Tx checks its description once, before its requirements (`Tx.invalidDescription`,
`server/store/DescriptionValidator`): its pool's IDA (`Frontend.poolIDA`), the primary object and leader, one update
per object, and the pin rule. A refusal votes abort for every object, is never waited out, and logs a warning. It
depends only on the description and the store's pool, so every store of that pool refuses alike. Details:
`transaction-requirements.md`.

---

## 7. The driver

### 7.1 State

`TransactionDriver` holds a `Proposer(self index, w, t)`, a `Learner(w, t)`, `peerDispositions` (votes of the current
round), `acceptedPeers`, `knownResolved`, `resolved`/`resolvedValue`, `late`, `holdsProof`, `commitErrors`, the
finalizer, `peerIncarnations` and `owed`. A new round clears `peerDispositions`, `acceptedPeers` and
`acceptQueuedThisRound` (`TransactionDriver.nextRound`). The local proposal is sticky (`Proposer.setLocalProposal`):
votes never change, so a later round would tally the same value. `Proposer.highestAccepted` also survives rounds (P8).

### 7.2 Tally

Run when the proposer has a promise quorum, holds no proof, may propose, and has no local proposal. For each object
pointer, with `w`, `t` of its pool: `c` = stores of that pool whose current-round reply is `VoteCommit`, `a` = those
whose reply is `VoteAbort`. `Undetermined` (a Tx resolved before it voted) is no vote: it counts neither as a reply nor
as an abort. Every object is classified:

| Condition | Class |
|---|---|
| `w - a < t` | abort-certain: the object can no longer reach `t` commit votes |
| otherwise `c < t` | undecided |
| otherwise | commit-certain |

Any abort-certain object makes the local proposal abort. Otherwise, all commit-certain makes it commit. Otherwise the
round queues an Accept only if its promise quorum reports an accepted value (`Proposer.maySendAccept`), which is
always safe to propose, so stores answering `Undetermined` cannot stall a round whose quorum reports the chosen value;
failing that, the driver waits. Properties:
* Commit implies `c >= t` on every object (P6).
* Once `c >= t` or `a > w - t` holds it stays true: votes are immutable and only accumulate. Drivers that see the same
  votes agree, and same-id proposers (P2's residual) propose the same value.
* An undecided tally never proposes abort (P7a), whatever the time; such a T ends by expiry.

### 7.3 Rounds, Nacks, backoff

* A Nack for the current id advances the round at once (`onNack`), from any store, secondary-pool ones included.
* `SimpleTransactionDriver.nextRound` doubles a backoff capped at `maxDelay` and sends after a random delay below it.
  The driver re-sends the current prepares or Accept every `maxDelay`, sending prepares before every third Accept so
  that stores that forgot the Tx answer again (a `TxAccept` for an unknown Tx is ignored). Once resolved it re-sends
  `TxResolved` to stores not in `knownResolved`.
* In a later round the tally is skipped; the round's first prepare quorum queues the Accept at once
  (`acceptQueuedThisRound`), and later promises of the round queue no second batch.

### 7.4 Learner and resolution

`Learner.receiveAccepted` counts distinct peers at the highest proposal id it has seen; the driver feeds it only
responses at its current id from valid acceptors. Resolution can also arrive without the learner:
`receiveTxResolved` calls `onResolution(committed, sendResolutionMessage = false)` for a non-late, non-proof message
(cache relay or another driver); a late commit takes `resolveLate`; a proof sets `holdsProof` (10.5).

### 7.5 Who drives

* The designated leader, on any prepare it receives for a transaction not finalized in the cache, not too far ahead,
  and not refused below its floor (`Store.handleTransactionMessage`). Only a client prepare creating the Tx there gives
  the initial proposal.
* Any store whose Tx is idle (1.1). A client prepare is not activity (`Tx.receivePrepare`), so a dead leader is not
  kept alive by the client; neither are a learn-only driver's prepares and heartbeats (`Tx.fromProposer`).
* An expiry commit at the store (`Store.finalizeExpiryCommit`).
* A driver is created at most once per transaction per store (`Store.driveTransaction`), never while the store's CRL
  state loads (`Frontend.recoveryPending`). It starts at the initial proposal only as the designated leader's first
  driver; every other starts at round 2 or above its own acceptor's highest id (`TransactionDriver.Start`, P2). It
  receives any proof its store's Tx holds (`Store.giveProof`), then the cached prepare responses. It is removed when
  its store receives a `TxFinalized` for T after which the driver is finalized (`TransactionDriver.isFinalized`; one
  that ignored an abort `TxFinalized` stays, P19), or when T is past expiry with no outcome and the store holds no Tx
  for it (`Store.removeExpiredDrivers`, not while the CRL state loads). `Store.close`
  and a reload stop every driver of the store (`shutdownDrivers`); a running finalizer completes.

### 7.6 Learn-only drivers

A driver on a store outside the primary pool (`mayPropose = false`) sends prepares and resolves, proves late and
finalizes from what it hears, but never sets a proposal and never sends an Accept (`currentAcceptMessage` returns
none). `designatedLeader` checks the pool too. With no primary store reachable, the Tx ends by expiry, which counts
only replies.

### 7.7 Finalization failures

A finalizer that cannot be created, or whose `complete` fails, is logged at error and started again from scratch after
a backoff (`finalizerFailed`, `finalizerRetryDelay`) until one completes, the driver finalizes another way, or it is
shut down. A transaction is never finalized without its actions: the marker's ErrorTree marks and UnackedCommits
entries are what repair and owed stores rely on. `owed` includes the planned stores of failed finalizers
(`failedHandedOff`). A committed transaction unfinalized 60 s after its resolution is a finalization stall
(`finalizationStall`, `Store.stalledFinalizations`, `FinalizationStallReport`).

---

## 8. Persistence ordering

All CRL writes are `crl.save(txid, TransactionRecoveryState, handler)`. A record holds `storeId`, the serialized txd,
the object updates (unless dropped), the disposition, the status and the acceptor's `(promised, accepted)`. A save
replaces the whole record and always carries the then-current acceptor state, so a later save never loses an earlier
promise. The handler runs after the record, and everything saved before it, is durable (`crash-recovery-log.md`), on
the CRL's completion thread (P3). Every save is charged to the device's `CrlAdmission` first: the prepare's admission
(`Frontend.admitPrepare`) counts the description, data and record, and a Tx whose aborted state was deleted saves
nothing more (`Tx.saveState` requires it; P9).

| Message sent | Durable first | Code |
|---|---|---|
| `TxPrepareResponse`, `TransactionVote` | txd, data, disposition, acceptor state | `Tx.answerPrepareAsAcceptor`, `sendDelayedPrepare` |
| `TxAcceptResponse` | acceptor state with `accepted = (id, v)`, the disposition as it then is | `Tx.receiveAccept` |
| `TxResolved`, `TransactionResolved` from a driver | `t` `Accepted`, each durable at its acceptor | `TransactionDriver.onResolution` |
| `TxCommitted` | every backend commit of the store for T completed; the CRL data drop enqueued | `Tx.allObjectCommitsComplete` |
| `TxFinalized(commit)` | every FinalizationAction completed | `TransactionDriver.startFinalizer` |

Not durable before sending: a proof (its evidence is the durable backend state), status-cache relays, a deleted
abort's `TxResolved(false)` (`Tx.answerResolved`), `TxCommitted` re-sends, status replies (10.10; a commit vote is
reported only once durable), and an expiry commit's `TxResolved`.

| Event | CRL effect |
|---|---|
| `TxResolved(false)` at a Tx | record deleted at once and its admission released (`Tx.stateDeleted`); the lingering Tx saves nothing more and answers prepares and accepts with the abort |
| a vote decided just before the freeze, its response dropped | saved without a reply (`Tx.saveUndurableVote`) |
| commit applied, every backend commit succeeded | object data dropped, record kept |
| `TxFinalized(true)` and every backend commit succeeded | record deleted, or replaced by an applied stub (9.1) |
| a backend commit failed | record with data kept until the store reloads |
| a recovered vote quarantining an object | record with data kept until the quarantine clears |
| a deletion of an object the Tx allocated | record re-saved without that object's update (`Tx.persistWithoutUpdateOf`) |
| expiry discard | record deleted (`Frontend.expireTransaction`) |

What is not persisted: the outcome at a store (saved only incidentally); the proposer's round or local proposal;
driver state; the status cache. After a crash a store knows only its vote and acceptor state, and re-learns the
outcome from a driver, the caches, a proof, a late commit, its UnackedCommits entry, or expiry. A record is saved on
every prepare and accept response, retransmits included: write amplification, not a safety issue.

---

## 9. Commit application and finalization

### 9.1 Resolution at a store

* `Tx.resolved(c)` sets `oresolution` once, ends any vote wait, records the status in the cache (unless the outcome
  came from an UnackedCommits entry), and if all objects are loaded applies it (`resolvedAndAllObjectsLoaded`);
  otherwise `completeLoad` applies it.
* Abort: delete the CRL state, unlock, release the objects.
* Commit first meets two holds:
  * the commit overtook the data (`commitAwaitsData`: not recovered, no client prepare seen, not finalized, data
    missing, or adopted data not yet durable): held without `TxCommitted`, so the driver does not yet count the store as
    having missed the update. Released by the data's save, a client prepare, or `TxFinalized`; after `TxFinalized` the
    store applies without the data, reports the object as uncommittable and repair restores it;
  * an earlier commit of an object it changes is in flight or held (`Tx.holdForTurn`): queued per object in the
    Frontend (`transaction-requirements.md`).
* `Tx.doCommit`: release the locks, then either apply nothing (an applied stub for T, or `notOwedHere`: T's changed
  objects reported in `objectCommitErrors`, so a non-late finalizer marks the store for repair) or `issueCommits`:
  `RequirementsApplyer.apply` re-checks the requirements with `applying = true`, skipping objects that already show T,
  one backend commit per changed object, and `TxCommitted` when all complete. A commit voter whose apply throws
  fail-stops the store (`Frontend.failStop`); one, live or recovered, whose re-check fails on a changed object
  quarantines the object until repair verifies it (`Frontend.quarantine`, transaction-requirements.md 8.5). A
  re-apply (a recovered commit vote, or one applying through its entry: `Tx.appliesThroughEntry`) first leaves out
  and quarantines every object on which it would insert a key now absent while the object is past T's start
  (`RequirementsApplyer.mayReinsertDeletedKeys`): the key may have been deleted since. Objects whose earlier backend
  commit failed, or that are quarantined, are not applied and are reported. A quarantining Tx keeps its CRL record,
  data included, until the quarantine clears (P13).
* **Forgetting a committed T.** Where its CRL record would be deleted (after `TxFinalized` and its backend commits), it
  is replaced by an applied stub if an UnackedCommits entry may name this store: `TxFinalized` listed it in `owed`, or
  it applied T through its entry (`Tx.stubDue`; `Frontend.transactionFinalized`, `concludeTransactionState`). A failed
  backend commit or a quarantine keeps the record, so no stub replaces it. A `TxFinalized` listing a store with no Tx
  writes a stub from `TxFinalized.startTimestamp`. A finalization that arrives while a commit is held for its turn is
  deferred (`Frontend.finalizationDeferred`); every deferring Tx of the txid is listed, a stub owed meanwhile passes to
  each, and the call that empties the list concludes the state.
* **Stub lifetime.** A stub is dropped (`Frontend.expireStubs`, at heartbeat) once T is past the horizon plus the
  expiry margin and the store holds no Tx for T, live, deferred, awaiting backend commits or quarantining. Below the
  floor no prepare creates a Tx for T, so the stub can suppress nothing. Stubs are therefore bounded by their rate times
  about 30 s. Encoding: `crash-recovery-log.md`.

### 9.2 Driver after a commit resolution

`onResolution(true)` starts the finalizer, giving it the counted commit voters (`countedCommitVoters`: the current
round's commit voters if they reach `t` on every object, else none, meaning "every silent store") with their
incarnations, and the `commitErrors` from `TxCommitted` messages. The finalizer runs the txd's FinalizationActions
(at-least-once, idempotent), the missed-update marker among them, all at once; its `complete` waits for every action
and fails with the first failure (`server/RegisteredTransactionFinalizerFactory.Finalizer`). An action whose type
cannot be loaded, or whose factory throws, is a failed action, not a failed finalizer creation (`Finalizer.of`). So
an UnackedCommits entry durable means T committed, not that the other actions are done; only `TxFinalized` implies
that. Missed
updates are recorded in the ErrorTree and UnackedCommits trees by `MissedUpdateFinalizationAction`
(`transaction-recovery.md`).

### 9.3 Heartbeats and lost messages

A driver's `heartbeat` sends `TxHeartbeat` (not on its first tick) or, once finalized, repeats `TxFinalized`. A store
that sees nothing from a primary-pool driver for `txHeartbeatTimeout` drives T itself, which is the recovery path for a
lost `TxFinalized`: the new driver's prepares draw status-cache relays from the stores that finalized. The cache (30 s)
outlasts the horizon plus that detection (P17). A store whose driver still learns nothing past the horizon meets
refusals, and its Tx ends by expiry, a resolved one concluding its known outcome (`Tx.concludeKnownOutcome`).

---

## 10. Edge cases, the horizon and expiry

### 10.1 Duplicate prepares

Re-processed idempotently: the acceptor re-promises, the vote is unchanged (P4), the response is saved and sent again.
A committed Tx also re-sends `TxCommitted` to the sender (`Tx.receivePrepare`). `lastProposer` is overwritten by every
prepare, client retransmits included, so a client retransmit after a recovery driver's prepare can redirect this
store's later `TxCommitted` to a dead leader. The recovery driver's finalizer then marks a store that did apply: extra
repair, no safety effect.

### 10.2 Late prepares

* Tx known: as above.
* Tx unknown, cache entry finalized: the Store answers `TxFinalized` (and `TransactionFinalized` to the client), late if
  committed; the Frontend ignores the prepare and releases its admission.
* Tx unknown, cache entry resolved but not finalized, T at or above the floor: the Store sends a `TxResolved` of the
  entry (late if the entry is) addressed to itself (`to = msg.to`, P-W6), and the Frontend creates a Tx with
  `outcomeDecided`: no locks, still votes, answers status queries with the cached outcome. The self-addressed relay
  arrives after the Tx and any driver exist, so the new Tx resolves from it and the store's own driver learns it; the
  sender learns nothing from it. The designated leader drives such a T on purpose: its deciding driver may have died
  on another host, and the self-addressed relay resolves the new driver at once.
  The same relay is sent whenever a resolved entry exists, also to a store that holds a Tx (a duplicate there).
* Tx unknown, no outcome cached, T below the store's floor: refused, no Tx at all (`Frontend.refusePrepare`); the
  designated leader starts no driver. The rebuilt store's `acceptorSince` mute is checked first and still creates its
  muted, possibly proving Tx.
* Tx unknown, no outcome cached, T at or above the floor: a fresh Tx, checked normally. A replay of a committed pinned
  transaction fails its pin; a store showing its update proves it (6.3). Accepted cost: a replay whose requirements pass
  holds commit-vote locks until a driver resolves it.
* Dropped under CRL backpressure (`Frontend.admitPrepare` `Full`: an unseen transaction while the device's admission
  is at its lane's ceiling, or uncounted object data past it), which asks the CRL to grow (`crl.requestGrowth`): the
  client retransmits, and the rest of the quorum can decide without this store, which repair catches up. A known
  transaction's data-less prepare, and one finalized in the cache, are admitted without a check.
* Dropped as `Oversized`: the transaction's counted bytes here (with data already counted, for a known one) exceed
  `crl.maxTransactionBytes`. Nothing is charged; the store stays as one that never received the prepare (or the
  data), warned once per `Frontend.OversizedLogPeriod` (10 s). Details: `crash-recovery-log.md`.

### 10.3 Duplicate and late accepts

A `TxAccept` for an unknown Tx is ignored, so a store that forgot T never acts as an acceptor with fresh state through
an Accept; the driver interleaves prepares for that reason. A duplicate Accept is re-accepted and re-answered;
`acceptedPeers` and the `Learner` bitset make that idempotent. An Accept may reach a store that has not voted yet: it
accepts (Paxos allows it) and saves its then-current, possibly `Undetermined`, disposition. A muted or frozen Tx
ignores an Accept; one whose aborted state was deleted answers it with `TxResolved(false)` (P9).

### 10.4 Data overtaken

A driver's prepare that overtakes the client's makes the store wait for the client's prepare (6.2). A commit that
overtakes the data is held (9.1). A client prepare with data that arrives after `TxFinalized` is ignored with a
warning (`Frontend.receivePrepare`); the store is repaired. A client prepare first reaching a store 500 ms or more after
T started is logged (`Tx.LateClientPrepareAfter`).

### 10.5 Proofs and late commits

* **Proof holder.** A driver receiving a proof sets `holdsProof` and `Proposer.commitProven`, and the first proof
  starts a new round (`receiveTxResolved`): promises to earlier rounds may predate another driver's choice of commit.
  It skips the tally and sets no local proposal. It adopts only an accepted commit reported in the current round
  (commit was chosen at some ballot `c`, every value accepted at `c` or higher is commit, so an accepted abort predates
  `c`), sends Accepts for it (if it may propose) and resolves normally.
* **Late condition 1.** A proof plus a current-round prepare quorum of valid acceptors none of whose promises carries
  an accepted commit (`Proposer.prepareQuorumWithoutAcceptedCommit`; an older accepted abort counts as memoryless):
  `resolveLate`. Argument: the proof shows a quorum Q accepted commit before the proof was sent; Q intersects the
  responders; an acceptor forgets a committed T only on `TxFinalized` or a late outcome; a rebuilt acceptor is muted;
  so T's finalization completed. The argument needs a commit chosen by Paxos, so the condition is taken only while T
  is inside the horizon by the driver's own clock (`TransactionDriver.Start.pastHorizon`). An expiry commit
  (`Frontend.decideExpiry`) is accepted by no acceptor: its holders not yet frozen by their own clocks answer
  memoryless promises while the expiry driver's finalizer still writes its marks, and a late outcome would cancel that
  finalizer (`receiveTxFinalized` -> `resolveLate`), leaving stores unmarked and never repaired. It is decided only
  past the horizon plus the expiry margin by the decider's clock, and the margin (8 s) covers the inter-host skew
  (`RebuildingStore.AcceptorSinceMargin` less `MaxStartLead`), so it exists only once T is past the horizon by every
  host's clock. Past the horizon the condition would prove nothing in any case: a store that forgot T refuses the
  prepare (forget floor), and inside its own horizon answers by the finalized relay (late condition 2). A proof holder
  past the horizon waits for a `TxFinalized`, a late message or an accepted commit, or is stopped with its store's Tx
  (`Store.removeExpiredDrivers`).
* **Late condition 2.** A `TxResolved` or `TxFinalized` with `late = true`, including a store's finalized relay.
* **Late resolution** runs no finalization actions, cancels any it started, finalizes at once, and every
  `TxResolved`/`TxFinalized` it sends carries `late = true` with an empty `owed`. A late commit after an abort
  resolution is a split decision and is ignored.
* **A Tx receiving a late commit** (`Tx.lateCommit`): already resolved committed with a stub due or held, it
  finalizes; resolved committed with neither, it looks its own entry up first (`forgetLookupHeld`), writes a stub if
  found and is forgotten either way (past the horizon plus the margin it is forgotten without waiting,
  `Tx.forgetWithoutStubPastExpiry`). An undecided commit vote with a stub applies nothing; one without a stub looks up
  its entry and applies only if it is found naming its incarnation (`lateCommitHeld`, `unackedCommitFound`,
  `unackedCommitAbsent`). Anything else (abort vote, no vote, muted, prover, abort learned) applies nothing.
* **Found entry as a proof.** A commit vote that found its entry gives its own driver a proof (`Store.giveProof`).

Under the horizon, proofs and late resolution are optimisations inside the window: a driver without the proof cannot
decide abort for a forgotten commit, because inside the window every store that knew T answers from its Tx or its
finalized cache entry, and past its floor it refuses.

### 10.6 Status cache

* Write sites: the Tx constructor (`Unresolved`, or a recovered status), `Tx.resolved`, `Tx.finalized`,
  `Tx.applyNothing`, and the Frontend's no-Tx `TxResolved` (not proofs; finalized and late if the message is late) and
  `TxFinalized` (finalized) paths. All go through the monotone `updateStatus` (P17).
* Read sites: the Store's relay and its designated-leader check (no driver for a finalized entry),
  `Frontend.admitPrepare` (a finalized entry is admitted without a check), `Frontend.receivePrepare` (ignore if
  finalized; `outcomeDecided` if resolved), `Frontend.receiveStatusRequestWithoutTx`, and `Tx.statusForExpiry` of a
  Tx created from a cached outcome (it reports the entry as the cache now holds it).
* `Tx.resolved` records nothing for an outcome learned from an UnackedCommits entry (`unackedCommitFound`,
  `unackedCommitNotOwed`: `record = false`), so a finalized entry always stems from a real `TxFinalized` or a late
  outcome.
* One cache per host, shared by its stores and keyed by transaction id. An `Unresolved` entry counts as no outcome.
* A size eviction raises the host's eviction watermark (wall clock + `MaxStartLead`) synchronously with the removal
  (`evictionListener`), a floor component, so an evicted entry's absence is never trusted; it is logged at error at
  most once per 10 s with the count. Expiry by time raises nothing: an entry expires only once its transaction is below
  the floor anyway.

### 10.7 Dueling proposers

Contention is resolved by backoff (7.3); stale responses are filtered by proposal id. Nacks carry the promised id, so
a new round starts above it.

### 10.8 Unknown or moved stores

`TxUnknownStore` makes the driver drop its endpoint cache for that store, so the next retransmit looks the endpoint up
again; the transaction proceeds without the store if a quorum remains, and repair catches it up.

### 10.9 Concurrent drivers

* Leader plus recovery drivers all run Paxos with distinct ids (P2). The client's retransmit at `(1, L)` is Nacked by
  acceptors that promised a higher id; the Nack goes to the leader's driver, which moves to a new round. Whichever
  driver learns first announces the outcome; the others learn it from `TxResolved` or `TxFinalized`.
* Several drivers may run FinalizationActions (at-least-once, idempotent). A driver reached by another's non-late
  commit `TxFinalized` cancels its own finalizer and finalizes at once (P14).
* A recovery driver racing a cache relay: the first learned value wins (P19).

### 10.10 Transaction horizon, forget floor and expiry

**What it removes.** A store forgets T's outcome after a bounded time. Without a bound on joining, a prepare reaching
the stores after they all forgot T would start a new Paxos instance for the same txid, which could decide the opposite
outcome: a commit re-decided abort, or an abort re-decided commit. Proofs cannot close that in general (traceless
changes, overwritten revisions), and memory cannot be infinite. The fix is a time bound on joining.

**The forget floor** (`Frontend.forgetFloorMillis`) is the latest of:
* `wallClockMillis() - horizon`;
* the load part, `Frontend.loadFloorMillis`. Every restart, adoption and transfer-in builds a new Frontend through
  `StoreLoader`. Normally it is `min(load + MaxStartLead, JoinCeiling.boundAtLoad)`: before its load the store may have
  joined, and forgotten, a T started up to `MaxStartLead` ahead of its clock, but never one at or above the join ceiling
  its previous life on this host ran under (below). A store that arrived from another host (a transfer in,
  `StoreTransfers`; an adoption, `DeviceDiscovery.bringIntoService`) durably writes `StoreLoader.ArrivedAtFile` before it
  can load here; a load less than `AcceptorSinceMargin` (10 s) after the arrival uses `load + AcceptorSinceMargin`,
  and a later one keeps `arrival + AcceptorSinceMargin` as a lower bound, covering the other host's clock lead. A store
  this process just created for a pool's Initializing slot (`StoreRebuilds.createNewStore`, `StoreLoader.newStores`)
  has never served and gets load time 0;
* the host's eviction watermark (`TransactionStatusCache.evictionWatermarkMillis`).

A prepare for a transaction the store does not hold (no Tx, no non-`Unresolved` cache entry) whose start is below the
floor is refused: no Tx, promise, vote, reply or CRL save, its admission released, a rate-limited warning for a client
prepare (`Frontend.receivePrepare`, `refusePrepare`). A resolved but unfinalized cache entry does not exempt it (another
store's Tx on the host may have written it after this store forgot T). The designated leader starts no driver for it.
A held Tx, recovered ones included, is unaffected.

**The join ceiling** (`server/host/JoinCeiling`). The host keeps a durable upper bound on the start of every T any of
its stores joined, renewed every 250 ms to now + `MaxStartLead` + 1 s and only ever raised. A store joins an unknown T
only if T started below the ceiling already persisted (`Frontend.startsTooFarAhead`, the same mute as a start too far
ahead). So every T a store joined in an earlier life on this host started below the ceiling that life ran under: the
previous process's (`JoinCeiling.atStart`) for a store's first load in a process, the current one for a reload. A
ceiling that cannot be read bounds nothing (the load floor applies alone). A failed renewal removes the file and stops
gating; if the removal fails too, the stale ceiling keeps gating, at a liveness cost only.

**Muting a start too far ahead.** A prepare for an unknown T whose start leads the host's wall clock by more than
`MaxStartLead`, or is at or past the join ceiling, creates a muted Tx (no promise, vote, accept or save; it may still
prove), forgotten once its objects load. The client's retransmission is answered once the clock catches up; the
designated leader does not drive such a T meanwhile, so no prefer-abort round aborts it. The bound uses the host's
wall clock, not the JVM-wide HLC, which the host's own client ratchets forward from the timestamps it reads. Since every
accepted start, and so every object timestamp, is at most a host's wall clock plus 2 s, client HLCs ratcheted from
them are capped the same way.

**The rebuilt store's mute.** A rebuilt store has `acceptorSince` (`RebuildingStore.writeAcceptorSince`: the HLC at
the rebuild's finish plus 10 s). A prepare for an unknown T started before it creates a muted Tx, below the floor too.
The old incarnation stopped before the rebuild began, so every T it joined started before
`wall_old(stop) + MaxStartLead`, which is before `acceptorSince` unless the old host's clock led the new one's by more
than 8 s plus the rebuild's duration.

**The rebuilt store's inherited votes.** The mute keeps the rebuilt store out of T, but a commit vote its old
incarnation cast still counts: a driver keeps a vote in `peerDispositions` until a nack or a proof starts a new round,
and a muted store sends neither; a client's vote count (`ClientTransactionDriver`) is final; rule B places no condition
on lock-only pools. The rebuilt store holds none of T's locks, so it votes abort (`InheritedVoteLock`) on every object
until it knows which transactions its old votes may still back, then on their objects until each is decided
(`Frontend.inheritedVoteLock`; rebuild-process.md, the rebuilt store's acceptor state). Otherwise, with `2t - w = 1`
(Replication(3,2), (5,3)), the rebuilt store plus T's abort voter commit a conflicting T' on a lock-only object and T
then commits with its lock requirement broken.

**Why the floor timing is safe without cross-host clock agreement.** A store writes its cache entry for T after it saw
T, when by `MaxStartLead` its clock read at least `start - 2 s`. The entry lives 30 s past its last write, so when it
expires the store's clock reads past `start + 28 s`, beyond the 20 s horizon: the store already refuses T. A restart or
size eviction loses memory at a moment that raises the floor past every T the store might have joined before it on
this host. A transfer-in or adoption loses the previous host's memory; every T the store joined there started at most
`MaxStartLead` plus the inter-host skew ahead of this host's clock at the arrival, within `AcceptorSinceMargin` under
the 8 s skew bound. So a store never forgets T while it would still join it. Same-host reasoning uses only the store's
own clock; client clocks enter only through `MaxStartLead`.

**Rejoin invariant.** Once a store discards or forgets T, its floor makes it refuse every later prepare for T: a
forgetful acceptor is indistinguishable from an offline one, and Paxos is safe with offline acceptors. A store never
rejoins a transaction it forgot. FrontendSuite "after expiry, a later prepare for T creates no Tx"; HorizonIntegrationSuite
"a host restart does not let its stores rejoin a transaction they forgot", "a store transferred to another host does
not rejoin a transaction it forgot".

**Status query.** `TxStatusRequest` carries `startTimestamp`. A store with a Tx answers with `Tx.statusForExpiry`, but
an unresolved Tx answers only once its reported state is final: frozen with its objects loaded, or muted for good
(amnesiac, or below the floor). A commit vote is final only once durable: `votedCommit` is reported, and a Tx's own
expiry round counts its own vote, only after a CRL save holding the vote completed (`Tx.voteDurable`; a recovered vote
is durable), and an unresolved commit voter whose save is still in flight is silent until it completes. Rules A and F
count each `votedCommit` as a voter holding T's locks, so the vote and its locks must survive a crash. A resolved Tx
answers at once, and so does a Tx created from a cached outcome (`Tx.outcomeDecided`, which holds no locks): it reports
that outcome as the cache now holds it, in its own expiry round too, so it is adopted (rule 1) and its vote is never
counted. A store without a Tx answers from a non-`Unresolved`
cache entry; a rebuilt store for T before its `acceptorSince` answers `amnesiac` (with any cached outcome); otherwise it
answers `None` ("unknown") only if T is below its floor, which makes the answer permanent, and stays silent above it
(`Frontend.receiveStatusRequestWithoutTx`). A store still loading its CRL holds the request with every other message.

**Freeze.** A Tx past T's expiry by its own store's clock (`Tx.frozen` = `Frontend.pastExpiry`) promises and accepts
nothing more, as if offline, which Paxos allows. A frozen Tx decides no vote and sends no delayed prepare response, so
one still undecided then (an in-flight commit, a collision, a client prepare arriving late) is a final non-voter. A
vote decided just before the freeze whose response is then dropped is still saved (`Tx.saveUndurableVote`), so its
status reply reports it once durable instead of staying silent.
Derived from the host's wall clock, so a reload cannot thaw it; a backward step of that clock can (12).

**Expiry.** A Tx still unfinalized past the horizon plus the expiry margin (`Tx.mayExpire`: not finalized, loaded, no
late commit or forget-lookup pending; unresolved, or resolved and waiting for a `TxFinalized` that may never come)
queries every other store of T at store heartbeat, at once and then after 1, 2, 4 and every 8 heartbeats, each round
with a fresh request id counting only its own replies plus the Tx's own state (`Frontend.expiryHeartbeat`,
`startExpiryRound`). The pure `ExpiryTally.decide` decides.

Rule 1, **Adopt**: a reply with an outcome is adopted, commit over abort. A commit is adopted through `TxResolved`
(late if finalized or late, so a commit voter looks up its entry), an abort as a finalized abort. A Tx that knows its
outcome (`known`) adopts only a finalized or late commit of a known commit, and otherwise concludes once more than
`w - t` primary stores are non-holders (no acceptor state and not amnesiac; a commit is forgotten only at
`TxFinalized`), or holds (`ExpiryTally.decideKnown`).

*Lemmas.* Both follow from immutable votes and P7a.
* **L1.** Every accepted abort traces back to an abort-certain tally: a local proposal is abort only on one;
  `Proposer.proposalValue` otherwise re-proposes an accepted value; a proof holder proposes only an accepted commit of
  its post-proof round; expiry and late resolution send no Accept. So if any store holds an accepted abort, some object
  never had `t` commit votes: no commit was chosen, and no client was told commit.
* **L2.** Every accepted commit traces back to a commit-certain tally or a proof. Either way commit is T's only outcome.
Abort-certain and commit-certain exclude each other, so accepted aborts and accepted commits never both exist.

*Counts*, per pool of every referenced object (changed or lock-only), with that pool's `t`: `vc` replied and voted
commit (`votedCommit`); `k` amnesiac; `s` silent. Every other replier is a provable non-commit-voter: "unknown", or a
reported Tx that did not vote commit. In the primary pool `aa` / `ac` stores report abort / commit as their latest
accept (amnesiac ones excluded), `k_p` is its `k`, and `n_p` stores replied "unknown". Each changed pool has its `r`.

Rules A-G, in order (`ExpiryTally.decideUnknown`):
* **A. Commit by votes.** Every referenced pool has `vc >= t`. No tally was abort-certain, so by L1 no abort was
  accepted, and `vc >= t >= r` holders can restore the update.
* **B. Commit, accepted.** `ac >= 1` and every changed pool has `vc >= r`. By L2 commit is the only outcome.
* **C. Discard, accepted abort.** `aa >= 1`. By L1 no commit was chosen or reported.
* **D. Discard, data lost.** Some changed pool has `vc + s < r`: the update can never be restored. `dataLost`, logged
  as an error, when a commit may have been chosen or reported: `ac >= 1`, or every pool has `vc + k + s >= t`.
* **E. Discard, commit impossible.** Some pool has `vc + k + s < t`: no tally was ever commit-certain.
* **F. Commit by survivors.** No store of T is silent, every pool has `vc + k >= t` and `vc > w - t`, `k_p + n_p < t`,
  and every changed pool has `vc >= r`. B and C did not fire, so every primary store that still holds T and is not
  amnesiac accepted nothing; only the amnesiac and "unknown" stores, fewer than a quorum, could hide an accepted abort,
  so no abort was chosen and no client heard one. Frozen acceptors and muted rebuilt stores accept nothing more. A
  conflicting commit would need `t` votes in some pool of T, but T's `vc` voters there hold its locks and `vc + t > w`;
  a changed pool has `vc > w - t` from `vc >= r`, a lock-only pool needs it stated, since its amnesiac stores hold no
  lock. ("Unknown" counts with `k_p` because a store that accepted, decided and forgot T and let its cache entry expire
  answers "unknown" too; with a changed primary object `n_p` never matters, since `vc_p >= r` leaves at most
  `w - r < t` others.)
* **G. Hold** otherwise: some store is silent (transient), or, every store replied, `k_p + n_p >= t` (with a changed
  primary object D fires first, so only a lock-only primary object reaches it) or some lock-only pool has
  `vc <= w - t`. Both are logged `UNDECIDABLE TRANSACTION`, then every `Frontend.UndecidableLogRounds` rounds.

*Actions.* A commit (`Frontend.decideExpiry`, Commit) sends non-late `TxResolved(commit)` to every other store of T and
to itself, and has a driver of its store finalize it (`Store.finalizeExpiryCommit`), whose missed-update marks repair
the stores that never applied it. Applying late is safe: a commit voter's locks have held its locked parts unchanged
since its vote (in memory, or through its CRL record and the recovery hold), repair defers to a store's own locks even
with a proof (`Reconcile.RepairSnapshot.proven`), a conflicting commit would need a locked voter, and writes T does
not lock are order-independent. A discard (C, D, E) releases locks and objects, deletes the CRL state, forgets the Tx
and leaves the status cache alone (`Frontend.expireTransaction`). An abort voter, a Tx with no vote, a prover or a muted
Tx is discarded at once; a commit voter first looks up its UnackedCommits entry (`Tx.decideExpiryDiscard`): none found
by a lookup started after the decision discards it, one found applies T as a late commit, a failed lookup keeps it
holding; it never discards on a failure. A Tx that knows its outcome concludes instead (`Tx.concludeKnownOutcome`): an
entry-applied commit as finalized with its stub, another commit as a late commit of a transaction applied here, an
abort as finalized. An expiring commit vote also holds an abort for a lookup, as a recovered one does, and its store
drives it meanwhile.

*"Unknown" on the discard side.* "Unknown" only lowers `vc`, so it can push a tally toward discard, never toward commit,
and a discard at the asking store is safe in every case: if T aborted, discarding is right; if T committed and was
finalized, a counted commit voter that never acknowledged has an UnackedCommits entry, written before any acceptor
forgot T (P14), which its pre-discard lookup finds; every other asker holds nothing it is owed and is repaired from its
ErrorTree mark.

*Consistency across observers.* No two observers reach a commit (A, B, F) and a discard (C, D, E). Reported states are
final, and a store leaves a count only by deciding, after which it reports its outcome (rule 1), so observers differ
only in which stores are silent to them and which have since forgotten T:
* A or B against C: L1, L2 and the exclusion of the two tallies.
* A or B against E: a tally was commit-certain, so every pool truly has `t` commit voters, each still voting, amnesiac
  or silent to the other observer, unless it has forgotten T, which a voter does only after T is decided (here commit),
  the safe discard-side case.
* A, B or F against D: the commit needs `vc >= r` at its observer, D needs `vc + s < r` at its own, and a store silent to
  one observer only adds to the other's `vc + s`.
* F against C: F's observer sees every primary store and finds no accepted abort; C needs one reported by a
  non-amnesiac store still holding T. One divergence remains, accepted: C's observer saw an abort accepted by fewer than
  a quorum (never chosen), the accepter then discarded or was finalized and forgot T, and F's observer later counts it
  "unknown". No client was told abort, F's voters exclude a conflicting commit, and the commit's missed-update marks
  repair the discarder.
* F against E: F's observer has no silence, so its `vc + k` counts every commit voter still holding or amnesiac, and E's
  observer counts at least that in `vc + k + s`.

*Why a rebuilt store counts as amnesiac.* Its acceptor state was lost with T undecided; counted as "unknown", it would
let the last holders of a chosen commit discard it beside one store that never heard of T. HorizonIntegrationSuite
"holders of a chosen commit apply it past expiry though a rebuilt store forgot it".

*Liveness* with every store replied and none amnesiac: every store is a commit voter or a provable non-voter, so A or E
fires. Holding is the answer while stores are silent, and with every store replied only when `k_p + n_p >= t` or some
lock-only pool has `vc <= w - t`.

*Drivers and the client past expiry.* A driver past the horizon plus the margin with no outcome is stopped and removed
once its store holds no Tx for T (`Store.removeExpiredDrivers`); one with an outcome keeps finalizing.
`SimpleClientTransactionDriver` stops at its clock's start + horizon + `MaxStartLead` and completes `Unknown` (or keeps
the `Committed` it learned from votes); `TransactionImpl` fails the commit with `TransactionOutcomeUnknown` and records
nothing in the client's status cache. `TransactionAborted` is final. `StopRetrying.unlessCollision` and the other retry
sites retry Unknown as they do an abort (10.11).

*Tests.* ExpiryTallySuite (one test per rule, `dataLost`, the forgotten-primary and lock-only cases);
ExpiryTallyConsistencySuite (exhaustive over the true final states of small pools, changed and lock-only, with
forgotten stores: no observer commits over a chosen abort or where a conflicting commit was possible, and no split
other than the accepted C-then-F one); FrontendSuite and StoreSuite floor and expiry tests; HorizonIntegrationSuite;
FastCommitIntegrationSuite; TransactionDriverSuite.

### 10.11 Retried unknowns

`TransactionOutcomeUnknown` may mean committed, and every `transactUntilSuccessful*` call re-runs its `prepare` with
fresh reads against what that attempt wrote. Safety is not at stake (a retry whose reads predate the commit's apply
collides with it and re-reads); the question is what the re-run does with work already done.
`StopRetrying.collisionRetry()` returns a `CollisionRetry` that records whether an earlier attempt ended Unknown
(`mayHaveCommitted`). A site that is not naturally idempotent accepts its own post-commit state as success only then
and stages nothing; "its own" is decided by an id or key fixed outside the retry closure, or by the exact state the call
writes, and post-commit announcements the committed attempt's `tx.result` would have made are made by the retry.
Guarded this way: `HostServices.initializeTaskExecutor` (an existing root is used), `BaseAspenClient.failStorageDevice`
and `createSystemDurableTask` (one task id per call), `SimpleTaskExecutor.allocateTaskNow` (one key per call),
`StoreRebuilds.requestRebuild`, `DeviceMetadata.adoptDevice`, `AspenClient.abortStoreTransfer`, `transferStore`,
`replaceRebuild`, the `BaseAspenClient` creates (`runIdempotentCreate`), and
`DeviceMetadata.updateStateForTransferredStore`/`markRebuiltStoreActive`. Tests: CommittedUnknownRetrySuite,
DeviceAdoptionSuite, SimpleTaskExecutorSuite; `TestNetwork.reportCommitAsUnknown` has stores commit while the client is
told Unknown. Accepted residuals are listed in 12.

### 10.12 Abort paths audit

P-VC says that once every object of T has `t` commit votes, T resolves only as commit. Let `c_o >= t` be the commit votes
on each object `o`; each store has one vote, so `a_o <= w - c_o <= w - t`.

| Path | Why it cannot abort T once `c_o >= t` everywhere | Code |
|---|---|---|
| Driver tally, abort-certain | Needs `a_o > w - t` on some object | `TransactionDriver.receiveTxPrepareResponse` |
| Driver tally, undecided | Never proposes abort (P7a) | same |
| `Proposer.proposalValue` adopting an accepted abort | Every proposal source is the tally (above), a re-proposal of an accepted value (recursively the same), or a proof holder's accepted commit; expiry and late resolution propose nothing (L1) | `Proposer.proposalValue` |
| Proof holders and late resolution | A proof means T committed; `resolveLate` resolves commit only; a proof holder never proposes abort | `TransactionDriver.resolveLate` |
| Status-cache relays | Repeat an outcome a real resolution wrote; the cache is monotone, Committed never replaced | `Store.handleTransactionMessage`, `TransactionStatusCache.updateStatus` |
| Expiry rule 1 | Repeats a reported outcome; commit preferred | `ExpiryTally.decide` |
| Expiry C, D, E | C needs an accepted abort (L1: impossible). E needs `vc + k + s < t`, impossible while each voter holds T, is amnesiac or silent; a voter answers "unknown" only after T was decided (commit, finalized), the safe discard side. D fires only below the read threshold: a loss beyond tolerance, logged `dataLost`, or stores that forgot a finalized commit | 10.10 |
| Expiry discard of a store owed the commit | A commit voter makes its UnackedCommits lookup before discarding; a finalized commit's entries are durable before any acceptor forgot T | `Frontend.decideExpiry`, `Tx.needsUnackedCheck` |
| UnackedCommits held aborts | An abort proposal exists only as above (L1) | `Tx.heldAborts` |
| `NoClientPrepare` votes | Cast only by stores that never saw the client's prepare, so not among the `c_o` voters; they count in `a_o <= w - c_o` | `Tx.awaitingClientPrepare` |
| `InvalidDescription` votes | Every store of the pool votes abort, so `c_o = 0` there | `Tx.invalidDescription` |
| A prover's saved abort vote | Not a commit voter; T committed; counts in `a_o` | `Tx.proveCommitted` |

`TxResolved(false)` and `TransactionResolved(false)` are sent only by a driver whose `Learner` saw a quorum accept abort,
or as a relay of one (P10). The client never completes `Aborted` on votes. The one divergence the expiry rules admit
(C-then-F over an unchosen abort) involves no abort a client was told or Paxos chose. On the commit side, F commits only
where every pool has more than `w - t` live commit voters holding T's locks; a reported commit vote is durable, so
its locks survive a crash of its store.

---

## 11. Failure scenarios

"Safe" = no split decision, no loss of a decided commit, no applied update of an aborted transaction. "Live" = the
transaction eventually resolves without operator action given a healthy quorum.

### 11.1 Message-level faults

| Fault | Outcome | Why |
|---|---|---|
| Client `TxPrepare` lost to some stores | Safe, live | The client retransmits; a store that has seen no client prepare withholds up to 5 s, then votes abort; commit needs `t` commit votes per object. |
| `TxPrepare` lost to the leader | Live after a delay | Other stores' responses wait in the Store's prepare-response cache (10 s) for a later driver; the client retransmits; an idle Tx starts a recovery driver after 5 heartbeats. |
| `TxPrepareResponse` lost | Safe, live | The driver retransmits; the store re-answers with its persisted vote. |
| `TxAccept` / `TxAcceptResponse` lost | Safe, live | Retransmitted; the learner needs `t` distinct acceptors. |
| `TxResolved` lost | Safe, live | The driver re-sends it to stores not in `knownResolved`; `TxFinalized` carries the outcome too. |
| `TxFinalized` lost | Safe, live | The idle store's driver asks within the cache's life (5-6 s idle < 30 s); past it, peers refuse and the Tx concludes its outcome by expiry. It holds its locks meanwhile (a resolved commit holds none). |
| `TxCommitted` lost | Safe | The finalizer marks the store; repair reconciles exactly. |
| `TransactionResolved`/`Finalized` lost to the client | Safe | A commit is still learned from votes (FastCommitIntegrationSuite "the client learns commit from votes while every leader outcome to it is dropped"); a retransmit inside the horizon is answered from a Tx or the cache; past give-up the client reports `TransactionOutcomeUnknown`. |
| `TransactionVote` lost | Safe, live | Re-sent with every prepare response; the leader's outcome completes the client anyway. |
| Duplicates, reordering | Safe | 10.1-10.3; proposal ids filter stale responses; a `TxResolved` before any prepare only reaches the cache, so the later Tx learns at `TxFinalized`. |
| Message to a store on another host | Safe | `TxUnknownStore`; the endpoint is looked up again. |

### 11.2 Slow or partitioned clients and stores

| Case | Outcome |
|---|---|
| Client prepare reaches a store after a driver's | Promise and vote withheld up to 5 s, then abort; late data still adopted if the others commit (6.2). |
| Data reaches a store after the commit decision | Commit held until the data or `TxFinalized` (9.1). |
| Client partitioned longer than the cache life | It stops at give-up and reports Unknown; a retransmit landing past a store's floor is refused, so no store re-decides T. |
| Client clock ahead | A start more than `MaxStartLead` ahead of a store's clock is muted there until the clock catches up. |
| Client clock behind | A start more than the horizon behind is refused, logged with the lag. |
| Slow store | Votes late or never; the quorum decides without it; marked for repair if it missed the commit. |
| Leader partitioned from some stores | Its driver waits; recovery drivers start after 5 heartbeats of idleness; dueling resolved by Paxos. |
| Minority of acceptors partitioned | Proceeds. A larger partition: no progress, no unsafe decision. |
| A secondary pool below `t` reachable, or a silent store whose vote would decide | An abort-certain object decides abort at once; otherwise the driver waits and never proposes abort. Past the horizon plus the margin the holders decide by rules A-G, holding while a store stays silent; commit-vote locks are held meanwhile. |

### 11.3 Crash at each step

| Crash point | State after restart | Outcome |
|---|---|---|
| Client, before any prepare reaches a store | Nothing | Nothing happened. |
| Client, after some prepares | Stores that got its prepare hold Txs; an idle one starts a recovery driver (data-less, round >= 2) | Commits only if stores that got the client's prepare supply `t` commit votes per object; the others vote abort after 5 s. The client never learns (acceptable). |
| Store, before the vote's save completes | No record, or an older one; nothing was sent to a driver or the client | As if it never saw the prepare; T is below its load floor unless the join ceiling bounds it lower, so a retransmit may get a fresh vote. |
| Store, vote saved, response not sent | Record with vote and acceptor state; a commit vote re-locks | Retransmitted prepares get the persisted vote. |
| Store, after an Accept is saved | Acceptor state recovered | P8 holds. |
| Store, after learning commit, before applying | Recovered, usually `Unresolved`, with its vote and data; re-locks | Re-learns from a driver (driven at once), the caches, a proof or its UnackedCommits lookup. If every other store finalized and forgot, its entry applies it, and without one it acknowledged or did not count, so a late commit applies nothing. Past the floor its Tx ends by expiry, applying T through an entry. |
| Store, after the in-memory apply, before the backend commit is durable | Data still in the CRL (P13) | As above. |
| Store, after the backend commit, before the CRL data drop | Recovered; objects show the update | `doCommit` skips already applied objects. |
| Leader/driver, before Accept sent | Driver state lost | A recovery driver (round >= 2, above its own acceptor's ids) after the idle timeout. |
| Leader, after a quorum accepted, before `TxResolved` | Accepted state at `t` acceptors | A recovery driver's promise quorum intersects it and re-proposes the same value (P8). |
| Leader, during FinalizationActions | Stores hold the resolved commit and their records | A recovery driver learns it from relays or retained accepted values and re-runs the finalizer (at-least-once). |
| Driver, after `TxFinalized` partly sent | Some stores finalized | The others recover through their own drivers and the cache relay. |
| Backend commit failure | Record kept; the store votes abort on that object until it reloads | `Frontend.commitFailed`. |
| CRL thread death | The store stalls | Liveness only. |

---

## 12. Accepted limitations

* **Post-load refusal.** After a reload a store refuses, for good, every T it does not hold started before its load
  floor. With a join ceiling that is at most about 3 s past the host going down; without one (a first load on a new
  host, bootstrap stores, a ceiling that cannot be read) it is `load + MaxStartLead`; within the arrival margin of a
  transfer or adoption it is `load + 10 s`. Such Ts see the store as offline and usually commit through the others; on
  a single host, or after most of a pool restarts, they end `TransactionOutcomeUnknown` at about 22 s and are retried.
* **Size eviction** can raise the floor to about "now" on its host, making its stores refuse every not-yet-held
  transaction started before it. It needs about 130 k transactions/s per host sustained; it fails safe and is counted
  and logged. The refusal warning is limited to once per 10 s per store (`Frontend.RefusalLogPeriod`).
* **Backward wall-clock steps.** The cache's expiry runs on `nanoTime`, the floor on the wall clock; a host clock
  stepped backward by more than about 8 s (30 - 20 - 2) between a store joining T and its entry expiring reopens the
  window for that T. A backward step after T's freeze (`Tx.frozen`, `Frontend.pastExpiry`) thaws its Txs, which may
  then vote or accept after a status reply reported their state as final, so expiry observers can disagree. An
  inter-host skew beyond the 8 s bound breaks the arrival margin and the rebuilt-store mute in the same way; so would a
  zombie old copy of a rebuilt store (`systemic-loss-protection.md`).
* **Slow client clocks.** A client clock more than 20 s slow has its transactions refused.
* **Undecidable transactions.** Rule G with every store replied holds T, its locks and its CRL state indefinitely,
  logged `UNDECIDABLE TRANSACTION`. Reached only after rebuilds or forgotten stores reach a primary quorum with a
  lock-only primary object, or a lock-only pool with too few live commit voters. A repair read of a held allocation times
  out, and its entry is deferred at every scan (repair-process.md 12.8).
* **Data lost at expiry.** Rule D with `dataLost` discards a T that may have committed, because its update is held by
  fewer than the read threshold of a changed pool (rebuilds beyond `t - r` of the commit voters).
* **C-then-F divergence.** One store may discard by C over an abort accepted by fewer than a quorum while another later
  commits by F; no client heard abort, and the commit's marks repair the discarder.
* **Prefer-abort aborts.** Orphaned data-less transactions, and a live client's transactions whose prepares to a store
  are all lost for 5 s, abort there; the client retries. A recovery driver after a client crash may wait up to 5 s for
  promises from stores reached only by its own prepares.
* **Stuck waits.** An in-flight commit to a hung device withholds votes on that object for as long as it hangs
  (Backlog.txt, "Isolate every device's I/O"); expiry then treats the Tx as a non-voter once frozen. Commit-vote locks
  of an undecided T wait up to the horizon plus the margin (about 28 s) while a silent store's vote would decide.
* **Finalization stalls.** A finalizer failing on every attempt keeps its transaction unfinalized (alarmed after 60 s).
* **`lastProposer` redirect.** A client retransmit can redirect a store's `TxCommitted` to a dead leader; the store
  then gets an extra ErrorTree mark and repair (10.1).
* **Re-adoption inside the horizon.** A recovering acceptor holding T's accepted commit can, inside the window, join
  stores that never saw T in a post-proof quorum and adopt commit non-late; the finalization actions then re-run
  (idempotent; the allocation action's orphan entry is left for repair, `transaction-recovery.md`), and T applies only at
  never-saw stores, which are owed it.
* **Retried unknowns.** `FailedStorageDeviceDurableTask` (`tombstone`, `moveStore`) and
  `MigratePoolToSetDurableTask.markComplete` detect their own commit by throwing, which each step handles (a misleading
  log line, a lost best-effort nudge, one extra poll). `DiskCatalog.takeOwnership` may bump the fence epoch twice
  (harmless). `RebalancingDurableService.enroll` could enroll a stale plan again only if the task finished between the
  attempts. The "done" checks of `requestRebuild`, `transferStore`, `replaceRebuild` and `abortStoreTransfer` accept a
  state someone else produced, but only after an Unknown. Durable tasks enrolled by a larger transaction
  (`UpdateAllocationGroupUsageTask`, `FsifReleaseIndicesTask`, `MigratePoolToSetDurableTask`) mint a task id per attempt;
  their callers' own guards keep a retry from enrolling again.

---

## Open weaknesses

### P-W6. The status-cache relay of a resolved, unfinalized entry is addressed to the relaying store (CONFIRMED; liveness and hygiene)
Mechanism: `Store.handleTransactionMessage` (TxPrepare case) builds the relay as `TxResolved(msg.to, msg.from, ...)`;
the first field is `to`, so it is sent to the store itself, while the finalized relay beside it,
`TxFinalized(msg.from, msg.to, ...)`, goes to the sender. StoreSuite "a relayed resolution of a non-finalized entry
carries the entry's late flag" checks the flags but not the address.
Trigger / scenario: a recovery driver on store A prepares T at store B, whose host cache holds T resolved but not yet
finalized (another store on B's host resolved it; B never held a Tx). B relays the outcome to itself: its new
`outcomeDecided` Tx resolves, and B's own driver if any, but A gets at most a promise and B's vote, never the
outcome.
Consequence: no safety effect. The driver learns T only from Paxos (a quorum's accepted values) or a later finalized
relay, so a takeover is slower than designed. The designated leader's fast
takeover (10.2) works only because the relay reaches its own new driver.
Suggested fix: send the relay to `msg.from`, and keep delivering it locally (or resolve the new `outcomeDecided` Tx and
the store's own driver directly), so both the sender and the designated leader's own driver learn it; assert the
address in StoreSuite. Fix it together with or after transaction-recovery.md R-W5: a restored tree-learned resolution
seeds the host cache with a non-finalized `Committed` for a T every peer forgot, and today its non-late relay reaches
only the relaying store; addressed to `msg.from`, it would also reach every remote recovery driver, which would then
resolve T non-late and re-run its finalization actions and marker.
Effort: S   Priority: 5

### P-W7. Orphaned scaladoc of Tx.determineDisposition (CONFIRMED; documentation)
Mechanism: in `server/transaction/Tx.scala` the scaladoc describing how the vote is decided ("Decides the vote, or
withholds it. A muted transaction is proven committed ...") is followed directly by the scaladoc of `invalidDescription`,
so it documents nothing, and `determineDisposition` itself has none.
Trigger / scenario: a reader or a doc tool looking up `determineDisposition`.
Consequence: the order of the vote rules (6.1) is hard to find in the code.
Suggested fix: move the first block onto `determineDisposition`.
Effort: S   Priority: 7
