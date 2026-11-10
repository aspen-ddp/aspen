# Paxos-based transaction resolution: safety specification

Status: derived from the code at commit `c3ad5493` (main); refreshed 2026-11-09 against `0d08e3c6` (branch
`pin_refactor`, replay pins: `docs/superpowers/specs/2026-11-09-replay-pins-design.md`). The horizon,
`TransactionTooOld` and the UnackedCommits name no longer exist; sections 3, 6, 10 and 12 describe the pin rule, late
commits and the UnackedCommits tree instead. Line numbers in passages the refresh did not touch are for `c3ad5493`;
refresh evidence is for `0d08e3c6`. Every mechanism is cited as `path:line`; paths are relative
to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`. Where this document disagrees with
`CLAUDE.md`, `Architecture.md` or `Todo.txt`, the code was taken as the truth and the disagreement is called out
(section 2).

Related documents (siblings, not duplicated here): [crash-recovery-log.md](crash-recovery-log.md) (CRL interface and
SimpleCRL), [transaction-requirements.md](transaction-requirements.md) (requirement checks, locks, commit-in-flight
ordering), [transaction-recovery.md](transaction-recovery.md) (driver and participant crash recovery,
MissedUpdateFinalizationAction), [repair-process.md](repair-process.md), [rebuild-process.md](rebuild-process.md),
[systemic-loss-protection.md](systemic-loss-protection.md). Design notes:
`docs/superpowers/specs/2026-11-07-per-device-crl-and-commit-ordering-design.md` and
`docs/superpowers/specs/2026-11-08-traceless-commits-design.md`.

Contents: 1 model; 2 design intent versus implementation; 3 invariants; 4 messages; 5 normal-case flow; 6 voting;
7 the driver (tally, rounds, learner); 8 persistence ordering; 9 commit application and finalization; 10 edge cases;
11 failure scenarios; 12 suspected flaws and open questions.

---

## 1. Model

### 1.1 Participants

| Role | Who | Notes |
|---|---|---|
| Client | `ClientTransactionDriver` / `SimpleClientTransactionDriver` | Builds the `TransactionDescription` (txd) and the per-store object data, sends prepares, retransmits until it hears an outcome. Not a Paxos participant. |
| Primary pool | the pool of `txd.primaryObject` | Chosen by `maxBy(poolIDAMap)` over all object requirements (`client/internal/transaction/TransactionBuilder.scala:113-116`). Its stores are the Paxos **acceptors**; its IDA gives the quorum (`ida.writeThreshold`). |
| Secondary pools | other pools referenced by the txd | Their stores **vote** (VoteCommit/VoteAbort on their own objects) but are not acceptors: the driver ignores their promises for Paxos purposes (`server/transaction/TransactionDriver.scala:251-252`, `isValidAcceptor` at `:144-146`) and `TxAccept` is sent only to `primaryObjectDataStores` (`:443`). |
| Designated leader | store `txd.designatedLeaderUID` of the primary pool | Chosen randomly by the client among stores believed online (`client/internal/transaction/DesignatedLeader.scala:19-28`). It hosts the normal-case **proposer and learner** (`TransactionDriver`). |
| Recovery driver | any store whose `Tx` has been idle for `Host.txHeartbeatTimeout` (5 heartbeats) | Same `TransactionDriver` class; started by `Store.heartbeat` (`server/store/Store.scala:67-78`) or `SimpleDriverRecoveryMixin` (`server/SimpleDriverRecoveryMixin.scala:19-29`). |
| Store-side participant | `Tx` (one per transaction per store, `server/transaction/Tx.scala`) inside a `Frontend` | Owns the **acceptor** (`Tx.scala:74`), the vote, the object locks, and commit application. |

Everything for one store runs on that store's executor (see "Host threading model" in `CLAUDE.md`); `TransactionDriver`
has its own lock and runs on timer/ec threads.

### 1.2 Identifiers

* `ProposalId(number: Int, peer: Byte)` ordered by number then peer (`common/paxos/ProposalId.scala:3-11`). `peer` is
  the **pool index** of the proposing store (`TransactionDriver.scala:88`: `new Proposer(storeId.poolIndex, ...)`).
  The client's prepare carries `ProposalId.initialProposal(designatedLeaderUID) = (1, leader)`
  (`ClientTransactionDriver.scala:77`).
* A leader driver starts at `(1, leader)`; a non-leader driver calls `nextRound()` in its constructor, giving
  `(2, self)` (`SimpleTransactionDriver.scala:48`, `Proposer.scala:48-52`).
* Paxos values are `Boolean` (true = commit). Store votes are `TransactionDisposition` {`Undetermined`, `VoteCommit`,
  `VoteAbort`} (`common/transaction/TransactionDisposition.scala`). Outcomes are `TransactionStatus` {`Unresolved`,
  `Committed`, `Aborted`}.

### 1.3 Quorum arithmetic

Acceptors are primary-pool stores `0..w-1`, quorum `t = writeThreshold`. `Proposer`/`Learner` `require(t >= w/2 + 1)`
(`Proposer.scala:8`, `Learner.scala:5`). The IDA constructors already enforce a majority write threshold: Replication
`writeThreshold >= width/2 + 1` (`common/ida/IDA.scala:203`); ReedSolomon and RandomAccessReedSolomon require
`restoreThreshold >= width/2+1` and `writeThreshold >= restoreThreshold` (`IDA.scala:408-415`, `:589-596`). So any two
write quorums intersect, which both Paxos and the "two conflicting transactions cannot both reach `t` commit votes on
one object" argument (see [transaction-requirements.md](transaction-requirements.md)) rely on.

---

## 2. Design intent versus implementation (read this first)

`Architecture.md:139-155` and `Todo.txt:11-14` describe the intended protocol:

> each DataStore writes its VoteCommit with the Paxos state, sends the Promise to **all** DataStores, tracks the
> promises, and once VoteCommits exceed the threshold for every object **self-generates the Accept** ... it is the only
> valid Accept in round 1 ... To resolve an abort, we have to move on to round 2. Recovery always tries to abort.

**The code does not do this.** What it does:

| Aspect | Documented intent | Code (verified) |
|---|---|---|
| Who receives the Promise/vote | every store | only the proposer named in the prepare's `from` (the leader): `TxPrepareResponse(m.from, ...)` at `Tx.scala:736` |
| Who generates Accept | each acceptor, locally | only a `TransactionDriver`: the sole construction site of `TxAccept` is `TransactionDriver.scala:444` |
| Who is the learner | each store (or the client) | the driver's `Learner` (`TransactionDriver.scala:89`, `:335`); the client's learner is commented out (`ClientTransactionDriver.scala:28`, `:45-59`) |
| Round-1 commit-only rule | commit may only be proposed/accepted in round 1; abort needs round 2 | **No such rule exists.** `Acceptor.receiveAccept` accepts any value at any `>=` promised id (`common/paxos/Acceptor.scala:24-35`). A leader tallying an abort proposes `Accept((1,L), false)` directly in round 1; a recovery driver (round >= 2) may propose commit if it tallies `t` commit votes (`TransactionDriver.scala:254-306`). |
| Latency | one round trip | 5 sequential one-way hops to the client (section 5), with two serial durable CRL writes (vote, accept) on the path. |

Consequences for readers of this spec:

* The statements "aborts require a new round" and "recovery always aborts" must **not** be used as invariants; they
  are not true of the code. Safety here rests on ordinary single-decree Paxos (P8) plus immutable, durable votes (P4,
  P6), not on a round-1 restriction.
* `Architecture.md`/`Todo.txt` should be updated, or the fast path implemented (flaw F1).

---

## 3. Invariants and guarantees

Each is stated as a property that must hold; "Code" gives where it is established. Items marked **(assumption)** are
not enforced by the Paxos machinery and depend on timing or state retention; they are the weak points listed in
section 12.

**P1 Quorum intersection.** The acceptor set is the primary pool's `w` stores and every quorum has size `t >= w/2+1`.
Code: 1.3.

**P2 Proposal-id uniqueness.** Two different proposers never use the same `ProposalId` for different values.
Intended mechanism: `peer` = pool index, `number` monotonically increasing per proposer. **Not fully enforced**: a
secondary-pool store can drive and shares the index space (F4); a restarted driver restarts its numbering (F5).
Acceptors accept an Accept whose id equals their promise (`Acceptor.scala:31`), so P2 is load-bearing.

**P3 Acceptor durability.** A `Promise` or `Accepted` (and the `Nack` that accompanies a same-path response) is sent
only after the acceptor state (`promised`, `accepted`) that justifies it is durable in the device CRL together with the
txd, the store's object data and its vote. Code: `Tx.scala:739-745` (prepare), `:420-431` (delayed prepare),
`:759-765` (accept); the CRL calls the handler only after the log entry is written (`DSYNC`,
`server/crl/simple/StreamWriter.scala:37-40`, `LogEntry.scala:38-53`).

**P4 Vote immutability.** A store's non-`Undetermined` disposition for a transaction is computed once and never
recomputed while the `Tx` lives; a recovered `Tx` keeps the persisted vote instead of recomputing it. Code:
`Tx.scala:400-401` (recovered with a vote skips `determineDisposition`), `:717-718` (only `Undetermined` is decided),
`:326` (`endCollisionWait`). (Exception: F13.)

**P5 Vote and promise are one durable record.** The disposition travels in the same `TxPrepareResponse` as the
`Promise` and is saved in the same `TransactionRecoveryState` (`Tx.scala:736-745`). A vote that a proposer has seen
therefore survives any crash of that store.

**P6 A commit proposal requires `t` durable commit votes on every object.** The driver proposes commit only if, for
every object in the txd, at least the pool's write threshold of that object's pool's stores reported `VoteCommit`
in the current round (`TransactionDriver.scala:261-296`, analysed in 7.2). Combined with P5 and P12 this means a
decided commit is backed by locked, durable commit votes at a write quorum of every object's pool.

**P7 Abort is always a valid free choice.** If no value has been chosen, any proposer may propose abort at any time
without violating anything (aborts apply nothing). So misclassifying a non-vote as an abort vote (F7) can cost
liveness/throughput but not safety, provided P8 holds.

**P8 Single chosen value.** For one transaction at most one of {commit, abort} is chosen by the primary pool's
acceptors, given P1-P3 and that the acceptor state of a possibly-chosen value is still held by a quorum-intersecting
set of acceptors (P9). The proposer picks the highest accepted value reported in its promise quorum before it falls
back to its tally (`Proposer.scala:27-36`, `receivePromise` `:76-84`).

**P9 Acceptor-state retention (assumption).** The state of a *possibly chosen* value is retained until it can no
longer matter. In code: a committed `Tx`'s CRL record (and in-memory acceptor) lives until `TxFinalized`
(`Tx.scala:790-800`, `Frontend.scala:537-551`). An **aborted** `Tx`'s CRL record is deleted at *resolution*, earlier
(`Tx.scala:624-627`, `Frontend.scala:481-483`); and after `TxFinalized` the store has no Paxos state at all. For a
transaction whose state a store has forgotten, safety is carried by *other* mechanisms: the pin rule (P16), the host
status cache (7 s, P17), `Tx.proveCommitted` with the late-commit rule (P15, 10.5), and the UnackedCommits tree. Since
the refresh nothing time-based remains in the prepare path.

**P10 Only the chosen value is announced.** `TxResolved`, `TransactionResolved`, `TxFinalized` are sent by a driver
only after its `Learner` has a quorum for one proposal id (`TransactionDriver.scala:337`, `:471-521`), or after
another party's resolution reached it (`receiveTxResolved` `:345-353`, `receiveTxFinalized` `:363-372`). Any store may
originate a `TxResolved(committed=true)` by proof (`Tx.proveCommitted`, P15). All receivers trust any `TxResolved`.

**P11 Application only on resolution.** A store applies a transaction's update to its objects only on: a non-late
`TxResolved(true)` (`Tx.scala:855-869`), a non-late `TxFinalized(true)` (`:874-890`), a found UnackedCommits entry
(`unackedCommitFound`, `:997-1026`), or a late commit that reaches a Tx holding its own commit vote whose entry is
found (`lateCommit`, `:920-932`). Never on its own vote, on an Accept, or on a proof: a prover applies nothing
(`applyNothing`, `:941-964`).

**P12 Commit-vote locks.** From the moment a store votes commit until it learns the outcome, the requirements of that
transaction hold locks on the store's object states (`Tx.scala:244-249`), including after a restart (a recovered commit
vote re-takes them, `:394-403`; the Frontend holds all messages until every recovered vote has loaded,
`Frontend.scala:279-312`). Details in [transaction-requirements.md](transaction-requirements.md).

**P13 Object data lives in the CRL until the backend commit succeeds.** `crl.dropTransactionObjectData` is called only
after every issued backend commit completed without error (`Tx.scala:604-613`, `Frontend.scala:542-551`); a failed
backend commit keeps the record for the life of the loaded store.

**P14 Finalization ordering.** A driver sends `TxFinalized(commit)` and `TransactionFinalized` only after all
FinalizationActions completed (`TransactionDriver.scala:496-499`). `TxFinalized(abort)` is sent immediately at
resolution (`:502-503`). A store deletes a committed transaction's CRL record only after `TxFinalized` **and** its
backend commits (`Frontend.scala:537-551`).

**P15 Proof of commit is sound.** If an object a store hosts shows revision `ObjectRevision(txid)` (or, for a
revision-less KeyValueUpdate, some key carries it), the transaction committed, because only a committed transaction
writes that revision (`RequirementsApplyer.appliedBy`, `server/store/RequirementsApplyer.scala:50-63`). A store that
finds this on the abort path, with a memoryless acceptor (`mayProve`, `Tx.scala:203-204`), sends the proposer a
`TxResolved(committed, late = false, proof = true)` instead of a vote (`Tx.scala:548-550`, `:585-597`). Since the
refresh the proof is not an outcome: the receiving driver only stops proposing abort and starts a post-proof round
(10.5); stores ignore proofs (`Tx.scala:858`, `Frontend.scala:636`). Not available for traceless changes
(refcount-only, key-delete) nor when later transactions have overwritten the revision.

**P16 Pin rule (client-enforced).** Every client transaction built by `TransactionBuilder` carries a *pin* (a
`DataUpdate` at a non-allocating revision, a `VersionBump`, a KV update with a required revision, a `KeyRevision` on a
key the same update inserts or deletes, or a refcount change), or is marked `replaySafe(reason)`; otherwise it fails
with `UnpinnedTransaction` before anything is sent (`client/internal/transaction/Pins.scala:18-33`,
`TransactionBuilder.scala:140-141`). After a pinned T commits, at most `w - t` stores of the pinned object both hold the
pre-T state and do not know T, so a replay cannot collect `t` commit votes on it. Stores do not check the rule (Q-N4 in
transaction-requirements.md). The rule covers committed transactions only (P-N4). There is no age test left: a
replayed prepare is checked like a new one (`Frontend.scala:671-691`).

**P17 Status cache (assumption).** The host-wide `TransactionStatusCache` remembers each transaction's status for
`txStatusCacheDuration` (7 s) after its last write, bounded to 4 M entries (`server/Host.scala:66-73`,
`server/transaction/TransactionStatusCache.scala:154-208`). A store answers prepares for transactions found there with
`TxResolved`/`TxFinalized` instead of running Paxos (`Store.scala:154-180`). Entries carry `late`; a finalized
committed entry is relayed as late (`late = entry.late || entry.finalized`, `TxFinalized(..., late = committed)`,
`Store.scala:159,171`), which is late-commit evidence (10.5). It is in memory only.

**P18 Client outcome.** The client's future completes with the first `TransactionResolved` or `TransactionFinalized`
it receives (`ClientTransactionDriver.scala:61-63`, `:37-44`); it never checks that two reports agree. Resolution
precedes finalization, so a client can see "committed" before FinalizationActions have run and before any store has
applied the update (the commit is durable only as votes and accepted state in CRLs; see P6, P13).

**P19 First resolution wins.** `Tx.resolved` (`Tx.scala:647-649`), `TransactionDriver.onResolution` (`:473`),
`Tx.receiveFinalized` (`:779`) and the client (`:37-44`) each keep the first outcome and silently ignore a different
later one. Nothing detects a split decision (F11).

---

## 4. Messages

| Message | From -> to | Carries | Defined |
|---|---|---|---|
| `TxPrepare` | client or driver -> every store of every referenced pool | txd, proposal id, this store's object updates (client only), `fromClient`. `from` always names the **proposer store**, even for client-sent prepares. | `common/network/Message.scala:251-265` |
| `TxPrepareResponse` | store -> `m.from` | `Left(Nack(promised))` or `Right(Promise(lastAccepted))`, plus `disposition` and `collisions` | `Message.scala:267-280` |
| `TxAccept` | driver -> primary-pool stores | proposal id, value | `Message.scala:282-288` |
| `TxAcceptResponse` | acceptor -> driver | `Nack` or `Accepted(value)` | `Message.scala:290-301` |
| `TxResolved` | driver -> all stores (+ `notifyOnResolution`); or store -> driver (cache reply, proof of commit) | `committed` | `Message.scala:303-308` |
| `TransactionResolved` / `TransactionFinalized` | driver (or store cache reply) -> originating client | `committed` | `Message.scala:222-236` |
| `TxCommitted` | store -> its last proposer | object ids this store could not apply | `Message.scala:310-316` |
| `TxFinalized` | driver -> all stores; store cache reply -> proposer | `committed` | `Message.scala:318-323` |
| `TxHeartbeat` | driver -> all stores | resets the stores' idle timers | `Message.scala:325-329` |
| `TxUnknownStore` | router -> sender | the addressed store is not on this host | `server/host/StoreRouter.scala:143-146` |
| `TxStatusRequest/Response` | - | handled by `Tx.receiveStatusRequest` (`Tx.scala:858-862`) but **no code sends a request** | dead code |

No network guarantee is assumed beyond "messages may be lost, delayed, duplicated and reordered" (the QUIC plugin
drops messages over budget, `CLAUDE.md`; Paxos and the retransmit loops tolerate the rest).

---

## 5. Normal-case flow

Notation: client `C`; primary pool stores `P0..P4`, leader `L = P2`, `t = 3`; one secondary-pool store `Q0` that also
hosts an object of the transaction.

```
 C                P0   P1   P2(L)  P3   P4        Q0
 |-- TxPrepare(1,L) data_i, fromClient, from=L -->  (to every store, in parallel)           (hop 1)
 |                 each store: load objects, check requirements, take locks (commit vote),
 |                 crl.save{txd, data, vote, promised=(1,L)}; ON DURABLE ->
 |                 |    |    |      |    |         |
 |                 +----+----+----->L<---+----+----+   TxPrepareResponse(Promise(None), vote)  (hop 2)
 |                                  L: first client prepare at the leader created the driver
 |                                     (Store.scala:156-158); tally votes; setLocalProposal(v)
 |                 <-- TxAccept((1,L), v) ---- L ----->  (primary pool stores only)           (hop 3)
 |                 each acceptor: acceptor.receiveAccept; crl.save{..., accepted=(1,L,v)};
 |                 ON DURABLE ->
 |                 +----+----+----->L   TxAcceptResponse(Accepted(v))                          (hop 4)
 |                                  L: Learner has t Accepted at (1,L) -> resolved(v)
 |<--- TransactionResolved(v) ------ L                                                         (hop 5)
 |                 <-- TxResolved(v) -- L ----> all stores incl. Q0 (queued with the above)
 |                 commit: unlock, apply to memory, backend commit, drop CRL data, then
 |                 +----+----+----->L   TxCommitted(errors)
 |                                  L (commit only): run FinalizationActions; when all complete:
 |<--- TransactionFinalized -------- L
 |                 <-- TxFinalized(v) -- L ----> all stores: forget Tx, delete CRL record
```

Step table with code references:

| # | Step | Code |
|---|---|---|
| 1 | Client builds txd: `startTimestamp = HLCTimestamp.now`, primary object, designated leader, `originatingClient`. | `TransactionBuilder.scala:89,113-127` |
| 2 | Client sends `TxPrepare((1,L), from=L, fromClient=true)` with each store's own object slices to **every** store of every referenced pool, then retransmits all of it every `retransmitDelay` until an outcome arrives (never gives up; logs "HUNG" every 3rd retry). | `ClientTransactionDriver.scala:65-84`, `SimpleClientTransactionDriver.scala:37-50` |
| 3 | Store entry: `Store.receiveTransactionMessage` applies CRL backpressure to unseen prepares (`admitPrepare`), then `handleTransactionMessage` consults the status cache (section 10.6) and, **if this store is the primary pool's designated leader**, calls `driveTransaction`. Then `Frontend` creates or finds the `Tx`. | `Store.scala:120-158,193`, `Frontend.scala:426-443,630-666` |
| 4 | New `Tx`: loads hosted objects; `completeLoad` runs `determineDisposition` and `lock()`. | `Tx.scala:345-418,440-478,244-249` |
| 5 | `respondToPrepare`: `acceptor.receivePrepare` (in memory), `crl.save(state)`, **then** send `TxPrepareResponse` to `m.from`. | `Tx.scala:730-749` |
| 6 | Leader driver collects responses for its **current** proposal id only; records each vote; feeds primary-pool promises to the `Proposer`. At promise quorum plus a sufficient tally, sets the local proposal and sets `shouldSendAcceptMessages`. | `TransactionDriver.scala:230-312` |
| 7 | `sendAcceptMessages`: `TxAccept` to primary-pool stores not yet in `acceptedPeers`. | `:438-450` |
| 8 | Acceptor: `Acceptor.receiveAccept`, `crl.save`, **then** `TxAcceptResponse`. | `Tx.scala:751-766`, `Acceptor.scala:24-35` |
| 9 | `Learner.receiveAccepted` counts distinct peers at the highest id; at `t` it fixes `resolvedValue`. | `Learner.scala:14-37`, `TransactionDriver.scala:335-337` |
| 10 | `onResolution`: commit -> create finalizer (runs FinalizationActions) and queue `TxResolved` to all data stores plus `notifyOnResolution`; abort -> `onFinalized(false)` at once. Client gets `TransactionResolved`. | `:471-521` |
| 11 | Stores apply/abort (section 9). | `Tx.scala:615-666` |
| 12 | Finalizer completes -> `onFinalized(true)` -> `TxFinalized` to all stores, `TransactionFinalized` to client, driver shuts down. | `:376-400` |
| 13 | Store `TxFinalized`: Tx forgotten, status cache `finalized=true`, CRL record deleted once backend commits are done. | `Tx.scala:778-800`, `Frontend.scala:616-624,537-551` |

Note the abort ordering in `onResolution(false, true)`: `onFinalized(false)` runs first and sends `TxFinalized` (and
`TransactionFinalized`) *before* the `TxResolved` messages queued afterwards (`TransactionDriver.scala:502-518`); the
receiver ordering does not matter for correctness (a `TxResolved` for a finalized transaction is dropped,
`Frontend.scala:609-615`).

In the commit case the order of `queueTxMessages(TxResolved)` and a concurrent `onFinalized` (from the finalizer's
future, possibly with no actions) is not fixed; `queueTxMessages` replaces unsent messages and logs a warning
(`:172-179`). Losing the `TxResolved` is harmless because `TxFinalized(commit)` implies resolution at the store
(`Tx.scala:790-792`).

---

## 6. Voting

### 6.1 What a store votes on

A `Tx` considers only the requirements of objects it hosts plus non-object requirements (`Tx.scala:59-61`). The vote
is determined in `determineDisposition` (`:440-478`), in this priority order:

1. `muted` (a rebuilt store and a transaction started before its `acceptor-since`): `VoteAbort`, never sent; it proves
   the commit if an object shows the update, and is otherwise forgotten unanswered (`Tx.scala:499-506`, `:807-813`).
   Requirements are not checked and no locks are taken. (The horizon's `tooOld` and `TransactionTooOld` are gone.)
2. Any hosted object whose earlier backend commit failed: `BackendCommitFailed` -> `VoteAbort`.
3. Any hosted object with a transaction commit in flight (`Frontend.commitInFlight`): the vote is **withheld** and the
   Tx registers to be told when the commit persists (`:454-460`); see [transaction-requirements.md](transaction-requirements.md).
4. Otherwise `RequirementsChecker.check`. If every failing requirement is a collision with a lock holder this
   transaction is built on (`awaitableHolders`, `:273-281`), the vote is withheld for up to `Tx.CollisionWait`
   (500 ms, evaluated at the store heartbeat, so up to ~1.5 s). Otherwise the result is `VoteCommit` (no errors) or
   `VoteAbort`.

"Withheld" means *no response is sent* to a prepare, which Paxos allows; drivers retransmit
(`SimpleTransactionDriver.scala:54-85`). A withheld Tx holds **no locks**, so waits cannot form cycles (`Tx.scala:268-270`).

`lock()` is called right after the vote is decided and **before** the vote is durable (`:403`, `:720`). That is safe:
locks are volatile, and a crash before the vote is durable simply discards them along with any chance the vote was
observed (P3, P5).

### 6.2 Missing data

A driver's prepare carries no object data (`TransactionDriver.scala:415,434`: `Nil, Nil`). If it reaches a store before
the client's, the store withholds its vote (`awaitingData`, `Tx.scala:144-147`) until the data arrives, a client's
prepare arrives, the outcome is known, or `Frontend.missingDataWait` (5 s) elapses; then the checker finds
`MissingObjectUpdate` (`RequirementsChecker.scala:101,140`) and the store votes abort. A later client prepare's data is
still adopted so a commit decided by others can be applied (`adoptObjectUpdates`, `Tx.scala:672-676`); adopted data is
**not** re-saved to the CRL until the next save (F14).

### 6.3 Proof of commit on an unknown transaction

If `decideDisposition` would vote abort, the acceptor is memoryless (`mayProve`: no accepted value and no recovered
Paxos state, `Tx.scala:203-204`) and an object already shows the transaction's update, the store `proveCommitted`s
(`Tx.scala:548-550`, `:585-597`): one `TxResolved(proof = true)` to `lastProposer`, no save, no locks, nothing in the
status cache, and no local resolution. The prover stays a live Tx: later prepares get an ordinary memoryless promise with
its abort vote, saved to the CRL (`answerPrepareAsAcceptor`, `:786-788`); a muted prover re-sends the proof instead. On
any real outcome it applies nothing and is forgotten (`applyNothing`, `:941-964`): it holds no commit vote, so it is not
owed the commit. Proof-first ordering is gone: the proof is tried only on the abort path. `awaitingData` is bypassed
when proof exists (`:183`).

### 6.4 Recovered votes

A `Tx` recovered from the CRL keeps its persisted disposition and acceptor state (`Tx.scala:76`, `:443-451`).
A recovered unresolved commit vote, of any transaction (not only traceless ones), looks up its UnackedCommits entry
once loaded and again before accepting any abort (`needsUnackedCheck` `:158-160`, `holdAbort` `:972-982`), and its
store starts no driver for it until the first lookup answers, nor while the store's CRL state loads
(`Store.driveTransaction`, `Store.scala:95-98`). A found entry resolves it committed without forgetting it, and its
driver starts holding a proof (`Tx.scala:997-1010`, `Store.scala:108-110`). See
[transaction-recovery.md](transaction-recovery.md).

---

## 7. The driver: tally, rounds, learner

### 7.1 State

`TransactionDriver` (`server/transaction/TransactionDriver.scala`) holds a `Proposer(self index, w, t)`, a
`Learner(w, t)`, `peerDispositions` (votes seen in the current round), `acceptedPeers`, `knownResolved`,
`resolved`/`resolvedValue`, and the finalizer. A new round clears `peerDispositions` and `acceptedPeers`
(`:402-406`). The proposer's `localProposal` is **sticky**: once set it is never cleared (`Proposer.scala:38`), so later
rounds re-propose the same tally result unless a promise reports an accepted value.

### 7.2 Tally (`TransactionDriver.scala:254-306`)

Run once, when the proposer has a promise quorum and no local proposal. For each object pointer `ptr` (iteration order
of a Set), with `w`, `t` of `ptr`'s pool, `n` = stores of that pool that answered this round, `c` = those that voted
`VoteCommit`, `a = n - c` (**every non-`VoteCommit` answer counts as an abort vote, including `Undetermined`**,
`:272-275`):

| Condition | Effect |
|---|---|
| `n < t` | cannot decide: wait (`cantAttempt`, `:281-284`) |
| `n != w` and `c < t` and `w - a >= t` | commit still possible with more answers: wait (`:286-289`) |
| `w - a < t` | object cannot reach `t` commit votes: `canCommitObject = false` (`:291-292`) |

Then `localProposal = canCommit(all objects)` (`:301-303`).

Properties (verified by case analysis):

* If the result is commit then `c >= t` for every object: when `n == w`, `w - a = c`; when `n < w` the second row would
  have forced a wait unless `c >= t` or `w - a < t`. So **commit implies `t` commit votes per object** (P6).
* Once `c >= t` or `a > w - t` is true it stays true (votes are immutable and only accumulate). So different drivers
  using the same vote set agree; they can differ only through non-votes counted as aborts (F7) or through amnesia
  (F2, F6).
* The decision is gated on *every* object's pool having `t` answers, even when abort is already certain for another
  object (F8).

### 7.3 Rounds, Nacks, backoff

* On the first Nack for the current id the driver advances its round immediately (`onNack`, `:452-458`); it does not
  wait for a quorum of Nacks (`Proposer.receiveNack` exists but is unused). Nacks from secondary-pool stores count too
  (the `Left` branch has no `isValidAcceptor` check, `:243-245`), though those stores never accept anything.
* `SimpleTransactionDriver.nextRound` doubles a backoff capped at `maxDelay` and sleeps a random time below it
  (`SimpleTransactionDriver.scala:87-99`). The driver re-broadcasts the current prepare/accept every `maxDelay`
  (`:54-85`), interleaving prepares with accepts every third send so stores that forgot the Tx restart it.
* After a round change `shouldSendAcceptMessages` is not set again (the tally block is skipped because
  `haveLocalProposal` is already true, `:254`); the Accept for the new round goes out at the next periodic send. Latency
  only (F12).
* `TransactionDriver.receiveTxPrepare` (`:226-228`), which would feed other proposers' ids into `highestProposalId`,
  has no caller.

### 7.4 Learner and resolution

`Learner.receiveAccepted` counts distinct peers for the highest proposal id seen (`Learner.scala:14-37`); the driver
only feeds it messages whose id equals its current id and whose sender is a valid acceptor (`TransactionDriver.scala:314-337`).
Resolution can also arrive without the learner: `receiveTxResolved` calls `onResolution(msg.committed,
sendResolutionMessage = false)` (`:345-353`) and trusts the sender (cache reply, proof of commit, another driver).

### 7.5 Who becomes a driver

* The designated leader, on any `TxPrepare` it receives for a transaction not finalized in the cache
  (`Store.scala:156-158`), including client retransmits. Guarded by pool id.
* Any store whose `Tx` is idle for more than `txHeartbeatTimeout` (`Store.scala:73-77`) or `failedDriverDuration`
  (`SimpleDriverRecoveryMixin.scala:19-29`). **Not guarded by pool id** (F4). A driver's own store is also one of its
  recipients; it addresses its own `Tx` like any other.
* A client's retransmitted prepare does not count as store activity (`Tx.scala:692-693`), so a dead leader cannot be kept
  "alive" by the client.
* A driver is created once per transaction per store (`Store.driveTransaction`, `:84-96`) and removed when its own
  store receives `TxFinalized` (`:181-184`).

---

## 8. Persistence ordering rules

All CRL writes are `crl.save(txid, TransactionRecoveryState, handler)` (`server/crl/CrashRecoveryLog.scala:141`). A saved
record contains: `storeId`, serialized txd, object updates (unless dropped), disposition, status, and the acceptor's
`(promised, accepted)` (`server/crl/TransactionRecoveryState.scala:9-15`). Saves overwrite the whole record
(`server/crl/simple/SimpleCRL.scala:266-289`). Each record always carries the then-current acceptor state, so a later save never loses an
earlier promise.

| Message sent | Must be durable first | Code |
|---|---|---|
| `TxPrepareResponse` (Promise or Nack) | txd, data, disposition, acceptor state | `Tx.scala:739-745`; delayed variant `:420-431` |
| `TxAcceptResponse` | acceptor state with `accepted=(id,v)` (disposition as it is then, possibly `Undetermined`) | `:759-765` |
| `TxResolved`, `TransactionResolved` (from the driver) | `t` `Accepted` (each durable at its acceptor) | `TransactionDriver.scala:337,471-521` |
| `TxCommitted` | all backend commits of this store for the transaction completed; CRL object data drop *enqueued* (not awaited) | `Tx.scala:604-613` |
| `TxFinalized(commit)` | all FinalizationActions complete | `TransactionDriver.scala:496-499` |

Not required durable before sending: `TxResolved(true)` from `proveCommitted` (the evidence is the already-durable
backend state, `Tx.scala:516-525`); status-cache replies; `TxCommitted` re-sends.

State deletions:

| Event | CRL effect |
|---|---|
| `TxResolved(false)` / `TxFinalized(false)` at a `Tx` | record deleted immediately (`Tx.scala:627`, `Frontend.scala:481-483`); a later accept or prepare handled by the lingering in-memory `Tx` re-creates it and `TxFinalized` deletes it again (`Tx.scala:794-799`). |
| commit applied and all backend commits succeeded | object data dropped, record kept (`Tx.scala:604-613`) |
| `TxFinalized(true)` and all backend commits succeeded | record deleted (`Frontend.scala:537-551`) |
| a backend commit failed | record (with data) kept until the store reloads (`Frontend.scala:703-709`) |

Ordering inside one store relies on the store executor being single-threaded and on the CRL completing handlers in
log order. The `CrashRecoveryLog.save` contract is specified in [crash-recovery-log.md](crash-recovery-log.md); this
spec only requires: *handler runs after the record, including everything saved before it, is durable*. Note a record
is saved on **every** prepare response (retransmits included), which is write amplification, not a safety issue.

What is **not** persisted: the commit/abort *outcome* at a store (status is saved only incidentally by a later save,
`Tx.scala:425-426,740-741,759-760`); the proposer's round number or local proposal; driver state of any kind; the
status cache. After a crash a store knows only its own vote and acceptor state; the outcome must be re-learned
(driver, caches, proof and late commits, UnackedCommits).

---

## 9. Commit application and finalization

### 9.1 Resolution at a store (`Tx.scala:647-666`, `:615-632`)

* `resolved(c)` sets `oresolution` once, updates the status cache, and if all objects are loaded calls
  `resolvedAndAllObjectsLoaded(c)`; otherwise the resolution is applied when loading finishes (`:405-408`).
* Abort: `frontend.deleteTransactionState`, `unlock()`, `releaseObjects()`.
* Commit: first the two holds:
  * **commit overtook the data** (`commitAwaitsData`, `:165`): no data and no client prepare seen and not finalized ->
    hold (`commitHeld`) with no `TxCommitted`, so the driver does not yet count this store as having missed the update.
    Ends with the data, a client prepare, or `TxFinalized`; after `TxFinalized` the store applies without the data,
    reports the object as uncommittable and repair restores it.
  * **earlier commit of an object in flight** (`holdForTurn`, `:178-188`): queued per object; see
    [transaction-requirements.md](transaction-requirements.md).
  Otherwise `doCommit` (`:527-602`): unlock, `RequirementsApplyer.apply` re-checking requirements with `applying=true`
  (objects that fail are skipped and reported in `TxCommitted.objectCommitErrors`; objects already showing the
  transaction are not re-applied), issue one backend commit per changed object, and send `TxCommitted` when all
  complete (`:604-613`).

### 9.2 Driver after commit resolution

`onResolution(true)` creates the finalizer immediately, passes it the commit voters (`countedCommitVoters`,
`:464-469`) and `commitErrors` from `TxCommitted` messages (`:355-361`); the finalizer runs the txd's
FinalizationActions (`server/RegisteredTransactionFinalizerFactory.scala:14-33`); at-least-once, idempotent. Missed
updates are recorded by `MissedUpdateFinalizationAction` ([transaction-recovery.md](transaction-recovery.md)).

### 9.3 Heartbeats and lost messages

Driver `heartbeat()` sends `TxHeartbeat` (not on its first tick) or, once finalized, repeats `TxFinalized`
(`TransactionDriver.scala:184-191`). A store that sees nothing for `txHeartbeatTimeout` drives the transaction itself
(`Store.scala:73-77`); that is the recovery path for a lost `TxFinalized` and works only because the status cache
(7 s) outlasts the detection (5 s + one heartbeat) (`Host.scala:57-67`).

---

## 10. Edge cases

### 10.1 Duplicate prepares

Re-processed idempotently: the acceptor re-promises (`Acceptor.scala:15-18`), the vote is unchanged (P4), the response is
saved and sent again. A committed `Tx` also re-sends `TxCommitted` to the sender (`Tx.scala:695-699`). `lastProposer`
is overwritten by every prepare, including client retransmits (F10).

### 10.2 Late prepares

* Tx known: handled above.
* Tx unknown, status cache says finalized: answered `TxFinalized` (+ `TransactionFinalized` to the client) at the Store
  and ignored by the Frontend (`Store.scala:144-151`, `Frontend.scala:639-647`).
* Tx unknown, cache says Committed/Aborted but not finalized: the Store sends `TxResolved` to the sender and the Frontend
  creates a `Tx` with `outcomeDecided()`; it takes no locks, still votes, and learns the outcome only from `TxFinalized`
  because the driver treats the cache's reply as "this store knows" (`knownResolved`) and does not re-send
  `TxResolved` to it (`TransactionDriver.scala:346-347`, `SimpleTransactionDriver.scala:62`; `Frontend.scala:649-665`).
* Tx unknown, no cache entry: a fresh `Tx`, checked normally whatever its age (the horizon is gone). A replay of a
  committed pinned transaction fails its pin; a store showing its update proves it (6.3), and the driver resolves late
  (10.5). Accepted cost: a replay whose other requirements pass holds commit-vote locks until a driver resolves it.
* Dropped under CRL backpressure: `Store.scala:123-124`; the client retransmits, and the rest of the quorum can decide
  without this store (repair catches up).

### 10.3 Duplicate and late accepts

`TxAccept` for an unknown `Tx` is ignored (`Frontend.scala:608`), so a forgotten store never acts as an acceptor with
fresh state through an Accept alone; the driver therefore interleaves prepares (`SimpleTransactionDriver.scala:67-73`).
A duplicate Accept is re-accepted and re-answered; the driver's `acceptedPeers` set and the `Learner` bitset make
this idempotent. An Accept may reach a store that has not voted yet; it accepts (Paxos allows this) and saves its
then-current, possibly `Undetermined`, disposition (`Tx.scala:759-760`).

### 10.4 Prepares that overtake data / commits that overtake data

See 6.2 and 9.1. The relevant constants: `Frontend.MissingDataWait = 5 s` (`Frontend.scala:27`), `Tx.LateClientPrepareAfter =
500 ms` (log only, `Tx.scala:29`). A client prepare with data arriving after `TxFinalized` is ignored with a warning
(`Frontend.scala:644-648`); the store is repaired later.

### 10.5 Late commits (replaces the horizon)

The horizon (`Host.txHorizon`, `TransactionTooOld`) was removed. In its place (design 1b/1c and its amendments):

* **Proof holder.** A driver receiving a proof sets `holdsProof`, and the first proof starts a new round
  (`TransactionDriver.scala:361-386`): promises to earlier rounds may predate another driver's choice of commit
  (Ruling 6). It never sets an abort local proposal (`:318-319`). With a prepare quorum whose promises carry an accepted
  value it sends accepts for that value without the tally (`:264-268`, Ruling 7), and resolves normally.
* **Late condition 1.** A proof plus a prepare quorum of valid acceptors in the current (post-proof) round, none
  carrying an accepted value (`Proposer.prepareQuorumWithoutAcceptedValue`, `common/paxos/Proposer.scala:24,54-59,83-88`):
  `resolveLate` (`TransactionDriver.scala:262-263`, `:539-552`). Argument: the proof shows commit was chosen by some
  quorum Q; Q intersects the responders; an acceptor forgets a committed T only on `TxFinalized` or a late outcome;
  rebuilt acceptors are muted. So finalization completed.
* **Late condition 2.** A `TxResolved` or `TxFinalized` with `late = true` (`:364-365`, `:399-400`), including the
  finalized relay of a status cache (`Store.scala:158-171`).
* **Late resolution** runs no finalization actions (it cancels any it started) and finalizes at once; every
  `TxResolved`/`TxFinalized` it sends carries `late = true` (`:446-479`, `:539-552`).
* **A Tx receiving a late commit** (`Tx.lateCommit`, `Tx.scala:920-932`): already resolved committed, it finalizes;
  holding its own undecided commit vote, it looks up its UnackedCommits entry and applies only if found
  (`unackedCommitFound`/`unackedCommitAbsent`, `:997-1041`); otherwise it applies nothing (`applyNothing`, `:941-964`).
* **Found entry as a proof** (Ruling 15): a recovered commit vote that found its entry gives its own driver a proof from
  the start (`Store.scala:108-110`).

Residuals: Ruling 17 (a late-acked store with an entry re-applies a replay's blind write; transaction-recovery R-N1
shows it is not limited to one store), the design's recovering-acceptor residual (P-N3), and a proof delivered to a
driver that never acts on it (P-N1).

### 10.6 Status cache

* Write sites: `Tx` construction writes `Unresolved` (`Tx.scala:114`); `resolved` writes the status
  (`:662`); `finalized` writes `finalized=true` (`:800`); a `TxResolved`/`TxFinalized` for an unknown Tx writes the
  status (`Frontend.scala:609-624`).
* Read sites: `Store.scala:131-158` (reply with `TxResolved`/`TxFinalized`), `Frontend.scala:432,639,651` (admission,
  ignore, `known`), `StoreRouter` completion queries.
* One cache per **host**, shared by all its stores and keyed by transaction id only. Entries live 7 s after the *last
  write*. The early-eviction warning was removed with the horizon: an evicted entry only loses late-commit evidence
  (`server/transaction/TransactionStatusCache.scala:8-24`).
* An `Unresolved` entry counts as "no outcome" when a fresh Tx is created (`Frontend.scala:673-675`).
* Since the refresh the no-Tx `TxResolved` path never overwrites a finalized entry and ignores proofs
  (`Frontend.scala:636-637`); `Tx.resolved` still writes `finalized = false` unconditionally (`Tx.scala:737-738`), see F9.

### 10.7 Proof of commit interplay

`Tx.awaitingData` is bypassed when objects are loaded and show the update (`Tx.scala:147`). `doCommit` skips objects that
already show the update and logs them as already applied (`:626-631` at `0d08e3c6`). A prover answers later prepares as
an ordinary memoryless acceptor with its abort vote; only a muted prover repeats the proof (`Tx.scala:786-788`).

### 10.8 Unknown or moved stores

`TxUnknownStore` makes the driver drop its endpoint cache for that store and not worry (`TransactionDriver.scala:216-224`);
the transaction proceeds without it if a quorum remains.

### 10.9 Nack / dueling proposers

Resolved by backoff (7.3). Stale responses are filtered by proposal id (`TransactionDriver.scala:235,319`).

---

## 11. Failure scenarios

Conventions: "safe" = no split decision, no loss of a decided commit, no applied update for an aborted transaction.
"Live" = the transaction eventually resolves without operator action given a healthy quorum.

### 11.1 Message-level faults

| Fault | Outcome | Why |
|---|---|---|
| Client `TxPrepare` lost to some stores | Safe, live | Client retransmits forever; a store lacking data withholds its vote up to 5 s then votes abort; the tally needs `t` commit votes per object, else abort. |
| `TxPrepare` lost to the leader | Live after delay | No driver is created at the leader; other stores' responses are cached 10 s for a later driver (`Store.scala:80-82,160-167`); the client retransmits; after 5 heartbeats of idleness a recovery driver starts. |
| `TxPrepareResponse` lost | Safe, live | Driver retransmits the prepare (`SimpleTransactionDriver.scala:54-85`); the store re-answers with the same persisted vote. |
| `TxAccept` or `TxAcceptResponse` lost | Safe, live | Retransmitted; the learner needs `t` distinct acceptors. |
| `TxResolved` lost | Safe, live | Driver re-sends `TxResolved` to stores not in `knownResolved` until finalized; otherwise `TxFinalized` carries the outcome. |
| `TxFinalized` lost | Safe, live if the status cache still has the entry when the idle store's driver asks (5 s idle < 7 s cache); otherwise see F2/F6 | `Store.scala:73-77`, `Host.scala:57-67`. Meanwhile the store holds its locks. |
| `TxCommitted` lost | Safe | Finalizer's missed-update logic marks the store for repair, which reconciles exactly. |
| `TransactionResolved`/`Finalized` lost to client | Safe if the client's retransmit arrives within the cache lifetime (answered from the cache); otherwise a false abort is possible (F2) | |
| Duplicates of any message | Safe | 10.1-10.3. |
| Reordering | Safe | Proposal ids filter stale responses; `TxFinalized` before `TxResolved` is fine (5); a commit decision arriving before the prepare/data is held (9.1). A `TxResolved` arriving before any prepare is only remembered in the cache (`Frontend.scala:610-615`), so the later-created `Tx` learns it only at `TxFinalized`. |
| Message to a store on another host after migration | Safe | `TxUnknownStore`, peer queries (`CLAUDE.md`). |

### 11.2 Slow or partitioned clients and stores

| Case | Outcome |
|---|---|
| Slow client: data reaches a store after another store's driver prepare | Vote withheld up to 5 s; then abort vote. Late data still adopted if the others commit (6.2). |
| Slow client: data reaches a store after the commit decision | Commit held until data/`TxFinalized` (9.1). |
| Client partitioned for longer than the status cache lifetime and then retransmits | Stores no longer know the Tx; fresh Txs check requirements and the pin fails. With a proof the driver resolves late and the client is told committed; without one (refcount pin, overwritten pin, lost proof) the client can still be told **aborted** (F2, narrowed). |
| Client frozen, clock ahead, retransmit after > 7 s | No longer special: clocks play no part (F3 fixed). |
| Slow store (behind) | It votes late or never; the quorum decides without it. If it missed the commit it is marked for repair by the finalizer. |
| Leader partitioned from some stores | Driver cannot reach a quorum -> waits; recovery drivers on other stores start after 5 heartbeats; dueling resolved by Paxos. |
| Minority of acceptors partitioned | Proceeds. A majority-or-more partition: no progress (liveness), no unsafe decision. |
| Secondary pool below `t` reachable | The tally cannot decide even an abort (F8); commit-vote locks stay held; the client retransmits forever. |

### 11.3 Crash at each step

"Store crash" means the process or host dies and the store restarts, recovering from its device CRL.

| Crash point | State after restart | Outcome |
|---|---|---|
| Client, before any prepare reaches a store | Nothing exists | Nothing happened. |
| Client, after some prepares | Stores that got data hold Tx; after 5 heartbeats of idleness a recovery driver (round >= 2, data-less) starts. | Commits only if stores that got data supply `t` commit votes for every object; else abort. Either is safe. Client never learns (that is acceptable). |
| Store, before the vote's save completes | No record, or an older record. Nothing was sent. | The store is as if it never saw the prepare; client/driver retransmit; a fresh vote is computed. |
| Store, vote saved but response not sent | Record with disposition and acceptor state; locks re-taken for a commit vote | Retransmitted prepares are answered with the persisted vote (P4/P5). |
| Store, after Accepted saved | Acceptor state recovered (`Tx.scala:74`) | Paxos state intact; P8 holds. |
| Store, after learning commit, before applying | Recovered as Unresolved with its vote and data; commit vote re-locks | Must re-learn: a live driver, status cache, a peer's proof, or the UnackedCommits lookup (`needsUnackedCheck`), which now runs for every recovered commit vote and gates its driver. If every other store finalized and forgot: an entry makes it apply; no entry means it acknowledged (or did not count), and a late commit applies nothing. Without a proof or entry, abort votes may decide abort (F2 residual). |
| Store, after in-memory apply, before backend commit durable | Object data still in the CRL (P13); recovered like the previous row | Same. |
| Store, after backend commit, before CRL data drop | Recovered Unresolved; `appliedBy` shows the update | `proveCommitted` on the next prepare or `doCommit` skips as already applied (`Tx.scala:556-561`). |
| Leader/driver, before Accept sent | Driver state lost | Recovery driver after idle timeout; values for round 1 not reused (round >= 2). Leader's own restart restarts at `(1,L)` (F5). |
| Leader, after quorum accepted, before `TxResolved` sent | Accepted state at `t` acceptors | Recovery driver's prepare quorum intersects the accepted set, so it re-proposes the same value (P8). |
| Leader, after resolution, during FinalizationActions | Stores hold the committed Tx (not finalized), their CRL records kept | Recovery driver: stores still within cache reply `TxResolved`, or answer Promise with their retained accepted commit (P9); it re-runs the finalizer (at-least-once). |
| Driver, after `TxFinalized` partly sent | Some stores finalized | Idle stores recover via their own driver and the cache (11.1). |
| Backend commit failure at a store | Record kept, store votes abort on that object (`BackendCommitFailed`) until reload | `Frontend.scala:703-709`. |
| CRL thread death | Store stalls (no vote can be sent) | Liveness only. |

### 11.4 Concurrent drivers

* Leader plus one or more recovery drivers: all run Paxos with distinct ids (P2 holds for primary stores). The client's
  retransmit at `(1,L)` is Nacked by acceptors that promised `(2,R)`; the Nack goes to the leader's driver, which bumps its
  round (`onNack`). Safe by P8; resolution is announced by whichever driver learns first; others learn via
  `TxResolved`/`TxFinalized`.
* Both resolve and both run FinalizationActions: duplicates are permitted (at-least-once, idempotent). Both send
  `TxFinalized`; stores handle it once (`Tx.scala:779`).
* Two drivers with the **same id** (secondary-pool store sharing the index; restarted leader): not safe in principle (F4,
  F5).
* A recovery driver racing a `TxResolved` from the cache: the first learned value wins in `onResolution` (P19).

---

## 12. Suspected flaws and open questions

Format: ID, confidence that the described behaviour exists and is a real problem, scenario, code. "Confidence" is
about the finding, not the likelihood of occurrence.

### Re-assessment for replay pins (2026-11-09, `0d08e3c6`)

| Id | Status | Reason | Evidence |
|---|---|---|---|
| F1 | unchanged | Only drivers send `TxAccept`; no round rule | `TransactionDriver.scala:481-493` |
| F2 | **narrowed** | A driver holding a proof never proposes abort and resolves late; a driver with no proof (refcount or overwritten pin, proof lost or sent to another driver, P-N1) still decides abort | `TransactionDriver.scala:262-268,318-319,361-386`; `Tx.scala:594` |
| F3 | **fixed** | The horizon is gone; replay is refused by the pin rule and the late-commit rule, not by clocks | `Frontend.scala:671-691` (no age test); `Pins.scala:18-33`; `TransactionBuilder.scala:140-141` |
| F4 | unchanged | Proposer peer is still the pool index | `TransactionDriver.scala:88` |
| F5 | unchanged | A new driver still starts at `(1,L)` | `Proposer.scala:11`; `SimpleTransactionDriver.scala` |
| F6 | unchanged (reach widened) | Abort still deletes the CRL record at resolution; with no horizon a forgotten abort is re-decidable by any later prepare (P-N4) | `Tx.scala:699`; `Frontend.scala:501-503` |
| F7 | unchanged | Every non-`VoteCommit` counts as abort | `TransactionDriver.scala:287-292` |
| F8 | unchanged | Tally still `break()`s on the first short pool (a proof holder adopting an accepted value skips it) | `TransactionDriver.scala:296-304` |
| F9 | **narrowed** | The no-Tx `TxResolved` path no longer overwrites a finalized entry; `Tx.resolved` and the `Tx` constructor still write unconditionally (X3) | `Frontend.scala:636-637`; `Tx.scala:144,737-738` |
| F10 | unchanged | `lastProposer` still follows client prepares; the proof is sent there too (P-N1) | `Tx.scala:756,594` |
| F11 | unchanged | Conflicting outcomes are still ignored silently; `resolveLate` logs an error only for late-after-abort | `Tx.scala:722`; `TransactionDriver.scala:541-542,569` |
| F12 | unchanged | Rounds >= 2 still wait for the timer (the post-proof round sends prepares at once) | `TransactionDriver.scala:318-321,384-385` |
| F13 | unchanged (now intended) | A prover's abort vote is saved on later prepares by design (Ruling 10); a recovered prover has Paxos state, so it no longer proves | `Tx.scala:203-204,786-788` |
| F14 | unchanged | Adopted data still not re-saved | `Tx.scala:748-752` |
| F15 | unchanged | `receiveTxPrepare` and `TxStatusRequest` still have no caller | `TransactionDriver.scala:234-236`; `Tx.scala:1047-1051` |

### F1. Documented 1-RTT protocol is not what the code does. Confidence: high (informational, wrong docs)

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only.** Only TransactionDriver.sendAcceptMessages constructs TxAccept (TransactionDriver.scala:438-447; grep finds no other constructor). Stores answer a prepare with Promise+vote addressed to the prepare's sender (Tx.respondToPrepare, Tx.scala:730-749, `TxPrepareResponse(m.from,...)`) after a CRL save; the driver sets its local proposal only after tallying (TransactionDriver.scala:247-300) and no round check exists in Acceptor (Acceptor.scala:24-35). Error-free path: client TxPrepare -> store (CRL write #1: vote+promise, Tx.scala:743-746) -> leader -> TxAccept -> acceptors (CRL write #2, Tx.scala:751-766) -> TxAcceptResponse -> leader -> TransactionResolved -> client = 5 one-way hops (about 2.5 RTT) and 2 serial durable writes before the client learns; stores learn at a 6th hop (TxResolved). Aborts are decided in round 1; there is no commit-only round 1 and no self-generated Accept. Todo.txt:11-14 and Architecture.md describe a protocol the code does not implement (no partial 1RTT).
> *Fix sketch:* Fix the docs (Todo.txt, Architecture.md) now; if 1RTT is wanted, have stores self-accept commit in round 1 and restrict round >=2 to abort-or-recover, then re-derive P2/P6.


`Architecture.md:139-155` and `Todo.txt:11-14` describe self-generated round-1 Accepts, a commit-only round 1 and
abort-requires-round-2. The code routes all promises to one leader, which sends the only Accepts (`Tx.scala:736`,
`TransactionDriver.scala:444`); there is no round restriction (`Acceptor.scala:24-35`). Hence: 5 one-way hops and two
serial durable writes before the client learns; aborts are decided in round 1; recovery can commit. Anyone reasoning
from the documented rule (for instance, "any accept in round >= 2 is an abort") will reach wrong conclusions.
Recommendation: fix the docs or implement the fast path; if implemented, P2/P6 must be re-derived (acceptors would accept
commit without a leader tally).

### F2. A stale prepare after the cache expires can produce a false abort of a committed transaction. Confidence: medium-high

> **Refresh (replay pins): NARROWED.** The stale prepare is now checked normally (no `TooOld`). A store showing T's
> update proves it; the driver that receives the proof never proposes abort (`TransactionDriver.scala:318-319`), runs a
> post-proof round, and resolves *late* on a memoryless quorum, telling the client committed and applying nothing
> (`:539-552`). The false abort remains wherever no driver holds a proof: the pin is a refcount change or was
> overwritten, the proof was sent once to a `lastProposer` with no driver (P-N1), or a second driver without the proof
> tallies first. Consequence (c) is also narrowed: a live commit voter that learns a *late* commit looks up its entry and
> applies it (`Tx.scala:926-930`); one that learns an abort still drops it (transaction-recovery F3).

> **Verification 2026-11-09: CONFIRMED (mechanism); outcome damage narrower than claimed. Severity: split-decision (two outcomes for one txid; no applied store is rolled back).** Acceptor state is NOT retained past finalization: a committed Tx keeps its Paxos record until TxFinalized (Tx.scala:790-800 -> Frontend.transactionFinalized, Frontend.scala:537-551), an aborted one is deleted at resolution (Tx.scala:627 -> deleteTransactionState, Frontend.scala:481). After TxFinalized every data store has dropped Tx+acceptor, the leader's driver is removed (Store.scala:181-183) and the host cache forgets after 7 s, so a stale client retransmit (client retransmits until it hears: SimpleClientTransactionDriver.scala:30-50) at t > ~10 s starts a NEW consensus instance: Store.handleTransactionMessage finds no cache entry and, at the leader, driveTransaction (Store.scala:128-158); every store builds a fresh Tx with tooOld=true (Frontend.scala:651-658) which proves commit only if an object still shows the update (appliedBy: revision of DataUpdate/VersionBump/KV-with-revision, or a key carrying the txid) and otherwise votes VoteAbort(TooOld) (Tx.scala:444-449, 489-493). For a traceless or since-overwritten change all votes are abort, the tally aborts (TransactionDriver.scala:291-298), empty acceptors accept, and the driver sends TxResolved(false)/TxFinalized(false) plus TransactionResolved/Finalized(false) to the client (:471-521). Consequences: (a) client told ABORT for a committed tx (real, client-visible); (b) cache says Aborted for 7 s; (c) a live store still holding the commit vote drops it (Tx.receiveResolved Tx.scala:768-776 -> resolved(false)). NOT possible: rolling back stores that already applied (a fresh Tx resolves false and only deleteTransactionState runs; nothing undoes an applied commit), and stores whose Tx is already resolved(true) ignore the second outcome (Tx.resolved guards oresolution.isEmpty, Tx.scala:645). (c) is repaired because the original finalizer marks every store that did not send TxCommitted within errorTimeout (MissedUpdateFinalizationAction.scala:60-100). So it is a split of DECISIONS, not of store state, and it needs a client partitioned/paused > ~horizon (6 s) whose reply was lost.
> *Fix sketch:* Do not let a TooOld-only tally resolve a transaction: report 'unknown' to the client (no TxResolved/TransactionResolved) and stop; or persist a small 'finalized(txid, outcome)' tombstone for longer than any client retransmit horizon.


Scenario: transaction T commits and finalizes at t0 (stores forget T; caches hold it 7 s). The reply to the client is
lost or the client is partitioned. At t0+20 s the client's retransmit reaches the designated leader L
(`ClientTransactionDriver.scala:65-84`). `Store.handleTransactionMessage` finds no cache entry and starts a driver
(`Store.scala:156-158`). Every store creates a fresh `Tx`, finds `tooOld` (age > 6 s), and votes `VoteAbort(TooOld)`
unless an object still shows T's revision (`Tx.scala:444-449,491-493`). If T's change is traceless, or the object has
been overwritten by later transactions, there is no proof; all stores answer abort; L's driver tallies `w - a < t`
(`TransactionDriver.scala:291`), proposes abort, all acceptors have empty state and accept, and L sends
`TransactionResolved(false)` and `TxResolved/TxFinalized(false)` (`:471-521`). Effects: (a) the client is told T
aborted although it committed (retry of a non-idempotent operation may double-apply; contradicts the client-visible
atomicity guarantee); (b) the status cache on every host now says `Aborted` for 7 s; (c) a store that is still holding T
as a live unresolved commit vote receives `TxFinalized(false)`/`TxResolved(false)` and drops the committed update
(`Tx.scala:771-773,779-788`). Repair is expected to restore (c) through the finalizer's ErrorTree/UnackedCommits
entries, but only if the finalizer actually recorded that store, which depends on the original driver having completed.
The design note acknowledges the cost for *legitimate late* prepares (`traceless-commits-design.md` section 6) but not
this case where the transaction already committed. Open question: should a `TooOld` abort be allowed to *resolve* a
transaction at all, rather than merely vote? An alternative is that a driver that sees only `TooOld` votes reports
"unknown" to the client.

### F3. The horizon trusts the client's clock; a client clock ahead of the hosts disables replay protection. Confidence: medium

> **Refresh (replay pins): FIXED.** The horizon and `TransactionTooOld` are deleted (`Frontend.scala:671-691` has no age
> test; grep finds no `txHorizon`/`tooOld`). A replayed blind write cannot re-commit through fresh votes, because its
> transaction must carry a pin (`TransactionBuilder.scala:140-141`) that fails at more than `w - t` stores of the pinned
> object. Residual clock dependence moved to the rebuilt store's mute (`acceptorSince`, `Frontend.scala:676`; P-N2).

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: resurrection (replay of blind writes).** tooOld = known.isEmpty && HLCTimestamp.now - txd.startTimestamp > horizon (Frontend.scala:655); startTimestamp is client-assigned and nothing on the server calls HLCTimestamp.update from received prepares (grep in server/ finds no such call), so a host's HLC only follows its own wall clock. A client whose clock/HLC is ahead by D makes a replay of age a look like a-D; ages in (7 s, 6 s + D) are treated as young. A replayed prepare then creates a fresh Tx that rechecks requirements; blind KV Insert/Delete have no state-dependent requirement (RequirementsChecker.scala:123-158), so t stores vote commit and the old write is re-applied. Client HLC ratchets forward from every object timestamp it reads (BaseReadDriver.scala:193-194), so ahead-skew needs no misconfigured NTP, only a faster clock elsewhere in the cluster.
> *Fix sketch:* Bound client timestamps at the receiving host (reject/clamp startTimestamp > hostNow + small epsilon), and also record the host-local receipt time of the first prepare it ever saw for a txid.


`tooOld` compares host `HLCTimestamp.now` with `txd.startTimestamp`, which the client set (`TransactionBuilder.scala:89`;
`Frontend.scala:655`). The cache covers only 7 s. If the client's clock (or its HLC, which ratchets forward from any
timestamp it reads, `client/internal/read/BaseReadDriver.scala:193-194`, and never backwards) is ahead of the hosts by
D, the apparent age is `real age - D`, so real ages in (7 s, 6 s + D) are treated as young and unprotected whenever
`D` exceeds ~1 s plus the finalization latency. A replayed prepare then creates a fresh `Tx`, rechecks requirements
against current state and votes commit. Transactions whose requirements are state-independent (a blind KeyValue insert
or a Delete without a required revision, `KVWrites`) pass, `t` stores vote commit, and the driver commits again:
an old value overwrites a newer one, or a deleted key reappears or a re-inserted key is deleted again. Stale prepares
come from client retransmit loops resumed after a pause, or delayed packets. The horizon doc only discusses a client
clock *behind*. Open question: measure age against the store's own receipt time of the first prepare it saw (not
possible for unknown transactions), or bound client-supplied timestamps against the receiving host's clock and reject
or clamp future timestamps.

### F4. Secondary-pool stores can become drivers and share the proposal-id space of primary stores. Confidence: structural 85%, exploitability low

> **Verification 2026-11-09: PLAUSIBLE. Severity: split-decision (only if proposers can compute different values).** Structure confirmed: designatedLeader compares only the pool index (TransactionDriver.scala:114) while Store.handleTransactionMessage checks the pool (Store.scala:156), Store.heartbeat starts a driver for ANY idle Tx (Store.scala:73-77), Proposer peerId = storeId.poolIndex (TransactionDriver.scala:88), and a secondary-pool store with the leader's index skips nextRound (SimpleTransactionDriver.scala:48) so it starts at the same (1,L). Acceptor accepts an equal id with any value (Acceptor.scala:31). But the tally decides only when conclusive (commit when every object has >= writeThreshold commit votes, abort when some object cannot reach it; TransactionDriver.scala:259-300) and votes are immutable, so same-id proposers compute the same value; a real split needs the non-vote inputs (F7 Undetermined, F2/F6).
> *Fix sketch:* Restrict driving to primary-pool stores, or make the proposer id the primary-pool index only.


`Store.heartbeat` and `SimpleDriverRecoveryMixin` call `driveTransaction` for *any* idle `Tx` on any store
(`Store.scala:73-77`, `SimpleDriverRecoveryMixin.scala:22-26`); only the designated-leader path checks the pool
(`Store.scala:156`). `TransactionDriver.designatedLeader` compares only the index (`:114`) and the proposer id byte is
`storeId.poolIndex` (`:88`), so a secondary-pool store with index equal to the leader's index starts at the **same**
`(1, L)` as the real leader and the client's prepare, and recovery drivers on `P_i` and `Q_i` both use `(2, i)`. Two
different proposers with one id violate P2; `Acceptor.receiveAccept` accepts an equal id with a different value
(`Acceptor.scala:31`). A split needs the two drivers to tally different values; the tally is monotone in immutable votes
(7.2) so differing values need the non-vote paths (F7, F2/F6). Fix: restrict drivers to primary-pool stores, or
include the pool in the id (for instance the leader's primary index only).

### F5. A restarted leader reuses `(1,L)`; old in-flight Accepts can flip acceptors. Confidence: low-medium

> **Verification 2026-11-09: PLAUSIBLE. Severity: split-decision (rare).** Confirmed that a new driver (restart, or any new driver after the leader's was removed) starts at (1,L) (Proposer.scala:11-13, SimpleTransactionDriver.scala:48) and that Acceptor accepts equal ids (Acceptor.scala:31). A delayed old Accept after a decision still requires the transaction to be unfinalized at a quorum; practically needs a message delayed beyond a restart.
> *Fix sketch:* Persist the highest round used per transaction, or start a restarted driver at round >= 2 after a Nack-probe.


Drivers hold no durable state; a restarted designated leader's driver (for a recovered `Tx` idle 5 heartbeats, or on a
client retransmit) starts again at `(1,L)` (`SimpleTransactionDriver.scala:48`, `Proposer.scala:11-13`). Equal ids are
accepted (`Acceptor.scala:31`). If the previous incarnation's `Accept((1,L), v1)` is still delayed in the network, and the
new incarnation has meanwhile decided `v2` with the transaction not yet finalized at a quorum, the delayed Accept
re-accepts `v1` at acceptors that held `v2`; a later driver reading their promises resolves `v1`. Needs a message delayed
beyond a leader restart plus a decision, so practically rare. Same for rounds >= 2 of a restarted recovery driver.
Fix: derive the round from durable state, or let a driver always advance past `promised` ids it learns from Nacks before
its first Accept (it does not, because `receiveTxPrepare` is dead, 7.3).

### F6. Abort resolution forgets acceptor state before finalization; safety then rests on in-memory caches and the clock. Confidence: low-medium

> **Verification 2026-11-09: PLAUSIBLE. Severity: split-decision (rare).** Abort resolution deletes the CRL record including `accepted` (Tx.scala:627, Frontend.scala:481), but the in-memory Tx (with its Acceptor) stays in Frontend.transactions until TxFinalized (Frontend.scala handleTransactionMessage TxFinalized), so a later round at a LIVE store still sees the accepted value; the loss needs a store restart (which also loses the host status cache) or a Tx dropped by close/recovery, plus a promise quorum made only of such stores and a young/skewed prepare so TooOld does not apply. Real but needs several coincidences.
> *Fix sketch:* Keep the Paxos record until TxFinalized for aborts too (as for commits).


`Tx.scala:624-627` deletes the CRL record (including `accepted`) at abort resolution, and at `TxFinalized` all Paxos
state of either outcome is dropped. Subsequent safety depends on: the host status cache having the entry (in memory,
lost on restart, evictable by size at 4 M entries, 7 s), the client-supplied timestamp being honest (F3), and proof of
commit being available (F2). Concretely: an abort chosen by acceptors A,B,C that then forgot it; a later driver round
whose promise quorum is made of such acceptors sees no accepted value and tallies **fresh** votes. If the fresh votes
reach `t` commits (the requirements that caused the original abort have since cleared), it may choose commit. Reachable
only if the cache entry is gone (host restarted within 6 s of the transaction's start, or size-evicted: both log or are
rare) and the horizon does not apply (young by timestamp or skewed clock). Open question: is deleting the record at
abort resolution rather than finalization worth the saved space? Retaining it until `TxFinalized` (as for commit) would
remove the restart variant at the cost of a longer-lived record.

### F7. `Undetermined` answers are counted as abort votes. Confidence: high that it happens, low impact

> **Verification 2026-11-09: CONFIRMED. Severity: liveness/hardening (safe by P7 while some acceptor holds the chosen value).** TransactionDriver.scala:272-275: every non-VoteCommit disposition, including Undetermined, is counted as an abort vote. Tx.answerPrepare/respondToPrepare answer with disposition as-is once oresolution is set (Tx.scala:700-749 region; determineDisposition is skipped when oresolution.nonEmpty, :441-460).
> *Fix sketch:* Count Undetermined as no vote.


`TransactionDriver.scala:272-275` treats every non-`VoteCommit` as an abort vote. A `Tx` that was resolved before it
voted (data-less prepare first, then `TxResolved`, then a driver's prepare) answers with `Undetermined`
(`Tx.scala:717` skips `determineDisposition` once `oresolution` is set; `respondToPrepare` sends `disposition`
as-is). A later-round driver may therefore see up to `a` spurious aborts and, with no accepted value in its promise
quorum, propose abort even though `t` real commit votes exist. Safe by P7 only while some acceptor still holds a chosen
value; combined with F6 it widens the amnesia window. Fix: count `Undetermined` as "no vote" (do not count it toward
`n`).

### F8. The tally cannot decide an abort while any referenced pool has fewer than `t` answers. Confidence: medium (liveness)

> **Verification 2026-11-09: CONFIRMED. Severity: liveness.** The object loop iterates a Set and `break()`s with cantAttempt on the first object whose pool has fewer than writeThreshold replies (TransactionDriver.scala:281-284) even if an earlier or later object is already abort-certain; with one pool down, voters on the other objects keep their locks and the client retransmits forever.
> *Fix sketch:* Evaluate all objects first; decide abort as soon as any object cannot reach commit; only wait when no object is abort-certain.


`TransactionDriver.scala:281-284`: `break()` out of the object loop with `cantAttempt` on the first (set-ordered)
object whose pool has `n < t`, even if another object's pool already makes commit impossible. A transaction touching an
object in a pool that is down (or whose stores are all withholding votes) neither commits nor aborts; stores that voted
commit on the other objects keep their locks; the client retransmits forever (`SimpleClientTransactionDriver.scala:37-50`).
The `TODO` at `:275` acknowledges the tally could be smarter. Fix: evaluate abort-certain objects first and decide abort
as soon as any object cannot reach `t` commits.

### F9. The host-wide status cache is written by every store and by constructors. Confidence: medium, low impact

> **Refresh (replay pins): NARROWED, impact raised.** `Frontend`'s no-Tx `TxResolved` path now skips a finalized entry
> (`Frontend.scala:636-637`), but `Tx.resolved` still writes `(status, finalized = false)` over another store's finalized
> entry (`Tx.scala:737-738`). Finalized entries are now late-commit evidence (`Store.scala:158-171`): a downgrade makes
> the relay answer a non-late `TxResolved`, so a replay's driver runs the finalization actions again and a fresh Tx at
> that store applies a blind write again (X3).

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica / hygiene.** TransactionStatusCache.updateStatus is an unconditional put (TransactionStatusCache.scala:64-66); Tx's constructor writes Unresolved (Tx.scala:114) and resolved() writes finalized=false (Tx.scala:662) over an entry another store of the host may have marked finalized (Tx.scala:800). Frontend.receivePrepare restores status for new Txs only (Frontend.scala:660-664).
> *Fix sketch:* Make updateStatus monotone (never downgrade finalized or a known outcome to Unresolved).


`Tx` construction overwrites the entry with `Unresolved` (`Tx.scala:114`), including for a recovered `Tx` of another store
on the same host; `resolved` writes `finalized=false` (`:662`) even over an entry another store marked finalized
(`:800`). `Frontend.receivePrepare` restores the status for the new-Tx case but not the finalized flag, and not for
recovered `Tx`s. Effect: a transaction finalized by store A can lose its "finalized" mark on that host while B finishes
late, so A may recreate a young `Tx` from a stray prepare and wait for a `TxFinalized` no live driver will re-send,
until its idle recovery fires or the entry expires. Mostly hygiene; it also widens the F6 window.

### F10. `lastProposer` is overwritten by client retransmits. Confidence: medium, low impact

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica (extra repair only).** Tx.receivePrepare sets lastProposer = m.from for every prepare, client prepares included (Tx.scala:680), and TxCommitted/proven TxResolved go to lastProposer (Tx.scala:~600, 519-525). After resolution drivers send TxResolved, not prepares, so the redirect is not corrected.
> *Fix sketch:* Do not update lastProposer from fromClient prepares.


`Tx.receivePrepare` sets `lastProposer = m.from` for every prepare (`Tx.scala:680`), and client prepares carry the
leader's id. `TxCommitted` and the proven-commit `TxResolved` go to `lastProposer` (`:611`, `:525`). If the leader is dead and
a recovery driver R is running, a client retransmit after R's prepare redirects this store's later `TxCommitted`
to the dead leader. R's finalizer then sees the store as not having committed, and
`MissedUpdateFinalizationAction` ErrorTree and UnackedCommits entries are written for a store that did apply. Harmless to correctness
(repair reconciles exactly) but it adds repair load and can delay finalization.

### F11. Split decisions are undetectable. Confidence: high (hardening)

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only (observability).** Tx.resolved (Tx.scala:645), onResolution (TransactionDriver.scala:471-474) and Tx.receiveFinalized (:783) silently ignore a conflicting second outcome.
> *Fix sketch:* Log error plus a counter on any conflicting TxResolved/TxFinalized.


`Tx.resolved`, `onResolution`, `receiveFinalized` and the client each ignore a conflicting second outcome silently
(P19). If any of F4-F6 ever produced two outcomes, nothing would log or alarm. Recommend an `error` log on a
conflicting `TxResolved`/`TxFinalized` at every site and a counter.

### F12. After a round change, Accept relies on the timer; `localProposal` is sticky. Confidence: high, latency only

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (latency).** TransactionDriver.scala:254 `!proposer.haveLocalProposal` leaves shouldSendAcceptMessages false in rounds >= 2; SimpleTransactionDriver.sendPeerMessages sends the Accept on the next timer (:54-85).
> *Fix sketch:* Set shouldSendAcceptMessages when the new round's quorum is reached.


`TransactionDriver.scala:254` skips the quorum handling when a local proposal already exists, so
`shouldSendAcceptMessages` is not set in rounds >= 2; the Accept goes out at the next periodic send (up to `maxDelay`)
(`SimpleTransactionDriver.scala:54-85`). The `NoRecovery` driver used in some tests has no timer. Also the
local proposal from round 1 is reused without re-tallying (`Proposer.scala:38`); safe because votes are immutable, but
the invariant is implicit.

### F13. `proveCommitted` leaves an in-memory `VoteAbort` that a later save can persist. Confidence: low

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: doc-only/low.** decideDisposition sets disposition = VoteAbort before proveCommitted (Tx.scala:489-493); answerPrepare then returns without respondToPrepare, but receiveAccept saves the in-memory disposition (Tx.scala:751-766).
> *Fix sketch:* Set disposition only after the proof check, or reset it to Undetermined in proveCommitted.


`decideDisposition` assigns `disposition = VoteAbort` before it calls `proveCommitted` (`Tx.scala:489-493`). That Tx
never answers with it, but `receiveAccept` (`:759-760`) and a later prepare save `disposition` as it stands, so a restart
can recover a `VoteAbort` on a committed transaction (status `Committed`). It would be sent as an abort vote to a later
driver. Practically masked by the proof, but P4's exception.

### F14. Adopted late object data is not persisted until the next save. Confidence: medium, low impact

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica (repaired).** adoptObjectUpdates mutates objectUpdates in memory (Tx.scala:672-676) and no CRL save follows.
> *Fix sketch:* Save the Tx when new object data is adopted.


`adoptObjectUpdates` (`Tx.scala:672-676`) changes `objectUpdates` in memory only. A store that had voted abort for
missing data, adopted data, applied a commit decided by the others, and crashes before its backend commit completes
recovers without the data (`TransactionRecoveryState` lacked it). It then reports an uncommittable object and repair
restores it. Safe by repair; noted because P13 ("data lives in the CRL until the backend commit succeeds") does not hold
for adopted data.

### F15. Dead or unused protocol code. Confidence: high (informational)

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only.** No caller of TransactionDriver.receiveTxPrepare, no constructor of TxStatusRequest outside codec/tests (grep).
> *Fix sketch:* Delete or wire in.


`TransactionDriver.receiveTxPrepare` (never called), `Proposer.receiveNack`/`maySendAccept`/`currentPrepareMessage`, the
whole `TxStatusRequest` path (`Tx.scala:858-862`; no sender), and the commented client learner
(`ClientTransactionDriver.scala:45-59`). The unused `receiveTxPrepare` means a new round's id is learned only through
Nacks (extra rounds under contention).

### New findings from the replay-pins refresh (2026-11-09)

#### P-N1. A proof is sent once, to `lastProposer`; a driver that did not receive it may decide abort. Confidence: high (mechanism)

> **Verdict: CONFIRMED (mechanism); trigger PLAUSIBLE. Severity: split-decision (client-visible false abort, as F2).**
> `proveCommitted` sends one proof to `lastProposer` (`Tx.scala:585-597`); a non-muted prover answers every later
> prepare as an acceptor with its abort vote (`:786-788`). The proof reaches a driver only if one exists on that store
> when it arrives (`Store.scala:199-201`); stores ignore proofs (`Tx.scala:858`, `Frontend.scala:636`). Any other driver,
> or a later one on the same store, has `holdsProof = false` and may set an abort proposal (`TransactionDriver.scala:318-319`).
> Paths: the replayed client prepare names the designated leader, whose store has no driver (down, or its copy of the
> prepare lost; F10 redirects `lastProposer` the same way); or a recovery driver tallies before the proof holder. The
> consequence is F2's: no store state changes (forgotten stores apply nothing for an abort), the client may be told
> aborted, and a live commit voter drops its update and relies on its ErrorTree mark.
> *Fix sketch:* have a prover re-send the proof to every new proposer id it sees, not only the first.

#### P-N2. Late condition 1 relies on rebuilt acceptors being muted, and the mute relies on clocks. Confidence: low-medium

> **Verdict: PLAUSIBLE. Severity: lost committed update (no ErrorTree marks).** The argument for `resolveLate`
> (`TransactionDriver.scala:514-538`) needs every acceptor that accepted the chosen commit either to remember it or to
> have received `TxFinalized`. A rebuilt acceptor forgot without `TxFinalized`; it is excluded only by
> `acceptorSince` (`Frontend.scala:676`: muted iff `txd.startTimestamp < acceptorSince`), and the start timestamp is
> client-chosen. A client clock fast by more than the rebuild's duration plus the 10 s margin (the X2 residual) makes
> the rebuilt store answer a memoryless promise for a live T, and can also make it the prover (it got T's state from the
> rebuild). With quorum intersection only through it (Replication(3,2): one store), a post-proof round sees a memoryless
> quorum and resolves late while T is still live: no finalization actions run, so no ErrorTree marks or UnackedCommits
> entries are written, and every live unresolved commit voter applies nothing on the late commit (`Tx.scala:926-932`,
> lookup absent, `:1032-1035`). Needs a fast client clock, a rebuild completing while T is unresolved, and a minimal
> intersection. Before the refresh the same window gave X2's split decision.
> *Fix sketch:* take `acceptorSince` from the host's receipt time of the first prepare, or bound client timestamps.

#### P-N3. A recovering acceptor's accepted commit produces a non-late commit for a forgotten transaction (design residual, broader than stated). Confidence: medium

> **Verdict: PLAUSIBLE. Severity: resurrection (pin-only blind writes, `DoesNotExist` inserts after a later delete,
> allocations).** The design's residual needs the pin object changed (no proof). The code shows it does not: with a
> proof, a post-proof prepare quorum that includes the recovering acceptor's accepted value is not memoryless, so the
> driver adopts the value and resolves *normally* (`TransactionDriver.scala:262-268`, Ruling 7), runs the finalization
> actions again (Ruling 13) and sends non-late `TxResolved`/`TxFinalized` (`:602-605`). Whichever of the recovering store
> or the other acceptors answers first decides between late and normal. Fresh Txs made by a concurrently replayed client
> prepare (which carries data) then apply on a non-late commit (`Tx.scala:866-869`), re-checked only by their own
> requirements: per-write checked writes are skipped, pin-only blind writes, `DoesNotExist` inserts after a later delete
> and allocations (whose `Allocating` state is built without a backend read, transaction-requirements F11) are applied.
> Fresh Txs made by the recovering store's own data-less prepares cannot apply data and only report errors, which marks
> every store again (extra repair). Needs an acceptor offline past the cache window holding T's accepted commit, and a
> replayed client prepare during its recovery.
> *Fix sketch:* when a driver holds a proof, treat an adopted accepted commit from a quorum that also contains
> memoryless acceptors as late.

#### P-N4. A forgotten abort can be re-decided commit: pins protect committed transactions only. Confidence: medium

> **Verdict: PLAUSIBLE. Severity: split-decision (a transaction reported aborted later commits).** The pin argument
> ("after T commits ...", P16) says nothing about an aborted T, whose pin precondition still holds. After
> `TxFinalized(false)` every store forgets T (an abort deletes its CRL record at resolution, `Tx.scala:699`) and the cache
> forgets it 7 s later. A later prepare of T, a delayed duplicate client prepare or the periodic prepares of a zombie
> driver (X4), creates fresh Txs checked against current state with no age test (`Frontend.scala:671-691`,
> `Tx.scala:518-534`). If the requirement that made T abort has since cleared (a `DoesNotExist` key deleted, a lock holder
> gone) and the pin object is unchanged, `t` stores vote commit and T commits after its client was told it aborted. A
> zombie driver's prepares carry no data, so only data-less transactions (refcount changes, locks) can commit that way.
> The horizon used to refuse prepares older than 6 s (with F3's clock hole). This widens F6.
> *Fix sketch:* a durable or long-lived abort tombstone per txid on the pinned object's stores, or a per-transaction
> "aborted" record the client writes before reporting abort.

#### P-N5. Liveness of "never propose abort while holding a proof". Confidence: medium

> **Verdict: REFUTED (no new hang).** A proof holder resolves on a memoryless post-proof quorum, on an adopted accepted
> value (sent without the tally, `TransactionDriver.scala:264-268`), or on any late message; a Nack moves it to another
> post-proof round (`:495-501`). It waits only for a prepare quorum of the primary pool, which every driver needs; the
> secondary-pool tally stall (F8) does not apply to it. A competing driver's chosen abort is adopted (Ruling 2). The cost
> is that a proof holder can never conclude an abort itself, which is correct since a proof implies commit.

#### P-N6. The finalized relay reports every finalized commit as late. Confidence: medium

> **Verdict: REFUTED.** `Store.scala:158-171` relays a finalized committed entry as `late`. A finalized entry is written
> only on a real `TxFinalized` (`Tx.scala:902`, `Frontend.scala:646`) or a late outcome (`Tx.scala:959-961`,
> `Frontend.scala:636-637`); proofs and found entries write nothing (`Tx.scala:1006`). A real `TxFinalized(true)` follows
> the finalizer (`TransactionDriver.scala:593-596`), so the relayed late claim holds. The weakness runs the other way: a
> downgraded entry loses the evidence (F9, X3).

### Verified safe (not flaws, listed so they are not re-investigated)

* Quorum intersection: IDA constructors enforce a majority write threshold (1.3).
* Vote and promise durability before send (`Tx.scala:739-745,759-765`), vote immutability across restart
  (`:394-403`), and commit requiring `t` commit votes per object (7.2).
* Commit-in-flight ordering and lock lifetime are in the sibling spec; nothing in the Paxos path bypasses them.
* Commit applies only on resolution or proof (P11); `TxFinalized(true)` before `TxResolved` is handled
  (`Tx.scala:790-799`).
* Stale responses from older rounds are filtered by proposal id (`TransactionDriver.scala:235,319`).
* A decided commit survives the leader's death: the recovery driver's promise quorum intersects the acceptors that
  accepted it, and a committed `Tx` keeps its Paxos record until `TxFinalized` (P9).

### Verification summary (2026-11-09)

* **1RTT:** not implemented. Only the designated leader (or a recovery driver) sends `TxAccept` (`TransactionDriver.scala:438-447`, the only constructor). Error-free: client prepare -> store (CRL write 1) -> leader -> Accept -> acceptors (CRL write 2) -> leader -> `TransactionResolved` to the client = 5 one-way hops, 2 serial durable writes. Aborts are decided in round 1.
* **Is a decision ever "fixed" by acceptor state after finalization?** No. Commit keeps acceptor state until `TxFinalized` at each store; abort drops it at resolution; after finalization a stale prepare starts a fresh consensus instance (F2). Applied stores are never rolled back by it, but the client and any still-live commit voter are told abort.
* **Refresh 2026-11-09 (replay pins):** the fresh instance is now steered by the pin rule and the late-commit rule
  (10.5): with a proof it resolves late (nothing applied, client told committed), otherwise it aborts (F2 narrowed). A
  forgotten abort is not protected (P-N4).
