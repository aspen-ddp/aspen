# Paxos-based transaction resolution: safety specification

Status: derived from the code at commit `c3ad5493` (main); refreshed 2026-11-09 against `0d08e3c6` (branch
`pin_refactor`, replay pins: `docs/superpowers/specs/2026-11-09-replay-pins-design.md`). The horizon,
`TransactionTooOld` and the UnackedCommits name no longer exist; sections 3, 6, 10 and 12 describe the pin rule, late
commits and the UnackedCommits tree instead. Line numbers in passages the refresh did not touch are for `c3ad5493`;
refresh evidence is for `0d08e3c6`. Refreshed again 2026-11-10 against `c181375b` for the replay-pins addendum
(`docs/superpowers/specs/2026-11-09-replay-pins-addendum-design.md`, with its "Amendments during implementation"):
prefer-abort driver prepares (6.2), applied stubs and `TxFinalized.owed` (9.1, 10.5); its evidence is for `c181375b`
and is marked "(addendum)". Every `path:line` citation was refreshed 2026-11-11 against `2df0c4f1`; a citation whose
code has since been removed names the commit it was taken at (e.g. "`Frontend.scala:655` at `c3ad5493`"). The text of
dated verification and refresh notes still describes the code of its date. Every mechanism is cited as `path:line`;
paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`. Where this document
disagrees with `CLAUDE.md`, `Architecture.md` or `Todo.txt`, the code was taken as the truth and the disagreement is
called out (section 2). Refreshed 2026-11-13 against `e9e6f3f6` for the transaction horizon
(`docs/superpowers/specs/2026-11-13-transaction-horizon-design.md`; Todo 1.3): a 20 s horizon past which no store joins
a transaction it does not hold (the per-store forget floor), a 30 s status cache, expiry of stuck Txs from their peers'
status, and the client error `TransactionOutcomeUnknown`. It is new mechanism, not the replay horizon removed on
2026-11-09 (that one made stores vote `TooOld`; this one makes them refuse to take part). Its passages are marked
"(horizon)" and cite `e9e6f3f6`; the mechanism and its safety argument are in 10.10, the findings it changes in the
re-assessment table at the start of section 12.

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
| Primary pool | the pool of `txd.primaryObject` | Chosen by `maxBy(poolIDAMap)` over all object requirements (`client/internal/transaction/TransactionBuilder.scala:122-125`). Its stores are the Paxos **acceptors**; its IDA gives the quorum (`ida.writeThreshold`). |
| Secondary pools | other pools referenced by the txd | Their stores **vote** (VoteCommit/VoteAbort on their own objects) but are not acceptors: the driver ignores their promises for Paxos purposes (`server/transaction/TransactionDriver.scala:302-303`, `isValidAcceptor` at `:185-187`) and `TxAccept` is sent only to `primaryObjectDataStores` (`:544`). |
| Designated leader | store `txd.designatedLeaderUID` of the primary pool | Chosen randomly by the client among stores believed online (`client/internal/transaction/DesignatedLeader.scala:20-28`). It hosts the normal-case **proposer and learner** (`TransactionDriver`). |
| Recovery driver | any store whose `Tx` has been idle for `Host.txHeartbeatTimeout` (5 heartbeats) | Same `TransactionDriver` class; started by `Store.heartbeat` (`server/store/Store.scala:78-89`) or `SimpleDriverRecoveryMixin` (`server/SimpleDriverRecoveryMixin.scala:18-28`). |
| Store-side participant | `Tx` (one per transaction per store, `server/transaction/Tx.scala`) inside a `Frontend` | Owns the **acceptor** (`Tx.scala:76`), the vote, the object locks, and commit application. |

Everything for one store runs on that store's executor (see "Host threading model" in `CLAUDE.md`); `TransactionDriver`
has its own lock and runs on timer/ec threads.

### 1.2 Identifiers

* `ProposalId(number: Int, peer: Byte)` ordered by number then peer (`common/paxos/ProposalId.scala:3-11`). `peer` is
  the **pool index** of the proposing store (`TransactionDriver.scala:102`: `new Proposer(storeId.poolIndex, ...)`).
  The client's prepare carries `ProposalId.initialProposal(designatedLeaderUID) = (1, leader)`
  (`ClientTransactionDriver.scala:77`).
* A leader driver starts at `(1, leader)`; a non-leader driver calls `nextRound()` in its constructor, giving
  `(2, self)` (`SimpleTransactionDriver.scala:48`, `Proposer.scala:64-68`).
* Paxos values are `Boolean` (true = commit). Store votes are `TransactionDisposition` {`Undetermined`, `VoteCommit`,
  `VoteAbort`} (`common/transaction/TransactionDisposition.scala`). Outcomes are `TransactionStatus` {`Unresolved`,
  `Committed`, `Aborted`}.

### 1.3 Quorum arithmetic

Acceptors are primary-pool stores `0..w-1`, quorum `t = writeThreshold`. `Proposer`/`Learner` `require(t >= w/2 + 1)`
(`Proposer.scala:8`, `Learner.scala:5`). The IDA constructors already enforce a majority write threshold: Replication
`writeThreshold >= width/2 + 1` (`common/ida/IDA.scala:203`); ReedSolomon and RandomAccessReedSolomon require
`restoreThreshold >= width/2+1` and `writeThreshold >= restoreThreshold` (`IDA.scala:408-415`, `:588-595`). So any two
write quorums intersect, which both Paxos and the "two conflicting transactions cannot both reach `t` commit votes on
one object" argument (see [transaction-requirements.md](transaction-requirements.md)) rely on.

---

## 2. Design intent versus implementation (read this first)

`Architecture.md:139-155` and `Todo.txt:11-14` at `c3ad5493` describe the intended protocol:

> each DataStore writes its VoteCommit with the Paxos state, sends the Promise to **all** DataStores, tracks the
> promises, and once VoteCommits exceed the threshold for every object **self-generates the Accept** ... it is the only
> valid Accept in round 1 ... To resolve an abort, we have to move on to round 2. Recovery always tries to abort.

**The code does not do this.** What it does:

| Aspect | Documented intent | Code (verified) |
|---|---|---|
| Who receives the Promise/vote | every store | only the proposer named in the prepare's `from` (the leader): `TxPrepareResponse(m.from, ...)` at `Tx.scala:873` |
| Who generates Accept | each acceptor, locally | only a `TransactionDriver`: the sole construction site of `TxAccept` is `TransactionDriver.scala:545` |
| Who is the learner | each store (or the client) | the driver's `Learner` (`TransactionDriver.scala:103`, `:394`); the client's learner is commented out (`ClientTransactionDriver.scala:28`, `:45-59`) |
| Round-1 commit-only rule | commit may only be proposed/accepted in round 1; abort needs round 2 | **No such rule exists.** `Acceptor.receiveAccept` accepts any value at any `>=` promised id (`common/paxos/Acceptor.scala:24-35`). A leader tallying an abort proposes `Accept((1,L), false)` directly in round 1; a recovery driver (round >= 2) may propose commit if it tallies `t` commit votes (`TransactionDriver.scala:314-365`). |
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
txd, the store's object data and its vote. Code: `Tx.scala:876-882` (prepare), `:499-516` (delayed prepare),
`:896-902` (accept); the CRL calls the handler only after the log entry is written (`DSYNC`,
`server/crl/simple/StreamWriter.scala:38-41`, `LogEntry.scala:46-69`).

**P4 Vote immutability.** A store's non-`Undetermined` disposition for a transaction is computed once and never
recomputed while the `Tx` lives; a recovered `Tx` keeps the persisted vote instead of recomputing it. Code:
`Tx.scala:479-480` (recovered with a vote skips `determineDisposition`), `:844-846` (only `Undetermined` is decided),
`:400` (`endCollisionWait`). (Exception: F13.)

**P5 Vote and promise are one durable record.** The disposition travels in the same `TxPrepareResponse` as the
`Promise` and is saved in the same `TransactionRecoveryState` (`Tx.scala:873-882`). A vote that a proposer has seen
therefore survives any crash of that store.

**P6 A commit proposal requires `t` durable commit votes on every object.** The driver proposes commit only if, for
every object in the txd, at least the pool's write threshold of that object's pool's stores reported `VoteCommit`
in the current round (`TransactionDriver.scala:321-356`, analysed in 7.2). Combined with P5 and P12 this means a
decided commit is backed by locked, durable commit votes at a write quorum of every object's pool.

**P7 Abort is always a valid free choice.** If no value has been chosen, any proposer may propose abort at any time
without violating anything (aborts apply nothing). So misclassifying a non-vote as an abort vote (F7) can cost
liveness/throughput but not safety, provided P8 holds.

**P7a The expiry abort is an ordinary abort proposal (2026-11-15, F8).** Past expiry a driver proposes abort on an
undecided tally. It does so under exactly the conditions of the vote-based abort: a promise quorum of the current
round, no proof held, and `Proposer.proposalValue` preferring any accepted value of that quorum. Only the vote count is
relaxed, and by P7 the vote count never makes an abort safe; P6 concerns commit only. A commit chosen and then
forgotten is not overturned: an acceptor forgets T only at TxFinalized and never rejoins it (forget floor), so every
promise quorum meets the accepting quorum at a store that still reports the accepted commit. A commit voter whose
driver decides abort still makes its UnackedCommits lookup first when expiring (`needsUnackedCheck`).

**P8 Single chosen value.** For one transaction at most one of {commit, abort} is chosen by the primary pool's
acceptors, given P1-P3 and that the acceptor state of a possibly-chosen value is still held by a quorum-intersecting
set of acceptors (P9). The proposer picks the highest accepted value reported in its promise quorum before it falls
back to its tally (`Proposer.scala:41-51`, `receivePromise` `:93-103`).

**P9 Acceptor-state retention (assumption).** The state of a *possibly chosen* value is retained until it can no
longer matter. In code: a committed `Tx`'s CRL record (and in-memory acceptor) lives until `TxFinalized`
(`Tx.scala:949-960`, `Frontend.scala:664-676`). An **aborted** `Tx`'s CRL record is deleted at *resolution*, earlier
(`Tx.scala:746-749`, `Frontend.scala:589-591`); and after `TxFinalized` the store has no Paxos state at all. For a
transaction whose state a store has forgotten, safety is carried by *other* mechanisms: the pin rule (P16), the host
status cache (30 s since the horizon, P17), `Tx.proveCommitted` with the late-commit rule (P15, 10.5), and the
UnackedCommits tree. (horizon) Since 2026-11-13 these no longer carry it alone: the forget floor (10.10) makes a store
refuse every prepare for a transaction it may have forgotten, so no driver ever counts the fresh state of an acceptor
that forgot T, and the time bound is back in the prepare path in that form. (addendum) A forgotten *abort* is carried by prefer-abort
(6.2): a store that forgot T never gives a driver a fresh commit vote. A committed T's record may be replaced by an
applied stub instead of deleted (`Frontend.concludeTransactionState`, `Frontend.scala:684-690`); a stub carries no
Paxos state, only "this store holds T's update" (crash-recovery-log.md 3.6).

**P10 Only the chosen value is announced.** `TxResolved`, `TransactionResolved`, `TxFinalized` are sent by a driver
only after its `Learner` has a quorum for one proposal id (`TransactionDriver.scala:396`, `:690-726`), or after
another party's resolution reached it (`receiveTxResolved` `:404-430`, `receiveTxFinalized` `:440-459`). Any store may
originate a `TxResolved(committed=true)` by proof (`Tx.proveCommitted`, P15). All receivers trust any `TxResolved`.

**P11 Application only on resolution.** A store applies a transaction's update to its objects only on: a non-late
`TxResolved(true)` (`Tx.scala:905-919`), a non-late `TxFinalized(true)` (`:924-947`), a found UnackedCommits entry
(`unackedCommitFound`, `:1078-1120`), or a late commit that reaches a Tx holding its own commit vote whose entry is
found (`lateCommit`, `:984-1007`). Never on its own vote, on an Accept, or on a proof: a prover applies nothing
(`applyNothing`, `:1016-1040`). (addendum) Never at a store that holds an applied stub for T: `doCommit` applies nothing
and reports T's changed objects as uncommitted (`Tx.scala:642-648`), and a late commit reaching an undecided commit
vote with a stub applies nothing without a lookup (`:998-1000`, `:1087-1088`).

**P12 Commit-vote locks.** From the moment a store votes commit until it learns the outcome, the requirements of that
transaction hold locks on the store's object states (`Tx.scala:318-323`), including after a restart (a recovered commit
vote re-takes them, `:472-482`; the Frontend holds all messages until every recovered vote has loaded,
`Frontend.scala:324-357`). Details in [transaction-requirements.md](transaction-requirements.md).

**P13 Object data lives in the CRL until the backend commit succeeds.** `crl.dropTransactionObjectData` is called only
after every issued backend commit completed without error (`Tx.scala:726-735`, `Frontend.scala:692-702`); a failed
backend commit keeps the record for the life of the loaded store.

**P14 Finalization ordering.** A driver sends `TxFinalized(commit)` and `TransactionFinalized` only after all
FinalizationActions completed (`TransactionDriver.scala:650-653`). `TxFinalized(abort)` is sent immediately at
resolution (`:707-708`). A store deletes a committed transaction's CRL record only after `TxFinalized` **and** its
backend commits (`Frontend.scala:664-702`). (addendum) A commit `TxFinalized` carries `owed`, the stores for which the
finalizer's marker handed UnackedCommits entries off, set atomically with finalization and empty for aborts and late
commits (`TransactionDriver.scala:465-469`); a store named there replaces its record by an applied stub at that same
point (`Tx.scala:935-936,959`; no Tx: `Frontend.scala:777-787`). A driver reached by another driver's non-late commit
`TxFinalized` cancels its finalizer and finalizes at once with the received `owed` union its marker's planned set
(`handedOffStores`, empty if nothing was handed off before the cancel): Ruling 7, `TransactionDriver.scala:440-459`,
replacing Ruling 3's wait. Its entries may then become durable after its `TxFinalized` (P14's ordering holds for the
first finalizer to complete, not for this driver's own late entries); the repair scan allows for that (Ruling 8).

**P15 Proof of commit is sound.** If an object a store hosts shows revision `ObjectRevision(txid)` (or, for a
revision-less KeyValueUpdate, some key carries it), the transaction committed, because only a committed transaction
writes that revision (`RequirementsApplyer.appliedBy`, `server/store/RequirementsApplyer.scala:50-63`). A store that
finds this on the abort path, with a memoryless acceptor (`mayProve`, `Tx.scala:228-229`), sends the proposer a
`TxResolved(committed, late = false, proof = true)` instead of a vote (`Tx.scala:584-586`, `:621-633`). Since the
refresh the proof is not an outcome: the receiving driver only stops proposing abort and starts a post-proof round
(10.5); stores ignore proofs (`Tx.scala:908`, `Frontend.scala:768`). Not available for traceless changes
(refcount-only, key-delete) nor when later transactions have overwritten the revision.

**P16 Pin rule (client-enforced).** Every client transaction built by `TransactionBuilder` carries a *pin* (a
`DataUpdate` at a non-allocating revision, a `VersionBump`, a KV update with a required revision, a `KeyRevision` on a
key the same update inserts or deletes, or a refcount change), or is marked `replaySafe(reason)`; otherwise it fails
with `UnpinnedTransaction` before anything is sent (`common/transaction/Pins.scala`, `TransactionBuilder.buildTranaction`).
After a pinned T commits, at most `w - t` stores of the pinned object both hold the pre-T state and do not know T, so a
replay cannot collect `t` commit votes on it. *2026-11-15:* stores check the rule too, from the description
(`DescriptionValidator`; `KeyValueUpdate.pinned` and `TransactionDescription.replaySafe`, Q-N4 in
transaction-requirements.md). The rule covers committed transactions only (P-N4). There is no age test left: a
replayed *client* prepare is checked like a new one (`Frontend.scala:812-831`). (addendum) Aborted transactions are
covered by prefer-abort instead (6.2): a driver's prepare never gathers a fresh commit vote, so only a delayed duplicate
of the client's own prepare can re-decide a forgotten abort (P-N4, narrowed). (horizon) "No age test left" no longer
holds: a prepare for a transaction the store does not hold is refused when its start is below the store's forget floor
(`Frontend.receivePrepare`, `server/store/Frontend.scala:1003-1031`; 10.10), which covers both outcomes, so a replayed
or delayed client prepare reaches a fresh `Tx` only inside the window in which every store that knew T still remembers
it. The pin rule still decides replays inside that window, for stores that never saw T.

**P17 Status cache (assumption).** The host-wide `TransactionStatusCache` remembers each transaction's status for
`txStatusCacheDuration` after its last write, bounded to 4 M entries. (horizon) The duration is
`TransactionHorizon.statusCacheDuration` = horizon + `MaxStartLead` + 8 heartbeats, 30 s with the 1 s host heartbeat
(`common/transaction/TransactionHorizon.scala:27-28`), and `Host.txStatusCacheDuration` asserts it is at least horizon
+ `MaxStartLead` + `txHeartbeatTimeout` + one heartbeat (`server/Host.scala:62-72`), so an entry outlasts the window in
which its store would still join T. Memory the cache loses early raises the forget floor instead: a size eviction
raises the host's eviction watermark synchronously with the removal (`evictionListener`,
`server/transaction/TransactionStatusCache.scala:32,49-51`), and a host restart or transfer-in builds a new Frontend whose
load time is a floor component (10.10). The cache is no longer the only memory of an outcome: past it, the floor
refuses. A store answers prepares for transactions found there with
`TxResolved`/`TxFinalized` instead of running Paxos (`Store.scala:163-189`). Entries carry `late`; a finalized
committed entry is relayed as late (`late = entry.late || entry.finalized`, `TxFinalized(..., late = committed)`,
`Store.scala:168,180`), which is late-commit evidence (10.5). It is in memory only.

**P18 Client outcome.** The client's future completes with the first `TransactionResolved` or `TransactionFinalized`
it receives (`ClientTransactionDriver.scala:61-63`, `:37-44`); it never checks that two reports agree. Resolution
precedes finalization, so a client can see "committed" before FinalizationActions have run and before any store has
applied the update (the commit is durable only as votes and accepted state in CRLs; see P6, P13).

**P19 First resolution wins.** `Tx.resolved` (`Tx.scala:772`), `TransactionDriver.onResolution` (`:691`),
`Tx.receiveFinalized` (`:925`) and the client (`:37-44`) each keep the first outcome and silently ignore a different
later one. Nothing detects a split decision (F11).

---

## 4. Messages

| Message | From -> to | Carries | Defined |
|---|---|---|---|
| `TxPrepare` | client or driver -> every store of every referenced pool | txd, proposal id, this store's object updates (client only), `fromClient`. `from` always names the **proposer store**, even for client-sent prepares. | `common/network/Message.scala:251-264` |
| `TxPrepareResponse` | store -> `m.from` | `Left(Nack(promised))` or `Right(Promise(lastAccepted))`, plus `disposition` and `collisions` | `Message.scala:267-280` |
| `TxAccept` | driver -> primary-pool stores | proposal id, value | `Message.scala:282-288` |
| `TxAcceptResponse` | acceptor -> driver | `Nack` or `Accepted(value)` | `Message.scala:290-301` |
| `TxResolved` | driver -> all stores (+ `notifyOnResolution`); or store -> driver (cache reply, proof of commit) | `committed` | `Message.scala:303-311` |
| `TransactionResolved` / `TransactionFinalized` | driver (or store cache reply) -> originating client | `committed` | `Message.scala:222-234` |
| `TxCommitted` | store -> its last proposer | object ids this store could not apply | `Message.scala:313-319` |
| `TxFinalized` | driver -> all stores; store cache reply -> proposer | `committed`, `late`, `owed` (addendum: stores the finalizer wrote UnackedCommits entries for; empty for aborts, late commits and the cache relay) | `Message.scala:321-329`; relay `Store.scala:180` |
| `TxHeartbeat` | driver -> all stores | resets the stores' idle timers | `Message.scala:331-335` |
| `TxUnknownStore` | router -> sender | the addressed store is not on this host | `server/host/StoreRouter.scala:143-146` |
| `TxStatusRequest/Response` | - | handled by `Tx.receiveStatusRequest` (`Tx.scala:1146-1150`) but **no code sends a request** | dead code |

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
 |                                     (Store.scala:191-193); tally votes; setLocalProposal(v)
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
| 1 | Client builds txd: `startTimestamp = HLCTimestamp.now`, primary object, designated leader, `originatingClient`. | `TransactionBuilder.scala:98,122-136` |
| 2 | Client sends `TxPrepare((1,L), from=L, fromClient=true)` with each store's own object slices to **every** store of every referenced pool, then retransmits all of it every `retransmitDelay` until an outcome arrives (never gives up; logs "HUNG" every 3rd retry). | `ClientTransactionDriver.scala:65-84`, `SimpleClientTransactionDriver.scala:37-50` |
| 3 | Store entry: `Store.receiveTransactionMessage` applies CRL backpressure to unseen prepares (`admitPrepare`), then `handleTransactionMessage` consults the status cache (section 10.6) and, **if this store is the primary pool's designated leader**, calls `driveTransaction`. Then `Frontend` creates or finds the `Tx`. | `Store.scala:150-193`, `Frontend.scala:751-758,793-831` |
| 4 | New `Tx`: loads hosted objects; `completeLoad` runs `determineDisposition` and `lock()`. | `Tx.scala:419-440,442-497,318-323` |
| 5 | `respondToPrepare`: `acceptor.receivePrepare` (in memory), `crl.save(state)`, **then** send `TxPrepareResponse` to `m.from`. | `Tx.scala:857-886` |
| 6 | Leader driver collects responses for its **current** proposal id only; records each vote; feeds primary-pool promises to the `Proposer`. At promise quorum plus a sufficient tally, sets the local proposal and sets `shouldSendAcceptMessages`. | `TransactionDriver.scala:281-365` |
| 7 | `sendAcceptMessages`: `TxAccept` to primary-pool stores not yet in `acceptedPeers`. | `:539-551` |
| 8 | Acceptor: `Acceptor.receiveAccept`, `crl.save`, **then** `TxAcceptResponse`. | `Tx.scala:888-903`, `Acceptor.scala:24-36` |
| 9 | `Learner.receiveAccepted` counts distinct peers at the highest id; at `t` it fixes `resolvedValue`. | `Learner.scala:14-37`, `TransactionDriver.scala:394-396` |
| 10 | `onResolution`: commit -> create finalizer (runs FinalizationActions) and queue `TxResolved` to all data stores plus `notifyOnResolution`; abort -> `onFinalized(false)` at once. Client gets `TransactionResolved`. | `:690-726` |
| 11 | Stores apply/abort (section 9). | `Tx.scala:737-792` |
| 12 | Finalizer completes -> `onFinalized(true)` -> `TxFinalized` to all stores, `TransactionFinalized` to client, driver shuts down. | `:471-496` |
| 13 | Store `TxFinalized`: Tx forgotten, status cache `finalized=true`, CRL record deleted once backend commits are done. | `Tx.scala:924-960`, `Frontend.scala:770-787,664-690` |

Note the abort ordering in `onResolution(false, true)`: `onFinalized(false)` runs first and sends `TxFinalized` (and
`TransactionFinalized`) *before* the `TxResolved` messages queued afterwards (`TransactionDriver.scala:707-717`); the
receiver ordering does not matter for correctness (a `TxResolved` for a transaction the store no longer holds only
updates the monotone status cache, which keeps a finalized entry finalized, `Frontend.scala:767-769`).

In the commit case the order of `queueTxMessages(TxResolved)` and a concurrent `onFinalized` (from the finalizer's
future, possibly with no actions) is not fixed; `queueTxMessages` replaces unsent messages and logs a warning
(`:217-224`). Losing the `TxResolved` is harmless because `TxFinalized(commit)` implies resolution at the store
(`Tx.scala:949-951`).

---

## 6. Voting

### 6.1 What a store votes on

A `Tx` considers only the requirements of objects it hosts plus non-object requirements (`Tx.scala:59-63`). The vote
is determined in `determineDisposition` (`:525-571`), in this priority order:

1. `muted` (a rebuilt store and a transaction started before its `acceptor-since`): `VoteAbort`, never sent; it proves
   the commit if an object shows the update, and is otherwise forgotten unanswered (`Tx.scala:529-536`, `:858-863`).
   Requirements are not checked and no locks are taken. (The horizon's `tooOld` and `TransactionTooOld` are gone.)
1b. (addendum) No client prepare seen (prefer-abort, 6.2): `VoteAbort` with `NoClientPrepare`, requirements unchecked,
   once the wait for the client's prepare is over or the outcome is known; it still proves the commit if an object shows
   the update (`Tx.scala:537-542`, `:584-586`).
2. Any hosted object whose earlier backend commit failed: `BackendCommitFailed` -> `VoteAbort`.
3. Any hosted object with a transaction commit in flight (`Frontend.commitInFlight`): the vote is **withheld** and the
   Tx registers to be told when the commit persists (`:547-553`); see [transaction-requirements.md](transaction-requirements.md).
4. Otherwise `RequirementsChecker.check`. If every failing requirement is a collision with a lock holder this
   transaction is built on (`awaitableHolders`, `:347-355`), the vote is withheld for up to `Tx.CollisionWait`
   (500 ms, evaluated at the store heartbeat, so up to ~1.5 s). Otherwise the result is `VoteCommit` (no errors) or
   `VoteAbort`.

"Withheld" means *no response is sent* to a prepare, which Paxos allows; drivers retransmit
(`SimpleTransactionDriver.scala:59-90`). A withheld Tx holds **no locks**, so waits cannot form cycles (`Tx.scala:342-344`).

`lock()` is called right after the vote is decided and **before** the vote is durable (`:482`, `:847`). That is safe:
locks are volatile, and a crash before the vote is durable simply discards them along with any chance the vote was
observed (P3, P5).

### 6.2 Prefer-abort driver prepares (replaces "Missing data"; addendum)

A driver's prepare carries no object data and has `fromClient = false` (`TransactionDriver.scala:516,535`). Every such
prepare is **prefer-abort**: it never gathers a fresh commit vote. A Tx with no vote (`Undetermined`, not muted, not
recovered with a vote) that has seen no client prepare (`clientPrepared`, set only by `fromClient` prepares,
`Tx.scala:808-811`) does not check its requirements (`awaitingClientPrepare`, `Tx.scala:205-208`):

1. **Proof first.** Once its objects load, if `mayProve` and an object shows T's update (`appliedHere`), the wait ends
   at once and the Tx proves to `lastProposer`, which is the sender of the latest prepare (`:806`), so the driver whose
   prepare created the Tx gets the proof (`:208`, `:537-542`, `:584-586`).
2. **Otherwise it withholds everything**, promise and vote, until the client's prepare arrives (then it checks its
   requirements as usual), the outcome is known, or `Frontend.MissingDataWait` (5 s from the Tx's creation,
   `Frontend.scala:27`) passes. It then votes abort with `NoClientPrepare` (`RequirementError.scala:40`), its
   requirements unchecked. The check runs only as prepares arrive (`answerPrepare`, `Tx.scala:835-855`), so the abort
   vote goes out with the first driver retransmit after the wait. A recovered `Undetermined` Tx follows the same rule
   (Task 1 removed the `!recovered` condition); a Tx recovered *with* a vote answers with it.

Why this closes the re-driven abort (P-N4 paths 1 and 2): a driver decides abort only when some object's pool has more
than `w - t` non-commit answers in one round, and the commit votes of that object can never exceed `t - 1` afterwards:
votes are fixed, stores that forgot T can only vote abort for a driver, and stores that did not answer that round held
at most the remaining `w - n` commit votes, which the decision already counted against commit.

(horizon) Path 3, a delayed duplicate of the client's own prepare, which prefer-abort cannot close, is closed by the
forget floor (10.10): inside the window every store that took part in the abort still holds its `Tx` or its cache entry
(an abort is finalized at once, so the entry is finalized and the Store answers the duplicate with the abort,
`Store.scala:199-221`); past the floor it refuses the duplicate outright. HorizonIntegrationSuite "a delayed client
prepare of a forgotten abort does not commit inside or past the horizon".

Effect on healthy transactions: none for a client that stays alive, since it retransmits every 1 s
(`cmdline/Main.scala:1268`) until it learns the outcome, and a known transaction's retransmit is admitted without a CRL
admission check (`Frontend.admitPrepare`, `Frontend.scala:519-536`). A data transaction already voted abort without its
data. Orphaned **data-less** transactions (refcount changes, version bumps, lock-only) now abort where a store got no
client prepare (P-N8). The commit's own wait for data is unchanged (`commitAwaitsData`, `Tx.scala:239`): a client
prepare's data is still adopted so a commit decided by others can be applied (`adoptObjectUpdates`, `:798-802`);
adopted data is saved to the CRL before a commit applies it (F14, fixed 2026-11-15: `Tx.adoptedDataUnsaved` holds the
commit until the save is durable).

### 6.3 Proof of commit on an unknown transaction

If `decideDisposition` would vote abort, the acceptor is memoryless (`mayProve`: no accepted value and no recovered
Paxos state, `Tx.scala:228-229`) and an object already shows the transaction's update, the store `proveCommitted`s
(`Tx.scala:584-586`, `:621-633`): one `TxResolved(proof = true)` to `lastProposer`, no save, no locks, nothing in the
status cache, and no local resolution. The prover stays a live Tx: later prepares get an ordinary memoryless promise with
its abort vote, saved to the CRL (`answerPrepareAsAcceptor`, `:836-838`); a muted prover re-sends the proof instead. On
any real outcome it applies nothing and is forgotten (`applyNothing`, `:1016-1040`): it holds no commit vote, so it is not
owed the commit. Proof-first ordering is gone: the proof is tried only on the abort path. (addendum) A Tx reached only
by driver prepares is on the abort path by construction (6.2), and its wait for the client's prepare is bypassed when a
proof exists (`awaitingClientPrepare`, `Tx.scala:208`), so it proves at once to the driver that prepared it.

### 6.4 Recovered votes

A `Tx` recovered from the CRL keeps its persisted disposition and acceptor state (`Tx.scala:76`, `:472-480`).
A recovered unresolved commit vote, of any transaction (not only traceless ones), looks up its UnackedCommits entry
once loaded and again before accepting any abort (`needsUnackedCheck` `:176-178`, `holdAbort` `:1048-1058`), and its
store starts no driver for it until the first lookup answers, nor while the store's CRL state loads
(`Store.driveTransaction`, `Store.scala:104-107`). A found entry resolves it committed without forgetting it, and its
driver starts holding a proof (`Tx.scala:1078-1104`, `Store.scala:117-119`). See
[transaction-recovery.md](transaction-recovery.md).

---

## 7. The driver: tally, rounds, learner

### 7.1 State

`TransactionDriver` (`server/transaction/TransactionDriver.scala`) holds a `Proposer(self index, w, t)`, a
`Learner(w, t)`, `peerDispositions` (votes seen in the current round), `acceptedPeers`, `knownResolved`,
`resolved`/`resolvedValue`, and the finalizer. A new round clears `peerDispositions` and `acceptedPeers`
(`:498-502`). The proposer's `localProposal` is **sticky**: once set it is never cleared (`Proposer.scala:54`), so later
rounds re-propose the same tally result unless a promise reports an accepted value.

### 7.2 Tally (`TransactionDriver.scala:314-365`)

Run once, when the proposer has a promise quorum and no local proposal. For each object pointer `ptr` (iteration order
of a Set), with `w`, `t` of `ptr`'s pool, `n` = stores of that pool that answered this round, `c` = those that voted
`VoteCommit`, `a = n - c` (**every non-`VoteCommit` answer counts as an abort vote, including `Undetermined`**,
`:332-337`):

| Condition | Effect |
|---|---|
| `n < t` | cannot decide: wait (`cantAttempt`, `:341-344`) |
| `n != w` and `c < t` and `w - a >= t` | commit still possible with more answers: wait (`:346-349`) |
| `w - a < t` | object cannot reach `t` commit votes: `canCommitObject = false` (`:351-352`) |

Then `localProposal = canCommit(all objects)` (`:361-362`).

Properties (verified by case analysis):

* If the result is commit then `c >= t` for every object: when `n == w`, `w - a = c`; when `n < w` the second row would
  have forced a wait unless `c >= t` or `w - a < t`. So **commit implies `t` commit votes per object** (P6).
* Once `c >= t` or `a > w - t` is true it stays true (votes are immutable and only accumulate). So different drivers
  using the same vote set agree; they can differ only through non-votes counted as aborts (F7) or through amnesia
  (F2, F6; since 2026-11-13 no driver counts an acceptor that forgot T, so amnesia no longer feeds the tally, 10.10).
* (2026-11-15, F8 fixed) Every object is classified, none skipped: commit-certain (`c >= t`), abort-certain
  (`w - a < t`) or undecided. Any abort-certain object decides abort, whatever the other objects lack. Otherwise any
  undecided object waits, and from expiry on proposes abort (P7a). This matches the table above except that the
  first short object no longer hides a later abort-certain one.

### 7.3 Rounds, Nacks, backoff

* On the first Nack for the current id the driver advances its round immediately (`onNack`, `:553-559`); it does not
  wait for a quorum of Nacks (`Proposer.receiveNack` exists but is unused). Nacks from secondary-pool stores count too
  (the `Left` branch has no `isValidAcceptor` check, `:294-296`), though those stores never accept anything.
* `SimpleTransactionDriver.nextRound` doubles a backoff capped at `maxDelay` and sleeps a random time below it
  (`SimpleTransactionDriver.scala:92-105`). The driver re-broadcasts the current prepare/accept every `maxDelay`
  (`:59-90`), interleaving prepares with accepts every third send so stores that forgot the Tx restart it.
* After a round change the tally is skipped (the local proposal is sticky, safe because votes are immutable), and the
  new round's first prepare quorum queues the Accept at once (`acceptQueuedThisRound`; F12, fixed 2026-11-15).
* `TransactionDriver.receiveTxPrepare` was deleted (F15): drivers never receive other proposers' prepares, and Nacks
  carry the promised id.

### 7.4 Learner and resolution

`Learner.receiveAccepted` counts distinct peers for the highest proposal id seen (`Learner.scala:14-37`); the driver
only feeds it messages whose id equals its current id and whose sender is a valid acceptor (`TransactionDriver.scala:373-396`).
Resolution can also arrive without the learner: `receiveTxResolved` calls `onResolution(msg.committed,
sendResolutionMessage = false)` (`:404-423`) and trusts the sender (cache reply, proof of commit, another driver).

### 7.5 Who becomes a driver

* The designated leader, on any `TxPrepare` it receives for a transaction not finalized in the cache
  (`Store.scala:191-193`), including client retransmits. Guarded by pool id.
* Any store whose `Tx` is idle for more than `txHeartbeatTimeout` (`Store.scala:84-88`) or `failedDriverDuration`
  (`SimpleDriverRecoveryMixin.scala:18-28`). **Not guarded by pool id** (F4). A driver's own store is also one of its
  recipients; it addresses its own `Tx` like any other.
* A client's retransmitted prepare does not count as store activity (`Tx.scala:818-819`), so a dead leader cannot be kept
  "alive" by the client.
* A driver is created once per transaction per store (`Store.driveTransaction`, `:104-126`) and removed when its own
  store receives `TxFinalized` (`:216-219`).

---

## 8. Persistence ordering rules

All CRL writes are `crl.save(txid, TransactionRecoveryState, handler)` (`server/crl/CrashRecoveryLog.scala:157`). A saved
record contains: `storeId`, serialized txd, object updates (unless dropped), disposition, status, and the acceptor's
`(promised, accepted)` (`server/crl/TransactionRecoveryState.scala:8-13`). Saves overwrite the whole record
(`server/crl/simple/SimpleCRL.scala:344-366`). Each record always carries the then-current acceptor state, so a later save never loses an
earlier promise.

| Message sent | Must be durable first | Code |
|---|---|---|
| `TxPrepareResponse` (Promise or Nack) | txd, data, disposition, acceptor state | `Tx.scala:876-882`; delayed variant `:499-516` |
| `TxAcceptResponse` | acceptor state with `accepted=(id,v)` (disposition as it is then, possibly `Undetermined`) | `:896-902` |
| `TxResolved`, `TransactionResolved` (from the driver) | `t` `Accepted` (each durable at its acceptor) | `TransactionDriver.scala:396,690-726` |
| `TxCommitted` | all backend commits of this store for the transaction completed; CRL object data drop *enqueued* (not awaited) | `Tx.scala:726-735` |
| `TxFinalized(commit)` | all FinalizationActions complete | `TransactionDriver.scala:650-653` |

Not required durable before sending: `TxResolved(true)` from `proveCommitted` (the evidence is the already-durable
backend state, `Tx.scala:621-633`); status-cache replies; `TxCommitted` re-sends.

State deletions:

| Event | CRL effect |
|---|---|
| `TxResolved(false)` / `TxFinalized(false)` at a `Tx` | record deleted immediately (`Tx.scala:749`, `Frontend.scala:589-591`); a later accept or prepare handled by the lingering in-memory `Tx` re-creates it and `TxFinalized` deletes it again (`Tx.scala:953-959`). |
| commit applied and all backend commits succeeded | object data dropped, record kept (`Tx.scala:726-735`) |
| `TxFinalized(true)` and all backend commits succeeded | record deleted (`Frontend.scala:664-702`) |
| a backend commit failed | record (with data) kept until the store reloads (`Frontend.scala:868-874`) |

Ordering inside one store relies on the store executor being single-threaded and on the CRL completing handlers in
log order. The `CrashRecoveryLog.save` contract is specified in [crash-recovery-log.md](crash-recovery-log.md); this
spec only requires: *handler runs after the record, including everything saved before it, is durable*. Note a record
is saved on **every** prepare response (retransmits included), which is write amplification, not a safety issue.

What is **not** persisted: the commit/abort *outcome* at a store (status is saved only incidentally by a later save,
`Tx.scala:509-510,877-878,896-897`); the proposer's round number or local proposal; driver state of any kind; the
status cache. After a crash a store knows only its own vote and acceptor state; the outcome must be re-learned
(driver, caches, proof and late commits, UnackedCommits).

---

## 9. Commit application and finalization

### 9.1 Resolution at a store (`Tx.scala:772-792`, `:737-754`)

* `resolved(c)` sets `oresolution` once, updates the status cache, and if all objects are loaded calls
  `resolvedAndAllObjectsLoaded(c)`; otherwise the resolution is applied when loading finishes (`:484-487`).
* Abort: `frontend.deleteTransactionState`, `unlock()`, `releaseObjects()`.
* Commit: first the two holds:
  * **commit overtook the data** (`commitAwaitsData`, `Tx.scala:239`): not recovered, no data and no
    client prepare seen and not finalized ->
    hold (`commitHeld`) with no `TxCommitted`, so the driver does not yet count this store as having missed the update.
    Ends with the data, a client prepare, or `TxFinalized`; after `TxFinalized` the store applies without the data,
    reports the object as uncommittable and repair restores it.
  * **earlier commit of an object in flight** (`holdForTurn`, `:252-262`): queued per object; see
    [transaction-requirements.md](transaction-requirements.md).
  Otherwise `doCommit` (`:635-724`): unlock, `RequirementsApplyer.apply` re-checking requirements with `applying=true`
  (objects that fail are skipped and reported in `TxCommitted.objectCommitErrors`; objects already showing the
  transaction are not re-applied), issue one backend commit per changed object, and send `TxCommitted` when all
  complete (`:726-735`). (addendum) A store holding an applied stub for T applies nothing and reports T's changed
  objects in `objectCommitErrors`, so a non-late finalizer marks it for repair rather than trusting it
  (`Tx.scala:642-648`).
* (addendum) **Forgetting a committed T.** Where its CRL record would be deleted (after `TxFinalized` and its backend
  commits, Ruling 1), it is replaced by an applied stub if an UnackedCommits entry may name this store: `TxFinalized`
  listed it in `owed`, or it applied T through its entry (`stubDue`, `Tx.scala:935-936,1082,1092`;
  `Frontend.transactionFinalized`/`concludeTransactionState`, `Frontend.scala:664-690`). A failed backend commit keeps
  the record, so no stub replaces it. A finalization deferred by a commit held for its turn registers the Tx, so a
  later `owed` reaches it (`Frontend.scala:223-229,784-787`; `Tx.scala:955-957`).

### 9.2 Driver after commit resolution

`onResolution(true)` creates the finalizer immediately, passes it the commit voters (`countedCommitVoters`,
`:565-570`) and `commitErrors` from `TxCommitted` messages (`:432-438`); the finalizer runs the txd's
FinalizationActions (`server/RegisteredTransactionFinalizerFactory.scala:33-69`); at-least-once, idempotent. Missed
updates are recorded by `MissedUpdateFinalizationAction` ([transaction-recovery.md](transaction-recovery.md)).

### 9.3 Heartbeats and lost messages

Driver `heartbeat()` sends `TxHeartbeat` (not on its first tick) or, once finalized, repeats `TxFinalized`
(`TransactionDriver.scala:229-242`). A store that sees nothing for `txHeartbeatTimeout` drives the transaction itself
(`Store.scala:84-88`); that is the recovery path for a lost `TxFinalized` and works only because the status cache
(7 s) outlasts the detection (5 s + one heartbeat) (`Host.scala:59-67`). (horizon) The cache is now 30 s and outlasts
the horizon plus that detection (P17). A store whose driver still finds no outcome past the horizon (a stall, or every
peer's entry gone) is not re-decided: its peers refuse the driver, and the Tx ends by expiry (10.10), a resolved one
concluding its known outcome (`Tx.concludeKnownOutcome`, `server/transaction/Tx.scala:228`).

---

## 10. Edge cases

### 10.1 Duplicate prepares

Re-processed idempotently: the acceptor re-promises (`Acceptor.scala:15-18`), the vote is unchanged (P4), the response is
saved and sent again. A committed `Tx` also re-sends `TxCommitted` to the sender (`Tx.scala:821-825`). `lastProposer`
is overwritten by every prepare, including client retransmits (F10).

### 10.2 Late prepares

* Tx known: handled above.
* Tx unknown, status cache says finalized: answered `TxFinalized` (+ `TransactionFinalized` to the client) at the Store
  and ignored by the Frontend (`Store.scala:179-186`, `Frontend.scala:802-810`).
* Tx unknown, cache says Committed/Aborted but not finalized: the Store sends `TxResolved` to the sender and the Frontend
  creates a `Tx` with `outcomeDecided()`; it takes no locks, still votes, and learns the outcome only from `TxFinalized`
  because the driver treats the cache's reply as "this store knows" (`knownResolved`) and does not re-send
  `TxResolved` to it (`TransactionDriver.scala:406`, `SimpleTransactionDriver.scala:67`; `Frontend.scala:812-830`).
* (horizon) Tx unknown, no non-Unresolved cache entry, start below the store's forget floor: refused, no `Tx` at all
  (`refusePrepare`, `Frontend.scala:1039-1046`), and the designated leader starts no driver (`Store.scala:230-232`).
  Exception: a rebuilt store's `acceptorSince` mute is checked first and still creates its muted, possibly proving
  `Tx` (`Frontend.scala:1026-1029`). 10.10.
* Tx unknown, no cache entry: a fresh `Tx`, checked normally (the old age test is gone; since 2026-11-13 only starts at
  or above the forget floor get here). A replay of a
  committed pinned transaction fails its pin; a store showing its update proves it (6.3), and the driver resolves late
  (10.5). Accepted cost: a replay whose other requirements pass holds commit-vote locks until a driver resolves it.
* Dropped under CRL backpressure: `Store.scala:153-154`; the client retransmits, and the rest of the quorum can decide
  without this store (repair catches up).

### 10.3 Duplicate and late accepts

`TxAccept` for an unknown `Tx` is ignored (`Frontend.scala:759`), so a forgotten store never acts as an acceptor with
fresh state through an Accept alone; the driver therefore interleaves prepares (`SimpleTransactionDriver.scala:72-78`).
A duplicate Accept is re-accepted and re-answered; the driver's `acceptedPeers` set and the `Learner` bitset make
this idempotent. An Accept may reach a store that has not voted yet; it accepts (Paxos allows this) and saves its
then-current, possibly `Undetermined`, disposition (`Tx.scala:896-897`).

### 10.4 Prepares that overtake data / commits that overtake data

See 6.2 and 9.1. (addendum) A driver's prepare that overtakes the client's now makes the store wait for the client's
prepare whether or not the transaction carries data for it. The relevant constants: `Frontend.MissingDataWait = 5 s`
(`Frontend.scala:27`), `Tx.LateClientPrepareAfter =
500 ms` (log only, `Tx.scala:29`). A client prepare with data arriving after `TxFinalized` is ignored with a warning
(`Frontend.scala:807-810`); the store is repaired later.

### 10.5 Late commits (replaces the horizon)

The horizon (`Host.txHorizon`, `TransactionTooOld`) was removed. In its place (design 1b/1c and its amendments):

* **Proof holder.** A driver receiving a proof sets `holdsProof`, and the first proof starts a new round
  (`TransactionDriver.scala:410-419`): promises to earlier rounds may predate another driver's choice of commit
  (Ruling 6). It skips the vote tally and sets no local proposal. Since Ruling 29 it adopts only an accepted *commit*
  reported in the current round (`Proposer.commitProven`): commit was chosen at some ballot c and every value accepted
  at c or higher is commit, so an accepted abort predates c and is never adopted. With such a commit it sends accepts
  for commit without the tally (Ruling 7), and resolves normally.
* **Late condition 1.** A proof plus a prepare quorum of valid acceptors in the current (post-proof) round, none
  carrying an accepted commit (`Proposer.prepareQuorumWithoutAcceptedCommit`; an older accepted abort counts as
  memoryless, Ruling 29):
  `resolveLate` (`TransactionDriver.scala:310-311`, `:599-612`). Argument: the proof shows commit was chosen by some
  quorum Q; Q intersects the responders; an acceptor forgets a committed T only on `TxFinalized` or a late outcome;
  rebuilt acceptors are muted. So finalization completed.
* **Late condition 2.** A `TxResolved` or `TxFinalized` with `late = true` (`:407-408`, `:443-444`), including the
  finalized relay of a status cache (`Store.scala:167-189`).
* **Late resolution** runs no finalization actions (it cancels any it started) and finalizes at once; every
  `TxResolved`/`TxFinalized` it sends carries `late = true` (`:465-496`, `:599-612`).
* **A Tx receiving a late commit** (`Tx.lateCommit`, `Tx.scala:984-1007`): already resolved committed, it finalizes;
  holding its own undecided commit vote, it looks up its UnackedCommits entry and applies only if found
  (`unackedCommitFound`/`unackedCommitAbsent`, `:1078-1140`); otherwise it applies nothing (`applyNothing`, `:1016-1040`).
  (addendum, `Tx.scala:984-1007`) An applied stub short-cuts both: a resolved Tx with a stub due or held finalizes, an
  undecided commit vote with a stub applies nothing. A Tx already resolved committed with neither (its `TxFinalized` was
  lost and its heartbeat driver resolved late or through the relay) looks up its own entry first, writes a stub if found
  and is forgotten either way (case 3, `forgetLookupHeld`, `:990-997,1079-1086,1127-1131`).
* **Found entry as a proof** (Ruling 15): a recovered commit vote that found its entry gives its own driver a proof from
  the start (`Store.scala:117-119`).

Residuals: Ruling 17 (a late-acked store with an entry re-applies a replay's writes in pools where T has no pin of its
own; transaction-recovery R-N1 and Ruling 32: it can reach a quorum when the whole pool acked late), the design's recovering-acceptor residual (P-N3), and a proof delivered to a
driver that never acts on it (P-N1; closed by the horizon 2026-11-13: a driver without the proof can no longer decide
abort for a forgotten commit, since no store that forgot T answers it, 10.10). (addendum) Applied stubs close Ruling 17 at every store that forgot T on a
`TxFinalized` listing it, or applied T through its entry; what remains is a store with an entry and no stub
(transaction-recovery R-N1 narrowed, R-N7 to R-N9).

### 10.6 Status cache

* Write sites: `Tx` construction writes `Unresolved` (`Tx.scala:162`); `resolved` writes the status
  (`:788`); `finalized` writes `finalized=true` (`:960`); a `TxResolved`/`TxFinalized` for an unknown Tx writes the
  status (`Frontend.scala:760-779`).
* Read sites: `Store.scala:167-189` (reply with `TxResolved`/`TxFinalized`), `Frontend.scala:525,802,815` (admission,
  ignore, `known`), `StoreRouter` completion queries.
* One cache per **host**, shared by all its stores and keyed by transaction id only. Entries live 7 s after the *last
  write*. The early-eviction warning was removed with the horizon: an evicted entry only loses late-commit evidence
  (`server/transaction/TransactionStatusCache.scala:11`). (horizon) Entries now live 30 s after the last write (P17).
  A size eviction is logged as an error (at most once per 10 s, with the count), counted, and raises the host-wide eviction watermark (wall clock +
  `MaxStartLead`), a forget-floor component, so an evicted entry's absence is never trusted (10.10). Expiry by time
  raises nothing: an entry expires only once its transaction is below the floor anyway.
* (horizon) An opposite outcome reaching `updateStatus` should never arise now; it is logged as an error ("Split
  decision") and counted (`splitDecisions`), and Committed still wins (TransactionStatusCacheSuite "an opposite outcome
  is logged as a split decision"; F11 narrowed).
* An `Unresolved` entry counts as "no outcome" when a fresh Tx is created (`Frontend.scala:814-816`).
* The no-Tx `TxResolved` path ignores proofs (`Frontend.scala:767-769`). `updateStatus` is monotone
  (`TransactionStatusCache.scala:34-44`), so no write site, `Tx.resolved` (`Tx.scala:787-788`) included, downgrades a
  finalized entry (F9, FIXED).

### 10.7 Proof of commit interplay

`Tx.awaitingClientPrepare` (formerly `awaitingData`) is bypassed when objects are loaded and show the update
(`Tx.scala:208`, addendum). `doCommit` skips objects that
already show the update and logs them as already applied (`:689-694`). A prover answers later prepares as
an ordinary memoryless acceptor with its abort vote; only a muted prover repeats the proof (`Tx.scala:836-838`).

### 10.8 Unknown or moved stores

`TxUnknownStore` makes the driver drop its endpoint cache for that store and not worry (`TransactionDriver.scala:267-275`);
the transaction proceeds without it if a quorum remains.

### 10.9 Nack / dueling proposers

Resolved by backoff (7.3). Stale responses are filtered by proposal id (`TransactionDriver.scala:286,378`).

---

### 10.10 Transaction horizon, forget floor and expiry (horizon, 2026-11-13)

Design: `docs/superpowers/specs/2026-11-13-transaction-horizon-design.md`. Evidence for `e9e6f3f6`.

**Root cause it removes.** A store forgets T's outcome after a bounded time (an abort at resolution, a commit at
`TxFinalized`, the host cache after its duration). Before the horizon, a prepare reaching every store after all had
forgotten started a new Paxos instance for the same txid, which could decide the opposite outcome: a commit re-decided
abort (F2, P-N1, F10's proof leg; recovery F3, F5, F8) or an abort re-decided commit (P-N4 path 3, F6). Better proof
delivery could not close that, and infinite memory is not available. The fix is a time bound on *joining*.

**Constants** (`common/transaction/TransactionHorizon.scala`): `Horizon` 20 s (`:11`); `MaxStartLead` 2 s (`:16`, unchanged,
`Frontend.MaxStartLead` is it); the client gives up at horizon + `MaxStartLead` (`clientGiveUp`, `:21`); the status
cache lasts horizon + `MaxStartLead` + 8 heartbeats, 30 s (`statusCacheDuration`, `:27-28`; asserted by
`Host.checkStatusCacheDuration`, `server/Host.scala:69-74`). The horizon is measured on `txd.startTimestamp.wallTime`
against the receiving host's wall clock (`Frontend.wallClockMillis`, not the JVM-wide HLC), and is injectable for tests.

**The forget floor** (`Frontend.forgetFloorMillis`, `server/store/Frontend.scala:126-139`) is the latest of:

* `wallClockMillis() - horizon`;
* the store's load on this host plus `MaxStartLead` (`loadFloorMillis`; `loadedAtMillis`). Every
  restart, adoption and transfer-in builds a new Frontend through `StoreLoader`, so the host's lack of a cache for the
  store's past is covered. The lead is there because before its load the store may have joined, and forgotten, a
  transaction started up to `MaxStartLead` ahead of its clock. A store that arrived from another host (a transfer in,
  `StoreTransfers.clearTransferredInMarker`; an adoption, `DeviceDiscovery.bringIntoService`) joined there by that
  host's clock, which may lead this one's: each arrival durably writes `StoreLoader.ArrivedAtFile` (this host's wall
  clock) before the store can load here, and a load less than `RebuildingStore.AcceptorSinceMargin` (10 s) after it
  adds that margin instead of the lead (`Frontend.arrivedAt`). The margin covers the lead plus the inter-host skew
  P-N2 already assumes. Being durable, it also covers a crash between the arrival and the load, and a same-host
  restart within the margin of the arrival. Exemption: a store this process just created for a
  pool's Initializing slot (`StoreRebuilds.createNewStore` -> `StoreLoader.newStores`,
  `server/host/StoreLoader.scala:53-55,193,281-282,355`) records no load time, since it has never served. Rebuilt
  stores keep `acceptorSince` (P-N2), checked first, whose muted Tx may still prove;
* the host's eviction watermark (`TransactionStatusCache.evictionWatermarkMillis`: wall clock at the latest size
  eviction plus `MaxStartLead`).

A prepare for a transaction the store does not hold (no `Tx`, no non-Unresolved cache entry) whose start is below the
floor is **refused**: no `Tx`, no promise, vote, reply or CRL save, its admission released, a warning with the lag for
a client's prepare (`receivePrepare`/`refusePrepare`, `:1003-1046`). The designated leader starts no driver for it
(`Store.scala:225-233`). A held `Tx` (recovered ones too) and a prepare whose outcome the cache knows are unaffected.
Tests: FrontendSuite "a prepare for an unknown transaction older than the horizon creates no Tx, sends nothing and
saves nothing", "... started before the store loaded is refused", "... started inside MaxStartLead after the store
loaded is refused", "a prepare below the eviction watermark is refused"; StoreSuite "the designated leader starts no
driver for a prepare below its forget floor"; HorizonIntegrationSuite "a host restart does not let its stores rejoin a
transaction they forgot", "a store transferred to another host does not rejoin a transaction it forgot".

**Why the floor timing is safe without cross-host clock agreement.** A store writes its cache entry for T after it saw
T, and by `MaxStartLead` its own clock then read at least `start - 2 s` (it mutes any T further ahead). The entry lives
30 s past its last write, so when it expires the store's clock reads past `start + 28 s`, beyond the 20 s horizon: the
store already refuses T. A restart or size eviction loses memory at a moment that raises the floor past every T the
store might have joined before it on this host (hence the `+ MaxStartLead`). A transfer-in or adoption loses the
previous host's memory: every T the store joined there started at most `MaxStartLead` ahead of that host's clock at
the join, before the arrival here, so at most `MaxStartLead` plus the skew between the two hosts' clocks ahead of this
host's clock at the arrival, which is within `AcceptorSinceMargin` under the P-N2 skew bound (hence the arrival's
margin). So a store never forgets T while it would still join it. A same-host restart uses only the store's own clock;
a store's arrival from another host relies on the same inter-host skew bound as P-N2 (under 8 s); client clocks enter
only through `MaxStartLead`. Residual:
a host wall clock stepped *backward* by more than about 8 s (30 - 20 - 2) between a store joining T and its entry
expiring (Caffeine's expiry runs on `nanoTime`, the floor on the wall clock) reopens the window for that T.

**Status query** (`TxStatusRequest`, which carries `startTimestamp`, and `TxStatusResponse.TxStatus`, which carries
`holdsAcceptorState`: `common/network/Message.scala:337-358`; this wires up the half-built path of F15). A store with
a `Tx` answers with its status, finalized, late and `holdsAcceptorState = !muted || !mutedForGood` (`Tx.receiveStatusRequest`,
`server/transaction/Tx.scala:1259-1265`). A store without one answers from its cache entry if it has a non-Unresolved
one (`holdsAcceptorState = false`); otherwise it answers `None` ("unknown") **only if T's start is below its floor**,
which makes the answer permanent, and above the floor it stays silent, since it could still join T
(`receiveStatusRequestWithoutTx`, `Frontend.scala:996-1001`). FrontendSuite "a store that neither holds nor remembers T
answers unknown only below its floor".

**Expiry.** A `Tx` still unfinalized (`Tx.mayExpire`, `Tx.scala:215`: unresolved, or resolved and waiting for a
`TxFinalized` that may never come) past the horizon plus `expiryMargin` (`MaxStartLead` + `txHeartbeatTimeout` + one
heartbeat, `Frontend.scala:50-55`) queries every other store of T at store heartbeat, at once and then after 1, 2, 4 and
every 8 heartbeats, each round with a fresh request id (`expiryHeartbeat`, `Frontend.scala:607-640`). The pure
`ExpiryTally.decide` (`server/transaction/ExpiryTally.scala:34-52`) applies, in order:

1. any reply with an outcome is adopted (commit over abort), a commit through `TxResolved` (late if finalized or late,
   so a commit voter looks up its UnackedCommits entry), an abort as a finalized abort;
2. otherwise the primary pool's (`txd.primaryObjectIDA`) stores that replied "unknown" or without acceptor state are
   non-holders, and silent stores are possible holders, never non-holders;
3. more than `width - writeThreshold` non-holders: **discard**. A commit voter (`needsUnackedCheck`, which now covers
   expiring Txs, `Tx.scala:187-189`) first looks up its entry: none found by a lookup started after the decision
   discards it, one found applies T and concludes it as a late commit, and a failed lookup keeps it holding. A `Tx`
   that already knows its outcome concludes it instead (`Tx.concludeKnownOutcome`, `Tx.scala:228`). Discarding
   (`Frontend.expireTransaction`, `:685-694`; `Tx.expire`, `Tx.scala:274`) releases locks and objects, deletes the CRL
   state as for a forgotten abort, forgets the `Tx`, and writes nothing in the status cache;
4. otherwise **hold** and ask again.

A driver past the horizon plus the margin that has no outcome is stopped and removed once its store holds no `Tx` for
T (`Store.removeExpiredDrivers`, `Store.scala:110-122`). The client's `TransactionManager` forgets its driver however
it concludes, `Unknown` included (`client/internal/transaction/TransactionManager.scala:33-45`).

**Why rule 3 is safe.** An acceptor forgets T only once T is decided: an abort at resolution, a commit at `TxFinalized`
(P9). So a committed but unfinalized T is still held by a full quorum (`t`) of the primary pool's acceptors, and more
than `w - t` non-holders rule it out. A non-holder can never become a holder, because it answered "unknown" only below
its floor, or holds a `Tx` muted for good, which accepts nothing: one below the store's floor
(`Tx.receiveStatusRequest`). A rebuilt store is never a non-holder for T started before its `acceptorSince`
(`Frontend.mayHaveForgottenUndecided`; 2026-11-15, cross-document G1): its old incarnation may have accepted T and lost
that state undecided, so it replies only with a cached outcome, counted as possibly holding, and otherwise stays
silent. A `Tx` muted only for starting too far ahead
reports itself a possible holder, since once forgotten a retransmission after the clock catches up creates an unmuted
one. The third kind of non-holder `ExpiryTally` counts is an
outcome reply without acceptor state (a status-cache answer) when the asking `Tx` already knows its outcome: a cache
entry exists only for a decided T, so counting it cannot discard an undecided one, and the asking `Tx` concludes the
outcome it knows rather than discarding (`concludeKnownOutcome`), while a finalized or late commit is adopted by rule 1.
(Until 2026-11-15 the rebuilt store counted as a non-holder, and this paragraph claimed that was correct within the
pool's tolerance. It was not: one amnesiac commit voter plus one store that never heard of T is more than `w - t`
non-holders for any pool with `w - t = 1`, and the last holders discarded a chosen commit. HorizonIntegrationSuite
"holders of a chosen commit keep it past expiry while a rebuilt store may have accepted it".) So T is one of: aborted; committed and finalized, in which
case the finalizer wrote this store's UnackedCommits entry before `TxFinalized` if it was owed (the lookup found none,
so it was not; ErrorTree marks cover a store whose update was not counted); or undecided and undecidable, since no
quorum can ever form past the horizon. Discarding is correct in all three. Stuck holders short of a quorum report
themselves honestly as holders and all discard (HorizonIntegrationSuite "stuck holders short of a quorum all
discard"). Tests: FrontendSuite "an abort voter past the horizon whose peers answer unknown is discarded and releases
its locks", "a commit voter past the horizon with no UnackedCommits entry is discarded once a later lookup finds none",
"a commit voter past the horizon with an UnackedCommits entry applies and resolves committed", "a Tx past the horizon
with a quorum of holders keeps holding and later learns the outcome".

**Rejoin invariant.** Once a store discards or forgets T, its floor makes it refuse every later prepare for T: a
discarded acceptor is indistinguishable from an offline one, and Paxos is safe with offline acceptors. **A store never
rejoins a transaction it forgot.** FrontendSuite "after expiry, a later prepare for T creates no Tx".

**Client.** `SimpleClientTransactionDriver` stops retransmitting once its clock is past start + horizon +
`MaxStartLead` and completes `ClientOutcome.Unknown`, failing fast at `begin()` if already that late
(`client/internal/transaction/SimpleClientTransactionDriver.scala:48,73`). `TransactionImpl` fails the commit with
`TransactionOutcomeUnknown` (`client/TransactionError.scala:78`, a sibling of `TransactionAborted`) and records nothing
in the client's status cache (`client/internal/transaction/TransactionImpl.scala:394-396`). `TransactionAborted` is now
final without exception; `StopRetrying.unlessCollision` and the other retry sites retry Unknown as they do an abort
(`client/StopRetrying.scala:26`). HorizonIntegrationSuite "a stale retransmit of a finalized traceless commit past the
horizon reports unknown, not aborted".

**Residual costs.**

* *Post-load refusal.* After a reload a store refuses, for good, every T it does not hold that started before
  `load + MaxStartLead` (`Frontend.loadFloorMillis`), or `load + AcceptorSinceMargin` within that margin of its arrival
  from another host. For Ts started before the load that only anticipates the horizon
  by up to 20 s; the extra cost is the Ts started in `[load, load + MaxStartLead)`, which the store would otherwise
  have joined. Those see it as offline and most commit through the other stores. On a single host, or after a restart of most of a pool's
  stores, too few stores remain and such Ts end `TransactionOutcomeUnknown` at about 22 s, and are retried. A durable
  per-store high-water mark of joined start times is the upgrade path.
* *Bootstrap stores.* Stores written by bootstrap are not exempt from the load floor (only `createNewStore`'s are). A
  store refuses every transaction it does not hold that started before its load + `MaxStartLead`
  (`Frontend.scala:135-139`), so on a fresh system every transaction started within `MaxStartLead` (2 s) of its
  stores' load is refused there; on a single host those all end `TransactionOutcomeUnknown` at about 22 s and are
  retried.
* *Size eviction* can raise the floor to about "now" on its host (eviction is not oldest-first), making its stores
  refuse every not-yet-held transaction started before it. It needs about 130 k transactions/s per host sustained; it
  fails safe and is counted, and logged as an error at most once per 10 s with the count
  (`TransactionStatusCache.EvictionLogPeriod`); the refusal warning is likewise limited to once per 10 s per store
  (`Frontend.RefusalLogPeriod`).
* *Slow client clocks.* A client clock more than 20 s slow has its transactions refused (logged with the lag).
* *Retried unknowns.* An Unknown may have committed, and the retry re-runs its `prepare` against what that attempt
  wrote. Audited and fixed 2026-11-15 (10.11); the residual is listed there.
* *Backward wall-clock steps* beyond about 8 s (above).

### 10.11 Retried unknowns (Todo 5.10, 2026-11-15)

`TransactionOutcomeUnknown` may mean committed. Every `transactUntilSuccessful*` call re-runs its `prepare` with
fresh reads, so after an Unknown that had committed, the retry runs against the state that attempt wrote. Safety is
not at stake: a retry whose reads predate the commit's apply collides with it and re-reads. The question is what the
re-run does with work already done. Every `StopRetrying.unlessCollision` site, `SimpleTaskExecutor.allocateTask` and
every plain `transactUntilSuccessful` site in `src/main` was audited, including the `StoreRebuilds.createNewStore` flip.

**Mechanism.** `StopRetrying.collisionRetry()` returns a `CollisionRetry`: `unlessCollision` that also records whether
an earlier attempt ended Unknown (`mayHaveCommitted`). A site that is not naturally idempotent accepts its own
post-commit state as success only then, and stages nothing, so the empty transaction succeeds. "Its own" is decided
by an id or key fixed outside the retry closure, or by the exact state the call writes. Outside an Unknown the same
state is still an error, so a first attempt that finds the work done by someone else still reports it. Post-commit
announcements that the committed attempt's `tx.result` would have made are made by the retry instead.

| Site | Before | Now |
|---|---|---|
| `HostServices.initializeTaskExecutor` (plain retry) | **Livelock**: the retry allocated a new root under `DoesNotExist(TaskExecutorRootKey)`, which aborted at the stores on every attempt; the host never started its task executor, usage tracking or system task runner | A root already present is used; nothing is staged |
| `BaseAspenClient.failStorageDevice`, `createSystemDurableTask` | **Duplicate**: a second durable task, since `prepareSystemDurableTask` minted a task id per attempt (`failStorageDevice` could also fail with `DeviceAlreadyFailed`) | One task id per call (`prepareSystemDurableTask(taskId = Some(..))`); a registry holding it stages nothing. The registry read is opt-in, so other enrollments pay nothing |
| `SimpleTaskExecutor.allocateTaskNow` | **Duplicate**: an extra empty task object, recorded in the executor object for good | One key per call; `onFail`'s existing re-read finds it and the retry returns that attempt's object |
| `StoreRebuilds.requestRebuild` | **Wrong result**: `false`, so the rebuild waited for the periodic device check | A Rebuilding entry after an Unknown counts as this request's: the device is checked at once |
| `DeviceMetadata.adoptDevice` | **Wrong result**: `Changed`, so the adopted device was not served until the next scan, and the previous host was not nudged | The device owned here after an Unknown is `Adopted(state)`; the nudge is sent |
| `AspenClient.abortStoreTransfer` | **Wrong result**: `false` and no nudges after a rollback | The retry reports the earlier attempt's rollback and nudges |
| `AspenClient.transferStore`, `replaceRebuild` | **False error**: `StoreNotActive`/`InvalidDestination` after the change was applied; nudges lost | One transfer id per call (`transferStore`); the call's own post-state is success, with the nudges |
| `BaseAspenClient` creates (group, device set, host, device, pool) | **False error**: `KeyAlreadyExists`, which reads as a taken name; a pool's host nudges lost | `runIdempotentCreate`: the tree mapping the call's fixed id is success; a pool's nudges are rebuilt from its store entries |
| `DeviceMetadata.updateStateForTransferredStore`, `markRebuiltStoreActive` | **Lost announcements**: `StoreMoved`/`StoreOffline`, the source host's nudge, `TransferComplete`, `gossipStoreOnline` were sent from the committed attempt's failed `tx.result` | The retry that finds the work done sends them (they are idempotent) |

Idempotent as written, verified: `StoreRebuilds.createNewStore` (the flip finds the entry no longer Initializing and
stages nothing), `StoreTransfers.abandonIn`, both usage managers, `completeDeviceShutdown`,
`AllocationGroupState.modifyPool`/`modifyGroup`, `StorageDeviceSetState.moveDevice`, `acceptRebuildLoss`,
`changeDeviceStatus`, `migratePoolToSet`, the system-task and rebalancing registry writes, `deallocateTask`,
`reconcileEndpoint`, `SimpleFileContent`, `NetworkCachingService.write`, `DiskCatalog.grow`.

Tests: CommittedUnknownRetrySuite (one per row above, except adoption and allocation), DeviceAdoptionSuite "an
adoption whose commit is reported Unknown is still brought into service", SimpleTaskExecutorSuite "an allocation
that committed but was reported Unknown is not repeated by its retry". `TestNetwork.reportCommitAsUnknown` has the
stores commit while the client is told Unknown. Each test fails without its fix.

**Residual (accepted).**

* `FailedStorageDeviceDurableTask` `tombstone` and `moveStore`, and `MigratePoolToSetDurableTask.markComplete`, detect
  their own commit by throwing (`DeviceAlreadyFailed`, `StoreAlreadyMoved`, `MigrationSuperseded`), which each step
  already handles. The cost is a misleading log line, a lost best-effort nudge and one extra poll.
* `DiskCatalog.takeOwnership` bumps the fence epoch twice; the owner holds the later epoch, which is harmless.
* `RebalancingDurableService.enroll` could enroll a stale plan again only if the task finished and was removed
  between the attempts.
* The "done" checks accept a state someone else produced, but only after an Unknown: `requestRebuild`
  (a Rebuilding entry), `transferStore` once the transfer has finished (the pool naming the destination),
  `replaceRebuild`, and `abortStoreTransfer` (nothing outstanding). Each then reports success for a change that
  happened, though possibly not by its own hand.
* Durable tasks enrolled by a larger transaction (`UpdateAllocationGroupUsageTask`, `FsifReleaseIndicesTask`,
  `MigratePoolToSetDurableTask`) still mint a task id per attempt. Their callers' own guards (the usage threshold, the
  group membership check, `migration` InProgress) keep a retry from enrolling again.

---

## 11. Failure scenarios

Conventions: "safe" = no split decision, no loss of a decided commit, no applied update for an aborted transaction.
"Live" = the transaction eventually resolves without operator action given a healthy quorum.

### 11.1 Message-level faults

| Fault | Outcome | Why |
|---|---|---|
| Client `TxPrepare` lost to some stores | Safe, live | Client retransmits forever; a store that has seen no client prepare withholds its promise and vote up to 5 s (prefer-abort, 6.2) then votes abort; the tally needs `t` commit votes per object, else abort. |
| `TxPrepare` lost to the leader | Live after delay | No driver is created at the leader; other stores' responses are cached 10 s for a later driver (`Store.scala:91-93,195-202`); the client retransmits; after 5 heartbeats of idleness a recovery driver starts. |
| `TxPrepareResponse` lost | Safe, live | Driver retransmits the prepare (`SimpleTransactionDriver.scala:59-90`); the store re-answers with the same persisted vote. |
| `TxAccept` or `TxAcceptResponse` lost | Safe, live | Retransmitted; the learner needs `t` distinct acceptors. |
| `TxResolved` lost | Safe, live | Driver re-sends `TxResolved` to stores not in `knownResolved` until finalized; otherwise `TxFinalized` carries the outcome. |
| `TxFinalized` lost | Safe, live: the idle store's driver asks within the status cache's life (5-6 s idle < 30 s cache, horizon); past it, its peers refuse the driver and the Tx concludes its outcome or is discarded by expiry (10.10) | `Store.scala:84-88`, `Host.scala:62-72`. Meanwhile the store holds its locks (until expiry at the latest). |
| `TxCommitted` lost | Safe | Finalizer's missed-update logic marks the store for repair, which reconciles exactly. |
| `TransactionResolved`/`Finalized` lost to client | Safe. A retransmit inside the horizon is answered from a `Tx` or the cache; past horizon + `MaxStartLead` the client stops and fails with `TransactionOutcomeUnknown` (horizon; F2 closed) | `SimpleClientTransactionDriver.scala:48,73` |
| Duplicates of any message | Safe | 10.1-10.3. |
| Reordering | Safe | Proposal ids filter stale responses; `TxFinalized` before `TxResolved` is fine (5); a commit decision arriving before the prepare/data is held (9.1). A `TxResolved` arriving before any prepare is only remembered in the cache (`Frontend.scala:763-769`), so the later-created `Tx` learns it only at `TxFinalized`. |
| Message to a store on another host after migration | Safe | `TxUnknownStore`, peer queries (`CLAUDE.md`). |

### 11.2 Slow or partitioned clients and stores

| Case | Outcome |
|---|---|
| Slow client: its prepare reaches a store after a driver's prepare | Promise and vote withheld up to 5 s, data or not (prefer-abort); then abort vote, requirements unchecked. Late data still adopted if the others commit (6.2). |
| Slow client: data reaches a store after the commit decision | Commit held until data/`TxFinalized` (9.1). |
| Client partitioned for longer than the status cache lifetime and then retransmits | (horizon) It stops retransmitting at horizon + `MaxStartLead` and reports `TransactionOutcomeUnknown`; a retransmit that still lands past a store's floor is refused, so no store re-decides T (F2 closed). Before 2026-11-13: with a proof late committed, without one a false **abort**. |
| Client frozen, clock ahead, retransmit after > 7 s | No longer special for replay (F3 fixed). (horizon) A start more than `MaxStartLead` ahead of a store's clock is muted there until the clock catches up; one more than the horizon behind is refused. |
| Slow store (behind) | It votes late or never; the quorum decides without it. If it missed the commit it is marked for repair by the finalizer. |
| Leader partitioned from some stores | Driver cannot reach a quorum -> waits; recovery drivers on other stores start after 5 heartbeats; dueling resolved by Paxos. |
| Minority of acceptors partitioned | Proceeds. A majority-or-more partition: no progress (liveness), no unsafe decision. |
| Secondary pool below `t` reachable, or a silent store whose vote would decide | (2026-11-15, F8) An object already abort-certain decides abort at once. Otherwise the driver waits until T is past the horizon plus the expiry margin, then proposes abort; commit-vote locks are held for at most that long plus one resend interval. |

### 11.3 Crash at each step

"Store crash" means the process or host dies and the store restarts, recovering from its device CRL.

| Crash point | State after restart | Outcome |
|---|---|---|
| Client, before any prepare reaches a store | Nothing exists | Nothing happened. |
| Client, after some prepares | Stores that got its prepare hold Tx; after 5 heartbeats of idleness a recovery driver (round >= 2, data-less) starts. | Commits only if stores that got the client's prepare supply `t` commit votes for every object; the others vote abort after 5 s (prefer-abort), data-less transactions included (P-N8). Either is safe. Client never learns (that is acceptable). |
| Store, before the vote's save completes | No record, or an older record. Nothing was sent. | The store is as if it never saw the prepare; client/driver retransmit; a fresh vote is computed. |
| Store, vote saved but response not sent | Record with disposition and acceptor state; locks re-taken for a commit vote | Retransmitted prepares are answered with the persisted vote (P4/P5). |
| Store, after Accepted saved | Acceptor state recovered (`Tx.scala:76`) | Paxos state intact; P8 holds. |
| Store, after learning commit, before applying | Recovered as Unresolved with its vote and data; commit vote re-locks | Must re-learn: a live driver, status cache, a peer's proof, or the UnackedCommits lookup (`needsUnackedCheck`), which now runs for every recovered commit vote and gates its driver. If every other store finalized and forgot: an entry makes it apply; no entry means it acknowledged (or did not count), and a late commit applies nothing. Without a proof or entry, abort votes may decide abort (F2 residual; closed by the horizon: past the floor no forgotten peer answers, and the Tx ends by expiry, applying T if its entry exists). |
| Store, after in-memory apply, before backend commit durable | Object data still in the CRL (P13); recovered like the previous row | Same. |
| Store, after backend commit, before CRL data drop | Recovered Unresolved; `appliedBy` shows the update | `proveCommitted` on the next prepare or `doCommit` skips as already applied (`Tx.scala:689-694`). |
| Leader/driver, before Accept sent | Driver state lost | Recovery driver after idle timeout; values for round 1 not reused (round >= 2). Since 2026-11-15 only the leader's first driver (its client's prepare creating the Tx) starts at `(1,L)`; a restarted or replacement leader driver starts at round 2 or above what its own acceptor promised (F5). |
| Leader, after quorum accepted, before `TxResolved` sent | Accepted state at `t` acceptors | Recovery driver's prepare quorum intersects the accepted set, so it re-proposes the same value (P8). |
| Leader, after resolution, during FinalizationActions | Stores hold the committed Tx (not finalized), their CRL records kept | Recovery driver: stores still within cache reply `TxResolved`, or answer Promise with their retained accepted commit (P9); it re-runs the finalizer (at-least-once). |
| Driver, after `TxFinalized` partly sent | Some stores finalized | Idle stores recover via their own driver and the cache (11.1). |
| Backend commit failure at a store | Record kept, store votes abort on that object (`BackendCommitFailed`) until reload | `Frontend.scala:868-874`. |
| CRL thread death | Store stalls (no vote can be sent) | Liveness only. |

### 11.4 Concurrent drivers

* Leader plus one or more recovery drivers: all run Paxos with distinct ids (P2 holds for primary stores). The client's
  retransmit at `(1,L)` is Nacked by acceptors that promised `(2,R)`; the Nack goes to the leader's driver, which bumps its
  round (`onNack`). Safe by P8; resolution is announced by whichever driver learns first; others learn via
  `TxResolved`/`TxFinalized`.
* Both resolve and both run FinalizationActions: duplicates are permitted (at-least-once, idempotent). Both send
  `TxFinalized`; stores handle it once (`Tx.scala:925`).
* Two drivers with the **same id**: a secondary-pool store sharing the index can no longer propose (learn-only, F4,
  2026-11-15), and a restarted leader no longer reuses `(1,L)` (F5). Left: a restarted driver of round >= 2 may repeat
  an id its own acceptor never saw (F5 residual).
* A recovery driver racing a `TxResolved` from the cache: the first learned value wins in `onResolution` (P19).

---

## 12. Suspected flaws and open questions

Format: ID, confidence that the described behaviour exists and is a real problem, scenario, code. "Confidence" is
about the finding, not the likelihood of occurrence.

### Re-assessment for Todo 2.13, Paxos hardening (2026-11-15)

| Id | Status | Reason | Evidence |
|---|---|---|---|
| F4 / Recovery F11 | **FIXED** | Only primary-pool stores propose (`TransactionDriver.mayPropose`). A driver on any other store is learn-only: it sends prepares and resolves, proves late and finalizes from what it hears, but sets no proposal and sends no Accept (the retransmit timer included). `designatedLeader` checks the pool too. Its prepares and heartbeats do not count as activity at a Tx (`Tx.fromProposer`), so a primary store still takes an idle transaction over when the leader is unreachable; with no primary store reachable the Tx ends by expiry, which counts only primary-pool answers | TransactionDriverSuite "a driver on a secondary-pool store is learn-only ..."; SimpleTransactionDriverSuite "a learn-only driver's retransmits send prepares, never an Accept ..."; FrontendSuite "prepares and heartbeats from a learn-only driver do not reset ...", "a secondary-pool commit voter whose primary stores answer unknown ..."; LearnOnlyDriverIntegrationSuite |
| F5 | **NARROWED** | Only the designated leader's first driver, the one a client's prepare creating the Tx starts (`Store.driveTransaction(initialProposal = true)`), keeps `(1,L)`. Every other driver starts at round 2 or higher, above the highest id its own acceptor promised or accepted (`TransactionDriver.Start.highestSeen`, `Tx.highestProposal`). Residual: a restarted driver of round >= 2 may repeat an id its own acceptor never saw. Same-id proposers then compute the same value, since the tally is conclusive only on immutable votes (7.2) now that Undetermined is no vote (F7), and a restarted host refuses every T started before its load (forget floor, 10.10) | SimpleTransactionDriverSuite "only the designated leader's first driver starts at the initial proposal", "... starts above what its own acceptor promised or accepted"; StoreSuite "only a client prepare creating the transaction at its leader ...", "a driver started for an idle Tx starts above ..." |
| F6 | **CLOSED** (accepted residual) | Not changed, by decision. A live store keeps its in-memory acceptor state until `TxFinalized` whatever the CRL holds, and a restarted one refuses T (forget floor). Keeping the abort's record until `TxFinalized` would only cover the clock-step residual (a wall clock stepping back by more than about 8 s) between resolution and `TxFinalized`, which for an abort is one message, while the same residual applies to every finalized transaction of either outcome | 10.10 |
| F7 | **FIXED** | `Undetermined` counts neither as a reply nor as an abort vote in the tally. When the votes cannot decide but a promise of the round's quorum carries an accepted value, the driver proposes that value (always safe in Paxos), so stores that answer Undetermined cannot stall a round whose quorum reports the chosen value | TransactionDriverSuite "Undetermined replies are no votes ...", "a quorum reporting an accepted value is proposed while Undetermined replies leave the tally undecided" |
| F11 | **FIXED** for stores and drivers | A conflicting second outcome is logged as an error and counted in the host status cache's `splitDecisions` (`TransactionStatusCache.recordSplitDecision`) at the Tx (`receiveResolved`, `receiveFinalized`) and the driver (`onResolution`, `receiveTxFinalized`, a late commit after an abort). Each keeps its first outcome; a driver that resolved commit ignores an abort `TxFinalized` (Committed wins, Ruling 30). The client logs a conflicting outcome only when it races the first: `TransactionManager` forgets a driver once it has one, so the stores are the detectors | TransactionDriverSuite "a conflicting TxResolved or TxFinalized after a resolution is reported and changes nothing"; FrontendSuite "an outcome opposite to the one a Tx holds is counted as a split decision ..." |

### Re-assessment for the transaction horizon (2026-11-13, `e9e6f3f6`)

Findings the horizon (10.10) touches. Every other finding of this document is unchanged by it.

| Id | Status | Reason | Evidence |
|---|---|---|---|
| F2 | **closed** (FIXED by the horizon) | No store joins a transaction it may have forgotten: a stale retransmit past the floor is refused by every store that forgot T, so no new instance forms, and the client gives up with `TransactionOutcomeUnknown` instead of hearing a false abort. Inside the window the 30 s caches answer. A live commit voter left behind ends by expiry and applies T through its entry if owed | `Frontend.scala:126-139,1003-1046`; `Store.scala:230-232`; `SimpleClientTransactionDriver.scala:48,73`; HorizonIntegrationSuite "a stale retransmit of a finalized traceless commit past the horizon reports unknown, not aborted" |
| F6 | **re-assessed**: no longer a split path; residual stated | An abort still deletes its CRL record at resolution, but a forgotten abort is no longer re-decidable: the store keeps its cache entry past the horizon, and a restart, transfer-in or eviction raises its floor, so it never rejoins T. What remains: the timing relies on each host's own wall clock not stepping back by more than about 8 s (10.10), and the post-load refusal cost. Todo 2.13 F6 (keep the record until `TxFinalized`) is no longer needed for safety | `Frontend.scala:126-139`; `TransactionStatusCache.scala:32,49-51` |
| F10 | **closed by the horizon** (not fixed directly) | `lastProposer` still follows client prepares (`Tx.receivePrepare`), but its safety leg, a proof sent to a proposer without a driver (P-N1), no longer matters (below). The `TxCommitted` redirect to a dead leader still costs an extra ErrorTree mark and repair (harmless, unchanged) | `Tx.scala:909-911` (`receivePrepare`) |
| F11 | **narrowed** | The host status cache logs an error ("Split decision") and counts every opposite outcome it is given (`splitDecisions`); `Tx.resolved`, `onResolution`, `receiveFinalized` and the client still ignore one silently | `TransactionStatusCache.updateStatus`; TransactionStatusCacheSuite "an opposite outcome is logged as a split decision" |
| F15 | **narrowed**: `TxStatusRequest` wired up | Expiry sends it and every store answers (10.10); `receiveTxPrepare` and the other dead `Proposer` methods are unchanged | `Frontend.scala:607-640,985-1001`; `Tx.scala:1259-1265` |
| P-N1 | **closed by the horizon** (not fixed directly) | A proof is still sent once. But a driver without it can decide abort only from a promise quorum of acceptors that forgot T, and inside the window those still answer from their `Tx` or cache entry, while past the floor they refuse. Proofs are now an optimisation inside the window, not load-bearing | 10.10; 10.5 |
| P-N4 | **closed** (FIXED by the horizon) | A delayed duplicate of the client's own prepare finds every store that took part in the abort still remembering it (finalized abort in the cache, relayed) inside the window, and refused past the floor. The `Transaction.commit` scaladoc's accepted exception is removed; `TransactionAborted` is final | HorizonIntegrationSuite "a delayed client prepare of a forgotten abort does not commit inside or past the horizon"; `client/Transaction.scala:150-162` |
| P-N2 | unchanged (FIXED stands) | `MaxStartLead` is now `TransactionHorizon.MaxStartLead`, the same 2 s; the floor's timing argument uses it too | `common/transaction/TransactionHorizon.scala:16` |

### Todo 5.4 (2026-11-15)

| Id | Status | Reason | Evidence |
|---|---|---|---|
| F12 | **fixed** | A later round's first prepare quorum queues its Accept | `TransactionDriver.queueAcceptMessages`; TransactionDriverSuite "after a round change, ..." |
| F14 | **fixed** | Adopted data is saved, and the commit held, until the save is durable | `Tx.adoptedDataUnsaved`, `saveAdoptedData`; FrontendSuite "... only once that data is durable" |
| F15 | **fixed** | Dead code deleted; `maySendAccept` was live and stays | `Proposer.scala`; `ClientTransactionDriver.scala` |

### Re-assessment for replay pins (2026-11-09, `0d08e3c6`)

| Id | Status | Reason | Evidence |
|---|---|---|---|
| F1 | unchanged | Only drivers send `TxAccept`; no round rule | `TransactionDriver.scala:539-551` |
| F2 | **narrowed** | A driver holding a proof never proposes abort and resolves late; a driver with no proof (refcount or overwritten pin, proof lost or sent to another driver, P-N1) still decides abort | `TransactionDriver.scala:310-313,361-362,404-430`; `Tx.scala:630` |
| F3 | **fixed** | The horizon is gone; replay is refused by the pin rule and the late-commit rule, not by clocks | `Frontend.scala:812-831` (no age test); `Pins.scala:18-33`; `TransactionBuilder.scala:140-141` |
| F4 | unchanged | Proposer peer is still the pool index | `TransactionDriver.scala:102` |
| F5 | unchanged | A new driver still starts at `(1,L)` | `Proposer.scala:11`; `SimpleTransactionDriver.scala` |
| F6 | unchanged (reach widened) | Abort still deletes the CRL record at resolution; with no horizon a forgotten abort is re-decidable by any later prepare (P-N4) | `Tx.scala:749`; `Frontend.scala:589-591` |
| F7 | unchanged | Every non-`VoteCommit` counts as abort | `TransactionDriver.scala:332-337` |
| F8 | **fixed** 2026-11-15 | Every object is classified; any abort-certain object decides abort at once; an undecided tally proposes abort past expiry | `TransactionDriver.receiveTxPrepareResponse`; `Store.driveTransaction` (`Start.pastExpiry`) |
| F9 | **fixed** (Ruling 30) | `TransactionStatusCache.updateStatus` is monotone (atomic compute): `finalized` and `late` are sticky and a resolved status never reverts to Unresolved, so neither `Tx.resolved` nor the `Tx` constructor can downgrade an entry (X3) | `TransactionStatusCache.scala` `updateStatus` |
| F10 | unchanged | `lastProposer` still follows client prepares; the proof is sent there too (P-N1) | `Tx.scala:806,630` |
| F11 | unchanged | Conflicting outcomes are still ignored silently; `resolveLate` logs an error only for late-after-abort | `Tx.scala:772`; `TransactionDriver.scala:601-602,691` |
| F12 | unchanged | Rounds >= 2 still wait for the timer (the post-proof round sends prepares at once) | `TransactionDriver.scala:361-364,428-429` |
| F13 | unchanged (now intended) | A prover's abort vote is saved on later prepares by design (Ruling 10); a recovered prover has Paxos state, so it no longer proves | `Tx.scala:228-229,836-838` |
| F14 | unchanged | Adopted data still not re-saved | `Tx.scala:798-802` |
| F15 | unchanged | `receiveTxPrepare` and `TxStatusRequest` still have no caller | `TransactionDriver.scala:277-279`; `Tx.scala:1146-1150` |

### Re-assessment for the replay-pins addendum (2026-11-10, `c181375b`)

Findings the addendum touches. Every other finding of this document is unchanged by it.

| Id | Status | Reason | Evidence |
|---|---|---|---|
| F2 | unchanged | A store that forgot a committed T no longer checks requirements for a driver's prepare, but its answer is the same: a proof if an object shows T, otherwise abort (now after up to 5 s, without the requirement check). The false abort without a proof stands | `Tx.scala:205-208,537-542,584-586` |
| F6 | **narrowed** | A forgotten abort re-decided by a later round needs fresh commit votes, and only a client prepare can produce one now; a driver-only round can only re-decide abort | `Tx.scala:537-542`; 6.2 |
| F7 | unchanged | Undetermined answers still count as aborts | `TransactionDriver.scala:332-337` |
| F14 | unchanged | Adopted data still not re-saved; the commit's own data wait is unchanged | `Tx.scala:239,798-802` |
| P-N1 | **narrowed** | A store reached first by a driver's prepare now proves to *that* driver as soon as its objects load (no abort vote goes out beside the proof), so a recovery driver whose prepare creates a store's Tx holds the proof. A second driver, or a proof that reaches a store with no driver yet, is still lost; the latter is now reproduced (Task 4) | `Tx.scala:806,208,584-586,630`; `Store.scala:208` |
| P-N3 | **narrowed** (2026-11-14, Todo 2.9: the blind writes at in-time ackers are closed by the horizon; left is a re-adoption inside the horizon, whose cost is the orphan allocation entry, Todo 5.5) | A store holding an applied stub applies nothing on the non-late commit and reports its objects, so the finalizer re-marks it; stores without a stub (they acknowledged in time) still apply a concurrently replayed client prepare's blind writes. Ruling 3 added a second source of non-late commits for a forgotten T (P-N10); Ruling 7 removed it | `Tx.scala:642-648` |
| P-N4 | **narrowed** to path 3 | Recovery and other store-side drivers can no longer re-decide a forgotten abort commit (prefer-abort, 6.2; tested by ReplayIntegrationSuite "an aborted data-less transaction re-driven by a recovered commit vote aborts again"). A delayed duplicate of the client's own prepare still can: the accepted corner case, documented in the `Transaction.commit` scaladoc | `Tx.scala:205-208,537-542`; `client/Transaction.scala:156-161` |
| P-N5, P-N6 | unchanged (REFUTED stands) | | |

### F1. Documented 1-RTT protocol is not what the code does. Confidence: high (informational, wrong docs)

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only.** Only TransactionDriver.sendAcceptMessages constructs TxAccept (TransactionDriver.scala:539-548; grep finds no other constructor). Stores answer a prepare with Promise+vote addressed to the prepare's sender (Tx.respondToPrepare, Tx.scala:857-886, `TxPrepareResponse(m.from,...)`) after a CRL save; the driver sets its local proposal only after tallying (TransactionDriver.scala:298-360) and no round check exists in Acceptor (Acceptor.scala:24-35). Error-free path: client TxPrepare -> store (CRL write #1: vote+promise, Tx.scala:879-882) -> leader -> TxAccept -> acceptors (CRL write #2, Tx.scala:888-903) -> TxAcceptResponse -> leader -> TransactionResolved -> client = 5 one-way hops (about 2.5 RTT) and 2 serial durable writes before the client learns; stores learn at a 6th hop (TxResolved). Aborts are decided in round 1; there is no commit-only round 1 and no self-generated Accept. Todo.txt:11-14 at c3ad5493 and Architecture.md describe a protocol the code does not implement (no partial 1RTT).
> *Fix sketch:* Fix the docs (Todo.txt, Architecture.md) now; if 1RTT is wanted, have stores self-accept commit in round 1 and restrict round >=2 to abort-or-recover, then re-derive P2/P6.


`Architecture.md:139-155` and `Todo.txt:11-14` at `c3ad5493` describe self-generated round-1 Accepts, a commit-only round 1 and
abort-requires-round-2. The code routes all promises to one leader, which sends the only Accepts (`Tx.scala:873`,
`TransactionDriver.scala:545`); there is no round restriction (`Acceptor.scala:24-35`). Hence: 5 one-way hops and two
serial durable writes before the client learns; aborts are decided in round 1; recovery can commit. Anyone reasoning
from the documented rule (for instance, "any accept in round >= 2 is an abort") will reach wrong conclusions.
Recommendation: fix the docs or implement the fast path; if implemented, P2/P6 must be re-derived (acceptors would accept
commit without a leader tally).

### F2. A stale prepare after the cache expires can produce a false abort of a committed transaction. Confidence: medium-high

> **Refresh (horizon, 2026-11-13): CLOSED.** No store joins a transaction it may have forgotten (the forget floor,
> 10.10, `Frontend.scala:126-139,1003-1046`), so the stale prepare past the cache creates no `Tx` anywhere T was
> forgotten and no new instance can form. The client stops at horizon + `MaxStartLead` and fails the commit with
> `TransactionOutcomeUnknown`, an honest "may have committed", instead of a false abort. Consequence (c), a live
> commit voter dropping its update, is closed too: it ends by expiry, and applies T through its UnackedCommits entry
> if it was owed (transaction-recovery F3). Tested end to end by HorizonIntegrationSuite "a stale retransmit of a
> finalized traceless commit past the horizon reports unknown, not aborted". No residual of the split itself; the cost
> moves to callers that retry an Unknown that had committed (audited and fixed 2026-11-15, 10.11).

> **Refresh (replay pins): NARROWED.** The stale prepare is now checked normally (no `TooOld`). A store showing T's
> update proves it; the driver that receives the proof never proposes abort (`TransactionDriver.scala:361-362`), runs a
> post-proof round, and resolves *late* on a memoryless quorum, telling the client committed and applying nothing
> (`:599-612`). The false abort remains wherever no driver holds a proof: the pin is a refcount change or was
> overwritten, the proof was sent once to a `lastProposer` with no driver (P-N1), or a second driver without the proof
> tallies first. Consequence (c) is also narrowed: a live commit voter that learns a *late* commit looks up its entry and
> applies it (`Tx.scala:1001-1005`); one that learns an abort still drops it (transaction-recovery F3).

> **Verification 2026-11-09: CONFIRMED (mechanism); outcome damage narrower than claimed. Severity: split-decision (two outcomes for one txid; no applied store is rolled back).** Acceptor state is NOT retained past finalization: a committed Tx keeps its Paxos record until TxFinalized (Tx.scala:949-960 -> Frontend.transactionFinalized, Frontend.scala:664-702), an aborted one is deleted at resolution (Tx.scala:749 -> deleteTransactionState, Frontend.scala:589). After TxFinalized every data store has dropped Tx+acceptor, the leader's driver is removed (Store.scala:216-218) and the host cache forgets after 7 s, so a stale client retransmit (client retransmits until it hears: SimpleClientTransactionDriver.scala:30-50) at t > ~10 s starts a NEW consensus instance: Store.handleTransactionMessage finds no cache entry and, at the leader, driveTransaction (Store.scala:158-193); every store builds a fresh Tx with tooOld=true (Frontend.scala:651-658 at c3ad5493) which proves commit only if an object still shows the update (appliedBy: revision of DataUpdate/VersionBump/KV-with-revision, or a key carrying the txid) and otherwise votes VoteAbort(TooOld) (Tx.scala:444-449 at c3ad5493, 582-586). For a traceless or since-overwritten change all votes are abort, the tally aborts (TransactionDriver.scala:351-358), empty acceptors accept, and the driver sends TxResolved(false)/TxFinalized(false) plus TransactionResolved/Finalized(false) to the client (:690-726). Consequences: (a) client told ABORT for a committed tx (real, client-visible); (b) cache says Aborted for 7 s; (c) a live store still holding the commit vote drops it (Tx.receiveResolved Tx.scala:905-919 -> resolved(false)). NOT possible: rolling back stores that already applied (a fresh Tx resolves false and only deleteTransactionState runs; nothing undoes an applied commit), and stores whose Tx is already resolved(true) ignore the second outcome (Tx.resolved guards oresolution.isEmpty, Tx.scala:772). (c) is repaired because the original finalizer marks every store that did not send TxCommitted within errorTimeout (MissedUpdateFinalizationAction.scala:88-138). So it is a split of DECISIONS, not of store state, and it needs a client partitioned/paused > ~horizon (6 s) whose reply was lost.
> *Fix sketch:* Do not let a TooOld-only tally resolve a transaction: report 'unknown' to the client (no TxResolved/TransactionResolved) and stop; or persist a small 'finalized(txid, outcome)' tombstone for longer than any client retransmit horizon.


Scenario: transaction T commits and finalizes at t0 (stores forget T; caches hold it 7 s). The reply to the client is
lost or the client is partitioned. At t0+20 s the client's retransmit reaches the designated leader L
(`ClientTransactionDriver.scala:65-84`). `Store.handleTransactionMessage` finds no cache entry and starts a driver
(`Store.scala:191-193`). Every store creates a fresh `Tx`, finds `tooOld` (age > 6 s), and votes `VoteAbort(TooOld)`
unless an object still shows T's revision (`Tx.scala:444-449` at `c3ad5493`, `:584-586`). If T's change is traceless, or the object has
been overwritten by later transactions, there is no proof; all stores answer abort; L's driver tallies `w - a < t`
(`TransactionDriver.scala:351`), proposes abort, all acceptors have empty state and accept, and L sends
`TransactionResolved(false)` and `TxResolved/TxFinalized(false)` (`:690-726`). Effects: (a) the client is told T
aborted although it committed (retry of a non-idempotent operation may double-apply; contradicts the client-visible
atomicity guarantee); (b) the status cache on every host now says `Aborted` for 7 s; (c) a store that is still holding T
as a live unresolved commit vote receives `TxFinalized(false)`/`TxResolved(false)` and drops the committed update
(`Tx.scala:918-919,925-947`). Repair is expected to restore (c) through the finalizer's ErrorTree/UnackedCommits
entries, but only if the finalizer actually recorded that store, which depends on the original driver having completed.
The design note acknowledges the cost for *legitimate late* prepares (`traceless-commits-design.md` section 6) but not
this case where the transaction already committed. Open question: should a `TooOld` abort be allowed to *resolve* a
transaction at all, rather than merely vote? An alternative is that a driver that sees only `TooOld` votes reports
"unknown" to the client.

### F3. The horizon trusts the client's clock; a client clock ahead of the hosts disables replay protection. Confidence: medium

> **Refresh (replay pins): FIXED.** The horizon and `TransactionTooOld` are deleted (`Frontend.scala:812-831` has no age
> test; grep finds no `txHorizon`/`tooOld`). A replayed blind write cannot re-commit through fresh votes, because its
> transaction must carry a pin (`TransactionBuilder.scala:140-141`) that fails at more than `w - t` stores of the pinned
> object. Residual clock dependence moved to the rebuilt store's mute (`acceptorSince`, `Frontend.scala:817`; P-N2).
> *2026-11-13:* the verification's fix sketch is now implemented for P-N2. A store mutes an unknown transaction whose
> start leads its host's wall clock by more than `Frontend.MaxStartLead` (2 s), which caps client-ahead skew D at 2 s
> plus host skew.

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: resurrection (replay of blind writes).** tooOld = known.isEmpty && HLCTimestamp.now - txd.startTimestamp > horizon (Frontend.scala:655 at c3ad5493); startTimestamp is client-assigned and nothing on the server calls HLCTimestamp.update from received prepares (grep in server/ finds no such call), so a host's HLC only follows its own wall clock. A client whose clock/HLC is ahead by D makes a replay of age a look like a-D; ages in (7 s, 6 s + D) are treated as young. A replayed prepare then creates a fresh Tx that rechecks requirements; blind KV Insert/Delete have no state-dependent requirement (RequirementsChecker.scala:127-162), so t stores vote commit and the old write is re-applied. Client HLC ratchets forward from every object timestamp it reads (BaseReadDriver.scala:193-194), so ahead-skew needs no misconfigured NTP, only a faster clock elsewhere in the cluster.
> *Fix sketch:* Bound client timestamps at the receiving host (reject/clamp startTimestamp > hostNow + small epsilon), and also record the host-local receipt time of the first prepare it ever saw for a txid.


`tooOld` compares host `HLCTimestamp.now` with `txd.startTimestamp`, which the client set (`TransactionBuilder.scala:98`;
`Frontend.scala:655` at `c3ad5493`). The cache covers only 7 s. If the client's clock (or its HLC, which ratchets forward from any
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

> **Refresh (Todo 2.13, 2026-11-15): FIXED.** Drivers on secondary-pool stores are learn-only (`TransactionDriver.mayPropose`):
> no proposal, no Accept. Their prepares and heartbeats do not reset a Tx's idle time, so a primary store takes an idle
> transaction over. See the 2026-11-15 re-assessment table.

> **Verification 2026-11-09: PLAUSIBLE. Severity: split-decision (only if proposers can compute different values).** Structure confirmed: designatedLeader compares only the pool index (TransactionDriver.scala:155) while Store.handleTransactionMessage checks the pool (Store.scala:191), Store.heartbeat starts a driver for ANY idle Tx (Store.scala:84-88), Proposer peerId = storeId.poolIndex (TransactionDriver.scala:102), and a secondary-pool store with the leader's index skips nextRound (SimpleTransactionDriver.scala:48) so it starts at the same (1,L). Acceptor accepts an equal id with any value (Acceptor.scala:31). But the tally decides only when conclusive (commit when every object has >= writeThreshold commit votes, abort when some object cannot reach it; TransactionDriver.scala:319-360) and votes are immutable, so same-id proposers compute the same value; a real split needs the non-vote inputs (F7 Undetermined, F2/F6).
> *Fix sketch:* Restrict driving to primary-pool stores, or make the proposer id the primary-pool index only.


`Store.heartbeat` and `SimpleDriverRecoveryMixin` call `driveTransaction` for *any* idle `Tx` on any store
(`Store.scala:84-88`, `SimpleDriverRecoveryMixin.scala:22-26`); only the designated-leader path checks the pool
(`Store.scala:191`). `TransactionDriver.designatedLeader` compares only the index (`:155`) and the proposer id byte is
`storeId.poolIndex` (`:102`), so a secondary-pool store with index equal to the leader's index starts at the **same**
`(1, L)` as the real leader and the client's prepare, and recovery drivers on `P_i` and `Q_i` both use `(2, i)`. Two
different proposers with one id violate P2; `Acceptor.receiveAccept` accepts an equal id with a different value
(`Acceptor.scala:31`). A split needs the two drivers to tally different values; the tally is monotone in immutable votes
(7.2) so differing values need the non-vote paths (F7, F2/F6). Fix: restrict drivers to primary-pool stores, or
include the pool in the id (for instance the leader's primary index only).

### F5. A restarted leader reuses `(1,L)`; old in-flight Accepts can flip acceptors. Confidence: low-medium

> **Refresh (Todo 2.13, 2026-11-15): NARROWED.** Only the leader's first driver keeps `(1,L)`; any other starts at round
> 2 or higher, above its own acceptor's highest id. Residual: a restarted round->=2 driver may repeat an id its acceptor
> never saw, safe while same-id proposers tally the same immutable votes (F7 fixed). See the 2026-11-15 table.

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

> **Refresh (Todo 2.13, 2026-11-15): CLOSED as an accepted residual.** The record matters only across a restart, where
> the forget floor already refuses T. Keeping it until `TxFinalized` would narrow the clock-step residual to the one
> message between an abort's resolution and its `TxFinalized`, while the same residual covers every finalized
> transaction. Not worth the extra recovery path.

> **Refresh (horizon, 2026-11-13): RE-ASSESSED; no longer reachable as a split.** The record is still deleted at abort
> resolution, but a forgotten abort can no longer be re-decided: the store's cache entry outlasts the window in which
> it would join T, and the restart variant this finding needed (a store restart wiping the cache) now raises the
> store's forget floor past every T it may have joined (load + `MaxStartLead`), as do a transfer-in and a size eviction
> (10.10). So no promise quorum of memoryless acceptors can form for T. What remains is the clock the floor relies on:
> each host's own wall clock, which must not step backward by more than about 8 s between a store joining T and its
> entry expiring; and the floor's cost (post-load refusal). Keeping the abort record until `TxFinalized` (Todo 2.13 F6)
> would no longer buy safety, only remove the reliance on the cache for live stores.

> **Refresh (addendum): NARROWED.** The fresh votes a memoryless promise quorum would tally come only from stores that
> saw a client prepare: a store reached by driver prepares alone never checks its requirements and votes abort
> (`Tx.scala:205-208,537-542`, prefer-abort). The record is still deleted at abort resolution; what remains is a client
> prepare (a retransmit of a client that never learned the abort, or a delayed duplicate, P-N4) reaching stores that
> forgot T.

> **Verification 2026-11-09: PLAUSIBLE. Severity: split-decision (rare).** Abort resolution deletes the CRL record including `accepted` (Tx.scala:749, Frontend.scala:589), but the in-memory Tx (with its Acceptor) stays in Frontend.transactions until TxFinalized (Frontend.scala handleTransactionMessage TxFinalized), so a later round at a LIVE store still sees the accepted value; the loss needs a store restart (which also loses the host status cache) or a Tx dropped by close/recovery, plus a promise quorum made only of such stores and a young/skewed prepare so TooOld does not apply. Real but needs several coincidences.
> *Fix sketch:* Keep the Paxos record until TxFinalized for aborts too (as for commits).


`Tx.scala:746-749` deletes the CRL record (including `accepted`) at abort resolution, and at `TxFinalized` all Paxos
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

> **Refresh (Todo 2.13, 2026-11-15): FIXED.** Undetermined is no vote in the tally, and a round whose votes cannot
> decide proposes an accepted value its promise quorum reports.

> **Verification 2026-11-09: CONFIRMED. Severity: liveness/hardening (safe by P7 while some acceptor holds the chosen value).** TransactionDriver.scala:332-335: every non-VoteCommit disposition, including Undetermined, is counted as an abort vote. Tx.answerPrepare/respondToPrepare answer with disposition as-is once oresolution is set (Tx.scala:835-886 region; determineDisposition is skipped when oresolution.nonEmpty, :526-553).
> *Fix sketch:* Count Undetermined as no vote.


`TransactionDriver.scala:332-335` treats every non-`VoteCommit` as an abort vote. A `Tx` that was resolved before it
voted (data-less prepare first, then `TxResolved`, then a driver's prepare) answers with `Undetermined`
(`Tx.scala:844` skips `determineDisposition` once `oresolution` is set; `respondToPrepare` sends `disposition`
as-is). A later-round driver may therefore see up to `a` spurious aborts and, with no accepted value in its promise
quorum, propose abort even though `t` real commit votes exist. Safe by P7 only while some acceptor still holds a chosen
value; combined with F6 it widens the amnesia window. Fix: count `Undetermined` as "no vote" (do not count it toward
`n`).

### F8. The tally cannot decide an abort while any referenced pool has fewer than `t` answers. Confidence: medium (liveness)

> **FIXED 2026-11-15 (Todo 4.1, with rebuild-process F6).** The tally classifies every object (no `break()`): one
> abort-certain object (`w - a < t`) decides abort at once, since votes never change. A tally that is undecided because
> stores have not voted waits, as a transient partition or restart may heal, until T is past the horizon plus the
> expiry margin on the driver's store (`TransactionDriver.Start.pastExpiry`, `Frontend.pastExpiry`); it then proposes
> abort, unless the promise quorum reports an accepted value (P7a). The horizon alone did not bound the wait: expiry
> discards only with more than `w - t` non-holders, so a commit voter and an abort voter that both hold acceptor state
> `Hold` forever, and a silent store returning past the horizon refuses the prepare without a reply. Tests:
> TransactionDriverSuite "an object that can no longer commit decides abort though another object is short of
> answers", "an undecided tally waits until past expiry, then proposes abort", "past expiry, a quorum reporting an
> accepted commit is proposed, not abort"; StoreSuite "a commit voter whose peer voted abort and whose third store is
> silent aborts past expiry, not before". Residual: locks wait up to horizon + expiry margin (about 31 s) plus one
> driver resend interval while a silent store's vote would decide.

> **Verification 2026-11-09: CONFIRMED. Severity: liveness.** The object loop iterates a Set and `break()`s with cantAttempt on the first object whose pool has fewer than writeThreshold replies (TransactionDriver.scala:341-344) even if an earlier or later object is already abort-certain; with one pool down, voters on the other objects keep their locks and the client retransmits forever.
> *Fix sketch:* Evaluate all objects first; decide abort as soon as any object cannot reach commit; only wait when no object is abort-certain.


`TransactionDriver.scala:341-344`: `break()` out of the object loop with `cantAttempt` on the first (set-ordered)
object whose pool has `n < t`, even if another object's pool already makes commit impossible. A transaction touching an
object in a pool that is down (or whose stores are all withholding votes) neither commits nor aborts; stores that voted
commit on the other objects keep their locks; the client retransmits forever (`SimpleClientTransactionDriver.scala:37-50`).
The `TODO` at `:335` acknowledges the tally could be smarter. Fix: evaluate abort-certain objects first and decide abort
as soon as any object cannot reach `t` commits.

### F9. The host-wide status cache is written by every store and by constructors. Confidence: medium, low impact

> **Refresh (replay pins): NARROWED, impact raised.** `Frontend`'s no-Tx `TxResolved` path now skips a finalized entry
> (`Frontend.scala:636-637` at `0d08e3c6`), but `Tx.resolved` still writes `(status, finalized = false)` over another store's finalized
> entry (`Tx.scala:787-788`). Finalized entries are now late-commit evidence (`Store.scala:167-180`): a downgrade makes
> the relay answer a non-late `TxResolved`, so a replay's driver runs the finalization actions again and a fresh Tx at
> that store applies a blind write again (X3).
>
> **Refresh (Ruling 30): FIXED.** `updateStatus` is now monotone by an atomic compute on the cache's map: `finalized`
> and `late` are sticky and a resolved status never reverts to Unresolved. Every write site (the `Tx` constructor,
> `Tx.resolved`, `Tx.finalized`, `applyNothing`, Frontend's no-Tx paths) goes through it, so no store's write can
> downgrade another's. Frontend's two workarounds (the finalized check on the no-Tx `TxResolved` path, and the restore
> after a new Tx) were removed as redundant.

> **Refresh (post-addendum): opposite outcomes.** The monotone merge ORed `finalized`/`late` across opposite statuses,
> reachable through split decisions (paxos F2, P-N4): a stray abort over a finalized late commit became a finalized
> abort that the relay spread, and a commit over a finalized abort became a finalized late commit whose drivers skip
> the finalization actions. Now Committed wins (an abort never replaces a commit) and a commit replacing an abort
> takes only its own flags (TransactionStatusCacheSuite "a false abort never replaces a finalized late commit", "a
> commit replacing a finalized abort takes none of the abort's flags").

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica / hygiene.** TransactionStatusCache.updateStatus is an unconditional put (TransactionStatusCache.scala:64-66 at c3ad5493); Tx's constructor writes Unresolved (Tx.scala:162) and resolved() writes finalized=false (Tx.scala:788) over an entry another store of the host may have marked finalized (Tx.scala:960). Frontend.receivePrepare restores status for new Txs only (Frontend.scala:660-664 at c3ad5493).
> *Fix sketch:* Make updateStatus monotone (never downgrade finalized or a known outcome to Unresolved).


`Tx` construction overwrites the entry with `Unresolved` (`Tx.scala:162`), including for a recovered `Tx` of another store
on the same host; `resolved` writes `finalized=false` (`:788`) even over an entry another store marked finalized
(`:960`). `Frontend.receivePrepare` restores the status for the new-Tx case but not the finalized flag, and not for
recovered `Tx`s. Effect: a transaction finalized by store A can lose its "finalized" mark on that host while B finishes
late, so A may recreate a young `Tx` from a stray prepare and wait for a `TxFinalized` no live driver will re-send,
until its idle recovery fires or the entry expires. Mostly hygiene; it also widens the F6 window.

### F10. `lastProposer` is overwritten by client retransmits. Confidence: medium, low impact

> **Refresh (horizon, 2026-11-13): CLOSED BY THE HORIZON (not fixed directly).** The overwrite is unchanged. Its
> safety consequence was a proof sent to a proposer with no driver (P-N1), which no longer lets any driver decide a
> false abort (P-N1's refresh). The remaining effect is the one below: a redirected `TxCommitted` makes the finalizer
> mark a store that did apply (extra repair, harmless).

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica (extra repair only).** Tx.receivePrepare sets lastProposer = m.from for every prepare, client prepares included (Tx.scala:806), and TxCommitted/proven TxResolved go to lastProposer (Tx.scala:733, 621-633). After resolution drivers send TxResolved, not prepares, so the redirect is not corrected.
> *Fix sketch:* Do not update lastProposer from fromClient prepares.


`Tx.receivePrepare` sets `lastProposer = m.from` for every prepare (`Tx.scala:806`), and client prepares carry the
leader's id. `TxCommitted` and the proven-commit `TxResolved` go to `lastProposer` (`:733`, `:633`). If the leader is dead and
a recovery driver R is running, a client retransmit after R's prepare redirects this store's later `TxCommitted`
to the dead leader. R's finalizer then sees the store as not having committed, and
`MissedUpdateFinalizationAction` ErrorTree and UnackedCommits entries are written for a store that did apply. Harmless to correctness
(repair reconciles exactly) but it adds repair load and can delay finalization.

### F11. Split decisions are undetectable. Confidence: high (hardening)

> **Refresh (Todo 2.13, 2026-11-15): FIXED for stores and drivers.** Tx and driver log and count a conflicting outcome
> (`TransactionStatusCache.recordSplitDecision`, `splitDecisions`) and keep the first. The client detects only racing
> duplicates (it forgets a transaction on its first outcome).

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only (observability).** Tx.resolved (Tx.scala:772), onResolution (TransactionDriver.scala:690-693) and Tx.receiveFinalized (:925-930) silently ignore a conflicting second outcome.
> *Fix sketch:* Log error plus a counter on any conflicting TxResolved/TxFinalized.


`Tx.resolved`, `onResolution`, `receiveFinalized` and the client each ignore a conflicting second outcome silently
(P19). If any of F4-F6 ever produced two outcomes, nothing would log or alarm. Recommend an `error` log on a
conflicting `TxResolved`/`TxFinalized` at every site and a counter.

### F12. After a round change, Accept relies on the timer; `localProposal` is sticky. Confidence: high, latency only

> **FIXED 2026-11-15 (Todo 5.4).** Reproduced with a timer-less driver: after a Nack the round-2 quorum sent no Accept.
> The first prepare quorum of every round now queues its Accept (`TransactionDriver.queueAcceptMessages`,
> `acceptQueuedThisRound`, reset in `nextRound`); later promises of the round queue no second batch. The sticky local
> proposal is documented at the new branch (`Proposer.proposalValue` still prefers an accepted value the quorum
> reports). TransactionDriverSuite "after a round change, the new round's prepare quorum sends its Accept at once, and
> only once".

> **Verification 2026-11-09: CONFIRMED. Severity: liveness (latency).** TransactionDriver.scala:314 `!proposer.haveLocalProposal` leaves shouldSendAcceptMessages false in rounds >= 2; SimpleTransactionDriver.sendPeerMessages sends the Accept on the next timer (:59-90).
> *Fix sketch:* Set shouldSendAcceptMessages when the new round's quorum is reached.


`TransactionDriver.scala:314` skips the quorum handling when a local proposal already exists, so
`shouldSendAcceptMessages` is not set in rounds >= 2; the Accept goes out at the next periodic send (up to `maxDelay`)
(`SimpleTransactionDriver.scala:59-90`). The `NoRecovery` driver used in some tests has no timer. Also the
local proposal from round 1 is reused without re-tallying (`Proposer.scala:54`); safe because votes are immutable, but
the invariant is implicit.

### F13. `proveCommitted` leaves an in-memory `VoteAbort` that a later save can persist. Confidence: low

> **Verification 2026-11-09: CONFIRMED (mechanism). Severity: doc-only/low.** decideDisposition sets disposition = VoteAbort before proveCommitted (Tx.scala:582-586); answerPrepare then returns without respondToPrepare, but receiveAccept saves the in-memory disposition (Tx.scala:888-903).
> *Fix sketch:* Set disposition only after the proof check, or reset it to Undetermined in proveCommitted.


`decideDisposition` assigns `disposition = VoteAbort` before it calls `proveCommitted` (`Tx.scala:582-586`). That Tx
never answers with it, but `receiveAccept` (`:896-897`) and a later prepare save `disposition` as it stands, so a restart
can recover a `VoteAbort` on a committed transaction (status `Committed`). It would be sent as an abort vote to a later
driver. Practically masked by the proof, but P4's exception.

### F14. Adopted late object data is not persisted until the next save. Confidence: medium, low impact

> **FIXED 2026-11-15 (Todo 5.4).** Reproduced: a commit held for data was applied on the late client prepare with no
> save holding the data, and an abort voter's later commit applied before the prepare's save was durable.
> `adoptObjectUpdates` now sets `adoptedDataUnsaved` (not for a muted Tx, which applies nothing) and saves the state at
> once if the objects have loaded, otherwise at `completeLoad` ahead of any outcome learned meanwhile.
> `commitAwaitsData` includes the flag, so the commit waits for the save, whose completion (through `frontend.post`)
> clears it and releases the held commit. Only the latest save clears it. Late-data path only, no hot-path cost.
> FrontendSuite "a commit held for data applies the late client data only once a save holding that data is durable",
> "an abort voter given late client data applies a later commit only once that data is durable".

> **Verification 2026-11-09: CONFIRMED. Severity: stale-replica (repaired).** adoptObjectUpdates mutates objectUpdates in memory (Tx.scala:798-802) and no CRL save follows.
> *Fix sketch:* Save the Tx when new object data is adopted.


`adoptObjectUpdates` (`Tx.scala:798-802`) changes `objectUpdates` in memory only. A store that had voted abort for
missing data, adopted data, applied a commit decided by the others, and crashes before its backend commit completes
recovers without the data (`TransactionRecoveryState` lacked it). It then reports an uncommittable object and repair
restores it. Safe by repair; noted because P13 ("data lives in the CRL until the backend commit succeeds") does not hold
for adopted data.

### F15. Dead or unused protocol code. Confidence: high (informational)

> **FIXED 2026-11-15 (Todo 5.4).** Deleted `TransactionDriver.receiveTxPrepare`, `Proposer.receiveNack`,
> `nacksReceived`/`numNacks` and `currentPrepareMessage`, and the commented client learner and `receive` block in
> `ClientTransactionDriver`. `Proposer.maySendAccept` was not dead: the tally uses it to propose a quorum's accepted
> value (F7), so it stays.

> **Refresh (horizon, 2026-11-13): NARROWED; `TxStatusRequest` is wired up.** Expiry sends it to every other store of
> T, it now carries `startTimestamp`, and every store answers: with a `Tx` (`holdsAcceptorState = !muted || !mutedForGood`), from a
> cache entry, "unknown" below its floor, or nothing (10.10; `Frontend.scala:607-640,985-1001`,
> `Tx.scala:1259-1265`). `receiveTxPrepare`, `Proposer.receiveNack`/`maySendAccept`/`currentPrepareMessage` and the
> commented client learner remain dead.

> **Verification 2026-11-09: CONFIRMED. Severity: doc-only.** No caller of TransactionDriver.receiveTxPrepare, no constructor of TxStatusRequest outside codec/tests (grep).
> *Fix sketch:* Delete or wire in.


`TransactionDriver.receiveTxPrepare` (never called), `Proposer.receiveNack`/`maySendAccept`/`currentPrepareMessage`, the
whole `TxStatusRequest` path (`Tx.scala:1146-1150`; no sender), and the commented client learner
(`ClientTransactionDriver.scala:45-59`). The unused `receiveTxPrepare` means a new round's id is learned only through
Nacks (extra rounds under contention).

### New findings from the replay-pins refresh (2026-11-09)

#### P-N1. A proof is sent once, to `lastProposer`; a driver that did not receive it may decide abort. Confidence: high (mechanism)

> **Verdict: CONFIRMED (mechanism); trigger PLAUSIBLE. Severity: split-decision (client-visible false abort, as F2).**
> `proveCommitted` sends one proof to `lastProposer` (`Tx.scala:621-633`); a non-muted prover answers every later
> prepare as an acceptor with its abort vote (`:836-838`). The proof reaches a driver only if one exists on that store
> when it arrives (`Store.scala:208-210`); stores ignore proofs (`Tx.scala:908`, `Frontend.scala:768`). Any other driver,
> or a later one on the same store, has `holdsProof = false` and may set an abort proposal (`TransactionDriver.scala:361-362`).
> Paths: the replayed client prepare names the designated leader, whose store has no driver (down, or its copy of the
> prepare lost; F10 redirects `lastProposer` the same way); or a recovery driver tallies before the proof holder. The
> consequence is F2's: no store state changes (forgotten stores apply nothing for an abort), the client may be told
> aborted, and a live commit voter drops its update and relies on its ErrorTree mark.
> *Fix sketch:* have a prover re-send the proof to every new proposer id it sees, not only the first.
>
> **Refresh (addendum): NARROWED; the dropped-proof trigger is now CONFIRMED (reproduced).** Prefer-abort changes who
> gets the proof: a Tx created by a driver's prepare withholds its answer until its objects load and then proves to
> `lastProposer`, the sender of the latest prepare (`Tx.scala:806`, `:208`, `:584-586`, `:630`), with no abort vote
> beside it, so a recovery driver whose prepare reaches a forgotten store first holds that store's proof. A second
> driver still gets the prover's abort vote (`answerPrepareAsAcceptor`, `:836-838`). The other path is now concrete:
> the replayed client prepare names the designated leader as `from`, a prover in another pool can load and prove before
> the leader's own copy of the prepare has created its driver, and `Store` hands a `TxResolved` only to an existing
> driver (`Store.scala:208-210`; it caches prepare responses, not proofs). Task 4 hit exactly this while building
> ReplayIntegrationSuite's headline test with the default leader-last delivery: the replay resolved abort
> (`task-4-report.md`, concern 4). The consequence is unchanged (no store state changes; the client may be told aborted).
> *Fix sketch (additional):* cache proofs in `prepareResponseCache` beside prepare responses, as `driveTransaction`
> already replays the cache into a new driver.
>
> **Refresh (horizon, 2026-11-13): CLOSED BY THE HORIZON (not fixed directly).** The proof is still sent once, and the
> fix sketches (Todo 1.3a) are dropped, not implemented. A driver without the proof could decide abort only from a
> promise quorum of acceptors that forgot T. Inside the window every store that knew T still answers from its `Tx` or
> its status-cache entry (the relay), and past its forget floor it refuses the prepare (10.10), so no such quorum
> forms. A dropped proof now costs at most latency inside the window.

#### P-N2. Late condition 1 relies on rebuilt acceptors being muted, and the mute relies on clocks. Confidence: low-medium

> **FIXED (2026-11-13); residual: host clock skew beyond 8 s, and zombie old copies (X1, SL M9).** The receipt time of
> the first prepare cannot help: a rebuilt store never received the first prepare of a transaction its old incarnation
> joined. Instead every store now enforces the invariant *a store joins T only if `T.startTimestamp <= hostWall +
> Frontend.MaxStartLead` (2 s) when it creates T's Tx*. `Frontend.receivePrepare` mutes (`Tx.muted`, the existing
> mechanism: no promise, vote, accept or CRL save, still a prover) an unknown transaction whose start leads the
> host's wall clock by more (`Frontend.startsTooFarAhead`). It reads `System.currentTimeMillis` through
> `wallClockMillis`, not `HLCTimestamp.now`: the HLC is global to the JVM and the host's own client ratchets it from
> object timestamps, so an HLC bound would creep forward by the lead on every round trip. The old incarnation stopped
> before the rebuild began, so any T it joined has `start <= wall_old(stop) + 2 s`. That is earlier than
> `acceptorSince = wall_new(finish) + 10 s` unless the two hosts' clocks differ by more than 8 s less the rebuild's
> duration. Client clocks no longer enter the argument, so late condition 1 ("a rebuilt one is muted and does not
> answer", `TransactionDriver.resolveLate`) holds under that host-skew bound. A fast client is delayed rather than
> refused: its retransmission is answered once the host's clock catches up, and the designated leader does not drive
> a too-far-ahead T meanwhile (`Store`), so no prefer-abort round aborts it. Since every accepted start, and so every
> object timestamp, is at most the fastest host's wall clock plus 2 s, client HLCs ratcheted from them are capped the
> same way. Tests: FrontendSuite "a rebuilt store gives no vote to a transaction its old incarnation may have joined,
> though a fast client stamped it after the acceptor-since time" (failed before the fix), "a store gives no promise or
> vote to a transaction started too far ahead of its clock, and saves nothing", "a store votes on a transaction
> started ahead of its clock by less than MaxStartLead", "a store votes on a retransmitted prepare once its clock
> catches up with the transaction's start", "a store still proves committed a transaction started too far ahead whose
> update it shows"; StoreSuite "a designated leader does not drive a transaction started too far ahead of its clock".

> **Verdict: PLAUSIBLE. Severity: lost committed update (no ErrorTree marks).** The argument for `resolveLate`
> (`TransactionDriver.scala:572-598`) needs every acceptor that accepted the chosen commit either to remember it or to
> have received `TxFinalized`. A rebuilt acceptor forgot without `TxFinalized`; it is excluded only by
> `acceptorSince` (`Frontend.scala:817`: muted iff `txd.startTimestamp < acceptorSince`), and the start timestamp is
> client-chosen. A client clock fast by more than the rebuild's duration plus the 10 s margin (the X2 residual) makes
> the rebuilt store answer a memoryless promise for a live T, and can also make it the prover (it got T's state from the
> rebuild). With quorum intersection only through it (Replication(3,2): one store), a post-proof round sees a memoryless
> quorum and resolves late while T is still live: no finalization actions run, so no ErrorTree marks or UnackedCommits
> entries are written, and every live unresolved commit voter applies nothing on the late commit (`Tx.scala:1001-1007`,
> lookup absent, `:1132-1134`). Needs a fast client clock, a rebuild completing while T is unresolved, and a minimal
> intersection. Before the refresh the same window gave X2's split decision.
> *Fix sketch:* take `acceptorSince` from the host's receipt time of the first prepare, or bound client timestamps.

#### P-N3. A recovering acceptor's accepted commit produces a non-late commit for a forgotten transaction (design residual, broader than stated). Confidence: medium

> **Verdict: PLAUSIBLE. Severity: resurrection (pin-only blind writes, `DoesNotExist` inserts after a later delete,
> allocations).** The design's residual needs the pin object changed (no proof). The code shows it does not: with a
> proof, a post-proof prepare quorum that includes the recovering acceptor's accepted value is not memoryless, so the
> driver adopts the value and resolves *normally* (`TransactionDriver.scala:310-313`, Ruling 7), runs the finalization
> actions again (Ruling 13) and sends non-late `TxResolved`/`TxFinalized` (`:710-713`). Whichever of the recovering store
> or the other acceptors answers first decides between late and normal. Fresh Txs made by a concurrently replayed client
> prepare (which carries data) then apply on a non-late commit (`Tx.scala:916-919`), re-checked only by their own
> requirements: per-write checked writes are skipped, pin-only blind writes, `DoesNotExist` inserts after a later delete
> and allocations of an id whose deletion has persisted are applied. An allocation over a live object is refused
> since 2026-11-14 (`ObjectExists`, transaction-requirements F11).
> Fresh Txs made by the recovering store's own data-less prepares cannot apply data and only report errors, which marks
> every store again (extra repair). Needs an acceptor offline past the cache window holding T's accepted commit, and a
> replayed client prepare during its recovery.
> *Fix sketch:* when a driver holds a proof, treat an adopted accepted commit from a quorum that also contains
> memoryless acceptors as late.
>
> **Refresh (addendum): NARROWED.** A fresh Tx at a store holding an applied stub for T applies nothing on the non-late
> commit and reports T's objects in `objectCommitErrors` (`Tx.scala:642-648`), so the re-run finalizer marks it; this
> covers the stores that acknowledged T late. Stores that acknowledged in time hold no stub and still apply the replay's
> blind writes. The re-run marker's entries now come with a non-late `TxFinalized` listing them in `owed`, so stores
> with no Tx write stubs for them (`Frontend.scala:779-787`), which closes the re-armed R-N1 half of X12 unless that
> `TxFinalized` is lost. Prefer-abort can tilt the race the verdict describes toward the non-late outcome: memoryless
> acceptors reached only by driver prepares withhold their promises for up to 5 s, while the recovering acceptor answers
> at once (P-N7).
>
> **2026-11-14 (Todo 2.9): NARROWED to inside the horizon.** Stores that acknowledged in time saw T and forgot it, so
> they never hold a replay's Tx (rejoin invariant, 10.10): the blind-write leg is closed. Past the horizon every peer
> refuses the recovering acceptor's driver and its Tx ends by expiry. Inside the window it can still join stores that
> never saw T in a post-proof quorum and adopt commit non-late. The finalization actions then re-run (the marker is
> harmless, Todo 2.4; the allocation action's orphan entry is Todo 5.5), and T applies only at never-saw stores,
> which are owed it (transaction-recovery R-N9). The sketched fix is not needed (transaction-recovery, re-assessment "replays through entries with no stub", 2026-11-14, Todo 2.9).

#### P-N4. A forgotten abort can be re-decided commit: pins protect committed transactions only. Confidence: medium

> **Verdict: PLAUSIBLE. Severity: split-decision (a transaction reported aborted later commits).** The pin argument
> ("after T commits ...", P16) says nothing about an aborted T, whose pin precondition still holds. After
> `TxFinalized(false)` every store forgets T (an abort deletes its CRL record at resolution, `Tx.scala:749`) and the cache
> forgets it 7 s later. A later prepare of T, a delayed duplicate client prepare or the periodic prepares of a zombie
> driver (X4), creates fresh Txs checked against current state with no age test (`Frontend.scala:812-831`,
> `Tx.scala:554-570`). If the requirement that made T abort has since cleared (a `DoesNotExist` key deleted, a lock holder
> gone) and the pin object is unchanged, `t` stores vote commit and T commits after its client was told it aborted. A
> zombie driver's prepares carry no data, so only data-less transactions (refcount changes, locks) can commit that way.
> The horizon used to refuse prepares older than 6 s (with F3's clock hole). This widens F6.
> *Fix sketch:* a durable or long-lived abort tombstone per txid on the pinned object's stores, or a per-transaction
> "aborted" record the client writes before reporting abort.
>
> **Refresh (addendum): NARROWED to the delayed duplicate client prepare (accepted corner case). Verdict for the
> residual: PLAUSIBLE (accepted, documented).** Of the three paths (addendum design, problem 2): a recovered commit
> vote's driver re-preparing T is closed by prefer-abort; a zombie driver was already closed by Ruling 31; a delayed duplicate
> of the client's own prepare remains. Argument (6.2): T aborted because some object's pool had more than `w - t`
> non-commit answers in one round, so its commit votes can never exceed `t - 1` afterwards; every store that forgot T
> answers a driver's prepare with an abort vote after the wait (`Tx.scala:537-542`), and a recovered commit vote first
> looks up its entry, finds none for an aborted T, and accepts the abort (`holdAbort`, `:1048-1058`). The residual
> needs the delayed duplicates to reach a write threshold of the stores of every pool T writes (stores the duplicates
> miss see only the replay leader's driver prepares and vote abort), more than the status-cache duration after the
> abort, with T's requirements holding again. A successful client retry closes it, since the retry consumes the same
> pin. Documented in the `Transaction.commit` scaladoc (`client/Transaction.scala:156-161`), `Architecture.md` and
> `UsageGuide.md`. Tested: ReplayIntegrationSuite "an aborted data-less transaction re-driven by a recovered commit vote
> aborts again".
>
> **Refresh (horizon, 2026-11-13): CLOSED.** The delayed duplicate needed every store of T to have forgotten the abort.
> Under the horizon a store remembers it (its finalized abort entry, relayed by `Store.scala:199-221`) for longer than
> it would join T, and past its forget floor refuses the duplicate outright, so no fresh commit vote is ever gathered
> for a forgotten abort (10.10, 6.2). Tested: HorizonIntegrationSuite "a delayed client prepare of a forgotten abort
> does not commit inside or past the horizon" (inside 20 s, after the old 7 s cache would have expired, and past it).
> The accepted corner case is gone from the `Transaction.commit` scaladoc (`client/Transaction.scala:150-162`),
> `Architecture.md` and `UsageGuide.md`. Todo 2.15 closed.

#### P-N5. Liveness of "never propose abort while holding a proof". Confidence: medium

> **Verdict: REFUTED (no new hang).** A proof holder resolves on a memoryless post-proof quorum, on an adopted accepted
> value (sent without the tally, `TransactionDriver.scala:312-313`), or on any late message; a Nack moves it to another
> post-proof round (`:553-559`). It waits only for a prepare quorum of the primary pool, which every driver needs; the
> secondary-pool tally stall (F8) does not apply to it. A competing driver's chosen abort is adopted (Ruling 2). The cost
> is that a proof holder can never conclude an abort itself, which is correct since a proof implies commit.
> *Refresh (Ruling 29):* a proof holder no longer adopts an accepted abort: such an abort predates the ballot at which
> commit was chosen, and adopting it could resolve a committed T as abort. A post-proof quorum reporting no accepted
> commit resolves late. Liveness is unchanged: every post-proof quorum still resolves.

#### P-N6. The finalized relay reports every finalized commit as late. Confidence: medium

> **Verdict: REFUTED.** `Store.scala:167-180` relays a finalized committed entry as `late`. A finalized entry is written
> only on a real `TxFinalized` (`Tx.scala:960`, `Frontend.scala:779`) or a late outcome (`Tx.scala:1035-1037`,
> `Frontend.scala:768-769`); proofs and found entries write nothing (`Tx.scala:1100`). A real `TxFinalized(true)` follows
> the finalizer (`TransactionDriver.scala:650-653`), so the relayed late claim holds. The weakness runs the other way: a
> downgraded entry loses the evidence (F9, X3; fixed by Ruling 30).

### New findings from the addendum refresh (2026-11-10, `c181375b`)

#### P-N7. The prefer-abort wait withholds promises as well as votes. Confidence: high

> **Verdict: CONFIRMED (by design). Severity: liveness (latency up to `MissingDataWait`).** While
> `awaitingClientPrepare` holds, `answerPrepare` sends nothing at all (`Tx.scala:839-840`): the promise is withheld with
> the vote. So for up to 5 s after a store's Tx is created by a driver's prepare, that store is no acceptor for the
> driver. Effects: a recovered `Undetermined` Tx whose acceptor accepted a value reports it only after the wait (Task 1
> concern 1); a proof holder's post-proof round (late condition 1) waits for memoryless promises from stores reached
> only by its own prepares, unless they prove; a recovery driver after a client crash needs up to 5 s more for a promise
> quorum. No safety effect: no reply is always allowed by Paxos, and the vote, once given, is an abort. It interacts
> with P-N3 (the recovering acceptor's accepted commit answers first). A recovered `Undetermined` Tx could vote abort
> at once instead, which is also safe.
>
> **FIXED 2026-11-15 (Todo 4.10).** A recovered Tx without a vote (`Undetermined`) no longer withholds its answer from
> a driver: `answerPrepare` skips the wait for a recovered Tx, so the driver's prepare decides the vote at once
> (`NoClientPrepare` abort, requirements unchecked) and the promise carries what the acceptor accepted. Once loaded
> such a Tx still stays undecided (`completeLoad` honours the wait), so a retransmit of its client's prepare that
> arrives first is checked as usual. Live Txs keep the wait. Tests: FrontendSuite "a recovered undecided Tx answers a
> driver's prepare at once, ..." (with and without an accepted value), "a recovered Tx whose acceptor accepted a value
> does not prove, ..." (now with the default wait), and "a recovered transaction with no persisted vote decides only
> after recovered votes have locked" (the client retransmit path).

#### P-N8. Orphaned and client-starved data-less transactions now abort. Confidence: high

> **Verdict: CONFIRMED (by design). Severity: liveness (aborts, not a safety issue).** Before the addendum a data-less
> transaction (refcount change, version bump, lock-only) got requirement-checked votes from a driver's prepare alone.
> Now a store that has seen no client prepare votes `NoClientPrepare` abort once the wait is over (`Tx.scala:537-542`),
> so an orphan (client died mid-transaction) commits only if its client's prepares reached a write threshold of every
> pool first. The same holds for a live client whose prepares to a store are lost for more than 5 s in a row (QUIC drops
> messages over budget, CLAUDE.md), since the 5 s run from the Tx's creation, not from the last client retransmit: a
> new abort source for data-less transactions, which data transactions already had through the missing-data wait.
> Aborts apply nothing, the client retries. This is the user's stated intent (addendum design B, "Orphaned transactions
> without data").
>
> **CLOSED 2026-11-15 by design (Todo 4.10).** Restarting the wait on each client retransmit is not possible: a store
> never sees the retransmits that are lost, and the first one it sees sets `clientPrepared` and ends the wait. The 5 s
> therefore run from the Tx's creation, as the `Tx.awaitingClientPrepare` scaladoc now states; the client retries.

#### P-N9. Prefer-abort under CRL backpressure. Confidence: medium

> **Verdict: REFUTED (no new abort source).** A store whose device admission is full drops prepares of unseen
> transactions, driver and client alike (`Frontend.admitPrepare`, `Frontend.scala:519-536`). If a driver's prepare is
> admitted first, the Tx exists, and the client's retransmits (every 1 s, `cmdline/Main.scala:1268`) are then prepares
> of a known transaction, admitted without a check (`:525`), well inside the 5 s wait. If neither is admitted there is
> no Tx and no wait. The cost is that the waiting Tx keeps its admission charge (txd plus static record) for up to 5 s.

#### P-N10. Ruling 3 delays a non-late `TxFinalized` past the status-cache window. Confidence: medium

> **Verdict: PLAUSIBLE (mechanism CONFIRMED). Severity: resurrection (blind writes at stores that acknowledged in
> time).** See transaction-recovery R-N8 for the trace. A driver D1 finalized by D0's non-late commit `TxFinalized`
> while its own marker has handed entries off sends its own non-late `TxFinalized` only once those entries are durable
> (`TransactionDriver.scala:426-433` at `c181375b`), which a slow metadata group can delay without bound. Once the stores' finalized
> status-cache entries expire (7 s), a replayed client prepare creates fresh Txs (`Frontend.scala:812-825`), and D1's
> late-arriving non-late `TxFinalized(commit)` makes each of them apply T (`Tx.scala:945-946`, `doCommit`). This is
> P-N3's consequence through a new source of non-late commits. Stores with a stub skip it.
>
> **Refresh (final fix wave, Ruling 7): FIXED.** D1 no longer waits for its entries: on D0's non-late commit
> `TxFinalized` it finalizes at once with D0's `owed` union its planned set (`TransactionDriver.scala:440-459`), inside
> the window in which every store that forgot T still holds T's finalized status entry, and once finalized it sends
> neither `TxResolved` nor a later `TxFinalized` with another value (see transaction-recovery R-N8's refresh). The
> non-late messages that remain after the window are P-N3's own, which predate the addendum.

### Verified safe (not flaws, listed so they are not re-investigated)

* Quorum intersection: IDA constructors enforce a majority write threshold (1.3).
* Vote and promise durability before send (`Tx.scala:876-882,896-902`), vote immutability across restart
  (`:472-482`), and commit requiring `t` commit votes per object (7.2).
* Commit-in-flight ordering and lock lifetime are in the sibling spec; nothing in the Paxos path bypasses them.
* Commit applies only on resolution or proof (P11); `TxFinalized(true)` before `TxResolved` is handled
  (`Tx.scala:949-959`).
* Stale responses from older rounds are filtered by proposal id (`TransactionDriver.scala:286,378`).
* A decided commit survives the leader's death: the recovery driver's promise quorum intersects the acceptors that
  accepted it, and a committed `Tx` keeps its Paxos record until `TxFinalized` (P9).

### Verification summary (2026-11-09)

* **1RTT:** not implemented. Only the designated leader (or a recovery driver) sends `TxAccept` (`TransactionDriver.scala:539-548`, the only constructor). Error-free: client prepare -> store (CRL write 1) -> leader -> Accept -> acceptors (CRL write 2) -> leader -> `TransactionResolved` to the client = 5 one-way hops, 2 serial durable writes. Aborts are decided in round 1.
* **Is a decision ever "fixed" by acceptor state after finalization?** No. Commit keeps acceptor state until `TxFinalized` at each store; abort drops it at resolution; after finalization a stale prepare starts a fresh consensus instance (F2). Applied stores are never rolled back by it, but the client and any still-live commit voter are told abort.
* **Refresh 2026-11-09 (replay pins):** the fresh instance is now steered by the pin rule and the late-commit rule
  (10.5): with a proof it resolves late (nothing applied, client told committed), otherwise it aborts (F2 narrowed). A
  forgotten abort is not protected (P-N4).
* **Refresh 2026-11-10 (addendum):** driver prepares are prefer-abort (6.2), so a forgotten abort is re-decidable only
  by a delayed duplicate client prepare (P-N4 narrowed to the accepted corner case). Applied stubs stop a store that
  holds T's update from applying T again through any later commit (P11). New: P-N7, P-N8 (liveness, by design), P-N9
  (refuted), P-N10 (Ruling 3's delayed non-late `TxFinalized`; fixed by Ruling 7).
* **Refresh 2026-11-13 (horizon):** past the transaction horizon no store joins a transaction it does not hold, and
  every store remembers an outcome for longer (10.10), so a decision is never re-made by a fresh instance: F2 and P-N4
  closed, P-N1 and F10 closed by the horizon, F6 no longer a split path. A transaction whose outcome the client cannot
  learn in time fails with `TransactionOutcomeUnknown`; a stuck `Tx` ends by expiry from its peers' status.
