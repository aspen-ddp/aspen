# Client 1-RTT Commit and Parked Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The client learns that its transaction committed directly from the stores' durable votes, one round trip
after sending its prepares. Recovery and expiry can never contradict that, and a read made right after such a commit
waits at the store for the commit to apply instead of spinning.

**Architecture:** Three layers.
- **Server invariant first** (tasks 1–3): drivers stop proposing abort on undecided tallies past expiry, expiry
  replies report only settled votes, and `ExpiryTally` gets the vote-aware rules A–G.
- **The fast path** (tasks 4–6): a new `TransactionVote` client message, sent from `Tx`'s existing post-save handlers,
  and tallied by `ClientTransactionDriver`.
- **Part 2** (tasks 8–9): `Read.awaitCommit`, set by `BaseReadDriver` on re-reads caused by a committed lock, and
  parked by `Frontend` until that transaction's locks release.

**Tech Stack:** Scala 3, sbt, ScalaTest (`AnyFunSuite`), ScalaPB (`src/main/protobuf/codec.proto`),
`LockstepThreading` / `TestNetwork`.

**Spec:** `docs/superpowers/specs/2026-11-16-client-1rtt-commit-design.md` (read it first; section numbers below
refer to it).

## Global Constraints

- Client fast threshold is `t` = the object's pool `writeThreshold`, per object, over `txd.allReferencedObjectsSet`
  (`TransactionDriver.allObjects`). No fast abort: abort comes only from `TransactionResolved`/`TransactionFinalized`.
- A `TransactionVote` is sent only from the completion handler of the CRL save that holds the vote, never before.
  Only `VoteCommit`/`VoteAbort`, never `Undetermined`. Muted and frozen Txs send none.
- No new durable write and no extra synchronous work on the transaction hot path.
- Server threading: `Tx`/`Frontend` state only on the store executor. Callbacks never run on a shared pool. Nothing
  blocks the control thread (`server/CLAUDE.md`).
- `ParkedReadWait` = 1 s, checked on the store heartbeat. Per-store parked-read cap = 1024. A re-sent `readUUID`
  replaces its parked entry.
- Tests: deterministic, never flaky, as fast as possible. Run single suites with
  `sbt 'testOnly *Suite -- -z "substring"'`. For the full test run use a 70 s hang detector (CLAUDE.local.md).
- No compatibility with persisted data or older peers is needed.
- Commit to the current branch (`paxos_1rtt`). End each commit message with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- When a subsystem's behaviour changes, update its module CLAUDE.md in the same task.

## Review Focus

1. **A vote message sent before its CRL save is durable.** A crash would then let the client count a vote that no
   longer exists. Task 5 tests that a vote is not sent while the save is pending.
2. **Two expiry observers with different silent sets reaching commit and discard.** That is a split at the stores.
   Task 3's exhaustive consistency test covers it.
3. **A client that completes on votes but stops retransmitting before the leader's prepare lands.** Locks would be
   held for about 5 s until an idle driver starts. Task 6 tests that retransmits continue until a leader message.
4. **A parked read that never answers** (the Tx is discarded, expires, or the store fail-stops or closes). Task 9
   tests discard and timeout.
5. **A stale frozen Tx reporting "no vote" and then voting commit.** Task 2 tests that it stays silent until its
   disposition is decided.

---

### Task 1: Drivers never propose abort on an undecided tally

Spec 4.3(a).

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriver.scala`
  - delete the `else if (start.pastExpiry())` branch (about line 460)
  - remove the `pastExpiry` parameter of `TransactionDriver.Start` (lines 48–57) and its scaladoc
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala:204`: drop `pastExpiry = ...`.
  `Frontend.pastExpiry` stays, because `Tx.frozen` and `Store.scala:171` use it.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriverSuite.scala`

**Interfaces:**
- Produces: `TransactionDriver.Start` without `pastExpiry`. Fix every call site the compiler reports, tests included.

- [ ] **Step 1: Rewrite the test "an undecided tally waits until past expiry, then proposes abort"** as "an undecided
  tally never proposes abort". Keep the same setup: ds0 `VoteCommit`, ds1 `VoteAbort`, ds2 silent, no `Start`
  argument. After a second `promiseFrom(driver, ds1, VoteAbort)`, assert `driver.testHaveLocalProposal shouldBe false`
  and `accepts(messenger) shouldBe Nil`. Update the comment above it: rebuild F6 now ends by expiry rule E.
- [ ] **Step 2: Update the two "past expiry, ..." tests** (about lines 254 and 266). Drop `pastExpiry = () => true`
  and keep their assertions. A quorum reporting an accepted commit still proposes it. A proof holder still never
  proposes abort.
- [ ] **Step 3: Add the test "an abort-certain tally still proposes abort"**, with Replication(3,2) votes ds0 abort and
  ds1 abort. Assert accepts to ds0, ds1 and ds2 with value `false`.
- [ ] **Step 4: Run** `sbt 'testOnly *TransactionDriverSuite'`. Expected: the rewritten test FAILs (an abort is
  proposed) or fails to compile.
- [ ] **Step 5: Delete the branch and the parameter.**
- [ ] **Step 6: Run** `sbt 'testOnly *TransactionDriverSuite *HorizonIntegrationSuite *ReplayIntegrationSuite'`.
  Expected: PASS. A HorizonIntegrationSuite test that depended on the driver's abort may need task 3: note it, and run
  it again after task 3.
- [ ] **Step 7: Update `server/transaction/CLAUDE.md`**, the Paxos bullet "Every object is classified ...". Remove the
  past-expiry abort sentence and state that an undecided tally waits, and that expiry ends the Tx (rules in task 3).
- [ ] **Step 8: Commit** with the message "Drivers never propose abort on an undecided tally (Todo 8.1)".

### Task 2: An unresolved Tx answers status requests only once its vote is decided

Spec 4.3(b), "A reported vote must be final".

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala:1542-1548` (`receiveStatusRequest`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`

**Interfaces:**
- Produces: no new names. In `receiveStatusRequest`, an unresolved, non-muted Tx replies only when `frozen &&
  allObjectsLoaded`. Resolved and muted-for-good Txs reply as now.

- [ ] **Step 1: Write the test "a frozen Tx whose objects have not loaded is silent to status requests until they
  load"**.
  - Create a Tx whose object's backend read is held, using the suite's existing deferred-backend pattern (search
    FrontendSuite for a held `backend.read`).
  - Move the wall clock past `pastExpiry` and deliver a `TxStatusRequest`. Assert that no `TxStatusResponse` was sent.
  - Complete the read and deliver a new request. Assert that one response was sent, with `votedCommit` matching the
    vote now decided.
- [ ] **Step 2: Run** `sbt 'testOnly *FrontendSuite -- -z "silent to status requests"'`. Expected: FAIL (a response
  is sent with `votedCommit = false`).
- [ ] **Step 3: Add the `allObjectsLoaded` gate.**
- [ ] **Step 4: Run** `sbt 'testOnly *FrontendSuite *HorizonIntegrationSuite'`. Expected: PASS.
- [ ] **Step 5: Update `server/store/CLAUDE.md`**, the status-query bullet: "an unresolved Tx answers only once frozen
  and its objects are loaded (its vote decided)".
- [ ] **Step 6: Commit** with the message "Expiry status replies report only decided votes (Todo 8.1)".

### Task 3: Vote-aware expiry rules A–G

Spec 4.3(b), the full rule text and the consistency argument.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/ExpiryTally.scala`. Rewrite `decideUnknown` and its
  scaladoc. Rewrite the `Decision.Commit` / `Discard` / `Hold` scaladoc to cite rules A–G. `decide`'s rule 1 and
  `decideKnown` are unchanged.
- Check: does `Tx.statusForExpiry`'s `votedCommit` hold for a commit voter whose objects are lock-only? It is
  `disposition == VoteCommit`, so it should. Confirm it with a test below. Add no field unless the test fails.
- Test: modify `src/test/scala/org/aspen_ddp/aspen/server/transaction/ExpiryTallySuite.scala`, and create
  `src/test/scala/org/aspen_ddp/aspen/server/transaction/ExpiryTallyConsistencySuite.scala`.

**Interfaces:**
- Consumes: `TxStatus(status, finalized, late, holdsAcceptorState, accepted: Option[Boolean], votedCommit, amnesiac)`.
- Produces: `ExpiryTally.decide(txd, replies, known)` with the same signature and `Decision` cases. A rule letter
  appears only in scaladoc and log text.

Counts, computed in `decideUnknown`:
- Per referenced pool, over `txd.allReferencedObjectsSet.map(_.poolId)`:
  - `vc`: replied `Some` with `votedCommit`
  - `k`: replied `Some` with `amnesiac`
  - `s`: absent from `replies`
- Primary pool: `aa`/`ac` are stores replying `accepted == Some(false)` / `Some(true)`. `k_p` and `sp` are the primary
  pool's `k` and `s`.
- Changed pools are the existing `changedPools`, each with `r = consistentRestoreThreshold`.

Rules, in order, with `t` per pool:
- **A.** Every referenced pool has `vc >= t`: Commit.
- **B.** `ac >= 1` and every changed pool has `vc >= r`: Commit.
- **C.** `aa >= 1`: Discard(dataLost = false).
- **D.** Some changed pool has `vc + s < r`: Discard(dataLost = `ac >= 1 ||` every referenced pool has
  `vc + k + s >= t`).
- **E.** Some referenced pool has `vc + k + s < t`: Discard(dataLost = false).
- **F.** All of these hold: no store of T is silent, every referenced pool has `vc + k >= t`, `k_p < t_primary`, and
  every changed pool has `vc >= r`: Commit.
- **G.** Otherwise: Hold(undecidable = `replies.size >= txd.allDataStores.size`).

- [ ] **Step 1: Write the failing per-rule tests in ExpiryTallySuite.** Add a `secondary` pool via `txdWith(...,
  others = Map(otherPoolId -> Replication(3,2)), extra = <an update of a secondary object>)` where needed, and
  `holder(voted, accepted)` / `amnesiac` / `unknown` as the suite defines them.
  - "every pool at its write threshold of commit votes commits though a secondary store is silent and nothing was
    accepted": rule A, Commit.
  - "one accepted commit with commit voters at the read threshold commits": rule B. Today it needs `ac > w - t`.
  - "an accepted abort discards": rule C.
  - "the F6 split, one commit vote, one abort vote and an unknown store, discards": rule E. Use Replication(3,2) as
    the primary.
  - "a rebuilt store's lost vote with every other store replied commits": rule F. ReedSolomon(5,3,4) primary: three
    `holder(voted = true)`, one `holder(voted = false)`, one `amnesiac`, nothing accepted. Commit.
  - "fewer known commit votes than the write threshold with a store silent holds": rule G, `Hold(false)`.
  - "t or more rebuilt primary stores with every store replied is undecidable": rule G, `Hold(true)`.
  - "a lock-only pool's commit voters count toward rule A": a secondary pool whose only object is lock-only.
  - "dataLost is set when the votes may have decided commit": D with every pool `vc + k + s >= t`.
- [ ] **Step 2: Re-examine the existing tests** whose expectations the new rules change. Update each expectation and
  put the reason in its name or comment.
  - "stuck holders that accepted nothing discard" (line 88): its voters may now be enough for A or F.
  - "one commit accept with a rebuilt store discards" (line 97): now B commits if `vc >= r`.
- [ ] **Step 3: Write `ExpiryTallyConsistencySuite` with the test "no two observers decide commit and discard"**.
  - Configurations: primary Replication(3,2), Replication(5,3), Replication(5,4) and ReedSolomon(5,3,4), each alone
    and with a Replication(3,2) secondary changed pool.
  - Enumerate every true final state per store, restricted to what the protocol can produce:
    - vote ∈ {commit, abort, none} per store
    - for a primary store: accepted ∈ {None, Some(true), Some(false)}
    - an accepted commit is allowed only if every pool truly has `>= t` commit votes (or a proof). An accepted abort is
      allowed only if some pool has `> w - t` non-commit votes (L1, L2)
    - amnesiac stores: any subset of up to `t` stores per pool; their reply is `amnesiac` whatever they truly held
    - "none" with no Tx replies `unknown`
  - For each true state, enumerate pairs of silent sets: each observer independently omits a subset of up to 2 stores.
  - Assert that no pair has one `Decision.Commit` and one `Decision.Discard`.
  - Also assert that A, B and F never fire when no tally could have been commit-certain, and that C never fires when
    every pool truly has `t` commit votes.
  - Cap the enumeration so the suite runs in under 5 s. If the full product is too large, sample silent sets with a
    fixed seed, but enumerate the true states exhaustively.
- [ ] **Step 4: Run** `sbt 'testOnly *ExpiryTallySuite *ExpiryTallyConsistencySuite'`. Expected: new tests FAIL.
- [ ] **Step 5: Implement `decideUnknown`.** Its scaladoc carries lemmas L1/L2, the rules, and the consistency cases
  from spec 4.3(b).
- [ ] **Step 6: Run** `sbt 'testOnly *ExpiryTally* *HorizonIntegrationSuite *TransactionDriverSuite'`. Expected:
  PASS. A HorizonIntegrationSuite expectation the rules change (for example "stuck holders short of a quorum all
  discard") is updated, with the reason in the commit message.
- [ ] **Step 7: Update `server/transaction/CLAUDE.md`**, the Expiry section: replace "The pure `ExpiryTally.decide`
  rules" items (2) to (4) with rules A–G, and drop the rebuild F6 driver-abort reference.
- [ ] **Step 8: Commit** with the message "Vote-aware expiry rules: commit is final once every object has t commit
  votes (Todo 8.1)".

### Task 4: `TransactionVote` message

Spec 4.1.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala`. Add after `TransactionFinalized`:
  ```scala
  final case class TransactionVote(toClient: ClientId, fromStore: StoreId, transactionId: TransactionId,
                                   disposition: TransactionDisposition.Value) extends ClientResponse
  ```
  Use the same disposition type `TxPrepareResponse.disposition` uses, and a `toString` like `TransactionResolved`'s.
- Modify: `src/main/protobuf/codec.proto`. Add `message TransactionVote { UUID to_client = 1; StoreId from_store = 2;
  UUID transaction_id = 3; TransactionDisposition disposition = 4; }`, with the disposition encoded as
  `TxPrepareResponse` encodes it. Add it to `Message.msg` as `TransactionVote tx_vote = 38;`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`. Add `encode(o: TransactionVote)` /
  `decode(m: codec.TransactionVote)` next to `TransactionResolved`'s, at about line 945.
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicMessageCodec.scala`, at about
  line 110, in both directions.
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:654`. Add
  `case m: TransactionVote => txManager.receive(m)`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionManager.scala`. Add
  `def receive(vote: TransactionVote): Unit`, which delivers to the registered driver and does not forget it.
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/ClientTransactionDriver.scala`. Add
  `def receive(vote: TransactionVote): Unit = ()` for now. Task 6 implements it.
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`

**Interfaces:**
- Produces: `TransactionVote` as above, `TransactionManager.receive(vote: TransactionVote)` and
  `ClientTransactionDriver.receive(vote: TransactionVote)`.

- [ ] **Step 1: Add the test "TransactionVote round-trip"**, modelled on "TransactionResolved round-trip" (line 535).
  Run it once each for `VoteCommit`, `VoteAbort` and `Undetermined`. Each must survive `Codec` and the QUIC message
  codec.
- [ ] **Step 2: Run** `sbt 'testOnly *CodecRoundTripSuite -- -z TransactionVote'`. Expected: compile FAIL.
- [ ] **Step 3: Implement the message and its wiring as listed.**
- [ ] **Step 4: Run** `sbt 'testOnly *CodecRoundTripSuite'`. Expected: PASS.
- [ ] **Step 5: Update the messages section of `common/network/CLAUDE.md`** if it lists the client responses.
- [ ] **Step 6: Commit** with the message "TransactionVote client message (Todo 8.1)".

### Task 5: Stores send their durable vote to the client

Spec 4.1.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala`. The two post-save send sites are
  `sendDelayedPrepare` (line 760) and `answerPrepareAsAcceptor` (line 1213). Both call a new helper
  `private def sendPrepareResponse(r: TxPrepareResponse): Unit`. It sends `r`, then, when `r.disposition` is
  `VoteCommit` or `VoteAbort` and `txd.originatingClient` is defined, sends
  `net.sendClientResponse(TransactionVote(client, storeId, transactionId, r.disposition))`.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`. `TestNet` (line 56) keeps only the last
  client response in `cr`. Add `val clientResponses = ListBuffer[ClientResponse]()`, appended by
  `sendClientResponse`, and keep `cr`.

**Interfaces:**
- Consumes: `TransactionVote` (task 4).

- [ ] **Step 1: Write the tests.**
  - "a client prepare's durable vote is sent to the originating client". Use a txd with
    `originatingClient = Some(clientId)` and a CRL whose save completion the test controls (the suite's recording or
    deferred CRL). After the prepare, assert that `clientResponses` has no `TransactionVote`. Complete the save, then
    assert exactly one `TransactionVote(clientId, storeId, txid, VoteCommit)`.
  - "a retransmitted prepare re-sends the vote": after a second identical prepare and its save, there are two votes.
  - "an abort vote is sent too": with a failing requirement, the vote is `VoteAbort`.
  - "a frozen or muted Tx sends no vote": past expiry by the store clock, a prepare yields no `TransactionVote`.
  - "no originating client, no vote".
- [ ] **Step 2: Run** `sbt 'testOnly *FrontendSuite -- -z vote'`. Expected: FAIL.
- [ ] **Step 3: Add the helper and route both sites through it.**
- [ ] **Step 4: Run** `sbt 'testOnly *FrontendSuite *StoreSuite *TransactionDriverSuite'`. Expected: PASS.
- [ ] **Step 5: Update `server/transaction/CLAUDE.md`, Lifecycle:** "each store sends its vote to the originating
  client (`TransactionVote`) from the handler of the CRL save that holds it, alongside every prepare response".
- [ ] **Step 6: Commit** with the message "Stores send their durable vote to the originating client (Todo 8.1)".

### Task 6: The client completes Committed from votes and keeps retransmitting

Spec 4.2.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/ClientTransactionDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/SimpleClientTransactionDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionManager.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/ClientTransactionDriverSuite.scala`

**Interfaces:**
- Consumes: `TransactionVote` (task 4).
- Produces, in `ClientTransactionDriver`:
  - `def receive(vote: TransactionVote): Unit`. It records the store's first vote and calls
    `complete(ClientOutcome.Committed)` once `commitCertain`. A second, different vote from a store is logged as an
    error and ignored.
  - `protected def commitCertain: Boolean`: every `ptr` in `txd.allReferencedObjectsSet` has, over
    `StoreId(ptr.poolId, 0 until txd.poolIDAMap(ptr.poolId).width)`, at least `writeThreshold` stores with
    `VoteCommit`.
  - `def stopped: Future[Unit]`: completes when the driver needs no more messages, that is when it hears a
    `TransactionResolved`/`TransactionFinalized` or reaches `Unknown`. A completion by votes alone does not complete
    it. In the base class (no retransmits), `stopped` completes together with `result`.
- `SimpleClientTransactionDriver`:
  - `complete(Committed)` from votes no longer cancels the retransmit task. `tick()` keeps sending prepares until a
    leader message arrives or `pastGiveUp`.
  - Reaching the give-up point after a vote-completion cancels the task and completes `stopped` without changing
    `result`. `complete` already ignores later outcomes.
  - `receive(TransactionResolved | TransactionFinalized)` cancels the task and completes `stopped`.
- `TransactionManager`: forget the driver on `td.stopped`, not `td.result`. `receive(resolved)` and
  `receive(finalized)` still deliver, then forget.

- [ ] **Step 1: Write the tests in ClientTransactionDriverSuite.** Use the suite's Fixture and the Replication(3,2)
  pool. Add a two-pool txd helper: primary Replication(3,2), plus a second pool Replication(5,3) holding one updated
  object.
  - "two commit votes complete Committed; one does not": deliver `TransactionVote` from store 0, then assert
    `result.isCompleted shouldBe false`; deliver store 1, then assert `result.value shouldBe Some(Success(Committed))`.
  - "each object needs its own pool's write threshold": two-pool txd. Two primary commit votes plus two secondary ones
    stay incomplete. A third secondary vote completes.
  - "abort votes never complete the driver": all three primary stores vote abort, and the result is incomplete.
    `TransactionResolved(committed = false)` then completes `Aborted`.
  - "a duplicate vote counts once": store 0 twice, and the result is incomplete.
  - "Undetermined is no vote".
  - "after a vote-completion the driver keeps retransmitting until the leader's outcome": `tick()` still raises
    `msgr.prepares`. After `receive(TransactionResolved(..., true))`, `tick()` sends nothing and `stopped` is
    completed.
  - "after a vote-completion the driver stops at the give-up point with its result unchanged": advance `now` past
    `giveUp`, call `tick()`, then `stopped` is completed and `result` is still `Committed`.
  - "a leader abort after a vote commit is logged as a split, and the result stays Committed".
- [ ] **Step 2: Run** `sbt 'testOnly *ClientTransactionDriverSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement as specified.**
- [ ] **Step 4: Run** `sbt 'testOnly *ClientTransactionDriverSuite *TransactionOutcomeUnknownSuite
  *BasicIntegrationSuite'`. Expected: PASS.
- [ ] **Step 5: Update `client/internal/CLAUDE.md`**, the `ClientTransactionDriver.result` bullet: commit from `t`
  votes per object, abort only from the leader, retransmits until the leader's outcome or give-up, and
  `TransactionManager` forgetting on `stopped`.
- [ ] **Step 6: Commit** with the message "Client learns commit from the stores' votes in one round trip (Todo 8.1)".

### Task 7: End-to-end fast commit, and the protocol docs

Spec 7 (integration tests) and 8 (docs).

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/FastCommitIntegrationSuite.scala`. It
  `extends IntegrationTestSuite`, with `testHostOptions` like HorizonIntegrationSuite's (100 ms heartbeat,
  `recoveryDrivers = true`).
- Modify: `Architecture.md` (the paragraph at about line 139, "Transactions are designed to reach consensus ...",
  and the following one, "It's worth pointing out ...")
- Modify: `docs/safety/paxos-transactions.md`:
  - §2 (table, and the "must not be used as invariants" note)
  - §3 (a new invariant "P-VC vote-decided commit", with the P2/P6 derivation redone)
  - §4 (`TransactionVote` row)
  - §5 (client vote leg in the diagram and step table)
  - §10.10 (expiry rules A–G, lemmas L1/L2, consistency argument)
  - a new §10.12 "Abort paths audit" (spec 4.3(c): every abort path checked against P-VC)
  - F1 marked FIXED with a dated note (2026-11-16)
- Modify: `docs/safety/README.md`, the F1 ranked row.
- Modify: `docs/superpowers/specs/2026-11-13-transaction-horizon-design.md`, section 2. Add a dated note that its
  decision rules are superseded by the 2026-11-16 design.
- Modify: `docs/safety/rebuild-process.md`, the F6 entry: it now ends by expiry rule E, not a driver abort.
- Modify: `Todo.txt`: remove item 8.1 and keep the other numbers.

**Interfaces:**
- Consumes: everything from tasks 1–6. TestNetwork's `dropTxMessage: TxMessage => Boolean` and
  `dropClientResponse: ClientResponse => Boolean`.

- [ ] **Step 1: Write the tests.**
  - "the client learns commit from votes while every leader outcome to it is dropped": set `dropClientResponse` to
    drop `TransactionResolved` and `TransactionFinalized`. Run one overwrite transaction. Its `result` succeeds well
    before the horizon. Assert within 2 s, using the suite's lockstep advance. A read then returns the new revision.
  - "the client learns commit while the Accept round is blocked, and the stores commit once it is unblocked": set
    `dropTxMessage` to drop every `TxAccept`. The client's commit succeeds. Then reset `dropTxMessage` and advance.
    The read returns the new revision, and every store's Tx is finalized (no transactions left in
    `frontend.transactions`).
  - "a transaction with an abort vote is reported aborted only by the leader": a failing requirement on one store of
    Replication(3,2) plus commits elsewhere still commits. Make two stores fail, and assert `TransactionAborted` is
    raised only after `TransactionResolved` is delivered. Do this by counting delivered `TransactionResolved` in a
    `dropClientResponse` hook that drops nothing and counts.
- [ ] **Step 2: Run** `sbt 'testOnly *FastCommitIntegrationSuite'`. Expected: PASS, since tasks 1–6 are in. If a test
  fails, fix the cause in the owning task's code rather than the test.
- [ ] **Step 3: Write the doc changes listed above.** Architecture.md describes what is built:
  - the client learns commit from `t` durable commit votes per object, in one round trip
  - abort only through the leader's Paxos round
  - no commit-only round 1
  - recovery prepares are prefer-abort, but a driver never proposes abort unless the tally is abort-certain
  - expiry decides by rules A–G
- [ ] **Step 4: Run the full suite** (`sbt test`) with a 70 s timeout as the hang detector (CLAUDE.local.md). Look for any single
  suite exceeding 70 s of silence. Expected: all PASS.
- [ ] **Step 5: Commit** with the message "Fast-commit integration tests; protocol docs describe the 1-RTT client
  commit (Todo 8.1, P-F1)".

### Task 8: `Read.awaitCommit`, set on committed-lock re-reads

Spec 5.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala:152`. Add
  `awaitCommit: Set[TransactionId] = Set()` to `Read`.
- Modify: `src/main/protobuf/codec.proto`, `message Read`. Add `repeated UUID await_commit = 15;`.
- Modify: `Codec.scala`, the `Read` encode and decode.
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala`.
  - `sendReadRequest(store)` gains `awaitCommit: Set[TransactionId] = Set()`.
  - The re-read at about line 170 (the `hasLocksForKnownCommittedTransactions` branch) passes the known-committed
    txids among that reply's `lockedWriteTransactions`.
  - The re-read at about line 76 (`undecidedLockStores` now known committed) passes the known-committed txids among
    that store's recorded locks.
  - Extract `private def knownCommitted(store: StoreId, locks: Set[TransactionId]): Set[TransactionId]` from
    `holdsKnownCommittedLock`, which becomes `knownCommitted(...).nonEmpty`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala` (or wherever `Read` is dispatched to
  `Frontend.readObjectForNetwork`): pass `awaitCommit` through. `readObjectForNetwork` gains
  `awaitCommit: Set[TransactionId] = Set()`, ignored until task 9.
- Test: `CodecRoundTripSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`

**Interfaces:**
- Produces: `Read.awaitCommit` and `Frontend.readObjectForNetwork(clientId, readUUID, pointer, readType, awaitCommit)`.

- [ ] **Step 1: Write the tests.**
  - "Read with awaitCommit round-trip" in CodecRoundTripSuite: an empty set and a two-element set.
  - In BaseReadDriverSuite, "a re-read for a known-committed lock names that transaction": the client's
    `txStatusCache` has T committed, and a store's reply holds T's lock. Assert that the next `Read` sent to that
    store has `awaitCommit == Set(T)`, and that first reads have it empty.
- [ ] **Step 2: Run** `sbt 'testOnly *CodecRoundTripSuite *BaseReadDriverSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement as specified.**
- [ ] **Step 4: Run** the same suites. Expected: PASS.
- [ ] **Step 5: Commit** with the message "Reads carry the committed transactions a re-read waits for (Todo 8.1
  part 2)".

### Task 9: Frontend parks reads until the named commit applies

Spec 5.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala`
  - `readForNetwork` (line 1606)
  - a new `parkedReads` structure beside `parkedRepairs` (line 522)
  - `expireParkedReads()` beside `expireParkedRepairs()` (line 1780)
  - fail-stop and close clear it, where `failParkedRepairs()` is called (lines 1216, 1389)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala:613` (`unlock`): after
  `RequirementsLocker.unlock`, call `frontend.locksReleased(<the ids of the objects it unlocked>)`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala:152`: call `frontend.expireParkedReads()` next
  to `expireParkedRepairs()`.
- Test: `FrontendSuite.scala`

**Interfaces:**
- Consumes: `readObjectForNetwork(..., awaitCommit)` (task 8).
- Produces:
  - `Frontend.ParkedReadWait: FiniteDuration = 1.second` and `Frontend.MaxParkedReads = 1024`
  - `def locksReleased(objectIds: Iterable[ObjectId]): Unit`
  - `def expireParkedReads(): Unit`
- Parking rule: `servableState(objectId)` exists, `awaitCommit` is non-empty, and
  `os.lockedWriteTransactions.intersect(awaitCommit).nonEmpty`. The read is then parked, keyed by `(objectId,
  readUUID)`, with deadline `wallClockMillis() + ParkedReadWait.toMillis`. A same-`readUUID` entry is replaced. At
  `MaxParkedReads` the read is answered at once.
- Release: `locksReleased` re-runs `readForNetwork` with an empty `awaitCommit` for each parked read on those objects
  whose lock intersection is now empty. `expireParkedReads` answers each read past its deadline with the current state.
- Discard and expiry paths that drop a Tx without `unlock()` must also call `locksReleased`. Check `expire()`,
  `forgetMuted` and `expireTransaction` (line 1095).

- [ ] **Step 1: Write the tests in FrontendSuite.**
  - "a read awaiting a lock holder's commit is answered when the commit applies": a Tx holds T's lock on `op1`;
    `readObjectForNetwork(..., awaitCommit = Set(T))` sends no response. Resolve T committed. Exactly one
    `ReadResponse`, showing T's revision and no T lock.
  - "a read awaiting a transaction that holds no lock is answered at once".
  - "a parked read is answered at ParkedReadWait with the current state": advance the test clock by 1 s and call
    `expireParkedReads()`. One response, still showing T's lock.
  - "a parked read is answered when its transaction is discarded at expiry".
  - "a resent read replaces its parked entry": two parks with the same `readUUID`, then release, give one response.
  - "past MaxParkedReads a read is answered at once".
  - "fail-stop drops parked reads without answering".
- [ ] **Step 2: Run** `sbt 'testOnly *FrontendSuite -- -z "parked read\|awaiting"'`. Expected: FAIL.
- [ ] **Step 3: Implement as specified.**
- [ ] **Step 4: Add an end-to-end test to FastCommitIntegrationSuite, "a read straight after a fast commit returns the
  new revision without spinning"**. Drop `TxAccept` for 300 ms of lockstep time after the votes, then read. The read
  returns T's revision, and the read driver's `committedLockRereads` is at most one per store. Expose the counter to
  the test the way BaseReadDriverSuite reads it.
- [ ] **Step 5: Run** `sbt 'testOnly *FrontendSuite *FastCommitIntegrationSuite *StoreSuite'`, then the full suite
  with the 70 s hang detector. Expected: PASS.
- [ ] **Step 6: Update the docs.**
  - `server/store/CLAUDE.md`: add a "Parked reads" bullet beside "Parked repairs".
  - `client/internal/CLAUDE.md`: on known-committed locks, re-reads carry `awaitCommit`.
- [ ] **Step 7: Commit** with the message "Stores park a re-read until the committed transaction it names applies
  (Todo 8.1 part 2)".
