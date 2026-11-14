# Transaction Horizon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** No store joins a transaction it may have forgotten, stores remember outcomes longer than that horizon, every
in-memory Tx ends by resolution or expiry, and a client that cannot learn an outcome gets `TransactionOutcomeUnknown`.

**Architecture:** A per-store *forget floor* in `Frontend` refuses prepares for unknown transactions started before it
(no Tx is created). The host status cache outlasts the horizon. An unresolved Tx past the horizon queries its peers with
`TxStatusRequest` and discards itself only when the replies prove no primary-pool quorum can still hold the transaction.
The client stops retransmitting past the horizon and fails with a new sibling of `TransactionAborted`.

**Tech Stack:** Scala 3, sbt, ScalaTest, Scaffeine/Caffeine, protobuf (`src/main/protobuf/codec.proto`).

**Spec:** `docs/superpowers/specs/2026-11-13-transaction-horizon-design.md` (read it first; this plan argues from it).

## Global Constraints

- Horizon: **20 s**, on `txd.startTimestamp.wallTime` against the receiving host's `Frontend.wallClockMillis()`.
- `MaxStartLead` stays **2 s**.
- Status cache duration = `horizon + MaxStartLead + 8 * heartbeatPeriod` (= **30 s** at the production 1 s host
  heartbeat). Asserted with a `require` that it is `>= horizon + MaxStartLead + txHeartbeatTimeout + heartbeatPeriod`.
- The horizon is injectable (Frontend, Host, TestNetwork.HostOptions, client driver factory) so integration tests use
  short values; production code paths use the 20 s constant.
- `TransactionOutcomeUnknown` is a **sibling** of `TransactionAborted` (`extends TransactionProcessingError`).
- Unknown is retried by default wherever aborts are retried.
- Discard rule: more than `w − t` explicit non-holders among the **primary pool** (`txd.primaryObjectIDA`: `width`,
  `writeThreshold`), and no UnackedCommits entry for a commit voter.
- A store answers a status query with "unknown" **only** if T's start is below its forget floor; above it, it stays
  silent (designer's call 7: silence).
- Invariant: **a store never rejoins a transaction it forgot.**
- Scala 3 quiet syntax, indented if/then/else; os-lib for files; tests deterministic (LockstepThreading /
  FrontendSuite's synchronous harness where possible). No flaky tests; keep every test as fast as correctness allows.
- Full test run: `sbt test` with a 70 s hang budget per the user's rules (normal load).
- Tests that use a short injected horizon must not flake under load: Task 6 runs the full suite under 100 % CPU.
- Commit directly on `main`. End every commit message with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Existing tests with ancient start timestamps.** FrontendSuite uses `ago(3600 s)` / `appliedKVPrepare(..., -3600 s)`
   style prepares (e.g. "a prover answers a later prepare…", rebuilt-store and MaxStartLead tests). Under the floor
   these are refused outright. Each must be re-aged inside the horizon (or given a long injected horizon) so it still
   tests what its name says, not deleted. Owned by Task 3.
2. **A store's own CRL-recovered Tx older than the floor** must still be driven and answered: the floor applies only to
   transactions the store does not hold. Test in Task 3 ("a recovered Tx older than the floor still answers its
   peers' prepares").
3. **A prepare that arrives with a non-Unresolved status-cache entry below the floor** keeps today's behaviour (relay /
   fresh Tx with `outcomeDecided`), so a late commit is still learned. Test in Task 3.
4. **Expiry while the UnackedCommits lookup fails** (metadata pool unreachable): the Tx keeps holding and retries; it
   never discards on a failed lookup. Test in Task 5.
5. **Client clock slower than the horizon**: every transaction is refused; the store logs a warning naming the lag, and
   the client fails fast with unknown rather than hanging. Tests in Task 3 (warning, no Tx) and Task 1 (fail fast).

---

### Task 1: Client outcome type, give-up and callers

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionHorizon.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/TransactionError.scala` (~71)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/ClientTransactionDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/SimpleClientTransactionDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionManager.scala` (~20-30)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala` (~380-400)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala` (~34)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala` (commit scaladoc ~150-163)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/StopRetrying.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala` (~88)
- Modify: `src/main/scala/org/aspen_ddp/aspen/vdisk/DiskCatalog.scala` (~244)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (`MaxStartLead` becomes an alias)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/ClientTransactionDriverSuite.scala` (create)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/StopRetryingSuite.scala` (create, or extend an existing suite if one
  covers `unlessCollision`)
- Docs: `client/CLAUDE.md`, `client/internal/CLAUDE.md`

**Interfaces:**
- Produces:
  - `object TransactionHorizon { val Horizon: FiniteDuration = 20.seconds; val MaxStartLead: FiniteDuration = 2.seconds;
    def clientGiveUp(horizon: FiniteDuration): FiniteDuration = horizon + MaxStartLead;
    def statusCacheDuration(horizon: FiniteDuration, heartbeatPeriod: Duration): FiniteDuration }` (the formula in
    Global Constraints).
  - `Frontend.MaxStartLead` = `TransactionHorizon.MaxStartLead` (keep the name and scaladoc).
  - `final case class TransactionOutcomeUnknown(txd: TransactionDescription) extends TransactionProcessingError`.
  - `enum ClientOutcome { case Committed, Aborted, Unknown }` in `ClientTransactionDriver.scala`;
    `ClientTransactionDriver.result: Future[ClientOutcome]`, `protected def complete(outcome: ClientOutcome): Unit`;
    `TransactionManager.runTransaction(...): Future[ClientOutcome]`.
  - `SimpleClientTransactionDriver.factory(retransmitDelay: Duration, horizon: FiniteDuration = TransactionHorizon.Horizon)`.

- [ ] **Step 1: Write the failing tests**

  In `ClientTransactionDriverSuite` (drive `SimpleClientTransactionDriver` with a test client/messenger and an
  injected clock or a txd whose `startTimestamp` is aged; follow how existing client suites build an `AspenClient`
  test double):
  - `"a client gives up with Unknown once the transaction is past the horizon plus MaxStartLead"`: txd started
    `horizon + MaxStartLead + 1 retransmit` ago (use `horizon = 500.millis`), no replies; `result` completes with
    `ClientOutcome.Unknown` and no further prepares are sent afterwards.
  - `"a client still past the horizon at begin fails fast without sending"`: txd started `clientGiveUp + 1 s` ago;
    after `begin()`, `result` is `Unknown` and the messenger saw zero prepares.
  - `"a resolution before the give-up completes Committed or Aborted"`: `receive(TransactionResolved(committed=true))`
    gives `Committed`; a later give-up tick changes nothing.

  In `StopRetryingSuite`:
  - `"unlessCollision retries TransactionOutcomeUnknown"`: `unlessCollision(TransactionOutcomeUnknown(txd))` completes
    with `()`; `unlessCollision(new Exception)` throws `StopRetrying`.

  In `TransactionImpl`'s existing suite (find it with `grep -rl "TransactionAborted" src/test`), or a new test in
  `TransactionBuilderSuite`'s neighbour: `"an Unknown outcome fails commit with TransactionOutcomeUnknown and records
  nothing in the client status cache"`.

- [ ] **Step 2: Run them and confirm they fail to compile/fail**

  Run: `sbt 'testOnly *ClientTransactionDriverSuite *StopRetryingSuite'`
  Expected: compilation errors (`ClientOutcome`, `TransactionOutcomeUnknown` not found).

- [ ] **Step 3: Implement**
  - `TransactionHorizon` as in Interfaces; `Frontend.MaxStartLead` aliases it.
  - `ClientOutcome` and the widened `complete` / `result`; `receive(TransactionFinalized|TransactionResolved)` maps
    `committed` to `Committed`/`Aborted`.
  - `SimpleClientTransactionDriver`: at `begin()` and on each periodic tick, if
    `HLCTimestamp.now.wallTime - txd.startTimestamp.wallTime > clientGiveUp(horizon).toMillis` then
    `complete(Unknown)` (cancels the task) instead of sending.
  - `TransactionManager.runTransaction` returns `Future[ClientOutcome]`.
  - `TransactionImpl`: `Committed` → as today; `Aborted` → as today; `Unknown` →
    `promise.failure(TransactionOutcomeUnknown(txd))`, no `txStatusCache` call.
  - `SimpleAspenClient`: pass the horizon through (default constant).
  - Callers: `StopRetrying.unlessCollision` adds `case _: TransactionOutcomeUnknown => Future.unit` and its scaladoc
    says commits fail transiently with an abort or an unknown outcome; `SimpleTaskExecutor.allocateTask.onFail` treats
    it like an abort, with a comment that a committed-but-unknown attempt leaves a duplicate task (as a false abort
    did); `DiskCatalog.deleteContents` maps it to `Fenced`.
  - `Transaction.commit` scaladoc: remove the "one accepted exception" paragraph; describe
    `TransactionOutcomeUnknown` (may have committed or aborted; no new instance can change it; an instance already in
    progress at stores may still resolve; retry helpers retry it).
  - Update `client/CLAUDE.md` (new error, retry default) and `client/internal/CLAUDE.md` (driver give-up / fail fast).

- [ ] **Step 4: Run the new and existing client tests**

  Run: `sbt 'testOnly *ClientTransactionDriverSuite *StopRetryingSuite org.aspen_ddp.aspen.client.*'`
  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add -A src/main/scala/org/aspen_ddp/aspen/client src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionHorizon.scala \
    src/main/scala/org/aspen_ddp/aspen/compute src/main/scala/org/aspen_ddp/aspen/vdisk src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala src/test
  git commit -m "Client: TransactionOutcomeUnknown and give-up past the transaction horizon"
  ```

---

### Task 2: Status cache outlasts the horizon; synchronous eviction watermark

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionStatusCache.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` (~60-67, ~189)
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` (`HostOptions`, ~138)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/transaction/TransactionStatusCacheSuite.scala`
- Test: a `HostSuite`-level or plain unit test for the duration formula (put it in `TransactionStatusCacheSuite` if no
  Host unit suite exists)

**Interfaces:**
- Consumes: `TransactionHorizon.statusCacheDuration`, `TransactionHorizon.MaxStartLead` (Task 1).
- Produces:
  - `class TransactionStatusCache(cacheDuration: FiniteDuration = 30.seconds, maxEntries: Long = DefaultMaxEntries,
    wallClockMillis: () => Long = () => System.currentTimeMillis())`
  - `def evictionWatermarkMillis: Long` — 0 until a size eviction; then `wallClockMillis() + MaxStartLead.toMillis` at
    the latest size eviction (monotone). Set inside Caffeine's `evictionListener` (synchronous, atomic with removal);
    logs an error and increments `sizeEvictions: Long` (exposed for tests).
  - `Host.txStatusCacheDuration(heartbeatPeriod: Duration, horizon: FiniteDuration): FiniteDuration` with the
    `require` from Global Constraints.
  - `Host` constructor parameter `transactionHorizon: FiniteDuration = TransactionHorizon.Horizon`;
    `TestNetwork.HostOptions(transactionHorizon: FiniteDuration = TransactionHorizon.Horizon)` passed through.

- [ ] **Step 1: Write the failing tests**
  - `"a size eviction raises the eviction watermark to the eviction time plus MaxStartLead"`: cache with
    `maxEntries = 1`, injected clock `c`; write two entries, `cleanUpForTest()`; `evictionWatermarkMillis shouldBe
    c + 2000`, `sizeEvictions shouldBe 1`.
  - `"an expiry by time does not raise the watermark"`: short `cacheDuration`, advance time past it (use Caffeine's
    `Ticker` if injected, otherwise a 50 ms duration and `cleanUpForTest()`), watermark stays 0.
  - `"evictForTest raises the watermark as a size eviction does"` — keep `evictForTest` usable by later tasks: make it
    apply the same watermark update.
  - `"the host status cache outlasts horizon, MaxStartLead and the idle self-drive"`:
    `Host.txStatusCacheDuration(1.second, 20.seconds) shouldBe 30.seconds`; and the `require` fails for a hand-built
    duration below the bound (test the helper that checks it).

- [ ] **Step 2: Run, confirm failure**

  Run: `sbt 'testOnly *TransactionStatusCacheSuite'` — Expected: compile errors / FAIL.

- [ ] **Step 3: Implement** as in Interfaces (Scaffeine `.evictionListener` with `RemovalCause.SIZE`; if Scaffeine's
  version lacks it, use `cache.underlying` construction via `Caffeine.newBuilder().evictionListener(...)`).

- [ ] **Step 4: Run** `sbt 'testOnly *TransactionStatusCacheSuite *ReplayIntegrationSuite *UnackedCommitsIntegrationSuite'`
  — Expected: PASS (the integration suites only see a longer cache; if one waited for expiry, re-time it with a short
  `transactionHorizon` in its `HostOptions`).

- [ ] **Step 5: Commit** — `git commit -m "Status cache outlasts the transaction horizon; synchronous eviction watermark"`

---

### Task 3: The forget floor

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (`receivePrepare` ~817-865, constructor)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala` (constructor ~30-58, driving check ~185-189)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/StoreLoader.scala` (~332; verify transfer-in also constructs
  its `Store` here — if a transfer-in path builds a `Store` elsewhere, pass the same values there)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/StoreSuite.scala`
- Docs: `server/store/CLAUDE.md`

**Interfaces:**
- Consumes: `TransactionStatusCache.evictionWatermarkMillis` (Task 2), `TransactionHorizon.Horizon` (Task 1).
- Produces:
  - Frontend constructor params `horizon: FiniteDuration = TransactionHorizon.Horizon` and
    `loadedAtMillis: Long` (defaults to `wallClockMillis()` at construction; Store construction covers restart and
    transfer-in because both build a new Store).
  - `def forgetFloorMillis: Long = max(wallClockMillis() - horizon.toMillis, loadedAtMillis,
    statusCache.evictionWatermarkMillis)`.
  - `def belowForgetFloor(txd: TransactionDescription): Boolean = txd.startTimestamp.wallTime < forgetFloorMillis`.
  - `Store` passes `horizon` through and does not call `driveTransaction` for a prepare with
    `frontend.belowForgetFloor(m.txd)` (alongside `startsTooFarAhead`).

- [ ] **Step 1: Write the failing tests** (FrontendSuite synchronous harness; `withPlainFrontend` / explicit
  `new Frontend(..., wallClockMillis = () => clock, horizon = …, loadedAtMillis = …)`):
  - `"a prepare for an unknown transaction older than the horizon creates no Tx, sends nothing and saves nothing"`:
    `startTimestamp = ago(21 s)`; `frontend.transactions.contains(txid1) shouldBe false`, `net.sentTx shouldBe empty`,
    `crl.txSaved shouldBe false`, no locks.
  - `"a prepare for an unknown transaction started before the store loaded is refused"`: `loadedAtMillis = clock`,
    start `clock - 1000`.
  - `"a prepare below the eviction watermark is refused"`: status cache with an eviction (via `evictForTest`) after T's
    start; refused.
  - `"a prepare inside the horizon after load is answered as today"`: start `ago(19 s)`, `loadedAtMillis` earlier →
    vote as today.
  - `"a recovered Tx older than the floor still answers its peers' prepares"` (Review Focus 2): recovered Tx from CRL
    state with old start; a driver prepare gets its promise/vote.
  - `"a prepare below the floor with a resolved status-cache entry still learns the outcome"` (Review Focus 3).
  - `"a refused prepare from a client clock slower than the horizon logs a warning with the lag"` (Review Focus 5):
    capture via the suite's existing log-capture pattern if one exists; otherwise assert through a counter exposed
    for tests (`refusedBelowFloor: Long`).
  - StoreSuite: `"the designated leader starts no driver for a prepare below its forget floor"`.

- [ ] **Step 2: Run, confirm failure** — `sbt 'testOnly *FrontendSuite *StoreSuite'`.

- [ ] **Step 3: Implement.** In `receivePrepare`'s `case None` branch (and before the finalized-relay guard only where
  no cache entry exists): if `known.isEmpty && belowForgetFloor(m.txd)` then release any admission taken
  (`releaseAdmission`), log at warn when `m.fromClient` (lag = `wallClockMillis() - start`), and return without
  creating a Tx. The `acceptorSince` and `tooFarAhead` mute paths are unchanged.

- [ ] **Step 4: Fix the Review Focus 1 tests.** Re-age every existing FrontendSuite/StoreSuite/integration test whose
  prepares start older than 20 s (search for `ago(`, `-3600`, `FiniteDuration(3600`), keeping each test's intent; for
  rebuilt-store tests, keep `acceptorSince` inside the horizon.

- [ ] **Step 5: Run** `sbt 'testOnly org.aspen_ddp.aspen.server.*'` — Expected: PASS.

- [ ] **Step 6: Update `server/store/CLAUDE.md`** (floor components, refusal = no Tx, cache-known exception) and commit:
  `git commit -m "Forget floor: no store joins an unknown transaction older than the horizon, its load or an eviction"`

---

### Task 4: Status query answers from every store

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala` (~344-353)
- Modify: `src/main/protobuf/codec.proto` (`TxStatusResponse`, ~290)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (~669 onward)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala` (`receiveStatusRequest` ~1148)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (~813)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala` (route `TxStatusResponse` to the Frontend)
- Test: `FrontendSuite.scala`, the Codec round-trip suite (`grep -rl "TxStatusRequest" src/test` or the general codec
  suite)

**Interfaces:**
- Consumes: `Frontend.belowForgetFloor` (Task 3).
- Produces:
  - `TxStatusResponse.TxStatus(status: TransactionStatus.Value, finalized: Boolean, late: Boolean, holdsAcceptorState: Boolean)`.
  - `TxStatusResponse.status = None` means "unknown: not held and below my floor"; sent only then.
  - Replies from a Tx: `holdsAcceptorState = !muted` (a muted Tx holds none).
  - Replies from a store without a Tx but with a non-Unresolved cache entry: that status, `finalized`/`late` from the
    entry, `holdsAcceptorState = false`.
  - Above the floor and unknown: no reply.
  - `TxStatusRequest(to, from, transactionId, requestUUID, startTimestamp: HLCTimestamp)`: the new field (and proto
    field) lets a store without a Tx apply the below-floor test; `Frontend.belowForgetFloor` gets an overload taking
    an `HLCTimestamp`.

- [ ] **Step 1: Write the failing tests**
  - Codec: round-trip `TxStatusRequest` with `startTimestamp` and `TxStatusResponse` with each of `None`,
    `holdsAcceptorState = true/false`.
  - FrontendSuite: `"a store holding T answers a status request with its status and acceptor state"`;
    `"a store with only a cached outcome answers with it"`; `"a store that neither holds nor remembers T answers
    unknown only below its floor"` (two cases: start `ago(21 s)` → `None`; start `ago(5 s)` → no message);
    `"a muted Tx answers with holdsAcceptorState = false"`.

- [ ] **Step 2: Run, confirm failure** — `sbt 'testOnly *FrontendSuite *Codec*'`.

- [ ] **Step 3: Implement** the message, proto field, codec, and the Frontend no-Tx branch.

- [ ] **Step 4: Run** `sbt 'testOnly *FrontendSuite *Codec* *Message*'` — Expected: PASS.

- [ ] **Step 5: Commit** — `git commit -m "TxStatusRequest answered by every store; unknown only below the forget floor"`

---

### Task 5: Expiry tally and expiry of unresolved Txs

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/transaction/ExpiryTally.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala` (`needsUnackedCheck` ~170-186, new expiry
  state, discard)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (handle `TxStatusResponse`; discard path;
  `requestUnackedLookup`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala` (`heartbeat` ~78-88: send queries on backoff;
  remove drivers past the horizon once their Tx expired or when the store holds no Tx)
- Create: `src/test/scala/org/aspen_ddp/aspen/server/transaction/ExpiryTallySuite.scala`
- Test: `FrontendSuite.scala`, `StoreSuite.scala`
- Docs: `server/transaction/CLAUDE.md` (expiry, the rejoin invariant), `server/store/CLAUDE.md`

**Interfaces:**
- Consumes: `TxStatusRequest`/`TxStatusResponse` (Task 4), `Frontend.belowForgetFloor` (Task 3).
- Produces:
  - `object ExpiryTally { enum Decision { case Adopt(committed: Boolean, finalizedOrLate: Boolean); case Discard; case Hold }
    def decide(txd: TransactionDescription, replies: Map[StoreId, Option[TxStatusResponse.TxStatus]]): Decision }`
    — `replies` holds only stores that replied (absent = silent). Rules, in order: any reply with a non-Unresolved
    status → `Adopt`; otherwise count primary-pool (`txd.primaryObject.poolId`) stores whose reply is `None` or
    `Some(_, holdsAcceptorState = false)` as non-holders; `Discard` iff non-holders `> width - writeThreshold` of
    `txd.primaryObjectIDA`; else `Hold`. Pure; the UnackedCommits check is the caller's.
  - `Tx.expiring: Boolean`; `Tx.pastHorizon(nowMillis: Long, horizon: FiniteDuration, margin: FiniteDuration): Boolean`.
  - Expiry margin (designer's call within the spec's bounds): `MaxStartLead + txHeartbeatTimeout + heartbeatPeriod`.
  - Query backoff: first at the heartbeat after the Tx becomes past horizon + margin, then doubling to a cap of
    `8 * heartbeatPeriod`; each query round uses a fresh `requestUUID` and counts only its own replies.
  - `needsUnackedCheck` becomes `(recovered || expiring) && disposition == VoteCommit && txStatus == Unresolved &&
    oresolution.isEmpty`.
  - On `Discard` for a commit voter: request a lookup; on `Success(false)` from a lookup started after the Discard
    decision → discard; `Success(true)` → `unackedCommitFound()` as today; `Failure` → keep holding, retry
    (Review Focus 4). For an abort voter / no vote / muted Tx: discard at once.
  - Discard = release locks and objects, `crl` state deleted as for a forgotten abort, removed from `transactions`,
    status cache untouched (no outcome is known).

- [ ] **Step 1: Write `ExpiryTallySuite`** (pure, no harness), with `ReedSolomon(5, 3, 4)`-style IDA where
  `w = 5, t = 3` so `w − t = 2`:
  - `"any outcome-bearing reply is adopted"` (committed+late → `Adopt(true, true)`; aborted → `Adopt(false, _)`).
  - `"three explicit primary-pool non-holders discard"`; `"two non-holders and three silent stores hold"`;
    `"holders that cannot form a quorum with silent stores discard"` (the user's corner case: 2 holders, 3 unknown →
    Discard); `"three holders keep holding"`; `"non-holders in a secondary pool do not count"`;
    `"a muted Tx's reply counts as a non-holder"`.
- [ ] **Step 2: Run, confirm failure** — `sbt 'testOnly *ExpiryTallySuite'`.
- [ ] **Step 3: Implement `ExpiryTally`.** Run again — PASS.
- [ ] **Step 4: Write the wiring tests** (FrontendSuite / StoreSuite with an injected clock and short horizon):
  - `"an abort voter past the horizon whose peers answer unknown is discarded and releases its locks"`.
  - `"a commit voter past the horizon with no UnackedCommits entry is discarded once a later lookup finds none"`.
  - `"a commit voter past the horizon with an UnackedCommits entry applies and resolves committed"`.
  - `"a commit voter keeps holding while its UnackedCommits lookup fails"` (Review Focus 4).
  - `"a Tx past the horizon with a quorum of holders keeps holding and later learns the outcome"` (spec test 8).
  - `"after expiry, a later prepare for T creates no Tx"` (rejoin invariant, spec test 11).
  - StoreSuite: `"a driver past the horizon is removed once its store's Tx expired"`.
- [ ] **Step 5: Run, confirm failure; implement the wiring; run** `sbt 'testOnly org.aspen_ddp.aspen.server.*'` — PASS.
- [ ] **Step 6: Update `server/transaction/CLAUDE.md`** (expiry rule, rejoin invariant stated verbatim) and
  `server/store/CLAUDE.md`; commit: `git commit -m "Expiry: an unresolved Tx past the horizon decides from its peers' status"`

---

### Task 6: End-to-end horizon tests

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/HorizonIntegrationSuite.scala` (model it on
  `ReplayIntegrationSuite`: `IntegrationTestSuite`, `recordCrl = true`, `recoveryDrivers = true`, short
  `heartbeatPeriod`, and a short `transactionHorizon`, e.g. 1 s, so the suite stays fast)
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/ReplayIntegrationSuite.scala` where a test encodes the old F2 or
  P-N4 outcome (update the expectation; do not delete the test)

**Interfaces:**
- Consumes: everything above; `TestNetwork.HostOptions.transactionHorizon` (Task 2).

- [ ] **Step 1: Write the tests** (spec tests 1-3, 9, and 6 end-to-end):
  - `"a stale retransmit of a finalized traceless commit past the horizon reports unknown, not aborted"` — refcount
    pin; re-deliver the client prepare after the horizon; no new Tx anywhere; the client future fails with
    `TransactionOutcomeUnknown`; the object still shows the commit.
  - `"a delayed client prepare of a forgotten abort does not commit inside or past the horizon"` (P-N4 path 3).
  - `"a store first reached near the horizon after finalization learns the outcome from its peers' caches"`.
  - `"stuck holders short of a quorum all discard"` (corner case end-to-end).
  - `"a host restart does not let its stores rejoin a transaction they forgot"`.
  - `"a store transferred to another host does not rejoin a transaction it forgot"` (use the existing transfer test
    helpers; if none can be driven within the suite, cover it in StoreSuite by asserting the transferred-in Store's
    `loadedAtMillis` is its construction time on the destination host).
- [ ] **Step 2: Run, confirm the expected failures on the parts not covered by unit tests** (most should pass if
  Tasks 1-5 are right; any failure is a real gap — fix it in the owning code, not the test).
- [ ] **Step 3: Update `ReplayIntegrationSuite` expectations** that assumed today's outcomes.
- [ ] **Step 4: Full run** — `timeout 70 sbt test` (repeat once to catch flakes; investigate any failure, CPU at
  100 % allowed). Expected: all PASS, twice.
- [ ] **Step 5: Runs under 100 % CPU load.** Short test horizons (about 1 s) must not turn scheduling delays into
  refusals, give-ups or expiries. Saturate the machine with busy loops, 24-48 on 24 cores (`load.sh N` if it exists
  in the repo root or scratchpad; otherwise
  `for i in $(seq N); do (while :; do :; done) & done`, and kill them afterwards). Then run the full suite at least
  **twice** (`sbt test`; one sbt JVM at a time, since test ports collide), plus one run pinned with
  `taskset -c 0-2 sbt test`. Under load the 70 s hang budget does not apply. Expected: every run passes.
  - Any failure in a suite touched by Tasks 1-6 is treated as a real flake. Fix it by deriving the test's horizon
    and waits from what the test needs, or by driving time explicitly (an injected clock), never by adding sleeps.
  - Then repeat the loaded runs until two consecutive runs pass.
  - Record the run count and outcomes in the commit message.
- [ ] **Step 6: Commit** — `git commit -m "HorizonIntegrationSuite: end-to-end horizon, floor and expiry"`

---

### Task 7: Safety documents and Todo

**Files:**
- Modify: `docs/safety/paxos-transactions.md`, `docs/safety/transaction-recovery.md`, `docs/safety/README.md`,
  `docs/safety/cross-document-review.md`, `Todo.txt`,
  `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionStatusCache.scala` (scaladoc)

- [ ] **Step 1: Update each finding** with a dated note (landing date), following the spec's "Documentation" list
  exactly: paxos P16, P17, 6.2 cross-reference, a new horizon/expiry section carrying the spec's safety arguments
  (floor timing, rule 3, rejoin invariant); F2 closed; F6 re-assessed (state any residual); F10 and P-N1 "closed by the
  horizon (not fixed directly)"; P-N4 closed; P-F15 wired up; recovery F3, F5, F8; README ranked rows; any
  cross-document rows citing F2, P-N1, P-N4, G4. Update line citations you touch to current code.
- [ ] **Step 2: `Todo.txt`**: mark 1.3 done (1.3a and 1.3c dropped, closed by the horizon); close 2.15; note 2.13 F6's
  re-assessment; add a priority-5 entry: "Audit the `unlessCollision` sites and `host/StoreRebuilds.scala` ~118 for
  idempotence when a retried `TransactionOutcomeUnknown` had actually committed (`SimpleTaskExecutor.allocateTask` is
  known to duplicate)."
- [ ] **Step 3: `TransactionStatusCache` scaladoc**: opposite outcomes "should never arise" now; log an error when
  `updateStatus` sees one (add the log line; covered by an added `TransactionStatusCacheSuite` test
  `"an opposite outcome is logged as a split decision"` using the suite's log capture or a counter).
- [ ] **Step 4: Check** every id the spec lists is updated:
  `grep -n "P-N1\|P-N4\|F2\b\|F10\|P-F15\|R-F3\|F5\b\|F8\b" docs/safety/*.md` and read each hit.
- [ ] **Step 5: Commit** — `git commit -m "docs/safety: transaction horizon closes F2, P-N1, P-N4; Todo 1.3 done"`
