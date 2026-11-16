# Transaction horizon: no store joins a transaction it may have forgotten

Date: 2026-11-13. Addresses Todo.txt item 1.3 (paxos P-N1, P-F2, P-F10; transaction-recovery R-F3, R-F5, R-F8) and
closes the accepted corner case P-N4 path 3 (Todo 2.15).

## Intent and why

Stores remember a transaction's outcome only for a bounded time: a Tx is dropped at resolution (abort) or
TxFinalized (commit), after which only the host's in-memory `TransactionStatusCache` (7 s today) knows it. A prepare
that arrives after every store has forgotten T starts a brand-new Paxos instance for the same txid. That instance can
decide the opposite of the original outcome:

- **Commit re-decided as abort** (paxos F2, R-F8, 1.3b). A stale client retransmit after finalization finds memoryless
  acceptors; with no proof (refcount pin, overwritten pin, proof lost per P-N1) the tally aborts and the client is told
  "aborted" for a committed transaction. A live commit voter that learns this abort drops its update (R-F3).
- **Abort re-decided as commit** (P-N4 path 3). A delayed duplicate of the client's own prepare reaches stores that
  forgot an abort, and they vote commit afresh.

Neither can be closed by better proof delivery: forgetting is the root cause, and infinite memory is not available.
The fix is a time bound on joining: past a **horizon**, no store creates a Tx for a transaction it does not already
hold, and stores remember outcomes for longer than the horizon. Inside the window every store that knew T either still
remembers it or refuses to join, so no driver ever counts votes from acceptors that forgot T. A client that cannot learn
the outcome within the window gets a new, honest result: **outcome unknown**.

Background analysis: `docs/safety/paxos-transactions.md` (P15-P17, 6.2, F2, F6, F10, P-N1, P-N4),
`docs/safety/transaction-recovery.md` (F3, F5, F8).

## Goals

1. No transaction is ever decided twice with different outcomes because of forgotten acceptor state.
2. `TransactionAborted` becomes final with no exceptions; the `Transaction.commit` scaladoc's accepted exception is
   removed.
3. Every in-memory Tx and driver ends, by Paxos resolution or by expiry. Nothing is held in memory, in the CRL, or as
   locks forever because peers have forgotten the transaction.
4. Existing callers keep working: "unknown" is retried by default wherever aborts are retried today.

## Non-goals

- Item 1.3a (proof caching and re-sending, P-F10's `lastProposer` overwrite) and 1.3c (traceless hold for live commit
  voters). Both become unnecessary under the horizon; they are **dropped** and marked "closed by the horizon" in the
  safety docs, not implemented.
- Durable outcome tombstones. Outcome memory stays in memory (decided: option 1 plus the restart rule).
- Changing Paxos rounds, proposer ids, or other 2.13 hardening items.
- Distinguishing "unknown" in callers beyond the minimum below. Callers that later want the distinction (see
  `SimpleTaskExecutor.allocateTask`) are future work.

## Anti-goals (a failure even if it technically works)

- A Tx, driver, lock or CRL record that can live forever because no peer answers.
- A store that forgets T and later joins T again (rejoin after forgetting is the whole bug).
- Ordinary, healthy transactions failing as "unknown" or aborting because of the horizon. Only transactions already
  pathologically late (client paused or partitioned for ~20 s) may be affected.
- Behaviour change at the 25 `StopRetrying.unlessCollision` call sites.

## Decided parameters

- **Horizon: 20 s**, measured on the transaction's `startTimestamp` against the receiving host's wall clock (the same
  clock `startsTooFarAhead` uses, `Frontend.wallClockMillis`).
- **Status cache duration: 30 s** (up from 7 heartbeats = 7 s). It must outlast horizon + `MaxStartLead` + the idle-Tx
  self-drive delay (`Host.txHeartbeatTimeout` + one heartbeat, ~6 s) so that a Tx first created near the horizon still
  finds its peers' cache entries when its idle driver starts. Builder's call: whether to express it as
  `max(30 s, 7 * heartbeatPeriod)` or as a fixed constant with a `require` checking the inequality against the
  configured heartbeat; either way the inequality must be asserted in code and in a test.
- **Outcome-unknown is a sibling** of `TransactionAborted`, not a subtype.
- **Unknown is retried by default**; callers distinguish it when they need to.

## Design

### 1. The forget floor (server, store side)

Generalise `Frontend.acceptorSince` into a per-store **forget floor**. A prepare for a transaction the store does not
hold (no Tx in `transactions`, no non-Unresolved status-cache entry) whose `startTimestamp` is below the floor is
refused. Refused means: **no Tx is created at all**, no promise, no vote, no reply. (This differs from today's
`acceptorSince` mute, which creates a muted Tx that may prove commit; see 5.) The floor is the maximum of:

- `now − horizon` (wall clock),
- the time the store was loaded on this host (restart / crash recovery),
- the time the store was transferred in (adoption or transfer from another host, whose cache this host lacks),
- the host's **eviction watermark**: the largest `startTimestamp` of any status-cache entry evicted for size
  (a Scaffeine removal listener with cause SIZE; expiry by time needs no watermark because expiry happens only after
  the horizon has passed for that entry). The watermark is host-wide since the cache is. The cache entry must therefore
  carry the transaction's start timestamp (or the listener must otherwise obtain it).
- `acceptorSince` for a rebuilt store (existing).

The rebuilt-store mute (`acceptorSince`) keeps its current semantics, including creating a muted Tx that may prove.
Builder's call whether to keep the two paths separate or fold them; the refusal semantics for the new floor
components above are fixed: no Tx.

The designated leader must not start a driver for a prepare below the floor (`Store.handleTransactionMessage`, next to
the `startsTooFarAhead` check). The finalized-relay answer from the status cache (`Store.scala` ~162-185) and an
existing Tx's handling are unchanged: the floor applies only to transactions the store does not hold.

**Why the timing is safe without cross-host clock agreement.** A store writes its cache entry for T after it saw T, and
by `MaxStartLead` its clock then read at least `start − 2 s`. The entry lives 30 s past its last write, so by the time
it expires the store's own clock reads past `start + 28 s`, beyond the 20 s horizon: the store already refuses T.
Restart, transfer and eviction raise the floor at the moment memory is lost. So a store never forgets T while it would
still join it.

### 2. Expiry (server, store side)

Under the floor, a Tx whose peers have forgotten T gets no answers from them, so without a further rule it would wait
forever (today it is driven to an abort, often a false one, F2). Expiry decides such a Tx from its peers' state, which
they report on request. The design is the user's: ask the peers, and discard only when they prove the transaction can
no longer be decided.

**Status query.** Reuse the existing, half-wired `TxStatusRequest` / `TxStatusResponse` (`common/network/Message.scala`
~337-352, `Tx.receiveStatusRequest`, `Frontend.scala` ~813; this also closes P-F15's "delete or wire up"). They are
store-addressed TxMessages, which keeps logical addressing; no HostMessage is added. Changes:

- A store **with a Tx** for T answers as today (status, finalized, late), plus whether it holds Paxos acceptor state
  (a muted Tx holds none and `receiveAccept` ignores accepts while muted).
- A store **without a Tx** now answers too: with the outcome if its status cache has a non-Unresolved entry; otherwise
  with "unknown" (`status = None`) **only if T's `startTimestamp` is below its forget floor**. Above the floor it
  could still create a Tx for T, so it does not answer (or answers "not yet", counted as a possible holder; builder's
  call). This condition is what makes an "unknown" reply permanent.

**Trigger.** A Tx still unresolved when T is past the horizon plus a margin sends the query to the primary pool's
stores (and, builder's call, to every store of the transaction, since any outcome-bearing reply is useful), repeating on
a backoff until it decides. The margin covers `MaxStartLead` and the idle self-drive delay. Recovered Txs follow the same
rule; the query is harmless for a Tx whose peers still answer, because only the replies decide.

> **Superseded in part (2026-11-15, cross-document G1).** Rules 2-4 below were replaced: past expiry every Tx is
> frozen, replies carry the latest accept, the vote and `amnesiac`, and the tally commits, discards or holds by the
> rules in `server/transaction/CLAUDE.md` (Expiry) and paxos-transactions 10.10. The original rule 3 discarded a
> chosen commit when a rebuilt store answered "unknown".

**Decision, from the replies:**

1. **Any reply carries an outcome** (from a Tx or a status cache): adopt it. Committed takes the normal resolved path,
   or the late-commit path (UnackedCommits lookup) when the outcome is finalized/late; aborted goes through the
   existing abort handling.
2. Otherwise, classify the **primary pool's** stores: a **holder** has a Tx with acceptor state for T; a **non-holder**
   replied "unknown" or holds only a muted Tx; a **silent** store is counted as a possible holder, never a non-holder.
3. **Discard** when the non-holders number more than `w − t` of the primary pool (`txd.primaryObjectIDA`: the driver's
   Proposer and Learner are built with `(ida.width, ida.writeThreshold)` over that pool, `TransactionDriver.scala`
   ~102-104, ~186) **and** this store's UnackedCommits lookup finds no entry (for a commit voter; generalise
   `needsUnackedCheck`, today gated on `recovered`, to expiring live Txs). An entry means the store owes T: apply it
   and resolve committed. Discard means: release locks, delete CRL state, remove from `transactions`.
4. Otherwise **keep holding**: enough possible holders remain to form a quorum, so a driver can still decide T and a
   TxFinalized will eventually arrive. Re-query on the backoff.

**Why rule 3 is safe.** An acceptor forgets T only after T is decided: an abort at resolution, a commit only at
TxFinalized (commits keep their Paxos record until then). So a committed but unfinalized T is still held by a full
quorum (`t`) of primary-pool acceptors, and more than `w − t` non-holders rule that out. Non-holders can never become
holders, because they answered "unknown" only below their floor. So T is one of: aborted; committed and finalized, in
which case the finalizer wrote this store's UnackedCommits entry before TxFinalized if it was owed (and the lookup found
none, so it was not; ErrorTree marks cover a store whose update was not counted); or undecided and now undecidable,
since no quorum can ever form past the horizon. Discarding is correct in all three.

**The user's corner case** (several stuck stores showing T as in progress to each other) needs no special state: they
report themselves honestly as holders, and they are stuck precisely because they cannot form a quorum. If they could,
their own drivers would decide T normally, since holders answer drivers.

**Drivers** for a transaction past the horizon that have no outcome are stopped and removed from
`Store.transactionDrivers` once their store's Tx decides by expiry (or immediately if the store holds no Tx). A driver
holds no durable state, so dropping it is safe.

**Rejoin invariant.** Once a store discards T, its floor makes it refuse every later prepare for T: a discarded acceptor
is indistinguishable from an offline one, and Paxos is safe with offline acceptors. **A store never rejoins a
transaction it forgot.** State it in `server/transaction/CLAUDE.md` and test it.

### 3. Client side

- New error: `final case class TransactionOutcomeUnknown(txd: TransactionDescription) extends TransactionProcessingError`
  in `client/TransactionError.scala`, sibling of `TransactionAborted`. Scaladoc: the transaction may have committed or
  aborted and nothing can now find out; it will never change *because of a new instance*, but an instance already in
  progress at stores may still resolve.
- `SimpleClientTransactionDriver` stops retransmitting once the transaction is past horizon + `MaxStartLead` (client
  HLC against `txd.startTimestamp`; builder's call on an extra margin) and completes with "unknown". It needs a third
  completion state; `ClientTransactionDriver.complete(committed: Boolean)` and `TransactionManager.runTransaction`'s
  `Future[Boolean]` need widening (builder's call on the shape, e.g. a small sealed outcome type).
- `TransactionImpl` (~line 385-400) maps the unknown outcome to `promise.failure(TransactionOutcomeUnknown(txd))`, and
  records nothing definite in `client.txStatusCache` (neither committed nor aborted).
- `Transaction.commit` scaladoc: remove the "one accepted exception"; describe `TransactionOutcomeUnknown`.

### 4. Callers (retry by default)

| Site | Change |
|---|---|
| `client/StopRetrying.unlessCollision` | add `case _: TransactionOutcomeUnknown => Future.unit`; update its scaladoc (commits now fail transiently with abort *or* unknown) |
| `compute/impl/SimpleTaskExecutor.allocateTask` | treat unknown like abort (refresh revision, retry). Keeps today's duplicate-task risk; note it in a comment |
| `vdisk/DiskCatalog.deleteContents` | map unknown to `Fenced`, like abort |
| `compute/SteppedDurableTask` | none required (its `case _` already retries); optionally a log line |
| `server/repair/RepairProofs.prove` | none required (`NonFatal` returns false); optionally a debug-level case |
| `AspenClient.transactUntilSuccessful` | none (retries every failure) |

### 5. Interaction with existing mechanisms

- **Proofs (P15).** Unchanged and still useful inside the window, but no longer load-bearing. Past the floor no Tx is
  created, so no proof is produced there; a proof-holding driver past the horizon could not gather promises anyway.
- **Prefer-abort (6.2), `MissingDataWait`, `MaxStartLead`, `acceptorSince`.** Unchanged.
- **Status-cache monotonicity (Ruling 30).** Unchanged. Its scaladoc's "opposite outcomes arise only from a split
  decision" becomes "should never arise"; consider logging an error when it happens (overlaps 2.13 F11; builder's
  call).
- **Host restart (G4).** Covered by the restart component of the floor.
- **Post-restart refusal window.** For up to 20 s after a store loads, it refuses transactions started before the
  load. Those see the store as offline; most still commit through the other stores.

## Tests (deterministic, LockstepThreading where possible)

At minimum, each as a test that fails before the change:

1. Commit finalized and forgotten; a stale client retransmit at start + > 20 s: no store creates a Tx, no outcome is
   sent, the client gets `TransactionOutcomeUnknown` (replaces the F2 false abort; use a traceless or overwritten pin).
2. P-N4 path 3: abort resolved; delayed client prepare reaches the other stores after their memory would have expired
   under the old 7 s cache, but inside 20 s: it does not commit. And past 20 s: refused.
3. Store first sees T at start + ~19 s after T finalized elsewhere: its idle driver gets the outcome from the caches
   (the 30 s inequality).
4. Restart floor: a store reloaded after forgetting T refuses a prepare for T started before its load.
5. Transfer-in floor: same for a store transferred to another host.
6. Eviction floor: force a size eviction (`evictForTest`) and show the watermark refuses T.
7. Expiry: a live Tx whose primary-pool peers forgot T (abort voter, non-voter, commit voter with and without an
   UnackedCommits entry) ends, releasing locks and CRL state; a driver past the horizon is removed.
8. Expiry holds: with `t` primary-pool stores still holding T (committed, not finalized), an expiring Tx keeps holding
   and later learns the outcome; with silent stores making up the difference, it keeps holding too.
9. The corner case: several stuck holders, fewer than a quorum, plus more than `w − t` non-holders: all of them
   discard; none keeps the others alive.
10. A store above its floor never answers "unknown" to a status query.
11. Rejoin invariant: after expiry, a later prepare for T creates no Tx.
12. Callers: `unlessCollision` retries unknown; `DiskCatalog.deleteContents` restarts on it.

Existing suites that assert today's F2/P-N4 behaviour (e.g. ReplayIntegrationSuite cases built on stale retransmits)
must be revisited; a test that encodes the old wrong outcome is updated, not deleted silently.

## Documentation (part of this task)

`docs/safety` updates, each with a dated note (2026-11-13 or the landing date) per the Todo working rule:

- `paxos-transactions.md`: P16 ("no age test left") and P17 (cache duration, the new floor); 6.2 cross-reference; new
  section for the horizon and expiry with the safety argument above; F2 → closed; F6 → re-assess (a forgotten abort is
  no longer re-decidable; state what remains, if anything); F10, P-N1 → closed by the horizon (not fixed directly);
  P-N4 → closed; P-F15 → status request wired up; ranked table rows.
- `transaction-recovery.md`: F3, F5 (the ~1 s takeover margin becomes 30 s vs ~6 s), F8 → closed or narrowed with the
  residual stated; ranked rows.
- `README.md`: ranked rows for paxos F2 / recovery F3, F8; P-N1; P-N4; any X/G rows in `cross-document-review.md` that
  cite these.
- `cross-document-review.md`: rows citing F2, P-N1, P-N4, G4 (restart now covered).
- `Todo.txt`: 1.3 marked done with what was dropped and why; 2.15 closed; 2.13 F6 note.
- Module guides: `server/transaction/CLAUDE.md` (floor, expiry, rejoin invariant), `server/store/CLAUDE.md` (refusal in
  `receivePrepare`), `client/CLAUDE.md` (new error, retry default), `client/internal/CLAUDE.md` (client driver give-up).
- `Transaction.commit` scaladoc and `TransactionStatusCache` scaladoc.

## Left to the builder

Exact code shape of the floor and of the client outcome type; where the restart and transfer-in times are captured
(store loader / router); the expiry trigger and margin within the constraints in 2; logging levels; whether the
rebuilt-store mute and the new refusal share code.

## Calls made by the designer (not discussed with the user; check these)

1. **Eviction watermark.** Use Caffeine's `evictionListener` (invoked synchronously, atomically with the removal), not
   the asynchronous `removalListener`, so no prepare can see the entry gone before the floor rises. Size eviction needs
   sustained load above ~130 k transactions/s per host (4 M entries / 30 s) and is not oldest-first, so one eviction
   can raise the floor to about "now" and make the host's stores refuse every not-yet-held transaction started before
   it. Accepted as overload behaviour: it fails safe (those stores look offline), and it is logged as an error with a
   counter. `DefaultMaxEntries` stays.
2. **Expiry trigger** is "unresolved past horizon + margin", with decisions taken only from replies (section 2), so
   slow-but-answering quorums, lock waits and recovered Txs whose peers still hold T keep holding.
3. **Slow client clocks.** A client whose clock runs more than 20 s slow has its transactions refused. Intended; the
   store logs a warning with the measured lag, as `tooFarAhead` does.
4. **Client fails fast.** If a transaction is already past horizon + `MaxStartLead` when the client would first send
   it, the client completes with `TransactionOutcomeUnknown` at once without sending.
5. **No idempotence audit** of the 25 `unlessCollision` sites and `host/StoreRebuilds.scala` ~118 in this task: retry
   by default was decided, and those sites already retry the same cases today, as false aborts. A follow-up Todo entry
   records the audit.
6. The 30 s change lives in `Host.txStatusCacheDuration` (`Host.scala` ~66; `TransactionStatusCache`'s own default is
   already 30 s).
7. Classifying the status query's "not yet" reply above the floor as silence or as an explicit reply is left to the
   builder; both count as a possible holder.
