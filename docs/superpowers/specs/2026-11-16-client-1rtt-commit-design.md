# Client 1-RTT transaction outcome, and parked reads (design)

Todo 8.1 (paxos-transactions P-F1). Date: 2026-11-16. Branch: `paxos_1rtt`.

## 1. Intent

A client should learn its transaction's outcome after one round trip: the prepares go out, and the stores' votes come
back. Today it takes 5 one-way hops and two serial CRL writes (paxos-transactions §5): client → stores (vote saved) →
leader → Accept → acceptors (accept saved) → leader → `TransactionResolved` → client.

How fast the *stores* learn the outcome matters less. The store-to-store protocol (leader tally, Accept, `TxResolved`,
finalization) stays as it is. Avoiding all-to-all store messages is part of why it stays.

Why votes can decide: a store's `VoteCommit`/`VoteAbort` is saved to the CRL with its Paxos state before the store
answers, and it never changes. Two conflicting transactions cannot both get `t` commit votes on one object: the vote
takes locks, and `t` is a majority. So once every object of T has `t` commit votes, the protocol can be made to never
resolve T any other way. Making that true is part 1's server work: today two expiry paths can contradict it (section
4.3).

Part 2 is a separate, smaller change. A client that reads an object it has just written, before the stores apply the
commit, sees the transaction's lock and re-reads at once, repeatedly, until the stores have applied it. With the fast
outcome that window gets longer, by the Accept round. Part 2 has the store hold such a read until the commit is
applied.

## 2. Goals, non-goals, anti-goals

Goals
- The client completes `Committed` once every object of T has at least `t` (its pool's `writeThreshold`) commit
  votes, with no wait for the leader. It learns **abort only from the leader** (`TransactionResolved` /
  `TransactionFinalized`, after Paxos chose it), as today. See section 3 for why there is no fast abort.
- Server rule, from now on an invariant: **if every object of T has `t` commit votes, commit is T's only possible
  outcome.** Recovery and expiry may abort or discard T only when that cannot be the case, or when Paxos already
  chose abort. Otherwise they decide by the rules in 4.3(b), or wait while stores are silent.
- A permanent hold needs more failures than the pool tolerates (`t` or more primary stores rebuilt). A store rebuilt
  mid-transaction must not leave T undecidable.
- Part 2: a re-read caused by a lock of a known-committed transaction waits at the store for that commit to apply,
  instead of spinning.
- The docs describe the protocol that is actually built. Architecture.md, paxos-transactions (§2, §5, P2/P6, F1) and
  the module CLAUDE.md files are corrected, and P-F1 is closed.

Non-goals
- Faster learning at the stores, store-to-store vote exchange, or acceptors self-generating Accepts. The documented
  "commit-only round 1, abort needs round 2" rule is not adopted. The docs are corrected to match the code instead.
- A fast quorum larger than `t`. Fast Paxos needs ⌊2N/3⌋+1 because several values compete in one fast round. Here the
  only value votes can decide is commit, and locks rule out the competing case. A larger threshold would only let
  recovery prove an abort with fewer abort votes. Holding is acceptable, so that gain isn't wanted. (Research note:
  Lamport, "Fast Paxos", MSR-TR-2005-112; the condition N > 2E + F.)
- Proactive read-your-writes, meaning the client keeps an index of objects it recently wrote. Part 2 is reactive only.
- Compatibility with persisted data or older peers (CLAUDE.local.md).

Anti-goals (a failure even if it "works")
- A client told `Committed` (or `Aborted`) for a transaction that any store, driver or expiry later resolves the other
  way. That would be a split decision.
- Any new durable write, or any extra synchronous work, on the transaction hot path. The vote message is sent from
  the same CRL-save completion handler that already sends the prepare response.
- Reads that hang. A parked read always answers within a bounded time.

## 3. Approaches considered

How the vote reaches the client:
- **A. A new client-addressed message, `TransactionVote` (chosen).** It is small, it is clearly a client message, and
  it is routed like `TransactionResolved`.
- B. Copy `TxPrepareResponse` to the client. There'd be no new type, but that message is addressed to a store
  (`to: StoreId`) and carries Paxos promise state the client has no use for. It blurs the store/client split in the
  codec. Rejected.

Client threshold:
- **`t` (chosen)**, the same test as the driver's "commit-certain". It allows the most fast commits.
- ⌊2w/3⌋+1, as in Fast Paxos. It would cost fast commits on Replication(3,2), which would need all 3 votes, and on
  w=5/t=3, which would need 4. It buys only cheaper abort proofs in recovery. Rejected (section 2).

Fast abort (the client completing `Aborted` once some object has more than `w - t` abort votes): **dropped** (the
user's call, after review). With both fast outcomes, one store rebuilt mid-transaction can make T undecidable for good.
Example: Replication(5,4), three commit votes, one abort, one vote lost to a rebuild. If the lost vote was commit, the
client may have been told commit. If it was abort, the client may have been told abort. No expiry rule can then
decide, and the voters' locks would freeze the object. With commit as the only fast outcome, every abort a client
hears was chosen by Paxos, and accepted values tell the observers which history happened (4.3(b)). Aborts were never
the fast path (the client retries them), so the cost is small.

Unmuting rebuilt or silent stores to get a fresh ruling: **rejected**. A rebuild destroys the old vote and accept.
A fresh vote or promise need not match them, and could contradict an outcome already reported or chosen. That is
exactly why rebuilt stores are muted (cross-document G1). Pool metadata isn't needed either: rebuilt stores already
report themselves as `amnesiac`.

What happens when stores are silent at expiry: **hold** (the user's call). The system is designed to freeze below its
thresholds, and failures are expected to be transient.

Read-after-write:
- **Reactive, with the committed transaction id (chosen).** The re-read that `BaseReadDriver` already makes for a
  known-committed lock carries the txid, and the store parks it. A txid covers object revisions, key revisions and
  refcount-only updates the same way.
- Expected revision instead of txid. Rejected: it needs a separate check per kind of update, and refcount-only updates
  don't change the revision at all.
- Proactive, which saves one round trip but needs a client-side object→txid index with expiry. Rejected for now.

## 4. Part 1: client learns the outcome from votes

### 4.1 Message and sending side

- `TransactionVote(toClient: ClientId, fromStore: StoreId, transactionId: TransactionId, disposition:
  TransactionDisposition)`. Add it to `Message.scala`, `codec.proto`, `Codec`, `QuicMessageCodec`, and the client
  dispatch (`BaseAspenClient` → `TransactionManager.receive` → `ClientTransactionDriver.receive`). Follow how
  `TransactionResolved` is wired.
- `Tx` sends it to `txd.originatingClient` (if defined) wherever it sends a `TxPrepareResponse` *after* the CRL save
  that holds the vote. That is, in the same save-completion handler: `answerPrepareAsAcceptor`, the delayed-prepare
  path, and the withheld-response path for driver prepares (`MissingDataWait`). It is sent with a Promise or with a
  Nack alike: the vote is durable either way.
  - Only for `VoteCommit` / `VoteAbort`, never `Undetermined`.
  - Never before the save. This is the safety-critical rule: the client must never count a vote that a crash could
    take back.
  - Re-sent on every such response, so lost votes recover through the client's prepare retransmits, as prepares do.
  - Muted and frozen Txs send nothing (they send no prepare response either). Proofs (`TxResolved(proof = true)`) are
    not votes and are not sent to the client as votes.

### 4.2 Client tally (`ClientTransactionDriver`)

- Keep each store's first vote. A later, different vote from the same store is impossible, since votes are immutable.
  If one arrives, log it as an error (like the existing split-decision log) and keep the first.
- For each object T references, use exactly the object set the server's tally uses (`TransactionDriver`'s
  `allObjects`, lock-only and requirement-only objects included). Over that object's pool stores `0 until w`, count `c`
  commit votes against the pool's `IDA` in `txd.poolIDAMap`.
  - **Committed** once every object has `c >= writeThreshold`.
  - Abort votes complete nothing. Abort comes only from the leader's messages (section 3).
  - Otherwise keep waiting.
- `TransactionResolved` and `TransactionFinalized` from the leader still complete the driver, as now. Whichever comes
  first wins. A disagreement is logged as a split decision, as `complete` already does.
- The tally lives in the base `ClientTransactionDriver`. `SimpleClientTransactionDriver` keeps its retransmit timer
  and its horizon `Unknown`. A vote-completion only completes `result`; `complete` ignores whatever comes later.
  (Builder's call, recorded.)
- **Retransmits continue after a vote-completion** until `TransactionResolved` or `TransactionFinalized` arrives, or
  until the client's give-up horizon (`TransactionHorizon.clientGiveUp`). This is a design call, made after review.
  Without it, if the leader's prepare was lost and the client completed on other stores' votes, no driver would start
  until the idle-Tx heartbeat (`txHeartbeatTimeout`, about 5 s), and the voters' locks would be held meanwhile.
  Continuing keeps today's driver-start behaviour at no cost on the normal path, where the leader's outcome follows
  within about one RTT.
  - So `TransactionManager` must keep the driver registered after `result` completes, until a leader message arrives
    or the driver stops itself. Today it forgets the driver as soon as `result` completes. Replace that with a driver
    "stopped" signal.
- Downstream is otherwise unchanged. `TransactionImpl` records the outcome in `client.txStatusCache`, which the read
  path relies on. A store that never received the prepare votes abort on a driver's prefer-abort prepare and is
  repaired by the missed-update marks, as today.

### 4.3 Server: make vote-decided commit an invariant

Two existing paths can choose abort while every object has `t` commit votes. Both must change.

**(a) `TransactionDriver`: remove the past-expiry abort of an undecided tally** (the `start.pastExpiry()` branch,
about `TransactionDriver.scala:460`). A driver then proposes abort only when the tally is abort-certain. It still
proposes an accepted value its promise quorum reports (P7), as now. That branch existed for rebuild F6, so that a Tx
whose peers all hold acceptor state could still end. Rule (b) below now covers F6. Remove the `pastExpiry` plumbing
too if nothing else uses it.

**(b) `ExpiryTally.decideUnknown`: count votes, not just accepts.** Today's rule 3 second branch discards T when no
commit is chosen (`ac + k + sp < t` and `ac + sp <= w - t`), whatever the votes say. Example: every store votes commit
and the client is told so; the leader dies before Accept; one secondary pool's voters are down until expiry; every
primary store reports "nothing accepted"; T is discarded.

Two lemmas carry the rules. Both follow from (a) and from votes being immutable:
- **L1.** Every accepted abort traces back to an abort-certain tally. A driver's local proposal is abort only when its
  tally is abort-certain, and `Proposer.proposalValue` otherwise only re-proposes an accepted value. So if any store
  holds an accepted abort, some object never had `t` commit votes. No commit was ever chosen, and no client was told
  commit.
- **L2.** Every accepted commit traces back to a commit-certain tally (every object had `t` commit votes), or to a
  proof of commit (an object already showed T's update, so T was committed). Either way, commit is T's only outcome.

Abort-certain and commit-certain exclude each other, so accepted aborts and accepted commits never both exist.

Counts. For every pool of every referenced object, not just the changed ones:
- `vc`: stores that replied and voted commit. Check that `TxStatus.votedCommit` is true for a commit voter in a
  lock-only pool. If it isn't, add a field.
- `k`: amnesiac stores. They count as possibly having voted, or accepted, anything.
- `s`: silent stores.
- Every other replier is a provable non-commit-voter. It replied "unknown" (`status = None`), or with a Tx that is no
  commit voter (its state is final, see below).

In the primary pool: `aa` stores report an accepted abort, `ac` an accepted commit, `k_p` are amnesiac and `sp` are
silent. For each changed pool, `r` is its read threshold.

A reported vote must be final. Gap found in review: a frozen Tx whose objects have not loaded answers a status request
with `Undetermined`, then loads, votes, and (with this change) tells the client. `sendDelayedPrepare` has no frozen
check. Fix, a design call: `Tx.receiveStatusRequest` answers an unresolved, non-muted Tx only once its disposition is
decided (`allObjectsLoaded`). Until then it is silent, counted as `s`. A Tx whose objects never load therefore holds T,
which is the accepted cost.

*Amendment 2026-11-16 (final review).* A loaded Tx can still be undecided (waiting on an in-flight commit, a
collision, or a client prepare that arrives after the freeze), so `allObjectsLoaded` did not mean "vote decided".
Fix: a frozen Tx never decides a vote (`Tx.determineDisposition` returns at once while T's outcome is unknown there
and it is not muted) and `sendDelayedPrepare` sends nothing once frozen. "Frozen, loaded and undecided" is then a
final non-voter, and the gate is correct. A Tx whose objects load only after the freeze is such a non-voter too.

A store with no Tx for T answers "unknown" at this point. Past expiry implies below the forget floor: the floor is at
least now − horizon, and expiry is start + horizon + margin. So such stores count as non-commit-voters (checked
against `Frontend.forgetFloorMillis` / `pastExpiry`).

Rules for an unknown outcome, in order. Rule 1, Adopt, is unchanged. These replace today's rules 2 and 3.
- **A. Commit by votes**: every referenced pool has `vc >= t`. Silence is allowed. No tally was ever abort-certain, so
  by L1 no abort was accepted, and `vc >= t >= r` means the update can be restored.
- **B. Commit, accepted**: `ac >= 1` and every changed pool has `vc >= r`. Silence is allowed. By L2 commit is the only
  outcome. This replaces today's `ac > w - t`.
- **C. Discard, accepted abort**: `aa >= 1`. By L1 no commit was chosen or reported.
- **D. Discard, data lost**: some changed pool has `vc + s < r`. The update can never be restored. `dataLost` (logged
  as an error) is true when a commit may have been chosen or reported: `ac >= 1`, or every referenced pool has
  `vc + k + s >= t`.
- **E. Discard, commit impossible**: some referenced pool has `vc + k + s < t`. No tally was ever commit-certain, so no
  commit was chosen or reported.
- **F. Commit by survivors** (new; this resolves the rebuild case). All of these must hold:
  - no store of T is silent, in any pool
  - every referenced pool has `vc + k >= t`
  - `k_p < t`
  - every changed pool has `vc >= r`

  Rules B and C did not fire, and no store is silent, so every primary store not reported accepted nothing. Only the
  `k_p < t` amnesiac stores could hide an accepted abort, fewer than a quorum, so no abort was chosen, and no client
  heard one: a client hears abort only through Paxos. Frozen acceptors and muted rebuilt stores accept nothing more,
  so nothing is chosen later. A conflicting transaction can't have committed: it would need `t` commit votes, but T's
  `vc >= r` voters hold its locks, and `t + r > w` (the root CLAUDE.md threshold invariant), which is the argument
  today's rule 2 uses.

  *Amendment 2026-11-16 (found while documenting, Task 7).* Rule F as written above is unsafe. A primary store that
  accepted abort, was finalized and let its cached outcome expire answers "unknown". With a lock-only primary
  object, it and one amnesiac store can hide a chosen abort that the client was told of. Example: Replication(3,2),
  S0 commit, S1 and S2 abort and accept it, S2 forgets T, S1 is rebuilt. As built, F requires `k_p + n_p < t`,
  where `n_p` is the number of primary stores replying "unknown". G holds otherwise, including with every store
  replied when `k_p + n_p >= t`. With a changed primary object `n_p` never matters, because `vc_p >= r` leaves at
  most `w - r < t` other stores. One divergence between observers remains, and is accepted. One observer discards by
  C over an abort accepted by fewer than a quorum. The accepter then forgets T, and another observer commits by F.
  No client was told abort, F's locked voters exclude a conflicting commit, and the commit's missed-update marks
  repair the store that discarded. See `docs/safety/paxos-transactions.md` 10.10 and `ExpiryTallyConsistencySuite`.

  *Amendment 2026-11-16 (final review).* F's conflict argument above is unsafe for lock-only pools. "T's `vc >= r`
  voters hold its locks" covers only changed pools; a lock-only pool needs only `vc + k >= t`, and its amnesiac stores
  hold no lock. Example: P primary, changed, Replication(3,2); L lock-only Replication(3,2), where T holds
  `ExclusiveRevisionLock(Y)`. T' updates Y: L1 and L2 vote commit for it and T' commits. T gets abort votes from L1
  and L2 (requirement failed) and a stale commit vote from L0. L1 is rebuilt before its abort reaches T's driver. At
  expiry P has `vc = 3`, L has `vc = 1, k = 1, s = 0`: A-E fail and F committed T over its failed requirement. The
  same happens with a lock-only primary (p0 commit, p1 amnesiac, p2 a frozen abort voter). F now also requires
  **every referenced pool to have `vc > w - t`**: then a conflicting commit's `t` votes overlap T's live voters there
  (`vc + t > w`). Changed pools already have it from `vc >= r`, since `t + r > w`. G holds otherwise, logged as
  undecidable with every store replied: the amnesiac stores may have voted commit with the voters seen, so a client
  may have been told commit. `ExpiryTallyConsistencySuite` checks that no observer commits where some pool truly had
  fewer than `t` commit votes and at least `t` stores holding none of T's locks.
- **G. Hold** otherwise. That happens while some store is silent (transient). With every store replied, it happens only
  when `k_p >= t`, beyond the pool's loss tolerance (`t - r < t`), so data was lost too. Keep the
  `UNDECIDABLE TRANSACTION` log for that case.

Actions are today's: a commit (A, B, F) is `onExpiryCommit`, which resolves at every store, applies, and has the
store's driver finalize. A discard (C, D, E) is today's discard, including a commit voter's UnackedCommits lookup
before it discards.

Consistency across observers. The scaladoc must prove that no two observers reach a commit (A, B, F) and a discard
(C, D, E). Reported states are final, so observers differ only in which stores are silent to them.
- **A or B against C:** excluded by L1/L2 and the exclusion of the two tallies.
- **A or B against E:** A's or B's evidence means a tally was commit-certain. So every pool truly has `t` commit voters,
  each still voting commit or amnesiac, and every observer sees `vc + k + s >= t`.
- **A or B against D:** A or B needs `vc >= r` at its observer. D needs `vc + s < r` at its own, and a store silent to
  one observer can only add to the other's `vc + s`.
- **F against C:** F's observer sees every non-amnesiac primary store and finds no accepted abort. C needs an accepted
  abort reported by a non-amnesiac store, which F's observer would have seen.
- **F against E:** F's observer has no silence, so its `vc + k` is the true count. E's observer counts at least that
  much in `vc + k + s`.
- **F against D:** as for A or B against D.

The example in section 3 (Replication(5,4): three commit votes, one abort, one lost vote, every store replied) now
decides by F, commit. The three voters' update is repaired onto the other two stores by the finalization's
missed-update marks.

Liveness for F6 (every store replied, none amnesiac): every store is a commit voter or a provable non-voter. So either
every pool has `vc >= t` (rule A), or some pool has `vc < t = vc + k + s` (rule E).

`decideKnown` is unchanged.

**(c) Audit, written into paxos-transactions.** Show that every other way T can resolve abort is impossible once every
object has `t` commit votes:
- abort-certain tally (`w - a < t` contradicts `c >= t`)
- `Proposer.proposalValue` adopting an accepted abort (only an abort-certain driver ever proposed one: lemma L1, which
  the audit must establish over every proposer path, proof holders and expiry included)
- proof holders and late resolution (they never propose abort)
- status-cache relays and expiry Adopt (they repeat a real outcome)
- UnackedCommits held aborts (an accept of an abort proposal, so as above)
- `NoClientPrepare` and `InvalidDescription` votes (they produce abort votes, so `c >= t` everywhere can't coexist with
  `InvalidDescription`; `NoClientPrepare` only affects non-voters)

If the audit finds another path, fix it in the same way, or bring it back as a question.

### 4.4 Latency after the change

Client → stores (vote saved) → client: 2 one-way hops and one CRL write. Stores learn as before.

## 5. Part 2: parked reads after a known commit

- `Read` gains `awaitCommit: Set[TransactionId]` (default empty), with a `codec.proto` field.
- `BaseReadDriver`: both places that re-read a store because of `holdsKnownCommittedLock` set `awaitCommit` to the
  known-committed txids among that store's `lockedWriteTransactions`.
  - One place is the immediate re-read in `receiveReadResponse`.
  - The other is the later re-read of stores in `undecidedLockStores`, once their locks are learned to be committed.
  - Other reads send it empty.
- `Frontend.readForNetwork`: if `awaitCommit` intersects the servable state's `lockedWriteTransactions`, park the read
  on that object instead of answering.
  - Answer it with the current state once the intersection is empty. That is, re-check whenever a Tx releases its
    locks on the object: commit applied in memory (locks release then, `server/store/CLAUDE.md`, Commits), abort,
    expiry or discard, forget.
  - Also answer when it has waited `ParkedReadWait`, with whatever state there is then. Check that on the store
    heartbeat, like `expireParkedRepairs`. Use about one heartbeat (1 s). That is well under the client's first read
    retransmit (`ReadManager`: exponential backoff from 10 s).
  - An object with no such lock is answered at once, as now. An uncached object can't be locked: a Tx holding it
    keeps it cached.
  - Fail-stop and close drop parked reads, and the client's retransmit covers them.
  - A retransmit of the same `readUUID` replaces the parked entry rather than adding one.
  - Cap the number of reads parked per store. Past the cap, answer at once.
- Safety: none affected. A false or stale claim from a client only delays its own read.

## 6. Constraints

- Server threading: `Tx` and `Frontend` work stays on the store executor. Sends go from the existing save-completion
  handlers. Nothing blocks the control thread.
- Crash-only. A vote message is never sent before its vote is durable.
- No hot-path cost for rare cases, and simplicity on rare failure paths (holding is the answer, not optimized
  recovery).
- Tests: deterministic (LockstepThreading / TestNetwork where possible), no flakes, as fast as possible.
- When behaviour changes, update the module CLAUDE.md files: `server/transaction` (Paxos, Expiry),
  `server/store` (reads), `client/internal` (outcomes), `common/network` (messages).

## 7. Tests (minimum)

Part 1
- `ClientTransactionDriver` unit tests:
  - commit at exactly `t` votes per object, and not at `t - 1`
  - several pools with different IDAs
  - abort votes alone never complete it; abort comes only from the leader
  - `Undetermined` ignored
  - duplicate votes counted once
  - a conflicting vote logged
  - leader messages still complete the driver
- Integration, with every `TxAccept` (or `TransactionResolved`) dropped: the client still completes `Committed` from
  votes, and once the drops stop the stores converge on commit.
- Leader dies after the votes: the client is told commit, and T later commits through a recovery driver or expiry.
- `TransactionDriverSuite`: an undecided tally past expiry no longer proposes abort. An abort-certain one still does.
- `ExpiryTallySuite`:
  - One case per rule, A to G, including:
    - all commit votes, nothing accepted, a secondary pool partly silent: rule A commits
    - fewer than `t` commit votes known, nothing accepted, a store silent: hold
    - the section 3 rebuild example: rule F commits
    - the F6 scenario: rule E
    - `k_p >= t` with every store replied: hold, undecidable
    - `dataLost` as specified
  - The existing tests keep passing, or are updated with the reason in the commit where the new rules change the
    outcome.
  - An exhaustive or property test of observer consistency, for small pools: Replication(3,2), Replication(5,3),
    Replication(5,4), and one RS pool, with a secondary pool. Enumerate the true final states, give two observers
    different silent sets, and assert they never reach both a commit and a discard. This is the test that guards the
    section 4.3(b) argument.

Part 2
- Frontend: a read with `awaitCommit` naming a lock holder parks, and is answered with the new state when the commit
  applies.
  - It is answered at once when the object isn't locked by those txids.
  - It is answered at `ParkedReadWait` with the current state.
  - The cap and readUUID replacement behave as specified.
- End to end: a read right after a fast commit returns the committed revision, with `committedLockRereads` at most 1
  per store.

## 8. Docs to update

- `Architecture.md` paragraph at about line 139: describe the client learning from votes, the unchanged leader path,
  the absence of a commit-only round 1, and holding instead of discarding.
- `docs/safety/paxos-transactions.md` (and the transaction-horizon design's rule list, superseded by this one):
  - §2 table, and its "must not be used as invariants" note
  - §4 messages (`TransactionVote`)
  - §5 flow (client vote leg)
  - §3 a new invariant for vote-decided commit, with P2/P6 re-derived
  - §10.10 expiry rules
  - the 4.3(c) audit
  - F1 status set to FIXED, with a dated note
  - the README ranked row
- `server/transaction/CLAUDE.md` Expiry rules (rules A to G replace 2 to 4) and the Paxos bullet on past-expiry abort, which
  goes away. Also the expiry section of `server/store/CLAUDE.md` and the rebuild docs where rebuild F6 cites the
  driver abort.
- `Todo.txt`: remove 8.1.

## 9. Left to the builder

- Exact hook points (where a Tx's locks release, for parked reads; the precise send sites in `Tx`), field numbers,
  constant values (`ParkedReadWait`, the park cap), test names and suites.
- Whether `TxStatus` needs a new field for lock-only commit voters (section 4.3(b)).
- Order of work: part 1 before part 2; within part 1, the server invariant (4.3) may land before or with the client
  tally, but the client must never ship counting votes without it.
