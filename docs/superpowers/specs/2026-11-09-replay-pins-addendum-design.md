# Replay pins addendum: applied stubs and prefer-abort prepares

Date: 2026-11-09. Branch: `pin_refactor`. This extends `2026-11-09-replay-pins-design.md` (the design), including
its "Amendments during implementation", and closes the two problems the final whole-branch review raised:
- the replay of a committed transaction at stores that acknowledged late (safety docs R-N1/X11);
- the later commit of an aborted transaction (P-N4).

The user chose option b for the first and proposed prefer-abort prepares for the second.

## Problem 1: late-acknowledged stores re-apply a replay

Store S in pool P votes commit for T and applies it. Its TxCommitted reaches the driver after `errorTimeout`. The
finalizer therefore writes UnackedCommits entry (S, T), "S owes T", although S has the update. The entry lives until
S's repair scan deletes it, 30 s to 15 min later.

A replayed client prepare for T can arrive in that window. S then:
- checks only T's requirements on P's objects, holds them all, and votes commit;
- sees a pin store elsewhere prove T, after which the driver resolves late;
- finds its entry and applies T again.

A pin in P prevents this, because the pin's requirement fails on S, which applied T, so S votes abort. Pools that T
writes without a pin are exposed:
- `DoesNotExist` inserts after a later delete;
- allocations, whose store-fabricated state always passes;
- blind `tx.update(..., None, ...)` writes.

If every store of P acknowledged late, the stale write lands on a quorum and repair follows it. The user ruled out
requiring a pin in every pool, as too restrictive.

## Problem 2: an aborted transaction commits later

The pin rule fixes only committed transactions in place. An aborted T wrote nothing, so its requirements can hold
again. After the status cache (7 s by default) expires no store remembers T, and three paths can bring it back:
1. A commit voter that crashed before learning of the abort recovers its vote, and its driver re-prepares T.
2. A driver left running after its store closed re-prepares T (already closed by Ruling 31).
3. A delayed duplicate of the client's own prepare arrives.

The other stores check T's requirements afresh. If they hold, T commits after the client was told it aborted. Paths
1 and 2 send no object data. Stores without data vote abort after `MissingDataWait`. Only transactions with no data
can commit through them: refcount changes (deletes included), version bumps and lock-only transactions.

## Goals

- **Problem 1 closed.** A store that applied T never applies T again through a late commit, for any write class,
  however late the replay. A residual that can only affect a minority of a pool is acceptable, because repair
  restores it from the majority.
- **Problem 2 closed for paths 1 and 2.** A re-driven aborted transaction can only abort again.
- **No normal-path cost.** A transaction whose client stays alive and whose stores acknowledge in time pays nothing
  new.

## Non-goals

- **A pin in every written pool.** The user ruled this too restrictive.
- **Owed-abort entries** (a second tree for aborts). Prefer-abort makes them unnecessary.
- **Path 3 of problem 2** (a delayed duplicate client prepare of an aborted transaction). This is the accepted
  corner case, documented below. Closing it would need a client-set expiry on prepares, which reintroduces a
  timestamp. The user removed the horizon precisely to avoid that.

## Anti-goals

- **No clock or age test** returns to the prepare path.
- **No new abort source on the fast path.** A driver's prepare that merely overtakes the client's prepare must never
  abort an ordinary transaction.
- **No stub or abort state that grows without cleanup.**

## Design

### A. Applied stubs (problem 1)

A **stub** is a small durable record, (S, T) "applied", kept by store S. It means S already holds T's update, so
an UnackedCommits entry naming S for T is stale.

**Storage: the device CRL (`CrashRecoveryLog`)**, as a minimal record for (S, T). The record replaces T's
transaction state at the point where that state would otherwise be deleted. It carries no object data, no Paxos
state and no requirements.

The CRL already gives durability, per-store association, transfer (`extractCrl` / `crl_save.log`), adoption and
rebuild (`dropStore`), so stubs inherit all of them. A rebuilt store is muted for older transactions and never votes
on them, so losing its stubs is harmless.

On load, stub records are not recovered as transactions. The Frontend keeps them as a set of txids.

**When S writes a stub.** S writes one when it forgets a committed T that it applied, and an entry for S might exist:
1. **TxFinalized names S.**
   - TxFinalized gains `owed: Set[StoreId]`, the stores for which the finalizer's marker wrote UnackedCommits
     entries. It is empty for a late TxFinalized (no finalization actions ran) and for an abort.
   - The marker's batch has completed before TxFinalized is sent, so the set is final (Ruling 8;
     `MissedUpdateMarker.mark` completes once the batches are written).
   - A store named in `owed` writes a stub before deleting T's state, whether its Tx applied T or it has no Tx for T
     at all.
   - A store with no Tx either applied T and forgot it, or never saw T. A stub is right in both cases:
     - the first is not owed;
     - the second is owed but must not apply through a replay's fresh vote, because applying T now could overwrite
       newer writes it has since applied. ErrorTree repair restores it from the majority instead.
2. **The store applied T through its entry.** This covers `unackedCommitFound`, a late commit that found the entry,
   and Ruling 9's lingering Tx. It writes the stub when it applies.
3. **Fallback: a committed T applied here is being forgotten without a TxFinalized that lists `owed`.** For example,
   TxFinalized was lost and the Tx was resolved by its heartbeat driver through a late or relay path.
   - The store looks up its own entry first, using the existing lookup, hold and retry machinery.
   - If the entry is found, it writes a stub. If it is absent, it forgets T.
   - A failed lookup holds and retries at heartbeat.

The stub is written before T's state is deleted, so a crash between the two leaves both. A recovered commit vote that
has a stub applies nothing.

**Using the stub.** Before applying through an entry, whether in `lateCommit` or in `unackedCommitFound` for any Tx,
the store checks for a stub. If one exists it calls `applyNothing()`.

**Cleanup.**
- When S's repair scan handles entry (S, T) and deletes it, it also deletes S's stub for T, through the store's
  executor.
- A stub whose entry no longer exists, for example because the entry was already deleted, is deleted on the next
  scan. The scan compares the store's stub set with its UnackedCommits range.
- So stubs live as long as the entries that need them, and never longer than one scan period after.

**Residual.** S is named in `owed` but never receives that TxFinalized (the message is lost), and S has no Tx for T.
Such an S never saw T: had it seen T it would hold a Tx, and the fallback in case 3 would cover it.
- Stores that never saw T are at most width − writeThreshold of any pool, because T committed with a write threshold
  of commit votes. That is fewer than a quorum.
- A replay can therefore re-apply T at fewer than a quorum of P's stores. Quorum reads mask it, and the stores'
  ErrorTree marks repair it from the majority.
- Document this in docs/safety (R-N1/X11 narrowed to a self-healing minority).

### B. Prefer-abort prepares (problem 2)

Every prepare a store-side `TransactionDriver` sends is **prefer-abort**: that is exactly `TxPrepare.fromClient ==
false`, which already exists. No new field is needed. Client prepares are unchanged.

**The rule.** A Tx without a vote, meaning its disposition is Undetermined, it was not recovered with a vote, and it
has seen no client prepare, reacts to a prefer-abort prepare as follows:
1. **It does not check T's requirements.**
2. **Proof first.** Once its objects load, if `mayProve` and an object shows T's update (`appliedBy`), it proves at
   once, as today. A proof is not a vote. Late commits and Ruling 15 rely on proofs from stores answering drivers'
   prepares, so this must not wait.
3. **Otherwise it waits for the client's prepare**, up to `Frontend.MissingDataWait` (5 s). This is the existing
   missing-data wait, now applied to every transaction, data-less ones included.
   - If the client's prepare arrives, the Tx checks requirements normally and votes.
   - If it does not, the Tx votes abort.

A Tx that already has a vote, live or recovered from its CRL, answers prefer-abort prepares with that vote.

Prefer-abort never gathers a fresh commit vote, but the wait in rule 3 withholds the Tx's whole prepare response:
its promise as well as its vote, until the client's prepare, the outcome or the end of the wait. Accepts are
unchanged. A memoryless promise therefore still counts toward late condition 1 once it is given (after the wait, with
the abort vote), so late resolution of a data-less transaction can take up to `MissingDataWait` longer; provers are
unaffected, since the proof comes first.

**Why paths 1 and 2 close.**
- A driver decides abort only when some object cannot reach a write threshold of commit votes, which means at least
  width − writeThreshold + 1 of its stores voted abort.
- Votes are fixed once given.
- Stores that forgot T have no vote, and under prefer-abort they can only vote abort.
- So a re-driven aborted T never regains the commit votes it lacked.

**Effect on the rest of the system.**
- **Transactions with data: none.** A store without its data already votes abort after `MissingDataWait`.
- **Transactions without data, client alive: none.** The client retransmits its prepares until it learns the outcome
  (`SimpleClientTransactionDriver`), so every store eventually checks requirements on a client prepare.
- **Orphaned transactions without data** (the client died mid-transaction): these now abort where they might once
  have committed. This is the user's stated intent, and it matches the original design's accepted cost for orphans.
- **CRL backpressure.** A store that dropped the client's prepare and gets a driver's prepare waits rather than
  aborts. The client's retransmit lands within the wait under normal conditions, and the waiting Tx keeps its
  admission charge for up to 5 s.
- **Late commits, proofs and UnackedCommits:** unchanged (see the rule).

### C. The accepted corner case, documented

> A transaction the client was told aborted can still commit later in one case: a delayed duplicate of the client's
> own prepare reaches enough stores after every store has forgotten the transaction (more than the status-cache duration (7 s by default)
> later), and the transaction's requirements hold again at that time. Recovery and other store-side drivers can never
> commit an aborted transaction (prefer-abort prepares). A client that retries an aborted transaction successfully
> closes the case, because the retry consumes the same pin and the old requirements then fail.

It goes in:
- **The `Transaction` scaladoc** (`client/Transaction.scala`), next to `commit()`/`result`. Required by the user.
- **Architecture.md "Transaction Model"**, the paragraph that begins "It's worth pointing out that the transaction
  recovery process is pessimistic". Extend it: recovery and other store-side drivers send prefer-abort prepares, so
  they can never gather fresh commit votes; then one sentence for the corner case. This is a natural fit.
- **UsageGuide.md "What to take away"**: one bullet after the `TransactionAborted` bullets, saying an abort is final
  except for that corner case, and that a successful retry closes it. This is a natural fit.
- **CLAUDE.md Transaction Design**: one terse line each for prefer-abort and stubs, in the existing style.
- **docs/safety**: update P-N4 (narrowed to path 3), R-N1/X11 (narrowed to the self-healing minority), and any
  finding these mechanisms change.

## Implementation order

Each step is test-first.

1. **Confirm premises.**
   - `MissedUpdateMarker.mark`'s future completes only once its batches are written.
   - The CRL can hold a small per-(store, txid) record that survives rotation, transfer extraction and recovery
     loading.
   - If either is false, stop and bring it back.
2. **Prefer-abort (B).**
3. **Stubs (A):** in this order:
   - the CRL record;
   - Frontend's stub set;
   - `owed` on TxFinalized, with the marker reporting its entries;
   - stub writes (cases 1-3);
   - the stub check;
   - scan cleanup.
4. **Docs (C)**, then **docs/safety**.

## Testing

- **Prefer-abort:**
  - A store with no vote and no client prepare votes abort after the wait, without checking requirements, for a
    data-less transaction whose requirements hold.
  - If the client's prepare arrives within the wait, the store votes normally.
  - A prover receiving a driver's prepare proves at once.
  - A recovered commit vote answers with its vote.
- **Aborted transaction re-driven** (integration, `TestNetwork`): a data-less transaction (refcount change) aborts on
  a collision. A commit voter's vote is then recovered after every store forgot T, and the blocking condition has
  cleared. T aborts again, and the object is unchanged.
- **Stubs:**
  - A store named in `owed` writes a stub before forgetting.
  - A replayed client prepare at that store, with its entry present and a pin proof elsewhere, applies nothing.
  - The same replay against a pool where every store acknowledged late leaves every store unchanged. This is the
    R-N1 quorum scenario, end to end.
  - The case 3 fallback writes a stub after a found lookup.
  - The scan deletes the stub with its entry, and deletes orphan stubs.
  - Stubs survive restart, and travel with a transfer.
- **Full `sbt test`:** 70 s hang bound, no flaky tests, no new "Hung Test" prints.

## Decisions

Made by the user:
- **Option b** (durable applied stubs) for problem 1. A pin in every pool (option a) was rejected as too restrictive.
- **Prefer-abort prepares** for problem 2, with the wait adjustment.
- **Path 3 accepted** as a corner case. It is documented in the `Transaction` scaladoc, and in existing .md
  transaction sections where it fits.

My calls, for review:
- **Stubs live in the device CRL**, reusing its durability, transfer and rebuild handling.
- **Stores with no Tx also write stubs** when named in `owed`. The minority-bounded residual is accepted.
- **Prefer-abort is `fromClient == false`**, so no new message field.
- **The proof runs before the wait** for prefer-abort prepares.
- **The doc placements** listed in C.

## Left to the builder

- The CRL stub record's encoding, and how recovery separates it from transaction state.
- How the marker reports the stores it wrote entries for (`FinalizationAction` API).
- How the scan finds orphan stubs.
- Exact scaladoc wording, within the meaning above.

## Amendments during implementation

Decisions made while building that refined the text above. Where they conflict, these win, and a later amendment
wins over an earlier one.

- **1 (stub write point).** A stub is written where the Tx's CRL state would be deleted today (after its backend commits
  persist), never at TxFinalized receipt while commits are outstanding or failed. The Tx remembers TxFinalized's owed
  membership until then.
- **2 (hand-off).** `MissedUpdateFinalizationAction` sets `handedOff` when it hands a mark to `MissedUpdateMarker`;
  after that only the write callback (which sets `owed` first) may complete the action. Everything the finalizer writes
  is durable before TxFinalized.
- **3 (second driver; its wait is superseded by 7).** A driver finalized by another driver's non-late commit TxFinalized, whose own finalizer has
  handed off a mark, waits for that mark and sends its own TxFinalized with owed = received owed union its own (first
  send and heartbeat resends). The store does not discard such a driver on the other TxFinalized until it has sent its
  own. Stores that already forgot T write stubs from it. Residual: the second driver's host crashing after its entries
  commit but before its TxFinalized reaches the stores leaves a rare replay exposure at stores that applied T.
- **4 (owed atomic).** `owed` is passed into `onFinalized` atomically, so a late TxFinalized never carries a non-empty
  owed.
- **5 (recovered stub holder).** A recovered non-late commit vote holding a stub resolves committed and stays until
  TxFinalized (`doCommit` applies nothing), rather than applying nothing and being forgotten, preserving "acceptors
  forget only on TxFinalized or a late outcome".
- **6 (scan snapshot; amended by 8).** The repair scan snapshots the store's stub set before walking its UnackedCommits range and
  drops only snapshot stubs whose txid had no entry in the walk. A stub written mid-walk survives to the next scan;
  stubs are written after their entry is durable, so a pre-walk stub's entry is visible to the walk.
- **7 (second driver, no wait; replaces 3's wait).** At the hand-off (amendment 2) the marker records the stores it
  plans UnackedCommits entries for (`handedOffStores`), whatever the write's outcome. A driver D1 reached by another
  driver's non-late commit TxFinalized cancels its finalizer (so a mark not yet handed off never writes) and then reads
  `handedOffStores`, and finalizes at once, non-late, with owed = received owed union that planned set. That is the
  timing before amendment 3, inside the status-cache window, so D1 sends no non-late TxResolved after it and no late
  message can later finalize it with an empty owed. Amendment 4 stays: late and abort messages carry an empty owed.
  The store drops D1 on that TxFinalized as before amendment 3. A cancelled marker whose write was handed off may still
  write; its stores are listed. A failed multi-pool mark no longer drops any planned store from owed. A single driver
  still finalizes only once its own write succeeded; a write that gives up never completes the action, as before.
  Cost: a stub may now precede its entry's durability (amendment 8). The "set is final" sentence of case 1 holds for
  the first finalizer to complete, not for D1's own entries.
- **8 (two-scan orphan drop; amends 6).** Because of amendment 7, a stub may be written before its entry is durable.
  The repair scan drops an orphan stub only after two consecutive successful scans of the store found it with no
  entry (pre-walk snapshot, not seen in the walk, empty point lookup); one found with its entry in between is no
  longer suspected. The suspicions are per store, in memory on the repairing host. The pre-walk snapshot rule of
  amendment 6 stays. So a stub lives at most two scan periods past its entry, not one. Residual: a second driver's
  marker batch still in flight across two whole successful scans of the same store leaves its entry without a stub.
- **9 (test seam).** `missingDataWait` is a seam from `Host.Seams` through `DeviceControl`, `StoreLoader` and `Store`
  to `Frontend`, exposed as `TestNetwork.HostOptions.missingDataWait`, so the prefer-abort integration test waits
  200 ms instead of 5 s. No global var: suites run in parallel.
