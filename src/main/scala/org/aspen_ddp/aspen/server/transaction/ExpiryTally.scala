package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.network.TxStatusResponse
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionStatus}

/** How an unresolved Tx past the transaction horizon decides from its peers' status replies (transaction horizon
  * design, section 2, rule 1; client 1-RTT commit design, section 4.3(b), rules A-G). Pure: the UnackedCommits check
  * a commit voter makes before it discards is its caller's (Frontend)
  */
object ExpiryTally:

  enum Decision:
    /** Rule 1: some reply carries the outcome. `finalizedOrLate`: the commit is finalized or late, so it takes the
      * late-commit path (an UnackedCommits lookup for a commit voter)
      */
    case Adopt(committed: Boolean, finalizedOrLate: Boolean)
    /** Rules A, B and F (decideUnknown): abort was never chosen and never can be (A: every pool has its write
      * threshold of commit votes; B: an accepted commit; F: every store replied, too few are amnesiac or "unknown" to
      * hide a chosen abort, and every pool has more than `w - t` live commit voters, so no conflicting transaction
      * committed), and enough commit voters hold T's update to restore each changed object at its read threshold. T is
      * decided committed here: applied for real and finalized, and the finalization's missed-update marks repair the
      * stores that never applied it
      */
    case Commit
    /** Rules C, D and E (decideUnknown): an accepted abort (C), T's update can never be restored for some changed
      * object (D; `dataLost`: a commit may have been chosen or reported, so a committed update is lost, as an object
      * below its read threshold is), or some pool never had its write threshold of commit votes (E). No commit of T is
      * chosen and unfinalized, and none ever can be. Discarded here, a commit voter only once its UnackedCommits
      * lookup finds no entry. A Tx that knows its outcome concludes it instead (decideKnown)
      */
    case Discard(dataLost: Boolean)
    /** Rule G: some store's state is still unknown (it is silent), or, with every store reported (`undecidable`),
      * at least the primary pool's write threshold of stores are amnesiac or answered "unknown", so they may hide a
      * chosen abort, or some pool has at most `w - t` commit voters, so a conflicting transaction may have committed
      */
    case Hold(undecidable: Boolean)

  /** @param replies the replies of one query round, by the store that sent them, the asking store's own included.
    *                A store absent from it was silent: its state is unknown, so it counts as possibly having voted
    *                or accepted anything. `None`: "unknown", from a store with no Tx and no
    *                cached outcome below its forget floor, which neither holds T nor ever can. An unresolved Tx
    *                replies only once frozen (past expiry by its own clock, Tx.frozen), so a reported state is final:
    *                its vote is immutable and it accepts nothing more. A rebuilt store reports `amnesiac` for T started
    *                before its acceptor: no state, but its old incarnation may have accepted T
    * @param known the outcome the asking Tx already knows (it resolved, and waits for a TxFinalized that may never
    *              come), if any. Re-adopting it would change nothing, so rule 1 adopts only a finalized or late commit
    *              of a known commit, and otherwise it concludes once more than `width - writeThreshold` of the primary
    *              pool's stores are non-holders (no acceptor state, and not amnesiac), or holds
    */
  def decide(txd: TransactionDescription, replies: Map[StoreId, Option[TxStatusResponse.TxStatus]],
             known: Option[Boolean] = None): Decision =
    val outcomes = replies.valuesIterator.flatten.filter(_.status != TransactionStatus.Unresolved).toList
    val adoptable = known match
      case None => outcomes
      case Some(true) => outcomes.filter(s => s.status == TransactionStatus.Committed && (s.finalized || s.late))
      case Some(false) => Nil
    if adoptable.nonEmpty then
      // Opposite outcomes should never arise (a split decision); a commit is preferred, as a proof is, so that a store
      // owed the commit never drops it
      val committed = adoptable.filter(_.status == TransactionStatus.Committed)
      val adopted = if committed.nonEmpty then committed else adoptable
      Decision.Adopt(committed.nonEmpty, adopted.exists(s => s.finalized || s.late))
    else if known.nonEmpty then
      decideKnown(txd, replies)
    else
      decideUnknown(txd, replies)

  private def poolStores(txd: TransactionDescription, poolId: PoolId): Iterator[StoreId] =
    (0 until txd.poolIDAMap(poolId).width).iterator.map(i => StoreId(poolId, i.toByte))

  /** A resolved Tx waits only for a TxFinalized: it concludes once T was finalized, which more than `w - t` primary
    * stores that forgot T show (a commit is forgotten only at TxFinalized)
    */
  private def decideKnown(txd: TransactionDescription,
                          replies: Map[StoreId, Option[TxStatusResponse.TxStatus]]): Decision =
    val ida = txd.primaryObjectIDA
    val nonHolders = poolStores(txd, txd.primaryObject.poolId).count: store =>
      replies.get(store).exists(_.forall(s => !s.holdsAcceptorState && !s.amnesiac))
    if nonHolders > ida.width - ida.writeThreshold then Decision.Discard(dataLost = false)
    else Decision.Hold(undecidable = false)

  /** Rules A-G for a Tx that does not know the outcome (client 1-RTT commit design, section 4.3(b)).
    *
    * Two lemmas carry them. Both follow from votes being immutable and from drivers proposing abort only on an
    * abort-certain tally (TransactionDriver never aborts an undecided tally past expiry):
    *  - L1. Every accepted abort traces back to an abort-certain tally: a driver proposes abort only on one, and
    *    `Proposer.proposalValue` otherwise only re-proposes an accepted value. So if any store holds an accepted abort,
    *    some object never had `t` commit votes: no commit was ever chosen, and no client was told commit.
    *  - L2. Every accepted commit traces back to a commit-certain tally (every object had `t` commit votes) or to a
    *    proof of commit (T's update was already applied, so T committed). Either way commit is T's only outcome.
    * Abort-certain and commit-certain tallies exclude each other, so accepted aborts and accepted commits never both
    * exist.
    *
    * Counts, per pool of every referenced object (changed or lock-only), with that pool's write threshold `t`: `vc`
    * stores replied and voted commit, `k` replied amnesiac (they may have voted, or accepted, anything) and `s` are
    * silent. Every other replier is a provable non-commit-voter now: "unknown" (below its floor, it can never join T)
    * or a frozen Tx that did not vote commit. In the primary pool `aa` / `ac` stores report abort / commit as their
    * latest accept, `k_p` / `sp` are its `k` / `s`, and `n_p` stores replied "unknown". Each changed pool has a read
    * threshold `r` (IDA.consistentRestoreThreshold).
    *
    * "Unknown" does not mean the store never held T. A store that accepted T's outcome, was finalized (or discarded T
    * by expiry) and then let its cached outcome expire answers "unknown" too. That never makes a vote count wrong: a
    * store that forgot T no longer holds its update, so not counting it in `vc` only pushes a tally toward discard,
    * which is safe (a commit voter's UnackedCommits lookup before it discards; missed-update marks for the rest). But
    * its accept is hidden: `aa` and `ac` count only stores that still hold T. Rules A and B do not depend on that
    * (L1/L2 argue from votes and from a reported accept), C and D only discard, and F counts "unknown" primary replies
    * with the amnesiac ones. In order:
    *  - A. Commit: every referenced pool has `vc >= t`. No tally was ever abort-certain, so by L1 no abort was
    *    accepted; and `vc >= t >= r` holders of the update can restore it.
    *  - B. Commit: `ac >= 1` and every changed pool has `vc >= r`. By L2 commit is the only outcome.
    *  - C. Discard: `aa >= 1`. By L1 no commit was chosen or reported.
    *  - D. Discard: some changed pool has `vc + s < r`, so T's update can never be restored. `dataLost` when a commit
    *    may have been chosen or reported: `ac >= 1`, or every referenced pool has `vc + k + s >= t`.
    *  - E. Discard: some referenced pool has `vc + k + s < t`. No tally was ever commit-certain, so no commit was
    *    chosen or reported.
    *  - F. Commit: no store of T is silent, every referenced pool has `vc + k >= t` and `vc > w - t`, `k_p + n_p < t`
    *    (primary) and every changed pool has `vc >= r`. B and C did not fire and no store is silent, so every primary
    *    store that still holds T and is not amnesiac accepted nothing. Only the `k_p` amnesiac and `n_p` "unknown"
    *    stores, fewer than a quorum, could hide an accepted abort, so no abort was chosen, and no client heard one (a
    *    client hears abort only through Paxos). (Counting only `k_p` was unsafe, found 2026-11-16: with a lock-only
    *    primary object, a quorum of one rebuilt store and one that accepted abort, was finalized and forgot T could
    *    hide a chosen abort. With a changed primary object `n_p` never matters: `vc_p >= r` leaves at most
    *    `w - r < t` others.)
    *    Frozen acceptors and muted rebuilt stores accept nothing more, so nothing is chosen later. No conflicting
    *    transaction committed: it would need `t` commit votes in some pool of T, but T's `vc` voters there hold its
    *    locks and `vc + t > w`. That premise needs `vc > w - t` in every referenced pool, lock-only ones included; a
    *    changed pool has it from `vc >= r`, since `t + r > w`. (Requiring it only through `vc >= r` was unsafe, found
    *    2026-11-16: in a lock-only pool of Replication(3, 2), a stale commit vote, a rebuilt abort voter and a frozen
    *    abort voter give `vc + k >= t`, yet T' may have committed with the two stores holding no lock of T, so T's
    *    requirement failed.)
    *  - G. Hold otherwise: some store is silent (transient), or, every store replied (`undecidable`), either
    *    `k_p + n_p >= t`: the amnesiac and "unknown" primary stores may hide a chosen abort (with a changed primary
    *    object D fires first then, `t + r > w`, so only an unchanged primary object reaches it); or some lock-only
    *    pool has `vc <= w - t < t <= vc + k`: a conflicting transaction may have committed with the stores holding no
    *    lock of T. Either way the amnesiac stores may have voted commit with the voters seen, so a client may have been
    *    told either outcome.
    *
    * No two observers reach a commit (A, B, F) and a discard (C, D, E). Reported states are final (an unresolved Tx
    * replies only once frozen and its objects are loaded), and a store leaves a count only by deciding, after which
    * it reports its outcome (rule 1), so observers differ only in which stores are silent to them:
    *  - A or B against C: by L1 and L2, and the exclusion of the two tallies.
    *  - A or B against E: A's or B's evidence means a tally was commit-certain, so every pool truly has `t` commit
    *    voters, each still voting commit or amnesiac, and every observer sees `vc + k + s >= t`.
    *  - A, B or F against D: the commit needs `vc >= r` at its observer, D needs `vc + s < r` at its own, and a store
    *    silent to one observer can only add to the other's `vc + s`.
    *  - F against C: F's observer sees every non-amnesiac primary store and finds no accepted abort; C needs one
    *    reported by a non-amnesiac store, which F's observer would have seen. One divergence remains, and is accepted:
    *    C's observer saw an abort accepted by fewer than a quorum (never chosen), the accepter then discarded or was
    *    finalized and forgot T, and F's observer, later, counts it as "unknown". That is safe for clients: the abort
    *    was never chosen, so no client was told abort; F's `vc > w - t` voters hold T's locks, so no conflicting commit
    *    exists (`vc + t > w`); and the discarding store is repaired by the commit's finalization (its missed-update
    *    marks), as any store that missed T is.
    *  - F against E: F's observer has no silence, so its `vc + k` is the true count, and E's observer counts at least
    *    that much in `vc + k + s`.
    * ExpiryTallyConsistencySuite checks this exhaustively over the states the protocol can produce, stores that forgot
    * T included, and that no observer commits over a chosen abort.
    *
    * Liveness with every store replied and none amnesiac (rebuild F6): every store is a commit voter or a provable
    * non-voter, so either every pool has `vc >= t` (A) or some pool has `vc < t = vc + k + s` (E).
    */
  private def decideUnknown(txd: TransactionDescription,
                            replies: Map[StoreId, Option[TxStatusResponse.TxStatus]]): Decision =
    val primaryIda = txd.primaryObjectIDA
    val primaryPool = txd.primaryObject.poolId
    def reply(st: StoreId) = replies.get(st).flatten
    val primary = poolStores(txd, primaryPool).toList
    val ac = primary.count(st => reply(st).exists(s => !s.amnesiac && s.accepted.contains(true)))
    val aa = primary.count(st => reply(st).exists(s => !s.amnesiac && s.accepted.contains(false)))
    /** Per referenced pool: (commit voters, amnesiac stores, silent stores) */
    final case class Counts(vc: Int, k: Int, s: Int)
    val counts: Map[PoolId, Counts] = txd.allReferencedObjectsSet.map(_.poolId).iterator.map: poolId =>
      val stores = poolStores(txd, poolId).toList
      poolId -> Counts(
        vc = stores.count(st => reply(st).exists(s => !s.amnesiac && s.votedCommit)),
        k = stores.count(st => reply(st).exists(_.amnesiac)),
        s = stores.count(st => !replies.contains(st)))
    .toMap
    def t(poolId: PoolId) = txd.poolIDAMap(poolId).writeThreshold
    def r(poolId: PoolId) = txd.poolIDAMap(poolId).consistentRestoreThreshold
    val changedPools = txd.allReferencedObjectsSet.filterNot(p => txd.lockOnlyObjects.contains(p.id))
      .map(_.poolId).toList
    def everyPool(f: (PoolId, Counts) => Boolean) = counts.forall(f.tupled)
    def changedRestorable = changedPools.forall(p => counts(p).vc >= r(p))
    val kp = counts.get(primaryPool).map(_.k).getOrElse(0)
    /** Primary stores that replied "unknown": never joined T, or accepted, decided and forgot it */
    val np = primary.count(st => replies.get(st).exists(_.isEmpty))

    if everyPool((p, c) => c.vc >= t(p)) then
      Decision.Commit // A
    else if ac >= 1 && changedRestorable then
      Decision.Commit // B
    else if aa >= 1 then
      Decision.Discard(dataLost = false) // C
    else if changedPools.exists(p => counts(p).vc + counts(p).s < r(p)) then
      Decision.Discard(dataLost = ac >= 1 || everyPool((p, c) => c.vc + c.k + c.s >= t(p))) // D
    else if counts.exists((p, c) => c.vc + c.k + c.s < t(p)) then
      Decision.Discard(dataLost = false) // E
    else if counts.valuesIterator.forall(_.s == 0) && everyPool((p, c) => c.vc + c.k >= t(p)) &&
      kp + np < primaryIda.writeThreshold && changedRestorable &&
      everyPool((p, c) => c.vc > txd.poolIDAMap(p).width - t(p)) then
      Decision.Commit // F
    else
      Decision.Hold(undecidable = replies.size >= txd.allDataStores.size) // G
