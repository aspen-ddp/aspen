package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.network.TxStatusResponse
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionStatus}

/** How an unresolved Tx past the transaction horizon decides from its peers' status replies (transaction horizon
  * design, section 2, decision rules 1-4). Pure: the UnackedCommits check a commit voter makes before it discards is
  * its caller's (Frontend)
  */
object ExpiryTally:

  enum Decision:
    /** Rule 1: some reply carries the outcome. `finalizedOrLate`: the commit is finalized or late, so it takes the
      * late-commit path (an UnackedCommits lookup for a commit voter)
      */
    case Adopt(committed: Boolean, finalizedOrLate: Boolean)
    /** Rule 3: more than `width - writeThreshold` of the primary pool's stores hold no acceptor state for T and never
      * can, so no quorum can ever decide T again, and a committed T was finalized (a commit is forgotten only then)
      */
    case Discard
    /** Rule 4: enough possible holders remain to form a quorum, so a driver can still decide T */
    case Hold

  /** @param replies the replies of one query round, by the store that sent them, the asking store's own included.
    *                A store absent from it was silent, and counts as a possible holder. `None`: "unknown", from a
    *                store with no Tx and no cached outcome, whose forget floor is above T's start, so it can never
    *                join T. A reply without acceptor state (a muted Tx) counts as a non-holder too. A rebuilt store
    *                never replies as a non-holder for T started before its acceptor (Frontend.mayHaveForgottenUndecided):
    *                rule 3 rests on non-holders having forgotten T only once it was decided, and its old incarnation
    *                lost its state undecided
    * @param known the outcome the asking Tx already knows (it resolved, and waits for a TxFinalized that may never
    *              come), if any. Re-adopting it would change nothing, so rule 1 adopts only a finalized or late commit
    *              of a known commit (which concludes it), and every other reply is classified by its acceptor state:
    *              one with an outcome but none (a status cache answering) is a non-holder
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
    else
      val ida = txd.primaryObjectIDA
      val nonHolders = replies.count: (store, reply) =>
        store.poolId == txd.primaryObject.poolId && (store.poolIndex & 0xFF) < ida.width &&
          reply.forall(!_.holdsAcceptorState)
      if nonHolders > ida.width - ida.writeThreshold then Decision.Discard else Decision.Hold
