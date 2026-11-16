package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.network.TxStatusResponse
import org.aspen_ddp.aspen.common.pool.PoolId
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
    /** Rule 2 (repair): abort was never chosen and never can be, and enough commit voters hold T's update to restore
      * each changed object at its read threshold. T is decided committed here: applied for real and finalized, and
      * the finalization's missed-update marks repair the stores that never applied it
      */
    case Commit
    /** Rule 3: T can never be applied (`dataLost`: too few commit voters hold its update for some changed object, so
      * any commit of it is lost, as an object below its read threshold is), or no commit of it is chosen and
      * unfinalized and none ever can be. Discarded here, a commit voter only once its UnackedCommits lookup finds no
      * entry. A Tx that knows its outcome concludes it instead
      */
    case Discard(dataLost: Boolean)
    /** Rule 4: some store's state is still unknown (it is silent), or, with every store reported (`undecidable`),
      * more stores lost their acceptor state than the pool tolerates
      */
    case Hold(undecidable: Boolean)

  /** @param replies the replies of one query round, by the store that sent them, the asking store's own included.
    *                A store absent from it was silent: its state is unknown, so it counts both as possibly holding a
    *                commit accept and as possibly voting commit. `None`: "unknown", from a store with no Tx and no
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

  /** Rules 2-4 for a Tx that does not know the outcome. In the primary pool (width `w`, write threshold `t`):
    * `ac` stores report commit as their latest accepted value, `k` are amnesiac and `sp` are silent. For each pool of a
    * changed object (read threshold `r`, IDA.consistentRestoreThreshold): `vc` stores voted commit and hold T's update,
    * and `s` are silent.
    *
    * Commit when `ac > w - t` and every changed pool has `vc >= r`. Any quorum that chose abort would include one of
    * the `ac` stores, whose latest accept would then be abort (Paxos: every proposal above a chosen ballot carries its
    * value), so abort was never chosen. Those stores are frozen, and the other `t - 1` cannot form a quorum, so
    * nothing is ever chosen again. And the update is held at its read threshold, so it can be applied and repaired.
    *
    * Discard when some changed pool has `vc + s < r` (T's update can never be restored: any commit of it is lost), or
    * when `ac + k + sp < t` and `ac + sp <= w - t` (no commit is chosen and unfinalized, since its `t` acceptors would
    * still hold it or be amnesiac, and nothing new can be chosen: no observer can see the commit rule's `ac`).
    *
    * The two never both hold, for any observers: a frozen store's vote and accept never change, a silent store counts
    * on both sides, and a store leaves a count only by deciding, after which it reports its outcome (rule 1). Commit
    * needs `vc >= r` where discard needs `vc + s < r`, and `ac > w - t` where discard needs `ac + sp <= w - t`.
    */
  private def decideUnknown(txd: TransactionDescription,
                            replies: Map[StoreId, Option[TxStatusResponse.TxStatus]]): Decision =
    val ida = txd.primaryObjectIDA
    val primary = poolStores(txd, txd.primaryObject.poolId).toList
    val ac = primary.count(st => replies.get(st).flatten.exists(_.accepted.contains(true)))
    val k = primary.count(st => replies.get(st).flatten.exists(_.amnesiac))
    val sp = primary.count(st => !replies.contains(st))
    val changedPools = (txd.allReferencedObjectsSet.filterNot(p => txd.lockOnlyObjects.contains(p.id))
      .map(_.poolId)).toList
    // Per changed pool: (commit voters holding the update, silent stores, read threshold)
    val data = changedPools.map: poolId =>
      val stores = poolStores(txd, poolId).toList
      val vc = stores.count(st => replies.get(st).flatten.exists(_.votedCommit))
      val s = stores.count(st => !replies.contains(st))
      (vc, s, txd.poolIDAMap(poolId).consistentRestoreThreshold)
    val margin = ida.width - ida.writeThreshold
    if ac > margin && data.forall((vc, _, r) => vc >= r) then
      Decision.Commit
    else if data.exists((vc, s, r) => vc + s < r) then
      Decision.Discard(dataLost = ac + k + sp >= ida.writeThreshold)
    else if ac + k + sp < ida.writeThreshold && ac + sp <= margin then
      Decision.Discard(dataLost = false)
    else
      Decision.Hold(undecidable = replies.size >= txd.allDataStores.size)
