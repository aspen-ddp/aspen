package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.TxStatusResponse.TxStatus
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, TransactionDescription,
  TransactionStatus}
import org.aspen_ddp.aspen.server.store.FrontendSuite
import org.aspen_ddp.aspen.server.transaction.ExpiryTally.Decision
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/** Exhaustive check that rules A-G of ExpiryTally never let two observers of the same final state decide commit and
  * discard. Reported states are final (a frozen Tx's vote and accepts never change), so observers differ only in which
  * stores are silent to them.
  *
  * The model is the true final state of each store: whether it voted commit, its latest accept (primary pool only)
  * and whether it was rebuilt (amnesiac). An abort vote and no vote are one state here: L1 below counts non-commit
  * votes, and their replies (a frozen holder that did not vote commit, or "unknown") count the same in every rule,
  * since neither is a commit voter, amnesiac or silent. Only states the protocol can produce are kept:
  *  - an accepted commit only if every pool truly has its write threshold `t` of commit votes (L2; a proof of commit
  *    implies one too)
  *  - an accepted abort only if some pool truly has more than `w - t` non-commit votes (L1)
  *  - at most `t` amnesiac stores per pool
  * A store's reply derives from its true state: an amnesiac store replies amnesiac whatever it held; a store with no
  * commit vote and no accept replies "unknown" (no Tx, below its floor); any other replies as a frozen holder.
  *
  * Stores of a pool with the same true state are interchangeable, so the true states are enumerated as multisets of
  * per-store states, and silent sets as multisets of the silenced stores' states. Each observer omits any subset of up
  * to two stores. "No pair decides commit and discard" is the same as "the set of decisions over all silent sets does
  * not contain both"
  */
class ExpiryTallyConsistencySuite extends AnyFunSuite:
  import FrontendSuite.{op1, otherPoolId, poolId, rev0}

  private enum Vote:
    case Commit, NonCommit

  /** One store's true final state */
  private case class St(amnesiac: Boolean, vote: Vote, accepted: Option[Boolean]):
    val reply: Option[TxStatus] =
      if amnesiac then
        Some(TxStatus(TransactionStatus.Unresolved, finalized = false, late = false, holdsAcceptorState = true,
          amnesiac = true))
      else if vote == Vote.NonCommit && accepted.isEmpty then
        None
      else
        Some(TxStatus(TransactionStatus.Unresolved, finalized = false, late = false, holdsAcceptorState = true,
          accepted = accepted, votedCommit = vote == Vote.Commit))

  private val primaryStates: Vector[St] =
    for
      a <- Vector(false, true)
      v <- Vote.values.toVector
      acc <- Vector(None, Some(true), Some(false))
    yield St(a, v, acc)
  private val secondaryStates: Vector[St] =
    for
      a <- Vector(false, true)
      v <- Vote.values.toVector
    yield St(a, v, None)

  /** Every multiset of `n` elements of `states`, as non-decreasing index sequences */
  private def multisets(states: Vector[St], n: Int): Iterator[Vector[St]] =
    def go(from: Int, n: Int): Iterator[List[Int]] =
      if n == 0 then Iterator(Nil)
      else (from until states.size).iterator.flatMap(i => go(i, n - 1).map(i :: _))
    go(0, n).map(_.map(states).toVector)

  private case class Config(name: String, txd: TransactionDescription, pools: List[(PoolId, IDA, Vector[St])])

  private val otherPtr = DataObjectPointer(ObjectId(new UUID(0, 99)), otherPoolId, Array[Byte]())

  private def config(primary: IDA, withSecondary: Boolean): Config =
    val secondary = Replication(3, 2)
    val extra = if withSecondary then List(DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)) else Nil
    val txd = FrontendSuite.mktxd().copy(primaryObjectIDA = primary,
      poolIDAMap = Map(poolId -> primary) ++ (if withSecondary then Map(otherPoolId -> secondary) else Map()),
      requirements = DataUpdate(op1, rev0, DataUpdateOperation.Overwrite) :: extra)
    val pools = (poolId, primary, primaryStates) ::
      (if withSecondary then List((otherPoolId, secondary, secondaryStates)) else Nil)
    Config(s"primary $primary${if withSecondary then s", secondary $secondary" else ""}", txd, pools)

  private val configs =
    for
      primary <- List(Replication(3, 2), Replication(5, 3), Replication(5, 4), ReedSolomon(5, 3, 4))
      secondary <- List(false, true)
    yield config(primary, secondary)

  /** Every subset of up to two stores, deduplicated by the silenced stores' (pool, state) */
  private def silentSets(stores: Vector[(StoreId, St)]): List[Set[StoreId]] =
    val subsets = Iterator(Set.empty[StoreId]) ++ stores.indices.iterator.map(i => Set(stores(i)._1)) ++
      (for i <- stores.indices.iterator; j <- (i + 1) until stores.size yield Set(stores(i)._1, stores(j)._1))
    val byId = stores.toMap
    subsets.toList.groupBy(s => s.toList.map(id => (id.poolId, byId(id))).sortBy(_.toString)).values.map(_.head)
      .toList

  test("no two observers decide commit and discard"):
    var checked = 0L
    configs.foreach: cfg =>
      val perPool = cfg.pools.map((pid, ida, states) => multisets(states, ida.width).toVector)
      // The cartesian product of the pools' multisets
      val combos = perPool.foldRight(Iterator(List.empty[Vector[St]]))((ms, rest) =>
        val restV = rest.toVector
        ms.iterator.flatMap(m => restV.iterator.map(m :: _)))
      combos.foreach: poolStates =>
        // Per pool: its id, IDA and the true states of its stores
        val byPool = cfg.pools.zip(poolStates).map((p, sts) => (p._1, p._2, sts))
        def commits(sts: Vector[St]) = sts.count(_.vote == Vote.Commit)
        val commitCertain = byPool.forall((_, ida, sts) => commits(sts) >= ida.writeThreshold)
        val mayAbort = byPool.exists((_, ida, sts) => ida.width - commits(sts) > ida.width - ida.writeThreshold)
        val accCommit = poolStates.exists(_.exists(_.accepted.contains(true)))
        val accAbort = poolStates.exists(_.exists(_.accepted.contains(false)))
        val amnesiacOk = byPool.forall((_, ida, sts) => sts.count(_.amnesiac) <= ida.writeThreshold)
        if amnesiacOk && (!accCommit || commitCertain) && (!accAbort || mayAbort) then
          val stores = byPool.flatMap((pid, _, sts) => sts.zipWithIndex.map((st, i) => StoreId(pid, i.toByte) -> st))
            .toVector
          val all = stores.toMap
          val primaryT = cfg.txd.primaryObjectIDA.writeThreshold
          val primaryAcceptedAborts = poolStates.head.count(_.accepted.contains(false))
          def describe(silent: Set[StoreId]) =
            def name(id: StoreId) = s"${if id.poolId == poolId then "p" else "s"}${id.poolIndex}"
            s"${cfg.name}; true states ${stores.map((id, st) => s"${name(id)}=$st").mkString(", ")}; " +
              s"silent ${silent.map(name).mkString("{", ", ", "}")}"
          val decisions = silentSets(stores).map: silent =>
            checked += 1
            val replies = all.collect { case (id, st) if !silent.contains(id) => id -> st.reply }
            val d = ExpiryTally.decide(cfg.txd, replies)
            d match
              case Decision.Commit =>
                // Which rule committed: A (every pool at t commit votes seen) or B (an accept of commit seen), else F
                val seen = replies.valuesIterator.flatten.toList
                val aOrB = seen.exists(s => !s.amnesiac && s.accepted.contains(true)) || cfg.pools.forall:
                  (pid, ida, _) => replies.count((id, r) => id.poolId == pid && r.exists(_.votedCommit)) >= ida.writeThreshold
                if aOrB && !commitCertain then fail(s"A or B committed with no commit-certain tally: ${describe(silent)}")
                if !aOrB && primaryAcceptedAborts >= primaryT then fail(s"F committed over a chosen abort: ${describe(silent)}")
              case Decision.Discard(false) if commitCertain =>
                fail(s"Discarded without dataLost though a tally was commit-certain: ${describe(silent)}")
              case _ =>
            (d, silent)
          val commit = decisions.find(_._1 == Decision.Commit)
          val discard = decisions.find(_._1.isInstanceOf[Decision.Discard])
          for c <- commit; x <- discard do
            fail(s"Split decision: ${c._1} with ${describe(c._2)} versus ${x._1} with silent " +
              s"${x._2.map(id => s"${if id.poolId == poolId then "p" else "s"}${id.poolIndex}").mkString("{", ", ", "}")}")
    info(s"$checked decisions checked")
