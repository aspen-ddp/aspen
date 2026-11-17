package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.TxStatusResponse.TxStatus
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ExclusiveRevisionLock,
  TransactionDescription, TransactionObjectRequirement, TransactionStatus}
import org.aspen_ddp.aspen.server.store.FrontendSuite
import org.aspen_ddp.aspen.server.transaction.ExpiryTally.Decision
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/** Exhaustive check of ExpiryTally rules A-G over the true final states the protocol can produce: no two observers
  * of one state decide commit and discard, and no observer commits over a chosen abort. Reported states are final (a
  * frozen Tx's vote and accepts never change), so observers differ only in which stores are silent to them.
  *
  * The model is the true final state of each store: its vote (commit, abort, or none: it never joined T), its latest
  * accept (primary pool only), whether it was rebuilt (amnesiac), and whether it forgot T (it finalized or discarded T
  * and its cached outcome expired). Only states the protocol can produce are kept:
  *  - an accepted commit only if every pool truly has its write threshold `t` of commit votes (L2; a proof of commit
  *    implies one too)
  *  - an accepted abort only if some pool truly has more than `w - t` non-commit votes (L1)
  *  - at most `t` amnesiac stores per pool; a store that never joined T accepted nothing
  * A store's reply derives from its true state: an amnesiac store replies amnesiac whatever it held; a store that never
  * joined T, or forgot it, replies "unknown" (no Tx, below its floor); any other replies as a frozen holder. In a
  * secondary pool an abort voter and a store that never joined count the same in every rule (neither is a commit
  * voter, amnesiac or silent), so they are one state there. In the primary pool they differ: F counts "unknown" replies
  * as possibly hiding an accept.
  *
  * Pools: changed or lock-only primary, and a changed or lock-only secondary. Stores of a pool with the same true state
  * are interchangeable, so the true states are enumerated as multisets of per-store states, and silent sets as
  * multisets of the silenced stores' states. Each observer omits any subset of up to two stores.
  */
class ExpiryTallyConsistencySuite extends AnyFunSuite with org.scalatest.ParallelTestExecution:
  import FrontendSuite.{op1, otherPoolId, poolId, rev0}

  private enum Vote:
    case Commit, Abort, NotJoined

  /** One store's true final state */
  private case class St(amnesiac: Boolean, vote: Vote, accepted: Option[Boolean], forgotten: Boolean = false):
    val reply: Option[TxStatus] =
      if amnesiac then
        Some(TxStatus(TransactionStatus.Unresolved, finalized = false, late = false, holdsAcceptorState = true,
          amnesiac = true))
      else if forgotten || vote == Vote.NotJoined then
        None
      else
        Some(TxStatus(TransactionStatus.Unresolved, finalized = false, late = false, holdsAcceptorState = true,
          accepted = accepted, votedCommit = vote == Vote.Commit))
    def commitVote: Boolean = vote == Vote.Commit
    /** Distinct for distinct states: the key silent sets are deduplicated by */
    val ord: Int = (if amnesiac then 1 else 0) + 2 * vote.ordinal + 6 * accepted.fold(0)(a => if a then 1 else 2) +
      18 * (if forgotten then 1 else 0)
    override def toString: String =
      (if amnesiac then "amnesiac " else "") + (if forgotten then "forgotten " else "") + vote +
        accepted.fold("")(a => s"/accepted ${if a then "commit" else "abort"}")

  private val primaryStates: Vector[St] =
    val joined = for
      a <- Vector(false, true)
      v <- Vector(Vote.Commit, Vote.Abort)
      acc <- Vector(None, Some(true), Some(false))
    yield St(a, v, acc)
    joined :+ St(amnesiac = false, Vote.NotJoined, None)
  private val secondaryStates: Vector[St] =
    for
      a <- Vector(false, true)
      v <- Vector(Vote.Commit, Vote.NotJoined)
    yield St(a, v, None)

  /** Every multiset of `n` elements of `states`, as non-decreasing index sequences */
  private def multisets(states: Vector[St], n: Int): Iterator[Vector[St]] =
    def go(from: Int, n: Int): Iterator[List[Int]] =
      if n == 0 then Iterator(Nil)
      else (from until states.size).iterator.flatMap(i => go(i, n - 1).map(i :: _))
    go(0, n).map(_.map(states).toVector)

  private case class Config(name: String, txd: TransactionDescription, pools: List[(PoolId, IDA, Vector[St])])

  private val otherPtr = DataObjectPointer(ObjectId(new UUID(0, 99)), otherPoolId, Array[Byte]())

  private def config(primary: IDA, secondary: Option[IDA], primaryLockOnly: Boolean = false,
                     secondaryLockOnly: Boolean = false): Config =
    val primaryReq: TransactionObjectRequirement =
      if primaryLockOnly then ExclusiveRevisionLock(op1, rev0) else DataUpdate(op1, rev0, DataUpdateOperation.Overwrite)
    val extra: List[TransactionObjectRequirement] = secondary.toList.map: _ =>
      if secondaryLockOnly then ExclusiveRevisionLock(otherPtr, rev0)
      else DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)
    val txd = FrontendSuite.mktxd().copy(primaryObjectIDA = primary,
      poolIDAMap = Map(poolId -> primary) ++ secondary.map(otherPoolId -> _),
      requirements = primaryReq :: extra)
    val pools = (poolId, primary, primaryStates) :: secondary.toList.map(s => (otherPoolId, s, secondaryStates))
    def kind(lockOnly: Boolean) = if lockOnly then "lock-only" else "changed"
    Config(s"${kind(primaryLockOnly)} primary $primary" +
      secondary.fold("")(s => s", ${kind(secondaryLockOnly)} secondary $s"), txd, pools)

  private val wide = List(Replication(5, 3), Replication(5, 4), ReedSolomon(5, 3, 4))

  private val narrow = Replication(3, 2)
  private val single = Replication(1, 1)

  /** Every variant of the Replication(3, 2) primary with a Replication(3, 2) secondary, and of the width-5 primaries
    * with `wideSecondary` */
  private def variants(wideSecondary: IDA, wideBothChanged: List[IDA]): List[Config] =
    List(config(narrow, None), config(narrow, Some(narrow)), config(narrow, Some(narrow), primaryLockOnly = true),
      config(narrow, Some(narrow), secondaryLockOnly = true)) ++
      wide.flatMap(p => List(config(p, None), config(p, Some(wideSecondary), primaryLockOnly = true),
        config(p, Some(wideSecondary), secondaryLockOnly = true))) ++
      wideBothChanged.flatMap(p => List(config(p, Some(narrow))))

  /** The width-5 primaries take a width-1 secondary in the lock-only variants, which keeps the run short, and a
    * Replication(3, 2) one with both objects changed */
  private val configs: List[Config] = variants(single, wide)

  /** With forgotten stores the states multiply, so every width-5 variant takes the width-1 secondary */
  private val forgottenConfigs: List[Config] = variants(single, Nil) ++ wide.map(p => config(p, Some(single)))

  /** One true state of every store of a config, with the facts the checks need */
  private final class World(val cfg: Config, val byPool: List[(PoolId, IDA, Vector[St])]):
    val stores: Vector[(StoreId, St)] =
      byPool.flatMap((pid, _, sts) => sts.zipWithIndex.map((st, i) => StoreId(pid, i.toByte) -> st)).toVector
    private def commits(sts: Vector[St]) = sts.count(_.commitVote)
    val commitCertain: Boolean = byPool.forall((_, ida, sts) => commits(sts) >= ida.writeThreshold)
    val mayAbort: Boolean = byPool.exists((_, ida, sts) => ida.width - commits(sts) > ida.width - ida.writeThreshold)
    private val primaryIda = cfg.txd.primaryObjectIDA
    /** At least a quorum of primary stores truly accepted abort: abort was chosen, and a client may have been told */
    val chosenAbort: Boolean = byPool.head._3.count(_.accepted.contains(false)) >= primaryIda.writeThreshold
    def valid: Boolean =
      val all = byPool.flatMap(_._3)
      val accCommit = all.exists(_.accepted.contains(true))
      val accAbort = all.exists(_.accepted.contains(false))
      byPool.forall((_, ida, sts) => sts.count(_.amnesiac) <= ida.writeThreshold) &&
        (!accCommit || commitCertain) && (!accAbort || mayAbort)

    private def name(id: StoreId) = s"${if id.poolId == poolId then "p" else "s"}${id.poolIndex}"
    def describe(silent: Set[StoreId]): String =
      s"${cfg.name}; true states ${stores.map((id, st) => s"${name(id)}=$st").mkString(", ")}; " +
        s"silent ${silent.map(name).mkString("{", ", ", "}")}"

    private val byId = stores.toMap
    /** A store's (pool, state), as one Int */
    def key(id: StoreId): Int = (if id.poolId == poolId then 0 else 64) + byId(id).ord

    /** Every subset of up to two stores, deduplicated by the silenced stores' (pool, state) */
    lazy val silentSets: List[Set[StoreId]] =
      dedup(Iterator(Set.empty[StoreId]) ++ stores.indices.iterator.map(i => Set(stores(i)._1)) ++
        (for i <- stores.indices.iterator; j <- (i + 1) until stores.size yield Set(stores(i)._1, stores(j)._1)))

    /** silentSets with at most `pairs` of its two-store sets, drawn by `rnd` */
    def sampledSilentSets(pairs: Int, rnd: scala.util.Random): List[Set[StoreId]] =
      val (small, two) = silentSets.partition(_.size < 2)
      small ++ (if two.size <= pairs then two else rnd.shuffle(two).take(pairs))

    /** One set of each multiset of (pool, state)s */
    def dedup(sets: Iterator[Set[StoreId]]): List[Set[StoreId]] =
      val seen = scala.collection.mutable.HashSet.empty[List[Int]]
      sets.filter(s => seen.add(s.toList.map(key).sorted)).toList
    /** Whether store `x` can make a view equivalent to silent set `s` (it is never silent to itself): `s` stands for
      * every set silencing the same (pool, state)s, and one of those leaves `x` out unless it silences every store of
      * `x`'s type */
    def observableBy(x: StoreId, s: Set[StoreId]): Boolean =
      val k = key(x)
      s.count(key(_) == k) < stores.count((id, _) => key(id) == k)

    def replies(silent: Set[StoreId]): Map[StoreId, Option[TxStatus]] =
      stores.iterator.collect { case (id, st) if !silent.contains(id) => id -> st.reply }.toMap

    /** Whether a view's Discard was rule C: A and B did not fire (it discarded), and it saw an accepted abort */
    def discardByC(rs: Map[StoreId, Option[TxStatus]]): Boolean =
      rs.exists((id, r) => id.poolId == poolId && r.exists(s => !s.amnesiac && s.accepted.contains(false)))

    /** Whether a view's Commit was rule A or B (else F) */
    def commitByAOrB(rs: Map[StoreId, Option[TxStatus]]): Boolean =
      rs.valuesIterator.flatten.exists(s => !s.amnesiac && s.accepted.contains(true)) || cfg.pools.forall:
        (pid, ida, _) => rs.count((id, r) => id.poolId == pid && r.exists(_.votedCommit)) >= ida.writeThreshold

    /** This world with the stores of `ids` forgetting T */
    def forgetting(ids: Set[StoreId]): World =
      new World(cfg, byPool.map: (pid, ida, sts) =>
        (pid, ida, sts.zipWithIndex.map((st, i) => if ids.contains(StoreId(pid, i.toByte)) then st.copy(forgotten = true)
          else st)))

  /** Every valid true state of every config, without forgotten stores */
  private def worlds(configs: List[Config]): Iterator[World] =
    configs.iterator.flatMap: cfg =>
      val perPool = cfg.pools.map((_, ida, states) => multisets(states, ida.width).toVector)
      val combos = perPool.foldRight(Iterator(List.empty[Vector[St]]))((ms, rest) =>
        val restV = rest.toVector
        ms.iterator.flatMap(m => restV.iterator.map(m :: _)))
      combos.map(poolStates => new World(cfg, cfg.pools.zip(poolStates).map((p, sts) => (p._1, p._2, sts))))
        .filter(_.valid)

  /** The checks every observer's decision must pass */
  private def checkDecision(w: World, silent: Set[StoreId], rs: Map[StoreId, Option[TxStatus]], d: Decision,
                            forgotten: Boolean): Unit = d match
    case Decision.Commit =>
      if w.chosenAbort then fail(s"Committed over a chosen abort: ${w.describe(silent)}")
      if w.commitByAOrB(rs) && !w.commitCertain then
        fail(s"A or B committed with no commit-certain tally: ${w.describe(silent)}")
    case Decision.Discard(false) if w.commitCertain && !forgotten =>
      fail(s"Discarded without dataLost though a tally was commit-certain: ${w.describe(silent)}")
    case _ =>

  /** Every view of `w` with its replies and decision, each passing checkDecision */
  private def views(w: World, forgotten: Boolean,
                    silentSets: Option[List[Set[StoreId]]] = None)
  : List[(Set[StoreId], Map[StoreId, Option[TxStatus]], Decision)] =
    silentSets.getOrElse(w.silentSets).map: silent =>
      val rs = w.replies(silent)
      val d = ExpiryTally.decide(w.cfg.txd, rs)
      checkDecision(w, silent, rs, d, forgotten)
      (silent, rs, d)

  /** Fails on a commit and a discard among the views of a world without forgotten stores */
  private def checkNoSplit(w: World, vs: List[(Set[StoreId], Map[StoreId, Option[TxStatus]], Decision)]): Unit =
    for c <- vs.find(_._3 == Decision.Commit); x <- vs.find(_._3.isInstanceOf[Decision.Discard]) do
      fail(s"Split decision: Commit with ${w.describe(c._1)} versus ${x._3} with silent ${x._1}")

  test("no two observers decide commit and discard, and none commits over a chosen abort"):
    var checked = 0L
    worlds(configs).foreach: w =>
      val vs = views(w, forgotten = false)
      checked += vs.size
      checkNoSplit(w, vs)
    info(s"$checked decisions checked")

  /** Stores of `w` that may forget T, up to two, deduplicated by (pool, state): joined and not rebuilt */
  private def forgettable(w: World): List[Set[StoreId]] =
    val candidates = w.stores.filter((_, st) => !st.amnesiac && st.vote != Vote.NotJoined)
    val subsets = candidates.indices.iterator.map(i => Set(candidates(i)._1)) ++
      (for i <- candidates.indices.iterator; j <- (i + 1) until candidates.size
       yield Set(candidates(i)._1, candidates(j)._1))
    w.dedup(subsets)

  // A store forgets T only once T is decided: it learned a chosen abort, or a commit (chosen, or decided by expiry),
  // and was finalized; or it discarded T by expiry itself. Then its cached outcome expires and it answers "unknown".
  // The one divergence accepted (by ruling): an observer discards by rule C over an abort that was never chosen, the
  // accepter later forgets T, and another observer commits by F. No client was told abort (abort was never chosen); F's
  // `vc >= r` voters hold T's locks, so no conflicting commit exists (`t + r > w`); and the finalization's missed-update
  // marks repair the discarding store. Discards of a finalized commit are safe through the discarding commit voter's
  // UnackedCommits lookup (paxos-transactions 10.12), so they are counted, not failed
  /** How many two-store silent sets each world with forgotten stores is checked with. Its base world (no store
    * forgotten) is checked with all of them, and so is every one-store or empty silent set */
  private val SampledPairs = 4

  // One test per primary IDA, so that they run in parallel
  (narrow :: wide).foreach: primaryIda =>
    test(s"stores that forgot T never make an observer commit over a chosen abort or split from a store's own " +
        s"discard: primary $primaryIda"):
      forgottenStores(forgottenConfigs.filter(_.txd.primaryObjectIDA == primaryIda))

  private def forgottenStores(cfgs: List[Config]): Unit =
    var checked = 0L
    var commitFinalizedDiscards = 0L
    var acceptedDivergences = 0L
    val rnd = new scala.util.Random(20261116)
    worlds(cfgs).foreach: w =>
      // The base world's views, decided once
      val base = views(w, forgotten = false)
      checked += base.size
      checkNoSplit(w, base)
      val commitOutcome = w.commitCertain || base.exists(_._3 == Decision.Commit)
      forgettable(w).foreach: fg =>
        // Each forgetter's own discard views (it is never silent to itself), and whether any is not by rule C
        val ownDiscards = fg.toList.map: x =>
          x -> base.filter((s, _, d) => w.observableBy(x, s) && d.isInstanceOf[Decision.Discard])
        val discarded = ownDiscards.forall(_._2.nonEmpty)
        val discardedNotByC = discarded && ownDiscards.exists(_._2.exists((_, rs, _) => !w.discardByC(rs)))
        if w.chosenAbort || commitOutcome || discarded then
          val after = w.forgetting(fg)
          val vs = views(after, forgotten = true, Some(after.sampledSilentSets(SampledPairs, rnd)))
          checked += vs.size
          val commit = vs.find(_._3 == Decision.Commit)
          val discards = vs.filter(_._3.isInstanceOf[Decision.Discard])
          commit.foreach: (cs, _, _) =>
            if discarded && !w.chosenAbort && !commitOutcome then
              // T was discarded by the forgetters, never committed
              if discardedNotByC then
                fail(s"Split decision: a store that forgot T discarded it not by rule C, and ${after.describe(cs)} " +
                  "commits")
              discards.find((_, rs, _) => !after.discardByC(rs)).foreach: (xs, _, xd) =>
                fail(s"Split decision: $xd not by rule C with silent $xs versus Commit with ${after.describe(cs)}")
              acceptedDivergences += 1
            else if commitOutcome && discards.nonEmpty then
              commitFinalizedDiscards += 1
    info(s"$checked decisions checked; $acceptedDivergences rule-C-then-F divergences (accepted); " +
      s"$commitFinalizedDiscards states with a discard beside a commit outcome (safe by the UnackedCommits lookup)")
