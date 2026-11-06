package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.duration.*

/** StoreThreadPool's decisions, kept free of threads and clocks so they can be tested directly.
 *
 *  A load is the fraction of one thread's time spent running a store's tasks (0.5 is half a thread); a thread's load
 *  is the sum of its stores' loads. */
object StoreBalancer:

  /** The pool's thresholds. They are starting points from one workload (docs/host-threading-measurement.md), not
   *  tuned values.
   *
   *  @param window         how far back store loads are measured
   *  @param growAbove      the average thread load above which the pool is short of threads
   *  @param growAfter      consecutive checks above `growAbove` before a thread is added
   *  @param imbalance      the gap between the busiest and the least busy thread above which stores are moved,
   *                        outside the checks that add a thread (those always rebalance)
   *  @param movesPerCheck  the most stores moved by one check
   *  @param settle         how long a moved store stays where it was put
   *  @param batch          the most tasks a store runs before its thread runs other stores' work */
  final case class Config(
    window: FiniteDuration = 5.seconds,
    growAbove: Double = 0.7,
    growAfter: Int = 5,
    imbalance: Double = 0.3,
    movesPerCheck: Int = 2,
    settle: FiniteDuration = 10.seconds,
    batch: Int = 64):
    require(window > Duration.Zero, s"window must be positive: $window")
    require(growAfter >= 1, s"growAfter must be at least 1: $growAfter")
    require(imbalance >= 0, s"imbalance must not be negative: $imbalance")
    require(movesPerCheck >= 0, s"movesPerCheck must not be negative: $movesPerCheck")
    require(batch >= 1, s"batch must be at least 1: $batch")

  /** A placed store: the thread it is on, its load, and whether it may be moved (it has not been moved recently). */
  final case class Placed(storeId: StoreId, thread: Int, load: Double, movable: Boolean)

  final case class Move(storeId: StoreId, from: Int, to: Int)

  /** Thread loads by thread index. */
  def threadLoads(threads: Int, stores: Iterable[Placed]): Vector[Double] =
    stores.foldLeft(Vector.fill(threads)(0.0))((acc, s) => acc.updated(s.thread, acc(s.thread) + s.load))

  /** The thread a new store goes on: the least loaded, then the one with the fewest stores, then the lowest index. */
  def placeNew(threads: Int, stores: Iterable[Placed]): Int =
    require(threads >= 1, "no thread to place a store on")
    val loads = threadLoads(threads, stores)
    val counts = stores.foldLeft(Vector.fill(threads)(0))((acc, s) => acc.updated(s.thread, acc(s.thread) + 1))
    (0 until threads).minBy(i => (loads(i), counts(i), i))

  /** Whether the average thread load is above `growAbove`. Growth also needs `growAfter` such checks in a row, room
   *  under the pool's cap and more stores than threads; StoreThreadPool checks those. */
  def overloaded(threads: Int, stores: Iterable[Placed], config: Config): Boolean =
    threads >= 1 && stores.iterator.map(_.load).sum / threads > config.growAbove

  /** Moves that even out the thread loads, greedily: the busiest thread gives the least busy one the movable store
   *  that brings their two loads closest, provided that lowers the busiest thread's load. Stops after
   *  `movesPerCheck` moves, when no store helps, or (unless `force`, as right after a thread is added) once the gap
   *  between them is at most `imbalance`. A store moves at most once per plan. */
  def plan(threads: Int, stores: Iterable[Placed], config: Config, force: Boolean): List[Move] =
    var placed = stores.toVector
    var moves = List.empty[Move]
    var done = threads < 2
    while !done && moves.size < config.movesPerCheck do
      val loads = threadLoads(threads, placed)
      val hi = loads.indices.maxBy(i => (loads(i), -i))
      val lo = loads.indices.minBy(i => (loads(i), i))
      val gap = loads(hi) - loads(lo)
      if gap <= 0 || (!force && gap <= config.imbalance) then
        done = true
      else
        // Moving load l leaves the pair at hi - l and lo + l; it helps only while l < gap
        val candidates = placed.filter(s => s.thread == hi && s.movable && s.load > 0 && s.load < gap)
        if candidates.isEmpty then
          done = true
        else
          val best = candidates.minBy(s => (math.abs(s.load - gap / 2), s.storeId.toString))
          moves = Move(best.storeId, hi, lo) :: moves
          placed = placed.map: s =>
            if s.storeId == best.storeId then s.copy(thread = lo, movable = false) else s
    moves.reverse
