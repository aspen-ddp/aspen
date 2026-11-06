package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.duration.*

/** StoreThreadPool's decisions, kept free of threads and clocks so they can be tested directly.
 *
 *  A load is the fraction of one thread's time spent running a store's tasks (0.5 is half a thread); a thread's load
 *  is the sum of its stores' loads. A thread is congested when its stores' work waits too long for it: their drains'
 *  average wait, from being queued on the thread to starting, is above `growWait`.
 *
 *  The policy packs stores onto as few threads as can carry them, because a parked thread costs nothing and every
 *  task that wakes one costs CPU: in the measurements, CPU per operation rose with every thread the same work was
 *  spread over (docs/host-threading-measurement.md). Threads are filled in index order up to `target`; a thread over
 *  it gives stores to busy threads that have room, and to an empty thread only if it is congested, since an
 *  uncongested thread above the target is still keeping up. Stores on the highest busy thread move down to lower ones
 *  with room (`consolidateBelow`) when nothing needs relieving, so a lighter load gathers back onto fewer threads.
 *  Only congestion makes the pool grow. */
object StoreBalancer:

  /** The pool's thresholds. They are starting points from one workload (docs/host-threading-measurement.md), not
   *  tuned values.
   *
   *  @param window            how far back store loads and drain waits are measured
   *  @param target            the load a thread is filled to; above it, its stores move to threads with room
   *  @param consolidateBelow  the most load a lower thread may reach by taking a store from the highest busy thread.
   *                           Below `target`, so a consolidated store is not moved straight back
   *  @param growWait          the average drain wait above which a thread is congested
   *  @param growAfter         consecutive checks needing a thread before one is added
   *  @param movesPerCheck     the most stores moved by one check
   *  @param settle            how long a moved store stays where it was put
   *  @param batch             the most tasks a store runs before its thread runs other stores' work
   *  @param reportEvery       checks between the pool's summary log lines while its stores are busy; 0 for none */
  final case class Config(
    window: FiniteDuration = 5.seconds,
    target: Double = 0.65,
    consolidateBelow: Double = 0.5,
    growWait: FiniteDuration = 1.millis,
    growAfter: Int = 5,
    movesPerCheck: Int = 2,
    settle: FiniteDuration = 10.seconds,
    batch: Int = 64,
    reportEvery: Int = 10):
    require(window > Duration.Zero, s"window must be positive: $window")
    require(target > 0, s"target must be positive: $target")
    require(consolidateBelow >= 0 && consolidateBelow <= target,
      s"consolidateBelow ($consolidateBelow) must be between 0 and target ($target)")
    require(growWait >= Duration.Zero, s"growWait must not be negative: $growWait")
    require(growAfter >= 1, s"growAfter must be at least 1: $growAfter")
    require(movesPerCheck >= 0, s"movesPerCheck must not be negative: $movesPerCheck")
    require(batch >= 1, s"batch must be at least 1: $batch")
    require(reportEvery >= 0, s"reportEvery must not be negative: $reportEvery")

  /** A placed store: the thread it is on, its load, and whether it may be moved (it has not been moved recently). */
  final case class Placed(storeId: StoreId, thread: Int, load: Double, movable: Boolean)

  final case class Move(storeId: StoreId, from: Int, to: Int)

  /** Thread loads by thread index. */
  def threadLoads(threads: Int, stores: Iterable[Placed]): Vector[Double] =
    stores.foldLeft(Vector.fill(threads)(0.0))((acc, s) => acc.updated(s.thread, acc(s.thread) + s.load))

  /** Stores on each thread, by thread index. */
  def storeCounts(threads: Int, stores: Iterable[Placed]): Vector[Int] =
    stores.foldLeft(Vector.fill(threads)(0))((acc, s) => acc.updated(s.thread, acc(s.thread) + 1))

  /** The thread a new store goes on: the lowest-index one that is below `target` and not congested, as the store's
   *  load is not known yet; failing that, the least loaded. */
  def placeNew(threads: Int, stores: Iterable[Placed], congested: Vector[Boolean], config: Config): Int =
    require(threads >= 1, "no thread to place a store on")
    val loads = threadLoads(threads, stores)
    (0 until threads).find(i => loads(i) < config.target && !congested(i))
      .getOrElse((0 until threads).minBy(i => (loads(i), i)))

  /** Whether the pool needs another thread: a thread is congested and none is empty to take stores from it. The pool
   *  also needs `growAfter` such checks in a row, room under its cap and more stores than threads. */
  def needsThread(threads: Int, stores: Iterable[Placed], congested: Vector[Boolean]): Boolean =
    congested.exists(identity) && storeCounts(threads, stores).forall(_ >= 1)

  /** The moves for one check, at most `movesPerCheck`, each store at most once.
   *
   *  Relief comes first: a thread over `target`, or congested, with more than one store gives its busiest movable
   *  store that fits to the lowest-index busy, uncongested thread with room for it (staying at most `target`), or,
   *  only if the giving thread is congested, to the lowest-index empty thread. A move must leave the receiving thread
   *  less loaded than the giving one was, so it never just swaps which is busy.
   *
   *  When no thread needs relief, consolidation: the highest-index busy thread gives its stores, least loaded first,
   *  to the lowest-index busy, uncongested thread below it that stays at most `consolidateBelow`. */
  def plan(threads: Int, stores: Iterable[Placed], congested: Vector[Boolean], config: Config): List[Move] =
    var placed = stores.toVector
    var moves = List.empty[Move]
    var done = threads < 2

    def move(storeId: StoreId, from: Int, to: Int): Unit =
      moves = Move(storeId, from, to) :: moves
      placed = placed.map(s => if s.storeId == storeId then s.copy(thread = to, movable = false) else s)

    def needsRelief(loads: Vector[Double], counts: Vector[Int])(i: Int): Boolean =
      counts(i) >= 2 && (loads(i) > config.target || congested(i))

    // Decided once, before any move: a check that relieves does not also consolidate
    val anyRelief = (0 until threads).exists(needsRelief(threadLoads(threads, placed), storeCounts(threads, placed)))

    while !done && moves.size < config.movesPerCheck do
      val loads = threadLoads(threads, placed)
      val counts = storeCounts(threads, placed)
      val relief: Option[Move] =
        if !anyRelief then None
        else
          val sources = (0 until threads).filter(needsRelief(loads, counts)).sortBy(i => (-loads(i), i))
          sources.iterator.flatMap: src =>
            val destinations = (0 until threads).filter: d =>
              d != src && !congested(d) && (counts(d) >= 1 || congested(src))
            placed.filter(s => s.thread == src && s.movable && s.load > 0).sortBy(s => -s.load).iterator.flatMap: s =>
              destinations.find: d =>
                val after = loads(d) + s.load
                (counts(d) == 0 || after <= config.target) && after < loads(src)
              .map(d => Move(s.storeId, src, d))
          .nextOption()

      relief match
        case Some(m) => move(m.storeId, m.from, m.to)
        case None if anyRelief => done = true
        case None =>
          val busy = (0 until threads).filter(counts(_) >= 1)
          val consolidation =
            if busy.size < 2 then None
            else
              val top = busy.last
              placed.filter(s => s.thread == top && s.movable).sortBy(s => (s.load, s.storeId.toString))
                .iterator.flatMap: s =>
                  busy.find(d => d < top && !congested(d) && loads(d) + s.load <= config.consolidateBelow)
                    .map(d => Move(s.storeId, top, d))
                .nextOption()
          consolidation match
            case Some(m) => move(m.storeId, m.from, m.to)
            case None => done = true
    moves.reverse
