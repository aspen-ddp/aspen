package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.util.concurrent.{Executor, Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** The production threading (`cmdline/Main.scala`): device management on one control thread, stores on a small pool
 *  of store threads (spec section 3; docs/host-threading-measurement.md for why).
 *
 *  - `control` and `controlEvents` are one non-daemon thread, `aspen-host-control`, as in SingleThreadThreading.
 *  - `dispatch` runs each task inline on the calling thread (network, I/O completion, timer or CRL callback), which
 *    routes it straight to the store's executor without a hop through the control thread.
 *  - Each store's executor is its own StoreMailbox, placed on one of the `aspen-store-N` daemon threads. Few, busy
 *    threads are cheaper than many idle ones: a task posted to a thread that is parked pays for waking it, so CPU per
 *    operation grows with the number of threads the same work is spread over.
 *
 *  By default (`adaptive` None) the pool is fixed: at most `maxThreads` threads (StoreThreadPool.DefaultThreads),
 *  created lazily, one per newly assigned store while every existing thread has a store; after that each store goes
 *  to the thread with the fewest stores, ties going to the lowest index. Stores never move. In the measurements a
 *  fixed pool of 2 to 4 threads beat both adaptive policies tried, so that is the default until adaptive placement is
 *  revisited with longer, more varied runs.
 *
 *  With `adaptive` set, the pool runs StoreBalancer's policy instead. Once a second (`checkPeriod`) a check on the
 *  control thread measures, over the last `config.window`, each store's load (its mailbox's busy time) and each
 *  thread's average drain wait (how long its stores' work waited for it), and packs stores onto as few threads as can
 *  carry them:
 *  - Threads: the first `minThreads` are created lazily, one per newly assigned store. Beyond them a thread is added
 *    only when one is congested (its drain wait is above `config.growWait`) and none is empty, for
 *    `config.growAfter` checks in a row, with more stores than threads and fewer than `maxThreads`. Threads are never
 *    removed before `shutdown`; one left without stores stays parked and costs nothing.
 *  - Placement: a new store goes to the lowest-index thread below `config.target` (StoreBalancer.placeNew). Each check
 *    moves at most `config.movesPerCheck` stores (StoreBalancer.plan): off a thread over the target or congested, and
 *    otherwise down from the highest busy thread onto lower ones with room. A moved store stays put for
 *    `config.settle`. StoreMailbox explains why a move never runs a store on two threads at once.
 *  - Every `config.reportEvery` checks while the stores are busy, it logs each thread's load and drain wait.
 *  Its known problems, from the measurements: idle load packs every store onto one thread, so the next burst starts
 *  from there; when every thread is over `target` it stops balancing; it moves near-idle stores to no effect; and
 *  seqwrite's normal drain wait is close to `growWait`, so it flips between thread counts.
 *
 *  The placement and load bookkeeping is read and written only on the control thread (`assign`, `release` and the
 *  check), or a test's thread, so it needs no lock. `loops` is volatile only because `shutdown` may be called from
 *  another thread.
 *
 *  Control tasks queued before `shutdown` still run after it, and one of them may load a store. `assign` then returns
 *  a mailbox on a loop that is already shut down, so the store's work is dropped (with SerialLoop's warning) and no
 *  thread is left running that nothing would stop.
 *
 *  An adaptive `checkPeriod` of None runs no checks; tests then call `check` themselves, with `nanoClock` a manual
 *  clock. */
class StoreThreadPool(
    maxThreads: Int = StoreThreadPool.DefaultThreads,
    adaptive: Option[StoreThreadPool.Adaptive] = None,
    nanoClock: () => Long = () => System.nanoTime()) extends HostThreading with Logging:

  private val minThreads = adaptive.fold(maxThreads)(_.minThreads)
  private val config = adaptive.fold(StoreBalancer.Config())(_.config)
  private val checkPeriod = adaptive.flatMap(_.checkPeriod)

  require(maxThreads >= 1, s"maxThreads must be at least 1: $maxThreads")
  require(minThreads >= 1, s"minThreads must be at least 1: $minThreads")
  require(maxThreads >= minThreads, s"maxThreads ($maxThreads) must be at least minThreads ($minThreads)")

  private val controlLoop = new SerialLoop("aspen-host-control", daemon = false)
  private val inline: Executor = task => task.run()

  @volatile private var shut = false
  @volatile private var loops = Vector.empty[SerialLoop]
  /** Every store loop this pool has created, including those created already shut down. Testing hook. */
  @volatile private var created = Vector.empty[SerialLoop]
  @volatile private var timer: Option[ScheduledExecutorService] = None

  /** A store's mailbox counters at one check. */
  private final case class Sample(time: Long, busy: Long, waited: Long, drains: Long)

  private final class Assigned(val mailbox: StoreMailbox, var thread: Int):
    /** The samples within the window, oldest first. */
    var samples: Vector[Sample] = Vector(sample(mailbox, nanoClock()))
    var load = 0.0
    /** Drain wait and drains over the window. */
    var waited = 0L
    var drains = 0L
    var movedAt: Option[Long] = None

  private def sample(m: StoreMailbox, now: Long) = Sample(now, m.busyNanos, m.waitNanos, m.drains)

  private var assigned = Map.empty[StoreId, Assigned]
  /** Whether each thread was congested at the last check, by thread index. */
  private var congested = Vector.empty[Boolean]
  /** Consecutive checks in which the pool needed another thread. */
  private var needyChecks = 0
  private var checks = 0
  private var movesMade = 0

  override def control: ExecutionContext = controlLoop
  override def controlEvents: Executor = controlLoop
  override def dispatch: Executor = inline

  override def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor =
    if assigned.contains(storeId) then
      throw HostThreading.AlreadyAssigned(storeId)
    if shut then
      // Never started, so no thread is left behind; work posted to it is dropped
      val dead = new SerialLoop(s"aspen-store-${loops.size}", daemon = true)
      dead.shutdown()
      created = created :+ dead
      new StoreMailbox(storeId, dead, config.batch, nanoClock)
    else
      val placed = placedStores
      val counts = (0 until loops.size).map(i => placed.count(_.thread == i))
      val thread =
        if loops.size < minThreads && counts.forall(_ >= 1) then
          addThread()
        else if adaptive.isEmpty then
          counts.indices.minBy(i => (counts(i), i))
        else
          StoreBalancer.placeNew(loops.size, placed, congestion, config)
      val mailbox = new StoreMailbox(storeId, loops(thread), config.batch, nanoClock)
      assigned += storeId -> new Assigned(mailbox, thread)
      mailbox

  override def release(storeId: StoreId): Unit = assigned -= storeId

  override def start(): Unit =
    controlLoop.start()
    checkPeriod.foreach: period =>
      if !shut && timer.isEmpty then
        val t = Executors.newSingleThreadScheduledExecutor: r =>
          val thread = new Thread(r, "aspen-store-balancer")
          thread.setDaemon(true)
          thread
        // The check itself runs on the control thread, which owns the bookkeeping
        t.scheduleAtFixedRate(() => controlLoop.execute(() => check()), period.toNanos, period.toNanos,
          TimeUnit.NANOSECONDS)
        timer = Some(t)

  override def shutdown(): Unit =
    shut = true
    timer.foreach(_.shutdownNow())
    controlLoop.shutdown()
    loops.foreach(_.shutdown())

  /** Measures the stores and threads, adds a thread if one is needed, and moves stores. Runs on the control thread
   *  (or a test's thread). */
  private[host] def check(): Unit =
    if adaptive.nonEmpty && !shut && loops.nonEmpty then
      val now = nanoClock()
      val windowStart = now - config.window.toNanos
      for a <- assigned.values do
        val latest = sample(a.mailbox, now)
        // The oldest sample kept is the newest one at or before the window's start, so the figures cover the whole
        // window once the store has been assigned that long, and the time since its assignment until then
        var samples = a.samples :+ latest
        while samples.size >= 2 && samples(1).time <= windowStart do
          samples = samples.tail
        a.samples = samples
        val first = samples.head
        a.load = if now > first.time then (latest.busy - first.busy).toDouble / (now - first.time) else 0.0
        a.waited = latest.waited - first.waited
        a.drains = latest.drains - first.drains

      val waits = threadWaits
      congested = waits.map(_.exists(_ > config.growWait.toNanos))
      val placed = placedStores
      needyChecks = if StoreBalancer.needsThread(loops.size, placed, congested) then needyChecks + 1 else 0
      if needyChecks >= config.growAfter && loops.size < maxThreads && assigned.size > loops.size then
        needyChecks = 0
        val worst = waits.flatten.maxOption.getOrElse(0.0)
        val added = addThread()
        congested = congested :+ false
        logger.info(f"Added store thread aspen-store-$added: work waited up to ${worst / 1e6}%.2f ms for its store " +
          "thread on average")

      val moves = StoreBalancer.plan(loops.size, placedStores, congested, config)
      for m <- moves do
        val a = assigned(m.storeId)
        a.mailbox.moveTo(loops(m.to))
        a.thread = m.to
        a.movedAt = Some(now)
        movesMade += 1
        logger.info(f"Moved store ${m.storeId} (${a.load * 100}%.0f%% busy) from aspen-store-${m.from} to " +
          s"aspen-store-${m.to}")

      checks += 1
      if config.reportEvery > 0 && checks % config.reportEvery == 0 then
        val loads = StoreBalancer.threadLoads(loops.size, placedStores)
        if loads.sum > 0.01 then
          val counts = StoreBalancer.storeCounts(loops.size, placedStores)
          val waitsNow = threadWaits
          val parts = loops.indices.map: i =>
            val wait = waitsNow(i).fold("-")(w => f"${w / 1e6}%.2f ms")
            f"aspen-store-$i ${counts(i)} stores ${loads(i) * 100}%.0f%% busy wait $wait"
          logger.info(s"Store threads: ${counts.count(_ > 0)} in use of ${loops.size} (at most $maxThreads): " +
            parts.mkString("; "))

  /** Each thread's average drain wait over the window, by thread index; None for a thread that ran no drains. */
  private def threadWaits: Vector[Option[Double]] =
    val waited = Array.fill(loops.size)(0L)
    val drains = Array.fill(loops.size)(0L)
    for a <- assigned.values do
      waited(a.thread) += a.waited
      drains(a.thread) += a.drains
    loops.indices.toVector.map(i => if drains(i) > 0 then Some(waited(i).toDouble / drains(i)) else None)

  /** The congestion seen by the last check, padded for threads added since. */
  private def congestion: Vector[Boolean] = congested.padTo(loops.size, false)

  private def placedStores: Vector[StoreBalancer.Placed] =
    val now = nanoClock()
    assigned.values.toVector.map: a =>
      val movable = a.movedAt.forall(t => now - t >= config.settle.toNanos)
      StoreBalancer.Placed(a.mailbox.storeId, a.thread, a.load, movable)

  /** Creates and starts the next store thread and returns its index. */
  private def addThread(): Int =
    val loop = new SerialLoop(s"aspen-store-${loops.size}", daemon = true)
    loop.start()
    created = created :+ loop
    loops = loops :+ loop
    // A shutdown racing this: it either saw the new loop in `loops` or is seen here (both volatile)
    if shut then
      loop.shutdown()
    loops.size - 1

  private[host] def threadCount: Int = loops.size
  private[host] def createdLoops: Vector[SerialLoop] = created
  /** Stores on each thread, by thread index. */
  private[host] def assignedCounts: Vector[Int] =
    assigned.values.foldLeft(Vector.fill(loops.size)(0))((acc, a) => acc.updated(a.thread, acc(a.thread) + 1))
  private[host] def threadOf(storeId: StoreId): Option[Int] = assigned.get(storeId).map(_.thread)
  private[host] def loadOf(storeId: StoreId): Option[Double] = assigned.get(storeId).map(_.load)
  private[host] def moveCount: Int = movesMade
  private[host] def waitOf(thread: Int): Option[Double] = threadWaits.lift(thread).flatten

object StoreThreadPool:
  /** A quarter of the CPUs, at most 4 and at least 1: the store threads share the host with the network, FSIF and GC
   *  threads, and in the measurements fixed pools of 2 and 4 threads did best (docs/host-threading-measurement.md). */
  def DefaultThreads: Int = math.max(1, math.min(4, Runtime.getRuntime.availableProcessors() / 4))

  /** The adaptive policy's settings; see the class comment. */
  final case class Adaptive(
    minThreads: Int = 1,
    config: StoreBalancer.Config = StoreBalancer.Config(),
    checkPeriod: Option[FiniteDuration] = Some(1.second))
