package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.util.concurrent.{Executor, Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** The production threading (`cmdline/Main.scala`): device management on one control thread, stores on a pool of
 *  store threads that grows with load (spec section 3; docs/host-threading-measurement.md for why).
 *
 *  - `control` and `controlEvents` are one non-daemon thread, `aspen-host-control`, as in SingleThreadThreading.
 *  - `dispatch` runs each task inline on the calling thread (network, I/O completion, timer or CRL callback), which
 *    routes it straight to the store's executor without a hop through the control thread.
 *  - Each store's executor is its own StoreMailbox, placed on one of the `aspen-store-N` daemon threads and movable
 *    between them. Few, busy threads are cheaper than many idle ones: a task posted to a thread that is parked pays
 *    for waking it, so CPU per operation grows with the number of threads the same work is spread over.
 *
 *  Threads: the first `minThreads` are created lazily, one per newly assigned store, until there are that many.
 *  Beyond them a thread is added only when the stores' measured load needs it: once a second (`checkPeriod`) a check
 *  on the control thread measures each store's load over the last `config.window` from its mailbox's busy time; when
 *  the average thread load has been above `config.growAbove` for `config.growAfter` checks in a row, there are more
 *  stores than threads and fewer than `maxThreads` threads, it adds one. Threads are never removed before
 *  `shutdown`.
 *
 *  Placement is by load: a new store goes to the least loaded thread (StoreBalancer.placeNew), and each check moves
 *  stores off the busiest thread (StoreBalancer.plan) when it is more than `config.imbalance` busier than the least
 *  busy one, and always right after adding a thread, at most `config.movesPerCheck` stores per check. A moved store
 *  stays put for `config.settle`, so stores do not bounce between threads. StoreMailbox explains why a move never
 *  runs a store on two threads at once.
 *
 *  The placement and load bookkeeping is read and written only on the control thread (`assign`, `release` and the
 *  check), or a test's thread, so it needs no lock. `loops` is volatile only because `shutdown` may be called from
 *  another thread.
 *
 *  Control tasks queued before `shutdown` still run after it, and one of them may load a store. `assign` then returns
 *  a mailbox on a loop that is already shut down, so the store's work is dropped (with SerialLoop's warning) and no
 *  thread is left running that nothing would stop.
 *
 *  `checkPeriod` None runs no checks; tests then call `check` themselves, with `nanoClock` a manual clock. */
class StoreThreadPool(
    maxThreads: Int = StoreThreadPool.DefaultMaxThreads,
    minThreads: Int = 1,
    config: StoreBalancer.Config = StoreBalancer.Config(),
    checkPeriod: Option[FiniteDuration] = Some(1.second),
    nanoClock: () => Long = () => System.nanoTime()) extends HostThreading with Logging:

  require(minThreads >= 1, s"minThreads must be at least 1: $minThreads")
  require(maxThreads >= minThreads, s"maxThreads ($maxThreads) must be at least minThreads ($minThreads)")

  private val controlLoop = new SerialLoop("aspen-host-control", daemon = false)
  private val inline: Executor = task => task.run()

  @volatile private var shut = false
  @volatile private var loops = Vector.empty[SerialLoop]
  /** Every store loop this pool has created, including those created already shut down. Testing hook. */
  @volatile private var created = Vector.empty[SerialLoop]
  @volatile private var timer: Option[ScheduledExecutorService] = None

  private final class Assigned(val mailbox: StoreMailbox, var thread: Int):
    /** (time, busy nanos) at each check within the window, oldest first. */
    var samples: Vector[(Long, Long)] = Vector((nanoClock(), mailbox.busyNanos))
    var load = 0.0
    var movedAt: Option[Long] = None

  private var assigned = Map.empty[StoreId, Assigned]
  /** Consecutive checks with the average thread load above `config.growAbove`. */
  private var hotChecks = 0
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
        else
          StoreBalancer.placeNew(loops.size, placed)
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

  /** Measures each store's load, adds a thread if the stores need one, and moves stores to even out the threads.
   *  Runs on the control thread (or a test's thread). */
  private[host] def check(): Unit =
    if !shut && loops.nonEmpty then
      val now = nanoClock()
      val windowStart = now - config.window.toNanos
      for a <- assigned.values do
        val busy = a.mailbox.busyNanos
        // The oldest sample kept is the newest one at or before the window's start, so the load covers the whole
        // window once the store has been assigned that long, and the time since its assignment until then
        var samples = a.samples :+ (now, busy)
        while samples.size >= 2 && samples(1)._1 <= windowStart do
          samples = samples.tail
        a.samples = samples
        val (t0, b0) = samples.head
        a.load = if now > t0 then (busy - b0).toDouble / (now - t0) else 0.0

      val placed = placedStores
      hotChecks = if StoreBalancer.overloaded(loops.size, placed, config) then hotChecks + 1 else 0
      val grow = hotChecks >= config.growAfter && loops.size < maxThreads && assigned.size > loops.size
      if grow then
        hotChecks = 0
        val average = placed.iterator.map(_.load).sum / loops.size
        val added = addThread()
        logger.info(f"Added store thread aspen-store-$added: the average store thread was ${average * 100}%.0f%% busy")

      val moves = StoreBalancer.plan(loops.size, placedStores, config, force = grow)
      for m <- moves do
        val a = assigned(m.storeId)
        a.mailbox.moveTo(loops(m.to))
        a.thread = m.to
        a.movedAt = Some(now)
        movesMade += 1
        logger.info(f"Moved store ${m.storeId} (${a.load * 100}%.0f%% busy) from aspen-store-${m.from} to " +
          s"aspen-store-${m.to}")

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

object StoreThreadPool:
  /** A quarter of the CPUs: the store threads share the host with the network, FSIF and GC threads, and in the
   *  measurements a few busy store threads beat many idle ones (docs/host-threading-measurement.md). */
  def DefaultMaxThreads: Int = math.max(1, Runtime.getRuntime.availableProcessors() / 4)
