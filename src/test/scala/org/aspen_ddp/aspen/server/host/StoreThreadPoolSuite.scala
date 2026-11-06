package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class StoreThreadPoolSuite extends AnyFunSuite with Matchers:

  private val Device = StorageDeviceId(new UUID(0, 1))
  private val Pool = PoolId(new UUID(0, 2))

  private def store(n: Int): StoreId = StoreId(Pool, n.toByte)

  /** The pool's clock in the load tests: it moves only when a test moves it. */
  private final class ManualClock:
    private val nanos = new AtomicLong(1000000000L)
    def apply(): Long = nanos.get
    def advance(d: FiniteDuration): Unit = nanos.addAndGet(d.toNanos)

  /** The default: a fixed pool. */
  private def withFixedPool(threads: Int)(f: StoreThreadPool => Unit): Unit =
    val t = new StoreThreadPool(threads)
    t.start()
    try f(t) finally t.shutdown()

  /** An adaptive pool whose checks never run. */
  private def withPool(maxThreads: Int, minThreads: Int = 1)(f: StoreThreadPool => Unit): Unit =
    val t = new StoreThreadPool(maxThreads, Some(StoreThreadPool.Adaptive(minThreads, checkPeriod = None)))
    t.start()
    try f(t) finally t.shutdown()

  /** A pool whose checks the test runs, on `clock`. Checks run on the test's thread, which also assigns: the pool's
   *  bookkeeping then has a single owner, as it does on the control thread. */
  private def withLoadPool(maxThreads: Int, minThreads: Int = 1, config: StoreBalancer.Config = StoreBalancer.Config())
                          (f: (StoreThreadPool, ManualClock) => Unit): Unit =
    val clock = new ManualClock
    val t = new StoreThreadPool(maxThreads, Some(StoreThreadPool.Adaptive(minThreads, config, checkPeriod = None)),
      nanoClock = () => clock())
    t.start()
    try f(t, clock) finally t.shutdown()

  /** Makes the store's mailbox record `d` of busy time without moving the pool's wall clock: a task advances the
   *  clock by `d` while it runs, and once the mailbox has counted it the clock is moved back. */
  private def addBusy(clock: ManualClock, ex: StoreExecutor, d: FiniteDuration): Unit =
    val mailbox = ex.asInstanceOf[StoreMailbox]
    val target = mailbox.busyNanos + d.toNanos
    ex.execute(() => clock.advance(d))
    val deadline = System.nanoTime() + 10.seconds.toNanos
    while mailbox.busyNanos < target && System.nanoTime() < deadline do
      Thread.onSpinWait()
    mailbox.busyNanos should be >= target
    clock.advance(-d)

  /** One second in which each store was busy for its fraction of it, followed by a check. */
  private def second(t: StoreThreadPool, clock: ManualClock, loads: (StoreExecutor, Double)*): Unit =
    for (ex, load) <- loads do
      addBusy(clock, ex, (load * 1000).toLong.millis)
    clock.advance(1.second)
    t.check()

  /** Makes `waiter`'s next drain wait `d` for its thread, without moving the pool's wall clock: a task of `holder`, on
   *  the same thread, occupies it while `waiter`'s drain is queued, and advances the clock by `d` before it returns.
   *  Once both mailboxes have counted it the clock is moved back. `holder` is also busy for `d`. */
  private def addWait(clock: ManualClock, holder: StoreExecutor, waiter: StoreExecutor, d: FiniteDuration): Unit =
    val h = holder.asInstanceOf[StoreMailbox]
    val w = waiter.asInstanceOf[StoreMailbox]
    (h.currentLoop eq w.currentLoop) should be(true)
    val drains = w.drains
    val busy = h.busyNanos + d.toNanos
    val running = new CountDownLatch(1)
    val go = new CountDownLatch(1)
    holder.execute: () =>
      running.countDown()
      go.await(10, TimeUnit.SECONDS)
      clock.advance(d)
    running.await(10, TimeUnit.SECONDS) should be(true)
    waiter.execute(() => ())
    go.countDown()
    val deadline = System.nanoTime() + 10.seconds.toNanos
    // The waiter's drain must have finished, not just started: it reads the clock again at its end
    while (w.drains == drains || !w.isIdle || h.busyNanos < busy) && System.nanoTime() < deadline do
      Thread.onSpinWait()
    w.drains should be(drains + 1)
    w.isIdle should be(true)
    clock.advance(-d)

  /** One second in which `waiter`'s work waited 20 ms behind `holder`'s, followed by a check. */
  private def congestedSecond(t: StoreThreadPool, clock: ManualClock, holder: StoreExecutor,
                              waiter: StoreExecutor): Unit =
    addWait(clock, holder, waiter, 20.millis)
    addBusy(clock, waiter, 10.millis)
    clock.advance(1.second)
    t.check()

  private def threadName(ex: StoreExecutor): String =
    val name = new AtomicReference[String]()
    val ran = new CountDownLatch(1)
    ex.execute: () =>
      name.set(Thread.currentThread.getName)
      ran.countDown()
    ran.await(10, TimeUnit.SECONDS) should be(true)
    name.get

  test("fixed: threads are created lazily, one per store until there are maxThreads"):
    withFixedPool(4): t =>
      t.threadCount should be(0)
      t.assign(store(1), Device)
      t.assign(store(2), Device)
      t.threadCount should be(2)
      for n <- 3 until 10 do t.assign(store(n), Device)
      t.threadCount should be(4)
      t.assignedCounts.sorted should be(Vector(2, 2, 2, 3))

  test("fixed: a new store goes to the thread with the fewest stores, which a release can make an earlier one"):
    withFixedPool(2): t =>
      t.assign(store(1), Device)
      val s2 = t.assign(store(2), Device).asInstanceOf[StoreMailbox]
      t.assign(store(3), Device)
      t.assignedCounts should be(Vector(2, 1))
      t.release(store(1))
      // Thread 0 lost a store: both threads now hold one, and the tie goes to thread 0
      val s4 = t.assign(store(4), Device).asInstanceOf[StoreMailbox]
      t.assignedCounts should be(Vector(2, 1))
      (s4.currentLoop eq s2.currentLoop) should be(false)

  test("fixed: stores never move and a check does nothing"):
    withFixedPool(2): t =>
      for n <- 1 to 4 do t.assign(store(n), Device)
      val before = (1 to 4).map(n => t.threadOf(store(n)))
      t.check()
      (1 to 4).map(n => t.threadOf(store(n))) should be(before)
      t.moveCount should be(0)

  test("adaptive: one store thread, created at the first assign, until the load needs more"):
    withPool(4): t =>
      t.threadCount should be(0)
      for n <- 0 until 10 do t.assign(store(n), Device)
      t.threadCount should be(1)
      t.assignedCounts should be(Vector(10))

  test("adaptive: the first minThreads stores each get a new thread"):
    withPool(4, minThreads = 3): t =>
      for n <- 0 until 5 do t.assign(store(n), Device)
      t.threadCount should be(3)
      // After those, packing: the lowest-index thread below target
      t.assignedCounts should be(Vector(3, 1, 1))

  test("release frees the store's place"):
    withFixedPool(4): t =>
      for _ <- 0 until 100 do
        t.assign(store(1), Device)
        t.release(store(1))
      t.threadCount should be(1)
      t.assignedCounts should be(Vector(0))
      t.assign(store(1), Device)
      an[HostThreading.AlreadyAssigned] should be thrownBy t.assign(store(1), Device)

  test("adaptive: a new store goes to the lowest-index thread below target"):
    withLoadPool(2, minThreads = 2): (t, clock) =>
      val s1 = t.assign(store(1), Device)
      t.assign(store(2), Device)
      // Both threads are idle: packing fills thread 0 first
      t.assign(store(3), Device)
      t.threadOf(store(3)) should be(Some(0))
      second(t, clock, s1 -> 0.8)
      t.loadOf(store(1)).get should be(0.8 +- 0.001)
      t.assign(store(4), Device)
      t.threadOf(store(4)) should be(Some(1))

  test("adaptive: busy time alone never adds a thread"):
    withLoadPool(4, config = StoreBalancer.Config(growAfter = 1)): (t, clock) =>
      val s1 = t.assign(store(1), Device)
      val s2 = t.assign(store(2), Device)
      for _ <- 0 until 3 do second(t, clock, s1 -> 0.5, s2 -> 0.45)
      t.threadCount should be(1)

  test("adaptive: drain wait is measured from a drain being queued to it starting"):
    withLoadPool(1): (t, clock) =>
      val s1 = t.assign(store(1), Device)
      val s2 = t.assign(store(2), Device)
      addWait(clock, s1, s2, 20.millis)
      val m2 = s2.asInstanceOf[StoreMailbox]
      m2.waitNanos should be(20.millis.toNanos)
      m2.drains should be(1)
      s1.asInstanceOf[StoreMailbox].waitNanos should be(0)

  test("adaptive: a thread is added after growAfter congested checks, and the congested thread's stores move onto it"):
    withLoadPool(2, config = StoreBalancer.Config(growAfter = 2, growWait = 1.millis)): (t, clock) =>
      val s1 = t.assign(store(1), Device)
      val s2 = t.assign(store(2), Device)
      congestedSecond(t, clock, s1, s2)
      t.waitOf(0).get should be > 1.millis.toNanos.toDouble
      t.threadCount should be(1)
      congestedSecond(t, clock, s1, s2)
      t.threadCount should be(2)
      t.assignedCounts should be(Vector(1, 1))
      t.moveCount should be(1)
      // The busier store moved: its share of the holding task's time makes it s1
      t.threadOf(store(1)) should be(Some(1))
      threadName(s1) should be("aspen-store-1")

  test("adaptive: congestion must be sustained: a quiet check starts the count again"):
    withLoadPool(2, config = StoreBalancer.Config(growAfter = 2)): (t, clock) =>
      val s1 = t.assign(store(1), Device)
      val s2 = t.assign(store(2), Device)
      congestedSecond(t, clock, s1, s2)
      // Long enough idle that the window holds no drains
      clock.advance(10.seconds)
      t.check()
      congestedSecond(t, clock, s1, s2)
      t.threadCount should be(1)

  test("adaptive: no thread is added beyond maxThreads"):
    withLoadPool(1, config = StoreBalancer.Config(growAfter = 1)): (t, clock) =>
      val s1 = t.assign(store(1), Device)
      val s2 = t.assign(store(2), Device)
      for _ <- 0 until 3 do congestedSecond(t, clock, s1, s2)
      t.threadCount should be(1)

  test("adaptive: light load gathers onto the lowest threads, and threads left without stores are kept"):
    withLoadPool(3, minThreads = 3): (t, clock) =>
      val s1 = t.assign(store(1), Device)
      val s2 = t.assign(store(2), Device)
      val s3 = t.assign(store(3), Device)
      t.assignedCounts should be(Vector(1, 1, 1))
      second(t, clock, s1 -> 0.1, s2 -> 0.1, s3 -> 0.1)
      t.assignedCounts should be(Vector(3, 0, 0))
      t.moveCount should be(2)
      t.threadCount should be(3)
      threadName(s3) should be("aspen-store-0")

  test("adaptive: a store over target moves to a busy thread with room, and a moved store settles before moving again"):
    // A one-second window, so each check sees only the second before it
    val config = StoreBalancer.Config(window = 1.second, settle = 10.seconds)
    withLoadPool(2, minThreads = 2, config): (t, clock) =>
      val s1 = t.assign(store(1), Device) // thread 0
      val s2 = t.assign(store(2), Device) // thread 1
      val s3 = t.assign(store(3), Device) // thread 0, the lowest below target
      // Thread 0 at 0.8: s1 fits on thread 1 (0.55)
      second(t, clock, s1 -> 0.5, s3 -> 0.3, s2 -> 0.05)
      t.moveCount should be(1)
      t.threadOf(store(1)) should be(Some(1))
      // Now light: thread 1's stores gather onto thread 0, but s1 only once it has settled, 10 s after its move
      for _ <- 0 until 9 do second(t, clock, s1 -> 0.05, s3 -> 0.05, s2 -> 0.05)
      t.moveCount should be(2)
      t.threadOf(store(1)) should be(Some(1))
      second(t, clock, s1 -> 0.05, s3 -> 0.05, s2 -> 0.05)
      t.moveCount should be(3)
      t.assignedCounts should be(Vector(3, 0))

  test("store threads are daemon threads named aspen-store-N"):
    withFixedPool(4): t =>
      val seen = new AtomicReference[(String, Boolean)]()
      val ran = new CountDownLatch(1)
      t.assign(store(1), Device).execute: () =>
        seen.set((Thread.currentThread.getName, Thread.currentThread.isDaemon))
        ran.countDown()
      ran.await(10, TimeUnit.SECONDS) should be(true)
      seen.get should be(("aspen-store-0", true))

  test("tasks of one store never overlap and run in posting order, while stores move between threads"):
    val Stores = 16
    val Posters = 8
    val TasksPerPoster = 4000
    withFixedPool(4): t =>
      val executors = (0 until Stores).map(n => t.assign(store(n), Device).asInstanceOf[StoreMailbox])
      val loops = executors.map(_.currentLoop).distinct
      loops.size should be(4)
      val inUse = Vector.fill(Stores)(new AtomicBoolean(false))
      val overlapSeen = new AtomicBoolean(false)
      // Per store: (poster, sequence number) in the order the store ran them
      val records = Vector.fill(Stores)(new ConcurrentLinkedQueue[(Int, Int)]())
      val done = new CountDownLatch(Posters * TasksPerPoster)
      val stopMoving = new AtomicBoolean(false)
      val moves = new AtomicInteger(0)
      // Moves stores at random for as long as the posters run. The pool moves stores from the control thread; any
      // single thread calling moveTo exercises the same handover
      val mover = new Thread(() =>
        val rnd = ThreadLocalRandom.current()
        while !stopMoving.get do
          executors(rnd.nextInt(Stores)).moveTo(loops(rnd.nextInt(loops.size)))
          moves.incrementAndGet()
          Thread.`yield`())
      val posters = (0 until Posters).map: poster =>
        new Thread(() =>
          for seq <- 0 until TasksPerPoster do
            val s = (poster + seq) % Stores
            executors(s).execute: () =>
              if !inUse(s).compareAndSet(false, true) then
                overlapSeen.set(true)
              records(s).add((poster, seq))
              inUse(s).set(false)
              done.countDown())
      mover.start()
      posters.foreach(_.start())
      posters.foreach(_.join())
      done.await(30, TimeUnit.SECONDS) should be(true)
      stopMoving.set(true)
      mover.join()
      moves.get should be > 0
      overlapSeen.get should be(false)
      for s <- 0 until Stores; poster <- 0 until Posters do
        val seqs = records(s).asScala.toList.filter(_._1 == poster).map(_._2)
        seqs should be(seqs.sorted)
        seqs.distinct.size should be(seqs.size)
      records.map(_.size).sum should be(Posters * TasksPerPoster)

  test("a store runs at most batch tasks before its thread runs another store's"):
    val t = new StoreThreadPool(1,
      Some(StoreThreadPool.Adaptive(config = StoreBalancer.Config(batch = 2), checkPeriod = None)))
    t.start()
    try
      val a = t.assign(store(1), Device)
      val b = t.assign(store(2), Device)
      val order = new ConcurrentLinkedQueue[String]()
      val hold = new CountDownLatch(1)
      val done = new CountDownLatch(6)
      // Holds the thread so that everything below is queued before any of it runs
      a.execute(() => { hold.await(10, TimeUnit.SECONDS); () })
      for i <- 1 to 4 do a.execute(() => { order.add(s"a$i"); done.countDown() })
      for i <- 1 to 2 do b.execute(() => { order.add(s"b$i"); done.countDown() })
      hold.countDown()
      done.await(10, TimeUnit.SECONDS) should be(true)
      // a's first drain ran the holding task and a1; b's drain was queued behind it
      order.asScala.toList should be(List("a1", "b1", "b2", "a2", "a3", "a4"))
    finally t.shutdown()

  test("a throwing task does not kill its store thread"):
    withFixedPool(1): t =>
      val ex = t.assign(store(1), Device)
      val ran = new CountDownLatch(1)
      ex.execute(() => throw new RuntimeException("expected by the test"))
      ex.execute(() => ran.countDown())
      ran.await(10, TimeUnit.SECONDS) should be(true)

  test("adaptive: the periodic check runs on its own once started"):
    // Any drain wait at all is congestion, and one congested check adds a thread
    val config = StoreBalancer.Config(growWait = Duration.Zero, growAfter = 1, window = 100.millis)
    val t = new StoreThreadPool(2, Some(StoreThreadPool.Adaptive(config = config, checkPeriod = Some(10.millis))))
    t.start()
    try
      // On the control thread, where the checks run: the pool's bookkeeping has that single owner
      val assigned = new AtomicReference[(StoreExecutor, StoreExecutor)]()
      val ready = new CountDownLatch(1)
      t.controlEvents.execute: () =>
        assigned.set((t.assign(store(1), Device), t.assign(store(2), Device)))
        ready.countDown()
      ready.await(10, TimeUnit.SECONDS) should be(true)
      val (s1, s2) = assigned.get
      // s2's work waits behind s1's sleep, until a check sees it
      val deadline = System.nanoTime() + 10.seconds.toNanos
      while t.threadCount < 2 && System.nanoTime() < deadline do
        s1.execute(() => Thread.sleep(5))
        s2.execute(() => ())
        Thread.sleep(10)
      t.threadCount should be(2)
    finally t.shutdown()

  test("assign after shutdown leaves no live store thread and drops the store's work"):
    val t = new StoreThreadPool(4)
    t.start()
    val release = new CountDownLatch(1)
    val assigned = new CountDownLatch(1)
    val ran = new AtomicInteger(0)
    // Holds the control thread so the assigning task is still queued when shutdown is called
    t.controlEvents.execute(() => { release.await(10, TimeUnit.SECONDS); () })
    t.controlEvents.execute: () =>
      t.assign(store(1), Device).execute(() => ran.incrementAndGet())
      assigned.countDown()
    t.shutdown()
    release.countDown()
    assigned.await(10, TimeUnit.SECONDS) should be(true)
    // Directly, too
    t.assign(store(2), Device).execute(() => ran.incrementAndGet())
    // Only this pool's threads: other suites run StoreThreadPool hosts in the same JVM
    t.createdLoops.size should be(2)
    t.createdLoops.exists(_.threadIsAlive) should be(false)
    t.threadCount should be(0)
    ran.get should be(0)
