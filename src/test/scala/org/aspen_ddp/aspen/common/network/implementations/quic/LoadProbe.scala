package org.aspen_ddp.aspen.common.network.implementations.quic

import java.lang.management.ManagementFactory
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.{Duration, FiniteDuration}

/** Detects when the machine is too loaded for the QUIC tests' short timings. The tests lean on timings of a few
  * hundred milliseconds (idle timeouts, repair windows), which are fine on an idle machine but not when the test
  * JVM's threads wait on the CPU for a significant part of a second.
  *
  * The probe runs one spinning thread per core for a few milliseconds and measures what share of that wall time each
  * got as CPU time. A thread's wakeup latency is no use here: the scheduler favours sleepers, so it stays low even on a
  * saturated machine, while the QUIC event loops that do real work are starved. When every core is contended (other
  * suites, GC, other processes) the share drops, and its inverse is how much longer than normal a stretch of work takes.
  *
  * An idle machine yields a factor of exactly 1, so the common case pays nothing and only a loaded machine stretches
  * the timings.
  */
object LoadProbe:
  private val SpinMillis = 20L
  /** CPU share tolerated before anything is stretched */
  private val ToleratedShare = 0.7
  private val MaxFactor = 8.0
  private val MaxAgeNanos = TimeUnit.MILLISECONDS.toNanos(500)

  @volatile private var sampled: (Long, Double) = (0L, 1.0)

  private def measure(): Double =
    val bean = ManagementFactory.getThreadMXBean
    val n = Runtime.getRuntime.availableProcessors()
    val shares = new Array[Double](n)
    val start = new CountDownLatch(1)
    val threads = (0 until n).map: i =>
      val t = new Thread(() =>
        start.await()
        val cpu0 = bean.getCurrentThreadCpuTime
        val wall0 = System.nanoTime()
        val end = wall0 + TimeUnit.MILLISECONDS.toNanos(SpinMillis)
        while System.nanoTime() < end do ()
        val wall = System.nanoTime() - wall0
        shares(i) = (bean.getCurrentThreadCpuTime - cpu0).toDouble / wall
      )
      t.setDaemon(true)
      t.start()
      t
    start.countDown()
    threads.foreach(_.join())
    val share = shares.sum / n
    if share >= ToleratedShare then 1.0 else math.min(MaxFactor, 1.0 / math.max(share, 0.01))

  /** How much to stretch the timings: 1.0 on an idle machine, up to 8.0. Re-measured when the last sample is over half
    * a second old.
    */
  def factor: Double =
    val (at, f) = sampled
    if at != 0L && System.nanoTime() - at < MaxAgeNanos then f
    else
      val m = measure()
      sampled = (System.nanoTime(), m)
      m

  def scale(d: Duration): FiniteDuration =
    FiniteDuration((d.toNanos * factor).toLong, TimeUnit.NANOSECONDS)

  def scaleMillis(millis: Long): Long = (millis * factor).toLong
