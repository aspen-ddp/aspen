package org.aspen_ddp.aspen.server.host

import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** HealthProbe on its own: a manual clock, an injected write, and a 5 ms interval. Every wait polls a condition with a
 *  deadline of a few seconds. */
class HealthProbeSuite extends AnyFunSuite with Matchers:
  private val cfg = HealthProbe.Config(interval = 5.millis, hangThreshold = 120.seconds)

  @volatile private var t = 0L

  private def eventually(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 5.seconds.toNanos
    while !cond && System.nanoTime() < deadline do Thread.sleep(1)
    assert(cond, "condition not reached within 5 s")

  private def newProbe(name: String, write: os.Path => Unit,
                       observer: DeviceIoObserver = new DeviceIoObserver(() => t)): HealthProbe =
    new HealthProbe(os.Path("/tmp/probe-dev"), cfg, observer, () => name, write = write, clock = () => t)

  /** Runs `f` and always leaves the probe disarmed, its thread gone */
  private def withProbe[A](p: HealthProbe)(f: => A): A =
    try f
    finally
      p.disarm()
      eventually(!p.threadAlive)

  private def threadsNamed(name: String): Int =
    Thread.getAllStackTraces.keySet.asScala.count(th => th.getName == name && th.isAlive)

  test("an attempt records ok and its duration"):
    val p = newProbe("dev-ok-probe", _ => t += 7)
    withProbe(p):
      p.arm()
      p.armed should be(true)
      eventually(p.lastResult.exists(_.error.isEmpty))
      p.lastResult.get.duration should be(7.nanos)
      p.inFlightAge should be(None)
      p.hung should be(false)

  test("a blocked attempt is in flight with its age"):
    val latch = new CountDownLatch(1)
    val observer = new DeviceIoObserver(() => t)
    val p = newProbe("dev-blocked-probe", _ => latch.await(), observer)
    withProbe(p):
      try
        p.arm()
        eventually(p.inFlightAge.isDefined)
        t += 121.seconds.toNanos
        p.inFlightAge.get should be >= 121.seconds
        p.hung should be(true)
        observer.inFlight.keySet should contain(HealthProbe.Kind)
      finally latch.countDown()
      eventually(p.inFlightAge.isEmpty)
      p.hung should be(false)

  test("a failing attempt records its error and is not hung"):
    val p = newProbe("dev-fail-probe", _ => throw new IOException("injected EIO"))
    withProbe(p):
      p.arm()
      eventually(p.lastResult.exists(_.error.isDefined))
      p.lastResult.get.error.get shouldBe an[IOException]
      p.hung should be(false)

  test("attempts never overlap"):
    val inside = new AtomicInteger(0)
    val maxInside = new AtomicInteger(0)
    val attempts = new AtomicInteger(0)
    val p = newProbe("dev-overlap-probe", _ =>
      val n = inside.incrementAndGet()
      maxInside.accumulateAndGet(n, math.max)
      Thread.sleep(1)
      inside.decrementAndGet()
      attempts.incrementAndGet(): Unit)
    withProbe(p):
      p.arm()
      // Rearming while armed starts nothing more
      (0 until 10).foreach(_ => p.arm())
      eventually(attempts.get >= 50)
      maxInside.get should be(1)

  test("disarm stops attempts and the thread exits"):
    val attempts = new AtomicInteger(0)
    val p = newProbe("dev-disarm-probe", _ => attempts.incrementAndGet(): Unit)
    withProbe(p):
      p.arm()
      eventually(attempts.get >= 3)
      p.disarm()
      p.armed should be(false)
      eventually(!p.threadAlive)
      val after = attempts.get
      Thread.sleep(cfg.interval.toMillis * 4)
      attempts.get should be(after)

  test("rearm while an attempt is stuck starts no second thread"):
    val latch = new CountDownLatch(1)
    val attempts = new AtomicInteger(0)
    val threads = java.util.concurrent.ConcurrentHashMap.newKeySet[Thread]()
    val p = newProbe("dev-x-probe", _ =>
      threads.add(Thread.currentThread())
      latch.await()
      attempts.incrementAndGet(): Unit)
    withProbe(p):
      try
        p.arm()
        eventually(p.inFlightAge.isDefined)
        p.disarm()
        p.arm()
        threadsNamed("dev-x-probe") should be(1)
        p.inFlightAge.isDefined should be(true)
      finally latch.countDown()
      eventually(attempts.get >= 3)
      threads.size should be(1)
      threadsNamed("dev-x-probe") should be(1)

  test("writeAndForce creates a 4 KiB file and overwrites it in place"):
    val dir = os.temp.dir(prefix = "health-probe", deleteOnExit = false)
    try
      val file = dir / HealthProbe.FileName
      HealthProbe.writeAndForce(file)
      os.size(file) should be(HealthProbe.Size)
      def inode = java.nio.file.Files.readAttributes(file.toNIO,
        classOf[java.nio.file.attribute.BasicFileAttributes]).fileKey
      val first = inode
      HealthProbe.writeAndForce(file)
      os.size(file) should be(HealthProbe.Size)
      inode should be(first)
    finally os.remove.all(dir)
