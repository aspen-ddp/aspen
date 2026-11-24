package org.aspen_ddp.aspen.server.host

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.*

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ThreadedDeviceIoSuite extends AnyFunSuite with Matchers:
  private val cfg = DeviceIo.Config()
  private val wait = 20.seconds

  private def withIo[A](gate: () => Unit = () => ())(f: ThreadedDeviceIo => A): A =
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg, gate)
    try f(io)
    finally io.close()

  test("jobs run in submission order on one lane thread"):
    withIo() : io =>
      val order = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
      val threads = new java.util.concurrent.ConcurrentLinkedQueue[Thread]()
      val hs = (0 until 100).map: i =>
        io.submit(JobKind.Scan, None):
          order.add(i)
          threads.add(Thread.currentThread())
      hs.foreach(h => Await.result(h.future, wait))
      order.toArray.toList should be((0 until 100).toList)
      threads.toArray.toSet.size should be(1)
      threads.peek().getName should startWith("dev-")

  test("cancel before start skips the job"):
    withIo() : io =>
      val started = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val ran = new AtomicBoolean(false)
      val a = io.submit(JobKind.Scan, None):
        started.countDown()
        release.await()
      started.await()
      val b = io.submit(JobKind.Scan, None)(ran.set(true))
      a.cancel() should be(false)
      b.cancel() should be(true)
      release.countDown()
      Await.result(a.future, wait)
      val ex = intercept[Throwable](Await.result(b.future, wait))
      ex should be theSameInstanceAs DeviceIo.Cancelled
      // a later job on the same lane proves B was already passed over
      Await.result(io.submit(JobKind.Scan, None)(()).future, wait)
      ran.get should be(false)

  test("a hung DeviceIo does not delay another"):
    val latch = new CountDownLatch(1)
    withIo(() => latch.await()) : a =>
      withIo() : b =>
        val hung = a.submit(JobKind.Scan, None)(())
        val jobs = (0 until 50).map(_ => b.submit(JobKind.Scan, None)(()))
        val tasks = (0 until 50).map(_ => Future(())(using b.dataEc))
        jobs.foreach(h => Await.result(h.future, wait))
        tasks.foreach(f => Await.result(f, wait))
        hung.future.isCompleted should be(false)
        latch.countDown()
        Await.result(hung.future, wait)

  test("a job blocked in the gate is running and cannot be cancelled; the job queued behind it can be"):
    val latch = new CountDownLatch(1)
    val entered = new CountDownLatch(1)
    withIo(() => { entered.countDown(); latch.await() }) : io =>
      val ran = new AtomicBoolean(false)
      val a = io.submit(JobKind.Scan, None)(())
      entered.await()
      val b = io.submit(JobKind.Scan, None)(ran.set(true))
      a.cancel() should be(false)
      b.cancel() should be(true)
      latch.countDown()
      Await.result(a.future, wait)
      intercept[Throwable](Await.result(b.future, wait)) should be theSameInstanceAs DeviceIo.Cancelled
      ran.get should be(false)

  test("observer sees start and finish"):
    withIo() : io =>
      val started = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val h = io.submit(JobKind.Scan, None):
        started.countDown()
        release.await()
      started.await()
      io.observer.inFlight("Scan")._1 should be(1)
      release.countDown()
      Await.result(h.future, wait)
      // finished() is recorded before the promise completes
      io.observer.completed("Scan") should be(1L)
      io.observer.inFlight.contains("Scan") should be(false)

  test("read bound"):
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), DeviceIo.Config(readBound = 2))
    try
      io.readsOverBound should be(false)
      io.beginRead()
      io.beginRead()
      io.readsOverBound should be(true)
      io.endRead()
      io.readsOverBound should be(false)
    finally io.close()

  // ---- Lifecycle: no job is ever left with a future that never completes ----

  test("a job submitted to a closed DeviceIo fails with Cancelled at once and never runs"):
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg)
    io.close()
    val ran = new AtomicBoolean(false)
    val h = io.submit(JobKind.Scan, None)(ran.set(true))
    h.future.value.map(_.failed.get) should be(Some(DeviceIo.Cancelled))
    h.cancel() should be(false)
    ran.get should be(false)

  test("a job submitted to a closed DeviceIo whose lane ran fails with Cancelled at once"):
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg)
    Await.result(io.submit(JobKind.Scan, None)(()).future, wait)
    io.close()
    io.submit(JobKind.Scan, None)(()).future.value.map(_.failed.get) should be(Some(DeviceIo.Cancelled))

  test("close fails every queued job with Cancelled, and a running job's interrupt fails it"):
    val entered = new CountDownLatch(1)
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg)
    val running = io.submit(JobKind.Scan, None):
      entered.countDown()
      new CountDownLatch(1).await()
    entered.await()
    val queued = (0 until 5).map(_ => io.submit(JobKind.Scan, None)(()))
    io.close()
    queued.foreach: h =>
      intercept[Throwable](Await.result(h.future, wait)) should be theSameInstanceAs DeviceIo.Cancelled
    // A Future fails with a fatal throwable wrapped in an ExecutionException
    intercept[java.util.concurrent.ExecutionException](Await.result(running.future, wait))
      .getCause shouldBe an[InterruptedException]
    io.observer.inFlight should be(Map())

  test("a fatal throwable from a job fails its future and ends its observer entry"):
    withIo() : io =>
      val h = io.submit(JobKind.Scan, None)(throw new StackOverflowError("injected"))
      intercept[java.util.concurrent.ExecutionException](Await.result(h.future, wait))
        .getCause shouldBe a[StackOverflowError]
      io.observer.inFlight should be(Map())
      // The lane goes on
      Await.result(io.submit(JobKind.Scan, None)(7).future, wait) should be(7)

  test("a throwing gate fails the job's future rather than leaving it running"):
    val fail = new AtomicBoolean(true)
    withIo(() => if fail.getAndSet(false) then throw new IllegalStateException("injected gate")) : io =>
      val h = io.submit(JobKind.Scan, None)(())
      intercept[IllegalStateException](Await.result(h.future, wait))
      io.observer.inFlight should be(Map())
      Await.result(io.submit(JobKind.Scan, None)(1).future, wait) should be(1)

  test("a lockstep DeviceIo, closed, cancels its queued jobs and every later submit"):
    val threading = new LockstepThreading(scala.concurrent.ExecutionContext.parasitic)
    val io = threading.newDeviceIo(os.Path("/tmp/devX"))
    val ran = new AtomicBoolean(false)
    val queued = io.submit(JobKind.Scan, None)(ran.set(true))
    io.close()
    val later = io.submit(JobKind.Scan, None)(ran.set(true))
    later.future.value.map(_.failed.get) should be(Some(DeviceIo.Cancelled))
    threading.drain()
    queued.future.value.map(_.failed.get) should be(Some(DeviceIo.Cancelled))
    ran.get should be(false)
    io.idle should be(true)

  test("close disarms the probe, even while an attempt is stuck"):
    val entered = new CountDownLatch(1)
    val unhang = new CountDownLatch(1)
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg, probeWrite = _ =>
      entered.countDown()
      unhang.await())
    try
      io.probe.arm()
      entered.await(wait.toSeconds, TimeUnit.SECONDS) should be(true)
      val closed = Future(io.close())(using scala.concurrent.ExecutionContext.global)
      Await.result(closed, wait)
      io.probe.armed should be(false)
      io.probe.inFlightAge.isDefined should be(true)
    finally unhang.countDown()
    val deadline = System.nanoTime() + wait.toNanos
    while io.probe.threadAlive && System.nanoTime() < deadline do Thread.sleep(1)
    io.probe.threadAlive should be(false)

  test("the lane closing disarms the probe, which does not hold the DeviceIo busy"):
    val entered = new CountDownLatch(1)
    val unhang = new CountDownLatch(1)
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg, probeWrite = _ =>
      entered.countDown()
      unhang.await())
    try
      io.probe.arm()
      entered.await(wait.toSeconds, TimeUnit.SECONDS) should be(true)
      // A stuck probe attempt is no lane job and no pool task: DeviceIos.forget may close the lane
      io.idle should be(true)
      io.closeLane()
      io.probe.armed should be(false)
    finally
      unhang.countDown()
      io.close()

  test("close fails the jobs queued behind a lane job hung past its interrupt"):
    val entered = new CountDownLatch(1)
    val unhang = new CountDownLatch(1)
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg)
    // A hung syscall: the interrupt does not end it
    val hung = io.submit(JobKind.Scan, None):
      entered.countDown()
      var done = false
      while !done do
        try
          unhang.await()
          done = true
        catch case _: InterruptedException => ()
    entered.await()
    val queued = (0 until 5).map(_ => io.submit(JobKind.Scan, None)(()))
    io.close()
    try
      queued.foreach: h =>
        intercept[Throwable](Await.result(h.future, wait)) should be theSameInstanceAs DeviceIo.Cancelled
      hung.future.isCompleted should be(false)
    finally unhang.countDown()
    Await.result(hung.future, wait)

  test("a forgotten DeviceIo still runs the data and serial tasks of a backend that outlives it"):
    val threading = new SingleThreadThreading(cfg)
    val ios = new DeviceIos(threading, os.Path("/tmp/hostX"))
    val io = ios.forPath(os.Path("/tmp/devX"))
    // As an FSIF backend's serial executor: alive, idle between tasks, when its path's DeviceIo is forgotten
    val serial = new org.aspen_ddp.aspen.server.store.backend.fsif.SerialExecutor(io.serialEc)
    Await.result(Future(1)(using serial), wait) should be(1)
    // The task's pool count ends just after its Future completes
    val deadline = System.nanoTime() + wait.toNanos
    while !io.idle && System.nanoTime() < deadline do Thread.sleep(1)
    io.idle should be(true)
    ios.forget(io.path) should be(true)
    ios.get(io.path) should be(None)
    // The lane is closed: a late job fails Cancelled, at once
    io.submit(JobKind.Scan, None)(()).future.value.map(_.failed.get) should be(Some(DeviceIo.Cancelled))
    try
      Await.result(Future(2)(using serial), wait) should be(2)
      Await.result(Future(3)(using io.dataEc), wait) should be(3)
      Await.result(Future(4)(using serial), wait) should be(4)
    finally io.close()

  test("idle pool threads time out, and the pool starts new ones for later tasks"):
    val io = new ThreadedDeviceIo(os.Path("/tmp/devX"), cfg.copy(poolKeepAliveMillis = 20))
    try
      Await.result(Future(1)(using io.dataEc), wait)
      Await.result(Future(1)(using io.serialEc), wait)
      val deadline = System.nanoTime() + wait.toNanos
      while io.poolThreads > 0 && System.nanoTime() < deadline do Thread.sleep(5)
      io.poolThreads should be(0)
      Await.result(Future(2)(using io.dataEc), wait) should be(2)
    finally io.close()
