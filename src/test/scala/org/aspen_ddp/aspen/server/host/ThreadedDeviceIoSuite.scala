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
    withIo(() => latch.await()) : io =>
      val ran = new AtomicBoolean(false)
      val a = io.submit(JobKind.Scan, None)(())
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
