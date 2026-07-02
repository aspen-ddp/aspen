package org.aspen_ddp.aspen.common.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

class RunBoundedParallelSuite extends AnyFunSuite with Matchers:

  given ExecutionContext = ExecutionContext.global
  val timeout: FiniteDuration = 5.seconds

  test("returns results in input order regardless of completion order"):
    // Later items complete sooner, so completion order != input order.
    val items = List(1, 2, 3, 4, 5)
    val f = runBoundedParallel(items, maxConcurrent = 5): i =>
      Future:
        Thread.sleep((6 - i) * 10L)
        i * 10
    Await.result(f, timeout) should be(List(10, 20, 30, 40, 50))

  test("never exceeds the concurrency limit"):
    val inFlight = new AtomicInteger(0)
    val maxObserved = new AtomicInteger(0)
    val items = (1 to 50).toList
    val f = runBoundedParallel(items, maxConcurrent = 4): _ =>
      Future:
        val cur = inFlight.incrementAndGet()
        maxObserved.updateAndGet(m => math.max(m, cur))
        Thread.sleep(5)
        inFlight.decrementAndGet()
        ()
    Await.result(f, timeout)
    maxObserved.get() should be <= 4

  test("empty input yields empty result without invoking f"):
    val called = new AtomicInteger(0)
    val f = runBoundedParallel(List.empty[Int], maxConcurrent = 4): _ =>
      called.incrementAndGet()
      Future.successful(())
    Await.result(f, timeout) should be(empty)
    called.get() should be(0)

  test("processes all items when there are fewer items than the limit"):
    val items = List("a", "b")
    val f = runBoundedParallel(items, maxConcurrent = 10)(s => Future.successful(s.toUpperCase))
    Await.result(f, timeout) should be(List("A", "B"))

  test("fails fast when an invocation fails"):
    val boom = new RuntimeException("boom")
    val items = List(1, 2, 3)
    val f = runBoundedParallel(items, maxConcurrent = 2): i =>
      if i == 2 then Future.failed(boom) else Future.successful(i)
    val thrown = intercept[RuntimeException](Await.result(f, timeout))
    thrown.getMessage should be("boom")

  test("requires a positive concurrency limit"):
    intercept[IllegalArgumentException]:
      runBoundedParallel(List(1, 2, 3), maxConcurrent = 0)(i => Future.successful(i))
