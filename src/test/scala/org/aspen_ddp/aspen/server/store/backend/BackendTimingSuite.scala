package org.aspen_ddp.aspen.server.store.backend

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BackendTimingSuite extends AnyFunSuite with Matchers:
  private val ms = 1000000L

  private def timing(): (BackendTiming, () => List[String]) =
    var reports = List[String]()
    (new BackendTiming(1000 * ms, s => reports = s :: reports), () => reports.reverse)

  test("nothing is reported before the period ends") {
    val (t, reports) = timing()
    t.record("a", 5 * ms, now = 0)
    t.record("a", 5 * ms, now = 999 * ms)

    reports() shouldBe Nil
  }

  test("a report totals each stage, with its size if it has one") {
    val (t, reports) = timing()
    t.record("write", 2 * ms, size = 10, now = 0)
    t.record("write", 4 * ms, size = 30, now = 10 * ms)
    t.record("commit", 8 * ms, now = 1000 * ms)

    reports() shouldBe List(
      "Backend timing over 1000ms. commit: 1 avg 8.0ms max 8.0ms; write: 2 avg 3.0ms max 4.0ms, avg size 20")
  }

  test("the next report starts empty") {
    val (t, reports) = timing()
    t.record("a", 1 * ms, now = 0)
    t.record("a", 1 * ms, now = 1000 * ms)
    t.record("b", 2 * ms, now = 1500 * ms)
    t.record("b", 2 * ms, now = 2000 * ms)

    reports().last shouldBe "Backend timing over 1000ms. b: 2 avg 2.0ms max 2.0ms"
  }
