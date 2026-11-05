package org.aspen_ddp.aspen.common.network.implementations.quic

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StreamDelayReportSuite extends AnyFunSuite with Matchers:
  private val ms = 1000000L
  private val mib = 1024L * 1024

  private def report() =
    val r = new StreamDelayReport(2, 1000 * ms, 500 * ms)
    r.completed(0, 0, System.nanoTime() + 2000 * ms) // Starts a period at about `now`, with nothing to report
    r

  test("nothing is reported while no wait is long") {
    val now = System.nanoTime() + 2000 * ms
    val r = report()
    r.written(0, 10 * mib)
    r.completed(0, 500 * ms, now + 2000 * ms) shouldBe None
  }

  test("a long wait is reported per stream once the period ends") {
    val now = System.nanoTime() + 2000 * ms
    val r = report()
    r.written(0, 30 * mib)
    r.written(0, 40 * mib)
    r.written(1, mib)
    r.completed(0, 1200 * ms, now + 100 * ms) shouldBe None
    r.completed(1, 3 * ms, now + 200 * ms) shouldBe None

    val text = r.completed(1, 4 * ms, now + 3000 * ms).get
    text should include("stream 0: 1200ms, 40 MiB queued, 2 writes")
    text should include("stream 1: 4ms, 1 MiB queued, 1 writes")
  }

  test("the next period starts empty") {
    val now = System.nanoTime() + 2000 * ms
    val r = report()
    r.completed(0, 1200 * ms, now + 3000 * ms) should not be empty
    r.completed(0, 1 * ms, now + 6000 * ms) shouldBe None
  }
