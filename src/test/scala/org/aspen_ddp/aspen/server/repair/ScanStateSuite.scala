package org.aspen_ddp.aspen.server.repair

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.util.Random

class ScanStateSuite extends AnyFunSuite with Matchers:

  private def rand = new Random(12345L)
  private val policy = RepairPolicy.Default

  test("a newly admitted store is due within the first floor interval"):
    val r = rand
    (0 until 200).foreach: _ =>
      val s = ScanState.admit(1000L, policy, r)
      s.currentInterval shouldBe policy.scanIntervalFloor
      s.nextDue should be >= 1000L
      s.nextDue should be < 1000L + policy.scanIntervalFloor.toMillis

  test("jitter stays within plus or minus 25 percent"):
    val r = rand
    (0 until 500).foreach: _ =>
      val j = ScanState.jitter(1000L, r)
      j should be >= 750L
      j should be <= 1250L

  test("clean scans double the interval up to the cap and stop there"):
    val r = rand
    var s = ScanState(0L, policy.scanIntervalFloor)
    val intervals = (0 until 8).map: _ =>
      s = ScanState.advance(s, 0L, foundEntries = false, policy, r)
      s.currentInterval
    intervals.take(5) shouldBe Seq(
      Duration(60, SECONDS), Duration(120, SECONDS), Duration(240, SECONDS),
      Duration(480, SECONDS), Duration(15, MINUTES))
    intervals.drop(5).foreach(_ shouldBe Duration(15, MINUTES))

  test("a scan that finds entries snaps back to the floor"):
    val r = rand
    val backedOff = ScanState(0L, Duration(15, MINUTES))
    val s = ScanState.advance(backedOff, 0L, foundEntries = true, policy, r)
    s.currentInterval shouldBe policy.scanIntervalFloor

  test("the next due time is the new interval from now, jittered"):
    val r = rand
    val s = ScanState.advance(ScanState(0L, policy.scanIntervalFloor), 5000L,
                              foundEntries = true, policy, r)
    s.nextDue should be >= 5000L + 22_500L
    s.nextDue should be <= 5000L + 37_500L

  test("an interval below the pool floor is raised to it rather than doubled from"):
    val r = rand
    val stale = ScanState(0L, Duration(1, SECONDS))
    val s = ScanState.advance(stale, 0L, foundEntries = false, policy, r)
    s.currentInterval shouldBe policy.scanIntervalFloor
