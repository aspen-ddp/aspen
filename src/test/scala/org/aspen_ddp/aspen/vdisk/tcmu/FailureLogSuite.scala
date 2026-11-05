package org.aspen_ddp.aspen.vdisk.tcmu

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scribe.Level

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*

class FailureLogSuite extends AnyFunSuite with Matchers:

  private final class Fixture:
    val clock = AtomicLong(0L)
    val lines = ListBuffer[(Level, String)]()
    val log = FailureLog("dev1 (uio3, disk vd1)", clock = () => clock.get,
      log = (l, m) => lines.synchronized(lines += ((l, m))): Unit)
    def at(level: Level): List[String] = lines.synchronized(lines.toList).filter(_._1 == level).map(_._2)
    def advance(d: FiniteDuration): Unit = clock.addAndGet(d.toNanos): Unit

  test("the first full report of a kind carries the cause's stack trace; a summary does not"):
    val fx = Fixture()
    val cause = IllegalStateException("boom")
    fx.log.report("command-error", "cmd_id=1 failed", "ring", Some(cause))
    fx.log.report("command-error", "cmd_id=2 failed", "ring", Some(cause))
    fx.advance(61.seconds)
    fx.log.report("command-error", "cmd_id=3 failed", "ring", Some(cause))
    val List(full) = fx.at(Level.Error): @unchecked
    full should startWith ("dev1 (uio3, disk vd1): command-error: cmd_id=1 failed; ring\njava.lang.IllegalStateException: boom")
    full should include ("at org.aspen_ddp.aspen.vdisk.tcmu.FailureLogSuite")
    val List(summary) = fx.at(Level.Warn): @unchecked
    summary should include ("2 more command-error since")
    summary should not include ("IllegalStateException")

  test("a full report may be logged at warn"):
    val fx = Fixture()
    fx.log.report("bad-request", "short payload", "cdb=2a", level = Level.Warn)
    fx.at(Level.Warn) should be (List("dev1 (uio3, disk vd1): bad-request: short payload; cdb=2a"))
    fx.at(Level.Error) should be (empty)

  test("a burst that stops is summarized by the next report of another kind after its window"):
    val fx = Fixture()
    for i <- 1 to 4 do fx.log.report("disk-error", s"read $i failed", "cdb")
    fx.advance(30.seconds)
    fx.log.report("bad-request", "x", "cdb")
    // Still within disk-error's window: nothing summarized yet
    fx.at(Level.Warn) should be (empty)
    fx.advance(31.seconds)
    fx.log.report("bad-request", "y", "cdb")
    val warns = fx.at(Level.Warn)
    warns.exists(w => w.contains("3 more disk-error since") && w.contains("latest: read 4 failed")) should be (true)
    // bad-request's own second report, within its window, stays quiet
    warns.count(_.contains("bad-request")) should be (0)
    // The burst was summarized, so the next disk-error is logged in full
    fx.log.report("disk-error", "read 5 failed", "cdb")
    fx.at(Level.Error).count(_.contains("disk-error")) should be (2)
    fx.log.counts("disk-error") should be (5L)

  test("flush summarizes every kind with suppressed reports, whatever its window"):
    val fx = Fixture()
    fx.log.report("disk-error", "a", "cdb")
    fx.log.report("disk-error", "b", "cdb")
    fx.log.report("ring-corrupt", "c", "dump")
    fx.log.flush()
    fx.at(Level.Warn) match
      case List(w) => w should include ("1 more disk-error since")
      case other => fail(s"expected one summary, got $other")
    fx.log.flush()
    fx.at(Level.Warn).size should be (1)
    // Flushed, so the next one is logged in full even within the window
    fx.log.report("disk-error", "d", "cdb")
    fx.at(Level.Error).count(_.contains("disk-error")) should be (2)
