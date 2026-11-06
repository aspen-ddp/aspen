package org.aspen_ddp.aspen.server.host

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class ManualControlSchedulerSuite extends AnyFunSuite with Matchers:

  test("a scheduled task waits, and runs posted to controlEvents rather than inline"):
    val t = new LockstepThreading(ExecutionContext.parasitic)
    val s = new ManualControlScheduler(t.controlEvents)
    val ran = ListBuffer[String]()
    s.schedule(5.seconds)(ran += "a")
    s.pending should be(List(5.seconds))
    s.runAll()
    ran.toList should be(Nil)
    s.pending should be(Nil)
    t.drain()
    ran.toList should be(List("a"))
    s.scheduled should be(List(5.seconds))

  test("runWhere runs only the matching delays"):
    val t = new LockstepThreading(ExecutionContext.parasitic)
    val s = new ManualControlScheduler(t.controlEvents)
    val ran = ListBuffer[String]()
    s.schedule(5.seconds)(ran += "short")
    s.schedule(60.seconds)(ran += "long")
    s.runWhere(_ == 60.seconds)
    t.drain()
    ran.toList should be(List("long"))
    s.pending should be(List(5.seconds))

  test("a cancelled task never runs"):
    val t = new LockstepThreading(ExecutionContext.parasitic)
    val s = new ManualControlScheduler(t.controlEvents)
    var ran = false
    s.schedule(5.seconds) { ran = true }.cancel()
    s.runAll()
    t.drain()
    ran should be(false)
