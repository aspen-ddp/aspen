package org.aspen_ddp.aspen.common.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class BackgroundTaskManagerSuite extends AnyFunSuite with Matchers:

  test("a task scheduled before shutdown and not yet due never runs once the manager is shut down"):
    val bg = new BackgroundTaskManager(ExecutionContext.parasitic)
    val ran = new AtomicBoolean(false)
    bg.schedule(200.millis)(ran.set(true))
    bg.shutdown(Duration.Zero)
    // Past its delay: a later task on a fresh manager has had time to run, this one has not
    val later = new CountDownLatch(1)
    val other = new BackgroundTaskManager(ExecutionContext.parasitic)
    other.schedule(400.millis)(later.countDown())
    later.await(10, TimeUnit.SECONDS) should be(true)
    other.shutdown(Duration.Zero)
    ran.get should be(false)

  test("a schedule after shutdown is dropped"):
    val bg = new BackgroundTaskManager(ExecutionContext.parasitic)
    bg.shutdown(Duration.Zero)
    val ran = new AtomicBoolean(false)
    bg.schedule(Duration.Zero)(ran.set(true)) should be(BackgroundTaskManager.ShutdownTask)
    bg.schedulePeriodic(10.millis)(ran.set(true)) should be(BackgroundTaskManager.ShutdownTask)
    ran.get should be(false)
