package org.aspen_ddp.aspen.common.util

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object RetryWithBackoffSuite:

  /** Runs nothing by itself: a test fires the scheduled tasks on a thread of its choosing */
  class ManualTasks extends BackgroundTaskManager(ExecutionContext.global):
    private var tasks: List[() => Unit] = Nil
    var scheduled = 0

    override def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask = synchronized:
      scheduled += 1
      tasks = (() => fn) :: tasks
      BackgroundTaskManager.NoTask

    def takeTask(): () => Unit = synchronized:
      val t = tasks.head
      tasks = tasks.tail
      t

class RetryWithBackoffSuite extends AnyFunSuite with Matchers:
  import RetryWithBackoffSuite.*

  def retry(tasks: ManualTasks)(fn: => Boolean): BackgroundTaskManager.ScheduledTask =
    tasks.retryWithExponentialBackoff(tryNow = false, 100.milliseconds, 1.second)(fn)

  test("a failed attempt schedules another and a successful one does not") {
    val tasks = new ManualTasks
    var result = false
    retry(tasks)(result)
    tasks.scheduled shouldBe 1

    tasks.takeTask()()
    tasks.scheduled shouldBe 2

    result = true
    tasks.takeTask()()
    tasks.scheduled shouldBe 2
  }

  // A read completing holds its driver's lock and cancels the retry, while the retry's attempt takes that lock to send
  // its requests. Cancelling must therefore not wait for a running attempt, or the two threads deadlock
  test("cancel returns while an attempt is running, and that attempt schedules no other") {
    val tasks = new ManualTasks
    val running = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val handle = retry(tasks):
      running.countDown()
      finish.await()
      false

    val task = tasks.takeTask()
    val attempt = new Thread(() => task())
    attempt.setDaemon(true)
    attempt.start()
    running.await(5, TimeUnit.SECONDS) shouldBe true

    val cancelled = new CountDownLatch(1)
    val canceller = new Thread(() => {
      handle.cancel()
      cancelled.countDown()
    })
    canceller.setDaemon(true)
    canceller.start()
    val cancelReturned = cancelled.await(5, TimeUnit.SECONDS)

    finish.countDown()
    attempt.join(5000)

    cancelReturned shouldBe true
    attempt.isAlive shouldBe false
    tasks.scheduled shouldBe 1
  }

  test("a cancelled retry does not run an attempt that was already due") {
    val tasks = new ManualTasks
    var attempts = 0
    val handle = retry(tasks):
      attempts += 1
      false

    val task = tasks.takeTask()
    handle.cancel()
    task()

    attempts shouldBe 0
    tasks.scheduled shouldBe 1
  }
