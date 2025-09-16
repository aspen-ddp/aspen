package org.aspen_ddp.aspen.common.util

import java.util.concurrent.{Executors, ScheduledFuture, ThreadLocalRandom, TimeUnit}

import org.aspen_ddp.aspen.common.util.BackgroundTask.ScheduledTask

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS}

class BackgroundTaskPool extends BackgroundTask {
  private  var sched = Executors.newScheduledThreadPool(1)
  private  var ec = ExecutionContext.fromExecutorService(sched)
  private  val rand = new java.util.Random

  def shutdown(gracefulShutdownDelay: Duration): Boolean = {
    sched.shutdown()
    sched.awaitTermination(gracefulShutdownDelay.toMillis, TimeUnit.MILLISECONDS)
    sched.isTerminated
  }

  def resizeThreadPool(numThreads: Int): Unit = synchronized {
    sched.shutdown() // Previously submitted tasks will be executed before the pool is destroyed
    sched = Executors.newScheduledThreadPool(numThreads)
    ec = ExecutionContext.fromExecutorService(sched)
  }

  private case class BGTask[T](sf: ScheduledFuture[T]) extends ScheduledTask {
    override def cancel(): Unit = sf.cancel(false)
  }

  def schedule(delay: Duration)(fn: => Unit): ScheduledTask = synchronized {
    BGTask(sched.schedule( () => fn , delay.length, delay.unit))
  }

  def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask = synchronized {
    // TODO: Fix Long -> Int conversion
    val actualDelay = rand.nextInt(window.length.asInstanceOf[Int])

    BGTask(sched.schedule(() => fn, actualDelay, window.unit))
  }

  /** initialDelay uses the same units as the period
    *
    * @param callNow Defaults to false. If true, the function will be executed immediately otherwise it waits for the polling period to elapse
    */
  def schedulePeriodic(period: Duration, callNow: Boolean=false)(fn: => Unit): ScheduledTask = synchronized {
    val initialDelay = if (callNow) 0L else period.length
    BGTask(sched.scheduleAtFixedRate(() => fn, initialDelay, period.length, period.unit))
  }

  def retryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean): ScheduledTask = {
    RetryWithExponentialBackoff(tryNow, initialDelay, maxDelay)(fn)
  }

  /** Continually retries the function until it returns true */
  protected case class RetryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean) extends ScheduledTask {
    private var task: Option[ScheduledTask] = None
    private var backoffDelay = initialDelay

    if (tryNow)
      attempt()
    else
      reschedule(false)

    private def attempt(): Unit = synchronized {
      if (!fn) reschedule(true)
    }

    override def cancel(): Unit = synchronized {
      task.foreach(_.cancel())
      task = None
    }

    private def reschedule(backoff: Boolean): Unit = synchronized {
      val thisDelay = if (backoff) {
        backoffDelay = backoffDelay * 2
        if (backoffDelay > maxDelay)
          backoffDelay = maxDelay

        Duration(ThreadLocalRandom.current().nextInt(0, backoffDelay.toMillis.asInstanceOf[Int]), MILLISECONDS)
      }
      else
        backoffDelay

      task = Some(schedule(thisDelay) { attempt() })
    }
  }
}
