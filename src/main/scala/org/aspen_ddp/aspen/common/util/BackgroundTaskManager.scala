package org.aspen_ddp.aspen.common.util

import java.util.concurrent.{Executors, ScheduledFuture, ThreadLocalRandom, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/**
 * Spawns a background thread to use for scheduling the execution of the call by name
 * code blocks passed to the various scheduling methods. The ExecutionContext passed to
 * the constructor will be used for executing all of the scheduled functions. The
 * background thread is used purely for scheduling purposes.
 * 
 * Note that "Future.unit.map(_ => fn)" in the class implementation is used to pass the
 * call by name parameter to the given ExecutionContext.
 */
class BackgroundTaskManager(protected val executionContext: ExecutionContext) {
  import BackgroundTaskManager.*

  private  val sched = Executors.newScheduledThreadPool(1)
  private  val rand = new java.util.Random

  given ExecutionContext = executionContext

  def shutdown(gracefulShutdownDelay: Duration): Boolean = {
    sched.shutdown()
    sched.awaitTermination(gracefulShutdownDelay.toMillis, TimeUnit.MILLISECONDS)
    sched.isTerminated
  }

  private case class BGTask[T](sf: ScheduledFuture[T]) extends ScheduledTask {
    override def cancel(): Unit = sf.cancel(false)
  }

  def schedule(delay: Duration)(fn: => Unit): ScheduledTask = synchronized {
    try
      BGTask(sched.schedule( () => Future.unit.map(_ => fn), delay.length, delay.unit))
    catch
      case _: java.util.concurrent.RejectedExecutionException => ShutdownTask
  }

  def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask = synchronized {
    // TODO: Fix Long -> Int conversion
    val actualDelay = rand.nextInt(window.length.toInt)

    try
      BGTask(sched.schedule(() => Future.unit.map(_ => fn), actualDelay, window.unit))
    catch
      case _: java.util.concurrent.RejectedExecutionException => ShutdownTask
  }

  /** initialDelay uses the same units as the period
    *
    * @param callNow Defaults to false. If true, the function will be executed immediately otherwise it waits for the polling period to elapse
    */
  def schedulePeriodic(period: Duration, callNow: Boolean=false)(fn: => Unit): ScheduledTask = synchronized {
    val initialDelay = if (callNow) 0L else period.length
    try
      BGTask(sched.scheduleAtFixedRate(() => Future.unit.map(_ => fn), initialDelay, period.length, period.unit))
    catch
      case _: java.util.concurrent.RejectedExecutionException => ShutdownTask
  }

  def retryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean): ScheduledTask = {
    RetryWithExponentialBackoff(tryNow, initialDelay, maxDelay)(fn)
  }

  /** Continually retries the function until it returns true */
  protected case class RetryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean) extends ScheduledTask {
    private var task: Option[ScheduledTask] = None
    private var backoffDelay = initialDelay

    if (tryNow)
      Future.unit.map(_ => attempt())
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

        Duration(ThreadLocalRandom.current().nextInt(0, backoffDelay.toMillis.toInt), MILLISECONDS)
      }
      else
        backoffDelay

      task = Some(schedule(thisDelay) { attempt() })
    }
  }
}

object BackgroundTaskManager:

  trait ScheduledTask:
    def cancel(): Unit

  // Not much we can do if the scheduler has been shut down. Most likely the client
  // has been shut down and background finalization tasks are continuing to retry.
  // This object will be returned by the schedule* methods which should allow for
  // a clean shutdown
  object ShutdownTask extends ScheduledTask:
    def cancel(): Unit = ()

  // We'll ust the global execution context as the argument to the base class since
  // we have to provide something. It'll never be used though due to the overridden
  // methods doing nothing
  object NoBackgroundTaskManager extends BackgroundTaskManager(scala.concurrent.ExecutionContext.Implicits.global):

    override def shutdown(gracefulShutdownDelay: Duration): Boolean = true

    override def schedule(delay: Duration)(fn: => Unit): ScheduledTask = ShutdownTask

    override def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask = ShutdownTask

    override def schedulePeriodic(period: Duration, callNow: Boolean)(fn: => Unit): ScheduledTask = ShutdownTask

    override def retryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean): ScheduledTask = ShutdownTask
  
