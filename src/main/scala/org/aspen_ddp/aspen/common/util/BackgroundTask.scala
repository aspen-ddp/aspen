package org.aspen_ddp.aspen.common.util

import scala.concurrent.duration.Duration

trait BackgroundTask {

  import BackgroundTask._

  /** Waits for up to gracefulShutdownDelay for all outstanding tasks to finish.
    * @return true if the shutdown was clean. false otherwise
    */
  def shutdown(gracefulShutdownDelay: Duration): Boolean

  def schedule(delay: Duration)(fn: => Unit): ScheduledTask

  def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask

  /** initialDelay uses the same units as the period
    *
    * @param callNow Defaults to false. If true, the function will be executed immediately otherwise it waits for the polling period to elapse
    */
  def schedulePeriodic(period: Duration, callNow: Boolean=false)(fn: => Unit): ScheduledTask

  def retryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean): ScheduledTask
}

object BackgroundTask {

  trait ScheduledTask {
    def cancel(): Unit
  }

  object NoBackgroundTasks extends BackgroundTask {
    /** Waits for up to gracefulShutdownDelay for all outstanding tasks to finish.
      *
      * @return true if the shutdown was clean. false otherwise
      */
    override def shutdown(gracefulShutdownDelay: Duration): Boolean = true

    override def schedule(delay: Duration)(fn: => Unit): ScheduledTask = new ScheduledTask {
      override def cancel(): Unit = ()
    }

    override def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask = new ScheduledTask {
      override def cancel(): Unit = ()
    }

    /** initialDelay uses the same units as the period
      *
      * @param callNow Defaults to false. If true, the function will be executed immediately otherwise it waits for the polling period to elapse
      */
    override def schedulePeriodic(period: Duration, callNow: Boolean)(fn: => Unit): ScheduledTask = new ScheduledTask {
      override def cancel(): Unit = ()
    }

    override def retryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean): ScheduledTask = new ScheduledTask {
      override def cancel(): Unit = ()
    }
  }
}
