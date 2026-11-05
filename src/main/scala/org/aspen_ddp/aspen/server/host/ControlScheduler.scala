package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import scala.concurrent.duration.Duration

/** Runs delayed work for the host's control logic. */
trait ControlScheduler:
  def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask

/** Schedules on a BackgroundTaskManager. `fn` is never run on the timer thread: when the delay expires it is
 *  handed to `post`, which runs it where the host's control logic runs. */
class BackgroundControlScheduler(bg: BackgroundTaskManager, post: (() => Unit) => Unit) extends ControlScheduler:
  override def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask =
    bg.schedule(delay)(post(() => fn))
