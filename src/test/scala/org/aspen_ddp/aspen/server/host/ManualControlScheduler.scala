package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import java.util.concurrent.Executor
import scala.collection.mutable
import scala.concurrent.duration.Duration

/** A ControlScheduler whose delays never elapse by themselves: a test runs the tasks it wants run, by delay.
 *
 *  A task that is run is posted to `controlEvents`, as production does when a delay expires, rather than run inline.
 *  Under LockstepThreading it therefore runs at the next drain, after whatever was already queued. A cancelled task
 *  that has not been run never runs. */
class ManualControlScheduler(controlEvents: Executor) extends ControlScheduler:
  private final class Entry(val delay: Duration, val fn: () => Unit) extends BackgroundTaskManager.ScheduledTask:
    override def cancel(): Unit = ManualControlScheduler.this.synchronized(entries -= this)

  private val entries = mutable.ListBuffer[Entry]()
  private val history = mutable.ListBuffer[Duration]()

  override def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask = synchronized:
    val entry = new Entry(delay, () => fn)
    entries += entry
    history += delay
    entry

  /** The delays of the tasks scheduled and neither run nor cancelled, in scheduling order. */
  def pending: List[Duration] = synchronized(entries.map(_.delay).toList)

  /** The delay of every task ever scheduled, in scheduling order, whether or not it has been run. */
  def scheduled: List[Duration] = synchronized(history.toList)

  /** Posts every pending task to `controlEvents`. */
  def runAll(): Unit = runWhere(_ => true)

  /** Posts the pending tasks whose delay satisfies `p` to `controlEvents`. */
  def runWhere(p: Duration => Boolean): Unit =
    val due = synchronized:
      val matching = entries.filter(e => p(e.delay)).toList
      entries --= matching
      matching
    due.foreach(e => controlEvents.execute(() => e.fn()))
