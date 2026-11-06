package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/** One store's executor under StoreThreadPool: a FIFO of the store's tasks that schedules itself onto the store
 *  thread it is currently placed on, as an actor's mailbox does.
 *
 *  A drain runs the queued tasks in posting order on that thread, at most `batch` of them before it queues itself
 *  again behind the thread's other work, so one busy store cannot starve the others sharing its thread. At most one
 *  drain is queued or running at a time (`scheduled`), so the store's tasks never overlap and keep their order
 *  whichever thread runs them.
 *
 *  That is what makes moving a store safe: `moveTo` only changes where the next drain is queued. A drain already
 *  queued or running finishes on the old thread, and the next one cannot start until it has ended (it is queued only
 *  after `scheduled` is cleared), so the store is never on two threads at once. The handover's happens-before edge is
 *  the `scheduled` flag together with the task queue, as for any two consecutive drains.
 *
 *  The pool measures the store with `nanoClock`: `busyNanos` is the total time spent running its tasks, and
 *  `waitNanos` the total time its drains waited on their thread, from being queued to starting, over `drains`
 *  drains. A drain's wait is time lost to the thread's other work; time a task waits behind the store's own earlier
 *  tasks is not counted, as no other thread could have run it sooner. The counters are written only by the running
 *  drain (drains never overlap) and read by the pool. */
final class StoreMailbox(val storeId: StoreId, initial: SerialLoop, batch: Int, nanoClock: () => Long)
  extends StoreExecutor with Logging:

  require(batch >= 1, s"batch must be at least 1: $batch")

  private val queue = new ConcurrentLinkedQueue[Runnable]()
  private val scheduled = new AtomicBoolean(false)
  @volatile private var loop: SerialLoop = initial
  /** When the queued drain was queued. Written before the drain is queued, so the drain sees it. */
  @volatile private var queuedAt = 0L
  @volatile private var busy = 0L
  @volatile private var waited = 0L
  @volatile private var drainCount = 0L

  /** Total time spent running this store's tasks. */
  def busyNanos: Long = busy

  /** Total time this store's drains waited for their thread. */
  def waitNanos: Long = waited

  /** Drains run so far. */
  def drains: Long = drainCount

  /** No drain is queued or running: every drain so far has finished, counters included. Testing hook. */
  private[host] def isIdle: Boolean = !scheduled.get

  /** The thread loop the next drain is queued on. */
  private[host] def currentLoop: SerialLoop = loop

  /** Places the store on `target`; takes effect from the next drain. Called by StoreThreadPool on the control
   *  thread. */
  private[host] def moveTo(target: SerialLoop): Unit = loop = target

  override def execute(task: Runnable): Unit =
    queue.add(task)
    schedule()

  override def reportFailure(cause: Throwable): Unit =
    logger.error(s"Unhandled exception in a callback for store $storeId", cause)

  private def schedule(): Unit =
    if scheduled.compareAndSet(false, true) then
      queuedAt = nanoClock()
      loop.execute(drain)

  private val drain: Runnable = () =>
    val start = nanoClock()
    waited += start - queuedAt
    drainCount += 1
    var ran = 0
    var task = queue.poll()
    while task ne null do
      HostThreading.runLogged(task)
      ran += 1
      task = if ran < batch then queue.poll() else null
    busy += nanoClock() - start
    scheduled.set(false)
    // A task posted after the last poll saw `scheduled` still set and queued no drain: queue one for it
    if !queue.isEmpty then
      schedule()
