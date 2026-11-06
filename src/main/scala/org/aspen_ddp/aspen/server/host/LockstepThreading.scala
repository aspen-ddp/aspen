package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.concurrent.{Executor, LinkedBlockingQueue}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

/** The regression tests' threading: everything posted, by `dispatch` or `controlEvents`, waits on one FIFO until a
 *  test drains it (`drain`, through Host.testingOnlyHandleEvents), and Future callbacks run on `ec`, the test's own
 *  ExecutionContext. That keeps the ordering the integration suites have always seen. Stores run inline on whatever
 *  thread posts to them, which is the draining thread.
 *
 *  Unlike production, a throwing task is not logged and swallowed: it propagates out of `drain` to the test.
 *
 *  Thread-safe to post to from any thread; draining may be re-entered from a task that is itself being drained. */
class LockstepThreading(ec: ExecutionContext) extends HostThreading:
  private val queue = new LinkedBlockingQueue[Runnable]()

  private val enqueue: Executor = task => queue.put(task)

  override def control: ExecutionContext = ec
  override def controlEvents: Executor = enqueue
  override def dispatch: Executor = enqueue
  override def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor = HostThreading.InlineStoreExecutor
  override def release(storeId: StoreId): Unit = ()
  override def start(): Unit = ()
  /** Nothing to stop: work still queued runs if a test drains again, as before the host had a threading. */
  override def shutdown(): Unit = ()

  /** Runs queued tasks, including any they post, until the queue is empty. A task's exception is not caught: it
   *  propagates to the caller and every later task stays queued for the next drain, as testingOnlyHandleEvents
   *  always behaved. */
  def drain(): Unit =
    var task = queue.poll()
    while task != null do
      task.run()
      task = queue.poll()

  def hasQueued: Boolean = !queue.isEmpty

  /** Testing hook: the queued tasks, in order. */
  private[aspen] def queuedItems: List[Runnable] = queue.asScala.toList

  /** Testing hook: removes the queued tasks matching `p` and returns them in queue order, leaving the rest queued. */
  private[aspen] def removeQueued(p: Runnable => Boolean): List[Runnable] =
    val it = queue.iterator()
    var removed = List[Runnable]()
    while it.hasNext do
      val task = it.next()
      if p(task) then
        it.remove()
        removed = task :: removed
    removed.reverse
