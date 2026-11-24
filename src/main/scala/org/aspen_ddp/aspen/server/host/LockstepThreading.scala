package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.concurrent.{CountDownLatch, Executor, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicInteger
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

  override def newDeviceIo(path: os.Path): DeviceIo = new LockstepDeviceIo(path, this, DeviceIo.Config())

  private val holdLock = new Object
  private var holds = Map.empty[os.Path, CountDownLatch]
  private var heldTasks = Map.empty[os.Path, Vector[LockstepThreading.DeviceTask]]

  /** A management-lane job of `path`'s DeviceIo: on the FIFO, or on the path's side queue while it is held. */
  private[host] def enqueueDevice(path: os.Path, task: LockstepThreading.DeviceTask): Unit =
    holdLock.synchronized:
      if holds.contains(path) then heldTasks += path -> (heldTasks.getOrElse(path, Vector()) :+ task)
      else queue.put(task)

  /** Blocks the calling (device) thread while `path` is held. */
  private[host] def awaitReleased(path: os.Path): Unit =
    val latch = holdLock.synchronized(holds.get(path))
    latch.foreach(_.await())

  /** Testing hook: from now on `path`'s management jobs wait on a side queue, and its data and serial tasks block,
   *  until `release`. Jobs already on the FIFO are not affected. */
  def hold(path: os.Path): Unit =
    holdLock.synchronized:
      if !holds.contains(path) then holds += path -> new CountDownLatch(1)

  /** Testing hook: puts the path's held jobs on the FIFO in submission order and frees its blocked threads. */
  def release(path: os.Path): Unit =
    val latch = holdLock.synchronized:
      heldTasks.getOrElse(path, Vector()).foreach(queue.put)
      heldTasks -= path
      val l = holds.get(path)
      holds -= path
      l
    latch.foreach(_.countDown())

  /** Testing hook: whether `path` is held (`hold`) */
  def isHeld(path: os.Path): Boolean = holdLock.synchronized(holds.contains(path))

  /** Testing hook: the kinds of `path`'s held management jobs, in submission order. */
  def held(path: os.Path): List[JobKind] =
    holdLock.synchronized(heldTasks.getOrElse(path, Vector()).map(_.kind).toList)

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

object LockstepThreading:
  /** One management-lane job of a LockstepDeviceIo. `state`: 0 queued, 1 running, 2 cancelled. */
  abstract class DeviceTask(val kind: JobKind) extends Runnable:
    val state = new AtomicInteger(0)
