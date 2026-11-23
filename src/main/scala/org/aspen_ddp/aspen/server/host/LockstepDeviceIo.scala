package org.aspen_ddp.aspen.server.host

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

/** A device's lanes under LockstepThreading. Management-lane jobs (`submit`) wait on the threading's FIFO, in posting
 *  order with every other posted task, until a test drains it, or on the path's held side queue while a test holds
 *  the path. The data and serial pools run on real daemon threads (an internal `ThreadedDeviceIo` of the same path),
 *  as backend I/O already does in tests; its gate blocks while the path is held, so `hold` stalls them too and
 *  `release` frees them. */
final class LockstepDeviceIo(val path: os.Path, threading: LockstepThreading, cfg: DeviceIo.Config) extends DeviceIo:
  private val threaded = new ThreadedDeviceIo(path, cfg, () => threading.awaitReleased(path))

  def label: String = threaded.label
  def setDeviceId(id: StorageDeviceId): Unit = threaded.setDeviceId(id)
  def observer: DeviceIoObserver = threaded.observer
  def dataEc: ExecutionContext = threaded.dataEc
  def serialEc: ExecutionContext = threaded.serialEc
  def beginRead(): Unit = threaded.beginRead()
  def endRead(): Unit = threaded.endRead()
  def readsOverBound: Boolean = threaded.readsOverBound

  // Management jobs submitted whose task has not run yet (on the FIFO, held, or cancelled and not yet drained)
  private val laneJobs = new java.util.concurrent.atomic.AtomicInteger(0)

  def idle: Boolean = laneJobs.get() == 0 && threaded.poolIdle

  def submit[T](jobKind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T] =
    val p = Promise[T]()
    val task = new LockstepThreading.DeviceTask(jobKind):
      def run(): Unit =
        try
          if state.compareAndSet(0, 1) then
            val token = observer.started(kind.label, store, 0L)
            val r = Try(job)
            observer.finished(token, r.isSuccess)
            p.complete(r)
        finally laneJobs.decrementAndGet(): Unit
    laneJobs.incrementAndGet()
    threading.enqueueDevice(path, task)
    new JobHandle[T]:
      def future: Future[T] = p.future
      def cancel(): Boolean =
        if task.state.compareAndSet(0, 2) then
          p.tryFailure(DeviceIo.Cancelled)
          true
        else false

  def close(): Unit = threaded.close()
