package org.aspen_ddp.aspen.server.host

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.fsif.FsifDeviceJournal

/** A device's lanes under LockstepThreading. Management-lane jobs (`submit`) wait on the threading's FIFO, in posting
 *  order with every other posted task, until a test drains it, or on the path's held side queue while a test holds
 *  the path. The data, serial and journal pools run on real daemon threads (an internal `ThreadedDeviceIo` of the same path),
 *  as backend I/O already does in tests; its gate blocks while the path is held, so `hold` stalls them too and
 *  `release` frees them. The health probe (`probe`) runs there too, behind the same gate. `clock` and `probeWrite`
 *  pass to the internal ThreadedDeviceIo (test seams). */
final class LockstepDeviceIo(val path: os.Path, threading: LockstepThreading, cfg: DeviceIo.Config,
                             clock: () => Long = () => System.nanoTime(),
                             probeWrite: os.Path => Unit = HealthProbe.writeAndForce) extends DeviceIo:
  private val threaded = new ThreadedDeviceIo(path, cfg, () => threading.awaitReleased(path), clock, probeWrite)

  def label: String = threaded.label
  def setDeviceId(id: StorageDeviceId): Unit = threaded.setDeviceId(id)
  def observer: DeviceIoObserver = threaded.observer
  /** The internal ThreadedDeviceIo's, on a real daemon thread: its gate blocks while the path is held */
  def probe: HealthProbe = threaded.probe
  def dataEc: ExecutionContext = threaded.dataEc
  def serialEc: ExecutionContext = threaded.serialEc
  /** The internal ThreadedDeviceIo's journal thread, behind the same gate */
  def journalRun: Runnable => Unit = threaded.journalRun
  def fsifJournal: AtomicReference[Option[FsifDeviceJournal]] = threaded.fsifJournal
  def beginRead(): Unit = threaded.beginRead()
  def endRead(): Unit = threaded.endRead()
  def readsOverBound: Boolean = threaded.readsOverBound

  // Management jobs submitted whose task has not run yet (on the FIFO, held, or cancelled and not yet drained)
  private val laneJobs = new java.util.concurrent.atomic.AtomicInteger(0)

  def idle: Boolean = laneJobs.get() == 0 && threaded.poolIdle

  @volatile private var closed = false

  def submit[T](jobKind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T] =
    val p = Promise[T]()
    val task = new LockstepThreading.DeviceTask(jobKind, path):
      def run(): Unit =
        try
          if closed && state.compareAndSet(0, 2) then
            p.tryFailure(DeviceIo.Cancelled)
          else if state.compareAndSet(0, 1) then
            val token = observer.started(kind.label, store, 0L)
            // Every throwable fails the future, fatal ones included, as ThreadedDeviceIo's
            val r: Try[T] =
              try Success(job)
              catch case t: Throwable => Failure(t)
            observer.finished(token, r.isSuccess)
            p.complete(r)
        finally laneJobs.decrementAndGet(): Unit
    // As ThreadedDeviceIo: a closed DeviceIo runs nothing more, and a job queued before its close fails Cancelled
    if closed then
      task.state.set(2)
      p.tryFailure(DeviceIo.Cancelled)
    else
      laneJobs.incrementAndGet()
      threading.enqueueDevice(path, task)
    new JobHandle[T]:
      def future: Future[T] = p.future
      def cancel(): Boolean =
        if task.state.compareAndSet(0, 2) then
          p.tryFailure(DeviceIo.Cancelled)
          true
        else false

  def closeLane(): Unit =
    closed = true
    threaded.closeLane()

  def close(): Unit =
    closed = true
    threaded.close()
