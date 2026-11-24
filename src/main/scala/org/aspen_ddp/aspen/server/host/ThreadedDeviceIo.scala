package org.aspen_ddp.aspen.server.host

import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, ThreadFactory}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

import scribe.Logging
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

/** `gate` runs before every task on every thread of this DeviceIo; hang tests pass a latch await. */
class ThreadedDeviceIo(val path: os.Path, cfg: DeviceIo.Config, gate: () => Unit = () => ()) extends DeviceIo, Logging:
  private val baseName: String = path.segments.toList.lastOption.getOrElse("root")
  @volatile private var currentLabel: String = baseName
  val observer: DeviceIoObserver = new DeviceIoObserver

  private final class Named(val role: String, val index: Int, val thread: Thread)
  private val threads = new java.util.concurrent.ConcurrentLinkedQueue[Named]()

  private def threadName(role: String, index: Int): String =
    if index < 0 then s"dev-$currentLabel-$role" else s"dev-$currentLabel-$role-$index"

  private def factory(role: String, single: Boolean): ThreadFactory =
    val counter = new AtomicInteger(0)
    (r: Runnable) =>
      val idx = if single then -1 else counter.getAndIncrement()
      val t = new Thread(r, threadName(role, idx))
      t.setDaemon(true)
      threads.add(new Named(role, idx, t))
      t

  def label: String = currentLabel

  def setDeviceId(id: StorageDeviceId): Unit =
    currentLabel = s"$baseName-${id.uuid}"
    threads.forEach(n => n.thread.setName(threadName(n.role, n.index)))

  private val laneThread = new Thread(() => laneLoop(), threadName("mgmt", -1))
  laneThread.setDaemon(true)
  threads.add(new Named("mgmt", -1, laneThread))

  private val Queued = 0
  private val Running = 1
  private val Cancelled = 2

  private final class Entry(val state: AtomicInteger, val run: () => Unit, val skip: () => Unit)
  private val lane = new LinkedBlockingQueue[Entry]()
  // Lane jobs submitted and not yet run or skipped, and pool tasks executed and not yet ended (idle)
  private val laneJobs = new AtomicInteger(0)
  private val poolTasks = new AtomicInteger(0)
  @volatile private var closed = false

  private def laneLoop(): Unit =
    try
      while !closed do
        val e = lane.take()
        try
          if e.state.compareAndSet(Queued, Running) then e.run()
          else e.skip()
        finally laneJobs.decrementAndGet(): Unit
    catch case _: InterruptedException => ()
    finally cancelQueued()

  /** Fails every job still queued with `DeviceIo.Cancelled` (close): none is left with a future that never completes */
  private def cancelQueued(): Unit =
    var e = lane.poll()
    while e != null do
      try
        // Polled, so the lane never takes it: Queued, or Cancelled by its handle
        e.state.set(Cancelled)
        e.skip()
      finally laneJobs.decrementAndGet(): Unit
      e = lane.poll()

  private val laneStarted = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** The lane thread starts with the first job, so a DeviceIo that never runs one (a host root nobody lists, the
   *  unused lane of a LockstepDeviceIo) holds no thread. */
  private def startLane(): Unit =
    if laneStarted.compareAndSet(false, true) && !closed then laneThread.start()

  def submit[T](kind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T] =
    val p = Promise[T]()
    val state = new AtomicInteger(Queued)
    // Everything the job (or the gate) throws fails the future, fatal throwables included (an InterruptedException
    // from close): a job that ran never leaves its future incomplete or its observer entry open
    val run: () => Unit = () =>
      val token = observer.started(kind.label, store, 0L)
      val r: Try[T] =
        try
          gate()
          Success(job)
        catch case t: Throwable => Failure(t)
      // Finished before the future completes, so a caller that saw the result sees the job ended
      observer.finished(token, r.isSuccess)
      p.complete(r)
      ()
    val skip: () => Unit = () => { p.tryFailure(DeviceIo.Cancelled); () }
    if closed then
      // A closed DeviceIo (DeviceIos.forget, Host.shutdown) runs nothing more: Cancelled at once, "nothing happened"
      state.set(Cancelled)
      p.tryFailure(DeviceIo.Cancelled)
    else
      laneJobs.incrementAndGet()
      lane.add(new Entry(state, run, skip))
      startLane()
      // Closed meanwhile: the lane thread may already have drained its queue, so drain it here too
      if closed then cancelQueued()
    new JobHandle[T]:
      def future: Future[T] = p.future
      def cancel(): Boolean =
        if state.compareAndSet(Queued, Cancelled) then
          p.tryFailure(DeviceIo.Cancelled)
          true
        else false

  private def pool(role: String, n: Int): (ExecutorService, ExecutionContext) =
    val exec = Executors.newFixedThreadPool(n, factory(role, false))
    val ec = new ExecutionContext:
      def execute(r: Runnable): Unit =
        poolTasks.incrementAndGet()
        try
          exec.execute: () =>
            val token = observer.started(if role == "io" then "data" else "serial", None, 0L)
            var ok = false
            try
              gate()
              r.run()
              ok = true
            finally
              observer.finished(token, ok)
              poolTasks.decrementAndGet(): Unit
        catch case t: Throwable =>
          poolTasks.decrementAndGet()
          throw t
      def reportFailure(t: Throwable): Unit = logger.error(s"device io task failed: $t")
    (exec, ec)

  private val (dataExec, dataEcImpl) = pool("io", cfg.dataThreads)
  private val (serialExec, serialEcImpl) = pool("serial", cfg.serialThreads)

  def dataEc: ExecutionContext = dataEcImpl
  def serialEc: ExecutionContext = serialEcImpl

  private val reads = new AtomicLong(0)
  def beginRead(): Unit = reads.incrementAndGet()
  def endRead(): Unit = reads.decrementAndGet()
  def readsOverBound: Boolean = reads.get() >= cfg.readBound

  /** Backend reads begun and not ended (beginRead, endRead): the backlog the read bound limits */
  private[aspen] def outstandingReads: Long = reads.get()

  /** Data and serial tasks executed and not yet ended: queued or running on the pools */
  private[aspen] def poolTasksOutstanding: Int = poolTasks.get()

  def idle: Boolean = laneJobs.get() == 0 && poolIdle

  /** No data or serial task queued or running */
  private[host] def poolIdle: Boolean = poolTasks.get() == 0

  /** Stops the lane and both pools without waiting. Every lane job not yet started fails with `DeviceIo.Cancelled`,
   *  and every later submit at once. A running job is interrupted (a hung syscall is not: its thread stays, confined
   *  to this device); a data or serial task not yet started is dropped, which only the owner of a device no longer in
   *  use (DeviceIos.forget closes only an idle one) or a host shutting down ever does */
  def close(): Unit =
    closed = true
    if laneStarted.get() then laneThread.interrupt() else cancelQueued()
    dataExec.shutdownNow()
    serialExec.shutdownNow()
