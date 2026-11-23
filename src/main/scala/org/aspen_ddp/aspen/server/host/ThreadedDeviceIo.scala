package org.aspen_ddp.aspen.server.host

import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, ThreadFactory}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import scribe.Logging
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

/** `gate` runs before every task on every thread of this DeviceIo; hang tests pass a latch await. */
class ThreadedDeviceIo(val path: os.Path, cfg: DeviceIo.Config, gate: () => Unit = () => ()) extends DeviceIo, Logging:
  @volatile private var currentLabel: String = path.last
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
    currentLabel = s"${path.last}-${id.uuid}"
    threads.forEach(n => n.thread.setName(threadName(n.role, n.index)))

  private val laneThread = new Thread(() => laneLoop(), threadName("mgmt", -1))
  laneThread.setDaemon(true)
  threads.add(new Named("mgmt", -1, laneThread))

  private val Queued = 0
  private val Running = 1
  private val Cancelled = 2

  private final class Entry(val state: AtomicInteger, val run: () => Unit, val skip: () => Unit)
  private val lane = new LinkedBlockingQueue[Entry]()
  @volatile private var closed = false

  private def laneLoop(): Unit =
    try
      while !closed do
        val e = lane.take()
        try
          gate()
          if e.state.compareAndSet(Queued, Running) then e.run() else e.skip()
        catch case t: Throwable => logger.error(s"device io lane task failed: $t")
    catch case _: InterruptedException => ()

  laneThread.start()

  def submit[T](kind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T] =
    val p = Promise[T]()
    val state = new AtomicInteger(Queued)
    val run: () => Unit = () =>
      val token = observer.started(kind.label, store, 0L)
      val r = Try(job)
      observer.finished(token, r.isSuccess)
      p.complete(r)
      ()
    val skip: () => Unit = () => { p.tryFailure(DeviceIo.Cancelled); () }
    lane.add(new Entry(state, run, skip))
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
        exec.execute: () =>
          val token = observer.started(if role == "io" then "data" else "serial", None, 0L)
          var ok = false
          try
            gate()
            r.run()
            ok = true
          finally observer.finished(token, ok)
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

  def close(): Unit =
    closed = true
    laneThread.interrupt()
    dataExec.shutdownNow()
    serialExec.shutdownNow()
