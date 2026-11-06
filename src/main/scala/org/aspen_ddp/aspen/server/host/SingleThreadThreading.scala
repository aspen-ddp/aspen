package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executor, LinkedBlockingQueue}
import scala.concurrent.ExecutionContext

/** Production threading, as the host's event loop ran: one thread, `aspen-host-control`, runs everything posted
 *  through `control`, `controlEvents` and `dispatch`, one task at a time in posting order. Stores run inline, as
 *  their work already runs on that thread.
 *
 *  Work posted before `start` waits for it. The thread is not a daemon: `shutdown` stops it once the work already
 *  queued has run, and anything posted after that is dropped. */
class SingleThreadThreading extends HostThreading with Logging:
  private val queue = new LinkedBlockingQueue[Runnable]()

  /** Queued by shutdown: the loop ends on taking it. */
  private val Stop: Runnable = () => ()

  private val started = new AtomicBoolean(false)
  @volatile private var stopped = false
  private val droppedWarned = new AtomicBoolean(false)

  private val thread = new Thread(() => loop(), "aspen-host-control")

  private def loop(): Unit =
    var task = queue.take()
    while task ne Stop do
      HostThreading.runLogged(task)
      task = queue.take()

  private val enqueue: Executor = task =>
    if !stopped then
      queue.put(task)
    else if droppedWarned.compareAndSet(false, true) then
      logger.warn("Dropping work posted to the host's control thread after its shutdown. Later drops are not logged")

  override val control: ExecutionContext = ExecutionContext.fromExecutor(enqueue,
    cause => logger.error("Unhandled exception in a control-thread callback", cause))
  override def controlEvents: Executor = enqueue
  override def dispatch: Executor = enqueue
  override def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor = HostThreading.InlineStoreExecutor
  override def release(storeId: StoreId): Unit = ()

  override def start(): Unit =
    if !stopped && started.compareAndSet(false, true) then
      thread.start()

  override def shutdown(): Unit =
    stopped = true
    queue.put(Stop)
