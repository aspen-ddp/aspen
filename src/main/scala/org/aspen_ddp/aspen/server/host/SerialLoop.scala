package org.aspen_ddp.aspen.server.host

import scribe.Logging

import java.util.concurrent.{Executor, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean

/** One named thread that runs everything posted to it, one task at a time in posting order. A task that throws is
 *  logged by `HostThreading.runLogged` and the loop continues.
 *
 *  The thread starts on `start`; work posted before then waits for it. `shutdown` stops the thread once the work
 *  already queued has run, and anything posted after that is dropped, with a warning the first time. */
class SerialLoop(name: String, daemon: Boolean) extends StoreExecutor with Executor with Logging:
  private val queue = new LinkedBlockingQueue[Runnable]()

  /** Queued by shutdown: the loop ends on taking it. */
  private val Stop: Runnable = () => ()

  private val started = new AtomicBoolean(false)
  @volatile private var stopped = false
  private val droppedWarned = new AtomicBoolean(false)

  private val thread = new Thread(() => loop(), name)
  thread.setDaemon(daemon)

  private def loop(): Unit =
    var task = queue.take()
    while task ne Stop do
      HostThreading.runLogged(task)
      task = queue.take()

  override def execute(task: Runnable): Unit =
    if !stopped then
      queue.put(task)
    else if droppedWarned.compareAndSet(false, true) then
      logger.warn(s"Dropping work posted to thread $name after its shutdown. Later drops are not logged")

  override def reportFailure(cause: Throwable): Unit =
    logger.error(s"Unhandled exception in a callback on thread $name", cause)

  def start(): Unit =
    if !stopped && started.compareAndSet(false, true) then
      thread.start()

  def shutdown(): Unit =
    stopped = true
    queue.put(Stop)
