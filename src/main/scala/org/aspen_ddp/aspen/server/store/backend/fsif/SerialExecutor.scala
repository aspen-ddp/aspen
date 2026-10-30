package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/** Runs submitted tasks one at a time, in submission order, on `underlying`. Lock-free and without a thread of its
  * own: the submitter that takes the pending count from zero schedules a drain, and the drain runs tasks until the
  * count returns to zero. A task that throws is reported through `underlying.reportFailure` and later tasks still run; a fatal Throwable
  * propagates to `underlying`'s thread, and later tasks run on a fresh drain.
  */
final class SerialExecutor(underlying: ExecutionContext) extends ExecutionContext:
  private val queue = new ConcurrentLinkedQueue[Runnable]()
  // Tasks submitted and not yet run. A task is queued before it is counted, so a counted task is always pollable.
  private val pending = new AtomicInteger(0)

  private object Drain extends Runnable:
    def run(): Unit =
      var budget = SerialExecutor.Budget
      var more = true
      while more do
        val task = queue.poll()
        var finished = false
        try
          if task != null then
            try task.run()
            catch case NonFatal(t) => underlying.reportFailure(t)
          finished = true
        finally
          // Also when a fatal Throwable (InterruptedException, a ControlThrowable, ...) escapes the task: the count
          // is still decremented and a fresh drain runs the rest, so the executor never wedges
          if pending.decrementAndGet() == 0 then
            more = false
          else if !finished then
            more = false
            underlying.execute(Drain)
          else
            budget -= 1
            if budget == 0 then
              // Yield the underlying thread so one busy executor can't starve a shared pool
              more = false
              underlying.execute(Drain)

  def execute(runnable: Runnable): Unit =
    queue.add(runnable)
    if pending.getAndIncrement() == 0 then
      underlying.execute(Drain)

  def reportFailure(cause: Throwable): Unit = underlying.reportFailure(cause)

object SerialExecutor:
  private val Budget = 64
