package org.aspen_ddp.aspen.server.store.backend

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration.FiniteDuration

/** Runs a task once after a delay: the backends' retry timer (FSIF's ENOSPC retry, RocksDB's transient read retry) */
trait RetryScheduler:
  def schedule(delay: FiniteDuration, task: Runnable): Unit

/** Host-wide retry timer: one daemon thread named backend-retry. Tasks must only enqueue or issue I/O elsewhere. */
object SharedRetryScheduler extends RetryScheduler:
  private lazy val executor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor: r =>
      val t = new Thread(r, "backend-retry")
      t.setDaemon(true)
      t
  def schedule(delay: FiniteDuration, task: Runnable): Unit =
    executor.schedule(task, delay.toNanos, TimeUnit.NANOSECONDS)
