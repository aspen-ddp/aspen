package org.aspen_ddp.aspen.server.store.backend

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration.*

/** Runs a task once after a delay: the backends' retry timer (FSIF's ENOSPC retry, RocksDB's transient read retry) */
trait RetryScheduler:
  def schedule(delay: FiniteDuration, task: Runnable): Unit

object RetryScheduler:
  /** Backoff before each retry of a read failure that is not evidence of damage, in both backends: about 1.1 s in
    * all, enough to ride out a start-up burst of EMFILE, ENOMEM or a transient EIO while every store of a host
    * recovers at once. Then the read reports `CorruptedObject`, on which a recovered commit vote fail-stops its store
    * (systemic-loss D7).
    */
  val ReadRetryDelays: Seq[FiniteDuration] = Seq(10.millis, 100.millis, 1.second)

/** Host-wide retry timer: one daemon thread named backend-retry. Tasks must only enqueue or issue I/O elsewhere. */
object SharedRetryScheduler extends RetryScheduler:
  private lazy val executor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor: r =>
      val t = new Thread(r, "backend-retry")
      t.setDaemon(true)
      t
  def schedule(delay: FiniteDuration, task: Runnable): Unit =
    executor.schedule(task, delay.toNanos, TimeUnit.NANOSECONDS)
