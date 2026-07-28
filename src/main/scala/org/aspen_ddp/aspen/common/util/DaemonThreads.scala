package org.aspen_ddp.aspen.common.util

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

/**
 * Helpers for creating named daemon threads.
 *
 * Aspen uses a crash-only architecture; there is no clean shutdown process. Consequently
 * every thread the system spawns must be a daemon thread so that a process which has
 * finished its work simply exits rather than lingering until it is killed. Processes that
 * are meant to run indefinitely, such as the storage host and the NFS server, keep
 * themselves alive by blocking the main thread rather than by relying on a non-daemon
 * worker.
 *
 * The names are purely for debuggability. They show up in thread dumps and make it obvious
 * which subsystem a stuck thread belongs to.
 */
object DaemonThreads:

  /** Creates a named daemon thread that will run `body`. The thread is not started. */
  def thread(name: String)(body: => Unit): Thread =
    val t = new Thread(() => body, name)
    t.setDaemon(true)
    t

  /** Creates a ThreadFactory that produces daemon threads named "namePrefix-N". */
  def factory(namePrefix: String): ThreadFactory =
    val counter = new AtomicInteger(0)

    (r: Runnable) =>
      val t = new Thread(r, s"$namePrefix-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t
