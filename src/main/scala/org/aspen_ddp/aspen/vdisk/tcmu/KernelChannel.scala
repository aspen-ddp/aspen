package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.sys.{Libc, Linux}

import java.lang.foreign.Arena

/** Notifications between a TCMU device's ring and the kernel. */
trait KernelChannel:
  /** Blocks until the kernel signals new ring entries. Throws InterruptedException once `close` has been called. */
  def await(): Unit

  /** Tells the kernel that completions are waiting on the ring. A no-op after `close`. */
  def kick(): Unit

  /** Wakes a blocked `await` and releases the channel. Idempotent. */
  def close(): Unit

/** Kernel-side ring recovery, through configfs `action/reset_ring`. */
trait RingControl:
  def resetRing(): Unit

/** The UIO fd of a TCMU device: `read` of 4 bytes waits for a kernel notification, `write` of 4 bytes kicks.
  *
  * Closing an fd does not wake a thread blocked reading it, so `await` polls the UIO fd together with a wake-up
  * pipe that `close` writes to. The fds are released by whichever of `close` and a running `await` finishes last,
  * so no call ever uses an fd number that has been closed and possibly reused.
  */
final class UioChannel(fd: Int) extends KernelChannel:
  private val (wakeRead, wakeWrite) = Libc.pipe()
  // Guarded by this
  private var closed = false
  private var awaiting = false
  private var released = false

  private def closedError = InterruptedException("UIO channel closed")

  private def withBuffer[T](size: Int)(body: java.lang.foreign.MemorySegment => T): T =
    val arena = Arena.ofConfined()
    try body(arena.allocate(size.toLong))
    finally arena.close()

  /** Caller holds the lock. */
  private def release(): Unit =
    if !released then
      released = true
      for f <- List(fd, wakeRead, wakeWrite) do
        try Libc.close(f)
        catch case e: Exception => scribe.warn(s"UIO channel: close of fd $f failed: $e")

  def await(): Unit =
    synchronized:
      if closed then throw closedError
      awaiting = true
    try
      val revents = Libc.poll(Seq(fd -> Linux.POLLIN, wakeRead -> Linux.POLLIN), -1)
      if revents(1) == 0 && revents(0) != 0 then
        withBuffer(4)(Libc.read(fd, _)): Unit
    finally
      synchronized:
        awaiting = false
        if closed then release()
    if synchronized(closed) then throw closedError

  def kick(): Unit = synchronized:
    if !closed then
      withBuffer(4)(Libc.write(fd, _)): Unit

  def close(): Unit = synchronized:
    if !closed then
      closed = true
      try withBuffer(1)(Libc.write(wakeWrite, _)): Unit
      catch case e: Exception => scribe.warn(s"UIO channel: wake-up write failed: $e")
      if !awaiting then release()
