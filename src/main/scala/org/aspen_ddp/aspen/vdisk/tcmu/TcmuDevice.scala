package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.ring.{Iovec, Ring, RingCorrupt, RingItem}
import org.aspen_ddp.aspen.vdisk.tcmu.scsi.{Cdb, ScsiHandler, ScsiResult, Sense}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{LockSupport, ReentrantReadWriteLock}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Serves one TCMU device: a platform thread named `tcmu-<name>` waits on `channel`, takes every new entry off
  * `ring` and dispatches it. Commands run through `commands`, and each one is completed exactly once, on the
  * thread that completes its Future (`completionContext`).
  *
  * Ring access from completions is guarded by a read-write lock. Completions and data-out gathers take the read
  * lock, so they still run concurrently with each other. A ring reset and `stop` take the write lock, so:
  *  - a completion for a command consumed before a ring reset never touches the ring after it (its data area may
  *    belong to a retried command by then); it is dropped and logged at warn, and the kernel retries the command;
  *  - once `stop` returns, nothing touches the ring segment, so the caller may unmap it.
  *
  * `stop` closes `channel`. It must not be called from a completion callback, which holds the read lock.
  *
  * When `channel.await` fails (say, the UIO device went away), the loop waits `park(nanos)` before trying again,
  * doubling from 1 ms to at most 1 s and resetting on the next successful await. `stop` unparks it.
  */
final class TcmuDevice(
    name: String,
    ring: Ring,
    channel: KernelChannel,
    control: RingControl,
    commands: ScsiHandler,
    failures: FailureLog,
    completionContext: ExecutionContext = ExecutionContext.parasitic,
    park: Long => Unit = nanos => LockSupport.parkNanos(nanos)):

  import TcmuDevice.*

  private val lock = ReentrantReadWriteLock()
  /** Written under the write lock, read under either lock. */
  @volatile private var stopped = false
  /** Ring resets so far. Written by the loop thread under the write lock. */
  @volatile private var generation = 0L
  /** A failed resync is retried on the next notification. Loop thread only. */
  private var resyncPending = false
  @volatile private var running = false
  private var thread: Option[Thread] = None
  private val processed = AtomicLong()

  def processedCount: Long = processed.get

  def start(): Unit = synchronized:
    require(thread.isEmpty, s"$name: already started")
    running = true
    thread = Some(Thread.ofPlatform().name(s"tcmu-$name").daemon(true).start(() => loop()))

  /** Stops the loop, waits for its thread, then marks the device stopped: later completions are dropped. */
  def stop(): Unit =
    val t = synchronized:
      running = false
      thread
    channel.close()
    t.foreach: th =>
      LockSupport.unpark(th)
      if th ne Thread.currentThread() then th.join()
    locked(lock.writeLock):
      stopped = true

  private def locked[T](l: java.util.concurrent.locks.Lock)(body: => T): T =
    l.lock()
    try body
    finally l.unlock()

  private def fatal(e: Throwable): Boolean = e.isInstanceOf[VirtualMachineError]

  /** The ring state for a report, without touching the segment once the device has stopped. */
  private def mailbox: String = locked(lock.readLock):
    if stopped then "(device stopped)" else ring.mailboxDump

  private def loop(): Unit =
    var go = true
    var backoff = 0L
    while go && running do
      val notified =
        try
          channel.await()
          backoff = 0L
          true
        catch
          case e: InterruptedException =>
            if running then scribe.warn(s"$name: serving loop interrupted while running: $e")
            go = false
            false
          case e: Throwable if !fatal(e) =>
            failures.report("channel-error", e.toString, mailbox)
            backoff = if backoff == 0L then MinBackoff else math.min(backoff * 2, MaxBackoff)
            if running then park(backoff)
            false
      if notified && running then
        try serve()
        catch case e: Throwable if !fatal(e) => failures.report("loop-error", e.toString, mailbox)

  private def serve(): Unit =
    if !resyncPending || resync() then
      val items =
        try ring.nextItems()
        catch
          case RingCorrupt(reason, dump) =>
            failures.report("ring-corrupt", reason, dump)
            resync()
            Nil
      items.foreach: item =>
        processed.incrementAndGet()
        try dispatch(item)
        catch
          case e: Throwable if !fatal(e) =>
            // A Command is handed to its Future before anything that can throw, so only a Malformed one can
            // still be uncompleted here.
            failures.report("dispatch-error", s"$item: $e", mailbox)
            item match
              case RingItem.Malformed(cmdId, _, _, true) =>
                finish(cmdId, generation, Sense.HardwareError, Array.emptyByteArray, Vector.empty)
              case _ => ()

  /** Resets the ring through `control` and resyncs, both under the ring's completion lock and this device's write
    * lock. Returns false, to be retried on the next notification, if either fails.
    */
  private def resync(): Boolean =
    val result = locked(lock.writeLock):
      generation += 1
      try ring.resync(() => control.resetRing())
      catch case e: Throwable if !fatal(e) => Left(s"ring reset failed: $e")
    result match
      case Right(()) =>
        resyncPending = false
        scribe.warn(s"$name: ring reset; the kernel retries outstanding commands")
        true
      case Left(reason) =>
        resyncPending = true
        failures.report("resync-failed", reason, mailbox)
        false

  private def dispatch(item: RingItem): Unit =
    item match
      case c: RingItem.Command => command(c)
      case RingItem.Malformed(cmdId, reason, dump, completable) =>
        failures.report("malformed-entry", reason, dump)
        if completable then
          finish(cmdId, generation, Sense.HardwareError, Array.emptyByteArray, Vector.empty)
      case RingItem.Tmr(tmrType, cmdIds) =>
        scribe.info(s"$name: TMR type $tmrType for cmd_ids ${cmdIds.mkString(",")}; no action taken")

  private def command(c: RingItem.Command): Unit =
    val gen = generation
    val start = System.nanoTime()
    val capacity = ring.dataLength(c.iovecs)
    val result =
      try commands.handle(c.cdb, () => gather(c.iovecs, capacity, gen), capacity)
      catch case e: Throwable if !fatal(e) => Future.failed(e)
    result.onComplete(r => completed(c, gen, start, capacity, r))(using completionContext)

  private def gather(iovecs: Vector[Iovec], capacity: Long, gen: Long): Array[Byte] = locked(lock.readLock):
    if stopped then throw IllegalStateException(s"$name is stopped")
    if gen != generation then throw IllegalStateException(s"$name: the ring was reset since the command was consumed")
    ring.gather(iovecs, math.min(capacity, Int.MaxValue.toLong).toInt)

  private def cdbHex(cdb: Array[Byte]): String = cdb.map(b => f"${b & 0xff}%02x").mkString

  private def describe(c: RingItem.Command): String =
    s"cmd_id=${c.cmdId} cdb=${cdbHex(c.cdb)} iovecs=${c.iovecs.map(v => s"(${v.offset},${v.length})")
      .mkString("[", ",", "]")}"

  private def completed(c: RingItem.Command, gen: Long, start: Long, capacity: Long,
                        r: Try[(ScsiResult, Array[Byte])]): Unit =
    try
      val (result, data) = r match
        case Success((_, data)) if data.length > capacity =>
          failures.report("data-overflow", s"${describe(c)} returned ${data.length} bytes for $capacity bytes " +
            "of iovecs", mailbox)
          (Sense.HardwareError, Array.emptyByteArray)
        case Success(rd) => rd
        case Failure(e) =>
          failures.report("command-error", s"${describe(c)} failed: $e", mailbox)
          (Sense.HardwareError, Array.emptyByteArray)
      finish(c.cmdId, gen, result, data, c.iovecs)
      scribe.debug(trace(c, start, result))
    catch case e: Throwable if !fatal(e) => failures.report("completion-error", s"${describe(c)}: $e", mailbox)

  private def trace(c: RingItem.Command, start: Long, r: ScsiResult): String =
    val op = c.cdb(0) & 0xff
    val lbaLen = Try(s" lba=${Cdb.lba(c.cdb)} len=${Cdb.transferLength(c.cdb)}").getOrElse("")
    val fua = Try(Cdb.fua(c.cdb)).getOrElse(false)
    val key = if r.sense.length > 2 then r.sense(2) & 0x0f else 0
    f"$name: op=0x$op%02x$lbaLen fua=$fua cmd_id=${c.cmdId} latency=${(System.nanoTime() - start) / 1000}us " +
      f"status=0x${r.status}%02x sense_key=0x$key%x"

  /** Writes the response, unless the device stopped or the ring was reset since the command was consumed. */
  private def finish(cmdId: Int, gen: Long, result: ScsiResult, data: Array[Byte], iovecs: Vector[Iovec]): Unit =
    locked(lock.readLock):
      if stopped then
        failures.warn(s"cmd_id=$cmdId completed after stop; dropped")
      else if gen != generation then
        failures.warn(s"cmd_id=$cmdId completed after a ring reset; dropped, the kernel retries it")
      else
        val ok =
          try
            if data.nonEmpty then ring.scatter(iovecs, data): Unit
            ring.complete(cmdId, result.status, result.sense, result.readLen)
          catch
            case e: Throwable if !fatal(e) =>
              failures.report("completion-error", s"cmd_id=$cmdId: $e", mailbox)
              val h = Sense.HardwareError
              ring.complete(cmdId, h.status, h.sense, None)
        if ok then
          try channel.kick()
          catch case e: Throwable if !fatal(e) => failures.report("kick-error", s"cmd_id=$cmdId: $e", mailbox)
        else
          failures.report("stale-completion", s"cmd_id=$cmdId is not outstanding", mailbox)

object TcmuDevice:
  private val MinBackoff = 1.millis.toNanos
  private val MaxBackoff = 1.second.toNanos
