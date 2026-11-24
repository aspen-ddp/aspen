package org.aspen_ddp.aspen.server.host

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** A device's health probe (device hang detection, spec section 5.1): while armed, one daemon thread of its own that
 *  overwrites `<device>/health-probe` in place and forces it, `cfg.interval` after the previous attempt completed, so
 *  attempts never overlap and no other work queues ahead of one. It publishes the attempt in flight and the last
 *  completed one, both thread-safe and non-blocking to read, and records every attempt in the device's `observer` as
 *  kind `HealthProbe.Kind`.
 *
 *  One per device path (its `DeviceIo`'s): armed when the device's load completes, disarmed when it unloads.
 *  Disarming only stops new attempts: an attempt stuck in its write stays in flight, and a rearm meanwhile inherits it
 *  on the same thread, so a reloaded device is judged Hung at once and no second thread starts. `gate` runs before
 *  each write (the DeviceIo's fault-injection seam), so a gated or held path stalls the probe too. `clock` stamps the
 *  attempts (ages, durations); the interval is waited in real time. */
final class HealthProbe(devicePath: os.Path, cfg: HealthProbe.Config, observer: DeviceIoObserver,
                        threadName: () => String, gate: () => Unit = () => (),
                        write: os.Path => Unit = HealthProbe.writeAndForce,
                        clock: () => Long = () => System.nanoTime()):
  private val lock = new Object
  // Written under `lock`; volatile so `armed` reads without it
  @volatile private var armedFlag = false
  // The probe's thread, None once it has decided to exit (under `lock`), so a later arm starts a new one
  private var thread: Option[Thread] = None
  @volatile private var inFlightStart: Option[Long] = None
  @volatile private var last: Option[HealthProbe.Result] = None

  def config: HealthProbe.Config = cfg

  /** Starts the probe's thread unless one is alive, whose first attempt runs at once. A thread still alive (one stuck
   *  in an attempt, or one waiting out its interval) carries on. */
  def arm(): Unit = lock.synchronized:
    armedFlag = true
    if !thread.exists(_.isAlive) then
      val t = new Thread(() => loop(), threadName())
      t.setDaemon(true)
      thread = Some(t)
      t.start()

  /** No new attempt starts; a thread waiting out its interval wakes and exits. An attempt in flight stays in flight
   *  until its write returns, and its thread then exits. Never blocks. */
  def disarm(): Unit = lock.synchronized:
    armedFlag = false
    lock.notifyAll()

  def armed: Boolean = armedFlag

  /** How long the attempt in flight has run, by the probe's clock */
  def inFlightAge: Option[FiniteDuration] = inFlightStart.map(s => (clock() - s).nanos)

  def lastResult: Option[HealthProbe.Result] = last

  /** The attempt in flight is older than `hangThreshold` */
  def hung: Boolean = inFlightAge.exists(_ > cfg.hangThreshold)

  private[host] def threadAlive: Boolean = lock.synchronized(thread.exists(_.isAlive))

  private def loop(): Unit =
    try
      while continueRunning() do
        attempt()
        awaitInterval()
    finally
      // A thread ended by a fatal throwable: a later arm starts a new one
      lock.synchronized:
        if thread.contains(Thread.currentThread()) then thread = None

  /** Whether to run another attempt; when not, this thread is no longer the probe's (under the lock, so an arm
   *  racing the exit starts a new thread rather than counting on this one) */
  private def continueRunning(): Boolean = lock.synchronized:
    if !armedFlag then thread = None
    armedFlag

  private def awaitInterval(): Unit = lock.synchronized:
    val end = System.nanoTime() + cfg.interval.toNanos
    var remaining = cfg.interval.toNanos
    while armedFlag && remaining > 0 do
      TimeUnit.NANOSECONDS.timedWait(lock, remaining)
      remaining = end - System.nanoTime()

  private def attempt(): Unit =
    val start = clock()
    inFlightStart = Some(start)
    val token = observer.started(HealthProbe.Kind, None, HealthProbe.Size)
    var error: Option[Throwable] = None
    var completed = false
    try
      try
        gate()
        write(devicePath / HealthProbe.FileName)
      catch case NonFatal(t) => error = Some(t)
      completed = true
    finally
      observer.finished(token, completed && error.isEmpty)
      if completed then
        val end = clock()
        last = Some(HealthProbe.Result(end, (end - start).nanos, error))
      inFlightStart = None

object HealthProbe:
  val FileName = "health-probe"
  val Size = 4096
  /** The observer kind of a probe attempt; not `JobKind.DeviceProbe`, discovery's probe of a path */
  val Kind = "HealthProbe"

  final case class Config(interval: FiniteDuration = 90.seconds, hangThreshold: FiniteDuration = 120.seconds)

  /** A completed attempt: when it ended (probe clock), how long it took, and its error if it failed */
  final case class Result(endNanos: Long, duration: FiniteDuration, error: Option[Throwable])

  /** Writes `Size` bytes at the start of `file` and forces them (`force(false)`): the first write creates and fills
   *  the file, every later one overwrites the same blocks in place, so a full file system does not fail it. java.nio,
   *  as os-lib has no force */
  def writeAndForce(file: os.Path): Unit =
    val ch = FileChannel.open(file.toNIO, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    try
      val buf = ByteBuffer.allocate(Size)
      var pos = 0L
      while buf.hasRemaining do pos += ch.write(buf, pos)
      ch.force(false)
    finally ch.close()
