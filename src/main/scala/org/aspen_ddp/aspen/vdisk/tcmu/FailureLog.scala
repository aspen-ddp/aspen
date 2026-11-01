package org.aspen_ddp.aspen.vdisk.tcmu

import scribe.Level

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Rate-limited failure reporting for one device.
  *
  * The first report of a `kind` is logged at error with its dump. Later reports within `window` of it are only
  * counted. The next report after the window logs a warn line "N more <kind> since <first>" (N counts the
  * suppressed reports and this one, no dump) and starts a new window; if nothing was suppressed it is logged in
  * full like a first report. `log` is the sink, scribe by default.
  */
final class FailureLog(
    device: String,
    window: FiniteDuration = 60.seconds,
    clock: () => Long = System.nanoTime,
    log: (Level, String) => Unit = FailureLog.toScribe):

  private final class Entry(var windowStart: Long, var since: Instant, var suppressed: Long, var total: Long)

  // Guarded by this
  private val entries = mutable.HashMap[String, Entry]()

  private enum Outcome:
    case Full
    case Summary(n: Long, since: Instant)
    case Quiet

  def report(kind: String, message: String, dump: => String): Unit =
    val now = clock()
    val outcome = synchronized:
      entries.get(kind) match
        case None =>
          entries(kind) = Entry(now, Instant.now(), 0, 1)
          Outcome.Full
        case Some(e) =>
          e.total += 1
          if now - e.windowStart < window.toNanos then
            e.suppressed += 1
            Outcome.Quiet
          else
            val n = e.suppressed
            val since = e.since
            e.windowStart = now
            e.since = Instant.now()
            e.suppressed = 0
            if n == 0 then Outcome.Full else Outcome.Summary(n + 1, since)
    outcome match
      case Outcome.Full =>
        val d =
          try dump
          catch case NonFatal(e) => s"(dump failed: $e)"
        log(Level.Error, s"$device: $kind: $message; $d")
      case Outcome.Summary(n, since) =>
        log(Level.Warn, s"$device: $n more $kind since $since; latest: $message")
      case Outcome.Quiet => ()

  /** A warning through the same sink, not rate limited */
  def warn(message: String): Unit = log(Level.Warn, s"$device: $message")

  /** Total reports per kind */
  def counts: Map[String, Long] = synchronized(entries.view.mapValues(_.total).toMap)

object FailureLog:
  val toScribe: (Level, String) => Unit = (level, message) =>
    if level == Level.Error then scribe.error(message)
    else if level == Level.Warn then scribe.warn(message)
    else if level == Level.Info then scribe.info(message)
    else scribe.debug(message)
