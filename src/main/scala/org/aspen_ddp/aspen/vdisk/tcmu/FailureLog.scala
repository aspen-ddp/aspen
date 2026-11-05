package org.aspen_ddp.aspen.vdisk.tcmu

import scribe.Level

import java.io.{PrintWriter, StringWriter}
import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Rate-limited failure reporting for one device.
  *
  * The first report of a `kind` is logged in full (at `level`, error by default) with its dump and, when given, the
  * stack trace of its `cause`. Later reports within `window` of it are only counted. The next report after the
  * window logs a warn line "N more <kind> since <first>" (N counts the suppressed reports and this one, no dump) and
  * starts a new window; if nothing was suppressed it is logged in full like a first report.
  *
  * A burst that stops is not lost: any report, of any kind, first logs "N more <kind> since <first>" for every other
  * kind whose window has passed with reports suppressed, and `flush` does so for every kind regardless of its
  * window. Either way the next report of that kind is logged in full. `log` is the sink, scribe by default.
  */
final class FailureLog(
    device: String,
    window: FiniteDuration = 60.seconds,
    clock: () => Long = System.nanoTime,
    log: (Level, String) => Unit = FailureLog.toScribe):

  private final class Entry(var windowStart: Long, var since: Instant, var suppressed: Long, var total: Long,
                            var latest: String)

  // Guarded by this
  private val entries = mutable.HashMap[String, Entry]()

  private enum Outcome:
    case Full
    case Summary(n: Long, since: Instant)
    case Quiet

  def report(kind: String, message: String, dump: => String, cause: Option[Throwable] = None,
             level: Level = Level.Error): Unit =
    val now = clock()
    val (outcome, flushed) = synchronized:
      val flushed = pending(e => now - e.windowStart >= window.toNanos, except = Some(kind))
      val outcome = entries.get(kind) match
        case None =>
          entries(kind) = Entry(now, Instant.now(), 0, 1, message)
          Outcome.Full
        case Some(e) =>
          e.total += 1
          e.latest = message
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
      (outcome, flushed)
    flushed.foreach(log(Level.Warn, _))
    outcome match
      case Outcome.Full =>
        val d =
          try dump
          catch case NonFatal(e) => s"(dump failed: $e)"
        log(level, s"$device: $kind: $message; $d${cause.map(FailureLog.stackTrace).getOrElse("")}")
      case Outcome.Summary(n, since) =>
        log(Level.Warn, s"$device: $n more $kind since $since; latest: $message")
      case Outcome.Quiet => ()

  /** Logs the count of every kind with suppressed reports, whatever its window. */
  def flush(): Unit =
    val lines = synchronized(pending(_ => true, except = None))
    lines.foreach(log(Level.Warn, _))

  /** The summary lines of the entries `due` that have suppressed reports, which are then cleared so that the next
    * report of each is logged in full. Caller holds the lock.
    */
  private def pending(due: Entry => Boolean, except: Option[String]): List[String] =
    entries.iterator.collect:
      case (kind, e) if !except.contains(kind) && e.suppressed > 0 && due(e) =>
        val line = s"$device: ${e.suppressed} more $kind since ${e.since}; latest: ${e.latest}"
        e.suppressed = 0
        // Expire the window: the next report of this kind is logged in full
        e.windowStart = clock() - window.toNanos
        line
    .toList

  /** A warning through the same sink, not rate limited */
  def warn(message: String): Unit = log(Level.Warn, s"$device: $message")

  /** Total reports per kind */
  def counts: Map[String, Long] = synchronized(entries.view.mapValues(_.total).toMap)

object FailureLog:
  val toScribe: (Level, String) => Unit = (level, message) =>
    level match
      case Level.Error => scribe.error(message)
      case Level.Warn => scribe.warn(message)
      case Level.Info => scribe.info(message)
      case _ => scribe.debug(message)

  private def stackTrace(e: Throwable): String =
    val w = StringWriter()
    e.printStackTrace(PrintWriter(w))
    s"\n$w"
