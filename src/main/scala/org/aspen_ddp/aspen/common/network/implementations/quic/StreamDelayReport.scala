package org.aspen_ddp.aspen.common.network.implementations.quic

import java.util.concurrent.TimeUnit

/** Per stream, how long messages waited to be accepted by QUIC and how many bytes were queued, summarized once a period
  * if any wait was long. A message queued behind a backlog on its stream is delayed however small it is, which other
  * streams' messages are not. Used by one thread.
  *
  * @param names         what to call each stream in a summary, by index
  * @param periodNanos   the least time between summaries
  * @param reportAtNanos the longest wait that goes unreported
  */
final class StreamDelayReport(names: IndexedSeq[String], periodNanos: Long, reportAtNanos: Long):
  private val nStreams = names.size
  private val maxDelay = new Array[Long](nStreams)
  private val maxPending = new Array[Long](nStreams)
  private val writes = new Array[Long](nStreams)
  private var periodStart: Long = System.nanoTime()

  /** A message was written to `stream`, which now has `pendingBytes` not yet accepted */
  def written(stream: Int, pendingBytes: Long): Unit =
    writes(stream) += 1
    maxPending(stream) = math.max(maxPending(stream), pendingBytes)

  /** A write finished after `delayNanos`. Returns a summary if one is due. */
  def completed(stream: Int, delayNanos: Long, now: Long): Option[String] =
    maxDelay(stream) = math.max(maxDelay(stream), delayNanos)
    if now - periodStart < periodNanos then
      None
    else
      val text =
        if maxDelay.max > reportAtNanos then
          val ms = TimeUnit.NANOSECONDS.toMillis
          val mib = 1024 * 1024
          val streams = (0 until nStreams).map: s =>
            s"${names(s)}: ${ms(maxDelay(s))}ms, ${maxPending(s) / mib} MiB queued, ${writes(s)} writes"
          Some(s"longest wait for a stream write over ${ms(now - periodStart)}ms. ${streams.mkString("; ")}")
        else
          None
      java.util.Arrays.fill(maxDelay, 0L)
      java.util.Arrays.fill(maxPending, 0L)
      java.util.Arrays.fill(writes, 0L)
      periodStart = now
      text

object StreamDelayReport:
  val PeriodNanos: Long = TimeUnit.SECONDS.toNanos(1)
  val ReportAtNanos: Long = TimeUnit.MILLISECONDS.toNanos(500)
