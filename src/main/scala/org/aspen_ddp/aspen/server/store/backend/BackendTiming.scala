package org.aspen_ddp.aspen.server.store.backend

import scribe.Logging

import scala.collection.mutable

/** Totals of how long each stage of a write takes, handed to `emit` about once per period while writes happen.
  *
  * The report comes from whichever thread records the first time past the end of the period, so an idle host reports
  * nothing and needs no scheduling. A stage's size is whatever it says it counts, such as a batch's bytes.
  */
final class BackendTiming(periodNanos: Long, emit: String => Unit):
  private final class Totals:
    var count = 0L
    var nanos = 0L
    var maxNanos = 0L
    var size = 0L

  private val stages = mutable.TreeMap[String, Totals]()
  private var periodStart = Long.MinValue

  def record(stage: String, nanos: Long, size: Long = 0, now: Long = System.nanoTime()): Unit =
    val text = synchronized:
      if periodStart == Long.MinValue then
        periodStart = now
      val t = stages.getOrElseUpdate(stage, new Totals)
      t.count += 1
      t.nanos += nanos
      t.maxNanos = math.max(t.maxNanos, nanos)
      t.size += size
      if now - periodStart >= periodNanos then Some(take(now)) else None
    text.foreach(emit)

  private def take(now: Long): String =
    def millis(nanos: Double): String = f"${nanos / 1e6}%.1fms"
    val lines = stages.toList.map: (stage, t) =>
      val size = if t.size > 0 then s", avg size ${t.size / t.count}" else ""
      s"$stage: ${t.count} avg ${millis(t.nanos.toDouble / t.count)} max ${millis(t.maxNanos.toDouble)}$size"
    val text = s"Backend timing over ${((now - periodStart) / 1000000)}ms. ${lines.mkString("; ")}"
    stages.clear()
    periodStart = now
    text

object BackendTiming extends Logging:
  /** One per process: a host's stores share its disks and thread pools, so their totals belong together */
  val report: BackendTiming = new BackendTiming(1000000000L, s => logger.info(s))
