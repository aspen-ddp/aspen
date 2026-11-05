package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.transaction.{RequirementError, TransactionId}
import scribe.Logging

import scala.collection.mutable

/** Totals of the abort votes a host's stores give, by the requirement that failed, handed to `emit` about once per
  * period while aborts happen. Abort reasons are otherwise trace only, and a busy store gives far too many to log each.
  *
  * The report comes from whichever thread records the first vote past the end of the period, as in `BackendTiming`.
  */
final class AbortVoteReport(periodNanos: Long, emit: String => Unit):
  private final class Totals:
    var errors = 0L
    val transactions = mutable.HashSet[TransactionId]()
    val objects = mutable.HashMap[ObjectId, Long]()

  private val reasons = mutable.TreeMap[String, Totals]()
  private var votes = 0L
  private var periodStart = Long.MinValue

  /** One abort vote, with each failed requirement and the object it is on (None for a non-object requirement) */
  def record(txid: TransactionId, errors: List[(Option[ObjectId], RequirementError)],
             now: Long = System.nanoTime()): Unit =
    val text = synchronized:
      if periodStart == Long.MinValue then
        periodStart = now
      votes += 1
      errors.foreach: (obj, err) =>
        val t = reasons.getOrElseUpdate(err.getClass.getSimpleName, new Totals)
        t.errors += 1
        t.transactions += txid
        obj.foreach(o => t.objects(o) = t.objects.getOrElse(o, 0L) + 1)
      if now - periodStart >= periodNanos then Some(take(now)) else None
    text.foreach(emit)

  private def take(now: Long): String =
    val lines = reasons.toList.map: (reason, t) =>
      val top = t.objects.toList.sortBy((o, n) => (-n, o.toString)).take(3)
        .map((o, n) => s"${o.uuid}=$n").mkString(" ")
      val objects = if t.objects.isEmpty then "" else s" on ${t.objects.size} objects (most: $top)"
      s"$reason: ${t.errors} from ${t.transactions.size} transactions$objects"
    val text = s"Abort votes over ${(now - periodStart) / 1000000}ms: $votes. ${lines.mkString("; ")}"
    reasons.clear()
    votes = 0
    periodStart = now
    text

object AbortVoteReport extends Logging:
  /** One per process, so that all of a host's stores are summarized together */
  val report: AbortVoteReport = new AbortVoteReport(1000000000L, s => logger.info(s))
