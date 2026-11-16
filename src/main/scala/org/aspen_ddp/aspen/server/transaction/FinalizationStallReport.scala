package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

import scala.concurrent.duration.{FiniteDuration, SECONDS}

/** A committed transaction whose driver has not finalized it `TransactionDriver.FinalizationStallAfter` or longer after
  * resolving it (TransactionDriver.finalizationStall)
  *
  * @param age         time since the driver resolved it committed
  * @param failures    the driver's failed finalizers so far
  * @param lastFailure the last failed finalizer's error
  */
final case class FinalizationStall(transactionId: TransactionId, age: FiniteDuration, failures: Int,
                                   lastFailure: Option[Throwable])

object FinalizationStallReport:
  /** The shortest interval between two reports of one store */
  val Every: FiniteDuration = FiniteDuration(60, SECONDS)

  /** The most stalled transactions a report names besides the oldest */
  val MaxListed = 10

/** The finalization stall alarm of one store (Todo 4.9; transaction-recovery R-N2, F14; repair-process 13.0b).
  * Finalization stalls by design while an action fails on every attempt or the marker cannot write: the driver retries
  * with backoff and never finalizes without its actions. The store passes its drivers' stalls at every heartbeat, and
  * this returns a `FINALIZATION STALLED` line for it to log at error at most once per `Every`, while stalls exist. A
  * heartbeat with no stall resets it, so the next stall is reported at once. Owned by the store's executor
  */
final class FinalizationStallReport(storeId: StoreId, periodNanos: Long = FinalizationStallReport.Every.toNanos):
  private var lastReport: Option[Long] = None

  def report(stalls: List[FinalizationStall], now: Long): Option[String] =
    if stalls.isEmpty then
      lastReport = None
      None
    else if lastReport.exists(last => now - last < periodNanos) then
      None
    else
      lastReport = Some(now)
      val sorted = stalls.sortBy(_.age).reverse
      val oldest = sorted.head
      val others = sorted.tail
      val listed = others.take(FinalizationStallReport.MaxListed).map(_.transactionId).mkString(", ")
      val more = others.size - FinalizationStallReport.MaxListed
      val othersText =
        if others.isEmpty then ""
        else s"; others: $listed" + (if more > 0 then s" and $more more" else "")
      Some(s"FINALIZATION STALLED: store $storeId drives ${stalls.size} committed transactions unfinalized for over " +
        s"${TransactionDriver.FinalizationStallAfter.toSeconds}s; oldest ${oldest.transactionId} " +
        s"${oldest.age.toSeconds}s, ${oldest.failures} finalizer failures, last: " +
        s"${oldest.lastFailure.fold("none")(_.toString)}$othersText")
