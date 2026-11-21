package org.aspen_ddp.aspen.server.transaction

/** Decides when a transaction, or its driver, that has been unfinished for long enough is logged as stuck: not before
  * `Tx.StuckReportAfter` since `startNanos`, then at most once per `Tx.StuckReportEvery`. Each owner holds one and
  * calls it from its own thread
  */
private[transaction] class StuckReportThrottle(startNanos: Long):
  private var lastReport: Option[Long] = None

  /** Whether to log now: `stuck` holds and the age and repeat intervals have passed. A true answer counts as a report */
  def due(now: Long, stuck: Boolean): Boolean =
    val isDue = stuck && now - startNanos >= Tx.StuckReportAfter.toNanos &&
      lastReport.forall(last => now - last >= Tx.StuckReportEvery.toNanos)
    if isDue then lastReport = Some(now)
    isDue
