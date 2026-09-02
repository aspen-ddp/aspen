package org.aspen_ddp.aspen.server.repair

import scala.concurrent.duration.Duration
import scala.util.Random

/** Per-store pacing state.
  *
  * @param nextDue         monotonic-clock milliseconds; the store is scanned once the sweep's
  *                        clock reaches this
  * @param currentInterval the un-jittered interval that produced nextDue
  */
case class ScanState(nextDue: Long, currentInterval: Duration)

object ScanState:

  private val JitterFraction = 0.25

  /** Spreads a due time by up to plus or minus 25%, so a host that restarts with a thousand
    * stores does not make them all due on the same instant forever after.
    */
  private[repair] def jitter(millis: Long, rand: Random): Long =
    val span = (millis * JitterFraction).toLong
    if span <= 0 then millis
    else millis - span + rand.nextLong(2 * span + 1)

  /** A newly admitted store starts at the floor, due at a random offset inside the first floor
    * interval -- eager, because a store that just came online is the likeliest to have missed
    * updates, but spread, so admission of many stores at once does not stampede.
    */
  def admit(now: Long, policy: RepairPolicy, rand: Random): ScanState =
    val floorMs = math.max(1L, policy.scanIntervalFloor.toMillis)
    ScanState(now + rand.nextLong(floorMs), policy.scanIntervalFloor)

  /** Advances after a completed scan. A scan that found entries snaps back to the floor; a clean
    * scan doubles toward the cap. A failed scan passes foundEntries = false: a failed read says
    * nothing about whether the store is damaged, and retrying hard against a pool that cannot be
    * read is exactly the load this service exists to bound.
    */
  def advance(prev: ScanState, now: Long, foundEntries: Boolean,
              policy: RepairPolicy, rand: Random): ScanState =
    val next =
      if foundEntries then
        policy.scanIntervalFloor
      else
        val doubled = prev.currentInterval * 2
        if doubled < policy.scanIntervalFloor then policy.scanIntervalFloor
        else if doubled > policy.scanIntervalCap then policy.scanIntervalCap
        else doubled

    ScanState(now + jitter(next.toMillis, rand), next)
