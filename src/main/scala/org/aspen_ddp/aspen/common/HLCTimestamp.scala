package org.aspen_ddp.aspen.common

import scala.concurrent.duration._

/** A hybrid logical clock timestamp: wall time in milliseconds in the high 48 bits, a logical counter in the low 16.
  * The encoding orders exactly as (wallTime, logical), so timestamps compare as their encoded Longs.
  */
final class HLCTimestamp private (private val longValue: Long) extends AnyVal with Ordered[HLCTimestamp] {
  def asLong: Long = longValue
  def wallTime: Long = longValue >> 16
  def logical: Int = (longValue & 0xFFFFL).toInt
  def asDuration: Duration = Duration(wallTime, MILLISECONDS)

  def compare(t: HLCTimestamp): Int = java.lang.Long.compare(longValue, t.longValue)

  def -(rhs: HLCTimestamp): Duration = this.asDuration - rhs.asDuration

  override def toString: String = s"HLCTimestamp($longValue)"
}

object HLCTimestamp {

  val EncodedSize: Int = 8

  private def currentWallTime: Long = System.currentTimeMillis() << 16

  private var lastObserved: HLCTimestamp = HLCTimestamp(currentWallTime)

  def update(seen: HLCTimestamp): Unit = synchronized {
    if (seen > lastObserved)
      lastObserved = seen
  }

  def now: HLCTimestamp = HLCTimestamp()

  val Zero = HLCTimestamp(0)

  def apply(longValue: Long): HLCTimestamp = new HLCTimestamp(longValue)

  def apply(): HLCTimestamp = synchronized { happensAfter(lastObserved) }

  def happensAfter(ts: HLCTimestamp): HLCTimestamp = {
    val n = HLCTimestamp(currentWallTime)

    // Past ts's wall time, the clock's own; else one tick later, a full logical counter carrying into the wall time
    val next = if (n > ts) n else HLCTimestamp(ts.asLong + 1)

    if (!(next > ts)) {
      println(s"INVALID HLC!!!! Old $ts New $next")
    }

    next
  }
}
