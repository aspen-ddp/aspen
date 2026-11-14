package org.aspen_ddp.aspen.common.util

/** Lets a recurring log line through at most once per `periodMillis` of `nowMillis`, counting the ones held back in
  * between, so that an overload cannot flood the log with it. The first passes at once. A clock stepped backward lets
  * the next one through. Thread-safe.
  */
final class LogRateLimit(periodMillis: Long, nowMillis: () => Long = () => System.currentTimeMillis()):
  private var lastMillis: Option[Long] = None
  private var held: Long = 0

  /** Some(the number held back since the last one let through) if this one is to be logged, otherwise None */
  def admit(): Option[Long] = synchronized:
    val now = nowMillis()
    if lastMillis.forall(last => now - last >= periodMillis || now < last) then
      lastMillis = Some(now)
      val n = held
      held = 0
      Some(n)
    else
      held += 1
      None

object LogRateLimit:
  /** For appending to a line let through: how many were held back since the last, if any */
  def heldText(held: Long): String =
    if held == 0 then "" else s" ($held more like it since the last report)"
