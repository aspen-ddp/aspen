package org.aspen_ddp.aspen.common.transaction

import scala.concurrent.duration.*

/** The transaction horizon: past it, no store joins a transaction it does not already hold, and the client stops
  * retransmitting it. Measured on the transaction's `startTimestamp` against the wall clock of the host (stores) or the
  * client (client give-up)
  */
object TransactionHorizon:

  val Horizon: FiniteDuration = 20.seconds

  /** How far ahead of its host's clock a transaction may start for a store to join it. Also bounds how far timestamps
    * that clients ratchet their HLCs from can run ahead of the hosts' clocks
    */
  val MaxStartLead: FiniteDuration = 2.seconds

  /** How long after its start a client keeps retransmitting a transaction: the horizon plus the lead a store tolerates,
    * so a store whose clock runs that far behind the client's still joins the last retransmit
    */
  def clientGiveUp(horizon: FiniteDuration): FiniteDuration = horizon + MaxStartLead

  /** How long a host remembers a transaction's outcome. It must outlast the horizon, the start lead and the idle
    * transaction's self-drive delay (the heartbeat timeout plus one heartbeat), so that a Tx first created near the
    * horizon still finds its peers' cache entries when its idle driver starts
    */
  def statusCacheDuration(horizon: FiniteDuration, heartbeatPeriod: Duration): FiniteDuration =
    horizon + MaxStartLead + (8 * heartbeatPeriod.toMillis).millis
