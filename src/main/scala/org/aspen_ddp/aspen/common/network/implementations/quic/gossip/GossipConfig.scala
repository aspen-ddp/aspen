package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import scala.concurrent.duration.*

/** Scalecube timings for host membership. The defaults are scalecube's own production defaults.
  *
  * The suspicion timeout, after which an unresponsive member is declared dead, is
  * `suspicionMult * ceil(log2(clusterSize)) * pingInterval`.
  */
final case class GossipConfig(pingInterval: Duration = 1000.millis,
                              pingTimeout: Duration = 500.millis,
                              syncInterval: Duration = 3000.millis,
                              gossipInterval: Duration = 200.millis,
                              suspicionMult: Int = 5)

object GossipConfig:
  /** Short timings for in-process tests. */
  val ForTests: GossipConfig = GossipConfig(100.millis, 50.millis, 200.millis, 20.millis, 2)
