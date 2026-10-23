package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import scala.concurrent.duration.*

/** Scalecube timings for host membership, and the settings for relaying gossip to clients. The scalecube defaults
  * are scalecube's own production defaults.
  *
  * The suspicion timeout, after which an unresponsive member is declared dead, is
  * `suspicionMult * ceil(log2(clusterSize)) * pingInterval`.
  *
  * Clients are not members. Each subscribes to `relayCount` hosts, which forward it gossip and host online/offline
  * changes, renewing every `relayRenewal`; a host forgets a subscription not renewed within `relayLease`. A
  * `relayCount` of 0 turns relaying off.
  */
final case class GossipConfig(pingInterval: Duration = 1000.millis,
                              pingTimeout: Duration = 500.millis,
                              syncInterval: Duration = 3000.millis,
                              gossipInterval: Duration = 200.millis,
                              suspicionMult: Int = 5,
                              relayCount: Int = 3,
                              relayRenewal: Duration = 60.seconds,
                              relayLease: Duration = 180.seconds)

object GossipConfig:
  /** Short timings for in-process tests. */
  val ForTests: GossipConfig = GossipConfig(100.millis, 50.millis, 200.millis, 20.millis, 2,
    relayRenewal = 300.millis, relayLease = 900.millis)
