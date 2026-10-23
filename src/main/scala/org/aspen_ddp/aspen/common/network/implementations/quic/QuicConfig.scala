package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.handler.codec.quic.QuicCodecBuilder
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.GossipConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}

/** Tunables for the QUIC network plugin.
  *
  * @param connectionBudgetBytes bound for the handoff queue and the pre-handshake queue. The per-stream high-water
  *                              mark is `connectionBudgetBytes / streamsPerConnection`
  * @param idleTimeout           QUIC max idle timeout. None means `heartbeatPeriod * 3`, resolved by the plugin
  * @param gossip                scalecube timings for host membership, and gossip relaying to clients
  * @param peerQueryDelay        how long dials to a host reported online must have been failing before its peers are
  *                              asked for its current endpoint (`HostEndpointQuery`)
  * @param peerQueryInterval     how often the query repeats while the host stays unreachable; also the period of the
  *                              scan that starts queries
  * @param peerQueryFanout       how many peers each query goes to
  */
final case class QuicConfig(
  eventLoopThreads: Int = math.min(Runtime.getRuntime.availableProcessors, 4),
  streamsPerConnection: Int = 4,
  connectionBudgetBytes: Long = 16L * 1024 * 1024,
  idleTimeout: Option[Duration] = None,
  handshakeTimeout: Duration = Duration(5, SECONDS),
  datagramQueueLength: Int = 1024,
  initialMaxData: Long = 64L * 1024 * 1024,
  initialMaxStreamDataUnidirectional: Long = 16L * 1024 * 1024,
  initialMaxStreamsUnidirectional: Long = 64,
  gossip: GossipConfig = GossipConfig(),
  peerQueryDelay: Duration = Duration(2, MINUTES),
  peerQueryInterval: Duration = Duration(2, MINUTES),
  peerQueryFanout: Int = 3):

  /** Applies the transport settings shared by the server and client codecs so both are configured identically */
  def applyTo[B <: QuicCodecBuilder[B]](b: B, idle: Duration): B =
    b.maxIdleTimeout(idle.toMillis, TimeUnit.MILLISECONDS)
      .initialMaxData(initialMaxData)
      .initialMaxStreamDataUnidirectional(initialMaxStreamDataUnidirectional)
      .initialMaxStreamsUnidirectional(initialMaxStreamsUnidirectional)
      .datagram(datagramQueueLength, datagramQueueLength)
