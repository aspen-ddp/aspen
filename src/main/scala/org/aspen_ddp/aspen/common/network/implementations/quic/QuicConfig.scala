package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.handler.codec.quic.QuicCodecBuilder
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.GossipConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, SECONDS}

/** Tunables for the QUIC network plugin.
  *
  * @param connectionBudgetBytes bound for the handoff queue and the pre-handshake queue. The per-stream high-water
  *                              mark is `connectionBudgetBytes / streamsPerConnection`
  * @param idleTimeout           QUIC max idle timeout. None means `heartbeatPeriod * 3`, resolved by the plugin
  * @param gossip                scalecube timings for host membership (host mode only)
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
  gossip: GossipConfig = GossipConfig()):

  /** Applies the transport settings shared by the server and client codecs so both are configured identically */
  def applyTo[B <: QuicCodecBuilder[B]](b: B, idle: Duration): B =
    b.maxIdleTimeout(idle.toMillis, TimeUnit.MILLISECONDS)
      .initialMaxData(initialMaxData)
      .initialMaxStreamDataUnidirectional(initialMaxStreamDataUnidirectional)
      .initialMaxStreamsUnidirectional(initialMaxStreamsUnidirectional)
      .datagram(datagramQueueLength, datagramQueueLength)
