package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.handler.codec.quic.QuicCodecBuilder
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.GossipConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}

/** Tunables for the QUIC network plugin.
  *
  * Send buffering is bounded by bytes. A connection's usage is its handoff queue plus the stream bytes whose writes
  * have not completed, and it may take more while that is below `connectionFloorBytes`, or below
  * `connectionCeilingBytes` while the host-wide `sendBudgetBytes`, shared by every connection, has room. Anything
  * else is dropped. So a small network gets large bursts, a host with thousands of connections stays within the
  * shared budget plus their floors, and a stalled peer holds at most its ceiling.
  *
  * @param sendBudgetBytes       host-wide send budget shared by all of a `QuicNet`'s connections
  * @param connectionFloorBytes  usage below which a connection may always queue, whatever the shared budget
  * @param connectionCeilingBytes most a connection may use: its handoff is admitted only below it, and each stream's
  *                              high-water mark is `connectionCeilingBytes / streamsPerConnection` (each may overshoot
  *                              by one message). `initialMaxData` should be at least this, or the peer's flow control
  *                              throttles bursts the budget would allow
  * @param preHandshakeBytes     bound for messages held until a connection's handshake completes, room for the burst
  *                              that often opens a connection. It takes nothing from the shared budget, so sends to a
  *                              host that cannot be reached hold at most this, until the handshake times out
  * @param datagramQueueLength   quiche's datagram send and receive queue lengths. Once a connection has written about
  *                              half that many datagrams in the last ~10 ms, datagram-sized messages go on a stream
  *                              instead: a full quiche queue drops datagrams silently
  * @param idleTimeout           QUIC max idle timeout. None means `heartbeatPeriod * 3`, resolved by the plugin
  * @param gossip                scalecube timings for host membership, and gossip relaying to clients
  * @param peerQueryDelay        how long dials to a host reported online must have been failing before its peers are
  *                              asked for its current endpoint (`HostEndpointQuery`)
  * @param peerQueryInterval     how often the query repeats while the host stays unreachable; also the period of the
  *                              scan that starts queries
  * @param peerQueryFanout       how many peers each query goes to
  * @param verifyPeerIdentity    check that the certificate of a host dialed through its host entry is for that HostId;
  *                              a mismatch marks the entry known-wrong (see `QuicNet`'s wrong-host detection)
  * @param wrongHostReplyInterval host mode: the least time between two `WrongHost` replies on one connection
  * @param knownWrongRetryAfter  how long a known-wrong host entry stays marked before the next send re-dials it; the
  *                              TLS check or a `WrongHost` reply marks it again if its address is still wrong
  */
final case class QuicConfig(
  eventLoopThreads: Int = math.min(Runtime.getRuntime.availableProcessors, 4),
  streamsPerConnection: Int = 4,
  sendBudgetBytes: Long = 256L * 1024 * 1024,
  connectionFloorBytes: Long = 256L * 1024,
  connectionCeilingBytes: Long = 64L * 1024 * 1024,
  preHandshakeBytes: Long = 16L * 1024 * 1024,
  idleTimeout: Option[Duration] = None,
  handshakeTimeout: Duration = Duration(5, SECONDS),
  datagramQueueLength: Int = 1024,
  initialMaxData: Long = 64L * 1024 * 1024,
  initialMaxStreamDataUnidirectional: Long = 16L * 1024 * 1024,
  initialMaxStreamsUnidirectional: Long = 64,
  gossip: GossipConfig = GossipConfig(),
  peerQueryDelay: Duration = Duration(2, MINUTES),
  peerQueryInterval: Duration = Duration(2, MINUTES),
  peerQueryFanout: Int = 3,
  verifyPeerIdentity: Boolean = true,
  wrongHostReplyInterval: Duration = Duration(1, SECONDS),
  knownWrongRetryAfter: Duration = Duration(10, MINUTES)):

  /** Applies the transport settings shared by the server and client codecs so both are configured identically */
  def applyTo[B <: QuicCodecBuilder[B]](b: B, idle: Duration): B =
    b.maxIdleTimeout(idle.toMillis, TimeUnit.MILLISECONDS)
      .initialMaxData(initialMaxData)
      .initialMaxStreamDataUnidirectional(initialMaxStreamDataUnidirectional)
      .initialMaxStreamsUnidirectional(initialMaxStreamsUnidirectional)
      .datagram(datagramQueueLength, datagramQueueLength)
