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
  * The defaults scale with the JVM's heap, since queued messages hold their payloads in memory. Objects of 10 MiB and
  * more are ordinary, and one such object is sent as a message per store, so a connection must be able to hold many of
  * them. A queue is only useful while it drains in time: at 100 MiB/s, 1 GiB of queue is 10 s of delay.
  *
  * @param sendBudgetBytes       host-wide send budget shared by all of a `QuicNet`'s connections
  * @param connectionFloorBytes  usage below which a connection may always queue, whatever the shared budget
  * @param connectionCeilingBytes most a connection may use: its handoff is admitted only below it, and each stream's
  *                              high-water mark, the small-message stream's included, is
  *                              `connectionCeilingBytes / streamsPerConnection` (each may overshoot
  *                              by one message). This queue is separate from the flow control windows
  *                              (`initialMaxData`, `initialMaxStreamDataUnidirectional`), which bound what is on its way
  *                              to the peer: what doesn't fit in them waits here until the peer reads
  * @param preHandshakeBytes     bound for messages held until a connection's handshake completes, room for the burst
  *                              that often opens a connection. It takes nothing from the shared budget, so sends to a
  *                              host that cannot be reached hold at most this, until the handshake times out
  * @param datagramQueueLength   quiche's datagram send and receive queue lengths. Only keepalives are datagrams
  * @param udpSocketBufferBytes  receive and send buffer size requested for each UDP socket. The OS default (208 KiB on
  *                              Linux) overflows under bursts of large messages, and every packet it drops is lost
  *                              data or a QUIC retransmission that slows the whole connection. Linux grants at most
  *                              `net.core.rmem_max` / `net.core.wmem_max`, and a smaller grant is logged
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
object QuicConfig:
  private val MiB = 1024L * 1024

  /** A quarter of the heap, and at least 256 MiB */
  def defaultSendBudget(maxHeapBytes: Long): Long = math.max(256 * MiB, maxHeapBytes / 4)

  /** A quarter of the default send budget, and at least 64 MiB */
  def defaultConnectionCeiling(maxHeapBytes: Long): Long = math.max(64 * MiB, defaultSendBudget(maxHeapBytes) / 4)

final case class QuicConfig(
  eventLoopThreads: Int = math.min(Runtime.getRuntime.availableProcessors, 4),
  streamsPerConnection: Int = 4,
  sendBudgetBytes: Long = QuicConfig.defaultSendBudget(Runtime.getRuntime.maxMemory),
  connectionFloorBytes: Long = 256L * 1024,
  connectionCeilingBytes: Long = QuicConfig.defaultConnectionCeiling(Runtime.getRuntime.maxMemory),
  preHandshakeBytes: Long = 16L * 1024 * 1024,
  idleTimeout: Option[Duration] = None,
  handshakeTimeout: Duration = Duration(5, SECONDS),
  datagramQueueLength: Int = 16,
  udpSocketBufferBytes: Int = 16 * 1024 * 1024,
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
