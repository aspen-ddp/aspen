package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import io.scalecube.cluster.transport.api.{Message, MessageCodec, Transport}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ClusterTransportMessage
import reactor.core.publisher.{Flux, Mono, Sinks}
import scribe.Logging

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID
import scala.util.Try

object QuicClusterTransport:
  /** scalecube requires `host:port`. Aspen addresses hosts logically, so the port is always 0. */
  def addressOf(hostId: HostId): String = s"${hostId.uuid}:0"

  def hostIdOf(address: String): Option[HostId] =
    Try(Transport.parseHost(address)).toOption
      .flatMap(h => Try(UUID.fromString(h)).toOption)
      .map(HostId(_))

/** A scalecube `Transport` that carries cluster traffic as Aspen ClusterTransportMessages.
  *
  * Outbound messages are encoded with `codec` and handed to `sendFn`; the owner feeds inbound
  * ClusterTransportMessages to `receive`. No sockets are owned here. Delivery is best-effort,
  * as it is for the underlying QUIC datagrams. The default codec is `FilteredMessageCodec`:
  * inbound payloads come from any credentialed peer, so deserialization is allowlisted.
  */
final class QuicClusterTransport(localHost: HostId,
                                 bootId: UUID,
                                 sendFn: ClusterTransportMessage => Unit,
                                 codec: MessageCodec = FilteredMessageCodec) extends Transport with Logging:

  private val inbound = Sinks.many().multicast().directBestEffort[Message]()
  @volatile private var started = false
  @volatile private var stopped = false

  override def address(): String = QuicClusterTransport.addressOf(localHost)

  override def start(): Mono[Transport] =
    started = true
    Mono.just(this)

  override def stop(): Mono[Void] =
    Mono.fromRunnable[Void]: () =>
      if !stopped then
        stopped = true
        inbound.synchronized:
          inbound.tryEmitComplete()

  override def isStopped: Boolean = stopped

  override def listen(): Flux[Message] = inbound.asFlux()

  /** Inbound entry point. Undecodable payloads are dropped; so is anything outside start..stop. */
  def receive(m: ClusterTransportMessage): Unit =
    if started && !stopped then
      Try(codec.deserialize(new ByteArrayInputStream(m.payload))).fold(
        err => logger.debug(s"Dropping undecodable cluster message from ${m.fromHost}: $err"),
        emit)

  private def emit(msg: Message): Unit =
    inbound.synchronized:
      inbound.tryEmitNext(msg)

  /** Lazy, like scalecube's own transport: nothing happens until the Mono is subscribed. */
  override def send(address: String, message: Message): Mono[Void] =
    Mono.defer[Void]: () =>
      if stopped then
        Mono.error(new IllegalStateException("Transport is stopped"))
      else
        QuicClusterTransport.hostIdOf(address) match
          case None => Mono.error(new IllegalArgumentException(s"Cannot parse address: $address"))
          case Some(target) if target == localHost =>
            emit(message)
            Mono.empty()
          case Some(target) =>
            Try:
              val out = new ByteArrayOutputStream()
              codec.serialize(message, out)
              sendFn(ClusterTransportMessage(localHost, bootId, target, out.toByteArray))
            .fold(err => Mono.error(err), _ => Mono.empty())

  /** Mirrors scalecube's TransportImpl.requestResponse. Adds no timeout; callers apply their own. */
  override def requestResponse(address: String, request: Message): Mono[Message] =
    if request.correlationId() == null then
      Mono.error(new IllegalArgumentException("correlationId must not be null"))
    else
      Mono.create[Message]: sink =>
        val receive = listen()
          .filter(r => r.correlationId() != null && r.correlationId() == request.correlationId())
          .take(1)
          .subscribe(m => sink.success(m), err => sink.error(err), () => sink.success())
        val sending = send(address, request).subscribe(
          _ => (),
          err =>
            receive.dispose()
            sink.error(err))
        sink.onDispose(() =>
          sending.dispose()
          receive.dispose())
