package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, Unpooled}
import io.netty.channel.{Channel, ChannelFuture, ChannelHandler, ChannelHandlerContext, ChannelInboundHandlerAdapter,
  ChannelInitializer, EventLoop, IoHandlerFactory, MultiThreadIoEventLoopGroup}
import io.netty.channel.epoll.{Epoll, EpollDatagramChannel, EpollIoHandler}
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.channel.unix.UnixChannelOption
import io.netty.handler.codec.quic.{QuicChannel, QuicClientCodecBuilder, QuicCodecDispatcher, QuicConnectionIdGenerator,
  QuicServerCodecBuilder, QuicSslContext, QuicStreamChannel}
import io.netty.util.{AttributeKey, NetUtil}
import io.netty.util.concurrent.Future as NFuture
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.{HostMembership, MembershipListener}
import org.aspen_ddp.aspen.common.network.snapshot.{HostSnapshot, NetworkSnapshotReader, StoreSnapshot}
import org.aspen_ddp.aspen.common.network.tls.CertificateVerifier
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, DaemonThreads, EvictingQueue}
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import scribe.Logging

import java.net.{InetAddress, InetSocketAddress}
import java.security.cert.X509Certificate
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, RejectedExecutionException, TimeUnit,
  TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*
import scala.util.Random
import scala.util.control.NonFatal

object QuicNet:
  /** The `QuicConnection` of a `QuicChannel`, set when the channel's pipeline is initialized */
  private val ConnectionKey: AttributeKey[QuicConnection] = AttributeKey.valueOf("aspen-quic-connection")

  private val CloseAwaitMillis: Long = 5000

  private val FlushPollMillis: Long = 25

  /** A client's gossip subscription on a relay host: the connection it arrived on and when its lease runs out */
  private final case class Subscription(conn: QuicConnection, expiresNanos: Long)

  /** The address of an IP literal, built without any lookup; None for a host name */
  private[quic] def ipLiteral(address: String): Option[InetAddress] =
    Option(NetUtil.createByteArrayFromIpAddressString(address)).map(InetAddress.getByAddress)

  /** The default name resolver: the JVM's, which blocks */
  def blockingResolve(name: String): InetAddress = InetAddress.getByName(name)

  private def remoteOf(ch: Channel): Any = ch match
    case q: QuicChannel => q.remoteSocketAddress()
    case other => other.remoteAddress()

  private class CliMessenger(net: QuicNet) extends ClientMessenger:
    def sendClientRequest(msg: ClientRequest): Unit = net.sendToStore(msg.toStore, msg)

    def sendTransactionMessage(msg: TxMessage): Unit = net.sendToStore(msg.to, msg)

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)

    def sendHostMessage(msg: HostMessage): Unit = net.sendToHostId(msg.toHost, msg)

    def dropCacheForStore(storeId: StoreId): Unit = net.metadataManager.dropStoreMapping(storeId)

    override def isStoreOffline(storeId: StoreId): Boolean = net.isStoreOffline(storeId)

    override def isStoreHostOnline(storeId: StoreId): Boolean = net.isStoreHostOnline(storeId)

  private class SrvMessenger(net: QuicNet) extends ServerMessenger:
    def sendClientResponse(msg: ClientResponse): Unit = net.sendToClient(msg)

    def sendTransactionMessage(msg: TxMessage): Unit = net.sendToStore(msg.to, msg)

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)

    def dropCacheForStore(storeId: StoreId): Unit = net.metadataManager.dropStoreMapping(storeId)

    /** Through membership to the other hosts, and directly to this host's own subscribers: scalecube does not hand a
      * member its own gossip, so store availability is applied here too. A host whose endpoint changed can gossip
      * too: see `sendClusterMessage`.
      */
    override def broadcastGossip(g: Gossip): Unit =
      net.membership.foreach(_.broadcast(g))
      net.forwardToSubscribers(g)
      net.storeAvailability.gossipReceived(g)


/** A `NetworkPlugin` over Netty QUIC with mutual TLS.
  *
  * Threading (see the design's Threading section):
  *  - One `MultiThreadIoEventLoopGroup` of `config.eventLoopThreads` daemon loops. Each loop owns one client datagram
  *    channel, and in host mode one server datagram channel (all bound to `dataPort` with `SO_REUSEPORT` and a shared
  *    `QuicCodecDispatcher`; without epoll, a single server channel on one loop).
  *  - A connection lives on the loop of the datagram channel it runs over, for its whole life, together with its
  *    streams, decoders and `QuicConnection` state. All receive-side work and every `MessageHandler` call run there.
  *  - Sends run on the caller's thread up to `QuicConnection.submit`: destination lookup, connection get-or-dial
  *    (CAS on `QuicHostEntry.connection`; only the winner dials) and encoding. Host names are resolved on the
  *    resolver pool (see `startDial`); IP literals need no lookup.
  *  - Shared state touched from both sides is lock-free: `clientRouteMap` and `liveConnections` are concurrent
  *    maps, `QuicHostEntry.connection` is an `AtomicReference`.
  *  - Host membership (host mode only) runs its listeners on its own "aspen-gossip" thread, never on a loop.
  *
  * Gossip reaches clients through relays. Clients are not membership members: each client keeps
  * `config.gossip.relayCount` relay hosts, picked at random from the hosts it has resolved, and sends each a
  * `GossipSubscribe` every `relayRenewal` (see `renewRelays`). A relay forwards every gossip message and every host
  * online/offline change to its subscribers, over streams so nothing is silently lost, until a subscription's
  * `relayLease` runs out or its connection closes (see `forwardToSubscribers`). Each renewal swaps one relay for
  * another, so clients do not stay on the bootstrap hosts they start with, and a relay reported offline is replaced
  * at once. A client therefore gets `relayCount` copies of each message, however many hosts it talks to.
  *
  * Construction order matters: `MetadataManager`'s constructor calls `createHostEntry` for each bootstrap host, so
  * everything the send path uses (loops, channels, maps) is initialized above `metadataManager`. `membership` is
  * created before the channels are bound, since `dispatch` reads it, but is only started at the very end.
  *
  * Store availability (`storeAvailability`) is learned from StoreOffline and StoreMoved gossip, from the snapshot
  * a relay sends when a subscription starts, and from a recheck run when the IO thread starts and every
  * `StoreAvailabilityTracker.RecheckPeriod` after.
  *
  * Peer endpoint query: a host entry can be stale (the host moved while we were not listening, or a cached address is
  * out of date). When dials to a host's current entry have been failing for `config.peerQueryDelay` and the host is
  * reported online (by membership, or by our relays in client mode), a small `HostEndpointQuery` datagram goes to
  * `config.peerQueryFanout` peers, repeating every `config.peerQueryInterval` until a dial succeeds or the entry is
  * replaced. A peer answers from its `MetadataManager` alone; an answer newer than our entry is applied exactly as
  * `HostEndpointChanged` gossip is. The failure clock is per HostId, starts when a dial to the host's current entry
  * closes without completing its handshake, and is cleared when an outbound handshake to the current entry completes
  * or the entry is replaced, so a new address gets a full delay. A query about a host never goes to that host, so
  * the design's "a host asked about itself answers with its own endpoint" never arises: every host just answers from
  * `MetadataManager`. `failingSince` and `lastQueried` are concurrent maps; the scan and every query run on
  * `loops(0)`, which owns `relays`.
  *
  * Wrong-host detection: a host entry's address can be up but belong to a different host (e.g. after a DHCP
  * reassignment). Two detectors catch it, both on the sending side's outbound connection for that entry:
  *  - TLS identity (`config.verifyPeerIdentity`): when the handshake completes, the peer certificate's UUID must be
  *    the entry's HostId (host certificates are signed for their HostId). It is checked before the connection is
  *    attached, so nothing queued behind the handshake reaches the wrong host.
  *  - `WrongHost` reply: every host-bound message, whether sent through a host entry or over a cluster route (see
  *    `sendClusterMessage`), carries the intended HostId in its envelope (`Received.toHost`). A host receiving one
  *    meant for another HostId drops it and, at most once per connection per `config.wrongHostReplyInterval`,
  *    replies `WrongHost` over the same connection. Messages without a `toHost`, and every message a client
  *    receives, are never checked.
  * Either signal marks the entry `knownWrong` (see `markWrong`): its connection is closed, sends through it are
  * dropped, and its peers are queried at once and then on every scan, whatever the failure clock and online state,
  * until an answer replaces the entry or `config.knownWrongRetryAfter` passes and the scan clears the mark, so the
  * next send re-dials. Only solicited answers are applied (see `endpointAnswer`). `lastWrongHostReply` is a
  * concurrent map, written on each connection's loop.
  *
  * @param resolveHost resolves a host address that is not an IP literal. It may block: it only runs on the
  *                    plugin's resolver threads, never on a caller or an event loop. Replaceable for tests.
  * @param storeAvailabilityCacheFile host mode: where store availability is saved across restarts
  * @param networkCache persists the store and host maps across restarts; seeds, and is written through by, MetadataManager
  */
class QuicNet(initialBootstrapConfig: BootstrapConfig.Config,
              onBootstrapConfigRefreshed: (String, BootstrapConfig.Config) => Unit,
              val oclientId: Option[ClientId],
              val ohost: Option[(HostId, Int)],
              val heartbeatPeriod: Duration,
              val messageHandler: MessageHandler,
              tls: TlsCredentials,
              config: QuicConfig = QuicConfig(),
              resolveHost: String => InetAddress = QuicNet.blockingResolve,
              storeAvailabilityCacheFile: Option[os.Path] = None,
              networkCache: NetworkCache = NetworkCache.NoCache) extends NetworkPlugin with Logging:

  import QuicNet.*

  val clientId: ClientId = oclientId.getOrElse(ClientId(UUID.randomUUID()))

  logger.debug(s"QuicNet Client ID: ${clientId.uuid.toString}")

  private val idleTimeout: Duration = config.idleTimeout.getOrElse(heartbeatPeriod * 3)

  private val useEpoll: Boolean = Epoll.isAvailable

  private val datagramChannelClass: Class[? <: Channel] =
    if useEpoll then classOf[EpollDatagramChannel] else classOf[NioDatagramChannel]

  private val group: MultiThreadIoEventLoopGroup =
    val factory: IoHandlerFactory = if useEpoll then EpollIoHandler.newFactory() else NioIoHandler.newFactory()
    new MultiThreadIoEventLoopGroup(config.eventLoopThreads, DaemonThreads.factory("aspen-quic"), factory)

  private val loops: Array[EventLoop] = group.iterator().asScala.map(_.asInstanceOf[EventLoop]).toArray

  /** Outbound connections, and inbound ones until they close. Membership is how `closeForTests` finds them. */
  private val liveConnections = ConcurrentHashMap.newKeySet[QuicConnection]()
  private val inboundConnections = ConcurrentHashMap.newKeySet[QuicConnection]()

  /** Most recent connection each client's `ClientRequest`s arrived on. Written on loops, read by any sender. */
  private val clientRouteMap = new ConcurrentHashMap[ClientId, QuicConnection]()

  /** Most recent connection each host's scalecube traffic arrived on. Written on loops, read by membership's sends.
    * See `sendClusterMessage`.
    */
  private val clusterRouteMap = new ConcurrentHashMap[HostId, QuicConnection]()

  /** Host mode: the clients subscribed to this host's gossip. Written on loops, read by forwarding threads. */
  private val subscribers = new ConcurrentHashMap[ClientId, Subscription]()

  /** Which stores are offline for a transfer or rebuild. Thread-safe. */
  private[quic] val storeAvailability = new StoreAvailabilityTracker(storeAvailabilityCacheFile)

  /** Rechecks offline stores. Set once, by `startIoThread`. */
  @volatile private var recheckTask: Option[NFuture[?]] = None

  /** Applies the daily network snapshots. Set once, by `startIoThread`. */
  @volatile private var snapshotReader: Option[NetworkSnapshotReader[QuicHostEntry]] = None

  /** Client mode: the hosts our relays last reported online. Written on loops, read by `createHostEntry`. */
  private val relayedOnline = ConcurrentHashMap.newKeySet[HostId]()

  /** Client mode: our current gossip relay hosts. Only touched on `loops(0)`. See `renewRelays`. */
  private var relays: Set[HostId] = Set()

  /** When (System.nanoTime) dials to each host's current entry started failing. Written on loops. */
  private val failingSince = new ConcurrentHashMap[HostId, java.lang.Long]()

  /** When (System.nanoTime) each host was last asked about. Written on `loops(0)`, cleared by `endpointChanged`. */
  private val lastQueried = new ConcurrentHashMap[HostId, java.lang.Long]()

  private val peerQueriesSent = new AtomicLong(0)

  private val wrongHostRepliesSent = new AtomicLong(0)

  /** Host mode: when (System.nanoTime) each connection last carried a `WrongHost` reply. Written on its loop. */
  private val lastWrongHostReply = new ConcurrentHashMap[QuicConnection, java.lang.Long]()

  private val nextDialIndex = new AtomicInteger(0)

  /** The last boot id seen from each host in a `ClusterTransportMessage`. Written on loops. See `clusterMessage`. */
  private val bootIds = new ConcurrentHashMap[HostId, UUID]()

  /** Runs `resolveHost` for host names. Threads are created on demand, so a stuck lookup blocks no other. */
  private val resolverPool: ExecutorService = Executors.newCachedThreadPool(DaemonThreads.factory("aspen-quic-resolve"))

  /** Every datagram channel bound so far, so a failed construction can close them */
  private val boundChannels = new java.util.concurrent.ConcurrentLinkedQueue[Channel]()

  /** Runs one step of construction. If it throws, everything built since the group was created is torn down (bound
    * channels closed, resolver stopped, group shut down and awaited) and the exception is rethrown. Every fallible
    * step after the group's creation goes through this.
    */
  private def initOrAbort[T](body: => T): T =
    try body
    catch
      case t: Throwable =>
        try
          // Null while membership itself is being constructed. Membership shutdown never blocks.
          if membership != null then
            membership.foreach(_.shutdown())
          boundChannels.forEach(ch => ch.close().awaitUninterruptibly(CloseAwaitMillis))
          resolverPool.shutdownNow()
          group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(CloseAwaitMillis * 2)
        catch
          case NonFatal(e) => t.addSuppressed(e)
        throw t

  /** Set by `shutdown`; every send after that is a silent drop */
  @volatile private var shuttingDown = false
  private val shutdownStarted = new AtomicBoolean(false)

  private val keepaliveCount = new AtomicLong(0)
  private val undecodableCount = new AtomicLong(0)

  /** Scalecube host membership; None in client mode. Started at the end of construction, after its listener is
    * registered (listeners added later get no replay of hosts already online). Its traffic goes out through
    * `sendToHostId`, which drops everything once `shutdown` has begun.
    */
  private[quic] val membership: Option[HostMembership] = initOrAbort:
    ohost.map: (hostId, _) =>
      // Seeds are fixed here: scalecube cannot change them after start. They matter only for the first join (later
      // the cluster's own membership finds peers), so a bootstrap refresh need not reach them.
      new HostMembership(hostId, UUID.randomUUID(), initialBootstrapConfig.hosts.map(_.hostId), config.gossip,
        sendClusterMessage)

  // ---- Pipeline handlers ---------------------------------------------------------------------------------------

  /** Installs a `StreamFrameDecoder` on every inbound stream of every connection, both directions */
  private val inboundStreamInitializer: ChannelHandler = new ChannelInitializer[QuicStreamChannel]:
    override def isSharable: Boolean = true

    override def initChannel(ch: QuicStreamChannel): Unit =
      val conn = ch.parent().attr(ConnectionKey).get()
      ch.pipeline().addLast(new StreamFrameDecoder(
        m => dispatch(conn, m),
        reason => logger.warn(s"QUIC stream protocol error from ${remoteOf(ch.parent())}: $reason")))

  /** Per-connection handler on the `QuicChannel` pipeline: datagram receive, and attach for inbound connections */
  private final class ConnectionHandler(conn: QuicConnection, inbound: Boolean) extends ChannelInboundHandlerAdapter:
    /** The dialed channel, so a handshake timeout can close it. Loop-owned. */
    var channel: Channel = null

    override def handlerAdded(ctx: ChannelHandlerContext): Unit =
      channel = ctx.channel()
      channel.attr(ConnectionKey).set(conn)

    override def channelActive(ctx: ChannelHandlerContext): Unit =
      if inbound then
        conn.attach(ctx.channel().asInstanceOf[QuicChannel])
      ctx.fireChannelActive()

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit =
      msg match
        case buf: ByteBuf if QuicConnection.isKeepalive(buf) =>
          // Sent by the dialing side of a quiet connection; see QuicConnection.startKeepalive
          buf.release()
          keepaliveCount.incrementAndGet()
        case buf: ByteBuf =>
          val decoded =
            try QuicMessageCodec.decodeDatagram(buf)
            finally buf.release()
          decoded match
            case Right(m) => dispatch(conn, m)
            case Left(err) =>
              undecodableCount.incrementAndGet()
              logger.warn(s"Undecodable QUIC datagram from ${remoteOf(ctx.channel())}: $err")
        case other => ctx.fireChannelRead(other)

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit =
      logger.debug(s"QUIC connection ${remoteOf(ctx.channel())} error: $cause")

  /** Server codec `handler`: gives each inbound `QuicChannel` its own `QuicConnection` at channel init */
  private val inboundConnectionInitializer: ChannelHandler = new ChannelInitializer[QuicChannel]:
    override def isSharable: Boolean = true

    override def initChannel(ch: QuicChannel): Unit =
      val conn = new QuicConnection(ch.eventLoop(), config, inboundClosed)
      liveConnections.add(conn)
      inboundConnections.add(conn)
      // The peer's datagram-extension event fires before channelActive, so pipelineHandler goes in now
      ch.pipeline().addLast(conn.pipelineHandler, new ConnectionHandler(conn, inbound = true))
      // Covers channels that close before channelActive, when attach never ran
      ch.closeFuture().addListener((_: ChannelFuture) => conn.close())

  // ---- Datagram channels ---------------------------------------------------------------------------------------

  private def serverCodecBuilder(ssl: QuicSslContext): QuicServerCodecBuilder =
    config.applyTo(new QuicServerCodecBuilder(), idleTimeout)
      .sslContext(ssl)
      .handler(inboundConnectionInitializer)
      .streamHandler(inboundStreamInitializer)

  private def bind(loop: EventLoop, handler: ChannelHandler, address: InetSocketAddress, reusePort: Boolean): Channel =
    val b = new Bootstrap().group(loop).channel(datagramChannelClass).handler(handler)
    if reusePort then
      // SO_REUSEPORT is declared on UnixChannelOption; EpollChannelOption only inherits it
      b.option(UnixChannelOption.SO_REUSEPORT, java.lang.Boolean.TRUE)
    val ch = b.bind(address).sync().channel()
    boundChannels.add(ch)
    ch

  /** Fails unless `port` can be bound without `SO_REUSEPORT`, i.e. no other socket holds it. The server channels
    * bind with `SO_REUSEPORT`, which would otherwise let a second process silently share a host's data port.
    */
  private def probeDataPort(port: Int): Unit =
    val probe = java.nio.channels.DatagramChannel.open()
    try
      probe.setOption(java.net.StandardSocketOptions.SO_REUSEADDR, java.lang.Boolean.FALSE)
      probe.bind(new InetSocketAddress(port))
    catch
      case e: java.io.IOException =>
        val err = new java.net.BindException(s"QUIC data port $port is already in use")
        err.initCause(e)
        throw err
    finally
      probe.close()

  private val (clientChannels, serverChannels): (Array[Channel], Array[Channel]) = initOrAbort:
    ohost.foreach((_, port) => probeDataPort(port))
    val clientSsl = QuicTls.clientContext(tls)
    val clients = loops.map: loop =>
      val codec = config.applyTo(new QuicClientCodecBuilder(), idleTimeout).sslContext(clientSsl).build()
      bind(loop, codec, new InetSocketAddress(0), reusePort = false)
    val servers = ohost match
      case None => Array.empty[Channel]
      case Some((_, port)) =>
        val serverSsl = QuicTls.serverContext(tls)
        if useEpoll then
          // One dispatcher shared by every SO_REUSEPORT channel, so packets reach the codec owning their connection
          val dispatcher = new QuicCodecDispatcher():
            override protected def initChannel(ch: Channel, localConnectionIdLength: Int,
                                               idGenerator: QuicConnectionIdGenerator): Unit =
              ch.pipeline().addLast(serverCodecBuilder(serverSsl)
                .localConnectionIdLength(localConnectionIdLength)
                .connectionIdAddressGenerator(idGenerator)
                .build())
          loops.map(loop => bind(loop, dispatcher, new InetSocketAddress(port), reusePort = true))
        else
          Array(bind(loops(0), serverCodecBuilder(serverSsl).build(), new InetSocketAddress(port), reusePort = false))
    (clients, servers)

  // ---- MetadataManager -----------------------------------------------------------------------------------------

  // Called on arbitrary threads, possibly under MetadataManager's lock: these only hand off to the send path,
  // which never calls back into MetadataManager for a known entry.
  private val networkImpl = new MetadataManager.NetworkImplInterface[QuicHostEntry]:
    def createHostEntry(hostId: HostId,
                        name: String,
                        address: String,
                        dataPort: Int,
                        httpPort: Int,
                        storeTransferPort: Int,
                        queuedMessages: EvictingQueue[Message]): QuicHostEntry =
      val entry = new QuicHostEntry(hostId, name, address, dataPort, httpPort, storeTransferPort)
      // A host whose online event arrived before its entry existed (or that was dropped and re-read)
      if reportedOnline(hostId) then
        entry.setOnline()
      sendQueued(entry, queuedMessages)
      entry

    def storeResolved(hostEntry: QuicHostEntry, storeId: StoreId, queuedMessages: EvictingQueue[Message]): Unit =
      sendQueued(hostEntry, queuedMessages)

  val metadataManager: MetadataManager[QuicHostEntry] = initOrAbort:
    new MetadataManager[QuicHostEntry](initialBootstrapConfig, onBootstrapConfigRefreshed, networkImpl,
                                      networkCache, ohost.map(_._1))

  val clientMessenger: ClientMessenger = new CliMessenger(this)
  val serverMessenger: ServerMessenger = new SrvMessenger(this)

  /** Applies membership events and gossip, and forwards them to our subscribers. Runs on the membership's
    * "aspen-gossip" thread.
    */
  private val membershipListener = new MembershipListener:
    // Peek, never look up: an entry created later seeds its online state from membership (see createHostEntry)
    override def online(hostId: HostId): Unit =
      metadataManager.peekHostEntry(hostId).foreach(_.setOnline())
      forwardToSubscribers(HostOnlineStatus(List(hostId), Nil))

    override def offline(hostId: HostId): Unit =
      metadataManager.peekHostEntry(hostId).foreach(_.setOffline())
      forwardToSubscribers(HostOnlineStatus(Nil, List(hostId)))

    // Gossip that arrived through membership is never re-broadcast
    override def gossipReceived(g: Gossip): Unit =
      applyGossip(g)
      forwardToSubscribers(g)

  /** Whether membership (host mode) or our relays (client mode) last reported `hostId` online */
  private def reportedOnline(hostId: HostId): Boolean = membership match
    case Some(m) => m.isOnline(hostId)
    case None => relayedOnline.contains(hostId)

  /** Gossip from membership (host mode) or a relay (client mode). Idempotent: every message carries the timestamp
    * of the state it announces, and a message no newer than what we hold is ignored.
    */
  private def applyGossip(g: Gossip): Unit =
    g match
      case m: StoreMoved => metadataManager.storeMoved(m.storeId, m.hostId, m.timestamp)
      case m: HostEndpointChanged => endpointChanged(m)
      case _: StoreOffline => ()
    storeAvailability.gossipReceived(g)

  /** Replaces `m.hostId`'s entry with one for the new endpoint, unless ours is at least as new, and closes our
    * outbound connection to the old one so the next send dials the new endpoint. Our own entry is left alone.
    */
  private def endpointChanged(m: HostEndpointChanged): Unit =
    if !ohost.exists(_._1 == m.hostId) then
      metadataManager.endpointChanged(m).foreach: old =>
        logger.info(s"Host ${old.name} changed endpoint to ${m.address}:${m.dataPort}; closing the old connection")
        // A new address to try: restart the failure clock. The old entry's closing dial is not counted against it
        // (see outboundClosed).
        failingSince.remove(m.hostId)
        lastQueried.remove(m.hostId)
        closeOutbound(old)

  /** Any thread. `old` was replaced or dropped by a network snapshot: as `endpointChanged` does for the entry it
    * replaces, restarts the failure clock and closes the old outbound connection, so the next send dials anew.
    */
  private def snapshotRetired(old: QuicHostEntry): Unit =
    logger.info(s"Network snapshot retired host ${old.name} at ${old.address}:${old.dataPort}; closing its connection")
    failingSince.remove(old.hostId)
    lastQueried.remove(old.hostId)
    closeOutbound(old)

  initOrAbort:
    membership.foreach: m =>
      m.addListener(membershipListener)
      m.start()

  /** Client mode: renews our relay subscriptions every `relayRenewal`, starting now */
  private val relayTask: Option[NFuture[?]] = initOrAbort:
    if membership.isDefined then
      None
    else
      val ms = math.max(1L, config.gossip.relayRenewal.toMillis)
      Some(loops(0).scheduleAtFixedRate((() => renewRelays(rotate = true)): Runnable, 0, ms, TimeUnit.MILLISECONDS))

  /** Starts peer endpoint queries every `peerQueryInterval`. See `scanForPeerQueries`. */
  private val peerQueryTask: NFuture[?] = initOrAbort:
    val ms = math.max(1L, config.peerQueryInterval.toMillis)
    loops(0).scheduleAtFixedRate((() => scanForPeerQueries()): Runnable, ms, ms, TimeUnit.MILLISECONDS)

  // ---- NetworkPlugin -------------------------------------------------------------------------------------------

  /** The loops started with the group; this only supplies the client used for metadata lookups and starts the
    * offline store recheck, which first runs now, and the network snapshot reader
    */
  def startIoThread(client: AspenClient): Unit =
    metadataManager.setAspenClient(client)
    if recheckTask.isEmpty then
      val ms = StoreAvailabilityTracker.RecheckPeriod.toMillis
      val recheck: Runnable = () =>
        if !shuttingDown then
          try storeAvailability.recheck(client)
          catch case NonFatal(e) => logger.warn(s"Offline store recheck failed: $e")
      recheckTask = Some(loops(0).scheduleAtFixedRate(recheck, 0, ms, TimeUnit.MILLISECONDS))
      snapshotReader = Some(NetworkSnapshotReader.start(client, metadataManager, snapshotRetired))

  override def isStoreOffline(storeId: StoreId): Boolean = storeAvailability.isOffline(storeId)

  /** True if `storeId` is mapped to this host, or to one membership or our relays last reported online */
  def isStoreHostOnline(storeId: StoreId): Boolean =
    metadataManager.peekStoreHost(storeId).exists(h => ohost.exists(_._1 == h) || reportedOnline(h))

  def joinIoThread(): Unit = group.terminationFuture().awaitUninterruptibly()

  /** Blocks the caller until every message submitted before the call has been accepted by QUIC (its datagram or
    * stream write future has completed) or dropped, or until `timeout`. Returns true only if that happened in time.
    *
    * Runs against one deadline:
    *  1. Polls every 25ms until `MetadataManager` holds no messages parked on a lookup.
    *  2. Posts a barrier on every live connection, outbound and inbound. A barrier completes once every write handed
    *     to its connection before it has completed or been dropped; on a connection still handshaking, once the
    *     pre-handshake messages have been written or dropped.
    *  3. Waits for all barriers.
    *
    * Messages submitted after a barrier was posted are not waited for. Dropped messages (full budgets, failed lookups,
    * failed handshakes) count as no longer pending. Acceptance by QUIC is not a delivery guarantee: Netty does not
    * report when quiche has put the packets on the wire, and nothing waits for the peer.
    *
    * Known limit: the connections are snapshotted after step 1. A message released from a lookup by step 1 is
    * covered only if the connection it went to already exists at the snapshot. If the send that the lookup
    * completion triggered has not yet created its connection by then (the resolution and the dial race the
    * snapshot), that message is not waited for. Similarly a message whose sender is between installing a new
    * connection on its host entry and registering it may be missed, though connections on resolved host entries are
    * included directly to narrow that window.
    *
    * Must not be called on one of this plugin's event loops: that would block the loop the barriers need.
    *
    * @throws IllegalStateException if called on one of this plugin's event loops
    */
  def awaitPendingMessagesSent(timeout: Duration): Boolean =
    if loops.exists(_.inEventLoop()) then
      throw new IllegalStateException("QuicNet.awaitPendingMessagesSent must not be called on an event loop")

    val deadline = System.nanoTime() + timeout.toNanos
    def remainingMillis: Long = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())

    while metadataManager.hasParkedMessages && remainingMillis > 0 do
      Thread.sleep(math.min(FlushPollMillis, remainingMillis))

    if metadataManager.hasParkedMessages then
      false
    else
      val conns = new java.util.IdentityHashMap[QuicConnection, Unit]()
      liveConnections.forEach(c => conns.put(c, ()))
      metadataManager.resolvedHostEntries.foreach: entry =>
        val c = entry.connection.get()
        if c != null then
          conns.put(c, ())
      clientRouteMap.values().forEach(c => conns.put(c, ()))

      val barriers = conns.keySet().asScala.toList.map(_.postBarrier())
      barriers.forall: b =>
        b.await(math.max(0L, remainingMillis), TimeUnit.MILLISECONDS)

  /** Stops the network. Idempotent: calls after the first do nothing.
    *
    *  0. Host membership stops without leaving the cluster (crash-only: peers detect the failure). Never blocks.
    *  1. Every send from now on is silently dropped, and relay renewal (client mode) stops.
    *  2. Sleeps the full `linger` on the caller's thread, so quiche can transmit what flush handed it.
    *     `QuicConnectionStats` exposes no "unsent bytes" signal, so it cannot end early.
    *  3. On each connection's own loop, closes it with a QUIC application close (code 0, empty reason).
    *  4. Closes the datagram channels.
    *  5. Starts `shutdownGracefully(0, 1s)` on the group. `joinIoThread` returns once it has terminated.
    *
    * Call `awaitPendingMessagesSent` first: anything still in the plugin's own queues is discarded. The close frame
    * is a best-effort courtesy (crash-only). Exceptions are swallowed and logged at trace.
    */
  def shutdown(linger: Duration): Unit =
    if shutdownStarted.compareAndSet(false, true) then
      quietly("stop membership"):
        membership.foreach(_.shutdown())
      shuttingDown = true
      quietly("stop relay renewal"):
        relayTask.foreach(_.cancel(false))
        recheckTask.foreach(_.cancel(false))
        peerQueryTask.cancel(false)
      quietly("stop snapshot reader"):
        snapshotReader.foreach(_.stop())
      quietly("linger"):
        val ms = linger.toMillis
        if ms > 0 then
          Thread.sleep(ms)
      closeAll(awaitMillis = if loops.exists(_.inEventLoop()) then 0L else CloseAwaitMillis)

  /** Runs `body`, logging anything it throws at trace. Interrupts are re-asserted. */
  private def quietly(step: String)(body: => Unit): Unit =
    try body
    catch
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        logger.trace(s"QuicNet shutdown ($step) interrupted: $e")
      case t: Throwable => logger.trace(s"QuicNet shutdown ($step) failed: $t")

  /** Steps 3-5 of `shutdown`, shared with `closeForTests`. Each step is attempted even if an earlier one failed.
    * `awaitMillis` bounds each wait for the connection closes and channel closes; 0 skips waiting (on a loop).
    */
  private def closeAll(awaitMillis: Long): Unit =
    quietly("close connections"):
      val closes = liveConnections.toArray(new Array[QuicConnection](0)).toList.flatMap: conn =>
        try Some(conn.loop.submit((() => closeOnLoop(conn)): Runnable))
        catch case _: RejectedExecutionException => None
      if awaitMillis > 0 then
        closes.foreach(_.awaitUninterruptibly(awaitMillis))
    quietly("close datagram channels"):
      val closes = (serverChannels ++ clientChannels).toList.map(_.close())
      if awaitMillis > 0 then
        closes.foreach(_.awaitUninterruptibly(awaitMillis))
    quietly("shut down resolver"):
      resolverPool.shutdownNow()
    quietly("shut down event loop group"):
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS)

  /** On `conn`'s loop: QUIC application close of an attached channel, then the connection's own close */
  private def closeOnLoop(conn: QuicConnection): Unit =
    val ch = conn.attachedChannel
    if ch != null && ch.isOpen then
      ch.close(true, 0, Unpooled.EMPTY_BUFFER)
    conn.close()

  // ---- Test accessors ------------------------------------------------------------------------------------------

  private[quic] def connectionCount: Int = liveConnections.size

  private[quic] def inboundConnectionCount: Int = inboundConnections.size

  private[quic] def outboundConnectionCount: Int = liveConnections.asScala.count(c => !inboundConnections.contains(c))

  private[quic] def clientRoutes: Int = clientRouteMap.size

  private[quic] def inboundConnectionLoops: Set[EventLoop] = inboundConnections.asScala.map(_.loop).toSet

  private[quic] def eventLoopsForTests: Seq[EventLoop] = loops.toSeq

  /** Keepalive datagrams received and discarded */
  private[quic] def keepalivesReceived: Long = keepaliveCount.get()

  /** Datagrams that failed to decode */
  private[quic] def undecodableDatagrams: Long = undecodableCount.get()

  /** Client mode: our current relay hosts. Blocks until `loops(0)` answers. */
  private[quic] def relaysForTests: Set[HostId] = loops(0).submit(() => relays).get()

  /** Host mode: the clients holding a subscription, lapsed or not */
  private[quic] def subscribersForTests: Set[ClientId] = subscribers.keySet().asScala.toSet

  /** The last boot id recorded for `hostId` */
  private[quic] def lastBootIdOf(hostId: HostId): Option[UUID] = Option(bootIds.get(hostId))

  /** `HostEndpointQuery` messages sent so far, counting each peer separately */
  private[quic] def peerQueriesSentForTests: Long = peerQueriesSent.get()

  /** When (System.nanoTime) dials to `hostId`'s current entry started failing, if they are */
  private[quic] def failingSinceForTests(hostId: HostId): Option[Long] = Option(failingSince.get(hostId)).map(_.longValue)

  /** When (System.nanoTime) peers were last asked for `hostId`'s endpoint, if they have been */
  private[quic] def lastQueriedForTests(hostId: HostId): Option[Long] = Option(lastQueried.get(hostId)).map(_.longValue)

  /** Our relays (client mode) and the peers a query about `target` would go to now. Blocks until `loops(0)` answers. */
  private[quic] def peerQueryPlanForTests(target: HostId): (Set[HostId], List[HostId]) =
    loops(0).submit(() => (relays, queryPeers(target))).get()

  /** Whether `hostId`'s scalecube traffic has a recorded route back (see `sendClusterMessage`) */
  private[quic] def hasClusterRouteForTests(hostId: HostId): Boolean = clusterRouteMap.containsKey(hostId)

  /** Membership's send path, as scalecube would call it */
  private[quic] def sendClusterMessageForTests(m: ClusterTransportMessage): Unit = sendClusterMessage(m)

  /** Starts a snapshot reader reading from `fetch`, as `startIoThread` starts the real one, with nothing scheduled:
    * it reads once now if the network cache was empty. Stopped with the net.
    */
  private[quic] def startSnapshotReaderForTests(fetch: () => Future[Option[(HostSnapshot, StoreSnapshot)]]): Unit =
    val reader = new NetworkSnapshotReader[QuicHostEntry](metadataManager, fetch,
      BackgroundTaskManager.NoBackgroundTaskManager, snapshotRetired)(using ExecutionContext.global)
    snapshotReader.foreach(_.stop())
    snapshotReader = Some(reader)
    reader.start()

  /** `WrongHost` replies sent so far */
  private[quic] def wrongHostRepliesSentForTests: Long = wrongHostRepliesSent.get()

  /** Dispatches `m` as if it had arrived on a connection, with `toHost` as its envelope's intended destination */
  private[quic] def dispatchForTests(m: Message, toHost: Option[HostId] = None): Unit =
    dispatch(null, QuicMessageCodec.Received(m, toHost))

  /** `shutdown` without the shutting-down flag or the linger, waiting for the group to terminate: stops
    * membership, closes every connection on its loop (which cancels its timers), closes the datagram
    * channels and terminates the group
    */
  private[quic] def closeForTests(): Unit =
    membership.foreach(_.shutdown())
    relayTask.foreach(_.cancel(false))
    recheckTask.foreach(_.cancel(false))
    peerQueryTask.cancel(false)
    snapshotReader.foreach(_.stop())
    closeAll(CloseAwaitMillis)
    group.terminationFuture().awaitUninterruptibly(CloseAwaitMillis * 2)

  /** Simulates a crash: stops membership without leaving the cluster (peers must detect the failure), closes the
    * datagram channels first, so no QUIC close frame can reach a peer, then terminates the group. Peers keep their
    * connections to this net until they find out by other means.
    */
  private[quic] def crashForTests(): Unit =
    membership.foreach(_.shutdown())
    relayTask.foreach(_.cancel(false))
    recheckTask.foreach(_.cancel(false))
    peerQueryTask.cancel(false)
    snapshotReader.foreach(_.stop())
    (serverChannels ++ clientChannels).foreach(_.close().awaitUninterruptibly(CloseAwaitMillis))
    resolverPool.shutdownNow()
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS)
    group.terminationFuture().awaitUninterruptibly(CloseAwaitMillis * 2)

  // ---- Send path (caller threads) ------------------------------------------------------------------------------

  private def sendQueued(entry: QuicHostEntry, queued: EvictingQueue[Message]): Unit =
    var omsg = queued.dequeue()
    while omsg.isDefined do
      omsg.foreach(sendToHost(entry, _))
      omsg = queued.dequeue()

  private def sendToStore(storeId: StoreId, msg: Message): Unit =
    if shuttingDown then
      droppedAfterShutdown(msg)
    else
      try
        // None: parked on a lookup; storeResolved or createHostEntry sends it later
        metadataManager.getHostEntryOrQueueMessage(storeId, msg).foreach(sendToHost(_, msg))
      catch
        case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to store $storeId failed: $e")

  private def sendToHostId(hostId: HostId, msg: Message): Unit =
    if shuttingDown then
      droppedAfterShutdown(msg)
    else
      try
        metadataManager.getHostEntryOrQueueMessage(hostId, msg).foreach(sendToHost(_, msg))
      catch
        case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to host $hostId failed: $e")

  private[quic] def sendToHost(entry: QuicHostEntry, msg: Message): Unit =
    if shuttingDown then
      droppedAfterShutdown(msg)
    else
      try
        if entry.knownWrong then
          logger.debug(s"Dropping ${msg.getClass.getSimpleName} for host ${entry.name}: its address belongs to another host")
        else
          val conn = connectionFor(entry)
          conn.submit(QuicMessageCodec.encode(msg, PooledByteBufAllocator.DEFAULT, toHost = Some(entry.hostId)))
      catch
        case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to host ${entry.name} failed: $e")

  /** Membership's send path. Scalecube traffic goes back over the connection the peer's own scalecube traffic last
    * arrived on, and only to a host not heard from yet (or whose connection closed) through its host entry.
    *
    * Why: a host whose endpoint changed (its host object updated at startup, see `Host.reconcileEndpoint`) is still
    * known to every peer by its old address, because a resolved host entry is only replaced by
    * `HostEndpointChanged` gossip. The moved host can reach its peers, since its own outbound connections
    * use fresh addresses, but scalecube answers every message (SYNC_ACK, ping acks, metadata responses) by sending
    * to the sender's member address, and a send through the stale entry goes to the old address and is lost. The
    * moved host would then never learn the member list, and its `HostEndpointChanged` gossip, which is what makes
    * peers replace its entry, would have nobody to go to. Replying over the connection the moved host dialed
    * breaks that deadlock: it joins normally, and its gossip spreads to everyone through the cluster.
    *
    * This was chosen over having the moved host also send `HostEndpointChanged` directly to every host it had
    * resolved (which they re-gossiped). That works too, but adds a second delivery path for one message; with reply
    * routing, endpoint changes travel through gossip alone, as every other gossip does. Nor can a restart drop the
    * peer's entry to force a re-read: the re-read can wait forever behind the restarted host's own bootstrap stores
    * (see `clusterMessage`).
    *
    * A crashed peer's route stays until its connection idles out; sends to it are lost, which is what failure
    * detection expects. A restarted peer's first message arrives on its new connection and replaces the route.
    */
  private def sendClusterMessage(m: ClusterTransportMessage): Unit =
    val route = clusterRouteMap.get(m.toHost)
    if route != null && !route.isClosed && !shuttingDown then
      try route.submit(QuicMessageCodec.encode(m, PooledByteBufAllocator.DEFAULT, toHost = Some(m.toHost)))
      catch case NonFatal(e) => logger.warn(s"QUIC send of cluster traffic to host ${m.toHost} failed: $e")
    else
      sendToHostId(m.toHost, m)

  private def sendToClient(msg: ClientResponse): Unit =
    if shuttingDown then
      droppedAfterShutdown(msg)
    else
      try
        val conn = clientRouteMap.get(msg.toClient)
        if conn == null then
          logger.debug(s"Dropping ${msg.getClass.getSimpleName} for unknown client ${msg.toClient.uuid}")
        else
          conn.submit(QuicMessageCodec.encode(msg, PooledByteBufAllocator.DEFAULT))
      catch
        case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to client ${msg.toClient.uuid} failed: $e")

  private def droppedAfterShutdown(msg: Message): Unit =
    logger.trace(s"QuicNet is shut down; dropping ${msg.getClass.getSimpleName}")

  /** The entry's open connection, or a new one installed by CAS and dialed by whichever caller wins */
  private[quic] def connectionFor(entry: QuicHostEntry): QuicConnection =
    var result: QuicConnection = null
    while result == null do
      val cur = entry.connection.get()
      if cur != null && !cur.isClosed then
        result = cur
      else
        val parent = clientChannels(Math.floorMod(nextDialIndex.getAndIncrement(), clientChannels.length))
        val conn = new QuicConnection(parent.eventLoop(), config, c => outboundClosed(entry, c), c => outboundActive(entry, c))
        if entry.connection.compareAndSet(cur, conn) then
          liveConnections.add(conn)
          try
            startDial(entry, conn, parent)
          catch
            case e: RejectedExecutionException =>
              liveConnections.remove(conn)
              entry.connection.compareAndSet(conn, null)
              throw e
          result = conn
    result

  /** Dials `conn` on its loop. An IP literal is used as is. A host name is resolved on `resolverPool` first, never
    * on the caller (which may hold `MetadataManager`'s lock) or a loop; meanwhile `conn` collects messages in its
    * pre-handshake queue. A failed resolution, or one that takes longer than the handshake timeout, fails the
    * handshake: queued messages are dropped and barriers completed.
    *
    * @throws RejectedExecutionException if the loop or the resolver pool has shut down
    */
  private def startDial(entry: QuicHostEntry, conn: QuicConnection, parent: Channel): Unit =
    val loop = parent.eventLoop()
    ipLiteral(entry.address) match
      case Some(ip) =>
        val address = new InetSocketAddress(ip, entry.dataPort)
        loop.execute(() => dial(entry, conn, parent, address))
      case None =>
        val name = entry.address
        val guard = loop.schedule((() =>
          conn.handshakeFailed(new TimeoutException(s"resolving QUIC host address $name timed out"))): Runnable,
          config.handshakeTimeout.toMillis, TimeUnit.MILLISECONDS)
        try
          resolverPool.execute: () =>
            val resolved =
              try Right(new InetSocketAddress(resolveHost(name), entry.dataPort))
              catch case NonFatal(e) => Left(e)
            try
              loop.execute: () =>
                guard.cancel(false)
                resolved match
                  case Right(address) => dial(entry, conn, parent, address)
                  case Left(e) => conn.handshakeFailed(e)
            catch case _: RejectedExecutionException => () // the loop is gone and with it the connection
        catch
          case e: RejectedExecutionException =>
            guard.cancel(false)
            throw e

  // ---- Loop side -----------------------------------------------------------------------------------------------

  private def dial(entry: QuicHostEntry, conn: QuicConnection, parent: Channel, address: InetSocketAddress): Unit =
    if !conn.isClosed then
      connect(entry, conn, parent, address)

  /** Connects `conn` to `address`. With `config.verifyPeerIdentity`, a peer whose certificate is not for `entry`'s
    * HostId is never attached, so nothing queued behind the handshake reaches it: see `markWrong`.
    */
  private def connect(entry: QuicHostEntry, conn: QuicConnection, parent: Channel, address: InetSocketAddress): Unit =
    val handler = new ConnectionHandler(conn, inbound = false)
    try
      val f = QuicChannel.newBootstrap(parent)
        .handler(handler)
        .streamHandler(inboundStreamInitializer)
        .remoteAddress(address)
        .connect()
      val onTimeout: Runnable = () =>
        if !f.isDone then
          if handler.channel != null then
            handler.channel.close()
          conn.handshakeFailed(new TimeoutException(s"QUIC handshake with $address timed out"))
      val guard = conn.loop.schedule(onTimeout, config.handshakeTimeout.toMillis, TimeUnit.MILLISECONDS)
      f.addListener: (r: NFuture[QuicChannel]) =>
        guard.cancel(false)
        if r.isSuccess then
          val ch = r.getNow
          val peerId = if config.verifyPeerIdentity then peerCertificateId(ch) else Some(entry.hostId.uuid)
          if peerId.contains(entry.hostId.uuid) then
            conn.attach(ch)
          else
            ch.close()
            markWrong(entry, s"its certificate is for ${peerId.getOrElse("no UUID")}")
            conn.close()
        else
          conn.handshakeFailed(r.cause())
    catch
      case NonFatal(e) => conn.handshakeFailed(e)

  /** On the connection's loop, once an outbound connection is up: starts keepalive, which is driven by the dialing
    * side only, after `idleTimeout / 2` without an outbound write, and stops the host's failure clock.
    *
    * A connection to an entry already marked known-wrong is closed instead. A sender that passed `sendToHost`'s
    * `knownWrong` check just before `markWrong` can dial after `markWrong` closed the old connection. Nothing else
    * would close that connection: a `WrongHost` reply on it finds the entry already marked, and keepalive would hold
    * it open until the entry is replaced.
    */
  private def outboundActive(entry: QuicHostEntry, conn: QuicConnection): Unit =
    if entry.knownWrong then
      conn.close()
    else
      conn.startKeepalive(idleTimeout / 2)
      if isCurrentEntry(entry) then
        failingSince.remove(entry.hostId)

  /** On the connection's loop. A dial that closed before its handshake completed starts the host's failure clock,
    * unless `entry` has since been replaced: the replacement is a new address, with a clock of its own.
    */
  private def outboundClosed(entry: QuicHostEntry, conn: QuicConnection): Unit =
    entry.connection.compareAndSet(conn, null)
    if !conn.isActive && isCurrentEntry(entry) then
      failingSince.putIfAbsent(entry.hostId, System.nanoTime())
    connectionClosed(conn)

  /** The UUID of `ch`'s peer certificate, if it has one */
  private def peerCertificateId(ch: QuicChannel): Option[UUID] =
    try
      ch.sslEngine().getSession.getPeerCertificates.headOption.flatMap:
        case c: X509Certificate => CertificateVerifier.extractId(c)
        case _ => None
    catch
      case NonFatal(e) =>
        logger.debug(s"QUIC peer certificate of ${ch.remoteSocketAddress()} unreadable: $e")
        None

  private def isCurrentEntry(entry: QuicHostEntry): Boolean =
    metadataManager.peekHostEntry(entry.hostId).exists(_ eq entry)

  private def inboundClosed(conn: QuicConnection): Unit =
    inboundConnections.remove(conn)
    connectionClosed(conn)

  private def connectionClosed(conn: QuicConnection): Unit =
    liveConnections.remove(conn)
    lastWrongHostReply.remove(conn)
    clientRouteMap.forEach: (id, c) =>
      if c eq conn then
        clientRouteMap.remove(id, c)
    clusterRouteMap.forEach: (id, c) =>
      if c eq conn then
        clusterRouteMap.remove(id, c)
    subscribers.forEach: (id, s) =>
      if s.conn eq conn then
        subscribers.remove(id, s)

  /** Closes `entry`'s outbound connection, if open (and, with `establishedOnly`, past its handshake), on its own
    * loop, so the next send re-dials
    */
  private def closeOutbound(entry: QuicHostEntry, establishedOnly: Boolean = false): Unit =
    val out = entry.connection.get()
    if out != null && !out.isClosed && (!establishedOnly || out.isActive) then
      if out.loop.inEventLoop() then
        out.close()
      else
        try out.loop.execute(() => out.close())
        catch case _: RejectedExecutionException => () // shutting down

  /** On a loop. Hands scalecube traffic to membership (host mode only), after recording `conn` as the route our
    * scalecube traffic to the sender goes back over (see `sendClusterMessage`) and re-dialing a restarted peer.
    *
    * Netty's stateless-reset key is random per JVM and its server codec does not send resets, so after a peer
    * restarts our outbound connection to it is black-holed until the idle timeout, and scalecube cannot see the new
    * member until our replies get through. The restarted peer's first message reaches us over its own new
    * connection and carries a new boot id: when a host's boot id differs from the last one recorded, our outbound
    * connection to it is closed so the next send re-dials. Connection churn never changes a boot id, so an ordinary
    * re-dial is never mistaken for a restart. The first boot id seen from a host is only recorded.
    *
    * Only an established connection is closed. One still handshaking is not black-holed: the restarted peer answers
    * its retransmitted Initial. Closing it would only drop the messages queued behind its handshake.
    */
  private def clusterMessage(conn: QuicConnection, m: ClusterTransportMessage): Unit =
    membership.foreach: hm =>
      if conn != null && !conn.isClosed && (clusterRouteMap.get(m.fromHost) ne conn) then
        clusterRouteMap.put(m.fromHost, conn)
      val previous = bootIds.put(m.fromHost, m.fromBootId)
      if previous != null && previous != m.fromBootId then
        // Deliberately close-only: the entry is kept. Dropping it would force a host-object re-read, which can park
        // forever behind the restarted host's own bootstrap stores (the read's messages to them wait on that very
        // lookup). A host whose endpoint really moved rejoins over its own connection (see `sendClusterMessage`)
        // and gossips HostEndpointChanged, which replaces the entry.
        metadataManager.peekHostEntry(m.fromHost).foreach: entry =>
          logger.info(s"Host ${entry.name} restarted (new boot id); re-dialing it")
          closeOutbound(entry, establishedOnly = true)
      hm.receive(m)

  // ---- Peer endpoint query ------------------------------------------------------------------------------------

  /** On `loops(0)`, every `peerQueryInterval`. First clears the known-wrong mark of each current entry marked at
    * least `knownWrongRetryAfter` ago, so the next send re-dials it (see `markWrong`). Then queries about each host
    * reported online whose dials have been failing for at least `peerQueryDelay`, and about each host whose entry is
    * still known-wrong whatever its failure clock and online state, unless it was asked about within the last
    * interval. Scheduling jitter is allowed for, so a host still failing (or still known-wrong) is asked about on
    * every scan.
    */
  private def scanForPeerQueries(): Unit =
    if !shuttingDown then
      try
        val now = System.nanoTime()
        val delay = config.peerQueryDelay.toNanos
        val interval = config.peerQueryInterval.toNanos
        val retryAfter = config.knownWrongRetryAfter.toNanos
        val failing = failingSince.asScala.collect:
          case (h, since) if now - since >= delay && reportedOnline(h) => h
        metadataManager.resolvedHostEntries.foreach: entry =>
          if entry.knownWrongSince.exists(t => now - t >= retryAfter) && isCurrentEntry(entry) then
            logger.info(s"Retrying host ${entry.name} at ${entry.address}:${entry.dataPort}, known-wrong for " +
              s"${config.knownWrongRetryAfter}")
            entry.clearKnownWrong()
        val knownWrong = metadataManager.resolvedHostEntries.filter(_.knownWrong).map(_.hostId)
        val due = (failing.toSet ++ knownWrong).filter: h =>
          Option(lastQueried.get(h)).forall(t => now - t >= interval - interval / 10)
        due.toList.foreach(sendQuery)
      catch
        case NonFatal(e) => logger.warn(s"Peer endpoint query scan failed: $e")

  /** Starts an immediate query about `hostId`, whatever its failure clock and online state. Any thread. */
  private[quic] def requestPeerQuery(hostId: HostId): Unit =
    if !shuttingDown then
      try loops(0).execute(() => sendQuery(hostId))
      catch case _: RejectedExecutionException => () // shutting down

  /** On `loops(0)`. Sends `HostEndpointQuery(target)` to the peers `queryPeers` picks. */
  private def sendQuery(target: HostId): Unit =
    if !shuttingDown then
      try
        lastQueried.put(target, System.nanoTime())
        val peers = queryPeers(target)
        logger.debug(s"Dials to host $target are failing; asking ${peers.size} peers for its endpoint")
        peers.foreach: peer =>
          peerQueriesSent.incrementAndGet()
          sendToHostId(peer, HostEndpointQuery(target))
      catch
        case NonFatal(e) => logger.warn(s"Peer endpoint query about host $target failed: $e")

  /** On `loops(0)`, which owns `relays`. Up to `peerQueryFanout` random peers to ask about `target`, never `target`
    * itself. Host mode: hosts membership reports online, ourselves excluded; those we hold a resolved entry for come
    * first, and the rest only top up, since a send to an unresolved host starts a host object read. Client mode: our
    * relays first (the hosts we already hold connections to), topped up with other resolved hosts our relays report
    * online.
    */
  private def queryPeers(target: HostId): List[HostId] =
    val fanout = config.peerQueryFanout
    membership match
      case Some(m) =>
        val self = ohost.map(_._1)
        val online = Random.shuffle(m.onlineHosts.filterNot(h => h == target || self.contains(h)).toList)
        val (resolved, unresolved) = online.partition(h => metadataManager.peekHostEntry(h).isDefined)
        (resolved ++ unresolved).take(fanout)
      case None =>
        val fromRelays = Random.shuffle((relays - target).toList).take(fanout)
        val topUp = metadataManager.resolvedHostEntries.map(_.hostId)
          .filter(h => h != target && !fromRelays.contains(h) && reportedOnline(h))
        (fromRelays ++ Random.shuffle(topUp)).take(fanout)

  /** On `conn`'s loop. Answers from `MetadataManager` alone, never starting a lookup. A query with nowhere to send
    * the answer (from `dispatchForTests`) is ignored.
    */
  private def endpointQuery(conn: QuicConnection, m: HostEndpointQuery): Unit =
    if conn != null && !conn.isClosed then
      val answer = HostEndpointAnswer(m.hostId, metadataManager.endpointAnswer(m.hostId))
      conn.submit(QuicMessageCodec.encode(answer, PooledByteBufAllocator.DEFAULT))

  /** On `conn`'s loop. An answer newer than our entry is applied as `HostEndpointChanged` gossip would be, which
    * also writes it through to the network cache. An unknown answer, or one no newer than our entry, is ignored.
    *
    * Only a solicited answer is considered: we must have asked about `m.hostId` within the last two
    * `peerQueryInterval`s, and it must arrive on one of our outbound connections, which is where our queries go (in
    * client mode every connection is). Newer-wins makes an applied answer permanent, so an unsolicited one, from a
    * client or a buggy peer, with a far-future timestamp would otherwise displace every later correction. An answer
    * from `dispatchForTests` (no connection) is considered as if solicited.
    */
  private def endpointAnswer(conn: QuicConnection, m: HostEndpointAnswer): Unit =
    val solicited = conn == null || (!inboundConnections.contains(conn) && queryOutstanding(m.hostId))
    if !solicited then
      logger.debug(s"Ignoring an unsolicited endpoint answer about host ${m.hostId}")
    else
      m.endpoint.foreach: e =>
        if e.hostId == m.hostId && metadataManager.hostTimestamp(e.hostId).forall(_ < e.timestamp) then
          endpointChanged(e)

  /** Whether `hostId` was asked about within the last two `peerQueryInterval`s */
  private def queryOutstanding(hostId: HostId): Boolean =
    Option(lastQueried.get(hostId)).exists(t => System.nanoTime() - t <= 2 * config.peerQueryInterval.toNanos)

  // ---- Wrong-host detection ----------------------------------------------------------------------------------

  /** Any thread. `entry`'s address belongs to another host: unless the entry has been replaced (or is already
    * marked), marks it known-wrong, so sends through it are dropped, closes its connection and queries its peers at
    * once. The scan keeps querying every interval until an answer replaces the entry, or until
    * `knownWrongRetryAfter` has passed: the scan then clears the mark, and the next send re-dials the same address
    * (the address may have been reused only briefly, and peers answer unknown for a bootstrap host). If it is still
    * wrong, the TLS check or a `WrongHost` reply marks it again.
    */
  private def markWrong(entry: QuicHostEntry, why: String): Unit =
    if !entry.knownWrong && isCurrentEntry(entry) && !ohost.exists(_._1 == entry.hostId) then
      logger.warn(s"Address ${entry.address}:${entry.dataPort} of host ${entry.name} belongs to another host ($why); " +
        "dropping messages to it until a peer knows its endpoint")
      entry.markKnownWrong()
      closeOutbound(entry)
      requestPeerQuery(entry.hostId)

  /** Host mode, on `conn`'s loop. `msg` was meant for `intended`, not us: dropped, with a `WrongHost` reply unless
    * this connection carried one within `wrongHostReplyInterval`.
    */
  private def misaddressed(conn: QuicConnection, intended: HostId, ourId: HostId, msg: Message): Unit =
    logger.debug(s"Dropping ${msg.getClass.getSimpleName} meant for host $intended")
    if conn != null && !conn.isClosed then
      val now = System.nanoTime()
      val last = lastWrongHostReply.get(conn)
      if last == null || now - last >= config.wrongHostReplyInterval.toNanos then
        lastWrongHostReply.put(conn, now)
        wrongHostRepliesSent.incrementAndGet()
        conn.submit(QuicMessageCodec.encode(WrongHost(intended, ourId), PooledByteBufAllocator.DEFAULT))

  /** On `conn`'s loop. Only a reply over the intended host's current connection speaks for its current address; one
    * arriving after the entry was repaired, or over another connection, is ignored.
    */
  private def wrongHostReceived(conn: QuicConnection, m: WrongHost): Unit =
    metadataManager.peekHostEntry(m.intendedHostId).foreach: entry =>
      if conn == null || (entry.connection.get() eq conn) then
        markWrong(entry, s"host ${m.actualHostId} answers there")

  // ---- Gossip relaying -----------------------------------------------------------------------------------------

  /** Host mode, any thread. Sends `msg` to every subscriber whose lease is current, forgetting the lapsed ones. */
  private def forwardToSubscribers(msg: Message): Unit =
    if !shuttingDown then
      val now = System.nanoTime()
      subscribers.forEach: (id, s) =>
        if s.conn.isClosed || now - s.expiresNanos >= 0 then
          subscribers.remove(id, s)
        else
          forward(s.conn, msg)

  /** On a stream, whatever its size: a lost datagram would leave the client unaware until the next retransmit */
  private def forward(conn: QuicConnection, msg: Message): Unit =
    try conn.submit(QuicMessageCodec.encode(msg, PooledByteBufAllocator.DEFAULT, streamOnly = true))
    catch case NonFatal(e) => logger.warn(s"QUIC forward of ${msg.getClass.getSimpleName} to a client failed: $e")

  /** Host mode, on `conn`'s loop. Starts or renews `m.fromClient`'s lease. A subscription that is new, or renewed
    * over a different connection or after lapsing, starts with a snapshot of the hosts we see online, ourselves
    * included, and of the store availability we hold.
    */
  private def subscribe(conn: QuicConnection, m: GossipSubscribe): Unit =
    for hm <- membership; (hostId, _) <- ohost do
      if conn != null && !conn.isClosed then
        val now = System.nanoTime()
        val previous = subscribers.put(m.fromClient, Subscription(conn, now + config.gossip.relayLease.toNanos))
        if previous == null || (previous.conn ne conn) || now - previous.expiresNanos >= 0 then
          forward(conn, HostOnlineStatus(hostId :: hm.onlineHosts.toList, Nil, storeAvailability.snapshot))

  /** Client mode, on `loops(0)`. Keeps `relayCount` relays among the hosts we have resolved and renews each one's
    * subscription. With `rotate`, one relay is first swapped for a host not currently a relay, when there is one.
    * New relays are subscribed before a dropped one's lease lapses, so coverage never gaps. Relays are picked at
    * random, hosts reported online first.
    */
  private def renewRelays(rotate: Boolean): Unit =
    if !shuttingDown then
      try
        val known = metadataManager.resolvedHostEntries
        val count = config.gossip.relayCount
        var kept = relays.intersect(known.map(_.hostId).toSet)
        val candidates = Random.shuffle(known.filterNot(e => relays.contains(e.hostId))).sortBy(e => !e.isOnline)
        if rotate && kept.nonEmpty && kept.size >= count && candidates.nonEmpty then
          kept -= Random.shuffle(kept.toList).head
        relays = kept ++ candidates.take(count - kept.size).map(_.hostId)
        relays.foreach(sendToHostId(_, GossipSubscribe(clientId)))
      catch
        case NonFatal(e) => logger.warn(s"Gossip relay renewal failed: $e")

  /** Client mode, on a loop. Applies a relay's liveness report; a relay reported offline is replaced at once. */
  private def hostOnlineStatus(m: HostOnlineStatus): Unit =
    storeAvailability.merge(m.stores)
    m.online.foreach: h =>
      relayedOnline.add(h)
      metadataManager.peekHostEntry(h).foreach(_.setOnline())
    m.offline.foreach: h =>
      relayedOnline.remove(h)
      metadataManager.peekHostEntry(h).foreach(_.setOffline())
    if m.offline.nonEmpty then
      try
        loops(0).execute: () =>
          if m.offline.exists(relays.contains) then
            relays --= m.offline
            renewRelays(rotate = false)
      catch case _: RejectedExecutionException => () // shutting down

  private def recordRoute(id: ClientId, conn: QuicConnection): Unit =
    if conn != null && !conn.isClosed && (clientRouteMap.get(id) ne conn) then
      clientRouteMap.put(id, conn)

  /** Runs on `conn`'s loop; a throwing handler is logged, never propagated.
    * Host mode: a message whose envelope names another host is dropped (see `misaddressed`).
    */
  private def dispatch(conn: QuicConnection, received: QuicMessageCodec.Received): Unit =
    val msg = received.msg
    try
      val wrongFor = for
        (ourId, _) <- ohost
        intended <- received.toHost if intended != ourId
      yield (intended, ourId)
      wrongFor match
        case Some((intended, ourId)) => misaddressed(conn, intended, ourId, msg)
        case None => deliver(conn, msg)
    catch
      case NonFatal(e) => logger.error(s"Error dispatching ${msg.getClass.getSimpleName}: $e", e)

  private def deliver(conn: QuicConnection, msg: Message): Unit =
    msg match
      case req: ClientRequest =>
        recordRoute(req.fromClient, conn)
        messageHandler.onClientRequestReceived(req)

      case resp: ClientResponse =>
        messageHandler.onClientResponseReceived(resp)

      case tx: TxMessage =>
        messageHandler.onTransactionMessageReceived(tx)

      case hm: HostMessage =>
        messageHandler.onHostMessageReceived(hm)

      case m: ClusterTransportMessage =>
        clusterMessage(conn, m)

      case m: GossipSubscribe =>
        subscribe(conn, m)

      case g: Gossip =>
        // Forwarded by a relay. A host takes gossip only from membership, which also carries it between hosts.
        if membership.isEmpty then
          applyGossip(g)

      case m: HostOnlineStatus =>
        if membership.isEmpty then
          hostOnlineStatus(m)

      case _: HostResponse | _: HostHeartbeat =>
        // Not handled: liveness comes from host membership

      case m: HostEndpointQuery =>
        endpointQuery(conn, m)

      case m: HostEndpointAnswer =>
        endpointAnswer(conn, m)

      case m: WrongHost =>
        wrongHostReceived(conn, m)
