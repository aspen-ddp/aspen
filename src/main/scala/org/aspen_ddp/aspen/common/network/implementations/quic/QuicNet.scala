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
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{DaemonThreads, EvictingQueue}
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import scribe.Logging

import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, RejectedExecutionException, TimeUnit,
  TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

object QuicNet:
  /** The `QuicConnection` of a `QuicChannel`, set when the channel's pipeline is initialized */
  private val ConnectionKey: AttributeKey[QuicConnection] = AttributeKey.valueOf("aspen-quic-connection")

  private val CloseAwaitMillis: Long = 5000

  private val FlushPollMillis: Long = 25

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

  private class SrvMessenger(net: QuicNet) extends ServerMessenger:
    def sendClientResponse(msg: ClientResponse): Unit = net.sendToClient(msg)

    def sendTransactionMessage(msg: TxMessage): Unit = net.sendToStore(msg.to, msg)

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)

    def dropCacheForStore(storeId: StoreId): Unit = net.metadataManager.dropStoreMapping(storeId)

    /** Through membership, and for an endpoint change also directly to every host this one has resolved: a host
      * whose endpoint really changed cannot take part in membership until its peers re-read its host object, since
      * their replies still go to its old address (see `endpointChangedDirect`)
      */
    override def broadcastGossip(g: Gossip): Unit = net.membership.foreach: m =>
      m.broadcast(g)
      g match
        case h: HostEndpointChanged => net.tellResolvedHosts(h)
        case _: StoreMoved => ()


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
  * Construction order matters: `MetadataManager`'s constructor calls `createHostEntry` for each bootstrap host, so
  * everything the send path uses (loops, channels, maps) is initialized above `metadataManager`. `membership` is
  * created before the channels are bound, since `dispatch` reads it, but is only started at the very end.
  *
  * @param resolveHost resolves a host address that is not an IP literal. It may block: it only runs on the
  *                    plugin's resolver threads, never on a caller or an event loop. Replaceable for tests.
  */
class QuicNet(initialBootstrapConfig: BootstrapConfig.Config,
              onBootstrapConfigRefreshed: (String, BootstrapConfig.Config) => Unit,
              val oclientId: Option[ClientId],
              val ohost: Option[(HostId, Int)],
              val heartbeatPeriod: Duration,
              val messageHandler: MessageHandler,
              tls: TlsCredentials,
              config: QuicConfig = QuicConfig(),
              resolveHost: String => InetAddress = QuicNet.blockingResolve) extends NetworkPlugin with Logging:

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
        m => sendToHostId(m.toHost, m))

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
      if membership.exists(_.isOnline(hostId)) then
        entry.setOnline()
      sendQueued(entry, queuedMessages)
      entry

    def storeResolved(hostEntry: QuicHostEntry, storeId: StoreId, queuedMessages: EvictingQueue[Message]): Unit =
      sendQueued(hostEntry, queuedMessages)

  val metadataManager: MetadataManager[QuicHostEntry] = initOrAbort:
    new MetadataManager[QuicHostEntry](initialBootstrapConfig, onBootstrapConfigRefreshed, networkImpl)

  val clientMessenger: ClientMessenger = new CliMessenger(this)
  val serverMessenger: ServerMessenger = new SrvMessenger(this)

  /** Applies membership events and gossip. Runs on the membership's "aspen-gossip" thread. */
  private val membershipListener = new MembershipListener:
    // Peek, never look up: an entry created later seeds its online state from membership (see createHostEntry)
    override def online(hostId: HostId): Unit = metadataManager.peekHostEntry(hostId).foreach(_.setOnline())

    override def offline(hostId: HostId): Unit = metadataManager.peekHostEntry(hostId).foreach(_.setOffline())

    // Gossip that arrived through membership is never re-broadcast
    override def gossipReceived(g: Gossip): Unit = g match
      case StoreMoved(storeId) => metadataManager.dropStoreMapping(storeId)
      case HostEndpointChanged(hostId) => endpointChanged(hostId)

  /** Drops `hostId`'s entry and closes our outbound connection to it, so the next send re-reads the host object and
    * dials the new endpoint. Our own entry is left alone.
    */
  private def endpointChanged(hostId: HostId): Unit =
    if !ohost.exists(_._1 == hostId) then
      metadataManager.dropHostEntry(hostId).foreach: entry =>
        logger.info(s"Host ${entry.name} changed endpoint; dropping its entry and connection")
        closeOutbound(entry)

  /** Sends `g` as a top-level message to every resolved host but this one. See `SrvMessenger.broadcastGossip`. */
  private def tellResolvedHosts(g: HostEndpointChanged): Unit =
    metadataManager.resolvedHostEntries.foreach: entry =>
      if !ohost.exists(_._1 == entry.hostId) then
        sendToHost(entry, g)

  initOrAbort:
    membership.foreach: m =>
      m.addListener(membershipListener)
      m.start()

  // ---- NetworkPlugin -------------------------------------------------------------------------------------------

  /** The loops started with the group; this only supplies the client used for metadata lookups */
  def startIoThread(client: AspenClient): Unit = metadataManager.setAspenClient(client)

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
    *  1. Every send from now on is silently dropped.
    *  2. Sleeps the full `linger` on the caller's thread, so quiche can transmit what flush handed it.
    *     `QuicConnectionStats` exposes no "unsent bytes" signal, so it cannot end early.
    *  3. On each connection's own loop, closes it with a QUIC application close (code 0, empty reason).
    *  4. Closes the datagram channels.
    *  5. Starts `shutdownGracefully(0, 1s)` on the group. `joinIoThread` returns once it has terminated.
    *
    * Call `awaitPendingMessagesSent` first: anything still in the plugin's own queues is discarded. The close frame
    * is a best-effort courtesy (crash-only). Exceptions are swallowed and logged at trace, as in `ZMQNet`.
    */
  def shutdown(linger: Duration): Unit =
    if shutdownStarted.compareAndSet(false, true) then
      quietly("stop membership"):
        membership.foreach(_.shutdown())
      shuttingDown = true
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

  /** The last boot id recorded for `hostId` */
  private[quic] def lastBootIdOf(hostId: HostId): Option[UUID] = Option(bootIds.get(hostId))

  /** Dispatches `m` as if it had arrived on a connection */
  private[quic] def dispatchForTests(m: Message): Unit = dispatch(null, m)

  /** `shutdown` without the shutting-down flag or the linger, waiting for the group to terminate: stops
    * membership, closes every connection on its loop (which cancels its timers), closes the datagram
    * channels and terminates the group
    */
  private[quic] def closeForTests(): Unit =
    membership.foreach(_.shutdown())
    closeAll(CloseAwaitMillis)
    group.terminationFuture().awaitUninterruptibly(CloseAwaitMillis * 2)

  /** Simulates a crash: stops membership without leaving the cluster (peers must detect the failure), closes the
    * datagram channels first, so no QUIC close frame can reach a peer, then terminates the group. Peers keep their
    * connections to this net until they find out by other means.
    */
  private[quic] def crashForTests(): Unit =
    membership.foreach(_.shutdown())
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
        val conn = connectionFor(entry)
        conn.submit(QuicMessageCodec.encode(msg, PooledByteBufAllocator.DEFAULT))
      catch
        case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to host ${entry.name} failed: $e")

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
        val conn = new QuicConnection(parent.eventLoop(), config, c => outboundClosed(entry, c), outboundActive)
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
        loop.execute(() => dial(conn, parent, address))
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
                  case Right(address) => dial(conn, parent, address)
                  case Left(e) => conn.handshakeFailed(e)
            catch case _: RejectedExecutionException => () // the loop is gone and with it the connection
        catch
          case e: RejectedExecutionException =>
            guard.cancel(false)
            throw e

  // ---- Loop side -----------------------------------------------------------------------------------------------

  private def dial(conn: QuicConnection, parent: Channel, address: InetSocketAddress): Unit =
    if !conn.isClosed then
      connect(conn, parent, address)

  private def connect(conn: QuicConnection, parent: Channel, address: InetSocketAddress): Unit =
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
          conn.attach(r.getNow)
        else
          conn.handshakeFailed(r.cause())
    catch
      case NonFatal(e) => conn.handshakeFailed(e)

  /** On the connection's loop, once an outbound connection is up: starts keepalive, which is driven by the dialing
    * side only, after `idleTimeout / 2` without an outbound write
    */
  private def outboundActive(conn: QuicConnection): Unit =
    conn.startKeepalive(idleTimeout / 2)

  private def outboundClosed(entry: QuicHostEntry, conn: QuicConnection): Unit =
    entry.connection.compareAndSet(conn, null)
    connectionClosed(conn)

  private def inboundClosed(conn: QuicConnection): Unit =
    inboundConnections.remove(conn)
    connectionClosed(conn)

  private def connectionClosed(conn: QuicConnection): Unit =
    liveConnections.remove(conn)
    clientRouteMap.forEach: (id, c) =>
      if c eq conn then
        clientRouteMap.remove(id, c)

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

  /** On a loop. Hands scalecube traffic to membership (host mode only), re-dialing a restarted peer first.
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
  private def clusterMessage(m: ClusterTransportMessage): Unit =
    membership.foreach: hm =>
      val previous = bootIds.put(m.fromHost, m.fromBootId)
      if previous != null && previous != m.fromBootId then
        // Deliberately close-only: the entry is kept. Dropping it would force a host-object re-read, which can park
        // forever behind the restarted host's own bootstrap stores (the read's messages to them wait on that very
        // lookup). A host whose endpoint really moved says so with a direct HostEndpointChanged instead.
        metadataManager.peekHostEntry(m.fromHost).foreach: entry =>
          logger.info(s"Host ${entry.name} restarted (new boot id); re-dialing it")
          closeOutbound(entry, establishedOnly = true)
      hm.receive(m)

  /** On a loop. A `HostEndpointChanged` sent to us directly by the host that moved (host mode only): applied as if
    * gossiped, then gossiped on through our own membership, which (unlike the moved host's) can reach every member.
    */
  private def endpointChangedDirect(g: HostEndpointChanged): Unit =
    membership.foreach: hm =>
      endpointChanged(g.hostId)
      hm.broadcast(g)

  private def recordRoute(id: ClientId, conn: QuicConnection): Unit =
    if conn != null && !conn.isClosed && (clientRouteMap.get(id) ne conn) then
      clientRouteMap.put(id, conn)

  /** Mirrors `ZMQNet.decodeAndDispatch`. Runs on `conn`'s loop; a throwing handler is logged, never propagated. */
  private def dispatch(conn: QuicConnection, msg: Message): Unit =
    try
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
          clusterMessage(m)

        case g: HostEndpointChanged =>
          endpointChangedDirect(g)

        case _: HostResponse | _: HostHeartbeat | _: StoreMoved =>
          // Not handled: liveness comes from host membership, and StoreMoved only travels inside scalecube payloads
    catch
      case NonFatal(e) => logger.error(s"Error dispatching ${msg.getClass.getSimpleName}: $e", e)
