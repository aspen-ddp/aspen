package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import io.netty.channel.{Channel, ChannelFuture, ChannelHandler, ChannelHandlerContext, ChannelInboundHandlerAdapter,
  ChannelInitializer, EventLoop, IoHandlerFactory, MultiThreadIoEventLoopGroup}
import io.netty.channel.epoll.{Epoll, EpollDatagramChannel, EpollIoHandler}
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.channel.unix.UnixChannelOption
import io.netty.handler.codec.quic.{QuicChannel, QuicClientCodecBuilder, QuicCodecDispatcher, QuicConnectionIdGenerator,
  QuicServerCodecBuilder, QuicSslContext, QuicStreamChannel}
import io.netty.util.AttributeKey
import io.netty.util.concurrent.Future as NFuture
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{DaemonThreads, EvictingQueue}
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import scribe.Logging

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, RejectedExecutionException, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

object QuicNet:
  /** The `QuicConnection` of a `QuicChannel`, set when the channel's pipeline is initialized */
  private val ConnectionKey: AttributeKey[QuicConnection] = AttributeKey.valueOf("aspen-quic-connection")

  private val CloseAwaitMillis: Long = 5000

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


/** A `NetworkPlugin` over Netty QUIC with mutual TLS.
  *
  * Threading (see the design's Threading section):
  *  - One `MultiThreadIoEventLoopGroup` of `config.eventLoopThreads` daemon loops. Each loop owns one client datagram
  *    channel, and in host mode one server datagram channel (all bound to `dataPort` with `SO_REUSEPORT` and a shared
  *    `QuicCodecDispatcher`; without epoll, a single server channel on one loop).
  *  - A connection lives on the loop of the datagram channel it runs over, for its whole life, together with its
  *    streams, decoders and `QuicConnection` state. All receive-side work and every `MessageHandler` call run there.
  *  - Sends run on the caller's thread up to `QuicConnection.submit`: destination lookup, connection get-or-dial
  *    (CAS on `QuicHostEntry.connection`; only the winner dials), address resolution and encoding.
  *  - Shared state touched from both sides is lock-free: `clientRouteMap` and `liveConnections` are concurrent
  *    maps, `QuicHostEntry.connection` is an `AtomicReference`.
  *
  * Construction order matters: `MetadataManager`'s constructor calls `createHostEntry` for each bootstrap host, so
  * everything the send path uses (loops, channels, maps) is initialized above `metadataManager`.
  */
class QuicNet(initialBootstrapConfig: BootstrapConfig.Config,
              onBootstrapConfigRefreshed: (String, BootstrapConfig.Config) => Unit,
              val oclientId: Option[ClientId],
              val ohost: Option[(HostId, Int)],
              val heartbeatPeriod: Duration,
              val messageHandler: MessageHandler,
              tls: TlsCredentials,
              config: QuicConfig = QuicConfig()) extends NetworkPlugin with Logging:

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

  private val keepaliveCount = new AtomicLong(0)
  private val undecodableCount = new AtomicLong(0)

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
    b.bind(address).sync().channel()

  private val (clientChannels, serverChannels): (Array[Channel], Array[Channel]) =
    try
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
    catch
      case t: Throwable =>
        group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS)
        throw t

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
      sendQueued(entry, queuedMessages)
      entry

    def storeResolved(hostEntry: QuicHostEntry, storeId: StoreId, queuedMessages: EvictingQueue[Message]): Unit =
      sendQueued(hostEntry, queuedMessages)

  val metadataManager: MetadataManager[QuicHostEntry] =
    new MetadataManager[QuicHostEntry](initialBootstrapConfig, onBootstrapConfigRefreshed, networkImpl)

  private val livenessTask: NFuture[?] =
    val ms = math.max(1L, heartbeatPeriod.toMillis)
    loops(0).scheduleAtFixedRate((() => livenessTick()): Runnable, ms, ms, TimeUnit.MILLISECONDS)

  val clientMessenger: ClientMessenger = new CliMessenger(this)
  val serverMessenger: ServerMessenger = new SrvMessenger(this)

  // ---- NetworkPlugin -------------------------------------------------------------------------------------------

  /** The loops started with the group; this only supplies the client used for metadata lookups */
  def startIoThread(client: AspenClient): Unit = metadataManager.setAspenClient(client)

  def joinIoThread(): Unit = group.terminationFuture().awaitUninterruptibly()

  def awaitPendingMessagesSent(timeout: Duration): Boolean =
    throw new NotImplementedError("QuicNet.awaitPendingMessagesSent is not implemented yet")

  def shutdown(linger: Duration): Unit =
    throw new NotImplementedError("QuicNet.shutdown is not implemented yet")

  // ---- Test accessors ------------------------------------------------------------------------------------------

  private[quic] def connectionCount: Int = liveConnections.size

  private[quic] def inboundConnectionCount: Int = inboundConnections.size

  private[quic] def outboundConnectionCount: Int = liveConnections.asScala.count(c => !inboundConnections.contains(c))

  private[quic] def clientRoutes: Int = clientRouteMap.size

  private[quic] def inboundConnectionLoops: Set[EventLoop] = inboundConnections.asScala.map(_.loop).toSet

  /** Keepalive datagrams received and discarded */
  private[quic] def keepalivesReceived: Long = keepaliveCount.get()

  /** Datagrams that failed to decode */
  private[quic] def undecodableDatagrams: Long = undecodableCount.get()

  /** Cancels the liveness tick, closes every connection on its loop (which cancels its timers), closes the
    * datagram channels and terminates the group
    */
  private[quic] def closeForTests(): Unit =
    livenessTask.cancel(false)
    liveConnections.toArray(new Array[QuicConnection](0)).foreach: conn =>
      try conn.loop.submit((() => conn.close()): Runnable).awaitUninterruptibly(CloseAwaitMillis)
      catch case _: RejectedExecutionException => ()
    (serverChannels ++ clientChannels).foreach(_.close().awaitUninterruptibly(CloseAwaitMillis))
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS).awaitUninterruptibly(CloseAwaitMillis * 2)

  // ---- Send path (caller threads) ------------------------------------------------------------------------------

  private def sendQueued(entry: QuicHostEntry, queued: EvictingQueue[Message]): Unit =
    var omsg = queued.dequeue()
    while omsg.isDefined do
      omsg.foreach(sendToHost(entry, _))
      omsg = queued.dequeue()

  private def sendToStore(storeId: StoreId, msg: Message): Unit =
    try
      // None: parked on a lookup; storeResolved or createHostEntry sends it later
      metadataManager.getHostEntryOrQueueMessage(storeId, msg).foreach(sendToHost(_, msg))
    catch
      case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to store $storeId failed: $e")

  private def sendToHostId(hostId: HostId, msg: Message): Unit =
    try
      metadataManager.getHostEntryOrQueueMessage(hostId, msg).foreach(sendToHost(_, msg))
    catch
      case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to host $hostId failed: $e")

  private[quic] def sendToHost(entry: QuicHostEntry, msg: Message): Unit =
    try
      val conn = connectionFor(entry)
      conn.submit(QuicMessageCodec.encode(msg, PooledByteBufAllocator.DEFAULT))
    catch
      case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to host ${entry.name} failed: $e")

  private def sendToClient(msg: ClientResponse): Unit =
    try
      val conn = clientRouteMap.get(msg.toClient)
      if conn == null then
        logger.debug(s"Dropping ${msg.getClass.getSimpleName} for unknown client ${msg.toClient.uuid}")
      else
        conn.submit(QuicMessageCodec.encode(msg, PooledByteBufAllocator.DEFAULT))
    catch
      case NonFatal(e) => logger.warn(s"QUIC send of ${msg.getClass.getSimpleName} to client ${msg.toClient.uuid} failed: $e")

  /** The entry's open connection, or a new one installed by CAS and dialed by whichever caller wins */
  private[quic] def connectionFor(entry: QuicHostEntry): QuicConnection =
    var result: QuicConnection = null
    while result == null do
      val cur = entry.connection.get()
      if cur != null && !cur.isClosed then
        result = cur
      else
        val parent = clientChannels(Math.floorMod(nextDialIndex.getAndIncrement(), clientChannels.length))
        // Resolved here, never on a loop
        val address = new InetSocketAddress(entry.address, entry.dataPort)
        val conn = new QuicConnection(parent.eventLoop(), config, c => outboundClosed(entry, c), outboundActive)
        if entry.connection.compareAndSet(cur, conn) then
          liveConnections.add(conn)
          try
            parent.eventLoop().execute(() => dial(conn, parent, address))
          catch
            case e: RejectedExecutionException =>
              liveConnections.remove(conn)
              entry.connection.compareAndSet(conn, null)
              throw e
          result = conn
    result

  // ---- Loop side -----------------------------------------------------------------------------------------------

  private def dial(conn: QuicConnection, parent: Channel, address: InetSocketAddress): Unit =
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

  /** On the connection's loop, once an outbound connection is up: the initial heartbeat (host mode, matching
    * `ZMQNet`'s heartbeat on dealer creation) and keepalive. Keepalive is driven by the dialing side only, after
    * `idleTimeout / 2` without an outbound write. Periodic heartbeats come from `livenessTick`.
    */
  private def outboundActive(conn: QuicConnection): Unit =
    ohost.foreach: (hostId, _) =>
      conn.submit(QuicMessageCodec.encode(HostHeartbeat(hostId), PooledByteBufAllocator.DEFAULT))
    conn.startKeepalive(idleTimeout / 2)

  /** Every `heartbeatPeriod` on loop 0: the offline check, then (host mode) heartbeats. */
  private def livenessTick(): Unit =
    try
      offlineCheck()
      ohost.foreach((hostId, _) => heartbeatAll(hostId))
    catch
      case NonFatal(e) => logger.warn(s"QUIC liveness tick failed: $e")

  /** Same rule as `ZMQNet.heartbeat`: no heartbeat for 3 heartbeat periods means offline */
  private def offlineCheck(): Unit =
    val threshold = System.nanoTime() - (heartbeatPeriod * 3).toNanos
    metadataManager.resolvedHostEntries.foreach: host =>
      if host.lastHeartbeatTime <= threshold then
        host.setOffline()

  /** Heartbeats every resolved host, as `ZMQNet` does over the dealer it keeps for each. A ZMQ dealer reconnects by
    * itself; here the send dials any host without a live connection, so an idle host pair re-establishes
    * connectivity (and online status) within a heartbeat period of a peer restart.
    *
    * One heartbeat per host per tick: a connection that is not yet active (being dialed now, or still handshaking)
    * sends one from `outboundActive` when it activates, so the tick only makes sure it exists. `isActive` turns
    * true only after `outboundActive` has submitted, so the tick and the activation never both send for the same
    * tick. (The initial heartbeat can still land shortly before the next tick's; that duplicate is harmless.)
    *
    * The local host is not dialed. `ZMQNet` heartbeats its own entry over a loopback dealer, which only marks that
    * entry online; this does the same directly, without a self-connection.
    */
  private def heartbeatAll(hostId: HostId): Unit =
    metadataManager.resolvedHostEntries.foreach: entry =>
      if entry.hostId == hostId then
        entry.heartbeatReceived()
      else
        try
          val conn = connectionFor(entry)
          if conn.isActive then
            conn.submit(QuicMessageCodec.encode(HostHeartbeat(hostId), PooledByteBufAllocator.DEFAULT))
        catch
          case NonFatal(e) => logger.warn(s"QUIC heartbeat to host ${entry.name} failed: $e")

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

  private def recordRoute(id: ClientId, conn: QuicConnection): Unit =
    if conn != null && !conn.isClosed && (clientRouteMap.get(id) ne conn) then
      clientRouteMap.put(id, conn)

  /** Mirrors `ZMQNet.decodeAndDispatch`. Runs on `conn`'s loop; a throwing handler is logged, never propagated. */
  private def dispatch(conn: QuicConnection, msg: Message): Unit =
    try
      msg match
        case hb: HostHeartbeat =>
          metadataManager.getHostEntry(hb.hostId).foreach(_.heartbeatReceived())

        case req: ClientRequest =>
          recordRoute(req.fromClient, conn)
          messageHandler.onClientRequestReceived(req)

        case resp: ClientResponse =>
          messageHandler.onClientResponseReceived(resp)

        case tx: TxMessage =>
          messageHandler.onTransactionMessageReceived(tx)

        case hm: HostMessage =>
          messageHandler.onHostMessageReceived(hm)

        case _: HostResponse =>
          // Not currently handled
    catch
      case NonFatal(e) => logger.error(s"Error dispatching ${msg.getClass.getSimpleName}: $e", e)
