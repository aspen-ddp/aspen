package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.{Channel, ChannelFuture, ChannelHandler, ChannelHandlerContext, ChannelInboundHandlerAdapter,
  ChannelInitializer, ChannelOption, MultiThreadIoEventLoopGroup}
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.quic.{QuicChannel, QuicChannelBootstrap, QuicClientCodecBuilder,
  QuicServerCodecBuilder, QuicSslContext, QuicStreamChannel, QuicStreamType}
import io.netty.handler.ssl.SslHandshakeCompletionEvent
import org.aspen_ddp.aspen.common.network.Message
import org.aspen_ddp.aspen.common.util.DaemonThreads

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.duration.{Duration, SECONDS}

/** A minimal raw Netty QUIC peer on loopback, built from `QuicTls` and `QuicConfig`. Test-only.
  *
  * A peer is either a server (`QuicTestPeer.server`, bound to a free loopback port, see `localAddress`) or a client
  * (`QuicTestPeer.client`, bound to an ephemeral port, dials with `connect` or `bootstrap`). Each peer owns a one
  * thread daemon event loop group; `close()` shuts it down.
  *
  * Both kinds record, on every connection they own:
  *  - `received`: each decoded `Message` with the `Transport` it arrived on. Datagrams are decoded with
  *    `QuicMessageCodec.decodeDatagram`, inbound streams with one `StreamFrameDecoder` each.
  *  - `handshakes`: one `HandshakeOutcome` per connection, from Netty's `SslHandshakeCompletionEvent`.
  *  - `protocolErrors`: decode failures reported by either decoder.
  *  - `connections`: every `QuicChannel` whose handshake succeeded.
  *
  * `stallStreams = true` sets `autoRead = false` on inbound streams, so nothing is read from them and QUIC flow
  * control fills. `unstallStreams()` resumes them. `inboundStreams` records every inbound stream channel.
  * `config` overrides any `QuicConfig` setting; `idle` is the QUIC idle timeout used for both codecs
  * (`config.idleTimeout`, else `DefaultIdle`).
  *
  * Sending helpers (`sendDatagram`, `sendStream`) work on any `QuicChannel`, including ones dialed by other code.
  * Use `bootstrap(remote)` to get a client `QuicChannelBootstrap` preconfigured with this peer's recording handlers,
  * which callers may override with their own `handler`/`streamHandler` before calling `connect()`.
  */
final class QuicTestPeer private (val isServer: Boolean,
                                  sslContext: QuicSslContext,
                                  val config: QuicConfig,
                                  initialStall: Boolean):
  import QuicTestPeer.*

  val received: LinkedBlockingQueue[Received] = new LinkedBlockingQueue[Received]()
  val handshakes: LinkedBlockingQueue[HandshakeOutcome] = new LinkedBlockingQueue[HandshakeOutcome]()
  val protocolErrors: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
  val connections: ConcurrentLinkedQueue[QuicChannel] = new ConcurrentLinkedQueue[QuicChannel]()
  val inboundStreams: ConcurrentLinkedQueue[QuicStreamChannel] = new ConcurrentLinkedQueue[QuicStreamChannel]()

  /** Applies to streams opened after it is set; use `unstallStreams()` to resume streams already stalled */
  @volatile var stallStreams: Boolean = initialStall

  val idle: Duration = config.idleTimeout.getOrElse(DefaultIdle)

  private val group = new MultiThreadIoEventLoopGroup(1, DaemonThreads.factory("quic-test-peer"),
    NioIoHandler.newFactory())

  /** The UDP channel carrying this peer's QUIC codec */
  val channel: Channel =
    val codec =
      if isServer then
        config.applyTo(new QuicServerCodecBuilder(), idle)
          .sslContext(sslContext)
          .handler(connectionHandler)
          .streamHandler(inboundStreamInitializer)
          .build()
      else
        config.applyTo(new QuicClientCodecBuilder(), idle)
          .sslContext(sslContext)
          .build()
    try
      new Bootstrap().group(group).channel(classOf[NioDatagramChannel]).handler(codec)
        .bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0)).sync().channel()
    catch
      case t: Throwable =>
        group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS)
        throw t

  def localAddress: InetSocketAddress = channel.localAddress().asInstanceOf[InetSocketAddress]

  /** Recording handler for a `QuicChannel` (connection). Datagrams arrive as `ByteBuf` reads. */
  def connectionHandler: ChannelHandler = new ChannelInboundHandlerAdapter:
    override def isSharable: Boolean = true

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit =
      msg match
        case buf: ByteBuf =>
          try
            QuicMessageCodec.decodeDatagram(buf) match
              case Right(r) => received.add(Received(r.msg, Transport.Datagram))
              case Left(err) => protocolErrors.add(err)
          finally
            buf.release()
        case other => ctx.fireChannelRead(other)

    override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit =
      evt match
        case e: SslHandshakeCompletionEvent =>
          if e.isSuccess then
            connections.add(ctx.channel().asInstanceOf[QuicChannel])
            handshakes.add(HandshakeOutcome.Succeeded)
          else
            handshakes.add(HandshakeOutcome.Failed(e.cause()))
        case _ => ()
      ctx.fireUserEventTriggered(evt)

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = ()

  /** Installs a `StreamFrameDecoder` (recording into `received`) on each inbound stream */
  def inboundStreamInitializer: ChannelHandler = new ChannelInitializer[QuicStreamChannel]:
    override def initChannel(ch: QuicStreamChannel): Unit =
      inboundStreams.add(ch)
      if stallStreams then
        ch.config().setAutoRead(false)
      val id = ch.streamId()
      ch.pipeline().addLast(new StreamFrameDecoder(
        r => received.add(Received(r.msg, Transport.Stream(id))),
        err => protocolErrors.add(err)))

  /** Stops stalling: new inbound streams read normally and every stalled stream resumes reading */
  def unstallStreams(): Unit =
    stallStreams = false
    inboundStreams.forEach: ch =>
      ch.eventLoop().execute(() => ch.config().setAutoRead(true))

  /** A client bootstrap to `remote` with this peer's codec settings and recording handlers. Client peers only. */
  def bootstrap(remote: InetSocketAddress): QuicChannelBootstrap =
    require(!isServer, "bootstrap is only available on client peers")
    QuicChannel.newBootstrap(channel)
      .handler(connectionHandler)
      .streamHandler(inboundStreamInitializer)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Integer.valueOf(config.handshakeTimeout.toMillis.toInt))
      .remoteAddress(remote)

  /** Dials `remote` and waits up to `AwaitMillis` for the client side of the handshake */
  def connect(remote: InetSocketAddress): Either[Throwable, QuicChannel] =
    val f = bootstrap(remote).connect()
    if !f.await(AwaitMillis) then
      f.cancel(false)
      Left(new java.util.concurrent.TimeoutException(s"connect to $remote timed out"))
    else if f.isSuccess then
      Right(f.getNow)
    else
      Left(f.cause())

  def awaitReceived(): Received =
    val r = received.poll(AwaitMillis, TimeUnit.MILLISECONDS)
    if r == null then
      throw new AssertionError(s"no message received within ${AwaitMillis}ms")
    r

  def awaitHandshake(): HandshakeOutcome =
    val h = handshakes.poll(AwaitMillis, TimeUnit.MILLISECONDS)
    if h == null then
      throw new AssertionError(s"no handshake outcome within ${AwaitMillis}ms")
    h

  def close(): Unit =
    channel.close().awaitUninterruptibly(AwaitMillis)
    group.shutdownGracefully(0, AwaitMillis, TimeUnit.MILLISECONDS).awaitUninterruptibly(AwaitMillis)

object QuicTestPeer:
  val AwaitMillis: Long = 5000
  val DefaultIdle: Duration = Duration(10, SECONDS)

  enum Transport:
    case Datagram
    case Stream(streamId: Long)

  final case class Received(message: Message, transport: Transport)

  enum HandshakeOutcome:
    case Succeeded
    case Failed(cause: Throwable)

  def server(sslContext: QuicSslContext, config: QuicConfig = QuicConfig(), stallStreams: Boolean = false): QuicTestPeer =
    new QuicTestPeer(true, sslContext, config, stallStreams)

  def client(sslContext: QuicSslContext, config: QuicConfig = QuicConfig(), stallStreams: Boolean = false): QuicTestPeer =
    new QuicTestPeer(false, sslContext, config, stallStreams)

  /** Captures Netty `LEAK:` reports, which otherwise vanish because tests clear scribe's handlers.
    *
    * Netty reports a leak only when a later allocation polls the GC'd reference queue, so `leaks()` runs a GC and
    * allocates tracked buffers before returning what was captured. Use with `ResourceLeakDetector.Level.PARANOID`.
    */
  final class LeakWatch private[QuicTestPeer] ():
    private val reports = new ConcurrentLinkedQueue[String]()
    private val handler = scribe.handler.LogHandler(scribe.Level.Warn): r =>
      val text = r.logOutput.plainText
      if text.contains("LEAK:") then
        reports.add(text)
    scribe.Logger.root.withHandler(handler).replace()

    def leaks(): List[String] =
      var i = 0
      while i < 3 do
        System.gc()
        System.runFinalization()
        var j = 0
        while j < 64 do
          io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer(16).release()
          io.netty.buffer.UnpooledByteBufAllocator.DEFAULT.directBuffer(16).release()
          j += 1
        i += 1
      reports.toArray(new Array[String](0)).toList

    def stop(): Unit = scribe.Logger.root.withoutHandler(handler).replace()

  def watchLeaks(): LeakWatch = new LeakWatch()

  /** Encodes `msg` as one datagram (it must fit `DatagramThreshold`) and writes it to `ch` */
  def sendDatagram(ch: QuicChannel, msg: Message): ChannelFuture =
    val enc = QuicMessageCodec.encode(msg, ch.alloc())
    if !enc.isDatagram then
      enc.release()
      throw new IllegalArgumentException(s"message of ${enc.size} bytes is too large for a datagram")
    ch.writeAndFlush(enc.toDatagram(ch.alloc()))

  /** Opens a new unidirectional stream on `ch`, waits for it, and writes `msg` as one stream frame */
  def sendStream(ch: QuicChannel, msg: Message): QuicStreamChannel =
    val stream = openStream(ch)
    writeFrame(stream, msg)
    stream

  /** Opens a unidirectional stream on `ch`, waiting up to `AwaitMillis` */
  def openStream(ch: QuicChannel): QuicStreamChannel =
    val f = ch.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter)
    if !f.await(AwaitMillis) then
      throw new AssertionError(s"stream not opened within ${AwaitMillis}ms")
    f.get()

  /** Writes `msg` as one stream frame on `stream` */
  def writeFrame(stream: QuicStreamChannel, msg: Message): ChannelFuture =
    val alloc = stream.alloc()
    stream.writeAndFlush(QuicMessageCodec.encode(msg, alloc).toStreamFrame(alloc))
