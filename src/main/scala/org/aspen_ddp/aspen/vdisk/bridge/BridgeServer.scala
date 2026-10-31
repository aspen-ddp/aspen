package org.aspen_ddp.aspen.vdisk.bridge

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.epoll.{EpollDomainSocketChannel, EpollIoHandler, EpollServerDomainSocketChannel}
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.unix.DomainSocketAddress
import io.netty.channel.{Channel, ChannelInitializer, MultiThreadIoEventLoopGroup}
import io.netty.util.concurrent.GlobalEventExecutor
import org.aspen_ddp.aspen.common.util.DaemonThreads
import org.aspen_ddp.aspen.vdisk.LockableBlockDevice
import scribe.Logging

import java.io.IOException
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.channels.SocketChannel
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

/** Serves [[LockableBlockDevice]]s to the tcmu-runner plugin over a Unix domain socket */
final class BridgeServer(socketPath: os.Path, openDisk: String => Future[LockableBlockDevice]) extends Logging:

  private var group: MultiThreadIoEventLoopGroup = null
  private var listener: Channel = null
  private val sessions = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  /** Binds the socket. A leftover socket file nobody listens on is replaced. If a connect to it succeeds, fails
    * with IllegalStateException("socket in use: <path>")
    */
  def start(): Unit = synchronized:
    if listener != null then
      throw new IllegalStateException(s"socket in use: $socketPath")
    if os.exists(socketPath, followLinks = false) then
      if connects() then
        throw new IllegalStateException(s"socket in use: $socketPath")
      os.remove(socketPath)
    val g = new MultiThreadIoEventLoopGroup(2, DaemonThreads.factory("aspen-vdisk-bridge"),
      EpollIoHandler.newFactory())
    try
      val bootstrap = new ServerBootstrap()
        .group(g)
        .channel(classOf[EpollServerDomainSocketChannel])
        .childHandler(new ChannelInitializer[EpollDomainSocketChannel]:
          override def initChannel(ch: EpollDomainSocketChannel): Unit =
            sessions.add(ch)
            ch.pipeline().addLast(new FrameDecoder, new BridgeSession(openDisk)))
      listener = bootstrap.bind(new DomainSocketAddress(socketPath.toString)).sync().channel()
      group = g
    catch
      case t: Throwable =>
        g.shutdownGracefully(0, 0, TimeUnit.SECONDS)
        throw t

  /** Closes the listener and every session; their devices are closed crash-only */
  def stop(): Unit =
    val (g, l) = synchronized:
      val r = (group, listener)
      group = null
      listener = null
      r
    if g != null then
      l.close().awaitUninterruptibly()
      sessions.close().awaitUninterruptibly()
      g.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly()

  private def connects(): Boolean =
    try
      val ch = SocketChannel.open(StandardProtocolFamily.UNIX)
      try
        ch.connect(UnixDomainSocketAddress.of(socketPath.toNIO))
        true
      finally
        ch.close()
    catch
      case _: IOException => false
