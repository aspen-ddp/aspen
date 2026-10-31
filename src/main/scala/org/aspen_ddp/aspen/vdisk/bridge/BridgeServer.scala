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
import java.net.ConnectException
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, LinkOption, NoSuchFileException}
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.ThreadLocalRandom
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.channels.SocketChannel
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

/** Serves [[LockableBlockDevice]]s to the tcmu-runner plugin over a Unix domain socket */
final class BridgeServer(socketPath: os.Path, openDisk: String => Future[LockableBlockDevice]) extends Logging:

  private var group: MultiThreadIoEventLoopGroup = null
  private var listener: Channel = null
  private val sessions = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  /** Longest unix socket path the kernel accepts (sun_path is 108 bytes including the terminator) */
  private val MaxSocketPath = 107

  /** File key of the socket this server published, so stop() unlinks only its own */
  private var ownKey: Object = null

  /** Binds the socket. A leftover socket file nobody listens on is replaced. If a connect to it succeeds, fails
    * with IllegalStateException("socket in use: <path>")
    */
  def start(): Unit = synchronized:
    if listener != null then
      throw new IllegalStateException(s"socket in use: $socketPath")
    checkLength(socketPath)
    checkExisting()
    // Bind inside a private 0700 directory, restrict the socket to 0600, then hard-link it into place, so there is
    // never a moment when another user can connect, and a socket that appeared meanwhile is never replaced
    val tmpDir = makeBindDir()
    try
      val tmpSock = tmpDir / "s"
      checkLength(tmpSock)
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
        listener = bootstrap.bind(new DomainSocketAddress(tmpSock.toString)).sync().channel()
        os.perms.set(tmpSock, "rw-------")
        try
          Files.createLink(socketPath.toNIO, tmpSock.toNIO)
        catch
          case _: FileAlreadyExistsException =>
            checkExisting()
            Files.createLink(socketPath.toNIO, tmpSock.toNIO)
        ownKey = Files.readAttributes(socketPath.toNIO, classOf[BasicFileAttributes],
          LinkOption.NOFOLLOW_LINKS).fileKey()
        group = g
      catch
        case t: Throwable =>
          if listener != null then
            listener.close().awaitUninterruptibly()
            listener = null
          g.shutdownGracefully(0, 0, TimeUnit.SECONDS)
          throw t
    finally
      os.remove.all(tmpDir)

  private def checkLength(p: os.Path): Unit =
    if p.toString.getBytes(StandardCharsets.UTF_8).length > MaxSocketPath then
      throw new IllegalStateException(
        s"socket path too long: $p exceeds the $MaxSocketPath byte unix socket limit (108 including the terminator)")

  /** A short, private (0700) directory beside the socket path */
  private def makeBindDir(): os.Path =
    val parent = socketPath / os.up
    var attempts = 0
    while true do
      val name = ".b" + java.lang.Long.toString(ThreadLocalRandom.current().nextLong(1L << 31, 1L << 36), 36)
      val dir = parent / name
      try
        os.makeDir(dir, "rwx------")
        return dir
      catch
        case e: FileAlreadyExistsException =>
          attempts += 1
          if attempts >= 5 then throw e
    throw new IllegalStateException("unreachable")

  /** Closes the listener and every session; their devices are closed crash-only */
  def stop(): Unit =
    val (g, l, key) = synchronized:
      val r = (group, listener, ownKey)
      group = null
      listener = null
      ownKey = null
      r
    if g != null then
      l.close().awaitUninterruptibly()
      sessions.close().awaitUninterruptibly()
      g.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly()
      // Unlink only the file this server published
      try
        val current = Files.readAttributes(socketPath.toNIO, classOf[BasicFileAttributes],
          LinkOption.NOFOLLOW_LINKS).fileKey()
        if current == key then
          Files.deleteIfExists(socketPath.toNIO)
      catch
        case _: NoSuchFileException => ()

  /** A leftover socket nobody listens on (connection refused) is removed. Anything else already at the path, or
    * any other probe failure, fails the start and leaves the path untouched
    */
  private def checkExisting(): Unit =
    val mode =
      try
        Files.getAttribute(socketPath.toNIO, "unix:mode", LinkOption.NOFOLLOW_LINKS).asInstanceOf[Integer].intValue
      catch
        case _: NoSuchFileException => return
    if (mode & 0xF000) != 0xC000 then
      throw new IllegalStateException(s"not a socket: $socketPath")
    try
      val ch = SocketChannel.open(StandardProtocolFamily.UNIX)
      try
        ch.connect(UnixDomainSocketAddress.of(socketPath.toNIO))
      finally
        ch.close()
      throw new IllegalStateException(s"socket in use: $socketPath")
    catch
      case _: ConnectException => Files.deleteIfExists(socketPath.toNIO)
      case e: IOException => throw new IllegalStateException(s"cannot probe $socketPath: $e", e)
