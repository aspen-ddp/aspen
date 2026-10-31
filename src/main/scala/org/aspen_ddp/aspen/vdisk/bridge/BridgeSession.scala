package org.aspen_ddp.aspen.vdisk.bridge

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.DecoderException
import org.aspen_ddp.aspen.vdisk.*
import scribe.Logging

import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** One connection. Requests are dispatched to the device as they arrive and each reply is written when its own
  * Future completes, so replies may be reordered. All handler callbacks run on the connection's event loop, which
  * is the only place `state` and `device` are touched.
  */
final class BridgeSession(openDisk: String => Future[LockableBlockDevice]) extends ChannelInboundHandlerAdapter
    with Logging:

  import Protocol.{Op, Status}

  private enum State:
    case NotOpened, Opening, Opened

  private var state = State.NotOpened
  private var device: Option[LockableBlockDevice] = None
  private val parasitic = ExecutionContext.parasitic

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit =
    msg match
      case req: Request =>
        try
          handle(ctx, req)
        catch
          case t: Throwable =>
            logger.error(s"Bridge request ${req.op} failed unexpectedly: $t")
            respondError(ctx, req.op, req.requestId, Status.Internal, t.toString)
        finally
          req.payload.release()
      case other =>
        ctx.fireChannelRead(other)

  private def handle(ctx: ChannelHandlerContext, req: Request): Unit =
    if req.op == Op.Open then
      handleOpen(ctx, req)
    else if req.op < Op.Open || req.op > Op.LockState then
      respondError(ctx, req.op, req.requestId, Status.ProtocolError, s"unknown op ${req.op}")
    else
      state match
        case State.Opened => handleIo(ctx, req, device.get)
        case _ => respondError(ctx, req.op, req.requestId, Status.ProtocolError, "disk not open")

  private def handleOpen(ctx: ChannelHandlerContext, req: Request): Unit =
    if state != State.NotOpened then
      respondError(ctx, req.op, req.requestId, Status.ProtocolError, "disk already opened")
    else
      val bytes = new Array[Byte](req.payload.readableBytes())
      req.payload.getBytes(req.payload.readerIndex(), bytes)
      val name = new String(bytes, StandardCharsets.UTF_8)
      state = State.Opening
      val fut = Try(openDisk(name)) match
        case Success(f) => f
        case Failure(t) => Future.failed(t)
      val executor = ctx.executor()
      // Back onto the event loop, which owns the session state
      fut.onComplete(result => executor.execute(() => openCompleted(ctx, req, result)))(parasitic)

  private def openCompleted(ctx: ChannelHandlerContext, req: Request, result: Try[LockableBlockDevice]): Unit =
    result match
      case Failure(t) =>
        state = State.NotOpened
        replyError(ctx, req.op, req.requestId, t)
      case Success(dev) =>
        val g = dev.geometry
        if !ctx.channel().isActive then
          dev.close()
        else if g.size != req.offset || g.logicalBlockSize != req.length then
          dev.close()
          state = State.NotOpened
          respondError(ctx, req.op, req.requestId, Status.SizeMismatch,
            s"disk is ${g.size} bytes with ${g.logicalBlockSize} byte blocks, client reported " +
              s"${req.offset} bytes with ${req.length} byte blocks")
        else
          device = Some(dev)
          state = State.Opened
          val payload = Unpooled.buffer(8)
          payload.writeInt(g.blockObjectSize)
          payload.writeInt(g.logicalBlockSize)
          respond(ctx, req.op, req.requestId, Status.Ok, payload)

  private def handleIo(ctx: ChannelHandlerContext, req: Request, dev: LockableBlockDevice): Unit =
    val payloadLen = req.payload.readableBytes()

    def invalid(msg: String): Unit = respondError(ctx, req.op, req.requestId, Status.InvalidRequest, msg)

    def completeWith[A](f: => Future[A])(toPayload: A => ByteBuf): Unit =
      val fut = Try(f) match
        case Success(x) => x
        case Failure(t) => Future.failed(t)
      fut.onComplete {
        case Success(a) => respond(ctx, req.op, req.requestId, Status.Ok, toPayload(a))
        case Failure(t) => replyError(ctx, req.op, req.requestId, t)
      }(parasitic)

    def empty(u: Unit): ByteBuf = Unpooled.EMPTY_BUFFER

    req.op match
      case Op.Read =>
        if payloadLen != 0 then invalid("READ carries no payload")
        else completeWith(dev.read(req.offset, req.length))(Unpooled.wrappedBuffer(_))
      case Op.Write =>
        if req.length != payloadLen then invalid(s"WRITE length ${req.length} != payload length $payloadLen")
        else
          val data = new Array[Byte](payloadLen)
          req.payload.getBytes(req.payload.readerIndex(), data)
          val fua = (req.flags & Protocol.FlagFua) != 0
          completeWith(dev.write(req.offset, data, fua))(empty)
      case Op.Flush =>
        if payloadLen != 0 then invalid("FLUSH carries no payload")
        else completeWith(dev.flush())(empty)
      case Op.Unmap =>
        if payloadLen == 0 || payloadLen % 16 != 0 then invalid(s"UNMAP payload length $payloadLen")
        else
          val p = req.payload
          val ranges = (0 until payloadLen / 16).map: i =>
            (p.getLong(p.readerIndex() + i * 16), p.getLong(p.readerIndex() + i * 16 + 8))
          completeWith(dev.unmap(ranges.toList))(empty)
      case Op.Resize =>
        if payloadLen != 0 then invalid("RESIZE carries no payload")
        else completeWith(dev.grow(req.offset))(empty)
      case Op.Lock =>
        if payloadLen != 0 then invalid("LOCK carries no payload")
        else completeWith(dev.acquire())(empty)
      case Op.Unlock =>
        if payloadLen != 0 then invalid("UNLOCK carries no payload")
        else completeWith(dev.release())(empty)
      case Op.LockState =>
        if payloadLen != 0 then invalid("LOCK_STATE carries no payload")
        else
          val b = dev.lockState match
            case LockState.Unlocked => 0
            case LockState.Locked => 1
            case LockState.Fenced => 2
            case LockState.Failed(_) => 3
          respond(ctx, req.op, req.requestId, Status.Ok, Unpooled.buffer(1).writeByte(b))
      case other =>
        respondError(ctx, other, req.requestId, Status.ProtocolError, s"unknown op $other")

  private def respond(ctx: ChannelHandlerContext, op: Int, id: Long, status: Int, payload: ByteBuf): Unit =
    ctx.writeAndFlush(Protocol.encodeResponse(op, status, id, payload))

  private def respondError(ctx: ChannelHandlerContext, op: Int, id: Long, status: Int, msg: String): Unit =
    respond(ctx, op, id, status, Protocol.errorPayload(msg))

  private def replyError(ctx: ChannelHandlerContext, op: Int, id: Long, t: Throwable): Unit =
    val status = t match
      case _: VDiskError.InvalidRequest | _: VDiskError.InvalidGeometry => Status.InvalidRequest
      case _: VDiskError.OutOfRange => Status.OutOfRange
      case VDiskError.Fenced => Status.Fenced
      case VDiskError.NotOwner => Status.NotOwner
      case _: VDiskError.DiskFailed => Status.DiskFailed
      case _: VDiskError.DiskNotFound => Status.NotFound
      case _ =>
        logger.error(s"Bridge op $op failed internally: $t")
        Status.Internal
    respondError(ctx, op, id, status, Option(t.getMessage).getOrElse(t.toString))

  override def channelInactive(ctx: ChannelHandlerContext): Unit =
    // Crash-only: the device is closed, never released
    device.foreach(_.close())
    device = None
    state = State.NotOpened
    ctx.fireChannelInactive()

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit =
    val root = cause match
      case d: DecoderException if d.getCause != null => d.getCause
      case other => other
    root match
      case v: FrameViolation =>
        if ctx.channel().isActive then
          logger.error(s"Bridge framing violation, closing connection: ${v.getMessage}")
          ctx.close()
      case other =>
        if ctx.channel().isActive then
          logger.error(s"Bridge connection error, closing connection: $other")
          ctx.close()
