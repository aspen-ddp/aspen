package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.buffer.{ByteBuf, CompositeByteBuf}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.Message
import scribe.Logging

import scala.util.control.NonFatal

/** Incrementally decodes stream frames (see `QuicMessageCodec`) from an inbound stream.
  *
  * One instance per inbound stream (not sharable). Releases every inbound ByteBuf it is given. Section payloads are
  * copied exactly once, directly from the inbound buffers into plain heap arrays.
  */
class StreamFrameDecoder(onMessage: Message => Unit,
                         onProtocolError: String => Unit,
                         private[quic] val allocSection: Int => Array[Byte] = n => new Array[Byte](n))
  extends ChannelInboundHandlerAdapter with Logging:

  private enum State:
    case Lengths, Prefix, Section

  private val LengthsSize = QuicMessageCodec.FrameLengthSize + 4
  private val MinPrefixSize = 12L

  private var state = State.Lengths
  private var failed = false

  private var lengths: ByteBuf = null
  private var frameLen = 0L
  private var prefixLen = 0
  private var prefixCum: CompositeByteBuf = null
  private var prefix: QuicMessageCodec.Prefix = null
  private var sections: Array[Array[Byte]] = null
  private var sectionIdx = 0
  private var sectionOffset = 0

  override def isSharable: Boolean = false

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit =
    msg match
      case in: ByteBuf =>
        try
          if !failed then
            decode(ctx, in)
        catch
          case t: Throwable =>
            if !failed then
              fail(ctx, s"Unexpected failure decoding stream: $t")
        finally
          in.release()
      case other => ctx.fireChannelRead(other)

  private def decode(ctx: ChannelHandlerContext, in: ByteBuf): Unit =
    while !failed && in.isReadable do
      state match
        case State.Lengths => readLengths(ctx, in)
        case State.Prefix => readPrefix(ctx, in)
        case State.Section => readSection(ctx, in)

  private def readLengths(ctx: ChannelHandlerContext, in: ByteBuf): Unit =
    if lengths == null then
      lengths = ctx.alloc().buffer(LengthsSize, LengthsSize)
    lengths.writeBytes(in, Math.min(in.readableBytes(), LengthsSize - lengths.readableBytes()))
    if lengths.readableBytes() == LengthsSize then
      frameLen = lengths.readLong()
      val pl = lengths.readUnsignedInt()
      lengths.release()
      lengths = null
      if pl < MinPrefixSize || pl > Int.MaxValue then
        fail(ctx, s"Invalid prefix length $pl")
      else if 4L + pl > frameLen then
        fail(ctx, s"Frame length $frameLen too small for prefix length $pl")
      else
        prefixLen = pl.toInt
        state = State.Prefix
        prefixCum = ctx.alloc().compositeBuffer()

  private def readPrefix(ctx: ChannelHandlerContext, in: ByteBuf): Unit =
    val n = Math.min(in.readableBytes(), prefixLen - prefixCum.readableBytes())
    prefixCum.addComponent(true, in.readRetainedSlice(n))
    if prefixCum.readableBytes() == prefixLen then
      val parsed = QuicMessageCodec.parsePrefix(prefixCum)
      prefixCum.release()
      prefixCum = null
      parsed match
        case Left(err) => fail(ctx, err)
        case Right(p) =>
          val total = p.sectionSizes.foldLeft(0L)(_ + _)
          if p.sectionSizes.exists(_ < 0) then
            fail(ctx, "Negative section length")
          else if frameLen != 4L + prefixLen + total then
            fail(ctx, s"Frame length $frameLen does not match prefix ${4L + prefixLen} plus sections $total")
          else
            prefix = p
            sections = p.sectionSizes.map(allocSection)
            sectionIdx = 0
            sectionOffset = 0
            state = State.Section
            advanceSections(ctx)

  private def readSection(ctx: ChannelHandlerContext, in: ByteBuf): Unit =
    val arr = sections(sectionIdx)
    val n = Math.min(in.readableBytes(), arr.length - sectionOffset)
    in.readBytes(arr, sectionOffset, n)
    sectionOffset += n
    advanceSections(ctx)

  /** Skips past completed (including empty) sections and dispatches the message once all are full */
  private def advanceSections(ctx: ChannelHandlerContext): Unit =
    while sectionIdx < sections.length && sectionOffset == sections(sectionIdx).length do
      sectionIdx += 1
      sectionOffset = 0
    if sectionIdx == sections.length then
      val p = prefix
      val bufs = sections.map(a => DataBuffer(a))
      prefix = null
      sections = null
      state = State.Lengths
      QuicMessageCodec.assemble(p, bufs) match
        case Left(err) => fail(ctx, err)
        case Right(m) =>
          try
            onMessage(m)
          catch
            case NonFatal(t) => logger.error("Message handler failed", t)

  private def fail(ctx: ChannelHandlerContext, reason: String): Unit =
    failed = true
    releaseState()
    onProtocolError(reason)
    ctx.close()

  private def releaseState(): Unit =
    if lengths != null then
      lengths.release()
      lengths = null
    if prefixCum != null then
      prefixCum.release()
      prefixCum = null
    prefix = null
    sections = null

  override def channelInactive(ctx: ChannelHandlerContext): Unit =
    releaseState()
    ctx.fireChannelInactive()

  override def handlerRemoved(ctx: ChannelHandlerContext): Unit =
    releaseState()
