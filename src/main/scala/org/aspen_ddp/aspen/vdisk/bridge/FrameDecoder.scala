package org.aspen_ddp.aspen.vdisk.bridge

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{ByteToMessageDecoder, DecoderException}

import java.util.List as JList

/** Decodes request frames. Bad magic, bad version or an oversize payload throws FrameViolation. */
final class FrameDecoder extends ByteToMessageDecoder:

  override protected def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: JList[AnyRef]): Unit =
    if in.readableBytes() >= Protocol.RequestHeaderSize then
      val base = in.readerIndex()
      val magic = in.getUnsignedShort(base)
      if magic != Protocol.RequestMagic then
        throw violation(f"bad request magic 0x$magic%04X")
      val version = in.getUnsignedByte(base + 2).toInt
      if version != Protocol.Version then
        throw violation(s"unsupported protocol version $version")
      val payloadLength = in.getUnsignedInt(base + 28)
      if payloadLength > Protocol.MaxPayload then
        throw violation(s"payload length $payloadLength exceeds ${Protocol.MaxPayload}")
      if in.readableBytes() >= Protocol.RequestHeaderSize + payloadLength then
        val op = in.getUnsignedByte(base + 3).toInt
        val flags = in.getInt(base + 4)
        val requestId = in.getLong(base + 8)
        val offset = in.getLong(base + 16)
        val length = in.getInt(base + 24)
        in.skipBytes(Protocol.RequestHeaderSize)
        val payload = in.readRetainedSlice(payloadLength.toInt)
        out.add(Request(op, flags, requestId, offset, length, payload))

  /** Netty rethrows a DecoderException unwrapped, so the violation is carried as the cause */
  private def violation(msg: String): DecoderException =
    new DecoderException(new FrameViolation(msg))
