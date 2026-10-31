package org.aspen_ddp.aspen.vdisk.bridge

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.handler.codec.CorruptedFrameException

import java.nio.charset.StandardCharsets

/** Wire format shared with the C tcmu-runner plugin. All integers are big-endian. */
object Protocol:
  val RequestMagic = 0xA5D1
  val ResponseMagic = 0xA5D2
  val Version = 1
  val RequestHeaderSize = 32
  val ResponseHeaderSize = 24
  val MaxPayload: Int = 64 * 1024 * 1024
  val MaxErrorBytes = 1024

  val FlagFua = 1

  object Op:
    val Open = 1
    val Read = 2
    val Write = 3
    val Flush = 4
    val Unmap = 5
    val Resize = 6
    val Lock = 7
    val Unlock = 8
    val LockState = 9

  object Status:
    val Ok = 0
    val InvalidRequest = 1
    val OutOfRange = 2
    val Fenced = 3
    val NotOwner = 4
    val DiskFailed = 5
    val NotFound = 6
    val SizeMismatch = 7
    val ProtocolError = 8
    val Internal = 9

  /** Header plus payload as one composite buffer; the payload is not copied. Takes ownership of `payload` */
  def encodeResponse(op: Int, status: Int, requestId: Long, payload: ByteBuf): ByteBuf =
    val header = Unpooled.buffer(ResponseHeaderSize, ResponseHeaderSize)
    header.writeShort(ResponseMagic)
    header.writeByte(Version)
    header.writeByte(op)
    header.writeInt(status)
    header.writeLong(requestId)
    header.writeInt(payload.readableBytes())
    header.writeInt(0)
    val composite = Unpooled.compositeBuffer(2)
    composite.addComponents(true, header, payload)
    composite

  /** UTF-8, truncated to 1024 bytes */
  def errorPayload(msg: String): ByteBuf =
    val bytes = msg.getBytes(StandardCharsets.UTF_8)
    Unpooled.wrappedBuffer(bytes, 0, math.min(bytes.length, MaxErrorBytes))

/** `payload` is a retained slice of the inbound buffer; whoever handles the request releases it */
final case class Request(op: Int, flags: Int, requestId: Long, offset: Long, length: Int, payload: ByteBuf)

final class FrameViolation(msg: String) extends CorruptedFrameException(msg)
