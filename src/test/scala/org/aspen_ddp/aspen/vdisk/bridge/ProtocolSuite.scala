package org.aspen_ddp.aspen.vdisk.bridge

import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.DecoderException
import io.netty.util.ResourceLeakDetector
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ProtocolSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll:

  private var previousLevel: ResourceLeakDetector.Level = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    previousLevel = ResourceLeakDetector.getLevel
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)

  override def afterAll(): Unit =
    ResourceLeakDetector.setLevel(previousLevel)

  private def hex(s: String): Array[Byte] =
    s.trim.split("\\s+").map(h => Integer.parseInt(h, 16).toByte)

  private val goldenWrite = hex(
    "A5 D1 01 03 00 00 00 01 01 02 03 04 05 06 07 08 00 00 00 00 00 00 10 00 00 00 00 04 00 00 00 04 DE AD BE EF")
  private val goldenRead = hex(
    "A5 D2 01 02 00 00 00 00 00 00 00 00 00 00 00 07 00 00 00 02 00 00 00 00 01 02")

  private def toArray(b: ByteBuf): Array[Byte] =
    val a = new Array[Byte](b.readableBytes())
    b.getBytes(b.readerIndex(), a)
    a

  private def requestBytes(op: Int, id: Long, payload: Array[Byte], magic: Int = 0xA5D1,
                           version: Int = 1, declaredPayload: Long = -1): Array[Byte] =
    val b = Unpooled.buffer()
    b.writeShort(magic).writeByte(version).writeByte(op).writeInt(0).writeLong(id)
    b.writeLong(0).writeInt(payload.length)
    b.writeInt((if declaredPayload >= 0 then declaredPayload else payload.length.toLong).toInt)
    b.writeBytes(payload)
    val a = toArray(b)
    b.release()
    a

  private def inbound(ch: EmbeddedChannel, data: Array[Byte]): Unit =
    ch.writeInbound(Unpooled.wrappedBuffer(data))

  private def drain(ch: EmbeddedChannel): List[Request] =
    val out = List.newBuilder[Request]
    var m = ch.readInbound[Request]()
    while m != null do
      out += m
      m = ch.readInbound[Request]()
    out.result()

  private def release(rs: List[Request]): Unit = rs.foreach(_.payload.release())

  private def assertGolden(r: Request): Unit =
    r.op should be (3)
    r.flags should be (1)
    r.requestId should be (0x0102030405060708L)
    r.offset should be (4096L)
    r.length should be (4)
    toArray(r.payload) should be (hex("DE AD BE EF"))

  private def finish(ch: EmbeddedChannel): Unit =
    ch.finishAndReleaseAll()

  test("decodes the golden WRITE request") {
    val ch = new EmbeddedChannel(new FrameDecoder)
    inbound(ch, goldenWrite)
    val rs = drain(ch)
    rs.size should be (1)
    assertGolden(rs.head)
    release(rs)
    finish(ch)
  }

  test("byte at a time") {
    val ch = new EmbeddedChannel(new FrameDecoder)
    goldenWrite.foreach(b => ch.writeInbound(Unpooled.wrappedBuffer(Array(b))))
    val rs = drain(ch)
    rs.size should be (1)
    assertGolden(rs.head)
    release(rs)
    finish(ch)
  }

  test("two requests in one buffer decode to two") {
    val ch = new EmbeddedChannel(new FrameDecoder)
    inbound(ch, goldenWrite ++ requestBytes(4, 9, Array.empty))
    val rs = drain(ch)
    rs.size should be (2)
    assertGolden(rs(0))
    rs(1).op should be (4)
    rs(1).requestId should be (9L)
    rs(1).payload.readableBytes() should be (0)
    release(rs)
    finish(ch)
  }

  test("a 4 MiB payload split in 3 chunks decodes intact") {
    val payload = Array.tabulate[Byte](4 * 1024 * 1024)(i => (i * 31 + 7).toByte)
    val all = requestBytes(3, 5, payload)
    val ch = new EmbeddedChannel(new FrameDecoder)
    val c1 = 1000
    val c2 = all.length / 2
    inbound(ch, all.slice(0, c1))
    drain(ch) should be (Nil)
    inbound(ch, all.slice(c1, c2))
    drain(ch) should be (Nil)
    inbound(ch, all.slice(c2, all.length))
    val rs = drain(ch)
    rs.size should be (1)
    rs.head.requestId should be (5L)
    toArray(rs.head.payload) should be (payload)
    release(rs)
    finish(ch)
  }

  private def violation(data: Array[Byte]): Unit =
    val ch = new EmbeddedChannel(new FrameDecoder)
    val e = intercept[DecoderException](inbound(ch, data))
    e.getCause shouldBe a [FrameViolation]
    // closing re-decodes the retained bytes and violates again; the buffers are still released
    scala.util.Try(ch.finishAndReleaseAll())

  test("bad magic") {
    violation(requestBytes(4, 1, Array.empty, magic = 0xBEEF))
  }

  test("bad version") {
    violation(requestBytes(4, 1, Array.empty, version = 2))
  }

  test("payload over MaxPayload fails on the header alone") {
    val all = requestBytes(3, 1, Array.empty, declaredPayload = Protocol.MaxPayload.toLong + 1)
    all.length should be (Protocol.RequestHeaderSize)
    violation(all)
  }

  test("encodes the golden READ reply") {
    val buf = Protocol.encodeResponse(2, 0, 7, Unpooled.wrappedBuffer(Array[Byte](1, 2)))
    toArray(buf) should be (goldenRead)
    buf.release()
  }

  test("encodeResponse with an empty payload is just the header") {
    val buf = Protocol.encodeResponse(4, Protocol.Status.Fenced, 3, Unpooled.EMPTY_BUFFER)
    buf.readableBytes() should be (Protocol.ResponseHeaderSize)
    buf.release()
  }

  test("errorPayload truncates to 1024 bytes") {
    val p = Protocol.errorPayload("x" * 5000)
    p.readableBytes() should be (1024)
    p.release()
    val q = Protocol.errorPayload("boom")
    toArray(q) should be ("boom".getBytes("UTF-8"))
    q.release()
  }
