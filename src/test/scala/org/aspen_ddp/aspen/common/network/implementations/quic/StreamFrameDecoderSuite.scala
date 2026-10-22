package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.buffer.{ByteBuf, ByteBufAllocator, PooledByteBufAllocator, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class StreamFrameDecoderSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll:

  private val alloc: ByteBufAllocator = PooledByteBufAllocator.DEFAULT
  private var previousLevel: ResourceLeakDetector.Level = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    previousLevel = ResourceLeakDetector.getLevel
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)

  override def afterAll(): Unit =
    ResourceLeakDetector.setLevel(previousLevel)

  private def uuid(n: Int): UUID = new UUID(n.toLong << 32 | n.toLong, n.toLong)
  private def storeId(n: Int): StoreId = StoreId(PoolId(uuid(n)), n.toByte)
  private def dataPtr(n: Int): DataObjectPointer = DataObjectPointer(ObjectId(uuid(n)), PoolId(uuid(n + 100)), Array[Byte](n.toByte))
  private def rev(n: Int): ObjectRevision = ObjectRevision(TransactionId(uuid(n)))
  private def bytes(n: Int, seed: Int = 0): Array[Byte] = Array.tabulate[Byte](n)(i => (i * 7 + seed).toByte)
  private def meta(n: Int) = Metadata(rev(n), ObjectRefcount(n, n + 1), HLCTimestamp(1000L + n))

  private val ida: IDA = Replication(3, 2)
  private def txd = TransactionDescription(
    TransactionId(uuid(1)), HLCTimestamp(1000L), dataPtr(2), 1.toByte,
    List(DataUpdate(dataPtr(4), rev(5), DataUpdateOperation.Overwrite)), Nil, None, Nil, Nil, ida,
    Map(dataPtr(2).poolId -> ida))

  private def prepare: TxPrepare =
    val updates = (1 to 2).toList.map(i => ObjectUpdate(ObjectId(uuid(i)), DataBuffer(bytes(10 * i, i))))
    val rebuilds = List(PreTransactionOpportunisticRebuild(ObjectId(uuid(8)), meta(8), DataBuffer(bytes(40))))
    TxPrepare(storeId(10), storeId(11), txd, ProposalId(1, 1.toByte), updates, rebuilds)

  private def heartbeat = HostHeartbeat(HostId(uuid(1)))

  private def frameBytes(m: Message): Array[Byte] =
    val frame = QuicMessageCodec.encode(m, alloc).toStreamFrame(alloc)
    val arr = new Array[Byte](frame.readableBytes())
    frame.readBytes(arr)
    frame.release()
    arr

  private class Harness(mk: (Message => Unit, String => Unit) => StreamFrameDecoder =
                          (m, e) => new StreamFrameDecoder(m, e)):
    val msgs = ArrayBuffer.empty[Message]
    val errs = ArrayBuffer.empty[String]
    val fed = ArrayBuffer.empty[ByteBuf]
    val channel = new EmbeddedChannel(mk(msgs += _, errs += _))

    def feed(buf: ByteBuf): Unit =
      fed += buf
      channel.writeInbound(buf)

    def feed(arr: Array[Byte], from: Int, until: Int): Unit =
      feed(alloc.buffer(until - from).writeBytes(arr, from, until - from))

    def finish(): Unit =
      channel.finishAndReleaseAll()
      fed.foreach(_.refCnt() shouldBe 0)

  test("single whole frame dispatches one message"):
    val h = new Harness
    h.feed(frameBytes(prepare), 0, frameBytes(prepare).length)
    h.msgs.toList shouldBe List(prepare)
    h.errs shouldBe empty
    h.finish()

  test("two frames in one buffer dispatch two messages in order"):
    val h = new Harness
    val a = frameBytes(prepare) ++ frameBytes(heartbeat) ++ frameBytes(prepare)
    h.feed(a, 0, a.length)
    h.msgs.toList shouldBe List(prepare, heartbeat, prepare)
    h.finish()

  test("every split point of a TxPrepare frame decodes identically"):
    val arr = frameBytes(prepare)
    (0 to arr.length).foreach: k =>
      val h = new Harness
      h.feed(arr, 0, k)
      h.feed(arr, k, arr.length)
      withClue(s"split at $k: "):
        h.msgs.toList shouldBe List(prepare)
        h.errs shouldBe empty
      h.finish()

  test("one-byte-at-a-time delivery decodes"):
    val arr = frameBytes(prepare) ++ frameBytes(heartbeat)
    val h = new Harness
    arr.indices.foreach(i => h.feed(arr, i, i + 1))
    h.msgs.toList shouldBe List(prepare, heartbeat)
    h.finish()

  test("large payload arrives in many chunks"):
    val size = 64 * 1024 * 1024
    val payload = bytes(size, 3)
    val m = StoreTransferData(HostId(uuid(1)), ClientId(uuid(2)), uuid(3), DataBuffer(payload))
    // A private allocator, so suites running in parallel don't show up in the peak
    val local = new PooledByteBufAllocator(true)
    val frame = QuicMessageCodec.encode(m, local).toStreamFrame(local)
    val h = new Harness
    h.channel.config().setAllocator(local)
    def used = local.metric().usedHeapMemory() + local.metric().usedDirectMemory()
    val baseline = used
    var peak = 0L
    val chunk = 64 * 1024
    while frame.isReadable do
      val c = local.buffer(chunk)
      c.writeBytes(frame, Math.min(chunk, frame.readableBytes()))
      h.feed(c)
      peak = Math.max(peak, used - baseline)
    frame.release()
    peak should be < (4L * 1024 * 1024)
    h.msgs.size shouldBe 1
    val got = h.msgs.head.asInstanceOf[StoreTransferData]
    got.data.size shouldBe size
    java.util.Arrays.equals(got.data.getByteArray, payload) should be (true)
    h.finish()

  test("TxPrepare with 4 x 128 MiB updates in 64 KiB chunks decodes intact"):
    // The four updates share one 128 MiB source array (the send path wraps payloads without copying), so the test
    // holds 128 MiB of source plus the 512 MiB the receiver copies out, within sbt's 1 GiB test heap.
    val sectionSize = 128 * 1024 * 1024
    val source = new Array[Byte](sectionSize)
    new java.util.Random(42).nextBytes(source)
    val updates = (1 to 4).toList.map(i => ObjectUpdate(ObjectId(uuid(i)), DataBuffer(source)))
    val m = TxPrepare(storeId(10), storeId(11), txd, ProposalId(1, 1.toByte), updates, Nil)
    // A private allocator, so suites running in parallel don't show up in the peak
    val local = new PooledByteBufAllocator(true)
    val frame = QuicMessageCodec.encode(m, local).toStreamFrame(local)
    frame.readableBytes().toLong should be > (4L * sectionSize)
    val h = new Harness
    h.channel.config().setAllocator(local)
    def used = local.metric().usedHeapMemory() + local.metric().usedDirectMemory()
    val baseline = used
    var peak = 0L
    val chunk = 64 * 1024
    while frame.isReadable do
      val c = local.buffer(chunk)
      c.writeBytes(frame, Math.min(chunk, frame.readableBytes()))
      h.feed(c)
      peak = Math.max(peak, used - baseline)
    frame.release()
    peak should be < (4L * 1024 * 1024)
    h.errs shouldBe empty
    h.msgs.size shouldBe 1
    val got = h.msgs.head.asInstanceOf[TxPrepare]
    h.msgs.clear()
    got.txd shouldBe m.txd
    got.objectUpdates.map(_.objectId) shouldBe updates.map(_.objectId)
    got.objectUpdates.foreach: u =>
      u.data.size shouldBe sectionSize
      // Compared in place: getByteArray would copy another 128 MiB
      (u.data.asReadOnlyBuffer() == java.nio.ByteBuffer.wrap(source)) should be (true)
    h.finish()

  private def corrupt(mutate: Array[Byte] => Unit): Harness =
    val arr = frameBytes(prepare)
    mutate(arr)
    val h = new Harness
    h.feed(arr, 0, arr.length)
    h

  test("frameLen inconsistent with section sizes closes the stream"):
    val h = corrupt(a => a(7) = (a(7) + 1).toByte)
    h.errs.size shouldBe 1
    h.channel.isOpen shouldBe false
    h.msgs shouldBe empty
    h.finish()

  test("negative section length closes the stream"):
    val arr = frameBytes(prepare)
    // the last section length is the final u32 of the prefix, which ends at offset 12 + prefixLen
    val prefixLen = java.nio.ByteBuffer.wrap(arr, 8, 4).getInt
    val pos = 12 + prefixLen - 4
    arr(pos) = 0x80.toByte
    val h = new Harness
    h.feed(arr, 0, arr.length)
    h.errs.size shouldBe 1
    h.channel.isOpen shouldBe false
    h.msgs shouldBe empty
    h.finish()

  test("input after a protocol error is discarded and released"):
    val h = corrupt(a => a(7) = (a(7) + 1).toByte)
    val more = frameBytes(heartbeat)
    val late = alloc.buffer(more.length).writeBytes(more)
    h.fed += late
    h.channel.pipeline().fireChannelRead(late) // writeInbound refuses a closed channel
    h.errs.size shouldBe 1
    h.msgs shouldBe empty
    h.finish()

  test("onMessage throwing does not desync the stream"):
    val msgs = ArrayBuffer.empty[Message]
    var first = true
    val ch = new EmbeddedChannel(new StreamFrameDecoder(m =>
      if first then
        first = false
        throw new RuntimeException("handler failure")
      msgs += m, _ => fail("unexpected protocol error")))
    val a = frameBytes(prepare) ++ frameBytes(heartbeat)
    val buf = alloc.buffer(a.length).writeBytes(a)
    ch.writeInbound(buf)
    msgs.toList shouldBe List(heartbeat)
    ch.isOpen shouldBe true
    ch.finishAndReleaseAll()
    buf.refCnt() shouldBe 0

  test("non-codec failure inside decode closes the stream"):
    val h = new Harness((m, e) => new StreamFrameDecoder(m, e, n => throw new OutOfMemoryError("test")))
    val arr = frameBytes(prepare)
    h.feed(arr, 0, arr.length)
    h.errs.size shouldBe 1
    h.channel.isOpen shouldBe false
    h.msgs shouldBe empty
    h.finish()

  test("channelInactive mid-frame releases everything"):
    val arr = frameBytes(prepare)
    Seq(5, 20, arr.length - 3).foreach: k =>
      val h = new Harness
      h.feed(arr, 0, k)
      h.channel.close()
      h.finish()
      h.msgs shouldBe empty

  test("every inbound buffer is released"):
    val h = new Harness
    val arr = frameBytes(prepare) ++ frameBytes(heartbeat)
    h.feed(arr, 0, 30)
    h.feed(arr, 30, arr.length)
    h.fed.foreach(_.refCnt() shouldBe 0)
    h.msgs.size shouldBe 2
    h.finish()
