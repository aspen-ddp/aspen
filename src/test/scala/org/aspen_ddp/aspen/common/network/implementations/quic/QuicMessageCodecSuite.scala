package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.buffer.{ByteBuf, ByteBufAllocator, CompositeByteBuf, PooledByteBufAllocator, Unpooled}
import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
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

class QuicMessageCodecSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll:

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

  private def prepare(updates: List[ObjectUpdate], rebuilds: List[PreTransactionOpportunisticRebuild]) =
    TxPrepare(storeId(10), storeId(11), txd, ProposalId(1, 1.toByte), updates, rebuilds, fromClient = false)

  private def readResponse(data: Option[DataBuffer]) =
    ReadResponse(ClientId(uuid(1)), storeId(2), uuid(3), HLCTimestamp(1000L),
      Right(ReadResponse.CurrentState(rev(4), ObjectRefcount(1, 5), HLCTimestamp(2000L), 1024, data,
        Set(TransactionId(uuid(5)), TransactionId(uuid(6))))))

  /** A single-section message (the payload is its only out-of-band data section) */
  private def rebuildMsg(data: DataBuffer): OpportunisticRebuild =
    OpportunisticRebuild(storeId(1), ClientId(uuid(2)), dataPtr(3), rev(4), ObjectRefcount(1, 5),
      HLCTimestamp(1000L), data)

  private def fixtures: List[Message] = List(
    HostHeartbeat(HostId(uuid(1))),
    Read(storeId(10), ClientId(uuid(20)), uuid(30), dataPtr(1), FullObject()),
    OpportunisticRebuild(storeId(1), ClientId(uuid(2)), dataPtr(3), rev(4), ObjectRefcount(1, 5),
      HLCTimestamp(1000L), DataBuffer(bytes(20))),
    TransactionCompletionQuery(storeId(1), ClientId(uuid(2)), uuid(3), TransactionId(uuid(4))),
    ReadResponse(ClientId(uuid(1)), storeId(2), uuid(3), HLCTimestamp(1000L), Left(ReadError.ObjectNotFound)),
    readResponse(Some(DataBuffer(bytes(50)))),
    readResponse(None),
    TransactionCompletionResponse(ClientId(uuid(1)), storeId(2), uuid(3), isComplete = true),
    TransactionResolved(ClientId(uuid(1)), storeId(2), TransactionId(uuid(3)), committed = true),
    TransactionFinalized(ClientId(uuid(1)), storeId(2), TransactionId(uuid(3)), committed = false),
    prepare(Nil, Nil),
    prepare(List(ObjectUpdate(ObjectId(uuid(7)), DataBuffer(bytes(30)))),
      List(PreTransactionOpportunisticRebuild(ObjectId(uuid(8)), meta(8), DataBuffer(bytes(40))))),
    TxPrepareResponse(storeId(1), storeId(2), TransactionId(uuid(3)),
      Right(TxPrepareResponse.Promise(Some((ProposalId(4, 1.toByte), true)))),
      ProposalId(3, 1.toByte), TransactionDisposition.VoteCommit, Nil),
    TxAccept(storeId(1), storeId(2), TransactionId(uuid(3)), ProposalId(4, 1.toByte), true),
    TxAcceptResponse(storeId(1), storeId(2), TransactionId(uuid(3)), ProposalId(4, 1.toByte),
      Right(TxAcceptResponse.Accepted(true))),
    TxResolved(storeId(1), storeId(2), TransactionId(uuid(3)), committed = true),
    TxCommitted(storeId(1), storeId(2), TransactionId(uuid(3)), List(ObjectId(uuid(4)))),
    TxFinalized(storeId(1), storeId(2), TransactionId(uuid(3)), committed = true),
    TxHeartbeat(storeId(1), storeId(2), TransactionId(uuid(3))),
    TxStatusRequest(storeId(1), storeId(2), TransactionId(uuid(3)), uuid(4)),
    TxStatusResponse(storeId(1), storeId(2), TransactionId(uuid(3)), uuid(4),
      Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = true))),
    TxUnknownStore(storeId(1), storeId(2), TransactionId(uuid(3))),
    StartStoreTransfer(HostId(uuid(1)), ClientId(uuid(2)), StorageDeviceId(uuid(3)), storeId(4),
      HLCTimestamp(1000L), uuid(5)),
    rebuildMsg(DataBuffer(bytes(100))),
    CheckStorageDevice(HostId(uuid(1)), ClientId(uuid(2)), StorageDeviceId(uuid(3))),
    ServiceMessage(HostId(uuid(1)), ClientId(uuid(2)), uuid(3), bytes(5)),
    ExecuteSystemTask(HostId(uuid(1)), ClientId(uuid(2)), uuid(3),
      KeyValueObjectPointer(ObjectId(uuid(9)), PoolId(uuid(109)), Array[Byte](9)))
  )

  /** ServiceMessage holds an Array, so case-class equality does not apply to it */
  private def assertSame(actual: Message, expected: Message): Unit =
    (actual, expected) match
      case (a: ServiceMessage, e: ServiceMessage) =>
        a.toHost shouldBe e.toHost
        a.fromClient shouldBe e.fromClient
        a.serviceUUID shouldBe e.serviceUUID
        a.encodedContent.toList shouldBe e.encodedContent.toList
      case _ => actual shouldBe expected

  /** Reads a stream frame the way a receiver would: strip frameLen, parse the prefix, slice the sections */
  private def decodeFrame(frame: ByteBuf): Either[String, Message] =
    val frameLen = frame.readLong()
    frameLen shouldBe frame.readableBytes().toLong
    val prefixLen = frame.readInt()
    val prefixBuf = frame.readSlice(prefixLen)
    QuicMessageCodec.parsePrefix(prefixBuf).flatMap: p =>
      val sections = p.sectionSizes.map: n =>
        val arr = new Array[Byte](n)
        frame.readBytes(arr)
        DataBuffer(arr)
      QuicMessageCodec.assemble(p, sections)

  test("every message type round-trips through a datagram"):
    fixtures.foreach: m =>
      val enc = QuicMessageCodec.encode(m, alloc)
      val dg = enc.toDatagram(alloc)
      val result = QuicMessageCodec.decodeDatagram(dg)
      dg.release()
      enc.prefix.refCnt() shouldBe 0
      assertSame(result.toOption.getOrElse(fail(s"$m: $result")), m)

  test("every message type round-trips through a stream frame"):
    fixtures.foreach: m =>
      val enc = QuicMessageCodec.encode(m, alloc)
      val prefix = enc.prefix
      val frame = enc.toStreamFrame(alloc)
      val view = frame.retainedDuplicate()
      val result = decodeFrame(view)
      view.release()
      frame.release()
      frame.refCnt() shouldBe 0
      prefix.refCnt() shouldBe 0
      assertSame(result.toOption.getOrElse(fail(s"$m: $result")), m)

  test("TxPrepare with 3 updates and 2 rebuilds keeps order and metadata"):
    val updates = (1 to 3).toList.map(i => ObjectUpdate(ObjectId(uuid(i)), DataBuffer(bytes(10 * i, i))))
    val rebuilds = (4 to 5).toList.map(i =>
      PreTransactionOpportunisticRebuild(ObjectId(uuid(i)), meta(i), DataBuffer(bytes(10 * i, i))))
    val m = prepare(updates, rebuilds)
    val enc = QuicMessageCodec.encode(m, alloc)
    val dg = enc.toDatagram(alloc)
    val decoded = QuicMessageCodec.decodeDatagram(dg).toOption.get.asInstanceOf[TxPrepare]
    dg.release()
    decoded shouldBe m
    decoded.objectUpdates.map(_.objectId) shouldBe updates.map(_.objectId)
    decoded.preTxRebuilds.map(_.requiredMetadata) shouldBe rebuilds.map(_.requiredMetadata)

  test("payload slices with non-zero position encode only their visible bytes"):
    val arr = bytes(40)
    val slice = DataBuffer(arr).slice(5, 10)
    val m = rebuildMsg(slice)
    val enc = QuicMessageCodec.encode(m, alloc)
    enc.size shouldBe 4 + enc.prefix.readableBytes() + 10
    val dg = enc.toDatagram(alloc)
    val decoded = QuicMessageCodec.decodeDatagram(dg).toOption.get.asInstanceOf[OpportunisticRebuild]
    dg.release()
    decoded.data.getByteArray.toList shouldBe arr.slice(5, 15).toList

    val enc2 = QuicMessageCodec.encode(m, alloc)
    val frame = enc2.toStreamFrame(alloc)
    val view = frame.retainedDuplicate()
    val decoded2 = decodeFrame(view).toOption.get.asInstanceOf[OpportunisticRebuild]
    view.release()
    frame.release()
    decoded2.data.getByteArray.toList shouldBe arr.slice(5, 15).toList

  private def roundTrip(m: Message): Message =
    val enc = QuicMessageCodec.encode(m, alloc)
    val frame = enc.toStreamFrame(alloc)
    val result = decodeFrame(frame)
    frame.release()
    result.toOption.getOrElse(fail(s"$m: $result"))

  test("ClusterTransportMessage round trips"):
    val m = ClusterTransportMessage(HostId(UUID.randomUUID()), UUID.randomUUID(), HostId(UUID.randomUUID()),
                                    Array.tabulate[Byte](3000)(_.toByte))
    roundTrip(m) shouldBe m   // 3000 bytes forces the stream path

  test("StoreMoved round trips through the QUIC codec"):
    val m = StoreMoved(StoreId(PoolId(UUID.randomUUID()), 1))
    roundTrip(m) shouldBe m

  test("HostEndpointChanged round trips through the QUIC codec"):
    val m = HostEndpointChanged(HostId(UUID.randomUUID()))
    roundTrip(m) shouldBe m

  test("empty payloads round-trip"):
    val msgs: List[Message] = List(
      prepare(List(ObjectUpdate(ObjectId(uuid(1)), DataBuffer.Empty)), Nil),
      readResponse(None),
      rebuildMsg(DataBuffer.Empty))
    msgs.foreach: m =>
      val enc = QuicMessageCodec.encode(m, alloc)
      val dg = enc.toDatagram(alloc)
      val result = QuicMessageCodec.decodeDatagram(dg)
      dg.release()
      result shouldBe Right(m)

  test("ReadResponse with Some(empty) data decodes as None"):
    val enc = QuicMessageCodec.encode(readResponse(Some(DataBuffer.Empty)), alloc)
    val dg = enc.toDatagram(alloc)
    val result = QuicMessageCodec.decodeDatagram(dg)
    dg.release()
    result shouldBe Right(readResponse(None))

  test("send path does not copy payloads"):
    val arr = bytes(2000)
    val m = rebuildMsg(DataBuffer(arr))
    val enc = QuicMessageCodec.encode(m, alloc)
    val frame = enc.toStreamFrame(alloc)
    frame shouldBe a[CompositeByteBuf]
    val comp = frame.asInstanceOf[CompositeByteBuf]
    val last = comp.componentAtOffset(comp.readableBytes() - 1)
    last.readableBytes() shouldBe 2000
    arr(0) = 99.toByte
    arr(1999) = 77.toByte
    val nio = last.nioBuffer()
    nio.get(0) shouldBe 99.toByte
    nio.get(1999) shouldBe 77.toByte
    frame.release()

  private def rebuild(data: DataBuffer): OpportunisticRebuild =
    OpportunisticRebuild(storeId(1), ClientId(uuid(2)), dataPtr(3), rev(4), ObjectRefcount(1, 5),
      HLCTimestamp(1000L), data)

  test("OpportunisticRebuild data travels as its one section, outside the protobuf header"):
    val m = rebuild(DataBuffer(bytes(5000)))
    val enc = QuicMessageCodec.encode(m, alloc)
    enc.sections.length shouldBe 1
    enc.sections(0).size shouldBe 5000
    val p = QuicMessageCodec.parsePrefix(enc.prefix).toOption.get
    p.sectionSizes.toList shouldBe List(5000)
    p.header.msg.opportunisticRebuild.get.data.size() shouldBe 0
    val frame = enc.toStreamFrame(alloc)
    val result = decodeFrame(frame)
    frame.release()
    result shouldBe Right(m)

  test("OpportunisticRebuild with empty data round-trips"):
    val m = rebuild(DataBuffer.Empty)
    val enc = QuicMessageCodec.encode(m, alloc)
    val dg = enc.toDatagram(alloc)
    val result = QuicMessageCodec.decodeDatagram(dg)
    dg.release()
    result shouldBe Right(m)

  test("OpportunisticRebuild send path does not copy its data"):
    val arr = bytes(2000)
    val enc = QuicMessageCodec.encode(rebuild(DataBuffer(arr)), alloc)
    val frame = enc.toStreamFrame(alloc).asInstanceOf[CompositeByteBuf]
    val last = frame.componentAtOffset(frame.readableBytes() - 1)
    last.readableBytes() shouldBe 2000
    arr(0) = 99.toByte
    arr(1999) = 77.toByte
    val nio = last.nioBuffer()
    nio.get(0) shouldBe 99.toByte
    nio.get(1999) shouldBe 77.toByte
    frame.release()

  test("OpportunisticRebuild requires exactly one section"):
    val enc = QuicMessageCodec.encode(rebuild(DataBuffer(bytes(8))), alloc)
    val p = QuicMessageCodec.parsePrefix(enc.prefix).toOption.get
    enc.release()
    val none = p.copy(sectionSizes = Array.empty[Int])
    QuicMessageCodec.assemble(none, Array.empty[DataBuffer]).isLeft shouldBe true
    val two = p.copy(sectionSizes = Array(8, 8))
    QuicMessageCodec.assemble(two, Array(DataBuffer(bytes(8)), DataBuffer(bytes(8)))).isLeft shouldBe true

  test("size picks the transport at the boundary"):
    def sized(target: Int): QuicMessageCodec.Encoded =
      val base = QuicMessageCodec.encode(
        rebuildMsg(DataBuffer.Empty), alloc)
      // an empty payload still adds a section length entry, so measure with one byte
      base.release()
      val one = QuicMessageCodec.encode(
        rebuildMsg(DataBuffer(bytes(1))), alloc)
      val overhead = one.size - 1
      one.release()
      QuicMessageCodec.encode(
        rebuildMsg(DataBuffer(bytes((target - overhead).toInt))),
        alloc)
    List((1023, true), (1024, true), (1025, false)).foreach: (target, expectDatagram) =>
      val enc = sized(target)
      enc.size shouldBe target.toLong
      enc.isDatagram shouldBe expectDatagram
      enc.release()

  private def validPrefixBytes(m: Message): Array[Byte] =
    val enc = QuicMessageCodec.encode(m, alloc)
    val arr = new Array[Byte](enc.prefix.readableBytes())
    enc.prefix.getBytes(enc.prefix.readerIndex(), arr)
    enc.release()
    arr

  private def parse(arr: Array[Byte]): Either[String, QuicMessageCodec.Prefix] =
    val buf = Unpooled.wrappedBuffer(arr)
    try QuicMessageCodec.parsePrefix(buf)
    finally buf.release()

  test("malformed input returns Left"):
    val good = validPrefixBytes(rebuildMsg(DataBuffer(bytes(8))))
    parse(good).isRight shouldBe true

    // truncated at several points
    List(0, 2, 5, good.length - 1).foreach(n => parse(good.take(n)).isLeft shouldBe true)

    // headerLen beyond the buffer
    val tooBig = good.clone()
    tooBig(0) = 0x7f.toByte
    parse(tooBig).isLeft shouldBe true

    // negative headerLen
    val neg = good.clone()
    neg(0) = 0xff.toByte
    parse(neg).isLeft shouldBe true

    // unparseable protobuf header
    val headerLen = java.nio.ByteBuffer.wrap(good).getInt()
    val garbage = good.clone()
    (4 until 4 + headerLen).foreach(i => garbage(i) = 0xff.toByte)
    parse(garbage).isLeft shouldBe true

    // negative section length (the last 4 bytes are the only section length)
    val negSection = good.clone()
    negSection(good.length - 4) = 0x80.toByte
    parse(negSection).isLeft shouldBe true

    // trailing bytes after the prefix
    parse(good ++ Array[Byte](0)).isLeft shouldBe true

    // TxPrepare whose aux disagrees with the section count
    val prep = prepare(List(ObjectUpdate(ObjectId(uuid(1)), DataBuffer(bytes(4)))), Nil)
    val enc = QuicMessageCodec.encode(prep, alloc)
    val p = QuicMessageCodec.parsePrefix(enc.prefix).toOption.get
    p.sectionSizes.length shouldBe 1
    enc.release()
    QuicMessageCodec.assemble(p, Array.empty[DataBuffer]).isLeft shouldBe true
    QuicMessageCodec.assemble(p, Array(DataBuffer(bytes(4)), DataBuffer(bytes(4)))).isLeft shouldBe true
    val badSections = p.copy(sectionSizes = Array(4, 4))
    QuicMessageCodec.assemble(badSections, Array(DataBuffer(bytes(4)), DataBuffer(bytes(4)))).isLeft shouldBe true

    // whole-datagram level: sections shorter than advertised, empty buffer
    val dgEnc = QuicMessageCodec.encode(rebuildMsg(DataBuffer(bytes(8))), alloc)
    val dg = dgEnc.toDatagram(alloc)
    val short = dg.retainedSlice(0, dg.readableBytes() - 1)
    QuicMessageCodec.decodeDatagram(short).isLeft shouldBe true
    short.release()
    dg.release()
    val empty = Unpooled.buffer(0)
    QuicMessageCodec.decodeDatagram(empty).isLeft shouldBe true
    empty.release()

  test("nothing leaks"):
    val enc = QuicMessageCodec.encode(readResponse(Some(DataBuffer(bytes(10)))), alloc)
    val prefix = enc.prefix
    enc.release()
    prefix.refCnt() shouldBe 0
    enc.release() // idempotent

    val enc2 = QuicMessageCodec.encode(readResponse(Some(DataBuffer(bytes(10)))), alloc)
    val prefix2 = enc2.prefix
    val dg = enc2.toDatagram(alloc)
    prefix2.refCnt() shouldBe 0
    enc2.release() // must not double release
    dg.release()

    val enc3 = QuicMessageCodec.encode(readResponse(Some(DataBuffer(bytes(10)))), alloc)
    val prefix3 = enc3.prefix
    val frame = enc3.toStreamFrame(alloc)
    enc3.release() // must not double release
    frame.refCnt() shouldBe 1
    frame.release()
    prefix3.refCnt() shouldBe 0
    System.gc()
