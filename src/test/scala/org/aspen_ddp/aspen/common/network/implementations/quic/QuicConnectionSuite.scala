package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.EventLoop
import io.netty.handler.codec.quic.QuicChannel
import io.netty.util.ResourceLeakDetector
import io.netty.util.concurrent.{Future as NFuture, GenericFutureListener}
import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, OpportunisticRebuild}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.network.tls.CertificateAuthority
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

class QuicConnectionSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach:
  import QuicConnectionSuite.*
  import QuicTestPeer.{AwaitMillis, Received, Transport}

  private var previousLevel: ResourceLeakDetector.Level = scala.compiletime.uninitialized
  private var leakWatch: QuicTestPeer.LeakWatch = scala.compiletime.uninitialized
  private val peers = ArrayBuffer.empty[QuicTestPeer]
  private val connections = ArrayBuffer.empty[QuicConnection]
  private val cleanups = ArrayBuffer.empty[() => Unit]

  override def beforeAll(): Unit =
    previousLevel = ResourceLeakDetector.getLevel
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)
    leakWatch = QuicTestPeer.watchLeaks()

  override def afterAll(): Unit =
    leakWatch.stop()
    ResourceLeakDetector.setLevel(previousLevel)

  override def afterEach(): Unit =
    cleanups.foreach(f => f())
    cleanups.clear()
    connections.foreach(c => if !c.loop.isShutdown then onLoop(c.loop)(c.close()))
    connections.clear()
    peers.foreach(_.close())
    peers.clear()

  // ---------------------------------------------------------------------------------------------------------------

  private def creds(ca: CertificateAuthority = TestTLS.ca): TlsCredentials =
    val kp = CertificateAuthority.generateKeyPair()
    TlsCredentials(ca.certificate, ca.signCertificate(UUID.randomUUID(), kp.getPublic), kp.getPrivate)

  private final class Fixture(val srv: QuicTestPeer, val cli: QuicTestPeer, val conn: QuicConnection,
                              val closedCalls: ConcurrentLinkedQueue[QuicConnection]):
    def loop: EventLoop = conn.loop

    /** Dials the server and hands the result to `conn` from the connect future, as `QuicNet` does */
    def dial(): NFuture[QuicChannel] =
      val f = cli.bootstrap(srv.localAddress).connect()
      f.addListener((r: NFuture[QuicChannel]) =>
        if r.isSuccess then
          conn.attach(r.getNow)
        else
          conn.handshakeFailed(r.cause()))
      f

    def dialAndWait(): QuicChannel =
      val f = dial()
      f.await(AwaitMillis) shouldBe true
      if !f.isSuccess then
        fail(f.cause())
      f.getNow

    def submit(m: OpportunisticRebuild): (Boolean, QuicMessageCodec.Encoded) =
      val enc = QuicMessageCodec.encode(m, PooledByteBufAllocator.DEFAULT)
      (conn.submit(enc), enc)

  private def fixture(config: QuicConfig = QuicConfig(), stall: Boolean = false,
                      clientCreds: TlsCredentials = creds()): Fixture =
    val srv = QuicTestPeer.server(QuicTls.serverContext(creds()), config, stallStreams = stall)
    peers += srv
    val cli = QuicTestPeer.client(QuicTls.clientContext(clientCreds), config)
    peers += cli
    val closedCalls = new ConcurrentLinkedQueue[QuicConnection]()
    val conn = new QuicConnection(cli.channel.eventLoop(), config, c => closedCalls.add(c))
    connections += conn
    Fixture(srv, cli, conn, closedCalls)

  /** Blocks the loop until the returned latch is released (released automatically in afterEach) */
  private def blockLoop(loop: EventLoop): CountDownLatch =
    val release = new CountDownLatch(1)
    val started = new CountDownLatch(1)
    loop.execute: () =>
      started.countDown()
      release.await(AwaitMillis * 2, TimeUnit.MILLISECONDS)
    started.await(AwaitMillis, TimeUnit.MILLISECONDS) shouldBe true
    cleanups += (() => release.countDown())
    release

  private def awaitAll(peer: QuicTestPeer, n: Int): List[Received] =
    List.fill(n)(peer.awaitReceived())

  private def eventually(what: String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(AwaitMillis)
    while !cond do
      if System.nanoTime() > deadline then
        fail(s"timed out waiting for $what")
      Thread.sleep(5)

  private def seqOf(r: Received): Long = r.message.asInstanceOf[OpportunisticRebuild].pointer.id.uuid.getLeastSignificantBits

  private def streamId(r: Received): Long = r.transport match
    case Transport.Stream(id) => id
    case Transport.Datagram => fail(s"message ${seqOf(r)} arrived as a datagram")

  // ---------------------------------------------------------------------------------------------------------------

  test("small messages arrive as datagrams and large ones on streams"):
    val fx = fixture()
    fx.dialAndWait()
    val (ok1, e1) = fx.submit(sized(1, QuicMessageCodec.DatagramThreshold))
    val (ok2, e2) = fx.submit(sized(2, QuicMessageCodec.DatagramThreshold + 1))
    e1.size shouldBe 1024
    e2.size shouldBe 1025
    (ok1, ok2) shouldBe (true, true)
    val got = awaitAll(fx.srv, 2).map(r => seqOf(r) -> r.transport).toMap
    got(1) shouldBe Transport.Datagram
    got(2) shouldBe a [Transport.Stream]
    e1.prefix.refCnt() shouldBe 0
    e2.prefix.refCnt() shouldBe 0
    fx.conn.drops shouldBe 0

  test("large messages are spread across all 4 streams"):
    val fx = fixture()
    fx.dialAndWait()
    (0 until 40).foreach(i => fx.submit(sized(i, 64 * 1024))._1 shouldBe true)
    val got = awaitAll(fx.srv, 40)
    val perStream = got.groupBy(streamId).view.mapValues(_.size).toMap
    perStream.size shouldBe 4
    perStream.values.foreach(n => n should (be >= 8 and be <= 12))
    got.map(seqOf).toSet shouldBe (0L until 40L).toSet
    fx.conn.drops shouldBe 0

  test("messages submitted before attach are delivered after the handshake, including datagram-sized ones"):
    val fx = fixture()
    val small = (0 until 3).map(i => fx.submit(sized(i, 200)))
    val large = (3 until 6).map(i => fx.submit(sized(i, 100 * 1024)))
    (small ++ large).map(_._1).forall(identity) shouldBe true
    val barrier = fx.conn.postBarrier()
    fx.dialAndWait()
    barrier.await(AwaitMillis) shouldBe true
    barrier.isSuccess shouldBe true
    val got = awaitAll(fx.srv, 6).map(r => seqOf(r) -> r.transport).toMap
    (0 until 3).foreach(i => got(i) shouldBe Transport.Datagram)
    (3 until 6).foreach(i => got(i) shouldBe a [Transport.Stream])
    (small ++ large).foreach(_._2.prefix.refCnt() shouldBe 0)
    fx.conn.drops shouldBe 0

  test("handshake failure drops pre-handshake messages and completes barriers"):
    // The client trusts a foreign CA, so it rejects the server and the connect future fails
    val foreign = CertificateAuthority.create(UUID.randomUUID())
    val fx = fixture(clientCreds = creds(foreign))
    val subs = (0 until 3).map(i => fx.submit(sized(i, 200))) ++ (3 until 6).map(i => fx.submit(sized(i, 50_000)))
    subs.map(_._1).forall(identity) shouldBe true
    val barrier = fx.conn.postBarrier()
    val f = fx.dial()
    f.await(AwaitMillis) shouldBe true
    f.isSuccess shouldBe false
    barrier.await(AwaitMillis) shouldBe true
    eventually("onClosed")(fx.closedCalls.size == 1)
    fx.conn.isClosed shouldBe true
    fx.conn.drops shouldBe 6
    subs.foreach(_._2.prefix.refCnt() shouldBe 0)
    fx.srv.received.isEmpty shouldBe true

  test("handoff over budget drops on the caller thread"):
    // Accepted while the queued bytes are below the budget: 512K + 512K reaches it exactly, so the third is dropped.
    // One stream, so the per-message charge cap (the stream high-water mark) is the whole budget.
    val fx = fixture(QuicConfig(connectionBudgetBytes = 1024 * 1024, streamsPerConnection = 1))
    fx.dialAndWait()
    blockLoop(fx.loop)
    val subs = (0 until 3).map(i => fx.submit(sized(i, 512 * 1024)))
    subs.map(_._1) shouldBe Seq(true, true, false)
    subs(2)._2.prefix.refCnt() shouldBe 0 // released on this thread, before the loop ran
    fx.conn.drops shouldBe 1
    cleanups.foreach(f => f())
    awaitAll(fx.srv, 2).map(seqOf).toSet shouldBe Set(0L, 1L)

  test("handoff below budget accepts a message that overshoots it"):
    // 512K queued is below the 1 MiB budget, so a 4 MiB message is accepted; the queue then exceeds the budget
    val fx = fixture(QuicConfig(connectionBudgetBytes = 1024 * 1024, streamsPerConnection = 1))
    fx.dialAndWait()
    blockLoop(fx.loop)
    val first = fx.submit(sized(0, 512 * 1024))
    val big = fx.submit(sized(1, 4 * 1024 * 1024))
    val third = fx.submit(sized(2, 1025))
    (first._1, big._1, third._1) shouldBe (true, true, false)
    third._2.prefix.refCnt() shouldBe 0
    fx.conn.drops shouldBe 1
    cleanups.foreach(f => f())
    awaitAll(fx.srv, 2).map(seqOf).toSet shouldBe Set(0L, 1L)
    first._2.prefix.refCnt() shouldBe 0
    big._2.prefix.refCnt() shouldBe 0

  test("each message counts at most one stream's high-water mark against the handoff budget"):
    // 1 MiB over 4 streams: 256K high water. Four 512K messages are charged 256K each and fill the budget, one per
    // stream; the fifth is dropped.
    val fx = fixture(QuicConfig(connectionBudgetBytes = 1024 * 1024))
    fx.dialAndWait()
    blockLoop(fx.loop)
    val subs = (0 until 5).map(i => fx.submit(sized(i, 512 * 1024)))
    subs.map(_._1) shouldBe Seq(true, true, true, true, false)
    subs(4)._2.prefix.refCnt() shouldBe 0
    cleanups.foreach(f => f())
    val got = awaitAll(fx.srv, 4)
    got.map(seqOf).toSet shouldBe Set(0L, 1L, 2L, 3L)
    got.map(streamId).toSet.size shouldBe 4
    fx.conn.drops shouldBe 1

  test("pre-handshake queue accepts while below budget"):
    val fx = fixture(QuicConfig(connectionBudgetBytes = 1024 * 1024, streamsPerConnection = 1))
    // One at a time, so each reaches the pre-handshake queue before the next is submitted
    val subs = Seq(sized(0, 512 * 1024), sized(1, 4 * 1024 * 1024), sized(2, 512 * 1024)).zipWithIndex.map:
      (m, i) =>
        val r = fx.submit(m)
        eventually(s"drain of $i")(fx.conn.drainedItems == i + 1)
        r
    subs.map(_._1).forall(identity) shouldBe true
    fx.conn.drops shouldBe 1 // the third found 512K + 1 MiB (the capped 4 MiB) held, above the budget
    subs(2)._2.prefix.refCnt() shouldBe 0
    fx.dialAndWait()
    awaitAll(fx.srv, 2).map(seqOf).toSet shouldBe Set(0L, 1L)
    subs.foreach(_._2.prefix.refCnt() shouldBe 0)

  test("message larger than budget is accepted when queue is empty"):
    val fx = fixture(QuicConfig(connectionBudgetBytes = 1024 * 1024))
    fx.dialAndWait()
    val msg = sized(7, 4 * 1024 * 1024)
    val (ok, enc) = fx.submit(msg)
    ok shouldBe true
    val r = fx.srv.awaitReceived()
    val got = r.message.asInstanceOf[OpportunisticRebuild]
    got.pointer.id shouldBe msg.pointer.id
    got.data.size shouldBe msg.data.size
    java.util.Arrays.equals(got.data.getByteArray, msg.data.getByteArray) shouldBe true
    enc.prefix.refCnt() shouldBe 0
    fx.conn.drops shouldBe 0

  test("stalled peer causes drops, not queueing, and recovers"):
    // 4 streams x (256 KiB flow-control window + 512 KiB high water + one message) is far below 200 x 64 KiB
    val config = QuicConfig(initialMaxStreamDataUnidirectional = 256 * 1024, connectionBudgetBytes = 2 * 1024 * 1024)
    val fx = fixture(config, stall = true)
    fx.dialAndWait()
    val msgSize = 64 * 1024
    val subs = (0 until 200).map(i => fx.submit(sized(i, msgSize)))
    val callerDrops = subs.count(!_._1)
    val barrier = fx.conn.postBarrier()
    eventually("drain to finish")(fx.conn.drainedItems == 201 - callerDrops)
    fx.conn.drops should be > callerDrops.toLong // some were dropped on the loop for lack of a writable stream
    val high = config.connectionBudgetBytes / config.streamsPerConnection
    fx.conn.maxPendingStreamBytes should be <= (high + msgSize)
    barrier.isDone shouldBe false // stuck writes on stalled streams are still in flight

    fx.srv.unstallStreams()
    barrier.await(AwaitMillis) shouldBe true
    eventually("pending writes to complete")(fx.conn.pendingStreamBytes == 0)
    subs.foreach(_._2.prefix.refCnt() shouldBe 0)
    val dropsBefore = fx.conn.drops
    (1000 until 1010).foreach(i => fx.submit(sized(i, msgSize))._1 shouldBe true)
    eventually("new sends to arrive")(fx.srv.received.asScala.map(seqOf).count(_ >= 1000) == 10)
    fx.conn.drops shouldBe dropsBefore

  test("a burst from many threads schedules few drains"):
    val fx = fixture()
    fx.dialAndWait()
    val threads = 8
    val perThread = 1000
    val start = new CountDownLatch(1)
    val failures = new ConcurrentLinkedQueue[Throwable]()
    // Stream-sized (just over the datagram threshold) so delivery is reliable and "all delivered" is checkable
    val workers = (0 until threads).map: t =>
      val th = new Thread(runnable:
        try
          start.await()
          (0 until perThread).foreach: i =>
            val enc = QuicMessageCodec.encode(sized(t * perThread + i, 1100), PooledByteBufAllocator.DEFAULT)
            if !fx.conn.submit(enc) then
              failures.add(new AssertionError("submit dropped"))
        catch
          case e: Throwable => failures.add(e)
      )
      th.setDaemon(true)
      th.start()
      th
    start.countDown()
    workers.foreach(_.join(AwaitMillis))
    failures.asScala shouldBe empty
    val got = awaitAll(fx.srv, threads * perThread)
    got.map(seqOf).toSet.size shouldBe threads * perThread
    fx.conn.drops shouldBe 0
    fx.conn.drainExecutions should be < (threads * perThread / 10).toLong

  test("concurrent submits from one thread keep their order"):
    val fx = fixture()
    fx.dialAndWait()
    val n = 500
    val failures = new ConcurrentLinkedQueue[Throwable]()
    val th = new Thread(runnable:
      try
        (0 until n).foreach: i =>
          if !fx.conn.submit(QuicMessageCodec.encode(sized(i, 4096), PooledByteBufAllocator.DEFAULT)) then
            failures.add(new AssertionError(s"submit $i dropped"))
      catch
        case e: Throwable => failures.add(e)
    )
    th.start()
    th.join(AwaitMillis)
    failures.asScala shouldBe empty
    val got = awaitAll(fx.srv, n)
    got.map(seqOf).toSet shouldBe (0L until n.toLong).toSet
    got.groupBy(streamId).values.foreach: rs =>
      val seqs = rs.map(seqOf)
      seqs shouldBe seqs.sorted
    fx.conn.drops shouldBe 0

  test("barrier waits only for earlier writes"):
    val config = QuicConfig(initialMaxStreamDataUnidirectional = 256 * 1024)
    val fx = fixture(config, stall = true)
    fx.dialAndWait()
    // Earlier writes fit inside the stalled peer's flow-control windows, so QUIC accepts them
    val early = (0 until 8).map(i => fx.submit(sized(i, 16 * 1024)))
    early.map(_._1).forall(identity) shouldBe true
    val barrier = fx.conn.postBarrier()
    // Later writes overflow the windows and stay in flight while the peer is stalled
    val keepGoing = new AtomicBoolean(true)
    val later = new Thread(runnable:
      var i = 100
      while keepGoing.get() do
        fx.conn.submit(QuicMessageCodec.encode(sized(i, 128 * 1024), PooledByteBufAllocator.DEFAULT))
        i += 1
        Thread.sleep(1)
    )
    later.setDaemon(true)
    later.start()
    cleanups += (() => { keepGoing.set(false); later.join(AwaitMillis) })
    try
      barrier.await(AwaitMillis) shouldBe true
      barrier.isSuccess shouldBe true
      later.isAlive shouldBe true
      early.foreach(_._2.prefix.refCnt() shouldBe 0)
      // At most the streams' flow-control windows can complete while stalled, so pending beyond that is stuck
      val windows = config.streamsPerConnection * config.initialMaxStreamDataUnidirectional
      eventually("later writes to be stuck in flight")(fx.conn.pendingStreamBytes > windows)
    finally
      keepGoing.set(false)
      later.join(AwaitMillis)

    // A barrier posted after stuck writes waits for them
    val second = fx.conn.postBarrier()
    second.await(300) shouldBe false
    fx.srv.unstallStreams()
    second.await(AwaitMillis) shouldBe true

  test("submit after close returns false and releases"):
    val fx = fixture()
    fx.dialAndWait()
    onLoop(fx.loop)(fx.conn.close())
    fx.conn.isClosed shouldBe true
    fx.closedCalls.size shouldBe 1
    val (ok, enc) = fx.submit(sized(1, 200))
    ok shouldBe false
    enc.prefix.refCnt() shouldBe 0
    fx.conn.drops shouldBe 1
    fx.conn.postBarrier().isSuccess shouldBe true
    onLoop(fx.loop)(fx.conn.close())
    fx.closedCalls.size shouldBe 1

  test("a channel that closes after attach closes the connection"):
    val fx = fixture()
    val ch = fx.dialAndWait()
    // Stream-sized: a datagram can be lost, and this test only needs the connection to be working
    val (ok, enc) = fx.submit(sized(1, QuicMessageCodec.DatagramThreshold + 1))
    ok shouldBe true
    fx.srv.awaitReceived()
    ch.close().await(AwaitMillis) shouldBe true
    eventually("onClosed")(fx.closedCalls.size == 1)
    fx.conn.isClosed shouldBe true
    enc.prefix.refCnt() shouldBe 0

  test("no buffer leaks were reported"):
    leakWatch.leaks() shouldBe Nil

object QuicConnectionSuite:
  private val toStore = StoreId(PoolId.BootstrapPoolId, 0)
  private val fromClient = ClientId(UUID.randomUUID())
  private val revision = ObjectRevision(TransactionId(UUID.randomUUID()))

  private def rebuildOf(seq: Long, data: DataBuffer): OpportunisticRebuild =
    val ptr = DataObjectPointer(ObjectId(new UUID(0, seq)), PoolId.BootstrapPoolId, Array[Byte](1))
    OpportunisticRebuild(toStore, fromClient, ptr, revision, ObjectRefcount(1, 1), HLCTimestamp(1000L), data)

  /** An OpportunisticRebuild carrying `seq` (as its object id) whose encoded size is exactly `total` bytes */
  def sized(seq: Long, total: Int): OpportunisticRebuild =
    val empty = QuicMessageCodec.encode(rebuildOf(seq, DataBuffer.Empty), PooledByteBufAllocator.DEFAULT)
    val base = empty.size.toInt
    empty.release()
    val payload = new Array[Byte](total - base)
    var i = 0
    while i < payload.length do
      payload(i) = (i * 31 + seq).toByte
      i += 1
    rebuildOf(seq, DataBuffer(payload))

  def runnable(body: => Unit): Runnable = () => body

  /** Runs `f` on `loop` and waits for it */
  def onLoop[T](loop: EventLoop)(f: => T): T =
    if loop.inEventLoop() then
      f
    else
      val fut = loop.submit(() => f)
      if !fut.await(QuicTestPeer.AwaitMillis) then
        throw new AssertionError("loop task timed out")
      if !fut.isSuccess then
        throw fut.cause()
      fut.getNow
