package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.GossipConfig
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{Callable, CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

class QuicNetFlushSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach:
  import QuicNetFixture.*

  /** Sets the idle timeout (3x, so 6 s): long relative to every test, so no connection idles out mid-test */
  private val HeartbeatPeriod = Duration(2, SECONDS)

  /** Host membership's background traffic (pings, syncs, gossip rounds) kept out of the timing windows: after the
    * initial join, nothing runs more often than every few seconds, and nothing is declared dead within a test
    */
  private val SlowGossip = GossipConfig(pingInterval = Duration(5, SECONDS), pingTimeout = Duration(2, SECONDS),
    syncInterval = Duration(10, SECONDS), gossipInterval = Duration(2, SECONDS), suspicionMult = 5)

  private var previousLevel: ResourceLeakDetector.Level = scala.compiletime.uninitialized
  private var leakWatch: QuicTestPeer.LeakWatch = scala.compiletime.uninitialized
  private var fx: QuicNetFixture = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    previousLevel = ResourceLeakDetector.getLevel
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)
    leakWatch = QuicTestPeer.watchLeaks()

  override def afterAll(): Unit =
    leakWatch.stop()
    ResourceLeakDetector.setLevel(previousLevel)

  override def beforeEach(): Unit =
    fx = new QuicNetFixture(QuicConfig(gossip = SlowGossip), HeartbeatPeriod)

  override def afterEach(): Unit =
    fx.close()

  // ---------------------------------------------------------------------------------------------------------------

  private def ptr: DataObjectPointer =
    DataObjectPointer(ObjectId(UUID.randomUUID()), PoolId.BootstrapPoolId, Array[Byte](1))

  private def read(to: StoreId, from: ClientId): Read = Read(to, from, UUID.randomUUID(), ptr, FullObject())

  /** Stream-sized */
  private def rebuildData(to: StoreId): OpportunisticRebuild = rebuild(to, 4096)

  private def checkDevice(to: HostId, from: ClientId): CheckStorageDevice =
    CheckStorageDevice(to, from, StorageDeviceId(UUID.randomUUID()))

  private def elapsedMillis(start: Long): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)

  private def callable[T](body: => T): Callable[T] = new Callable[T]:
    def call(): T = body

  /** Takes the first delivery from `q`, calling `resend` whenever one second goes by without it. Datagrams are
    * unreliable, and under load the first one on a new connection is occasionally lost. `resend` must send the same
    * message again.
    */
  private def awaitOrResend[M](q: java.util.concurrent.LinkedBlockingQueue[Delivery[M]])(resend: => Unit): Delivery[M] =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(AwaitMillis)
    var d: Delivery[M] = null
    while d == null do
      if System.nanoTime() - deadline > 0 then
        throw new AssertionError(s"no message received within ${AwaitMillis}ms")
      d = q.poll(LoadProbe.scaleMillis(1000), TimeUnit.MILLISECONDS)
      if d == null then resend
    d

  private def flushOnThread(net: QuicNet, timeout: Duration): CompletableFuture[Boolean] =
    val f = new CompletableFuture[Boolean]()
    val t = new Thread(() =>
      try f.complete(net.awaitPendingMessagesSent(timeout))
      catch case e: Throwable => f.completeExceptionally(e)
      ())
    t.setDaemon(true)
    t.start()
    f

  // ---------------------------------------------------------------------------------------------------------------

  test("flush returns true after previously sent messages are accepted"):
    val c = fx.client()
    // The counts below need every datagram delivered, and the first one on a new connection is occasionally lost under
    // load. Stream-sized messages are reliable, so connect to both hosts with those first.
    c.net.clientMessenger.sendClientRequest(rebuildData(fx.storesA.head))
    c.net.clientMessenger.sendClientRequest(rebuildData(fx.storesB.head))
    await(fx.a.handler.clientRequests, 1)
    await(fx.b.handler.clientRequests, 1)
    (0 until 25).foreach: i =>
      val store = if i % 2 == 0 then fx.storesA(i % 2) else fx.storesB(i % 2)
      c.net.clientMessenger.sendClientRequest(read(store, c.net.clientId))
      c.net.clientMessenger.sendClientRequest(rebuildData(if i % 2 == 0 then fx.storesB.head else fx.storesA.head))
    c.net.awaitPendingMessagesSent(LoadProbe.scale(Duration(5, SECONDS))) shouldBe true
    // Host A receives 13 reads and 12 transfers; host B receives 12 reads and 13 transfers
    val gotA = await(fx.a.handler.clientRequests, 25).map(_.message)
    val gotB = await(fx.b.handler.clientRequests, 25).map(_.message)
    gotA.count(_.isInstanceOf[Read]) shouldBe 13
    gotA.count(_.isInstanceOf[OpportunisticRebuild]) shouldBe 12
    gotB.count(_.isInstanceOf[Read]) shouldBe 12
    gotB.count(_.isInstanceOf[OpportunisticRebuild]) shouldBe 13

  test("flush covers pre-handshake messages"):
    val c = fx.client()
    val req = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    c.net.awaitPendingMessagesSent(LoadProbe.scale(Duration(5, SECONDS))) shouldBe true
    awaitOrResend(fx.a.handler.clientRequests)(c.net.clientMessenger.sendClientRequest(req)).message shouldBe req

  test("flush covers messages parked on a metadata lookup"):
    val c = fx.client()
    val lookups = new LookupRecordingClient
    c.net.startIoThread(lookups)

    val poolId = PoolId(UUID.randomUUID())
    // A host the bootstrap config does not know, so its endpoint needs a lookup
    val hostId = HostId(UUID.randomUUID())
    val port = freeUdpPort()
    val target = fx.host(hostId, port)
    val req = read(StoreId(poolId, 0.toByte), c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    c.net.metadataManager.hasParkedMessages shouldBe true

    val flushed = flushOnThread(c.net, Duration(5, SECONDS))
    Thread.sleep(100)
    flushed.isDone shouldBe false

    lookups.poolLookupPromise(poolId).success(StoragePoolState(
      poolId, "flush_pool", Replication(1, 1), None,
      Array(StoragePoolState.StoreEntry(hostId, StorageDeviceId(UUID.randomUUID()))),
      RocksDBConfig(), StorageDeviceSetId(UUID.randomUUID())))
    lookups.lookupPromise(hostId).success(HostState(hostId, "flush_host", "127.0.0.1", port, 0, 0, Set()))

    flushed.get(LoadProbe.scaleMillis(5000), TimeUnit.MILLISECONDS) shouldBe true
    awaitOrResend(target.handler.clientRequests)(c.net.clientMessenger.sendClientRequest(req)).message shouldBe req

  test("flush returns false for unreachable host"):
    val c = fx.client()
    val entry = new QuicHostEntry(HostId(UUID.randomUUID()), "nowhere", "127.0.0.1", freeUdpPort(), 0, 0)
    c.net.sendToHost(entry, read(fx.storesA.head, c.net.clientId))
    val start = System.nanoTime()
    var result: Option[Boolean] = None
    noException should be thrownBy:
      result = Some(c.net.awaitPendingMessagesSent(Duration(500, MILLISECONDS)))
    elapsedMillis(start) should be < 10000L
    result shouldBe Some(false)

  test("a blocking name lookup blocks neither the sender nor flush"):
    val release = new java.util.concurrent.CountDownLatch(1)
    val resolving = new java.util.concurrent.CountDownLatch(1)
    val c = fx.client(resolve = name =>
      resolving.countDown()
      release.await(30, TimeUnit.SECONDS)
      throw new java.net.UnknownHostException(name))
    try
      val entry = new QuicHostEntry(HostId(UUID.randomUUID()), "slow_dns", "slow.invalid", fx.portA, 0, 0)
      val start = System.nanoTime()
      c.net.sendToHost(entry, read(fx.storesA.head, c.net.clientId))
      c.net.sendToHost(entry, read(fx.storesA.head, c.net.clientId))
      elapsedMillis(start) should be < 5000L // not blocked for the 30 s the lookup takes
      resolving.await(5, TimeUnit.SECONDS) shouldBe true
      val flushStart = System.nanoTime()
      c.net.awaitPendingMessagesSent(Duration(300, MILLISECONDS)) shouldBe false
      elapsedMillis(flushStart) should be < 10000L
    finally
      release.countDown()

  test("a failed name lookup drops queued messages and completes barriers"):
    val c = fx.client(resolve = name => throw new java.net.UnknownHostException(name))
    val entry = new QuicHostEntry(HostId(UUID.randomUUID()), "no_dns", "nowhere.invalid", fx.portA, 0, 0)
    val conn = c.net.connectionFor(entry)
    val encs = (0 until 3).map: i =>
      val e = QuicMessageCodec.encode(if i == 0 then read(fx.storesA.head, c.net.clientId) else rebuildData(fx.storesA.head),
        io.netty.buffer.PooledByteBufAllocator.DEFAULT)
      conn.submit(e)
      e
    val barrier = conn.postBarrier()
    barrier.await(5000) shouldBe true
    eventually("the connection closed")(conn.isClosed)
    conn.drops shouldBe 3
    encs.foreach(_.prefix.refCnt() shouldBe 0)
    entry.connection.get() shouldBe null
    c.net.awaitPendingMessagesSent(LoadProbe.scale(Duration(1, SECONDS))) shouldBe true

  test("a second host on a port in use fails clearly and leaves nothing behind"):
    // Built on a thread of its own ThreadGroup: every thread the failed net starts inherits it, so the group's
    // emptiness shows the failed net's loops and resolver are gone, regardless of other suites running alongside.
    val tg = new ThreadGroup("second-host")
    var thrown: Throwable = null
    val body: Runnable = () =>
      try fx.host(HostId(UUID.randomUUID()), fx.portA)
      catch case e: Throwable => thrown = e
    val t = new Thread(tg, body)
    t.setDaemon(true)
    t.start()
    t.join(10000)
    thrown shouldBe a [java.net.BindException]
    thrown.getMessage shouldBe s"QUIC data port ${fx.portA} is already in use"
    eventually("the failed net's threads have exited", 5000)(tg.activeCount() == 0)
    // Host A is unaffected
    val c = fx.client()
    val req = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    awaitOrResend(fx.a.handler.clientRequests)(c.net.clientMessenger.sendClientRequest(req)).message shouldBe req

  test("flush does not wait for messages sent after it was called"):
    val c = fx.client()
    // Connect first, so the sender's messages are written rather than held for the handshake
    val first = read(fx.storesB.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(first)
    awaitOrResend(fx.b.handler.clientRequests)(c.net.clientMessenger.sendClientRequest(first))

    val running = new AtomicBoolean(true)
    val sent = new AtomicLong(0)
    val sender = new Thread(() =>
      while running.get() do
        c.net.clientMessenger.sendClientRequest(rebuildData(fx.storesB.head))
        sent.incrementAndGet())
    sender.setDaemon(true)
    sender.start()
    try
      eventually("the sender is running"):
        sent.get() > 100
      val start = System.nanoTime()
      c.net.awaitPendingMessagesSent(LoadProbe.scale(Duration(5, SECONDS))) shouldBe true
      elapsedMillis(start) should be < 5000L
      sender.isAlive shouldBe true
    finally
      running.set(false)
      sender.join(5000)

  test("send, flush, shutdown delivers"):
    (0 until 20).foreach: i =>
      val c = fx.client(cfg = QuicConfig(eventLoopThreads = 1))
      // Stream-sized: a datagram can be lost, and this test is about what shutdown does to accepted messages
      val m = rebuildData(fx.storesA.head)
      c.net.clientMessenger.sendClientRequest(m)
      withClue(s"iteration $i: "):
        c.net.awaitPendingMessagesSent(LoadProbe.scale(Duration(5, SECONDS))) shouldBe true
        // Short linger: on loopback quiche transmits flushed bytes in well under a millisecond
        c.net.shutdown(LoadProbe.scale(Duration(50, MILLISECONDS)))
        await(fx.a.handler.clientRequests, 1).map(_.message) match
          case List(got: OpportunisticRebuild) => got.pointer shouldBe m.pointer
          case other => fail(s"unexpected deliveries $other")

  test("sends after shutdown are dropped without throwing"):
    val c = fx.client()
    val first = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(first)
    awaitOrResend(fx.a.handler.clientRequests)(c.net.clientMessenger.sendClientRequest(first))
    c.net.shutdown(Duration.Zero)

    noException should be thrownBy:
      c.net.clientMessenger.sendClientRequest(read(fx.storesA.head, c.net.clientId))
      c.net.clientMessenger.sendHostMessage(checkDevice(fx.hostIdB, c.net.clientId))
      c.net.clientMessenger.sendTransactionMessages(Nil)
      c.net.serverMessenger.sendClientResponse(
        ReadResponse(c.net.clientId, fx.storesA.head, UUID.randomUUID(),
          org.aspen_ddp.aspen.common.HLCTimestamp(1000L), Left(ReadError.ObjectNotFound)))
      c.net.shutdown(Duration.Zero) // idempotent
    Thread.sleep(300)
    fx.a.handler.total shouldBe 0
    fx.b.handler.total shouldBe 0
    c.net.awaitPendingMessagesSent(LoadProbe.scale(Duration(1, SECONDS))) shouldBe true

  test("event loop threads are gone after shutdown"):
    val c = fx.client()
    val first = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(first)
    awaitOrResend(fx.a.handler.clientRequests)(c.net.clientMessenger.sendClientRequest(first))
    // Other suites may run QuicNets in this JVM at the same time, so this net's own loop threads are checked
    val threads = c.net.eventLoopsForTests.map(l => l.submit(callable(Thread.currentThread())).get(5, TimeUnit.SECONDS))
    threads should not be empty
    threads.foreach(_.getName should startWith("aspen-quic"))

    c.net.shutdown(Duration.Zero)
    eventually("no aspen-quic loop thread of the shut-down net is alive", 2000):
      threads.forall(!_.isAlive)

    val joined = new CompletableFuture[Unit]()
    val joiner = new Thread(() =>
      c.net.joinIoThread()
      joined.complete(())
      ())
    joiner.setDaemon(true)
    joiner.start()
    joined.get(2, TimeUnit.SECONDS)

  test("flush on an event loop thread is rejected"):
    val c = fx.client()
    val f = c.net.eventLoopsForTests.last.submit(callable(c.net.awaitPendingMessagesSent(Duration(1, SECONDS))))
    val e = intercept[java.util.concurrent.ExecutionException](f.get(5, TimeUnit.SECONDS))
    e.getCause shouldBe a[IllegalStateException]
