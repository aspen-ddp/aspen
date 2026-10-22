package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.network.*
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

  /** Long relative to every test, so liveness-tick dials do not disturb the timing assertions */
  private val HeartbeatPeriod = Duration(2, SECONDS)

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
    fx = new QuicNetFixture(heartbeatPeriod = HeartbeatPeriod)

  override def afterEach(): Unit =
    fx.close()

  // ---------------------------------------------------------------------------------------------------------------

  private def ptr: DataObjectPointer =
    DataObjectPointer(ObjectId(UUID.randomUUID()), PoolId.BootstrapPoolId, Array[Byte](1))

  private def read(to: StoreId, from: ClientId): Read = Read(to, from, UUID.randomUUID(), ptr, FullObject())

  /** Stream-sized */
  private def transferData(to: HostId): StoreTransferData =
    StoreTransferData(to, ClientId(UUID.randomUUID()), UUID.randomUUID(), DataBuffer(new Array[Byte](4096)))

  private def checkDevice(to: HostId, from: ClientId): CheckStorageDevice =
    CheckStorageDevice(to, from, StorageDeviceId(UUID.randomUUID()))

  private def elapsedMillis(start: Long): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)

  private def callable[T](body: => T): Callable[T] = new Callable[T]:
    def call(): T = body

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
    (0 until 25).foreach: i =>
      val store = if i % 2 == 0 then fx.storesA(i % 2) else fx.storesB(i % 2)
      c.net.clientMessenger.sendClientRequest(read(store, c.net.clientId))
      c.net.clientMessenger.sendHostMessage(transferData(if i % 2 == 0 then fx.hostIdB else fx.hostIdA))
    c.net.awaitPendingMessagesSent(Duration(5, SECONDS)) shouldBe true
    await(fx.a.handler.clientRequests, 13)
    await(fx.b.handler.clientRequests, 12)
    await(fx.a.handler.hostMessages, 12)
    await(fx.b.handler.hostMessages, 13)

  test("flush covers pre-handshake messages"):
    val c = fx.client()
    val req = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    c.net.awaitPendingMessagesSent(Duration(5, SECONDS)) shouldBe true
    await(fx.a.handler.clientRequests, 1).map(_.message) shouldBe List(req)

  test("flush covers messages parked on a metadata lookup"):
    val c = fx.client()
    val lookups = new LookupRecordingClient
    c.net.startIoThread(lookups)

    val poolId = PoolId(UUID.randomUUID())
    val hostId = HostId(UUID.randomUUID())
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
    // Host A's address under a host id the bootstrap config does not know
    lookups.lookupPromise(hostId).success(HostState(hostId, "flush_host", "127.0.0.1", fx.portA, 0, 0, Set()))

    flushed.get(5, TimeUnit.SECONDS) shouldBe true
    await(fx.a.handler.clientRequests, 1).map(_.message) shouldBe List(req)

  test("flush returns false for unreachable host"):
    val c = fx.client()
    val entry = new QuicHostEntry(HostId(UUID.randomUUID()), "nowhere", "127.0.0.1", freeUdpPort(), 0, 0)
    c.net.sendToHost(entry, read(fx.storesA.head, c.net.clientId))
    val start = System.nanoTime()
    var result: Option[Boolean] = None
    noException should be thrownBy:
      result = Some(c.net.awaitPendingMessagesSent(Duration(500, MILLISECONDS)))
    elapsedMillis(start) should be < 1500L
    result shouldBe Some(false)

  test("flush does not wait for messages sent after it was called"):
    val c = fx.client()
    // Connect first, so the sender's messages are written rather than held for the handshake
    c.net.clientMessenger.sendClientRequest(read(fx.storesB.head, c.net.clientId))
    await(fx.b.handler.clientRequests, 1)

    val running = new AtomicBoolean(true)
    val sent = new AtomicLong(0)
    val sender = new Thread(() =>
      while running.get() do
        c.net.clientMessenger.sendHostMessage(transferData(fx.hostIdB))
        sent.incrementAndGet())
    sender.setDaemon(true)
    sender.start()
    try
      eventually("the sender is running"):
        sent.get() > 100
      val start = System.nanoTime()
      c.net.awaitPendingMessagesSent(Duration(5, SECONDS)) shouldBe true
      elapsedMillis(start) should be < 5000L
      sender.isAlive shouldBe true
    finally
      running.set(false)
      sender.join(5000)

  test("send, flush, shutdown delivers"):
    (0 until 20).foreach: i =>
      val c = fx.client(cfg = QuicConfig(eventLoopThreads = 1))
      val m = checkDevice(fx.hostIdA, c.net.clientId)
      c.net.clientMessenger.sendHostMessage(m)
      withClue(s"iteration $i: "):
        c.net.awaitPendingMessagesSent(Duration(5, SECONDS)) shouldBe true
        c.net.shutdown(Duration(1, SECONDS))
        await(fx.a.handler.hostMessages, 1, 5000).map(_.message) shouldBe List(m)

  test("sends after shutdown are dropped without throwing"):
    val c = fx.client()
    c.net.clientMessenger.sendClientRequest(read(fx.storesA.head, c.net.clientId))
    await(fx.a.handler.clientRequests, 1)
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
    c.net.awaitPendingMessagesSent(Duration(1, SECONDS)) shouldBe true

  test("event loop threads are gone after shutdown"):
    val c = fx.client()
    c.net.clientMessenger.sendClientRequest(read(fx.storesA.head, c.net.clientId))
    await(fx.a.handler.clientRequests, 1)
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
