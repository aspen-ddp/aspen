package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.buffer.PooledByteBufAllocator
import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.*
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.collection.mutable.ArrayBuffer

class QuicNetRoutingSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach:
  import QuicNetFixture.*

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
    fx = new QuicNetFixture()

  override def afterEach(): Unit =
    fx.close()

  // ---------------------------------------------------------------------------------------------------------------

  private def bytes(n: Int, seed: Int = 0): Array[Byte] = Array.tabulate[Byte](n)(i => (i * 31 + seed).toByte)

  private def ptr: DataObjectPointer =
    DataObjectPointer(ObjectId(UUID.randomUUID()), PoolId.BootstrapPoolId, Array[Byte](1))

  private def read(to: StoreId, from: ClientId): Read = Read(to, from, UUID.randomUUID(), ptr, FullObject())

  private def response(to: ClientId, from: StoreId, readUUID: UUID): ReadResponse =
    ReadResponse(to, from, readUUID, HLCTimestamp(1000L), Left(ReadError.ObjectNotFound))

  private def txHeartbeat(to: StoreId, from: StoreId): TxHeartbeat =
    TxHeartbeat(to, from, TransactionId(UUID.randomUUID()))

  private def transferData(to: StoreId, size: Int, seed: Int = 0): OpportunisticRebuild = rebuild(to, size, seed)

  private def prepare(to: StoreId, from: StoreId, updates: List[ObjectUpdate]): TxPrepare =
    val p = ptr
    val ida = Replication(4, 3)
    val txd = TransactionDescription(TransactionId(UUID.randomUUID()), HLCTimestamp(1000L), p, 1.toByte,
      List(DataUpdate(p, ObjectRevision(TransactionId(UUID.randomUUID())), DataUpdateOperation.Overwrite)),
      Nil, None, Nil, Nil, ida, Map(p.poolId -> ida))
    TxPrepare(to, from, txd, ProposalId(1, 1.toByte), updates, Nil)

  private def entryFor(net: QuicNet, hostId: HostId): QuicHostEntry =
    net.metadataManager.peekHostEntry(hostId).getOrElse(fail(s"no host entry for $hostId"))

  // ---------------------------------------------------------------------------------------------------------------

  test("client request reaches the host owning the store"):
    val c = fx.client()
    val reqB = read(fx.storesB.head, c.net.clientId)
    val reqA = read(fx.storesA(1), c.net.clientId)
    c.net.clientMessenger.sendClientRequest(reqB)
    c.net.clientMessenger.sendClientRequest(reqA)
    await(fx.b.handler.clientRequests, 1).map(_.message) shouldBe List(reqB)
    await(fx.a.handler.clientRequests, 1).map(_.message) shouldBe List(reqA)
    fx.a.handler.clientRequests.isEmpty shouldBe true
    fx.b.handler.clientRequests.isEmpty shouldBe true

  test("host response reaches the requesting client over the client's connection"):
    val c = fx.client()
    c.net.ohost shouldBe None
    val req = read(fx.storesB.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    await(fx.b.handler.clientRequests, 1)
    fx.b.net.clientRoutes shouldBe 1
    val resp = response(c.net.clientId, req.toStore, req.readUUID)
    fx.b.net.serverMessenger.sendClientResponse(resp)
    await(c.handler.clientResponses, 1).map(_.message) shouldBe List(resp)
    // B replied over the client's connection rather than dialing anything. B's own count is not checked: as a host
    // it dials A for heartbeats on its first liveness tick.
    c.net.connectionCount shouldBe 1

  test("TxMessages flow host to host in both directions"):
    val aToB = txHeartbeat(fx.storesB.head, fx.storesA.head)
    val bToA = txHeartbeat(fx.storesA.head, fx.storesB.head)
    fx.a.net.serverMessenger.sendTransactionMessage(aToB)
    fx.b.net.serverMessenger.sendTransactionMessage(bToA)
    await(fx.b.handler.txMessages, 1).map(_.message) shouldBe List(aToB)
    await(fx.a.handler.txMessages, 1).map(_.message) shouldBe List(bToA)
    eventually("one outbound and one inbound connection on each host"):
      fx.a.net.connectionCount == 2 && fx.b.net.connectionCount == 2

  test("HostMessage reaches the target host"):
    val c = fx.client()
    val m = CheckStorageDevice(fx.hostIdA, c.net.clientId,
      org.aspen_ddp.aspen.common.metadata.StorageDeviceId(UUID.randomUUID()))
    c.net.clientMessenger.sendHostMessage(m)
    await(fx.a.handler.hostMessages, 1).map(_.message) shouldBe List(m)
    fx.b.handler.total shouldBe 0

  test("TxPrepare with 8 MiB of updates arrives intact"):
    val updates = (0 until 8).toList.map(i => ObjectUpdate(ObjectId(UUID.randomUUID()), DataBuffer(bytes(1024 * 1024, i))))
    val m = prepare(fx.storesB.head, fx.storesA.head, updates)
    fx.a.net.serverMessenger.sendTransactionMessage(m)
    val got = await(fx.b.handler.txMessages, 1).head.message.asInstanceOf[TxPrepare]
    got.txd shouldBe m.txd
    got.objectUpdates.map(_.objectId) shouldBe m.objectUpdates.map(_.objectId)
    got.objectUpdates.zip(m.objectUpdates).foreach: (g, e) =>
      g.data.size shouldBe e.data.size
      java.util.Arrays.equals(g.data.getByteArray, e.data.getByteArray) shouldBe true

  test("three back-to-back 16 MiB messages to an idle connection all arrive"):
    val entry = entryFor(fx.a.net, fx.hostIdB)
    val conn = fx.a.net.connectionFor(entry)
    eventually("connection active")(conn.isActive)
    val msgs = (0 until 3).map(i => transferData(fx.storesB.head, 16 * 1024 * 1024, i))
    msgs.foreach(m => fx.a.net.sendToHost(entry, m)) // faster than the loop drains: all three queue at once
    val got = await(fx.b.handler.clientRequests, 3, 30000).map(_.message.asInstanceOf[OpportunisticRebuild])
    got.map(_.pointer.id).toSet shouldBe msgs.map(_.pointer.id).toSet
    got.foreach: g =>
      val e = msgs.find(_.pointer.id == g.pointer.id).get
      java.util.Arrays.equals(g.data.getByteArray, e.data.getByteArray) shouldBe true
    conn.drops shouldBe 0

  test("a host whose address is a name is resolved and reached"):
    val c = fx.client()
    val entry = new QuicHostEntry(fx.hostIdB, "host_b_by_name", "localhost", fx.portB, 0, 0)
    val m = CheckStorageDevice(fx.hostIdB, c.net.clientId,
      org.aspen_ddp.aspen.common.metadata.StorageDeviceId(UUID.randomUUID()))
    c.net.sendToHost(entry, m)
    await(fx.b.handler.hostMessages, 1).map(_.message) shouldBe List(m)

  test("two clients sharing one certificate get only their own responses"):
    val creds = credentials()
    val c1 = fx.client(creds = creds)
    val c2 = fx.client(creds = creds)
    val r1 = read(fx.storesA.head, c1.net.clientId)
    val r2 = read(fx.storesA.head, c2.net.clientId)
    c1.net.clientMessenger.sendClientRequest(r1)
    c2.net.clientMessenger.sendClientRequest(r2)
    await(fx.a.handler.clientRequests, 2)
    fx.a.net.clientRoutes shouldBe 2
    val p1 = response(c1.net.clientId, r1.toStore, r1.readUUID)
    val p2 = response(c2.net.clientId, r2.toStore, r2.readUUID)
    fx.a.net.serverMessenger.sendClientResponse(p2)
    fx.a.net.serverMessenger.sendClientResponse(p1)
    await(c1.handler.clientResponses, 1).map(_.message) shouldBe List(p1)
    await(c2.handler.clientResponses, 1).map(_.message) shouldBe List(p2)
    Thread.sleep(200)
    c1.handler.clientResponses.isEmpty shouldBe true
    c2.handler.clientResponses.isEmpty shouldBe true

  test("host can send to its own stores"):
    val req = read(fx.storesA.head, fx.a.net.clientId)
    fx.a.net.clientMessenger.sendClientRequest(req)
    await(fx.a.handler.clientRequests, 1).map(_.message) shouldBe List(req)
    val resp = response(req.fromClient, req.toStore, req.readUUID)
    fx.a.net.serverMessenger.sendClientResponse(resp)
    await(fx.a.handler.clientResponses, 1).map(_.message) shouldBe List(resp)
    fx.b.handler.total shouldBe 0

  test("responses to a departed client are dropped quietly"):
    val c = fx.client()
    val req = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    await(fx.a.handler.clientRequests, 1)
    fx.a.net.clientRoutes shouldBe 1
    c.net.closeForTests()
    eventually("host forgets the departed client's route"):
      fx.a.net.clientRoutes == 0
    noException should be thrownBy fx.a.net.serverMessenger.sendClientResponse(
      response(c.net.clientId, req.toStore, req.readUUID))
    Thread.sleep(200)
    c.handler.total shouldBe 0

  test("peer restart mid-transfer recovers"):
    val big = transferData(fx.storesB.head, 64 * 1024 * 1024)
    val enc = QuicMessageCodec.encode(big, PooledByteBufAllocator.DEFAULT)
    val conn = fx.a.net.connectionFor(entryFor(fx.a.net, fx.hostIdB))
    conn.submit(enc) shouldBe true
    Thread.sleep(100)
    val completedBeforeRestart = !fx.b.handler.clientRequests.isEmpty
    info(s"64 MiB message completed before restart: $completedBeforeRestart")
    fx.restartB()

    // A keeps sending until the restarted B gets something; A's stale connection must close and be re-dialed
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
    var delivered = false
    while !delivered && System.nanoTime() - deadline < 0 do
      fx.a.net.clientMessenger.sendClientRequest(transferData(fx.storesB.head, 100))
      delivered = fx.b.handler.clientRequests.poll(200, TimeUnit.MILLISECONDS) != null
    delivered shouldBe true

    eventually("the 64 MiB message's buffers are released", 5000):
      enc.prefix.refCnt() == 0
    conn.isClosed shouldBe true

  test("many threads sending to an unconnected host create exactly one connection"):
    val start = new CountDownLatch(1)
    val n = 16
    val threads = (0 until n).map: i =>
      val t = new Thread(() =>
        start.await()
        fx.a.net.serverMessenger.sendTransactionMessage(txHeartbeat(fx.storesB(i % 2), fx.storesA.head)))
      t.start()
      t
    start.countDown()
    threads.foreach(_.join(AwaitMillis))
    await(fx.b.handler.txMessages, n)
    // A's only possible outbound peer is B (its heartbeats dial B too, through the same CAS)
    fx.a.net.outboundConnectionCount shouldBe 1

  test("many threads sending concurrently lose nothing under budget"):
    // Stream-sized only: datagrams are unreliable by design, so "nothing lost" cannot be asserted for them
    val nThreads = 8
    val perThread = 200
    val start = new CountDownLatch(1)
    val failures = new ConcurrentLinkedQueue[Throwable]()
    val c = fx.client()
    val threads = (0 until nThreads).map: t =>
      val th = new Thread(() =>
        try
          start.await()
          for i <- 0 until perThread do
            val to = if (i + t) % 2 == 0 then fx.storesA.head else fx.storesB.head
            val size = 1100 + ((i * 7919 + t * 104729) % (16 * 1024))
            c.net.clientMessenger.sendClientRequest(transferData(to, size, i))
        catch case e: Throwable => failures.add(e): Unit)
      th.start()
      th
    start.countDown()
    threads.foreach(_.join(AwaitMillis))
    failures.isEmpty shouldBe true
    val half = nThreads * perThread / 2
    val gotA = await(fx.a.handler.clientRequests, half, 20000)
    val gotB = await(fx.b.handler.clientRequests, half, 20000)
    gotA.forall(_.message.toStore == fx.storesA.head) shouldBe true
    gotB.forall(_.message.toStore == fx.storesB.head) shouldBe true
    // A connection that closed would be null here; the client keeps both alive, so require them open
    val connA = entryFor(c.net, fx.hostIdA).connection.get()
    val connB = entryFor(c.net, fx.hostIdB).connection.get()
    connA should not be null
    connB should not be null
    connA.drops shouldBe 0
    connB.drops shouldBe 0

  test("MessageHandler callbacks run on event loop threads"):
    val c = fx.client()
    val req = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    val hostSide = await(fx.a.handler.clientRequests, 1).head
    hostSide.threadName should startWith("aspen-quic")
    fx.a.net.serverMessenger.sendClientResponse(response(c.net.clientId, req.toStore, req.readUUID))
    await(c.handler.clientResponses, 1).head.threadName should startWith("aspen-quic")
    fx.a.net.serverMessenger.sendTransactionMessage(txHeartbeat(fx.storesB.head, fx.storesA.head))
    await(fx.b.handler.txMessages, 1).head.threadName should startWith("aspen-quic")

  test("host with eventLoopThreads = 2 serves clients on both loops"):
    // A third host on its own port, so its listener uses exactly 2 loops
    val hid = HostId(UUID.randomUUID())
    val h = fx.host(hid, freeUdpPort(), fx.config.copy(eventLoopThreads = 2))
    if !io.netty.channel.epoll.Epoll.isAvailable then
      cancel("SO_REUSEPORT listener requires epoll")
    // The kernel spreads connections by a hash of the source address, so four clients can all land on one
    // socket (probability 1/8). Keep adding clients, up to 16, until both loops have been used.
    val clients = ArrayBuffer.empty[Node]
    def connectOne(): Unit =
      val c = fx.client(fx.config.copy(eventLoopThreads = 1))
      clients += c
      val m = CheckStorageDevice(hid, c.net.clientId, org.aspen_ddp.aspen.common.metadata.StorageDeviceId(UUID.randomUUID()))
      // The third host is not in the bootstrap config, so reach it through a hand-made entry
      val entry = new QuicHostEntry(hid, "host_c", "127.0.0.1", h.net.ohost.get._2, 0, 0)
      c.net.sendToHost(entry, m)
      await(h.handler.hostMessages, 1).head.message shouldBe m
    (0 until 4).foreach(_ => connectOne())
    while h.net.inboundConnectionLoops.size < 2 && clients.size < 16 do
      connectOne()
    info(s"clients needed: ${clients.size}")
    // Inbound only: as a host, h also dials A and B for heartbeats
    h.net.inboundConnectionCount shouldBe clients.size
    h.net.inboundConnectionLoops.size shouldBe 2

  test("no buffer leaks were reported"):
    leakWatch.leaks() shouldBe Nil
