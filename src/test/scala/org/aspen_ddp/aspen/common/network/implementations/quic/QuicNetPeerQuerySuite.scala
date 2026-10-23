package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.buffer.PooledByteBufAllocator
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.GossipConfig
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.netty.handler.codec.quic.QuicChannel

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt

class QuicNetPeerQuerySuite extends AnyFunSuite with Matchers with BeforeAndAfterEach:
  import QuicNetFixture.*

  /** Short peer-query timings. A 1s handshake timeout makes dials to a dead port fail quickly. */
  private val Config = QuicConfig(gossip = GossipConfig.ForTests, handshakeTimeout = 1.second,
    peerQueryDelay = 300.millis, peerQueryInterval = 300.millis, knownWrongRetryAfter = 1.second)

  private val WaitMillis = 15000L

  private var fx: QuicNetFixture = scala.compiletime.uninitialized

  override def beforeEach(): Unit = fx = new QuicNetFixture(Config)

  override def afterEach(): Unit = fx.close()

  private def ts(n: Long): HLCTimestamp = HLCTimestamp(n)

  private def endpoint(hostId: HostId, port: Int, timestamp: HLCTimestamp): HostEndpointChanged =
    HostEndpointChanged(hostId, "127.0.0.1", port, 0, 0, timestamp)

  private def cacheWith(hosts: CachedHost*): RecordingNetworkCache =
    new RecordingNetworkCache(NetworkCacheContent(Nil, hosts.toList))

  private def cachedHost(hostId: HostId, port: Int, timestamp: HLCTimestamp): CachedHost =
    CachedHost(hostId, "host_c", "127.0.0.1", port, 0, 0, timestamp)

  private def send(from: Node, to: HostId): Unit =
    from.net.clientMessenger.sendHostMessage(CheckStorageDevice(to, from.net.clientId, StorageDeviceId(UUID.randomUUID())))

  /** Sends from `from` to `to` every 100ms until one of the messages reaches `node` */
  private def sendUntilDelivered(from: Node, to: HostId, node: Node): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WaitMillis)
    var delivered = false
    while !delivered do
      if System.nanoTime() - deadline > 0 then
        throw new AssertionError(s"no message reached host $to within ${WaitMillis}ms")
      send(from, to)
      delivered = node.handler.hostMessages.poll(100, TimeUnit.MILLISECONDS) != null

  /** A third host C, in the directory at its real port, once A and B both see it as a member */
  private def startHostC(): (HostId, Int, Node) =
    val hostIdC = HostId(UUID.randomUUID())
    val port = freeUdpPort()
    fx.directory.put(HostState(hostIdC, "host_c", "127.0.0.1", port, 0, 0, Set()))
    val c = fx.host(hostIdC, port)
    eventually("A and B see C as a member", WaitMillis):
      fx.a.net.membership.exists(_.isOnline(hostIdC)) && fx.b.net.membership.exists(_.isOnline(hostIdC))
    (hostIdC, port, c)

  /** B resolves C (from the directory), then holds C's real endpoint at `timestamp` */
  private def giveBFreshEntry(hostIdC: HostId, portC: Int, c: Node, timestamp: HLCTimestamp): Unit =
    sendUntilDelivered(fx.b, hostIdC, c)
    fx.b.net.dispatchForTests(HostEndpointAnswer(hostIdC, Some(endpoint(hostIdC, portC, timestamp))))
    fx.b.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(timestamp)

  /** A new host D whose only knowledge of other non-bootstrap hosts comes from `cache` */
  private def startAsker(cache: NetworkCache, cfg: QuicConfig = Config): (HostId, Node) =
    val hostIdD = HostId(UUID.randomUUID())
    val port = freeUdpPort()
    fx.directory.put(HostState(hostIdD, "host_d", "127.0.0.1", port, 0, 0, Set()))
    (hostIdD, fx.host(hostIdD, port, cfg, networkCache = cache))

  /** Writes `answer` and then a marker host message for `to` on one new stream of `ch`, and waits for `to` to receive
    * the marker: frames on one stream are dispatched in order, so the answer has been handled by then
    */
  private def sendAnswerThenMarker(ch: QuicChannel, answer: HostEndpointAnswer, to: HostId, node: Node): Unit =
    val stream = QuicTestPeer.openStream(ch)
    QuicTestPeer.writeFrame(stream, answer)
    QuicTestPeer.writeFrame(stream, CheckStorageDevice(to, ClientId(UUID.randomUUID()), StorageDeviceId(UUID.randomUUID())))
    await(node.handler.hostMessages, 1)

  // ---------------------------------------------------------------------------------------------------------------

  test("a stale address is repaired from a peer"):
    val (hostIdC, portC, c) = startHostC()
    giveBFreshEntry(hostIdC, portC, c, ts(2))
    val cache = cacheWith(cachedHost(hostIdC, freeUdpPort(), ts(1)))
    val (_, d) = startAsker(cache)
    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(1))
    eventually("D sees C online", WaitMillis)(d.net.membership.exists(_.isOnline(hostIdC)))

    sendUntilDelivered(d, hostIdC, c)

    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(2))
    d.net.peerQueriesSentForTests should be > 0L
    d.net.failingSinceForTests(hostIdC) shouldBe None
    cache.synchronized(cache.hostPuts.toList).map(h => (h.hostId, h.dataPort, h.timestamp)) should contain(
      (hostIdC, portC, ts(2)))

  test("an unknown or older answer is ignored"):
    val (hostIdC, portC, c) = startHostC()
    giveBFreshEntry(hostIdC, portC, c, ts(3))
    val (_, d) = startAsker(cacheWith(cachedHost(hostIdC, freeUdpPort(), ts(5))))
    eventually("D sees C online", WaitMillis)(d.net.membership.exists(_.isOnline(hostIdC)))

    send(d, hostIdC)
    eventually("D queries its peers about C", WaitMillis)(d.net.peerQueriesSentForTests > 0)
    val first = d.net.peerQueriesSentForTests
    eventually("D queries again", WaitMillis)(d.net.peerQueriesSentForTests > first)
    val second = d.net.peerQueriesSentForTests
    eventually("D queries a third time", WaitMillis)(d.net.peerQueriesSentForTests > second)

    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(5))
    d.net.failingSinceForTests(hostIdC) shouldBe defined

  test("an answer older than the held entry is ignored, whatever order answers arrive in"):
    val hostIdC = HostId(UUID.randomUUID())
    val (_, d) = startAsker(cacheWith(cachedHost(hostIdC, freeUdpPort(), ts(5))))
    val freshPort = freeUdpPort()

    d.net.dispatchForTests(HostEndpointAnswer(hostIdC, Some(endpoint(hostIdC, freshPort, ts(7)))))
    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(7))

    // A slower peer's stale answer arriving after the fresher one
    d.net.dispatchForTests(HostEndpointAnswer(hostIdC, Some(endpoint(hostIdC, freeUdpPort(), ts(6)))))
    d.net.dispatchForTests(HostEndpointAnswer(hostIdC, None))

    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(7))
    d.net.metadataManager.peekHostEntry(hostIdC).map(_.dataPort) shouldBe Some(freshPort)

  test("a host reported offline is not queried"):
    val (hostIdC, _, c) = startHostC()
    val (_, d) = startAsker(cacheWith(cachedHost(hostIdC, freeUdpPort(), ts(1))))
    eventually("D sees C online", WaitMillis)(d.net.membership.exists(_.isOnline(hostIdC)))
    c.net.crashForTests()
    eventually("D sees C offline", WaitMillis)(!d.net.membership.exists(_.isOnline(hostIdC)))
    // While C was online, D's membership traffic to C may have gone through the stale entry and been queried
    // about. Let a scan already running finish, then count from here.
    Thread.sleep(400)
    val baseline = d.net.peerQueriesSentForTests

    send(d, hostIdC)
    eventually("D's dial to C fails", WaitMillis)(d.net.failingSinceForTests(hostIdC).isDefined)
    Thread.sleep(1500) // the delay plus several intervals: there is no event to wait for
    d.net.peerQueriesSentForTests shouldBe baseline

  test("the failure clock restarts when the entry is replaced"):
    // C is never started, so it is never reported online and no query interferes
    val hostIdC = HostId(UUID.randomUUID())
    val (_, d) = startAsker(cacheWith(cachedHost(hostIdC, freeUdpPort(), ts(1))))

    send(d, hostIdC)
    eventually("D's dial to C fails", WaitMillis)(d.net.failingSinceForTests(hostIdC).isDefined)

    // A dial still handshaking when the replacement lands: its close must not restart the old clock
    send(d, hostIdC)
    val old = d.net.metadataManager.peekHostEntry(hostIdC).get
    eventually("D is dialing C again", WaitMillis)(old.connection.get() != null)
    val replacedAt = System.nanoTime()
    d.net.dispatchForTests(HostEndpointAnswer(hostIdC, Some(endpoint(hostIdC, freeUdpPort(), ts(2)))))
    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(2))
    d.net.failingSinceForTests(hostIdC) shouldBe None
    eventually("the old dial is closed", WaitMillis)(old.connection.get() == null)
    d.net.failingSinceForTests(hostIdC) shouldBe None

    send(d, hostIdC)
    eventually("D's dial to C's new address fails", WaitMillis)(d.net.failingSinceForTests(hostIdC).isDefined)
    d.net.failingSinceForTests(hostIdC).get should be > replacedAt

  test("a client queries its relays and tops up from other online hosts"):
    val (hostIdC, portC, c) = startHostC()
    giveBFreshEntry(hostIdC, portC, c, ts(2))
    // Two relays among three resolved hosts (A, B, and C from the cache): C rotates in and out of the relays
    val cl = fx.client(Config, networkCache = cacheWith(cachedHost(hostIdC, freeUdpPort(), ts(1))))
    eventually("the relays report C online", WaitMillis):
      cl.net.metadataManager.peekHostEntry(hostIdC).exists(_.isOnline)

    eventually("a plan with C among the relays", WaitMillis):
      val (relays, peers) = cl.net.peerQueryPlanForTests(hostIdC)
      peers.toSet shouldBe Set(fx.hostIdA, fx.hostIdB)
      // Relays first, then the top-up
      peers.take((relays - hostIdC).size).toSet shouldBe relays - hostIdC
      relays.contains(hostIdC)

    sendUntilDelivered(cl, hostIdC, c)
    cl.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(2))

  test("queries go out as datagrams"):
    val alloc = PooledByteBufAllocator.DEFAULT
    val target = HostId(UUID.randomUUID())
    val query = QuicMessageCodec.encode(HostEndpointQuery(target), alloc, toHost = Some(HostId(UUID.randomUUID())))
    try query.isDatagram shouldBe true
    finally query.release()

    // The largest answer: a maximal DNS name
    val answer = QuicMessageCodec.encode(HostEndpointAnswer(target, Some(HostEndpointChanged(target, "a" * 253,
      65535, 65535, 65535, HLCTimestamp.now))), alloc, toHost = Some(HostId(UUID.randomUUID())))
    try answer.isDatagram shouldBe true
    finally answer.release()

  test("an unsolicited answer, or one arriving over an inbound connection, is ignored"):
    val hostIdC = HostId(UUID.randomUUID())
    // E is a raw QUIC server with E's host certificate: D dials it through its cached entry, an outbound connection
    val hostIdE = HostId(UUID.randomUUID())
    val e = QuicTestPeer.server(QuicTls.serverContext(credentials(hostIdE.uuid)))
    val inbound = QuicTestPeer.client(QuicTls.clientContext(credentials()))
    try
      val cache = cacheWith(cachedHost(hostIdC, freeUdpPort(), ts(1)),
        CachedHost(hostIdE, "host_e", "127.0.0.1", e.localAddress.getPort, 0, 0, ts(1)))
      // A long interval: the query below stays outstanding (2 intervals) for the rest of the test
      val (hostIdD, d) = startAsker(cache, Config.copy(peerQueryInterval = 10.seconds))
      val dPort = fx.directory.lookup(hostIdD).get.dataPort
      def answer(n: Long) = HostEndpointAnswer(hostIdC, Some(endpoint(hostIdC, freeUdpPort(), ts(n))))

      send(d, hostIdE)
      eventually("D connects to E", WaitMillis)(!e.connections.isEmpty)
      val outbound = e.connections.peek()
      val inboundCh = inbound.connect(new InetSocketAddress("127.0.0.1", dPort)).fold(throw _, identity)

      // No query outstanding: ignored on either kind of connection
      sendAnswerThenMarker(outbound, answer(9), hostIdD, d)
      sendAnswerThenMarker(inboundCh, answer(10), hostIdD, d)
      d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(1))

      // A query outstanding: still ignored over an inbound connection
      d.net.requestPeerQuery(hostIdC)
      eventually("D queries about C", WaitMillis)(d.net.peerQueriesSentForTests > 0)
      sendAnswerThenMarker(inboundCh, answer(11), hostIdD, d)
      d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(1))
      cache.synchronized(cache.hostPuts.toList).map(_.hostId) should not contain hostIdC

      // ... and accepted over our own outbound connection
      sendAnswerThenMarker(outbound, answer(12), hostIdD, d)
      d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(12))
    finally
      inbound.close()
      e.close()

  test("a host queries peers it holds entries for before unresolved ones"):
    // C is online but unresolvable at D, so asking it would start a host object read
    val (hostIdC, _, _) = startHostC()
    fx.directory.remove(hostIdC)
    val (_, d) = startAsker(NetworkCache.NoCache, Config.copy(peerQueryFanout = 2))
    eventually("D sees C online", WaitMillis)(d.net.membership.exists(_.isOnline(hostIdC)))
    d.net.metadataManager.peekHostEntry(hostIdC) shouldBe None

    val target = HostId(UUID.randomUUID())
    (1 to 20).foreach: _ =>
      d.net.peerQueryPlanForTests(target)._2.toSet shouldBe Set(fx.hostIdA, fx.hostIdB)
