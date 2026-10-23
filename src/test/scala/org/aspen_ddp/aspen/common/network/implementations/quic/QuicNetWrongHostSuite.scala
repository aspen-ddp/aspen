package org.aspen_ddp.aspen.common.network.implementations.quic

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.GossipConfig
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

class QuicNetWrongHostSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach:
  import QuicNetFixture.*

  /** Short peer-query timings, as in `QuicNetPeerQuerySuite` */
  private val Config = QuicConfig(gossip = GossipConfig.ForTests, handshakeTimeout = 1.second,
    peerQueryDelay = 300.millis, peerQueryInterval = 300.millis)

  private val WaitMillis = 15000L

  private var fx: QuicNetFixture = scala.compiletime.uninitialized

  override def beforeEach(): Unit = fx = new QuicNetFixture(Config)

  override def afterEach(): Unit = fx.close()

  private def ts(n: Long): HLCTimestamp = HLCTimestamp(n)

  private def checkDevice(to: HostId, from: ClientId): CheckStorageDevice =
    CheckStorageDevice(to, from, StorageDeviceId(UUID.randomUUID()))

  private def send(from: Node, to: HostId): Unit =
    from.net.clientMessenger.sendHostMessage(checkDevice(to, from.net.clientId))

  /** Sends from `from` to `to` every 100ms until one of the messages reaches `node` */
  private def sendUntilDelivered(from: Node, to: HostId, node: Node): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WaitMillis)
    var delivered = false
    while !delivered do
      if System.nanoTime() - deadline > 0 then
        throw new AssertionError(s"no message reached host $to within ${WaitMillis}ms")
      send(from, to)
      delivered = node.handler.hostMessages.poll(100, TimeUnit.MILLISECONDS) != null

  /** Host messages `node`'s handler received for `hostId` */
  private def receivedFor(node: Node, hostId: HostId): Int =
    node.handler.hostMessages.asScala.count(_.message.toHost == hostId)

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
    fx.b.net.dispatchForTests(HostEndpointAnswer(hostIdC,
      Some(HostEndpointChanged(hostIdC, "127.0.0.1", portC, 0, 0, timestamp))))
    fx.b.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(timestamp)

  /** A new host D whose cached entry for `hostIdC` points at B's port, at ts 1. Its failure-clock query never comes
    * due within a test, so only wrong-host detection can start a query.
    */
  private def startAskerWithEntryAtB(hostIdC: HostId, verifyPeerIdentity: Boolean): Node =
    val cfg = Config.copy(verifyPeerIdentity = verifyPeerIdentity, peerQueryDelay = 1.minute)
    val cache = new RecordingNetworkCache(NetworkCacheContent(Nil,
      List(CachedHost(hostIdC, "host_c", "127.0.0.1", fx.portB, 0, 0, ts(1)))))
    val hostIdD = HostId(UUID.randomUUID())
    val port = freeUdpPort()
    fx.directory.put(HostState(hostIdD, "host_d", "127.0.0.1", port, 0, 0, Set()))
    val d = fx.host(hostIdD, port, cfg, networkCache = cache)
    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(1))
    d

  /** A raw QUIC client connected to `port` */
  private def rawClientTo(port: Int): (QuicTestPeer, io.netty.handler.codec.quic.QuicChannel) =
    val peer = QuicTestPeer.client(QuicTls.clientContext(credentials()))
    peer.connect(new InetSocketAddress("127.0.0.1", port)) match
      case Right(ch) => (peer, ch)
      case Left(e) =>
        peer.close()
        throw e

  // ---------------------------------------------------------------------------------------------------------------

  test("a cached address answering as another host triggers WrongHost and repair"):
    val (hostIdC, portC, c) = startHostC()
    giveBFreshEntry(hostIdC, portC, c, ts(2))
    val d = startAskerWithEntryAtB(hostIdC, verifyPeerIdentity = false)

    send(d, hostIdC)

    eventually("B replies WrongHost", WaitMillis)(fx.b.net.wrongHostRepliesSentForTests > 0)
    eventually("D's entry for C is repaired", WaitMillis):
      d.net.metadataManager.hostTimestamp(hostIdC).contains(ts(2))
    d.net.metadataManager.peekHostEntry(hostIdC).map(_.dataPort) shouldBe Some(portC)
    d.net.metadataManager.peekHostEntry(hostIdC).map(_.knownWrong) shouldBe Some(false)

    sendUntilDelivered(d, hostIdC, c)
    receivedFor(fx.b, hostIdC) shouldBe 0

  test("TLS identity mismatch is detected at the handshake"):
    val (hostIdC, portC, c) = startHostC()
    giveBFreshEntry(hostIdC, portC, c, ts(2))
    val d = startAskerWithEntryAtB(hostIdC, verifyPeerIdentity = true)

    send(d, hostIdC)

    eventually("D's entry for C is repaired", WaitMillis):
      d.net.metadataManager.hostTimestamp(hostIdC).contains(ts(2))
    sendUntilDelivered(d, hostIdC, c)

    fx.b.net.wrongHostRepliesSentForTests shouldBe 0
    receivedFor(fx.b, hostIdC) shouldBe 0

  test("a known-wrong entry sends nothing until repaired"):
    // C never exists, so no peer knows better
    val hostIdC = HostId(UUID.randomUUID())
    val d = startAskerWithEntryAtB(hostIdC, verifyPeerIdentity = false)

    send(d, hostIdC)
    eventually("D marks C's entry known-wrong", WaitMillis):
      d.net.metadataManager.peekHostEntry(hostIdC).exists(_.knownWrong)
    val entry = d.net.metadataManager.peekHostEntry(hostIdC).get
    eventually("the connection to B's port is closed", WaitMillis):
      val conn = entry.connection.get()
      conn == null || conn.isClosed

    // Known-wrong hosts are queried every interval, whatever their failure clock or online state. The immediate query
    // goes to A and B, so more than two queries means the scan repeated it.
    eventually("D keeps querying about C", WaitMillis)(d.net.peerQueriesSentForTests >= 6)

    (1 to 20).foreach: _ =>
      send(d, hostIdC)
      Thread.sleep(25)

    fx.b.net.wrongHostRepliesSentForTests should be <= 1L
    receivedFor(fx.b, hostIdC) shouldBe 0
    entry.knownWrong shouldBe true
    val conn = entry.connection.get()
    (conn == null || conn.isClosed) shouldBe true
    d.net.metadataManager.hostTimestamp(hostIdC) shouldBe Some(ts(1))

  test("WrongHost replies are rate-limited per connection"):
    val hostIdE = HostId(UUID.randomUUID())
    val portE = freeUdpPort()
    val e = fx.host(hostIdE, portE, Config.copy(wrongHostReplyInterval = 10.seconds))
    val (peer, ch) = rawClientTo(portE)
    try
      // One stream, so the frames are dispatched in order and the last one marks the end of the burst
      val stream = QuicTestPeer.openStream(ch)
      (1 to 50).foreach: _ =>
        QuicTestPeer.writeFrame(stream, checkDevice(hostIdE, ClientId(UUID.randomUUID())),
          toHost = Some(HostId(UUID.randomUUID())))
      QuicTestPeer.writeFrame(stream, checkDevice(hostIdE, ClientId(UUID.randomUUID())), toHost = Some(hostIdE))

      await(e.handler.hostMessages, 1)
      e.handler.hostMessages.size shouldBe 0
      e.net.wrongHostRepliesSentForTests shouldBe 1
      val reply = peer.awaitReceived().message
      reply shouldBe a[WrongHost]
      reply.asInstanceOf[WrongHost].actualHostId shouldBe hostIdE
    finally
      peer.close()

  test("messages without toHost, and client-mode receivers, are never rejected"):
    val (peer, ch) = rawClientTo(fx.portB)
    try
      QuicTestPeer.sendStream(ch, checkDevice(fx.hostIdB, ClientId(UUID.randomUUID())))
      await(fx.b.handler.hostMessages, 1)
    finally
      peer.close()
    fx.b.net.wrongHostRepliesSentForTests shouldBe 0

    // The check is host-mode only
    val cl = fx.client(Config)
    val other = HostId(UUID.randomUUID())
    cl.net.dispatchForTests(checkDevice(other, cl.net.clientId), toHost = Some(other))
    await(cl.handler.hostMessages, 1)

    // A host does reject one addressed elsewhere
    fx.a.net.dispatchForTests(checkDevice(other, cl.net.clientId), toHost = Some(other))
    fx.a.handler.hostMessages.size shouldBe 0
