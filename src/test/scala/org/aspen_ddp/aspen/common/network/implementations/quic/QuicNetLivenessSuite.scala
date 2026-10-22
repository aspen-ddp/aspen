package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

class QuicNetLivenessSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach:
  import QuicNetFixture.*

  private val HeartbeatPeriod = Duration(200, MILLISECONDS)
  private val IdleTimeout = Duration(600, MILLISECONDS)
  private val RecoveryBudgetMillis = 2000L

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
    fx = new QuicNetFixture(QuicConfig(idleTimeout = Some(IdleTimeout)), HeartbeatPeriod)

  override def afterEach(): Unit =
    fx.close()

  // ---------------------------------------------------------------------------------------------------------------

  private def ptr: DataObjectPointer =
    DataObjectPointer(ObjectId(UUID.randomUUID()), PoolId.BootstrapPoolId, Array[Byte](1))

  private def read(to: StoreId, from: ClientId): Read = Read(to, from, UUID.randomUUID(), ptr, FullObject())

  private def response(to: ClientId, from: StoreId, readUUID: UUID): ReadResponse =
    ReadResponse(to, from, readUUID, HLCTimestamp(1000L), Left(ReadError.ObjectNotFound))

  private def txHeartbeat(to: StoreId, from: StoreId): TxHeartbeat =
    TxHeartbeat(to, from, TransactionId(UUID.randomUUID()))

  /** Stream-sized, so delivery is reliable */
  private def transferData(to: HostId): StoreTransferData =
    StoreTransferData(to, ClientId(UUID.randomUUID()), UUID.randomUUID(), DataBuffer(new Array[Byte](2000)))

  private def entryFor(net: QuicNet, hostId: HostId): QuicHostEntry =
    net.metadataManager.peekHostEntry(hostId).getOrElse(fail(s"no host entry for $hostId"))

  /** Each host dials the other by sending it a TxMessage */
  private def connectHosts(): Unit =
    fx.a.net.serverMessenger.sendTransactionMessage(txHeartbeat(fx.storesB.head, fx.storesA.head))
    fx.b.net.serverMessenger.sendTransactionMessage(txHeartbeat(fx.storesA.head, fx.storesB.head))
    await(fx.b.handler.txMessages, 1)
    await(fx.a.handler.txMessages, 1)

  // ---------------------------------------------------------------------------------------------------------------

  test("hosts mark each other online after connecting"):
    connectHosts()
    eventually("each host sees the other online", 1000):
      entryFor(fx.a.net, fx.hostIdB).isOnline && entryFor(fx.b.net, fx.hostIdA).isOnline

  test("a host is marked offline after its peer stops"):
    connectHosts()
    eventually("A sees B online"):
      entryFor(fx.a.net, fx.hostIdB).isOnline
    fx.stopB()
    eventually("A sees B offline", 1500):
      !entryFor(fx.a.net, fx.hostIdB).isOnline

  test("quiet client connection survives beyond the idle timeout"):
    val c = fx.client()
    val req = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    await(fx.a.handler.clientRequests, 1)
    val connsBefore = c.net.connectionCount
    connsBefore shouldBe 1
    // The client sends nothing for more than three idle timeouts
    Thread.sleep(2000)
    val resp = response(c.net.clientId, req.toStore, req.readUUID)
    fx.a.net.serverMessenger.sendClientResponse(resp)
    await(c.handler.clientResponses, 1).map(_.message) shouldBe List(resp)
    c.net.connectionCount shouldBe connsBefore

  test("idle host pair recovers online status after a peer restart"):
    connectHosts()
    eventually("each host sees the other online"):
      entryFor(fx.a.net, fx.hostIdB).isOnline && entryFor(fx.b.net, fx.hostIdA).isOnline
    // Restart B on the same port with no application traffic. B's down time is however long A takes to notice.
    fx.stopB()
    eventually("A sees the stopped B offline", 1500):
      !entryFor(fx.a.net, fx.hostIdB).isOnline
    val starting = System.nanoTime()
    fx.startB()
    // Timed from when the restarted B is up, so B's construction time (TLS contexts, binds) is not counted
    val restarted = System.nanoTime()
    // Recovery waits on quiche's Initial retransmission (PTO) for A's re-dial made while B was down, so its timing
    // varies (365-590ms observed). 2s still proves recovery without application traffic, far below the
    // "never recovers" failure this guards against, and stays under the 5s handshake timeout.
    eventually("A and the restarted B see each other online", RecoveryBudgetMillis):
      entryFor(fx.a.net, fx.hostIdB).isOnline && entryFor(fx.b.net, fx.hostIdA).isOnline
    info(s"B started in ${(restarted - starting) / 1000000}ms; " +
      s"recovered ${(System.nanoTime() - restarted) / 1000000}ms after it was up")

  test("dead peer's connection is cleaned up and re-dialed"):
    connectHosts()
    val stale = entryFor(fx.a.net, fx.hostIdB).connection.get()
    stale should not be null
    fx.stopB()
    eventually("A's connection to the stopped B closes", 3000):
      stale.isClosed
    fx.startB()
    val m = transferData(fx.hostIdB)
    fx.a.net.clientMessenger.sendHostMessage(m)
    await(fx.b.handler.hostMessages, 1).map(_.message) shouldBe List(m)

  test("a crashed and restarted peer is re-dialed long before the idle timeout"):
    // A long idle timeout, so only the heartbeat-connection change can make A drop its stale connection in time
    fx.close()
    fx = new QuicNetFixture(QuicConfig(idleTimeout = Some(Duration(30, SECONDS))), HeartbeatPeriod)
    connectHosts()
    eventually("A has heard B's heartbeats"):
      entryFor(fx.a.net, fx.hostIdB).isOnline
    val stale = entryFor(fx.a.net, fx.hostIdB).connection.get()
    stale should not be null
    fx.crashB()
    Thread.sleep(300)
    stale.isClosed shouldBe false // the crash sent no close frame
    fx.startB()
    val restarted = System.nanoTime()
    val deadline = restarted + TimeUnit.MILLISECONDS.toNanos(RecoveryBudgetMillis)
    var delivered = false
    while !delivered && System.nanoTime() - deadline < 0 do
      fx.a.net.clientMessenger.sendHostMessage(transferData(fx.hostIdB))
      delivered = fx.b.handler.hostMessages.poll(50, TimeUnit.MILLISECONDS) != null
    info(s"delivered to the restarted B ${(System.nanoTime() - restarted) / 1000000}ms after it was up")
    delivered shouldBe true
    stale.isClosed shouldBe true

  test("keepalive datagrams are not dispatched"):
    val c = fx.client()
    val req = read(fx.storesA.head, c.net.clientId)
    c.net.clientMessenger.sendClientRequest(req)
    await(fx.a.handler.clientRequests, 1)
    val totalBefore = fx.a.handler.total
    val undecodableBefore = fx.a.net.undecodableDatagrams
    // Quiet for several keepalive intervals (idleTimeout / 2 = 300ms)
    eventually("A receives keepalives from the quiet client", 3000):
      fx.a.net.keepalivesReceived >= 3
    fx.a.handler.total shouldBe totalBefore
    fx.a.net.undecodableDatagrams shouldBe undecodableBefore
    c.net.connectionCount shouldBe 1

  test("the offline check survives nanoTime wrapping"):
    val limit = 3000L
    QuicNet.heartbeatOverdue(5000L, 1000L, limit) shouldBe true
    QuicNet.heartbeatOverdue(3000L, 1000L, limit) shouldBe false
    // A heartbeat 1000ns before the counter wraps, checked 999ns after it: 1999ns elapsed, not overdue
    QuicNet.heartbeatOverdue(Long.MinValue + 999L, Long.MaxValue - 999L, limit) shouldBe false
    QuicNet.heartbeatOverdue(Long.MinValue + 2999L, Long.MaxValue - 999L, limit) shouldBe true

  test("no buffer leaks were reported"):
    leakWatch.leaks() shouldBe Nil
