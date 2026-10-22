package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.{GossipConfig, MembershipListener}
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.jdk.CollectionConverters.*

class QuicNetLivenessSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach:
  import QuicNetFixture.*

  private val HeartbeatPeriod = Duration(200, MILLISECONDS)
  private val IdleTimeout = Duration(600, MILLISECONDS)
  private val WaitMillis = 5000L

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
    fx = new QuicNetFixture(QuicConfig(idleTimeout = Some(IdleTimeout), gossip = GossipConfig.ForTests), HeartbeatPeriod)

  override def afterEach(): Unit =
    fx.close()

  // ---------------------------------------------------------------------------------------------------------------

  /** Records every membership event and gossip delivered to it */
  private class Recorder extends MembershipListener:
    val restarts = new ConcurrentLinkedQueue[HostId]()
    val gossip = new ConcurrentLinkedQueue[Gossip]()
    override def restarted(hostId: HostId): Unit = restarts.add(hostId)
    override def gossipReceived(g: Gossip): Unit = gossip.add(g)

  private def listen(net: QuicNet): Recorder =
    val r = new Recorder
    net.membership.getOrElse(fail("host-mode net has no membership")).addListener(r)
    r

  private def ptr: DataObjectPointer =
    DataObjectPointer(ObjectId(UUID.randomUUID()), PoolId.BootstrapPoolId, Array[Byte](1))

  private def read(to: StoreId, from: ClientId): Read = Read(to, from, UUID.randomUUID(), ptr, FullObject())

  private def response(to: ClientId, from: StoreId, readUUID: UUID): ReadResponse =
    ReadResponse(to, from, readUUID, HLCTimestamp(1000L), Left(ReadError.ObjectNotFound))

  private def txHeartbeat(to: StoreId, from: StoreId): TxHeartbeat =
    TxHeartbeat(to, from, TransactionId(UUID.randomUUID()))

  /** Stream-sized, so delivery is reliable */
  private def rebuildData(to: StoreId): OpportunisticRebuild = rebuild(to, 2000)

  private def entryFor(net: QuicNet, hostId: HostId): QuicHostEntry =
    net.metadataManager.peekHostEntry(hostId).getOrElse(fail(s"no host entry for $hostId"))

  /** True once `net` no longer holds `old` as its entry for `hostId`: dropped, or dropped and re-read */
  private def replaced(net: QuicNet, hostId: HostId, old: QuicHostEntry): Boolean =
    !net.metadataManager.peekHostEntry(hostId).exists(_ eq old)

  private def stateOf(hostId: HostId, port: Int): HostState =
    HostState(hostId, s"host_${hostId.uuid}", "127.0.0.1", port, 0, 0, Set())

  /** A third host C, known to the fixture's directory, once A and B both see it as a member */
  private def startHostC(): (HostId, Node) =
    val hostIdC = HostId(UUID.randomUUID())
    val port = freeUdpPort()
    fx.directory.put(stateOf(hostIdC, port))
    val c = fx.host(hostIdC, port)
    eventually("A and B see C as a member", WaitMillis):
      fx.a.net.membership.exists(_.isOnline(hostIdC)) && fx.b.net.membership.exists(_.isOnline(hostIdC))
    (hostIdC, c)

  /** Sends a host message from A to `hostId` and waits for it, so A has resolved (and cached) `hostId`'s entry */
  private def sendHostMessageTo(hostId: HostId, node: Node): Unit =
    val m = CheckStorageDevice(hostId, fx.a.net.clientId, StorageDeviceId(UUID.randomUUID()))
    fx.a.net.clientMessenger.sendHostMessage(m)
    await(node.handler.hostMessages, 1, WaitMillis).map(_.message) shouldBe List(m)

  private def awaitOnline(): Unit =
    eventually("each host sees the other online", WaitMillis):
      entryFor(fx.a.net, fx.hostIdB).isOnline && entryFor(fx.b.net, fx.hostIdA).isOnline

  // ---------------------------------------------------------------------------------------------------------------

  test("hosts mark each other online without application traffic"):
    awaitOnline()
    fx.a.handler.total shouldBe 0
    fx.b.handler.total shouldBe 0

  test("a crashed host is marked offline"):
    awaitOnline()
    fx.crashB()
    eventually("A sees the crashed B offline", WaitMillis):
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

  test("dead peer's connection is cleaned up and re-dialed"):
    awaitOnline()
    val stale = entryFor(fx.a.net, fx.hostIdB).connection.get()
    stale should not be null
    fx.stopB()
    eventually("A's connection to the stopped B closes", 3000):
      stale.isClosed
    fx.startB()
    val m = rebuildData(fx.storesB.head)
    fx.a.net.clientMessenger.sendClientRequest(m)
    await(fx.b.handler.clientRequests, 1).map(_.message) shouldBe List(m)

  test("fast restart re-dials and is reported as restarted"):
    // A long idle timeout, so only the boot-id re-dial can make A drop its black-holed connection in time
    fx.close()
    fx = new QuicNetFixture(QuicConfig(idleTimeout = Some(Duration(30, SECONDS)), gossip = GossipConfig.ForTests),
      HeartbeatPeriod)
    awaitOnline()
    val rec = listen(fx.a.net)
    // Membership replies travel over the connection B dialed, so being online says nothing about A's own connection
    // to B. Use it, so the restart leaves an established (and so black-holed) connection behind.
    val m = txHeartbeat(fx.storesB.head, fx.storesA.head)
    fx.a.net.serverMessenger.sendTransactionMessage(m)
    await(fx.b.handler.txMessages, 1, WaitMillis)
    val stale = entryFor(fx.a.net, fx.hostIdB).connection.get()
    stale should not be null
    stale.isActive shouldBe true
    fx.crashB()
    fx.startB()
    val restarted = System.nanoTime()
    val deadline = restarted + TimeUnit.MILLISECONDS.toNanos(WaitMillis)
    var delivered = false
    while !(delivered && rec.restarts.contains(fx.hostIdB)) && System.nanoTime() - deadline < 0 do
      if !delivered then
        fx.a.net.serverMessenger.sendTransactionMessage(txHeartbeat(fx.storesB.head, fx.storesA.head))
        delivered = fx.b.handler.txMessages.poll(50, TimeUnit.MILLISECONDS) != null
      else
        Thread.sleep(10)
    info(s"recovered ${(System.nanoTime() - restarted) / 1000000}ms after B was up")
    delivered shouldBe true
    rec.restarts.asScala.toList shouldBe List(fx.hostIdB)
    stale.isClosed shouldBe true

  test("an ordinary re-dial is not treated as a restart"):
    awaitOnline()
    val recA = listen(fx.a.net)
    val recB = listen(fx.b.net)
    val bOut = entryFor(fx.b.net, fx.hostIdA).connection.get()
    bOut should not be null
    val aOut = entryFor(fx.a.net, fx.hostIdB).connection.get()
    aOut should not be null
    aOut.loop.execute(() => aOut.close())
    eventually("A's outbound connection closes", WaitMillis)(aOut.isClosed)
    val m = txHeartbeat(fx.storesB.head, fx.storesA.head)
    fx.a.net.serverMessenger.sendTransactionMessage(m)
    await(fx.b.handler.txMessages, 1, WaitMillis).map(_.message) shouldBe List(m)
    Thread.sleep(500)
    recA.restarts shouldBe empty
    recB.restarts shouldBe empty
    entryFor(fx.b.net, fx.hostIdA).connection.get() should be theSameInstanceAs bOut

  test("StoreMoved gossip drops the store mapping on peers"):
    awaitOnline()
    val recB = listen(fx.b.net)
    val g = StoreMoved(fx.storesB.head)
    fx.a.net.serverMessenger.broadcastGossip(g)
    eventually("B receives the StoreMoved gossip", WaitMillis):
      recB.gossip.contains(g)

  test("HostEndpointChanged gossip drops the host entry on peers"):
    awaitOnline()
    val old = entryFor(fx.b.net, fx.hostIdA)
    val bOut = old.connection.get()
    bOut should not be null
    fx.a.net.serverMessenger.broadcastGossip(HostEndpointChanged(fx.hostIdA))
    // B re-reads A's host object on its next send, so the entry may already be back as a new instance
    eventually("B drops its entry for A", WaitMillis)(replaced(fx.b.net, fx.hostIdA, old))
    eventually("B's outbound connection to A closes", WaitMillis)(bOut.isClosed)

  test("a host that moved to a new port rejoins membership before anyone re-reads its host object"):
    awaitOnline()
    val (hostIdC, c1) = startHostC()
    val recA = listen(fx.a.net)
    sendHostMessageTo(hostIdC, c1)
    val oldPort = entryFor(fx.a.net, hostIdC).dataPort
    c1.net.crashForTests()
    val newPort = freeUdpPort()
    fx.directory.put(stateOf(hostIdC, newPort))
    val c2 = fx.host(hostIdC, newPort)
    // Nothing has told A that C moved, so A's entry still names C's old port. Membership can only complete if A's
    // scalecube replies go back over the connection C dialed, not to the address in that entry.
    eventually("A sees the moved C rejoin", WaitMillis)(recA.restarts.contains(hostIdC))
    entryFor(fx.a.net, hostIdC).dataPort shouldBe oldPort
    eventually("C sees A and B as members", WaitMillis):
      c2.net.membership.exists(m => m.isOnline(fx.hostIdA) && m.isOnline(fx.hostIdB))

  test("a moved host's HostEndpointChanged gossip makes peers re-read it, after which data traffic reaches it"):
    awaitOnline()
    val (hostIdC, c1) = startHostC()
    val recA = listen(fx.a.net)
    sendHostMessageTo(hostIdC, c1)
    c1.net.crashForTests()
    val newPort = freeUdpPort()
    fx.directory.put(stateOf(hostIdC, newPort))
    val c2 = fx.host(hostIdC, newPort)
    // Host gossips the change once its host-object update commits, by which time it has joined membership
    eventually("A sees the moved C rejoin", WaitMillis)(recA.restarts.contains(hostIdC))
    val stale = entryFor(fx.a.net, hostIdC)
    stale.dataPort should not be newPort
    c2.net.serverMessenger.broadcastGossip(HostEndpointChanged(hostIdC))
    eventually("A drops its stale entry for C", WaitMillis)(replaced(fx.a.net, hostIdC, stale))
    // The next data send re-reads C's host object and dials the new port
    sendHostMessageTo(hostIdC, c2)
    entryFor(fx.a.net, hostIdC).dataPort shouldBe newPort

  test("a top-level HostEndpointChanged is ignored"):
    awaitOnline()
    val old = entryFor(fx.b.net, fx.hostIdA)
    // Endpoint changes travel only as gossip; a direct one is not acted on
    noException should be thrownBy fx.b.net.dispatchForTests(HostEndpointChanged(fx.hostIdA))
    fx.b.net.metadataManager.peekHostEntry(fx.hostIdA).exists(_ eq old) shouldBe true

  test("endpoint gossip for an unknown host starts no lookup"):
    awaitOnline()
    val recB = listen(fx.b.net)
    val unknown = HostId(UUID.randomUUID())
    val g = HostEndpointChanged(unknown)
    fx.a.net.serverMessenger.broadcastGossip(g)
    // QuicNet's own listener was registered first, so it has handled the gossip once the recorder sees it
    eventually("B receives the gossip", WaitMillis):
      recB.gossip.contains(g)
    fx.b.net.metadataManager.peekHostEntry(unknown) shouldBe None
    fx.b.net.metadataManager.hasParkedMessages shouldBe false
    fx.directory.lookups.asScala.toList should not contain unknown

  test("client mode ignores cluster transport messages"):
    val c = fx.client()
    c.net.membership shouldBe None
    val m = ClusterTransportMessage(fx.hostIdA, UUID.randomUUID(), fx.hostIdB, Array[Byte](1, 2, 3))
    noException should be thrownBy c.net.dispatchForTests(m)
    c.net.lastBootIdOf(fx.hostIdA) shouldBe None

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

  test("no buffer leaks were reported"):
    leakWatch.leaks() shouldBe Nil
