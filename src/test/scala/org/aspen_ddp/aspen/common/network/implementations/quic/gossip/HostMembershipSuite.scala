package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.{ClusterTransportMessage, Codec, Gossip, StoreMoved}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.jdk.CollectionConverters.*

class HostMembershipSuite extends AnyFunSuite with Matchers:

  private val Cap = 5000L

  private class Recorder extends MembershipListener:
    val events = new ConcurrentLinkedQueue[HostEvent]()
    val gossip = new ConcurrentLinkedQueue[Gossip]()
    override def online(hostId: HostId): Unit = events.add(HostEvent.Online(hostId))
    override def offline(hostId: HostId): Unit = events.add(HostEvent.Offline(hostId))
    override def restarted(hostId: HostId): Unit = events.add(HostEvent.Restarted(hostId))
    override def gossipReceived(g: Gossip): Unit = gossip.add(g)
    def has(e: HostEvent): Boolean = events.asScala.exists(_ == e)

  /** Three hosts on an in-memory network. Every host lists all three as seeds. */
  private class Net:
    val a: HostId = HostId(UUID.randomUUID())
    val b: HostId = HostId(UUID.randomUUID())
    val c: HostId = HostId(UUID.randomUUID())
    val seeds: List[HostId] = List(a, b, c)

    val hosts = new ConcurrentHashMap[HostId, HostMembership]()
    val down: java.util.Set[HostId] = ConcurrentHashMap.newKeySet[HostId]()
    // Boot ids of "crashed" processes. Their traffic is dropped in both directions.
    val deadBoots: java.util.Set[UUID] = ConcurrentHashMap.newKeySet[UUID]()
    val recorders = new ConcurrentHashMap[HostId, Recorder]()
    private val all = new ConcurrentLinkedQueue[HostMembership]()

    private def send(m: ClusterTransportMessage): Unit =
      if !deadBoots.contains(m.fromBootId) && !down.contains(m.toHost) then
        val target = hosts.get(m.toHost)
        if target != null && !deadBoots.contains(target.bootId) then
          target.receive(m)

    def launch(h: HostId): HostMembership =
      val hm = new HostMembership(h, UUID.randomUUID(), seeds, GossipConfig.ForTests, send)
      val r = new Recorder
      hm.addListener(r)
      recorders.put(h, r)
      all.add(hm)
      hosts.put(h, hm)
      hm.start()
      hm

    def rec(h: HostId): Recorder = recorders.get(h)
    def hm(h: HostId): HostMembership = hosts.get(h)

    def shutdown(): Unit = all.asScala.foreach(_.shutdown())

  private def withNet(body: Net => Unit): Unit =
    val net = new Net
    try
      List(net.a, net.b, net.c).foreach(net.launch)
      body(net)
    finally net.shutdown()

  private def eventually(what: String)(cond: => Boolean): Unit =
    val deadline = System.currentTimeMillis() + Cap
    while !cond && System.currentTimeMillis() < deadline do
      Thread.sleep(10)
    assert(cond, s"timed out waiting for: $what")

  private def awaitAllOnline(net: Net): Unit =
    for h <- net.seeds do
      eventually(s"$h sees the other two"):
        net.hm(h).onlineHosts == net.seeds.filter(_ != h).toSet

  test("hosts join through seeds and see each other online"):
    withNet: net =>
      awaitAllOnline(net)
      for h <- net.seeds; o <- net.seeds if o != h do
        net.hm(h).isOnline(o) shouldBe true
        eventually(s"$h got online($o)")(net.rec(h).has(HostEvent.Online(o)))

  test("local host in seed list is ignored"):
    withNet: net =>
      awaitAllOnline(net)
      Thread.sleep(300)
      for h <- net.seeds do
        net.hm(h).isOnline(h) shouldBe false
        val named = net.rec(h).events.asScala.collect:
          case HostEvent.Online(x) => x
          case HostEvent.Offline(x) => x
          case HostEvent.Restarted(x) => x
        named should not contain h

  test("a crashed host is reported offline"):
    withNet: net =>
      awaitAllOnline(net)
      net.down.add(net.b)
      net.deadBoots.add(net.hm(net.b).bootId)
      for h <- List(net.a, net.c) do
        eventually(s"$h got offline(B)")(net.rec(h).has(HostEvent.Offline(net.b)))
        net.hm(h).isOnline(net.b) shouldBe false

  test("a fast restart is reported as restarted"):
    withNet: net =>
      awaitAllOnline(net)
      // Crash B without a graceful leave, then bring a new B up under the same HostId at once.
      net.deadBoots.add(net.hm(net.b).bootId)
      net.launch(net.b)
      for h <- List(net.a, net.c) do
        eventually(s"$h got restarted(B)")(net.rec(h).has(HostEvent.Restarted(net.b)))
        eventually(s"$h lists B online")(net.hm(h).isOnline(net.b))
        net.rec(h).events.asScala.count(_ == HostEvent.Restarted(net.b)) shouldBe 1

  test("broadcast gossip reaches every other host"):
    withNet: net =>
      awaitAllOnline(net)
      val g = StoreMoved(StoreId(PoolId(UUID.randomUUID()), 2.toByte), HostId(UUID.randomUUID()), HLCTimestamp(1))
      net.hm(net.a).broadcast(g)
      for h <- List(net.b, net.c) do
        eventually(s"$h received the gossip")(net.rec(h).gossip.contains(g))
      net.rec(net.a).gossip.contains(g) shouldBe false

  test("undecodable gossip is dropped"):
    withNet: net =>
      awaitAllOnline(net)
      val raw = Array[Byte](0xff.toByte, 0x01, 0x02)
      Codec.decodeGossip(raw).isLeft shouldBe true
      net.hm(net.a).spreadRawGossip(raw)
      val g = StoreMoved(StoreId(PoolId(UUID.randomUUID()), 1.toByte), HostId(UUID.randomUUID()), HLCTimestamp(1))
      net.hm(net.a).broadcast(g)
      for h <- List(net.b, net.c) do
        eventually(s"$h received the later gossip")(net.rec(h).gossip.contains(g))
        net.rec(h).gossip.asScala.toList shouldBe List(g)

  test("shutdown stops every scalecube scheduler thread"):
    val net = new Net
    try
      List(net.a, net.c).foreach(net.launch)
      for h <- List(net.a, net.c) do
        eventually(s"$h is joined")(net.hm(h).onlineHosts.size == 1)
    finally net.shutdown()
    val ours = List(net.a, net.c).map(_.uuid.toString)
    eventually("scheduler threads exit"):
      Thread.getAllStackTraces.keySet.asScala.forall(t => !ours.exists(t.getName.contains))
