package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import io.scalecube.cluster.transport.api.{Message, Transport}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ClusterTransportMessage
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import reactor.core.Exceptions

import java.time.Duration
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, TimeoutException}

class QuicClusterTransportSuite extends AnyFunSuite with Matchers:

  /** Two transports wired together in memory; each one's sendFn calls the other's receive. */
  private class Pair:
    val a: HostId = HostId(UUID.randomUUID())
    val b: HostId = HostId(UUID.randomUUID())
    val bootA: UUID = UUID.randomUUID()
    val bootB: UUID = UUID.randomUUID()
    val sentByA = new ConcurrentLinkedQueue[ClusterTransportMessage]()
    val sentByB = new ConcurrentLinkedQueue[ClusterTransportMessage]()
    val ta: QuicClusterTransport = new QuicClusterTransport(a, bootA, m => { sentByA.add(m); tb.receive(m) })
    lazy val tb: QuicClusterTransport = new QuicClusterTransport(b, bootB, m => { sentByB.add(m); ta.receive(m) })
    ta.start().block()
    tb.start().block()
    val addrA: String = QuicClusterTransport.addressOf(a)
    val addrB: String = QuicClusterTransport.addressOf(b)

  private def collect(t: Transport): ConcurrentLinkedQueue[Message] =
    val q = new ConcurrentLinkedQueue[Message]()
    t.listen().subscribe(m => q.add(m))
    q

  test("addressOf and hostIdOf invert, and the address matches scalecube's format"):
    val h = HostId(UUID.randomUUID())
    QuicClusterTransport.hostIdOf(QuicClusterTransport.addressOf(h)) shouldBe Some(h)
    Transport.parsePort(QuicClusterTransport.addressOf(h)) shouldBe 0
    QuicClusterTransport.hostIdOf("not-a-uuid:0") shouldBe None

  test("send delivers to the peer's listen()"):
    val p = new Pair
    val got = collect(p.tb)
    p.ta.send(p.addrB, Message.withData("x").qualifier("q").build()).block()
    got.size shouldBe 1
    val m = got.peek()
    m.qualifier() shouldBe "q"
    m.data[String]() shouldBe "x"
    val c = p.sentByA.peek()
    c.fromHost shouldBe p.a
    c.fromBootId shouldBe p.bootA
    c.toHost shouldBe p.b

  test("requestResponse completes with the correlated reply"):
    val p = new Pair
    p.tb.listen().subscribe: m =>
      if m.qualifier() == "req" then
        p.tb.send(p.addrA, Message.withQualifier("resp").correlationId(m.correlationId()).build()).subscribe()
    val r = p.ta.requestResponse(p.addrB, Message.withQualifier("req").correlationId("c1").build())
      .block(Duration.ofSeconds(1))
    r.qualifier() shouldBe "resp"

  test("requestResponse ignores replies with other correlation ids"):
    val p = new Pair
    p.tb.listen().subscribe: m =>
      if m.qualifier() == "req" then
        p.tb.send(p.addrA, Message.withQualifier("resp").correlationId("other").build()).subscribe()
    // block() wraps checked exceptions, so unwrap to see the TimeoutException
    val err = intercept[RuntimeException]:
      p.ta.requestResponse(p.addrB, Message.withQualifier("req").correlationId("c1").build())
        .timeout(Duration.ofMillis(200)).block()
    Exceptions.unwrap(err) shouldBe a[TimeoutException]

  test("send to own address loops back without calling sendFn"):
    val p = new Pair
    val got = collect(p.ta)
    p.ta.send(p.addrA, Message.withData("me").qualifier("q").build()).block()
    got.size shouldBe 1
    p.sentByA.size shouldBe 0

  test("garbage payload is dropped and transport keeps working"):
    val p = new Pair
    val got = collect(p.tb)
    p.tb.receive(ClusterTransportMessage(p.a, p.bootA, p.b, Array[Byte](1, 2, 3)))
    got.size shouldBe 0
    p.ta.send(p.addrB, Message.withData("ok").qualifier("q").build()).block()
    got.size shouldBe 1

  test("send after stop errors, receive after stop is ignored"):
    val p = new Pair
    val got = collect(p.tb)
    p.tb.stop().block()
    p.tb.isStopped shouldBe true
    p.ta.send(p.addrB, Message.withData("x").qualifier("q").build()).block()
    got.size shouldBe 0
    intercept[IllegalStateException]:
      p.tb.send(p.addrA, Message.withData("x").qualifier("q").build()).block()

  test("unparseable address and sendFn failure yield errors"):
    val p = new Pair
    intercept[IllegalArgumentException]:
      p.ta.send("bogus", Message.withData("x").build()).block()
    val t = new QuicClusterTransport(p.a, p.bootA, _ => throw new RuntimeException("boom"))
    t.start().block()
    intercept[RuntimeException]:
      t.send(p.addrB, Message.withData("x").build()).block()

  test("send is lazy: nothing happens until subscribed"):
    val p = new Pair
    val mono = p.ta.send(p.addrB, Message.withData("x").qualifier("q").build())
    p.sentByA.size shouldBe 0
    mono.block()
    p.sentByA.size shouldBe 1

  test("requestResponse without a correlationId fails fast"):
    val p = new Pair
    intercept[IllegalArgumentException]:
      p.ta.requestResponse(p.addrB, Message.withQualifier("req").build()).block()

  test("concurrent receive from many threads loses nothing"):
    val p = new Pair
    val count = new java.util.concurrent.atomic.AtomicInteger(0)
    p.tb.listen().subscribe(_ => count.incrementAndGet())
    val threads, per = (8, 1000)
    val payload =
      val out = new java.io.ByteArrayOutputStream()
      io.scalecube.cluster.transport.api.MessageCodec.INSTANCE.serialize(Message.withData("x").qualifier("q").build(), out)
      out.toByteArray
    val start = new java.util.concurrent.CountDownLatch(1)
    val ts = (0 until threads._1).map: _ =>
      val t = new Thread(() =>
        start.await()
        for _ <- 0 until per._2 do p.tb.receive(ClusterTransportMessage(p.a, p.bootA, p.b, payload)))
      t.start()
      t
    start.countDown()
    ts.foreach(_.join())
    count.get shouldBe threads._1 * per._2

  test("loopback send from inside a listen() callback does not deadlock"):
    val p = new Pair
    val got = collect(p.ta)
    p.ta.listen().subscribe: m =>
      if m.qualifier() == "first" then
        p.ta.send(p.addrA, Message.withQualifier("second").build()).subscribe()
    p.ta.send(p.addrA, Message.withQualifier("first").build()).block()
    got.size should be >= 1
