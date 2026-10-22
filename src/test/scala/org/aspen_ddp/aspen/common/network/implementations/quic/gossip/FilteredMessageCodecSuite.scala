package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import io.scalecube.cluster.Member
import io.scalecube.cluster.transport.api.{Message, MessageCodec}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ClusterTransportMessage
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InvalidClassException}
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

class FilteredMessageCodecSuite extends AnyFunSuite with Matchers:

  /** Serialized with scalecube's own unfiltered codec, as a hostile peer could */
  private def jdkBytes(m: Message): Array[Byte] =
    val out = new ByteArrayOutputStream()
    MessageCodec.INSTANCE.serialize(m, out)
    out.toByteArray

  private def roundTrip(m: Message): Message =
    val out = new ByteArrayOutputStream()
    FilteredMessageCodec.serialize(m, out)
    FilteredMessageCodec.deserialize(new ByteArrayInputStream(out.toByteArray))

  test("scalecube messages with byte, string and scalecube payloads round trip"):
    val bytes = roundTrip(Message.withData(Array[Byte](1, 2, 3)).qualifier("q").correlationId("c").build())
    bytes.data[Array[Byte]]().toList shouldBe List[Byte](1, 2, 3)
    bytes.qualifier() shouldBe "q"
    bytes.correlationId() shouldBe "c"
    roundTrip(Message.fromData("s")).data[String]() shouldBe "s"
    val member = new Member("id", null, "host:0", "ns")
    roundTrip(Message.fromData(member)).data[Member]() shouldBe member

  test("a payload of a class outside the allowlist is rejected"):
    val hostile = jdkBytes(Message.fromData(new java.io.File("/etc/passwd")))
    an[InvalidClassException] should be thrownBy
      FilteredMessageCodec.deserialize(new ByteArrayInputStream(hostile))

  test("an oversized array is rejected"):
    val big = jdkBytes(Message.fromData(new Array[Byte](FilteredMessageCodec.MaxArrayLength.toInt + 1)))
    an[InvalidClassException] should be thrownBy
      FilteredMessageCodec.deserialize(new ByteArrayInputStream(big))

  test("the transport drops a non-allowlisted payload and keeps delivering valid traffic"):
    val local = HostId(UUID.randomUUID())
    val peer = HostId(UUID.randomUUID())
    val t = new QuicClusterTransport(local, UUID.randomUUID(), _ => ())
    t.start().block()
    val got = new ConcurrentLinkedQueue[Message]()
    t.listen().subscribe(m => got.add(m))
    t.receive(ClusterTransportMessage(peer, UUID.randomUUID(), local,
      jdkBytes(Message.fromData(new java.io.File("/etc/passwd")))))
    got shouldBe empty
    t.receive(ClusterTransportMessage(peer, UUID.randomUUID(), local, jdkBytes(Message.fromData("ok"))))
    got.size shouldBe 1
    got.peek().data[String]() shouldBe "ok"
