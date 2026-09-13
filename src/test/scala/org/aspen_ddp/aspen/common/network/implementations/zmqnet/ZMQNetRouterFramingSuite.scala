package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.zeromq.{SocketType, ZContext, ZMQ}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*

/** Framing tests for ZMQNet.drainRouter.
 *
 *  A ROUTER socket delivers each message as two frames, [identity][payload], and the reader has
 *  to consume them in pairs. Reading an odd number of frames desynchronizes the socket for good:
 *  from then on every read pairs one message's payload with the next message's identity, so the
 *  codec is handed a 36-byte ClientId string instead of a protobuf and drops the message. See
 *  the last test in this suite for what that looked like in the log.
 */
class ZMQNetRouterFramingSuite extends AnyFunSuite with Matchers:

  private val hostId = HostId(new UUID(0, 1))

  private def payloadFrame(n: Int): Array[Byte] =
    ProtobufMessageCodec.encodeMessage(HostHeartbeat(HostId(new UUID(0, n))))

  private def identityFrame(n: Int): Array[Byte] =
    ClientId(new UUID(n, n)).toBytes

  /** Models the frame queue behind a jeromq ROUTER socket: non-blocking recv returning null when
   *  empty, and a hasReceiveMore that is scoped to the current message rather than the queue.
   */
  private class FakeRouter:
    // (frame, another frame follows in this same message)
    private var frames: List[(Array[Byte], Boolean)] = Nil
    private var scheduled: List[List[Array[Byte]]] = Nil
    private var more = false

    private def flatten(msg: List[Array[Byte]]): List[(Array[Byte], Boolean)] =
      msg.zipWithIndex.map((f, i) => (f, i < msg.length - 1))

    /** Message already sitting in the queue when the reader looks. */
    def deliver(msg: List[Array[Byte]]): Unit =
      frames = frames ::: flatten(msg)

    /** Message the ZMQ IO thread hands over during a recv() that found the queue empty -- the
     *  exact window the regression lives in: recv() returns null, and by the time the caller
     *  asks again the queue is no longer empty.
     */
    def scheduleArrivalDuringEmptyRecv(msg: List[Array[Byte]]): Unit =
      scheduled = scheduled :+ msg

    def queuedFrames: Int = frames.length

    def recv(): Array[Byte] =
      if frames.isEmpty then
        scheduled match
          case head :: tail =>
            scheduled = tail
            frames = flatten(head)
          case Nil => ()
        more = false
        null
      else
        val (frame, m) = frames.head
        frames = frames.tail
        more = m
        frame

    def hasReceiveMore: Boolean = more

  /** Drains `router` once, returning the (payload, identity) pairs dispatched. */
  private def drain(router: FakeRouter): List[(Array[Byte], Array[Byte])] =
    val dispatched = ArrayBuffer[(Array[Byte], Array[Byte])]()
    ZMQNet.drainRouter(
      () => router.recv(),
      () => router.hasReceiveMore,
      (payload, from) => dispatched += ((payload, from)))
    dispatched.toList

  test("drainRouter dispatches a well-formed identity/payload pair"):
    val router = new FakeRouter
    router.deliver(List(identityFrame(1), payloadFrame(1)))

    val dispatched = drain(router)

    dispatched should have size 1
    dispatched.head._1.toSeq shouldBe payloadFrame(1).toSeq
    dispatched.head._2.toSeq shouldBe identityFrame(1).toSeq
    router.queuedFrames shouldBe 0

  test("drainRouter dispatches every queued message in order"):
    val router = new FakeRouter
    (1 to 5).foreach(n => router.deliver(List(identityFrame(n), payloadFrame(n))))

    val dispatched = drain(router)

    dispatched should have size 5
    dispatched.map(_._2.toSeq) shouldBe (1 to 5).map(n => identityFrame(n).toSeq).toList
    dispatched.map(_._1.toSeq) shouldBe (1 to 5).map(n => payloadFrame(n).toSeq).toList
    router.queuedFrames shouldBe 0

  test("drainRouter stays frame-aligned when a message arrives mid-drain"):
    // The regression. The original loop issued a second, unconditional recv() after a first one
    // returned null, swallowing the identity frame of a message that landed in that gap and
    // orphaning its payload. Every later read was then off by one frame.
    val router = new FakeRouter
    router.scheduleArrivalDuringEmptyRecv(List(identityFrame(1), payloadFrame(1)))

    val firstDrain = drain(router)
    firstDrain shouldBe empty // nothing was available at the moment we looked

    val secondDrain = drain(router)
    secondDrain should have size 1
    secondDrain.head._1.toSeq shouldBe payloadFrame(1).toSeq
    secondDrain.head._2.toSeq shouldBe identityFrame(1).toSeq
    router.queuedFrames shouldBe 0

  test("drainRouter keeps delivering decodable payloads after a message arrives mid-drain"):
    // Same race, checked through the codec: a desynchronized reader hands decodeMessage an
    // identity frame, which cannot be decoded at all.
    val router = new FakeRouter
    router.scheduleArrivalDuringEmptyRecv(List(identityFrame(1), payloadFrame(1)))
    drain(router)
    (2 to 4).foreach(n => router.deliver(List(identityFrame(n), payloadFrame(n))))

    val dispatched = drain(router)

    dispatched should have size 4
    dispatched.foreach: (payload, _) =>
      ProtobufMessageCodec.decodeMessage(payload) shouldBe defined

  test("drainRouter drops an identity-only message without losing alignment"):
    val router = new FakeRouter
    router.deliver(List(identityFrame(1)))
    router.deliver(List(identityFrame(2), payloadFrame(2)))

    val dispatched = drain(router)

    dispatched should have size 1
    dispatched.head._2.toSeq shouldBe identityFrame(2).toSeq
    router.queuedFrames shouldBe 0

  test("drainRouter drops an over-long message without losing alignment"):
    val router = new FakeRouter
    router.deliver(List(identityFrame(1), payloadFrame(1), payloadFrame(9)))
    router.deliver(List(identityFrame(2), payloadFrame(2)))

    val dispatched = drain(router)

    dispatched should have size 1
    dispatched.head._2.toSeq shouldBe identityFrame(2).toSeq
    dispatched.head._1.toSeq shouldBe payloadFrame(2).toSeq
    router.queuedFrames shouldBe 0

  test("drainRouter does not consume anything from an empty socket"):
    val router = new FakeRouter

    drain(router) shouldBe empty

    router.deliver(List(identityFrame(1), payloadFrame(1)))
    drain(router) should have size 1

  test("drainRouter reads a real DEALER's traffic off a real ROUTER socket"):
    // The tests above run against a hand-written model of a ROUTER's frame queue. This one
    // checks that model against jeromq itself: that a DEALER's message arrives as exactly two
    // frames, that the first is the identity the DEALER set, and that hasReceiveMore is scoped
    // to the message rather than the queue.
    val messageCount = 200
    val clientId = ClientId(UUID.randomUUID())
    val context = new ZContext()

    try
      val router = context.createSocket(SocketType.ROUTER)
      router.bind("inproc://drain-router-test")

      val dealer = context.createSocket(SocketType.DEALER)
      dealer.setIdentity(clientId.toBytes)
      dealer.connect("inproc://drain-router-test")

      (1 to messageCount).foreach(n => dealer.send(payloadFrame(n)) shouldBe true)

      val dispatched = ArrayBuffer[(Array[Byte], Array[Byte])]()
      val deadline = System.nanoTime() + 30.seconds.toNanos

      // Drained repeatedly, which is what the IO thread does: each call must leave the socket on
      // a message boundary or the next one reads garbage.
      while dispatched.size < messageCount && System.nanoTime() - deadline < 0 do
        ZMQNet.drainRouter(
          () => router.recv(ZMQ.DONTWAIT),
          () => router.hasReceiveMore,
          (payload, from) => dispatched += ((payload, from)))

      dispatched should have size messageCount
      dispatched.map(_._2.toSeq).distinct shouldBe List(clientId.toBytes.toSeq)
      dispatched.map(_._1.toSeq).toList shouldBe (1 to messageCount).map(n => payloadFrame(n).toSeq).toList
      dispatched.foreach: (payload, _) =>
        ProtobufMessageCodec.decodeMessage(payload) shouldBe defined
    finally
      context.close()

  test("a misread ClientId identity frame produces the error seen in the field"):
    // Documents the fingerprint that identified this bug: the host logged
    //   "Message too short - expected 1681273442 bytes, got 32"
    // 1681273442 is the ASCII "d62b" that opens a ClientId's UUID string, read as a big-endian
    // length prefix, and 32 is the remainder of the 36-character identity frame.
    val identity = ClientId(UUID.fromString("d62b1e7c-0000-0000-0000-000000000000")).toBytes

    identity.length shouldBe 36
    ByteBuffer.wrap(identity).order(ByteOrder.BIG_ENDIAN).getInt() shouldBe 1681273442
    ProtobufMessageCodec.decodeMessage(identity) shouldBe None
