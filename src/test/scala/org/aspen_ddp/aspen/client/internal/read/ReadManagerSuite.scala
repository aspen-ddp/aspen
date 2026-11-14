package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{DataObjectState, ReadDriverClient}
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.{ClientRequest, HostMessage, Read, ReadResponse, TxMessage}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.objects.{ObjectRevision, ObjectPointer, ReadError, ReadType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

class ReadManagerSuite extends AnyFunSuite with Matchers:
  import BaseReadDriverSuite.*

  /** Runs every ReadManager hand-off on the calling thread, so state is settled when `receive` returns */
  private class InlineClient extends ReadDriverClient.NoOp(client):
    var cacheDrops: List[StoreId] = Nil

    override def clientContext: ExecutionContext = ExecutionContext.parasitic

    override private[client] val messenger: Messenger = new Messenger:
      def sendClientRequest(msg: ClientRequest): Unit = ()
      def sendTransactionMessage(msg: TxMessage): Unit = ()
      def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
      def sendHostMessage(msg: HostMessage): Unit = ()
      def dropCacheForStore(storeId: StoreId): Unit = cacheDrops = storeId :: cacheDrops

  private val factory: ReadDriver.Factory =
    (c: ReadDriverClient, op: ObjectPointer, i, id: UUID, comment: String, rt: ReadType) =>
      new BaseReadDriver(c, op, i, id, comment, rt) {}

  private def respond(store: StoreId, ts: HLCTimestamp) =
    ReadResponse(client, store, readUUID, ts,
      Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set())))

  test("a read is released when it completes, and a late response for it is dropped without a warning") {
    val sent = new java.util.concurrent.atomic.AtomicReference[List[UUID]](Nil)
    val recording = new InlineClient:
      override private[client] val messenger: Messenger = new Messenger:
        def sendClientRequest(msg: ClientRequest): Unit = msg match
          case r: Read => sent.updateAndGet(r.readUUID :: _)
          case _ => ()
        def sendTransactionMessage(msg: TxMessage): Unit = ()
        def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
        def sendHostMessage(msg: HostMessage): Unit = ()
        def dropCacheForStore(storeId: StoreId): Unit = ()
    val mgr = new ReadManager(recording, factory)
    val result = mgr.read(ptr, ida, "test")
    val uuid = sent.get.head
    mgr.outstandingReadCount should be (1)

    val ts = HLCTimestamp.now
    mgr.receive(respond(ds0, ts).copy(readUUID = uuid))
    result.isCompleted should be (false)
    mgr.outstandingReadCount should be (1)
    mgr.receive(respond(ds1, ts).copy(readUUID = uuid))
    result.isCompleted should be (true)
    Await.result(result, 1.second) shouldBe a [DataObjectState]

    // Threshold reached, one store still silent: the read must not be held waiting for it
    mgr.outstandingReadCount should be (0)

    // The silent store answers: accepted and dropped, with no read left to deliver it to
    mgr.receive(respond(ds2, ts).copy(readUUID = uuid))
    mgr.outstandingReadCount should be (0)
  }

  test("a read stays registered when every store has replied but the result is still undecided") {
    val sent = new java.util.concurrent.atomic.AtomicReference[List[UUID]](Nil)
    val recording = new InlineClient:
      override private[client] val messenger: Messenger = new Messenger:
        def sendClientRequest(msg: ClientRequest): Unit = msg match
          case r: Read => sent.updateAndGet(r.readUUID :: _)
          case _ => ()
        def sendTransactionMessage(msg: TxMessage): Unit = ()
        def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
        def sendHostMessage(msg: HostMessage): Unit = ()
        def dropCacheForStore(storeId: StoreId): Unit = ()
    val mgr = new ReadManager(recording, factory)
    val result = mgr.read(ptr, ida, "test")
    val uuid = sent.get.head
    val ts = HLCTimestamp.now
    def revOf(n: Long) = ObjectRevision(TransactionId(new UUID(0, n)))
    def reply(store: StoreId, r: ObjectRevision) =
      ReadResponse(client, store, uuid, ts,
        Right(ReadResponse.CurrentState(r, ref, ts, 5, Some(odata), Set())))

    // Three different revisions: no two stores agree, so the read cannot be decided yet
    mgr.receive(reply(ds0, revOf(1)))
    mgr.receive(reply(ds1, revOf(2)))
    mgr.receive(reply(ds2, revOf(3)))
    result.isCompleted should be (false)
    mgr.outstandingReadCount should be (1)

    // A later response still reaches the driver and decides the read
    mgr.receive(reply(ds0, revOf(3)))
    result.isCompleted should be (true)
    mgr.outstandingReadCount should be (0)
  }

  test("a late StoreNotFound response still drops the cached endpoint of that store") {
    val c = new InlineClient
    val mgr = new ReadManager(c, factory)
    mgr.receive(ReadResponse(client, ds2, UUID.randomUUID(), HLCTimestamp.now, Left(ReadError.StoreNotFound)))
    c.cacheDrops should be (List(ds2))
    mgr.outstandingReadCount should be (0)
  }
