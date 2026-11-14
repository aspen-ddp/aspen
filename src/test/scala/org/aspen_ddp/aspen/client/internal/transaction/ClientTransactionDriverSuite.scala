package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.{ReadDriverClient, TransactionStatusCache}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, HostMessage, TransactionResolved, TxMessage, TxPrepare}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{RefcountUpdate, TransactionDescription, TransactionHorizon, TransactionId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.ExecutionContext

class ClientTransactionDriverSuite extends AnyFunSuite with Matchers:

  private val horizon = 500.millis
  private val retransmit = 20.millis
  private val giveUp = TransactionHorizon.clientGiveUp(horizon)

  private val poolId = PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  private val pointer = DataObjectPointer(ObjectId(new UUID(0, 1)), poolId, Array[Byte]())
  private val ida = Replication(3, 2)
  private val client0 = ClientId(new UUID(0, 1))

  private class CountingMessenger extends ClientMessenger:
    val prepares = new AtomicInteger(0)
    def sendClientRequest(msg: ClientRequest): Unit = ()
    def sendTransactionMessage(msg: TxMessage): Unit =
      msg match
        case _: TxPrepare => prepares.incrementAndGet()
        case _ =>
    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)
    def sendHostMessage(msg: HostMessage): Unit = ()
    def dropCacheForStore(storeId: StoreId): Unit = ()

  private class TestClient(msgr: CountingMessenger, val tasks: BackgroundTaskManager) extends ReadDriverClient:
    val clientId: ClientId = ClientId(new UUID(0, 1))
    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache
    def clientContext: ExecutionContext = ExecutionContext.global
    def backgroundTaskManager: BackgroundTaskManager = tasks
    private[client] val messenger: ClientMessenger = msgr
    private[aspen] def getSystemAttribute(key: String): Option[String] = None

  /** A transaction that started at `startMillis` */
  private def mktxd(startMillis: Long): TransactionDescription =
    val requirements = RefcountUpdate(pointer, ObjectRefcount(0, 1), ObjectRefcount(1, 2)) :: Nil
    val start = HLCTimestamp(startMillis << 16)
    TransactionDescription(TransactionId(UUID.randomUUID()), start, pointer, 0, requirements, Nil, None, Nil, Nil, ida,
      Map(poolId -> ida))

  /** The driver's clock is `now`, and its periodic task never fires (an hour's delay): tests call `tick()` */
  private class Fixture:
    val tasks = new BackgroundTaskManager(ExecutionContext.global)
    val msgr = new CountingMessenger
    val client = new TestClient(msgr, tasks)
    var now = 1_000_000L
    def driver(age: FiniteDuration): SimpleClientTransactionDriver =
      val txd = mktxd(now - age.toMillis)
      SimpleClientTransactionDriver.factory(1.hour, horizon, () => now)(client, txd, Map())
        .asInstanceOf[SimpleClientTransactionDriver]
    def done(): Unit = tasks.shutdown(Duration.Zero)

  test("a client gives up with Unknown once the transaction is past the horizon plus MaxStartLead"):
    val f = new Fixture
    try
      val d = f.driver(giveUp - 1.milli)
      d.begin()
      val sent = f.msgr.prepares.get
      sent should be > 0
      d.tick()
      f.msgr.prepares.get should be > sent
      d.result.isCompleted should be(false)
      f.now += 2
      d.tick()
      d.result.value.get.get should be(ClientOutcome.Unknown)
      val after = f.msgr.prepares.get
      d.tick()
      f.msgr.prepares.get should be(after)
    finally f.done()

  test("a client still past the horizon at begin fails fast without sending"):
    val f = new Fixture
    try
      val d = f.driver(giveUp + 1.second)
      d.begin()
      d.result.value.get.get should be(ClientOutcome.Unknown)
      d.tick()
      f.msgr.prepares.get should be(0)
    finally f.done()

  test("a resolution before the give-up completes Committed or Aborted, and a later give-up changes nothing"):
    val f = new Fixture
    try
      val d = f.driver(giveUp - 1.milli)
      d.begin()
      d.receive(TransactionResolved(client0, StoreId(poolId, 1), d.txd.transactionId, committed = true))
      d.result.value.get.get should be(ClientOutcome.Committed)
      f.now += 1000
      d.tick()
      d.result.value.get.get should be(ClientOutcome.Committed)

      val d2 = f.driver(0.millis)
      d2.begin()
      d2.receive(TransactionResolved(client0, StoreId(poolId, 1), d2.txd.transactionId, committed = false))
      d2.result.value.get.get should be(ClientOutcome.Aborted)
    finally f.done()
