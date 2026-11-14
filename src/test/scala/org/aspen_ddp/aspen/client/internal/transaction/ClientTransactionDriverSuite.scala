package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.{ObjectCache, ReadDriverClient, TransactionStatusCache}
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
import scala.concurrent.{Await, ExecutionContext}

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
    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None
    private[client] val messenger: ClientMessenger = msgr
    private[client] val objectCache: ObjectCache = ObjectCache.NoCache
    private[aspen] def getSystemAttribute(key: String): Option[String] = None

  /** A transaction that started `age` ago */
  private def mktxd(age: FiniteDuration): TransactionDescription =
    val requirements = RefcountUpdate(pointer, ObjectRefcount(0, 1), ObjectRefcount(1, 2)) :: Nil
    val start = HLCTimestamp((System.currentTimeMillis() - age.toMillis) << 16)
    TransactionDescription(TransactionId(UUID.randomUUID()), start, pointer, 0, requirements, Nil, None, Nil, Nil, ida,
      Map(poolId -> ida))

  private class Fixture:
    val tasks = new BackgroundTaskManager(ExecutionContext.global)
    val msgr = new CountingMessenger
    val client = new TestClient(msgr, tasks)
    def driver(txd: TransactionDescription): ClientTransactionDriver =
      SimpleClientTransactionDriver.factory(retransmit, horizon)(client, txd, Map())
    def done(): Unit = tasks.shutdown(Duration.Zero)

  test("a client gives up with Unknown once the transaction is past the horizon plus MaxStartLead"):
    val f = new Fixture
    try
      val d = f.driver(mktxd(giveUp - 300.millis))
      d.begin()
      f.msgr.prepares.get should be > 0
      Await.result(d.result, 5.seconds) should be(ClientOutcome.Unknown)
      val sent = f.msgr.prepares.get
      Thread.sleep(retransmit.toMillis * 5)
      f.msgr.prepares.get should be(sent)
    finally f.done()

  test("a client still past the horizon at begin fails fast without sending"):
    val f = new Fixture
    try
      val d = f.driver(mktxd(giveUp + 1.second))
      d.begin()
      d.result.isCompleted should be(true)
      Await.result(d.result, 1.second) should be(ClientOutcome.Unknown)
      Thread.sleep(retransmit.toMillis * 5)
      f.msgr.prepares.get should be(0)
    finally f.done()

  test("a resolution before the give-up completes Committed or Aborted, and a later give-up changes nothing"):
    val f = new Fixture
    try
      val txd = mktxd(giveUp - 300.millis)
      val d = f.driver(txd)
      d.begin()
      d.receive(TransactionResolved(client0, StoreId(poolId, 1), txd.transactionId, committed = true))
      Await.result(d.result, 1.second) should be(ClientOutcome.Committed)
      Thread.sleep(600) // past the give-up
      Await.result(d.result, 1.second) should be(ClientOutcome.Committed)

      val txd2 = mktxd(0.millis)
      val d2 = f.driver(txd2)
      d2.begin()
      d2.receive(TransactionResolved(client0, StoreId(poolId, 1), txd2.transactionId, committed = false))
      Await.result(d2.result, 1.second) should be(ClientOutcome.Aborted)
    finally f.done()
