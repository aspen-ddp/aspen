package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.{ReadDriverClient, TransactionStatusCache}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, HostMessage, TransactionResolved, TransactionVote, TxMessage, TxPrepare}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{RefcountUpdate, TransactionDescription, TransactionDisposition, TransactionHorizon, TransactionId}
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

  private val pool2 = PoolId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
  private val pointer2 = DataObjectPointer(ObjectId(new UUID(0, 2)), pool2, Array[Byte]())
  private val ida2 = Replication(5, 3)

  private def mktxd2(startMillis: Long): TransactionDescription =
    val requirements = RefcountUpdate(pointer, ObjectRefcount(0, 1), ObjectRefcount(1, 2)) ::
      RefcountUpdate(pointer2, ObjectRefcount(0, 1), ObjectRefcount(1, 2)) :: Nil
    TransactionDescription(TransactionId(UUID.randomUUID()), HLCTimestamp(startMillis << 16), pointer, 0, requirements,
      Nil, None, Nil, Nil, ida, Map(poolId -> ida, pool2 -> ida2))

  private def vote(d: ClientTransactionDriver, pool: PoolId, idx: Int,
                   disp: TransactionDisposition.Value = TransactionDisposition.VoteCommit): Unit =
    d.receive(TransactionVote(client0, StoreId(pool, idx.toByte), d.txd.transactionId, disp))

  private def resolved(d: ClientTransactionDriver, committed: Boolean): Unit =
    d.receive(TransactionResolved(client0, StoreId(poolId, 1), d.txd.transactionId, committed))

  test("two commit votes complete Committed; one does not"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0)
      d.result.isCompleted shouldBe false
      vote(d, poolId, 1)
      d.result.value shouldBe Some(scala.util.Success(ClientOutcome.Committed))
    finally f.done()

  test("each object needs its own pool's write threshold"):
    val f = new Fixture
    try
      val d = SimpleClientTransactionDriver.factory(1.hour, horizon, () => f.now)(f.client, mktxd2(f.now), Map())
      d.begin()
      vote(d, poolId, 0); vote(d, poolId, 1)
      vote(d, pool2, 0); vote(d, pool2, 1)
      d.result.isCompleted shouldBe false
      vote(d, pool2, 2)
      d.result.value shouldBe Some(scala.util.Success(ClientOutcome.Committed))
    finally f.done()

  test("abort votes never complete the driver"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      (0 until 3).foreach(i => vote(d, poolId, i, TransactionDisposition.VoteAbort))
      d.result.isCompleted shouldBe false
      resolved(d, false)
      d.result.value shouldBe Some(scala.util.Success(ClientOutcome.Aborted))
    finally f.done()

  test("a duplicate vote counts once"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0); vote(d, poolId, 0)
      d.result.isCompleted shouldBe false
    finally f.done()

  test("Undetermined is no vote"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0); vote(d, poolId, 1, TransactionDisposition.Undetermined)
      d.result.isCompleted shouldBe false
    finally f.done()

  test("after a vote-completion the driver keeps retransmitting until the leader's outcome"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0); vote(d, poolId, 1)
      d.result.isCompleted shouldBe true
      d.stopped.isCompleted shouldBe false
      val sent = f.msgr.prepares.get
      d.tick()
      f.msgr.prepares.get should be > sent
      resolved(d, true)
      d.stopped.isCompleted shouldBe true
      val after = f.msgr.prepares.get
      d.tick()
      f.msgr.prepares.get shouldBe after
    finally f.done()

  test("after a vote-completion the driver stops at the give-up point with its result unchanged"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0); vote(d, poolId, 1)
      f.now += giveUp.toMillis + 1
      d.tick()
      d.stopped.isCompleted shouldBe true
      d.result.value shouldBe Some(scala.util.Success(ClientOutcome.Committed))
    finally f.done()

  /** The log lines, at Info or above, that name `txid`, with their levels.
    *
    * The handler goes on the driver's own logger (`scribe.Logging` names it by class), not the root: suites run in
    * parallel in one JVM and several swap root handlers with an unsynchronized read-modify-write
    * (`root.withHandler(h).replace()`), so a concurrent swap overwrites a root handler added here and its lines are
    * lost. No other suite replaces the driver's logger, and this suite's tests run sequentially.
    */
  private def logNaming(txid: TransactionId)(f: => Unit): List[(scribe.Level, String)] =
    val lines = new java.util.concurrent.LinkedBlockingQueue[(scribe.Level, String)]()
    val handler = scribe.handler.LogHandler(scribe.Level.Info): r =>
      val text = r.logOutput.plainText
      if text.contains(txid.toString) then lines.add((r.level, text))
    def driverLogger = scribe.Logger(classOf[SimpleClientTransactionDriver].getName)
    driverLogger.withHandler(handler).replace()
    try f
    finally driverLogger.withoutHandler(handler).replace()
    lines.toArray(Array.empty[(scribe.Level, String)]).toList

  test("a leader abort after a vote commit is logged as a split, and the result stays Committed"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0); vote(d, poolId, 1)
      val log = logNaming(d.txd.transactionId)(resolved(d, false))
      d.result.value shouldBe Some(scala.util.Success(ClientOutcome.Committed))
      d.stopped.isCompleted shouldBe true
      log.map(_._1) shouldBe List(scribe.Level.Error)
      log.head._2 should include("Split decision")
    finally f.done()

  test("a differing second vote from the same store is logged and ignored"):
    val f = new Fixture
    try
      // Commit first: the later abort is ignored, so store 0's commit still counts toward the write threshold
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0)
      val log = logNaming(d.txd.transactionId)(vote(d, poolId, 0, TransactionDisposition.VoteAbort))
      log.map(_._1) shouldBe List(scribe.Level.Error)
      log.head._2 should include("ignoring the later vote")
      vote(d, poolId, 1)
      d.result.value shouldBe Some(scala.util.Success(ClientOutcome.Committed))

      // Abort first: the later commit is ignored, so one commit vote is short of the write threshold
      val d2 = f.driver(0.millis)
      d2.begin()
      vote(d2, poolId, 0, TransactionDisposition.VoteAbort)
      logNaming(d2.txd.transactionId)(vote(d2, poolId, 0)).map(_._1) shouldBe List(scribe.Level.Error)
      vote(d2, poolId, 1)
      d2.result.isCompleted shouldBe false
    finally f.done()

  test("after a vote-completion a retransmit tick logs no hung transaction, and a give-up warns of no unknown outcome"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      vote(d, poolId, 0); vote(d, poolId, 1)
      // Three ticks reach the "HUNG" log of an undecided transaction
      logNaming(d.txd.transactionId)((0 until 3).foreach(_ => d.tick())) shouldBe Nil
      f.now += giveUp.toMillis + 1
      val log = logNaming(d.txd.transactionId)(d.tick())
      d.stopped.isCompleted shouldBe true
      log.map(_._1) shouldBe List(scribe.Level.Info)
      log.head._2 should not include "outcome unknown"
    finally f.done()

  test("an undecided transaction still logs that it is hung and warns that its outcome is unknown at give-up"):
    val f = new Fixture
    try
      val d = f.driver(0.millis)
      d.begin()
      logNaming(d.txd.transactionId)((0 until 3).foreach(_ => d.tick())).map(_._2).exists(_.contains("HUNG")) shouldBe
        true
      f.now += giveUp.toMillis + 1
      val log = logNaming(d.txd.transactionId)(d.tick())
      log.exists((level, text) => level == scribe.Level.Warn && text.contains("outcome unknown")) shouldBe true
    finally f.done()
