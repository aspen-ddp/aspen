package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.HLCTimestamp

import org.aspen_ddp.aspen.common.network.{TxAccept, TxAcceptResponse, TxCommitted, TxFinalized, TxPrepare, TxPrepareResponse, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, TransactionDisposition}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*

object SimpleTransactionDriverSuite:

  /** Runs nothing by itself: a test fires the scheduled tasks */
  class ManualTasks(ec: ExecutionContext) extends BackgroundTaskManager(ec):
    private class Task(val fn: () => Unit) extends BackgroundTaskManager.ScheduledTask:
      var cancelled = false
      override def cancel(): Unit = cancelled = true

    private var tasks: List[Task] = Nil

    override def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask =
      val t = new Task(() => fn)
      tasks = t :: tasks
      t

    def fireAll(ignoreCancel: Boolean = false): Unit =
      val due = if ignoreCancel then tasks else tasks.filterNot(_.cancelled)
      tasks = Nil
      due.foreach(_.fn())

    def pending: Int = tasks.count(!_.cancelled)

class SimpleTransactionDriverSuite extends AnyFunSuite with Matchers {
  import SimpleTransactionDriverSuite.*
  import TransactionDriverSuite.*

  // A live driver re-broadcasts only every maxDelay, so a shorter failure threshold would have
  // the other stores start competing drivers for a transaction whose leader is merely slow.
  test("factory waits longer than the driver's re-broadcast interval before declaring a driver failed") {
    val maxDelay: Duration = 3.seconds
    val factory = SimpleTransactionDriver.factory(100.milliseconds, maxDelay)

    factory.failedDriverDuration should be > maxDelay
  }

  /** A leader's first driver for stores 0 to 2 whose transaction has committed and whose finalization never completes */
  def resolvedDriver(handedOff: Set[StoreId] = Set()): (SimpleTransactionDriver, TMessenger, ManualTasks) =
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val tasks = new ManualTasks(summon[ExecutionContext])
    val finalizer = new TFinalizer(autoComplete = false)
    finalizer.handedOff = handedOff
    val driver = new SimpleTransactionDriver(summon[ExecutionContext], 100.milliseconds, 3.seconds, ds0, messenger, tasks,
      txd, finalizer, TransactionDriver.Start(initialProposal = true))
    val proposal = ProposalId(1, 0)

    for from <- List(ds0, ds1, ds2) do
      driver.receiveTxPrepareResponse(TxPrepareResponse(ds0, from, txd.transactionId,
        Right(TxPrepareResponse.Promise(None)), proposal, TransactionDisposition.VoteCommit, Nil))
    for from <- List(ds0, ds1) do
      driver.receiveTxAcceptResponse(TxAcceptResponse(ds0, from, txd.transactionId, proposal,
        Right(TxAcceptResponse.Accepted(true))))

    messenger.clear()
    (driver, messenger, tasks)

  def resolvedSentTo(messenger: TMessenger): Set[StoreId] =
    messenger.messages.collect { case m: TxResolved =>
      m.committed shouldBe true
      m.to
    }.toSet

  // A lost TxResolved otherwise leaves the stores holding the transaction's locks until it finalizes
  test("a resolved driver retransmits TxResolved until each store is known to have it") {
    val (driver, messenger, tasks) = resolvedDriver()

    tasks.fireAll()
    resolvedSentTo(messenger) shouldBe Set(ds0, ds1, ds2)

    messenger.clear()
    driver.receiveTxCommitted(TxCommitted(ds0, ds1, driver.txd.transactionId, Nil))

    tasks.fireAll()
    resolvedSentTo(messenger) shouldBe Set(ds0, ds2)
  }

  test("a finalized driver stops retransmitting TxResolved") {
    val (driver, messenger, tasks) = resolvedDriver()

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, driver.txd.transactionId, committed = true, late = false, owed = Set(),
      startTimestamp = HLCTimestamp.now))
    messenger.clear()

    tasks.fireAll()
    resolvedSentTo(messenger) shouldBe empty
  }

  // Ruling 7: a driver whose own mark is in flight finalizes on another driver's TxFinalized at once. Retransmitting
  // non-late TxResolved meanwhile could reach a replay's fresh Tx after the status cache forgot the transaction
  test("a driver whose mark was handed off sends no TxResolved after another driver's TxFinalized") {
    val (driver, messenger, tasks) = resolvedDriver(handedOff = Set(ds2))

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, driver.txd.transactionId, committed = true, late = false, owed = Set(ds1),
      startTimestamp = HLCTimestamp.now))
    val finals = messenger.messages.collect { case f: TxFinalized => (f.owed, f.late) }.toSet
    finals shouldBe Set((Set(ds1, ds2), false))
    messenger.clear()

    tasks.fireAll(ignoreCancel = true)
    resolvedSentTo(messenger) shouldBe empty
    tasks.pending shouldBe 0
  }

  def preparedAt(messenger: TMessenger): Set[ProposalId] = messenger.messages.collect { case p: TxPrepare => p.proposalId }.toSet

  def startedDriver(storeId: StoreId, start: TransactionDriver.Start,
                    txd: org.aspen_ddp.aspen.common.transaction.TransactionDescription =
                      mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil))
      : (SimpleTransactionDriver, TMessenger, ManualTasks) =
    val messenger = new TMessenger()
    val tasks = new ManualTasks(summon[ExecutionContext])
    val driver = new SimpleTransactionDriver(summon[ExecutionContext], 100.milliseconds, 3.seconds, storeId, messenger,
      tasks, txd, new TFinalizer(autoComplete = false), start)
    tasks.fireAll()
    (driver, messenger, tasks)

  // Paxos F5: a restarted or replacement leader driver reusing (1, L) would be a second proposer with the first one's id
  test("only the designated leader's first driver starts at the initial proposal") {
    val (_, first, _) = startedDriver(ds0, TransactionDriver.Start(initialProposal = true))
    preparedAt(first) shouldBe Set(ProposalId(1, 0))

    val (_, later, _) = startedDriver(ds0, TransactionDriver.Start())
    preparedAt(later) shouldBe Set(ProposalId(2, 0))
  }

  test("a driver that is not the leader's first starts above what its own acceptor promised or accepted") {
    val (_, leader, _) = startedDriver(ds0, TransactionDriver.Start(highestSeen = Some(ProposalId(5, 2))))
    preparedAt(leader) shouldBe Set(ProposalId(6, 0))

    val (_, recovery, _) = startedDriver(ds1, TransactionDriver.Start(highestSeen = Some(ProposalId(3, 0))))
    preparedAt(recovery) shouldBe Set(ProposalId(4, 1))
  }

  // Paxos F4: the retransmit timer sends the current Accept itself; a learn-only driver must not
  test("a learn-only driver's retransmits send prepares, never an Accept of an adopted value") {
    val otherPool = PoolId(java.util.UUID.randomUUID())
    val otherObj = DataObjectPointer(ObjectId(java.util.UUID.randomUUID()), otherPool, Array[Byte]())
    val txd = mktxd(simpleObj, List(DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite),
      DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite)))
    val secondary = StoreId(otherPool, 0)
    val (driver, messenger, tasks) = startedDriver(secondary, TransactionDriver.Start(initialProposal = true), txd)
    val round = preparedAt(messenger).head

    for from <- List(ds0, ds1, ds2) do
      driver.receiveTxPrepareResponse(TxPrepareResponse(secondary, from, txd.transactionId,
        Right(TxPrepareResponse.Promise(Some((ProposalId(1, 0), true)))), round, TransactionDisposition.VoteCommit, Nil))
    messenger.clear()

    tasks.fireAll()
    messenger.messages.collect { case a: TxAccept => a } shouldBe Nil
    preparedAt(messenger) should not be empty
  }

  // Ruling 31: Store.close shuts its drivers down. A timer already firing must not restart the retry loop
  test("a shut-down driver whose timer fires anyway sends nothing and schedules nothing") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val tasks = new ManualTasks(summon[ExecutionContext])
    val driver = new SimpleTransactionDriver(summon[ExecutionContext], 100.milliseconds, 3.seconds, ds0, messenger, tasks,
      txd, new TFinalizer(autoComplete = false))
    messenger.clear()

    driver.shutdown()
    tasks.fireAll(ignoreCancel = true)

    messenger.messages shouldBe Nil
    tasks.pending shouldBe 0
  }
}
