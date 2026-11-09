package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.network.{TxAcceptResponse, TxCommitted, TxFinalized, TxPrepareResponse, TxResolved}
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

    def fireAll(): Unit =
      val due = tasks.filterNot(_.cancelled)
      tasks = Nil
      due.foreach(_.fn())

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

  /** A leader for stores 0 to 2 whose transaction has committed and whose finalization never completes */
  def resolvedDriver(): (SimpleTransactionDriver, TMessenger, ManualTasks) =
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val messenger = new TMessenger()
    val tasks = new ManualTasks(summon[ExecutionContext])
    val driver = new SimpleTransactionDriver(summon[ExecutionContext], 100.milliseconds, 3.seconds, ds0, messenger, tasks,
      txd, new TFinalizer(autoComplete = false))
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

    driver.receiveTxFinalized(TxFinalized(ds0, ds1, driver.txd.transactionId, committed = true, late = false))
    messenger.clear()

    tasks.fireAll()
    resolvedSentTo(messenger) shouldBe empty
  }
}
