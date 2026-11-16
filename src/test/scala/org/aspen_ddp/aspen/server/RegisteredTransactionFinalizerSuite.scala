package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.FinalizationAction
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateFinalizationAction
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation}
import org.aspen_ddp.aspen.server.RegisteredTransactionFinalizerFactory.Finalizer
import org.aspen_ddp.aspen.server.transaction.TransactionDriverSuite
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future, Promise}

object RegisteredTransactionFinalizerSuite:
  /** Runs callbacks inline, so completions are observable without waiting */
  given ExecutionContext with
    def execute(runnable: Runnable): Unit = runnable.run()
    def reportFailure(cause: Throwable): Unit = throw cause

  class StubAction extends FinalizationAction:
    val done: Promise[Unit] = Promise()
    var executed = false
    var cancelled = false
    def complete: Future[Unit] = done.future
    def execute(): Unit = executed = true
    override def cancel(): Unit = cancelled = true

  private val txd =
    import TransactionDriverSuite.*
    mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)

  /** A real MissedUpdateFinalizationAction (so the Finalizer identifies it by type) whose execute is only recorded.
    * Its constructor touches no client state
    */
  class StubMarker extends MissedUpdateFinalizationAction(null, txd):
    val done: Promise[Unit] = Promise()
    var executed = false
    var cancelled = false
    var errors: List[Map[StoreId, List[ObjectId]]] = Nil
    var voters: List[Option[Set[StoreId]]] = Nil
    var owed: Set[StoreId] = Set()
    override def handedOffStores: Set[StoreId] = owed
    override def complete: Future[Unit] = done.future
    override def execute(): Unit = executed = true
    override def cancel(): Unit = cancelled = true
    override def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = errors = commitErrors :: errors
    override def commitVoters(voters: Option[Set[StoreId]], incarnations: Map[StoreId, StoreIncarnation]): Unit =
      this.voters = voters :: this.voters

class RegisteredTransactionFinalizerSuite extends AnyFunSuite with Matchers:
  import RegisteredTransactionFinalizerSuite.{*, given}

  test("the missed-update marker executes only after every other action settles, and complete waits for all") {
    val a = new StubAction
    val b = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker, b))

    a.executed should be (true)
    b.executed should be (true)
    marker.executed should be (false)

    // Commit errors and voters given before the marker executes reach it
    val errors = Map(TransactionDriverSuite.ds0 -> List.empty[ObjectId])
    f.commitVoters(Some(Set(TransactionDriverSuite.ds0)), Map())
    f.updateCommitErrors(errors)
    marker.voters should be (List(Some(Set(TransactionDriverSuite.ds0))))
    marker.errors should be (List(errors))

    a.done.success(())
    marker.executed should be (false)
    b.done.success(())
    marker.executed should be (true)
    f.complete.isCompleted should be (false)

    marker.done.success(())
    f.complete.isCompleted should be (true)
  }

  // Ruling 8 (revised 2026-11-15): a failed action still lets the marker write its marks, so the stores that missed
  // the transaction are repaired while finalization is retried (transaction-recovery R-N2)
  test("a failed action still runs the marker once the others settle, and complete fails only after the marker settles") {
    val a = new StubAction
    val b = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker, b))

    val err = new Exception("action failed")
    b.done.failure(err)
    marker.executed should be (false)
    f.complete.isCompleted should be (false)

    a.done.success(())
    marker.executed should be (true)
    f.complete.isCompleted should be (false)

    marker.done.success(())
    f.complete.value should be (Some(scala.util.Failure(err)))
  }

  test("a cancel after a failed action, before the others settle, keeps the marker from executing") {
    val a = new StubAction
    val b = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker, b))

    b.done.failure(new Exception("action failed"))
    f.cancel()
    a.done.success(())
    marker.executed should be (false)
    marker.cancelled should be (true)
  }

  test("an action that cannot be created is a failed action, and the marker still runs") {
    val a = new StubAction
    val marker = new StubMarker
    val err = new NoSuchElementException("unknown finalization action type")
    val f = Finalizer.of(List(scala.util.Success(a), scala.util.Failure(err), scala.util.Success(marker)))

    a.executed should be (true)
    a.done.success(())
    marker.executed should be (true)
    marker.done.success(())
    f.complete.value should be (Some(scala.util.Failure(err)))
  }

  test("a failed marker fails complete") {
    val a = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker))

    a.done.success(())
    marker.executed should be (true)
    val err = new Exception("marker gave up")
    marker.done.failure(err)
    f.complete.value should be (Some(scala.util.Failure(err)))
  }

  test("cancel cancels every action and keeps a marker not yet started from executing") {
    val a = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker))

    f.cancel()
    a.cancelled should be (true)
    marker.cancelled should be (true)

    a.done.success(())
    marker.executed should be (false)
  }

  test("a finalizer reports the stores its marker owes, and none without a marker") {
    val m = new StubMarker
    m.owed = Set(TransactionDriverSuite.ds0, TransactionDriverSuite.ds1)
    val f = new Finalizer(List(new StubAction, m))
    f.handedOffStores should be (m.owed)
    new Finalizer(List(new StubAction)).handedOffStores should be (Set())
  }

  test("without a marker, every action executes at once") {
    val a = new StubAction
    val b = new StubAction
    val f = new Finalizer(List(a, b))
    a.executed should be (true)
    b.executed should be (true)
    a.done.success(()); b.done.success(())
    f.complete.isCompleted should be (true)
  }
