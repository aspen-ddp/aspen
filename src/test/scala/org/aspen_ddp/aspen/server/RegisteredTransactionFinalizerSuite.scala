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

  test("the missed-update marker executes at once with the other actions, and complete waits for all") {
    val a = new StubAction
    val b = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker, b))

    a.executed should be (true)
    b.executed should be (true)
    marker.executed should be (true)

    // Commit errors and voters given after the marker executes reach it
    val errors = Map(TransactionDriverSuite.ds0 -> List.empty[ObjectId])
    f.commitVoters(Some(Set(TransactionDriverSuite.ds0)), Map())
    f.updateCommitErrors(errors)
    marker.voters should be (List(Some(Set(TransactionDriverSuite.ds0))))
    marker.errors should be (List(errors))

    marker.done.success(())
    a.done.success(())
    f.complete.isCompleted should be (false)
    b.done.success(())
    f.complete.value should be (Some(scala.util.Success(())))
  }

  // An action that never settles (one retrying through an outage) does not hold up the
  // marker, so the stores that missed the transaction are repaired meanwhile
  test("an action that never settles does not keep the marker from executing, and complete waits for it") {
    val stuck = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(stuck, marker))

    marker.executed should be (true)
    marker.done.success(())
    f.complete.isCompleted should be (false)
  }

  test("a failed action fails complete only after the marker settles") {
    val a = new StubAction
    val b = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker, b))

    val err = new Exception("action failed")
    b.done.failure(err)
    a.done.success(())
    f.complete.isCompleted should be (false)

    marker.done.success(())
    f.complete.value should be (Some(scala.util.Failure(err)))
  }

  test("an action that cannot be created is a failed action, and the marker still runs") {
    val a = new StubAction
    val marker = new StubMarker
    val err = new NoSuchElementException("unknown finalization action type")
    val f = Finalizer.of(List(scala.util.Success(a), scala.util.Failure(err), scala.util.Success(marker)))

    a.executed should be (true)
    marker.executed should be (true)
    a.done.success(())
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

  test("cancel cancels every action, the marker included") {
    val a = new StubAction
    val marker = new StubMarker
    val f = new Finalizer(List(a, marker))

    f.cancel()
    a.cancelled should be (true)
    marker.cancelled should be (true)
  }

  test("a finalizer reports the stores its marker owes, and none without a marker") {
    val m = new StubMarker
    m.owed = Set(TransactionDriverSuite.ds0, TransactionDriverSuite.ds1)
    val f = new Finalizer(List(new StubAction, m))
    f.handedOffStores should be (m.owed)
    new Finalizer(List(new StubAction)).handedOffStores should be (Set())
  }
