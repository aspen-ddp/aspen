package org.aspen_ddp.aspen.compute

import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.client.{AspenClient, StopRetrying, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object SteppedDurableTask:
  val StepStateKey: Key = Key(0xFE)

  def getInitialContent(initialState: Map[String, Array[Byte]]): (Key, Value) =
    val encoded = Codec.encodeSteppedDurableTaskState(0, initialState)
    (StepStateKey, Value(encoded))

abstract class SteppedDurableTask(
  val taskPointer: DurableTaskPointer,
  val client: AspenClient
) extends DurableTask with Logging:

  import SteppedDurableTask.*

  given ExecutionContext = client.clientContext

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]]

  def resultFromState(state: Map[String, Array[Byte]]): Option[AnyRef] = None

  private val promise = Promise[Option[AnyRef]]()

  def completed: Future[Option[AnyRef]] = promise.future

  doNextStep()

  def doNextStep(): Unit =
    for
      kvos <- client.read(taskPointer.kvPointer)
    yield
      val vs = kvos.contents(StepStateKey)
      val (step, stateMap) = Codec.decodeSteppedDurableTaskState(vs.value.bytes)

      if step >= steps.length then
        synchronized:
          if !promise.isCompleted then
            promise.success(resultFromState(stateMap))
      else
        val tx = client.newTransaction()

        steps(step)(tx, stateMap, vs.revision).onComplete:
          case Failure(err) => err match
            case e: StopRetrying =>
              synchronized:
                if !promise.isCompleted then
                  promise.failure(e)
            case e =>
              logger.error(s"Step $step failed for task ${taskPointer.kvPointer}, retrying", e)
              doNextStep()

          case Success(newState) =>
            val encoded = Codec.encodeSteppedDurableTaskState(step + 1, newState)
            val requirements = KeyValueUpdate.KeyRevision(StepStateKey, vs.revision) :: Nil
            val operations = Insert(StepStateKey, encoded) :: Nil

            tx.update(taskPointer.kvPointer, None, None, requirements, operations)

            tx.commit().onComplete:
              case Failure(err) =>
                err match
                  case _: TransactionAborted =>
                    logger.warn(s"Commit aborted at step $step for task ${taskPointer.kvPointer}, retrying")
                  case _ =>
                    logger.error(s"Commit failed at step $step for task ${taskPointer.kvPointer}, retrying", err)
                doNextStep()
              case Success(_) =>
                if step + 1 >= steps.length then
                  synchronized:
                    if !promise.isCompleted then
                      promise.success(resultFromState(newState))
                else
                  doNextStep()
