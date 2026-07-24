package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.{byte2long, long2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}

import java.util.UUID
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Test-only DurableTask. Performs `targetBumps` version-bump transactions on its own state
 *  object (one per scheduled tick), then completes. A `targetBumps` of 0 completes immediately. */
object CountingSystemTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("e7b3a1c9-5d42-4f86-9a0b-3c1e8d7f206b")

  val TargetKey: Key = Key(10)
  val DoneCountKey: Key = Key(11)

  def initialState(targetBumps: Long): Map[Key, Array[Byte]] =
    Map(TargetKey -> long2byte(targetBumps), DoneCountKey -> long2byte(0))

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    new CountingSystemTask(pointer, client)


class CountingSystemTask(val taskPointer: DurableTaskPointer,
                         client: AspenClient) extends DurableTask:
  import CountingSystemTask.*

  private given ExecutionContext = client.clientContext
  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  step()

  override protected def onStop(): Unit = synchronized:
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def step(): Unit =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
    else
      client.read(taskPointer.kvPointer).onComplete:
        case Failure(_) =>
          client.backgroundTaskManager.schedule(Duration(20, MILLISECONDS))(step())
        case Success(kvos) =>
          val target = byte2long(kvos.contents(TargetKey).value.bytes)
          val done = byte2long(kvos.contents(DoneCountKey).value.bytes)
          if done >= target then
            synchronized { if !promise.isCompleted then promise.success(None) }
          else
            val tx = client.newTransaction()
            val reqs = KeyValueUpdate.KeyRevision(DoneCountKey, kvos.contents(DoneCountKey).revision) :: Nil
            val ops = Insert(DoneCountKey, long2byte(done + 1)) :: Nil
            tx.update(taskPointer.kvPointer, None, None, reqs, ops)
            tx.commit().onComplete:
              case Success(_) => step()
              case Failure(_) => client.backgroundTaskManager.schedule(Duration(20, MILLISECONDS))(step())
