package org.aspen_ddp.aspen.compute.impl

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, Delete, Insert, Key, KeyValueObjectPointer, ObjectRevision, ObjectRevisionGuard}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.compute.{DurableTaskPointer, DurableTaskFactory, TaskExecutor}
import org.aspen_ddp.aspen.common.util.{uuid2byte, byte2uuid}

import scala.concurrent.{ExecutionContext, Future}

object SimpleTaskExecutor {

  val TaskTypeKey = Key(UUID.fromString("f13fbfd4-85fa-449c-b279-9859ee5e7de0"))

  def apply(client: AspenClient,
            taskStateAllocator: ObjectAllocator,
            executorObject: KeyValueObjectPointer): Future[SimpleTaskExecutor] = {

    given ExecutionContext = client.clientContext

    client.read(executorObject).map( kvos => new SimpleTaskExecutor(client, taskStateAllocator, kvos))
  }

  def createNewExecutor(client: AspenClient,
                        executorAllocator: ObjectAllocator,
                        taskStateAllocator: ObjectAllocator,
                        revisionGuard: AllocationRevisionGuard)
                       (using t: Transaction): Future[(KeyValueObjectPointer, SimpleTaskExecutor)] = {

    given ExecutionContext = client.clientContext

    for {
      executor <- executorAllocator.allocateKeyValueObject(revisionGuard, Map())
      kvos <- client.read(executor)
    } yield {
      (executor, new SimpleTaskExecutor(client, taskStateAllocator, kvos))
    }
  }
}

class SimpleTaskExecutor(val client: AspenClient,
                         val taskStateAllocator: ObjectAllocator,
                         kvos: KeyValueObjectState) extends TaskExecutor {

  import SimpleTaskExecutor._

  given ExecutionContext = client.clientContext

  private val executorObject: KeyValueObjectPointer = kvos.pointer
  private var executorRevision: ObjectRevision = kvos.revision

  protected var active: Set[DurableTaskPointer] = Set()
  protected var inactive: List[DurableTaskPointer] = Nil

  synchronized {
    kvos.contents.valuesIterator.foreach { vs =>
      val taskPointer = DurableTaskPointer(KeyValueObjectPointer(vs.value.bytes))
      client.read(taskPointer.kvPointer).foreach { kvos =>
        synchronized {
          if (kvos.contents.isEmpty || !kvos.contents.contains(TaskTypeKey))
            inactive = taskPointer :: inactive
          else {
            val taskType = byte2uuid(kvos.contents(TaskTypeKey).value.bytes)

            client.typeRegistry.getType[DurableTaskFactory](taskType) match {
              case None => // TODO Log a warning. This should not be possible
                inactive = taskPointer :: inactive

              case Some(dtt) =>
                dtt.createTask(client, taskPointer, kvos.revision, kvos.contents)
                active += taskPointer
            }
          }
        }
      }
    }
  }

  private def allocateTask(): Future[DurableTaskPointer] = {
    def onFail(err: Throwable): Future[Unit] = {
      client.read(executorObject).map { kvos =>
        synchronized {
          executorRevision = kvos.revision
        }
      }
    }
    client.transactUntilSuccessfulWithRecovery[DurableTaskPointer](onFail) { tx =>
      given Transaction = tx
      synchronized {
        val guard = ObjectRevisionGuard(executorObject, executorRevision)
        val taskKey = Key(UUID.randomUUID())
        val kreqs = KeyValueUpdate.DoesNotExist(taskKey) :: Nil
        for {
          ptr <- taskStateAllocator.allocateKeyValueObject(guard, Map())
        } yield {
          tx.update(executorObject, None, None, kreqs, Insert(taskKey, ptr.toArray) :: Nil)
          DurableTaskPointer(ptr)
        }
      }
    }
  }

  private def deallocateTask(task: DurableTaskPointer): Unit = {
    client.transactUntilSuccessful[Unit] { tx =>
      given Transaction = tx
      
      client.read(task.kvPointer).map { kvos =>
        val deletes = kvos.contents.keys.map(k => Delete(k)).toList
        tx.update(task.kvPointer, None, None, Nil, deletes)
        tx.result.foreach { _ =>
          synchronized {
            active -= task
            inactive = task :: inactive
          }
        }
      }
    }
  }

  override def prepareTask(taskType: DurableTaskFactory,
                           initialState: List[(Key, Array[Byte])])
                          (using tx: Transaction): Future[Future[Option[AnyRef]]] = synchronized {
    val initial = initialState.map(t => Insert(t._1, t._2))

    val ins = Insert(TaskTypeKey,uuid2byte(taskType.typeUUID)) :: initial

    val fptr = if (inactive.isEmpty) allocateTask() else {
      val p = inactive.head
      inactive = inactive.tail
      Future.successful(p)
    }

    for {
      taskPointer <- fptr
    } yield {
      tx.update(taskPointer.kvPointer, None, None, Nil, ins)

      tx.result.failed.foreach { _ =>
        synchronized {
          inactive = taskPointer :: inactive
        }
      }

      tx.result.flatMap { _ =>
        client.read(taskPointer.kvPointer).flatMap { kvos =>
          synchronized {
            val task = taskType.createTask(client, taskPointer, kvos.revision, kvos.contents)

            active += taskPointer
            task.completed.foreach { _ =>
              deallocateTask(taskPointer)
            }
            task.completed
          }
        }
      }
    }
  }
}