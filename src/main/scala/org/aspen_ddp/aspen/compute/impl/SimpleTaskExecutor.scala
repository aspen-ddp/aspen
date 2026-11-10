package org.aspen_ddp.aspen.compute.impl

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ObjectAllocator, StopRetrying, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.objects.{Delete, Insert, Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, DurableTaskFactory, TaskExecutor}
import scribe.Logging
import org.aspen_ddp.aspen.common.util.{uuid2byte, byte2uuid}

import scala.concurrent.{ExecutionContext, Future}

object SimpleTaskExecutor:

  val TaskTypeKey = Key(UUID.fromString("f13fbfd4-85fa-449c-b279-9859ee5e7de0"))

  def apply(client: AspenClient,
            taskStateAllocator: ObjectAllocator,
            executorObject: KeyValueObjectPointer): Future[SimpleTaskExecutor] =

    given ExecutionContext = client.clientContext

    client.read(executorObject).map(kvos => new SimpleTaskExecutor(client, taskStateAllocator, kvos))

  def createNewExecutor(client: AspenClient,
                        executorAllocator: ObjectAllocator,
                        taskStateAllocator: ObjectAllocator)
                       (using t: Transaction): Future[(KeyValueObjectPointer, SimpleTaskExecutor)] =

    given ExecutionContext = client.clientContext

    for
      executor <- executorAllocator.allocateKeyValueObject(Map())
      kvos <- client.read(executor)
    yield
      (executor, new SimpleTaskExecutor(client, taskStateAllocator, kvos))


class SimpleTaskExecutor(val client: AspenClient,
                         val taskStateAllocator: ObjectAllocator,
                         kvos: KeyValueObjectState) extends TaskExecutor with Logging:

  import SimpleTaskExecutor._

  given ExecutionContext = client.clientContext

  private val executorObject: KeyValueObjectPointer = kvos.pointer
  private var executorRevision: ObjectRevision = kvos.revision

  protected var active: Map[DurableTaskPointer, DurableTask] = Map.empty
  /** Task objects free for reuse, each with its revision as last read or written by this executor, its only writer.
    * prepareTask's write requires that revision, which pins the enrolling transaction
    */
  protected var inactive: List[(DurableTaskPointer, ObjectRevision)] = Nil

  def shutdown(): Unit = synchronized:
    active.values.foreach(_.stop())

  synchronized:
    kvos.contents.valuesIterator.foreach: vs =>
      val taskPointer = DurableTaskPointer(KeyValueObjectPointer(vs.value.bytes))
      client.read(taskPointer.kvPointer).foreach: kvos =>
        synchronized:
          if kvos.contents.isEmpty || !kvos.contents.contains(TaskTypeKey) then
            inactive = (taskPointer, kvos.revision) :: inactive
          else
            val taskType = byte2uuid(kvos.contents(TaskTypeKey).value.bytes)

            client.typeRegistry.getType[DurableTaskFactory](taskType) match
              case None =>
                // Neither resumed nor reusable: the task is pending state this client cannot interpret,
                // and reusing it would overwrite that state.
                logger.error(s"Task ${taskPointer.kvPointer} has unregistered type $taskType; it is neither resumed nor reused")

              case Some(dtt) =>
                val task = dtt.createTask(client, taskPointer, kvos.revision, kvos.contents, this)
                active += taskPointer -> task

  /** Allocates a task object and records it in the executor object. The executor object's update requires its
    * revision (cached, and refreshed after a collision), which pins the transaction: the new key alone
    * (`DoesNotExist`) would not. Completes with the object and its revision once allocated.
    */
  private def allocateTask(): Future[(DurableTaskPointer, ObjectRevision)] =
    // Only a collision is retried, after refreshing the executor's revision. Anything else -- an
    // unreadable executor object, an executor object grown past its size limit -- is permanent.
    def onFail(err: Throwable): Future[Unit] = err match
      case _: TransactionAborted =>
        client.read(executorObject).map: kvos =>
          synchronized:
            executorRevision = kvos.revision
      case e => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery[DurableTaskPointer](onFail): tx =>
      given Transaction = tx
      synchronized:
        val taskKey = Key(UUID.randomUUID())
        val kreqs = KeyValueUpdate.DoesNotExist(taskKey) :: Nil
        val requiredRevision = executorRevision
        for
          ptr <- taskStateAllocator.allocateKeyValueObject(Map())
        yield
          tx.update(executorObject, Some(requiredRevision), None, kreqs, Insert(taskKey, ptr.toArray) :: Nil)
          tx.result.foreach: _ =>
            synchronized:
              if executorRevision == requiredRevision then
                executorRevision = tx.revision
          DurableTaskPointer(ptr)
    .flatMap: taskPointer =>
      // A new KeyValue object's revision is not the allocating transaction's, so it is read
      client.readMetadata(taskPointer.kvPointer).map(m => (taskPointer, m.revision))

  /** Empties a completed task's object for reuse. The write requires the object's revision as read, which pins it */
  private def deallocateTask(task: DurableTaskPointer): Unit =
    client.transactUntilSuccessful[Unit]: tx =>
      given Transaction = tx

      client.read(task.kvPointer).map: kvos =>
        val deletes = kvos.contents.keys.map(k => Delete(k)).toList
        tx.update(task.kvPointer, Some(kvos.revision), None, Nil, deletes)
        tx.result.foreach: _ =>
          synchronized:
            active -= task
            inactive = (task, tx.revision) :: inactive

  /** The task's write requires the reused object's revision, as this executor last saw it, which pins `tx` */
  override def prepareTask(taskType: DurableTaskFactory,
                           initialState: List[(Key, Array[Byte])])
                          (using tx: Transaction): Future[Future[Option[AnyRef]]] = synchronized:
    val initial = initialState.map(t => Insert(t._1, t._2))

    val ins = Insert(TaskTypeKey, uuid2byte(taskType.typeUUID)) :: initial

    val fptr =
      if inactive.isEmpty then allocateTask()
      else
        val p = inactive.head
        inactive = inactive.tail
        Future.successful(p)

    for
      (taskPointer, taskRevision) <- fptr
    yield
      tx.update(taskPointer.kvPointer, Some(taskRevision), None, Nil, ins)

      // Not written, so free again. Re-read: a stale revision would fail every later use
      tx.result.failed.foreach: _ =>
        client.read(taskPointer.kvPointer).foreach: kvos =>
          synchronized:
            inactive = (taskPointer, kvos.revision) :: inactive

      tx.result.flatMap: _ =>
        client.read(taskPointer.kvPointer).flatMap: kvos =>
          synchronized:
            val task = taskType.createTask(client, taskPointer, kvos.revision, kvos.contents, this)

            active += taskPointer -> task
            task.completed.foreach: _ =>
              deallocateTask(taskPointer)
            task.completed
