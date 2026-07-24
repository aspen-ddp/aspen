package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.aspen_ddp.aspen.common.util.byte2uuid
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor}
import scribe.Logging

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Per-host transient executor for system tasks. Runs assigned tasks reconstructed from their
 *  durable state object; does NOT persist them to any per-host executor. If this host dies, the
 *  service detects the stalled heartbeat and reassigns.
 *
 *  `taskExecutor` is supplied to reconstructed tasks that spawn sub-tasks (e.g. cascading
 *  UpdateAllocationGroupUsageTask); tasks that don't spawn sub-tasks ignore it. */
class SystemTaskRunner(val client: AspenClient,
                       val serviceStatePtr: KeyValueObjectPointer,
                       val hostId: HostId,
                       taskExecutor: TaskExecutor = SystemTaskRunner.NoOpTaskExecutor) extends Logging:

  private given ExecutionContext = client.clientContext

  // taskId -> (task, state pointer)
  private var running: Map[UUID, (DurableTask, KeyValueObjectPointer)] = Map.empty
  @volatile private var stopped = false

  def shutdown(): Unit = synchronized:
    stopped = true
    running.values.foreach((task, _) => task.stop())
    running = Map.empty

  /** Handle an ExecuteSystemTask message: start the task if not already running it. */
  def receive(msg: ExecuteSystemTask): Unit =
    if synchronized(!stopped && !running.contains(msg.taskId)) then
      start(msg.taskId, msg.taskStatePointer)

  private def start(taskId: UUID, taskStatePtr: KeyValueObjectPointer): Unit =
    client.read(taskStatePtr).foreach: kvos =>
      kvos.contents.get(SimpleTaskExecutor.TaskTypeKey) match
        case None =>
          logger.warn(s"System task $taskId has no TaskTypeKey; ignoring")
        case Some(vs) =>
          val taskType = byte2uuid(vs.value.bytes)
          client.typeRegistry.getType[DurableTaskFactory](taskType) match
            case None =>
              logger.warn(s"No DurableTaskFactory for type $taskType (task $taskId); ignoring")
            case Some(factory) =>
              val ptr = DurableTaskPointer(taskStatePtr)
              val task = factory.createTask(client, ptr, kvos.revision, kvos.contents, taskExecutor)
              val added = synchronized:
                if stopped || running.contains(taskId) then false
                else { running += taskId -> (task, taskStatePtr); true }
              if !added then
                task.stop()
              else
                // Immediate claim/liveness bump.
                bumpOne(taskStatePtr)
                task.completed.onComplete: _ =>
                  onTaskComplete(taskId, taskStatePtr)

  private def onTaskComplete(taskId: UUID, taskStatePtr: KeyValueObjectPointer): Unit =
    synchronized { running -= taskId }
    // Remove from the registry and drop the task-state object, then wake the service.
    val f = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      SystemTaskServiceState.scan(client, serviceStatePtr).flatMap: enrolled =>
        if !enrolled.exists(_._1 == taskId) then
          // Already cleaned up by a prior committed attempt (idempotent): do not double-decrement.
          Future.unit
        else
          for
            taskKvos <- client.read(taskStatePtr)
            _ <- SystemTaskServiceState.removeInTx(client, serviceStatePtr, taskId)
          yield
            tx.setRefcount(taskStatePtr, taskKvos.refcount, taskKvos.refcount.decrement())
    f.onComplete:
      case scala.util.Success(_) =>
        client.sendServiceMessage(SystemTaskExecutorService.ServiceUUID,
          SystemTaskMessage.encode(SystemTaskComplete(taskId)))
      case scala.util.Failure(err) =>
        logger.warn(s"System task $taskId completion cleanup failed: $err")

  /** Bump the version of every running task's state object (the heartbeat). */
  def heartbeat(): Future[Unit] =
    val ptrs = synchronized(running.values.map(_._2).toList)
    Future.traverse(ptrs)(bumpOne).map(_ => ())

  private def bumpOne(taskStatePtr: KeyValueObjectPointer): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      client.read(taskStatePtr).map: kvos =>
        tx.bumpVersion(taskStatePtr, kvos.revision)


object SystemTaskRunner:
  /** For tasks that never spawn sub-tasks. Fails loudly if a task tries to use it. */
  object NoOpTaskExecutor extends TaskExecutor:
    def prepareTask(taskType: DurableTaskFactory,
                    initialState: List[(org.aspen_ddp.aspen.common.objects.Key, Array[Byte])])
                   (using tx: Transaction): Future[Future[Option[AnyRef]]] =
      Future.failed(new UnsupportedOperationException(
        "SystemTaskRunner.NoOpTaskExecutor cannot prepare sub-tasks"))
