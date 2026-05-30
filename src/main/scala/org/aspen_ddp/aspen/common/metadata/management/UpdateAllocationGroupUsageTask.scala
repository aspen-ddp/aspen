package org.aspen_ddp.aspen.common.metadata.management

import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ReadError, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.{byte2long, byte2uuid, byte2uuids, long2byte, uuid2byte, uuids2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.{Failure, Success}

object UpdateAllocationGroupUsageTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("7A3F9B2E-1D4C-4E8A-B5F7-6C9D0E2A8B1F")

  private[management] val ChildUUIDKey = Key(1)
  private[management] val CurrentUsageKey = Key(2)
  private[management] val MaxSizeKey = Key(3)
  private[management] val AllocationGroupsKey = Key(4)
  private[management] val NextIndexKey = Key(5)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val childUUID = byte2uuid(state(ChildUUIDKey).value.bytes)
    val currentUsage = byte2long(state(CurrentUsageKey).value.bytes)
    val maximumSize = byte2long(state(MaxSizeKey).value.bytes)
    val allocationGroups = byte2uuids(state(AllocationGroupsKey).value.bytes)

    new UpdateAllocationGroupUsageTask(pointer, client, childUUID, currentUsage,
      maximumSize, allocationGroups, taskExecutor)

  def prepareTask(childUUID: UUID,
                  currentUsage: Long,
                  maximumStoreSize: Long,
                  allocationGroups: List[UUID],
                  taskExecutor: TaskExecutor)
                 (using tx: Transaction): Future[Future[Option[AnyRef]]] =
    require(allocationGroups.nonEmpty)

    val istate = List(
      ChildUUIDKey -> uuid2byte(childUUID),
      CurrentUsageKey -> long2byte(currentUsage),
      MaxSizeKey -> long2byte(maximumStoreSize),
      AllocationGroupsKey -> uuids2byte(allocationGroups),
      NextIndexKey -> long2byte(0)
    )

    taskExecutor.prepareTask(UpdateAllocationGroupUsageTask, istate)


class UpdateAllocationGroupUsageTask(
  val taskPointer: DurableTaskPointer,
  client: AspenClient,
  childUUID: UUID,
  currentUsage: Long,
  maximumSize: Long,
  allocationGroups: List[UUID],
  taskExecutor: TaskExecutor
) extends DurableTask with Logging:

  import UpdateAllocationGroupUsageTask.NextIndexKey

  given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  private var retryDelay: Int = 16

  def completed: Future[Option[AnyRef]] = promise.future

  updateNextGroup()

  private def exceedsThreshold(newValue: Long, oldValue: Long): Boolean =
    math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005

  private def completeTask(): Unit = synchronized:
    if !promise.isCompleted then
      promise.success(None)

  private def failTask(err: Throwable): Unit = synchronized:
    if !promise.isCompleted then
      promise.failure(err)

  private def scheduleRetry(): Unit =
    val delay = ThreadLocalRandom.current().nextInt(retryDelay)
    retryDelay = Math.min(retryDelay * 2, 60000)
    client.backgroundTaskManager.schedule(Duration(delay, MILLISECONDS)):
      updateNextGroup()

  private def advanceIndex(kvos: KeyValueObjectState): Unit =
    val vs = kvos.contents(NextIndexKey)
    val nextIndex = byte2long(vs.value.bytes)

    val tx = client.newTransaction()
    val requirements = KeyValueUpdate.KeyRevision(NextIndexKey, vs.revision) :: Nil
    val operations = Insert(NextIndexKey, long2byte(nextIndex + 1)) :: Nil
    tx.update(taskPointer.kvPointer, None, None, requirements, operations)

    tx.commit().onComplete:
      case Failure(_) => scheduleRetry()
      case Success(_) =>
        retryDelay = 16
        updateNextGroup()

  private def updateNextGroup(): Unit =
    client.read(taskPointer.kvPointer).onComplete:
      case Failure(err) => scheduleRetry()
      case Success(kvos) =>
        val vs = kvos.contents(NextIndexKey)
        val nextIndex = byte2long(vs.value.bytes).toInt

        if nextIndex >= allocationGroups.length then
          completeTask()
        else
          val groupUUID = allocationGroups(nextIndex)
          val groupId = AllocationGroupId(groupUUID)

          val fFetch = for
            agsPtr <- client.getAllocationGroupPointer(groupId)
            agsDos <- client.read(agsPtr)
          yield (agsPtr, agsDos)

          fFetch.onComplete:
            case Failure(err) => err match
              case _: NoSuchElementException | _: ReadError =>
                advanceIndex(kvos)
              case e: StopRetrying =>
                failTask(e)
              case _ =>
                scheduleRetry()

            case Success((agsPtr, agsDos)) =>
              val ags = AllocationGroupState(agsDos)

              ags.members.find(_.uuid == childUUID) match
                case None =>
                  advanceIndex(kvos)

                case Some(member) =>
                  val oldGroupUsage = ags.currentUsage
                  val oldGroupMaxSize = ags.maximumSize

                  val updatedMembers = ags.members.map: m =>
                    if m.uuid == childUUID then
                      m.copy(currentUsage = currentUsage, maximumSize = maximumSize)
                    else m

                  val nags = ags.copy(members = updatedMembers)
                  val newGroupUsage = nags.currentUsage
                  val newGroupMaxSize = nags.maximumSize

                  val needsCascade = nags.parentGroups.nonEmpty &&
                    (exceedsThreshold(newGroupUsage, oldGroupUsage) ||
                      exceedsThreshold(newGroupMaxSize, oldGroupMaxSize))

                  val tx = client.newTransaction()

                  tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

                  val requirements = KeyValueUpdate.KeyRevision(NextIndexKey, vs.revision) :: Nil
                  val operations = Insert(NextIndexKey, long2byte(nextIndex + 1)) :: Nil
                  tx.update(taskPointer.kvPointer, None, None, requirements, operations)

                  given Transaction = tx

                  val fCascade =
                    if needsCascade then
                      UpdateAllocationGroupUsageTask.prepareTask(
                        groupId.uuid, newGroupUsage, newGroupMaxSize,
                        nags.parentGroups.map(_.uuid), taskExecutor
                      ).map(_ => ())
                    else
                      Future.unit

                  fCascade.flatMap: _ =>
                    tx.commit()
                  .onComplete:
                    case Success(_) =>
                      retryDelay = 16
                      updateNextGroup()
                    case Failure(_) =>
                      scheduleRetry()
