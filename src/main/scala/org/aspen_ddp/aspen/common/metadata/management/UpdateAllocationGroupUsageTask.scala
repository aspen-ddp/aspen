package org.aspen_ddp.aspen.common.metadata.management

import scribe.Logging
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ReadError, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.{byte2long, byte2uuid, byte2uuids, long2byte, uuid2byte, uuids2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.{Failure, Success}

object UpdateAllocationGroupUsageTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("7A3F9B2E-1D4C-4E8A-B5F7-6C9D0E2A8B1F")

  private[management] val ChildUUIDKey = Key(1)
  private[management] val AllocationGroupsKey = Key(4)
  private[management] val NextIndexKey = Key(5)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val childUUID = byte2uuid(state(ChildUUIDKey).value.bytes)
    val allocationGroups = byte2uuids(state(AllocationGroupsKey).value.bytes)

    new UpdateAllocationGroupUsageTask(pointer, client, childUUID, allocationGroups, taskExecutor)

  /** Enrolls a task that records the child's usage in each of `allocationGroups`. The task carries no usage: it reads
    * the child's state when it updates each group, so the transaction that changed that state should enroll it */
  def prepareTask(childUUID: UUID,
                  allocationGroups: List[UUID],
                  taskExecutor: TaskExecutor)
                 (using tx: Transaction): Future[Future[Option[AnyRef]]] =
    require(allocationGroups.nonEmpty)
    taskExecutor.prepareTask(UpdateAllocationGroupUsageTask, initialState(childUUID, allocationGroups).toList)

  def prepareSystemTask(client: AspenClient,
                        childUUID: UUID,
                        allocationGroups: List[UUID])
                       (using tx: Transaction): Future[Unit] =
    require(allocationGroups.nonEmpty)
    client.prepareSystemDurableTask(typeUUID, initialState(childUUID, allocationGroups))

  private def initialState(childUUID: UUID, allocationGroups: List[UUID]): Map[Key, Array[Byte]] = Map(
    ChildUUIDKey        -> uuid2byte(childUUID),
    AllocationGroupsKey -> uuids2byte(allocationGroups),
    NextIndexKey        -> long2byte(0))


class UpdateAllocationGroupUsageTask(
  val taskPointer: DurableTaskPointer,
  client: AspenClient,
  childUUID: UUID,
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

  /** The child's member entry as it should be now, from the child's own state */
  private def readCurrentMember(member: AllocationGroupState.Member): Future[AllocationGroupState.Member] =
    member.memberType match
      case AllocationGroupState.MemberType.Pool =>
        for
          ptr <- client.getStoragePoolPointer(PoolId(childUUID))
          kvos <- client.read(ptr)
        yield AllocationGroupState.Member.forPool(StoragePoolState(kvos))
      case AllocationGroupState.MemberType.Group =>
        for
          ptr <- client.getAllocationGroupPointer(AllocationGroupId(childUUID))
          dos <- client.read(ptr)
        yield AllocationGroupState.Member.forGroup(AllocationGroupState(dos))

  private def updateNextGroup(): Unit =
    if isStopped then
      failTask(new TaskStopped)
      return

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

          // The group is read before the child. A child change after the child read enrolls another task, and
          // that task's commit comes after this group read, so this task's overwrite collides with it and retries
          // instead of putting back older usage
          val fFetch = for
            agsPtr <- client.getAllocationGroupPointer(groupId)
            agsDos <- client.read(agsPtr)
            ags = AllocationGroupState(agsDos)
            current <- ags.members.find(_.uuid == childUUID) match
              case None => Future.successful(None)
              case Some(member) => readCurrentMember(member).map(Some(_))
          yield (agsPtr, agsDos, ags, current)

          fFetch.onComplete:
            case Failure(err) => err match
              case _: NoSuchElementException | _: ReadError =>
                advanceIndex(kvos)
              case e: StopRetrying =>
                failTask(e)
              case _ =>
                scheduleRetry()

            case Success((agsPtr, agsDos, ags, current)) =>
              current match
                case None =>
                  advanceIndex(kvos)

                // Already recorded, typically by a concurrent task for the same change: nothing to write
                case Some(member) if ags.members.contains(member) =>
                  advanceIndex(kvos)

                case Some(member) =>
                  val oldGroupUsage = ags.currentUsage
                  val oldGroupMaxSize = ags.maximumSize

                  val updatedMembers = ags.members.map: m =>
                    if m.uuid == childUUID then member else m

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
                        groupId.uuid, nags.parentGroups.map(_.uuid), taskExecutor).map(_ => ())
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
