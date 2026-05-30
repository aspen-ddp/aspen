package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.util.{long2byte, uuid2byte, uuids2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor}

import java.util.UUID
import scala.concurrent.{Future, Promise}

object UpdateAllocationGroupUsageTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("7A3F9B2E-1D4C-4E8A-B5F7-6C9D0E2A8B1F")

  private val ChildUUIDKey = Key(1)
  private val CurrentUsageKey = Key(2)
  private val MaxSizeKey = Key(3)
  private val AllocationGroupsKey = Key(4)
  private val NextIndexKey = Key(5)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask =
    new UpdateAllocationGroupUsageTask(pointer)

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
  val taskPointer: DurableTaskPointer
) extends DurableTask:
  private val promise = Promise[Option[AnyRef]]()
  promise.success(None)

  def completed: Future[Option[AnyRef]] = promise.future
