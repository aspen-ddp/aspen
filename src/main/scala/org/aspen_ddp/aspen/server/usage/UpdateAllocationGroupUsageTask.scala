package org.aspen_ddp.aspen.server.usage

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.util.{uuid2byte, byte2uuid, long2byte, byte2long}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, DurableTaskFactory, TaskExecutor}

import scala.concurrent.{Future, Promise}

object UpdateAllocationGroupUsageTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("7A3F9B2E-1D4C-4E8A-B5F7-6C9D0E2A8B1F")

  private val TimestampKey = Key(1)
  private val CurrentUsageKey = Key(2)
  private val MaxStoreSizeKey = Key(3)
  private val AllocationGroupCountKey = Key(4)
  private val AllocationGroupBaseKey = 5

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask =
    new UpdateAllocationGroupUsageTask(pointer)

  def prepareTask(timestamp: HLCTimestamp,
                  currentUsage: Long,
                  maximumStoreSize: Long,
                  allocationGroups: List[UUID],
                  taskExecutor: TaskExecutor)
                 (using tx: Transaction): Future[Future[Option[AnyRef]]] =
    val istate = List(
      TimestampKey -> long2byte(timestamp.asLong),
      CurrentUsageKey -> long2byte(currentUsage),
      MaxStoreSizeKey -> long2byte(maximumStoreSize),
      AllocationGroupCountKey -> long2byte(allocationGroups.size.toLong)
    ) ++ allocationGroups.zipWithIndex.map: (uuid, idx) =>
      Key(AllocationGroupBaseKey + idx) -> uuid2byte(uuid)

    taskExecutor.prepareTask(UpdateAllocationGroupUsageTask, istate)


class UpdateAllocationGroupUsageTask(
  val taskPointer: DurableTaskPointer
) extends DurableTask:
  private val promise = Promise[Option[AnyRef]]()
  promise.success(None)

  def completed: Future[Option[AnyRef]] = promise.future
