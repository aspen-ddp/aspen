package org.aspen_ddp.aspen.compute

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}


trait DurableTaskType {

  val typeUUID: UUID

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask

}
