package org.aspen_ddp.aspen.compute

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, RegisteredTypeFactory}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}


trait DurableTaskFactory extends RegisteredTypeFactory:

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask

