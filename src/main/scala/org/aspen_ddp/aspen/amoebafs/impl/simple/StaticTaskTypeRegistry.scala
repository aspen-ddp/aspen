package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.util.UUID

import org.aspen_ddp.aspen.compute.DurableTaskFactory

object StaticTaskTypeRegistry {
  val registeredTasks: Map[UUID, DurableTaskFactory] = Map(
    CreateFileTask.typeUUID -> CreateFileTask,
    UnlinkFileTask.typeUUID -> UnlinkFileTask
  )
}
