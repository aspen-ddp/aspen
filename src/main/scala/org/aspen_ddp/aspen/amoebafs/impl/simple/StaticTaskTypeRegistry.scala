package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.util.UUID

import org.aspen_ddp.aspen.compute.DurableTaskType

object StaticTaskTypeRegistry {
  val registeredTasks: Map[UUID, DurableTaskType] = Map(
    CreateFileTask.typeUUID -> CreateFileTask,
    UnlinkFileTask.typeUUID -> UnlinkFileTask
  )
}
