package org.aspen_ddp.aspen.common

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.common.metadata.management.{MigratePoolToSetDurableTask, UpdateAllocationGroupUsageTask}

object TypeFactories:

  val factories: List[RegisteredTypeFactory] = List(
    UpdateAllocationGroupUsageTask,
    MigratePoolToSetDurableTask
  )
