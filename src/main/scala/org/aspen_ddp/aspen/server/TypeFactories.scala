package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.server.usage.UpdateAllocationGroupUsageTask

object TypeFactories:

  val factories: List[RegisteredTypeFactory] = List(
    UpdateAllocationGroupUsageTask
  )