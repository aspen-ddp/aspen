package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService
import org.aspen_ddp.aspen.compute.systemtask.SystemTaskExecutorService

object TypeFactories:

  val factories: List[RegisteredTypeFactory] = List(
    RebalancingDurableService,
    SystemTaskExecutorService
  )