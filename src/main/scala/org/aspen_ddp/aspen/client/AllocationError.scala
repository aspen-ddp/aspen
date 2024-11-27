package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AmoebaError
import org.aspen_ddp.aspen.common.pool.PoolId

case class AllocationError(poolId: PoolId) extends AmoebaError

