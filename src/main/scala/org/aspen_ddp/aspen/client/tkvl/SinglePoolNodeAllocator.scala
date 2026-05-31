package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, PoolObjectAllocatorId}
import org.aspen_ddp.aspen.common.pool.PoolId

// TODO - Remove this. It's a shim for AmoebaFS which needs proper configuration for its
//        trees
class SinglePoolNodeAllocator(client: AspenClient, poolId: PoolId) extends NodeAllocator(client,
  List((PoolObjectAllocatorId(poolId), 1 * 1024 * 1024)))