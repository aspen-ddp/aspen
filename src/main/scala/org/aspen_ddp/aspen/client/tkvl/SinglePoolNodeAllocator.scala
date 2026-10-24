package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, PoolObjectAllocatorId, StoragePool}
import org.aspen_ddp.aspen.common.pool.PoolId

// TODO - Remove this. It's a shim for AmoebaFS which needs proper configuration for its
//        trees
class SinglePoolNodeAllocator(client: AspenClient,
                              poolId: PoolId,
                              maxNodeSize: Int = SinglePoolNodeAllocator.DefaultMaxNodeSize)
  extends NodeAllocator(client, List((PoolObjectAllocatorId(poolId), maxNodeSize)))

object SinglePoolNodeAllocator:
  val DefaultMaxNodeSize: Int = 1 * 1024 * 1024

  /** A single tier in `pool` using `DefaultMaxNodeSize`, or the pool's node size limit when that is smaller */
  def forPool(client: AspenClient, pool: StoragePool): SinglePoolNodeAllocator =
    new SinglePoolNodeAllocator(client, pool.poolId,
      NodeAllocator.fittingNodeSize(DefaultMaxNodeSize, NodeAllocator.nodeSizeLimit(pool.ida, pool.maxSliceSize)))
