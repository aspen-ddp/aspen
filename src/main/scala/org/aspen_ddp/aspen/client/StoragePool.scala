package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.pool.PoolId

trait StoragePool:

  val poolId: PoolId

  val ida: IDA

  /** Largest slice, in bytes, that any store of this pool accepts for a single object, or `None` for no limit.
    * This is the pool's effective limit (`StoragePoolState.effectiveMaxSliceSize`): the smaller of the slice limit
    * declared for the pool and the one configured for its backend. Fixed when the pool is created. Most code should
    * use `maxObjectSize` instead, which expresses the same limit in terms of object size.
    */
  val maxSliceSize: Option[Int]

  /** Largest object, in bytes, that this pool accepts, or `None` for no limit. Derived from `maxSliceSize` (the
    * effective limit, including the backend's) and the pool's IDA via `ida.maxObjectSize`: an object of exactly this
    * size encodes to slices no larger than `maxSliceSize`, and one byte more would not. See
    * `StoragePoolState.maxObjectSize`.
    */
  val maxObjectSize: Option[Int]

  def allocator: ObjectAllocator

  private[aspen] def allocationTree: TieredKeyValueList

  private[aspen] def errorTree: TieredKeyValueList
  
  /** Supplies ObjectPointers for objects allocated in this pool and is told of their deletion.
    * Created on first use and cached.
    */
  private[aspen] def allocationStrategy: StoragePoolAllocationStrategy = StoragePoolAllocationStrategy.Default
