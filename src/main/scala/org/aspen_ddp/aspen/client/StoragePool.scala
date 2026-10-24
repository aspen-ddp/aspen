package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.pool.PoolId

import java.util.UUID
import scala.concurrent.Future

trait StoragePool:

  val poolId: PoolId

  val ida: IDA

  /** Largest slice, in bytes, that any store of this pool holds for a single object, or `None` for no limit.
    * Fixed when the pool is created. Most code should use `maxObjectSize` instead, which expresses the same
    * limit in terms of object size.
    */
  val maxSliceSize: Option[Int]

  /** Largest object, in bytes, that this pool accepts, or `None` for no limit. Derived from `maxSliceSize` and the
    * pool's IDA via `ida.maxObjectSize`: an object of exactly this size encodes to slices no larger than
    * `maxSliceSize`, and one byte more would not.
    */
  def maxObjectSize: Option[Int] = maxSliceSize.map(ida.maxObjectSize(_))

  def allocator: ObjectAllocator

  def getState(): Future[StoragePoolState]

  def dropCachedState(): Unit

  private[aspen] def allocationTree: TieredKeyValueList

  private[aspen] def errorTree: TieredKeyValueList
  
  private[aspen] def allocationStrategy: Option[UUID] = None
  
  private[aspen] def allocationStrategyConfig: Option[Array[Byte]] = None
