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

  val maxObjectSize: Option[Int]

  def allocator: ObjectAllocator

  def getState(): Future[StoragePoolState]

  def dropCachedState(): Unit

  private[aspen] def allocationTree: TieredKeyValueList

  private[aspen] def errorTree: TieredKeyValueList
  
  private[aspen] def allocationStrategy: Option[UUID] = None
  
  private[aspen] def allocationStrategyConfig: Option[Array[Byte]] = None
