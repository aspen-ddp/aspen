package org.aspen_ddp.aspen.client.internal.pool

import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ObjectAllocator, PoolObjectAllocatorId, StoragePool}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.byte2uuid

import java.util.UUID
import scala.concurrent.ExecutionContext

object SimpleStoragePool:

  def apply(client: AspenClient, kvos: KeyValueObjectState): SimpleStoragePool =
    new SimpleStoragePool(client, kvos, StoragePoolState(kvos))


class SimpleStoragePool(val client: AspenClient,
                        kvos: KeyValueObjectState,
                        initialState: StoragePoolState) extends StoragePool:

  given ExecutionContext = client.clientContext

  val pointer: KeyValueObjectPointer = kvos.pointer
  
  val poolId: PoolId = initialState.poolId
  val ida: IDA = initialState.ida
  val maxSliceSize: Option[Int] = initialState.effectiveMaxSliceSize
  val maxObjectSize: Option[Int] = initialState.maxObjectSize
  
  val allocationTree: TieredKeyValueList = new TieredKeyValueList(client,
    new KVObjectRootManager(client, StoragePoolState.AllocationTreeKey, kvos.pointer))
  
  val errorTree: TieredKeyValueList = new TieredKeyValueList(client,
    new KVObjectRootManager(client, StoragePoolState.ErrorTreeKey, kvos.pointer))
  
  private val allocStrategy: Option[UUID] = kvos.contents.get(StoragePoolState.AllocationStrategyKey).map: vs =>
    byte2uuid(vs.value.bytes)
    
  private val allocStrategyConfig: Option[Array[Byte]] = kvos.contents.get(StoragePoolState.AllocationStrategyConfigKey).map(_.value.bytes)
  
  override def allocator: ObjectAllocator =
    client.getCachedAllocator(PoolObjectAllocatorId(poolId)) match
      case Some(allocator) => allocator
      case None =>
        val allocator = new PoolObjectAllocator(client, this)
        client.cacheAllocator(allocator)
        allocator

  override private[aspen] def allocationStrategy: Option[UUID] = allocStrategy

  override private[aspen] def allocationStrategyConfig: Option[Array[Byte]] = allocStrategyConfig
