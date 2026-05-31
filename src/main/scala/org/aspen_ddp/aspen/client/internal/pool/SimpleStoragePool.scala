package org.aspen_ddp.aspen.client.internal.pool

import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ObjectAllocator, StoragePool}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.byte2uuid

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

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
  val maxObjectSize: Option[Int] = initialState.maxObjectSize
  
  val allocationTree: TieredKeyValueList = new TieredKeyValueList(client,
    new KVObjectRootManager(client, StoragePoolState.AllocationTreeKey, kvos.pointer))
  
  val errorTree: TieredKeyValueList = new TieredKeyValueList(client,
    new KVObjectRootManager(client, StoragePoolState.ErrorTreeKey, kvos.pointer))
  
  private val allocStrategy: Option[UUID] = kvos.contents.get(StoragePoolState.AllocationStrategyKey).map: vs =>
    byte2uuid(vs.value.bytes)
    
  private val allocStrategyConfig: Option[Array[Byte]] = kvos.contents.get(StoragePoolState.AllocationStrategyConfigKey).map(_.value.bytes)
  
  private var cachedState: Option[StoragePoolState] = Some(initialState)

  def getState(): Future[StoragePoolState] =
    synchronized:
      cachedState match 
        case Some(state) => Future.successful(state)
        case None =>
          client.read(pointer).map: kvos =>
            val state = StoragePoolState(kvos)
            synchronized:
              cachedState = Some(state)
            state
  
  def dropCachedState(): Unit =
    synchronized:
      cachedState = None

  override def createAllocator: ObjectAllocator = new PoolObjectAllocator(client,
    this, maxObjectSize)

  override private[aspen] def allocationStrategy: Option[UUID] = allocStrategy

  override private[aspen] def allocationStrategyConfig: Option[Array[Byte]] = allocStrategyConfig
