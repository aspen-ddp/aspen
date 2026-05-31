package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

class PoolObjectAllocator(val client: AspenClient,
                          val pool: StoragePool) extends ObjectAllocator:

  given ExecutionContext = client.clientContext

  val allocatorId: ObjectAllocatorId = PoolObjectAllocatorId(pool.poolId)
  
  private val allocationStrategy = pool.allocationStrategy match 
    case None => StoragePoolAllocationStrategy.Default
    case Some(uuid) => 
      val factory = client.typeRegistry.getType(uuid).asInstanceOf[StoragePoolAllocationStrategy.Factory]
      factory.createStrategy(client, pool, pool.allocationStrategyConfig)
  
  override def maxObjectSize: Option[Int] = pool.maxObjectSize

  override protected[client] def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] =
    allocationStrategy.createDataObjectPointer(pool.poolId)

  override protected[client] def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] = 
    allocationStrategy.createKeyValueObjectPointer(pool.poolId)
    

  

