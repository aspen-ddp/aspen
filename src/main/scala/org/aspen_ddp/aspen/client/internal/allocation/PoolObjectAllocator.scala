package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

class PoolObjectAllocator(val client: AspenClient,
                          val pool: StoragePool) extends ObjectAllocator:

  given ExecutionContext = client.clientContext

  val allocatorId: ObjectAllocatorId = PoolObjectAllocatorId(pool.poolId)
  
  override def maxObjectSize: Option[Int] = pool.maxObjectSize

  override protected[client] def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] =
    pool.allocationStrategy.createDataObjectPointer(pool.poolId)

  override protected[client] def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] = 
    pool.allocationStrategy.createKeyValueObjectPointer(pool.poolId)
    

  

