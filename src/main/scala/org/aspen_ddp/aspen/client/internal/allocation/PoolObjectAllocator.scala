package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, ObjectAllocatorId, PoolObjectAllocatorId, StoragePool, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class PoolObjectAllocator(val client: AspenClient,
                          val pool: StoragePool) extends ObjectAllocator:

  given ExecutionContext = client.clientContext

  val allocatorId: ObjectAllocatorId = PoolObjectAllocatorId(pool.poolId)

  override def maxObjectSize: Option[Int] = pool.maxObjectSize

  override protected[client] def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] =
    Future.successful(new DataObjectPointer(ObjectId(UUID.randomUUID()), pool.poolId, Array.empty))

  override protected[client] def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] =
    Future.successful(new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), pool.poolId, Array.empty))

  

