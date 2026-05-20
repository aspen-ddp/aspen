package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, StoragePool, Transaction}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SinglePoolObjectAllocator(val client: AspenClient,
                                val pool: StoragePool,
                                val objectIDA: IDA,
                                val maxObjectSize: Option[Int]) extends ObjectAllocator:

  val executionContext: ExecutionContext = client.clientContext

  override protected def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] =
    Future.successful(new DataObjectPointer(ObjectId(UUID.randomUUID()), pool.poolId, Array.empty))

  override protected def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] =
    Future.successful(new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), pool.poolId, Array.empty))

  

