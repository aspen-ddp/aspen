package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, StoragePool, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, DataObjectPointer, Key, KeyValueObjectPointer, ObjectRefcount, Value}

import scala.concurrent.Future

class SinglePoolObjectAllocator(val client: AspenClient,
                                val pool: StoragePool,
                                val objectIDA: IDA,
                                val maxObjectSize: Option[Int]) extends ObjectAllocator {


  override def allocateDataObject(revisionGuard: AllocationRevisionGuard,
                                  initialContent: DataBuffer,
                                  initialRefcount: ObjectRefcount)(using t: Transaction): Future[DataObjectPointer] = {
    client.allocationManager.allocateDataObject(client, t, pool, maxObjectSize, objectIDA, initialRefcount,
      revisionGuard, initialContent)
  }

  override def allocateKeyValueObject(revisionGuard: AllocationRevisionGuard,
                                      initialContent: Map[Key, Value],
                                      minimum: Option[Key],
                                      maximum: Option[Key],
                                      left: Option[Value],
                                      right: Option[Value],
                                      initialRefcount: ObjectRefcount)(using t: Transaction): Future[KeyValueObjectPointer] = {
    client.allocationManager.allocateKeyValueObject(client, t, pool, maxObjectSize, objectIDA, initialRefcount,
      revisionGuard, initialContent, minimum, maximum, left, right)
  }
}
