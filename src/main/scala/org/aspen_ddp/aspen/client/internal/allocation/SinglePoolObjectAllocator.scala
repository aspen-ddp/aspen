package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, StoragePool, Transaction}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, DataObjectPointer, Insert, Key, KeyValueObjectPointer, KeyValueOperation, ObjectId, ObjectRefcount, ObjectRevision, SetLeft, SetMax, SetMin, SetRight, Value}
import org.aspen_ddp.aspen.server.store.ValueState

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SinglePoolObjectAllocator(val client: AspenClient,
                                val pool: StoragePool,
                                val objectIDA: IDA,
                                val maxObjectSize: Option[Int]) extends ObjectAllocator:

  given ExecutionContext = client.clientContext

  override def allocateDataObject()(using t: Transaction): Future[DataObjectPointer] =
    val ptr = new DataObjectPointer(ObjectId(UUID.randomUUID()), pool.poolId, Array.empty)
    t.addAllocatingObject(ptr)
    AllocationFinalizationAction.addToTransaction(ptr, t)
    Future.successful(ptr)

  override def allocateKeyValueObject()(using t: Transaction): Future[KeyValueObjectPointer] =
    val ptr = new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), pool.poolId, Array.empty)
    t.addAllocatingObject(ptr)
    AllocationFinalizationAction.addToTransaction(ptr, t)
    Future.successful(ptr)

  override def allocateDataObject(revisionGuard: AllocationRevisionGuard,
                                  initialContent: DataBuffer,
                                  initialRefcount: ObjectRefcount)(using t: Transaction): Future[DataObjectPointer] =
    allocateDataObject().map: ptr =>
      t.overwrite(ptr, ObjectRevision.Allocating, initialContent)
      t.setRefcount(ptr, ObjectRefcount.Allocating, initialRefcount)
      ptr

  override def allocateKeyValueObject(revisionGuard: AllocationRevisionGuard,
                                      initialContent: Map[Key, Value],
                                      minimum: Option[Key],
                                      maximum: Option[Key],
                                      left: Option[Value],
                                      right: Option[Value],
                                      initialRefcount: ObjectRefcount)(using t: Transaction): Future[KeyValueObjectPointer] =
    allocateKeyValueObject().map: ptr =>

      var ops: List[KeyValueOperation] = initialContent.map((k,v) => Insert(k, v.bytes)).toList

      minimum.foreach: min =>
        ops = SetMin(min) :: ops
      maximum.foreach: max =>
        ops = SetMax(max) :: ops
      left.foreach: left =>
        ops = SetLeft(left.bytes) :: ops
      right.foreach: right =>
        ops = SetRight(right.bytes) :: ops

      t.update(ptr, None, None, Nil, ops)
      t.setRefcount(ptr, ObjectRefcount.Allocating, initialRefcount)

      ptr

