package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.allocation.AllocationFinalizationAction
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.*

import scala.concurrent.{ExecutionContext, Future}


trait ObjectAllocator:

  val client: AspenClient

  val allocatorId: ObjectAllocatorId

  /** Largest object, in bytes, that this allocator can place, or `None` for no limit. For a pool allocator it is
    * the pool's `maxObjectSize` (its effective `maxSliceSize`, which includes the backend's limit, converted through
    * the pool's IDA). For an allocation group it is the smallest such value among the pools reachable through the
    * group, recursing into child groups. It is computed from the client's cached group states, which are refreshed
    * in the background, so a pool added anywhere below the group is reflected within the cache refresh interval.
    */
  def maxObjectSize: Option[Int]

  protected[client] def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer]

  protected[client] def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer]

  def allocateDataObject()(using t: Transaction): Future[DataObjectPointer] =
    given ExecutionContext = client.clientContext
    createDataObjectPointer().map: ptr =>
      t.addAllocatingObject(ptr)
      AllocationFinalizationAction.addToTransaction(ptr, t)
      ptr

  def allocateKeyValueObject()(using t: Transaction): Future[KeyValueObjectPointer] =
    given ExecutionContext = client.clientContext
    createKeyValueObjectPointer().map: ptr =>
      t.addAllocatingObject(ptr)
      AllocationFinalizationAction.addToTransaction(ptr, t)
      ptr

  def allocateDataObject(initialContent: DataBuffer,
                         initialRefcount: ObjectRefcount = ObjectRefcount(0,1))(using t: Transaction): Future[DataObjectPointer] =
    given ExecutionContext = client.clientContext
    createDataObjectPointer().map: ptr =>
      stageDataObjectAllocation(ptr, initialContent, initialRefcount)
      ptr

  /** Adds the allocation of `ptr`, a pointer from `createDataObjectPointer`, with its initial content to the
    * transaction. Creating the pointer stages nothing, so together they let a caller size the content by the new
    * object's pointer before adding anything to the transaction.
    */
  protected[client] def stageDataObjectAllocation(ptr: DataObjectPointer,
                                                  initialContent: DataBuffer,
                                                  initialRefcount: ObjectRefcount = ObjectRefcount(0,1))
                                                 (using t: Transaction): Unit =
    t.addAllocatingObject(ptr)
    AllocationFinalizationAction.addToTransaction(ptr, t)
    t.overwrite(ptr, ObjectRevision.Allocating, initialContent)
    t.setRefcount(ptr, ObjectRefcount.Allocating, initialRefcount)

  def allocateKeyValueObject(initialContent: Map[Key,Value],
                             minimum: Option[Key] = None,
                             maximum: Option[Key] = None,
                             left: Option[Value] = None,
                             right: Option[Value] = None,
                             initialRefcount: ObjectRefcount = ObjectRefcount(0,1))(using t: Transaction): Future[KeyValueObjectPointer] =
    given ExecutionContext = client.clientContext
    allocateKeyValueObject().map: ptr =>

      var ops: List[KeyValueOperation] = initialContent.map((k, v) => Insert(k, v.bytes)).toList

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

