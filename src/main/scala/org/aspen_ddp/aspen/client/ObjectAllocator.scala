package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.allocation.AllocationFinalizationAction
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.*

import scala.concurrent.{ExecutionContext, Future}


trait ObjectAllocator:

  val client: AspenClient

  val maxObjectSize: Option[Int]

  protected def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer]

  protected def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer]

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
    allocateDataObject().map: ptr =>
      t.overwrite(ptr, ObjectRevision.Allocating, initialContent)
      t.setRefcount(ptr, ObjectRefcount.Allocating, initialRefcount)
      ptr

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

