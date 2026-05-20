package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, KeyValueOperation, ObjectRefcount, ObjectRevision, SetLeft, SetMax, SetMin, SetRight, Value}

import scala.concurrent.{ExecutionContext, Future}

trait ObjectAllocator:

  val maxObjectSize: Option[Int]

  val objectIDA: IDA

  protected val executionContext: ExecutionContext

  def allocateDataObject()(using t: Transaction): Future[DataObjectPointer]

  def allocateKeyValueObject()(using t: Transaction): Future[KeyValueObjectPointer]

  def allocateDataObject(initialContent: DataBuffer,
                         initialRefcount: ObjectRefcount = ObjectRefcount(0,1))(using t: Transaction): Future[DataObjectPointer] =
    given ExecutionContext = executionContext
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
    given ExecutionContext = executionContext
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

