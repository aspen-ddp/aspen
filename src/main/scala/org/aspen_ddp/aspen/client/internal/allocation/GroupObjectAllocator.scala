package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.internal.ObjectAllocatorManager
import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

class GroupObjectAllocator(val groupId: AllocationGroupId,
                           val manager: ObjectAllocatorManager) extends ObjectAllocator:

  given ExecutionContext = manager.client.clientContext

  val client: AspenClient = manager.client

  override def maxObjectSize: Option[Int] = manager.getState(groupId).maximumObjectSize

  override protected def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] = ???
  
  override protected def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] = ???
    

  