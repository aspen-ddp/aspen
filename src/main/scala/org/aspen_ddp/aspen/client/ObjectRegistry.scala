package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, Value}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object ObjectRegistry:
  class DuplicateRegistration(val objectId: UUID, val existing: ObjectPointer)
    extends Exception(s"Duplicate registration for object $objectId")

class ObjectRegistry(val client: AspenClient,
                     containingObjectPtr: KeyValueObjectPointer,
                     treeRootKey: Key):

  given ExecutionContext = client.clientContext

  private val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, treeRootKey, containingObjectPtr))

  def getRegisteredObject(objectId: UUID): Future[ObjectPointer] =
    tkvl.get(Key(objectId)).map:
      case None => throw new NoSuchElementException(objectId.toString)
      case Some(vs) => ObjectPointer(vs.value.bytes)

  def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer] =
    getRegisteredObject(objectId).map(_.asInstanceOf[KeyValueObjectPointer])

  def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer] =
    getRegisteredObject(objectId).map(_.asInstanceOf[DataObjectPointer])
