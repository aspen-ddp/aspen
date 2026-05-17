package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectPointer}

import java.util.UUID
import scala.concurrent.Future

class UUIDObjectRegistry(val client: AspenClient,
                         containingObjectPtr: KeyValueObjectPointer,
                         treeRootKey: Key):

  private val registry = ObjectRegistry(client, containingObjectPtr, treeRootKey)

  def getRegisteredObject(objectId: UUID): Future[ObjectPointer] =
    registry.getRegisteredObject(Key(objectId))

  def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer] =
    registry.getRegisteredKeyValueObject(Key(objectId))

  def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer] =
    registry.getRegisteredDataObject(Key(objectId))

  def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    registry.prepareRegisterObject(Key(objectId), pointer)

  def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit] =
    registry.registerObject(Key(objectId), pointer)
