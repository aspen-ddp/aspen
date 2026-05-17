package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectPointer, Value}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class UUIDObjectRegistry(val client: AspenClient,
                         containingObjectPtr: KeyValueObjectPointer,
                         treeRootKey: Key):

  given ExecutionContext = client.clientContext

  private val registry = Registry(client, containingObjectPtr, treeRootKey)

  private def encodePointer(pointer: ObjectPointer): Value = Value(pointer.toArray)

  private def decodePointer(value: Value): ObjectPointer = ObjectPointer(value.bytes)

  def getRegisteredObject(objectId: UUID): Future[ObjectPointer] =
    registry.get(Key(objectId)).map(decodePointer)

  def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer] =
    getRegisteredObject(objectId).map(_.asInstanceOf[KeyValueObjectPointer])

  def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer] =
    getRegisteredObject(objectId).map(_.asInstanceOf[DataObjectPointer])

  def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    registry.prepareRegister(Key(objectId), encodePointer(pointer))

  def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit] =
    registry.register(Key(objectId), encodePointer(pointer))
