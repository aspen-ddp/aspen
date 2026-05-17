package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.Future

object NamespacedObjectRegistry:
  def makeKey(namespace: String, name: String): Key = Key(s"$namespace.$name")

class NamespacedObjectRegistry(val client: AspenClient,
                               containingObjectPtr: KeyValueObjectPointer,
                               treeRootKey: Key):

  import NamespacedObjectRegistry.makeKey

  private val registry = ObjectRegistry(client, containingObjectPtr, treeRootKey)

  def getRegisteredObject(namespace: String, name: String): Future[ObjectPointer] =
    registry.getRegisteredObject(makeKey(namespace, name))

  def getRegisteredKeyValueObject(namespace: String, name: String): Future[KeyValueObjectPointer] =
    registry.getRegisteredKeyValueObject(makeKey(namespace, name))

  def getRegisteredDataObject(namespace: String, name: String): Future[DataObjectPointer] =
    registry.getRegisteredDataObject(makeKey(namespace, name))

  def prepareRegisterObject(namespace: String, name: String, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    registry.prepareRegisterObject(makeKey(namespace, name), pointer)

  def registerObject(namespace: String, name: String, pointer: ObjectPointer): Future[Unit] =
    registry.registerObject(makeKey(namespace, name), pointer)
