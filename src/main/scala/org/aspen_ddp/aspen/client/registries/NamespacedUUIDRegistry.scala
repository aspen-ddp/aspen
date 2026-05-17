package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object NamespacedUUIDRegistry:
  def makeKey(namespace: String, name: String): Key = Key(s"$namespace.$name")

class NamespacedUUIDRegistry(val client: AspenClient,
                             containingObjectPtr: KeyValueObjectPointer,
                             treeRootKey: Key):

  import NamespacedUUIDRegistry.makeKey

  given ExecutionContext = client.clientContext

  private val registry = Registry(client, containingObjectPtr, treeRootKey)

  private def encodeUUID(uuid: UUID): Value =
    val arr = new Array[Byte](16)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    Value(arr)

  private def decodeUUID(value: Value): UUID =
    val bb = ByteBuffer.wrap(value.bytes)
    new UUID(bb.getLong(), bb.getLong())

  def getRegisteredObject(namespace: String, name: String): Future[UUID] =
    registry.get(makeKey(namespace, name)).map(decodeUUID)

  def prepareRegisterObject(namespace: String, name: String, objectId: UUID)(using tx: Transaction): Future[Unit] =
    registry.prepareRegister(makeKey(namespace, name), encodeUUID(objectId))

  def registerObject(namespace: String, name: String, objectId: UUID): Future[Unit] =
    registry.register(makeKey(namespace, name), encodeUUID(objectId))
