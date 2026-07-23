package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object NamespacedUUIDRegistry:
  def makeKey(namespace: String, name: String): Key = Key(s"$namespace.$name")
  def encodeUUID(uuid: UUID): Value = Value(uuid2byte(uuid))
  def decodeUUID(value: Value): UUID = byte2uuid(value.bytes)

class NamespacedUUIDRegistry(val client: AspenClient,
                             containingObjectPtr: KeyValueObjectPointer,
                             treeRootKey: Key):

  import NamespacedUUIDRegistry.{makeKey, encodeUUID, decodeUUID}

  given ExecutionContext = client.clientContext

  private val registry = Registry(client, containingObjectPtr, treeRootKey)

  def getRegisteredObject(namespace: String, name: String): Future[UUID] =
    registry.get(makeKey(namespace, name)).map(decodeUUID)

  def prepareRegisterObject(namespace: String, name: String, objectId: UUID)(using tx: Transaction): Future[Unit] =
    registry.prepareRegister(makeKey(namespace, name), encodeUUID(objectId))

  def registerObject(namespace: String, name: String, objectId: UUID): Future[Unit] =
    registry.register(makeKey(namespace, name), encodeUUID(objectId))

  def getAllEntries(namespace: String): Future[List[(String, UUID)]] =
    val fullPrefix = s"$namespace."
    registry.scan(namespace).map(_.flatMap { (key, value) =>
      val keyStr = new String(key.bytes, StandardCharsets.UTF_8)
      if keyStr.startsWith(fullPrefix) then
        Some(keyStr.substring(fullPrefix.length) -> decodeUUID(value))
      else
        None
    })
