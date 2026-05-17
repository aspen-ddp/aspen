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

  def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    tkvl.set(Key(objectId), Value(pointer.toArray), requirement = Some(Left(true))).map: _ =>
      tx.result.value match
        case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(Key(objectId))
        case _ => ()

  def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit] =
    val key = Key(objectId)
    client.retryStrategy.retryUntilSuccessful:
      tkvl.get(key).flatMap:
        case Some(vs) =>
          if ObjectPointer(vs.value.bytes) == pointer then
            Future.unit
          else
            throw StopRetrying(ObjectRegistry.DuplicateRegistration(objectId, ObjectPointer(vs.value.bytes)))
        case None =>
          client.transact: tx =>
            given Transaction = tx
            prepareRegisterObject(objectId, pointer)
