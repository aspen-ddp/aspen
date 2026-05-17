package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object ObjectRegistry:
  class DuplicateRegistration(val key: Key, val existing: ObjectPointer)
    extends Exception(s"Duplicate registration for key $key")

class ObjectRegistry(val client: AspenClient,
                     containingObjectPtr: KeyValueObjectPointer,
                     treeRootKey: Key):

  given ExecutionContext = client.clientContext

  private val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, treeRootKey, containingObjectPtr))

  def getRegisteredObject(key: Key): Future[ObjectPointer] =
    tkvl.get(key).map:
      case None => throw new NoSuchElementException(key.toString)
      case Some(vs) => ObjectPointer(vs.value.bytes)

  def getRegisteredKeyValueObject(key: Key): Future[KeyValueObjectPointer] =
    getRegisteredObject(key).map(_.asInstanceOf[KeyValueObjectPointer])

  def getRegisteredDataObject(key: Key): Future[DataObjectPointer] =
    getRegisteredObject(key).map(_.asInstanceOf[DataObjectPointer])

  def prepareRegisterObject(key: Key, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    tkvl.set(key, Value(pointer.toArray), requirement = Some(Left(true))).map: _ =>
      tx.result.value match
        case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(key)
        case _ => ()

  def registerObject(key: Key, pointer: ObjectPointer): Future[Unit] =
    client.retryStrategy.retryUntilSuccessful:
      tkvl.get(key).flatMap:
        case Some(vs) =>
          if ObjectPointer(vs.value.bytes) == pointer then
            Future.unit
          else
            throw StopRetrying(ObjectRegistry.DuplicateRegistration(key, ObjectPointer(vs.value.bytes)))
        case None =>
          client.transact: tx =>
            given Transaction = tx
            prepareRegisterObject(key, pointer)
