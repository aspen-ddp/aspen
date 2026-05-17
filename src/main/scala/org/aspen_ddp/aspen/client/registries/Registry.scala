package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{Key, KeyAlreadyExists, KeyValueObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object Registry:
  class DuplicateRegistration(val key: Key, val existing: Value)
    extends Exception(s"Duplicate registration for key $key")

class Registry(val client: AspenClient,
               containingObjectPtr: KeyValueObjectPointer,
               treeRootKey: Key):

  given ExecutionContext = client.clientContext

  private val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, treeRootKey, containingObjectPtr))

  def get(key: Key): Future[Value] =
    tkvl.get(key).map:
      case None => throw new NoSuchElementException(key.toString)
      case Some(vs) => vs.value

  def prepareRegister(key: Key, value: Value)(using tx: Transaction): Future[Unit] =
    tkvl.set(key, value, requirement = Some(Left(true))).map: _ =>
      tx.result.value match
        case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(key)
        case _ => ()

  def register(key: Key, value: Value): Future[Unit] =
    client.retryStrategy.retryUntilSuccessful:
      tkvl.get(key).flatMap:
        case Some(vs) =>
          if java.util.Arrays.equals(vs.value.bytes, value.bytes) then
            Future.unit
          else
            throw StopRetrying(Registry.DuplicateRegistration(key, vs.value))
        case None =>
          client.transact: tx =>
            given Transaction = tx
            prepareRegister(key, value)
