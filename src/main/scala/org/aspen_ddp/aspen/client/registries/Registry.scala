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

  def scan(prefix: String): Future[List[(Key, Value)]] =
    val minKey = Key(s"$prefix.")
    // Boundary trick: '/' (0x2F) is the byte immediately after '.' (0x2E). Because
    // keys are byte-ordered and namespace keys have the form "<prefix>.<name>", the
    // range [ "<prefix>.", "<prefix>/" ) captures exactly the keys for this namespace
    // and excludes any sibling namespace whose name merely starts with the same text.
    val maxKey = Key(s"$prefix/")
    val buf = scala.collection.mutable.ListBuffer[(Key, Value)]()
    tkvl.foreachInRange(minKey, maxKey, (_, key, vs) => {
      buf += key -> vs.value
      Future.unit
    }).map(_ => buf.toList)

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
