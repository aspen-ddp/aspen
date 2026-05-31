package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, Value}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

class MetadataTree(client: AspenClient, radicle: KeyValueObjectPointer, treeKey: Key):
  val tree = TieredKeyValueList(client, KVObjectRootManager(client, treeKey, radicle))

  given ExecutionContext = client.clientContext

  def get(uuid: UUID): Future[ObjectPointer] =
    tree.get(Key(uuid)).map:
      case None => throw new NoSuchElementException(uuid.toString)
      case Some(vs) => ObjectPointer(vs.value.bytes)

  def preparePut(uuid: UUID, ptr: ObjectPointer)(using tx: Transaction): Future[Unit] =
    val key = Key(uuid)
    val value = Value(ptr.toArray)
    tree.set(key, value, requirement = Some(Left(true))).map: _ =>
      tx.result.value match
        case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(key)
        case _ => ()
