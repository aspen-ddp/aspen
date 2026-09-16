package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, Value}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class MetadataTree(client: AspenClient, radicle: KeyValueObjectPointer, treeKey: Key):
  val tree = TieredKeyValueList(client, KVObjectRootManager(client, treeKey, radicle))

  given ExecutionContext = client.clientContext

  def get(uuid: UUID): Future[ObjectPointer] =
    tree.get(Key(uuid)).map:
      case None => throw new NoSuchElementException(uuid.toString)
      case Some(vs) => ObjectPointer(vs.value.bytes)

  /** Fails with KeyAlreadyExists if the uuid is already mapped. On a non-empty tree the
   *  requirement's abortAndThrow raises it directly out of the insert.
   */
  def preparePut(uuid: UUID, ptr: ObjectPointer)(using tx: Transaction): Future[Unit] =
    tree.set(Key(uuid), Value(ptr.toArray), requirement = Some(Left(true)))
