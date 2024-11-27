package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID

import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRevision, Value}

import scala.concurrent.{ExecutionContext, Future}

class TieredKeyValueListNode(val tkvl: TieredKeyValueList,
                             private val node: KeyValueListNode) {

  implicit val ec: ExecutionContext = tkvl.client.clientContext

  def nodeUUID: UUID = node.pointer.id.uuid

  def get(key: Key): Option[ValueState] = node.contents.get(key)

  def contains(key: Key): Boolean = node.contents.contains(key)

  def keys: Set[Key] = node.contents.keySet

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)(implicit t: Transaction): Future[Unit] = {

    def onSplit(newMinimum: Key, newNode: KeyValueObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(tkvl.rootManager, 1, newMinimum, newNode, t)
      Future.successful(())
    }

    for {
      alloc <- tkvl.rootManager.getAllocatorForTier(0)
      maxNodeSize <- tkvl.rootManager.getMaxNodeSize(0)

      _ <- node.insert(key, value, maxNodeSize, alloc, onSplit, requirement)
    } yield {
      ()
    }
  }

  def delete(key: Key)(implicit t: Transaction): Future[Unit] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(tkvl.rootManager, 1, delMinimum, delNode, t)
      Future.successful(())
    }

    node.delete(key, onJoin)
  }

  /** May only be used on nodes that contain both the old and new key */
  def rename(oldKey: Key, newKey: Key)(implicit t: Transaction): Future[Unit] = {
    assert(node.keyInRange(oldKey) && node.keyInRange(newKey))

    def onSplit(newMinimum: Key, newNode: KeyValueObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(tkvl.rootManager, 1, newMinimum, newNode, t)
      Future.successful(())
    }

    for {
      alloc <- tkvl.rootManager.getAllocatorForTier(0)
      maxNodeSize <- tkvl.rootManager.getMaxNodeSize(0)

      _ <- node.rename(oldKey, newKey, maxNodeSize, alloc, onSplit)
    } yield {
      ()
    }
  }
}
