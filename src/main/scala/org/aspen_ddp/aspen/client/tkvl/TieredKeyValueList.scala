package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectId, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future}

class TieredKeyValueList(val client: AspenClient,
                         val rootManager: RootManager) extends Logging {

  given ExecutionContext = client.clientContext

  import TieredKeyValueList._

  private val engine = new TierEngine(client, rootManager)

  // The root's kind can only be read asynchronously, so every operation checks it first. Over a root that is not a
  // KeyValueObjects root each fails with WrongTreeKind, before touching the transaction.
  private val checked = new TreeKindCheck(rootManager, TreeKind.KeyValueObjects)

  def get(key: Key): Future[Option[ValueState]] =
    checked(engine.getContainingNode(key).map(_.flatMap(node => kvNode(node).contents.get(key))))

  def getContainingNode(key: Key): Future[Option[TieredKeyValueListNode]] =
    checked(engine.getContainingNode(key).map(_.map(node => new TieredKeyValueListNode(this, kvNode(node)))))
  
  def deleteTree(odeleteKVPair: Option[(Key, ValueState) => Future[Unit]]): Future[Unit] =
    checked:
      engine.deleteTree: node =>
        val kvlp = KeyValueListPointer(Key.AbsoluteMinimum, kvNode(node).pointer)
        KeyValueListNode.deleteList(client, kvlp, node.ordering, odeleteKVPair)

  /**
   * Splits the tree into two trees at the supplied key. All keys comparing
   * less than splitAtKey will remain in the original tree, the rest will be
   * moved to a new tree of the same depth as the original. The resulting
   * Root object will be the root of the new tree. If the inclusive parameter
   * is set to true, the key exactly matching splitAtKey will be kept in
   * the original tree. Otherwise it will wind up in the new tree
   *
   * The splitting operation is completed in a single transaction but note
   * that race conditions with outstanding split/join finalization actions
   * could cause errant content to appear in the original (left) tree.
   * Use with caution.
   */
  def splitTree(splitAtKey: Key, inclusive: Boolean = false)
               (using t: Transaction): Future[Root] =
    checked(engine.splitTree(splitAtKey, inclusive))

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)
         (using t: Transaction): Future[Unit] =
    checked(engine.set(key, value, requirement))

  def delete(key: Key)(using t: Transaction): Future[Unit] = checked(engine.delete(key))

  /** KeyValue-object trees only: the requirements are key-value requirements on the containing node */
  def delete(key: Key, 
             requiredRevision: Option[ObjectRevision],
             requirements: List[KeyValueUpdate.KeyRequirement])(using t: Transaction): Future[Unit] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.unit
    }
    checked:
      engine.deleteFromContainingNode(key, node => kvNode(node).delete(key, requiredRevision, requirements, onJoin))
  }

  def foldLeft[B](z: B)(fn: (B, Map[Key, ValueState]) => B): Future[B] =
    checked(engine.foldLeft(z)((acc, node) => fn(acc, kvNode(node).contents)))

  /** Visit every entry in the tree, in ascending key order.
   *
   *  Exactly `foreachFrom(Key.AbsoluteMinimum, fn)`.
   *
   *  A failing `fn`, whether it returns a failed future or throws, is logged and the walk
   *  continues to the next key. A failing read fails the returned future.
   */
  def foreach(fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    checked(engine.foreach(withValueState(fn)))

  /** Visit every entry in `[minKey, maxKey)`, in ascending key order. */
  def foreachInRange(minKey: Key,
                     maxKey: Key,
                     fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    checked(engine.foreachInRange(minKey, maxKey, withValueState(fn)))

  /** Visit every entry at or above `minKey`, in ascending key order. The open-ended
   *  counterpart to foreachInRange, used to resume an interrupted walk from a checkpointed
   *  key.
   *
   *  The bound is inclusive: if `minKey` is still present it is visited again. A consumer that
   *  checkpoints the last key it finished therefore re-processes exactly that one key on
   *  resume, so whatever it does per key must be idempotent. That is the safe side of the
   *  trade - an exclusive bound would turn a crash between doing the work and recording the
   *  checkpoint into work that is permanently skipped.
   */
  def foreachFrom(minKey: Key,
                  fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    checked(engine.foreachFrom(minKey, withValueState(fn)))

  /** `fn` as a TierEngine walk callback, handed the KeyValueListNode and the entry's ValueState */
  private def withValueState(fn: (KeyValueListNode, Key, ValueState) => Future[Unit])
                            : (ListNode, Key, Value) => Future[Unit] =
    (node, key, _) =>
      val kvn = kvNode(node)
      fn(kvn, key, kvn.contents(key))
}

object TieredKeyValueList {

  /** The KeyValueListNode view of a node of a KeyValue-object tree */
  private[tkvl] def kvNode(node: ListNode): KeyValueListNode = node match
    case kvn: KeyValueListNode => kvn
    case other => throw new IllegalStateException(s"Expected a KeyValueListNode but found ${other.getClass.getName}")

  /** TierEngine.fetchContainingNode for KeyValue-object trees */
  private[tkvl] def fetchContainingNode(client: AspenClient,
                                        currentTier: Int,
                                        targetTier: Int,
                                        ordering: KeyOrdering,
                                        target: Key,
                                        currentNode: KeyValueListNode,
                                        initialBlacklist: Set[ObjectId],
                                        reversePath: List[KeyValueListNode] = Nil): Future[Either[Set[ObjectId], KeyValueListNode]] =
    given ExecutionContext = client.clientContext
    TierEngine.fetchContainingNode(client, currentTier, targetTier, ordering, target, currentNode,
      initialBlacklist, reversePath).map(_.map(kvNode))
}
