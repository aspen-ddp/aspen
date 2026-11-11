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

  def get(key: Key): Future[Option[ValueState]] =
    engine.getContainingNode(key).map(_.flatMap(node => kvNode(node).contents.get(key)))

  def getContainingNode(key: Key): Future[Option[TieredKeyValueListNode]] =
    engine.getContainingNode(key).map(_.map(node => new TieredKeyValueListNode(this, kvNode(node))))
  
  def deleteTree(odeleteKVPair: Option[(Key, ValueState) => Future[Unit]]): Future[Unit] =
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
   *
   * WARNING: this instance's cache is cleared, but other long-lived
   * instances of this tree (in other clients, other processes, or other
   * instances in this process) may keep returning entries that moved to the
   * new tree until their caches are cleared (`rootManager.cache.clear()`).
   * Alternatively, trees that get split can be opened with `NoCache`.
   * Handling this is the caller's responsibility.
   */
  def splitTree(splitAtKey: Key, inclusive: Boolean = false)
               (using t: Transaction): Future[Root] =
    engine.splitTree(splitAtKey, inclusive)

  def set(key: Key,
          value: Value,
          requirement: PutRequirement = PutRequirement.Checked)
         (using t: Transaction): Future[Unit] =
    engine.set(key, value, requirement)

  def delete(key: Key)(using t: Transaction): Future[Unit] = engine.delete(key)

  /** `set`, with the update of the node holding `key` also requiring that node's revision as read. For transactions
    * that have no other pin (see `Pins`): the insert of an absent key carries only `DoesNotExist`, which a later
    * delete makes true again, while the node's revision changes with every write to it. Every writer of the node is
    * serialized, so this suits low-rate trees such as registries.
    */
  def prepareSetPinned(key: Key, value: Value, requirement: PutRequirement)(using t: Transaction): Future[Unit] =
    engine.setPinned(key, value, requirement)

  /** Inserts and deletes many keys in one transaction. Each node is updated once, so keys sharing a node can change
    * together, which `set` and `delete` on the same node in one transaction cannot. Deleting a key the tree does not
    * hold does nothing. Fails with `IllegalArgumentException`, before touching the transaction, if a key is inserted
    * twice or both inserted and deleted. A failure once part of the batch is staged aborts the transaction.
    *
    * Each key of `deletesAtRevision` is deleted only if the node read holds it at the given revision, under that
    * `KeyRevision`. Otherwise (a concurrent write changed or removed it since the caller read it) the transaction is
    * aborted with `KeyNotAtRevision`, so a caller that derived other writes from the key's value as it read it never
    * commits them against a different value.
    */
  def prepareUpdates(inserts: List[TKVLInsert],
                     deletes: List[Key],
                     deletesAtRevision: List[(Key, ObjectRevision)] = Nil)(using t: Transaction): Future[Unit] =
    Future.delegate(engine.prepareUpdates(BatchOp.from(inserts, deletes, deletesAtRevision)))

  /** Requires, at commit, that each key is still held at its revision as given, writing nothing: one
    * requirement-only update of each containing node. Fails with `KeyNotAtRevision`, aborting the transaction, if a
    * node read does not hold its key at the revision. The transaction must not otherwise update these nodes. Such a
    * check alone is no pin: a transaction made of it writes nothing, so replaying it is harmless (`replaySafe`).
    */
  def prepareRequireRevisions(keys: List[(Key, ObjectRevision)])(using t: Transaction): Future[Unit] =
    Future.sequence(keys.map((k, rev) => engine.getContainingNode(k).map(on => (k, rev, on)))).map: found =>
      val located = found.map:
        case (k, rev, Some(node)) if kvNode(node).contents.get(k).exists(_.revision == rev) => (k, rev, kvNode(node))
        case (k, rev, _) => t.abortAndThrow(KeyNotAtRevision(k, rev))
      located.groupBy(_._3.pointer.id).values.foreach: group =>
        val reqs = group.map((k, rev, _) => KeyValueUpdate.KeyRevision(k, rev))
        t.update(group.head._3.pointer, None, None, reqs, Nil)

  /** KeyValue-object trees only: the requirements are key-value requirements on the containing node */
  def delete(key: Key, 
             requiredRevision: Option[ObjectRevision],
             requirements: List[KeyValueUpdate.KeyRequirement])(using t: Transaction): Future[Unit] =
    deleteIfPresent(key, requiredRevision, requirements).map(_ => ())

  /** As `delete`, but the result says whether the key was deleted. False when the node the delete planned against
    * did not hold the key, as after a concurrent delete of it: nothing was added to the transaction, the
    * requirements included, so a caller relying on them for its pin must not commit
    */
  def deleteIfPresent(key: Key,
                      requiredRevision: Option[ObjectRevision],
                      requirements: List[KeyValueUpdate.KeyRequirement])(using t: Transaction): Future[Boolean] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.unit
    }
    val deleted = new java.util.concurrent.atomic.AtomicBoolean(false)
    engine.deleteFromContainingNode(key, node =>
      kvNode(node).deleteIfPresent(key, requiredRevision, requirements, onJoin).map {
        case None => ()
        case Some(empty) =>
          deleted.set(true)
          if empty then
            TierEngine.prepareEmptyNodeRemoval(rootManager, 0, node)
      }).map(_ => deleted.get)
  }

  def foldLeft[B](z: B)(fn: (B, Map[Key, ValueState]) => B): Future[B] =
    engine.foldLeft(z)((acc, node) => fn(acc, kvNode(node).contents))

  /** Visit every entry in the tree, in ascending key order.
   *
   *  Exactly `foreachFrom(Key.AbsoluteMinimum, fn)`.
   *
   *  A failing `fn`, whether it returns a failed future or throws, is logged and the walk
   *  continues to the next key. A failing read fails the returned future.
   *
   *  The walk ends early, reading nothing more, once `stopWhen` holds; see TierEngine.foreach.
   */
  def foreach(fn: (KeyValueListNode, Key, ValueState) => Future[Unit],
              stopWhen: () => Boolean = () => false): Future[Unit] =
    engine.foreach(withValueState(fn), stopWhen)

  /** Visit every entry in `[minKey, maxKey)`, in ascending key order. */
  def foreachInRange(minKey: Key,
                     maxKey: Key,
                     fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    engine.foreachInRange(minKey, maxKey, withValueState(fn))

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
                  fn: (KeyValueListNode, Key, ValueState) => Future[Unit],
                  stopWhen: () => Boolean = () => false): Future[Unit] =
    engine.foreachFrom(minKey, withValueState(fn), stopWhen)

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
