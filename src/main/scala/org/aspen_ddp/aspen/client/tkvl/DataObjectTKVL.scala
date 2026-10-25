package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, Value}

import scala.concurrent.{ExecutionContext, Future}

/** A tiered key-value list whose nodes, at every tier, are DataObjects. Meant for single-writer and low-contention
  * use: every node update is a whole-node rewrite under the node's revision.
  *
  * Differs from `TieredKeyValueList` where the node kind shows through:
  *  - Entries are plain values. There are no per-key revisions or timestamps and no key-level requirements.
  *  - A transaction can modify each node only once. A `set` or `delete` reaching a node the same transaction already
  *    modifies fails with `NodeAlreadyModified`, so two keys that share a node cannot be changed in one transaction.
  *
  * The root's kind can only be read asynchronously, so the constructor cannot check it. Every operation checks it
  * first and fails with `WrongTreeKind` for a root that is not a `DataObjects` root, before touching the
  * transaction. `DataObjectTKVL.open` (and `KVObjectRootManager.getDataObjectTree`) check it up front.
  */
class DataObjectTKVL(val client: AspenClient, val rootManager: RootManager):

  given ExecutionContext = client.clientContext

  private val engine = new TierEngine(client, rootManager)

  // The kind of a tree never changes, so one successful check covers every later operation
  @volatile private var kindChecked = false

  /** Runs `f` once the root is known to be a DataObjects root */
  private def checked[A](f: => Future[A]): Future[A] =
    if kindChecked then
      f
    else
      DataObjectTKVL.checkKind(rootManager).flatMap: _ =>
        kindChecked = true
        f

  def get(key: Key): Future[Option[Value]] = checked(engine.get(key))

  def set(key: Key, value: Value)(using Transaction): Future[Unit] = checked(engine.set(key, value))

  def delete(key: Key)(using Transaction): Future[Unit] = checked(engine.delete(key))

  /** Folds `fn` over the entries of every tier-0 node, left to right */
  def foldLeft[B](z: B)(fn: (B, Map[Key, Value]) => B): Future[B] =
    checked(engine.foldLeft(z)((acc, node) => fn(acc, node.entries.toMap)))

  /** Visit every entry in the tree, in ascending key order.
    *
    * A failing `fn`, whether it returns a failed future or throws, is logged and the walk continues to the next key.
    * A failing read fails the returned future.
    */
  def foreach(fn: (Key, Value) => Future[Unit]): Future[Unit] =
    checked(engine.foreach((_, k, v) => fn(k, v)))

  /** Visit every entry at or above `minKey`, in ascending key order. The bound is inclusive, as for
    * `TieredKeyValueList.foreachFrom`.
    */
  def foreachFrom(minKey: Key, fn: (Key, Value) => Future[Unit]): Future[Unit] =
    checked(engine.foreachFrom(minKey, (_, k, v) => fn(k, v)))

  /** Visit every entry in `[minKey, maxKey)`, in ascending key order. */
  def foreachInRange(minKey: Key, maxKey: Key, fn: (Key, Value) => Future[Unit]): Future[Unit] =
    checked(engine.foreachInRange(minKey, maxKey, (_, k, v) => fn(k, v)))

  /** As `TieredKeyValueList.splitTree`. The left tree's last node has no maximum afterwards, so unlike a KeyValue
    * left tree it keeps accepting keys at or above `splitAtKey` (see `DataObjectNodeEncoding.splitAt`).
    */
  def splitTree(splitAtKey: Key, inclusive: Boolean = false)(using Transaction): Future[Root] =
    checked(engine.splitTree(splitAtKey, inclusive))

  /** Deletes every node of the tree, handing each entry to `odeleteEntry` first if given */
  def deleteTree(odeleteEntry: Option[(Key, Value) => Future[Unit]]): Future[Unit] =
    checked:
      engine.deleteTree: node =>
        DataObjectNodeEncoding.deleteList(client, NodeLink(Key.AbsoluteMinimum, node.pointer), node.ordering,
          odeleteEntry)

object DataObjectTKVL:

  /** The tree managed by `rootManager`. Fails with `WrongTreeKind` if its root is not a `DataObjects` root. */
  def open(client: AspenClient, rootManager: RootManager): Future[DataObjectTKVL] =
    given ExecutionContext = client.clientContext
    checkKind(rootManager).map(_ => new DataObjectTKVL(client, rootManager))

  private def checkKind(rootManager: RootManager)(using ExecutionContext): Future[Unit] =
    rootManager.getRoot().map: root =>
      if root.kind != TreeKind.DataObjects then
        throw WrongTreeKind(TreeKind.DataObjects, root.kind)
