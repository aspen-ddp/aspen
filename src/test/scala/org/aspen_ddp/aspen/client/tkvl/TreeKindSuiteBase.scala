package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, IntegerKeyOrdering, Key, KeyValueObjectPointer, ObjectPointer, Value}

import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.util.Random

/** What the TKVL suites that run each test against a KeyValue-object tree and a DataObject tree share: the two
  * facades behind one interface, tree creation, and views of a tree's shape. A suite's pool IDA decides the node size
  * each kind needs (`nodeSize`).
  */
abstract class TreeKindSuiteBase extends IntegrationTestSuite:

  import TreeKindSuiteBase.Op

  protected val ordering = IntegerKeyOrdering
  protected val treeKey = Key(Array[Byte](0))

  /** A `maxNodeSize` per kind that holds a handful of tier-0 entries. KeyValue-object pairs carry a revision and a
    * timestamp each and are IDA-encoded, so the same entries need larger KeyValue nodes, by an amount that depends
    * on the pool's IDA.
    */
  def nodeSize(kind: TreeKind): Int

  protected def value(i: Int): Value = Value(Array.fill[Byte](40)(i.toByte))

  protected def keys(range: Range): List[Key] = range.toList.map(i => Key(i))

  protected def before(k: Key, bound: Key): Boolean = ordering.compare(k, bound) < 0

  // ---------------------------------------------------------------------------------------------------------------
  // The facades behind one interface

  /** The operations under test, for either facade. Values come back as bytes, since Value compares by identity. */
  protected trait Tree:
    def get(k: Key): Future[Option[List[Byte]]]
    def set(k: Key, v: Value)(using Transaction): Future[Unit]
    def delete(k: Key)(using Transaction): Future[Unit]
    def prepareUpdates(inserts: List[TKVLInsert], deletes: List[Key])(using Transaction): Future[Unit]
    /** Every entry, in the order the tree walks them */
    def entries(): Future[List[(Key, List[Byte])]]
    def keysFrom(minKey: Key): Future[List[Key]]
    def keysInRange(minKey: Key, maxKey: Key): Future[List[Key]]
    def splitTree(k: Key, inclusive: Boolean)(using Transaction): Future[Root]
    def deleteTree(odeleteEntry: Option[(Key, List[Byte]) => Future[Unit]]): Future[Unit]

  /** Runs a walk, returning what it visited in order */
  protected def visitsOf[A](walk: (A => Unit) => Future[Unit]): Future[List[A]] =
    var visited = List[A]()
    walk(a => synchronized { visited = a :: visited }).map(_ => visited.reverse)

  protected def open(kind: TreeKind, rm: RootManager): Tree = kind match
    case TreeKind.KeyValueObjects =>
      val t = new TieredKeyValueList(client, rm)
      new Tree:
        def get(k: Key) = t.get(k).map(_.map(_.value.bytes.toList))
        def set(k: Key, v: Value)(using Transaction) = t.set(k, v)
        def delete(k: Key)(using Transaction) = t.delete(k)
        def prepareUpdates(inserts: List[TKVLInsert], deletes: List[Key])(using Transaction) =
          t.prepareUpdates(inserts, deletes)
        def entries() = visitsOf(f => t.foreach((_, k, vs) => Future.successful(f(k -> vs.value.bytes.toList))))
        def keysFrom(minKey: Key) = visitsOf(f => t.foreachFrom(minKey, (_, k, _) => Future.successful(f(k))))
        def keysInRange(minKey: Key, maxKey: Key) =
          visitsOf(f => t.foreachInRange(minKey, maxKey, (_, k, _) => Future.successful(f(k))))
        def splitTree(k: Key, inclusive: Boolean)(using Transaction) = t.splitTree(k, inclusive)
        def deleteTree(odeleteEntry: Option[(Key, List[Byte]) => Future[Unit]]) =
          t.deleteTree(odeleteEntry.map(fn => (k, vs) => fn(k, vs.value.bytes.toList)))
    case TreeKind.DataObjects =>
      val t = new DataObjectTKVL(client, rm)
      new Tree:
        def get(k: Key) = t.get(k).map(_.map(_.bytes.toList))
        def set(k: Key, v: Value)(using Transaction) = t.set(k, v)
        def delete(k: Key)(using Transaction) = t.delete(k)
        def prepareUpdates(inserts: List[TKVLInsert], deletes: List[Key])(using Transaction) =
          t.prepareUpdates(inserts, deletes)
        def entries() = visitsOf(f => t.foreach((k, v) => Future.successful(f(k -> v.bytes.toList))))
        def keysFrom(minKey: Key) = visitsOf(f => t.foreachFrom(minKey, (k, _) => Future.successful(f(k))))
        def keysInRange(minKey: Key, maxKey: Key) =
          visitsOf(f => t.foreachInRange(minKey, maxKey, (k, _) => Future.successful(f(k))))
        def splitTree(k: Key, inclusive: Boolean)(using Transaction) = t.splitTree(k, inclusive)
        def deleteTree(odeleteEntry: Option[(Key, List[Byte]) => Future[Unit]]) =
          t.deleteTree(odeleteEntry.map(fn => (k, v) => fn(k, v.bytes.toList)))

  /** A KV object to host tree roots */
  protected def rootHost(): Future[KeyValueObjectPointer] =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      tx0 = client.newTransaction()
      ptr <- pool.allocator.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()
    yield
      ptr

  protected def newTree(kind: TreeKind, maxNodeSize: Int): Future[(Tree, KVObjectRootManager)] =
    for
      host <- rootHost()
      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId, maxNodeSize)
      tx = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, host, treeKey, ordering, nodeAllocator, Map(), kind)(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      rm <- froot
    yield
      (open(kind, rm), rm)

  protected def newTree(kind: TreeKind): Future[(Tree, KVObjectRootManager)] = newTree(kind, nodeSize(kind))

  /** Runs `update` in a transaction of its own, committing it unless it staged nothing (a delete of an absent key) */
  protected def inTransaction(update: Transaction => Future[Unit]): Future[Unit] =
    val tx = client.newTransaction()
    update(tx).flatMap: _ =>
      if tx.isEmpty then
        Future.unit
      else
        tx.commit().map(_ => ())

  /** One transaction per update, in order, without waiting for split and join finalization in between (a descent
    * tolerates upper tiers that finalization has not caught up with). Everything is finalized before this completes.
    */
  protected def sequentially[A](items: List[A])(update: (A, Transaction) => Future[Unit]): Future[Unit] =
    items.foldLeft(Future.unit)((prev, a) => prev.flatMap(_ => inTransaction(tx => update(a, tx))))
      .flatMap(_ => waitForTransactionsToComplete())

  protected def setAll(tree: Tree, ks: List[Key]): Future[Unit] =
    sequentially(ks)((k, tx) => tree.set(k, value(k.bytes.last))(using tx))

  protected def deleteAll(tree: Tree, ks: List[Key]): Future[Unit] =
    sequentially(ks)((k, tx) => tree.delete(k)(using tx))

  protected def expectedEntries(ks: List[Key]): List[(Key, List[Byte])] = ks.map(k => k -> value(k.bytes.last).bytes.toList)

  // ---------------------------------------------------------------------------------------------------------------
  // Tree shape

  /** Every tier's nodes, top tier first, or Nil for a tree with no root node */
  protected def allTiers(kind: TreeKind, rm: RootManager): Future[List[List[ListNode]]] =
    val encoding = NodeEncoding(kind)
    def read(ptr: ObjectPointer, minimum: Key) = encoding.read(client, ptr, minimum, ordering, "test read")
    def tierNodes(head: ListNode): Future[List[ListNode]] = head.link match
      case None => Future.successful(head :: Nil)
      case Some(l) => read(l.pointer, l.minimum).flatMap(tierNodes).map(head :: _)
    def down(tier: Int, head: ListNode): Future[List[List[ListNode]]] =
      tierNodes(head).flatMap: nodes =>
        if tier == 0 then
          Future.successful(nodes :: Nil)
        else
          read(ObjectPointer(head.get(Key.AbsoluteMinimum).get.bytes), Key.AbsoluteMinimum)
            .flatMap(down(tier - 1, _)).map(nodes :: _)
    rm.getFreshRootNode().flatMap: (tier, _, oroot) =>
      oroot match
        case None => Future.successful(Nil)
        case Some(root) => down(tier, root)

  protected def tier0Nodes(kind: TreeKind, rm: RootManager): Future[List[ListNode]] =
    allTiers(kind, rm).map(_.lastOption.getOrElse(Nil))

  protected def numTiers(rm: RootManager): Future[Int] = rm.getFreshRootNode().map(_._1)

  protected def exists(p: ObjectPointer): Future[Boolean] = p match
    case dp: DataObjectPointer => client.readOptional(dp).map(_.isDefined)
    case kp: KeyValueObjectPointer => client.readOptional(kp).map(_.isDefined)

  /** Bytes one store holds for the object: the size of its slice, as the store measures it */
  protected def storedBytes(p: ObjectPointer): Future[Int] = p match
    case dp: DataObjectPointer => client.read(dp).map(_.sizeOnStore)
    case kp: KeyValueObjectPointer => client.read(kp).map(_.sizeOnStore)

  protected def ofKind(kind: TreeKind, nodes: List[ListNode]): Boolean = nodes.forall(n => TreeKind.of(n.pointer) == kind)

  /** `count` operations on keys in `[0, keySpace)`, `setPercent` of them sets of values of random length */
  protected def randomOps(rand: Random, count: Int, keySpace: Int, setPercent: Int): List[Op] =
    List.fill(count):
      val k = Key(rand.nextInt(keySpace))
      if rand.nextInt(100) < setPercent then
        Op.Set(k, Value(Array.fill[Byte](8 + rand.nextInt(40))(rand.nextInt(256).toByte)))
      else
        Op.Delete(k)

  /** Every key in `[0, keySpace)` read back individually matches the model */
  protected def readsMatch(tree: Tree, model: TreeMap[Key, List[Byte]], keySpace: Int): Future[Unit] =
    val all = keys(0 until keySpace)
    Future.sequence(all.map(tree.get)).map: values =>
      assert(values == all.map(model.get))

object TreeKindSuiteBase:

  enum Op:
    case Set(key: Key, value: Value)
    case Delete(key: Key)
