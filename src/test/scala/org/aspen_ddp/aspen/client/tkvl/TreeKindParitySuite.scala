package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, IntegerKeyOrdering, Key, KeyValueObjectPointer, ObjectPointer, Value}

import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.util.Random

/** The same tests, run against a KeyValue-object tree and a DataObject tree, show that both kinds behave alike
  * through their facades. The suite runs once per pool IDA (see the subclasses at the bottom of this file).
  */
abstract class TreeKindParitySuiteBase extends IntegrationTestSuite:

  private val ordering = IntegerKeyOrdering
  private val treeKey = Key(Array[Byte](0))

  /** A `maxNodeSize` per kind that holds a handful of tier-0 entries. KeyValue-object pairs carry a revision and a
    * timestamp each and are IDA-encoded, so the same entries need larger KeyValue nodes, by an amount that depends
    * on the pool's IDA.
    */
  def nodeSize(kind: TreeKind): Int

  private def value(i: Int): Value = Value(Array.fill[Byte](40)(i.toByte))

  private def keys(range: Range): List[Key] = range.toList.map(i => Key(i))

  private def before(k: Key, bound: Key): Boolean = ordering.compare(k, bound) < 0

  // ---------------------------------------------------------------------------------------------------------------
  // The facades behind one interface

  /** The operations under test, for either facade. Values come back as bytes, since Value compares by identity. */
  private trait Tree:
    def get(k: Key): Future[Option[List[Byte]]]
    def set(k: Key, v: Value)(using Transaction): Future[Unit]
    def delete(k: Key)(using Transaction): Future[Unit]
    /** Every entry, in the order the tree walks them */
    def entries(): Future[List[(Key, List[Byte])]]
    def keysFrom(minKey: Key): Future[List[Key]]
    def keysInRange(minKey: Key, maxKey: Key): Future[List[Key]]
    def splitTree(k: Key, inclusive: Boolean)(using Transaction): Future[Root]
    def deleteTree(odeleteEntry: Option[(Key, List[Byte]) => Future[Unit]]): Future[Unit]

  /** Runs a walk, returning what it visited in order */
  private def visitsOf[A](walk: (A => Unit) => Future[Unit]): Future[List[A]] =
    var visited = List[A]()
    walk(a => synchronized { visited = a :: visited }).map(_ => visited.reverse)

  private def open(kind: TreeKind, rm: RootManager): Tree = kind match
    case TreeKind.KeyValueObjects =>
      val t = new TieredKeyValueList(client, rm)
      new Tree:
        def get(k: Key) = t.get(k).map(_.map(_.value.bytes.toList))
        def set(k: Key, v: Value)(using Transaction) = t.set(k, v)
        def delete(k: Key)(using Transaction) = t.delete(k)
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
        def entries() = visitsOf(f => t.foreach((k, v) => Future.successful(f(k -> v.bytes.toList))))
        def keysFrom(minKey: Key) = visitsOf(f => t.foreachFrom(minKey, (k, _) => Future.successful(f(k))))
        def keysInRange(minKey: Key, maxKey: Key) =
          visitsOf(f => t.foreachInRange(minKey, maxKey, (k, _) => Future.successful(f(k))))
        def splitTree(k: Key, inclusive: Boolean)(using Transaction) = t.splitTree(k, inclusive)
        def deleteTree(odeleteEntry: Option[(Key, List[Byte]) => Future[Unit]]) =
          t.deleteTree(odeleteEntry.map(fn => (k, v) => fn(k, v.bytes.toList)))

  /** A KV object to host tree roots */
  private def rootHost(): Future[KeyValueObjectPointer] =
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

  private def newTree(kind: TreeKind, maxNodeSize: Int): Future[(Tree, KVObjectRootManager)] =
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

  private def newTree(kind: TreeKind): Future[(Tree, KVObjectRootManager)] = newTree(kind, nodeSize(kind))

  /** Runs `update` in a transaction of its own, committing it unless it staged nothing (a delete of an absent key) */
  private def inTransaction(update: Transaction => Future[Unit]): Future[Unit] =
    val tx = client.newTransaction()
    update(tx).flatMap: _ =>
      if tx.isEmpty then
        Future.unit
      else
        tx.commit().map(_ => ())

  /** One transaction per update, in order, without waiting for split and join finalization in between (a descent
    * tolerates upper tiers that finalization has not caught up with). Everything is finalized before this completes.
    */
  private def sequentially[A](items: List[A])(update: (A, Transaction) => Future[Unit]): Future[Unit] =
    items.foldLeft(Future.unit)((prev, a) => prev.flatMap(_ => inTransaction(tx => update(a, tx))))
      .flatMap(_ => waitForTransactionsToComplete())

  private def setAll(tree: Tree, ks: List[Key]): Future[Unit] =
    sequentially(ks)((k, tx) => tree.set(k, value(k.bytes.last))(using tx))

  private def deleteAll(tree: Tree, ks: List[Key]): Future[Unit] =
    sequentially(ks)((k, tx) => tree.delete(k)(using tx))

  private def expectedEntries(ks: List[Key]): List[(Key, List[Byte])] = ks.map(k => k -> value(k.bytes.last).bytes.toList)

  // ---------------------------------------------------------------------------------------------------------------
  // Tree shape

  /** Every tier's nodes, top tier first, or Nil for a tree with no root node */
  private def allTiers(kind: TreeKind, rm: RootManager): Future[List[List[ListNode]]] =
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
    rm.getRootNode().flatMap: (tier, _, oroot) =>
      oroot match
        case None => Future.successful(Nil)
        case Some(root) => down(tier, root)

  private def tier0Nodes(kind: TreeKind, rm: RootManager): Future[List[ListNode]] =
    allTiers(kind, rm).map(_.lastOption.getOrElse(Nil))

  private def numTiers(rm: RootManager): Future[Int] = rm.getRootNode().map(_._1)

  private def exists(p: ObjectPointer): Future[Boolean] = p match
    case dp: DataObjectPointer => client.readOptional(dp).map(_.isDefined)
    case kp: KeyValueObjectPointer => client.readOptional(kp).map(_.isDefined)

  /** Bytes one store holds for the object: the size of its slice, as the store measures it */
  private def storedBytes(p: ObjectPointer): Future[Int] = p match
    case dp: DataObjectPointer => client.read(dp).map(_.sizeOnStore)
    case kp: KeyValueObjectPointer => client.read(kp).map(_.sizeOnStore)

  private def ofKind(kind: TreeKind, nodes: List[ListNode]): Boolean = nodes.forall(n => TreeKind.of(n.pointer) == kind)

  // ---------------------------------------------------------------------------------------------------------------
  // The shared tests, run for each kind

  for kind <- TreeKind.values do

    atest(s"$kind: set, get, overwrite and delete"):
      for
        (tree, rm) <- newTree(kind)
        _ <- setAll(tree, keys(1 to 3))
        v2 <- tree.get(Key(2))
        _ <- inTransaction(tx => tree.set(Key(2), value(99))(using tx))
        v2b <- tree.get(Key(2))
        _ <- deleteAll(tree, List(Key(1)))
        v1 <- tree.get(Key(1))
        absent <- tree.get(Key(42))
        tx = client.newTransaction()
        _ <- tree.delete(Key(42))(using tx)
        entries <- tree.entries()
        nodes <- tier0Nodes(kind, rm)
      yield
        assert(v2 == Some(value(2).bytes.toList))
        assert(v2b == Some(value(99).bytes.toList))
        assert(v1.isEmpty)
        assert(absent.isEmpty)
        assert(tx.isEmpty)
        assert(entries == List(Key(2) -> value(99).bytes.toList, Key(3) -> value(3).bytes.toList))
        assert(nodes.size == 1)
        assert(ofKind(kind, nodes))

    atest(s"$kind: inserts split nodes and grow tiers, and every key reads back in order"):
      val all = keys(1 to 40)
      for
        (tree, rm) <- newTree(kind)
        _ <- setAll(tree, all)
        tiers <- allTiers(kind, rm)
        n <- numTiers(rm)
        values <- Future.sequence(all.map(tree.get))
        missing <- tree.get(Key(41))
        entries <- tree.entries()
      yield
        assert(n >= 2)
        assert(tiers.size == n + 1)
        assert(ofKind(kind, tiers.flatten))
        assert(tiers.last.size > 3)
        assert(tiers.last.flatMap(_.entries.map(_._1)) == all)
        assert(values == all.map(k => Some(value(k.bytes.last).bytes.toList)))
        assert(missing.isEmpty)
        assert(entries == expectedEntries(all))

    atest(s"$kind: deletes join nodes, and deleting every key of a two-tier tree leaves one empty node"):
      val all = keys(1 to 40)
      // Enough contiguous keys to empty several whole tier-0 nodes
      val middle = keys(5 to 32)
      val few = keys(1 to 6)
      for
        // Joins across a multi-tier tree
        (tree, rm) <- newTree(kind)
        _ <- setAll(tree, all)
        nodesBefore <- tier0Nodes(kind, rm)
        _ <- deleteAll(tree, middle)
        nodesAfter <- tier0Nodes(kind, rm)
        remaining <- Future.sequence(nodesBefore.map(n => exists(n.pointer)))
        entries <- tree.entries()
        values <- Future.sequence(all.map(tree.get))

        // Tier shrink: only a root at tier 1 drops to tier 0 (see DataObjectTKVLSuite)
        (small, smallRm) <- newTree(kind)
        _ <- setAll(small, few)
        smallTiersBefore <- numTiers(smallRm)
        smallNodesBefore <- tier0Nodes(kind, smallRm)
        _ <- deleteAll(small, few)
        smallAfter <- allTiers(kind, smallRm)
        smallTiersAfter <- numTiers(smallRm)
        smallEntries <- small.entries()
      yield
        val kept = all.filterNot(middle.contains)
        assert(nodesAfter.size < nodesBefore.size)
        // Every node joined into its left neighbour is deleted
        assert(remaining.count(identity) == nodesAfter.size)
        assert(nodesAfter.map(_.pointer).toSet.subsetOf(nodesBefore.map(_.pointer).toSet))
        assert(entries == expectedEntries(kept))
        assert(values.map(_.isDefined) == all.map(kept.contains))

        assert(smallTiersBefore == 1)
        assert(smallNodesBefore.size > 1)
        assert(smallTiersAfter == 0)
        assert(smallAfter.flatten.size == 1)
        assert(smallAfter.flatten.head.entries.isEmpty)
        assert(smallEntries.isEmpty)

    atest(s"$kind: foreachFrom and foreachInRange honor their bounds on node edges"):
      val all = keys(1 to 40)
      for
        (tree, rm) <- newTree(kind)
        _ <- setAll(tree, all)
        nodes <- tier0Nodes(kind, rm)
        // A key exactly on a node's left edge, and the right edge of the node before it
        edge = nodes(2).minimum
        from <- tree.keysFrom(edge)
        fromBelow <- tree.keysFrom(Key(0))
        fromAbove <- tree.keysFrom(Key(41))
        toEdge <- tree.keysInRange(Key(1), edge)
        fromEdge <- tree.keysInRange(edge, Key(41))
        inside <- tree.keysInRange(Key(3), Key(7))
        empty <- tree.keysInRange(edge, edge)
      yield
        assert(nodes.size > 3)
        assert(all.contains(edge))
        assert(from == all.filterNot(before(_, edge)))
        assert(fromBelow == all)
        assert(fromAbove.isEmpty)
        assert(toEdge == all.filter(before(_, edge)))
        assert(fromEdge == from)
        assert(inside == keys(3 to 6))
        assert(empty.isEmpty)

    for inclusive <- List(false, true) do
      atest(s"$kind: splitTree (inclusive = $inclusive) moves the keys above the split key to a new tree"):
        val all = keys(1 to 40)
        val splitKey = Key(20)
        for
          (tree, rm) <- newTree(kind)
          _ <- setAll(tree, all)
          tx = client.newTransaction()
          newRoot <- tree.splitTree(splitKey, inclusive)(using tx)
          fnewRm = KVObjectRootManager.setNewRoot(client, rm.pointer, Key(Array[Byte](1)), true, newRoot)(using tx)
          _ <- tx.commit()
          _ <- waitForTransactionsToComplete()
          newRm <- fnewRm
          right = open(kind, newRm)
          leftEntries <- tree.entries()
          rightEntries <- right.entries()
          leftValues <- Future.sequence(all.map(tree.get))
          rightValues <- Future.sequence(all.map(right.get))
          rightTiers <- allTiers(kind, newRm)
        yield
          val (leftKeys, rightKeys) = all.partition: k =>
            before(k, splitKey) || (inclusive && k == splitKey)
          assert(newRoot.kind == kind)
          assert(ofKind(kind, rightTiers.flatten))
          assert(leftEntries == expectedEntries(leftKeys))
          assert(rightEntries == expectedEntries(rightKeys))
          assert(leftValues.map(_.isDefined) == all.map(leftKeys.contains))
          assert(rightValues.map(_.isDefined) == all.map(rightKeys.contains))

    atest(s"$kind: deleteTree removes every node and hands each entry to the callback"):
      val all = keys(1 to 40)
      var deleted = Map[Key, List[Byte]]()
      val onEntry: (Key, List[Byte]) => Future[Unit] = (k, v) =>
        synchronized { deleted = deleted + (k -> v) }
        Future.unit
      for
        (tree, rm) <- newTree(kind)
        _ <- setAll(tree, all)
        nodes <- allTiers(kind, rm).map(_.flatten)
        _ <- tree.deleteTree(Some(onEntry))
        remaining <- Future.sequence(nodes.map(n => exists(n.pointer)))
        (tier, _, oroot) <- rm.getRootNode()
        v <- tree.get(Key(1))
      yield
        assert(nodes.size > 5)
        assert(remaining.forall(!_))
        assert(tier == 0)
        assert(oroot.isEmpty)
        assert(v.isEmpty)
        assert(deleted == expectedEntries(all).toMap)

  // ---------------------------------------------------------------------------------------------------------------
  // Random operations against a model

  private enum Op:
    case Set(key: Key, value: Value)
    case Delete(key: Key)

  /** `count` operations on keys in `[0, keySpace)`, `setPercent` of them sets of values of random length */
  private def randomOps(rand: Random, count: Int, keySpace: Int, setPercent: Int): List[Op] =
    List.fill(count):
      val k = Key(rand.nextInt(keySpace))
      if rand.nextInt(100) < setPercent then
        Op.Set(k, Value(Array.fill[Byte](8 + rand.nextInt(40))(rand.nextInt(256).toByte)))
      else
        Op.Delete(k)

  private val checkEvery = 20

  /** Applies `ops` one transaction each, comparing every key in order against a `TreeMap` model after every
    * `checkEvery` operations and at the end. Returns the model and the tier-0 node count at each check.
    */
  private def runAgainstModel(kind: TreeKind, tree: Tree, rm: RootManager, ops: List[Op],
                              initial: TreeMap[Key, List[Byte]]): Future[(TreeMap[Key, List[Byte]], List[Int])] =
    def check(model: TreeMap[Key, List[Byte]], applied: Int): Future[Int] =
      for
        _ <- waitForTransactionsToComplete()
        entries <- tree.entries()
        nodes <- tier0Nodes(kind, rm)
      yield
        assert(entries == model.toList, s"after $applied operations")
        nodes.size

    def loop(rest: List[Op], model: TreeMap[Key, List[Byte]], applied: Int,
             counts: List[Int]): Future[(TreeMap[Key, List[Byte]], List[Int])] =
      rest match
        case Nil => check(model, applied).map(n => (model, (n :: counts).reverse))
        case op :: tail =>
          val (update, next) = op match
            case Op.Set(k, v) => ((tx: Transaction) => tree.set(k, v)(using tx), model + (k -> v.bytes.toList))
            case Op.Delete(k) => ((tx: Transaction) => tree.delete(k)(using tx), model - k)
          inTransaction(update).flatMap: _ =>
            if (applied + 1) % checkEvery == 0 then
              check(next, applied + 1).flatMap(n => loop(tail, next, applied + 1, n :: counts))
            else
              loop(tail, next, applied + 1, counts)

    loop(ops, initial, 0, Nil)

  /** Every key in `[0, keySpace)` read back individually matches the model */
  private def readsMatch(tree: Tree, model: TreeMap[Key, List[Byte]], keySpace: Int): Future[Unit] =
    val all = keys(0 until keySpace)
    Future.sequence(all.map(tree.get)).map: values =>
      assert(values == all.map(model.get))

  /** Indices of the checks after which the tier-0 node count fell (nodes joined) and rose (nodes split) */
  private def falls(counts: List[Int]): List[Int] = counts.zip(counts.drop(1)).zipWithIndex.collect:
    case ((a, b), i) if b < a => i
  private def rises(counts: List[Int]): List[Int] = counts.zip(counts.drop(1)).zipWithIndex.collect:
    case ((a, b), i) if b > a => i

  private val keySpace = 48

  atest("DataObjects: interleaved random sets and deletes match a TreeMap model"):
    val rand = new Random(0x5eed)
    val ops = randomOps(rand, 200, keySpace, 60)
    for
      (tree, rm) <- newTree(TreeKind.DataObjects)
      (model, counts) <- runAgainstModel(TreeKind.DataObjects, tree, rm, ops, TreeMap.empty(using ordering))
      _ <- readsMatch(tree, model, keySpace)
    yield
      // The workload splits nodes, joins them, and splits again after a join
      assert(counts.max > 3)
      assert(falls(counts).nonEmpty)
      assert(rises(counts).exists(_ > falls(counts).head))
      assert(model.nonEmpty)

  // A KeyValue-object join (KeyValueListNode.delete) leaves the left node with its old stored maximum, so the store's
  // range check rejects a later insert into the joined node at or above that old maximum. That is a known, separate
  // KeyValue-object defect, so the KeyValue tree runs its random operations in phases: every insert happens
  // before the first delete, and nothing is inserted once nodes have joined.
  atest("KeyValueObjects: random sets, then random deletes, match a TreeMap model"):
    val rand = new Random(0x5eed)
    val sets = randomOps(rand, 100, keySpace, 100)
    val deletes = randomOps(rand, 100, keySpace, 0)
    for
      (tree, rm) <- newTree(TreeKind.KeyValueObjects)
      (afterSets, setCounts) <- runAgainstModel(TreeKind.KeyValueObjects, tree, rm, sets,
        TreeMap.empty(using ordering))
      (model, deleteCounts) <- runAgainstModel(TreeKind.KeyValueObjects, tree, rm, deletes, afterSets)
      _ <- readsMatch(tree, model, keySpace)
    yield
      assert(setCounts.max > 3)
      assert(falls(deleteCounts).nonEmpty)
      assert(model.nonEmpty)

  // ---------------------------------------------------------------------------------------------------------------
  // Compactness

  // Total node bytes is the sum, over every node of every tier, of the bytes one store holds for that node's object
  // (its slice size: DataObjectState.sizeOnStore as reported by the store, KeyValueObjectState.sizeOnStore as the
  // store measures a KeyValue slice). Both trees hold the same entries with the same maxNodeSize.
  atest("a DataObject tree stores the same entries in fewer bytes than a KeyValue-object tree"):
    val all = keys(1 to 60)
    val maxNodeSize = 1000
    def totalBytes(kind: TreeKind): Future[(Int, Int)] =
      for
        (tree, rm) <- newTree(kind, maxNodeSize)
        _ <- setAll(tree, all)
        entries <- tree.entries()
        nodes <- allTiers(kind, rm).map(_.flatten)
        sizes <- Future.sequence(nodes.map(n => storedBytes(n.pointer)))
      yield
        assert(entries == expectedEntries(all))
        (sizes.sum, nodes.size)
    for
      (kvBytes, kvNodes) <- totalBytes(TreeKind.KeyValueObjects)
      (dataBytes, dataNodes) <- totalBytes(TreeKind.DataObjects)
    yield
      assert(kvNodes > 1 && dataNodes > 1)
      assert(dataBytes < kvBytes)

/** Every node is a whole copy of its object */
class TreeKindParitySuite extends TreeKindParitySuiteBase:
  def nodeSize(kind: TreeKind): Int = kind match
    case TreeKind.KeyValueObjects => 400
    case TreeKind.DataObjects => 200

/** Classic Reed-Solomon: DataObject nodes need only whole-object writes, so they work in such a pool. KeyValue
  * values are IDA-encoded per pair, so KeyValue slices here are about a third of their Replication size.
  */
class ReedSolomonTreeKindParitySuite extends TreeKindParitySuiteBase:
  override def testIda: IDA = ReedSolomon(5, 3, 4)

  def nodeSize(kind: TreeKind): Int = kind match
    case TreeKind.KeyValueObjects => 200
    case TreeKind.DataObjects => 200
