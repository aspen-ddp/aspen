package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{InvalidObject, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, IntegerKeyOrdering, Key, KeyOrdering, KeyValueObjectPointer, LexicalKeyOrdering, ObjectPointer, Value}

import scala.concurrent.Future
import scala.util.{Random, Success}

class DataObjectTKVLSuite extends IntegrationTestSuite:

  private val treeKey = Key(Array[Byte](0))
  private val ordering = IntegerKeyOrdering

  /** Small enough that a few dozen keys build several tiers */
  private val smallNodeSize = 200

  private def value(i: Int): Value = Value(Array.fill[Byte](40)(i.toByte))

  /** Values compare by identity, so compare their bytes */
  private def bytesOf(ov: Option[Value]): Option[List[Byte]] = ov.map(_.bytes.toList)

  private def keys(range: Range): List[Key] = range.toList.map(i => Key(i))

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

  /** An empty tree of `kind` whose root is hosted under `treeKey` */
  private def newRootManager(maxNodeSize: Int = smallNodeSize,
                             kind: TreeKind = TreeKind.DataObjects,
                             treeOrdering: KeyOrdering = ordering): Future[KVObjectRootManager] =
    for
      host <- rootHost()
      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId, maxNodeSize)
      tx = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, host, treeKey, treeOrdering, nodeAllocator, Map(), kind)(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      rm <- froot
    yield
      rm

  private def newTree(maxNodeSize: Int = smallNodeSize): Future[(DataObjectTKVL, KVObjectRootManager)] =
    for
      rm <- newRootManager(maxNodeSize)
      tree <- rm.getDataObjectTree()
    yield
      (tree, rm)

  /** Commits each update in its own transaction, one after another, without waiting for split and join
    * finalization in between: tier-0 updates never write an upper tier and a descent tolerates upper tiers that
    * finalization has not caught up with, so the tier-0 shape is the same either way. Everything is finalized
    * before this completes.
    */
  private def sequentially(ks: List[Key])(update: (Key, Transaction) => Future[Unit]): Future[Unit] =
    ks.foldLeft(Future.unit): (prev, k) =>
      prev.flatMap: _ =>
        val tx = client.newTransaction()
        for
          _ <- update(k, tx)
          _ <- tx.commit()
        yield
          ()
    .flatMap(_ => waitForTransactionsToComplete())

  private def setAll(tree: DataObjectTKVL, ks: List[Key]): Future[Unit] =
    sequentially(ks)((k, tx) => tree.set(k, value(k.bytes.last))(using tx))

  private def deleteAll(tree: DataObjectTKVL, ks: List[Key]): Future[Unit] =
    sequentially(ks)((k, tx) => tree.delete(k)(using tx))

  private def readNode(ptr: ObjectPointer): Future[ListNode] =
    DataObjectNodeEncoding.read(client, ptr, Key.AbsoluteMinimum, ordering, "test read")

  /** Every node of the tier starting at `head`, left to right */
  private def tierNodes(head: ListNode): Future[List[ListNode]] =
    head.link match
      case None => Future.successful(head :: Nil)
      case Some(l) => readNode(l.pointer).flatMap(tierNodes).map(head :: _)

  /** Every tier's nodes, top tier first. Follows each tier's AbsoluteMinimum entry down. */
  private def allTiers(rm: RootManager): Future[List[List[ListNode]]] =
    def down(tier: Int, head: ListNode): Future[List[List[ListNode]]] =
      tierNodes(head).flatMap: nodes =>
        if tier == 0 then
          Future.successful(nodes :: Nil)
        else
          readNode(ObjectPointer(head.get(Key.AbsoluteMinimum).get.bytes)).flatMap(down(tier - 1, _)).map(nodes :: _)

    rm.getRootNode().flatMap: (tier, _, oroot) =>
      oroot match
        case None => Future.successful(Nil)
        case Some(root) => down(tier, root)

  private def tier0Nodes(rm: RootManager): Future[List[ListNode]] = allTiers(rm).map(_.lastOption.getOrElse(Nil))

  private def numTiers(rm: RootManager): Future[Int] = rm.getRootNode().map(_._1)

  private def dataPointer(p: ObjectPointer): DataObjectPointer = p.asInstanceOf[DataObjectPointer]

  private def entriesOf(tree: DataObjectTKVL): Future[List[(Key, List[Byte])]] =
    tree.foldLeft(List[(Key, List[Byte])]()): (acc, m) =>
      acc ++ m.toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0).map((k, v) => k -> v.bytes.toList)

  private def visitsOf(walk: ((Key, Value) => Future[Unit]) => Future[Unit]): Future[List[Key]] =
    var visited = List[Key]()
    val walked = walk: (k, _) =>
      visited = k :: visited
      Future.unit
    walked.map(_ => visited.reverse)

  // ---------------------------------------------------------------------------------------------------------------
  // Creation and kind

  atest("an empty DataObject tree reads nothing, deletes nothing and walks nothing"):
    for
      (tree, rm) <- newTree()
      root <- rm.getRoot()
      v <- tree.get(Key(1))
      tx = client.newTransaction()
      _ <- tree.delete(Key(1))(using tx)
      folded <- tree.foldLeft(0)((n, m) => n + m.size)
      visited <- visitsOf(tree.foreach)
      from <- visitsOf(tree.foreachFrom(Key(0), _))
      range <- visitsOf(tree.foreachInRange(Key(0), Key(10), _))
    yield
      assert(root.kind == TreeKind.DataObjects)
      assert(root.orootObject.isEmpty)
      assert(v.isEmpty)
      assert(tx.isEmpty)
      assert(folded == 0)
      assert(visited.isEmpty && from.isEmpty && range.isEmpty)

  atest("Root.create and createNewTree with initial content allocate a DataObject node"):
    val initial = Map(Key(1) -> value(1), Key(2) -> value(2))
    for
      host <- rootHost()
      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId, smallNodeSize)
      tx = client.newTransaction()
      root <- Root.create(client, ordering, nodeAllocator, initial, TreeKind.DataObjects)(using tx)
      froot <- KVObjectRootManager.createNewTree(client, host, treeKey, ordering, nodeAllocator, initial,
        TreeKind.DataObjects)(using tx)
      _ <- tx.commit()
      rm <- froot
      tree <- rm.getDataObjectTree()
      created <- rm.getRoot()
      direct <- readNode(root.orootObject.get)
      v1 <- tree.get(Key(1))
      v2 <- tree.get(Key(2))
    yield
      assert(root.kind == TreeKind.DataObjects)
      assert(root.orootObject.get.isInstanceOf[DataObjectPointer])
      assert(direct.entries.map(_._1) == keys(1 to 2))
      assert(created.kind == TreeKind.DataObjects)
      assert(created.orootObject.get.isInstanceOf[DataObjectPointer])
      assert(bytesOf(v1) == bytesOf(Some(value(1))))
      assert(bytesOf(v2) == bytesOf(Some(value(2))))

  atest("opening a KeyValue-object tree as a DataObjectTKVL fails with WrongTreeKind"):
    for
      rm <- newRootManager(kind = TreeKind.KeyValueObjects)
      openErr <- rm.getDataObjectTree().failed
      direct = new DataObjectTKVL(client, rm)
      getErr <- direct.get(Key(1)).failed
      tx = client.newTransaction()
      setErr <- direct.set(Key(1), value(1))(using tx).failed
      walkErr <- direct.foreach((_, _) => Future.unit).failed
    yield
      assert(openErr == WrongTreeKind(TreeKind.DataObjects, TreeKind.KeyValueObjects))
      assert(getErr == WrongTreeKind(TreeKind.DataObjects, TreeKind.KeyValueObjects))
      assert(setErr == WrongTreeKind(TreeKind.DataObjects, TreeKind.KeyValueObjects))
      assert(walkErr == WrongTreeKind(TreeKind.DataObjects, TreeKind.KeyValueObjects))
      assert(tx.isEmpty)

  // ---------------------------------------------------------------------------------------------------------------
  // set / get / delete

  atest("set, get, overwrite and delete on a single node"):
    for
      (tree, rm) <- newTree(4096)
      _ <- setAll(tree, keys(1 to 3))
      v2 <- tree.get(Key(2))
      tx = client.newTransaction()
      _ <- tree.set(Key(2), value(99))(using tx)
      _ <- tx.commit()
      v2b <- tree.get(Key(2))
      _ <- deleteAll(tree, List(Key(1)))
      v1 <- tree.get(Key(1))
      tx2 = client.newTransaction()
      _ <- tree.delete(Key(42))(using tx2)
      entries <- entriesOf(tree)
      nodes <- tier0Nodes(rm)
    yield
      assert(bytesOf(v2) == bytesOf(Some(value(2))))
      assert(bytesOf(v2b) == bytesOf(Some(value(99))))
      assert(v1.isEmpty)
      assert(tx2.isEmpty)
      assert(entries == List(Key(2) -> value(99).bytes.toList, Key(3) -> value(3).bytes.toList))
      assert(nodes.size == 1)
      assert(nodes.head.pointer.isInstanceOf[DataObjectPointer])

  atest("enough keys split nodes and grow tiers, and every key reads back in order"):
    val all = keys(1 to 24)
    for
      (tree, rm) <- newTree()
      _ <- setAll(tree, all)
      tiers <- allTiers(rm)
      n <- numTiers(rm)
      values <- Future.sequence(all.map(tree.get))
      missing <- tree.get(Key(25))
      entries <- entriesOf(tree)
      visited <- visitsOf(tree.foreach)
    yield
      assert(n >= 2)
      assert(tiers.size == n + 1)
      assert(tiers.flatten.forall(_.pointer.isInstanceOf[DataObjectPointer]))
      assert(tiers.last.size > 2)
      assert(values.map(bytesOf) == all.map(k => bytesOf(Some(value(k.bytes.last)))))
      assert(missing.isEmpty)
      assert(entries == all.map(k => k -> value(k.bytes.last).bytes.toList))
      assert(visited == all)

  // Ascending deletes empty the leftmost tier-0 node again and again, so every join absorbs its right neighbour into
  // it, and each absorbed node's entry leaves the tier above (see TierEngineDescentSuite for lookups that then pass
  // through an upper-tier node whose first entries are gone).

  atest("deleting keys joins nodes across a multi-tier tree"):
    val all = keys(1 to 24)
    val (deleted, kept) = all.splitAt(21)
    for
      (tree, rm) <- newTree()
      _ <- setAll(tree, all)
      before <- tier0Nodes(rm)
      tiersBefore <- numTiers(rm)
      _ <- deleteAll(tree, deleted)
      after <- tier0Nodes(rm)
      entries <- entriesOf(tree)
      remaining <- Future.sequence(before.map(n => client.readOptional(dataPointer(n.pointer))))
      values <- Future.sequence(all.map(tree.get))
    yield
      assert(tiersBefore >= 2)
      assert(after.size < before.size)
      // Every node that was joined into its left neighbour is deleted
      assert(remaining.count(_.isDefined) == after.size)
      assert(after.map(_.pointer).toSet.subsetOf(before.map(_.pointer).toSet))
      assert(entries.map(_._1) == kept)
      assert(values.map(_.isDefined) == all.map(kept.contains))

  // Tier reduction only ever goes from tier 1 to tier 0: the leftmost upper-tier node keeps its AbsoluteMinimum entry
  // and never empties, so a root above tier 1 never drops to the two entries a reduction needs.
  atest("deleting every key of a two-tier tree reduces it to a single empty node"):
    val all = keys(1 to 9)
    for
      (tree, rm) <- newTree()
      _ <- setAll(tree, all)
      before <- tier0Nodes(rm)
      tiersBefore <- numTiers(rm)
      _ <- deleteAll(tree, all)
      after <- allTiers(rm)
      tiersAfter <- numTiers(rm)
      visited <- visitsOf(tree.foreach)
    yield
      assert(tiersBefore == 1)
      assert(before.size > 1)
      assert(tiersAfter == 0)
      assert(after.flatten.size == 1)
      assert(after.flatten.head.entries.isEmpty)
      assert(visited.isEmpty)

  // ---------------------------------------------------------------------------------------------------------------
  // Walks

  atest("deleting every key of a lexical-key tree with varied key lengths leaves no empty tier-0 node behind"):
    // Left minimums often encode longer than their right neighbour's, which must not stop the joins
    val rnd = new Random(20261025L)
    val ks = List.fill(80)(Key(Array.fill(1 + rnd.nextInt(40))(('a' + rnd.nextInt(26)).toByte))).distinct
    for
      rm <- newRootManager(300, TreeKind.DataObjects, LexicalKeyOrdering)
      tree <- rm.getDataObjectTree()
      _ <- setAll(tree, ks)
      before <- tier0Nodes(rm)
      _ <- deleteAll(tree, rnd.shuffle(ks))
      after <- tier0Nodes(rm)
      remaining <- Future.sequence(ks.map(tree.get))
    yield
      assert(before.size > 5)
      assert(remaining.forall(_.isEmpty))
      assert(after.tail.forall(_.entries.nonEmpty))
      assert(after.size == 1)

  atest("foreachFrom and foreachInRange honor inclusive and exclusive bounds on node edges"):
    val all = keys(1 to 24)
    for
      (tree, rm) <- newTree()
      _ <- setAll(tree, all)
      nodes <- tier0Nodes(rm)
      // The minimum of the third tier-0 node: a key exactly on a node's left edge, and the right edge of the
      // node before it
      edge = nodes(2).minimum
      from <- visitsOf(tree.foreachFrom(edge, _))
      fromBelow <- visitsOf(tree.foreachFrom(Key(0), _))
      fromAbove <- visitsOf(tree.foreachFrom(Key(25), _))
      toEdge <- visitsOf(tree.foreachInRange(Key(1), edge, _))
      fromEdge <- visitsOf(tree.foreachInRange(edge, Key(25), _))
      inside <- visitsOf(tree.foreachInRange(Key(3), Key(7), _))
      empty <- visitsOf(tree.foreachInRange(edge, edge, _))
    yield
      assert(nodes.size > 3)
      assert(all.contains(edge))
      assert(from == all.filter(k => ordering.compare(k, edge) >= 0))
      assert(fromBelow == all)
      assert(fromAbove.isEmpty)
      assert(toEdge == all.filter(k => ordering.compare(k, edge) < 0))
      assert(fromEdge == from)
      assert(inside == keys(3 to 6))
      assert(empty.isEmpty)

  atest("a failing callback is logged and the walk continues, and a failing read fails the walk"):
    val all = keys(1 to 12)
    var visited = List[Key]()
    def record(k: Key, v: Value): Future[Unit] =
      visited = k :: visited
      if k == Key(3) then
        Future.failed(new Exception("fn failure"))
      else if k == Key(5) then
        throw new Exception("synchronous fn failure")
      else
        Future.unit

    for
      (tree, rm) <- newTree()
      _ <- setAll(tree, all)
      _ <- tree.foreach(record)
      visitedAll = visited.reverse
      nodes <- tier0Nodes(rm)
      last = nodes.last
      // Delete the last node without unlinking it, so any walk that reads it fails
      tx = client.newTransaction()
      _ = tx.setRefcount(last.pointer, last.refcount, last.refcount.decrement())
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      inRange <- visitsOf(tree.foreachInRange(Key(1), last.minimum, _))
      walkResult <- tree.foreach((_, _) => Future.unit).transform(Success(_))
      foldResult <- tree.foldLeft(0)((n, _) => n + 1).transform(Success(_))
    yield
      assert(visitedAll == all)
      assert(inRange == all.filter(k => ordering.compare(k, last.minimum) < 0))
      assert(walkResult.failed.get.isInstanceOf[InvalidObject])
      assert(foldResult.failed.get.isInstanceOf[InvalidObject])

  // ---------------------------------------------------------------------------------------------------------------
  // splitTree / deleteTree

  /** Splits a 24-key tree at key 12 into the original and a new tree hosted beside it */
  private def splitAt12(inclusive: Boolean): Future[(List[Key], List[Key], Root)] =
    val all = keys(1 to 24)
    for
      (tree, rm) <- newTree()
      _ <- setAll(tree, all)
      tx = client.newTransaction()
      newRoot <- tree.splitTree(Key(12), inclusive)(using tx)
      fnewRm = KVObjectRootManager.setNewRoot(client, rm.pointer, Key(Array[Byte](1)), true, newRoot)(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      newRm <- fnewRm
      newTree <- newRm.getDataObjectTree()
      left <- visitsOf(tree.foreach)
      right <- visitsOf(newTree.foreach)
      leftValues <- Future.sequence(all.map(tree.get))
      rightValues <- Future.sequence(all.map(newTree.get))
    yield
      assert(leftValues.map(_.isDefined) == all.map(left.contains))
      assert(rightValues.map(_.isDefined) == all.map(right.contains))
      (left, right, newRoot)

  atest("splitTree keeps keys below the split key on the left"):
    for
      (left, right, newRoot) <- splitAt12(false)
    yield
      assert(newRoot.kind == TreeKind.DataObjects)
      assert(newRoot.orootObject.get.isInstanceOf[DataObjectPointer])
      assert(left == keys(1 to 11))
      assert(right == keys(12 to 24))

  atest("inclusive splitTree also keeps the split key on the left"):
    for
      (left, right, _) <- splitAt12(true)
    yield
      assert(left == keys(1 to 12))
      assert(right == keys(13 to 24))

  private def deleteTreeRemovesEveryNode(withCallback: Boolean): Future[(Set[Key], List[Key])] =
    val all = keys(1 to 24)
    var deleted = Set[Key]()
    val onEntry: (Key, Value) => Future[Unit] = (k, _) =>
      synchronized { deleted = deleted + k }
      Future.unit
    for
      (tree, rm) <- newTree()
      _ <- setAll(tree, all)
      nodes <- allTiers(rm).map(_.flatten)
      _ <- tree.deleteTree(if withCallback then Some(onEntry) else None)
      remaining <- Future.sequence(nodes.map(n => client.readOptional(dataPointer(n.pointer))))
      (tier, _, oroot) <- rm.getRootNode()
      v <- tree.get(Key(1))
    yield
      assert(nodes.size > 5)
      assert(remaining.forall(_.isEmpty))
      assert(tier == 0)
      assert(oroot.isEmpty)
      assert(v.isEmpty)
      (deleted, all)

  atest("deleteTree removes every node object and hands each entry to the callback"):
    deleteTreeRemovesEveryNode(true).map: (deleted, all) =>
      assert(deleted == all.toSet)

  atest("deleteTree without a callback removes every node object"):
    deleteTreeRemovesEveryNode(false).map: (deleted, _) =>
      assert(deleted.isEmpty)

  // ---------------------------------------------------------------------------------------------------------------
  // Same-node guard

  private def alreadyModified(err: Throwable, node: ListNode): Boolean = err match
    case NodeAlreadyModified(p) => p == node.pointer
    case _ => false

  atest("two sets hitting the same node in one transaction fail with NodeAlreadyModified"):
    for
      (tree, rm) <- newTree(4096)
      _ <- setAll(tree, keys(1 to 3))
      nodes <- tier0Nodes(rm)
      tx = client.newTransaction()
      _ <- tree.set(Key(4), value(4))(using tx)
      err <- tree.set(Key(5), value(5))(using tx).failed
      deleteErr <- tree.delete(Key(1))(using tx).failed
      _ <- tx.commit()
      v4 <- tree.get(Key(4))
      v5 <- tree.get(Key(5))
      v1 <- tree.get(Key(1))
    yield
      assert(nodes.size == 1)
      assert(alreadyModified(err, nodes.head))
      assert(alreadyModified(deleteErr, nodes.head))
      assert(v4.isDefined)
      assert(v5.isEmpty)
      assert(v1.isDefined)

  atest("the guard does not trip for a split's newly allocated node or for sequential transactions"):
    // How many entries fill one node
    val perNode = (smallNodeSize - DataObjectNodeEncoding.nodeOverhead(Key.AbsoluteMinimum, None)) /
      DataObjectNodeEncoding.entrySize(Key(1), value(1))
    for
      (tree, rm) <- newTree()
      // Sequential transactions, each rewriting the same single node until it is full
      _ <- setAll(tree, keys(1 to perNode))
      before <- tier0Nodes(rm)
      // One more key overflows the node: the transaction rewrites it and allocates the new right node
      tx = client.newTransaction()
      _ <- tree.set(Key(perNode + 1), value(perNode + 1))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      after <- tier0Nodes(rm)
      visited <- visitsOf(tree.foreach)
    yield
      assert(before.size == 1)
      assert(after.size == 2)
      assert(visited == keys(1 to perNode + 1))



  atest("two sets on an empty tree in one transaction fail with RootAlreadyModified and abort it"):
    for
      (tree, rm) <- newTree()
      tx = client.newTransaction()
      _ <- tree.set(Key(1), value(1))(using tx)
      err <- tree.set(Key(2), value(2))(using tx).failed
      txErr <- tx.result.failed
      root <- rm.getRoot()
      v1 <- tree.get(Key(1))
      _ <- setAll(tree, List(Key(3)))
      v3 <- tree.get(Key(3))
    yield
      assert(err == RootAlreadyModified(rm.pointer))
      // The aborted transaction commits neither initial node, so neither allocation is left unreferenced
      assert(txErr == err)
      assert(root.orootObject.isEmpty)
      assert(v1.isEmpty)
      assert(bytesOf(v3) == bytesOf(Some(value(3))))
