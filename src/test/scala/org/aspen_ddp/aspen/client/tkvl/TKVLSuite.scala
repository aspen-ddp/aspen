package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{InvalidObject, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, IntegerKeyOrdering, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, Value}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState

import scala.concurrent.Future
import scala.util.Success
import scala.language.implicitConversions

class TKVLSuite extends IntegrationTestSuite {
  atest("Create new tree") {
    val treeKey = Key(Array[Byte](1))
    val key = Key(Array[Byte](2))
    val value = Value(Array[Byte](3))

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))(using tx1)

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      v <- tree.get(key)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (3)
    }
  }

  atest("Insert into tree") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](3))

    val key2 = Key(Array[Byte](4))

    val value2 = Value(new Array[Byte](512*1024))

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))(using tx1)

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      tx = client.newTransaction()
      _ <- tree.set(key2, value2)(using tx)
      r <- tx.commit()
      _ <- waitForTransactionsToComplete()

      v <- tree.get(key2)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (512*1024)
    }
  }

  atest("Many inserts and foreach") {
    val treeKey = Key(Array[Byte](0))
    val value = Value(Array[Byte](3))
    var insertedKeys = Set[Key]()
    var readKeys = Set[Key]()

    def insert(tree: TieredKeyValueList, key: Key): Future[Unit] =
      val tx: Transaction = client.newTransaction()
      tree.set(key, value)(using tx).map: _ =>
        tx.commit().map: _ =>
          insertedKeys = insertedKeys + key

    def foreachKV(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      readKeys = readKeys + key
      Future.unit

    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, IntegerKeyOrdering, nodeAllocator, Map())(using tx1)

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      _ <- Future.sequence((1 to 100).map(i => insert(tree, Key(i))))

      _ <- tree.foreach(foreachKV)

    yield
      readKeys should be (insertedKeys)
  }

  atest("Splitting tree insertion") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](3))

    val key2 = Key(Array[Byte](4))
    val value2 = Value(new Array[Byte](512*1024))

    val key3 = Key(Array[Byte](5))
    val value3 = Value(new Array[Byte](512*1024))

    for {
      ikvos <- client.read(radicle)

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))(using tx1)

      r <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      tx = client.newTransaction()
      _ <- tree.set(key2, value2)(using tx)
      r <- tx.commit()

      tx = client.newTransaction()
      _ <- tree.set(key3, value3)(using tx)
      r <- tx.commit()

      _ <- waitForTransactionsToComplete()

      tree <- root.getTree()
      v <- tree.get(key3)
      (numTiers, _, _) <- tree.rootManager.getRootNode()

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (512*1024)
      numTiers should be (1)
    }
  }

  atest("deleteTree") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](3))

    val key2 = Key(Array[Byte](4))
    val value2 = Value(new Array[Byte](512 * 1024))

    val key3 = Key(Array[Byte](5))
    val value3 = Value(new Array[Byte](512 * 1024))

    var deletedKeys = Set[Key]()

    def deleteKV(key: Key, vs: ValueState): Future[Unit] =
      deletedKeys = deletedKeys + key
      Future.unit

    for {
      ikvos <- client.read(radicle)

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))(using tx1)

      r <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      tx = client.newTransaction()
      _ <- tree.set(key2, value2)(using tx)
      r <- tx.commit()

      tx = client.newTransaction()
      _ <- tree.set(key3, value3)(using tx)
      r <- tx.commit()

      // Wait for background transactions complete since the tree is updated
      // Asynchronously in the background
      _ <- waitForTransactionsToComplete()

      tree <- root.getTree()
      v <- tree.get(key3)
      (numTiers, _, _) <- tree.rootManager.getRootNode()
      
      _ <- tree.deleteTree(Some(deleteKV))
      
      (numTiersAfterDelete, _, rootAfterDelete) <- tree.rootManager.getRootNode()

    } yield {
      v.isEmpty should be(false)
      val vs = v.get
      vs.value.bytes.length should be(512 * 1024)
      numTiers should be(1)

      deletedKeys should be(Set(key, key2, key3))
      numTiersAfterDelete should be (0)
      rootAfterDelete.isEmpty should be (true)
    }
  }

  atest("createInitialNode refuses to replace an existing root") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val key2 = Key(Array[Byte](2))
    val key3 = Key(Array[Byte](3))
    val value = Value(Array[Byte](3))

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()
      _ <- tree.deleteTree(None)
      (_, _, emptyRoot) <- tree.rootManager.getRootNode()

      // Recreate the initial node through the empty-tree path of set
      tx2 = client.newTransaction()
      _ <- tree.set(key2, value, requirement = Some(Left(true)))(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      // A second, late attempt must not replace the root created above
      tx3 = client.newTransaction()
      err <- tree.rootManager.createInitialNode(Map(key3 -> value))(using tx3).failed
      txErr <- tx3.result.failed

      v2 <- tree.get(key2)
      v3 <- tree.get(key3)
    } yield {
      emptyRoot.isEmpty should be (true)
      err.getMessage should be ("Initial TKVL node already exists.")
      txErr should be theSameInstanceAs err
      v2.isEmpty should be (false)
      v3.isEmpty should be (true)
    }
  }

  atest("Joining tree deletion with tier reduction") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](3))

    val key2 = Key(Array[Byte](4))
    val value2 = Value(new Array[Byte](512*1024))

    val key3 = Key(Array[Byte](7))
    val value3 = Value(new Array[Byte](512*1024))

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))(using tx1)

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      tx = client.newTransaction()
      _ <- tree.set(key2, value2)(using tx)
      r <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- waitForTransactionsToComplete()

      tx = client.newTransaction()
      _ <- tree.set(key3, value3)(using tx)
      r <- tx.commit()

      _ <- waitForTransactionsToComplete()

      tx = client.newTransaction()
      _ <- tree.delete(key2)(using tx)

      r <- tx.commit()

      _ <- waitForTransactionsToComplete()

      tx = client.newTransaction()
      _ <- tree.delete(key)(using tx)
      _ <- tx.commit()

      _ <- waitForTransactionsToComplete()

      tree <- root.getTree()
      v <- tree.get(key3)
      (numTiers, _, _) <- tree.rootManager.getRootNode()

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (512*1024)
      numTiers should be (0)
    }
  }

  /** Builds a tier-0 list guaranteed to span several nodes.
   *
   *  Splits are forced with oversized values, the same lever the "Splitting tree insertion"
   *  test uses. Inserts are sequential rather than concurrent so the node boundaries are
   *  deterministic from run to run.
   *
   *  `valueSize` sets how many keys fit in a node against the 1 MiB tier-0 budget. The default
   *  gives one key per node, which is what most of these tests want. A caller that needs nodes
   *  holding several keys - to place a key strictly inside a node rather than at its edge -
   *  passes something smaller. No test should assume a particular layout either way; derive it
   *  from `tier0NodeMinimums`.
   *
   *  Returns the tree, its root manager (needed to re-read the root after splits), and the
   *  inserted keys in ascending order.
   */
  private def buildSplitTree(numKeys: Int,
                             valueSize: Int = 512 * 1024): Future[(TieredKeyValueList, KVObjectRootManager, List[Key])] =
    val treeKey = Key(Array[Byte](0))
    val bigValue = Value(new Array[Byte](valueSize))
    val keys = (1 to numKeys).map(i => Key(i)).toList

    def insertSequentially(tree: TieredKeyValueList, remaining: List[Key]): Future[Unit] =
      remaining match
        case Nil => Future.unit
        case key :: rest =>
          val tx: Transaction = client.newTransaction()
          for
            _ <- tree.set(key, bigValue)(using tx)
            _ <- tx.commit()
            _ <- waitForTransactionsToComplete()
            _ <- insertSequentially(tree, rest)
          yield ()

    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, IntegerKeyOrdering,
        nodeAllocator, Map())(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()
      _ <- insertSequentially(tree, keys)

      // Re-read: splits replace the root, so the tree handle from before the inserts is stale.
      freshTree <- root.getTree()
    yield
      (freshTree, root, keys)

  /** The minimum key of every tier-0 node, in left-to-right list order.
   *
   *  Read out of the tree's own right pointers rather than reproduced from the split
   *  arithmetic, so the boundary tests below keep covering what they claim if that arithmetic
   *  ever shifts. Deliberately avoids the foreach variants: these are the fixtures those
   *  walks get checked against.
   */
  private def tier0NodeMinimums(tree: TieredKeyValueList): Future[List[Key]] =
    tier0Nodes(tree).map(_.map(_.minimum))

  /** Every tier-0 node, in left-to-right list order. See tier0NodeMinimums. */
  private def tier0Nodes(tree: TieredKeyValueList): Future[List[KeyValueListNode]] =

    def walkRight(node: KeyValueListNode): Future[List[KeyValueListNode]] =
      node.tail match
        case None => Future.successful(node :: Nil)
        case Some(nodeTail) =>
          client.read(nodeTail.pointer).flatMap: kvos =>
            walkRight(KeyValueListNode(client, nodeTail, node.ordering, kvos))
              .map(node :: _)

    for
      (tier, ordering, oroot) <- tree.rootManager.getKeyValueRootNode()
      root = oroot.getOrElse(throw new BrokenTree())
      e <- TieredKeyValueList.fetchContainingNode(client, tier, 0, ordering,
             Key.AbsoluteMinimum, root, Set())
      leftmost = e match
        case Left(_) => throw new BrokenTree()
        case Right(n) => n
      nodes <- walkRight(leftmost)
    yield
      nodes

  /** Hands out `stale` as a root of `staleTier` on the first getRootNode call, then defers to
   *  `real`.
   *
   *  Reproduces a client that read the root just before a join committed, so the races below
   *  are deterministic.
   */
  private class StaleOnceRootManager(real: RootManager,
                                     stale: KeyValueListNode,
                                     staleTier: Int = 0) extends RootManager:
    private var served = false
    def getRoot(): Future[Root] = real.getRoot()
    def getRootNode(): Future[(Int, KeyOrdering, Option[KeyValueListNode])] =
      if served then
        real.getKeyValueRootNode()
      else
        served = true
        Future.successful((staleTier, stale.ordering, Some(stale)))
    def createInitialNode(contents: Map[Key, Value])(using tx: Transaction): Future[Unit] =
      real.createInitialNode(contents)
    def getAllocatorForTier(tier: Int): Future[ObjectAllocator] =
      real.getAllocatorForTier(tier)
    def getMaxNodeSize(tier: Int): Future[Int] = real.getMaxNodeSize(tier)
    def prepareRootUpdate(newTier: Int, onewRoot: Option[ObjectPointer])
                         (using tx: Transaction): Future[Unit] = real.prepareRootUpdate(newTier, onewRoot)
    def encode(): Array[Byte] = real.encode()
    def typeId: RootManagerTypeId = real.typeId

  /** Deletes key 1 from a one-key-per-node tree, joining the leftmost node with its right
   *  neighbour, and returns the leftmost node as read before the join.
   */
  private def joinAfterSnapshot(tree: TieredKeyValueList): Future[KeyValueListNode] =
    for
      nodes <- tier0Nodes(tree)
      tx = client.newTransaction()
      _ <- tree.delete(Key(1))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
    yield
      nodes.head

  atest("A scan restarts when a racing join deletes the next node") {
    for
      (tree, root, keys) <- buildSplitTree(3)
      stale <- joinAfterSnapshot(tree)
      staleTree = new TieredKeyValueList(client, new StaleOnceRootManager(root, stale))
      v <- staleTree.get(Key(3))
    yield
      // The stale leftmost node still points at the node the join deleted
      stale.tail.map(_.minimum) should be (Some(Key(2)))
      v.isEmpty should be (false)
  }

  atest("A delete restarts when a racing join deletes the node it would join with") {
    for
      (tree, root, keys) <- buildSplitTree(3)
      stale <- joinAfterSnapshot(tree)
      staleTree = new TieredKeyValueList(client, new StaleOnceRootManager(root, stale))
      // The stale node still holds key 1 alone, so deleting it reads the deleted neighbour to join
      _ <- staleTree.delete(Key(1))(using client.newTransaction())
      v1 <- tree.get(Key(1))
      v2 <- tree.get(Key(2))
    yield
      v1 should be (None)
      v2.isEmpty should be (false)
  }

  atest("A scan restart falls back a tier when tier reduction deleted the root") {
    for
      (tree, root, keys) <- buildSplitTree(3)
      (numTiers, ordering, oroot) <- root.getKeyValueRootNode()
      realRoot = oroot.getOrElse(throw new BrokenTree())
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      // deadRoot stands in for a root that tier reduction deleted. x stands in for a node read
      // just before a join deleted its right neighbour, deadRight.
      tx0 = client.newTransaction()
      _ = tx0.lockRevision(radicle, ikvos.revision)
      deadRoot <- alloc.allocateKeyValueObject(Map[Key, Value]())(using tx0)
      deadRight <- alloc.allocateKeyValueObject(Map[Key, Value]())(using tx0)
      x <- alloc.allocateKeyValueObject(Map(Key(2) -> Value(Array[Byte](2))),
             right = Some(Value(KeyValueListPointer(Key(3), deadRight).toArray)))(using tx0)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      rootState <- client.read(deadRoot)
      rightState <- client.read(deadRight)
      tx1 = client.newTransaction()
      _ = tx1.setRefcount(deadRoot, rootState.refcount, rootState.refcount.decrement())
      _ = tx1.setRefcount(deadRight, rightState.refcount, rightState.refcount.decrement())
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      // The descent for key 3 picks x, scans right into deadRight, and must then restart
      // without refreshing deadRoot. Its leftmost child is the real tier-0 list.
      leftmost = realRoot.contents(Key.AbsoluteMinimum)
      stale = new KeyValueListNode(client, realRoot.ida, deadRoot, ordering, Key.AbsoluteMinimum,
        realRoot.revision, realRoot.refcount,
        Map(Key.AbsoluteMinimum -> leftmost, Key(2) -> leftmost.copy(value = Value(x.toArray))), None)
      staleTree = new TieredKeyValueList(client, new StaleOnceRootManager(root, stale, numTiers))
      v <- staleTree.get(Key(3))
    yield
      numTiers should be (1)
      v.isEmpty should be (false)
  }

  atest("A scan into a permanently deleted node fails rather than hanging") {
    for
      (tree, root, keys) <- buildSplitTree(3)
      (numTiers, _, _) <- root.getRootNode()
      nodes <- tier0Nodes(tree)
      last = nodes.last
      // Delete the last node without unlinking it, so the descent falls back to its left
      // neighbour and scans right into it on every attempt
      tx = client.newTransaction()
      _ = tx.setRefcount(last.pointer, last.refcount, last.refcount.decrement())
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      result <- tree.get(Key(3)).transform(Success(_))
    yield
      numTiers should be >= 1
      result.failed.get shouldBe a [InvalidObject]
  }

  atest("splitTree splits every tier of a multi-tier tree") {
    val newTreeKey = Key(Array[Byte](1))
    val splitAtKey = Key(3)
    for
      (tree, root, keys) <- buildSplitTree(4)
      (numTiers, ordering, _) <- root.getRootNode()

      splitTx = client.newTransaction()
      newRoot <- tree.splitTree(splitAtKey)(using splitTx)
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, root.pointer, newTreeKey, true, newRoot)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      newTreeRoot <- fnewTreeRoot
      newTree <- newTreeRoot.getTree()
      (_, _, ooldRoot) <- root.getKeyValueRootNode()
      oldValues <- Future.sequence(keys.map(tree.get))
      newValues <- Future.sequence(keys.map(newTree.get))
    yield
      numTiers should be >= 1
      newRoot.tier should be (numTiers)
      oldValues.map(_.isDefined) should be (List(true, true, false, false))
      newValues.map(_.isDefined) should be (List(false, false, true, true))
      // The original tree's upper tier must not keep entries for nodes that moved to the new tree
      ooldRoot.get.contents.keys.filter(k => ordering.compare(k, splitAtKey) >= 0) should be (empty)
  }

  atest("foreach visits every key exactly once on a split tree") {
    var visits = List[(Key, Boolean)]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      // keyInRange is the check that catches the wrong-node pairing: a key handed to the
      // caller with a node that does not own it is exactly the bug.
      visits = (key, node.keyInRange(key)) :: visits
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      _ <- tree.foreach(record)
    yield
      // Guard the guard: if the tree never split, this test proves nothing.
      numTiers should be >= 1

      // visits is built by prepending, so reversing it recovers visit order. keys is ascending
      // by construction, so asserting the exact ordered list also pins the ascending-order
      // invariant that crash-resume leans on: a consumer checkpointing "last restored key = K"
      // and resuming at foreachFrom(K) silently drops anything a non-monotonic walk delivered
      // after K. This subsumes the set-equality and length checks it replaces.
      visits.reverse.map(_._1) should be (keys)
      visits.filterNot(_._2) should be (Nil)
  }

  // Covers key visitation and pairing across node boundaries. It cannot observe the old
  // termination defect. That guard compared maxKey against a bare `minimum`, which bound to the
  // class field - the minimum of the node the walk started on, constant for the whole recursion.
  // Reached through the tiered wrapper the walk starts at fetchContainingNode(minKey), so
  // minimum <= minKey <= maxKey always held and the guard never fired: every remaining tier-0
  // node was read through to the end of the list on every range query. Zero extra keys reached
  // fn, so nothing is visible through the fn seam; catching it requires read-count
  // instrumentation. The one symptom observable without instrumentation was an unreadable node
  // past the range failing the returned future for a range that never touched it.
  atest("foreachInRange spans node boundaries and honors the half-open range") {
    var visits = List[(Key, Boolean)]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visits = (key, node.keyInRange(key)) :: visits
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      // Keys are 1..8 across several nodes; [3, 7) must yield exactly 3, 4, 5, 6 regardless
      // of where the node boundaries happen to fall.
      _ <- tree.foreachInRange(Key(3), Key(7), record)
    yield
      numTiers should be >= 1

      // Ascending order matters here for the same reason it does for foreachFrom: a caller
      // that stops partway and resumes from the last key it saw is only safe on a walk that
      // never goes backwards.
      visits.reverse.map(_._1) should be (List(Key(3), Key(4), Key(5), Key(6)))
      visits.filterNot(_._2) should be (Nil)
  }

  atest("foreachFrom resumes from a key inside a later node") {
    var visits = List[(Key, Boolean)]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visits = (key, node.keyInRange(key)) :: visits
      Future.unit

    for
      // Smaller values so nodes hold several keys each; at the default size every node holds
      // exactly one key and there is no "inside" for a key to be.
      (tree, root, keys) <- buildSplitTree(12, 128 * 1024)
      (numTiers, _, _) <- root.getRootNode()
      mins <- tier0NodeMinimums(tree)

      // Derived from the tree rather than hardcoded: the first key that lives in a node other
      // than the leftmost one AND sits strictly above that node's minimum. That is precisely
      // what "inside a later node" means, and picking it this way keeps the claim true if the
      // split arithmetic ever shifts.
      nodeMinimumOf = (k: Key) => mins.filter(m => IntegerKeyOrdering.compare(m, k) <= 0).last
      oresumeKey = keys.find: k =>
        val m = nodeMinimumOf(k)
        m != mins.head && IntegerKeyOrdering.compare(m, k) < 0

      // Assert the premise rather than assume it.
      _ = withClue(s"tier-0 node minimums: $mins - "):
            oresumeKey should not be (None)

      resumeKey = oresumeKey.get
      _ <- tree.foreachFrom(resumeKey, record)
    yield
      numTiers should be >= 1

      val expected = keys.filter(k => IntegerKeyOrdering.compare(k, resumeKey) >= 0)
      visits.reverse.map(_._1) should be (expected)
      visits.filterNot(_._2) should be (Nil)
  }

  atest("foreachFrom resumes from a key exactly on a node boundary") {
    var visits = List[(Key, Boolean)]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visits = (key, node.keyInRange(key)) :: visits
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      mins <- tier0NodeMinimums(tree)

      // The awkward descent: minKey is exactly a node's minimum, so the node to its left
      // reports keyInRange(minKey) == false because minKey is that node's maximum. The walk
      // has to land on the right of the two adjacent nodes.
      _ = withClue(s"tier-0 node minimums: $mins - "):
            mins.length should be >= 2

      boundaryKey = mins(1)
      _ <- tree.foreachFrom(boundaryKey, record)
    yield
      numTiers should be >= 1

      val expected = keys.filter(k => IntegerKeyOrdering.compare(k, boundaryKey) >= 0)
      expected should not be (Nil)
      visits.reverse.map(_._1) should be (expected)
      visits.filterNot(_._2) should be (Nil)
  }

  atest("foreachFrom below the first key visits the whole tree") {
    var visitedKeys = List[Key]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visitedKeys = key :: visitedKeys
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      _ <- tree.foreachFrom(Key(0), record)
    yield
      numTiers should be >= 1

      visitedKeys.reverse should be (keys)
  }

  atest("foreachFrom above the last key visits nothing") {
    var visitedKeys = List[Key]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visitedKeys = key :: visitedKeys
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      // Reachable in production when the checkpointed key is deleted between the crash and the
      // resume. The filter empties the last node, its tail is None, and the walk completes
      // successfully having visited nothing.
      _ <- tree.foreachFrom(Key(keys.length + 1), record)
    yield
      numTiers should be >= 1
      visitedKeys should be (Nil)
  }
}
