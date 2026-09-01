package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, IntegerKeyOrdering, Key, Value}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState

import scala.concurrent.Future
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
   *  Splits are forced with 512 KiB values, the same lever the "Splitting tree insertion"
   *  test uses. Inserts are sequential rather than concurrent so the node boundaries are
   *  deterministic from run to run.
   *
   *  Returns the tree, its root manager (needed to re-read the root after splits), and the
   *  inserted keys in ascending order.
   */
  private def buildSplitTree(numKeys: Int): Future[(TieredKeyValueList, KVObjectRootManager, List[Key])] =
    val treeKey = Key(Array[Byte](0))
    val bigValue = Value(new Array[Byte](512 * 1024))
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

      val visitedKeys = visits.map(_._1)
      visitedKeys.distinct.sortBy(k => keys.indexOf(k)) should be (keys)
      visitedKeys.length should be (keys.length)
      visits.filterNot(_._2) should be (Nil)
  }

  // Covers key visitation and pairing across node boundaries. Cannot observe the
  // termination guard (KeyValueListNode.scala:219): an extra read beyond the range
  // contributes zero keys after filtering. Detecting that requires read-count instrumentation.
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

      val visitedKeys = visits.map(_._1)
      visitedKeys.length should be (4)
      visitedKeys.toSet should be (Set(Key(3), Key(4), Key(5), Key(6)))
      visits.filterNot(_._2) should be (Nil)
  }
}
