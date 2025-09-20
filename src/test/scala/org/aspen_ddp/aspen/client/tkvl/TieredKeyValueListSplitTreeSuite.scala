package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.*

class TieredKeyValueListSplitTreeSuite extends IntegrationTestSuite {

  def createKey(byte: Byte): Key = Key(Array(byte))
  def createValue(byte: Byte): Value = Value(Array(byte))

  test("splitTree - simple single key case") {
    val treeKey = createKey(0)
    val key1 = createKey(1)
    val value1 = createValue(10)
    val splitAtKey = createKey(5)

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Now split the tree
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      // Test access to original tree
      originalValue <- tree.get(key1)

    } yield {
      // Original tree should have the key since 1 < 5
      originalValue.isDefined should be (true)
      originalValue.get.value.bytes(0) should be (10)
    }
  }

  test("splitTree - key should move to new tree") {
    val treeKey = createKey(0)
    val newTreeKey = createKey(1)
    val key1 = createKey(10)  // This key is >= splitAtKey
    val value1 = createValue(100)
    val splitAtKey = createKey(5)

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Now split the tree
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, ptr, newTreeKey, true, newRootPtr,
        ByteArrayKeyOrdering, nodeAllocator)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      /*
      setTx = client.newTransaction()
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, ptr, newTreeKey, true, newRootPtr,
        ByteArrayKeyOrdering, nodeAllocator)(using setTx)
      _ <- setTx.commit()
      _ <- waitForTransactionsToComplete()
       */

      newTreeRoot <- fnewTreeRoot
      newTree <- newTreeRoot.getTree()

      // Test access to original tree - should be empty now
      originalValue <- tree.get(key1)

      newValue <- newTree.get(key1)

    } yield {
      // Original tree should NOT have the key since 10 >= 5 (moved to new tree)
      originalValue should be (None)

      newValue.isDefined should be(true)
      newValue.get.value.bytes(0) should be(100)
    }
  }

  test("splitTree - multiple keys distributed correctly") {
    val treeKey = createKey(0)
    val newTreeKey = createKey(1)
    val key1 = createKey(1)
    val key2 = createKey(3)
    val key3 = createKey(7)
    val key4 = createKey(9)
    val value1 = createValue(10)
    val value2 = createValue(30)
    val value3 = createValue(70)
    val value4 = createValue(90)
    val splitAtKey = createKey(5)

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Insert additional keys
      tx2 = client.newTransaction()
      _ <- tree.set(key2, value2)(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      tx3 = client.newTransaction()
      _ <- tree.set(key3, value3)(using tx3)
      _ <- tx3.commit()
      _ <- waitForTransactionsToComplete()

      tx4 = client.newTransaction()
      _ <- tree.set(key4, value4)(using tx4)
      _ <- tx4.commit()
      _ <- waitForTransactionsToComplete()

      // Now split the tree at key 5
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, ptr, newTreeKey, true, newRootPtr,
        ByteArrayKeyOrdering, nodeAllocator)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      newTreeRoot <- fnewTreeRoot
      newTree <- newTreeRoot.getTree()

      // Test access to original tree - should have keys 1,3
      originalValue1 <- tree.get(key1)
      originalValue2 <- tree.get(key2)
      originalValue3 <- tree.get(key3)
      originalValue4 <- tree.get(key4)

      // Test access to new tree - should have keys 7,9
      newValue1 <- newTree.get(key1)
      newValue2 <- newTree.get(key2)
      newValue3 <- newTree.get(key3)
      newValue4 <- newTree.get(key4)

    } yield {
      // Original tree should have keys < 5 (keys 1, 3)
      originalValue1.isDefined should be (true)
      originalValue1.get.value.bytes(0) should be (10)

      originalValue2.isDefined should be (true)
      originalValue2.get.value.bytes(0) should be (30)

      // Original tree should NOT have keys >= 5 (keys 7, 9)
      originalValue3 should be (None)
      originalValue4 should be (None)

      // New tree should NOT have keys < 5 (keys 1, 3)
      newValue1 should be (None)
      newValue2 should be (None)

      // New tree should have keys >= 5 (keys 7, 9)
      newValue3.isDefined should be (true)
      newValue3.get.value.bytes(0) should be (70)

      newValue4.isDefined should be (true)
      newValue4.get.value.bytes(0) should be (90)
    }
  }

  test("splitTree - split at exact key") {
    val treeKey = createKey(0)
    val newTreeKey = createKey(1)
    val key1 = createKey(1)
    val key5 = createKey(5)
    val key7 = createKey(7)
    val value1 = createValue(10)
    val value5 = createValue(50)
    val value7 = createValue(70)
    val splitAtKey = createKey(5)

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Insert additional keys
      tx2 = client.newTransaction()
      _ <- tree.set(key5, value5)(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      tx3 = client.newTransaction()
      _ <- tree.set(key7, value7)(using tx3)
      _ <- tx3.commit()
      _ <- waitForTransactionsToComplete()

      // Now split the tree at key 5 (exact match)
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, ptr, newTreeKey, true, newRootPtr,
        ByteArrayKeyOrdering, nodeAllocator)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      newTreeRoot <- fnewTreeRoot
      newTree <- newTreeRoot.getTree()

      // Test access to original tree
      originalValue1 <- tree.get(key1)
      originalValue5 <- tree.get(key5)
      originalValue7 <- tree.get(key7)

      // Test access to new tree
      newValue1 <- newTree.get(key1)
      newValue5 <- newTree.get(key5)
      newValue7 <- newTree.get(key7)

    } yield {
      // Original tree should have keys < 5 (key 1)
      originalValue1.isDefined should be (true)
      originalValue1.get.value.bytes(0) should be (10)

      // Original tree should NOT have keys >= 5 (keys 5, 7)
      originalValue5 should be (None)
      originalValue7 should be (None)

      // New tree should NOT have keys < 5 (key 1)
      newValue1 should be (None)

      // New tree should have keys >= 5 (keys 5, 7)
      newValue5.isDefined should be (true)
      newValue5.get.value.bytes(0) should be (50)

      newValue7.isDefined should be (true)
      newValue7.get.value.bytes(0) should be (70)
    }
  }

  test("splitTree - empty tree after split") {
    val treeKey = createKey(0)
    val newTreeKey = createKey(1)
    val key1 = createKey(10)
    val value1 = createValue(100)
    val splitAtKey = createKey(5)  // All keys > splitAtKey

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Now split the tree
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, ptr, newTreeKey, true, newRootPtr,
        ByteArrayKeyOrdering, nodeAllocator)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      newTreeRoot <- fnewTreeRoot
      newTree <- newTreeRoot.getTree()

      // Test access to original tree - should be empty
      originalValue <- tree.get(key1)
      nonExistentValue <- tree.get(createKey(1))

      // Test access to new tree - should have the key
      newValue <- newTree.get(key1)

    } yield {
      // Original tree should be empty since all keys moved to new tree
      originalValue should be (None)
      nonExistentValue should be (None)

      // New tree should have the key that was moved
      newValue.isDefined should be (true)
      newValue.get.value.bytes(0) should be (100)
    }
  }

  test("splitTree - all keys remain in original tree") {
    val treeKey = createKey(0)
    val newTreeKey = createKey(1)
    val key1 = createKey(1)
    val key2 = createKey(3)
    val value1 = createValue(10)
    val value2 = createValue(30)
    val splitAtKey = createKey(10)  // All keys < splitAtKey

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Insert additional key
      tx2 = client.newTransaction()
      _ <- tree.set(key2, value2)(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      // Now split the tree
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, ptr, newTreeKey, true, newRootPtr,
        ByteArrayKeyOrdering, nodeAllocator)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      newTreeRoot <- fnewTreeRoot
      newTree <- newTreeRoot.getTree()

      // Test access to original tree - should have all keys
      originalValue1 <- tree.get(key1)
      originalValue2 <- tree.get(key2)

      // Test access to new tree - should be empty
      newValue1 <- newTree.get(key1)
      newValue2 <- newTree.get(key2)

    } yield {
      // Original tree should have all keys since they're all < splitAtKey
      originalValue1.isDefined should be (true)
      originalValue1.get.value.bytes(0) should be (10)

      originalValue2.isDefined should be (true)
      originalValue2.get.value.bytes(0) should be (30)

      // New tree should be empty since no keys >= splitAtKey
      newValue1 should be (None)
      newValue2 should be (None)
    }
  }

  test("splitTree - returns valid pointer") {
    val treeKey = createKey(0)
    val key1 = createKey(1)
    val value1 = createValue(10)
    val splitAtKey = createKey(5)

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Now split the tree
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

    } yield {
      // The new root pointer should be a valid KeyValueObjectPointer
      newRootPtr should not be null
      // The ID should be valid (non-null UUID)
      newRootPtr.id.uuid.toString should not be empty
      succeed
    }
  }

  test("splitTree - empty tree from split can be used") {
    val treeKey = createKey(0)
    val newTreeKey = createKey(1)
    val key1 = createKey(1)
    val key2 = createKey(3)
    val value1 = createValue(10)
    val value2 = createValue(30)
    val splitAtKey = createKey(10)  // All keys < splitAtKey (new tree will be empty)

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key1 -> value1))

      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()

      // Insert additional key
      tx2 = client.newTransaction()
      _ <- tree.set(key2, value2)(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      // Now split the tree (creating empty new tree)
      splitTx = client.newTransaction()
      newRootPtr <- tree.splitTree(splitAtKey)(using splitTx)
      fnewTreeRoot = KVObjectRootManager.setNewRoot(client, ptr, newTreeKey, true, newRootPtr,
        ByteArrayKeyOrdering, nodeAllocator)(using splitTx)
      _ <- splitTx.commit()
      _ <- waitForTransactionsToComplete()

      newTreeRoot <- fnewTreeRoot
      newTree <- newTreeRoot.getTree()

      // Verify new tree is empty
      newValue1 <- newTree.get(key1)
      newValue2 <- newTree.get(key2)

      // Now add a key to the new tree to verify it's functional
      newKey = createKey(15)
      newValueToAdd = createValue(150.toByte)
      tx3 = client.newTransaction()
      _ <- newTree.set(newKey, newValueToAdd)(using tx3)
      _ <- tx3.commit()
      _ <- waitForTransactionsToComplete()

      // Verify the key was added successfully
      addedValue <- newTree.get(newKey)

    } yield {
      // New tree should initially be empty
      newValue1 should be (None)
      newValue2 should be (None)

      // New tree should be able to accept new keys
      addedValue.isDefined should be (true)
      addedValue.get.value.bytes(0) should be (150.toByte)
    }
  }
}