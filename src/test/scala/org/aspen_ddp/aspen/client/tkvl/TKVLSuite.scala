package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, IntegerKeyOrdering, Key, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState

import scala.concurrent.Future
import scala.language.implicitConversions

class TKVLSuite extends IntegrationTestSuite {
  atest("Create new tree") {
    val treeKey = Key(Array[Byte](1))
    val key = Key(Array[Byte](2))
    val value = Value(Array[Byte](3))

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

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

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

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

    given tx1: Transaction = client.newTransaction()

    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3, 2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, IntegerKeyOrdering, nodeAllocator, Map())

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

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

      r <- tx1.commit()

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

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3, 2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

      r <- tx1.commit()

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

    given tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.get.createAllocator(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

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
}
