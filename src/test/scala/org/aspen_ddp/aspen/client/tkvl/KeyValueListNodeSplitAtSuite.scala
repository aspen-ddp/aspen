package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.*

import scala.concurrent.Future

class KeyValueListNodeSplitAtSuite extends IntegrationTestSuite {

  def createNode(contents: Map[Key, Value],
                 minimum: Key = Key.AbsoluteMinimum,
                 tail: Option[KeyValueListPointer] = None)(using tx: Transaction): Future[KeyValueListNode] = {
    for {
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator

      contentStates = contents.map((k, v) => k -> ValueState(v, tx.revision, HLCTimestamp.now))

      lptr <- alloc.allocateKeyValueObject(
        ObjectRevisionGuard(radicle, ikvos.revision),
        contents,
        if minimum == Key.AbsoluteMinimum then None else Some(minimum),
        None,
        None,
        tail.map(p => Value(p.toArray)))
    } yield new KeyValueListNode(
      client,
      pool.ida,
      lptr,
      ByteArrayKeyOrdering,
      minimum,
      tx.revision,
      ObjectRefcount(0,1),
      contentStates,
      tail)
  }

  def createKey(byte: Byte): Key = Key(Array(byte))
  def createValue(byte: Byte): Value = Value(Array(byte))

  atest("splitAt - basic split with keys on both sides") {
    val key1 = createKey(1)
    val key2 = createKey(2)
    val key3 = createKey(3)
    val key4 = createKey(4)

    val value1 = createValue(10)
    val value2 = createValue(20)
    val value3 = createValue(30)
    val value4 = createValue(40)

    val contents = Map(key1 -> value1, key2 -> value2, key3 -> value3, key4 -> value4)
    val splitAtKey = createKey(3)

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      // Read the original hostState (should now contain only keys < splitAtKey)
      originalState <- client.read(node.pointer)

      // Read the new left hostState
      leftState <- client.read(newLeftPtr.pointer)

      // Read the new right hostState by reading the left hostState's right pointer
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original hostState should have keys 1, 2 and maximum set to splitAtKey
      originalState.contents.size should be (2)
      originalState.contents should contain key (key1)
      originalState.contents should contain key (key2)
      originalState.contents should not contain key (key3)
      originalState.contents should not contain key (key4)
      originalState.maximum should be (Some(splitAtKey))
      originalState.right should be (None)

      // Right hostState should have keys 3, 4
      rightState.contents.size should be (2)
      rightState.contents should contain key (key3)
      rightState.contents should contain key (key4)
      rightState.minimum should be (Some(splitAtKey))

      // Left hostState should be empty in this case
      leftState.contents.size should be (0)
      leftState.minimum should be (None)
      leftState.maximum should be (Some(splitAtKey))
      leftState.right.isDefined should be (true)
    }
  }

  atest("splitAt - inclusive basic split with keys on both sides") {
    val key1 = createKey(1)
    val key2 = createKey(2)
    val key3 = createKey(3)
    val key4 = createKey(4)

    val value1 = createValue(10)
    val value2 = createValue(20)
    val value3 = createValue(30)
    val value4 = createValue(40)

    val contents = Map(key1 -> value1, key2 -> value2, key3 -> value3, key4 -> value4)
    val splitAtKey = createKey(3)

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, true, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      // Read the original hostState (should now contain only keys < splitAtKey)
      originalState <- client.read(node.pointer)

      // Read the new left hostState
      leftState <- client.read(newLeftPtr.pointer)

      // Read the new right hostState by reading the left hostState's right pointer
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original hostState should have keys 1, 2 and maximum set to splitAtKey
      originalState.contents.size should be(3)
      originalState.contents should contain key (key1)
      originalState.contents should contain key (key2)
      originalState.contents should contain key (key3)
      originalState.contents should not contain key(key4)
      originalState.maximum should be(Some(splitAtKey))
      originalState.right should be(None)

      // Right hostState should have keys 3, 4
      rightState.contents.size should be(1)
      rightState.contents should contain key (key4)
      rightState.minimum should be(Some(splitAtKey))

      // Left hostState should be empty in this case
      leftState.contents.size should be(0)
      leftState.minimum should be(None)
      leftState.maximum should be(Some(splitAtKey))
      leftState.right.isDefined should be(true)
    }
  }

  atest("splitAt - all keys go to right side") {
    val key3 = createKey(3)
    val key4 = createKey(4)
    val key5 = createKey(5)

    val value3 = createValue(30)
    val value4 = createValue(40)
    val value5 = createValue(50)

    val contents = Map(key3 -> value3, key4 -> value4, key5 -> value5)
    val splitAtKey = createKey(2) // All keys are >= 2

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original hostState should be empty
      originalState.contents.size should be (0)
      originalState.maximum should be (Some(splitAtKey))
      originalState.right should be (None)

      // Right hostState should have all keys
      rightState.contents.size should be (3)
      rightState.contents should contain key (key3)
      rightState.contents should contain key (key4)
      rightState.contents should contain key (key5)

      // Left hostState should be empty
      leftState.contents.size should be (0)
    }
  }

  atest("splitAt - all keys go to left side") {
    val key1 = createKey(1)
    val key2 = createKey(2)
    val key3 = createKey(3)

    val value1 = createValue(10)
    val value2 = createValue(20)
    val value3 = createValue(30)

    val contents = Map(key1 -> value1, key2 -> value2, key3 -> value3)
    val splitAtKey = createKey(5) // All keys are < 5

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original hostState should have all keys
      originalState.contents.size should be (3)
      originalState.contents should contain key (key1)
      originalState.contents should contain key (key2)
      originalState.contents should contain key (key3)
      originalState.maximum should be (Some(splitAtKey))

      // Right hostState should be empty
      rightState.contents.size should be (0)
      rightState.minimum should be (Some(splitAtKey))

      // Left hostState should be empty
      leftState.contents.size should be (0)
    }
  }

  atest("splitAt - with down pointer") {
    val key2 = createKey(2)
    val key4 = createKey(4)

    val value2 = createValue(20)
    val value4 = createValue(40)

    val contents = Map(key2 -> value2, key4 -> value4)
    val splitAtKey = createKey(3)

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()

      downContents = Map(createKey(10) -> createValue(100))
      downPtr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, node.revision), downContents)(using tx2)

      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, Some(downPtr), alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original hostState should have key2 only
      originalState.contents.size should be (1)
      originalState.contents should contain key (key2)
      originalState.maximum should be (Some(splitAtKey))

      // Right hostState should have key4
      rightState.contents.size should be (1)
      rightState.contents should contain key (key4)

      // Left hostState should have the down pointer as AbsoluteMinimum key
      leftState.contents.size should be (1)
      leftState.contents should contain key (Key.AbsoluteMinimum)
      leftState.contents(Key.AbsoluteMinimum).value.bytes should equal (downPtr.toArray)
    }
  }

  atest("splitAt - empty hostState") {
    val contents = Map.empty[Key, Value]
    val splitAtKey = createKey(3)

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // All hostStates should be empty
      originalState.contents.size should be (0)
      rightState.contents.size should be (0)
      leftState.contents.size should be (0)

      originalState.maximum should be (Some(splitAtKey))
      rightState.minimum should be (Some(splitAtKey))
    }
  }

  atest("splitAt - single key exactly at split point") {
    val key3 = createKey(3)
    val value3 = createValue(30)

    val contents = Map(key3 -> value3)
    val splitAtKey = createKey(3)

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original hostState should be empty (key3 >= splitAtKey)
      originalState.contents.size should be (0)
      originalState.maximum should be (Some(splitAtKey))

      // Right hostState should have the single key
      rightState.contents.size should be (1)
      rightState.contents should contain key (key3)

      // Left hostState should be empty
      leftState.contents.size should be (0)
    }
  }

  atest("splitAt - hostState with existing tail pointer") {
    val key1 = createKey(1)
    val key2 = createKey(2)
    val value1 = createValue(10)
    val value2 = createValue(20)

    val tailKey = createKey(10)
    val tailValue = createValue(100)

    for {
      // Create tail and main nodes in first transaction
      _ <- Future.unit
      tx1 = client.newTransaction()
      tailContents <- Future.successful(Map(tailKey -> tailValue))
      tailNode <- createNode(tailContents, tailKey)(using tx1)
      tailPtr = KeyValueListPointer(tailKey, tailNode.pointer)

      contents = Map(key1 -> value1, key2 -> value2)
      node <- createNode(contents, tail = Some(tailPtr))(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator

      splitAtKey = createKey(2)
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original hostState should have key1 only
      originalState.contents.size should be (1)
      originalState.contents should contain key (key1)
      originalState.maximum should be (Some(splitAtKey))
      originalState.right should be (None)

      // Right hostState should have key2 and preserve the tail pointer
      rightState.contents.size should be (1)
      rightState.contents should contain key (key2)
      rightState.right.isDefined should be (true)
      rightState.right.get.bytes should equal (tailPtr.toArray)

      // Left hostState should point to right hostState
      leftState.right.isDefined should be (true)
    }
  }

  atest("splitAt - require splitAtKey > AbsoluteMinimum") {
    val contents = Map(createKey(1) -> createValue(10))

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()

      result <- node.splitAt(ByteArrayKeyOrdering, Key.AbsoluteMinimum, false, None, alloc)(using tx2).recover {
        case _: IllegalArgumentException => "IllegalArgumentException caught"
        case other => throw other
      }

    } yield {
      result should be ("IllegalArgumentException caught")
    }
  }

  atest("splitAt - preserves value data integrity") {
    val key1 = createKey(1)
    val key3 = createKey(3)
    val key5 = createKey(5)

    // Use distinct values to verify data integrity
    val value1 = Value(Array[Byte](1, 2, 3, 4, 5))
    val value3 = Value(Array[Byte](10, 20, 30))
    val value5 = Value(Array[Byte](50, 60))

    val contents = Map(key1 -> value1, key3 -> value3, key5 -> value5)
    val splitAtKey = createKey(4)

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Verify original hostState has correct values
      originalState.contents.size should be (2)
      originalState.contents(key1).value.bytes should equal (value1.bytes)
      originalState.contents(key3).value.bytes should equal (value3.bytes)

      // Verify right hostState has correct values
      rightState.contents.size should be (1)
      rightState.contents(key5).value.bytes should equal (value5.bytes)
    }
  }

  atest("splitAt - large number of keys") {
    val numKeys = 20
    val contents = (1 to numKeys).map { i =>
      createKey(i.toByte) -> createValue((i * 10).toByte)
    }.toMap

    val splitAtKey = createKey(10)

    for {
      _ <- Future.unit
      tx1 = client.newTransaction()
      node <- createNode(contents)(using tx1)
      _ <- tx1.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      node <- node.refresh()

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx2 = client.newTransaction()
      newLeftPtr <- node.splitAt(ByteArrayKeyOrdering, splitAtKey, false, None, alloc)(using tx2)
      _ <- tx2.commit().map(_ => ())
      _ <- waitForTransactionsToComplete()

      originalState <- client.read(node.pointer)
      leftState <- client.read(newLeftPtr.pointer)
      rightPtr = KeyValueListPointer(leftState.right.get.bytes)
      rightState <- client.read(rightPtr.pointer)

    } yield {
      // Original should have keys 1-9
      originalState.contents.size should be (9)
      (1 to 9).foreach { i =>
        originalState.contents should contain key createKey(i.toByte)
      }

      // Right should have keys 10-20
      rightState.contents.size should be (11)
      val rightAssertions = (10 to 20).map { i =>
        rightState.contents should contain key createKey(i.toByte)
      }
      rightAssertions.last
    }
  }
}