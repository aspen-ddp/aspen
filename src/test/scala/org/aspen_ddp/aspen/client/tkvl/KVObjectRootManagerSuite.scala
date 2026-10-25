package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.Future

/** Root manager behavior and the tree-level edge cases that the larger TKVL suites leave out: insert requirements,
  * deletes of absent keys, deletes with requirements, the empty key, and nodes handed out by `getContainingNode`.
  */
class KVObjectRootManagerSuite extends IntegrationTestSuite:

  private val treeKey = Key(Array[Byte](0))

  private def value(i: Int): Value = Value(Array.fill[Byte](10)(i.toByte))

  private def bytes(ovs: Option[org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState]): Option[List[Byte]] =
    ovs.map(_.value.bytes.toList)

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

  private def newTree(host: KeyValueObjectPointer, maxNodeSize: Int = 4096): Future[KVObjectRootManager] =
    val tx = client.newTransaction()
    for
      froot <- KVObjectRootManager.createNewTree(client, host, treeKey, ByteArrayKeyOrdering,
        new SinglePoolNodeAllocator(client, Radicle.poolId, maxNodeSize), Map())(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      rm <- froot
    yield
      rm

  private def newTree(): Future[KVObjectRootManager] = rootHost().flatMap(newTree(_))

  private def inTx(update: Transaction => Future[Unit]): Future[Unit] =
    val tx = client.newTransaction()
    update(tx).flatMap(_ => tx.commit()).flatMap(_ => waitForTransactionsToComplete())

  atest("encode and createRootManager round-trip to a manager for the same tree"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(7), value(7))(using tx))
      copy = KVObjectRootManager.createRootManager(client, rm.encode())
      v <- new TieredKeyValueList(client, copy).get(Key(7))
    yield
      assert(copy.treeKey == rm.treeKey)
      assert(copy.pointer == rm.pointer)
      assert(copy.typeId == rm.typeId)
      assert(bytes(v) == Some(value(7).bytes.toList))

  atest("a missing tree key fails with InvalidRoot"):
    for
      rm <- newTree()
      missing = new KVObjectRootManager(client, Key(Array[Byte](9)), rm.pointer)
      err <- missing.getRoot().failed
      err2 <- missing.getRootNode().failed
    yield
      assert(err.isInstanceOf[InvalidRoot])
      assert(err2.isInstanceOf[InvalidRoot])

  atest("a corrupt root entry fails with InvalidRoot"):
    for
      rm <- newTree()
      _ <- inTx(tx => Future.successful(tx.update(rm.pointer, None, None, Nil,
        List(Insert(treeKey, Array[Byte](1, 2, 3))))))
      err <- rm.getRoot().failed
    yield
      assert(err.isInstanceOf[InvalidRoot])

  atest("createNewTree over an existing tree key fails and leaves the tree alone"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(1), value(1))(using tx))
      tx = client.newTransaction()
      _ <- KVObjectRootManager.createNewTree(client, rm.pointer, treeKey, ByteArrayKeyOrdering,
        new SinglePoolNodeAllocator(client, Radicle.poolId), Map())(using tx)
      err <- tx.commit().failed
      v <- tree.get(Key(1))
    yield
      assert(err != null)
      assert(bytes(v) == Some(value(1).bytes.toList))

  atest("a root whose node object was deleted reads as an empty tree"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(1), value(1))(using tx))
      root <- rm.getRoot()
      // The state of a deleteTree that removed the nodes but not yet updated the root
      _ <- KeyValueListNode.deleteList(client, KeyValueListPointer(Key.AbsoluteMinimum,
        root.orootObject.get.asInstanceOf[KeyValueObjectPointer]), ByteArrayKeyOrdering, None)
      (tier, _, onode) <- rm.getRootNode()
      v <- tree.get(Key(1))
    yield
      assert(tier == 0)
      assert(onode.isEmpty)
      assert(v.isEmpty)

  atest("prepareRootUpdate fails the transaction when the root changed after it was read"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      tx = client.newTransaction()
      _ <- rm.prepareRootUpdate(0, None)(using tx)
      // The first insert replaces the root entry, so the revision the staged update requires is stale
      _ <- inTx(t => tree.set(Key(1), value(1))(using t))
      err <- tx.commit().failed
      root <- rm.getRoot()
      v <- tree.get(Key(1))
    yield
      assert(err != null)
      assert(root.orootObject.isDefined)
      assert(bytes(v) == Some(value(1).bytes.toList))

  atest("the allocator and node size for a tier come from the root's node allocator"):
    for
      host <- rootHost()
      rm <- newTree(host, 2048)
      size0 <- rm.getMaxNodeSize(0)
      size1 <- rm.getMaxNodeSize(1)
      alloc <- rm.getAllocatorForTier(0)
    yield
      assert(size0 == 2048)
      assert(size1 == 2048)
      assert(alloc != null)

  atest("an insert requiring the key not exist succeeds on an empty tree and fails on an existing key"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(1), value(1), Some(Left(true)))(using tx))
      tx = client.newTransaction()
      err <- tree.set(Key(1), value(2), Some(Left(true)))(using tx).failed
      v <- tree.get(Key(1))
    yield
      assert(err.isInstanceOf[KeyAlreadyExists])
      assert(bytes(v) == Some(value(1).bytes.toList))

  atest("an insert requiring a revision succeeds with the current revision and fails with a stale one"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(1), value(1))(using tx))
      v1 <- tree.get(Key(1))
      _ <- inTx(tx => tree.set(Key(1), value(2), Some(Right(v1.get.revision)))(using tx))
      tx = client.newTransaction()
      _ <- tree.set(Key(1), value(3), Some(Right(v1.get.revision)))(using tx)
      err <- tx.commit().failed
      v <- tree.get(Key(1))
    yield
      assert(err != null)
      assert(bytes(v) == Some(value(2).bytes.toList))

  atest("deleting an absent key stages nothing in an empty tree and in a single node tree"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      emptyTx = client.newTransaction()
      _ <- tree.delete(Key(5))(using emptyTx)
      _ <- inTx(tx => tree.set(Key(1), value(1))(using tx))
      oneTx = client.newTransaction()
      _ <- tree.delete(Key(5))(using oneTx)
    yield
      assert(emptyTx.isEmpty)
      assert(oneTx.isEmpty)

  atest("a delete with a stale revision requirement fails and the key stays"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(1), value(1))(using tx))
      _ <- inTx(tx => tree.set(Key(2), value(2))(using tx))
      v1 <- tree.get(Key(1))
      _ <- inTx(tx => tree.set(Key(1), value(9))(using tx))
      tx = client.newTransaction()
      _ <- tree.delete(Key(1), None, List(KeyValueUpdate.KeyRevision(Key(1), v1.get.revision)))(using tx)
      err <- tx.commit().failed
      v <- tree.get(Key(1))
    yield
      assert(err != null)
      assert(bytes(v) == Some(value(9).bytes.toList))

  atest("a delete with a current revision requirement removes the key"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(1), value(1))(using tx))
      _ <- inTx(tx => tree.set(Key(2), value(2))(using tx))
      v1 <- tree.get(Key(1))
      _ <- inTx(tx => tree.delete(Key(1), None, List(KeyValueUpdate.KeyRevision(Key(1), v1.get.revision)))(using tx))
      v <- tree.get(Key(1))
      v2 <- tree.get(Key(2))
    yield
      assert(v.isEmpty)
      assert(v2.isDefined)

  atest("the empty key is an ordinary key, through splits and deletes"):
    // Small nodes so the empty key lives in the head node of a tree that has split several times
    val keys = Key.AbsoluteMinimum :: (1 to 25).map(i => Key(i)).toList
    for
      host <- rootHost()
      rm <- newTree(host, 500)
      tree <- rm.getTree()
      _ <- keys.foldLeft(Future.unit)((f, k) =>
        f.flatMap(_ => inTx(tx => tree.set(k, Value(new Array[Byte](60)))(using tx))))
      (tier, _, _) <- rm.getRootNode()
      found <- Future.sequence(keys.map(tree.get))
      seen <- {
        var visited = List[Key]()
        tree.foreach((_, k, _) => { visited = k :: visited; Future.unit }).map(_ => visited.reverse)
      }
      _ <- inTx(tx => tree.delete(Key.AbsoluteMinimum)(using tx))
      gone <- tree.get(Key.AbsoluteMinimum)
      stillThere <- tree.get(Key(1))
    yield
      assert(tier > 0)
      assert(found.forall(_.isDefined))
      assert(seen == keys)
      assert(gone.isEmpty)
      assert(stillThere.isDefined)

  atest("TieredKeyValueListNode set, delete and rename act on the containing node"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      _ <- inTx(tx => tree.set(Key(1), value(1))(using tx))
      _ <- inTx(tx => tree.set(Key(2), value(2))(using tx))
      onode <- tree.getContainingNode(Key(1))
      node = onode.get
      _ <- inTx(tx => node.set(Key(3), value(3))(using tx))
      _ <- inTx(tx => node.rename(Key(2), Key(20))(using tx))
      _ <- inTx(tx => node.delete(Key(1))(using tx))
      v1 <- tree.get(Key(1))
      v2 <- tree.get(Key(2))
      v20 <- tree.get(Key(20))
      v3 <- tree.get(Key(3))
    yield
      assert(node.contains(Key(1)) && node.keys.size == 2)
      assert(v1.isEmpty)
      assert(v2.isEmpty)
      assert(bytes(v20) == Some(value(2).bytes.toList))
      assert(bytes(v3) == Some(value(3).bytes.toList))

  atest("getContainingNode of an empty tree is None"):
    for
      rm <- newTree()
      tree <- rm.getTree()
      node <- tree.getContainingNode(Key(1))
      v <- tree.get(Key(1))
    yield
      assert(node.isEmpty)
      assert(v.isEmpty)
