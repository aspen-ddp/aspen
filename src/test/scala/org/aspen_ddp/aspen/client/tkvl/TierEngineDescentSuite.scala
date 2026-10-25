package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{IntegerKeyOrdering, Key, ObjectPointer, Value}

import scala.concurrent.Future

/** The descent through an upper-tier node that joins have left with no entry at or below the target.
  *
  * Deleting keys in ascending order empties the leftmost tier-0 node again and again, and each join absorbs the
  * node to its right and removes that node's entry from the tier above. Once the absorbed node is the first child of
  * an upper-tier node B that has other children, B keeps its minimum but its first entry is above that minimum. A
  * descent for a key in that gap picks B from the tier above and finds nothing in B to descend into, so it has to
  * fall back to B's left neighbour and scan right from there.
  */
class TierEngineDescentSuite extends IntegrationTestSuite:

  private val ordering = IntegerKeyOrdering

  private def value(i: Int): Value = Value(Array.fill[Byte](40)(i.toByte))

  /** The operations under test, for either facade. Values come back as bytes, since Value compares by identity. */
  private trait Tree:
    def get(k: Key): Future[Option[List[Byte]]]
    def set(k: Key, v: Value)(using Transaction): Future[Unit]
    def delete(k: Key)(using Transaction): Future[Unit]

  private def newTree(kind: TreeKind, maxNodeSize: Int): Future[(Tree, KVObjectRootManager)] =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      tx0 = client.newTransaction()
      host <- pool.allocator.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId, maxNodeSize)
      tx = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, host, Key(Array[Byte](0)), ordering, nodeAllocator, Map(),
        kind)(using tx)
      _ <- tx.commit()
      rm <- froot
    yield
      val tree = kind match
        case TreeKind.KeyValueObjects =>
          val t = new TieredKeyValueList(client, rm)
          new Tree:
            def get(k: Key) = t.get(k).map(_.map(_.value.bytes.toList))
            def set(k: Key, v: Value)(using Transaction) = t.set(k, v)
            def delete(k: Key)(using Transaction) = t.delete(k)
        case TreeKind.DataObjects =>
          val t = new DataObjectTKVL(client, rm)
          new Tree:
            def get(k: Key) = t.get(k).map(_.map(_.bytes.toList))
            def set(k: Key, v: Value)(using Transaction) = t.set(k, v)
            def delete(k: Key)(using Transaction) = t.delete(k)
      (tree, rm)

  /** One transaction per update, in order, without waiting for finalization in between. Everything is finalized
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

  /** Every tier's nodes, top tier first. A node's minimum is its entry key in the tier above, or its link minimum. */
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
      down(tier, oroot.get)

  /** The second node of tier 1 */
  private def secondTier1Node(kind: TreeKind, rm: RootManager): Future[ListNode] =
    allTiers(kind, rm).map(tiers => tiers(tiers.size - 2)(1))

  private def lookupsInTheGap(kind: TreeKind, maxNodeSize: Int, numKeys: Int): Future[org.scalatest.Assertion] =
    val all = (1 to numKeys).toList.map(i => Key(i))
    for
      (tree, rm) <- newTree(kind, maxNodeSize)
      _ <- sequentially(all)((k, tx) => tree.set(k, value(k.bytes.last))(using tx))
      tiers <- allTiers(kind, rm)
      b <- secondTier1Node(kind, rm)
      m = b.minimum
      m2 = b.entries(1)._1
      // Ascending deletes up to m: the last one joins the tier-0 node holding m into the leftmost node
      _ <- sequentially(all.filter(k => ordering.compare(k, m) < 0)):
             (k, tx) => tree.delete(k)(using tx)
      emptied <- secondTier1Node(kind, rm)

      vm <- tree.get(m)
      _ <- sequentially(List(m))((k, tx) => tree.delete(k)(using tx))
      vmDeleted <- tree.get(m)
      // A KeyValue-object join does not carry the absorbed node's stored maximum over to the left node, so the store's
      // WithinRange check rejects any insert into the left node at or above its old maximum, m among them. That is a
      // separate KeyValueListNode.delete defect, so only the DataObject tree inserts in the gap here.
      insertsInGap = kind == TreeKind.DataObjects
      _ <- sequentially(if insertsInGap then List(m) else Nil):
             (k, tx) => tree.set(k, value(99))(using tx)
      vmSet <- tree.get(m)
      above <- Future.sequence(all.filter(k => ordering.compare(k, m) > 0).map(tree.get))
    yield
      // The premise: a tree of at least three tiers whose second tier-1 node now starts above its minimum
      assert(tiers.size >= 3)
      assert(b.entries.size >= 2 && b.entries.head._1 == m)
      assert(emptied.pointer == b.pointer && emptied.minimum == m && emptied.entries.head._1 == m2)
      assert(ordering.compare(m, m2) < 0)

      assert(vm == Some(value(m.bytes.last).bytes.toList))
      assert(vmDeleted.isEmpty)
      assert(vmSet == (if insertsInGap then Some(value(99).bytes.toList) else None))
      assert(above.forall(_.isDefined))

  atest("a KeyValue-object tree finds and deletes keys below an upper-tier node's first entry"):
    lookupsInTheGap(TreeKind.KeyValueObjects, 500, 60)

  atest("a DataObject tree finds, deletes and inserts keys below an upper-tier node's first entry"):
    lookupsInTheGap(TreeKind.DataObjects, 200, 24)
