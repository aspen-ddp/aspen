package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.TransactionAborted
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.objects.{Key, KeyAlreadyExists, ObjectPointer, Value}

import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.util.Random

/** Helpers for the batch update (`prepareUpdates`) suites */
abstract class TKVLBatchSuiteSupport extends TreeKindSuiteBase:

  protected def inserts(ks: List[Key], mustNotExist: Boolean = false): List[TKVLInsert] =
    ks.map(k => TKVLInsert(k, value(k.bytes.last), mustNotExist))

  /** One batch in a transaction of its own. Finalization is not waited for. */
  protected def batch(tree: Tree, ins: List[TKVLInsert], dels: List[Key]): Future[Unit] =
    inTransaction(tx => tree.prepareUpdates(ins, dels)(using tx))

  /** `batch`, then every split, join and removal it started is finalized, for tests that look at the tree's shape */
  protected def batchAndFinalize(tree: Tree, ins: List[TKVLInsert], dels: List[Key]): Future[Unit] =
    batch(tree, ins, dels).flatMap(_ => waitForTransactionsToComplete())

  /** Tier-0 nodes left empty, other than the head of the tier, which is never removed */
  protected def emptyNodes(nodes: List[ListNode]): List[ListNode] = nodes.drop(1).filter(_.entries.isEmpty)

/** Batch updates whose outcome depends on node sizes, and so on the pool's IDA: splits, joins and removals, run against
  * both tree kinds once per IDA (see the subclasses below)
  */
abstract class TKVLBatchSuiteBase extends TKVLBatchSuiteSupport:

  import TreeKindSuiteBase.Op

  for kind <- TreeKind.values do

    atest(s"$kind: a batch on an empty tree too large for one node builds a list of nodes and the tiers above it"):
      val all = keys(1 to 60)
      for
        (tree, rm) <- newTree(kind)
        _ <- batchAndFinalize(tree, inserts(all), Nil)
        tiers <- allTiers(kind, rm)
        entries <- tree.entries()
        values <- Future.sequence(all.map(tree.get))
      yield
        assert(tiers.size >= 2)
        assert(tiers.last.size > 5)
        assert(ofKind(kind, tiers.flatten))
        assert(entries == expectedEntries(all))
        assert(values == all.map(k => Some(value(k.bytes.last).bytes.toList)))

    atest(s"$kind: one batch inserts, overwrites and deletes across many nodes and tiers"):
      val initial = keys(1 to 40)
      val added = keys(41 to 90)
      val overwritten = keys(2 to 40 by 3)
      val deleted = keys(3 to 40 by 6) ++ keys(200 to 203)
      for
        (tree, rm) <- newTree(kind)
        _ <- batchAndFinalize(tree, inserts(initial), Nil)
        nodesBefore <- tier0Nodes(kind, rm)
        _ <- batchAndFinalize(tree, inserts(added) ++ overwritten.map(k => TKVLInsert(k, value(99))), deleted)
        nodesAfter <- tier0Nodes(kind, rm)
        entries <- tree.entries()
        n <- numTiers(rm)
      yield
        val expected = TreeMap.from(expectedEntries(initial ++ added))(using ordering) ++
          overwritten.map(k => k -> value(99).bytes.toList) -- deleted
        assert(entries == expected.toList)
        assert(nodesBefore.size > 3)
        assert(nodesAfter.size > nodesBefore.size)
        assert(n >= 1)

    atest(s"$kind: a batch emptying adjacent nodes leaves none of them in the list"):
      val all = keys(1 to 40)
      val middle = keys(5 to 32)
      for
        (tree, rm) <- newTree(kind)
        _ <- batchAndFinalize(tree, inserts(all), Nil)
        nodesBefore <- tier0Nodes(kind, rm)
        _ <- batchAndFinalize(tree, Nil, middle)
        nodesAfter <- tier0Nodes(kind, rm)
        entries <- tree.entries()
      yield
        assert(nodesAfter.size < nodesBefore.size)
        assert(emptyNodes(nodesAfter).isEmpty)
        assert(entries == expectedEntries(all.filterNot(middle.contains)))

  for kind <- TreeKind.values do
    atest(s"$kind: random batches match a TreeMap model"):
      val rand = new Random(0xba7c4)
      val batchKeySpace = 160

      def randomBatch(setPercent: Int): (List[TKVLInsert], List[Key]) =
        val ops = randomOps(rand, 1 + rand.nextInt(if rand.nextInt(4) == 0 then 60 else 10), batchKeySpace, setPercent)
          .groupBy { case Op.Set(k, _) => k; case Op.Delete(k) => k }.values.map(_.head).toList
        (ops.collect { case Op.Set(k, v) => TKVLInsert(k, v) }, ops.collect { case Op.Delete(k) => k })

      // A batch can collide with a finalization action still rewriting one of its nodes and abort, having changed
      // nothing. Callers retry, as this does.
      def retried(tree: Tree, ins: List[TKVLInsert], dels: List[Key], attempts: Int = 10): Future[Unit] =
        batch(tree, ins, dels).recoverWith:
          case _: TransactionAborted if attempts > 1 => retried(tree, ins, dels, attempts - 1)

      // `count` batches, one transaction each, without waiting for finalization in between (a descent tolerates upper
      // tiers that finalization has not caught up with). Then everything is finalized and the tree checked.
      def phase(tree: Tree, rm: RootManager, model: TreeMap[Key, List[Byte]], count: Int,
                setPercent: Int): Future[(TreeMap[Key, List[Byte]], Int)] =
        val batches = List.fill(count)(randomBatch(setPercent))
        val next = batches.foldLeft(model)((m, b) => m ++ b._1.map(i => i.key -> i.value.bytes.toList) -- b._2)
        for
          _ <- batches.foldLeft(Future.unit)((prev, b) => prev.flatMap(_ => retried(tree, b._1, b._2)))
          _ <- waitForTransactionsToComplete()
          entries <- tree.entries()
          nodes <- tier0Nodes(kind, rm)
        yield
          assert(entries == next.toList)
          (next, nodes.size)

      for
        (tree, rm) <- newTree(kind)
        // Growing batches, then mostly deleting ones, so that nodes both split and empty
        (grown, grownNodes) <- phase(tree, rm, TreeMap.empty(using ordering), 12, 60)
        (model, shrunkNodes) <- phase(tree, rm, grown, 8, 15)
        _ <- readsMatch(tree, model, batchKeySpace)
      yield
        assert(grownNodes > 3)
        assert(shrunkNodes < grownNodes)
        assert(model.nonEmpty)

/** Every node is a whole copy of its object */
class TKVLBatchSuite extends TKVLBatchSuiteBase:
  def nodeSize(kind: TreeKind): Int = kind match
    case TreeKind.KeyValueObjects => 400
    case TreeKind.DataObjects => 200

/** Classic Reed-Solomon, as `ReedSolomonTreeKindParitySuite` */
class ReedSolomonTKVLBatchSuite extends TKVLBatchSuiteBase:
  override def testIda: IDA = ReedSolomon(5, 3, 4)

  def nodeSize(kind: TreeKind): Int = kind match
    case TreeKind.KeyValueObjects => 200
    case TreeKind.DataObjects => 200

/** Batch requirement and validation checks, which do not depend on the pool's IDA */
class TKVLBatchRequirementSuite extends TKVLBatchSuiteSupport:
  def nodeSize(kind: TreeKind): Int = kind match
    case TreeKind.KeyValueObjects => 400
    case TreeKind.DataObjects => 200

  for kind <- TreeKind.values do

    atest(s"$kind: a batch changes several keys of one node in one transaction"):
      for
        (tree, rm) <- newTree(kind)
        _ <- batch(tree, inserts(keys(1 to 3)), Nil)
        _ <- batch(tree, inserts(List(Key(4))) :+ TKVLInsert(Key(2), value(77)), List(Key(1), Key(3)))
        nodes <- tier0Nodes(kind, rm)
        entries <- tree.entries()
      yield
        assert(nodes.size == 1)
        assert(entries == List(Key(2) -> value(77).bytes.toList, Key(4) -> value(4).bytes.toList))

    atest(s"$kind: a batch rejects a key inserted twice or both inserted and deleted, staging nothing"):
      for
        (tree, _) <- newTree(kind)
        tx = client.newTransaction()
        twice <- tree.prepareUpdates(inserts(List(Key(5), Key(5))), Nil)(using tx).failed
        both <- tree.prepareUpdates(inserts(List(Key(5))), List(Key(5)))(using tx).failed
      yield
        assert(twice.isInstanceOf[IllegalArgumentException])
        assert(both.isInstanceOf[IllegalArgumentException])
        assert(tx.isEmpty)

    atest(s"$kind: an insert requiring its key not to exist fails the batch if the key exists, or was inserted since"):
      for
        (tree, _) <- newTree(kind)
        _ <- batch(tree, inserts(keys(1 to 3)), Nil)
        // The key is in the node the batch reads
        tx = client.newTransaction()
        err <- tree.prepareUpdates(inserts(List(Key(9))) ++ inserts(List(Key(2)), mustNotExist = true), Nil)(using tx)
          .failed
        aborted <- tx.result.failed
        // The key is inserted between the batch's read and its commit
        raced = client.newTransaction()
        _ <- tree.prepareUpdates(inserts(List(Key(5)), mustNotExist = true), Nil)(using raced)
        _ <- inTransaction(rtx => tree.set(Key(5), value(55))(using rtx))
        racedFailed <- raced.commit().map(_ => false).recover(_ => true)
        entries <- tree.entries()
      yield
        assert(err.isInstanceOf[KeyAlreadyExists])
        assert(aborted.isInstanceOf[KeyAlreadyExists])
        assert(racedFailed)
        assert(entries == expectedEntries(keys(1 to 3)) :+ (Key(5) -> value(55).bytes.toList))

    atest(s"$kind: retrying an allocate-and-insert whose key must not exist inserts it exactly once"):
      // Each attempt allocates an object and stores its pointer, as callers storing new objects in a tree do. A
      // retry of an attempt that did commit fails rather than storing a second pointer.
      def attempt(tree: Tree): Future[Option[ObjectPointer]] =
        val tx = client.newTransaction()
        val f = for
          pool <- client.getStoragePool(Radicle.poolId)
          ptr <- pool.allocator.allocateDataObject(org.aspen_ddp.aspen.common.DataBuffer(Array[Byte](1)))(using tx)
          _ <- tree.prepareUpdates(List(TKVLInsert(Key(7), Value(ptr.toArray), requireDoesNotExist = true)), Nil)(using tx)
          _ <- tx.commit()
        yield
          Some(ptr)
        f.recover { case _: KeyAlreadyExists => None }
      for
        (tree, _) <- newTree(kind)
        first <- attempt(tree)
        second <- attempt(tree)
        stored <- tree.get(Key(7))
      yield
        assert(first.isDefined)
        assert(second.isEmpty)
        assert(stored == first.map(_.toArray.toList))

  atest("DataObjects: a batch after a change to the same node in the transaction fails with NodeAlreadyModified"):
    for
      (tree, _) <- newTree(TreeKind.DataObjects)
      _ <- batch(tree, inserts(keys(1 to 3)), Nil)
      tx = client.newTransaction()
      _ <- tree.set(Key(4), value(4))(using tx)
      err <- tree.prepareUpdates(inserts(List(Key(5))), Nil)(using tx).failed
    yield
      assert(err.isInstanceOf[NodeAlreadyModified])
