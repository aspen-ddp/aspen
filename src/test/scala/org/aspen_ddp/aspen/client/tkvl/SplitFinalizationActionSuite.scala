package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{Insert, IntegerKeyOrdering, Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.RevisionLock

import scala.concurrent.Future

/** SplitFinalizationAction's upper-tier insert. Finalization actions run at least once, possibly long after their
  * transaction was finalized: a run must succeed whatever became of the new node meanwhile.
  */
class SplitFinalizationActionSuite extends IntegrationTestSuite:

  private val k5 = Key(5)

  /** A tier-1 root t over the tier-0 node n0, and n1, a tier-0 node with minimum k5 that t does not index yet.
    * Completes with the root manager, t and n1
    */
  private def tierOneTree(): Future[(KVObjectRootManager, KeyValueObjectPointer, KeyValueObjectPointer)] =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator
      tx0 = client.newTransaction()
      _ = tx0.lockRevision(radicle, ikvos.revision)
      hosting <- alloc.allocateKeyValueObject()(using tx0)
      n1 <- alloc.allocateKeyValueObject(Map(k5 -> Value(Array[Byte](5))), minimum = Some(k5))(using tx0)
      n0 <- alloc.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](1))),
              right = Some(Value(KeyValueListPointer(k5, n1).toArray)))(using tx0)
      t <- alloc.allocateKeyValueObject(Map(Key.AbsoluteMinimum -> Value(n0.toArray)))(using tx0)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      tx1 = client.newTransaction()
      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      fmgr = KVObjectRootManager.setNewRoot(client, hosting, Key(Array[Byte](0)), true,
                Root(1, IntegerKeyOrdering, Some(t), nodeAllocator))(using tx1)
      _ <- tx1.commit()
      mgr <- fmgr
      _ <- waitForTransactionsToComplete()
    yield (mgr, t, n1)

  private def runSplit(mgr: RootManager, n1: KeyValueObjectPointer): Future[Unit] =
    val action = new SplitFinalizationAction(client, null, mgr, 1, k5, n1)
    action.execute()
    for
      _ <- action.complete
      _ <- waitForTransactionsToComplete()
    yield ()

  private val reason = "cannot re-add once the node is gone: its revision lock fails"

  atest("a split FA run a second time after the first committed completes without change"):
    for
      (mgr, t, n1) <- tierOneTree()
      n1State <- client.read(n1)
      audit = new PinAudit(client, enforce = false)
      _ <- runSplit(mgr, n1)
      first = audit.builtExceptMarks
      afterFirst <- client.read(t)
      _ <- runSplit(mgr, n1)
      _ = audit.stop()
      afterSecond <- client.read(t)
    yield
      afterFirst.contents.get(k5).map(_.value.bytes.toList) shouldBe Some(n1.toArray.toList)
      first should not be empty
      audit.unpinned shouldBe empty
      audit.replaySafeWith(reason).size shouldBe first.size
      first.foreach(_.description.requirements should contain (RevisionLock(n1, n1State.revision)))
      // The entry is present with this value: the second run stages nothing
      audit.builtExceptMarks.size shouldBe first.size
      afterSecond.revision shouldBe afterFirst.revision

  atest("a split FA run after its node was updated completes"):
    for
      (mgr, t, n1) <- tierOneTree()
      before <- client.read(n1)
      tx = client.newTransaction()
      _ = tx.update(n1, Some(before.revision), None, Nil, List(Insert(Key(6), Array[Byte](6))))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      updated <- client.read(n1)
      audit = new PinAudit(client, enforce = false)
      _ <- runSplit(mgr, n1)
      _ = audit.stop()
      tState <- client.read(t)
    yield
      tState.contents.get(k5).map(_.value.bytes.toList) shouldBe Some(n1.toArray.toList)
      audit.unpinned shouldBe empty
      audit.built.flatMap(_.description.requirements) should contain (RevisionLock(n1, updated.revision))

  atest("a split FA run after its node was deleted completes without writing"):
    for
      (mgr, t, n1) <- tierOneTree()
      before <- client.read(n1)
      tx = client.newTransaction()
      _ = tx.setRefcount(n1, before.refcount, before.refcount.setCount(0))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      tBefore <- client.read(t)
      audit = new PinAudit(client, enforce = false)
      _ <- runSplit(mgr, n1)
      _ = audit.stop()
      tState <- client.read(t)
    yield
      audit.builtExceptMarks shouldBe empty
      tState.contents.contains(k5) shouldBe false
      tState.revision shouldBe tBefore.revision
