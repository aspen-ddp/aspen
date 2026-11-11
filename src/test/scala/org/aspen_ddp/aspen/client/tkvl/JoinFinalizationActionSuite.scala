package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{ObjectAllocator, TestTransactions, Transaction}

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{Insert, IntegerKeyOrdering, Key, KeyOrdering, ObjectPointer, Value}

import scala.concurrent.Future

class JoinFinalizationActionSuite extends IntegrationTestSuite:

  private val k3 = Key(3)
  private val k5 = Key(5)
  private val k9 = Key(9)

  /** Tier 2 root t with `rootKeys` (each beyond AbsoluteMinimum pointing at u3) over tier-1 nodes u1 (two entries,
    * linked to u3 when `linkU3`) and u3. Completes with the root manager, u1, u3 and t.
    */
  private def collapsibleTree(rootKeys: List[Key], linkU3: Boolean) =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = TestTransactions.replaySafe(client.newTransaction())
      _ = tx0.exclusiveLockRevision(radicle, ikvos.revision)
      hosting <- alloc.allocateKeyValueObject()(using tx0)
      child <- alloc.allocateKeyValueObject(Map[Key, Value]())(using tx0)
      childValue = Value(child.toArray)
      u3 <- alloc.allocateKeyValueObject(Map(k9 -> childValue))(using tx0)
      u1 <- alloc.allocateKeyValueObject(Map(Key.AbsoluteMinimum -> childValue, k3 -> childValue),
              right = if linkU3 then Some(Value(KeyValueListPointer(k9, u3).toArray)) else None)(using tx0)
      t <- alloc.allocateKeyValueObject(
              ((Key.AbsoluteMinimum -> Value(u1.toArray)) :: rootKeys.map(_ -> Value(u3.toArray))).toMap)(using tx0)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      tx1 = TestTransactions.replaySafe(client.newTransaction())
      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      fmgr = KVObjectRootManager.setNewRoot(client, hosting, Key(Array[Byte](0)), true,
                Root(2, IntegerKeyOrdering, Some(t), nodeAllocator))(using tx1)
      _ <- tx1.commit()
      mgr <- fmgr
    yield (mgr, u1, u3, t)

  /** Delegates to `inner`, running `beforeFirstRootUpdate` once, before the first root update is staged */
  private class RacingRootManager(inner: RootManager, beforeFirstRootUpdate: () => Future[Unit]) extends RootManager:
    private var raced = false
    def cache: TierCache = inner.cache
    def getRoot(): Future[Root] = inner.getRoot()
    def getRootNode(): Future[(Int, KeyOrdering, Option[ListNode])] = inner.getRootNode()
    def getFreshRootNode(): Future[(Int, KeyOrdering, Option[ListNode])] = inner.getFreshRootNode()
    def createInitialNode(contents: Map[Key, Value])(using tx: Transaction): Future[Unit] =
      inner.createInitialNode(contents)
    def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = inner.getAllocatorForTier(tier)
    def getMaxNodeSize(tier: Int): Future[Int] = inner.getMaxNodeSize(tier)
    def prepareRootUpdate(newTier: Int, onewRoot: Option[ObjectPointer])(using tx: Transaction): Future[Unit] =
      val before = if raced then Future.unit else
        raced = true
        beforeFirstRootUpdate()
      before.flatMap(_ => inner.prepareRootUpdate(newTier, onewRoot))
    def encode(): Array[Byte] = inner.encode()
    def typeId: RootManagerTypeId = inner.typeId

  /** Tier 2 root t over the tier-1 list u1 -> u2 -> u3. Completes with the root manager, u2, u3 and t */
  private def twoTierTree() =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      // Tier 2 root t over the tier-1 list u1 -> u2 -> u3. The tier-1 entries point at a placeholder,
      // which the join never reads.
      tx0 = TestTransactions.replaySafe(client.newTransaction())
      _ = tx0.exclusiveLockRevision(radicle, ikvos.revision)
      hosting <- alloc.allocateKeyValueObject()(using tx0)
      child <- alloc.allocateKeyValueObject(Map[Key, Value]())(using tx0)
      childValue = Value(child.toArray)
      u3 <- alloc.allocateKeyValueObject(Map(k9 -> childValue))(using tx0)
      u2 <- alloc.allocateKeyValueObject(Map(k5 -> childValue),
              right = Some(Value(KeyValueListPointer(k9, u3).toArray)))(using tx0)
      u1 <- alloc.allocateKeyValueObject(Map(Key.AbsoluteMinimum -> childValue),
              right = Some(Value(KeyValueListPointer(k5, u2).toArray)))(using tx0)
      t <- alloc.allocateKeyValueObject(Map(Key.AbsoluteMinimum -> Value(u1.toArray),
              k5 -> Value(u2.toArray), k9 -> Value(u3.toArray)))(using tx0)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      tx1 = TestTransactions.replaySafe(client.newTransaction())
      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      fmgr = KVObjectRootManager.setNewRoot(client, hosting, Key(Array[Byte](0)), true,
                Root(2, IntegerKeyOrdering, Some(t), nodeAllocator))(using tx1)
      _ <- tx1.commit()
      mgr <- fmgr
    yield (mgr, u2, u3, t)

  // The upper-tier delete of a join reads the right node before it stages anything, so a commit
  // that does not wait for the staged delete commits without it.
  atest("An upper-tier join commits only after its delete is staged"):
    for
      (mgr, u2, u3, t) <- twoTierTree()
      // Deleting u2's only key joins u3 into it
      action = new JoinFinalizationAction(client, null, mgr, 1, k5, u2)
      _ = action.execute()
      _ <- action.complete
      _ <- waitForTransactionsToComplete()

      u2State <- client.read(u2)
      u3State <- client.readOptional(u3)
      tState <- client.read(t)
    yield
      assert(u2State.contents.keySet == Set(k9))
      assert(u2State.right.isEmpty)
      assert(u3State.isEmpty)
      // The tier-2 join that the tier-1 join queued removed u3's entry
      assert(tState.contents.keySet == Set(Key.AbsoluteMinimum, k5))

  atest("an upper-tier join deletes under KeyRevision, and a second run completes without a transaction"):
    for
      (mgr, u2, _, _) <- twoTierTree()
      audit = new PinAudit(client)
      action = new JoinFinalizationAction(client, null, mgr, 1, k5, u2)
      _ = action.execute()
      _ <- action.complete
      _ <- waitForTransactionsToComplete()
      first = audit.builtExceptMarks
      again = new JoinFinalizationAction(client, null, mgr, 1, k5, u2)
      _ = again.execute()
      _ <- again.complete
      _ <- waitForTransactionsToComplete()
      _ = audit.stop()
    yield
      first should not be empty
      audit.unpinned shouldBe empty
      first.flatMap(_.description.requirements).exists:
        case kvu: KeyValueUpdate => kvu.requirements.exists:
          case KeyValueUpdate.KeyRevision(k, _) => k == k5
          case _ => false
        case _ => false
      .shouldBe(true)
      // The entry is gone: the repeat run reads that and stages nothing
      audit.builtExceptMarks.size shouldBe first.size

  // At-least-once: a repeat run after the tree grew back to the join's tier must not collapse the new root
  atest("a root join whose entry is gone leaves a two-entry root in place"):
    for
      (mgr, u1, u3, t) <- collapsibleTree(List(k9), linkU3 = true)
      action = new JoinFinalizationAction(client, null, mgr, 2, k5, u3)
      _ = action.execute()
      _ <- action.complete
      _ <- waitForTransactionsToComplete()
      (rootTier, _, orootNode) <- mgr.getFreshRootNode()
      tState <- client.readOptional(t)
    yield
      rootTier shouldBe 2
      orootNode.map(_.pointer) shouldBe Some(t)
      tState.map(_.contents.keySet) shouldBe Some(Set(Key.AbsoluteMinimum, k9))

  // An entry added to the root between the action's read of it and its commit must stop the collapse
  atest("a root join retries when the root gains an entry before it commits"):
    for
      (inner, u1, u3, t) <- collapsibleTree(List(k5), linkU3 = false)
      addEntry = () =>
        for
          tkvos <- client.read(t)
          tx = client.newTransaction()
          _ = tx.update(t, Some(tkvos.revision), None, Nil, List(new Insert(k9, u3.toArray)))
          _ <- tx.commit()
          _ <- waitForTransactionsToComplete()
        yield ()
      mgr = new RacingRootManager(inner, addEntry)
      action = new JoinFinalizationAction(client, null, mgr, 2, k5, u3)
      _ = action.execute()
      _ <- action.complete
      _ <- waitForTransactionsToComplete()
      (rootTier, _, orootNode) <- inner.getFreshRootNode()
      tState <- client.readOptional(t)
    yield
      rootTier shouldBe 2
      orootNode.map(_.pointer) shouldBe Some(t)
      tState.map(_.contents.keySet) shouldBe Some(Set(Key.AbsoluteMinimum, k9))
