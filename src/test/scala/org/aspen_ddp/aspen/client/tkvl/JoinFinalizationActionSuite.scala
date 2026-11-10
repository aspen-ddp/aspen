package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{IntegerKeyOrdering, Key, Value}

class JoinFinalizationActionSuite extends IntegrationTestSuite:

  private val k5 = Key(5)
  private val k9 = Key(9)

  /** Tier 2 root t over the tier-1 list u1 -> u2 -> u3. Completes with the root manager, u2, u3 and t */
  private def twoTierTree() =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      // Tier 2 root t over the tier-1 list u1 -> u2 -> u3. The tier-1 entries point at a placeholder,
      // which the join never reads.
      tx0 = client.newTransaction()
      _ = tx0.lockRevision(radicle, ikvos.revision)
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

      tx1 = client.newTransaction()
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
      audit = new PinAudit(client, enforce = false)
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
