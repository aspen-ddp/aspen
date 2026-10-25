package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{IntegerKeyOrdering, Key, Value}

class JoinFinalizationActionSuite extends IntegrationTestSuite:

  // The upper-tier delete of a join reads the right node before it stages anything, so a commit
  // that does not wait for the staged delete commits without it.
  atest("An upper-tier join commits only after its delete is staged"):
    val k5 = Key(5)
    val k9 = Key(9)
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
