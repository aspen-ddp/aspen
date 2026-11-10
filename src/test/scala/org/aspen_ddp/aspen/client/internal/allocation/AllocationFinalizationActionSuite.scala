package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.{RevisionLock, TransactionDescription, TransactionId}

import java.util.UUID
import scala.concurrent.Future

/** AllocationFinalizationAction's allocation tree insert. Finalization actions run at least once, possibly long after
  * their transaction was finalized: a run must succeed whatever became of the object meanwhile.
  */
class AllocationFinalizationActionSuite extends IntegrationTestSuite:

  private val reason = "cannot re-add once the object is gone: its revision lock fails"

  private def allocate(): Future[KeyValueObjectPointer] =
    for
      ptr <- client.transactUntilSuccessful: tx =>
               given Transaction = tx
               client.getStoragePool(Radicle.poolId).flatMap: pool =>
                 new PoolObjectAllocator(client, pool).allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](1))))
      _ <- waitForTransactionsToComplete()
    yield ptr

  private def txdFor(ptr: KeyValueObjectPointer): TransactionDescription =
    TransactionDescription(
      transactionId = TransactionId(UUID.randomUUID()),
      startTimestamp = HLCTimestamp.now,
      primaryObject = ptr,
      designatedLeaderUID = 0.toByte,
      requirements = List(RevisionLock(ptr, ObjectRevision(TransactionId(UUID.randomUUID())))),
      finalizationActions = Nil,
      primaryObjectIDA = net.ida,
      poolIDAMap = Map(ptr.poolId -> net.ida))

  private def run(ptr: KeyValueObjectPointer): Future[Unit] =
    val action = new AllocationFinalizationAction(client, txdFor(ptr), ptr)
    action.execute()
    for
      _ <- action.complete
      _ <- waitForTransactionsToComplete()
    yield ()

  private def entry(ptr: KeyValueObjectPointer): Future[Option[List[Byte]]] =
    for
      pool <- client.getStoragePool(ptr.poolId)
      vs <- pool.allocationTree.get(Key(ptr.id.toBytes))
    yield vs.map(_.value.bytes.toList)

  /** Removes the object's allocation tree entry, as if its AllocationFinalizationAction had not run yet */
  private def removeEntry(ptr: KeyValueObjectPointer): Future[Unit] =
    for
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             client.getStoragePool(ptr.poolId).flatMap(_.allocationTree.delete(Key(ptr.id.toBytes)))
      _ <- waitForTransactionsToComplete()
    yield ()

  atest("an allocation FA run a second time after the first committed completes without change"):
    for
      ptr <- allocate()
      _ <- removeEntry(ptr)
      state <- client.read(ptr)
      audit = new PinAudit(client, enforce = false)
      _ <- run(ptr)
      first = audit.builtExceptMarks
      afterFirst <- entry(ptr)
      _ <- run(ptr)
      _ = audit.stop()
      afterSecond <- entry(ptr)
    yield
      afterFirst shouldBe Some(ptr.toArray.toList)
      first should not be empty
      audit.unpinned shouldBe empty
      audit.replaySafeWith(reason).size shouldBe first.size
      first.foreach(_.description.requirements should contain (RevisionLock(ptr, state.revision)))
      // The entry is present with this value: the second run stages nothing
      audit.builtExceptMarks.size shouldBe first.size
      afterSecond shouldBe afterFirst

  atest("an allocation FA run after its object was updated completes"):
    for
      ptr <- allocate()
      _ <- removeEntry(ptr)
      before <- client.read(ptr)
      tx = client.newTransaction()
      _ = tx.update(ptr, Some(before.revision), None, Nil, List(Insert(Key(2), Array[Byte](2))))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      updated <- client.read(ptr)
      audit = new PinAudit(client, enforce = false)
      _ <- run(ptr)
      _ = audit.stop()
      after <- entry(ptr)
    yield
      after shouldBe Some(ptr.toArray.toList)
      audit.unpinned shouldBe empty
      audit.built.flatMap(_.description.requirements) should contain (RevisionLock(ptr, updated.revision))

  atest("an allocation FA run after its object was deleted completes without writing"):
    for
      ptr <- allocate()
      before <- client.read(ptr)
      tx = client.newTransaction()
      _ = tx.setRefcount(ptr, before.refcount, before.refcount.setCount(0))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      gone <- entry(ptr)
      audit = new PinAudit(client, enforce = false)
      _ <- run(ptr)
      _ = audit.stop()
      after <- entry(ptr)
    yield
      gone shouldBe None
      audit.builtExceptMarks shouldBe empty
      after shouldBe None


  // What makes a replay of the action's transaction harmless: once the object is deleted its revision lock fails, on
  // every backend (MapBackend keeps the zero-refcount state; the stores check it as missing)
  atest("a revision lock on a deleted object aborts the transaction"):
    for
      ptr <- allocate()
      other <- allocate()
      before <- client.read(ptr)
      tx = client.newTransaction()
      _ = tx.setRefcount(ptr, before.refcount, before.refcount.setCount(0))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      otherState <- client.read(other)
      replay = client.newTransaction()
      _ = replay.lockRevision(ptr, before.revision)
      _ = replay.update(other, Some(otherState.revision), None, Nil, List(Insert(Key(3), Array[Byte](3))))
      result <- replay.commit().failed
      _ <- waitForTransactionsToComplete()
      after <- client.read(other)
    yield
      result shouldBe a [org.aspen_ddp.aspen.client.TransactionAborted]
      after.contents.contains(Key(3)) shouldBe false
