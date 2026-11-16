package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.network.{TxMessage, TxResolved}
import org.aspen_ddp.aspen.common.transaction.{ExclusiveRevisionLock, SharedRevisionLock, TransactionDescription, TransactionId}

import java.util.UUID
import scala.concurrent.Future

/** AllocationFinalizationAction's allocation tree insert. Finalization actions run at least once, possibly long after
  * their transaction was finalized: a run must succeed whatever became of the object meanwhile.
  */
class AllocationFinalizationActionSuite extends IntegrationTestSuite:

  private val reason = "cannot add the entry once the object is deleted: its revision lock fails"

  private def allocate(): Future[KeyValueObjectPointer] =
    for
      ptr <- client.transactUntilSuccessful: tx =>
               given Transaction = tx
               tx.replaySafe("test")
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
      requirements = List(ExclusiveRevisionLock(ptr, ObjectRevision(TransactionId(UUID.randomUUID())))),
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
      audit = new PinAudit(client)
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
      first.foreach(_.description.requirements should contain (SharedRevisionLock(ptr, state.revision)))
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
      audit = new PinAudit(client)
      _ <- run(ptr)
      _ = audit.stop()
      after <- entry(ptr)
    yield
      after shouldBe Some(ptr.toArray.toList)
      audit.unpinned shouldBe empty
      audit.built.flatMap(_.description.requirements) should contain (SharedRevisionLock(ptr, updated.revision))

  // A run after the object's deletion: the read proves it deleted, so no entry is recorded and no orphan is left
  // (transaction-requirements Q-N5). A run that reads it before the stores apply the allocation retries instead
  // (FrontendSuite "a read of an allocation voted for but not applied answers AllocationPending")
  atest("a run after the object's deletion records no entry"):
    for
      ptr <- allocate()
      _ <- removeEntry(ptr)
      before <- client.read(ptr)
      tx = client.newTransaction()
      _ = tx.setRefcount(ptr, before.refcount, before.refcount.setCount(0))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      audit = new PinAudit(client)
      _ <- run(ptr)
      _ = audit.stop()
      after <- entry(ptr)
    yield
      after shouldBe None
      audit.builtExceptMarks shouldBe empty

  // The allocating transaction's own action runs as soon as it resolves. Its stores have not applied the allocation
  // yet when it reads (here they never hear the outcome until the test delivers it): each answers AllocationPending,
  // which proves nothing, so the action retries and records nothing meanwhile. Taking the miss as a deletion would
  // skip the entry; recording it without a lock would leave an orphan on a later run (transaction-requirements Q-N5)
  atest("the action retries until the stores apply the allocation, then records the entry under a lock"):
    @volatile var target: Option[TransactionId] = None
    val held = new java.util.concurrent.ConcurrentLinkedQueue[TxMessage]()
    net.dropTxMessage = (m: TxMessage) => m match
      case r: TxResolved if target.contains(r.transactionId) => held.add(r); true
      case _ => false
    val audit = new PinAudit(client)
    val tx = client.newTransaction()
    tx.replaySafe("test")
    target = Some(tx.id)
    def pumpFor(millis: Long): Future[Unit] =
      val until = System.nanoTime() + millis * 1000000
      def loop(): Future[Unit] =
        if System.nanoTime() > until then Future.unit
        else Future { Thread.sleep(1); net.handleEvents() }.flatMap(_ => loop())
      loop()
    for
      pool <- client.getStoragePool(Radicle.poolId)
      ptr <- new PoolObjectAllocator(client, pool).allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](1))))(using
               tx)
      _ <- tx.commit()
      _ <- pumpFor(300)
      during <- entry(ptr)
      _ = net.dropTxMessage = _ => false
      _ = held.forEach(m => net.deliverTransactionMessage(m))
      _ <- waitForTransactionsToComplete()
      _ = audit.stop()
      after <- entry(ptr)
      state <- client.read(ptr)
    yield
      held should not be empty
      during shouldBe None
      after shouldBe Some(ptr.toArray.toList)
      audit.replaySafeWith(reason).flatMap(_.description.requirements) should contain (
        SharedRevisionLock(ptr, state.revision))

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
      _ = replay.sharedLockRevision(ptr, before.revision)
      _ = replay.update(other, Some(otherState.revision), None, Nil, List(Insert(Key(3), Array[Byte](3))))
      result <- replay.commit().failed
      _ <- waitForTransactionsToComplete()
      after <- client.read(other)
    yield
      result shouldBe a [org.aspen_ddp.aspen.client.TransactionAborted]
      after.contents.contains(Key(3)) shouldBe false
