package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.{AspenClient, StoragePool, Transaction}
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer,
                                           KeyValueOperation, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import scribe.Logging

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

/** What one store's scan found. `seen` drives pacing: any entry at all, repaired or deferred,
  * means the store is not quiet and should be scanned again at the floor.
  */
case class ScanResult(seen: Int, repaired: Int, deferred: Int):
  def foundEntries: Boolean = seen > 0

object ScanResult:
  val Empty: ScanResult = ScanResult(0, 0, 0)

/** Repairs one store, once. Walks the store's slice of its pool's errorTree, brings each
  * recorded object back into agreement with the rest of the system, and removes the entry.
  */
class StoreRepairer(client: AspenClient, target: RepairTarget)
                   (using ec: ExecutionContext) extends Logging:

  import StoreRepairer.*

  def scan(storeId: StoreId, policy: RepairPolicy): Future[ScanResult] =
    val seen = AtomicInteger(0)
    val repaired = AtomicInteger(0)
    val deferred = AtomicInteger(0)

    // errorTree keys are poolIndex :: objectId, so one store's entries are the half-open range
    // [poolIndex, poolIndex+1).
    val min = Key(Array[Byte](storeId.poolIndex))
    val max = Key(Array[Byte]((storeId.poolIndex + 1).toByte))

    for
      pool <- client.getStoragePool(storeId.poolId)
      _ <- pool.errorTree.foreachInRange(min, max,
             repairOne(pool, storeId, policy, seen, repaired, deferred))
    yield
      ScanResult(seen.get, repaired.get, deferred.get)

  private def repairOne(pool: StoragePool, storeId: StoreId, policy: RepairPolicy,
                        seen: AtomicInteger, repaired: AtomicInteger, deferred: AtomicInteger)
                       (node: KeyValueListNode, key: Key, value: ValueState): Future[Unit] =
    if !target.repairableStoreIds.contains(storeId) then
      // The store left this host mid-scan. foreachInRange has no early exit so the remaining
      // tree-node reads still happen, but those are cheap next to the per-object read and two
      // transactions each remaining entry would otherwise cost. Entries stay put for whichever
      // host owns the store now.
      logger.debug(s"Abandoning repair scan of $storeId: no longer repairable here")
      Future.unit
    else
      seen.incrementAndGet()
      val objectId = decodeObjectId(key)

      for
        oAllocation <- pool.allocationTree.get(Key(objectId.toBytes))
        _ <- oAllocation match
          case Some(allocation) =>
            repairUpdate(storeId, ObjectPointer(allocation.value.bytes), node, key).map: _ =>
              repaired.incrementAndGet()
              ()

          case None =>
            // Absent from the allocation tree means either the object was deleted or its
            // allocation has yet to be recorded. Only the first is safe to act on; the age of
            // the error entry is what tells them apart.
            if errorEntryMayBeDeleted(value.timestamp, HLCTimestamp.now,
                                      policy.minErrorEntryAgeForDeletion) then
              // The entry value holds the storePointer bytes captured when the update was
              // missed -- the only surviving copy, since deletion removed the allocation tree
              // entry. Empty means "delete by ObjectId alone".
              repairDeletion(storeId, objectId, value.value.bytes, node, key).map: _ =>
                repaired.incrementAndGet()
                ()
            else
              logger.debug(s"Deferring repair of $objectId on $storeId: absent from the " +
                           s"allocation tree but its error entry is too recent to treat as a " +
                           s"deletion")
              deferred.incrementAndGet()
              Future.unit
      yield ()

  private def repairUpdate(storeId: StoreId, ptr: ObjectPointer,
                           node: KeyValueListNode, key: Key): Future[Unit] =
    val fos = ptr match
      case kp: KeyValueObjectPointer => client.read(kp)
      case dp: DataObjectPointer => client.read(dp)

    val completion = Promise[Unit]()

    for
      os <- fos
      _ = target.repair(storeId, os, completion)
      _ <- completion.future
      _ <- deleteErrorEntryIfUnchanged(os.timestamp, node, key)
    yield
      logger.info(s"Repair complete for object ${ptr.id} on store $storeId")

  private def repairDeletion(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte],
                             node: KeyValueListNode, key: Key): Future[Unit] =
    val completion = Promise[Unit]()
    target.repairDelete(storeId, objectId, storePointer, completion)

    for
      _ <- completion.future
      _ <- deleteErrorEntry(node, key)
    yield
      logger.info(s"Repair complete (deletion) for object $objectId on store $storeId")

  private def deleteErrorEntry(node: KeyValueListNode, key: Key): Future[Unit] =
    val tx = client.newRepairTransaction()
    val fdelete = node.delete(key)(using tx)
    for
      _ <- fdelete
      _ <- tx.commit()
    yield ()

  /** Deletion of the entry is best effort. A transient error or a collision only costs another
    * pass. The timestamp guard is the point: if the entry's timestamp moved between reading the
    * object and writing it to the store, another update was missed in that window, and letting
    * the delete fail is how the next pass learns to try again.
    */
  private def deleteErrorEntryIfUnchanged(timestamp: HLCTimestamp,
                                          node: KeyValueListNode, key: Key): Future[Unit] =
    val tx = client.newRepairTransaction()
    val fdelete = node.delete(key,
      None,
      List(KeyValueUpdate.TimestampLessThan(key, timestamp)),
      (_, _) => Future.unit)(using tx)
    for
      _ <- fdelete
      _ <- tx.commit()
    yield ()

object StoreRepairer:

  /** Decodes the ObjectId from an errorTree key: one poolIndex byte then the UUID, big-endian. */
  private[repair] def decodeObjectId(key: Key): ObjectId =
    val bb = ByteBuffer.wrap(key.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.get() // poolIndex
    val msb = bb.getLong()
    val lsb = bb.getLong()
    ObjectId(new UUID(msb, lsb))

  /** Whether an errorTree entry has sat long enough that its object's absence from the
    * allocation tree can be read as proof of deletion.
    *
    * An absent allocationTree entry has two possible meanings: the object was deleted, or the
    * allocating transaction's AllocationFinalizationAction has not landed yet -- it runs
    * concurrently with the MissedUpdateFinalizationAction that wrote the errorTree entry, and
    * both may re-run after a crash. Acting on the second case would drop the errorTree entry for
    * a live object, leaving the store silently short a slice with no record of it.
    *
    * Repair is explicitly not latency sensitive, so we wait long enough to make that race
    * implausible. Waiting costs one more pass; guessing wrong costs a replica.
    *
    * A future dated entry yields a negative age and is therefore ineligible, which is what we
    * want -- clock skew is not evidence that anything was deleted.
    */
  def errorEntryMayBeDeleted(entryTimestamp: HLCTimestamp,
                             now: HLCTimestamp,
                             minAge: Duration): Boolean =
    (now - entryTimestamp) >= minAge
