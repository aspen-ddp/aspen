package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.common.metadata.HostId

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ReadLocks, StoragePool,
                                   ObjectState as ClientObjectState}
import org.aspen_ddp.aspen.client.internal.transaction.{MissedUpdateFinalizationAction, UnackedCommitEntry}
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer,
                                           ObjectId, ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.{ReconcileResult, RepairSnapshot}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import scribe.Logging

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

/** What one store's scan found. `seen` and `unackedSeen` drive pacing: any entry at all, repaired, deferred or
  * skipped, means the store is not quiet and should be scanned again at the floor.
  *
  * @param seen errorTree entries seen
  * @param repaired entries of either tree repaired and removed
  * @param deferred entries of either tree left for a later pass
  * @param unackedSeen UnackedCommits entries seen
  */
case class ScanResult(seen: Int, repaired: Int, deferred: Int, unackedSeen: Int = 0):
  def foundEntries: Boolean = seen > 0 || unackedSeen > 0

object ScanResult:
  val Empty: ScanResult = ScanResult(0, 0, 0)

/** Lock deferrals of one store's repair entries, kept in memory across its scans by RepairService. A host restart
  * forgets them: the next pass then reconciles each whole object again and restarts the count.
  *
  * Keyed per entry and object (StoreRepairer.errorDeferralKey, unackedDeferralKey). Only parts deferred by a lock
  * count: `deferredKeys` and `objectLevelDeferred`, the latter including a refcount deferred under an object lock. A
  * pass that is only `stale` defers nothing by a lock: the object is retried on the next pass, but the pass neither
  * counts toward `RepairPolicy.proofAfterDeferrals` nor triggers a proof, since a proof cannot help an object whose
  * copy is busy with an outstanding write. Safe to call from any thread.
  */
final class DeferralState:

  private var entries: Map[Key, DeferralState.Deferral] = Map()

  /** Records one pass's reconciliation of the entry's object. Returns the count of consecutive passes in which
    * something was deferred by a lock, this one included. It resets, and the entry is forgotten, when a pass defers
    * nothing by a lock (it is complete, or only stale): 0 is returned then.
    */
  def record(entryKey: Key, result: ReconcileResult): Int = synchronized:
    if result.deferredKeys.isEmpty && !result.objectLevelDeferred then
      entries -= entryKey
      0
    else
      val count = entries.get(entryKey).map(_.count).getOrElse(0) + 1
      // The next pass may reconcile only the deferred keys once everything else of the object is settled
      val only = Option.when(!result.objectLevelDeferred && !result.stale)(result.deferredKeys)
      entries += entryKey -> DeferralState.Deferral(count, only)
      count

  /** Records the result of the reconcile that followed a proof on the same pass, in place of that pass's pre-proof
    * result: the count is kept (the pass was already counted), the entry is forgotten if nothing is left deferred by a
    * lock, and otherwise the next pass reconciles only what is still deferred
    */
  def afterProof(entryKey: Key, result: ReconcileResult): Unit = synchronized:
    if result.deferredKeys.isEmpty && !result.objectLevelDeferred then
      entries -= entryKey
    else
      entries.get(entryKey).foreach: d =>
        val only = Option.when(!result.objectLevelDeferred && !result.stale)(result.deferredKeys)
        entries += entryKey -> d.copy(only = only)

  def clear(entryKey: Key): Unit = synchronized:
    entries -= entryKey

  /** The keys the next pass reconciles, when the last pass deferred only keys and left nothing else undone. None
    * means the whole object
    */
  def deferredKeys(entryKey: Key): Option[Set[Key]] = synchronized:
    entries.get(entryKey).flatMap(_.only)

  /** Forgets every entry not in `entryKeys`: those a completed scan did not see are gone or were rewritten */
  private[repair] def retain(entryKeys: Set[Key]): Unit = synchronized:
    entries = entries.filter((k, _) => entryKeys.contains(k))

  private[repair] def size: Int = synchronized(entries.size)

object DeferralState:
  private final case class Deferral(count: Int, only: Option[Set[Key]])

/** Repairs one store, once. Walks the store's slice of its pool's errorTree, then of its UnackedCommits tree,
  * brings each recorded object back into agreement with the rest of the system, and removes the entry.
  *
  * Parts of an object deferred by a lock are retried on later passes, only those keys when nothing else was left
  * undone. Once they have been deferred for `RepairPolicy.proofAfterDeferrals` consecutive passes, the scan proves
  * them with a transaction (RepairProofs) and, if it commits, reconciles them from the read the proof used.
  */
class StoreRepairer(client: AspenClient, target: RepairTarget,
                    /** The host the scans run on. When given, a store is scanned only while its pool's entry names
                     *  this host: a copy left on a device declared failed, or otherwise superseded, must not consume
                     *  the entries the store's current copy needs. None (tests driving one network) skips the check */
                    hostId: Option[HostId] = None,
                    /** Test seam: runs between a repair's read and its install */
                    testingOnlyBeforeReconcile: (StoreId, ObjectPointer) => Future[Unit] = (_, _) => Future.unit,
                    /** Test seam: told the parts each proof run proved */
                    testingOnlyOnProof: (StoreId, ObjectPointer, ReadLocks) => Unit = (_, _, _) => ())
                   (using ec: ExecutionContext) extends Logging:

  import StoreRepairer.*

  private final class Counts:
    val seen = AtomicInteger(0)
    val repaired = AtomicInteger(0)
    val deferred = AtomicInteger(0)
    val unackedSeen = AtomicInteger(0)
    private var touched = Set[Key]()

    def touch(keys: Iterable[Key]): Unit = synchronized { touched ++= keys }
    def touchedKeys: Set[Key] = synchronized(touched)
    def result: ScanResult = ScanResult(seen.get, repaired.get, deferred.get, unackedSeen.get)

  def scan(storeId: StoreId, policy: RepairPolicy, deferral: DeferralState = DeferralState()): Future[ScanResult] =
    val counts = Counts()

    // Both trees' keys start with the poolIndex, so one store's entries are the half-open range [poolIndex,
    // poolIndex+1).
    val min = Key(Array[Byte](storeId.poolIndex))
    val max = Key(Array[Byte]((storeId.poolIndex + 1).toByte))

    ownedHere(storeId).flatMap: owned =>
      if !owned then
        logger.warn(s"Not repairing store $storeId: its pool records it on another host")
        Future.successful(counts.result)
      else
        for
          pool <- client.getStoragePool(storeId.poolId)
          _ <- pool.errorTree.foreachInRange(min, max, repairErrorEntry(pool, storeId, policy, deferral, counts))
          _ <- pool.unackedCommitsTree.foreachInRange(min, max,
                 repairUnackedEntry(pool, storeId, policy, deferral, counts))
        yield
          deferral.retain(counts.touchedKeys)
          counts.result

  /** Whether the pool's entry for the store names this host, read fresh for each scan */
  private def ownedHere(storeId: StoreId): Future[Boolean] = hostId match
    case None => Future.successful(true)
    case Some(h) =>
      client.getStoragePoolState(storeId.poolId).map: pool =>
        storeId.poolIndex < pool.stores.length && pool.stores(storeId.poolIndex).hostId == h

  /** The store left this host mid-scan. foreachInRange has no early exit so the remaining tree-node reads still
    * happen, but those are cheap next to the per-object read and transactions each remaining entry would otherwise
    * cost. Entries stay put for whichever host owns the store now.
    */
  private def abandoned(storeId: StoreId): Boolean =
    if target.isRepairable(storeId) then
      false
    else
      logger.debug(s"Abandoning repair scan of $storeId: no longer repairable here")
      true

  private def repairErrorEntry(pool: StoragePool, storeId: StoreId, policy: RepairPolicy, deferral: DeferralState,
                               counts: Counts)
                              (node: KeyValueListNode, key: Key, value: ValueState): Future[Unit] =
    if abandoned(storeId) then
      Future.unit
    else
      // Wrap synchronous portion to convert throws into failed Futures. decodeObjectId,
      // decodeErrorEntry and ObjectPointer construction all decode and can throw on malformed
      // input; a throw that
      // escapes before a Future is produced would hang the scan instead of letting foreachInRange
      // log the failure and continue.
      Future.unit.flatMap: _ =>
        counts.seen.incrementAndGet()
        val objectId = decodeObjectId(key)
        val (missedTimestamp, storePointer) = MissedUpdateFinalizationAction.decodeErrorEntry(value.value)
        val entryKey = errorDeferralKey(key, value.revision)
        counts.touch(List(entryKey))

        for
          oAllocation <- pool.allocationTree.get(Key(objectId.toBytes))
          _ <- oAllocation match
            case Some(allocation) =>
              repairUpdate(storeId, ObjectPointer(allocation.value.bytes), missedTimestamp, entryKey, policy,
                           deferral).flatMap: result =>
                if result.complete then
                  deferral.clear(entryKey)
                  deleteEntryIfUnchanged(value.revision, node, key).map(_ => counts.repaired.incrementAndGet())
                else
                  counts.deferred.incrementAndGet()
                  Future.unit

            case None =>
              // Absent from the allocation tree means either the object was deleted or its
              // allocation has yet to be recorded. Only the first is safe to act on; the age of
              // the error entry is what tells them apart.
              if errorEntryMayBeDeleted(value.timestamp, HLCTimestamp.now,
                                        policy.minErrorEntryAgeForDeletion) then
                // The entry value holds the storePointer bytes captured when the update was
                // missed -- the only surviving copy, since deletion removed the allocation tree
                // entry. Empty means "delete by ObjectId alone".
                for
                  _ <- repairDeletion(storeId, objectId, storePointer)
                  _ <- deleteEntryIfUnchanged(value.revision, node, key)
                yield
                  counts.repaired.incrementAndGet()
              else
                logger.debug(s"Deferring repair of $objectId on $storeId: absent from the " +
                             s"allocation tree but its error entry is too recent to treat as a " +
                             s"deletion")
                counts.deferred.incrementAndGet()
                Future.unit
        yield ()

  /** A UnackedCommits entry: the store missed transaction T, whose change to the entry's objects left nothing for
    * an errorTree comparison to see. Skipped while the store holds T live: its Tx applies T (recovery). Otherwise each
    * object is reconciled, or deleted from the store when its allocation is gone. As for errorTree entries, that
    * needs the entry to be older than `policy.minErrorEntryAgeForDeletion`: an allocation's RefcountUpdate is
    * traceless, so a store that missed it gets an entry before AllocationFinalizationAction records the allocation,
    * and an object absent from the allocation tree meanwhile is live. A younger one is left for a later pass. The
    * entry is deleted once every object is complete.
    */
  private def repairUnackedEntry(pool: StoragePool, storeId: StoreId, policy: RepairPolicy,
                                   deferral: DeferralState, counts: Counts)
                                  (node: KeyValueListNode, key: Key, value: ValueState): Future[Unit] =
    if abandoned(storeId) then
      Future.unit
    else
      Future.unit.flatMap: _ =>
        counts.unackedSeen.incrementAndGet()
        val txid = UnackedCommitEntry.decodeTransactionId(key)
        if target.holdsTransaction(storeId, txid) then
          logger.debug(s"Skipping UnackedCommits entry of $txid on $storeId: the store holds it live")
          counts.deferred.incrementAndGet()
          Future.unit
        else
          val entry = UnackedCommitEntry.decode(value.value.bytes)
          val entryKeys = entry.objects.map(ptr => ptr -> unackedDeferralKey(key, ptr.id))
          counts.touch(entryKeys.map(_._2))

          // One object at a time, as errorTree entries are
          val completed = entryKeys.foldLeft(Future.successful(true)): (f, objectEntry) =>
            val (ptr, entryKey) = objectEntry
            f.flatMap: allComplete =>
              pool.allocationTree.get(Key(ptr.id.toBytes)).flatMap:
                case None if errorEntryMayBeDeleted(value.timestamp, HLCTimestamp.now,
                                                    policy.minErrorEntryAgeForDeletion) =>
                  repairDeletion(storeId, ptr.id, ptr.storePointer).map(_ => allComplete)
                case None =>
                  logger.debug(s"Deferring repair of ${ptr.id} on $storeId: absent from the allocation tree but its " +
                               s"UnackedCommits entry is too recent to treat as a deletion")
                  Future.successful(false)
                case Some(_) =>
                  repairUpdate(storeId, ptr, entry.timestamp, entryKey, policy, deferral)
                    .map(result => allComplete && result.complete)

          completed.flatMap: allComplete =>
            if allComplete then
              entryKeys.foreach((_, entryKey) => deferral.clear(entryKey))
              deleteEntryIfUnchanged(value.revision, node, key).map: _ =>
                logger.info(s"Repair complete for UnackedCommits entry of $txid on store $storeId")
                counts.repaired.incrementAndGet()
            else
              counts.deferred.incrementAndGet()
              Future.unit

  /** Reconciles the store's copy of the object with a read of it (Reconcile): snapshot the store, read, then install
    * what still matches the snapshot. A read older than `minTimestamp`, the update the entry records, comes back as a
    * stale result, so `complete` says exactly whether the entry may be dropped. The result is recorded in `deferral`
    * under `entryKey`: a later pass reconciles only the keys deferred before, and once parts have been deferred by a
    * lock for `policy.proofAfterDeferrals` passes they are proven (RepairProofs) and, where proven, reconciled from
    * the same read.
    */
  private def repairUpdate(storeId: StoreId, ptr: ObjectPointer, minTimestamp: HLCTimestamp, entryKey: Key,
                           policy: RepairPolicy, deferral: DeferralState): Future[ReconcileResult] =
    val snapshot = Promise[RepairSnapshot]()
    target.snapshot(storeId, ptr, snapshot)

    def reconcile(snap: RepairSnapshot, os: ClientObjectState): Future[ReconcileResult] =
      val reconciled = Promise[ReconcileResult]()
      target.reconcile(storeId, snap, os, reconciled)
      reconciled.future.map: installed =>
        if os.timestamp >= minTimestamp then installed else installed.copy(stale = true)

    for
      taken <- snapshot.future
      snap = taken.copy(only = deferral.deferredKeys(entryKey))
      // A full read: only full reads report the stores' locks
      os <- ptr match
        case kp: KeyValueObjectPointer => client.read(kp)
        case dp: DataObjectPointer => client.read(dp)
      _ <- testingOnlyBeforeReconcile(storeId, ptr)
      first <- reconcile(snap, os)
      passes = deferral.record(entryKey, first)
      result <-
        if passes < policy.proofAfterDeferrals then
          Future.successful(first)
        else
          logger.info(s"Repair of object ${ptr.id} on store $storeId deferred by locks for $passes passes " +
                      s"($first): proving the deferred parts")
          prove(os, first).flatMap: proven =>
            testingOnlyOnProof(storeId, ptr, proven)
            if proven.keys.isEmpty && !proven.objectLevel then Future.successful(first)
            else reconcile(snap.copy(proven = proven), os).map: afterProof =>
              deferral.afterProof(entryKey, afterProof)
              afterProof
    yield
      if result.complete then
        logger.info(s"Repair complete for object ${ptr.id} on store $storeId")
      else
        logger.info(s"Repair of object ${ptr.id} on store $storeId left $result. Leaving the entry for the next pass")
      result

  /** Proves the parts `result` left deferred by a lock, from `read`, one proof at a time: proofs of one object would
    * otherwise collide with each other. Completes with the parts proven
    */
  private def prove(read: ClientObjectState, result: ReconcileResult): Future[ReadLocks] =
    val keyProofs: List[() => Future[Option[Key]]] = read match
      case kv: KeyValueObjectState =>
        result.deferredKeys.toList.map: key =>
          () => RepairProofs.proveKey(client, kv.pointer, key, kv.contents.get(key).map(_.revision))
                  .map(Option.when(_)(key))
      case _ => Nil

    // A refcount deferred under an object lock is proven on either object type; a KeyValue object's revision, min,
    // max, left and right by a FullContentLock as well
    val objectProofs: List[() => Future[Boolean]] =
      if !result.objectLevelDeferred then Nil
      else read match
        case kv: KeyValueObjectState =>
          List(() => RepairProofs.proveRefcount(client, kv.pointer, kv.refcount),
               () => RepairProofs.proveObjectLevel(client, kv))
        case _ =>
          List(() => RepairProofs.proveRefcount(client, read.pointer, read.refcount))

    for
      keys <- keyProofs.foldLeft(Future.successful(Set[Key]())): (f, proof) =>
                f.flatMap(proven => proof().map(proven ++ _))
      objectLevel <- objectProofs.foldLeft(Future.successful(objectProofs.nonEmpty)): (f, proof) =>
                       f.flatMap(proven => if proven then proof() else Future.successful(false))
    yield
      ReadLocks(keys, objectLevel)

  private def repairDeletion(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte]): Future[Unit] =
    val completion = Promise[Unit]()
    target.repairDelete(storeId, objectId, storePointer, completion)
    completion.future.map: _ =>
      logger.info(s"Repair complete (deletion) for object $objectId on store $storeId")

  /** Deletion of the entry is best effort. A transient error or a collision only costs another
    * pass. The revision guard is the point: the caller compared the repaired state against the
    * missed timestamp it scanned, and a store that misses another update while the repair is
    * underway rewrites the entry with that newer timestamp. Requiring the scanned revision makes
    * the delete fail in that case, which is how the next pass learns to try again. The KeyRevision of the deleted
    * key is also the transaction's pin, so a replay deletes no newer entry.
    */
  private def deleteEntryIfUnchanged(entryRevision: ObjectRevision,
                                     node: KeyValueListNode, key: Key): Future[Unit] =
    val tx = client.newRepairTransaction()
    val fdelete = node.delete(key,
      None,
      List(KeyValueUpdate.KeyRevision(key, entryRevision)),
      (_, _) => Future.unit)(using tx)
    for
      _ <- fdelete
      _ <- tx.commit()
    yield ()

object StoreRepairer:

  /** The DeferralState key of an errorTree entry: the entry's key and value revision. A store that misses another
    * update rewrites the entry, and the rewritten entry starts over with its whole object
    */
  private[repair] def errorDeferralKey(entryKey: Key, entryRevision: ObjectRevision): Key =
    Key(Array[Byte](0) ++ entryKey.bytes ++ entryRevision.toArray)

  /** The DeferralState key of one object of a UnackedCommits entry */
  private[repair] def unackedDeferralKey(entryKey: Key, objectId: ObjectId): Key =
    Key(Array[Byte](1) ++ entryKey.bytes ++ objectId.toBytes)

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
