package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.common.metadata.HostId

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, KeyValueObjectState, ReadLocks, StoragePool,
                                   Transaction, ObjectState as ClientObjectState}
import org.aspen_ddp.aspen.client.internal.transaction.{MissedUpdateFinalizationAction, UnackedCommitEntry}
import org.aspen_ddp.aspen.client.tkvl.{KeyValueListNode, TieredKeyValueList}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer,
                                           ObjectId, ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.{Frontend, ReconcileResult, RepairSnapshot}
import org.aspen_ddp.aspen.server.store.backend.RepairSlotOccupied
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionId}
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

/** What one sweep of a bootstrap store's copies of the bootstrap pool's own ErrorTree and UnackedCommits nodes did
  * (StoreRepairer.sweepBootstrapTrees).
  *
  * @param nodes nodes reconciled or attempted
  * @param reconciled nodes whose reconcile changed the store's copy
  * @param incomplete nodes left not complete: deferred, stale, or failed (unreadable, or deleted under the walk)
  * @param walkComplete false when either tree's walk missed nodes deleted under it
  *
  * A node skipped because the store stopped being repairable here (left the host, went offline, is rebuilding) counts
  * as incomplete: that may last only until the next tick, so the sweep reports needsAnotherPass = true and the store
  * keeps its short scan interval. If the store has left for good it drops out of the repairable set anyway.
  */
final case class SweepResult(nodes: Int, reconciled: Int, incomplete: Int, walkComplete: Boolean):
  def needsAnotherPass: Boolean = reconciled > 0 || incomplete > 0 || !walkComplete

object SweepResult:
  val Empty: SweepResult = SweepResult(0, 0, 0, walkComplete = true)

/** Lock deferrals of one store's repair entries, and its suspected orphan stubs (`confirmOrphans`), kept in memory
  * across its scans by RepairService. A host restart forgets them: the next pass then reconciles each whole object
  * again and restarts the count, and an orphan stub needs two more scans to be dropped.
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

  /** Forgets every entry not in `entryKeys`: those a completed scan did not see are gone or were rewritten. Sweep keys
    * (StoreRepairer.sweepDeferralKey) are kept: they belong to the sweep, which prunes them with `retainSweep`
    */
  private[repair] def retain(entryKeys: Set[Key]): Unit = synchronized:
    entries = entries.filter((k, _) => StoreRepairer.isSweepDeferralKey(k) || entryKeys.contains(k))

  /** Forgets every sweep key not in `sweepKeys`: the nodes a complete sweep did not visit are gone */
  private[repair] def retainSweep(sweepKeys: Set[Key]): Unit = synchronized:
    entries = entries.filter((k, _) => !StoreRepairer.isSweepDeferralKey(k) || sweepKeys.contains(k))

  private[repair] def size: Int = synchronized(entries.size)

  /** The store's applied stubs the last successful scan found with no UnackedCommits entry (Ruling 8) */
  private var orphanSuspects = Set[TransactionId]()

  /** Records the stubs a successful scan found with no entry, and returns those to drop: the ones the previous
    * successful scan found with no entry too. The rest are suspected until the next scan; a stub no longer found
    * orphaned (its entry landed, or it is gone) is no longer suspected
    */
  private[repair] def confirmOrphans(found: Set[TransactionId]): Set[TransactionId] = synchronized:
    val confirmed = found.intersect(orphanSuspects)
    orphanSuspects = found -- confirmed
    confirmed

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
                    testingOnlyOnProof: (StoreId, ObjectPointer, ReadLocks) => Unit = (_, _, _) => (),
                    /** Test seam: sees an entry deletion's transaction just before it commits */
                    testingOnlyBeforeEntryDelete: (PoolId, Transaction) => Unit = (_, _) => ())
                   (using ec: ExecutionContext) extends Logging:

  import StoreRepairer.*

  private final class Counts:
    val seen = AtomicInteger(0)
    val repaired = AtomicInteger(0)
    val deferred = AtomicInteger(0)
    val unackedSeen = AtomicInteger(0)
    private var touched = Set[Key]()
    private var unackedTxids = Set[TransactionId]()

    /** Notes a UnackedCommits entry of the walk, whatever became of it */
    def sawUnacked(txid: TransactionId): Unit = synchronized { unackedTxids += txid }
    def unackedTxidsSeen: Set[TransactionId] = synchronized(unackedTxids)

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
          // Read before the UnackedCommits walk: see dropOrphanStubs
          stubs <- target.appliedStubs(storeId)
          _ <- pool.errorTree.foreachInRange(min, max, repairErrorEntry(pool, storeId, policy, deferral, counts))
          _ <- pool.unackedCommitsTree.foreachInRange(min, max,
                 repairUnackedEntry(pool, storeId, policy, deferral, counts))
          _ <- dropOrphanStubs(pool, storeId, stubs, deferral, counts)
        yield
          deferral.retain(counts.touchedKeys)
          counts.result

  /** Reconciles the store's copy of every node of the bootstrap pool's own errorTree, then of its unackedCommitsTree,
    * every tier. Writes to those nodes run with missed-update tracking off (`limitTracking`: marker writes and entry
    * deletions in these trees), so a bootstrap store that misses one gets no entry: this sweep is what heals it. Each
    * node goes through `repairUpdate` with no minimum timestamp (complete means the store's copy matches the read) and
    * no proofs: a proof is an untracked write that would collide with the marker traffic these nodes carry, so a key
    * locked at every attempt waits for a later sweep. No entries are created or deleted.
    *
    * A node that cannot be read (deleted under the walk by a join or root reduction, or the pool degraded) counts as
    * incomplete, not as a failure of the sweep. Once the store stops being repairable here the remaining nodes are
    * skipped and counted incomplete.
    */
  def sweepBootstrapTrees(storeId: StoreId, policy: RepairPolicy, deferral: DeferralState): Future[SweepResult] =
    if storeId.poolId != PoolId.BootstrapPoolId then
      Future.failed(new IllegalArgumentException(s"Only stores of the bootstrap pool are swept, not $storeId"))
    else
      val nodes = AtomicInteger(0)
      val reconciled = AtomicInteger(0)
      val incomplete = AtomicInteger(0)
      var sweepKeys = Set[Key]()

      def sweepNode(tier: Int, pointer: KeyValueObjectPointer): Future[Unit] =
        if abandoned(storeId) then
          // Not repairable here, which may be only until the next tick (offline, rebuilding): the node was not swept
          incomplete.incrementAndGet()
          Future.unit
        else
          nodes.incrementAndGet()
          val key = sweepDeferralKey(pointer.id)
          synchronized { sweepKeys += key }
          Future.delegate(repairUpdate(storeId, pointer, HLCTimestamp.Zero, key, policy, deferral, allowProof = false))
            .map: result =>
              if result.changed then reconciled.incrementAndGet()
              if !result.complete then incomplete.incrementAndGet()
            .recover:
              case err =>
                logger.info(s"Sweep of tier $tier node ${pointer.id} on $storeId failed: $err")
                incomplete.incrementAndGet()

      ownedHere(storeId).flatMap: owned =>
        if !owned then
          // The entry scan of the same pass has already warned
          logger.debug(s"Not sweeping store $storeId: its pool records it on another host")
          Future.successful(SweepResult.Empty)
        else
          for
            pool <- client.getStoragePool(storeId.poolId)
            errorWalk <- pool.errorTree.foreachNode(sweepNode)
            unackedWalk <- pool.unackedCommitsTree.foreachNode(sweepNode)
          yield
            val walkComplete = errorWalk.complete && unackedWalk.complete
            if walkComplete && !abandoned(storeId) then deferral.retainSweep(synchronized(sweepKeys))
            val result = SweepResult(nodes.get, reconciled.get, incomplete.get, walkComplete)
            if result.needsAnotherPass then
              logger.info(s"Swept the bootstrap trees' nodes on $storeId: $result")
            else
              logger.debug(s"Swept the bootstrap trees' nodes on $storeId: $result")
            result

  /** Drops the stubs of the store that no UnackedCommits entry needs any more: those that this scan and the
    * previous successful one of the store both found orphaned (Ruling 8). A scan finds a stub orphaned when it is in
    * `before`, the store's stubs read before the walk, its transaction had no entry in the walk, and a point lookup
    * of its own entry finds nothing.
    *
    * A stub may be written before its entry is durable: a driver finalized by another driver's TxFinalized lists the
    * stores its own cancelled mark planned, without waiting for that write (Ruling 7). Two consecutive scans give such
    * an entry a whole scan period to land; once it has, the stub is no longer orphaned and the suspicion is dropped.
    * The suspicions live in the store's DeferralState, in memory on the repairing host: a restart, or a scan
    * abandoned when the store left the host, starts them again, which only delays a drop.
    *
    * The pre-walk snapshot still matters. A stub written during the walk is not in `before`, so this scan neither
    * suspects nor drops it: its entry may sit behind the walk's cursor. An entry can also leave a walk unseen
    * without being gone, if the tree is restructured under the walk, and one deleted by another scan is rightly
    * gone: hence the point lookup. The drop names the txid exactly, so a stub is never dropped for another's entry.
    * A scan that fails part way changes no suspicion; one whose store left this host mid-walk does nothing here.
    */
  private def dropOrphanStubs(pool: StoragePool, storeId: StoreId, before: Set[TransactionId],
                              deferral: DeferralState, counts: Counts): Future[Unit] =
    val candidates = before -- counts.unackedTxidsSeen
    if abandoned(storeId) then
      Future.unit
    else
      val found = candidates.foldLeft(Future.successful(Set[TransactionId]())): (f, txid) =>
        f.flatMap: orphans =>
          pool.unackedCommitsTree.get(UnackedCommitEntry.key(storeId, txid)).map: entry =>
            if entry.isEmpty then orphans + txid else orphans
      found.map: orphans =>
        deferral.confirmOrphans(orphans).foreach: txid =>
          logger.info(s"Dropping the applied stub of $txid on $storeId: two scans found no UnackedCommits entry for it")
          target.dropAppliedStub(storeId, txid)

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
        val errorEntry = MissedUpdateFinalizationAction.decodeErrorEntry(value.value)
        val entryKey = errorDeferralKey(key, value.revision)
        counts.touch(List(entryKey))

        def completed(complete: Boolean): Future[Unit] =
          if complete then
            deferral.clear(entryKey)
            deleteEntryIfUnchanged(storeId.poolId, pool.errorTree, value.revision, key)
              .map(_ => counts.repaired.incrementAndGet())
          else
            counts.deferred.incrementAndGet()
            Future.unit

        if target.mayJoinTransaction(storeId, errorEntry.missedTimestamp) then
          // As for UnackedCommits entries: a replay may still create a Tx for the missed transaction here and apply
          // it after this entry's reconcile, and the entry would be gone (transaction-recovery R-N9)
          logger.debug(s"Deferring error entry of $objectId on $storeId: the store may still join its transaction")
          counts.deferred.incrementAndGet()
          Future.unit
        else for
          oAllocation <- pool.allocationTree.get(Key(objectId.toBytes))
          _ <- oAllocation match
            case Some(allocation) =>
              repairUpdate(storeId, ObjectPointer(allocation.value.bytes), errorEntry.missedTimestamp, entryKey, policy,
                           deferral, allowProof = true).flatMap(result => completed(result.complete))

            case None =>
              // Absent from the allocation tree means either the object was deleted or its
              // allocation has yet to be recorded. Only an entry old enough is looked at, and then
              // a read of the object decides (repairAbsent).
              if errorEntryMayBeDeleted(value.timestamp, HLCTimestamp.now,
                                        policy.minErrorEntryAgeForDeletion) then
                // The pointer comes from the entry: deletion removed the allocation tree entry. An entry of unknown
                // type is read as a DataObject: only a read that proves the deletion is acted on then, and error
                // replies decode nothing
                val ptr = errorEntry.pointer(storeId.poolId, objectId)
                repairAbsent(storeId, ptr.getOrElse(DataObjectPointer(objectId, storeId.poolId, errorEntry.storePointer)),
                             errorEntry.missedTimestamp, entryKey, policy, deferral, typeKnown = ptr.isDefined)
                  .flatMap(completed)
              else
                logger.debug(s"Deferring repair of $objectId on $storeId: absent from the " +
                             s"allocation tree but its error entry is too recent to treat as a " +
                             s"deletion")
                counts.deferred.incrementAndGet()
                Future.unit
        yield ()

  /** A UnackedCommits entry: the store cast a commit vote on transaction T and had not acknowledged applying it when
    * T was finalized, so it may be owed T. The entry names every object of the store that T changes, traced or
    * traceless; it may also be left over after the store applied T late, and reconciling such objects is a no-op.
    * Skipped while the store holds T live: its Tx applies T (recovery). Also skipped while T's start is at or above
    * the store's forget floor (RepairTarget.mayJoinTransaction): a replay may still create a Tx for T there and apply
    * it late through this entry, after a reconcile, so the entry must outlive that window. Otherwise each
    * object is reconciled, or, when the allocation tree does not list it, handled by repairAbsent. As for errorTree
    * entries, that needs the entry to be older than `policy.minErrorEntryAgeForDeletion`: an allocation's
    * RefcountUpdate is traceless, so a store that missed it gets an entry before AllocationFinalizationAction records
    * the allocation, and an object absent from the allocation tree meanwhile is live. A younger one is left for a
    * later pass. The entry is deleted once every object is complete.
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
        counts.sawUnacked(txid)
        if target.holdsTransaction(storeId, txid) then
          logger.debug(s"Skipping UnackedCommits entry of $txid on $storeId: the store holds it live")
          counts.deferred.incrementAndGet()
          Future.unit
        else
          val entry = UnackedCommitEntry.decode(value.value.bytes)
          if target.mayJoinTransaction(storeId, entry.timestamp) then
            // A replay may still create a Tx for T here, after the holdsTransaction check above, and apply T late
            // through this entry once its reconcile is done: deleting the entry then would leave that apply
            // unrepaired (transaction-recovery R-N9). Below the store's forget floor no new Tx for T can appear
            logger.debug(s"Deferring UnackedCommits entry of $txid on $storeId: the store may still join it")
            counts.deferred.incrementAndGet()
            Future.unit
          else
            repairUnackedObjects(pool, storeId, policy, deferral, counts, key, value, txid, entry)

  /** The objects of a UnackedCommits entry the scan may act on (repairUnackedEntry), then the entry itself */
  private def repairUnackedObjects(pool: StoragePool, storeId: StoreId, policy: RepairPolicy, deferral: DeferralState,
                                   counts: Counts, key: Key, value: ValueState, txid: TransactionId,
                                   entry: UnackedCommitEntry): Future[Unit] =
      val entryKeys = entry.objects.map(ptr => ptr -> unackedDeferralKey(key, ptr.id))
      counts.touch(entryKeys.map(_._2))

      // One object at a time, as errorTree entries are
      val completed = entryKeys.foldLeft(Future.successful(true)): (f, objectEntry) =>
        val (ptr, entryKey) = objectEntry
        f.flatMap: allComplete =>
          pool.allocationTree.get(Key(ptr.id.toBytes)).flatMap:
            case None if errorEntryMayBeDeleted(value.timestamp, HLCTimestamp.now,
                                                policy.minErrorEntryAgeForDeletion) =>
              repairAbsent(storeId, ptr, entry.timestamp, entryKey, policy, deferral)
                .map(complete => allComplete && complete)
            case None =>
              logger.debug(s"Deferring repair of ${ptr.id} on $storeId: absent from the allocation tree but its " +
                           s"UnackedCommits entry is too recent to treat as a deletion")
              Future.successful(false)
            case Some(_) =>
              repairUpdate(storeId, ptr, entry.timestamp, entryKey, policy, deferral, allowProof = true)
                .map(result => allComplete && result.complete)

      completed.flatMap: allComplete =>
        if allComplete then
          entryKeys.foreach((_, entryKey) => deferral.clear(entryKey))
          deleteEntryIfUnchanged(storeId.poolId, pool.unackedCommitsTree, value.revision, key).map: _ =>
            // Only the head's deletion drops the stub: the lookups that decide whether the store applies a
            // late commit name the head (UnackedCommitEntry.key), so the stub must outlive it, not the other
            // chunks, which only carry more objects to repair
            if UnackedCommitEntry.isHeadKey(key) then target.dropAppliedStub(storeId, txid)
            logger.info(s"Repair complete for UnackedCommits entry of $txid on store $storeId")
            counts.repaired.incrementAndGet()
        else
          counts.deferred.incrementAndGet()
          Future.unit

  /** Repairs an object of an entry old enough (errorEntryMayBeDeleted) that the allocation tree does not list.
    * Absence alone does not prove a deletion: the allocating transaction's AllocationFinalizationAction may simply be
    * late (repair F5), and deleting the slice of a live object would leave the store short with no record of it. So
    * the object is read through its pool:
    *  - a read that proves it is not live (InvalidObject.provesDeletion) drops it from the store;
    *  - a read that restores it shows it live: it is reconciled as if the tree listed it;
    *  - anything else (a read that times out, a corrupted object, a failure that proves nothing) leaves the entry.
    * Without `typeKnown` (an errorTree entry recording no type) `ptr` is a guess, so only a proven deletion is acted on.
    * True when the entry's work on the object is done.
    */
  private def repairAbsent(storeId: StoreId, ptr: ObjectPointer, minTimestamp: HLCTimestamp, entryKey: Key,
                           policy: RepairPolicy, deferral: DeferralState,
                           typeKnown: Boolean = true): Future[Boolean] =
    readObject(ptr).map(_ => Some(true))
      .recover:
        case e: InvalidObject if e.provesDeletion => Some(false)
        case err =>
          logger.info(s"Deferring repair of ${ptr.id} on $storeId: absent from the allocation tree, and a read of " +
                      s"it neither restores it nor proves it deleted: $err")
          None
      .flatMap:
        case Some(false) => repairDeletion(storeId, ptr.id, ptr.storePointer)
        case Some(true) if !typeKnown =>
          logger.error(s"Object ${ptr.id} on $storeId is live but absent from the allocation tree, and its entry " +
                       s"records no object type: leaving the entry")
          Future.successful(false)
        case Some(true) =>
          logger.warn(s"Object ${ptr.id} on $storeId is live but absent from the allocation tree: repairing it")
          repairUpdate(storeId, ptr, minTimestamp, entryKey, policy, deferral, allowProof = true).map(_.complete)
        case None => Future.successful(false)

  private def readObject(ptr: ObjectPointer): Future[ClientObjectState] = ptr match
    case kp: KeyValueObjectPointer => client.read(kp)
    case dp: DataObjectPointer => client.read(dp)

  /** The store's slot for `ptr`, a live object (just read), holds `occupant` (Backend.RepairSlotOccupied): FSIF slot
    * reuse over a missed delete of the occupant (cross-document-review X6, rebuild F18). Two live objects never share
    * a slot, so the occupant is dead; it is dropped once the allocation tree does not list it either, by an
    * id-checked repairDelete. The repair of `ptr` is left stale, so its entry stays and the next pass installs it.
    * A listed occupant is an inconsistency nothing here can settle: it is logged and left.
    */
  private def dropOccupant(storeId: StoreId, ptr: ObjectPointer, occupant: ObjectId): Future[Unit] =
    for
      pool <- client.getStoragePool(ptr.poolId)
      listed <- pool.allocationTree.get(Key(occupant.toBytes))
      _ <-
        if listed.isDefined then
          logger.error(s"Repair of ${ptr.id} on $storeId: its slot holds $occupant, which the allocation tree " +
                       s"still lists. Leaving both")
          Future.unit
        else
          logger.warn(s"Repair of ${ptr.id} on $storeId: its slot holds $occupant, a deleted object. Dropping it")
          repairDeletion(storeId, occupant, ptr.storePointer).map(_ => ())
    yield ()

  /** Reconciles the store's copy of the object with a read of it (Reconcile): snapshot the store, read, then install
    * what still matches the snapshot. A read older than `minTimestamp`, the update the entry records, comes back as a
    * stale result, so `complete` says exactly whether the entry may be dropped. The result is recorded in `deferral`
    * under `entryKey`: a later pass reconciles only the keys deferred before, and once parts have been deferred by a
    * lock for `policy.proofAfterDeferrals` passes they are proven (RepairProofs) and, where proven, reconciled from
    * the same read. Without `allowProof` (the bootstrap sweep) nothing is proven and every pass
    * reconciles the whole object.
    */
  private def repairUpdate(storeId: StoreId, ptr: ObjectPointer, minTimestamp: HLCTimestamp, entryKey: Key,
                           policy: RepairPolicy, deferral: DeferralState,
                           allowProof: Boolean): Future[ReconcileResult] =
    val snapshot = Promise[RepairSnapshot]()
    target.snapshot(storeId, ptr, snapshot)

    def reconcile(snap: RepairSnapshot, os: ClientObjectState): Future[ReconcileResult] =
      val reconciled = Promise[ReconcileResult]()
      target.reconcile(storeId, snap, os, reconciled)
      reconciled.future
        .map: installed =>
          if os.timestamp >= minTimestamp then installed else installed.copy(stale = true)
        .recoverWith:
          case e: RepairSlotOccupied =>
            dropOccupant(storeId, ptr, e.occupant).map(_ => ReconcileResult(Set(), false, stale = true))

    for
      taken <- snapshot.future
      // Narrowing to the deferred keys serves proofs. Without them (the sweep) every pass reconciles the whole object:
      // a later untracked write the store missed records nothing, so `complete` must mean the whole copy matches
      snap = taken.copy(only = if allowProof then deferral.deferredKeys(entryKey) else None)
      // A full read: only full reads report the stores' locks
      os <- readObject(ptr)
      _ <- testingOnlyBeforeReconcile(storeId, ptr)
      first <- reconcile(snap, os)
      passes = deferral.record(entryKey, first)
      result <-
        if !allowProof || passes < policy.proofAfterDeferrals then
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
        // A sweep's node that was already converged is the common case
        if allowProof || result.changed then
          logger.info(s"Repair complete for object ${ptr.id} on store $storeId")
        else
          logger.debug(s"Sweep of node ${ptr.id} on store $storeId: unchanged")
      else if allowProof then
        logger.info(s"Repair of object ${ptr.id} on store $storeId left $result. Leaving the entry for the next pass")
      else
        logger.info(s"Sweep of node ${ptr.id} on store $storeId left $result. The next sweep retries it")
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

  /** Drops the object from the store. False when the store refused because the object is in use there
    * (Frontend.RepairDeleteDeferred): the entry stays for a later pass
    */
  private def repairDeletion(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte]): Future[Boolean] =
    val completion = Promise[Unit]()
    target.repairDelete(storeId, objectId, storePointer, completion)
    completion.future
      .map: _ =>
        logger.info(s"Repair complete (deletion) for object $objectId on store $storeId")
        true
      .recover:
        case e: Frontend.RepairDeleteDeferred =>
          logger.debug(e.getMessage)
          false

  /** Deletes `key` from `tree` if its value is still at `entryRevision`. Through the tree rather than the node the walk
    * handed over: a delete that empties its node joins it with its right neighbor or leaves it empty, and the tree's
    * delete registers the finalization action that removes the deleted node's upper-tier entry (and so lets the tree
    * shrink). Fails if the entry is gone, as the requirement on it did before.
    *
    * An ordinary (tracked) transaction: a store that misses the deletion is recorded like any other miss. The entry
    * is about `entryPool`, and its tree lives one level closer to the bootstrap pool, so the chain of entries ends
    * there. `limitTracking` turns tracking off only when the deletion touches `entryPool` itself, which is the
    * bootstrap pool's own trees (a miss recorded there would be an entry in the very tree being deleted from).
    *
    * Best effort: a transient error or a collision only costs another pass. The revision guard is the point: the
    * caller compared the repaired state against the missed timestamp it scanned, and a store that misses another
    * update while the repair is underway rewrites the entry with that newer timestamp. Requiring the scanned revision
    * makes the delete fail in that case, which is how the next pass learns to try again. The KeyRevision of the
    * deleted key is also the transaction's pin, so a replay deletes no newer entry.
    */
  private[repair] def deleteEntryIfUnchanged(entryPool: PoolId, tree: TieredKeyValueList,
                                             entryRevision: ObjectRevision, key: Key): Future[Unit] =
    val tx = client.newTransaction()
    tree.deleteIfPresent(key, None, List(KeyValueUpdate.KeyRevision(key, entryRevision)))(using tx).flatMap: deleted =>
      if deleted then
        MissedUpdateFinalizationAction.limitTracking(entryPool, tx)
        testingOnlyBeforeEntryDelete(entryPool, tx)
        tx.commit().map(_ => ())
      else
        Future.failed(new IllegalStateException(s"Repair entry $key is gone"))

object StoreRepairer:

  /** The DeferralState key of an errorTree entry: the entry's key and value revision. A store that misses another
    * update rewrites the entry, and the rewritten entry starts over with its whole object
    */
  private[repair] def errorDeferralKey(entryKey: Key, entryRevision: ObjectRevision): Key =
    Key(Array[Byte](0) ++ entryKey.bytes ++ entryRevision.toArray)

  /** The DeferralState key of one object of a UnackedCommits entry */
  private[repair] def unackedDeferralKey(entryKey: Key, objectId: ObjectId): Key =
    Key(Array[Byte](1) ++ entryKey.bytes ++ objectId.toBytes)

  /** The DeferralState key of a node of the bootstrap pool's own trees, swept by sweepBootstrapTrees */
  private[repair] def sweepDeferralKey(nodeId: ObjectId): Key =
    Key(Array[Byte](2) ++ nodeId.toBytes)

  private[repair] def isSweepDeferralKey(key: Key): Boolean = key.bytes.nonEmpty && key.bytes(0) == 2

  /** Decodes the ObjectId from an errorTree key: one poolIndex byte then the UUID, big-endian. */
  private[repair] def decodeObjectId(key: Key): ObjectId =
    val bb = ByteBuffer.wrap(key.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.get() // poolIndex
    val msb = bb.getLong()
    val lsb = bb.getLong()
    ObjectId(new UUID(msb, lsb))

  /** Whether an errorTree or UnackedCommits entry has sat long enough that its object's absence
    * from the allocation tree is worth a read to tell whether it was deleted (StoreRepairer.repairAbsent).
    *
    * An absent allocationTree entry has two possible meanings: the object was deleted, or the
    * allocating transaction's AllocationFinalizationAction has not landed yet -- it runs
    * concurrently with the MissedUpdateFinalizationAction that wrote the errorTree entry, and
    * both may re-run after a crash. Acting on the second case would drop the errorTree entry for
    * a live object, leaving the store silently short a slice with no record of it.
    *
    * The read decides, but just after an allocation commits it can report the object missing
    * while its stores have yet to apply it, so the read waits for the entry to age. Repair is
    * not latency sensitive: waiting costs one more pass.
    *
    * A future dated entry yields a negative age and is therefore ineligible, which is what we
    * want -- clock skew is not evidence that anything was deleted.
    */
  def errorEntryMayBeDeleted(entryTimestamp: HLCTimestamp,
                             now: HLCTimestamp,
                             minAge: Duration): Boolean =
    (now - entryTimestamp) >= minAge
