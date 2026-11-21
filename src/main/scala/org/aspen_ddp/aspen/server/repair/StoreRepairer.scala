package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.common.metadata.HostId

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, KeyValueObjectState, ReadLocks, ReadTimeout,
                                   StoragePool, Transaction, ObjectState as ClientObjectState}
import org.aspen_ddp.aspen.client.internal.transaction.{MissedUpdateFinalizationAction, UnackedCommitEntry}
import org.aspen_ddp.aspen.client.tkvl.{KeyValueListNode, TieredKeyValueList}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer,
                                           ObjectId, ObjectPointer, ObjectRevision, ObjectType}
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
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
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

/** One scan's (or sweep's) cancellation and progress, shared with whoever runs it (RepairService). A scan cut off by
  * its deadline keeps running in the background, since a hung read cannot be interrupted: `cancel` makes it stop at
  * the next entry or node, and skip the work that only a scan that ran to its end may do (pruning DeferralState).
  * So the store's DeferralState can be kept for its next scan (repair-process 8.1). `sawEntries` is
  * whether the scan had reached any entry by then: a scan cut off while working through a backlog is not quiet.
  * Safe to use from any thread.
  */
final class ScanControl:
  @volatile private var isCancelled = false
  private val entries = AtomicInteger(0)

  def cancel(): Unit = isCancelled = true
  def cancelled: Boolean = isCancelled

  /** Whether the scan has seen an entry of either tree */
  def sawEntries: Boolean = entries.get > 0
  private[repair] def sawEntry(): Unit = entries.incrementAndGet()

/** Repairs one store, once. Walks the store's slice of its pool's errorTree, then of its UnackedCommits tree,
  * brings each recorded object back into agreement with the rest of the system, and removes the entry.
  *
  * Parts of an object deferred by a lock are retried on later passes, only those keys when nothing else was left
  * undone. Once `RepairPolicy.proofAfterDeferrals` passes since the entry's last complete pass have deferred them
  * (a stale pass neither counts nor resets the count), the scan proves them with a transaction (RepairProofs) and,
  * if it commits, reconciles them from the read the proof used.
  */
class StoreRepairer(client: AspenClient, target: RepairTarget,
                    /** The host the scans run on. When given, a store is scanned only while its pool's entry names
                     *  this host and the device the store is loaded from here (`RepairTarget.localDevice`), and each
                     *  entry delete re-checks that: a copy left on a device declared failed, or otherwise superseded,
                     *  must not consume the entries the store's current copy needs. None (tests driving one network)
                     *  skips the check */
                    hostId: Option[HostId] = None,
                    /** How old a scan's ownership check may be when an entry delete relies on it */
                    ownershipRecheck: FiniteDuration = StoreRepairer.OwnershipRecheck,
                    /** Test seam: runs between a repair's read and its install */
                    testingOnlyBeforeReconcile: (StoreId, ObjectPointer) => Future[Unit] = (_, _) => Future.unit,
                    /** Test seam: told the parts each proof run proved */
                    testingOnlyOnProof: (StoreId, ObjectPointer, ReadLocks) => Unit = (_, _, _) => (),
                    /** Test seam: sees an entry deletion's transaction just before it commits */
                    testingOnlyBeforeEntryDelete: (PoolId, Transaction) => Unit = (_, _) => (),
                    /** How long one object read may take before its entry is deferred (ObjectReadTimeout) */
                    objectReadTimeout: FiniteDuration = StoreRepairer.ObjectReadTimeout)
                   (using ec: ExecutionContext) extends Logging:

  import StoreRepairer.*

  /** Every object read of a repair: a read that never completes would hold the scan at its entry (ObjectReadTimeout) */
  private val reader = client.withReadTimeout(objectReadTimeout)

  /** One scan's fence: whether the pool's entry for the store still names this host and device. An entry delete
    * awaits it first (`deleteIfStillOwned`). The read is shared by every delete within `ownershipRecheck` of it, and
    * redone after that. Once a check finds the store owned elsewhere the scan is abandoned: `lost` stays true for the
    * rest of it. Safe to call from any thread: the scan's entry callbacks may run concurrently
    */
  private final class OwnershipFence(storeId: StoreId):
    private var checked: Option[(Long, Future[Boolean])] = None
    @volatile private var isLost = false
    @volatile private var wasOwned = false

    def lost: Boolean = isLost

    def stillOwned(): Future[Boolean] =
      val check = synchronized:
        val now = System.nanoTime()
        checked match
          case Some((at, f)) if now - at < ownershipRecheck.toNanos => f
          case _ =>
            val f = ownedHere(storeId)
            checked = Some((now, f))
            f
      check.map: owned =>
        if owned then
          wasOwned = true
        else if !isLost then
          isLost = true
          // The scan's first check warns on its own
          if wasOwned then
            logger.warn(s"Abandoning repair scan of $storeId: its pool no longer records it on this host and device")
        owned

  private final class Counts(val fence: OwnershipFence, val control: ScanControl):
    val seen = AtomicInteger(0)
    val repaired = AtomicInteger(0)
    val deferred = AtomicInteger(0)
    val unackedSeen = AtomicInteger(0)
    private var touched = Set[Key]()
    private var completed = Map[ObjectId, HLCTimestamp]()

    def touch(keys: Iterable[Key]): Unit = synchronized { touched ++= keys }
    def touchedKeys: Set[Key] = synchronized(touched)

    /** The errorTree pass completed the store's work on `id` up to `missedTimestamp`: its copy now matches a read at
      * least that new, or the object was proven deleted and dropped, and the entry's delete committed. No mark
      * (including one for an earlier-started transaction) was written after the read, as it would have failed the
      * delete
      */
    def recordCompleted(id: ObjectId, missedTimestamp: HLCTimestamp): Unit = synchronized:
      if completed.get(id).forall(_ < missedTimestamp) then completed = completed.updated(id, missedTimestamp)

    /** The newest missedTimestamp up to which this scan's errorTree pass completed `id`, if it did */
    def completedAt(id: ObjectId): Option[HLCTimestamp] = synchronized(completed.get(id))
    def result: ScanResult = ScanResult(seen.get, repaired.get, deferred.get, unackedSeen.get)

  /** Scans the store once. `control` cancels it (RepairService, on its deadline): a cancelled scan starts no further
    * entry and leaves `deferral` unpruned, so the next scan may share it. A reconcile in flight when it is cancelled
    * still records its result, which can add one pass to an entry's lock-deferral count: a proof may come a pass
    * early, which is safe
    */
  def scan(storeId: StoreId, policy: RepairPolicy, deferral: DeferralState = DeferralState(),
           control: ScanControl = ScanControl()): Future[ScanResult] =
    val fence = OwnershipFence(storeId)
    val counts = Counts(fence, control)

    // Both trees' keys start with the poolIndex, so one store's entries are the half-open range [poolIndex,
    // poolIndex+1).
    val min = Key(Array[Byte](storeId.poolIndex))
    val max = Key(Array[Byte]((storeId.poolIndex + 1).toByte))

    fence.stillOwned().flatMap: owned =>
      if !owned then
        logger.warn(s"Not repairing store $storeId: its pool records it on another host or device")
        Future.successful(counts.result)
      else
        for
          pool <- client.getStoragePool(storeId.poolId)
          _ <- pool.errorTree.foreachInRange(min, max, repairErrorEntry(pool, storeId, policy, deferral, counts))
          _ <- pool.unackedCommitsTree.foreachInRange(min, max,
                 repairUnackedEntry(pool, storeId, policy, deferral, counts))
        yield
          // A cancelled scan did not see every entry it would have: pruning by what it touched would drop live ones
          if !control.cancelled then deferral.retain(counts.touchedKeys)
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
  def sweepBootstrapTrees(storeId: StoreId, policy: RepairPolicy, deferral: DeferralState,
                          control: ScanControl = ScanControl()): Future[SweepResult] =
    if storeId.poolId != PoolId.BootstrapPoolId then
      Future.failed(new IllegalArgumentException(s"Only stores of the bootstrap pool are swept, not $storeId"))
    else
      val nodes = AtomicInteger(0)
      val reconciled = AtomicInteger(0)
      val incomplete = AtomicInteger(0)
      var sweepKeys = Set[Key]()

      def sweepNode(tier: Int, pointer: KeyValueObjectPointer): Future[Unit] =
        if control.cancelled || abandoned(storeId) then
          // Cut off by its deadline, or not repairable here, which may be only until the next tick (offline,
          // rebuilding): the node was not swept
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
          logger.debug(s"Not sweeping store $storeId: its pool records it on another host or device")
          Future.successful(SweepResult.Empty)
        else
          for
            pool <- client.getStoragePool(storeId.poolId)
            errorWalk <- pool.errorTree.foreachNode(sweepNode)
            unackedWalk <- pool.unackedCommitsTree.foreachNode(sweepNode)
          yield
            val walkComplete = errorWalk.complete && unackedWalk.complete
            if walkComplete && !control.cancelled && !abandoned(storeId) then
              deferral.retainSweep(synchronized(sweepKeys))
            val result = SweepResult(nodes.get, reconciled.get, incomplete.get, walkComplete)
            if result.needsAnotherPass then
              logger.info(s"Swept the bootstrap trees' nodes on $storeId: $result")
            else
              logger.debug(s"Swept the bootstrap trees' nodes on $storeId: $result")
            result

  /** Whether the pool's entry for the store names this host and the device the store is loaded from here, read
    * fresh. A store with no device here is not owned
    */
  private def ownedHere(storeId: StoreId): Future[Boolean] = hostId match
    case None => Future.successful(true)
    case Some(h) =>
      client.getStoragePoolState(storeId.poolId).map: pool =>
        if storeId.poolIndex >= pool.stores.length || pool.stores(storeId.poolIndex).hostId != h then
          false
        else
          val entry = pool.stores(storeId.poolIndex)
          target.localDevice(storeId) match
            case None =>
              logger.warn(s"Store $storeId has no device on this host: its pool records it on device " +
                          s"${entry.storageDeviceId}")
              false
            case Some(device) => device == entry.storageDeviceId

  /** Deletes a repaired entry, after the scan's fence confirms the store is still owned here. False, leaving the
    * entry for the store's current copy, when it is not
    */
  private def deleteIfStillOwned(storeId: StoreId, counts: Counts, tree: TieredKeyValueList,
                                 revision: ObjectRevision, key: Key): Future[Boolean] =
    counts.fence.stillOwned().flatMap: owned =>
      if owned then
        deleteEntryIfUnchanged(storeId.poolId, tree, revision, key).map(_ => true)
      else
        Future.successful(false)

  /** As `abandoned`, or the scan's fence found the store owned elsewhere, or the scan was cancelled */
  private def abandoned(storeId: StoreId, counts: Counts): Boolean =
    counts.control.cancelled || counts.fence.lost || abandoned(storeId)

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
    if abandoned(storeId, counts) then
      Future.unit
    else
      // Wrap synchronous portion to convert throws into failed Futures. decodeObjectId,
      // decodeErrorEntry and ObjectPointer construction all decode and can throw on malformed
      // input; a throw that
      // escapes before a Future is produced would hang the scan instead of letting foreachInRange
      // log the failure and continue.
      Future.unit.flatMap: _ =>
        counts.seen.incrementAndGet()
        counts.control.sawEntry()
        val objectId = decodeObjectId(key)
        val errorEntry = MissedUpdateFinalizationAction.decodeErrorEntry(value.value)
        val entryKey = errorDeferralKey(key)
        counts.touch(List(entryKey))
        deferral.observeEntry(entryKey, value.revision)

        def completed(complete: Boolean): Future[Unit] =
          if complete then
            deferral.clear(entryKey)
            // Only a committed delete lets the UnackedCommits pass skip the object. A delete that fails its revision
            // found the entry rewritten, possibly by a mark for a transaction that committed after this pass's read
            deleteIfStillOwned(storeId, counts, pool.errorTree, value.revision, key).map: deleted =>
              if deleted then
                counts.recordCompleted(objectId, errorEntry.missedTimestamp)
                counts.repaired.incrementAndGet()
              else
                counts.deferred.incrementAndGet()
          else
            counts.deferred.incrementAndGet()
            Future.unit

        if target.mayJoinTransaction(storeId, errorEntry.missedTimestamp) then
          // As for UnackedCommits entries: a replay may still create a Tx for the missed transaction here and apply
          // it after this entry's reconcile, and the entry would be gone (repair-process 5.4)
          logger.debug(s"Deferring error entry of $objectId on $storeId: the store may still join its transaction")
          counts.deferred.incrementAndGet()
          Future.unit
        else for
          oAllocation <- pool.allocationTree.get(Key(objectId.toBytes))
          _ <- oAllocation match
            case Some(allocation) =>
              repairListed(storeId, ObjectPointer(allocation.value.bytes), errorEntry.missedTimestamp, entryKey,
                           policy, deferral).flatMap(completed)

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
    *
    * An object this scan's errorTree pass already completed, its entry's delete committed, at a missedTimestamp at or
    * after the entry's is complete without another read (repair-process 5.3): the store's copy matched a read that
    * new (the completion gate) and no mark followed that read, or the object was proven deleted, which is all the
    * entry asks of it. The marker marks every store that sent no
    * TxCommitted, so a silent voter's entry usually costs only its delete.
    */
  private def repairUnackedEntry(pool: StoragePool, storeId: StoreId, policy: RepairPolicy,
                                   deferral: DeferralState, counts: Counts)
                                  (node: KeyValueListNode, key: Key, value: ValueState): Future[Unit] =
    if abandoned(storeId, counts) then
      Future.unit
    else
      Future.unit.flatMap: _ =>
        counts.unackedSeen.incrementAndGet()
        counts.control.sawEntry()
        val txid = UnackedCommitEntry.decodeTransactionId(key)
        if target.holdsTransaction(storeId, txid) then
          logger.debug(s"Skipping UnackedCommits entry of $txid on $storeId: the store holds it live")
          counts.deferred.incrementAndGet()
          Future.unit
        else
          val entry = UnackedCommitEntry.decode(value.value.bytes)
          if target.mayJoinTransaction(storeId, entry.timestamp) then
            // A replay may still create a Tx for T here, after the holdsTransaction check above, and apply T late
            // through this entry once its reconcile is done: deleting the entry then would leave that apply
            // unrepaired (repair-process 5.4). Below the store's forget floor no new Tx for T can appear
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
          if counts.completedAt(ptr.id).exists(_ >= entry.timestamp) then
            logger.debug(s"UnackedCommits entry of $txid on $storeId: ${ptr.id} was repaired by its errorTree entry")
            Future.successful(allComplete)
          else
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
                repairListed(storeId, ptr, entry.timestamp, entryKey, policy, deferral)
                  .map(complete => allComplete && complete)

      completed.flatMap: allComplete =>
        if allComplete then
          entryKeys.foreach((_, entryKey) => deferral.clear(entryKey))
          deleteIfStillOwned(storeId, counts, pool.unackedCommitsTree, value.revision, key).map: deleted =>
            if deleted then
              logger.info(s"Repair complete for UnackedCommits entry of $txid on store $storeId")
              counts.repaired.incrementAndGet()
            else
              counts.deferred.incrementAndGet()
        else
          counts.deferred.incrementAndGet()
          Future.unit

  /** Repairs an object of an entry old enough (errorEntryMayBeDeleted) that the allocation tree does not list.
    * Absence alone does not prove a deletion: the allocating transaction's AllocationFinalizationAction may simply be
    * late (repair-process 9.2), and deleting the slice of a live object would leave the store short with no record of
    * it. So the object is read through its pool:
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

  /** Repairs an object of an entry that the allocation tree lists. The listing outlives a deletion until its
    * DeletionFinalizationAction runs, so the repair's read may find the object deleted (repair-process 9.2). A read
    * that proves it (InvalidObject.provesDeletion) is evidence enough whatever the tree says, as in repairAbsent: the
    * object is dropped from the store. A read that fails with InvalidObject short of a proof, or times out
    * (ObjectReadTimeout), leaves the entry for a later pass. True when the entry's work on the object is done.
    */
  private def repairListed(storeId: StoreId, ptr: ObjectPointer, minTimestamp: HLCTimestamp, entryKey: Key,
                           policy: RepairPolicy, deferral: DeferralState): Future[Boolean] =
    repairUpdate(storeId, ptr, minTimestamp, entryKey, policy, deferral, allowProof = true)
      .map(_.complete)
      .recoverWith:
        case e: InvalidObject if e.provesDeletion =>
          logger.info(s"Object ${ptr.id} on $storeId is deleted but still in the allocation tree: dropping it")
          repairDeletion(storeId, ptr.id, ptr.storePointer)
        case e: InvalidObject =>
          logger.info(s"Deferring repair of ${ptr.id} on $storeId: a read of it neither restores it nor proves it " +
                      s"deleted: $e")
          Future.successful(false)
        case _: ReadTimeout =>
          logger.info(s"Deferring repair of ${ptr.id} on $storeId: its read did not complete within $objectReadTimeout")
          Future.successful(false)

  private def readObject(ptr: ObjectPointer): Future[ClientObjectState] = ptr match
    case kp: KeyValueObjectPointer => reader.read(kp)
    case dp: DataObjectPointer => reader.read(dp)

  /** The store's slot for `ptr`, a live object (just read), holds `occupant` (Backend.RepairSlotOccupied): FSIF slot
    * reuse over a missed delete of the occupant (repair-process 6.5, rebuild-process 12 FS6). Two live objects never
    * share a slot, so the occupant is dead; it is dropped once the allocation tree does not list it either, by an
    * id-checked repairDelete. The repair of `ptr` is left stale, so its entry stays and the next pass installs it. A
    * listed occupant is an inconsistency nothing here can settle: it is logged and left.
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
      // An object stale for several passes since its last complete one has a busy copy here: the store waits for it
      // to settle
      snap = taken.copy(only = if allowProof then deferral.deferredKeys(entryKey) else None,
                        minTimestamp = minTimestamp,
                        awaitSettle = allowProof && deferral.staleCount(entryKey) >= ParkAfterStalePasses)
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
      if allowProof then
        val stale = deferral.recordStale(entryKey, result)
        if stale >= policy.proofAfterDeferrals && stale % policy.proofAfterDeferrals == 0 then
          logger.warn(s"Repair of object ${ptr.id} on store $storeId has been stale for $stale passes: its " +
                      s"copy there stays busy, or changes under every install")
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

    for
      keys <- keyProofs.foldLeft(Future.successful(Set[Key]())): (f, proof) =>
                f.flatMap(proven => proof().map(proven ++ _))
      // The object level of either type: its refcount and its revision, in one proof
      objectLevel <- if result.objectLevelDeferred then RepairProofs.proveObjectLevel(client, read)
                     else Future.successful(false)
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

  /** How long a scan may rely on one read of the pool's entry for its store before an entry delete reads it again */
  val OwnershipRecheck: FiniteDuration = 1.second

  /** How long a scan waits on one object read before deferring its entry. A read that has not completed by then fails
    * with ReadTimeout and sends nothing more, and the entry is left for a later pass, as for a read that neither
    * restores the object nor proves it deleted. Long enough for any healthy read, so only an object that cannot be
    * restored now (below its read threshold, an allocation parked past expiry, a key that stays NotRestorable) times
    * out, and short enough that such an object costs a fraction of a scan (RepairService.ScanDeadline)
    */
  val ObjectReadTimeout: FiniteDuration = 15.seconds

  /** Consecutive stale passes after which a repair asks the store to wait for the object to settle before
    * reconciling it (RepairSnapshot.awaitSettle, repair-process 8.2). A stale pass is usually a commit that happened to
    * be in flight, which the next pass misses, so waiting is kept for an object whose copy stays busy
    */
  val ParkAfterStalePasses: Int = 3

  /** A rebuilt store's inherited-growth scan (Frontend, Inherited growth; cross-document-review.md 2.3): first marks
    * each KeyValue object of `mark` the pool's allocation tree lists in the ErrorTree against `storeId` (missed at
    * HLCTimestamp.Zero, so it never displaces a newer mark), then lists every KeyValue object with an ErrorTree or
    * UnackedCommits entry against the store, those marks included. Only KeyValue objects can overfill a slice this way
    * (a DataObject's updates form a revision chain each voter checked in full, R16). An entry that cannot be decoded
    * fails the scan, as does any failed read or mark: the caller retries rather than miss an object. An object `mark`
    * names that the allocation tree does not list is not marked (deleted, or an allocation the rebuild's pending sweep
    * covers). An ErrorTree entry recording no type is left out: repair acts on one only for a proven deletion, so a
    * guard on its object would never end
    */
  def inheritedGrowthScan(client: AspenClient, storeId: StoreId, mark: Set[ObjectId]): Future[Set[ObjectId]] =
    given ExecutionContext = client.clientContext
    import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker.MissedUpdate
    val min = Key(Array[Byte](storeId.poolIndex))
    val max = Key(Array[Byte]((storeId.poolIndex + 1).toByte))
    for
      pool <- client.getStoragePool(storeId.poolId)
      listed <- Future.sequence(mark.toList.map: oid =>
                  pool.allocationTree.get(Key(oid.toBytes)).map(_.map(vs => ObjectPointer(vs.value.bytes))))
      marks = listed.flatten.filter(p => p.poolId == storeId.poolId && p.objectType == ObjectType.KeyValue).map: ptr =>
                MissedUpdate(storeId, ptr.id, Some(ptr.objectType), ptr.storePointer, HLCTimestamp.Zero)
      _ <- if marks.isEmpty then Future.unit else client.missedUpdateMarker.mark(marks)
      // foreachInRange logs and steps past a failing fn, so a decode failure is carried out of band
      found = new java.util.concurrent.ConcurrentHashMap[ObjectId, Unit]()
      undecodable = new AtomicInteger(0)
      _ <- pool.errorTree.foreachInRange(min, max, (_, key, value) =>
             try
               val oid = decodeObjectId(key)
               if MissedUpdateFinalizationAction.decodeErrorEntry(value.value).objectType
                    .contains(ObjectType.KeyValue) then found.put(oid, ())
             catch case scala.util.control.NonFatal(_) => undecodable.incrementAndGet()
             Future.unit)
      _ <- pool.unackedCommitsTree.foreachInRange(min, max, (_, _, value) =>
             try UnackedCommitEntry.decode(value.value.bytes).objects.foreach: ptr =>
                   if ptr.poolId == storeId.poolId && ptr.objectType == ObjectType.KeyValue then found.put(ptr.id, ())
             catch case scala.util.control.NonFatal(_) => undecodable.incrementAndGet()
             Future.unit)
      _ <- if undecodable.get == 0 then Future.unit
           else Future.failed(new Exception(s"Inherited-growth scan of $storeId: ${undecodable.get} entries against " +
                                            "it could not be decoded"))
    yield
      import scala.jdk.CollectionConverters.*
      found.keySet.asScala.toSet

  /** The DeferralState key of an errorTree entry: the entry's key alone. A store that misses another update rewrites
    * the entry; its counts carry over, and only the next pass's narrowing is dropped (DeferralState.observeEntry)
    */
  private[repair] def errorDeferralKey(entryKey: Key): Key =
    Key(Array[Byte](0) ++ entryKey.bytes)

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
    * The read decides, and the gate only paces it: a read just after an allocation commits cannot prove the object
    * deleted, since a commit voter that has not applied it answers AllocationPending (transaction-requirements 9.4).
    * The gate compares this host's HLC with the entry's (the marker transaction's start, another host's), so clock skew
    * moves it by at most the skew plus MaxStartLead either way, and safety does not rest on it (repair-process 9.2).
    * Repair is not latency sensitive: waiting costs one more pass.
    *
    * A future dated entry yields a negative age and is therefore ineligible, which is what we
    * want -- clock skew is not evidence that anything was deleted.
    */
  def errorEntryMayBeDeleted(entryTimestamp: HLCTimestamp,
                             now: HLCTimestamp,
                             minAge: Duration): Boolean =
    (now - entryTimestamp) >= minAge
