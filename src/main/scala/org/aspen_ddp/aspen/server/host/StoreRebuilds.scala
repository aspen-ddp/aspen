package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.server.rebuild.RebuildDisk
import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StoreOffline
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.host.DeviceMetadata.RebuildOutcome
import org.aspen_ddp.aspen.server.{Host, StoreConfig}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild}
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Creates the stores of new pool slots and rebuilds the ones that must be reconstructed from the rest of their
 *  pool, and retires stores that must never serve again (a lost CRL save file, a failed backend, a failed close).
 *
 *  Rebuild and create work lives in the copy registry (Work.Rebuild, Work.Create, by device and store); what is kept
 *  here is what Work cannot hold: each running rebuild's handle (to cancel it) and the promise a shutdown waits on,
 *  a pending flip, request and retry backoff. The rebuild slots are per destination device (slotsTaken), so a hung
 *  device holds only its own and never blocks a rebuild onto another. */
class StoreRebuilds(ctx: DeviceControl) extends Logging:
  import ctx.given

  // Created stores whose flip out of Initializing has not completed. The create's work (Work.Create) ends when its
  // copy is served, its open fails or the create is skipped; the flip that follows touches no copy, so Work cannot
  // hold it, and it outlives that work on purpose (I3: once served the copy may be rebuilt or transferred). A store
  // with either is not created again
  private var pendingCreateFlips: Set[StoreId] = Set()

  // The handle of each running rebuild (Work.Rebuild Running or Abandoned on its device), to cancel it, and the promise
  // completed once it has released its slot, its outcome acted on: what a shutdown waits on for the rebuilds it
  // cancels. Its device, episode and stage are its Work's
  private var runningRebuilds: Map[StoreId, StoreRebuilds.RunningRebuild] = Map()

  /** Stores whose requestRebuild transaction is in flight, so a reconcile meanwhile does
   *  not start a second one. */
  private var pendingRebuildRequests: Set[StoreId] = Set()
  // rebuild-process 5.7, retry backoff: by store, how many passes or flips have failed in a row, and the stores
  // with a retry scheduled, each with what the failure that scheduled it (the latest) ran for
  private var rebuildFailures: Map[StoreId, Int] = Map()
  private var scheduledRebuildRetries: Map[StoreId, StoreRebuilds.HeldRebuild] = Map()

  // rebuild-process 6.2: a store's Rebuild work (Work.Rebuild on its device) lasts from the first reconcile that sees
  // the Rebuilding entry (Queued, waiting for a slot) through the discard of its lost copy (DiscardingLost) and its run
  // (Running, or Abandoned once cancelled for an entry that left) until the rebuild's outcome or the entry's leaving
  // ends it (endRebuildWork). The work keeps the store silent (HostCopies.silence): a store rebuilt on a device that
  // never held it is not loaded, so this host would otherwise answer UnknownStore/StoreNotFound for it, and readers
  // would re-send at once, for the whole rebuild. A discard and a run each hold a rebuild slot: the discard is the
  // first destructive step, so maxConcurrentRebuilds bounds discards too (systemic-loss-protection.md)

  private def registry: HostCopies = ctx.copyRegistry

  /** Sets the store's Rebuild work on `deviceId` to exactly `w` (None ends it). A claim: false, recorded, where other
   *  work holds the (device, store) */
  private def setWork(storeId: StoreId, deviceId: StorageDeviceId, w: Option[Work.Rebuild]): Boolean =
    registry.work("rebuild", deviceId, storeId, _.isInstanceOf[Work.Rebuild], w, exact = true)

  /** The store's Rebuild work on `deviceId` back at Queued, if it is there: a discard or a run that ended without the
   *  work's end (a failure retried after its backoff, a superseded flip, a cancel for a shutdown) */
  private def requeue(storeId: StoreId, deviceId: StorageDeviceId): Unit =
    registry.rebuildWorkOn(deviceId, storeId).foreach: w =>
      if w.stage != RebuildStage.Queued then
        setWork(storeId, deviceId, Some(Work.Rebuild(w.episode, RebuildStage.Queued)))

  /** Whether rebuild work for `storeId` is registered on `deviceId`, any stage: queued for a slot, discarding its lost
   *  copy, or running, one cancelled (cancelRebuild) included until its slot is released, since a rebuild past
   *  finish's latch re-check ignores the cancel and can still move a copy into rebuilt/. A transfer in of the
   *  store onto the device waits while it holds (StoreTransfers.startIn): one piece of work per (device, store) */
  private[host] def hasRebuildWork(storeId: StoreId, deviceId: StorageDeviceId): Boolean =
    registry.rebuildWorkOn(deviceId, storeId).isDefined

  /** Whether create work for the store runs here: its copy not yet served, nor its open failed (createNewStore) */
  private[server] def isCreating(storeId: StoreId): Boolean = registry.isCreating(storeId)

  /** The created stores whose flip out of Initializing has not completed yet */
  private[server] def createFlipsPending: Set[StoreId] = pendingCreateFlips

  /** The stores with a backoff re-check of their device scheduled */
  private[server] def rebuildRetriesScheduled: Set[StoreId] = scheduledRebuildRetries.keySet

  /** The rebuild slots taken on destination device `deviceId`: each rebuild running onto it (by its handle, so one
   *  whose device was unloaded meanwhile still counts until its slot is released), and each discard on it but
   *  `except`'s. Slots are per device (maxConcurrentRebuilds each): every slot holder waits only on its own device's
   *  lane (its staging jobs, its finish's listing, its flip's move, its discard's retire), so a hung device holds only
   *  its own slots and never blocks a rebuild onto a healthy one (spec 7a) */
  private def slotsTaken(deviceId: StorageDeviceId, except: Option[(StorageDeviceId, StoreId)]): Int =
    runningRebuilds.valuesIterator.count(_.deviceId == deviceId) + registry.discardsOn(deviceId, except)

  /** Test-only (HostCopies.verifyAgainstDisk): the phase a copy placed by the running rebuild of `storeId` on device
   *  `deviceId` reads (Rebuilt, with its episode in phase-param.rebuilt), while its completion's handler has not yet
   *  recorded it. RebuildingStore.finish places the copy in rebuilt/ off the control thread, before the handler runs,
   *  so rebuilt/ may meanwhile hold a copy in that phase that the registry does not know yet. None outside that
   *  rebuild */
  private[server] def finishPlacing(deviceId: StorageDeviceId, storeId: StoreId): Option[Phase] =
    runningRebuilds.get(storeId).filter(r => r.deviceId == deviceId && !r.finishRecorded)
      .map(r => Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(r.episode)))

  /** Test-only: finishPlacing of every store of device `deviceId` it is defined for */
  private[server] def finishPlacingOn(deviceId: StorageDeviceId): Map[StoreId, Phase] =
    runningRebuilds.keys.flatMap(s => finishPlacing(deviceId, s).map(s -> _)).toMap

  /** The rebuild slots taken now on every device together (tests) */
  private[server] def rebuildSlotsTaken: Int = runningRebuilds.size + registry.discards()

  /** The rebuild slots taken now on device `deviceId` (tests) */
  private[server] def rebuildSlotsTakenOn(deviceId: StorageDeviceId): Int = slotsTaken(deviceId, None)

  /** Cancels the rebuilds of the given stores of `deviceId` for its shutdown, returning, by store, the future of
   *  each one's slot release. A cancel during the walk, sweep, retries or finish's close fails the pass; a rebuild
   *  already past finish's abort-latch re-check (or found finished by start, awaiting its flip) completes instead
   *  and writes its entry Offline. */
  def cancelFor(deviceId: StorageDeviceId, rebuilding: Set[StoreId]): Map[StoreId, Future[Unit]] =
    rebuilding.toList.flatMap: storeId =>
      runningRebuilds.get(storeId).map: r =>
        logger.info(s"Cancelling the rebuild of store $storeId for the shutdown of device $deviceId")
        r.sr.cancel()
        storeId -> r.settled.future
    .toMap

  def handleBackendFailed(e: Host.BackendFailed): Unit =
    backendFailed(e.storeId, Option(e.backend.get), e.err)

  /** A served store can no longer be trusted: it failed to recover its CRL transactions (it may be missing some of
   *  their votes and locks), or it fail-stopped (Frontend.failStop: a commit voter that cannot hold the update it
   *  voted for). It is fail-stopped and rebuilt as a failed backend is, but only while it is still the store installed
   *  under its id: one already closed (transferred out, unloaded, replaced by a reload) is left alone, since
   *  backendFailed would mark the directory of a store it no longer serves */
  def handleFailStopped(e: Host.StoreFailStopped): Unit =
    val storeId = e.store.storeId
    if ctx.router.entry(storeId).exists(_.store eq e.store) then
      backendFailed(storeId, Some(e.store.backend), e.err, e.what)
    else
      logger.warn(s"Ignoring the fail-stop of store $storeId, which is no longer served (${e.what}): ${e.err}")

  /** Creates and loads an Initializing store, then flips its entry out of Initializing.
   *
   *  The flip writes serviceStatus: Active normally, Offline when the device has been asked to
   *  shut down since the create began. The store is loaded either way, its copy Opening then Served, so the
   *  shutdown closes it like the device's other open stores.
   *
   *  A create that has not started its local work by the time this device's shutdown begins (or
   *  its unload) does none: nothing is created or loaded, and the entry stays Initializing for a
   *  later check once the device is back in service. Writing Offline over a store that does not
   *  exist would have a restart bring an empty slot into service. The decision and the copy's
   *  Opening record (startOpen) are made in one control-thread task, so a shutdown either sees the store or the
   *  create sees the shutdown.
   *
   *  Only a collision is retried. Any other failure -- the pool cannot be read, the directory
   *  cannot be written -- is logged and releases pendingCreateFlips, so the next check tries again
   *  rather than a silent retry loop running for the life of the process.
   *
   *  The create's work on the store (Work.Create) ends when its copy is served, its open fails, or the create is
   *  skipped, not when the flip completes: once served, the copy is a store like any other, and a backend failure may
   *  have it rebuilt (or an operator transfer it) as soon as the flip commits. pendingCreateFlips, which holds until
   *  the flip completes, keeps a check meanwhile from creating it again (I3: one piece of work per (device, store)).
   */
  def createNewStore(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    // The create's target, live/<copy> under a fresh copy id, which its record (registered by its open, before the
    // create places the copy) takes
    val storePath = CopyLayout.freshLivePath(local.devicePath, storeId)
    val deviceId = local.storageDeviceId

    // The create claims its work first (I3): refused (other work holds the copy, recorded), nothing starts, and the
    // next check tries again
    if ! registry.isCreating(storeId) && ! pendingCreateFlips.contains(storeId) &&
       ! ctx.shutdown.isShuttingDown(deviceId) &&
       registry.work("create", deviceId, storeId, _ == Work.Create, Some(Work.Create)) then
      pendingCreateFlips += storeId

      // True when the store exists on disk (created here, or by an attempt that crashed before its flip, whose Live
      // copy the device's load registered), false when the create was skipped for a shutdown. A Lost copy counts as
      // created too: an earlier create's copy that may have served before its flip, then fail-stopped (moved to
      // lost/ when its close ended, P5) or lost the device CRL at a restart. A fresh empty copy could drop what it
      // acknowledged, so the entry is flipped out of Initializing over it, and the Lost CRL pass then has the store
      // rebuilt from its peers. A copy in any other phase fails the flip, which the next check tries again: never a
      // create beside a copy of the store (T1).
      val existing = registry.copiesOf(local).flatMap(_.copies.get(storeId))
      val fcreate: Future[Boolean] = existing match
        case Some(r) if r.phase == Phase.Live || r.phase == Phase.Lost => Future.successful(true)
        case Some(r) =>
          Future.failed(IllegalStateException(s"Store $storeId, Initializing on device $deviceId, has a copy there in " +
                                              s"${r.phase}: not creating it"))
        case None =>
          ctx.client.getStoragePoolState(storeId.poolId).flatMap: pstate =>
            if ctx.shutdown.isShuttingDown(deviceId) || ! ctx.devices.isLoaded(local) then
              logger.info(s"Not creating store $storeId: device $deviceId is being shut down")
              Future.successful(false)
            else
              val storeConfig = StoreConfig.forPool(storeId, pstate)

              // Created wherever its backend opens, atomically (BackendOpener.create, StoreCreation): the
              // directory, config included, appears at storePath only complete, so a Live record after a restart
              // means the store exists. startOpen registers its copy Opening now, so a shutdown of this
              // device waits for the create and then closes the store. A create that fails, out of space
              // included, leaves nothing at storePath, and the next device check tries again.
              val created = Promise[Unit]()
              ctx.loader.startOpen(local, storeConfig, storePath, create = true, Some(created))
              created.future.map(_ => true)

      // The create's work ends here: served, failed, or skipped. Registered before the flip's callback, which runs
      // only once fcreate has completed
      fcreate.onComplete: _ =>
        registry.work("create", deviceId, storeId, _ == Work.Create, None)

      // The transaction's own machinery (commit, retries) runs on ctx.ec, and its result comes back through
      // the control-context onComplete; the prepare closure's combinators run on the control thread
      val fflip = ctx.client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision) { tx =>
        for
          created <- fcreate
          ptr <- ctx.client.getStorageDevicePointer(deviceId)
          kvos <- ctx.client.read(ptr)
          state = StorageDeviceState(kvos)
        yield
          state.stores.get(storeId).foreach: entry =>
            if created && entry.status == StorageDeviceState.StoreStatus.Initializing then
              val status = state.serviceStatus
              val newState = state.setStoreEntry(storeId, status, None)

              val reqs = List(KeyRevision(StorageDeviceState.StateKey,
                                          kvos.contents(StorageDeviceState.StateKey).revision))
              val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

              logger.info(s"Updating device state to mark store $storeId as $status")
              tx.update(ptr, None, None, reqs, ops)

              tx.result.foreach: _ =>
                logger.info(s"Successfully updated device state to mark store $storeId as $status")
      }(using ctx.ec)

      fflip.onComplete: result =>
        result.failed.foreach: t =>
          logger.error(s"Failed to create store $storeId on device $deviceId. The next check of the " +
                       s"device will try again. Error: $t")
        // Normally after fcreate's callback has ended the create's work; a flip failing before its prepare ran
        // leaves that work to fcreate, which still blocks a second create until it ends
        pendingCreateFlips -= storeId

  /** Begins reconstruction of one store from the rest of its pool.
   *
   *  Runs on the control thread, so it does no more than construct the rebuild and register
   *  its continuation; the walk itself runs on the client's execution context.
   *
   *  On success the walk places the rebuilt store in `<device>/rebuilt/` (T2), and its completion reports the copy id,
   *  with which the handler records it. Loading
   *  is downstream of the metadata decision: markRebuiltStoreActive decides whether the device is
   *  allowed to adopt the copy (reading the tree state inside a transaction to guard against
   *  concurrent tombstone), then the completion handler loads if adopted or discards if the device
   *  was failed. The slot is released only after the flip commits, so a concurrent check cannot
   *  restart the same store between the release and the commit.
   *
   *  Every completion re-checks the host's other loaded devices. The slots are per destination device, so a slot
   *  released by device A's rebuild unblocks only A's queued rebuilds; the re-check of the others is cheap and keeps a
   *  rebuild queued on device B for any other reason from waiting for the periodic sweep (one
   *  hour) with data under-replicated. Every flip outcome but `NotOwned` (`Adopt`, `AdoptOffline`,
   *  `Discard`, `Superseded`, `Unconfirmed`) additionally re-checks this device, to start the next
   *  store queued behind the slot just freed; after `NotOwned` the device is another host's. A
   *  failed walk and a permanently failed flip do not, since that re-check would restart the same
   *  store at once: they schedule a backoff re-check instead (scheduleRebuildRetry,
   *  rebuild-process 5.7).
   *
   *  A slot is taken from the start of a lost store's discard (its work DiscardingLost), not only once its walk runs,
   *  and passes straight from the discard to the rebuild (`handoff`: the work goes DiscardingLost -> Running, the
   *  discard's own slot not counted against it): otherwise one pass would discard every lost store on the device at
   *  once (systemic-loss-protection.md).
   *
   *  `episode` is the Rebuilding entry's episode (StoreEntry.rebuildEpisode). The rebuild resumes only a checkpoint,
   *  and takes only a finished copy, of this episode, and its flip commits only while the entry still names it: a
   *  store re-placed away and back is a new episode, and nothing of the earlier one is resumed or promoted
   *  (rebuild-process.md 5.1).
   */
  def startRebuild(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID],
                   acceptLoss: Boolean, handoff: Boolean = false,
                   disk: Option[ReconcileDisk.ReconcileDiskInputs] = None,
                   checkpointDropped: Boolean = false): Unit =
    val deviceId = local.storageDeviceId
    val held = StoreRebuilds.HeldRebuild(deviceId, episode, acceptLoss)
    // Host-wide by store id: a rebuild of the store runs (on any device), so nothing more starts for it. Its handle
    // counts too: a rebuild running for a device unloaded under it (crlFailed cancels nothing; a cancel past the abort latch completes) has lost
    // its Work with the device's records, and still holds its slot until it releases it
    val runs = registry.rebuilding(HostCopies.RebuildReads).contains(storeId) || runningRebuilds.contains(storeId)
    // Nor queued while a transfer out is still closing it (and extracting its CRL state from its directory): that
    // close is the (device, store)'s one piece of work (I3), and its end, whatever its outcome, checks the device
    // again (StoreTransfersOut.rebuildWaitsForClose). The close took the store out of service already. The reconcile
    // does not even ask then (CopyDecision: other work holds the store); a direct caller is refused here
    val closing = !runs && ctx.transfersOut.rebuildWaitsForClose(storeId)
    if closing then
      logger.debug(s"Not rebuilding store $storeId on device $deviceId yet: a transfer out is still closing it")
    // Silent from the first reconcile that sees the entry, a store queued for a slot included (rebuild-process 6.2).
    // Queuing claims the work (I3): refused (other work holds the copy, recorded), nothing is queued or started, and a
    // later reconcile asks again
    val current = registry.rebuildWorkOn(deviceId, storeId)
    val refused = !closing && current.isEmpty && !runs &&
      !setWork(storeId, deviceId, Some(Work.Rebuild(episode, RebuildStage.Queued)))
    if refused then
      logger.error(s"Not rebuilding store $storeId on device $deviceId yet: the copy registry refused its work")
    // A queued store whose entry is Rebuilding under another episode now (re-placed away and back) queues for it
    current.foreach: w =>
      if w.stage == RebuildStage.Queued && w.episode != episode then
        setWork(storeId, deviceId, Some(Work.Rebuild(episode, RebuildStage.Queued)))
    // A store inside its backoff (scheduleRebuildRetry) waits for that re-check, which clears the mark before it
    // checks the device: otherwise stores whose passes keep failing, sorting first, would retake the slots at every
    // check of the device and starve the stores queued behind them. An operator's remedy since the failure (a
    // re-placement to another device or episode, an accept-rebuild-loss) is not held back (rebuild-process 5.7)
    // A store still being shipped waits, re-checked shortly: a discard of its lost copy would move the directory the
    // send may still be reading (I3, one piece of work per (device, store)). CopyDecision's Lost and Outbound/Parked
    // cases make the same check. Not asked while a rebuild of the store is registered: nothing
    // starts then anyway, and the rebuild's end checks the device again (a send cannot begin meanwhile, since startOut
    // refuses while the rebuild is registered), so a re-check would only repeat at every reconcile
    val shipping = !closing && !refused && !runs && ctx.storeTransferService.isTransferringOut(storeId)
    if shipping then
      logger.debug(s"Not rebuilding store $storeId on device $deviceId yet: a transfer out of it is still sending")
      ctx.reconciler.scheduleDeviceRecheck(deviceId)
    // Nor while the store's staging checkpoint drop runs (retireForeignCopy): its apply continues the start
    if !closing && !refused && !shipping && !runs && !scheduledRebuildRetries.get(storeId).contains(held) &&
       !stagingDrops.contains((deviceId, storeId)) &&
       slotsTaken(deviceId, Option.when(handoff)((deviceId, storeId))) < ctx.maxConcurrentRebuilds &&
       ! ctx.shutdown.isShuttingDown(local.storageDeviceId) &&
       retireForeignCopy(local, storeId, episode, acceptLoss, checkpointDropped, disk) &&
       discardLostStore(local, storeId, episode, held) then
      // The copy already there, as its record has it: the rebuild's start (on this thread, in its constructor) takes
      // this episode's Rebuilt copy for done and fails on any other
      val existing = registry.copiesOf(local).flatMap(_.copies.get(storeId)).filter(_.phase != Phase.Retired)
        .map(r => (r.phase, CopyLayout.path(local.devicePath, storeId, r.copyId, r.phase)))
      val sr = ctx.storeRebuildFactory.createRebuild(
        ctx.client, storeId, local.storageDeviceId, local.devicePath, ctx.backends, local.io, episode, existing)

      val settled = Promise[Unit]()

      // Set by an outcome that ends the store's Rebuild work (rebuild-process 6.2). The work stays Running through the
      // outcome's steps (the move to Live and load an Adopt makes) and ends at the release
      var workEnds = false

      // Every completion branch runs it. The work ends here if its outcome ended it; otherwise (a failure retried after
      // its backoff, a superseded flip, a cancel for a shutdown) it is Queued again, which clears an Abandoned stage
      // too: an abandoned rebuild past finish's latch re-check, or with another cause latched, ignores the cancel and
      // ends in some other outcome, and a stage left behind would stop cancelRebuild cancelling a later rebuild of the
      // store here
      def release(): Unit =
        if runningRebuilds.get(storeId).exists(_.settled eq settled) then
          runningRebuilds -= storeId
        if workEnds then
          endRebuildWork(storeId, deviceId)
        else
          requeue(storeId, deviceId)
        ctx.publishView()
        settled.trySuccess(())

      def recheckAll(): Unit =
        ctx.devices.devices.keys.foreach(ctx.reconciler.checkStorageDevice)

      /** Every loaded device except the one this rebuild ran on.
       *
       *  For the failure path only. The other devices are still
       *  worth re-checking, but re-checking this one restarts the rebuild that just failed:
       *  `release()` has already put the store's work back to Queued, the tree entry still
       *  reads `Rebuilding`, and reconcileDeviceState's Rebuilding pass therefore starts it
       *  again. The single-flight guard does not intervene -- this callback fires long after
       *  startDeviceCheck returned, so `activeDeviceChecks` is empty.
       *
       *  The cost, accepted: on a single-device host the filter leaves nothing to re-check, so
       *  a sibling store queued behind the freed slot waits for the periodic sweep rather than
       *  starting at once. Delaying an innocent sibling by one sweep is the cheaper error than
       *  spinning on the failed store with no backoff.
       */
      def recheckOtherDevices(): Unit =
        ctx.devices.devices.keys
          .filter(_ != local.storageDeviceId)
          .foreach(ctx.reconciler.checkStorageDevice)

      // The store's metadata is settled (a later failure starts the backoff afresh): its slot is released, then
      // `recheck` runs
      def finishSettled(recheck: => Unit): Unit =
        // Once: a lane job's apply settles, and a bug's throw after it must not release the slot twice
        if !settled.isCompleted then
          rebuildFailures -= storeId
          release()
          recheck

      // Guard-map leak defense: invoke `complete` in a try/catch that substitutes Future.failed(t)
      // on throw, so that either way a Future exists and onComplete below is registered. That
      // guarantees the Failure branch's release() runs. The map entry is added unconditionally
      // after the catch, so reordering alone does not close the leak — what matters is that a
      // throwing complete cannot leave the callback unregistered.
      val completeFuture = try
        sr.complete
      catch
        case t: Throwable =>
          logger.error(s"Rebuild factory's complete threw for store $storeId: $t")
          Future.failed(t)

      runningRebuilds += storeId -> StoreRebuilds.RunningRebuild(sr, deviceId, episode, settled)
      // From Queued, or from the discard that hands its slot to it
      setWork(storeId, deviceId, Some(Work.Rebuild(episode, RebuildStage.Running)))
      ctx.publishView()

      // The pool already names this device, so peers would otherwise pick a store that cannot
      // answer until the rebuild completes
      ctx.services.gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))

      completeFuture.onComplete:
        case Success(copyId) =>
          // The rebuild placed its finished copy in rebuilt/ as `copyId` (RebuildingStore.finish, off the control
          // thread, T2), or found it there
          runningRebuilds.get(storeId).filter(_.settled eq settled).foreach: r =>
            runningRebuilds += storeId -> r.copy(finishRecorded = true)
          // A rename into rebuilt/ that landed unforced: the copy is recorded unsynced, its directories held for the
          // resync. Read only after the completion, which publishes it
          val unforced = sr.placedUnforced
          unforced.foreach(e => ctx.moves.placedUnforced(e.source, e.target))
          ctx.copyRegistry.rebuilt("rebuild finish", local, storeId,
            Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode)), copyId, unsynced = unforced.isDefined)
          // Another copy of the store on the device, back while the rebuild ran (a mount or restore finishing late,
          // after the retire before this pass): its device-CRL entries must be saved into it before the rebuilt copy's
          // first load drops them, and the device holds one copy of a store (P2). The rebuilt copy, never served, is
          // discarded unflipped, and the next pass's retire (retireForeignCopy, which re-reads the disk) retires the
          // returned copy with its entries before the store is rebuilt again. The store's copies are listed by one
          // RebuildBookkeeping job on the device's lane; the rebuild's Running work keeps every other decision off the
          // copy meanwhile, and its slot stays taken until the outcome below releases it. A listing that fails (or is
          // cancelled) says nothing: the copy is not flipped yet, as an unsynced one is not
          def flipFinished(others: Try[List[CopyLayout.Found]]): Unit =
            if !others.toOption.exists(_.isEmpty) then
              try
                others.foreach: found =>
                  logger.error(s"Rebuild of store $storeId on device ${local.storageDeviceId}: ${found.map(_.path)
                    .mkString(", ")} came back while it ran. Discarding the rebuilt copy, so that copy is retired with " +
                    "its CRL state and the store rebuilt again")
                  ctx.copies.discard(storeId, local.storageDeviceId,
                    "Another copy of the store came back while it was rebuilt"):
                    sds => ctx.copyRegistry.removed("rebuild finish beside a returned copy", sds, storeId)
                others.failed.foreach: t =>
                  if t != DeviceIo.Cancelled then
                    logger.warn(s"Rebuild of store $storeId on device ${local.storageDeviceId}: its device's copies " +
                                s"of the store cannot be listed. Not flipping it yet: $t")
              finally
                release()
                ctx.reconciler.scheduleDeviceRecheck(deviceId)
            else if registry.isUnsynced(local, storeId) then
              // Not flipped over a copy a crash could still undo: the store would be Active with no copy, or its copy
              // back in staging. Its work is queued again, and the device check scheduled here forces it first
              // (CopyMoves.resync) and then restarts the rebuild, which takes this copy for done (RebuildingStore.start)
              // and flips it. No second rebuild runs over it
              try
                logger.warn(s"Rebuild of store $storeId on device ${local.storageDeviceId}: its copy in rebuilt/ is " +
                            "not forced yet. Flipping it once a device check has forced it")
              finally
                release()
                ctx.reconciler.scheduleDeviceRecheck(deviceId)
            else
              flip()

          def flip(): Unit =
            // Loading is downstream of the metadata decision, not ahead of it. An unconditional
            // load hands a live copy to a device that markRebuiltStoreActive may be about to
            // decide must not own it (because the device was tombstoned mid-rebuild), and nothing
            // would ever take it back.
            ctx.metadata.markRebuiltStoreActive(local.storageDeviceId, storeId, episode, ctx.gossipStoreOnline)
              .onComplete:
              case Success(RebuildOutcome.Adopt) =>
                try
                  // Loaded only once it is Live, or tryLoadStore leaves it offline. Its copy keeps it silent until the
                  // load's install serves it
                  workEnds = true
                  // Otherwise its copy is offline on its device: a failed move leaves it Rebuilt for the Completed
                  // Rebuilds pass, and one not forced leaves it Live and unsynced, which the next check of the device
                  // forces and loads (CopyMoves, rebuild-process 5.7)
                  // The move is a lane job (bringRebuiltLiveOnLane); its slot is released once its result is
                  // applied, whatever it is (a stale or cancelled one is NotMoved), and a load follows a durable one
                  bringRebuiltLiveOnLane(local, storeId): moved =>
                    try
                      if moved == MoveResult.Durable then
                        ctx.loadStoreById(local.storageDeviceId, storeId)
                    finally finishSettled(recheckAll())
                catch case NonFatal(t) =>
                  logger.error(s"Failed to bring the rebuilt store $storeId Live, a bug: $t")
                  finishSettled(recheckAll())

              case Success(RebuildOutcome.AdoptOffline) =>
                try
                  workEnds = true
                  // A lane job too, released once its result is applied
                  keepRebuiltStoreOffline(local, storeId, copyId)(finishSettled(recheckAll()))
                catch case NonFatal(t) =>
                  logger.error(s"Failed to park the rebuilt store $storeId, a bug: $t")
                  finishSettled(recheckAll())

              case Success(RebuildOutcome.Discard) =>
                try
                  workEnds = true
                  ctx.copies.discard(storeId, local.storageDeviceId,
                    "The tree does not say this device owns a rebuilt store"):
                    sds => ctx.copyRegistry.removed("RebuildOutcome.Discard", sds, storeId)
                finally
                  finishSettled(recheckAll())

              case Success(RebuildOutcome.Superseded) =>
                // The entry is Rebuilding here under a later episode: the store was re-placed away and back while this
                // rebuild ran. Its copy stays in rebuilt/, offline, Rebuilt for this episode, so the next rebuild
                // retires it (retireForeignCopy) and starts over, or the Deleted Stores pass does if the entry leaves
                // the device first
                try
                  logger.info(s"Rebuild of store $storeId on device ${local.storageDeviceId} finished for an earlier " +
                              "Rebuilding episode. Not promoting it; the store is rebuilt again")
                finally
                  finishSettled(recheckAll())

              case Success(RebuildOutcome.Unconfirmed) =>
                // An attempt reported Unknown may have made the copy the owner's before its entry moved on (a transfer
                // out, say). Kept in place, offline and unpromoted, for the pass that owns the entry's state: the
                // transfer ships it, an aborted one reinstates it, the Deleted Stores pass retires it
                try
                  workEnds = true
                finally
                  finishSettled(recheckAll())

              case Success(RebuildOutcome.NotOwned) =>
                // The device is another host's now; the copy is left for it. Only the other
                // devices are worth re-checking: this one is not ours to reconcile.
                try
                  // Its copy keeps it silent, and once the device is unloaded its tree entry does
                  workEnds = true
                  logger.info(s"Rebuild of store $storeId finished after device " +
                              s"${local.storageDeviceId} moved to another host. Leaving it to that host")
                finally
                  finishSettled(recheckOtherDevices())

              case Failure(t) =>
                // The only observer this Future has. Its onFail routes the permanent errors to
                // StopRetrying, which fails the promise -- and an unobserved failed Promise is
                // not reported by the ExecutionContext, so without this line a permanent give-up
                // is exactly as silent as the infinite retry loop it replaced. What is left
                // behind matters: the store is on disk, the tree still says Rebuilding.
                try
                  // Its copy is offline on its device, so the Deleted Stores pass retires it if the entry leaves the
                  // device before the retry
                  logger.error(s"Failed to record the completed rebuild of store $storeId on " +
                               s"device ${local.storageDeviceId}. The store is on disk in " +
                               s"${ctx.copyRegistry.pathOf(local, storeId).getOrElse("rebuilt/")} but is not " +
                               s"loaded, and the device entry still says Rebuilding. Retrying with a backoff", t)
                finally
                  release()
                  // Not this device at once: its reconcile restarts the rebuild, which flips again
                  // (rebuild-process 5.7)
                  recheckOtherDevices()
                  scheduleRebuildRetry(storeId, held)

          val devicePath = local.devicePath
          // The path's current DeviceIo: the device may have been unloaded meanwhile, and its DeviceIo forgotten
          ctx.onControl("rebuild finish listing", ctx.deviceIos.forPath(devicePath).submit(JobKind.RebuildBookkeeping,
              Some(storeId))(
              RebuildBookkeepingDisk.copiesOf(devicePath, storeId))): listed =>
            try flipFinished(listed.map(_.filter(_.copyId != copyId)))
            catch case NonFatal(t) =>
              // A bug (the flip's start threw): the slot is released as for a failed flip, retried after its backoff
              logger.error(s"Failed to flip the rebuilt store $storeId on device $deviceId, a bug: $t")
              if !settled.isCompleted then
                release()
                recheckOtherDevices()
                scheduleRebuildRetry(storeId, held)

        case Failure(RebuildingStore.RebuildCancelled(_))
            if registry.rebuildWorkOn(deviceId, storeId).exists(_.stage == RebuildStage.Abandoned) =>
          // Its entry left this device (cancelRebuild). This device is checked once more: if the entry is
          // still gone, that check deletes the staging copy; if it reads Rebuilding after all, the rebuild resumes
          try
            workEnds = true
            logger.info(s"Rebuild of store $storeId on device ${local.storageDeviceId} abandoned: the store is " +
                        "no longer being rebuilt here")
          finally
            release()
            recheckAll()

        case Failure(RebuildingStore.RebuildCancelled(_)) =>
          // Cancelled by a shutdown of this device. Not an error, and this device is not
          // re-checked: it is shutting down. The checkpoint stays in staging, so the rebuild
          // resumes once the device is back in service.
          try
            logger.info(s"Rebuild of store $storeId cancelled")
          finally
            release()
            recheckOtherDevices()

        case Failure(err) =>
          // The checkpoint survives in the staging directory, so the next check of this device
          // resumes rather than restarting. That check is not immediate: this device is excluded
          // from the re-check here, because a rebuild that fails in well under a millisecond --
          // an unreadable checkpoint, a pool that cannot be read -- would spin through tree reads
          // and WARN lines forever. It comes from scheduleRebuildRetry's backoff (rebuild-process 5.7). The
          // other devices are re-checked, as every completion re-checks them, and may be
          // what a queued rebuild elsewhere is waiting on.
          try
            logger.warn(s"Rebuild of store $storeId failed: $err")
          finally
            release()
            recheckOtherDevices()
            scheduleRebuildRetry(storeId, held)

  /** Cancels the running rebuild of `storeId` on `local`, whose entry there is no longer Rebuilding for the
   *  rebuild's episode: re-placed elsewhere by AspenClient.replaceRebuild, gone for another reason, or re-placed away
   *  and back since it started (the reconcile's CopyDecision.CancelRebuild, which checked the work is Running and the
   *  handle there). Its work goes to Abandoned until its slot is released; its completion checks the device again,
   *  which starts the current episode */
  def cancelRebuild(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    val deviceId = local.storageDeviceId
    registry.rebuildWorkOn(deviceId, storeId).foreach: w =>
      runningRebuilds.get(storeId).foreach: r =>
        logger.info(s"Cancelling the rebuild of store $storeId on device $deviceId: its entry there is no " +
                    "longer Rebuilding for this rebuild's episode")
        setWork(storeId, deviceId, Some(Work.Rebuild(w.episode, RebuildStage.Abandoned)))
        r.sr.cancel()

  /** Whether a rebuild of `storeId` holds its handle here (runningRebuilds): CopyDecision.StoreFacts.rebuildHandle */
  private[server] def hasRunningRebuild(storeId: StoreId): Boolean = runningRebuilds.contains(storeId)

  // The devices (by path) with a staging sweep's removal job outstanding (sweepUnlistedStaging): at most one each
  private var stagingSweeps: Set[os.Path] = Set()

  /** Whether a staging sweep's removal job is outstanding on the device at `devicePath` (tests) */
  private[server] def stagingSweepRunning(devicePath: os.Path): Boolean = stagingSweeps.contains(devicePath)

  /** Deletes each rebuild staging directory on `local` of a store whose entry there is not Rebuilding and that no
   *  rebuild reads: nothing will resume it, and a later rebuild of the store here starts over. Called by the reconcile
   *  of a device this host owns and serves, after its stores' decisions, with the check's disk inputs: the staging
   *  listing is read with them (ReconcileDisk.gather), the decision made here from control state as it is now, and
   *  the removals are one RebuildBookkeeping job on the device's lane, at most one outstanding per device (a sweep
   *  that finds one pending removes nothing; the next check sweeps again). A rebuild of a store that starts after the
   *  removal was submitted runs its staging jobs behind it on the same lane, so it never resumes a checkpoint being
   *  removed: it starts over. A listing that failed removes nothing that round */
  def sweepUnlistedStaging(local: LocalStorageDeviceState, remote: StorageDeviceState,
                           disk: ReconcileDisk.ReconcileDiskInputs): Unit =
    def listed(storeId: StoreId): Boolean =
      remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Rebuilding)

    val devicePath = local.devicePath
    if !stagingSweeps.contains(devicePath) then
      disk.staging match
        case Failure(t) =>
          logger.warn(s"Failed to list ${RebuildBookkeepingDisk.stagingRoot(devicePath)}: $t")
        case Success(staged) =>
          val doomed = staged.filter: dir =>
            Try(StoreId(dir.last)).toOption.exists: storeId =>
              !listed(storeId) && !registry.rebuilding(HostCopies.RebuildReads).contains(storeId)
          if doomed.nonEmpty then
            doomed.foreach: dir =>
              logger.info(s"Deleting the staging copy at $dir: its store is no longer being rebuilt on " +
                          s"device ${local.storageDeviceId}")
            stagingSweeps += devicePath
            val job = local.io.submit(JobKind.RebuildBookkeeping, None)(RebuildBookkeepingDisk.removeStaging(doomed,
              dir => local.io.fsifJournal.get.foreach(_.releaseDirectory(dir))))
            ctx.onControl("rebuild staging sweep", job): result =>
              stagingSweeps -= devicePath
              result match
                case Success(outcomes) =>
                  outcomes.foreach:
                    case (dir, Some(t)) =>
                      logger.warn(s"Failed to delete $dir. Retrying at a later check. Error: $t")
                    case _ => ()
                case Failure(DeviceIo.Cancelled) => ()
                case Failure(t) => logger.warn(s"Failed to delete the staging of device ${local.storageDeviceId}: $t")

  /** Ends the queued Rebuild work of `storeId` on `local`, whose entry there is no longer Rebuilding and whose
   *  rebuild does not run (rebuild-process 6.2; the reconcile's CopyDecision.EndQueuedRebuild, which checked): a
   *  queued store whose entry left before it started, or the kept work of a superseded or permanently failed flip */
  def endQueuedRebuild(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    endRebuildWork(storeId, local.storageDeviceId)

  /** Ends the Rebuild work this host holds for `storeId` on `deviceId` (rebuild-process 6.2). Whatever else names the
   *  store here (its copy, an unloaded device's entry) keeps it silent; nothing else does */
  private def endRebuildWork(storeId: StoreId, deviceId: StorageDeviceId): Unit =
    setWork(storeId, deviceId, None): Unit

  /** Checks `deviceId` again after a backoff, so a store whose rebuild pass or flip failed is retried without waiting
   *  for the periodic sweep (an hour) and without spinning (rebuild-process 5.7). The first delay is
   *  Host.RebuildRetryInitialDelay, doubled for each failure of the store in a row, up to Host.RebuildRetryMaxDelay;
   *  a settled flip resets it. At most one is scheduled per store. Until it runs, startRebuild does not start the store
   *  for what failed last (HeldRebuild; any other check of the device would restart it, and stores failing for good
   *  would starve the ones queued behind them); a re-placement to another device or episode, or an accept-rebuild-loss
   *  since, starts at once.
   *  The check is an ordinary reconcile: a device unloaded meanwhile, or an entry that has left Rebuilding, does
   *  nothing. */
  private def scheduleRebuildRetry(storeId: StoreId, held: StoreRebuilds.HeldRebuild): Unit =
    val failures = rebuildFailures.getOrElse(storeId, 0)
    rebuildFailures += storeId -> (failures + 1)
    val scheduled = scheduledRebuildRetries.contains(storeId)
    // One already scheduled covers this failure: it holds back, and then checks the device of, the one that failed last
    scheduledRebuildRetries += storeId -> held
    if !scheduled then
      val delay = StoreRebuilds.rebuildRetryDelay(failures)
      logger.info(s"Retrying the rebuild of store $storeId on device ${held.deviceId} in $delay")
      ctx.scheduler.schedule(delay):
        scheduledRebuildRetries.get(storeId).foreach: latest =>
          scheduledRebuildRetries -= storeId
          ctx.reconciler.checkStorageDevice(latest.deviceId)

  /** Keeps a rebuilt store whose entry was written Offline (RebuildOutcome.AdoptOffline) out of service: it is not
   *  loaded, and its copy is parked, moved from rebuilt/ to parked/ in one move (T4), beside the empty CRL save file
   *  the rebuild wrote (RebuildDisk.writeEmptyCrlState). A restart keeps it offline there until the reconcile
   *  (the Abandoned Transfers Out pass) or bringIntoService finds its entry Active with no transfer device and moves it
   *  Live (T5), and an adoption classes it Clean.
   *
   *  Moved even when the device has already been unloaded (its shutdown finished first), at its rebuilt/ path as
   *  `copyId`: the device path is still known, and without the move an adopting host would find a Rebuilt copy whose
   *  entry is no longer Rebuilding. A disk already pulled makes the move fail, which is logged; the copy then stays
   *  Rebuilt, offline.
   *
   *  The move is one `Move` job on the device's lane (CopyMoves.moveOnLane, or moveUnattachedOnLane for an unloaded
   *  device), and `andThen` runs on the control thread once its result is applied, whatever it is: the rebuild's slot
   *  is released there.
   */
  private def keepRebuiltStoreOffline(local: LocalStorageDeviceState, storeId: StoreId, copyId: UUID)
                                     (andThen: => Unit): Unit =
    val site = "keep rebuilt store offline"
    def done(result: MoveResult): Unit =
      try
        result match
          case MoveResult.NotMoved(_) =>
            // Logged by the move. Offline either way: a Rebuilt copy goes Live only once its entry is Active (the
            // Completed Rebuilds pass)
            logger.error(s"Failed to park rebuilt store $storeId on device ${local.storageDeviceId}: it stays " +
                         "offline, Rebuilt")
          case _ => ()
      finally andThen
    ctx.devices.devices.get(local.storageDeviceId) match
      case Some(sds) if ctx.copyRegistry.copiesOf(sds).isDefined =>
        ctx.moves.moveOnLane(site, sds, storeId, Phase.Parked)(done)
      case _ =>
        // Unloaded: its copy is where its rebuild finished it, in rebuilt/, moved by a job on its device's lane
        val rebuilt = CopyLayout.path(local.devicePath, storeId, copyId, Phase.Rebuilt(""))
        // On the path's current DeviceIo: an unloaded device's may have been forgotten (DeviceIos.forget)
        ctx.moves.moveUnattachedOnLane(site, ctx.deviceIos.forPath(local.devicePath), rebuilt, storeId, copyId,
          Phase.Parked)((r, _) => done(r))

  /** T3: moves the Rebuilt copy of `storeId` on `sds`, whose entry has left Rebuilding for Active, to Live, so it can be
   *  loaded. Durable once it may be loaded. A failed move is logged and leaves the copy Rebuilt and offline, for the
   *  Completed Rebuilds pass (DeviceReconciler) at a later check; one not forced leaves it Live and unsynced, which the
   *  next check forces and loads (CopyMoves). A device no longer attached moves nothing: its next load lists the copy
   *  Rebuilt, and that pass moves it. One `Move` job on the device's lane (CopyMoves.moveOnLane), its result given to
   *  `andThen` on the control thread: NotMoved for a device unloaded or a record changed meanwhile, or a cancelled
   *  job */
  def bringRebuiltLiveOnLane(sds: LocalStorageDeviceState, storeId: StoreId)(andThen: MoveResult => Unit): Unit =
    if ctx.copyRegistry.copiesOf(sds).isEmpty then
      logger.info(s"Not moving the rebuilt copy of store $storeId Live: device ${sds.storageDeviceId} is unloaded")
      andThen(MoveResult.NotMoved(IllegalStateException(s"Device ${sds.storageDeviceId} is unloaded")))
    else
      ctx.moves.moveOnLane("rebuilt copy goes live", sds, storeId, Phase.Live)(andThen)

  /** A copy of a store being rebuilt here that is neither this `episode`'s finished rebuild (Rebuilt for it, awaiting
   *  its flip) nor Lost (in lost/, which discardLostStore clears) was not produced by this rebuild: a leftover of an
   *  earlier tenancy of the store on this device (a transferred-away copy whose delete failed, or a copy finished for
   *  an earlier Rebuilding episode, superseded by a re-placement away and back), or a copy put there by hand. Nothing
   *  ties it to the store's current state, so it is retired as a lost copy: moved to lost/, and then kept in
   *  pending-delete by discardLostStore like any other. A served one is first taken out of service as a failed backend
   *  is, and its rebuild waits for a later check. The copy is told by its record's phase; a store with no record has
   *  no copy on the device.
   *
   *  Any copy other than this episode's finished one, a foreign or a Lost one, may have served since the rebuild's
   *  first pass began: a restart's device load serves a Live copy until the device's first reconcile, and a served
   *  copy that fails is moved to lost/. A resumed walk would leave its acknowledged updates on the objects walked
   *  before the checkpoint stale with no mark, so the store's staging checkpoint is first removed durably, making the
   *  next pass a first pass (dropStagingCheckpoint). No rebuild of the store runs here when this is called. Returns
   *  whether the rebuild may go on now.
   *
   *  Its disk steps are lane jobs, and it returns false while one runs: the checkpoint's drop (one RebuildBookkeeping
   *  job, dropStagingCheckpoint) and the move to lost/ (moveToLostOnLane). Each one's apply continues the start
   *  (continueQueued) with `checkpointDropped`, so the drop is not made again, but only while the store's Rebuild work
   *  on the device is still Queued for `episode` on a device still loaded; otherwise a later check decides afresh. */
  private def retireForeignCopy(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID],
                                acceptLoss: Boolean, checkpointDropped: Boolean,
                                disk: Option[ReconcileDisk.ReconcileDiskInputs]): Boolean =
    // The record is first matched to the device check's disk inputs (HostCopies.reread): a copy back since the
    // device's load is retired with its device-CRL entries, and the record of one gone since is forgotten, rather than
    // its move to lost/ failing for good. A re-read that fails waits for a later check. A start with no inputs (the
    // handoff after a discard, whose own start re-read them moments before, or a continuation after a drop or a move)
    // re-reads nothing: a copy that came back in between fails the rebuild's start, and the backoff's check re-reads it
    if disk.exists(d => !registry.reread("retireForeignCopy", local, storeId, d)) then
      return false
    val storePath = ctx.copyRegistry.pathOf(local, storeId)
    // Some(None): no copy or this episode's finished copy; Some(Some(lost)): another copy, Lost (in lost/) or not
    val found: Option[Option[Boolean]] =
      registry.copiesOf(local).flatMap(_.copies.get(storeId)).map(_.phase) match
        case Some(Phase.Lost) => Some(Some(true))
        case Some(phase) if RebuildingStore.isRebuiltFor(phase, episode) => Some(None)
        case Some(_) => Some(Some(false))
        case None => Some(None)
    if ctx.moves.isPendingLost(local, storeId) then
      // Its move to lost/ failed: held offline until a device check moves it
      false
    else if found.isEmpty then
      false
    else if found.contains(None) then
      true
    else if !checkpointDropped then
      dropStagingCheckpoint(local, storeId, episode, acceptLoss)
      false
    else if found.contains(Some(true)) then
      true
    else if ctx.loader.isOpening(storeId) then
      false
    else
      val err = new IllegalStateException(s"The copy of store $storeId${storePath.fold("")(p => s" at $p")} is " +
                                          "one no rebuild produced, while the store is Rebuilding on this device")
      ctx.router.entry(storeId) match
        case Some(entry) if storePath.contains(entry.store.backend.path) =>
          backendFailed(storeId, Some(entry.store.backend), err, "is served from a copy no rebuild produced")
          false
        case _ =>
          logger.error(s"Retiring a store directory before rebuilding: ${err.getMessage}. It is kept in " +
                       s"pending-delete until the store is Active again")
          // Durable in lost/, or the rebuild waits: a move not forced could be undone by a crash. The checkpoint was
          // dropped just before, so the start goes on from the Lost copy's discard
          moveToLostOnLane("retireForeignCopy", local, storeId): moved =>
            if moved == MoveResult.Durable then
              continueQueued(local, storeId, episode, acceptLoss)
          false

  // The (device, store) pairs with a staging checkpoint drop outstanding (dropStagingCheckpoint): at most one each, and
  // no rebuild of the store starts on the device meanwhile
  private var stagingDrops: Set[(StorageDeviceId, StoreId)] = Set()

  /** Whether a staging checkpoint drop of `storeId` on `deviceId` is outstanding (tests) */
  private[server] def stagingDropRunning(deviceId: StorageDeviceId, storeId: StoreId): Boolean =
    stagingDrops.contains((deviceId, storeId))

  /** Removes the rebuild checkpoint of `storeId` from its staging directory on `local`, durably, then the rest of the
   *  directory (a first pass's createStaging discards whatever is left), as one RebuildBookkeeping job on the device's
   *  lane (RebuildBookkeepingDisk.dropStagingCheckpoint). A staging directory without a checkpoint is a first pass's.
   *  The apply continues the start (continueQueued) once the drop is done (or there was nothing to drop); a stat or
   *  removal that fails is logged and the rebuild waits for a later check, as does a cancelled job. */
  private def dropStagingCheckpoint(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID],
                                    acceptLoss: Boolean): Unit =
    val key = (local.storageDeviceId, storeId)
    if !stagingDrops.contains(key) then
      val staging = RebuildBookkeepingDisk.stagingOf(local.devicePath, storeId)
      stagingDrops += key
      val job = local.io.submit(JobKind.RebuildBookkeeping, Some(storeId))(
        RebuildBookkeepingDisk.dropStagingCheckpoint(staging))
      ctx.onControl("rebuild staging checkpoint drop", job): result =>
        stagingDrops -= key
        result match
          case Success(RebuildBookkeepingDisk.CheckpointDrop.Failed(t)) =>
            logger.error(s"Failed to discard the rebuild checkpoint of store $storeId at $staging. Not rebuilding it " +
                         s"yet: $t")
          case Success(drop) =>
            if drop == RebuildBookkeepingDisk.CheckpointDrop.Dropped then
              logger.info(s"Discarded the rebuild staging copy of store $storeId at $staging: a copy at the store's " +
                          "path may have served since its first pass began, so the rebuild starts over")
            continueQueued(local, storeId, episode, acceptLoss)
          case Failure(DeviceIo.Cancelled) => ()
          case Failure(t) =>
            logger.error(s"Failed to discard the rebuild checkpoint of store $storeId at $staging. Not rebuilding it " +
                         s"yet: $t")

  /** A lane job of a rebuild's start (a checkpoint drop, a move to lost/) has been applied: the start goes on, its
   *  checkpoint dropped, but only while the store's Rebuild work on `local` is still Queued for `episode` and the
   *  device still the loaded one. Otherwise nothing: the work ended or moved on (a later check decides afresh) */
  private def continueQueued(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID],
                             acceptLoss: Boolean): Unit =
    if ctx.devices.isLoaded(local) &&
       registry.rebuildWorkOn(local.storageDeviceId, storeId).exists(w =>
         w.stage == RebuildStage.Queued && w.episode == episode) then
      startRebuild(local, storeId, episode, acceptLoss, checkpointDropped = true)

  /** A store served from `local` that the device's entry does not list: a copy put at the store path by hand, loaded
   *  without reading the tree. Nothing ties it to the store's current state, so it is taken out of service as a
   *  failed backend is (closed, moved to lost/ when its close ends, its CRL entries kept), and the Deleted Stores
   *  pass of the check that follows retires it to pending-delete. A store still opening is left for a later check. */
  def retireUnlistedServed(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    takeOutOfService(local, storeId, "is served from a copy its device's entry does not list")

  /** Takes the copy of `storeId` served from `local` out of service as a failed backend is: a FailStop close, the copy
   *  moved to lost/ when the close ends (P5), its CRL entries kept for its retire. `why` completes "store <id> ...".
   *  Nothing is done for a store still opening, or one not served from this device's copy */
  def takeOutOfService(local: LocalStorageDeviceState, storeId: StoreId, why: String): Unit =
    val storePath = ctx.copyRegistry.pathOf(local, storeId)
    if !ctx.loader.isOpening(storeId) then
      ctx.router.entry(storeId) match
        case Some(entry) if entry.store.storageDeviceId == local.storageDeviceId &&
                            storePath.contains(entry.store.backend.path) =>
          val err = new IllegalStateException(s"${entry.store.backend.path} holds a copy of store $storeId that $why")
          backendFailed(storeId, Some(entry.store.backend), err, why)
        case _ => ()

  /** T10 for a closed copy of an attached device: moves the copy of `storeId` on `sds` into lost/, where it is never
   *  loaded, shipped or promoted, and only retired (T11), as one `Move` job on the device's lane (CopyMoves.moveOnLane),
   *  its result given to `andThen` on the control thread. A copy already Lost is left there (Durable). A move that
   *  fails is logged at error and its copy held offline (CopyMoves.pendingLost: never loaded, shipped or retired) and
   *  retried at each device check before the passes; a crash forgets the hold, and the copy then reloads from where it
   *  is, as after any crash (P5). An unsynced move leaves the copy Lost and unsynced until a device check forces it.
   *  Called once the copy is closed. One opened, opening or closing again since (a failed unserved backend's close
   *  ending after a new open began) is not moved, held or retried: logged at warn, NotMoved, left to its own open or
   *  close (CopyMoves.move would refuse it). The copy is held offline (pendingLost) from the job's submission, so
   *  every reader takes it for Lost while the job runs. A device unloaded meanwhile holds nothing (its holds went
   *  with its detach). The job is tracked as a close is (closeStarted), so the device's unload waits for it: every move to
   *  lost/ is made on the attached device */
  def moveToLostOnLane(site: String, sds: LocalStorageDeviceState, storeId: StoreId)
                      (andThen: MoveResult => Unit): Unit =
    lostMoveDecided(site, sds, storeId) match
      case Some(result) => andThen(result)
      case None =>
        val held = registry.copiesOf(sds).flatMap(_.copies.get(storeId)).isDefined
        // Held offline from now, as a failed move's copy is (CopyMoves.pendingLost: never loaded, shipped or
        // retired): a copy on its way to lost/ is Lost to every reader. The move's record releases the hold
        // (HostCopies.moved); a NotMoved one keeps it, for the retry at each device check
        if held then
          ctx.moves.holdLost(sds, storeId, held = true)
        val tracked = closeStarted(sds.storageDeviceId)
        try
          ctx.moves.moveOnLane(site, sds, storeId, Phase.Lost): result =>
            try
              if ctx.devices.isLoaded(sds) then
                lostMoveDone(site, sds, storeId, held, result)
              andThen(result)
            finally closeDone(sds.storageDeviceId, tracked)
        catch case NonFatal(t) =>
          closeDone(sds.storageDeviceId, tracked)
          throw t

  /** moveToLostOnLane's outcome where no move is made: a copy already Lost (Durable), or one opened, opening or closing
   *  since (NotMoved, its hold released). A copy busy with a disk job is held (pendingLost) and NotMoved: the move
   *  waits for the job's end, retried at the next device check, never made under it. None: the move is to be made */
  private def lostMoveDecided(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Option[MoveResult] =
    registry.copiesOf(sds).flatMap(_.copies.get(storeId)) match
      case Some(r) if r.phase == Phase.Lost =>
        ctx.moves.holdLost(sds, storeId, held = false)
        Some(MoveResult.Durable)
      case Some(r) if r.activity.isInstanceOf[Activity.DiskJob] =>
        logger.info(s"Not moving the copy of store $storeId on device ${sds.storageDeviceId} to lost/ yet ($site): " +
                    s"it is ${r.activity}. Held offline, and the move retried at the device's next check")
        ctx.moves.holdLost(sds, storeId, held = true)
        Some(MoveResult.NotMoved(IllegalStateException(s"the copy of $storeId is ${r.activity}")))
      case Some(r) if r.activity.isInstanceOf[Activity.Opening] || r.activity.underlying == Activity.Served ||
                      r.activity.isInstanceOf[Activity.Closing] =>
        // Opened, opening or closing since this move was asked for (a failed unserved backend's close ending after
        // a new open began): never moved while installed or opening, and not held for a retry, which would refuse at
        // every check for as long as it is served. Its own open or close path ends it: a backend that fails again
        // moves it then. Logged once, here
        ctx.moves.holdLost(sds, storeId, held = false)
        logger.warn(s"Not moving the copy of store $storeId on device ${sds.storageDeviceId} to lost/ ($site): it " +
                    s"is ${r.activity} now. Left to its own open or close")
        Some(MoveResult.NotMoved(IllegalStateException(s"the copy of $storeId is ${r.activity}")))
      case _ => None

  /** A move to lost/ that failed holds its copy offline, for the retry at each device check. No record: no copy to
   *  move, NotMoved, nothing held */
  private def lostMoveDone(site: String, sds: LocalStorageDeviceState, storeId: StoreId, recorded: Boolean,
                           result: MoveResult): Unit =
    result match
      case MoveResult.NotMoved(t) if recorded =>
        logger.error(s"Failed to move the copy of store $storeId on device ${sds.storageDeviceId} to lost/ " +
                     s"($site). It is held offline, and the move retried at each check of its device. Error: $t")
        ctx.moves.holdLost(sds, storeId, held = true)
      case _ => ()

  /** Retries the move to lost/ of every copy of `local` whose move failed (CopyMoves.pendingLost): each device check
   *  does, before its passes. `disk`: the check's disk inputs (ReconcileDisk.gather), None for a device-level branch,
   *  which re-reads nothing */
  def retryPendingLost(local: LocalStorageDeviceState, disk: Option[ReconcileDisk.ReconcileDiskInputs]): Unit =
    ctx.moves.pendingLost(local).toList.sortBy(s => (s.poolId.uuid, s.poolIndex)).foreach: storeId =>
      // A copy whose directory is gone has its record forgotten, which releases the hold: no move can succeed
      disk.foreach(d => registry.reread("retry move to lost", local, storeId, d): Unit)
      if ctx.moves.isPendingLost(local, storeId) then
        moveToLostOnLane("retry move to lost", local, storeId)(_ => ())

  // The closes running on each device (closeStarted): a store's close (handleClose, closeUnloaded) and an unserved
  // backend's (retireUnserved, handleBackendOpened's failed backend, StoreLoader.closeAbandoned). Every unload of a
  // device waits for them (DeviceShutdown.unloadOnceClosed): a move to lost/ their end makes is made on the attached
  // device, and no reload of the device opens a backend over one still closing
  private var lostCloses: Map[StorageDeviceId, Set[Promise[Unit]]] = Map()

  /** A close on device `deviceId` begins (a store's, or an unserved backend's): the device is not unloaded until the
   *  returned promise is given back to closeDone, after any move to lost/ its end makes */
  private[host] def closeStarted(deviceId: StorageDeviceId): Promise[Unit] =
    val p = Promise[Unit]()
    lostCloses += deviceId -> (lostCloses.getOrElse(deviceId, Set()) + p)
    p

  /** The close `p` (closeStarted) on device `deviceId` has ended, its copy moved to lost/ if it was to be */
  private[host] def closeDone(deviceId: StorageDeviceId, p: Promise[Unit]): Unit =
    val left = lostCloses.getOrElse(deviceId, Set()) - p
    lostCloses = if left.isEmpty then lostCloses - deviceId else lostCloses + (deviceId -> left)
    p.trySuccess(())

  /** Whether a close on device `deviceId` whose end may move its copy to lost/ is still running */
  private[server] def closesRunning(deviceId: StorageDeviceId): Boolean = lostCloses.contains(deviceId)

  /** Completes once every close on device `deviceId` running now (closeStarted) has ended */
  private[host] def closesEnded(deviceId: StorageDeviceId): Future[Unit] =
    Future.sequence(lostCloses.getOrElse(deviceId, Set()).toList.map(_.future)).map(_ => ())

  /** The closed copy of `storeId` at `path` on device `deviceId`: moved to lost/ (moveToLostOnLane), then its device checked
   *  so the reconcile flips its entry to Rebuilding. Every unload waits for the close that leads here
   *  (closeStarted), so a device not loaded is a bug: logged at error, and the copy moved all the same, unattached
   *  (CopyMoves.moveUnattachedOnLane), so that no later load or adoption takes it for a clean copy */
  private def moveClosedToLost(site: String, deviceId: StorageDeviceId, storeId: StoreId, path: os.Path): Unit =
    ctx.devices.devices.get(deviceId) match
      case Some(sds) =>
        // The move's job is tracked (moveToLostOnLane), so the device's unload waits for it after the close's end
        moveToLostOnLane(site, sds, storeId): _ =>
          if ctx.devices.isLoaded(sds) then
            ctx.reconciler.checkStorageDevice(deviceId)
      case None =>
        logger.error(s"The copy of store $storeId at $path closed after its device $deviceId was unloaded ($site), " +
                     "a bug: an unload waits for its closes. Moving it to lost/ unattached")
        // A copy in live/ (the only phase a served copy closes from); one already elsewhere is left. Moved by a job on
        // its device's lane
        CopyLayout.nameOf(path).filter(_ => CopyLayout.isLivePath(path)).foreach: (_, copyId) =>
          val io = ctx.deviceIos.forPath(path / os.up / os.up)
          ctx.moves.moveUnattachedOnLane(site, io, path, storeId, copyId, Phase.Lost): (moved, _) =>
            moved match
              case MoveResult.NotMoved(t) =>
                logger.error(s"Failed to move the copy of store $storeId at $path to lost/: $t")
              case _ => ()

  /** The LoadStore handler found `backend`'s CRL save file unusable. The file sits in the store's
   *  own directory, so it is the only copy of that state: the store is permanently failed. It
   *  is closed unserved, moved to lost/, and left offline; the reconcile then flips its entry to
   *  Rebuilding, and the rebuild discards the copy.
   *
   *  A store arriving over the network never reaches here with a bad file: its staged copy is
   *  checked before promotion, while the source still holds the state.
   */
  def crlSaveFileLost(storageDeviceId: StorageDeviceId,
                      backend: Backend,
                      err: Throwable): Unit =
    logger.error(s"Store ${backend.storeId} cannot be loaded: its CRL save file is unusable and its transaction " +
                 s"state is lost. Rebuilding it. Error: $err")
    retireUnserved(storageDeviceId, backend)

  /** crlSaveFileLost's steps, for any store that must never serve: closes `backend` unserved and, once the close has
   *  ended (no move is made of an open backend's directory), moves the copy to lost/, leaves it offline, and checks
   *  the device so the reconcile flips its entry to Rebuilding. A crash before the move finds the same unusable state
   *  at the next start and lands here again.
   *
   *  While the close runs its copy is held Closing(Unserved) (HostCopies.closingUnserved), not Idle: an Idle, synced
   *  Live copy under Active is CopyDecision's Load state, and a load in that window would open a second backend on the
   *  directory, which for a failed backend may serve, leaving the move below to find it Opening or Served and give
   *  up. The hold ends just before the move. A copy a reload's earlier store still serves is not held: it is Served */
  def retireUnserved(storageDeviceId: StorageDeviceId, backend: Backend): Unit =
    val storeId = backend.storeId
    val tracked = closeStarted(storageDeviceId)
    val held = registry.closingUnserved(storageDeviceId, storeId)
    val closed =
      try backend.close()
      catch case NonFatal(t) => Future.failed(t)
    closed.onComplete: result =>
      try
        result.failed.foreach(t => logger.warn(s"Failed to close the unserved backend of store $storeId: $t"))
        if held then
          registry.closeEnded(storageDeviceId, storeId)
        moveClosedToLost("retireUnserved", storageDeviceId, storeId, backend.path)
      finally closeDone(storageDeviceId, tracked)

  /** A store closed by ShutdownStore whose backend failed-stop meanwhile: its page-cache state can't be trusted, so
   *  it is rebuilt rather than treated as cleanly closed. Its copy, closed (Idle), is moved to lost/, where nothing
   *  takes it for a clean close and a shutdown retry does not pick it again. A save file the close wrote (a transfer
   *  out's, whose entries have left the device CRL) is the only copy of the store's transaction state: it moves with
   *  the copy, and the rebuild's retire keeps it in the copy as its lost CRL state (StoreCopies.retire). */
  def failedOnClose(store: Store): Unit =
    val storeId = store.storeId
    logger.error(s"Store $storeId failed on an I/O error while closing. Moving it to lost/ instead of closing it " +
                 s"cleanly. ${store.backend.path}")
    moveClosedToLost("failedOnClose", store.storageDeviceId, storeId, store.backend.path)

  /** The FailStop close of `storeId` on device `deviceId` ended (StoreLoader.closeUnloaded): the copy is moved to
   *  lost/ now it is closed (P5). A crash between the close and the move leaves it where it was, and the restart loads
   *  it as after any crash */
  def failStopClosed(deviceId: StorageDeviceId, storeId: StoreId, path: os.Path): Unit =
    moveClosedToLost("fail-stop close ended", deviceId, storeId, path)

  /** A backend failed-stop: its state can no longer be trusted, so the store is rebuilt. If it is
   *  served, it is unloaded, its CRL state left for the rebuild's discard to save and drop, and closed with purpose
   *  FailStop: when the close ends its copy is moved to lost/ and left offline, and its device checked so the
   *  reconcile flips its entry to Rebuilding (StoreLoader.closeUnloaded, failStopClosed). A crash between the close
   *  and the move leaves the copy where it was: the restart recovers it from its CRL as after any crash, and a backend
   *  still broken fails again (P5). Nothing is awaited: the close completes in the background. A backend not (or no
   *  longer) served is moved to lost/ if it is closed, unless replaced: its LoadStore, still queued, or its close,
   *  still running, retires it on seeing it failed. */
  private def backendFailed(storeId: StoreId, backend: Option[Backend], err: Throwable,
                            what: String = "failed on an I/O error"): Unit =
    // Store.backend and Backend.path are immutable, so the control side may read them while the store is served
    ctx.router.entry(storeId) match
      case Some(entry) if backend.exists(_ eq entry.store.backend) =>
        val store = entry.store
        logger.error(s"Store $storeId $what and can no longer be trusted. Taking it offline and " +
                     s"rebuilding it. Error: $err")
        ctx.copyRegistry.closingFor(storeId, ClosePurpose.FailStop)
        ctx.router.remove(storeId)
        // Its object cache cleared and close run on its executor; its slot released after, and its copy moved to lost/.
        // Its CRL entries stay in the device CRL: the rebuild's discard saves them with the copy it keeps, then drops
        // them (discardLostStore)
        ctx.loader.closeUnloaded(entry, dropDurably = false, ClosePurpose.FailStop)
        ctx.reconciler.checkStorageDevice(store.storageDeviceId)
      case served =>
        // Not served: its LoadStore is still queued (and retires it), it was replaced by a newer
        // backend, or it was closed. Unless replaced, its copy is moved to lost/, so a store closed
        // around the failure is never later served, shipped or adopted as cleanly closed.
        def recordAt(b: Backend): Option[(LocalStorageDeviceState, CopyRecord)] =
          ctx.devices.devices.valuesIterator.flatMap(sds =>
            registry.copiesOf(sds).flatMap(_.copies.get(storeId)).filter(_ =>
              registry.pathOf(sds, storeId).contains(b.path)).map(sds -> _)).nextOption()
        backend match
          case Some(b) if served.isEmpty && !ctx.router.loaded.valuesIterator.exists(_.backend.path == b.path) =>
            recordAt(b) match
              case Some((_, r)) if r.activity.isInstanceOf[Activity.Opening] ||
                                   r.activity.isInstanceOf[Activity.Closing] =>
                // Its LoadStore (handleLoad: failed before served) or its close's end (failedOnClose) retires it
                logger.error(s"Store $storeId failed on an I/O error while it was ${r.activity}. Its " +
                             s"${if r.activity.isInstanceOf[Activity.Opening] then "load" else "close"} retires it. " +
                             s"Error: $err")
              case Some((sds, _)) =>
                logger.error(s"Store $storeId failed on an I/O error after it stopped being served. Moving it to " +
                             s"lost/. Error: $err")
                moveToLostOnLane("backendFailed", sds, storeId)(_ => ())
              case None =>
                logger.info(s"Ignoring the failure of a backend of store $storeId whose copy is not recorded at " +
                            s"${b.path}. Error: $err")
          case _ =>
            logger.info(s"Ignoring the failure of a replaced backend of store $storeId. Error: $err")

  /** Flips the entry of a store whose CRL state was lost from Active to Rebuilding, on its own
   *  device. Nothing else changes: the pool already names this device and host. Writes nothing
   *  if the device is failed or owned elsewhere, or the entry is no longer Active. Re-checks the
   *  device afterwards, so the Rebuilding pass starts the rebuild now.
   *
   *  Only a collision is retried, as everywhere else in Host. A permanent failure is logged and
   *  left for a later reconcile: the Lost CRL State pass requests it again while the store's
   *  copy is in lost/, and the Missing Stores pass (which has no copy to go by) while
   *  the store it declared stays Active and unserved (LocalStorageDeviceState.declaredMissing),
   *  its directory back or not.
   */
  def requestRebuild(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    if !pendingRebuildRequests.contains(storeId) then
      pendingRebuildRequests += storeId
      val f =
        try
          ctx.rebuildRequestFailure(storeId).foreach(t => throw t)
          // The transaction's own machinery (commit, retries) runs on ctx.ec, and its result comes back through
          // the control-context onComplete; the prepare closure's combinators run on the control thread
          // A Rebuilding entry found after an attempt that committed but was reported TransactionOutcomeUnknown is
          // this request's own, so the rebuild is started now rather than at the next periodic check.
          // Starting it is idempotent should the entry be another writer's
          val retry = StopRetrying.collisionRetry()
          // One episode for every attempt, so a retry recognizes its own commit by it
          val episode = UUID.randomUUID()
          ctx.client.transactUntilSuccessfulWithRecovery(retry.onFailure) { tx =>
            for
              ptr <- ctx.client.getStorageDevicePointer(storageDeviceId)
              kvos <- ctx.client.read(ptr)
              state = StorageDeviceState(kvos)
            yield
              val entry = state.stores.get(storeId)
              val active = entry.exists(_.status == StorageDeviceState.StoreStatus.Active)
              val ours = !state.isFailed && state.hostId == ctx.hostId
              if ours && retry.mayHaveCommitted && entry.exists(_.rebuildEpisode.contains(episode)) then
                true
              else if ours && active then
                val newState = state.setRebuilding(storeId, episode)
                tx.update(ptr, None, None,
                  List(KeyRevision(StorageDeviceState.StateKey,
                    kvos.contents(StorageDeviceState.StateKey).revision)),
                  List(Insert(StorageDeviceState.StateKey, newState.encode())))
                true
              else
                // No update staged, as in markRebuiltStoreActive.
                false
          }(using ctx.ec)
        catch case t: Throwable => Future.failed(t)

      f.onComplete: result =>
        pendingRebuildRequests -= storeId
        result match
          case Success(true) =>
            logger.info(s"Store $storeId on device $storageDeviceId is now Rebuilding")
            ctx.reconciler.checkStorageDevice(storageDeviceId)
          case Success(false) => ()
          case Failure(t) =>
            logger.warn(s"Failed to mark store $storeId on device $storageDeviceId for rebuild. " +
                        s"Will retry on a later check. Error: $t")

  /** Clears the device of a store whose CRL state was lost, before its rebuild starts: a rebuild refuses to start
   *  while the device holds any copy of the store that is not its finished rebuild (RebuildingStore.start). Returns whether
   *  the path is clear now, so the rebuild may start.
   *
   *  The copy is retired (StoreCopies.retire: moved to lost/, CRL state saved into the copy, device-CRL entries dropped,
   *  moved into pending-delete), and deleted once the store is Active on the device its pool names. The retire
   *  completes asynchronously: this returns false meanwhile, the store holding a rebuild slot (its work DiscardingLost).
   *  Once the copy is in pending-delete the rebuild is started at once, in the same callback, so the slot passes to
   *  it with no gap in which another store's discard could take it, but only while the device's latest reconcile still
   *  read the entry Rebuilding under `episode`: otherwise the slot is freed and the device checked again. A failure
   *  frees the slot, leaves everything in place, and schedules the device's backoff re-check as a failed pass does.
   */
  private def discardLostStore(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID],
                               held: StoreRebuilds.HeldRebuild): Boolean =
    // Only a Lost copy is discarded: retireForeignCopy, before it, moved any other copy at the path to lost/
    if !registry.copiesOf(local).flatMap(_.copies.get(storeId)).exists(_.phase == Phase.Lost) then
      true
    else
      val deviceId = local.storageDeviceId
      // Host-wide by store id: one discard of the store at a time
      if !registry.rebuilding(Set(RebuildStage.DiscardingLost)).contains(storeId) &&
         setWork(storeId, deviceId, Some(Work.Rebuild(episode, RebuildStage.DiscardingLost))) then
        ctx.copies.retire(local, storeId).foreach: clear =>
          if clear then
            // The retire may outlast the entry it began for (a slow CRL read or drop during a re-placement): the
            // rebuild starts only for an entry the device's latest reconcile still read as Rebuilding under this
            // episode. Otherwise the slot is released and the device checked again, which acts on the entry as it is
            local.reconciledEntries.flatMap(_.get(storeId)).filter(entry =>
              entry.status == StorageDeviceState.StoreStatus.Rebuilding && entry.rebuildEpisode == episode
            ) match
              case Some(entry) =>
                // The discard's slot passes straight to the rebuild (DiscardingLost -> Running); a rebuild that does
                // not start leaves its work Queued, its slot released
                startRebuild(local, storeId, episode, entry.acceptLoss, handoff = true)
                if registry.rebuildWorkOn(deviceId, storeId).exists(_.stage == RebuildStage.DiscardingLost) then
                  requeue(storeId, deviceId)
              case None =>
                requeue(storeId, deviceId)
                logger.info(s"Not rebuilding store $storeId on device $deviceId after discarding its " +
                            "lost copy: its entry there is no longer Rebuilding for this episode")
                ctx.reconciler.checkStorageDevice(deviceId)
          else
            requeue(storeId, deviceId)
            // A failed retire is a failed pass (rebuild-process 5.7): the freed slot re-checks the other devices, and
            // this device is checked after a backoff, not at once (a CRL that keeps failing would spin). That check
            // also starts whatever the discard held back here, a transfer in of the store re-placed away and back
            ctx.devices.devices.keys.filter(_ != deviceId).foreach(ctx.reconciler.checkStorageDevice)
            scheduleRebuildRetry(storeId, held)
      false

object StoreRebuilds:
  /** What a failed rebuild pass or flip ran for, which its backoff holds back (scheduleRebuildRetry): the device, the
   *  Rebuilding episode and whether the entry accepted the loss of unreadable objects */
  private case class HeldRebuild(deviceId: StorageDeviceId, episode: Option[UUID], acceptLoss: Boolean)

  /** A running rebuild's handle, the device and episode it runs for, the promise completed once it has released its
   *  slot, and whether its completion's handler has recorded the copy it placed (`finishRecorded`) */
  private final case class RunningRebuild(sr: StoreRebuild, deviceId: StorageDeviceId, episode: Option[UUID],
                                          settled: Promise[Unit], finishRecorded: Boolean = false)

  /** Host.StoreFailStopped's `what` for a store whose CRL recovery failed */
  val RecoveryFailed = "failed to recover its transactions from its CRL"
  /** Host.StoreFailStopped's `what` for a store that fail-stopped (Frontend.failStop) */
  val FailStopped = "fail-stopped: it cannot hold an update it voted to commit"

  /** The backoff before the re-check that follows a store's `failures + 1`th failed rebuild pass or flip in a row */
  def rebuildRetryDelay(failures: Int): FiniteDuration =
    val doubled = Host.RebuildRetryInitialDelay * (1L << math.min(failures, 20))
    if doubled > Host.RebuildRetryMaxDelay then Host.RebuildRetryMaxDelay else doubled


