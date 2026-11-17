package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StoreOffline
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.host.DeviceMetadata.RebuildOutcome
import org.aspen_ddp.aspen.server.{DurableFiles, Host, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild}
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Creates the stores of new pool slots and rebuilds the ones that must be reconstructed from the rest of their
 *  pool, and retires stores that must never serve again (a lost CRL save file, a failed backend, a failed close).
 *
 *  The shared rebuild and creation bookkeeping lives here: `rebuildingStores` is host-wide, so a slot released by one
 *  device's rebuild can unblock another's. */
class StoreRebuilds(ctx: DeviceControl) extends Logging:
  import ctx.given

  private var creatingStores: Set[StoreId] = Set()

  private var rebuildingStores: Map[StoreId, StoreRebuild] = Map()
  // The device each one in rebuildingStores runs on
  private var rebuildDevices: Map[StoreId, StorageDeviceId] = Map()
  // The Rebuilding episode each one in rebuildingStores runs for (StoreEntry.rebuildEpisode)
  private var rebuildEpisodes: Map[StoreId, Option[UUID]] = Map()
  // Rebuilds cancelled because their entry left their device (abandonUnlisted, Rebuild F12)
  private var abandonedRebuilds: Set[StoreId] = Set()
  // Completed when the rebuild of each store in rebuildingStores has released its slot, its
  // outcome acted on. What a shutdown waits on for the rebuilds it cancels.
  private var rebuildsSettled: Map[StoreId, Promise[Unit]] = Map()

  /** Stores whose requestRebuild transaction is in flight, so a reconcile meanwhile does
   *  not start a second one. */
  private var pendingRebuildRequests: Set[StoreId] = Set()
  // Stores whose device-CRL entries are being dropped before their marked directory is discarded
  // (discardLostStore). Each one's rebuild waits for the drop to be durable. Each holds a rebuild slot, as a
  // running rebuild does: the discard is the first destructive step, so maxConcurrentRebuilds bounds discards too
  // (systemic-loss-protection.md S3).
  private var droppingCrlState: Set[StoreId] = Set()

  // Rebuild F5, F11: by store, how many passes or flips have failed in a row, and the stores with a retry scheduled
  private var rebuildFailures: Map[StoreId, Int] = Map()
  private var scheduledRebuildRetries: Set[StoreId] = Set()

  // Rebuild F7: stores this class marked offline in the router for a Rebuilding entry, by device. A store rebuilt on
  // a device that never held it is not loaded, so without the mark this host would answer UnknownStore/StoreNotFound
  // for it, and readers would re-send at once, for the whole rebuild.
  private var silenced: Map[StoreId, StorageDeviceId] = Map()

  /** The stores marked offline for their Rebuilding entry */
  private[server] def silencedStores: Set[StoreId] = silenced.keySet

  /** The stores with a backoff re-check of their device scheduled */
  private[server] def rebuildRetriesScheduled: Set[StoreId] = scheduledRebuildRetries

  /** The stores whose rebuild is running here, for any device. */
  def rebuildingStoreIds: Set[StoreId] = rebuildingStores.keySet

  /** Cancels the rebuilds of the given stores of `deviceId` for its shutdown, returning, by store, the future of
   *  each one's slot release. A rebuild already past its walk finishes instead and writes its entry Offline. */
  def cancelFor(deviceId: StorageDeviceId, rebuilding: Set[StoreId]): Map[StoreId, Future[Unit]] =
    rebuilding.toList.flatMap: storeId =>
      rebuildingStores.get(storeId).map: sr =>
        logger.info(s"Cancelling the rebuild of store $storeId for the shutdown of device $deviceId")
        sr.cancel()
        storeId -> rebuildsSettled.get(storeId).map(_.future).getOrElse(Future.unit)
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
   *  shut down since the create began. The store is loaded either way, and recorded in
   *  loadedStores, so the shutdown closes and marks it like the device's other open stores.
   *
   *  A create that has not started its local work by the time this device's shutdown begins (or
   *  its unload) does none: nothing is created or loaded, and the entry stays Initializing for a
   *  later check once the device is back in service. Writing Offline over a store that does not
   *  exist would have a restart bring an empty slot into service. The decision and the
   *  loadedStores record are made in one control-thread task, so a shutdown either sees the store or the
   *  create sees the shutdown.
   *
   *  Only a collision is retried. Any other failure -- the pool cannot be read, the directory
   *  cannot be written -- is logged and releases creatingStores, so the next check tries again
   *  rather than a silent retry loop running for the life of the process.
   */
  def createNewStore(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    val storePath = local.devicePath / storeId.directoryName
    val deviceId = local.storageDeviceId

    if ! creatingStores.contains(storeId) && ! ctx.shutdown.isShuttingDown(deviceId) then
      creatingStores += storeId

      // True when the store exists on disk (created here, or by an attempt that crashed before
      // its flip), false when the create was skipped for a shutdown.
      val fcreate: Future[Boolean] = if os.exists(storePath) then
        Future.successful(true)
      else
        ctx.client.getStoragePoolState(storeId.poolId).flatMap: pstate =>
          if ctx.shutdown.isShuttingDown(deviceId) || ! ctx.devices.devices.get(deviceId).contains(local) then
            logger.info(s"Not creating store $storeId: device $deviceId is being shut down")
            Future.successful(false)
          else
            val storeConfig = StoreConfig.forPool(storeId, pstate)

            // Created wherever its backend opens, atomically (BackendOpener.create, StoreCreation): the
            // directory, config included, appears at storePath only complete, so the os.exists check above
            // means the store exists. startOpen records the store in loadedStores now, so a shutdown of this
            // device waits for the create and then closes the store. A create that fails, out of space
            // included, leaves nothing at storePath, and the next device check tries again.
            val created = Promise[Unit]()
            ctx.loader.startOpen(local, storeConfig, storePath, create = true, Some(created))
            created.future.map(_ => true)

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
        creatingStores -= storeId

  /** Begins reconstruction of one store from the rest of its pool.
   *
   *  Runs on the control thread, so it does no more than construct the rebuild and register
   *  its continuation; the walk itself runs on the client's execution context.
   *
   *  On success the walk places the rebuilt store at `<device>/<storeId.directoryName>`. Loading
   *  is downstream of the metadata decision: markRebuiltStoreActive decides whether the device is
   *  allowed to adopt the copy (reading the tree state inside a transaction to guard against
   *  concurrent tombstone), then the completion handler loads if adopted or discards if the device
   *  was failed. The slot is released only after the flip commits, so a concurrent check cannot
   *  restart the same store between the release and the commit.
   *
   *  Every completion re-checks the host's other loaded devices, because
   *  `rebuildingStores` is host-wide: a slot released by device A's rebuild could unblock a queued
   *  rebuild on device B, and without a re-check of B it would wait for the periodic sweep (one
   *  hour) with data under-replicated. The outcomes that settle the store's metadata (`Adopt`,
   *  `AdoptOffline`, `Discard`) additionally re-check this device, to start the next store queued
   *  behind the slot just freed. A failed walk and a permanently failed flip do not, since that
   *  re-check would restart the same store at once: they schedule a backoff re-check instead
   *  (scheduleRebuildRetry, Rebuild F5, F11).
   *
   *  A slot is taken from the start of a lost store's discard (`droppingCrlState`), not only once its walk runs, and
   *  passes straight from the discard to the rebuild: otherwise one pass would discard every marked store on the
   *  device at once (systemic-loss-protection.md S3).
   *
   *  `episode` is the Rebuilding entry's episode (StoreEntry.rebuildEpisode). The rebuild resumes only a checkpoint,
   *  and takes only a finished copy, of this episode, and its flip commits only while the entry still names it: a
   *  store re-placed away and back is a new episode, and nothing of the earlier one is resumed or promoted
   *  (rebuild-process.md 5.1).
   */
  def startRebuild(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID]): Unit =
    // Silent from the first reconcile that sees the entry, a store queued for a slot included (Rebuild F7)
    if !silenced.contains(storeId) then
      ctx.router.markOffline(storeId)
      silenced += storeId -> local.storageDeviceId
    if !rebuildingStores.contains(storeId) &&
       rebuildingStores.size + droppingCrlState.size < ctx.maxConcurrentRebuilds &&
       ! ctx.shutdown.isShuttingDown(local.storageDeviceId) && retireForeignCopy(local, storeId, episode) &&
       discardLostStore(local, storeId, episode) then
      val sr = ctx.storeRebuildFactory.createRebuild(
        ctx.client, storeId, local.storageDeviceId, local.devicePath, ctx.backends, episode)

      val settled = Promise[Unit]()

      def release(): Unit =
        rebuildingStores -= storeId
        rebuildDevices -= storeId
        rebuildEpisodes -= storeId
        ctx.publishView()
        if rebuildsSettled.get(storeId).exists(_ eq settled) then
          rebuildsSettled -= storeId
        settled.trySuccess(())

      def recheckAll(): Unit =
        ctx.devices.devices.keys.foreach(ctx.reconciler.checkStorageDevice)

      /** Every loaded device except the one this rebuild ran on.
       *
       *  For the failure path only. The freed slot is host-wide, so the other devices are still
       *  worth re-checking, but re-checking this one restarts the rebuild that just failed:
       *  `release()` has already dropped the store from `rebuildingStores`, the tree entry still
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

      // The store's metadata is settled: a later failure starts the backoff afresh
      def settledOutcome(): Unit =
        rebuildFailures -= storeId

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

      rebuildingStores += storeId -> sr
      rebuildDevices += storeId -> local.storageDeviceId
      rebuildEpisodes += storeId -> episode
      rebuildsSettled += storeId -> settled
      ctx.publishView()

      // The pool already names this device, so peers would otherwise pick a store that cannot
      // answer until the rebuild completes
      ctx.services.gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))

      completeFuture.onComplete:
        case Success(_) =>
          // Loading is downstream of the metadata decision, not ahead of it. An unconditional
          // load hands a live copy to a device that markRebuiltStoreActive may be about to
          // decide must not own it (because the device was tombstoned mid-rebuild), and nothing
          // would ever take it back.
          ctx.metadata.markRebuiltStoreActive(local.storageDeviceId, storeId, episode, ctx.gossipStoreOnline).onComplete:
            case Success(RebuildOutcome.Adopt) =>
              try
                // Loaded only once the marker is gone, or tryLoadStore leaves it offline; a failed removal is
                // retried by the reconcile, which finds the entry Active
                // The load's install clears the offline mark; a store not loaded stays silent
                unsilence(storeId, clear = false)
                if clearRebuildCompleteMarker(local.devicePath / storeId.directoryName) then
                  ctx.loadStoreById(local.storageDeviceId, storeId)
              finally
                settledOutcome()
                release()
                recheckAll()

            case Success(RebuildOutcome.AdoptOffline) =>
              try
                unsilence(storeId, clear = false)
                keepRebuiltStoreOffline(local, storeId)
              finally
                settledOutcome()
                release()
                recheckAll()

            case Success(RebuildOutcome.Discard) =>
              try
                unsilence(storeId, clear = true)
                ctx.devices.discardStoreDirectory(storeId, local.storageDeviceId,
                  "The tree does not say this device owns a rebuilt store")
              finally
                settledOutcome()
                release()
                recheckAll()

            case Success(RebuildOutcome.Superseded) =>
              // The entry is Rebuilding here under a later episode: the store was re-placed away and back while this
              // rebuild ran. Its copy stays at the store's path, still silent; its rebuild-complete marker names this
              // episode, so the next rebuild retires it (retireForeignCopy) and starts over
              try
                logger.info(s"Rebuild of store $storeId on device ${local.storageDeviceId} finished for an earlier " +
                            "Rebuilding episode. Not promoting it; the store is rebuilt again")
              finally
                settledOutcome()
                release()
                recheckAll()

            case Success(RebuildOutcome.NotOwned) =>
              // The device is another host's now; the copy is left for it. Only the other
              // devices are worth re-checking: this one is not ours to reconcile.
              try
                // Stays silent: a device not loaded here has its stores silenced by the reconcile anyway
                unsilence(storeId, clear = false)
                logger.info(s"Rebuild of store $storeId finished after device " +
                            s"${local.storageDeviceId} moved to another host. Leaving it to that host")
              finally
                settledOutcome()
                release()
                recheckOtherDevices()

            case Failure(t) =>
              // The only observer this Future has. Its onFail routes the permanent errors to
              // StopRetrying, which fails the promise -- and an unobserved failed Promise is
              // not reported by the ExecutionContext, so without this line a permanent give-up
              // is exactly as silent as the infinite retry loop it replaced. What is left
              // behind matters: the store is on disk, the tree still says Rebuilding.
              try
                logger.error(s"Failed to record the completed rebuild of store $storeId on " +
                             s"device ${local.storageDeviceId}. The store is on disk in " +
                             s"${local.devicePath / storeId.directoryName} but is not " +
                             s"loaded, and the device entry still says Rebuilding. Retrying with a backoff", t)
              finally
                release()
                // Not this device at once: its reconcile restarts the rebuild, which flips again (Rebuild F11)
                recheckOtherDevices()
                scheduleRebuildRetry(local.storageDeviceId, storeId)

        case Failure(RebuildingStore.RebuildCancelled(_)) if abandonedRebuilds.contains(storeId) =>
          // Its entry left this device (abandonUnlisted). This device is checked once more: if the entry is
          // still gone, that check deletes the staging copy; if it reads Rebuilding after all, the rebuild resumes
          try
            unsilence(storeId, clear = true)
            logger.info(s"Rebuild of store $storeId on device ${local.storageDeviceId} abandoned: the store is " +
                        "no longer being rebuilt here")
          finally
            abandonedRebuilds -= storeId
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
          // and WARN lines forever. It comes from scheduleRebuildRetry's backoff (Rebuild F5). The
          // other devices are re-checked because the slot just freed is host-wide and may be
          // what a queued rebuild elsewhere is waiting on.
          try
            logger.warn(s"Rebuild of store $storeId failed: $err")
          finally
            release()
            recheckOtherDevices()
            scheduleRebuildRetry(local.storageDeviceId, storeId)

  /** Rebuild F12. Retires the rebuild work on `local` for stores whose entry on it is no longer Rebuilding: re-placed
   *  elsewhere by AspenClient.replaceRebuild, or gone for another reason. A rebuild still running for one, or for an
   *  earlier episode of an entry still Rebuilding here (re-placed away and back since it started), is cancelled; its
   *  completion checks the device again, which starts the current episode. A staging directory for one with no rebuild running is
   *  deleted: nothing will resume it, and a later rebuild of the store here starts over. Called by the reconcile of a
   *  device this host owns and serves. */
  def abandonUnlisted(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    def listed(storeId: StoreId): Boolean =
      remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Rebuilding)

    def current(storeId: StoreId): Boolean =
      remote.stores.get(storeId).exists: entry =>
        entry.status == StorageDeviceState.StoreStatus.Rebuilding &&
        rebuildEpisodes.get(storeId).contains(entry.rebuildEpisode)

    rebuildDevices.foreach: (storeId, deviceId) =>
      if deviceId == local.storageDeviceId && !current(storeId) && !abandonedRebuilds.contains(storeId) then
        rebuildingStores.get(storeId).foreach: sr =>
          logger.info(s"Cancelling the rebuild of store $storeId on device $deviceId: its entry there is no " +
                      "longer Rebuilding for this rebuild's episode")
          abandonedRebuilds += storeId
          sr.cancel()

    // A store queued for a slot whose entry left before its rebuild started (Rebuild F7)
    silenced.foreach: (storeId, deviceId) =>
      if deviceId == local.storageDeviceId && !listed(storeId) && !rebuildingStores.contains(storeId) then
        unsilence(storeId, clear = true)

    val stagingRoot = local.devicePath / RebuildingStore.RebuildDirectory
    val staged =
      try
        if os.isDir(stagingRoot) then os.list(stagingRoot).filter(os.isDir(_)).toList else Nil
      catch
        case t: Throwable =>
          logger.warn(s"Failed to list $stagingRoot: $t")
          Nil
    staged.foreach: dir =>
      scala.util.Try(StoreId(dir.last)).toOption.foreach: storeId =>
        if !listed(storeId) && !rebuildingStores.contains(storeId) then
          logger.info(s"Deleting the staging copy of store $storeId at $dir: it is no longer being rebuilt on " +
                      s"device ${local.storageDeviceId}")
          try os.remove.all(dir)
          catch case t: Throwable => logger.warn(s"Failed to delete $dir. Retrying at a later check. Error: $t")

  /** Forgets the Rebuild F7 offline mark of `storeId`. With `clear`, for a store the tree no longer puts here, the
   *  router mark is removed too, so the host answers UnknownStore for it again, unless a device's offlineStores
   *  holds the store: that mark belongs to another path (a shutdown, transfer or lost CRL). */
  private def unsilence(storeId: StoreId, clear: Boolean): Unit =
    if silenced.contains(storeId) then
      silenced -= storeId
      if clear && !ctx.devices.devices.values.exists(_.offlineStores.contains(storeId)) then
        ctx.router.clearOffline(storeId)

  /** Checks `deviceId` again after a backoff, so a store whose rebuild pass or flip failed is retried without waiting
   *  for the periodic sweep (an hour) and without spinning (Rebuild F5, F11). The first delay is
   *  Host.RebuildRetryInitialDelay, doubled for each failure of the store in a row, up to Host.RebuildRetryMaxDelay;
   *  a settled flip resets it. At most one is scheduled per store. The check is an ordinary reconcile: a device
   *  unloaded meanwhile, or an entry that has left Rebuilding, does nothing. */
  private def scheduleRebuildRetry(deviceId: StorageDeviceId, storeId: StoreId): Unit =
    val failures = rebuildFailures.getOrElse(storeId, 0)
    rebuildFailures += storeId -> (failures + 1)
    if !scheduledRebuildRetries.contains(storeId) then
      scheduledRebuildRetries += storeId
      val delay = StoreRebuilds.rebuildRetryDelay(failures)
      logger.info(s"Retrying the rebuild of store $storeId on device $deviceId in $delay")
      ctx.scheduler.schedule(delay):
        scheduledRebuildRetries -= storeId
        ctx.reconciler.checkStorageDevice(deviceId)

  /** Keeps a rebuilt store whose entry was written Offline (RebuildOutcome.AdoptOffline) out of
   *  service: it is not loaded, and it is recorded offline behind an empty transfer-out marker
   *  plus an empty CRL save file. A restart keeps it offline behind the marker until the
   *  reconcile or bringIntoService finds its entry Active, and an adoption classes it Clean (a
   *  transfer-out marker with a valid save file). Empty because a store fresh from a rebuild has
   *  no transaction state.
   *
   *  Written even when the device has already been unloaded (its shutdown finished first): the
   *  device path is still known, and without the pair an adopting host would refuse the whole
   *  device for a store that is in fact complete. A disk already pulled makes the writes fail,
   *  which is logged; an adopter then refuses the device until it is returned and shut down
   *  again, the same as any other store whose close did not finish.
   */
  private def keepRebuiltStoreOffline(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    ctx.router.markOffline(storeId)
    if ctx.devices.devices.get(local.storageDeviceId).contains(local) then
      local.offlineStores += storeId
    val storePath = local.devicePath / storeId.directoryName
    try
      DurableFiles.write(storePath / StoreTransfer.TransferOutMarkerFile, "")
      if ! os.exists(Backend.crlSaveFile(storePath)) then
        CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState.Empty, Backend.crlSaveFile(storePath))
      // After the pair: the transfer-out marker now keeps it offline, and its reinstatement loads it
      DurableFiles.remove(storePath / RebuildingStore.RebuildCompleteMarkerFile)
    catch
      case t: Throwable =>
        // Offline for the life of this process either way. A restart cancelling the shutdown
        // loses the abandoned-transfer-out pass's reinstatement without the marker, so the store
        // waits for this host's next start to load; an adopter refuses the device.
        logger.error(s"Failed to mark rebuilt store $storeId offline at $storePath: $t")

  /** Removes the rebuild-complete marker of a rebuilt store whose entry has left Rebuilding, durably, so it can be
   *  loaded. Returns whether it is gone; a failure is logged and leaves the store offline for a later reconcile. */
  def clearRebuildCompleteMarker(storePath: os.Path): Boolean =
    try
      DurableFiles.remove(storePath / RebuildingStore.RebuildCompleteMarkerFile)
      true
    catch
      case t: Throwable =>
        logger.error(s"Failed to remove the rebuild-complete marker of the rebuilt store at $storePath. It stays " +
                     s"offline until a later check of its device. Error: $t")
        false

  /** Rebuild F3. A directory at the path of a store being rebuilt here that carries neither the rebuild-complete
   *  marker of this `episode` (a finished rebuild awaiting its flip) nor rebuild-required (a lost copy
   *  discardLostStore clears) was not produced by this rebuild: a leftover of an earlier tenancy of the store on this
   *  device (a transferred-away copy whose delete failed and whose marker was lost, or a copy finished for an earlier
   *  Rebuilding episode, superseded by a re-placement away and back), or a copy put there by hand. Nothing ties it to the store's
   *  current state, so it is retired as a lost copy: marked rebuild-required, and then kept in pending-delete by
   *  discardLostStore like any other. A served one is first taken out of service as a failed backend is, and its
   *  rebuild waits for a later check. Returns whether the rebuild may go on now. */
  private def retireForeignCopy(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID]): Boolean =
    val storePath = local.devicePath / storeId.directoryName
    if !os.exists(storePath) ||
       RebuildingStore.isRebuildCompleteFor(storePath, episode) ||
       os.exists(storePath / RebuildingStore.RebuildRequiredMarkerFile) then
      true
    else if ctx.loader.isOpening(storeId) then
      false
    else
      val err = new IllegalStateException(s"$storePath holds a copy of store $storeId that no rebuild produced, " +
                                          "while the store is Rebuilding on this device")
      ctx.router.entry(storeId) match
        case Some(entry) if entry.store.backend.path == storePath =>
          backendFailed(storeId, Some(entry.store.backend), err, "is served from a copy no rebuild produced")
          false
        case _ =>
          logger.error(s"Retiring a store directory before rebuilding: ${err.getMessage}. It is kept in " +
                       s"pending-delete until the store is Active again")
          try
            markRebuildRequired(storePath)
            true
          catch
            case t: Throwable =>
              logger.error(s"Failed to mark store $storeId for rebuild at $storePath. Not rebuilding it yet: $t")
              false

  /** Writes the marker that keeps the store at `storePath` from ever being loaded again: its CRL
   *  save file is unusable and was the only copy of its transaction state. */
  def markRebuildRequired(storePath: os.Path): Unit = ctx.writeRebuildRequiredMarker(storePath)

  /** The LoadStore handler found `backend`'s CRL save file unusable. The file sits in the store's
   *  own directory, so it is the only copy of that state: the store is permanently failed. It
   *  is closed unserved, marked, and left offline; the reconcile then flips its entry to
   *  Rebuilding, and the rebuild discards the directory.
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

  /** crlSaveFileLost's steps, for any store that must never serve: closes `backend` unserved, marks the store
   *  rebuild-required, leaves it offline, and checks the device so the reconcile flips its entry to Rebuilding.
   *  The close is not awaited. */
  def retireUnserved(storageDeviceId: StorageDeviceId, backend: Backend): Unit =
    val storeId = backend.storeId
    ctx.router.markOffline(storeId)
    ctx.devices.devices.get(storageDeviceId).foreach: sds =>
      sds.loadedStores -= storeId
      sds.offlineStores += storeId
    backend.close()
    try
      markRebuildRequired(backend.path)
    catch
      case t: Throwable =>
        // Offline for the life of this process either way. The next start finds the same
        // unusable state and lands here again.
        logger.error(s"Failed to mark store $storeId for rebuild at ${backend.path}: $t")
    ctx.reconciler.checkStorageDevice(storageDeviceId)

  /** A store closed by ShutdownStore whose backend failed-stop meanwhile: its page-cache state can't be trusted, so
   *  it is rebuilt rather than treated as cleanly closed. Marked rebuild-required, and the CRL save file the close
   *  just wrote is removed so nothing takes the directory for a clean close; it is dropped from loadedStores so a
   *  shutdown retry does not pick it again. */
  def failedOnClose(store: Store): Unit =
    val storeId = store.storeId
    val path = store.backend.path
    logger.error(s"Store $storeId failed on an I/O error while closing. Marking it for rebuild instead of " +
                 s"closing it cleanly. $path")
    ctx.router.markOffline(storeId)
    ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
      sds.loadedStores -= storeId
      sds.offlineStores += storeId
    try
      markRebuildRequired(path)
      DurableFiles.remove(Backend.crlSaveFile(path))
    catch
      case t: Throwable => logger.error(s"Failed to mark store $storeId for rebuild at $path: $t")
    ctx.reconciler.checkStorageDevice(store.storageDeviceId)

  /** A backend failed-stop: its state can no longer be trusted, so the store is rebuilt. If it is
   *  served, it is unloaded, its CRL state left for the rebuild's discard to save and drop,
   *  marked rebuild-required and left offline, and its device checked so the reconcile flips its entry to
   *  Rebuilding. The mark is written before the close, so a crash meanwhile still never serves it. Nothing is
   *  awaited: the close completes in the background. A backend not (or no longer) served is ignored: either its
   *  LoadStore is still queued, and retires it on seeing it failed, or it was closed and replaced. */
  private def backendFailed(storeId: StoreId, backend: Option[Backend], err: Throwable,
                            what: String = "failed on an I/O error"): Unit =
    // Store.backend and Backend.path are immutable, so the control side may read them while the store is served
    ctx.router.entry(storeId) match
      case Some(entry) if backend.exists(_ eq entry.store.backend) =>
        val store = entry.store
        logger.error(s"Store $storeId $what and can no longer be trusted. Taking it offline and " +
                     s"rebuilding it. Error: $err")
        ctx.router.remove(storeId)
        ctx.devices.devices.get(store.storageDeviceId).foreach: sds =>
          sds.loadedStores -= storeId
          sds.offlineStores += storeId
        try
          markRebuildRequired(store.backend.path)
        catch
          case t: Throwable =>
            // Offline for the life of this process either way. A restart recovers whatever is
            // durable, which a failed store may well still hold.
            logger.error(s"Failed to mark store $storeId for rebuild at ${store.backend.path}: $t")
        // Its object cache cleared and close run on its executor; its slot released after. Its CRL entries stay in the
        // device CRL: the rebuild's discard saves them with the copy it keeps, then drops them (discardLostStore)
        ctx.loader.closeUnloaded(entry, dropDurably = false)
        ctx.reconciler.checkStorageDevice(store.storageDeviceId)
      case served =>
        // Not served: its LoadStore is still queued (and retires it), it was replaced by a newer
        // backend, or it was closed. Unless replaced, its directory is marked, so a store closed
        // around the failure is never later served, shipped or adopted as cleanly closed.
        backend match
          case Some(b) if served.isEmpty && !ctx.router.loaded.valuesIterator.exists(_.backend.path == b.path) &&
                          os.isDir(b.path) &&
                          ctx.devices.devices.valuesIterator.exists(_.devicePath == b.path / os.up) =>
            logger.error(s"Store $storeId failed on an I/O error after it stopped being served. Marking it for " +
                         s"rebuild. Error: $err")
            try
              markRebuildRequired(b.path)
            catch
              case t: Throwable => logger.error(s"Failed to mark store $storeId for rebuild at ${b.path}: $t")
          case _ =>
            logger.info(s"Ignoring the failure of a replaced backend of store $storeId. Error: $err")

  /** Flips the entry of a store whose CRL state was lost from Active to Rebuilding, on its own
   *  device. Nothing else changes: the pool already names this device and host. Writes nothing
   *  if the device is failed or owned elsewhere, or the entry is no longer Active. Re-checks the
   *  device afterwards, so the Rebuilding pass starts the rebuild now.
   *
   *  Only a collision is retried, as everywhere else in Host. A permanent failure is logged and
   *  left for a later reconcile, which finds the marker again.
   */
  def requestRebuild(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    if !pendingRebuildRequests.contains(storeId) then
      pendingRebuildRequests += storeId
      val f =
        try
          // The transaction's own machinery (commit, retries) runs on ctx.ec, and its result comes back through
          // the control-context onComplete; the prepare closure's combinators run on the control thread
          // A Rebuilding entry found after an attempt that committed but was reported TransactionOutcomeUnknown is
          // this request's own, so the rebuild is started now rather than at the next periodic check (Todo 5.10).
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

  /** Clears the path of a store whose CRL state was lost, before its rebuild starts: a rebuild refuses to start
   *  over a directory at the store's path that is not a finished rebuild (RebuildingStore.start). Returns whether
   *  the path is clear now, so the rebuild may start.
   *
   *  Nothing is destroyed: the copy is kept under RebuildingStore.PendingDeleteDirectory until the store is Active
   *  again (DeviceReconciler deletes it then), so a rebuild that cannot finish loses nothing. The single point where
   *  a discarded store's entries leave its device CRL, in a fixed order, each step durable before the next: the
   *  `rebuild-required` marker (already there: it is what says the state is gone), then the entries saved into the
   *  directory (RebuildingStore.LostCrlStateFile, kept if a crash left it), then dropped (CrashRecoveryLog.dropStore:
   *  a rebuild onto this device would recover them otherwise), then the directory renamed into pending-delete. A
   *  crash at any point leaves the marker, so the next attempt saves (unless already saved) and drops again. The
   *  store is never served meanwhile, so the entries cannot change. The save and drop complete asynchronously: this
   *  returns false meanwhile, the store holding a rebuild slot (`droppingCrlState`). Once the drop is durable the
   *  directory is moved and the rebuild started at once, in the same callback, so the slot passes to it with no gap
   *  in which another store's discard could take it. A failure frees the slot and leaves everything in place for a
   *  later check.
   */
  private def discardLostStore(local: LocalStorageDeviceState, storeId: StoreId, episode: Option[UUID]): Boolean =
    val storePath = local.devicePath / storeId.directoryName
    if !os.exists(storePath / RebuildingStore.RebuildRequiredMarkerFile) then
      true
    else
      if !droppingCrlState.contains(storeId) then
        droppingCrlState += storeId
        val saveFile = storePath / RebuildingStore.LostCrlStateFile
        val dropped =
          try
            val saved =
              if os.exists(saveFile) then
                Future.unit
              else
                local.crl.readStoreState(storeId).map(trs => CrashRecoveryLog.saveStoreState(storeId, trs, saveFile))
            saved.flatMap(_ => local.crl.dropStore(storeId))
          catch case NonFatal(t) => Future.failed(t)
        dropped.onComplete: result =>
          droppingCrlState -= storeId
          result match
            case Failure(t) =>
              logger.error(s"Failed to save and drop the CRL state of store $storeId before rebuilding it. Not " +
                           s"rebuilding it yet. Error: $t")
            case Success(_) =>
              local.admission.releaseRetained(storeId)
              // A device unloaded meanwhile is discarded by whichever load of it comes next
              if ctx.devices.devices.get(local.storageDeviceId).exists(_ eq local) then
                if moveLostStoreToPendingDelete(local, storeId) then
                  startRebuild(local, storeId, episode)
      false

  /** Renames a discarded store's directory into pending-delete, durably. Returns whether the path is clear. */
  private def moveLostStoreToPendingDelete(local: LocalStorageDeviceState, storeId: StoreId): Boolean =
    val storePath = local.devicePath / storeId.directoryName
    try
      if os.exists(storePath) then
        val pendingDir = local.devicePath / RebuildingStore.PendingDeleteDirectory
        if !os.exists(pendingDir) then
          os.makeDir.all(pendingDir)
          DurableFiles.syncDirectory(local.devicePath)
        val target = pendingDir / s"${storeId.directoryName}-${UUID.randomUUID()}"
        logger.warn(s"Moving store $storeId at $storePath, whose CRL state was lost, to $target before rebuilding " +
                    s"it. It is deleted once the store is Active again")
        os.move(storePath, target)
        DurableFiles.syncDirectory(local.devicePath)
        DurableFiles.syncDirectory(pendingDir)
      true
    catch
      case t: Throwable =>
        logger.error(s"Failed to discard store $storeId at $storePath. Not rebuilding it yet. Error: $t")
        false

object StoreRebuilds:
  /** Host.StoreFailStopped's `what` for a store whose CRL recovery failed */
  val RecoveryFailed = "failed to recover its transactions from its CRL"
  /** Host.StoreFailStopped's `what` for a store that fail-stopped (Frontend.failStop) */
  val FailStopped = "fail-stopped: it cannot hold an update it voted to commit"

  /** The backoff before the re-check that follows a store's `failures + 1`th failed rebuild pass or flip in a row */
  def rebuildRetryDelay(failures: Int): FiniteDuration =
    val doubled = Host.RebuildRetryInitialDelay * (1L << math.min(failures, 20))
    if doubled > Host.RebuildRetryMaxDelay then Host.RebuildRetryMaxDelay else doubled

  /** Writes the rebuild-required marker of the store at `storePath`, durably */
  def writeRebuildRequiredMarker(storePath: os.Path): Unit =
    DurableFiles.write(storePath / RebuildingStore.RebuildRequiredMarkerFile, "")
