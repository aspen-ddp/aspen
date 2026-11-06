package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StoreOffline
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.Host.RebuildOutcome
import org.aspen_ddp.aspen.server.{DurableFiles, Host, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild}
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.{Backend, FsifConfig, RocksDBConfig}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/** Creates the stores of new pool slots and rebuilds the ones that must be reconstructed from the rest of their
 *  pool, and retires stores that must never serve again (a lost CRL save file, a failed backend, a failed close).
 *
 *  Everything here runs on the host's event thread under the host's lock, except the completions of the rebuilds
 *  and metadata transactions, which take the lock as they need it. The shared rebuild and creation bookkeeping
 *  lives here: `rebuildingStores` is host-wide, so a slot released by one device's rebuild can unblock another's. */
class StoreRebuilds(ctx: DeviceControl) extends Logging:
  import ctx.given

  private var creatingStores: Set[StoreId] = Set()

  private var rebuildingStores: Map[StoreId, StoreRebuild] = Map()
  // Completed when the rebuild of each store in rebuildingStores has released its slot, its
  // outcome acted on. What a shutdown waits on for the rebuilds it cancels.
  private var rebuildsSettled: Map[StoreId, Promise[Unit]] = Map()

  /** Stores whose requestRebuild transaction is in flight, so a reconcile meanwhile does
   *  not start a second one. */
  private var pendingRebuildRequests: Set[StoreId] = Set()

  def isRebuilding(storeId: StoreId): Boolean = ctx.lock.synchronized(rebuildingStores.contains(storeId))

  /** The stores whose rebuild is running here, for any device. */
  def rebuildingStoreIds: Set[StoreId] = ctx.lock.synchronized(rebuildingStores.keySet)

  def isCreating(storeId: StoreId): Boolean = ctx.lock.synchronized(creatingStores.contains(storeId))

  /** Cancels the rebuilds of the given stores of `deviceId` for its shutdown, returning, by store, the future of
   *  each one's slot release. A rebuild already past its walk finishes instead and writes its entry Offline. Caller
   *  holds the instance lock. */
  def cancelFor(deviceId: StorageDeviceId, rebuilding: Set[StoreId]): Map[StoreId, Future[Unit]] =
    rebuilding.toList.flatMap: storeId =>
      rebuildingStores.get(storeId).map: sr =>
        logger.info(s"Cancelling the rebuild of store $storeId for the shutdown of device $deviceId")
        sr.cancel()
        storeId -> rebuildsSettled.get(storeId).map(_.future).getOrElse(Future.unit)
    .toMap

  def handleBackendFailed(e: Host.BackendFailed): Unit =
    backendFailed(e.storeId, Option(e.backend.get), e.err)

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
   *  loadedStores record share one hold of the lock, so a shutdown either sees the store or the
   *  create sees the shutdown.
   *
   *  Only a collision is retried. Any other failure -- the pool cannot be read, the directory
   *  cannot be written -- is logged and releases creatingStores, so the next check tries again
   *  rather than a silent retry loop running for the life of the process.
   */
  def createNewStore(local: LocalStorageDeviceState, storeId: StoreId): Unit = ctx.lock.synchronized {
    val storePath = local.devicePath / storeId.directoryName
    val deviceId = local.storageDeviceId

    if ! creatingStores.contains(storeId) && ! ctx.host.shuttingDownDevices.contains(deviceId) then
      creatingStores += storeId

      // True when the store exists on disk (created here, or by an attempt that crashed before
      // its flip), false when the create was skipped for a shutdown.
      val fcreate: Future[Boolean] = if os.exists(storePath) then
        Future.successful(true)
      else
        ctx.client.getStoragePoolState(storeId.poolId).flatMap: pstate =>
          ctx.lock.synchronized {
            if ctx.host.shuttingDownDevices.contains(deviceId) || ! ctx.devices.devices.get(deviceId).contains(local) then
              logger.info(s"Not creating store $storeId: device $deviceId is being shut down")
              Future.successful(false)
            else
              val backendCfg = pstate.backendConfig match
                case cfg: RocksDBConfig => StoreConfig.RocksDB(cfg.maxSliceSize)
                case cfg: FsifConfig => StoreConfig.Fsif(cfg.segmentPayloadSize, cfg.segmentCount)
              val storeConfig = StoreConfig(storeId, backendCfg, pstate.maxSliceSize)

              os.makeDir.all(storePath)
              os.write.over(storePath / StoreConfig.configFilename, storeConfig.yamlConfig)

              if ctx.loader.isFsif(storeConfig) then
                // Initialized on fsifOpenEc; startFsifOpen records the store in loadedStores now, so a
                // shutdown of this device waits for the create and then closes the store. Out of space is
                // handed to the load retries (fsifOpened).
                val created = Promise[Unit]()
                ctx.loader.startFsifOpen(local, storeConfig, storePath, create = true, Some(created))
                created.future.map(_ => true)
              else
                val backend = ctx.loader.openBackend(storeConfig, storePath, create = true)
                // Recorded as tryLoadStore records a load, so a shutdown of this device finds the
                // store and closes it.
                local.loadedStores += storeId
                ctx.loadStore(deviceId, backend, storeConfig.effectiveMaxSliceSize.map(_.toLong)).map(_ => true)
          }

      val fflip = ctx.client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
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

              val reqs = List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision))
              val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

              logger.info(s"Updating device state to mark store $storeId as $status")
              tx.update(ptr, None, None, reqs, ops)

              tx.result.foreach: _ =>
                logger.info(s"Successfully updated device state to mark store $storeId as $status")

      fflip.onComplete: result =>
        result.failed.foreach: t =>
          logger.error(s"Failed to create store $storeId on device $deviceId. The next check of the " +
                       s"device will try again. Error: $t")
        ctx.lock.synchronized:
          creatingStores -= storeId
  }

  /** Begins reconstruction of one store from the rest of its pool.
   *
   *  Runs under the instance lock, so it does no more than construct the rebuild and register
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
   *  hour) with data under-replicated. The three outcomes that settle the store's metadata
   *  (`Adopt`, `Discard`, and a permanently failed flip) additionally re-check this device, to
   *  start the next store queued behind the slot just freed. The walk-failure path does not: see
   *  its comment.
   */
  def startRebuild(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    ctx.lock.synchronized {
      if !rebuildingStores.contains(storeId) && rebuildingStores.size < ctx.maxConcurrentRebuilds &&
         !ctx.host.shuttingDownDevices.contains(local.storageDeviceId) && discardLostStore(local, storeId) then
        val sr = ctx.storeRebuildFactory.createRebuild(
          ctx.client, storeId, local.storageDeviceId, local.devicePath)

        val settled = Promise[Unit]()

        def release(): Unit = ctx.lock.synchronized:
          rebuildingStores -= storeId
          if rebuildsSettled.get(storeId).exists(_ eq settled) then
            rebuildsSettled -= storeId
          settled.trySuccess(())

        def recheckAll(): Unit =
          ctx.lock.synchronized(ctx.devices.devices.keys).foreach(ctx.host.checkStorageDevice)

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
          ctx.lock.synchronized(ctx.devices.devices.keys)
            .filter(_ != local.storageDeviceId)
            .foreach(ctx.host.checkStorageDevice)

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

        ctx.lock.synchronized:
          rebuildingStores += storeId -> sr
          rebuildsSettled += storeId -> settled

        // The pool already names this device, so peers would otherwise pick a store that cannot
        // answer until the rebuild completes
        ctx.host.gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))

        completeFuture.onComplete:
          case Success(_) =>
            // Loading is downstream of the metadata decision, not ahead of it. An unconditional
            // load hands a live copy to a device that markRebuiltStoreActive may be about to
            // decide must not own it (because the device was tombstoned mid-rebuild), and nothing
            // would ever take it back.
            ctx.metadata.markRebuiltStoreActive(local.storageDeviceId, storeId, ctx.host.gossipStoreOnlineSeam).onComplete:
              case Success(RebuildOutcome.Adopt) =>
                try
                  ctx.host.loadStoreById(local.storageDeviceId, storeId)
                finally
                  release()
                  recheckAll()

              case Success(RebuildOutcome.AdoptOffline) =>
                try
                  keepRebuiltStoreOffline(local, storeId)
                finally
                  release()
                  recheckAll()

              case Success(RebuildOutcome.Discard) =>
                try
                  ctx.devices.discardStoreDirectory(storeId, local.storageDeviceId,
                    "The tree does not say this device owns a rebuilt store")
                finally
                  release()
                  recheckAll()

              case Success(RebuildOutcome.NotOwned) =>
                // The device is another host's now; the copy is left for it. Only the other
                // devices are worth re-checking: this one is not ours to reconcile.
                try
                  logger.info(s"Rebuild of store $storeId finished after device " +
                              s"${local.storageDeviceId} moved to another host. Leaving it to that host")
                finally
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
                               s"loaded, and the device entry still says Rebuilding", t)
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
            // resumes rather than restarting. That next check is deliberately left to the
            // ordinary periodic sweep: this device is excluded from the re-check here because
            // including it is a retry, and an unbounded one. A rebuild that fails in well under a
            // millisecond -- an unreadable checkpoint, a pool that cannot be read -- would spin
            // through tree reads and WARN lines forever, with no backoff and no give-up. The
            // other devices are re-checked because the slot just freed is host-wide and may be
            // what a queued rebuild elsewhere is waiting on.
            try
              logger.warn(s"Rebuild of store $storeId failed: $err")
            finally
              release()
              recheckOtherDevices()
    }

  /** Keeps a rebuilt store whose entry was written Offline (RebuildOutcome.AdoptOffline) out of
   *  service: it is not loaded, and it is recorded offline behind an empty transfer-out marker
   *  plus an empty CRL save file. That is exactly how the shutdown leaves the stores it closed, so
   *  a restart, or an adoption's check that every Offline store was closed cleanly, treats it
   *  the same way. Empty because a store fresh from a rebuild has no transaction state.
   *
   *  Written even when the device has already been unloaded (its shutdown finished first): the
   *  device path is still known, and without the pair an adopting host would refuse the whole
   *  device for a store that is in fact complete. A disk already pulled makes the writes fail,
   *  which is logged; an adopter then refuses the device until it is returned and shut down
   *  again, the same as any other store whose close did not finish.
   */
  private def keepRebuiltStoreOffline(local: LocalStorageDeviceState, storeId: StoreId): Unit = ctx.lock.synchronized {
    ctx.router.markOffline(storeId)
    if ctx.devices.devices.get(local.storageDeviceId).contains(local) then
      local.offlineStores += storeId
    val storePath = local.devicePath / storeId.directoryName
    try
      DurableFiles.write(storePath / StoreTransfer.TransferOutMarkerFile, "")
      if ! os.exists(Backend.crlSaveFile(storePath)) then
        CrashRecoveryLog.saveStoreState(storeId, Nil, Backend.crlSaveFile(storePath))
    catch
      case t: Throwable =>
        // Offline for the life of this process either way. A restart cancelling the shutdown
        // loses the abandoned-transfer-out pass's reinstatement without the marker, so the store
        // waits for this host's next start to load; an adopter refuses the device.
        logger.error(s"Failed to mark rebuilt store $storeId offline at $storePath: $t")
  }

  /** Writes the marker that keeps the store at `storePath` from ever being loaded again: its CRL
   *  save file is unusable and was the only copy of its transaction state. */
  def markRebuildRequired(storePath: os.Path): Unit =
    DurableFiles.write(storePath / RebuildingStore.RebuildRequiredMarkerFile, "")

  /** The LoadStore handler found `backend`'s CRL save file unusable. The file sits in the store's
   *  own directory, so it is the only copy of that state: the store is permanently failed. It
   *  is closed unserved, marked, and left offline; the reconcile then flips its entry to
   *  Rebuilding, and the rebuild discards the directory.
   *
   *  A store arriving over the network never reaches here with a bad file: its staged copy is
   *  checked before promotion, while the source still holds the state.
   *
   *  Caller holds the instance lock.
   */
  def crlSaveFileLost(storageDeviceId: StorageDeviceId,
                      backend: Backend,
                      err: Throwable): Unit =
    logger.error(s"Store ${backend.storeId} cannot be loaded: its CRL save file is unusable and its transaction " +
                 s"state is lost. Rebuilding it. Error: $err")
    retireUnserved(storageDeviceId, backend)

  /** crlSaveFileLost's steps, for any store that must never serve: closes `backend` unserved, marks the store
   *  rebuild-required, leaves it offline, and checks the device so the reconcile flips its entry to Rebuilding.
   *  The close is not awaited. Caller holds the instance lock. */
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
    ctx.host.checkStorageDevice(storageDeviceId)

  /** A store closed by ShutdownStore whose backend failed-stop meanwhile: its page-cache state can't be trusted, so
   *  it is rebuilt rather than treated as cleanly closed. Marked rebuild-required, and the CRL save file the close
   *  just wrote is removed so nothing takes the directory for a clean close; it is dropped from loadedStores so a
   *  shutdown retry does not pick it again. Caller holds the instance lock. */
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
    ctx.host.checkStorageDevice(store.storageDeviceId)

  /** An FSIF backend failed-stop: page-cache state can no longer be trusted, so the store is rebuilt. If it is
   *  served, it is unloaded as UnloadFailedStore does, its CRL state dropped (the rebuild discards the directory),
   *  marked rebuild-required and left offline, and its device checked so the reconcile flips its entry to
   *  Rebuilding. The mark is written before the close, so a crash meanwhile still never serves it. Nothing is
   *  awaited: the close completes in the background. A backend not (or no longer) served is ignored: either its
   *  LoadStore is still queued, and retires it on seeing it failed, or it was closed and replaced.
   *
   *  Caller holds the instance lock. */
  private def backendFailed(storeId: StoreId, backend: Option[Backend], err: Throwable): Unit =
    ctx.router.loaded.get(storeId) match
      case Some(store) if backend.exists(_ eq store.backend) =>
        logger.error(s"Store $storeId failed on an I/O error and can no longer be trusted. Taking it offline and " +
                     s"rebuilding it. Error: $err")
        ctx.router.remove(storeId)
        store.objectCache.clear()
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
        ctx.crl.closeStore(storeId).onComplete: _ =>
          store.close()
        ctx.host.checkStorageDevice(store.storageDeviceId)
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
    ctx.lock.synchronized:
      if !pendingRebuildRequests.contains(storeId) then
        pendingRebuildRequests += storeId
        val f =
          try
            ctx.client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
              for
                ptr <- ctx.client.getStorageDevicePointer(storageDeviceId)
                kvos <- ctx.client.read(ptr)
                state = StorageDeviceState(kvos)
              yield
                val active = state.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active)
                if !state.isFailed && state.hostId == ctx.hostId && active then
                  val newState = state.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Rebuilding, None)
                  tx.update(ptr, None, None,
                    List(KeyRevision(StorageDeviceState.StateKey,
                      kvos.contents(StorageDeviceState.StateKey).revision)),
                    List(Insert(StorageDeviceState.StateKey, newState.encode())))
                  true
                else
                  // No update staged, as in markRebuiltStoreActive.
                  false
          catch case t: Throwable => Future.failed(t)

        f.onComplete: result =>
          ctx.lock.synchronized(pendingRebuildRequests -= storeId)
          result match
            case Success(true) =>
              logger.info(s"Store $storeId on device $storageDeviceId is now Rebuilding")
              ctx.host.checkStorageDevice(storageDeviceId)
            case Success(false) => ()
            case Failure(t) =>
              logger.warn(s"Failed to mark store $storeId on device $storageDeviceId for rebuild. " +
                          s"Will retry on a later check. Error: $t")

  /** Removes a store directory whose CRL state was lost, before its rebuild starts: a rebuild that
   *  finds a directory at the store's path takes it for a store already in place. Renamed out of
   *  the way first, which is atomic and forced to disk, so a crash during the delete never leaves
   *  a partial directory at the store's path. Returns whether the path is now clear.
   *
   *  Caller holds the instance lock.
   */
  private def discardLostStore(local: LocalStorageDeviceState, storeId: StoreId): Boolean =
    val storePath = local.devicePath / storeId.directoryName
    if !os.exists(storePath / RebuildingStore.RebuildRequiredMarkerFile) then
      true
    else
      try
        val discardDir = local.devicePath / RebuildingStore.DiscardDirectory
        os.makeDir.all(discardDir)
        val target = discardDir / s"${storeId.directoryName}-${UUID.randomUUID()}"
        logger.warn(s"Discarding store $storeId at $storePath, whose CRL state was lost, before rebuilding it")
        os.move(storePath, target)
        DurableFiles.syncDirectory(local.devicePath)
        try os.remove.all(target)
        catch case t: Throwable => logger.warn(s"Failed to delete discarded store copy $target: $t")
        true
      catch
        case t: Throwable =>
          logger.error(s"Failed to discard store $storeId at $storePath. Not rebuilding it yet. Error: $t")
          false
