package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StoreOffline
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.runSequentially
import org.aspen_ddp.aspen.server.Host.{ShutdownOutcome, StoreFailedOnClose}
import org.aspen_ddp.aspen.server.{DeviceMarkers, DurableFiles}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/** Shuts a device down for a planned physical move: stops the work running on it, closes its stores, commits Shutdown
 *  and unloads it. Also owns the guard that keeps one shutdown sequence per device at a time.
 *
 *  Metadata goes through `ctx.metadata`; the close of each store, the work timeout and the reconcile of a device
 *  go through the overridable seams on Host (via `ctx`). */
class DeviceShutdown(ctx: DeviceControl) extends Logging:
  import ctx.given

  // Devices whose shutDownDevice sequence is running. One at a time per device: every check of a
  // ShuttingDown device calls shutDownDevice, and the sequence spans several asynchronous steps,
  // so without this a nudge or the hourly check arriving mid-shutdown would close stores, write
  // markers and transact the device state a second time in parallel with the first.
  private var shuttingDownDevices: Set[StorageDeviceId] = Set()

  /** True while the shutdown sequence of `id` is running. */
  def isShuttingDown(id: StorageDeviceId): Boolean = ctx.lock.synchronized(shuttingDownDevices.contains(id))

  /** Takes one store out of service for a device shutdown. This is the default of Host.closeStoreForShutdown, the
   *  seam a test replaces. */
  def closeForShutdown(storeId: StoreId): Future[Unit] = ctx.loader.closeStore(storeId)

  /** Shuts down a loaded device whose state reads ShuttingDown (or Shutdown, see below).
   *
   *  Called from reconcileDeviceState in place of its normal passes, so while a device is shutting
   *  down nothing new starts on it: no store is created, no transfer in begins, no rebuild starts.
   *  The sequence, each step after the previous one completes:
   *  1. stopDeviceWork: cancel the rebuilds and transfers running on the device, then
   *     awaitShutdownWork: wait, at most shutdownWorkTimeout, for the cancelled rebuilds and any
   *     transfer-in pool update already running to finish acting on their outcome.
   *  2. closeStoresForShutdown: close every open store, mark it and announce it offline.
   *  3. Write shutdown.state naming this host, so the device is not loaded again on its own.
   *  4. completeDeviceShutdown, then act on its outcome.
   *
   *  Every step is idempotent, and that is what makes the whole safe to repeat. A crash anywhere
   *  leaves the device ShuttingDown with this host's owning-host file: the restart loads it (the
   *  load table's ShuttingDown row) and the first check runs this again, which skips whatever
   *  was already done. A failure releases the guard and leaves the device to the next check.
   *
   *  A loaded device reading Shutdown gets the same treatment. That is a shutdown whose commit
   *  landed but whose unload did not happen in this process, and finishing it is the same work:
   *  completeDeviceShutdown reports Completed without writing.
   *
   *  Caller holds the instance lock; the continuations take it as they need it.
   */
  def shutDownDevice(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val deviceId = local.storageDeviceId
    if ! shuttingDownDevices.contains(deviceId) then
      shuttingDownDevices += deviceId
      logger.info(s"Shutting down storage device $deviceId at ${local.devicePath}")

      def release(): Unit = ctx.lock.synchronized:
        shuttingDownDevices -= deviceId

      // A throw before the first Future exists would otherwise skip the onComplete below, and
      // with it the guard's release: every later check of this device would then do nothing.
      val fshutdown =
        try
          val stopping = stopDeviceWork(local, remote)
          for
            _ <- awaitShutdownWork(deviceId, stopping)
            _ <- closeStoresForShutdown(local)
            _ = DeviceMarkers.writeShutdownState(local.devicePath, ctx.hostId)
            outcome <- ctx.metadata.completeDeviceShutdown(deviceId)
            _ <-
              if outcome == ShutdownOutcome.Completed then closeThenUnload(local)
              else
                Future.unit
          yield outcome
        catch case t: Throwable => Future.failed(t)

      fshutdown.onComplete:
        case Success(ShutdownOutcome.Completed) =>
          release()
          logger.info(s"Storage device $deviceId is shut down and unloaded. It is safe to remove")

        case Success(ShutdownOutcome.StoresReactivated) =>
          // The newly Offline entries' stores may be open. Going round again closes them; the
          // guard is released first so that check's reconcile can start the sequence.
          logger.info(s"Stores on storage device $deviceId became Active during its shutdown. " +
                      s"Taking them offline and trying again")
          release()
          ctx.reconciler.checkStorageDevice(deviceId)

        case Success(ShutdownOutcome.Cancelled) =>
          // A restart arrived mid-shutdown. Removing shutdown.state lets the device load again
          // after a restart of this host. The check that follows runs the normal reconcile,
          // whose abandoned-transfer-out pass finds each store this shutdown closed -- Active
          // again, offline here, and carrying the marker -- and loads it.
          logger.info(s"Shutdown of storage device $deviceId was cancelled by a restart")
          try
            if os.exists(local.devicePath / DeviceMarkers.ShutdownStateFile) then
              DeviceMarkers.removeShutdownState(local.devicePath)
          catch case t: Throwable =>
            // Harmless while this process runs: the device stays loaded. A restart of this host
            // would find the file and treat the device as shut down until it is resolved.
            logger.error(s"Failed to remove ${DeviceMarkers.ShutdownStateFile} from " +
                         s"${local.devicePath}: $t")
          release()
          ctx.reconciler.checkStorageDevice(deviceId)

        case Failure(t) =>
          release()
          logger.warn(s"Shutdown of storage device $deviceId failed. The next check of the device " +
                      s"will try again. Error: $t")

  /** Step 1 of shutDownDevice: stops everything running on the device, returning, by store, what
   *  awaitShutdownWork must wait for: each cancelled rebuild's slot release, and each transfer in
   *  whose pool update is already running. Work that finishes regardless -- a rebuild already past
   *  its last read, a transfer in already in its pool update, a store creation already loading --
   *  writes its entry Offline rather than Active, because each of those transactions reads the
   *  device state and writes serviceStatus.
   *
   *  The device entries are left exactly as they are. The interrupted work is resumed after a
   *  restart or adoption by the same crash-recovery paths that resume it after a crash: a
   *  rebuild from its checkpoint in staging, a transfer in by the reconcile's TransferringIn
   *  pass, a transfer out by the receiver's retransmitted StartStoreTransfer.
   */
  private def stopDeviceWork(local: LocalStorageDeviceState,
                             remote: StorageDeviceState): Map[StoreId, Future[Unit]] =
    val (cancelledIns, stopping) = ctx.lock.synchronized {
      val deviceId = local.storageDeviceId
      def entriesIn(status: StorageDeviceState.StoreStatus): Set[StoreId] =
        remote.stores.collect { case (storeId, entry) if entry.status == status => storeId }.toSet

      // Cancelled rebuilds fail their complete with RebuildCancelled, which releases their slot.
      // One already past its walk finishes instead, and writes its entry Offline.
      val rebuilds = ctx.rebuilds.cancelFor(deviceId, entriesIn(StorageDeviceState.StoreStatus.Rebuilding))

      // Transfers in: the pool updates already running are waited for, the attempts still shipping are
      // forgotten here and cancelled with the service below. See StoreTransfers.inFlightFor.
      val (ins, recording) =
        ctx.transfers.inFlightFor(deviceId, entriesIn(StorageDeviceState.StoreStatus.TransferringIn))

      // The store is already closed and marked by its transfer. Cancelling the send leaves it so.
      entriesIn(StorageDeviceState.StoreStatus.TransferringOut)
        .filter(ctx.host.storeTransferService.isTransferringOut)
        .foreach: storeId =>
          logger.info(s"Cancelling the transfer out of store $storeId for the shutdown of device $deviceId")
          ctx.host.storeTransferService.cancelTransferOut(storeId)

      (ins, rebuilds ++ recording)
    }

    cancelledIns.foreach: (storeId, transferUUID) =>
      logger.info(s"Cancelling the transfer in of store $storeId for the shutdown of device ${local.storageDeviceId}")
      ctx.host.storeTransferService.cancelTransferIn(storeId, transferUUID)

    stopping

  /** The wait at the end of step 1 of shutDownDevice: completes once every future in `work` has
   *  (success or failure alike), or once shutdownWorkTimeout has passed, whichever is first.
   *
   *  Without it the shutdown could close the device's stores and commit Shutdown -- telling the
   *  operator the disk may be pulled -- while a cancelled rebuild still had its staging RocksDB
   *  open on it, or a transfer in was about to load the store it had just recorded. The timeout
   *  bounds what a stuck walk (a read that never returns) can cost: the shutdown then proceeds
   *  as it did before this wait existed, and the warning names the stores left running. They
   *  are left as a crash leaves them; their entries keep the device's state honest either way.
   */
  private def awaitShutdownWork(deviceId: StorageDeviceId, work: Map[StoreId, Future[Unit]]): Future[Unit] =
    if work.isEmpty then
      Future.unit
    else
      val done = Promise[Unit]()
      Future.sequence(work.valuesIterator.map(_.transform(_ => Success(()))).toList)
        .foreach(_ => done.trySuccess(()))
      ctx.host.scheduleShutdownWorkTimeoutSeam(deviceId):
        if done.trySuccess(()) then
          val running = work.collect { case (storeId, f) if !f.isCompleted => storeId }
            .toList.sortBy(s => (s.poolId.uuid, s.poolIndex))
          logger.warn(s"Work on storage device $deviceId did not stop within ${ctx.host.shutdownWorkTimeout} of " +
                      s"its shutdown starting: stores ${running.mkString(", ")}. Shutting it down anyway")
      done.future

  /** Step 2 of shutDownDevice, also run once more after Shutdown commits: closes each store open
   *  on the device, then writes its transfer-out marker (empty, as no transfer owns it) and
   *  gossips StoreOffline.
   *
   *  The marker is what keeps the store from loading on any later start of this device, here or
   *  on an adopting host, until a restart or adoption clears it. It is the same file a transfer
   *  out writes, which lets the reconcile's abandoned-transfer-out pass bring the store back if
   *  the shutdown is cancelled: that pass fires only for an Active entry, so it cannot misfire
   *  against the Offline entries of a device that stays shut down.
   *
   *  Picked: every store loaded on the device that is still open, plus any closed one whose
   *  directory lacks the marker -- an earlier attempt whose marker write failed. Stores closed by
   *  a transfer out carry their own marker and are left alone, as are stores never loaded here
   *  (Rebuilding, TransferringIn), which get no handling: they are left as a crash leaves them.
   *  One at a time, so each store's close has finished before its marker is written.
   */
  private def closeStoresForShutdown(local: LocalStorageDeviceState): Future[Unit] =
    runSequentially(storesToClose(local)): storeId =>
      // A store whose backend failed while closing is marked rebuild-required instead of closed
      // cleanly (see StoreRebuilds.failedOnClose), and an adopter takes it as Rebuilding.
      ctx.host.closeStoreForShutdownSeam(storeId).transform:
        case Failure(_: StoreFailedOnClose) => Success(false)
        case other => other.map(_ => true)
      .map: clean =>
        ctx.lock.synchronized:
          // ShutdownStore records this too. Done here as well so the bookkeeping that later
          // steps read does not depend on how the store was closed.
          ctx.router.markOffline(storeId)
          local.offlineStores += storeId
          // Forced to disk, as the CRL file ShutdownStore saved beside it was: once Shutdown
          // commits the disk may be pulled at any moment, and an adopter refuses a store whose
          // marker or CRL file did not survive.
          if clean then
            DurableFiles.write(local.devicePath / storeId.directoryName / StoreTransfer.TransferOutMarkerFile, "")
          ctx.services.gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))
    .map(_ => ())

  /** The stores closeStoresForShutdown picks. */
  private def storesToClose(local: LocalStorageDeviceState): List[StoreId] = ctx.lock.synchronized {
    local.loadedStores.filter: storeId =>
      val storePath = local.devicePath / storeId.directoryName
      val unmarked = os.isDir(storePath) && !os.exists(storePath / StoreTransfer.TransferOutMarkerFile)
      !ctx.router.isOffline(storeId) || unmarked
    .toList.sortBy(s => (s.poolId.uuid, s.poolIndex))
  }

  /** Unloads a device whose Shutdown has committed, first closing any store loaded on it since
   *  the shutdown's own close: a rebuild or transfer in that flipped its entry Active before
   *  completeDeviceShutdown flipped it back may have loaded its store meanwhile. Unloaded with
   *  such a store open, nothing would ever close it.
   *
   *  The emptiness check and the unload share one hold of the lock, which is the lock the
   *  LoadStoreById handler takes. A load that has not happened by then finds the device gone
   *  and does nothing; one that has is in loadedStores and is closed by another round.
   */
  private def closeThenUnload(local: LocalStorageDeviceState): Future[Unit] = ctx.lock.synchronized {
    if storesToClose(local).isEmpty then
      unloadDevice(local.storageDeviceId)
      Future.unit
    else
      closeStoresForShutdown(local).flatMap(_ => closeThenUnload(local))
  }

  /** Forgets a device that has been shut down. Its stores stay in offlineStores, so messages for
   *  them are dropped rather than answered with UnknownStore, and it stays in this host's
   *  HostState: this host still owns it until another host adopts it.
   *
   *  Dropping its loadedDeviceDirs entry means the scan reads the directory again, finds
   *  shutdown.state and leaves the device unloaded until a restart or adoption resolves it.
   */
  def unloadDevice(storageDeviceId: StorageDeviceId): Unit = ctx.lock.synchronized {
    ctx.devices.devices -= storageDeviceId
    ctx.discovery.forgetDevice(storageDeviceId)
  }
