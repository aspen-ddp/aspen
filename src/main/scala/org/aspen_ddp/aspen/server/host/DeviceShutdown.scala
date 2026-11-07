package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StoreOffline
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.runSequentially
import org.aspen_ddp.aspen.server.Host.StoreFailedOnClose
import org.aspen_ddp.aspen.server.host.DeviceMetadata.ShutdownOutcome
import org.aspen_ddp.aspen.server.DeviceMarkers
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Shuts a device down for a planned physical move: stops the work running on it, closes its stores, commits Shutdown
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

  // The stores each device's shutdown has closed cleanly in this process and nothing has loaded again since. A
  // cancelled shutdown leaves them offline with nothing on disk to say so, so the reconcile loads them back
  // (reloadClosedStores). Forgotten when the device is unloaded: a restart or an adoption loads them from disk.
  private var closedForShutdown: Map[StorageDeviceId, Set[StoreId]] = Map()

  /** True while the shutdown sequence of `id` is running. */
  def isShuttingDown(id: StorageDeviceId): Boolean = shuttingDownDevices.contains(id)

  /** Takes one store out of service for a device shutdown. This is the default of Host.closeStoreForShutdown, the
   *  seam a test replaces. */
  def closeForShutdown(storeId: StoreId): Future[Unit] = ctx.loader.closeStore(storeId, extractCrl = false)

  /** Shuts down a loaded device whose state reads ShuttingDown (or Shutdown, see below).
   *
   *  Called from reconcileDeviceState in place of its normal passes, so while a device is shutting
   *  down nothing new starts on it: no store is created, no transfer in begins, no rebuild starts.
   *  The sequence, each step after the previous one completes:
   *  1. stopDeviceWork: cancel the rebuilds and transfers running on the device, then
   *     awaitShutdownWork: wait, at most shutdownWorkTimeout, for the cancelled rebuilds and any
   *     transfer-in pool update already running to finish acting on their outcome.
   *  2. closeStoresForShutdown: close every open store and announce it offline.
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
   */
  def shutDownDevice(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val deviceId = local.storageDeviceId
    if ! shuttingDownDevices.contains(deviceId) then
      shuttingDownDevices += deviceId
      logger.info(s"Shutting down storage device $deviceId at ${local.devicePath}")

      def release(): Unit =
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
          // which loads each store this shutdown closed and that is Active again
          // (reloadClosedStores).
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
    val (cancelledIns, stopping) =
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
        .filter(ctx.storeTransferService.isTransferringOut)
        .foreach: storeId =>
          logger.info(s"Cancelling the transfer out of store $storeId for the shutdown of device $deviceId")
          ctx.storeTransferService.cancelTransferOut(storeId)
      ctx.publishView()

      (ins, rebuilds ++ recording)

    cancelledIns.foreach: (storeId, transferUUID) =>
      logger.info(s"Cancelling the transfer in of store $storeId for the shutdown of device ${local.storageDeviceId}")
      ctx.storeTransferService.cancelTransferIn(storeId, transferUUID)

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
      ctx.scheduleShutdownWorkTimeout(deviceId, () =>
        if done.trySuccess(()) then
          val running = work.collect { case (storeId, f) if !f.isCompleted => storeId }
            .toList.sortBy(s => (s.poolId.uuid, s.poolIndex))
          logger.warn(s"Work on storage device $deviceId did not stop within ${ctx.shutdownWorkTimeout} of " +
                      s"its shutdown starting: stores ${running.mkString(", ")}. Shutting it down anyway"))
      done.future

  /** Step 2 of shutDownDevice, also run once more after Shutdown commits: closes each store open
   *  on the device, leaving its CRL state live in the device CRL, and gossips StoreOffline.
   *
   *  Nothing is written into the store's directory: the device CRL already on the drive holds its
   *  state, and the device as a whole is not loaded again while it reads Shutdown. A restart or an
   *  adoption loads the store from the plain directory and recovers its transactions from the
   *  device CRL. A cancelled shutdown's stores are loaded again by the reconcile
   *  (reloadClosedStores), which is why the stores closed cleanly are remembered.
   *
   *  Picked: every store loaded on the device that is not offline (storesToClose). One at a time,
   *  so each close has finished before the next starts.
   */
  private def closeStoresForShutdown(local: LocalStorageDeviceState): Future[Unit] =
    val deviceId = local.storageDeviceId
    runSequentially(storesToClose(local)): storeId =>
      // A store whose backend failed while closing is marked rebuild-required instead of closed
      // cleanly (see StoreRebuilds.failedOnClose), and an adopter takes it as Rebuilding.
      ctx.closeStoreForShutdown(storeId).transform:
        case Failure(_: StoreFailedOnClose) => Success(false)
        case other => other.map(_ => true)
      .map: clean =>
        // ShutdownStore records this too. Done here as well so the bookkeeping that later
        // steps read does not depend on how the store was closed.
        ctx.router.markOffline(storeId)
        local.offlineStores += storeId
        if clean then
          closedForShutdown += deviceId -> (closedForShutdown.getOrElse(deviceId, Set()) + storeId)
        ctx.services.gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))
    .map(_ => ())

  /** The stores closeStoresForShutdown picks: those loaded on the device and not offline. A crash
   *  mid-shutdown reloads the device, ShuttingDown, with its stores served, and they are closed again. */
  private def storesToClose(local: LocalStorageDeviceState): List[StoreId] =
    local.loadedStores.filter(storeId => !ctx.router.isOffline(storeId))
      .toList.sortBy(s => (s.poolId.uuid, s.poolIndex))

  /** Run by the reconcile of an Operational device that is not shutting down: loads each store a
   *  cancelled shutdown of it closed, once its entry is Active with no transfer named. A store whose
   *  directory has since gained a marker (rebuild-required, a transfer's) is left to the pass that
   *  owns that marker. One that is no longer offline here was loaded by something else. Each is
   *  offered to tryLoadStore once; a load that fails leaves it offline until a restart loads it. */
  def reloadClosedStores(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val deviceId = local.storageDeviceId
    closedForShutdown.get(deviceId).foreach: closed =>
      def storePath(storeId: StoreId): os.Path = local.devicePath / storeId.directoryName
      val (ready, notYet) = closed.filter(local.offlineStores.contains).partition: storeId =>
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.Active && entry.transferDevice.isEmpty)
      if notYet.isEmpty then
        closedForShutdown -= deviceId
      else
        closedForShutdown += deviceId -> notYet
      val plain = ready.filter: storeId =>
        List(RebuildingStore.RebuildRequiredMarkerFile, StoreTransfer.TransferOutMarkerFile,
             StoreTransfer.TransferredInMarkerFile).forall(marker => !os.exists(storePath(storeId) / marker))
      plain.toList.sortBy(s => (s.poolId.uuid, s.poolIndex)).foreach: storeId =>
        logger.info(s"Loading store $storeId on device $deviceId again: the shutdown that closed it was cancelled")
        ctx.tryLoadStore(local, storePath(storeId), Some(() => ctx.gossipStoreOnline(storeId)))

  /** Unloads a device whose Shutdown has committed, first closing any store loaded on it since
   *  the shutdown's own close: a rebuild or transfer in that flipped its entry Active before
   *  completeDeviceShutdown flipped it back may have loaded its store meanwhile. Unloaded with
   *  such a store open, nothing would ever close it.
   *
   *  The emptiness check and the unload run in one control-thread task, as does the
   *  LoadStoreById handler. A load that has not happened by then finds the device gone
   *  and does nothing; one that has is in loadedStores and is closed by another round.
   */
  private def closeThenUnload(local: LocalStorageDeviceState): Future[Unit] =
    if storesToClose(local).isEmpty then
      unloadDevice(local.storageDeviceId)
      Future.unit
    else
      closeStoresForShutdown(local).flatMap(_ => closeThenUnload(local))

  /** Forgets a device that has been shut down. Its stores stay in offlineStores, so messages for
   *  them are dropped rather than answered with UnknownStore, and it stays in this host's
   *  HostState: this host still owns it until another host adopts it.
   *
   *  Dropping its loadedDeviceDirs entry means the scan reads the directory again, finds
   *  shutdown.state and leaves the device unloaded until a restart or adoption resolves it.
   */
  def unloadDevice(storageDeviceId: StorageDeviceId): Unit =
    closedForShutdown -= storageDeviceId
    ctx.devices.devices.get(storageDeviceId).foreach(sds => shutDownCrl(sds.devicePath, sds.crl))
    ctx.devices.devices -= storageDeviceId
    ctx.discovery.forgetDevice(storageDeviceId)
    ctx.publishView()

  /** Shuts `crl`, the CRL of the device at `devicePath`, down on DeviceShutdown.CrlShutdownEc, since the shutdown
   *  blocks. Until it ends the path is in closingCrls, so the device is not loaded again over files still open. */
  private[host] def shutDownCrl(devicePath: os.Path, crl: CrashRecoveryLog): Unit =
    val closed = Future(crl.shutdown())(using DeviceShutdown.CrlShutdownEc)
    ctx.devices.closingCrls += devicePath -> closed
    closed.onComplete: result =>
      result.failed.foreach(t => logger.error(s"Failed to shut down the CRL of the device at $devicePath: $t"))
      if ctx.devices.closingCrls.get(devicePath).contains(closed) then
        ctx.devices.closingCrls -= devicePath

object DeviceShutdown:
  /** Where device CRLs are shut down: cached daemon threads named crl-shutdown-N. Not one thread, as a CRL whose
   *  thread has died never completes its shutdown and would hold every later one up behind it. */
  private[aspen] lazy val CrlShutdownEc: ExecutionContext =
    val counter = new AtomicInteger(0)
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool: r =>
      val t = new Thread(r, s"crl-shutdown-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t)
