package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StoreOffline
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.runSequentially
import org.aspen_ddp.aspen.server.Host.StoreFailedOnClose
import org.aspen_ddp.aspen.server.host.DeviceMetadata.ShutdownOutcome
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
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
  // shutdown.state and transact the device state a second time in parallel with the first.
  private var shuttingDownDevices: Set[StorageDeviceId] = Set()

  // Devices whose relinquishDevice sequence is running. One at a time per device, for the same reason.
  private var relinquishingDevices: Set[StorageDeviceId] = Set()

  /** True while the shutdown sequence of `id` is running. */
  def isShuttingDown(id: StorageDeviceId): Boolean = shuttingDownDevices.contains(id)

  /** True while `id` is being taken out of service by a relinquish, a Failed retirement or a CRL failure
   *  (stopServing, crlFailed): no store opens on it (StoreLoader.startOpen), so the unload, which unloads whatever is
   *  open once the closes it waits for have ended, never leaves a store opened meanwhile served */
  def isStoppingServing(id: StorageDeviceId): Boolean = relinquishingDevices.contains(id)

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
            _ <- writeMarker(local, "shutdown.state write", requireLoaded = true)(
                   DeviceStateDisk.writeShutdownState(local.devicePath, ctx.hostId))
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
          // (CopyDecision.Load).
          logger.info(s"Shutdown of storage device $deviceId was cancelled by a restart")
          // One Markers job; its failure is only logged. Harmless while this process runs: the device stays loaded. A
          // restart of this host would find the file and treat the device as shut down until it is resolved
          writeMarker(local, "shutdown.state removal")(DeviceStateDisk.removeShutdownState(local.devicePath))
            .failed.foreach: t =>
              logger.error(s"Failed to remove ${DeviceStateDisk.ShutdownStateFile} from ${local.devicePath}: $t")
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
      // One already past finish's abort-latch re-check (or awaiting only its flip) completes instead, and writes
      // its entry Offline.
      val rebuilds = ctx.rebuilds.cancelFor(deviceId, entriesIn(StorageDeviceState.StoreStatus.Rebuilding))

      // Transfers in: the pool updates already running are waited for, the attempts still shipping are
      // forgotten here and cancelled with the service below. See StoreTransfers.inFlightFor.
      val (ins, recording) =
        ctx.transfers.inFlightFor(deviceId, entriesIn(StorageDeviceState.StoreStatus.TransferringIn))

      // The store is already closed and in outbound/ for its transfer. Cancelling the send leaves it so.
      entriesIn(StorageDeviceState.StoreStatus.TransferringOut)
        .filter(ctx.storeTransferService.isTransferringOut)
        .foreach: storeId =>
          logger.info(s"Cancelling the transfer out of store $storeId for the shutdown of device $deviceId")
          ctx.storeTransferService.cancelTransferOut(storeId)
      ctx.publishView()

      (ins, rebuilds ++ recording)

    cancelledIns.foreach: (storeId, transferUUID) =>
      logger.info(s"Cancelling the transfer in of store $storeId for the shutdown of device ${local.storageDeviceId}")
      // Its removal is a job on the device's lane; the attempt's failure removes its staging (StoreTransfers)
      ctx.storeTransferService.cancelTransferIn(storeId, transferUUID): Unit

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
   *  device CRL. A cancelled shutdown's stores are loaded again by the reconcile: each is an Idle
   *  Live copy under an Active entry, served nowhere (CopyDecision.Load).
   *
   *  Picked: every store loaded on the device that is not offline (storesToClose). One at a time,
   *  so each close has finished before the next starts.
   */
  private def closeStoresForShutdown(local: LocalStorageDeviceState): Future[Unit] =
    runSequentially(storesToClose(local)): storeId =>
      // A store whose backend failed while closing is moved to lost/ instead of closed
      // cleanly (see StoreRebuilds.failedOnClose), and an adopter takes it as Rebuilding.
      ctx.copyRegistry.closingFor(storeId, ClosePurpose.Shutdown)
      ctx.closeStoreForShutdown(storeId).recover:
        case _: StoreFailedOnClose => ()
      .map: _ =>
        ctx.copyRegistry.closeAskEnded(storeId)
        ctx.services.gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))
    .map(_ => ())

  /** Stops serving a loaded device that the storage-devices tree records as owned by `remote.hostId`, another host:
   *  its drive was pulled from this host and adopted there. Its work is stopped as a shutdown's is, its loaded
   *  stores closed and taken offline, and the device and its CRL unloaded. No marker, shutdown.state or tree entry
   *  is written for it: it is that host's now (closing a backend writes only what a crash would leave, and on a
   *  pulled drive nothing at all). No StoreOffline is gossiped either, as the stores are served there (its
   *  StoreMoved has told peers so). A store whose close fails is left as its failure leaves it.
   *
   *  The directory is disowned first, so a scan meanwhile, or after the unload, does not load it again. One sequence
   *  at a time per device; a failure leaves the device loaded for the next check to try again. */
  def relinquishDevice(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    stopServing(local, remote,
      s"is now owned by host ${remote.hostId}. Unloading it without writing to it",
      s"owned by host ${remote.hostId}")

  /** Stops serving a loaded device that the storage-devices tree records as Failed: an operator declared it dead and
   *  its stores are being rebuilt elsewhere, so a copy served here would be a second replica of each. The durable
   *  `failed` marker (DeviceMarkers) keeps a restart from loading it again; then it is unloaded as relinquishDevice
   *  unloads a device. Nothing else on it is written or deleted: a healthy disk failed by mistake keeps its data. */
  def retireFailedDevice(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    if ! relinquishingDevices.contains(local.storageDeviceId) then
      // One Markers job on the device's lane, queued ahead of everything the unload's closes submit there. The unload
      // does not wait for it, so a hung lane never keeps an operator's Failed serving: unloaded for the life of this
      // process either way, and a restart before the marker is durable loads the device and retires it again
      writeMarker(local, "failed marker write")(DeviceStateDisk.writeFailed(local.devicePath)).failed.foreach: t =>
        logger.error(s"Failed to write the failed marker of storage device ${local.storageDeviceId} at " +
                     s"${local.devicePath}: $t")
      stopServing(local, remote, "has been declared failed. Unloading it", "declared failed")

  /** Writes one of the device's markers (DeviceStateDisk: durable once the job ends) as one `Markers` job on `local`'s
   *  lane: a Future completed on the control thread once its result is applied, failed by the job's failure or a
   *  cancel, and, when `requireLoaded`, by a device no longer the loaded one (a shutdown's step then goes no further:
   *  the device was taken out of service meanwhile). A marker's durability order is the caller's: what must follow
   *  the write waits for this Future */
  private def writeMarker(local: LocalStorageDeviceState, what: String, requireLoaded: Boolean = false)
                         (job: => Unit): Future[Unit] =
    val p = Promise[Unit]()
    try
      ctx.onControl(s"marker $what", local.io.submit(JobKind.Markers, None)(job)): result =>
        if requireLoaded && !ctx.devices.isLoaded(local) then
          p.failure(IllegalStateException(s"Device ${local.storageDeviceId} was unloaded while its $what ran"))
        else
          p.complete(result)
    catch case t: Throwable => p.tryFailure(t)
    p.future

  /** relinquishDevice's sequence, logging that the device `what` */
  private def stopServing(local: LocalStorageDeviceState, remote: StorageDeviceState, what: String,
                          unloadedAs: String): Unit =
    val deviceId = local.storageDeviceId
    if ! relinquishingDevices.contains(deviceId) then
      relinquishingDevices += deviceId
      logger.warn(s"Storage device $deviceId at ${local.devicePath} $what")
      ctx.discovery.disown(local.devicePath)
      val frelinquish =
        try
          val stopping = stopDeviceWork(local, remote)
          for
            _ <- awaitShutdownWork(deviceId, stopping)
            _ <- closeStoresToRelinquish(local)
          yield ()
        catch case t: Throwable => Future.failed(t)
      frelinquish.flatMap(_ => unloadOnceClosed(local)).onComplete: result =>
        relinquishingDevices -= deviceId
        result match
          case Success(true) =>
            logger.info(s"Storage device $deviceId, $unloadedAs, is unloaded")
          case Success(false) => ()
          case Failure(t) =>
            logger.warn(s"Failed to unload storage device $deviceId, $unloadedAs. The next " +
                        s"check of the device will try again. Error: $t")

  /** Takes a loaded device whose CRL has failed (CrashRecoveryLog.failed: its thread or its writer died) out of
   *  service: none of its stores can vote again, and every call on the CRL fails (crash-recovery-log 5.10). The
   *  directory is disowned so no scan loads it again, its stores are closed and gossiped offline, and the device and
   *  its CRL are unloaded. Nothing is written: the files hold what the CRL wrote before it failed, so the host's
   *  restart recovers them as after a crash. Not reloaded before that: a CRL failing again would loop, and repeated
   *  open failures end in a lost CRL (DeviceDiscovery.CrlOpenAttempts). Nor is the device declared Failed in the tree,
   *  which would rebuild every store it holds (rebuild-process 3: nothing declares a device Failed automatically).
   *
   *  Rebuilds and transfers on the device are not cancelled: their CRL calls and store loads now fail, and a restart
   *  resumes them from what they left on disk, as after a crash. */
  def crlFailed(local: LocalStorageDeviceState, cause: Throwable): Unit =
    val deviceId = local.storageDeviceId
    if ctx.devices.isLoaded(local) && !relinquishingDevices.contains(deviceId) then
      relinquishingDevices += deviceId
      logger.error(s"The CRL of storage device $deviceId at ${local.devicePath} has failed. None of its stores can " +
                   s"vote: unloading the device until the host restarts", cause)
      ctx.discovery.disown(local.devicePath)
      val closed =
        try
          closeStoresToRelinquish(local, (storeId: StoreId) =>
            ctx.services.gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now)))
        catch case t: Throwable => Future.failed(t)
      closed.transformWith: result =>
        result.failed.foreach(t => logger.error(s"Failed to close the stores of storage device $deviceId, whose CRL " +
                                                s"has failed. Unloading it anyway: $t"))
        unloadOnceClosed(local)
      .onComplete: result =>
        relinquishingDevices -= deviceId
        if result.toOption.contains(true) then
          logger.info(s"Storage device $deviceId, whose CRL has failed, is unloaded")

  /** Closes every store open on a relinquished device, one at a time, until none is left: one loaded meanwhile is
   *  closed by another round. `onClosed` runs for each once it is offline. */
  private def closeStoresToRelinquish(local: LocalStorageDeviceState,
                                      onClosed: StoreId => Unit = _ => ()): Future[Unit] =
    val open = storesToClose(local)
    if open.isEmpty then
      Future.unit
    else
      runSequentially(open): storeId =>
        ctx.copyRegistry.closingFor(storeId, ClosePurpose.Relinquish)
        ctx.closeStoreForShutdown(storeId).transform(_ => Success(())).map: _ =>
          ctx.copyRegistry.closeAskEnded(storeId)
          onClosed(storeId)
      .flatMap(_ => closeStoresToRelinquish(local, onClosed))

  /** The stores closeStoresForShutdown picks: those whose copy on the device is Served or Opening (not Closing: a
   *  close is under way), a create's open included (its record is registered at its open). The close of one opening
   *  waits for its install, then closes the served store (StoreLoader.handleClose). A crash mid-shutdown reloads the
   *  device, ShuttingDown, with its stores served, and they are closed again. */
  private def storesToClose(local: LocalStorageDeviceState): List[StoreId] =
    ctx.copyRegistry.openCopies(local).toList.sortBy(s => (s.poolId.uuid, s.poolIndex))

  /** Unloads a device whose Shutdown has committed, first closing any store loaded on it since
   *  the shutdown's own close: a rebuild or transfer in that flipped its entry Active before
   *  completeDeviceShutdown flipped it back may have loaded its store meanwhile. Unloaded with
   *  such a store open, nothing would ever close it.
   *
   *  The emptiness check and the unload run in one control-thread task, as does the
   *  LoadStoreById handler. A load that has not happened by then finds the device gone
   *  and does nothing; one that has is Opening or Served there and is closed by another round.
   */
  private def closeThenUnload(local: LocalStorageDeviceState): Future[Unit] =
    if storesToClose(local).isEmpty then
      unloadOnceClosed(local, untilOpen = true).flatMap: _ =>
        // A store loaded while the closes ended is closed by another round
        if ctx.devices.isLoaded(local) then closeThenUnload(local) else Future.unit
    else
      closeStoresForShutdown(local).flatMap(_ => closeThenUnload(local))

  /** Unloads `local` once every close on it whose end may move its copy to lost/ has ended
   *  (StoreRebuilds.closeStarted: a FailStop close, a failed close, an unserved failed backend's), so each such move is
   *  made on the attached device: unloaded first, the copy would stay where it was, and a load or an adoption would
   *  take it for a clean one. The check and the unload run in one control task. False if it was not loaded, or, when
   *  `untilOpen` (a shutdown), a store is open on it again (the caller's next round closes it); a relinquish or a CRL
   *  failure unloads whatever is open, which is only what its closes have not reached: no store opens on a device
   *  being taken out of service (isStoppingServing) */
  private def unloadOnceClosed(local: LocalStorageDeviceState, untilOpen: Boolean = false): Future[Boolean] =
    val deviceId = local.storageDeviceId
    if !ctx.devices.isLoaded(local) || (untilOpen && storesToClose(local).nonEmpty) then
      Future.successful(false)
    else if ctx.rebuilds.closesRunning(deviceId) then
      logger.info(s"Unloading storage device $deviceId once the closes still running on it have ended")
      ctx.rebuilds.closesEnded(deviceId).flatMap(_ => unloadOnceClosed(local, untilOpen))
    else
      unloadDevice(deviceId)
      Future.successful(true)

  /** Forgets a device that has been shut down. Its stores stay silent (the copy registry's unloaded entries), so
   *  messages for them are dropped rather than answered with UnknownStore, and it stays in this host's
   *  HostState: this host still owns it until another host adopts it.
   *
   *  Dropping its loadedDeviceDirs entry means the scan reads the directory again, finds
   *  shutdown.state and leaves the device unloaded until a restart or adoption resolves it.
   */
  def unloadDevice(storageDeviceId: StorageDeviceId): Unit =
    ctx.devices.devices.get(storageDeviceId).foreach: sds =>
      shutDownCrl(sds.devicePath, sds.crl)
      // Its last tree entry's stores, and the copies it holds: an unload before its first reconcile lists those
      ctx.copyRegistry.detach(storageDeviceId,
        sds.reconciledEntries.map(_.keySet).getOrElse(Set()) ++ sds.copies.copies.keySet)
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
   *  write hangs never completes its shutdown and would hold every later one up behind it (a CRL whose thread has
   *  died completes it itself). */
  private[aspen] lazy val CrlShutdownEc: ExecutionContext =
    val counter = new AtomicInteger(0)
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool: r =>
      val t = new Thread(r, s"crl-shutdown-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t)
