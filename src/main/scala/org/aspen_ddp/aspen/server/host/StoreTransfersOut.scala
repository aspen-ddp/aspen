package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import org.aspen_ddp.aspen.common.network.{StartStoreTransfer, StoreOffline}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.StoreFailedOnClose
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The sending end of a store transfer on this host (transfer out): taking the store out of service with its CRL
 *  state extracted, answering the receiver's request, and returning a lost or abandoned transfer to the rebuild or
 *  the loader. The receiving end is StoreTransfers. */
class StoreTransfersOut(ctx: DeviceControl) extends Logging:
  import ctx.given

  // Stores marked rebuild-required whose transfer out returnLostTransferToRebuild is aborting.
  private var abortingLostTransfers: Set[StoreId] = Set()

  /** Takes a store out of service for a transfer out, extracting its CRL state into its crl_save.log to travel with
   *  it: closes it (extractCrl) if it is open or opening, else extracts the state of the closed store from the
   *  device CRL of `sourceDs`, the device the transfer ships from (extractUnloaded). This is the default of
   *  Host.closeStoreForTransferOut, the seam a test replaces. */
  def closeForTransferOut(storeId: StoreId, sourceDs: LocalStorageDeviceState): Future[Unit] =
    if ctx.router.loaded.contains(storeId) || ctx.loader.isOpening(storeId) then
      ctx.loader.closeStore(storeId, extractCrl = true)
    else
      extractUnloaded(storeId, sourceDs)

  /** A closed store's entries may still be live in its device CRL (closed without extraction, or awaiting space).
   *  Without a save file they are read (`crl.readStoreState`, leaving them live) and written to its crl_save.log, then
   *  deleted from the device CRL once the file is durable. A crash or a failed save before that leaves the device CRL
   *  holding them. Nothing to take leaves the directory as it is. A crash between the save file and the deletion
   *  leaves the same entries in both: harmless, as the file is then the store's whole state (below).
   *
   *  A valid save file already there is the store's whole state (a load removes it, so the store has served nowhere
   *  since it was written; crash-recovery-log 3.3 step 4). Whatever the device CRL holds for the store is then a copy
   *  of part of it or an earlier tenancy's leftover, which StoreLoader.awaitCrlLoad would drop before loading the
   *  file. So the file ships unchanged and the entries are dropped (`dropStore`), releasing only the admission charge
   *  retained for them when the extraction began. An unusable save file fails the close before anything is read: its
   *  state is lost, and writing the CRL's entries over it would hide that. So does a stat of the directory or the
   *  file that fails (FileStat): it says nothing of whether the file is there, and taken for an absence it would
   *  write the CRL's entries over a valid file. Runs on the control thread, as the marker writes beside it do;
   *  nothing blocks on the CRL.
   *
   *  The store must stay closed throughout: one loaded meanwhile recovered the entries read, which the deletion would
   *  take from under it, and may have saved others the file would lack (or, from a save file, removed the file).
   *  Nothing keeps it closed, so an open started during the read, the deletion or the drop fails the extraction
   *  (`opensStarted`, checked when each answers) and leaves the charge to the load; the transfer's retransmitted
   *  request closes the store and extracts again. The release is of the charge read when the extraction began
   *  (`releaseRetained(storeId, charge)`), so it never takes a later one. startOut checks again in the control task
   *  that writes the transfer-out marker, which keeps every later open off the store.
   *
   *  Only `sds`, the transfer's source, is read: another device of the host may hold its own copy of the store (an
   *  earlier tenancy's, a transfer's source awaiting retire), whose entries are its retire's to save. */
  private def extractUnloaded(storeId: StoreId, sds: LocalStorageDeviceState): Future[Unit] =
    // A stat that fails says nothing (FileStat): taken for an absence, the CRL's entries would be written over a save
    // file, or nothing extracted from a directory that ships
    Try(FileStat.isDir(sds.devicePath / storeId.directoryName)) match
      case Failure(err) => Future.failed(err)
      case Success(false) => Future.unit
      case Success(true) => extractFromDirectory(storeId, sds)

  private def extractFromDirectory(storeId: StoreId, sds: LocalStorageDeviceState): Future[Unit] =
    val saveFile = Backend.crlSaveFile(sds.devicePath / storeId.directoryName)
    val opens = ctx.loader.opensStarted(storeId)
    def openedMeanwhile: Option[Future[Unit]] =
      if ctx.loader.opensStarted(storeId) != opens then
        Some(Future.failed(new IllegalStateException(
          s"store $storeId was opened while its CRL state was being extracted")))
      else
        None
    // Read before anything is, so the release below covers only the entries the extraction takes
    val charge = sds.admission.retainedCharge(storeId)
    def released(): Future[Unit] =
      openedMeanwhile.getOrElse:
        charge.foreach(sds.admission.releaseRetained(storeId, _))
        Future.unit
    Try(FileStat.exists(saveFile)) match
      case Failure(err) => Future.failed(err)
      case Success(true) =>
        // A file that cannot be read (CrashRecoveryLog.isUnreadable) fails the close as an invalid one does
        val valid =
          try CrashRecoveryLog.loadValidStoreState(saveFile, storeId)
          catch case t: Throwable => Left(t)
        valid match
          case Left(err) => Future.failed(err)
          case Right(_) => sds.crl.dropStore(storeId).flatMap(_ => released())
      case Success(false) =>
        sds.crl.readStoreState(storeId).flatMap: state =>
          openedMeanwhile.getOrElse:
            if state.isEmpty then
              Future.unit
            else
              // A failed save leaves them in the device CRL, for a retried request to extract again
              CrashRecoveryLog.saveStoreState(storeId, state, saveFile)
              sds.crl.deleteTransactions(storeId, state.entryIds).flatMap(_ => released())

  /** Returns the request's handling, which fails when the request is refused. Only tests look at
   *  it (they call this directly); the control thread drops it, as a refusal is answered by
   *  the receiver's retransmit. */
  def startOut(m: StartStoreTransfer): Future[Unit] =
    def err(msg: String): Nothing =
      logger.info(msg)
      throw new Exception(msg)

    // Any failure here, including a destination HostState that cannot be read, drops this
    // request; the receiver's retransmit is what tries again. Retransmits and superseded
    // requests are not filtered here either: storeTransferService ignores a request whose
    // timestamp is not newer than the last one it acted on for the store, and a newer one
    // replaces the send in progress.
    //
    // This path reads pool and device state fresh and closes the store itself, so a request
    // that arrives before this host's own device check has noticed the transfer needs no
    // separate scan.
    for
      // The pool state must be read fresh: a StoragePool object holds no store-to-device mapping,
      // and a snapshot would show where the stores were when it was taken. The source device must be read fresh --
      // for a store that has already moved once (a migration retarget, or two successive rebalances) the snapshot names the
      // device it used to live on, that device no longer lists the store, and the transfer-out
      // dies below on "Transfer probably completed" without ever starting.
      pstate <- ctx.client.getStoragePoolState(m.storeId.poolId)
      poolEntry = pstate.stores(m.storeId.poolIndex)
      fromDevice <- ctx.client.getStorageDeviceState(poolEntry.storageDeviceId)
      devEntry = fromDevice.stores.get(m.storeId) match
        case None => err(s"Store ${m.storeId} missing from device. Transfer probably completed")
        case Some(e) => e
      toDeviceId = devEntry.transferDevice match
        case None => err(s"Store ${m.storeId} not in transfer state. Transfer probably completed")
        case Some(sid) => sid
      toDevice <- ctx.client.getStorageDeviceState(toDeviceId)
      // The destination was declared failed while the transfer was in flight, so both its ids
      // read as zero. There is no fresher source for them -- unlike the source device's id
      // below, which the pool entry carries -- so the only safe thing is to refuse. Shipping
      // bytes to HostId(0,0) would run indefinitely against a host that does not exist.
      _ = if toDevice.isFailed then
            err(s"Destination device $toDeviceId for transfer of store ${m.storeId} has been " +
                s"declared failed")
      // A source being shut down has had, or is about to have, its stores closed for the shelf,
      // and its transfers out cancelled. Starting one would reopen work the shutdown waits on.
      // The receiver's retransmit keeps asking, so the transfer resumes after a restart, or from
      // an adopting host once the device moves. Failed is not refused here: its stores are
      // drained by FailedStorageDeviceDurableTask.
      _ = if fromDevice.status == StorageDeviceState.DeviceStatus.ShuttingDown ||
             fromDevice.status == StorageDeviceState.DeviceStatus.Shutdown then
            err(s"Source device ${poolEntry.storageDeviceId} for transfer of store ${m.storeId} is " +
                s"${fromDevice.status}")
      // Keyed off the pool entry, not fromDevice.storageDeviceId: those are the same value in
      // the healthy case, but a tombstoned source zeroes its own copy, and the lookup would
      // then miss and blame a removed disk for a device that is loaded and present.
      sourceDs = ctx.devices.devices.get(poolEntry.storageDeviceId) match
        case None => err(s"Source storage device for transfer ${poolEntry.storageDeviceId} not loaded. Disk removed?")
        case Some(sds) => sds
      // Resolved fresh on every request, so a destination that has moved since the last
      // attempt is reached at its current address.
      toHostState <- ctx.client.getHostState(toDevice.hostId).recover:
        case t =>
          logger.warn(s"Failed to read the HostState of host ${toDevice.hostId}, destination of " +
                      s"transfer ${m.transferUUID} of store ${m.storeId}. Error: $t")
          throw t
    yield
      // The reads above may predate the shutdown request, so the device's own guard is checked
      // as well: the shutdown has started and its step 1 may already be past this store.
      if ctx.shutdown.isShuttingDown(sourceDs.storageDeviceId) then
        err(s"Source device ${sourceDs.storageDeviceId} for transfer of store ${m.storeId} is " +
            s"being shut down")
      val markedPath = sourceDs.devicePath / m.storeId.directoryName
      if devEntry.status == StorageDeviceState.StoreStatus.TransferringOut &&
         os.exists(markedPath / RebuildingStore.RebuildRequiredMarkerFile) then
        // A store marked for rebuild (its backend failed, here during an earlier request's close;
        // rebuild-process) has no trustworthy state to ship, and the destination would refuse
        // its missing CRL save file forever. It goes back to the rebuild path instead.
        returnLostTransferToRebuild(m.storeId, sourceDs)
        err(s"Not starting the transfer out of store ${m.storeId}: it is marked rebuild-required")
      if devEntry.status == StorageDeviceState.StoreStatus.TransferringOut then
        // A retransmitted request finds the store already closed, and its going offline already gossiped
        val wasLoaded = ctx.router.loaded.contains(m.storeId)
        // Nothing keeps the store closed between its close and the marker below, written in a later control task: an
        // open meanwhile (StoreLoader.retryFailedLoads) serves it, so it is not marked or shipped
        val opens = ctx.loader.opensStarted(m.storeId)
        val fclosed = ctx.closeStoreForTransferOut(m.storeId, sourceDs)

        fclosed.failed.foreach:
          case e: StoreFailedOnClose =>
            logger.error(s"Not starting the transfer out of store ${m.storeId}, transfer ${m.transferUUID}: " +
                         s"its backend failed while closing. Returning it to the rebuild path")
            returnLostTransferToRebuild(m.storeId, sourceDs)
          case t =>
            logger.warn(s"Not starting the transfer out of store ${m.storeId}, transfer ${m.transferUUID}: " +
                        s"its close failed. Error: $t")

        fclosed.foreach: _ =>
          val storePath = sourceDs.devicePath / m.storeId.directoryName
          // Checked again now the close is done: a shutdown that began during it has already
          // run stopDeviceWork, which found no send to cancel, so a send begun here would run
          // on a device being shelved. The store is left closed and unmarked, its state in the
          // crl_save.log its close extracted, which its next load (here after a restart, or on an
          // adopting host) reads back. A device unloaded meanwhile -- its shutdown already
          // finished -- is not ours to send from.
          if ctx.shutdown.isShuttingDown(sourceDs.storageDeviceId) ||
             !ctx.devices.isLoaded(sourceDs) then
            logger.info(s"Not starting the transfer out of store ${m.storeId}: source device " +
                        s"${sourceDs.storageDeviceId} began shutting down while the store closed")
          else if ctx.router.loaded.contains(m.storeId) || ctx.loader.isOpening(m.storeId) ||
                  ctx.loader.opensStarted(m.storeId) != opens then
            // Opened since its close: the open serves it, and a retransmitted request closes it again
            logger.info(s"Not starting the transfer out of store ${m.storeId}: it was opened after its close")
          else if os.exists(storePath / RebuildingStore.RebuildRequiredMarkerFile) then
            // Marked while it closed (a BackendFailed for it): as above, never shipped.
            logger.error(s"Not starting the transfer out of store ${m.storeId}: it was marked " +
                         s"rebuild-required while it closed")
            returnLostTransferToRebuild(m.storeId, sourceDs)
          else try
            // Written before archiving, so it travels with the store and keeps this copy
            // offline across restarts until the transfer is resolved one way or the other.
            DurableFiles.write(storePath / StoreTransfer.TransferOutMarkerFile,
                               devEntry.transferId.map(_.toString).getOrElse(""))
            ctx.storeTransferService.beginTransferOut(m.storeId, m.transferUUID, storePath,
              toHostState.address, toHostState.storeTransferPort, m.timestamp)
            ctx.publishView()
            // The transfer may have been aborted after the reads above, and the abort's
            // nudge handled before the marker above existed. No check would then ever see
            // the marker beside the Active entry, leaving the store dark for a full
            // checkStorageDevicePeriod while the tree counts it online.
            ctx.reconciler.scheduleDeviceRecheck(sourceDs.storageDeviceId)
            if wasLoaded then
              ctx.services.gossipRetransmitter.broadcast(StoreOffline(m.storeId, HLCTimestamp.now))
          catch
            case t: Throwable =>
              logger.warn(s"Failed to start transfer out of store ${m.storeId}, transfer " +
                          s"${m.transferUUID}. Error: $t")

  /** Takes a TransferringOut store marked rebuild-required back to the rebuild path (rebuild-process). The transfer
   *  is aborted (AspenClient.abortStoreTransfer: the source entry back to Active, or Offline on a device shutting down,
   *  and the destination's TransferringIn entry removed), and the device is checked, so the reconcile's lost-CRL pass
   *  flips the now Active entry to Rebuilding. One abort at a time per store: retransmits arriving meanwhile do nothing
   *  more. */
  private def returnLostTransferToRebuild(storeId: StoreId, sds: LocalStorageDeviceState): Unit =
    if ! abortingLostTransfers.contains(storeId) then
      abortingLostTransfers += storeId
      logger.warn(s"Aborting the transfer out of store $storeId, which is marked rebuild-required, so it is rebuilt")
      val fabort =
        try ctx.metadata.abortLostTransfer(storeId)
        catch case t: Throwable => Future.failed(t)
      fabort.onComplete: result =>
        abortingLostTransfers -= storeId
        result.failed.foreach: t =>
          logger.warn(s"Failed to abort the transfer out of store $storeId for its rebuild. A retransmitted " +
                      s"request tries again. Error: $t")
        if ctx.devices.isLoaded(sds) then
          ctx.reconciler.checkStorageDevice(sds.storageDeviceId)

  /** Brings a store back online after its transfer out was abandoned rather than completed.
   *
   *  Purely local: the tree already says Active and the pool already names this device, so there
   *  is nothing to transact and no retry loop to get wrong. Removing the marker is what makes
   *  the repair survive a restart; loading is what makes it take effect now.
   *
   *  Loading goes through tryLoadStore rather than opening a backend here, because that method
   *  owns the rest of the bookkeeping -- reading the store config, and clearing the store from
   *  both this device's offlineStores and the host.s. Those two sets and the marker file are
   *  three representations of one fact and must not be allowed to disagree.
   *
   *  StoreMoved is gossiped from tryLoadStore's onServed, once the store is served: once the LoadStore its open
   *  queues has installed it and it has recovered, whether the backend opened in the caller or off the control
   *  thread. A store whose open or LoadStore leaves it offline is not gossiped.
   *
   *  If the load fails after the marker is gone, this pass will not pick it up again, since it
   *  keys off the marker that is now removed: the store stays offline and the device scans retry
   *  it (StoreLoader.storesAwaitingRetry), its gossip kept for the load that serves it.
   *
   *  Caller has established the tree entry and the marker.
   */
  def reinstateAbandonedOut(local: LocalStorageDeviceState,
                            storeId: StoreId): Unit =
    val storePath = local.devicePath / storeId.directoryName

    logger.warn(s"Store $storeId on device ${local.storageDeviceId} is offline behind a " +
                s"transfer-out marker, but the storage-devices tree records it as Active with " +
                s"no transfer outstanding. Clearing the abandoned transfer and loading the store")

    val markerRemoved =
      try
        os.remove(storePath / StoreTransfer.TransferOutMarkerFile)
        true
      catch
        case t: Throwable =>
          // Leave the store offline. Loading it with the marker still in place would be undone
          // by the next restart, so this pass would have logged a repair that did not hold.
          logger.error(s"Failed to remove the transfer-out marker for store $storeId at " +
                       s"$storePath. The store stays offline. Error: $t")
          false

    if markerRemoved then
      ctx.tryLoadStore(local, storePath, Some(() => ctx.gossipStoreOnline(storeId)))
