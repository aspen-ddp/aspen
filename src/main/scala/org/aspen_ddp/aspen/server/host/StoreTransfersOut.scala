package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostState, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.{StartStoreTransfer, StoreOffline}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.StoreFailedOnClose
import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.store.backend.Backend
import scribe.Logging
import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The sending end of a store transfer on this host (transfer out): taking the store out of service with its CRL
 *  state extracted, answering the receiver's request, and returning a lost or abandoned transfer to the rebuild or
 *  the loader. The receiving end is StoreTransfers. */
class StoreTransfersOut(ctx: DeviceControl) extends Logging:
  import ctx.given

  // A transfer out's work is its copy's Work.TransferOut on the source device: Closing from startOut's close (and CRL
  // extraction) until the control task that moves the copy to outbound/, or fails the request, has run, and
  // AbortingLost while returnLostTransferToRebuild aborts the transfer of a Lost copy. It refuses
  // every open of the copy meanwhile (StoreLoader.startOpen, R12), so a store is never served from a save file whose
  // entries the extraction is still deleting from the device CRL

  // Stores whose rebuild start waited for their close for a transfer out (deferRebuildUntilClosed): the close's end checks the
  // device again, which starts it
  private var rebuildsAwaitingClose: Set[StoreId] = Set()

  /** Claims the store's TransferOut work on `sds` at `stage` before a transfer out's close or abort starts (I3): false
   *  if other work holds the (device, store), and then nothing starts. An abort claimed during the close (a copy marked
   *  while it closed) takes the close's work over */
  private def claimWork(storeId: StoreId, sds: LocalStorageDeviceState, transferId: Option[UUID],
                        stage: TransferOutStage): Boolean =
    ctx.copyRegistry.work("transfer out", sds.storageDeviceId, storeId, _.isInstanceOf[Work.TransferOut],
      Some(Work.TransferOut(transferId, stage)))

  /** Ends the store's TransferOut work on `sds` if it is at `stage`: another stage there is other work's to end */
  private def endWork(storeId: StoreId, sds: LocalStorageDeviceState, stage: TransferOutStage): Unit =
    if ctx.copyRegistry.transferOutOn(sds.storageDeviceId, storeId).exists(_.stage == stage) then
      ctx.copyRegistry.work("transfer out", sds.storageDeviceId, storeId, _.isInstanceOf[Work.TransferOut], None): Unit

  /** Whether a rebuild of the store must wait for its close for a transfer out (I3: a discard of its copy would move
   *  the directory the close is extracting from). When it must, the close's end checks the source device again */
  private[host] def deferRebuildUntilClosed(storeId: StoreId): Boolean =
    val closing = ctx.copyRegistry.transferringOut(storeId, TransferOutStage.Closing)
    if closing then
      rebuildsAwaitingClose += storeId
    closing

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
   *  write the CRL's entries over a valid file. Runs on the control thread, as the move beside it does;
   *  nothing blocks on the CRL.
   *
   *  The store must stay closed throughout: one loaded meanwhile would recover the entries read, which the deletion
   *  would take from under it, and might save others the file would lack (or, from a save file, remove the file). The
   *  store's TransferOut work on the device keeps it closed: an open of the copy is refused from startOut's close
   *  until the control task that moves the copy to outbound/, which keeps every later open off the store
   *  (StoreLoader.startOpen, R12). The release is of the charge read when the extraction began
   *  (`releaseRetained(storeId, charge)`), so it never takes a later one.
   *
   *  Only `sds`, the transfer's source, is read: another device of the host may hold its own copy of the store (an
   *  earlier tenancy's, a transfer's source awaiting retire), whose entries are its retire's to save. */
  private def extractUnloaded(storeId: StoreId, sds: LocalStorageDeviceState): Future[Unit] =
    // A stat that fails says nothing (FileStat): taken for an absence, the CRL's entries would be written over a save
    // file, or nothing extracted from a directory that ships
    Try(FileStat.isDir(ctx.copyRegistry.pathOrLive(sds, storeId))) match
      case Failure(err) => Future.failed(err)
      case Success(false) => Future.unit
      case Success(true) => extractFromDirectory(storeId, sds)

  private def extractFromDirectory(storeId: StoreId, sds: LocalStorageDeviceState): Future[Unit] =
    val saveFile = Backend.crlSaveFile(ctx.copyRegistry.pathOrLive(sds, storeId))
    // Read before anything is, so the release below covers only the entries the extraction takes
    val charge = sds.admission.retainedCharge(storeId)
    def released(): Future[Unit] =
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
    // A refusal the receiver's retransmit resolves: the request is retried as it is, once the work it waits on ends
    def refuse(msg: String): Nothing =
      logger.debug(msg)
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
      // Single flight per store (I3: one piece of work per (device, store)). A retransmit or readdressed request that
      // arrives while an earlier one is closing the store, or extracting its CRL state (extractUnloaded), does
      // nothing: a second close would find the store gone and move the copy and begin the send while the first
      // is still extracting, and its end would end the first's work under it. If the first fails, the receiver's next
      // retransmit tries again. Keyed host-wide by store id
      if ctx.copyRegistry.transferringOut(m.storeId, TransferOutStage.Closing) then
        refuse(s"Not starting the transfer out of store ${m.storeId} again: an earlier request is still closing it")
      // Nor while a rebuild of the store is registered here (I3), its flip's retry after an outcome-unknown attempt
      // included: that attempt may have flipped the entry Active, and a transfer made since is then this request. The
      // rebuild's end keeps the copy (RebuildOutcome.Unconfirmed), and the receiver's retransmit starts the transfer.
      // A discard of its lost copy (DiscardingLost) is rebuild work too, a queued rebuild not: HostCopies.RebuildHolds.
      // Keyed host-wide by store id: a rebuild for a copy on another device of this host delays it too, which only
      // delays
      if ctx.copyRegistry.rebuilding(HostCopies.RebuildHolds).contains(m.storeId) then
        refuse(s"Not starting the transfer out of store ${m.storeId} yet: a rebuild of it is still registered")
      // A Lost copy never ships
      if devEntry.status == StorageDeviceState.StoreStatus.TransferringOut && isLost(sourceDs, m.storeId) then
        // A store in lost/ (its backend failed, here during an earlier request's close; rebuild-process), or whose
        // move there awaits its retry, has no trustworthy state to ship, and the destination would refuse its
        // missing CRL save file forever. It goes back to the rebuild path instead.
        returnLostTransferToRebuild(m.storeId, sourceDs)
        err(s"Not starting the transfer out of store ${m.storeId}: it is Lost")
      if devEntry.status == StorageDeviceState.StoreStatus.TransferringOut then
        // A retransmitted request finds the store already closed, and its going offline already gossiped
        val wasLoaded = ctx.router.loaded.contains(m.storeId)
        // Only a Live, Rebuilt or parked copy is a T6 source (P10), or one already Outbound, which a retransmit (or a
        // restart's) ships again. An Arrived copy, whose move to Live failed, waits for the Completed Transfers In pass
        sourcePhase(sourceDs, m.storeId) match
          case None =>
            refuse(s"Not starting the transfer out of store ${m.storeId}: device ${sourceDs.storageDeviceId} holds no " +
                   s"copy of it")
          case Some(phase) if !isSource(phase) =>
            refuse(s"Not starting the transfer out of store ${m.storeId}: its copy on device " +
                   s"${sourceDs.storageDeviceId} is $phase, which is never shipped")
          // A send of the store still registered (an earlier transfer's, aborted, its archiver possibly still reading
          // the copy) keeps its directory where it is: the move this request needs waits until that send has ended.
          // A request for the transfer the copy is already Outbound for moves nothing, and its send supersedes
          case Some(phase) if needsMove(phase, tidOf(devEntry)) && ctx.storeTransferService.isTransferringOut(m.storeId) =>
            refuse(s"Not starting the transfer out of store ${m.storeId} yet: an earlier send of it is still " +
                   s"registered, and its copy on device ${sourceDs.storageDeviceId} ($phase) must move")
          case Some(_) => ()
        // The store stays closed between its close and its move to outbound/ below, made in a later control task: its
        // TransferOut work, started here before the close, refuses every open of the copy until then (R12). A claim
        // refused (other work holds the copy, recorded) starts nothing: the receiver's retransmit asks again. Nor does
        // a copy whose last change is not durable (unsynced: a crash could undo it, an earlier move to outbound/
        // among them): the next device check forces it, and the retransmit after that ships it
        if ctx.copyRegistry.isUnsynced(sourceDs, m.storeId) then
          refuse(s"Not starting the transfer out of store ${m.storeId} yet: a change to its copy is not durable")
        if !claimWork(m.storeId, sourceDs, devEntry.transferId, TransferOutStage.Closing) then
          refuse(s"Not starting the transfer out of store ${m.storeId} yet: the copy registry refused its work")
        val fclosed =
          try ctx.closeStoreForTransferOut(m.storeId, sourceDs)
          catch case t: Throwable => Future.failed(t)

        fclosed.onComplete: result =>
          try closedForTransfer(m, devEntry, sourceDs, toHostState, wasLoaded, result)
          finally
            endWork(m.storeId, sourceDs, TransferOutStage.Closing)
            if rebuildsAwaitingClose.contains(m.storeId) then
              rebuildsAwaitingClose -= m.storeId
              if ctx.devices.isLoaded(sourceDs) then
                ctx.reconciler.checkStorageDevice(sourceDs.storageDeviceId)

  /** The phase of the record of `storeId`'s copy on `sds`, None without one */
  private def sourcePhase(sds: LocalStorageDeviceState, storeId: StoreId): Option[Phase] =
    ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(storeId)).map(_.phase)

  /** Whether a copy in `phase` may be shipped (P10): Live, Rebuilt (an Unconfirmed rebuild's), parked (Outbound(""))
   *  or already Outbound (a retransmit's) */
  private def isSource(phase: Phase): Boolean = phase match
    case Phase.Live | Phase.Rebuilt(_) | Phase.Outbound(_) => true
    case _ => false

  /** The transfer id a transfer out of `devEntry` moves its copy to Outbound with ("" for none) */
  private def tidOf(devEntry: StorageDeviceState.StoreEntry): String = devEntry.transferId.map(_.toString).getOrElse("")

  /** Whether a copy in `phase` must change place or parameter to be Outbound(tid) */
  private def needsMove(phase: Phase, tid: String): Boolean = phase != Phase.Outbound(tid)

  /** T6: moves the closed copy of `storeId` on `sds` into outbound/ with transfer id `tid`, returning the copy's path
   *  there once the move is durable. One already Outbound for `tid` stays where it is. One Outbound for another
   *  transfer (aborted, then a new one started before the Abandoned Transfers Out pass reinstated it) goes back to
   *  Live first (T7), its state still in its crl_save.log. None, logged, when a move fails or is not forced: nothing
   *  ships, and the receiver's retransmit retries (an unsynced copy once a device check has forced it) */
  private def moveToOutbound(sds: LocalStorageDeviceState, storeId: StoreId, tid: String): Option[os.Path] =
    def moveTo(to: Phase): Boolean =
      ctx.moves.move("transfer out", sds, storeId, to) match
        case MoveResult.Durable => true
        case MoveResult.Unsynced(_) | MoveResult.NotMoved(_) => false
    val moved = sourcePhase(sds, storeId) match
      case Some(Phase.Outbound(t)) if t == tid => true
      case Some(phase) if ctx.storeTransferService.isTransferringOut(storeId) =>
        // startOut refused this; a send registered since (none starts but here) would have its directory moved
        logger.warn(s"Not shipping store $storeId: a send of it is registered, and its copy ($phase) must move")
        false
      case Some(Phase.Outbound(t)) if t.nonEmpty => moveTo(Phase.Live) && moveTo(Phase.Outbound(tid))
      case Some(phase) if isSource(phase) => moveTo(Phase.Outbound(tid))
      case other =>
        logger.warn(s"Not shipping store $storeId: its copy on device ${sds.storageDeviceId} is now $other")
        false
    if moved then ctx.copyRegistry.pathOf(sds, storeId) else None

  /** The rest of startOut once the store's close for its transfer out has completed, in that completion's control
   *  task: either the closed copy is moved to outbound/ (T6) and the send begun from there, or the request is
   *  refused */
  private def closedForTransfer(m: StartStoreTransfer, devEntry: StorageDeviceState.StoreEntry,
                                sourceDs: LocalStorageDeviceState, toHostState: HostState, wasLoaded: Boolean,
                                result: Try[Unit]): Unit =
    result.failed.foreach:
      case e: StoreFailedOnClose =>
        logger.error(s"Not starting the transfer out of store ${m.storeId}, transfer ${m.transferUUID}: " +
                     s"its backend failed while closing. Returning it to the rebuild path")
        returnLostTransferToRebuild(m.storeId, sourceDs)
      case t =>
        logger.warn(s"Not starting the transfer out of store ${m.storeId}, transfer ${m.transferUUID}: " +
                    s"its close failed. Error: $t")

    result.foreach: _ =>
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
      // No open of the copy can have started since its close: its TransferOut work refused them (R12). An open of
      // another device's copy of the store on this host reads that device's CRL, never this extraction's
      else if isLost(sourceDs, m.storeId) then
        // Moved to lost/ while it closed (a BackendFailed for it), or its move there awaits its retry: as above, never
        // shipped.
        logger.error(s"Not starting the transfer out of store ${m.storeId}: it became Lost while it closed")
        returnLostTransferToRebuild(m.storeId, sourceDs)
      else try
        // Moved before archiving, after the extraction made crl_save.log durable: outbound/ keeps this copy offline
        // across restarts until the transfer is resolved one way or the other, and the shipped copy carries its
        // phase-param.outbound, inert at the destination. A move that fails or is not forced ships nothing
        val outbound = moveToOutbound(sourceDs, m.storeId, tidOf(devEntry))
        outbound.foreach: outboundPath =>
          ctx.storeTransferService.beginTransferOut(m.storeId, m.transferUUID, outboundPath,
            toHostState.address, toHostState.storeTransferPort, m.timestamp)
        ctx.publishView()
        // The transfer may have been aborted after the reads above, and the abort's nudge handled before the copy
        // was in outbound/. No check would then ever see it beside the Active entry, leaving the store dark for a
        // full checkStorageDevicePeriod while the tree counts it online. An unsynced move is forced by it too
        ctx.reconciler.scheduleDeviceRecheck(sourceDs.storageDeviceId)
        if wasLoaded then
          ctx.services.gossipRetransmitter.broadcast(StoreOffline(m.storeId, HLCTimestamp.now))
      catch
        case t: Throwable =>
          logger.warn(s"Failed to start transfer out of store ${m.storeId}, transfer " +
                      s"${m.transferUUID}. Error: $t")

  /** Whether the copy of `storeId` on `sds` is Lost (in lost/), or held for a move there that failed
   *  (CopyMoves.pendingLost): such a copy is never shipped */
  private def isLost(sds: LocalStorageDeviceState, storeId: StoreId): Boolean =
    ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(storeId)).exists(_.phase == Phase.Lost) ||
      ctx.moves.isPendingLost(sds, storeId)

  /** Takes a TransferringOut store that is Lost back to the rebuild path (rebuild-process). The transfer
   *  is aborted (AspenClient.abortStoreTransfer: the source entry back to Active, or Offline on a device shutting down,
   *  and the destination's TransferringIn entry removed), and the device is checked, so the reconcile's lost-CRL pass
   *  flips the now Active entry to Rebuilding. One abort at a time per store: retransmits arriving meanwhile do nothing
   *  more. */
  private def returnLostTransferToRebuild(storeId: StoreId, sds: LocalStorageDeviceState): Unit =
    if ! ctx.copyRegistry.transferringOut(storeId, TransferOutStage.AbortingLost) &&
       claimWork(storeId, sds, None, TransferOutStage.AbortingLost) then
      logger.warn(s"Aborting the transfer out of store $storeId, which is Lost, so it is rebuilt")
      val fabort =
        try ctx.metadata.abortLostTransfer(storeId)
        catch case t: Throwable => Future.failed(t)
      fabort.onComplete: result =>
        endWork(storeId, sds, TransferOutStage.AbortingLost)
        result.failed.foreach: t =>
          logger.warn(s"Failed to abort the transfer out of store $storeId for its rebuild. A retransmitted " +
                      s"request tries again. Error: $t")
        if ctx.devices.isLoaded(sds) then
          ctx.reconciler.checkStorageDevice(sds.storageDeviceId)

  /** Brings a store back online after its transfer out was abandoned rather than completed.
   *
   *  Purely local: the tree already says Active and the pool already names this device, so there
   *  is nothing to transact and no retry loop to get wrong. The move from outbound/ back to Live (T7) is what makes
   *  the repair survive a restart; loading is what makes it take effect now.
   *
   *  Loading goes through tryLoadStore rather than opening a backend here, because that method
   *  owns the rest of the bookkeeping -- reading the store config, and taking its copy from offline
   *  to Opening in the copy registry, which derives the host's silence from it.
   *
   *  StoreMoved is gossiped from tryLoadStore's onServed, once the store is served: once the LoadStore its open
   *  queues has installed it and it has recovered, whether the backend opened in the caller or off the control
   *  thread. A store whose open or LoadStore leaves it offline is not gossiped.
   *
   *  If the load fails after the move, this pass will not pick it up again, since it keys off the
   *  Outbound phase the copy has left: the store stays offline and the device scans retry
   *  it (its copy AwaitingRetry, StoreLoader.retryFailedLoads), its gossip kept for the load that serves it.
   *
   *  Caller has established the tree entry and the copy's Outbound phase.
   */
  def reinstateAbandonedOut(local: LocalStorageDeviceState,
                            storeId: StoreId): Unit =
    logger.warn(s"Store $storeId on device ${local.storageDeviceId} is offline in outbound/, but the " +
                s"storage-devices tree records it as Active with no transfer outstanding. Clearing the abandoned " +
                s"transfer and loading the store")

    // T7. A failed move leaves the store offline in outbound/, for a later check. Loading it before its move is
    // durable could be undone by the next restart, so this pass would have logged a repair that did not hold. A move
    // not forced leaves it unsynced, which the next check of the device forces and loads (DeviceReconciler)
    ctx.moves.move("reinstate abandoned transfer out", local, storeId, Phase.Live) match
      case MoveResult.Durable =>
        ctx.tryLoadStore(local, ctx.copyRegistry.pathOrLive(local, storeId),
          Some(() => ctx.gossipStoreOnline(storeId)))
      case MoveResult.Unsynced(_) | MoveResult.NotMoved(_) => ()
