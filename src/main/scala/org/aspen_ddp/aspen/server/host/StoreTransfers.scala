package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.host.DeviceMetadata.TransferOutcome
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object StoreTransfers:
  /** `toDeviceId` is the local device receiving the store, so a check of that device can tell
   *  which of its transfers in the tree no longer records. `msg` is re-addressed (see
   *  StoreTransfers.readdressPendingTransfer) when its source device turns out to have moved host. */
  class PendingTransfer(var msg: StartStoreTransfer,
                        val toDeviceId: StorageDeviceId,
                        var lastSend: HLCTimestamp)

  /** A failed transfer in waiting out TransferInRetryDelay. `fromDeviceId` is the source the retry
   *  would ask for the store again, `toDeviceId` the local device receiving it. */
  class TransferInRetry(val fromDeviceId: StorageDeviceId,
                        val toDeviceId: StorageDeviceId,
                        val run: () => Unit)

  /** How long a failed transfer in waits before it is restarted */
  val TransferInRetryDelay: Duration = Duration(30, SECONDS)

import StoreTransfers.*

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The receiving end of a store transfer on this host (transfer in): starting it, retrying it, recording its pool
 *  update, and the bookkeeping that lets a shutdown, an abort or a failed source cancel it. The sending end is
 *  StoreTransfersOut.
 *
 *  Metadata and store loading go through the
 *  overridable seams on Host (via `ctx`). */
class StoreTransfers(ctx: DeviceControl) extends Logging:
  import ctx.given

  // The pool updates of transfers in now running (recordTransferIn), with the device receiving
  // each, completed once its outcome has been acted on. A shutdown of that device waits on them.
  private var recordingTransfersIn: Map[StoreId, (StorageDeviceId, Promise[Unit])] = Map()
  // The transfer UUID of each store this host is currently receiving. Stores being shipped out
  // are tracked by storeTransferService, which is what knows when a send has ended.
  private[server] var transferringIn: Map[StoreId, UUID] = Map()
  private var abandoningTransferIns: Set[StoreId] = Set()
  private[host] var pendingStartTransfers: Map[StoreId, PendingTransfer] = Map()
  // The restart of each failed transfer in that is waiting out TransferInRetryDelay. Its store
  // stays in transferringIn meanwhile, so device checks do not start a parallel attempt.
  private[server] var transferInRetries: Map[StoreId, TransferInRetry] = Map()

  /** The transfer-in work on `deviceId` that a shutdown of it must stop or wait for. `ingStores` are the stores
   *  the device's tree entry records as TransferringIn.
   *
   *  Returns the transfer UUID of each attempt forgotten here, for the caller to cancel with the transfer service,
   *  and, by store, the pool updates already running, which cannot be cancelled and
   *  are waited for instead.
   *
   *  Past promotion, a transfer in cannot be cancelled: its pool update is a transaction already under way.
   *  Waited for, so the shutdown neither closes the device under it nor commits Shutdown before its outcome -- an
   *  Offline entry -- has been acted on.
   *
   *  The attempts are forgotten before the service is told, so each attempt's completion handler finds itself no
   *  longer current and neither retries nor promotes. An attempt already past that check runs its pool update,
   *  which writes the entry Offline. */
  def inFlightFor(deviceId: StorageDeviceId,
                  ingStores: Set[StoreId]): (List[(StoreId, UUID)], Map[StoreId, Future[Unit]]) =
    val recording = recordingTransfersIn.collect:
      case (storeId, (toDeviceId, settled)) if toDeviceId == deviceId => storeId -> settled.future

    val ins = ingStores.toList.flatMap: storeId =>
      if pendingStartTransfers.get(storeId).exists(_.toDeviceId == deviceId) then
        pendingStartTransfers -= storeId
      if transferInRetries.get(storeId).exists(_.toDeviceId == deviceId) then
        transferInRetries -= storeId
      val uuid = transferringIn.get(storeId)
      transferringIn -= storeId
      uuid.map(storeId -> _)

    (ins, recording)

  /** Re-sends the StartStoreTransfer of every transfer in that has not yet completed and was last
   *  sent more than 30 s ago. `force` skips the age check so tests need not wait it out.
   *
   *  Each re-send also re-reads the source device, and if the device now names another host the
   *  request is re-addressed and sent again there: a source shut down and adopted elsewhere
   *  would otherwise be asked for at its old host for as long as the attempt lives, since the
   *  reconcile's TransferringIn pass never starts a second attempt while this one holds the store
   *  in transferringIn. The re-send itself stays synchronous and goes to the address already
   *  held, so a failed or slow read costs nothing over not reading at all.
   */
  def retransmitPendingStarts(force: Boolean = false): Unit =
    val now = HLCTimestamp.now
    pendingStartTransfers.valuesIterator.foreach: pt =>
      if force || now - pt.lastSend > Duration(30, SECONDS) then
        pt.lastSend = now
        ctx.client.sendHostMessage(pt.msg)
        val fsource =
          try ctx.metadata.readTransferSourceState(pt.msg.fromDevice)
          catch case t: Throwable => Future.failed(t)
        fsource.onComplete:
          case Success(source) =>
            // Not re-sent if the attempt ended while the read was out.
            if pendingStartTransfers.get(pt.msg.storeId).contains(pt) && readdressPendingTransfer(pt, source) then
              ctx.client.sendHostMessage(pt.msg)
          case Failure(t) =>
            logger.warn(s"Failed to re-read source device ${pt.msg.fromDevice} of the transfer in of " +
                        s"store ${pt.msg.storeId}. Its request stays addressed to host ${pt.msg.toHost}. Error: $t")

  /** Points a pending transfer's request at `source`'s current host when that has changed, so
   *  the next send reaches a source that has been adopted by another host. A failed source keeps
   *  its address: its zeroed host id names nobody, and the reconcile abandons the transfer.
   *  Returns whether the address changed. */
  private[host] def readdressPendingTransfer(pt: PendingTransfer, source: StorageDeviceState): Boolean =
    if !source.isFailed && source.hostId != pt.msg.toHost then
      logger.info(s"Source device ${pt.msg.fromDevice} of the transfer in of store ${pt.msg.storeId} " +
                  s"is now on host ${source.hostId}, not ${pt.msg.toHost}. Asking there")
      pt.msg = pt.msg.copy(toHost = source.hostId)
      true
    else
      false

  private def discardArrivedTransferIn(storeId: StoreId,
                                       toDeviceId: StorageDeviceId): Unit =
    ctx.devices.discardStoreDirectory(storeId, toDeviceId,
      "The pool does not name this device, so this copy belongs to nobody")

  /** Restarts a failed transfer in after TransferInRetryDelay rather than at once. A deterministic
   *  failure (the sender's archiver exiting non-zero, a full disk here) would otherwise loop hot,
   *  re-shipping the store each time, since every restart supersedes the sender's attempt at once.
   *
   *  The store stays in transferringIn until the retry fires, so device checks in the meantime
   *  do not start a parallel attempt. `transferUUID` is the failed attempt's: the retry does
   *  nothing if that attempt is no longer the store's current one.
   *
   *  The destination's entry is re-read before restarting, since the transfer may have been
   *  abandoned during the wait. The retry stays scheduled until that read returns, so an abandon
   *  committing meanwhile still finds it in transferInRetries and cancels it.
   */
  private def scheduleTransferInRetry(storeId: StoreId,
                                      transferUUID: UUID,
                                      fromHostId: HostId,
                                      fromDeviceId: StorageDeviceId,
                                      toDeviceid: StorageDeviceId,
                                      transferId: Option[UUID]): Unit =
    def current: Boolean =
      transferInRetries.contains(storeId) && transferringIn.get(storeId).contains(transferUUID)

    def restart(entryState: scala.util.Try[StorageDeviceState], sourceHostId: HostId): Unit =
      if current then
        transferInRetries -= storeId
        transferringIn -= storeId
        val stillWanted = entryState match
          case Success(state) =>
            state.stores.get(storeId).exists: entry =>
              entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
              entry.transferDevice.contains(fromDeviceId) &&
              entry.transferId == transferId
          case Failure(t) =>
            // No worse than restarting blind: an abandoned attempt's restart is cancelled again
            // once the abandon commits.
            logger.warn(s"Failed to re-read device $toDeviceid before retrying the transfer in of " +
                        s"store $storeId. Retrying anyway. Error: $t")
            true
        if stillWanted then
          startIn(storeId, sourceHostId, fromDeviceId, toDeviceid, transferId)
        else
          logger.info(s"Not retrying the transfer in of store $storeId: device $toDeviceid no " +
                      s"longer records it as transferring in from device $fromDeviceId")

    def retry(): Unit =
      if current then
        val fstate =
          try ctx.client.getStorageDeviceState(toDeviceid)
          catch case t: Throwable => Future.failed(t)
        // The source's owner is re-read too: the device may have been shut down and adopted by
        // another host since the failed attempt asked for it. A failed read keeps the old one.
        val fsourceHost =
          (try ctx.metadata.readTransferSourceState(fromDeviceId) catch case t: Throwable => Future.failed(t))
            .map(source => if source.isFailed then fromHostId else source.hostId)
            .recover:
              case t =>
                logger.warn(s"Failed to re-read source device $fromDeviceId before retrying the " +
                            s"transfer in of store $storeId. Asking host $fromHostId. Error: $t")
                fromHostId
        fstate.transformWith(st => fsourceHost.map(h => (st, h))).foreach((st, h) => restart(st, h))

    logger.info(s"Retrying the transfer in of store $storeId in $TransferInRetryDelay")
    transferInRetries += storeId -> new TransferInRetry(fromDeviceId, toDeviceid, () => retry())
    ctx.scheduler.schedule(TransferInRetryDelay)(retry())

  /** Forgets this host's attempt to receive `storeId` from `fromDeviceId`, whose transfer has been
   *  abandoned. Covers an attempt still waiting on bytes and one waiting out its retry delay; an
   *  attempt already past promotion is left to recordTransferIn, which drops its own entry.
   *
   *  The service's registration is dropped too, failing its future. The completion handler in
   *  startIn sees the attempt is no longer current and neither retries nor promotes.
   */
  private[host] def dropAbandonedTransferIn(storeId: StoreId,
                                      fromDeviceId: StorageDeviceId,
                                      toDeviceId: StorageDeviceId): Unit =
    val cancelled =
      val pending = pendingStartTransfers.get(storeId).filter(_.msg.fromDevice == fromDeviceId)
      val retry = transferInRetries.get(storeId).filter(_.fromDeviceId == fromDeviceId)
      transferringIn.get(storeId) match
        case Some(transferUUID) if pending.nonEmpty || retry.nonEmpty =>
          logger.info(s"Dropping the abandoned transfer in of store $storeId from device $fromDeviceId")
          pendingStartTransfers -= storeId
          transferInRetries -= storeId
          transferringIn -= storeId
          // A retry waiting on a failed promotion still has the extracted copy in staging, which
          // the service no longer owns. Nothing else can be receiving this store now.
          if retry.nonEmpty then
            ctx.devices.devices.get(toDeviceId).foreach: sds =>
              val staged = sds.devicePath / StoreTransfer.StagingDirectory / storeId.directoryName
              try os.remove.all(staged)
              catch case t: Throwable =>
                logger.error(s"Failed to remove the staged copy of store $storeId at $staged: $t")
          Some(transferUUID)
        case _ => None
    cancelled.foreach(ctx.storeTransferService.cancelTransferIn(storeId, _))

  /** Removes the transferred-in marker from a promoted store, returning whether the store may now
   *  be loaded. A store loaded with the marker still in place would be skipped by the next
   *  restart's scan, so on failure it is left unloaded for reconcileDeviceState's
   *  transferred-in pass to try again. The store's arrival (StoreLoader.ArrivedAtFile) is recorded
   *  first, while the marker still keeps it from loading: it came from another host, whose clock
   *  may lead this one's, and its load floor must cover that.
   */
  private[host] def clearTransferredInMarker(storeId: StoreId, storePath: os.Path): Boolean =
    try
      StoreLoader.writeArrivedAt(storePath)
      os.remove(storePath / StoreTransfer.TransferredInMarkerFile)
      true
    catch
      case t: Throwable =>
        logger.error(s"Failed to remove the transferred-in marker for store $storeId at " +
                     s"$storePath. The store stays offline. Error: $t")
        false

  /** Runs the metadata step for a transferred store that has been promoted to its final path,
   *  then loads or discards the arrived copy according to the outcome. Reached both straight
   *  after promotion and, via startIn, for a copy promoted before a crash.
   *
   *  `transferUUID` is the attempt's entry in transferringIn, which is dropped only once nothing
   *  more can happen to the arrived copy. Dropped any earlier, a device check could start a
   *  second attempt that re-ships the whole store, or resume this step a second time in parallel.
   */
  private def recordTransferIn(storeId: StoreId,
                               transferUUID: UUID,
                               fromDeviceId: StorageDeviceId,
                               toDeviceid: StorageDeviceId): Unit =
    // Registered before the update starts, so a shutdown of toDeviceid that begins at any point
    // from here on finds it and waits for it.
    val settled = Promise[Unit]()
    recordingTransfersIn += storeId -> (toDeviceid, settled)

    def finished(): Unit =
      if transferringIn.get(storeId).contains(transferUUID) then
        transferringIn -= storeId
      if recordingTransfersIn.get(storeId).exists(_._2 eq settled) then
        recordingTransfersIn -= storeId
      settled.trySuccess(())

    // Loading is downstream of the metadata decision, not ahead of it. An unconditional load
    // hands a live copy of the store to a device that updateStateForTransferredStore may be about
    // to decide must not own it, and nothing would ever take it back.
    //
    // Waiting costs no availability: the pool goes on naming the source until that transaction
    // commits, so nothing routes here before then anyway.
    val foutcome =
      try ctx.metadata.updateStateForTransferredStore(storeId, fromDeviceId, toDeviceid,
                                                          ctx.services.gossipRetransmitter)
      catch case t: Throwable => Future.failed(t)
    foutcome.onComplete: outcome =>
      try
        outcome match
          case Success(TransferOutcome.PoolRepointed) =>
            ctx.devices.devices.get(toDeviceid).foreach: sds =>
              val storePath = sds.devicePath / storeId.directoryName
              if clearTransferredInMarker(storeId, storePath) then
                // A rebuilt copy whose flip was Unconfirmed ships with its rebuild-complete marker
                // (rebuild-process 5.7). The flip committed before the source's entry could become
                // TransferringOut, and this entry is Active now, which is all the Completed Rebuilds
                // pass checks: cleared here, or tryLoadStore keeps the pool's only owner copy offline
                // until this device's next check. A failed removal is left to that pass, as there.
                if !os.exists(storePath / RebuildingStore.RebuildCompleteMarkerFile) ||
                   ctx.rebuilds.clearRebuildCompleteMarker(storeId, storePath)
                then
                  ctx.loadStoreById(toDeviceid, storeId)
                else
                  ctx.markStoreOffline(sds, storeId)
              else
                // Recorded as a restart's scan would have, which is what the transferred-in
                // pass keys off.
                ctx.markStoreOffline(sds, storeId)

          case Success(TransferOutcome.PoolRepointedOffline) =>
            // Left exactly as it arrived, behind its transferred-in marker, and recorded offline
            // as a restart's scan would record it. Once a restart or adoption makes the entry
            // Active, the reconcile's completed-transfers-in pass clears the marker and loads it.
            // Unlike the shutdown's own markers this needs no disk write, so it is the same
            // whether or not the device is still loaded.
            ctx.router.markOffline(storeId)
            ctx.devices.devices.get(toDeviceid).foreach: sds =>
              sds.offlineStores += storeId
            logger.info(s"Store $storeId has transferred onto device $toDeviceid, which is being " +
                        s"shut down. It stays offline")

          case Success(TransferOutcome.SourceRestored) | Success(TransferOutcome.Aborted) =>
            discardArrivedTransferIn(storeId, toDeviceid)

          case Success(TransferOutcome.NotOwned) =>
            // Not ours to load or discard. Its device is not loaded here either, or will not be
            // for long: a shutdown unloaded it before the move.
            ()

          case Failure(t) =>
            // The only observer this Future has. Its onFail routes the permanent errors to
            // StopRetrying, which fails the promise -- and an unobserved failed Promise is not
            // reported by the ExecutionContext, so without this line a permanent give-up is
            // exactly as silent as the infinite retry loop it replaced. The arrived copy keeps its
            // transferred-in marker and the destination's entry stays TransferringIn, so the next
            // device check resumes this step; until one succeeds the source's copy stays offline
            // behind its own marker.
            logger.error(s"Failed to record the completed transfer of store $storeId " +
                         s"from device $fromDeviceId to device $toDeviceid. The store is " +
                         s"not loaded here and the pool still names the source", t)
      finally
        finished()

  def startIn(storeId: StoreId,
                                   fromHostId: HostId,
                                   fromDeviceId: StorageDeviceId,
                                   toDeviceid: StorageDeviceId,
                                   transferId: Option[UUID]): Unit =
    require(fromDeviceId != toDeviceid)

    // The shutdown guard covers a source read issued before this device's shutdown began, and a
    // retry whose delay ran out during it: the shutdown has already cancelled this device's
    // transfers in and would not see a new one.
    if ! transferringIn.contains(storeId) && ! ctx.shutdown.isShuttingDown(toDeviceid) then
      ctx.devices.devices.get(toDeviceid).foreach: toDevice =>
        val devicePath = toDevice.devicePath
        val finalPath = devicePath / storeId.directoryName

        // A promoted copy belongs to the transfer whose id its marker records. One left by an
        // earlier transfer that was aborted or given up on predates whatever the source has
        // accepted since, so resuming it would install stale data. One found lost since its
        // promotion -- marked rebuild-required (a CRL loss declaration that could not read it),
        // or its save file no longer valid -- would be repointed onto, and the source's intact
        // copy then released, as promotion refuses for a staged copy. Until the pool update
        // commits the source still holds the state, so either is discarded and the transfer
        // started over.
        // A save file that cannot be read (CrashRecoveryLog.isUnreadable), or a marker whose stat or read fails
        // (FileStat), says nothing of the copy: it is neither resumed nor discarded, and the next check reads it again.
        // Nor is one whose delete could not stat it (StoreCopies.delete answers not gone): the copy stays stale-kept.
        val promotedMarker = finalPath / StoreTransfer.TransferredInMarkerFile
        val promoted = FileStat.existence(promotedMarker)
        val staleKept = !promoted.contains(false) &&
          locally:
            val unusable: Either[Throwable, Option[String]] =
              try
                if promoted.isEmpty then
                  FileStat.exists(promotedMarker) // Throws the stat's error, to be logged below
                // A read that fails says nothing of which transfer the copy belongs to: thrown, so the copy waits
                val markerId = os.read(promotedMarker).trim
                if !transferId.exists(_.toString == markerId) then
                  Right(Some(s"it belongs to transfer '$markerId', not to the current transfer $transferId"))
                else if StoreDirMarkers.has(finalPath, StoreDirMarkers.Marker.RebuildRequired) then
                  Right(Some("it is marked rebuild-required"))
                else
                  CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(finalPath), storeId) match
                    case Right(_) => Right(None)
                    case Left(err) => Right(Some(s"its CRL save file is no longer usable: $err"))
              catch case t: Throwable => Left(t)
            unusable match
              case Left(err) =>
                logger.error(s"Not transferring store $storeId onto device $toDeviceid yet: its arrived copy at " +
                             s"$finalPath could not be checked. Retrying at a later check: $err")
                true
              case Right(None) => false
              case Right(Some(why)) =>
                logger.warn(s"Discarding the arrived copy of store $storeId on device $toDeviceid: $why")
                // Rename-first: a failure leaves the copy whole behind its marker. The source still owns the store
                if StoreCopies.delete(devicePath, finalPath, s"an unusable arrived copy of store $storeId") then
                  toDevice.offlineStores -= storeId
                  ctx.router.clearOfflineUnlessHeld(storeId)
                  false
                else
                  logger.error(s"Not transferring store $storeId onto device $toDeviceid yet: the unusable arrived " +
                               s"copy at $finalPath could not be deleted. Retrying at a later check")
                  true

        // A copy that is unusable but could not be deleted, or could not be checked, must not be resumed: the next
        // check tries again
        if staleKept then
          ()

        // A copy promoted by an earlier attempt -- normally one that crashed before its metadata
        // step finished -- needs no bytes, only that step.
        else if StoreDirMarkers.knownPresent(finalPath, StoreDirMarkers.Marker.TransferredIn) then
          logger.info(s"Store $storeId has already arrived on device $toDeviceid. Resuming its " +
                      s"pool update")
          val transferUUID = UUID.randomUUID()
          transferringIn += storeId -> transferUUID
          recordTransferIn(storeId, transferUUID, fromDeviceId, toDeviceid)

        // A finished rebuild awaiting its flip or a lost copy, left by an earlier tenancy of the store on this device
        // (rebuild-process): retired, and the transfer starts at the check that follows
        else if StoreDirMarkers.knownPresent(finalPath, StoreDirMarkers.Marker.RebuildComplete) ||
                StoreDirMarkers.knownPresent(finalPath, StoreDirMarkers.Marker.RebuildRequired) then
          retireEarlierTenancy(toDevice, storeId)

        // Promotion moves the arrived copy here and would fail on an existing directory, after
        // the whole store had been shipped. Whatever is here is not ours to delete, so refuse
        // before asking for any bytes. A stat that fails is no absence (FileStat): it refuses too, until a later
        // check's stats answer. A marker test above that failed reaches this test, which then fails or finds the path.
        else if !FileStat.existence(finalPath).contains(false) then
          logger.error(s"Cannot transfer store $storeId onto device $toDeviceid: store path " +
                       s"already exists or cannot be stat'd. $finalPath")
        else
          // Fresh per attempt. The sender ignores a request whose timestamp is not newer than the
          // last one it acted on, and the service accepts a connection only for the UUID it is
          // currently registered with, so a retry after a failure needs both to be new.
          val transferUUID = UUID.randomUUID()
          val timestamp = HLCTimestamp.now

          val fcomplete = ctx.storeTransferService.beginTransferIn(
            storeId, transferUUID, devicePath / StoreTransfer.StagingDirectory)

          transferringIn += storeId -> transferUUID

          // Retransmission stops on completion, success or failure, not on first data: the
          // sender needs the request until it has finished, and a failure restarts with a new
          // message below.
          def stopRetransmitting(): Unit =
            if pendingStartTransfers.get(storeId).exists(_.msg.transferUUID == transferUUID) then
              pendingStartTransfers -= storeId

          // False once the attempt has been dropped by dropAbandonedTransferIn, whose cancellation
          // is what completed the future, or by a supersession.
          def isCurrent: Boolean = transferringIn.get(storeId).contains(transferUUID)

          fcomplete.onComplete:
            case Success(_) if ! isCurrent =>
              // The bytes landed as the transfer was abandoned. Nobody owns the staged copy now,
              // and nothing can be receiving this store, since a new attempt needs the
              // transferringIn entry that dropping this one removed.
              if ! transferringIn.contains(storeId) then
                val stagedPath = devicePath / StoreTransfer.StagingDirectory / storeId.directoryName
                try os.remove.all(stagedPath)
                catch case t: Throwable =>
                  logger.error(s"Failed to remove the staged copy of store $storeId at $stagedPath: $t")

            case Failure(_) if ! isCurrent =>
              stopRetransmitting()

            case Success(_) =>
              stopRetransmitting()

              // The transfer-out marker was archived along with the source's store directory and
              // is swapped for the transferred-in marker before the move, so the copy reaches
              // finalPath already marked. Unmarked, a crash before the metadata step finishes
              // would have the restart load a copy the pool does not name, and the device check
              // would then refuse the existing directory instead of resuming that step.
              val stagedPath = devicePath / StoreTransfer.StagingDirectory / storeId.directoryName

              // The copy's CRL save file is checked while the source still holds the state: once
              // the pool update commits, the source deletes its copy and the file is the only one
              // left. The source always writes it when closing the store, so a missing file is as bad as a damaged
              // one, and one that cannot be read (CrashRecoveryLog.isUnreadable) is not promoted either: the source
              // still holds the state, so the transfer restarts
              val crlValid =
                (try CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(stagedPath), storeId)
                 catch case t: Throwable => Left(t)) match
                  case Right(_) => true
                  case Left(err) =>
                    logger.error(s"The transferred copy of store $storeId has an unusable CRL save " +
                                 s"file. Restarting the transfer. Error: $err")
                    try os.remove.all(stagedPath)
                    catch case t: Throwable =>
                      logger.error(s"Failed to remove the staged copy of store $storeId at $stagedPath: $t")
                    false

              val promoted =
                if !crlValid then
                  false
                else
                  try
                    val marker = stagedPath / StoreTransfer.TransferOutMarkerFile
                    if os.exists(marker) then
                      os.remove(marker)
                    // Durable, marker and move both: a copy that lands during a shutdown is
                    // accepted by an adopter on the strength of this marker alone.
                    DurableFiles.write(stagedPath / StoreTransfer.TransferredInMarkerFile,
                                       transferId.map(_.toString).getOrElse(""))
                    DurableFiles.move(stagedPath, finalPath)
                    true
                  catch
                    case t: Throwable =>
                      logger.error(s"Failed to move the transferred store $storeId from " +
                                   s"$stagedPath to $finalPath. Restarting the transfer. Error: $t")
                      false

              if ! promoted then
                // Nothing has been recorded yet, so the pool still names the source and this is
                // no different from a transfer that failed in flight. The restart's extraction
                // replaces the staged copy, and if the failure left a directory at finalPath the
                // restart refuses with an error rather than retrying forever.
                scheduleTransferInRetry(storeId, transferUUID, fromHostId, fromDeviceId, toDeviceid, transferId)
              else
                recordTransferIn(storeId, transferUUID, fromDeviceId, toDeviceid)
            case Failure(_) =>
              stopRetransmitting()
              scheduleTransferInRetry(storeId, transferUUID, fromHostId, fromDeviceId, toDeviceid, transferId)

          val msg = StartStoreTransfer(
            fromHostId,
            ctx.client.clientId,
            fromDeviceId,
            storeId,
            timestamp,
            transferUUID
          )

          pendingStartTransfers += storeId -> new PendingTransfer(msg, toDeviceid, HLCTimestamp.now)

          ctx.client.sendHostMessage(msg)

  /** Clears the path of a store transferring onto `local` that holds a copy of an earlier tenancy of the store there:
   *  a finished rebuild awaiting its flip (`rebuild-complete`) or a lost copy (`rebuild-required`), left when the
   *  store was re-placed away (AspenClient.replaceRebuild) while this host was down. No other pass clears it while
   *  the entry is TransferringIn. The copy is retired (StoreCopies.retire), not deleted: it is not the owner's, and
   *  the source still is. Once the path is clear the device is checked again, which starts the transfer.
   *
   *  Retired before any bytes are asked for, so before the transfer's metadata step can make the entry Active:
   *  a rebuild's flip finding an Active entry adopts it without an episode check (DeviceMetadata.markRebuiltStoreActive).
   *  For the same reason it waits while a rebuild of the store runs here: the reconcile cancels one whose entry has
   *  moved (StoreRebuilds.abandonUnlisted), and one past its latch could still move a copy into the path. A copy
   *  served from this path, or a store still opening, waits too: whatever path took it out of service retires or
   *  reloads it. The store served from another device of this host does not: that is the source of a transfer
   *  between two devices of one host, closed only by the StartStoreTransfer sent once this path is clear. */
  private def retireEarlierTenancy(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    val storePath = local.devicePath / storeId.directoryName
    val servedHere = ctx.router.entry(storeId).exists(_.store.backend.path == storePath)
    if ctx.rebuilds.rebuildingStoreIds.contains(storeId) || servedHere || ctx.loader.isOpening(storeId) then
      logger.info(s"Not transferring store $storeId onto device ${local.storageDeviceId} yet: its path holds a copy " +
                  s"of an earlier tenancy, which waits for a rebuild, an open or a close of the store here to end")
    else if !ctx.copies.isRetiring(storeId) then
      logger.warn(s"Retiring the copy of an earlier tenancy of store $storeId at its path on device " +
                  s"${local.storageDeviceId} before transferring the store onto it")
      ctx.copies.retire(local, storeId).foreach: clear =>
        if clear then
          local.loadedStores -= storeId
          local.offlineStores -= storeId
          ctx.router.clearOfflineUnlessHeld(storeId)
          ctx.reconciler.checkStorageDevice(local.storageDeviceId)

  /** Drops a TransferringIn entry whose source device has been declared failed.
   *
   *  The store itself is not lost: FailedStorageDeviceDurableTask's drain owns the source's
   *  TransferringOut entry and rebuilds the store onto a live device. This only clears the
   *  destination's half, which that drain deliberately does not touch.
   *
   *  The two cannot fight. The drain writes the source device, the pool, and the rebuild
   *  destination it selected; this writes only this device. Where they coincide -- the drain
   *  happening to select this device -- both writes carry a KeyRevision on the same state key,
   *  so one loses and re-reads: if the drain wins, the entry reads Rebuilding and the guard
   *  below leaves it alone; if this wins, the drain's Rebuilding entry simply re-adds the store.
   */
  def abandonIn(toDeviceId: StorageDeviceId,
                storeId: StoreId,
                fromDeviceId: StorageDeviceId): Unit =
    // The entry that triggers this stands until the transaction commits, so every device check
    // in the meantime calls here again. Without the guard each one starts its own retry loop,
    // all writing the same key and losing to each other on KeyRevision. Same shape as
    // createNewStore's creatingStores and startIn's transferringIn.
    if ! abandoningTransferIns.contains(storeId) then
      abandoningTransferIns += storeId

      logger.warn(s"Abandoning transfer of store $storeId onto device $toDeviceId: source " +
                  s"device $fromDeviceId has been declared failed")

      // Nothing consumes this Future, so retrying a permanent error would be a silent 60 s loop
      // for the life of the process -- and, with the guard above set, one that no later
      // check could ever displace. Only a collision is retried.
      // The transaction's own machinery (commit, retries) runs on ctx.ec, and its result comes back through
      // the control-context onComplete; the prepare closure's combinators run on the control thread
      val fabandon = ctx.client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision) { tx =>
        for
          ptr <- ctx.client.getStorageDevicePointer(toDeviceId)
          kvos <- ctx.client.read(ptr)
          state = StorageDeviceState(kvos)
        yield
          // Re-read inside the transaction. A concurrent check may have dropped the entry already,
          // and the entry may have moved on to Rebuilding or Active, neither of which is ours to
          // remove.
          state.stores.get(storeId).foreach: entry =>
            if entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
               entry.transferDevice.contains(fromDeviceId) then
              val newState = state.removeStore(storeId)
              tx.update(ptr, None, None,
                List(KeyRevision(StorageDeviceState.StateKey,
                  kvos.contents(StorageDeviceState.StateKey).revision)),
                List(Insert(StorageDeviceState.StateKey, newState.encode())))
      }(using ctx.ec)

      // Cleared on failure as well as success. A StopRetrying means this attempt gave up, and the
      // next device check -- an hour out, and possibly after whatever made the read fail has been
      // repaired -- should be free to try again rather than find the flag stuck forever.
      fabandon.onComplete: result =>
        abandoningTransferIns -= storeId
        // Whether or not this transaction was the one to remove the entry, it is gone (or has
        // moved on to Rebuilding), so nothing will complete this host's attempt. Its registration
        // has no timeout of its own and its retransmits go to a host that does not exist.
        if result.isSuccess then
          dropAbandonedTransferIn(storeId, fromDeviceId, toDeviceId)
