package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.{StartStoreTransfer, StoreOffline}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.Host.StoreFailedOnClose
import org.aspen_ddp.aspen.server.host.DeviceMetadata.TransferOutcome
import org.aspen_ddp.aspen.server.DurableFiles
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

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
 *  Both ends of a store transfer on this host: receiving a store from another device (transfer in), shipping one
 *  away (transfer out), and the bookkeeping that lets a shutdown, an abort or a failed source cancel either.
 *
 *  Metadata and store loading go through the
 *  overridable seams on Host (via `ctx`). */
class StoreTransfers(ctx: DeviceControl) extends Logging:
  import ctx.given

  // Stores marked rebuild-required whose transfer out returnLostTransferToRebuild is aborting.
  private var abortingLostTransfers: Set[StoreId] = Set()
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

  /** Takes a store out of service for a transfer out: closes it if it is open, else nothing.
   *  This is the default of Host.closeStoreForTransferOut, the seam a test replaces. */
  def closeForTransferOut(storeId: StoreId): Future[Unit] =
    if ctx.router.loaded.contains(storeId) then ctx.loader.closeStore(storeId) else Future.unit

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
   *  transferred-in pass to try again.
   */
  private[host] def clearTransferredInMarker(storeId: StoreId, storePath: os.Path): Boolean =
    try
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
              if clearTransferredInMarker(storeId, sds.devicePath / storeId.directoryName) then
                ctx.loadStoreById(toDeviceid, storeId)
              else
                // Recorded as a restart's scan would have, which is what the transferred-in
                // pass keys off.
                ctx.router.markOffline(storeId)
                sds.offlineStores += storeId

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
        // accepted since, so resuming it would install stale data. Discard it and start over.
        val promotedMarker = finalPath / StoreTransfer.TransferredInMarkerFile
        if os.exists(promotedMarker) then
          val markerId = try os.read(promotedMarker).trim catch case _: Throwable => ""
          if !transferId.exists(_.toString == markerId) then
            logger.warn(s"Discarding the arrived copy of store $storeId on device $toDeviceid: it " +
                        s"belongs to transfer '$markerId', not to the current transfer $transferId")
            try
              os.remove.all(finalPath)
              ctx.router.clearOffline(storeId)
              toDevice.offlineStores -= storeId
            catch case t: Throwable =>
              logger.error(s"Failed to remove the stale arrived copy of store $storeId at $finalPath: $t")

        // A copy promoted by an earlier attempt -- normally one that crashed before its metadata
        // step finished -- needs no bytes, only that step.
        if os.exists(finalPath / StoreTransfer.TransferredInMarkerFile) then
          logger.info(s"Store $storeId has already arrived on device $toDeviceid. Resuming its " +
                      s"pool update")
          val transferUUID = UUID.randomUUID()
          transferringIn += storeId -> transferUUID
          recordTransferIn(storeId, transferUUID, fromDeviceId, toDeviceid)

        // Promotion moves the arrived copy here and would fail on an existing directory, after
        // the whole store had been shipped. Whatever is here is not ours to delete, so refuse
        // before asking for any bytes.
        else if os.exists(finalPath) then
          logger.error(s"Cannot transfer store $storeId onto device $toDeviceid: store path " +
                       s"already exists. $finalPath")
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
              // left. The source always writes it when closing the store, so a missing file is as
              // bad as a damaged one.
              val crlValid =
                CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(stagedPath), storeId) match
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
                    os.move(stagedPath, finalPath)
                    DurableFiles.syncDirectory(devicePath)
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
      // drained by FailedStorageDeviceDurableTask, which this path predates.
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
        // Ruling 25: a store marked for rebuild (its backend failed, here during an earlier
        // request's close) has no trustworthy state to ship, and the destination would refuse
        // its missing CRL save file forever. It goes back to the rebuild path instead.
        returnLostTransferToRebuild(m.storeId, sourceDs)
        err(s"Not starting the transfer out of store ${m.storeId}: it is marked rebuild-required")
      if devEntry.status == StorageDeviceState.StoreStatus.TransferringOut then
        // A retransmitted request finds the store already closed, and its going offline already gossiped
        val wasLoaded = ctx.router.loaded.contains(m.storeId)
        val fclosed = ctx.closeStoreForTransferOut(m.storeId)

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
          // on a device being shelved. The store is left closed and unmarked, which the
          // shutdown's closeStoresForShutdown picks up and marks like its own. A device
          // unloaded meanwhile -- its shutdown already finished -- is not ours to send from.
          if ctx.shutdown.isShuttingDown(sourceDs.storageDeviceId) ||
             !ctx.devices.devices.get(sourceDs.storageDeviceId).contains(sourceDs) then
            logger.info(s"Not starting the transfer out of store ${m.storeId}: source device " +
                        s"${sourceDs.storageDeviceId} began shutting down while the store closed")
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

  /** Ruling 25: takes a TransferringOut store marked rebuild-required back to the rebuild path. The transfer is
   *  aborted (AspenClient.abortStoreTransfer: the source entry back to Active, or Offline on a device shutting
   *  down, and the destination's TransferringIn entry removed), and the device is checked, so the reconcile's
   *  lost-CRL pass flips the now Active entry to Rebuilding. One abort at a time per store: retransmits arriving
   *  meanwhile do nothing more. */
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
        if ctx.devices.devices.get(sds.storageDeviceId).exists(_ eq sds) then
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
   *  StoreMoved is gossiped from tryLoadStore's onServed, once the store is served: at once for RocksDB, and for
   *  FSIF when its open, which runs off the control thread, has succeeded. A store whose open fails is not gossiped.
   *
   *  If the load fails after the marker is gone, tryLoadStore's own catch logs it and the store
   *  stays in offlineStores for the life of the process: this pass will not pick it up again,
   *  since it keys off the marker that is now removed. The next restart loads it normally. That
   *  is a far smaller failure than the permanent, silent one this pass exists to close.
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
      // for the life of the process -- and now that the guard above is set, one that no later
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
