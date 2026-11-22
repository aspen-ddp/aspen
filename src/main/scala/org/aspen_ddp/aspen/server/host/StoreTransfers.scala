package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.host.DeviceMetadata.TransferOutcome
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

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

  /** What startIn does for the copy of the store on the receiving device */
  private[host] enum Next:
    case Wait, Resume, Retire, Receive

  /** The staging directory of receive attempt `transferUUID` on the device at `devicePath`,
   *  `<device>/transferring-in/<transferUUID>` (P7): the service extracts the store into `<that>/<storeDir>`. Each attempt
   *  has its own, removed whole with its attempt, so no attempt ever sees another's files */
  def stagingOf(devicePath: os.Path, transferUUID: UUID): os.Path =
    devicePath / StoreTransfer.StagingDirectory / transferUUID.toString

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

  // A transfer in's work is its store's Work.TransferIn on the receiving device (the copy registry): its receive
  // attempt (`attempt`, the transfer UUID of the attempt this host is running: from its start until its pool update is
  // done, it is forgotten or it is restarted; stores being shipped out are tracked by storeTransferService, which is
  // what knows when a send has ended), and its stage: RetiringEarlierTenancy, Receiving, Recording (its pool update)
  // or Abandoning (the source failed). An abandon overlays a receive or a pool update and ends back at whichever
  // still runs.
  //
  // The pool updates of transfers in now running (recordTransferIn), with the device receiving each, completed once
  // its outcome has been acted on: a shutdown of that device waits on them. Work cannot hold the promise, and an
  // abandon's stage may overlay the Recording stage, so it is kept here
  private var recordingTransfersIn: Map[StoreId, (StorageDeviceId, Promise[Unit])] = Map()
  private[host] var pendingStartTransfers: Map[StoreId, PendingTransfer] = Map()
  // The restart of each failed transfer in that is waiting out TransferInRetryDelay. Its attempt stays meanwhile, so
  // device checks do not start a parallel one.
  private[server] var transferInRetries: Map[StoreId, TransferInRetry] = Map()

  private def registry: HostCopies = ctx.copyRegistry

  /** The receive attempts this host holds, by store: the attempt's transfer UUID (Work.TransferIn's attempt) */
  private[server] def receiving: Map[StoreId, UUID] = registry.receiveAttempts.view.mapValues(_._2).toMap

  /** The receive attempt of `storeId`, on any device of the host */
  private def attemptOf(storeId: StoreId): Option[UUID] = registry.receiveAttempts.get(storeId).map(_._2)

  private def inWork(storeId: StoreId, deviceId: StorageDeviceId): Option[Work.TransferIn] =
    registry.transferInOn(deviceId, storeId)

  /** Sets the store's TransferIn work on `deviceId` to exactly `w` (None ends it). A claim: false where other work
   *  holds the (device, store), recorded */
  private def setWork(storeId: StoreId, deviceId: StorageDeviceId, w: Option[Work.TransferIn]): Boolean =
    registry.work("transfer in", deviceId, storeId, _.isInstanceOf[Work.TransferIn], w, exact = true)

  /** Claims the store's TransferIn work on `deviceId` as `w` before a transfer in's step starts (I3). False if other
   *  work holds it (recorded), and then nothing starts; a later check asks again */
  private def claimWork(storeId: StoreId, deviceId: StorageDeviceId, w: Work.TransferIn): Boolean =
    val claimed = setWork(storeId, deviceId, Some(w))
    if !claimed then
      logger.error(s"Not transferring store $storeId onto device $deviceId yet: the copy registry refused its work")
    claimed

  /** The store's TransferIn work on `deviceId` after one of its steps ended, `w` its work with that step taken out:
   *  an abandon stays while it runs (`abandoning`), then a pool update still running (Recording), then the receive
   *  attempt still held (Receiving); otherwise it ends */
  private def restage(storeId: StoreId, deviceId: StorageDeviceId, w: Work.TransferIn, abandoning: Boolean): Unit =
    val stage =
      if abandoning then Some(TransferInStage.Abandoning)
      else if recordingTransfersIn.get(storeId).exists(_._1 == deviceId) then Some(TransferInStage.Recording)
      else if w.attempt.isDefined then Some(TransferInStage.Receiving)
      else None
    val next: Option[Work.TransferIn] = stage.map(s => new Work.TransferIn(w.transferId, s, w.attempt))
    if inWork(storeId, deviceId) != next then
      setWork(storeId, deviceId, next): Unit

  /** The receive attempt `attempt` of `storeId`, on whichever device holds it, is forgotten (None: whichever it is) */
  private def dropAttempt(storeId: StoreId, attempt: Option[UUID] = None): Unit =
    registry.receiveAttempts.get(storeId).foreach: (deviceId, held) =>
      if attempt.forall(_ == held) then
        inWork(storeId, deviceId).foreach: w =>
          restage(storeId, deviceId, Work.TransferIn(w.transferId, w.stage, None), w.stage == TransferInStage.Abandoning)

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
      val uuid = attemptOf(storeId)
      dropAttempt(storeId)
      uuid.map(storeId -> _)

    (ins, recording)

  /** Re-sends the StartStoreTransfer of every transfer in that has not yet completed and was last
   *  sent more than 30 s ago. `force` skips the age check so tests need not wait it out.
   *
   *  Each re-send also re-reads the source device, and if the device now names another host the
   *  request is re-addressed and sent again there: a source shut down and adopted elsewhere
   *  would otherwise be asked for at its old host for as long as the attempt lives, since the
   *  reconcile's TransferringIn pass never starts a second attempt while this one holds the store's
   *  attempt. The re-send itself stays synchronous and goes to the address already
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
      .foreach(sds => ctx.copyRegistry.removed("discardArrivedTransferIn", sds, storeId))

  /** Restarts a failed transfer in after TransferInRetryDelay rather than at once. A deterministic
   *  failure (the sender's archiver exiting non-zero, a full disk here) would otherwise loop hot,
   *  re-shipping the store each time, since every restart supersedes the sender's attempt at once.
   *
   *  The store's attempt stays until the retry fires, so device checks in the meantime
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
      transferInRetries.contains(storeId) && attemptOf(storeId).contains(transferUUID)

    def restart(entryState: scala.util.Try[StorageDeviceState], sourceHostId: HostId): Unit =
      if current then
        transferInRetries -= storeId
        dropAttempt(storeId, Some(transferUUID))
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
      attemptOf(storeId) match
        case Some(transferUUID) if pending.nonEmpty || retry.nonEmpty =>
          logger.info(s"Dropping the abandoned transfer in of store $storeId from device $fromDeviceId")
          pendingStartTransfers -= storeId
          transferInRetries -= storeId
          dropAttempt(storeId, Some(transferUUID))
          // The attempt's staging directory goes with it (a retry's was removed when its attempt failed; this is
          // only a safety net). It is the attempt's own, so nothing else can be using it
          ctx.devices.devices.get(toDeviceId).foreach(sds => removeStaging(sds.devicePath, storeId, transferUUID))
          Some(transferUUID)
        case _ => None
    cancelled.foreach(ctx.storeTransferService.cancelTransferIn(storeId, _))

  /** Removes the staging directory of receive attempt `transferUUID` of `storeId` on the device at `devicePath`,
   *  whole: logged, never thrown, as the device's next load sweeps `transferring-in/` anyway */
  private def removeStaging(devicePath: os.Path, storeId: StoreId, transferUUID: UUID): Unit =
    val staging = stagingOf(devicePath, transferUUID)
    try os.remove.all(staging)
    catch case NonFatal(t) =>
      logger.error(s"Failed to remove the staging directory $staging of the transfer in of store $storeId: $t")

  /** Moves the Arrived copy of `storeId` on `sds` to Live (T9), once its pool update has committed. Its arrival
   *  (StoreLoader.ArrivedAtFile) is written durably first, while the copy is still Arrived and never loaded: it came
   *  from another host, whose clock may lead this one's, and its load floor must cover that. A failed write or move
   *  (NotMoved) leaves the copy Arrived and offline, for the Completed Transfers In pass to retry; an Unsynced move
   *  leaves it Live and unsynced, which the next device check forces and loads (DeviceReconciler.loadResynced). A copy
   *  with no Arrived record is not moved (NotMoved) */
  private[host] def bringArrivedLive(sds: LocalStorageDeviceState, storeId: StoreId): MoveResult =
    registry.copiesOf(sds).flatMap(_.copies.get(storeId)) match
      case Some(r) if r.phase.isInstanceOf[Phase.Arrived] =>
        val path = CopyLayout.path(sds.devicePath, storeId, r.copyId, r.phase)
        try
          StoreLoader.writeArrivedAt(path)
          ctx.moves.move("arrived copy goes live", sds, storeId, Phase.Live)
        catch case NonFatal(t) =>
          logger.error(s"Failed to record the arrival of store $storeId at $path before moving it live. The store " +
                       s"stays offline. Error: $t")
          MoveResult.NotMoved(t)
      case other =>
        val t = IllegalStateException(s"The copy of store $storeId on device ${sds.storageDeviceId} is not Arrived " +
                                      s"(${other.map(_.phase)})")
        logger.info(s"Not moving store $storeId live: $t")
        MoveResult.NotMoved(t)

  /** Runs the metadata step for a transferred store that has been promoted to its final path,
   *  then loads or discards the arrived copy according to the outcome. Reached both straight
   *  after promotion and, via startIn, for a copy promoted before a crash.
   *
   *  `transferUUID` is the store's receive attempt, which is dropped only once nothing
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
    // Recording, unless an abandon overlays it (its end comes back to Recording while this runs)
    inWork(storeId, toDeviceid).foreach: w =>
      if w.stage != TransferInStage.Abandoning then
        setWork(storeId, toDeviceid, Some(Work.TransferIn(w.transferId, TransferInStage.Recording, w.attempt)))

    def finished(): Unit =
      if recordingTransfersIn.get(storeId).exists(_._2 eq settled) then
        recordingTransfersIn -= storeId
      inWork(storeId, toDeviceid).foreach: w =>
        val attempt = if w.attempt.contains(transferUUID) then None else w.attempt
        restage(storeId, toDeviceid, Work.TransferIn(w.transferId, w.stage, attempt), w.stage == TransferInStage.Abandoning)
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
              // A shipped Rebuilt copy (an Unconfirmed flip's) arrives as any other: its phase-param files are inert
              // here, and it goes Live with the move out of arrived/ (T9)
              if bringArrivedLive(sds, storeId) == MoveResult.Durable then
                ctx.loadStoreById(toDeviceid, storeId)
              // Otherwise its copy stays offline on its device: Arrived, which the Completed Transfers In pass moves,
              // or Live and unsynced, which the next check forces and loads

          case Success(TransferOutcome.PoolRepointedOffline) =>
            // Left exactly as it arrived, in arrived/, offline as a restart's scan would leave it. Once a
            // restart or adoption makes the entry Active, the reconcile's completed-transfers-in pass moves it
            // Live and loads it. This needs no disk write, so it is the same whether or not the device is
            // still loaded.
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
            // exactly as silent as the infinite retry loop it replaced. The copy stays Arrived and the
            // destination's entry TransferringIn, so the next device check resumes this step; until one
            // succeeds the source's copy stays offline in its outbound/.
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

    // One piece of work per (device, store) at a time: while rebuild work of the store is registered on the device
    // (queued, discarding its lost copy, running, or cancelled and not yet ended: one past finish's latch can still
    // move a copy into the path), nothing is received. The re-placement that moved the entry away cancels it
    // (StoreRebuilds.abandonUnlisted, which the reconcile runs before this pass), and its end checks the device again.
    if ctx.rebuilds.hasRebuildWork(storeId, toDeviceid) then
      logger.debug(s"Not transferring store $storeId onto device $toDeviceid yet: rebuild work of the store is " +
                   "still registered there. Retrying at a later check")

    // The shutdown guard covers a source read issued before this device's shutdown began, and a
    // retry whose delay ran out during it: the shutdown has already cancelled this device's
    // transfers in and would not see a new one.
    // Nor while an abandon or an earlier tenancy's retire of the store runs on the device: its end checks the device
    // again (an abandon is for a failed source, which never sends)
    else if inWork(storeId, toDeviceid).nonEmpty then
      logger.debug(s"Not transferring store $storeId onto device $toDeviceid yet: other transfer-in work of the store " +
                   "is still running there")

    else if attemptOf(storeId).isEmpty && ! ctx.shutdown.isShuttingDown(toDeviceid) then
      ctx.devices.devices.get(toDeviceid).foreach: toDevice =>
        val devicePath = toDevice.devicePath
        val record = registry.copiesOf(toDevice).flatMap(_.copies.get(storeId))

        // The copy of the store on the device, by its record. An Arrived copy belongs to the transfer its record names
        // (phase-param.arrived, read at the device's load or its promotion). One left by an earlier transfer that was
        // aborted or given up on predates whatever the source has accepted since, so resuming it would install stale
        // data. One whose save file is no longer valid would be repointed onto, and the source's intact copy then
        // released, as promotion refuses for a staged copy. Until the pool update commits the source still holds the
        // state, so either is deleted (T13) and the transfer started over. A save file that cannot be read
        // (CrashRecoveryLog.isUnreadable), or a delete that could not stat the copy (StoreCopies.delete answers not
        // gone), says nothing of the copy: it is neither resumed nor deleted, and the next check reads it again.
        // A copy in any other phase is an earlier tenancy's, retired before the receive (retireEarlierTenancy).
        val next: Next = record match
          case None => Next.Receive
          // A retired copy still recorded awaits the resync that forces its move (CopyMoves)
          case Some(r) if r.phase == Phase.Retired => Next.Wait
          // A move a crash could undo (a promotion's included): the next device check forces it first
          case Some(r) if r.unsynced =>
            logger.info(s"Not transferring store $storeId onto device $toDeviceid yet: a move of its copy is not " +
                        "durable. Retrying once a device check forces it")
            Next.Wait
          case Some(r) =>
            r.phase match
              case Phase.Arrived(arrivedId) =>
                val path = CopyLayout.path(devicePath, storeId, r.copyId, r.phase)
                val unusable: Either[Throwable, Option[String]] =
                  try
                    if !transferId.exists(_.toString == arrivedId) then
                      Right(Some(s"it belongs to transfer '$arrivedId', not to the current transfer $transferId"))
                    else
                      CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(path), storeId) match
                        case Right(_) => Right(None)
                        case Left(err) => Right(Some(s"its CRL save file is no longer usable: $err"))
                  catch case t: Throwable => Left(t)
                unusable match
                  case Left(err) =>
                    logger.error(s"Not transferring store $storeId onto device $toDeviceid yet: its arrived copy at " +
                                 s"$path could not be checked. Retrying at a later check: $err")
                    Next.Wait
                  case Right(None) => Next.Resume
                  case Right(Some(why)) =>
                    logger.warn(s"Deleting the arrived copy of store $storeId on device $toDeviceid: $why")
                    // Rename-first (T13): a failure leaves the copy whole in arrived/. The source still owns the store
                    if StoreCopies.delete(devicePath, path, s"an unusable arrived copy of store $storeId") then
                      ctx.copyRegistry.removed("startIn unusable arrived copy", toDevice, storeId)
                      Next.Receive
                    else
                      logger.error(s"Not transferring store $storeId onto device $toDeviceid yet: the unusable " +
                                   s"arrived copy at $path could not be deleted. Retrying at a later check")
                      Next.Wait
              case _ => Next.Retire

        // The attempt's id, fresh per attempt. The sender ignores a request whose timestamp is not newer than the last
        // one it acted on, and the service accepts a connection only for the UUID it is currently registered with, so
        // a retry after a failure needs both to be new.
        val transferUUID = UUID.randomUUID()

        next match
          case Next.Wait =>
            ()

          // A copy promoted by an earlier attempt -- normally one that crashed before its metadata
          // step finished -- needs no bytes, only that step.
          case Next.Resume =>
            if claimWork(storeId, toDeviceid,
                         Work.TransferIn(transferId, TransferInStage.Receiving, Some(transferUUID))) then
              logger.info(s"Store $storeId has already arrived on device $toDeviceid. Resuming its " +
                          s"pool update")
              recordTransferIn(storeId, transferUUID, fromDeviceId, toDeviceid)

          // A Live, Outbound, Rebuilt, Parked or Lost copy left by an earlier tenancy of the store on this device
          // (re-placed away while this host was down, then transferred back; a store transferred back onto its old
          // source): never the owner's, so retired, and the transfer starts at the check that follows
          case Next.Retire =>
            retireEarlierTenancy(toDevice, storeId, transferId)

          case Next.Receive if !claimWork(storeId, toDeviceid,
                                 Work.TransferIn(transferId, TransferInStage.Receiving, Some(transferUUID))) =>
            ()

          case Next.Receive =>
            val timestamp = HLCTimestamp.now
            val staging = stagingOf(devicePath, transferUUID)
            // What the service extracts (StoreTransferService.beginTransferIn)
            val stagedPath = staging / storeId.directoryName

            val fcomplete = ctx.storeTransferService.beginTransferIn(storeId, transferUUID, staging)

            // Retransmission stops on completion, success or failure, not on first data: the
            // sender needs the request until it has finished, and a failure restarts with a new
            // message below.
            def stopRetransmitting(): Unit =
              if pendingStartTransfers.get(storeId).exists(_.msg.transferUUID == transferUUID) then
                pendingStartTransfers -= storeId

            // False once the attempt has been dropped by dropAbandonedTransferIn, whose cancellation
            // is what completed the future, or by a supersession.
            def isCurrent: Boolean = attemptOf(storeId).contains(transferUUID)

            fcomplete.onComplete:
              case Success(_) if ! isCurrent =>
                // The bytes landed as the transfer was abandoned, or as its device's unload forgot the attempt
                // (crlFailed cancels no transfer in, so no abandon will stop its retransmission). Nobody owns the
                // staged copy now: the attempt's staging goes with it.
                stopRetransmitting()
                removeStaging(devicePath, storeId, transferUUID)

              case Failure(_) if ! isCurrent =>
                stopRetransmitting()
                removeStaging(devicePath, storeId, transferUUID)

              case Success(_) =>
                stopRetransmitting()

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
                      false

                // Promotion (T8): one move of the staged copy into arrived/ under a fresh copy id, its transfer id
                // written durably first as phase-param.arrived (the source's phase-param.outbound, shipped with it, is
                // inert here). Arrived, a crash before the metadata step finishes leaves a copy no restart loads, and
                // the device check resumes that step
                val promoted =
                  if !crlValid then
                    MoveResult.NotMoved(IllegalStateException(s"unusable CRL save file at $stagedPath"))
                  else
                    ctx.moves.place("transfer-in promotion", toDevice, storeId, stagedPath,
                      Phase.Arrived(transferId.map(_.toString).getOrElse("")))

                promoted match
                  case MoveResult.Durable =>
                    removeStaging(devicePath, storeId, transferUUID)
                    recordTransferIn(storeId, transferUUID, fromDeviceId, toDeviceid)
                  case MoveResult.Unsynced(_) =>
                    // Arrived, but a crash could undo the rename: no pool update until it is forced. The attempt ends
                    // here, and the device check that follows forces the copy (its staging directory is one of the
                    // directories it forces, so it stays until the device's next load sweeps it) and then resumes
                    // the pool update
                    dropAttempt(storeId, Some(transferUUID))
                    ctx.reconciler.checkStorageDevice(toDeviceid)
                  case MoveResult.NotMoved(_) =>
                    // Nothing has been recorded yet, so the pool still names the source and this is
                    // no different from a transfer that failed in flight. Its staging goes with the attempt, and the
                    // restart receives into its own
                    removeStaging(devicePath, storeId, transferUUID)
                    scheduleTransferInRetry(storeId, transferUUID, fromHostId, fromDeviceId, toDeviceid, transferId)
              case Failure(_) =>
                stopRetransmitting()
                // The service removed what it extracted; the attempt's directory goes too
                removeStaging(devicePath, storeId, transferUUID)
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

  /** Retires the copy of an earlier tenancy of a store transferring onto `local`, in any phase but this transfer's
   *  Arrived (P1): a Live copy, a finished rebuild awaiting its flip (in `rebuilt/`), a parked or outbound copy, or a
   *  lost copy (in `lost/`), left when the store was re-placed away (AspenClient.replaceRebuild) while this host was
   *  down, or a store transferred back onto its old source. No other pass retires it while the entry is TransferringIn.
   *  The copy is retired (StoreCopies.retire: moved to Lost first, T10, "a copy that stopped being the owner's"), not
   *  deleted: it is not the owner's, and the source still is. Once it is retired the device is checked again, which
   *  starts the transfer.
   *
   *  Retired before any bytes are asked for, so before the new copy's first load (a save-file load that drops the
   *  store's device-CRL entries, StoreLoader.awaitCrlLoad) and before the transfer's metadata step can make the entry
   *  Active: a rebuild's flip finding an Active entry adopts it without an episode check
   *  (DeviceMetadata.markRebuiltStoreActive). For the same reason it waits while a rebuild of the store runs here: the
   *  reconcile cancels one whose entry has moved (StoreRebuilds.abandonUnlisted), and one past its latch could still
   *  place a copy. A copy opening or closing on this device, or a store still opening, waits too. A copy served from
   *  this device is not the owner's: it is taken out of service (StoreRebuilds.takeOutOfService) and retired by the
   *  check its close's end makes. The store served from another device of this host is left: that is the source of
   *  a transfer between two devices of one host, closed only by the StartStoreTransfer sent once this copy is
   *  retired. Nothing is acted on before a fresh read of this device's state confirms the store is still
   *  TransferringIn for `transferId` (see the body). */
  private def retireEarlierTenancy(local: LocalStorageDeviceState, storeId: StoreId, transferId: Option[UUID]): Unit =
    val deviceId = local.storageDeviceId
    def storePath: Option[os.Path] = ctx.copyRegistry.pathOf(local, storeId)
    def record: Option[CopyRecord] = registry.copiesOf(local).flatMap(_.copies.get(storeId))
    def openingOrClosing: Boolean =
      ctx.loader.isOpening(storeId) || record.exists(r =>
        r.activity.isInstanceOf[Activity.Opening] || r.activity.isInstanceOf[Activity.Closing])
    def endWork(): Unit =
      if inWork(storeId, deviceId).exists(_.stage == TransferInStage.RetiringEarlierTenancy) then
        setWork(storeId, deviceId, None)
    if registry.rebuilding(HostCopies.RebuildReads).contains(storeId) || openingOrClosing then
      logger.info(s"Not transferring store $storeId onto device $deviceId yet: it holds a copy of an earlier " +
                  s"tenancy, which waits for a rebuild, an open or a close of the store here to end")
      // A rebuild's end and a close's end check the device again; an open's end does not, so a store opening is
      // checked again soon rather than at the periodic check
      if openingOrClosing then
        ctx.reconciler.scheduleDeviceRecheck(deviceId)
    else if !ctx.copies.isRetiring(storeId) && inWork(storeId, deviceId).isEmpty &&
            claimWork(storeId, deviceId, Work.TransferIn(None, TransferInStage.RetiringEarlierTenancy)) then
      // The reconcile's tree read may predate this transfer's own pool update: once that commits, recordTransferIn
      // moves the transfer's copy Live, and a pass reading the earlier TransferringIn entry would take it for an
      // earlier tenancy's. So the device's own state is read again, and the copy acted on only while the store is
      // still TransferringIn for this transfer: the transfer's copy goes Live only after its commit made the entry
      // Active, so a copy found under that fresh read is never the transfer's. The work held meanwhile keeps every
      // other transfer-in step of the store off the device (I3)
      val fstate =
        try ctx.metadata.lookupStorageDeviceState(deviceId)
        catch case NonFatal(t) => Future.failed(t)
      fstate.onComplete: result =>
        val confirmed = result match
          case Success(state) =>
            state.stores.get(storeId).exists: entry =>
              entry.status == StorageDeviceState.StoreStatus.TransferringIn && entry.transferId == transferId
          case Failure(t) =>
            logger.warn(s"Not retiring the copy of store $storeId on device $deviceId yet: its device state could " +
                        s"not be read again. Retrying at a later check. Error: $t")
            // Checked again after a delay, not at once, so a read that keeps failing cannot spin
            ctx.reconciler.scheduleDeviceRecheck(deviceId)
            false
        if !confirmed || !ctx.devices.isLoaded(local) then
          endWork()
          if result.isSuccess && ctx.devices.isLoaded(local) then
            logger.info(s"Not retiring the copy of store $storeId on device $deviceId: its entry is no longer " +
                        s"TransferringIn for transfer $transferId. Checking the device again")
            ctx.reconciler.checkStorageDevice(deviceId)
        else if registry.rebuilding(HostCopies.RebuildReads).contains(storeId) || openingOrClosing then
          // Begun while the state was read: a rebuild's or a close's end comes back here, and a check soon after
          // covers an open's, whose end checks nothing
          endWork()
          ctx.reconciler.scheduleDeviceRecheck(deviceId)
        else if ctx.router.entry(storeId).exists(e => e.store.storageDeviceId == deviceId &&
                                                      storePath.contains(e.store.backend.path)) then
          // A copy served here is not the owner's (the transfer's source is): taken out of service as a failed
          // backend is (a FailStop close, the copy moved to lost/ when the close ends, its CRL entries kept), and the
          // check its close's end makes retires it
          endWork()
          logger.warn(s"Taking the served copy of an earlier tenancy of store $storeId at ${storePath.get} on device " +
                      s"$deviceId out of service before transferring the store onto it")
          ctx.rebuilds.takeOutOfService(local, storeId,
            s"is served from a copy of an earlier tenancy on device $deviceId, which the store is transferring onto")
        else if record.exists(_.activity == Activity.Served) then
          // Served by nothing installed here (a stand-in): left for whatever ends it
          endWork()
        else
          logger.warn(s"Retiring the copy of an earlier tenancy of store $storeId at ${storePath.getOrElse("(none)")} on device " +
                      s"$deviceId before transferring the store onto it")
          ctx.copies.retire(local, storeId).foreach: clear =>
            endWork()
            if clear then
              ctx.reconciler.checkStorageDevice(deviceId)

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
    // createNewStore's Work.Create and startIn's attempt. The abandon overlays a receive or a pool update running
    // there; an earlier tenancy's retire it waits out, as its end checks the device again
    val current = inWork(storeId, toDeviceId)
    if ! registry.transferringIn(storeId, TransferInStage.Abandoning) &&
       ! current.exists(_.stage == TransferInStage.RetiringEarlierTenancy) &&
       claimWork(storeId, toDeviceId, Work.TransferIn(current.flatMap(_.transferId), TransferInStage.Abandoning,
         current.flatMap(_.attempt))) then

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
        inWork(storeId, toDeviceId).foreach: w =>
          if w.stage == TransferInStage.Abandoning then
            restage(storeId, toDeviceId, w, abandoning = false)
        // Whether or not this transaction was the one to remove the entry, it is gone (or has
        // moved on to Rebuilding), so nothing will complete this host's attempt. Its registration
        // has no timeout of its own and its retransmits go to a host that does not exist.
        if result.isSuccess then
          dropAbandonedTransferIn(storeId, fromDeviceId, toDeviceId)
