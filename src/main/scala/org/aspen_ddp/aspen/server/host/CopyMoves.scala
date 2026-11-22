package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import scribe.Logging

import java.util.UUID
import scala.util.control.NonFatal

/** The outcome of a CopyMoves move */
enum MoveResult:
  /** Moved, and forced in both directories */
  case Durable
  /** Renamed; a directory sync failed. The record takes the phase, unsynced, until a device check forces it */
  case Unsynced(cause: Throwable)
  /** Nothing changed; the issuing pass retries */
  case NotMoved(cause: Throwable)

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Every phase change of a copy between phase directories (spec "Uniform move and Unsynced"): the target phase's own
 *  parameter file (CopyLayout.paramFile) written durably first, when it carries one, then the rename through
 *  DurableFiles.move, which forces the directories that name the copy before and after. A rename that landed but was not forced leaves the copy
 *  unsynced (CopyRecord.unsynced, the one unsynced mechanism, which CopyMarkers' marker changes share): it is never
 *  loaded, shipped or released until a device check forces both directories again (`resync`, at the top of
 *  DeviceReconciler.reconcileDeviceState). A rename that failed changes nothing.
 *
 *  While flat marker copies remain (stage 2, Tasks 1-5), marker files inside a copy in a phase directory are inert:
 *  its location is its phase. A copy moved from the flat path into a phase directory is stripped of its markers once
 *  the move is durable (best effort, logged); one moved from a phase directory to the flat path is stripped first,
 *  durably, and a strip that fails is a NotMoved. A marker is never removed before the copy leaves the flat path: a
 *  crash would leave an unmarked flat copy, i.e. Live. */
final class CopyMoves(ctx: DeviceControl) extends Logging:

  /** How a move, a marker site (CopyMarkers) and a resync force a directory. Replaced only by tests */
  private[server] var syncDirectory: os.Path => Unit = DurableFiles.syncDirectory

  /** How a move renames a copy. Replaced only by tests */
  private[server] var rename: (os.Path, os.Path) => Unit = DurableFiles.atomicRename

  // Each unsynced move's copy, by its new path: the directories it still needs forced (its source's and target's),
  // and whether it left the flat path, so its inert markers are stripped once the move is forced
  private var unforced: Map[os.Path, (List[os.Path], Boolean)] = Map()

  // The copies whose move to Lost failed (StoreRebuilds.moveToLost), by device: held offline, never loaded, shipped
  // or retired, until a device check moves them (retryPendingLost). In memory only: a crash forgets the hold, and the
  // copy then reloads from where it is, as after any crash (P5)
  private var lostPending: Map[StorageDeviceId, Set[StoreId]] = Map()

  /** The stores of `local` whose move to Lost failed and is retried at each device check */
  def pendingLost(local: LocalStorageDeviceState): Set[StoreId] =
    if ctx.copyRegistry.copiesOf(local).isDefined then lostPending.getOrElse(local.storageDeviceId, Set()) else Set()

  /** Whether the move to Lost of the copy of `storeId` on `local` failed and awaits its retry */
  def isPendingLost(local: LocalStorageDeviceState, storeId: StoreId): Boolean = pendingLost(local).contains(storeId)

  /** Holds the copy of `storeId` on `local` offline until its move to Lost succeeds (`held`), or releases it */
  private[host] def holdLost(local: LocalStorageDeviceState, storeId: StoreId, held: Boolean): Unit =
    val id = local.storageDeviceId
    val now = lostPending.getOrElse(id, Set())
    val next = if held then now + storeId else now - storeId
    lostPending = if next.isEmpty then lostPending - id else lostPending + (id -> next)

  /** The device `id` was unloaded: its holds go with its records */
  private[host] def detached(id: StorageDeviceId): Unit =
    lostPending -= id

  /** Moves the copy of `storeId` on the attached `sds` to `to`, recording the fact (HostCopies.moved). A copy with no
   *  copyId gets a fresh one (P4). A copy that is opening, served or closing is never moved (no move of an installed
   *  store's directory): refused, NotMoved, and logged at error as the caller's bug. A copy already unsynced stays
   *  unsynced after the move, whose syncs force only the directories naming it, until a resync forces it whole */
  def move(site: String, sds: LocalStorageDeviceState, storeId: StoreId, to: Phase): MoveResult =
    ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(storeId)) match
      case None =>
        val t = IllegalStateException(s"No record of a copy of $storeId on device ${sds.storageDeviceId} to move")
        logger.error(s"Copy move ($site): $t")
        MoveResult.NotMoved(t)
      case Some(r) if r.activity.isInstanceOf[Activity.Opening] || r.activity == Activity.Served ||
                      r.activity.isInstanceOf[Activity.Closing] =>
        val t = IllegalStateException(s"The copy of $storeId on device ${sds.storageDeviceId} is ${r.activity}: " +
                                      s"its directory is never moved while its store is installed or opening")
        logger.error(s"Copy move ($site), a bug: $t")
        MoveResult.NotMoved(t)
      case Some(r) =>
        val copyId = r.copyId.getOrElse(UUID.randomUUID())
        val from = CopyLayout.path(sds.devicePath, storeId, r.copyId, r.phase)
        val target = CopyLayout.path(sds.devicePath, storeId, Some(copyId), to)
        // An earlier unsynced change's directories still to force go with the copy to its new path
        val earlier = unforced.get(from)
        val result = relocateCopy(site, from, CopyLayout.dirOf(r.phase).isEmpty, target, to)
        result match
          case MoveResult.NotMoved(_) => ()
          case _ =>
            earlier.foreach: (dirs, leftFlat) =>
              unforced -= from
              val (now, nowLeft) = unforced.getOrElse(target, (Nil, false))
              unforced += target -> ((dirs ++ now).distinct, leftFlat || nowLeft)
            ctx.copyRegistry.moved(site, sds, storeId, to, copyId,
              unsynced = result.isInstanceOf[MoveResult.Unsynced])
        result

  /** As `move`, for a copy of a device being loaded (lost CRL, adoption), at `from` as `copyId` (None while flat):
   *  returns the copy's new path, and an unsynced result is held for the device's attach, as an unsynced marker change
   *  of such a device is (HostCopies.unsynced) */
  def moveUnattached(site: String, from: os.Path, storeId: StoreId, copyId: Option[UUID],
                     to: Phase): (MoveResult, os.Path) =
    // A copy in a phase directory is named for its copy; a flat one for its store
    val fromFlat = CopyLayout.parseName(from.last).isEmpty
    val devicePath = if fromFlat then from / os.up else from / os.up / os.up
    val target = CopyLayout.path(devicePath, storeId, Some(copyId.getOrElse(UUID.randomUUID())), to)
    val result = relocateCopy(site, from, fromFlat, target, to)
    result match
      case MoveResult.Unsynced(_) => ctx.copyRegistry.unsynced(site, target)
      case _ => ()
    (result, target)

  /** Forces again each unsynced copy's source and target directories, and the copy's own (the device check). Returns
   *  the stores synced now */
  def resync(local: LocalStorageDeviceState): List[StoreId] =
    ctx.copyRegistry.unsyncedCopies(local).filter: storeId =>
      ctx.copyRegistry.pathOf(local, storeId).exists: path =>
        try
          val (dirs, leftFlat) = unforced.getOrElse(path, (Nil, false))
          (dirs :+ path).foreach(syncDirectory)
          unforced -= path
          if leftFlat then
            stripInert(path)
          ctx.copyRegistry.synced(path)
          true
        catch case NonFatal(t) =>
          logger.warn(s"Failed to force $path again. Its copy stays unsynced and unloaded. Error: $t")
          false

  /** `relocate` for a copy in `fromPhase` moving to `to`, logged */
  private def relocateCopy(site: String, from: os.Path, fromFlat: Boolean, target: os.Path, to: Phase): MoveResult =
    val toFlat = CopyLayout.dirOf(to).isEmpty
    if fromFlat && toFlat then
      // Between two flat phases the markers move it (CopyMarkers), never a rename
      MoveResult.NotMoved(IllegalArgumentException(s"$to is the flat path $from, where the copy already is"))
    else
      val result = relocate(from, target, fromFlat, toFlat, to)
      result match
        case MoveResult.Durable => logger.info(s"Moved the copy at $from to $target ($site)")
        case MoveResult.Unsynced(t) =>
          logger.error(s"Moved the copy at $from to $target ($site), but the move could not be forced. The copy stays " +
                       s"unsynced until a later check of its device forces it. Error: $t")
        case MoveResult.NotMoved(t) =>
          logger.error(s"Failed to move the copy at $from to $target ($site). It stays where it is. Error: $t")
      result

  /** The steps of a move from `from` to `target`, into phase `to`: the target's directory made if absent, the
   *  markers stripped when leaving a phase directory for the flat path, `to`'s own parameter file placed durably (the
   *  source phase's file is never touched, so a failed move leaves the source reading what it read), the rename
   *  forced, then the markers stripped when the copy left the flat path for a phase directory and the move is
   *  durable */
  private[host] def relocate(from: os.Path, target: os.Path, fromFlat: Boolean, toFlat: Boolean,
                             to: Phase): MoveResult =
    val prepared =
      try
        val dir = target / os.up
        if !FileStat.exists(dir) then
          os.makeDir.all(dir)
          syncDirectory(dir / os.up)
        if !fromFlat && toFlat && stripMarkers(from) then
          syncDirectory(from)
        for
          file <- CopyLayout.paramFile(to)
          p <- CopyLayout.param(to)
        do DurableFiles.write(from / file, p)
        None
      catch case NonFatal(t) => Some(MoveResult.NotMoved(t))
    prepared.getOrElse:
      try
        DurableFiles.move(from, target, syncDirectory, rename)
        if fromFlat && !toFlat then
          stripInert(target)
        MoveResult.Durable
      catch
        case t: DurableFiles.MoveNotForced =>
          unforced += target -> (List(target / os.up, from / os.up).distinct, fromFlat && !toFlat)
          MoveResult.Unsynced(t)
        case NonFatal(t) => MoveResult.NotMoved(t)

  /** Strips the markers of a copy now durably in a phase directory, where they are inert: best effort, logged */
  private def stripInert(path: os.Path): Unit =
    try stripMarkers(path)
    catch case NonFatal(t) => logger.warn(s"Failed to strip the inert markers of $path: $t")

  /** Removes every marker file from the copy at `path`. Returns whether any was there */
  private def stripMarkers(path: os.Path): Boolean =
    StoreDirMarkers.Marker.values.toList.map(m => os.remove(path / m.fileName, checkExists = false)).exists(identity)
