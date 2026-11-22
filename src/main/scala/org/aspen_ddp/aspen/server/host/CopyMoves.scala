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
 *  unsynced (CopyRecord.unsynced, the one unsynced mechanism): it is never loaded, shipped or released until a device
 *  check forces both directories again (`resync`, at the top of DeviceReconciler.reconcileDeviceState). A rename that
 *  failed changes nothing. */
final class CopyMoves(ctx: DeviceControl) extends Logging:

  /** How a move and a resync force a directory. Replaced only by tests */
  private[server] var syncDirectory: os.Path => Unit = DurableFiles.syncDirectory

  /** How a move renames a copy. Replaced only by tests */
  private[server] var rename: (os.Path, os.Path) => Unit = DurableFiles.atomicRename

  // Each unsynced move's copy, by its new path: the directories it still needs forced (its source's and target's)
  private var unforced: Map[os.Path, List[os.Path]] = Map()

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

  /** Moves the copy of `storeId` on the attached `sds` to `to`, recording the fact (HostCopies.moved). The copy keeps
   *  its copyId (P4). A copy that is opening, served or closing is never moved (no move of an installed
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
        val copyId = r.copyId
        val from = CopyLayout.path(sds.devicePath, storeId, copyId, r.phase)
        val target = CopyLayout.path(sds.devicePath, storeId, copyId, to)
        // An earlier unsynced change's directories still to force go with the copy to its new path
        val earlier = unforced.get(from)
        val result = relocateCopy(site, from, target, to)
        result match
          case MoveResult.NotMoved(_) => ()
          case _ =>
            earlier.foreach: dirs =>
              unforced -= from
              unforced += target -> (dirs ++ unforced.getOrElse(target, Nil)).distinct
            ctx.copyRegistry.moved(site, sds, storeId, to, copyId,
              unsynced = result.isInstanceOf[MoveResult.Unsynced])
        result

  /** Places the copy staged at `from`, in a scratch area (a transfer's staging, T8), into `to` on the attached `sds`
   *  under a fresh copy id (P4), as one move: `to`'s parameter file written durably, then the rename forced. Recorded
   *  as a fact (HostCopies.placed), unsynced for a rename that landed but was not forced. Returns the result */
  def place(site: String, sds: LocalStorageDeviceState, storeId: StoreId, from: os.Path, to: Phase): MoveResult =
    val copyId = UUID.randomUUID()
    val target = CopyLayout.path(sds.devicePath, storeId, copyId, to)
    val result = relocateCopy(site, from, target, to)
    result match
      case MoveResult.NotMoved(_) => ()
      case _ =>
        ctx.copyRegistry.placed(site, sds, storeId, to, copyId, unsynced = result.isInstanceOf[MoveResult.Unsynced])
    result

  /** As `move`, for a copy of a device being loaded (lost CRL, adoption, P2), at `from` as `copyId`:
   *  returns the copy's new path, and an unsynced result is held for the device's attach, as an unsynced move
   *  of such a device is (HostCopies.unsynced) */
  def moveUnattached(site: String, from: os.Path, storeId: StoreId, copyId: UUID, to: Phase): (MoveResult, os.Path) =
    val target = CopyLayout.path(from / os.up / os.up, storeId, copyId, to)
    val result = relocateCopy(site, from, target, to)
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
          (unforced.getOrElse(path, Nil) :+ path).foreach(syncDirectory)
          unforced -= path
          ctx.copyRegistry.synced(path)
          true
        catch case NonFatal(t) =>
          logger.warn(s"Failed to force $path again. Its copy stays unsynced and unloaded. Error: $t")
          false

  /** `relocate` of the copy at `from` to `target`, in `to`, logged */
  private def relocateCopy(site: String, from: os.Path, target: os.Path, to: Phase): MoveResult =
    val result = relocate(from, target, to)
    result match
      case MoveResult.Durable => logger.info(s"Moved the copy at $from to $target ($site)")
      case MoveResult.Unsynced(t) =>
        logger.error(s"Moved the copy at $from to $target ($site), but the move could not be forced. The copy stays " +
                     s"unsynced until a later check of its device forces it. Error: $t")
      case MoveResult.NotMoved(t) =>
        logger.error(s"Failed to move the copy at $from to $target ($site). It stays where it is. Error: $t")
    result

  /** The steps of a move from `from` to `target`, into phase `to`: the target's directory made if absent, `to`'s own
   *  parameter file placed durably (the source phase's file is never touched, so a failed move leaves the source
   *  reading what it read), then the rename forced. A move onto the copy's own path is no move: NotMoved */
  private[host] def relocate(from: os.Path, target: os.Path, to: Phase): MoveResult =
    if from == target then
      MoveResult.NotMoved(IllegalArgumentException(s"$to is the path $from, where the copy already is"))
    else
      renameCopy(from, target, to)

  private def renameCopy(from: os.Path, target: os.Path, to: Phase): MoveResult =
    val prepared =
      try
        val dir = target / os.up
        if !FileStat.exists(dir) then
          os.makeDir.all(dir)
          syncDirectory(dir / os.up)
        for
          file <- CopyLayout.paramFile(to)
          p <- CopyLayout.param(to)
        do DurableFiles.write(from / file, p)
        None
      catch case NonFatal(t) => Some(MoveResult.NotMoved(t))
    prepared.getOrElse:
      try
        DurableFiles.move(from, target, syncDirectory, rename)
        MoveResult.Durable
      catch
        case t: DurableFiles.MoveNotForced =>
          unforced += target -> List(target / os.up, from / os.up).distinct
          MoveResult.Unsynced(t)
        case NonFatal(t) => MoveResult.NotMoved(t)
