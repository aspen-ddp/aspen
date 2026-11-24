package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.DurableFiles
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
    movable(site, sds, storeId) match
      case Left(refused) => refused
      case Right(r) =>
        val from = CopyLayout.path(sds.devicePath, storeId, r.copyId, r.phase)
        val target = CopyLayout.path(sds.devicePath, storeId, r.copyId, to)
        val result = relocateCopy(site, from, target, to)
        recordMove(site, sds, storeId, r.copyId, from, target, to, result)
        result

  /** The record of the copy of `storeId` on `sds` a move may move, or the refusal (NotMoved, logged): no record, or a
   *  copy opening, served or closing (no move of an installed store's directory), or busy with a disk job (another
   *  job holds its disk: the move waits for a later attempt, never runs under it) */
  private def movable(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Either[MoveResult, CopyRecord] =
    ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(storeId)) match
      case None =>
        val t = IllegalStateException(s"No record of a copy of $storeId on device ${sds.storageDeviceId} to move")
        logger.error(s"Copy move ($site): $t")
        Left(MoveResult.NotMoved(t))
      case Some(r) if r.activity.isInstanceOf[Activity.DiskJob] =>
        val t = IllegalStateException(s"The copy of $storeId on device ${sds.storageDeviceId} is ${r.activity}: " +
                                      "a disk job for it is running")
        logger.info(s"Not moving the copy ($site) yet: $t")
        Left(MoveResult.NotMoved(t))
      case Some(r) if r.activity.isInstanceOf[Activity.Opening] || r.activity.underlying == Activity.Served ||
                      r.activity.isInstanceOf[Activity.Closing] =>
        val t = IllegalStateException(s"The copy of $storeId on device ${sds.storageDeviceId} is ${r.activity}: " +
                                      s"its directory is never moved while its store is installed or opening")
        logger.error(s"Copy move ($site), a bug: $t")
        Left(MoveResult.NotMoved(t))
      case Some(r) => Right(r)

  /** Records a move of the copy `copyId` of `storeId` on `sds` from `from` to `target` that reached `result`: an
   *  earlier unsynced change's directories still to force go with the copy to its new path, and the registry takes
   *  the phase (HostCopies.moved). An Unsynced result's own directories were held by `relocate` */
  private def recordMove(site: String, sds: LocalStorageDeviceState, storeId: StoreId, copyId: UUID, from: os.Path,
                         target: os.Path, to: Phase, result: MoveResult): Unit =
    result match
      case MoveResult.NotMoved(_) => ()
      case _ =>
        unforced.get(from).foreach: dirs =>
          unforced -= from
          unforced += target -> (dirs ++ unforced.getOrElse(target, Nil)).distinct
        ctx.copyRegistry.moved(site, sds, storeId, to, copyId, unsynced = result.isInstanceOf[MoveResult.Unsynced])

  /** `move` run as one `Move` job on the device's lane (CopyDisk.relocate), its result given to `andThen` on the
   *  control thread. The copy is claimed DiskJob(Move) before the job is submitted (HostCopies.diskJob; refused as
   *  `move` refuses, or where the claim is refused: NotMoved at once) and its mark cleared in the apply whatever the
   *  outcome. The apply rechecks that the device is still the loaded one and the copy's record the one moved (same copy
   *  id and phase): then it records the move as `move` does. Otherwise, or for a cancelled job, it records nothing
   *  and gives `andThen` NotMoved; a rename that landed unforced on a device unloaded meanwhile is held for its next
   *  attach (HostCopies.unsynced), as moveUnattached holds one. A job that never returns (a hung device) leaves the
   *  copy busy and `andThen` waiting */
  def moveOnLane(site: String, sds: LocalStorageDeviceState, storeId: StoreId, to: Phase)
                (andThen: MoveResult => Unit): Unit =
    movable(site, sds, storeId) match
      case Left(refused) => andThen(refused)
      case Right(r) =>
        if !ctx.copyRegistry.diskJob(sds, storeId, JobKind.Move) then
          andThen(MoveResult.NotMoved(IllegalStateException(s"The copy of $storeId could not be claimed for a move")))
        else
          val from = CopyLayout.path(sds.devicePath, storeId, r.copyId, r.phase)
          val target = CopyLayout.path(sds.devicePath, storeId, r.copyId, to)
          val sync = syncDirectory
          val renameCopy = rename
          val job = sds.io.submit(JobKind.Move, Some(storeId))(CopyDisk.relocate(from, target, to, sync, renameCopy))
          ctx.onControl(s"copy move ($site)", job): done =>
            val result = done match
              case scala.util.Success(m) => m
              case scala.util.Failure(t) => MoveResult.NotMoved(t)
            val current = ctx.devices.isLoaded(sds) &&
              ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(storeId)).exists: now =>
                now.copyId == r.copyId && now.phase == r.phase && now.activity.isInstanceOf[Activity.DiskJob]
            if done != scala.util.Failure(DeviceIo.Cancelled) then
              logMove(site, from, target, result)
            if current then
              result match
                case MoveResult.Unsynced(_) => heldUnforced(from, target)
                case _ => ()
              // Recorded under the mark (a retry beneath it retargeted, HostCopies.moved), then the mark cleared
              recordMove(site, sds, storeId, r.copyId, from, target, to, result)
              ctx.copyRegistry.diskJobEnded(sds, storeId)
              andThen(result)
            else
              ctx.copyRegistry.diskJobEnded(sds, storeId)
              result match
                case MoveResult.Unsynced(_) if !ctx.devices.isLoaded(sds) =>
                  heldUnforced(from, target)
                  ctx.copyRegistry.unsynced(site, target)
                case _ => ()
              andThen(MoveResult.NotMoved(IllegalStateException(
                s"The move of $storeId ($site) ended after its device was unloaded or its copy changed")))

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

  /** A copy placed into a phase directory off the control thread (a rebuild's finish, T2) whose rename landed but was
   *  not forced (`e`): its source's and target's directories are held for the resync, as `place` holds an unsynced
   *  one's. Its record is HostCopies.rebuilt's, unsynced */
  def placedUnforced(e: DurableFiles.MoveNotForced): Unit =
    unforced += e.target -> List(e.target / os.up, e.source / os.up).distinct

  /** As `move`, for a copy of a device not attached (an adoption's, a rebuilt copy kept offline), at `from` as `copyId`:
   *  returns the copy's new path, and an unsynced result is held for the device's attach, as an unsynced move
   *  of such a device is (HostCopies.unsynced) */
  def moveUnattached(site: String, from: os.Path, storeId: StoreId, copyId: UUID, to: Phase): (MoveResult, os.Path) =
    val target = CopyLayout.path(from / os.up / os.up, storeId, copyId, to)
    val result = relocateCopy(site, from, target, to)
    result match
      case MoveResult.Unsynced(_) => ctx.copyRegistry.unsynced(site, target)
      case _ => ()
    (result, target)

  /** The directories still to force of each unsynced move's copy on the device at `devicePath`, by the copy's path:
   *  what a lost device CRL's open (DeviceCrlDisk.open) forces with its lost/ copies, off the control thread */
  private[host] def unforcedUnder(devicePath: os.Path): Map[os.Path, List[os.Path]] =
    unforced.filter((path, _) => path.startsWith(devicePath))

  /** A lost device CRL's open forced each of `copies` (their paths) and their earlier unforced moves' directories:
   *  those moves are done with, and each copy is synced */
  private[host] def forcedUnattached(copies: List[os.Path]): Unit =
    copies.foreach: path =>
      unforced -= path
      ctx.copyRegistry.synced(path)

  // The devices with a resync job outstanding (resync), by path: at most one each
  private var resyncing: Set[os.Path] = Set()

  /** Whether a resync of the device at `devicePath` is outstanding */
  private[server] def resyncRunning(devicePath: os.Path): Boolean = resyncing.contains(devicePath)

  /** Forces again each unsynced copy's source and target directories, and the copy's own (the device check), in one
   *  `Move` job on the device's lane (CopyDisk.resync). Each copy is claimed DiskJob(Move) for the job (one already
   *  busy is left for a later check); a device with a resync outstanding starts none (coalesced). The apply clears
   *  the marks and, for a device still the loaded one, takes each copy whose record is still at the forced path as
   *  synced; a copy whose force failed stays unsynced (logged). `andThen` runs on the control thread once the job's
   *  result is applied, or at once when no job is submitted (nothing to force, or a resync already outstanding):
   *  the device check's decisions run there, so they see the copies synced (DeviceReconciler). A hung device's resync
   *  never applies, and its check waits with it */
  def resync(local: LocalStorageDeviceState)(andThen: => Unit): Unit =
    if resyncing.contains(local.devicePath) then
      andThen
    else
      val registry = ctx.copyRegistry
      val claimed = registry.unsyncedCopies(local).flatMap: storeId =>
        registry.pathOf(local, storeId).filter(_ => registry.diskJob(local, storeId, JobKind.Move))
          .map(path => (storeId, path, unforced.getOrElse(path, Nil) :+ path))
      if claimed.isEmpty then
        andThen
      else
        resyncing += local.devicePath
        val sync = syncDirectory
        val dirs = claimed.map((_, path, ds) => path -> ds)
        ctx.onControl("copy resync", local.io.submit(JobKind.Move, None)(CopyDisk.resync(dirs, sync))): done =>
          resyncing -= local.devicePath
          claimed.foreach((storeId, _, _) => registry.diskJobEnded(local, storeId))
          if ctx.devices.isLoaded(local) then
            val outcomes = done.getOrElse(Nil).toMap
            done.failed.foreach: t =>
              if t != DeviceIo.Cancelled then
                logger.warn(s"Failed to force the unsynced copies of device ${local.storageDeviceId} again: $t")
            claimed.foreach: (storeId, path, _) =>
              outcomes.get(path) match
                case Some(None) if registry.pathOf(local, storeId).contains(path) =>
                  unforced -= path
                  registry.synced(path)
                case Some(Some(t)) =>
                  logger.warn(s"Failed to force $path again. Its copy stays unsynced and unloaded. Error: $t")
                case _ => ()
          andThen

  /** `relocate` of the copy at `from` to `target`, in `to`, logged */
  private def relocateCopy(site: String, from: os.Path, target: os.Path, to: Phase): MoveResult =
    val result = relocate(from, target, to)
    logMove(site, from, target, result)
    result

  private def logMove(site: String, from: os.Path, target: os.Path, result: MoveResult): Unit =
    result match
      case MoveResult.Durable => logger.info(s"Moved the copy at $from to $target ($site)")
      case MoveResult.Unsynced(t) =>
        logger.error(s"Moved the copy at $from to $target ($site), but the move could not be forced. The copy stays " +
                     s"unsynced until a later check of its device forces it. Error: $t")
      case MoveResult.NotMoved(t) =>
        logger.error(s"Failed to move the copy at $from to $target ($site). It stays where it is. Error: $t")

  /** CopyDisk.relocate through this host's seams, an Unsynced result's directories held for the resync */
  private[host] def relocate(from: os.Path, target: os.Path, to: Phase): MoveResult =
    val result = CopyDisk.relocate(from, target, to, syncDirectory, rename)
    result match
      case MoveResult.Unsynced(_) => heldUnforced(from, target)
      case _ => ()
    result

  /** A copy moved from `from` to `target` off the control thread whose rename landed unforced: both directories are
   *  held for the resync, as `relocate` holds them */
  private[host] def heldUnforced(from: os.Path, target: os.Path): Unit =
    unforced += target -> List(target / os.up, from / os.up).distinct
