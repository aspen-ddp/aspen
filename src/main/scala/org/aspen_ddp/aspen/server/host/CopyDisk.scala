package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.Backend
import scribe.Logging

import java.util.UUID
import scala.util.Try
import scala.util.control.NonFatal

/** The disk steps of copy moves, retirement, release and deletion, as plain functions of paths and values. Each runs as
 *  one job on the copy's device's management lane (CopyMoves, StoreCopies), or, until their callers are converted, on
 *  the control thread (CopyMoves' synchronous `move`, `place` and `moveUnattached`; `delete` from
 *  StoreTransfers.startIn; DiscoveryDisk's sweep). They touch no control state; the caller applies what they report. */
object CopyDisk extends Logging:
  /** The directory sync and copy rename every move uses unless a test replaces them (CopyMoves' seams) */
  val DefaultSyncDirectory: os.Path => Unit = DurableFiles.syncDirectory
  val DefaultRename: (os.Path, os.Path) => Unit = DurableFiles.atomicRename

  /** The steps of a move from `from` to `target`, into phase `to`: the target's directory made if absent, `to`'s own
   *  parameter file placed durably (the source phase's file is never touched, so a failed move leaves the source
   *  reading what it read), then the rename forced, through `syncDirectory` and `rename` (CopyMoves' seams). A move onto
   *  the copy's own path is no move: NotMoved. An Unsynced result leaves `target`'s and `from`'s parent directories to
   *  force again (CopyMoves.unforced) */
  def relocate(from: os.Path, target: os.Path, to: Phase,
               syncDirectory: os.Path => Unit, rename: (os.Path, os.Path) => Unit): MoveResult =
    if from == target then
      MoveResult.NotMoved(IllegalArgumentException(s"$to is the path $from, where the copy already is"))
    else
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
          case t: DurableFiles.MoveNotForced => MoveResult.Unsynced(t)
          case NonFatal(t) => MoveResult.NotMoved(t)

  /** Whether `path` exists, as FileStat.existence: None where the stat fails */
  def existence(path: os.Path): Option[Boolean] = FileStat.existence(path)

  /** The phase directory holding the copy of `storeId` named `copyId` on the device at `devicePath`
   *  (CopyLayout.locate); None where none does or the location cannot be read */
  def locate(devicePath: os.Path, storeId: StoreId, copyId: UUID): Option[Phase] =
    Try(CopyLayout.locate(devicePath, storeId, copyId)).toOption.flatten

  /** The device's copies (CopyLayout.list) */
  def list(devicePath: os.Path): CopyLayout.Listing = CopyLayout.list(devicePath)

  /** Retire step 2 (StoreCopies.retire) for the copy durably in lost/ at `storePath`: its CRL state saved in the copy
   *  as RebuildingStore.LostCrlStateFile. An existing one is kept (a crash left it). Else the copy's `crl_save.log` is
   *  renamed to it (it already holds the state, which a transfer-out close deleted from the device CRL); `state`, the
   *  store's device-CRL entries read before this job, is first saved beside it as RebuildingStore.LostCrlEntriesFile
   *  when it holds any and no such file is there (a crash before that deletion). Else `state` is saved as it. Each
   *  write is durable before the next. Throws on a failure, leaving what is done for the next retire */
  def retire(storeId: StoreId, storePath: os.Path, state: StoreRecoveryState): Unit =
    val lostState = storePath / RebuildingStore.LostCrlStateFile
    val saveFile = Backend.crlSaveFile(storePath)
    if FileStat.exists(lostState) then
      ()
    else if FileStat.exists(saveFile) then
      val entries = storePath / RebuildingStore.LostCrlEntriesFile
      if !state.isEmpty && !FileStat.exists(entries) then
        CrashRecoveryLog.saveStoreState(storeId, state, entries)
      DurableFiles.move(saveFile, lostState)
    else
      CrashRecoveryLog.saveStoreState(storeId, state, lostState)

  /** `devicePath / name`, created durably if absent. A stat that fails throws (FileStat) */
  private def ensureDirectory(devicePath: os.Path, name: String): os.Path =
    val dir = devicePath / name
    if !FileStat.exists(dir) then
      os.makeDir.all(dir)
      DurableFiles.syncDirectory(devicePath)
    dir

  /** Deletes the directory `storePath` on the device at `devicePath`, rename-first: it is renamed into the device's
   *  deleting directory (StoreCopies.DeletingDirectory), durably, then removed. A crash or failure mid-removal leaves
   *  the remains there, never at `storePath`: an interrupted delete cannot leave part of a store copy in a phase
   *  directory, loadable, nor leave a partial copy in pending-delete. Used for every delete of a copy outside the
   *  device's scratch areas (`creating/`, `rebuilding/`, transfer staging), which are never loaded and are swept whole
   *  at device load. Returns whether `storePath` is gone; a failed rename is logged and leaves it in place. So is a
   *  stat of `storePath` that fails (FileStat): only a known absence is gone, never a copy that may still be there. */
  def delete(devicePath: os.Path, storePath: os.Path, reason: String): Boolean =
    try
      if !FileStat.exists(storePath) then
        true
      else
        val deleting = ensureDirectory(devicePath, StoreCopies.DeletingDirectory)
        val target = deleting / s"${storePath.last}-${UUID.randomUUID()}"
        logger.info(s"Deleting $storePath: $reason")
        DurableFiles.move(storePath, target)
        removeQuietly(target)
        true
    catch
      case t: Throwable =>
        logger.warn(s"Failed to delete $storePath. It stays in place. Error: $t")
        false

  /** What a discard (StoreCopies.discard) found and did */
  enum Discarded:
    /** The copy was known absent: nothing to delete */
    case Absent
    /** Deleted (rename-first) */
    case Deleted
    /** Still there: its delete failed, logged */
    case Kept

  /** A discard's disk steps: the copy at `storePath` deleted (`delete`) unless a stat answers it absent. A stat that
   *  fails is no absence: the delete is attempted, and fails rather than report it gone */
  def discard(devicePath: os.Path, storePath: os.Path, reason: String): Discarded =
    if FileStat.existence(storePath).contains(false) then
      Discarded.Absent
    else if delete(devicePath, storePath, reason) then
      Discarded.Deleted
    else
      Discarded.Kept

  /** Removes whatever an interrupted delete left in the device's deleting directory */
  def sweepDeleting(devicePath: os.Path): Unit =
    val deleting = devicePath / StoreCopies.DeletingDirectory
    try
      if FileStat.isDir(deleting) then os.list(deleting).foreach(removeQuietly)
    catch
      case t: Throwable => logger.warn(s"Failed to list $deleting: $t")

  /** A release's disk steps before its decisions (StoreCopies.release): the remains of interrupted deletes swept
   *  (sweepDeleting), then the device's pending-delete directory listed. Nil for no such directory */
  def pendingDeletes(devicePath: os.Path): Try[List[os.Path]] =
    sweepDeleting(devicePath)
    val pendingDir = devicePath / RebuildingStore.PendingDeleteDirectory
    Try(if FileStat.isDir(pendingDir) then os.list(pendingDir).toList else Nil)

  /** A resync's disk steps (CopyMoves.resync): for each unsynced copy, by its path, its directories forced again,
   *  through `syncDirectory`, its own last. Each copy's outcome: None once all were forced, or the failure */
  def resync(copies: List[(os.Path, List[os.Path])],
             syncDirectory: os.Path => Unit): List[(os.Path, Option[Throwable])] =
    copies.map: (path, dirs) =>
      try
        dirs.foreach(syncDirectory)
        path -> None
      catch case NonFatal(t) => path -> Some(t)

  /** A record of the copy registry as the disk check (HostCopies.verifyAgainstDisk) reads it */
  final case class Recorded(storeId: StoreId, phase: Phase, path: os.Path, creating: Boolean)

  /** The disk check of the device `id` at `devicePath` (HostCopies.verifyAgainstDisk), against `records`, its copy
   *  registry's records taken when the job was submitted. `placing`: the phase each store's running rebuild's finish
   *  places its copy in, not yet recorded (StoreRebuilds.finishPlacing); `skipped`: copy paths a test moves by hand.
   *  The mismatches, each as a message:
   *   - a copy with a config has no record at its path (its phase and copy id), Retired copies aside;
   *   - a record has no directory at its path, but one whose create is still placing it.
   *  Stats and reads that fail are skipped: they say nothing. Empty when consistent */
  def verify(id: StorageDeviceId, devicePath: os.Path, records: List[Recorded], placing: Map[StoreId, Phase],
             skipped: Set[os.Path]): List[String] =
    val listed = Try(list(devicePath).copies)
    // A store path holding the copy a running rebuild's finish placed, reading that rebuild's Rebuilt phase, is
    // skipped: finish places the copy off the control thread, ahead of its completion's handler
    def placed(storeId: StoreId): Boolean =
      placing.get(storeId).exists(p => listed.toOption.exists(_.exists(f => f.storeId == storeId && f.phase == p)))
    val kept = records.filterNot(r => placed(r.storeId))
    val onDisk = listed.toOption.getOrElse(Nil).filter: f =>
      f.phase != Phase.Retired && !placed(f.storeId) &&
        FileStat.existence(f.path / StoreConfig.configFilename).contains(true)
    val dirMissing = kept.sortBy(_.storeId.directoryName).flatMap: r =>
      if r.creating || skipped.contains(r.path) then
        Nil
      else if Try(FileStat.isDir(r.path)).toOption.contains(false) then
        List(s"device $id: record of ${r.storeId} (${r.phase}) has no directory at ${r.path}")
      else
        Nil
    val unrecorded = onDisk
      .filterNot(f => skipped.contains(f.path) || kept.exists(r => r.storeId == f.storeId && r.path == f.path))
      .map(f => s"device $id: directory ${f.path} (${f.phase}) has no record")
    dirMissing ++ unrecorded

  private def removeQuietly(path: os.Path): Unit =
    try os.remove.all(path)
    catch
      case t: Throwable => logger.warn(s"Failed to remove $path. Retrying at a later check. Error: $t")
