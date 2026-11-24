package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import org.aspen_ddp.aspen.server.rebuild.{RebuildDisk, RebuildingStore}
import scribe.Logging

import scala.util.control.NonFatal

/** The disk steps of StoreRebuilds' bookkeeping around a rebuild, as plain functions of paths: the `rebuilding/`
 *  staging listing (read with a device check's inputs, ReconcileDisk.gather) and the removal of the staging of
 *  stores no longer rebuilt (sweepUnlistedStaging), a staging checkpoint's durable drop (retireForeignCopy) and the
 *  listing of a store's copies when its rebuild finishes. Each runs as one `RebuildBookkeeping` job on the device's
 *  management lane (or inside the check's `ReconcileInputs` job); they touch no control state, and the control
 *  thread applies what they report. */
object RebuildBookkeepingDisk extends Logging:

  /** The device's rebuild staging root, `rebuilding/` */
  def stagingRoot(devicePath: os.Path): os.Path = devicePath / RebuildingStore.RebuildDirectory

  /** The staging directory of `storeId`'s rebuild on the device at `devicePath` */
  def stagingOf(devicePath: os.Path, storeId: StoreId): os.Path =
    stagingRoot(devicePath) / storeId.directoryName

  /** The directories under the device's `rebuilding/` (each a store's staging), Nil for no such directory. Throws a
   *  failed listing or stat: the sweep then deletes nothing that round */
  def listStaging(devicePath: os.Path): List[os.Path] =
    val root = stagingRoot(devicePath)
    if FileStat.isDir(root) then os.list(root).filter(FileStat.isDir(_)).toList else Nil

  /** Removes each staging directory of `dirs` whole. Each one's outcome: None once removed, or the failure (the next
   *  sweep tries again) */
  def removeStaging(dirs: List[os.Path]): List[(os.Path, Option[Throwable])] =
    dirs.map: dir =>
      try
        os.remove.all(dir)
        dir -> None
      catch case NonFatal(t) => dir -> Some(t)

  /** What dropStagingCheckpoint found and did */
  enum CheckpointDrop:
    /** No staging directory: a first pass's */
    case NoStaging
    /** The checkpoint removed durably (the directory's other contents too, best effort) */
    case Dropped
    /** The stat or the checkpoint's removal failed: the checkpoint may still be there */
    case Failed(cause: Throwable)

  /** Removes the rebuild checkpoint from the staging directory `staging`, durably, then the rest of the directory (a
   *  first pass's createStaging discards whatever is left, so a failure there is only logged) */
  def dropStagingCheckpoint(staging: os.Path): CheckpointDrop =
    try
      if !FileStat.exists(staging) then
        CheckpointDrop.NoStaging
      else
        RebuildDisk.removeAll(staging)
        DurableFiles.syncDirectory(staging)
        try os.remove.all(staging)
        catch case NonFatal(t) => logger.warn(s"Failed to delete $staging. A first pass discards it. Error: $t")
        CheckpointDrop.Dropped
    catch case NonFatal(t) => CheckpointDrop.Failed(t)

  /** The copies of `storeId` on the device at `devicePath` outside pending-delete/ (CopyLayout.copiesOf): what a
   *  rebuild's completion lists before its flip. Throws a failed listing */
  def copiesOf(devicePath: os.Path, storeId: StoreId): List[CopyLayout.Found] =
    CopyLayoutDisk.copiesOf(devicePath, storeId)
