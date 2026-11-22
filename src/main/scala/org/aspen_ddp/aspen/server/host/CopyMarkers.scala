package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.DurableFiles
import org.aspen_ddp.aspen.server.host.StoreDirMarkers.Marker
import scribe.Logging

import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Every write and removal of a marker file at a store path (StoreDirMarkers), each made durable by forcing the store
 *  directory, and each recorded in the copy registry: the copy's phase follows the markers on disk, durable or not.
 *  A write or unlink that lands but whose directory sync fails leaves the copy unsynced (CopyRecord.unsynced): its
 *  change could be undone by a crash, so it is never loaded (StoreLoader.tryLoadStore), shipped
 *  (StoreTransfersOut.startOut) or retired (StoreCopies.retire) until a device check forces the directory again
 *  (CopyMoves.resync, DeviceReconciler: the one unsynced mechanism, shared with moves). A write or unlink that itself
 *  fails changes nothing.
 *
 *  Scratch areas (a transfer's or a rebuild's staging) are not store paths: their markers travel with the move that
 *  places the copy, which DurableFiles.move forces. */
final class CopyMarkers(ctx: DeviceControl) extends Logging:

  /** How a marker site forces the store directory: CopyMoves' seam, which the resync uses too. Replaced only by
   *  tests, to fail the sync after the write or unlink */
  private[server] def syncDirectory: os.Path => Unit = ctx.moves.syncDirectory
  private[server] def syncDirectory_=(sync: os.Path => Unit): Unit = ctx.moves.syncDirectory = sync

  /** Writes `marker` with `content` into the copy of `storeId` on `sds`, durably. Throws if it fails, unsynced or not
   *  written at all */
  def write(sds: LocalStorageDeviceState, storeId: StoreId, marker: Marker, content: String): Unit =
    writeAt(ctx.copyRegistry.pathOrLive(sds, storeId), marker, content)

  /** As `write`, for the copy at `storePath`: one of a loaded device, or of a device being loaded, whose attach then
   *  takes an unsynced mark */
  def writeAt(storePath: os.Path, marker: Marker, content: String): Unit =
    val site = s"write ${marker.fileName}"
    DurableFiles.place(storePath / marker.fileName, content)
    // In place: the copy's phase follows, whether or not the sync below holds
    ctx.copyRegistry.markersChanged(site, storePath)
    force(site, storePath)

  /** Removes `marker` from the copy of `storeId` on `sds`. Returns whether the copy is now known durably free of it:
   *  false, logged, on a failed unlink (nothing changed) or a failed sync after it (the copy unsynced). A marker
   *  already absent needs no sync, unless the copy is unsynced (an earlier unlink's sync failed): then it is forced */
  def remove(sds: LocalStorageDeviceState, storeId: StoreId, marker: Marker): Boolean =
    removeAt(ctx.copyRegistry.pathOrLive(sds, storeId), marker)

  /** As `remove`, for the copy at `storePath`, as writeAt is for a write */
  def removeAt(storePath: os.Path, marker: Marker): Boolean =
    val site = s"remove ${marker.fileName}"
    val file = storePath / marker.fileName
    val unlinked =
      try Right(os.remove(file, checkExists = false))
      catch case NonFatal(t) => Left(t)
    unlinked match
      case Left(t) =>
        logger.error(s"Failed to remove $file. The copy keeps it. Error: $t")
        false
      case Right(unlinkedNow) =>
        // The registry follows the markers on disk even when this unlink found none
        ctx.copyRegistry.markersChanged(site, storePath)
        if !unlinkedNow && !ctx.copyRegistry.isUnsyncedAt(storePath) then
          true
        else
          try
            force(site, storePath)
            true
          catch case NonFatal(t) =>
            logger.error(s"Removed $file, but its directory could not be forced. The copy stays unloaded until a " +
                         s"later check of its device forces it. Error: $t")
            false

  /** Forces `storePath`: synced, or else unsynced and the failure rethrown */
  private def force(site: String, storePath: os.Path): Unit =
    try
      syncDirectory(storePath)
      ctx.copyRegistry.synced(storePath)
    catch case NonFatal(t) =>
      ctx.copyRegistry.unsynced(site, storePath)
      throw t
