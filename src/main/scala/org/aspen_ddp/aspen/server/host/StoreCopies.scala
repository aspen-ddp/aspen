package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.Backend
import scribe.Logging

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Every move and delete of a store copy that is not its owner's. A store has one owner: the device its pool names.
 *  Ownership moves by a transfer or a rebuild, and takes effect when the new device's entry becomes Active; from then
 *  on every other copy of the store, whatever it is, may go. So a copy that stops being the owner's (a transfer out's
 *  source, a lost copy, a re-placed rebuild's source) is never deleted where it stands. It is retired: kept, with its
 *  CRL state, in the device's pending-delete directory, and released (deleted) by the device's reconcile once its
 *  store is Active on its owner device. That is the only release condition. A delete is rename-first, so no
 *  interrupted delete leaves a partial directory anywhere a store copy is looked for (StoreCopies.delete). */
class StoreCopies(ctx: DeviceControl) extends Logging:
  import ctx.given

  // Pending-delete copies whose store's owner state is being read (release)
  private var releaseLookups: Set[os.Path] = Set()

  /** Whether a copy of the store is being retired on any device of the host: its copy's Retiring activity */
  def isRetiring(storeId: StoreId): Boolean = ctx.copyRegistry.isRetiring(storeId)

  private[server] def releaseLookupsInFlight: Set[os.Path] = releaseLookups

  /** Retires the copy of `storeId` on `local`, in a fixed order, each step durable before the next:
   *   1. the copy moved to lost/ (T10; StoreRebuilds.moveToLost), unless it is there, where it is never loaded,
   *      shipped or adopted as clean;
   *   2. its CRL state saved in the copy as RebuildingStore.LostCrlStateFile: an existing one is kept (a crash left
   *      it), else the copy's `crl_save.log` is renamed to it (it already holds the state, which a transfer-out
   *      close deleted from the device CRL; any entries the device CRL still holds for the store, after a crash
   *      before that deletion, are first saved beside it as RebuildingStore.LostCrlEntriesFile), else the store's
   *      device-CRL entries are read and saved;
   *   3. the store's device-CRL entries dropped (CrashRecoveryLog.dropStore): a store loaded or rebuilt onto this
   *      device later would recover them otherwise;
   *   4. the copy moved from lost/ to pending-delete/ under its copy name (T11; CopyMoves.move to Retired).
   *  A crash at any step after the first leaves the copy in lost/, and the next retire redoes what remains. The copy
   *  is never served meanwhile, so its entries cannot change.
   *
   *  The result is whether the store's path is clear: true once the copy is in pending-delete, or if there was none.
   *  False on a failure (logged, everything left for a later call), for a device unloaded meanwhile, while a retire
   *  of the store is already running, while the copy is unsynced (a move a crash could undo; the
   *  next device check forces it), or while its move to lost/ awaits its retry (CopyMoves.pendingLost). */
  def retire(local: LocalStorageDeviceState, storeId: StoreId): Future[Boolean] =
    val storePath = ctx.copyRegistry.pathOrLive(local, storeId)
    if isRetiring(storeId) then
      Future.successful(false)
    else if ctx.copyRegistry.isUnsynced(local, storeId) then
      logger.info(s"Not retiring the copy of store $storeId at $storePath yet: a change to it is not durable")
      Future.successful(false)
    else if ctx.moves.isPendingLost(local, storeId) then
      logger.info(s"Not retiring the copy of store $storeId at $storePath yet: its move to lost/ is still to retry")
      Future.successful(false)
    else
      // A stat that fails is no absence (FileStat): the retire fails, to be retried, rather than report a clear path
      val present =
        try Success(FileStat.exists(storePath))
        catch case NonFatal(t) => Failure(t)
      present match
        case Failure(t) =>
          logger.error(s"Failed to tell whether a copy of store $storeId is at $storePath. Retrying at a later " +
                       s"check. Error: $t")
          Future.successful(false)
        case Success(false) => Future.successful(true)
        case Success(true) => retirePresent(local, storeId, storePath)

  /** retire's steps for a copy known to be at `at` */
  private def retirePresent(local: LocalStorageDeviceState, storeId: StoreId, at: os.Path): Future[Boolean] =
    // Step 1: into lost/, durably, unless it is there. A move not forced is retried once a device check forces it
    ctx.rebuilds.moveToLost("retire", local, storeId) match
      case MoveResult.Durable if ctx.copyRegistry.pathOf(local, storeId).isDefined =>
        retireLost(local, storeId, ctx.copyRegistry.pathOf(local, storeId).get)
      case result =>
        logger.error(s"Failed to move the copy of store $storeId at $at to lost/ before retiring it ($result). " +
                     s"Retrying at a later check")
        Future.successful(false)

  /** retire's steps 2-4 for a copy durably in lost/ at `storePath` */
  private def retireLost(local: LocalStorageDeviceState, storeId: StoreId, storePath: os.Path): Future[Boolean] =
    val dropped =
      try
        // Retiring from here until it ends: isRetiring, and a second retire does nothing
        ctx.copyRegistry.retiring(local, storeId)
        val lostState = storePath / RebuildingStore.LostCrlStateFile
        val saveFile = Backend.crlSaveFile(storePath)
        val saved =
          if FileStat.exists(lostState) then
            Future.unit
          else if FileStat.exists(saveFile) then
            // The device CRL may hold the state too (a crash between the save file and its entries' deletion), and
            // the file may be what was found unusable: those entries are kept beside it, durably before the move
            val entries = storePath / RebuildingStore.LostCrlEntriesFile
            local.crl.readStoreState(storeId).map: trs =>
              if !trs.isEmpty && !FileStat.exists(entries) then
                CrashRecoveryLog.saveStoreState(storeId, trs, entries)
              DurableFiles.move(saveFile, lostState)
          else
            local.crl.readStoreState(storeId).map(trs => CrashRecoveryLog.saveStoreState(storeId, trs, lostState))
        saved.flatMap(_ => local.crl.dropStore(storeId))
      catch case NonFatal(t) => Future.failed(t)
    dropped.transform: result =>
      ctx.copyRegistry.retireEnded(local, storeId)
      result match
        case Failure(t) =>
          logger.error(s"Failed to save and drop the CRL state of store $storeId before retiring its copy at " +
                       s"$storePath. Retrying at a later check. Error: $t")
          Success(false)
        case Success(_) =>
          local.admission.releaseRetained(storeId)
          // A device unloaded meanwhile is retired by whichever load of it comes next
          Success(ctx.devices.isLoaded(local) &&
                  moveToPendingDelete(local, storeId))

  /** Step 4 of retire: lost/ to pending-delete/ (T11). Returns whether the path is clear: the move is durable, and
   *  the copy left the registry (P3). One not forced stays recorded, Retired and unsynced, until a device check
   *  forces it; one that failed leaves the copy in lost/ for the next retire */
  private def moveToPendingDelete(local: LocalStorageDeviceState, storeId: StoreId): Boolean =
    val storePath = ctx.copyRegistry.pathOrLive(local, storeId)
    logger.warn(s"Retiring the copy of store $storeId at $storePath to pending-delete. It is deleted once the store " +
                s"is Active on the device its pool names")
    ctx.moves.move("moveToPendingDelete", local, storeId, Phase.Retired) == MoveResult.Durable

  /** Deletes each copy in the device's pending-delete directory whose store is Active on its owner device: this
   *  device by its own entry, or else the device the pool names, by that device's state. Anything else keeps the
   *  copy for a later check: an entry not Active, a failed read, or a name that does not identify a store (logged).
   *  The store is identified by the copy's name, never by parsing anything inside it. Also finishes any delete an
   *  earlier attempt left in the device's deleting directory. */
  def release(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    StoreCopies.sweepDeleting(local.devicePath)
    val pendingDir = local.devicePath / RebuildingStore.PendingDeleteDirectory
    val copies =
      try
        if FileStat.isDir(pendingDir) then os.list(pendingDir).toList else Nil
      catch
        case t: Throwable =>
          logger.warn(s"Failed to list $pendingDir: $t")
          Nil

    def delete(copy: os.Path, why: String): Unit =
      StoreCopies.delete(local.devicePath, copy, why)

    // A copy whose move here is not forced yet (CopyMoves) could come back at its path after a crash: never released
    copies.filterNot(c => releaseLookups.contains(c) || ctx.copyRegistry.isUnsyncedAt(c)).foreach: copy =>
      StoreCopies.pendingDeleteStore(copy.last) match
        case None =>
          ctx.warn(copy, s"Keeping $copy: its name does not identify a store copy")
        case Some(storeId) =>
          remote.stores.get(storeId) match
            case Some(entry) =>
              if entry.status == StorageDeviceState.StoreStatus.Active then
                delete(copy, s"store $storeId is Active again on this device")
            case None =>
              releaseLookups += copy
              val factive =
                try
                  for
                    pool <- ctx.client.getStoragePoolState(storeId.poolId)
                    owner = pool.stores(storeId.poolIndex)
                    state <- ctx.metadata.lookupStorageDeviceState(owner.storageDeviceId)
                  yield
                    state.storageDeviceId == owner.storageDeviceId &&
                    state.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active)
                catch case NonFatal(t) => Future.failed(t)
              factive.onComplete: result =>
                releaseLookups -= copy
                result match
                  case Success(true) =>
                    delete(copy, s"store $storeId is Active on the device its pool names")
                  case Success(false) => ()
                  case Failure(t) =>
                    logger.info(s"Keeping $copy: the state of store $storeId could not be read. Error: $t")

object StoreCopies extends Logging:
  /** Device-root directory every delete of a store copy passes through: the copy is renamed into it, durably, and only
   *  then removed. Whatever is in it is garbage, removed by the device's load and each reconcile. Invisible to
   *  StoreLoader.tryLoadStore (it holds no store config of its own) */
  val DeletingDirectory = "deleting"

  /** The store a pending-delete copy named `name` belongs to: a copy name (CopyLayout.copyName), parsed */
  def pendingDeleteStore(name: String): Option[StoreId] = CopyLayout.parseName(name).map(_._1)

  /** `devicePath / name`, created durably if absent. A stat that fails throws (FileStat) */
  private[host] def ensureDirectory(devicePath: os.Path, name: String): os.Path =
    val dir = devicePath / name
    if !FileStat.exists(dir) then
      os.makeDir.all(dir)
      DurableFiles.syncDirectory(devicePath)
    dir

  /** Deletes the directory `dir` on the device at `devicePath`, rename-first: it is renamed into the device's
   *  deleting directory, durably, then removed. A crash or failure mid-removal leaves the remains there, never at
   *  `dir`: an interrupted delete cannot strip a marker from a store copy and leave the rest loadable, nor leave a
   *  partial copy in pending-delete. Used for every delete of a copy outside the device's scratch areas
   *  (`creating/`, `rebuilding/`, transfer staging), which are never loaded and are swept whole at device load.
   *  Returns whether `dir` is gone; a failed rename is logged and leaves it in place. So is a stat of `dir` that fails
   *  (FileStat): only a known absence is gone, never a copy that may still be there. */
  def delete(devicePath: os.Path, dir: os.Path, why: String): Boolean =
    try
      if !FileStat.exists(dir) then
        true
      else
        val deleting = ensureDirectory(devicePath, DeletingDirectory)
        val target = deleting / s"${dir.last}-${UUID.randomUUID()}"
        logger.info(s"Deleting $dir: $why")
        DurableFiles.move(dir, target)
        removeQuietly(target)
        true
    catch
      case t: Throwable =>
        logger.warn(s"Failed to delete $dir. It stays in place. Error: $t")
        false

  /** Removes whatever an interrupted delete left in the device's deleting directory */
  def sweepDeleting(devicePath: os.Path): Unit =
    val deleting = devicePath / DeletingDirectory
    try
      if FileStat.isDir(deleting) then os.list(deleting).foreach(removeQuietly)
    catch
      case t: Throwable => logger.warn(s"Failed to list $deleting: $t")

  private def removeQuietly(path: os.Path): Unit =
    try os.remove.all(path)
    catch
      case t: Throwable => logger.warn(s"Failed to remove $path. Retrying at a later check. Error: $t")
