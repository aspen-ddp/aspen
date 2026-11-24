package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import scribe.Logging

import scala.concurrent.{Future, Promise}
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
 *  interrupted delete leaves a partial directory anywhere a store copy is looked for (CopyDisk.delete). Every disk
 *  step runs as a job on the copy's device's lane (CopyDisk). */
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
   *  next device check forces it), while its move to lost/ awaits its retry (CopyMoves.pendingLost), or while another
   *  disk job holds the copy (DiskJob).
   *
   *  No step touches the disk on the control thread: the copy's stat is a `Retire` job (the copy DiskJob(Retire)
   *  meanwhile), step 1 a `Move` job (StoreRebuilds.moveToLostOnLane), step 2 a `Retire` job (CopyDisk.retire) and
   *  step 4 a `Move` job, each on the copy's device's lane and applied after a recheck that the device is still the
   *  loaded one and the copy still at its path. A step on a hung device waits there: the future stays incomplete, the
   *  copy busy, and nothing is decided around it (spec section 7a). */
  def retire(local: LocalStorageDeviceState, storeId: StoreId): Future[Boolean] =
    // A store with no record has no copy on the device: its path is clear
    val recorded = ctx.copyRegistry.pathOf(local, storeId)
    def at = recorded.fold("")(p => s" at $p")
    val busy = ctx.copyRegistry.copiesOf(local).flatMap(_.copies.get(storeId))
      .exists(_.activity.isInstanceOf[Activity.DiskJob])
    if isRetiring(storeId) then
      Future.successful(false)
    else if ctx.copyRegistry.isUnsynced(local, storeId) then
      logger.info(s"Not retiring the copy of store $storeId$at yet: a change to it is not durable")
      Future.successful(false)
    else if ctx.moves.isPendingLost(local, storeId) then
      logger.info(s"Not retiring the copy of store $storeId$at yet: its move to lost/ is still to retry")
      Future.successful(false)
    else if busy then
      // Another disk job holds the copy: never decided around, the retire waits for a later check
      logger.info(s"Not retiring the copy of store $storeId$at yet: a disk job for it is running")
      Future.successful(false)
    else
      recorded match
        case None => Future.successful(true)
        case Some(storePath) =>
          if !ctx.copyRegistry.diskJob(local, storeId, JobKind.Retire) then
            Future.successful(false)
          else
            // A stat that fails is no absence (FileStat): the retire fails, to be retried, rather than report a clear
            // path
            val p = Promise[Boolean]()
            val job = local.io.submit(JobKind.Retire, Some(storeId))(CopyDisk.existence(storePath))
            ctx.onControl("copy retire: present", job): result =>
              ctx.copyRegistry.diskJobEnded(local, storeId)
              if !stillAt(local, storeId, storePath) then
                p.success(false)
              else
                result match
                  case Success(Some(false)) => p.success(true)
                  case Success(Some(true)) => p.completeWith(retirePresent(local, storeId, storePath))
                  case Success(None) | Failure(_) =>
                    if result != Failure(DeviceIo.Cancelled) then
                      logger.error(s"Failed to tell whether a copy of store $storeId is at $storePath. Retrying at " +
                                   s"a later check. Error: ${result.failed.getOrElse("the stat failed")}")
                    p.success(false)
            p.future

  /** Whether `local` is still the loaded device and the copy of `storeId` there is still recorded at `storePath`: the
   *  recheck of every retire step's apply */
  private def stillAt(local: LocalStorageDeviceState, storeId: StoreId, storePath: os.Path): Boolean =
    ctx.devices.isLoaded(local) && ctx.copyRegistry.pathOf(local, storeId).contains(storePath)

  /** retire's steps for a copy known to be at `at` */
  private def retirePresent(local: LocalStorageDeviceState, storeId: StoreId, at: os.Path): Future[Boolean] =
    // Step 1: into lost/, durably, unless it is there. A move not forced is retried once a device check forces it
    val p = Promise[Boolean]()
    ctx.rebuilds.moveToLostOnLane("retire", local, storeId):
      case MoveResult.Durable if ctx.devices.isLoaded(local) && ctx.copyRegistry.pathOf(local, storeId).isDefined =>
        p.completeWith(retireLost(local, storeId, ctx.copyRegistry.pathOf(local, storeId).get))
      case result =>
        logger.error(s"Failed to move the copy of store $storeId at $at to lost/ before retiring it ($result). " +
                     s"Retrying at a later check")
        p.success(false)
    p.future

  /** retire's steps 2-4 for a copy durably in lost/ at `storePath`. The store's device-CRL entries are read first
   *  (the CRL's own threads), then step 2's file work is one `Retire` job (CopyDisk.retire, the copy DiskJob(Retire)
   *  over Retiring meanwhile), then the entries are dropped, then the move to pending-delete is a `Move` job. The copy
   *  is Retiring from the read until the move's end; each apply rechecks the device and the copy's path, and a
   *  mismatch ends the retire (false) for a later check */
  private def retireLost(local: LocalStorageDeviceState, storeId: StoreId, storePath: os.Path): Future[Boolean] =
    // Retiring from here until it ends: isRetiring, and a second retire does nothing
    ctx.copyRegistry.retiring(local, storeId)
    def ended(clear: Boolean): Boolean =
      ctx.copyRegistry.retireEnded(local, storeId)
      clear
    val read =
      try local.crl.readStoreState(storeId)
      catch case NonFatal(t) => Future.failed(t)
    val saved = read.flatMap: state =>
      if !stillAt(local, storeId, storePath) then
        Future.failed(IllegalStateException(s"device ${local.storageDeviceId} unloaded or its copy moved"))
      else if !ctx.copyRegistry.diskJob(local, storeId, JobKind.Retire) then
        Future.failed(IllegalStateException(s"the copy of $storeId could not be claimed for its retire"))
      else
        val p = Promise[Unit]()
        val job = local.io.submit(JobKind.Retire, Some(storeId))(CopyDisk.retire(storeId, storePath, state))
        ctx.onControl("copy retire: save CRL state", job): result =>
          ctx.copyRegistry.diskJobEnded(local, storeId)
          if !stillAt(local, storeId, storePath) then
            p.failure(IllegalStateException(s"device ${local.storageDeviceId} unloaded or its copy moved"))
          else
            p.complete(result)
        p.future
    val dropped = saved.flatMap: _ =>
      try local.crl.dropStore(storeId)
      catch case NonFatal(t) => Future.failed(t)
    dropped.transformWith:
      case Failure(t) =>
        logger.error(s"Failed to save and drop the CRL state of store $storeId before retiring its copy at " +
                     s"$storePath. Retrying at a later check. Error: $t")
        Future.successful(ended(false))
      case Success(_) =>
        local.admission.releaseRetained(storeId)
        // A device unloaded meanwhile is retired by whichever load of it comes next
        if !stillAt(local, storeId, storePath) then
          Future.successful(ended(false))
        else
          moveToPendingDelete(local, storeId).map(ended)

  /** Step 4 of retire: lost/ to pending-delete/ (T11), a `Move` job. Completes with whether the path is clear: the
   *  move is durable, and the copy left the registry (P3). One not forced stays recorded, Retired and unsynced, until a
   *  device check forces it; one that failed leaves the copy in lost/ for the next retire */
  private def moveToPendingDelete(local: LocalStorageDeviceState, storeId: StoreId): Future[Boolean] =
    val at = ctx.copyRegistry.pathOf(local, storeId).fold("")(p => s" at $p")
    logger.warn(s"Retiring the copy of store $storeId$at to pending-delete. It is deleted once the store " +
                s"is Active on the device its pool names")
    val p = Promise[Boolean]()
    ctx.moves.moveOnLane("moveToPendingDelete", local, storeId, Phase.Retired)(r => p.success(r == MoveResult.Durable))
    p.future

  // Devices with a release outstanding (its sweep and listing job, release), by path: at most one each
  private var releasing: Set[os.Path] = Set()
  // Pending-delete copies whose delete job is outstanding
  private var deleting: Set[os.Path] = Set()

  private[server] def releasesRunning: Set[os.Path] = releasing
  private[server] def deletesRunning: Set[os.Path] = deleting

  /** Deletes each copy in the device's pending-delete directory whose store is Active on its owner device: this
   *  device by its own entry, or else the device the pool names, by that device's state. Anything else keeps the
   *  copy for a later check: an entry not Active, a failed read, or a name that does not identify a store (logged).
   *  An Active entry here over a Lost copy of the store here (one about to be flipped to Rebuilding: a device load
   *  that found two copies of the store, P2, or a copy that failed since) is no owner's copy yet: kept too.
   *  The store is identified by the copy's name, never by parsing anything inside it. Also finishes any delete an
   *  earlier attempt left in the device's deleting directory.
   *
   *  The sweep and the listing are one `Release` job on the device's lane (CopyDisk.pendingDeletes; a device with one
   *  outstanding starts none, so a hung device queues at most one), and each delete one `Delete` job there
   *  (CopyDisk.delete; one per copy outstanding). Each apply acts only while `local` is still the loaded device. A
   *  release on a hung device waits on its own lane: nothing is inferred deleted, and no other device waits */
  def release(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val devicePath = local.devicePath
    if !releasing.contains(devicePath) then
      releasing += devicePath
      val job = local.io.submit(JobKind.Release, None)(CopyDisk.pendingDeletes(devicePath))
      ctx.onControl("copy release", job): result =>
        releasing -= devicePath
        if ctx.devices.isLoaded(local) then
          result.flatten match
            case Failure(DeviceIo.Cancelled) => ()
            case Failure(t) =>
              logger.warn(s"Failed to list ${devicePath / RebuildingStore.PendingDeleteDirectory}: $t")
            case Success(copies) => releaseListed(local, remote, copies)

  private def releaseListed(local: LocalStorageDeviceState, remote: StorageDeviceState, copies: List[os.Path]): Unit =
    // A copy whose move here is not forced yet (CopyMoves) could come back at its path after a crash: never released
    copies.filterNot(c => releaseLookups.contains(c) || deleting.contains(c) || ctx.copyRegistry.isUnsyncedAt(c))
      .foreach: copy =>
        StoreCopies.pendingDeleteStore(copy.last) match
          case None =>
            ctx.warn(copy, s"Keeping $copy: its name does not identify a store copy")
          case Some(storeId) =>
            val lostHere = ctx.moves.isPendingLost(local, storeId) ||
              ctx.copyRegistry.copiesOf(local).flatMap(_.copies.get(storeId)).exists(_.phase == Phase.Lost)
            CopyDecision.release(remote.stores.get(storeId), lostHere) match
              case CopyDecision.ReleaseDecision.Release =>
                deleteRetired(local, copy, s"store $storeId is Active again on this device")
              case CopyDecision.ReleaseDecision.Keep => ()
              case CopyDecision.ReleaseDecision.AskOwner =>
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
                      if ctx.devices.isLoaded(local) then
                        deleteRetired(local, copy, s"store $storeId is Active on the device its pool names")
                    case Success(false) => ()
                    case Failure(t) =>
                      logger.info(s"Keeping $copy: the state of store $storeId could not be read. Error: $t")

  /** Deletes the pending-delete copy `copy` on `local` as a `Delete` job on its lane (rename-first, CopyDisk.delete).
   *  The copy has no record (P3): `deleting` marks it busy until the job's result returns */
  private def deleteRetired(local: LocalStorageDeviceState, copy: os.Path, why: String): Unit =
    if !deleting.contains(copy) then
      deleting += copy
      val job = local.io.submit(JobKind.Delete, None)(CopyDisk.delete(local.devicePath, copy, why))
      ctx.onControl("copy release: delete", job)(_ => deleting -= copy)

  /** Deletes a copy that never was its store's owner copy and holds nothing its source lacks, so it needs no retiring:
   *  the copy a transfer delivered onto a device that turned out to be tombstoned (TransferOutcome.SourceRestored:
   *  the pool still names the source, which still holds the data), or one a rebuild built from its peers on a device
   *  the tree does not say owns it (RebuildOutcome.Discard), or one beside a copy that came back while it was
   *  rebuilt. Its place in arrived/ or rebuilt/ keeps tryLoadStore from loading it, but nothing else would ever
   *  reclaim it, since reconcileDeviceState ignores a failed device outright. Were it loaded, and the source happened
   *  to live on this same host, the load would replace the source's entry in `stores`, which is keyed by StoreId
   *  alone. Nothing is unloaded because nothing was loaded: the load is downstream of the decision.
   *
   *  One `Delete` job on the device's lane (CopyDisk.discard: rename-first; a copy a stat answers absent is not
   *  deleted, nor reported discarded), the copy claimed DiskJob(Delete) meanwhile (a copy busy with another job is
   *  kept, logged, as a failed delete keeps it). `discarded` runs with the device, on the control thread, once the
   *  copy is deleted, for the copy registry, provided the device is still the loaded one and the copy's record still
   *  at the deleted path. A device not loaded, or a copy with no record (no copy of the store there), discards
   *  nothing */
  def discard(storeId: StoreId, deviceId: StorageDeviceId, reason: String)
             (discarded: LocalStorageDeviceState => Unit): Unit =
    ctx.devices.devices.get(deviceId) match
      case None =>
        // The operation completed on a device this process never loaded, or one a shutdown has
        // since unloaded. Log rather than assume.
        logger.warn(s"Cannot discard the copy of store $storeId on unloaded device $deviceId")
      case Some(sds) =>
        ctx.copyRegistry.pathOf(sds, storeId).foreach: storePath =>
          if !ctx.copyRegistry.diskJob(sds, storeId, JobKind.Delete) then
            logger.warn(s"Not discarding the copy of store $storeId at $storePath yet: it is busy")
          else
            logger.warn(s"Discarding the copy of store $storeId on device $deviceId. $reason: $storePath")
            // Rename-first: a failure leaves the copy whole where it is, so a restart never loads it.
            // The store is not loaded, so nothing serves it now
            val job = sds.io.submit(JobKind.Delete, Some(storeId))(CopyDisk.discard(sds.devicePath, storePath, reason))
            ctx.onControl("copy discard", job): result =>
              ctx.copyRegistry.diskJobEnded(sds, storeId)
              if result == Success(CopyDisk.Discarded.Deleted) && stillAt(sds, storeId, storePath) then
                discarded(sds)

object StoreCopies:
  /** Device-root directory every delete of a store copy passes through: the copy is renamed into it, durably, and only
   *  then removed. Whatever is in it is garbage, removed by the device's load and each reconcile. Invisible to
   *  StoreLoader.tryLoadStore (it holds no store config of its own) */
  val DeletingDirectory = "deleting"

  /** The store a pending-delete copy named `name` belongs to: a copy name (CopyLayout.copyName), parsed */
  def pendingDeleteStore(name: String): Option[StoreId] = CopyLayout.parseName(name).map(_._1)
