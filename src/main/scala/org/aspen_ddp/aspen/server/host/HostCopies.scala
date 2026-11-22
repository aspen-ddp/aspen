package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.Host.LoadRetry
import scribe.Logging

import scala.util.Try
import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The copy registry of the host (store copy state machine, stage 1): one `DeviceCopies` per loaded device, held on
 *  its LocalStorageDeviceState, attached here at the device's load and detached at its unload, plus what the host
 *  knows of the stores of devices it has not loaded.
 *
 *  The router's silence and the view's repairability are derived from it (`silence`, `repairable`): after every change
 *  to what they read, `onChange` recomputes the store's silence, sets the router's mark and republishes the view. A
 *  throw there is a bug: with `strict` (tests) it propagates (SilenceFailure, which `fact` and `claim` do not swallow) and
 *  fails the next drain; otherwise it is logged at error and the store is left silent, the conservative answer, as
 *  clients retransmit.
 *
 *  The loader's opens and load retries live here only: a copy's Opening and AwaitingRetry activities (isOpening,
 *  retries), and an open of a copy with TransferOut work on its device is refused (transferOutWork, R12).
 *
 *  Every other update is one of two kinds (R22):
 *   - a fact (`fact`): something that already happened (an install, a router removal, the end of a close, an open,
 *     a retire or a retry, a retire's start, a phase a disk mutation reached, a copy's leaving its path). It is
 *     applied even when the table does not allow it (DeviceCopies.applyActivity / applyPhase; a phase the table
 *     refuses is set to what the copy's directory reads, CopyPhase.known), and the violation recorded;
 *   - a claim (`claim`): something about to start that another actor may hold (Opening, AwaitingRetry, a work's
 *     start or update, a new record). An illegal one is refused, recorded, and returned as `false`, and the caller
 *     backs off: no open, no work start.
 *  Production logs each recorded violation at error. `Host.testingOnlyHandleEvents` fails a strict host's drain on
 *  any of them, and on any mismatch `verifyAgainstDisk` finds, when `Host.Seams.verifyCopyRegistry` is set. */
final class HostCopies(ctx: DeviceControl) extends Logging:
  private var attached: Map[StorageDeviceId, LocalStorageDeviceState] = Map()
  // Unloaded devices, by id, with the stores their last tree entry listed
  private var unloaded: Map[StorageDeviceId, Set[StoreId]] = Map()
  // Device directories whose ownership is being decided, with the stores found in them
  private var awaiting: Map[os.Path, Set[StoreId]] = Map()
  private var errors: Vector[String] = Vector()
  // Copies a load found with no record (readAtLoad), for the drain check to report
  private var learnedCopies: Vector[String] = Vector()
  // The records of stores this host serves from a device it has not loaded: only a load through the unloaded-device
  // CRL seam (Host.Seams.unloadedDeviceCrl, tests) makes them, never production. No marker site reaches them, and the
  // disk check skips them: their backends have no directory
  private var untracked: Map[StorageDeviceId, DeviceCopies] = Map()
  // The purpose each close asked for (closingFor) is for, until the router removes its store
  private var closePurposes: Map[StoreId, ClosePurpose] = Map()
  // The load retry of each copy that was Closing or Retiring when the loader recorded it (awaitingRetry), by device
  // and store: the copy takes it once it is Idle again (closeEnded, retireEnded), and it goes with a copy that leaves
  // its path. Every other retry is its copy's AwaitingRetry activity
  private var parkedRetries: Map[(StorageDeviceId, StoreId), LoadRetry] = Map()
  // Throws of the silence path (onChange), for the drain check
  private var silenceErrors: Vector[String] = Vector()
  // Store paths whose marker change could not be synced before their device was attached (unsynced): its attach
  // marks their records
  private var unsyncedPaths: Set[os.Path] = Set()

  /** Tests: a throw of the silence path propagates rather than leaving the store silent (Host.Seams) */
  var strict: Boolean = false

  /** Test-only: called after every change to a record, a work or a missing mark, with the device's copies */
  var observer: (DeviceCopies, StoreId) => Unit = (_, _) => ()

  /** The records of device `id`: a loaded one's, or those of the stores served from it untracked */
  def device(id: StorageDeviceId): Option[DeviceCopies] = attached.get(id).map(_.copies).orElse(untracked.get(id))

  /** Every copy a load learned of (readAtLoad), each as a message */
  def learned: Vector[String] = learnedCopies

  /** The unloaded devices' stores, by device */
  def unloadedEntries: Map[StorageDeviceId, Set[StoreId]] = unloaded

  /** The device directories awaiting their ownership decision, with their stores */
  def awaitingDevices: Map[os.Path, Set[StoreId]] = awaiting

  /** Runs `f`, the record of a fact at the production site `site`: it uses the `apply*` moves, which never refuse,
   *  and reports what the table does not allow through `violation`. Anything else it throws (a missing record) is
   *  recorded too; a strict silence path's throw (SilenceFailure) propagates */
  def fact(site: String)(f: => Unit): Unit =
    try f
    catch
      case e: SilenceFailure => throw e
      case NonFatal(t) => violation(site, t.toString)

  /** Runs `f`, a claim at the production site `site`, with the moves that throw on what the table does not allow.
   *  Returns whether it was made; a refused one is recorded, and its caller backs off. A strict silence path's throw
   *  (SilenceFailure) propagates */
  def claim(site: String)(f: => Unit): Boolean =
    try
      f
      true
    catch
      case e: SilenceFailure => throw e
      case NonFatal(t) =>
        violation(site, s"refused: $t")
        false

  /** Logs and records a registry violation at `site` */
  private def violation(site: String, message: String): Unit =
    logger.error(s"Copy registry ($site): $message")
    errors :+= s"$site: $message"

  /** Records an activity fact on `copies`, applied whatever the table says */
  private def activityFact(site: String, copies: DeviceCopies, storeId: StoreId, to: Activity): Unit =
    copies.applyActivity(storeId, to).foreach(violation(site, _))

  /** Records a phase fact on `copies`: one the table refuses takes what the copy's directory reads */
  private def phaseFact(site: String, copies: DeviceCopies, storeId: StoreId, to: Phase): Unit =
    val path = copies.devicePath / storeId.directoryName
    copies.applyPhase(storeId, to, CopyPhase.known(path).getOrElse(Phase.Unread)).foreach(violation(site, _))

  /** The throws of the silence path since the last call, which clears them */
  def takeSilenceErrors(): Vector[String] =
    val e = silenceErrors
    silenceErrors = Vector()
    e

  /** Something `silence(storeId)` reads changed: recomputes it, sets the router's mark, and republishes the view. A
   *  throw is a bug: logged at error, the store left silent (clients retransmit), and recorded for the drain check;
   *  with `strict` it then propagates */
  def onChange(storeId: StoreId): Unit = onChange(List(storeId))

  /** `onChange` of each of `storeIds`, publishing the view once */
  def onChange(storeIds: Iterable[StoreId]): Unit =
    val failed = storeIds.flatMap: storeId =>
      try
        ctx.router.setSilent(storeId, silence(storeId).isInstanceOf[Silence.Silent])
        None
      catch case NonFatal(t) =>
        logger.error(s"Copy registry: the silence of store $storeId could not be derived. Leaving it silent: $t")
        ctx.router.setSilent(storeId, true)
        Some(s"silence of $storeId: $t" -> t)
    val published =
      try
        ctx.publishView()
        None
      catch case NonFatal(t) =>
        logger.error(s"Copy registry: the host's store view could not be published: $t")
        Some(s"publishView: $t" -> t)
    val all = failed.toList ++ published
    silenceErrors ++= all.map(_._1)
    if strict then
      all.headOption.foreach((msg, t) => throw SilenceFailure(msg, t))

  /** The violations facts and claims recorded since the last call, which clears them */
  def takeRegistryErrors(): Vector[String] =
    val e = errors
    errors = Vector()
    e

  /** At load, before any store is offered: registers each of the device's `children` (its listing) whose name is a
   *  store's and that is a directory with its phase, Unread where its stat or phase cannot be read. Never throws: the
   *  device is attached whatever the registration meets, and a copy it cannot read waits for its load to read it
   *  (readAtLoad) */
  def attach(sds: LocalStorageDeviceState, children: Seq[os.Path]): Unit =
    attached += sds.storageDeviceId -> sds
    // Its last tree entry's stores (unloaded) stay silent until its first reconcile reads the entry anew
    sds.onCopyChange = s =>
      onChange(s)
      observer(sds.copies, s)
    sds.onReconciled = (before, after) =>
      val listed = unloaded.getOrElse(sds.storageDeviceId, Set())
      if attached.get(sds.storageDeviceId).exists(_ eq sds) then
        unloaded -= sds.storageDeviceId
      onChange(listed ++ before ++ after)
    fact("attach"):
      children.foreach: path =>
        HostCopies.storeIdOf(path).foreach: storeId =>
          Try(FileStat.isDir(path)) match
            case scala.util.Success(false) => ()
            case scala.util.Success(true) =>
              sds.copies.register(storeId, CopyPhase.known(path).getOrElse(Phase.Unread))
            case scala.util.Failure(_) => sds.copies.register(storeId, Phase.Unread)
          if unsyncedPaths.contains(path) && sds.copies.copies.contains(storeId) then
            unsyncedPaths -= path
            sds.copies.setUnsynced(storeId, true)

  /** As attach, listing the device itself (ControlFixture.injectLoadedDevice). A listing that fails registers none */
  def attach(sds: LocalStorageDeviceState): Unit =
    val children =
      try
        if FileStat.isDir(sds.devicePath) then os.list(sds.devicePath).toList else Nil
      catch case NonFatal(t) =>
        fact("attach")(throw t)
        Nil
    attach(sds, children)

  /** A load (StoreLoader.tryLoadStore) read the copy at `storePath`, whose config names `storeId`, in `phase`. A copy
   *  with no record is registered and reported as learned; an Unread record takes the phase; a record in another
   *  phase is a miswired site: recorded, and the record takes what the load read */
  def readAtLoad(sds: LocalStorageDeviceState, storePath: os.Path, storeId: StoreId, phase: Phase): Unit =
    fact("readAtLoad"):
      if HostCopies.storeIdOf(storePath).contains(storeId) && storePath / os.up == sds.devicePath then
        copiesOf(sds).foreach: copies =>
          copies.copies.get(storeId).map(_.phase) match
            case None =>
              logger.warn(s"The copy of store $storeId at $storePath was not known to the copy registry. Learned " +
                          s"at its load, $phase")
              learnedCopies :+= s"device ${sds.storageDeviceId}: $storeId learned at load, $phase"
              copies.register(storeId, phase)
            case Some(Phase.Unread) => copies.setPhase(storeId, phase)
            case Some(p) if p == phase => ()
            case Some(p) =>
              violation("readAtLoad", s"record of $storeId is $p, its load read $phase at $storePath; recorded $phase")
              copies.applyPhase(storeId, phase, phase)

  // ---- Activity: what the host is doing with each copy's open store ----

  /** The record of `storeId` on device `id`, attached or untracked */
  private def recordOf(id: StorageDeviceId, storeId: StoreId): Option[(DeviceCopies, CopyRecord)] =
    device(id).flatMap(c => c.copies.get(storeId).map(c -> _))

  /** The TransferOut work of `storeId` on `sds`, if any: an open of the copy is refused while it lasts (R12), so the
   *  open path asks before it opens (StoreLoader.startOpen), and DeviceCopies throws on the move to Opening */
  def transferOutWork(sds: LocalStorageDeviceState, storeId: StoreId): Option[Work.TransferOut] =
    copiesOf(sds).flatMap(_.work.get(storeId)).collect { case w: Work.TransferOut => w }

  /** StoreLoader.startOpen began open `openId` of the copy at `storeId`'s path on `sds`: its record is Opening until
   *  the open is served or ends, taking the loader's retry if it was AwaitingRetry. A create has no record yet: one is
   *  registered Live, for the copy its create places at the path before the open (StoreCreation), and unregistered
   *  if the create fails leaving nothing there (createFailed). A claim: false when refused (a record Closing,
   *  Retiring or not Live, TransferOut work), and then the caller opens nothing */
  def opening(sds: LocalStorageDeviceState, storeId: StoreId, openId: Long, create: Boolean): Boolean =
    claim("startOpen"):
      copiesOf(sds).foreach: copies =>
        if create && !copies.copies.contains(storeId) then
          copies.register(storeId, Phase.Live)
        copies.setActivity(storeId, Activity.Opening(openId))

  /** A create's open failed and left nothing at the path (StoreCreation): the record its open registered goes */
  def createFailed(sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    fact("create failed"):
      copiesOf(sds).foreach: copies =>
        if copies.copies.contains(storeId) then
          copies.unregister(storeId)
          parkedRetries -= ((copies.deviceId, storeId))

  /** Whether an open of `storeId` is under way on any device of the host: its copy is Opening, from startOpen until
   *  its open fails or the LoadStore its open queued is handled (StoreLoader.handleLoad) */
  def isOpening(storeId: StoreId): Boolean =
    allCopies.exists(_.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.Opening]))

  /** An open of `storeId` on `sds` ended without its store being served (StoreLoader.handleBackendOpened, handleLoad):
   *  an Opening record is Idle, unless the loader holds a retry for it (awaitingRetry took it already) */
  def openEnded(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    fact(site):
      copiesOf(sds).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.Opening]) then
          activityFact(site, copies, storeId, Activity.Idle)

  /** The loader records `retry` for `storeId`: its copy's record is AwaitingRetry(retry), learned Unread if it has
   *  none. A record Closing or Retiring parks it until it is Idle again (closeEnded, retireEnded). A claim: refused
   *  (false) for a record Served, which needs no retry */
  def awaitingRetry(site: String, storeId: StoreId, retry: LoadRetry): Boolean =
    claim(site):
      copiesOf(retry.sds).foreach: copies =>
        copies.copies.get(storeId) match
          case None =>
            // Put at its path after the device's load (by hand or by a test): learned, as readAtLoad learns one
            logger.warn(s"The copy of store $storeId at ${retry.storePath} was not known to the copy registry. " +
                        "Learned awaiting a load retry, Unread")
            learnedCopies :+= s"device ${retry.sds.storageDeviceId}: $storeId learned awaiting a retry, Unread"
            copies.register(storeId, Phase.Unread)
            copies.setActivity(storeId, Activity.AwaitingRetry(retry))
          case Some(r) => r.activity match
            case Activity.Closing(_) | Activity.Retiring => parkedRetries += (copies.deviceId, storeId) -> retry
            case _ => copies.setActivity(storeId, Activity.AwaitingRetry(retry))

  /** The loader dropped the retry of `storeId` on `sds` */
  def retryDropped(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    fact(site):
      parkedRetries -= ((sds.storageDeviceId, storeId))
      copiesOf(sds).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.AwaitingRetry]) then
          activityFact(site, copies, storeId, Activity.Idle)

  /** The retry the copy of `storeId` on `sds` awaits, if its record is AwaitingRetry */
  def retryOn(sds: LocalStorageDeviceState, storeId: StoreId): Option[LoadRetry] =
    copiesOf(sds).flatMap(_.copies.get(storeId)).collect { case CopyRecord(_, _, _, Activity.AwaitingRetry(r)) => r }

  /** A retry of `storeId` the loader holds on any loaded device: a record's AwaitingRetry, or one parked until its
   *  record is Idle again */
  def retryOf(storeId: StoreId): Option[LoadRetry] =
    retries.collectFirst { case (s, r) if s == storeId => r }
      .orElse(parkedRetries.toList.sortBy(_._1._1.uuid).collectFirst { case ((_, s), r) if s == storeId => r })

  /** The retry parked for `storeId` on device `id`, if any, taken now its record is Idle again */
  private def takeParked(site: String, copies: DeviceCopies, storeId: StoreId): Unit =
    parkedRetries.get((copies.deviceId, storeId)).foreach: retry =>
      parkedRetries -= ((copies.deviceId, storeId))
      if copies.copies.get(storeId).exists(_.activity == Activity.Idle) then
        claim(site)(copies.setActivity(storeId, Activity.AwaitingRetry(retry)))

  /** Every copy awaiting a load retry on the loaded devices, with its store, in device then store order
   *  (StoreLoader.retryFailedLoads) */
  def retries: List[(StoreId, LoadRetry)] =
    attached.toList.sortBy(_._1.uuid).flatMap: (_, sds) =>
      sds.copies.copies.values.toList.sortBy(r => (r.storeId.poolId.uuid, r.storeId.poolIndex)).collect:
        case CopyRecord(storeId, _, _, Activity.AwaitingRetry(r)) => storeId -> r

  /** The router installed `storeId`, loaded from device `id` (StoreRouter.install): its record (`known`) is Served */
  def installed(id: StorageDeviceId, storeId: StoreId): Unit =
    fact("install"):
      known("install", id, storeId)
      // Served -> Served: a reload over the served store (StoreRouter.install replacing an entry)
      device(id).foreach: copies =>
        if copies.copies.contains(storeId) then
          activityFact("install", copies, storeId, Activity.Served)

  /** A LoadStore of `storeId` onto device `id` is handled (StoreLoader.handleLoad): its copy has a record before the
   *  load's outcome, whatever it is (see `known`) */
  def loadHandled(id: StorageDeviceId, storeId: StoreId): Unit =
    fact("LoadStore")(known("LoadStore", id, storeId))

  /** Gives the copy of `storeId` loaded onto device `id` a record if it has none. A store loaded onto a device this
   *  host has not loaded, through the unloaded-device CRL seam (tests), gets an untracked record (Live), kept until
   *  the host goes, as a copy's record is kept on a loaded device. A copy a loaded device holds with no record was put
   *  at its path after the device's load and loaded without tryLoadStore (Host.loadStore, tests): learned, as
   *  readAtLoad learns one. A device neither loaded nor seamed (a test's stand-in) has no records */
  private def known(site: String, id: StorageDeviceId, storeId: StoreId): Unit =
    if !attached.contains(id) && !ctx.devices.devices.contains(id) && ctx.unloadedDeviceCrl.isDefined then
      val copies = untracked.getOrElse(id, new DeviceCopies(id, os.root, s =>
        onChange(s)
        observer(untracked(id), s)))
      untracked += id -> copies
      if !copies.copies.contains(storeId) then
        copies.register(storeId, Phase.Live)
    attached.get(id).foreach: sds =>
      if !sds.copies.copies.contains(storeId) then
        val path = sds.devicePath / storeId.directoryName
        val phase = CopyPhase.known(path).getOrElse(Phase.Live)
        logger.warn(s"The copy of store $storeId at $path was not known to the copy registry. Learned at its " +
                    s"$site, $phase")
        learnedCopies :+= s"device $id: $storeId learned at $site, $phase"
        sds.copies.register(storeId, phase)

  /** The close of `storeId` about to be asked for is for `purpose` (DeviceShutdown, StoreLoader.handleClose): taken
   *  by the router's removal of the store */
  def closingFor(storeId: StoreId, purpose: ClosePurpose): Unit =
    closePurposes += storeId -> purpose

  /** The purpose a close of `storeId` was asked for, until the router removes the store */
  def closePurposeOf(storeId: StoreId): Option[ClosePurpose] = closePurposes.get(storeId)

  /** A close asked for with closingFor ended without the router removing the store (a test's close seam) */
  def closeAskEnded(storeId: StoreId): Unit =
    closePurposes -= storeId

  /** The router removed `storeId`, loaded from device `id`: its record is Closing, for the purpose its close was
   *  asked for (closingFor), else FailStop, until the close ends (closeEnded) */
  def removedFromRouter(id: StorageDeviceId, storeId: StoreId): Unit =
    val purpose = closePurposes.getOrElse(storeId, ClosePurpose.FailStop)
    closePurposes -= storeId
    fact("router remove"):
      device(id).foreach: copies =>
        if copies.copies.contains(storeId) then
          activityFact("router remove", copies, storeId, Activity.Closing(purpose))

  /** The close of `storeId`, loaded from device `id`, ended (StoreLoader: handleClose, closeUnloaded): its record is
   *  Idle, or AwaitingRetry if the loader recorded a retry for it while it closed */
  def closeEnded(id: StorageDeviceId, storeId: StoreId): Unit =
    fact("close ended"):
      device(id).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.Closing]) then
          activityFact("close ended", copies, storeId, Activity.Idle)
          takeParked("close ended", copies, storeId)

  /** StoreCopies.retire took the copy at `storeId`'s path on `local`, marked Lost, as its own. A retry the copy awaited
   *  is dropped: a retired copy never loads */
  def retiring(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    fact("retire"):
      copiesOf(local).foreach: copies =>
        parkedRetries -= ((copies.deviceId, storeId))
        if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.AwaitingRetry]) then
          activityFact("retire", copies, storeId, Activity.Idle)
        activityFact("retire", copies, storeId, Activity.Retiring)

  /** The retire of `storeId` on `local` ended. A copy it moved to pending-delete has no record any more */
  def retireEnded(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    fact("retire ended"):
      copiesOf(local).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity == Activity.Retiring) then
          activityFact("retire ended", copies, storeId, Activity.Idle)
          takeParked("retire ended", copies, storeId)
        else if !copies.copies.contains(storeId) then
          parkedRetries -= ((copies.deviceId, storeId))

  // ---- Work: the long-running work a (device, store) is in the middle of ----

  /** The work of `storeId` on device `id` of the kind `kind` is now `desired`: started, updated or ended to match.
   *  An id `desired` lacks is kept from the current work (a stage change made where the id is not known), unless
   *  `exact`. A claim: work of another kind already there is a second piece of work for the (device, store) (I3),
   *  refused (false), and a caller starting work backs off. An end never is */
  def work(site: String, id: StorageDeviceId, storeId: StoreId, kind: Work => Boolean, desired: Option[Work],
           exact: Boolean = false): Boolean =
    claim(site):
      device(id).foreach: copies =>
        val current = copies.work.get(storeId)
        desired match
          case None =>
            if current.exists(kind) then
              copies.endWork(storeId)
          case Some(w) =>
            current match
              case Some(c) if kind(c) =>
                val next = if exact then w else HostCopies.keepIds(c, w)
                if next != c then
                  copies.updateWork(storeId, next)
              case _ => copies.startWork(storeId, w)

  /** The stores with Rebuild work in one of `stages` on any device of the host. The one "is a rebuild of the store
   *  registered here" predicate; each caller names the stages it means (R20): RebuildReads where the rebuild reading
   *  the pool matters (repairable, the Completed Rebuilds pass, an earlier tenancy's retire, a second start),
   *  RebuildHolds where a rebuild holding its copy does (a load retry, a transfer out's start), all stages for any
   *  rebuild work (silence) */
  def rebuilding(stages: Set[RebuildStage] = RebuildStage.values.toSet): Set[StoreId] =
    allCopies.flatMap(_.work.collect { case (s, Work.Rebuild(_, stage)) if stages.contains(stage) => s }).toSet

  /** The Rebuild work of `storeId` on device `id`, if any */
  def rebuildWorkOn(id: StorageDeviceId, storeId: StoreId): Option[Work.Rebuild] =
    device(id).flatMap(_.work.get(storeId)).collect { case w: Work.Rebuild => w }

  /** The Rebuild work on device `id`, by store */
  def rebuildWorks(id: StorageDeviceId): Map[StoreId, Work.Rebuild] =
    device(id).map(_.work.collect { case (s, w: Work.Rebuild) => s -> w }).getOrElse(Map())

  /** How many (device, store) pairs hold discard work (RebuildStage.DiscardingLost), but `except`'s: each takes a
   *  rebuild slot, as a running rebuild does */
  def discards(except: Option[(StorageDeviceId, StoreId)] = None): Int =
    attached.toList.map: (id, sds) =>
      sds.copies.work.count:
        case (s, Work.Rebuild(_, RebuildStage.DiscardingLost)) => !except.contains((id, s))
        case _ => false
    .sum

  /** Whether a create of `storeId` runs on any device of the host (Work.Create) */
  def isCreating(storeId: StoreId): Boolean = allCopies.exists(_.work.get(storeId).contains(Work.Create))

  /** The TransferOut work of `storeId` on device `id`, if any */
  def transferOutOn(id: StorageDeviceId, storeId: StoreId): Option[Work.TransferOut] =
    device(id).flatMap(_.work.get(storeId)).collect { case w: Work.TransferOut => w }

  /** Whether TransferOut work of `storeId` in `stage` exists on any device of the host */
  def transferringOut(storeId: StoreId, stage: TransferOutStage): Boolean =
    allCopies.exists(_.work.get(storeId).exists:
      case Work.TransferOut(_, s) => s == stage
      case _ => false)

  /** The TransferIn work of `storeId` on device `id`, if any */
  def transferInOn(id: StorageDeviceId, storeId: StoreId): Option[Work.TransferIn] =
    device(id).flatMap(_.work.get(storeId)).collect { case w: Work.TransferIn => w }

  /** Whether TransferIn work of `storeId` in `stage` exists on any device of the host */
  def transferringIn(storeId: StoreId, stage: TransferInStage): Boolean =
    allCopies.exists(_.work.get(storeId).exists:
      case w: Work.TransferIn => w.stage == stage
      case _ => false)

  /** The receive attempts the host holds (Work.TransferIn's attempt), by store: the device and the attempt's id */
  def receiveAttempts: Map[StoreId, (StorageDeviceId, java.util.UUID)] =
    allCopies.toList.sortBy(_.deviceId.uuid).flatMap: c =>
      c.work.collect { case (s, Work.TransferIn(_, _, Some(a))) => s -> (c.deviceId, a) }
    .toMap

  /** Whether a copy of `storeId` is being retired on any device of the host (its Retiring activity) */
  def isRetiring(storeId: StoreId): Boolean =
    allCopies.exists(_.copies.get(storeId).exists(_.activity == Activity.Retiring))

  /** Whether an entry says no transfer out of its store is outstanding: Active, no destination named. A copy behind a
   *  transfer-out marker under it was left by a transfer that ended without completing */
  def noTransferOut(entry: StorageDeviceState.StoreEntry): Boolean =
    entry.status == StorageDeviceState.StoreStatus.Active && entry.transferDevice.isEmpty

  /** Whether a copy of `storeId` left offline by a transfer out may be put back in service under `entry`: the tree says
   *  no transfer is outstanding (noTransferOut), and this process is not sending it (a drain may restore the entry
   *  while the bytes still go). The Abandoned Transfers Out pass, an adoption's bringIntoService and
   *  reloadClosedStores ask it */
  def mayReinstateOut(entry: StorageDeviceState.StoreEntry, storeId: StoreId): Boolean =
    noTransferOut(entry) && !ctx.storeTransferService.isTransferringOut(storeId)

  /** The stores `local`'s Missing Stores pass declares (LocalStorageDeviceState.declaredMissing), mirrored */
  def missingIs(local: LocalStorageDeviceState): Unit =
    fact("declaredMissing"):
      copiesOf(local).foreach: copies =>
        (copies.missing -- local.declaredMissing).foreach(copies.clearMissing)
        (local.declaredMissing -- copies.missing).foreach(copies.declareMissing)

  // ---- Derived silence: the router's silent marks and the view's repairable stores ----

  private def allCopies: Iterable[DeviceCopies] = attached.values.map(_.copies) ++ untracked.values

  /** Whether the router should answer for `storeId` (spec, "Router silence is derived"): Served if a copy of it is
   *  Served on any device; otherwise Silent, with every reason that holds; otherwise Unknown. A create (Work.Create:
   *  its copy is registered only once at its path), and an entry a loaded device's latest reconcile read
   *  Initializing (a create to come, a failed one's included), are Opening reasons */
  def silence(storeId: StoreId): Silence =
    // A store installed with no record (a test's stand-in, served from a device neither loaded nor seamed) is served
    if allCopies.exists(_.copies.get(storeId).exists(_.activity == Activity.Served)) || ctx.router.get(storeId).isDefined
    then
      Silence.Served
    else
      val onDevices = allCopies.flatMap: c =>
        c.copies.get(storeId).map(r => r.activity match
          case Activity.Opening(_) => SilenceReason.Opening
          case _ => SilenceReason.CopyHere).toList ++
        c.work.get(storeId).toList.collect:
          case _: Work.Rebuild => SilenceReason.Rebuilding
          case _: Work.TransferIn => SilenceReason.TransferringIn
          case Work.Create => SilenceReason.Opening
        ++ (if c.missing.contains(storeId) then List(SilenceReason.Missing) else Nil)
      // An entry a loaded device's latest reconcile read Initializing: its create is pending, even after one that
      // failed and left nothing at the path, for the next check creates it again
      val creating = attached.values.exists(_.reconciledEntries.exists(_.get(storeId).exists(
        _.status == StorageDeviceState.StoreStatus.Initializing)))
      val reasons = onDevices.toSet ++
        (if creating then Set(SilenceReason.Opening) else Set()) ++
        (if unloaded.valuesIterator.exists(_.contains(storeId)) then Set(SilenceReason.UnloadedDevice) else Set()) ++
        (if awaiting.valuesIterator.exists(_.contains(storeId)) then Set(SilenceReason.DeviceUndecided) else Set())
      if reasons.isEmpty then Silence.Unknown else Silence.Silent(reasons)

  /** Whether a repair scan may run against `storeId` here: Served, not Closing, with no Rebuild work Running or
   *  Abandoned (cancelled, still running past finish's latch until its slot is released) and no TransferOut work on
   *  any device, and not being sent out (storeTransferService): `rebuilding(RebuildReads)`. A running rebuild
   *  reads everything; one queued or discarding its lost copy reads nothing yet, and a flip that keeps failing keeps
   *  its work queued, so it must not stop the served copy's repair scans (R20) */
  def repairable(storeId: StoreId): Boolean =
    silence(storeId) == Silence.Served &&
      !allCopies.exists(_.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.Closing])) &&
      !allCopies.exists(_.work.get(storeId).exists:
        case Work.Rebuild(_, RebuildStage.Running | RebuildStage.Abandoned) | Work.TransferOut(_, _) => true
        case _ => false) &&
      !ctx.storeTransferService.transferringOutStores.contains(storeId)

  /** At unload: the device's records go with it, and `listed` is kept as its unloaded entries */
  def detach(id: StorageDeviceId, listed: Set[StoreId]): Unit =
    val had = attached.get(id).map(_.copies)
    attached -= id
    parkedRetries = parkedRetries.filter((key, _) => key._1 != id)
    // A device unloaded before its first reconcile keeps what it listed before its load too
    val before = unloaded.getOrElse(id, Set())
    unloaded += id -> (listed ++ before)
    onChange(before ++ listed ++ had.toList.flatMap(c => c.copies.keySet ++ c.work.keySet ++ c.missing))

  /** The stores a device not loaded here lists (DeviceReconciler, a device check of an unloaded device) */
  def recordUnloadedEntries(id: StorageDeviceId, listed: Set[StoreId]): Unit =
    if !attached.contains(id) then
      val before = unloaded.getOrElse(id, Set())
      unloaded += id -> listed
      onChange(before ++ listed)

  /** The stores of the device directory `path` await its ownership decision */
  def awaitDevice(path: os.Path, stores: Set[StoreId]): Unit =
    val before = awaiting.getOrElse(path, Set())
    awaiting += path -> stores
    onChange(before ++ stores)

  /** The ownership of the device directory `path` has been decided */
  def deviceDecided(path: os.Path): Unit =
    awaiting.get(path).foreach: stores =>
      awaiting -= path
      onChange(stores)

  /** Whether `r` is neither served nor opening */
  private def isOffline(r: CopyRecord): Boolean =
    !(r.activity == Activity.Served || r.activity.isInstanceOf[Activity.Opening])

  /** The stores of `local`'s copies that the reconcile's passes may act on: Idle or awaiting a load retry (neither
   *  served, opening, closing nor retiring), with no work on the device. I3: a transfer in, rebuild, create or transfer
   *  out owns its copy, as a close or a retire does, and each one's end checks the device again */
  def offlineCopies(local: LocalStorageDeviceState): Set[StoreId] =
    copiesOf(local).map(c => c.copies.values.collect {
      case r if (r.activity == Activity.Idle || r.activity.isInstanceOf[Activity.AwaitingRetry]) &&
                !c.work.contains(r.storeId) => r.storeId
    }.toSet).getOrElse(Set())

  /** Whether `local` holds a copy of `storeId` that is neither served nor opening, whatever work it has (a load
   *  retry waits out that work rather than being dropped: StoreLoader.retryFailedLoads) */
  def heldOffline(local: LocalStorageDeviceState, storeId: StoreId): Boolean =
    copiesOf(local).exists(_.copies.get(storeId).exists(isOffline))

  /** The stores of `local`'s copies that are served or opening */
  def openCopies(local: LocalStorageDeviceState): Set[StoreId] =
    copiesOf(local).map(_.copies.values.collect { case r if !isOffline(r) => r.storeId }.toSet).getOrElse(Set())

  /** The registry of `sds`, while it is the attached state of its device */
  def copiesOf(sds: LocalStorageDeviceState): Option[DeviceCopies] =
    attached.get(sds.storageDeviceId).filter(_ eq sds).map(_.copies)

  /** The registry of the attached device whose directory holds the store directory `storePath`, with its store: the
   *  one with a record of it, where two attached states name the directory (a test's injected device) */
  private def copiesAt(storePath: os.Path): Option[(DeviceCopies, StoreId)] =
    for
      storeId <- HostCopies.storeIdOf(storePath)
      candidates = attached.values.toList.sortBy(_.storageDeviceId.uuid).filter(_.devicePath == storePath / os.up)
      sds <- candidates.find(_.copies.copies.contains(storeId)).orElse(candidates.headOption)
    yield (sds.copies, storeId)

  // ---- Updates from the disk mutation sites, right after each durable step: facts, but a new record's claim ----

  /** A new copy at `storeId`'s path on `sds`. A claim: refused (false) where a record exists */
  def registered(site: String, sds: LocalStorageDeviceState, storeId: StoreId, phase: Phase): Boolean =
    claim(site):
      copiesOf(sds).foreach(_.register(storeId, phase))

  /** A copy a rebuild produced at `storeId`'s path on `sds`: registered, unless the rebuild found its own finished
   *  copy there, which is already registered in `phase` */
  def rebuilt(site: String, sds: LocalStorageDeviceState, storeId: StoreId, phase: Phase): Unit =
    placed(site, sds, storeId, phase)

  /** The copy at `storePath` was marked rebuild-required: Lost, unless it already was */
  def markedLost(site: String, storePath: os.Path): Unit =
    fact(site):
      copiesAt(storePath).foreach: (copies, storeId) =>
        copies.copies.get(storeId) match
          case None =>
            // Put back at its path after the device's load (a restore finishing late): learned, as readAtLoad learns
            logger.warn(s"The copy of store $storeId at $storePath was not known to the copy registry. Learned at " +
                        s"$site, Lost")
            learnedCopies :+= s"device ${copies.deviceId}: $storeId learned at $site, Lost"
            copies.register(storeId, Phase.Lost)
          case Some(r) if r.phase != Phase.Lost => phaseFact(site, copies, storeId, Phase.Lost)
          case Some(_) => ()

  /** A marker was written to or removed from the copy at `storePath` (CopyMarkers): its phase is what its markers
   *  say now (CopyPhase), Unread where they cannot be read */
  def markersChanged(site: String, storePath: os.Path): Unit =
    fact(site):
      copiesAt(storePath).foreach: (copies, storeId) =>
        val now = CopyPhase.known(storePath).getOrElse(Phase.Unread)
        if !copies.copies.get(storeId).exists(_.phase == now) then
          copies.applyPhase(storeId, now, now).foreach(violation(site, _))

  /** A marker write or removal at `storePath` reached the directory but its sync failed (CopyMarkers): its copy is
   *  unsynced, never loaded, shipped or retired until a device check forces the directory again. A copy of a device
   *  not attached yet (an adoption, a lost CRL's marks) is held here until its attach */
  def unsynced(site: String, storePath: os.Path): Unit =
    fact(site):
      copiesAt(storePath) match
        case Some((copies, storeId)) if copies.copies.contains(storeId) => copies.setUnsynced(storeId, true)
        case _ => unsyncedPaths += storePath

  /** The directory at `storePath` was forced: its copy is synced */
  def synced(storePath: os.Path): Unit =
    unsyncedPaths -= storePath
    fact("synced"):
      copiesAt(storePath).foreach: (copies, storeId) =>
        if copies.copies.get(storeId).exists(_.unsynced) then
          copies.setUnsynced(storeId, false)

  /** Whether the copy at `storePath` is unsynced, on an attached device or awaiting its attach */
  def isUnsyncedAt(storePath: os.Path): Boolean =
    unsyncedPaths.contains(storePath) ||
      copiesAt(storePath).exists((copies, storeId) => copies.copies.get(storeId).exists(_.unsynced))

  /** Whether the copy of `storeId` on `sds` is unsynced */
  def isUnsynced(sds: LocalStorageDeviceState, storeId: StoreId): Boolean =
    copiesOf(sds).exists(_.copies.get(storeId).exists(_.unsynced))

  /** The unsynced copies of `local`, in store order */
  def unsyncedCopies(local: LocalStorageDeviceState): List[StoreId] =
    copiesOf(local).toList.flatMap(_.copies.values.filter(_.unsynced).map(_.storeId))
      .sortBy(s => (s.poolId.uuid, s.poolIndex))

  /** A copy now at `storeId`'s path on `sds`, in `phase`: registered, or, where a record is there already, moved to
   *  `phase`. A fact: the copy is at the path whatever the registry held */
  def placed(site: String, sds: LocalStorageDeviceState, storeId: StoreId, phase: Phase): Unit =
    fact(site):
      copiesOf(sds).foreach: copies =>
        copies.copies.get(storeId) match
          case Some(r) if r.phase == phase => ()
          case Some(_) => phaseFact(site, copies, storeId, phase)
          case None => copies.register(storeId, phase)

  /** The copy at `storeId`'s path on `sds` left it: retired into pending-delete, or deleted */
  def removed(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    fact(site):
      copiesOf(sds).foreach: copies =>
        parkedRetries -= ((copies.deviceId, storeId))
        copies.unregister(storeId)

  /** Test-only. For every loaded device, where its records and its disk disagree:
   *   - a record's phase is not the phase its directory's markers read, for records neither Unread nor unsynced;
   *   - a store directory with a config has no record;
   *   - a record has no directory, but one whose create is still placing it (Work.Create).
   *  Stats and reads that fail are skipped: they say nothing. Empty when consistent. */
  def verifyAgainstDisk(): List[String] =
    ctx.devices.devices.toList.sortBy(_._1.uuid).flatMap: (id, sds) =>
      if !attached.get(id).exists(_ eq sds) then
        List(s"device $id is loaded but not attached to the copy registry")
      else
        val records = sds.copies.copies
        val onDisk =
          try storeDirectories(sds.devicePath).filter((_, path) =>
            FileStat.existence(path / StoreConfig.configFilename).contains(true))
          catch case NonFatal(t) => Nil
        // A create's record is registered at its open, before its create places the copy (off the control thread
        // for FSIF): its directory may not be there yet
        val dirMissing = records.keys.toList.sortBy(_.directoryName).flatMap: storeId =>
          val path = sds.devicePath / storeId.directoryName
          if sds.copies.work.get(storeId).contains(Work.Create) then
            Nil
          else if Try(FileStat.isDir(path)).toOption.contains(false) then
            List(s"device $id: record of $storeId (${records(storeId).phase}) has no directory at $path")
          else
            Nil
        val unrecorded = onDisk.filterNot((storeId, _) => records.contains(storeId)).map: (storeId, path) =>
          s"device $id: directory $path (${CopyPhase.known(path)}) has no record"
        val wrongPhase = records.values.toList.sortBy(_.storeId.directoryName).flatMap: r =>
          val path = sds.devicePath / r.storeId.directoryName
          if r.phase == Phase.Unread || r.unsynced then
            Nil
          else
            CopyPhase.known(path) match
              case Some(p) if p != r.phase && Try(FileStat.isDir(path)).toOption.contains(true) =>
                List(s"device $id: record of ${r.storeId} is ${r.phase}, its directory reads $p")
              case _ => Nil
        dirMissing ++ unrecorded ++ wrongPhase

  /** The store directories under `devicePath`, by their names. None if it is not a directory; a listing or stat that
   *  fails throws */
  private def storeDirectories(devicePath: os.Path): List[(StoreId, os.Path)] =
    if !FileStat.isDir(devicePath) then
      Nil
    else
      os.list(devicePath).toList.flatMap: path =>
        HostCopies.storeIdOf(path).filter(_ => FileStat.isDir(path)).map(_ -> path)

/** Why a store the router does not serve is silent (HostCopies.silence) */
enum SilenceReason:
  case CopyHere, Opening, Rebuilding, TransferringIn, Missing, UnloadedDevice, DeviceUndecided

/** What the router should do with a message for a store (HostCopies.silence) */
enum Silence:
  case Served
  case Silent(reasons: Set[SilenceReason])
  case Unknown

object HostCopies:
  /** A rebuild holds its copy: it discards its lost copy or runs (the former StoreRebuilds.isRebuilding) */
  val RebuildHolds: Set[RebuildStage] = Set(RebuildStage.DiscardingLost, RebuildStage.Running, RebuildStage.Abandoned)
  /** A rebuild reads its pool: it runs, a cancelled one included until its slot is released (the former rebuildingStoreIds) */
  val RebuildReads: Set[RebuildStage] = Set(RebuildStage.Running, RebuildStage.Abandoned)

  /** `w`, with any id it lacks taken from `current`, of the same kind */
  private def keepIds(current: Work, w: Work): Work = (current, w) match
    case (Work.Rebuild(Some(e), _), Work.Rebuild(None, stage)) => Work.Rebuild(Some(e), stage)
    case (Work.TransferIn(Some(t), _, _), Work.TransferIn(None, stage, attempt)) => Work.TransferIn(Some(t), stage, attempt)
    case (Work.TransferOut(Some(t), _), Work.TransferOut(None, stage)) => Work.TransferOut(Some(t), stage)
    case _ => w

  /** The store a directory's name identifies, if it is a store directory name */
  def storeIdOf(path: os.Path): Option[StoreId] =
    Try(StoreId(path.last)).toOption.filter(_.directoryName == path.last)

/** A throw of the copy registry's silence path (HostCopies.onChange) when `strict`: never swallowed by `fact` or `claim` */
final class SilenceFailure(message: String, cause: Throwable) extends RuntimeException(message, cause)
