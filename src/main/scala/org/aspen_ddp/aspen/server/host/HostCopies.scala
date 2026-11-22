package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
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
 *  Shadow mode: every disk mutation site keeps the phases up to date, but nothing reads the registry. A registry call
 *  at a production site goes through `shadow`, which logs a throw (an illegal move, a missing record) at error with
 *  the site's name and swallows it, so a bookkeeping gap never becomes an outage. Each swallowed throw is recorded;
 *  `Host.testingOnlyHandleEvents` fails the drain on any of them, and on any mismatch `verifyAgainstDisk` finds, when
 *  `Host.Seams.verifyCopyRegistry` is set. */
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
  // Opens that began while their store had TransferOut work on the device (R1: refused from Task 7)
  private var transferOutOpens: Vector[String] = Vector()

  /** Test-only: the name of the test that built this host, for KnownDefects */
  var testName: String = ""

  /** Test-only: called after every change to a record, a work or a missing mark, with the device's copies */
  var observer: (DeviceCopies, StoreId) => Unit = (_, _) => ()

  /** Every open that began while its store had TransferOut work on its device, each as a message (R1) */
  def opensDuringTransferOut: Vector[String] = transferOutOpens

  /** The records of device `id`: a loaded one's, or those of the stores served from it untracked */
  def device(id: StorageDeviceId): Option[DeviceCopies] = attached.get(id).map(_.copies).orElse(untracked.get(id))

  /** Every copy a load learned of (readAtLoad), each as a message */
  def learned: Vector[String] = learnedCopies

  /** The unloaded devices' stores, by device */
  def unloadedEntries: Map[StorageDeviceId, Set[StoreId]] = unloaded

  /** The device directories awaiting their ownership decision, with their stores */
  def awaitingDevices: Map[os.Path, Set[StoreId]] = awaiting

  /** Runs `f`, a registry update at the production site `site`. A throw is logged, recorded and swallowed */
  def shadow(site: String)(f: => Unit): Unit =
    try f
    catch case NonFatal(t) =>
      logger.error(s"Copy registry ($site): $t")
      errors :+= s"$site: ${t.getMessage}"

  /** The throws `shadow` swallowed since the last call, which clears them */
  def takeShadowErrors(): Vector[String] =
    val e = errors
    errors = Vector()
    e

  /** At load, before any store is offered: registers each of the device's `children` (its listing) whose name is a
   *  store's and that is a directory with its phase, Unread where its stat or phase cannot be read. Never throws: the
   *  device is attached whatever the registration meets, and a copy it cannot read waits for its load to read it
   *  (readAtLoad) */
  def attach(sds: LocalStorageDeviceState, children: Seq[os.Path]): Unit =
    attached += sds.storageDeviceId -> sds
    unloaded -= sds.storageDeviceId
    sds.onCopyChange = s => observer(sds.copies, s)
    shadow("attach"):
      children.foreach: path =>
        HostCopies.storeIdOf(path).foreach: storeId =>
          Try(FileStat.isDir(path)) match
            case scala.util.Success(false) => ()
            case scala.util.Success(true) =>
              sds.copies.register(storeId, CopyPhase.known(path).getOrElse(Phase.Unread))
            case scala.util.Failure(_) => sds.copies.register(storeId, Phase.Unread)

  /** As attach, listing the device itself (ControlFixture.injectLoadedDevice). A listing that fails registers none */
  def attach(sds: LocalStorageDeviceState): Unit =
    val children =
      try
        if FileStat.isDir(sds.devicePath) then os.list(sds.devicePath).toList else Nil
      catch case NonFatal(t) =>
        shadow("attach")(throw t)
        Nil
    attach(sds, children)

  /** A load (StoreLoader.tryLoadStore) read the copy at `storePath`, whose config names `storeId`, in `phase`. A copy
   *  with no record is registered and reported as learned; an Unread record takes the phase; a record in another
   *  phase is a miswired site, recorded as a shadow error and left as it is */
  def readAtLoad(sds: LocalStorageDeviceState, storePath: os.Path, storeId: StoreId, phase: Phase): Unit =
    shadow("readAtLoad"):
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
              throw IllegalStateException(s"record of $storeId is $p, its load read $phase at $storePath")

  // ---- Activity (shadow): what the host is doing with each copy's open store ----

  /** The record of `storeId` on device `id`, attached or untracked */
  private def recordOf(id: StorageDeviceId, storeId: StoreId): Option[(DeviceCopies, CopyRecord)] =
    device(id).flatMap(c => c.copies.get(storeId).map(c -> _))

  /** StoreLoader.startOpen began open `openId` of the copy at `storeId`'s path on `sds`. A create has no record yet:
   *  it is registered, and set Opening, once its copy is at the path (createOpened). An open of a store with
   *  TransferOut work on the device is not refused yet (Task 7): the move is skipped and listed (opensDuringTransferOut) */
  def opening(sds: LocalStorageDeviceState, storeId: StoreId, openId: Long, create: Boolean): Unit =
    shadow("startOpen"):
      copiesOf(sds).foreach: copies =>
        if !create || copies.copies.contains(storeId) then
          if copies.work.get(storeId).exists(_.isInstanceOf[Work.TransferOut]) then
            logger.warn(s"Copy registry: store $storeId opens on device ${sds.storageDeviceId} during its transfer out")
            transferOutOpens :+= s"device ${sds.storageDeviceId}: $storeId opened during ${copies.work(storeId)}"
            // The open took the loader's retry, if any
            if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.AwaitingRetry]) then
              copies.setActivity(storeId, Activity.Idle)
          else
            copies.setActivity(storeId, Activity.Opening(openId))

  /** A create's copy is at its path and its open succeeded: the record registered at the path is Opening until its
   *  install */
  def createOpened(sds: LocalStorageDeviceState, storeId: StoreId, openId: Long): Unit =
    shadow("createOpened"):
      copiesOf(sds).foreach(_.setActivity(storeId, Activity.Opening(openId)))

  /** An open of `storeId` on `sds` ended without its store being served (StoreLoader.handleBackendOpened, handleLoad):
   *  an Opening record is Idle, unless the loader holds a retry for it (awaitingRetry took it already) */
  def openEnded(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    shadow(site):
      copiesOf(sds).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.Opening]) then
          copies.setActivity(storeId, Activity.Idle)

  /** The loader holds `retry` for `storeId` (storesAwaitingRetry). A record Closing takes it once its close ends
   *  (closeEnded), and a record Served never waits */
  def awaitingRetry(site: String, storeId: StoreId, retry: LoadRetry): Unit =
    shadow(site):
      copiesOf(retry.sds).foreach: copies =>
        copies.copies.get(storeId) match
          case None => throw IllegalStateException(s"No copy record for $storeId, awaiting a load retry")
          case Some(r) => r.activity match
            case Activity.Closing(_) => ()
            case _ => copies.setActivity(storeId, Activity.AwaitingRetry(retry))

  /** The loader dropped the retry of `storeId` on `sds` */
  def retryDropped(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    shadow(site):
      copiesOf(sds).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.AwaitingRetry]) then
          copies.setActivity(storeId, Activity.Idle)

  /** The router installed `storeId`, loaded from device `id` (StoreRouter.install): its record is Served. A store
   *  loaded onto a device this host has not loaded, through the unloaded-device CRL seam (tests), gets an untracked
   *  record (Live) first, kept until the host goes, as a copy's record is kept on a loaded device. A device neither
   *  loaded nor seamed (a test's stand-in) has no records */
  def installed(id: StorageDeviceId, storeId: StoreId): Unit =
    shadow("install"):
      if !attached.contains(id) && !ctx.devices.devices.contains(id) && ctx.unloadedDeviceCrl.isDefined then
        val copies = untracked.getOrElse(id, new DeviceCopies(id, os.root, s => observer(untracked(id), s)))
        untracked += id -> copies
        if !copies.copies.contains(storeId) then
          copies.register(storeId, Phase.Live)
      // Served -> Served: a reload over the served store (StoreRouter.install replacing an entry)
      device(id).foreach(_.setActivity(storeId, Activity.Served))

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
    shadow("router remove"):
      device(id).foreach: copies =>
        if copies.copies.contains(storeId) then
          copies.setActivity(storeId, Activity.Closing(purpose))

  /** The close of `storeId`, loaded from device `id`, ended (StoreLoader: handleClose, closeUnloaded): its record is
   *  Idle, or AwaitingRetry if the loader holds a retry for it on that device */
  def closeEnded(id: StorageDeviceId, storeId: StoreId): Unit =
    shadow("close ended"):
      device(id).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.Closing]) then
          copies.setActivity(storeId, Activity.Idle)
          ctx.loader.retryOf(storeId).filter(_.sds.storageDeviceId == id).foreach: retry =>
            copies.setActivity(storeId, Activity.AwaitingRetry(retry))

  /** StoreCopies.retire took the copy at `storeId`'s path on `local`, marked Lost, as its own */
  def retiring(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    shadow("retire"):
      copiesOf(local).foreach(_.setActivity(storeId, Activity.Retiring))

  /** The retire of `storeId` on `local` ended. A copy it moved to pending-delete has no record any more */
  def retireEnded(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    shadow("retire ended"):
      copiesOf(local).foreach: copies =>
        if copies.copies.get(storeId).exists(_.activity == Activity.Retiring) then
          copies.setActivity(storeId, Activity.Idle)

  // ---- Work (shadow): the long-running work a (device, store) is in the middle of ----

  /** The work of `storeId` on device `id` of the kind `kind` is now `desired`: started, updated or ended to match.
   *  Work of another kind already there is a second piece of work for the (device, store) (I3), a shadow error.
   *  An id `desired` lacks is kept from the current work (a stage change made where the id is not known) */
  def work(site: String, id: StorageDeviceId, storeId: StoreId, kind: Work => Boolean, desired: Option[Work]): Unit =
    shadow(site):
      device(id).foreach: copies =>
        val current = copies.work.get(storeId)
        desired match
          case None =>
            if current.exists(kind) then
              copies.endWork(storeId)
          case Some(w) =>
            current match
              case Some(c) if kind(c) =>
                val next = HostCopies.keepIds(c, w)
                if next != c then
                  copies.updateWork(storeId, next)
              case _ => copies.startWork(storeId, w)

  /** The stores `local`'s Missing Stores pass declares (LocalStorageDeviceState.declaredMissing), mirrored */
  def missingIs(local: LocalStorageDeviceState): Unit =
    shadow("declaredMissing"):
      copiesOf(local).foreach: copies =>
        (copies.missing -- local.declaredMissing).foreach(copies.clearMissing)
        (local.declaredMissing -- copies.missing).foreach(copies.declareMissing)

  // ---- Derived silence (shadow): compared with the router's marks after each test drain ----

  private def allCopies: Iterable[DeviceCopies] = attached.values.map(_.copies) ++ untracked.values

  /** Whether the router should answer for `storeId` (spec, "Router silence is derived"): Served if a copy of it is
   *  Served on any device; otherwise Silent, with every reason that holds; otherwise Unknown. A create's open in
   *  flight (Work.Create: its copy is registered only once at its path) is an Opening reason */
  def silence(storeId: StoreId): Silence =
    if allCopies.exists(_.copies.get(storeId).exists(_.activity == Activity.Served)) then
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
      val reasons = onDevices.toSet ++
        (if unloaded.valuesIterator.exists(_.contains(storeId)) then Set(SilenceReason.UnloadedDevice) else Set()) ++
        (if awaiting.valuesIterator.exists(_.contains(storeId)) then Set(SilenceReason.AwaitingDevice) else Set())
      if reasons.isEmpty then Silence.Unknown else Silence.Silent(reasons)

  /** Whether a repair scan may run against `storeId` here: Served, not Closing, with no Rebuild or TransferOut work on
   *  any device, and not being sent out (storeTransferService) */
  def repairable(storeId: StoreId): Boolean =
    silence(storeId) == Silence.Served &&
      !allCopies.exists(_.copies.get(storeId).exists(_.activity.isInstanceOf[Activity.Closing])) &&
      !allCopies.exists(_.work.get(storeId).exists(w => w.isInstanceOf[Work.Rebuild] ||
                                                       w.isInstanceOf[Work.TransferOut])) &&
      !ctx.storeTransferService.transferringOutStores.contains(storeId)

  /** Test-only. Each store whose silence or repairability the registry derives otherwise than the router's marks
   *  give it: silent there when not installed and offline, loading or awaiting its device; repairable when installed
   *  and not offline, rebuilding or transferring out (HostStoreView.of). Empty for a test whose name
   *  HostCopies.KnownDefects lists (a defect of the router's marks, Task 6 removes them) */
  def compareWithRouter(): List[String] =
    if HostCopies.KnownDefects.contains(testName) then
      Nil
    else
      val router = ctx.router
      val installed = router.loaded.keySet
      val offline = router.offlineStores
      val marked = offline ++ router.loadingStores ++ router.awaitingDeviceStores
      val rebuilding = ctx.rebuilds.rebuildingStoreIds
      val sending = ctx.storeTransferService.transferringOutStores
      val known = allCopies.flatMap(c => c.copies.keySet ++ c.work.keySet ++ c.missing) ++
        unloaded.values.flatten ++ awaiting.values.flatten
      (installed ++ marked ++ known).toList.sortBy(_.directoryName).flatMap: storeId =>
        val oldSilent = !installed.contains(storeId) && marked.contains(storeId)
        val derived = silence(storeId)
        val oldRepairable = installed.contains(storeId) && !offline.contains(storeId) &&
          !rebuilding.contains(storeId) && !sending.contains(storeId)
        val newRepairable = repairable(storeId)
        (if oldSilent != derived.isInstanceOf[Silence.Silent] then
           List(s"$storeId: the router's marks say ${if oldSilent then "silent" else "not silent"} (installed " +
                s"${installed.contains(storeId)}, offline ${offline.contains(storeId)}, loading " +
                s"${router.isLoading(storeId)}, awaiting device ${router.isAwaitingDevice(storeId)}), the registry " +
                s"$derived")
         else Nil) ++
        (if oldRepairable != newRepairable then
           List(s"$storeId: repairable by the router's view $oldRepairable, by the registry $newRepairable")
         else Nil)

  /** At unload: the device's records go with it, and `listed` is kept as its unloaded entries */
  def detach(id: StorageDeviceId, listed: Set[StoreId]): Unit =
    attached -= id
    unloaded += id -> listed

  /** The stores a device not loaded here lists (DeviceReconciler, a device check of an unloaded device) */
  def recordUnloadedEntries(id: StorageDeviceId, listed: Set[StoreId]): Unit =
    if !attached.contains(id) then
      unloaded += id -> listed

  /** The stores of the device directory `path` await its ownership decision */
  def awaitDevice(path: os.Path, stores: Set[StoreId]): Unit =
    awaiting += path -> stores
    stores.foreach(ctx.router.markAwaitingDevice)

  /** The ownership of the device directory `path` has been decided */
  def deviceDecided(path: os.Path): Unit =
    awaiting.get(path).foreach: stores =>
      awaiting -= path
      stores.foreach(ctx.router.clearAwaitingDevice)

  /** The registry of `sds`, while it is the attached state of its device */
  def copiesOf(sds: LocalStorageDeviceState): Option[DeviceCopies] =
    attached.get(sds.storageDeviceId).filter(_ eq sds).map(_.copies)

  /** The registry of the attached device whose directory holds the store directory `storePath`, with its store */
  private def copiesAt(storePath: os.Path): Option[(DeviceCopies, StoreId)] =
    for
      storeId <- HostCopies.storeIdOf(storePath)
      sds <- attached.valuesIterator.find(_.devicePath == storePath / os.up)
    yield (sds.copies, storeId)

  // ---- Updates from the disk mutation sites. Each runs under `shadow`, right after its durable step ----

  /** A new copy at `storeId`'s path on `sds` */
  def registered(site: String, sds: LocalStorageDeviceState, storeId: StoreId, phase: Phase): Unit =
    shadow(site):
      copiesOf(sds).foreach(_.register(storeId, phase))

  /** A copy a rebuild produced at `storeId`'s path on `sds`: registered, unless the rebuild found its own finished
   *  copy there, which is already registered in `phase` */
  def rebuilt(site: String, sds: LocalStorageDeviceState, storeId: StoreId, phase: Phase): Unit =
    shadow(site):
      copiesOf(sds).foreach: copies =>
        copies.copies.get(storeId) match
          case Some(r) if r.phase == phase => ()
          case Some(_) => copies.setPhase(storeId, phase)
          case None => copies.register(storeId, phase)

  /** The copy at `storeId`'s path on `sds` moved to `phase` */
  def moved(site: String, sds: LocalStorageDeviceState, storeId: StoreId, phase: Phase): Unit =
    shadow(site):
      copiesOf(sds).foreach(_.setPhase(storeId, phase))

  /** The copy at `storePath` was marked rebuild-required: Lost, unless it already was */
  def markedLost(site: String, storePath: os.Path): Unit =
    shadow(site):
      copiesAt(storePath).foreach: (copies, storeId) =>
        if !copies.copies.get(storeId).exists(_.phase == Phase.Lost) then
          copies.setPhase(storeId, Phase.Lost)

  /** A marker below the copy's phase marker, or its phase marker, was removed from the copy at `storePath`: its phase
   *  is what its remaining markers say (CopyPhase), Unread where they cannot be read */
  def markerRemoved(site: String, storePath: os.Path): Unit =
    shadow(site):
      copiesAt(storePath).foreach: (copies, storeId) =>
        val now = CopyPhase.known(storePath).getOrElse(Phase.Unread)
        if !copies.copies.get(storeId).exists(_.phase == now) then
          copies.setPhase(storeId, now)

  /** The copy at `storeId`'s path on `sds` left it: retired into pending-delete, or deleted */
  def removed(site: String, sds: LocalStorageDeviceState, storeId: StoreId): Unit =
    shadow(site):
      copiesOf(sds).foreach(_.unregister(storeId))

  /** Test-only. For every loaded device, where its records and its disk disagree:
   *   - a record's phase is not the phase its directory's markers read, for records neither Unread nor unsynced;
   *   - a store directory with a config has no record;
   *   - a record has no directory.
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
        val dirMissing = records.keys.toList.sortBy(_.directoryName).flatMap: storeId =>
          val path = sds.devicePath / storeId.directoryName
          if Try(FileStat.isDir(path)).toOption.contains(false) then
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
  case CopyHere, Opening, Rebuilding, TransferringIn, Missing, UnloadedDevice, AwaitingDevice

/** What the router should do with a message for a store (HostCopies.silence) */
enum Silence:
  case Served
  case Silent(reasons: Set[SilenceReason])
  case Unknown

object HostCopies:
  /** Tests whose drain disagrees with the router's marks through a defect of those marks (spec, Stage 1 audit
   *  findings, Step 3 addenda), by test name: compareWithRouter reports nothing for them until Task 6 removes the
   *  marks */
  val KnownDefects: Map[String, String] =
    val transferIn = "D1: a transfer in's store has no router mark on its destination while it is received and its " +
      "pool update runs, so the router answers UnknownStore once the pool names the destination and before the load"
    Map(
      "a transfer in registers its promoted copy Arrived and its pool update makes it Live (arrived copy)" -> transferIn,
      "a transfer in registers its promoted copy Arrived and its pool update makes it Live (shipped rebuilt copy)" ->
        transferIn,
      "a transfer in deletes an arrived copy of another transfer and unregisters it" -> transferIn,
      "a check of an unloaded device that lists a store served here takes it out of the router's repairable view" ->
        ("D2: a check of a device not loaded here (DeviceReconciler, the unloaded-device branch) marks every store " +
         "its entry lists offline by id, one served from another device of this host included"),
      "an open during a transfer out's close is listed, not refused, and the transfer does not start" ->
        ("D3: the view counts a store repairable while a transfer out is closing it, its CRL state being extracted " +
         "(an open during the close serves it; R1, refused from Task 7)"))

  /** `w`, with any id it lacks taken from `current`, of the same kind */
  private def keepIds(current: Work, w: Work): Work = (current, w) match
    case (Work.Rebuild(Some(e), _), Work.Rebuild(None, stage)) => Work.Rebuild(Some(e), stage)
    case (Work.TransferIn(Some(t), _), Work.TransferIn(None, stage)) => Work.TransferIn(Some(t), stage)
    case (Work.TransferOut(Some(t)), Work.TransferOut(None)) => Work.TransferOut(Some(t))
    case _ => w

  /** The store a directory's name identifies, if it is a store directory name */
  def storeIdOf(path: os.Path): Option[StoreId] =
    Try(StoreId(path.last)).toOption.filter(_.directoryName == path.last)
