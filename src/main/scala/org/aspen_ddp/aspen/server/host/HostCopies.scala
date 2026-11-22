package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{FileStat, StoreConfig}
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

  /** A store loaded onto device `id`, which this host has not loaded, through the unloaded-device CRL seam, was
   *  installed: it has a record (Live, Idle) until the router removes it */
  def installedUntracked(id: StorageDeviceId, storeId: StoreId): Unit =
    shadow("installedUntracked"):
      val copies = untracked.getOrElse(id, new DeviceCopies(id, os.root, _ => ()))
      untracked += id -> copies
      if !copies.copies.contains(storeId) then
        copies.register(storeId, Phase.Live)

  /** The router removed `storeId`: an untracked record of it goes */
  def removedFromRouter(storeId: StoreId): Unit =
    shadow("removedFromRouter"):
      untracked.foreach: (id, copies) =>
        if copies.copies.contains(storeId) then
          copies.unregister(storeId)

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

object HostCopies:
  /** The store a directory's name identifies, if it is a store directory name */
  def storeIdOf(path: os.Path): Option[StoreId] =
    Try(StoreId(path.last)).toOption.filter(_.directoryName == path.last)
