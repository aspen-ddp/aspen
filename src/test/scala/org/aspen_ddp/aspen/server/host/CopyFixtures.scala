package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore

import java.util.UUID

/** How tests place, find and read store copies on a device, wherever the layout puts each phase (CopyLayout). Tests
 *  never build a copy's path by hand: each later stage of the layout changes only these bodies. */
object CopyFixtures:

  /** Moves the copy of `storeId` on `sds` to `to` as `host`'s own moves do (CopyMoves.moveOnLane, one lane job), and
   *  returns the result: a refusal at once, otherwise once a drain of `host` has applied the job's result */
  def moveNow(host: org.aspen_ddp.aspen.server.Host, site: String, sds: LocalStorageDeviceState, storeId: StoreId,
              to: Phase): MoveResult =
    var result = Option.empty[MoveResult]
    host.control.moves.moveOnLane(site, sds, storeId, to)(r => result = Some(r))
    if result.isEmpty then host.testingOnlyHandleEvents()
    result.getOrElse(throw IllegalStateException(s"The move of $storeId to $to did not complete in a drain"))

  /** Places a configured copy of `storeId` in `phase` on the device at `deviceDir`, under a fresh copy id, returning
   *  its path. Writes the store config (as ControlFixture.writeStoreConfig), unless not `configured` (a copy holding
   *  nothing: a test's stand-in for a landed rebuild, or a copy that lost its config), and the phase's parameter
   *  file */
  def placeCopy(deviceDir: os.Path, storeId: StoreId, phase: Phase = Phase.Live, configured: Boolean = true): os.Path =
    val dir = freshCopyPath(deviceDir, storeId, phase)
    os.makeDir.all(dir)
    if configured then
      os.write.over(dir / StoreConfig.configFilename,
        StoreConfig(storeId, StoreConfig.RocksDB(None), None, Replication(3, 2)).yamlConfig)
    for
      file <- CopyLayout.paramFile(phase)
      p <- CopyLayout.param(phase)
    do os.write.over(dir / file, p)
    dir

  /** A fresh path for a copy of `storeId` in `phase` on the device (nothing made there): where a test builds a copy
   *  itself (a real backend's files) before its device's load */
  def freshCopyPath(deviceDir: os.Path, storeId: StoreId, phase: Phase = Phase.Live): os.Path =
    CopyLayout.path(deviceDir, storeId, UUID.randomUUID(), phase)

  /** Puts the copy of `storeId` already on the device in `phase`, keeping its files and its copy id, as something
   *  outside the host would (a crash's leftovers, a hand edit, a stand-in for a host site): returns its path. Throws
   *  if it has none. The copy is moved there (with the phase's parameter file), so on a loaded device the host's
   *  record does not follow: use it before the device's load, or move through the host */
  def markCopy(deviceDir: os.Path, storeId: StoreId, phase: Phase): os.Path =
    val found = one(deviceDir, storeId).getOrElse(
      throw IllegalStateException(s"$storeId has no copy on $deviceDir to mark"))
    val target = CopyLayout.path(deviceDir, storeId, found.copyId, phase)
    if target != found.path then
      os.makeDir.all(target / os.up)
      os.move(found.path, target)
    for
      file <- CopyLayout.paramFile(phase)
      p <- CopyLayout.param(phase)
    do os.write.over(target / file, p)
    target

  /** The copy id in the name of the copy at `path` */
  def copyIdOf(path: os.Path): UUID =
    CopyLayout.parseName(path.last).map(_._2).getOrElse(throw IllegalArgumentException(s"$path is no copy name"))

  /** The copy id of the one copy of `storeId` on the device, a fresh one if it has none */
  def copyIdOn(deviceDir: os.Path, storeId: StoreId): UUID =
    one(deviceDir, storeId).map(_.copyId).getOrElse(UUID.randomUUID())

  /** Where the one copy of `storeId` on the device is, or will be once moved, in live/: its copy id keeps across
   *  moves. Call it once the copy is there (absent, it names a fresh copy id) */
  def livePathOf(deviceDir: os.Path, storeId: StoreId): os.Path =
    CopyLayout.path(deviceDir, storeId, copyIdOn(deviceDir, storeId), Phase.Live)

  /** The copy of `storeId` on the device outside pending-delete/, with its phase and path, as a rebuild's start is
   *  given it (StoreRebuilds.startRebuild reads it from the copy registry) */
  def existingCopy(deviceDir: os.Path, storeId: StoreId): Option[(Phase, os.Path)] =
    copies(deviceDir, storeId).headOption.map(f => (f.phase, f.path))

  /** The copies of `storeId` on the device, outside pending-delete/ */
  private def copies(deviceDir: os.Path, storeId: StoreId): List[CopyLayout.Found] =
    CopyLayout.list(deviceDir).copies.filter(f => f.storeId == storeId && f.phase != Phase.Retired)

  private def one(deviceDir: os.Path, storeId: StoreId): Option[CopyLayout.Found] =
    copies(deviceDir, storeId) match
      case Nil => None
      case List(f) => Some(f)
      case many => throw IllegalStateException(s"$storeId has ${many.size} copies on $deviceDir: $many")

  /** The path of the one copy of `storeId` on the device, wherever its phase puts it; None if absent */
  def copyPath(deviceDir: os.Path, storeId: StoreId): Option[os.Path] = one(deviceDir, storeId).map(_.path)

  /** The phase of the one copy of `storeId` on the device, None if absent */
  def phaseOf(deviceDir: os.Path, storeId: StoreId): Option[Phase] = one(deviceDir, storeId).map(_.phase)

  /** The retired copies of `storeId` in pending-delete/, by copy name */
  def retiredCopies(deviceDir: os.Path, storeId: StoreId): List[os.Path] =
    val dir = deviceDir / RebuildingStore.PendingDeleteDirectory
    if !os.isDir(dir) then
      Nil
    else
      os.list(dir).toList.filter: p =>
        CopyLayout.parseName(p.last).map(_._1).contains(storeId)
