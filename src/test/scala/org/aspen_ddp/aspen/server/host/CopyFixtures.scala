package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID

/** How tests place, find and read store copies on a device, wherever the layout puts each phase (CopyLayout). Tests
 *  never build a copy's path or touch a marker by hand: each later stage of the layout changes only these bodies. */
object CopyFixtures:

  /** Places a configured copy of `storeId` in `phase` on the device at `deviceDir`, returning its path. Writes the
   *  store config (as ControlFixture.writeStoreConfig), unless not `configured` (a copy holding nothing: a test's
   *  stand-in for a landed rebuild, or a copy that lost its config), and the phase's representation (marker or
   *  directory and the phase's parameter file) */
  def placeCopy(deviceDir: os.Path, storeId: StoreId, phase: Phase = Phase.Live, configured: Boolean = true): os.Path =
    val dir = CopyLayout.dirOf(phase) match
      case None => deviceDir / storeId.directoryName
      case Some(_) => CopyLayout.path(deviceDir, storeId, Some(UUID.randomUUID()), phase)
    os.makeDir.all(dir)
    if configured then
      os.write.over(dir / StoreConfig.configFilename,
        StoreConfig(storeId, StoreConfig.RocksDB(None), None, Replication(3, 2)).yamlConfig)
    if CopyLayout.dirOf(phase).isDefined then
      for
        file <- CopyLayout.paramFile(phase)
        p <- CopyLayout.param(phase)
      do os.write.over(dir / file, p)
    else
      marker(phase).foreach((file, content) => os.write.over(dir / file, content))
    dir

  /** Puts the copy of `storeId` already on the device in `phase`, keeping its files, as something outside the host
   *  would (a crash's leftovers, a hand edit, a stand-in for a host site): returns its path. Throws if it has none. A
   *  phase directory's copy is moved there (with the phase's parameter file), so on a loaded device the host's record
   *  does not follow: use it before the device's load, or move through the host */
  def markCopy(deviceDir: os.Path, storeId: StoreId, phase: Phase): os.Path =
    val dir = copyPath(deviceDir, storeId).getOrElse(
      throw IllegalStateException(s"$storeId has no copy on $deviceDir to mark"))
    if CopyLayout.dirOf(phase).isDefined then
      val target = CopyLayout.path(deviceDir, storeId, Some(UUID.randomUUID()), phase)
      os.makeDir.all(target / os.up)
      os.move(dir, target)
      for
        file <- CopyLayout.paramFile(phase)
        p <- CopyLayout.param(phase)
      do os.write.over(target / file, p)
      target
    else
      marker(phase).foreach((file, content) => os.write.over(dir / file, content))
      dir

  /** The marker file and content a flat copy in `phase` carries */
  private def marker(phase: Phase): Option[(String, String)] = phase match
    case Phase.Live => None
    case Phase.Arrived(t) => Some(StoreTransfer.TransferredInMarkerFile -> t)
    case Phase.Rebuilt(e) => Some(RebuildingStore.RebuildCompleteMarkerFile -> e)
    case p => throw IllegalArgumentException(s"No copy can be placed in $p")

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
