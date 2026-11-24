package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.host.CopyLayout.{Found, Listing, parseName, paramFile, phaseDirectories}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore

import java.util.UUID
import scala.util.Try

/** The reads of CopyLayout's device layout: where a device's copies are and the phase parameters they carry. Device
 *  I/O, run only inside a job on the device's lane (the *Disk objects that call them) or by tests */
object CopyLayoutDisk:
  /** The phase of the child `copy` of the phase directory whose phase is `phase` (CopyLayout.phaseDirectories),
   *  reading its parameter for a phase that carries one */
  private def phaseOf(phase: Phase, copy: os.Path): Phase = phase match
    case Phase.Arrived(_) => Phase.Arrived(readParam(copy, phase))
    case Phase.Outbound(_) => Phase.Outbound(readParam(copy, phase))
    case Phase.Rebuilt(_) => Phase.Rebuilt(readParam(copy, phase))
    case other => other

  /** The phase a copy at `path` is in by its location: Some for a copy named as one (copyName) in a phase directory,
   *  None for any other path. Throws a failed read of the phase's parameter file */
  def phaseAt(path: os.Path): Option[Phase] =
    CopyLayout.phaseByLocation(path).map(phaseOf(_, path))

  /** The phase the copy of `storeId` named `copyId` is in on the device at `devicePath`: the phase directory holding
   *  it, None where none does. Throws a failed stat or parameter read */
  def locate(devicePath: os.Path, storeId: StoreId, copyId: UUID): Option[Phase] =
    val name = CopyLayout.copyName(storeId, copyId)
    phaseDirectories.collectFirst:
      case (dir, phase) if FileStat.isDir(devicePath / dir / name) => phaseOf(phase, devicePath / dir / name)

  /** Reads `phase`'s parameter file in the copy at `copyPath`: "" when known absent, or for a phase that carries
   *  none; throws a failed stat or read */
  def readParam(copyPath: os.Path, phase: Phase): String =
    paramFile(phase) match
      case None => ""
      case Some(name) =>
        val file = copyPath / name
        if FileStat.exists(file) then os.read(file).trim else ""

  /** The copies of `storeId` on the device at `devicePath` outside pending-delete/, as `list` finds them, reading only
   *  this store's: each phase directory's children named as a copy of it (one whose stat fails counts as a copy).
   *  Throws a failed listing, stat or parameter read. A device path that is not a directory holds none */
  def copiesOf(devicePath: os.Path, storeId: StoreId): List[Found] =
    if !FileStat.isDir(devicePath) then
      Nil
    else
      phaseDirectories.filter(_._1 != RebuildingStore.PendingDeleteDirectory).flatMap: (name, phase) =>
        val dir = devicePath / name
        if !FileStat.isDir(dir) then
          Nil
        else
          os.list(dir).toList.flatMap: child =>
            parseName(child.last).filter((s, _) => s == storeId && Try(FileStat.isDir(child)).getOrElse(true))
              .map((s, copyId) => Found(s, copyId, phaseOf(phase, child), child))

  /** Every copy on the device: the children of each phase directory. Throws a failed listing, stat or parameter
   *  read (P6: the device then stays unloaded for the next scan). A child that is not a directory named as a copy
   *  (copyName) is skipped, and returned in `strays`; one whose stat fails is listed. A copy without a `config.yaml`
   *  is listed all the same (a copy that lost it, which the Missing Stores pass rebuilds and a retire takes); whoever
   *  needs a configured store (the CRL-lost decision, the drain's disk check) tests the config itself. A device path
   *  that is not a directory lists nothing */
  def list(devicePath: os.Path): Listing =
    if !FileStat.isDir(devicePath) then
      Listing(Nil, Nil)
    else
      val inPhases = phaseDirectories.map: (name, phase) =>
        val dir = devicePath / name
        if !FileStat.isDir(dir) then
          (Nil, Nil)
        else
          // A copy is a directory named as one; a stat that fails counts as a copy, never as an absence (a store may
          // be there). Anything else is a stray
          os.list(dir).toList.partitionMap: child =>
            parseName(child.last).filter(_ => Try(FileStat.isDir(child)).getOrElse(true)) match
              case Some((storeId, copyId)) => Left(Found(storeId, copyId, phaseOf(phase, child), child))
              case None => Right(child)
      Listing(inPhases.flatMap(_._1), inPhases.flatMap(_._2))
