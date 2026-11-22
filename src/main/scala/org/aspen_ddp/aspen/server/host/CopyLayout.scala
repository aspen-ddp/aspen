package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore

import java.util.UUID
import scala.util.Try

/** Where each copy of a store lives on its device (spec "Device layout"): the location of a copy is its phase. A
 *  converted phase is a directory under the device root, holding each copy as `<storeId.directoryName>.<copyId>`,
 *  with the phase's parameter in the copy's `phase-param`. A phase not converted yet is the flat path,
 *  `<device>/<storeId.directoryName>`, whose marker files say the phase (CopyPhase). Code never builds a copy's path
 *  itself: it asks here, or the registry (HostCopies.pathOf).
 *
 *  Stage 2 converts the phases one task at a time; so far only Retired (`pending-delete/`) is a directory. */
object CopyLayout:
  val PhaseParamFile = "phase-param"

  /** The converted phase directories, by name, each with the phase its children are in given their `phase-param`
   *  (read only by a phase that carries one). Each task that converts a phase adds its row */
  private val phaseDirectories: List[(String, (=> String) => Phase)] = List(
    RebuildingStore.PendingDeleteDirectory -> (_ => Phase.Retired))

  /** The directory of `phase` under the device root, None while the phase is still a flat marker copy */
  def dirOf(phase: Phase): Option[String] = phase match
    case Phase.Retired => Some(RebuildingStore.PendingDeleteDirectory)
    case _ => None

  def copyName(storeId: StoreId, copyId: UUID): String = s"${storeId.directoryName}.$copyId"

  /** The inverse of copyName; the store id comes from the name only */
  def parseName(name: String): Option[(StoreId, UUID)] =
    val dot = name.lastIndexOf('.')
    if dot <= 0 then
      None
    else
      val (dir, id) = (name.take(dot), name.drop(dot + 1))
      Try((StoreId(dir), UUID.fromString(id))).toOption.filter((s, u) => s.directoryName == dir && u.toString == id)

  /** The path of the copy of `storeId` with `copyId` in `phase` on the device at `devicePath`. A copy in a converted
   *  phase must have its copyId */
  def path(devicePath: os.Path, storeId: StoreId, copyId: Option[UUID], phase: Phase): os.Path =
    dirOf(phase) match
      case None => devicePath / storeId.directoryName
      case Some(dir) =>
        val id = copyId.getOrElse(throw IllegalArgumentException(s"A copy of $storeId in $phase needs its copy id"))
        devicePath / dir / copyName(storeId, id)

  /** The parameter `phase` carries: the transfer id of Arrived and Outbound, the episode of Rebuilt */
  def param(phase: Phase): Option[String] = phase match
    case Phase.Arrived(t) => Some(t)
    case Phase.Outbound(t) => Some(t)
    case Phase.Rebuilt(e) => Some(e)
    case _ => None

  /** Reads `phase-param` of the copy at `copyPath`: "" when known absent; throws a failed stat or read */
  def readParam(copyPath: os.Path): String =
    val file = copyPath / PhaseParamFile
    if FileStat.exists(file) then os.read(file).trim else ""

  final case class Found(storeId: StoreId, copyId: Option[UUID], phase: Phase, path: os.Path)

  final case class Listing(copies: List[Found], strays: List[os.Path])

  /** Every copy on the device: flat store directories (phase from CopyPhase.read) and children of each converted
   *  phase directory. Throws a failed listing or param read. A child whose name is not a copy name is skipped, and
   *  returned in `strays`.
   *
   *  A flat child counts as a copy when its name is a store's directory name (so no phase or scratch directory, CRL
   *  directory or device file ever does) and it is a directory, or its stat fails (it may be a store). One without a
   *  `config.yaml` is a copy that lost it, which the Missing Stores pass rebuilds and a retire takes; whoever needs a
   *  configured store (the CRL-lost decision, the drain's disk check) tests the config itself. Its phase is Unread
   *  where its markers cannot be read: its load reads them again. A device path that is not a directory lists
   *  nothing */
  def list(devicePath: os.Path): Listing =
    if !FileStat.isDir(devicePath) then
      Listing(Nil, Nil)
    else
      val flat = os.list(devicePath).toList.flatMap: child =>
        // A stat that fails is no absence: the child may be a store
        HostCopies.storeIdOf(child).filter(_ => Try(FileStat.isDir(child)).getOrElse(true))
        .map(storeId => Found(storeId, None, CopyPhase.known(child).getOrElse(Phase.Unread), child))
      val inPhases = phaseDirectories.map: (name, phaseOf) =>
        val dir = devicePath / name
        if !FileStat.isDir(dir) then
          (Nil, Nil)
        else
          os.list(dir).toList.partitionMap: child =>
            parseName(child.last) match
              case Some((storeId, copyId)) => Left(Found(storeId, Some(copyId), phaseOf(readParam(child)), child))
              case None => Right(child)
      Listing(flat ++ inPhases.flatMap(_._1), inPhases.flatMap(_._2))
