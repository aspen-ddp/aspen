package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore

import java.util.UUID
import scala.util.Try

/** Where each copy of a store lives on its device (spec "Device layout"): the location of a copy is its phase. A
 *  converted phase is a directory under the device root, holding each copy as `<storeId.directoryName>.<copyId>`,
 *  with the phase's parameter in the copy's own file for that phase, `phase-param.<phase directory>` (paramFile): a
 *  move writes only its target phase's file, and a location reads only its own, so a move that fails or a crash
 *  before its rename leaves the source's parameter as it was, and a stale file of an earlier phase is inert. A phase
 *  not converted yet is the flat path, `<device>/<storeId.directoryName>`, whose marker files say the phase
 *  (CopyPhase). Code never builds a copy's path
 *  itself: it asks here, or the registry (HostCopies.pathOf).
 *
 *  Stage 2 converts the phases one task at a time; so far Lost (`lost/`) and Retired (`pending-delete/`) are
 *  directories. */
object CopyLayout:
  /** The directory of Lost copies: their CRL state is gone or unusable, and they await their retire */
  val LostDirectory = "lost"

  /** The converted phase directories, by name, each with the phase of a child copy there (reading that phase's own
   *  parameter file, readParam, for a phase that carries one). Each task that converts a phase adds its row */
  private val phaseDirectories: List[(String, os.Path => Phase)] = List(
    LostDirectory -> (_ => Phase.Lost),
    RebuildingStore.PendingDeleteDirectory -> (_ => Phase.Retired))

  /** Whether `name`, a child of a device root, is a phase directory (it holds copies; it is none itself) */
  def isPhaseDirectory(name: String): Boolean = phaseDirectories.exists(_._1 == name)

  /** The directory of `phase` under the device root, None while the phase is still a flat marker copy */
  def dirOf(phase: Phase): Option[String] = phase match
    case Phase.Lost => Some(LostDirectory)
    case Phase.Retired => Some(RebuildingStore.PendingDeleteDirectory)
    case _ => None

  /** The phase a copy at `path` is in by its location: Some for a copy named as one (copyName) in a converted phase
   *  directory, None for any other path (a flat copy, whose markers say its phase) */
  def phaseAt(path: os.Path): Option[Phase] =
    if parseName(path.last).isEmpty then
      None
    else
      phaseDirectories.collectFirst { case (name, phaseOf) if (path / os.up).last == name => phaseOf(path) }

  /** The phase of the copy at `path`: by location in a phase directory (phaseAt), else what its markers read
   *  (CopyPhase.known), None where they cannot be read */
  def knownPhase(path: os.Path): Option[Phase] = phaseAt(path).orElse(CopyPhase.known(path))

  /** As knownPhase, throwing a failed read (CopyPhase.read) */
  def readPhase(path: os.Path): Phase = phaseAt(path).getOrElse(CopyPhase.read(path))

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

  /** The file in a copy holding `phase`'s parameter, `phase-param.<the phase's directory>`; None for a phase that
   *  carries none. Named for the spec's directory whether or not the phase is converted yet */
  def paramFile(phase: Phase): Option[String] = phase match
    case Phase.Arrived(_) => Some("phase-param.arrived")
    case Phase.Outbound(_) => Some("phase-param.outbound")
    case Phase.Rebuilt(_) => Some("phase-param.rebuilt")
    case _ => None

  /** Reads `phase`'s parameter file in the copy at `copyPath`: "" when known absent, or for a phase that carries
   *  none; throws a failed stat or read */
  def readParam(copyPath: os.Path, phase: Phase): String =
    paramFile(phase) match
      case None => ""
      case Some(name) =>
        val file = copyPath / name
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
              case Some((storeId, copyId)) => Left(Found(storeId, Some(copyId), phaseOf(child), child))
              case None => Right(child)
      Listing(flat ++ inPhases.flatMap(_._1), inPhases.flatMap(_._2))
