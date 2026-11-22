package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore

import java.util.UUID
import scala.util.Try

/** Where each copy of a store lives on its device (spec "Device layout"): the location of a copy is its phase. Each
 *  phase is a directory under the device root, holding each copy as `<storeId.directoryName>.<copyId>`, with the
 *  phase's parameter in the copy's own file for that phase, `phase-param.<phase directory>` (paramFile): a move
 *  writes only its target phase's file, and a location reads only its own, so a move that fails or a crash before its
 *  rename leaves the source's parameter as it was, and a stale file of an earlier phase is inert. Live is `live/`,
 *  Arrived `arrived/` (its transfer id in `phase-param.arrived`), Outbound `outbound/` (its transfer id in
 *  `phase-param.outbound`), Rebuilt `rebuilt/` (its episode in `phase-param.rebuilt`), Parked `parked/`, Lost `lost/`
 *  and Retired `pending-delete/`. Code never builds a copy's path itself: it asks here, or the registry
 *  (HostCopies.pathOf). */
object CopyLayout:
  /** The directory of Live copies: loaded and served */
  val LiveDirectory = "live"

  /** The directory of Lost copies: their CRL state is gone or unusable, and they await their retire */
  val LostDirectory = "lost"

  /** The directory of Arrived copies: transferred in, awaiting their pool update (T8) */
  val ArrivedDirectory = "arrived"

  /** The directory of Outbound copies: closed for a transfer out, their state in crl_save.log (T6) */
  val OutboundDirectory = "outbound"

  /** The directory of Rebuilt copies: finished rebuilds awaiting their entry's flip out of Rebuilding (T2) */
  val RebuiltDirectory = "rebuilt"

  /** The directory of Parked copies: complete copies whose entry is Offline (an AdoptOffline rebuild's, T4) */
  val ParkedDirectory = "parked"

  /** The phase directories, by name, each with the phase of a child copy there (reading that phase's own parameter
   *  file, readParam, for a phase that carries one) */
  private val phaseDirectories: List[(String, os.Path => Phase)] = List(
    LiveDirectory -> (_ => Phase.Live),
    ArrivedDirectory -> (p => Phase.Arrived(readParam(p, Phase.Arrived("")))),
    OutboundDirectory -> (p => Phase.Outbound(readParam(p, Phase.Outbound("")))),
    RebuiltDirectory -> (p => Phase.Rebuilt(readParam(p, Phase.Rebuilt("")))),
    ParkedDirectory -> (_ => Phase.Parked),
    LostDirectory -> (_ => Phase.Lost),
    RebuildingStore.PendingDeleteDirectory -> (_ => Phase.Retired))

  /** Whether `name`, a child of a device root, is a phase directory (it holds copies; it is none itself) */
  def isPhaseDirectory(name: String): Boolean = phaseDirectories.exists(_._1 == name)

  /** The directory of `phase` under the device root */
  def dirOf(phase: Phase): String = phase match
    case Phase.Live => LiveDirectory
    case Phase.Arrived(_) => ArrivedDirectory
    case Phase.Outbound(_) => OutboundDirectory
    case Phase.Rebuilt(_) => RebuiltDirectory
    case Phase.Parked => ParkedDirectory
    case Phase.Lost => LostDirectory
    case Phase.Retired => RebuildingStore.PendingDeleteDirectory

  /** The phase a copy at `path` is in by its location: Some for a copy named as one (copyName) in a phase directory,
   *  None for any other path. Throws a failed read of the phase's parameter file */
  def phaseAt(path: os.Path): Option[Phase] =
    if nameOf(path).isEmpty then
      None
    else
      phaseDirectories.collectFirst { case (name, phaseOf) if (path / os.up).last == name => phaseOf(path) }

  /** Whether `path` names a copy in live/ (a Live copy's path), reading nothing */
  def isLivePath(path: os.Path): Boolean =
    nameOf(path).isDefined && (path / os.up).last == LiveDirectory

  /** The phase the copy of `storeId` named `copyId` is in on the device at `devicePath`: the phase directory holding
   *  it, None where none does. Throws a failed stat or parameter read */
  def locate(devicePath: os.Path, storeId: StoreId, copyId: UUID): Option[Phase] =
    val name = copyName(storeId, copyId)
    phaseDirectories.collectFirst:
      case (dir, phaseOf) if FileStat.exists(devicePath / dir / name) => phaseOf(devicePath / dir / name)

  def copyName(storeId: StoreId, copyId: UUID): String = s"${storeId.directoryName}.$copyId"

  /** The store and copy id the last segment of `path` names (parseName); None for the root or any other name */
  def nameOf(path: os.Path): Option[(StoreId, UUID)] =
    if path.segmentCount == 0 then None else parseName(path.last)

  /** The inverse of copyName; the store id comes from the name only */
  def parseName(name: String): Option[(StoreId, UUID)] =
    val dot = name.lastIndexOf('.')
    if dot <= 0 then
      None
    else
      val (dir, id) = (name.take(dot), name.drop(dot + 1))
      Try((StoreId(dir), UUID.fromString(id))).toOption.filter((s, u) => s.directoryName == dir && u.toString == id)

  /** The path of the copy of `storeId` with `copyId` in `phase` on the device at `devicePath` */
  def path(devicePath: os.Path, storeId: StoreId, copyId: UUID, phase: Phase): os.Path =
    devicePath / dirOf(phase) / copyName(storeId, copyId)

  /** Where a new copy of `storeId` is made Live on the device at `devicePath`, under a fresh copy id: a create's
   *  target (StoreRebuilds.createNewStore, bootstrap) */
  def freshLivePath(devicePath: os.Path, storeId: StoreId): os.Path =
    path(devicePath, storeId, UUID.randomUUID(), Phase.Live)

  /** The parameter `phase` carries: the transfer id of Arrived and Outbound, the episode of Rebuilt */
  def param(phase: Phase): Option[String] = phase match
    case Phase.Arrived(t) => Some(t)
    case Phase.Outbound(t) => Some(t)
    case Phase.Rebuilt(e) => Some(e)
    case _ => None

  /** The file in a copy holding `phase`'s parameter, `phase-param.<the phase's directory>`; None for a phase that
   *  carries none */
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

  final case class Found(storeId: StoreId, copyId: UUID, phase: Phase, path: os.Path)

  final case class Listing(copies: List[Found], strays: List[os.Path])

  /** Every copy on the device: the children of each phase directory. Throws a failed listing, stat or parameter
   *  read (P6: the device then stays unloaded for the next scan). A child whose name is not a copy name is skipped,
   *  and returned in `strays`. A copy without a `config.yaml` is listed all the same (a copy that lost it, which the
   *  Missing Stores pass rebuilds and a retire takes); whoever needs a configured store (the CRL-lost decision, the
   *  drain's disk check) tests the config itself. A device path that is not a directory lists nothing */
  def list(devicePath: os.Path): Listing =
    if !FileStat.isDir(devicePath) then
      Listing(Nil, Nil)
    else
      val inPhases = phaseDirectories.map: (name, phaseOf) =>
        val dir = devicePath / name
        if !FileStat.isDir(dir) then
          (Nil, Nil)
        else
          os.list(dir).toList.partitionMap: child =>
            parseName(child.last) match
              case Some((storeId, copyId)) => Left(Found(storeId, copyId, phaseOf(child), child))
              case None => Right(child)
      Listing(inPhases.flatMap(_._1), inPhases.flatMap(_._2))
