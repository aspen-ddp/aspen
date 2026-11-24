package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
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
 *  (HostCopies.pathOf). The functions that read a device are CopyLayoutDisk's. */
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

  /** The phase directories, by name, each with the phase of a child copy there, its parameter (for a phase that
   *  carries one) left "": CopyLayoutDisk reads it from the copy's own parameter file */
  private[host] val phaseDirectories: List[(String, Phase)] = List(
    LiveDirectory -> Phase.Live,
    ArrivedDirectory -> Phase.Arrived(""),
    OutboundDirectory -> Phase.Outbound(""),
    RebuiltDirectory -> Phase.Rebuilt(""),
    ParkedDirectory -> Phase.Parked,
    LostDirectory -> Phase.Lost,
    RebuildingStore.PendingDeleteDirectory -> Phase.Retired)

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

  /** The phase directory a copy at `path` is in, its parameter left "" (reading nothing; CopyLayoutDisk.phaseAt
   *  reads it): Some for a copy named as one (copyName) in a phase directory, None for any other path */
  def phaseByLocation(path: os.Path): Option[Phase] =
    if nameOf(path).isEmpty then
      None
    else
      phaseDirectories.collectFirst { case (name, phase) if (path / os.up).last == name => phase }

  /** Whether `path` names a copy in live/ (a Live copy's path), reading nothing */
  def isLivePath(path: os.Path): Boolean =
    nameOf(path).isDefined && (path / os.up).last == LiveDirectory

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

  final case class Found(storeId: StoreId, copyId: UUID, phase: Phase, path: os.Path)

  final case class Listing(copies: List[Found], strays: List[os.Path])
