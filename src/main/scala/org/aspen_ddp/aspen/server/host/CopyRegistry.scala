package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.LoadRetry

/** Why a store is being closed. FailStop: the copy moves to lost/ when its close ends (P5). Reload: the copy stays
 *  where it is, to load again (a failed CRL load, a failed save-file removal). Unserved: a backend never served, which
 *  must never serve, closes (StoreRebuilds.retireUnserved), and the copy moves to lost/ when its close ends; the only
 *  close of a copy that is not Served */
enum ClosePurpose:
  case TransferOut, Shutdown, Relinquish, FailStop, Reload, Unserved

/** What the host is doing with a copy's open store right now */
enum Activity:
  case Idle
  case AwaitingRetry(retry: LoadRetry)
  case Opening(openId: Long)
  case Served
  case Closing(purpose: ClosePurpose)
  case Retiring
  /** A disk job of `kind` for the copy runs on its device's lane (DeviceIo). The copy is busy until its result is
   *  applied: no reconcile action fires for it (CopyDecision.decide), and `prior`, the activity it was in (Idle,
   *  Served or AwaitingRetry), comes back when the job ends (HostCopies.diskJobEnded) */
  case DiskJob(kind: JobKind, prior: Activity)

  /** The activity the copy is in apart from any disk job in flight */
  def underlying: Activity = this match
    case DiskJob(_, prior) => prior.underlying
    case a => a

enum RebuildStage:
  case Queued, DiscardingLost, Running, Abandoned

enum TransferInStage:
  case RetiringEarlierTenancy, Receiving, Recording, Abandoning

enum TransferOutStage:
  /** From startOut's close (and CRL extraction) until the control task that moves its copy to outbound/ has run */
  case Closing
  /** returnLostTransferToRebuild is aborting the transfer of a Lost copy */
  case AbortingLost

/** The long-running work a copy is in the middle of. Ids come from tree entries, so they may be absent */
enum Work:
  case Create
  case Rebuild(episode: Option[UUID], stage: RebuildStage)
  /** `attempt`: the receive attempt this host holds for the store (StoreTransfers.startIn's transfer UUID), from its
   *  start until its pool update is done, it is forgotten (a shutdown, an abandon) or it is restarted. A stage may
   *  outlive it (Recording past a shutdown's forget) or overlay it (Abandoning while it waits on bytes) */
  case TransferIn(transferId: Option[UUID], stage: TransferInStage, attempt: Option[UUID] = None)
  case TransferOut(transferId: Option[UUID], stage: TransferOutStage = TransferOutStage.Closing)

/** The one in-memory record of a store copy on a device. `copyId` is assigned when the copy's directory first enters
 *  a phase directory (a create's rename into live/, a rebuild's finish into rebuilt/, a transfer's promotion into
 *  arrived/; P4) and never changes on the device */
final case class CopyRecord(storeId: StoreId, copyId: UUID, phase: Phase, unsynced: Boolean, activity: Activity)

/** The legal phase moves (spec "Phases and transitions", T1-T13; T1, T2, T8 place a new copy, T12 and T13 remove
 *  one). Anything not listed is a bug in the caller */
object Transitions:

  def phaseLegal(from: Phase, to: Phase): Boolean = (from, to) match
    // Every retire goes through Lost (T11)
    case (Phase.Lost, Phase.Retired) => true
    case (Phase.Lost, _) => false
    case (Phase.Retired, _) | (_, Phase.Retired) => false
    // T6 from Live, T10
    case (Phase.Live, Phase.Lost | Phase.Outbound(_)) => true
    // T7 (an aborted or abandoned transfer out reinstated; also an Outbound copy of another transfer moved back
    // before its new move to outbound/), T10. Never back to Rebuilt, Parked or Arrived: a reinstated copy is Live (P8)
    case (Phase.Outbound(_), Phase.Live | Phase.Lost) => true
    // T9, T10
    case (Phase.Arrived(_), Phase.Live | Phase.Lost) => true
    // T3, T4, T6 (an Unconfirmed rebuilt copy shipped), T10
    case (Phase.Rebuilt(_), Phase.Live | Phase.Parked | Phase.Outbound(_) | Phase.Lost) => true
    // T5, T6 (a parked copy shipped), T10
    case (Phase.Parked, Phase.Live | Phase.Outbound(_) | Phase.Lost) => true
    case _ => false

  def activityLegal(phase: Phase, from: Activity, to: Activity): Boolean = (from, to) match
    case (Activity.Idle, Activity.Opening(_)) => phase == Phase.Live
    case (Activity.Idle, Activity.AwaitingRetry(_) | Activity.Served) => true
    case (Activity.Idle, Activity.Retiring) => phase == Phase.Lost
    case (Activity.AwaitingRetry(_), Activity.AwaitingRetry(_) | Activity.Opening(_) | Activity.Idle) => true
    case (Activity.Opening(_), Activity.Served | Activity.Idle | Activity.AwaitingRetry(_)) => true
    case (Activity.Served, Activity.Closing(_) | Activity.Served) => true
    // An unserved backend that must never serve closing (StoreRebuilds.retireUnserved): its copy is held Closing until
    // the close ends, so no load opens it meanwhile
    case (Activity.Idle | Activity.AwaitingRetry(_) | Activity.Opening(_), Activity.Closing(ClosePurpose.Unserved)) =>
      phase == Phase.Live
    case (Activity.Closing(_), Activity.Idle) => true
    // A disk job holds an Idle, Served or awaiting-retry copy, and gives it back as it was
    case (Activity.Idle | Activity.Served | Activity.AwaitingRetry(_), Activity.DiskJob(_, prior)) => prior == from
    case (Activity.DiskJob(_, prior), _) => to == prior
    case (Activity.Retiring, Activity.Idle) => true
    case _ => false

/** The copies of one device, owned by the control thread. Every mutator checks its move, applies it, then calls
 *  `onChange(storeId)`; a refused move throws IllegalStateException and changes nothing. */
final class DeviceCopies(val deviceId: StorageDeviceId, val devicePath: os.Path, onChange: StoreId => Unit):
  private var records: Map[StoreId, CopyRecord] = Map()
  private var works: Map[StoreId, Work] = Map()
  private var missingIds: Set[StoreId] = Set()

  def copies: Map[StoreId, CopyRecord] = records

  /** The path of the copy of `storeId`, from its record (CopyLayout.path); None without one */
  def pathOf(storeId: StoreId): Option[os.Path] =
    records.get(storeId).map(r => CopyLayout.path(devicePath, storeId, r.copyId, r.phase))

  def work: Map[StoreId, Work] = works
  def missing: Set[StoreId] = missingIds

  private def record(storeId: StoreId): CopyRecord =
    records.getOrElse(storeId, throw IllegalStateException(s"No copy record for $storeId"))

  /** A new record. Throws where one exists */
  def register(storeId: StoreId, phase: Phase, copyId: UUID, unsynced: Boolean = false): Unit =
    if records.contains(storeId) then
      throw IllegalStateException(s"Copy record for $storeId already exists")
    records += storeId -> CopyRecord(storeId, copyId, phase, unsynced, Activity.Idle)
    onChange(storeId)

  def setPhase(storeId: StoreId, to: Phase): Unit =
    val r = record(storeId)
    if !Transitions.phaseLegal(r.phase, to) then
      throw IllegalStateException(s"Illegal phase move for $storeId: ${r.phase} -> $to")
    records += storeId -> r.copy(phase = to)
    onChange(storeId)

  def setActivity(storeId: StoreId, to: Activity): Unit =
    val r = record(storeId)
    if !Transitions.activityLegal(r.phase, r.activity, to) then
      throw IllegalStateException(s"Illegal activity move for $storeId at ${r.phase}: ${r.activity} -> $to")
    to match
      case Activity.Opening(_) if works.get(storeId).exists(_.isInstanceOf[Work.TransferOut]) =>
        throw IllegalStateException(s"$storeId cannot open during a transfer out")
      case _ =>
    records += storeId -> r.copy(activity = to)
    onChange(storeId)

  /** A fact (HostCopies.fact): the activity move already happened, so it is applied whatever the table says. Returns
   *  the description of a move the table does not allow, for the caller to record */
  def applyActivity(storeId: StoreId, to: Activity): Option[String] =
    val r = record(storeId)
    val illegal = Option.when(!Transitions.activityLegal(r.phase, r.activity, to))(
      s"Illegal activity move for $storeId at ${r.phase}: ${r.activity} -> $to, applied as a fact")
    records += storeId -> r.copy(activity = to)
    onChange(storeId)
    illegal

  /** A fact: the copy is in `to` (a load read it there). A move the table does not allow sets where the copy is now
   *  (`onDisk`, its location) instead, and returns its description, for the caller to record */
  def applyPhase(storeId: StoreId, to: Phase, onDisk: => Phase): Option[String] =
    val r = record(storeId)
    if Transitions.phaseLegal(r.phase, to) then
      records += storeId -> r.copy(phase = to)
      onChange(storeId)
      None
    else
      val disk = onDisk
      records += storeId -> r.copy(phase = disk)
      onChange(storeId)
      Some(s"Illegal phase move for $storeId: ${r.phase} -> $to; recorded $disk, where the copy is")

  /** A fact: a move (CopyMoves) put the copy at its path in `to`, as `copyId`, durably unless `unsynced`. A copy
   *  unsynced before the move stays so: the move forced only the directories naming it. The copy's location is its
   *  phase, so a move the table does not allow is applied all the same, and its description returned for the caller
   *  to record */
  def applyMove(storeId: StoreId, to: Phase, copyId: UUID, unsynced: Boolean): Option[String] =
    val r = record(storeId)
    val illegal = Option.when(!Transitions.phaseLegal(r.phase, to))(
      s"Illegal phase move for $storeId: ${r.phase} -> $to, applied as a fact")
    records += storeId -> r.copy(copyId = copyId, phase = to, unsynced = unsynced || r.unsynced)
    onChange(storeId)
    illegal

  def setUnsynced(storeId: StoreId, unsynced: Boolean): Unit =
    records += storeId -> record(storeId).copy(unsynced = unsynced)
    onChange(storeId)

  /** The copy left its path: pending-delete or deleted */
  def unregister(storeId: StoreId): Unit =
    record(storeId)
    records -= storeId
    onChange(storeId)

  /** Throws where work exists */
  def startWork(storeId: StoreId, w: Work): Unit =
    if works.contains(storeId) then
      throw IllegalStateException(s"$storeId already has work ${works(storeId)}")
    works += storeId -> w
    onChange(storeId)

  /** Throws where work is absent or of another case */
  def updateWork(storeId: StoreId, w: Work): Unit =
    val current = works.getOrElse(storeId, throw IllegalStateException(s"$storeId has no work"))
    if current.ordinal != w.ordinal then
      throw IllegalStateException(s"$storeId work $current cannot become $w")
    works += storeId -> w
    onChange(storeId)

  def endWork(storeId: StoreId): Unit =
    if !works.contains(storeId) then
      throw IllegalStateException(s"$storeId has no work")
    works -= storeId
    onChange(storeId)

  def declareMissing(storeId: StoreId): Unit =
    missingIds += storeId
    onChange(storeId)

  def clearMissing(storeId: StoreId): Unit =
    missingIds -= storeId
    onChange(storeId)
