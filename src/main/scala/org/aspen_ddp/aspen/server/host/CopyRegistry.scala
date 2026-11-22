package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.LoadRetry

/** Why a served store is being closed */
enum ClosePurpose:
  case TransferOut, Shutdown, Relinquish, FailStop

/** What the host is doing with a copy's open store right now */
enum Activity:
  case Idle
  case AwaitingRetry(retry: LoadRetry)
  case Opening(openId: Long)
  case Served
  case Closing(purpose: ClosePurpose)
  case Retiring

enum RebuildStage:
  case Queued, DiscardingLost, Running, Abandoned

enum TransferInStage:
  case RetiringEarlierTenancy, Receiving, Recording, Abandoning

enum TransferOutStage:
  /** From startOut's close (and CRL extraction) until the control task that writes its marker has run */
  case Closing
  /** returnLostTransferToRebuild is aborting the transfer of a copy marked rebuild-required */
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

/** The one in-memory record of a store copy on a device */
final case class CopyRecord(storeId: StoreId, phase: Phase, unsynced: Boolean, activity: Activity)

/** The stage-1 legal moves. Anything not listed is a bug in the caller */
object Transitions:

  def phaseLegal(from: Phase, to: Phase): Boolean = (from, to) match
    case (Phase.Unread, _) => true
    case (Phase.Lost, _) => false
    case (_, Phase.Unread) => true
    case (Phase.Live, Phase.Lost | Phase.Outbound(_)) => true
    // Outbound -> Rebuilt | Arrived: the transfer-out marker removed from a copy that also carries rebuild-complete
    // (an Unconfirmed rebuilt copy shipped, then reinstated) or transferred-in (a failed TI removal at the source)
    case (Phase.Outbound(_), Phase.Live | Phase.Lost | Phase.Rebuilt(_) | Phase.Arrived(_)) => true
    // Outbound("") -> Outbound(tid): a parked copy (an AdoptOffline rebuild's empty transfer-out marker) transferred out
    case (Phase.Outbound(""), Phase.Outbound(tid)) => tid.nonEmpty
    // Arrived -> Outbound: a transfer out of a copy whose transferred-in marker could not be removed
    case (Phase.Arrived(_), Phase.Rebuilt(_) | Phase.Live | Phase.Lost | Phase.Outbound(_)) => true
    case (Phase.Rebuilt(_), Phase.Live | Phase.Outbound(_) | Phase.Lost) => true
    case _ => false

  def activityLegal(phase: Phase, from: Activity, to: Activity): Boolean = (from, to) match
    case (Activity.Idle, Activity.Opening(_)) => phase == Phase.Live
    case (Activity.Idle, Activity.AwaitingRetry(_) | Activity.Served) => true
    case (Activity.Idle, Activity.Retiring) => phase == Phase.Lost
    case (Activity.AwaitingRetry(_), Activity.AwaitingRetry(_) | Activity.Opening(_) | Activity.Idle) => true
    case (Activity.Opening(_), Activity.Served | Activity.Idle | Activity.AwaitingRetry(_)) => true
    case (Activity.Served, Activity.Closing(_) | Activity.Served) => true
    case (Activity.Closing(_), Activity.Idle) => true
    case (Activity.Retiring, Activity.Idle) => true
    case _ => false

/** The copies of one device, owned by the control thread. Every mutator checks its move, applies it, then calls
 *  `onChange(storeId)`; a refused move throws IllegalStateException and changes nothing. */
final class DeviceCopies(val deviceId: StorageDeviceId, val devicePath: os.Path, onChange: StoreId => Unit):
  private var records: Map[StoreId, CopyRecord] = Map()
  private var works: Map[StoreId, Work] = Map()
  private var missingIds: Set[StoreId] = Set()

  def copies: Map[StoreId, CopyRecord] = records
  def work: Map[StoreId, Work] = works
  def missing: Set[StoreId] = missingIds

  private def record(storeId: StoreId): CopyRecord =
    records.getOrElse(storeId, throw IllegalStateException(s"No copy record for $storeId"))

  /** A new record. Throws where one exists */
  def register(storeId: StoreId, phase: Phase): Unit =
    if records.contains(storeId) then
      throw IllegalStateException(s"Copy record for $storeId already exists")
    records += storeId -> CopyRecord(storeId, phase, false, Activity.Idle)
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

  /** A fact: the copy's markers moved it to `to`. A move the table does not allow sets what its directory reads now
   *  (`onDisk`) instead, and returns its description, for the caller to record */
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
      Some(s"Illegal phase move for $storeId: ${r.phase} -> $to; recorded $disk, as its directory reads")

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
