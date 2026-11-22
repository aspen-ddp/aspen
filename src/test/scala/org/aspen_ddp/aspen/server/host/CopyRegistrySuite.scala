package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host.LoadRetry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CopyRegistrySuite extends AnyFunSuite with Matchers:

  private val Live = Phase.Live
  private val phases: List[Phase] = List(Phase.Live, Phase.Arrived("a"), Phase.Arrived("b"), Phase.Outbound("a"),
    Phase.Outbound("b"), Phase.Outbound(""), Phase.Rebuilt("e"), Phase.Rebuilt(""), Phase.Lost, Phase.Unread)

  private def kind(p: Phase): String = p match
    case Phase.Live => "Live"
    case Phase.Arrived(_) => "Arrived"
    case Phase.Outbound(_) => "Outbound"
    case Phase.Rebuilt(_) => "Rebuilt"
    case Phase.Lost => "Lost"
    case Phase.Unread => "Unread"

  private def expectedPhase(from: Phase, to: Phase): Boolean =
    val legal = Set(
      "Live" -> "Lost", "Live" -> "Outbound",
      "Outbound" -> "Live", "Outbound" -> "Lost", "Outbound" -> "Rebuilt", "Outbound" -> "Arrived",
      "Arrived" -> "Rebuilt", "Arrived" -> "Live", "Arrived" -> "Lost", "Arrived" -> "Outbound",
      "Rebuilt" -> "Live", "Rebuilt" -> "Outbound", "Rebuilt" -> "Lost")
    kind(from) match
      case "Unread" => true
      case "Lost" => false
      // A parked copy (empty TO, AdoptOffline) transferred out: Outbound("") -> Outbound(tid), tid non-empty (R14)
      case _ if from == Phase.Outbound("") && kind(to) == "Outbound" => to != Phase.Outbound("")
      case f => kind(to) == "Unread" || legal.contains(f -> kind(to))

  private val retry = LoadRetry(null.asInstanceOf[LocalStorageDeviceState], os.root / "x", 1, 1)
  private val activities: List[Activity] = List(Activity.Idle, Activity.AwaitingRetry(retry), Activity.Opening(1L),
    Activity.Served, Activity.Closing(ClosePurpose.TransferOut), Activity.Closing(ClosePurpose.Shutdown),
    Activity.Closing(ClosePurpose.Relinquish), Activity.Closing(ClosePurpose.FailStop), Activity.Retiring)

  private def akind(a: Activity): String = a match
    case Activity.Idle => "Idle"
    case Activity.AwaitingRetry(_) => "AwaitingRetry"
    case Activity.Opening(_) => "Opening"
    case Activity.Served => "Served"
    case Activity.Closing(_) => "Closing"
    case Activity.Retiring => "Retiring"

  private def expectedActivity(phase: Phase, from: Activity, to: Activity): Boolean =
    val any = Set("Idle" -> "AwaitingRetry", "Idle" -> "Served", "AwaitingRetry" -> "AwaitingRetry",
      "AwaitingRetry" -> "Opening", "AwaitingRetry" -> "Idle", "Opening" -> "Served", "Opening" -> "Idle",
      "Opening" -> "AwaitingRetry", "Served" -> "Closing", "Served" -> "Served", "Closing" -> "Idle",
      "Retiring" -> "Idle")
    val pair = akind(from) -> akind(to)
    any.contains(pair) ||
      (pair == ("Idle" -> "Opening") && phase == Phase.Live) ||
      (pair == ("Idle" -> "Retiring") && phase == Phase.Lost)

  private val storeId = StoreId(PoolId(UUID.randomUUID()), 0)
  private val other = StoreId(PoolId(UUID.randomUUID()), 1)

  private def device(changes: collection.mutable.ArrayBuffer[StoreId] = collection.mutable.ArrayBuffer()) =
    DeviceCopies(StorageDeviceId(UUID.randomUUID()), os.root / "dev", changes += _)

  /** A registry holding `storeId` at `phase`, with its activity driven to `activity` over legal moves */
  private def at(phase: Phase, activity: Activity): DeviceCopies =
    val d = device()
    d.register(storeId, phase)
    val path: List[Activity] = activity match
      case Activity.Idle => Nil
      case a @ Activity.AwaitingRetry(_) => List(a)
      case a @ Activity.Opening(_) => List(Activity.AwaitingRetry(retry), a)
      case Activity.Served => List(Activity.Served)
      case c @ Activity.Closing(_) => List(Activity.Served, c)
      case Activity.Retiring => List(Activity.Retiring)
    path.foreach(d.setActivity(storeId, _))
    d

  test("every legal phase move succeeds and every other pair throws"):
    for from <- phases; to <- phases do
      Transitions.phaseLegal(from, to) should be(expectedPhase(from, to))
      val d = device()
      d.register(storeId, from)
      if expectedPhase(from, to) then
        d.setPhase(storeId, to)
        d.copies(storeId).phase should be(to)
      else
        an[IllegalStateException] should be thrownBy d.setPhase(storeId, to)
        d.copies(storeId).phase should be(from)

  test("Outbound to a different Outbound is illegal"):
    Transitions.phaseLegal(Phase.Outbound("a"), Phase.Outbound("b")) should be(false)
    Transitions.phaseLegal(Phase.Outbound("a"), Phase.Outbound("a")) should be(false)
    Transitions.phaseLegal(Phase.Outbound("a"), Phase.Outbound("")) should be(false)
    Transitions.phaseLegal(Phase.Outbound(""), Phase.Outbound("")) should be(false)

  test("a parked copy (empty transfer-out marker) may be transferred out (R14)"):
    Transitions.phaseLegal(Phase.Outbound(""), Phase.Outbound("t")) should be(true)

  // Retiring is only ever entered from Idle while Lost, so it is no starting point under another phase
  private def reachable(phase: Phase, from: Activity): Boolean = from != Activity.Retiring || phase == Phase.Lost

  test("every legal activity move succeeds and every other pair throws, per phase"):
    // Closing's FailStop under Lost is covered by Served -> Closing(_)
    for phase <- phases; from <- activities; to <- activities if reachable(phase, from) do
      val legal = expectedActivity(phase, from, to)
      Transitions.activityLegal(phase, from, to) should be(legal)
      val d = at(phase, from)
      if legal then
        d.setActivity(storeId, to)
        d.copies(storeId).activity should be(to)
      else
        an[IllegalStateException] should be thrownBy d.setActivity(storeId, to)
        d.copies(storeId).activity should be(from)

  test("Served to Opening stays illegal"):
    Transitions.activityLegal(Live, Activity.Served, Activity.Opening(1L)) should be(false)

  test("register over an existing record throws"):
    val d = device()
    d.register(storeId, Live)
    an[IllegalStateException] should be thrownBy d.register(storeId, Phase.Lost)
    d.copies(storeId).phase should be(Live)

  test("mutators on an unregistered store throw, and unregister removes the record"):
    val d = device()
    an[IllegalStateException] should be thrownBy d.setPhase(storeId, Phase.Lost)
    an[IllegalStateException] should be thrownBy d.setActivity(storeId, Activity.Served)
    an[IllegalStateException] should be thrownBy d.setUnsynced(storeId, true)
    d.register(storeId, Live)
    d.unregister(storeId)
    d.copies.contains(storeId) should be(false)

  test("startWork over existing work throws"):
    val d = device()
    d.startWork(storeId, Work.Create)
    an[IllegalStateException] should be thrownBy d.startWork(storeId, Work.TransferOut(None))
    d.work(storeId) should be(Work.Create)

  test("updateWork needs work of the same case"):
    val d = device()
    an[IllegalStateException] should be thrownBy d.updateWork(storeId, Work.Rebuild(None, RebuildStage.Running))
    d.startWork(storeId, Work.Rebuild(None, RebuildStage.Queued))
    an[IllegalStateException] should be thrownBy d.updateWork(storeId, Work.Create)
    val e = Some(UUID.randomUUID())
    d.updateWork(storeId, Work.Rebuild(e, RebuildStage.Running))
    d.work(storeId) should be(Work.Rebuild(e, RebuildStage.Running))
    d.endWork(storeId)
    d.work.contains(storeId) should be(false)

  test("Opening is refused during TransferOut work"):
    val d = device()
    d.register(storeId, Live)
    d.startWork(storeId, Work.TransferOut(None))
    an[IllegalStateException] should be thrownBy d.setActivity(storeId, Activity.Opening(1L))
    d.endWork(storeId)
    d.setActivity(storeId, Activity.Opening(1L))

  test("unsynced and missing are tracked"):
    val d = device()
    d.register(storeId, Live)
    d.setUnsynced(storeId, true)
    d.copies(storeId).unsynced should be(true)
    d.declareMissing(other)
    d.missing should be(Set(other))
    d.clearMissing(other)
    d.missing should be(Set.empty)

  test("onChange runs once per successful mutation and not on a throw"):
    val changes = collection.mutable.ArrayBuffer[StoreId]()
    val d = device(changes)
    d.register(storeId, Live)
    changes.toList should be(List(storeId))
    an[IllegalStateException] should be thrownBy d.register(storeId, Live)
    an[IllegalStateException] should be thrownBy d.setPhase(storeId, Phase.Live)
    an[IllegalStateException] should be thrownBy d.setActivity(storeId, Activity.Retiring)
    an[IllegalStateException] should be thrownBy d.updateWork(storeId, Work.Create)
    changes.size should be(1)
    d.setPhase(storeId, Phase.Lost)
    d.setActivity(storeId, Activity.Retiring)
    d.setUnsynced(storeId, true)
    d.startWork(storeId, Work.Create)
    d.updateWork(storeId, Work.Create)
    d.endWork(storeId)
    d.declareMissing(storeId)
    d.clearMissing(storeId)
    d.unregister(storeId)
    changes.size should be(10)
    changes.toSet should be(Set(storeId))
