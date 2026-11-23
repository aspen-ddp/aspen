package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.StorageDeviceState.{StoreEntry, StoreStatus}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.CopyDecision.*
import org.aspen_ddp.aspen.server.host.CopyDecision.Action.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CopyDecisionSuite extends AnyFunSuite with Matchers:

  private val deviceId = StorageDeviceId(UUID.fromString("00000000-0000-0000-0000-0000000000d1"))
  private val otherDevice = StorageDeviceId(UUID.fromString("00000000-0000-0000-0000-0000000000d2"))
  private val thirdDevice = StorageDeviceId(UUID.fromString("00000000-0000-0000-0000-0000000000d3"))
  private val storeId = StoreId(PoolId(UUID.fromString("22222222-2222-2222-2222-222222222222")), 1)
  private val e1 = UUID.fromString("00000000-0000-0000-0000-0000000000e1")
  private val e2 = UUID.fromString("00000000-0000-0000-0000-0000000000e2")

  private val phases: List[Phase] = List(Phase.Live, Phase.Arrived("a"), Phase.Arrived("b"), Phase.Outbound("a"),
    Phase.Outbound("b"), Phase.Outbound(""), Phase.Rebuilt("e"), Phase.Rebuilt(""), Phase.Parked, Phase.Lost, Phase.Retired)

  private def entry(status: StoreStatus, to: Option[StorageDeviceId] = None, id: Option[UUID] = None,
                    acceptLoss: Boolean = false) = Some(StoreEntry(status, to, id, acceptLoss))
  private val active = entry(StoreStatus.Active)

  private def copy(phase: Phase, activity: ActivityKind = ActivityKind.Idle, unsynced: Boolean = false,
                   pendingLost: Boolean = false) = Some(CopyState(phase, unsynced, activity, pendingLost))

  private def in(e: Option[StoreEntry], c: Option[CopyState], w: Option[Work] = None,
                 facts: StoreFacts = StoreFacts.none) =
    StoreInputs(deviceId, storeId, e, c, w, facts)

  private val none = StoreFacts.none

  test("a queued rebuild under a non-Rebuilding entry ends first, and the copy is then offline"):
    val w = Some(Work.Rebuild(Some(e1), RebuildStage.Queued))
    decide(in(None, copy(Phase.Live), w, none.copy(dirPresent = Some(true)))) should be(
      List(EndQueuedRebuild, Retire))
    // Under a Rebuilding entry it stays, and holds the copy
    decide(in(entry(StoreStatus.Rebuilding, None, Some(e1)), copy(Phase.Live), w)) should be(
      List(StartRebuild(Some(e1), false)))

  test("a served copy with no entry is taken out of service"):
    decide(in(None, copy(Phase.Live, ActivityKind.Served))) should be(List(TakeOutOfService))

  test("an offline copy whose directory is gone is forgotten, one whose stat fails waits"):
    decide(in(None, copy(Phase.Live), facts = none)) should be(List(ForgetGoneCopy))
    decide(in(None, copy(Phase.Live), facts = none.copy(dirPresent = None))) should be(Nil)
    decide(in(None, copy(Phase.Live), facts = none.copy(dirPresent = Some(true)))) should be(List(Retire))

  test("a copy being retired anywhere on the host is not retired again"):
    decide(in(None, copy(Phase.Live), facts = none.copy(dirPresent = Some(true), retiringOnHost = true))) should be(Nil)

  test("an Active entry with no config is declared missing once, and stays declared while unserved"):
    decide(in(active, None, facts = none)) should be(List(DeclareMissing(true)))
    decide(in(active, None, facts = none.copy(configPresent = Some(true), declaredMissing = true))) should be(
      List(DeclareMissing(false)))
    decide(in(active, None, facts = none.copy(servedOnHost = true))) should be(Nil)
    decide(in(active, None, facts = none.copy(configPresent = Some(true)))) should be(Nil)
    decide(in(active, None, facts = none.copy(configPresent = None))) should be(Nil)
    decide(in(active, copy(Phase.Live, ActivityKind.Served), facts = none)) should be(Nil)
    decide(in(active, None, facts = none.copy(openingOnHost = true))) should be(Nil)

  test("a lost copy under an Active entry is flipped, or rechecked while sending"):
    val f = none.copy(configPresent = Some(true))
    decide(in(active, copy(Phase.Lost), facts = f)) should be(List(RequestRebuild))
    decide(in(active, copy(Phase.Lost), facts = f.copy(sending = true))) should be(List(Recheck))
    decide(in(entry(StoreStatus.Offline), copy(Phase.Lost), facts = f)) should be(Nil)

  test("an outbound or parked copy under Active with no transfer device is reinstated, or rechecked while sending"):
    val f = none.copy(configPresent = Some(true))
    for p <- List(Phase.Outbound("a"), Phase.Parked) do
      decide(in(active, copy(p), facts = f)) should be(List(ReinstateOut))
      decide(in(active, copy(p), facts = f.copy(sending = true))) should be(List(Recheck))
      decide(in(entry(StoreStatus.Active, Some(otherDevice)), copy(p), facts = f)) should be(Nil)

  test("an arrived copy under Active is brought live unless a receive attempt holds the store"):
    val f = none.copy(configPresent = Some(true))
    decide(in(active, copy(Phase.Arrived("a")), facts = f)) should be(List(BringArrivedLive))
    decide(in(active, copy(Phase.Arrived("a")), facts = f.copy(receivingOnHost = true))) should be(Nil)

  test("a rebuilt copy under Active is brought live unless a rebuild reads the store"):
    val f = none.copy(configPresent = Some(true))
    decide(in(active, copy(Phase.Rebuilt("e")), facts = f)) should be(List(BringRebuiltLive))
    decide(in(active, copy(Phase.Rebuilt("e")), facts = f.copy(rebuildReadsOnHost = true))) should be(Nil)

  test("a held Live copy loads only once synced"):
    val f = none.copy(configPresent = Some(true), heldForSync = true)
    decide(in(active, copy(Phase.Live), facts = f)) should be(List(LoadResynced))
    decide(in(active, copy(Phase.Live, unsynced = true), facts = f)) should be(Nil)
    decide(in(active, copy(Phase.Live), facts = f.copy(heldForSync = false))) should be(Nil)

  test("an Initializing entry is created; other work holds it"):
    val init = entry(StoreStatus.Initializing)
    decide(in(init, None)) should be(List(Create))
    decide(in(init, None, Some(Work.Create))) should be(List(Create))
    decide(in(init, None, Some(Work.TransferIn(None, TransferInStage.Receiving)))) should be(Nil)

  test("a tracked transfer in that its entry no longer names is dropped, and one targeted at another device is " +
       "dropped when this device's entry is TransferringIn"):
    val gone = none.copy(tracked = List(TrackedIn(otherDevice, deviceId)))
    decide(in(None, None, facts = gone)) should be(List(DropTransferIn(otherDevice, deviceId)))
    decide(in(entry(StoreStatus.TransferringIn, Some(thirdDevice)), None, facts = gone)) should be(
      List(DropTransferIn(otherDevice, deviceId), StartTransferIn(thirdDevice, None)))
    decide(in(entry(StoreStatus.TransferringIn, Some(otherDevice)), None, facts = gone)) should be(
      List(StartTransferIn(otherDevice, None)))
    val elsewhere = none.copy(tracked = List(TrackedIn(otherDevice, thirdDevice)))
    decide(in(entry(StoreStatus.TransferringIn, Some(otherDevice), Some(e1)), None, facts = elsewhere)) should be(
      List(DropTransferIn(otherDevice, thirdDevice), StartTransferIn(otherDevice, Some(e1))))
    decide(in(None, None, facts = elsewhere)) should be(Nil)

  test("a rebuild of an earlier episode is cancelled before the current one is started"):
    val w = Some(Work.Rebuild(Some(e1), RebuildStage.Running))
    val rebuilding = entry(StoreStatus.Rebuilding, None, Some(e2))
    decide(in(rebuilding, None, w, none.copy(rebuildHandle = true))) should be(
      List(CancelRebuild, StartRebuild(Some(e2), false)))

  test("a running rebuild without its handle is not cancelled"):
    val w = Some(Work.Rebuild(Some(e1), RebuildStage.Running))
    decide(in(entry(StoreStatus.Rebuilding, None, Some(e2), acceptLoss = true), None, w)) should be(
      List(StartRebuild(Some(e2), true)))

  test("a running rebuild of the current episode is not cancelled"):
    val w = Some(Work.Rebuild(Some(e2), RebuildStage.Running))
    decide(in(entry(StoreStatus.Rebuilding, None, Some(e2)), None, w, none.copy(rebuildHandle = true))) should be(
      List(StartRebuild(Some(e2), false)))

  test("a TransferringIn entry starts its transfer in; Rebuild work holds it"):
    val ti = entry(StoreStatus.TransferringIn, Some(otherDevice), Some(e1))
    decide(in(ti, None)) should be(List(StartTransferIn(otherDevice, Some(e1))))
    decide(in(ti, None, Some(Work.TransferIn(Some(e1), TransferInStage.Receiving)))) should be(
      List(StartTransferIn(otherDevice, Some(e1))))
    decide(in(ti, None, Some(Work.Rebuild(Some(e1), RebuildStage.Running)))) should be(Nil)

  test("a Rebuilding entry holds back for other work"):
    val rb = entry(StoreStatus.Rebuilding, None, Some(e1))
    decide(in(rb, None, Some(Work.TransferIn(None, TransferInStage.Receiving)))) should be(Nil)

  test("release: Active and no lost copy here releases; a lost copy keeps it; no entry asks the owner"):
    for status <- StoreStatus.values; lostHere <- List(false, true) do
      val expected =
        if status == StoreStatus.Active && !lostHere then ReleaseDecision.Release else ReleaseDecision.Keep
      release(entry(status, None), lostHere) should be(expected)
    release(None, false) should be(ReleaseDecision.AskOwner)
    release(None, true) should be(ReleaseDecision.AskOwner)

  test("decide keeps I1-I5 and never loads a non-Live copy over every combination"):
    val ids = List(None, Some(e1))
    val entries: List[Option[StoreEntry]] = None :: (for
      status <- StoreStatus.values.toList
      to <- List(None, Some(otherDevice))
      id <- ids
    yield Some(StoreEntry(status, to, id)))
    val copies: List[Option[CopyState]] = None :: (for
      p <- phases; a <- ActivityKind.values.toList; u <- List(false, true); l <- List(false, true)
    yield Some(CopyState(p, u, a, l)))
    val works: List[Option[Work]] =
      None :: Some(Work.Create) ::
        (for ep <- List(None, Some(e1), Some(e2)); s <- RebuildStage.values.toList
         yield Some(Work.Rebuild(ep, s))) :::
        (for s <- TransferInStage.values.toList yield Some(Work.TransferIn(Some(e1), s))) :::
        (for s <- TransferOutStage.values.toList yield Some(Work.TransferOut(Some(e1), s)))
    val bools = List(
      (f: StoreFacts) => f.copy(servedOnHost = true), (f: StoreFacts) => f.copy(openingOnHost = true),
      (f: StoreFacts) => f.copy(retiringOnHost = true), (f: StoreFacts) => f.copy(sending = true),
      (f: StoreFacts) => f.copy(receivingOnHost = true), (f: StoreFacts) => f.copy(rebuildReadsOnHost = true),
      (f: StoreFacts) => f.copy(rebuildHoldsOnHost = true), (f: StoreFacts) => f.copy(rebuildHandle = true),
      (f: StoreFacts) => f.copy(declaredMissing = true), (f: StoreFacts) => f.copy(heldForSync = true))
    val allTrue = bools.foldLeft(StoreFacts.none)((f, set) => set(f)).copy(dirPresent = Some(true),
      configPresent = Some(true))
    val existence = List(None, Some(true), Some(false))
    val facts: List[StoreFacts] =
      (StoreFacts.none :: allTrue :: bools.map(_(StoreFacts.none))) :::
        existence.map(d => StoreFacts.none.copy(dirPresent = d)) :::
        existence.map(c => StoreFacts.none.copy(configPresent = c)) :::
        List(List(TrackedIn(otherDevice, deviceId)), List(TrackedIn(otherDevice, thirdDevice)),
          // Tracked in both pendingStartTransfers and transferInRetries for the same (from, to)
          List(TrackedIn(otherDevice, deviceId), TrackedIn(otherDevice, deviceId))).map(t =>
          StoreFacts.none.copy(tracked = t))

    def bad(in: StoreInputs, what: String): Nothing =
      fail(s"$what: $in")

    val start = System.nanoTime()
    var count = 0L
    var emitted = Set.empty[Int]
    for e <- entries; c <- copies; w <- works do
      val d4Work = w match
        case Some(Work.Rebuild(_, RebuildStage.Queued)) if !e.exists(_.status == StoreStatus.Rebuilding) => None
        case other => other
      val phase = c.map(_.phase)
      val kind = c.map(_.activity)
      val touchesCopy = kind.exists(Set(ActivityKind.Opening, ActivityKind.Served, ActivityKind.Closing,
                                        ActivityKind.Retiring))
      val untouchable = c.exists(x => x.phase == Phase.Retired || x.pendingLost) || d4Work.nonEmpty
      for f <- facts do
        val input = StoreInputs(deviceId, storeId, e, c, w, f)
        val actions = decide(input)
        count += 1
        emitted ++= actions.map(_.ordinal)
        if actions != decide(input) then bad(input, "A7 nondeterministic")
        val ordinals = actions.map(_.ordinal)
        if ordinals != ordinals.sorted then bad(input, s"A6 out of order $actions")
        val distinct = actions.filterNot(_.isInstanceOf[DropTransferIn])
        if distinct.size != distinct.distinct.size || distinct.map(_.ordinal).toSet.size != distinct.size then
          bad(input, s"A6 repeated $actions")
        val drops = actions.collect { case x: DropTransferIn => x }
        if drops.size != drops.distinct.size then bad(input, s"A6 repeated drop $actions")
        var starts = 0
        actions.foreach:
          case LoadResynced =>
            if !(phase.contains(Phase.Live) && c.exists(!_.unsynced)) then bad(input, "A1 LoadResynced")
          case BringArrivedLive =>
            if !phase.exists(_.isInstanceOf[Phase.Arrived]) then bad(input, "A1 BringArrivedLive")
          case BringRebuiltLive =>
            if !phase.exists(_.isInstanceOf[Phase.Rebuilt]) then bad(input, "A1 BringRebuiltLive")
          case ReinstateOut =>
            if !phase.exists(p => p.isInstanceOf[Phase.Outbound] || p == Phase.Parked) then bad(input, "A1 ReinstateOut")
          case Create =>
            starts += 1
            if !d4Work.forall(_ == Work.Create) then bad(input, "A3 Create")
          case _: StartTransferIn =>
            starts += 1
            if !d4Work.forall(_.isInstanceOf[Work.TransferIn]) then bad(input, "A3 StartTransferIn")
          case _: StartRebuild =>
            starts += 1
            if !d4Work.forall(_.isInstanceOf[Work.Rebuild]) then bad(input, "A3 StartRebuild")
          case _ => ()
        if starts > 1 then bad(input, "A3 more than one start")
        actions.foreach: a =>
          val copyTouching = a match
            case LoadResynced | BringArrivedLive | BringRebuiltLive | ReinstateOut | Retire | ForgetGoneCopy |
                 RequestRebuild => true
            case _ => false
          if copyTouching && touchesCopy then bad(input, s"A2 $a")
          if copyTouching && untouchable then bad(input, s"A5 $a")
          a match
            case Retire | ForgetGoneCopy | TakeOutOfService =>
              if e.isDefined then bad(input, s"A4 $a")
            case _ => ()
    info(s"$count combinations in ${(System.nanoTime() - start) / 1000000} ms")
    // Every Action is emitted by some combination, so none of the invariants above holds vacuously for one
    val mirror = summon[scala.deriving.Mirror.SumOf[Action]]
    val actionCount = scala.compiletime.constValue[Tuple.Size[mirror.MirroredElemLabels]]
    emitted should be((0 until actionCount).toSet)
