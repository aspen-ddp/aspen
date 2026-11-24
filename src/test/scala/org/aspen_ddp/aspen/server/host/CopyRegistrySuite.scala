package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host.LoadRetry
import org.aspen_ddp.aspen.server.transfer.StoreTransferService
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, SECONDS}

class CopyRegistrySuite extends AnyFunSuite with Matchers:

  private val Live = Phase.Live
  private val phases: List[Phase] = List(Phase.Live, Phase.Arrived("a"), Phase.Arrived("b"), Phase.Outbound("a"),
    Phase.Outbound("b"), Phase.Outbound(""), Phase.Rebuilt("e"), Phase.Rebuilt(""), Phase.Parked, Phase.Lost, Phase.Retired)

  private def kind(p: Phase): String = p match
    case Phase.Live => "Live"
    case Phase.Arrived(_) => "Arrived"
    case Phase.Outbound(_) => "Outbound"
    case Phase.Rebuilt(_) => "Rebuilt"
    case Phase.Parked => "Parked"
    case Phase.Lost => "Lost"
    case Phase.Retired => "Retired"

  private def expectedPhase(from: Phase, to: Phase): Boolean =
    val legal = Set(
      "Live" -> "Lost", "Live" -> "Outbound",
      // Only Live, Rebuilt and parked copies are T6 sources, and a reinstated one is Live (P8, P10)
      "Outbound" -> "Live", "Outbound" -> "Lost",
      "Arrived" -> "Live", "Arrived" -> "Lost",
      "Rebuilt" -> "Live", "Rebuilt" -> "Parked", "Rebuilt" -> "Outbound", "Rebuilt" -> "Lost",
      "Parked" -> "Live", "Parked" -> "Outbound", "Parked" -> "Lost",
      // Every retire goes through Lost (T11)
      "Lost" -> "Retired")
    legal.contains(kind(from) -> kind(to))

  private val retry = LoadRetry(null.asInstanceOf[LocalStorageDeviceState], os.root / "x", 1, 1)
  private val activities: List[Activity] = List(Activity.Idle, Activity.AwaitingRetry(retry), Activity.Opening(1L),
    Activity.Served, Activity.Closing(ClosePurpose.TransferOut), Activity.Closing(ClosePurpose.Shutdown),
    Activity.Closing(ClosePurpose.Relinquish), Activity.Closing(ClosePurpose.FailStop),
    Activity.Closing(ClosePurpose.Unserved), Activity.Retiring,
    Activity.DiskJob(JobKind.Move, Activity.Idle), Activity.DiskJob(JobKind.Move, Activity.Served),
    Activity.DiskJob(JobKind.Move, Activity.AwaitingRetry(retry)), Activity.DiskJob(JobKind.Scan, Activity.Idle))

  private def akind(a: Activity): String = a match
    case Activity.Idle => "Idle"
    case Activity.AwaitingRetry(_) => "AwaitingRetry"
    case Activity.Opening(_) => "Opening"
    case Activity.Served => "Served"
    case Activity.Closing(_) => "Closing"
    case Activity.Retiring => "Retiring"
    case Activity.DiskJob(_, _) => "DiskJob"

  private def expectedActivity(phase: Phase, from: Activity, to: Activity): Boolean =
    val any = Set("Idle" -> "AwaitingRetry", "Idle" -> "Served", "AwaitingRetry" -> "AwaitingRetry",
      "AwaitingRetry" -> "Opening", "AwaitingRetry" -> "Idle", "Opening" -> "Served", "Opening" -> "Idle",
      "Opening" -> "AwaitingRetry", "Served" -> "Closing", "Served" -> "Served", "Closing" -> "Idle",
      "Retiring" -> "Idle")
    val pair = akind(from) -> akind(to)
    // A disk job holds an Idle, Served or AwaitingRetry copy and returns it to exactly that activity
    val diskJob = (from, to) match
      case (_, Activity.DiskJob(_, prior)) => prior == from && Set("Idle", "Served", "AwaitingRetry").contains(akind(from))
      case (Activity.DiskJob(_, prior), _) => to == prior
      case _ => false
    diskJob || any.contains(pair) ||
      (pair == ("Idle" -> "Opening") && phase == Phase.Live) ||
      (pair == ("Idle" -> "Retiring") && phase == Phase.Lost) ||
      // An unserved backend's close holds its Live copy (StoreRebuilds.retireUnserved)
      (Set("Idle", "AwaitingRetry", "Opening").contains(akind(from)) &&
        to == Activity.Closing(ClosePurpose.Unserved) && phase == Phase.Live)

  private val storeId = StoreId(PoolId(UUID.randomUUID()), 0)
  private val other = StoreId(PoolId(UUID.randomUUID()), 1)

  private def device(changes: collection.mutable.ArrayBuffer[StoreId] = collection.mutable.ArrayBuffer()) =
    DeviceCopies(StorageDeviceId(UUID.randomUUID()), os.root / "dev", changes += _)

  /** A registry holding `storeId` at `phase`, with its activity driven to `activity` over legal moves */
  private def at(phase: Phase, activity: Activity): DeviceCopies =
    val d = device()
    d.register(storeId, phase, UUID.randomUUID())
    val path: List[Activity] = activity match
      case Activity.Idle => Nil
      case a @ Activity.AwaitingRetry(_) => List(a)
      case a @ Activity.Opening(_) => List(Activity.AwaitingRetry(retry), a)
      case Activity.Served => List(Activity.Served)
      case c @ Activity.Closing(_) => List(Activity.Served, c)
      case Activity.Retiring => List(Activity.Retiring)
      case j @ Activity.DiskJob(_, prior) => (if prior == Activity.Idle then Nil else List(prior)) :+ j
    path.foreach(d.setActivity(storeId, _))
    d

  test("every legal phase move succeeds and every other pair throws"):
    for from <- phases; to <- phases do
      Transitions.phaseLegal(from, to) should be(expectedPhase(from, to))
      val d = device()
      d.register(storeId, from, UUID.randomUUID())
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

  test("a parked copy may be transferred out, rebuilt or parked copies never go Arrived, and Outbound(\"\") is no park"):
    Transitions.phaseLegal(Phase.Parked, Phase.Outbound("t")) should be(true)
    Transitions.phaseLegal(Phase.Rebuilt("e"), Phase.Parked) should be(true)
    Transitions.phaseLegal(Phase.Outbound(""), Phase.Outbound("t")) should be(false)
    Transitions.phaseLegal(Phase.Arrived("t"), Phase.Rebuilt("e")) should be(false)
    Transitions.phaseLegal(Phase.Parked, Phase.Rebuilt("e")) should be(false)

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
    d.register(storeId, Live, UUID.randomUUID())
    an[IllegalStateException] should be thrownBy d.register(storeId, Phase.Lost, UUID.randomUUID())
    d.copies(storeId).phase should be(Live)

  test("mutators on an unregistered store throw, and unregister removes the record"):
    val d = device()
    an[IllegalStateException] should be thrownBy d.setPhase(storeId, Phase.Lost)
    an[IllegalStateException] should be thrownBy d.setActivity(storeId, Activity.Served)
    an[IllegalStateException] should be thrownBy d.setUnsynced(storeId, true)
    d.register(storeId, Live, UUID.randomUUID())
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
    d.register(storeId, Live, UUID.randomUUID())
    d.startWork(storeId, Work.TransferOut(None))
    an[IllegalStateException] should be thrownBy d.setActivity(storeId, Activity.Opening(1L))
    d.endWork(storeId)
    d.setActivity(storeId, Activity.Opening(1L))

  test("unsynced and missing are tracked"):
    val d = device()
    d.register(storeId, Live, UUID.randomUUID())
    d.setUnsynced(storeId, true)
    d.copies(storeId).unsynced should be(true)
    d.declareMissing(other)
    d.missing should be(Set(other))
    d.clearMissing(other)
    d.missing should be(Set.empty)

  test("onChange runs once per successful mutation and not on a throw"):
    val changes = collection.mutable.ArrayBuffer[StoreId]()
    val d = device(changes)
    d.register(storeId, Live, UUID.randomUUID())
    changes.toList should be(List(storeId))
    an[IllegalStateException] should be thrownBy d.register(storeId, Live, UUID.randomUUID())
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

  // ---- The spec's transition table (Tests: "Table"), over the copy registry's claims and facts ----

  // Every phase, each that carries a parameter drawn from "" and one id
  private val tablePhases: List[Phase] = List(Phase.Live, Phase.Arrived(""), Phase.Arrived("t-1"), Phase.Outbound(""),
    Phase.Outbound("t-1"), Phase.Rebuilt(""), Phase.Rebuilt("t-1"), Phase.Parked, Phase.Lost, Phase.Retired)

  /** The legal phase moves, written out from the spec's table (T1-T13) and the rulings since, never from
   *  Transitions.phaseLegal. T1 (a create), T2 (a rebuild's finish) and T8 (a promotion) place a new copy, and T12
   *  (a release), T13 (an arrival's or a Discarded rebuild's delete) remove one: none is a move between phases. A move
   *  into Outbound is legal whatever the parameters; Outbound to Outbound never is (P10: a copy Outbound for the
   *  transfer is shipped where it is, one Outbound for another goes back to Live first) */
  private def tableLegal(from: Phase, to: Phase): Boolean = (from, to) match
    // T3: an Adopt flip, or the Completed Rebuilds pass
    case (Phase.Rebuilt(_), Phase.Live) => true
    // T4: an AdoptOffline flip
    case (Phase.Rebuilt(_), Phase.Parked) => true
    // T5: a parked copy whose entry is Active with no transfer device
    case (Phase.Parked, Phase.Live) => true
    // T6: from Live, Rebuilt or Parked (P10)
    case (Phase.Live | Phase.Rebuilt(_) | Phase.Parked, Phase.Outbound(_)) => true
    // T7: an aborted or abandoned transfer out; P10's move back before a new transfer's T6
    case (Phase.Outbound(_), Phase.Live) => true
    // T9: a committed pool update
    case (Phase.Arrived(_), Phase.Live) => true
    // T10
    case (Phase.Live | Phase.Outbound(_) | Phase.Arrived(_) | Phase.Rebuilt(_) | Phase.Parked, Phase.Lost) => true
    // T11: every retire goes through Lost
    case (Phase.Lost, Phase.Retired) => true
    case _ => false

  /** A DeviceControl with only what the copy registry and the moves read: the router, the loaded devices and the
   *  transfer service. Everything else is never touched */
  private def control(): DeviceControl =
    new DeviceControl(
      metadata = null, scheduler = null, backends = null, devices = new LocalDevices(), client = null,
      hostId = HostId.BootstrapHostId, aspenSystemId = new UUID(0, 0), rootDir = os.root, net = null,
      backgroundTasks = BackgroundTaskManager.NoBackgroundTaskManager,
      threading = new LockstepThreading(ExecutionContext.parasitic),
      // A lane job's result (CopyMoves.moveOnLane) is applied as it is posted
      post = { case e: org.aspen_ddp.aspen.server.Host.DeviceJobDone => e.apply(); case _ => () }, crlFactory = null,
      unloadedDeviceCrl = None, txStatusCache = null, wrapCompletionHandler = identity, objectCacheFactory = null,
      finalizerFactory = null, txDriverFactory = null, heartbeatPeriod = Duration(5, SECONDS),
      deviceScanPeriod = Duration(10, SECONDS), storeRebuildFactory = null, maxConcurrentRebuilds = 2,
      loadStore = null, tryLoadStore = null, loadStoreById = null, warn = null,
      storeTransferService = StoreTransferService.Disabled, shutdownWorkTimeout = Duration(60, SECONDS),
      gossipStoreOnline = null, closeStoreForTransferOut = null, closeStoreForShutdown = null,
      scheduleShutdownWorkTimeout = null, scheduleEndpointRetry = null, ec = ExecutionContext.parasitic)

  /** A strict registry with a device attached at a fresh directory, holding a copy of `storeId` placed in `from` and
   *  recorded there: the registry, the device and the copy's path */
  private def attachedCopy(root: os.Path, from: Phase): (DeviceControl, LocalStorageDeviceState, os.Path) =
    val ctx = control()
    ctx.copyRegistry.strict = true
    val dev = os.temp.dir(dir = root, prefix = "dev", deleteOnExit = false)
    val sds = new LocalStorageDeviceState(StorageDeviceId(UUID.randomUUID()), dev, dev / "config", null, ctx.deviceIos.forPath(dev))
    val path = CopyFixtures.placeCopy(dev, storeId, from)
    ctx.copyRegistry.attach(sds, CopyLayout.list(dev))
    if from == Phase.Retired then
      // Not recorded once durable (P3): recorded only as an unsynced move to Retired leaves it
      sds.copies.register(storeId, from, CopyFixtures.copyIdOf(path), unsynced = true)
    ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(storeId)).map(_.phase) should be(Some(from))
    (ctx, sds, path)

  test("a disk job returns the copy to its prior activity"):
    val root = os.temp.dir(prefix = "aspen-disk-job", deleteOnExit = false)
    try
      val (ctx, sds, _) = attachedCopy(root, Phase.Live)
      val registry = ctx.copyRegistry
      def activity = sds.copies.copies(storeId).activity
      registry.installed(sds.storageDeviceId, storeId, ctx.copyRegistry.pathOf(sds, storeId).get)
      activity should be(Activity.Served)
      registry.diskJob(sds, storeId, JobKind.Move) should be(true)
      activity should be(Activity.DiskJob(JobKind.Move, Activity.Served))
      registry.silence(storeId) should be(Silence.Served)
      registry.diskJobEnded(sds, storeId)
      activity should be(Activity.Served)
      // From Idle, and a second ended job leaves the copy alone
      sds.copies.setActivity(storeId, Activity.Closing(ClosePurpose.Shutdown))
      sds.copies.setActivity(storeId, Activity.Idle)
      registry.diskJob(sds, storeId, JobKind.Scan) should be(true)
      activity should be(Activity.DiskJob(JobKind.Scan, Activity.Idle))
      registry.diskJobEnded(sds, storeId)
      registry.diskJobEnded(sds, storeId)
      activity should be(Activity.Idle)
      registry.takeRegistryErrors() should be(Vector())
    finally os.remove.all(root)

  test("a second disk job on a busy copy is refused"):
    val root = os.temp.dir(prefix = "aspen-disk-job", deleteOnExit = false)
    try
      val (ctx, sds, _) = attachedCopy(root, Phase.Live)
      val registry = ctx.copyRegistry
      registry.diskJob(sds, storeId, JobKind.Scan) should be(true)
      registry.takeRegistryErrors() should be(Vector())
      registry.diskJob(sds, storeId, JobKind.Move) should be(false)
      sds.copies.copies(storeId).activity should be(Activity.DiskJob(JobKind.Scan, Activity.Idle))
      registry.takeRegistryErrors().size should be(1)
      // An activity a disk job cannot hold is refused too, and an unknown store
      registry.diskJobEnded(sds, storeId)
      sds.copies.setActivity(storeId, Activity.Served)
      sds.copies.setActivity(storeId, Activity.Closing(ClosePurpose.Shutdown))
      registry.diskJob(sds, storeId, JobKind.Scan) should be(false)
      registry.diskJob(sds, other, JobKind.Scan) should be(false)
      registry.takeRegistryErrors().size should be(2)
    finally os.remove.all(root)

  test("every (from, to) pair is legal exactly as the spec's table says"):
    val root = os.temp.dir(prefix = "aspen-copy-table", deleteOnExit = false)
    try table(root)
    finally os.remove.all(root)

  private def table(root: os.Path): Unit =
    for from <- tablePhases; to <- tablePhases do
      val legal = tableLegal(from, to)
      withClue(s"$from -> $to: "):
        // The table itself
        Transitions.phaseLegal(from, to) should be(legal)

        // A claim (HostCopies.claim over DeviceCopies.setPhase): an illegal move is refused, recorded and changes
        // nothing; a legal one is made
        val (claimCtx, claimSds, _) = attachedCopy(root, from)
        val claimRegistry = claimCtx.copyRegistry
        claimRegistry.claim("table")(claimSds.copies.setPhase(storeId, to)) should be(legal)
        claimSds.copies.copies(storeId).phase should be(if legal then to else from)
        claimRegistry.takeRegistryErrors().size should be(if legal then 0 else 1)

        // A fact (HostCopies.moved, what every CopyMoves move records): applied whatever the table says, and a
        // violation recorded for each illegal one. A durable move to Retired takes the copy out of the registry (P3)
        val (factCtx, factSds, _) = attachedCopy(root, from)
        val factId = factSds.copies.copies(storeId).copyId
        factCtx.copyRegistry.moved("table", factSds, storeId, to, factId, unsynced = false)
        val factRecord = factSds.copies.copies.get(storeId)
        if to == Phase.Retired && !factRecord.exists(_.unsynced) then
          factRecord should be(None)
        else
          factRecord.map(_.phase) should be(Some(to))
        factCtx.copyRegistry.takeRegistryErrors().size should be(if legal then 0 else 1)

        // The move itself (CopyMoves.move): between two phase directories it moves the directory, with the target's
        // parameter, and records the fact, legal or not; within one directory it is no move (NotMoved) and changes
        // nothing
        val (moveCtx, moveSds, at) = attachedCopy(root, from)
        // As a loaded device's: moveOnLane applies only for the loaded device, its job run by a drain
        moveCtx.devices.devices += moveSds.storageDeviceId -> moveSds
        var moveResult = Option.empty[MoveResult]
        moveCtx.moves.moveOnLane("table", moveSds, storeId, to)(r => moveResult = Some(r))
        moveCtx.threading.asInstanceOf[LockstepThreading].drain()
        val result = moveResult.get
        val target = CopyLayout.path(moveSds.devicePath, storeId, CopyFixtures.copyIdOf(at), to)
        if CopyLayout.dirOf(from) == CopyLayout.dirOf(to) then
          result shouldBe a[MoveResult.NotMoved]
          os.isDir(at) should be(true)
          moveSds.copies.copies.get(storeId).map(_.phase) should be(Some(from))
          moveCtx.copyRegistry.takeRegistryErrors() should be(Vector())
        else
          result should be(MoveResult.Durable)
          os.exists(at) should be(false)
          os.isDir(target) should be(true)
          CopyLayout.phaseAt(target) should be(Some(to))
          val moved = moveSds.copies.copies.get(storeId)
          if to == Phase.Retired && from != Phase.Retired then
            moved should be(None)
          else
            moved.map(_.phase) should be(Some(to))
          moveCtx.copyRegistry.takeRegistryErrors().size should be(if legal then 0 else 1)
