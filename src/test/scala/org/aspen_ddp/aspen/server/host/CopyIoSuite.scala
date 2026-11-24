package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, Host}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Copy moves, retirement, release and deletion on the device lanes (device I/O isolation, Task 12): every disk step
 *  is one job on the copy's device's lane (CopyDisk), its copy marked busy (DiskJob) or its device's release
 *  coalesced while it runs, and its result applied on the control thread after a recheck. Lockstep: a job runs when
 *  the test drains it, and a held device's jobs wait in `heldJobs`. Two devices: A (dev0) and B (dev1) */
class CopyIoSuite extends IntegrationTestSuite with ControlFixture:

  private val s = StoreId(PoolId(UUID.fromString("33333333-3333-3333-3333-333333333333")), 0)
  private val s2 = StoreId(s.poolId, 1.toByte)

  private def device(hostRoot: os.Path, name: String, id: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, name, id)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  /** A host with devices A and B, `placeA` and `placeB` placing their copies before the load */
  private def twoDevices(placeA: os.Path => Unit, placeB: os.Path => Unit,
                         bId: StorageDeviceId = deviceB): (Host, os.Path, os.Path) =
    val hostRoot = newHostDir()
    val devA = device(hostRoot, "dev0", deviceA)
    val devB = device(hostRoot, "dev1", bId)
    placeA(devA)
    placeB(devB)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    (host, devA, devB)

  private def local(host: Host, id: StorageDeviceId): LocalStorageDeviceState = loadedDevices(host)(id)

  private def recordOn(host: Host, id: StorageDeviceId, sid: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.device(id).flatMap(_.copies.get(sid))

  private def drain(host: Host): Unit = host.testingOnlyHandleEvents()

  private def noViolations(host: Host): org.scalatest.Assertion =
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  /** `dir` without search permission: every stat of a child fails. Returns it, to restore */
  private def unstatable(dir: os.Path): os.Path =
    os.perms.set(dir, "rw-------")
    dir

  private def restore(dir: os.Path): Unit = os.perms.set(dir, "rwx------")

  // ---- Retire ----

  atest("a retire waits on a held device and others proceed"):
    given ExecutionContext = executionContext
    val (host, devA, devB) = twoDevices(writeStoreConfig(_, s): Unit, writeStoreConfig(_, s2): Unit)
    holdDevice(host, devA)
    val onA = host.control.copies.retire(local(host, deviceA), s)
    val onB = host.control.copies.retire(local(host, deviceB), s2)
    settled(host, onB).flatMap: clearB =>
      heldJobs(host, devA) should be(List(JobKind.Retire))
      onA.isCompleted should be(false)
      // Busy while its stat waits: never decided around, and a second retire waits rather than starting another job
      recordOn(host, deviceA, s).map(_.activity) should be(Some(Activity.DiskJob(JobKind.Retire, Activity.Idle)))
      host.control.copies.retire(local(host, deviceA), s).value should be(Some(scala.util.Success(false)))
      heldJobs(host, devA) should be(List(JobKind.Retire))
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))
      clearB should be(true)
      CopyFixtures.retiredCopies(devB, s2).size should be(1)
      recordOn(host, deviceB, s2) should be(None)
      noViolations(host)
      releaseDevice(host, devA)
      settled(host, onA).map: clearA =>
        clearA should be(true)
        CopyFixtures.retiredCopies(devA, s).size should be(1)
        os.exists(CopyFixtures.retiredCopies(devA, s).head / RebuildingStore.LostCrlStateFile) should be(true)
        recordOn(host, deviceA, s) should be(None)
        heldJobs(host, devA) should be(Nil)
        noViolations(host)

  atest("a retire completing after its device's unload changes nothing"):
    given ExecutionContext = executionContext
    val (host, devA, _) = twoDevices(writeStoreConfig(_, s): Unit, _ => ())
    val before = CopyFixtures.copyPath(devA, s)
    holdDevice(host, devA)
    val onA = host.control.copies.retire(local(host, deviceA), s)
    drain(host)
    host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(host, devA)
    settled(host, onA).map: clear =>
      clear should be(false)
      // The stat ran over the unloaded device; its apply moved nothing and recorded nothing
      loadedDevices(host).keySet should be(Set(deviceB))
      host.control.copyRegistry.device(deviceA) should be(None)
      host.control.copies.isRetiring(s) should be(false)
      CopyFixtures.copyPath(devA, s) should be(before)
      CopyFixtures.retiredCopies(devA, s) should be(Nil)
      heldJobs(host, devA) should be(Nil)
      noViolations(host)

  atest("a retire whose copy a stat answers absent reports its path clear and moves nothing"):
    given ExecutionContext = executionContext
    val (host, devA, _) = twoDevices(writeStoreConfig(_, s): Unit, _ => ())
    val path = CopyFixtures.copyPath(devA, s).get
    host.testingOnlySkipDiskCheckAt(path)
    os.remove.all(path)
    settled(host, host.control.copies.retire(local(host, deviceA), s)).map: clear =>
      clear should be(true)
      CopyFixtures.retiredCopies(devA, s) should be(Nil)
      recordOn(host, deviceA, s).map(_.activity) should be(Some(Activity.Idle))
      noViolations(host)

  atest("a retire whose copy cannot be stat'd fails, changing nothing"):
    given ExecutionContext = executionContext
    val (host, devA, _) = twoDevices(writeStoreConfig(_, s): Unit, _ => ())
    val live = unstatable(devA / CopyLayout.LiveDirectory)
    settled(host, host.control.copies.retire(local(host, deviceA), s)).transform: r =>
      restore(live)
      r
    .map: clear =>
      clear should be(false)
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))
      recordOn(host, deviceA, s).map(r => (r.phase, r.activity)) should be(Some((Phase.Live, Activity.Idle)))
      noViolations(host)

  // ---- Moves ----

  test("a move waits on a held device and others proceed"):
    val (host, devA, devB) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Lost): Unit,
                                        CopyFixtures.placeCopy(_, s2, Phase.Lost): Unit)
    holdDevice(host, devA)
    var resultA = Option.empty[MoveResult]
    var resultB = Option.empty[MoveResult]
    host.control.moves.moveOnLane("test", local(host, deviceA), s, Phase.Retired)(r => resultA = Some(r))
    host.control.moves.moveOnLane("test", local(host, deviceB), s2, Phase.Retired)(r => resultB = Some(r))
    drain(host)
    heldJobs(host, devA) should be(List(JobKind.Move))
    resultA should be(None)
    recordOn(host, deviceA, s).map(_.activity) should be(Some(Activity.DiskJob(JobKind.Move, Activity.Idle)))
    // A synchronous move of a busy copy is refused, never made under the job
    host.control.moves.move("test", local(host, deviceA), s, Phase.Retired) shouldBe a[MoveResult.NotMoved]
    CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Lost))
    resultB should be(Some(MoveResult.Durable))
    CopyFixtures.retiredCopies(devB, s2).size should be(1)
    releaseDevice(host, devA)
    drain(host)
    resultA should be(Some(MoveResult.Durable))
    CopyFixtures.retiredCopies(devA, s).size should be(1)
    recordOn(host, deviceA, s) should be(None)
    noViolations(host)

  test("runCopyJobs runs a copy job early but applies its result only after the events queued before it"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Lost): Unit, _ => ())
    var order = List.empty[String]
    host.control.post(Host.DeviceJobDone("an earlier event", () => order :+= "earlier"))
    host.control.moves.moveOnLane("test", local(host, deviceA), s, Phase.Retired)(r => order :+= s"moved $r")
    runCopyJobs(host)
    // The lane job ran (the copy is in pending-delete), its result still queued behind the earlier event
    CopyFixtures.retiredCopies(devA, s).size should be(1)
    order should be(Nil)
    recordOn(host, deviceA, s).map(_.activity) should be(Some(Activity.DiskJob(JobKind.Move, Activity.Idle)))
    drain(host)
    order should be(List("earlier", s"moved ${MoveResult.Durable}"))
    recordOn(host, deviceA, s) should be(None)
    noViolations(host)

  test("a move completing after its device's unload changes nothing"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Lost): Unit, _ => ())
    holdDevice(host, devA)
    var result = Option.empty[MoveResult]
    host.control.moves.moveOnLane("test", local(host, deviceA), s, Phase.Retired)(r => result = Some(r))
    drain(host)
    host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(host, devA)
    drain(host)
    // The job ran over the unloaded device's files; its apply recorded nothing
    result.get shouldBe a[MoveResult.NotMoved]
    host.control.copyRegistry.device(deviceA) should be(None)
    host.control.copyRegistry.isUnsyncedAt(CopyFixtures.retiredCopies(devA, s).head) should be(false)
    heldJobs(host, devA) should be(Nil)
    noViolations(host)

  test("a move whose rename lands unforced records the copy unsynced, its directories held for the resync"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Live): Unit, _ => ())
    val outbound = devA / CopyLayout.OutboundDirectory
    var failing = true
    host.control.moves.syncDirectory = d =>
      if failing && d == outbound then throw new java.io.IOException("injected sync")
      DurableFiles.syncDirectory(d)
    var result = Option.empty[MoveResult]
    host.control.moves.moveOnLane("test", local(host, deviceA), s, Phase.Outbound("t"))(r => result = Some(r))
    drain(host)
    result.get shouldBe a[MoveResult.Unsynced]
    recordOn(host, deviceA, s).map(r => (r.phase, r.unsynced, r.activity)) should be(
      Some((Phase.Outbound("t"), true, Activity.Idle)))
    host.control.moves.unforcedUnder(devA).keySet should be(Set(CopyFixtures.copyPath(devA, s).get))
    failing = false
    host.control.moves.resync(local(host, deviceA))(())
    drain(host)
    recordOn(host, deviceA, s).map(_.unsynced) should be(Some(false))
    host.control.moves.unforcedUnder(devA) should be(Map())
    noViolations(host)

  test("a move whose rename fails changes nothing"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Lost): Unit, _ => ())
    val before = recordOn(host, deviceA, s)
    host.control.moves.rename = (_, _) => throw new java.io.IOException("injected rename")
    var result = Option.empty[MoveResult]
    host.control.moves.moveOnLane("test", local(host, deviceA), s, Phase.Retired)(r => result = Some(r))
    drain(host)
    result.get shouldBe a[MoveResult.NotMoved]
    recordOn(host, deviceA, s) should be(before)
    CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Lost))
    noViolations(host)

  test("a move into Live retargets a retry beneath its busy mark, and a move elsewhere drops it"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Outbound("t")): Unit,
                                     CopyFixtures.placeCopy(_, s2, Phase.Live): Unit)
    val registry = host.control.copyRegistry
    val sds = local(host, deviceA)
    registry.awaitingRetry("test", s, Host.LoadRetry(sds, CopyFixtures.copyPath(devA, s).get, 1, 1)) should be(true)
    host.control.moves.moveOnLane("test", sds, s, Phase.Live)(_ => ())
    drain(host)
    val live = CopyFixtures.copyPath(devA, s).get
    live / os.up should be(devA / CopyLayout.LiveDirectory)
    recordOn(host, deviceA, s).map(_.activity) match
      case Some(Activity.AwaitingRetry(r)) => r.storePath should be(live)
      case other => fail(s"expected the retry at the copy's new path, found $other")
    // HostCopies.moved over a DiskJob(AwaitingRetry) copy rewrites only what the mark restores
    val sdsB = local(host, deviceB)
    registry.awaitingRetry("test", s2, Host.LoadRetry(sdsB, CopyFixtures.copyPath(sdsB.devicePath, s2).get, 1, 1))
    registry.diskJob(sdsB, s2, JobKind.Move) should be(true)
    val copyId = recordOn(host, deviceB, s2).get.copyId
    registry.moved("test", sdsB, s2, Phase.Lost, copyId, unsynced = false)
    recordOn(host, deviceB, s2).map(_.activity) should be(Some(Activity.DiskJob(JobKind.Move, Activity.Idle)))
    registry.diskJobEnded(sdsB, s2)
    recordOn(host, deviceB, s2).map(_.activity) should be(Some(Activity.Idle))
    noViolations(host)

  test("a copy busy with a disk job is never moved by the pendingLost retry"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Live): Unit, _ => ())
    val sds = local(host, deviceA)
    val registry = host.control.copyRegistry
    registry.diskJob(sds, s, JobKind.Move) should be(true)
    host.control.moves.holdLost(sds, s, held = true)
    host.control.rebuilds.retryPendingLost(sds)
    CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))
    host.control.moves.isPendingLost(sds, s) should be(true)
    recordOn(host, deviceA, s).map(r => (r.phase, r.activity)) should be(
      Some((Phase.Live, Activity.DiskJob(JobKind.Move, Activity.Idle))))
    // Once the job ends, the next retry moves it
    registry.diskJobEnded(sds, s)
    host.control.rebuilds.retryPendingLost(sds)
    CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Lost))
    host.control.moves.isPendingLost(sds, s) should be(false)
    drain(host)
    noViolations(host)

  // ---- Resync ----

  atest("a device check waits for its resync on a held device, and another device's check completes"):
    given ExecutionContext = executionContext
    val (host, devA, devB) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Live): Unit,
                                        CopyFixtures.placeCopy(_, s2, Phase.Live): Unit)
    val registry = host.control.copyRegistry
    registry.copiesOf(local(host, deviceA)).get.setUnsynced(s, true)
    registry.copiesOf(local(host, deviceB)).get.setUnsynced(s2, true)
    holdDevice(host, devA)
    for id <- List(deviceA, deviceB) do
      recorded(host).armLookup(id).success(deviceState(id))
      host.control.reconciler.checkStorageDevice(id)
    awaitLoudly("device B's check") {
      drain(host)
      host.control.reconciler.activeDeviceChecks == Set(deviceA) && recordOn(host, deviceB, s2).isEmpty
    }.flatMap: _ =>
      heldJobs(host, devA) should be(List(JobKind.Move))
      recordOn(host, deviceA, s).map(r => (r.unsynced, r.activity)) should be(
        Some((true, Activity.DiskJob(JobKind.Move, Activity.Idle))))
      // B's check forced its copy and then ran its decisions: the copy has no entry, so it is retired
      CopyFixtures.retiredCopies(devB, s2).size should be(1)
      releaseDevice(host, devA)
      awaitLoudly("device A's check") {
        drain(host)
        host.control.reconciler.activeDeviceChecks.isEmpty && recordOn(host, deviceA, s).isEmpty
      }.map: _ =>
        CopyFixtures.retiredCopies(devA, s).size should be(1)
        heldJobs(host, devA) should be(Nil)
        noViolations(host)

  for (variant, tree) <- List[(String, StorageDeviceState => StorageDeviceState)](
        "declared Failed" -> (_.copy(status = StorageDeviceState.DeviceStatus.Failed)),
        "owned by another host" -> (_.copy(hostId = org.aspen_ddp.aspen.common.metadata.HostId(UUID.randomUUID()))))
  do
    atest(s"a device $variant is stopped at once, not after the resync its held lane holds"):
      given ExecutionContext = executionContext
      val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Live): Unit, _ => ())
      host.control.copyRegistry.copiesOf(local(host, deviceA)).get.setUnsynced(s, true)
      holdDevice(host, devA)
      recorded(host).armLookup(deviceA).success(tree(deviceState(deviceA)))
      host.control.reconciler.checkStorageDevice(deviceA)
      awaitLoudly(s"device A's check, $variant") {
        drain(host)
        host.control.reconciler.activeDeviceChecks.isEmpty
      }.flatMap: _ =>
        // The resync was asked for and waits on the held lane; the stop did not wait for it
        heldJobs(host, devA) should be(List(JobKind.Move))
        awaitLoudly(s"device A unloaded, $variant") {
          drain(host)
          !loadedDevices(host).contains(deviceA)
        }.map: _ =>
          loadedDevices(host).contains(deviceA) should be(false)
          heldJobs(host, devA) should be(List(JobKind.Move))
          if variant == "declared Failed" then
            os.exists(devA / "failed") should be(true)
          releaseDevice(host, devA)
          drain(host)
          heldJobs(host, devA) should be(Nil)
          host.control.moves.resyncRunning(devA) should be(false)
          noViolations(host)

  test("a resync whose force fails leaves the copy unsynced, and one completing after unload changes nothing"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Live): Unit, _ => ())
    val sds = local(host, deviceA)
    val path = CopyFixtures.copyPath(devA, s).get
    host.control.copyRegistry.copiesOf(sds).get.setUnsynced(s, true)
    host.control.moves.syncDirectory = d =>
      if d == path then throw new java.io.IOException("injected sync")
      DurableFiles.syncDirectory(d)
    var ran = 0
    host.control.moves.resync(sds)(ran += 1)
    drain(host)
    ran should be(1)
    recordOn(host, deviceA, s).map(r => (r.unsynced, r.activity)) should be(Some((true, Activity.Idle)))
    host.control.moves.syncDirectory = DurableFiles.syncDirectory
    holdDevice(host, devA)
    host.control.moves.resync(sds)(ran += 1)
    drain(host)
    host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(host, devA)
    drain(host)
    ran should be(2)
    host.control.copyRegistry.device(deviceA) should be(None)
    host.control.copyRegistry.isUnsyncedAt(path) should be(false)
    host.control.moves.resyncRunning(devA) should be(false)
    noViolations(host)

  // ---- Discard ----

  test("a discard waits on a held device and others proceed"):
    val (host, devA, devB) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Rebuilt("e")): Unit,
                                        CopyFixtures.placeCopy(_, s2, Phase.Rebuilt("e")): Unit)
    holdDevice(host, devA)
    var discarded = List.empty[StorageDeviceId]
    def discard(id: StorageDeviceId, sid: StoreId): Unit =
      host.control.copies.discard(sid, id, "a test"): sds =>
        discarded ::= sds.storageDeviceId
        host.control.copyRegistry.removed("test", sds, sid)
    discard(deviceA, s)
    discard(deviceB, s2)
    drain(host)
    heldJobs(host, devA) should be(List(JobKind.Delete))
    recordOn(host, deviceA, s).map(_.activity) should be(Some(Activity.DiskJob(JobKind.Delete, Activity.Idle)))
    CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Rebuilt("e")))
    discarded should be(List(deviceB))
    CopyFixtures.copyPath(devB, s2) should be(None)
    recordOn(host, deviceB, s2) should be(None)
    releaseDevice(host, devA)
    drain(host)
    discarded should be(List(deviceA, deviceB))
    CopyFixtures.copyPath(devA, s) should be(None)
    noViolations(host)

  test("a discard completing after its device's unload reports nothing"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Rebuilt("e")): Unit, _ => ())
    holdDevice(host, devA)
    var discarded = false
    host.control.copies.discard(s, deviceA, "a test")(_ => discarded = true)
    drain(host)
    host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(host, devA)
    drain(host)
    discarded should be(false)
    host.control.copyRegistry.device(deviceA) should be(None)
    heldJobs(host, devA) should be(Nil)
    noViolations(host)

  test("a discard of a copy known absent, or one whose delete fails, reports nothing"):
    val (host, devA, devB) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Rebuilt("e")): Unit,
                                        CopyFixtures.placeCopy(_, s2, Phase.Rebuilt("e")): Unit)
    var discarded = List.empty[StoreId]
    // Absent: removed by hand
    val gone = CopyFixtures.copyPath(devA, s).get
    host.testingOnlySkipDiskCheckAt(gone)
    os.remove.all(gone)
    host.control.copies.discard(s, deviceA, "a test")(_ => discarded ::= s)
    // Kept: its stat fails, so the delete is attempted and fails
    val rebuilt = unstatable(devB / CopyLayout.RebuiltDirectory)
    try
      host.control.copies.discard(s2, deviceB, "a test")(_ => discarded ::= s2)
      drain(host)
    finally restore(rebuilt)
    discarded should be(Nil)
    recordOn(host, deviceA, s).map(_.activity) should be(Some(Activity.Idle))
    recordOn(host, deviceB, s2).map(_.activity) should be(Some(Activity.Idle))
    CopyFixtures.phaseOf(devB, s2) should be(Some(Phase.Rebuilt("e")))
    noViolations(host)

  // ---- Release ----

  private def active(id: StorageDeviceId, sid: StoreId): StorageDeviceState =
    deviceState(id, Map(sid -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)))

  test("a release waits on a held device and others proceed"):
    val (host, devA, devB) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Retired): Unit,
                                        CopyFixtures.placeCopy(_, s2, Phase.Retired): Unit)
    holdDevice(host, devA)
    host.control.copies.release(local(host, deviceA), active(deviceA, s))
    host.control.copies.release(local(host, deviceB), active(deviceB, s2))
    // A second release of the held device queues nothing more (coalesced)
    host.control.copies.release(local(host, deviceA), active(deviceA, s))
    drain(host)
    heldJobs(host, devA) should be(List(JobKind.Release))
    CopyFixtures.retiredCopies(devA, s).size should be(1)
    CopyFixtures.retiredCopies(devB, s2) should be(Nil)
    releaseDevice(host, devA)
    drain(host)
    CopyFixtures.retiredCopies(devA, s) should be(Nil)
    host.control.copies.releasesRunning should be(Set())
    host.control.copies.deletesRunning should be(Set())
    noViolations(host)

  test("a release completing after its device's unload deletes nothing"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Retired): Unit, _ => ())
    holdDevice(host, devA)
    host.control.copies.release(local(host, deviceA), active(deviceA, s))
    drain(host)
    host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(host, devA)
    drain(host)
    CopyFixtures.retiredCopies(devA, s).size should be(1)
    host.control.copies.releasesRunning should be(Set())
    host.control.copies.deletesRunning should be(Set())
    heldJobs(host, devA) should be(Nil)

  test("a release whose pending-delete directory cannot be listed keeps every copy"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Retired): Unit, _ => ())
    val pending = devA / RebuildingStore.PendingDeleteDirectory
    os.perms.set(pending, "-wx------")
    try
      host.control.copies.release(local(host, deviceA), active(deviceA, s))
      drain(host)
    finally os.perms.set(pending, "rwx------")
    CopyFixtures.retiredCopies(devA, s).size should be(1)
    host.control.copies.releasesRunning should be(Set())
    host.control.copies.deletesRunning should be(Set())

  test("a released copy's delete on a held device waits, and the copy stays until it runs"):
    val (host, devA, _) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Retired): Unit, _ => ())
    val sds = local(host, deviceA)
    val copy = CopyFixtures.retiredCopies(devA, s).head
    host.control.copies.release(sds, active(deviceA, s))
    // The listing's job runs; then the device hangs before the delete's job, which its apply submits
    val ls = host.threading.asInstanceOf[LockstepThreading]
    ls.removeQueued:
      case t: LockstepThreading.DeviceTask => t.kind == JobKind.Release
      case _ => false
    .foreach(_.run())
    holdDevice(host, devA)
    drain(host)
    heldJobs(host, devA) should be(List(JobKind.Delete))
    host.control.copies.deletesRunning should be(Set(copy))
    // A later release lists again, but queues no second delete of the same copy
    host.control.copies.release(sds, active(deviceA, s))
    drain(host)
    heldJobs(host, devA) should be(List(JobKind.Delete, JobKind.Release))
    os.exists(copy) should be(true)
    releaseDevice(host, devA)
    drain(host)
    CopyFixtures.retiredCopies(devA, s) should be(Nil)
    host.control.copies.deletesRunning should be(Set())
    host.control.copies.releasesRunning should be(Set())

  atest("a release of a copy on a held device waits while the store's owner copy on another device is Active and served"):
    given ExecutionContext = executionContext
    val bootstrapStore = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    client.getStoragePoolState(PoolId.BootstrapPoolId).flatMap: pool =>
      // Device B is the device the pool names for the store: its owner
      val owner = pool.stores(bootstrapStore.poolIndex).storageDeviceId
      val (host, devA, devB) = twoDevices(CopyFixtures.placeCopy(_, bootstrapStore, Phase.Retired): Unit,
                                          CopyFixtures.placeCopy(_, bootstrapStore, Phase.Live): Unit, bId = owner)
      servedHere(host, local(host, owner), bootstrapStore)
      holdDevice(host, devA)
      // Device A's entry no longer lists the store: the owner device is asked, once A's listing returns
      host.control.copies.release(local(host, deviceA), deviceState(deviceA))
      drain(host)
      heldJobs(host, devA) should be(List(JobKind.Release))
      // The owner copy is Active and served on B meanwhile; A's copy is untouched, never inferred gone
      recordOn(host, owner, bootstrapStore).map(r => (r.phase, r.activity)) should be(
        Some((Phase.Live, Activity.Served)))
      host.control.copyRegistry.silence(bootstrapStore) should be(Silence.Served)
      CopyFixtures.retiredCopies(devA, bootstrapStore).size should be(1)
      recorded(host).armLookup(owner).success(active(owner, bootstrapStore))
      releaseDevice(host, devA)
      awaitLoudly("the release of device A's copy") {
        drain(host)
        CopyFixtures.retiredCopies(devA, bootstrapStore).isEmpty
      }.map: _ =>
        CopyFixtures.retiredCopies(devA, bootstrapStore) should be(Nil)
        CopyFixtures.phaseOf(devB, bootstrapStore) should be(Some(Phase.Live))
        recordOn(host, owner, bootstrapStore).map(_.activity) should be(Some(Activity.Served))
        noViolations(host)

  // ---- The registry's disk check ----

  test("the registry's disk check runs as a job on each device's lane, a held device's left unchecked"):
    val (host, devA, devB) = twoDevices(CopyFixtures.placeCopy(_, s, Phase.Live): Unit,
                                        CopyFixtures.placeCopy(_, s2, Phase.Live): Unit)
    host.testingOnlyVerifyCopyRegistry() should be(Nil)
    holdDevice(host, devA)
    os.remove.all(CopyFixtures.copyPath(devB, s2).get)
    // A's record is as stale, but its lane is held: no check is submitted there
    val pathA = CopyFixtures.copyPath(devA, s).get
    os.remove.all(pathA)
    val found = host.testingOnlyVerifyCopyRegistry()
    found.size should be(1)
    found.head should include(deviceB.toString)
    heldJobs(host, devA) should be(Nil)
    host.control.copyRegistry.verifyAgainstDisk().size should be(2)
    // Its job, submitted while held, waits there
    heldJobs(host, devA) should be(List(JobKind.Other("VerifyCopies")))
    host.testingOnlySkipDiskCheck(deviceA)
    host.testingOnlySkipDiskCheck(deviceB)
    releaseDevice(host, devA)
    drain(host)
    heldJobs(host, devA) should be(Nil)
