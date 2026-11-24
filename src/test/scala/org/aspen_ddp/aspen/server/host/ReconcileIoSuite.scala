package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog

import java.util.UUID
import scala.concurrent.ExecutionContext

/** A device check's disk inputs and the periodic usage poll on the device lanes (device I/O isolation, Task 13): a
 *  check reads every disk-derived input of its store decisions in one `ReconcileInputs` job (ReconcileDisk.gather)
 *  and decides on the control thread from those values once its result is applied; each device's usage is one
 *  `Usage` job, published on its own. At most one of each is outstanding per device. Lockstep: a job runs when the
 *  test drains it, and a held device's jobs wait in `heldJobs`. Two devices: A (dev0) and B (dev1) */
class ReconcileIoSuite extends IntegrationTestSuite with ControlFixture:

  private val s = StoreId(PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444")), 0)
  private val s2 = StoreId(s.poolId, 1.toByte)

  private def device(hostRoot: os.Path, name: String, id: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, name, id)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  /** A host with devices A and B, each holding one Live copy (s on A, s2 on B) that no tree entry names */
  private def twoDevices(): (Host, os.Path, os.Path) =
    val hostRoot = newHostDir()
    val devA = device(hostRoot, "dev0", deviceA)
    val devB = device(hostRoot, "dev1", deviceB)
    CopyFixtures.placeCopy(devA, s, Phase.Live)
    CopyFixtures.placeCopy(devB, s2, Phase.Live)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    (host, devA, devB)

  private def local(host: Host, id: StorageDeviceId): LocalStorageDeviceState = loadedDevices(host)(id)

  private def recordOn(host: Host, id: StorageDeviceId, sid: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.device(id).flatMap(_.copies.get(sid))

  private def drain(host: Host): Unit = host.testingOnlyHandleEvents()

  private def noViolations(host: Host): org.scalatest.Assertion =
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  /** Checks `id` against a tree entry naming none of its stores */
  private def check(host: Host, id: StorageDeviceId): Unit =
    recorded(host).armLookup(id).success(deviceState(id))
    host.control.reconciler.checkStorageDevice(id)

  // ---- ReconcileInputs ----

  atest("a device check's disk inputs wait on a held device and others proceed"):
    given ExecutionContext = executionContext
    val (host, devA, devB) = twoDevices()
    holdDevice(host, devA)
    check(host, deviceA)
    check(host, deviceB)
    awaitLoudly("device B's check") {
      drain(host)
      host.control.reconciler.activeDeviceChecks == Set(deviceA) && recordOn(host, deviceB, s2).isEmpty
    }.flatMap: _ =>
      // B's inputs were read and its decision taken: no entry names its copy, so it was retired
      CopyFixtures.retiredCopies(devB, s2).size should be(1)
      // A's inputs wait on its held lane, its decisions with them: nothing was decided for its copy
      heldJobs(host, devA) should be(List(JobKind.ReconcileInputs))
      host.control.reconciler.outstandingJobs should be(Set((devA, JobKind.ReconcileInputs)))
      recordOn(host, deviceA, s).map(r => (r.phase, r.activity)) should be(Some((Phase.Live, Activity.Idle)))
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))
      releaseDevice(host, devA)
      awaitLoudly("device A's check") {
        drain(host)
        host.control.reconciler.activeDeviceChecks.isEmpty && recordOn(host, deviceA, s).isEmpty
      }.map: _ =>
        CopyFixtures.retiredCopies(devA, s).size should be(1)
        heldJobs(host, devA) should be(Nil)
        host.control.reconciler.outstandingJobs should be(empty)
        noViolations(host)

  /** Drains `host` until `id`'s check has ended: its lookup's answer reaches the control thread asynchronously */
  private def checked(host: Host, id: StorageDeviceId)(using ExecutionContext) =
    awaitLoudly(s"device $id's check") {
      drain(host)
      !host.control.reconciler.activeDeviceChecks.contains(id)
    }

  /** Drains `host` until a job is held on `path` */
  private def heldOn(host: Host, path: os.Path)(using ExecutionContext) =
    awaitLoudly(s"a job held on $path") {
      drain(host)
      heldJobs(host, path).nonEmpty
    }

  atest("a device check's disk inputs completing after unload change nothing"):
    given ExecutionContext = executionContext
    val (host, devA, _) = twoDevices()
    val before = CopyFixtures.copyPath(devA, s)
    holdDevice(host, devA)
    check(host, deviceA)
    heldOn(host, devA).map: _ =>
      heldJobs(host, devA) should be(List(JobKind.ReconcileInputs))
      host.control.shutdown.unloadDevice(deviceA)
      drain(host)
      releaseDevice(host, devA)
      drain(host)
      // The gather ran over the unloaded device; its apply decided nothing and ended the check
      loadedDevices(host).keySet should be(Set(deviceB))
      host.control.copyRegistry.device(deviceA) should be(None)
      CopyFixtures.copyPath(devA, s) should be(before)
      CopyFixtures.retiredCopies(devA, s) should be(Nil)
      host.control.reconciler.activeDeviceChecks should be(empty)
      host.control.reconciler.outstandingJobs should be(empty)
      heldJobs(host, devA) should be(Nil)
      noViolations(host)

  atest("a copy whose record changed since its disk inputs were read is skipped this round, and the recheck decides it"):
    given ExecutionContext = executionContext
    val (host, devA, _) = twoDevices()
    val sds = local(host, deviceA)
    val live = CopyFixtures.copyPath(devA, s).get
    val copyId = recordOn(host, deviceA, s).get.copyId
    holdDevice(host, devA)
    check(host, deviceA)
    heldOn(host, devA).flatMap: _ =>
      heldJobs(host, devA) should be(List(JobKind.ReconcileInputs))
      // While the inputs wait, the copy moves to lost/ (as a move recorded under its own mark would): the stats read
      // are of its Live path, not its copy's
      val lost = CopyLayout.path(devA, s, copyId, Phase.Lost)
      os.makeDir.all(lost / os.up)
      os.move(live, lost)
      host.control.copyRegistry.moved("test", sds, s, Phase.Lost, copyId, unsynced = false)
      releaseDevice(host, devA)
      checked(host, deviceA).flatMap: _ =>
        // Not decided from the stale stats: no retire was asked for, and the device is checked again shortly
        recordOn(host, deviceA, s).map(r => (r.phase, r.activity)) should be(Some((Phase.Lost, Activity.Idle)))
        CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Lost))
        host.control.reconciler.scheduledDeviceRechecks should be(Set(deviceA))
        recorded(host).armLookup(deviceA).success(deviceState(deviceA))
        runDeviceRechecks(host)
        awaitLoudly("the recheck's retire") {
          drain(host)
          recordOn(host, deviceA, s).isEmpty && host.control.reconciler.activeDeviceChecks.isEmpty
        }.map: _ =>
          // The recheck reads its inputs anew and retires it
          CopyFixtures.retiredCopies(devA, s).size should be(1)
          host.control.reconciler.outstandingJobs should be(empty)
          noViolations(host)

  atest("a copy whose directory the disk inputs find gone has its record forgotten"):
    given ExecutionContext = executionContext
    val (host, devA, _) = twoDevices()
    val path = CopyFixtures.copyPath(devA, s).get
    host.testingOnlySkipDiskCheckAt(path)
    os.remove.all(path)
    check(host, deviceA)
    checked(host, deviceA).map: _ =>
      recordOn(host, deviceA, s) should be(None)
      CopyFixtures.retiredCopies(devA, s) should be(Nil)
      noViolations(host)

  atest("a copy whose directory the disk inputs cannot stat is left for a later check"):
    given ExecutionContext = executionContext
    val (host, devA, _) = twoDevices()
    val liveDir = devA / CopyLayout.LiveDirectory
    // Every stat of a child fails; the drain's own disk check skips the copy, which it could not stat either
    host.testingOnlySkipDiskCheckAt(CopyFixtures.copyPath(devA, s).get)
    os.perms.set(liveDir, "rw-------")
    check(host, deviceA)
    // Run without draining the checks' own retire: the retire's stat would fail too, which is not what this tests
    awaitLoudly("device A's check") {
      runCopyJobs(host)
      !host.control.reconciler.activeDeviceChecks.contains(deviceA)
    }.map: _ =>
      try
        // Neither forgotten (its absence is not known) nor retired (no Retire job: its stat failed)
        recordOn(host, deviceA, s).map(r => (r.phase, r.activity)) should be(Some((Phase.Live, Activity.Idle)))
        heldJobs(host, devA) should be(Nil)
      finally os.perms.set(liveDir, "rwx------")
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))
      CopyFixtures.retiredCopies(devA, s) should be(Nil)
      noViolations(host)

  test("ReconcileDisk.gather stats every recorded and listed copy and reads the listing's phase parameters"):
    val dev = newHostDir() / "dev"
    val rebuilt = CopyFixtures.placeCopy(dev, s, Phase.Rebuilt("episode-1"))
    val gone = CopyLayout.path(dev, s2, UUID.randomUUID(), Phase.Live)
    val disk = ReconcileDisk.gather(dev, Map(s2 -> gone))
    disk.listing.get.copies.map(f => (f.storeId, f.phase)) should be(List((s, Phase.Rebuilt("episode-1"))))
    disk.stats.get(gone) should be(Some(ReconcileDisk.PathStats(Some(false), Some(false))))
    disk.stats.get(rebuilt).map(_.dir) should be(Some(Some(true)))
    disk.current(s2, Some(gone)) should be(true)
    disk.current(s2, None) should be(false)
    disk.current(s, None) should be(true)

  // ---- Usage ----

  test("usage ticks never queue a second job for a held device"):
    val (host, devA, _) = twoDevices()
    var published = List.empty[StorageDeviceId]
    def tick(): Unit =
      host.control.reconciler.pollUsage((sds, _, _) => published :+= sds.storageDeviceId)
      drain(host)
    holdDevice(host, devA)
    tick()
    tick()
    tick()
    heldJobs(host, devA) should be(List(JobKind.Usage))
    published should be(List(deviceB, deviceB, deviceB))
    local(host, deviceA).usage should be(None)
    local(host, deviceB).usage.exists((used, total) => total > 0 && used >= 0 && used <= total) should be(true)
    releaseDevice(host, devA)
    drain(host)
    published.count(_ == deviceA) should be(1)
    local(host, deviceA).usage.isDefined should be(true)
    heldJobs(host, devA) should be(Nil)
    host.control.reconciler.outstandingJobs should be(empty)
    // Cleared on its outcome: the next tick measures A again
    tick()
    published.count(_ == deviceA) should be(2)

  test("a usage measurement completing after unload publishes nothing"):
    val (host, devA, _) = twoDevices()
    val sdsA = local(host, deviceA)
    var published = List.empty[StorageDeviceId]
    holdDevice(host, devA)
    host.control.reconciler.pollUsage((sds, _, _) => published :+= sds.storageDeviceId)
    drain(host)
    host.control.shutdown.unloadDevice(deviceA)
    drain(host)
    releaseDevice(host, devA)
    drain(host)
    published should be(List(deviceB))
    sdsA.usage should be(None)
    host.control.reconciler.outstandingJobs should be(empty)

  // ---- readAtLoad's illegal case ----

  test("a load's read the record's table refuses waits for its copy's location on a held device, then takes it"):
    val (host, devA, _) = twoDevices()
    val sds = local(host, deviceA)
    val registry = host.control.copyRegistry
    val copyId = recordOn(host, deviceA, s).get.copyId
    // The record reads Lost, its copy still in live/ (a miswired site): a load then reads another Live copy's path
    registry.copiesOf(sds).get.applyPhase(s, Phase.Lost, Phase.Lost) should be(None)
    holdDevice(host, devA)
    registry.readAtLoad(sds, CopyLayout.path(devA, s, UUID.randomUUID(), Phase.Live), s, Phase.Live)
    registry.takeRegistryErrors().size should be(1)
    // Lost -> Live is refused: the record waits for its copy's location, read on the device's lane
    heldJobs(host, devA) should be(List(JobKind.CopyLayout))
    recordOn(host, deviceA, s).map(r => (r.copyId, r.phase)) should be(Some((copyId, Phase.Lost)))
    releaseDevice(host, devA)
    drain(host)
    recordOn(host, deviceA, s).map(r => (r.copyId, r.phase)) should be(Some((copyId, Phase.Live)))
    heldJobs(host, devA) should be(Nil)
    noViolations(host)
