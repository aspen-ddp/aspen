package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.host.DeviceMetadata.RebuildOutcome
import org.aspen_ddp.aspen.server.rebuild.{RebuildDisk, RebuildState, RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.BackendOpener

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** A rebuild's bookkeeping on the device lanes (device I/O isolation, Task 14): the listing of its store's copies at
 *  its finish, the Adopt's move to Live, the staging checkpoint's drop before a lost or foreign copy is retired, a
 *  foreign copy's move to lost/ and the sweep of unlisted staging are each a `RebuildBookkeeping` (or `Move`) job on
 *  the device's lane, applied on the control thread after rechecking. Lockstep: a job runs when the test drains it,
 *  and a held device's jobs wait in `heldJobs`. Two devices: A (dev0) and B (dev1). Kinds are compared by label */
class RebuildIoSuite extends IntegrationTestSuite with ControlFixture:

  private val sA = StoreId(PoolId(UUID.fromString("66666666-6666-6666-6666-666666666666")), 0)
  private val sB = StoreId(sA.poolId, 1.toByte)

  private def device(hostRoot: os.Path, name: String, id: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, name, id)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  private final case class Bench(host: Host, factory: HeldRebuildFactory, devA: os.Path, devB: os.Path)

  /** A host with devices A and B; `place` puts copies on them before their load */
  private def rig(place: (os.Path, os.Path) => Unit = (_, _) => ()): Bench =
    val hostRoot = newHostDir()
    val devA = device(hostRoot, "dev0", deviceA)
    val devB = device(hostRoot, "dev1", deviceB)
    place(devA, devB)
    val factory = new HeldRebuildFactory
    val host = newHost(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 2)
    host.testingOnlyHandleEvents()
    Bench(host, factory, devA, devB)

  private def local(host: Host, id: StorageDeviceId): LocalStorageDeviceState = loadedDevices(host)(id)

  private def recordOn(host: Host, id: StorageDeviceId, s: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.device(id).flatMap(_.copies.get(s))

  private def labels(host: Host, dev: os.Path): List[String] = heldJobs(host, dev).map(_.label)

  private def noViolations(host: Host): org.scalatest.Assertion =
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  private def lockstep(host: Host): LockstepThreading = host.threading.asInstanceOf[LockstepThreading]

  /** Releases `dev`, runs the one management job its hold kept (of kind `label`), and holds `dev` again before that
   *  job's result is applied: whatever its apply submits on `dev` is then held */
  private def stepOnce(host: Host, dev: os.Path, label: String): Unit =
    labels(host, dev) should be(List(label))
    releaseDevice(host, dev)
    val jobs = lockstep(host).removeQueued:
      case t: LockstepThreading.DeviceTask => t.kind.label == label
      case _ => false
    jobs.size should be(1)
    jobs.foreach(_.run())
    holdDevice(host, dev)

  /** Starts the rebuild of `s` on `id` for `episode`, its entry Rebuilding in the latest reconcile's entries */
  private def start(host: Host, id: StorageDeviceId, s: StoreId, episode: UUID): Unit =
    val sds = local(host, id)
    sds.reconciledEntries = Some(sds.reconciledEntries.getOrElse(Map()) + (s -> StorageDeviceState.StoreEntry(
      StorageDeviceState.StoreStatus.Rebuilding, None, Some(episode))))
    host.control.rebuilds.startRebuild(sds, s, Some(episode), acceptLoss = false)

  /** Yields and drains (or only runs the copy and bookkeeping jobs, when `drain` is false: a held device's disk check
   *  would wait) until `condition` holds, failing loudly otherwise */
  private def pump(host: Host, what: String, drain: Boolean = true)(condition: => Boolean): Future[Unit] =
    awaitLoudly(what) {
      if drain then host.testingOnlyHandleEvents() else runCopyJobs(host)
      condition
    }

  /** A checkpoint of `s` in its staging directory on `dev`, as a pass that walked past one object leaves */
  private def writeCheckpoint(dev: os.Path, s: StoreId): os.Path =
    val staging = dev / RebuildingStore.RebuildDirectory / s.directoryName
    os.makeDir.all(staging)
    RebuildDisk.save(staging, RebuildState(s, None, Some(Key(Array[Byte](1))), Nil, Nil))
    staging

  private def outcome(host: Host, id: StorageDeviceId, s: StoreId): Option[RebuildOutcome] =
    recorded(host).rebuildOutcomes.get((id, s))

  private def running(host: Host, s: StoreId): Boolean = host.control.rebuilds.hasRunningRebuild(s)

  /** Both devices in the tree, sA Rebuilding on A and sB on B, each rebuild started */
  private def startBoth(r: Bench, eA: UUID, eB: UUID)(using ExecutionContext): Future[Unit] =
    for
      _ <- net.createDevice(deviceA)
      _ <- net.createDevice(deviceB)
      _ <- setDeviceState(deviceA)(_.setRebuilding(sA, eA))
      _ <- setDeviceState(deviceB)(_.setRebuilding(sB, eB))
      _ <- waitForTransactionsToComplete()
      _ = start(r.host, deviceA, sA, eA)
      _ = start(r.host, deviceB, sB, eB)
      _ <- pump(r.host, "both rebuilds created")(r.factory.created.size == 2)
    yield ()

  private def rebuildOf(r: Bench, s: StoreId): HeldRebuild = r.factory.created.find(_.storeId == s).get

  // ---- The finish's listing ----

  atest("a rebuild's finish waits on a held device and another device's rebuild completes"):
    given ExecutionContext = executionContext
    val r = rig()
    val (eA, eB) = (UUID.randomUUID(), UUID.randomUUID())
    for
      _ <- startBoth(r, eA, eB)
      _ = holdDevice(r.host, r.devA)
      _ = rebuildOf(r, sA).placeAndComplete()
      _ = rebuildOf(r, sB).placeAndComplete()
      _ <- pump(r.host, "B's rebuild settled", drain = false)(
             outcome(r.host, deviceB, sB).nonEmpty && !running(r.host, sB))
      // A's finish lists its store's copies on its held lane: no flip, its slot still taken
      heldA = labels(r.host, r.devA)
      outcomeA = outcome(r.host, deviceA, sA)
      slots = r.host.control.rebuilds.rebuildSlotsTaken
      phaseA = recordOn(r.host, deviceA, sA).map(_.phase)
      _ = releaseDevice(r.host, r.devA)
      _ <- pump(r.host, "A's rebuild settled")(outcome(r.host, deviceA, sA).nonEmpty && !running(r.host, sA))
    yield
      outcome(r.host, deviceB, sB) should be(Some(RebuildOutcome.Adopt))
      recorded(r.host).loadStoreByIdRequests should contain((deviceB, sB))
      heldA should be(List("RebuildBookkeeping"))
      outcomeA should be(None)
      slots should be(1)
      phaseA should be(Some(Phase.Rebuilt(eA.toString)))
      outcome(r.host, deviceA, sA) should be(Some(RebuildOutcome.Adopt))
      recordOn(r.host, deviceA, sA).map(_.phase) should be(Some(Phase.Live))
      recorded(r.host).loadStoreByIdRequests should contain((deviceA, sA))
      r.host.control.rebuilds.rebuildSlotsTaken should be(0)
      labels(r.host, r.devA) should be(Nil)
      noViolations(r.host)

  atest("a rebuild's finish listing completing after unload loads nothing and frees its slot"):
    given ExecutionContext = executionContext
    val r = rig()
    val (eA, eB) = (UUID.randomUUID(), UUID.randomUUID())
    for
      _ <- startBoth(r, eA, eB)
      _ = holdDevice(r.host, r.devA)
      _ = rebuildOf(r, sA).placeAndComplete()
      _ <- pump(r.host, "A's listing held", drain = false)(labels(r.host, r.devA).nonEmpty)
      held = labels(r.host, r.devA)
      _ = r.host.testingOnlySkipDiskCheck(deviceA)
      _ = r.host.control.shutdown.unloadDevice(deviceA)
      _ = releaseDevice(r.host, r.devA)
      _ <- pump(r.host, "A's slot released")(!running(r.host, sA))
    yield
      held should be(List("RebuildBookkeeping"))
      // Its device unloaded: nothing recorded or loaded there, and the slot is free whatever the flip decided
      r.host.control.copyRegistry.device(deviceA) should be(None)
      recorded(r.host).loadStoreByIdRequests should not contain((deviceA, sA))
      // B's rebuild, still running, holds the other
      r.host.control.rebuilds.rebuildSlotsTaken should be(1)
      noViolations(r.host)

  // ---- The Adopt's move to Live ----

  /** Starts both, then runs A's finish listing with A held from then on: A's Adopt move is submitted onto the hold */
  private def adoptHeld(r: Bench, eA: UUID, eB: UUID)(using ExecutionContext): Future[Unit] =
    for
      _ <- startBoth(r, eA, eB)
      _ = holdDevice(r.host, r.devA)
      _ = rebuildOf(r, sA).placeAndComplete()
      _ <- pump(r.host, "A's listing held", drain = false)(labels(r.host, r.devA).nonEmpty)
      _ = stepOnce(r.host, r.devA, "RebuildBookkeeping")
      _ <- pump(r.host, "A's move held", drain = false)(labels(r.host, r.devA).nonEmpty)
    yield ()

  atest("an Adopt's move to Live waits on a held device, its slot taken, and is loaded once it applies"):
    given ExecutionContext = executionContext
    val r = rig()
    val (eA, eB) = (UUID.randomUUID(), UUID.randomUUID())
    for
      _ <- adoptHeld(r, eA, eB)
      held = labels(r.host, r.devA)
      flipped = outcome(r.host, deviceA, sA)
      busy = recordOn(r.host, deviceA, sA).map(c => (c.phase, c.activity))
      slotHeld = running(r.host, sA)
      loadedBefore = recorded(r.host).loadStoreByIdRequests.contains((deviceA, sA))
      _ = releaseDevice(r.host, r.devA)
      _ <- pump(r.host, "A's rebuild settled")(!running(r.host, sA))
    yield
      held should be(List("Move"))
      flipped should be(Some(RebuildOutcome.Adopt))
      busy should be(Some((Phase.Rebuilt(eA.toString), Activity.DiskJob(JobKind.Move, Activity.Idle))))
      slotHeld should be(true)
      loadedBefore should be(false)
      recordOn(r.host, deviceA, sA).map(c => (c.phase, c.activity)) should be(Some((Phase.Live, Activity.Idle)))
      CopyFixtures.phaseOf(r.devA, sA) should be(Some(Phase.Live))
      recorded(r.host).loadStoreByIdRequests should contain((deviceA, sA))
      noViolations(r.host)

  atest("an Adopt's move completing after unload loads nothing and frees its slot"):
    given ExecutionContext = executionContext
    val r = rig()
    val (eA, eB) = (UUID.randomUUID(), UUID.randomUUID())
    for
      _ <- adoptHeld(r, eA, eB)
      _ = r.host.testingOnlySkipDiskCheck(deviceA)
      _ = r.host.control.shutdown.unloadDevice(deviceA)
      _ = releaseDevice(r.host, r.devA)
      _ <- pump(r.host, "A's slot released")(!running(r.host, sA))
    yield
      r.host.control.copyRegistry.device(deviceA) should be(None)
      recorded(r.host).loadStoreByIdRequests should not contain((deviceA, sA))
      labels(r.host, r.devA) should be(Nil)
      noViolations(r.host)

  // ---- The staging checkpoint's drop ----

  atest("a checkpoint drop waits on a held device and another device's rebuild starts"):
    given ExecutionContext = executionContext
    var stagingA: os.Path = null
    val r = rig: (devA, _) =>
      CopyFixtures.placeCopy(devA, sA, Phase.Lost)
      stagingA = writeCheckpoint(devA, sA)
    val (eA, eB) = (UUID.randomUUID(), UUID.randomUUID())
    holdDevice(r.host, r.devA)
    start(r.host, deviceA, sA, eA)
    start(r.host, deviceB, sB, eB)
    for
      _ <- pump(r.host, "B's rebuild created", drain = false)(r.factory.created.exists(_.storeId == sB))
      held = labels(r.host, r.devA)
      stagingWhileHeld = os.exists(stagingA / RebuildState.stateFilename)
      workWhileHeld = r.host.control.copyRegistry.rebuildWorkOn(deviceA, sA).map(_.stage)
      createdWhileHeld = r.factory.created.exists(_.storeId == sA)
      _ = releaseDevice(r.host, r.devA)
      _ <- pump(r.host, "A's rebuild created")(r.factory.created.exists(_.storeId == sA))
    yield
      held should be(List("RebuildBookkeeping"))
      stagingWhileHeld should be(true)
      workWhileHeld should be(Some(RebuildStage.Queued))
      createdWhileHeld should be(false)
      // Dropped, then the Lost copy's discard and the handoff to the rebuild
      os.exists(stagingA) should be(false)
      CopyFixtures.copyPath(r.devA, sA) should be(None)
      CopyFixtures.retiredCopies(r.devA, sA).size should be(1)
      noViolations(r.host)

  atest("a checkpoint drop completing after its rebuild work ended starts nothing"):
    given ExecutionContext = executionContext
    val r = rig((devA, _) =>
      CopyFixtures.placeCopy(devA, sA, Phase.Lost)
      writeCheckpoint(devA, sA): Unit)
    val eA = UUID.randomUUID()
    holdDevice(r.host, r.devA)
    start(r.host, deviceA, sA, eA)
    val held = labels(r.host, r.devA)
    // Its entry left the device: the reconcile ends the queued work
    r.host.control.rebuilds.endQueuedRebuild(local(r.host, deviceA), sA)
    releaseDevice(r.host, r.devA)
    pump(r.host, "the drop applied")(labels(r.host, r.devA).isEmpty &&
                                     !r.host.control.rebuilds.stagingDropRunning(deviceA, sA)).map: _ =>
      held should be(List("RebuildBookkeeping"))
      r.factory.created should be(empty)
      r.host.control.copyRegistry.rebuildWorkOn(deviceA, sA) should be(None)
      r.host.control.rebuilds.rebuildSlotsTaken should be(0)
      // No discard: the Lost copy stays for whatever decides on the entry now
      recordOn(r.host, deviceA, sA).map(_.phase) should be(Some(Phase.Lost))
      CopyFixtures.retiredCopies(r.devA, sA) should be(Nil)
      noViolations(r.host)

  atest("a checkpoint drop completing after unload starts nothing"):
    given ExecutionContext = executionContext
    val r = rig((devA, _) =>
      CopyFixtures.placeCopy(devA, sA, Phase.Lost)
      writeCheckpoint(devA, sA): Unit)
    holdDevice(r.host, r.devA)
    start(r.host, deviceA, sA, UUID.randomUUID())
    val held = labels(r.host, r.devA)
    r.host.testingOnlySkipDiskCheck(deviceA)
    r.host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(r.host, r.devA)
    pump(r.host, "the drop applied")(labels(r.host, r.devA).isEmpty &&
                                     !r.host.control.rebuilds.stagingDropRunning(deviceA, sA)).map: _ =>
      held should be(List("RebuildBookkeeping"))
      r.factory.created should be(empty)
      r.host.control.copyRegistry.device(deviceA) should be(None)
      r.host.control.rebuilds.rebuildSlotsTaken should be(0)
      CopyFixtures.retiredCopies(r.devA, sA) should be(Nil)
      noViolations(r.host)

  atest("a checkpoint drop whose stat fails starts nothing, and a later start drops it and goes on"):
    given ExecutionContext = executionContext
    var stagingA: os.Path = null
    val r = rig: (devA, _) =>
      CopyFixtures.placeCopy(devA, sA, Phase.Lost)
      stagingA = writeCheckpoint(devA, sA)
    val eA = UUID.randomUUID()
    val root = r.devA / RebuildingStore.RebuildDirectory
    // The staging directory cannot be stat'd: the drop fails, and the rebuild waits for a later start
    os.perms.set(root, "rw-------")
    start(r.host, deviceA, sA, eA)
    try
      runCopyJobs(r.host)
      r.factory.created should be(empty)
      r.host.control.copyRegistry.rebuildWorkOn(deviceA, sA).map(_.stage) should be(Some(RebuildStage.Queued))
      r.host.control.rebuilds.stagingDropRunning(deviceA, sA) should be(false)
      recordOn(r.host, deviceA, sA).map(_.phase) should be(Some(Phase.Lost))
    finally os.perms.set(root, "rwx------")
    os.exists(stagingA / RebuildState.stateFilename) should be(true)
    start(r.host, deviceA, sA, eA)
    pump(r.host, "A's rebuild created")(r.factory.created.nonEmpty).map: _ =>
      os.exists(stagingA) should be(false)
      CopyFixtures.retiredCopies(r.devA, sA).size should be(1)
      noViolations(r.host)

  atest("a Lost copy with no staging goes straight on from the drop to its discard"):
    given ExecutionContext = executionContext
    val r = rig((devA, _) => CopyFixtures.placeCopy(devA, sA, Phase.Lost): Unit)
    start(r.host, deviceA, sA, UUID.randomUUID())
    pump(r.host, "A's rebuild created")(r.factory.created.nonEmpty).map: _ =>
      CopyFixtures.copyPath(r.devA, sA) should be(None)
      CopyFixtures.retiredCopies(r.devA, sA).size should be(1)
      noViolations(r.host)

  // ---- A foreign copy's move to lost/ ----

  atest("a foreign copy's move to lost/ waits on a held device, the copy held offline, and the rebuild goes on"):
    given ExecutionContext = executionContext
    val r = rig((devA, _) => CopyFixtures.placeCopy(devA, sA, Phase.Live): Unit)
    holdDevice(r.host, r.devA)
    start(r.host, deviceA, sA, UUID.randomUUID())
    // The drop runs; its apply asks for the move, which the hold keeps
    stepOnce(r.host, r.devA, "RebuildBookkeeping")
    for
      _ <- pump(r.host, "A's move held", drain = false)(labels(r.host, r.devA).nonEmpty)
      held = labels(r.host, r.devA)
      busy = recordOn(r.host, deviceA, sA).map(c => (c.phase, c.activity))
      offline = r.host.control.moves.isPendingLost(local(r.host, deviceA), sA)
      created = r.factory.created.nonEmpty
      _ = releaseDevice(r.host, r.devA)
      _ <- pump(r.host, "A's rebuild created")(r.factory.created.nonEmpty)
    yield
      held should be(List("Move"))
      busy should be(Some((Phase.Live, Activity.DiskJob(JobKind.Move, Activity.Idle))))
      offline should be(true)
      created should be(false)
      // Moved to lost/, then retired by the discard, and the rebuild started
      r.host.control.moves.isPendingLost(local(r.host, deviceA), sA) should be(false)
      CopyFixtures.copyPath(r.devA, sA) should be(None)
      CopyFixtures.retiredCopies(r.devA, sA).size should be(1)
      noViolations(r.host)

  // ---- The staging sweep ----

  /** Checks `id` against a tree entry naming none of its stores */
  private def check(host: Host, id: StorageDeviceId): Unit =
    recorded(host).armLookup(id).success(deviceState(id))
    host.control.reconciler.checkStorageDevice(id)

  atest("a staging sweep waits on a held device and another device's sweep completes"):
    given ExecutionContext = executionContext
    var (stagingA, stagingB): (os.Path, os.Path) = (null, null)
    val r = rig: (devA, devB) =>
      stagingA = writeCheckpoint(devA, sA)
      stagingB = writeCheckpoint(devB, sB)
    holdDevice(r.host, r.devA)
    check(r.host, deviceA)
    check(r.host, deviceB)
    for
      _ <- pump(r.host, "A's inputs held", drain = false)(labels(r.host, r.devA).nonEmpty)
      // A's check reads its inputs, the staging listing with them; the sweep's removal it asks for is held
      _ = stepOnce(r.host, r.devA, "ReconcileInputs")
      _ <- pump(r.host, "B's sweep and A's held removal", drain = false)(
             !os.exists(stagingB) && labels(r.host, r.devA).nonEmpty)
      held = labels(r.host, r.devA)
      keptA = os.exists(stagingA)
      _ = releaseDevice(r.host, r.devA)
      _ <- pump(r.host, "A's sweep")(!os.exists(stagingA))
    yield
      // The removal, then the release the same check asks for after it
      held should be(List("RebuildBookkeeping", "Release"))
      keptA should be(true)
      r.host.control.rebuilds.stagingSweepRunning(r.devA) should be(false)
      noViolations(r.host)

  atest("a staging sweep never queues a second removal while one is held"):
    given ExecutionContext = executionContext
    var stagingA: os.Path = null
    val r = rig((devA, _) => stagingA = writeCheckpoint(devA, sA))
    val sds = local(r.host, deviceA)
    holdDevice(r.host, r.devA)
    val disk = ReconcileDisk.gather(r.devA, Map())
    r.host.control.rebuilds.sweepUnlistedStaging(sds, deviceState(deviceA), disk)
    r.host.control.rebuilds.sweepUnlistedStaging(sds, deviceState(deviceA), disk)
    labels(r.host, r.devA) should be(List("RebuildBookkeeping"))
    r.host.control.rebuilds.stagingSweepRunning(r.devA) should be(true)
    releaseDevice(r.host, r.devA)
    pump(r.host, "A's sweep")(!os.exists(stagingA)).map: _ =>
      r.host.control.rebuilds.stagingSweepRunning(r.devA) should be(false)
      noViolations(r.host)

  atest("a staging sweep keeps a listed store's staging and removes an unlisted one"):
    given ExecutionContext = executionContext
    var (listedStaging, unlisted): (os.Path, os.Path) = (null, null)
    val r = rig: (devA, _) =>
      listedStaging = writeCheckpoint(devA, sA)
      unlisted = writeCheckpoint(devA, sB)
    val sds = local(r.host, deviceA)
    val listed = deviceState(deviceA, Map(sA -> StorageDeviceState.StoreEntry(
      StorageDeviceState.StoreStatus.Rebuilding, None, Some(UUID.randomUUID()))))
    r.host.control.rebuilds.sweepUnlistedStaging(sds, listed, ReconcileDisk.gather(r.devA, Map()))
    pump(r.host, "the sweep")(!r.host.control.rebuilds.stagingSweepRunning(r.devA)).map: _ =>
      os.exists(listedStaging) should be(true)
      os.exists(unlisted) should be(false)
      noViolations(r.host)

  test("RebuildBookkeepingDisk lists staging directories, drops a checkpoint and reports a missing staging"):
    val dev = newHostDir() / "dev"
    RebuildBookkeepingDisk.listStaging(dev) should be(Nil)
    val staging = writeCheckpoint(dev, sA)
    RebuildBookkeepingDisk.listStaging(dev) should be(List(staging))
    RebuildBookkeepingDisk.dropStagingCheckpoint(staging) should be(RebuildBookkeepingDisk.CheckpointDrop.Dropped)
    os.exists(staging) should be(false)
    RebuildBookkeepingDisk.dropStagingCheckpoint(staging) should be(RebuildBookkeepingDisk.CheckpointDrop.NoStaging)
    RebuildBookkeepingDisk.removeStaging(List(writeCheckpoint(dev, sB))).map(_._2) should be(List(None))
    RebuildBookkeepingDisk.listStaging(dev) should be(Nil)

