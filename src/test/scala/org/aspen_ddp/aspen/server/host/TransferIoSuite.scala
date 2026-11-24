package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.store.backend.Backend

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Transfers on the device lanes (device I/O isolation, Task 15): a transfer in's promotion, its arrived-copy check
 *  and its staging removals, and a transfer out's extraction reads and its reinstating move, each one job on the
 *  device's lane (TransferDisk), applied on the control thread after a recheck. Lockstep: a job runs when the test
 *  drains it, and a held device's jobs wait in `heldJobs` */
class TransferIoSuite extends IntegrationTestSuite with ControlFixture:

  private val first = StoreId(PoolId.BootstrapPoolId, 1)
  private val second = StoreId(PoolId.BootstrapPoolId, 2)

  // The hosts the pump drains: every event of theirs, lane results included, in FIFO order
  private val pumped = scala.collection.mutable.ArrayBuffer[Host]()

  private def pumpUntil(condition: => Boolean, rounds: Int = 20): Future[Unit] =
    given ExecutionContext = executionContext
    if condition || rounds == 0 then
      Future.unit
    else
      pumped.foreach(_.testingOnlyHandleEvents())
      for
        _ <- yieldUntil(condition, 20)
        _ <- waitForTransactionsToComplete()
        _ <- pumpUntil(condition, rounds - 1)
      yield ()

  /** pumpUntil, waiting in real time between rounds for the client's retry backoff (two pool updates of one pool
   *  collide, and one retries), until `condition` or 20 s; callers assert the end state */
  private def pumpPolling(condition: => Boolean): Future[Unit] =
    val deadline = System.nanoTime() + 20_000_000_000L
    given ExecutionContext = executionContext
    def loop(): Future[Unit] =
      pumpUntil(condition, rounds = 2).flatMap: _ =>
        if condition || System.nanoTime() > deadline then Future.unit
        else
          val p = scala.concurrent.Promise[Unit]()
          client.backgroundTaskManager.schedule(scala.concurrent.duration.Duration(10, "ms"))(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  private def drain(host: Host): Unit = host.testingOnlyHandleEvents()

  private def transfers(host: Host): StoreTransfers = host.control.transfers

  /** A receiving host with two devices, the second device (dev0) and the third (dev1), and a transfer in begun onto
   *  each: `first` onto dev0, `second` onto dev1 */
  private def receivingTwo(): Future[(Host, os.Path, os.Path, StagedTransferService)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev0 = writeDevice(hostRoot, "dev0", net.secondDeviceId)
    val dev1 = writeDevice(hostRoot, "dev1", net.thirdDeviceId)
    val service = new StagedTransferService
    val host = newHost(hostRoot, transferService = service)
    pumped.clear()
    pumped += host
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(first, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(second, net.thirdDeviceId)
      _ <- waitForTransactionsToComplete()
      _ = checkAllNow(host)
      _ <- pumpUntil(service.startedTransfersIn == Set(first, second))
    yield
      service.startedTransfersIn should be(Set(first, second))
      (host, dev0, dev1, service)

  // ---- Promotion ----

  atest("a transfer-in promotion waits on a held device and others proceed"):
    given ExecutionContext = executionContext
    for
      (host, dev0, dev1, service) <- receivingTwo()
      _ = holdDevice(host, dev0)
      _ = service.finishTransferIn(first)
      _ = service.finishTransferIn(second)
      _ <- pumpPolling(recorded(host).transferOutcomes.contains(second) &&
                     CopyFixtures.phaseOf(dev1, second).contains(Phase.Live))
      heldWhile = heldJobs(host, dev0)
      promotingWhile = transfers(host).promotingStores
      placedWhile = CopyFixtures.copyPath(dev0, first)
      _ = releaseDevice(host, dev0)
      _ <- pumpPolling(recorded(host).transferOutcomes.contains(first) &&
                     CopyFixtures.phaseOf(dev0, first).contains(Phase.Live))
    yield
      // The promotion's job waits on the held lane: nothing placed, the store still promoting, no pool update
      heldWhile should contain(JobKind.Move)
      promotingWhile should be(Set(first))
      placedWhile should be(None)
      // The same sequence on the other device completed
      CopyFixtures.phaseOf(dev1, second) should be(Some(Phase.Live))
      // Once released, the held one completes as well
      CopyFixtures.phaseOf(dev0, first) should be(Some(Phase.Live))
      transfers(host).promotingStores should be(Set())
      transfers(host).stagedAttempts should be(Set())
      (!os.exists(dev0 / "transferring-in") || os.list(dev0 / "transferring-in").isEmpty) should be(true)

  atest("a transfer-in promotion completing after unload changes nothing"):
    given ExecutionContext = executionContext
    for
      (host, dev0, _, service) <- receivingTwo()
      _ = holdDevice(host, dev0)
      _ = service.finishTransferIn(first)
      _ <- pumpUntil(transfers(host).promotingStores.contains(first))
      _ = host.control.shutdown.unloadDevice(net.secondDeviceId)
      unloadedAttempts = transfers(host).receiving.get(first)
      _ = releaseDevice(host, dev0)
      _ <- pumpUntil(transfers(host).promotingStores.isEmpty)
      _ <- pumpUntil(false, rounds = 2)
    yield
      unloadedAttempts should be(None)
      transfers(host).promotingStores should be(Set())
      loadedDevices(host).contains(net.secondDeviceId) should be(false)
      // No pool update, no record: the copy the job placed is the unloaded device's, for its next load to list
      recorded(host).transferOutcomes.contains(first) should be(false)
      host.control.copyRegistry.device(net.secondDeviceId) should be(None)
      transfers(host).receiving.contains(first) should be(false)

  atest("an arrived copy's move to Live (T9) waits on a held device and others proceed"):
    given ExecutionContext = executionContext
    for
      (host, dev0, dev1, service) <- receivingTwo()
      // Both promoted, their pool updates held, so the moves to Live come after the hold
      gate = recorded(host).holdTransferOutcomes()
      _ = service.finishTransferIn(first)
      _ = service.finishTransferIn(second)
      _ <- pumpUntil(CopyFixtures.phaseOf(dev0, first).exists(_.isInstanceOf[Phase.Arrived]) &&
                     CopyFixtures.phaseOf(dev1, second).exists(_.isInstanceOf[Phase.Arrived]) &&
                     transfers(host).promotingStores.isEmpty)
      _ = holdDevice(host, dev0)
      _ = gate.success(())
      _ <- pumpPolling(recorded(host).transferOutcomes.contains(first) &&
                       CopyFixtures.phaseOf(dev1, second).contains(Phase.Live) &&
                       heldJobs(host, dev0).contains(JobKind.Move))
      heldWhile = heldJobs(host, dev0)
      phaseWhile = CopyFixtures.phaseOf(dev0, first)
      arrivalWhile = CopyFixtures.copyPath(dev0, first).map(p => os.exists(p / StoreLoader.ArrivedAtFile))
      activityWhile = host.control.copyRegistry.device(net.secondDeviceId).flatMap(_.copies.get(first)).map(_.activity)
      _ = releaseDevice(host, dev0)
      _ <- pumpPolling(CopyFixtures.phaseOf(dev0, first).contains(Phase.Live))
    yield
      heldWhile should contain(JobKind.Move)
      // Still Arrived, its arrival not yet written (the write is the move's own job), the copy busy
      phaseWhile.exists(_.isInstanceOf[Phase.Arrived]) should be(true)
      arrivalWhile should be(Some(false))
      activityWhile should be(Some(Activity.DiskJob(JobKind.Move, Activity.Idle)))
      CopyFixtures.phaseOf(dev1, second) should be(Some(Phase.Live))
      CopyFixtures.copyPath(dev1, second).map(p => os.exists(p / StoreLoader.ArrivedAtFile)) should be(Some(true))
      // Released: the arrival written, then Live
      CopyFixtures.phaseOf(dev0, first) should be(Some(Phase.Live))
      CopyFixtures.copyPath(dev0, first).map(p => os.exists(p / StoreLoader.ArrivedAtFile)) should be(Some(true))

  // ---- Staging removal ----

  atest("a cancelled transfer-in behind a hung job queues one removal"):
    given ExecutionContext = executionContext
    for
      (host, dev0, _, service) <- receivingTwo()
      attempt = transfers(host).receiving(first)
      staging = StoreTransfers.stagingOf(dev0, attempt)
      _ = os.makeDir.all(staging / first.directoryName)
      _ = holdDevice(host, dev0)
      // Cancelled twice: the abandon's drop (the service's cancel, then the staging's removal), then the service again
      source = transfers(host).pendingStartTransfers(first).msg.fromDevice
      _ = transfers(host).dropAbandonedTransferIn(first, source, net.secondDeviceId)
      _ = host.control.storeTransferService.cancelTransferIn(first, attempt)
      _ <- pumpUntil(false, rounds = 2)
      heldWhile = heldJobs(host, dev0)
      stagedWhile = transfers(host).stagedAttempts
      _ = releaseDevice(host, dev0)
      _ <- pumpUntil(!os.exists(staging) && !transfers(host).stagedAttempts.contains(attempt))
    yield
      // Exactly one removal, behind the hung lane; its staging still recorded, nothing taken for removed
      heldWhile should be(List(JobKind.Delete))
      stagedWhile should contain(attempt)
      service.cancelledTransfersIn.map(_._2).toSet should contain(attempt)
      os.exists(staging) should be(false)
      // The cancelled attempt is recorded nowhere (the device's check may have begun a new one: the tree still names
      // the transfer)
      transfers(host).stagedAttempts should not contain attempt
      transfers(host).receiving.get(first) should not be Some(attempt)

  test("an attempt's staging removal is one job, and the service's job supersedes a queued one before it starts"):
    val threading = new LockstepThreading(ExecutionContext.parasitic)
    val dev = os.temp.dir(prefix = "transfer-io")
    try
      val staging = dev / "transferring-in" / UUID.randomUUID().toString
      os.makeDir.all(staging / "copy")
      val io = threading.newDeviceIo(dev)
      val st = new StoreTransfers.AttemptStaging(io, dev, first, staging)
      threading.hold(dev)
      val queued = st.remove()
      // Asked again behind the hung lane: the same removal, nothing more queued
      st.remove() should be theSameInstanceAs queued
      threading.held(dev) should be(List(JobKind.Delete))
      // The service's own removal supersedes it: the queued job is cancelled first, then one job does both
      var ran = 0
      val superseding = st.run { ran += 1 }
      queued.value should be(Some(Failure(DeviceIo.Cancelled)))
      threading.held(dev) should be(List(JobKind.Delete, JobKind.Delete))
      os.exists(staging) should be(true)
      threading.release(dev)
      threading.drain()
      ran should be(1)
      superseding.value should be(Some(Success(())))
      os.exists(staging) should be(false)
      // Done: asked again, it is that one's
      st.remove() should be theSameInstanceAs superseding
      threading.held(dev) should be(Nil)
      // The service's job after the removal ran runs alone
      st.run { ran += 1 }
      threading.drain()
      ran should be(2)
    finally os.remove.all(dev)

  // ---- Arrived copy check ----

  /** A receiving host whose device dev0 already holds an Arrived copy of `first` of another transfer, and the transfer
   *  of `first` onto it requested; the host's device check not yet run */
  private def arrivedOfAnother(): Future[(Host, os.Path, StagedTransferService, () => Unit)] =
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val dev0 = writeDevice(hostRoot, "dev0", net.secondDeviceId)
    val arrived = CopyFixtures.placeCopy(dev0, first, Phase.Arrived("another-transfer"))
    CrashRecoveryLog.saveStoreState(first, StoreRecoveryState.Empty, Backend.crlSaveFile(arrived))
    val service = new StagedTransferService
    val host = newHost(hostRoot, transferService = service)
    pumped.clear()
    pumped += host
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(first, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      st <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      drain(host)
      val entry = st.stores(first)
      val start = () => transfers(host).startIn(first, org.aspen_ddp.aspen.common.metadata.HostId.BootstrapHostId,
        entry.transferDevice.get, net.secondDeviceId, entry.transferId)
      (host, dev0, service, start)

  atest("an arrived copy's check waits on a held device, then deletes another transfer's copy and receives"):
    given ExecutionContext = executionContext
    for
      (host, dev0, service, start) <- arrivedOfAnother()
      _ = holdDevice(host, dev0)
      _ = start()
      _ <- pumpUntil(heldJobs(host, dev0).contains(JobKind.Delete))
      heldWhile = heldJobs(host, dev0)
      busyWhile = host.control.copyRegistry.device(net.secondDeviceId).flatMap(_.copies.get(first)).map(_.activity)
      startedWhile = service.startedTransfersIn
      _ = releaseDevice(host, dev0)
      _ <- pumpUntil(service.startedTransfersIn.contains(first))
    yield
      heldWhile should be(List(JobKind.Delete))
      busyWhile should be(Some(Activity.DiskJob(JobKind.Delete, Activity.Idle)))
      startedWhile should be(Set())
      // Deleted rename-first, its record removed, and the receive begun
      CopyFixtures.copyPath(dev0, first) should be(None)
      service.startedTransfersIn should be(Set(first))

  atest("an arrived copy's check completing after unload changes nothing"):
    given ExecutionContext = executionContext
    for
      (host, dev0, service, start) <- arrivedOfAnother()
      _ = holdDevice(host, dev0)
      _ = start()
      _ <- pumpUntil(heldJobs(host, dev0).contains(JobKind.Delete))
      _ = host.control.shutdown.unloadDevice(net.secondDeviceId)
      _ = releaseDevice(host, dev0)
      _ <- pumpUntil(false, rounds = 2)
    yield
      // The job ran (its delete is the disk's), but nothing is recorded or started for the unloaded device
      loadedDevices(host).contains(net.secondDeviceId) should be(false)
      host.control.copyRegistry.device(net.secondDeviceId) should be(None)
      service.startedTransfersIn should be(Set())
      transfers(host).receiving.contains(first) should be(false)

  test("checkArrived's branches: usable, deleted, not deleted and unreadable"):
    val dev = os.temp.dir(prefix = "transfer-io")
    try
      val usable = CopyFixtures.placeCopy(dev, first, Phase.Arrived("t"))
      CrashRecoveryLog.saveStoreState(first, StoreRecoveryState.Empty, Backend.crlSaveFile(usable))
      TransferDisk.checkArrived(dev, usable, first, None) should be(TransferDisk.ArrivedCheck.Usable)
      os.exists(usable) should be(true)
      // Another transfer's: deleted without reading its save file
      TransferDisk.checkArrived(dev, usable, first, Some("another's")) should be(
        TransferDisk.ArrivedCheck.Deleted("another's"))
      os.exists(usable) should be(false)
      // An unusable save file: deleted
      val truncated = CopyFixtures.placeCopy(dev, second, Phase.Arrived("t"))
      os.write.over(Backend.crlSaveFile(truncated), "")
      TransferDisk.checkArrived(dev, truncated, second, None) shouldBe a[TransferDisk.ArrivedCheck.Deleted]
      os.exists(truncated) should be(false)
      // A save file whose stat fails says nothing: neither resumed nor deleted
      val locked = CopyFixtures.placeCopy(dev, first, Phase.Arrived("t"))
      os.perms.set(locked, "rw-------")
      try
        TransferDisk.checkArrived(dev, locked, first, None) shouldBe a[TransferDisk.ArrivedCheck.Unreadable]
        // An unusable copy the delete cannot answer gone stays
        os.perms.set(dev / CopyLayout.ArrivedDirectory, "r-x------")
        TransferDisk.checkArrived(dev, locked, first, Some("another's")) shouldBe
          a[TransferDisk.ArrivedCheck.NotDeleted]
      finally
        os.perms.set(dev / CopyLayout.ArrivedDirectory, "rwx------")
        os.perms.set(locked, "rwx------")
      os.exists(locked) should be(true)
    finally os.remove.all(dev)

  test("promote's branches: an unusable save file is not moved, a valid one is, and the staging goes with it"):
    val dev = os.temp.dir(prefix = "transfer-io")
    try
      val sync = CopyDisk.DefaultSyncDirectory
      val rename = CopyDisk.DefaultRename
      val staging = dev / "transferring-in" / "a"
      val staged = staging / first.directoryName
      os.makeDir.all(staged)
      os.write.over(Backend.crlSaveFile(staged), "")
      val target = CopyLayout.path(dev, first, UUID.randomUUID(), Phase.Arrived("t"))
      TransferDisk.promote(first, staged, staging, target, Phase.Arrived("t"), sync, rename) shouldBe a[
        TransferDisk.Promotion.Invalid]
      os.exists(target) should be(false)
      os.exists(staging) should be(false)
      os.makeDir.all(staged)
      CrashRecoveryLog.saveStoreState(first, StoreRecoveryState.Empty, Backend.crlSaveFile(staged))
      TransferDisk.promote(first, staged, staging, target, Phase.Arrived("t"), sync, rename) should be(
        TransferDisk.Promotion.Moved(MoveResult.Durable))
      CopyLayoutDisk.phaseAt(target) should be(Some(Phase.Arrived("t")))
      CopyLayoutDisk.readParam(target, Phase.Arrived("")) should be("t")
      os.exists(staging) should be(false)
      // A rename that lands unforced keeps the staging, which the resync forces
      os.makeDir.all(staged)
      CrashRecoveryLog.saveStoreState(second, StoreRecoveryState.Empty, Backend.crlSaveFile(staged))
      val unforced = CopyLayout.path(dev, second, UUID.randomUUID(), Phase.Arrived("t"))
      val failingSync: os.Path => Unit = d => if d == unforced / os.up then throw java.io.IOException("injected")
      TransferDisk.promote(second, staged, staging, unforced, Phase.Arrived("t"), failingSync, rename) match
        case TransferDisk.Promotion.Moved(MoveResult.Unsynced(_)) => ()
        case other => fail(s"Expected an unsynced move, got $other")
      os.exists(staging) should be(true)
    finally os.remove.all(dev)

  // ---- Transfer out ----

  /** A host with devices A (dev0) and B (dev1), each holding an Outbound copy of its store (`first` on A, `second` on
   *  B) with a valid save file */
  private def outboundOnTwo(): (Host, os.Path, os.Path) =
    val hostRoot = newHostDir()
    val devA = writeDevice(hostRoot, "dev0", deviceA)
    val devB = writeDevice(hostRoot, "dev1", deviceB)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devA))
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devB))
    for (dev, sid) <- List(devA -> first, devB -> second) do
      val p = CopyFixtures.placeCopy(dev, sid, Phase.Outbound("t"))
      CrashRecoveryLog.saveStoreState(sid, StoreRecoveryState.Empty, Backend.crlSaveFile(p))
    val host = newHost(hostRoot)
    drain(host)
    (host, devA, devB)

  test("an abandoned transfer out's reinstating move waits on a held device and others proceed"):
    val (host, devA, devB) = outboundOnTwo()
    holdDevice(host, devA)
    host.control.transfersOut.reinstateAbandonedOut(loadedDevices(host)(deviceA), first)
    host.control.transfersOut.reinstateAbandonedOut(loadedDevices(host)(deviceB), second)
    drain(host)
    heldJobs(host, devA) should be(List(JobKind.Move))
    host.control.copyRegistry.device(deviceA).flatMap(_.copies.get(first)).map(_.activity) should be(
      Some(Activity.DiskJob(JobKind.Move, Activity.Idle)))
    CopyFixtures.phaseOf(devA, first) should be(Some(Phase.Outbound("t")))
    CopyFixtures.phaseOf(devB, second) should be(Some(Phase.Live))
    releaseDevice(host, devA)
    drain(host)
    CopyFixtures.phaseOf(devA, first) should be(Some(Phase.Live))
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  test("an abandoned transfer out's reinstating move completing after unload changes nothing"):
    val (host, devA, _) = outboundOnTwo()
    holdDevice(host, devA)
    host.control.transfersOut.reinstateAbandonedOut(loadedDevices(host)(deviceA), first)
    drain(host)
    heldJobs(host, devA) should be(List(JobKind.Move))
    host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(host, devA)
    drain(host)
    // The rename is the disk's; nothing is recorded or loaded for the unloaded device
    loadedDevices(host).contains(deviceA) should be(false)
    host.control.copyRegistry.device(deviceA) should be(None)
    host.control.router.loaded.contains(first) should be(false)
    host.control.loader.isOpening(first) should be(false)

  atest("a closed store's extraction reads wait on a held device and others proceed"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val devA = writeDevice(hostRoot, "dev0", deviceA)
    val devB = writeDevice(hostRoot, "dev1", deviceB)
    for dev <- List(devA, devB) do os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    CopyFixtures.placeCopy(devA, first, Phase.Live)
    CopyFixtures.placeCopy(devB, second, Phase.Live)
    val host = newHost(hostRoot)
    drain(host)
    holdDevice(host, devA)
    val onA = host.control.transfersOut.closeForTransferOut(first, loadedDevices(host)(deviceA))
    val onB = host.control.transfersOut.closeForTransferOut(second, loadedDevices(host)(deviceB))
    settled(host, onB).flatMap: _ =>
      onB.value should be(Some(Success(())))
      heldJobs(host, devA) should be(List(JobKind.SaveFile))
      onA.isCompleted should be(false)
      releaseDevice(host, devA)
      settled(host, onA).map: _ =>
        onA.value should be(Some(Success(())))

  atest("a closed store's extraction read completing after unload fails the extraction"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val devA = writeDevice(hostRoot, "dev0", deviceA)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devA))
    CopyFixtures.placeCopy(devA, first, Phase.Live)
    val host = newHost(hostRoot)
    drain(host)
    holdDevice(host, devA)
    val onA = host.control.transfersOut.closeForTransferOut(first, loadedDevices(host)(deviceA))
    drain(host)
    host.control.shutdown.unloadDevice(deviceA)
    releaseDevice(host, devA)
    settled(host, onA).transform(_ => Success(())).map: _ =>
      onA.value.exists(_.isFailure) should be(true)

  test("inspectSaveFile's branches: no copy, absent, valid and unusable"):
    val dev = os.temp.dir(prefix = "transfer-io")
    try
      val copy = dev / "copy"
      TransferDisk.inspectSaveFile(copy, first) should be(TransferDisk.SaveFileState.NoCopy)
      os.makeDir.all(copy)
      TransferDisk.inspectSaveFile(copy, first) should be(TransferDisk.SaveFileState.Absent)
      CrashRecoveryLog.saveStoreState(first, StoreRecoveryState.Empty, Backend.crlSaveFile(copy))
      TransferDisk.inspectSaveFile(copy, first) should be(TransferDisk.SaveFileState.Valid)
      // Another store's file is unusable
      TransferDisk.inspectSaveFile(copy, second) shouldBe a[TransferDisk.SaveFileState.Failed]
      os.perms.set(copy, "rw-------")
      try TransferDisk.inspectSaveFile(copy, first) shouldBe a[TransferDisk.SaveFileState.Failed]
      finally os.perms.set(copy, "rwx------")
    finally os.remove.all(dev)
