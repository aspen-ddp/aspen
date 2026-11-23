package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.server.{DeviceMarkers, Host, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.store.backend.{DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Device discovery on the device lanes (device I/O isolation, Task 9): the listing on the host root's lane, one probe
 *  job per path on its own DeviceIo, the load's sweep and P2 jobs, and the results applied on the control thread after
 *  a recheck. Lockstep: every job runs when the test drains it, and a held path's jobs wait in `heldJobs`. */
class DiscoveryIoSuite extends IntegrationTestSuite with ControlFixture:

  private def lockstep(host: Host): LockstepThreading = host.threading.asInstanceOf[LockstepThreading]

  /** Runs the first queued management job of `kind` (it posts its result), leaving everything else queued */
  private def runJob(host: Host, kind: JobKind): Unit =
    val jobs = lockstep(host).removeQueued:
      case t: LockstepThreading.DeviceTask => t.kind == kind
      case _ => false
    jobs.size should be(1)
    jobs.foreach(_.run())

  /** Applies the queued job results (Host.DeviceJobDone), leaving everything else queued */
  private def applyResults(host: Host): Unit =
    lockstep(host).removeQueued:
      case t: Host.EventTask => t.event.isInstanceOf[Host.DeviceJobDone]
      case _ => false
    .foreach(_.run())

  private def warningsAt(host: Host, path: os.Path): List[String] =
    recorded(host).deviceDirWarnings.toList.collect { case (p, m) if p == path => m }

  /** A device directory holding its CRL directory, so the load does not take its CRL for lost */
  private def device(hostRoot: os.Path, name: String, id: org.aspen_ddp.aspen.common.metadata.StorageDeviceId,
                     owner: Option[HostId] = Some(HostId.BootstrapHostId)): os.Path =
    val dev = writeDevice(hostRoot, name, id, owner = owner)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  atest("device probe waits on a held device and others proceed"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val devA = device(hostRoot, "dev0", deviceA)
    val devB = device(hostRoot, "dev1", deviceB)
    holdDevice(host, devA)
    checkAllNow(host)

    heldJobs(host, devA) should be(List(JobKind.Markers))
    loadedDevices(host).keySet should be(Set(deviceB))
    host.control.discovery.busyDeviceDirs should be(Set(devA))

    // Later scans skip the path whose probe is outstanding: the held lane holds one probe, not one per scan
    checkAllNow(host)
    scanNow(host)
    heldJobs(host, devA) should be(List(JobKind.Markers))

    releaseDevice(host, devA)
    runDiscovery(host)
    host.control.discovery.busyDeviceDirs should be(empty)
    Future.successful(loadedDevices(host).keySet should be(Set(deviceA, deviceB)))

  atest("startup scan with one path held starts the host and loads the other device"):
    given ExecutionContext = executionContext
    val hostRoot = newHostDir()
    val devA = device(hostRoot, "dev0", deviceA)
    val devB = device(hostRoot, "dev1", deviceB)
    val storeB = CopyFixtures.freshCopyPath(devB, storeId)
    DefaultBackendOpener.createStore(StoreConfig(storeId, StoreConfig.RocksDB(None), None, Replication(3, 2)),
      storeB, devB / StoreCreation.CreatingDirectory)
    var threading: LockstepThreading = null
    // Held before the Host exists, so its constructor's probe of A never returns
    val host = newHost(hostRoot, keepCheckAllDevices = true, adjustSeams = _.copy(tryLoadStore = None),
      threadingOf = ec =>
        threading = new LockstepThreading(ec)
        threading.hold(devA)
        threading)

    // The constructor returned, and B is loaded while A's probe waits
    heldJobs(host, devA) should be(List(JobKind.Markers))
    loadedDevices(host).keySet should be(Set(deviceB))
    host.control.discovery.startupScanDecided should be(false)
    // Not ready to answer for stores it does not know: A may hold any of them
    lockstep(host).queuedItems.exists:
      case t: Host.EventTask => t.event.isInstanceOf[Host.StoresReady]
      case _ => false
    .should(be(false))

    host.testingOnlyHandleEvents() // B's store opens and loads
    host.getStoreIds should contain(storeId)

    releaseDevice(host, devA)
    host.testingOnlyHandleEvents()
    host.control.discovery.startupScanDecided should be(true)
    Future.successful(loadedDevices(host).keySet should be(Set(deviceA, deviceB)))

  atest("device probe completing after its path went away changes nothing"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val devA = device(hostRoot, "dev0", deviceA)
    holdDevice(host, devA)
    checkAllNow(host)
    heldJobs(host, devA) should be(List(JobKind.Markers))
    val io = host.control.deviceIos.get(devA)
    io should not be empty

    // The disk is pulled while its probe is outstanding: the next listing no longer names it, and its DeviceIo is kept
    // while the probe is outstanding
    os.move(devA, hostRoot / "pulled")
    checkAllNow(host)
    host.control.deviceIos.get(devA) should be(io)

    // The probe reads the directory back (the job runs over whatever is there): its result is for a path no longer
    // listed, so nothing is loaded
    os.move(hostRoot / "pulled", devA)
    releaseDevice(host, devA)
    runDiscovery(host)
    loadedDevices(host) should be(empty)
    host.control.discovery.busyDeviceDirs should be(empty)
    warningsAt(host, devA) should be(Nil)

    // With nothing outstanding, a listing that still does not name it closes its DeviceIo
    os.move(devA, hostRoot / "pulled")
    checkAllNow(host)
    host.control.deviceIos.get(devA) should be(None)
    Future.successful(loadedDevices(host) should be(empty))

  atest("device load sweep completing after its path went away changes nothing"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val devA = device(hostRoot, "dev0", deviceA)
    holdDevice(host, devA)
    host.control.reconciler.checkAll()
    runJob(host, JobKind.Scan)
    applyResults(host) // submits the probe, held
    releaseDevice(host, devA)
    runJob(host, JobKind.Markers)
    holdDevice(host, devA)
    applyResults(host) // the probe says ours: the load's sweep is submitted, held
    heldJobs(host, devA) should be(List(JobKind.Sweep))

    os.move(devA, hostRoot / "pulled")
    checkAllNow(host)
    os.move(hostRoot / "pulled", devA)
    releaseDevice(host, devA)
    runDiscovery(host)
    loadedDevices(host) should be(empty)
    host.control.copyRegistry.device(deviceA) should be(None)
    Future.successful(host.control.discovery.busyDeviceDirs should be(empty))

  atest("device load P2 completing after its path went away shuts the opened CRL and loads nothing"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val devA = device(hostRoot, "dev0", deviceA)
    holdDevice(host, devA)
    host.control.reconciler.checkAll()
    runJob(host, JobKind.Scan)
    applyResults(host)
    releaseDevice(host, devA)
    runJob(host, JobKind.Markers)
    applyResults(host)
    runJob(host, JobKind.Sweep)
    holdDevice(host, devA)
    applyResults(host) // the CRL opened on the control thread, then P2 submitted, held
    heldJobs(host, devA) should be(List(JobKind.Move))

    os.move(devA, hostRoot / "pulled")
    checkAllNow(host)
    os.move(hostRoot / "pulled", devA)
    releaseDevice(host, devA)
    runDiscovery(host)
    loadedDevices(host) should be(empty)
    host.control.copyRegistry.device(deviceA) should be(None)
    // Shut down as an unload does: the next load waits for it rather than open the files twice
    host.control.devices.closingCrls should contain key devA
    Future.successful(host.control.discovery.busyDeviceDirs should be(empty))

  atest("a probe of a directory without a device config loads nothing and warns nothing"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dir = hostRoot / "storage-devices" / "empty"
    os.makeDir.all(dir)
    checkAllNow(host)
    loadedDevices(host) should be(empty)
    Future.successful(warningsAt(host, dir) should be(Nil))

  atest("a probe of a device carrying the failed marker loads nothing and warns once"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dev = device(hostRoot, "dev0", deviceA)
    os.write(dev / DeviceMarkers.FailedFile, "")
    checkAllNow(host)
    checkAllNow(host)
    loadedDevices(host) should be(empty)
    warningsAt(host, dev).size should be(1)
    Future.successful(warningsAt(host, dev).head should include("declared failed"))

  atest("a probe whose read fails loads nothing and is retried at the next scan"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dev = device(hostRoot, "dev0", deviceA)
    os.write.over(dev / StorageDeviceConfig.configFilename, "this is not: valid: device config yaml")
    checkAllNow(host)
    loadedDevices(host) should be(empty)
    warningsAt(host, dev).size should be(1)
    writeDeviceConfig(dev, deviceA)
    checkAllNow(host)
    Future.successful(loadedDevices(host).keySet should be(Set(deviceA)))

  atest("a probe of a device of another Aspen system loads nothing"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dev = writeDevice(hostRoot, "dev0", deviceA, UUID.randomUUID())
    checkAllNow(host)
    loadedDevices(host) should be(empty)
    Future.successful(warningsAt(host, dev).head should include("does not belong to this Aspen system"))

  atest("a probe of a device of ours loads it, sweeping its scratch areas, and checks it"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dev = device(hostRoot, "dev0", deviceA)
    val scratch = List(dev / StoreTransfer.StagingDirectory / "x", dev / StoreCreation.CreatingDirectory / "y",
      dev / StoreCopies.DeletingDirectory / "z", CrashRecoveryLog.replacementCrlDir(dev) / "w")
    scratch.foreach(os.makeDir.all(_))
    val live = writeStoreConfig(dev, storeId)
    checkAllNow(host)
    loadedDevices(host).keySet should be(Set(deviceA))
    scratch.map(_ / os.up).filter(os.exists(_)).filter(d => os.list(d).nonEmpty) should be(Nil)
    recorded(host).storeLoadAttempts.toList should be(List((deviceA, live)))
    // Its check began at its load
    Future.successful(recorded(host).lookupAttempts.toList should contain(deviceA))

  atest("a probe of a device carrying shutdown.state hands it to the ownership decision"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dev = device(hostRoot, "dev0", deviceA)
    DeviceMarkers.writeShutdownState(dev, HostId.BootstrapHostId)
    recorded(host).armLookup(deviceA)
    checkAllNow(host)
    loadedDevices(host) should be(empty)
    Future.successful(host.control.discovery.pendingMarkerResolutions should be(Set(dev)))

  atest("a probe of a device another host owns hands it to the ownership decision"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dev = device(hostRoot, "dev0", deviceA, owner = Some(HostId(new UUID(7, 7))))
    recorded(host).armLookup(deviceA)
    checkAllNow(host)
    loadedDevices(host) should be(empty)
    recorded(host).lookupAttempts.toList should be(List(deviceA))
    Future.successful(host.control.discovery.pendingMarkerResolutions should be(Set(dev)))

  atest("a probe of a device without an owning-host file loads nothing"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)
    val dev = device(hostRoot, "dev0", deviceA, owner = None)
    checkAllNow(host)
    loadedDevices(host) should be(empty)
    Future.successful(warningsAt(host, dev).head should include(DeviceMarkers.OwningHostFile))

  atest("a DeviceIo is forgotten only once nothing is queued or running on it"):
    val threading = new LockstepThreading(ExecutionContext.parasitic)
    val root = os.temp.dir(deleteOnExit = true)
    val ios = new DeviceIos(threading, root)
    val p = root / "dev"
    val io = ios.forPath(p)
    io.submit(JobKind.Scan, None)(())
    // Queued, not started: closing now would drop it
    ios.forget(p) should be(false)
    ios.get(p) should be(Some(io))
    threading.drain()
    // A data task blocked on the held path keeps it too
    threading.hold(p)
    val ran = new java.util.concurrent.CountDownLatch(1)
    io.dataEc.execute(() => ran.countDown())
    ios.forget(p) should be(false)
    threading.release(p)
    ran.await()
    val deadline = System.nanoTime() + 10_000_000_000L
    while !io.idle && System.nanoTime() < deadline do Thread.onSpinWait()
    ios.forget(p) should be(true)
    Future.successful(ios.get(p) should be(None))
