package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.server.{DurableFiles, Host, StorageDeviceConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, CrlLocked}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}

/** The device CRL's open on the device lane (device I/O isolation, Task 10): one CrlOpen job per load
 *  (DeviceCrlDisk.open, recovery and the whole lost-CRL path), its result applied on the control thread after
 *  discovery's recheck. Lockstep: a path held at its CrlOpen keeps the job in `heldJobs` until released. */
class DeviceCrlIoSuite extends IntegrationTestSuite with ControlFixture:

  /** A no-op CRL recording its shutdown */
  private final class Crl(val devicePath: os.Path) extends TestNetwork.TestCRL:
    val shut = new CountDownLatch(1)
    override def shutdown(): Unit = shut.countDown()

  /** TestNetwork.TestCRL's factory, recording each CRL it opens; `failOpen` decides, per open, an error to throw */
  private final class Factory(failOpen: os.Path => Option[Throwable] = _ => None) extends CrashRecoveryLogFactory:
    @volatile var opened: List[Crl] = Nil
    def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
      failOpen(devicePath).foreach(t => throw t)
      TestNetwork.TestCRL.openCRL(devicePath, config, holdsStores)
      val crl = new Crl(devicePath)
      synchronized { opened = opened :+ crl }
      crl
    def createCRL(devicePath: os.Path, config: StorageDeviceConfig, dir: os.Path): Unit = ()

  /** Holds `path` as soon as its CrlOpen job is submitted, so that job, and the path's later ones, wait */
  private def holdingCrlOpenOf(path: os.Path)(ec: ExecutionContext): LockstepThreading =
    new LockstepThreading(ec):
      override private[host] def enqueueDevice(p: os.Path, task: LockstepThreading.DeviceTask): Unit =
        if p == path && task.kind == JobKind.CrlOpen then hold(p)
        super.enqueueDevice(p, task)

  private def lockstep(host: Host): LockstepThreading = host.threading.asInstanceOf[LockstepThreading]

  private def queuedEvent(host: Host)(p: Host.Event => Boolean): Boolean =
    lockstep(host).queuedItems.exists:
      case t: Host.EventTask => p(t.event)
      case _ => false

  private def warningsAt(host: Host, path: os.Path): List[String] =
    recorded(host).deviceDirWarnings.toList.collect { case (p, m) if p == path => m }

  private def devicePath(hostRoot: os.Path, name: String): os.Path =
    hostRoot / "storage-devices" / name

  /** A device directory holding its CRL directory */
  private def device(hostRoot: os.Path, name: String, id: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, name, id)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  private def awaitShutdown(crl: Crl): Boolean = crl.shut.await(30, TimeUnit.SECONDS)

  atest("device CRL open waits on a held device and others proceed"):
    val hostRoot = newHostDir()
    val devA = devicePath(hostRoot, "dev0")
    val factory = new Factory
    val host = newHost(hostRoot, crlFactory = factory, threadingOf = holdingCrlOpenOf(devA))
    device(hostRoot, "dev0", deviceA)
    device(hostRoot, "dev1", deviceB)
    checkAllNow(host)

    heldJobs(host, devA) should be(List(JobKind.CrlOpen))
    loadedDevices(host).keySet should be(Set(deviceB))
    host.control.discovery.busyDeviceDirs should be(Set(devA))
    factory.opened.map(_.devicePath) should be(List(devicePath(hostRoot, "dev1")))

    // Later scans skip the path whose open is outstanding: the held lane holds one open, not one per scan
    checkAllNow(host)
    scanNow(host)
    heldJobs(host, devA) should be(List(JobKind.CrlOpen))

    releaseDevice(host, devA)
    runDiscovery(host)
    host.control.discovery.busyDeviceDirs should be(empty)
    loadedDevices(host).keySet should be(Set(deviceA, deviceB))
    Future.successful(loadedDevices(host)(deviceA).crl should be theSameInstanceAs factory.opened.last)

  atest("a startup path whose device CRL open is held starts the host, and the gate opens at the first ScanDevices"):
    val hostRoot = newHostDir()
    val devA = device(hostRoot, "dev0", deviceA)
    device(hostRoot, "dev1", deviceB)
    val host = newHost(hostRoot, keepCheckAllDevices = true, crlFactory = new Factory,
      threadingOf = holdingCrlOpenOf(devA))
    heldJobs(host, devA) should be(List(JobKind.CrlOpen))
    loadedDevices(host).keySet should be(Set(deviceB))
    host.control.discovery.startupScanDecided should be(false)
    queuedEvent(host)(_ == Host.CheckLoadedDevice(deviceB)) should be(true)
    queuedEvent(host)(_.isInstanceOf[Host.StoresReady]) should be(false)

    // The bounded rule: the first ScanDevices releases the gate while the open stays outstanding
    host.control.reconciler.scan()
    queuedEvent(host)(_.isInstanceOf[Host.StoresReady]) should be(true)
    host.testingOnlyHandleEvents()
    host.control.discovery.startupScanDecided should be(true)
    heldJobs(host, devA) should be(List(JobKind.CrlOpen))
    loadedDevices(host).keySet should be(Set(deviceB))

    releaseDevice(host, devA)
    runDiscovery(host)
    Future.successful(loadedDevices(host).keySet should be(Set(deviceA, deviceB)))

  /** A host with device A held at its CRL open, the open submitted; returns the host, A's path and the factory */
  private def heldAtCrlOpen(withStore: Boolean): (Host, os.Path, Factory) =
    val hostRoot = newHostDir()
    val devA = devicePath(hostRoot, "dev0")
    val factory = new Factory
    val host = newHost(hostRoot, crlFactory = factory, threadingOf = holdingCrlOpenOf(devA))
    if withStore then
      // Stores but no crl/: the open declares the CRL lost
      writeDevice(hostRoot, "dev0", deviceA)
      writeStoreConfig(devA, storeId)
    else
      device(hostRoot, "dev0", deviceA)
    checkAllNow(host)
    heldJobs(host, devA) should be(List(JobKind.CrlOpen))
    (host, devA, factory)

  /** Pulls the device at `devA` while its open is held, lists the devices without it, puts it back and releases the
   *  open, which runs over whatever is there: its result is for a path no longer listed */
  private def completeAfterPulled(host: Host, devA: os.Path): Unit =
    val pulled = devA / os.up / os.up / "pulled"
    os.move(devA, pulled)
    checkAllNow(host)
    os.move(pulled, devA)
    releaseDevice(host, devA)
    runDiscovery(host)

  atest("device CRL open completing after its path went away changes nothing and closes the CRL"):
    val (host, devA, factory) = heldAtCrlOpen(withStore = false)
    completeAfterPulled(host, devA)
    factory.opened.size should be(1)
    loadedDevices(host) should be(empty)
    host.control.copyRegistry.device(deviceA) should be(None)
    host.control.discovery.busyDeviceDirs should be(empty)
    warningsAt(host, devA) should be(Nil)
    // Shut down as an unload does: the next load waits for it rather than open the files twice
    host.control.devices.closingCrls should contain key devA
    Future.successful(awaitShutdown(factory.opened.head) should be(true))

  atest("a lost device CRL's open completing after its path went away closes the replacement and loads nothing"):
    val (host, devA, factory) = heldAtCrlOpen(withStore = true)
    completeAfterPulled(host, devA)
    // The job ran: its copy is in lost/ and the replacement CRL in place, but nothing of it is applied
    CopyFixtures.phaseOf(devA, storeId) should be(Some(Phase.Lost))
    os.isDir(CrashRecoveryLog.deviceCrlDir(devA)) should be(true)
    factory.opened.size should be(1)
    loadedDevices(host) should be(empty)
    host.control.copyRegistry.device(deviceA) should be(None)
    host.control.discovery.busyDeviceDirs should be(empty)
    recorded(host).storeLoadAttempts should be(empty)
    Future.successful(awaitShutdown(factory.opened.head) should be(true))

  atest("an opened device CRL is the loaded device's"):
    val (host, devA, factory) = heldAtCrlOpen(withStore = false)
    releaseDevice(host, devA)
    runDiscovery(host)
    loadedDevices(host).keySet should be(Set(deviceA))
    Future.successful(loadedDevices(host)(deviceA).crl should be theSameInstanceAs factory.opened.head)

  atest("a lost device CRL's open moves the copies to lost, replaces the CRL and loads the device"):
    val (host, devA, factory) = heldAtCrlOpen(withStore = true)
    releaseDevice(host, devA)
    runDiscovery(host)
    loadedDevices(host).keySet should be(Set(deviceA))
    CopyFixtures.phaseOf(devA, storeId) should be(Some(Phase.Lost))
    val sds = loadedDevices(host)(deviceA)
    sds.crl should be theSameInstanceAs factory.opened.head
    host.control.copyRegistry.device(deviceA).flatMap(_.copies.get(storeId)).map(_.phase) should be(Some(Phase.Lost))
    host.control.copyRegistry.unsyncedCopies(sds) should be(Nil)
    // A lost copy is never offered to the loader
    Future.successful(recorded(host).storeLoadAttempts should be(empty))

  atest("a lost device CRL whose move to lost lands unforced leaves the device unloaded, and the next load forces it"):
    val hostRoot = newHostDir()
    val factory = new Factory
    val host = newHost(hostRoot, crlFactory = factory)
    val failing = new java.util.concurrent.atomic.AtomicBoolean(true)
    host.control.moves.syncDirectory = dir =>
      if failing.get && dir.last == CopyLayout.LostDirectory then
        throw new java.io.IOException("sync failed")
      DurableFiles.syncDirectory(dir)
    val devA = writeDevice(hostRoot, "dev0", deviceA)
    writeStoreConfig(devA, storeId)
    checkAllNow(host)
    // The rename landed, unforced: held for the resync, and the CRL left as it was for the next load
    loadedDevices(host) should be(empty)
    factory.opened should be(Nil)
    CopyFixtures.phaseOf(devA, storeId) should be(Some(Phase.Lost))
    val lostPath = CopyFixtures.copyPath(devA, storeId).get
    host.control.copyRegistry.isUnsyncedAt(lostPath) should be(true)
    os.exists(CrashRecoveryLog.deviceCrlDir(devA)) should be(false)

    failing.set(false)
    checkAllNow(host)
    loadedDevices(host).keySet should be(Set(deviceA))
    val sds = loadedDevices(host)(deviceA)
    host.control.copyRegistry.isUnsyncedAt(lostPath) should be(false)
    Future.successful(host.control.copyRegistry.unsyncedCopies(sds) should be(Nil))

  atest("a device CRL another writer holds leaves the device unloaded, never lost"):
    val hostRoot = newHostDir()
    @volatile var locked = true
    val factory = new Factory(p => if locked then Some(CrlLocked(CrashRecoveryLog.deviceCrlDir(p))) else None)
    val host = newHost(hostRoot, crlFactory = factory)
    val devA = device(hostRoot, "dev0", deviceA)
    writeStoreConfig(devA, storeId)
    for _ <- 1 to DeviceDiscovery.CrlOpenAttempts + 1 do
      scanNow(host)
    loadedDevices(host) should be(empty)
    CopyFixtures.phaseOf(devA, storeId) should be(Some(Phase.Live))
    os.list(devA).exists(_.last.startsWith("crl.lost-")) should be(false)
    warningsAt(host, devA).size should be(1)

    locked = false
    scanNow(host)
    Future.successful(loadedDevices(host).keySet should be(Set(deviceA)))
