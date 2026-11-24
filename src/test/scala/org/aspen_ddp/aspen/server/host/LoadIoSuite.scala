package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StoreConfig}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.StoreIncarnationFile
import org.aspen_ddp.aspen.server.store.backend.Backend

import scala.concurrent.{ExecutionContext, Future}

/** Store loads read their inputs on the device lane (device I/O isolation, Task 11): tryLoadStore's config read is
 *  one `ReadConfig` job, and a LoadStore's reads (save file, incarnation, rebuilt gates, arrived-at) one `LoadInputs`
 *  job, each applied on the control thread after a recheck. The copy is Opening throughout. Lockstep, real RocksDB
 *  stores loaded through the host's own loader (CopyHostFixture.realHost). */
class LoadIoSuite extends IntegrationTestSuite with CopyHostFixture:

  private val s2 = StoreId(s.poolId, 1.toByte)

  /** Holds `path` as soon as the first job of `kind` is submitted on it, so that job, and the path's later ones, wait
   *  until released */
  private def holdingAt(path: os.Path, kind: JobKind)(ec: ExecutionContext): LockstepThreading =
    val first = new java.util.concurrent.atomic.AtomicBoolean(true)
    new LockstepThreading(ec):
      override private[host] def enqueueDevice(p: os.Path, task: LockstepThreading.DeviceTask): Unit =
        if p == path && task.kind == kind && first.getAndSet(false) then hold(p)
        super.enqueueDevice(p, task)

  /** Devices A and B (dev0, dev1), each holding one RocksDB store, `s` on A and `s2` on B; returns the host root and
   *  both device paths */
  private def twoDevices(): (os.Path, os.Path, os.Path) =
    val (hostRoot, devA) = device()
    val devB = writeDevice(hostRoot, "dev1", deviceB)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devB))
    rocksCopy(devA, s)
    rocksCopy(devB, s2)
    (hostRoot, devA, devB)

  private def opening(host: Host, sid: StoreId): Boolean = host.control.loader.isOpening(sid)

  private def served(host: Host, sid: StoreId): Boolean = host.control.router.loaded.contains(sid)

  private def activityOf(host: Host, sid: StoreId): Option[Activity] =
    loadedDevices(host).get(deviceA).flatMap(recordOf(host, _, sid)).map(_.activity)

  private def close(host: Host): Future[Unit] =
    host.control.router.loaded.get(s).map(_.backend.close()).getOrElse(Future.unit)

  for (kind, what) <- List((JobKind.ReadConfig, "config read"), (JobKind.LoadInputs, "LoadStore's inputs read")) do
    atest(s"a store load's $what waits on a held device and others proceed"):
      given ExecutionContext = executionContext
      val (hostRoot, devA, _) = twoDevices()
      val opener = new TestOpener
      val host = realHost(hostRoot, opener, threadingOf = holdingAt(devA, kind))
      pump(host, served(host, s2)).flatMap: _ =>
        heldJobs(host, devA) should be(List(kind))
        served(host, s2) should be(true)
        served(host, s) should be(false)
        // Busy and silent while its read waits: its copy is Opening, and a second offer starts nothing
        opening(host, s) should be(true)
        host.control.router.silentStores should contain(s)
        host.control.loader.tryLoadStore(loadedDevices(host)(deviceA), CopyFixtures.copyPath(devA, s).get)
        heldJobs(host, devA) should be(List(kind))
        releaseDevice(host, devA)
        pump(host, served(host, s)).flatMap: _ =>
          served(host, s) should be(true)
          opening(host, s) should be(false)
          heldJobs(host, devA) should be(Nil)
          close(host).map(_ => succeed)

  for (kind, what) <- List((JobKind.ReadConfig, "config read"), (JobKind.LoadInputs, "LoadStore's inputs read")) do
    atest(s"a store load's $what completing after its device's unload changes nothing and leaks nothing"):
      given ExecutionContext = executionContext
      val (hostRoot, devA, _) = twoDevices()
      val opener = new TestOpener
      val host = realHost(hostRoot, opener, threadingOf = holdingAt(devA, kind))
      pump(host, served(host, s2)).flatMap: _ =>
        heldJobs(host, devA) should be(List(kind))
        val opensBefore = opener.opens.get
        host.control.shutdown.unloadDevice(deviceA)
        loadedDevices(host).keySet should be(Set(deviceB))
        releaseDevice(host, devA)
        // The job runs over the unloaded device's files; its apply changes nothing
        pump(host, heldJobs(host, devA).isEmpty && !host.control.rebuilds.closesRunning(deviceA), rounds = 50)
          .flatMap: _ =>
            loadedDevices(host).keySet should be(Set(deviceB))
            served(host, s) should be(false)
            opening(host, s) should be(false)
            host.control.rebuilds.closesRunning(deviceA) should be(false)
            host.control.copyRegistry.retryOf(s) should be(None)
            // The config read's apply opens nothing; an opened backend is closed unserved
            if kind == JobKind.ReadConfig then
              opener.opens.get should be(opensBefore)
            // Nothing leaked: the device loads again, its store served from the same directory (a RocksDB lock
            // left held by an unclosed backend would fail the open), once its CRL's shutdown has ended
            pump(host, !host.control.devices.closingCrls.contains(devA)).flatMap: _ =>
              checkAllNow(host)
              pump(host, served(host, s)).flatMap: _ =>
                served(host, s) should be(true)
                close(host).map(_ => succeed)

  /** A host loading device A, whose only store `s` is a RocksDB copy that `prepare` adjusts first; pumps until
   *  `until` holds. Returns the host and the device path */
  private def loadWith(prepare: os.Path => Unit, until: Host => Boolean): Future[(Host, os.Path)] =
    given ExecutionContext = executionContext
    val (hostRoot, devA) = device()
    val dir = rocksCopy(devA, s)
    prepare(dir)
    val host = realHost(hostRoot, new TestOpener)
    pump(host, until(host)).map(_ => (host, devA))

  private def awaitingRetry(host: Host): Boolean = host.control.loader.awaitingRetryReady(s).contains(true)

  /** A link at `at` into a directory lacking search permission: every stat of `at` fails */
  private def unstatable(at: os.Path): os.Path =
    val blocked = at / os.up / os.up / os.up / s"blocked-${at.last}"
    os.makeDir.all(blocked)
    os.perms.set(blocked, "rw-------")
    os.symlink(at, blocked / "file")
    blocked

  // ---- tryLoadStore's config read (ConfigRead) ----

  atest("a config read finding the config opens the store"):
    given ExecutionContext = executionContext
    loadWith(_ => (), served(_, s)).flatMap: (host, _) =>
      served(host, s) should be(true)
      close(host).map(_ => succeed)

  atest("a config read finding no config ends the open: the copy Idle, nothing opened, no retry"):
    val opener = new TestOpener
    val (hostRoot, devA) = device()
    val dir = rocksCopy(devA, s)
    os.remove(dir / StoreConfig.configFilename)
    val host = realHost(hostRoot, opener)
    pump(host, false, rounds = 20).map: _ =>
      served(host, s) should be(false)
      opening(host, s) should be(false)
      opener.opens.get should be(0)
      activityOf(host, s) should be(Some(Activity.Idle))
      host.control.copyRegistry.retryOf(s) should be(None)

  atest("a config read that fails leaves the store offline awaiting a retry, which loads it once it reads"):
    given ExecutionContext = executionContext
    var cfg = Array.emptyByteArray
    loadWith(dir =>
      cfg = os.read.bytes(dir / StoreConfig.configFilename)
      os.write.over(dir / StoreConfig.configFilename, "not: [a store config"), awaitingRetry).flatMap: (host, devA) =>
      served(host, s) should be(false)
      awaitingRetry(host) should be(true)
      os.write.over(CopyFixtures.copyPath(devA, s).get / StoreConfig.configFilename, cfg)
      scanNow(host)
      pump(host, served(host, s)).flatMap: _ =>
        served(host, s) should be(true)
        close(host).map(_ => succeed)

  atest("a config read finding another store's config loads nothing"):
    val opener = new TestOpener
    val (hostRoot, devA) = device()
    val dir = rocksCopy(devA, s)
    os.write.over(dir / StoreConfig.configFilename, StoreConfig(s2, StoreConfig.RocksDB(None), None, ida).yamlConfig)
    StoreConfig.loadStoreConfig(dir / StoreConfig.configFilename).storeId should be(s2)
    val host = realHost(hostRoot, opener)
    pump(host, false, rounds = 20).map: _ =>
      served(host, s) should be(false)
      served(host, s2) should be(false)
      opening(host, s) should be(false)
      opener.opens.get should be(0)
      activityOf(host, s) should be(Some(Activity.Idle))

  // ---- a LoadStore's inputs (LoadInputs) ----

  atest("a LoadStore whose save file cannot be stat'd is left offline awaiting a retry"):
    given ExecutionContext = executionContext
    var blocked = os.root
    loadWith(dir => blocked = unstatable(Backend.crlSaveFile(dir)), awaitingRetry).map: (host, devA) =>
      os.perms.set(blocked, "rwx------")
      served(host, s) should be(false)
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))

  atest("a LoadStore whose save file is unusable moves the store to lost/"):
    loadWith(dir => os.write(Backend.crlSaveFile(dir), "not a crl state"),
      h => loadedDevices(h).get(deviceA).flatMap(recordOf(h, _, s)).exists(_.phase == Phase.Lost)).map: (host, devA) =>
      served(host, s) should be(false)
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Lost))

  atest("a LoadStore whose incarnation file is missing moves the store to lost/"):
    loadWith(dir => os.remove(dir / StoreIncarnationFile.FileName),
      h => loadedDevices(h).get(deviceA).flatMap(recordOf(h, _, s)).exists(_.phase == Phase.Lost)).map: (host, devA) =>
      served(host, s) should be(false)
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Lost))

  atest("a LoadStore whose incarnation file cannot be parsed is left offline awaiting a retry"):
    loadWith(dir => os.write.over(dir / StoreIncarnationFile.FileName, "not a uuid"), awaitingRetry).map: (host, devA) =>
      served(host, s) should be(false)
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))

  atest("a LoadStore whose acceptor-since file cannot be stat'd is left offline awaiting a retry"):
    var blocked = os.root
    loadWith(dir => blocked = unstatable(dir / RebuildingStore.AcceptorSinceFile), awaitingRetry).map: (host, devA) =>
      os.perms.set(blocked, "rwx------")
      served(host, s) should be(false)
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))

  atest("a LoadStore whose inherited-votes file cannot be stat'd is left offline awaiting a retry"):
    var blocked = os.root
    loadWith(dir => blocked = unstatable(dir / RebuildingStore.InheritedVotesFile), awaitingRetry).map: (host, devA) =>
      os.perms.set(blocked, "rwx------")
      served(host, s) should be(false)
      CopyFixtures.phaseOf(devA, s) should be(Some(Phase.Live))

  atest("a LoadStore whose files all read serves the store with its rebuilt gates"):
    given ExecutionContext = executionContext
    loadWith(dir =>
      LoadDisk.writeArrivedAt(dir)
      RebuildingStore.writeAcceptorSince(dir)
      RebuildingStore.writeInheritedVotes(dir, System.currentTimeMillis()), served(_, s)).flatMap: (host, _) =>
      val store = host.control.router.loaded(s)
      store.frontend.acceptorSince should be(defined)
      store.frontend.inheritedVotesAfter should be(defined)
      close(host).map(_ => succeed)
