package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, RecordingRebuildFactory, StoreConfig}
import org.aspen_ddp.aspen.server.crl.simple.CrlLock
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.backend.{DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.store.backend.fsif.{DeviceJournalFiles, FsifDeviceJournal, FsifLayout, FsifRecovery,
  IndexFile, IndexMap, IndexMeta, JournalSizing, NioFsifFiles}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** The FSIF device journal at device load (spec 4.7) and its runtime failure (4.8, R16), on hosts that load their
 *  stores for real: a lost journal moves every FSIF copy to lost/ through the lost-CRL path's steps, a copy whose own
 *  replay fails is moved alone, a locked journal refuses the device, a device whose FSIF copies all arrived detached
 *  loads with no journal, and a journal failing at runtime takes the device out of service as a failed CRL does. */
class FsifJournalLostSuite extends IntegrationTestSuite with CopyHostFixture:

  private val layout = FsifLayout(1024, 64)
  private val s1 = StoreId(s.poolId, 1)
  private val s2 = StoreId(s.poolId, 2)
  private val s3 = StoreId(s.poolId, 3)

  private def fsifCfg(sid: StoreId): StoreConfig =
    StoreConfig(sid, StoreConfig.Fsif(layout.segmentPayloadSize, layout.segmentCount), None, ida)

  /** A fresh FSIF store of `sid`, Live on the device: its index detached, attached to no journal */
  private def fsifCopy(dev: os.Path, sid: StoreId): os.Path =
    val dir = CopyFixtures.freshCopyPath(dev, sid)
    DefaultBackendOpener.createStore(fsifCfg(sid), dir, dev / StoreCreation.CreatingDirectory)
    dir

  /** A fresh journal on the device, as its lazy creation leaves it (nothing written) */
  private def journal(dev: os.Path): JournalSizing =
    val sizing = JournalSizing(UUID.randomUUID(), 3, 1L << 20)
    DeviceJournalFiles.create(dev / DeviceJournalFiles.DirName, sizing)
    sizing

  private def journalId(dev: os.Path): UUID = DeviceJournalFiles.readSizing(dev / DeviceJournalFiles.DirName).journalId

  /** `copy`'s newest index made seq 1 with `meta` and `map`, as an open's attach index (or a detach) leaves it */
  private def writeIndex(copy: os.Path, meta: IndexMeta, map: IndexMap = IndexMap.empty(layout.segmentCount)): Unit =
    os.write.over(copy / FsifRecovery.IndexFiles(1), IndexFile.encode(1, meta, map))

  private def attached(journal: UUID, covered: Long): IndexMeta = IndexMeta(journal, UUID.randomUUID(), covered, false)

  private def newestMeta(copy: os.Path): IndexMeta = FsifRecovery.newestIndex(new NioFsifFiles(copy), layout)._3

  private def lostAside(dev: os.Path): Seq[os.Path] =
    os.list(dev).filter(_.last.startsWith("fsif-journal.lost-"))

  private def active(stores: StoreId*): Map[StoreId, StorageDeviceState.StoreEntry] =
    stores.map(_ -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)).toMap

  atest("a corrupt journal moves every FSIF copy to lost/, leaves RocksDB copies, keeps fsif-journal.lost-*, and the reconcile rebuilds them"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    val old = journal(dev)
    // Attached with a covered serial while no file header is valid: every header rotted (JournalCorrupt)
    val attachedCopy = fsifCopy(dev, s)
    writeIndex(attachedCopy, attached(old.journalId, covered = 5))
    val detachedCopy = fsifCopy(dev, s2)
    val rocks = rocksCopy(dev, s1)
    // An FSIF rebuild staging directory: its rebuild starts over
    val staging = dev / RebuildingStore.RebuildDirectory / s3.directoryName
    DefaultBackendOpener.createStore(fsifCfg(s3), staging, dev / StoreCreation.CreatingDirectory)
    val rebuilds = new RecordingRebuildFactory
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = active(s, s1, s2)))
      _ <- waitForTransactionsToComplete()
      host = realHost(hostRoot, new TestOpener, rebuilds = rebuilds)
      _ <- pump(host, host.control.router.loaded.contains(s1))
      phases = List(s, s2, s1).map(CopyFixtures.phaseOf(dev, _))
      aside = lostAside(dev)
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ <- pumpTx(host, rebuilds.created.map(_.storeId).toSet == Set(s, s2))
    yield
      phases should be(List(Some(Phase.Lost), Some(Phase.Lost), Some(Phase.Live)))
      CopyFixtures.copyPath(dev, s1) should be(Some(rocks))
      os.exists(attachedCopy) should be(false)
      os.exists(detachedCopy) should be(false)
      os.exists(staging) should be(false)
      aside.size should be(1)
      DeviceJournalFiles.readSizing(aside.head).journalId should be(old.journalId)
      journalId(dev) should not be old.journalId
      loadedDevices(host).keySet should be(Set(deviceA))
      rebuilds.created.map(_.storeId).toSet should be(Set(s, s2))

  atest("a device with no journal whose FSIF copy is attached to one (J2) moves its FSIF copies to lost/"):
    val (hostRoot, dev) = device()
    val copy = fsifCopy(dev, s)
    writeIndex(copy, attached(UUID.randomUUID(), covered = -1))
    val host = realHost(hostRoot, new TestOpener)
    host.testingOnlyHandleEvents()
    Future.successful:
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
      lostAside(dev) should be(Nil)
      os.exists(dev / DeviceJournalFiles.DirName) should be(true)
      loadedDevices(host).keySet should be(Set(deviceA))

  atest("a move to lost/ that fails leaves the device unloaded and the journal untouched; the next scan completes it"):
    val (hostRoot, dev) = device()
    val old = journal(dev)
    val copy = fsifCopy(dev, s)
    writeIndex(copy, attached(old.journalId, covered = 5))
    @volatile var failLostRenames = 1
    val rename: (os.Path, os.Path) => Unit = (from, to) =>
      if (to / os.up).last == CopyLayout.LostDirectory && failLostRenames > 0 then
        failLostRenames -= 1
        throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    val host = realHost(hostRoot, new TestOpener, rename = Some(rename))
    host.testingOnlyHandleEvents()
    val loadedFirst = loadedDevices(host).contains(deviceA)
    val phaseFirst = CopyFixtures.phaseOf(dev, s)
    val idFirst = journalId(dev)
    val asideFirst = lostAside(dev)
    given ExecutionContext = executionContext
    // The failed load shut the CRL it opened: the next scan loads the path once that has ended
    yieldUntil(host.control.devices.closingCrls.isEmpty).map: _ =>
      discoverNow(host)
      host.testingOnlyHandleEvents()
      failLostRenames should be(0)
      loadedFirst should be(false)
      phaseFirst should be(Some(Phase.Live))
      idFirst should be(old.journalId)
      asideFirst should be(Nil)
      loadedDevices(host).contains(deviceA) should be(true)
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
      lostAside(dev).size should be(1)
      journalId(dev) should not be old.journalId

  atest("one copy's replay failure moves only that copy to lost/"):
    val (hostRoot, dev) = device()
    val sizing = journal(dev)
    // Its index maps an ext to a slot past the end of its (empty) data file: the replay's slot check finds it lost
    val broken = fsifCopy(dev, s)
    val map = IndexMap.empty(layout.segmentCount)
    map.slots(0) = 0
    map.ids(0) = ObjectId(new UUID(7, 7))
    writeIndex(broken, attached(sizing.journalId, covered = -1), map)
    val fine = fsifCopy(dev, s2)
    writeIndex(fine, attached(sizing.journalId, covered = -1))
    val host = realHost(hostRoot, new TestOpener)
    given ExecutionContext = executionContext
    pump(host, host.control.router.loaded.contains(s2)).map: _ =>
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
      CopyFixtures.copyPath(dev, s2) should be(Some(fine))
      // Replayed and detached, then attached afresh by its open
      newestMeta(fine).journalId should be(sizing.journalId)
      host.control.router.loaded.keySet should contain(s2)
      journalId(dev) should be(sizing.journalId)
      lostAside(dev) should be(Nil)
      loadedDevices(host).keySet should be(Set(deviceA))

  atest("a locked journal refuses the device and moves nothing"):
    val (hostRoot, dev) = device()
    val sizing = journal(dev)
    val copy = fsifCopy(dev, s)
    writeIndex(copy, attached(sizing.journalId, covered = -1))
    val lock = CrlLock.acquire(dev / DeviceJournalFiles.DirName)
    val host =
      try
        val h = realHost(hostRoot, new TestOpener)
        h.testingOnlyHandleEvents()
        h
      finally lock.release()
    val loadedFirst = loadedDevices(host).contains(deviceA)
    val phaseFirst = CopyFixtures.phaseOf(dev, s)
    val metaFirst = newestMeta(copy)
    given ExecutionContext = executionContext
    yieldUntil(host.control.devices.closingCrls.isEmpty).map: _ =>
      discoverNow(host)
      host.testingOnlyHandleEvents()
      loadedFirst should be(false)
      phaseFirst should be(Some(Phase.Live))
      metaFirst.detached should be(false)
      lostAside(dev) should be(Nil)
      journalId(dev) should be(sizing.journalId)
      // Once the lock is free, the next scan opens and replays it
      loadedDevices(host).contains(deviceA) should be(true)
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Live))
      // Replayed: its newest index names the journal, under a new attachment once its open attached it
      newestMeta(copy).attachment should not be metaFirst.attachment

  atest("a copy whose config cannot be stat'd leaves a device with a journal unloaded, and it loads once it can be"):
    val (hostRoot, dev) = device()
    val sizing = journal(dev)
    val copy = fsifCopy(dev, s)
    writeIndex(copy, attached(sizing.journalId, covered = -1))
    // No search permission: its config's stat fails, so whether it is attached to the journal is unknown
    os.perms.set(copy, "rw-------")
    val host =
      try
        val h = realHost(hostRoot, new TestOpener)
        h.testingOnlyHandleEvents()
        h
      finally os.perms.set(copy, "rwx------")
    val loadedFirst = loadedDevices(host).contains(deviceA)
    val metaFirst = newestMeta(copy)
    given ExecutionContext = executionContext
    yieldUntil(host.control.devices.closingCrls.isEmpty).map: _ =>
      discoverNow(host)
      host.testingOnlyHandleEvents()
      loadedFirst should be(false)
      metaFirst.detached should be(false)
      loadedDevices(host).contains(deviceA) should be(true)
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Live))
      lostAside(dev) should be(Nil)

  atest("Review Focus 2: a device whose FSIF copies all came from another host loads Absent"):
    val (hostRoot, dev) = device()
    val foreign = UUID.randomUUID()
    // Transferred in: each detached by its close on the source, naming the source device's journal
    val a = fsifCopy(dev, s)
    writeIndex(a, IndexMeta(foreign, UUID.randomUUID(), 7, detached = true))
    val b = fsifCopy(dev, s2)
    writeIndex(b, IndexMeta(foreign, UUID.randomUUID(), 3, detached = true))
    val host = realHost(hostRoot, new TestOpener)
    host.testingOnlyHandleEvents()
    // The load opened nothing: no journal before the stores' opens
    val slotAtLoad = loadedDevices(host).get(deviceA).map(_.io.fsifJournal.get)
    given ExecutionContext = executionContext
    pump(host, host.control.router.loaded.contains(s) && host.control.router.loaded.contains(s2)).map: _ =>
      slotAtLoad.isDefined should be(true)
      CopyFixtures.copyPath(dev, s) should be(Some(a))
      CopyFixtures.copyPath(dev, s2) should be(Some(b))
      lostAside(dev) should be(Nil)
      // The first FSIF open made the device's journal, a fresh one
      journalId(dev) should not be foreign
      host.control.router.loaded.keySet should contain allOf(s, s2)

  atest("an FSIF journal failing at runtime fail-stops its stores and takes the device out of service as a failed CRL does"):
    val (hostRoot, dev) = device()
    fsifCopy(dev, s)
    val rocks = rocksCopy(dev, s1)
    val host = realHost(hostRoot, new TestOpener)
    given ExecutionContext = executionContext
    for
      _ <- pump(host, host.control.router.loaded.contains(s) && host.control.router.loaded.contains(s1))
      sds = loadedDevices(host)(deviceA)
      journal = sds.io.fsifJournal.get.get
      // The ring's first file cannot be opened for writing: the journal's next write fails
      _ = os.remove(dev / DeviceJournalFiles.DirName / "0.fj")
      _ = os.makeDir(dev / DeviceJournalFiles.DirName / "0.fj")
      probe = UUID.randomUUID()
      _ = journal.attach(probe, dev / "probe", new FsifDeviceJournal.Owner:
            def complete(task: Runnable): Unit = task.run()
            def roomAvailable(): Unit = ()
            def checkpointRequested(): Unit = ()
            def journalFailed(t: Throwable): Unit = ())
      _ = journal.append(probe, Nil)
      _ <- pump(host, journal.failed.isDefined && !loadedDevices(host).contains(deviceA) &&
                      CopyFixtures.phaseOf(dev, s).contains(Phase.Lost))
    yield
      journal.failed should not be empty
      loadedDevices(host).contains(deviceA) should be(false)
      host.control.discovery.disownedDeviceDirs should contain(dev)
      // The FSIF store took its own fail-stop route; the RocksDB copy is left where it is, for the restart
      CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
      CopyFixtures.copyPath(dev, s1) should be(Some(rocks))
      host.control.router.loaded.keySet should not contain s1
      sds.io.fsifJournal.get should be(None)
