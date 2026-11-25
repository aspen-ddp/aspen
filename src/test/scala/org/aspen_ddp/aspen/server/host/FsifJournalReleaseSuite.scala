package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.{DurableFiles, Host, RecordingRebuildFactory, StoreConfig}
import org.aspen_ddp.aspen.server.store.backend.{CommitState, DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.store.backend.fsif.{DeviceJournalFiles, DeviceJournalFormat, FsifBackend,
  FsifDeviceJournal, FsifLayout, FsifRecovery, IndexFile, IndexMap, IndexMeta, JournalRecord, JournalSizing,
  NioFsifFiles}

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Success, Try}

/** The release of FSIF device-journal attachments with their copies (spec 4.5 "released"), and the transfer out's
 *  detach check (4.6), on hosts that load their stores for real: a fail-stopped copy's attachment pins the ring until
 *  its move out of replay's scope is durable, and a copy whose close did not detach never ships. */
class FsifJournalReleaseSuite extends IntegrationTestSuite with CopyHostFixture:

  private val layout = FsifLayout(1024, 64)
  private val s2 = StoreId(s.poolId, 2)

  private def fsifCfg(sid: StoreId): StoreConfig =
    StoreConfig(sid, StoreConfig.Fsif(layout.segmentPayloadSize, layout.segmentCount), None, ida)

  /** A fresh FSIF store of `sid`, Live on the device */
  private def fsifCopy(dev: os.Path, sid: StoreId): os.Path =
    val dir = CopyFixtures.freshCopyPath(dev, sid)
    DefaultBackendOpener.createStore(fsifCfg(sid), dir, dev / StoreCreation.CreatingDirectory)
    dir

  /** A small journal on the device (3 files of 16 blocks), so a few appends fill its ring */
  private def journal(dev: os.Path): JournalSizing =
    val sizing = JournalSizing(UUID.randomUUID(), 3, 64 * 1024)
    DeviceJournalFiles.create(dev / DeviceJournalFiles.DirName, sizing)
    sizing

  private def newestMeta(copy: os.Path): IndexMeta = FsifRecovery.newestIndex(new NioFsifFiles(copy), layout)._3

  private def active(stores: StoreId*): Map[StoreId, StorageDeviceState.StoreEntry] =
    stores.map(_ -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)).toMap

  private def backendOf(host: Host, sid: StoreId): FsifBackend =
    host.control.router.loaded(sid).backend.asInstanceOf[FsifBackend]

  /** A repair allocating a 512-byte object in ext `x` of `sid`'s store: one Delta appended to the device journal */
  private def repair(host: Host, sid: StoreId, x: Int): Promise[Unit] =
    val id = ObjectId(new UUID(x, sid.poolIndex))
    val pointer = DataObjectPointer(id, sid.poolId, ByteBuffer.allocate(4).putInt(x).array())
    val md = Metadata(ObjectRevision(TransactionId(new UUID(x, 6))), ObjectRefcount(1, 1), HLCTimestamp(1))
    val p = Promise[Unit]()
    backendOf(host, sid).repair(CommitState(pointer, md, ObjectType.Data, DataBuffer(new Array[Byte](512)),
      allocating = true), p)
    p

  private def recordOn(host: Host, sid: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.copiesOf(loadedDevices(host)(deviceA)).flatMap(_.copies.get(sid))

  /** Device A in the tree, its entries Active for `stores`, so the host's device checks read them */
  private def deviceInTree(stores: StoreId*): Future[Unit] =
    given ExecutionContext = executionContext
    for
      _ <- net.createDevice(deviceA)
      _ <- setDeviceState(deviceA)(_.copy(stores = active(stores*)))
      _ <- waitForTransactionsToComplete()
    yield ()

  atest("a fail-stopped FSIF copy pins the ring until its move to lost/ is durable"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    journal(dev)
    val copy = fsifCopy(dev, s)
    val lostDir = dev / CopyLayout.LostDirectory
    // Every force of lost/ fails while set: the move to lost/ lands unforced, and resyncs fail too
    @volatile var failLostSync = false
    for
      _ <- deviceInTree(s)
      host = realHost(hostRoot, new TestOpener, rebuilds = new RecordingRebuildFactory)
      _ = host.control.moves.syncDirectory = d =>
            if d == lostDir && failLostSync then throw new java.io.IOException("injected sync")
            DurableFiles.syncDirectory(d)
      _ <- pump(host, host.control.router.loaded.contains(s))
      journal = loadedDevices(host)(deviceA).io.fsifJournal.get.get
      written = repair(host, s, 0)
      _ <- pump(host, written.isCompleted)
      attachment = newestMeta(copy).attachment
      pinnedOpen = journal.needsReplay(attachment)
      _ = failLostSync = true
      _ = backendOf(host, s).failStopForTest(new java.io.IOException("injected fatal"))
      _ <- pumpTx(host, recordOn(host, s).exists(_.phase == Phase.Lost) &&
                        host.control.reconciler.activeDeviceChecks.isEmpty)
      unsyncedRecord = recordOn(host, s).map(r => (r.phase, r.unsynced))
      pinnedUnsynced = journal.needsReplay(attachment)
      // The next device check's resync forces it: durable, so released
      _ = failLostSync = false
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ <- pumpTx(host, !journal.needsReplay(attachment) && host.control.reconciler.activeDeviceChecks.isEmpty)
    yield
      written.future.value should be(Some(Success(())))
      pinnedOpen should be(true)
      unsyncedRecord should be(Some((Phase.Lost, true)))
      pinnedUnsynced should be(true)
      journal.needsReplay(attachment) should be(false)
      host.control.copyRegistry.isUnsynced(loadedDevices(host)(deviceA), s) should be(false)
      os.exists(copy) should be(false)

  atest("Review Focus 5: when that move fails, commits on the device hold; the device check's retry moves it and they resume"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    journal(dev)
    val copy = fsifCopy(dev, s)
    fsifCopy(dev, s2)
    // Every rename into lost/ fails while set
    @volatile var failLost = false
    val rename: (os.Path, os.Path) => Unit = (from, to) =>
      if (to / os.up).last == CopyLayout.LostDirectory && failLost then throw new java.io.IOException("injected rename")
      DurableFiles.atomicRename(from, to)
    for
      _ <- deviceInTree(s, s2)
      host = realHost(hostRoot, new TestOpener, rename = Some(rename), rebuilds = new RecordingRebuildFactory)
      _ <- pump(host, host.control.router.loaded.contains(s) && host.control.router.loaded.contains(s2))
      sds = loadedDevices(host)(deviceA)
      journal = sds.io.fsifJournal.get.get
      // s's record lies in the ring's first file, uncovered
      written = repair(host, s, 0)
      _ <- pump(host, written.isCompleted)
      attachment = newestMeta(copy).attachment
      _ = failLost = true
      _ = backendOf(host, s).failStopForTest(new java.io.IOException("injected fatal"))
      _ <- pumpTx(host, host.control.moves.isPendingLost(sds, s) &&
                        host.control.reconciler.activeDeviceChecks.isEmpty)
      pendingAfterFailure = CopyFixtures.phaseOf(dev, s)
      // Another attachment fills the rest of the ring, covering each append: only s's record pins a file
      filled = fillRing(journal, dev / "probe")
      held = repair(host, s2, 1)
      _ <- pump(host, held.isCompleted, rounds = 200)
      heldWhilePinned = !held.isCompleted
      pinned = journal.needsReplay(attachment)
      // The device check retries the move: durable, so released, and s2's commit is admitted
      _ = failLost = false
      _ = host.control.reconciler.checkStorageDevice(deviceA)
      _ <- pumpTx(host, held.isCompleted && host.control.reconciler.activeDeviceChecks.isEmpty)
    yield
      written.future.value should be(Some(Success(())))
      pendingAfterFailure should be(Some(Phase.Live))
      filled should be(true)
      heldWhilePinned should be(true)
      pinned should be(true)
      held.future.value should be(Some(Success(())))
      journal.needsReplay(attachment) should be(false)
      host.control.moves.isPendingLost(sds, s) should be(false)
      os.exists(copy) should be(false)

  /** Appends one small record at a time under a probe attachment at `at`, each covered once written, until the
   *  journal refuses a reservation for one: true once refused, false if it never was */
  private def fillRing(journal: FsifDeviceJournal, at: os.Path): Boolean =
    val probe = UUID.randomUUID()
    journal.attach(probe, at, new FsifDeviceJournal.Owner:
      def complete(task: Runnable): Unit = task.run()
      def roomAvailable(): Unit = ()
      def checkpointRequested(): Unit = ()
      def journalFailed(t: Throwable): Unit = ())
    val record = JournalRecord.Mapping(0, 0, ObjectId(new UUID(9, 9)))
    val bytes = DeviceJournalFormat.taggedRecordSize(record).toLong
    // The ring holds 45 blocks: far fewer appends fill it
    var refused = false
    var appends = 0
    while !refused && appends < 100 do
      if !journal.tryReserve(probe, bytes) then
        refused = true
      else
        val serial = Await.result(journal.append(probe, List(record)), Duration(10, SECONDS))
        journal.unreserve(probe, bytes)
        journal.covered(probe, serial)
        appends += 1
    refused

  /** A probe attachment at `at` holding one record above its covered serial, as a store closed without a detach
   *  leaves its attachment: needsReplay until released */
  private def pinningProbe(journal: FsifDeviceJournal, at: os.Path): UUID =
    val probe = UUID.randomUUID()
    journal.attach(probe, at, new FsifDeviceJournal.Owner:
      def complete(task: Runnable): Unit = task.run()
      def roomAvailable(): Unit = ()
      def checkpointRequested(): Unit = ()
      def journalFailed(t: Throwable): Unit = ())
    Await.result(journal.append(probe, List(JournalRecord.Mapping(0, 0, ObjectId(new UUID(8, 8))))),
      Duration(10, SECONDS))
    probe

  atest("a released pending-delete copy's delete releases its attachments"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    journal(dev)
    val retired = CopyFixtures.placeCopy(dev, s, Phase.Retired)
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(deviceA)
    val journalNow = local.io.fsifJournal.get.get
    // Attached where the copy was served from, before its moves
    val probe = pinningProbe(journalNow, CopyLayout.path(dev, s, CopyFixtures.copyIdOf(retired), Phase.Live))
    val pinnedBefore = journalNow.needsReplay(probe)
    host.control.copies.release(local, deviceState(deviceA, active(s)))
    pump(host, !os.exists(retired) && host.control.copies.deletesRunning.isEmpty).map: _ =>
      pinnedBefore should be(true)
      os.exists(retired) should be(false)
      journalNow.needsReplay(probe) should be(false)

  atest("a discarded copy's delete releases its attachments"):
    given ExecutionContext = executionContext
    val (hostRoot, dev) = device()
    journal(dev)
    val rebuilt = CopyFixtures.placeCopy(dev, s, Phase.Rebuilt("ep"))
    val host = newHost(hostRoot)
    host.testingOnlyHandleEvents()
    val journalNow = loadedDevices(host)(deviceA).io.fsifJournal.get.get
    val probe = pinningProbe(journalNow, rebuilt)
    val pinnedBefore = journalNow.needsReplay(probe)
    @volatile var discarded = false
    // As its callers do: the deleted copy's record is forgotten
    host.control.copies.discard(s, deviceA, "test"): sds =>
      host.control.copyRegistry.removed("test", sds, s)
      discarded = true
    pump(host, discarded).map: _ =>
      pinnedBefore should be(true)
      discarded should be(true)
      os.exists(rebuilt) should be(false)
      journalNow.needsReplay(probe) should be(false)

  /** `copy`'s newest index made seq `seq` with `meta`, as an open's attach index (or a detach) leaves it */
  private def writeIndex(copy: os.Path, seq: Long, meta: IndexMeta): Unit =
    os.write.over(copy / FsifRecovery.IndexFiles((seq % 2).toInt), IndexFile.encode(seq, meta,
      IndexMap.empty(layout.segmentCount)))

  atest("a transfer out of an undetached FSIF copy is refused"):
    given ExecutionContext = executionContext
    val sends = new StagedSendService
    var copy = os.root
    def place(dev: os.Path): Unit =
      journal(dev)
      copy = fsifCopy(dev, shipped)
    for
      (host, local, dev, tid) <- transferring(place, sends)
      journalId = local.io.fsifJournal.get.get.sizing.journalId
      // As a close that could not detach leaves it (a frozen store's): attached to the device's journal
      _ = writeIndex(copy, 1, IndexMeta(journalId, UUID.randomUUID(), -1, detached = false))
      refused <- askAndSettle(host)
      sourcesAfterRefusal = sends.sources
      phaseAfterRefusal = CopyFixtures.phaseOf(dev, shipped)
      // Detached since: the receiver's retransmit ships it
      _ = writeIndex(copy, 2, IndexMeta(journalId, UUID.randomUUID(), -1, detached = true))
      _ <- askAndSettle(host)
    yield
      refused.isSuccess should be(true)
      sourcesAfterRefusal should be(Vector())
      phaseAfterRefusal should be(Some(Phase.Live))
      sends.sources.map(_._1) should be(Vector(shipped))
      CopyFixtures.phaseOf(dev, shipped) should be(Some(Phase.Outbound(tid)))
