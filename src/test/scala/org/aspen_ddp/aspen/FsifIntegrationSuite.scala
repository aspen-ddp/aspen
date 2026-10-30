package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{AppendPosition, AspenClient, RangedWrite, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, uuid2byte}
import org.aspen_ddp.aspen.server.{Host, HostTestHarness, StoreConfig}
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.{Completion, CompletionHandler, FsifConfig}
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifRecovery}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.transfer.StoreTransferService

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration, SECONDS}
import scala.util.Random

/** A real Host serving FSIF stores inside the TestNetwork.
  *
  * Its object cache is disabled, so every read is served by the FSIF backends. Every backend it loads reports its
  * completions through a handler that also schedules `network.handleEvents()` on the test's execution context: FSIF
  * completes I/O on its own threads, and nothing else would drain the events they queue. Constructor parameters are
  * named apart from Host's members, as RecordingHost's are.
  */
private class FsifTestHost(network: TestNetwork,
                           mgrClient: AspenClient,
                           hid: HostId,
                           systemId: UUID,
                           hostRootDir: os.Path,
                           execCtx: ExecutionContext)
  extends Host(mgrClient, hid, systemId, hostRootDir, execCtx, _ => ObjectCache.NoCache, network,
    BackgroundTaskManager.NoBackgroundTaskManager, TestNetwork.TestCRL, network.FinalizerFactory,
    TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
    StoreTransferService.Disabled, StoreRebuildFactory.Filesystem, 2):

  // Lazy: Host's constructor loads the stores already on disk, before this subclass's fields are initialized
  private lazy val pumpingHandler: CompletionHandler =
    val queue = new Host.IOHandler(this)
    new CompletionHandler:
      override def complete(op: Completion): Unit =
        queue.complete(op)
        execCtx.execute(() => network.handleEvents())

  /** Replaces the handler the LoadStore event installs. Parasitic, so it runs inside that event, before the store
    * can start any I/O.
    */
  override def loadStore(storageDeviceId: StorageDeviceId,
                         backend: org.aspen_ddp.aspen.server.store.backend.Backend,
                         maxSliceSize: Option[Long]): Future[Unit] =
    val loaded = super.loadStore(storageDeviceId, backend, maxSliceSize)
    loaded.foreach(_ => backend.setCompletionHandler(pumpingHandler))(using ExecutionContext.parasitic)
    loaded

  def fsifBackends: List[FsifBackend] = synchronized:
    stores.values.toList.map(_.backend).collect { case b: FsifBackend => b }

object FsifIntegrationSuite:
  private val timer: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor: r =>
    val t = new Thread(r, "fsif-integration-poll")
    t.setDaemon(true)
    t

  /** Completes after `ms`. Only paces the polling of asynchronous conditions; nothing is ordered by it. */
  def delay(ms: Long): Future[Unit] =
    val p = Promise[Unit]()
    timer.schedule((() => p.success(())): Runnable, ms, TimeUnit.MILLISECONDS)
    p.future

/** A RandomAccessReedSolomon pool on FSIF stores, driven through the real client: appends, range reads, ranged
  * writes, a crash-only host restart and a rebuild of one store.
  *
  * The bootstrap pool stays on the TestNetwork's in-memory host. The FSIF pool's five stores live on one device of a
  * real Host (FsifTestHost) with its own HostId, device set and directory tree; TestNetwork routes the pool's
  * messages to it. Pointers come from the pool's FsifIndexAllocationStrategy, installed at creation.
  */
class FsifIntegrationSuite extends IntegrationTestSuite with HostTestHarness:
  import FsifIntegrationSuite.delay

  /** The IDA of the RARS integration suites. The bootstrap pool uses it too: the test client resolves every
    * pointer's IDA to the bootstrap pool's.
    */
  override def testIda: IDA = RandomAccessReedSolomon(5, 3, 4, 64)


  private given ExecutionContext = executionContext

  private val fsifHostId = HostId(UUID.fromString("f5f5f5f5-0000-0000-0000-000000000001"))
  private val segmentCount = 256

  private def ida: IDA = net.ida
  private def fsifConfig: FsifConfig = FsifConfig.forObjectSize(ida, 64 * 1024, segmentCount)

  private var host: FsifTestHost = scala.compiletime.uninitialized
  private var hostRoot: os.Path = scala.compiletime.uninitialized
  private var deviceDir: os.Path = scala.compiletime.uninitialized
  private var deviceId: StorageDeviceId = scala.compiletime.uninitialized
  private var poolId: PoolId = scala.compiletime.uninitialized

  override def subFixtureTeardown(): Unit =
    if host != null then
      // Parasitic: this blocks the test's serial execution context, which could otherwise never combine them
      val closes = Future.sequence(host.fsifBackends.map(_.close()))(using implicitly, ExecutionContext.parasitic)
      try Await.ready(closes, Duration(10, SECONDS))
      catch case _: Throwable => ()
      host = null
    super.subFixtureTeardown()

  private def storeIds: List[StoreId] = (0 until ida.width).toList.map(i => StoreId(poolId, i.toByte))

  private def storeDir(storeId: StoreId): os.Path = deviceDir / storeId.directoryName

  // ---- Polling ----------------------------------------------------------------------------------------------

  /** Polls `cond` until it holds, draining every host's events before each test of it. Fails after `timeout`. */
  private def awaitCondition(what: String, timeout: FiniteDuration = 30.seconds)(cond: => Future[Boolean]): Future[Unit] =
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      net.handleEvents()
      cond.flatMap: ok =>
        if ok then
          Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"Timed out after $timeout waiting for $what"))
        else
          delay(2).flatMap(_ => loop())
    loop()

  private def deviceEntries: Future[Map[StoreId, StorageDeviceState.StoreEntry]] =
    client.getStorageDeviceState(deviceId).map(_.stores.filter((s, _) => s.poolId == poolId))

  /** Every store of the pool is Active on the device and served by the current host */
  private def awaitStoresServed(): Future[Unit] =
    awaitCondition("every FSIF store to be Active and served"):
      deviceEntries.map: entries =>
        entries.size == ida.width &&
          entries.values.forall(_.status == StorageDeviceState.StoreStatus.Active) &&
          storeIds.forall(host.containsStore)

  /** Waits until `storeId` is Active and served again, and returns whether its entry was seen Rebuilding on the
    * way: the reconcile flips a lost store's entry to Rebuilding before its rebuild starts.
    */
  private def awaitRebuilt(storeId: StoreId): Future[Boolean] =
    var sawRebuilding = false
    awaitCondition(s"store $storeId to be rebuilt and served"):
      deviceEntries.map: entries =>
        val status = entries.get(storeId).map(_.status)
        if status.contains(StorageDeviceState.StoreStatus.Rebuilding) then
          sawRebuilding = true
        status.contains(StorageDeviceState.StoreStatus.Active) && host.containsStore(storeId)
    .map(_ => sawRebuilding)

  // ---- Setup ------------------------------------------------------------------------------------------------

  private def newFsifHost(): FsifTestHost =
    new FsifTestHost(net, client, fsifHostId, systemId, hostRoot, executionContext)

  /** The FSIF host, its device and the pool, with every store created and served */
  private def setUp(): Future[Unit] =
    for
      _ <- client.createHost("fsif-host", "127.0.0.1", 5850, 5851, 5852, fsifHostId)
      setId <- client.createStorageDeviceSet("fsif-set", 0, None)
      devId <- client.createStorageDevice(fsifHostId, setId)
      _ = deviceId = devId
      _ = hostRoot = newHostDir()
      _ = deviceDir = writeDevice(hostRoot, "dev0", deviceId, owner = Some(fsifHostId))
      _ = host = newFsifHost()
      // The set's only device takes every store of the pool
      pid <- client.createNewStoragePool("fsif-pool", ida, None, fsifConfig, setId, 0L)
      _ = poolId = pid
      _ = net.poolHosts += poolId -> host
      _ = host.testingOnlyCheckAllDevices()
      _ <- awaitStoresServed()
    yield ()

  /** Crash-only restart: the host is dropped, never shut down, and a new one starts from the same directories.
    *
    * The old backends are quiesced and their file handles closed first. That is the in-process stand-in for the
    * process being gone, so it cannot write behind the new host's recovery; FSIF's close writes nothing (no
    * checkpoint, no marker), and the Host close path, which writes CRL save files and markers, is never run.
    * `whileDown` runs on the directories with no host running.
    */
  private def crashAndRestart(whileDown: () => Unit = () => ()): Future[Unit] =
    for
      _ <- waitForTransactionsToComplete()
      _ <- Future.sequence(host.fsifBackends.map(_.close()))
    yield
      whileDown()
      host = newFsifHost()
      net.poolHosts += poolId -> host

  // ---- Data -------------------------------------------------------------------------------------------------

  private def chunkOf(rnd: Random, length: Int): Array[Byte] =
    val a = new Array[Byte](length)
    rnd.nextBytes(a)
    a

  private def bytesShouldMatch(actual: Array[Byte], expected: Array[Byte]): Unit =
    withClue("length: "):
      actual.length should be(expected.length)
    val mismatch = actual.indices.find(i => actual(i) != expected(i))
    withClue(s"bytes differ at index ${mismatch.getOrElse(-1)}: "):
      mismatch should be(None)

  /** The radicle key linking an object allocated by these tests */
  private def linkKey(dp: DataObjectPointer): Key = Key(dp.id.toBytes)

  /** Allocates a DataObject holding `content` in the FSIF pool and links it from the radicle, in the bootstrap pool,
    * in the same transaction. `radicleRevision` adds a revision requirement on the radicle.
    */
  private def allocate(content: Array[Byte], radicleRevision: Option[ObjectRevision] = None): Future[DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    for
      pool <- client.getStoragePool(poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(content))
      _ = tx.update(radicle, radicleRevision, None, List(KeyValueUpdate.DoesNotExist(linkKey(dp))),
                    List(Insert(linkKey(dp), dp.toArray)))
      _ <- tx.commit()
    yield dp

  /** The pointer the radicle links under `dp`'s key, if any */
  private def radicleLink(dp: DataObjectPointer): Future[Option[Array[Byte]]] =
    client.read(radicle).map(_.contents.get(linkKey(dp)).map(_.value.bytes))

  private def appendAll(start: AppendPosition, chunks: List[Array[Byte]]): Future[AppendPosition] =
    chunks.foldLeft(Future.successful(start)): (fpos, chunk) =>
      fpos.flatMap: pos =>
        val tx = client.newTransaction()
        val next = tx.append(pos, DataBuffer(chunk))
        tx.commit().map(_ => next)

  private def applyWrites(content: Array[Byte], writes: List[RangedWrite]): Array[Byte] =
    val result = content.clone()
    writes.foreach(w => System.arraycopy(w.data, 0, result, w.offset, w.data.length))
    result

  private def sequentially[A](items: List[A])(f: A => Future[Unit]): Future[Unit] =
    items.foldLeft(Future.unit)((acc, item) => acc.flatMap(_ => f(item)))

  /** Completes with `f`, draining every host's events while it is pending: Host.repair only queues its work */
  private def pumpUntilComplete[T](f: Future[T], deadline: Long): Future[T] =
    if f.isCompleted then
      f
    else if System.nanoTime() > deadline then
      Future.failed(new AssertionError("Timed out pumping events for a repair"))
    else
      net.handleEvents()
      delay(1).flatMap(_ => pumpUntilComplete(f, deadline))

  /** Brings every store of the pool to the current state of `dp` through Host.repair, the path StoreRepairer uses.
    *
    * The TestNetwork delivers messages synchronously, so a transaction can resolve before its prepare reaches the
    * last stores, and those stores miss the update. Reads with stores silenced need every remaining store current,
    * so tests repair first, as the in-memory integration suites do. Repairing a current store rewrites the same
    * state: Frontend.repair skips a store whose copy is not older. No other writer runs meanwhile.
    */
  private def repairEveryStore(dp: DataObjectPointer, timeout: FiniteDuration = 30.seconds): Future[Unit] =
    val deadline = System.nanoTime() + timeout.toNanos
    for
      _ <- waitForTransactionsToComplete()
      state <- client.read(dp)
      _ <- sequentially(storeIds): sid =>
             val done = Promise[Unit]()
             host.repair(sid, state, done)
             pumpUntilComplete(done.future, deadline)
    yield ()

  /** Reads `dp` once for every set of stores that can be silenced while the rest still restore it, never silencing
    * the stores in `mustServe`, and requires `expected` from each. With k = 3 of 5, each read uses exactly the three
    * stores left, so every store's slice is checked. With `repairFirst`, stale stores are repaired before; without
    * it, every store must already be current (the caller repaired them before a crash, or rebuilt one).
    */
  private def readsWithStoresDown(dp: DataObjectPointer, expected: Array[Byte], mustServe: Set[Int] = Set(),
                                  repairFirst: Boolean = true): Future[Unit] =
    val silenceable = (0 until ida.width).filterNot(mustServe.contains).toList
    val subsets = silenceable.combinations(ida.width - ida.consistentRestoreThreshold).toList
    val ready = if repairFirst then repairEveryStore(dp) else waitForTransactionsToComplete()
    ready.flatMap: _ =>
      sequentially(subsets): silenced =>
        net.withReadSilenced(silenced.map(storeIds(_)).toSet)(client.read(dp)).map: s =>
          withClue(s"silenced ${silenced.sorted}: "):
            bytesShouldMatch(s.data.getByteArray, expected)

  /** A full read, a tail-reaching and a bounded range read, and the append position all match `expected` */
  private def verify(dp: DataObjectPointer, expected: Array[Byte]): Future[Unit] =
    val len = expected.length
    val mid = len / 3
    for
      _ <- waitForTransactionsToComplete()
      state <- client.read(dp)
      tail <- client.readRange(dp, mid, None)
      window <- client.readRange(dp, mid, Some(len / 4))
      pos <- client.readAppendPosition(dp)
    yield
      bytesShouldMatch(state.data.getByteArray, expected)
      withClue("tail range: "):
        bytesShouldMatch(tail.data.getByteArray, expected.slice(mid, len))
        tail.objectLength should be(Some(len))
      withClue("bounded range: "):
        bytesShouldMatch(window.data.getByteArray, expected.slice(mid, mid + len / 4))
      pos.length should be(len)

  // ---- On-disk checks ---------------------------------------------------------------------------------------

  private val rocksDbFiles = Set("CURRENT", "IDENTITY", "LOCK", "LOG")

  /** Every store directory is an initialized FSIF store and holds nothing RocksDB writes */
  private def storesShouldBeFsifOnDisk(): Unit =
    storeIds.foreach: sid =>
      val dir = storeDir(sid)
      withClue(s"$dir: "):
        StoreConfig.loadStoreConfig(dir / StoreConfig.configFilename).backend should be(
          StoreConfig.Fsif(fsifConfig.segmentPayloadSize, fsifConfig.segmentCount))
        val names = os.list(dir).map(_.last).toSet
        names should contain allOf(FsifRecovery.DataFile, FsifRecovery.IndexFiles(0), FsifRecovery.JournalFiles(0),
          FsifBackend.InitializedMarkerFile)
        val rocks = names.filter: n =>
          rocksDbFiles.contains(n) || n.endsWith(".sst") || n.endsWith(".log") && n != "crl_save.log" ||
            n.startsWith("MANIFEST-") || n.startsWith("OPTIONS-")
        rocks should be(Set())

  private def externalIndex(dp: DataObjectPointer): Int =
    dp.storePointer.length should be(4)
    ByteBuffer.wrap(dp.storePointer).getInt

  // ---- Tests ------------------------------------------------------------------------------------------------

  atest("a RARS pool on FSIF stores serves 50 appends, range reads and the append position"):
    val rnd = new Random(1)
    val chunks = List.fill(50)(chunkOf(rnd, 1 + rnd.nextInt(300)))
    val expected = chunks.reduce(_ ++ _)
    for
      _ <- setUp()
      _ = storesShouldBeFsifOnDisk()
      dp <- allocate(Array[Byte]())
      other <- allocate(Array[Byte](1, 2, 3))
      start <- client.readAppendPosition(dp)
      last <- appendAll(start, chunks)
      _ <- verify(dp, expected)
      ranges <- client.readRanges(dp, List((0, 10), (expected.length - 64, 64)))
      finalPos <- client.readAppendPosition(dp)
      _ <- readsWithStoresDown(dp, expected)
      otherState <- client.read(other)
      dpLink <- radicleLink(dp)
      otherLink <- radicleLink(other)
    yield
      dpLink.map(_.toList) should be(Some(dp.toArray.toList))
      otherLink.map(_.toList) should be(Some(other.toArray.toList))
      List(externalIndex(dp), externalIndex(other)) should be(List(0, 1))
      start.length should be(0)
      last.length should be(expected.length)
      finalPos should be(last)
      bytesShouldMatch(ranges.head.data.getByteArray, expected.slice(0, 10))
      bytesShouldMatch(ranges(1).data.getByteArray, expected.takeRight(64))
      bytesShouldMatch(otherState.data.getByteArray, Array[Byte](1, 2, 3))
      succeed

  atest("a failed requirement on the radicle aborts an allocation in the FSIF pool linked from it"):
    var allocated: Option[DataObjectPointer] = None
    for
      _ <- setUp()
      before <- client.read(radicle)
      aborted <- allocate(Array[Byte](1, 2, 3), Some(new ObjectRevision(UUID.randomUUID())))
                   .map: dp =>
                     allocated = Some(dp)
                     false
                   .recover { case _ => true }
      _ <- waitForTransactionsToComplete()
      after <- client.read(radicle)
      // The rejected transaction leaves the next allocation free to link from the radicle
      dp <- allocate(Array[Byte](4, 5))
      _ <- waitForTransactionsToComplete()
      state <- client.read(dp)
      link <- radicleLink(dp)
    yield
      aborted should be(true)
      allocated should be(None)
      after.contents.keySet should be(before.contents.keySet)
      bytesShouldMatch(state.data.getByteArray, Array[Byte](4, 5))
      link.map(_.toList) should be(Some(dp.toArray.toList))

  atest("rangedWrites updates a middle range of an object on FSIF stores"):
    val rnd = new Random(2)
    val content = chunkOf(rnd, 4000)
    val writes = List(RangedWrite(1500, chunkOf(rnd, 700)))
    val expected = applyWrites(content, writes)
    for
      _ <- setUp()
      dp <- allocate(content)
      _ <- waitForTransactionsToComplete()
      state <- client.readRangedWriteState(dp, writes)
      tx = client.newTransaction()
      _ = tx.rangedWrites(state, writes)
      _ <- tx.commit()
      _ <- verify(dp, expected)
      middle <- client.readRange(dp, 1400, Some(900))
      _ <- readsWithStoresDown(dp, expected)
    yield
      bytesShouldMatch(middle.data.getByteArray, expected.slice(1400, 2300))
      succeed

  atest("data on FSIF stores survives a crash-only restart of their host"):
    val rnd = new Random(3)
    val chunks = List.fill(20)(chunkOf(rnd, 1 + rnd.nextInt(300)))
    val appended = chunks.reduce(_ ++ _)
    val writes = List(RangedWrite(100, chunkOf(rnd, 250)))
    val expected = applyWrites(appended, writes)
    val more = List.fill(5)(chunkOf(rnd, 1 + rnd.nextInt(300)))
    for
      _ <- setUp()
      dp <- allocate(Array[Byte]())
      start <- client.readAppendPosition(dp)
      _ <- appendAll(start, chunks)
      _ <- waitForTransactionsToComplete()
      state <- client.readRangedWriteState(dp, writes)
      tx = client.newTransaction()
      _ = tx.rangedWrites(state, writes)
      _ <- tx.commit()
      // Every store holds the acknowledged state before the crash, so each must recover it unaided
      _ <- repairEveryStore(dp)
      _ <- crashAndRestart()
      _ <- awaitStoresServed()
      _ <- verify(dp, expected)
      _ <- readsWithStoresDown(dp, expected, repairFirst = false)
      // The recovered stores accept new commits
      pos <- client.readAppendPosition(dp)
      _ <- appendAll(pos, more)
      _ <- readsWithStoresDown(dp, expected ++ more.reduce(_ ++ _))
    yield
      storesShouldBeFsifOnDisk()
      succeed

  atest("an FSIF store whose recovery finds it lost is rebuilt through the rebuild path"):
    val rnd = new Random(4)
    val first = chunkOf(rnd, 3000)
    val chunks = List.fill(10)(chunkOf(rnd, 1 + rnd.nextInt(300)))
    val expected = first ++ chunks.reduce(_ ++ _)
    val small = chunkOf(rnd, 100)
    val more = List.fill(3)(chunkOf(rnd, 1 + rnd.nextInt(300)))
    val lost = 0
    for
      _ <- setUp()
      dp <- allocate(first)
      other <- allocate(small)
      start <- client.readAppendPosition(dp)
      _ <- appendAll(start, chunks)
      // The rebuild walks the allocation tree, which a finalization action fills after each allocation commits
      pool <- client.getStoragePool(poolId)
      _ <- awaitCondition("both objects to be in the allocation tree"):
             for
               a <- pool.allocationTree.get(Key(dp.id.toBytes))
               b <- pool.allocationTree.get(Key(other.id.toBytes))
             yield a.isDefined && b.isDefined
      _ <- repairEveryStore(dp)
      _ <- repairEveryStore(other)
      lostDir = storeDir(storeIds(lost))
      // With no index file left, recovery cannot reconstruct the acknowledged state: StoreLost
      _ <- crashAndRestart: () =>
             FsifRecovery.IndexFiles.map(lostDir / _).filter(os.exists(_)).foreach(os.remove)
      // The new host's startup load (recovery, off the event thread) finds the store lost and marks it, so the
      // reconcile flips its entry to Rebuilding: awaitRebuilt sees that state
      sawRebuilding <- awaitRebuilt(storeIds(lost))
      _ <- awaitStoresServed()
      _ <- verify(dp, expected)
      // Silencing only other stores forces the rebuilt store's slice into every read. No repair: what the store
      // serves is what the rebuild wrote
      _ <- readsWithStoresDown(dp, expected, mustServe = Set(lost), repairFirst = false)
      _ <- readsWithStoresDown(other, small, mustServe = Set(lost), repairFirst = false)
      pos <- client.readAppendPosition(dp)
      _ <- appendAll(pos, more)
      _ <- readsWithStoresDown(dp, expected ++ more.reduce(_ ++ _), mustServe = Set(lost))
    yield
      sawRebuilding should be(true)
      storesShouldBeFsifOnDisk()
      os.exists(lostDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)
      os.exists(deviceDir / RebuildingStore.RebuildDirectory / storeIds(lost).directoryName) should be(false)
      succeed
