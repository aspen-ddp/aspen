package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.host.{CopyFixtures, DeviceIo, JobKind, LockstepDeviceIo, LockstepThreading}
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendOpener, DefaultBackendOpener, MapBackend}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS}
import scala.language.implicitConversions

/** A rebuild's staging I/O runs on its destination device's DeviceIo, never on the client context: a hung destination
 *  stalls only the rebuild. Driven in lockstep, with the destination device held mid-rebuild. */
class RebuildingStoreIoSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  private def populate(n: Int): Future[List[ObjectId]] =
    given ExecutionContext = executionContext
    Future.sequence((0 until n).toList.map: i =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        tx.replaySafe("test")
        client.getStoragePool(PoolId.BootstrapPoolId).flatMap(_.allocator.allocateDataObject(Array[Byte](i.toByte)))
          .map(_.id))

  /** Drains the device's lockstep FIFO and yields the test's context until `cond` holds, or gives up (false) once
   *  `limit` has passed. Never awaits: a stalled rebuild fails the test rather than hang it. Bounded by time, not
   *  turns, because part of the work runs on the device's real data threads, whose pace a loaded machine sets. Past
   *  the first FastRounds each round waits a millisecond on a timer (holding no thread), so a long wait spins no core */
  private def pumpUntil(threading: LockstepThreading, cond: () => Boolean,
                        limit: FiniteDuration = FiniteDuration(30, SECONDS)): Future[Boolean] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + limit.toNanos
    def loop(round: Int): Future[Boolean] =
      threading.drain()
      if cond() then Future.successful(true)
      else if System.nanoTime() > deadline then Future.successful(false)
      else
        val pause = if round < FastRounds then Future(()) else RebuildingStoreSuite.timer(FiniteDuration(1, MILLISECONDS))
        pause.flatMap(_ => loop(round + 1))
    loop(0)

  private val FastRounds = 1000

  /** Creates the staging store as the default opener does (a real directory for finish to complete), but opens it as
   *  an in-memory MapBackend: its writes and flushes touch no disk, so every disk touch of the walk is a lane job */
  private def mapOpener: BackendOpener =
    val default = new DefaultBackendOpener(executionContext)
    new BackendOpener:
      def create(cfg: StoreConfig, path: os.Path, creatingRoot: os.Path, io: DeviceIo): Future[Unit] =
        default.create(cfg, path, creatingRoot, io)
      def open(cfg: StoreConfig, path: os.Path, onFatal: Throwable => Unit, io: DeviceIo): Future[Backend] =
        Future.successful(new MapBackend(cfg.storeId))

  /** Allocates an object and reads it back: a client transaction committed end to end */
  private def allocateOne(): Future[List[Byte]] =
    given ExecutionContext = executionContext
    for
      ptr <- client.transactUntilSuccessful: tx =>
               given Transaction = tx
               tx.replaySafe("test")
               client.getStoragePool(PoolId.BootstrapPoolId).flatMap(_.allocator.allocateDataObject(Array[Byte](42)))
      os <- client.read(ptr)
    yield os.data.getByteArray.toList

  atest("a rebuild onto a held device never blocks the client context"):
    given ExecutionContext = executionContext
    val dev = os.temp.dir(prefix = "rebuilding-store-io")
    val threading = new LockstepThreading(executionContext)
    val io = new LockstepDeviceIo(dev, threading, DeviceIo.Config())
    var rebuild: RebuildingStore = null
    val run = for
      _ <- populate(6)
      _ <- net.waitForTransactionsToComplete()
      _ = rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, mapOpener, io,
                                        CopyFixtures.existingCopy(dev, storeId), checkpointInterval = 2,
                                        // Held once the staging store is open: the walk's first checkpoint waits
                                        testingOnlyAfterQuery = () => { threading.hold(dev); Future.unit })
      heldCheckpoint <- pumpUntil(threading, () => threading.held(dev).nonEmpty)
      heldKinds = threading.held(dev)
      // The client context is free while the destination's checkpoint waits: another transaction commits
      committed <- allocateOne()
      _ <- net.waitForTransactionsToComplete()
      stillHeld = threading.held(dev)
      doneWhileHeld = rebuild.complete.isCompleted
      _ = threading.release(dev)
      finished <- pumpUntil(threading, () => rebuild.complete.isCompleted)
      outcome = rebuild.complete.value
    yield
      heldCheckpoint should be(true)
      heldKinds should be(List(JobKind.Staging))
      committed should be(List[Byte](42))
      stillHeld should be(List(JobKind.Staging))
      doneWhileHeld should be(false)
      finished should be(true)
      outcome.exists(_.isSuccess) should be(true)
      CopyFixtures.copyPath(dev, storeId) should be(defined)
    run.andThen(_ => io.close())

  atest("a rebuild whose staging writes hang on the held device leaves the client context free"):
    given ExecutionContext = executionContext
    val dev = os.temp.dir(prefix = "rebuilding-store-io")
    val threading = new LockstepThreading(executionContext)
    val io = new LockstepDeviceIo(dev, threading, DeviceIo.Config())
    var rebuild: RebuildingStore = null
    var queried = false
    val run = for
      _ <- populate(6)
      _ <- net.waitForTransactionsToComplete()
      _ = rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev,
                                        new DefaultBackendOpener(executionContext), io,
                                        CopyFixtures.existingCopy(dev, storeId), checkpointInterval = 2,
                                        testingOnlyTrackRestoredKeys = true,
                                        // Held before the walk's first write: the RocksDB staging put waits on the
                                        // device's data pool
                                        testingOnlyAfterQuery = () => { threading.hold(dev); queried = true; Future.unit })
      reached <- pumpUntil(threading, () => queried)
      committed <- allocateOne()
      _ <- net.waitForTransactionsToComplete()
      doneWhileHeld = rebuild.complete.isCompleted
      restoredWhileHeld = rebuild.testingOnlyRestoredKeys
      _ = threading.release(dev)
      finished <- pumpUntil(threading, () => rebuild.complete.isCompleted)
      outcome = rebuild.complete.value
    yield
      reached should be(true)
      committed should be(List[Byte](42))
      doneWhileHeld should be(false)
      // The first staging write waits on the held data pool, so nothing is restored meanwhile
      restoredWhileHeld shouldBe empty
      finished should be(true)
      rebuild.testingOnlyRestoredKeys.size should be >= 6
      outcome.exists(_.isSuccess) should be(true)
      CopyFixtures.copyPath(dev, storeId) should be(defined)
    run.andThen(_ => io.close())

  private val mainRoot = os.pwd / "src" / "main" / "scala" / "org" / "aspen_ddp" / "aspen" / "server"

  /** `Await.` on code lines (comments skipped), per file under `dir` */
  private def awaitLines(dir: os.Path): Map[String, List[String]] =
    os.walk(dir).filter(_.ext == "scala").toList.flatMap: file =>
      val hits = os.read.lines(file).toList.filter: line =>
        val t = line.trim
        val code = line.indexOf("//") match
          case -1 => line
          case i => line.take(i)
        !(t.startsWith("//") || t.startsWith("*") || t.startsWith("/*")) && code.contains("Await.")
      if hits.isEmpty then None else Some(file.relativeTo(mainRoot).toString -> hits.map(_.trim))
    .toMap

  test("no Await on any rebuild path"):
    awaitLines(mainRoot / "rebuild") shouldBe empty
    // The one Await left under the backends is RocksDB's bootstrapPut, run only by bootstrap
    awaitLines(mainRoot / "store" / "backend") should be(
      Map("store/backend/rocksdb/BufferedConsistentRocksDB.scala" -> List("Await.result(fcommit, 10.seconds)")))
    val rocks = os.read.lines(mainRoot / "store" / "backend" / "rocksdb" / "BufferedConsistentRocksDB.scala").toList
    val at = rocks.indexWhere(_.contains("Await.result(fcommit"))
    rocks.take(at).reverse.find(_.trim.startsWith("def ")).map(_.trim) should
      be(Some("def bootstrapPut(key: Array[Byte], value: Array[Byte]): Unit = {"))
