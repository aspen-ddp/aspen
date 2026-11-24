package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.BaseAspenClient
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, Read, ReadResponse}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, FullObject, Insert, Key, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.rebuild.StoreRebuildFactory
import org.aspen_ddp.aspen.server.store.backend.{FsifConfig, RocksDBConfig}
import org.aspen_ddp.aspen.server.store.backend.fsif.FsifBackend
import org.aspen_ddp.aspen.server.store.backend.rocksdb.RocksDBBackend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.transfer.StoreTransferService

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration, MILLISECONDS, SECONDS}
import scala.jdk.CollectionConverters.*

/** The gate of the hung device's lanes: every task of its DeviceIo runs `await` first, which blocks while the latch is
 *  shut. Its own class, so a thread dump shows who waits on it */
private final class HungGate:
  @volatile private var latch: Option[CountDownLatch] = None
  def shut(): Unit = latch = Some(new CountDownLatch(1))
  def open(): Unit =
    val l = latch
    latch = None
    l.foreach(_.countDown())
  def await(): Unit = latch.foreach(_.await())

/** A fixed StoreThreadPool whose DeviceIo of `hungPath` runs `gate` before every task and probe attempt
 *  (ThreadedDeviceIo's fault-injection seam), with `hungConfig`; every other device's is as production's, with
 *  `otherConfig` */
private final class GatedThreading(hungPath: os.Path, gate: HungGate, hungConfig: DeviceIo.Config,
                                   otherConfig: DeviceIo.Config) extends StoreThreadPool(maxThreads = 3):
  val ios = new ConcurrentHashMap[os.Path, ThreadedDeviceIo]()
  override def newDeviceIo(path: os.Path): DeviceIo =
    val io =
      if path == hungPath then new ThreadedDeviceIo(path, hungConfig, () => gate.await())
      else new ThreadedDeviceIo(path, otherConfig)
    ios.put(path, io)
    io

object ThreadedHungDeviceSuite:
  private[host] val timer: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor: r =>
    val t = new Thread(r, "hung-device-suite-poll")
    t.setDaemon(true)
    t

  /** Completes after `ms`: paces the polling of conditions, orders nothing */
  def delay(ms: Long): Future[Unit] =
    val p = Promise[Unit]()
    timer.schedule((() => p.success(())): Runnable, ms, TimeUnit.MILLISECONDS)
    p.future

/** The project goal of device I/O isolation on real threads (spec section 9): a host on a StoreThreadPool with two
 *  devices, A and B, each with its own device set. A's DeviceIo is gated shut (HungGate), modelling a device whose
 *  every operation blocks. B's stores, a RocksDB pool and an FSIF pool, keep committing and answering reads, every
 *  store keeps heartbeating, no thread but A's own waits on A, and A's read backlog stays bounded under a flood of
 *  client reads. Once the gate opens, A's stores answer again. The TestNetwork's bootstrap host runs on its own
 *  StoreThreadPool too, so no thread drains a lockstep host. Assertions are on outcomes, polled to deadlines */
class ThreadedHungDeviceSuite extends IntegrationTestSuite with ControlFixture:
  import ThreadedHungDeviceSuite.delay

  override def testThreading: ExecutionContext => HostThreading = _ => new StoreThreadPool(maxThreads = 3)

  override def testHostOptions: TestNetwork.HostOptions = super.testHostOptions.copy(clientStatusCache = true)

  private given ExecutionContext = executionContext

  private val hungHostId = HostId(UUID.fromString("4a4a4a4a-0000-0000-0000-000000000001"))
  private val ReadBound = 16
  private val Flood = 10 * ReadBound

  private var host: Host = scala.compiletime.uninitialized
  private var hostEc: ExecutionContextExecutorService = scala.compiletime.uninitialized
  private var hostTasks: BackgroundTaskManager = scala.compiletime.uninitialized
  private var gate: HungGate = scala.compiletime.uninitialized

  /** Stops the host: its gate opened, then each store closed through the host as a device shutdown closes it (the
   *  router drops it first, so no later store task, a usage poll's size estimate say, reaches its closed backend),
   *  then its threads (its shutdown closes its DeviceIos) and its timers */
  private def stopHost(): Future[Unit] =
    if gate != null then gate.open()
    if host == null then
      Future.unit
    else
      val h = host
      host = null
      val closed = Promise[Unit]()
      h.threading.controlEvents.execute: () =>
        val closes = h.control.router.loaded.keys.toList.map: id =>
          h.control.copyRegistry.closingFor(id, ClosePurpose.Shutdown)
          h.control.loader.closeStore(id, extractCrl = false)
        closed.completeWith(Future.sequence(closes)(using implicitly, ExecutionContext.parasitic)
          .map(_ => ())(using ExecutionContext.parasitic))
      closed.future.transform: _ =>
        h.shutdown()(using ExecutionContext.parasitic)
        // Its timer thread too (a later schedule on it is dropped)
        hostTasks.shutdown(Duration.Zero)
        scala.util.Success(())

  /** Each test's last step. First the client idle (no read outstanding, no transaction on any host) while the host
   *  still runs, for 10 polls in a row; then the host stopped; then the client idle again for 60 polls in a row (at
   *  least 300 ms, longer than a commit's missed-update marking waits, MissedUpdateFinalizationAction.errorTimeout,
   *  50 ms here, before it writes through the client): reads, and the bootstrap host's transactions, the stopped
   *  host's drivers staying as they were. The host's last client calls continue on the suite's serial context; a
   *  read or a retry still outstanding when the fixture shuts the client and the bootstrap host down would never end */
  private def quiesce(): Future[Unit] =
    def reads: Int = client.asInstanceOf[BaseAspenClient].testingOnlyOutstandingReads
    def settled(what: String, polls: Int)(idle: => Boolean): Future[Unit] =
      var stable = 0
      await(what):
        stable = if idle then stable + 1 else 0
        stable >= polls
    for
      _ <- settled("the client idle", 10)(reads == 0 && !net.hasTransactions)
      _ <- stopHost()
      _ <- settled("the client idle once the host stopped", 60)(reads == 0 && !net.smgr.hasTransactions)
    yield ()

  override def subFixtureTeardown(): Unit =
    // A test that failed before its quiesce: stop what it left running
    if host != null then
      try Await.ready(stopHost(), Duration(10, SECONDS))
      catch case _: Throwable => ()
    net.smgr.threading.shutdown()
    super.subFixtureTeardown()

  private def await(what: String, timeout: FiniteDuration = 30.seconds)(cond: => Boolean): Future[Unit] =
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      if cond then Future.unit
      else if System.nanoTime() > deadline then
        Future.failed(new IllegalStateException(s"Timed out after $timeout: $what"))
      else delay(5).flatMap(_ => loop())
    loop()

  private final case class HungRig(threading: GatedThreading, devA: os.Path, devB: os.Path, idA: StorageDeviceId,
                               idB: StorageDeviceId, setA: StorageDeviceSetId, setB: StorageDeviceSetId):
    def ioA: ThreadedDeviceIo = threading.ios.get(devA)

  /** The host with devices A and B (A gated, shut first when `hungFromStart`), A probing with `probe` and B with
   *  `probeB`, started */
  private def rig(hungFromStart: Boolean, probe: HealthProbe.Config = HealthProbe.Config(),
                  probeB: HealthProbe.Config = HealthProbe.Config()): Future[HungRig] =
    gate = new HungGate
    if hungFromStart then gate.shut()
    for
      _ <- client.createHost("hung-device-host", "127.0.0.1", 5870, 5871, 5872, hungHostId)
      setA <- client.createStorageDeviceSet("hung-set-a", 0, None)
      setB <- client.createStorageDeviceSet("hung-set-b", 0, None)
      idA <- client.createStorageDevice(hungHostId, setA)
      idB <- client.createStorageDevice(hungHostId, setB)
    yield
      val hostRoot = newHostDir()
      val devA = writeDevice(hostRoot, "dev0", idA, owner = Some(hungHostId))
      val devB = writeDevice(hostRoot, "dev1", idB, owner = Some(hungHostId))
      val threading = new GatedThreading(devA, gate, DeviceIo.Config(readBound = ReadBound, probe = probe),
        DeviceIo.Config(probe = probeB))
      // Never shut down: a continuation of the host's last work may still be submitted to it after the test (a
      // rejection would print). Its daemon threads end once idle for a second
      val pool = new java.util.concurrent.ThreadPoolExecutor(3, 3, 1, TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue[Runnable](), r =>
          val t = new Thread(r, "hung-device-host-ec")
          t.setDaemon(true)
          t)
      pool.allowCoreThreadTimeOut(true)
      hostEc = ExecutionContext.fromExecutorService(pool)
      hostTasks = new BackgroundTaskManager(hostEc)
      host = new Host(client, hungHostId, systemId, hostRoot, hostEc, threading, (_, _) => ObjectCache.NoCache, net,
        hostTasks, TestNetwork.TestCRL, net.FinalizerFactory,
        TransactionDriver.noErrorRecoveryFactory, heartbeatPeriod = Duration(50, MILLISECONDS),
        checkStorageDevicePeriod = Duration(200, MILLISECONDS), StoreTransferService.Disabled,
        StoreRebuildFactory.Filesystem, 2, deviceScanPeriod = Duration(200, MILLISECONDS))
      host.start()
      HungRig(threading, devA, devB, idA, idB, setA, setB)

  private def fsifConfig: FsifConfig = FsifConfig.forObjectSize(net.ida, 64 * 1024, 64)

  private def storesOf(pool: PoolId): List[StoreId] = (0 until net.ida.width).toList.map(i => StoreId(pool, i.toByte))

  /** A new pool on `set`. Its creation is one transaction that a concurrent metadata update (the host's own device
   *  and usage updates) can collide with; an aborted one changes nothing, so it is made again */
  private def createPool(name: String, set: StorageDeviceSetId, fsif: Boolean, attempts: Int = 0): Future[PoolId] =
    client.createNewStoragePool(name, net.ida, None, if fsif then fsifConfig else RocksDBConfig(), set, 0L)
      .recoverWith:
        case _: org.aspen_ddp.aspen.client.TransactionAborted if attempts < 10 =>
          delay(20).flatMap(_ => createPool(name, set, fsif, attempts + 1))

  /** A pool on `set`, its messages routed to the host, once every store is served */
  private def pool(name: String, set: StorageDeviceSetId, fsif: Boolean): Future[PoolId] =
    for
      pid <- createPool(name, set, fsif)
      _ = net.poolHosts += pid -> host
      _ <- await(s"pool $name served")(storesOf(pid).forall(host.containsStore))
    yield pid

  /** `f`, a failure naming the step that failed */
  private def step[T](what: String)(f: => Future[T]): Future[T] =
    f.transform(identity, e => new IllegalStateException(s"$what failed: ${e match
      case ta: org.aspen_ddp.aspen.client.TransactionAborted => s"aborted ${ta.txd}"
      case other => other.toString}", e))

  private def linkKey(dp: DataObjectPointer): Key = Key(dp.id.toBytes)

  /** A DataObject of `content` in `poolId`, linked from the radicle in the same transaction, retried until it commits
   *  (a collision under load aborts it; each attempt allocates afresh) */
  private def allocate(poolId: PoolId, content: Array[Byte]): Future[DataObjectPointer] =
    client.transactUntilSuccessful: tx =>
      tx.replaySafe("the radicle link's DoesNotExist fails a replay")
      given Transaction = tx
      for
        p <- client.getStoragePool(poolId)
        dp <- p.allocator.allocateDataObject(DataBuffer(content))
      yield
        tx.update(radicle, None, None, List(KeyValueUpdate.DoesNotExist(linkKey(dp))),
                  List(Insert(linkKey(dp), dp.toArray)))
        dp

  /** `n` overwrites of `ptr`, one after another, each requiring the revision the one before wrote. A write that
   *  aborts (a collision: under load a store may still hold the previous commit's lock when the next prepare arrives)
   *  is retried from a fresh read, as a client does; each write commits exactly once */
  private def overwrites(ptr: DataObjectPointer, n: Int, tag: Byte): Future[Array[Byte]] =
    def write(i: Int, rev: ObjectRevision, attempts: Int): Future[ObjectRevision] =
      val tx = client.newTransaction()
      tx.overwrite(ptr, rev, DataBuffer(Array[Byte](tag, i.toByte)))
      tx.commit().map(_ => ObjectRevision(tx.id)).recoverWith:
        case _: org.aspen_ddp.aspen.client.TransactionAborted if attempts < 20 =>
          delay(10).flatMap(_ => client.read(ptr)).flatMap(state => write(i, state.revision, attempts + 1))
    client.read(ptr).flatMap: first =>
      (1 to n).foldLeft(Future.successful(first.revision))((frev, i) => frev.flatMap(write(i, _, 0)))
      .map(_ => Array[Byte](tag, n.toByte))

  private def heartbeats(stores: List[StoreId]): Map[StoreId, Long] =
    stores.map(s => s -> host.control.router.heartbeatsRun(s)).toMap

  /** Threads, other than device A's own (`dev-dev0...`), whose stack is inside A's gate */
  private def waitingOnA(): List[String] =
    Thread.getAllStackTraces.asScala.toList.collect:
      case (t, stack) if stack.exists(_.getClassName.contains("HungGate")) && !t.getName.startsWith("dev-dev0") =>
        s"${t.getName} ${t.getState}"

  private def threadsInGate(): List[String] =
    Thread.getAllStackTraces.asScala.toList.collect:
      case (t, stack) if stack.exists(_.getClassName.contains("HungGate")) => t.getName

  atest("a hung device stalls only its own stores"):
    val fakeClient = ClientId(UUID.randomUUID())
    val answered = ConcurrentHashMap.newKeySet[UUID]()
    // The flood's answers are recorded and dropped: they are no client's
    net.dropClientResponse =
      case rr: ReadResponse if rr.toClient == fakeClient =>
        answered.add(rr.readUUID)
        true
      case _ => false
    for
      r <- step("rig")(rig(hungFromStart = false))
      poolA <- step("pool A")(pool("hung-a-rocks", r.setA, fsif = false))
      poolR <- step("pool R")(pool("hung-b-rocks", r.setB, fsif = false))
      poolF <- step("pool F")(pool("hung-b-fsif", r.setB, fsif = true))
      ptrA <- step("allocate A")(allocate(poolA, Array[Byte](1, 1)))
      ptrR <- step("allocate R")(allocate(poolR, Array[Byte](2, 0)))
      ptrF <- step("allocate F")(allocate(poolF, Array[Byte](3, 0)))
      _ <- waitForTransactionsToComplete()
      storesB = storesOf(poolR) ++ storesOf(poolF)
      storesA = storesOf(poolA)
      // B's backends are a real RocksDB and a real FSIF, on B's own lanes
      _ = host.control.router.loaded(storesOf(poolR).head).backend shouldBe a[RocksDBBackend]
      _ = host.control.router.loaded(storesOf(poolF).head).backend shouldBe a[FsifBackend]
      // A hangs: everything its DeviceIo runs from now on blocks
      _ = gate.shut()
      beatsBefore = heartbeats(storesA ++ storesB)
      // A flood of client reads of objects of A's stores, ten times the read bound: each a backend read on A
      _ = (0 until Flood).foreach: i =>
            val ptr = new DataObjectPointer(ObjectId(UUID.randomUUID()), poolA, Array[Byte]())
            host.receiveClientRequest(Read(storesA(i % storesA.size), fakeClient, UUID.randomUUID(), ptr, FullObject()))
      _ <- await("A's read backlog over its bound")(r.ioA.readsOverBound)
      // 50 transactions on B's stores commit, 25 on each pool, and B's objects read back
      contents <- step("B's writes")(overwrites(ptrR, 25, 2).zip(overwrites(ptrF, 25, 3)))
      (contentR, contentF) = contents
      readR <- client.read(ptrR)
      readF <- client.read(ptrF)
      // Every store heartbeats, A's included: no store thread waits on A
      _ <- await("every store heartbeating")(
             heartbeats(storesA ++ storesB).forall((s, n) => n >= beatsBefore(s) + 3))
      backlog = (r.ioA.outstandingReads, r.ioA.poolTasksOutstanding)
      answeredWhile = answered.size
      blocked = waitingOnA()
      inGate = threadsInGate()
      // The gate opens: A's admitted reads are answered, the dropped ones never are, and A serves again
      _ = gate.open()
      _ <- await("A's admitted reads answered")(r.ioA.outstandingReads == 0 && answered.size >= backlog._1)
      readA <- client.read(ptrA)
      _ <- quiesce()
    yield
      contentR should be(Array[Byte](2, 25))
      readR.data.getByteArray.toList should be(contentR.toList)
      readF.data.getByteArray.toList should be(contentF.toList)
      // No thread but A's own waits on A, and some of A's do
      blocked should be(Nil)
      inGate should not be empty
      // The backlog is bounded: each of A's three stores may pass the bound's check once more, and each admitted
      // read holds one data task
      backlog._1 should be >= ReadBound.toLong
      backlog._1 should be <= (ReadBound + storesA.size).toLong
      backlog._2.toLong should be <= backlog._1
      // Over the bound, reads get no answer: none while hung, only the admitted ones after
      answeredWhile should be(0)
      answered.size.toLong should be(backlog._1)
      readA.data.getByteArray.toList should be(List[Byte](1, 1))

  atest("a host whose device hangs on its first stat starts, heartbeats and serves its other device"):
    // Review focus 3: A's startup probe blocks in the gate. The host still opens its startup gate (at its first
    // ScanDevices), loads B, creates and serves B's stores, and commits on them; A loads once the gate opens
    for
      r <- rig(hungFromStart = true)
      poolB <- pool("startup-b-rocks", r.setB, fsif = false)
      ptr <- allocate(poolB, Array[Byte](4, 0))
      content <- overwrites(ptr, 5, 4)
      read <- client.read(ptr)
      beats = heartbeats(storesOf(poolB))
      _ <- await("B's stores heartbeating")(heartbeats(storesOf(poolB)).forall((s, n) => n >= beats(s) + 3))
      blocked = waitingOnA()
      aWhile = host.getDevicePath(r.idA)
      _ = gate.open()
      _ <- await("A loaded once its gate opens")(host.getDevicePath(r.idA).isDefined)
      devicePathB = host.getDevicePath(r.idB)
      _ <- quiesce()
    yield
      read.data.getByteArray.toList should be(content.toList)
      blocked should be(Nil)
      aWhile should be(None)
      devicePathB should be(Some(r.devB))

  /** On the host's control thread: one judge pass (as `Host.CheckDeviceHealth` runs it), then the health of each
   *  device in `ids`, None for one not loaded */
  private def judgePass(ids: StorageDeviceId*): Future[List[Option[DeviceHealth]]] =
    val h = host
    val p = Promise[List[Option[DeviceHealth]]]()
    h.threading.controlEvents.execute: () =>
      p.complete(scala.util.Try:
        h.control.health.check()
        ids.toList.map(id => h.control.devices.devices.get(id).map(_.health)))
    p.future

  atest("a hung device is judged Hung, the other stays Healthy, and it recovers"):
    // Device hang detection, spec section 6: both devices probe every 50 ms; A is Hung past 500 ms. The judge is
    // driven by passes on the control thread, not its 30 s timer; each pass also publishes. B is sampled at every
    // pass, from before A's gate shuts until A's record shows Healthy again
    val Hung = StorageDeviceState.HealthStatus.Hung
    val Healthy = StorageDeviceState.HealthStatus.Healthy
    val fast = HealthProbe.Config(interval = 50.millis, hangThreshold = 500.millis)
    // B's real fsync may exceed 500 ms under a loaded full run: its threshold is far above any such stall
    val fastB = HealthProbe.Config(interval = 50.millis, hangThreshold = 30.seconds)
    // Written only in judgeUntil's flatMap chain, whose steps run one after another, and read after it ends
    var bSeen = List.empty[DeviceHealth]
    def judgeUntil(what: String, r: HungRig, timeout: FiniteDuration = 5.seconds)(
        done: DeviceHealth => Future[Boolean]): Future[DeviceHealth] =
      val deadline = System.nanoTime() + timeout.toNanos
      def loop(): Future[DeviceHealth] =
        judgePass(r.idA, r.idB).flatMap:
          case List(Some(a), Some(b)) =>
            bSeen = b :: bSeen
            done(a).flatMap: ok =>
              if ok then Future.successful(a)
              else if System.nanoTime() > deadline then
                Future.failed(new IllegalStateException(s"Timed out after $timeout: $what (A $a)"))
              else delay(10).flatMap(_ => loop())
          case other => Future.failed(new IllegalStateException(s"$what: a device not loaded: $other"))
      loop()
    def recordOf(id: StorageDeviceId): Future[StorageDeviceState.HealthStatus] =
      client.getStorageDeviceState(id).map(_.health)
    for
      r <- rig(hungFromStart = false, probe = fast, probeB = fastB)
      _ <- await("A and B loaded")(host.getDevicePath(r.idA).isDefined && host.getDevicePath(r.idB).isDefined)
      _ <- judgeUntil("A Healthy, its probe run", r)(a => Future.successful(
             a.status == Healthy && r.ioA.probe.lastResult.exists(_.error.isEmpty)))
      _ = gate.shut()
      hung <- judgeUntil("A Hung", r)(a => Future.successful(a.status == Hung))
      _ <- judgeUntil("A's record Hung", r)(_ => recordOf(r.idA).map(_ == Hung))
      _ = gate.open()
      healed <- judgeUntil("A Healthy again", r)(a => Future.successful(a.status == Healthy))
      _ <- judgeUntil("A's record Healthy", r)(_ => recordOf(r.idA).map(_ == Healthy))
      recordB <- recordOf(r.idB)
      _ <- quiesce()
    yield
      hung.evidence should include("probe in flight")
      // The probe's own attempt is stamped in the observer before the gate, so it is listed in flight
      hung.evidence should include("HealthProbe")
      healed.since should be > hung.since
      bSeen should not be empty
      bSeen.filter(_.status != Healthy) should be(Nil)
      recordB should be(Healthy)
