package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, Read, ReadResponse, TxAccept, TxAcceptResponse,
  TxMessage, TxPrepare, TxUnknownStore}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, FullObject, Metadata, ObjectId, ObjectRefcount,
  ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription,
  TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, StoreRecoveryState, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.crl.simple.{GrowthPolicy, SimpleCRL}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.StoreIncarnationFile
import org.aspen_ddp.aspen.server.store.backend.{CompletionHandler, MapBackend}
import org.aspen_ddp.aspen.server.store.cache.{HostObjectCache, ObjectCache}
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionStatusCache}
import org.aspen_ddp.aspen.server.transfer.StoreTransferService
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

object StoreRouterSuite:
  private val Bound = 10L // seconds

  private val poolId = PoolId(new UUID(15, 1))
  private val deviceId = StorageDeviceId(new UUID(15, 2))
  private val clientId = ClientId(new UUID(15, 3))
  private val oid = ObjectId(new UUID(15, 4))
  private val ptr = DataObjectPointer(oid, poolId, Array[Byte]())
  private val rev0 = ObjectRevision(TransactionId(new UUID(15, 5)))
  private val ida = Replication(3, 2)

  /** Started now: a transaction past its expiry is frozen, and its acceptor answers nothing (Tx.frozen) */
  private def overwriteTxd(id: TransactionId): TransactionDescription =
    TransactionDescription(id, HLCTimestamp.now, ptr, 0,
      List(DataUpdate(ptr, rev0, DataUpdateOperation.Overwrite)), Nil, None, Nil, Nil, ida, Map(poolId -> ida))

  /** Records every message sent, with the sending thread's name, and the backend's closed flag at send time. */
  private class RecordingNet extends Messenger:
    val tx = new ConcurrentLinkedQueue[(TxMessage, String)]()
    val client = new ConcurrentLinkedQueue[ClientResponse]()
    val sentAfterClose = new AtomicInteger(0)
    @volatile var backend: Option[TrackingBackend] = None
    val acceptResponse = new CountDownLatch(1)

    private def check(): Unit =
      if backend.exists(_.closed) then
        sentAfterClose.incrementAndGet()

    override def sendClientResponse(msg: ClientResponse): Unit =
      check()
      client.add(msg)

    override def sendTransactionMessage(msg: TxMessage): Unit =
      check()
      tx.add((msg, Thread.currentThread.getName))
      if msg.isInstanceOf[TxAcceptResponse] then
        acceptResponse.countDown()

    override def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)

    override def dropCacheForStore(storeId: StoreId): Unit = ()

  /** A MapBackend in its own directory that counts every use after its close. */
  private class TrackingBackend(storeId: StoreId, dir: os.Path) extends MapBackend(storeId):
    @volatile var closed = false
    val usedAfterClose = new AtomicInteger(0)
    val reads = new AtomicInteger(0)

    override def path: os.Path = dir
    // Every store directory has its incarnation file from its creation
    if !os.exists(dir / StoreIncarnationFile.FileName) then StoreIncarnationFile.writeFresh(dir)

    override def close(): Future[Unit] =
      closed = true
      Future.unit

    override def read(pointer: org.aspen_ddp.aspen.common.objects.ObjectPointer): Unit =
      if closed then
        usedAfterClose.incrementAndGet()
      reads.incrementAndGet()
      super.read(pointer)

  /** A TrackingBackend whose completions are handed to `completer`, a thread of its own, as an FSIF or RocksDB
   *  backend's arrive from their I/O threads. */
  private class AsyncCompletingBackend(storeId: StoreId, dir: os.Path, completer: java.util.concurrent.Executor)
    extends TrackingBackend(storeId, dir):
    override def setCompletionHandler(handler: CompletionHandler): Unit =
      super.setCompletionHandler(op => completer.execute(() => handler.complete(op)))

  /** A TrackingBackend whose setCompletionHandler throws, failing a LoadStore after its executor is assigned. */
  private class UnwirableBackend(storeId: StoreId, dir: os.Path) extends TrackingBackend(storeId, dir):
    override def setCompletionHandler(handler: CompletionHandler): Unit =
      throw new IllegalStateException("injected setCompletionHandler failure")

  /** An in-memory CRL whose loadStore waits for `gate`. */
  private class GatedCrl(gate: Future[Unit]) extends CrashRecoveryLog:
    private var states: Map[StoreId, Map[TransactionId, TransactionRecoveryState]] = Map()

    def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = synchronized:
      states.getOrElse(storeId, Map()).values.toList

    override def loadStore(storeId: StoreId, state: StoreRecoveryState): Future[Unit] =
      gate.flatMap(_ => super.loadStore(storeId, state))(using ExecutionContext.parasitic)

    def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
      synchronized:
        states += state.storeId -> (states.getOrElse(state.storeId, Map()) + (txid -> state))
      completionHandler()

    def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

    def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = synchronized:
      states += storeId -> (states.getOrElse(storeId, Map()) - txid)

    def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = Map()
    def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
      completionHandler()
    def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit = ()

  /** A DeviceControl on a StoreThreadPool with what loading, routing and closing a store use. Its posted events run
   *  the StoreLoader handlers on the control thread. Its router is ready (as after a Host's startup scan) unless
   *  `ready` is false. */
  private class Fixture(crl: CrashRecoveryLog, ready: Boolean,
                        objectCacheFactory: (StoreId, ExecutionContext) => ObjectCache,
                        txDriverFactory: TransactionDriver.Factory):
    val threading = new StoreThreadPool(maxThreads = 2)
    val net = new RecordingNet

    val ctx: DeviceControl = new DeviceControl(
      metadata = null, scheduler = null, backends = null, devices = new LocalDevices(), client = null,
      hostId = HostId.BootstrapHostId, aspenSystemId = new UUID(0, 0), rootDir = os.root, net = net,
      backgroundTasks = BackgroundTaskManager.NoBackgroundTaskManager, threading = threading,
      post = event => threading.controlEvents.execute(() => handle(event)),
      crlFactory = null, unloadedDeviceCrl = Some(crl), txStatusCache = new TransactionStatusCache(), wrapCompletionHandler = identity,
      objectCacheFactory = objectCacheFactory,
      finalizerFactory = null,
      txDriverFactory = txDriverFactory, heartbeatPeriod = Duration(5, SECONDS),
      deviceScanPeriod = Duration(10, SECONDS), storeRebuildFactory = null, maxConcurrentRebuilds = 2,
      loadStore = null, tryLoadStore = null, loadStoreById = null, warn = null,
      storeTransferService = StoreTransferService.Disabled, shutdownWorkTimeout = Duration(60, SECONDS),
      gossipStoreOnline = null, closeStoreForTransferOut = null, closeStoreForShutdown = null,
      scheduleShutdownWorkTimeout = null, scheduleEndpointRetry = null, ec = ExecutionContext.global)

    private def handle(event: Host.Event): Unit = event match
      case e: Host.LoadStore => ctx.loader.handleLoad(e)
      case e: Host.ShutdownStore => ctx.loader.handleClose(e)
      case e: Host.UnloadFailedStore => ctx.loader.handleUnloadFailed(e)
      case e: Host.SaveFileRemovalFailed => ctx.loader.handleSaveFileRemovalFailed(e)
      case _ => ()

    if ready then
      ctx.router.markReady()
    threading.start()

    def load(backend: MapBackend): Future[Unit] =
      val p = Promise[Unit]()
      ctx.post(Host.LoadStore(deviceId, backend, None, p))
      p.future

    /** Returns once every control task posted before it has run. */
    def awaitControl(): Unit =
      val done = new CountDownLatch(1)
      threading.controlEvents.execute(() => done.countDown())
      assert(done.await(Bound, TimeUnit.SECONDS))

    /** Runs `f` on the control thread and returns its result. */
    def onControl[T](f: => T): T =
      val p = Promise[T]()
      threading.controlEvents.execute(() => p.complete(scala.util.Try(f)))
      Await.result(p.future, Duration(Bound, SECONDS))

    /** A loaded device, onto which `storeId` is loaded (its copy is learned at its LoadStore) */
    def addDevice(storeId: StoreId): LocalStorageDeviceState = onControl:
      val dir = os.temp.dir()
      val sds = new LocalStorageDeviceState(deviceId, dir, dir / "device.cfg", crl)
      ctx.copyRegistry.attach(sds)
      ctx.devices.devices += deviceId -> sds
      sds

    /** Returns once every task posted to `executor` before it has run. */
    def awaitExecutor(executor: ExecutionContext): Unit =
      val done = new CountDownLatch(1)
      executor.execute(() => done.countDown())
      assert(done.await(Bound, TimeUnit.SECONDS))

    def shutdown(): Unit = threading.shutdown()

  private def withRouter(crl: CrashRecoveryLog, ready: Boolean = true,
                         objectCacheFactory: (StoreId, ExecutionContext) => ObjectCache = (_, _) => ObjectCache.NoCache,
                         txDriverFactory: TransactionDriver.Factory = TransactionDriver.noErrorRecoveryFactory)
                        (f: Fixture => Unit): Unit =
    val fx = new Fixture(crl, ready, objectCacheFactory, txDriverFactory)
    try f(fx) finally fx.shutdown()

  private def emptyCrl: CrashRecoveryLog = new GatedCrl(Future.unit)

  /** A GatedCrl whose loadStore waits for `loadGate`, whose readStoreState waits for `readGate` as set when it is
   *  called, and whose transactions take at most `maxTx` CRL bytes */
  /** `maxLimit` is its admission limit at its growth cap: 0, below any device admission's limit, for no growth */
  private class ReadGatedCrl(loadGate: Future[Unit] = Future.unit, maxTx: Long = Long.MaxValue, maxLimit: Long = 0)
    extends GatedCrl(loadGate):
    @volatile var readGate: Future[Unit] = Future.unit
    override def maxTransactionBytes: Long = maxTx
    override def maxAdmissionLimitBytes: Long = maxLimit
    /** How many times growth was requested */
    val growthRequests = new java.util.concurrent.atomic.AtomicInteger(0)
    override def requestGrowth(): Unit = growthRequests.incrementAndGet()
    override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
      readGate.flatMap(_ => super.readStoreState(storeId))(using ExecutionContext.parasitic)

  /** A commit vote of `s0` overwriting `oid` with `dataSize` bytes */
  private def voteState(s0: StoreId, txid: TransactionId, dataSize: Int = 1): TransactionRecoveryState =
    TransactionRecoveryState(s0, overwriteTxd(txid).serialize(), List(ObjectUpdate(oid, DataBuffer(new Array[Byte](dataSize)))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial)

  /** Waits, bounded, until `cond` holds */
  private def awaitTrue(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + Bound * 1_000_000_000L
    while !cond && System.nanoTime() < deadline do Thread.sleep(1)
    assert(cond)

  /** Records every driver it creates and whether it was shut down (Store.close stops them) */
  private class RecordingDriverFactory extends TransactionDriver.Factory:
    val created = new ConcurrentLinkedQueue[RecordingDriver]()

    class RecordingDriver(ec: ExecutionContext, storeId: StoreId, messenger: Messenger,
                          backgroundTasks: BackgroundTaskManager, txd: TransactionDescription,
                          finalizerFactory: org.aspen_ddp.aspen.server.transaction.TransactionFinalizer.Factory,
                          start: TransactionDriver.Start)
      extends TransactionDriver.noErrorRecoveryFactory.NoRecoveryTransactionDriver(ec, storeId, messenger,
        backgroundTasks, txd, finalizerFactory, start):
      @volatile var stopped = false
      override def shutdown(): Unit =
        stopped = true
        super.shutdown()

    def create(ec: ExecutionContext, storeId: StoreId, messenger: Messenger, backgroundTasks: BackgroundTaskManager,
               txd: TransactionDescription,
               finalizerFactory: org.aspen_ddp.aspen.server.transaction.TransactionFinalizer.Factory,
               start: TransactionDriver.Start): TransactionDriver =
      val d = new RecordingDriver(ec, storeId, messenger, backgroundTasks, txd, finalizerFactory, start)
      created.add(d)
      d

class StoreRouterSuite extends AnyFunSuite with Matchers:
  import StoreRouterSuite.*

  private def await[T](f: Future[T]): T = Await.result(f, Duration(Bound, SECONDS))

  /** A store's backend in `dir` (a fresh temp directory by default, a bare directory no device holds) */
  private def bootstrappedBackend(storeId: StoreId, dir: os.Path = os.temp.dir()): TrackingBackend =
    os.makeDir.all(dir)
    val backend = new TrackingBackend(storeId, dir)
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](7)))
    backend

  private def read(storeId: StoreId): Read = Read(storeId, clientId, UUID.randomUUID(), ptr, FullObject())

  private def objectPtr(i: Int): DataObjectPointer = DataObjectPointer(ObjectId(new UUID(16, i)), poolId, Array[Byte]())

  /** The load failed before its store was installed: nothing serves it, it is offline, its copy is neither served nor
   *  opening, and its backend is closed. */
  private def assertAbandoned(fx: Fixture, s0: StoreId, sds: LocalStorageDeviceState, backend: TrackingBackend): Unit =
    val (entry, offline, loaded, offlineInDevice) = fx.onControl:
      (fx.ctx.router.entry(s0), fx.ctx.router.isOffline(s0), fx.ctx.copyRegistry.openCopies(sds).contains(s0),
        fx.ctx.copyRegistry.offlineCopies(sds).contains(s0))
    entry should be(None)
    offline should be(true)
    loaded should be(false)
    offlineInDevice should be(true)
    backend.closed should be(true)

  test("close racing traffic gets offline handling"):
    val s0 = StoreId(poolId, 0)
    withRouter(emptyCrl): fx =>
      val backend = bootstrappedBackend(s0)
      fx.net.backend = Some(backend)
      await(fx.load(backend))
      val executor = fx.ctx.router.entry(s0).get.executor

      // The store serves before the race starts
      fx.ctx.router.deliverClient(read(s0))
      fx.awaitExecutor(executor)
      fx.awaitExecutor(executor)
      fx.net.client.size should be(1)

      val Senders = 4
      val PerSender = 1250
      val firstThousand = new CountDownLatch(1000)
      val failed = new AtomicBoolean(false)
      val senders = (0 until Senders).map: _ =>
        new Thread(() =>
          try
            for _ <- 0 until PerSender do
              fx.ctx.router.deliverClient(read(s0))
              firstThousand.countDown()
          catch case _: Throwable => failed.set(true))
      senders.foreach(_.start())
      firstThousand.await(Bound, TimeUnit.SECONDS) should be(true)
      val closed = fx.ctx.loader.closeStore(s0, extractCrl = false)
      await(closed)
      senders.foreach(_.join(TimeUnit.SECONDS.toMillis(Bound)))
      senders.exists(_.isAlive) should be(false)
      failed.get should be(false)
      // Everything the senders queued has run
      fx.awaitExecutor(executor)

      backend.closed should be(true)
      backend.usedAfterClose.get should be(0)
      fx.net.sentAfterClose.get should be(0)
      val responses = fx.net.client.asScala.toList.collect { case r: ReadResponse => r }
      responses.size should be(fx.net.client.size)
      responses.size should be <= (Senders * PerSender + 1)
      responses.map(_.readUUID).distinct.size should be(responses.size)
      responses.filter(_.result.isLeft) should be(empty)
      responses.exists(_.result == Left(ReadError.StoreNotFound)) should be(false)
      fx.ctx.router.isOffline(s0) should be(true)
      fx.ctx.router.entry(s0) should be(None)

  test("messages during CRL recovery are held until recover"):
    val s0 = StoreId(poolId, 0)
    val peer = StoreId(poolId, 1)
    val txid = TransactionId(new UUID(15, 6))
    val gate = Promise[Unit]()
    withRouter(new GatedCrl(gate.future)): fx =>
      val backend = bootstrappedBackend(s0)
      val trs = TransactionRecoveryState(s0, overwriteTxd(txid).serialize(),
        List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), TransactionDisposition.VoteCommit,
        TransactionStatus.Unresolved, PersistentState.initial)
      CrashRecoveryLog.saveStoreState(s0, StoreRecoveryState(List(trs)), backend.crlSaveFile)

      val loaded = fx.load(backend)
      // The LoadStore handler has run: the store is installed and its CRL load waits on the gate
      fx.awaitControl()
      val executor = fx.ctx.router.entry(s0).get.executor
      loaded.isCompleted should be(false)

      fx.ctx.router.deliverTx(TxAccept(s0, peer, txid, ProposalId(1, 0), value = true))
      fx.awaitExecutor(executor)
      fx.net.tx.asScala.map(_._1).collect { case m: TxAcceptResponse => m } should be(empty)

      gate.success(())
      await(loaded)
      fx.net.acceptResponse.await(Bound, TimeUnit.SECONDS) should be(true)
      fx.awaitExecutor(executor)
      val responses = fx.net.tx.asScala.map(_._1).collect { case m: TxAcceptResponse => m }.toList
      responses.map(_.transactionId) should be(List(txid))
      os.exists(backend.crlSaveFile) should be(false)

  // crash-recovery-log 3.3 step 4: a failed durable removal of crl_save.log after its load. The store directory's
  // mode makes it fail: r-x refuses the unlink; -wx allows it but refuses opening the directory to force it
  for (what, mode, fileKept) <- List(("its unlink", "r-x------", true), ("its directory force", "-wx------", false)) do
    test(s"a crl_save.log whose removal fails at $what unloads the store, keeps its CRL entries, and retries"):
      val s0 = StoreId(poolId, 0)
      val txid = TransactionId(new UUID(15, 30))
      val gate = Promise[Unit]()
      val crl = new GatedCrl(gate.future)
      withRouter(crl): fx =>
        val sds = fx.addDevice(s0)
        val backend = bootstrappedBackend(s0)
        CrashRecoveryLog.saveStoreState(s0, StoreRecoveryState(List(voteState(s0, txid))), backend.crlSaveFile)
        val loaded = fx.load(backend)
        // Installed; its CRL load waits on the gate
        fx.awaitControl()
        os.perms.set(backend.path, mode)
        try
          gate.success(())
          intercept[Exception](await(loaded))
          // Its executor slot is released once its close has completed
          awaitTrue(fx.onControl(fx.threading.threadOf(s0).isEmpty))
          assertAbandoned(fx, s0, sds, backend)
          os.exists(backend.crlSaveFile) should be(fileKept)
          // Not dropped: with the file unlinked they are the store's whole state
          crl.getFullRecoveryState(s0).map(_.txd.transactionId) should be(List(txid))
          fx.onControl(fx.ctx.loader.awaitingRetryHasCallback(s0)) should be(Some(false))

          // A retry while the directory still cannot be forced fails the same way, even with the file gone
          val again = new TrackingBackend(s0, backend.path)
          intercept[Exception](await(fx.load(again)))
          awaitTrue(fx.onControl(fx.threading.threadOf(s0).isEmpty))
          assertAbandoned(fx, s0, sds, again)
        finally os.perms.set(backend.path, "rwx------")

        // Once the directory is forced the retry serves the store, recovered from its device-CRL entries
        val retried = new TrackingBackend(s0, backend.path)
        await(fx.load(retried))
        os.exists(backend.crlSaveFile) should be(false)
        val store = fx.onControl(fx.ctx.router.entry(s0)).get.store
        awaitTrue(!store.frontend.recoveryPending)
        store.holdsTransaction(txid) should be(true)

  test("unknown store gets TxUnknownStore from the calling thread"):
    val unknown = StoreId(poolId, 2)
    val peer = StoreId(poolId, 1)
    val txid = TransactionId(new UUID(15, 7))
    withRouter(emptyCrl): fx =>
      fx.ctx.router.deliverTx(TxPrepare(unknown, peer, overwriteTxd(txid), ProposalId.initialProposal(0),
        Nil, true))
      fx.net.tx.asScala.toList should be(List((TxUnknownStore(peer, unknown, txid), Thread.currentThread.getName)))

  test("tasks queued ahead of a close run against the open store"):
    val s0 = StoreId(poolId, 0)
    // No object cache: every queued read needs backend I/O, whose completion arrives after the store is removed
    withRouter(emptyCrl): fx =>
      val backend = bootstrappedBackend(s0)
      await(fx.load(backend))
      val executor = fx.ctx.router.entry(s0).get.executor

      val unblock = new CountDownLatch(1)
      executor.execute(() => { unblock.await(Bound, TimeUnit.SECONDS); () })
      val Queued = 10
      val reads = (0 until Queued).map(_ => read(s0))
      reads.foreach(fx.ctx.router.deliverClient)
      val closed = fx.ctx.loader.closeStore(s0, extractCrl = false)
      // handleClose has run: the store is removed and its close queued behind the reads
      fx.awaitControl()
      fx.ctx.router.entry(s0) should be(None)
      unblock.countDown()
      await(closed)

      val answered = fx.net.client.asScala.toList.collect { case r: ReadResponse => r }
      answered.map(_.readUUID).toSet should be(reads.map(_.readUUID).toSet)
      answered.size should be(Queued)
      answered.forall(_.result.isRight) should be(true)
      backend.closed should be(true)

  test("a message for an unknown store gets no reply until the router is ready"):
    val unknown = StoreId(poolId, 2)
    val peer = StoreId(poolId, 1)
    val txid = TransactionId(new UUID(15, 8))
    withRouter(emptyCrl, ready = false): fx =>
      def prepare = TxPrepare(unknown, peer, overwriteTxd(txid), ProposalId.initialProposal(0), Nil, true)
      fx.ctx.router.deliverTx(prepare)
      fx.net.tx.asScala.toList should be(empty)
      fx.ctx.router.markReady()
      fx.ctx.router.deliverTx(prepare)
      fx.net.tx.asScala.toList.map(_._1) should be(List(TxUnknownStore(peer, unknown, txid)))

  test("a store whose device is undecided gets no reply until its device is decided"):
    val unknown = StoreId(poolId, 2)
    val peer = StoreId(poolId, 1)
    val txid = TransactionId(new UUID(15, 9))
    withRouter(emptyCrl): fx =>
      def prepare = TxPrepare(unknown, peer, overwriteTxd(txid), ProposalId.initialProposal(0), Nil, true)
      val router = fx.ctx.router
      val undecided = os.temp.dir()
      fx.onControl(fx.ctx.copyRegistry.awaitDevice(undecided, Set(unknown)))
      router.deliverTx(prepare)
      router.deliverClient(read(unknown))
      fx.awaitControl()
      fx.net.tx.asScala.toList should be(empty)
      fx.net.client.asScala.toList should be(empty)
      fx.ctx.currentView.offline should be(Set(unknown))

      fx.onControl(fx.ctx.copyRegistry.deviceDecided(undecided))
      router.deliverTx(prepare)
      router.deliverClient(read(unknown))
      fx.net.tx.asScala.toList.map(_._1) should be(List(TxUnknownStore(peer, unknown, txid)))
      fx.net.client.asScala.toList.collect { case r: ReadResponse => r.result } should be(
        List(Left(ReadError.StoreNotFound)))

  test("installing a store serves it, silent no more"):
    val s0 = StoreId(poolId, 0)
    withRouter(emptyCrl): fx =>
      val sds = fx.addDevice(s0)
      await(fx.load(bootstrappedBackend(s0)))
      fx.onControl(fx.ctx.router.answersUnknown(s0)) should be(false)
      fx.onControl(fx.ctx.copyRegistry.silence(s0)) should be(Silence.Served)
      fx.ctx.currentView.offline should be(empty)

  test("a load whose store fails to build releases what its recovered transactions were counted for"):
    val s0 = StoreId(poolId, 0)
    val crl = emptyCrl
    crl.save(TransactionId(new UUID(15, 7)), TransactionRecoveryState(s0,
      overwriteTxd(TransactionId(new UUID(15, 7))).serialize(), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial), () => ())
    val admission = new CrlAdmission(Long.MaxValue)
    withRouter(crl): fx =>
      fx.onControl:
        val dir = os.temp.dir()
        val sds = new LocalStorageDeviceState(deviceId, dir, dir / "device.cfg", crl, admission)
        fx.ctx.copyRegistry.attach(sds)
        fx.ctx.devices.devices += deviceId -> sds
      intercept[IllegalStateException](await(fx.load(new UnwirableBackend(s0, os.temp.dir()))))
      admission.admittedBytes should be(0)

  test("a load whose store fails to build is undone and the store can load again"):
    val s0 = StoreId(poolId, 0)
    withRouter(emptyCrl): fx =>
      val sds = fx.addDevice(s0)
      val failing = new UnwirableBackend(s0, os.temp.dir())
      val err = intercept[IllegalStateException](await(fx.load(failing)))
      err.getMessage should be("injected setCompletionHandler failure")
      assertAbandoned(fx, s0, sds, failing)
      // Its executor slot was released
      fx.onControl(fx.threading.assignedCounts.sum) should be(0)
      // Retried at the next device scan, then with backoff per build failure in a row (SL-W17)
      fx.onControl(fx.ctx.loader.awaitingRetryHasCallback(s0)) should be(Some(false))
      fx.onControl(fx.ctx.loader.awaitingRetryWaitScans(s0)) should be(Some(1))
      intercept[IllegalStateException](await(fx.load(new UnwirableBackend(s0, os.temp.dir()))))
      fx.onControl(fx.ctx.loader.awaitingRetryWaitScans(s0)) should be(Some(2))

      val backend = bootstrappedBackend(s0)
      await(fx.load(backend))
      fx.onControl(fx.ctx.router.entry(s0).map(_.store.backend)) should be(Some(backend))
      fx.onControl(fx.threading.assignedCounts.sum) should be(1)

      // A failed rebuild over the served store leaves it serving, awaiting no retry, and the backoff starts over
      fx.onControl(fx.ctx.loader.retryFailedLoads())
      fx.onControl(fx.ctx.loader.awaitingRetryWaitScans(s0)) should be(None)
      intercept[IllegalStateException](await(fx.load(new UnwirableBackend(s0, os.temp.dir()))))
      fx.onControl(fx.ctx.router.entry(s0).map(_.store.backend)) should be(Some(backend))
      fx.onControl(fx.ctx.loader.awaitingRetryWaitScans(s0)) should be(None)

  test("a reload over a loaded store closes the old one, releasing what its transactions were counted for"):
    val s0 = StoreId(poolId, 0)
    val crl = emptyCrl
    crl.save(TransactionId(new UUID(15, 8)), TransactionRecoveryState(s0,
      overwriteTxd(TransactionId(new UUID(15, 8))).serialize(), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial), () => ())
    val admission = new CrlAdmission(Long.MaxValue)
    withRouter(crl): fx =>
      fx.onControl:
        val dir = os.temp.dir()
        val sds = new LocalStorageDeviceState(deviceId, dir, dir / "device.cfg", crl, admission)
        fx.ctx.copyRegistry.attach(sds)
        fx.ctx.devices.devices += deviceId -> sds
      val first = bootstrappedBackend(s0)
      await(fx.load(first))
      val executor = fx.ctx.router.entry(s0).get.executor
      // Recovered from the device CRL on the store's executor, after the load completes
      fx.awaitExecutor(executor)
      val once = admission.admittedBytes
      once should be > 0L
      val second = bootstrappedBackend(s0)
      await(fx.load(second))
      fx.awaitExecutor(executor)
      fx.awaitExecutor(executor)
      // The recovered transaction is counted once, by the new store
      admission.admittedBytes should be(once)
      first.closed should be(true)
      second.closed should be(false)

  /** A device over `crl` counting with `admission`, onto which `storeId` is loaded */
  /** Whether the load retired the copy of `s0` on `sds` unserved (StoreRebuilds.retireUnserved): any move to lost/
   *  made or attempted, its copy's record Lost or held for the move's retry (CopyMoves.pendingLost). The move runs in
   *  a control task once the backend's close completes, queued before the load's promise fails */
  private def retiredLost(fx: Fixture, sds: LocalStorageDeviceState, s0: StoreId): Boolean = fx.onControl:
    fx.ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(s0)).exists(_.phase == Phase.Lost) ||
      fx.ctx.moves.isPendingLost(sds, s0)

  /** Whether the copy of `s0` on `sds` is in lost/, on disk and in its record, nothing left at its flat path, and not
   *  held for a move's retry */
  private def inLost(fx: Fixture, sds: LocalStorageDeviceState, s0: StoreId): Boolean = fx.onControl:
    fx.ctx.copyRegistry.copiesOf(sds).flatMap(_.copies.get(s0)).exists(_.phase == Phase.Lost) &&
      !fx.ctx.moves.isPendingLost(sds, s0) &&
      CopyFixtures.phaseOf(sds.devicePath, s0).contains(Phase.Lost) &&
      CopyFixtures.copyPath(sds.devicePath, s0).exists(_ / os.up == sds.devicePath / CopyLayout.LostDirectory) &&
      !os.exists(CopyLayout.path(sds.devicePath, s0, None, Phase.Live))

  private def addCountingDevice(fx: Fixture, storeId: StoreId, crl: CrashRecoveryLog,
                                admission: CrlAdmission): LocalStorageDeviceState = fx.onControl:
    val dir = os.temp.dir()
    val sds = new LocalStorageDeviceState(deviceId, dir, dir / "device.cfg", crl, admission)
    fx.ctx.copyRegistry.attach(sds)
    fx.ctx.devices.devices += deviceId -> sds
    sds

  // CRL admission rule A2 (crash-recovery-log 6.1): nothing live is uncounted across a load
  test("a loading store's recovered state is counted before what the device's open retained for it is released"):
    val s0 = StoreId(poolId, 0)
    val crl = new ReadGatedCrl
    val trs = voteState(s0, TransactionId(new UUID(15, 9)))
    crl.save(TransactionId(new UUID(15, 9)), trs, () => ())
    val bytes = CrlAdmission.recoveredBytes(trs)
    val admission = new CrlAdmission(Long.MaxValue)
    // As the device's open counts what its CRL holds
    admission.chargeRetained(s0, bytes)
    val read = Promise[Unit]()
    crl.readGate = read.future
    withRouter(crl): fx =>
      addCountingDevice(fx, s0, crl, admission)
      await(fx.load(bootstrappedBackend(s0)))
      fx.awaitExecutor(fx.ctx.router.entry(s0).get.executor)
      // Installed, its CRL read outstanding: its records are still counted
      admission.admittedBytes should be(bytes)
      read.success(())
      awaitTrue(admission.retainedBytes(s0) == 0)
      // Counted once, by the store
      admission.admittedBytes should be(bytes)

  test("a reload over a loaded store keeps its records counted until the new store has counted them"):
    val s0 = StoreId(poolId, 0)
    val crl = new ReadGatedCrl
    val trs = voteState(s0, TransactionId(new UUID(15, 10)))
    crl.save(TransactionId(new UUID(15, 10)), trs, () => ())
    val bytes = CrlAdmission.recoveredBytes(trs)
    val admission = new CrlAdmission(Long.MaxValue)
    withRouter(crl): fx =>
      addCountingDevice(fx, s0, crl, admission)
      val first = bootstrappedBackend(s0)
      await(fx.load(first))
      awaitTrue(admission.admittedBytes == bytes)
      val executor = fx.ctx.router.entry(s0).get.executor
      val read = Promise[Unit]()
      crl.readGate = read.future
      await(fx.load(bootstrappedBackend(s0)))
      fx.awaitExecutor(executor)
      // The old store is closed on the executor, the new one's CRL read outstanding: still counted
      first.closed should be(true)
      admission.admittedBytes should be(bytes)
      read.success(())
      awaitTrue(admission.retainedBytes(s0) == 0)
      admission.admittedBytes should be(bytes)

  // CRL admission rule A3 (crash-recovery-log 6.1): a crl_save.log state is charged, checked, before it is saved
  test("a crl_save.log state that fits is charged before it is saved, and counted once after recovery"):
    val s0 = StoreId(poolId, 0)
    val gate = Promise[Unit]()
    val crl = new ReadGatedCrl(gate.future, maxTx = 10000)
    val trs = voteState(s0, TransactionId(new UUID(15, 11)))
    val bytes = CrlAdmission.recoveredBytes(trs)
    val admission = new CrlAdmission(1000)
    withRouter(crl): fx =>
      addCountingDevice(fx, s0, crl, admission)
      val backend = bootstrappedBackend(s0)
      CrashRecoveryLog.saveStoreState(s0, StoreRecoveryState(List(trs)), backend.crlSaveFile)
      val loaded = fx.load(backend)
      fx.awaitControl()
      // The save waits on the gate: charged already, retained for the store
      admission.admittedBytes should be(bytes)
      admission.retainedBytes(s0) should be(bytes)
      gate.success(())
      await(loaded)
      awaitTrue(admission.retainedBytes(s0) == 0)
      admission.admittedBytes should be(bytes)
      os.exists(backend.crlSaveFile) should be(false)

  test("a crl_save.log state that does not fit now is refused: the store stays offline, its file kept, and retried"):
    val s0 = StoreId(poolId, 0)
    val crl = new ReadGatedCrl(maxTx = 10000)
    val trs = voteState(s0, TransactionId(new UUID(15, 12)))
    val bytes = CrlAdmission.recoveredBytes(trs)
    val admission = new CrlAdmission(1000)
    // Other stores' transactions leave less than the state's bytes below limit + T
    admission.charge(1000 + 10000 - bytes + 1)
    val before = admission.admittedBytes
    withRouter(crl): fx =>
      val sds = addCountingDevice(fx, s0, crl, admission)
      val backend = bootstrappedBackend(s0)
      CrashRecoveryLog.saveStoreState(s0, StoreRecoveryState(List(trs)), backend.crlSaveFile)
      intercept[Exception](await(fx.load(backend)))
      assertAbandoned(fx, s0, sds, backend)
      admission.admittedBytes should be(before)
      admission.retainedBytes(s0) should be(0)
      os.exists(backend.crlSaveFile) should be(true)
      retiredLost(fx, sds, s0) should be(false)
      crl.getFullRecoveryState(s0) should be(empty)
      // The full CRL is asked to grow
      crl.growthRequests.get should be(1)
      // Retried at the device scans, as a store awaiting space is
      fx.onControl(fx.ctx.loader.awaitingRetryHasCallback(s0)) should be(Some(false))

  // CRL-W6: never-fits is judged against the limit the CRL reaches at its growth cap, not the one now
  test("a crl_save.log state above limit + T but within the grown limit waits offline for growth, not rebuilt"):
    val s0 = StoreId(poolId, 0)
    val crl = new ReadGatedCrl(maxTx = 10000, maxLimit = 100000)
    val trs = (1 to 3).toList.map(i => voteState(s0, TransactionId(new UUID(17, 20 + i)), 4000))
    val admission = new CrlAdmission(100)
    CrlAdmission.recoveredBytes(StoreRecoveryState(trs)) should be > (100L + 10000L)
    withRouter(crl): fx =>
      val sds = addCountingDevice(fx, s0, crl, admission)
      val backend = bootstrappedBackend(s0)
      CrashRecoveryLog.saveStoreState(s0, StoreRecoveryState(trs), backend.crlSaveFile)
      intercept[Exception](await(fx.load(backend)))
      assertAbandoned(fx, s0, sds, backend)
      admission.admittedBytes should be(0)
      os.exists(backend.crlSaveFile) should be(true)
      retiredLost(fx, sds, s0) should be(false)
      crl.growthRequests.get should be(1)
      fx.onControl(fx.ctx.loader.awaitingRetryHasCallback(s0)) should be(Some(false))

  // CRL-W9: the growth cap a device config sets is no bound on what the CRL may ever grow to: a lower or invalid
  // crl-max-streams (resolveExisting disables growth for the latter) must not declare a state lost
  for (what, policy) <- List(
    "growth disabled by an invalid crl-max-streams" -> GrowthPolicy.Disabled,
    "crl-max-streams lowered to the ring" -> GrowthPolicy(4, 5, () => 1L << 40, () => 1L << 39)) do
    test(s"a crl_save.log state above the limit at the configured cap + T waits offline, $what"):
      val s0 = StoreId(poolId, 0)
      val crl = SimpleCRL(os.temp.dir(), 4, 64 * 1024L, growth = policy).crl
      try
        val trs = (1 to 5).toList.map(i => voteState(s0, TransactionId(new UUID(17, 40 + i)), 20000))
        val state = StoreRecoveryState(trs)
        trs.foreach(t => CrlAdmission.recoveredBytes(t) should be <= crl.maxTransactionBytes)
        CrlAdmission.recoveredBytes(state) should be > (crl.admissionLimitBytes + crl.maxTransactionBytes)
        val admission = new CrlAdmission(crl.admissionLimitBytes)
        withRouter(crl): fx =>
          val sds = addCountingDevice(fx, s0, crl, admission)
          val backend = bootstrappedBackend(s0)
          CrashRecoveryLog.saveStoreState(s0, state, backend.crlSaveFile)
          intercept[Exception](await(fx.load(backend)))
          assertAbandoned(fx, s0, sds, backend)
          admission.admittedBytes should be(0)
          os.exists(backend.crlSaveFile) should be(true)
          retiredLost(fx, sds, s0) should be(false)
          fx.onControl(fx.ctx.loader.awaitingRetryHasCallback(s0)) should be(Some(false))
      finally crl.shutdown()

  for (what, states) <- List(
    "a state above limit + T" -> ((s0: StoreId) => (1 to 3).toList.map(i => voteState(s0, TransactionId(new UUID(17, i)), 4000))),
    "a transaction above T" -> ((s0: StoreId) => List(voteState(s0, TransactionId(new UUID(17, 9)), 20000)))) do
    test(s"a crl_save.log holding $what never fits: its save file is treated as lost and the store rebuilt"):
      val s0 = StoreId(poolId, 0)
      val crl = new ReadGatedCrl(maxTx = 10000)
      val admission = new CrlAdmission(if what.contains("limit") then 100 else Long.MaxValue)
      withRouter(crl): fx =>
        val sds = addCountingDevice(fx, s0, crl, admission)
        // At its flat path on the device, so its retire moves it into lost/
        val backend = bootstrappedBackend(s0, CopyLayout.path(sds.devicePath, s0, None, Phase.Live))
        CrashRecoveryLog.saveStoreState(s0, StoreRecoveryState(states(s0)), backend.crlSaveFile)
        intercept[Exception](await(fx.load(backend)))
        assertAbandoned(fx, s0, sds, backend)
        admission.admittedBytes should be(0)
        inLost(fx, sds, s0) should be(true)
        crl.getFullRecoveryState(s0) should be(empty)
        fx.onControl(fx.ctx.loader.awaitingRetryHasCallback(s0)) should be(None)

  test("a reload over a loaded store leaves its view to its queued tasks, then uncounts it"):
    val s0 = StoreId(poolId, 0)
    val hc = new HostObjectCache(1024 * 1024)
    withRouter(emptyCrl, objectCacheFactory = hc.forStore): fx =>
      val first = bootstrappedBackend(s0)
      await(fx.load(first))
      val executor = fx.ctx.router.entry(s0).get.executor
      fx.ctx.router.deliverClient(read(s0))
      fx.awaitExecutor(executor)
      fx.awaitExecutor(executor)
      first.reads.get should be(1)
      val cached = hc.sizeInBytes
      cached should be > 0L

      // The old store's queued read runs after the reload replaced it in the router
      val unblock = new CountDownLatch(1)
      executor.execute(() => { unblock.await(Bound, TimeUnit.SECONDS); () })
      fx.ctx.router.deliverClient(read(s0))
      val second = bootstrappedBackend(s0)
      await(fx.load(second))
      fx.onControl(fx.ctx.router.entry(s0).map(_.store.backend)) should be(Some(second))
      unblock.countDown()
      fx.awaitExecutor(executor)
      fx.awaitExecutor(executor)

      // Served from the old store's view, which was still its own
      first.reads.get should be(1)
      fx.net.client.asScala.toList.collect { case r: ReadResponse => r.result.isRight } should be(List(true, true))
      // The old view was cleared behind the old store's tasks; the new one holds nothing yet
      hc.sizeInBytes should be(0L)
      hc.viewCount should be(1)

      fx.ctx.router.deliverClient(read(s0))
      fx.awaitExecutor(executor)
      fx.awaitExecutor(executor)
      second.reads.get should be(1)
      hc.sizeInBytes should be(cached)

  test("a load whose store fails to build clears its object cache view"):
    val s0 = StoreId(poolId, 0)
    val hc = new HostObjectCache(1024 * 1024)
    val executors = new ConcurrentLinkedQueue[ExecutionContext]()
    val factory: (StoreId, ExecutionContext) => ObjectCache = (id, ec) =>
      executors.add(ec)
      hc.forStore(id, ec)
    withRouter(emptyCrl, objectCacheFactory = factory): fx =>
      fx.addDevice(s0)
      intercept[IllegalStateException](await(fx.load(new UnwirableBackend(s0, os.temp.dir()))))
      executors.asScala.foreach(fx.awaitExecutor)
      hc.viewCount should be(0)

      await(fx.load(bootstrappedBackend(s0)))
      hc.viewCount should be(1)

  test("a load while the store still holds an executor slot fails without freeing it"):
    val s0 = StoreId(poolId, 0)
    withRouter(emptyCrl): fx =>
      val sds = fx.addDevice(s0)
      // As a close still running leaves it: removed from the router, its slot not yet released
      fx.onControl(fx.threading.assign(s0, deviceId))
      val first = bootstrappedBackend(s0)
      intercept[HostThreading.AlreadyAssigned](await(fx.load(first))) should be(HostThreading.AlreadyAssigned(s0))
      assertAbandoned(fx, s0, sds, first)
      // The slot is the earlier holder's, so the failed load left it in place
      fx.onControl(fx.threading.assignedCounts.sum) should be(1)
      // Retried at the device scans once the slot is free (X-W8)
      fx.onControl(fx.ctx.loader.awaitingRetryHasCallback(s0)) should be(Some(false))

      fx.onControl(fx.threading.release(s0))
      val second = bootstrappedBackend(s0)
      await(fx.load(second))
      fx.onControl(fx.ctx.router.entry(s0).map(_.store.backend)) should be(Some(second))
      fx.onControl(fx.threading.assignedCounts.sum) should be(1)

  test("completions from a backend's own thread: those posted before the close are delivered, later ones dropped"):
    val s0 = StoreId(poolId, 0)
    val PerBatch = 8
    val completer = java.util.concurrent.Executors.newSingleThreadExecutor: r =>
      val t = new Thread(r, "test-completer")
      t.setDaemon(true)
      t
    def awaitCompleter(): Unit =
      val done = new CountDownLatch(1)
      completer.execute(() => done.countDown())
      done.await(Bound, TimeUnit.SECONDS) should be(true)
    try
      withRouter(emptyCrl): fx =>
        val backend = new AsyncCompletingBackend(s0, os.temp.dir(), completer)
        fx.net.backend = Some(backend)
        // One object per read: concurrent reads of one object share a single backend read
        for i <- 0 until 2 * PerBatch do
          backend.bootstrapAllocate(objectPtr(i).id, ObjectType.Data,
            Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)), DataBuffer(Array[Byte](7)))
        await(fx.load(backend))
        val executor = fx.ctx.router.entry(s0).get.executor
        def readOf(i: Int): Read = Read(s0, clientId, UUID.randomUUID(), objectPtr(i), FullObject())

        // The completer holds every completion until a gate opens: before = 0 until PerBatch, after = the rest
        val gateBefore = new CountDownLatch(1)
        val gateAfter = new CountDownLatch(1)
        val beforePosted = new CountDownLatch(1)
        completer.execute(() => { gateBefore.await(Bound, TimeUnit.SECONDS); () })
        val before = (0 until PerBatch).map(readOf)
        before.foreach(fx.ctx.router.deliverClient)
        // Every read of the first batch has reached the backend, so its completion waits behind gateBefore
        fx.awaitExecutor(executor)
        completer.execute: () =>
          beforePosted.countDown()
          gateAfter.await(Bound, TimeUnit.SECONDS)
          ()
        val after = (PerBatch until 2 * PerBatch).map(readOf)
        after.foreach(fx.ctx.router.deliverClient)
        fx.awaitExecutor(executor)
        fx.net.client.size should be(0)

        // The store's executor is held, so the first batch's completions queue on it ahead of the close
        val unblock = new CountDownLatch(1)
        executor.execute(() => { unblock.await(Bound, TimeUnit.SECONDS); () })
        gateBefore.countDown()
        beforePosted.await(Bound, TimeUnit.SECONDS) should be(true)
        val closed = fx.ctx.loader.closeStore(s0, extractCrl = false)
        fx.awaitControl()
        fx.ctx.router.entry(s0) should be(None)
        unblock.countDown()
        await(closed)
        backend.closed should be(true)

        // The second batch's completions are posted only now, after the close
        gateAfter.countDown()
        awaitCompleter()
        fx.awaitExecutor(executor)

        val answered = fx.net.client.asScala.toList.collect { case r: ReadResponse => r }
        answered.map(_.readUUID).toSet should be(before.map(_.readUUID).toSet)
        answered.size should be(PerBatch)
        answered.forall(_.result.isRight) should be(true)
        backend.usedAfterClose.get should be(0)
        fx.net.sentAfterClose.get should be(0)
    finally completer.shutdown()

  /** Starts a driver of a fresh transaction on the store loaded as `storeId`, on its executor, and returns it */
  private def startDriver(fx: Fixture, drivers: RecordingDriverFactory, storeId: StoreId,
                          txid: TransactionId): RecordingDriverFactory#RecordingDriver =
    val entry = fx.onControl(fx.ctx.router.entry(storeId).get)
    val before = drivers.created.size
    entry.executor.execute(() => entry.store.driveTransaction(overwriteTxd(txid)))
    fx.awaitExecutor(entry.executor)
    drivers.created.size should be(before + 1)
    drivers.created.asScala.last

  // A transfer out (closeForTransferOut: extractCrl) and a shutdown or relinquish (closeForShutdown,
  // which closeStoresToRelinquish uses) both close through handleClose, so Store.close stops the store's drivers
  for (what, extractCrl) <- List(("a transfer out", true), ("a shutdown or relinquish", false)) do
    test(s"the close of $what shuts the store's transaction drivers down"):
      val s0 = StoreId(poolId, 0)
      val drivers = new RecordingDriverFactory
      withRouter(emptyCrl, txDriverFactory = drivers): fx =>
        fx.addDevice(s0)
        await(fx.load(bootstrappedBackend(s0)))
        val driver = startDriver(fx, drivers, s0, TransactionId(new UUID(15, 20)))
        driver.stopped should be(false)
        await(fx.ctx.loader.closeStore(s0, extractCrl = extractCrl))
        driver.stopped should be(true)

  // A reload over a loaded store that reuses its backend (TestNetwork.restartStore) skips Store.close,
  // which would close the shared backend; its drivers must still stop, as they would in the crash it stands for
  test("a reload over a loaded store that reuses its backend shuts the old store's drivers down"):
    val s0 = StoreId(poolId, 0)
    val drivers = new RecordingDriverFactory
    withRouter(emptyCrl, txDriverFactory = drivers): fx =>
      fx.addDevice(s0)
      val backend = bootstrappedBackend(s0)
      await(fx.load(backend))
      val executor = fx.onControl(fx.ctx.router.entry(s0).get.executor)
      val driver = startDriver(fx, drivers, s0, TransactionId(new UUID(15, 21)))
      await(fx.load(backend))
      fx.awaitExecutor(executor)
      driver.stopped should be(true)
      backend.closed should be(false)
