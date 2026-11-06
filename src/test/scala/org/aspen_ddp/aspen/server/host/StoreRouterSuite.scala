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
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
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

  private def overwriteTxd(id: TransactionId): TransactionDescription =
    TransactionDescription(id, HLCTimestamp(100), ptr, 0,
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

    override def path: os.Path = dir

    override def close(): Future[Unit] =
      closed = true
      Future.unit

    override def read(pointer: org.aspen_ddp.aspen.common.objects.ObjectPointer): Unit =
      if closed then
        usedAfterClose.incrementAndGet()
      super.read(pointer)

  /** An in-memory CRL whose loadStore waits for `gate`. */
  private class GatedCrl(gate: Future[Unit]) extends CrashRecoveryLog:
    private var states: Map[StoreId, Map[TransactionId, TransactionRecoveryState]] = Map()

    def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = synchronized:
      states.getOrElse(storeId, Map()).values.toList

    def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]] = synchronized:
      val l = getFullRecoveryState(storeId)
      states -= storeId
      Future.successful(l)

    override def loadStore(storeId: StoreId, trsList: List[TransactionRecoveryState]): Future[Unit] =
      gate.flatMap(_ => super.loadStore(storeId, trsList))(using ExecutionContext.parasitic)

    def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
      synchronized:
        states += state.storeId -> (states.getOrElse(state.storeId, Map()) + (txid -> state))
      completionHandler()

    def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

    def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = synchronized:
      states += storeId -> (states.getOrElse(storeId, Map()) - txid)

  /** A DeviceControl on a StoreThreadPool with what loading, routing and closing a store use. Its posted events run
   *  the StoreLoader handlers on the control thread. Its router is ready (as after a Host's startup scan) unless
   *  `ready` is false. */
  private class Fixture(crl: CrashRecoveryLog, ready: Boolean):
    val threading = new StoreThreadPool(maxThreads = 2)
    val net = new RecordingNet

    val ctx: DeviceControl = new DeviceControl(
      metadata = null, scheduler = null, backends = null, devices = new LocalDevices(), client = null,
      hostId = HostId.BootstrapHostId, aspenSystemId = new UUID(0, 0), rootDir = os.root, net = net,
      backgroundTasks = BackgroundTaskManager.NoBackgroundTaskManager, threading = threading,
      post = event => threading.controlEvents.execute(() => handle(event)),
      crl = crl, txStatusCache = new TransactionStatusCache(), wrapCompletionHandler = identity,
      objectCacheFactory = _ => ObjectCache.NoCache,
      finalizerFactory = null,
      txDriverFactory = TransactionDriver.noErrorRecoveryFactory, heartbeatPeriod = Duration(5, SECONDS),
      deviceScanPeriod = Duration(10, SECONDS), storeRebuildFactory = null, maxConcurrentRebuilds = 2,
      loadStore = null, tryLoadStore = null, loadStoreById = null, warn = null,
      storeTransferService = StoreTransferService.Disabled, shutdownWorkTimeout = Duration(60, SECONDS),
      gossipStoreOnline = null, closeStoreForTransferOut = null, closeStoreForShutdown = null,
      scheduleShutdownWorkTimeout = null, scheduleEndpointRetry = null, ec = ExecutionContext.global)

    private def handle(event: Host.Event): Unit = event match
      case e: Host.LoadStore => ctx.loader.handleLoad(e)
      case e: Host.ShutdownStore => ctx.loader.handleClose(e)
      case e: Host.UnloadFailedStore => ctx.loader.handleUnloadFailed(e)
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

    /** Returns once every task posted to `executor` before it has run. */
    def awaitExecutor(executor: StoreExecutor): Unit =
      val done = new CountDownLatch(1)
      executor.execute(() => done.countDown())
      assert(done.await(Bound, TimeUnit.SECONDS))

    def shutdown(): Unit = threading.shutdown()

  private def withRouter(crl: CrashRecoveryLog, ready: Boolean = true)(f: Fixture => Unit): Unit =
    val fx = new Fixture(crl, ready)
    try f(fx) finally fx.shutdown()

  private def emptyCrl: CrashRecoveryLog = new GatedCrl(Future.unit)

class StoreRouterSuite extends AnyFunSuite with Matchers:
  import StoreRouterSuite.*

  private def await[T](f: Future[T]): T = Await.result(f, Duration(Bound, SECONDS))

  private def bootstrappedBackend(storeId: StoreId): TrackingBackend =
    val backend = new TrackingBackend(storeId, os.temp.dir())
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](7)))
    backend

  private def read(storeId: StoreId): Read = Read(storeId, clientId, UUID.randomUUID(), ptr, FullObject())

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
      val closed = fx.ctx.loader.closeStore(s0)
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
      CrashRecoveryLog.saveStoreState(s0, List(trs), backend.crlSaveFile)

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

  test("unknown store gets TxUnknownStore from the calling thread"):
    val unknown = StoreId(poolId, 2)
    val peer = StoreId(poolId, 1)
    val txid = TransactionId(new UUID(15, 7))
    withRouter(emptyCrl): fx =>
      fx.ctx.router.deliverTx(TxPrepare(unknown, peer, overwriteTxd(txid), ProposalId.initialProposal(0),
        Nil, Nil, true))
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
      val closed = fx.ctx.loader.closeStore(s0)
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
      def prepare = TxPrepare(unknown, peer, overwriteTxd(txid), ProposalId.initialProposal(0), Nil, Nil, true)
      fx.ctx.router.deliverTx(prepare)
      fx.net.tx.asScala.toList should be(empty)
      fx.ctx.router.markReady()
      fx.ctx.router.deliverTx(prepare)
      fx.net.tx.asScala.toList.map(_._1) should be(List(TxUnknownStore(peer, unknown, txid)))
