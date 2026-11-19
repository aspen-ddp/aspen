package org.aspen_ddp.aspen

import java.util.UUID
import org.aspen_ddp.aspen
import org.aspen_ddp.aspen.client.internal.BaseAspenClient
import org.aspen_ddp.aspen.client.{AspenClient, RegisteredTypeFactory, Transaction, TransactionStatusCache}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.BaseReadDriver
import org.aspen_ddp.aspen.client.ReadDriverClient
import org.aspen_ddp.aspen.client.internal.transaction.{ClientOutcome, ClientTransactionDriver,
  MissedUpdateFinalizationAction}
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.TransactionData
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, ClientResponse, Gossip, HostMessage, Read, ReadResponse, TxMessage, TxPrepare}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionHorizon, TransactionId}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, printStack}
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.server.{RegisteredTransactionFinalizerFactory, Host, StorageDeviceConfig, transaction}
import org.aspen_ddp.aspen.server.host.{HostThreading, JoinCeiling, LockstepThreading}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.store.{Bootstrap, Frontend, UnackedCommitLookup}
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.{HostObjectCache, ObjectCache as StoreObjectCache}
import org.aspen_ddp.aspen.server.transaction.{SimpleTransactionDriver, TransactionDriver, TransactionFinalizer}
import org.aspen_ddp.aspen.common.ida.IDA

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.implicitConversions


object TestNetwork {

  val bootstrapHost = HostState(HostId.BootstrapHostId, "testhost", "localhost", 1234, 1235, 1236, Set())

  class TestCRL extends CrashRecoveryLog {
    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = Nil

    override def save(txid: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit = {
      completionHandler()
    }

    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = ()

    override def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = Map()

    override def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
      completionHandler()

    override def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit = ()
  }

  /** A CRL that keeps what it is given in memory, so a store loaded again recovers its transactions and applied stubs
    * from it */
  class RecordingCRL extends CrashRecoveryLog:
    private var states: Map[StoreId, Map[TransactionId, TransactionRecoveryState]] = Map()
    private var stubs: Map[StoreId, Map[TransactionId, HLCTimestamp]] = Map()

    def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = synchronized:
      states.getOrElse(storeId, Map()).values.toList

    def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
      synchronized:
        states += state.storeId -> (states.getOrElse(state.storeId, Map()) + (txid -> state))
      completionHandler()

    def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

    def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = synchronized:
      states += storeId -> (states.getOrElse(storeId, Map()) - txid)

    def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = synchronized:
      stubs.getOrElse(storeId, Map())

    /** Replaces the transaction's state, as SimpleCRL does */
    def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
      synchronized:
        states += storeId -> (states.getOrElse(storeId, Map()) - txid)
        stubs += storeId -> (stubs.getOrElse(storeId, Map()) + (txid -> start))
      completionHandler()

    def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit = synchronized:
      stubs += storeId -> (stubs.getOrElse(storeId, Map()) - txid)

    /** Rewrites the state held for `txid` of `storeId`: what a crash between two of its saves leaves */
    def rewrite(storeId: StoreId, txid: TransactionId)(f: TransactionRecoveryState => TransactionRecoveryState): Unit =
      synchronized:
        val held = states.getOrElse(storeId, Map())
        require(held.contains(txid), s"store $storeId holds no state for $txid")
        states += storeId -> (held + (txid -> f(held(txid))))

  /** Tasks run only by runDue, on its caller's thread: timers a lockstep test drives itself */
  class ManualTimers extends BackgroundTaskManager(ExecutionContext.parasitic):
    private final class Task(val due: Long, fn: () => Unit) extends BackgroundTaskManager.ScheduledTask:
      @volatile var cancelled = false
      def cancel(): Unit = cancelled = true
      def run(): Unit = if !cancelled then fn()

    private var tasks: List[Task] = Nil

    override def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask = synchronized:
      val t = new Task(System.nanoTime() + delay.toNanos, () => fn)
      tasks = t :: tasks
      t

    override def schedulePeriodic(period: Duration, callNow: Boolean)(fn: => Unit): BackgroundTaskManager.ScheduledTask =
      throw new UnsupportedOperationException("ManualTimers runs one-shot tasks only")

    /** Runs the tasks whose delay has passed */
    def runDue(): Unit =
      val now = System.nanoTime()
      val due = synchronized:
        val (d, rest) = tasks.partition(_.due <= now)
        tasks = rest
        d
      due.reverse.foreach(_.run())

  /** What the bootstrap host recovers with. The defaults recover nothing: no CRL state, and store-side drivers that
    * never send (TransactionDriver.noErrorRecoveryFactory)
    *
    * @param heartbeatPeriod the host's, from which its status cache duration follows. Nothing calls the
    *                        heartbeat on its own: TestNetwork.heartbeat does
    * @param recordCrl the stores' CRL is a RecordingCRL, so restartStore recovers their transactions
    * @param recoveryDrivers drivers of transactions a store is not the designated leader of (recovery drivers) are
    *                        SimpleTransactionDrivers on TestNetwork.driverTimers
    * @param leaderRetransmits with recoveryDrivers, the designated leader's driver is a SimpleTransactionDriver too, as
    *                          a host's is: it sends data-less prepares of its own, which can reach a store before the
    *                          client's prepare does. Otherwise it never sends, and relies on the client's prepares
    * @param missingDataWait how long the stores' Txs wait for a client's prepare (Frontend.missingDataWait)
    * @param parkedReadWait how long the stores' parked re-reads wait for the commit they name (Frontend.parkedReadWait)
    * @param transactionHorizon the hosts' (Host.transactionHorizon), and the client's too: its transactions give up
    *                           with TransactionOutcomeUnknown past it (HorizonClientDriver)
    */
  final case class HostOptions(heartbeatPeriod: Duration = Duration(5, SECONDS),
                               recordCrl: Boolean = false,
                               recoveryDrivers: Boolean = false,
                               leaderRetransmits: Boolean = false,
                               wrapUnackedLookup: Option[UnackedCommitLookup => UnackedCommitLookup] = None,
                               missingDataWait: Duration = Frontend.MissingDataWait,
                               parkedReadWait: FiniteDuration = Frontend.ParkedReadWait,
                               transactionHorizon: FiniteDuration = TransactionHorizon.Horizon,
                               /** The client records its transactions' outcomes, as a production client does
                                 * (SimpleAspenClient), so its reads know its own commits. Default: NoCache */
                               clientStatusCache: Boolean = false)

  /** Opens a fresh no-op CRL for each device, creating the crl directory a real one would, so discovery does not
    * take the device's stores to have lost theirs */
  object TestCRL extends CrashRecoveryLogFactory {
    override def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
      os.makeDir.all(CrashRecoveryLog.deviceCrlDir(devicePath))
      new TestCRL()
  }

  class TClient(executionContext: ExecutionContext,
                msngr: ClientMessenger,
                radicle: KeyValueObjectPointer,
                ida: IDA,
                userTypeFactories: List[RegisteredTypeFactory] = Nil,
                txDriverFactory: ClientTransactionDriver.Factory = ClientTransactionDriver.noErrorRecoveryFactory,
                statusCache: TransactionStatusCache = TransactionStatusCache.NoCache)
    extends BaseAspenClient(
      executionContext,
      radicle,
      ClientId(new UUID(0, 1)),
      msngr,
      statusCache,
      BaseReadDriver.noErrorRecoveryReadDriver,
      txDriverFactory,
      userTypeFactories):

    // Fixed IDA for the bootstrap pool: resolving it per-read would recurse, since the pool states live in it.
    // Other pools are resolved from their state (read through the bootstrap pool), so objects in pools of another
    // IDA, such as a test's metadata pool, can be read. A pool the pools tree does not know (a test's synthetic
    // pointer) keeps the fixed IDA; any other failure surfaces.
    override protected def resolveIda(pointer: ObjectPointer): Future[IDA] =
      if pointer.poolId == PoolId.BootstrapPoolId then
        Future.successful(ida)
      else
        given ExecutionContext = executionContext
        getStoragePool(pointer.poolId).map(_.ida).recover { case _: NoSuchElementException => ida }

    /** Retries creates as the production client does (SimpleAspenClient.runCreate), for tests of a create whose
      * committed attempt the client is told is Unknown */
    @volatile var retryCreates: Boolean = false

    // Single attempt, no retry: test failures must surface rather than being retried away.
    override protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T] =
      if retryCreates then
        given ExecutionContext = executionContext
        transactUntilSuccessfulWithRecovery(onCommitFailure)(prepare)
      else
        transact(prepare)

  /** The TestNetwork client's transaction driver. It sends its prepares once and never retransmits, as
    * ClientTransactionDriver.noErrorRecoveryFactory's does, and gives up with ClientOutcome.Unknown once the client's
    * clock is past the horizon plus MaxStartLead (TransactionHorizon.clientGiveUp), as SimpleClientTransactionDriver
    * does. TestNetwork.heartbeat checks that, on the test's thread, so a lockstep host never sees another thread
    */
  final class HorizonClientDriver(horizon: FiniteDuration,
                                  client: ReadDriverClient,
                                  txd: TransactionDescription,
                                  updateData: Map[StoreId, TransactionData],
                                  commitAsUnknown: TransactionDescription => Boolean = _ => false)
    extends ClientTransactionDriver(client, txd, updateData):

    // A transaction the test asked about commits at the stores, but the client is told Unknown, as when it gives up
    // past the horizon just before the resolution would have reached it (TestNetwork.reportCommitAsUnknown)
    override protected def complete(outcome: ClientOutcome): Unit =
      if outcome == ClientOutcome.Committed && !result.isCompleted && commitAsUnknown(txd) then
        super.complete(ClientOutcome.Unknown)
      else
        super.complete(outcome)

    /** Gives up if past the horizon; whether the driver has completed */
    def checkGiveUp(): Boolean =
      if HLCTimestamp.now.wallTime - txd.startTimestamp.wallTime > TransactionHorizon.clientGiveUp(horizon).toMillis then
        complete(ClientOutcome.Unknown)
      result.isCompleted
}


class TestNetwork(executionContext: ExecutionContext,
                  userTypeFactories: List[RegisteredTypeFactory] = Nil,
                  val ida: IDA = Replication(3, 2),
                  poolMaxSliceSize: Option[Int] = None,
                  storeMaxSliceSize: Option[Long] = None,
                  backendMaxSliceSize: Option[Int] = None,
                  /** The bootstrap host's threading. Only ThreadedHostSuite passes anything but lockstep. */
                  threading: ExecutionContext => HostThreading = new LockstepThreading(_),
                  hostOptions: TestNetwork.HostOptions = TestNetwork.HostOptions()) extends ServerMessenger {
  import TestNetwork._

  /** A new cache on each call. A HostObjectCache must not be shared between Hosts */
  def objectCacheFactory: (StoreId, ExecutionContext) => StoreObjectCache =
    new HostObjectCache(64 * 1024 * 1024).forStore

  /** One store per IDA element. Bootstrap.initialize requires width == stores.length, which
   *  deriving the list from the width satisfies by construction. */
  val stores: List[MapBackend] =
    (0 until ida.width).toList.map(i => new MapBackend(StoreId(Radicle.poolId, i.toByte)))

  // Retained for the suites that reach for individual stores by name. Every IDA used with this
  // harness is at least three wide.
  val store0 = stores(0)
  val store1 = stores(1)
  val store2 = stores(2)

  /** The initial metadata pool's stores, one per IDA element. Bootstrap writes nothing to them. */
  val metadataStores: List[MapBackend] =
    (0 until ida.width).toList.map(i => new MapBackend(StoreId(PoolId.InitialMetadataPoolId, i.toByte)))

  val storeId0 = store0.storeId
  val storeId1 = store1.storeId
  val storeId2 = store2.storeId

  val storageDeviceId = StorageDeviceId.BootstrapStorageDeviceId

  var handleDepth = 0
  var capturedHostMessages: List[HostMessage] = Nil
  var capturedGossip: List[Gossip] = Nil

  val bootstrapSD = StorageDeviceState(
    storageDeviceId,
    bootstrapHost.hostId,
    0L,
    0L,
    (stores ++ metadataStores).map(s =>
      s.storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)).toMap,
    StorageDeviceSetId.BootstrapStorageDeviceSetId
  )

  val radicle: KeyValueObjectPointer = Bootstrap.initialize(
    new UUID(0,0),
    ida,
    bootstrapHost,
    bootstrapSD,
    stores,
    TestTLS.ca,
    poolMaxSliceSize,
    backendMaxSliceSize)

  // Tests that hold a store behind (withStoreBehind) should not wait long for the error tree to be updated
  MissedUpdateFinalizationAction.errorTimeout = Duration(50, MILLISECONDS)

  object FinalizerFactory extends TransactionFinalizer.Factory {
    var client: AspenClient = null

    /** Transactions whose finalizers never complete and do nothing, as one whose driver's host crashed before it
      * finalized: no UnackedCommits entry, ErrorTree mark or TxFinalized */
    @volatile var stalled: TransactionDescription => Boolean = _ => false

    def create(txd: TransactionDescription, messenger: ServerMessenger): TransactionFinalizer =
      if stalled(txd) then
        new TransactionFinalizer:
          def complete: Future[Unit] = Promise[Unit]().future
          def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]], keptForReplay: Set[StoreId]): Unit = ()
          def debugStatus: List[(String, Boolean)] = List("stalled" -> false)
          def cancel(): Unit = ()
      else
        val rfa = new RegisteredTransactionFinalizerFactory(client)
        rfa.create(txd, messenger)
  }

  /** Hosts the client messenger reports offline. Everything is online unless a test says otherwise. */
  @volatile var offlineHostIds: Set[HostId] = Set.empty

  /** Transaction messages dropped, by destination store and transaction: the store misses that transaction
   *  outright, as an unreachable store does, while every other transaction (missed-update marks included) reaches it */
  @volatile var droppedTx: Set[(StoreId, TransactionId)] = Set()

  /** Stores the client messenger reports offline, so no transaction chooses one as its designated leader: a leader
   *  whose messages are dropped would stall a transaction, as clients here do not recover them */
  @volatile var offlineStores: Set[StoreId] = Set()

  /** Further transaction messages to drop, as a test chooses (a store missing a transaction's outcome, say) */
  @volatile var dropTxMessage: TxMessage => Boolean = _ => false

  /** Transaction messages to record, sent by the client or by a store, whether or not they are then dropped */
  @volatile var recordTxMessage: TxMessage => Boolean = _ => false

  private var recordedTx: List[TxMessage] = Nil

  private var behind: Option[(StoreId, ObjectId)] = None

  /** The (store, transaction) pairs withStoreBehind recorded: every message of the transaction to the store is
    * dropped, for good */
  private var behindTx: Set[(StoreId, TransactionId)] = Set()

  /** Runs `f` with `store` missing every transaction that updates `objectId` while `f` runs, as a store unreachable
    * for those transactions alone (missed-update marks and repair still reach it). It is reported offline meanwhile,
    * so none of them chooses it as leader. A transaction is recognised by any prepare naming the object
    */
  def withStoreBehind[T](store: StoreId, objectId: ObjectId)(f: => Future[T]): Future[T] =
    given ExecutionContext = executionContext
    synchronized:
      behind = Some((store, objectId))
    offlineStores += store
    f.andThen: _ =>
      synchronized:
        behind = None
      offlineStores -= store

  /** A client's initial prepares for a transaction are held until it has sent one to every store, then delivered
   *  together before events are drained. The client sends them one at a time, and draining after each would let the
   *  first stores decide and finalize before the last sees its prepare: that store would miss the update, and nothing
   *  here repairs it, so two stores each missing a different object a later transaction requires would make that
   *  transaction abort on every attempt. A test that needs a store behind says so (withStoreBehind)
   */
  private var heldPrepares: Map[TransactionId, List[TxPrepare]] = Map()

  /** The held batch for `p`'s transaction, oldest first, once it holds a prepare for every store */
  private def holdPrepare(p: TxPrepare): Option[List[TxPrepare]] = synchronized:
    val batch = p :: heldPrepares.getOrElse(p.transactionId, Nil)
    if batch.size >= p.txd.allDataStores.size then
      heldPrepares -= p.transactionId
      Some(batch.reverse)
    else
      heldPrepares += p.transactionId -> batch
      None

  /** The messages recorded since the last call, oldest first */
  def takeRecordedTxMessages(): List[TxMessage] = synchronized:
    val taken = recordedTx.reverse
    recordedTx = Nil
    taken

  private def dropped(msg: TxMessage): Boolean =
    if recordTxMessage(msg) then
      synchronized:
        recordedTx = msg :: recordedTx
    val missed = synchronized:
      (behind, msg) match
        case (Some((store, objectId)), p: TxPrepare) if p.txd.allReferencedObjectsSet.exists(_.id == objectId) =>
          behindTx += ((store, p.transactionId))
        case _ => ()
      behindTx.contains((msg.to, msg.transactionId))
    missed || droppedTx.contains((msg.to, msg.transactionId)) || dropTxMessage(msg) || crashed(hostFor(msg.to)) ||
      (!isClientPrepare(msg) && crashed(hostFor(msg.from)))

  private def isClientPrepare(msg: TxMessage): Boolean = msg match
    case p: TxPrepare => p.fromClient
    case _ => false

  /** Delivers `msg` to its store as the client messenger does, bypassing the drop rules and the recording: a message
    * re-delivered late, as a network replay does
    */
  def deliverTransactionMessage(msg: TxMessage): Unit =
    handleEvents()
    hostFor(msg.to).receiveTransactionMessage(msg)
    handleEvents()

  /** Hosts other than smgr, each serving every store of the pools mapped to it. Messages to the stores of an
   *  unmapped pool go to smgr. handleEvents drains every mapped host, and a test remaps a pool to model a restart.
   */
  @volatile var poolHosts: Map[PoolId, Host] = Map()

  /** Stores served by a host newHost built (moveStore), taking precedence over poolHosts */
  @volatile var storeHosts: Map[StoreId, Host] = Map()

  /** The host serving `storeId` */
  def hostFor(storeId: StoreId): Host =
    storeHosts.getOrElse(storeId, poolHosts.getOrElse(storeId.poolId, smgr))

  private def allHosts: List[Host] =
    (poolHosts.values.toList ++ storeHosts.values.toList).foldLeft(List(smgr)): (hosts, h) =>
      if hosts.exists(_ eq h) || crashed(h) then hosts else hosts :+ h

  /** Hosts crashHost stopped: they run nothing more, and messages to or from their stores are dropped */
  @volatile private var crashedHosts: List[Host] = Nil

  private def crashed(host: Host): Boolean = crashedHosts.exists(_ eq host)

  private val smgrThreading: HostThreading = threading(executionContext)

  /** Whether the bootstrap host is lockstep, so the test drains its events (handleEvents). Otherwise it runs them
   *  itself once started. */
  private val lockstep: Boolean = smgrThreading.isInstanceOf[LockstepThreading]

  /** Called with every Read the test's client sends, before it is delivered */
  @volatile var observeRead: Read => Unit = _ => ()

  private var readCounting = false
  private var countedReads: Set[(UUID, ObjectId)] = Set()

  private def recordRead(r: Read): Unit = synchronized:
    if readCounting then
      countedReads += ((r.readUUID, r.objectPointer.id))

  /** Runs `f`, counting `client`'s object reads: one per read operation (distinct readUUID), per object */
  def countReads[T](f: => Future[T]): Future[(T, Map[ObjectId, Int])] =
    given ExecutionContext = executionContext
    synchronized:
      countedReads = Set()
      readCounting = true
    def stop(): Map[ObjectId, Int] = synchronized:
      readCounting = false
      countedReads.groupMapReduce(_._2)(_ => 1)(_ + _)
    f.transform: result =>
      val counts = stop()
      result.map(t => (t, counts))

  val cliMessenger = new ClientMessenger {

    def sendClientRequest(msg: ClientRequest): Unit = {
      msg match
        case r: Read if r.fromClient == client.clientId =>
          recordRead(r)
          observeRead(r)
        case _ => ()
      msg match
        case r: Read => noteSilencedObjectRead(r)
        case _ => ()
      handleEvents()
      if !crashed(hostFor(msg.toStore)) then
        hostFor(msg.toStore).receiveClientRequest(msg)
      handleEvents()
    }

    def sendTransactionMessage(msg: TxMessage): Unit = msg match
      case p: TxPrepare if p.fromClient =>
        holdPrepare(p).foreach: batch =>
          handleEvents()
          batch.foreach(m => if !dropped(m) then hostFor(m.to).receiveTransactionMessage(m))
          handleEvents()
      case _ =>
        handleEvents()
        if !dropped(msg) then
          hostFor(msg.to).receiveTransactionMessage(msg)
        handleEvents()

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)

    def sendHostMessage(msg: HostMessage): Unit = TestNetwork.this.synchronized:
      capturedHostMessages = capturedHostMessages :+ msg

    def dropCacheForStore(storeId: StoreId): Unit = ()

    override def isHostOnline(hostId: HostId): Boolean = !offlineHostIds.contains(hostId)

    override def isStoreOffline(storeId: StoreId): Boolean = offlineStores.contains(storeId)
  }

  /** Transactions still to be reported Unknown although they commit, by how many remain to be reported */
  private var unknownCommits: List[(TransactionDescription => Boolean, Int)] = Nil

  /** The next `count` client transactions matching `matching` commit at the stores, but the client is told
    * TransactionOutcomeUnknown, as past the horizon. Each match uses up one */
  def reportCommitAsUnknown(matching: TransactionDescription => Boolean, count: Int = 1): Unit = synchronized:
    unknownCommits = unknownCommits :+ (matching, count)

  /** How many reportCommitAsUnknown matches are still to come */
  def pendingUnknownCommits: Int = synchronized(unknownCommits.map(_._2).sum)

  private def commitAsUnknown(txd: TransactionDescription): Boolean = synchronized:
    unknownCommits.indexWhere((m, _) => m(txd)) match
      case -1 => false
      case i =>
        val (m, n) = unknownCommits(i)
        unknownCommits = if n > 1 then unknownCommits.updated(i, (m, n - 1)) else unknownCommits.patch(i, Nil, 1)
        true

  /** The client's transactions not yet completed (HorizonClientDriver), checked by heartbeat */
  private var clientDrivers: List[HorizonClientDriver] = Nil

  /** The client's driver of transaction `txid`, until heartbeat drops it once completed */
  def clientDriver(txid: TransactionId): Option[ClientTransactionDriver] = synchronized:
    clientDrivers.find(_.txd.transactionId == txid)

  private def clientDriverFactory(c: ReadDriverClient, txd: TransactionDescription,
                                  updateData: Map[StoreId, TransactionData]): ClientTransactionDriver =
    val d = new HorizonClientDriver(hostOptions.transactionHorizon, c, txd, updateData, commitAsUnknown)
    synchronized:
      clientDrivers = d :: clientDrivers
    d

  val client: AspenClient = new TClient(executionContext, cliMessenger, radicle, ida, userTypeFactories,
    clientDriverFactory,
    if hostOptions.clientStatusCache then new TransactionStatusCache() else TransactionStatusCache.NoCache)
  FinalizerFactory.client = client

  /** The CRL of every store the bootstrap host loads */
  val crl: CrashRecoveryLog = if hostOptions.recordCrl then new RecordingCRL() else new TestCRL()

  /** The timers of recovery drivers (HostOptions.recoveryDrivers), run by heartbeat */
  val driverTimers: ManualTimers = new ManualTimers

  private val txDriverFactory: TransactionDriver.Factory =
    if !hostOptions.recoveryDrivers then
      TransactionDriver.noErrorRecoveryFactory
    else
      val recovery = SimpleTransactionDriver.factory(Duration(10, MILLISECONDS), Duration(100, MILLISECONDS))
      new TransactionDriver.Factory:
        def create(ec: ExecutionContext, storeId: StoreId, messenger: ServerMessenger,
                   backgroundTasks: BackgroundTaskManager, txd: TransactionDescription,
                   finalizerFactory: TransactionFinalizer.Factory,
                   start: TransactionDriver.Start): TransactionDriver =
          if !hostOptions.leaderRetransmits && txd.designatedLeaderUID == storeId.poolIndex &&
              txd.primaryObject.poolId == storeId.poolId then
            TransactionDriver.noErrorRecoveryFactory.create(ec, storeId, messenger, backgroundTasks, txd,
              finalizerFactory, start)
          else
            recovery.create(ec, storeId, messenger, driverTimers, txd, finalizerFactory, start)

  /** The stores any host of this network has loaded */
  private var everLoaded: Set[StoreId] = Set()

  /** A store's first load is of a new store, which has never served, so there is nothing it could have forgotten: it
    * records no load time (Frontend.loadedAt) and joins transactions at once, as a store a host creates does
    * (StoreLoader). Every later load of it (restartStore, moveStore) records its load, as a real reload does
    */
  private def storeLoadedAt(storeId: StoreId): Option[Long] = synchronized:
    if everLoaded.contains(storeId) then
      None
    else
      everLoaded += storeId
      Some(0L)

  /** Where a host of this network keeps its join ceiling (Host.Seams.joinCeiling): in memory, written at once, so it
    * outlives the Host object as a host's file outlives its process (restartHost)
    */
  final class MemoryCeiling extends JoinCeiling.Persistence:
    @volatile private var value: Option[Long] = None
    def read(): Option[Long] = value
    def write(ceiling: Long): Unit = value = Some(ceiling)
    def remove(): Unit = value = None

  private val smgrCeiling = new MemoryCeiling

  /** The hosts' JoinCeiling.Step: short, so a test waits little past a crashed host's ceiling. Every handleEvents
    * renews it, so it never mutes a transaction a test sends
    */
  val CeilingStep: FiniteDuration = Duration(100, MILLISECONDS)

  /** Each host's join ceiling, by host (identity) */
  private var ceilings: List[(Host, MemoryCeiling)] = Nil

  val smgr = new Host(
    client,
    HostId.BootstrapHostId,
    new UUID(0, 0),
    os.root,
    executionContext,
    smgrThreading,
    objectCacheFactory,
    this,
    BackgroundTaskManager.NoBackgroundTaskManager,
    TestCRL,
    FinalizerFactory,
    txDriverFactory,
    hostOptions.heartbeatPeriod,
    Duration(60, SECONDS),
    // Its stores are loaded directly (below), onto a device it never loads
    seams = Host.Seams(unloadedDeviceCrl = Some(crl), wrapUnackedLookup = hostOptions.wrapUnackedLookup,
      missingDataWait = hostOptions.missingDataWait, parkedReadWait = hostOptions.parkedReadWait, storeLoadedAt = storeLoadedAt, joinCeiling = Some(smgrCeiling),
      joinCeilingStep = CeilingStep),
    transactionHorizon = hostOptions.transactionHorizon)

  // Stand in for each store's StoreConfig.effectiveMaxSliceSize and ida, which Host reads from the store config file
  private val storesLoaded = (stores ++ metadataStores).map(s => smgr.loadStore(storageDeviceId, s, storeMaxSliceSize, Some(ida)))

  // A host not in lockstep runs its own events once started; the network is usable once its stores are loaded
  if !lockstep then
    smgr.start()
    storesLoaded.foreach(loaded => Await.result(loaded, Duration(10, SECONDS)))

  /** Crash-only restart of bootstrap-host store `backend`: a new Store replaces the loaded one, dropping everything it
    * held in memory, and recovers its transactions from `crl` (HostOptions.recordCrl). Completes once loaded; the
    * caller drains events meanwhile
    */
  def restartStore(backend: MapBackend): Future[Unit] = smgr.loadStore(storageDeviceId, backend, storeMaxSliceSize, Some(ida))

  private var lastHeartbeat: Long = System.nanoTime()

  /** One step of the time a lockstep host does not keep itself: the heartbeat of its stores but those of `except`,
    * once a heartbeat period has passed since the last, then the recovery drivers' due timers. Then drains events
    */
  def heartbeat(except: Set[StoreId] = Set()): Unit =
    val now = System.nanoTime()
    if now - lastHeartbeat >= hostOptions.heartbeatPeriod.toNanos then
      lastHeartbeat = now
      allHosts.foreach: host =>
        val router = host.control.router
        router.loaded.keys.filterNot(except.contains).foreach(id => router.post(id)(_.heartbeat()))
    handleEvents()
    driverTimers.runDue()
    handleEvents()
    val pending = synchronized:
      val ds = clientDrivers
      clientDrivers = Nil
      ds
    val live = pending.filterNot(_.checkGiveUp())
    synchronized:
      clientDrivers = live ++ clientDrivers
    handleEvents()

  /** Whether a transaction started now is at or above the load floor of store `storeId` (Frontend.forgetFloorMillis:
    * its load plus MaxStartLead), so the store joins it: a store refuses every transaction it does not hold that
    * started within MaxStartLead after it loaded, since it may have joined and forgotten one before
    */
  def loadFloorPassed(storeId: StoreId): Boolean =
    hostFor(storeId).control.router.loaded.get(storeId).exists(_.frontend.loadFloorPassed)

  private var hostsBuilt = 0

  /** Another lockstep host like the bootstrap one (the same HostOptions and CRL), serving no store yet: moveStore. Its
    * own status cache starts empty, as a host that restarted or never saw the bootstrap host's transactions
    */
  def newHost(): Host = buildHost(new MemoryCeiling)

  private def buildHost(ceiling: MemoryCeiling): Host =
    hostsBuilt += 1
    val host = new Host(client, HostId(new UUID(1, hostsBuilt)), new UUID(0, 0), os.root, executionContext,
      new LockstepThreading(executionContext), objectCacheFactory, this, BackgroundTaskManager.NoBackgroundTaskManager,
      TestCRL, FinalizerFactory, txDriverFactory, hostOptions.heartbeatPeriod, Duration(60, SECONDS),
      seams = Host.Seams(unloadedDeviceCrl = Some(crl), wrapUnackedLookup = hostOptions.wrapUnackedLookup,
        missingDataWait = hostOptions.missingDataWait, parkedReadWait = hostOptions.parkedReadWait, storeLoadedAt = storeLoadedAt, joinCeiling = Some(ceiling),
        joinCeilingStep = CeilingStep),
      transactionHorizon = hostOptions.transactionHorizon)
    synchronized:
      ceilings = (host, ceiling) :: ceilings
    host

  /** The stores a host newHost built serves (moveStore) */
  def storesOf(host: Host): List[MapBackend] =
    stores.filter(s => storeHosts.get(s.storeId).exists(_ eq host))

  /** Crashes `host`, built by newHost: it runs nothing more, its join ceiling is renewed no more, and every message to
    * or from its stores, and every client request to them, is dropped. Its stores are reported offline, so no
    * transaction chooses one as leader
    */
  def crashHost(host: Host): Unit =
    require(!(host eq smgr), "the bootstrap host cannot crash")
    offlineStores ++= storesOf(host).map(_.storeId)
    synchronized:
      crashedHosts = host :: crashedHosts

  /** The crashed `host` restarted: a new host with the same join ceiling (its file outlives its process) and an empty
    * status cache, which loads every store the crashed one served, as its process's startup does. Completes once they
    * are loaded; the caller drains events meanwhile
    */
  def restartHost(host: Host): Future[Host] =
    given ExecutionContext = executionContext
    require(crashed(host), "only a crashed host restarts")
    val ceiling = synchronized(ceilings.collectFirst { case (h, c) if h eq host => c }).get
    val restarted = buildHost(ceiling)
    val served = storesOf(host)
    Future.traverse(served)(s => moveStore(s, restarted, closeFirst = false)).map: _ =>
      offlineStores --= served.map(_.storeId)
      restarted

  /** Serves store `backend` on `to` from now on: a new Store there recovers its transactions from `crl`, as a store
    * transferred to `to`, or reloaded once its host restarted, does. With `closeFirst` the host that served it closes
    * it first, as a transfer out does; otherwise that host is left as it is, as a crashed one. Completes once loaded;
    * the caller drains events meanwhile
    */
  def moveStore(backend: MapBackend, to: Host, closeFirst: Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    val from = hostFor(backend.storeId)
    val closed =
      if closeFirst then from.control.loader.closeStore(backend.storeId, extractCrl = false) else Future.unit
    closed.flatMap: _ =>
      storeHosts += backend.storeId -> to
      to.loadStore(storageDeviceId, backend, storeMaxSliceSize, Some(ida))

  // ---- Rebalancing test helpers -------------------------------------------------

  /** A second, empty device in the bootstrap set so stores have a transfer destination. */
  val secondDeviceId: StorageDeviceId = StorageDeviceId(new UUID(0, 100))

  /** A third, empty device, for scenarios that need a destination distinct from both the
   *  bootstrap device and the second device. */
  val thirdDeviceId: StorageDeviceId = StorageDeviceId(new UUID(0, 101))

  def createSecondDevice(): Future[Unit] = createDevice(secondDeviceId)

  def createThirdDevice(): Future[Unit] = createDevice(thirdDeviceId)

  /** Register an empty StorageDeviceState in the StorageDevicesTree and append it to
   *  the bootstrap set's memberDevices. Mirrors Bootstrap's device indexing. */
  def createDevice(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    val sd = StorageDeviceState(
      deviceId,
      bootstrapHost.hostId,
      0L, 1_000_000L,
      Map.empty,
      StorageDeviceSetId.BootstrapStorageDeviceSetId)
    val devicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.StorageDevicesTreeKey, Radicle.pointer))
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        pool <- client.getStoragePool(Radicle.poolId)
        allocator = new PoolObjectAllocator(client, pool)
        devPtr <- allocator.allocateKeyValueObject(
                    Map(StorageDeviceState.StateKey -> Value(sd.encode())))
        setPtr <- client.getStorageDeviceSetPointer(StorageDeviceSetId.BootstrapStorageDeviceSetId)
        setDos <- client.read(setPtr)
        _ <- devicesTkvl.set(Key(deviceId.uuid), Value(devPtr.toArray))
      yield
        val curSet = StorageDeviceSetState(setDos)
        val updated = curSet.copy(memberDevices = curSet.memberDevices :+ deviceId)
        tx.overwrite(setPtr, setDos.revision, DataBuffer(updated.toBytes))

  /** Simulate completion of a single store transfer by performing the same metadata flip
   *  Host.updateStateForTransferredStore performs: pool StoreEntry -> (destHost, toDevice),
   *  remove store from source device, set store Active on destination device. In-memory KV only. */
  def simulateTransferComplete(storeId: StoreId,
                               fromDeviceId: StorageDeviceId,
                               toDeviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        fromDevPtr <- client.getStorageDevicePointer(fromDeviceId)
        toDevPtr <- client.getStorageDevicePointer(toDeviceId)
        poolKvos <- client.read(poolPtr)
        fromDevKvos <- client.read(fromDevPtr)
        toDevKvos <- client.read(toDevPtr)
      yield
        val poolCfg = StoragePoolState(poolKvos)
        val fromDev = StorageDeviceState(fromDevKvos)
        val toDev = StorageDeviceState(toDevKvos)
        if fromDev.stores.contains(storeId) then
          poolCfg.stores(storeId.poolIndex) = StoragePoolState.StoreEntry(toDev.hostId, toDeviceId)
          val poolReqs = List(KeyRevision(StoragePoolState.ConfigKey,
            poolKvos.contents(StoragePoolState.ConfigKey).revision))
          val poolOps = List(Insert(StoragePoolState.ConfigKey, poolCfg.encode()))
          tx.update(poolPtr, None, None, poolReqs, poolOps)

          val newFromDev = fromDev.removeStore(storeId)
          val fromReqs = List(KeyRevision(StorageDeviceState.StateKey,
            fromDevKvos.contents(StorageDeviceState.StateKey).revision))
          val fromOps = List(Insert(StorageDeviceState.StateKey, newFromDev.encode()))
          tx.update(fromDevPtr, None, None, fromReqs, fromOps)

          val newToDev = toDev.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Active, None)
          val toReqs = List(KeyRevision(StorageDeviceState.StateKey,
            toDevKvos.contents(StorageDeviceState.StateKey).revision))
          val toOps = List(Insert(StorageDeviceState.StateKey, newToDev.encode()))
          tx.update(toDevPtr, None, None, toReqs, toOps)

  /** Complete every store currently in TransferringIn across the given set's member devices.
   *  Returns the number of transfers completed this pass. A store in TransferringIn on a
   *  destination device carries transferDevice = Some(sourceDeviceId). */
  def completeInFlightTransfers(setId: StorageDeviceSetId): Future[Int] =
    given ExecutionContext = executionContext
    for
      setState <- client.getStorageDeviceSetState(setId)
      deviceStates <- Future.sequence(setState.memberDevices.map(client.getStorageDeviceState))
      inFlight = deviceStates.flatMap: ds =>
                   ds.stores.collect:
                     case (sid, entry)
                       if entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
                          entry.transferDevice.isDefined =>
                       (sid, entry.transferDevice.get, ds.storageDeviceId)
      _ <- Future.sequence(inFlight.map((sid, from, to) => simulateTransferComplete(sid, from, to)))
    yield inFlight.size

  override def broadcastGossip(g: Gossip): Unit = synchronized:
    capturedGossip = capturedGossip :+ g

  /** Remove and return all gossip broadcast so far. */
  def takeCapturedGossip(): List[Gossip] = synchronized:
    val gs = capturedGossip
    capturedGossip = Nil
    gs

  /** Remove and return all currently-captured host messages (best-effort delivery is the
   *  caller's job). Mirrors how transfer tests inspect capturedHostMessages, but drains. */
  def takeCapturedHostMessages(): List[HostMessage] = synchronized:
    val msgs = capturedHostMessages
    capturedHostMessages = Nil
    msgs

  // -------------------------------------------------------------------------------

  var otestThreadId: Option[Long] = None

  /** Drains every lockstep host's events. A host on any other threading runs its own, so it is left alone. */
  def handleEvents(): Unit =
    // Each live host keeps its join ceiling ahead, as its background renewal does (JoinCeiling.renew)
    allHosts.foreach(_.joinCeiling.renew())
    val lockstepHosts = allHosts.filter(_.threading.isInstanceOf[LockstepThreading])
    if lockstepHosts.nonEmpty then
      otestThreadId match
        case None => otestThreadId = Some(Thread.currentThread().threadId())
        case Some(testThreadId) =>
          if testThreadId != Thread.currentThread().threadId() then
            println(s"*********** Current thread ${Thread.currentThread().threadId()} != testThreadId $testThreadId")
            printStack()

      synchronized:
        lockstepHosts.foreach(_.testingOnlyHandleEvents())

  // process load store events
  smgr.testingOnlyHandleEvents()

  // ---- Degraded-read helpers ----------------------------------------------------

  /** Stores whose ReadResponses are dropped on the floor, simulating unavailability at the
   *  network layer. Transaction traffic is deliberately left alone, so a silenced store still
   *  participates in commits -- this exists to exercise reads that must be restored from fewer
   *  than `width` slices, not to model a genuinely dead store.
   *
   *  Dropping responses rather than deleting objects out of the MapBackends is what makes this
   *  work at all: Frontend.readObjectForNetwork consults the host's object cache before
   *  the backend, so a deleted object would keep being served from cache.
   */
  @volatile var readSilencedStores: Set[StoreId] = Set()

  /** Runs `f` with the given stores' ReadResponses suppressed, restoring the previous set
   *  whether `f` succeeds or fails. */
  def withReadSilenced[T](silenced: Set[StoreId])(f: => Future[T]): Future[T] =
    given ExecutionContext = executionContext
    val previous = readSilencedStores
    readSilencedStores = silenced
    f.transform: result =>
      readSilencedStores = previous
      result

  /** Objects whose reads never complete: every ReadResponse to a Read of one of them is dropped, whatever store sends
   *  it. Unlike readSilencedStores this leaves every other object of the pool readable (the allocation tree, say) */
  @volatile var readSilencedObjects: Set[ObjectId] = Set()

  /** readUUIDs of the Reads sent for readSilencedObjects */
  private var silencedReadIds: Set[UUID] = Set()

  private def noteSilencedObjectRead(r: Read): Unit =
    if readSilencedObjects.contains(r.objectPointer.id) then
      synchronized { silencedReadIds += r.readUUID }

  private def isSilencedObjectRead(rr: ReadResponse): Boolean = synchronized(silencedReadIds.contains(rr.readUUID))

  /** Runs `f` with every read of `ids` left unanswered, restoring the previous set whether `f` succeeds or fails.
   *  A read sent meanwhile stays unanswered after `f` ends: only reads sent later are answered */
  def withObjectReadsSilenced[T](ids: Set[ObjectId])(f: => Future[T]): Future[T] =
    given ExecutionContext = executionContext
    val previous = readSilencedObjects
    readSilencedObjects = ids
    f.transform: result =>
      readSilencedObjects = previous
      result

  // -------------------------------------------------------------------------------

  /** Client responses to drop, as a test chooses (a client never hearing a transaction's outcome, say) */
  @volatile var dropClientResponse: ClientResponse => Boolean = _ => false

  override def sendClientResponse(msg: ClientResponse): Unit = msg match {
    case rr: ReadResponse if readSilencedStores.contains(rr.fromStore) || isSilencedObjectRead(rr) =>
      handleEvents()

    case _ if dropClientResponse(msg) =>
      handleEvents()

    case _ =>
      handleEvents()
      client.receiveClientResponse(msg)
      handleEvents()
  }

  override def sendTransactionMessage(msg: TxMessage): Unit = {
    handleEvents()
    if !dropped(msg) then
      hostFor(msg.to).receiveTransactionMessage(msg)
    handleEvents()
  }

  override def sendTransactionMessages(msg: List[TxMessage]): Unit = {
    handleEvents()
    msg.foreach(m => if !dropped(m) then hostFor(m.to).receiveTransactionMessage(m))
    handleEvents()
  }

  override def dropCacheForStore(storeId: StoreId): Unit = ()

  def hasTransactions: Boolean = allHosts.exists(_.hasTransactions)

  def printTransactionStatus(): Unit = {
    if hasTransactions then
      val test = client.getSystemAttribute("unittest.name")
      println(s"*********** Transaction Status. Hung Test: $test ***********")
      allHosts.foreach(_.logTransactionStatus(s => println(s)))
      println("******************************************")
  }

  def waitForTransactionsToComplete(): Future[Unit] = {
    //val stack = com.ibm.aspen.util.getStack()

    handleEvents()

    val p = Promise[Unit]()
    val pollDelay = Duration(100, MILLISECONDS)

    val myRunnable: Runnable = () => {
      var count = 0
      while hasTransactions do
        count += 1
        if (count == 20)
          printTransactionStatus()
        Thread.sleep(100) // Simulate some work
      p.success(())
    }

    val checkerThread = new Thread(myRunnable, "waitForTransactionCompleteThread")

    checkerThread.start()

    p.future
  }
}
