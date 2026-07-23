package org.aspen_ddp.aspen

import java.util.UUID
import org.aspen_ddp.aspen
import org.aspen_ddp.aspen.client.internal.{BaseAspenClient, OpportunisticRebuildManager}
import org.aspen_ddp.aspen.client.{AspenClient, ObjectCache, RegisteredTypeFactory, Transaction, TransactionStatusCache}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.BaseReadDriver
import org.aspen_ddp.aspen.client.internal.transaction.{ClientTransactionDriver, MissedUpdateFinalizationAction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, ClientResponse, HostMessage, TxMessage}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, printStack}
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.server.{RegisteredTransactionFinalizerFactory, StoreManager, transaction}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.store.Bootstrap
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.SimpleLRUObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}
import org.aspen_ddp.aspen.common.ida.IDA

import java.nio.file.Path
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions


object TestNetwork {

  val bootstrapHost = HostState(HostId.BootstrapHostId, "testhost", "localhost", 1234, 1235, 1236, Set())

  class TestCRL extends CrashRecoveryLog {
    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = Nil

    override def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]] =
      Future.successful(Nil)

    override def save(txid: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit = {
      completionHandler()
    }

    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = ()
  }

  object TestCRL extends CrashRecoveryLogFactory {
    override def createCRL(): CrashRecoveryLog = new TestCRL()
  }

  class TClient(executionContext: ExecutionContext,
                msngr: ClientMessenger,
                radicle: KeyValueObjectPointer,
                ida: IDA,
                userTypeFactories: List[RegisteredTypeFactory] = Nil)
    extends BaseAspenClient(
      executionContext,
      radicle,
      ClientId(new UUID(0, 1)),
      msngr,
      TransactionStatusCache.NoCache,
      ObjectCache.NoCache,
      BaseReadDriver.noErrorRecoveryReadDriver,
      ClientTransactionDriver.noErrorRecoveryFactory,
      userTypeFactories):

    override def opportunisticRebuildManager: OpportunisticRebuildManager =
      OpportunisticRebuildManager.None

    // Fixed IDA: the test network lives entirely within the bootstrap pool, so resolving the
    // pool per-read would recurse. Returning the known IDA breaks that cycle.
    override protected def resolveIda(pointer: ObjectPointer): Future[IDA] =
      Future.successful(ida)

    // Single attempt, no retry: test failures must surface rather than being retried away.
    override protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T] =
      transact(prepare)
}


class TestNetwork(executionContext: ExecutionContext,
                  userTypeFactories: List[RegisteredTypeFactory] = Nil) extends ServerMessenger {
  import TestNetwork._

  val objectCacheFactory: () => SimpleLRUObjectCache = () => new SimpleLRUObjectCache(1000)

  val storeId0 = StoreId(Radicle.poolId, 0)
  val storeId1 = StoreId(Radicle.poolId, 1)
  val storeId2 = StoreId(Radicle.poolId, 2)

  val store0 = new MapBackend(storeId0)
  val store1 = new MapBackend(storeId1)
  val store2 = new MapBackend(storeId2)

  val ida = Replication(3, 2)

  val storageDeviceId = StorageDeviceId.BootstrapStorageDeviceId

  var handleDepth = 0
  var capturedHostMessages: List[HostMessage] = Nil

  val bootstrapSD = StorageDeviceState(
    storageDeviceId,
    bootstrapHost.hostId,
    0L,
    0L,
    Map(
      store0.storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None),
      store1.storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None),
      store2.storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
    ),
    StorageDeviceSetId.BootstrapStorageDeviceSetId
  )

  val radicle: KeyValueObjectPointer = Bootstrap.initialize(
    new UUID(0,0),
    ida,
    bootstrapHost,
    bootstrapSD,
    List(store0, store1, store2))

  // All transactions will miss the third store. Don't wait long before updating the
  // error tree
  MissedUpdateFinalizationAction.errorTimeout = Duration(50, MILLISECONDS)

  object FinalizerFactory extends TransactionFinalizer.Factory {
    var client: AspenClient = null

    def create(txd: TransactionDescription, messenger: ServerMessenger): TransactionFinalizer = {
      val rfa = new RegisteredTransactionFinalizerFactory(client)
      rfa.create(txd, messenger)
    }
  }

  private val cliMessenger = new ClientMessenger {

    def sendClientRequest(msg: ClientRequest): Unit = {
      handleEvents()
      smgr.receiveClientRequest(msg)
      handleEvents()
    }

    def sendTransactionMessage(msg: TxMessage): Unit = {
      handleEvents()
      smgr.receiveTransactionMessage(msg)
      handleEvents()
    }

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)

    def sendHostMessage(msg: HostMessage): Unit = TestNetwork.this.synchronized:
      capturedHostMessages = capturedHostMessages :+ msg

    def dropCacheForStore(storeId: StoreId): Unit = ()
  }

  val client: AspenClient = new TClient(executionContext, cliMessenger, radicle, ida, userTypeFactories)
  FinalizerFactory.client = client

  val smgr = new StoreManager(
    client,
    HostId.BootstrapHostId,
    new UUID(0, 0),
    Path.of("/"),
    executionContext,
    objectCacheFactory,
    this,
    BackgroundTaskManager.NoBackgroundTaskManager,
    TestCRL,
    FinalizerFactory,
    TransactionDriver.noErrorRecoveryFactory,
    Duration(5, SECONDS),
    Duration(60, SECONDS))

  smgr.loadStore(storageDeviceId, store0)
  smgr.loadStore(storageDeviceId, store1)
  smgr.loadStore(storageDeviceId, store2)

  // ---- Rebalancing test helpers -------------------------------------------------

  /** A second, empty device in the bootstrap set so stores have a transfer destination. */
  val secondDeviceId: StorageDeviceId = StorageDeviceId(new UUID(0, 100))

  /** Register a second, empty StorageDeviceState in the StorageDevicesTree and append it to
   *  the bootstrap set's memberDevices. Mirrors Bootstrap's device indexing. */
  def createSecondDevice(): Future[Unit] =
    given ExecutionContext = executionContext
    val sd = StorageDeviceState(
      secondDeviceId,
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
        _ <- devicesTkvl.set(Key(secondDeviceId.uuid), Value(devPtr.toArray))
      yield
        val curSet = StorageDeviceSetState(setDos)
        val updated = curSet.copy(memberDevices = curSet.memberDevices :+ secondDeviceId)
        tx.overwrite(setPtr, setDos.revision, DataBuffer(updated.toBytes))

  /** Simulate completion of a single store transfer by performing the same metadata flip
   *  StoreManager.updateStateForTransferredStore performs: pool StoreEntry -> (destHost, toDevice),
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

  // -------------------------------------------------------------------------------

  var otestThreadId: Option[Long] = None

  def handleEvents(): Unit = {
    otestThreadId match
      case None => otestThreadId = Some(Thread.currentThread().threadId())
      case Some(testThreadId) =>
        if testThreadId != Thread.currentThread().threadId() then
          println(s"*********** Current thread ${Thread.currentThread().threadId()} != testThreadId $testThreadId")
          printStack()

    synchronized:
      smgr.testingOnlyHandleEvents()
  }

  // process load store events
  smgr.testingOnlyHandleEvents()

  override def sendClientResponse(msg: ClientResponse): Unit = {
    handleEvents()
    client.receiveClientResponse(msg)
    handleEvents()
  }

  override def sendTransactionMessage(msg: TxMessage): Unit = {
    handleEvents()
    smgr.receiveTransactionMessage(msg)
    handleEvents()
  }

  override def sendTransactionMessages(msg: List[TxMessage]): Unit = {
    handleEvents()
    msg.foreach(smgr.receiveTransactionMessage)
    handleEvents()
  }

  override def dropCacheForStore(storeId: StoreId): Unit = ()

  def hasTransactions: Boolean = smgr.hasTransactions

  def printTransactionStatus(): Unit = {
    if smgr.hasTransactions then
      val test = client.getSystemAttribute("unittest.name")
      println(s"*********** Transaction Status. Hung Test: $test ***********")
      smgr.logTransactionStatus(s => println(s))
      println("******************************************")
  }

  def waitForTransactionsToComplete(): Future[Unit] = {
    //val stack = com.ibm.aspen.util.getStack()

    handleEvents()

    val p = Promise[Unit]()
    val pollDelay = Duration(100, MILLISECONDS)

    val myRunnable: Runnable = () => {
      var count = 0
      while smgr.hasTransactions do
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
