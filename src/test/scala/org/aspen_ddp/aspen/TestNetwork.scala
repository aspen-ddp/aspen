package org.aspen_ddp.aspen

import java.util.UUID
import org.aspen_ddp.aspen
import org.aspen_ddp.aspen.client.internal.{OpportunisticRebuildManager, StaticTypeRegistry}
import org.aspen_ddp.aspen.client.internal.allocation.{AllocationManager, BaseAllocationDriver}
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, ExponentialBackoffRetryStrategy, Host, HostId, KeyValueObjectState, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.internal.read.{BaseReadDriver, ReadManager}
import org.aspen_ddp.aspen.client.internal.transaction.{ClientTransactionDriver, MissedUpdateFinalizationAction, TransactionImpl, TransactionManager}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{AllocateResponse, ClientId, ClientRequest, ClientResponse, ReadResponse, TransactionCompletionResponse, TransactionFinalized, TransactionResolved, TxMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.aspen_ddp.aspen.common.util.{BackgroundTask, BackgroundTaskPool}
import org.aspen_ddp.aspen.server.{RegisteredTransactionFinalizerFactory, StoreManager, transaction}
import org.aspen_ddp.aspen.server.crl.{AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogFactory, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.store.{BackendStoreLoader, Bootstrap}
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendType, MapBackend}
import org.aspen_ddp.aspen.server.store.cache.SimpleLRUObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
import org.aspen_ddp.aspen.common.ida.IDA

import java.nio.file.Path
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions


object TestNetwork {

  class TestCRL extends CrashRecoveryLog {
    override def getFullRecoveryState(storeId: StoreId): (List[TransactionRecoveryState], List[AllocationRecoveryState]) = (Nil, Nil)

    override def closeStore(storeId: StoreId): Future[(List[TransactionRecoveryState], List[AllocationRecoveryState])] =
      Future.successful((List[TransactionRecoveryState](), List[AllocationRecoveryState]()))

    override def save(txid: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit = {
      completionHandler()
    }

    override def save(state: AllocationRecoveryState, completionHandler: () => Unit): Unit = {
      completionHandler()
    }

    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = ()

    override def deleteAllocation(storeId: StoreId, txid: TransactionId): Unit = ()
  }

  object TestCRL extends CrashRecoveryLogFactory {
    override def createCRL(): CrashRecoveryLog = new TestCRL()
  }

  /*
  class NullFinalizer extends TransactionFinalizer {
    override def complete: Future[Unit] = Future.successful(())

    override def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = ()


    override def debugStatus: List[(String, Boolean)] = Nil

    override def cancel(): Unit = ()
  }

  object NullFinalizer extends TransactionFinalizer.Factory {
    override def create(txd: TransactionDescription, messenger: ServerMessenger): TransactionFinalizer = new NullFinalizer
  }
  */

  class TClient(msngr: ClientMessenger, val radicle: KeyValueObjectPointer) extends AspenClient {

    import scala.concurrent.ExecutionContext.Implicits.global

    var attributes: Map[String, String] = Map()

    override val clientId: ClientId = ClientId(new UUID(0,1))

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache

    val typeRegistry: TypeRegistry = new TypeRegistry(StaticTypeRegistry.types.toMap)

    val rmgr = new ReadManager(this, BaseReadDriver.noErrorRecoveryReadDriver)

    def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] = {
      rmgr.read(pointer, comment).map(_.asInstanceOf[DataObjectState])
    }

    def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] = {
      rmgr.read(pointer, comment).map(_.asInstanceOf[KeyValueObjectState])
    }

    val txManager = new TransactionManager(this, ClientTransactionDriver.noErrorRecoveryFactory)

    def newTransaction(): Transaction = {
      new TransactionImpl(this, txManager, _ => 0, None)
    }

    def getStoragePool(poolName: String): Future[Option[StoragePool]] = ???

    def getStoragePool(poolId: PoolId): Future[Option[StoragePool]] = {
      val root = new KVObjectRootManager(this, Radicle.PoolTreeKey, radicle)
      val tkvl = new TieredKeyValueList(this, root)
      for {
        poolPtr <- tkvl.get(Key(poolId.uuid))
        poolKvos <- read(KeyValueObjectPointer(poolPtr.get.value.bytes))
      } yield {
        Some(SimpleStoragePool(this, poolKvos))
      }
    }

    override def updateStorageHost(storeId: StoreId, newHostId: HostId): Future[Unit] = ???

    override def newStoragePool(newPoolName: String,
                       hostCncFrontends: List[CnCFrontend],
                       ida: IDA,
                       backendType: BackendType): Future[StoragePool] = ???

    protected def createStoragePool(config: StoragePool.Config): Future[StoragePool] = ???

    def getHost(hostId: HostId): Future[Option[Host]] = Future.successful(Some(Host(HostId(new UUID(0,0)), "testhost", "localhost", 1234, 1235, 1236)))

    def getHost(hostName: String): Future[Option[Host]] = getHost(HostId(new UUID(0,0)))
    
    override def shutdown(): Unit = backgroundTasks.shutdown(Duration(50, MILLISECONDS))

    val retryStrategy: RetryStrategy = new ExponentialBackoffRetryStrategy(this)

    def backgroundTasks: BackgroundTask = new BackgroundTaskPool

    def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None

    val messenger: ClientMessenger = msngr

    val allocationManager: AllocationManager = new AllocationManager(this,
      BaseAllocationDriver.NoErrorRecoveryAllocationDriver)

    val objectCache: ObjectCache = ObjectCache.NoCache

    def receiveClientResponse(msg: ClientResponse): Unit = msg match {
      case m: ReadResponse => rmgr.receive(m)
      case m: TransactionCompletionResponse => rmgr.receive(m)
      case m: TransactionResolved => txManager.receive(m)
      case m: TransactionFinalized => txManager.receive(m)
      case m: AllocateResponse => allocationManager.receive(m)
    }

    def getSystemAttribute(key: String): Option[String] = attributes.get(key)
    def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value
  }
}


class TestNetwork extends ServerMessenger {
  import TestNetwork._

  val objectCacheFactory: () => SimpleLRUObjectCache = () => new SimpleLRUObjectCache(1000)

  val storeId0 = StoreId(Radicle.poolId, 0)
  val storeId1 = StoreId(Radicle.poolId, 1)
  val storeId2 = StoreId(Radicle.poolId, 2)

  val store0 = new MapBackend(storeId0)
  val store1 = new MapBackend(storeId1)
  val store2 = new MapBackend(storeId2)

  val ida = Replication(3, 2)

  var handleDepth = 0

  val radicle: KeyValueObjectPointer = Bootstrap.initialize(ida,
    List(store0, store1, store2), List(
      ("node1", new UUID(0,0)),
      ("node2", new UUID(0,1)),
      ("node3", new UUID(0,2))))

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

  val storeLoader = new BackendStoreLoader {
    override def loadStoreFromPath(storePath: Path)(using ec: ExecutionContext): Option[Backend] = ???
  }

  val smgr = new StoreManager(Path.of("/"),
    scala.concurrent.ExecutionContext.Implicits.global,
    objectCacheFactory, this, BackgroundTask.NoBackgroundTasks,
    TestCRL, FinalizerFactory, TransactionDriver.noErrorRecoveryFactory,
    storeLoader,
    Duration(5, SECONDS))
  
  smgr.loadStore(store0)
  smgr.loadStore(store1)
  smgr.loadStore(store2)

  def handleEvents(): Unit = synchronized {
    smgr.handleEvents()
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
  }

  val client = new TClient(cliMessenger, radicle)

  FinalizerFactory.client = client

  // process load store events
  smgr.handleEvents()

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
  }//msg.foreach(sendTransactionMessage)

  def printTransactionStatus(): Unit = {
    val test = client.getSystemAttribute("unittest.name")
    println(s"*********** Transaction Status. Hung Test: $test ***********")
    smgr.logTransactionStatus(s => println(s))
    println("******************************************")
  }

  def waitForTransactionsToComplete(): Future[Unit] = {
    //val stack = com.ibm.aspen.util.getStack()

    handleEvents()

    val bgTasks = new BackgroundTaskPool

    val p = Promise[Unit]()
    val pollDelay = Duration(5, MILLISECONDS)

    var count = 1

    def check(): Unit = {
      if (!smgr.hasTransactions) {
        bgTasks.shutdown(pollDelay)
        p.success(())
      } else {
        count += 1
        if (count == 20)
          printTransactionStatus()
        bgTasks.schedule(pollDelay)(check())
      }
    }

    bgTasks.schedule(pollDelay)(check())

    p.future
  }
}
