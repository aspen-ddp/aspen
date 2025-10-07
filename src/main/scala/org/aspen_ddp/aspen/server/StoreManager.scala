package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{StorageDevice, StorageDeviceId, ObjectState as ClientObjectState}

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, Completion, CompletionHandler}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.store.{Frontend, Store}
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend}

import java.io.File
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

object StoreManager:
  private sealed abstract class Event

  private case class IOCompletion(op: Completion) extends Event
  private case class TransactionMessage(msg: TxMessage) extends Event
  private case class ClientReq(msg: ClientRequest) extends Event
  private case class Repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]) extends Event
  private case class LoadStore(backend: Backend, completion: Promise[Unit]) extends Event
  private case class LoadStoreById(sstorageDeviceId: StorageDeviceId, toreId: StoreId) extends Event
  private case class Exit() extends Event
  private case class RecoveryEvent() extends Event
  private case class HeartbeatEvent() extends Event
  private case class ShutdownStore(storeId: StoreId, completion: Promise[Unit]) extends Event

  class IOHandler(mgr: StoreManager) extends CompletionHandler:
    override def complete(op: Completion): Unit =
      mgr.events.add(IOCompletion(op))

  class StorageDeviceState(val storageDeviceId: StorageDeviceId,
                           val devicePath: Path):
    var loadedStores: Set[StoreId] = Set()


class StoreManager(val aspenSystemId: UUID,
                   val rootDir: Path,
                   val ec: ExecutionContext,
                   val objectCacheFactory: () => ObjectCache,
                   val net: Messenger,
                   val backgroundTasks: BackgroundTaskManager,
                   crlFactory: CrashRecoveryLogFactory,
                   val finalizerFactory: TransactionFinalizer.Factory,
                   val txDriverFactory: TransactionDriver.Factory,
                   val heartbeatPeriod: Duration) extends Logging {
  import StoreManager._
  
  given ExecutionContext = ec

  private val events = new LinkedBlockingQueue[Event]()

  private val ioHandler = new IOHandler(this)

  private val txStatusCache = new TransactionStatusCache()

  private val crl = crlFactory.createCRL()

  private val threadPool = Executors.newFixedThreadPool(1)

  protected var shutdownCalled = false
  private val shutdownPromise: Promise[Unit] = Promise()

  val storageDevicesDir: Path = rootDir.resolve("storage-devices")

  protected var storageDevices: Map[StorageDeviceId, StorageDeviceState] = Map()
  protected var stores: Map[StoreId, Store] = Map()

  backgroundTasks.schedulePeriodic(heartbeatPeriod) {
    events.put(HeartbeatEvent())
  }

  if ! Files.isDirectory(storageDevicesDir) then
    logger.warn(s"Invalid storage devices directory: $storageDevicesDir")
  else
    storageDevicesDir.toFile.listFiles().foreach: sdFile =>
      tryLoadDevice(sdFile)

  private def tryLoadDevice(sdFile: File): Unit =
    val storageDevicePath = sdFile.toPath
    val sdCfgPath = storageDevicePath.resolve(StorageDeviceConfig.configFilename)

    if Files.isDirectory(sdFile.toPath) && Files.exists(sdCfgPath) then
      val sdCfg = StorageDeviceConfig.loadHostConfig(sdCfgPath.toFile)
      if sdCfg.aspenSystemId != aspenSystemId then
        logger.warn(s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
      else
        val sds = new StorageDeviceState(sdCfg.storageDeviceId, storageDevicePath)
        storageDevices += sdCfg.storageDeviceId -> sds
        sdFile.listFiles.foreach: potentialStoreFile =>
          tryLoadStore(sds, potentialStoreFile)

  private def tryLoadStore(sds: StorageDeviceState, potentialStoreFile: File): Unit =
    val storeCfgPath = potentialStoreFile.toPath.resolve(StoreConfig.configFilename)
    if Files.exists(storeCfgPath) then
      val storeCfg = StoreConfig.loadStoreConfig(storeCfgPath.toFile)
      val backend = storeCfg.backend match
        case b: StoreConfig.RocksDB => new RocksDBBackend(potentialStoreFile.toPath, storeCfg.storeId, ec)
      sds.loadedStores += backend.storeId
      logger.info(s"Loading store ${storeCfg.storeId}: $potentialStoreFile")
      loadStore(backend)

  def start(): Unit =
    threadPool.submit(new Runnable {
      override def run(): Unit =
        while !shutdownCalled do
          var event = events.poll(3, TimeUnit.SECONDS)
          while (event != null)
            handleEvent(event)
            event = events.poll(0, TimeUnit.SECONDS)
    })

  def getDevicePath(storageDeviceId: StorageDeviceId): Option[Path] = synchronized {
    storageDevices.get(storageDeviceId).map(_.devicePath)
  }

  def containsStore(storeId: StoreId): Boolean = synchronized {
    logger.trace(s"********* CONTAINS STORE: ${storeId}: ${stores.contains(storeId)}. Stores: ${stores}")
    stores.contains(storeId)
  } 
  
  def getStoreIds: List[StoreId] = synchronized {
    stores.keysIterator.toList
  }

  def hasTransactions: Boolean = synchronized {
    stores.valuesIterator.exists(_.hasTransactions)
  }

  def logTransactionStatus(log: String => Unit): Unit = synchronized {
    stores.values.foreach(_.logTransactionStatus(log))
  }

  def loadStore(backend: Backend): Future[Unit] = {
    val p = Promise[Unit]()
    events.put(LoadStore(backend, p))
    p.future
  }

  def loadStoreById(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    events.put(LoadStoreById(storageDeviceId, storeId))

  def receiveTransactionMessage(msg: TxMessage): Unit = {
    events.put(TransactionMessage(msg))
  }

  def receiveClientRequest(msg: ClientRequest): Unit = {
    events.put(ClientReq(msg))
  }

  def repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
    events.put(Repair(storeId, os, completion))

  def shutdown()(using ec: ExecutionContext): Future[Unit] = {
    events.put(Exit())
    shutdownPromise.future
  }
  
  def closeStore(storeId: StoreId): Future[Unit] = {
    val p = Promise[Unit]()
    events.put(ShutdownStore(storeId, p))
    p.future
  }

  protected def addRecoveryEvent(): Unit = events.add(RecoveryEvent())

  /** Placeholder for mixin class to implement transaction and allocation recovery */
  protected def handleRecoveryEvent(): Unit = ()

  def hasEvents: Boolean = synchronized {
    events.size() != 0
  }

  /** Handles all events in the event queue. Returns when the queue is empty */
  def testingOnlyHandleEvents(): Unit = {
    var event = events.poll(0, TimeUnit.NANOSECONDS)
    while (event != null) {
      handleEvent(event)
      event = events.poll(0, TimeUnit.NANOSECONDS)
    }
  }

  private def handleEvent(event: Event): Unit = synchronized {
    event match {

      case IOCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.frontend.backendOperationComplete(op)
      }

      case TransactionMessage(msg) =>
        stores.get(msg.to).foreach { store =>
          store.receiveTransactionMessage(msg)
        }

      case ClientReq(msg) => stores.get(msg.toStore).foreach { store =>
        msg match {
          case a: Allocate => store.frontend.allocateObject(a)

          case r: Read =>
            r.objectPointer.getStoreLocater(store.storeId).foreach { locater =>
              store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, locater)
            }

          case op: OpportunisticRebuild => store.frontend.readObjectForOpportunisticRebuild(op)

          case s: TransactionCompletionQuery =>
            val isComplete = txStatusCache.getStatus(s.transactionId) match {
              case None => false
              case Some(e) => e.status match {
                case TransactionStatus.Unresolved => false
                case _ => true
              }
            }
            val r = TransactionCompletionResponse(s.fromClient, s.toStore, s.queryUUID, isComplete)
            net.sendClientResponse(r)
        }
      }

      case Repair(storeId, os, completion) => stores.get(storeId).foreach: store =>
        store.repair(os, completion)

      case RecoveryEvent() =>
        handleRecoveryEvent()

      case LoadStore(backend, p) =>
        val store = new Store(ec, backend, objectCacheFactory(), net, backgroundTasks, crl,
          txStatusCache,finalizerFactory, txDriverFactory, heartbeatPeriod*8)
        backend.setCompletionHandler(ioHandler)
        stores += (backend.storeId -> store)

        if Files.exists(backend.crlSaveFile) then
          val (storeId, trs, ars) = CrashRecoveryLog.loadStoreState(backend.crlSaveFile)
          crl.loadStore(storeId, trs, ars).foreach: _ =>
            Files.delete(backend.crlSaveFile)
            p.success(())
        else
          p.success(())

      case LoadStoreById(storageDeviceId, storeId) =>
        storageDevices.get(storageDeviceId).foreach: sds =>
          tryLoadStore(sds, sds.devicePath.resolve(storeId.directoryName).toFile)
        
      case HeartbeatEvent() =>
        //logger.trace("Main loop got heartbeat event")
        stores.valuesIterator.foreach(_.heartbeat())
        
      case ShutdownStore(storeId, completion) =>
        stores.get(storeId) match
          case None => completion.success(())
          case Some(store) =>
            stores -= storeId
            crl.closeStore(storeId).foreach: (trs, ars) =>
              CrashRecoveryLog.saveStoreState(storeId, trs, ars, store.backend.crlSaveFile)
              store.close().foreach: _ =>
                completion.success(())
        
      case null => // nothing to do
      
      case _:Exit =>
        shutdownCalled = true
        shutdownPromise.success(())
    }
  }
}
