package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.common.util.BackgroundTask
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, Completion, CompletionHandler}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.store.{BackendStoreLoader, Frontend, Store}
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend}

import java.io.File
import java.nio.file.{Files, Path}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

object StoreManager {
  sealed abstract class Event

  case class IOCompletion(op: Completion) extends Event
  case class TransactionMessage(msg: TxMessage) extends Event
  case class ClientReq(msg: ClientRequest) extends Event
  case class Repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]) extends Event
  case class LoadStore(backend: Backend, completion: Promise[Unit]) extends Event
  case class LoadStoreById(storeId: StoreId) extends Event
  case class Exit() extends Event
  case class RecoveryEvent() extends Event
  case class HeartbeatEvent() extends Event
  case class ShutdownStore(storeId: StoreId, completion: Promise[Unit]) extends Event

  class IOHandler(mgr: StoreManager) extends CompletionHandler {
    override def complete(op: Completion): Unit = {
      mgr.events.add(IOCompletion(op))
    }
  }
  
}

class StoreManager(val rootDir: Path,
                   val ec: ExecutionContext,
                   val objectCacheFactory: () => ObjectCache,
                   val net: Messenger,
                   val backgroundTasks: BackgroundTask,
                   crlFactory: CrashRecoveryLogFactory,
                   val finalizerFactory: TransactionFinalizer.Factory,
                   val txDriverFactory: TransactionDriver.Factory,
                   val storeLoader: BackendStoreLoader,
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

  val storesDir: Path = rootDir.resolve("stores")

  def start(): Unit = {
    threadPool.submit(new Runnable {
      override def run(): Unit = {
        while (!shutdownCalled) {
          var event = events.poll(3, TimeUnit.SECONDS)
          while (event != null) {
            handleEvent(event)
            event = events.poll(0, TimeUnit.SECONDS)
          }
        }
      }
    })
  }

  backgroundTasks.schedulePeriodic(heartbeatPeriod) {
    events.put(HeartbeatEvent())
  }

  protected var stores: Map[StoreId, Store] = Map()

  val filesArray = rootDir.resolve("stores").toFile.listFiles()

  if filesArray != null then
    filesArray.toList.filter{ fn =>
      Files.exists(fn.toPath.resolve("store_config.yaml"))
    }.foreach: fn =>
      loadStoreFromPath(fn.toPath)
  
  protected def loadStoreFromPath(storePath: Path): Unit =
    storeLoader.loadStoreFromPath(storePath) match
      case None => logger.info(s"Skipping non-store directory: ${storePath}")
      case Some(backend) =>
        logger.info(s"Loading store $storePath")
        loadStore(backend)

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

  def loadStoreById(storeId: StoreId): Unit =
    events.put(LoadStoreById(storeId))

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

  def hasEvents: Boolean = {
    events.size() != 0
  }

  /** Handles all events in the event queue. Returns when the queue is empty */
  def handleEvents(): Unit = {
    var event = events.poll(0, TimeUnit.NANOSECONDS)
    while (event != null) {
      handleEvent(event)
      event = events.poll(0, TimeUnit.NANOSECONDS)
    }
  }

  /** Preforms a blocking poll on the event queue, awaitng the delivery of an event */
  protected def awaitEvent(): Unit = {
    handleEvent(events.poll(1, TimeUnit.DAYS))
  }

  private def handleEvent(event: Event): Unit = {
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
        val store = new Store(backend, objectCacheFactory(), net, backgroundTasks, crl,
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

      case LoadStoreById(storeId) =>
        if ! stores.contains(storeId) then
          loadStoreFromPath(storesDir.resolve(storeId.directoryName))
        
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
