package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{EvictingQueue, atomicWrite}
import scribe.Logging

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object MetadataManager:

  class HostEntry(val hostId: HostId,
                  val name: String,
                  val address: String,
                  val dataPort: Int,
                  val cncPort: Int,
                  val storeTransferPort: Int)

  trait NetworkImplInterface[T <: HostEntry]:
    def createHostEntry(hostId: HostId,
                        name: String,
                        address: String,
                        dataPort: Int,
                        cncPort: Int,
                        storeTransferPort: Int,
                        queuedMessages: EvictingQueue[Message]): T

    def storeResolved(hostEntry: T, storeId: StoreId, queuedMessages: EvictingQueue[Message]): Unit

  private class PendingPoolLookup(storeQueueSize: Int):
    var storeQueues: Map[StoreId, EvictingQueue[Message]] = Map()

    def enqueueMessage(storeId: StoreId, msg: Message): Unit =
      val q = storeQueues.get(storeId) match
        case Some(o) => o
        case None =>
          val newQ = new EvictingQueue[Message](storeQueueSize)
          storeQueues += storeId -> newQ
          newQ
      q.enqueue(msg)

  private class PendingHostLookup(hostQueueSize: Int):
    val messageQueue = new EvictingQueue[Message](hostQueueSize)

    def enqueueMessage(msg: Message): Unit = messageQueue.enqueue(msg)

    def drainIntoQueue(storeQueue: EvictingQueue[Message]): Unit =
      while !storeQueue.isEmpty do
        storeQueue.dequeue().foreach(messageQueue.enqueue)


class MetadataManager[T <: MetadataManager.HostEntry](val bootstrapConfigFile: os.Path,
                                                      val networkImplInterface: MetadataManager.NetworkImplInterface[T],
                                                      val pendingStoreLookupQueueSize: Int = 20,
                                                      val pendingHostLookupQueueSize: Int = 100) extends Logging:
  import MetadataManager.*

  private var pendingPoolLookups: Map[PoolId, PendingPoolLookup] = Map()
  private var oClient: Option[AspenClient] = None
  private var bootstrapStores: Set[StoreId] = Set()
  private var stores: Map[StoreId, HostId] = Map()
  private var hosts: Map[HostId, Either[PendingHostLookup, T]] = Map()
  private var refreshingBootstrapConfig: Boolean = false

  BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO).hosts.foreach: bsHost =>
    bsHost.stores.foreach: storeId =>
      stores += storeId -> bsHost.hostId
      bootstrapStores += storeId

    hosts += bsHost.hostId -> Right(networkImplInterface.createHostEntry(
      bsHost.hostId,
      bsHost.name,
      bsHost.address,
      bsHost.dataPort,
      bsHost.cncPort,
      bsHost.storeTransferPort,
      new EvictingQueue[Message](1)
    ))

  def setAspenClient(client: AspenClient): Unit =
    synchronized:
      oClient = Some(client)

  def isBootstrapStore(storeId: StoreId): Boolean = bootstrapStores.contains(storeId)

  def receivedUnknownStoreFromHost(hostId: HostId, storeId: StoreId): Unit =
    synchronized:
      if isBootstrapStore(storeId) then
        refreshBootstrapConfig()
      else
        stores.get(storeId) match
          case None =>
          case Some(oldHostId) =>
            if oldHostId == hostId then
              stores -= storeId

  def getHostEntry(hostId: HostId): Option[T] =
    synchronized:
      hosts.get(hostId) match
        case Some(e) => e match
          case Right(hostEntry) => Some(hostEntry)
          case Left(pendingHostLookup) => None
        case None =>
          startHostLookup(hostId, None)
          None

  def getHostEntryOrQueueMessage(hostId: HostId, msg: Message): Option[T] =
    synchronized:
      hosts.get(hostId) match
        case Some(e) => e match
          case Right(hostEntry) => Some(hostEntry)
          case Left(pendingHostLookup) =>
            pendingHostLookup.enqueueMessage(msg)
            None
        case None =>
          startHostLookup(hostId, Some(msg))
          None

  def getHostEntryOrQueueMessage(storeId: StoreId, msg: Message): Option[T] =
    synchronized:
      stores.get(storeId) match
        case Some(hostId) => getHostEntryOrQueueMessage(hostId, msg)
        case None =>
          pendingPoolLookups.get(storeId.poolId) match
            case Some(ppl) =>
              ppl.enqueueMessage(storeId, msg)
              None
            case None =>
              startPoolLookup(storeId, msg)
              None

  private def refreshBootstrapConfig(): Unit =
    if !refreshingBootstrapConfig then
      logger.info("Refreshing bootstrap config file")
      oClient match
        case None => logger.error(s"Refreshing bootstrap config before AspenClient initialized!")
        case Some(client) =>
          refreshingBootstrapConfig = true
          given ExecutionContext = client.clientContext

          client.getBootstrapConfig().foreach: cfg =>
            try
              atomicWrite(bootstrapConfigFile.toNIO, cfg)
              logger.info(s"Updated bootstrap config written to $bootstrapConfigFile")
            catch
              case err => logger.error(s"Failed to update bootstrap config file $bootstrapConfigFile. Error: $err")
            finally
              synchronized:
                // Update the stores map to match the new host ids
                BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO).hosts.foreach: bsHost =>
                  bsHost.stores.foreach: storeId =>
                    stores += storeId -> bsHost.hostId

                refreshingBootstrapConfig = false


  private def startHostLookup(hostId: HostId, oMsg: Option[Message]): Unit =
    oClient match
      case None => logger.error(s"Host lookup preformed before AspenClient initialized. HostId: $hostId")
      case Some(client) =>
        given ExecutionContext = client.clientContext
        val phl = new PendingHostLookup(pendingHostLookupQueueSize)
        oMsg.foreach(phl.enqueueMessage)
        hosts += hostId -> Left(phl)
        client.getHostState(hostId).onComplete:
          case Failure(err) =>
            logger.error(s"HostState lookup failed for hostId $hostId. Error: $err")
            hosts -= hostId
          case Success(hostState) =>
            synchronized:
              hosts += hostId -> Right(networkImplInterface.createHostEntry(
                hostId,
                hostState.name,
                hostState.address,
                hostState.dataPort,
                hostState.cncPort,
                hostState.storeTransferPort,
                phl.messageQueue
              ))

  private def startPoolLookup(storeId: StoreId, msg: Message): Unit =
    oClient match
      case None => logger.error(s"StoragePool lookup preformed before AspenClient initialized. PoolId: ${storeId.poolId}")
      case Some(client) =>
        given ExecutionContext = client.clientContext

        val ppl = new PendingPoolLookup(pendingStoreLookupQueueSize)
        ppl.enqueueMessage(storeId, msg)
        pendingPoolLookups += storeId.poolId -> ppl
        client.getStoragePoolState(storeId.poolId).onComplete:
          case Failure(err) =>
            logger.error(s"StoragePool lookup failed for poolId ${storeId.poolId}. Error: $err")
            pendingPoolLookups -= storeId.poolId
          case Success(poolState) =>
            synchronized:
              pendingPoolLookups -= storeId.poolId
              poolState.stores.zipWithIndex.foreach: (se, index) =>
                val sid = StoreId(storeId.poolId, index.toByte)
                stores += sid -> se.hostId
                ppl.storeQueues.get(sid).foreach: storeQueue =>
                  hosts.get(se.hostId) match
                    case None =>
                    case Some(e) => e match
                      case Left(phl) => phl.drainIntoQueue(storeQueue)
                      case Right(hostEntry) => networkImplInterface.storeResolved(hostEntry, sid, storeQueue)
