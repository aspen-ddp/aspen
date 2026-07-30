package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{EvictingQueue, atomicWrite}
import scribe.Logging

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
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
                                                      // Must be >= pendingStoreLookupQueueSize; see
                                                      // startPoolLookup's rescue of a parked store queue
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

  /** Returns the host entry only if it is already resolved. Unlike getHostEntry, this never
   *  starts a host lookup, so it is side-effect free and safe to call from a polling loop.
   *
   *  Because of that, it cannot drive resolution forward on its own: some earlier get*HostEntry
   *  call must have started the lookup. A failed lookup drops the entry entirely, returning the
   *  host to the never-looked-up state, so a poll-only caller will spin until its own timeout. */
  def peekHostEntry(hostId: HostId): Option[T] =
    synchronized:
      hosts.get(hostId) match
        case Some(Right(hostEntry)) => Some(hostEntry)
        case _ => None

  /** Every host entry that has resolved, in no particular order. Excludes hosts whose lookup
   *  is still pending and hosts never looked up at all.
   *
   *  Exists for ZMQNet.awaitPendingMessagesSent, which has to inspect every host rather than
   *  one named host: its callers cannot name the hosts a message was addressed to. */
  def resolvedHostEntries: List[T] =
    synchronized:
      hosts.values.collect { case Right(hostEntry) => hostEntry }.toList

  /** True if any message is parked behind an unresolved host or pool lookup.
   *
   *  A parked message is invisible to a caller inspecting host entries: the host it is
   *  addressed to has no entry yet, so there is nothing to look at. A drain that ignored this
   *  would report success while still holding the message -- which is the common case for a
   *  nudge sent to a host the process has not talked to before.
   *
   *  Note that a failed host or pool lookup drops the entry and everything parked on it (see
   *  peekHostEntry's scaladoc for the host case), so this can go false because the message was
   *  discarded rather than sent. Nothing at this layer can tell the two apart.
   *
   *  A message parked on a pool lookup moves to a host lookup when the pool resolves onto a host
   *  that is not already resolved, so that drop can happen at a later stage than the one the
   *  message was originally parked on. */
  def hasParkedMessages: Boolean =
    synchronized:
      val parkedOnHost = hosts.values.exists:
        case Left(pendingHostLookup) => !pendingHostLookup.messageQueue.isEmpty
        case Right(_) => false

      val parkedOnPool = pendingPoolLookups.values.exists: ppl =>
        ppl.storeQueues.values.exists(q => !q.isEmpty)

      parkedOnHost || parkedOnPool

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

  def dropStoreMapping(storeId: StoreId): Unit =
    synchronized:
      if isBootstrapStore(storeId) then
        refreshBootstrapConfig()
      else
        stores -= storeId

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
    val phl = new PendingHostLookup(pendingHostLookupQueueSize)
    oMsg.foreach(phl.enqueueMessage)
    startHostLookup(hostId, phl)

  /** Starts `hostId`'s lookup, parking `phl`'s messages until it resolves.
   *
   *  The caller supplies the PendingHostLookup already seeded rather than adding to it
   *  afterwards, because getHostState's continuation can run inline on this thread -- it does in
   *  tests under ExecutionContext.parasitic, and would under any EC that dispatches an
   *  already-complete future's callback directly -- and it builds the host entry out of
   *  phl.messageQueue. Anything enqueued after this returns would miss that handoff.
   *
   *  If no client is set the lookup is not started and phl's messages are discarded along with
   *  it. That is unreachable from a caller already running inside a client callback, which is
   *  where the rescued-queue call site lives.
   *
   *  Caller must hold this object's monitor. */
  private def startHostLookup(hostId: HostId, phl: PendingHostLookup): Unit =
    oClient match
      case None => logger.error(s"Host lookup preformed before AspenClient initialized. HostId: $hostId")
      case Some(client) =>
        given ExecutionContext = client.clientContext
        hosts += hostId -> Left(phl)
        try
          client.getHostState(hostId).onComplete:
            case Failure(err) =>
              logger.error(s"HostState lookup failed for hostId $hostId. Error: $err")
              synchronized:
                hosts -= hostId
            case Success(hostState) =>
              synchronized:
                try
                  hosts += hostId -> Right(networkImplInterface.createHostEntry(
                    hostId,
                    hostState.name,
                    hostState.address,
                    hostState.dataPort,
                    hostState.cncPort,
                    hostState.storeTransferPort,
                    phl.messageQueue
                  ))
                catch
                  case NonFatal(t) =>
                    // Same rule as a failed lookup: drop back to never-looked-up so a later call
                    // retries. Without this the entry stays at Left forever -- the lookup did
                    // resolve, so nothing will ever run again to advance or remove it. Repair
                    // first and log second, so a logger that throws cannot leave the wedge behind.
                    hosts -= hostId
                    logger.error(s"Failed to create the host entry for hostId $hostId. Error: $t", t)
        catch
          case NonFatal(t) =>
            // A lookup that fails by throwing is still a failed lookup, so undo the pending entry
            // installed above and let a later call retry -- exactly what the Failure branch does.
            // Left in place it would never resolve, because no continuation exists to resolve it:
            // the host would be unreachable and hasParkedMessages stuck true for the life of the
            // process. Clean up the map first, then log: if logger.error itself throws the entry
            // is already repaired. The monitor is held by the caller, per this method's contract,
            // so the removal needs no synchronized of its own.
            //
            // The entry removed is always the Left installed above, never a freshly installed
            // Right. A NonFatal throw from the Success continuation is caught by
            // Promise.Transformation and routed to reportFailure rather than rethrown, and a fatal
            // one -- which Transformation does rethrow, out through onComplete -- is not matched
            // below. Neither can reach this removal after the Right has landed.
            //
            // Swallowing rather than rethrowing is what keeps a failed lookup from taking down the
            // send loop -- regardless of whether that loop has its own guard, this layer must not
            // depend on one existing.
            hosts -= hostId
            logger.error(s"HostState lookup call threw for hostId $hostId. Error: $t", t)

  /** Starts `storeId.poolId`'s lookup, parking `msg` and any later messages to the pool until it
   *  resolves. When the pool resolves, every store in it is mapped to its host, and each store's
   *  queue is handed off to that host -- either to an already-resolved host entry or onto a host
   *  lookup if that host is unknown.
   *
   *  If no client is set the lookup is not started and the message is discarded.
   *
   *  Caller must hold this object's monitor. */
  private def startPoolLookup(storeId: StoreId, msg: Message): Unit =
    oClient match
      case None => logger.error(s"StoragePool lookup preformed before AspenClient initialized. PoolId: ${storeId.poolId}")
      case Some(client) =>
        given ExecutionContext = client.clientContext

        val ppl = new PendingPoolLookup(pendingStoreLookupQueueSize)
        ppl.enqueueMessage(storeId, msg)
        pendingPoolLookups += storeId.poolId -> ppl
        try
          client.getStoragePoolState(storeId.poolId).onComplete:
            case Failure(err) =>
              logger.error(s"StoragePool lookup failed for poolId ${storeId.poolId}. Error: $err")
              synchronized:
                pendingPoolLookups -= storeId.poolId
            case Success(poolState) =>
              synchronized:
                pendingPoolLookups -= storeId.poolId
                poolState.stores.zipWithIndex.foreach: (se, index) =>
                  val sid = StoreId(storeId.poolId, index.toByte)
                  // Outside the try on purpose. Nothing here can throw, so this is legibility
                  // rather than mechanism: the mapping is the part that must land for every store
                  // regardless of what the handoff does, and its position says so.
                  stores += sid -> se.hostId
                  try
                    ppl.storeQueues.get(sid).foreach: storeQueue =>
                      hosts.get(se.hostId) match
                        case None =>
                          // The pool named a host this process has never looked up. Move the queue
                          // onto a host lookup instead of dropping it: pendingPoolLookups was cleared
                          // above, so nothing else will ever come back for these messages. Doing it
                          // here, inside the same synchronized block, is also what keeps
                          // hasParkedMessages from dipping false while they are between queues.
                          // The move is guaranteed loss-free only while pendingHostLookupQueueSize is
                          // at least pendingStoreLookupQueueSize; otherwise EvictingQueue silently
                          // drops the oldest of what is being rescued.
                          // A fresh queue per store: one shared across the loop would end up
                          // referenced by every rescued host entry, so whichever host resolved first
                          // would drain all of them.
                          val phl = new PendingHostLookup(pendingHostLookupQueueSize)
                          phl.drainIntoQueue(storeQueue)
                          startHostLookup(se.hostId, phl)
                        case Some(e) => e match
                          case Left(phl) => phl.drainIntoQueue(storeQueue)
                          case Right(hostEntry) => networkImplInterface.storeResolved(hostEntry, sid, storeQueue)
                  catch
                    case NonFatal(t) =>
                      // Contain the throw to this store. Uncaught it escapes foreach, and since
                      // pendingPoolLookups was cleared above, every later store's queue becomes
                      // unreachable -- the tail of the pool lost to one store's failure.
                      //
                      // Now that startHostLookup swallows, the None and Left branches cannot throw
                      // and storeResolved is the only live throw site here. The guard spans the
                      // handoff anyway, because the handoff is the unit being contained -- not
                      // because of where the throw happens to come from today.
                      logger.error(s"Handoff failed for messages parked on store $sid, host ${se.hostId}. " +
                                   s"Those messages may not have been delivered; the rest of the pool " +
                                   s"is unaffected. Error: $t", t)
        catch
          case NonFatal(t) =>
            // Same rule as startHostLookup: a lookup that fails by throwing is a failed lookup, so
            // undo the pending entry and let a later call retry. Left in place it wedges the whole
            // pool -- every store in it, not just the one addressed here. Repair first and log
            // second, so a logger that throws cannot leave the wedge behind.
            pendingPoolLookups -= storeId.poolId
            logger.error(s"StoragePool lookup call threw for poolId ${storeId.poolId}. Error: $t", t)
