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

  loadInitialBootstrapConfig()

  def setAspenClient(client: AspenClient): Unit =
    synchronized:
      oClient = Some(client)

  /** Reads bootstrapStores, which a refresh rewrites; loadInitialBootstrapConfig's scaladoc has
   *  the publication argument for both writers. */
  def isBootstrapStore(storeId: StoreId): Boolean =
    synchronized:
      bootstrapStores.contains(storeId)

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
   *  discarded rather than sent. Nothing at this layer can tell the two apart. A lookup call that
   *  fails by throwing is handled the same way as one that returns a failed Future, so it has the
   *  same effect here.
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

  /** Refetches the bootstrap config and reinstalls the store -> host mappings it names, along
   *  with the set of stores that count as bootstrap stores.
   *
   *  refreshingBootstrapConfig coalesces concurrent requests: a bootstrap store whose host has
   *  moved is usually noticed by several sends at once, and one refetch answers all of them.
   *  Because it is a plain Boolean rather than an entry in a map, a path that leaves it set
   *  disables refresh permanently -- and dropStoreMapping has no fallback for a bootstrap store,
   *  so the correction is dropped rather than degraded. Every exit reachable by a throw
   *  therefore releases it, including the one that rethrows a fatal rather than reporting it.
   *  One exit is not reachable by a throw and does strand it: see the end of the next paragraph.
   *
   *  The try/catch wraps the getBootstrapConfig call and nothing else. Widening it over the
   *  onComplete registration would not catch a NonFatal throw from the callback body even under
   *  an inline or parasitic ExecutionContext: Promise.Transformation absorbs those and routes
   *  them to ExecutionContext.reportFailure, so onComplete returns normally. That is the same
   *  fact startHostLookup's catch relies on. What a wider wrapper would reach is a fatal from
   *  the callback body, which Transformation does rethrow out through onComplete -- and nothing
   *  here should intercept that: the callback's own finally has already released the flag, so
   *  the only effect would be a second, redundant release on its way past. What the narrow
   *  scope gives up is nothing a wider one could take back. An ExecutionContext that rejects
   *  the submission strands the flag -- the callback never runs, so its finally never runs --
   *  and no wrapper at this call site can reach that, because nothing throws here to catch:
   *  Transformation absorbs the rejection into reportFailure, and for a future still pending at
   *  registration the rejection is raised later, on the thread that completes it. See TODO.txt.
   *
   *  clientContext is an abstract def, so binding it can throw. A bare `given` would not help:
   *  an alias given compiles to a lazy val, so the call would not run until the implicit is
   *  first needed -- at the onComplete registration, after the flag is set and outside the
   *  wrapper, which is precisely where a throw strands it. Hence the strict val below, whose
   *  initializer runs before the flag is set, so such a throw propagates with nothing acquired.
   *  Unlike StoreManager.startDeviceCheck this method has no caller in a finally, so a
   *  propagating throw masks nothing.
   *
   *  Caller must hold this object's monitor. */
  private def refreshBootstrapConfig(): Unit =
    if !refreshingBootstrapConfig then
      logger.info("Refreshing bootstrap config file")
      oClient match
        case None => logger.error(s"Refreshing bootstrap config before AspenClient initialized!")
        case Some(client) =>
          // Strict, then aliased: forcing the abstract clientContext call here rather than
          // leaving it to the lazy given keeps it ahead of the flag. See the scaladoc.
          val ec: ExecutionContext = client.clientContext
          given ExecutionContext = ec
          refreshingBootstrapConfig = true

          val fetch =
            try Success(client.getBootstrapConfig())
            catch
              case NonFatal(t) => Failure(t)
              case t: Throwable =>
                // Not ours to swallow -- startHostLookup and startPoolLookup both let a fatal
                // out of the same position -- but it must not strand the flag either.
                refreshingBootstrapConfig = false
                throw t

          fetch match
            case Success(fCfg) =>
              fCfg.onComplete: result =>
                try
                  result match
                    case Success(cfg) => applyBootstrapConfig(cfg)
                    case Failure(err) =>
                      logger.error(s"Failed to fetch the bootstrap config. Error: $err", err)
                catch
                  case NonFatal(t) =>
                    logger.error(s"Failed to apply the fetched bootstrap config. Error: $t", t)
                finally
                  synchronized:
                    refreshingBootstrapConfig = false

            case Failure(t) =>
              // Repair first, log second: a throwing logger must not be able to wedge the flag.
              refreshingBootstrapConfig = false
              logger.error(s"The getBootstrapConfig call threw. Error: $t", t)

  /** Installs a freshly fetched bootstrap config, writing it through to disk on the way.
   *
   *  Parse first. The config is written only once it is known to be readable, so a bad fetch
   *  cannot replace a good file with one that fails at the next construction. A parse failure
   *  propagates to the caller's catch, which logs it; the guard release is in that caller's
   *  finally, so a rejected config leaves the refresh retryable.
   *
   *  Then map, then write. A failed write cannot stop the mapping: it has already been applied.
   *  And the mapping comes from the parsed config rather than from a re-read of the file, so it
   *  neither reinstates stale placements nor depends on the file being readable at all -- the
   *  process picks up the new placements immediately and only loses them across a restart,
   *  strictly better than discarding a good config because the disk is full or read-only.
   *
   *  Do not reorder those two. The mapping is the repair this refresh exists to deliver and it
   *  is free; the write is best-effort, and going first it would delay the repair by however
   *  long the disk takes.
   */
  private def applyBootstrapConfig(cfg: String): Unit =
    val config = BootstrapConfig.parseBootstrapConfig(cfg)

    synchronized:
      mapBootstrapStores(config)

    try
      atomicWrite(bootstrapConfigFile.toNIO, cfg)
      logger.info(s"Updated bootstrap config written to $bootstrapConfigFile")
    catch
      case NonFatal(t) =>
        logger.error(s"Failed to update bootstrap config file $bootstrapConfigFile. Error: $t", t)

  /** Installs every store -> host mapping the config names and marks each store as a bootstrap
   *  store.
   *
   *  Add-only: nothing is removed here, from `stores` or from `bootstrapStores`. A store the
   *  refreshed config omits therefore keeps both its old mapping and its bootstrap status, which
   *  is the conservative reading. The config is the only source that can name a bootstrap store,
   *  and forgetting one would send it down dropStoreMapping's non-bootstrap branch, which drops
   *  its mapping and leaves the next send to start a pool lookup -- against a pool whose state
   *  can only be read through the bootstrap stores themselves.
   *
   *  That is add-only along this path only, for `stores`: dropStoreMapping and
   *  receivedUnknownStoreFromHost both remove from it elsewhere. `bootstrapStores` has no removal
   *  site anywhere in the class.
   *
   *  The constructor and a refresh share this method so the two can never disagree about which
   *  stores are bootstrap stores.
   *
   *  Caller must hold this object's monitor.
   */
  private def mapBootstrapStores(config: BootstrapConfig.Config): Unit =
    config.hosts.foreach: bsHost =>
      bsHost.stores.foreach: storeId =>
        stores += storeId -> bsHost.hostId
        bootstrapStores += storeId

  /** Loads the on-disk bootstrap config at construction: the store mappings, plus a HostEntry
   *  for each named host so those hosts are reachable before any lookup completes.
   *
   *  Only the mapping half is shared with a refresh. A refresh deliberately does not touch
   *  `hosts`: an existing entry may hold parked messages and a live dealer socket -- ZMQNet's
   *  does -- so recreating it would discard queued work. The cost is that a refresh naming a host
   *  this process has never seen installs the mapping but not the entry, even though the config
   *  carries that host's address -- and the resulting host lookup routes back through the
   *  bootstrap pool it is trying to repair. See TODO.txt.
   *
   *  Synchronized despite running before the instance escapes, for two reasons. It keeps
   *  mapBootstrapStores' monitor contract unconditional, so a future caller has one rule rather
   *  than a rule and an exception. And it publishes what this method writes: `stores`,
   *  `bootstrapStores` and `hosts` are plain vars, so it is the release of this monitor at the
   *  end of construction that gives a later synchronized reader -- isBootstrapStore's, say -- a
   *  happens-before edge to these writes. Uncontended and once per manager, so it costs nothing.
   *  createHostEntry under the monitor is not a new pattern: startHostLookup's Success
   *  continuation already calls it that way. The file read and parse are hoisted out, though,
   *  both to keep that the only call-out under this monitor and to match applyBootstrapConfig,
   *  which likewise parses first and takes the monitor only to install the result.
   */
  private def loadInitialBootstrapConfig(): Unit =
    val config = BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO)

    synchronized:
      mapBootstrapStores(config)

      config.hosts.foreach: bsHost =>
        hosts += bsHost.hostId -> Right(networkImplInterface.createHostEntry(
          bsHost.hostId,
          bsHost.name,
          bsHost.address,
          bsHost.dataPort,
          bsHost.cncPort,
          bsHost.storeTransferPort,
          new EvictingQueue[Message](1)
        ))

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
   *  A synchronous throw from the lookup call is treated as a lookup failure rather than
   *  propagated: the pending entry is removed and phl's messages are dropped, exactly as the
   *  Failure branch does. No NonFatal throw from that call therefore escapes -- which matters
   *  because getHostEntryOrQueueMessage runs on ZMQNet's IO thread. That loop guards each item
   *  too, so an escape would now cost one send rather than the thread; handling it here is what
   *  makes the host retryable, whereas the send loop's guard is log-only and could not undo the
   *  pending entry.
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
                    // Retryability rule.
                    // Same rule as a failed lookup: drop back to never-looked-up so a later call
                    // retries. Without this the entry stays at Left forever -- the lookup did
                    // resolve, so nothing will ever run again to advance or remove it. Repair
                    // first and log second, so a logger that throws cannot leave the wedge behind.
                    hosts -= hostId
                    logger.error(s"Failed to create the host entry for hostId $hostId. Error: $t", t)
        catch
          case NonFatal(t) =>
            // Retryability rule.
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
                      // Containment rule: this one costs a store, it does not restore one.
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
            // Retryability rule.
            // Same rule as startHostLookup: a lookup that fails by throwing is a failed lookup, so
            // undo the pending entry and let a later call retry. Left in place it wedges the whole
            // pool -- every store in it, not just the one addressed here. Repair first and log
            // second, so a logger that throws cannot leave the wedge behind.
            pendingPoolLookups -= storeId.poolId
            logger.error(s"StoragePool lookup call threw for poolId ${storeId.poolId}. Error: $t", t)
