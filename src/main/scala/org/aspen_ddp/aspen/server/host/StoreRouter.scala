package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{ObjectId, ReadError}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.server.repair.StoreNotHosted
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.CompletionHandler
import scribe.Logging

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

object StoreRouter:
  /** A loaded store and the executor it runs on. A store loaded again gets a new Entry.
   *
   *  `closed` is confined to the executor: the store's close (or its replacement by a reload) sets it there, so tasks
   *  queued ahead of the close still run against the open store (spec section 5) and tasks queued behind it never
   *  reach it. `closing` is set on the executor when the close starts and read by I/O completions as they are
   *  posted: those posted earlier are delivered, those posted later dropped (StoreLoader.startClose). */
  final class Entry(val store: Store, val executor: StoreExecutor):
    var closed: Boolean = false
    @volatile var closing: Boolean = false

/** The loaded stores, and the delivery of messages, I/O completions and repairs to them (spec section 4).
 *
 *  The map of loaded stores and the set of offline stores are concurrent: only the control thread writes them, and
 *  any thread may read them. Every write republishes the host's HostStoreView.
 *
 *  Work for a store reaches it in one of two ways:
 *  - From a foreign thread (network, timer, CRL callback): `post` and the `deliver*`, `repair*` and
 *    `heartbeat` methods built on it. One `threading.dispatch` step looks the store up, then hands the task to the
 *    store's executor, where it runs unless the store's close has started there. I/O completions skip the lookup:
 *    each backend's handler is bound to its entry (`completionHandler`). In production the dispatch step
 *    runs on the calling thread; in lockstep it is the one queued item, and the executor runs inline when it is
 *    drained, as the host's event loop did.
 *  - From the control thread: `onStore` and `onEachStore`, straight to the executor with no dispatch step, and any
 *    result returned through a Future whose callbacks name the control context.
 *
 *  A store that is not loaded but is marked offline is silent: a message for it is dropped instead of answered with
 *  an UnknownStore reply. The mark is set when a store is removed and cleared when one is installed; stores that were
 *  never loaded are marked and unmarked directly, so `offline` is not a subset of the stores loaded. `remove` marks
 *  the store offline before it leaves the map, so any thread that finds it gone also finds it offline. */
class StoreRouter(ctx: DeviceControl) extends Logging:
  import StoreRouter.Entry

  private val stores = new ConcurrentHashMap[StoreId, Entry]()
  private val offline: java.util.Set[StoreId] = ConcurrentHashMap.newKeySet[StoreId]()

  /** Set (markReady) by a control event the Host constructor posts after its device scan, so FIFO order puts it after
   *  every LoadStore that scan queued. Until then a message for a store not loaded is dropped silently rather than
   *  answered UnknownStore/StoreNotFound: production dispatch is inline, and traffic arriving between the network
   *  starting and the host's start would otherwise be told that stores about to load are not here, costing clients
   *  re-reads, location-cache drops and missed-update marks at every host start. Silence makes them retry, as for an
   *  offline store. */
  @volatile private var ready = false

  /** Called on the control thread once the stores found at startup have been loaded (their LoadStores handled). */
  private[server] def markReady(): Unit = ready = true

  /** Whether a message for a store that is not loaded should be answered: never while starting, nor for an offline
   *  store. */
  private def answersUnknown(storeId: StoreId): Boolean = ready && ! offline.contains(storeId)

  /** Runs `task` on the store's executor if it is loaded, unless its close has started there by then. */
  def post(storeId: StoreId)(task: Store => Unit): Unit = route(storeId, () => ())(task)

  /** `post`, with `missing` run instead of the task when the store is not installed: in the dispatch step if it is
   *  not loaded, or on its executor if its close started in between. Exactly one of the two runs. */
  private def route(storeId: StoreId, missing: () => Unit)(task: Store => Unit): Unit =
    ctx.threading.dispatch.execute: () =>
      val entry = stores.get(storeId)
      if entry == null then
        missing()
      else
        entry.executor.execute: () =>
          if ! entry.closed then
            task(entry.store)
          else
            missing()

  /** One dispatch step that runs `onEachStore`. Callable from any thread. */
  private def postToEachStore(task: Store => Unit): Unit =
    ctx.threading.dispatch.execute(() => onEachStore(task))

  /** Posts `task` to each loaded store's executor, where it runs unless that store's close has started. Called on the
   *  control thread or in a dispatch step. */
  def onEachStore(task: Store => Unit): Unit =
    stores.values.forEach: entry =>
      entry.executor.execute: () =>
        if ! entry.closed then
          task(entry.store)

  /** Control side: runs `f` on the store's executor unless its close has started there, and completes with its
   *  result, or None if the store is not loaded or its close started first. */
  def onStore[T](storeId: StoreId)(f: Store => T): Future[Option[T]] =
    Option(stores.get(storeId)) match
      case None => Future.successful(None)
      case Some(entry) =>
        val p = Promise[Option[T]]()
        entry.executor.execute: () =>
          if ! entry.closed then
            try p.success(Some(f(entry.store)))
            catch case NonFatal(t) => p.failure(t)
          else
            p.success(None)
        p.future

  /** The completion handler of `entry`'s backend, set before the entry is installed. It never looks in the map: a
   *  completion arriving after `remove` but before the close starts on the executor belongs to I/O the still-open
   *  store issued (reads queued ahead of the close), so it is delivered; only those arriving after the close has
   *  started there are dropped (spec section 5). One dispatch step, then the executor: in lockstep it is one queued
   *  item, as the host's IOCompletion event was. */
  private[host] def completionHandler(entry: Entry): CompletionHandler = op =>
    ctx.threading.dispatch.execute: () =>
      if ! entry.closing then
        entry.executor.execute: () =>
          if ! entry.closed then
            entry.store.frontend.backendOperationComplete(op)

  def deliverTx(msg: TxMessage): Unit =
    val unknown = () =>
      if answersUnknown(msg.to) then
        val reply = TxUnknownStore(msg.from, msg.to, msg.transactionId)
        ctx.net.sendTransactionMessage(reply)
    route(msg.to, unknown)(_.receiveTransactionMessage(msg))

  def deliverClient(msg: ClientRequest): Unit = msg match
    case r: Read =>
      val unknown = () =>
        if answersUnknown(r.toStore) then
          val msg = ReadResponse(
            r.fromClient,
            r.toStore,
            r.readUUID,
            HLCTimestamp.now,
            Left(ReadError.StoreNotFound)
          )
          ctx.net.sendClientResponse(msg)
      route(r.toStore, unknown): store =>
        if r.objectPointer.poolId == store.storeId.poolId then
          store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer, r.readType)

    case op: OpportunisticRebuild => post(op.toStore)(_.frontend.readObjectForOpportunisticRebuild(op))

    case s: TransactionCompletionQuery =>
      // Reads only the thread-safe txStatusCache, so it is answered in the dispatch step
      ctx.threading.dispatch.execute: () =>
        if stores.containsKey(s.toStore) then
          val isComplete = ctx.txStatusCache.getStatus(s.transactionId) match
            case None => false
            case Some(e) => e.status match
              case TransactionStatus.Unresolved => false
              case _ => true
          val r = TransactionCompletionResponse(s.fromClient, s.toStore, s.queryUUID, isComplete)
          ctx.net.sendClientResponse(r)

  def repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
    val notHosted: () => Unit = () =>
      // The store left between the sweep selecting it and this task running. Leaving
      // the promise open would strand the scan that awaits it, and with it the sweep's
      // single-flight slot -- repair would stop host-wide until the process restarts.
      // Failing also leaves the errorTree entry in place for the store's new host.
      logger.debug(s"Ignoring repair of ${os.pointer.id}: store $storeId is not hosted here")
      completion.failure(StoreNotHosted(storeId))
    route(storeId, notHosted)(_.repair(os, completion))

  def repairDelete(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte], completion: Promise[Unit]): Unit =
    val notHosted: () => Unit = () =>
      // Must fail, not succeed. Succeeding would tell the caller the slice was deleted, and
      // the caller would drop the errorTree entry -- leaving the object on the departed
      // store with no record that it needs removing.
      logger.debug(s"Ignoring repair deletion of $objectId: store $storeId is not hosted here")
      completion.failure(StoreNotHosted(storeId))
    route(storeId, notHosted)(_.repairDelete(objectId, storePointer, completion))

  /** One dispatch step that posts `store.heartbeat()` to each loaded store's executor. */
  def heartbeat(): Unit = postToEachStore(_.heartbeat())

  /** The CRL holds the state loaded from `store`'s crl_save.log. On its executor, if it is still the store
   *  installed: removes the save file and recovers its transactions. `p` is completed either way. A store closed
   *  meanwhile keeps its save file, which its close rewrote, and recovers from it when next loaded. */
  private[host] def crlLoaded(store: Store, saveFile: os.Path, p: Promise[Unit]): Unit =
    route(store.storeId, () => { p.success(()); () }): installed =>
      if installed eq store then
        os.remove(saveFile)
        installed.frontend.recover()
      p.success(())

  def get(storeId: StoreId): Option[Store] = Option(stores.get(storeId)).map(_.store)

  /** The store's entry, if it is loaded */
  private[host] def entry(storeId: StoreId): Option[Entry] = Option(stores.get(storeId))

  def isOffline(storeId: StoreId): Boolean = offline.contains(storeId)

  /** Callable from any thread (TestNetwork's checker thread): Store.hasTransactions is synchronized. */
  def hasTransactions: Boolean = stores.values.asScala.exists(_.store.hasTransactions)

  def logTransactionStatus(log: String => Unit): Unit = stores.values.forEach(_.store.logTransactionStatus(log))

  /** A snapshot of the loaded stores, by id */
  private[aspen] def loaded: Map[StoreId, Store] = stores.asScala.view.mapValues(_.store).toMap

  /** A snapshot of the stores marked offline */
  private[server] def offlineStores: Set[StoreId] = offline.asScala.toSet

  /** `install` of a new entry, for a store whose backend needs no completion handler bound to it (tests). */
  private[host] def install(store: Store, executor: StoreExecutor): Unit = install(new Entry(store, executor))

  /** Makes the entry's store loaded and clears its offline mark. From here on only the executor touches the store:
   *  the map's put is the happens-before edge for everything the control thread did to it first. */
  private[host] def install(entry: Entry): Unit =
    val store = entry.store
    val replaced = stores.put(store.storeId, entry)
    // A reload over a loaded store, with no close: its queued tasks run, later ones skip it
    if replaced != null then
      replaced.executor.execute: () =>
        replaced.closing = true
        replaced.closed = true
    offline.remove(store.storeId)
    ctx.publishView()

  /** Marks the store offline, then unloads it, returning the entry it had. */
  private[host] def remove(storeId: StoreId): Option[Entry] =
    offline.add(storeId)
    val removed = Option(stores.remove(storeId))
    ctx.publishView()
    removed

  /** Marks a store offline without touching the loaded stores */
  private[host] def markOffline(storeId: StoreId): Unit =
    offline.add(storeId)
    ctx.publishView()

  /** Clears a store's offline mark without touching the loaded stores */
  private[host] def clearOffline(storeId: StoreId): Unit =
    offline.remove(storeId)
    ctx.publishView()
