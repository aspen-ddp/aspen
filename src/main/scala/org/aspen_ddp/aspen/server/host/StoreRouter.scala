package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{ObjectId, ReadError}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.server.repair.StoreNotHosted
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.{Completion, CompletionHandler}
import scribe.Logging

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

object StoreRouter:
  /** A loaded store and the executor it runs on. Compared by identity: a store loaded again gets a new Entry. */
  final class Entry(val store: Store, val executor: StoreExecutor)

/** The loaded stores, and the delivery of messages, I/O completions and repairs to them (spec section 4).
 *
 *  The map of loaded stores and the set of offline stores are concurrent: only the control thread writes them, and
 *  any thread may read them. Every write republishes the host's HostStoreView.
 *
 *  Work for a store reaches it in one of two ways:
 *  - From a foreign thread (network, I/O completion, timer, CRL callback): `post` and the `deliver*`, `repair*` and
 *    `heartbeat` methods built on it. One `threading.dispatch` step looks the store up, then hands the task to the
 *    store's executor, where it runs only if that same store is still installed. In production the dispatch step
 *    runs on the calling thread; in lockstep it is the one queued item, and the executor runs inline when it is
 *    drained, as the host's event loop did.
 *  - From the control thread: `onStore`, straight to the executor with no dispatch step, and its result returned
 *    through a Future whose callbacks name the control context.
 *
 *  A store that is not loaded but is marked offline is silent: a message for it is dropped instead of answered with
 *  an UnknownStore reply. The mark is set when a store is removed and cleared when one is installed; stores that were
 *  never loaded are marked and unmarked directly, so `offline` is not a subset of the stores loaded. `remove` marks
 *  the store offline before it leaves the map, so any thread that finds it gone also finds it offline. */
class StoreRouter(ctx: DeviceControl) extends Logging:
  import StoreRouter.Entry

  private val stores = new ConcurrentHashMap[StoreId, Entry]()
  private val offline: java.util.Set[StoreId] = ConcurrentHashMap.newKeySet[StoreId]()

  /** Runs `task` on the store's executor if it is loaded, and is still installed when the task runs. */
  def post(storeId: StoreId)(task: Store => Unit): Unit = route(storeId, () => ())(task)

  /** `post`, with `missing` run instead of the task when the store is not installed: in the dispatch step if it is
   *  not loaded, or on its executor if it left in between. Exactly one of the two runs. */
  private def route(storeId: StoreId, missing: () => Unit)(task: Store => Unit): Unit =
    ctx.threading.dispatch.execute: () =>
      val entry = stores.get(storeId)
      if entry == null then
        missing()
      else
        entry.executor.execute: () =>
          if stores.get(storeId) eq entry then
            task(entry.store)
          else
            missing()

  /** One dispatch step that posts `task` to each loaded store's executor, where it runs only if that store is still
   *  installed. Callable from any thread. */
  def postToEachStore(task: Store => Unit): Unit =
    ctx.threading.dispatch.execute: () =>
      stores.values.forEach: entry =>
        entry.executor.execute: () =>
          if stores.get(entry.store.storeId) eq entry then
            task(entry.store)

  /** Control side: runs `f` on the store's executor while it is still installed, and completes with its result, or
   *  None if the store is not (or no longer) installed. */
  def onStore[T](storeId: StoreId)(f: Store => T): Future[Option[T]] =
    Option(stores.get(storeId)) match
      case None => Future.successful(None)
      case Some(entry) =>
        val p = Promise[Option[T]]()
        entry.executor.execute: () =>
          if stores.get(storeId) eq entry then
            try p.success(Some(f(entry.store)))
            catch case NonFatal(t) => p.failure(t)
          else
            p.success(None)
        p.future

  /** Each backend's completion handler: delivers its completions to its store. */
  private[host] def completionHandler(): CompletionHandler = op => deliverCompletion(op)

  def deliverTx(msg: TxMessage): Unit =
    val unknown = () =>
      if ! offline.contains(msg.to) then
        val reply = TxUnknownStore(msg.from, msg.to, msg.transactionId)
        ctx.net.sendTransactionMessage(reply)
    route(msg.to, unknown)(_.receiveTransactionMessage(msg))

  def deliverClient(msg: ClientRequest): Unit = msg match
    case r: Read =>
      val unknown = () =>
        if ! offline.contains(r.toStore) then
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

  def deliverCompletion(op: Completion): Unit =
    post(op.storeId)(_.frontend.backendOperationComplete(op))

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

  /** Makes the store loaded on `executor` and clears its offline mark. From here on only the executor touches the
   *  store: the map's put is the happens-before edge for everything the control thread did to it first. */
  private[host] def install(store: Store, executor: StoreExecutor): Unit =
    stores.put(store.storeId, new Entry(store, executor))
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
