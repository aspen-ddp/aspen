package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{ObjectId, ReadError}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.server.repair.StoreNotHosted
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.Completion
import scribe.Logging

import scala.concurrent.Promise

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The loaded stores, and the delivery of messages, I/O completions and repairs to them.
 *
 *  A store that is not loaded but is marked offline is silent: a message for it is dropped instead of answered with
 *  an UnknownStore reply. The mark is set when a store is removed and cleared when one is installed; stores that were
 *  never loaded are marked and unmarked directly, so `offline` is not a subset of the stores loaded.
 *
 *  The two maps are immutable values in `@volatile` fields, written only on the control thread, so `storeIds`,
 *  `hasTransactions` and `logTransactionStatus` may read them from any thread (TestNetwork's checker thread calls the
 *  latter two). Every write republishes the host's HostStoreView. */
class StoreRouter(ctx: DeviceControl) extends Logging:
  @volatile private var stores: Map[StoreId, Store] = Map()
  @volatile private var offline: Set[StoreId] = Set()

  def deliverTx(msg: TxMessage): Unit =
    stores.get(msg.to) match
      case None =>
        if ! offline.contains(msg.to) then
          val reply = TxUnknownStore(msg.from, msg.to, msg.transactionId)
          ctx.net.sendTransactionMessage(reply)

      case Some(store) => store.receiveTransactionMessage(msg)

  def deliverClient(msg: ClientRequest): Unit =
    stores.get(msg.toStore) match
      case None => msg match
        case r: Read =>
          if ! offline.contains(r.toStore) then
            val msg = ReadResponse(
              r.fromClient,
              r.toStore,
              r.readUUID,
              HLCTimestamp.now,
              Left(ReadError.StoreNotFound)
            )
            ctx.net.sendClientResponse(msg)

        case op: OpportunisticRebuild =>
        case s: TransactionCompletionQuery =>

      case Some(store) => msg match
        case r: Read =>
          if r.objectPointer.poolId == store.storeId.poolId then
            store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer, r.readType)

        case op: OpportunisticRebuild => store.frontend.readObjectForOpportunisticRebuild(op)

        case s: TransactionCompletionQuery =>
          val isComplete = ctx.txStatusCache.getStatus(s.transactionId) match
            case None => false
            case Some(e) => e.status match
              case TransactionStatus.Unresolved => false
              case _ => true
          val r = TransactionCompletionResponse(s.fromClient, s.toStore, s.queryUUID, isComplete)
          ctx.net.sendClientResponse(r)

  def deliverCompletion(op: Completion): Unit =
    stores.get(op.storeId).foreach: store =>
      store.frontend.backendOperationComplete(op)

  def repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
    stores.get(storeId) match
      case Some(store) => store.repair(os, completion)
      case None =>
        // The store left between the sweep selecting it and this event being handled. Leaving
        // the promise open would strand the scan that awaits it, and with it the sweep's
        // single-flight slot -- repair would stop host-wide until the process restarts.
        // Failing also leaves the errorTree entry in place for the store's new host.
        logger.debug(s"Ignoring repair of ${os.pointer.id}: store $storeId is not hosted here")
        completion.failure(StoreNotHosted(storeId))

  def repairDelete(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte], completion: Promise[Unit]): Unit =
    stores.get(storeId) match
      case Some(store) => store.repairDelete(objectId, storePointer, completion)
      case None =>
        // Must fail, not succeed. Succeeding would tell the caller the slice was deleted, and
        // the caller would drop the errorTree entry -- leaving the object on the departed
        // store with no record that it needs removing.
        logger.debug(s"Ignoring repair deletion of $objectId: store $storeId is not hosted here")
        completion.failure(StoreNotHosted(storeId))

  def heartbeat(): Unit =
    stores.valuesIterator.foreach(_.heartbeat())

  def get(storeId: StoreId): Option[Store] = stores.get(storeId)

  def isOffline(storeId: StoreId): Boolean = offline.contains(storeId)

  def storeIds: List[StoreId] = stores.keysIterator.toList

  def hasTransactions: Boolean = stores.valuesIterator.exists(_.hasTransactions)

  def logTransactionStatus(log: String => Unit): Unit = stores.values.foreach(_.logTransactionStatus(log))

  /** The loaded stores, by id */
  private[server] def loaded: Map[StoreId, Store] = stores

  /** The stores marked offline */
  private[server] def offlineStores: Set[StoreId] = offline

  /** Makes the store loaded and clears its offline mark */
  private[host] def install(store: Store): Unit =
    stores += store.storeId -> store
    offline -= store.storeId
    ctx.publishView()

  /** Unloads the store and marks it offline */
  private[host] def remove(storeId: StoreId): Unit =
    stores -= storeId
    offline += storeId
    ctx.publishView()

  /** Marks a store offline without touching the loaded stores */
  private[host] def markOffline(storeId: StoreId): Unit =
    offline += storeId
    ctx.publishView()

  /** Clears a store's offline mark without touching the loaded stores */
  private[host] def clearOffline(storeId: StoreId): Unit =
    offline -= storeId
    ctx.publishView()
