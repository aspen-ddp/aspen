package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, TaskStopped}
import scribe.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Drives one storage-device-set rebalance to completion, serially: one store transfer in
 *  flight at a time. Progress is the set's `pendingTransfers` list, which is drained as each
 *  transfer completes. Blocked transfers (write-threshold/offline) are skipped and revisited.
 *  Extends DurableTask for stop()/isStopped; it is constructed and driven directly by
 *  RebalancingDurableService (not via SimpleTaskExecutor). */
class SetRebalanceDurableTask(
    val taskPointer: DurableTaskPointer,
    val client: AspenClient,
    val setId: StorageDeviceSetId,
    pollPeriod: Duration
) extends DurableTask with Logging:

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  // Begin driving immediately.
  processNext()

  /** External wake-up (e.g. a TransferComplete message arrived). */
  def wake(): Unit = processNext()

  override protected def onStop(): Unit = synchronized:
    pollTask.cancel()
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def scheduleRecheck(): Unit = synchronized:
    if !isStopped then
      pollTask.cancel()
      pollTask = client.backgroundTaskManager.schedule(pollPeriod):
        processNext()

  private def finishOk(): Unit = synchronized:
    if !promise.isCompleted then promise.success(None)

  private type Transfer = (StoreId, StorageDeviceId, StorageDeviceId)

  private def processNext(): Unit =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
    else
      val f = client.getStorageDeviceSetState(setId).flatMap: setState =>
        val pending = setState.pendingTransfers
        if pending.isEmpty then
          finishOk()
          Future.unit
        else
          drive(pending)
      f.failed.foreach: err =>
        logger.warn(s"Rebalance set $setId: transient error, will retry: $err")
        scheduleRecheck()

  /** Given a non-empty pending list, either (a) finish an already-completed transfer by
   *  removing it, (b) wait for an in-flight transfer, or (c) start the first safe not-started
   *  transfer. Falls through to scheduleRecheck() when nothing can progress right now. */
  private def drive(pending: List[Transfer]): Future[Unit] =
    Future.traverse(pending)(classify).flatMap: classified =>
      classified.collectFirst { case (t, Phase.Completed) => t } match
        case Some(t) =>
          removePending(t).map(_ => processNext())
        case None =>
          if classified.exists(_._2 == Phase.InFlight) then
            scheduleRecheck()
            Future.unit
          else
            client.getStorageDeviceSetState(setId).flatMap: setState =>
              State.getStateForRebalancePlanning(client, setState).flatMap: planningState =>
                val notStarted = classified.collect { case (t, Phase.NotStarted) => t }

                // A pool that has left planning state (its migration is InProgress) is owned by
                // MigratePoolToSetDurableTask. isSafe would return false forever for its
                // transfers -- pools.get(poolId) is None -- wedging this set's whole rebalance.
                // Drop them instead. migratePoolToSet strips this list at migration start; this
                // is the backstop for a plan enrolled just after that strip committed.
                notStarted.find(t => !planningState.pools.contains(t._1.poolId)) match
                  case Some(t) =>
                    logger.info(s"Rebalance set $setId: dropping transfer for ${t._1}; " +
                      s"its pool is no longer in planning state")
                    removePending(t).map(_ => processNext())

                  case None =>
                    val offline = client.offlineHosts()
                    notStarted.find(t => TransferSafety.isSafe(planningState, offline,
                        Plan.Transfer(t._1, t._2, t._3))) match
                      case Some((storeId, _, toDevice)) =>
                        client.transferStore(storeId, toDevice).failed.foreach: err =>
                          logger.warn(s"Rebalance set $setId: transferStore for $storeId failed " +
                            s"(will retry via poll): $err")
                        scheduleRecheck()
                      case None =>
                        scheduleRecheck()
                    Future.unit

  private enum Phase:
    case Completed, InFlight, NotStarted

  private def classify(t: Transfer): Future[(Transfer, Phase)] =
    val (storeId, fromDevice, toDevice) = t
    for
      dst <- client.getStorageDeviceState(toDevice)
      src <- client.getStorageDeviceState(fromDevice)
    yield
      val dstEntry = dst.stores.get(storeId)
      val srcEntry = src.stores.get(storeId)
      val phase =
        if dstEntry.exists(_.status == StorageDeviceState.StoreStatus.Active) && srcEntry.isEmpty then
          Phase.Completed
        else if dstEntry.exists(_.status == StorageDeviceState.StoreStatus.TransferringIn) ||
                srcEntry.exists(_.status == StorageDeviceState.StoreStatus.TransferringOut) then
          Phase.InFlight
        else
          Phase.NotStarted
      (t, phase)

  /** Remove one completed transfer from the set's pendingTransfers, revision-checked. */
  private def removePending(t: Transfer): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given org.aspen_ddp.aspen.client.Transaction = tx
      for
        setPtr <- client.getStorageDeviceSetPointer(setId)
        dos <- client.read(setPtr)
      yield
        val current = StorageDeviceSetState(dos)
        val updated = current.copy(pendingTransfers = current.pendingTransfers.filterNot(_ == t))
        tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))
