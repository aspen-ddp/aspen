package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, TaskStopped}
import scribe.Logging

import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

object SetRebalanceDurableTask:

  private[rebalancing] type Transfer = (StoreId, StorageDeviceId, StorageDeviceId)

  /** Backoff for starts that were RolledBack or failed (see TransferBackoff). */
  val StartBackoffBase: Duration = Duration(1, SECONDS)
  val StartBackoffMax: Duration = Duration(2, MINUTES)

  private[rebalancing] enum Phase:
    case Completed, InFlight, NotStarted

  private[rebalancing] def classify(client: AspenClient, t: Transfer): Future[Phase] =
    given ExecutionContext = client.clientContext
    val (storeId, fromDevice, toDevice) = t
    for
      dst <- client.getStorageDeviceState(toDevice)
      src <- client.getStorageDeviceState(fromDevice)
    yield
      val dstEntry = dst.stores.get(storeId)
      val srcEntry = src.stores.get(storeId)
      if dstEntry.exists(_.status == StorageDeviceState.StoreStatus.Active) && srcEntry.isEmpty then
        Phase.Completed
      else if dstEntry.exists(_.status == StorageDeviceState.StoreStatus.TransferringIn) ||
              srcEntry.exists(_.status == StorageDeviceState.StoreStatus.TransferringOut) then
        Phase.InFlight
      else if dstEntry.exists(_.status == StorageDeviceState.StoreStatus.Rebuilding) then
        // The source died and FailedStorageDeviceDurableTask is rebuilding the store onto the
        // planned destination. The rebuild is effectively the transfer: once it is Active there
        // it reads as Completed, so wait for it rather than trying to start anything.
        Phase.InFlight
      else
        Phase.NotStarted

/** Drives one storage-device-set rebalance to completion. Several store transfers may be in
 *  flight at once, as many as `TransferSafety` admits. Progress is the set's `pendingTransfers`
 *  list, which is drained as each transfer completes. Blocked transfers (below the floor,
 *  offline, backing off, no usable destination) are skipped and revisited on the next poll.
 *  Extends DurableTask for stop()/isStopped; it is constructed and driven directly by
 *  RebalancingDurableService (not via SimpleTaskExecutor). */
class SetRebalanceDurableTask(
    val taskPointer: DurableTaskPointer,
    val client: AspenClient,
    val setId: StorageDeviceSetId,
    pollPeriod: Duration,
    rng: Random = new Random()  // Destination-selection randomness; a test seam.
) extends DurableTask with Logging:
  import SetRebalanceDurableTask.{Phase, Transfer}

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  // In memory only, which is fine for a crash-only task: the durable progress is
  // pendingTransfers, and a restarted task merely retries a little sooner.
  private val backoff = new TransferBackoff(() => System.currentTimeMillis(),
    SetRebalanceDurableTask.StartBackoffBase, SetRebalanceDurableTask.StartBackoffMax)

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
   *  removing it, (b) drop a transfer whose pool has left planning state, or (c) go through
   *  every not-started transfer whose backoff allows it, replacing an unusable destination or
   *  starting the transfer. In-flight transfers are simply waited for. Ends in scheduleRecheck()
   *  unless it re-enters processNext(). */
  private def drive(pending: List[Transfer]): Future[Unit] =
    Future.traverse(pending)(t => SetRebalanceDurableTask.classify(client, t).map(t -> _)).flatMap: classified =>
      classified.collectFirst { case (t, Phase.Completed) => t } match
        case Some(t) =>
          removePending(t).map(_ => processNext())
        case None =>
          client.getStorageDeviceSetState(setId).flatMap: setState =>
            State.getStateForRebalancePlanning(client, setState).flatMap: planningState =>
              val notStarted = classified.collect:
                case (t, Phase.NotStarted) if backoff.ready(t._1) => t

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
                  // Sequential, not parallel: each start re-reads the pool's state, so it sees
                  // the transfers this pass has already started.
                  notStarted.foldLeft(Future.unit): (prev, t) =>
                    prev.flatMap(_ => advance(t, setState, planningState))
                  .map(_ => scheduleRecheck())

  /** Make progress on one not-started transfer: replace its destination if that is no longer
   *  usable, otherwise try to start it. Never fails; errors are logged and retried by the poll. */
  private def advance(t: Transfer,
                      setState: StorageDeviceSetState,
                      planningState: State.PlanningState): Future[Unit] =
    val (storeId, _, toDevice) = t
    val f = client.getStorageDeviceState(toDevice).flatMap: dst =>
      if !usable(dst) then
        replaceDestination(t, setState, planningState)
      else
        // KNOWN RACE -- the admission check inside TransferStarter.start and the start itself
        // are not atomic, so a competing initiator (another set's rebalance, a pool migration)
        // can pass the same check and together take the pool below the floor. TransferStarter
        // detects that after the start commits and rolls this transfer back (RolledBack), which
        // is why RolledBack backs off here. See the TransferStarter and TransferSafety scaladoc.
        TransferStarter.start(client, storeId, toDevice).transform:
          case Success(StartResult.Started) =>
            backoff.succeeded(storeId)
            Success(())
          case Success(StartResult.RolledBack) =>
            logger.info(s"Rebalance set $setId: start of $storeId was rolled back; backing off")
            backoff.failed(storeId)
            Success(())
          case Success(StartResult.Rejected) =>
            // Not admissible right now; the poll re-evaluates.
            Success(())
          case Failure(err) =>
            // Ambiguous: transferStore failed, or the rollback's abort failed and left the
            // transfer started. Either way the next poll's classify sees the true state (a
            // started transfer reads as InFlight), so just back off.
            logger.warn(s"Rebalance set $setId: start of $storeId failed (will retry via poll): $err")
            backoff.failed(storeId)
            Success(())
    f.recover:
      case err =>
        logger.warn(s"Rebalance set $setId: could not advance transfer of $storeId " +
          s"(will retry via poll): $err")

  /** A destination is usable when it is not failed and its host is not offline. */
  private def usable(device: StorageDeviceState): Boolean =
    !device.isFailed && !client.offlineHosts().contains(device.hostId)

  /** Pick a new destination for a not-started transfer whose planned one is unusable and write
   *  it back into pendingTransfers. The entry is never dropped: when nothing usable can be
   *  selected it is left as is for the next poll. Selection soft-excludes the pool's devices and
   *  the destinations of its other pending transfers, as the migration task does. */
  private def replaceDestination(t: Transfer,
                                 setState: StorageDeviceSetState,
                                 planningState: State.PlanningState): Future[Unit] =
    val (storeId, fromDevice, toDevice) = t
    val poolDevices = planningState.devices.values
      .filter(_.stores.keys.exists(_.poolId == storeId.poolId))
      .map(_.deviceId).toSet
    // Planning state drops TransferringIn entries and knows nothing of planned destinations, so
    // the destinations of the pool's other pending transfers are excluded too. Without them a
    // replacement can land on a device already receiving, or planned to receive, another store
    // of the same pool. Still a SOFT exclude, as the migration task's.
    val pendingDestinations = setState.pendingTransfers.collect:
      case (sid, _, to) if sid.poolId == storeId.poolId && sid != storeId => to
    val requiredSize = planningState.pools.get(storeId.poolId)
      .flatMap(_.stores.get(storeId))
      .map(_.currentSize)
      .getOrElse(0L)
    val currentDevice = planningState.devices.values.find(_.stores.contains(storeId)).map(_.deviceId)

    def skip(why: String): Future[Unit] =
      logger.info(s"Rebalance set $setId: no new destination for $storeId (planned $toDevice is " +
        s"unusable): $why; will retry")
      Future.unit

    setState.selectDeviceForStore(requiredSize, poolDevices ++ pendingDestinations, client, rng)
      .map(Some(_))
      .recover { case _ => None }
      .flatMap:
        case None =>
          skip("selection found no device with room")
        case Some(candidate) =>
          // Selection falls back to soft-excluded devices, so it can return the store's own
          // device, which is not a legal destination.
          if candidate == fromDevice || currentDevice.contains(candidate) || candidate == toDevice then
            skip(s"only $candidate was selectable")
          else
            client.getStorageDeviceState(candidate).flatMap: dev =>
              if !usable(dev) then
                skip(s"selected $candidate is itself unusable")
              else
                logger.info(s"Rebalance set $setId: replacing unusable destination $toDevice " +
                  s"of $storeId with $candidate")
                replacePending(t, (storeId, fromDevice, candidate))

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

  /** Replace one transfer in the set's pendingTransfers, revision-checked. A no-op when `old`
   *  is no longer listed (a concurrent pass already replaced or removed it). */
  private def replacePending(old: Transfer, replacement: Transfer): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given org.aspen_ddp.aspen.client.Transaction = tx
      for
        setPtr <- client.getStorageDeviceSetPointer(setId)
        dos <- client.read(setPtr)
      yield
        val current = StorageDeviceSetState(dos)
        if current.pendingTransfers.contains(old) then
          val updated = current.copy(pendingTransfers =
            current.pendingTransfers.map(p => if p == old then replacement else p))
          tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))
