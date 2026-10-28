package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{boundedSingleFlight, runBoundedParallel}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, TaskStopped}
import scribe.Logging

import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

object SetRebalanceDurableTask:

  private[rebalancing] type Transfer = (StoreId, StorageDeviceId, StorageDeviceId)

  /** Backoff for starts that were RolledBack or failed (see TransferBackoff). */
  val StartBackoffBase: FiniteDuration = Duration(1, SECONDS)
  val StartBackoffMax: FiniteDuration = Duration(2, MINUTES)
  val StartBackoffJitter: Double = 0.25

  /** How long a transfer may stay InFlight before the task starts warning about it, and the
   *  interval between further warnings. */
  val StuckWarnAfter: Duration = Duration(30, MINUTES)

  /** Selection attempts per poll when replacing an unusable destination. */
  val ReplacementAttempts: Int = 3

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
      // Offline counts as arrived: a transfer that finishes onto a destination asked to shut down
      // writes its entry Offline, not Active (Host.updateStateForTransferredStore). The store has
      // moved, and planning it again would move it a second time.
      if srcEntry.isEmpty && dstEntry.exists(e =>
           e.status == StorageDeviceState.StoreStatus.Active ||
           e.status == StorageDeviceState.StoreStatus.Offline) then
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
    SetRebalanceDurableTask.StartBackoffBase, SetRebalanceDurableTask.StartBackoffMax, new Random(), SetRebalanceDurableTask.StartBackoffJitter)

  /** Single-flight guard shared by the poll and wake(). Two passes may overlap (maxInFlight 2) so
   *  a wake-up arriving mid-pass is not lost, but a pileup is bounded. A pass handles its own
   *  errors, so a failed Future or synchronous throw only releases the guard. */
  private val guardedRun: () => Unit =
    boundedSingleFlight(s"set-rebalance-task $setId", maxInFlight = 2)(countedPass())

  private val passes = new java.util.concurrent.atomic.AtomicInteger(0)

  private def countedPass(): Future[Unit] =
    processNext().andThen(_ => passes.incrementAndGet())

  /** Testing hook: how many polling passes have finished, for waiting on "N more passes ran"
   *  instead of sleeping. */
  private[rebalancing] def testingOnlyCompletedPasses: Int = passes.get

  /** Every invocation, including one the guard drops, arms the next tick first. Arming at the
   *  end of a pass instead loses the wakeup whenever a tick fires between that arming and the
   *  guard's release: the tick is dropped and nothing is left to re-arm, so the task stalls
   *  for good. */
  private def run(): Unit =
    scheduleRecheck()
    guardedRun()

  // Begin driving immediately.
  run()

  /** External wake-up (e.g. a TransferComplete message arrived). */
  def wake(): Unit = run()

  override protected def onStop(): Unit = synchronized:
    pollTask.cancel()
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def scheduleRecheck(): Unit = synchronized:
    if !isStopped && !promise.isCompleted then
      pollTask.cancel()
      pollTask = client.backgroundTaskManager.schedule(pollPeriod):
        run()

  private def finishOk(): Unit = synchronized:
    if !promise.isCompleted then
      pollTask.cancel()
      promise.success(None)

  private def processNext(): Future[Unit] =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
      Future.unit
    else
      val f = client.getStorageDeviceSetState(setId).flatMap: setState =>
        val pending = setState.pendingTransfers
        if pending.isEmpty then
          finishOk()
          Future.unit
        else
          drive(pending)
      f.recover:
        case err => logger.warn(s"Rebalance set $setId: transient error, will retry: $err")

  // When each store was first seen InFlight, and when it was last warned about. In memory only,
  // like the backoff. Passes may overlap, hence the monitor.
  private var inFlightSince: Map[StoreId, Long] = Map.empty
  private var lastStuckWarning: Map[StoreId, Long] = Map.empty

  /** Warns, every StuckWarnAfter, about a transfer that has stayed InFlight that long. The task
   *  only waits for these: an InFlight transfer or a Rebuilding destination is owned by the
   *  transfer machinery or by FailedStorageDeviceDurableTask, and acting on it from here could
   *  fight them. A rebuild that never finishes would otherwise stall the set's rebalance
   *  without a trace. */
  private def warnIfStuck(classified: Seq[(Transfer, Phase)]): Unit = synchronized:
    val now = System.currentTimeMillis()
    val warnAfterMs = SetRebalanceDurableTask.StuckWarnAfter.toMillis
    val current = classified.collect { case (t, Phase.InFlight) => t._1 }.toSet
    inFlightSince = current.map(sid => sid -> inFlightSince.getOrElse(sid, now)).toMap
    lastStuckWarning = lastStuckWarning.filter((sid, _) => current.contains(sid))
    inFlightSince.foreach: (sid, since) =>
      val last = lastStuckWarning.getOrElse(sid, since)
      if now - since >= warnAfterMs && now - last >= warnAfterMs then
        lastStuckWarning += sid -> now
        logger.warn(s"Rebalance set $setId: transfer of $sid has been in flight for " +
          s"${(now - since) / 60000} minutes. If its destination is Rebuilding, the rebuild " +
          s"may be stuck; this task only waits for it")

  /** Given a non-empty pending list, either (a) finish an already-completed transfer by
   *  removing it, (b) drop a transfer whose pool has left planning state, or (c) go through
   *  every not-started transfer whose backoff allows it, replacing an unusable destination or
   *  starting the transfer. In-flight transfers are simply waited for. Re-enters processNext()
   *  after a removal; the poll was already armed by run(). */
  private def drive(pending: List[Transfer]): Future[Unit] =
    runBoundedParallel(pending, State.DefaultMaxConcurrentReads): t =>
      SetRebalanceDurableTask.classify(client, t).map(t -> _)
    .flatMap: classified =>
      warnIfStuck(classified)
      classified.collectFirst { case (t, Phase.Completed) => t } match
        case Some(t) =>
          removePending(t).flatMap(_ => processNext())
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
                  removePending(t).flatMap(_ => processNext())

                case None =>
                  // Sequential, not parallel: each start re-reads the pool's state, so it sees
                  // the transfers this pass has already started.
                  notStarted.foldLeft(Future.unit): (prev, t) =>
                    prev.flatMap(_ => advance(t, setState, planningState))
                  .map(_ => ())

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

  /** A destination is usable when it is operational and its host is not offline. */
  private def usable(device: StorageDeviceState): Boolean =
    !device.isFailed && device.isOperational && !client.offlineHosts().contains(device.hostId)

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
    val currentDevice = planningState.devices.values.find(_.stores.contains(storeId)).map(_.deviceId)

    def skip(why: String): Future[Unit] =
      logger.info(s"Rebalance set $setId: no new destination for $storeId (planned $toDevice is " +
        s"unusable): $why; will retry")
      Future.unit

    // Never guess a size: a requirement of 0 would accept a device with no room for the store.
    requiredSize match
      case None =>
        skip("the store is not in the planning state, so its size is unknown")
      case Some(size) =>
        // A rejected candidate is soft-excluded from the next attempt, so one unusable pick does
        // not cost a whole poll period.
        def attempt(n: Int, excluded: Set[StorageDeviceId]): Future[Unit] =
          setState.selectDeviceForStore(size, excluded, client, rng)
            .map(Some(_))
            .recover { case _ => None }
            .flatMap:
              case None =>
                skip("selection found no device with room")
              case Some(candidate) =>
                def next(why: String): Future[Unit] =
                  if n < SetRebalanceDurableTask.ReplacementAttempts then
                    attempt(n + 1, excluded + candidate)
                  else
                    skip(why)

                // Selection falls back to soft-excluded devices, so it can return the store's own
                // device, which is not a legal destination.
                if candidate == fromDevice || currentDevice.contains(candidate) || candidate == toDevice then
                  next(s"only $candidate was selectable")
                else
                  client.getStorageDeviceState(candidate).flatMap: dev =>
                    if !usable(dev) then
                      next(s"selected $candidate is itself unusable")
                    else
                      logger.info(s"Rebalance set $setId: replacing unusable destination $toDevice " +
                        s"of $storeId with $candidate")
                      replacePending(t, (storeId, fromDevice, candidate))

        attempt(1, poolDevices ++ pendingDestinations)

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
