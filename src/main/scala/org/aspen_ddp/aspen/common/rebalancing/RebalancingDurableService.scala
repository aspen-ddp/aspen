package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StorageDeviceSetState}
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceFactory, DurableTaskPointer, ServiceEntry}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, HOURS, MINUTES}
import scala.concurrent.{ExecutionContext, Future}

object RebalancingDurableService extends DurableServiceFactory with Logging:

  val ServiceTypeUUID: UUID = UUID.fromString("d5f1e2a7-0b3c-4e6a-9f21-7c8a4b1e0d33")
  val ServiceUUID: UUID     = UUID.fromString("b7c04e18-2a9f-4d55-8e13-6f0a1c2d3e4b")

  val typeUUID: UUID = ServiceTypeUUID

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  /** Overridable poll period (test seam; mirrors MissedUpdateFinalizationAction.errorTimeout). */
  @volatile var pollPeriod: Duration = DefaultPollPeriod

  /** Default interval between automatic rebalance sweeps. Distinct from DefaultPollPeriod,
   *  which is how often reconcile() runs. */
  val DefaultAutoRebalancePeriod: Duration = Duration(8, HOURS)

  override def createService(client: AspenClient,
                             statePointer: KeyValueObjectPointer,
                             state: KeyValueObjectState): DurableService =
    new RebalancingDurableService(client, statePointer, pollPeriod)

  /** The initial contents of the service's KV state object. Written into the services tree by
   *  the Bootstrap process (the service is a system-critical singleton that must always exist).
   *  All three keys are always written, so readers never have to handle an absent key. */
  def initialServiceState: Map[Key, Array[Byte]] =
    Map(
      RebalancingServiceState.ActiveTasksKey ->
        RebalancingServiceState.encodeActiveTasks(Nil),
      RebalancingServiceState.AutoRebalancePeriodKey ->
        RebalancingServiceState.encodeAutoRebalancePeriod(DefaultAutoRebalancePeriod),
      RebalancingServiceState.LastAutoRebalanceKey ->
        RebalancingServiceState.encodeLastAutoRebalance(HLCTimestamp.Zero)
    )

  /** Read the service state object's pointer via the services TKVL. */
  private def readServiceStatePointer(client: AspenClient): Future[KeyValueObjectPointer] =
    given ExecutionContext = client.clientContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(ServiceUUID)).map:
      case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
      case None => throw new IllegalStateException("RebalancingDurableService is not registered")

  /** The automatic rebalance period together with the time of the last completed sweep. Read
   *  as a pair because the two live in the same object and the cmdline displays both, so a
   *  caller that wants both should not pay for two round trips. */
  private[aspen] def getAutoRebalanceStatus(client: AspenClient): Future[(Duration, HLCTimestamp)] =
    given ExecutionContext = client.clientContext
    for
      statePtr <- readServiceStatePointer(client)
      kvos <- client.read(statePtr)
    yield
      (RebalancingServiceState.decodeAutoRebalancePeriod(
         kvos.contents(RebalancingServiceState.AutoRebalancePeriodKey).value.bytes),
       RebalancingServiceState.decodeLastAutoRebalance(
         kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).value.bytes))

  /** The interval between automatic rebalance sweeps. Zero means automatic rebalancing is off. */
  def getAutoRebalancePeriod(client: AspenClient): Future[Duration] =
    given ExecutionContext = client.clientContext
    getAutoRebalanceStatus(client).map(_._1)

  /** Set the interval between automatic rebalance sweeps; zero disables them. Deliberately does
   *  not touch LastAutoRebalanceKey, so shortening the period can make a sweep immediately due
   *  and lengthening it defers the next one relative to the last sweep that actually ran.
   *
   *  The nudge that follows is best-effort, as everywhere else in this service: the running
   *  service re-reads this value on every poll, so a dropped message costs latency, not
   *  correctness. */
  def setAutoRebalancePeriod(client: AspenClient, period: Duration): Future[Unit] =
    given ExecutionContext = client.clientContext

    val done: Future[Unit] = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        statePtr <- readServiceStatePointer(client)
        kvos <- client.read(statePtr)
      yield
        val reqs = KeyValueUpdate.KeyRevision(
                     RebalancingServiceState.AutoRebalancePeriodKey,
                     kvos.contents(RebalancingServiceState.AutoRebalancePeriodKey).revision) :: Nil
        val ops = Insert(RebalancingServiceState.AutoRebalancePeriodKey,
                    RebalancingServiceState.encodeAutoRebalancePeriod(period)) :: Nil
        tx.update(statePtr, None, None, reqs, ops)

    done.map: _ =>
      client.sendServiceMessage(ServiceUUID, RebalancingMessage.encode(AutoRebalancePeriodChanged))
      ()

  /** Build a plan for `setId` and enroll a SetRebalanceDurableTask, unless one is already in
   *  progress. Idempotent and safe against concurrent callers (revision-checked writes). */
  def rebalanceStorageDeviceSet(client: AspenClient, setId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.getStorageDeviceSetState(setId).flatMap(rebalanceReadSet(client, setId, _))

  /** As above, for a caller that has already read the set's state. The automatic sweep reads
   *  it to filter on level, and would otherwise pay for the same read twice.
   *
   *  Note that the level check is deliberately *not* here: the public entry point above must
   *  keep surfacing the planning failure when a user names a non-level-0 set on the command
   *  line, rather than silently doing nothing. */
  private def rebalanceReadSet(client: AspenClient,
                               setId: StorageDeviceSetId,
                               setState: StorageDeviceSetState): Future[Unit] =
    given ExecutionContext = client.clientContext

    if setState.pendingTransfers.nonEmpty then
      Future.unit
    else
      for
        statePtr <- readServiceStatePointer(client)
        stateKvos <- client.read(statePtr)
        active = RebalancingServiceState.decodeActiveTasks(
                   stateKvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        r <-
          if active.exists(_._1 == setId) then
            Future.unit
          else
            planAndEnroll(client, setId, setState, statePtr)
      yield r

  private def planAndEnroll(client: AspenClient,
                            setId: StorageDeviceSetId,
                            setState: StorageDeviceSetState,
                            statePtr: KeyValueObjectPointer): Future[Unit] =
    given ExecutionContext = client.clientContext
    for
      planningState <- State.getStateForRebalancePlanning(client, setState)
      plan = Plan.computePlan(planningState)
      _ <-
        if plan.isEmpty then Future.unit
        else enroll(client, setId, plan.map(t => (t.storeId, t.fromDevice, t.toDevice)), statePtr)
    yield ()

  private def enroll(client: AspenClient,
                     setId: StorageDeviceSetId,
                     transfers: List[(StoreId, StorageDeviceId, StorageDeviceId)],
                     statePtr: KeyValueObjectPointer): Future[Unit] =
    given ExecutionContext = client.clientContext

    val done: Future[Unit] = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        setPtr <- client.getStorageDeviceSetPointer(setId)
        setDos <- client.read(setPtr)
        stateKvos <- client.read(statePtr)
        active = RebalancingServiceState.decodeActiveTasks(
                   stateKvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        _ <-
          if active.exists(_._1 == setId) then
            // A concurrent caller enrolled this set between the pre-check and here. No-op:
            // do not allocate a task-state object and do not write anything.
            Future.unit
          else
            for
              pool <- client.getStoragePool(Radicle.poolId)
              allocator = new PoolObjectAllocator(client, pool)
              taskStatePtr <- allocator.allocateKeyValueObject(
                                SetRebalanceTaskState.initialContent(setId).map((k, v) => k -> Value(v)))
            yield
              val updatedSet = StorageDeviceSetState(setDos).copy(pendingTransfers = transfers)
              tx.overwrite(setPtr, setDos.revision, DataBuffer(updatedSet.toBytes))

              val newActive = active :+ (setId -> taskStatePtr)
              val reqs = KeyValueUpdate.KeyRevision(
                           RebalancingServiceState.ActiveTasksKey,
                           stateKvos.contents(RebalancingServiceState.ActiveTasksKey).revision) :: Nil
              val ops = Insert(RebalancingServiceState.ActiveTasksKey,
                          RebalancingServiceState.encodeActiveTasks(newActive)) :: Nil
              tx.update(statePtr, None, None, reqs, ops)
      yield ()

    done.map: _ =>
      client.sendServiceMessage(ServiceUUID, RebalancingMessage.encode(NewSetRebalanceInitiated(setId)))
      ()


class RebalancingDurableService(val client: AspenClient,
                                val statePointer: KeyValueObjectPointer,
                                pollPeriod: Duration) extends DurableService with Logging:

  private given ExecutionContext = client.clientContext

  private var tasks: Map[StorageDeviceSetId, SetRebalanceDurableTask] = Map.empty
  private var completing: Set[StorageDeviceSetId] = Set.empty
  private var pollTask: ScheduledTask = NoTask
  private var sweeping = false
  @volatile private var stopped = false

  reconcile()
  pollTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask(pollPeriod):
    reconcile()

  override def shutdown(): Unit = synchronized:
    stopped = true
    pollTask.cancel()
    tasks.values.foreach(_.stop())
    tasks = Map.empty
    completing = Set.empty

  override def receiveMessage(msg: ServiceMessage): Unit =
    RebalancingMessage.decode(msg.encodedContent) match
      case NewSetRebalanceInitiated(_) =>
        reconcile()
      case TransferComplete(setId, _, _, _) =>
        synchronized(tasks.get(setId)).foreach(_.wake())
      case AutoRebalancePeriodChanged =>
        reconcile()

  private def reconcile(): Future[Unit] =
    if stopped then Future.unit
    else
      client.read(statePointer).flatMap: kvos =>
        val active = RebalancingServiceState.decodeActiveTasks(
                       kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        synchronized:
          if !stopped then
            active.foreach: (setId, taskStatePtr) =>
              if !tasks.contains(setId) && !completing.contains(setId) then
                startTask(setId, taskStatePtr)

        val period = RebalancingServiceState.decodeAutoRebalancePeriod(
                       kvos.contents(RebalancingServiceState.AutoRebalancePeriodKey).value.bytes)
        val lastSweep = RebalancingServiceState.decodeLastAutoRebalance(
                          kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).value.bytes)

        if !sweepIsDue(period, lastSweep) || !beginSweep() then
          Future.unit
        else
          sweep()
            .andThen { case _ => endSweep() }
            .recover:
              case err => logger.warn(s"RebalancingDurableService sweep failed: $err")
      .recover:
        case err => logger.warn(s"RebalancingDurableService reconcile failed: $err")

  /** A period of zero disables sweeping. The value is still read on every poll, so re-enabling
   *  takes effect within one poll interval even if the nudge is lost.
   *
   *  A lastSweep in the future -- clock skew, or an HLC advanced by a peer -- yields a negative
   *  difference and simply defers the sweep until wall time catches up. */
  private def sweepIsDue(period: Duration, lastSweep: HLCTimestamp): Boolean =
    period > Duration.Zero && (HLCTimestamp.now - lastSweep) >= period

  /** Claim the sweep, returning false if one is already running. scheduleNonConcurrentPollingTask
   *  suppresses overlapping *timer* ticks, but receiveMessage calls reconcile() directly and can
   *  land on top of a timer-driven sweep. */
  private def beginSweep(): Boolean = synchronized:
    if sweeping || stopped then
      false
    else
      sweeping = true
      true

  private def endSweep(): Unit = synchronized { sweeping = false }

  /** Plan and enroll a rebalance for every level-0 storage device set, one set at a time, then
   *  record the sweep. Sequential rather than fanned out: each enrollment is a revision-checked
   *  write to the shared ActiveTasksKey, so concurrency here buys collisions rather than speed. */
  private def sweep(): Future[Unit] =
    for
      sets <- client.listStorageDeviceSets()
      _ <- sets.foldLeft(Future.unit): (prior, entry) =>
             prior.flatMap(_ => sweepOneSet(entry._2))
      _ <- recordSweep()
    yield ()

  /** One set's share of a sweep. A failure is logged and swallowed: one bad set must not stop
   *  the rest of the sweep. */
  private def sweepOneSet(setId: StorageDeviceSetId): Future[Unit] =
    if stopped then Future.unit
    else
      client.getStorageDeviceSetState(setId).flatMap: setState =>
        // Level 1+ sets hold sets rather than devices; getStateForRebalancePlanning throws on
        // them, so they are filtered out here rather than allowed to abort the sweep.
        if setState.level != 0 then Future.unit
        else RebalancingDurableService.rebalanceReadSet(client, setId, setState)
      .recover:
        case err =>
          logger.warn(s"Automatic rebalance of storage device set ${setId.uuid} failed: $err")

  /** Record the sweep even when individual sets failed. Holding the timestamp back on failure
   *  would turn the poll into a retry loop against the whole cluster for as long as one set
   *  stayed broken.
   *
   *  Revision-checked on LastAutoRebalanceKey alone, so this does not contend with the
   *  ActiveTasksKey writes that enrollment and completion perform. */
  private def recordSweep(): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      client.read(statePointer).map: kvos =>
        val reqs = KeyValueUpdate.KeyRevision(
                     RebalancingServiceState.LastAutoRebalanceKey,
                     kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).revision) :: Nil
        val ops = Insert(RebalancingServiceState.LastAutoRebalanceKey,
                    RebalancingServiceState.encodeLastAutoRebalance(HLCTimestamp.now)) :: Nil
        tx.update(statePointer, None, None, reqs, ops)

  private def startTask(setId: StorageDeviceSetId, taskStatePtr: KeyValueObjectPointer): Unit =
    val task = new SetRebalanceDurableTask(DurableTaskPointer(taskStatePtr), client, setId, pollPeriod)
    tasks += setId -> task
    task.completed.onComplete: result =>
      synchronized:
        tasks -= setId
        if result.isSuccess && !stopped then
          completing += setId
      if result.isSuccess && !stopped then
        removeCompleted(setId, taskStatePtr)

  private def removeCompleted(setId: StorageDeviceSetId, taskStatePtr: KeyValueObjectPointer): Unit =
    val f = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        kvos <- client.read(statePointer)
        active = RebalancingServiceState.decodeActiveTasks(
                   kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        _ <-
          if !active.exists(e => e._1 == setId && e._2.id == taskStatePtr.id) then
            // Already removed by a prior call (idempotent): do not double-decrement refcount.
            Future.unit
          else
            client.read(taskStatePtr).map: taskKvos =>
              val newActive = active.filterNot(e => e._1 == setId && e._2.id == taskStatePtr.id)
              val reqs = KeyValueUpdate.KeyRevision(
                           RebalancingServiceState.ActiveTasksKey,
                           kvos.contents(RebalancingServiceState.ActiveTasksKey).revision) :: Nil
              val ops = Insert(RebalancingServiceState.ActiveTasksKey,
                          RebalancingServiceState.encodeActiveTasks(newActive)) :: Nil
              tx.update(statePointer, None, None, reqs, ops)
              tx.setRefcount(taskStatePtr, taskKvos.refcount, taskKvos.refcount.decrement())
      yield ()
    f.foreach: _ =>
      synchronized { completing -= setId }
