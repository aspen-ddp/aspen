package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StorageDeviceSetState}
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceExecutor, DurableServiceFactory, DurableTaskPointer, ServiceEntry}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future}

object RebalancingDurableService extends DurableServiceFactory with Logging:

  val ServiceTypeUUID: UUID = UUID.fromString("d5f1e2a7-0b3c-4e6a-9f21-7c8a4b1e0d33")
  val ServiceUUID: UUID     = UUID.fromString("b7c04e18-2a9f-4d55-8e13-6f0a1c2d3e4b")

  val typeUUID: UUID = ServiceTypeUUID

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  /** Overridable poll period (test seam; mirrors MissedUpdateFinalizationAction.errorTimeout). */
  @volatile var pollPeriod: Duration = DefaultPollPeriod

  override def createService(client: AspenClient,
                             statePointer: KeyValueObjectPointer,
                             state: KeyValueObjectState): DurableService =
    new RebalancingDurableService(client, statePointer, pollPeriod)

  /** Register the singleton service (idempotent). Call once per system at host startup, from
   *  the same place the DurableServiceExecutor is created. */
  def register(executor: DurableServiceExecutor): Future[Unit] =
    executor.registerService(
      ServiceTypeUUID,
      ServiceUUID,
      Map(RebalancingServiceState.ActiveTasksKey -> RebalancingServiceState.encodeActiveTasks(Nil)))

  /** Read the service state object's pointer via the services TKVL. */
  private def readServiceStatePointer(client: AspenClient): Future[KeyValueObjectPointer] =
    given ExecutionContext = client.clientContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(ServiceUUID)).map:
      case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
      case None => throw new IllegalStateException("RebalancingDurableService is not registered")

  /** Build a plan for `setId` and enroll a SetRebalanceDurableTask, unless one is already in
   *  progress. Idempotent and safe against concurrent callers (revision-checked writes). */
  def rebalanceStorageDeviceSet(client: AspenClient, setId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = client.clientContext

    for
      setState <- client.getStorageDeviceSetState(setId)
      result <-
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
    yield result

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

  private def reconcile(): Future[Unit] =
    if stopped then Future.unit
    else
      client.read(statePointer).map: kvos =>
        val active = RebalancingServiceState.decodeActiveTasks(
                       kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        synchronized:
          if !stopped then
            active.foreach: (setId, taskStatePtr) =>
              if !tasks.contains(setId) && !completing.contains(setId) then
                startTask(setId, taskStatePtr)
      .recover:
        case err => logger.warn(s"RebalancingDurableService reconcile failed: $err")

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
